"""
MEGA IA - Motor SMC/FVG + HL Reversals
Versão adaptada para sinais de opções binárias com entrada na PRÓXIMA VELA.

Base lógica:
- FVG de 3 velas (bullish/bearish)
- Estrutura HH / HL / LH / LL por pivôs confirmados
- Suporte e resistência pelos últimos pivôs confirmados
- Rejeição de preço na zona
- Contexto MTF opcional (não bloqueia obrigatoriamente)
- Somente candles fechados: sem sinal retroativo / sem "offset" enganoso
- Uma entrada por FVG para evitar repetição

Entrada esperada:
candles = [
    {"time": 1720000000, "open": 1.10, "high": 1.11, "low": 1.09, "close": 1.105, "closed": True},
    ...
]

Uso:
    from mega_smc_fvg_hl import analyze_signal

    result = analyze_signal(
        candles_m1,
        timeframe_seconds=60,
        higher_timeframes={
            "M15": candles_m15,
            "H1": candles_h1,
            "H4": candles_h4,
        },
    )

    # result["signal"] => CALL, PUT ou WAIT
    # result["entry"]  => NEXT_CANDLE quando houver sinal
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from statistics import fmean
from typing import Any, Dict, List, Optional, Sequence, Tuple


ENGINE_NAME = "SMC_FVG_HL_NEXT"
ENGINE_VERSION = "1.0.0"


@dataclass
class EngineConfig:
    left_pivot: int = 4
    right_pivot: int = 2

    # Mantém a ideia do FVG original, mas sem depender da vela aberta.
    max_fvg_age: int = 20

    # True = primeiro toque encerra a zona, como o strict_mode do Pine original.
    # False = zona só encerra após atravessar completamente.
    strict_mitigation: bool = True

    # Distância de S/R adaptativa, em múltiplos da média do range.
    sr_tolerance_range_mult: float = 0.45
    range_lookback: int = 14

    # Rejeição: pavio mínimo em relação ao corpo.
    min_wick_body_ratio: float = 0.35

    # Confiança mínima para liberar CALL/PUT.
    min_confidence: int = 68

    # Evita sinal sem estrutura/SR.
    require_structure_or_sr: bool = True

    # Contexto MTF só pesa na confiança; não bloqueia o sinal.
    mtf_bonus_each: int = 3
    mtf_penalty_each: int = 2


@dataclass
class FVGZone:
    side: str            # BULL / BEAR
    top: float
    bottom: float
    created_index: int
    mitigated: bool = False
    signaled: bool = False


def _num(x: Any) -> float:
    return float(x)


def _normalize_candles(candles: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for c in candles:
        try:
            row = {
                "time": c.get("time", c.get("timestamp")),
                "open": _num(c["open"]),
                "high": _num(c["high"]),
                "low": _num(c["low"]),
                "close": _num(c["close"]),
                "closed": bool(c.get("closed", True)),
            }
        except (KeyError, TypeError, ValueError):
            continue

        if row["high"] < row["low"]:
            continue
        out.append(row)

    # Se o provedor marca explicitamente a última vela como aberta, removemos.
    while out and out[-1].get("closed") is False:
        out.pop()

    return out


def _avg_range(data: Sequence[Dict[str, Any]], i: int, lookback: int) -> float:
    start = max(0, i - lookback + 1)
    vals = [max(0.0, data[j]["high"] - data[j]["low"]) for j in range(start, i + 1)]
    vals = [v for v in vals if v > 0]
    if not vals:
        return max(abs(data[i]["close"]) * 0.0005, 1e-12)
    return fmean(vals)


def _pivot_high(data: Sequence[Dict[str, Any]], p: int, left: int, right: int) -> bool:
    if p - left < 0 or p + right >= len(data):
        return False
    v = data[p]["high"]
    left_vals = [data[j]["high"] for j in range(p - left, p)]
    right_vals = [data[j]["high"] for j in range(p + 1, p + right + 1)]
    return v > max(left_vals) and v >= max(right_vals)


def _pivot_low(data: Sequence[Dict[str, Any]], p: int, left: int, right: int) -> bool:
    if p - left < 0 or p + right >= len(data):
        return False
    v = data[p]["low"]
    left_vals = [data[j]["low"] for j in range(p - left, p)]
    right_vals = [data[j]["low"] for j in range(p + 1, p + right + 1)]
    return v < min(left_vals) and v <= min(right_vals)


def _candle_rejection(c: Dict[str, Any], side: str, min_ratio: float) -> bool:
    o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
    body = abs(cl - o)
    full = max(h - l, 1e-12)
    body_ref = max(body, full * 0.08)

    lower_wick = min(o, cl) - l
    upper_wick = h - max(o, cl)

    if side == "BULL":
        # Rejeição compradora: fecha acima da abertura e deixa pavio inferior.
        return cl > o and lower_wick >= body_ref * min_ratio and cl >= l + full * 0.55

    # Rejeição vendedora: fecha abaixo da abertura e deixa pavio superior.
    return cl < o and upper_wick >= body_ref * min_ratio and cl <= h - full * 0.55


def _touches_zone(c: Dict[str, Any], z: FVGZone) -> bool:
    return c["high"] > z.bottom and c["low"] < z.top


def _fully_mitigated(c: Dict[str, Any], z: FVGZone) -> bool:
    if z.side == "BULL":
        return c["low"] <= z.bottom
    return c["high"] >= z.top


def _near_level(level: Optional[float], z: FVGZone, tolerance: float, candle: Dict[str, Any]) -> bool:
    if level is None:
        return False

    # Nível dentro/encostado na zona.
    if (z.bottom - tolerance) <= level <= (z.top + tolerance):
        return True

    # Ou o extremo do candle encostou no S/R enquanto tocava o FVG.
    probe = candle["low"] if z.side == "BULL" else candle["high"]
    return abs(probe - level) <= tolerance


def _detect_new_fvg(data: Sequence[Dict[str, Any]], i: int) -> List[FVGZone]:
    if i < 2:
        return []

    c0 = data[i]
    c1 = data[i - 1]
    c2 = data[i - 2]
    zones: List[FVGZone] = []

    # Mesmo padrão do código-fonte, mas calculado somente depois de c0 fechar.
    if c2["high"] < c0["low"] and c1["close"] > c1["open"]:
        zones.append(
            FVGZone(
                side="BULL",
                top=c0["low"],
                bottom=c2["high"],
                created_index=i,
            )
        )

    if c2["low"] > c0["high"] and c1["close"] < c1["open"]:
        zones.append(
            FVGZone(
                side="BEAR",
                top=c2["low"],
                bottom=c0["high"],
                created_index=i,
            )
        )

    return zones


def _next_time(value: Any, timeframe_seconds: int) -> Any:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        # Detecta milissegundos.
        if abs(value) > 10_000_000_000:
            return int(value + timeframe_seconds * 1000)
        return int(value + timeframe_seconds)

    if isinstance(value, str):
        raw = value.strip()
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return (dt + timedelta(seconds=timeframe_seconds)).isoformat()
        except ValueError:
            return None

    return None


def _structure_bias(
    data: Sequence[Dict[str, Any]],
    left: int,
    right: int,
) -> Dict[str, Any]:
    prev_high: Optional[float] = None
    prev_low: Optional[float] = None
    last_high_type: Optional[str] = None
    last_low_type: Optional[str] = None
    resistance: Optional[float] = None
    support: Optional[float] = None

    for i in range(len(data)):
        p = i - right
        if p < left:
            continue

        if _pivot_high(data, p, left, right):
            value = data[p]["high"]
            if prev_high is not None:
                last_high_type = "HH" if value > prev_high else "LH" if value < prev_high else "EQH"
            prev_high = value
            resistance = value

        if _pivot_low(data, p, left, right):
            value = data[p]["low"]
            if prev_low is not None:
                last_low_type = "HL" if value > prev_low else "LL" if value < prev_low else "EQL"
            prev_low = value
            support = value

    bull = int(last_high_type == "HH") + int(last_low_type == "HL")
    bear = int(last_high_type == "LH") + int(last_low_type == "LL")

    if bull > bear:
        bias = "BULL"
    elif bear > bull:
        bias = "BEAR"
    else:
        bias = "NEUTRAL"

    return {
        "bias": bias,
        "last_high_type": last_high_type,
        "last_low_type": last_low_type,
        "resistance": resistance,
        "support": support,
    }


def analyze_signal(
    candles: Sequence[Dict[str, Any]],
    timeframe_seconds: int = 60,
    higher_timeframes: Optional[Dict[str, Sequence[Dict[str, Any]]]] = None,
    config: Optional[EngineConfig] = None,
) -> Dict[str, Any]:
    """
    Analisa toda a sequência e retorna apenas a decisão da ÚLTIMA vela fechada.

    Importante:
    - A função não usa vela marcada como closed=False.
    - O sinal só nasce depois do fechamento da vela de confirmação.
    - A entrada indicada é sempre a próxima vela.
    """
    cfg = config or EngineConfig()
    data = _normalize_candles(candles)

    minimum = max(cfg.left_pivot + cfg.right_pivot + 2, 8)
    if len(data) < minimum:
        return {
            "engine": ENGINE_NAME,
            "version": ENGINE_VERSION,
            "signal": "WAIT",
            "entry": None,
            "confidence": 0,
            "reason": "Poucos candles fechados para confirmar FVG e estrutura.",
            "no_repaint": True,
        }

    mtf_context: Dict[str, Any] = {}
    if higher_timeframes:
        for name, tf_candles in higher_timeframes.items():
            tf_data = _normalize_candles(tf_candles)
            if len(tf_data) >= minimum:
                mtf_context[name] = _structure_bias(
                    tf_data,
                    cfg.left_pivot,
                    cfg.right_pivot,
                )

    zones: List[FVGZone] = []
    prev_high: Optional[float] = None
    prev_low: Optional[float] = None
    resistance: Optional[float] = None
    support: Optional[float] = None
    last_high_type: Optional[str] = None
    last_low_type: Optional[str] = None
    candidates_on_last_bar: List[Dict[str, Any]] = []

    last_i = len(data) - 1

    for i, candle in enumerate(data):
        # 1) Confirma pivô SOMENTE agora, depois das 'right_pivot' velas.
        p = i - cfg.right_pivot
        if p >= cfg.left_pivot:
            if _pivot_high(data, p, cfg.left_pivot, cfg.right_pivot):
                value = data[p]["high"]
                if prev_high is not None:
                    last_high_type = (
                        "HH" if value > prev_high else
                        "LH" if value < prev_high else
                        "EQH"
                    )
                prev_high = value
                resistance = value

            if _pivot_low(data, p, cfg.left_pivot, cfg.right_pivot):
                value = data[p]["low"]
                if prev_low is not None:
                    last_low_type = (
                        "HL" if value > prev_low else
                        "LL" if value < prev_low else
                        "EQL"
                    )
                prev_low = value
                support = value

        # 2) Remove/ignora FVG muito antigo.
        for z in zones:
            if not z.mitigated and (i - z.created_index) > cfg.max_fvg_age:
                z.mitigated = True

        # 3) Procura retorno às zonas já existentes.
        bar_candidates: List[Dict[str, Any]] = []
        avg_rng = _avg_range(data, i, cfg.range_lookback)
        tolerance = avg_rng * cfg.sr_tolerance_range_mult

        for z in zones:
            if z.mitigated or z.signaled or z.created_index >= i:
                continue

            touched = _touches_zone(candle, z)
            if not touched:
                continue

            bullish_structure = last_low_type == "HL" or last_high_type == "HH"
            bearish_structure = last_high_type == "LH" or last_low_type == "LL"

            if z.side == "BULL":
                structure_ok = bullish_structure
                sr_ok = _near_level(support, z, tolerance, candle)
                rejection_ok = _candle_rejection(candle, "BULL", cfg.min_wick_body_ratio)
                signal = "CALL"
            else:
                structure_ok = bearish_structure
                sr_ok = _near_level(resistance, z, tolerance, candle)
                rejection_ok = _candle_rejection(candle, "BEAR", cfg.min_wick_body_ratio)
                signal = "PUT"

            reasons = [f"toque em FVG {z.side.lower()} confirmado"]
            confidence = 55

            if rejection_ok:
                confidence += 15
                reasons.append("rejeição de preço confirmada")

            if sr_ok:
                confidence += 12
                reasons.append("confluência com suporte" if z.side == "BULL" else "confluência com resistência")

            if structure_ok:
                confidence += 10
                reasons.append("estrutura bullish HH/HL" if z.side == "BULL" else "estrutura bearish LH/LL")

            # Contexto MTF: ajuda, mas não trava.
            mtf_matches = 0
            mtf_opposites = 0
            wanted = z.side
            for tf_name, ctx in mtf_context.items():
                if ctx["bias"] == wanted:
                    mtf_matches += 1
                    reasons.append(f"{tf_name} favorece {wanted.lower()}")
                elif ctx["bias"] in ("BULL", "BEAR") and ctx["bias"] != wanted:
                    mtf_opposites += 1

            confidence += mtf_matches * cfg.mtf_bonus_each
            confidence -= mtf_opposites * cfg.mtf_penalty_each
            confidence = max(0, min(100, confidence))

            gate_ok = rejection_ok
            if cfg.require_structure_or_sr:
                gate_ok = gate_ok and (structure_ok or sr_ok)

            if gate_ok and confidence >= cfg.min_confidence:
                bar_candidates.append({
                    "signal": signal,
                    "confidence": confidence,
                    "reasons": reasons,
                    "zone": z,
                    "structure_ok": structure_ok,
                    "sr_ok": sr_ok,
                    "rejection_ok": rejection_ok,
                })

            # Mitigação só acontece DEPOIS da avaliação do toque,
            # para não perder a primeira oportunidade.
            if cfg.strict_mitigation:
                z.mitigated = True
            elif _fully_mitigated(candle, z):
                z.mitigated = True

        if bar_candidates:
            # Primeira ocasião válida; se duas acontecerem na mesma vela,
            # usa a de maior confiança.
            best = max(bar_candidates, key=lambda x: x["confidence"])
            best["zone"].signaled = True

            if i == last_i:
                candidates_on_last_bar.append(best)

        # 4) Só agora cria FVG da vela que ACABOU de fechar.
        # Isso impede usar a própria vela de criação como reteste.
        zones.extend(_detect_new_fvg(data, i))

    structure = {
        "last_high_type": last_high_type,
        "last_low_type": last_low_type,
        "resistance": resistance,
        "support": support,
    }

    if not candidates_on_last_bar:
        return {
            "engine": ENGINE_NAME,
            "version": ENGINE_VERSION,
            "signal": "WAIT",
            "entry": None,
            "confidence": 0,
            "closed_candle_time": data[-1]["time"],
            "structure": structure,
            "mtf_context": mtf_context,
            "reason": "Última vela fechada não formou uma oportunidade completa.",
            "no_repaint": True,
        }

    best = max(candidates_on_last_bar, key=lambda x: x["confidence"])
    z: FVGZone = best["zone"]

    return {
        "engine": ENGINE_NAME,
        "version": ENGINE_VERSION,
        "signal": best["signal"],
        "entry": "NEXT_CANDLE",
        "entry_time": _next_time(data[-1]["time"], timeframe_seconds),
        "confidence": best["confidence"],
        "closed_candle_time": data[-1]["time"],
        "reasons": best["reasons"],
        "fvg": {
            "side": z.side,
            "top": z.top,
            "bottom": z.bottom,
            "created_index": z.created_index,
        },
        "structure": structure,
        "mtf_context": mtf_context,
        "no_repaint": True,
    }


def app_signal_payload(
    candles: Sequence[Dict[str, Any]],
    symbol: str,
    timeframe: str = "M1",
    timeframe_seconds: int = 60,
    higher_timeframes: Optional[Dict[str, Sequence[Dict[str, Any]]]] = None,
    config: Optional[EngineConfig] = None,
) -> Dict[str, Any]:
    """
    Wrapper pronto para o backend do MEGA IA.
    Retorna um payload fácil de conectar ao /signal, radar ou Telegram.
    """
    result = analyze_signal(
        candles=candles,
        timeframe_seconds=timeframe_seconds,
        higher_timeframes=higher_timeframes,
        config=config,
    )

    result["symbol"] = symbol
    result["timeframe"] = timeframe
    result["strategy"] = "SMC FVG + HL REVERSALS"
    result["expiry_candles"] = 1
    return result


__all__ = [
    "EngineConfig",
    "analyze_signal",
    "app_signal_payload",
    "ENGINE_NAME",
    "ENGINE_VERSION",
]
