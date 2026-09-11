import os
import asyncio
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

APP_NAME = "Ismael Trade"
APP_VERSION = "9.0.3"
KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
BASE_URL = "https://api.twelvedata.com/time_series"
SP_TZ = ZoneInfo("America/Sao_Paulo")

# Licença do aplicativo
LICENSE_EXPIRES = os.getenv("ISMAEL_TRADE_LICENSE_EXPIRES", "2026-12-31").strip()
LICENSE_WHATSAPP_1 = "55 84 99841-1282"
LICENSE_WHATSAPP_2 = "55 84 99449-9442"
LICENSE_INSTAGRAM = "@Ismaelartur26"

def license_status() -> Dict[str, Any]:
    try:
        expires = datetime.strptime(LICENSE_EXPIRES, "%Y-%m-%d").replace(tzinfo=SP_TZ)
    except ValueError:
        expires = datetime(2026, 12, 31, 23, 59, 59, tzinfo=SP_TZ)
    expires_end = expires.replace(hour=23, minute=59, second=59)
    current = now_sp()
    active = current <= expires_end
    return {
        "active": active,
        "expires": expires_end.strftime("%d/%m/%Y"),
        "expires_iso": expires_end.isoformat(),
        "whatsapp_1": LICENSE_WHATSAPP_1,
        "whatsapp_2": LICENSE_WHATSAPP_2,
        "instagram": LICENSE_INSTAGRAM,
    }

ALLOWED_INTERVALS = {"1min": 1, "5min": 5, "15min": 15, "30min": 30}
SYMBOLS = {
    "EUR/USD": "EUR/USD",
    "GBP/USD": "GBP/USD",
    "USD/JPY": "USD/JPY",
    "AUD/USD": "AUD/USD",
    "USD/CAD": "USD/CAD",
    "USD/CHF": "USD/CHF",
    "NZD/USD": "NZD/USD",
    "EUR/JPY": "EUR/JPY",
    "GBP/JPY": "GBP/JPY",
    "EUR/GBP": "EUR/GBP",
    "BTC/USD": "BTC/USD",
    "ETH/USD": "ETH/USD",
}

app = FastAPI(title=APP_NAME, version=APP_VERSION)

# Cache local para evitar chamadas repetidas à Twelve Data.
# Isso reduz bastante o consumo de créditos e evita chamadas simultâneas.
CACHE_TTL_SECONDS = 60.0
RATE_LIMIT_COOLDOWN_SECONDS = 20.0
CANDLE_CACHE: Dict[tuple, tuple] = {}
CANDLE_LOCKS: Dict[tuple, asyncio.Lock] = {}
RATE_LIMIT_UNTIL = 0.0
RADAR_CACHE: Dict[tuple, tuple] = {}
RADAR_SYMBOLS = [
    "EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD", "USD/CHF",
]
RADAR_TTL_SECONDS = 120.0


def now_sp() -> datetime:
    return datetime.now(SP_TZ)


def parse_time(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=SP_TZ)
        return dt.astimezone(SP_TZ)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(text, fmt).replace(tzinfo=SP_TZ)
            except ValueError:
                pass
    raise ValueError(f"Timestamp inválido: {value}")


async def get_candles(symbol: str, interval: str, outputsize: int = 100) -> List[Dict[str, Any]]:
    global RATE_LIMIT_UNTIL

    if not KEY:
        raise HTTPException(status_code=500, detail="TWELVE_DATA_API_KEY não configurada no Render.")

    if symbol not in SYMBOLS:
        raise HTTPException(status_code=400, detail="Ativo inválido.")

    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(status_code=400, detail="Timeframe inválido.")

    key = (symbol, interval)
    now = time.monotonic()

    cached = CANDLE_CACHE.get(key)
    if cached and (now - cached[0]) < CACHE_TTL_SECONDS:
        return cached[1]

    if now < RATE_LIMIT_UNTIL:
        if cached:
            return cached[1]
        remaining = max(1, int(RATE_LIMIT_UNTIL - now))
        raise HTTPException(
            status_code=429,
            detail={
                "error": "RATE_LIMIT",
                "message": f"Limite da Twelve Data atingido. Aguarde {remaining}s e tente novamente.",
                "retry_after": remaining,
            },
        )

    lock = CANDLE_LOCKS.setdefault(key, asyncio.Lock())
    async with lock:
        # Outra requisição pode ter preenchido o cache enquanto aguardávamos o lock.
        now = time.monotonic()
        cached = CANDLE_CACHE.get(key)
        if cached and (now - cached[0]) < CACHE_TTL_SECONDS:
            return cached[1]

        if now < RATE_LIMIT_UNTIL:
            remaining = max(1, int(RATE_LIMIT_UNTIL - now))
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "RATE_LIMIT",
                    "message": f"Limite da Twelve Data atingido. Aguarde {remaining}s e tente novamente.",
                    "retry_after": remaining,
                },
            )

        params = {
            "symbol": SYMBOLS[symbol],
            "interval": interval,
            "outputsize": min(max(int(outputsize), 20), 120),
            "timezone": "America/Sao_Paulo",
            "order": "ASC",
        }

        headers = {"Authorization": f"apikey {KEY}"}

        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                response = await client.get(BASE_URL, params=params, headers=headers)
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail="Não foi possível conectar à Twelve Data.") from exc

        if response.status_code == 429:
            retry_header = response.headers.get("Retry-After", "")
            try:
                retry_after = max(5, min(int(float(retry_header)), 120)) if retry_header else RATE_LIMIT_COOLDOWN_SECONDS
            except ValueError:
                retry_after = RATE_LIMIT_COOLDOWN_SECONDS
            RATE_LIMIT_UNTIL = time.monotonic() + retry_after
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "RATE_LIMIT",
                    "message": f"A Twelve Data informou limite de requisições. Aguarde {int(retry_after)}s.",
                    "retry_after": int(retry_after),
                },
            )

        if response.status_code >= 400:
            raise HTTPException(
                status_code=502,
                detail=f"Twelve Data retornou HTTP {response.status_code}.",
            )

        try:
            data = response.json()
        except ValueError as exc:
            raise HTTPException(status_code=502, detail="Resposta inválida da Twelve Data.") from exc

        if data.get("status") == "error" or "values" not in data:
            message = data.get("message", "Resposta inválida da Twelve Data.")
            raise HTTPException(status_code=502, detail=message)

        candles: List[Dict[str, Any]] = []
        for item in data["values"]:
            try:
                candles.append(
                    {
                        "datetime": item["datetime"],
                        "open": float(item["open"]),
                        "high": float(item["high"]),
                        "low": float(item["low"]),
                        "close": float(item["close"]),
                        "volume": float(item.get("volume", 0) or 0),
                    }
                )
            except (KeyError, TypeError, ValueError):
                continue

        candles.sort(key=lambda x: parse_time(x["datetime"]))
        if len(candles) < 20:
            raise HTTPException(status_code=502, detail="Poucas velas retornadas pela Twelve Data.")

        CANDLE_CACHE[key] = (time.monotonic(), candles)
        return candles


def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1.0)
    result = [values[0]]
    for value in values[1:]:
        result.append((value * alpha) + (result[-1] * (1.0 - alpha)))
    return result


def rsi(values: List[float], period: int = 14) -> List[float]:
    if len(values) < period + 1:
        return [50.0] * len(values)

    gains = [0.0]
    losses = [0.0]
    for i in range(1, len(values)):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))

    result = [50.0] * len(values)
    avg_gain = sum(gains[1 : period + 1]) / period
    avg_loss = sum(losses[1 : period + 1]) / period

    def calc(g: float, l: float) -> float:
        if l == 0:
            return 100.0 if g > 0 else 50.0
        rs = g / l
        return 100.0 - (100.0 / (1.0 + rs))

    result[period] = calc(avg_gain, avg_loss)

    for i in range(period + 1, len(values)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period
        result[i] = calc(avg_gain, avg_loss)

    return result


def true_ranges(candles: List[Dict[str, Any]]) -> List[float]:
    tr: List[float] = []
    for i, candle in enumerate(candles):
        if i == 0:
            tr.append(candle["high"] - candle["low"])
            continue
        previous_close = candles[i - 1]["close"]
        tr.append(
            max(
                candle["high"] - candle["low"],
                abs(candle["high"] - previous_close),
                abs(candle["low"] - previous_close),
            )
        )
    return tr


def adx(candles: List[Dict[str, Any]], period: int) -> List[float]:
    n = len(candles)
    if n < period + 2:
        return [0.0] * n

    tr = true_ranges(candles)
    plus_dm = [0.0] * n
    minus_dm = [0.0] * n

    for i in range(1, n):
        up_move = candles[i]["high"] - candles[i - 1]["high"]
        down_move = candles[i - 1]["low"] - candles[i]["low"]
        if up_move > down_move and up_move > 0:
            plus_dm[i] = up_move
        if down_move > up_move and down_move > 0:
            minus_dm[i] = down_move

    result = [0.0] * n
    atr = sum(tr[1 : period + 1]) / period
    plus = sum(plus_dm[1 : period + 1]) / period
    minus = sum(minus_dm[1 : period + 1]) / period

    dx_values: List[float] = [0.0] * n

    for i in range(period, n):
        if i > period:
            atr = ((atr * (period - 1)) + tr[i]) / period
            plus = ((plus * (period - 1)) + plus_dm[i]) / period
            minus = ((minus * (period - 1)) + minus_dm[i]) / period

        plus_di = 100.0 * plus / atr if atr else 0.0
        minus_di = 100.0 * minus / atr if atr else 0.0
        denom = plus_di + minus_di
        dx_values[i] = 100.0 * abs(plus_di - minus_di) / denom if denom else 0.0

    start = period
    if n > start + period:
        initial = dx_values[start : start + period]
        adx_value = sum(initial) / len(initial)
        result[start + period - 1] = adx_value

        for i in range(start + period, n):
            adx_value = ((adx_value * (period - 1)) + dx_values[i]) / period
            result[i] = adx_value

    return result


def analyze(candles: List[Dict[str, Any]], strategy: str = "rsi") -> Dict[str, Any]:
    """Analisa somente candles fechados para reduzir repintura.

    SNIPER 01: leitura de price action/confluência por candles.
    SNIPER X: confluência RSI 9 + RSI 14.
    """
    if len(candles) < 30:
        raise HTTPException(status_code=422, detail="Dados insuficientes para análise.")

    closed = candles[:-1]
    reference = closed[-1]
    previous = closed[-2]
    next_candle = candles[-1]
    strategy = strategy.lower().strip()

    def body(c):
        return abs(c["close"] - c["open"])

    def rng(c):
        return max(c["high"] - c["low"], 1e-12)

    if strategy == "old_sniper":
        # SNIPER 01 — Price Action:
        # CALL: engolfo de alta, rejeição de fundo, rompimento da máxima,
        # vela de força compradora e confirmação pela estrutura anterior.
        # PUT: condições equivalentes para baixa.
        prev_body = body(previous)
        ref_body = body(reference)
        ref_range = rng(reference)
        ref_upper = reference["high"] - max(reference["open"], reference["close"])
        ref_lower = min(reference["open"], reference["close"]) - reference["low"]

        bullish = reference["close"] > reference["open"]
        bearish = reference["close"] < reference["open"]
        prev_bearish = previous["close"] < previous["open"]
        prev_bullish = previous["close"] > previous["open"]

        bull_engulf = (bullish and prev_bearish and
                       reference["open"] <= previous["close"] and
                       reference["close"] >= previous["open"])
        bear_engulf = (bearish and prev_bullish and
                       reference["open"] >= previous["close"] and
                       reference["close"] <= previous["open"])

        # Rejeição: pavio contrário relativamente grande.
        bull_rejection = bullish and ref_lower >= max(ref_body * 1.2, ref_range * 0.30)
        bear_rejection = bearish and ref_upper >= max(ref_body * 1.2, ref_range * 0.30)

        bull_break = reference["high"] > previous["high"] and reference["close"] > previous["high"]
        bear_break = reference["low"] < previous["low"] and reference["close"] < previous["low"]

        bull_strength = bullish and ref_body >= ref_range * 0.60 and ref_upper <= ref_range * 0.20
        bear_strength = bearish and ref_body >= ref_range * 0.60 and ref_lower <= ref_range * 0.20

        # Estrutura: fechamento na metade correspondente e direção coerente.
        bull_structure = bullish and reference["close"] >= previous["close"]
        bear_structure = bearish and reference["close"] <= previous["close"]

        bull_flags = [bull_engulf, bull_rejection, bull_break, bull_strength, bull_structure]
        bear_flags = [bear_engulf, bear_rejection, bear_break, bear_strength, bear_structure]
        bull_score = sum(bull_flags)
        bear_score = sum(bear_flags)

        if bull_score >= 3 and bull_score > bear_score:
            signal = "CALL"
            confidence = min(95, 55 + bull_score * 7)
        elif bear_score >= 3 and bear_score > bull_score:
            signal = "PUT"
            confidence = min(95, 55 + bear_score * 7)
        else:
            signal = "NEUTRO"
            confidence = 50

        return {
            "signal": signal,
            "confidence": int(confidence),
            "reference_candle": reference["datetime"],
            "next_candle": next_candle["datetime"],
            "strategy": "SNIPER 01",
            "strategy_code": "old_sniper",
            "bull_score": bull_score,
            "bear_score": bear_score,
            "bullish_engulfing": bull_engulf,
            "bearish_engulfing": bear_engulf,
            "bullish_rejection": bull_rejection,
            "bearish_rejection": bear_rejection,
            "bullish_breakout": bull_break,
            "bearish_breakout": bear_break,
            "bullish_strength": bull_strength,
            "bearish_strength": bear_strength,
            "structure_confirmation": bull_structure if signal == "CALL" else bear_structure if signal == "PUT" else False,
            "non_repaint_reference": True,
            "ok": True,
        }

    if strategy == "sniper_02":
        # SNIPER 02 — rompimento + reteste + rejeição + confirmação.
        # Não entra no primeiro rompimento.
        # Usa apenas candles fechados para reduzir repintura.
        if len(closed) < 25:
            raise HTTPException(status_code=422, detail="Dados insuficientes para SNIPER 02.")

        # Níveis simples de suporte/resistência por swing, usando histórico anterior
        # ao movimento recente. Isso evita usar o próprio candle de rompimento como nível.
        lookback = closed[-22:-4]
        resistance = max(c["high"] for c in lookback)
        support = min(c["low"] for c in lookback)
        recent = closed[-4:]

        def bullish_pin(c):
            b = body(c); r = rng(c)
            lower = min(c["open"], c["close"]) - c["low"]
            upper = c["high"] - max(c["open"], c["close"])
            return c["close"] > c["open"] and lower >= max(b * 1.5, r * 0.40) and upper <= r * 0.25

        def bearish_pin(c):
            b = body(c); r = rng(c)
            upper = c["high"] - max(c["open"], c["close"])
            lower = min(c["open"], c["close"]) - c["low"]
            return c["close"] < c["open"] and upper >= max(b * 1.5, r * 0.40) and lower <= r * 0.25

        def hammer(c):
            b = body(c); r = rng(c)
            lower = min(c["open"], c["close"]) - c["low"]
            upper = c["high"] - max(c["open"], c["close"])
            return lower >= max(b * 2.0, r * 0.45) and upper <= r * 0.20

        def shooting_star(c):
            b = body(c); r = rng(c)
            upper = c["high"] - max(c["open"], c["close"])
            lower = min(c["open"], c["close"]) - c["low"]
            return upper >= max(b * 2.0, r * 0.45) and lower <= r * 0.20

        def bull_engulf(a, b):
            return (b["close"] > b["open"] and a["close"] < a["open"] and
                    b["open"] <= a["close"] and b["close"] >= a["open"])

        def bear_engulf(a, b):
            return (b["close"] < b["open"] and a["close"] > a["open"] and
                    b["open"] >= a["close"] and b["close"] <= a["open"])

        # Procura uma sequência fechada: rompimento, retorno ao nível, rejeição e confirmação.
        call_setup = False
        put_setup = False
        call_pattern = ""
        put_pattern = ""
        for j in range(max(2, len(closed) - 5), len(closed) - 1):
            breakout = closed[j]
            retest = closed[j + 1]
            confirmation = closed[j + 2] if j + 2 < len(closed) else None
            if confirmation is None:
                continue

            # CALL: fechamento acima da resistência, depois reteste sem operar no rompimento.
            if breakout["close"] > resistance:
                touched = retest["low"] <= resistance * 1.0015
                rejection = touched and (hammer(retest) or bullish_pin(retest) or
                                         bull_engulf(closed[j], retest))
                confirmed = (confirmation["close"] > confirmation["open"] and
                             confirmation["close"] > retest["high"])
                if rejection and confirmed:
                    call_setup = True
                    call_pattern = ("Martelo" if hammer(retest) else
                                    "Pin Bar de alta" if bullish_pin(retest) else
                                    "Engolfo de alta")

            # PUT: fechamento abaixo do suporte, depois reteste sem operar no rompimento.
            if breakout["close"] < support:
                touched = retest["high"] >= support * 0.9985
                rejection = touched and (shooting_star(retest) or bearish_pin(retest) or
                                         bear_engulf(closed[j], retest))
                confirmed = (confirmation["close"] < confirmation["open"] and
                             confirmation["close"] < retest["low"])
                if rejection and confirmed:
                    put_setup = True
                    put_pattern = ("Shooting Star" if shooting_star(retest) else
                                   "Pin Bar de baixa" if bearish_pin(retest) else
                                   "Engolfo de baixa")

        if call_setup and not put_setup:
            signal = "CALL"; confidence = 88
        elif put_setup and not call_setup:
            signal = "PUT"; confidence = 88
        else:
            signal = "NEUTRO"; confidence = 50

        return {
            "signal": signal,
            "confidence": confidence,
            "reference_candle": reference["datetime"],
            "next_candle": next_candle["datetime"],
            "strategy": "SNIPER 02",
            "strategy_code": "sniper_02",
            "resistance": resistance,
            "support": support,
            "call_setup": call_setup,
            "put_setup": put_setup,
            "call_pattern": call_pattern,
            "put_pattern": put_pattern,
            "no_first_breakout": True,
            "non_repaint_reference": True,
            "ok": True,
        }

    if strategy == "sniper_03":
        # SNIPER 03 — Tendência + Retração + LTA/LTB + zonas fortes.
        if len(closed) < 45:
            raise HTTPException(status_code=422, detail="Dados insuficientes para SNIPER 03.")
        closes = [c["close"] for c in closed]; highs=[c["high"] for c in closed]; lows=[c["low"] for c in closed]
        ema20=ema(closes,20); ema50=ema(closes,50)
        trv=true_ranges(closed); atr=sum(trv[-14:])/14
        tol=max(atr*0.35, abs(reference["close"])*0.0005)
        def pl(i): return i>=2 and i+2<len(closed) and lows[i]<=lows[i-1] and lows[i]<=lows[i-2] and lows[i]<lows[i+1] and lows[i]<lows[i+2]
        def ph(i): return i>=2 and i+2<len(closed) and highs[i]>=highs[i-1] and highs[i]>=highs[i-2] and highs[i]>highs[i+1] and highs[i]>highs[i+2]
        lp=[i for i in range(max(2,len(closed)-35),len(closed)-2) if pl(i)]
        hp=[i for i in range(max(2,len(closed)-35),len(closed)-2) if ph(i)]
        lta_level=ltb_level=None; lta_valid=ltb_valid=False
        if len(lp)>=2:
            a,b=lp[-2],lp[-1]
            if lows[b]>lows[a]:
                slope=(lows[b]-lows[a])/(b-a); lta_level=lows[b]+slope*(len(closed)-1-b); lta_valid=slope>0 and lta_level<=reference["high"]+tol
        if len(hp)>=2:
            a,b=hp[-2],hp[-1]
            if highs[b]<highs[a]:
                slope=(highs[b]-highs[a])/(b-a); ltb_level=highs[b]+slope*(len(closed)-1-b); ltb_valid=slope<0 and ltb_level>=reference["low"]-tol
        support_level=min((lows[i] for i in lp[-5:]),default=None)
        resistance_level=max((highs[i] for i in hp[-5:]),default=None)
        def zone_score(level,side):
            if level is None:return 0
            touches=reject=0
            for c in closed[max(0,len(closed)-40):-1]:
                if c["low"]<=level+tol and c["high"]>=level-tol:
                    touches+=1
                    if side=="support" and (c["close"]>c["open"] or min(c["open"],c["close"])-c["low"]>body(c)): reject+=1
                    if side=="resistance" and (c["close"]<c["open"] or c["high"]-max(c["open"],c["close"])>body(c)): reject+=1
            return touches+min(reject,3)
        ss=zone_score(support_level,"support"); rs=zone_score(resistance_level,"resistance")
        strong_support=support_level is not None and ss>=4; strong_resistance=resistance_level is not None and rs>=4
        rb=body(reference); rr=rng(reference); lw=min(reference["open"],reference["close"])-reference["low"]; uw=reference["high"]-max(reference["open"],reference["close"])
        bull_rej=reference["close"]>reference["open"] and lw>=max(rb*1.2,rr*.30); bear_rej=reference["close"]<reference["open"] and uw>=max(rb*1.2,rr*.30)
        near_lta=lta_level is not None and abs(reference["low"]-lta_level)<=tol; near_ltb=ltb_level is not None and abs(reference["high"]-ltb_level)<=tol
        near_sup=support_level is not None and abs(reference["low"]-support_level)<=tol; near_res=resistance_level is not None and abs(reference["high"]-resistance_level)<=tol
        prior=closed[-6:-1]
        up=ema20[-1]>ema50[-1] and ema20[-1]>ema20[-4] and reference["close"]>ema50[-1]
        down=ema20[-1]<ema50[-1] and ema20[-1]<ema20[-4] and reference["close"]<ema50[-1]
        pull_call=any(c["close"]<c["open"] for c in prior[-3:]) and (near_lta or near_sup or abs(reference["low"]-ema20[-1])<=tol)
        pull_put=any(c["close"]>c["open"] for c in prior[-3:]) and (near_ltb or near_res or abs(reference["high"]-ema20[-1])<=tol)
        conf_call=reference["close"]>reference["open"] and reference["close"]>=previous["high"]
        conf_put=reference["close"]<reference["open"] and reference["close"]<=previous["low"]
        call_score=(2 if up else 0)+(2 if lta_valid else 0)+(2 if strong_support else 0)+(2 if pull_call else 0)+(1 if bull_rej else 0)+(2 if conf_call else 0)+(1 if near_lta else 0)+(1 if near_sup else 0)
        put_score=(2 if down else 0)+(2 if ltb_valid else 0)+(2 if strong_resistance else 0)+(2 if pull_put else 0)+(1 if bear_rej else 0)+(2 if conf_put else 0)+(1 if near_ltb else 0)+(1 if near_res else 0)
        if call_score>=8 and call_score>put_score: signal="CALL"; confidence=min(95,55+call_score*4)
        elif put_score>=8 and put_score>call_score: signal="PUT"; confidence=min(95,55+put_score*4)
        else: signal="NEUTRO"; confidence=50
        return {"signal":signal,"confidence":confidence,"reference_candle":reference["datetime"],"next_candle":next_candle["datetime"],"strategy":"SNIPER 03","strategy_code":"sniper_03","trend":"ALTA" if up else "BAIXA" if down else "NEUTRA","lta":round(lta_level,8) if lta_level is not None else None,"ltb":round(ltb_level,8) if ltb_level is not None else None,"support":round(support_level,8) if support_level is not None else None,"resistance":round(resistance_level,8) if resistance_level is not None else None,"strong_support":strong_support,"strong_resistance":strong_resistance,"near_lta":near_lta,"near_ltb":near_ltb,"near_support":near_sup,"near_resistance":near_res,"pullback_call":pull_call,"pullback_put":pull_put,"bullish_rejection":bull_rej,"bearish_rejection":bear_rej,"call_score":call_score,"put_score":put_score,"non_repaint_reference":True,"ok":True}

    # SNIPER X — apenas RSI 9 + RSI 14 em confluência.
    closes = [float(c["close"]) for c in closed]
    rsi9_values = rsi(closes, 9)
    rsi14_values = rsi(closes, 14)
    i = len(closed) - 1
    rsi9_value = rsi9_values[i]
    rsi14_value = rsi14_values[i]

    if rsi9_value > 50.0 and rsi14_value > 50.0:
        signal = "CALL"
    elif rsi9_value < 50.0 and rsi14_value < 50.0:
        signal = "PUT"
    else:
        signal = "NEUTRO"

    confidence = 50 if signal == "NEUTRO" else int(min(
        95, 55 + min(abs(rsi9_value - 50), abs(rsi14_value - 50)) * 1.8
    ))
    return {
        "signal": signal,
        "confidence": confidence,
        "reference_candle": reference["datetime"],
        "next_candle": next_candle["datetime"],
        "rsi9": round(rsi9_value, 2),
        "rsi14": round(rsi14_value, 2),
        "rsi_confluence": signal != "NEUTRO",
        "strategy": "SNIPER X",
        "strategy_code": "rsi",
        "non_repaint_reference": True,
        "ok": True,
    }



def detect_market_trend(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Detecta a tendência usando somente candles fechados.
    EMA 9/20/50 + inclinação da EMA 20.
    """
    closed = candles[:-1]
    if len(closed) < 55:
        return {"trend": "NEUTRA", "trend_text": "Tendência neutra", "trend_score": 0}

    closes = [float(c["close"]) for c in closed]
    e9 = ema(closes, 9)
    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    slope = e20[-1] - e20[-6]
    price = closes[-1]
    atr_values = true_ranges(closed)
    atr = sum(atr_values[-14:]) / 14 if len(atr_values) >= 14 else max(abs(price) * 0.0001, 1e-12)
    threshold = max(atr * 0.05, abs(price) * 0.00001)

    bull_score = int(e9[-1] > e20[-1]) + int(e20[-1] > e50[-1]) + int(price > e50[-1]) + int(slope > threshold)
    bear_score = int(e9[-1] < e20[-1]) + int(e20[-1] < e50[-1]) + int(price < e50[-1]) + int(slope < -threshold)

    if bull_score >= 3 and bull_score > bear_score:
        return {"trend": "ALTA", "trend_text": "Tendência do gráfico é de alta", "trend_score": bull_score}
    if bear_score >= 3 and bear_score > bull_score:
        return {"trend": "BAIXA", "trend_text": "Tendência do gráfico é de baixa", "trend_score": bear_score}
    return {"trend": "NEUTRA", "trend_text": "Tendência neutra", "trend_score": max(bull_score, bear_score)}


def radar_score(analysis: Dict[str, Any], strategy: str) -> Dict[str, Any]:
    """Converte a análise da estratégia em proximidade de CALL/PUT para o radar."""
    strategy = strategy.lower().strip()
    if strategy == "rsi":
        r9 = float(analysis.get("rsi9", 50.0))
        r14 = float(analysis.get("rsi14", 50.0))
        bull = max(0.0, min(100.0, 50.0 + ((r9 - 50.0) + (r14 - 50.0)) * 1.5))
        bear = max(0.0, min(100.0, 50.0 + ((50.0 - r9) + (50.0 - r14)) * 1.5))
    elif strategy == "old_sniper":
        bull = min(100.0, 50.0 + float(analysis.get("bull_score", 0)) * 10.0)
        bear = min(100.0, 50.0 + float(analysis.get("bear_score", 0)) * 10.0)
    elif strategy == "sniper_02":
        bull = 90.0 if analysis.get("call_setup") else 50.0
        bear = 90.0 if analysis.get("put_setup") else 50.0
    else:
        bull_score = float(analysis.get("call_score", 0))
        bear_score = float(analysis.get("put_score", 0))
        bull = min(99.0, 50.0 + bull_score * 5.0)
        bear = min(99.0, 50.0 + bear_score * 5.0)

    if bull >= bear and bull >= 58.0:
        direction = "CALL"
        proximity = int(round(bull))
    elif bear > bull and bear >= 58.0:
        direction = "PUT"
        proximity = int(round(bear))
    else:
        direction = "NEUTRO"
        proximity = int(round(max(bull, bear)))
    return {"direction": direction, "proximity": max(0, min(99, proximity)),
            "call_proximity": int(round(bull)), "put_proximity": int(round(bear))}


@app.get("/radar")
async def radar(
    interval: str = "1min",
    strategy: str = "rsi",
) -> Dict[str, Any]:
    require_active_license()
    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(status_code=400, detail="Timeframe inválido.")
    if strategy not in {"rsi", "old_sniper", "sniper_02", "sniper_03"}:
        raise HTTPException(status_code=400, detail="Estratégia inválida.")

    cache_key = (interval, strategy)
    cached = RADAR_CACHE.get(cache_key)
    now_mono = time.monotonic()
    if cached and (now_mono - cached[0]) < RADAR_TTL_SECONDS:
        return cached[1]

    # Para evitar estourar rapidamente o limite da Twelve Data, o radar
    # usa primeiro dados já em cache e completa apenas o necessário.
    results: List[Dict[str, Any]] = []
    errors = 0
    for symbol_name in RADAR_SYMBOLS:
        try:
            values = await get_candles(symbol_name, interval, 100)
            analysis = analyze(values, strategy)
            proximity = radar_score(analysis, strategy)
            results.append({
                "symbol": symbol_name,
                "signal": analysis["signal"],
                "direction": proximity["direction"],
                "proximity": proximity["proximity"],
                "call_proximity": proximity["call_proximity"],
                "put_proximity": proximity["put_proximity"],
                "confidence": int(analysis.get("confidence", 50)),
                "reference_candle": analysis.get("reference_candle"),
            })
        except HTTPException:
            errors += 1
            continue
        except Exception:
            errors += 1
            continue

    results.sort(key=lambda x: (x["direction"] == "NEUTRO", -x["proximity"]))
    payload = {
        "ok": True,
        "interval": interval,
        "strategy": strategy,
        "symbols_checked": len(results),
        "errors": errors,
        "results": results,
        "warning": "Radar probabilístico: proximidade não é garantia de sinal ou WIN.",
    }
    RADAR_CACHE[cache_key] = (time.monotonic(), payload)
    return payload



def candle_is_closed(candle_time: datetime, interval: str) -> bool:
    minutes = ALLOWED_INTERVALS[interval]
    return now_sp() >= candle_time + timedelta(minutes=minutes)


@app.get("/", response_class=HTMLResponse)
async def home() -> HTMLResponse:
    html = r"""<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ismael Trade</title>
<style>
*{box-sizing:border-box}
body{margin:0;font-family:Arial,sans-serif;background:#0b1020;color:#f5f7ff}
.container{max-width:760px;margin:auto;padding:18px}
h1{margin:0 0 4px;font-size:28px}
.sub{color:#aeb7cc;margin-bottom:16px}
.card{background:#151c30;border:1px solid #29324b;border-radius:16px;padding:16px;margin:12px 0}
.row{display:flex;gap:10px;flex-wrap:wrap}
label{display:block;color:#aeb7cc;font-size:13px;margin-bottom:6px}
select,button{width:100%;padding:12px;border-radius:10px;border:1px solid #34405e;background:#0f1526;color:#fff}
.field{flex:1;min-width:180px}
button{cursor:pointer;font-weight:bold}
.signal{text-align:center;padding:22px;border-radius:14px;font-size:38px;font-weight:800;margin-top:12px}
.call{background:#103c2b;color:#52f09d}
.put{background:#481d28;color:#ff718b}
.neutral{background:#2b3040;color:#d9deeb}
.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}
.stat{background:#0f1526;border-radius:12px;padding:14px;text-align:center}
.stat b{display:block;font-size:25px;margin-top:4px}
.params{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.param{background:#0f1526;border-radius:10px;padding:10px}
.small{font-size:12px;color:#aeb7cc}
.value{font-weight:bold}
.hidden{display:none}
.good{color:#52f09d}.bad{color:#ff718b}
.footer{font-size:12px;color:#7f8aa5;line-height:1.5}
.clock{font-size:30px;font-weight:800;letter-spacing:1px}
.clockLabel{font-size:11px;color:#8f9ab2;text-transform:uppercase}
.onlineBtn{border-radius:999px;padding:10px 16px;font-weight:800}.onlineBtn.online{background:#0d3b2a;border-color:#2ee88a;color:#52f09d}.onlineBtn.offline{background:#431b25;border-color:#ff718b;color:#ff718b}
.badge{display:inline-block;padding:7px 11px;border-radius:999px;background:#0f1526;border:1px solid #34405e;font-size:12px;font-weight:800}
.sniper{border:1px solid #394765;background:linear-gradient(135deg,#151c30,#10172a)}
.aiCard{border:1px solid #6b55a3;background:linear-gradient(135deg,#1b1733,#10172a)}
.aiMain{font-size:24px;font-weight:800;margin:10px 0}.aiMeta{font-size:13px;color:#aeb7cc}.aiReason{font-size:13px;color:#d9deeb;line-height:1.5;margin-top:8px}
.sniperTitle{font-size:20px;font-weight:800;margin:4px 0}
.countdown{font-size:25px;font-weight:800}
.entry{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:12px}
.entryBox{background:#0f1526;border-radius:12px;padding:12px}
.radarRow{display:grid;grid-template-columns:110px 1fr 48px;gap:10px;align-items:center;background:#0f1526;border-radius:12px;padding:10px;margin:7px 0}.radarMeter{height:9px;background:#252c40;border-radius:999px;overflow:hidden}.radarFill{height:100%;border-radius:999px}.radarFill.call{background:#2ee88a}.radarFill.put{background:#ff718b}.radarFill.neutral{background:#8892a8}
@media(max-width:520px){.entry{grid-template-columns:1fr}}
@media(max-width:520px){.grid{grid-template-columns:1fr 1fr}.params{grid-template-columns:1fr}}
</style>
</head>
<body>
<div class="container">
<h1>Ismael Trade</h1>
<div class="sub">Analisador de sinais M1 • dados Twelve Data</div>

<div class="card" style="display:flex;justify-content:space-between;align-items:center;gap:12px">
<div><div class="clockLabel">HORÁRIO DE BRASÍLIA</div><div id="clock" class="clock">--:--:--</div><div id="dateBr" class="small">--/--/----</div></div>
<div class="badge">● MERCADO • MONITORANDO</div>
</div>

<div class="card" id="licenseCard">
<div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap">
<div><div class="small">LICENÇA ISMAEL TRADE</div><div id="licenseStatus" class="value">VERIFICANDO...</div></div>
<div><div class="small">VALIDADE</div><div id="licenseExpires" class="value">--</div></div>
</div>
<div class="small" style="margin-top:10px">Renovação: WhatsApp <b>55 84 99841-1282</b> / <b>55 84 99449-9442</b> • Instagram <b>@Ismaelartur26</b></div>
</div>

<div class="card">
<div class="row">
<div class="field">
<label>ATIVO</label>
<select id="symbol">
<option>EUR/USD</option><option>GBP/USD</option><option>USD/JPY</option>
<option>AUD/USD</option><option>USD/CAD</option><option>USD/CHF</option>
<option>NZD/USD</option><option>EUR/JPY</option><option>GBP/JPY</option>
<option>EUR/GBP</option><option>BTC/USD</option><option>ETH/USD</option>
</select>
</div>
<div class="field">
<label>TEMPO</label>
<select id="interval">
<option value="1min">M1</option><option value="5min">M5</option>
<option value="15min">M15</option><option value="30min">M30</option>
</select>
</div>
</div>
<button id="refresh" style="margin-top:10px">ATUALIZAR SINAL</button>
</div>

<div class="card sniper">
<div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap">
<div><div class="sniperTitle">SNIPER X</div></div>
<button id="rsiToggle" class="onlineBtn online" style="width:auto;margin:0">● ONLINE</button>
</div></div>
<div class="card sniper">
<div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap">
<div><div class="sniperTitle">SNIPER 01</div></div>
<button id="oldToggle" class="onlineBtn offline" style="width:auto;margin:0">● OFFLINE</button>
</div></div>
<div class="card sniper">
<div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap">
<div><div class="sniperTitle">SNIPER 02</div></div>
<button id="sniper02Toggle" class="onlineBtn offline" style="width:auto;margin:0">● OFFLINE</button>
</div></div>
<div class="card sniper">
<div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap">
<div><div class="sniperTitle">SNIPER 03</div></div>
<button id="sniper03Toggle" class="onlineBtn offline" style="width:auto;margin:0">● OFFLINE</button>
</div></div>

<div class="card aiCard">
<div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap">
<div><div class="sniperTitle">🤖 INTELIGÊNCIA ARTIFICIAL</div><div class="small">Trade Sniper AI • confluência de mercado</div></div>
<button id="aiToggle" class="onlineBtn online" style="width:auto;margin:0">● ONLINE</button>
</div>
<div id="aiStatus" class="small" style="margin-top:10px">IA aguardando análise...</div>
<div id="aiMain" class="aiMain">AGUARDANDO</div>
<div id="aiMeta" class="aiMeta">Selecione ativo e tempo para analisar.</div>
<div id="aiReason" class="aiReason"></div>
</div>

<div class="card">
<div class="small">SINAL</div>
<div id="signal" class="signal neutral">AGUARDANDO</div>
<div id="signalError" class="small" style="display:none;margin-top:8px"></div>
<div class="entry">
<div class="entryBox"><div class="small">HORÁRIO DE ENTRADA</div><div id="entryTime" class="value">--</div></div>
<div class="entryBox"><div class="small">EXPIRAÇÃO</div><div id="expiry" class="value">--</div></div>
</div>
<div style="margin-top:12px"><div class="small">CRONÔMETRO DE ENTRADA / PRÓXIMA VELA</div><div id="countdown" class="countdown">--:--</div></div>
<div class="row" style="margin-top:12px">
<div class="field"><div class="small">Confiança</div><div id="confidence" class="value">--</div></div>
<div class="field"><div class="small">Referência</div><div id="reference" class="value">--</div></div>
<div class="field"><div class="small">Próxima vela</div><div id="next" class="value">--</div></div>
</div>
</div>

<div class="card">
<div class="small">RESULTADOS</div>
<div class="grid">
<div class="stat"><span>WIN</span><b id="wins" class="good">0</b></div>
<div class="stat"><span>LOSS</span><b id="losses" class="bad">0</b></div>
<div class="stat"><span>ASSERTIVIDADE</span><b id="accuracy">0%</b></div>
</div>
<button id="reset" style="margin-top:10px">ZERAR RESULTADOS</button>
</div>

<div class="card" id="radarCard">
<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap">
<div><div class="small">RADAR DE OPORTUNIDADES</div><div class="sniperTitle">PARES PRÓXIMOS DE SINAL</div></div>
<div id="radarStatus" class="badge">AGUARDANDO</div>
</div>
<div class="small" style="margin-top:8px">Mostra os pares com maior proximidade de CALL ou PUT na estratégia ativa.</div>
<div id="radarList" style="margin-top:12px"></div>
</div>

<div class="card footer">
O sinal é probabilístico e não garante WIN. A análise usa velas fechadas para reduzir repintura. Os preços da Twelve Data podem apresentar diferenças em relação à cotação da sua corretora.
</div>
</div>

<script>
const $ = id => document.getElementById(id);
let pending = JSON.parse(localStorage.getItem("is_trade_pending") || "null");
let stats = JSON.parse(localStorage.getItem("is_trade_stats") || '{"wins":0,"losses":0}');
let isOnline = true;
let rsiOnline = localStorage.getItem("is_trade_rsi_online") !== "off";
let oldOnline = localStorage.getItem("is_trade_old_online") === "on";
let sniper02Online = localStorage.getItem("is_trade_sniper02_online") === "on";
let sniper03Online = localStorage.getItem("is_trade_sniper03_online") === "on";
let selectedStrategy = localStorage.getItem("is_trade_selected_strategy") || (oldOnline ? "old_sniper" : sniper03Online ? "sniper_03" : sniper02Online ? "sniper_02" : "rsi");

function save(){
  localStorage.setItem("is_trade_stats", JSON.stringify(stats));
  if(pending) localStorage.setItem("is_trade_pending", JSON.stringify(pending));
  else localStorage.removeItem("is_trade_pending");
}
function renderStats(){
  $("wins").textContent = stats.wins;
  $("losses").textContent = stats.losses;
  const total = stats.wins + stats.losses;
  $("accuracy").textContent = total ? ((stats.wins/total)*100).toFixed(1)+"%" : "0%";
}
function setStrategyButton(id, online){const b=$(id);b.textContent=online?"● ONLINE":"● OFFLINE";b.className=online?"onlineBtn online":"onlineBtn offline";}
function activeStrategy(){
  if(selectedStrategy==="rsi" && rsiOnline)return "rsi";
  if(selectedStrategy==="old_sniper" && oldOnline)return "old_sniper";
  if(selectedStrategy==="sniper_02" && sniper02Online)return "sniper_02";
  if(selectedStrategy==="sniper_03" && sniper03Online)return "sniper_03";
  if(rsiOnline)return "rsi";
  if(oldOnline)return "old_sniper";
  if(sniper02Online)return "sniper_02";
  if(sniper03Online)return "sniper_03";
  return null;
}
function renderMode(){setStrategyButton("rsiToggle",rsiOnline);setStrategyButton("oldToggle",oldOnline);setStrategyButton("sniper02Toggle",sniper02Online);setStrategyButton("sniper03Toggle",sniper03Online);const active=activeStrategy();if(!isOnline||!active){$("signal").textContent="OFFLINE";$("signal").className="signal neutral";}}

function fmt(v){ return v == null ? "--" : v; }

let resultTimer = null;

// Alerta sonoro de 5 segundos para novos sinais CALL/PUT.
// O navegador exige uma interação do usuário antes de liberar áudio em muitos celulares.
let audioCtx = null;
let lastSoundSignal = localStorage.getItem("is_trade_last_sound_signal") || "";

function unlockAudio(){
  try{
    if(!audioCtx){
      const AC = window.AudioContext || window.webkitAudioContext;
      if(!AC) return;
      audioCtx = new AC();
    }
    if(audioCtx.state === "suspended") audioCtx.resume();
  }catch(e){}
}

function playSignalSound(direction){
  if(direction !== "CALL" && direction !== "PUT") return;
  try{
    unlockAudio();
    if(!audioCtx || audioCtx.state === "suspended") return;

    const now = audioCtx.currentTime;
    const osc = audioCtx.createOscillator();
    const gain = audioCtx.createGain();
    osc.type = "sine";
    osc.frequency.setValueAtTime(direction === "CALL" ? 880 : 440, now);
    gain.gain.setValueAtTime(0.0001, now);
    gain.gain.exponentialRampToValueAtTime(0.16, now + 0.03);
    gain.gain.setValueAtTime(0.16, now + 4.7);
    gain.gain.exponentialRampToValueAtTime(0.0001, now + 5.0);
    osc.connect(gain);
    gain.connect(audioCtx.destination);
    osc.start(now);
    osc.stop(now + 5.02);
  }catch(e){}
}

// Libera o áudio após qualquer toque/clique do usuário.
document.addEventListener("pointerdown", unlockAudio, {passive:true});
document.addEventListener("touchstart", unlockAudio, {passive:true});

function alertNewSignal(direction, referenceCandle, entryTime){
  if(direction !== "CALL" && direction !== "PUT") return;
  const key = `${referenceCandle || ""}|${entryTime || ""}|${direction}`;
  if(key === lastSoundSignal) return;
  lastSoundSignal = key;
  localStorage.setItem("is_trade_last_sound_signal", key);
  playSignalSound(direction);
}

function showError(message){
  $("signal").textContent = "SEM DADOS";
  $("signal").className = "signal neutral";
  $("confidence").textContent = "--";
  if($("signalError")){
    $("signalError").textContent = message || "Não foi possível atualizar o sinal.";
    $("signalError").style.display = "block";
  }
}
function updateClock(){
  const now = new Date();
  const parts = new Intl.DateTimeFormat("pt-BR", {timeZone:"America/Sao_Paulo",hour:"2-digit",minute:"2-digit",second:"2-digit",hour12:false}).formatToParts(now);
  const get = k => parts.find(p => p.type === k)?.value || "00";
  $("clock").textContent = `${get("hour")}:${get("minute")}:${get("second")}`;
  $("dateBr").textContent = new Intl.DateTimeFormat("pt-BR", {timeZone:"America/Sao_Paulo",day:"2-digit",month:"2-digit",year:"numeric"}).format(now);
}
let serverOffsetMs = 0;
function updateCountdown(){
  const interval = $("interval").value;
  const minutes = ({"1min":1,"5min":5,"15min":15,"30min":30})[interval] || 1;
  const now = new Date(Date.now() + serverOffsetMs);
  const block = minutes*60*1000;
  const next = Math.ceil(now.getTime()/block)*block;
  const total = Math.max(0, Math.floor((next-now.getTime())/1000));
  const mm = String(Math.floor(total/60)).padStart(2,"0");
  const ss = String(total%60).padStart(2,"0");
  $("countdown").textContent = `${mm}:${ss}`;
}
async function syncServerClock(){
  try{
    const t0=Date.now();
    const r=await fetch("/server-time",{cache:"no-store"});
    const d=await r.json();
    const t1=Date.now();
    const serverMs=new Date(d.brasilia).getTime();
    serverOffsetMs=serverMs-((t0+t1)/2);
  }catch(e){}
}
async function loadLicense(){
  try{
    const r=await fetch("/license",{cache:"no-store"});
    const d=await r.json();
    if(d.active){
      $("licenseStatus").textContent="● LICENÇA ATIVA";
      $("licenseStatus").className="value good";
    }else{
      $("licenseStatus").textContent="● LICENÇA EXPIRADA";
      $("licenseStatus").className="value bad";
    }
    $("licenseExpires").textContent=d.expires || "--";
  }catch(e){ $("licenseStatus").textContent="NÃO VERIFICADA"; }
}


function scheduleResultCheck(){
  if(resultTimer) clearTimeout(resultTimer);
  if(!pending) return;

  const minutes = ({"1min":1,"5min":5,"15min":15,"30min":30})[pending.interval] || 1;
  const refMs = new Date(pending.reference_candle.replace(" ", "T") + (pending.reference_candle.length <= 16 ? ":00" : "" )).getTime();
  if(Number.isNaN(refMs)){
    resultTimer = setTimeout(checkResult, minutes * 60 * 1000 + 5000);
    return;
  }

  // Entrada no fechamento da referência; resultado no fechamento da vela seguinte.
  const due = refMs + (2 * minutes * 60 * 1000) + 3000;
  const wait = Math.max(1000, due - Date.now());
  resultTimer = setTimeout(checkResult, wait);
}

let ultimoSinalFalado=localStorage.getItem("is_trade_last_voice_signal")||"";
function falarSinal(direcao,tendencia,confianca){
 if(direcao!=="CALL"&&direcao!=="PUT")return;
 const dirTexto=direcao==="CALL"?"sinal de compra":"sinal de venda";
 const tendenciaTexto=tendencia==="ALTA"?"tendência do gráfico é de alta":tendencia==="BAIXA"?"tendência do gráfico é de baixa":"tendência do gráfico é neutra";
 const mensagem=`${dirTexto}. ${tendenciaTexto}. Confiança ${Math.round(Number(confianca)||0)} por cento.`;
 const chave=`${direcao}|${tendencia}|${confianca}|${$("entryTime").textContent}`;
 if(chave===ultimoSinalFalado)return; ultimoSinalFalado=chave; localStorage.setItem("is_trade_last_voice_signal",chave);
 if(!("speechSynthesis" in window))return; window.speechSynthesis.cancel(); const voz=new SpeechSynthesisUtterance(mensagem); voz.lang="pt-BR"; voz.rate=.92; voz.pitch=1; voz.volume=1; window.speechSynthesis.speak(voz);
}

async function loadAI(){
 const status=$("aiStatus"),main=$("aiMain"),meta=$("aiMeta"),reason=$("aiReason");
 if(!status||!main||!aiOnline)return;
 status.textContent="ANALISANDO 240 VELAS...";
 try{const symbol=$("symbol").value,interval=$("interval").value;const r=await fetch(`/ai-analysis?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}`,{cache:"no-store"});const d=await r.json();if(!r.ok||!d.ok)throw new Error(d.detail||"Falha na IA");const a=d.analysis,t=d.trend||{};main.textContent=`${a.signal} — ${a.confidence}%`;main.className="aiMain "+(a.signal==="CALL"?"good":a.signal==="PUT"?"bad":"");meta.textContent=`Qualidade: ${a.quality} | Regime: ${a.regime} | Tendência: ${t.trend||"NEUTRA"} | RSI 9: ${a.rsi9??"-"}`;reason.textContent=a.reason||"";status.textContent="● IA ONLINE • atualizada";}catch(e){status.textContent="IA AGUARDAR";main.textContent="ERRO NA ANÁLISE";meta.textContent=e.message||"";reason.textContent="";}}

async function loadSignal(){
  if(!isOnline){ renderMode(); return; }
  const symbol = $("symbol").value;
  const interval = $("interval").value;
  const strategy = activeStrategy();
  if(!strategy){ renderMode(); return; }
  $("signal").textContent = "ANALISANDO...";
  $("signal").className = "signal neutral";
  try{
    const r = await fetch(`/signal?symbol=${encodeURIComponent(symbol)}&interval=${interval}&strategy=${encodeURIComponent(strategy)}`, {cache:"no-store"});
    const d = await r.json();
    if(!r.ok){
      const msg = d.detail && typeof d.detail === "object" ? d.detail.message : (d.detail || "Erro ao consultar o sinal.");
      throw new Error(msg);
    }
    $("signal").textContent = d.signal;
    $("signal").className = "signal " + (d.signal==="CALL" ? "call" : d.signal==="PUT" ? "put" : "neutral");
    $("confidence").textContent = d.confidence + "%";
    if($("trend")){
      $("trend").textContent = d.trend_text || d.trend || "Tendência neutra";
    }
    if($("signalError")){ $("signalError").textContent = ""; $("signalError").style.display = "none"; }
    $("reference").textContent = d.reference_candle;
    $("next").textContent = d.entry_time || d.next_candle;
    $("entryTime").textContent = (d.entry_time || d.next_candle || "--").split(" ")[1] || "--";
    $("expiry").textContent = (d.expiry_time || "").split(" ")[1] || "--";
    $("rsi9").textContent = fmt(d.rsi9);
    $("rsi14").textContent = fmt(d.rsi14);
    if($("engulf")) $("engulf").textContent = d.bullish_engulfing || d.bearish_engulfing ? "SIM" : "NÃO";
    if($("rejection")) $("rejection").textContent = d.bullish_rejection || d.bearish_rejection ? "SIM" : "NÃO";
    if($("breakout")) $("breakout").textContent = d.bullish_breakout || d.bearish_breakout ? "SIM" : "NÃO";
    if($("strength")) $("strength").textContent = (d.bullish_strength || d.bearish_strength) ? "SIM" : "NÃO";

    if(d.signal !== "NEUTRO"){
      falarSinal(d.signal, d.trend, d.confidence);
      pending = {symbol, interval, reference_candle:d.reference_candle, entry_time:d.entry_time, direction:d.signal};
      save();
      scheduleResultCheck();
    }
  }catch(e){
    showError(e.message || "Erro ao consultar o sinal.");
  }
}

async function checkResult(){
  if(!pending) return;
  try{
    const q = new URLSearchParams(pending).toString();
    const r = await fetch(`/result?${q}`, {cache:"no-store"});
    const d = await r.json();
    if(!r.ok) return;
    if(d.result === "WIN"){
      stats.wins++; pending = null; save(); renderStats();
    }else if(d.result === "LOSS"){
      stats.losses++; pending = null; save(); renderStats();
    }else if(d.result === "DRAW"){
      pending = null; save();
    }else{
      // Se ainda não fechou, não fica consultando a cada 15 segundos.
      scheduleResultCheck();
    }
  }catch(e){
    scheduleResultCheck();
  }
}


function radarRow(item){
  const cls=item.direction==="CALL"?"call":item.direction==="PUT"?"put":"neutral";
  const label=item.direction==="NEUTRO"?(item.proximity>=75?"PRÓXIMO":"OBSERVAR"):item.direction;
  return `<div class="radarRow"><div><b>${item.symbol}</b><div class="small">${label} • confiança ${item.confidence}%</div></div><div class="radarMeter"><div class="radarFill ${cls}" style="width:${item.proximity}%"></div></div><b>${item.proximity}%</b></div>`;
}
async function loadRadar(){
  const strategy=activeStrategy();
  if(!isOnline||!strategy){$("radarStatus").textContent="OFFLINE";$("radarList").innerHTML="<div class='small'>Ative uma estratégia para iniciar o radar.</div>";return;}
  $("radarStatus").textContent="ANALISANDO";
  try{
    const interval=$("interval").value;
    const r=await fetch(`/radar?interval=${encodeURIComponent(interval)}&strategy=${encodeURIComponent(strategy)}`,{cache:"no-store"});
    const d=await r.json();
    if(!r.ok){const msg=d.detail&&typeof d.detail==="object"?d.detail.message:(d.detail||"Erro no radar.");throw new Error(msg);}
    const items=(d.results||[]);
    $("radarStatus").textContent=items.length?`${items.length} PARES`:"SEM DADOS";
    $("radarList").innerHTML=items.length?items.map(radarRow).join(""):"<div class='small'>Nenhum par disponível no momento.</div>";
  }catch(e){
    $("radarStatus").textContent="AGUARDAR";
    $("radarList").innerHTML=`<div class='small'>Radar temporariamente indisponível: ${e.message||"erro de dados"}</div>`;
  }
}


$("refresh").onclick = ()=>{loadSignal();loadAI();loadRadar();};
$("rsiToggle").onclick=()=>{rsiOnline=!rsiOnline;if(rsiOnline)selectedStrategy="rsi";localStorage.setItem("is_trade_rsi_online",rsiOnline?"on":"off");localStorage.setItem("is_trade_selected_strategy",selectedStrategy);pending=null;save();renderMode();if(isOnline&&rsiOnline){loadSignal();loadRadar();}};
$("oldToggle").onclick=()=>{oldOnline=!oldOnline;if(oldOnline)selectedStrategy="old_sniper";localStorage.setItem("is_trade_old_online",oldOnline?"on":"off");localStorage.setItem("is_trade_selected_strategy",selectedStrategy);pending=null;save();renderMode();if(isOnline&&oldOnline){loadSignal();loadRadar();}};
$("sniper02Toggle").onclick=()=>{sniper02Online=!sniper02Online;if(sniper02Online)selectedStrategy="sniper_02";localStorage.setItem("is_trade_sniper02_online",sniper02Online?"on":"off");localStorage.setItem("is_trade_selected_strategy",selectedStrategy);pending=null;save();renderMode();if(isOnline&&sniper02Online){loadSignal();loadRadar();}};
$("sniper03Toggle").onclick=()=>{sniper03Online=!sniper03Online;if(sniper03Online)selectedStrategy="sniper_03";localStorage.setItem("is_trade_sniper03_online",sniper03Online?"on":"off");localStorage.setItem("is_trade_selected_strategy",selectedStrategy);pending=null;save();renderMode();if(isOnline&&sniper03Online){loadSignal();loadRadar();}};
$("aiToggle").onclick=()=>{aiOnline=!aiOnline;localStorage.setItem("is_trade_ai_online",aiOnline?"on":"off");const b=$("aiToggle");b.textContent=aiOnline?"● ONLINE":"● OFFLINE";b.className="onlineBtn "+(aiOnline?"online":"offline");if(aiOnline)loadAI();};

$("reset").onclick = () => {
  if(confirm("Zerar WIN e LOSS?")){
    stats = {wins:0,losses:0};
    pending = null;
    save();
    renderStats();
  }
};

renderStats();
renderMode();
updateClock();
updateCountdown();
setInterval(updateClock, 1000);
setInterval(updateCountdown, 250);
syncServerClock();
loadLicense();
loadAI();
loadRadar();
// Se não houver operação pendente, atualiza somente na virada da vela.
let lastEntrySlot = "";
setInterval(() => {
  if(pending) return;
  const interval = $("interval").value;
  const mins = ({"1min":1,"5min":5,"15min":15,"30min":30})[interval] || 1;
  const now = new Date(Date.now() + serverOffsetMs);
  const slot = Math.floor(now.getTime()/(mins*60000));
  const seconds = Math.floor((now.getTime()%(mins*60000))/1000);
  if(seconds <= 3 && String(slot) !== lastEntrySlot){
    lastEntrySlot = String(slot);
    loadSignal();
  }
}, 1000);
if(pending) scheduleResultCheck();
setInterval(syncServerClock, 30000);
setInterval(loadLicense, 60000);
setInterval(loadRadar, 120000);
setInterval(loadAI, 60000);
</script>
</body>
</html>"""
    return HTMLResponse(html)


@app.get("/health")
async def health() -> Dict[str, Any]:
    return {"ok": True, "app": APP_NAME, "version": APP_VERSION}


@app.get("/server-time")
async def server_time() -> Dict[str, str]:
    current = now_sp()
    return {
        "brasilia": current.isoformat(),
        "utc": datetime.utcnow().isoformat() + "+00:00",
    }


@app.get("/license")
async def license() -> Dict[str, Any]:
    return {"ok": True, **license_status()}


def require_active_license() -> None:
    status = license_status()
    if not status["active"]:
        raise HTTPException(status_code=403, detail={"error": "LICENSE_EXPIRED", **status})


@app.get("/candles")
async def candles(
    symbol: str = "EUR/USD",
    interval: str = "1min",
    size: int = 120,
) -> Dict[str, Any]:
    require_active_license()
    requested_size = int(size)
    values = await get_candles(symbol, interval, requested_size)
    return {
        "ok": True,
        "source": "Twelve Data",
        "symbol": symbol,
        "interval": interval,
        "values": values,
    }


@app.get("/ai-analysis")
async def ai_analysis(symbol: str = "EUR/USD", interval: str = "1min") -> Dict[str, Any]:
    require_active_license()
    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(status_code=400, detail="Timeframe inválido.")
    values = await get_candles(symbol, interval, 240)
    closed = values[:-1]
    if len(closed) < 60:
        analysis = {"signal":"NEUTRO","confidence":50,"quality":"BAIXA","regime":"INDEFINIDO","reason":"Dados insuficientes para a IA."}
    else:
        closes=[float(c["close"]) for c in closed]
        e9=ema(closes,9); e20=ema(closes,20); e50=ema(closes,50)
        r9=float(rsi(closes,9)[-1]); r14=float(rsi(closes,14)[-1])
        bull=bear=0.0; reasons=[]
        if e9[-1]>e20[-1]: bull+=2; reasons.append("EMA 9 acima da EMA 20")
        else: bear+=2; reasons.append("EMA 9 abaixo da EMA 20")
        if e20[-1]>e50[-1]: bull+=2
        else: bear+=2
        slope=e20[-1]-e20[-6]
        tr=true_ranges(closed); atr=sum(tr[-14:])/14 if len(tr)>=14 else 0
        threshold=max(atr*0.05,abs(closes[-1])*0.00001)
        if slope>threshold: bull+=1.5
        elif slope<-threshold: bear+=1.5
        if r9<35 and r14<45: bull+=1.5; reasons.append("RSI favorece recuperação")
        elif r9>65 and r14>55: bear+=1.5; reasons.append("RSI favorece pressão vendedora")
        last=closed[-1]; strength=abs(float(last["close"])-float(last["open"]))/max(float(last["high"])-float(last["low"]),1e-12)
        if float(last["close"])>float(last["open"]) and strength>=.55: bull+=1
        elif float(last["close"])<float(last["open"]) and strength>=.55: bear+=1
        diff=abs(bull-bear)
        signal="CALL" if bull>bear and bull>=5 and diff>=2 else "PUT" if bear>bull and bear>=5 and diff>=2 else "NEUTRO"
        confidence=int(max(50,min(95,58+diff*6))) if signal!="NEUTRO" else int(max(50,min(65,50+diff*4)))
        quality="ALTA" if confidence>=75 else "MÉDIA" if confidence>=60 else "BAIXA"
        regime="ALTA" if e9[-1]>e20[-1]>e50[-1] else "BAIXA" if e9[-1]<e20[-1]<e50[-1] else "LATERAL"
        analysis={"signal":signal,"confidence":confidence,"quality":quality,"regime":regime,"reason":("IA: "+("COMPRA" if signal=="CALL" else "VENDA" if signal=="PUT" else "AGUARDAR")+". "+"; ".join(reasons[:3])),"rsi9":round(r9,2),"rsi14":round(r14,2),"non_repaint":True}
    return {"ok":True,"source":"Twelve Data","symbol":symbol,"interval":interval,"analysis":analysis,"trend":detect_market_trend(values)}


@app.get("/signal")
async def signal(
    symbol: str = "EUR/USD",
    interval: str = "1min",
    strategy: str = "rsi",
) -> Dict[str, Any]:
    require_active_license()
    if strategy not in {"rsi", "old_sniper", "sniper_02", "sniper_03"}:
        raise HTTPException(status_code=400, detail="Estratégia inválida.")
    values = await get_candles(symbol, interval, 240)
    result = analyze(values, strategy)
    trend_info = detect_market_trend(values)
    result.update(trend_info)
    # A análise usa a última vela fechada. A entrada deve ser na próxima
    # abertura futura, nunca em uma vela cujo início já passou.
    minutes = ALLOWED_INTERVALS[interval]
    current = now_sp()
    block_seconds = minutes * 60
    epoch = int(current.timestamp())
    next_epoch = ((epoch // block_seconds) + 1) * block_seconds
    entry_dt = datetime.fromtimestamp(next_epoch, tz=SP_TZ)
    expiry_dt = entry_dt + timedelta(minutes=minutes)

    result.update(
        {
            "source": "Twelve Data",
            "symbol": symbol,
            "interval": interval,
            "entry_time": entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "next_candle": entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "expiry_time": expiry_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "expiry": "1 vela do intervalo selecionado",
            "warning": "Sinal probabilístico; não garante WIN. A fonte Twelve Data pode não coincidir com os preços da corretora.",
        }
    )
    return result


@app.get("/result")
async def result(
    symbol: str,
    interval: str,
    reference_candle: str,
    direction: str,
    entry_time: Optional[str] = None,
) -> Dict[str, Any]:
    require_active_license()
    direction = direction.upper().strip()
    if direction not in {"CALL", "PUT"}:
        raise HTTPException(status_code=400, detail="Direção inválida.")

    values = await get_candles(symbol, interval, 100)
    reference_dt = parse_time(reference_candle)

    reference_index: Optional[int] = None
    for i, candle in enumerate(values):
        if parse_time(candle["datetime"]) == reference_dt:
            reference_index = i
            break

    if reference_index is None:
        return {"ok": True, "status": "PENDING", "result": None}

    if entry_time:
        entry_dt = parse_time(entry_time)
        entry_index: Optional[int] = None
        for i, candle in enumerate(values):
            if parse_time(candle["datetime"]) == entry_dt:
                entry_index = i
                break
        if entry_index is None or entry_index + 1 >= len(values):
            return {"ok": True, "status": "PENDING", "result": None}
        result_candle = values[entry_index]
        result_index = entry_index
        result_time = parse_time(result_candle["datetime"])
        if not candle_is_closed(result_time, interval):
            return {"ok": True, "status": "PENDING", "result": None, "entry_candle": result_candle["datetime"]}
        reference_close = result_candle["open"]
        result_close = result_candle["close"]
        result_candle_time = result_candle["datetime"]
    else:
        next_index = reference_index + 1
        if next_index >= len(values):
            return {"ok": True, "status": "PENDING", "result": None}
        next_candle = values[next_index]
        next_time = parse_time(next_candle["datetime"])
        if not candle_is_closed(next_time, interval):
            return {"ok": True, "status": "PENDING", "result": None, "next_candle": next_candle["datetime"]}
        reference_close = values[reference_index]["close"]
        result_close = next_candle["close"]
        result_candle_time = next_candle["datetime"]

    # A entrada é considerada no fechamento da vela de referência.
    # A expiração de 1 vela compara o fechamento seguinte com essa entrada.
    tolerance = max(abs(reference_close) * 1e-10, 1e-12)
    if abs(result_close - reference_close) <= tolerance:
        outcome = "DRAW"
    elif direction == "CALL":
        outcome = "WIN" if result_close > reference_close else "LOSS"
    else:
        outcome = "WIN" if result_close < reference_close else "LOSS"

    return {
        "ok": True,
        "status": "CLOSED",
        "result": outcome,
        "direction": direction,
        "reference_candle": reference_candle,
        "result_candle": result_candle_time,
        "entry_close": reference_close,
        "result_close": result_close,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
