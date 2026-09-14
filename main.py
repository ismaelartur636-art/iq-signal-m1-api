import os
import asyncio
import time
import json
import re
import secrets
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict

import httpx

try:
    from iqoptionapi.stable_api import IQ_Option
except Exception:
    IQ_Option = None

try:
    from olymptrade_ws import OlympTradeClient
except Exception:
    try:
        from olymptrade_ws.main import OlympTradeClient
    except Exception:
        OlympTradeClient = None

# Conector alternativo que aceita e-mail/senha.
try:
    from olymptradeapi.stable_api import Olymptrade as OlympTradeLoginClient
except Exception:
    OlympTradeLoginClient = None

from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel
from fastapi.responses import HTMLResponse, FileResponse

app = FastAPI(title="MEGA IA", version="33.54.0")
print("[MEGA IA] versão 33.54.0 carregada", flush=True)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
IMAGE_PATH = os.path.join(BASE_DIR, "mega_ia.png")
ICON_512_PATH = os.path.join(BASE_DIR, "mega_ia_icon.png")
ICON_192_PATH = os.path.join(BASE_DIR, "mega_ia_icon_192.png")

BR_TZ = ZoneInfo("America/Sao_Paulo")
UTC = timezone.utc

TD_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
OAI_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OAI_MODEL = os.getenv("OPENAI_MODEL", "").strip()
GEMINI_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "").strip()
GEMINI_MODEL_FALLBACKS = ["gemini-3.8-flash", "gemini-3.6-flash", "gemini-2.0-flash"]
OAI_MIN = float(os.getenv("OPENAI_MIN_CONFIDENCE", "70"))
OAI_TIMEOUT = float(os.getenv("OPENAI_TIMEOUT", "15"))

OLYMPTRADE_TOKEN = os.getenv("OLYMPTRADE_TOKEN", "").strip()

LICENSE = os.getenv("LICENSE_EXPIRES", "2026-12-31")
WA1 = os.getenv("WHATSAPP_1", "55 84 99841-1282")
WA2 = os.getenv("WHATSAPP_2", "55 84 99449-9442")
IG = os.getenv("INSTAGRAM", "@Ismaelartur26")

TD_URL = "https://api.twelvedata.com/time_series"
OAI_URL = "https://api.openai.com/v1/responses"
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

INTERVALS = {"1min": 60, "5min": 300, "15min": 900, "30min": 1800, "1h": 3600, "4h": 14400}
SYMBOLS = [
    "EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD", "USD/CHF",
    "NZD/USD", "EUR/JPY", "GBP/JPY", "EUR/GBP", "BTC/USD", "ETH/USD", "LTC/USD"
]

OTC_BASE = {
    "EUR/USD": "EURUSD-OTC", "GBP/USD": "GBPUSD-OTC", "USD/JPY": "USDJPY-OTC",
    "AUD/USD": "AUDUSD-OTC", "USD/CAD": "USDCAD-OTC", "USD/CHF": "USDCHF-OTC",
    "NZD/USD": "NZDUSD-OTC", "EUR/JPY": "EURJPY-OTC", "GBP/JPY": "GBPJPY-OTC",
    "EUR/GBP": "EURGBP-OTC", "BTC/USD": "BTCUSD-OTC", "ETH/USD": "ETHUSD-OTC",
    "LTC/USD": "LTCUSD-OTC"
}

cache: Dict[str, Any] = {}
# Controle anti-repetição de sinais.
# Mantém o mesmo sinal até a expiração e exige um novo setup antes de liberar outro
# sinal na mesma direção.
signal_release_state: Dict[str, Any] = {}
ai_scan_state: Dict[str, Any] = {}
oai_cache: Dict[str, Any] = {}
results: Dict[str, Any] = {}
# Contabilidade principal independente do fluxo de Gale.
# Cada sinal confirmado entra aqui e é avaliado na vela original.
accounting_pending: Dict[str, Dict[str, Any]] = {}
accounting_results: Dict[str, Dict[str, Any]] = {}
radar_cache: Dict[str, Any] = {}
pre_signal_cache: Dict[str, Any] = {}
chart_pre_signal_lock: Dict[str, Dict[str, Any]] = {}
chart_pre_signal_last_at: Dict[str, float] = {}
chart_pre_signal_candidate: Dict[str, Dict[str, Any]] = {}
CHART_SIGNAL_COOLDOWN_SECONDS = 180
CHART_SIGNAL_CONFIRM_READS = 3
CHART_SIGNAL_CONFIRM_MAX_GAP = 6
PRE_SIGNAL_TTL = 75
PRE_SIGNAL_BATCH = 1

td_sem = asyncio.Semaphore(1)
td_candle_cache: Dict[str, Any] = {}
td_locks: Dict[str, asyncio.Lock] = {}
td_rate_lock = asyncio.Lock()
td_last_call_at = 0.0
td_backoff_until = 0.0
td_backoff_reason = ""
TD_MIN_CALL_INTERVAL = float(os.getenv("TWELVE_DATA_MIN_INTERVAL", "8.0"))
TD_STALE_MAX_AGE = float(os.getenv("TWELVE_DATA_STALE_MAX_AGE", "900"))

# IQ OPTION — implementação reconstruída do zero.
# O login é feito somente pelo painel; não há credenciais IQ no Render.
IQ_SESSION_COOKIE = "mega_iq_session"
IQ_SESSION_TTL = int(os.getenv("IQ_SESSION_TTL", "43200"))
IQ_CONNECT_TIMEOUT = float(os.getenv("IQ_CONNECT_TIMEOUT", "35"))
IQ_CANDLE_TIMEOUT = float(os.getenv("IQ_CANDLE_TIMEOUT", "15"))
IQ_CANDLE_CACHE_TTL = float(os.getenv("IQ_CANDLE_CACHE_TTL", "3"))
iq_sessions: Dict[str, Dict[str, Any]] = {}

VALID_MARKETS = ("OPEN", "IQ_OTC", "OLYMP_OTC")

olymp_client = None
olymp_sessions: Dict[str, Dict[str, Any]] = {}
OLYMP_SESSION_COOKIE = "mega_olymp_session"
OLYMP_SESSION_TTL = 60 * 60 * 12
olymp_lock = asyncio.Lock()
olymp_candle_cache: Dict[str, Any] = {}
OLYMP_CANDLE_TTL = float(os.getenv("OLYMP_CANDLE_TTL", "15"))


class IQLoginBody(BaseModel):
    email: str
    password: str


class OlympLoginBody(BaseModel):
    email: str
    password: str


def now():
    return datetime.now(BR_TZ)


def iso(d: datetime):
    return d.astimezone(BR_TZ).isoformat()


def parse_dt(value: str):
    d = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return (d if d.tzinfo else d.replace(tzinfo=UTC)).astimezone(BR_TZ)



def _candle_time_candidates(value: str):
    """
    Twelve Data pode entregar datetime sem offset.
    Para fechar WIN/LOSS, aceita tanto UTC quanto horário de Brasília
    e escolhe a interpretação mais próxima da entrada.
    """
    raw = str(value or "").strip()
    if not raw:
        return []

    out = []
    try:
        d = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return out

    if d.tzinfo is not None:
        return [d.astimezone(BR_TZ)]

    # Interpretação 1: datetime recebido já está em Brasília.
    try:
        out.append(d.replace(tzinfo=BR_TZ))
    except Exception:
        pass

    # Interpretação 2: datetime recebido está em UTC.
    try:
        out.append(d.replace(tzinfo=UTC).astimezone(BR_TZ))
    except Exception:
        pass

    # Remove duplicatas.
    unique = []
    seen = set()
    for item in out:
        key = item.isoformat()
        if key not in seen:
            seen.add(key)
            unique.append(item)
    return unique


def _nearest_candle_for_time(candles_list, target_dt, interval_seconds):
    target = None
    best_delta = None
    best_dt = None

    for c in candles_list or []:
        for cdt in _candle_time_candidates(c.get("datetime")):
            delta = abs((cdt - target_dt).total_seconds())
            if best_delta is None or delta < best_delta:
                best_delta = delta
                target = c
                best_dt = cdt

    # Um candle do timeframe deve ficar muito próximo do horário esperado.
    tolerance = max(45.0, float(interval_seconds) * 0.80)
    if target is None or best_delta is None or best_delta > tolerance:
        return None, best_delta, best_dt

    return target, best_delta, best_dt


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def _mask_email(email: str):
    if "@" not in email:
        return "***"
    name, domain = email.split("@", 1)
    visible = name[:1] if name else ""
    return visible + "•••••@" + domain


def _iq_connected(state: Dict[str, Any] | None) -> bool:
    if not state:
        return False
    client = state.get("client")
    if client is None:
        return False
    try:
        return bool(client.check_connect())
    except Exception:
        return False


def _iq_close_state(state: Dict[str, Any] | None):
    if not state:
        return

    client = state.get("client")
    state["client"] = None
    state["connected"] = False

    if client is None:
        return

    try:
        api = getattr(client, "api", None)
        close = getattr(api, "close", None)
        if callable(close):
            close()
    except Exception:
        pass


def _cleanup_iq_sessions():
    now_ts = time.time()
    expired = [
        token
        for token, state in iq_sessions.items()
        if now_ts - float(state.get("last_seen", now_ts)) > IQ_SESSION_TTL
    ]

    for token in expired:
        state = iq_sessions.pop(token, None)
        if state:
            state["password"] = ""
            _iq_close_state(state)


def _iq_session_state(request: Request, required: bool = False):
    _cleanup_iq_sessions()

    header_token = request.headers.get("X-IQ-Session", "")
    cookie_token = request.cookies.get(IQ_SESSION_COOKIE, "")

    for token in (header_token, cookie_token):
        if not token:
            continue

        state = iq_sessions.get(token)
        if state:
            state["session_id"] = token
            state["last_seen"] = time.time()
            state["connected"] = _iq_connected(state)
            return state

    if required:
        raise HTTPException(
            401,
            "Faça login na IQ Option pela aba Corretora para usar o OTC."
        )

    return None


def _iq_state_for_request(request: Request, required: bool = False):
    # Alias interno para manter os outros módulos simples.
    return _iq_session_state(request, required=required)



def _olymp_asset(symbol: str) -> str:
    # OlympTrade examples use pair symbols without slash.
    # OTC naming may vary by instrument; try standard OTC aliases in order.
    return symbol.replace("/", "").replace(" ", "")


def _normalize_olymp_candle(item):
    if hasattr(item, "__dict__") and not isinstance(item, dict):
        item = vars(item)
    if not isinstance(item, dict):
        return None

    ts = (
        item.get("time")
        or item.get("timestamp")
        or item.get("from")
        or item.get("datetime")
    )
    try:
        if isinstance(ts, (int, float)):
            # tolerate ms timestamps
            if ts > 10_000_000_000:
                ts = ts / 1000.0
            dt = datetime.fromtimestamp(float(ts), tz=UTC).astimezone(BR_TZ).isoformat()
        else:
            dt = str(ts)
    except Exception:
        dt = str(ts or "")

    try:
        return {
            "datetime": dt,
            "open": float(item.get("open", item.get("o"))),
            "high": float(item.get("high", item.get("h"))),
            "low": float(item.get("low", item.get("l"))),
            "close": float(item.get("close", item.get("c"))),
            "volume": float(item.get("volume", item.get("v", 0)) or 0),
        }
    except Exception:
        return None


async def _get_olymp_client(request: Request | None = None):
    global olymp_client

    if request is not None:
        state = _olymp_session_state(request)
        if state and state.get("client") is not None:
            return state["client"]

    if not OLYMPTRADE_TOKEN:
        raise HTTPException(
            503,
            "Olymptrade OTC não está conectada. Faça login com e-mail e senha no painel "
            "ou configure OLYMPTRADE_TOKEN no servidor."
        )
    if OlympTradeClient is None:
        raise HTTPException(
            503,
            "Biblioteca olymptrade_ws não carregada. Adicione a biblioteca ao requirements.txt."
        )

    async with olymp_lock:
        if olymp_client is None:
            try:
                olymp_client = OlympTradeClient(access_token=OLYMPTRADE_TOKEN)
                start = getattr(olymp_client, "start", None)
                if start:
                    result = start()
                    if asyncio.iscoroutine(result):
                        await asyncio.wait_for(result, timeout=15)
            except Exception as exc:
                olymp_client = None
                raise HTTPException(503, f"Falha ao conectar à Olymptrade: {str(exc)[:220]}")
        return olymp_client


async def candles_olymp(symbol: str, interval: str, n: int = 80, request: Request | None = None):
    if symbol not in SYMBOLS or interval not in INTERVALS:
        raise HTTPException(400, "Ativo ou intervalo inválido.")

    key = f"{symbol}|{interval}"
    cached = olymp_candle_cache.get(key)
    if cached and time.time() - cached[0] < OLYMP_CANDLE_TTL and len(cached[1]) >= min(n, 20):
        return cached[1][-n:]

    client = await _get_olymp_client(request)
    period = INTERVALS[interval]
    asset_base = _olymp_asset(symbol)
    candidates = [
        asset_base + "_OTC",
        asset_base + "_otc",
        asset_base + "-OTC",
        asset_base,
    ]

    # Cliente por e-mail/senha.
    if hasattr(client, "get_candle"):
        last_error = ""
        for asset in candidates:
            try:
                raw = await asyncio.wait_for(
                    asyncio.to_thread(client.get_candle, asset, period, int(time.time())),
                    timeout=15,
                )

                if isinstance(raw, tuple) and len(raw) >= 2:
                    raw = raw[1]

                out = []
                for item in raw or []:
                    c = _normalize_olymp_candle(item)
                    if c:
                        out.append(c)

                if len(out) >= min(n, 20):
                    out.sort(key=lambda x: x["datetime"])
                    olymp_candle_cache[key] = (time.time(), out)
                    return out[-n:]
            except Exception as exc:
                last_error = str(exc)

        if cached and time.time() - cached[0] < 90:
            return cached[1][-n:]

        raise HTTPException(
            503,
            "Olymptrade OTC sem candles pelo login. "
            + (last_error[:180] if last_error else "Ativo OTC não disponível.")
        )

    last_error = ""
    for asset in candidates:
        try:
            market_api = getattr(client, "market", None)
            if market_api is None:
                raise RuntimeError("Módulo market não encontrado no cliente Olymptrade.")

            getter = getattr(market_api, "get_candles", None)
            if getter is None:
                raise RuntimeError("Método market.get_candles não encontrado.")

            data = getter(asset, size=period, count=max(100, min(150, int(n))))
            if asyncio.iscoroutine(data):
                data = await asyncio.wait_for(data, timeout=15)

            out = []
            for item in data or []:
                c = _normalize_olymp_candle(item)
                if c:
                    out.append(c)

            if len(out) >= min(n, 20):
                out.sort(key=lambda x: x["datetime"])
                olymp_candle_cache[key] = (time.time(), out)
                return out[-n:]
        except Exception as exc:
            last_error = str(exc)

    if cached and time.time() - cached[0] < 90:
        return cached[1][-n:]

    raise HTTPException(
        503,
        "Olymptrade OTC sem candles. " + (last_error[:180] if last_error else "Ativo OTC não disponível.")
    )


def ema(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    e = sum(values[:period]) / period
    for x in values[period:]:
        e = x * k + e * (1 - k)
    return e


def rsi(values, period=14):
    if len(values) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    return 100 - 100 / (1 + avg_gain / avg_loss)


def sma(values, period):
    return sum(values[-period:]) / period if len(values) >= period else None


def stdev(values, period):
    if len(values) < period:
        return None
    m = sum(values[-period:]) / period
    return (sum((x - m) ** 2 for x in values[-period:]) / period) ** 0.5


def bollinger(values, period=20, deviation=2.0):
    mid = sma(values, period)
    sd = stdev(values, period)
    if mid is None or sd is None:
        return None
    return {"middle": mid, "upper": mid + deviation * sd, "lower": mid - deviation * sd}


def stochastic(cs, k_period=14, smooth_k=3, smooth_d=3):
    if len(cs) < k_period + smooth_k + smooth_d:
        return None
    raw = []
    for i in range(k_period - 1, len(cs)):
        w = cs[i - k_period + 1:i + 1]
        hh = max(c["high"] for c in w)
        ll = min(c["low"] for c in w)
        den = hh - ll
        raw.append(50.0 if den == 0 else (cs[i]["close"] - ll) / den * 100)
    kline = []
    for i in range(smooth_k - 1, len(raw)):
        kline.append(sum(raw[i - smooth_k + 1:i + 1]) / smooth_k)
    dline = []
    for i in range(smooth_d - 1, len(kline)):
        dline.append(sum(kline[i - smooth_d + 1:i + 1]) / smooth_d)
    if len(kline) < 2 or len(dline) < 2:
        return None
    return {
        "k": kline[-1], "d": dline[-1],
        "k_prev": kline[-2], "d_prev": dline[-2],
        "cross_up": kline[-2] <= dline[-2] and kline[-1] > dline[-1],
        "cross_down": kline[-2] >= dline[-2] and kline[-1] < dline[-1],
    }


def sideways_filter(values):
    if len(values) < 25:
        return False, 0.0
    e5, e20 = ema(values, 5), ema(values, 20)
    if e5 is None or e20 is None:
        return False, 0.0
    separation = abs(e5 - e20) / max(abs(e20), 1e-12) * 100
    recent = values[-10:]
    total_move = sum(abs(recent[i] - recent[i - 1]) for i in range(1, len(recent)))
    net_move = abs(recent[-1] - recent[0])
    efficiency = net_move / max(total_move, 1e-12)
    score = clamp(100 - separation * 250 - efficiency * 70, 0, 100)
    return separation < 0.12 and efficiency < 0.55, round(score, 1)


def wick_info(c):
    rng = max(c["high"] - c["low"], 1e-12)
    upper = c["high"] - max(c["open"], c["close"])
    lower = min(c["open"], c["close"]) - c["low"]
    return {
        "call_wick": lower / rng,
        "put_wick": upper / rng,
        "body_ratio": abs(c["close"] - c["open"]) / rng,
    }


def bollinger_stochastic(cs):
    if len(cs) < 45:
        return {"direction": "NEUTRO", "confidence": 0, "confirmed": False,
                "reason": "Poucos candles fechados.", "strategy": "Bollinger 20/2 + Estocástico 14,3,3"}

    closes = [c["close"] for c in cs]
    bb = bollinger(closes, 20, 2)
    old_bb = bollinger(closes[:-1], 20, 2)
    st = stochastic(cs, 14, 3, 3)
    if not bb or not old_bb or not st:
        return {"direction": "NEUTRO", "confidence": 0, "confirmed": False,
                "reason": "Indicadores insuficientes.", "strategy": "Bollinger 20/2 + Estocástico 14,3,3"}

    last = cs[-1]
    call_break = last["close"] < bb["lower"] and cs[-2]["close"] >= old_bb["lower"]
    put_break = last["close"] > bb["upper"] and cs[-2]["close"] <= old_bb["upper"]
    sideways, side_score = sideways_filter(closes)
    wi = wick_info(last)

    call_ok = call_break and st["k"] <= 20 and st["d"] <= 20 and st["cross_up"]
    put_ok = put_break and st["k"] >= 80 and st["d"] >= 80 and st["cross_down"]

    call_score = 0
    put_score = 0
    call_reasons, put_reasons = [], []

    if call_break:
        call_score += 5
        call_reasons.append("rompimento abaixo da banda inferior")
    if put_break:
        put_score += 5
        put_reasons.append("rompimento acima da banda superior")
    if st["k"] <= 20 and st["d"] <= 20:
        call_score += 2
        call_reasons.append("Estocástico em sobrevenda")
    if st["k"] >= 80 and st["d"] >= 80:
        put_score += 2
        put_reasons.append("Estocástico em sobrecompra")
    if st["cross_up"]:
        call_score += 2
        call_reasons.append("cruzamento para cima")
    if st["cross_down"]:
        put_score += 2
        put_reasons.append("cruzamento para baixo")
    if sideways:
        call_score += 1
        put_score += 1
    else:
        call_score -= 1
        put_score -= 1
    if wi["call_wick"] >= 0.25:
        call_score += 1
        call_reasons.append("pavio inferior de rejeição")
    if wi["put_wick"] >= 0.25:
        put_score += 1
        put_reasons.append("pavio superior de rejeição")

    if call_ok:
        conf = 65 + call_score * 3
        if not sideways:
            conf = min(conf, 72)
        return {"direction": "CALL", "confidence": round(clamp(conf, 65, 96), 1), "confirmed": True,
                "reason": "; ".join(call_reasons), "strategy": "Bollinger 20/2 + Estocástico 14,3,3",
                "sideways": sideways, "sideways_score": side_score,
                "stochastic": {"k": round(st["k"], 2), "d": round(st["d"], 2)}, "bollinger": bb}

    if put_ok:
        conf = 65 + put_score * 3
        if not sideways:
            conf = min(conf, 72)
        return {"direction": "PUT", "confidence": round(clamp(conf, 65, 96), 1), "confirmed": True,
                "reason": "; ".join(put_reasons), "strategy": "Bollinger 20/2 + Estocástico 14,3,3",
                "sideways": sideways, "sideways_score": side_score,
                "stochastic": {"k": round(st["k"], 2), "d": round(st["d"], 2)}, "bollinger": bb}

    return {"direction": "NEUTRO", "confidence": round(clamp(max(call_score, put_score) * 5 + 35, 0, 70), 1),
            "confirmed": False, "reason": "Sem confluência completa de Bollinger + Estocástico.",
            "strategy": "Bollinger 20/2 + Estocástico 14,3,3", "sideways": sideways,
            "sideways_score": side_score, "stochastic": {"k": round(st["k"], 2), "d": round(st["d"], 2)},
            "bollinger": bb}




def ema_series(values, period):
    if len(values) < period:
        return []
    k = 2 / (period + 1)
    out = [sum(values[:period]) / period]
    for x in values[period:]:
        out.append(x * k + out[-1] * (1 - k))
    return out


def atr(cs, period=14):
    if len(cs) < period + 1:
        return None
    trs = []
    for i in range(1, len(cs)):
        h, l, pc = cs[i]["high"], cs[i]["low"], cs[i-1]["close"]
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    return sum(trs[-period:]) / period if len(trs) >= period else None









def _macd_snapshot(closes, fast_period=12, slow_period=26, signal_period=9):
    """Retorna MACD, sinal, histograma e cruzamentos recentes."""
    if len(closes) < slow_period + signal_period + 4:
        return None

    fast = ema_series(closes, fast_period)
    slow = ema_series(closes, slow_period)
    if not fast or not slow:
        return None

    # Alinha as séries pelo final, suficiente para leitura dos pontos mais recentes.
    common = min(len(fast), len(slow))
    macd_line = [fast[-common + i] - slow[-common + i] for i in range(common)]
    signal_line = ema_series(macd_line, signal_period)
    if len(signal_line) < 3:
        return None

    macd_aligned = macd_line[-len(signal_line):]
    hist = [m - s for m, s in zip(macd_aligned, signal_line)]
    if len(hist) < 3:
        return None

    m_prev, m_now = macd_aligned[-2], macd_aligned[-1]
    s_prev, s_now = signal_line[-2], signal_line[-1]
    h_prev2, h_prev, h_now = hist[-3], hist[-2], hist[-1]

    return {
        "macd": m_now,
        "signal": s_now,
        "hist": h_now,
        "hist_prev": h_prev,
        "cross_up": m_prev <= s_prev and m_now > s_now,
        "cross_down": m_prev >= s_prev and m_now < s_now,
        "hist_improving_up": h_now > h_prev and h_prev >= h_prev2,
        "hist_improving_down": h_now < h_prev and h_prev <= h_prev2,
        # Aproximação de "barras caminhando para zero".
        "hist_toward_zero_up": h_prev < 0 and h_now > h_prev,
        "hist_toward_zero_down": h_prev > 0 and h_now < h_prev,
    }


def _rsi_series(values, period=14):
    out = []
    for i in range(period + 1, len(values) + 1):
        out.append(rsi(values[:i], period))
    return out


def _rsi_divergence(cs, lookback=18, period=14):
    """
    Detecta divergência simples nos dois extremos recentes:
    bullish: preço faz fundo menor e RSI fundo maior;
    bearish: preço faz topo maior e RSI topo menor.
    """
    if len(cs) < max(lookback + period + 2, 35):
        return {"bullish": False, "bearish": False}

    sample = cs[-(lookback + period + 3):]
    closes = [c["close"] for c in sample]
    rs = _rsi_series(closes, period)
    if len(rs) < lookback:
        return {"bullish": False, "bearish": False}

    # Mapeia RSI de volta aos candles finais.
    offset = len(sample) - len(rs)
    lows = []
    highs = []
    for i in range(2, len(sample) - 2):
        if sample[i]["low"] <= sample[i-1]["low"] and sample[i]["low"] <= sample[i+1]["low"]:
            ri = i - offset
            if 0 <= ri < len(rs) and rs[ri] is not None:
                lows.append((i, sample[i]["low"], rs[ri]))
        if sample[i]["high"] >= sample[i-1]["high"] and sample[i]["high"] >= sample[i+1]["high"]:
            ri = i - offset
            if 0 <= ri < len(rs) and rs[ri] is not None:
                highs.append((i, sample[i]["high"], rs[ri]))

    bullish = False
    bearish = False

    if len(lows) >= 2:
        a, b = lows[-2], lows[-1]
        bullish = b[1] < a[1] and b[2] > a[2]

    if len(highs) >= 2:
        a, b = highs[-2], highs[-1]
        bearish = b[1] > a[1] and b[2] < a[2]

    return {"bullish": bullish, "bearish": bearish}


def _cluster_levels(points, tolerance):
    """Agrupa níveis próximos e exige pelo menos dois toques."""
    if not points:
        return []

    points = sorted(float(x) for x in points)
    clusters = []

    for p in points:
        placed = False
        for c in clusters:
            center = sum(c) / len(c)
            if abs(p - center) <= tolerance:
                c.append(p)
                placed = True
                break
        if not placed:
            clusters.append([p])

    levels = []
    for c in clusters:
        if len(c) >= 2:
            levels.append({
                "price": sum(c) / len(c),
                "touches": len(c),
            })
    return levels


def _support_resistance_levels(cs, timeframe_name):
    """
    Marca zonas por pivôs H1/H4 e só aceita níveis com 2+ toques,
    conforme a estratégia enviada.
    """
    if not cs or len(cs) < 25:
        return {"supports": [], "resistances": [], "tolerance": 0.0}

    recent = cs[-100:]
    a = atr(recent, 14)
    price = recent[-1]["close"]
    base_tol = (a * 0.28) if a else abs(price) * 0.0007
    if timeframe_name == "4h":
        base_tol *= 1.15
    tolerance = max(base_tol, abs(price) * 0.00025)

    lows, highs = [], []
    for i in range(2, len(recent) - 2):
        c = recent[i]
        if c["low"] <= min(recent[i-1]["low"], recent[i-2]["low"], recent[i+1]["low"], recent[i+2]["low"]):
            lows.append(c["low"])
        if c["high"] >= max(recent[i-1]["high"], recent[i-2]["high"], recent[i+1]["high"], recent[i+2]["high"]):
            highs.append(c["high"])

    supports = _cluster_levels(lows, tolerance)
    resistances = _cluster_levels(highs, tolerance)

    return {
        "supports": supports,
        "resistances": resistances,
        "tolerance": tolerance,
    }


def htf_sr_rsi_macd_strategy(cs, h1=None, h4=None, interval="5min"):
    """
    Estratégia:
    - zonas de suporte/resistência em H1/H4;
    - gatilho em M5/M15;
    - RSI 14 (30/70);
    - MACD 12,26,9;
    - rejeita candle de força/elefantíase;
    - divergência de RSI aumenta a confiança.
    """
    strategy_name = "S/R H1-H4 + RSI 14 + MACD 12/26/9"

    if interval not in ("5min", "15min"):
        return {
            "direction": "NEUTRO", "confidence": 0, "confirmed": False,
            "reason": "Estratégia H1/H4 ativa somente nos gatilhos M5 e M15.",
            "strategy": strategy_name,
        }

    if len(cs) < 55 or not h1 or not h4 or len(h1) < 25 or len(h4) < 25:
        return {
            "direction": "NEUTRO", "confidence": 0, "confirmed": False,
            "reason": "Aguardando candles H1/H4 suficientes.",
            "strategy": strategy_name,
        }

    closes = [c["close"] for c in cs]
    last = cs[-1]
    price = last["close"]

    r_now = rsi(closes, 14)
    macd = _macd_snapshot(closes, 12, 26, 9)
    a = atr(cs, 14)

    if r_now is None or not macd or not a:
        return {
            "direction": "NEUTRO", "confidence": 0, "confirmed": False,
            "reason": "RSI/MACD/ATR ainda sem dados suficientes.",
            "strategy": strategy_name,
        }

    h1_levels = _support_resistance_levels(h1, "1h")
    h4_levels = _support_resistance_levels(h4, "4h")

    supports = [
        ("H1", x["price"], x["touches"], h1_levels["tolerance"]) for x in h1_levels["supports"]
    ] + [
        ("H4", x["price"], x["touches"], h4_levels["tolerance"]) for x in h4_levels["supports"]
    ]
    resistances = [
        ("H1", x["price"], x["touches"], h1_levels["tolerance"]) for x in h1_levels["resistances"]
    ] + [
        ("H4", x["price"], x["touches"], h4_levels["tolerance"]) for x in h4_levels["resistances"]
    ]

    nearest_support = min(
        (x for x in supports if x[1] <= price + x[3]),
        key=lambda x: abs(price - x[1]),
        default=None,
    )
    nearest_resistance = min(
        (x for x in resistances if x[1] >= price - x[3]),
        key=lambda x: abs(price - x[1]),
        default=None,
    )

    touch_support = bool(nearest_support and abs(price - nearest_support[1]) <= max(nearest_support[3], a * 0.35))
    touch_resistance = bool(nearest_resistance and abs(price - nearest_resistance[1]) <= max(nearest_resistance[3], a * 0.35))

    wi = wick_info(last)
    body = abs(last["close"] - last["open"])
    elephant = body >= 1.6 * a and wi["body_ratio"] >= 0.68

    div = _rsi_divergence(cs, 18, 14)

    # "Abaixo de 30 ou muito próximo": usamos 32/68 como margem pequena.
    rsi_call = r_now <= 32
    rsi_put = r_now >= 68

    macd_call = macd["cross_up"] and (macd["hist_toward_zero_up"] or macd["hist_improving_up"])
    macd_put = macd["cross_down"] and (macd["hist_toward_zero_down"] or macd["hist_improving_down"])

    if elephant:
        return {
            "direction": "NEUTRO", "confidence": 25, "confirmed": False,
            "reason": "Filtro de segurança: candle de força/elefantíase detectado na zona.",
            "strategy": strategy_name,
            "rsi14": round(r_now, 2),
            "elephant_candle": True,
        }

    if touch_support and rsi_call and macd_call:
        tf, level, touches, _ = nearest_support
        conf = 84
        if tf == "H4":
            conf += 4
        if div["bullish"]:
            conf += 5
        if wi["call_wick"] >= 0.35:
            conf += 3
        return {
            "direction": "CALL",
            "confidence": round(clamp(conf, 80, 97), 1),
            "confirmed": True,
            "reason": (
                f"Preço em suporte {tf} ({touches} toques); RSI 14 em {r_now:.1f}; "
                "MACD cruzou para cima com histograma reagindo em direção ao zero"
                + ("; divergência altista de RSI" if div["bullish"] else "")
                + "."
            ),
            "strategy": strategy_name,
            "rsi14": round(r_now, 2),
            "level": round(level, 8),
            "level_timeframe": tf,
            "level_touches": touches,
            "rsi_divergence": "BULLISH" if div["bullish"] else "NONE",
            "elephant_candle": False,
        }

    if touch_resistance and rsi_put and macd_put:
        tf, level, touches, _ = nearest_resistance
        conf = 84
        if tf == "H4":
            conf += 4
        if div["bearish"]:
            conf += 5
        if wi["put_wick"] >= 0.35:
            conf += 3
        return {
            "direction": "PUT",
            "confidence": round(clamp(conf, 80, 97), 1),
            "confirmed": True,
            "reason": (
                f"Preço em resistência {tf} ({touches} toques); RSI 14 em {r_now:.1f}; "
                "MACD cruzou para baixo com histograma reagindo em direção ao zero"
                + ("; divergência baixista de RSI" if div["bearish"] else "")
                + "."
            ),
            "strategy": strategy_name,
            "rsi14": round(r_now, 2),
            "level": round(level, 8),
            "level_timeframe": tf,
            "level_touches": touches,
            "rsi_divergence": "BEARISH" if div["bearish"] else "NONE",
            "elephant_candle": False,
        }

    reasons = []
    if not touch_support and not touch_resistance:
        reasons.append("preço fora das zonas H1/H4")
    if touch_support and not rsi_call:
        reasons.append(f"RSI {r_now:.1f} não está em sobrevenda")
    if touch_resistance and not rsi_put:
        reasons.append(f"RSI {r_now:.1f} não está em sobrecompra")
    if (touch_support and rsi_call and not macd_call) or (touch_resistance and rsi_put and not macd_put):
        reasons.append("MACD ainda não confirmou o cruzamento")

    return {
        "direction": "NEUTRO",
        "confidence": 55 if (touch_support or touch_resistance) else 35,
        "confirmed": False,
        "reason": "; ".join(reasons) if reasons else "Sem confluência completa H1/H4 + RSI + MACD.",
        "strategy": strategy_name,
        "rsi14": round(r_now, 2),
        "elephant_candle": False,
        "rsi_divergence": (
            "BULLISH" if div["bullish"] else "BEARISH" if div["bearish"] else "NONE"
        ),
    }



def otc_bb_rsi_rejection(cs):
    """Reversão OTC: Banda de Bollinger + RSI + rejeição por pavio."""
    if len(cs) < 35:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Poucos candles.","strategy":"OTC BB + RSI + rejeição"}

    closes = [c["close"] for c in cs]
    bb = bollinger(closes, 20, 2)
    r = rsi(closes, 7)
    if not bb or r is None:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Indicadores insuficientes.","strategy":"OTC BB + RSI + rejeição"}

    last = cs[-1]
    prev = cs[-2]
    wi = wick_info(last)

    call = (
        (last["low"] <= bb["lower"] or prev["low"] <= bb["lower"])
        and last["close"] > bb["lower"]
        and r <= 34
        and wi["call_wick"] >= 0.42
        and last["close"] >= last["open"]
    )
    put = (
        (last["high"] >= bb["upper"] or prev["high"] >= bb["upper"])
        and last["close"] < bb["upper"]
        and r >= 66
        and wi["put_wick"] >= 0.42
        and last["close"] <= last["open"]
    )

    if call:
        return {"direction":"CALL","confidence":84,"confirmed":True,
                "reason":"Rejeição da banda inferior com RSI baixo e pavio comprador.",
                "strategy":"OTC BB + RSI + rejeição"}
    if put:
        return {"direction":"PUT","confidence":84,"confirmed":True,
                "reason":"Rejeição da banda superior com RSI alto e pavio vendedor.",
                "strategy":"OTC BB + RSI + rejeição"}

    return {"direction":"NEUTRO","confidence":52,"confirmed":False,
            "reason":"Sem rejeição completa nas bandas.","strategy":"OTC BB + RSI + rejeição"}


def otc_stochastic_reversal(cs):
    """Reversão curta OTC com Estocástico e confirmação da vela."""
    if len(cs) < 35:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Poucos candles.","strategy":"OTC Estocástico reversão"}

    st = stochastic(cs, 14, 3, 3)
    if not st:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Estocástico insuficiente.","strategy":"OTC Estocástico reversão"}

    last = cs[-1]
    wi = wick_info(last)

    call = (
        st["k"] <= 24 and st["d"] <= 26 and st["cross_up"]
        and last["close"] > last["open"]
        and wi["call_wick"] >= 0.25
    )
    put = (
        st["k"] >= 76 and st["d"] >= 74 and st["cross_down"]
        and last["close"] < last["open"]
        and wi["put_wick"] >= 0.25
    )

    if call:
        return {"direction":"CALL","confidence":82,"confirmed":True,
                "reason":"Estocástico saiu da sobrevenda com vela compradora.",
                "strategy":"OTC Estocástico reversão"}
    if put:
        return {"direction":"PUT","confidence":82,"confirmed":True,
                "reason":"Estocástico saiu da sobrecompra com vela vendedora.",
                "strategy":"OTC Estocástico reversão"}

    return {"direction":"NEUTRO","confidence":49,"confirmed":False,
            "reason":"Estocástico sem reversão confirmada.","strategy":"OTC Estocástico reversão"}


def otc_ema_pullback(cs):
    """Continuação OTC após pullback curto em EMA 9/21."""
    if len(cs) < 35:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Poucos candles.","strategy":"OTC EMA pullback"}

    closes = [c["close"] for c in cs]
    e9 = ema(closes, 9)
    e21 = ema(closes, 21)
    r = rsi(closes, 7)
    if None in (e9, e21, r):
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Indicadores insuficientes.","strategy":"OTC EMA pullback"}

    last = cs[-1]
    prev = cs[-2]
    spread = abs(e9 - e21)
    avg_range = sum(max(1e-12, c["high"]-c["low"]) for c in cs[-10:]) / 10.0
    near_fast = abs(prev["close"] - e9) <= max(avg_range * .45, spread * 1.2)

    call = (
        e9 > e21 and near_fast
        and last["close"] > last["open"]
        and last["close"] > e9
        and 52 <= r <= 69
    )
    put = (
        e9 < e21 and near_fast
        and last["close"] < last["open"]
        and last["close"] < e9
        and 31 <= r <= 48
    )

    if call:
        return {"direction":"CALL","confidence":81,"confirmed":True,
                "reason":"Pullback curto na EMA 9 dentro de tendência compradora.",
                "strategy":"OTC EMA pullback"}
    if put:
        return {"direction":"PUT","confidence":81,"confirmed":True,
                "reason":"Pullback curto na EMA 9 dentro de tendência vendedora.",
                "strategy":"OTC EMA pullback"}

    return {"direction":"NEUTRO","confidence":48,"confirmed":False,
            "reason":"Sem pullback alinhado à tendência curta.","strategy":"OTC EMA pullback"}



def otc_two_candle_color_continuation(cs):
    """
    Estratégia OTC de continuação por cor:
    - 2 velas verdes consecutivas -> procura CALL na 3ª vela;
    - 2 velas vermelhas consecutivas -> procura PUT na 3ª vela.

    A cor sozinha NÃO libera a entrada. O padrão precisa passar por filtros de
    corpo/range, EMA 9/21, RSI 7 e continuidade do fechamento. Depois disso,
    o motor OTC ainda exige confluência com pelo menos outra estratégia.
    """
    if len(cs) < 35:
        return {
            "direction": "NEUTRO",
            "confidence": 0,
            "confirmed": False,
            "reason": "Poucos candles para validar o padrão de 2 cores.",
            "strategy": "OTC 2 velas mesma cor",
        }

    c1 = cs[-2]
    c2 = cs[-1]

    def _metrics(c):
        op = float(c["open"])
        cl = float(c["close"])
        hi = float(c["high"])
        lo = float(c["low"])
        rng = max(1e-12, hi - lo)
        body = abs(cl - op)
        return {
            "green": cl > op,
            "red": cl < op,
            "body_ratio": body / rng,
            "range": rng,
        }

    m1 = _metrics(c1)
    m2 = _metrics(c2)

    recent = cs[-12:-2]
    avg_range = (
        sum(max(1e-12, float(c["high"]) - float(c["low"])) for c in recent)
        / max(1, len(recent))
    )

    # Evita doji, velas muito fracas ou explosões anormais.
    body_ok = m1["body_ratio"] >= 0.45 and m2["body_ratio"] >= 0.45
    range_ok = (
        0.45 * avg_range <= m1["range"] <= 1.90 * avg_range
        and 0.45 * avg_range <= m2["range"] <= 1.90 * avg_range
    )

    closes = [float(c["close"]) for c in cs]
    e9 = ema(closes, 9)
    e21 = ema(closes, 21)
    r = rsi(closes, 7)

    if e9 is None or e21 is None or r is None:
        return {
            "direction": "NEUTRO",
            "confidence": 0,
            "confirmed": False,
            "reason": "Indicadores insuficientes para validar o padrão de 2 cores.",
            "strategy": "OTC 2 velas mesma cor",
        }

    two_green = m1["green"] and m2["green"]
    two_red = m1["red"] and m2["red"]

    # A segunda vela precisa mostrar continuação real do movimento.
    green_progress = float(c2["close"]) > float(c1["close"])
    red_progress = float(c2["close"]) < float(c1["close"])

    trend_call = e9 > e21 and float(c2["close"]) >= e9
    trend_put = e9 < e21 and float(c2["close"]) <= e9

    # Evita seguir movimento já excessivamente esticado.
    rsi_call_ok = 50 <= r <= 72
    rsi_put_ok = 28 <= r <= 50

    call = (
        two_green
        and body_ok
        and range_ok
        and green_progress
        and trend_call
        and rsi_call_ok
    )

    put = (
        two_red
        and body_ok
        and range_ok
        and red_progress
        and trend_put
        and rsi_put_ok
    )

    if call:
        return {
            "direction": "CALL",
            "confidence": 84,
            "confirmed": True,
            "reason": (
                "Duas velas verdes consecutivas com corpos firmes, "
                "continuidade, EMA 9/21 e RSI favoráveis à compra."
            ),
            "strategy": "OTC 2 velas mesma cor",
        }

    if put:
        return {
            "direction": "PUT",
            "confidence": 84,
            "confirmed": True,
            "reason": (
                "Duas velas vermelhas consecutivas com corpos firmes, "
                "continuidade, EMA 9/21 e RSI favoráveis à venda."
            ),
            "strategy": "OTC 2 velas mesma cor",
        }

    if two_green or two_red:
        return {
            "direction": "NEUTRO",
            "confidence": 58,
            "confirmed": False,
            "reason": (
                "Duas velas da mesma cor foram encontradas, mas a análise "
                "de tendência, força, RSI ou qualidade das velas não confirmou."
            ),
            "strategy": "OTC 2 velas mesma cor",
        }

    return {
        "direction": "NEUTRO",
        "confidence": 45,
        "confirmed": False,
        "reason": "Sem sequência válida de duas velas da mesma cor.",
        "strategy": "OTC 2 velas mesma cor",
    }


def otc_exhaustion_reversal(cs):
    """Exaustão OTC: sequência de velas + rejeição no extremo."""
    if len(cs) < 12:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Poucos candles.","strategy":"OTC exaustão 3 velas"}

    seq = cs[-4:-1]
    last = cs[-1]
    wi = wick_info(last)
    closes = [c["close"] for c in cs]
    r = rsi(closes, 7)

    three_down = all(c["close"] < c["open"] for c in seq)
    three_up = all(c["close"] > c["open"] for c in seq)

    call = (
        three_down and last["close"] > last["open"]
        and wi["call_wick"] >= 0.38
        and r is not None and r <= 38
    )
    put = (
        three_up and last["close"] < last["open"]
        and wi["put_wick"] >= 0.38
        and r is not None and r >= 62
    )

    if call:
        return {"direction":"CALL","confidence":80,"confirmed":True,
                "reason":"Sequência vendedora mostrou exaustão e rejeição compradora.",
                "strategy":"OTC exaustão 3 velas"}
    if put:
        return {"direction":"PUT","confidence":80,"confirmed":True,
                "reason":"Sequência compradora mostrou exaustão e rejeição vendedora.",
                "strategy":"OTC exaustão 3 velas"}

    return {"direction":"NEUTRO","confidence":46,"confirmed":False,
            "reason":"Sem exaustão confirmada.","strategy":"OTC exaustão 3 velas"}


def otc_local_sr_rejection(cs):
    """Suporte/resistência local OTC usando extremos recentes e rejeição."""
    if len(cs) < 30:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Poucos candles.","strategy":"OTC suporte/resistência local"}

    recent = cs[-21:-1]
    last = cs[-1]
    support = min(c["low"] for c in recent)
    resistance = max(c["high"] for c in recent)
    span = max(1e-12, resistance - support)
    tolerance = span * 0.08
    wi = wick_info(last)

    call = (
        last["low"] <= support + tolerance
        and last["close"] > last["open"]
        and wi["call_wick"] >= 0.40
        and last["close"] > support
    )
    put = (
        last["high"] >= resistance - tolerance
        and last["close"] < last["open"]
        and wi["put_wick"] >= 0.40
        and last["close"] < resistance
    )

    if call:
        return {"direction":"CALL","confidence":83,"confirmed":True,
                "reason":"Rejeição compradora em suporte local recente.",
                "strategy":"OTC suporte/resistência local"}
    if put:
        return {"direction":"PUT","confidence":83,"confirmed":True,
                "reason":"Rejeição vendedora em resistência local recente.",
                "strategy":"OTC suporte/resistência local"}

    return {"direction":"NEUTRO","confidence":50,"confirmed":False,
            "reason":"Preço fora de zona local de rejeição.","strategy":"OTC suporte/resistência local"}


def otc_micro_macd(cs):
    """Momentum curto OTC com MACD 6/13/5 e filtro RSI."""
    if len(cs) < 35:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Poucos candles.","strategy":"OTC MACD curto 6/13/5"}

    closes = [c["close"] for c in cs]
    snap = _macd_snapshot(closes, 6, 13, 5)
    r = rsi(closes, 7)
    if not snap or r is None:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"MACD curto insuficiente.","strategy":"OTC MACD curto 6/13/5"}

    call = snap.get("cross_up") and snap.get("hist", 0) > 0 and 50 < r < 70
    put = snap.get("cross_down") and snap.get("hist", 0) < 0 and 30 < r < 50

    if call:
        return {"direction":"CALL","confidence":80,"confirmed":True,
                "reason":"MACD curto cruzou para cima com RSI favorável.",
                "strategy":"OTC MACD curto 6/13/5"}
    if put:
        return {"direction":"PUT","confidence":80,"confirmed":True,
                "reason":"MACD curto cruzou para baixo com RSI favorável.",
                "strategy":"OTC MACD curto 6/13/5"}

    return {"direction":"NEUTRO","confidence":47,"confirmed":False,
            "reason":"MACD curto sem confirmação.","strategy":"OTC MACD curto 6/13/5"}



def _otc_candle_context(cs):
    """Contexto curto para validar padrões de vela OTC."""
    if len(cs) < 25:
        return None

    recent = cs[-21:-1]
    support = min(c["low"] for c in recent)
    resistance = max(c["high"] for c in recent)
    span = max(1e-12, resistance - support)
    tolerance = span * 0.10

    closes = [c["close"] for c in cs]
    r7 = rsi(closes, 7)
    e9 = ema(closes, 9)
    e21 = ema(closes, 21)

    return {
        "support": support,
        "resistance": resistance,
        "tolerance": tolerance,
        "rsi7": r7,
        "ema9": e9,
        "ema21": e21,
    }


def otc_candle_engulfing(cs):
    """Engolfo de alta/baixa com contexto de extremo local."""
    if len(cs) < 25:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Poucos candles.","strategy":"OTC Engolfo"}

    ctx = _otc_candle_context(cs)
    if not ctx:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Contexto insuficiente.","strategy":"OTC Engolfo"}

    a, b = cs[-2], cs[-1]
    a_body = abs(a["close"] - a["open"])
    b_body = abs(b["close"] - b["open"])

    bull = (
        a["close"] < a["open"]
        and b["close"] > b["open"]
        and b["open"] <= a["close"]
        and b["close"] >= a["open"]
        and b_body >= a_body * 1.05
        and b["low"] <= ctx["support"] + ctx["tolerance"]
        and ctx["rsi7"] is not None
        and ctx["rsi7"] <= 44
    )

    bear = (
        a["close"] > a["open"]
        and b["close"] < b["open"]
        and b["open"] >= a["close"]
        and b["close"] <= a["open"]
        and b_body >= a_body * 1.05
        and b["high"] >= ctx["resistance"] - ctx["tolerance"]
        and ctx["rsi7"] is not None
        and ctx["rsi7"] >= 56
    )

    if bull:
        return {"direction":"CALL","confidence":86,"confirmed":True,
                "reason":"Engolfo comprador em região de suporte local com RSI favorável.",
                "strategy":"OTC Engolfo"}
    if bear:
        return {"direction":"PUT","confidence":86,"confirmed":True,
                "reason":"Engolfo vendedor em região de resistência local com RSI favorável.",
                "strategy":"OTC Engolfo"}

    return {"direction":"NEUTRO","confidence":50,"confirmed":False,
            "reason":"Engolfo sem contexto suficiente.","strategy":"OTC Engolfo"}


def otc_candle_hammer_star(cs):
    """Martelo e Shooting Star com pavio dominante e zona local."""
    if len(cs) < 25:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Poucos candles.","strategy":"OTC Martelo/Shooting Star"}

    ctx = _otc_candle_context(cs)
    last = cs[-1]
    wi = wick_info(last)
    body = max(abs(last["close"] - last["open"]), 1e-12)
    lower = min(last["open"], last["close"]) - last["low"]
    upper = last["high"] - max(last["open"], last["close"])

    hammer = (
        lower >= body * 2.0
        and upper <= body * 0.8
        and wi["body_ratio"] <= 0.42
        and last["low"] <= ctx["support"] + ctx["tolerance"]
        and ctx["rsi7"] is not None
        and ctx["rsi7"] <= 42
    )

    shooting = (
        upper >= body * 2.0
        and lower <= body * 0.8
        and wi["body_ratio"] <= 0.42
        and last["high"] >= ctx["resistance"] - ctx["tolerance"]
        and ctx["rsi7"] is not None
        and ctx["rsi7"] >= 58
    )

    if hammer:
        return {"direction":"CALL","confidence":85,"confirmed":True,
                "reason":"Martelo com forte rejeição em suporte local.",
                "strategy":"OTC Martelo/Shooting Star"}
    if shooting:
        return {"direction":"PUT","confidence":85,"confirmed":True,
                "reason":"Shooting Star com forte rejeição em resistência local.",
                "strategy":"OTC Martelo/Shooting Star"}

    return {"direction":"NEUTRO","confidence":49,"confirmed":False,
            "reason":"Sem Martelo/Shooting Star validado no contexto.",
            "strategy":"OTC Martelo/Shooting Star"}


def otc_candle_tweezer(cs):
    """Tweezer Bottom/Top em dois candles próximos do mesmo extremo."""
    if len(cs) < 25:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Poucos candles.","strategy":"OTC Tweezer"}

    ctx = _otc_candle_context(cs)
    a, b = cs[-2], cs[-1]
    local_range = sum(max(1e-12, c["high"] - c["low"]) for c in cs[-10:]) / 10.0
    tol = local_range * 0.16

    bottom = (
        abs(a["low"] - b["low"]) <= tol
        and a["close"] < a["open"]
        and b["close"] > b["open"]
        and min(a["low"], b["low"]) <= ctx["support"] + ctx["tolerance"]
        and ctx["rsi7"] is not None
        and ctx["rsi7"] <= 45
    )

    top = (
        abs(a["high"] - b["high"]) <= tol
        and a["close"] > a["open"]
        and b["close"] < b["open"]
        and max(a["high"], b["high"]) >= ctx["resistance"] - ctx["tolerance"]
        and ctx["rsi7"] is not None
        and ctx["rsi7"] >= 55
    )

    if bottom:
        return {"direction":"CALL","confidence":83,"confirmed":True,
                "reason":"Tweezer Bottom em suporte local.",
                "strategy":"OTC Tweezer"}
    if top:
        return {"direction":"PUT","confidence":83,"confirmed":True,
                "reason":"Tweezer Top em resistência local.",
                "strategy":"OTC Tweezer"}

    return {"direction":"NEUTRO","confidence":47,"confirmed":False,
            "reason":"Sem Tweezer confirmado.","strategy":"OTC Tweezer"}


def otc_candle_morning_evening_star(cs):
    """Morning Star e Evening Star adaptados ao fluxo OTC."""
    if len(cs) < 26:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Poucos candles.","strategy":"OTC Morning/Evening Star"}

    ctx = _otc_candle_context(cs)
    a, b, c = cs[-3], cs[-2], cs[-1]

    a_body = abs(a["close"] - a["open"])
    b_body = abs(b["close"] - b["open"])
    c_body = abs(c["close"] - c["open"])

    mid_a = (a["open"] + a["close"]) / 2.0

    morning = (
        a["close"] < a["open"]
        and a_body > 0
        and b_body <= a_body * 0.55
        and c["close"] > c["open"]
        and c_body >= a_body * 0.55
        and c["close"] > mid_a
        and min(a["low"], b["low"], c["low"]) <= ctx["support"] + ctx["tolerance"]
        and ctx["rsi7"] is not None
        and ctx["rsi7"] <= 46
    )

    evening = (
        a["close"] > a["open"]
        and a_body > 0
        and b_body <= a_body * 0.55
        and c["close"] < c["open"]
        and c_body >= a_body * 0.55
        and c["close"] < mid_a
        and max(a["high"], b["high"], c["high"]) >= ctx["resistance"] - ctx["tolerance"]
        and ctx["rsi7"] is not None
        and ctx["rsi7"] >= 54
    )

    if morning:
        return {"direction":"CALL","confidence":87,"confirmed":True,
                "reason":"Morning Star em zona de suporte com reversão confirmada.",
                "strategy":"OTC Morning/Evening Star"}
    if evening:
        return {"direction":"PUT","confidence":87,"confirmed":True,
                "reason":"Evening Star em zona de resistência com reversão confirmada.",
                "strategy":"OTC Morning/Evening Star"}

    return {"direction":"NEUTRO","confidence":51,"confirmed":False,
            "reason":"Sem Morning/Evening Star confirmado.",
            "strategy":"OTC Morning/Evening Star"}


def otc_candle_pinbar(cs):
    """Pin Bar de rejeição com filtro de posição e RSI."""
    if len(cs) < 25:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Poucos candles.","strategy":"OTC Pin Bar"}

    ctx = _otc_candle_context(cs)
    last = cs[-1]
    wi = wick_info(last)

    bull = (
        wi["call_wick"] >= 0.58
        and wi["body_ratio"] <= 0.32
        and last["low"] <= ctx["support"] + ctx["tolerance"]
        and ctx["rsi7"] is not None
        and ctx["rsi7"] <= 43
    )

    bear = (
        wi["put_wick"] >= 0.58
        and wi["body_ratio"] <= 0.32
        and last["high"] >= ctx["resistance"] - ctx["tolerance"]
        and ctx["rsi7"] is not None
        and ctx["rsi7"] >= 57
    )

    if bull:
        return {"direction":"CALL","confidence":84,"confirmed":True,
                "reason":"Pin Bar comprador rejeitando suporte local.",
                "strategy":"OTC Pin Bar"}
    if bear:
        return {"direction":"PUT","confidence":84,"confirmed":True,
                "reason":"Pin Bar vendedor rejeitando resistência local.",
                "strategy":"OTC Pin Bar"}

    return {"direction":"NEUTRO","confidence":48,"confirmed":False,
            "reason":"Pin Bar sem contexto de reversão.","strategy":"OTC Pin Bar"}


def otc_candle_inside_breakout(cs):
    """Inside Bar + rompimento da máxima/mínima da barra-mãe."""
    if len(cs) < 26:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Poucos candles.","strategy":"OTC Inside Bar rompimento"}

    mother, inside, last = cs[-3], cs[-2], cs[-1]
    closes = [c["close"] for c in cs]
    e9 = ema(closes, 9)
    e21 = ema(closes, 21)

    is_inside = (
        inside["high"] < mother["high"]
        and inside["low"] > mother["low"]
    )

    call = (
        is_inside
        and last["close"] > mother["high"]
        and last["close"] > last["open"]
        and e9 is not None and e21 is not None
        and e9 >= e21
    )

    put = (
        is_inside
        and last["close"] < mother["low"]
        and last["close"] < last["open"]
        and e9 is not None and e21 is not None
        and e9 <= e21
    )

    if call:
        return {"direction":"CALL","confidence":81,"confirmed":True,
                "reason":"Inside Bar rompeu para cima alinhado à tendência curta.",
                "strategy":"OTC Inside Bar rompimento"}
    if put:
        return {"direction":"PUT","confidence":81,"confirmed":True,
                "reason":"Inside Bar rompeu para baixo alinhado à tendência curta.",
                "strategy":"OTC Inside Bar rompimento"}

    return {"direction":"NEUTRO","confidence":46,"confirmed":False,
            "reason":"Inside Bar sem rompimento válido.",
            "strategy":"OTC Inside Bar rompimento"}


def otc_candle_doji_reversal(cs):
    """Doji de indecisão seguido de confirmação direcional."""
    if len(cs) < 26:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Poucos candles.","strategy":"OTC Doji reversão"}

    ctx = _otc_candle_context(cs)
    doji, confirm = cs[-2], cs[-1]
    doji_wi = wick_info(doji)

    is_doji = doji_wi["body_ratio"] <= 0.12

    call = (
        is_doji
        and doji["low"] <= ctx["support"] + ctx["tolerance"]
        and confirm["close"] > confirm["open"]
        and confirm["close"] > doji["high"]
        and ctx["rsi7"] is not None
        and ctx["rsi7"] <= 48
    )

    put = (
        is_doji
        and doji["high"] >= ctx["resistance"] - ctx["tolerance"]
        and confirm["close"] < confirm["open"]
        and confirm["close"] < doji["low"]
        and ctx["rsi7"] is not None
        and ctx["rsi7"] >= 52
    )

    if call:
        return {"direction":"CALL","confidence":82,"confirmed":True,
                "reason":"Doji em suporte seguido de confirmação compradora.",
                "strategy":"OTC Doji reversão"}
    if put:
        return {"direction":"PUT","confidence":82,"confirmed":True,
                "reason":"Doji em resistência seguido de confirmação vendedora.",
                "strategy":"OTC Doji reversão"}

    return {"direction":"NEUTRO","confidence":45,"confirmed":False,
            "reason":"Doji sem confirmação direcional.",
            "strategy":"OTC Doji reversão"}


def _otc_swing_points(cs, left=2, right=2):
    """Detecta pivôs locais simples sem usar candles futuros além da janela disponível."""
    highs, lows = [], []
    n = len(cs)

    for i in range(left, n - right):
        h = cs[i]["high"]
        l = cs[i]["low"]

        if all(h >= cs[j]["high"] for j in range(i-left, i+right+1) if j != i):
            highs.append({"index": i, "price": h, "time": cs[i].get("datetime")})

        if all(l <= cs[j]["low"] for j in range(i-left, i+right+1) if j != i):
            lows.append({"index": i, "price": l, "time": cs[i].get("datetime")})

    return highs, lows


def _otc_cluster_zones(points, tolerance):
    """Agrupa pivôs próximos e mede quantas vezes a região foi respeitada."""
    zones = []

    for p in points:
        price = float(p["price"])
        matched = None

        for z in zones:
            if abs(price - z["price"]) <= tolerance:
                matched = z
                break

        if matched is None:
            zones.append({
                "price": price,
                "touches": 1,
                "last_index": p["index"],
            })
        else:
            t = matched["touches"]
            matched["price"] = (matched["price"] * t + price) / (t + 1)
            matched["touches"] = t + 1
            matched["last_index"] = max(matched["last_index"], p["index"])

    zones.sort(key=lambda z: (z["touches"], z["last_index"]), reverse=True)
    return zones


def otc_market_structure_map(cs):
    """
    Mapeia estrutura OTC: suportes, resistências, regiões fortes e tendência curta.
    Isso é leitura de preço; não presume manipulação real da corretora.
    """
    if len(cs) < 35:
        return None

    sample = cs[-80:] if len(cs) > 80 else cs
    ranges = [max(1e-12, c["high"] - c["low"]) for c in sample[-20:]]
    avg_range = sum(ranges) / len(ranges)
    tolerance = avg_range * 0.45

    highs, lows = _otc_swing_points(sample, 2, 2)
    rz = _otc_cluster_zones(highs, tolerance)
    sz = _otc_cluster_zones(lows, tolerance)

    current = float(sample[-1]["close"])
    supports = [z for z in sz if z["price"] <= current + tolerance]
    resistances = [z for z in rz if z["price"] >= current - tolerance]

    nearest_support = max(supports, key=lambda z: z["price"], default=None)
    nearest_resistance = min(resistances, key=lambda z: z["price"], default=None)

    closes = [c["close"] for c in sample]
    e9 = ema(closes, 9)
    e21 = ema(closes, 21)

    if e9 is not None and e21 is not None and e9 > e21:
        trend = "UP"
    elif e9 is not None and e21 is not None and e9 < e21:
        trend = "DOWN"
    else:
        trend = "FLAT"

    return {
        "avg_range": avg_range,
        "tolerance": tolerance,
        "nearest_support": nearest_support,
        "nearest_resistance": nearest_resistance,
        "strong_supports": [z for z in supports if z["touches"] >= 2][:3],
        "strong_resistances": [z for z in resistances if z["touches"] >= 2][:3],
        "trend": trend,
        "ema9": e9,
        "ema21": e21,
    }


def otc_strong_zone_rejection(cs):
    """Entrada por rejeição em região forte tocada pelo menos duas vezes."""
    smap = otc_market_structure_map(cs)
    if not smap:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Estrutura insuficiente.","strategy":"OTC Região forte"}

    last = cs[-1]
    wi = wick_info(last)
    tol = smap["tolerance"]
    sup = smap["nearest_support"]
    res = smap["nearest_resistance"]

    call = (
        sup is not None
        and sup["touches"] >= 2
        and last["low"] <= sup["price"] + tol
        and last["close"] > sup["price"]
        and wi["call_wick"] >= 0.38
        and last["close"] >= last["open"]
    )

    put = (
        res is not None
        and res["touches"] >= 2
        and last["high"] >= res["price"] - tol
        and last["close"] < res["price"]
        and wi["put_wick"] >= 0.38
        and last["close"] <= last["open"]
    )

    if call:
        return {"direction":"CALL","confidence":86,"confirmed":True,
                "reason":f"Rejeição em suporte forte com {sup['touches']} toques.",
                "strategy":"OTC Região forte"}
    if put:
        return {"direction":"PUT","confidence":86,"confirmed":True,
                "reason":f"Rejeição em resistência forte com {res['touches']} toques.",
                "strategy":"OTC Região forte"}

    return {"direction":"NEUTRO","confidence":52,"confirmed":False,
            "reason":"Sem rejeição válida em região forte.","strategy":"OTC Região forte"}


def otc_liquidity_sweep(cs):
    """
    Detecta sweep/falso rompimento de máxima ou mínima recente.
    É um proxy técnico de caça de liquidez; não afirma manipulação real do gráfico.
    """
    if len(cs) < 25:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Poucos candles.","strategy":"OTC Sweep de liquidez"}

    last = cs[-1]
    prev = cs[-13:-1]
    recent_high = max(c["high"] for c in prev)
    recent_low = min(c["low"] for c in prev)
    wi = wick_info(last)

    bull_sweep = (
        last["low"] < recent_low
        and last["close"] > recent_low
        and last["close"] > last["open"]
        and wi["call_wick"] >= 0.42
    )

    bear_sweep = (
        last["high"] > recent_high
        and last["close"] < recent_high
        and last["close"] < last["open"]
        and wi["put_wick"] >= 0.42
    )

    if bull_sweep:
        return {"direction":"CALL","confidence":88,"confirmed":True,
                "reason":"Varreu mínima recente e fechou novamente acima: possível sweep de liquidez.",
                "strategy":"OTC Sweep de liquidez"}
    if bear_sweep:
        return {"direction":"PUT","confidence":88,"confirmed":True,
                "reason":"Varreu máxima recente e fechou novamente abaixo: possível sweep de liquidez.",
                "strategy":"OTC Sweep de liquidez"}

    return {"direction":"NEUTRO","confidence":50,"confirmed":False,
            "reason":"Sem falso rompimento/sweep confirmado.","strategy":"OTC Sweep de liquidez"}


def otc_break_retest(cs):
    """Rompimento de região recente seguido de reteste e rejeição."""
    if len(cs) < 30:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Poucos candles.","strategy":"OTC Rompimento + reteste"}

    last = cs[-1]
    prev = cs[-2]
    base = cs[-16:-2]

    hi = max(c["high"] for c in base)
    lo = min(c["low"] for c in base)

    avg_range = sum(max(1e-12, c["high"]-c["low"]) for c in cs[-12:]) / 12.0
    tol = avg_range * 0.30
    wi = wick_info(last)

    call = (
        prev["close"] > hi
        and last["low"] <= hi + tol
        and last["close"] > hi
        and last["close"] > last["open"]
        and wi["call_wick"] >= 0.20
    )

    put = (
        prev["close"] < lo
        and last["high"] >= lo - tol
        and last["close"] < lo
        and last["close"] < last["open"]
        and wi["put_wick"] >= 0.20
    )

    if call:
        return {"direction":"CALL","confidence":84,"confirmed":True,
                "reason":"Rompimento de resistência com reteste comprador.",
                "strategy":"OTC Rompimento + reteste"}
    if put:
        return {"direction":"PUT","confidence":84,"confirmed":True,
                "reason":"Rompimento de suporte com reteste vendedor.",
                "strategy":"OTC Rompimento + reteste"}

    return {"direction":"NEUTRO","confidence":48,"confirmed":False,
            "reason":"Sem rompimento e reteste confirmados.","strategy":"OTC Rompimento + reteste"}


def otc_displacement_reversal(cs):
    """Detecta candle de deslocamento forte chegando em região extrema, seguido de rejeição."""
    if len(cs) < 30:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Poucos candles.","strategy":"OTC Deslocamento + rejeição"}

    smap = otc_market_structure_map(cs)
    if not smap:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Estrutura insuficiente.","strategy":"OTC Deslocamento + rejeição"}

    impulse = cs[-2]
    last = cs[-1]
    impulse_range = max(1e-12, impulse["high"] - impulse["low"])
    impulse_body = abs(impulse["close"] - impulse["open"])
    strong = impulse_body >= smap["avg_range"] * 0.9 and impulse_body / impulse_range >= 0.65

    sup = smap["nearest_support"]
    res = smap["nearest_resistance"]
    tol = smap["tolerance"]
    wi = wick_info(last)

    call = (
        strong
        and impulse["close"] < impulse["open"]
        and sup is not None
        and impulse["low"] <= sup["price"] + tol
        and last["close"] > last["open"]
        and wi["call_wick"] >= 0.30
    )

    put = (
        strong
        and impulse["close"] > impulse["open"]
        and res is not None
        and impulse["high"] >= res["price"] - tol
        and last["close"] < last["open"]
        and wi["put_wick"] >= 0.30
    )

    if call:
        return {"direction":"CALL","confidence":83,"confirmed":True,
                "reason":"Deslocamento vendedor forte encontrou suporte e sofreu rejeição.",
                "strategy":"OTC Deslocamento + rejeição"}
    if put:
        return {"direction":"PUT","confidence":83,"confirmed":True,
                "reason":"Deslocamento comprador forte encontrou resistência e sofreu rejeição.",
                "strategy":"OTC Deslocamento + rejeição"}

    return {"direction":"NEUTRO","confidence":47,"confirmed":False,
            "reason":"Sem deslocamento extremo com rejeição.","strategy":"OTC Deslocamento + rejeição"}


def otc_structure_bias(cs):
    """Leitura de sequência de pivôs: HH/HL ou LH/LL."""
    if len(cs) < 35:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,
                "reason":"Poucos candles.","strategy":"OTC Estrutura de mercado"}

    highs, lows = _otc_swing_points(cs[-60:], 2, 2)

    if len(highs) < 2 or len(lows) < 2:
        return {"direction":"NEUTRO","confidence":45,"confirmed":False,
                "reason":"Poucos pivôs confiáveis.","strategy":"OTC Estrutura de mercado"}

    h1, h2 = highs[-2], highs[-1]
    l1, l2 = lows[-2], lows[-1]
    last = cs[-1]

    up = h2["price"] > h1["price"] and l2["price"] > l1["price"] and last["close"] > last["open"]
    down = h2["price"] < h1["price"] and l2["price"] < l1["price"] and last["close"] < last["open"]

    if up:
        return {"direction":"CALL","confidence":80,"confirmed":True,
                "reason":"Estrutura com máxima e mínima ascendentes (HH/HL).",
                "strategy":"OTC Estrutura de mercado"}
    if down:
        return {"direction":"PUT","confidence":80,"confirmed":True,
                "reason":"Estrutura com máxima e mínima descendentes (LH/LL).",
                "strategy":"OTC Estrutura de mercado"}

    return {"direction":"NEUTRO","confidence":46,"confirmed":False,
            "reason":"Estrutura sem direção limpa.","strategy":"OTC Estrutura de mercado"}



def adx(cs, period=21):
    """ADX clássico (Wilder simplificado) para medir força da tendência."""
    if len(cs) < period + 2:
        return None
    trs, plus_dm, minus_dm = [], [], []
    for i in range(1, len(cs)):
        cur, prev = cs[i], cs[i - 1]
        up = cur["high"] - prev["high"]
        down = prev["low"] - cur["low"]
        plus_dm.append(up if up > down and up > 0 else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
        trs.append(max(
            cur["high"] - cur["low"],
            abs(cur["high"] - prev["close"]),
            abs(cur["low"] - prev["close"]),
        ))
    if len(trs) < period:
        return None

    dxs = []
    for end in range(period, len(trs) + 1):
        tr = sum(trs[end-period:end])
        if tr <= 1e-12:
            continue
        pdi = 100.0 * sum(plus_dm[end-period:end]) / tr
        mdi = 100.0 * sum(minus_dm[end-period:end]) / tr
        den = pdi + mdi
        dxs.append(0.0 if den <= 1e-12 else 100.0 * abs(pdi - mdi) / den)
    if not dxs:
        return None
    return sum(dxs[-period:]) / min(period, len(dxs))


def _mega_ia_confluence_score(cs, direction, context=None):
    """
    Score M1/M5 de 0 a 100:
      M5 tendência 20; EMA 3/7 15; RSI 9 15; ADX 21 10;
      S/R local 15; padrão/rejeição 15; volume 10.
    """
    context = context or {}
    if len(cs) < 25 or direction not in ("CALL", "PUT"):
        return {"score": 0, "passed": False, "details": {}}

    closes = [float(c["close"]) for c in cs]
    e3, e7 = ema(closes, 3), ema(closes, 7)
    r9 = rsi(closes, 9)
    a21 = adx(cs, 21)
    last = cs[-1]
    details = {}
    score = 0

    # 1) Tendência M5 - 20 pontos.
    m5 = context.get("m5") or []
    m5_ok = False
    if len(m5) >= 10:
        m5c = [float(c["close"]) for c in m5]
        m5e3, m5e7 = ema(m5c, 3), ema(m5c, 7)
        if m5e3 is not None and m5e7 is not None:
            m5_ok = (m5e3 > m5e7) if direction == "CALL" else (m5e3 < m5e7)
    details["trend_m5"] = m5_ok
    if m5_ok:
        score += 20

    # 2) EMA 3/7 M1 - 15 pontos.
    ema_ok = False
    if e3 is not None and e7 is not None:
        ema_ok = (e3 > e7) if direction == "CALL" else (e3 < e7)
    details["ema_3_7"] = ema_ok
    if ema_ok:
        score += 15

    # 3) RSI 9 - 15 pontos. Usa reação/momentum, não apenas extremo rígido.
    r_prev = rsi(closes[:-1], 9) if len(closes) > 10 else None
    rsi_ok = False
    if r9 is not None and r_prev is not None:
        if direction == "CALL":
            rsi_ok = (30 <= r9 <= 68 and r9 >= r_prev) or (r_prev < 30 <= r9)
        else:
            rsi_ok = (32 <= r9 <= 70 and r9 <= r_prev) or (r_prev > 70 >= r9)
    details["rsi9"] = round(r9, 2) if r9 is not None else None
    details["rsi_ok"] = rsi_ok
    if rsi_ok:
        score += 15

    # 4) ADX 21 >= 20 - 10 pontos.
    adx_ok = a21 is not None and a21 >= 20.0
    details["adx21"] = round(a21, 2) if a21 is not None else None
    details["adx_ok"] = adx_ok
    if adx_ok:
        score += 10

    # 5) Suporte/resistência local - 15 pontos.
    recent = cs[-20:-1] if len(cs) >= 21 else cs[:-1]
    support = min((c["low"] for c in recent), default=last["low"])
    resistance = max((c["high"] for c in recent), default=last["high"])
    avg_range = sum(max(1e-12, c["high"]-c["low"]) for c in recent) / max(1, len(recent))
    tol = max(avg_range * 0.45, 1e-12)
    sr_ok = (last["low"] <= support + tol) if direction == "CALL" else (last["high"] >= resistance - tol)
    details["sr_ok"] = sr_ok
    if sr_ok:
        score += 15

    # 6) Padrão/rejeição da última vela - 15 pontos.
    wi = wick_info(last)
    bull = last["close"] > last["open"]
    bear = last["close"] < last["open"]
    reject_call = bull and wi.get("call_wick", 0) >= max(wi.get("body_ratio", 0), 0.01) * 0.7
    reject_put = bear and wi.get("put_wick", 0) >= max(wi.get("body_ratio", 0), 0.01) * 0.7
    # Também aceita duas velas consecutivas da direção, mas apenas como parte do score.
    c1, c2 = cs[-2], cs[-1]
    two_call = c1["close"] > c1["open"] and c2["close"] > c2["open"]
    two_put = c1["close"] < c1["open"] and c2["close"] < c2["open"]
    pattern_ok = (reject_call or two_call) if direction == "CALL" else (reject_put or two_put)
    details["pattern_ok"] = pattern_ok
    if pattern_ok:
        score += 15

    # 7) Volume relativo - 10 pontos.
    vols = [float(c.get("volume", 0) or 0) for c in cs[-20:-1]]
    vavg = sum(vols) / len(vols) if vols else 0.0
    vlast = float(last.get("volume", 0) or 0)
    # Algumas fontes OTC retornam volume 0; nesse caso não damos pontos nem bloqueamos sozinho.
    volume_ok = vavg > 0 and vlast >= vavg * 0.90
    details["volume"] = round(vlast, 2)
    details["volume_avg"] = round(vavg, 2)
    details["volume_ok"] = volume_ok
    if volume_ok:
        score += 10

    return {"score": int(score), "passed": score >= 75, "details": details}



def _otc_pattern_feature(cs, end_idx):
    """Cria uma assinatura normalizada do comportamento recente até end_idx."""
    if end_idx < 12 or end_idx >= len(cs):
        return None

    start = max(0, end_idx - 13)
    recent = cs[start:end_idx + 1]
    ranges = [max(1e-12, float(c["high"]) - float(c["low"])) for c in recent]
    avg_range = sum(ranges) / max(1, len(ranges))
    if avg_range <= 1e-12:
        return None

    feats = []
    for c in cs[end_idx - 2:end_idx + 1]:
        o, h, l, cl = map(float, (c["open"], c["high"], c["low"], c["close"]))
        rg = max(1e-12, h - l)
        body = cl - o
        upper = h - max(o, cl)
        lower = min(o, cl) - l
        feats.extend([
            max(-1.5, min(1.5, body / rg)),
            max(0.0, min(1.5, upper / rg)),
            max(0.0, min(1.5, lower / rg)),
            max(0.0, min(3.0, rg / avg_range)),
        ])

    closes = [float(c["close"]) for c in cs[:end_idx + 1]]
    e3 = ema(closes, 3)
    e7 = ema(closes, 7)
    r9 = rsi(closes, 9)
    if e3 is None or e7 is None or r9 is None:
        return None

    feats.append(max(-3.0, min(3.0, (e3 - e7) / avg_range)))
    feats.append(max(-1.0, min(1.0, (r9 - 50.0) / 50.0)))
    return feats


def otc_pattern_detector(cs, context=None):
    """
    Detector estatístico de padrões OTC.

    Compara a assinatura das 3 velas mais recentes com situações anteriores
    do MESMO fluxo de candles e mede o que aconteceu na vela seguinte.
    Não lê algoritmo interno da corretora e não promete prever o futuro.
    """
    context = context or {}
    min_history = 55
    min_matches = 8
    max_neighbors = 20
    similarity_floor = 0.66

    if len(cs) < min_history:
        return {
            "direction": "NEUTRO", "confidence": 0, "confirmed": False,
            "reason": f"Detector OTC aguardando histórico suficiente ({len(cs)}/{min_history} candles).",
            "strategy": "Detector de Padrões OTC",
            "pattern_samples": 0, "pattern_probability": 0,
        }

    target = _otc_pattern_feature(cs, len(cs) - 1)
    if not target:
        return {
            "direction": "NEUTRO", "confidence": 0, "confirmed": False,
            "reason": "Não foi possível montar a assinatura do padrão atual.",
            "strategy": "Detector de Padrões OTC",
            "pattern_samples": 0, "pattern_probability": 0,
        }

    candidates = []
    # end_idx precisa deixar uma vela posterior para medir o resultado.
    for end_idx in range(14, len(cs) - 2):
        feat = _otc_pattern_feature(cs, end_idx)
        if not feat or len(feat) != len(target):
            continue
        dist = (sum((a - b) ** 2 for a, b in zip(target, feat)) / len(target)) ** 0.5
        similarity = 1.0 / (1.0 + dist)
        if similarity < similarity_floor:
            continue

        nxt = cs[end_idx + 1]
        if float(nxt["close"]) > float(nxt["open"]):
            outcome = "CALL"
        elif float(nxt["close"]) < float(nxt["open"]):
            outcome = "PUT"
        else:
            continue
        candidates.append((similarity, outcome))

    candidates.sort(key=lambda x: x[0], reverse=True)
    neighbors = candidates[:max_neighbors]
    if len(neighbors) < min_matches:
        return {
            "direction": "NEUTRO", "confidence": 45, "confirmed": False,
            "reason": f"Só {len(neighbors)} padrões suficientemente parecidos; mínimo {min_matches}.",
            "strategy": "Detector de Padrões OTC",
            "pattern_samples": len(neighbors), "pattern_probability": 0,
        }

    call_w = sum(sim for sim, out in neighbors if out == "CALL")
    put_w = sum(sim for sim, out in neighbors if out == "PUT")
    total = call_w + put_w
    if total <= 1e-12:
        return {
            "direction": "NEUTRO", "confidence": 45, "confirmed": False,
            "reason": "Amostras históricas sem peso estatístico suficiente.",
            "strategy": "Detector de Padrões OTC",
            "pattern_samples": len(neighbors), "pattern_probability": 0,
        }

    p_call = call_w / total
    p_put = put_w / total
    direction = "CALL" if p_call > p_put else "PUT"
    prob = max(p_call, p_put)
    avg_similarity = sum(sim for sim, _ in neighbors) / len(neighbors)

    # Confirma apenas com vantagem clara; a confluência geral ainda exige score >=75.
    confirmed = prob >= 0.66 and avg_similarity >= 0.69
    conf = max(50.0, min(90.0, prob * 100.0 * 0.75 + avg_similarity * 100.0 * 0.25))

    symbol = context.get("symbol") or "ativo OTC"
    return {
        "direction": direction if confirmed else "NEUTRO",
        "confidence": round(conf if confirmed else min(conf, 64.0), 1),
        "confirmed": confirmed,
        "reason": (
            f"{len(neighbors)} padrões parecidos em {symbol}: estimativa empírica "
            f"{direction} {prob*100:.1f}% (similaridade média {avg_similarity*100:.1f}%)."
            if confirmed else
            f"Padrões históricos sem vantagem clara: CALL {p_call*100:.1f}% / PUT {p_put*100:.1f}%."
        ),
        "strategy": "Detector de Padrões OTC",
        "pattern_samples": len(neighbors),
        "pattern_probability": round(prob * 100.0, 1),
        "call_probability": round(p_call * 100.0, 1),
        "put_probability": round(p_put * 100.0, 1),
        "avg_similarity": round(avg_similarity * 100.0, 1),
    }

def otc_noise_filter(cs):
    """
    Filtro de proteção contra gráfico errático:
    evita entrada quando a última vela é anormalmente grande ou quando há alternância excessiva.
    """
    if len(cs) < 20:
        return {"blocked": True, "reason": "Poucos candles para filtro de ruído."}

    recent = cs[-12:]
    ranges = [max(1e-12, c["high"] - c["low"]) for c in recent[:-1]]
    avg_range = sum(ranges) / len(ranges)
    last_range = max(1e-12, recent[-1]["high"] - recent[-1]["low"])

    colors = [1 if c["close"] > c["open"] else -1 if c["close"] < c["open"] else 0 for c in recent[-8:]]
    flips = sum(1 for a, b in zip(colors, colors[1:]) if a and b and a != b)

    if last_range > avg_range * 2.8:
        return {"blocked": True, "reason": "Vela anormalmente grande; aguardando normalização."}

    if flips >= 6:
        return {"blocked": True, "reason": "Mercado muito alternado/ruidoso no curto prazo."}

    return {"blocked": False, "reason": "Fluxo aceitável."}

def otc_engine(cs, context=None):
    """
    Motor EXCLUSIVO para OTC.
    Observa padrões, estrutura, regiões fortes e falsos rompimentos.
    """
    noise = otc_noise_filter(cs)
    structure_map = otc_market_structure_map(cs)

    pattern_detector = otc_pattern_detector(cs, context)

    strategies = [
        # Detector estatístico do comportamento do próprio ativo OTC
        pattern_detector,

        # Estratégias técnicas OTC
        otc_bb_rsi_rejection(cs),
        otc_stochastic_reversal(cs),
        otc_ema_pullback(cs),
        otc_two_candle_color_continuation(cs),
        otc_exhaustion_reversal(cs),
        otc_local_sr_rejection(cs),
        otc_micro_macd(cs),

        # Estratégias exclusivas de padrões de vela OTC
        otc_candle_engulfing(cs),
        otc_candle_hammer_star(cs),
        otc_candle_tweezer(cs),
        otc_candle_morning_evening_star(cs),
        otc_candle_pinbar(cs),
        otc_candle_inside_breakout(cs),
        otc_candle_doji_reversal(cs),

        # Leitura estrutural do gráfico OTC
        otc_strong_zone_rejection(cs),
        otc_liquidity_sweep(cs),
        otc_break_retest(cs),
        otc_displacement_reversal(cs),
        otc_structure_bias(cs),
    ]

    if noise.get("blocked"):
        best = max((float(x.get("confidence", 0)) for x in strategies), default=0)
        return {
            "direction": "NEUTRO",
            "confidence": round(min(best, 65), 1),
            "confirmed": False,
            "strategy": "Motor OTC multiestratégia",
            "reason": noise.get("reason", "Filtro de ruído bloqueou a entrada."),
            "pattern_detector": pattern_detector,
            "strategies": strategies,
            "engine": "OTC",
            "structure": structure_map,
            "noise_filter": noise,
        }

    confirmed = [
        x for x in strategies
        if x.get("confirmed") and x.get("direction") in ("CALL", "PUT")
    ]
    calls = [x for x in confirmed if x["direction"] == "CALL"]
    puts = [x for x in confirmed if x["direction"] == "PUT"]

    winner = calls if len(calls) > len(puts) else puts if len(puts) > len(calls) else []

    if winner:
        direction = winner[0]["direction"]

        # Se o detector histórico estiver forte na direção oposta, não liberamos entrada.
        if (
            pattern_detector.get("confirmed")
            and pattern_detector.get("direction") in ("CALL", "PUT")
            and pattern_detector.get("direction") != direction
            and float(pattern_detector.get("confidence", 0) or 0) >= 72
        ):
            return {
                "direction": "NEUTRO",
                "confidence": 68,
                "confirmed": False,
                "strategy": "MEGA IA + Detector de Padrões OTC",
                "reason": "Conflito entre confluência técnica e padrão histórico; entrada bloqueada.",
                "pattern_detector": pattern_detector,
                "strategies": strategies,
                "engine": "OTC",
                "structure": structure_map,
                "noise_filter": noise,
            }
        avg = sum(float(x.get("confidence", 0)) for x in winner) / len(winner)

        # Preferência: pelo menos 2 estratégias OTC concordando.
        # Com vários detectores de padrão, exigimos pelo menos 2 confirmações.
        if len(winner) >= 2:
            mega_score = _mega_ia_confluence_score(cs, direction, context)
            score = int(mega_score.get("score", 0))
            if mega_score.get("passed"):
                # A confiança passa a refletir tanto as estratégias quanto o score 0-100.
                conf = min(96, max(75, avg * 0.55 + score * 0.45))
                return {
                    "direction": direction,
                    "confidence": round(conf, 1),
                    "confirmed": True,
                    "strategy": "MEGA IA 75/100 + OTC multiestratégia",
                    "reason": f"{len(winner)} estratégias OTC em confluência; score MEGA IA {score}/100.",
                    "score": score,
                    "score_min": 75,
                    "score_details": mega_score.get("details", {}),
                    "pattern_detector": pattern_detector,
                    "strategies": strategies,
                    "engine": "OTC",
                    "structure": structure_map,
                    "noise_filter": noise,
                }
            return {
                "direction": "NEUTRO",
                "confidence": round(min(74, max(avg, score)), 1),
                "confirmed": False,
                "strategy": "MEGA IA 75/100 + OTC multiestratégia",
                "reason": f"Confluência técnica detectada, mas score {score}/100 abaixo do mínimo 75.",
                "score": score,
                "score_min": 75,
                "score_details": mega_score.get("details", {}),
                "pattern_detector": pattern_detector,
                "strategies": strategies,
                "engine": "OTC",
                "structure": structure_map,
                "noise_filter": noise,
            }

        # Um único padrão/indicador não libera sinal OTC.
        # O motor aguarda outra confirmação independente.

    best = max((float(x.get("confidence", 0)) for x in strategies), default=0)
    return {
        "direction": "NEUTRO",
        "confidence": round(min(best, 69), 1),
        "confirmed": False,
        "strategy": "Motor OTC multiestratégia",
        "reason": "Sem confluência suficiente nas estratégias OTC.",
        "pattern_detector": pattern_detector,
        "strategies": strategies,
        "engine": "OTC",
        "structure": structure_map,
        "noise_filter": noise,
    }


def strategy_engine_for_market(cs, market="OPEN", context=None):
    market = (market or "OPEN").upper()
    if market in ("IQ_OTC", "OLYMP_OTC"):
        return otc_engine(cs, context)
    return local_engine(cs, context)

def open_confluence_ema_rsi_structure(cs):
    """
    Estratégia exclusiva de MERCADO ABERTO.
    Confluência: EMA 9/21/50, RSI 7/14, força/rejeição da vela,
    rompimento de estrutura (12 velas) e Bollinger 20/2.

    Pontuação máxima por direção: 79.
    Libera confirmação somente com >= 50 pontos e vantagem >= 15 pontos.
    """
    if len(cs) < 55:
        return {
            "direction": "NEUTRO", "confidence": 0, "confirmed": False,
            "reason": "Poucos candles fechados para a confluência 9/21/50.",
            "strategy": "OPEN Confluência EMA 9/21/50 + RSI 7/14",
        }

    closes = [float(c["close"]) for c in cs]
    highs = [float(c["high"]) for c in cs]
    lows = [float(c["low"]) for c in cs]
    last = cs[-1]

    e9 = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)
    r7 = rsi(closes, 7)
    r14 = rsi(closes, 14)
    bb = bollinger(closes, 20, 2.0)

    if None in (e9, e21, e50, r7, r14) or not bb:
        return {
            "direction": "NEUTRO", "confidence": 0, "confirmed": False,
            "reason": "Indicadores insuficientes para a confluência de mercado aberto.",
            "strategy": "OPEN Confluência EMA 9/21/50 + RSI 7/14",
        }

    scores = {"CALL": 0.0, "PUT": 0.0}
    reasons = {"CALL": [], "PUT": []}

    def add(direction, weight, text):
        scores[direction] += float(weight)
        reasons[direction].append(text)

    # 1) Tendência - 22 pontos
    if e9 > e21 > e50 and closes[-1] > e9:
        add("CALL", 22, "tendência de alta pelas EMA 9/21/50")
    elif e9 < e21 < e50 and closes[-1] < e9:
        add("PUT", 22, "tendência de baixa pelas EMA 9/21/50")

    # 2) RSI - 14 pontos
    if r7 >= 55 and r14 >= 52 and r7 < 78:
        add("CALL", 14, "momentum comprador confirmado pelo RSI 7/14")
    elif r7 <= 45 and r14 <= 48 and r7 > 22:
        add("PUT", 14, "momentum vendedor confirmado pelo RSI 7/14")

    # 3) Price action / força da vela - 12 pontos
    body = abs(float(last["close"]) - float(last["open"]))
    rng = max(float(last["high"]) - float(last["low"]), 1e-12)
    upper_wick = float(last["high"]) - max(float(last["open"]), float(last["close"]))
    lower_wick = min(float(last["open"]), float(last["close"])) - float(last["low"])

    if last["close"] > last["open"] and body / rng >= 0.55:
        add("CALL", 12, "vela de força compradora")
    elif last["close"] < last["open"] and body / rng >= 0.55:
        add("PUT", 12, "vela de força vendedora")

    # 4) Rejeição - 10 pontos
    if lower_wick / rng >= 0.45 and last["close"] > last["open"]:
        add("CALL", 10, "rejeição de preços baixos")
    elif upper_wick / rng >= 0.45 and last["close"] < last["open"]:
        add("PUT", 10, "rejeição de preços altos")

    # 5) Estrutura - 14 pontos (12 velas anteriores)
    lookback = min(12, len(cs) - 2)
    recent_high = max(highs[-lookback - 1:-1])
    recent_low = min(lows[-lookback - 1:-1])
    if float(last["close"]) > recent_high:
        add("CALL", 14, "rompimento da máxima recente")
    elif float(last["close"]) < recent_low:
        add("PUT", 14, "rompimento da mínima recente")

    # 6) Bollinger 20/2 - 7 pontos; usada como confluência, não como gatilho isolado.
    if closes[-1] > bb["middle"] and closes[-1] < bb["upper"]:
        add("CALL", 7, "preço acima da média das Bollinger")
    elif closes[-1] < bb["middle"] and closes[-1] > bb["lower"]:
        add("PUT", 7, "preço abaixo da média das Bollinger")

    direction = "CALL" if scores["CALL"] > scores["PUT"] else "PUT"
    other = "PUT" if direction == "CALL" else "CALL"
    best = scores[direction]
    advantage = best - scores[other]
    confirmed = best >= 50 and advantage >= 15

    if confirmed:
        # Converte 50..79 pontos para ~78..96% sem prometer probabilidade real.
        confidence = clamp(78 + (best - 50) * (18 / 29), 78, 96)
        reason = "; ".join(reasons[direction])
        return {
            "direction": direction, "confidence": round(confidence, 1), "confirmed": True,
            "reason": reason,
            "strategy": "OPEN Confluência EMA 9/21/50 + RSI 7/14",
            "score": round(best, 1), "score_call": round(scores["CALL"], 1),
            "score_put": round(scores["PUT"], 1), "score_advantage": round(advantage, 1),
            "ema9": e9, "ema21": e21, "ema50": e50,
            "rsi7": round(r7, 2), "rsi14": round(r14, 2), "bollinger": bb,
        }

    best_score = max(scores.values())
    return {
        "direction": "NEUTRO",
        "confidence": round(clamp(35 + best_score * 0.6, 35, 69), 1),
        "confirmed": False,
        "reason": (
            f"Confluência OPEN insuficiente: CALL {scores['CALL']:.0f}/79, "
            f"PUT {scores['PUT']:.0f}/79; exige mínimo 50 e vantagem de 15 pontos."
        ),
        "strategy": "OPEN Confluência EMA 9/21/50 + RSI 7/14",
        "score_call": round(scores["CALL"], 1), "score_put": round(scores["PUT"], 1),
        "ema9": e9, "ema21": e21, "ema50": e50,
        "rsi7": round(r7, 2), "rsi14": round(r14, 2), "bollinger": bb,
    }



def open_volume_force_strategy(cs):
    """Mercado Aberto: Volume + Vela de Força + EMA 9/21 + RSI 7 + ATR 14."""
    name = "OPEN Impulso de Volume + Vela de Força"
    if len(cs) < 35:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,"strategy":name,
                "reason":"Poucos candles para analisar volume e força."}
    closes=[float(c["close"]) for c in cs]
    last=cs[-1]
    volumes=[float(c.get("volume",0) or 0) for c in cs]
    hist=[v for v in volumes[-21:-1] if v > 0]
    current=volumes[-1]
    if current <= 0 or len(hist) < 15:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,"strategy":name,
                "reason":"Fonte sem volume suficiente; estratégia bloqueada.","volume_available":False}
    avg=sum(hist[-20:])/len(hist[-20:])
    vr=current/max(avg,1e-12)
    e9,e21=ema(closes,9),ema(closes,21)
    r7=rsi(closes,7)
    a14=atr(cs,14)
    if None in (e9,e21,r7,a14):
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,"strategy":name,
                "reason":"Indicadores insuficientes."}
    o,h,l,c=map(float,(last["open"],last["high"],last["low"],last["close"]))
    rng=max(h-l,1e-12)
    body=abs(c-o)
    br=body/rng
    uw=(h-max(o,c))/rng
    lw=(min(o,c)-l)/rng
    ar=body/max(float(a14),1e-12)
    volume_ok=vr >= 1.50
    force_ok=br >= 0.65
    atr_ok=0.80 <= ar <= 1.80
    call_ok=c>o and e9>e21 and 55<=r7<=72 and volume_ok and force_ok and uw<=0.20 and atr_ok
    put_ok=c<o and e9<e21 and 28<=r7<=45 and volume_ok and force_ok and lw<=0.20 and atr_ok
    details={"volume":round(current,2),"volume_avg20":round(avg,2),"volume_ratio":round(vr,2),
             "body_ratio":round(br,3),"atr14":round(float(a14),8),"body_atr_ratio":round(ar,2),
             "ema9":e9,"ema21":e21,"rsi7":round(float(r7),2),"volume_available":True}
    if call_ok or put_ok:
        d="CALL" if call_ok else "PUT"
        conf=82+min(6,max(0,(vr-1.5)*6))+min(4,max(0,(br-0.65)*20))
        return {"direction":d,"confidence":round(clamp(conf,82,94),1),"confirmed":True,
                "strategy":name,
                "reason":f"{d}: volume {vr:.2f}x da média, corpo {br*100:.0f}%, EMA 9/21, RSI 7 {r7:.1f} e ATR confirmados.",
                **details}
    return {"direction":"NEUTRO","confidence":min(69,round(35+min(vr,2)*8+br*18,1)),
            "confirmed":False,"strategy":name,
            "reason":"Sem confluência completa de volume, vela de força, tendência, RSI e ATR.",**details}


def local_engine(cs, context=None):
    context = context or {}
    strategies = [
        open_confluence_ema_rsi_structure(cs),
        open_volume_force_strategy(cs),
        htf_sr_rsi_macd_strategy(
            cs,
            context.get("h1"),
            context.get("h4"),
            context.get("interval", "5min"),
        ),
        bollinger_stochastic(cs),
    ]
    confirmed=[x for x in strategies if x.get("confirmed") and x.get("direction") in ("CALL","PUT")]
    calls=[x for x in confirmed if x["direction"]=="CALL"]
    puts=[x for x in confirmed if x["direction"]=="PUT"]
    winner = calls if len(calls)>len(puts) else puts if len(puts)>len(calls) else []
    if winner:
        direction=winner[0]["direction"]
        conf=sum(float(x.get("confidence",0)) for x in winner)/len(winner)
        if len(winner)>=2:
            conf=min(96,conf+4+min(4,len(winner)-2))
            return {"direction":direction,"confidence":round(conf,1),"confirmed":True,
                    "strategy":"Motor multiestratégia","reason":"Confluência interna confirmada.","strategies":strategies}
        one=winner[0]
        if float(one.get("confidence",0))>=80:
            return {"direction":direction,"confidence":round(float(one["confidence"]),1),"confirmed":True,
                    "strategy":"Motor multiestratégia","reason":"Sinal técnico forte confirmado.","strategies":strategies}
    best=max((float(x.get("confidence",0)) for x in strategies), default=0)
    return {"direction":"NEUTRO","confidence":round(min(best,69),1),"confirmed":False,
            "strategy":"Motor multiestratégia","reason":"Sem confirmação suficiente entre os filtros internos.","strategies":strategies}


def json_extract(text):
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.I)
    try:
        return json.loads(text)
    except Exception:
        m = re.search(r"\{.*\}", text, re.S)
        return json.loads(m.group(0)) if m else None


def _td_cache_ttl(interval: str) -> float:
    sec = int(INTERVALS.get(interval, 60))
    return max(35.0, min(180.0, sec * 0.45))


def _td_cache_age(symbol: str, interval: str) -> float:
    item = td_candle_cache.get(f"{symbol}|{interval}")
    if not item:
        return 10**9
    return max(0.0, time.time() - float(item[0]))


async def candles_open(symbol, interval, n=80):
    global td_last_call_at, td_backoff_until, td_backoff_reason
    if not TD_KEY:
        raise HTTPException(500, "TWELVE_DATA_API_KEY não configurada.")

    n = max(20, min(int(n), 150))
    key = f"{symbol}|{interval}"
    now_ts = time.time()
    cached = td_candle_cache.get(key)
    ttl = _td_cache_ttl(interval)

    if cached and now_ts - cached[0] < ttl and len(cached[1]) >= min(n, 20):
        return cached[1][-n:]

    if now_ts < td_backoff_until:
        if cached and now_ts - cached[0] <= TD_STALE_MAX_AGE:
            return cached[1][-n:]
        wait = max(1, int(td_backoff_until - now_ts + 0.999))
        raise HTTPException(503, f"Twelve Data em limite temporário. Nova tentativa em {wait}s.")

    lock = td_locks.setdefault(key, asyncio.Lock())
    async with lock:
        now_ts = time.time()
        cached = td_candle_cache.get(key)
        if cached and now_ts - cached[0] < ttl and len(cached[1]) >= min(n, 20):
            return cached[1][-n:]

        if now_ts < td_backoff_until:
            if cached and now_ts - cached[0] <= TD_STALE_MAX_AGE:
                return cached[1][-n:]
            wait = max(1, int(td_backoff_until - now_ts + 0.999))
            raise HTTPException(503, f"Twelve Data em limite temporário. Nova tentativa em {wait}s.")

        fetch_n = max(100, n)
        params = {"symbol": symbol, "interval": interval, "outputsize": fetch_n, "apikey": TD_KEY, "format": "JSON"}

        async with td_rate_lock:
            delay = TD_MIN_CALL_INTERVAL - (time.time() - td_last_call_at)
            if delay > 0:
                await asyncio.sleep(delay)

            async with td_sem:
                try:
                    async with httpx.AsyncClient(timeout=15) as client:
                        response = await client.get(TD_URL, params=params)
                    td_last_call_at = time.time()
                except Exception as exc:
                    if cached and time.time() - cached[0] <= TD_STALE_MAX_AGE:
                        return cached[1][-n:]
                    raise HTTPException(503, f"Twelve Data indisponível temporariamente: {str(exc)[:120]}")

        if response.status_code == 429:
            retry_header = response.headers.get("Retry-After", "")
            try:
                retry_after = max(30.0, float(retry_header)) if retry_header else 60.0
            except Exception:
                retry_after = 60.0
            td_backoff_until = time.time() + retry_after
            td_backoff_reason = "HTTP 429"
            if cached and time.time() - cached[0] <= TD_STALE_MAX_AGE:
                return cached[1][-n:]
            raise HTTPException(503, f"Limite da Twelve Data atingido. Aguarde cerca de {int(retry_after)}s.")

        try:
            response.raise_for_status()
            data = response.json()
        except Exception as exc:
            if cached and time.time() - cached[0] <= TD_STALE_MAX_AGE:
                return cached[1][-n:]
            raise HTTPException(502, f"Falha ao consultar Twelve Data: {str(exc)[:140]}")

        if data.get("status") == "error":
            msg = str(data.get("message", "Erro Twelve Data."))
            code = str(data.get("code", ""))
            if "429" in code or "limit" in msg.lower() or "credit" in msg.lower():
                td_backoff_until = time.time() + 60.0
                td_backoff_reason = msg[:160]
                if cached and time.time() - cached[0] <= TD_STALE_MAX_AGE:
                    return cached[1][-n:]
                raise HTTPException(503, "Limite temporário da Twelve Data. Aguarde 60s.")
            if cached and time.time() - cached[0] <= TD_STALE_MAX_AGE:
                return cached[1][-n:]
            raise HTTPException(502, msg[:220])

        out = []
        for x in reversed(data.get("values", [])):
            try:
                out.append({
                    "datetime": x["datetime"],
                    "open": float(x["open"]),
                    "high": float(x["high"]),
                    "low": float(x["low"]),
                    "close": float(x["close"]),
                    "volume": float(x.get("volume", 0) or 0)
                })
            except Exception:
                pass

        if not out:
            if cached and time.time() - cached[0] <= TD_STALE_MAX_AGE:
                return cached[1][-n:]
            raise HTTPException(502, "Nenhum candle recebido da Twelve Data.")

        td_candle_cache[key] = (time.time(), out)
        td_backoff_reason = ""
        return out[-n:]


def iq_active_candidates(symbol: str):
    base = symbol.replace("/", "").upper()
    preferred = OTC_BASE.get(symbol, f"{base}-OTC")

    return list(dict.fromkeys([
        preferred,
        f"{base}-OTC",
        f"{base}_OTC",
        base,
    ]))


def iq_regular_active_candidates(symbol: str):
    """Candidatos do mercado normal da IQ Option para espelhar o gráfico."""
    base = symbol.replace("/", "").upper()
    return list(dict.fromkeys([
        base,
        symbol.replace("/", "").upper(),
    ]))


def iq_seconds(interval: str):
    if interval not in INTERVALS:
        raise RuntimeError("Intervalo inválido para IQ Option.")
    return INTERVALS[interval]


def _iq_lowlevel_connect_worker(client, result_box):
    """
    Abre somente a camada HTTP/WebSocket da iqoptionapi.
    Evita o travamento conhecido de algumas versões antigas de
    stable_api.IQ_Option.connect() esperando balance_id.
    """
    try:
        from iqoptionapi.api import IQOptionAPI

        api = IQOptionAPI(
            "iqoption.com",
            client.email,
            client.password,
        )

        api.set_session(
            headers=getattr(client, "SESSION_HEADER", {}),
            cookies=getattr(client, "SESSION_COOKIE", {}),
        )

        client.api = api
        check, reason = api.connect()

        result_box["ok"] = bool(check)
        result_box["reason"] = str(reason or "")

    except Exception as exc:
        result_box["error"] = repr(exc)


def _iq_network_probe():
    """Diagnóstico leve da rota Render -> IQ Option, sem usar credenciais."""
    import socket
    host = "iqoption.com"
    out = {"dns_ok": False, "tcp443_ok": False, "dns_ms": None, "tcp_ms": None, "ip": ""}
    try:
        t0 = time.monotonic()
        infos = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        out["dns_ms"] = round((time.monotonic() - t0) * 1000)
        out["dns_ok"] = bool(infos)
        if infos:
            out["ip"] = str(infos[0][4][0])
    except Exception as exc:
        out["dns_error"] = f"{type(exc).__name__}: {str(exc)[:160]}"
        return out

    try:
        t0 = time.monotonic()
        sock = socket.create_connection((host, 443), timeout=6.0)
        out["tcp_ms"] = round((time.monotonic() - t0) * 1000)
        out["tcp443_ok"] = True
        try:
            sock.close()
        except Exception:
            pass
    except Exception as exc:
        out["tcp_error"] = f"{type(exc).__name__}: {str(exc)[:160]}"
    return out


def _iq_connect_fresh(email: str, password: str):
    """Login IQ Option com diagnóstico por etapas e timeout real."""
    if IQ_Option is None:
        raise RuntimeError("Biblioteca iqoptionapi não carregada no servidor.")

    email = (email or "").strip()
    password = password or ""
    if not email or not password:
        raise RuntimeError("Informe e-mail e senha da IQ Option.")

    # 1) Verifica primeiro se o Render consegue resolver o domínio e abrir TCP 443.
    probe = _iq_network_probe()
    print(
        "[IQ DIAG] rede "
        f"dns={'OK' if probe.get('dns_ok') else 'FALHA'} "
        f"dns_ms={probe.get('dns_ms')} "
        f"tcp443={'OK' if probe.get('tcp443_ok') else 'FALHA'} "
        f"tcp_ms={probe.get('tcp_ms')} "
        f"ip={probe.get('ip') or '-'}",
        flush=True,
    )
    if probe.get("dns_error"):
        print(f"[IQ DIAG] DNS erro={probe['dns_error']}", flush=True)
    if probe.get("tcp_error"):
        print(f"[IQ DIAG] TCP443 erro={probe['tcp_error']}", flush=True)
    if not probe.get("dns_ok") or not probe.get("tcp443_ok"):
        # O teste TCP é apenas diagnóstico. Em algumas rotas/clouds ele pode
        # falhar momentaneamente mesmo quando a própria biblioteca consegue
        # negociar a sessão logo depois. Por isso não bloqueamos o login aqui:
        # deixamos o IQ_Option.connect() fazer a tentativa real.
        print(
            "[IQ DIAG] aviso: probe 443 falhou; continuando com a tentativa real da iqoptionapi",
            flush=True,
        )

    stable_box = {"stage": "aguardando", "started": time.monotonic()}

    def stable_worker():
        try:
            stable_box["stage"] = "construtor_IQ_Option"
            stable_box["stage_at"] = time.monotonic()
            client = IQ_Option(email, password)
            stable_box["client"] = client
            stable_box["constructor_ms"] = round((time.monotonic() - stable_box["stage_at"]) * 1000)
            stable_box["stage"] = "connect_HTTP_WebSocket"
            stable_box["stage_at"] = time.monotonic()
            stable_box["result"] = client.connect()
            stable_box["connect_ms"] = round((time.monotonic() - stable_box["stage_at"]) * 1000)
            stable_box["stage"] = "connect_retorno"
        except Exception as exc:
            stable_box["stage"] = "excecao"
            stable_box["error"] = exc

    print("[IQ LOGIN] iniciando stable_api", flush=True)
    th = threading.Thread(target=stable_worker, daemon=True)
    th.start()
    th.join(32.0)

    if not th.is_alive():
        elapsed_ms = round((time.monotonic() - stable_box["started"]) * 1000)
        print(
            f"[IQ DIAG] stable terminou etapa={stable_box.get('stage')} "
            f"total_ms={elapsed_ms} construtor_ms={stable_box.get('constructor_ms')} "
            f"connect_ms={stable_box.get('connect_ms')}",
            flush=True,
        )
        if "error" in stable_box:
            exc = stable_box["error"]
            print(f"[IQ LOGIN] stable_api erro={type(exc).__name__}: {str(exc)[:180]}", flush=True)
        else:
            client = stable_box.get("client")
            result = stable_box.get("result")
            if isinstance(result, (tuple, list)):
                ok = bool(result[0]) if result else False
                reason = str(result[1] if len(result) > 1 else "")
            else:
                ok = bool(result)
                reason = ""
            print(
                f"[IQ DIAG] stable connect retornou ok={ok} reason={reason[:120] or '-'}",
                flush=True,
            )
            if ok and client is not None:
                try:
                    stable_box["stage"] = "check_connect_WebSocket"
                    connected = bool(client.check_connect())
                    print(f"[IQ DIAG] check_connect={connected}", flush=True)
                    if connected:
                        print("[IQ LOGIN] stable_api conectado", flush=True)
                        return client
                except Exception as exc:
                    print(f"[IQ DIAG] check_connect erro={type(exc).__name__}: {str(exc)[:160]}", flush=True)
            low = reason.lower()
            if "2fa" in low or "verify" in low:
                raise RuntimeError("A IQ Option está exigindo autenticação em duas etapas (2FA).")
            if "invalid_credentials" in low or "wrong credentials" in low or "invalid credentials" in low:
                raise RuntimeError("E-mail ou senha da IQ Option estão incorretos.")
            print(f"[IQ LOGIN] stable_api sem conexão; fallback. motivo={reason[:120]}", flush=True)
    else:
        elapsed_ms = round((time.monotonic() - stable_box["started"]) * 1000)
        stage = stable_box.get("stage") or "desconhecida"
        stage_age_ms = None
        if stable_box.get("stage_at"):
            stage_age_ms = round((time.monotonic() - stable_box["stage_at"]) * 1000)
        print(
            f"[IQ DIAG] TIMEOUT stable etapa={stage} total_ms={elapsed_ms} etapa_ms={stage_age_ms} "
            f"construtor_ms={stable_box.get('constructor_ms')}",
            flush=True,
        )
        print("[IQ LOGIN] stable_api excedeu 32s; cancelando sem abrir conexão concorrente", flush=True)
        if stage == "construtor_IQ_Option":
            detail = "travou ao criar o cliente IQ_Option antes de iniciar o login."
        elif stage == "connect_HTTP_WebSocket":
            detail = "travou dentro de client.connect(), na etapa de autenticação/sessão/WebSocket da iqoptionapi."
        else:
            detail = f"travou na etapa {stage}."
        raise TimeoutError(
            "A IQ Option não respondeu ao servidor dentro de 32 segundos; " + detail
        )

    fallback_box = {"stage": "aguardando", "started": time.monotonic()}

    def fallback_worker():
        try:
            fallback_box["stage"] = "construtor_IQ_Option"
            fallback_box["stage_at"] = time.monotonic()
            client = IQ_Option(email, password)
            fallback_box["constructor_ms"] = round((time.monotonic() - fallback_box["stage_at"]) * 1000)
            from iqoptionapi.api import IQOptionAPI
            fallback_box["stage"] = "criar_IQOptionAPI"
            api = IQOptionAPI("iqoption.com", email, password)
            fallback_box["stage"] = "set_session"
            api.set_session(
                headers=getattr(client, "SESSION_HEADER", {}),
                cookies=getattr(client, "SESSION_COOKIE", {}),
            )
            client.api = api
            fallback_box["stage"] = "api_connect_HTTP_WebSocket"
            fallback_box["stage_at"] = time.monotonic()
            check, reason = api.connect()
            fallback_box["connect_ms"] = round((time.monotonic() - fallback_box["stage_at"]) * 1000)
            fallback_box["stage"] = "api_connect_retorno"
            fallback_box["client"] = client
            fallback_box["ok"] = bool(check)
            fallback_box["reason"] = str(reason or "")
        except Exception as exc:
            fallback_box["stage"] = "excecao"
            fallback_box["error"] = exc

    print("[IQ LOGIN] iniciando fallback low-level", flush=True)
    ft = threading.Thread(target=fallback_worker, daemon=True)
    ft.start()
    ft.join(20.0)

    if ft.is_alive():
        stage = fallback_box.get("stage") or "desconhecida"
        stage_age_ms = None
        if fallback_box.get("stage_at"):
            stage_age_ms = round((time.monotonic() - fallback_box["stage_at"]) * 1000)
        print(
            f"[IQ DIAG] TIMEOUT fallback etapa={stage} etapa_ms={stage_age_ms} "
            f"construtor_ms={fallback_box.get('constructor_ms')}",
            flush=True,
        )
        print("[IQ LOGIN] fallback excedeu 20s", flush=True)
        raise TimeoutError(
            f"A biblioteca da IQ Option ficou travada no fallback na etapa {stage}."
        )

    if "error" in fallback_box:
        exc = fallback_box["error"]
        print(
            f"[IQ DIAG] fallback exceção etapa={fallback_box.get('stage')} "
            f"erro={type(exc).__name__}: {str(exc)[:180]}",
            flush=True,
        )
        print(f"[IQ LOGIN] fallback erro={type(exc).__name__}: {str(exc)[:180]}", flush=True)
        raise RuntimeError(f"Fallback IQ Option falhou: {str(exc)[:240]}")

    ok = bool(fallback_box.get("ok"))
    reason = str(fallback_box.get("reason") or "")
    client = fallback_box.get("client")
    print(
        f"[IQ DIAG] fallback terminou etapa={fallback_box.get('stage')} ok={ok} "
        f"connect_ms={fallback_box.get('connect_ms')} reason={reason[:120] or '-'}",
        flush=True,
    )

    if not ok or client is None:
        low = reason.lower()
        if "2fa" in low or "verify" in low:
            raise RuntimeError("A IQ Option está exigindo autenticação em duas etapas (2FA).")
        if "invalid_credentials" in low or "wrong credentials" in low or "invalid credentials" in low:
            raise RuntimeError("E-mail ou senha da IQ Option estão incorretos.")
        raise RuntimeError("A IQ Option recusou a conexão" + (f": {reason}" if reason else "."))

    try:
        fallback_box["stage"] = "check_connect_WebSocket"
        connected = bool(client.check_connect())
        print(f"[IQ DIAG] fallback check_connect={connected}", flush=True)
    except Exception as exc:
        connected = False
        print(f"[IQ DIAG] fallback check_connect erro={type(exc).__name__}: {str(exc)[:160]}", flush=True)

    if not connected:
        raise RuntimeError("A IQ Option abriu a sessão, mas o WebSocket não permaneceu conectado.")

    print("[IQ LOGIN] fallback conectado", flush=True)
    return client

def _iq_reconnect_state(state: Dict[str, Any]):
    if _iq_connected(state):
        state["connected"] = True
        return state["client"]

    email = str(state.get("email") or "").strip()
    password = str(state.get("password") or "")

    if not email or not password:
        raise RuntimeError(
            "Sessão da IQ Option sem credenciais ativas. Faça login novamente."
        )

    _iq_close_state(state)

    client = _iq_connect_fresh(
        email,
        password,
    )

    state["client"] = client
    state["connected"] = True
    state["last_connected"] = time.time()
    state["last_seen"] = time.time()
    state["last_error"] = ""

    return client


def _normalize_iq_candle(item):
    if not isinstance(item, dict):
        return None

    raw_time = (
        item.get("from")
        or item.get("timestamp")
        or item.get("time")
        or item.get("at")
    )

    try:
        ts = float(raw_time)
        if ts > 10_000_000_000:
            ts /= 1000.0

        candle_dt = datetime.fromtimestamp(
            ts,
            tz=UTC,
        ).astimezone(BR_TZ).isoformat()

    except Exception:
        return None

    try:
        return {
            "datetime": candle_dt,
            "open": float(item.get("open")),
            "high": float(item.get("max", item.get("high"))),
            "low": float(item.get("min", item.get("low"))),
            "close": float(item.get("close")),
            "volume": float(item.get("volume", 0) or 0),
        }
    except Exception:
        return None


def _iq_get_candles_once(client, active: str, duration: int, count: int, endtime: float):
    """Leitura direta pelo websocket da iqoptionapi."""
    try:
        import iqoptionapi.constants as iq_constants
    except Exception as exc:
        raise RuntimeError(
            f"Constantes IQ Option indisponíveis: {exc}"
        )

    active_id = iq_constants.ACTIVES.get(active)
    if active_id is None:
        raise RuntimeError(
            f"Ativo OTC não reconhecido pela biblioteca: {active}"
        )

    api = getattr(client, "api", None)
    if api is None:
        raise RuntimeError("Cliente IQ Option sem websocket ativo.")

    candles_obj = getattr(api, "candles", None)
    if candles_obj is None:
        raise RuntimeError("Canal de candles da IQ Option não inicializado.")

    candles_obj.candles_data = None
    api.getcandles(active_id, duration, count, endtime)

    deadline = time.monotonic() + IQ_CANDLE_TIMEOUT
    while time.monotonic() < deadline:
        data = candles_obj.candles_data
        if data is not None:
            return data

        try:
            if not client.check_connect():
                raise RuntimeError(
                    "Websocket IQ Option desconectou durante a leitura de candles."
                )
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(
                f"Falha ao verificar websocket IQ Option: {exc}"
            )

        time.sleep(0.05)

    raise TimeoutError(
        f"A IQ Option não respondeu candles em {IQ_CANDLE_TIMEOUT:.0f} segundos."
    )


def iq_candles_blocking(state: Dict[str, Any], symbol: str, interval: str, n: int, regular_market: bool = False):
    client = _iq_reconnect_state(state)
    duration = iq_seconds(interval)
    count = max(20, min(int(n), 150))
    errors = []

    candidates = iq_regular_active_candidates(symbol) if regular_market else iq_active_candidates(symbol)

    for active in candidates:
        try:
            if not _iq_connected(state):
                client = _iq_reconnect_state(state)

            raw = _iq_get_candles_once(
                client,
                active,
                duration,
                count,
                time.time(),
            )

            out = []
            for item in raw or []:
                candle = _normalize_iq_candle(item)
                if candle:
                    candle["source"] = active
                    out.append(candle)

            if len(out) >= 5:
                out.sort(key=lambda row: row["datetime"])
                state["connected"] = True
                state["last_seen"] = time.time()
                state["last_error"] = ""
                return out[-count:]

            errors.append(f"{active}: sem candles")

        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            errors.append(f"{active}: {msg}")
            state["last_error"] = msg

            low = msg.lower()
            if any(
                key in low
                for key in (
                    "websocket",
                    "desconect",
                    "closed",
                    "timeout",
                    "timed out",
                    "sock",
                )
            ):
                _iq_close_state(state)
                raise RuntimeError(
                    "A conexão da IQ Option caiu: " + msg[:220]
                )

    detail = " | ".join(errors[-3:])
    raise RuntimeError(
        "IQ Option sem candles. " + detail[:350]
    )


async def candles(
    symbol,
    interval,
    n=80,
    market="OPEN",
    iq_state=None,
    request: Request | None = None,
):
    market = (market or "OPEN").upper()

    if market not in VALID_MARKETS:
        raise HTTPException(400, "Mercado inválido.")

    if symbol not in SYMBOLS or interval not in INTERVALS:
        raise HTTPException(400, "Ativo ou intervalo inválido.")

    if market == "OLYMP_OTC":
        return await candles_olymp(
            symbol,
            interval,
            n,
            request=request,
        )

    if market == "IQ_OTC":
        if iq_state is None and request is not None:
            iq_state = _iq_session_state(
                request,
                required=False,
            )

        if iq_state is None:
            raise HTTPException(
                401,
                "Faça login na IQ Option pela aba Corretora para carregar o OTC."
            )

        cache_key = f"{symbol}|{interval}"
        candle_cache = iq_state.setdefault("candle_cache", {})
        cached = candle_cache.get(cache_key)

        if (
            cached
            and time.time() - cached[0] < IQ_CANDLE_CACHE_TTL
            and len(cached[1]) >= min(int(n), 20)
        ):
            return cached[1][-int(n):]

        lock = iq_state.get("lock")
        if lock is None:
            lock = asyncio.Lock()
            iq_state["lock"] = lock

        try:
            async with lock:
                cached = candle_cache.get(cache_key)

                if (
                    cached
                    and time.time() - cached[0] < IQ_CANDLE_CACHE_TTL
                    and len(cached[1]) >= min(int(n), 20)
                ):
                    return cached[1][-int(n):]

                data = await asyncio.wait_for(
                    asyncio.to_thread(
                        iq_candles_blocking,
                        iq_state,
                        symbol,
                        interval,
                        max(80, int(n)),
                    ),
                    timeout=IQ_CANDLE_TIMEOUT + 5,
                )

                if not data:
                    raise RuntimeError(
                        "A IQ Option retornou zero candles."
                    )

                candle_cache[cache_key] = (
                    time.time(),
                    data,
                )
                return data[-int(n):]

        except asyncio.TimeoutError:
            raise HTTPException(
                504,
                "A IQ Option demorou demais para responder aos candles OTC."
            )
        except HTTPException:
            raise
        except Exception as exc:
            # No modo IQ OTC, só aceitamos cache antigo da própria IQ.
            stale = candle_cache.get(cache_key)

            if stale and time.time() - stale[0] < 90:
                return stale[1][-int(n):]

            raise HTTPException(
                503,
                f"IQ Option OTC indisponível: {str(exc)[:260]}"
            )

    return await candles_open(symbol, interval, n)


_gemini_selected_model = None
_gemini_model_checked_at = 0.0

async def _gemini_pick_model(client):
    """Escolhe um modelo generateContent realmente disponível para esta chave/projeto."""
    global _gemini_selected_model, _gemini_model_checked_at
    if _gemini_selected_model and time.time() - _gemini_model_checked_at < 3600:
        return _gemini_selected_model

    headers = {"x-goog-api-key": GEMINI_KEY}
    response = await client.get("https://generativelanguage.googleapis.com/v1beta/models", headers=headers)
    response.raise_for_status()
    models = response.json().get("models") or []
    available = []
    for item in models:
        methods = item.get("supportedGenerationMethods") or item.get("supportedActions") or []
        if "generateContent" not in methods:
            continue
        name = str(item.get("name", "")).replace("models/", "", 1)
        if name:
            available.append(name)

    preferred = ([GEMINI_MODEL] if GEMINI_MODEL else []) + GEMINI_MODEL_FALLBACKS
    for name in preferred:
        if name and name in available:
            _gemini_selected_model = name
            break
    if not _gemini_selected_model:
        flash = [m for m in available if "flash" in m.lower() and "image" not in m.lower()]
        _gemini_selected_model = flash[0] if flash else (available[0] if available else None)
    if not _gemini_selected_model:
        raise RuntimeError("Nenhum modelo Gemini com generateContent disponível para este projeto.")
    _gemini_model_checked_at = time.time()
    print(f"[IA GEMINI] modelo disponível selecionado: {_gemini_selected_model}", flush=True)
    return _gemini_selected_model

async def _gemini_json(prompt):
    """Executa o Gemini sem colocar a chave na URL/log e devolve JSON."""
    global _gemini_selected_model, _gemini_model_checked_at
    if not GEMINI_KEY:
        raise RuntimeError("GEMINI_API_KEY não configurada.")
    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"responseMimeType": "application/json", "temperature": 0.15},
    }
    headers = {"x-goog-api-key": GEMINI_KEY, "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=OAI_TIMEOUT) as client:
        model = await _gemini_pick_model(client)
        url = GEMINI_URL.format(model=model)
        response = await client.post(url, headers=headers, json=body)
        if response.status_code == 404:
            _gemini_selected_model = None
            _gemini_model_checked_at = 0.0
            model = await _gemini_pick_model(client)
            url = GEMINI_URL.format(model=model)
            response = await client.post(url, headers=headers, json=body)
        response.raise_for_status()
        payload = response.json()
    parts = (((payload.get("candidates") or [{}])[0].get("content") or {}).get("parts") or [])
    text = "".join(str(x.get("text", "")) for x in parts if isinstance(x, dict))
    parsed = json_extract(text)
    if not isinstance(parsed, dict):
        raise ValueError("Resposta inválida da IA Gemini.")
    return parsed


async def openai_confirm(symbol, interval, cs, analysis):
    if not GEMINI_KEY:
        return {"available": False, "reason": "GEMINI_API_KEY não configurada."}

    key = f"{symbol}|{interval}|{cs[-1]['datetime']}"
    if key in oai_cache and time.time() - oai_cache[key][0] < 55:
        return oai_cache[key][1]

    data = [
        {"time": c["datetime"], "o": c["open"], "h": c["high"], "l": c["low"], "c": c["close"], "v": c["volume"]}
        for c in cs[-40:]
    ]

    prompt = f"""Você é o módulo de confirmação da MEGA IA.
Ativo {symbol}, timeframe {interval}.
Use SOMENTE candles fechados.
Não invente dados futuros.
Estratégias internas: reversão, tendência, momentum, price action e breakout com filtro de volatilidade.
Análise técnica preliminar: {json.dumps(analysis, ensure_ascii=False)}
Retorne SOMENTE JSON:
{{"direction":"CALL|PUT|NEUTRO","confidence":0,"confirmed":true,"reason":"curto","risk":"LOW|MEDIUM|HIGH"}}
Candles: {json.dumps(data, ensure_ascii=False)}"""

    try:
        p = await _gemini_json(prompt)

        out = {
            "available": True,
            "direction": str(p.get("direction", "NEUTRO")).upper(),
            "confidence": clamp(float(p.get("confidence", 0)), 0, 100),
            "confirmed": bool(p.get("confirmed", False)),
            "risk": str(p.get("risk", "HIGH")).upper(),
            "reason": str(p.get("reason", ""))[:300],
        }

        if out["direction"] not in ("CALL", "PUT", "NEUTRO"):
            out["direction"] = "NEUTRO"

        oai_cache[key] = (time.time(), out)
        return out

    except Exception as exc:
        return {"available": False, "reason": str(exc)[:200]}


async def openai_direct_signal(symbol, interval, cs, market="OPEN"):
    """Modo ONLINE otimizado.

    O painel continua lendo o gráfico a cada ~5 s, mas a API da IA só é chamada
    quando o candle mudou de forma relevante, nasceu um candle novo ou passou
    um tempo máximo sem nova avaliação. Isso mantém resposta rápida sem fazer
    uma chamada cara a cada polling.
    """
    if not GEMINI_KEY:
        return {
            "available": False, "direction": "NEUTRO", "confidence": 0,
            "confirmed": False, "risk": "HIGH",
            "reason": "GEMINI_API_KEY não configurada.",
        }

    if not cs:
        return {
            "available": False, "direction": "NEUTRO", "confidence": 0,
            "confirmed": False, "risk": "HIGH",
            "reason": "Sem candles suficientes.",
        }

    live = cs[-1]
    prev = cs[-2] if len(cs) > 1 else live
    state_key = f"AI_SMART|{market}|{symbol}|{interval}"
    st = ai_scan_state.setdefault(state_key, {})
    now_ts = time.time()

    def f(v, default=0.0):
        try:
            return float(v)
        except Exception:
            return default

    o, h, l, c = (f(live.get(k)) for k in ("open", "high", "low", "close"))
    rng = max(abs(h - l), 1e-12)
    body = abs(c - o)
    prev_rng = max(abs(f(prev.get("high")) - f(prev.get("low"))), 1e-12)
    prev_close = f(prev.get("close"), c)
    volume = f(live.get("volume"))

    last_candle = st.get("candle_time")
    last_close = f(st.get("close"), c)
    last_volume = f(st.get("volume"), volume)
    last_call = f(st.get("last_call"), 0.0)

    new_candle = bool(last_candle and last_candle != live.get("datetime"))
    price_move = abs(c - last_close)
    # Mudança relevante é medida pela faixa recente, sem transformar isso em
    # indicador de decisão. Serve apenas para decidir QUANDO pedir nova análise.
    meaningful_price = price_move >= max(prev_rng * 0.08, rng * 0.06)
    meaningful_body = body >= prev_rng * 0.35
    meaningful_volume = volume > 0 and last_volume > 0 and abs(volume - last_volume) / max(last_volume, 1.0) >= 0.20

    # Nunca chama a API em rajada: mínimo de 12 s. Mesmo sem grande mudança,
    # força uma revisão em até 30 s para não deixar o gráfico sem reavaliação.
    min_gap = 12.0
    heartbeat = 30.0
    due = (now_ts - last_call) >= heartbeat
    changed = new_candle or meaningful_price or meaningful_body or meaningful_volume
    should_call = last_call <= 0 or ((now_ts - last_call) >= min_gap and (changed or due))

    st.update({
        "candle_time": live.get("datetime"),
        "close": c,
        "volume": volume,
        "last_scan": now_ts,
    })

    if not should_call and st.get("last_result"):
        cached = dict(st["last_result"])
        cached["smart_scan"] = True
        cached["api_called"] = False
        cached["next_review_seconds"] = max(0, int(min(heartbeat, max(min_gap, heartbeat - (now_ts - last_call)))))
        return cached

    data = [
        {"time": x["datetime"], "o": x["open"], "h": x["high"], "l": x["low"], "c": x["close"], "v": x.get("volume", 0)}
        for x in cs[-60:]
    ]

    prompt = f"""Você é a inteligência artificial autônoma da MEGA IA.
Ativo: {symbol}. Timeframe: {interval}. Mercado: {market}.
Este é o MODO IA PURA: não receba nem use sinais de RSI, MACD, Bollinger, médias, score técnico ou estratégias internas do aplicativo.
Analise SOMENTE os candles OHLCV fornecidos, os mesmos dados usados no gráfico do painel. O último candle pode estar EM FORMAÇÃO. Avalie contexto, sequência, força, rejeição, estrutura, momentum visível no preço e volume disponível.
Não invente dados futuros. Se não houver vantagem clara, responda NEUTRO.
Só confirme CALL ou PUT quando confidence >= {OAI_MIN:.0f} e risk não for HIGH.
Retorne SOMENTE JSON válido:
{{"direction":"CALL|PUT|NEUTRO","confidence":0,"confirmed":true,"reason":"curto","risk":"LOW|MEDIUM|HIGH"}}
Candles: {json.dumps(data, ensure_ascii=False)}"""

    try:
        parsed = await _gemini_json(prompt)

        direction = str(parsed.get("direction", "NEUTRO")).upper()
        if direction not in ("CALL", "PUT", "NEUTRO"):
            direction = "NEUTRO"
        confidence = clamp(float(parsed.get("confidence", 0) or 0), 0, 100)
        risk = str(parsed.get("risk", "HIGH")).upper()
        confirmed = bool(parsed.get("confirmed", False))
        if direction in ("CALL", "PUT") and (not confirmed or confidence < OAI_MIN or risk == "HIGH"):
            direction, confirmed = "NEUTRO", False

        out = {
            "available": True,
            "direction": direction,
            "confidence": confidence,
            "confirmed": confirmed and direction in ("CALL", "PUT"),
            "risk": risk if risk in ("LOW", "MEDIUM", "HIGH") else "HIGH",
            "reason": str(parsed.get("reason", ""))[:300],
            "smart_scan": True,
            "api_called": True,
        }
        st["last_call"] = now_ts
        st["last_result"] = dict(out)
        return out
    except Exception as exc:
        # Em erro de API, conserva a última leitura apenas como contexto, mas
        # nunca reutiliza um CALL/PUT antigo como novo sinal.
        out = {
            "available": False, "direction": "NEUTRO", "confidence": 0,
            "confirmed": False, "risk": "HIGH", "reason": str(exc)[:200],
            "smart_scan": True, "api_called": True,
        }
        st["last_call"] = now_ts
        st["last_result"] = dict(out)
        return out


def next_boundary(interval):
    seconds = INTERVALS[interval]
    timestamp = int(now().timestamp())
    return datetime.fromtimestamp(((timestamp // seconds) + 1) * seconds, tz=BR_TZ)


def entry_window(interval):
    entry = next_boundary(interval)
    return entry - timedelta(seconds=15), entry, entry + timedelta(seconds=INTERVALS[interval])


def neutral_signal(symbol, interval, market, status, reason, *, confidence=0, source_state="UNAVAILABLE"):
    return {
        "symbol": symbol,
        "interval": interval,
        "market": market,
        "direction": "NEUTRO",
        "confidence": round(float(confidence or 0), 1),
        "entry_time": None,
        "announce_time": None,
        "expiry_time": None,
        "status": status,
        "ai_confirmed": False,
        "risk": "HIGH",
        "strategy": "Proteção de disponibilidade",
        "reason": str(reason)[:300],
        "non_repaint": True,
        "technical": {},
        "source_state": source_state,
    }


async def signal(symbol, interval, market="OPEN", iq_state=None, request: Request | None = None, ai_only: bool = False):
    if symbol not in SYMBOLS or interval not in INTERVALS:
        raise HTTPException(400, "Ativo ou intervalo inválido.")

    market = (market or "OPEN").upper()
    session_part = iq_state.get("session_id", "") if (market == "IQ_OTC" and iq_state) else market
    key = f"{session_part}|{market}|{symbol}|{interval}|AI_ONLY={int(ai_only)}"

    release_key = f"{market}|{symbol}|{interval}|AI_ONLY={int(ai_only)}"
    release_state = signal_release_state.get(release_key) or {}

    active_signal = release_state.get("active_signal")
    if active_signal and active_signal.get("expiry_time"):
        try:
            active_expiry = parse_dt(active_signal["expiry_time"])
        except Exception:
            active_expiry = None

        # Enquanto a operação ainda está ativa, devolve exatamente o MESMO sinal.
        # Isso impede que cada polling gere um novo horário de entrada.
        if active_expiry and now() < active_expiry:
            return active_signal

    if key in cache and time.time() - cache[key][0] < 1:
        return cache[key][1]

    try:
        raw = await candles(symbol, interval, 150, market, iq_state, request=request)
    except HTTPException as exc:
        status = (
            "FONTE EM LIMITE"
            if market == "OPEN" and exc.status_code in (429, 503)
            else ("IQ OPTION RECONECTANDO" if market == "IQ_OTC" else ("OLYMPTRADE INDISPONÍVEL" if market == "OLYMP_OTC" else "FONTE INDISPONÍVEL"))
        )
        out = neutral_signal(symbol, interval, market, status, exc.detail, source_state="DEGRADED")
        cache[key] = (time.time(), out)
        return out
    except Exception as exc:
        status = "IQ OPTION RECONECTANDO" if market == "IQ_OTC" else ("OLYMPTRADE INDISPONÍVEL" if market == "OLYMP_OTC" else "FONTE INDISPONÍVEL")
        out = neutral_signal(symbol, interval, market, status, str(exc), source_state="DEGRADED")
        cache[key] = (time.time(), out)
        return out

    if len(raw) < 25:
        out = neutral_signal(
            symbol, interval, market,
            "AGUARDANDO DADOS",
            "Ainda não há candles suficientes para uma análise segura.",
            source_state="WAITING",
        )
        cache[key] = (time.time(), out)
        return out

    if market == "OPEN":
        age = _td_cache_age(symbol, interval)
        safe_age = max(75.0, INTERVALS[interval] * 0.75)
        if age > safe_age:
            out = neutral_signal(
                symbol, interval, market,
                "AGUARDANDO DADOS ATUALIZADOS",
                "A fonte de mercado está temporariamente limitada. Nenhuma entrada será liberada com candles antigos.",
                source_state="WAITING",
            )
            cache[key] = (time.time(), out)
            return out

    closed = raw[:-1] if len(raw) > 1 else raw

    # MODO ONLINE / IA PURA: analisa continuamente, mas libera no máximo 1 sinal a cada 5 minutos.
    if ai_only:
        release_state = signal_release_state.setdefault(release_key, {})
        ai_cycle_seconds = 300
        last_ai_signal_ts = float(release_state.get("last_ai_signal_ts", 0.0) or 0.0)
        ai_cycle_remaining = max(0, int(ai_cycle_seconds - (time.time() - last_ai_signal_ts))) if last_ai_signal_ts else 0

        # Mesmo durante o bloqueio, o front continua consultando a cada 5s.
        # Não chama a IA novamente até abrir o próximo ciclo, evitando sinais duplicados e custo desnecessário.
        if ai_cycle_remaining > 0:
            active_signal = release_state.get("active_signal")
            if active_signal and active_signal.get("expiry_time"):
                try:
                    if now() < parse_dt(active_signal["expiry_time"]):
                        held = dict(active_signal)
                        held["ai_cycle_remaining"] = ai_cycle_remaining
                        held["ai_cycle_seconds"] = ai_cycle_seconds
                        return held
                except Exception:
                    pass
            out = neutral_signal(
                symbol, interval, market,
                "IA PURA • NOVO CICLO EM %02d:%02d" % divmod(ai_cycle_remaining, 60),
                "A IA já liberou um sinal neste ciclo e continua aguardando a próxima janela de 5 minutos.",
                source_state="READY",
            )
            out.update({"strategy":"IA PURA", "mode":"AI_ONLY", "technical":{"disabled":True,"mode":"AI_ONLY"},
                        "ai_cycle_remaining":ai_cycle_remaining, "ai_cycle_seconds":ai_cycle_seconds})
            cache[key] = (time.time(), out)
            return out

        ai = await openai_direct_signal(symbol, interval, raw, market)
        base = {
            "symbol": symbol,
            "interval": interval,
            "market": market,
            "direction": "NEUTRO",
            "confidence": round(float(ai.get("confidence", 0) or 0), 1),
            "entry_time": None,
            "announce_time": None,
            "expiry_time": None,
            "status": "IA PURA • VARREDURA 5s • ANÁLISE INTELIGENTE",
            "ai_confirmed": bool(ai.get("confirmed", False)),
            "risk": ai.get("risk", "HIGH"),
            "strategy": "IA GEMINI",
            "ai_provider": "GEMINI",
            "reason": ai.get("reason") or "IA analisando os mesmos candles exibidos no gráfico.",
            "non_repaint": True,
            "technical": {"disabled": True, "mode": "AI_ONLY"},
            "source_state": "READY" if ai.get("available") else "AI_UNAVAILABLE",
            "mode": "AI_ONLY",
        }

        if ai.get("available") and ai.get("confirmed") and ai.get("direction") in ("CALL", "PUT"):
            base["direction"] = ai["direction"]
            base["status"] = "SINAL IA GEMINI LIBERADO"
            announce, entry, expiry = entry_window(interval)
            base["entry_time"] = iso(entry)
            base["announce_time"] = iso(announce)
            base["expiry_time"] = iso(expiry)
            base["reference_candle"] = raw[-1]["datetime"] if raw else None
        elif not ai.get("available"):
            ai_reason = str(ai.get("reason") or "Erro não identificado da IA.")
            low_reason = ai_reason.lower()
            if "não configurados" in low_reason or "api_key" in low_reason or "api key" in low_reason:
                base["status"] = "IA INDISPONÍVEL • CONFIGURAÇÃO AUSENTE"
            elif "timeout" in low_reason or "timed out" in low_reason or "tempo" in low_reason:
                base["status"] = "IA INDISPONÍVEL • TIMEOUT"
            elif "401" in low_reason or "unauthorized" in low_reason or "authentication" in low_reason:
                base["status"] = "IA INDISPONÍVEL • CHAVE INVÁLIDA"
            elif "429" in low_reason or "rate limit" in low_reason or "quota" in low_reason:
                base["status"] = "IA INDISPONÍVEL • LIMITE DA API"
            else:
                base["status"] = "IA INDISPONÍVEL • ERRO DA API"
            base["reason"] = ai_reason[:300]
            print(f"[IA STATUS] {symbol} {interval} {base['status']} | {ai_reason[:220]}", flush=True)

        base["ai_cycle_remaining"] = 0
        base["ai_cycle_seconds"] = ai_cycle_seconds
        active_signal = release_state.get("active_signal")
        if active_signal and active_signal.get("expiry_time"):
            try:
                if now() < parse_dt(active_signal["expiry_time"]):
                    return active_signal
            except Exception:
                pass

        if base["direction"] in ("CALL", "PUT"):
            locked_direction = release_state.get("locked_direction")
            if locked_direction == base["direction"]:
                base.update(
                    direction="NEUTRO", entry_time=None, announce_time=None, expiry_time=None,
                    status="IA PURA • AGUARDANDO NOVO SINAL",
                    reason="O sinal anterior da IA já foi utilizado; aguardando uma nova decisão.",
                    risk="HIGH",
                )
            else:
                release_state["locked_direction"] = base["direction"]
                release_state["locked_strategy"] = "IA PURA"
                release_state["active_signal"] = dict(base)
                release_state["last_ai_signal_ts"] = time.time()
        else:
            release_state["locked_direction"] = None
            release_state["locked_strategy"] = None
            release_state["active_signal"] = None

        cache[key] = (time.time(), base)
        return base

    # Mercado aberto mantém as estratégias/indicadores já existentes,
    # incluindo o mapa H1/H4. OTC usa SOMENTE o motor OTC próprio.
    h1_closed = None
    h4_closed = None
    m5_closed = None

    # Para entradas M1 em OTC, a tendência principal é confirmada no M5.
    if market in ("IQ_OTC", "OLYMP_OTC") and interval == "1min":
        try:
            m5_raw = await candles(symbol, "5min", 80, market, iq_state, request=request)
            m5_closed = m5_raw[:-1] if len(m5_raw) > 1 else m5_raw
        except Exception:
            m5_closed = None

    if market == "OPEN" and interval in ("5min", "15min"):
        try:
            h1_raw, h4_raw = await asyncio.gather(
                candles(symbol, "1h", 100, market, iq_state, request=request),
                candles(symbol, "4h", 100, market, iq_state, request=request),
            )
            h1_closed = h1_raw[:-1] if len(h1_raw) > 1 else h1_raw
            h4_closed = h4_raw[:-1] if len(h4_raw) > 1 else h4_raw
        except Exception:
            h1_closed = None
            h4_closed = None

    analysis = strategy_engine_for_market(
        closed,
        market,
        {
            "h1": h1_closed,
            "h4": h4_closed,
            "m5": m5_closed,
            "interval": interval,
            "symbol": symbol,
            "market": market,
        },
    )

    # Se a condição anterior deixou de existir, libera o gate para um novo setup.
    release_state = signal_release_state.setdefault(release_key, {})
    if not analysis.get("confirmed") or analysis.get("direction") not in ("CALL", "PUT"):
        release_state["locked_direction"] = None
        release_state["locked_strategy"] = None
        release_state["active_signal"] = None

    base = {
        "symbol": symbol,
        "interval": interval,
        "market": market,
        "direction": "NEUTRO",
        "confidence": analysis["confidence"],
        "entry_time": None,
        "announce_time": None,
        "expiry_time": None,
        "status": "MONITORANDO OTC" if market in ("IQ_OTC", "OLYMP_OTC") else "MONITORANDO MERCADO ABERTO",
        "ai_confirmed": False,
        "risk": "HIGH",
        "strategy": analysis["strategy"],
        "reason": analysis["reason"],
        "non_repaint": True,
        "technical": analysis,
    }

    if analysis["confirmed"]:
        ai = await openai_confirm(symbol, interval, closed, analysis)

        if not ai.get("available"):
            base.update(
                direction=analysis["direction"],
                confidence=analysis["confidence"],
                status="SINAL TÉCNICO",
                risk="MEDIUM",
            )
        elif (
            ai["direction"] == analysis["direction"]
            and ai["confirmed"]
            and ai["confidence"] >= OAI_MIN
            and ai["risk"] != "HIGH"
        ):
            base.update(
                direction=analysis["direction"],
                confidence=round(clamp(analysis["confidence"] * .45 + ai["confidence"] * .55, 0, 97), 1),
                status="SINAL LIBERADO",
                ai_confirmed=True,
                risk=ai["risk"],
                reason=ai.get("reason") or analysis["reason"],
            )
        else:
            base.update(
                direction="NEUTRO",
                confidence=round(min(analysis["confidence"], ai["confidence"]), 1),
                status="AGUARDANDO CONFIRMAÇÃO DA IA",
                risk=ai.get("risk", "HIGH"),
                reason=ai.get("reason") or "A IA não confirmou.",
            )

    if base["direction"] in ("CALL", "PUT"):
        release_state = signal_release_state.setdefault(release_key, {})
        locked_direction = release_state.get("locked_direction")
        locked_strategy = release_state.get("locked_strategy")

        # O mesmo setup não pode liberar uma nova entrada minuto após minuto.
        # Para voltar a liberar na mesma direção, o setup precisa primeiro desaparecer
        # (ficar NEUTRO) e depois se formar novamente.
        if (
            locked_direction == base["direction"]
            and locked_strategy == base.get("strategy")
        ):
            base.update(
                direction="NEUTRO",
                entry_time=None,
                announce_time=None,
                expiry_time=None,
                status="AGUARDANDO NOVO SETUP",
                reason="O sinal anterior já foi utilizado. Aguardando a condição desaparecer e se formar novamente.",
                risk="HIGH",
            )
        else:
            announce, entry, expiry = entry_window(interval)
            base["entry_time"] = iso(entry)
            base["announce_time"] = iso(announce)
            base["expiry_time"] = iso(expiry)
            base["reference_candle"] = closed[-1]["datetime"] if closed else None

            release_state["locked_direction"] = base["direction"]
            release_state["locked_strategy"] = base.get("strategy")
            release_state["active_signal"] = dict(base)

    base["source_state"] = "READY"

    # Atualiza a cópia ativa já com source_state.
    if base["direction"] in ("CALL", "PUT"):
        release_state = signal_release_state.setdefault(release_key, {})
        release_state["active_signal"] = dict(base)

    cache[key] = (time.time(), base)
    return base


@app.get("/health")
async def health():
    now_ts = time.time()
    return {
        "status": "ok",
        "app": "MEGA IA",
        "version": "33.35.0",
        "brasilia_time": iso(now()),
        "twelve_data": {
            "configured": bool(TD_KEY),
            "backoff": now_ts < td_backoff_until,
            "retry_in": max(0, int(td_backoff_until - now_ts)),
            "cache_items": len(td_candle_cache),
            "last_reason": td_backoff_reason[:120],
        },
        "iq_option": {
            "library": bool(IQ_Option is not None),
            "sessions": len(iq_sessions),
        },
        "openai": {
            "configured": bool(OAI_KEY and OAI_MODEL),
            "model": OAI_MODEL or None,
        },
    }


@app.get("/server-time")
async def server_time():
    return {"datetime": iso(now()), "timezone": "America/Sao_Paulo"}


@app.get("/license")
async def license_info():
    try:
        exp = datetime.strptime(LICENSE, "%Y-%m-%d").date()
        active = now().date() <= exp
    except Exception:
        active = False

    if active:
        return {"active": True}

    return {
        "active": False,
        "message": "Licença expirada. Entre em contato para renovar o aplicativo.",
        "whatsapp_1": WA1,
        "whatsapp_2": WA2,
        "instagram": IG,
    }


@app.get("/mega-ia.png")
async def mega_ia_image():
    if not os.path.exists(IMAGE_PATH):
        raise HTTPException(404, "Imagem MEGA IA não encontrada.")
    return FileResponse(IMAGE_PATH, media_type="image/png")


@app.get("/mega-ia-icon.png")
async def mega_ia_icon():
    if not os.path.exists(ICON_512_PATH):
        raise HTTPException(404, "Ícone MEGA IA não encontrado.")
    return FileResponse(
        ICON_512_PATH,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@app.get("/mega-ia-icon-192.png")
async def mega_ia_icon_192():
    if not os.path.exists(ICON_192_PATH):
        raise HTTPException(404, "Ícone 192x192 MEGA IA não encontrado.")
    return FileResponse(
        ICON_192_PATH,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@app.get("/manifest.webmanifest")
async def manifest():
    manifest_data = {
        "id": "/mega-ia-trader-v44",
        "name": "Mega IA Trader",
        "short_name": "Mega IA",
        "description": "Mega IA Trader",
        "start_url": "/?pwa=v44",
        "scope": "/",
        "display": "standalone",
        "orientation": "portrait",
        "background_color": "#02050b",
        "theme_color": "#07182b",
        "icons": [
            {"src": "/mega-ia-icon-192.png?v=44", "sizes": "192x192", "type": "image/png", "purpose": "any"},
            {"src": "/mega-ia-icon.png?v=44", "sizes": "512x512", "type": "image/png", "purpose": "any"},
            {"src": "/mega-ia-icon.png?v=44", "sizes": "512x512", "type": "image/png", "purpose": "maskable"},
        ],
    }
    return Response(
        content=json.dumps(manifest_data, ensure_ascii=False),
        media_type="application/manifest+json",
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


def _olymp_session_state(request: Request):
    token = request.headers.get("X-OLYMP-Session", "") or request.cookies.get(OLYMP_SESSION_COOKIE, "")
    state = olymp_sessions.get(token)
    if state:
        state["last_seen"] = time.time()
    return state


def _olymp_login_blocking(email: str, password: str):
    if OlympTradeLoginClient is None:
        raise RuntimeError(
            "O conector Olymptrade por e-mail/senha não está instalado no servidor "
            "(módulo olymptradeapi)."
        )

    client = OlympTradeLoginClient(email, password)
    result = client.connect()

    if isinstance(result, (list, tuple)):
        ok = bool(result[0]) if result else False
        reason = str(result[1]) if len(result) > 1 else ""
    else:
        ok = bool(result)
        reason = ""

    if not ok:
        raise RuntimeError(reason or "A Olymptrade recusou o login.")

    return client


@app.post("/iq-login")
async def iq_login(body: IQLoginBody, response: Response):
    email = body.email.strip()
    password = body.password
    print(f"[IQ LOGIN] POST recebido dominio={email.split('@')[-1] if '@' in email else 'invalido'}", flush=True)

    if not email or not password:
        raise HTTPException(
            400,
            "Informe e-mail e senha da IQ Option."
        )

    if IQ_Option is None:
        raise HTTPException(
            503,
            "Biblioteca iqoptionapi não carregada no servidor."
        )

    token = secrets.token_urlsafe(32)

    client = None
    last_exc = None

    # _iq_connect_fresh já possui stable_api + fallback interno.
    # Uma única tentativa evita deixar duas conexões presas no Render.
    for attempt in range(1):
        print("[IQ LOGIN] conexão robusta stable_api + fallback", flush=True)
        try:
            client = await asyncio.wait_for(
                asyncio.to_thread(
                    _iq_connect_fresh,
                    email,
                    password,
                ),
                timeout=58,
            )
            if client is not None:
                break
        except Exception as exc:
            last_exc = exc
            low = str(exc).lower()
            fatal = (
                "senha" in low
                or "credenciais" in low
                or "invalid_credentials" in low
                or "2fa" in low
                or "duas etapas" in low
            )
            if fatal or attempt >= 0:
                break
            await asyncio.sleep(1.5)

    if client is None:
        exc = last_exc or RuntimeError("Falha desconhecida ao conectar.")
        print(f"[IQ LOGIN] falhou: {type(exc).__name__}: {str(exc)[:260]}", flush=True)
        if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
            raise HTTPException(
                504,
                "A IQ Option não respondeu ao servidor dentro do tempo limite. Tente novamente em alguns segundos."
            )
        raise HTTPException(
            401,
            "Não foi possível conectar à IQ Option: "
            + str(exc)[:280]
        )

    print("[IQ LOGIN] conectado com sucesso", flush=True)

    state = {
        "session_id": token,
        "email": email,
        "password": password,
        "client": client,
        "lock": asyncio.Lock(),
        "connected": True,
        "last_seen": time.time(),
        "last_connected": time.time(),
        "last_error": "",
        "candle_cache": {},
        "results": {},
    }
    iq_sessions[token] = state

    response.set_cookie(
        IQ_SESSION_COOKIE,
        token,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=IQ_SESSION_TTL,
        expires=IQ_SESSION_TTL,
        path="/",
    )

    return {
        "connected": True,
        "message": "IQ Option conectada.",
        "email_masked": _mask_email(email),
        "session_token": token,
    }


@app.post("/iq-logout")
async def iq_logout(
    request: Request,
    response: Response,
):
    header_token = request.headers.get(
        "X-IQ-Session",
        "",
    )
    cookie_token = request.cookies.get(
        IQ_SESSION_COOKIE,
        "",
    )

    token = (
        header_token
        if header_token in iq_sessions
        else cookie_token
    )

    state = iq_sessions.pop(token, None)

    if state:
        state["password"] = ""
        state["candle_cache"] = {}
        _iq_close_state(state)

    response.delete_cookie(
        IQ_SESSION_COOKIE,
        path="/",
    )

    return {
        "connected": False,
        "message": "IQ Option desconectada.",
    }


@app.post("/olymp-login")
async def olymp_login(body: OlympLoginBody, response: Response):
    email = body.email.strip()
    password = body.password

    if not email or not password:
        raise HTTPException(400, "Informe e-mail e senha da Olymptrade.")

    token = secrets.token_urlsafe(32)

    try:
        client = await asyncio.wait_for(
            asyncio.to_thread(_olymp_login_blocking, email, password),
            timeout=30,
        )
    except asyncio.TimeoutError:
        raise HTTPException(504, "A tentativa de login da Olymptrade excedeu o tempo limite.")
    except Exception as exc:
        raise HTTPException(
            401,
            "Não foi possível conectar à Olymptrade por e-mail/senha: " + str(exc)[:300]
        )

    olymp_sessions[token] = {
        "session_id": token,
        "email": email,
        "password": password,
        "client": client,
        "last_seen": time.time(),
        "candle_cache": {},
        "results": {},
    }

    response.set_cookie(
        OLYMP_SESSION_COOKIE,
        token,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=OLYMP_SESSION_TTL,
        expires=OLYMP_SESSION_TTL,
        path="/",
    )

    return {
        "connected": True,
        "message": "Olymptrade conectada.",
        "email_masked": _mask_email(email),
        "session_token": token,
    }


@app.post("/olymp-logout")
async def olymp_logout(request: Request, response: Response):
    token = (
        request.headers.get("X-OLYMP-Session", "")
        or request.cookies.get(OLYMP_SESSION_COOKIE, "")
    )
    state = olymp_sessions.pop(token, None)

    if state:
        state["password"] = ""
        client = state.get("client")
        try:
            close = getattr(client, "close", None) or getattr(client, "disconnect", None)
            if close:
                close()
        except Exception:
            pass

    response.delete_cookie(OLYMP_SESSION_COOKIE, path="/")
    return {"connected": False, "message": "Conta Olymptrade desconectada."}


@app.get("/otc-status")
async def otc_status(request: Request, broker: str = "IQ_OPTION"):
    broker = (broker or "IQ_OPTION").upper()

    if broker == "OLYMPTRADE":
        session_state = _olymp_session_state(request)
        session_connected = bool(session_state and session_state.get("client"))
        token_configured = bool(OLYMPTRADE_TOKEN) and OlympTradeClient is not None
        configured = session_connected or token_configured
        return {
            "broker": "OLYMPTRADE",
            "configured": configured,
            "connected": session_connected or bool(olymp_client),
            "message": (
                "Olymptrade conectada pelo painel."
                if session_connected
                else (
                    "Olymptrade OTC configurada por token no servidor."
                    if token_configured
                    else "Faça login com e-mail e senha no painel."
                )
            ),
            "pairs": len(OTC_BASE),
        }

    state = _iq_session_state(
        request,
        required=False,
    )
    connected = _iq_connected(state)

    return {
        "broker": "IQ_OPTION",
        "configured": IQ_Option is not None,
        "connected": connected,
        "message": (
            "IQ Option conectada pelo painel."
            if connected
            else (
                "Faça login com e-mail e senha na aba Corretora."
                if IQ_Option is not None
                else "Biblioteca iqoptionapi não carregada no servidor."
            )
        ),
        "pairs": len(OTC_BASE),
    }


@app.get("/candles")
async def candles_endpoint(
    request: Request,
    symbol: str = "EUR/USD",
    interval: str = "1min",
    n: int = 80,
    market: str = "OPEN",
    mirror_iq: bool = False,
):
    market = (market or "OPEN").upper()

    if symbol not in SYMBOLS or interval not in INTERVALS or market not in VALID_MARKETS:
        raise HTTPException(400, "Ativo, intervalo ou mercado inválido.")

    n = max(20, min(int(n), 150))

    # Espelho do gráfico da IQ Option: sem sessão IQ, o gráfico fica OFFLINE.
    if mirror_iq:
        iq_state = _iq_session_state(request, required=False)
        if not iq_state or not _iq_connected(iq_state):
            return {
                "ok": False,
                "symbol": symbol,
                "interval": interval,
                "market": market,
                "candles": [],
                "status": "GRÁFICO OFFLINE",
                "message": "Conecte sua conta da IQ Option para espelhar o gráfico.",
                "mirror_iq": True,
            }
        try:
            # OTC usa o fluxo OTC existente. Mercado aberto busca o ativo normal da IQ.
            if market == "IQ_OTC":
                values = await candles(symbol, interval, n, "IQ_OTC", iq_state, request=request)
            else:
                lock = iq_state.get("lock") or asyncio.Lock()
                iq_state["lock"] = lock
                async with lock:
                    values = await asyncio.wait_for(
                        asyncio.to_thread(
                            iq_candles_blocking,
                            iq_state,
                            symbol,
                            interval,
                            max(80, int(n)),
                            True,
                        ),
                        timeout=IQ_CANDLE_TIMEOUT + 5,
                    )
            return {
                "ok": True,
                "symbol": symbol,
                "interval": interval,
                "market": market,
                "candles": values[-n:],
                "status": "IQ OPTION ESPELHADA",
                "mirror_iq": True,
            }
        except Exception as exc:
            return {
                "ok": False,
                "symbol": symbol,
                "interval": interval,
                "market": market,
                "candles": [],
                "status": "GRÁFICO IQ INDISPONÍVEL",
                "message": str(exc)[:220],
                "mirror_iq": True,
            }

    state = _iq_session_state(request, required=False) if market == "IQ_OTC" else None

    try:
        if market == "IQ_OTC" and not state:
            return {
                "ok": False,
                "symbol": symbol,
                "interval": interval,
                "market": market,
                "candles": [],
                "status": "LOGIN NECESSÁRIO",
                "message": "Faça login na IQ Option pela aba Corretora para carregar o OTC real.",
            }

        values = await candles(symbol, interval, n, market, state, request=request)

        return {
            "ok": True,
            "symbol": symbol,
            "interval": interval,
            "market": market,
            "candles": values,
            "status": "OK",
        }

    except HTTPException as exc:
        stale = []

        if market == "OPEN":
            item = td_candle_cache.get(f"{symbol}|{interval}")
            if item:
                stale = item[1][-n:]

        elif market == "IQ_OTC" and state:
            item = state.setdefault("candle_cache", {}).get(f"{symbol}|{interval}")
            if item:
                stale = item[1][-n:]
        elif market == "OLYMP_OTC":
            item = olymp_candle_cache.get(f"{symbol}|{interval}")
            if item:
                stale = item[1][-n:]

        return {
            "ok": False,
            "symbol": symbol,
            "interval": interval,
            "market": market,
            "candles": stale,
            "status": "TEMPORARIAMENTE INDISPONÍVEL",
            "message": str(exc.detail)[:220],
            "stale": bool(stale),
        }

    except Exception as exc:
        return {
            "ok": False,
            "symbol": symbol,
            "interval": interval,
            "market": market,
            "candles": [],
            "status": "TEMPORARIAMENTE INDISPONÍVEL",
            "message": str(exc)[:220],
        }


@app.get("/signal-ai")
async def signal_ai(request: Request, symbol="EUR/USD", interval="1min", market="OPEN", ai_only: bool = False):
    requested_market = (market or "OPEN").upper()

    if symbol not in SYMBOLS or interval not in INTERVALS or requested_market not in VALID_MARKETS:
        raise HTTPException(400, "Ativo, intervalo ou mercado inválido.")

    state = _iq_session_state(request, required=False) if requested_market == "IQ_OTC" else None
    fallback_twelve = requested_market == "IQ_OTC" and not state
    effective_market = "OPEN" if fallback_twelve else requested_market

    try:
        data = await signal(
            symbol,
            interval,
            effective_market,
            state if effective_market == "IQ_OTC" else None,
            request=request,
            ai_only=ai_only,
        )
        if isinstance(data, dict):
            # Sempre informa ao frontend qual mercado foi pedido e qual fonte
            # realmente gerou o sinal. Isto evita fechar um sinal IQ_OTC como OPEN.
            data["requested_market"] = requested_market
            data["feed_source"] = "TWELVE_DATA" if fallback_twelve else effective_market
            data["feed_fallback"] = bool(fallback_twelve)
            if fallback_twelve:
                data["feed_message"] = "IQ Option desconectada: sinais usando Twelve Data (mercado aberto)."
        _remember_accounting_signal(request, data)
        return data
    except Exception as exc:
        return neutral_signal(
            symbol,
            interval,
            effective_market,
            "FONTE TEMPORARIAMENTE INDISPONÍVEL",
            str(exc),
            source_state="DEGRADED",
        )


@app.get("/signal")
async def get_signal(request: Request, symbol="EUR/USD", interval="1min", market="OPEN"):
    return await signal_ai(request, symbol, interval, market)


@app.get("/ai-analysis")
async def ai_analysis(request: Request, symbol: str = "EUR/USD", interval: str = "1min", market: str = "OPEN"):
    data = await signal_ai(request, symbol, interval, market)
    public_keys = [
        "symbol", "interval", "direction", "confidence", "status",
        "ai_confirmed", "risk", "entry_time", "announce_time",
        "expiry_time", "non_repaint",
    ]
    return {k: data.get(k) for k in public_keys}


def _pre_signal_from_live_candle(raw, interval: str, market: str = "OPEN", symbol: str | None = None):
    """
    Pré-sinal NÃO confirmado.
    Avalia a vela atual ainda em formação como se fechasse naquele instante.
    Serve apenas para avisar que um CALL/PUT está perto de confirmar.
    """
    if not raw or len(raw) < 35:
        return None

    preview = strategy_engine_for_market(
        raw,
        market,
        {"interval": interval, "symbol": symbol, "market": market},
    )

    if not preview.get("confirmed"):
        return None

    direction = preview.get("direction")
    if direction not in ("CALL", "PUT"):
        return None

    confidence = float(preview.get("confidence", 0) or 0)

    return {
        "direction": direction,
        "confidence": round(confidence, 1),
        "strategy": preview.get("strategy", "Motor multiestratégia"),
        "reason": preview.get("reason", "Condição técnica próxima de confirmar."),
    }


@app.get("/pre-signals")
async def pre_signals(
    request: Request,
    interval: str = "1min",
    market: str = "OPEN",
    limit: int = 4,
):
    market = (market or "OPEN").upper()
    limit = max(1, min(int(limit), 4))

    if interval not in INTERVALS or market not in VALID_MARKETS:
        raise HTTPException(400, "Intervalo ou mercado inválido.")

    requested_market = market
    iq_state = (
        _iq_session_state(request, required=False)
        if requested_market == "IQ_OTC"
        else None
    )
    fallback_twelve = requested_market == "IQ_OTC" and not iq_state
    if fallback_twelve:
        market = "OPEN"

    if market == "OLYMP_OTC":
        olymp_state = _olymp_session_state(request)
        if (
            not olymp_state
            and (not OLYMPTRADE_TOKEN or OlympTradeClient is None)
        ):
            return {
                "ok": False,
                "message": "Olymptrade não conectada.",
                "items": [],
            }

    entry_dt = next_boundary(interval)
    seconds_to_entry = int(max(0, (entry_dt - now()).total_seconds()))

    # A lista só representa "faltando até 1 minuto".
    # Para M5/M15/M30 ela aparece apenas no último minuto da vela.
    if seconds_to_entry > 60:
        return {
            "ok": True,
            "message": f"Aguardando a janela de 1 minuto antes da próxima entrada ({seconds_to_entry}s).",
            "items": [],
            "seconds_to_entry": seconds_to_entry,
        }

    group_key = f"{market}|{interval}"
    pointer_key = f"PRE_SIGNAL_INDEX|{group_key}"
    pointer = int(cache.get(pointer_key, (0, 0))[1] or 0) % len(SYMBOLS)

    batch_size = min(len(SYMBOLS), max(PRE_SIGNAL_BATCH, limit))
    batch = [
        SYMBOLS[(pointer + i) % len(SYMBOLS)]
        for i in range(batch_size)
    ]
    cache[pointer_key] = (
        time.time(),
        (pointer + batch_size) % len(SYMBOLS),
    )

    # Remove candidatos vencidos.
    now_ts = time.time()
    stale_keys = []
    for k, payload in pre_signal_cache.items():
        if not k.startswith(group_key + "|"):
            continue
        if now_ts - float(payload.get("updated_at", 0)) > PRE_SIGNAL_TTL:
            stale_keys.append(k)
            continue
        try:
            if parse_dt(payload["entry_time"]) <= now():
                stale_keys.append(k)
        except Exception:
            stale_keys.append(k)

    for k in stale_keys:
        pre_signal_cache.pop(k, None)

    # Atualiza um lote por chamada para não sobrecarregar o feed OTC.
    for symbol in batch:
        key = f"{group_key}|{symbol}"
        try:
            raw = await candles(
                symbol,
                interval,
                90,
                market,
                iq_state,
                request=request,
            )

            preview = _pre_signal_from_live_candle(raw, interval, market, symbol)

            if preview:
                pre_signal_cache[key] = {
                    "symbol": symbol,
                    "direction": preview["direction"],
                    "confidence": preview["confidence"],
                    "strategy": preview["strategy"],
                    "reason": preview["reason"],
                    "entry_time": iso(entry_dt),
                    "seconds_to_entry": seconds_to_entry,
                    "updated_at": time.time(),
                    "status": "PRÉ-SINAL • AGUARDANDO FECHAMENTO",
                }
            else:
                pre_signal_cache.pop(key, None)

        except Exception:
            # Um ativo com erro não derruba os outros.
            continue

    items = []
    for key, payload in pre_signal_cache.items():
        if not key.startswith(group_key + "|"):
            continue

        try:
            entry = parse_dt(payload["entry_time"])
            remain = int(max(0, (entry - now()).total_seconds()))
        except Exception:
            continue

        if remain > 60:
            continue

        item = dict(payload)
        item["seconds_to_entry"] = remain
        item.pop("updated_at", None)
        items.append(item)

    items.sort(
        key=lambda x: (
            -float(x.get("confidence", 0) or 0),
            int(x.get("seconds_to_entry", 999)),
        )
    )

    return {
        "ok": True,
        "message": (
            "Pré-sinais calculados com a vela em formação. "
            "O CALL/PUT só é confirmado no fechamento."
        ),
        "items": items[:limit],
        "seconds_to_entry": seconds_to_entry,
        "feed_source": "TWELVE_DATA" if fallback_twelve else market,
        "feed_fallback": fallback_twelve,
        "requested_market": requested_market,
    }



@app.get("/chart-pre-signal")
async def chart_pre_signal(
    request: Request,
    symbol: str = "EUR/USD",
    interval: str = "1min",
    market: str = "OPEN",
):
    """
    SINAL DE ENTRADA NA BOLINHA, SEM REPINTAR:
    - só pode nascer nos últimos 20 segundos da vela atual;
    - exige 3 leituras consecutivas na mesma direção antes de liberar a bolinha;
    - se a direção mudar ou o setup desaparecer, a contagem reinicia;
    - depois que a bolinha aparece, direção/confiança/estratégia ficam TRAVADAS;
    - depois de um sinal, outro só pode ser liberado após 3 minutos;
    - a operação indicada continua sendo avaliada na vela seguinte.
    """
    market = (market or "OPEN").upper()

    if symbol not in SYMBOLS or interval not in INTERVALS or market not in VALID_MARKETS:
        raise HTTPException(400, "Ativo, intervalo ou mercado inválido.")

    requested_market = market
    iq_state = _iq_session_state(request, required=False) if requested_market == "IQ_OTC" else None
    fallback_twelve = requested_market == "IQ_OTC" and not iq_state
    if fallback_twelve:
        market = "OPEN"

    entry_dt = next_boundary(interval)
    seconds_to_entry = int(max(0, (entry_dt - now()).total_seconds()))
    entry_iso = iso(entry_dt)
    lock_key = f"{market}|{symbol}|{interval}"

    # Intervalo mínimo entre sinais da bolinha: 3 minutos.
    last_signal_at = float(chart_pre_signal_last_at.get(lock_key, 0.0) or 0.0)
    cooldown_remaining = max(
        0,
        int(CHART_SIGNAL_COOLDOWN_SECONDS - (time.time() - last_signal_at))
    )

    # Limpa trava antiga somente quando a vela de entrada já começou.
    locked = chart_pre_signal_lock.get(lock_key)
    if locked:
        try:
            locked_entry = parse_dt(locked["entry_time"])
            if now() >= locked_entry:
                chart_pre_signal_lock.pop(lock_key, None)
                chart_pre_signal_candidate.pop(lock_key, None)
                locked = None
        except Exception:
            chart_pre_signal_lock.pop(lock_key, None)
            locked = None

    # Se já apareceu uma bolinha neste ciclo, NÃO recalcula a direção.
    if locked:
        out = dict(locked)
        out["seconds_to_entry"] = int(
            max(0, (parse_dt(out["entry_time"]) - now()).total_seconds())
        )
        out["locked"] = True
        out["cooldown_remaining"] = cooldown_remaining
        return out

    if cooldown_remaining > 0:
        chart_pre_signal_candidate.pop(lock_key, None)
        return {
            "ok": True,
            "active": False,
            "locked": False,
            "cooldown": True,
            "cooldown_remaining": cooldown_remaining,
            "seconds_to_entry": seconds_to_entry,
            "entry_time": entry_iso,
            "status": "AGUARDANDO 3 MINUTOS ENTRE SINAIS",
        }

    # Antes dos 20 segundos não existe marcação.
    if seconds_to_entry > 20:
        chart_pre_signal_candidate.pop(lock_key, None)
        return {
            "ok": True,
            "active": False,
            "locked": False,
            "seconds_to_entry": seconds_to_entry,
            "entry_time": entry_iso,
        }

    try:
        raw = await candles(
            symbol,
            interval,
            150,
            market,
            iq_state,
            request=request,
        )

        preview = _pre_signal_from_live_candle(raw, interval, market)

        if not preview:
            # Setup desapareceu: perde a sequência e precisa confirmar 3 vezes novamente.
            chart_pre_signal_candidate.pop(lock_key, None)
            return {
                "ok": True,
                "active": False,
                "locked": False,
                "confirming": False,
                "confirmation_count": 0,
                "confirmation_required": CHART_SIGNAL_CONFIRM_READS,
                "seconds_to_entry": seconds_to_entry,
                "entry_time": entry_iso,
            }

        current_candle = raw[-1] if raw else None

        # CONFIRMAÇÃO DE ESTABILIDADE:
        # a mesma direção precisa aparecer em 3 leituras consecutivas.
        now_ts = time.time()
        direction = preview["direction"]
        candidate = chart_pre_signal_candidate.get(lock_key)

        same_sequence = bool(
            candidate
            and candidate.get("entry_time") == entry_iso
            and candidate.get("direction") == direction
            and (now_ts - float(candidate.get("last_seen", 0.0) or 0.0)) <= CHART_SIGNAL_CONFIRM_MAX_GAP
        )

        confirmation_count = int(candidate.get("count", 0)) + 1 if same_sequence else 1
        chart_pre_signal_candidate[lock_key] = {
            "entry_time": entry_iso,
            "direction": direction,
            "count": confirmation_count,
            "last_seen": now_ts,
        }

        if confirmation_count < CHART_SIGNAL_CONFIRM_READS:
            return {
                "ok": True,
                "active": False,
                "locked": False,
                "confirming": True,
                "direction": direction,
                "confirmation_count": confirmation_count,
                "confirmation_required": CHART_SIGNAL_CONFIRM_READS,
                "seconds_to_entry": seconds_to_entry,
                "entry_time": entry_iso,
                "status": f"CONFIRMANDO {direction} • {confirmation_count}/{CHART_SIGNAL_CONFIRM_READS}",
            }

        # Terceira leitura consecutiva confirmou o setup: agora trava a bolinha.
        payload = {
            "ok": True,
            "active": True,
            "locked": True,
            "direction": preview["direction"],
            "confidence": preview["confidence"],
            "strategy": preview["strategy"],
            "reason": preview["reason"],
            "seconds_to_entry": seconds_to_entry,
            "entry_time": entry_iso,
            "expiry_time": iso(entry_dt + timedelta(seconds=INTERVALS[interval])),
            "market": market,
            "symbol": symbol,
            "interval": interval,
            "reference_candle": (
                current_candle.get("datetime")
                if isinstance(current_candle, dict)
                else None
            ),
            "status": f"ENTRADA {preview['direction']} • SINAL NA BOLINHA",
            "signal_market": "MERCADO ABERTO" if market == "OPEN" else "OTC",
            "cooldown_seconds": CHART_SIGNAL_COOLDOWN_SECONDS,
            "cooldown_remaining": CHART_SIGNAL_COOLDOWN_SECONDS,
            "confirmation_count": CHART_SIGNAL_CONFIRM_READS,
            "confirmation_required": CHART_SIGNAL_CONFIRM_READS,
            "feed_source": "TWELVE_DATA" if fallback_twelve else market,
            "feed_fallback": fallback_twelve,
            "requested_market": requested_market,
        }

        chart_pre_signal_lock[lock_key] = dict(payload)
        chart_pre_signal_candidate.pop(lock_key, None)
        chart_pre_signal_last_at[lock_key] = time.time()
        _remember_accounting_signal(request, payload)
        return payload

    except Exception as exc:
        # Erro temporário não inventa nem troca sinal.
        return {
            "ok": False,
            "active": False,
            "locked": False,
            "seconds_to_entry": seconds_to_entry,
            "entry_time": entry_iso,
            "message": str(exc)[:220],
        }

@app.get("/radar")
async def radar(request: Request, interval="1min", market="OPEN"):
    market = (market or "OPEN").upper()

    if interval not in INTERVALS or market not in VALID_MARKETS:
        raise HTTPException(400, "Intervalo ou mercado inválido.")

    requested_market = market
    iq_state = _iq_session_state(request, required=False) if requested_market == "IQ_OTC" else None
    fallback_twelve = requested_market == "IQ_OTC" and not iq_state
    if fallback_twelve:
        market = "OPEN"

    if market == "OLYMP_OTC" and not _olymp_session_state(request) and (not OLYMPTRADE_TOKEN or OlympTradeClient is None):
        return [
            {
                "symbol": s + " • OLYMP OTC",
                "direction": "NEUTRO",
                "confidence": 0,
                "status": "OLYMP OTC NÃO CONFIGURADA",
            }
            for s in SYMBOLS
        ]

    rkey = f"{market}|{interval}"
    previous = radar_cache.get(rkey)
    suffix = "" if market == "OPEN" else (" • IQ OTC" if market == "IQ_OTC" else " • OLYMP OTC")

    out = list(previous[1]) if previous else [
        {
            "symbol": sym + suffix,
            "direction": "NEUTRO",
            "confidence": 0,
            "status": "AGUARDANDO LEITURA",
        }
        for sym in SYMBOLS
    ]

    # Aquece apenas um par por ciclo para não sobrecarregar os feeds.
    idx_key = f"RADAR_INDEX|{market}|{interval}"
    idx = int(cache.get(idx_key, (0, 0))[1] or 0) % len(SYMBOLS)
    sym = SYMBOLS[idx]
    cache[idx_key] = (time.time(), (idx + 1) % len(SYMBOLS))

    try:
        raw = await candles(sym, interval, 90, market, iq_state, request=request)
        if len(raw) >= 25:
            tech = strategy_engine_for_market(
                raw[:-1] if len(raw) > 1 else raw,
                market,
                {"interval": interval},
            )
            direction = tech["direction"] if tech.get("confirmed") else "NEUTRO"
            item = {
                "symbol": sym + suffix,
                "direction": direction,
                "confidence": round(float(tech.get("confidence", 0) or 0), 1),
                "status": (
                    "TWELVE DATA • OPORTUNIDADE TÉCNICA" if fallback_twelve and direction != "NEUTRO"
                    else "TWELVE DATA • MONITORANDO" if fallback_twelve
                    else "OPORTUNIDADE TÉCNICA" if direction != "NEUTRO"
                    else "MONITORANDO"
                ),
                "feed_source": "TWELVE_DATA" if fallback_twelve else market,
                "feed_fallback": fallback_twelve,
                "requested_market": requested_market,
            }
        else:
            item = {
                "symbol": sym + suffix,
                "direction": "NEUTRO",
                "confidence": 0,
                "status": "POUCOS CANDLES",
            }
    except Exception:
        item = {
            "symbol": sym + suffix,
            "direction": "NEUTRO",
            "confidence": 0,
            "status": "FONTE EM ESPERA",
        }

    # replace same symbol slot
    base_symbol = sym + suffix
    replaced = False
    for i, old in enumerate(out):
        if old.get("symbol") == base_symbol:
            out[i] = item
            replaced = True
            break
    if not replaced:
        out.append(item)

    radar_cache[rkey] = (time.time(), out)
    return out



def _accounting_key(payload: Dict[str, Any]) -> str:
    return "|".join([
        str(payload.get("market") or "OPEN").upper(),
        str(payload.get("symbol") or ""),
        str(payload.get("interval") or ""),
        str(payload.get("direction") or "").upper(),
        str(payload.get("entry_time") or ""),
        str(payload.get("expiry_time") or ""),
    ])


def _accounting_buckets(request: Request, market: str):
    market = (market or "OPEN").upper()
    if market == "IQ_OTC":
        state = _iq_session_state(request, required=False)
        if state is None:
            return None, None
        return (
            state.setdefault("accounting_pending", {}),
            state.setdefault("accounting_results", {}),
        )
    return (
        accounting_pending.setdefault(market, {}),
        accounting_results.setdefault(market, {}),
    )


def _remember_accounting_signal(request: Request, payload: Dict[str, Any]):
    """Registra uma entrada confirmada no servidor, independente do navegador."""
    if not isinstance(payload, dict):
        return
    direction = str(payload.get("direction") or "").upper()
    if direction not in ("CALL", "PUT"):
        return
    if not payload.get("entry_time") or not payload.get("expiry_time"):
        return
    market = str(payload.get("market") or "OPEN").upper()
    pending, done = _accounting_buckets(request, market)
    if pending is None or done is None:
        return
    item = {
        "market": market,
        "symbol": payload.get("symbol"),
        "interval": payload.get("interval"),
        "direction": direction,
        "entry_time": payload.get("entry_time"),
        "expiry_time": payload.get("expiry_time"),
        "source": payload.get("source") or payload.get("strategy") or "SIGNAL",
    }
    key = _accounting_key(item)
    if key and key not in done and key not in pending:
        pending[key] = item


async def _settle_accounting_pending(request: Request, market: str):
    """Fecha sinais expirados pelo candle da entrada original e grava WIN/LOSS uma vez."""
    market = (market or "OPEN").upper()
    pending, done = _accounting_buckets(request, market)
    if pending is None or done is None or not pending:
        return

    iq_state = _iq_session_state(request, required=False) if market == "IQ_OTC" else None
    for key, t in list(pending.items())[:30]:
        try:
            expiry_dt = parse_dt(t["expiry_time"])
            # Pequena folga para não ler o candle ainda em formação/cache antigo.
            if now() < expiry_dt + timedelta(seconds=2):
                continue
            symbol = t["symbol"]
            interval = t["interval"]
            direction = str(t["direction"]).upper()
            entry_dt = parse_dt(t["entry_time"])
            cs = await candles(symbol, interval, 50, market, iq_state, request=request)

            target, best_delta, matched_dt = _nearest_candle_for_time(
                cs, entry_dt, INTERVALS[interval]
            )

            if not target:
                continue

            op = float(target["open"])
            cl = float(target["close"])
            if cl == op:
                # Empate não vira WIN nem LOSS; marca como DRAW para não duplicar.
                result_value = "DRAW"
            else:
                won = ((direction == "CALL" and cl > op) or (direction == "PUT" and cl < op))
                result_value = "WIN" if won else "LOSS"

            done[key] = {
                **t,
                "result": result_value,
                "candle_time": target.get("datetime"),
                "open": op,
                "close": cl,
                "settled_at": iso(now()),
            }
            pending.pop(key, None)
        except Exception:
            # Mantém pendente para tentar novamente no próximo ciclo.
            continue


@app.get("/performance")
async def performance(request: Request, interval="1min", market="OPEN"):
    market=(market or "OPEN").upper()

    if market not in VALID_MARKETS:
        raise HTTPException(400, "Mercado inválido.")

    await _settle_accounting_pending(request, market)
    pending, done = _accounting_buckets(request, market)
    done = done or {}

    # Une resultados da contabilidade automática e do endpoint /result.
    # A chave simplificada impede dupla contagem da mesma entrada.
    merged_direct = {}
    for x in done.values():
        if x.get("result") in ("WIN", "LOSS", "DRAW"):
            k = "|".join([
                market, str(x.get("symbol") or ""), str(x.get("interval") or ""),
                str(x.get("direction") or ""), str(x.get("expiry_time") or "")
            ])
            merged_direct[k] = x

    if market == "IQ_OTC":
        state_direct = _iq_session_state(request, required=False)
        direct_store = state_direct.setdefault("results", {}) if state_direct else {}
    else:
        direct_store = results.setdefault(market, {})

    for x in direct_store.values():
        if x.get("result") in ("WIN", "LOSS", "DRAW"):
            k = "|".join([
                market, str(x.get("symbol") or ""), str(x.get("interval") or ""),
                str(x.get("direction") or ""), str(x.get("expiry_time") or "")
            ])
            merged_direct[k] = x

    wins = sum(1 for x in merged_direct.values() if x.get("result") == "WIN")
    losses = sum(1 for x in merged_direct.values() if x.get("result") == "LOSS")
    draws = sum(1 for x in merged_direct.values() if x.get("result") == "DRAW")
    total = wins + losses

    # Gale fica apenas como informação separada.
    if market == "IQ_OTC":
        state = _iq_session_state(request, required=False)
        gale_store = state.setdefault("results", {}) if state else {}
    else:
        gale_store = results.setdefault(market, {})
    win_g1 = sum(1 for x in gale_store.values() if x.get("result") == "WIN G1")
    win_g2 = sum(1 for x in gale_store.values() if x.get("result") == "WIN G2")
    loss_g2 = sum(1 for x in gale_store.values() if x.get("result") == "LOSS G2")

    return {
        "wins": wins,
        "losses": losses,
        "total": total,
        "accuracy": round(wins / total * 100, 2) if total else 0,
        "win_direct": wins,
        "loss_direct": losses,
        "draws": draws,
        "pending": len(pending or {}),
        "win_g1": win_g1,
        "win_g2": win_g2,
        "loss_g2": loss_g2,
    }



def _cached_open_candles_for_result(symbol: str, interval: str, n: int = 100):
    """Usa o cache já carregado da Twelve Data sem gastar uma nova chamada."""
    item = td_candle_cache.get(f"{symbol}|{interval}")
    if not item:
        return []
    try:
        values = item[1] or []
        return values[-max(20, min(int(n), 150)):]
    except Exception:
        return []


async def _fetch_open_result_window(symbol: str, interval: str, target_dt: datetime):
    """Busca uma janela pequena exatamente ao redor da entrada na Twelve Data.

    É usada somente quando o cache/histórico recente não contém a vela necessária
    para fechar WIN/LOSS. Isso evita deixar uma operação presa em
    "AGUARDANDO CANDLE" por causa de cache curto ou sessão de mercado.
    """
    global td_last_call_at, td_backoff_until, td_backoff_reason
    if not TD_KEY:
        return []
    try:
        seconds = int(INTERVALS.get(interval, 60))
        start_dt = (target_dt - timedelta(seconds=seconds * 2)).astimezone(BR_TZ)
        end_dt = (target_dt + timedelta(seconds=seconds * 3)).astimezone(BR_TZ)
        params = {
            "symbol": symbol,
            "interval": interval,
            "start_date": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "end_date": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "timezone": "America/Sao_Paulo",
            "outputsize": 12,
            "apikey": TD_KEY,
            "format": "JSON",
        }

        # Respeita o mesmo limitador global usado pelo restante da Twelve Data.
        async with td_rate_lock:
            delay = TD_MIN_CALL_INTERVAL - (time.time() - td_last_call_at)
            if delay > 0:
                await asyncio.sleep(delay)
            async with td_sem:
                async with httpx.AsyncClient(timeout=15) as client:
                    response = await client.get(TD_URL, params=params)
                td_last_call_at = time.time()

        if response.status_code == 429:
            td_backoff_until = time.time() + 60.0
            td_backoff_reason = "HTTP 429 em resultado"
            return []
        response.raise_for_status()
        data = response.json()
        if data.get("status") == "error":
            return []

        out = []
        for x in reversed(data.get("values", [])):
            try:
                out.append({
                    "datetime": x["datetime"],
                    "open": float(x["open"]),
                    "high": float(x["high"]),
                    "low": float(x["low"]),
                    "close": float(x["close"]),
                    "volume": float(x.get("volume", 0) or 0),
                })
            except Exception:
                pass
        if out:
            print(
                f"[RESULTADO] janela exata Twelve Data recuperada {symbol} {interval} "
                f"entrada={iso(target_dt)} candles={len(out)}",
                flush=True,
            )
        return out
    except Exception as exc:
        print(
            f"[RESULTADO] falha janela exata Twelve Data {symbol} {interval}: {str(exc)[:180]}",
            flush=True,
        )
        return []


@app.get("/result")
async def result(
    request: Request,
    symbol="EUR/USD",
    interval="1min",
    direction="CALL",
    expiry_time="",
    market="OPEN",
    direct_only: bool = False,
):
    if not expiry_time:
        raise HTTPException(400, "expiry_time é obrigatório.")

    market = (market or "OPEN").upper()
    direction = (direction or "CALL").upper()

    if market not in VALID_MARKETS:
        raise HTTPException(400, "Mercado inválido.")

    if direction not in ("CALL", "PUT"):
        raise HTTPException(400, "Direção inválida.")

    state = _iq_session_state(request, required=True) if market == "IQ_OTC" else None
    store = state.setdefault("results", {}) if market == "IQ_OTC" else results.setdefault(market, {})
    key = f"{market}|{symbol}|{interval}|{direction}|{expiry_time}"

    if key in store and store[key].get("status") == "FINALIZADA":
        return store[key]

    expiry_dt = parse_dt(expiry_time)
    step = timedelta(seconds=INTERVALS[interval])

    # Entrada original começa uma vela antes do expiry_time.
    entry_dt = expiry_dt - step
    g1_entry_dt = expiry_dt
    g1_expiry_dt = expiry_dt + step
    g2_entry_dt = g1_expiry_dt
    g2_expiry_dt = g1_expiry_dt + step

    if now() < expiry_dt:
        return {
            "status": "PENDENTE",
            "stage": "ENTRADA",
            "result": None,
            "next_check": iso(expiry_dt),
        }

    # Para resultado OPEN, tenta primeiro o cache já existente. Isso evita
    # consumir créditos só para fechar WIN/LOSS.
    cs = _cached_open_candles_for_result(symbol, interval, 100) if market == "OPEN" else []

    if not cs:
        try:
            cs = await candles(symbol, interval, 40, market, state, request=request)
        except HTTPException as exc:
            # Não devolve 503 ao painel. O resultado continua pendente e o
            # navegador tenta novamente em ritmo controlado.
            return {
                "status": "AGUARDANDO_FONTE",
                "stage": "ENTRADA",
                "result": None,
                "retry_after": 15,
                "message": str(exc.detail)[:220],
            }
        except Exception as exc:
            return {
                "status": "AGUARDANDO_FONTE",
                "stage": "ENTRADA",
                "result": None,
                "retry_after": 15,
                "message": str(exc)[:220],
            }

    def candle_near(target_dt):
        target, best_delta, matched_dt = _nearest_candle_for_time(
            cs, target_dt, INTERVALS[interval]
        )
        return target

    def candle_result(candle):
        if candle["close"] == candle["open"]:
            return "EMPATE"

        won = (
            (direction == "CALL" and candle["close"] > candle["open"])
            or
            (direction == "PUT" and candle["close"] < candle["open"])
        )
        return "WIN" if won else "LOSS"

    # Entrada inicial.
    base = candle_near(entry_dt)
    if not base and market == "OPEN":
        # O cache pode estar preenchido, mas não conter mais a vela da entrada.
        # Força uma leitura maior uma única vez para conseguir fechar WIN/LOSS.
        try:
            cache_key = f"{symbol}|{interval}"
            cached_item = td_candle_cache.pop(cache_key, None)
            try:
                cs = await candles(symbol, interval, 120, market, state, request=request)
            finally:
                # Se a consulta falhar e havia cache anterior, não o perde.
                if cache_key not in td_candle_cache and cached_item is not None:
                    td_candle_cache[cache_key] = cached_item
            base = candle_near(entry_dt)
        except Exception:
            base = None

        # Se o histórico recente ainda não trouxe o horário da entrada,
        # consulta uma janela exata ao redor da operação. É a última tentativa
        # antes de devolver AGUARDANDO CANDLE.
        if not base:
            exact_cs = await _fetch_open_result_window(symbol, interval, entry_dt)
            if exact_cs:
                cs = exact_cs
                base = candle_near(entry_dt)

    if not base and market == "IQ_OTC" and state is not None:
        # No OTC da IQ, o cache curto pode conter apenas candles muito recentes.
        # Remove somente o cache deste ativo/timeframe e força histórico maior
        # para localizar exatamente a vela da entrada e fechar WIN/LOSS.
        try:
            cache_key = f"{symbol}|{interval}"
            iq_cache = state.setdefault("candle_cache", {})
            old_iq_cache = iq_cache.pop(cache_key, None)
            try:
                cs = await candles(symbol, interval, 150, "IQ_OTC", state, request=request)
            finally:
                if cache_key not in iq_cache and old_iq_cache is not None:
                    iq_cache[cache_key] = old_iq_cache
            base = candle_near(entry_dt)
            if base:
                print(
                    f"[RESULTADO] candle IQ_OTC recuperado {symbol} {interval} entrada={iso(entry_dt)}",
                    flush=True,
                )
        except Exception as exc:
            print(
                f"[RESULTADO] falha ao recuperar histórico IQ_OTC {symbol} {interval}: {str(exc)[:180]}",
                flush=True,
            )
            base = None

    if not base:
        print(
            f"[RESULTADO] aguardando candle {symbol} {interval} {direction} entrada={iso(entry_dt)}",
            flush=True,
        )
        return {
            "status": "AGUARDANDO CANDLE",
            "stage": "ENTRADA",
            "result": None,
            "retry_after": 15,
        }

    base_result = candle_result(base)

    # PLACAR PRINCIPAL: sempre fecha na vela original da entrada.
    # G1/G2 não seguram mais WIN/LOSS nem a assertividade.
    if base_result == "EMPATE":
        final_direct = "DRAW"
    else:
        final_direct = "WIN" if base_result == "WIN" else "LOSS"

    out = {
        "status": "FINALIZADA",
        "stage": "ENTRADA",
        "result": final_direct,
        "candle_time": base["datetime"],
        "entry_time": iso(entry_dt),
        "expiry_time": expiry_time,
        "simulated": True,
        "direct_only": True,
    }
    store[key] = out
    print(
        f"[RESULTADO] {symbol} {interval} {direction} -> {final_direct} "
        f"entrada={iso(entry_dt)} candle={base.get('datetime')}",
        flush=True,
    )

    # Mantém a contabilidade do servidor sincronizada com o /result.
    pending_acc, done_acc = _accounting_buckets(request, market)
    if done_acc is not None:
        acc_item = {
            "market": market,
            "symbol": symbol,
            "interval": interval,
            "direction": direction,
            "entry_time": iso(entry_dt),
            "expiry_time": expiry_time,
        }
        acc_key = _accounting_key(acc_item)
        done_acc[acc_key] = {**acc_item, "result": final_direct, "candle_time": base.get("datetime")}
        if pending_acc is not None:
            pending_acc.pop(acc_key, None)

    return out

    # Fluxo normal do painel continua acompanhando G1/G2.
    if now() < g1_expiry_dt:
        out = {
            "status": "AGUARDANDO G1",
            "stage": "G1",
            "result": None,
            "entry_result": base_result,
            "previous": base_result,
            "next_check": iso(g1_expiry_dt),
            "g1_entry_time": iso(g1_entry_dt),
            "g1_expiry_time": iso(g1_expiry_dt),
            "entry_time": iso(entry_dt),
            "expiry_time": expiry_time,
        }
        # Salva a entrada original imediatamente para o WIN/LOSS principal.
        store[key] = out
        return out

    g1 = candle_near(g1_entry_dt)
    if not g1:
        out = {
            "status": "AGUARDANDO CANDLE G1",
            "stage": "G1",
            "result": None,
            "entry_result": base_result,
            "entry_time": iso(entry_dt),
            "expiry_time": expiry_time,
        }
        store[key] = out
        return out

    g1_result = candle_result(g1)

    if g1_result == "WIN":
        out = {
            "status": "FINALIZADA",
            "stage": "G1",
            "result": "WIN G1",
            "entry_result": base_result,
            "g1_result": "WIN",
            "candle_time": g1["datetime"],
            "entry_time": iso(entry_dt),
            "expiry_time": iso(g1_expiry_dt),
            "simulated": True,
        }
        store[key] = out
        return out

    if now() < g2_expiry_dt:
        out = {
            "status": "AGUARDANDO G2",
            "stage": "G2",
            "result": None,
            "entry_result": base_result,
            "g1_result": g1_result,
            "previous": g1_result,
            "next_check": iso(g2_expiry_dt),
            "g2_entry_time": iso(g2_entry_dt),
            "g2_expiry_time": iso(g2_expiry_dt),
            "entry_time": iso(entry_dt),
            "expiry_time": expiry_time,
        }
        store[key] = out
        return out

    g2 = candle_near(g2_entry_dt)
    if not g2:
        out = {
            "status": "AGUARDANDO CANDLE G2",
            "stage": "G2",
            "result": None,
            "entry_result": base_result,
            "g1_result": g1_result,
            "entry_time": iso(entry_dt),
            "expiry_time": expiry_time,
        }
        store[key] = out
        return out

    g2_result = candle_result(g2)
    final_result = "WIN G2" if g2_result == "WIN" else "LOSS G2"

    out = {
        "status": "FINALIZADA",
        "stage": "G2",
        "result": final_result,
        "entry_result": base_result,
        "g1_result": g1_result,
        "g2_result": g2_result,
        "candle_time": g2["datetime"],
        "entry_time": iso(entry_dt),
        "expiry_time": iso(g2_expiry_dt),
        "simulated": True,
    }

    store[key] = out
    return out


HTML_PAGE = r"""
<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Mega IA Trader</title>
<link rel="manifest" href="/manifest.webmanifest?v=44">
<link rel="icon" type="image/png" sizes="512x512" href="/mega-ia-icon.png?v=44">
<link rel="apple-touch-icon" sizes="192x192" href="/mega-ia-icon-192.png?v=44">
<meta name="theme-color" content="#07182b">
<meta name="application-name" content="Mega IA Trader">
<meta name="apple-mobile-web-app-title" content="Mega IA Trader">
<meta name="apple-mobile-web-app-capable" content="yes">

<style>
body{margin:0;background:radial-gradient(circle at 50% 0,#07182b 0,#030812 42%,#02050b 100%);color:#eef5ff;font-family:Arial,sans-serif}
.wrap{max-width:1150px;margin:auto;padding:18px}
.brand{display:flex;align-items:center;gap:10px;font-size:42px;font-weight:900;letter-spacing:1px;margin:8px 0 2px}
.brand span{color:#14c8ff}
.brand-robot{width:54px;height:54px;object-fit:contain;border-radius:14px;filter:drop-shadow(0 0 8px #14c8ff55)}
.subtitle{font-size:13px;color:#91a9c8;letter-spacing:.7px}
.card{background:linear-gradient(180deg,#0c1a2c,#091422);border:1px solid #164f80;border-radius:20px;padding:16px;box-shadow:0 12px 30px #0008,0 0 18px #009cff12;margin-top:12px}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}
.signal{grid-column:span 2;text-align:center;min-height:270px;position:relative}
.big{font-size:32px;font-weight:800;margin:8px}
.call{color:#45ff9b}
.put{color:#ff5c7a}
.neutral{color:#ffd166}
.label{font-size:11px;color:#8190a8}
.controls{display:flex;gap:10px;flex-wrap:wrap;margin-top:12px}
select,button,input{background:#0d2035;color:#fff;border:1px solid #22689d;border-radius:14px;padding:12px 14px;font-size:15px}
select:focus,button:focus,input:focus{outline:none;box-shadow:0 0 0 2px #00bfff55}
button{cursor:pointer}
input{box-sizing:border-box;width:100%;margin-top:6px}
.account-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.account-actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}
.radar{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}
.radar div{background:#101b2b;padding:10px;border-radius:12px;border:1px solid #173c5e}
.tabs{display:flex;gap:8px;margin-top:12px;margin-bottom:20px;flex-wrap:wrap}
.tabbtn.active{border-color:#168cff;box-shadow:0 0 15px #168cff44}
.tab{display:none}
.tab.active{display:block}
#chartTab{width:100%;display:none;justify-content:center;align-items:flex-start}
#chartTab.active{display:flex;padding-top:7vh;box-sizing:border-box}
#chartTab>.card{width:min(96vw,1100px)!important;max-width:1100px!important;margin:0 auto!important;padding:14px!important;box-sizing:border-box}
.chartbox{position:relative;width:100%!important;height:clamp(460px,68vh,700px)!important;margin:0 auto!important;background:#07101c;border:1px solid #1d3049;border-radius:16px;overflow:hidden}
.chartbox canvas{width:100%!important;height:100%!important;display:block}
.chartmeta{display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:10px}
.chartbadge{padding:7px 10px;border-radius:10px;background:#101b2b;color:#b8c7dd;font-size:12px}
.hero{display:none;position:relative;width:100%;max-width:760px;margin:26px auto 18px;border-radius:20px;overflow:hidden;border:1px solid #0bbcff;box-shadow:0 0 30px #00aaff55;background:#05111f}
.hero img{display:block;width:100%;height:300px;object-fit:contain;object-position:center;background:#05111f}
.entry-arrow{display:none;position:absolute;inset:0;align-items:center;justify-content:center;flex-direction:column;background:#02081488;backdrop-filter:blur(1px);font-weight:900;text-shadow:0 0 18px currentColor}
.entry-arrow .arrow{font-size:110px;line-height:.8}
.entry-arrow .arrow-label{font-size:28px;margin-top:8px}
.entry-arrow.call{display:flex;color:#31ff87}
.entry-arrow.put{display:flex;color:#ff405f}
.analysisbar{text-align:center;font-size:20px;color:#22c9ff;border-color:#0bbcff}
.robot-mode-card{display:flex;align-items:center;gap:10px;width:max-content;max-width:100%;margin:12px 0 4px;padding:8px 10px;border:1px solid #1a74a8;border-radius:16px;background:#071423;box-shadow:0 0 18px #00aaff22}
.robot-mode-card img{width:58px;height:58px;border-radius:13px;object-fit:cover;background:#05111f;border:1px solid #0bbcff}
.robot-mode-copy{min-width:150px}
.robot-mode-title{font-weight:900;font-size:13px;letter-spacing:.4px}
.robot-mode-desc{font-size:11px;color:#9fb2ca;margin-top:3px;max-width:245px}
#robotPowerBtn{padding:9px 12px;border-radius:12px;min-width:105px;font-size:13px}
.app-power-card{display:flex;align-items:center;justify-content:space-between;gap:14px;margin:14px 0 6px;padding:14px 16px;border:1px solid #227db5;border-radius:18px;background:linear-gradient(180deg,#0b1c30,#081523);box-shadow:0 0 22px #00aaff22}
.app-power-copy{min-width:0}
.app-power-title{font-size:14px;font-weight:900;letter-spacing:.5px}
.app-power-desc{font-size:12px;color:#9fb2ca;margin-top:4px;line-height:1.35}
#appPowerBtn{min-width:230px;min-height:58px;padding:16px 22px;border-radius:16px;font-size:17px;font-weight:1000;letter-spacing:.5px;box-shadow:0 8px 20px #0007;transition:transform .12s ease,box-shadow .12s ease,background .18s ease}
#appPowerBtn:active{transform:scale(.97)}
#appPowerBtn.app-on{background:linear-gradient(180deg,#159452,#0b6e3a);border:2px solid #35e889;color:#fff;box-shadow:0 0 18px #1ad87355,0 8px 20px #0007}
#appPowerBtn.app-off{background:linear-gradient(180deg,#a62d36,#741b23);border:2px solid #ff6673;color:#fff;box-shadow:0 0 18px #ff405055,0 8px 20px #0007}

/* MEGA IA 33.51.0 — placar e cronograma com leitura forte */
.score-card{position:relative;text-align:center;overflow:hidden;min-height:118px;display:flex;flex-direction:column;justify-content:center;align-items:center}
.score-card .label{font-size:16px!important;font-weight:1000;letter-spacing:.8px;color:#f4f8ff!important;text-shadow:0 0 10px currentColor}
.score-card .big{font-size:44px!important;line-height:1;font-weight:1000;margin-top:12px;text-shadow:0 0 16px currentColor}
.score-win{border-color:#20e979;box-shadow:inset 0 0 22px #18d66a18,0 0 18px #18d66a30}
.score-win .label,.score-win .big{color:#42ff9b!important}
.score-loss{border-color:#ff405f;box-shadow:inset 0 0 22px #ff405f18,0 0 18px #ff405f30}
.score-loss .label,.score-loss .big{color:#ff5572!important}
.score-accuracy{border-color:#18a8ff;box-shadow:inset 0 0 22px #18a8ff18,0 0 18px #18a8ff30}
.score-accuracy .label,.score-accuracy .big{color:#43c8ff!important}
.score-result{border-color:#b34cff;box-shadow:inset 0 0 22px #a53cff18,0 0 18px #a53cff30}
.score-result .label,.score-result .big{color:#d277ff!important}
.schedule-card{text-align:center;border-color:#198fff;box-shadow:inset 0 0 24px #168cff15,0 0 18px #168cff22}
.schedule-card .label{font-size:17px!important;font-weight:1000;color:#4fd2ff!important;letter-spacing:.8px;text-shadow:0 0 10px #18a8ff}
.schedule-card #entry{font-size:38px!important;color:#ffd43b!important;text-shadow:0 0 15px #ffd43b66}
.schedule-card #countdown{font-size:16px;font-weight:900;color:#f4f8ff;margin-top:6px}
.schedule-card #expiryCountdown{font-size:18px!important;color:#ff5872!important;text-shadow:0 0 10px #ff405f66}
@media(max-width:720px){.score-card{min-height:105px}.score-card .label{font-size:15px!important}.score-card .big{font-size:38px!important}.schedule-card #entry{font-size:34px!important}}
@media(max-width:450px){.score-card{min-height:100px}.score-card .label{font-size:14px!important}.score-card .big{font-size:34px!important}.schedule-card .label{font-size:15px!important}.schedule-card #entry{font-size:31px!important}.schedule-card #expiryCountdown{font-size:16px!important}}

@media(max-width:720px){
  .wrap{padding:10px}
  .grid{grid-template-columns:1fr 1fr}
  .signal{grid-column:span 2}
  .radar{grid-template-columns:1fr 1fr}
  .hero img{height:250px}
  .brand{font-size:36px}
  .brand-robot{width:48px;height:48px}
  .app-power-card{align-items:stretch;flex-direction:column}
  #appPowerBtn{width:100%;min-width:0;min-height:62px;font-size:18px}
  #chartTab.active{padding-top:8vh}
  #chartTab>.card{width:calc(100vw - 20px)!important;max-width:none!important;padding:10px!important}
  .chartbox{height:58vh!important;min-height:440px!important;max-height:620px!important}
}

@media(max-width:600px){
  .account-grid{grid-template-columns:1fr}
}

@media(max-width:450px){
  .wrap{padding:8px}
  .grid{grid-template-columns:1fr}
  .signal{grid-column:span 1}
  .radar{grid-template-columns:1fr}
  .hero img{height:230px}
  #chartTab.active{padding-top:9vh}
  #chartTab>.card{width:calc(100vw - 12px)!important;padding:7px!important}
  .chartbox{height:56vh!important;min-height:420px!important}
}
</style>
</head>

<body>
<div class="wrap">
  <div class="brand"><img class="brand-robot" src="__MEGA_IMAGE__" alt="Robô MEGA IA"> MEGA <span>IA</span></div>
  <div class="subtitle">ANÁLISE EM TEMPO REAL • HORÁRIO DE BRASÍLIA</div>
  <div id="clock" style="font-size:22px;margin-top:4px"></div>

  <div class="app-power-card" id="appPowerCard">
    <div class="app-power-copy">
      <div class="app-power-title">⚡ CONTROLE PRINCIPAL</div>
      <div class="app-power-desc" id="appPowerDesc">App ligado • sinais e análises ativos.</div>
    </div>
    <button id="appPowerBtn" class="app-on" type="button">🟢 DESLIGAR APP</button>
  </div>

  <div class="controls">
    <select id="broker">
      <option value="IQ_OPTION">🏦 IQ Option</option>
      <option value="OLYMPTRADE">🏦 Olymptrade</option>
    </select>

    <select id="marketMode">
      <option value="OPEN">🌐 Mercado Aberto</option>
      <option value="OTC">🟣 OTC da corretora</option>
    </select>
    <input id="market" type="hidden" value="OPEN">

    <select id="symbol"></select>

    <select id="interval">
      <option>1min</option>
      <option>5min</option>
      <option>15min</option>
      <option>30min</option>
    </select>

    <button id="voiceBtn" type="button" onclick="voice()" style="font-weight:900">🔇 VOZ OFFLINE</button>
    <div id="otcNote" class="label" style="display:none"></div>
  </div>

  <div class="robot-mode-card" id="robotModeCard">
    <img src="__MEGA_IMAGE__" alt="Robô MEGA IA">
    <div class="robot-mode-copy">
      <div class="robot-mode-title">🤖 MODO DO ROBÔ</div>
      <div class="robot-mode-desc" id="robotModeDesc">ONLINE: somente Inteligência Artificial.</div>
    </div>
    <button id="robotPowerBtn" type="button" style="font-weight:900">🟢 ONLINE</button>
  </div>

  <div class="tabs">
    <button class="tabbtn active" id="tabMain">📊 Painel</button>
    <button class="tabbtn" id="tabChart">📈 Gráfico</button>
    <button class="tabbtn" id="tabResults">🎯 Resultados</button>
    <button class="tabbtn" id="tabAccount">🏦 Corretora</button>
  </div>

  <div id="mainTab" class="tab active">
    <div id="heroBox" class="hero">
      <img id="aiImage" src="__MEGA_IMAGE__" alt="MEGA IA analisando o mercado">
      <div id="entryArrow" class="entry-arrow">
        <div class="arrow" id="entryArrowIcon">⬆</div>
        <div class="arrow-label" id="entryArrowLabel">CALL • COMPRAR</div>
      </div>
    </div>

    <div id="analysisText" class="card analysisbar" style="display:none">
      🧠 ESTOU ANALISANDO O MERCADO, AGUARDE...
    </div>

    <div class="grid">
      <div class="card signal">
        <div class="label">SINAL ATUAL</div>
        <div id="direction" class="big neutral">AGUARDANDO</div>
        <div id="confidence">Confiança: --</div>
      </div>

      <div class="card schedule-card">
        <div class="label">⏱ CRONOGRAMA • ENTRADA</div>
        <div id="entry" class="big">--:--:--</div>
        <div id="countdown">--</div>
        <div id="expiryCountdown" style="margin-top:8px;font-weight:800">⏱ EXPIRAÇÃO: --:--</div>
      </div>

      <div class="card">
        <div class="label">STATUS IA</div>
        <div id="status" class="big" style="font-size:18px">MONITORANDO</div>
        <div id="risk">Risco: --</div>
      </div>
    </div>

    <div class="grid">
      <div class="card score-card score-win">
        <div class="label">🏆 WIN</div>
        <div id="wins" class="big call">0</div>
      </div>
      <div class="card score-card score-loss">
        <div class="label">✖ LOSS</div>
        <div id="losses" class="big put">0</div>
      </div>
      <div class="card score-card score-accuracy">
        <div class="label">🎯 ASSERTIVIDADE</div>
        <div id="accuracy" class="big">0%</div>
      </div>
      <div class="card score-card score-result">
        <div class="label">★ RESULTADO</div>
        <div id="result" class="big">--</div>
      </div>
    </div>
  </div>

  <div id="chartTab" class="tab">
    <div class="card">
      <div class="chartmeta">
        <b>📈 Gráfico em tempo real</b>
        <span class="chartbadge" id="chartInfo">--</span>
      </div>
      <div class="chartbox"><canvas id="priceChart"></canvas></div>
      <div class="label" style="margin-top:8px">
        O gráfico acompanha o mercado, par e período selecionados e atualiza automaticamente com a vela atual.
      </div>
    </div>
  </div>


  <div id="resultsTab" class="tab">
    <div class="card">
      <h2 style="margin-top:0">🎯 Resultados até Gale 2</h2>
      <div class="label">ACOMPANHAMENTO DA ENTRADA • G1 • G2</div>

      <div class="grid" style="margin-top:12px">
        <div class="card">
          <div class="label">WIN DIRETO</div>
          <div id="winDirect" class="big call">0</div>
        </div>

        <div class="card">
          <div class="label">WIN G1</div>
          <div id="winG1" class="big call">0</div>
        </div>

        <div class="card">
          <div class="label">WIN G2</div>
          <div id="winG2" class="big call">0</div>
        </div>

        <div class="card">
          <div class="label">LOSS DIRETO</div>
          <div id="lossDirect" class="big put">0</div>
        </div>

        <div class="card">
          <div class="label">LOSS G2</div>
          <div id="lossG2" class="big put">0</div>
        </div>
      </div>

      <div class="card" style="margin-top:12px">
        <div class="label">ÚLTIMO RESULTADO</div>
        <div id="galeLastResult" class="big">--</div>
        <div id="galeStageStatus" style="margin-top:8px">Aguardando operação.</div>
      </div>

      <div class="label" style="margin-top:10px;line-height:1.5">
        WIN DIRETO e LOSS DIRETO são registrados quando a operação termina sem Gale.
        Quando o fluxo normal acompanha G1/G2, WIN G1, WIN G2 e LOSS G2 também entram no placar.
        Cada operação é contabilizada uma única vez e o histórico fica salvo neste aparelho.
      </div>
    </div>
  </div>

  <div id="accountTab" class="tab">
    <div class="card">
      <h2 style="margin-top:0">🏦 Corretora</h2>
      <div class="label">ESCOLHA ONDE VOCÊ VAI EXECUTAR A ENTRADA</div>

      <select id="brokerAccount" style="width:100%;margin-top:10px">
        <option value="IQ_OPTION">IQ Option</option>
        <option value="OLYMPTRADE">Olymptrade</option>
      </select>

      <div id="iqAccountStatus" class="card" style="margin-top:12px">
        ⚪ Selecione a corretora e faça login para usar o OTC.
      </div>

      <div style="margin-top:12px">
        <div class="label">E-MAIL DA CORRETORA</div>
        <input id="iqEmail" type="email" autocomplete="username"
               placeholder="seuemail@exemplo.com"
               style="width:100%;box-sizing:border-box;margin-top:6px">
      </div>

      <div style="margin-top:10px">
        <div class="label">SENHA</div>
        <input id="iqPassword" type="password" autocomplete="current-password"
               placeholder="Sua senha"
               style="width:100%;box-sizing:border-box;margin-top:6px">
      </div>

      <button id="iqConnectBtn" type="button" style="width:100%;margin-top:12px">🔐 CONECTAR</button>
      <button id="iqLogoutBtn" style="width:100%;margin-top:8px;display:none">🚪 DESCONECTAR</button>

      <div class="label" style="margin-top:10px;line-height:1.5">
        O e-mail e a senha são usados apenas para abrir a sessão da corretora.
        A senha não é salva no navegador.
      </div>
    </div>
  </div>

  <div class="card">
    <b>Radar de oportunidades</b>
    
    <div class="card" style="margin-top:12px">
      <div style="display:flex;align-items:center;justify-content:space-between;gap:10px">
        <div>
          <div class="label">ANÁLISE 1 MIN • CONFIRMAÇÃO NO ÚLTIMO MINUTO</div>
          <small style="opacity:.75">Vela em formação — ainda não é entrada confirmada</small>
        </div>
        <select id="preSignalLimit" style="max-width:86px">
          <option value="1">1 ativo</option>
          <option value="2">2 ativos</option>
          <option value="3">3 ativos</option>
          <option value="4" selected>4 ativos</option>
        </select>
      </div>
      <div id="preSignalStatus" style="margin-top:10px;font-size:12px;opacity:.8">
        Monitorando...
      </div>
      <div id="preSignals" class="radar" style="margin-top:10px"></div>
    </div>

<div id="radar" class="radar"></div>
  </div>

  <div id="licenseCard" class="card" style="display:none">
    <div class="label">RENOVAÇÃO</div>
    <div id="license"></div>
  </div>
</div>

<script>
// MEGA IA build 33.38.0 — força o PWA antigo a abrir a versão atual.
(function(){
  try{
    const u=new URL(window.location.href);
    if(u.searchParams.get('pwa')!=='v38'){
      u.searchParams.set('pwa','v38');
      window.history.replaceState({},'',u.pathname+u.search+u.hash);
    }
  }catch(_){}
})();
const market=document.getElementById('market');
const marketMode=document.getElementById('marketMode');
const broker=document.getElementById('broker');
const brokerAccount=document.getElementById('brokerAccount');

function brokerName(){
  const v=broker ? broker.value : 'IQ_OPTION';
  if(v==='OLYMPTRADE') return 'Olymptrade';
  return 'IQ Option';
}

function syncMarketFromBroker(){
  if(!market || !marketMode) return;
  if(marketMode.value==='OPEN'){
    market.value='OPEN';
  }else{
    market.value=(broker && broker.value==='OLYMPTRADE') ? 'OLYMP_OTC' : 'IQ_OTC';
  }
  try{localStorage.setItem('mega_market_mode',marketMode.value)}catch(_){}
}

function syncBroker(value){
  const v=['IQ_OPTION','OLYMPTRADE'].includes(value) ? value : 'IQ_OPTION';
  if(broker) broker.value=v;
  if(brokerAccount) brokerAccount.value=v;
  try{localStorage.setItem('mega_broker',v)}catch(_){}
  syncMarketFromBroker();

  if(typeof iqAccountStatus!=='undefined' && iqAccountStatus){
    const name=v==='OLYMPTRADE'?'Olymptrade':'IQ Option';
    const mode=marketMode && marketMode.value==='OTC' ? 'OTC' : 'MERCADO ABERTO';
    const connected=!!brokerConnected[v];
    iqAccountStatus.textContent=(connected?'🟢 ':'⚪ ')+name+' • '+mode+
      (connected?' • CONECTADA':' • AGUARDANDO LOGIN');
    if(iqLogoutBtn) iqLogoutBtn.style.display=connected?'block':'none';
    if(iqConnectBtn) iqConnectBtn.style.display=connected?'none':'block';
    if(iqPassword) iqPassword.value='';
  }
}

try{
  const savedLimit=Number(localStorage.getItem('mega_pre_signal_limit')||4);
  if(preSignalLimit){
    preSignalLimit.value=String(Math.max(1,Math.min(4,savedLimit)));
  }

  const savedBroker=localStorage.getItem('mega_broker')||'IQ_OPTION';
  const savedMode=localStorage.getItem('mega_market_mode')||'OPEN';
  if(marketMode) marketMode.value=(savedMode==='OTC'?'OTC':'OPEN');
  setTimeout(()=>syncBroker(savedBroker),0);
}catch(_){}
const interval=document.getElementById('interval');
const appPowerBtn=document.getElementById('appPowerBtn');
const appPowerDesc=document.getElementById('appPowerDesc');
let appEnabled=true;
try{
  appEnabled=localStorage.getItem('mega_app_power')!=='OFF';
}catch(_){}
const robotPowerBtn=document.getElementById('robotPowerBtn');
const robotModeDesc=document.getElementById('robotModeDesc');
const voiceBtn=document.getElementById('voiceBtn');
let robotEnabled=true; // true = ONLINE / IA PURA; false = OFFLINE / MODO NORMAL
try{
  robotEnabled=localStorage.getItem('mega_robot_power')!=='OFFLINE';
}catch(_){}
const otcNote=document.getElementById('otcNote');
const preSignalLimit=document.getElementById('preSignalLimit');
const preSignals=document.getElementById('preSignals');
const preSignalStatus=document.getElementById('preSignalStatus');
let preSignalBusy=false;
let iqLoginInProgress=false; // pausa temporariamente as consultas durante o login da IQ Option
const heroBox=document.getElementById('heroBox');
const entryArrow=document.getElementById('entryArrow');
const entryArrowIcon=document.getElementById('entryArrowIcon');
const entryArrowLabel=document.getElementById('entryArrowLabel');
const analysisText=document.getElementById('analysisText');
const mainTab=document.getElementById('mainTab');
const chartTab=document.getElementById('chartTab');
const accountTab=document.getElementById('accountTab');
const resultsTab=document.getElementById('resultsTab');
const tabResults=document.getElementById('tabResults');
const winDirect=document.getElementById('winDirect');
const winG1=document.getElementById('winG1');
const winG2=document.getElementById('winG2');
const lossDirect=document.getElementById('lossDirect');
const lossG2=document.getElementById('lossG2');
const galeLastResult=document.getElementById('galeLastResult');
const galeStageStatus=document.getElementById('galeStageStatus');
const tabMain=document.getElementById('tabMain');
const tabChart=document.getElementById('tabChart');
const tabAccount=document.getElementById('tabAccount');
const iqEmail=document.getElementById('iqEmail');
const iqPassword=document.getElementById('iqPassword');
const iqConnectBtn=document.getElementById('iqConnectBtn');
const iqLogoutBtn=document.getElementById('iqLogoutBtn');
const iqAccountStatus=document.getElementById('iqAccountStatus');
let brokerConnected={IQ_OPTION:false,OLYMPTRADE:false};
const chartInfo=document.getElementById('chartInfo');
const direction=document.getElementById('direction');
const confidence=document.getElementById('confidence');
const entry=document.getElementById('entry');
const countdown=document.getElementById('countdown');
const statusBox=document.getElementById('status');
const risk=document.getElementById('risk');
const wins=document.getElementById('wins');
const losses=document.getElementById('losses');
const accuracy=document.getElementById('accuracy');
const result=document.getElementById('result');
const radar=document.getElementById('radar');
const licenseCard=document.getElementById('licenseCard');
const licenseBox=document.getElementById('license');
const clock=document.getElementById('clock');
const expiryCountdown=document.getElementById('expiryCountdown');
const chartCanvas=document.getElementById('priceChart');
const chartCtx=chartCanvas.getContext('2d');

const syms=[
  'EUR/USD','GBP/USD','USD/JPY','AUD/USD','USD/CAD','USD/CHF',
  'NZD/USD','EUR/JPY','GBP/JPY','EUR/GBP','BTC/USD','ETH/USD','LTC/USD'
];

const S=document.getElementById('symbol');

let cur=null;
let voiceEnabled=false;
try{
  voiceEnabled=localStorage.getItem('mega_voice_power')==='ONLINE';
}catch(_){
  voiceEnabled=false;
}
let lastSignalVoice='';
let lastAnalysis=0;
let fifteen=false;
let five=false;
let entered=false;
let reskey='';
let sigBusy=false;
let chartBusy=false;
let radBusy=false;
let perfBusy=false;
let robotTimer=null;
let arrowTimer=null;
let megaVoices=[];
let chartData=[];
let chartPreSignal=null;
let resultBusy=false;
let pendingTrade=null;
let pendingTradeQueue=[];
let lastCountdownSignalKey='';
let lastChartSignalVoice='';
const RESULT_RETRY_MS=15000;
const RESULT_MAX_PENDING_AGE_MS=30*60*1000;

const RESULT_STATS_KEY='mega_result_stats_v33310';
const PENDING_QUEUE_KEY='mega_pending_trade_queue_v33450';
const RESULT_MARKETS=['OPEN','IQ_OTC','OLYMP_OTC'];

function emptyResultBucket(){
  return {
    win_direct:0,
    win_g1:0,
    win_g2:0,
    loss_direct:0,
    loss_g2:0,
    processed_keys:[],
    entry_keys:[],
    gale_keys:[]
  };
}

let persistentResults={
  OPEN:emptyResultBucket(),
  IQ_OTC:emptyResultBucket(),
  OLYMP_OTC:emptyResultBucket()
};

function normalizeResultBucket(x){
  const b=emptyResultBucket();
  if(!x || typeof x!=='object') return b;
  b.win_direct=Math.max(0,Number(x.win_direct||0));
  b.win_g1=Math.max(0,Number(x.win_g1||0));
  b.win_g2=Math.max(0,Number(x.win_g2||0));
  b.loss_direct=Math.max(0,Number(x.loss_direct||0));
  b.loss_g2=Math.max(0,Number(x.loss_g2||0));
  b.processed_keys=Array.isArray(x.processed_keys)?x.processed_keys.slice(-1500):[];
  b.entry_keys=Array.isArray(x.entry_keys)?x.entry_keys.slice(-1500):[];
  b.gale_keys=Array.isArray(x.gale_keys)?x.gale_keys.slice(-1500):[];
  return b;
}

function resultMarket(m){
  m=String(m||'OPEN').toUpperCase();
  return RESULT_MARKETS.includes(m)?m:'OPEN';
}

function activeResultMarket(){
  // Quando IQ OTC está selecionado mas a corretora está desconectada,
  // os sinais são calculados pelo fallback real da Twelve Data (OPEN).
  // O placar precisa seguir a FONTE REAL do sinal; caso contrário /result
  // fecha em OPEN enquanto /performance consulta IQ_OTC e WIN/LOSS parece zerado.
  const selected=resultMarket(market&&market.value);
  if(selected==='IQ_OTC' && !(brokerConnected&&brokerConnected.IQ_OPTION)){
    return 'OPEN';
  }
  return selected;
}

function signalResultMarket(sig){
  if(sig && (sig.feed_fallback===true || String(sig.feed_source||'').toUpperCase()==='TWELVE_DATA')){
    return 'OPEN';
  }
  // Prioriza o mercado originalmente solicitado. Em IQ_OTC conectado,
  // o resultado deve ser fechado com candles da própria IQ Option.
  const requested=resultMarket(sig&&sig.requested_market ? sig.requested_market : (market&&market.value));
  if(requested==='IQ_OTC' && brokerConnected&&brokerConnected.IQ_OPTION){
    return 'IQ_OTC';
  }
  const m=resultMarket(sig&&sig.market ? sig.market : requested);
  if(m==='IQ_OTC' && !(brokerConnected&&brokerConnected.IQ_OPTION)){
    return 'OPEN';
  }
  return m;
}

function resultTradeKey(t){
  if(!t) return '';
  return [
    resultMarket(t.market),
    t.symbol||'',
    t.interval||'',
    t.direction||'',
    t.entry_time||'',
    t.expiry_time||''
  ].join('|');
}

function loadPersistentResults(){
  try{
    const raw=localStorage.getItem(RESULT_STATS_KEY);
    if(!raw) return;
    const data=JSON.parse(raw)||{};
    RESULT_MARKETS.forEach(m=>{
      persistentResults[m]=normalizeResultBucket(data[m]);
    });
  }catch(_){ }
}

function savePersistentResults(){
  try{
    localStorage.setItem(RESULT_STATS_KEY,JSON.stringify(persistentResults));
  }catch(_){ }
}

function isResultAlreadyCounted(t){
  const key=resultTradeKey(t);
  if(!key) return false;
  const b=persistentResults[resultMarket(t.market)]||emptyResultBucket();
  return b.processed_keys.includes(key);
}

function registerPersistentResult(t,x){
  if(!t || !x) return false;
  const m=resultMarket(t.market);
  const b=persistentResults[m]||emptyResultBucket();
  persistentResults[m]=b;
  const key=resultTradeKey(t);
  if(!key) return false;

  let changed=false;
  const entryResult=String(x.entry_result||((x.result==='WIN'||x.result==='LOSS')?x.result:'')).toUpperCase();
  const entryKey=key+'|ENTRY';

  // WIN/LOSS principal: contabiliza assim que a PRIMEIRA vela fecha.
  if((entryResult==='WIN'||entryResult==='LOSS') && !b.entry_keys.includes(entryKey)){
    b.entry_keys.push(entryKey);
    if(b.entry_keys.length>1500) b.entry_keys=b.entry_keys.slice(-1500);
    if(entryResult==='WIN') b.win_direct++;
    else b.loss_direct++;
    changed=true;
  }

  // Gale é apenas estatística separada e nunca altera o placar principal.
  const r=String(x.result||'').toUpperCase();
  const galeKey=key+'|FINAL';
  if(['WIN G1','WIN G2','LOSS G2'].includes(r) && !b.gale_keys.includes(galeKey)){
    b.gale_keys.push(galeKey);
    if(b.gale_keys.length>1500) b.gale_keys=b.gale_keys.slice(-1500);
    if(r==='WIN G1') b.win_g1++;
    else if(r==='WIN G2') b.win_g2++;
    else b.loss_g2++;
    changed=true;
  }

  if(r && ['WIN','LOSS','WIN G1','WIN G2','LOSS G2'].includes(r) && !b.processed_keys.includes(key)){
    b.processed_keys.push(key);
    if(b.processed_keys.length>1500) b.processed_keys=b.processed_keys.slice(-1500);
    changed=true;
  }

  if(changed){
    savePersistentResults();
    paintPersistentResults();
  }
  return changed;
}

function mergeServerPerformance(p,m){
  if(!p) return;
  m=resultMarket(m);
  const b=persistentResults[m]||emptyResultBucket();
  persistentResults[m]=b;

  // O servidor funciona como segunda fonte. Uma resposta zerada/atrasada
  // NUNCA pode apagar um WIN/LOSS que o navegador acabou de confirmar.
  const serverWins=Math.max(0,Number(p.win_direct||p.wins||0));
  const serverLosses=Math.max(0,Number(p.loss_direct||p.losses||0));
  b.win_direct=Math.max(Number(b.win_direct||0),serverWins);
  b.loss_direct=Math.max(Number(b.loss_direct||0),serverLosses);
  // Gales continuam apenas como estatística separada.
  b.win_g1=Math.max(b.win_g1,Number(p.win_g1||0));
  b.win_g2=Math.max(b.win_g2,Number(p.win_g2||0));
  b.loss_g2=Math.max(b.loss_g2,Number(p.loss_g2||0));
  savePersistentResults();
}

function paintPersistentResults(){
  const m=activeResultMarket();
  const b=persistentResults[m]||emptyResultBucket();
  const totalWins=b.win_direct;
  const totalLosses=b.loss_direct;
  const total=totalWins+totalLosses;
  const acc=total?((totalWins/total)*100):0;

  if(wins) wins.textContent=String(totalWins);
  if(losses) losses.textContent=String(totalLosses);
  if(accuracy) accuracy.textContent=acc.toFixed(2)+'%';
  if(winDirect) winDirect.textContent=String(b.win_direct);
  if(winG1) winG1.textContent=String(b.win_g1);
  if(winG2) winG2.textContent=String(b.win_g2);
  if(lossDirect) lossDirect.textContent=String(b.loss_direct);
  if(lossG2) lossG2.textContent=String(b.loss_g2);
}

loadPersistentResults();

try{
  // Remove filas antigas que ficaram presas em versões anteriores.
  [
    'mega_pending_trade_v33310',
    'mega_pending_trade_queue_v33310',
    'mega_pending_trade_v33400',
    'mega_pending_trade_queue_v33400'
    ,'mega_pending_trade_v33410'
    ,'mega_pending_trade_queue_v33410'
  ].forEach(k=>{ try{ localStorage.removeItem(k); }catch(_){} });
}catch(_){}

try{
  const savedPending=localStorage.getItem('mega_pending_trade_v33450');
  if(savedPending){
    pendingTrade=normalizePendingTradeMarket(JSON.parse(savedPending));
  }
  const savedQueue=localStorage.getItem(PENDING_QUEUE_KEY);
  if(savedQueue){
    const q=JSON.parse(savedQueue);
    pendingTradeQueue=Array.isArray(q)?q.slice(0,100).map(normalizePendingTradeMarket):[];
  }
}catch(e){
  pendingTrade=null;
  pendingTradeQueue=[];
}

function savePendingTrade(){
  try{
    if(pendingTrade){
      localStorage.setItem('mega_pending_trade_v33450',JSON.stringify(pendingTrade));
    }else{
      localStorage.removeItem('mega_pending_trade_v33450');
    }
    localStorage.setItem(PENDING_QUEUE_KEY,JSON.stringify(pendingTradeQueue.slice(0,100)));
  }catch(e){}
}

function normalizePendingTradeMarket(t){
  if(!t || typeof t!=='object') return t;
  // Fallback real da Twelve Data sempre fecha em OPEN.
  if(t.feed_fallback===true || String(t.feed_source||'').toUpperCase()==='TWELVE_DATA'){
    t.market='OPEN';
    return t;
  }
  // Se a operação nasceu no IQ_OTC e a sessão continua conectada,
  // nunca deixa uma pendência antiga/sem metadata cair em OPEN.
  const requested=resultMarket(t.requested_market || t.market || (market&&market.value));
  if(requested==='IQ_OTC' && brokerConnected&&brokerConnected.IQ_OPTION){
    t.market='IQ_OTC';
  }
  return t;
}

function enqueuePendingTrade(t){
  if(!t || !t.expiry_time || !t.entry_time) return;
  if(t.direction!=='CALL' && t.direction!=='PUT') return;
  if(isResultAlreadyCounted(t)) return;

  const key=resultTradeKey(t);
  if(pendingTrade && resultTradeKey(pendingTrade)===key) return;
  if(pendingTradeQueue.some(x=>resultTradeKey(x)===key)) return;

  if(!pendingTrade){
    pendingTrade=t;
  }else{
    pendingTradeQueue.push(t);
    pendingTradeQueue.sort((a,b)=>new Date(a.expiry_time).getTime()-new Date(b.expiry_time).getTime());
    if(pendingTradeQueue.length>100) pendingTradeQueue=pendingTradeQueue.slice(0,100);
  }
  savePendingTrade();
}

function promoteNextPendingTrade(){
  pendingTrade=null;
  while(pendingTradeQueue.length){
    const next=pendingTradeQueue.shift();
    if(next && !isResultAlreadyCounted(next)){
      pendingTrade=next;
      break;
    }
  }
  savePendingTrade();
}

function rememberPendingTrade(sig){
  if(!sig) return;
  if(sig.direction!=='CALL' && sig.direction!=='PUT') return;
  if(!sig.expiry_time || !sig.entry_time) return;

  enqueuePendingTrade({
    source:sig.source||'SIGNAL',
    // Placar principal sempre fecha na vela original da entrada.
    direct_only:true,
    market:signalResultMarket(sig),
    requested_market:sig.requested_market || (market&&market.value) || 'OPEN',
    feed_source:sig.feed_source||'',
    feed_fallback:!!sig.feed_fallback,
    symbol:sig.symbol,
    interval:sig.interval,
    direction:sig.direction,
    entry_time:sig.entry_time,
    expiry_time:sig.expiry_time
  });
}

function intervalSecondsValue(v){
  const map={
    '1min':60,
    '5min':300,
    '15min':900,
    '30min':1800
  };
  return map[v]||60;
}

function rememberChartSignal(pre){
  if(!pre || !pre.active) return;
  if(pre.direction!=='CALL' && pre.direction!=='PUT') return;
  if(!pre.entry_time) return;

  const chartTradeMarket=signalResultMarket(pre);

  const signalKey=[
    chartTradeMarket,
    S.value,
    interval.value,
    pre.direction,
    pre.entry_time
  ].join('|');

  // A voz fala uma única vez no instante em que a bolinha nasce.
  if(signalKey!==lastChartSignalVoice){
    lastChartSignalVoice=signalKey;

    if(voiceEnabled){
      const lado=pre.direction==='CALL'?'CALL':'PUT';
      speak('Entrada '+lado+'. Sinal confirmado após três leituras consecutivas.');
    }
  }

  const entryMs=new Date(pre.entry_time).getTime();
  if(!entryMs) return;

  const expiryMs=entryMs+(intervalSecondsValue(interval.value)*1000);
  const expiryIso=new Date(expiryMs).toISOString();

  enqueuePendingTrade({
    source:'CHART_20S',
    direct_only:true,
    market:chartTradeMarket,
    feed_source:pre.feed_source||'',
    feed_fallback:!!pre.feed_fallback,
    symbol:S.value,
    interval:interval.value,
    direction:pre.direction,
    entry_time:pre.entry_time,
    expiry_time:expiryIso
  });
}

function fillSymbols(){
  const previous=S.value;
  S.innerHTML='';

  const suffix=(marketMode && marketMode.value==='OTC')
    ? (broker && broker.value==='OLYMPTRADE' ? ' • OLYMP OTC' : ' • IQ OTC')
    : '';

  syms.forEach(x=>{
    S.add(new Option(x+suffix,x));
  });

  if(previous && [...S.options].some(o=>o.value===previous)){
    S.value=previous;
  }
}

function showRobot(){
  heroBox.style.display='block';
  clearTimeout(robotTimer);
  robotTimer=setTimeout(()=>{
    if(!entryArrow.classList.contains('call') && !entryArrow.classList.contains('put')){
      heroBox.style.display='none';
      analysisText.style.display='none';
    }
  },10000);
}

function showEntryArrow(dir){
  showRobot();
  entryArrow.className='entry-arrow '+(dir==='CALL'?'call':'put');
  entryArrowIcon.textContent=dir==='CALL'?'⬆':'⬇';
  entryArrowLabel.textContent=dir==='CALL'?'CALL • COMPRAR':'PUT • VENDER';

  clearTimeout(arrowTimer);
  arrowTimer=setTimeout(()=>{
    entryArrow.className='entry-arrow';
    heroBox.style.display='none';
  },6000);
}

function loadMegaVoices(){
  if(window.speechSynthesis){
    megaVoices=speechSynthesis.getVoices()||[];
  }
}

if(window.speechSynthesis){
  loadMegaVoices();
  if(speechSynthesis.addEventListener){
    speechSynthesis.addEventListener('voiceschanged',loadMegaVoices);
  }
}

function pickMalePtBRVoice(){
  const all=(megaVoices.length?megaVoices:speechSynthesis.getVoices());
  const br=all.filter(v=>String(v.lang||'').replace('_','-').toLowerCase()==='pt-br');
  const pt=br.length?br:all.filter(v=>String(v.lang||'').toLowerCase().startsWith('pt'));
  const natural=/natural|neural|premium|enhanced|wavenet|google.*portugu|microsoft.*portugu/i;
  const male=/antonio|antônio|daniel|ricardo|felipe|paulo|carlos|thiago|bruno|marcelo|male|masculino|homem/i;
  const female=/maria|luciana|fernanda|camila|female|feminina|mulher/i;

  return pt.find(v=>natural.test(v.name||'')&&male.test(v.name||'')) ||
         pt.find(v=>natural.test(v.name||'')&&!female.test(v.name||'')) ||
         pt.find(v=>male.test(v.name||'')) ||
         pt.find(v=>!female.test(v.name||'')) ||
         pt[0] || null;
}

function speak(t){
  if(!voiceEnabled || !window.speechSynthesis) return;

  // Sempre que a Mega IA falar, mostra a imagem do robô no painel.
  showRobot();

  speechSynthesis.cancel();

  const u=new SpeechSynthesisUtterance(t);
  u.lang='pt-BR';
  u.rate=.94;
  u.pitch=.90;
  u.volume=1;

  const mv=pickMalePtBRVoice();
  if(mv) u.voice=mv;

  u.onstart=()=>{
    showRobot();
  };

  u.onend=()=>{
    clearTimeout(robotTimer);
    robotTimer=setTimeout(()=>{
      if(!entryArrow.classList.contains('call') && !entryArrow.classList.contains('put')){
        heroBox.style.display='none';
        analysisText.style.display='none';
      }
    },10000);
  };

  u.onerror=()=>{
    clearTimeout(robotTimer);
    robotTimer=setTimeout(()=>{
      if(!entryArrow.classList.contains('call') && !entryArrow.classList.contains('put')){
        heroBox.style.display='none';
      }
    },10000);
  };

  speechSynthesis.speak(u);
}

function applyVoiceState(){
  if(!voiceBtn) return;

  if(voiceEnabled){
    voiceBtn.textContent='🔊 VOZ ONLINE';
    voiceBtn.style.background='#0b7a3d';
    voiceBtn.style.color='#fff';
    voiceBtn.style.borderColor='#16c56b';
  }else{
    voiceBtn.textContent='🔇 VOZ OFFLINE';
    voiceBtn.style.background='#7d1d1d';
    voiceBtn.style.color='#fff';
    voiceBtn.style.borderColor='#ff5252';

    if(window.speechSynthesis){
      speechSynthesis.cancel();
    }
  }
}

function voice(){
  const wasEnabled=voiceEnabled;
  voiceEnabled=!voiceEnabled;

  try{
    localStorage.setItem(
      'mega_voice_power',
      voiceEnabled ? 'ONLINE' : 'OFFLINE'
    );
  }catch(_){}

  applyVoiceState();

  if(voiceEnabled){
    // Fala somente quando o usuário liga manualmente a voz.
    speak('Voz da Mega IA online.');
    setTimeout(()=>sig(true),650);
  }else if(wasEnabled && window.speechSynthesis){
    speechSynthesis.cancel();
  }
}

function ft(x){
  return x ? new Date(x).toLocaleTimeString('pt-BR',{hour12:false}) : '--:--:--';
}

function iqSessionToken(){
  return '';
}

function authHeaders(extra={}){
  const h={...extra};
  try{
    const iqSession=localStorage.getItem('mega_iq_session')||'';
    if(iqSession){
      h['X-IQ-Session']=iqSession;
    }
  }catch(_){}
  return h;
}


async function get(u){
  const r=await fetch(u,{
    cache:'no-store',
    credentials:'include',
    headers:authHeaders()
  });

  let j=null;

  try{
    j=await r.json();
  }catch(_){
    j=null;
  }

  if(!r.ok){
    throw Error((j&&j.detail)?j.detail:'HTTP '+r.status);
  }

  return j;
}

async function post(u,data={}){
  const r=await fetch(u,{
    method:'POST',
    credentials:'include',
    headers:authHeaders({'Content-Type':'application/json'}),
    body:JSON.stringify(data)
  });

  let j=null;

  try{
    j=await r.json();
  }catch(_){
    j=null;
  }

  if(!r.ok){
    throw Error((j&&j.detail)?j.detail:'HTTP '+r.status);
  }

  return j;
}

async function updateMarketNote(){
  syncMarketFromBroker();
  if(!otcNote) return;

  if(marketMode.value!=='OTC'){
    otcNote.style.display='none';
    return;
  }

  otcNote.style.display='block';
  otcNote.textContent='🟣 Verificando '+brokerName()+' OTC...';

  try{
    const d=await get('/otc-status?broker='+encodeURIComponent(broker.value));
    otcNote.textContent=(d.configured?'🟢 ':'🟠 ')+brokerName()+' OTC • '+(d.message||'');
  }catch(e){
    otcNote.textContent='🔴 '+brokerName()+' OTC indisponível';
  }
}


function resizeChart(){
  // Apenas redimensiona/redesenha; não liga nem desliga robô ou voz.
  const r=chartCanvas.getBoundingClientRect();
  const d=window.devicePixelRatio||1;

  chartCanvas.width=Math.max(1,r.width*d);
  chartCanvas.height=Math.max(1,r.height*d);
  chartCtx.setTransform(d,0,0,d,0,0);

  if(chartData.length) drawChart(chartData);
}

function drawChart(a){
  const w=chartCanvas.clientWidth;
  const h=chartCanvas.clientHeight;

  chartCtx.clearRect(0,0,w,h);

  if(!a.length) return;

  const pad={l:55,r:12,t:18,b:28};
  const cw=w-pad.l-pad.r;
  const ch=h-pad.t-pad.b;

  /*
    VELA ATUAL SEMPRE NO CENTRO:
    - a última vela recebida é considerada a vela atual/em formação;
    - ela fica fixa exatamente no meio horizontal do gráfico;
    - candles antigos caminham somente para a esquerda;
    - a metade direita fica livre para o movimento futuro.
  */
  const currentIndex=a.length-1;
  const centerX=pad.l+(cw/2);

  // Quantidade aproximada de candles históricos visíveis na metade esquerda.
  // Em telas pequenas mantém boa leitura sem tirar a vela atual do centro.
  const visiblePast=Math.max(18,Math.min(36,a.length-1));
  const candleSpacing=(cw/2)/Math.max(1,visiblePast);
  const candleWidth=Math.max(2,Math.min(8,candleSpacing*.68));

  // Escala vertical considera principalmente os candles que realmente aparecem.
  const firstVisible=Math.max(0,currentIndex-visiblePast);
  const visible=a.slice(firstVisible);

  let lo=Math.min(...visible.map(c=>Number(c.low)));
  let hi=Math.max(...visible.map(c=>Number(c.high)));
  const extra=(hi-lo)*.08||1;

  lo-=extra;
  hi+=extra;

  const px=i=>centerX-((currentIndex-i)*candleSpacing);
  const py=v=>pad.t+(hi-v)/(hi-lo)*ch;

  chartCtx.strokeStyle='#19304a';
  chartCtx.lineWidth=1;
  chartCtx.font='11px Arial';
  chartCtx.fillStyle='#8190a8';

  // Linhas horizontais de preço
  for(let j=0;j<5;j++){
    const y=pad.t+j*ch/4;
    chartCtx.beginPath();
    chartCtx.moveTo(pad.l,y);
    chartCtx.lineTo(w-pad.r,y);
    chartCtx.stroke();

    const v=hi-(hi-lo)*j/4;
    chartCtx.fillText(v.toFixed(5),4,y+4);
  }

  // Linha vertical fixa indicando onde SEMPRE fica a vela atual.
  chartCtx.save();
  chartCtx.strokeStyle='#6b86a8';
  chartCtx.lineWidth=1;
  chartCtx.setLineDash([5,5]);
  chartCtx.beginPath();
  chartCtx.moveTo(centerX,pad.t);
  chartCtx.lineTo(centerX,h-pad.b);
  chartCtx.stroke();
  chartCtx.setLineDash([]);

  chartCtx.font='bold 10px Arial';
  chartCtx.textAlign='center';
  chartCtx.fillStyle='#a8bbd3';
  chartCtx.fillText('VELA ATUAL',centerX,pad.t+11);
  chartCtx.restore();

  // Desenha somente candles que cabem dentro da área visível.
  a.forEach((c,i)=>{
    const x=px(i);

    if(x < pad.l-candleWidth || x > w-pad.r+candleWidth){
      return;
    }

    const o=Number(c.open);
    const cl=Number(c.close);
    const hh=Number(c.high);
    const ll=Number(c.low);
    const up=cl>=o;

    chartCtx.strokeStyle=up?'#45ff9b':'#ff5c7a';
    chartCtx.fillStyle=up?'#45ff9b':'#ff5c7a';

    chartCtx.beginPath();
    chartCtx.moveTo(x,py(hh));
    chartCtx.lineTo(x,py(ll));
    chartCtx.stroke();

    const top=py(Math.max(o,cl));
    const bot=py(Math.min(o,cl));

    chartCtx.fillRect(
      x-candleWidth/2,
      top,
      candleWidth,
      Math.max(1,bot-top)
    );

    // Horários espaçados para não sobrepor texto.
    const relative=currentIndex-i;
    const labelEvery=Math.max(4,Math.round(visiblePast/5));

    if(relative%labelEvery===0){
      chartCtx.fillStyle='#8190a8';
      chartCtx.font='10px Arial';
      chartCtx.textAlign='center';
      chartCtx.fillText(
        new Date(c.datetime).toLocaleTimeString(
          'pt-BR',
          {hour:'2-digit',minute:'2-digit'}
        ),
        x,
        h-7
      );
    }
  });

  // Reforça visualmente a vela atual no centro, sem mover a posição dela.
  const current=a[currentIndex];
  if(current){
    const currentClose=Number(current.close);

    chartCtx.save();
    chartCtx.fillStyle='#d7e7fb';
    chartCtx.font='bold 10px Arial';
    chartCtx.textAlign='left';
    chartCtx.fillText(
      Number.isFinite(currentClose) ? currentClose.toFixed(5) : '',
      Math.min(w-pad.r-48,centerX+7),
      Math.max(pad.t+22,Math.min(h-pad.b-5,py(currentClose)-6))
    );
    chartCtx.restore();
  }

  // BOLINHA = SINAL DE ENTRADA — MERCADO ABERTO E OTC:
  // aparece somente após 3 leituras consecutivas na mesma direção;
  // a confirmação ocorre dentro dos últimos 20s da vela atual;
  // depois de uma bolinha, outra só pode ser liberada após 3 minutos.
  if(chartPreSignal && chartPreSignal.active &&
     (chartPreSignal.direction==='CALL' || chartPreSignal.direction==='PUT')){

    const idx=currentIndex;
    const x=px(idx);
    const dir=chartPreSignal.direction;
    const v=dir==='CALL'
      ? Number(a[idx].low)
      : Number(a[idx].high);

    chartCtx.save();
    chartCtx.fillStyle=dir==='CALL'?'#45ff9b':'#ff5c7a';
    chartCtx.strokeStyle='#07111f';
    chartCtx.lineWidth=2;

    chartCtx.beginPath();
    chartCtx.arc(x,py(v),7,0,Math.PI*2);
    chartCtx.fill();
    chartCtx.stroke();

    chartCtx.font='bold 12px Arial';
    chartCtx.textAlign='left';
    chartCtx.fillStyle=dir==='CALL'?'#45ff9b':'#ff5c7a';
    chartCtx.fillText(dir,x+10,py(v)-9);

    chartCtx.restore();

  }else if(cur && cur.direction && cur.direction!=='NEUTRO' && cur.reference_candle){
    // Mantém o marcador do sinal confirmado quando não há pré-sinal de 20s.
    const idx=a.findIndex(c=>c.datetime===cur.reference_candle);

    if(idx>=0){
      const x=px(idx);

      if(x>=pad.l && x<=w-pad.r){
        const v=cur.direction==='CALL'
          ? Number(a[idx].low)
          : Number(a[idx].high);

        chartCtx.fillStyle=cur.direction==='CALL'?'#45ff9b':'#ff5c7a';
        chartCtx.beginPath();
        chartCtx.arc(x,py(v),6,0,Math.PI*2);
        chartCtx.fill();

        chartCtx.font='bold 12px Arial';
        chartCtx.textAlign='left';
        chartCtx.fillText(cur.direction,x+8,py(v)-8);
      }
    }
  }
}

function candleTimeKey(c){
  if(!c) return '';
  return String(c.datetime || c.time || c.timestamp || '');
}

function mergeChartCandles(oldData,newData){
  const map=new Map();

  (oldData||[]).forEach(c=>{
    const k=candleTimeKey(c);
    if(k) map.set(k,c);
  });

  (newData||[]).forEach(c=>{
    const k=candleTimeKey(c);
    if(!k) return;

    // O candle mais novo substitui a versão anterior do mesmo horário,
    // fazendo a vela em formação crescer/diminuir sem "pular".
    map.set(k,{...(map.get(k)||{}),...c});
  });

  const merged=[...map.values()].sort((a,b)=>{
    const ta=new Date(candleTimeKey(a)).getTime();
    const tb=new Date(candleTimeKey(b)).getTime();
    return ta-tb;
  });

  // Mantém histórico suficiente, sem deixar o navegador pesado.
  return merged.slice(-120);
}

async function loadChart(){
  if(!appEnabled) return;
  if(chartBusy) return;

  const iqSelected=(broker && broker.value==='IQ_OPTION');
  const openMode=(marketMode && marketMode.value==='OPEN');
  const iqConnected=!!brokerConnected.IQ_OPTION;

  // Regras do gráfico:
  // 1) IQ Option + Mercado Aberto + conectada = candles reais do mercado aberto da IQ Option.
  // 2) IQ Option + OTC + conectada = candles reais OTC da IQ Option.
  // 3) Mercado Aberto sem sessão IQ = gráfico atual da Twelve Data, para o gráfico não ficar vazio.
  // 4) OTC sem sessão IQ continua offline, pois OTC precisa da própria corretora.
  if(iqSelected && !openMode && !iqConnected){
    chartData=[];
    chartPreSignal=null;
    if(chartInfo) chartInfo.textContent='⚪ OTC IQ OPTION OFFLINE • CONECTE NA IQ OPTION';
    drawChart([]);
    return;
  }

  chartBusy=true;

  try{
    const useIqMirror=iqSelected && iqConnected;
    const chartMarket=(iqSelected && openMode && !iqConnected) ? 'OPEN' : market.value;
    const mirrorParam=useIqMirror?'&mirror_iq=true':'';
    const [d,pre]=await Promise.all([
      get(
        `/candles?market=${encodeURIComponent(chartMarket)}&symbol=${encodeURIComponent(S.value)}&interval=${encodeURIComponent(interval.value)}&n=80${mirrorParam}`
      ),
      (robotEnabled ? Promise.resolve(null) : get(
        `/chart-pre-signal?market=${encodeURIComponent(market.value)}&symbol=${encodeURIComponent(S.value)}&interval=${encodeURIComponent(interval.value)}`
      ).catch(()=>null))
    ]);

    if((d.candles||[]).length){
      chartData=mergeChartCandles(chartData,d.candles||[]);
    }

    if(pre && pre.active){
      chartPreSignal=pre;
      rememberChartSignal(pre);
    }else if(chartPreSignal && chartPreSignal.active && chartPreSignal.entry_time){
      // Não deixa polling/latência apagar ou trocar a bolinha antes da próxima vela.
      const entryMs=new Date(chartPreSignal.entry_time).getTime();
      if(!entryMs || Date.now()>=entryMs){
        chartPreSignal=null;
      }else{
        chartPreSignal.seconds_to_entry=Math.max(
          0,
          Math.ceil((entryMs-Date.now())/1000)
        );
      }
    }else{
      chartPreSignal=null;
    }

    if(useIqMirror && d.ok===false && !(d.candles||[]).length){
      chartData=[];
      chartPreSignal=null;
      chartInfo.textContent='⚪ '+(d.status||'GRÁFICO OFFLINE')+' • '+(d.message||'Conecte na IQ Option');
      drawChart([]);
      return;
    }

    let chartSourceLabel='MERCADO ABERTO • TWELVE DATA';
    if(useIqMirror && openMode) chartSourceLabel='IQ OPTION • MERCADO ABERTO';
    else if(useIqMirror && !openMode) chartSourceLabel='IQ OPTION • OTC';
    else if(!openMode) chartSourceLabel=brokerName()+' • OTC';

    chartInfo.textContent=
      (d.ok===false?'⚠️ ':'🟢 ')+
      d.symbol+' • '+chartSourceLabel+
      ' • '+d.interval+
      (d.ok===false?' • '+(d.status||'INDISPONÍVEL'):'');

    drawChart(chartData);

  }catch(e){
    if(chartPreSignal && chartPreSignal.entry_time){
      const entryMs=new Date(chartPreSignal.entry_time).getTime();
      if(!entryMs || Date.now()>=entryMs){
        chartPreSignal=null;
      }
    }
    chartInfo.textContent='⚠️ Dados temporariamente indisponíveis';
    if(!chartData.length) drawChart([]);
  }finally{
    chartBusy=false;
  }
}

function showTab(which){
  const main=which==='main';
  const chart=which==='chart';
  const results=which==='results';
  const account=which==='account';

  mainTab.classList.toggle('active',main);
  chartTab.classList.toggle('active',chart);
  resultsTab.classList.toggle('active',results);
  accountTab.classList.toggle('active',account);

  tabMain.classList.toggle('active',main);
  tabChart.classList.toggle('active',chart);
  tabResults.classList.toggle('active',results);
  tabAccount.classList.toggle('active',account);

  if(chart){
    loadChart();
    setTimeout(resizeChart,50);
  }

  if(results){
    perf();
  }

  if(account){
    refreshAccountStatus();
  }
}

tabMain.onclick=()=>showTab('main');
tabChart.onclick=()=>showTab('chart');
tabResults.onclick=()=>showTab('results');
tabAccount.onclick=()=>showTab('account');

window.addEventListener('resize',resizeChart);

async function refreshAccountStatus(){
  const b=(broker&&broker.value)||'IQ_OPTION';
  try{
    const d=await get('/otc-status?broker='+encodeURIComponent(b));
    brokerConnected[b]=!!d.connected;
  }catch(_){
    brokerConnected[b]=false;
  }
  syncBroker(b);
  await updateMarketNote();
}

if(preSignalLimit){
  preSignalLimit.onchange=()=>{
    loadPreSignals();
  };
}

if(broker){
  broker.onchange=()=>{
    syncBroker(broker.value);
    fillSymbols();
    updateMarketNote();
    lastSignalVoice='';
    lastChartSignalVoice='';
  chartData=[];
    chartPreSignal=null;
    sig(true);
    rad();
    loadPreSignals();
    if(appEnabled && !iqLoginInProgress && chartTab.classList.contains('active')) loadChart();
  };
}

if(brokerAccount){
  brokerAccount.onchange=async()=>{
    syncBroker(brokerAccount.value);
    fillSymbols();
    await refreshAccountStatus();
    lastSignalVoice='';
    lastChartSignalVoice='';
  chartData=[];
    chartPreSignal=null;
    sig(true);
    rad();
    loadPreSignals();
    if(chartTab.classList.contains('active')) loadChart();
  };
}

window.megaConnectIQ=async function(event){
  if(event){
    try{ event.preventDefault(); }catch(_){}
    try{ event.stopPropagation(); }catch(_){}
  }

  const b=(brokerAccount&&brokerAccount.value)||((broker&&broker.value)||'IQ_OPTION');
  const email=(iqEmail&&iqEmail.value||'').trim();
  const password=(iqPassword&&iqPassword.value||'');

  if(!email || !password){
    if(iqAccountStatus) iqAccountStatus.textContent='🟠 Informe e-mail e senha.';
    return false;
  }

  if(iqConnectBtn) iqConnectBtn.disabled=true;
  iqLoginInProgress=(b==='IQ_OPTION');
  if(iqAccountStatus) iqAccountStatus.textContent='🟡 Enviando login para '+(b==='OLYMPTRADE'?'Olymptrade':'IQ Option')+'...';

  try{
    if(b==='IQ_OPTION'){
      try{ localStorage.removeItem('mega_iq_session'); }catch(_){}
    }

    const url=b==='OLYMPTRADE'?'/olymp-login':'/iq-login';
    const r=await fetch(url,{
      method:'POST',
      credentials:'include',
      cache:'no-store',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({email:email,password:password})
    });

    let d=null;
    try{ d=await r.json(); }catch(_){}

    if(!r.ok){
      throw new Error((d&&d.detail)?d.detail:('HTTP '+r.status));
    }

    if(b==='IQ_OPTION' && d && d.session_token){
      try{ localStorage.setItem('mega_iq_session',d.session_token); }catch(_){}
    }

    brokerConnected[b]=true;
    if(iqPassword) iqPassword.value='';
    if(iqAccountStatus) iqAccountStatus.textContent='🟢 '+((d&&d.message)||'Conectada.');

    try{ syncBroker(b); }catch(_){}
    try{ await updateMarketNote(); }catch(_){}
    chartData=[];
    chartPreSignal=null;
    try{ if(chartTab && chartTab.classList.contains('active')) await loadChart(); }catch(_){}
    try{ sig(true); }catch(_){}
    try{ rad(); }catch(_){}
  }catch(e){
    brokerConnected[b]=false;
    if(iqAccountStatus) iqAccountStatus.textContent='🔴 '+String((e&&e.message)||e);
    if(iqConnectBtn) iqConnectBtn.style.display='block';
    if(iqLogoutBtn) iqLogoutBtn.style.display='none';
  }finally{
    iqLoginInProgress=false;
    if(iqConnectBtn) iqConnectBtn.disabled=false;
  }
  return false;
};


if(iqConnectBtn){
  // Fallback para Android/PWA: garante que o toque sempre chegue ao /iq-login.
  iqConnectBtn.addEventListener('click',function(ev){
    if(window.megaConnectIQ) window.megaConnectIQ(ev);
  });
}

if(interval){
  interval.addEventListener('change',()=>{
    loadPreSignals();
  });
}

iqLogoutBtn.onclick=async()=>{
  const b=(brokerAccount&&brokerAccount.value)||broker.value||'IQ_OPTION';
  try{
    await post(b==='OLYMPTRADE'?'/olymp-logout':'/iq-logout',{});
  }catch(_){}
  if(b==='IQ_OPTION'){
    try{
      localStorage.removeItem('mega_iq_session');
    }catch(_){}
  }
  brokerConnected[b]=false;
  iqPassword.value='';
  syncBroker(b);
  iqAccountStatus.textContent='⚪ '+(b==='OLYMPTRADE'?'Olymptrade':'IQ Option')+' desconectada.';
  if(b==='IQ_OPTION'){
    chartData=[];
    chartPreSignal=null;
    if(chartInfo) chartInfo.textContent='⚪ GRÁFICO OFFLINE • CONECTE NA IQ OPTION';
    drawChart([]);
  }
  await updateMarketNote();
};



function applyAppPowerState(){
  if(!appPowerBtn) return;

  if(appEnabled){
    appPowerBtn.textContent='🟢 DESLIGAR APP';
    appPowerBtn.classList.remove('app-off');
    appPowerBtn.classList.add('app-on');
    if(appPowerDesc) appPowerDesc.textContent='App ligado • sinais, análises e atualizações ativos.';
  }else{
    appPowerBtn.textContent='🔴 LIGAR APP';
    appPowerBtn.classList.remove('app-on');
    appPowerBtn.classList.add('app-off');
    if(appPowerDesc) appPowerDesc.textContent='App desligado • novas análises e sinais pausados.';
    if(statusBox) statusBox.textContent='APP DESLIGADO • ANÁLISES PAUSADAS';
    if(direction){
      direction.textContent='DESLIGADO';
      direction.className='big neutral';
    }
    if(confidence) confidence.textContent='Confiança: --';
    if(entry) entry.textContent='--:--:--';
    if(countdown) countdown.textContent='--';
    if(radar) radar.innerHTML='<div>⏸ APP DESLIGADO • radar pausado</div>';
    if(preSignals) preSignals.innerHTML='<div style="opacity:.75">⏸ APP DESLIGADO • pré-sinais pausados</div>';
  }
}

async function setAppPower(enabled){
  appEnabled=!!enabled;
  try{ localStorage.setItem('mega_app_power', appEnabled ? 'ON' : 'OFF'); }catch(_){}

  if(!appEnabled){
    cur=null;
    chartPreSignal=null;
    lastCountdownSignalKey='';
    fifteen=false;
    five=false;
    entered=false;
  }

  applyAppPowerState();

  if(appEnabled){
    await Promise.allSettled([sig(false), perf(), updateMarketNote()]);
    if(!robotEnabled) await Promise.allSettled([rad(), loadPreSignals()]);
    if(chartTab.classList.contains('active')) await loadChart();
    if(voiceEnabled) speak('Mega IA ligado. Análises e sinais ativados.');
  }else if(voiceEnabled){
    speak('Mega IA desligado. Análises e sinais pausados.');
  }
}

if(appPowerBtn){
  appPowerBtn.onclick=()=>{ setAppPower(!appEnabled); };
}

function applyRobotPowerState(){
  if(!robotPowerBtn) return;

  if(robotEnabled){
    robotPowerBtn.textContent='🟢 ONLINE';
    robotPowerBtn.style.background='#0b7a3d';
    robotPowerBtn.style.color='#fff';
    robotPowerBtn.style.borderColor='#16c56b';
    if(robotModeDesc) robotModeDesc.textContent='ONLINE: indicadores internos desligados • somente IA.';
    if(statusBox && (!cur || cur.direction==='NEUTRO')){
      statusBox.textContent='MODO ONLINE • IA PURA MONITORANDO';
    }
    if(preSignalStatus) preSignalStatus.textContent='Modo IA pura: pré-sinais técnicos desativados.';
    if(preSignals) preSignals.innerHTML='<div style="opacity:.75">🧠 ONLINE: somente a IA gera sinais.</div>';
    if(radar) radar.innerHTML='<div>🧠 IA PURA ativa • indicadores internos desligados</div>';
  }else{
    robotPowerBtn.textContent='🔴 OFFLINE';
    robotPowerBtn.style.background='#7d1d1d';
    robotPowerBtn.style.color='#fff';
    robotPowerBtn.style.borderColor='#ff5252';
    if(robotModeDesc) robotModeDesc.textContent='OFFLINE: modo normal • estratégias e indicadores internos ativos.';
    if(statusBox && (!cur || cur.direction==='NEUTRO')){
      statusBox.textContent='MODO NORMAL • INDICADORES ATIVOS';
    }
  }
}

async function setRobotPower(enabled){
  robotEnabled=!!enabled;

  try{
    localStorage.setItem('mega_robot_power', robotEnabled ? 'ONLINE' : 'OFFLINE');
  }catch(_){}

  // Limpa o último sinal visual ao trocar de motor para não misturar modos.
  cur=null;
  lastSignalVoice='';
  lastChartSignalVoice='';
  chartData=[];
  chartPreSignal=null;
  lastCountdownSignalKey='';
  fifteen=false;
  five=false;
  entered=false;

  applyRobotPowerState();

  await Promise.allSettled([sig(true), perf()]);

  // Radar e pré-sinais pertencem ao motor técnico e só funcionam no modo normal.
  if(!robotEnabled){
    await Promise.allSettled([rad(), loadPreSignals()]);
  }

  if(chartTab.classList.contains('active')) loadChart();

  if(voiceEnabled){
    speak(robotEnabled
      ? 'Modo online. Somente a inteligência artificial está gerando os sinais.'
      : 'Modo offline. Estratégias e indicadores internos voltaram ao normal.');
  }
}

if(robotPowerBtn){
  robotPowerBtn.onclick=()=>{ setRobotPower(!robotEnabled); };
}

async function sig(announce=false){
  if(!appEnabled) return;
  if(sigBusy) return;

  sigBusy=true;

  if(announce && voiceEnabled && Date.now()-lastAnalysis>2500){
    lastAnalysis=Date.now();
    showRobot();
    analysisText.style.display='block';
    statusBox.textContent='ANALISANDO O MERCADO...';
    speak('O Mega IA está analisando o mercado.');
  }

  try{
    cur=await get(
      `/signal-ai?market=${encodeURIComponent(market.value)}&symbol=${encodeURIComponent(S.value)}&interval=${encodeURIComponent(interval.value)}&ai_only=${robotEnabled?'true':'false'}`
    );

    if(announce){
      showRobot();
      analysisText.style.display='block';
    }

    direction.textContent=cur.direction||'NEUTRO';
    direction.className='big '+(
      cur.direction==='CALL'?'call':
      cur.direction==='PUT'?'put':
      'neutral'
    );

    confidence.textContent='Confiança: '+Number(cur.confidence||0).toFixed(0)+'%';

    entry.textContent=
      cur.direction==='NEUTRO'||!cur.entry_time
      ? 'AGUARDANDO SINAL'
      : ft(cur.entry_time);

    countdown.textContent=
      cur.direction==='NEUTRO'
      ? 'Sem entrada confirmada'
      : 'Preparando entrada';

    statusBox.textContent=cur.status||'MONITORANDO';
    risk.textContent='Risco: '+(cur.risk||'--');

    rememberPendingTrade(cur);

    if(cur.direction!=='NEUTRO'){
      const k=cur.symbol+'|'+cur.interval+'|'+cur.entry_time+'|'+cur.direction;

      if(k!==lastSignalVoice){
        lastSignalVoice=k;

        if(voiceEnabled){
          speak(
            cur.direction==='CALL'
            ? 'Análise concluída. Sinal de CALL identificado.'
            : 'Análise concluída. Sinal de PUT identificado.'
          );
        }
      }

    }else if(announce && voiceEnabled && cur.source_state==='READY'){
      speak('Análise concluída. Não há oportunidade segura no momento.');
    }

    const countdownSignalKey =
      cur && cur.direction!=='NEUTRO' && cur.entry_time
      ? [cur.symbol,cur.interval,cur.direction,cur.entry_time].join('|')
      : '';

    if(countdownSignalKey !== lastCountdownSignalKey){
      lastCountdownSignalKey=countdownSignalKey;
      fifteen=false;
      five=false;
      entered=false;
    }

  }catch(e){
    statusBox.textContent='PAINEL ATIVO • FONTE TEMPORARIAMENTE INDISPONÍVEL';
    direction.textContent='NEUTRO';
    direction.className='big neutral';
    entry.textContent='AGUARDANDO DADOS';
    countdown.textContent='Sem entrada confirmada';

    if(announce&&voiceEnabled){
      speak('A fonte de dados está temporariamente indisponível. O painel continua monitorando.');
    }

  }finally{
    sigBusy=false;
  }
}

async function perf(){
  if(perfBusy) return;

  perfBusy=true;

  try{
    const currentMarket=activeResultMarket();
    const p=await get('/performance?market='+encodeURIComponent(currentMarket));
    mergeServerPerformance(p,currentMarket);
    paintPersistentResults();
  }catch(e){
    // Mesmo se a API estiver temporariamente indisponível, mantém o placar salvo.
    paintPersistentResults();
  }finally{
    perfBusy=false;
  }
}

async function rad(){
  if(!appEnabled) return;
  if(robotEnabled){
    if(radar) radar.innerHTML='<div>🧠 IA PURA ativa • radar técnico desativado</div>';
    return;
  }
  if(radBusy) return;

  radBusy=true;

  try{
    const a=await get(
      '/radar?market='+encodeURIComponent(market.value)+
      '&interval='+encodeURIComponent(interval.value)
    );

    radar.innerHTML=a.map(x=>`
      <div>
        <b>${x.symbol}</b><br>
        <span class="${x.direction==='CALL'?'call':x.direction==='PUT'?'put':'neutral'}">${x.direction}</span>
        • ${x.confidence}%<br>
        <small>${x.status}</small>
      </div>
    `).join('');

  }catch(e){
    radar.innerHTML='<div>⚠️ Radar temporariamente indisponível</div>';
  }finally{
    radBusy=false;
  }
}


async function loadPreSignals(){
  if(!appEnabled) return;
  if(robotEnabled){
    if(preSignalStatus) preSignalStatus.textContent='Modo IA pura: pré-sinais técnicos desativados.';
    if(preSignals) preSignals.innerHTML='<div style="opacity:.75">🧠 Somente a IA gera sinais.</div>';
    return;
  }
  if(preSignalBusy || !preSignals || !preSignalLimit) return;

  preSignalBusy=true;

  try{
    const limit=Math.max(1,Math.min(4,Number(preSignalLimit.value||4)));

    try{
      localStorage.setItem('mega_pre_signal_limit',String(limit));
    }catch(_){}

    const d=await get(
      '/pre-signals?market='+encodeURIComponent(market.value)+
      '&interval='+encodeURIComponent(interval.value)+
      '&limit='+encodeURIComponent(limit)
    );

    const items=(d&&Array.isArray(d.items))?d.items:[];

    if(preSignalStatus){
      preSignalStatus.textContent=d.message||'Monitorando pré-sinais...';
    }

    if(!items.length){
      preSignals.innerHTML=
        '<div style="opacity:.75">Nenhum CALL/PUT próximo de confirmar agora.</div>';
      return;
    }

    preSignals.innerHTML=items.map(x=>{
      const cls=x.direction==='CALL'?'call':'put';
      const remain=Math.max(0,Number(x.seconds_to_entry||0));
      return `
        <div>
          <b>${x.symbol}</b><br>
          <span class="${cls}">PRÉ-${x.direction}</span>
          • ${Number(x.confidence||0).toFixed(0)}%<br>
          <b>⏳ ${remain}s para a próxima entrada</b><br>
          <small>${x.status||'AGUARDANDO FECHAMENTO'}</small>
        </div>
      `;
    }).join('');

  }catch(e){
    if(preSignalStatus){
      preSignalStatus.textContent='Pré-sinais temporariamente indisponíveis.';
    }
    preSignals.innerHTML='';
  }finally{
    preSignalBusy=false;
  }
}

async function lic(){
  try{
    const x=await get('/license');

    if(x.active){
      licenseCard.style.display='none';
      licenseBox.textContent='';
      return;
    }

    licenseCard.style.display='block';
    licenseBox.innerHTML=
      `<b>⚠️ LICENÇA EXPIRADA</b><br>
       Renove o aplicativo para continuar usando.<br><br>
       WhatsApp: ${x.whatsapp_1} / ${x.whatsapp_2}<br>
       Instagram: ${x.instagram}`;

  }catch(e){
    licenseCard.style.display='none';
  }
}

async function clk(){
  if(iqLoginInProgress){
    const local=new Date();
    clock.textContent=local.toLocaleTimeString('pt-BR',{hour12:false})+' • Brasília';
    return;
  }
  try{
    const x=await get('/server-time');
    clock.textContent=ft(x.datetime)+' • Brasília';
  }catch(e){
    const local=new Date();
    clock.textContent=local.toLocaleTimeString('pt-BR',{hour12:false})+' • Brasília';
  }
}

function cd(){
  if(!cur || cur.direction==='NEUTRO' || !cur.entry_time){
    countdown.textContent='Sem entrada confirmada';
    expiryCountdown.textContent='⏱ EXPIRAÇÃO: --:--';
    return;
  }

  const nowMs=Date.now();
  const et=new Date(cur.entry_time).getTime();
  const xt=cur.expiry_time?new Date(cur.expiry_time).getTime():0;
  const n=Math.ceil((et-nowMs)/1000);

  countdown.textContent=n>0?'Entrada em '+n+'s':'Entrada liberada';

  if(n>0){
    expiryCountdown.textContent='⏱ EXPIRAÇÃO: aguardando entrada';
  }else if(xt){
    const rem=Math.max(0,Math.ceil((xt-nowMs)/1000));
    const mm=String(Math.floor(rem/60)).padStart(2,'0');
    const ss=String(rem%60).padStart(2,'0');
    expiryCountdown.textContent='⏱ EXPIRAÇÃO: '+mm+':'+ss;
  }else{
    expiryCountdown.textContent='⏱ EXPIRAÇÃO: --:--';
  }

  if(n<=15 && n>13 && !fifteen){
    fifteen=true;
    if(voiceEnabled){
      speak('Atenção. Sinal confirmado de '+cur.direction+'. Entrada em 15 segundos.');
    }
  }

  if(n<=5 && n>3 && !five){
    five=true;
    if(voiceEnabled){
      speak('Atenção. Entrada em 5 segundos.');
    }
  }

  if(n<=0 && n>-2 && !entered){
    entered=true;
    showEntryArrow(cur.direction);

    if(voiceEnabled){
      speak(
        cur.direction==='CALL'
        ? 'Entrada liberada. Comprar agora.'
        : 'Entrada liberada. Vender agora.'
      );
    }
  }
}

async function resultCheck(){
  if(resultBusy) return;

  if(!pendingTrade && pendingTradeQueue.length){
    promoteNextPendingTrade();
  }

  if(!pendingTrade){
    rememberPendingTrade(cur);
  }

  if(
    !pendingTrade ||
    !pendingTrade.expiry_time ||
    Date.now() < new Date(pendingTrade.expiry_time).getTime()
  ){
    return;
  }

  // Evita consultar /result a cada 3 segundos quando a fonte está em limite.
  if(Number(pendingTrade.next_result_check_at||0) > Date.now()){
    return;
  }

  // Uma pendência muito antiga não pode bloquear para sempre as operações novas.
  const expiryMs=new Date(pendingTrade.expiry_time).getTime();
  if(Number.isFinite(expiryMs) && Date.now()-expiryMs > RESULT_MAX_PENDING_AGE_MS){
    if(galeStageStatus){
      galeStageStatus.textContent='⚠️ Resultado antigo descartado após 30 min sem dados';
    }
    promoteNextPendingTrade();
    return;
  }

  resultBusy=true;

  try{
    const t=normalizePendingTradeMarket(pendingTrade);
    pendingTrade=t;

    const x=await get(
      `/result?market=${encodeURIComponent(t.market||'OPEN')}&symbol=${encodeURIComponent(t.symbol)}&interval=${encodeURIComponent(t.interval)}&direction=${encodeURIComponent(t.direction)}&expiry_time=${encodeURIComponent(t.expiry_time)}&direct_only=true`
    );

    if(x && !x.result && (x.status==='AGUARDANDO_FONTE' || String(x.status||'').startsWith('AGUARDANDO'))){
      pendingTrade.next_result_check_at=Date.now()+Math.max(
        RESULT_RETRY_MS,
        Number(x.retry_after||15)*1000
      );
      pendingTrade.result_failures=Number(pendingTrade.result_failures||0)+1;
      savePendingTrade();
      if(galeStageStatus){
        galeStageStatus.textContent='⏳ Resultado aguardando dados da fonte';
      }
      return;
    }

    // A entrada original é contabilizada imediatamente após o fechamento da vela,
    // mesmo que o acompanhamento de G1/G2 ainda continue.
    const accountingChanged=registerPersistentResult(t,x);
    if(accountingChanged){
      // Atualiza WIN/LOSS na tela imediatamente.
      paintPersistentResults();
      // Sincroniza depois, sem permitir que o servidor zere o placar local.
      await perf();
    }

    if(galeStageStatus){
      const stage=x.stage||'ENTRADA';

      if(!x.result){
        if(stage==='G1'){
          galeStageStatus.textContent='⏳ Entrada inicial não venceu • aguardando resultado do G1';
        }else if(stage==='G2'){
          galeStageStatus.textContent='⏳ G1 não venceu • aguardando resultado do G2';
        }else{
          galeStageStatus.textContent='⏳ Aguardando resultado da entrada inicial';
        }
      }
    }

    if(x.result){
      result.textContent=x.result;

      if(galeLastResult){
        galeLastResult.textContent=x.result;
        galeLastResult.className='big '+(String(x.result).startsWith('WIN')?'call':'put');
      }

      if(galeStageStatus){
        galeStageStatus.textContent=
          x.result==='WIN' ? '✅ Venceu na entrada' :
          x.result==='LOSS' ? '❌ Loss na entrada' :
          x.result==='WIN G1' ? '✅ Venceu no Gale 1' :
          x.result==='WIN G2' ? '✅ Venceu no Gale 2' :
          '❌ Não venceu até o Gale 2';
      }

      const k=t.symbol+'|'+t.direction+'|'+t.expiry_time;

      if(k!==reskey){
        reskey=k;

        if(voiceEnabled){
          if(x.result==='WIN'){
            speak('Resultado da entrada: WIN.');
          }else if(x.result==='LOSS'){
            speak('Resultado da entrada: LOSS.');
          }else{
            speak('Operação finalizada. Resultado '+x.result+'.');
          }
        }
      }

      // Atualiza também os dados do servidor após o resultado final.
      await perf();

      // Finalizada: passa para a próxima operação que estiver aguardando resultado.
      promoteNextPendingTrade();
    }

  }catch(e){
    // Falha temporária: mantém pendente, mas não consulta de novo a cada 3 segundos.
    if(pendingTrade){
      pendingTrade.next_result_check_at=Date.now()+RESULT_RETRY_MS;
      pendingTrade.result_failures=Number(pendingTrade.result_failures||0)+1;
      savePendingTrade();
    }
  }finally{
    resultBusy=false;
  }
}

marketMode.onchange=async()=>{
  syncMarketFromBroker();
  fillSymbols();
  updateMarketNote();
  lastSignalVoice='';
  lastChartSignalVoice='';
  chartData=[];
  sig(true);
  rad();
  loadPreSignals();
  if(chartTab.classList.contains('active')) loadChart();
};


S.onchange=()=>{
  try{localStorage.setItem('mega_symbol',S.value)}catch(_){}

  lastSignalVoice='';
  lastChartSignalVoice='';
  chartData=[];

  sig(true);
  rad();

  if(chartTab.classList.contains('active')){
    loadChart();
  }
};

interval.onchange=()=>{
  try{localStorage.setItem('mega_interval',interval.value)}catch(_){}

  lastSignalVoice='';
  lastChartSignalVoice='';
  chartData=[];

  sig(true);
  rad();

  if(chartTab.classList.contains('active')){
    loadChart();
  }
};

try{
  const sm=localStorage.getItem('mega_market_mode');
  if(sm==='OPEN'||sm==='OTC'){
    marketMode.value=sm;
  }
  syncMarketFromBroker();
  fillSymbols();

  const ss=localStorage.getItem('mega_symbol');
  if(ss && [...S.options].some(o=>o.value===ss)){
    S.value=ss;
  }

  const si=localStorage.getItem('mega_interval');
  if(si && [...interval.options].some(o=>o.value===si)){
    interval.value=si;
  }

}catch(_){
  fillSymbols();
}

async function bootApp(){
  syncMarketFromBroker();
  applyAppPowerState();
  applyRobotPowerState();
  applyVoiceState();

  const safe=(name,fn)=>
    Promise.resolve()
      .then(fn)
      .catch(err=>{
        console.error('[MEGA IA] '+name,err);

        if(name==='signal'){
          statusBox.textContent='MONITORANDO • DADOS TEMPORARIAMENTE INDISPONÍVEIS';
        }
      });

  safe('clock',clk);
  safe('license',lic);
  safe('account',refreshAccountStatus);
  safe('market-status',updateMarketNote);

  if(market.value!=='OPEN'){
    await new Promise(r=>setTimeout(r,700));
  }

  if(appEnabled) safe('signal',()=>sig(false));
  if(appEnabled && !robotEnabled){
    safe('radar',rad);
    safe('pre-signals',loadPreSignals);
  }
  if(appEnabled && chartTab.classList.contains('active')){
    safe('chart',loadChart);
  }

  if(appEnabled) safe('performance',perf);
}

bootApp().catch(err=>{
  console.error('[MEGA IA] boot',err);
  statusBox.textContent='PAINEL INICIADO COM AVISO';
});

setInterval(()=>{ if(appEnabled && !iqLoginInProgress) sig(false); },5000);

setInterval(()=>{
  if(appEnabled && !iqLoginInProgress && chartTab.classList.contains('active')) loadChart();
},2000);

setInterval(()=>{ if(appEnabled && !iqLoginInProgress) perf(); },5000);
// Radar completo atualizado a cada 1 minuto.
setInterval(()=>{
  if(appEnabled && !iqLoginInProgress && !robotEnabled) rad();
},60000);
// Pré-análise atualizada a cada 1 minuto; a confirmação continua usando a janela final de 1 minuto.
setInterval(()=>{
  if(appEnabled && !iqLoginInProgress && !robotEnabled) loadPreSignals();
},60000);

// Com o app ligado, acompanha o resultado das operações abertas.
setInterval(()=>{ if(appEnabled && !iqLoginInProgress) resultCheck(); },5000);
setInterval(clk,1000);
setInterval(()=>{ if(appEnabled) cd(); },250);
</script>
</body>
</html>
"""

HTML_PAGE = HTML_PAGE.replace("__MEGA_IMAGE__", "/mega-ia.png")


@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def home():
    return HTMLResponse(
        HTML_PAGE,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
            "X-Mega-IA-Build": "33.36.0",
        },
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
