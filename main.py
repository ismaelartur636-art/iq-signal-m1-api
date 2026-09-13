import os
import asyncio
import time
import json
import re
import secrets
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict

import httpx

IQ_IMPORT_ERROR = ""
try:
    from iqoptionapi.aio import AsyncIQOption
except Exception as exc:
    AsyncIQOption = None
    IQ_IMPORT_ERROR = f"{type(exc).__name__}: {exc}"

from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse

app = FastAPI(title="MEGA IA", version="32.9.0")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
IMAGE_PATH = os.path.join(BASE_DIR, "mega_ia.png")
ICON_512_PATH = os.path.join(BASE_DIR, "mega_ia_icon.png")
ICON_192_PATH = os.path.join(BASE_DIR, "mega_ia_icon_192.png")

BR_TZ = ZoneInfo("America/Sao_Paulo")
UTC = timezone.utc

TD_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
OAI_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OAI_MODEL = os.getenv("OPENAI_MODEL", "").strip()
OAI_MIN = float(os.getenv("OPENAI_MIN_CONFIDENCE", "70"))
OAI_TIMEOUT = float(os.getenv("OPENAI_TIMEOUT", "15"))


TD_URL = "https://api.twelvedata.com/time_series"
OAI_URL = "https://api.openai.com/v1/responses"

INTERVALS = {"1min": 60, "5min": 300, "15min": 900, "30min": 1800}
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
oai_cache: Dict[str, Any] = {}
results: Dict[str, Any] = {}
radar_cache: Dict[str, Any] = {}

td_sem = asyncio.Semaphore(1)
td_candle_cache: Dict[str, Any] = {}
td_locks: Dict[str, asyncio.Lock] = {}
td_rate_lock = asyncio.Lock()
td_last_call_at = 0.0
td_backoff_until = 0.0
td_backoff_reason = ""
TD_MIN_CALL_INTERVAL = float(os.getenv("TWELVE_DATA_MIN_INTERVAL", "8.0"))
TD_STALE_MAX_AGE = float(os.getenv("TWELVE_DATA_STALE_MAX_AGE", "900"))

IQ_SESSION_COOKIE = "mega_iq_session"
IQ_SESSION_TTL = int(os.getenv("IQ_SESSION_TTL", "43200"))
IQ_RECONNECT_BASE_DELAY = float(os.getenv("IQ_RECONNECT_BASE_DELAY", "6"))
IQ_RECONNECT_MAX_DELAY = float(os.getenv("IQ_RECONNECT_MAX_DELAY", "60"))
IQ_CANDLE_TIMEOUT = float(os.getenv("IQ_CANDLE_TIMEOUT", "9"))
iq_sessions: Dict[str, Dict[str, Any]] = {}


class IQLoginBody(BaseModel):
    email: str
    password: str


class IQ2FABody(BaseModel):
    code: str


class IQTwoFactorRequired(RuntimeError):
    pass


def now():
    return datetime.now(BR_TZ)


def iso(d: datetime):
    return d.astimezone(BR_TZ).isoformat()


def parse_dt(value: str):
    d = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return (d if d.tzinfo else d.replace(tzinfo=UTC)).astimezone(BR_TZ)


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def _cleanup_iq_sessions():
    now_ts = time.time()
    expired = [k for k, v in iq_sessions.items() if now_ts - v.get("last_seen", now_ts) > IQ_SESSION_TTL]
    for token in expired:
        state = iq_sessions.pop(token, None)
        if state:
            state["password"] = ""
            _iq_dispose_client(state)


def _mask_email(email: str):
    if "@" not in email:
        return "***"
    name, domain = email.split("@", 1)
    visible = name[:1] if name else ""
    return visible + "•••••@" + domain


def _session_state(request: Request, required: bool = False):
    _cleanup_iq_sessions()
    cookie_token = request.cookies.get(IQ_SESSION_COOKIE, "")
    header_token = request.headers.get("X-IQ-Session", "")
    for token in (header_token, cookie_token):
        if not token:
            continue
        state = iq_sessions.get(token)
        if state:
            state["last_seen"] = time.time()
            state["session_id"] = token
            return state
    if required:
        raise HTTPException(401, "Conecte sua conta da IQ Option na aba Conta IQ Option.")
    return None


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


def ema_rsi_strategy(cs):
    if len(cs) < 35:
        return {"direction": "NEUTRO", "confidence": 0, "confirmed": False,
                "reason": "Poucos candles fechados.", "strategy": "EMA 9/21 + RSI 7"}

    closes = [c["close"] for c in cs]
    e9_now, e21_now = ema(closes, 9), ema(closes, 21)
    e9_prev, e21_prev = ema(closes[:-1], 9), ema(closes[:-1], 21)
    r_now = rsi(closes, 7)
    r_prev = rsi(closes[:-1], 7)
    if None in (e9_now, e21_now, e9_prev, e21_prev, r_now, r_prev):
        return {"direction": "NEUTRO", "confidence": 0, "confirmed": False,
                "reason": "Indicadores insuficientes.", "strategy": "EMA 9/21 + RSI 7"}

    cross_up = e9_prev <= e21_prev and e9_now > e21_now
    cross_down = e9_prev >= e21_prev and e9_now < e21_now
    call_ok = cross_up and r_now > 50 and r_now > r_prev and r_now < 70
    put_ok = cross_down and r_now < 50 and r_now < r_prev and r_now > 30

    if call_ok:
        return {"direction": "CALL", "confidence": 82, "confirmed": True,
                "reason": "EMA 9 cruzou acima da EMA 21; RSI 7 acima de 50 e subindo, abaixo de 70.",
                "strategy": "EMA 9/21 + RSI 7", "ema9": e9_now, "ema21": e21_now, "rsi7": r_now}
    if put_ok:
        return {"direction": "PUT", "confidence": 82, "confirmed": True,
                "reason": "EMA 9 cruzou abaixo da EMA 21; RSI 7 abaixo de 50 e caindo, acima de 30.",
                "strategy": "EMA 9/21 + RSI 7", "ema9": e9_now, "ema21": e21_now, "rsi7": r_now}

    return {"direction": "NEUTRO", "confidence": 50, "confirmed": False,
            "reason": "Não houve cruzamento EMA 9/21 com confirmação completa do RSI 7.",
            "strategy": "EMA 9/21 + RSI 7", "ema9": e9_now, "ema21": e21_now, "rsi7": r_now}


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


def macd_strategy(cs):
    if len(cs) < 50:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,"reason":"Poucos candles.","strategy":"MACD momentum"}
    closes=[c["close"] for c in cs]
    fast=ema_series(closes,12)
    slow=ema_series(closes,26)
    if len(fast)<10 or len(slow)<3:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,"reason":"MACD insuficiente.","strategy":"MACD momentum"}
    m=[]
    common=min(len(fast),len(slow))
    for i in range(1, common+1):
        m.append(fast[-i]-slow[-i])
    m=list(reversed(m))
    sig=ema_series(m,9)
    if len(sig)<2 or len(m)<2:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,"reason":"MACD sem sinal.","strategy":"MACD momentum"}
    line_now,line_prev=m[-1],m[-2]
    sig_now,sig_prev=sig[-1],sig[-2]
    e50=ema(closes,50)
    r=rsi(closes,14)
    up=line_prev<=sig_prev and line_now>sig_now and closes[-1]>e50 and r and 52<r<72
    dn=line_prev>=sig_prev and line_now<sig_now and closes[-1]<e50 and r and 28<r<48
    if up:
        return {"direction":"CALL","confidence":80,"confirmed":True,"reason":"MACD cruzou para cima com tendência e RSI favoráveis.","strategy":"MACD momentum"}
    if dn:
        return {"direction":"PUT","confidence":80,"confirmed":True,"reason":"MACD cruzou para baixo com tendência e RSI favoráveis.","strategy":"MACD momentum"}
    return {"direction":"NEUTRO","confidence":48,"confirmed":False,"reason":"MACD sem confirmação completa.","strategy":"MACD momentum"}


def price_action_strategy(cs):
    if len(cs) < 30:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,"reason":"Poucos candles.","strategy":"Price action"}
    closes=[c["close"] for c in cs]
    e21=ema(closes,21)
    e50=ema(closes,50) if len(closes)>=50 else ema(closes,21)
    a,b=cs[-2],cs[-1]
    bull_engulf=b["close"]>b["open"] and a["close"]<a["open"] and b["open"]<=a["close"] and b["close"]>=a["open"]
    bear_engulf=b["close"]<b["open"] and a["close"]>a["open"] and b["open"]>=a["close"] and b["close"]<=a["open"]
    wi=wick_info(b)
    pin_call=wi["call_wick"]>=0.55 and wi["body_ratio"]<=0.35
    pin_put=wi["put_wick"]>=0.55 and wi["body_ratio"]<=0.35
    trend_up=e21 is not None and e50 is not None and e21>=e50
    trend_dn=e21 is not None and e50 is not None and e21<=e50
    if (bull_engulf or pin_call) and trend_up:
        return {"direction":"CALL","confidence":78 if bull_engulf else 74,"confirmed":True,"reason":"Rejeição/engolfo comprador alinhado à tendência.","strategy":"Price action"}
    if (bear_engulf or pin_put) and trend_dn:
        return {"direction":"PUT","confidence":78 if bear_engulf else 74,"confirmed":True,"reason":"Rejeição/engolfo vendedor alinhado à tendência.","strategy":"Price action"}
    return {"direction":"NEUTRO","confidence":45,"confirmed":False,"reason":"Padrão de preço sem alinhamento.","strategy":"Price action"}


def volatility_breakout_strategy(cs):
    if len(cs) < 35:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,"reason":"Poucos candles.","strategy":"Breakout ATR"}
    closes=[c["close"] for c in cs]
    a=atr(cs,14)
    if not a:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,"reason":"ATR insuficiente.","strategy":"Breakout ATR"}
    last=cs[-1]
    prev=cs[-11:-1]
    hi=max(c["high"] for c in prev)
    lo=min(c["low"] for c in prev)
    body=abs(last["close"]-last["open"])
    e20=ema(closes,20)
    call=last["close"]>hi and body>=0.45*a and last["close"]>e20
    put=last["close"]<lo and body>=0.45*a and last["close"]<e20
    if call:
        return {"direction":"CALL","confidence":79,"confirmed":True,"reason":"Rompimento de máxima recente com expansão de volatilidade.","strategy":"Breakout ATR"}
    if put:
        return {"direction":"PUT","confidence":79,"confirmed":True,"reason":"Rompimento de mínima recente com expansão de volatilidade.","strategy":"Breakout ATR"}
    return {"direction":"NEUTRO","confidence":44,"confirmed":False,"reason":"Sem breakout válido.","strategy":"Breakout ATR"}


def local_engine(cs):
    strategies = [
        bollinger_stochastic(cs),
        ema_rsi_strategy(cs),
        macd_strategy(cs),
        price_action_strategy(cs),
        volatility_breakout_strategy(cs),
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


def iq_active_candidates(symbol):
    base = symbol.replace("/", "").upper()
    preferred = OTC_BASE.get(symbol, f"{base}-OTC")
    return list(dict.fromkeys([preferred, f"{base}-OTC", f"{base}_OTC"]))


def iq_seconds(interval):
    return INTERVALS[interval]


def _iq_is_connected(state):
    client = state.get("client")
    return bool(client is not None and getattr(client, "_ws", None) is not None)


def _iq_dispose_client(state):
    client = state.get("client")
    state["client"] = None
    if client is None:
        return
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(client.close())
    except RuntimeError:
        pass
    except Exception:
        pass


async def _iq_close_client(state):
    client = state.get("client")
    state["client"] = None
    if client is not None:
        try:
            await client.close()
        except Exception:
            pass


def _iq_mark_failure(state, message):
    failures = int(state.get("failure_count", 0)) + 1
    state["failure_count"] = failures
    delay = min(IQ_RECONNECT_MAX_DELAY, IQ_RECONNECT_BASE_DELAY * (2 ** min(failures - 1, 4)))
    state["reconnect_after"] = time.time() + delay
    state["last_error"] = str(message)[:350]
    _iq_dispose_client(state)
    return delay


def _iq_mark_connected(state, client):
    state["client"] = client
    state["last_error"] = ""
    state["failure_count"] = 0
    state["reconnect_after"] = 0.0
    state["last_seen"] = time.time()
    state["last_connected"] = time.time()
    state["requires_2fa"] = False
    return client


def _iq_reason_text(reason):
    if reason is None:
        return ""
    if isinstance(reason, (dict, list, tuple)):
        try:
            return json.dumps(reason, ensure_ascii=False)
        except Exception:
            return str(reason)
    return str(reason)


def _iq_reason_requires_2fa(reason):
    t = _iq_reason_text(reason).lower()
    return any(x in t for x in (
        "2fa", "two-factor", "two factor", "two_factor",
        "verification", "verify", "otp", "sms code", "auth code"
    ))


async def iq_connect_async(state, force=False):
    if AsyncIQOption is None:
        raise RuntimeError("Cliente assíncrono da IQ Option não carregado no servidor.")

    now_ts = time.time()
    if not force and _iq_is_connected(state):
        return state.get("client")

    reconnect_after = float(state.get("reconnect_after", 0.0) or 0.0)
    if now_ts < reconnect_after:
        wait = max(1, int(reconnect_after - now_ts + 0.999))
        raise RuntimeError(f"IQ Option reconectando. Nova tentativa em {wait}s.")

    if force:
        await _iq_close_client(state)

    email = state.get("email", "")
    password = state.get("password", "")
    if not email or not password:
        raise RuntimeError("Sessão IQ Option sem credenciais ativas. Conecte novamente.")

    state["last_attempt"] = now_ts
    state["reconnecting"] = True
    state["requires_2fa"] = False

    client = AsyncIQOption(email, password)
    state["client"] = client
    try:
        await client.connect()
        if getattr(client, "_ws", None) is None:
            raise RuntimeError("A IQ Option não manteve o websocket conectado.")
        return _iq_mark_connected(state, client)
    except Exception as exc:
        msg = f"{type(exc).__name__}: {exc}"
        if _iq_reason_requires_2fa(msg):
            state["requires_2fa"] = True
            state["last_error"] = (
                "A conta exige 2FA. O cliente assíncrono atual da biblioteca "
                "ainda não implementa a etapa de verificação."
            )
            await _iq_close_client(state)
            raise IQTwoFactorRequired(state["last_error"])
        delay = _iq_mark_failure(state, msg)
        raise RuntimeError(f"Falha no login/websocket IQ Option: {msg[:260]}. Nova tentativa em {int(delay)}s.")
    finally:
        state["reconnecting"] = False


async def iq_candles_async(state, symbol, interval, n):
    duration = iq_seconds(interval)
    candidates = iq_active_candidates(symbol)
    errors = []
    client = await iq_connect_async(state, force=False)

    for name in candidates:
        try:
            chunk = await client.get_candles(
                active=name,
                size=duration,
                count=int(n),
                endtime=int(time.time()),
                timeout=IQ_CANDLE_TIMEOUT,
            )
            if chunk and len(chunk) >= 5:
                out = []
                for x in chunk:
                    try:
                        ts = float(x.get("from", x.get("at", 0)))
                        dt = datetime.fromtimestamp(ts, tz=UTC).astimezone(BR_TZ).isoformat()
                        out.append({
                            "datetime": dt,
                            "open": float(x["open"]),
                            "high": float(x.get("max", x.get("high"))),
                            "low": float(x.get("min", x.get("low"))),
                            "close": float(x["close"]),
                            "volume": float(x.get("volume", 0) or 0),
                            "source": name,
                        })
                    except Exception:
                        continue
                out.sort(key=lambda z: z["datetime"])
                if len(out) >= 5:
                    _iq_mark_connected(state, client)
                    return out
            errors.append(f"{name}: sem candles")
        except Exception as exc:
            errors.append(f"{name}: {type(exc).__name__}: {exc}")

    detail = " | ".join(errors[-3:])
    delay = _iq_mark_failure(state, f"OTC sem candles. {detail}")
    raise RuntimeError(f"OTC sem candles. {detail[:220]}. Nova tentativa em {int(delay)}s.")


async def candles(symbol, interval, n=80, market="OPEN", iq_state=None):
    market = (market or "OPEN").upper()

    if market == "OTC":
        if symbol not in SYMBOLS or interval not in INTERVALS:
            raise HTTPException(400, "Ativo ou intervalo inválido.")
        if iq_state is None:
            raise HTTPException(401, "Conecte sua conta da IQ Option na aba Conta IQ Option.")

        ckey = f"{symbol}|{interval}"
        candle_cache = iq_state.setdefault("candle_cache", {})
        cached = candle_cache.get(ckey)
        now_ts = time.time()

        if cached and now_ts - cached[0] < 20 and len(cached[1]) >= min(n, 20):
            return cached[1][-n:]

        reconnect_after = float(iq_state.get("reconnect_after", 0.0) or 0.0)
        reconnecting = bool(iq_state.get("reconnecting")) or now_ts < reconnect_after

        if reconnecting:
            if cached and now_ts - cached[0] < 90:
                return cached[1][-n:]
            wait = max(1, int(reconnect_after-now_ts+0.999)) if reconnect_after > now_ts else 2
            raise HTTPException(503, f"IQ Option reconectando. Aguarde {wait}s.")

        try:
            async with iq_state["lock"]:
                cached = candle_cache.get(ckey)
                if cached and time.time() - cached[0] < 20 and len(cached[1]) >= min(n, 20):
                    return cached[1][-n:]

                fetch_n = max(100, min(150, int(n)))
                out = await asyncio.wait_for(
                    iq_candles_async(iq_state, symbol, interval, fetch_n),
                    timeout=max(15.0, IQ_CANDLE_TIMEOUT + 5.0),
                )
                candle_cache[ckey] = (time.time(), out)

        except HTTPException:
            raise
        except Exception as exc:
            stale = candle_cache.get(ckey)
            if stale and time.time() - stale[0] < 90:
                return stale[1][-n:]
            raise HTTPException(503, f"IQ Option OTC indisponível: {str(exc)[:300]}")

        if not out:
            raise HTTPException(503, "Nenhum candle OTC recebido da IQ Option.")

        return out[-n:]

    return await candles_open(symbol, interval, n)


async def openai_confirm(symbol, interval, cs, analysis):
    if not OAI_KEY or not OAI_MODEL:
        return {"available": False, "reason": "OPENAI_API_KEY/OPENAI_MODEL não configurados."}

    key = f"{symbol}|{interval}|{cs[-1]['datetime']}"
    if key in oai_cache and time.time() - oai_cache[key][0] < 55:
        return oai_cache[key][1]

    data = [
        {"time": c["datetime"], "o": c["open"], "h": c["high"], "l": c["low"], "c": c["close"], "v": c["volume"]}
        for c in cs[-40:]
    ]

    prompt = f"""Você é o módulo de confirmação da MEGA IA. Ativo {symbol}, timeframe {interval}. Use SOMENTE candles fechados. Não invente dados futuros. Estratégias internas: reversão, tendência, momentum, price action e breakout com filtro de volatilidade. Análise técnica preliminar: {json.dumps(analysis, ensure_ascii=False)} Retorne SOMENTE JSON: {{"direction":"CALL|PUT|NEUTRO","confidence":0,"confirmed":true,"reason":"curto","risk":"LOW|MEDIUM|HIGH"}} Candles: {json.dumps(data, ensure_ascii=False)}"""

    try:
        async with httpx.AsyncClient(timeout=OAI_TIMEOUT) as client:
            response = await client.post(
                OAI_URL,
                headers={"Authorization": f"Bearer {OAI_KEY}", "Content-Type": "application/json"},
                json={"model": OAI_MODEL, "input": prompt},
            )
            response.raise_for_status()
            payload = response.json()

        text = payload.get("output_text", "")
        if not text:
            for item in payload.get("output", []):
                for content in item.get("content", []):
                    if content.get("type") in ("output_text", "text"):
                        text += content.get("text", "")

        p = json_extract(text)
        if not isinstance(p, dict):
            raise ValueError("Resposta inválida da IA.")

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


async def signal(symbol, interval, market="OPEN", iq_state=None):
    if symbol not in SYMBOLS or interval not in INTERVALS:
        raise HTTPException(400, "Ativo ou intervalo inválido.")

    market = (market or "OPEN").upper()
    session_part = iq_state.get("session_id", "") if (market == "OTC" and iq_state) else "PUBLIC"
    key = f"{session_part}|{market}|{symbol}|{interval}"

    if key in cache and time.time() - cache[key][0] < 4:
        return cache[key][1]

    if market == "OTC" and iq_state is not None:
        ra = float(iq_state.get("reconnect_after", 0.0) or 0.0)
        # Durante o backoff real, não libera entrada. Fora dele, deixa candles()
        # tentar restaurar automaticamente o websocket da sessão preservada.
        if iq_state.get("reconnecting") or time.time() < ra:
            neutral = neutral_signal(
                symbol, interval, market,
                "IQ OPTION RECONECTANDO",
                "A sessão OTC está sendo restaurada. Nenhuma entrada será liberada até a conexão voltar.",
                source_state="WAITING",
            )
            cache[key] = (time.time(), neutral)
            return neutral

    try:
        raw = await candles(symbol, interval, 90, market, iq_state)
    except HTTPException as exc:
        status = (
            "FONTE EM LIMITE"
            if market == "OPEN" and exc.status_code in (429, 503)
            else ("IQ OPTION RECONECTANDO" if market == "OTC" else "FONTE INDISPONÍVEL")
        )
        out = neutral_signal(symbol, interval, market, status, exc.detail, source_state="DEGRADED")
        cache[key] = (time.time(), out)
        return out
    except Exception as exc:
        status = "IQ OPTION RECONECTANDO" if market == "OTC" else "FONTE INDISPONÍVEL"
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
    analysis = local_engine(closed)

    base = {
        "symbol": symbol,
        "interval": interval,
        "market": market,
        "direction": "NEUTRO",
        "confidence": analysis["confidence"],
        "entry_time": None,
        "announce_time": None,
        "expiry_time": None,
        "status": "MONITORANDO",
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
        announce, entry, expiry = entry_window(interval)
        base["entry_time"] = iso(entry)
        base["announce_time"] = iso(announce)
        base["expiry_time"] = iso(expiry)
        base["reference_candle"] = closed[-1]["datetime"] if closed else None

    base["source_state"] = "READY"
    cache[key] = (time.time(), base)
    return base


@app.get("/health")
async def health():
    now_ts = time.time()
    return {
        "status": "ok",
        "app": "MEGA IA",
        "version": "32.8.0",
        "brasilia_time": iso(now()),
        "twelve_data": {
            "configured": bool(TD_KEY),
            "backoff": now_ts < td_backoff_until,
            "retry_in": max(0, int(td_backoff_until - now_ts)),
            "cache_items": len(td_candle_cache),
            "last_reason": td_backoff_reason[:120],
        },
        "iq_option": {
            "library": bool(AsyncIQOption is not None),
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
        "id": "/mega-ia-trader-v3290",
        "name": "Mega IA Trader",
        "short_name": "Mega IA",
        "description": "Mega IA Trader",
        "start_url": "/?pwa=v3290",
        "scope": "/",
        "display": "standalone",
        "orientation": "portrait",
        "background_color": "#02050b",
        "theme_color": "#07182b",
        "icons": [
            {"src": "/mega-ia-icon-192.png?v=3282", "sizes": "192x192", "type": "image/png", "purpose": "any"},
            {"src": "/mega-ia-icon.png?v=3282", "sizes": "512x512", "type": "image/png", "purpose": "any"},
            {"src": "/mega-ia-icon.png?v=3282", "sizes": "512x512", "type": "image/png", "purpose": "maskable"},
        ],
    }
    return Response(
        content=json.dumps(manifest_data, ensure_ascii=False),
        media_type="application/manifest+json",
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )



@app.get("/iq-diagnostic")
async def iq_diagnostic():
    info = {
        "app_version": "32.9.0",
        "iq_async_library_loaded": AsyncIQOption is not None,
        "import_error": IQ_IMPORT_ERROR if AsyncIQOption is None else "",
        "active_sessions": len(iq_sessions),
    }
    if AsyncIQOption is not None:
        try:
            import iqoptionapi
            info["iqoptionapi_module"] = getattr(iqoptionapi, "__file__", "")
            info["iqoptionapi_version"] = getattr(iqoptionapi, "__version__", "")
        except Exception as exc:
            info["module_probe_error"] = f"{type(exc).__name__}: {exc}"
    return info


@app.post("/iq-login")
async def iq_login(body: IQLoginBody, response: Response):
    email = body.email.strip()
    password = body.password
    print(f"[IQ LOGIN] tentativa para domínio={email.split('@')[-1] if '@' in email else 'invalido'} biblioteca_async={'OK' if AsyncIQOption is not None else 'AUSENTE'}", flush=True)

    if not email or not password:
        raise HTTPException(400, "Informe e-mail e senha da IQ Option.")
    if AsyncIQOption is None:
        raise HTTPException(503, f"Biblioteca IQ Option não carregada no Render. {IQ_IMPORT_ERROR or 'Verifique o requirements.txt.'}")

    token = secrets.token_urlsafe(32)
    state = {
        "session_id": token, "email": email, "password": password, "client": None,
        "lock": asyncio.Lock(), "last_error": "", "last_attempt": 0.0,
        "last_seen": time.time(), "last_connected": 0.0, "failure_count": 0,
        "reconnect_after": 0.0, "reconnecting": False, "requires_2fa": False,
        "generation": 0, "candle_cache": {}, "results": {},
    }

    try:
        async with state["lock"]:
            client = await asyncio.wait_for(iq_connect_async(state), timeout=22)
        if not _iq_is_connected(state):
            raise RuntimeError("A sessão assíncrona não permaneceu conectada.")

    except IQTwoFactorRequired:
        print("[IQ LOGIN] 2FA solicitado pela IQ Option", flush=True)
        iq_sessions[token] = state
        response.set_cookie(
            IQ_SESSION_COOKIE, token, httponly=True, secure=True, samesite="lax",
            max_age=IQ_SESSION_TTL, expires=IQ_SESSION_TTL, path="/",
        )
        return {
            "connected": False, "requires_2fa": True,
            "message": "A IQ Option solicitou um código de verificação.",
            "email_masked": _mask_email(email), "session_token": token,
        }

    except asyncio.TimeoutError:
        print("[IQ LOGIN] timeout aguardando resposta da IQ Option", flush=True)
        state["password"] = ""
        _iq_dispose_client(state)
        raise HTTPException(504, "A IQ Option não respondeu ao login dentro do tempo esperado.")

    except Exception as exc:
        print(f"[IQ LOGIN] falhou: {type(exc).__name__}: {str(exc)[:350]}", flush=True)
        state["password"] = ""
        _iq_dispose_client(state)
        raise HTTPException(401, f"Não foi possível conectar à IQ Option: {type(exc).__name__}: {str(exc)[:350]}")

    print("[IQ LOGIN] conectado com sucesso", flush=True)
    iq_sessions[token] = state
    response.set_cookie(
        IQ_SESSION_COOKIE, token, httponly=True, secure=True, samesite="lax",
        max_age=IQ_SESSION_TTL, expires=IQ_SESSION_TTL, path="/",
    )
    return {
        "connected": True, "requires_2fa": False,
        "message": "IQ Option OTC conectada.",
        "email_masked": _mask_email(email), "session_token": token,
    }


@app.post("/iq-2fa")
async def iq_2fa(body: IQ2FABody, request: Request):
    state = _session_state(request, required=True)
    raise HTTPException(
        501,
        "A biblioteca assíncrona atual da IQ Option ainda não implementa a conclusão do 2FA. "
        "Desative temporariamente o 2FA da conta para testar esta conexão, ou use uma integração que suporte essa etapa."
    )


@app.post("/iq-logout")
async def iq_logout(request: Request, response: Response):
    header_token = request.headers.get("X-IQ-Session", "")
    cookie_token = request.cookies.get(IQ_SESSION_COOKIE, "")
    token = header_token if header_token in iq_sessions else cookie_token
    state = iq_sessions.pop(token, None)

    if state:
        state["password"] = ""
        await _iq_close_client(state)
        state["candle_cache"] = {}

    response.delete_cookie(IQ_SESSION_COOKIE, path="/")

    return {"connected": False, "message": "Conta IQ Option desconectada."}


@app.get("/otc-status")
async def otc_status(request: Request):
    if AsyncIQOption is None:
        return {
            "configured": False,
            "connected": False,
            "message": "Biblioteca IQ Option não foi carregada.",
        }

    state = _session_state(request, required=False)

    if not state:
        return {
            "configured": True,
            "connected": False,
            "message": "Conecte sua conta na aba Conta IQ Option.",
        }

    connected = _iq_is_connected(state)
    if state.get("requires_2fa"):
        return {
            "configured": True,
            "connected": False,
            "requires_2fa": True,
            "message": "Digite o código de verificação enviado pela IQ Option.",
            "email_masked": _mask_email(state.get("email", "")),
        }

    reconnect_after = float(state.get("reconnect_after", 0) or 0)

    # Se a página foi atualizada e a sessão ainda existe, recupera o websocket
    # automaticamente quando necessário.
    if (
        not connected
        and not state.get("reconnecting")
        and time.time() >= reconnect_after
        and state.get("email")
        and state.get("password")
    ):
        try:
            async with state["lock"]:
                await asyncio.wait_for(iq_connect_async(state, False), timeout=18)
            connected = _iq_is_connected(state)
        except Exception as exc:
            state["last_error"] = str(exc)[:250]

    reconnect_after = float(state.get("reconnect_after", 0) or 0)
    reconnecting = bool(state.get("reconnecting")) or (not connected and time.time() < reconnect_after)

    if connected:
        msg = "IQ Option OTC conectada."
    elif reconnecting:
        wait = max(1, int(reconnect_after-time.time()+0.999)) if reconnect_after > time.time() else 2
        msg = f"Sessão preservada. Reconectando em {wait}s."
    else:
        msg = "Sessão preservada. Reconexão automática em andamento."

    return {
        "configured": True,
        "connected": connected,
        "reconnecting": reconnecting,
        "message": msg,
        "email_masked": _mask_email(state.get("email", "")),
        "pairs": len(OTC_BASE),
    }


@app.get("/candles")
async def candles_endpoint( request: Request, symbol: str = "EUR/USD", interval: str = "1min", n: int = 80, market: str = "OPEN", ):
    market = (market or "OPEN").upper()

    if symbol not in SYMBOLS or interval not in INTERVALS or market not in ("OPEN", "OTC"):
        raise HTTPException(400, "Ativo, intervalo ou mercado inválido.")

    n = max(20, min(int(n), 150))
    state = _session_state(request, required=False)

    try:
        if market == "OTC" and not state:
            return {
                "ok": False,
                "symbol": symbol,
                "interval": interval,
                "market": market,
                "candles": [],
                "status": "LOGIN NECESSÁRIO",
                "message": "Conecte sua conta IQ Option.",
            }

        values = await candles(symbol, interval, n, market, state)

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

        elif state:
            item = state.setdefault("candle_cache", {}).get(f"{symbol}|{interval}")
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
async def signal_ai(request: Request, symbol="EUR/USD", interval="1min", market="OPEN"):
    market = (market or "OPEN").upper()

    if symbol not in SYMBOLS or interval not in INTERVALS or market not in ("OPEN", "OTC"):
        raise HTTPException(400, "Ativo, intervalo ou mercado inválido.")

    state = _session_state(request, required=False)

    if market == "OTC" and not state:
        return neutral_signal(
            symbol,
            interval,
            market,
            "LOGIN IQ OPTION NECESSÁRIO",
            "Conecte sua conta IQ Option para receber candles OTC.",
            source_state="LOGIN_REQUIRED",
        )

    try:
        return await signal(symbol, interval, market, state)
    except Exception as exc:
        return neutral_signal(
            symbol,
            interval,
            market,
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


@app.get("/radar")
async def radar(request: Request, interval="1min", market="OPEN"):
    market = (market or "OPEN").upper()

    if interval not in INTERVALS or market not in ("OPEN", "OTC"):
        raise HTTPException(400, "Intervalo inválido.")

    if market == "OTC":
        state = _session_state(request, required=False)

        if not state:
            return [
                {
                    "symbol": s + " • OTC",
                    "direction": "NEUTRO",
                    "confidence": 0,
                    "status": "LOGIN IQ OPTION",
                }
                for s in SYMBOLS
            ]

        cc = state.setdefault("candle_cache", {})

        # V32.2:
        # O radar antigo apenas LIA o cache. Por isso somente o par selecionado
        # tinha dados e todos os outros ficavam eternamente em "AGUARDANDO DADOS".
        # Agora cada chamada do radar aquece 1 par OTC por vez, em sequência,
        # reutilizando o mesmo lock da sessão para não criar chamadas websocket
        # simultâneas na IQ Option.
        idx_key = f"radar_index|{interval}"
        idx = int(state.get(idx_key, 0) or 0) % len(SYMBOLS)

        # Prioriza um par que ainda não possui cache recente.
        chosen_idx = idx
        for offset in range(len(SYMBOLS)):
            test_idx = (idx + offset) % len(SYMBOLS)
            sym_test = SYMBOLS[test_idx]
            item = cc.get(f"{sym_test}|{interval}")
            if not item or time.time() - item[0] > 75:
                chosen_idx = test_idx
                break

        chosen = SYMBOLS[chosen_idx]
        state[idx_key] = (chosen_idx + 1) % len(SYMBOLS)

        # Evita rajadas se houver duas abas abertas ou chamadas muito próximas.
        radar_next = float(state.get("radar_next_fetch", 0.0) or 0.0)
        if time.time() >= radar_next:
            state["radar_next_fetch"] = time.time() + 12.0
            try:
                await candles(chosen, interval, 90, "OTC", state)
            except Exception as exc:
                state["radar_last_error"] = str(exc)[:180]

        out = []

        for sym in SYMBOLS:
            item = cc.get(f"{sym}|{interval}")

            if not item:
                out.append({
                    "symbol": sym + " • OTC",
                    "direction": "NEUTRO",
                    "confidence": 0,
                    "status": "CARREGANDO DADOS",
                })
                continue

            age = time.time() - item[0]

            if age > 180:
                out.append({
                    "symbol": sym + " • OTC",
                    "direction": "NEUTRO",
                    "confidence": 0,
                    "status": "ATUALIZANDO DADOS",
                })
                continue

            try:
                raw = item[1]

                if len(raw) < 25:
                    out.append({
                        "symbol": sym + " • OTC",
                        "direction": "NEUTRO",
                        "confidence": 0,
                        "status": "POUCOS CANDLES",
                    })
                    continue

                tech = local_engine(raw[:-1] if len(raw) > 1 else raw)
                direction = tech["direction"] if tech.get("confirmed") else "NEUTRO"

                out.append({
                    "symbol": sym + " • OTC",
                    "direction": direction,
                    "confidence": round(float(tech.get("confidence", 0) or 0), 1),
                    "status": "OPORTUNIDADE TÉCNICA" if direction != "NEUTRO" else "MONITORANDO",
                })

            except Exception:
                out.append({
                    "symbol": sym + " • OTC",
                    "direction": "NEUTRO",
                    "confidence": 0,
                    "status": "SEM DADOS",
                })

        return out

    rkey = f"OPEN|{interval}"
    previous = radar_cache.get(rkey)

    out = list(previous[1]) if previous else [
        {
            "symbol": sym,
            "direction": "NEUTRO",
            "confidence": 0,
            "status": "AGUARDANDO LEITURA",
        }
        for sym in SYMBOLS
    ]

    if time.time() >= td_backoff_until:
        idx_key = f"OPEN_RADAR_INDEX|{interval}"
        idx = int(cache.get(idx_key, (0, 0))[1] or 0) % len(SYMBOLS)
        sym = SYMBOLS[idx]
        cache[idx_key] = (time.time(), (idx + 1) % len(SYMBOLS))
        by = {x["symbol"]: i for i, x in enumerate(out)}

        try:
            raw = await candles(sym, interval, 90, "OPEN", None)
            age = _td_cache_age(sym, interval)

            if age <= max(90.0, INTERVALS[interval] * 0.8):
                tech = local_engine(raw[:-1] if len(raw) > 1 else raw)
                direction = tech["direction"] if tech.get("confirmed") else "NEUTRO"

                item = {
                    "symbol": sym,
                    "direction": direction,
                    "confidence": round(float(tech.get("confidence", 0) or 0), 1),
                    "status": "OPORTUNIDADE TÉCNICA" if direction != "NEUTRO" else "MONITORANDO",
                }
            else:
                item = {
                    "symbol": sym,
                    "direction": "NEUTRO",
                    "confidence": 0,
                    "status": "AGUARDANDO DADOS",
                }

        except Exception:
            item = {
                "symbol": sym,
                "direction": "NEUTRO",
                "confidence": 0,
                "status": "FONTE EM ESPERA",
            }

        out[by.get(sym, idx)] = item

    radar_cache[rkey] = (time.time(), out)
    return out


@app.get("/performance")
async def performance(request: Request, interval="1min", market="OPEN"):
    market=(market or "OPEN").upper()

    if market == "OTC":
        state = _session_state(request, required=True)
        store = state.setdefault("results", {})
    else:
        store = results

    wins = sum(1 for x in store.values() if x.get("result") == "WIN")
    losses = sum(1 for x in store.values() if x.get("result") == "LOSS")
    total = wins + losses

    return {
        "wins": wins,
        "losses": losses,
        "total": total,
        "accuracy": round(wins / total * 100, 2) if total else 0,
    }


@app.get("/result")
async def result( request: Request, symbol="EUR/USD", interval="1min", direction="CALL", expiry_time="", market="OPEN", ):
    if not expiry_time:
        raise HTTPException(400, "expiry_time é obrigatório.")

    market=(market or "OPEN").upper()
    state = _session_state(request, required=(market == "OTC"))
    store = state.setdefault("results", {}) if market == "OTC" else results
    key = f"{market}|{symbol}|{interval}|{direction}|{expiry_time}"

    if key in store:
        return store[key]

    expiry_dt = parse_dt(expiry_time)

    if now() < expiry_dt:
        return {"status": "PENDENTE", "result": None}

    cs = await candles(symbol, interval, 30, market, state)

    entry_dt = expiry_dt - timedelta(seconds=INTERVALS[interval])

    target = None
    best_delta = None

    for c in cs:
        try:
            cdt = parse_dt(c["datetime"])
        except Exception:
            continue

        delta = abs((cdt - entry_dt).total_seconds())

        if best_delta is None or delta < best_delta:
            best_delta = delta
            target = c

    if not target or (best_delta is not None and best_delta > INTERVALS[interval] * 0.75):
        return {"status": "AGUARDANDO CANDLE", "result": None}

    direction = direction.upper()
    if direction not in ("CALL", "PUT"):
        raise HTTPException(400, "Direção inválida.")

    if target["close"] == target["open"]:
        res = "EMPATE"
    else:
        res = "WIN" if (
            (direction == "CALL" and target["close"] > target["open"])
            or
            (direction == "PUT" and target["close"] < target["open"])
        ) else "LOSS"

    out = {
        "status": "FINALIZADA",
        "result": res,
        "candle_time": target["datetime"],
        "entry_time": iso(entry_dt),
        "expiry_time": expiry_time,
        "simulated": True,
    }

    store[key] = out
    return out


HTML_PAGE = r""" <!doctype html> <html lang="pt-BR"> <head> <meta charset="utf-8"> <meta name="viewport" content="width=device-width,initial-scale=1"> <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate"> <meta http-equiv="Pragma" content="no-cache"> <meta http-equiv="Expires" content="0"> <title>Mega IA Trader</title> <link rel="manifest" href="/manifest.webmanifest?v=3280"> <link rel="icon" type="image/png" sizes="512x512" href="/mega-ia-icon.png?v=3282"> <link rel="apple-touch-icon" sizes="192x192" href="/mega-ia-icon-192.png?v=3282"> <meta name="theme-color" content="#07182b"> <meta name="application-name" content="Mega IA Trader"> <meta name="apple-mobile-web-app-title" content="Mega IA Trader"> <meta name="apple-mobile-web-app-capable" content="yes"> <style> body{margin:0;background:radial-gradient(circle at 50% 0,#07182b 0,#030812 42%,#02050b 100%);color:#eef5ff;font-family:Arial,sans-serif} .wrap{max-width:1150px;margin:auto;padding:18px} .brand{font-size:42px;font-weight:900;letter-spacing:1px;margin:8px 0 2px} .brand span{color:#14c8ff} .subtitle{font-size:13px;color:#91a9c8;letter-spacing:.7px} .card{background:linear-gradient(180deg,#0c1a2c,#091422);border:1px solid #164f80;border-radius:20px;padding:16px;box-shadow:0 12px 30px #0008,0 0 18px #009cff12;margin-top:12px} .grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px} .signal{grid-column:span 2;text-align:center;min-height:270px;position:relative} .big{font-size:32px;font-weight:800;margin:8px} .call{color:#45ff9b} .put{color:#ff5c7a} .neutral{color:#ffd166} .label{font-size:11px;color:#8190a8} .controls{display:flex;gap:10px;flex-wrap:wrap;margin-top:12px} select,button,input{background:#0d2035;color:#fff;border:1px solid #22689d;border-radius:14px;padding:12px 14px;font-size:15px} select:focus,button:focus,input:focus{outline:none;box-shadow:0 0 0 2px #00bfff55} button{cursor:pointer} input{box-sizing:border-box;width:100%;margin-top:6px} .account-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px} .account-actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px} .radar{display:grid;grid-template-columns:repeat(3,1fr);gap:8px} .radar div{background:#101b2b;padding:10px;border-radius:12px;border:1px solid #173c5e} .tabs{display:flex;gap:8px;margin-top:12px;margin-bottom:20px;flex-wrap:wrap} .tabbtn.active{border-color:#168cff;box-shadow:0 0 15px #168cff44} .tab{display:none} .tab.active{display:block} #chartTab{width:100%;display:none;justify-content:center;align-items:flex-start} #chartTab.active{display:flex;padding-top:7vh;box-sizing:border-box} #chartTab>.card{width:min(96vw,1100px)!important;max-width:1100px!important;margin:0 auto!important;padding:14px!important;box-sizing:border-box} .chartbox{position:relative;width:100%!important;height:clamp(460px,68vh,700px)!important;margin:0 auto!important;background:#07101c;border:1px solid #1d3049;border-radius:16px;overflow:hidden} .chartbox canvas{width:100%!important;height:100%!important;display:block} .chartmeta{display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:10px} .chartbadge{padding:7px 10px;border-radius:10px;background:#101b2b;color:#b8c7dd;font-size:12px} .hero{display:none;position:relative;width:100%;max-width:760px;margin:26px auto 18px;border-radius:20px;overflow:hidden;border:1px solid #0bbcff;box-shadow:0 0 30px #00aaff55;background:#05111f} .hero img{display:block;width:100%;height:300px;object-fit:contain;object-position:center;background:#05111f} .entry-arrow{display:none;position:absolute;inset:0;align-items:center;justify-content:center;flex-direction:column;background:#02081488;backdrop-filter:blur(1px);font-weight:900;text-shadow:0 0 18px currentColor} .entry-arrow .arrow{font-size:110px;line-height:.8} .entry-arrow .arrow-label{font-size:28px;margin-top:8px} .entry-arrow.call{display:flex;color:#31ff87} .entry-arrow.put{display:flex;color:#ff405f} .analysisbar{text-align:center;font-size:20px;color:#22c9ff;border-color:#0bbcff} @media(max-width:720px){ .wrap{padding:10px} .grid{grid-template-columns:1fr 1fr} .signal{grid-column:span 2} .radar{grid-template-columns:1fr 1fr} .hero img{height:250px} .brand{font-size:36px} #chartTab.active{padding-top:8vh} #chartTab>.card{width:calc(100vw - 20px)!important;max-width:none!important;padding:10px!important} .chartbox{height:58vh!important;min-height:440px!important;max-height:620px!important} } @media(max-width:600px){ .account-grid{grid-template-columns:1fr} } @media(max-width:450px){ .wrap{padding:8px} .grid{grid-template-columns:1fr} .signal{grid-column:span 1} .radar{grid-template-columns:1fr} .hero img{height:230px} #chartTab.active{padding-top:9vh} #chartTab>.card{width:calc(100vw - 12px)!important;padding:7px!important} .chartbox{height:56vh!important;min-height:420px!important} } </style> </head> <body> <div class="wrap"> <div class="brand">🤖 MEGA <span>IA</span></div> <div class="subtitle">ANÁLISE EM TEMPO REAL • HORÁRIO DE BRASÍLIA</div> <div id="clock" style="font-size:22px;margin-top:4px"></div> <div class="controls"> <select id="market" onchange="handleMarketChange()"> <option value="OPEN">🌐 Mercado Aberto</option> <option value="OTC">🟣 IQ Option OTC</option> </select> <select id="symbol" onchange="handleSymbolChange()"> <option value="EUR/USD" selected>EUR/USD</option> <option value="GBP/USD">GBP/USD</option> <option value="USD/JPY">USD/JPY</option> <option value="AUD/USD">AUD/USD</option> <option value="USD/CAD">USD/CAD</option> <option value="USD/CHF">USD/CHF</option> <option value="NZD/USD">NZD/USD</option> <option value="EUR/JPY">EUR/JPY</option> <option value="GBP/JPY">GBP/JPY</option> <option value="EUR/GBP">EUR/GBP</option> <option value="BTC/USD">BTC/USD</option> <option value="ETH/USD">ETH/USD</option> <option value="LTC/USD">LTC/USD</option> </select> <select id="interval" onchange="handleIntervalChange()"> <option>1min</option> <option>5min</option> <option>15min</option> <option>30min</option> </select> <button type="button" id="voiceBtn" onclick="voice();return false;">🔊 Ativar voz</button> <div id="otcNote" class="label" style="margin-top:6px;display:none">🟣 IQ Option OTC: verificando conexão...</div> </div> <div class="tabs"> <button type="button" class="tabbtn active" id="tabMain" onclick="showTab('main');return false;">📊 Painel</button> <button type="button" class="tabbtn" id="tabChart" onclick="showTab('chart');return false;">📈 Gráfico</button> <button type="button" class="tabbtn" id="tabAccount" onclick="showTab('account');return false;">⚙️ Conta IQ Option</button> </div> <div id="mainTab" class="tab active"> <div id="heroBox" class="hero"> <img id="aiImage" src="__MEGA_IMAGE__" alt="MEGA IA analisando o mercado"> <div id="entryArrow" class="entry-arrow"> <div class="arrow" id="entryArrowIcon">⬆</div> <div class="arrow-label" id="entryArrowLabel">CALL • COMPRAR</div> </div> </div> <div id="analysisText" class="card analysisbar" style="display:none"> 🧠 ESTOU ANALISANDO O MERCADO, AGUARDE... </div> <div class="grid"> <div class="card signal"> <div class="label">SINAL ATUAL</div> <div id="direction" class="big neutral">AGUARDANDO</div> <div id="confidence">Confiança: --</div> </div> <div class="card"> <div class="label">ENTRADA</div> <div id="entry" class="big">--:--:--</div> <div id="countdown">--</div> <div id="expiryCountdown" style="margin-top:8px;font-weight:800">⏱ EXPIRAÇÃO: --:--</div> </div> <div class="card"> <div class="label">STATUS IA</div> <div id="status" class="big" style="font-size:18px">MONITORANDO</div> <div id="risk">Risco: --</div> </div> </div> <div class="grid"> <div class="card"> <div class="label">WIN</div> <div id="wins" class="big call">0</div> </div> <div class="card"> <div class="label">LOSS</div> <div id="losses" class="big put">0</div> </div> <div class="card"> <div class="label">ASSERTIVIDADE</div> <div id="accuracy" class="big">0%</div> </div> <div class="card"> <div class="label">RESULTADO</div> <div id="result" class="big">--</div> </div> </div> </div> <div id="chartTab" class="tab"> <div class="card"> <div class="chartmeta"> <b>📈 Gráfico espelhado</b> <span class="chartbadge" id="chartInfo">--</span> </div> <div class="chartbox"><canvas id="priceChart"></canvas></div> <div class="label" style="margin-top:8px"> O gráfico acompanha o mesmo mercado, par e período selecionados no painel. </div> </div> </div> <div id="accountTab" class="tab"> <div class="card"> <h2 style="margin-top:0">⚙️ Conta IQ Option</h2> <div class="label"> Use sua própria conta para liberar o gráfico e os candles OTC. </div> <div class="account-grid"> <label>E-mail <input id="iqEmail" type="email" autocomplete="username" placeholder="Seu e-mail da IQ Option"> </label> <label>Senha <input id="iqPassword" type="password" autocomplete="current-password" placeholder="Sua senha"> </label> <label id="iq2faWrap" style="display:none">Código de verificação <input id="iq2faCode" type="text" inputmode="numeric" autocomplete="one-time-code" placeholder="Código enviado pela IQ Option"> </label> </div> <div class="account-actions"> <button type="button" id="iqConnectBtn">🟢 Conectar</button> <button type="button" id="iq2faBtn" style="display:none">🔐 Validar código</button> <button type="button" id="iqLogoutBtn">🔴 Desconectar</button> </div> <div id="iqAccountStatus" class="card" style="margin-top:12px">● Desconectado</div> </div> </div> <div class="card"> <b>Radar de oportunidades</b> <div id="radar" class="radar"></div> </div> </div> <script> (function(){ 'use strict'; var VERSION='32.9.0'; var symbols=['EUR/USD','GBP/USD','USD/JPY','AUD/USD','USD/CAD','USD/CHF','NZD/USD','EUR/JPY','GBP/JPY','EUR/GBP','BTC/USD','ETH/USD','LTC/USD']; var E={}; var currentSignal=null; var voiceEnabled=false; var signalBusy=false, radarBusy=false, perfBusy=false, chartBusy=false, resultBusy=false; var pendingTrade=null; var chartData=[]; var lastResultKey=''; var stats={OPEN:{wins:0,losses:0,keys:[]},OTC:{wins:0,losses:0,keys:[]}}; function id(x){ return document.getElementById(x); } function text(el,v){ if(el) el.textContent=v; } function safeStoreGet(k){ try{return localStorage.getItem(k)||'';}catch(e){return '';} } function safeStoreSet(k,v){ try{localStorage.setItem(k,v);}catch(e){} } function safeStoreDel(k){ try{localStorage.removeItem(k);}catch(e){} } function encode(v){ return encodeURIComponent(v==null?'':String(v)); } function timeFmt(v){ if(!v) return '--:--:--'; try{return new Date(v).toLocaleTimeString('pt-BR',{hour12:false});}catch(e){return '--:--:--';} } function bindElements(){ var names=['market','symbol','interval','voiceBtn','otcNote','heroBox','entryArrow','entryArrowIcon','entryArrowLabel','analysisText','mainTab','chartTab','accountTab','tabMain','tabChart','tabAccount','iqEmail','iqPassword','iq2faWrap','iq2faCode','iqConnectBtn','iq2faBtn','iqLogoutBtn','iqAccountStatus','chartInfo','direction','confidence','entry','countdown','status','risk','wins','losses','accuracy','result','radar','clock','expiryCountdown','priceChart']; for(var i=0;i<names.length;i++) E[names[i]]=id(names[i]); } function request(url,options){ options=options||{}; options.cache='no-store'; options.credentials='include'; options.headers=options.headers||{}; var token=safeStoreGet('mega_iq_session_token'); if(token) options.headers['X-IQ-Session']=token; return fetch(url,options).then(function(r){ return r.text().then(function(raw){ var data=null; try{data=raw?JSON.parse(raw):{};}catch(e){data={};} if(!r.ok){ throw new Error((data&&data.detail)||('HTTP '+r.status)); } return data; }); }); } function get(url){ return request(url); } function post(url,data){ return request(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data||{})}); } function loadStats(){ try{ var raw=safeStoreGet('mega_stats_3280'); if(!raw) raw=safeStoreGet('mega_winloss_v3271'); if(!raw) return; var x=JSON.parse(raw); ['OPEN','OTC'].forEach(function(m){ if(x&&x[m]){ stats[m].wins=Number(x[m].wins||0); stats[m].losses=Number(x[m].losses||0); stats[m].keys=Array.isArray(x[m].keys)?x[m].keys.slice(-500):[]; } }); }catch(e){} } function saveStats(){ safeStoreSet('mega_stats_3280',JSON.stringify(stats)); } function paintStats(){ var m=(E.market&&E.market.value)||'OPEN'; var s=stats[m]||stats.OPEN; var total=s.wins+s.losses; text(E.wins,String(s.wins)); text(E.losses,String(s.losses)); text(E.accuracy,(total?((s.wins/total)*100).toFixed(2):'0.00')+'%'); } function registerResult(t,x){ if(!t||!x||(x.result!=='WIN'&&x.result!=='LOSS')) return; var m=(t.market||'OPEN').toUpperCase(); var s=stats[m]||stats.OPEN; var key=[m,t.symbol,t.interval,t.direction,t.entry_time,t.expiry_time].join('|'); if(s.keys.indexOf(key)>=0) return; s.keys.push(key); if(s.keys.length>500) s.keys=s.keys.slice(-500); if(x.result==='WIN') s.wins++; else s.losses++; saveStats(); paintStats(); } function fillSymbols(){ if(!E.market||!E.symbol) return; var old=E.symbol.value||safeStoreGet('mega_symbol')||'EUR/USD'; var otc=E.market.value==='OTC'; while(E.symbol.firstChild) E.symbol.removeChild(E.symbol.firstChild); for(var i=0;i<symbols.length;i++){ var s=symbols[i],o=document.createElement('option'); o.value=s; o.textContent=otc?('🟣 '+s.replace('/','')+'-OTC'):s; E.symbol.appendChild(o); } E.symbol.value=old; if(!E.symbol.value) E.symbol.selectedIndex=0; } function currentSymbol(){ if(!E.symbol||!E.symbol.value) fillSymbols(); return (E.symbol&&E.symbol.value)||'EUR/USD'; } function showTab(which){ var m=which==='main',c=which==='chart',a=which==='account'; if(E.mainTab) E.mainTab.classList.toggle('active',m); if(E.chartTab) E.chartTab.classList.toggle('active',c); if(E.accountTab) E.accountTab.classList.toggle('active',a); if(E.tabMain) E.tabMain.classList.toggle('active',m); if(E.tabChart) E.tabChart.classList.toggle('active',c); if(E.tabAccount) E.tabAccount.classList.toggle('active',a); if(c){ setTimeout(function(){ resizeChart(); loadChart(); },50); } if(a) refreshAccountStatus(); } window.showTab=showTab; function speak(msg){ if(!voiceEnabled||!window.speechSynthesis) return; try{ window.speechSynthesis.cancel(); var u=new SpeechSynthesisUtterance(msg); u.lang='pt-BR';u.rate=.95;u.pitch=.9; window.speechSynthesis.speak(u); }catch(e){} } function voice(){ voiceEnabled=true;text(E.voiceBtn,'🔊 Voz ativada');speak('Voz da Mega IA ativada.');setTimeout(function(){loadSignal(true);},300); } window.voice=voice; function updateMarketNote(){ if(!E.otcNote||!E.market) return Promise.resolve(); if(E.market.value!=='OTC'){E.otcNote.style.display='none';return Promise.resolve();} E.otcNote.style.display='block';text(E.otcNote,'🟣 IQ Option OTC: verificando conexão...'); return get('/otc-status').then(function(x){ text(E.otcNote,(x.connected?'🟢 ':x.reconnecting?'🟡 ':'🔴 ')+(x.message||'')); }).catch(function(e){text(E.otcNote,'🔴 IQ Option OTC: '+e.message);}); } function handleMarketChange(){ if(!E.market) return; safeStoreSet('mega_market',E.market.value);fillSymbols();paintStats();updateMarketNote(); chartData=[];loadSignal(true);loadRadar();if(E.chartTab&&E.chartTab.classList.contains('active'))loadChart(); } function handleSymbolChange(){ safeStoreSet('mega_symbol',currentSymbol());chartData=[];loadSignal(true);loadRadar();if(E.chartTab&&E.chartTab.classList.contains('active'))loadChart(); } function handleIntervalChange(){ if(E.interval)safeStoreSet('mega_interval',E.interval.value);chartData=[];loadSignal(true);loadRadar();if(E.chartTab&&E.chartTab.classList.contains('active'))loadChart(); } window.handleMarketChange=handleMarketChange;window.handleSymbolChange=handleSymbolChange;window.handleIntervalChange=handleIntervalChange; function rememberPending(sig){ if(!sig||(sig.direction!=='CALL'&&sig.direction!=='PUT')||!sig.entry_time||!sig.expiry_time) return; if(pendingTrade&&pendingTrade.expiry_time&&Date.now()<new Date(pendingTrade.expiry_time).getTime()+120000) return; pendingTrade={market:sig.market||E.market.value,symbol:sig.symbol,interval:sig.interval,direction:sig.direction,entry_time:sig.entry_time,expiry_time:sig.expiry_time}; safeStoreSet('mega_pending_trade',JSON.stringify(pendingTrade)); } function paintSignal(x){ currentSignal=x||{}; var d=currentSignal.direction||'NEUTRO'; text(E.direction,d); if(E.direction) E.direction.className='big '+(d==='CALL'?'call':d==='PUT'?'put':'neutral'); text(E.confidence,'Confiança: '+Number(currentSignal.confidence||0).toFixed(0)+'%'); text(E.entry,(d==='NEUTRO'||!currentSignal.entry_time)?'AGUARDANDO SINAL':timeFmt(currentSignal.entry_time)); text(E.countdown,d==='NEUTRO'?'Sem entrada confirmada':'Preparando entrada'); text(E.status,currentSignal.status||'MONITORANDO'); text(E.risk,'Risco: '+(currentSignal.risk||'--')); rememberPending(currentSignal); } function loadSignal(announce){ if(signalBusy||!E.market||!E.interval) return Promise.resolve(); signalBusy=true; if(announce){text(E.status,'ANALISANDO O MERCADO...');} var u='/signal-ai?market='+encode(E.market.value)+'&symbol='+encode(currentSymbol())+'&interval='+encode(E.interval.value); return get(u).then(function(x){paintSignal(x);if(announce&&x.direction&&x.direction!=='NEUTRO')speak('Sinal de '+x.direction+' identificado.');}) .catch(function(e){text(E.status,'PAINEL ATIVO • FONTE TEMPORARIAMENTE INDISPONÍVEL');text(E.direction,'NEUTRO');text(E.entry,'AGUARDANDO DADOS');}) .then(function(){signalBusy=false;},function(){signalBusy=false;}); } function loadPerformance(){ if(perfBusy||!E.market)return;perfBusy=true; var m=E.market.value; get('/performance?market='+encode(m)).then(function(p){ var s=stats[m]||stats.OPEN;s.wins=Math.max(s.wins,Number(p.wins||0));s.losses=Math.max(s.losses,Number(p.losses||0));saveStats(); }).catch(function(){}).then(function(){paintStats();perfBusy=false;}); } function loadRadar(){ if(radarBusy||!E.radar||!E.market||!E.interval)return;radarBusy=true; get('/radar?market='+encode(E.market.value)+'&interval='+encode(E.interval.value)).then(function(a){ if(!Array.isArray(a)) a=[];var h=''; for(var i=0;i<a.length;i++){ var x=a[i]||{},cls=x.direction==='CALL'?'call':x.direction==='PUT'?'put':'neutral'; h+='<div><b>'+String(x.symbol||'')+'</b><br><span class="'+cls+'">'+String(x.direction||'NEUTRO')+'</span> • '+Number(x.confidence||0)+'%<br><small>'+String(x.status||'')+'</small></div>'; } E.radar.innerHTML=h||'<div>Monitorando oportunidades...</div>'; }).catch(function(){E.radar.innerHTML='<div>⚠️ Radar temporariamente indisponível</div>';}).then(function(){radarBusy=false;}); } function updateClock(){ get('/server-time').then(function(x){text(E.clock,timeFmt(x.datetime)+' • Brasília');}).catch(function(){text(E.clock,new Date().toLocaleTimeString('pt-BR',{hour12:false})+' • Brasília');}); } function resizeChart(){ if(!E.priceChart)return;var ctx=E.priceChart.getContext('2d');if(!ctx)return; var r=E.priceChart.getBoundingClientRect(),d=window.devicePixelRatio||1; E.priceChart.width=Math.max(1,Math.floor(r.width*d));E.priceChart.height=Math.max(1,Math.floor(r.height*d));ctx.setTransform(d,0,0,d,0,0);drawChart(chartData); } function drawChart(a){ if(!E.priceChart)return;var ctx=E.priceChart.getContext('2d');if(!ctx)return; var w=E.priceChart.clientWidth,h=E.priceChart.clientHeight;ctx.clearRect(0,0,w,h);if(!a||!a.length)return; var lows=[],highs=[],i;for(i=0;i<a.length;i++){lows.push(Number(a[i].low));highs.push(Number(a[i].high));} var lo=Math.min.apply(null,lows),hi=Math.max.apply(null,highs),pad=20,span=(hi-lo)||1; function px(n){return pad+n/(Math.max(1,a.length-1))*(w-pad*2);}function py(v){return pad+(hi-v)/span*(h-pad*2);} for(i=0;i<a.length;i++){ var c=a[i],x=px(i),o=Number(c.open),cl=Number(c.close),hh=Number(c.high),ll=Number(c.low),up=cl>=o; ctx.strokeStyle=up?'#45ff9b':'#ff5c7a';ctx.fillStyle=ctx.strokeStyle;ctx.beginPath();ctx.moveTo(x,py(hh));ctx.lineTo(x,py(ll));ctx.stroke(); var y1=py(Math.max(o,cl)),y2=py(Math.min(o,cl));ctx.fillRect(x-2,y1,4,Math.max(1,y2-y1)); } } function loadChart(){ if(chartBusy||!E.market||!E.interval)return;chartBusy=true; var u='/candles?market='+encode(E.market.value)+'&symbol='+encode(currentSymbol())+'&interval='+encode(E.interval.value)+'&n=80'; get(u).then(function(d){ if(d&&Array.isArray(d.candles)&&d.candles.length)chartData=d.candles; text(E.chartInfo,(d.market==='OTC'?'🟣 '+String(d.symbol||'')+' • OTC':String(d.symbol||''))+' • '+String(d.interval||''));resizeChart(); }).catch(function(){text(E.chartInfo,'⚠️ Dados temporariamente indisponíveis');}).then(function(){chartBusy=false;}); } function show2FA(show){ if(E.iq2faWrap)E.iq2faWrap.style.display=show?'block':'none'; if(E.iq2faBtn)E.iq2faBtn.style.display=show?'inline-flex':'none'; } function refreshAccountStatus(){ return get('/otc-status').then(function(x){ if(x.requires_2fa){ show2FA(true); text(E.iqAccountStatus,'🟡 Digite o código de verificação enviado pela IQ Option.'); if(E.iqConnectBtn)E.iqConnectBtn.disabled=true; if(E.iqLogoutBtn)E.iqLogoutBtn.disabled=false; return; } show2FA(false); var rr=!!x.reconnecting; text(E.iqAccountStatus,(x.connected&&!rr?'🟢 ':rr?'🟡 ':'🔴 ')+(x.connected&&!rr?('Conectado: '+(x.email_masked||'')):(x.message||'Desconectado'))); if(E.iqConnectBtn)E.iqConnectBtn.disabled=!!x.connected||rr; if(E.iqLogoutBtn)E.iqLogoutBtn.disabled=!x.connected&&!rr; }).catch(function(){text(E.iqAccountStatus,'🟡 Verificando conexão...');}); } function connectIQ(){ var email=E.iqEmail?E.iqEmail.value.trim():'',password=E.iqPassword?E.iqPassword.value:''; if(!email||!password){text(E.iqAccountStatus,'🔴 Informe e-mail e senha.');return;} show2FA(false); text(E.iqAccountStatus,'🟡 Conectando à IQ Option...'); if(E.iqConnectBtn)E.iqConnectBtn.disabled=true; post('/iq-login',{email:email,password:password}).then(function(x){ if(x.session_token)safeStoreSet('mega_iq_session_token',x.session_token); if(x.requires_2fa){ show2FA(true); text(E.iqAccountStatus,'🟡 A IQ Option pediu um código de verificação. Digite o código recebido.'); if(E.iqPassword)E.iqPassword.value=''; return; } if(E.iqPassword)E.iqPassword.value=''; text(E.iqAccountStatus,'🟢 Conectado: '+(x.email_masked||'')); updateMarketNote(); loadSignal(true); }).catch(function(e){ text(E.iqAccountStatus,'🔴 '+e.message); }).then(function(){ if(E.iqConnectBtn)E.iqConnectBtn.disabled=false; }); } function submitIQ2FA(){ var code=E.iq2faCode?E.iq2faCode.value.trim():''; if(!code){text(E.iqAccountStatus,'🔴 Digite o código enviado pela IQ Option.');return;} text(E.iqAccountStatus,'🟡 Validando código...'); if(E.iq2faBtn)E.iq2faBtn.disabled=true; post('/iq-2fa',{code:code}).then(function(x){ if(x.session_token)safeStoreSet('mega_iq_session_token',x.session_token); if(E.iq2faCode)E.iq2faCode.value=''; show2FA(false); text(E.iqAccountStatus,'🟢 Conectado: '+(x.email_masked||'')); updateMarketNote(); loadSignal(true); }).catch(function(e){ text(E.iqAccountStatus,'🔴 '+e.message); }).then(function(){ if(E.iq2faBtn)E.iq2faBtn.disabled=false; }); } function logoutIQ(){ post('/iq-logout',{}).catch(function(){}).then(function(){ safeStoreDel('mega_iq_session_token'); show2FA(false); if(E.iq2faCode)E.iq2faCode.value=''; text(E.iqAccountStatus,'● Desconectado'); updateMarketNote(); }); } function checkResult(){ if(resultBusy)return;if(!pendingTrade){try{var p=safeStoreGet('mega_pending_trade');if(p)pendingTrade=JSON.parse(p);}catch(e){}} if(!pendingTrade||!pendingTrade.expiry_time||Date.now()<new Date(pendingTrade.expiry_time).getTime())return; resultBusy=true;var t=pendingTrade; var u='/result?market='+encode(t.market)+'&symbol='+encode(t.symbol)+'&interval='+encode(t.interval)+'&direction='+encode(t.direction)+'&expiry_time='+encode(t.expiry_time); get(u).then(function(x){if(x&&x.result){text(E.result,x.result);registerResult(t,x);var k=t.symbol+'|'+t.direction+'|'+t.expiry_time;if(k!==lastResultKey){lastResultKey=k;speak('Operação finalizada. Resultado '+x.result+'.');}pendingTrade=null;safeStoreDel('mega_pending_trade');loadPerformance();}}) .catch(function(){}).then(function(){resultBusy=false;}); } function countdown(){ var c=currentSignal;if(!c||c.direction==='NEUTRO'||!c.entry_time){text(E.countdown,'Sem entrada confirmada');text(E.expiryCountdown,'⏱ EXPIRAÇÃO: --:--');return;} var n=Math.ceil((new Date(c.entry_time).getTime()-Date.now())/1000),xt=c.expiry_time?new Date(c.expiry_time).getTime():0; text(E.countdown,n>0?'Entrada em '+n+'s':'Entrada liberada'); if(n>0)text(E.expiryCountdown,'⏱ EXPIRAÇÃO: aguardando entrada');else if(xt){var rem=Math.max(0,Math.ceil((xt-Date.now())/1000));text(E.expiryCountdown,'⏱ EXPIRAÇÃO: '+String(Math.floor(rem/60)).padStart(2,'0')+':'+String(rem%60).padStart(2,'0'));} } function restoreSelections(){ var m=safeStoreGet('mega_market');if(m==='OPEN'||m==='OTC')E.market.value=m; fillSymbols();var s=safeStoreGet('mega_symbol');if(s&&symbols.indexOf(s)>=0)E.symbol.value=s; var it=safeStoreGet('mega_interval');if(it&&['1min','5min','15min','30min'].indexOf(it)>=0)E.interval.value=it; } function bindEvents(){ if(E.market)E.market.onchange=handleMarketChange;if(E.symbol)E.symbol.onchange=handleSymbolChange;if(E.interval)E.interval.onchange=handleIntervalChange; if(E.voiceBtn)E.voiceBtn.onclick=function(){voice();return false;}; if(E.tabMain)E.tabMain.onclick=function(){showTab('main');return false;};if(E.tabChart)E.tabChart.onclick=function(){showTab('chart');return false;};if(E.tabAccount)E.tabAccount.onclick=function(){showTab('account');return false;}; if(E.iqConnectBtn)E.iqConnectBtn.onclick=connectIQ;if(E.iq2faBtn)E.iq2faBtn.onclick=submitIQ2FA;if(E.iqLogoutBtn)E.iqLogoutBtn.onclick=logoutIQ; } function boot(){ bindElements(); if(!E.market||!E.symbol||!E.interval){document.documentElement.setAttribute('data-mega-js','missing-elements');return;} loadStats();restoreSelections();bindEvents();paintStats();showTab('main'); try{var p=safeStoreGet('mega_pending_trade');if(p)pendingTrade=JSON.parse(p);}catch(e){} document.documentElement.setAttribute('data-mega-js','ready');document.documentElement.setAttribute('data-mega-version',VERSION); text(E.status,'MONITORANDO'); updateMarketNote();refreshAccountStatus();updateClock();loadSignal(false);loadPerformance();loadRadar(); setInterval(function(){loadSignal(false);},5000);setInterval(updateClock,1000);setInterval(loadRadar,20000);setInterval(loadPerformance,30000);setInterval(checkResult,3000);setInterval(countdown,250);setInterval(function(){if(E.chartTab&&E.chartTab.classList.contains('active'))loadChart();},15000); } window.addEventListener('error',function(ev){try{console.error('[MEGA IA]',ev.message||ev.error);document.documentElement.setAttribute('data-mega-js','error');text(id('status'),'ERRO DE INTERFACE • RECARREGUE A PÁGINA');}catch(e){}}); if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',boot);else boot(); })(); </script> </body> </html> """

HTML_PAGE = HTML_PAGE.replace("__MEGA_IMAGE__", "/mega-ia.png")


@app.get("/version")
async def version_info():
    return JSONResponse(
        {"app": "MEGA IA", "version": "32.8.0", "js": "ready", "license": "disabled"},
        headers={"Cache-Control": "no-store"},
    )


@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def home():
    return HTMLResponse(
        HTML_PAGE,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
            "X-Mega-Panel-Version": "32.8.0",
        },
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))