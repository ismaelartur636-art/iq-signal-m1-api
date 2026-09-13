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

from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel
from fastapi.responses import HTMLResponse, FileResponse

app = FastAPI(title="MEGA IA", version="32.7.0")

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

LICENSE = os.getenv("LICENSE_EXPIRES", "2026-12-31")
WA1 = os.getenv("WHATSAPP_1", "55 84 99841-1282")
WA2 = os.getenv("WHATSAPP_2", "55 84 99449-9442")
IG = os.getenv("INSTAGRAM", "@Ismaelartur26")

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
    if client is None:
        return False
    try:
        return bool(client.check_connect())
    except Exception:
        return False


def _iq_dispose_client(state):
    client = state.get("client")
    state["client"] = None
    if client is None:
        return
    try:
        api = getattr(client, "api", None)
        if api is not None:
            try:
                api.close()
            except Exception:
                pass
    except Exception:
        pass
    time.sleep(0.15)


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
    return client


def _iq_lowlevel_connect_worker(client, result_box):
    """
    Conecta somente a camada HTTP/WebSocket necessária para leitura de candles.

    Motivo:
    algumas versões antigas de iqoptionapi ficam presas dentro de
    stable_api.IQ_Option.connect() esperando global_value.balance_id sem timeout.
    Para este painel não precisamos de saldo/ordens; precisamos apenas do websocket
    e dos candles OTC.
    """
    try:
        from iqoptionapi.api import IQOptionAPI

        api = IQOptionAPI("iqoption.com", client.email, client.password)
        api.set_session(
            headers=getattr(client, "SESSION_HEADER", {}),
            cookies=getattr(client, "SESSION_COOKIE", {}),
        )

        client.api = api
        check, reason = api.connect()

        result_box["check"] = bool(check)
        result_box["reason"] = reason

    except Exception as exc:
        result_box["error"] = repr(exc)


def iq_connect_blocking(state, force=False):
    if IQ_Option is None:
        raise RuntimeError("Biblioteca IQ Option não carregada no servidor.")

    now_ts = time.time()

    if not force and _iq_is_connected(state):
        return state.get("client")

    reconnect_after = float(state.get("reconnect_after", 0.0) or 0.0)
    if now_ts < reconnect_after:
        wait = max(1, int(reconnect_after - now_ts + 0.999))
        raise RuntimeError(f"IQ Option reconectando. Nova tentativa em {wait}s.")

    if force:
        _iq_dispose_client(state)

    email = state.get("email", "")
    password = state.get("password", "")

    if not email or not password:
        raise RuntimeError("Sessão IQ Option sem credenciais ativas. Conecte novamente.")

    last_attempt = float(state.get("last_attempt", 0.0) or 0.0)
    if now_ts - last_attempt < 2.5:
        raise RuntimeError("IQ Option reconectando. Aguarde alguns segundos.")

    state["last_attempt"] = now_ts
    state["reconnecting"] = True
    state["login_stage"] = "CRIANDO CLIENTE"

    try:
        client = IQ_Option(email, password)
        state["client"] = client

        # Não usamos client.connect() aqui. Em versões antigas da biblioteca,
        # ele pode ficar preso para sempre esperando balance_id.
        state["login_stage"] = "ABRINDO HTTP/WEBSOCKET"

        result_box = {}
        worker = threading.Thread(
            target=_iq_lowlevel_connect_worker,
            args=(client, result_box),
            daemon=True,
        )
        worker.start()
        worker.join(timeout=22.0)

        if worker.is_alive():
            try:
                api = getattr(client, "api", None)
                if api is not None:
                    api.close()
            except Exception:
                pass

            delay = _iq_mark_failure(
                state,
                "A camada HTTP/WebSocket da IQ Option não respondeu em 22 segundos."
            )
            raise RuntimeError(
                f"A IQ Option não respondeu ao servidor. Nova tentativa em {int(delay)}s."
            )

        if "error" in result_box:
            delay = _iq_mark_failure(state, result_box["error"])
            raise RuntimeError(
                f"Erro ao abrir conexão IQ Option. Nova tentativa em {int(delay)}s."
            )

        ok = bool(result_box.get("check"))
        reason = result_box.get("reason")

        if not ok:
            reason_text = str(reason or "").strip()
            low = reason_text.lower()

            if reason_text == "2FA" or "2fa" in low:
                _iq_dispose_client(state)
                state["last_error"] = "Conta com autenticação em duas etapas (2FA)."
                state["reconnect_after"] = 0.0
                raise RuntimeError(
                    "Sua conta IQ Option está pedindo 2FA. Desative temporariamente o 2FA para este login."
                )

            if "invalid_credentials" in low or "wrong credentials" in low:
                _iq_dispose_client(state)
                state["last_error"] = "Credenciais inválidas."
                state["reconnect_after"] = 0.0
                raise RuntimeError("E-mail ou senha da IQ Option estão incorretos.")

            delay = _iq_mark_failure(
                state,
                f"Falha no login IQ Option: {reason_text or 'sem motivo retornado'}"
            )
            raise RuntimeError(
                f"A IQ Option recusou a conexão: {reason_text or 'sem motivo retornado'}. "
                f"Nova tentativa em {int(delay)}s."
            )

        state["login_stage"] = "VALIDANDO WEBSOCKET"

        ready = False
        for _ in range(24):
            try:
                if bool(client.check_connect()):
                    ready = True
                    break
            except Exception:
                pass
            time.sleep(0.25)

        if not ready:
            delay = _iq_mark_failure(
                state,
                "Login HTTP respondeu, mas o websocket não ficou conectado."
            )
            raise RuntimeError(
                f"Login aceito, mas o websocket da IQ Option não ficou ativo. "
                f"Nova tentativa em {int(delay)}s."
            )

        state["login_stage"] = "CONECTADO"
        return _iq_mark_connected(state, client)

    except RuntimeError:
        raise
    except Exception as exc:
        delay = _iq_mark_failure(state, f"Erro ao abrir conexão IQ Option: {exc}")
        raise RuntimeError(
            f"Falha ao conectar à IQ Option. Nova tentativa em {int(delay)}s."
        )
    finally:
        state["reconnecting"] = False


def _iq_get_candles_once(client, active, duration, count, endtime):
    try:
        import iqoptionapi.constants as iq_constants
    except Exception as exc:
        raise RuntimeError(f"Constantes IQ Option indisponíveis: {exc}")

    active_id = iq_constants.ACTIVES.get(active)
    if active_id is None:
        raise RuntimeError(f"Ativo OTC não reconhecido pela biblioteca: {active}")

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
                raise RuntimeError("Websocket IQ Option desconectou durante a leitura de candles.")
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Falha ao verificar websocket IQ Option: {exc}")
        time.sleep(0.05)

    raise TimeoutError(f"IQ Option não respondeu candles em {IQ_CANDLE_TIMEOUT:.0f}s.")


def iq_candles_blocking(state, symbol, interval, n):
    duration = iq_seconds(interval)
    candidates = iq_active_candidates(symbol)
    errors = []
    client = iq_connect_blocking(state, force=False)

    for name in candidates:
        try:
            if not _iq_is_connected(state):
                raise RuntimeError("sessão desconectada antes de get_candles")

            chunk = _iq_get_candles_once(client, name, duration, n, time.time())

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
            msg = str(exc) or exc.__class__.__name__
            errors.append(f"{name}: {msg}")
            low = msg.lower()
            if any(k in low for k in ("reconnect", "disconnect", "is_ssl", "websocket", "closed", "timeout", "timed out", "sock")):
                delay = _iq_mark_failure(state, msg)
                raise RuntimeError(f"Conexão OTC caiu. Reconectando em {int(delay)}s.")

    detail = " | ".join(errors[-3:])
    delay = _iq_mark_failure(state, f"OTC sem candles. {detail}")
    raise RuntimeError(f"OTC sem candles. Nova tentativa em {int(delay)}s.")


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
                    asyncio.to_thread(iq_candles_blocking, iq_state, symbol, interval, fetch_n),
                    timeout=15,
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
        "version": "32.7.0",
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
        "id": "/mega-ia-trader-v34",
        "name": "Mega IA Trader",
        "short_name": "Mega IA",
        "description": "Mega IA Trader",
        "start_url": "/?pwa=v34",
        "scope": "/",
        "display": "standalone",
        "orientation": "portrait",
        "background_color": "#02050b",
        "theme_color": "#07182b",
        "icons": [
            {"src": "/mega-ia-icon-192.png?v=34", "sizes": "192x192", "type": "image/png", "purpose": "any"},
            {"src": "/mega-ia-icon.png?v=34", "sizes": "512x512", "type": "image/png", "purpose": "any"},
            {"src": "/mega-ia-icon.png?v=34", "sizes": "512x512", "type": "image/png", "purpose": "maskable"},
        ],
    }
    return Response(
        content=json.dumps(manifest_data, ensure_ascii=False),
        media_type="application/manifest+json",
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


@app.post("/iq-login")
async def iq_login(body: IQLoginBody, response: Response):
    email = body.email.strip()
    password = body.password

    if not email or not password:
        raise HTTPException(400, "Informe e-mail e senha da IQ Option.")

    if IQ_Option is None:
        raise HTTPException(503, "Biblioteca IQ Option não carregada. Verifique o requirements.txt.")

    token = secrets.token_urlsafe(32)

    state = {
        "session_id": token,
        "email": email,
        "password": password,
        "client": None,
        "lock": asyncio.Lock(),
        "last_error": "",
        "last_attempt": 0.0,
        "last_seen": time.time(),
        "last_connected": time.time(),
        "failure_count": 0,
        "reconnect_after": 0.0,
        "reconnecting": False,
        "generation": 0,
        "candle_cache": {},
        "results": {},
    }

    try:
        async with state["lock"]:
            client = await asyncio.wait_for(asyncio.to_thread(iq_connect_blocking, state), timeout=28)

        if not bool(client.check_connect()):
            raise RuntimeError("A sessão não permaneceu conectada.")

    except asyncio.TimeoutError:
        state["password"] = ""
        raise HTTPException(
            504,
            "O servidor encerrou a tentativa porque a biblioteca IQ Option ficou presa no login."
        )
    except Exception as exc:
        state["password"] = ""
        raise HTTPException(401, f"Não foi possível conectar à IQ Option: {str(exc)[:250]}")

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
        "message": "IQ Option OTC conectada.",
        "email_masked": _mask_email(email),
        "session_token": token,
    }


@app.post("/iq-logout")
async def iq_logout(request: Request, response: Response):
    header_token = request.headers.get("X-IQ-Session", "")
    cookie_token = request.cookies.get(IQ_SESSION_COOKIE, "")
    token = header_token if header_token in iq_sessions else cookie_token
    state = iq_sessions.pop(token, None)

    if state:
        state["password"] = ""
        _iq_dispose_client(state)
        state["candle_cache"] = {}

    response.delete_cookie(IQ_SESSION_COOKIE, path="/")

    return {"connected": False, "message": "Conta IQ Option desconectada."}


@app.get("/otc-status")
async def otc_status(request: Request):
    if IQ_Option is None:
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
                await asyncio.wait_for(
                    asyncio.to_thread(iq_connect_blocking, state, False),
                    timeout=18,
                )
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
async def candles_endpoint(
    request: Request,
    symbol: str = "EUR/USD",
    interval: str = "1min",
    n: int = 80,
    market: str = "OPEN",
):
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
async def result(
    request: Request,
    symbol="EUR/USD",
    interval="1min",
    direction="CALL",
    expiry_time="",
    market="OPEN",
):
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


HTML_PAGE = r"""
<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Mega IA Trader</title>
<link rel="manifest" href="/manifest.webmanifest?v=34">
<link rel="icon" type="image/png" sizes="512x512" href="/mega-ia-icon.png?v=34">
<link rel="apple-touch-icon" sizes="192x192" href="/mega-ia-icon-192.png?v=34">
<meta name="theme-color" content="#07182b">
<meta name="application-name" content="Mega IA Trader">
<meta name="apple-mobile-web-app-title" content="Mega IA Trader">
<meta name="apple-mobile-web-app-capable" content="yes">

<style>
body{margin:0;background:radial-gradient(circle at 50% 0,#07182b 0,#030812 42%,#02050b 100%);color:#eef5ff;font-family:Arial,sans-serif}
.wrap{max-width:1150px;margin:auto;padding:18px}
.brand{font-size:42px;font-weight:900;letter-spacing:1px;margin:8px 0 2px}
.brand span{color:#14c8ff}
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

@media(max-width:720px){
  .wrap{padding:10px}
  .grid{grid-template-columns:1fr 1fr}
  .signal{grid-column:span 2}
  .radar{grid-template-columns:1fr 1fr}
  .hero img{height:250px}
  .brand{font-size:36px}
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
  <div class="brand">🤖 MEGA <span>IA</span></div>
  <div class="subtitle">ANÁLISE EM TEMPO REAL • HORÁRIO DE BRASÍLIA</div>
  <div id="clock" style="font-size:22px;margin-top:4px"></div>

  <div class="controls">
    <select id="market">
      <option value="OPEN">🌐 Mercado Aberto</option>
      <option value="OTC">🟣 IQ Option OTC</option>
    </select>

    <select id="symbol"></select>

    <select id="interval">
      <option>1min</option>
      <option>5min</option>
      <option>15min</option>
      <option>30min</option>
    </select>

    <button id="voiceBtn" onclick="voice()">🔊 Ativar voz</button>
    <div id="otcNote" class="label" style="margin-top:6px;display:none">🟣 IQ Option OTC: verificando conexão...</div>
  </div>

  <div class="tabs">
    <button class="tabbtn active" id="tabMain">📊 Painel</button>
    <button class="tabbtn" id="tabChart">📈 Gráfico</button>
    <button class="tabbtn" id="tabAccount">⚙️ Conta IQ Option</button>
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

      <div class="card">
        <div class="label">ENTRADA</div>
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
      <div class="card">
        <div class="label">WIN</div>
        <div id="wins" class="big call">0</div>
      </div>
      <div class="card">
        <div class="label">LOSS</div>
        <div id="losses" class="big put">0</div>
      </div>
      <div class="card">
        <div class="label">ASSERTIVIDADE</div>
        <div id="accuracy" class="big">0%</div>
      </div>
      <div class="card">
        <div class="label">RESULTADO</div>
        <div id="result" class="big">--</div>
      </div>
    </div>
  </div>

  <div id="chartTab" class="tab">
    <div class="card">
      <div class="chartmeta">
        <b>📈 Gráfico espelhado</b>
        <span class="chartbadge" id="chartInfo">--</span>
      </div>
      <div class="chartbox"><canvas id="priceChart"></canvas></div>
      <div class="label" style="margin-top:8px">
        O gráfico acompanha o mesmo mercado, par e período selecionados no painel.
      </div>
    </div>
  </div>

  <div id="accountTab" class="tab">
    <div class="card">
      <h2 style="margin-top:0">⚙️ Conta IQ Option</h2>
      <div class="label">
        Use sua própria conta para liberar o gráfico e os candles OTC.
      </div>

      <div class="account-grid">
        <label>E-mail
          <input id="iqEmail" type="email" autocomplete="username" placeholder="Seu e-mail da IQ Option">
        </label>
        <label>Senha
          <input id="iqPassword" type="password" autocomplete="current-password" placeholder="Sua senha">
        </label>
      </div>

      <div class="account-actions">
        <button id="iqConnectBtn">🟢 Conectar</button>
        <button id="iqLogoutBtn">🔴 Desconectar</button>
      </div>

      <div id="iqAccountStatus" class="card" style="margin-top:12px">● Desconectado</div>
    </div>
  </div>

  <div class="card">
    <b>Radar de oportunidades</b>
    <div id="radar" class="radar"></div>
  </div>

  <div id="licenseCard" class="card" style="display:none">
    <div class="label">RENOVAÇÃO</div>
    <div id="license"></div>
  </div>
</div>

<script>
const market=document.getElementById('market');
const interval=document.getElementById('interval');
const voiceBtn=document.getElementById('voiceBtn');
const otcNote=document.getElementById('otcNote');
const heroBox=document.getElementById('heroBox');
const entryArrow=document.getElementById('entryArrow');
const entryArrowIcon=document.getElementById('entryArrowIcon');
const entryArrowLabel=document.getElementById('entryArrowLabel');
const analysisText=document.getElementById('analysisText');
const mainTab=document.getElementById('mainTab');
const chartTab=document.getElementById('chartTab');
const accountTab=document.getElementById('accountTab');
const tabMain=document.getElementById('tabMain');
const tabChart=document.getElementById('tabChart');
const tabAccount=document.getElementById('tabAccount');
const iqEmail=document.getElementById('iqEmail');
const iqPassword=document.getElementById('iqPassword');
const iqConnectBtn=document.getElementById('iqConnectBtn');
const iqLogoutBtn=document.getElementById('iqLogoutBtn');
const iqAccountStatus=document.getElementById('iqAccountStatus');
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
let resultBusy=false;
let pendingTrade=null;

try{
  const savedPending=localStorage.getItem('mega_pending_trade');
  if(savedPending){
    pendingTrade=JSON.parse(savedPending);
  }
}catch(e){
  pendingTrade=null;
}

function savePendingTrade(){
  try{
    if(pendingTrade){
      localStorage.setItem('mega_pending_trade',JSON.stringify(pendingTrade));
    }else{
      localStorage.removeItem('mega_pending_trade');
    }
  }catch(e){}
}

function rememberPendingTrade(sig){
  if(!sig) return;
  if(sig.direction!=='CALL' && sig.direction!=='PUT') return;
  if(!sig.expiry_time || !sig.entry_time) return;

  // Não troca uma operação ainda aguardando resultado por outro polling do sinal.
  if(pendingTrade && pendingTrade.expiry_time){
    const oldExpiry=new Date(pendingTrade.expiry_time).getTime();
    if(oldExpiry && Date.now() < oldExpiry + 120000){
      return;
    }
  }

  pendingTrade={
    market:sig.market || market.value,
    symbol:sig.symbol,
    interval:sig.interval,
    direction:sig.direction,
    entry_time:sig.entry_time,
    expiry_time:sig.expiry_time
  };

  savePendingTrade();
}

function fillSymbols(){
  const otc=market.value==='OTC';
  const previous=S.value;
  S.innerHTML='';

  syms.forEach(x=>{
    S.add(new Option(otc?'🟣 '+x+' • OTC':x,x));
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

  speechSynthesis.cancel();

  const u=new SpeechSynthesisUtterance(t);
  u.lang='pt-BR';
  u.rate=.94;
  u.pitch=.90;
  u.volume=1;

  const mv=pickMalePtBRVoice();
  if(mv) u.voice=mv;

  speechSynthesis.speak(u);
}

function voice(){
  voiceEnabled=true;
  voiceBtn.textContent='🔊 Voz ativada';
  speak('Voz da Mega IA ativada.');
  setTimeout(()=>sig(true),650);
}

function ft(x){
  return x ? new Date(x).toLocaleTimeString('pt-BR',{hour12:false}) : '--:--:--';
}

function iqSessionToken(){
  try{
    return localStorage.getItem('mega_iq_session_token')||'';
  }catch(_){
    return '';
  }
}

function authHeaders(extra={}){
  const h={...extra};
  const t=iqSessionToken();
  if(t) h['X-IQ-Session']=t;
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
  const otc=market.value==='OTC';
  otcNote.style.display=otc?'block':'none';

  if(!otc) return;

  otcNote.textContent='🟣 IQ Option OTC: verificando conexão...';

  try{
    const x=await Promise.race([
      get('/otc-status'),
      new Promise((_,rej)=>setTimeout(()=>rej(Error('tempo limite de conexão')),16000))
    ]);

    otcNote.textContent=(x.connected?'🟢 ':x.reconnecting?'🟡 ':'🔴 ')+x.message;
  }catch(e){
    otcNote.textContent='🔴 IQ Option OTC: '+(e.message||'falha de conexão');
  }
}

function resizeChart(){
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

  let lo=Math.min(...a.map(c=>Number(c.low)));
  let hi=Math.max(...a.map(c=>Number(c.high)));
  const extra=(hi-lo)*.08||1;

  lo-=extra;
  hi+=extra;

  const px=i=>pad.l+(i/(a.length-1||1))*cw;
  const py=v=>pad.t+(hi-v)/(hi-lo)*ch;

  chartCtx.strokeStyle='#19304a';
  chartCtx.lineWidth=1;
  chartCtx.font='11px Arial';
  chartCtx.fillStyle='#8190a8';

  for(let j=0;j<5;j++){
    const y=pad.t+j*ch/4;
    chartCtx.beginPath();
    chartCtx.moveTo(pad.l,y);
    chartCtx.lineTo(w-pad.r,y);
    chartCtx.stroke();

    const v=hi-(hi-lo)*j/4;
    chartCtx.fillText(v.toFixed(5),4,y+4);
  }

  const step=Math.max(2,cw/a.length*.72);

  a.forEach((c,i)=>{
    const x=px(i);
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

    chartCtx.fillRect(x-step/2,top,step,Math.max(1,bot-top));

    if(i%Math.ceil(a.length/6)===0){
      chartCtx.fillStyle='#8190a8';
      chartCtx.fillText(
        new Date(c.datetime).toLocaleTimeString('pt-BR',{hour:'2-digit',minute:'2-digit'}),
        x-18,
        h-7
      );
    }
  });

  if(cur && cur.direction && cur.direction!=='NEUTRO' && cur.reference_candle){
    const idx=a.findIndex(c=>c.datetime===cur.reference_candle);

    if(idx>=0){
      const x=px(idx);
      const v=cur.direction==='CALL'?Number(a[idx].low):Number(a[idx].high);

      chartCtx.fillStyle=cur.direction==='CALL'?'#45ff9b':'#ff5c7a';
      chartCtx.beginPath();
      chartCtx.arc(x,py(v),6,0,Math.PI*2);
      chartCtx.fill();

      chartCtx.font='bold 12px Arial';
      chartCtx.fillText(cur.direction,x+8,py(v)-8);
    }
  }
}

async function loadChart(){
  if(chartBusy) return;

  chartBusy=true;

  try{
    const d=await get(
      `/candles?market=${encodeURIComponent(market.value)}&symbol=${encodeURIComponent(S.value)}&interval=${encodeURIComponent(interval.value)}&n=80`
    );

    if((d.candles||[]).length){
      chartData=d.candles||[];
    }

    chartInfo.textContent=
      (d.ok===false?'⚠️ ':'')+
      (d.market==='OTC'?'🟣 '+d.symbol+' • OTC':d.symbol)+
      ' • '+d.interval+
      (d.ok===false?' • '+(d.status||'INDISPONÍVEL'):'');

    drawChart(chartData);

  }catch(e){
    chartInfo.textContent='⚠️ Dados temporariamente indisponíveis';
    if(!chartData.length) drawChart([]);
  }finally{
    chartBusy=false;
  }
}

function showTab(which){
  const main=which==='main';
  const chart=which==='chart';
  const account=which==='account';

  mainTab.classList.toggle('active',main);
  chartTab.classList.toggle('active',chart);
  accountTab.classList.toggle('active',account);

  tabMain.classList.toggle('active',main);
  tabChart.classList.toggle('active',chart);
  tabAccount.classList.toggle('active',account);

  if(chart){
    loadChart();
    setTimeout(resizeChart,50);
  }

  if(account){
    refreshAccountStatus();
  }
}

tabMain.onclick=()=>showTab('main');
tabChart.onclick=()=>showTab('chart');
tabAccount.onclick=()=>showTab('account');

window.addEventListener('resize',resizeChart);

async function refreshAccountStatus(){
  try{
    const x=await get('/otc-status');
    const rr=!!x.reconnecting;

    iqAccountStatus.textContent=
      (x.connected&&!rr?'🟢 ':rr?'🟡 ':'🔴 ')+
      (x.connected&&!rr?('Conectado: '+(x.email_masked||'')):x.message);

    iqConnectBtn.disabled=!!x.connected||rr;
    iqLogoutBtn.disabled=!x.connected&&!rr;

    if(rr) setTimeout(refreshAccountStatus,2500);

  }catch(e){
    iqAccountStatus.textContent='🟡 Reconectando à IQ Option...';
    setTimeout(refreshAccountStatus,2500);
  }
}

iqConnectBtn.onclick=async()=>{
  const email=iqEmail.value.trim();
  const password=iqPassword.value;

  if(!email||!password){
    iqAccountStatus.textContent='🔴 Informe e-mail e senha.';
    return;
  }

  iqAccountStatus.textContent='🟡 Conectando...';
  iqConnectBtn.disabled=true;

  try{
    const x=await post('/iq-login',{email,password});

    if(x.session_token){
      try{
        localStorage.setItem('mega_iq_session_token',x.session_token);
      }catch(_){}
    }

    iqPassword.value='';
    iqEmail.value='';
    iqAccountStatus.textContent='🟢 Conectado: '+(x.email_masked||'');

    await updateMarketNote();

    if(market.value==='OTC'){
      sig(true);
      rad();
      if(chartTab.classList.contains('active')) loadChart();
    }

  }catch(e){
    iqAccountStatus.textContent='🔴 '+e.message;
  }finally{
    iqConnectBtn.disabled=false;
    refreshAccountStatus();
  }
};

iqLogoutBtn.onclick=async()=>{
  try{
    await post('/iq-logout',{});

    try{
      localStorage.removeItem('mega_iq_session_token');
    }catch(_){}

    iqAccountStatus.textContent='● Desconectado';
    otcNote.textContent='🔴 Conecte sua conta na aba Conta IQ Option.';
    chartData=[];
    drawChart([]);

  }catch(e){
    iqAccountStatus.textContent='🔴 '+e.message;
  }

  refreshAccountStatus();
};

async function sig(announce=false){
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
      `/signal-ai?market=${encodeURIComponent(market.value)}&symbol=${encodeURIComponent(S.value)}&interval=${encodeURIComponent(interval.value)}`
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

    fifteen=false;
    five=false;
    entered=false;

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
    const p=await get('/performance?market='+encodeURIComponent(market.value));
    wins.textContent=p.wins;
    losses.textContent=p.losses;
    accuracy.textContent=p.accuracy+'%';
  }catch(e){
  }finally{
    perfBusy=false;
  }
}

async function rad(){
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

  resultBusy=true;

  try{
    const t=pendingTrade;

    const x=await get(
      `/result?market=${encodeURIComponent(t.market || market.value)}&symbol=${encodeURIComponent(t.symbol)}&interval=${encodeURIComponent(t.interval)}&direction=${encodeURIComponent(t.direction)}&expiry_time=${encodeURIComponent(t.expiry_time)}`
    );

    if(x.result){
      result.textContent=x.result;

      const k=t.symbol+'|'+t.direction+'|'+t.expiry_time;

      if(k!==reskey){
        reskey=k;

        if(voiceEnabled){
          speak('Operação finalizada. Resultado '+x.result+'.');
        }
      }

      await perf();

      // Só remove depois que o servidor realmente devolveu o resultado.
      pendingTrade=null;
      savePendingTrade();
    }

  }catch(e){
    // Mantém pendente e tenta novamente no próximo ciclo.
  }finally{
    resultBusy=false;
  }
}

market.onchange=async()=>{
  try{localStorage.setItem('mega_market',market.value)}catch(_){}

  fillSymbols();

  try{
    const savedSym=localStorage.getItem('mega_symbol');
    if(savedSym && [...S.options].some(o=>o.value===savedSym)){
      S.value=savedSym;
    }
  }catch(_){}

  await updateMarketNote();

  lastSignalVoice='';
  chartData=[];

  sig(true);
  rad();

  if(chartTab.classList.contains('active')){
    loadChart();
  }
};

S.onchange=()=>{
  try{localStorage.setItem('mega_symbol',S.value)}catch(_){}

  lastSignalVoice='';
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
  chartData=[];

  sig(true);
  rad();

  if(chartTab.classList.contains('active')){
    loadChart();
  }
};

try{
  const sm=localStorage.getItem('mega_market');
  if(sm==='OPEN'||sm==='OTC'){
    market.value=sm;
  }

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

  if(market.value==='OTC'){
    await new Promise(r=>setTimeout(r,700));
  }

  safe('signal',()=>sig(false));
  safe('performance',perf);
  safe('radar',rad);

  if(chartTab.classList.contains('active')){
    safe('chart',loadChart);
  }
}

bootApp().catch(err=>{
  console.error('[MEGA IA] boot',err);
  statusBox.textContent='PAINEL INICIADO COM AVISO';
});

setInterval(()=>sig(false),5000);
setInterval(()=>{
  if(chartTab.classList.contains('active')){
    loadChart();
  }
},15000);

setInterval(perf,30000);
setInterval(rad,20000);
setInterval(resultCheck,3000);
setInterval(clk,1000);
setInterval(cd,250);
</script>
</body>
</html>
"""

HTML_PAGE = HTML_PAGE.replace("__MEGA_IMAGE__", "/mega-ia.png")


@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def home():
    return HTMLResponse(HTML_PAGE)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
