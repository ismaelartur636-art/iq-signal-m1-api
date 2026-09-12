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

try:
    from iqoptionapi.stable_api import IQ_Option
except Exception:
    IQ_Option = None
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel
from fastapi.responses import HTMLResponse, FileResponse

app = FastAPI(title="MEGA IA", version="25.0.0")

IMAGE_PATH = os.path.join(os.path.dirname(__file__), "mega_ia.png")
ICON_512_PATH = os.path.join(os.path.dirname(__file__), "mega_ia_icon.png")
ICON_192_PATH = os.path.join(os.path.dirname(__file__), "mega_ia_icon_192.png")


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
td_sem = asyncio.Semaphore(3)

IQ_SESSION_COOKIE = "mega_iq_session"
IQ_SESSION_TTL = int(os.getenv("IQ_SESSION_TTL", "43200"))  # 12 horas
iq_sessions: Dict[str, Dict[str, Any]] = {}

class IQLoginBody(BaseModel):
    email: str
    password: str

def _cleanup_iq_sessions():
    now_ts = time.time()
    expired = [k for k,v in iq_sessions.items() if now_ts - v.get("last_seen", now_ts) > IQ_SESSION_TTL]
    for token in expired:
        state = iq_sessions.pop(token, None)
        if state:
            state["password"] = ""
            state["client"] = None

def _mask_email(email: str):
    if "@" not in email:
        return "***"
    name, domain = email.split("@", 1)
    visible = name[:1] if name else ""
    return visible + "•••••@" + domain

def _session_state(request: Request, required: bool = False):
    _cleanup_iq_sessions()
    # Em refresh o navegador pode manter um cookie antigo enquanto o token salvo
    # no app já aponta para a sessão atual. Teste os dois separadamente.
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


def now():
    return datetime.now(BR_TZ)


def iso(d: datetime):
    return d.astimezone(BR_TZ).isoformat()


def parse_dt(value: str):
    d = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return (d if d.tzinfo else d.replace(tzinfo=UTC)).astimezone(BR_TZ)


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


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
        call_score += 5; call_reasons.append("rompimento abaixo da banda inferior")
    if put_break:
        put_score += 5; put_reasons.append("rompimento acima da banda superior")
    if st["k"] <= 20 and st["d"] <= 20:
        call_score += 2; call_reasons.append("Estocástico em sobrevenda")
    if st["k"] >= 80 and st["d"] >= 80:
        put_score += 2; put_reasons.append("Estocástico em sobrecompra")
    if st["cross_up"]:
        call_score += 2; call_reasons.append("cruzamento para cima")
    if st["cross_down"]:
        put_score += 2; put_reasons.append("cruzamento para baixo")
    if sideways:
        call_score += 1; put_score += 1
    else:
        call_score -= 1; put_score -= 1
    if wi["call_wick"] >= 0.25:
        call_score += 1; call_reasons.append("pavio inferior de rejeição")
    if wi["put_wick"] >= 0.25:
        put_score += 1; put_reasons.append("pavio superior de rejeição")

    if call_ok:
        conf = 65 + call_score * 3
        if not sideways: conf = min(conf, 72)
        return {"direction": "CALL", "confidence": round(clamp(conf, 65, 96), 1), "confirmed": True,
                "reason": "; ".join(call_reasons), "strategy": "Bollinger 20/2 + Estocástico 14,3,3",
                "sideways": sideways, "sideways_score": side_score,
                "stochastic": {"k": round(st["k"], 2), "d": round(st["d"], 2)}, "bollinger": bb}

    if put_ok:
        conf = 65 + put_score * 3
        if not sideways: conf = min(conf, 72)
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
    fast=ema_series(closes,12); slow=ema_series(closes,26)
    if len(fast)<10 or len(slow)<3:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,"reason":"MACD insuficiente.","strategy":"MACD momentum"}
    # Alinha as séries pelo final.
    m=[]
    common=min(len(fast),len(slow))
    for i in range(1, common+1):
        m.append(fast[-i]-slow[-i])
    m=list(reversed(m))
    sig=ema_series(m,9)
    if len(sig)<2 or len(m)<2:
        return {"direction":"NEUTRO","confidence":0,"confirmed":False,"reason":"MACD sem sinal.","strategy":"MACD momentum"}
    line_now,line_prev=m[-1],m[-2]; sig_now,sig_prev=sig[-1],sig[-2]
    e50=ema(closes,50); r=rsi(closes,14)
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
    e21=ema(closes,21); e50=ema(closes,50) if len(closes)>=50 else ema(closes,21)
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
    last=cs[-1]; prev=cs[-11:-1]
    hi=max(c["high"] for c in prev); lo=min(c["low"] for c in prev)
    body=abs(last["close"]-last["open"]); e20=ema(closes,20)
    call=last["close"]>hi and body>=0.45*a and last["close"]>e20
    put=last["close"]<lo and body>=0.45*a and last["close"]<e20
    if call:
        return {"direction":"CALL","confidence":79,"confirmed":True,"reason":"Rompimento de máxima recente com expansão de volatilidade.","strategy":"Breakout ATR"}
    if put:
        return {"direction":"PUT","confidence":79,"confirmed":True,"reason":"Rompimento de mínima recente com expansão de volatilidade.","strategy":"Breakout ATR"}
    return {"direction":"NEUTRO","confidence":44,"confirmed":False,"reason":"Sem breakout válido.","strategy":"Breakout ATR"}

def local_engine(cs):
    # Motor interno multiestratégia. As estratégias não são expostas no painel.
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
        # Confluência de 2+ métodos recebe pequeno bônus; um sinal isolado precisa ser forte.
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


async def candles_open(symbol, interval, n=80):
    if not TD_KEY:
        raise HTTPException(500, "TWELVE_DATA_API_KEY não configurada.")
    params = {"symbol": symbol, "interval": interval, "outputsize": n, "apikey": TD_KEY, "format": "JSON"}
    async with td_sem:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                response = await client.get(TD_URL, params=params)
                response.raise_for_status()
                data = response.json()
        except Exception as exc:
            raise HTTPException(502, f"Erro Twelve Data: {exc}")
    if data.get("status") == "error":
        raise HTTPException(502, data.get("message", "Erro Twelve Data."))
    out = []
    for x in reversed(data.get("values", [])):
        try:
            out.append({"datetime": x["datetime"], "open": float(x["open"]), "high": float(x["high"]),
                        "low": float(x["low"]), "close": float(x["close"]), "volume": float(x.get("volume", 0) or 0)})
        except Exception:
            pass
    if not out:
        raise HTTPException(502, "Nenhum candle recebido.")
    return out

def iq_active_candidates(symbol):
    base = symbol.replace("/", "").upper()
    preferred = OTC_BASE.get(symbol, f"{base}-OTC")
    # Nunca usa o ativo normal como fallback: em modo OTC só aceita nomes OTC.
    return list(dict.fromkeys([preferred, f"{base}-OTC", f"{base}_OTC"]))

def iq_seconds(interval):
    return INTERVALS[interval]

def iq_connect_blocking(state, force=False):
    if IQ_Option is None:
        raise RuntimeError("Biblioteca IQ Option não carregada no servidor.")

    now_ts = time.time()
    if force:
        state["client"] = None

    client = state.get("client")
    if client is not None:
        try:
            if bool(client.check_connect()):
                return client
        except Exception:
            pass
        state["client"] = None

    last_error = state.get("last_error", "")
    last_attempt = float(state.get("last_attempt", 0.0))
    if last_error and now_ts - last_attempt < 3:
        raise RuntimeError(last_error)

    email = state.get("email", "")
    password = state.get("password", "")
    if not email or not password:
        raise RuntimeError("Sessão IQ Option sem credenciais ativas. Conecte novamente.")

    state["last_attempt"] = now_ts
    client = IQ_Option(email, password)
    ok, reason = client.connect()
    if not ok:
        state["last_error"] = f"Falha ao conectar na IQ Option: {reason}"
        raise RuntimeError(state["last_error"])

    ready = False
    for _ in range(10):
        try:
            if bool(client.check_connect()):
                ready = True
                break
        except Exception:
            pass
        time.sleep(0.25)

    if not ready:
        state["last_error"] = "A IQ Option aceitou o login, mas o websocket OTC não permaneceu conectado."
        raise RuntimeError(state["last_error"])

    state["client"] = client
    state["last_error"] = ""
    state["last_seen"] = time.time()
    state["last_connected"] = time.time()
    return client

def iq_candles_blocking(state, symbol, interval, n):
    duration = iq_seconds(interval)
    candidates = iq_active_candidates(symbol)
    errors = []

    for round_no in range(2):
        client = iq_connect_blocking(state, force=(round_no > 0))
        for name in candidates:
            try:
                if not bool(client.check_connect()):
                    raise RuntimeError("sessão desconectada antes de get_candles")
                chunk = client.get_candles(name, duration, n, time.time())
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
                        state["last_error"] = ""
                        state["last_seen"] = time.time()
                        return out
                errors.append(f"{name}: sem candles")
            except Exception as exc:
                msg = str(exc) or exc.__class__.__name__
                errors.append(f"{name}: {msg}")
                if "reconnect" in msg.lower() or "disconnect" in msg.lower():
                    state["client"] = None
                    break
        time.sleep(0.35)

    state["client"] = None
    detail = " | ".join(errors[-4:])
    state["last_error"] = f"OTC conectado no login, mas sem candles. {detail}"
    raise RuntimeError(state["last_error"])


async def candles(symbol, interval, n=80, market="OPEN", iq_state=None):
    market=(market or "OPEN").upper()
    if market == "OTC":
        if symbol not in SYMBOLS or interval not in INTERVALS:
            raise HTTPException(400, "Ativo ou intervalo inválido.")
        if iq_state is None:
            raise HTTPException(401, "Conecte sua conta da IQ Option na aba Conta IQ Option.")
        ckey = f"{symbol}|{interval}|{n}"
        candle_cache = iq_state.setdefault("candle_cache", {})
        cached = candle_cache.get(ckey)
        if cached and time.time() - cached[0] < 5:
            return cached[1][-n:]
        try:
            async with iq_state["lock"]:
                cached = candle_cache.get(ckey)
                if cached and time.time() - cached[0] < 5:
                    return cached[1][-n:]
                out = await asyncio.wait_for(asyncio.to_thread(iq_candles_blocking, iq_state, symbol, interval, n), timeout=18)
                candle_cache[ckey] = (time.time(), out)
        except Exception as exc:
            raise HTTPException(503, f"IQ Option OTC indisponível: {str(exc)[:500]}")
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
    data = [{"time": c["datetime"], "o": c["open"], "h": c["high"], "l": c["low"], "c": c["close"], "v": c["volume"]} for c in cs[-40:]]
    prompt = f'''Você é o módulo de confirmação da MEGA IA. Ativo {symbol}, timeframe {interval}. Use SOMENTE candles fechados. Não invente dados futuros. Estratégias internas: reversão, tendência, momentum, price action e breakout com filtro de volatilidade. Análise técnica preliminar: {json.dumps(analysis, ensure_ascii=False)} Retorne SOMENTE JSON: {{"direction":"CALL|PUT|NEUTRO","confidence":0,"confirmed":true,"reason":"curto","risk":"LOW|MEDIUM|HIGH"}} Candles: {json.dumps(data, ensure_ascii=False)}'''
    try:
        async with httpx.AsyncClient(timeout=OAI_TIMEOUT) as client:
            response = await client.post(OAI_URL, headers={"Authorization": f"Bearer {OAI_KEY}", "Content-Type": "application/json"},
                                         json={"model": OAI_MODEL, "input": prompt})
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
        out = {"available": True, "direction": str(p.get("direction", "NEUTRO")).upper(),
               "confidence": clamp(float(p.get("confidence", 0)), 0, 100), "confirmed": bool(p.get("confirmed", False)),
               "risk": str(p.get("risk", "HIGH")).upper(), "reason": str(p.get("reason", ""))[:300]}
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


async def signal(symbol, interval, market="OPEN", iq_state=None):
    if symbol not in SYMBOLS or interval not in INTERVALS:
        raise HTTPException(400, "Ativo ou intervalo inválido.")
    market=(market or "OPEN").upper()
    session_part = iq_state.get("session_id", "") if (market == "OTC" and iq_state) else "PUBLIC"
    key = f"{session_part}|{market}|{symbol}|{interval}"
    if key in cache and time.time() - cache[key][0] < 4:
        return cache[key][1]

    raw = await candles(symbol, interval, 90, market, iq_state)
    closed = raw[:-1] if len(raw) > 1 else raw  # sem vela em formação = não repinta
    analysis = local_engine(closed)

    base = {"symbol": symbol, "interval": interval, "market": market, "direction": "NEUTRO", "confidence": analysis["confidence"],
            "entry_time": None, "announce_time": None, "expiry_time": None,
            "status": "MONITORANDO", "ai_confirmed": False, "risk": "HIGH", "strategy": analysis["strategy"],
            "reason": analysis["reason"], "non_repaint": True, "technical": analysis}

    if analysis["confirmed"]:
        ai = await openai_confirm(symbol, interval, closed, analysis)
        if not ai.get("available"):
            base.update(direction=analysis["direction"], confidence=analysis["confidence"], status="SINAL TÉCNICO", risk="MEDIUM")
        elif ai["direction"] == analysis["direction"] and ai["confirmed"] and ai["confidence"] >= OAI_MIN and ai["risk"] != "HIGH":
            base.update(direction=analysis["direction"],
                        confidence=round(clamp(analysis["confidence"] * .45 + ai["confidence"] * .55, 0, 97), 1),
                        status="SINAL LIBERADO", ai_confirmed=True, risk=ai["risk"], reason=ai.get("reason") or analysis["reason"])
        else:
            base.update(direction="NEUTRO", confidence=round(min(analysis["confidence"], ai["confidence"]), 1),
                        status="AGUARDANDO CONFIRMAÇÃO DA IA", risk=ai.get("risk", "HIGH"), reason=ai.get("reason") or "A IA não confirmou.")

    if base["direction"] in ("CALL", "PUT"):
        announce, entry, expiry = entry_window(interval)
        base["entry_time"] = iso(entry)
        base["announce_time"] = iso(announce)
        base["expiry_time"] = iso(expiry)
        base["reference_candle"] = closed[-1]["datetime"] if closed else None

    cache[key] = (time.time(), base)
    return base


@app.get("/health")
async def health():
    return {"status": "ok", "app": "MEGA IA", "version": "16.0.0", "brasilia_time": iso(now()),
            "twelve_data_configured": bool(TD_KEY), "iq_option_otc_library": bool(IQ_Option is not None), "openai_configured": bool(OAI_KEY), "openai_model_configured": bool(OAI_MODEL)}


@app.get("/server-time")
async def server_time():
    return {"datetime": iso(now()), "timezone": "America/Sao_Paulo"}


@app.get("/license")
async def license_info():
    # A data de validade fica somente no servidor e nunca é exibida ao usuário.
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
        "id": "/mega-ia-trader-v25",
        "name": "Mega IA Trader",
        "short_name": "Mega IA",
        "description": "Mega IA Trader",
        "start_url": "/?pwa=v25",
        "scope": "/",
        "display": "standalone",
        "orientation": "portrait",
        "background_color": "#02050b",
        "theme_color": "#07182b",
        "icons": [
            {
                "src": "/mega-ia-icon-192.png?v=25",
                "sizes": "192x192",
                "type": "image/png",
                "purpose": "any"
            },
            {
                "src": "/mega-ia-icon.png?v=25",
                "sizes": "512x512",
                "type": "image/png",
                "purpose": "any"
            },
            {
                "src": "/mega-ia-icon.png?v=25",
                "sizes": "512x512",
                "type": "image/png",
                "purpose": "maskable"
            }
        ]
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
        "candle_cache": {},
        "results": {},
    }
    try:
        async with state["lock"]:
            client = await asyncio.wait_for(asyncio.to_thread(iq_connect_blocking, state), timeout=18)
        if not bool(client.check_connect()):
            raise RuntimeError("A sessão não permaneceu conectada.")
    except Exception as exc:
        state["password"] = ""
        raise HTTPException(401, f"Não foi possível conectar à IQ Option: {str(exc)[:250]}")

    iq_sessions[token] = state
    response.set_cookie(
        IQ_SESSION_COOKIE, token, httponly=True, secure=True, samesite="lax",
        max_age=IQ_SESSION_TTL, path="/"
    )
    return {"connected": True, "message": "IQ Option OTC conectada.", "email_masked": _mask_email(email), "session_token": token}

@app.post("/iq-logout")
async def iq_logout(request: Request, response: Response):
    header_token = request.headers.get("X-IQ-Session", "")
    cookie_token = request.cookies.get(IQ_SESSION_COOKIE, "")
    token = header_token if header_token in iq_sessions else cookie_token
    state = iq_sessions.pop(token, None)
    if state:
        state["password"] = ""
        state["client"] = None
        state["candle_cache"] = {}
    response.delete_cookie(IQ_SESSION_COOKIE, path="/")
    return {"connected": False, "message": "Conta IQ Option desconectada."}

@app.get("/otc-status")
async def otc_status(request: Request):
    if IQ_Option is None:
        return {"configured": False, "connected": False, "message": "Biblioteca IQ Option não foi carregada."}
    state = _session_state(request, required=False)
    if not state:
        return {"configured": True, "connected": False, "message": "Conecte sua conta na aba Conta IQ Option."}
    # Ao atualizar a página, preserve a sessão já autenticada.
    # Só tenta reconectar se o websocket realmente caiu; o cookie não é descartado.
    client = state.get("client")
    try:
        connected = bool(client and getattr(client, "check_connect", lambda: False)())
    except Exception:
        connected = False
    if not connected:
        # Um refresh não é logout. A sessão continua válida e a reconexão é feita
        # usando a mesma sessão do usuário.
        try:
            async with state["lock"]:
                client = await asyncio.wait_for(asyncio.to_thread(iq_connect_blocking, state), timeout=15)
            connected = bool(getattr(client, "check_connect", lambda: False)())
        except Exception as exc:
            state["last_error"] = str(exc)[:300]
            recently_connected = time.time() - float(state.get("last_connected", 0.0)) < 45
            return {
                "configured": True, "connected": bool(recently_connected),
                "reconnecting": True,
                "message": "Sessão IQ Option mantida. Reconectando...",
                "email_masked": _mask_email(state.get("email", "")),
                "pairs": len(OTC_BASE)
            }
    return {
        "configured": True, "connected": connected,
        "message": "IQ Option OTC conectada." if connected else "Sessão preservada. Reconectando à IQ Option...",
        "email_masked": _mask_email(state.get("email", "")),
        "pairs": len(OTC_BASE)
    }


@app.get("/candles")
async def candles_endpoint(request: Request, symbol: str = "EUR/USD", interval: str = "1min", n: int = 80, market: str = "OPEN"):
    market=(market or "OPEN").upper()
    if symbol not in SYMBOLS or interval not in INTERVALS or market not in ("OPEN", "OTC"):
        raise HTTPException(400, "Ativo, intervalo ou mercado inválido.")
    n = max(20, min(int(n), 150))
    values = await candles(symbol, interval, n, market, _session_state(request, required=(market == "OTC")))
    return {"symbol": symbol, "interval": interval, "market": market, "candles": values}


@app.get("/signal-ai")
async def signal_ai(request: Request, symbol="EUR/USD", interval="1min", market="OPEN"):
    return await signal(symbol, interval, market, _session_state(request, required=(market.upper() == "OTC")))


@app.get("/signal")
async def get_signal(request: Request, symbol="EUR/USD", interval="1min", market="OPEN"):
    return await signal(symbol, interval, market, _session_state(request, required=(market.upper() == "OTC")))


@app.get("/ai-analysis")
async def ai_analysis(request: Request, symbol: str = "EUR/USD", interval: str = "1min", market: str = "OPEN"):
    """Retorna somente os dados destinados ao painel; estratégias ficam internas."""
    data = await signal(symbol, interval, market, _session_state(request, required=(market.upper() == "OTC")))
    public_keys = [
        "symbol", "interval", "direction", "confidence", "status",
        "ai_confirmed", "risk", "entry_time", "announce_time",
        "expiry_time", "non_repaint"
    ]
    return {k: data.get(k) for k in public_keys}

@app.get("/radar")
async def radar(request: Request, interval="1min", market="OPEN"):
    market=(market or "OPEN").upper()
    if interval not in INTERVALS or market not in ("OPEN", "OTC"):
        raise HTTPException(400, "Intervalo inválido.")
    rkey=f"{market}|{interval}"
    if rkey in radar_cache and time.time() - radar_cache[rkey][0] < 45:
        return radar_cache[rkey][1]
    market=(market or "OPEN").upper()
    if market == "OTC":
        state = _session_state(request, required=True)
        sid = state.get("session_id", "")
        rkey=f"{sid}|OTC|{interval}"
        if rkey in radar_cache and time.time() - radar_cache[rkey][0] < 60:
            return radar_cache[rkey][1]
        out=[]
        # Consulta sequencial para preservar a estabilidade do websocket da IQ Option.
        for sym in SYMBOLS:
            try:
                raw = await candles(sym, interval, 90, "OTC", state)
                closed = raw[:-1] if len(raw) > 1 else raw
                tech = local_engine(closed)
                direction = tech["direction"] if tech.get("confirmed") else "NEUTRO"
                conf = tech["confidence"] if direction != "NEUTRO" else min(tech["confidence"], 69)
                status = "OPORTUNIDADE TÉCNICA" if direction != "NEUTRO" else "MONITORANDO"
                out.append({"symbol": sym + " • OTC", "direction": direction, "confidence": round(conf,1), "status": status})
            except Exception:
                out.append({"symbol": sym + " • OTC", "direction": "NEUTRO", "confidence": 0, "status": "SEM DADOS"})
            await asyncio.sleep(0.08)
        radar_cache[rkey]=(time.time(),out)
        return out
    values = await asyncio.gather(*(signal(s, interval, market) for s in SYMBOLS), return_exceptions=True)
    out = []
    for symbol, value in zip(SYMBOLS, values):
        if isinstance(value, Exception):
            out.append({"symbol": symbol, "direction": "NEUTRO", "confidence": 0, "status": "SEM DADOS"})
        else:
            out.append({"symbol": symbol, "direction": value["direction"], "confidence": value["confidence"], "status": value["status"]})
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
    return {"wins": wins, "losses": losses, "total": total, "accuracy": round(wins / total * 100, 2) if total else 0}


@app.get("/result")
async def result(request: Request, symbol="EUR/USD", interval="1min", direction="CALL", expiry_time="", market="OPEN"):
    if not expiry_time:
        raise HTTPException(400, "expiry_time é obrigatório.")
    market=(market or "OPEN").upper()
    state = _session_state(request, required=(market == "OTC"))
    store = state.setdefault("results", {}) if market == "OTC" else results
    key = f"{market}|{symbol}|{interval}|{direction}|{expiry_time}"
    if key in store:
        return store[key]
    if now() < parse_dt(expiry_time):
        return {"status": "PENDENTE", "result": None}
    cs = await candles(symbol, interval, 20, market, state)
    target = next((c for c in cs if parse_dt(c["datetime"]) >= parse_dt(expiry_time)), None)
    if not target:
        return {"status": "AGUARDANDO CANDLE", "result": None}
    direction = direction.upper()
    res = "WIN" if ((direction == "CALL" and target["close"] > target["open"]) or (direction == "PUT" and target["close"] < target["open"])) else "LOSS"
    out = {"status": "FINALIZADA", "result": res, "candle_time": target["datetime"], "simulated": True}
    store[key] = out
    return out


HTML_PAGE = r'''<!doctype html> <html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"> <title>Mega IA Trader</title> <link rel="manifest" href="/manifest.webmanifest?v=25"> <link rel="icon" type="image/png" sizes="512x512" href="/mega-ia-icon.png?v=25"> <link rel="apple-touch-icon" sizes="192x192" href="/mega-ia-icon-192.png?v=25"> <meta name="theme-color" content="#07182b"> <meta name="application-name" content="Mega IA Trader"> <meta name="apple-mobile-web-app-title" content="Mega IA Trader"> <meta name="apple-mobile-web-app-capable" content="yes"> <style> body{margin:0;background:radial-gradient(circle at 50% 0,#07182b 0,#030812 42%,#02050b 100%);color:#eef5ff;font-family:Arial,sans-serif}.wrap{max-width:1150px;margin:auto;padding:18px}.brand{font-size:42px;font-weight:900;letter-spacing:1px;margin:8px 0 2px}.brand span{color:#14c8ff}.subtitle{font-size:13px;color:#91a9c8;letter-spacing:.7px} .card{background:linear-gradient(180deg,#0c1a2c,#091422);border:1px solid #164f80;border-radius:20px;padding:16px;box-shadow:0 12px 30px #0008,0 0 18px #009cff12;margin-top:12px} .grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}.signal{grid-column:span 2;text-align:center;min-height:270px} .big{font-size:32px;font-weight:800;margin:8px}.call{color:#45ff9b}.put{color:#ff5c7a}.neutral{color:#ffd166}.label{font-size:11px;color:#8190a8} .controls{display:flex;gap:10px;flex-wrap:wrap;margin-top:12px}select,button,input{background:#0d2035;color:#fff;border:1px solid #22689d;border-radius:14px;padding:12px 14px;font-size:15px}select:focus,button:focus,input:focus{outline:none;box-shadow:0 0 0 2px #00bfff55}button{cursor:pointer}input{box-sizing:border-box;width:100%;margin-top:6px}.account-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.account-actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}@media(max-width:600px){.account-grid{grid-template-columns:1fr}} .ai-img{display:none;width:100%;max-width:560px;height:230px;object-fit:contain;border-radius:18px;border:1px solid #168cff66;box-shadow:0 0 35px #008cff55;margin:12px auto;animation:pulse 1.2s infinite alternate} @keyframes pulse{from{filter:brightness(.8)}to{filter:brightness(1.25);box-shadow:0 0 45px #008cff99}} .radar{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}.radar div{background:#101b2b;padding:10px;border-radius:12px} .strategy{line-height:1.65}.status-analysis{color:#33baff}.tabs{display:flex;gap:8px;margin-top:12px;margin-bottom:20px;flex-wrap:wrap}.tabbtn.active{border-color:#168cff;box-shadow:0 0 15px #168cff44}.tab{display:none}.tab.active{display:block} #chartTab>.card{width:min(94vw,920px);margin:14px auto 0;box-sizing:border-box} .chartbox{position:relative;width:100%;height:520px;margin:0 auto;background:#07101c;border:1px solid #1d3049;border-radius:16px;overflow:hidden}.chartbox canvas{width:100%!important;height:100%!important;display:block}.chartmeta{display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:10px}.chartbadge{padding:7px 10px;border-radius:10px;background:#101b2b;color:#b8c7dd;font-size:12px}#chartTab{width:100%}#chartTab>.card{width:min(100%,980px);margin:12px auto 0;padding:18px;box-sizing:border-box} @media(max-width:720px){.wrap{padding:10px}#chartTab>.card{width:100%;max-width:100%;padding:10px}.chartbox{height:430px}} @media(max-width:450px){.wrap{padding:8px}#chartTab>.card{width:100%;padding:8px}.chartbox{height:400px}} .hero{display:none;position:relative;width:100%;max-width:760px;margin:26px auto 18px;border-radius:20px;overflow:hidden;border:1px solid #0bbcff;box-shadow:0 0 30px #00aaff55;background:#05111f}.hero img{display:block;width:100%;height:300px;object-fit:contain;object-position:center;background:#05111f}.entry-arrow{display:none;position:absolute;inset:0;align-items:center;justify-content:center;flex-direction:column;background:#02081488;backdrop-filter:blur(1px);font-weight:900;text-shadow:0 0 18px currentColor}.entry-arrow .arrow{font-size:110px;line-height:.8}.entry-arrow .arrow-label{font-size:28px;margin-top:8px}.entry-arrow.call{display:flex;color:#31ff87}.entry-arrow.put{display:flex;color:#ff405f}.analysisbar{text-align:center;font-size:20px;color:#22c9ff;border-color:#0bbcff}.signal{position:relative}.radar div{border:1px solid #173c5e}.radar .call{filter:drop-shadow(0 0 6px #45ff9b44)}.radar .put{filter:drop-shadow(0 0 6px #ff5c7a44)} @media(max-width:720px){.grid{grid-template-columns:1fr 1fr}.signal{grid-column:span 2}.radar{grid-template-columns:1fr 1fr}.tabs{margin-bottom:22px}.hero{margin:22px auto 16px}.hero img{height:250px}.brand{font-size:36px}.entry-arrow .arrow{font-size:90px}.entry-arrow .arrow-label{font-size:22px}} @media(max-width:450px){.grid{grid-template-columns:1fr}.signal{grid-column:span 1}.radar{grid-template-columns:1fr}.tabs{margin-bottom:24px}.hero{margin:24px auto 14px}.hero img{height:230px}} /* V21: gráfico realmente central e legível no celular */ #chartTab{width:100%;display:none;justify-content:center;align-items:flex-start} #chartTab.active{display:flex} #chartTab>.card{width:min(96vw,1100px)!important;max-width:1100px!important;margin:22px auto 0!important;padding:14px!important} .chartbox{width:100%!important;height:clamp(460px,68vh,700px)!important;margin:0 auto!important} @media(max-width:720px){ #chartTab{width:100%;margin:0 auto;padding:0;justify-content:center} #chartTab>.card{width:calc(100vw - 20px)!important;max-width:none!important;margin:18px auto 0!important;padding:10px!important;transform:none!important} .chartbox{height:58vh!important;min-height:440px!important;max-height:620px!important} .chartmeta{padding:2px 4px 6px} } @media(max-width:450px){ #chartTab>.card{width:calc(100vw - 12px)!important;padding:7px!important;margin-left:auto!important;margin-right:auto!important} .chartbox{height:56vh!important;min-height:420px!important} } /* V22: gráfico mais abaixo e visualmente centralizado */ #chartTab.active{padding-top:7vh!important;box-sizing:border-box} #chartTab.active>.card{margin-top:0!important} @media(max-width:720px){#chartTab.active{padding-top:8vh!important}} @media(max-width:450px){#chartTab.active{padding-top:9vh!important}} </style></head> <body><div class="wrap"> <div class="brand">🤖 MEGA <span>IA</span></div><div class="subtitle">ANÁLISE EM TEMPO REAL • HORÁRIO DE BRASÍLIA</div><div id="clock" style="font-size:22px;margin-top:4px"></div> <div class="controls"><select id="market"><option value="OPEN">🌐 Mercado Aberto</option><option value="OTC">🟣 IQ Option OTC</option></select><select id="symbol"></select><select id="interval"><option>1min</option><option>5min</option><option>15min</option><option>30min</option></select><button id="voiceBtn" onclick="voice()">🔊 Ativar voz</button><div id="otcNote" class="label" style="margin-top:6px;display:none">🟣 IQ Option OTC: verificando conexão...</div></div> <div class="tabs"><button class="tabbtn active" id="tabMain">📊 Painel</button><button class="tabbtn" id="tabChart">📈 Gráfico</button><button class="tabbtn" id="tabAccount">⚙️ Conta IQ Option</button></div> <div id="mainTab" class="tab active"> <div id="heroBox" class="hero"><img id="aiImage" src="__MEGA_IMAGE__" alt="MEGA IA analisando o mercado"><div id="entryArrow" class="entry-arrow"><div class="arrow" id="entryArrowIcon">⬆</div><div class="arrow-label" id="entryArrowLabel">CALL • COMPRAR</div></div></div> <div id="analysisText" class="card analysisbar" style="display:none">🧠 ESTOU ANALISANDO O MERCADO, AGUARDE...</div> <div class="grid"> <div class="card signal"><div class="label">SINAL ATUAL</div><div id="direction" class="big neutral">AGUARDANDO</div><div id="confidence">Confiança: --</div></div> <div class="card"><div class="label">ENTRADA</div><div id="entry" class="big">--:--:--</div><div id="countdown">--</div><div id="expiryCountdown" style="margin-top:8px;font-weight:800">⏱ EXPIRAÇÃO: --:--</div></div> <div class="card"><div class="label">STATUS IA</div><div id="status" class="big" style="font-size:18px">MONITORANDO</div><div id="risk">Risco: --</div></div></div> <div class="grid"><div class="card"><div class="label">WIN</div><div id="wins" class="big call">0</div></div><div class="card"><div class="label">LOSS</div><div id="losses" class="big put">0</div></div><div class="card"><div class="label">ASSERTIVIDADE</div><div id="accuracy" class="big">0%</div></div><div class="card"><div class="label">RESULTADO</div><div id="result" class="big">--</div></div></div> </div> <div id="chartTab" class="tab"> <div class="card"><div class="chartmeta"><b>📈 Gráfico espelhado</b><span class="chartbadge" id="chartInfo">--</span></div><div class="chartbox"><canvas id="priceChart"></canvas></div><div class="label" style="margin-top:8px">O gráfico acompanha o mesmo mercado, par e período selecionados no painel. Para OTC, os candles são solicitados à IQ Option.</div></div> </div> <div id="accountTab" class="tab"> <div class="card"> <h2 style="margin-top:0">⚙️ Conta IQ Option</h2> <div class="label">Use sua própria conta para liberar o gráfico e os candles OTC. Os dados não são gravados no GitHub ou no navegador.</div> <div class="account-grid"> <label>E-mail<input id="iqEmail" type="email" autocomplete="username" placeholder="Seu e-mail da IQ Option"></label> <label>Senha<input id="iqPassword" type="password" autocomplete="current-password" placeholder="Sua senha"></label> </div> <div class="account-actions"><button id="iqConnectBtn">🟢 Conectar</button><button id="iqLogoutBtn">🔴 Desconectar</button></div> <div id="iqAccountStatus" class="card" style="margin-top:12px">● Desconectado</div> <div class="label" style="margin-top:8px">🔒 A senha fica apenas na memória temporária do servidor durante a sessão ativa e é descartada ao desconectar ou expirar.</div> </div> </div> <div class="card"><b>Radar de oportunidades</b><div id="radar" class="radar"></div></div> <div id="licenseCard" class="card" style="display:none"><div class="label">RENOVAÇÃO</div><div id="license"></div></div> </div> <script> const syms=['EUR/USD','GBP/USD','USD/JPY','AUD/USD','USD/CAD','USD/CHF','NZD/USD','EUR/JPY','GBP/JPY','EUR/GBP','BTC/USD','ETH/USD','LTC/USD']; const S=document.getElementById('symbol'); function fillSymbols(){const otc=market.value==='OTC';S.innerHTML='';syms.forEach(x=>S.add(new Option(otc?'🟣 '+x+' • OTC':x,x)));} fillSymbols(); let cur=null,voiceEnabled=false,lastSignalVoice='',lastAnalysis=0,five=false,entered=false,reskey=''; let robotTimer=null,arrowTimer=null;function showRobot(){const h=document.getElementById('heroBox');h.style.display='block';clearTimeout(robotTimer);robotTimer=setTimeout(()=>{if(!entryArrow.classList.contains('call')&&!entryArrow.classList.contains('put')){h.style.display='none';analysisText.style.display='none'}},10000)}function showEntryArrow(dir){showRobot();entryArrow.style.display='';entryArrow.className='entry-arrow '+(dir==='CALL'?'call':'put');entryArrowIcon.textContent=dir==='CALL'?'⬆':'⬇';entryArrowLabel.textContent=dir==='CALL'?'CALL • COMPRAR':'PUT • VENDER';clearTimeout(arrowTimer);arrowTimer=setTimeout(()=>{entryArrow.className='entry-arrow';entryArrow.style.display='';heroBox.style.display='none'},6000)} async function updateMarketNote(){const otc=market.value==='OTC';otcNote.style.display=otc?'block':'none';if(!otc)return;otcNote.textContent='🟣 IQ Option OTC: verificando conexão...';try{const x=await Promise.race([get('/otc-status'),new Promise((_,rej)=>setTimeout(()=>rej(Error('tempo limite de conexão')),16000))]);otcNote.textContent=(x.connected?'🟢 ':x.reconnecting?'🟡 ':'🔴 ')+x.message}catch(e){otcNote.textContent='🔴 IQ Option OTC: '+(e.message||'falha de conexão')}} updateMarketNote(); market.onchange=()=>{fillSymbols();updateMarketNote();sig(true);rad();if(chartTab.classList.contains('active'))loadChart()}; let megaVoices=[];function loadMegaVoices(){if(window.speechSynthesis)megaVoices=speechSynthesis.getVoices()||[]}if(window.speechSynthesis){loadMegaVoices();speechSynthesis.addEventListener?.('voiceschanged',loadMegaVoices)} function pickMalePtBRVoice(){const vs=(megaVoices.length?megaVoices:speechSynthesis.getVoices()).filter(v=>/^pt(-|_)?BR$/i.test((v.lang||'').replace('_','-'))||/^pt(-|_)/i.test(v.lang||''));const male=/antonio|antônio|daniel|ricardo|felipe|paulo|carlos|thiago|bruno|marcelo|male|masculino|homem/i;const female=/maria|luciana|fernanda|camila|female|feminina|mulher/i;return vs.find(v=>male.test(v.name||''))||vs.find(v=>!female.test(v.name||'')&&/google|microsoft|samsung|android/i.test(v.name||''))||vs.find(v=>!female.test(v.name||''))||vs[0]||null} function speak(t){if(!voiceEnabled||!window.speechSynthesis)return;speechSynthesis.cancel();const u=new SpeechSynthesisUtterance(t);u.lang='pt-BR';u.rate=.86;u.pitch=.62;u.volume=1;const mv=pickMalePtBRVoice();if(mv)u.voice=mv;speechSynthesis.speak(u)} function voice(){voiceEnabled=true;voiceBtn.textContent='🔊 Voz ativada';speak('Voz da Mega IA ativada.');setTimeout(()=>sig(true),650)} function ft(x){return x?new Date(x).toLocaleTimeString('pt-BR',{hour12:false}):'--:--:--'} function iqSessionToken(){try{return localStorage.getItem('mega_iq_session_token')||''}catch(_){return ''}} function authHeaders(extra={}){const h={...extra};const t=iqSessionToken();if(t)h['X-IQ-Session']=t;return h} async function get(u){const r=await fetch(u,{cache:'no-store',credentials:'include',headers:authHeaders()});let j=null;try{j=await r.json()}catch(_){j=null}if(!r.ok)throw Error((j&&j.detail)?j.detail:'HTTP '+r.status);return j} async function post(u,data={}){const r=await fetch(u,{method:'POST',credentials:'include',headers:authHeaders({'Content-Type':'application/json'}),body:JSON.stringify(data)});let j=null;try{j=await r.json()}catch(_){j=null}if(!r.ok)throw Error((j&&j.detail)?j.detail:'HTTP '+r.status);return j} let chartTimer=null; const chartCanvas=document.getElementById('priceChart'); const chartCtx=chartCanvas.getContext('2d'); function resizeChart(){const r=chartCanvas.getBoundingClientRect();const d=window.devicePixelRatio||1;chartCanvas.width=Math.max(1,r.width*d);chartCanvas.height=Math.max(1,r.height*d);chartCtx.setTransform(d,0,0,d,0,0);if(chartData.length)drawChart(chartData);} let chartData=[]; function drawChart(a){ const w=chartCanvas.clientWidth,h=chartCanvas.clientHeight;chartCtx.clearRect(0,0,w,h);if(!a.length)return; const pad={l:55,r:12,t:18,b:28},cw=w-pad.l-pad.r,ch=h-pad.t-pad.b; let lo=Math.min(...a.map(c=>Number(c.low))),hi=Math.max(...a.map(c=>Number(c.high)));const extra=(hi-lo)*.08||1;lo-=extra;hi+=extra; const px=i=>pad.l+(i/(a.length-1||1))*cw;const py=v=>pad.t+(hi-v)/(hi-lo)*ch; chartCtx.strokeStyle='#19304a';chartCtx.lineWidth=1;chartCtx.font='11px Arial';chartCtx.fillStyle='#8190a8'; for(let j=0;j<5;j++){const y=pad.t+j*ch/4;chartCtx.beginPath();chartCtx.moveTo(pad.l,y);chartCtx.lineTo(w-pad.r,y);chartCtx.stroke();const v=hi-(hi-lo)*j/4;chartCtx.fillText(v.toFixed(5),4,y+4)} const step=Math.max(2,cw/a.length*.72); a.forEach((c,i)=>{const x=px(i),o=Number(c.open),cl=Number(c.close),hh=Number(c.high),ll=Number(c.low);const up=cl>=o;chartCtx.strokeStyle=up?'#45ff9b':'#ff5c7a';chartCtx.fillStyle=up?'#45ff9b':'#ff5c7a';chartCtx.beginPath();chartCtx.moveTo(x,py(hh));chartCtx.lineTo(x,py(ll));chartCtx.stroke();const top=py(Math.max(o,cl)),bot=py(Math.min(o,cl));chartCtx.fillRect(x-step/2,top,step,Math.max(1,bot-top));if(i%Math.ceil(a.length/6)===0){chartCtx.fillStyle='#8190a8';chartCtx.fillText(new Date(c.datetime).toLocaleTimeString('pt-BR',{hour:'2-digit',minute:'2-digit'}),x-18,h-7)}}); if(cur&&cur.direction&&cur.direction!=='NEUTRO'){const idx=a.findIndex(c=>c.datetime===cur.reference_candle);if(idx>=0){const x=px(idx),v=cur.direction==='CALL'?Number(a[idx].low):Number(a[idx].high);chartCtx.fillStyle=cur.direction==='CALL'?'#45ff9b':'#ff5c7a';chartCtx.beginPath();chartCtx.arc(x,py(v),6,0,Math.PI*2);chartCtx.fill();chartCtx.font='bold 12px Arial';chartCtx.fillText(cur.direction,x+8,py(v)-8)}} } async function loadChart(){try{const d=await get(`/candles?market=${encodeURIComponent(market.value)}&symbol=${encodeURIComponent(S.value)}&interval=${encodeURIComponent(interval.value)}&n=80`);chartData=d.candles||[];chartInfo.textContent=(d.market==='OTC'?'🟣 '+d.symbol+' • OTC':d.symbol)+' • '+d.interval;drawChart(chartData)}catch(e){chartInfo.textContent='OTC sem dados: '+(e.message||'verifique a conexão')}} function showTab(which){const main=which==='main',chart=which==='chart',account=which==='account';mainTab.classList.toggle('active',main);chartTab.classList.toggle('active',chart);accountTab.classList.toggle('active',account);tabMain.classList.toggle('active',main);tabChart.classList.toggle('active',chart);tabAccount.classList.toggle('active',account);if(chart){loadChart();setTimeout(resizeChart,50)}if(account)refreshAccountStatus()} tabMain.onclick=()=>showTab('main');tabChart.onclick=()=>showTab('chart');tabAccount.onclick=()=>showTab('account');window.addEventListener('resize',resizeChart); async function refreshAccountStatus(){try{const x=await get('/otc-status');const rr=!!x.reconnecting;iqAccountStatus.textContent=(x.connected&&!rr?'🟢 ':rr?'🟡 ':'🔴 ')+(x.connected&&!rr?('Conectado: '+(x.email_masked||'')):x.message);iqConnectBtn.disabled=!!x.connected||rr;iqLogoutBtn.disabled=!x.connected&&!rr;if(rr)setTimeout(refreshAccountStatus,2500)}catch(e){iqAccountStatus.textContent='🟡 Reconectando à IQ Option...';setTimeout(refreshAccountStatus,2500)}} iqConnectBtn.onclick=async()=>{const email=iqEmail.value.trim(),password=iqPassword.value;if(!email||!password){iqAccountStatus.textContent='🔴 Informe e-mail e senha.';return}iqAccountStatus.textContent='🟡 Conectando...';iqConnectBtn.disabled=true;try{const x=await post('/iq-login',{email,password});if(x.session_token){try{localStorage.setItem('mega_iq_session_token',x.session_token)}catch(_){}}iqPassword.value='';iqEmail.value='';iqAccountStatus.textContent='🟢 Conectado: '+(x.email_masked||'');await updateMarketNote();if(market.value==='OTC'){sig(true);if(chartTab.classList.contains('active'))loadChart()}}catch(e){iqAccountStatus.textContent='🔴 '+e.message}finally{iqConnectBtn.disabled=false;refreshAccountStatus()}}; iqLogoutBtn.onclick=async()=>{try{await post('/iq-logout',{});try{localStorage.removeItem('mega_iq_session_token')}catch(_){}iqAccountStatus.textContent='● Desconectado';otcNote.textContent='🔴 Conecte sua conta na aba Conta IQ Option.';chartData=[];drawChart([])}catch(e){iqAccountStatus.textContent='🔴 '+e.message}refreshAccountStatus()}; async function sig(announce=false){ if(announce&&voiceEnabled&&Date.now()-lastAnalysis>2500){lastAnalysis=Date.now();showRobot();analysisText.style.display='block';status.textContent='ANALISANDO O MERCADO...';speak('O Mega IA está analisando o mercado.')} try{ cur=await get(`/signal-ai?market=${encodeURIComponent(market.value)}&symbol=${encodeURIComponent(S.value)}&interval=${encodeURIComponent(interval.value)}`); if(announce){showRobot();analysisText.style.display='block';} direction.textContent=cur.direction;direction.className='big '+(cur.direction==='CALL'?'call':cur.direction==='PUT'?'put':'neutral'); confidence.textContent='Confiança: '+cur.confidence+'%';entry.textContent=cur.direction==='NEUTRO'?'AGUARDANDO SINAL':ft(cur.entry_time);countdown.textContent=cur.direction==='NEUTRO'?'Sem entrada confirmada':'Preparando entrada';status.textContent=cur.status;risk.textContent='Risco: '+cur.risk; if(cur.direction!=='NEUTRO'){ const k=cur.symbol+'|'+cur.interval+'|'+cur.entry_time+'|'+cur.direction; if(k!==lastSignalVoice){lastSignalVoice=k;if(voiceEnabled){speak(cur.direction==='CALL'?'Análise concluída. Sinal de CALL identificado.':'Análise concluída. Sinal de PUT identificado.')}} }else if(announce&&voiceEnabled){speak('Análise concluída. Não há oportunidade segura no momento.')} fifteen=false;five=false;entered=false; }catch(e){status.textContent='ERRO DE DADOS';if(announce&&voiceEnabled)speak('Não foi possível concluir a análise. Aguarde.')} } async function perf(){try{const p=await get('/performance?market='+encodeURIComponent(market.value));wins.textContent=p.wins;losses.textContent=p.losses;accuracy.textContent=p.accuracy+'%'}catch(e){}} async function rad(){try{const a=await get('/radar?market='+encodeURIComponent(market.value)+'&interval='+encodeURIComponent(interval.value));radar.innerHTML=a.map(x=>`<div><b>${x.symbol}</b><br><span class="${x.direction==='CALL'?'call':x.direction==='PUT'?'put':'neutral'}">${x.direction}</span> • ${x.confidence}%<br><small>${x.status}</small></div>`).join('')}catch(e){}} async function lic(){try{const x=await get('/license');if(x.active){licenseCard.style.display='none';license.textContent='';return}licenseCard.style.display='block';license.innerHTML=`<b>⚠️ LICENÇA EXPIRADA</b><br>Renove o aplicativo para continuar usando.<br><br>WhatsApp: ${x.whatsapp_1} / ${x.whatsapp_2}<br>Instagram: ${x.instagram}`;}catch(e){licenseCard.style.display='none'}} async function clk(){try{const x=await get('/server-time');clock.textContent=ft(x.datetime)+' • Brasília'}catch(e){}} let fifteen=false;function cd(){const ex=document.getElementById('expiryCountdown');if(!cur||cur.direction==='NEUTRO'||!cur.entry_time){countdown.textContent='Sem entrada confirmada';if(ex)ex.textContent='⏱ EXPIRAÇÃO: --:--';return}const now=Date.now(),et=new Date(cur.entry_time).getTime(),xt=cur.expiry_time?new Date(cur.expiry_time).getTime():0;const n=Math.ceil((et-now)/1000);countdown.textContent=n>0?'Entrada em '+n+'s':'Entrada liberada';if(n>0){if(ex)ex.textContent='⏱ EXPIRAÇÃO: aguardando entrada'}else if(xt){const rem=Math.max(0,Math.ceil((xt-now)/1000)),mm=String(Math.floor(rem/60)).padStart(2,'0'),ss=String(rem%60).padStart(2,'0');if(ex)ex.textContent='⏱ EXPIRAÇÃO: '+mm+':'+ss}else if(ex)ex.textContent='⏱ EXPIRAÇÃO: --:--';if(n<=15&&n>13&&!fifteen){fifteen=true;if(voiceEnabled)speak('Atenção. Sinal confirmado de '+cur.direction+'. Entrada em 15 segundos.')}if(n<=5&&n>3&&!five){five=true;if(voiceEnabled)speak('Atenção. Entrada em 5 segundos.')}if(n<=0&&n>-2&&!entered){entered=true;showEntryArrow(cur.direction);if(voiceEnabled)speak(cur.direction==='CALL'?'Entrada liberada. Comprar agora.':'Entrada liberada. Vender agora.')}} async function resultCheck(){if(!cur||cur.direction==='NEUTRO')return;try{const x=await get(`/result?market=${encodeURIComponent(market.value)}&symbol=${encodeURIComponent(cur.symbol)}&interval=${cur.interval}&direction=${cur.direction}&expiry_time=${encodeURIComponent(cur.expiry_time)}`);if(x.result){result.textContent=x.result;const k=cur.symbol+'|'+cur.expiry_time;if(k!==reskey){reskey=k;if(voiceEnabled)speak('Operação finalizada. Resultado '+x.result+'.')}perf()}}catch(e){}} market.onchange=async()=>{localStorage.setItem('mega_market',market.value);fillSymbols();const savedSym=localStorage.getItem('mega_symbol');if(savedSym&&[...S.options].some(o=>o.value===savedSym))S.value=savedSym;await updateMarketNote();lastSignalVoice='';sig(true);rad();if(chartTab.classList.contains('active'))loadChart()};S.onchange=()=>{localStorage.setItem('mega_symbol',S.value);lastSignalVoice='';sig(true);rad();if(chartTab.classList.contains('active'))loadChart()};interval.onchange=()=>{localStorage.setItem('mega_interval',interval.value);lastSignalVoice='';sig(true);rad();if(chartTab.classList.contains('active'))loadChart()}; // Restaura mercado/par/período após atualizar a página, sem armazenar e-mail ou senha. try{const sm=localStorage.getItem('mega_market');if(sm==='OPEN'||sm==='OTC')market.value=sm;fillSymbols();const ss=localStorage.getItem('mega_symbol');if(ss&&[...S.options].some(o=>o.value===ss))S.value=ss;const si=localStorage.getItem('mega_interval');if(si&&[...interval.options].some(o=>o.value===si))interval.value=si}catch(_){} async function bootApp(){ await refreshAccountStatus(); await updateMarketNote(); // Dá tempo para a sessão OTC ser recuperada antes das primeiras chamadas de dados. if(market.value==='OTC') await new Promise(r=>setTimeout(r,900)); sig(false);perf();rad();lic();clk(); } bootApp();setInterval(()=>sig(false),5000);setInterval(()=>{if(chartTab.classList.contains('active'))loadChart()},5000);setInterval(perf,5000);setInterval(rad,90000);setInterval(resultCheck,3000);setInterval(clk,1000);setInterval(cd,250); </script></body></html>'''


HTML_PAGE = HTML_PAGE.replace("__MEGA_IMAGE__", "/mega-ia.png")

@app.get("/", response_class=HTMLResponse)
async def home():
    return HTMLResponse(HTML_PAGE)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))