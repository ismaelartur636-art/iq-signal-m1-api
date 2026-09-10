import os
from datetime import datetime, timezone
from typing import Any
import httpx
from fastapi import FastAPI, HTTPException

app = FastAPI(title="IQ Signal M1/M5/M15/M30 API", version="3.0.0")

KEY = os.getenv("TWELVE_DATA_API_KEY", "")
URL = "https://api.twelvedata.com/time_series"
ALLOWED_INTERVALS = {"1min", "5min", "15min", "30min"}

@app.get("/")
def root():
    return {
        "ok": True,
        "service": "IQ Signal Multi-Ativos API",
        "version": "3.0.0",
        "intervals": ["1min", "5min", "15min", "30min"],
        "examples": ["EUR/USD", "GBP/USD", "USD/JPY", "BTC/USD", "ETH/USD"],
        "docs": "/docs"
    }

@app.get("/health")
def health():
    return {"ok": True}

async def get_candles(symbol: str, interval: str, size: int = 200):
    if not KEY:
        raise HTTPException(500, "TWELVE_DATA_API_KEY não configurada no Render.")
    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(400, "Intervalo inválido. Use 1min, 5min, 15min ou 30min.")

    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": max(80, min(size, 500)),
        "apikey": KEY,
        "timezone": "America/Sao_Paulo",
        "format": "JSON"
    }

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(URL, params=params)
            response.raise_for_status()
            data: dict[str, Any] = response.json()
    except httpx.HTTPError as exc:
        raise HTTPException(502, f"Erro ao consultar a fonte de dados: {exc}") from exc

    if data.get("status") == "error":
        raise HTTPException(502, data.get("message", "Erro retornado pela Twelve Data."))

    values = data.get("values", [])
    if not values:
        raise HTTPException(502, "A fonte de dados não retornou candles para esse ativo/intervalo.")
    return values

@app.get("/candles")
async def candles(symbol: str = "EUR/USD", interval: str = "1min", outputsize: int = 100):
    values = await get_candles(symbol, interval, outputsize)
    return {
        "ok": True, "source": "Twelve Data", "symbol": symbol,
        "interval": interval, "count": len(values), "values": values
    }

def ema(x, p):
    if len(x) < p:
        return None
    a = 2.0 / (p + 1.0)
    e = sum(x[:p]) / p
    for z in x[p:]:
        e = z * a + e * (1.0 - a)
    return e

def rsi(x, p=14):
    if len(x) < p + 1:
        return None
    gains = [max(x[i] - x[i-1], 0.0) for i in range(1, len(x))]
    losses = [max(x[i-1] - x[i], 0.0) for i in range(1, len(x))]
    ag = sum(gains[:p]) / p
    al = sum(losses[:p]) / p
    for i in range(p, len(gains)):
        ag = ((p-1)*ag + gains[i]) / p
        al = ((p-1)*al + losses[i]) / p
    return 100.0 if al == 0 else 100.0 - 100.0/(1.0 + ag/al)

def adx(values, p=14):
    if len(values) < 2*p + 2:
        return None
    v = list(reversed(values))
    h = [float(a["high"]) for a in v]
    l = [float(a["low"]) for a in v]
    c = [float(a["close"]) for a in v]
    tr, plus, minus = [], [], []

    for i in range(1, len(c)):
        tr.append(max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])))
        up = h[i]-h[i-1]
        down = l[i-1]-l[i]
        plus.append(up if up > down and up > 0 else 0.0)
        minus.append(down if down > up and down > 0 else 0.0)

    atr = sum(tr[:p])/p
    pp = sum(plus[:p])/p
    mm = sum(minus[:p])/p
    dx = []

    for i in range(p, len(tr)):
        atr = ((p-1)*atr + tr[i])/p
        pp = ((p-1)*pp + plus[i])/p
        mm = ((p-1)*mm + minus[i])/p
        pi = 100.0*pp/atr if atr else 0.0
        mi = 100.0*mm/atr if atr else 0.0
        dx.append(100.0*abs(pi-mi)/(pi+mi) if pi+mi else 0.0)

    if len(dx) < p:
        return None
    value = sum(dx[:p])/p
    for z in dx[p:]:
        value = ((p-1)*value + z)/p
    return value

def analyze(values):
    # Newest candle is ignored. The signal uses the most recent CLOSED candle.
    if len(values) < 100:
        return {"signal": "WAIT", "confidence": 0, "reason": "Dados insuficientes."}

    closed = values[1:]
    x = [float(a["close"]) for a in reversed(closed)]

    e3 = ema(x, 3)
    e7 = ema(x, 7)
    rr = rsi(x, 14)
    a21 = adx(closed, 21)
    a48 = adx(closed, 48)

    if None in (e3, e7, rr, a21, a48):
        return {"signal": "WAIT", "confidence": 0, "reason": "Dados insuficientes para os filtros."}

    call = put = 0

    if e3 > e7:
        call += 2
    elif e3 < e7:
        put += 2

    if 52 <= rr < 70:
        call += 1
    elif 30 < rr <= 48:
        put += 1

    if a21 >= 20:
        if e3 > e7: call += 1
        elif e3 < e7: put += 1

    if a48 >= 20:
        if e3 > e7: call += 1
        elif e3 < e7: put += 1

    if call >= 4 and call > put:
        signal = "CALL"
        score = call
    elif put >= 4 and put > call:
        signal = "PUT"
        score = put
    else:
        signal = "WAIT"
        score = max(call, put)

    confidence = min(95, 50 + score*8) if signal != "WAIT" else 50 + score*3

    return {
        "signal": signal,
        "confidence": confidence,
        "reference_candle": closed[0].get("datetime"),
        "next_candle": values[0].get("datetime"),
        "ema3": round(e3, 6),
        "ema7": round(e7, 6),
        "rsi14": round(rr, 2),
        "adx21": round(a21, 2),
        "adx48": round(a48, 2),
        "call_score": call,
        "put_score": put,
        "non_repaint_reference": True
    }

@app.get("/signal")
async def signal(symbol: str = "EUR/USD", interval: str = "1min"):
    values = await get_candles(symbol, interval, 200)
    return {
        "ok": True,
        "source": "Twelve Data",
        "symbol": symbol,
        "interval": interval,
        "expiry": "1 candle do intervalo selecionado",
        **analyze(values),
        "warning": "Sinal probabilístico; não garante WIN. A Twelve Data pode não coincidir com os preços OTC da IQ Option."
    }

@app.get("/server-time")
def server_time():
    return {"utc": datetime.now(timezone.utc).isoformat()}