import os
import asyncio
import time
import json
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse

app = FastAPI(title="MEGA IA", version="14.0.0")

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
IMAGE_PATH = os.path.join(os.path.dirname(__file__), "mega_ia.png")

INTERVALS = {"1min": 60, "5min": 300, "15min": 900, "30min": 1800}
SYMBOLS = [
    "EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD", "USD/CHF",
    "NZD/USD", "EUR/JPY", "GBP/JPY", "EUR/GBP", "BTC/USD", "ETH/USD"
]

cache: Dict[str, Any] = {}
oai_cache: Dict[str, Any] = {}
results: Dict[str, Any] = {}
radar_cache: Dict[str, Any] = {}
td_sem = asyncio.Semaphore(3)


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


def local_engine(cs):
    a = bollinger_stochastic(cs)
    b = ema_rsi_strategy(cs)
    if a["confirmed"] and b["confirmed"] and a["direction"] == b["direction"]:
        return {"direction": a["direction"], "confidence": round((a["confidence"] + b["confidence"]) / 2 + 5, 1),
                "confirmed": True, "strategy": "CONFLUÊNCIA: Bollinger + Estocástico + EMA + RSI",
                "reason": a["reason"] + " | " + b["reason"], "bollinger": a, "ema_rsi": b}
    if a["confirmed"]:
        return {"direction": a["direction"], "confidence": a["confidence"], "confirmed": True,
                "strategy": a["strategy"], "reason": a["reason"], "bollinger": a, "ema_rsi": b}
    if b["confirmed"]:
        return {"direction": b["direction"], "confidence": b["confidence"], "confirmed": True,
                "strategy": b["strategy"], "reason": b["reason"], "bollinger": a, "ema_rsi": b}
    return {"direction": "NEUTRO", "confidence": max(a["confidence"], b["confidence"]), "confirmed": False,
            "strategy": "Duas estratégias sem confirmação", "reason": a["reason"] + " | " + b["reason"],
            "bollinger": a, "ema_rsi": b}


def json_extract(text):
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.I)
    try:
        return json.loads(text)
    except Exception:
        m = re.search(r"\{.*\}", text, re.S)
        return json.loads(m.group(0)) if m else None


async def candles(symbol, interval, n=80):
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


async def openai_confirm(symbol, interval, cs, analysis):
    if not OAI_KEY or not OAI_MODEL:
        return {"available": False, "reason": "OPENAI_API_KEY/OPENAI_MODEL não configurados."}
    key = f"{symbol}|{interval}|{cs[-1]['datetime']}"
    if key in oai_cache and time.time() - oai_cache[key][0] < 55:
        return oai_cache[key][1]
    data = [{"time": c["datetime"], "o": c["open"], "h": c["high"], "l": c["low"], "c": c["close"], "v": c["volume"]} for c in cs[-40:]]
    prompt = f'''Você é o módulo de confirmação da MEGA IA. Ativo {symbol}, timeframe {interval}.
Use SOMENTE candles fechados. Não invente dados futuros.
Estratégias: Bollinger 20/2 + Estocástico 14,3,3 e EMA 9/21 + RSI 7.
Análise técnica preliminar: {json.dumps(analysis, ensure_ascii=False)}
Retorne SOMENTE JSON: {{"direction":"CALL|PUT|NEUTRO","confidence":0,"confirmed":true,"reason":"curto","risk":"LOW|MEDIUM|HIGH"}}
Candles: {json.dumps(data, ensure_ascii=False)}'''
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
    return entry - timedelta(seconds=5), entry, entry + timedelta(seconds=INTERVALS[interval])


async def signal(symbol, interval):
    if symbol not in SYMBOLS or interval not in INTERVALS:
        raise HTTPException(400, "Ativo ou intervalo inválido.")
    key = f"{symbol}|{interval}"
    if key in cache and time.time() - cache[key][0] < 4:
        return cache[key][1]

    raw = await candles(symbol, interval, 90)
    closed = raw[:-1] if len(raw) > 1 else raw  # sem vela em formação = não repinta
    analysis = local_engine(closed)
    announce, entry, expiry = entry_window(interval)

    base = {"symbol": symbol, "interval": interval, "direction": "NEUTRO", "confidence": analysis["confidence"],
            "entry_time": iso(entry), "announce_time": iso(announce), "expiry_time": iso(expiry),
            "status": "AGUARDANDO", "ai_confirmed": False, "risk": "HIGH", "strategy": analysis["strategy"],
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

    cache[key] = (time.time(), base)
    return base


@app.get("/health")
async def health():
    return {"status": "ok", "app": "MEGA IA", "version": "14.0.0", "brasilia_time": iso(now()),
            "twelve_data_configured": bool(TD_KEY), "openai_configured": bool(OAI_KEY), "openai_model_configured": bool(OAI_MODEL)}


@app.get("/server-time")
async def server_time():
    return {"datetime": iso(now()), "timezone": "America/Sao_Paulo"}


@app.get("/license")
async def license_info():
    try:
        exp = datetime.strptime(LICENSE, "%Y-%m-%d").date()
        days = max(0, (exp - now().date()).days)
        active = now().date() <= exp
    except Exception:
        days, active = 0, False
    return {"active": active, "expires": LICENSE, "days_remaining": days, "whatsapp_1": WA1, "whatsapp_2": WA2, "instagram": IG}


@app.get("/mega-ia.png")
async def mega_image():
    if not os.path.exists(IMAGE_PATH):
        raise HTTPException(404, "Imagem mega_ia.png não encontrada no servidor.")
    return FileResponse(IMAGE_PATH, media_type="image/png")


@app.get("/candles")
async def get_candles(symbol="EUR/USD", interval="1min", limit=50):
    if symbol not in SYMBOLS or interval not in INTERVALS:
        raise HTTPException(400, "Ativo ou intervalo inválido.")
    return {"symbol": symbol, "interval": interval, "candles": await candles(symbol, interval, int(clamp(limit, 10, 100)))}


@app.get("/signal-ai")
async def signal_ai(symbol="EUR/USD", interval="1min"):
    return await signal(symbol, interval)


@app.get("/signal")
async def get_signal(symbol="EUR/USD", interval="1min"):
    return await signal(symbol, interval)


@app.get("/ai-analysis")
async def ai_analysis(symbol="EUR/USD", interval="1min"):
    s = await signal(symbol, interval)
    return s


@app.get("/radar")
async def radar(interval="1min"):
    if interval not in INTERVALS:
        raise HTTPException(400, "Intervalo inválido.")
    if interval in radar_cache and time.time() - radar_cache[interval][0] < 45:
        return radar_cache[interval][1]
    values = await asyncio.gather(*(signal(s, interval) for s in SYMBOLS), return_exceptions=True)
    out = []
    for symbol, value in zip(SYMBOLS, values):
        if isinstance(value, Exception):
            out.append({"symbol": symbol, "direction": "NEUTRO", "confidence": 0, "status": "SEM DADOS"})
        else:
            out.append({"symbol": symbol, "direction": value["direction"], "confidence": value["confidence"], "status": value["status"], "strategy": value["strategy"]})
    radar_cache[interval] = (time.time(), out)
    return out


@app.get("/performance")
async def performance(interval="1min"):
    wins = sum(1 for x in results.values() if x.get("result") == "WIN")
    losses = sum(1 for x in results.values() if x.get("result") == "LOSS")
    total = wins + losses
    return {"wins": wins, "losses": losses, "total": total, "accuracy": round(wins / total * 100, 2) if total else 0}


@app.get("/result")
async def result(symbol="EUR/USD", interval="1min", direction="CALL", expiry_time=""):
    if not expiry_time:
        raise HTTPException(400, "expiry_time é obrigatório.")
    key = f"{symbol}|{interval}|{direction}|{expiry_time}"
    if key in results:
        return results[key]
    if now() < parse_dt(expiry_time):
        return {"status": "PENDENTE", "result": None}
    cs = await candles(symbol, interval, 20)
    target = next((c for c in cs if parse_dt(c["datetime"]) >= parse_dt(expiry_time)), None)
    if not target:
        return {"status": "AGUARDANDO CANDLE", "result": None}
    direction = direction.upper()
    res = "WIN" if ((direction == "CALL" and target["close"] > target["open"]) or (direction == "PUT" and target["close"] < target["open"])) else "LOSS"
    out = {"status": "FINALIZADA", "result": res, "candle_time": target["datetime"], "simulated": True}
    results[key] = out
    return out


HTML_PAGE = r'''<!doctype html>
<html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MEGA IA</title>
<style>
body{margin:0;background:#050913;color:#eef5ff;font-family:Arial,sans-serif}.wrap{max-width:1150px;margin:auto;padding:16px}
.card{background:#0d1625;border:1px solid #1d3049;border-radius:18px;padding:16px;box-shadow:0 10px 30px #0005;margin-top:12px}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}.signal{grid-column:span 2;text-align:center;min-height:270px}
.big{font-size:32px;font-weight:800;margin:8px}.call{color:#45ff9b}.put{color:#ff5c7a}.neutral{color:#ffd166}.label{font-size:11px;color:#8190a8}
.controls{display:flex;gap:10px;flex-wrap:wrap;margin-top:12px}select,button{background:#111f33;color:#fff;border:1px solid #2b4463;border-radius:12px;padding:11px}button{cursor:pointer}
.ai-img{display:none;width:100%;max-width:560px;height:270px;object-fit:cover;border-radius:18px;border:1px solid #168cff66;box-shadow:0 0 35px #008cff55;margin:12px auto;animation:pulse 1.2s infinite alternate}
@keyframes pulse{from{filter:brightness(.8)}to{filter:brightness(1.25);box-shadow:0 0 45px #008cff99}}
.radar{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}.radar div{background:#101b2b;padding:10px;border-radius:12px}
.strategy{line-height:1.65}.status-analysis{color:#33baff}
@media(max-width:720px){.grid{grid-template-columns:1fr 1fr}.signal{grid-column:span 2}.radar{grid-template-columns:1fr 1fr}.ai-img{height:220px}}
@media(max-width:450px){.grid{grid-template-columns:1fr}.signal{grid-column:span 1}.radar{grid-template-columns:1fr}}
</style></head>
<body><div class="wrap">
<h1>🤖 MEGA IA</h1><div class="label">ANÁLISE EM TEMPO REAL • HORÁRIO DE BRASÍLIA</div><div id="clock"></div>
<div class="controls"><select id="symbol"></select><select id="interval"><option>1min</option><option>5min</option><option>15min</option><option>30min</option></select><button id="voiceBtn" onclick="voice()">🔊 Ativar voz</button></div>
<img id="aiImage" class="ai-img" src="/mega-ia.png" alt="MEGA IA analisando o mercado">
<div id="analysisText" class="card status-analysis" style="display:none;text-align:center;font-size:20px">🧠 ESTOU ANALISANDO O MERCADO, AGUARDE...</div>
<div class="grid">
<div class="card signal"><div class="label">SINAL ATUAL</div><div id="direction" class="big neutral">AGUARDANDO</div><div id="confidence">Confiança: --</div><div id="strategyName">Estratégia: --</div></div>
<div class="card"><div class="label">ENTRADA</div><div id="entry" class="big">--:--:--</div><div id="countdown">--</div></div>
<div class="card"><div class="label">STATUS IA</div><div id="status" class="big" style="font-size:18px">MONITORANDO</div><div id="risk">Risco: --</div></div></div>
<div class="grid"><div class="card"><div class="label">WIN</div><div id="wins" class="big call">0</div></div><div class="card"><div class="label">LOSS</div><div id="losses" class="big put">0</div></div><div class="card"><div class="label">ASSERTIVIDADE</div><div id="accuracy" class="big">0%</div></div><div class="card"><div class="label">RESULTADO</div><div id="result" class="big">--</div></div></div>
<div class="card strategy"><b>📊 Estratégias ativas</b><br>1) Bollinger <b>20/2</b> + Estocástico <b>14,3,3</b>: rompimento e fechamento fora da banda + zona extrema + cruzamento de retorno.<br>2) EMA <b>9/21</b> + RSI <b>7</b>: cruzamento das médias + RSI acima/abaixo de 50 e ainda dentro de 70/30.<br><small>Somente candles fechados são usados para evitar repintura. A entrada é programada para a abertura da próxima vela e o aviso ocorre 5 segundos antes.</small></div>
<div class="card"><b>Radar de oportunidades</b><div id="radar" class="radar"></div></div>
<div class="card"><div class="label">LICENÇA</div><div id="license">Verificando...</div></div>
</div>
<script>
const syms=['EUR/USD','GBP/USD','USD/JPY','AUD/USD','USD/CAD','USD/CHF','NZD/USD','EUR/JPY','GBP/JPY','EUR/GBP','BTC/USD','ETH/USD'];
const S=document.getElementById('symbol');syms.forEach(x=>S.add(new Option(x,x)));
let cur=null,voiceEnabled=false,lastSignalVoice='',lastAnalysis=0,five=false,entered=false,reskey='';
function speak(t){if(!voiceEnabled||!window.speechSynthesis)return;speechSynthesis.cancel();const u=new SpeechSynthesisUtterance(t);u.lang='pt-BR';u.rate=.95;speechSynthesis.speak(u)}
function voice(){voiceEnabled=true;voiceBtn.textContent='🔊 Voz ativada';speak('Voz da Mega IA ativada.');setTimeout(()=>sig(true),650)}
function ft(x){return x?new Date(x).toLocaleTimeString('pt-BR',{hour12:false}):'--:--:--'}
async function get(u){const r=await fetch(u,{cache:'no-store'});if(!r.ok)throw Error('HTTP '+r.status);return r.json()}
async function sig(announce=false){
 if(announce&&voiceEnabled&&Date.now()-lastAnalysis>2500){lastAnalysis=Date.now();document.getElementById('aiImage').style.display='block';analysisText.style.display='block';status.textContent='ANALISANDO O MERCADO...';speak('Estou analisando o mercado, aguarde.')}
 try{
  cur=await get(`/signal-ai?symbol=${encodeURIComponent(S.value)}&interval=${encodeURIComponent(interval.value)}`);
  document.getElementById('aiImage').style.display=announce?'block':'none';analysisText.style.display=announce?'block':'none';
  direction.textContent=cur.direction;direction.className='big '+(cur.direction==='CALL'?'call':cur.direction==='PUT'?'put':'neutral');
  confidence.textContent='Confiança: '+cur.confidence+'%';entry.textContent=ft(cur.entry_time);status.textContent=cur.status;risk.textContent='Risco: '+cur.risk;strategyName.textContent='Estratégia: '+(cur.strategy||'--');
  if(cur.direction!=='NEUTRO'){
   const k=cur.symbol+'|'+cur.interval+'|'+cur.entry_time+'|'+cur.direction;
   if(k!==lastSignalVoice){lastSignalVoice=k;if(voiceEnabled){speak(cur.direction==='CALL'?'Análise concluída. Sinal de CALL identificado.':'Análise concluída. Sinal de PUT identificado.')}}
  }else if(announce&&voiceEnabled){speak('Análise concluída. Não há oportunidade segura no momento.')}
  five=false;entered=false;
 }catch(e){status.textContent='ERRO DE DADOS';if(announce&&voiceEnabled)speak('Não foi possível concluir a análise. Aguarde.')}
}
async function perf(){try{const p=await get('/performance');wins.textContent=p.wins;losses.textContent=p.losses;accuracy.textContent=p.accuracy+'%'}catch(e){}}
async function rad(){try{const a=await get('/radar?interval='+encodeURIComponent(interval.value));radar.innerHTML=a.map(x=>`<div><b>${x.symbol}</b><br><span class="${x.direction==='CALL'?'call':x.direction==='PUT'?'put':'neutral'}">${x.direction}</span> • ${x.confidence}%<br><small>${x.status}</small></div>`).join('')}catch(e){}}
async function lic(){try{const x=await get('/license');license.textContent=x.active?`● LICENÇA ATIVA • ${x.expires} • ${x.days_remaining} dias restantes`:`● LICENÇA EXPIRADA • ${x.whatsapp_1} / ${x.whatsapp_2} • ${x.instagram}`}catch(e){}}
async function clk(){try{const x=await get('/server-time');clock.textContent=ft(x.datetime)+' • Brasília'}catch(e){}}
function cd(){if(!cur)return;const n=Math.ceil((new Date(cur.entry_time)-Date.now())/1000);countdown.textContent=n>0?'Entrada em '+n+'s':'Entrada liberada';if(n===5&&!five){five=true;if(voiceEnabled)speak('Atenção. Entrada em 5 segundos.')}if(n<=0&&n>-2&&!entered){entered=true;if(voiceEnabled&&cur.direction!=='NEUTRO')speak('Entrada liberada. '+cur.direction+' agora.')}}
async function resultCheck(){if(!cur||cur.direction==='NEUTRO')return;try{const x=await get(`/result?symbol=${encodeURIComponent(cur.symbol)}&interval=${cur.interval}&direction=${cur.direction}&expiry_time=${encodeURIComponent(cur.expiry_time)}`);if(x.result){result.textContent=x.result;const k=cur.symbol+'|'+cur.expiry_time;if(k!==reskey){reskey=k;if(voiceEnabled)speak('Operação finalizada. Resultado '+x.result+'.')}perf()}}catch(e){}}
S.onchange=()=>{lastSignalVoice='';sig(true);rad()};interval.onchange=()=>{lastSignalVoice='';sig(true);rad()};
sig(false);perf();rad();lic();clk();setInterval(()=>sig(false),5000);setInterval(perf,5000);setInterval(rad,90000);setInterval(resultCheck,3000);setInterval(clk,1000);setInterval(cd,250);
</script></body></html>'''


@app.get("/", response_class=HTMLResponse)
async def home():
    return HTMLResponse(HTML_PAGE)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
