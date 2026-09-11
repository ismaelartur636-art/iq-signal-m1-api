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
APP_VERSION = "8.6.0"
KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
BASE_URL = "https://api.twelvedata.com/time_series"
SP_TZ = ZoneInfo("America/Sao_Paulo")

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
CACHE_TTL_SECONDS = 12.0
RATE_LIMIT_COOLDOWN_SECONDS = 20.0
CANDLE_CACHE: Dict[tuple, tuple] = {}
CANDLE_LOCKS: Dict[tuple, asyncio.Lock] = {}
RATE_LIMIT_UNTIL = 0.0


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
.sniperTitle{font-size:20px;font-weight:800;margin:4px 0}
.countdown{font-size:25px;font-weight:800}
.entry{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:12px}
.entryBox{background:#0f1526;border-radius:12px;padding:12px}
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
<div><div class="small">ESTRATÉGIA 1</div><div class="sniperTitle">SNIPER X</div><div class="small">Confluência entre RSI 9 e RSI 14</div></div>
<button id="rsiToggle" class="onlineBtn online" style="width:auto;margin:0">● ONLINE</button>
</div><div style="margin-top:10px" class="badge">RSI 9 + RSI 14 • CONFLUÊNCIA</div></div>
<div class="card sniper">
<div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap">
<div><div class="small">ESTRATÉGIA 2</div><div class="sniperTitle">SNIPER 01</div><div class="small">Price Action • 5 confirmações</div></div>
<button id="oldToggle" class="onlineBtn offline" style="width:auto;margin:0">● OFFLINE</button>
</div><div style="margin-top:10px" class="badge">Engolfo • Rejeição • Rompimento • Força • Estrutura</div></div>

<div class="card">
<div class="small">SINAL</div>
<div id="signal" class="signal neutral">AGUARDANDO</div>
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

<div class="card"><div class="small">CONFIGURAÇÃO DAS ESTRATÉGIAS</div><div class="params" style="margin-top:12px">
<div class="param"><span class="small">Estratégia ativa</span><div id="strategyStatus" class="value">SNIPER X</div></div><div class="param"><span class="small">Modo geral</span><div id="modeStatus" class="value">ONLINE</div></div>
<div class="param"><span class="small">RSI 9 atual</span><div id="rsi9" class="value">--</div></div><div class="param"><span class="small">RSI 14 atual</span><div id="rsi14" class="value">--</div></div>
<div class="param"><span class="small">Engolfo</span><div id="engulf" class="value">--</div></div><div class="param"><span class="small">Rejeição</span><div id="rejection" class="value">--</div></div>
<div class="param"><span class="small">Rompimento</span><div id="breakout" class="value">--</div></div><div class="param"><span class="small">Força / Estrutura</span><div id="strength" class="value">--</div></div>
</div></div>

<div class="card footer">
O sinal é probabilístico e não garante WIN. A análise usa velas fechadas para reduzir repintura. Os preços da Twelve Data podem apresentar diferenças em relação à cotação da sua corretora.
</div>
</div>

<script>
const $ = id => document.getElementById(id);
let pending = JSON.parse(localStorage.getItem("is_trade_pending") || "null");
let stats = JSON.parse(localStorage.getItem("is_trade_stats") || '{"wins":0,"losses":0}');
let paramsVisible = localStorage.getItem("is_trade_params") !== "hidden";
let isOnline = true;
let rsiOnline = localStorage.getItem("is_trade_rsi_online") !== "off";
let oldOnline = localStorage.getItem("is_trade_old_online") === "on";

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
function activeStrategy(){if(oldOnline)return "old_sniper";if(rsiOnline)return "rsi";return null;}
function renderMode(){setStrategyButton("rsiToggle",rsiOnline);setStrategyButton("oldToggle",oldOnline);const active=activeStrategy();$("modeStatus").textContent=isOnline&&active?"ONLINE":"OFFLINE";$("strategyStatus").textContent=active==="old_sniper"?"SNIPER 01":active==="rsi"?"SNIPER X":"NENHUMA";if(!isOnline||!active){$("signal").textContent="OFFLINE";$("signal").className="signal neutral";}}

function fmt(v){ return v == null ? "--" : v; }

let resultTimer = null;

function showError(message){
  $("signal").textContent = "ERRO";
  $("signal").className = "signal neutral";
  $("confidence").textContent = message;
}
function updateClock(){
  const now = new Date();
  const parts = new Intl.DateTimeFormat("pt-BR", {timeZone:"America/Sao_Paulo",hour:"2-digit",minute:"2-digit",second:"2-digit",hour12:false}).formatToParts(now);
  const get = k => parts.find(p => p.type === k)?.value || "00";
  $("clock").textContent = `${get("hour")}:${get("minute")}:${get("second")}`;
  $("dateBr").textContent = new Intl.DateTimeFormat("pt-BR", {timeZone:"America/Sao_Paulo",day:"2-digit",month:"2-digit",year:"numeric"}).format(now);
}
function updateCountdown(){
  const interval = $("interval").value;
  const minutes = ({"1min":1,"5min":5,"15min":15,"30min":30})[interval] || 1;
  const now = new Date();
  const ms = now.getTime();
  const block = minutes*60*1000;
  const next = Math.ceil(ms/block)*block;
  let left = Math.max(0, next-ms);
  const total = Math.floor(left/1000);
  const mm = String(Math.floor(total/60)).padStart(2,"0");
  const ss = String(total%60).padStart(2,"0");
  $("countdown").textContent = `${mm}:${ss}`;
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
    $("reference").textContent = d.reference_candle;
    $("next").textContent = d.next_candle;
    $("entryTime").textContent = d.next_candle;
    const nextDt = new Date(d.next_candle.replace(" ","T"));
    const mins = ({"1min":1,"5min":5,"15min":15,"30min":30})[interval] || 1;
    $("expiry").textContent = Number.isNaN(nextDt.getTime()) ? "--" : new Date(nextDt.getTime()+mins*60000).toLocaleString("pt-BR", {hour:"2-digit",minute:"2-digit",second:"2-digit"});
    $("rsi9").textContent = fmt(d.rsi9);
    $("rsi14").textContent = fmt(d.rsi14);
    if($("engulf")) $("engulf").textContent = d.bullish_engulfing || d.bearish_engulfing ? "SIM" : "NÃO";
    if($("rejection")) $("rejection").textContent = d.bullish_rejection || d.bearish_rejection ? "SIM" : "NÃO";
    if($("breakout")) $("breakout").textContent = d.bullish_breakout || d.bearish_breakout ? "SIM" : "NÃO";
    if($("strength")) $("strength").textContent = (d.bullish_strength || d.bearish_strength) ? "SIM" : "NÃO";

    if(d.signal !== "NEUTRO"){
      pending = {symbol, interval, reference_candle:d.reference_candle, direction:d.signal};
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


$("refresh").onclick = loadSignal;
$("rsiToggle").onclick=()=>{rsiOnline=!rsiOnline;if(rsiOnline){oldOnline=false;localStorage.setItem("is_trade_old_online","off");}localStorage.setItem("is_trade_rsi_online",rsiOnline?"on":"off");pending=null;save();renderMode();if(isOnline&&rsiOnline)loadSignal();};
$("oldToggle").onclick=()=>{oldOnline=!oldOnline;if(oldOnline){rsiOnline=false;localStorage.setItem("is_trade_rsi_online","off");}localStorage.setItem("is_trade_old_online",oldOnline?"on":"off");pending=null;save();renderMode();if(isOnline&&oldOnline)loadSignal();};
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
loadSignal();
if(pending) scheduleResultCheck();
// Uma análise automática por minuto. O cache do servidor evita chamadas duplicadas.
setInterval(() => {
  if(!pending) loadSignal();
}, 65000);
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


@app.get("/candles")
async def candles(
    symbol: str = "EUR/USD",
    interval: str = "1min",
    size: int = 120,
) -> Dict[str, Any]:
    requested_size = int(size)
    values = await get_candles(symbol, interval, requested_size)
    return {
        "ok": True,
        "source": "Twelve Data",
        "symbol": symbol,
        "interval": interval,
        "values": values,
    }


@app.get("/signal")
async def signal(
    symbol: str = "EUR/USD",
    interval: str = "1min",
    strategy: str = "rsi",
) -> Dict[str, Any]:
    if strategy not in {"rsi", "old_sniper"}:
        raise HTTPException(status_code=400, detail="Estratégia inválida.")
    values = await get_candles(symbol, interval, 100)
    result = analyze(values, strategy)
    result.update(
        {
            "source": "Twelve Data",
            "symbol": symbol,
            "interval": interval,
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
) -> Dict[str, Any]:
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

    next_index = reference_index + 1
    if next_index >= len(values):
        return {"ok": True, "status": "PENDING", "result": None}

    next_candle = values[next_index]
    next_time = parse_time(next_candle["datetime"])

    if not candle_is_closed(next_time, interval):
        return {
            "ok": True,
            "status": "PENDING",
            "result": None,
            "next_candle": next_candle["datetime"],
        }

    reference_close = values[reference_index]["close"]
    result_close = next_candle["close"]

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
        "result_candle": next_candle["datetime"],
        "entry_close": reference_close,
        "result_close": result_close,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))