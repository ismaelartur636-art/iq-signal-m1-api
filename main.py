import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

# ============================================================
# ISMAEL TRADE - API DE SINAIS
# ============================================================

APP_NAME = "Ismael Trade"
APP_VERSION = "8.1.2"

KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
BASE_URL = "https://api.twelvedata.com/time_series"

SP_TZ = ZoneInfo("America/Sao_Paulo")

# ============================================================
# LICENÇA
# ============================================================

LICENSE_EXPIRES = (
    os.getenv("LICENSE_EXPIRES", "").strip()
    or "2026-12-31"
)

WHATSAPP_1 = "5584998411282"
WHATSAPP_2 = "5584994499442"
INSTAGRAM = "Ismaelartur26"

# ============================================================
# CONFIGURAÇÕES
# ============================================================

ALLOWED_INTERVALS = {
    "1min": 1,
    "5min": 5,
    "15min": 15,
    "30min": 30,
}

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

app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION
)


# ============================================================
# TEMPO
# ============================================================

def now_sp() -> datetime:
    return datetime.now(SP_TZ)


def parse_time(value: str) -> datetime:
    value = value.strip()

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    dt = datetime.fromisoformat(value)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=SP_TZ)

    return dt.astimezone(SP_TZ)


def interval_minutes(interval: str) -> int:
    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(
            status_code=400,
            detail="Intervalo inválido."
        )

    return ALLOWED_INTERVALS[interval]


# ============================================================
# LICENÇA
# ============================================================

def license_status() -> Dict[str, Any]:
    try:
        expiry = datetime.strptime(
            LICENSE_EXPIRES,
            "%Y-%m-%d"
        ).date()
    except ValueError:
        expiry = datetime(2026, 12, 31).date()

    today = now_sp().date()

    active = today <= expiry

    if active:
        days_remaining = (expiry - today).days
    else:
        days_remaining = 0

    return {
        "active": active,
        "expires": expiry.isoformat(),
        "expires_br": expiry.strftime("%d/%m/%Y"),
        "days_remaining": days_remaining,
        "whatsapp_1": WHATSAPP_1,
        "whatsapp_2": WHATSAPP_2,
        "instagram": INSTAGRAM,
    }


def require_license() -> None:
    status = license_status()

    if not status["active"]:
        raise HTTPException(
            status_code=403,
            detail={
                "error": "LICENÇA EXPIRADA",
                "message": (
                    "A licença do Ismael Trade expirou. "
                    "Entre em contato para renovar."
                ),
                "expires": status["expires_br"],
                "whatsapp_1": WHATSAPP_1,
                "whatsapp_2": WHATSAPP_2,
                "instagram": INSTAGRAM,
            }
        )


# ============================================================
# TWELVE DATA
# ============================================================

async def get_candles(
    symbol: str,
    interval: str,
    outputsize: int = 150
) -> List[Dict[str, Any]]:

    require_license()

    if not KEY:
        raise HTTPException(
            status_code=500,
            detail="TWELVE_DATA_API_KEY não configurada no Render."
        )

    if symbol not in SYMBOLS:
        raise HTTPException(
            status_code=400,
            detail="Ativo não permitido."
        )

    interval_minutes(interval)

    outputsize = max(20, min(int(outputsize), 500))

    params = {
        "symbol": SYMBOLS[symbol],
        "interval": interval,
        "outputsize": outputsize,
        "timezone": "America/Sao_Paulo",
        "apikey": KEY,
        "order": "ASC",
    }

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(
                BASE_URL,
                params=params
            )

        response.raise_for_status()
        data = response.json()

    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Erro ao consultar Twelve Data: {exc}"
        )

    if not isinstance(data, dict):
        raise HTTPException(
            status_code=502,
            detail="Resposta inválida da Twelve Data."
        )

    if data.get("status") == "error":
        raise HTTPException(
            status_code=502,
            detail=data.get(
                "message",
                "Erro retornado pela Twelve Data."
            )
        )

    raw_values = data.get("values")

    if not isinstance(raw_values, list):
        raise HTTPException(
            status_code=502,
            detail="Nenhuma vela recebida da Twelve Data."
        )

    candles: List[Dict[str, Any]] = []

    for item in raw_values:
        try:
            candles.append({
                "datetime": str(item["datetime"]),
                "open": float(item["open"]),
                "high": float(item["high"]),
                "low": float(item["low"]),
                "close": float(item["close"]),
                "volume": float(item.get("volume", 0) or 0),
            })
        except (KeyError, TypeError, ValueError):
            continue

    candles.sort(
        key=lambda x: parse_time(x["datetime"])
    )

    if len(candles) < 20:
        raise HTTPException(
            status_code=502,
            detail="Quantidade insuficiente de candles."
        )

    return candles


# ============================================================
# INDICADORES
# ============================================================

def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []

    alpha = 2.0 / (period + 1.0)

    result = [values[0]]

    for value in values[1:]:
        result.append(
            alpha * value
            + (1.0 - alpha) * result[-1]
        )

    return result


def rsi(values: List[float], period: int = 14) -> List[float]:
    if len(values) < period + 1:
        return [50.0] * len(values)

    gains: List[float] = []
    losses: List[float] = []

    for i in range(1, len(values)):
        change = values[i] - values[i - 1]

        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    result = [50.0] * period

    if avg_loss == 0:
        result.append(100.0)
    else:
        rs = avg_gain / avg_loss
        result.append(100.0 - (100.0 / (1.0 + rs)))

    for i in range(period, len(gains)):
        avg_gain = (
            (avg_gain * (period - 1))
            + gains[i]
        ) / period

        avg_loss = (
            (avg_loss * (period - 1))
            + losses[i]
        ) / period

        if avg_loss == 0:
            value = 100.0
        else:
            rs = avg_gain / avg_loss
            value = 100.0 - (100.0 / (1.0 + rs))

        result.append(value)

    while len(result) < len(values):
        result.insert(0, 50.0)

    return result[-len(values):]


def true_ranges(
    candles: List[Dict[str, Any]]
) -> List[float]:

    if not candles:
        return []

    result = [
        candles[0]["high"] - candles[0]["low"]
    ]

    for i in range(1, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        previous_close = candles[i - 1]["close"]

        tr = max(
            high - low,
            abs(high - previous_close),
            abs(low - previous_close)
        )

        result.append(tr)

    return result


def adx(
    candles: List[Dict[str, Any]],
    period: int
) -> List[float]:

    if len(candles) < period + 2:
        return [0.0] * len(candles)

    tr = true_ranges(candles)

    plus_dm = [0.0]
    minus_dm = [0.0]

    for i in range(1, len(candles)):
        up_move = (
            candles[i]["high"]
            - candles[i - 1]["high"]
        )

        down_move = (
            candles[i - 1]["low"]
            - candles[i]["low"]
        )

        if up_move > down_move and up_move > 0:
            plus_dm.append(up_move)
        else:
            plus_dm.append(0.0)

        if down_move > up_move and down_move > 0:
            minus_dm.append(down_move)
        else:
            minus_dm.append(0.0)

    def smooth(
        values: List[float],
        p: int
    ) -> List[float]:

        result = [0.0] * len(values)

        if len(values) <= p:
            return result

        current = sum(values[1:p + 1])
        result[p] = current

        for i in range(p + 1, len(values)):
            current = (
                current
                - current / p
                + values[i]
            )
            result[i] = current

        return result

    tr_s = smooth(tr, period)
    plus_s = smooth(plus_dm, period)
    minus_s = smooth(minus_dm, period)

    dx = [0.0] * len(candles)

    for i in range(period, len(candles)):
        if tr_s[i] <= 0:
            continue

        plus_di = (
            100.0 * plus_s[i] / tr_s[i]
        )

        minus_di = (
            100.0 * minus_s[i] / tr_s[i]
        )

        denominator = plus_di + minus_di

        if denominator > 0:
            dx[i] = (
                100.0
                * abs(plus_di - minus_di)
                / denominator
            )

    result = [0.0] * len(candles)

    if len(candles) <= period * 2:
        return result

    initial = period * 2

    result[initial] = (
        sum(dx[period:initial + 1])
        / period
    )

    for i in range(initial + 1, len(candles)):
        result[i] = (
            (
                result[i - 1]
                * (period - 1)
            )
            + dx[i]
        ) / period

    return result


# ============================================================
# CANDLE FECHADO
# ============================================================

def candle_is_closed(
    candle_time: datetime,
    interval: str
) -> bool:

    minutes = interval_minutes(interval)

    close_time = (
        candle_time
        + timedelta(minutes=minutes)
    )

    return now_sp() >= close_time


def get_closed_candles(
    candles: List[Dict[str, Any]],
    interval: str
) -> List[Dict[str, Any]]:

    closed = []

    for candle in candles:
        try:
            candle_time = parse_time(
                candle["datetime"]
            )

            if candle_is_closed(
                candle_time,
                interval
            ):
                closed.append(candle)

        except Exception:
            continue

    return closed


# ============================================================
# ANÁLISE SNIPER
# ============================================================

def analyze(
    candles: List[Dict[str, Any]],
    interval: str
) -> Dict[str, Any]:

    closed = get_closed_candles(
        candles,
        interval
    )

    if len(closed) < 60:
        raise HTTPException(
            status_code=422,
            detail=(
                "Ainda não existem candles fechados "
                "suficientes para análise."
            )
        )

    closes = [
        float(c["close"])
        for c in closed
    ]

    ema3 = ema(closes, 3)
    ema7 = ema(closes, 7)

    rsi14 = rsi(closes, 14)

    adx21 = adx(closed, 21)
    adx48 = adx(closed, 48)

    i = len(closed) - 1

    price = closes[i]

    e3 = ema3[i]
    e7 = ema7[i]

    r = rsi14[i]

    a21 = adx21[i]
    a48 = adx48[i]

    call_score = 0
    put_score = 0

    # --------------------------------------------------------
    # EMA
    # --------------------------------------------------------

    if e3 > e7:
        call_score += 2
    elif e3 < e7:
        put_score += 2

    # --------------------------------------------------------
    # RSI
    # --------------------------------------------------------

    if r >= 50:
        call_score += 1
    else:
        put_score += 1

    # --------------------------------------------------------
    # ADX 21
    # --------------------------------------------------------

    if a21 >= 20:
        if e3 > e7:
            call_score += 1
        elif e3 < e7:
            put_score += 1

    # --------------------------------------------------------
    # ADX 48
    # --------------------------------------------------------

    if a48 >= 20:
        if e3 > e7:
            call_score += 1
        elif e3 < e7:
            put_score += 1

    # --------------------------------------------------------
    # ZONAS EXTREMAS RSI
    # --------------------------------------------------------

    if r < 30:
        call_score += 1

    if r > 70:
        put_score += 1

    # --------------------------------------------------------
    # DECISÃO
    # --------------------------------------------------------

    difference = abs(
        call_score - put_score
    )

    if (
        call_score >= 4
        and call_score > put_score
    ):
        direction = "CALL"

    elif (
        put_score >= 4
        and put_score > call_score
    ):
        direction = "PUT"

    else:
        direction = "NEUTRO"

    if direction == "NEUTRO":
        confidence = 50

    else:
        confidence = min(
            95,
            70 + (difference * 5)
        )

    reference = closed[-1]

    reference_time = parse_time(
        reference["datetime"]
    )

    next_time = (
        reference_time
        + timedelta(
            minutes=interval_minutes(interval)
        )
    )

    if direction == "CALL":
        reason = (
            "Tendência de alta confirmada por "
            "EMA3/EMA7, com confirmação de RSI/ADX."
        )

    elif direction == "PUT":
        reason = (
            "Tendência de baixa confirmada por "
            "EMA3/EMA7, com confirmação de RSI/ADX."
        )

    else:
        reason = (
            "Mercado sem confirmação suficiente "
            "para uma entrada."
        )

    return {
        "signal": direction,
        "confidence": confidence,
        "strategy": "SNIPER",
        "non_repaint": True,
        "signal_mode": "CLOSED_CANDLE",

        "reference_candle": reference["datetime"],

        "next_candle": next_time.isoformat(),

        "price": price,

        "scores": {
            "call": call_score,
            "put": put_score,
        },

        "indicators": {
            "EMA3": round(e3, 6),
            "EMA7": round(e7, 6),
            "RSI14": round(r, 2),
            "ADX21": round(a21, 2),
            "ADX48": round(a48, 2),
            "ADX_LEVEL": 20,
        },

        "reason": reason,
    }


# ============================================================
# TEMPO RESTANTE
# ============================================================

def seconds_to_next_candle(
    interval: str
) -> int:

    minutes = interval_minutes(interval)

    now = now_sp()

    elapsed = (
        (now.minute % minutes) * 60
        + now.second
    )

    total = minutes * 60

    remaining = total - elapsed

    if remaining < 0:
        remaining = 0

    return remaining


# ============================================================
# PÁGINA PRINCIPAL
# ============================================================

HTML = r"""
<!DOCTYPE html>
<html lang="pt-BR">

<head>

<meta charset="UTF-8">

<meta name="viewport"
content="width=device-width,initial-scale=1">

<title>Ismael Trade</title>

<style>

*{
    box-sizing:border-box;
}

body{
    margin:0;
    background:#08111f;
    color:#fff;
    font-family:Arial,sans-serif;
}

.container{
    width:100%;
    max-width:900px;
    margin:auto;
    padding:15px;
}

.header{
    text-align:center;
    padding:15px;
    background:#0d1b2d;
    border-radius:15px;
    margin-bottom:15px;
}

.header h1{
    margin:0;
    font-size:30px;
}

.header p{
    margin:6px 0 0;
    color:#9fb1c7;
}

.card{
    background:#0d1b2d;
    border-radius:15px;
    padding:15px;
    margin-bottom:15px;
}

label{
    display:block;
    margin-bottom:6px;
    color:#aabbd0;
}

select,
button{
    width:100%;
    padding:13px;
    border:0;
    border-radius:10px;
    margin-bottom:10px;
    font-size:16px;
}

select{
    background:#172942;
    color:#fff;
}

button{
    background:#18a957;
    color:#fff;
    font-weight:bold;
}

button.secondary{
    background:#34465e;
}

.signal{
    text-align:center;
    font-size:44px;
    font-weight:bold;
    padding:25px;
    border-radius:15px;
    background:#172942;
    margin:10px 0;
}

.call{
    color:#31e67a;
}

.put{
    color:#ff5964;
}

.neutral{
    color:#ffc857;
}

.grid{
    display:grid;
    grid-template-columns:
    repeat(2,1fr);
    gap:10px;
}

.stat{
    background:#172942;
    padding:14px;
    border-radius:10px;
    text-align:center;
}

.stat b{
    display:block;
    font-size:23px;
    margin-top:5px;
}

.timer{
    text-align:center;
    font-size:28px;
    font-weight:bold;
    margin:10px;
}

.license{
    text-align:center;
    padding:12px;
    border-radius:10px;
    background:#102b20;
    color:#6dffad;
}

.license.expired{
    background:#40151b;
    color:#ff6975;
}

.small{
    color:#9fb1c7;
    font-size:13px;
    line-height:1.5;
}

.overlay{
    position:fixed;
    inset:0;
    background:rgba(0,0,0,.92);
    display:none;
    align-items:center;
    justify-content:center;
    padding:20px;
    z-index:9999;
}

.overlay-box{
    width:100%;
    max-width:430px;
    background:#101c2e;
    padding:25px;
    border-radius:18px;
    text-align:center;
}

.overlay-box h2{
    color:#ff5964;
}

.contact{
    background:#172942;
    border-radius:10px;
    padding:12px;
    margin:8px 0;
}

</style>

</head>

<body>

<div id="licenseOverlay"
class="overlay">

<div class="overlay-box">

<h2>LICENÇA EXPIRADA</h2>

<p>
A licença do Ismael Trade expirou.
</p>

<div class="contact">
WhatsApp:<br>
<strong>+55 84 99841-1282</strong>
</div>

<div class="contact">
WhatsApp:<br>
<strong>+55 84 99449-9442</strong>
</div>

<div class="contact">
Instagram:<br>
<strong>@Ismaelartur26</strong>
</div>

</div>

</div>

<div class="container">

<div class="header">

<h1>ISMAEL TRADE</h1>

<p>
Sistema de análise M1 • M5 • M15 • M30
</p>

</div>

<div id="license"
class="license">
Verificando licença...
</div>

<div class="card">

<label>Ativo</label>

<select id="symbol">

<option>EUR/USD</option>
<option>GBP/USD</option>
<option>USD/JPY</option>
<option>AUD/USD</option>
<option>USD/CAD</option>
<option>USD/CHF</option>
<option>NZD/USD</option>
<option>EUR/JPY</option>
<option>GBP/JPY</option>
<option>EUR/GBP</option>
<option>BTC/USD</option>
<option>ETH/USD</option>

</select>

<label>Período</label>

<select id="interval">

<option value="1min">M1</option>
<option value="5min">M5</option>
<option value="15min">M15</option>
<option value="30min">M30</option>

</select>

<button onclick="loadSignal()">
ANALISAR AGORA
</button>

</div>

<div class="card">

<div id="signal"
class="signal neutral">
AGUARDANDO
</div>

<div class="timer"
id="timer">
--:--
</div>

<div class="grid">

<div class="stat">
Confiança
<b id="confidence">--%</b>
</div>

<div class="stat">
Estratégia
<b id="strategy">SNIPER</b>
</div>

<div class="stat">
WIN
<b id="wins">0</b>
</div>

<div class="stat">
LOSS
<b id="losses">0</b>
</div>

<div class="stat">
Assertividade
<b id="accuracy">0%</b>
</div>

<div class="stat">
Status
<b id="status">ONLINE</b>
</div>

</div>

</div>

<div class="card">

<p>
<strong>Referência:</strong>
<span id="reference">--</span>
</p>

<p>
<strong>Próxima vela:</strong>
<span id="next">--</span>
</p>

<p>
<strong>Motivo:</strong>
<span id="reason">--</span>
</p>

<p class="small">
O sistema utiliza candles fechados para
evitar alteração retroativa do sinal.
</p>

</div>

<div class="card">

<button class="secondary"
onclick="resetStats()">
RESETAR WIN/LOSS
</button>

</div>

</div>

<script>

let wins =
Number(localStorage.getItem("it_wins") || 0);

let losses =
Number(localStorage.getItem("it_losses") || 0);

let pending =
JSON.parse(
localStorage.getItem("it_pending") || "null"
);

let secondsRemaining = 0;

function updateStats(){

    document.getElementById("wins")
        .innerText = wins;

    document.getElementById("losses")
        .innerText = losses;

    let total = wins + losses;

    let acc = total > 0
        ? ((wins / total) * 100).toFixed(1)
        : "0";

    document.getElementById("accuracy")
        .innerText = acc + "%";

    localStorage.setItem(
        "it_wins",
        wins
    );

    localStorage.setItem(
        "it_losses",
        losses
    );
}

function resetStats(){

    wins = 0;
    losses = 0;
    pending = null;

    localStorage.removeItem(
        "it_pending"
    );

    updateStats();
}

function formatTimer(sec){

    sec = Math.max(0, sec);

    let m = Math.floor(sec / 60);
    let s = sec % 60;

    return String(m).padStart(2,"0")
        + ":"
        + String(s).padStart(2,"0");
}

async function checkLicense(){

    try{

        const response =
            await fetch("/license");

        const data =
            await response.json();

        const box =
            document.getElementById(
                "license"
            );

        if(data.active){

            box.className =
                "license";

            box.innerText =
                "LICENÇA ATIVA • VÁLIDA ATÉ "
                + data.expires_br;

            document.getElementById(
                "licenseOverlay"
            ).style.display = "none";

        }else{

            box.className =
                "license expired";

            box.innerText =
                "LICENÇA EXPIRADA";

            document.getElementById(
                "licenseOverlay"
            ).style.display = "flex";
        }

    }catch(e){

        document.getElementById(
            "status"
        ).innerText = "ERRO";
    }
}

async function loadSignal(){

    try{

        const symbol =
            document.getElementById(
                "symbol"
            ).value;

        const interval =
            document.getElementById(
                "interval"
            ).value;

        const response =
            await fetch(
                "/signal?symbol="
                + encodeURIComponent(symbol)
                + "&interval="
                + encodeURIComponent(interval)
            );

        if(!response.ok){

            let err = await response.json();

            alert(
                err.detail?.message
                || err.detail
                || "Erro na análise."
            );

            return;
        }

        const data =
            await response.json();

        const signal =
            document.getElementById(
                "signal"
            );

        signal.innerText =
            data.signal;

        signal.className =
            "signal "
            + (
                data.signal === "CALL"
                ? "call"
                : data.signal === "PUT"
                ? "put"
                : "neutral"
            );

        document.getElementById(
            "confidence"
        ).innerText =
            data.confidence + "%";

        document.getElementById(
            "strategy"
        ).innerText =
            data.strategy;

        document.getElementById(
            "reference"
        ).innerText =
            data.reference_candle;

        document.getElementById(
            "next"
        ).innerText =
            data.next_candle;

        document.getElementById(
            "reason"
        ).innerText =
            data.reason;

        if(data.signal !== "NEUTRO"){

            pending = {
                symbol: symbol,
                interval: interval,
                reference_candle:
                    data.reference_candle,
                direction:
                    data.signal
            };

            localStorage.setItem(
                "it_pending",
                JSON.stringify(pending)
            );
        }

        startTimer(interval);

    }catch(e){

        document.getElementById(
            "status"
        ).innerText =
            "ERRO";

        console.error(e);
    }
}

async function checkResult(){

    if(!pending){
        return;
    }

    try{

        const url =
            "/result"
            + "?symbol="
            + encodeURIComponent(
                pending.symbol
            )
            + "&interval="
            + encodeURIComponent(
                pending.interval
            )
            + "&reference_candle="
            + encodeURIComponent(
                pending.reference_candle
            )
            + "&direction="
            + encodeURIComponent(
                pending.direction
            );

        const response =
            await fetch(url);

        if(!response.ok){
            return;
        }

        const data =
            await response.json();

        if(data.status === "PENDING"){
            return;
        }

        if(data.result === "WIN"){
            wins++;
        }

        if(data.result === "LOSS"){
            losses++;
        }

        pending = null;

        localStorage.removeItem(
            "it_pending"
        );

        updateStats();

    }catch(e){
        console.log(e);
    }
}

async function startTimer(interval){

    try{

        const response =
            await fetch(
                "/server-time"
            );

        const data =
            await response.json();

        const now =
            new Date(
                data.brasilia
            );

        const minutes =
            interval === "1min"
            ? 1
            : interval === "5min"
            ? 5
            : interval === "15min"
            ? 15
            : 30;

        const elapsed =
            (
                now.getMinutes()
                % minutes
            ) * 60
            + now.getSeconds();

        secondsRemaining =
            minutes * 60
            - elapsed;

    }catch(e){
        secondsRemaining = 0;
    }
}

setInterval(function(){

    if(secondsRemaining > 0){
        secondsRemaining--;
    }

    document.getElementById(
        "timer"
    ).innerText =
        formatTimer(
            secondsRemaining
        );

},1000);

setInterval(
    loadSignal,
    60000
);

setInterval(
    checkResult,
    15000
);

checkLicense();
updateStats();
loadSignal();
checkResult();

</script>

</body>

</html>
"""


# ============================================================
# ROTAS
# ============================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
async def home():

    return HTML


@app.get("/health")
async def health():

    return {
        "ok": True,
        "app": APP_NAME,
        "version": APP_VERSION,
        "time_brasilia": now_sp().isoformat(),
    }


@app.get("/server-time")
async def server_time():

    current = now_sp()

    return {
        "brasilia": current.isoformat(),
        "utc": datetime.now(
            timezone.utc
        ).isoformat(),
    }


@app.get("/license")
async def license():

    return license_status()


@app.get("/candles")
async def candles(
    symbol: str = "EUR/USD",
    interval: str = "1min",
    size: int = 120
):

    values = await get_candles(
        symbol,
        interval,
        size
    )

    return {
        "app": APP_NAME,
        "symbol": symbol,
        "interval": interval,
        "count": len(values),
        "candles": values,
    }


@app.get("/signal")
async def signal(
    symbol: str = "EUR/USD",
    interval: str = "1min"
):

    require_license()

    values = await get_candles(
        symbol,
        interval,
        200
    )

    result = analyze(
        values,
        interval
    )

    result.update({
        "app": APP_NAME,
        "version": APP_VERSION,
        "source": "Twelve Data",
        "symbol": symbol,
        "interval": interval,
        "expiry": "1 candle",
        "warning": (
            "Sinal baseado em candle fechado. "
            "Não é garantia de resultado."
        ),
        "seconds_to_next_candle":
            seconds_to_next_candle(interval),
    })

    return result


# ============================================================
# RESULTADO DA OPERAÇÃO
# ============================================================

@app.get("/result")
async def result(
    symbol: str,
    interval: str,
    reference_candle: str,
    direction: str
):

    require_license()

    direction = direction.upper().strip()

    if direction not in {"CALL", "PUT"}:
        raise HTTPException(
            status_code=400,
            detail="Direção deve ser CALL ou PUT."
        )

    values = await get_candles(
        symbol,
        interval,
        200
    )

    reference_time = parse_time(
        reference_candle
    )

    reference_index = None

    for index, candle in enumerate(values):

        try:
            candle_time = parse_time(
                candle["datetime"]
            )

            if candle_time == reference_time:
                reference_index = index
                break

        except Exception:
            continue

    if reference_index is None:

        return {
            "status": "PENDING",
            "message":
                "Candle de referência ainda não encontrado."
        }

    next_index = reference_index + 1

    if next_index >= len(values):

        return {
            "status": "PENDING",
            "message":
                "Candle de resultado ainda não disponível."
        }

    reference = values[
        reference_index
    ]

    next_candle = values[
        next_index
    ]

    next_time = parse_time(
        next_candle["datetime"]
    )

    if not candle_is_closed(
        next_time,
        interval
    ):

        return {
            "status": "PENDING",
            "message":
                "Candle de resultado ainda está aberto.",
            "reference_candle":
                reference["datetime"],
            "next_candle":
                next_candle["datetime"],
        }

    # --------------------------------------------------------
    # RESULTADO:
    # entrada considerada no fechamento
    # do candle de referência.
    # --------------------------------------------------------

    entry_price = float(
        reference["close"]
    )

    result_price = float(
        next_candle["close"]
    )

    if result_price == entry_price:

        operation_result = "DRAW"

    elif direction == "CALL":

        operation_result = (
            "WIN"
            if result_price > entry_price
            else "LOSS"
        )

    else:

        operation_result = (
            "WIN"
            if result_price < entry_price
            else "LOSS"
        )

    return {
        "status": "DONE",
        "result": operation_result,
        "direction": direction,

        "reference_candle":
            reference["datetime"],

        "result_candle":
            next_candle["datetime"],

        "entry_price":
            entry_price,

        "result_price":
            result_price,
    }


# ============================================================
# EXECUÇÃO LOCAL
# ============================================================

if __name__ == "__main__":

    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(
            os.getenv(
                "PORT",
                "8000"
            )
        )
    )