import os
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

app = FastAPI(
    title="IQ Signal M1/M5/M15/M30 API",
    version="4.0.0"
)

KEY = os.getenv("TWELVE_DATA_API_KEY", "")
URL = "https://api.twelvedata.com/time_series"

ALLOWED_INTERVALS = {
    "1min",
    "5min",
    "15min",
    "30min"
}


HTML_PAGE = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport"
      content="width=device-width, initial-scale=1.0">

<title>IQ Signal M1</title>

<style>
* {
    box-sizing: border-box;
}

body {
    margin: 0;
    font-family: Arial, sans-serif;
    background: #0b1020;
    color: white;
}

.container {
    max-width: 500px;
    margin: auto;
    padding: 18px;
}

.title {
    text-align: center;
    font-size: 28px;
    font-weight: bold;
    margin-bottom: 5px;
}

.subtitle {
    text-align: center;
    color: #9ca3af;
    margin-bottom: 20px;
}

.card {
    background: #151c31;
    border-radius: 18px;
    padding: 18px;
    margin-bottom: 15px;
    box-shadow: 0 8px 25px rgba(0,0,0,.25);
}

label {
    display: block;
    margin-bottom: 7px;
    color: #cbd5e1;
    font-weight: bold;
}

select,
button {
    width: 100%;
    padding: 14px;
    border-radius: 12px;
    border: none;
    font-size: 16px;
    margin-bottom: 15px;
}

select {
    background: #222b45;
    color: white;
}

button {
    background: #2563eb;
    color: white;
    font-weight: bold;
    cursor: pointer;
}

button:active {
    transform: scale(.98);
}

.signal {
    text-align: center;
    font-size: 42px;
    font-weight: bold;
    padding: 20px;
    border-radius: 15px;
    background: #222b45;
}

.call {
    color: #22c55e;
}

.put {
    color: #ef4444;
}

.wait {
    color: #facc15;
}

.info {
    display: flex;
    justify-content: space-between;
    padding: 9px 0;
    border-bottom: 1px solid #27314b;
}

.info:last-child {
    border-bottom: none;
}

.value {
    font-weight: bold;
}

.warning {
    font-size: 12px;
    color: #9ca3af;
    line-height: 1.5;
    text-align: center;
}

.loading {
    text-align: center;
    color: #60a5fa;
    display: none;
}

.time {
    text-align: center;
    color: #9ca3af;
    font-size: 13px;
}
</style>
</head>

<body>

<div class="container">

<div class="title">
IQ SIGNAL M1
</div>

<div class="subtitle">
Análise probabilística
</div>

<div class="card">

<label>Ativo</label>

<select id="symbol">
<option value="EUR/USD">EUR/USD</option>
<option value="GBP/USD">GBP/USD</option>
<option value="USD/JPY">USD/JPY</option>
<option value="BTC/USD">BTC/USD</option>
<option value="ETH/USD">ETH/USD</option>
</select>

<label>Tempo gráfico</label>

<select id="interval">
<option value="1min">M1</option>
<option value="5min">M5</option>
<option value="15min">M15</option>
<option value="30min">M30</option>
</select>

<button onclick="analisar()">
ANALISAR AGORA
</button>

<div id="loading" class="loading">
Analisando mercado...
</div>

</div>

<div class="card">

<div id="signal" class="signal wait">
WAIT
</div>

</div>

<div class="card">

<div class="info">
<span>Confiança</span>
<span id="confidence" class="value">--</span>
</div>

<div class="info">
<span>EMA 3</span>
<span id="ema3" class="value">--</span>
</div>

<div class="info">
<span>EMA 7</span>
<span id="ema7" class="value">--</span>
</div>

<div class="info">
<span>RSI 14</span>
<span id="rsi" class="value">--</span>
</div>

<div class="info">
<span>ADX 21</span>
<span id="adx21" class="value">--</span>
</div>

<div class="info">
<span>ADX 48</span>
<span id="adx48" class="value">--</span>
</div>

</div>

<div class="card">

<div class="info">
<span>Vela de referência</span>
<span id="reference" class="value">--</span>
</div>

<div class="info">
<span>Próxima vela</span>
<span id="next" class="value">--</span>
</div>

<div class="info">
<span>Ativo</span>
<span id="asset" class="value">--</span>
</div>

<div class="info">
<span>Timeframe</span>
<span id="tf" class="value">--</span>
</div>

</div>

<div class="card">

<div class="warning">
⚠️ Sinal probabilístico; não garante WIN.
<br><br>
A fonte de dados pode não coincidir com os preços OTC
da IQ Option.
<br><br>
Este sistema não executa operações automaticamente.
</div>

</div>

<div class="time" id="updated">
Aguardando análise...
</div>

</div>

<script>

async function analisar() {

    const symbol =
        document.getElementById("symbol").value;

    const interval =
        document.getElementById("interval").value;

    const loading =
        document.getElementById("loading");

    loading.style.display = "block";

    try {

        const response =
            await fetch(
                "/signal?symbol=" +
                encodeURIComponent(symbol) +
                "&interval=" +
                interval
            );

        const data = await response.json();

        if (!response.ok) {
            throw new Error(
                data.detail || "Erro na análise."
            );
        }

        mostrarResultado(data);

    } catch (error) {

        document.getElementById("signal").innerText =
            "ERRO";

        document.getElementById("signal").className =
            "signal wait";

        alert(error.message);

    } finally {

        loading.style.display = "none";
    }
}


function mostrarResultado(data) {

    const signal =
        document.getElementById("signal");

    signal.innerText = data.signal;

    signal.className = "signal";

    if (data.signal === "CALL") {
        signal.classList.add("call");
    }
    else if (data.signal === "PUT") {
        signal.classList.add("put");
    }
    else {
        signal.classList.add("wait");
    }

    document.getElementById("confidence").innerText =
        data.confidence + "%";

    document.getElementById("ema3").innerText =
        data.ema3 ?? "--";

    document.getElementById("ema7").innerText =
        data.ema7 ?? "--";

    document.getElementById("rsi").innerText =
        data.rsi14 ?? "--";

    document.getElementById("adx21").innerText =
        data.adx21 ?? "--";

    document.getElementById("adx48").innerText =
        data.adx48 ?? "--";

    document.getElementById("reference").innerText =
        data.reference_candle ?? "--";

    document.getElementById("next").innerText =
        data.next_candle ?? "--";

    document.getElementById("asset").innerText =
        data.symbol ?? "--";

    document.getElementById("tf").innerText =
        data.interval ?? "--";

    document.getElementById("updated").innerText =
        "Última análise: " +
        new Date().toLocaleTimeString("pt-BR");
}

@app.get("/health")
def health():
    return {"ok": True}


async def get_candles(
    symbol: str,
    interval: str,
    size: int = 200
):
    if not KEY:
        raise HTTPException(
            500,
            "TWELVE_DATA_API_KEY não configurada no Render."
        )

    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(
            400,
            "Intervalo inválido. Use 1min, 5min, 15min ou 30min."
        )

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
            response = await client.get(
                URL,
                params=params
            )

            response.raise_for_status()

            data: dict[str, Any] = response.json()

    except httpx.HTTPError as exc:
        raise HTTPException(
            502,
            f"Erro ao consultar a fonte de dados: {exc}"
        ) from exc

    if data.get("status") == "error":
        raise HTTPException(
            502,
            data.get(
                "message",
                "Erro retornado pela Twelve Data."
            )
        )

    values = data.get("values", [])

    if not values:
        raise HTTPException(
            502,
            "A fonte de dados não retornou candles."
        )

    return values


@app.get("/candles")
async def candles(
    symbol: str = "EUR/USD",
    interval: str = "1min",
    outputsize: int = 100
):
    values = await get_candles(
        symbol,
        interval,
        outputsize
    )

    return {
        "ok": True,
        "source": "Twelve Data",
        "symbol": symbol,
        "interval": interval,
        "count": len(values),
        "values": values
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

    gains = [
        max(x[i] - x[i - 1], 0.0)
        for i in range(1, len(x))
    ]

    losses = [
        max(x[i - 1] - x[i], 0.0)
        for i in range(1, len(x))
    ]

    ag = sum(gains[:p]) / p
    al = sum(losses[:p]) / p

    for i in range(p, len(gains)):

        ag = ((p - 1) * ag + gains[i]) / p
        al = ((p - 1) * al + losses[i]) / p

    if al == 0:
        return 100.0

    return 100.0 - 100.0 / (1.0 + ag / al)


def adx(values, p=14):

    if len(values) < 2 * p + 2:
        return None

    v = list(reversed(values))

    h = [float(a["high"]) for a in v]
    l = [float(a["low"]) for a in v]
    c = [float(a["close"]) for a in v]

    tr = []
    plus = []
    minus = []

    for i in range(1, len(c)):

        tr.append(
            max(
                h[i] - l[i],
                abs(h[i] - c[i - 1]),
                abs(l[i] - c[i - 1])
            )
        )

        up = h[i] - h[i - 1]
        down = l[i - 1] - l[i]

        plus.append(
            up if up > down and up > 0 else 0.0
        )

        minus.append(
            down if down > up and down > 0 else 0.0
        )

    atr = sum(tr[:p]) / p
    pp = sum(plus[:p]) / p
    mm = sum(minus[:p]) / p

    dx = []

    for i in range(p, len(tr)):

        atr = ((p - 1) * atr + tr[i]) / p
        pp = ((p - 1) * pp + plus[i]) / p
        mm = ((p - 1) * mm + minus[i]) / p

        pi = 100.0 * pp / atr if atr else 0.0
        mi = 100.0 * mm / atr if atr else 0.0

        if pi + mi:
            dx.append(
                100.0 * abs(pi - mi) / (pi + mi)
            )
        else:
            dx.append(0.0)

    if len(dx) < p:
        return None

    value = sum(dx[:p]) / p

    for z in dx[p:]:
        value = (
            ((p - 1) * value + z) / p
        )

    return value


def analyze(values):

    if len(values) < 100:
        return {
            "signal": "WAIT",
            "confidence": 0,
            "reason": "Dados insuficientes."
        }

    # Ignora a vela mais recente.
    # Usa somente a última vela fechada.
    closed = values[1:]

    x = [
        float(a["close"])
        for a in reversed(closed)
    ]

    e3 = ema(x, 3)
    e7 = ema(x, 7)

    rr = rsi(x, 14)

    a21 = adx(closed, 21)
    a48 = adx(closed, 48)

    if None in (e3, e7, rr, a21, a48):
        return {
            "signal": "WAIT",
            "confidence": 0,
            "reason": "Dados insuficientes para os filtros."
        }

    call = 0
    put = 0

    # EMA 3 x EMA 7
    if e3 > e7:
        call += 2

    elif e3 < e7:
        put += 2

    # RSI
    if 52 <= rr < 70:
        call += 1

    elif 30 < rr <= 48:
        put += 1

    # ADX 21
    if a21 >= 20:

        if e3 > e7:
            call += 1

        elif e3 < e7:
            put += 1

    # ADX 48
    if a48 >= 20:

        if e3 > e7:
            call += 1

        elif e3 < e7:
            put += 1

    # Resultado
    if call >= 4 and call > put:

        signal = "CALL"
        score = call

    elif put >= 4 and put > call:

        signal = "PUT"
        score = put

    else:

        signal = "WAIT"
        score = max(call, put)

    if signal != "WAIT":
        confidence = min(
            95,
            50 + score * 8
        )
    else:
        confidence = 50 + score * 3

    return {
        "signal": signal,
        "confidence": confidence,
        "reference_candle": closed[0].get(
            "datetime"
        ),
        "next_candle": values[0].get(
            "datetime"
        ),
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
async def signal(
    symbol: str = "EUR/USD",
    interval: str = "1
analisar();

setInterval(analisar, 60000);

</script>

</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def root():
    return HTML_PAGE