import os
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

app = FastAPI(
    title="Ismael Trade",
    version="5.0.0"
)

KEY = os.getenv(
    "TWELVE_DATA_API_KEY",
    ""
)

URL = "https://api.twelvedata.com/time_series"

ALLOWED_INTERVALS = {
    "1min",
    "5min",
    "15min",
    "30min"
}

ASSETS = [
    "EUR/USD",
    "GBP/USD",
    "USD/JPY",
    "AUD/USD",
    "USD/CAD",
    "USD/CHF",
    "NZD/USD",
    "EUR/JPY",
    "GBP/JPY",
    "EUR/GBP",
    "BTC/USD",
    "ETH/USD"
]
HTML_PAGE = """
<!DOCTYPE html>
<html lang="pt-BR">

<head>

<meta charset="UTF-8">

<meta name="viewport"
content="width=device-width, initial-scale=1.0">

<title>ISMAEL TRADE</title>

<style>

body {
    margin: 0;
    padding: 15px;
    background: #111827;
    color: white;
    font-family: Arial, sans-serif;
}

.container {
    max-width: 600px;
    margin: auto;
}

h1 {
    text-align: center;
    margin-bottom: 5px;
}

.subtitle {
    text-align: center;
    color: #9ca3af;
    margin-bottom: 20px;
}

label {
    display: block;
    margin-top: 12px;
    margin-bottom: 5px;
}

select,
button {
    width: 100%;
    padding: 13px;
    border-radius: 8px;
    border: none;
    font-size: 16px;
}

select {
    background: #1f2937;
    color: white;
}

button {
    margin-top: 18px;
    background: #2563eb;
    color: white;
    font-weight: bold;
}

button:active {
    transform: scale(0.98);
}

#loading {
    display: none;
    text-align: center;
    margin: 15px;
}

.card {
    background: #1f2937;
    border-radius: 12px;
    padding: 15px;
    margin-top: 15px;
}

.signal {
    text-align: center;
    font-size: 42px;
    font-weight: bold;
    padding: 20px;
    border-radius: 10px;
}

.call {
    background: #065f46;
}

.put {
    background: #991b1b;
}

.wait {
    background: #374151;
}

.grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 10px;
}

.item {
    background: #111827;
    padding: 10px;
    border-radius: 8px;
}

.value {
    font-weight: bold;
    margin-top: 5px;
}

.all-item {
    background: #111827;
    padding: 12px;
    margin-bottom: 8px;
    border-radius: 8px;
}

.mini-call {
    color: #34d399;
    font-weight: bold;
}

.mini-put {
    color: #f87171;
    font-weight: bold;
}

.mini-wait {
    color: #9ca3af;
    font-weight: bold;
}

.warning {
    color: #fbbf24;
    font-size: 13px;
    line-height: 1.5;
}

</style>

</head>

<body>

<div class="container">

<h1>ISMAEL TRADE</h1>

<div class="subtitle">
Análise probabilística de mercado
</div>

<label>Ativo</label>

<select id="asset">

<option value="TODOS">
TODOS OS ATIVOS
</option>

<option value="EUR/USD">
EUR/USD
</option>

<option value="GBP/USD">
GBP/USD
</option>

<option value="USD/JPY">
USD/JPY
</option>

<option value="AUD/USD">
AUD/USD
</option>

<option value="USD/CAD">
USD/CAD
</option>

<option value="USD/CHF">
USD/CHF
</option>

<option value="NZD/USD">
NZD/USD
</option>

<option value="EUR/JPY">
EUR/JPY
</option>

<option value="GBP/JPY">
GBP/JPY
</option>

<option value="EUR/GBP">
EUR/GBP
</option>

<option value="BTC/USD">
BTC/USD
</option>

<option value="ETH/USD">
ETH/USD
</option>

</select>

<label>Timeframe</label>

<select id="timeframe">

<option value="1min">
M1
</option>

<option value="5min">
M5
</option>

<option value="15min">
M15
</option>

<option value="30min">
M30
</option>

</select>

<button id="analyzeBtn">
ANALISAR AGORA
</button>

<div id="loading">
Analisando mercado...
</div>

<div class="card" id="singleResult">

<div id="signal"
class="signal wait">
AGUARDANDO
</div>

<div class="grid">

<div class="item">
Confiança
<div class="value" id="confidence">--</div>
</div>

<div class="item">
Ativo
<div class="value" id="assetName">--</div>
</div>

<div class="item">
Timeframe
<div class="value" id="intervalName">--</div>
</div>

<div class="item">
Próxima vela
<div class="value" id="next">--</div>
</div>

</div>

</div>


<div class="card">

<h3>Indicadores</h3>

<div class="grid">

<div class="item">
EMA 3
<div class="value" id="ema3">--</div>
</div>

<div class="item">
EMA 7
<div class="value" id="ema7">--</div>
</div>

<div class="item">
RSI 14
<div class="value" id="rsi">--</div>
</div>

<div class="item">
ADX 21
<div class="value" id="adx21">--</div>
</div>

<div class="item">
ADX 48
<div class="value" id="adx48">--</div>
</div>

</div>

</div>


<div class="card">

<h3>Referência da análise</h3>

<div class="grid">

<div class="item">
Vela de referência
<div class="value" id="reference">--</div>
</div>

<div class="item">
Próxima vela
<div class="value" id="next">--</div>
</div>

</div>

</div>


<div class="card"
id="allResult"
style="display:none;">

<h3>Análise de todos os ativos</h3>

<div id="allResults"></div>

</div>


<div class="card warning">

<b>Atenção:</b><br><br>

Este sistema apresenta uma análise
probabilística baseada em dados de mercado.

O sinal não garante WIN e pode ocorrer LOSS.

Os dados da Twelve Data podem ser
diferentes dos dados da IQ Option,
principalmente em ativos OTC.

O sistema não executa operações
automaticamente.

</div>


<div class="card">

Última atualização:

<span id="updated">
--
</span>

</div>
<script>

const ASSETS = [
    "EUR/USD",
    "GBP/USD",
    "USD/JPY",
    "AUD/USD",
    "USD/CAD",
    "USD/CHF",
    "NZD/USD",
    "EUR/JPY",
    "GBP/JPY",
    "EUR/GBP",
    "BTC/USD",
    "ETH/USD"
];


async function getSignal(symbol, interval) {

    const url =
        "/signal?symbol=" +
        encodeURIComponent(symbol) +
        "&interval=" +
        interval;

    const response =
        await fetch(url);

    const data =
        await response.json();

    if (!response.ok) {
        throw new Error(
            data.detail ||
            "Erro na análise."
        );
    }

    return data;
}


function mostrarResultado(data) {

    const signal =
        document.getElementById("signal");

    signal.innerText =
        data.signal || "WAIT";

    if (data.signal === "CALL") {

        signal.className =
            "signal call";

    } else if (data.signal === "PUT") {

        signal.className =
            "signal put";

    } else {

        signal.className =
            "signal wait";
    }


    document.getElementById(
        "confidence"
    ).innerText =
        (data.confidence ?? "--") + "%";


    document.getElementById(
        "ema3"
    ).innerText =
        data.ema3 ?? "--";


    document.getElementById(
        "ema7"
    ).innerText =
        data.ema7 ?? "--";


    document.getElementById(
        "rsi"
    ).innerText =
        data.rsi14 ?? "--";


    document.getElementById(
        "adx21"
    ).innerText =
        data.adx21 ?? "--";


    document.getElementById(
        "adx48"
    ).innerText =
        data.adx48 ?? "--";
}
async function analisar() {

    const symbol =
        document.getElementById("asset").value;

    const interval =
        document.getElementById("timeframe").value;

    const loading =
        document.getElementById("loading");

    loading.style.display = "block";


    try {

        if (symbol === "TODOS") {

            const resultados = [];

            for (const ativo of ASSETS) {

                try {

                    const data =
                        await getSignal(
                            ativo,
                            interval
                        );

                    resultados.push(data);

                } catch (error) {

                    console.log(
                        "Erro em " + ativo,
                        error
                    );
                }
            }


            const container =
                document.getElementById(
                    "allResults"
                );

            container.innerHTML = "";


            resultados.forEach(data => {

                const item =
                    document.createElement("div");

                item.className =
                    "all-item";


                let classe =
                    "mini-wait";

                if (data.signal === "CALL") {
                    classe = "mini-call";
                }

                if (data.signal === "PUT") {
                    classe = "mini-put";
                }


                item.innerHTML =
                    "<b>" +
                    data.symbol +
                    "</b><br>" +

                    "<span class='" +
                    classe +
                    "'>" +
                    data.signal +
                    "</span> " +

                    data.confidence +
                    "%";


                container.appendChild(item);

            });


            document.getElementById(
                "singleResult"
            ).style.display = "none";


            document.getElementById(
                "allResult"
            ).style.display = "block";


        } else {

            const data =
                await getSignal(
                    symbol,
                    interval
                );


            mostrarResultado(data);


            document.getElementById(
                "singleResult"
            ).style.display = "block";


            document.getElementById(
                "allResult"
            ).style.display = "none";


            document.getElementById(
                "reference"
            ).innerText =
                data.reference_candle ?? "--";


            document.getElementById(
                "next"
            ).innerText =
                data.next_candle ?? "--";


            document.getElementById(
                "assetName"
            ).innerText =
                data.symbol ?? symbol;


            document.getElementById(
                "intervalName"
            ).innerText =
                data.interval ?? interval;

        }


        document.getElementById(
            "updated"
        ).innerText =
            new Date().toLocaleTimeString(
                "pt-BR"
            );


    } catch (error) {

        alert(
            error.message ||
            "Erro ao realizar análise."
        );

    } finally {

        loading.style.display = "none";
    }
}
document
    .getElementById("analyzeBtn")
    .addEventListener(
        "click",
        analisar
    );


document
    .getElementById("asset")
    .addEventListener(
        "change",
        function() {

            const todos =
                this.value === "TODOS";

            document.getElementById(
                "singleResult"
            ).style.display =
                todos ? "none" : "block";

            document.getElementById(
                "allResult"
            ).style.display =
                todos ? "block" : "none";
        }
    );


analisar();


setInterval(
    analisar,
    60000
);

</script>

</body>

</html>
"""
def ema(values, period):
    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    result = sum(values[:period]) / period

    for price in values[period:]:
        result = (
            (price - result) * multiplier
        ) + result

    return result


def rsi(values, period=14):
    if len(values) <= period:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):
        change = values[i] - values[i - 1]

        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))

    avg_gain = sum(
        gains[:period]
    ) / period

    avg_loss = sum(
        losses[:period]
    ) / period

    for i in range(
        period,
        len(gains)
    ):
        avg_gain = (
            (avg_gain * (period - 1))
            + gains[i]
        ) / period

        avg_loss = (
            (avg_loss * (period - 1))
            + losses[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (
        100 / (1 + rs)
    )
def adx(values, period):
    if len(values) < period + 1:
        return None

    changes = []

    for i in range(1, len(values)):
        changes.append(
            abs(
                values[i] -
                values[i - 1]
            )
        )

    if len(changes) < period:
        return None

    return (
        sum(changes[-period:]) /
        period
    )


def format_value(value):
    if value is None:
        return None

    return round(
        float(value),
        5
    )


def calculate_signal(candles):
    if len(candles) < 60:
        raise HTTPException(
            status_code=502,
            detail="Dados insuficientes para análise."
        )

    closed = candles[1:]

    closes = [
        float(c["close"])
        for c in reversed(closed)
    ]

    ema3 = ema(
        closes,
        3
    )

    ema7 = ema(
        closes,
        7
    )

    rsi14 = rsi(
        closes,
        14
    )

    adx21 = adx(
        closes,
        21
    )

    adx48 = adx(
        closes,
        48
    )

    call_score = 0
    put_score = 0


    if ema3 is not None and ema7 is not None:

        if ema3 > ema7:
            call_score += 2

        elif ema3 < ema7:
            put_score += 2


    if rsi14 is not None:

        if rsi14 < 30:
            call_score += 2

        elif rsi14 > 70:
            put_score += 2

        elif rsi14 >= 50:
            call_score += 1

        else:
            put_score += 1


    if adx21 is not None:

        if adx21 >= 20:

            if ema3 > ema7:
                call_score += 1

            elif ema3 < ema7:
                put_score += 1


    if adx48 is not None:

        if adx48 >= 20:

            if ema3 > ema7:
                call_score += 1

            elif ema3 < ema7:
                put_score += 1
    if call_score >= 4 and call_score > put_score:

        signal = "CALL"

    elif put_score >= 4 and put_score > call_score:

        signal = "PUT"

    else:

        signal = "WAIT"


    total_score = max(
        call_score,
        put_score
    )

    confidence = 59 + (
        total_score * 5
    )

    if confidence > 95:
        confidence = 95


    reference = closed[0].get(
        "datetime",
        "--"
    )

    if len(closed) > 1:

        next_candle = closed[1].get(
            "datetime",
            "--"
        )

    else:

        next_candle = "--"


    return {
        "signal": signal,
        "confidence": confidence,
        "ema3": format_value(ema3),
        "ema7": format_value(ema7),
        "rsi14": format_value(rsi14),
        "adx21": format_value(adx21),
        "adx48": format_value(adx48),
        "reference_candle": reference,
        "next_candle": next_candle
    }
@app.get("/candles")
async def candles_endpoint(
    symbol: str = "EUR/USD",
    interval: str = "1min",
    outputsize: int = 100
):
    return await get_candles(
        symbol,
        interval,
        outputsize
    )


@app.get("/signal")
async def signal_endpoint(
    symbol: str = "EUR/USD",
    interval: str = "1min"
):
    if symbol not in ASSETS:
        raise HTTPException(
            status_code=400,
            detail="Ativo não permitido."
        )

    candles = await get_candles(
        symbol,
        interval,
        100
    )

    result = calculate_signal(
        candles
    )

    result["symbol"] = symbol
    result["interval"] = interval

    return result
@app.get("/")

async def root():

    return HTMLResponse(
        content=HTML_PAGE
    )