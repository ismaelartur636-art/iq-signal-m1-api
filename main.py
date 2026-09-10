import os
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

app = FastAPI(
    title="Ismael Trade",
    version="6.0.0"
)

# ============================================================
# CONFIGURAÇÃO
# ============================================================

KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()

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


# ============================================================
# PÁGINA ISMAEL TRADE
# ============================================================

HTML_PAGE = """
<!DOCTYPE html>
<html lang="pt-BR">

<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">

<title>Ismael Trade</title>

<style>

*{
    box-sizing:border-box;
}

body{
    margin:0;
    font-family:Arial,sans-serif;
    background:#080d18;
    color:white;
}

.container{
    max-width:560px;
    margin:auto;
    padding:18px;
}

.title{
    text-align:center;
    font-size:32px;
    font-weight:bold;
    margin-top:10px;
}

.subtitle{
    text-align:center;
    color:#94a3b8;
    margin:5px 0 20px;
}

.card{
    background:#111827;
    border-radius:18px;
    padding:18px;
    margin-bottom:15px;
    box-shadow:0 5px 20px rgba(0,0,0,.25);
}

label{
    display:block;
    margin-bottom:7px;
    color:#cbd5e1;
    font-weight:bold;
}

select,
button{
    width:100%;
    padding:14px;
    border:0;
    border-radius:12px;
    font-size:16px;
    margin-bottom:15px;
}

select{
    background:#1e293b;
    color:white;
}

button{
    background:#2563eb;
    color:white;
    font-weight:bold;
    cursor:pointer;
}

button:active{
    transform:scale(.98);
}

.signal{
    text-align:center;
    font-size:42px;
    font-weight:bold;
    padding:24px;
    border-radius:15px;
    background:#1e293b;
}

.call{
    color:#22c55e;
}

.put{
    color:#ef4444;
}

.wait{
    color:#facc15;
}

.info{
    display:flex;
    justify-content:space-between;
    padding:10px 0;
    border-bottom:1px solid #273449;
}

.info:last-child{
    border-bottom:0;
}

.value{
    font-weight:bold;
}

.loading{
    text-align:center;
    color:#60a5fa;
    display:none;
    margin-top:5px;
}

.warning{
    font-size:12px;
    color:#94a3b8;
    line-height:1.6;
    text-align:center;
}

.time{
    text-align:center;
    color:#94a3b8;
    font-size:13px;
    margin-bottom:10px;
}

.assetrow{
    display:flex;
    justify-content:space-between;
    background:#1e293b;
    padding:12px;
    border-radius:10px;
    margin:6px 0;
}

.green{
    color:#22c55e;
}

.red{
    color:#ef4444;
}

.yellow{
    color:#facc15;
}

.rowtitle{
    font-weight:bold;
    margin-bottom:10px;
}

</style>

</head>

<body>

<div class="container">

<div class="title">
ISMAEL TRADE
</div>

<div class="subtitle">
Sistema de análise probabilística M1
</div>


<div class="card">

<label>Ativo</label>

<select id="symbol">

<option value="ALL">
TODOS OS ATIVOS
</option>

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


<div id="singleCard" class="card">

<div id="signal" class="signal wait">
WAIT
</div>

</div>


<div id="details" class="card">

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


<div id="referenceCard" class="card">

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


<div id="allCard" class="card" style="display:none">

<div class="rowtitle">
Resultado dos ativos
</div>

<div id="allResults"></div>

</div>


<div class="card">

<div class="warning">

⚠️ O sinal é probabilístico e não garante WIN.

<br><br>

A fonte de dados pode apresentar diferenças em relação aos ativos OTC da IQ Option.

<br><br>

Este sistema não executa operações automaticamente.

</div>

</div>


<div class="time" id="updated">
Aguardando análise...
</div>

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


async function getSignal(symbol, interval){

    const url =
        "/signal?symbol="
        + encodeURIComponent(symbol)
        + "&interval="
        + encodeURIComponent(interval);

    const response = await fetch(url);

    let data;

    try{
        data = await response.json();
    }catch{
        throw new Error("Resposta inválida da API.");
    }

    if(!response.ok){

        throw new Error(
            data.detail || "Erro na análise."
        );

    }

    return data;
}


function mostrarResultado(data){

    const signal =
        document.getElementById("signal");

    signal.innerText =
        data.signal || "WAIT";

    signal.className =
        "signal " +
        (
            data.signal === "CALL"
            ? "call"
            : data.signal === "PUT"
            ? "put"
            : "wait"
        );


    document.getElementById("confidence").innerText =
        data.confidence != null
        ? data.confidence + "%"
        : "--";


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
}


async function analisar(){

    const symbol =
        document.getElementById("symbol").value;

    const interval =
        document.getElementById("interval").value;

    const loading =
        document.getElementById("loading");

    loading.style.display = "block";


    try{

        if(symbol === "ALL"){

            document.getElementById("singleCard").style.display =
                "none";

            document.getElementById("details").style.display =
                "none";

            document.getElementById("referenceCard").style.display =
                "none";

            document.getElementById("allCard").style.display =
                "block";


            const box =
                document.getElementById("allResults");

            box.innerHTML =
                "Carregando...";


            const results =
                await Promise.all(

                    ASSETS.map(async asset => {

                        try{

                            return await getSignal(
                                asset,
                                interval
                            );

                        }catch(error){

                            return {
                                symbol:asset,
                                signal:"ERRO",
                                confidence:0
                            };

                        }

                    })

                );


            box.innerHTML =
                results.map(data => {

                    const cls =
                        data.signal === "CALL"
                        ? "green"
                        : data.signal === "PUT"
                        ? "red"
                        : "yellow";


                    return `
                    <div class="assetrow">
                        <span>${data.symbol}</span>
                        <b class="${cls}">
                            ${data.signal}
                            ${data.confidence || 0}%
                        </b>
                    </div>
                    `;

                }).join("");


        }else{

            document.getElementById("singleCard").style.display =
                "block";

            document.getElementById("details").style.display =
                "block";

            document.getElementById("referenceCard").style.display =
                "block";

            document.getElementById("allCard").style.display =
                "none";


            const data =
                await getSignal(
                    symbol,
                    interval
                );

            mostrarResultado(data);

        }


        document.getElementById("updated").innerText =
            "Última análise: "
            + new Date().toLocaleTimeString("pt-BR");


    }catch(error){

        alert(
            error.message ||
            "Erro desconhecido."
        );

    }finally{

        loading.style.display =
            "none";

    }

}


analisar();

setInterval(
    analisar,
    60000
);

</script>

</body>

</html>
"""


# ============================================================
# ROTAS BÁSICAS
# ============================================================

@app.get("/", response_class=HTMLResponse)
def root():
    return HTML_PAGE


@app.get("/health")
def health():

    return {
        "ok": True,
        "app": "Ismael Trade",
        "version": "6.0.0"
    }


@app.get("/server-time")
def server_time():

    return {
        "utc":
            datetime.now(
                timezone.utc
            ).isoformat()
    }


# ============================================================
# DADOS DE MERCADO
# ============================================================

async def get_candles(
    symbol: str,
    interval: str,
    size: int = 200
):

    if not KEY:

        raise HTTPException(
            status_code=500,
            detail=
            "TWELVE_DATA_API_KEY não configurada no Render."
        )


    if interval not in ALLOWED_INTERVALS:

        raise HTTPException(
            status_code=400,
            detail=
            "Intervalo inválido. Use 1min, 5min, 15min ou 30min."
        )


    if symbol not in ASSETS:

        raise HTTPException(
            status_code=400,
            detail="Ativo não suportado."
        )


    outputsize = max(
        100,
        min(int(size), 500)
    )


    params = {

        "symbol": symbol,

        "interval": interval,

        "outputsize": outputsize,

        "apikey": KEY,

        "timezone":
            "America/Sao_Paulo",

        "format": "JSON"
    }


    try:

        async with httpx.AsyncClient(
            timeout=20
        ) as client:

            response = await client.get(
                URL,
                params=params
            )

            response.raise_for_status()

            data: dict[str, Any] =
                response.json()


    except httpx.HTTPError as exc:

        raise HTTPException(
            status_code=502,
            detail=
            f"Erro ao consultar a fonte de dados: {exc}"
        ) from exc


    if data.get("status") == "error":

        raise HTTPException(
            status_code=502,
            detail=
            data.get(
                "message",
                "Erro retornado pela Twelve Data."
            )
        )


    values = data.get(
        "values",
        []
    )


    if not values:

        raise HTTPException(
            status_code=502,
            detail=
            "A fonte de dados não retornou candles."
        )


    return values


@app.get("/candles")
async def candles(
    symbol: str = "EUR/USD",
    interval: str = "1min",
    outputsize: int = 100
):

    values =
        await get_candles(
            symbol,
            interval,
            outputsize
        )


    return {

        "ok": True,

        "source":
            "Twelve Data",

        "symbol":
            symbol,

        "interval":
            interval,

        "count":
            len(values),

        "values":
            values
    }


# ============================================================
# EMA
# ============================================================

def ema(
    values,
    period
):

    if len(values) < period:
        return None


    alpha =
        2.0 / (
            period + 1.0
        )


    value =
        sum(
            values[:period]
        ) / period


    for price in values[period:]:

        value =
            price * alpha \
            + value * (
                1.0 - alpha
            )


    return value


# ============================================================
# RSI
# ============================================================

def rsi(
    values,
    period=14
):

    if len(values) < period + 1:
        return None


    gains = [

        max(
            values[i] -
            values[i - 1],
            0.0
        )

        for i in range(
            1,
            len(values)
        )

    ]


    losses = [

        max(
            values[i - 1] -
            values[i],
            0.0
        )

        for i in range(
            1,
            len(values)
        )

    ]


    average_gain =
        sum(
            gains[:period]
        ) / period


    average_loss =
        sum(
            losses[:period]
        ) / period


    for i in range(
        period,
        len(gains)
    ):

        average_gain =
            (
                (period - 1)
                * average_gain
                + gains[i]
            ) / period


        average_loss =
            (
                (period - 1)
                * average_loss
                + losses[i]
            ) / period


    if average_loss == 0:

        return 100.0


    rs =
        average_gain /
        average_loss


    return (
        100.0 -
        100.0 /
        (1.0 + rs)
    )


# ============================================================
# ADX
# ============================================================

def adx(
    values,
    period=14
):

    if len(values) < (
        2 * period + 2
    ):

        return None


    data =
        list(
            reversed(values)
        )


    highs = [
        float(x["high"])
        for x in data
    ]

    lows = [
        float(x["low"])
        for x in data
    ]

    closes = [
        float(x["close"])
        for x in data
    ]


    tr = []
    plus_dm = []
    minus_dm = []


    for i in range(
        1,
        len(closes)
    ):

        true_range = max(

            highs[i] -
            lows[i],

            abs(
                highs[i] -
                closes[i - 1]
            ),

            abs(
                lows[i] -
                closes[i - 1]
            )

        )


        up_move =
            highs[i] -
            highs[i - 1]


        down_move =
            lows[i - 1] -
            lows[i]


        plus_dm.append(

            up_move
            if (
                up_move >
                down_move
                and
                up_move > 0
            )
            else 0.0

        )


        minus_dm.append(

            down_move
            if (
                down_move >
                up_move
                and
                down_move > 0
            )
            else 0.0

        )


        tr.append(
            true_range
        )


    atr =
        sum(
            tr[:period]
        ) / period


    plus =
        sum(
            plus_dm[:period]
        ) / period


    minus =
        sum(
            minus_dm[:period]
        ) / period


    dx = []


    for i in range(
        period,
        len(tr)
    ):

        atr =
            (
                (period - 1)
                * atr
                + tr[i]
            ) / period


        plus =
            (
                (period - 1)
                * plus
                + plus_dm[i]
            ) / period


        minus =
            (
                (period - 1)
                * minus
                + minus_dm[i]
            ) / period


        plus_di =
            100.0 * plus / atr \
            if atr else 0.0


        minus_di =
            100.0 * minus / atr \
            if atr else 0.0


        total =
            plus_di +
            minus_di


        dx.append(

            100.0 *
            abs(
                plus_di -
                minus_di
            ) / total

            if total
            else 0.0

        )


    if len(dx) < period:
        return None


    value =
        sum(
            dx[:period]
        ) / period


    for item in dx[period:]:

        value =
            (
                (period - 1)
                * value
                + item
            ) / period


    return value


# ============================================================
# ANÁLISE ISMAEL TRADE
# ============================================================

def analyze(values):

    if len(values) < 100:

        return {

            "signal":
                "WAIT",

            "confidence":
                0,

            "reason":
                "Dados insuficientes."
        }


    # A Twelve Data normalmente entrega
    # a vela mais recente primeiro.
    #
    # Ignoramos a vela mais recente para
    # reduzir sinais baseados em vela ainda aberta.

    closed =
        values[1:]


    closes = [

        float(
            candle["close"]
        )

        for candle in reversed(
            closed
        )

    ]


    ema3 =
        ema(
            closes,
            3
        )


    ema7 =
        ema(
            closes,
            7
        )


    rsi14 =
        rsi(
            closes,
            14
        )


    adx21 =
        adx(
            closed,
            21
        )


    adx48 =
        adx(
            closed,
            48
        )


    if None in (
        ema3,
        ema7,
        rsi14,
        adx21,
        adx48
    ):

        return {

            "signal":
                "WAIT",

            "confidence":
                0,

            "reason":
                "Dados insuficientes para os filtros."
        }


    call_score = 0
    put_score = 0


    # --------------------------------------------------------
    # EMA 3 x EMA 7
    # --------------------------------------------------------

    if ema3 > ema7:

        call_score += 2

    elif ema3 < ema7:

        put_score += 2


    # --------------------------------------------------------
    # RSI
    # --------------------------------------------------------

    if 52 <= rsi14 < 70:

        call_score += 1

    elif 30 < rsi14 <= 48:

        put_score += 1


    # --------------------------------------------------------
    # ADX 21
    # --------------------------------------------------------

    if adx21 >= 20:

        if ema3 > ema7:

            call_score += 1

        elif ema3 < ema7:

            put_score += 1


    # --------------------------------------------------------
    # ADX 48
    # --------------------------------------------------------

    if adx48 >= 20:

        if ema3 > ema7:

            call_score += 1

        elif ema3 < ema7:

            put_score += 1


    # --------------------------------------------------------
    # DECISÃO
    # --------------------------------------------------------

    if (
        call_score >= 4
        and
        call_score > put_score
    ):

        signal = "CALL"

        score = call_score


    elif (
        put_score >= 4
        and
        put_score > call_score
    ):

        signal = "PUT"

        score = put_score


    else:

        signal = "WAIT"

        score =
            max(
                call_score,
                put_score
            )


    # --------------------------------------------------------
    # CONFIANÇA
    # --------------------------------------------------------

    if signal != "WAIT":

        confidence =
            min(
                95,
                50 + score * 8
            )

    else:

        confidence =
            50 + score * 3


    return {

        "signal":
            signal,

        "confidence":
            confidence,

        "reference_candle":
            closed[0].get(
                "datetime"
            ),

        "next_candle":
            values[0].get(
                "datetime"
            ),

        "ema3":
            round(
                ema3,
                6
            ),

        "ema7":
            round(
                ema7,
                6
            ),

        "rsi14":
            round(
                rsi14,
                2
            ),

        "adx21":
            round(
                adx21,
                2
            ),

        "adx48":
            round(
                adx48,
                2
            ),

        "call_score":
            call_score,

        "put_score":
            put_score,

        "non_repaint_reference":
            True
    }


# ============================================================
# ENDPOINT DE SINAL
# ============================================================

@app.get("/signal")
async def signal(
    symbol: str = "EUR/USD",
    interval: str = "1min"
):

    if symbol not in ASSETS:

        raise HTTPException(
            status_code=400,
            detail="Ativo não suportado."
        )


    if interval not in ALLOWED_INTERVALS:

        raise HTTPException(
            status_code=400,
            detail=
            "Intervalo inválido."
        )


    values =
        await get_candles(
            symbol,
            interval,
            200
        )


    result =
        analyze(
            values
        )


    result.update({

        "ok":
            True,

        "source":
            "Twelve Data",

        "symbol":
            symbol,

        "interval":
            interval,

        "expiry":
            "1 vela do intervalo selecionado",

        "warning":
            "Sinal probabilístico; não garante WIN. "
            "A fonte Twelve Data pode não coincidir "
            "com os preços OTC da IQ Option."
    })


    return result