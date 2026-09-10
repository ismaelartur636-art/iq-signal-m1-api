import os
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse


# =========================================================
# CONFIGURAÇÃO
# =========================================================

app = FastAPI(
    title="Ismael Trade",
    version="7.0.0"
)

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


# =========================================================
# HTML
# =========================================================

HTML_PAGE = r"""
<!DOCTYPE html>
<html lang="pt-BR">

<head>

<meta charset="UTF-8">

<meta name="viewport"
      content="width=device-width, initial-scale=1">

<title>Ismael Trade</title>

<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    font-family: Arial, Helvetica, sans-serif;
    background: #070b14;
    color: white;
}

.container {
    width: 100%;
    max-width: 620px;
    margin: auto;
    padding: 15px;
}

.title {
    text-align: center;
    font-size: 30px;
    font-weight: 900;
    margin-top: 10px;
    letter-spacing: 1px;
}

.subtitle {
    text-align: center;
    color: #94a3b8;
    margin: 5px 0 18px;
}

.card {
    background: #111827;
    border-radius: 18px;
    padding: 17px;
    margin-bottom: 15px;
    box-shadow: 0 5px 22px rgba(0,0,0,.25);
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
    border: 0;
    border-radius: 12px;
    padding: 14px;
    font-size: 16px;
    margin-bottom: 12px;
}

select {
    background: #1e293b;
    color: white;
}

button {
    background: #2563eb;
    color: white;
    font-weight: bold;
    cursor: pointer;
}

button:active {
    transform: scale(.99);
}

.secondary {
    background: #334155;
}

.danger {
    background: #7f1d1d;
}

.signal {
    text-align: center;
    font-size: 44px;
    font-weight: 900;
    padding: 25px 10px;
    border-radius: 15px;
    background: #1e293b;
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
    align-items: center;
    gap: 10px;
    padding: 11px 0;
    border-bottom: 1px solid #273449;
}

.info:last-child {
    border-bottom: 0;
}

.value {
    font-weight: bold;
    text-align: right;
}

.stats {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 8px;
}

.stat {
    background: #1e293b;
    border-radius: 14px;
    padding: 14px 6px;
    text-align: center;
}

.stat-title {
    color: #94a3b8;
    font-size: 12px;
    margin-bottom: 8px;
}

.stat-value {
    font-size: 25px;
    font-weight: 900;
}

.win {
    color: #22c55e;
}

.loss {
    color: #ef4444;
}

.accuracy {
    color: #38bdf8;
}

.params-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 10px;
    margin-bottom: 12px;
}

.params-title {
    font-size: 18px;
    font-weight: bold;
}

.toggle {
    width: auto;
    padding: 9px 12px;
    margin: 0;
    font-size: 13px;
    background: #475569;
}

.loading {
    display: none;
    text-align: center;
    color: #60a5fa;
    margin: 8px 0;
}

.warning {
    color: #94a3b8;
    font-size: 12px;
    line-height: 1.6;
    text-align: center;
}

.time {
    text-align: center;
    color: #64748b;
    font-size: 12px;
    margin: 8px 0 15px;
}

.assetrow {
    display: flex;
    justify-content: space-between;
    background: #1e293b;
    padding: 12px;
    border-radius: 10px;
    margin: 6px 0;
}

.green {
    color: #22c55e;
}

.red {
    color: #ef4444;
}

.yellow {
    color: #facc15;
}

.status {
    text-align: center;
    padding: 10px;
    border-radius: 10px;
    background: #0f172a;
    color: #94a3b8;
    font-size: 13px;
}

.result-box {
    text-align: center;
    padding: 14px;
    border-radius: 12px;
    background: #0f172a;
    font-weight: bold;
}

@media (max-width: 420px) {

    .title {
        font-size: 26px;
    }

    .signal {
        font-size: 38px;
    }

    .stat-value {
        font-size: 21px;
    }

}

</style>

</head>


<body>

<div class="container">

    <div class="title">
        ISMAEL TRADE
    </div>

    <div class="subtitle">
        Sistema de análise probabilística
    </div>


    <!-- CONTROLES -->

    <div class="card">

        <label>Ativo</label>

        <select id="symbol">

            <option value="ALL">
                TODOS OS ATIVOS
            </option>

            <option value="EUR/USD">EUR/USD</option>
            <option value="GBP/USD">GBP/USD</option>
            <option value="USD/JPY">USD/JPY</option>
            <option value="AUD/USD">AUD/USD</option>
            <option value="USD/CAD">USD/CAD</option>
            <option value="USD/CHF">USD/CHF</option>
            <option value="NZD/USD">NZD/USD</option>
            <option value="EUR/JPY">EUR/JPY</option>
            <option value="GBP/JPY">GBP/JPY</option>
            <option value="EUR/GBP">EUR/GBP</option>
            <option value="BTC/USD">BTC/USD</option>
            <option value="ETH/USD">ETH/USD</option>

        </select>


        <label>
            Tempo gráfico
        </label>

        <select id="interval">

            <option value="1min">M1</option>
            <option value="5min">M5</option>
            <option value="15min">M15</option>
            <option value="30min">M30</option>

        </select>


        <button onclick="analisar()">
            ANALISAR AGORA
        </button>


        <button
            class="secondary"
            onclick="toggleParametros()"
            id="toggleButton">
            ⚙️ PARÂMETROS: VISÍVEIS
        </button>


        <button
            class="danger"
            onclick="limparEstatisticas()">
            LIMPAR WIN/LOSS
        </button>


        <div id="loading" class="loading">
            Analisando mercado...
        </div>

    </div>


    <!-- SINAL -->

    <div id="singleCard" class="card">

        <div id="signal"
             class="signal wait">
            WAIT
        </div>

    </div>


    <!-- ESTATÍSTICAS -->

    <div class="card">

        <div class="params-title">
            📊 RESULTADOS
        </div>

        <br>

        <div class="stats">

            <div class="stat">

                <div class="stat-title">
                    WIN
                </div>

                <div id="wins"
                     class="stat-value win">
                    0
                </div>

            </div>


            <div class="stat">

                <div class="stat-title">
                    LOSS
                </div>

                <div id="losses"
                     class="stat-value loss">
                    0
                </div>

            </div>


            <div class="stat">

                <div class="stat-title">
                    ASSERTIVIDADE
                </div>

                <div id="accuracy"
                     class="stat-value accuracy">
                    --
                </div>

            </div>

        </div>

    </div>


    <!-- RESULTADO PENDENTE -->

    <div class="card">

        <div class="params-title">
            🎯 ÚLTIMO RESULTADO
        </div>

        <br>

        <div id="lastResult"
             class="result-box">
            Aguardando resultado...
        </div>

    </div>


    <!-- PARÂMETROS -->

    <div id="parametersCard"
         class="card">

        <div class="params-header">

            <div class="params-title">
                ⚙️ PARÂMETROS DOS INDICADORES
            </div>

            <button
                class="toggle"
                onclick="toggleParametros()"
                id="toggleButton2">
                OCULTAR
            </button>

        </div>


        <div class="info">

            <span>EMA 3</span>

            <span id="ema3"
                  class="value">
                --
            </span>

        </div>


        <div class="info">

            <span>EMA 7</span>

            <span id="ema7"
                  class="value">
                --
            </span>

        </div>


        <div class="info">

            <span>RSI 14</span>

            <span id="rsi"
                  class="value">
                --
            </span>

        </div>


        <div class="info">

            <span>ADX 21</span>

            <span id="adx21"
                  class="value">
                --
            </span>

        </div>


        <div class="info">

            <span>ADX 48</span>

            <span id="adx48"
                  class="value">
                --
            </span>

        </div>


        <div class="info">

            <span>CALL SCORE</span>

            <span id="callScore"
                  class="value">
                --
            </span>

        </div>


        <div class="info">

            <span>PUT SCORE</span>

            <span id="putScore"
                  class="value">
                --
            </span>

        </div>

    </div>


    <!-- REFERÊNCIA -->

    <div id="referenceCard"
         class="card">

        <div class="info">

            <span>
                Vela de referência
            </span>

            <span id="reference"
                  class="value">
                --
            </span>

        </div>


        <div class="info">

            <span>
                Próxima vela
            </span>

            <span id="next"
                  class="value">
                --
            </span>

        </div>


        <div class="info">

            <span>
                Entrada
            </span>

            <span id="entry"
                  class="value">
                --
            </span>

        </div>


        <div class="info">

            <span>
                Expiração
            </span>

            <span id="expiration"
                  class="value">
                1 vela
            </span>

        </div>


        <div class="info">

            <span>
                Ativo
            </span>

            <span id="asset"
                  class="value">
                --
            </span>

        </div>


        <div class="info">

            <span>
                Timeframe
            </span>

            <span id="tf"
                  class="value">
                --
            </span>

        </div>

    </div>


    <!-- TODOS OS ATIVOS -->

    <div id="allCard"
         class="card"
         style="display:none">

        <div class="params-title">
            📈 RESULTADO DOS ATIVOS
        </div>

        <br>

        <div id="allResults"></div>

    </div>


    <!-- STATUS -->

    <div class="card">

        <div id="status"
             class="status">
            Sistema pronto.
        </div>

    </div>


    <!-- AVISO -->

    <div class="card">

        <div class="warning">

            ⚠️ O sinal é probabilístico e não garante WIN.

            <br><br>

            Este sistema utiliza dados do mercado normal
            fornecidos pela Twelve Data.

            <br><br>

            Os resultados de WIN/LOSS são calculados
            com base no comportamento da vela seguinte
            recebido pela fonte de dados.

            <br><br>

            O sistema não executa operações automaticamente.

        </div>

    </div>


    <div id="updated"
         class="time">

        Aguardando análise...

    </div>

</div>


<script>


// =========================================================
// CONFIGURAÇÕES
// =========================================================

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

const STATS_KEY = "ismael_trade_stats_v7";

const PARAMS_KEY = "ismael_trade_params_visible_v7";

const PENDING_KEY = "ismael_trade_pending_v7";


// =========================================================
// ESTATÍSTICAS
// =========================================================

function getStats() {

    try {

        const data =
            JSON.parse(
                localStorage.getItem(STATS_KEY)
            );

        if (data) {
            return data;
        }

    } catch (error) {
        console.log(error);
    }

    return {
        wins: 0,
        losses: 0
    };
}


function saveStats(stats) {

    localStorage.setItem(
        STATS_KEY,
        JSON.stringify(stats)
    );

}


function updateStatsScreen() {

    const stats = getStats();

    document.getElementById(
        "wins"
    ).innerText = stats.wins;


    document.getElementById(
        "losses"
    ).innerText = stats.losses;


    const total =
        stats.wins + stats.losses;


    let accuracy = "--";


    if (total > 0) {

        accuracy =
            (
                stats.wins /
                total *
                100
            ).toFixed(1) + "%";

    }


    document.getElementById(
        "accuracy"
    ).innerText = accuracy;

}


function limparEstatisticas() {

    if (
        !confirm(
            "Deseja realmente zerar WIN e LOSS?"
        )
    ) {
        return;
    }


    localStorage.removeItem(
        STATS_KEY
    );

    localStorage.removeItem(
        PENDING_KEY
    );


    updateStatsScreen();


    document.getElementById(
        "lastResult"
    ).innerText =
        "Estatísticas zeradas.";

}


// =========================================================
// PARÂMETROS VISÍVEIS / OCULTOS
// =========================================================

function parametrosVisiveis() {

    const saved =
        localStorage.getItem(
            PARAMS_KEY
        );

    if (saved === null) {
        return true;
    }

    return saved === "true";

}


function aplicarVisibilidade() {

    const visible =
        parametrosVisiveis();


    const card =
        document.getElementById(
            "parametersCard"
        );


    const button =
        document.getElementById(
            "toggleButton"
        );


    const button2 =
        document.getElementById(
            "toggleButton2"
        );


    card.style.display =
        visible ? "block" : "none";


    button.innerText =
        visible
            ? "⚙️ PARÂMETROS: VISÍVEIS"
            : "⚙️ PARÂMETROS: OCULTOS";


    button2.innerText =
        visible
            ? "OCULTAR"
            : "MOSTRAR";

}


function toggleParametros() {

    const current =
        parametrosVisiveis();


    localStorage.setItem(
        PARAMS_KEY,
        String(!current)
    );


    aplicarVisibilidade();

}


// =========================================================
// PENDÊNCIAS
// =========================================================

function getPending() {

    try {

        const data =
            JSON.parse(
                localStorage.getItem(
                    PENDING_KEY
                )
            );

        if (Array.isArray(data)) {
            return data;
        }

    } catch (error) {
        console.log(error);
    }

    return [];

}


function savePending(data) {

    localStorage.setItem(
        PENDING_KEY,
        JSON.stringify(data)
    );

}


// =========================================================
// BUSCAR SINAL
// =========================================================

async function getSignal(
    symbol,
    interval
) {

    const url =
        "/signal?symbol=" +
        encodeURIComponent(symbol) +
        "&interval=" +
        encodeURIComponent(interval);


    const response =
        await fetch(url);


    let data;


    try {

        data =
            await response.json();

    } catch (error) {

        throw new Error(
            "Resposta inválida da API."
        );

    }


    if (!response.ok) {

        throw new Error(
            data.detail ||
            "Erro na análise."
        );

    }


    return data;

}


// =========================================================
// BUSCAR RESULTADO
// =========================================================

async function getResult(
    pending
) {

    const url =
        "/result?symbol=" +
        encodeURIComponent(
            pending.symbol
        ) +
        "&interval=" +
        encodeURIComponent(
            pending.interval
        ) +
        "&reference_candle=" +
        encodeURIComponent(
            pending.reference_candle
        ) +
        "&direction=" +
        encodeURIComponent(
            pending.direction
        );


    const response =
        await fetch(url);


    let data;


    try {

        data =
            await response.json();

    } catch (error) {

        return {
            status: "WAIT"
        };

    }


    if (!response.ok) {

        return {
            status: "WAIT"
        };

    }


    return data;

}


// =========================================================
// REGISTRAR NOVO SINAL
// =========================================================

function registerSignal(data) {

    if (
        !data ||
        !data.signal ||
        !data.reference_candle
    ) {
        return;
    }


    if (
        data.signal !== "CALL" &&
        data.signal !== "PUT"
    ) {
        return;
    }


    const pending =
        getPending();


    const key =
        data.symbol +
        "|" +
        data.interval +
        "|" +
        data.reference_candle +
        "|" +
        data.signal;


    const exists =
        pending.some(
            function(item) {

                return item.key === key;

            }
        );


    if (exists) {
        return;
    }


    pending.push({

        key: key,

        symbol: data.symbol,

        interval: data.interval,

        reference_candle:
            data.reference_candle,

        direction:
            data.signal,

        created_at:
            new Date().toISOString()

    });


    // Mantém somente os últimos 20
    while (pending.length > 20) {
        pending.shift();
    }


    savePending(pending);

}


// =========================================================
// CONFERIR RESULTADOS
// =========================================================

async function checkPendingResults() {

    const pending =
        getPending();


    if (!pending.length) {
        return;
    }


    const remaining = [];


    for (
        const item of pending
    ) {

        try {

            const result =
                await getResult(item);


            if (
                result.status === "WIN"
            ) {

                const stats =
                    getStats();


                stats.wins += 1;


                saveStats(stats);


                document.getElementById(
                    "lastResult"
                ).innerHTML =
                    "✅ WIN — " +
                    item.direction +
                    " — " +
                    item.symbol;


                updateStatsScreen();


                continue;

            }


            if (
                result.status === "LOSS"
            ) {

                const stats =
                    getStats();


                stats.losses += 1;


                saveStats(stats);


                document.getElementById(
                    "lastResult"
                ).innerHTML =
                    "❌ LOSS — " +
                    item.direction +
                    " — " +
                    item.symbol;


                updateStatsScreen();


                continue;

            }


            remaining.push(item);

        } catch (error) {

            remaining.push(item);

        }

    }


    savePending(remaining);

}


// =========================================================
// MOSTRAR SINAL
// =========================================================

function mostrarResultado(data) {

    const signal =
        document.getElementById(
            "signal"
        );


    signal.innerText =
        data.signal || "WAIT";


    if (
        data.signal === "CALL"
    ) {

        signal.className =
            "signal call";

    } else if (
        data.signal === "PUT"
    ) {

        signal.className =
            "signal put";

    } else {

        signal.className =
            "signal wait";

    }


    document.getElementById(
        "ema3"
    ).innerText =
        data.ema3 != null
            ? data.ema3
            : "--";


    document.getElementById(
        "ema7"
    ).innerText =
        data.ema7 != null
            ? data.ema7
            : "--";


    document.getElementById(
        "rsi"
    ).innerText =
        data.rsi14 != null
            ? data.rsi14
            : "--";


    document.getElementById(
        "adx21"
    ).innerText =
        data.adx21 != null
            ? data.adx21
            : "--";


    document.getElementById(
        "adx48"
    ).innerText =
        data.adx48 != null
            ? data.adx48
            : "--";


    document.getElementById(
        "callScore"
    ).innerText =
        data.call_score != null
            ? data.call_score
            : "--";


    document.getElementById(
        "putScore"
    ).innerText =
        data.put_score != null
            ? data.put_score
            : "--";


    document.getElementById(
        "reference"
    ).innerText =
        data.reference_candle ||
        "--";


    document.getElementById(
        "next"
    ).innerText =
        data.next_candle ||
        "--";


    document.getElementById(
        "entry"
    ).innerText =
        data.next_candle ||
        "--";


    document.getElementById(
        "asset"
    ).innerText =
        data.symbol ||
        "--";


    document.getElementById(
        "tf"
    ).innerText =
        data.interval ||
        "--";


    document.getElementById(
        "status"
    ).innerText =
        data.signal === "WAIT"
            ? "Sem entrada confirmada neste momento."
            : "Sinal " +
              data.signal +
              " identificado para a próxima vela.";


    registerSignal(data);

}


// =========================================================
// ANALISAR
// =========================================================

async function analisar() {

    const symbol =
        document.getElementById(
            "symbol"
        ).value;


    const interval =
        document.getElementById(
            "interval"
        ).value;


    const loading =
        document.getElementById(
            "loading"
        );


    loading.style.display =
        "block";


    try {

        if (
            symbol === "ALL"
        ) {

            document.getElementById(
                "singleCard"
            ).style.display =
                "none";


            document.getElementById(
                "parametersCard"
            ).style.display =
                "none";


            document.getElementById(
                "referenceCard"
            ).style.display =
                "none";


            document.getElementById(
                "allCard"
            ).style.display =
                "block";


            const box =
                document.getElementById(
                    "allResults"
                );


            box.innerHTML =
                "Carregando...";


            const results =
                await Promise.all(

                    ASSETS.map(
                        async function(asset) {

                            try {

                                return await getSignal(
                                    asset,
                                    interval
                                );

                            } catch (error) {

                                return {

                                    symbol: asset,

                                    signal: "ERRO",

                                    confidence: 0

                                };

                            }

                        }
                    )

                );


            box.innerHTML =
                results.map(
                    function(data) {

                        let cls =
                            "yellow";


                        if (
                            data.signal ===
                            "CALL"
                        ) {

                            cls =
                                "green";

                        } else if (
                            data.signal ===
                            "PUT"
                        ) {

                            cls =
                                "red";

                        }


                        return (

                            '<div class="assetrow">' +

                            '<span>' +

                            data.symbol +

                            '</span>' +

                            '<b class="' +
                            cls +
                            '">' +

                            data.signal +

                            " " +

                            (
                                data.confidence ||
                                0
                            ) +

                            "%" +

                            '</b>' +

                            '</div>'

                        );

                    }

                ).join("");


        } else {

            document.getElementById(
                "singleCard"
            ).style.display =
                "block";


            document.getElementById(
                "referenceCard"
            ).style.display =
                "block";


            document.getElementById(
                "allCard"
            ).style.display =
                "none";


            aplicarVisibilidade();


            const data =
                await getSignal(
                    symbol,
                    interval
                );


            mostrarResultado(data);

        }


        document.getElementById(
            "updated"
        ).innerText =
            "Última análise: " +
            new Date()
                .toLocaleTimeString(
                    "pt-BR"
                );


    } catch (error) {

        document.getElementById(
            "status"
        ).innerText =
            error.message ||
            "Erro desconhecido.";


    } finally {

        loading.style.display =
            "none";

    }

}


// =========================================================
// INICIALIZAÇÃO
// =========================================================

updateStatsScreen();

aplicarVisibilidade();

analisar();


// Atualiza análise a cada 60 segundos
setInterval(
    analisar,
    60000
);


// Verifica WIN/LOSS a cada 15 segundos
setInterval(
    checkPendingResults,
    15000
);


// Primeira verificação
setTimeout(
    checkPendingResults,
    5000
);

</script>

</body>

</html>
"""


# =========================================================
# PÁGINA PRINCIPAL
# =========================================================

@app.get("/", response_class=HTMLResponse)
def root():

    return HTML_PAGE


# =========================================================
# HEALTH
# =========================================================

@app.get("/health")
def health():

    return {
        "ok": True,
        "app": "Ismael Trade",
        "version": "7.0.0"
    }


# =========================================================
# HORÁRIO DO SERVIDOR
# =========================================================

@app.get("/server-time")
def server_time():

    return {
        "utc":
            datetime.now(
                timezone.utc
            ).isoformat()
    }


# =========================================================
# BUSCAR CANDLES
# =========================================================

async def get_candles(
    symbol: str,
    interval: str,
    size: int = 200
):

    if not KEY:

        raise HTTPException(
            status_code=500,
            detail=(
                "TWELVE_DATA_API_KEY "
                "não configurada no Render."
            )
        )


    if symbol not in ASSETS:

        raise HTTPException(
            status_code=400,
            detail="Ativo não suportado."
        )


    if interval not in ALLOWED_INTERVALS:

        raise HTTPException(
            status_code=400,
            detail=(
                "Intervalo inválido. "
                "Use 1min, 5min, 15min ou 30min."
            )
        )


    try:

        requested_size =
            int(size)

    except (
        TypeError,
        ValueError
    ):

        requested_size = 200


    outputsize = max(
        100,
        min(
            requested_size,
            500
        )
    )


    params = {

        "symbol": symbol,

        "interval": interval,

        "outputsize":
            outputsize,

        "apikey": KEY,

        "timezone":
            "America/Sao_Paulo",

        "format":
            "JSON"

    }


    try:

        async with httpx.AsyncClient(
            timeout=20
        ) as client:

            response =
                await client.get(
                    URL,
                    params=params
                )


            response.raise_for_status()


            data: dict[str, Any] =
                response.json()


    except httpx.HTTPError as exc:

        raise HTTPException(
            status_code=502,
            detail=(
                "Erro ao consultar "
                "a Twelve Data."
            )
        ) from exc


    if data.get("status") == "error":

        raise HTTPException(
            status_code=502,
            detail=data.get(
                "message",
                "Erro retornado pela Twelve Data."
            )
        )


    values =
        data.get(
            "values",
            []
        )


    if not values:

        raise HTTPException(
            status_code=502,
            detail=(
                "A fonte de dados não "
                "retornou candles."
            )
        )


    return values


# =========================================================
# CANDLES ENDPOINT
# =========================================================

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


# =========================================================
# EMA
# =========================================================

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
            (
                price * alpha
            ) + (
                value *
                (1.0 - alpha)
            )


    return value


# =========================================================
# RSI
# =========================================================

def rsi(
    values,
    period=14
):

    if len(values) < period + 1:

        return None


    gains = []
    losses = []


    for i in range(
        1,
        len(values)
    ):

        change =
            values[i] -
            values[i - 1]


        if change > 0:

            gains.append(change)
            losses.append(0.0)

        else:

            gains.append(0.0)
            losses.append(
                abs(change)
            )


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
                (
                    (period - 1)
                    * average_gain
                )
                +
                gains[i]
            ) / period


        average_loss =
            (
                (
                    (period - 1)
                    * average_loss
                )
                +
                losses[i]
            ) / period


    if average_loss == 0:

        return 100.0


    rs =
        average_gain /
        average_loss


    return (
        100.0 -
        (
            100.0 /
            (1.0 + rs)
        )
    )


# =========================================================
# ADX
# =========================================================

def adx(
    candles_data,
    period=14
):

    if len(candles_data) < (
        period * 2 + 5
    ):

        return None


    data =
        list(
            reversed(
                candles_data
            )
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
        len(data)
    ):

        high =
            highs[i]

        low =
            lows[i]

        previous_high =
            highs[i - 1]

        previous_low =
            lows[i - 1]

        previous_close =
            closes[i - 1]


        true_range =
            max(
                high - low,
                abs(
                    high -
                    previous_close
                ),
                abs(
                    low -
                    previous_close
                )
            )


        up_move =
            high -
            previous_high


        down_move =
            previous_low -
            low


        plus =
            up_move if (
                up_move > down_move
                and up_move > 0
            ) else 0.0


        minus =
            down_move if (
                down_move > up_move
                and down_move > 0
            ) else 0.0


        tr.append(
            true_range
        )

        plus_dm.append(
            plus
        )

        minus_dm.append(
            minus
        )


    if len(tr) < period + 1:

        return None


    atr =
        sum(
            tr[:period]
        ) / period


    plus_smoothed =
        sum(
            plus_dm[:period]
        ) / period


    minus_smoothed =
        sum(
            minus_dm[:period]
        ) / period


    dx_values = []


    for i in range(
        period,
        len(tr)
    ):

        atr =
            (
                (
                    atr *
                    (period - 1)
                )
                +
                tr[i]
            ) / period


        plus_smoothed =
            (
                (
                    plus_smoothed *
                    (period - 1)
                )
                +
                plus_dm[i]
            ) / period


        minus_smoothed =
            (
                (
                    minus_smoothed *
                    (period - 1)
                )
                +
                minus_dm[i]
            ) / period


        if atr == 0:

            dx_values.append(0.0)

            continue


        plus_di =
            100.0 * (
                plus_smoothed /
                atr
            )


        minus_di =
            100.0 * (
                minus_smoothed /
                atr
            )


        denominator =
            plus_di +
            minus_di


        if denominator == 0:

            dx = 0.0

        else:

            dx =
                100.0 * (
                    abs(
                        plus_di -
                        minus_di
                    )
                    /
                    denominator
                )


        dx_values.append(dx)


    if len(dx_values) < period:

        return None


    adx_value =
        sum(
            dx_values[:period]
        ) / period


    for i in range(
        period,
        len(dx_values)
    ):

        adx_value =
            (
                (
                    adx_value *
                    (period - 1)
                )
                +
                dx_values[i]
            ) / period


    return adx_value


# =========================================================
# ANALISAR MERCADO
# =========================================================

def analyze(values):

    if len(values) < 80:

        raise HTTPException(
            status_code=502,
            detail=(
                "Quantidade insuficiente "
                "de candles para análise."
            )
        )


    # Twelve Data retorna do mais recente
    # para o mais antigo.
    #
    # values[0] = candle atual
    # values[1] = último candle fechado
    #
    # Para evitar utilizar a vela em formação,
    # os indicadores são calculados somente
    # com candles fechados.

    closed =
        values[1:]


    closed_chronological =
        list(
            reversed(
                closed
            )
        )


    closes = [
        float(x["close"])
        for x in closed_chronological
    ]


    ema3_value =
        ema(
            closes,
            3
        )


    ema7_value =
        ema(
            closes,
            7
        )


    rsi14_value =
        rsi(
            closes,
            14
        )


    adx21_value =
        adx(
            closed,
            21
        )


    adx48_value =
        adx(
            closed,
            48
        )


    if (
        ema3_value is None
        or
        ema7_value is None
        or
        rsi14_value is None
        or
        adx21_value is None
        or
        adx48_value is None
    ):

        return {

            "signal":
                "WAIT",

            "confidence":
                0,

            "ema3":
                None,

            "ema7":
                None,

            "rsi14":
                None,

            "adx21":
                None,

            "adx48":
                None,

            "call_score":
                0,

            "put_score":
                0

        }


    call_score = 0
    put_score = 0


    # =====================================================
    # CALL
    # =====================================================

    if ema3_value > ema7_value:

        call_score += 2


    if 52 <= rsi14_value <= 70:

        call_score += 1


    if adx21_value >= 20:

        call_score += 1


    if adx48_value >= 18:

        call_score += 1


    if (
        adx21_value >
        adx48_value
    ):

        call_score += 1


    # =====================================================
    # PUT
    # =====================================================

    if ema3_value < ema7_value:

        put_score += 2


    if 30 <= rsi14_value <= 48:

        put_score += 1


    if adx21_value >= 20:

        put_score += 1


    if adx48_value >= 18:

        put_score += 1


    if (
        adx21_value >
        adx48_value
    ):

        put_score += 1


    signal = "WAIT"
    confidence = 0


    if (
        call_score >= 4
        and
        call_score > put_score
    ):

        signal = "CALL"

        confidence =
            min(
                95,
                70 +
                (
                    call_score - 4
                ) * 5
            )


    elif (
        put_score >= 4
        and
        put_score > call_score
    ):

        signal = "PUT"

        confidence =
            min(
                95,
                70 +
                (
                    put_score - 4
                ) * 5
            )


    return {

        "signal":
            signal,

        "confidence":
            confidence,

        "ema3":
            round(
                ema3_value,
                6
            ),

        "ema7":
            round(
                ema7_value,
                6
            ),

        "rsi14":
            round(
                rsi14_value,
                2
            ),

        "adx21":
            round(
                adx21_value,
                2
            ),

        "adx48":
            round(
                adx48_value,
                2
            ),

        "call_score":
            call_score,

        "put_score":
            put_score

    }


# =========================================================
# SIGNAL ENDPOINT
# =========================================================

@app.get("/signal")
async def signal(
    symbol: str = Query(
        "EUR/USD"
    ),

    interval: str = Query(
        "1min"
    )
):

    values =
        await get_candles(
            symbol,
            interval,
            250
        )


    analysis =
        analyze(values)


    # Candle 0:
    # atual/em formação
    #
    # Candle 1:
    # último candle fechado

    current_candle =
        values[0]


    reference_candle =
        values[1]


    return {

        "ok": True,

        "source":
            "Twelve Data",

        "symbol":
            symbol,

        "interval":
            interval,

        "signal":
            analysis["signal"],

        "confidence":
            analysis["confidence"],

        "reference_candle":
            reference_candle["datetime"],

        "next_candle":
            current_candle["datetime"],

        "entry":
            current_candle["datetime"],

        "expiration":
            "1 vela",

        "ema3":
            analysis["ema3"],

        "ema7":
            analysis["ema7"],

        "rsi14":
            analysis["rsi14"],

        "adx21":
            analysis["adx21"],

        "adx48":
            analysis["adx48"],

        "call_score":
            analysis["call_score"],

        "put_score":
            analysis["put_score"],

        "non_repaint_reference":
            True,

        "warning":
            (
                "Sinal probabilístico; "
                "não garante WIN. "
                "Análise baseada em candles "
                "fechados da Twelve Data."
            )

    }


# =========================================================
# RESULTADO WIN / LOSS
# =========================================================

@app.get("/result")
async def result(
    symbol: str = Query(...),

    interval: str = Query(...),

    reference_candle: str = Query(...),

    direction: str = Query(...)
):

    if direction not in (
        "CALL",
        "PUT"
    ):

        raise HTTPException(
            status_code=400,
            detail="Direção inválida."
        )


    values =
        await get_candles(
            symbol,
            interval,
            250
        )


    # Ordena do mais antigo
    # para o mais recente.

    chronological =
        list(
            reversed(
                values
            )
        )


    reference_index = -1


    for i, candle in enumerate(
        chronological
    ):

        if (
            candle.get(
                "datetime"
            )
            ==
            reference_candle
        ):

            reference_index = i

            break


    if reference_index < 0:

        return {

            "ok": True,

            "status":
                "WAIT",

            "message":
                "Vela de referência ainda não localizada."

        }


    next_index =
        reference_index + 1


    if next_index >= len(
        chronological
    ):

        return {

            "ok": True,

            "status":
                "WAIT",

            "message":
                "Aguardando próxima vela."

        }


    next_candle =
        chronological[
            next_index
        ]


    try:

        open_price =
            float(
                next_candle["open"]
            )

        close_price =
            float(
                next_candle["close"]
            )

    except (
        TypeError,
        ValueError,
        KeyError
    ):

        return {

            "ok": True,

            "status":
                "WAIT"

        }


    # Se a vela ainda estiver em formação,
    # não registra resultado.

    try:

        candle_time =
            datetime.strptime(
                next_candle["datetime"],
                "%Y-%m-%d %H:%M:%S"
            )

        now =
            datetime.now()

        interval_minutes = {
            "1min": 1,
            "5min": 5,
            "15min": 15,
            "30min": 30
        }.get(
            interval,
            1
        )


        elapsed =
            (
                now -
                candle_time
            ).total_seconds()


        if elapsed < (
            interval_minutes * 60
        ):

            return {

                "ok": True,

                "status":
                    "WAIT",

                "message":
                    "Aguardando fechamento da vela."

            }

    except Exception:

        pass


    if close_price == open_price:

        return {

            "ok": True,

            "status":
                "DRAW",

            "open":
                open_price,

            "close":
                close_price,

            "candle":
                next_candle["datetime"]

        }


    candle_up =
        close_price > open_price


    if (
        direction == "CALL"
        and
        candle_up
    ):

        status = "WIN"

    elif (
        direction == "PUT"
        and
        not candle_up
    ):

        status = "WIN"

    else:

        status = "LOSS"


    return {

        "ok": True,

        "status":
            status,

        "direction":
            direction,

        "open":
            open_price,

        "close":
            close_price,

        "candle":
            next_candle["datetime"]

    }