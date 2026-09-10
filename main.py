import os
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(
    title="Ismael Trade",
    version="6.0.0"
)

# A chave deve ser cadastrada no Render:
# Environment -> TWELVE_DATA_API_KEY
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


async def get_candles(
    symbol: str,
    interval: str = "1min",
    outputsize: int = 100
) -> list[dict[str, Any]]:

    if not KEY:
        raise HTTPException(
            status_code=503,
            detail="TWELVE_DATA_API_KEY não configurada no Render."
        )

    if symbol not in ASSETS:
        raise HTTPException(
            status_code=400,
            detail="Ativo não permitido."
        )

    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(
            status_code=400,
            detail="Timeframe inválido."
        )

    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": max(60, min(outputsize, 500)),
        "apikey": KEY,
        "format": "JSON"
    }

    try:

        async with httpx.AsyncClient(
            timeout=20.0
        ) as client:

            response = await client.get(
                URL,
                params=params
            )

    except httpx.RequestError as error:

        raise HTTPException(
            status_code=502,
            detail=(
                "Erro de conexão com o provedor "
                "de dados: "
                + error.__class__.__name__
            )
        )

    # CORREÇÃO DO ERRO:
    # Não tentar response.json() sem verificar
    # se a resposta realmente é JSON.

    content_type = (
        response.headers
        .get("content-type", "")
        .lower()
    )

    if "json" not in content_type:

        raise HTTPException(
            status_code=502,
            detail=(
                "O provedor de dados retornou "
                "uma resposta que não é JSON."
            )
        )

    try:

        data = response.json()

    except ValueError:

        raise HTTPException(
            status_code=502,
            detail=(
                "Resposta inválida do provedor "
                "de dados: JSON malformado."
            )
        )

    if response.status_code >= 400:

        detail = None

        if isinstance(data, dict):
            detail = data.get("message")

        raise HTTPException(
            status_code=502,
            detail=detail or "Erro no provedor de dados."
        )

    if not isinstance(data, dict):

        raise HTTPException(
            status_code=502,
            detail="Formato inesperado da resposta."
        )

    if data.get("status") == "error":

        raise HTTPException(
            status_code=502,
            detail=(
                data.get("message")
                or "Erro retornado pelo provedor."
            )
        )

    values = data.get("values")

    if not isinstance(values, list):

        raise HTTPException(
            status_code=502,
            detail=(
                "O provedor não retornou "
                "dados de candles."
            )
        )

    candles = []

    for item in values:

        try:

            candles.append({
                "datetime": item["datetime"],
                "open": float(item["open"]),
                "high": float(item["high"]),
                "low": float(item["low"]),
                "close": float(item["close"])
            })

        except (
            KeyError,
            TypeError,
            ValueError
        ):

            continue

    if len(candles) < 60:

        raise HTTPException(
            status_code=502,
            detail=(
                "Dados insuficientes para análise. "
                f"Recebidos: {len(candles)} candles."
            )
        )

    return candles


def ema(
    values: list[float],
    period: int
) -> float | None:

    if len(values) < period:
        return None

    multiplier = 2.0 / (period + 1)

    result = (
        sum(values[:period])
        / period
    )

    for price in values[period:]:

        result = (
            (price - result)
            * multiplier
        ) + result

    return result


def rsi(
    values: list[float],
    period: int = 14
) -> float | None:

    if len(values) <= period:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):

        change = (
            values[i]
            - values[i - 1]
        )

        if change > 0:

            gains.append(change)
            losses.append(0.0)

        else:

            gains.append(0.0)
            losses.append(abs(change))

    avg_gain = (
        sum(gains[:period])
        / period
    )

    avg_loss = (
        sum(losses[:period])
        / period
    )

    for i in range(
        period,
        len(gains)
    ):

        avg_gain = (
            (
                avg_gain
                * (period - 1)
            )
            + gains[i]
        ) / period

        avg_loss = (
            (
                avg_loss
                * (period - 1)
            )
            + losses[i]
        ) / period

    if avg_loss == 0:

        if avg_gain > 0:
            return 100.0

        return 50.0

    rs = (
        avg_gain
        / avg_loss
    )

    return 100.0 - (
        100.0 / (1.0 + rs)
    )


def adx(
    values: list[float],
    period: int
) -> float | None:

    if len(values) < period + 1:
        return None

    changes = []

    for i in range(1, len(values)):

        changes.append(
            abs(
                values[i]
                - values[i - 1]
            )
        )

    if len(changes) < period:
        return None

    average_change = (
        sum(changes[-period:])
        / period
    )

    reference = (
        sum(values[-period:])
        / period
    )

    if reference == 0:
        return 0.0

    return min(
        100.0,
        (
            average_change
            / reference
        ) * 10000.0
    )


def format_value(
    value: float | None
) -> float | None:

    if value is None:
        return None

    return round(
        float(value),
        5
    )


def calculate_signal(
    candles: list[dict[str, Any]]
) -> dict[str, Any]:

    if len(candles) < 60:

        raise HTTPException(
            status_code=502,
            detail="Dados insuficientes para análise."
        )

    # Retira a vela mais recente,
    # tratando-a como vela em formação.
    closed = candles[1:]

    closes = [
        float(candle["close"])
        for candle in reversed(closed)
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

    # EMA 3 x EMA 7

    if (
        ema3 is not None
        and ema7 is not None
    ):

        if ema3 > ema7:
            call_score += 2

        elif ema3 < ema7:
            put_score += 2

    # RSI 14

    if rsi14 is not None:

        if rsi14 < 30:

            call_score += 2

        elif rsi14 > 70:

            put_score += 2

        elif rsi14 >= 50:

            call_score += 1

        else:

            put_score += 1

    # ADX 21

    if (
        adx21 is not None
        and adx21 >= 20
        and ema3 is not None
        and ema7 is not None
    ):

        if ema3 > ema7:
            call_score += 1

        elif ema3 < ema7:
            put_score += 1

    # ADX 48

    if (
        adx48 is not None
        and adx48 >= 20
        and ema3 is not None
        and ema7 is not None
    ):

        if ema3 > ema7:
            call_score += 1

        elif ema3 < ema7:
            put_score += 1

    # Resultado

    if (
        call_score >= 4
        and call_score > put_score
    ):

        signal = "CALL"

    elif (
        put_score >= 4
        and put_score > call_score
    ):

        signal = "PUT"

    else:

        signal = "WAIT"

    total_score = max(
        call_score,
        put_score
    )

    confidence = min(
        95,
        59 + (total_score * 5)
    )

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
<script>

async function getSignal() {

const asset = document.getElementById("asset").value;
const interval = document.getElementById("interval").value;

const loading = document.getElementById("loading");
const error = document.getElementById("error");
const result = document.getElementById("result");

loading.innerText = "Analisando...";
error.style.display = "none";

try {

const response = await fetch(
"/signal?symbol=" +
encodeURIComponent(asset) +
"&interval=" +
encodeURIComponent(interval)
);

const text = await response.text();

let data;

try {
data = JSON.parse(text);
} catch(e) {
throw new Error(
"Servidor não retornou JSON válido."
);
}

if (!response.ok) {
throw new Error(
data.detail || data.error || "Erro no servidor."
);
}

function mostrarResultado(data) {

const signal = document.getElementById("signal");
const result = document.getElementById("result");

result.style.display = "block";

let s = String(
data.signal || data.direction || "AGUARDE"
).toUpperCase();

if (s.includes("CALL") || s.includes("BUY")) {
signal.innerText = "CALL";
signal.className = "signal call";
}
else if (s.includes("PUT") || s.includes("SELL")) {
signal.innerText = "PUT";
signal.className = "signal put";
}
else {
signal.innerText = s;
signal.className = "signal wait";
}

document.getElementById("confidence").innerText =
"Probabilidade: " +
(data.confidence ?? "--") + "%";

mostrarResultado(data);

} catch(err) {

error.innerText = "Erro: " + err.message;
error.style.display = "block";

} finally {

loading.innerText = "";

}

}

document.getElementById("ema3").innerText =
data.ema3 ?? "--";

document.getElementById("ema7").innerText =
data.ema7 ?? "--";

document.getElementById("rsi").innerText =
data.rsi ?? "--";

document.getElementById("adx").innerText =
data.adx ?? "--";

document.getElementById("assetResult").innerText =
data.symbol ?? "--";
document.getElementById("intervalResult").innerText =
data.interval ?? "--";

document.getElementById("price").innerText =
data.reference_price ??
data.price ??
data.close ??
"--";

document.getElementById("nextRef").innerText =
data.next_candle ??
data.next ??
"--";

}
}

function formatNumber(value) {

if (value === undefined || value === null)
return "--";

const n = Number(value);

if (Number.isNaN(n))
return String(value);

return n.toFixed(5);
}

</script>
# ROTAS DA API

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "ISMAEL TRADE",
        "version": APP_VERSION,
        "key_configured": bool(KEY)
    }


@app.get("/candles")
async def candles_endpoint(
    symbol: str,
    interval: str = "1min"
):
    candles = await get_candles(
        symbol,
        interval
    )

    return {
        "status": "ok",
        "symbol": symbol,
        "interval": interval,
        "count": len(candles),
        "candles": candles
    }
@app.get("/signal")
async def signal_endpoint(
    symbol: str,
    interval: str = "1min"
):
    candles = await get_candles(
        symbol,
        interval
    )

    result = calculate_signal(
        candles
    )

    result["status"] = "ok"
    result["symbol"] = symbol
    result["interval"] = interval
    return JSONResponse(
        content=result
    )


@app.get("/", response_class=HTMLResponse)
async def home():
    return HTML_PAGE
if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port
    )