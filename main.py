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
from fastapi.responses import HTMLResponse


# ============================================================
# CONFIGURAÇÃO
# ============================================================

app = FastAPI(
    title="MEGA IA",
    version="12.2.0"
)

BR_TZ = ZoneInfo("America/Sao_Paulo")
UTC = timezone.utc

TD_KEY = os.getenv(
    "TWELVE_DATA_API_KEY",
    ""
).strip()

OAI_KEY = os.getenv(
    "OPENAI_API_KEY",
    ""
).strip()

OAI_MODEL = os.getenv(
    "OPENAI_MODEL",
    "gpt-5.6-luna"
).strip()

OAI_MIN = float(
    os.getenv(
        "OPENAI_MIN_CONFIDENCE",
        "70"
    )
)

OAI_TIMEOUT = float(
    os.getenv(
        "OPENAI_TIMEOUT",
        "15"
    )
)

LICENSE = os.getenv(
    "LICENSE_EXPIRES",
    "2026-12-31"
)

WA1 = os.getenv(
    "WHATSAPP_1",
    "55 84 99841-1282"
)

WA2 = os.getenv(
    "WHATSAPP_2",
    "55 84 99449-9442"
)

IG = os.getenv(
    "INSTAGRAM",
    "@Ismaelartur26"
)


# ============================================================
# TWELVE DATA
# ============================================================

TD_URL = (
    "https://api.twelvedata.com/time_series"
)

INTERVALS = {
    "1min": 60,
    "5min": 300,
    "15min": 900,
    "30min": 1800
}


SYMBOLS = [
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
# OPENAI
# ============================================================

OAI_URL = (
    "https://api.openai.com/v1/responses"
)


# ============================================================
# CACHE
# ============================================================

candle_cache: Dict[
    str,
    Dict[str, Any]
] = {}

signal_cache: Dict[
    str,
    Any
] = {}

oai_cache: Dict[
    str,
    Any
] = {}

results: Dict[
    str,
    Any
] = {}


# ============================================================
# CONTROLE TWELVE DATA
# ============================================================

TD_MIN_INTERVAL = float(
    os.getenv(
        "TD_MIN_INTERVAL",
        "8"
    )
)

TD_CACHE_SECONDS = float(
    os.getenv(
        "TD_CACHE_SECONDS",
        "20"
    )
)

TD_MAX_RETRIES = int(
    os.getenv(
        "TD_MAX_RETRIES",
        "3"
    )
)

td_lock = asyncio.Lock()

last_td_request = 0.0


# ============================================================
# FUNÇÕES BÁSICAS
# ============================================================

def now():

    return datetime.now(
        BR_TZ
    )


def iso(d):

    return d.astimezone(
        BR_TZ
    ).isoformat()


def parse(s):

    d = datetime.fromisoformat(
        str(s).replace(
            "Z",
            "+00:00"
        )
    )

    if d.tzinfo is None:

        d = d.replace(
            tzinfo=UTC
        )

    return d.astimezone(
        BR_TZ
    )


def clamp(x, a, b):

    return max(
        a,
        min(b, x)
    )


# ============================================================
# INDICADORES
# ============================================================

def ema(v, p):

    if len(v) < p:

        return None

    k = 2 / (
        p + 1
    )

    e = sum(
        v[:p]
    ) / p

    for x in v[p:]:

        e = (
            x * k
            +
            e * (1 - k)
        )

    return e


def rsi(v, p=14):

    if len(v) < p + 1:

        return None

    gains = []
    losses = []

    for i in range(
        1,
        len(v)
    ):

        d = (
            v[i]
            -
            v[i - 1]
        )

        gains.append(
            max(d, 0)
        )

        losses.append(
            max(-d, 0)
        )

    avg_gain = (
        sum(gains[-p:])
        / p
    )

    avg_loss = (
        sum(losses[-p:])
        / p
    )

    if avg_loss == 0:

        return 100

    return (
        100
        -
        100 /
        (
            1
            +
            avg_gain /
            avg_loss
        )
    )


# ============================================================
# CONTROLE DE RATE LIMIT
# ============================================================

async def wait_td_slot():

    global last_td_request

    async with td_lock:

        elapsed = (
            time.monotonic()
            -
            last_td_request
        )

        if elapsed < TD_MIN_INTERVAL:

            await asyncio.sleep(
                TD_MIN_INTERVAL
                -
                elapsed
            )

        last_td_request = (
            time.monotonic()
        )


# ============================================================
# BUSCAR CANDLES
# ============================================================

async def candles(
    symbol,
    interval,
    n=80
):

    if not TD_KEY:

        raise HTTPException(
            status_code=500,
            detail=(
                "TWELVE_DATA_API_KEY "
                "não configurada."
            )
        )

    if symbol not in SYMBOLS:

        raise HTTPException(
            status_code=400,
            detail="Ativo inválido."
        )

    if interval not in INTERVALS:

        raise HTTPException(
            status_code=400,
            detail="Intervalo inválido."
        )

    n = int(
        clamp(
            n,
            10,
            100
        )
    )

    cache_key = (
        f"{symbol}|"
        f"{interval}|"
        f"{n}"
    )

    cached = candle_cache.get(
        cache_key
    )

    if cached:

        age = (
            time.time()
            -
            cached["time"]
        )

        if age < TD_CACHE_SECONDS:

            return cached["data"]

    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": n,
        "apikey": TD_KEY,
        "format": "JSON"
    }

    last_error = (
        "Erro desconhecido."
    )

    for attempt in range(
        TD_MAX_RETRIES
    ):

        try:

            await wait_td_slot()

            async with httpx.AsyncClient(
                timeout=15
            ) as client:

                response = await client.get(
                    TD_URL,
                    params=params
                )

            # ------------------------------------------------
            # RATE LIMIT
            # ------------------------------------------------

            if response.status_code == 429:

                retry_after = (
                    response.headers.get(
                        "Retry-After"
                    )
                )

                try:

                    wait_seconds = float(
                        retry_after
                    )

                except Exception:

                    wait_seconds = (
                        10
                        *
                        (attempt + 1)
                    )

                last_error = (
                    "Twelve Data: "
                    "limite de requisições "
                    "atingido."
                )

                if (
                    attempt
                    <
                    TD_MAX_RETRIES - 1
                ):

                    await asyncio.sleep(
                        min(
                            wait_seconds,
                            30
                        )
                    )

                    continue

                break

            # ------------------------------------------------
            # OUTROS ERROS HTTP
            # ------------------------------------------------

            if response.status_code >= 400:

                try:

                    data = response.json()

                    message = data.get(
                        "message",
                        f"HTTP {response.status_code}"
                    )

                except Exception:

                    message = (
                        f"HTTP "
                        f"{response.status_code}"
                    )

                last_error = message

                if (
                    attempt
                    <
                    TD_MAX_RETRIES - 1
                ):

                    await asyncio.sleep(
                        2
                        *
                        (attempt + 1)
                    )

                    continue

                break

            data = response.json()

            # ------------------------------------------------
            # ERRO TWELVE DATA
            # ------------------------------------------------

            if data.get(
                "status"
            ) == "error":

                message = data.get(
                    "message",
                    "Erro Twelve Data."
                )

                last_error = message

                low = message.lower()

                if (
                    "limit" in low
                    or
                    "rate" in low
                    or
                    "too many" in low
                ):

                    if (
                        attempt
                        <
                        TD_MAX_RETRIES - 1
                    ):

                        await asyncio.sleep(
                            10
                            *
                            (attempt + 1)
                        )

                        continue

                break

            values = data.get(
                "values",
                []
            )

            if not values:

                last_error = (
                    "Nenhum candle recebido."
                )

                if (
                    attempt
                    <
                    TD_MAX_RETRIES - 1
                ):

                    await asyncio.sleep(
                        2
                        *
                        (attempt + 1)
                    )

                    continue

                break

            # ------------------------------------------------
            # NORMALIZAR
            # ------------------------------------------------

            out = []

            for item in reversed(
                values
            ):

                try:

                    out.append(
                        {
                            "datetime":
                                item[
                                    "datetime"
                                ],

                            "open":
                                float(
                                    item["open"]
                                ),

                            "high":
                                float(
                                    item["high"]
                                ),

                            "low":
                                float(
                                    item["low"]
                                ),

                            "close":
                                float(
                                    item["close"]
                                ),

                            "volume":
                                float(
                                    item.get(
                                        "volume",
                                        0
                                    )
                                    or 0
                                )
                        }
                    )

                except Exception:

                    continue

            if not out:

                last_error = (
                    "Candles inválidos "
                    "recebidos."
                )

                break

            # ------------------------------------------------
            # CACHE
            # ------------------------------------------------

            candle_cache[
                cache_key
            ] = {
                "time":
                    time.time(),

                "data":
                    out
            }

            return out

        except Exception as exc:

            last_error = str(
                exc
            )

            if (
                attempt
                <
                TD_MAX_RETRIES - 1
            ):

                await asyncio.sleep(
                    2
                    *
                    (attempt + 1)
                )

    # --------------------------------------------------------
    # CACHE ANTIGO
    # --------------------------------------------------------

    cached = candle_cache.get(
        cache_key
    )

    if cached and cached.get(
        "data"
    ):

        return cached[
            "data"
        ]

    raise HTTPException(
        status_code=503,
        detail=(
            "Twelve Data indisponível: "
            +
            last_error
        )
    )


# ============================================================
# IA LOCAL
# ============================================================

def local_ai(cs):

    if len(cs) < 35:

        return {
            "direction": "NEUTRO",
            "confidence": 0,
            "confirmed": False,
            "reason":
                "Poucos candles disponíveis."
        }

    values = [
        c["close"]
        for c in cs
    ]

    fast = ema(
        values,
        3
    )

    slow = ema(
        values,
        7
    )

    rsi_value = rsi(
        values,
        14
    )

    last = cs[-1]
    prev = cs[-2]

    votes = {
        "CALL": 0.0,
        "PUT": 0.0
    }

    # EMA
    if (
        fast is not None
        and
        slow is not None
    ):

        if fast > slow:

            votes[
                "CALL"
            ] += 1.0

        elif fast < slow:

            votes[
                "PUT"
            ] += 1.0

    # RSI
    if rsi_value is not None:

        if rsi_value <= 35:

            votes[
                "CALL"
            ] += 1.2

        elif rsi_value >= 65:

            votes[
                "PUT"
            ] += 1.2

        elif rsi_value > 50:

            votes[
                "CALL"
            ] += 0.4

        elif rsi_value < 50:

            votes[
                "PUT"
            ] += 0.4

    # Direção
    if last["close"] > prev["close"]:

        votes[
            "CALL"
        ] += 0.7

    elif last["close"] < prev["close"]:

        votes[
            "PUT"
        ] += 0.7

    # Força
    candle_range = max(
        last["high"]
        -
        last["low"],
        1e-12
    )

    body_ratio = (
        abs(
            last["close"]
            -
            last["open"]
        )
        /
        candle_range
    )

    if body_ratio >= 0.6:

        if (
            last["close"]
            >
            last["open"]
        ):

            votes[
                "CALL"
            ] += 0.7

        elif (
            last["close"]
            <
            last["open"]
        ):

            votes[
                "PUT"
            ] += 0.7

    if (
        votes["CALL"]
        >
        votes["PUT"]
    ):

        direction = "CALL"

    elif (
        votes["PUT"]
        >
        votes["CALL"]
    ):

        direction = "PUT"

    else:

        direction = "NEUTRO"

    total = sum(
        votes.values()
    )

    if total:

        confidence = (
            50
            +
            abs(
                votes["CALL"]
                -
                votes["PUT"]
            )
            /
            total
            *
            45
        )

    else:

        confidence = 0

    return {
        "direction":
            direction,

        "confidence":
            round(
                clamp(
                    confidence,
                    0,
                    97
                ),
                1
            ),

        "confirmed":
            confidence >= 65,

        "reason":
            (
                "EMA3/7, "
                "RSI14="
                +
                (
                    str(
                        round(
                            rsi_value,
                            1
                        )
                    )
                    if
                    rsi_value
                    is not None
                    else
                    "--"
                )
                +
                ", força do candle="
                +
                str(
                    round(
                        body_ratio * 100,
                        1
                    )
                )
                +
                "%"
            )
    }


# ============================================================
# JSON OPENAI
# ============================================================

def json_extract(s):

    if not s:

        return None

    s = re.sub(
        r"^```(?:json)?\s*|\s*```$",
        "",
        s.strip(),
        flags=re.I
    )

    try:

        return json.loads(
            s
        )

    except Exception:

        pass

    match = re.search(
        r"\{.*\}",
        s,
        re.S
    )

    if not match:

        return None

    try:

        return json.loads(
            match.group(0)
        )

    except Exception:

        return None


# ============================================================
# CONFIRMAÇÃO OPENAI
# ============================================================

async def openai_confirm(
    symbol,
    interval,
    cs,
    local
):

    if not OAI_KEY:

        return {
            "available":
                False,

            "error":
                "OPENAI_API_KEY "
                "não configurada."
        }

    key = (
        f"{symbol}|"
        f"{interval}|"
        f"{cs[-1]['datetime']}"
    )

    cached = oai_cache.get(
        key
    )

    if cached:

        if (
            time.time()
            -
            cached[0]
            <
            55
        ):

            return cached[1]

    data = [
        {
            "time":
                c["datetime"],

            "o":
                c["open"],

            "h":
                c["high"],

            "l":
                c["low"],

            "c":
                c["close"],

            "v":
                c["volume"]
        }

        for c in cs[-40:]
    ]

    prompt = f"""
Você é o módulo de confirmação da MEGA IA.

Ativo: {symbol}
Timeframe: {interval}

Use somente candles fechados.
Não invente dados futuros.

Analise:
- tendência
- momentum
- estrutura
- força
- volatilidade
- possível reversão

Direção preliminar:
{local["direction"]}

Confiança preliminar:
{local["confidence"]}

Retorne somente JSON válido:

{{
  "direction":"CALL|PUT|NEUTRO",
  "confidence":0,
  "confirmed":true,
  "reason":"curto",
  "risk":"LOW|MEDIUM|HIGH"
}}

Candles:
{json.dumps(data)}
"""

    try:

        async with httpx.AsyncClient(
            timeout=OAI_TIMEOUT
        ) as client:

            response = await client.post(
                OAI_URL,

                headers={
                    "Authorization":
                        f"Bearer {OAI_KEY}",

                    "Content-Type":
                        "application/json"
                },

                json={
                    "model":
                        OAI_MODEL,

                    "input":
                        prompt
                }
            )

        if response.status_code >= 400:

            try:

                error_data = (
                    response.json()
                )

                message = (
                    error_data
                    .get(
                        "error",
                        {}
                    )
                    .get(
                        "message",
                        f"OpenAI HTTP "
                        f"{response.status_code}"
                    )
                )

            except Exception:

                message = (
                    f"OpenAI HTTP "
                    f"{response.status_code}"
                )

            return {
                "available":
                    False,

                "error":
                    message
            }

        response_data = (
            response.json()
        )

        text = response_data.get(
            "output_text",
            ""
        )

        if not text:

            for item in (
                response_data.get(
                    "output",
                    []
                )
            ):

                for content in (
                    item.get(
                        "content",
                        []
                    )
                ):

                    if content.get(
                        "type"
                    ) in (
                        "output_text",
                        "text"
                    ):

                        text += (
                            content.get(
                                "text",
                                ""
                            )
                        )

        parsed = json_extract(
            text
        )

        if not isinstance(
            parsed,
            dict
        ):

            return {
                "available":
                    False,

                "error":
                    "Resposta JSON "
                    "da OpenAI inválida."
            }

        direction = str(
            parsed.get(
                "direction",
                "NEUTRO"
            )
        ).upper()

        if direction not in (
            "CALL",
            "PUT",
            "NEUTRO"
        ):

            direction = "NEUTRO"

        try:

            confidence = float(
                parsed.get(
                    "confidence",
                    0
                )
            )

        except Exception:

            confidence = 0

        risk = str(
            parsed.get(
                "risk",
                "HIGH"
            )
        ).upper()

        if risk not in (
            "LOW",
            "MEDIUM",
            "HIGH"
        ):

            risk = "HIGH"

        out = {
            "available":
                True,

            "direction":
                direction,

            "confidence":
                clamp(
                    confidence,
                    0,
                    100
                ),

            "confirmed":
                bool(
                    parsed.get(
                        "confirmed",
                        False
                    )
                ),

            "reason":
                str(
                    parsed.get(
                        "reason",
                        ""
                    )
                ),

            "risk":
                risk
        }

        oai_cache[
            key
        ] = (
            time.time(),
            out
        )

        return out

    except Exception as exc:

        return {
            "available":
                False,

            "error":
                str(exc)
        }


# ============================================================
# PRÓXIMA ENTRADA
# ============================================================

def next_entry(
    interval
):

    seconds = INTERVALS[
        interval
    ]

    timestamp = int(
        now().timestamp()
    )

    next_timestamp = (
        (
            timestamp
            //
            seconds
        )
        +
        1
    ) * seconds

    return datetime.fromtimestamp(
        next_timestamp,
        tz=BR_TZ
    )


# ============================================================
# SINAL
# ============================================================

async def signal(
    symbol,
    interval
):

    key = (
        f"{symbol}|"
        f"{interval}"
    )

    cached = signal_cache.get(
        key
    )

    if cached:

        if (
            time.time()
            -
            cached[0]
            <
            8
        ):

            return cached[1]

    # --------------------------------------------------------
    # CANDLES
    # --------------------------------------------------------

    try:

        cs = await candles(
            symbol,
            interval,
            80
        )

    except HTTPException as exc:

        if cached:

            old = dict(
                cached[1]
            )

            old[
                "status"
            ] = (
                "DADOS "
                "TEMPORARIAMENTE "
                "INDISPONÍVEIS"
            )

            old[
                "error"
            ] = str(
                exc.detail
            )

            return old

        # Não esconder o erro.
        return {
            "symbol":
                symbol,

            "interval":
                interval,

            "direction":
                "NEUTRO",

            "confidence":
                0,

            "entry_time":
                "",

            "expiry_time":
                "",

            "status":
                "ERRO NOS DADOS",

            "ai_confirmed":
                False,

            "risk":
                "HIGH",

            "error":
                str(
                    exc.detail
                )
        }

    # --------------------------------------------------------
    # REMOVE CANDLE ATUAL
    # --------------------------------------------------------

    if len(cs) > 1:

        cs_closed = cs[
            :-1
        ]

    else:

        cs_closed = cs

    local = local_ai(
        cs_closed
    )

    entry = next_entry(
        interval
    )

    expiry = (
        entry
        +
        timedelta(
            seconds=
            INTERVALS[
                interval
            ]
        )
    )

    base = {
        "symbol":
            symbol,

        "interval":
            interval,

        "direction":
            "NEUTRO",

        "confidence":
            local[
                "confidence"
            ],

        "entry_time":
            iso(entry),

        "expiry_time":
            iso(expiry),

        "status":
            "ANALISANDO",

        "ai_confirmed":
            False,

        "risk":
            "MEDIUM",

        "local_reason":
            local.get(
                "reason",
                ""
            ),

        "error":
            "",

        "ai_error":
            ""
    }

    # --------------------------------------------------------
    # SEM DIREÇÃO
    # --------------------------------------------------------

    if (
        local[
            "direction"
        ]
        ==
        "NEUTRO"
    ):

        base[
            "status"
        ] = (
            "SEM OPORTUNIDADE"
        )

        base[
            "risk"
        ] = "HIGH"

    # --------------------------------------------------------
    # CONFIRMAÇÃO
    # --------------------------------------------------------

    elif local[
        "confirmed"
    ]:

        ai = await openai_confirm(
            symbol,
            interval,
            cs_closed,
            local
        )

        base[
            "ai_error"
        ] = ai.get(
            "error",
            ""
        )

        # IA confirmou
        if (
            ai.get(
                "available"
            )
            and
            ai.get(
                "direction"
            )
            ==
            local[
                "direction"
            ]
            and
            ai.get(
                "confirmed"
            )
            and
            ai.get(
                "confidence",
                0
            )
            >=
            OAI_MIN
            and
            ai.get(
                "risk"
            )
            !=
            "HIGH"
        ):

            final_confidence = (
                local[
                    "confidence"
                ]
                *
                0.45
                +
                ai[
                    "confidence"
                ]
                *
                0.55
            )

            base.update(
                direction=
                    local[
                        "direction"
                    ],

                confidence=
                    round(
                        clamp(
                            final_confidence,
                            0,
                            97
                        ),
                        1
                    ),

                status=
                    "SINAL LIBERADO",

                ai_confirmed=
                    True,

                risk=
                    ai.get(
                        "risk",
                        "MEDIUM"
                    )
            )

        # OpenAI indisponível:
        # usa análise local.
        elif not ai.get(
            "available"
        ):

            base.update(
                direction=
                    local[
                        "direction"
                    ],

                status=
                    "SINAL LOCAL",

                ai_confirmed=
                    False,

                risk=
                    "MEDIUM"
            )

        # IA discordou
        else:

            base.update(
                direction=
                    "NEUTRO",

                confidence=
                    round(
                        min(
                            local[
                                "confidence"
                            ],

                            ai.get(
                                "confidence",
                                0
                            )
                        ),
                        1
                    ),

                status=
                    "AGUARDANDO "
                    "CONFIRMAÇÃO",

                ai_confirmed=
                    False,

                risk=
                    ai.get(
                        "risk",
                        "HIGH"
                    )
            )

    else:

        base[
            "status"
        ] = "MONITORANDO"

        base[
            "risk"
        ] = "MEDIUM"

    signal_cache[
        key
    ] = (
        time.time(),
        base
    )

    return base


# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
async def health():

    return {
        "status":
            "ok",

        "app":
            "MEGA IA",

        "version":
            "12.2.0",

        "brasilia_time":
            iso(now()),

        "twelve_data_configured":
            bool(TD_KEY),

        "openai_configured":
            bool(OAI_KEY),

        "openai_model":
            OAI_MODEL,

        "td_cache_seconds":
            TD_CACHE_SECONDS,

        "td_min_interval":
            TD_MIN_INTERVAL
    }


# ============================================================
# DIAGNÓSTICO
# ============================================================

@app.get("/diagnostic")
async def diagnostic(
    symbol="EUR/USD",
    interval="1min"
):

    value = await signal(
        symbol,
        interval
    )

    return {
        "ok":
            value.get(
                "status"
            )
            not in (
                "ERRO NOS DADOS",
            ),

        "signal":
            value,

        "config": {
            "twelve_data_configured":
                bool(TD_KEY),

            "openai_configured":
                bool(OAI_KEY),

            "openai_model":
                OAI_MODEL
        }
    }


# ============================================================
# SERVER TIME
# ============================================================

@app.get("/server-time")
async def server_time():

    return {
        "datetime":
            iso(now()),

        "timezone":
            "America/Sao_Paulo"
    }


# ============================================================
# LICENSE
# ============================================================

@app.get("/license")
async def license_info():

    try:

        expiration = datetime.strptime(
            LICENSE,
            "%Y-%m-%d"
        ).date()

        days = max(
            0,
            (
                expiration
                -
                now().date()
            ).days
        )

        active = (
            now().date()
            <=
            expiration
        )

    except Exception:

        active = False
        days = 0

    return {
        "active":
            active,

        "expires":
            LICENSE,

        "days_remaining":
            days,

        "whatsapp_1":
            WA1,

        "whatsapp_2":
            WA2,

        "instagram":
            IG
    }


# ============================================================
# CANDLES
# ============================================================

@app.get("/candles")
async def get_candles(
    symbol="EUR/USD",
    interval="1min",
    limit=50
):

    if symbol not in SYMBOLS:

        raise HTTPException(
            400,
            "Ativo inválido."
        )

    if interval not in INTERVALS:

        raise HTTPException(
            400,
            "Intervalo inválido."
        )

    return {
        "symbol":
            symbol,

        "interval":
            interval,

        "candles":
            await candles(
                symbol,
                interval,
                int(
                    clamp(
                        limit,
                        10,
                        100
                    )
                )
            )
    }


# ============================================================
# SIGNAL AI
# ============================================================

@app.get("/signal-ai")
async def signal_ai(
    symbol="EUR/USD",
    interval="1min"
):

    if (
        symbol not in SYMBOLS
        or
        interval not in INTERVALS
    ):

        raise HTTPException(
            400,
            "Ativo ou intervalo inválido."
        )

    return await signal(
        symbol,
        interval
    )


# ============================================================
# SIGNAL
# ============================================================

@app.get("/signal")
async def get_signal(
    symbol="EUR/USD",
    interval="1min"
):

    return await signal(
        symbol,
        interval
    )


# ============================================================
# AI ANALYSIS
# ============================================================

@app.get("/ai-analysis")
async def ai_analysis(
    symbol="EUR/USD",
    interval="1min"
):

    s = await signal(
        symbol,
        interval
    )

    return {
        key:
            s.get(key)

        for key in (
            "symbol",
            "interval",
            "direction",
            "confidence",
            "status",
            "ai_confirmed",
            "risk",
            "error",
            "ai_error",
            "local_reason"
        )
    }


# ============================================================
# RADAR
# ============================================================

@app.get("/radar")
async def radar(
    interval="1min"
):

    if interval not in INTERVALS:

        raise HTTPException(
            400,
            "Intervalo inválido."
        )

    output = []

    for symbol in SYMBOLS:

        try:

            value = await signal(
                symbol,
                interval
            )

            output.append(
                {
                    "symbol":
                        symbol,

                    "direction":
                        value.get(
                            "direction",
                            "NEUTRO"
                        ),

                    "confidence":
                        value.get(
                            "confidence",
                            0
                        ),

                    "status":
                        value.get(
                            "status",
                            "SEM DADOS"
                        ),

                    "error":
                        value.get(
                            "error",
                            ""
                        )
                }
            )

        except Exception as exc:

            output.append(
                {
                    "symbol":
                        symbol,

                    "direction":
                        "NEUTRO",

                    "confidence":
                        0,

                    "status":
                        "SEM DADOS",

                    "error":
                        str(exc)
                }
            )

    return output


# ============================================================
# SNIPER RANKING
# ============================================================

@app.get("/sniper-ranking")
async def ranking():

    return [
        {
            "name":
                name,

            "score":
                0
        }

        for name in (
            "Modelo A",
            "Modelo B",
            "Modelo C",
            "Modelo D"
        )
    ]


# ============================================================
# PERFORMANCE
# ============================================================

@app.get("/performance")
async def performance(
    interval="1min"
):

    wins = sum(
        1
        for x in results.values()
        if x.get(
            "result"
        ) == "WIN"
    )

    losses = sum(
        1
        for x in results.values()
        if x.get(
            "result"
        ) == "LOSS"
    )

    total = (
        wins
        +
        losses
    )

    accuracy = (
        round(
            wins
            /
            total
            *
            100,
            2
        )
        if total
        else 0
    )

    return {
        "wins":
            wins,

        "losses":
            losses,

        "total":
            total,

        "accuracy":
            accuracy
    }


# ============================================================
# RESULTADO
# ============================================================

@app.get("/result")
async def result(
    symbol="EUR/USD",
    interval="1min",
    direction="CALL",
    expiry_time=""
):

    if not expiry_time:

        raise HTTPException(
            400,
            "expiry_time é obrigatório."
        )

    key = (
        f"{symbol}|"
        f"{interval}|"
        f"{direction}|"
        f"{expiry_time}"
    )

    if key in results:

        return results[
            key
        ]

    if now() < parse(
        expiry_time
    ):

        return {
            "status":
                "PENDENTE",

            "result":
                None
        }

    try:

        cs = await candles(
            symbol,
            interval,
            20
        )

    except HTTPException:

        return {
            "status":
                "AGUARDANDO DADOS",

            "result":
                None
        }

    target = None

    for c in cs:

        try:

            if (
                parse(
                    c[
                        "datetime"
                    ]
                )
                >=
                parse(
                    expiry_time
                )
            ):

                target = c
                break

        except Exception:

            continue

    if not target:

        return {
            "status":
                "AGUARDANDO CANDLE",

            "result":
                None
        }

    direction = (
        direction.upper()
    )

    if (
        direction == "CALL"
        and
        target["close"]
        >
        target["open"]
    ):

        res = "WIN"

    elif (
        direction == "PUT"
        and
        target["close"]
        <
        target["open"]
    ):

        res = "WIN"

    else:

        res = "LOSS"

    output = {
        "status":
            "FINALIZADA",

        "result":
            res,

        "candle_time":
            target[
                "datetime"
            ]
    }

    results[
        key
    ] = output

    return output


# ============================================================
# PÁGINA WEB
# ============================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
async def home():

    return HTMLResponse(
        """
<!doctype html>

<html lang="pt-BR">

<head>

<meta charset="utf-8">

<meta
name="viewport"
content="width=device-width,initial-scale=1"
>

<title>MEGA IA</title>

<style>

body{
margin:0;
background:#070b12;
color:#eaf2ff;
font-family:Arial,sans-serif
}

.wrap{
max-width:1100px;
margin:auto;
padding:18px
}

.grid{
display:grid;
grid-template-columns:
repeat(4,1fr);
gap:12px;
margin-top:14px
}

.card{
background:#0e1522;
border:1px solid #1c2a3e;
border-radius:18px;
padding:16px
}

.signal{
grid-column:span 2;
text-align:center;
min-height:220px
}

.big{
font-size:32px;
font-weight:bold;
margin:8px
}

.call{
color:#4cff9b
}

.put{
color:#ff5c7a
}

.neutral{
color:#ffd166
}

.controls{
display:flex;
gap:10px;
margin-top:14px;
flex-wrap:wrap
}

select,
button{
background:#111d2e;
color:#fff;
border:1px solid #2a3d59;
border-radius:12px;
padding:11px
}

button{
cursor:pointer
}

.radar{
display:grid;
grid-template-columns:
repeat(3,1fr);
gap:8px
}

.radar div{
background:#101a29;
padding:10px;
border-radius:12px
}

.label,
small{
color:#8291a8;
font-size:11px
}

.error{
color:#ff9a9a;
font-size:12px;
margin-top:8px;
word-break:break-word
}

@media(max-width:700px){

.grid{
grid-template-columns:
1fr 1fr
}

.signal{
grid-column:span 2
}

.radar{
grid-template-columns:
1fr 1fr
}

}

@media(max-width:450px){

.grid{
grid-template-columns:
1fr
}

.signal{
grid-column:span 1
}

}

</style>

</head>

<body>

<div class="wrap">

<h1>🤖 MEGA IA</h1>

<small>
ANÁLISE EM TEMPO REAL • HORÁRIO DE BRASÍLIA
</small>

<div id="clock">
--:--:-- • Brasília
</div>

<div class="controls">

<select id="symbol"></select>

<select id="interval">

<option value="1min">
1min
</option>

<option value="5min">
5min
</option>

<option value="15min">
15min
</option>

<option value="30min">
30min
</option>

</select>

<button id="voiceButton">
🔊 Ativar voz
</button>

</div>


<div class="grid">

<div class="card signal">

<div style="font-size:65px">
🤖
</div>

<div class="label">
SINAL ATUAL
</div>

<div
id="direction"
class="big neutral"
>
AGUARDANDO
</div>

<div id="confidence">
Confiança: --
</div>

<div
id="signalError"
class="error"
>
</div>

</div>


<div class="card">

<div class="label">
ENTRADA
</div>

<div
id="entry"
class="big"
>
--:--:--
</div>

<div id="countdown">
--
</div>

</div>


<div class="card">

<div class="label">
STATUS IA
</div>

<div
id="status"
class="big"
style="font-size:20px"
>
MONITORANDO
</div>

<div id="risk">
Risco: --
</div>

</div>

</div>


<div class="grid">

<div class="card">

<div class="label">
WIN
</div>

<div
id="wins"
class="big call"
>
0
</div>

</div>


<div class="card">

<div class="label">
LOSS
</div>

<div
id="losses"
class="big put"
>
0
</div>

</div>


<div class="card">

<div class="label">
ASSERTIVIDADE
</div>

<div
id="accuracy"
class="big"
>
0%
</div>

</div>


<div class="card">

<div class="label">
RESULTADO
</div>

<div
id="result"
class="big"
>
--
</div>

</div>

</div>


<div
class="card"
style="margin-top:12px"
>

<b>
Radar de oportunidades
</b>

<div
id="radar"
class="radar"
style="margin-top:12px"
>

<div>
Radar iniciando...
</div>

</div>

</div>


<div
class="card"
style="margin-top:12px"
>

<div class="label">
LICENÇA
</div>

<div id="license">
Verificando...
</div>

</div>

</div>


<script>

const API_BASE = '';

const syms = [
'EUR/USD',
'GBP/USD',
'USD/JPY',
'AUD/USD',
'USD/CAD',
'USD/CHF',
'NZD/USD',
'EUR/JPY',
'GBP/JPY',
'EUR/GBP',
'BTC/USD',
'ETH/USD'
];


const el = {

symbol:
document.getElementById(
'symbol'
),

interval:
document.getElementById(
'interval'
),

voiceButton:
document.getElementById(
'voiceButton'
),

clock:
document.getElementById(
'clock'
),

direction:
document.getElementById(
'direction'
),

confidence:
document.getElementById(
'confidence'
),

entry:
document.getElementById(
'entry'
),

countdown:
document.getElementById(
'countdown'
),

status:
document.getElementById(
'status'
),

risk:
document.getElementById(
'risk'
),

wins:
document.getElementById(
'wins'
),

losses:
document.getElementById(
'losses'
),

accuracy:
document.getElementById(
'accuracy'
),

result:
document.getElementById(
'result'
),

radar:
document.getElementById(
'radar'
),

license:
document.getElementById(
'license'
),

signalError:
document.getElementById(
'signalError'
)

};


syms.forEach(
x =>
el.symbol.add(
new Option(x,x)
)
);


let cur = null;

let voiceEnabled = false;

let lastAnnounced = '';

let five = false;

let entered = false;

let reskey = '';

let radarBusy = false;

let signalBusy = false;


function speak(text){

if(
!voiceEnabled ||
!window.speechSynthesis
)
return;

speechSynthesis.cancel();

const u =
new SpeechSynthesisUtterance(
text
);

u.lang = 'pt-BR';

speechSynthesis.speak(
u
);

}


el.voiceButton.onclick =
() => {

voiceEnabled = true;

speak(
'Voz da Mega IA ativada.'
);

};


function ft(x){

if(!x)
return '--:--:--';

const d =
new Date(x);

if(
Number.isNaN(
d.getTime()
)
)
return '--:--:--';

return d.toLocaleTimeString(
'pt-BR',
{
hour12:false
}
);

}


async function get(url){

const r =
await fetch(
API_BASE + url,
{
cache:'no-store'
}
);

let data = null;

try{

data = await r.json();

}catch(e){

data = null;

}

if(!r.ok){

const detail =
data &&
(
data.detail ||
data.message
)
?
(
data.detail ||
data.message
)
:
'HTTP ' + r.status;

throw new Error(
detail
);

}

return data;

}


function renderSignal(
data
){

cur = data;

const direction =
data.direction ||
'NEUTRO';


el.direction.textContent =
direction;


el.direction.className =
'big ' +
(
direction === 'CALL'
?
'call'
:
direction === 'PUT'
?
'put'
:
'neutral'
);


el.confidence.textContent =
'Confiança: ' +
(
typeof data.confidence ===
'number'
?
data.confidence
:
'--'
)
+
'%';


el.entry.textContent =
ft(
data.entry_time
);


el.status.textContent =
data.status ||
'MONITORANDO';


el.risk.textContent =
'Risco: ' +
(
data.risk ||
'--'
);


el.signalError.textContent =
data.error ||
data.ai_error ||
'';


const k =
(data.symbol || '') +
'|' +
(data.entry_time || '') +
'|' +
direction;


if(
k !== lastAnnounced &&
direction !== 'NEUTRO' &&
data.entry_time
){

lastAnnounced = k;

speak(
'Atenção. A Mega IA encontrou uma oportunidade no ' +
(data.symbol || '')
.replace('/',' ') +
'. Sinal ' +
direction +
'. Entrada programada para ' +
ft(data.entry_time) +
'.'
);

}

}


async function sig(){

if(signalBusy)
return;

signalBusy = true;

try{

const data =
await get(
'/signal-ai?symbol=' +
encodeURIComponent(
el.symbol.value
) +
'&interval=' +
encodeURIComponent(
el.interval.value
)
);

renderSignal(
data
);

}catch(e){

el.status.textContent =
'ERRO DE CONEXÃO';

el.signalError.textContent =
e.message ||
'Falha ao consultar sinal.';

}finally{

signalBusy = false;

}

}


async function perf(){

try{

const p =
await get(
'/performance'
);

el.wins.textContent =
p.wins ?? 0;

el.losses.textContent =
p.losses ?? 0;

el.accuracy.textContent =
(
p.accuracy ?? 0
)
+
'%';

}catch(e){}

}


async function rad(){

if(radarBusy)
return;

radarBusy = true;

try{

el.radar.innerHTML =
'<div>Radar atualizando...</div>';

const a =
await get(
'/radar?interval=' +
encodeURIComponent(
el.interval.value
)
);

if(
!Array.isArray(a)
||
!a.length
){

el.radar.innerHTML =
'<div>Sem oportunidades no momento.</div>';

return;

}


el.radar.innerHTML =
a.map(
x => {

const direction =
x.direction ||
'NEUTRO';

return `
<div>
<b>${x.symbol}</b>
<br>

<span class="${
direction === 'CALL'
?
'call'
:
direction === 'PUT'
?
'put'
:
'neutral'
}">
${direction}
</span>

•
${x.confidence ?? 0}%

<br>

<small>
${x.status || 'SEM DADOS'}
</small>

${
x.error
?
`<div class="error">
${x.error}
</div>`
:
''
}

</div>
`;

}
).join('');


}catch(e){

el.radar.innerHTML =
`<div class="error">
Radar: ${
e.message ||
'sem dados'
}
</div>`;

}finally{

radarBusy = false;

}

}


async function lic(){

try{

const x =
await get(
'/license'
);

el.license.textContent =
x.active
?
`● LICENÇA ATIVA • ${x.expires} • ${x.days_remaining} dias restantes`
:
`● LICENÇA EXPIRADA • ${x.whatsapp_1} / ${x.whatsapp_2} • ${x.instagram}`;

}catch(e){

el.license.textContent =
'Não foi possível verificar a licença.';

}

}


async function clk(){

try{

const x =
await get(
'/server-time'
);

el.clock.textContent =
ft(
x.datetime
)
+
' • Brasília';

}catch(e){}

}


function cd(){

if(
!cur ||
!cur.entry_time
){

el.countdown.textContent =
'--';

return;

}


const n =
Math.ceil(
(
new Date(
cur.entry_time
).getTime()
-
Date.now()
)
/
1000
);


el.countdown.textContent =
n > 0
?
'Entrada em ' +
n +
's'
:
'Entrada liberada';


if(
n === 5 &&
!five
){

five = true;

speak(
'Atenção. Entrada em 5 segundos.'
);

}


if(
n <= 0 &&
n > -2 &&
!entered
){

entered = true;

if(
cur.direction !==
'NEUTRO'
){

speak(
'Entrada liberada. ' +
cur.direction +
' agora.'
);

}

}

}


async function resultCheck(){

if(
!cur ||
cur.direction ===
'NEUTRO'
)
return;

try{

const x =
await get(
'/result?symbol=' +
encodeURIComponent(
cur.symbol
) +
'&interval=' +
encodeURIComponent(
cur.interval
) +
'&direction=' +
encodeURIComponent(
cur.direction
) +
'&expiry_time=' +
encodeURIComponent(
cur.expiry_time
)
);


if(x.result){

el.result.textContent =
x.result;

const k =
cur.symbol +
'|' +
cur.expiry_time;


if(k !== reskey){

reskey = k;

speak(
'Operação finalizada. Resultado ' +
x.result +
'.'
);

}

perf();

}

}catch(e){}

}


el.symbol.onchange =
() => {

lastAnnounced = '';

five = false;

entered = false;

cur = null;

el.result.textContent =
'--';

sig();

};


el.interval.onchange =
() => {

lastAnnounced = '';

five = false;

entered = false;

cur = null;

el.result.textContent =
'--';

sig();

rad();

};


sig();

perf();

rad();

lic();

clk();


setInterval(
sig,
10000
);


setInterval(
perf,
10000
);


setInterval(
rad,
120000
);


setInterval(
resultCheck,
5000
);


setInterval(
clk,
1000
);


setInterval(
cd,
250
);

</script>

</body>

</html>
"""
    )


# ============================================================
# START
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