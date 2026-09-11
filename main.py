import os
import asyncio
import time
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse


# ============================================================
# MEGA IA
# ============================================================

APP_NAME = "MEGA IA"
APP_VERSION = "11.0.0"

BR_TZ = ZoneInfo("America/Sao_Paulo")
UTC = timezone.utc


# ============================================================
# CONFIGURAÇÃO TWELVE DATA
# ============================================================

TWELVE_DATA_API_KEY = os.getenv(
    "TWELVE_DATA_API_KEY",
    ""
).strip()

BASE_URL = "https://api.twelvedata.com/time_series"


# ============================================================
# LICENÇA
# ============================================================

LICENSE_EXPIRES = os.getenv(
    "LICENSE_EXPIRES",
    "2026-12-31"
)

WHATSAPP_1 = os.getenv(
    "WHATSAPP_1",
    "55 84 99841-1282"
)

WHATSAPP_2 = os.getenv(
    "WHATSAPP_2",
    "55 84 99449-9442"
)

INSTAGRAM = os.getenv(
    "INSTAGRAM",
    "@Ismaelartur26"
)


# ============================================================
# CONFIGURAÇÕES DA IA
# ============================================================

ENTRY_WINDOW_SECONDS = 5

AI_MIN_CONFIDENCE = 68.0

CACHE_SECONDS = 20

RADAR_CACHE_SECONDS = 90

RANKING_CACHE_SECONDS = 180


# ============================================================
# TIMEFRAMES
# ============================================================

INTERVALS = {
    "1min": 60,
    "5min": 300,
    "15min": 900,
    "30min": 1800,
}


# ============================================================
# PARES DISPONÍVEIS
# ============================================================

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
    "ETH/USD",
]


RADAR_SYMBOLS = SYMBOLS


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION
)


# ============================================================
# CACHE
# ============================================================

_candle_cache: Dict[
    Tuple[str, str],
    Tuple[float, List[Dict[str, Any]]]
] = {}

_radar_cache: Dict[
    str,
    Tuple[float, Any]
] = {}

_ranking_cache: Dict[
    str,
    Tuple[float, Any]
] = {}


# ============================================================
# CONTROLE DE REQUISIÇÕES
# ============================================================

_api_lock = asyncio.Semaphore(4)


# ============================================================
# HORÁRIO
# ============================================================

def now_sp() -> datetime:
    return datetime.now(BR_TZ)


def parse_dt(value: Any) -> Optional[datetime]:

    if value is None:
        return None

    s = str(value).strip()

    s = s.replace(
        "Z",
        "+00:00"
    )

    try:

        dt = datetime.fromisoformat(s)

    except ValueError:

        dt = None

        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
        ):

            try:

                dt = datetime.strptime(
                    s,
                    fmt
                )

                break

            except ValueError:

                pass

        if dt is None:
            return None

    if dt.tzinfo is None:

        dt = dt.replace(
            tzinfo=UTC
        )

    return dt.astimezone(UTC)


def fmt_br(dt: datetime) -> str:

    return dt.astimezone(
        BR_TZ
    ).strftime(
        "%H:%M:%S"
    )


def iso_br(dt: datetime) -> str:

    return dt.astimezone(
        BR_TZ
    ).isoformat(
        timespec="seconds"
    )


# ============================================================
# LICENÇA
# ============================================================

def license_status() -> Dict[str, Any]:

    try:

        exp = datetime.strptime(
            LICENSE_EXPIRES,
            "%Y-%m-%d"
        ).date()

    except ValueError:

        exp = datetime(
            2026,
            12,
            31
        ).date()

    today = now_sp().date()

    active = today <= exp

    return {

        "active": active,

        "expires": exp.isoformat(),

        "days_remaining": max(
            (exp - today).days,
            0
        ),

        "whatsapp_1": WHATSAPP_1,

        "whatsapp_2": WHATSAPP_2,

        "instagram": INSTAGRAM,

    }


def require_active_license():

    status = license_status()

    if not status["active"]:

        raise HTTPException(
            status_code=403,
            detail="Licença expirada."
        )


# ============================================================
# CANDLES
# ============================================================

async def get_candles(
    symbol: str,
    interval: str,
    outputsize: int = 240
) -> List[Dict[str, Any]]:

    if symbol not in SYMBOLS:

        raise HTTPException(
            status_code=400,
            detail="Par não disponível."
        )

    if interval not in INTERVALS:

        raise HTTPException(
            status_code=400,
            detail="Período inválido."
        )

    if not TWELVE_DATA_API_KEY:

        raise HTTPException(
            status_code=500,
            detail=(
                "TWELVE_DATA_API_KEY "
                "não configurada no Render."
            )
        )

    key = (
        symbol,
        interval
    )

    cached = _candle_cache.get(key)

    if cached:

        age = time.time() - cached[0]

        if (
            age < CACHE_SECONDS
            and
            len(cached[1]) >= min(
                outputsize,
                240
            )
        ):

            return cached[1]

    params = {

        "symbol": symbol,

        "interval": interval,

        "outputsize": min(
            max(outputsize, 80),
            5000
        ),

        "apikey": TWELVE_DATA_API_KEY,

        "format": "JSON",

        "timezone": "UTC",

    }

    async with _api_lock:

        async with httpx.AsyncClient(
            timeout=15
        ) as client:

            response = await client.get(
                BASE_URL,
                params=params
            )

    if response.status_code != 200:

        raise HTTPException(
            status_code=502,
            detail=(
                "Twelve Data HTTP "
                f"{response.status_code}"
            )
        )

    data = response.json()

    if "values" not in data:

        raise HTTPException(
            status_code=502,
            detail=data.get(
                "message",
                "Twelve Data não retornou candles."
            )
        )

    result = []

    for item in data["values"]:

        dt = parse_dt(
            item.get("datetime")
        )

        if not dt:
            continue

        try:

            candle = {

                "time": dt,

                "open": float(
                    item["open"]
                ),

                "high": float(
                    item["high"]
                ),

                "low": float(
                    item["low"]
                ),

                "close": float(
                    item["close"]
                ),

                "volume": float(
                    item.get(
                        "volume",
                        0
                    ) or 0
                ),

            }

            result.append(candle)

        except (
            TypeError,
            ValueError
        ):

            continue

    result.sort(
        key=lambda x: x["time"]
    )

    _candle_cache[key] = (
        time.time(),
        result
    )

    return result


# ============================================================
# EMA
# ============================================================

def ema(
    values: List[float],
    period: int
) -> List[float]:

    if not values:
        return []

    multiplier = 2.0 / (
        period + 1.0
    )

    result = [
        values[0]
    ]

    for value in values[1:]:

        previous = result[-1]

        current = (
            value * multiplier
            +
            previous * (
                1 - multiplier
            )
        )

        result.append(
            current
        )

    return result


# ============================================================
# SMA
# ============================================================

def sma(
    values: List[float],
    period: int
) -> float:

    if not values:
        return 0.0

    count = min(
        period,
        len(values)
    )

    return sum(
        values[-count:]
    ) / count


# ============================================================
# RSI
# ============================================================

def rsi(
    values: List[float],
    period: int = 14
) -> List[float]:

    if len(values) < period + 1:

        return [
            50.0
        ] * len(values)

    gains = []

    losses = []

    for i in range(
        1,
        len(values)
    ):

        change = (
            values[i]
            -
            values[i - 1]
        )

        gains.append(
            max(change, 0.0)
        )

        losses.append(
            max(-change, 0.0)
        )

    average_gain = (
        sum(gains[:period])
        /
        period
    )

    average_loss = (
        sum(losses[:period])
        /
        period
    )

    result = [
        50.0
    ] * period

    if average_loss == 0:

        first_rsi = 100.0

    else:

        relative_strength = (
            average_gain
            /
            average_loss
        )

        first_rsi = (
            100
            -
            (
                100
                /
                (
                    1
                    +
                    relative_strength
                )
            )
        )

    result.append(
        first_rsi
    )

    for i in range(
        period,
        len(gains)
    ):

        average_gain = (
            (
                average_gain
                *
                (period - 1)
            )
            +
            gains[i]
        ) / period

        average_loss = (
            (
                average_loss
                *
                (period - 1)
            )
            +
            losses[i]
        ) / period

        if average_loss == 0:

            current_rsi = 100.0

        else:

            rs = (
                average_gain
                /
                average_loss
            )

            current_rsi = (
                100
                -
                (
                    100
                    /
                    (
                        1 + rs
                    )
                )
            )

        result.append(
            current_rsi
        )

    return result[
        -len(values):
    ]


# ============================================================
# DESVIO PADRÃO
# ============================================================

def stddev(
    values: List[float],
    period: int = 20
) -> float:

    data = values[
        -period:
    ]

    if len(data) < 2:

        return 0.0

    mean = sum(data) / len(data)

    variance = sum(
        (
            value - mean
        ) ** 2
        for value in data
    ) / len(data)

    return variance ** 0.5


# ============================================================
# ATR
# ============================================================

def atr(
    candles: List[Dict[str, Any]],
    period: int = 14
) -> float:

    if len(candles) < 2:
        return 0.0

    true_ranges = []

    for i in range(
        1,
        len(candles)
    ):

        current = candles[i]

        previous_close = (
            candles[i - 1]["close"]
        )

        tr = max(

            current["high"]
            -
            current["low"],

            abs(
                current["high"]
                -
                previous_close
            ),

            abs(
                current["low"]
                -
                previous_close
            )

        )

        true_ranges.append(
            tr
        )

    return sma(
        true_ranges,
        period
    )


# ============================================================
# CLAMP
# ============================================================

def clamp(
    value: float,
    minimum: float,
    maximum: float
) -> float:

    return max(
        minimum,
        min(
            maximum,
            value
        )
    )


# ============================================================
# DIREÇÃO DA VELA
# ============================================================

def candle_direction(
    candle: Dict[str, Any]
) -> int:

    if candle["close"] > candle["open"]:

        return 1

    if candle["close"] < candle["open"]:

        return -1

    return 0


# ============================================================
# SNIPER RSI
# ============================================================

def sniper_rsi(
    closed: List[Dict[str, Any]]
) -> Tuple[str, float]:

    closes = [
        candle["close"]
        for candle in closed
    ]

    if len(closes) < 30:

        return (
            "NONE",
            0.0
        )

    rsi9 = rsi(
        closes,
        9
    )[-1]

    rsi14 = rsi(
        closes,
        14
    )[-1]

    candle = closed[-1]

    body = abs(
        candle["close"]
        -
        candle["open"]
    )

    candle_range = max(
        candle["high"]
        -
        candle["low"],
        1e-12
    )

    # CALL

    if (
        rsi9 <= 35
        and
        rsi14 <= 45
        and
        candle["close"]
        >
        candle["open"]
    ):

        confidence = (

            70

            +

            (
                45
                -
                rsi14
            )
            * 0.7

            +

            (
                35
                -
                rsi9
            )
            * 0.5

            +

            (
                body
                /
                candle_range
            )
            * 5

        )

        return (
            "CALL",
            clamp(
                confidence,
                70,
                94
            )
        )

    # PUT

    if (
        rsi9 >= 65
        and
        rsi14 >= 55
        and
        candle["close"]
        <
        candle["open"]
    ):

        confidence = (

            70

            +

            (
                rsi14
                -
                55
            )
            * 0.7

            +

            (
                rsi9
                -
                65
            )
            * 0.5

            +

            (
                body
                /
                candle_range
            )
            * 5

        )

        return (
            "PUT",
            clamp(
                confidence,
                70,
                94
            )
        )

    # Momentum CALL

    if (
        rsi9 > 52
        and
        rsi14 > 50
        and
        candle["close"]
        >
        candle["open"]
    ):

        return (
            "CALL",
            64.0
        )

    # Momentum PUT

    if (
        rsi9 < 48
        and
        rsi14 < 50
        and
        candle["close"]
        <
        candle["open"]
    ):

        return (
            "PUT",
            64.0
        )

    return (
        "NONE",
        0.0
    )


# ============================================================
# SNIPER TENDÊNCIA
# ============================================================

def sniper_trend(
    closed: List[Dict[str, Any]]
) -> Tuple[str, float]:

    closes = [
        candle["close"]
        for candle in closed
    ]

    ema9 = ema(
        closes,
        9
    )[-1]

    ema20 = ema(
        closes,
        20
    )[-1]

    ema50 = ema(
        closes,
        50
    )[-1]

    close = closed[-1]["close"]

    if (
        ema9 > ema20
        and
        ema20 > ema50
        and
        close > ema9
    ):

        return (
            "CALL",
            72.0
        )

    if (
        ema9 < ema20
        and
        ema20 < ema50
        and
        close < ema9
    ):

        return (
            "PUT",
            72.0
        )

    return (
        "NONE",
        0.0
    )


# ============================================================
# SNIPER REJEIÇÃO
# ============================================================

def sniper_rejection(
    closed: List[Dict[str, Any]]
) -> Tuple[str, float]:

    candle = closed[-1]

    candle_range = max(
        candle["high"]
        -
        candle["low"],
        1e-12
    )

    body = abs(
        candle["close"]
        -
        candle["open"]
    )

    upper_wick = (
        candle["high"]
        -
        max(
            candle["open"],
            candle["close"]
        )
    )

    lower_wick = (
        min(
            candle["open"],
            candle["close"]
        )
        -
        candle["low"]
    )

    if (
        lower_wick
        >
        body * 1.8

        and

        lower_wick
        /
        candle_range
        >
        0.45

        and

        candle["close"]
        >
        candle["open"]
    ):

        return (
            "CALL",
            74.0
        )

    if (
        upper_wick
        >
        body * 1.8

        and

        upper_wick
        /
        candle_range
        >
        0.45

        and

        candle["close"]
        <
        candle["open"]
    ):

        return (
            "PUT",
            74.0
        )

    return (
        "NONE",
        0.0
    )


# ============================================================
# SNIPER ROMPIMENTO
# ============================================================

def sniper_breakout(
    closed: List[Dict[str, Any]]
) -> Tuple[str, float]:

    if len(closed) < 30:

        return (
            "NONE",
            0.0
        )

    candle = closed[-1]

    previous_highs = [
        x["high"]
        for x in closed[-21:-1]
    ]

    previous_lows = [
        x["low"]
        for x in closed[-21:-1]
    ]

    resistance = max(
        previous_highs
    )

    support = min(
        previous_lows
    )

    if (
        candle["close"]
        >
        resistance

        and

        candle["close"]
        >
        candle["open"]
    ):

        return (
            "CALL",
            76.0
        )

    if (
        candle["close"]
        <
        support

        and

        candle["close"]
        <
        candle["open"]
    ):

        return (
            "PUT",
            76.0
        )

    return (
        "NONE",
        0.0
    )


# ============================================================
# CANDIDATOS INTERNOS
#
# Estes nomes NÃO são enviados ao frontend.
# ============================================================

def get_sniper_candidates(
    closed: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:

    strategies = [

        (
            "SNIPER X",
            sniper_rsi
        ),

        (
            "SNIPER 01",
            sniper_trend
        ),

        (
            "SNIPER 02",
            sniper_rejection
        ),

        (
            "SNIPER 03",
            sniper_breakout
        ),

    ]

    result = []

    for name, function in strategies:

        direction, confidence = (
            function(closed)
        )

        if direction != "NONE":

            result.append({

                "name": name,

                "direction": direction,

                "confidence": round(
                    confidence,
                    2
                ),

            })

    return result


# ============================================================
# MOTOR DA MEGA IA
#
# Os indicadores ficam somente no servidor.
# ============================================================

def ai_multi_strategy(
    closed: List[Dict[str, Any]],
    trigger: Dict[str, Any]
) -> Dict[str, Any]:

    closes = [
        candle["close"]
        for candle in closed
    ]

    ema9 = ema(
        closes,
        9
    )[-1]

    ema20 = ema(
        closes,
        20
    )[-1]

    ema50 = ema(
        closes,
        50
    )[-1]

    rsi9 = rsi(
        closes,
        9
    )[-1]

    rsi14 = rsi(
        closes,
        14
    )[-1]

    current = closed[-1]

    previous = closed[-2]

    current_atr = max(
        atr(
            closed,
            14
        ),
        1e-12
    )

    candle_range = max(
        current["high"]
        -
        current["low"],
        1e-12
    )

    body = abs(
        current["close"]
        -
        current["open"]
    )

    body_ratio = (
        body
        /
        candle_range
    )

    upper_wick = (
        current["high"]
        -
        max(
            current["open"],
            current["close"]
        )
    )

    lower_wick = (
        min(
            current["open"],
            current["close"]
        )
        -
        current["low"]
    )

    direction = trigger[
        "direction"
    ]

    score = 0.0


    # ========================================================
    # TENDÊNCIA
    # ========================================================

    if direction == "CALL":

        if ema9 > ema20:
            score += 12

        if ema20 > ema50:
            score += 10

    else:

        if ema9 < ema20:
            score += 12

        if ema20 < ema50:
            score += 10


    # ========================================================
    # MOMENTUM
    # ========================================================

    if direction == "CALL":

        if (
            rsi9 > 50
            and
            rsi14 > 50
        ):

            score += 12

    else:

        if (
            rsi9 < 50
            and
            rsi14 < 50
        ):

            score += 12


    # ========================================================
    # PRICE ACTION
    # ========================================================

    if direction == "CALL":

        if current["close"] > current["open"]:
            score += 9

        if current["close"] > previous["high"]:
            score += 10

        if lower_wick > body:
            score += 6

    else:

        if current["close"] < current["open"]:
            score += 9

        if current["close"] < previous["low"]:
            score += 10

        if upper_wick > body:
            score += 6


    # ========================================================
    # BANDA / LOCALIZAÇÃO
    # ========================================================

    deviation = stddev(
        closes,
        20
    )

    middle = sma(
        closes,
        20
    )

    upper_band = (
        middle
        +
        2 * deviation
    )

    lower_band = (
        middle
        -
        2 * deviation
    )

    if direction == "CALL":

        if (
            current["close"] > middle
            and
            current["close"] < upper_band
        ):

            score += 8

    else:

        if (
            current["close"] < middle
            and
            current["close"] > lower_band
        ):

            score += 8


    # ========================================================
    # VOLATILIDADE
    # ========================================================

    volatility_ratio = (
        candle_range
        /
        current_atr
    )

    if (
        0.35
        <=
        volatility_ratio
        <=
        2.8
    ):

        score += 8


    # ========================================================
    # CONTEXTO DAS ÚLTIMAS VELAS
    # ========================================================

    recent_directions = [

        candle_direction(
            candle
        )

        for candle
        in closed[-4:]

    ]

    if direction == "CALL":

        if sum(recent_directions) > 0:
            score += 5

    else:

        if sum(recent_directions) < 0:
            score += 5


    # ========================================================
    # EVITA ENTRADA EXTREMAMENTE ESTICADA
    # ========================================================

    if direction == "CALL":

        if rsi9 > 82:
            score -= 8

    else:

        if rsi9 < 18:
            score -= 8


    # ========================================================
    # CONFIANÇA FINAL
    # ========================================================

    confidence = clamp(
        50
        +
        score * 0.58,
        50,
        97
    )

    confirmed = (
        confidence
        >=
        AI_MIN_CONFIDENCE
    )


    if confirmed:

        final_direction = direction

        message = (
            "Condições favoráveis "
            "identificadas. "
            "Entrada programada."
        )

    else:

        final_direction = "NEUTRO"

        message = (
            "Condições insuficientes. "
            "Aguardando uma oportunidade melhor."
        )


    return {

        "direction":
            final_direction,

        "confidence":
            round(
                confidence,
                2
            ),

        "confirmed":
            confirmed,

        "message":
            message,

    }


# ============================================================
# PRÓXIMO HORÁRIO DE ENTRADA
# ============================================================

def next_entry_time(
    last_closed: datetime,
    interval: str
) -> datetime:

    seconds = INTERVALS[
        interval
    ]

    entry = (
        last_closed
        +
        timedelta(
            seconds=seconds
        )
    )

    now = datetime.now(
        UTC
    )

    while entry <= now:

        entry += timedelta(
            seconds=seconds
        )

    return entry


# ============================================================
# CONSTRUIR SINAL
# ============================================================

async def build_signal(
    symbol: str,
    interval: str
) -> Dict[str, Any]:

    require_active_license()

    candles = await get_candles(
        symbol,
        interval,
        240
    )

    if len(candles) < 80:

        raise HTTPException(
            status_code=502,
            detail=(
                "Histórico insuficiente "
                "para análise."
            )
        )


    # ========================================================
    # IMPORTANTE:
    # A ÚLTIMA VELA É DESCARTADA.
    #
    # A IA trabalha somente com velas fechadas.
    # ========================================================

    closed = candles[:-1]


    # ========================================================
    # PROCURA UMA OPORTUNIDADE
    # ========================================================

    candidates = (
        get_sniper_candidates(
            closed
        )
    )


    # Nenhum gatilho
    if not candidates:

        return {

            "app": APP_NAME,

            "symbol": symbol,

            "interval": interval,

            "signal": "NEUTRO",

            "confidence": 0.0,

            "status": "MONITORANDO",

            "message": (
                "A Mega IA está "
                "monitorando o mercado."
            ),

            "entry_time": None,

            "entry_time_br": None,

            "entry_epoch": None,

            "expiry_time": None,

            "expiry_time_br": None,

            "expiry_epoch": None,

            "seconds_to_entry": None,

            "voice_event":
                "monitoring",

            "non_repaint_reference":
                True,

            "server_time_br":
                iso_br(
                    now_sp()
                ),

        }


    # ========================================================
    # UM ÚNICO SNIPER PODE ACIONAR A IA
    # ========================================================

    trigger = max(
        candidates,
        key=lambda x:
            x["confidence"]
    )


    # ========================================================
    # IA CONFIRMA
    # ========================================================

    ai = ai_multi_strategy(
        closed,
        trigger
    )


    # ========================================================
    # IA NÃO CONFIRMOU
    # ========================================================

    if (
        not ai["confirmed"]
        or
        ai["direction"]
        !=
        trigger["direction"]
    ):

        return {

            "app": APP_NAME,

            "symbol": symbol,

            "interval": interval,

            "signal": "NEUTRO",

            "confidence":
                ai["confidence"],

            "status":
                "MONITORANDO",

            "message": (
                "Uma oportunidade foi "
                "detectada, mas a IA "
                "ainda não confirmou "
                "a entrada."
            ),

            "entry_time": None,

            "entry_time_br": None,

            "entry_epoch": None,

            "expiry_time": None,

            "expiry_time_br": None,

            "expiry_epoch": None,

            "seconds_to_entry": None,

            "voice_event":
                "monitoring",

            "non_repaint_reference":
                True,

            "server_time_br":
                iso_br(
                    now_sp()
                ),

        }


    # ========================================================
    # HORÁRIO DA ENTRADA
    # ========================================================

    entry = next_entry_time(
        closed[-1]["time"],
        interval
    )


    expiry = (
        entry
        +
        timedelta(
            seconds=INTERVALS[
                interval
            ]
        )
    )


    now = datetime.now(
        UTC
    )


    seconds_to_entry = max(

        0,

        int(
            (
                entry
                -
                now
            ).total_seconds()
        )

    )


    in_entry_window = (
        seconds_to_entry
        <=
        ENTRY_WINDOW_SECONDS
    )


    # ========================================================
    # EVENTO DE VOZ
    # ========================================================

    if in_entry_window:

        voice_event = "entry_now"

        status = "ENTRAR_AGORA"

        message = (
            f"Entrada liberada para "
            f"{ai['direction']}."
        )

    else:

        voice_event = "opportunity"

        status = (
            "OPORTUNIDADE_ENCONTRADA"
        )

        message = (
            "A Mega IA encontrou "
            "uma oportunidade e "
            "está aguardando o "
            "horário de entrada."
        )


    # ========================================================
    # RESPOSTA PÚBLICA
    #
    # NÃO RETORNAMOS:
    # RSI
    # EMA
    # ADX
    # Bollinger
    # ATR
    # nomes dos Snipers
    # pesos
    # fórmulas
    # parâmetros internos
    # ========================================================

    return {

        "app": APP_NAME,

        "symbol": symbol,

        "interval": interval,

        "signal":
            ai["direction"],

        "confidence":
            ai["confidence"],

        "status":
            status,

        "message":
            message,

        "entry_time":
            entry.astimezone(
                BR_TZ
            ).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),

        "entry_time_br":
            fmt_br(entry),

        "entry_epoch":
            int(
                entry.timestamp()
            ),

        "expiry_time":
            expiry.astimezone(
                BR_TZ
            ).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),

        "expiry_time_br":
            fmt_br(expiry),

        "expiry_epoch":
            int(
                expiry.timestamp()
            ),

        "seconds_to_entry":
            seconds_to_entry,

        "entry_window_seconds":
            ENTRY_WINDOW_SECONDS,

        "voice_event":
            voice_event,

        "non_repaint_reference":
            True,

        "server_time_br":
            iso_br(
                now_sp()
            ),

    }


# ============================================================
# RADAR
# ============================================================

async def radar_data():

    require_active_license()

    cache_key = "all"

    cached = _radar_cache.get(
        cache_key
    )

    if cached:

        age = (
            time.time()
            -
            cached[0]
        )

        if age < RADAR_CACHE_SECONDS:

            return cached[1]


    result = []


    for symbol in RADAR_SYMBOLS:

        try:

            data = await build_signal(
                symbol,
                "1min"
            )

            if data["signal"] in (
                "CALL",
                "PUT"
            ):

                result.append({

                    "symbol":
                        symbol,

                    "signal":
                        data["signal"],

                    "confidence":
                        data["confidence"],

                    "entry_time":
                        data["entry_time_br"],

                    "status":
                        data["status"],

                })

        except Exception:

            continue


    result.sort(
        key=lambda x:
            x["confidence"],
        reverse=True
    )


    _radar_cache[
        cache_key
    ] = (
        time.time(),
        result
    )


    return result


# ============================================================
# RANKING
# ============================================================

async def ranking_data(
    interval: str = "1min"
):

    require_active_license()

    cached = _ranking_cache.get(
        interval
    )

    if cached:

        age = (
            time.time()
            -
            cached[0]
        )

        if age < RANKING_CACHE_SECONDS:

            return cached[1]


    candles_by_symbol = {}


    for symbol in SYMBOLS:

        try:

            candles_by_symbol[
                symbol
            ] = await get_candles(
                symbol,
                interval,
                240
            )

        except Exception:

            continue


    since = (
        datetime.now(UTC)
        -
        timedelta(hours=3)
    )


    stats = {

        "SNIPER X": {
            "signals": 0,
            "wins": 0
        },

        "SNIPER 01": {
            "signals": 0,
            "wins": 0
        },

        "SNIPER 02": {
            "signals": 0,
            "wins": 0
        },

        "SNIPER 03": {
            "signals": 0,
            "wins": 0
        },

    }


    for candles in (
        candles_by_symbol.values()
    ):

        if len(candles) < 90:
            continue


        closed = candles[:-1]


        for i in range(
            60,
            len(closed) - 1
        ):

            if (
                closed[i]["time"]
                <
                since
            ):

                continue


            sample = closed[
                :i + 1
            ]


            candidates = (
                get_sniper_candidates(
                    sample
                )
            )


            for candidate in candidates:

                name = candidate[
                    "name"
                ]

                direction = candidate[
                    "direction"
                ]


                stats[name][
                    "signals"
                ] += 1


                next_candle = (
                    closed[i + 1]
                )


                win = (

                    direction == "CALL"

                    and

                    next_candle[
                        "close"
                    ]
                    >
                    next_candle[
                        "open"
                    ]

                ) or (

                    direction == "PUT"

                    and

                    next_candle[
                        "close"
                    ]
                    <
                    next_candle[
                        "open"
                    ]

                )


                if win:

                    stats[name][
                        "wins"
                    ] += 1


    result = []


    for name, values in (
        stats.items()
    ):

        total = int(
            values["signals"]
        )

        wins = int(
            values["wins"]
        )

        losses = max(
            total - wins,
            0
        )


        accuracy = (

            wins
            /
            total
            *
            100

            if total

            else

            0.0

        )


        result.append({

            "name":
                name,

            "signals":
                total,

            "wins":
                wins,

            "losses":
                losses,

            "accuracy":
                round(
                    accuracy,
                    2
                ),

        })


    result.sort(

        key=lambda x:
            (
                x["accuracy"],
                x["wins"]
            ),

        reverse=True

    )


    for position, item in enumerate(
        result,
        1
    ):

        item["rank"] = position


    _ranking_cache[
        interval
    ] = (
        time.time(),
        result
    )


    return result


# ============================================================
# LOCALIZAR VELA
# ============================================================

def find_candle(
    candles: List[Dict[str, Any]],
    target: datetime
) -> Optional[
    Dict[str, Any]
]:

    target = (
        target
        .astimezone(UTC)
        .replace(
            microsecond=0
        )
    )


    for candle in candles:

        current = (
            candle["time"]
            .replace(
                microsecond=0
            )
        )

        if current == target:

            return candle


    return None


# ============================================================
# HOME
# ============================================================

HOME_HTML = """
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

/* =========================================================
   RESET
========================================================= */

* {
    box-sizing: border-box;
}


/* =========================================================
   BODY
========================================================= */

body {

    margin: 0;

    background:
        radial-gradient(
            circle at 20% 0%,
            #073363 0,
            #031322 30%,
            #020814 70%
        );

    color: #eaf4ff;

    font-family:
        Inter,
        Arial,
        sans-serif;

    min-height: 100vh;

}


/* =========================================================
   APP
========================================================= */

.app {

    max-width: 1450px;

    margin: auto;

    padding: 16px;

}


/* =========================================================
   HERO
========================================================= */

.hero {

    min-height: 190px;

    border:
        1px solid #12365d;

    border-radius: 22px;

    padding: 20px;

    display: flex;

    align-items: center;

    gap: 24px;

    background:
        radial-gradient(
            circle at 20% 20%,
            #06356a 0,
            #031322 45%,
            #020814 100%
        );

    box-shadow:
        0 0 35px #001d3d;

}


.robot {

    width: 120px;

    height: 120px;

    border-radius: 50%;

    display: grid;

    place-items: center;

    border:
        2px solid #0b9cff;

    background:
        #06182c;

    font-size: 64px;

    box-shadow:
        0 0 30px #007cff;

}


.brand h1 {

    font-size: 48px;

    margin: 0;

    letter-spacing: 2px;

}


.brand p {

    margin: 7px 0;

    color: #7ccaff;

    letter-spacing: 2px;

}


.status {

    margin-left: auto;

    border:
        1px solid #17456d;

    border-radius: 14px;

    padding: 14px 18px;

    min-width: 220px;

}


.dot {

    display: inline-block;

    width: 10px;

    height: 10px;

    border-radius: 50%;

    background: #18e69a;

    box-shadow:
        0 0 12px #18e69a;

}


/* =========================================================
   GRID
========================================================= */

.grid {

    display: grid;

    grid-template-columns:
        280px
        1fr
        330px;

    gap: 16px;

    margin-top: 16px;

}


/* =========================================================
   CARD
========================================================= */

.card {

    background:
        linear-gradient(
            145deg,
            #061426,
            #030b15
        );

    border:
        1px solid #123250;

    border-radius: 18px;

    padding: 16px;

    box-shadow:
        0 10px 30px #0008;

}


.title {

    font-weight: 800;

    font-size: 17px;

    margin-bottom: 12px;

    color: #d9eeff;

}


/* =========================================================
   SELECT
========================================================= */

select {

    width: 100%;

    background: #071a2d;

    color: white;

    border:
        1px solid #1b5c8d;

    border-radius: 11px;

    padding: 12px;

    margin-bottom: 10px;

}


/* =========================================================
   PARES
========================================================= */

.pair {

    width: 100%;

    text-align: left;

    padding: 12px;

    border:
        1px solid #123250;

    background: #061526;

    color: #dcecff;

    border-radius: 10px;

    margin: 4px 0;

    cursor: pointer;

}


.pair.active {

    border-color: #00a8ff;

    background: #082847;

}


/* =========================================================
   CENTRO
========================================================= */

.center {

    text-align: center;

}


.asset {

    font-size: 25px;

    font-weight: 800;

    margin: 6px;

}


.badge {

    display: inline-block;

    padding: 7px 12px;

    border-radius: 99px;

    background: #08243c;

    color: #7fcfff;

}


/* =========================================================
   SIGNAL
========================================================= */

.signal {

    font-size: 54px;

    font-weight: 900;

    margin: 20px auto;

    padding: 15px;

    border-radius: 30px;

    max-width: 500px;

}


.call {

    color: #19f59e;

    border:
        2px solid #13d982;

    box-shadow:
        0 0 35px #00e98a55;

}


.put {

    color: #ff5368;

    border:
        2px solid #ff425b;

    box-shadow:
        0 0 35px #ff284655;

}


.neutral {

    color: #ffc54d;

    border:
        2px solid #a96d00;

}


/* =========================================================
   CONFIDENCE
========================================================= */

.conf {

    font-size: 18px;

}


.conf strong {

    font-size: 34px;

}


/* =========================================================
   ENTRY
========================================================= */

.entry {

    margin-top: 16px;

    padding: 18px;

    border-radius: 16px;

    border:
        2px solid #b87900;

    background: #171102;

}


.entry.now {

    border-color: #00ed9b;

    background: #021a13;

    box-shadow:
        0 0 35px #00ed9b44;

}


.entry h2 {

    margin: 0;

    color: #ffc84d;

}


.entry.now h2 {

    color: #20f6a2;

}


.count {

    font-size: 36px;

    font-weight: 900;

    margin: 8px;

}


/* =========================================================
   VOICE
========================================================= */

.voice {

    width: 100%;

    padding: 13px;

    border-radius: 12px;

    border:
        1px solid #158cff;

    background: #082442;

    color: white;

    font-weight: 800;

    cursor: pointer;

    margin-top: 10px;

}


.voice.on {

    border-color: #16e69a;

    background: #063324;

}


/* =========================================================
   MESSAGE
========================================================= */

.ai-msg {

    text-align: left;

    margin-top: 16px;

    padding: 15px;

    border:
        1px solid #075fa2;

    border-radius: 14px;

    background: #041525;

}


.robotmini {

    font-size: 28px;

}


/* =========================================================
   STATS
========================================================= */

.stats {

    display: grid;

    grid-template-columns:
        1fr 1fr;

    gap: 10px;

}


.stat {

    padding: 14px;

    border:
        1px solid #153a5b;

    border-radius: 12px;

}


.num {

    font-size: 26px;

    font-weight: 900;

}


.good {

    color: #1df2a1;

}


.bad {

    color: #ff5468;

}


/* =========================================================
   RANKING
========================================================= */

.rankrow {

    display: grid;

    grid-template-columns:
        28px
        1fr
        60px;

    gap: 7px;

    padding: 11px 0;

    border-bottom:
        1px solid #12304a;

}


/* =========================================================
   SMALL
========================================================= */

.small {

    color: #83a6c4;

    font-size: 13px;

}


/* =========================================================
   CLOCK
========================================================= */

.clock {

    font-size: 32px;

    font-weight: 900;

    color: #5bc5ff;

}


/* =========================================================
   FOOTER
========================================================= */

.footer {

    margin-top: 16px;

    text-align: center;

    color: #7190aa;

    font-size: 12px;

}


/* =========================================================
   RESPONSIVE
========================================================= */

@media (
    max-width: 1050px
) {

    .grid {

        grid-template-columns: 1fr;

    }

    .hero {

        flex-wrap: wrap;

    }

    .status {

        margin-left: 0;

    }

    .brand h1 {

        font-size: 38px;

    }

}

</style>

</head>


<body>


<div class="app">


<!-- =====================================================
     CABEÇALHO
===================================================== -->

<section class="hero">


    <div class="robot">

        🤖

    </div>


    <div class="brand">

        <h1>

            MEGA IA

        </h1>


        <p>

            ANÁLISE INTELIGENTE • ENTRADAS EM TEMPO REAL

        </p>

    </div>


    <div class="status">

        <span class="dot"></span>

        <b> IA ONLINE </b>

        <br>

        <span class="small">

            Monitorando o mercado

        </span>

    </div>


</section>



<!-- =====================================================
     GRID PRINCIPAL
===================================================== -->

<div class="grid">


<!-- =====================================================
     ESQUERDA
===================================================== -->

<aside>


<div class="card">


    <div class="title">

        🔄 PARES DISPONÍVEIS

    </div>


    <div id="pairs">

    </div>


</div>



<div
    class="card"
    style="margin-top:16px"
>


    <div class="title">

        ⏱️ TEMPO GRÁFICO

    </div>


    <select id="interval">

        <option value="1min">

            1 minuto

        </option>

        <option value="5min">

            5 minutos

        </option>

        <option value="15min">

            15 minutos

        </option>

        <option value="30min">

            30 minutos

        </option>

    </select>


</div>


</aside>



<!-- =====================================================
     CENTRO
===================================================== -->

<main class="card center">


    <div class="small">

        ANÁLISE DO ATIVO

    </div>


    <div
        class="asset"
        id="asset"
    >

        EUR/USD

    </div>


    <span
        class="badge"
        id="tf"
    >

        1 minuto

    </span>


    <div
        id="signal"
        class="signal neutral"
    >

        AGUARDANDO

    </div>


    <div class="conf">

        Confiança da IA

        <br>

        <strong id="confidence">

            --%

        </strong>

    </div>



    <!-- =================================================
         MOMENTO DE ENTRADA
    ================================================== -->

    <div
        id="entryBox"
        class="entry"
    >


        <h2 id="entryTitle">

            MOMENTO DA ENTRADA

        </h2>


        <div id="entryText">

            A Mega IA está procurando
            uma oportunidade.

        </div>


        <div
            class="count"
            id="countdown"
        >

            --:--

        </div>


    </div>



    <!-- =================================================
         VOZ
    ================================================== -->

    <button
        id="voiceButton"
        class="voice"
        onclick="toggleVoice()"
    >

        🔇 ATIVAR VOZ DA MEGA IA

    </button>



    <!-- =================================================
         MENSAGEM DA IA
    ================================================== -->

    <div class="ai-msg">


        <span class="robotmini">

            🤖

        </span>


        <b>

            MEGA IA

        </b>


        <div
            id="aiMessage"
            style="margin-top:7px"
        >

            Monitorando o mercado...

        </div>


    </div>


</main>



<!-- =====================================================
     DIREITA
===================================================== -->

<aside>


<div class="card">


    <div class="title">

        📊 DESEMPENHO

    </div>


    <div class="stats">


        <div class="stat">

            Win

            <div
                id="wins"
                class="num good"
            >

                0

            </div>

        </div>


        <div class="stat">

            Loss

            <div
                id="losses"
                class="num bad"
            >

                0

            </div>

        </div>


        <div class="stat">

            Assertividade

            <div
                id="accuracy"
                class="num"
            >

                0%

            </div>

        </div>


        <div class="stat">

            Sinais

            <div
                id="signals"
                class="num"
            >

                0

            </div>

        </div>


    </div>


</div>



<!-- =====================================================
     RANKING
===================================================== -->

<div
    class="card"
    style="margin-top:16px"
>


    <div class="title">

        🔥 RANKING — 3 HORAS

    </div>


    <div id="ranking">

        <span class="small">

            Carregando...

        </span>

    </div>


</div>



<!-- =====================================================
     HORÁRIO
===================================================== -->

<div
    class="card"
    style="margin-top:16px"
>


    <div class="title">

        ⏰ HORÁRIO DE BRASÍLIA

    </div>


    <div
        class="clock"
        id="clock"
    >

        --:--:--

    </div>


    <div
        class="small"
        id="date"
    >

        --/--/----

    </div>


</div>


</aside>


</div>



<!-- =====================================================
     RADAR
===================================================== -->

<div
    class="card"
    style="margin-top:16px"
>


    <div class="title">

        📡 RADAR — PARES PRÓXIMOS DE SINAL

    </div>


    <div
        id="radar"
        class="small"
    >

        Analisando...

    </div>


</div>



<!-- =====================================================
     FOOTER
===================================================== -->

<div class="footer">

    MEGA IA • Sistema de análise técnica automatizada

    •

    Licença

    <span id="license">

        verificando...

    </span>

</div>


</div>



<script>


// =========================================================
// CONFIGURAÇÃO
// =========================================================

const SYMBOLS = %SYMBOLS%;


let selectedSymbol =
    "EUR/USD";


let voiceEnabled =
    false;


let lastOpportunity =
    "";


let lastEntry =
    "";


let lastResult =
    "";


let trackedSignal =
    null;


let wins =
    Number(
        localStorage.getItem(
            "mega_wins"
        ) || 0
    );


let losses =
    Number(
        localStorage.getItem(
            "mega_losses"
        ) || 0
    );


let signalCount =
    Number(
        localStorage.getItem(
            "mega_signals"
        ) || 0
    );


// =========================================================
// ATALHO
// =========================================================

function $(id) {

    return document.getElementById(id);

}


// =========================================================
// PARES
// =========================================================

function renderPairs() {

    $("pairs").innerHTML =
        SYMBOLS.map(
            symbol => `

                <button
                    class="pair ${
                        symbol === selectedSymbol
                        ? "active"
                        : ""
                    }"
                    onclick="selectPair('${symbol}')"
                >

                    ${symbol}

                    <span
                        style="
                            float:right;
                            color:#18e69a
                        "
                    >

                        ●

                    </span>

                </button>

            `
        ).join("");

}


function selectPair(symbol) {

    selectedSymbol =
        symbol;

    renderPairs();

    loadSignal();

}


// =========================================================
// INTERVALO
// =========================================================

function selectedInterval() {

    return $("interval").value;

}


// =========================================================
// VOZ
// =========================================================

function speak(text) {

    if (
        !voiceEnabled
        ||
        !("speechSynthesis" in window)
    ) {

        return;

    }


    speechSynthesis.cancel();


    const utterance =
        new SpeechSynthesisUtterance(
            text
        );


    utterance.lang =
        "pt-BR";


    utterance.rate =
        0.94;


    utterance.pitch =
        0.90;


    utterance.volume =
        1.0;


    const voices =
        speechSynthesis.getVoices();


    const brazilVoice =
        voices.find(
            voice =>
                (
                    voice.lang || ""
                )
                .toLowerCase()
                .startsWith(
                    "pt-br"
                )
        );


    if (brazilVoice) {

        utterance.voice =
            brazilVoice;

    }


    speechSynthesis.speak(
        utterance
    );

}


// =========================================================
// ATIVAR VOZ
// =========================================================

function toggleVoice() {

    if (
        !("speechSynthesis" in window)
    ) {

        alert(
            "Seu navegador não suporta voz."
        );

        return;

    }


    voiceEnabled =
        !voiceEnabled;


    $("voiceButton")
        .classList
        .toggle(
            "on",
            voiceEnabled
        );


    $("voiceButton").textContent =
        voiceEnabled
        ? "🔊 VOZ ATIVADA"
        : "🔇 ATIVAR VOZ DA MEGA IA";


    if (voiceEnabled) {

        speak(
            "Mega IA ativada. Monitoramento iniciado."
        );

    } else {

        speechSynthesis.cancel();

    }

}


// =========================================================
// VOZ - OPORTUNIDADE
// =========================================================

function sayOpportunity(data) {

    if (!voiceEnabled) {

        return;

    }


    const key =

        (data.symbol || "")
        +
        "_"
        +
        (data.interval || "")
        +
        "_"
        +
        (data.signal || "")
        +
        "_"
        +
        (data.entry_time || "");


    if (
        key === lastOpportunity
    ) {

        return;

    }


    lastOpportunity =
        key;


    const asset =
        (
            data.symbol || ""
        )
        .replace(
            "/",
            " "
        );


    speak(

        `Atenção. A Mega IA encontrou ` +
        `uma oportunidade no ${asset}. ` +
        `Sinal ${data.signal}. ` +
        `Entrada programada para ` +
        `${data.entry_time_br}.`

    );

}


// =========================================================
// VOZ - ENTRADA
// =========================================================

function sayEntry(data) {

    if (!voiceEnabled) {

        return;

    }


    const key =

        (data.symbol || "")
        +
        "_"
        +
        (data.interval || "")
        +
        "_"
        +
        (data.signal || "")
        +
        "_"
        +
        (data.entry_time || "");


    if (
        key === lastEntry
    ) {

        return;

    }


    lastEntry =
        key;


    speak(

        `Entrada liberada. ` +
        `${data.signal} agora.`

    );

}


// =========================================================
// CONTADOR
// =========================================================

function formatCountdown(
    seconds
) {

    seconds =
        Math.max(
            0,
            Math.floor(
                seconds
            )
        );


    const minutes =
        Math.floor(
            seconds / 60
        )
        .toString()
        .padStart(
            2,
            "0"
        );


    const secs =
        (
            seconds % 60
        )
        .toString()
        .padStart(
            2,
            "0"
        );


    return (
        minutes
        +
        ":"
        +
        secs
    );

}


// =========================================================
// ESTATÍSTICAS
// =========================================================

function updateStats() {

    $("wins").textContent =
        wins;


    $("losses").textContent =
        losses;


    $("signals").textContent =
        signalCount;


    const total =
        wins + losses;


    $("accuracy").textContent =

        total

        ?

        (
            (
                wins / total
            )
            *
            100
        ).toFixed(1)
        + "%"

        :

        "0%";

}


// =========================================================
// CARREGAR SINAL
// =========================================================

async function loadSignal() {

    try {

        const url =

            `/signal-ai?symbol=` +
            `${encodeURIComponent(
                selectedSymbol
            )}` +
            `&interval=` +
            `${selectedInterval()}`;


        const response =
            await fetch(url);


        const data =
            await response.json();


        trackedSignal =
            data;


        $("asset").textContent =
            data.symbol
            ||
            selectedSymbol;


        const names = {

            "1min":
                "1 minuto",

            "5min":
                "5 minutos",

            "15min":
                "15 minutos",

            "30min":
                "30 minutos"

        };


        $("tf").textContent =
            names[
                data.interval
            ]
            ||
            data.interval;


        const signal =
            data.signal
            ||
            "NEUTRO";


        $("confidence").textContent =

            Number(
                data.confidence
                ||
                0
            ).toFixed(1)
            +
            "%";


        $("aiMessage").textContent =
            data.message
            ||
            "Monitorando o mercado.";


        const signalBox =
            $("signal");


        if (
            signal === "CALL"
        ) {

            signalBox.className =
                "signal call";

            signalBox.textContent =
                "CALL ↑";

        }

        else if (
            signal === "PUT"
        ) {

            signalBox.className =
                "signal put";

            signalBox.textContent =
                "PUT ↓";

        }

        else {

            signalBox.className =
                "signal neutral";

            signalBox.textContent =
                "AGUARDANDO";

        }


        // =================================================
        // EXISTE OPORTUNIDADE
        // =================================================

        if (
            signal === "CALL"
            ||
            signal === "PUT"
        ) {


            if (
                data.voice_event
                ===
                "opportunity"
            ) {

                sayOpportunity(
                    data
                );

            }


            if (
                data.voice_event
                ===
                "entry_now"
            ) {

                sayEntry(
                    data
                );

            }


            if (
                data.entry_epoch
            ) {

                $("entryText").innerHTML =

                    `Entrada programada: ` +
                    `<b>${data.entry_time_br}</b>` +
                    `<br>` +
                    `Expiração: ${data.expiry_time_br}`;


                $("entryBox")
                    .classList
                    .toggle(
                        "now",
                        data.status
                        ===
                        "ENTRAR_AGORA"
                    );

            }

        }

        else {

            $("entryText").textContent =
                "A Mega IA está procurando uma oportunidade.";


            $("countdown").textContent =
                "--:--";


            $("entryBox")
                .classList
                .remove(
                    "now"
                );

        }


    }

    catch (error) {

        $("aiMessage").textContent =
            "Não foi possível atualizar a análise agora.";

    }

}


// =========================================================
// CONTAGEM REGRESSIVA
// =========================================================

function updateCountdown() {

    if (
        !trackedSignal
        ||
        !trackedSignal.entry_epoch
    ) {

        return;

    }


    const now =
        Math.floor(
            Date.now() / 1000
        );


    const seconds =
        trackedSignal.entry_epoch
        -
        now;


    $("countdown").textContent =
        formatCountdown(
            seconds
        );


    // =====================================================
    // AVISOS DOS 5 SEGUNDOS
    // =====================================================

    if (
        seconds <= 5
        &&
        seconds > 0
        &&
        trackedSignal.signal
    ) {

        if (
            trackedSignal._spokenSecond
            !==
            seconds
        ) {

            trackedSignal._spokenSecond =
                seconds;


            if (
                seconds === 5
            ) {

                speak(
                    "Atenção. Entrada em 5 segundos."
                );

            }

            else if (
                seconds === 4
            ) {

                speak(
                    "Entrada em 4 segundos."
                );

            }

            else if (
                seconds === 3
            ) {

                speak(
                    "Entrada em 3 segundos."
                );

            }

            else if (
                seconds === 2
            ) {

                speak(
                    "Entrada em 2 segundos."
                );

            }

            else if (
                seconds === 1
            ) {

                speak(
                    "Entrada em 1 segundo."
                );

            }

        }

    }


    // =====================================================
    // ENTRADA
    // =====================================================

    if (
        seconds <= 0
        &&
        trackedSignal.signal
    ) {

        if (
            trackedSignal.status
            !==
            "ENTRAR_AGORA"
        ) {

            trackedSignal.status =
                "ENTRAR_AGORA";


            $("entryBox")
                .classList
                .add(
                    "now"
                );


            sayEntry(
                trackedSignal
            );

        }

    }

}


// =========================================================
// RANKING
// =========================================================

async function loadRanking() {

    try {

        const url =
            `/sniper-ranking?interval=` +
            `${selectedInterval()}`;


        const response =
            await fetch(url);


        const data =
            await response.json();


        const items =
            data.items || [];


        $("ranking").innerHTML =

            items
            .slice(
                0,
                4
            )
            .map(
                item => `

                    <div class="rankrow">

                        <b>
                            ${item.rank}
                        </b>

                        <div>

                            <b>
                                ${item.name}
                            </b>

                            <br>

                            <span class="small">

                                ${item.wins}W /
                                ${item.losses}L

                            </span>

                        </div>

                        <b>

                            ${Number(
                                item.accuracy
                            ).toFixed(1)}%

                        </b>

                    </div>

                `
            )
            .join("");


        if (!items.length) {

            $("ranking").textContent =
                "Sem dados suficientes.";

        }


    }

    catch (error) {

        $("ranking").textContent =
            "Ranking indisponível.";

    }

}


// =========================================================
// RADAR
// =========================================================

async function loadRadar() {

    try {

        const response =
            await fetch(
                "/radar"
            );


        const data =
            await response.json();


        const items =
            data.items || [];


        $("radar").innerHTML =

            items
            .slice(
                0,
                8
            )
            .map(
                item => `

                    <span
                        style="
                            display:inline-block;
                            padding:10px 14px;
                            margin:4px;
                            border:1px solid #164568;
                            border-radius:12px
                        "
                    >

                        <b>
                            ${item.symbol}
                        </b>

                        <span
                            class="${
                                item.signal === "CALL"
                                ? "good"
                                : "bad"
                            }"
                        >

                            ${item.signal}

                        </span>

                        ${Number(
                            item.confidence
                        ).toFixed(0)}%

                    </span>

                `
            )
            .join("");


        if (!items.length) {

            $("radar").textContent =
                "Nenhuma oportunidade confirmada neste momento.";

        }


    }

    catch (error) {

        $("radar").textContent =
            "Radar indisponível.";

    }

}


// =========================================================
// LICENÇA
// =========================================================

async function loadLicense() {

    try {

        const response =
            await fetch(
                "/license"
            );


        const data =
            await response.json();


        if (data.active) {

            $("license").textContent =

                `ATIVA • validade ${data.expires}`;

        }

        else {

            $("license").textContent =

                `EXPIRADA • ${data.expires}`;

        }


    }

    catch (error) {

        $("license").textContent =
            "não verificada";

    }

}


// =========================================================
// RESULTADO
// =========================================================

async function checkResult() {

    if (
        !trackedSignal
        ||
        !trackedSignal.entry_time
        ||
        !trackedSignal.signal
    ) {

        return;

    }


    const key =

        trackedSignal.symbol
        +
        "_"
        +
        trackedSignal.entry_time
        +
        "_"
        +
        trackedSignal.signal;


    if (
        key === lastResult
    ) {

        return;

    }


    const now =
        Math.floor(
            Date.now() / 1000
        );


    if (
        !trackedSignal.expiry_epoch
        ||
        now
        <
        trackedSignal.expiry_epoch + 2
    ) {

        return;

    }


    try {

        const url =

            `/result?symbol=` +
            `${encodeURIComponent(
                trackedSignal.symbol
            )}` +
            `&interval=` +
            `${trackedSignal.interval}` +
            `&direction=` +
            `${trackedSignal.signal}` +
            `&entry_time=` +
            `${encodeURIComponent(
                trackedSignal.entry_time
            )}`;


        const response =
            await fetch(url);


        const data =
            await response.json();


        if (
            data.result === "WIN"
            ||
            data.result === "LOSS"
        ) {


            lastResult =
                key;


            if (
                data.result === "WIN"
            ) {

                wins++;

                signalCount++;

                localStorage.setItem(
                    "mega_wins",
                    wins
                );

                localStorage.setItem(
                    "mega_signals",
                    signalCount
                );


                speak(
                    "Operação finalizada. Resultado WIN."
                );

            }

            else {

                losses++;

                signalCount++;

                localStorage.setItem(
                    "mega_losses",
                    losses
                );

                localStorage.setItem(
                    "mega_signals",
                    signalCount
                );


                speak(
                    "Operação finalizada. Resultado LOSS."
                );

            }


            updateStats();

        }

    }

    catch (error) {

        console.log(
            "Resultado ainda indisponível."
        );

    }

}


// =========================================================
// RELÓGIO DE BRASÍLIA
// =========================================================

function updateClock() {

    const now =
        new Date();


    $("clock").textContent =

        now.toLocaleTimeString(
            "pt-BR",
            {
                hour12: false
            }
        );


    $("date").textContent =

        now.toLocaleDateString(
            "pt-BR"
        );

}


// =========================================================
// INICIALIZAÇÃO
// =========================================================

$("interval")
    .addEventListener(
        "change",
        function() {

            loadSignal();

            loadRanking();

        }
    );


renderPairs();

updateStats();

loadLicense();

loadSignal();

loadRanking();

loadRadar();

updateClock();


// Relógio
setInterval(
    updateClock,
    1000
);


// Contador
setInterval(
    updateCountdown,
    250
);


// Atualização da análise
setInterval(
    loadSignal,
    5000
);


// Verificação do resultado
setInterval(
    checkResult,
    5000
);


// Radar
setInterval(
    loadRadar,
    90000
);


// Ranking
setInterval(
    loadRanking,
    180000
);


</script>


</body>

</html>
""".replace(
    "%SYMBOLS%",
    json.dumps(
        SYMBOLS
    )
)


# ============================================================
# ENDPOINTS
# ============================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
async def home():

    return HOME_HTML


@app.get("/health")
async def health():

    return {

        "ok": True,

        "app": APP_NAME,

        "version":
            APP_VERSION,

        "time_br":
            iso_br(
                now_sp()
            ),

    }


@app.get("/server-time")
async def server_time():

    current = now_sp()

    return {

        "datetime":
            iso_br(current),

        "time":
            current.strftime(
                "%H:%M:%S"
            ),

        "date":
            current.strftime(
                "%d/%m/%Y"
            ),

        "timezone":
            "America/Sao_Paulo",

    }


@app.get("/license")
async def license():

    return license_status()


@app.get("/candles")
async def candles(
    symbol: str = Query(...),
    interval: str = Query("1min"),
    outputsize: int = Query(
        240,
        ge=80,
        le=5000
    )
):

    values = await get_candles(
        symbol,
        interval,
        outputsize
    )


    return {

        "symbol":
            symbol,

        "interval":
            interval,

        "values": [

            {
                **candle,

                "time":
                    iso_br(
                        candle["time"]
                    ),

            }

            for candle
            in values

        ],

    }


@app.get("/signal-ai")
async def signal_ai(
    symbol: str = Query(
        "EUR/USD"
    ),
    interval: str = Query(
        "1min"
    )
):

    return await build_signal(
        symbol,
        interval
    )


@app.get("/ai-analysis")
async def ai_analysis(
    symbol: str = Query(
        "EUR/USD"
    ),
    interval: str = Query(
        "1min"
    )
):

    return await build_signal(
        symbol,
        interval
    )


@app.get("/signal")
async def signal(
    symbol: str = Query(
        "EUR/USD"
    ),
    interval: str = Query(
        "1min"
    )
):

    return await build_signal(
        symbol,
        interval
    )


@app.get("/radar")
async def radar():

    return {

        "items":
            await radar_data(),

        "updated_at":
            iso_br(
                now_sp()
            ),

    }


@app.get("/sniper-ranking")
async def sniper_ranking(
    interval: str = Query(
        "1min"
    )
):

    return {

        "interval":
            interval,

        "period":
            "3h",

        "items":
            await ranking_data(
                interval
            ),

    }


@app.get("/result")
async def result(
    symbol: str = Query(...),
    interval: str = Query(
        "1min"
    ),
    direction: str = Query(...),
    entry_time: str = Query(...)
):

    require_active_license()


    dt = parse_dt(
        entry_time
    )


    if not dt:

        raise HTTPException(
            status_code=400,
            detail="entry_time inválido."
        )


    candles = await get_candles(
        symbol,
        interval,
        300
    )


    candle = find_candle(
        candles,
        dt
    )


    if not candle:

        return {

            "status":
                "PENDING",

            "result":
                "PENDING",

            "message":
                "A vela da entrada ainda não está disponível.",

        }


    direction = direction.upper()


    if direction not in (
        "CALL",
        "PUT"
    ):

        raise HTTPException(
            status_code=400,
            detail=(
                "direction deve ser "
                "CALL ou PUT."
            )
        )


    if (
        candle["close"]
        ==
        candle["open"]
    ):

        outcome = "DRAW"

    elif (

        direction == "CALL"

        and

        candle["close"]
        >
        candle["open"]

    ) or (

        direction == "PUT"

        and

        candle["close"]
        <
        candle["open"]

    ):

        outcome = "WIN"

    else:

        outcome = "LOSS"


    return {

        "status":
            "CLOSED",

        "result":
            outcome,

        "symbol":
            symbol,

        "interval":
            interval,

        "direction":
            direction,

        "entry_time":
            iso_br(
                candle["time"]
            ),

        "open":
            candle["open"],

        "close":
            candle["close"],

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