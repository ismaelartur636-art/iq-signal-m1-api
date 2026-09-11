import os
import asyncio
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse


# ============================================================
# CONFIGURAÇÃO
# ============================================================

APP_NAME = "Trade Sniper"
APP_VERSION = "10.2.0"

KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()

BASE_URL = "https://api.twelvedata.com/time_series"

SP_TZ = ZoneInfo("America/Sao_Paulo")

LICENSE_EXPIRES = os.getenv(
    "ISMAEL_TRADE_LICENSE_EXPIRES",
    "2026-12-31"
).strip()

LICENSE_WHATSAPP_1 = "55 84 99841-1282"
LICENSE_WHATSAPP_2 = "55 84 99449-9442"
LICENSE_INSTAGRAM = "@Ismaelartur26"


# ============================================================
# TIMEFRAMES
# ============================================================

ALLOWED_INTERVALS = {
    "1min": 1,
    "5min": 5,
    "15min": 15,
    "30min": 30,
}

ENTRY_WINDOW_SECONDS = 5

AI_MIN_CONFIDENCE = 68.0


# ============================================================
# ATIVOS
# ============================================================

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


# ============================================================
# ESTRATÉGIAS
# ============================================================

STRATEGIES = [
    ("rsi", "SNIPER X"),
    ("old_sniper", "SNIPER 01"),
    ("sniper_02", "SNIPER 02"),
    ("sniper_03", "SNIPER 03"),
]


app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION
)


# ============================================================
# CACHE / RATE LIMIT
# ============================================================

CACHE_TTL_SECONDS = 60.0
RATE_LIMIT_COOLDOWN_SECONDS = 20.0

CANDLE_CACHE: Dict[tuple, tuple] = {}
CANDLE_LOCKS: Dict[tuple, asyncio.Lock] = {}

RATE_LIMIT_UNTIL = 0.0

RADAR_CACHE: Dict[tuple, tuple] = {}
RADAR_TTL_SECONDS = 120.0

RANKING_CACHE: Dict[tuple, tuple] = {}
RANKING_TTL_SECONDS = 120.0


# ============================================================
# HORÁRIO
# ============================================================

def now_sp() -> datetime:
    return datetime.now(SP_TZ)


# ============================================================
# LICENÇA
# ============================================================

def license_status() -> Dict[str, Any]:

    try:
        expires = datetime.strptime(
            LICENSE_EXPIRES,
            "%Y-%m-%d"
        ).replace(tzinfo=SP_TZ)

    except ValueError:
        expires = datetime(
            2026,
            12,
            31,
            tzinfo=SP_TZ
        )

    expires = expires.replace(
        hour=23,
        minute=59,
        second=59
    )

    return {
        "active": now_sp() <= expires,
        "expires": expires.strftime("%d/%m/%Y"),
        "expires_iso": expires.isoformat(),
        "whatsapp_1": LICENSE_WHATSAPP_1,
        "whatsapp_2": LICENSE_WHATSAPP_2,
        "instagram": LICENSE_INSTAGRAM,
    }


def require_active_license():

    status = license_status()

    if not status["active"]:
        raise HTTPException(
            status_code=403,
            detail={
                "error": "LICENSE_EXPIRED",
                **status
            }
        )


# ============================================================
# DATA / HORA
# ============================================================

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
        pass

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M"
    ):

        try:
            return datetime.strptime(
                text,
                fmt
            ).replace(tzinfo=SP_TZ)

        except ValueError:
            pass

    raise ValueError(
        f"Timestamp inválido: {value}"
    )


# ============================================================
# BUSCA DE VELAS
# ============================================================

async def get_candles(
    symbol: str,
    interval: str,
    outputsize: int = 100
) -> List[Dict[str, Any]]:

    global RATE_LIMIT_UNTIL

    if not KEY:
        raise HTTPException(
            status_code=500,
            detail=(
                "TWELVE_DATA_API_KEY não configurada "
                "no Render."
            )
        )

    if symbol not in SYMBOLS:
        raise HTTPException(
            status_code=400,
            detail="Ativo inválido."
        )

    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(
            status_code=400,
            detail="Timeframe inválido."
        )

    requested = min(
        max(int(outputsize), 20),
        240
    )

    key = (symbol, interval)

    now = time.monotonic()

    cached = CANDLE_CACHE.get(key)

    if (
        cached
        and now - cached[0] < CACHE_TTL_SECONDS
        and len(cached[1]) >= requested
    ):
        return cached[1]

    if now < RATE_LIMIT_UNTIL:

        if cached:
            return cached[1]

        remaining = max(
            1,
            int(RATE_LIMIT_UNTIL - now)
        )

        raise HTTPException(
            status_code=429,
            detail={
                "error": "RATE_LIMIT",
                "message": (
                    f"Limite da Twelve Data atingido. "
                    f"Aguarde {remaining}s."
                ),
                "retry_after": remaining,
            }
        )

    lock = CANDLE_LOCKS.setdefault(
        key,
        asyncio.Lock()
    )

    async with lock:

        now = time.monotonic()

        cached = CANDLE_CACHE.get(key)

        if (
            cached
            and now - cached[0] < CACHE_TTL_SECONDS
            and len(cached[1]) >= requested
        ):
            return cached[1]

        if now < RATE_LIMIT_UNTIL:

            if cached:
                return cached[1]

            remaining = max(
                1,
                int(RATE_LIMIT_UNTIL - now)
            )

            raise HTTPException(
                status_code=429,
                detail={
                    "error": "RATE_LIMIT",
                    "message": (
                        f"Limite da Twelve Data atingido. "
                        f"Aguarde {remaining}s."
                    ),
                    "retry_after": remaining,
                }
            )

        params = {
            "symbol": SYMBOLS[symbol],
            "interval": interval,
            "outputsize": requested,
            "timezone": "America/Sao_Paulo",
            "order": "ASC",
        }

        try:

            async with httpx.AsyncClient(
                timeout=20.0
            ) as client:

                response = await client.get(
                    BASE_URL,
                    params=params,
                    headers={
                        "Authorization": f"apikey {KEY}"
                    },
                )

        except httpx.RequestError as exc:

            raise HTTPException(
                status_code=502,
                detail=(
                    "Não foi possível conectar "
                    "à Twelve Data."
                )
            ) from exc

        if response.status_code == 429:

            retry_header = response.headers.get(
                "Retry-After",
                ""
            )

            try:

                retry_after = (
                    max(
                        5,
                        min(
                            int(float(retry_header)),
                            120
                        )
                    )
                    if retry_header
                    else RATE_LIMIT_COOLDOWN_SECONDS
                )

            except ValueError:

                retry_after = (
                    RATE_LIMIT_COOLDOWN_SECONDS
                )

            RATE_LIMIT_UNTIL = (
                time.monotonic()
                + retry_after
            )

            raise HTTPException(
                status_code=429,
                detail={
                    "error": "RATE_LIMIT",
                    "message": (
                        "A Twelve Data informou "
                        "limite de requisições."
                    ),
                    "retry_after": int(
                        retry_after
                    ),
                }
            )

        if response.status_code >= 400:

            raise HTTPException(
                status_code=502,
                detail=(
                    f"Twelve Data retornou "
                    f"HTTP {response.status_code}."
                )
            )

        try:

            data = response.json()

        except ValueError as exc:

            raise HTTPException(
                status_code=502,
                detail=(
                    "Resposta inválida da "
                    "Twelve Data."
                )
            ) from exc

        if (
            data.get("status") == "error"
            or "values" not in data
        ):

            raise HTTPException(
                status_code=502,
                detail=data.get(
                    "message",
                    "Resposta inválida da Twelve Data."
                )
            )

        candles = []

        for item in data["values"]:

            try:

                candles.append({
                    "datetime": item["datetime"],
                    "open": float(item["open"]),
                    "high": float(item["high"]),
                    "low": float(item["low"]),
                    "close": float(item["close"]),
                    "volume": float(
                        item.get(
                            "volume",
                            0
                        ) or 0
                    ),
                })

            except (
                KeyError,
                TypeError,
                ValueError
            ):
                continue

        candles.sort(
            key=lambda x: parse_time(
                x["datetime"]
            )
        )

        if len(candles) < 20:

            raise HTTPException(
                status_code=502,
                detail=(
                    "Poucas velas retornadas "
                    "pela Twelve Data."
                )
            )

        CANDLE_CACHE[key] = (
            time.monotonic(),
            candles
        )

        return candles


# ============================================================
# INDICADORES
# ============================================================

def ema(values, period):

    if not values:
        return []

    alpha = 2.0 / (
        period + 1.0
    )

    result = [values[0]]

    for value in values[1:]:

        result.append(
            value * alpha
            + result[-1] * (1.0 - alpha)
        )

    return result


def rsi(values, period=14):

    if len(values) < period + 1:
        return [50.0] * len(values)

    gains = [0.0]
    losses = [0.0]

    for i in range(1, len(values)):

        change = (
            values[i]
            - values[i - 1]
        )

        gains.append(
            max(change, 0.0)
        )

        losses.append(
            max(-change, 0.0)
        )

    result = [50.0] * len(values)

    avg_gain = (
        sum(gains[1:period + 1])
        / period
    )

    avg_loss = (
        sum(losses[1:period + 1])
        / period
    )

    def calc(g, l):

        if l == 0:

            return (
                100.0
                if g > 0
                else 50.0
            )

        return (
            100.0
            - 100.0
            / (1.0 + g / l)
        )

    result[period] = calc(
        avg_gain,
        avg_loss
    )

    for i in range(
        period + 1,
        len(values)
    ):

        avg_gain = (
            (
                avg_gain * (period - 1)
                + gains[i]
            )
            / period
        )

        avg_loss = (
            (
                avg_loss * (period - 1)
                + losses[i]
            )
            / period
        )

        result[i] = calc(
            avg_gain,
            avg_loss
        )

    return result


def true_ranges(candles):

    result = []

    for i, c in enumerate(candles):

        if i == 0:

            result.append(
                c["high"] - c["low"]
            )

        else:

            pc = candles[i - 1]["close"]

            result.append(
                max(
                    c["high"] - c["low"],
                    abs(c["high"] - pc),
                    abs(c["low"] - pc)
                )
            )

    return result


# ============================================================
# UTILITÁRIOS DA IA
# ============================================================

def _clamp(
    value,
    low=0.0,
    high=1.0
):

    return max(
        low,
        min(high, float(value))
    )


def _safe_mean(values):

    return (
        sum(values) / len(values)
        if values
        else 0.0
    )


def _std(values):

    if len(values) < 2:
        return 0.0

    m = _safe_mean(values)

    return (
        sum(
            (x - m) ** 2
            for x in values
        )
        / len(values)
    ) ** 0.5


# ============================================================
# SNIPER / RSI / PRICE ACTION
# ============================================================

def analyze(
    candles,
    strategy="rsi"
):

    if len(candles) < 30:

        raise HTTPException(
            status_code=422,
            detail=(
                "Dados insuficientes "
                "para análise."
            )
        )

    # IMPORTANTE:
    # a última vela pode estar em formação.
    # Portanto a análise usa somente velas fechadas.
    closed = candles[:-1]

    reference = closed[-1]
    previous = closed[-2]

    next_candle = candles[-1]

    strategy = (
        strategy
        .lower()
        .strip()
    )

    def body(c):

        return abs(
            c["close"] - c["open"]
        )

    def rng(c):

        return max(
            c["high"] - c["low"],
            1e-12
        )

    # ========================================================
    # RSI
    # ========================================================

    if strategy == "rsi":

        closes = [
            float(c["close"])
            for c in closed
        ]

        rv9 = rsi(
            closes,
            9
        )

        rv14 = rsi(
            closes,
            14
        )

        i = len(closed) - 1

        r9 = rv9[i]
        r14 = rv14[i]

        if r9 > 50 and r14 > 50:

            signal = "CALL"

        elif r9 < 50 and r14 < 50:

            signal = "PUT"

        else:

            signal = "NEUTRO"

        confidence = (
            50
            if signal == "NEUTRO"
            else int(
                min(
                    95,
                    55
                    + min(
                        abs(r9 - 50),
                        abs(r14 - 50)
                    ) * 1.8
                )
            )
        )

        return {
            "signal": signal,
            "confidence": confidence,
            "reference_candle":
                reference["datetime"],
            "next_candle":
                next_candle["datetime"],
            "rsi9": round(r9, 2),
            "rsi14": round(r14, 2),
            "rsi_confluence":
                signal != "NEUTRO",
            "strategy": "SNIPER X",
            "strategy_code": "rsi",
            "non_repaint_reference": True,
            "ok": True,
        }

    # ========================================================
    # SNIPER 01
    # ========================================================

    if strategy == "old_sniper":

        rb = body(reference)
        rr = rng(reference)

        upper = (
            reference["high"]
            - max(
                reference["open"],
                reference["close"]
            )
        )

        lower = (
            min(
                reference["open"],
                reference["close"]
            )
            - reference["low"]
        )

        bullish = (
            reference["close"]
            > reference["open"]
        )

        bearish = (
            reference["close"]
            < reference["open"]
        )

        prev_bearish = (
            previous["close"]
            < previous["open"]
        )

        prev_bullish = (
            previous["close"]
            > previous["open"]
        )

        bull_engulf = (
            bullish
            and prev_bearish
            and reference["open"]
            <= previous["close"]
            and reference["close"]
            >= previous["open"]
        )

        bear_engulf = (
            bearish
            and prev_bullish
            and reference["open"]
            >= previous["close"]
            and reference["close"]
            <= previous["open"]
        )

        bull_rej = (
            bullish
            and lower
            >= max(
                rb * 1.2,
                rr * .30
            )
        )

        bear_rej = (
            bearish
            and upper
            >= max(
                rb * 1.2,
                rr * .30
            )
        )

        bull_break = (
            reference["high"]
            > previous["high"]
            and reference["close"]
            > previous["high"]
        )

        bear_break = (
            reference["low"]
            < previous["low"]
            and reference["close"]
            < previous["low"]
        )

        bull_strength = (
            bullish
            and rb >= rr * .60
            and upper <= rr * .20
        )

        bear_strength = (
            bearish
            and rb >= rr * .60
            and lower <= rr * .20
        )

        bull_structure = (
            bullish
            and reference["close"]
            >= previous["close"]
        )

        bear_structure = (
            bearish
            and reference["close"]
            <= previous["close"]
        )

        bull_score = sum([
            bull_engulf,
            bull_rej,
            bull_break,
            bull_strength,
            bull_structure
        ])

        bear_score = sum([
            bear_engulf,
            bear_rej,
            bear_break,
            bear_strength,
            bear_structure
        ])

        if (
            bull_score >= 3
            and bull_score > bear_score
        ):

            signal = "CALL"
            confidence = min(
                95,
                55 + bull_score * 7
            )

        elif (
            bear_score >= 3
            and bear_score > bull_score
        ):

            signal = "PUT"
            confidence = min(
                95,
                55 + bear_score * 7
            )

        else:

            signal = "NEUTRO"
            confidence = 50

        return {
            "signal": signal,
            "confidence": int(confidence),
            "reference_candle":
                reference["datetime"],
            "next_candle":
                next_candle["datetime"],
            "strategy": "SNIPER 01",
            "strategy_code":
                "old_sniper",
            "bull_score":
                bull_score,
            "bear_score":
                bear_score,
            "bullish_engulfing":
                bull_engulf,
            "bearish_engulfing":
                bear_engulf,
            "bullish_rejection":
                bull_rej,
            "bearish_rejection":
                bear_rej,
            "bullish_breakout":
                bull_break,
            "bearish_breakout":
                bear_break,
            "non_repaint_reference":
                True,
            "ok": True,
        }

    # ========================================================
    # SNIPER 02
    # ========================================================

    if strategy == "sniper_02":

        lookback = closed[-22:-4]

        resistance = max(
            c["high"]
            for c in lookback
        )

        support = min(
            c["low"]
            for c in lookback
        )

        def bullish_pin(c):

            b = body(c)
            r = rng(c)

            lo = (
                min(
                    c["open"],
                    c["close"]
                )
                - c["low"]
            )

            up = (
                c["high"]
                - max(
                    c["open"],
                    c["close"]
                )
            )

            return (
                c["close"] > c["open"]
                and lo >= max(
                    b * 1.5,
                    r * .40
                )
                and up <= r * .25
            )

        def bearish_pin(c):

            b = body(c)
            r = rng(c)

            up = (
                c["high"]
                - max(
                    c["open"],
                    c["close"]
                )
            )

            lo = (
                min(
                    c["open"],
                    c["close"]
                )
                - c["low"]
            )

            return (
                c["close"] < c["open"]
                and up >= max(
                    b * 1.5,
                    r * .40
                )
                and lo <= r * .25
            )

        def hammer(c):

            b = body(c)
            r = rng(c)

            lo = (
                min(
                    c["open"],
                    c["close"]
                )
                - c["low"]
            )

            up = (
                c["high"]
                - max(
                    c["open"],
                    c["close"]
                )
            )

            return (
                lo >= max(
                    b * 2,
                    r * .45
                )
                and up <= r * .20
            )

        def shooting_star(c):

            b = body(c)
            r = rng(c)

            up = (
                c["high"]
                - max(
                    c["open"],
                    c["close"]
                )
            )

            lo = (
                min(
                    c["open"],
                    c["close"]
                )
                - c["low"]
            )

            return (
                up >= max(
                    b * 2,
                    r * .45
                )
                and lo <= r * .20
            )

        def bull_engulf(a, b):

            return (
                b["close"] > b["open"]
                and a["close"] < a["open"]
                and b["open"]
                <= a["close"]
                and b["close"]
                >= a["open"]
            )

        def bear_engulf(a, b):

            return (
                b["close"] < b["open"]
                and a["close"] > a["open"]
                and b["open"]
                >= a["close"]
                and b["close"]
                <= a["open"]
            )

        call_setup = False
        put_setup = False

        call_pattern = ""
        put_pattern = ""

        for j in range(
            max(
                2,
                len(closed) - 5
            ),
            len(closed) - 2
        ):

            breakout = closed[j]
            retest = closed[j + 1]
            confirmation = closed[j + 2]

            if breakout["close"] > resistance:

                touched = (
                    retest["low"]
                    <= resistance * 1.0015
                )

                rejection = (
                    touched
                    and (
                        hammer(retest)
                        or bullish_pin(retest)
                        or bull_engulf(
                            breakout,
                            retest
                        )
                    )
                )

                confirmed = (
                    confirmation["close"]
                    > confirmation["open"]
                    and confirmation["close"]
                    > retest["high"]
                )

                if rejection and confirmed:

                    call_setup = True

                    if hammer(retest):
                        call_pattern = "Martelo"

                    elif bullish_pin(retest):
                        call_pattern = (
                            "Pin Bar de alta"
                        )

                    else:
                        call_pattern = (
                            "Engolfo de alta"
                        )

            if breakout["close"] < support:

                touched = (
                    retest["high"]
                    >= support * .9985
                )

                rejection = (
                    touched
                    and (
                        shooting_star(retest)
                        or bearish_pin(retest)
                        or bear_engulf(
                            breakout,
                            retest
                        )
                    )
                )

                confirmed = (
                    confirmation["close"]
                    < confirmation["open"]
                    and confirmation["close"]
                    < retest["low"]
                )

                if rejection and confirmed:

                    put_setup = True

                    if shooting_star(retest):
                        put_pattern = (
                            "Shooting Star"
                        )

                    elif bearish_pin(retest):
                        put_pattern = (
                            "Pin Bar de baixa"
                        )

                    else:
                        put_pattern = (
                            "Engolfo de baixa"
                        )

        if call_setup and not put_setup:

            signal = "CALL"
            confidence = 88

        elif put_setup and not call_setup:

            signal = "PUT"
            confidence = 88

        else:

            signal = "NEUTRO"
            confidence = 50

        return {
            "signal": signal,
            "confidence": confidence,
            "reference_candle":
                reference["datetime"],
            "next_candle":
                next_candle["datetime"],
            "strategy": "SNIPER 02",
            "strategy_code":
                "sniper_02",
            "resistance":
                resistance,
            "support":
                support,
            "call_setup":
                call_setup,
            "put_setup":
                put_setup,
            "call_pattern":
                call_pattern,
            "put_pattern":
                put_pattern,
            "non_repaint_reference":
                True,
            "ok": True,
        }

    # ========================================================
    # SNIPER 03
    # ========================================================

    if strategy == "sniper_03":

        if len(closed) < 45:

            raise HTTPException(
                status_code=422,
                detail=(
                    "Dados insuficientes "
                    "para SNIPER 03."
                )
            )

        closes = [
            c["close"]
            for c in closed
        ]

        highs = [
            c["high"]
            for c in closed
        ]

        lows = [
            c["low"]
            for c in closed
        ]

        ema20 = ema(
            closes,
            20
        )

        ema50 = ema(
            closes,
            50
        )

        trv = true_ranges(
            closed
        )

        atr = (
            sum(trv[-14:])
            / 14
        )

        tol = max(
            atr * .35,
            abs(
                reference["close"]
            ) * .0005
        )

        prior = closed[-6:-1]

        up = (
            ema20[-1]
            > ema50[-1]
            and ema20[-1]
            > ema20[-4]
            and reference["close"]
            > ema50[-1]
        )

        down = (
            ema20[-1]
            < ema50[-1]
            and ema20[-1]
            < ema20[-4]
            and reference["close"]
            < ema50[-1]
        )

        rb = body(reference)
        rr = rng(reference)

        lw = (
            min(
                reference["open"],
                reference["close"]
            )
            - reference["low"]
        )

        uw = (
            reference["high"]
            - max(
                reference["open"],
                reference["close"]
            )
        )

        bull_rej = (
            reference["close"]
            > reference["open"]
            and lw >= max(
                rb * 1.2,
                rr * .30
            )
        )

        bear_rej = (
            reference["close"]
            < reference["open"]
            and uw >= max(
                rb * 1.2,
                rr * .30
            )
        )

        pull_call = (
            any(
                c["close"]
                < c["open"]
                for c in prior[-3:]
            )
            and (
                abs(
                    reference["low"]
                    - ema20[-1]
                ) <= tol
            )
        )

        pull_put = (
            any(
                c["close"]
                > c["open"]
                for c in prior[-3:]
            )
            and (
                abs(
                    reference["high"]
                    - ema20[-1]
                ) <= tol
            )
        )

        conf_call = (
            reference["close"]
            > reference["open"]
            and reference["close"]
            >= previous["high"]
        )

        conf_put = (
            reference["close"]
            < reference["open"]
            and reference["close"]
            <= previous["low"]
        )

        call_score = (
            (2 if up else 0)
            + (2 if pull_call else 0)
            + (1 if bull_rej else 0)
            + (2 if conf_call else 0)
        )

        put_score = (
            (2 if down else 0)
            + (2 if pull_put else 0)
            + (1 if bear_rej else 0)
            + (2 if conf_put else 0)
        )

        if (
            call_score >= 5
            and call_score > put_score
        ):

            signal = "CALL"

            confidence = min(
                95,
                55 + call_score * 5
            )

        elif (
            put_score >= 5
            and put_score > call_score
        ):

            signal = "PUT"

            confidence = min(
                95,
                55 + put_score * 5
            )

        else:

            signal = "NEUTRO"
            confidence = 50

        return {
            "signal": signal,
            "confidence": confidence,
            "reference_candle":
                reference["datetime"],
            "next_candle":
                next_candle["datetime"],
            "strategy": "SNIPER 03",
            "strategy_code":
                "sniper_03",
            "trend":
                "ALTA"
                if up
                else "BAIXA"
                if down
                else "NEUTRA",
            "pullback_call":
                pull_call,
            "pullback_put":
                pull_put,
            "bullish_rejection":
                bull_rej,
            "bearish_rejection":
                bear_rej,
            "call_score":
                call_score,
            "put_score":
                put_score,
            "non_repaint_reference":
                True,
            "ok": True,
        }

    raise HTTPException(
        status_code=400,
        detail="Estratégia inválida."
    )


# ============================================================
# IA HÍBRIDA
# ============================================================

def ai_analyze(
    candles: List[Dict[str, Any]]
) -> Dict[str, Any]:

    if len(candles) < 60:

        return {
            "signal": "NEUTRO",
            "confidence": 0.0,
            "score": 0.0,
            "quality":
                "DADOS_INSUFICIENTES",
            "features": {},
            "reason": (
                "São necessárias "
                "pelo menos 60 velas."
            ),
        }

    # Somente candles fechados
    closed = candles[:-1]

    closes = [
        c["close"]
        for c in closed
    ]

    highs = [
        c["high"]
        for c in closed
    ]

    lows = [
        c["low"]
        for c in closed
    ]

    volumes = [
        c.get(
            "volume",
            0.0
        )
        for c in closed
    ]

    e9 = ema(
        closes,
        9
    )

    e20 = ema(
        closes,
        20
    )

    e50 = ema(
        closes,
        50
    )

    r9 = rsi(
        closes,
        9
    )

    r14 = rsi(
        closes,
        14
    )

    trs = true_ranges(
        closed
    )

    atr14 = (
        _safe_mean(
            trs[-14:]
        )
        if trs
        else 0.0
    )

    last = closed[-1]
    prev = closed[-2]

    body = abs(
        last["close"]
        - last["open"]
    )

    rng = max(
        last["high"]
        - last["low"],
        1e-12
    )

    body_ratio = (
        body / rng
    )

    upper_wick = (
        last["high"]
        - max(
            last["open"],
            last["close"]
        )
    )

    lower_wick = (
        min(
            last["open"],
            last["close"]
        )
        - last["low"]
    )

    # ========================================================
    # TENDÊNCIA
    # ========================================================

    trend_fast = (
        e9[-1]
        - e20[-1]
    ) / max(
        atr14,
        1e-12
    )

    trend_slow = (
        e20[-1]
        - e50[-1]
    ) / max(
        atr14,
        1e-12
    )

    slope20 = (
        e20[-1]
        - e20[-6]
    ) / max(
        atr14,
        1e-12
    )

    trend_call = (
        (
            1 if trend_fast > 0
            else -1
        )
        + (
            1 if trend_slow > 0
            else -1
        )
        + (
            1 if slope20 > 0
            else -1
        )
    ) / 3.0

    # ========================================================
    # RSI
    # ========================================================

    rsi_bias = (
        (
            r9[-1] - 50
        ) / 50
        +
        (
            r14[-1] - 50
        ) / 50
    ) / 2

    # ========================================================
    # ESTRUTURA
    # ========================================================

    look = 20

    hh = max(
        highs[-look:]
    )

    ll = min(
        lows[-look:]
    )

    pos = (
        last["close"] - ll
    ) / max(
        hh - ll,
        1e-12
    )

    structure_bias = (
        pos - 0.5
    ) * 2.0

    # ========================================================
    # PRICE ACTION
    # ========================================================

    candle_bias = (
        last["close"]
        - last["open"]
    ) / rng

    rejection_bias = 0.0

    if (
        lower_wick / rng > 0.45
        and last["close"]
        > last["open"]
    ):

        rejection_bias += 0.35

    if (
        upper_wick / rng > 0.45
        and last["close"]
        < last["open"]
    ):

        rejection_bias -= 0.35

    # ========================================================
    # VOLUME
    # ========================================================

    recent_vol = (
        volumes[-21:-1]
    )

    vol_mean = _safe_mean(
        recent_vol
    )

    if (
        vol_mean > 0
        and volumes[-1] > 0
    ):

        volume_factor = _clamp(
            volumes[-1]
            / vol_mean,
            0.25,
            2.5
        )

    else:

        volume_factor = 1.0

    volume_bias = (
        candle_bias
        * (
            volume_factor
            - 1.0
        )
    )

    # ========================================================
    # VOLATILIDADE
    # ========================================================

    recent_ranges = [
        c["high"] - c["low"]
        for c in closed[-20:]
    ]

    avg_range = _safe_mean(
        recent_ranges
    )

    volatility_factor = _clamp(
        (
            last["high"]
            - last["low"]
        )
        / max(
            avg_range,
            1e-12
        ),
        0.0,
        3.0
    )

    # ========================================================
    # PADRÃO DE 2 VELAS
    # ========================================================

    pattern_bias = 0.0

    if (
        last["close"]
        > last["open"]
        and prev["close"]
        < prev["open"]
    ):

        if (
            last["close"]
            >= prev["open"]
            and last["open"]
            <= prev["close"]
        ):

            pattern_bias += 0.75

    elif (
        last["close"]
        < last["open"]
        and prev["close"]
        > prev["open"]
    ):

        if (
            last["close"]
            <= prev["open"]
            and last["open"]
            >= prev["close"]
        ):

            pattern_bias -= 0.75

    else:

        pattern_bias += (
            0.20
            * candle_bias
        )

    # ========================================================
    # REGIME
    # ========================================================

    regime = (
        "TENDENCIA"
        if abs(trend_slow) >= 0.35
        else "LATERAL"
    )

    # ========================================================
    # SCORE IA
    # ========================================================

    raw = (
        0.32 * trend_call
        + 0.22 * rsi_bias
        + 0.12 * structure_bias
        + 0.12 * candle_bias
        + 0.08 * rejection_bias
        + 0.06 * pattern_bias
        + 0.05 * volume_bias
        + 0.03
        * slope20
        / max(
            abs(slope20),
            1.0
        )
    )

    confirmation = 0.0

    if (
        regime == "TENDENCIA"
        and trend_call
        * rsi_bias > 0
    ):

        confirmation += 0.10

    if body_ratio >= 0.55:
        confirmation += 0.05

    if volatility_factor >= 0.80:
        confirmation += 0.03

    if abs(pattern_bias) >= 0.50:
        confirmation += 0.04

    conflict = 0.0

    if (
        trend_call
        * rsi_bias < -0.20
    ):

        conflict = 0.12

    directional = _clamp(
        abs(raw),
        0.0,
        1.0
    )

    confidence = _clamp(
        50.0
        + directional * 43.0
        + confirmation * 100.0
        - conflict * 100.0,
        50.0,
        98.0
    )

    if (
        abs(raw) < 0.18
        or confidence < 62.0
    ):

        signal = "NEUTRO"
        quality = "BAIXA"

    elif raw > 0:

        signal = "CALL"

        quality = (
            "ALTA"
            if confidence >= 78
            else "MEDIA"
        )

    else:

        signal = "PUT"

        quality = (
            "ALTA"
            if confidence >= 78
            else "MEDIA"
        )

    return {
        "signal": signal,
        "confidence":
            round(
                confidence,
                1
            ),
        "score":
            round(
                raw * 100,
                1
            ),
        "quality":
            quality,
        "regime":
            regime,
        "features": {
            "rsi9":
                round(
                    r9[-1],
                    2
                ),
            "rsi14":
                round(
                    r14[-1],
                    2
                ),
            "ema9":
                round(
                    e9[-1],
                    8
                ),
            "ema20":
                round(
                    e20[-1],
                    8
                ),
            "ema50":
                round(
                    e50[-1],
                    8
                ),
            "atr14":
                round(
                    atr14,
                    8
                ),
            "body_ratio":
                round(
                    body_ratio,
                    3
                ),
            "volatility_factor":
                round(
                    volatility_factor,
                    3
                ),
            "trend_score":
                round(
                    trend_call,
                    3
                ),
            "structure_score":
                round(
                    structure_bias,
                    3
                ),
            "pattern_score":
                round(
                    pattern_bias,
                    3
                ),
        },
        "reason": (
            f"Regime {regime}; "
            f"tendência={trend_call:+.2f}; "
            f"RSI={r9[-1]:.1f}/"
            f"{r14[-1]:.1f}; "
            f"força da vela="
            f"{body_ratio:.2f}."
        ),
        "non_repaint_reference":
            True,
    }


# ============================================================
# IA MULTI-ESTRATÉGIA
# ============================================================

def ai_multi_strategy(
    candles: List[Dict[str, Any]],
    sniper_signal: str = "NEUTRO"
) -> Dict[str, Any]:

    if len(candles) < 60:

        return {
            "signal": "NEUTRO",
            "confidence": 0.0,
            "confirmed": False,
            "reason": (
                "Poucas velas para "
                "análise multi-estratégia."
            ),
            "strategies": {},
            "non_repaint_reference":
                True,
        }

    closed = candles[:-1]

    closes = [
        float(c["close"])
        for c in closed
    ]

    highs = [
        float(c["high"])
        for c in closed
    ]

    lows = [
        float(c["low"])
        for c in closed
    ]

    last = closed[-1]

    e9 = ema(
        closes,
        9
    )

    e21 = ema(
        closes,
        21
    )

    e50 = ema(
        closes,
        50
    )

    r7 = rsi(
        closes,
        7
    )

    r14 = rsi(
        closes,
        14
    )

    trs = true_ranges(
        closed
    )

    atr14 = (
        sum(trs[-14:])
        / 14.0
    )

    # ========================================================
    # BOLLINGER
    # ========================================================

    n = min(
        20,
        len(closes)
    )

    mean20 = (
        sum(closes[-n:])
        / n
    )

    var20 = (
        sum(
            (
                x - mean20
            ) ** 2
            for x in closes[-n:]
        )
        / n
    )

    sd20 = var20 ** 0.5

    upper = (
        mean20
        + 2.0 * sd20
    )

    lower = (
        mean20
        - 2.0 * sd20
    )

    body = abs(
        last["close"]
        - last["open"]
    )

    rng = max(
        last["high"]
        - last["low"],
        1e-12
    )

    upper_wick = (
        last["high"]
        - max(
            last["open"],
            last["close"]
        )
    )

    lower_wick = (
        min(
            last["open"],
            last["close"]
        )
        - last["low"]
    )

    scores = {
        "CALL": 0.0,
        "PUT": 0.0
    }

    reasons = []

    def add(
        direction,
        weight,
        text
    ):

        scores[direction] += weight
        reasons.append(text)

    # ========================================================
    # 1 - TENDÊNCIA
    # ========================================================

    if (
        e9[-1]
        > e21[-1]
        > e50[-1]
        and closes[-1]
        > e9[-1]
    ):

        add(
            "CALL",
            22,
            "tendência de alta pelas EMA 9/21/50"
        )

    elif (
        e9[-1]
        < e21[-1]
        < e50[-1]
        and closes[-1]
        < e9[-1]
    ):

        add(
            "PUT",
            22,
            "tendência de baixa pelas EMA 9/21/50"
        )

    # ========================================================
    # 2 - RSI
    # ========================================================

    if (
        r7[-1] >= 55
        and r14[-1] >= 52
        and r7[-1] < 78
    ):

        add(
            "CALL",
            14,
            "momentum comprador confirmado pelo RSI"
        )

    elif (
        r7[-1] <= 45
        and r14[-1] <= 48
        and r7[-1] > 22
    ):

        add(
            "PUT",
            14,
            "momentum vendedor confirmado pelo RSI"
        )

    # ========================================================
    # 3 - PRICE ACTION
    # ========================================================

    if (
        last["close"]
        > last["open"]
        and body / rng >= 0.55
    ):

        add(
            "CALL",
            12,
            "vela de força compradora"
        )

    elif (
        last["close"]
        < last["open"]
        and body / rng >= 0.55
    ):

        add(
            "PUT",
            12,
            "vela de força vendedora"
        )

    # ========================================================
    # 4 - REJEIÇÃO
    # ========================================================

    if (
        lower_wick / rng >= 0.45
        and last["close"]
        > last["open"]
    ):

        add(
            "CALL",
            10,
            "rejeição de preços baixos"
        )

    elif (
        upper_wick / rng >= 0.45
        and last["close"]
        < last["open"]
    ):

        add(
            "PUT",
            10,
            "rejeição de preços altos"
        )

    # ========================================================
    # 5 - ESTRUTURA
    # ========================================================

    lookback = min(
        12,
        len(closed) - 2
    )

    recent_high = max(
        highs[
            -lookback - 1:-1
        ]
    )

    recent_low = min(
        lows[
            -lookback - 1:-1
        ]
    )

    if last["close"] > recent_high:

        add(
            "CALL",
            14,
            "rompimento da máxima recente"
        )

    elif last["close"] < recent_low:

        add(
            "PUT",
            14,
            "rompimento da mínima recente"
        )

    # ========================================================
    # 6 - BOLLINGER
    # ========================================================

    if (
        closes[-1] > mean20
        and closes[-1] < upper
    ):

        add(
            "CALL",
            7,
            "preço acima da média das Bollinger"
        )

    elif (
        closes[-1] < mean20
        and closes[-1] > lower
    ):

        add(
            "PUT",
            7,
            "preço abaixo da média das Bollinger"
        )

    # ========================================================
    # 7 - VOLATILIDADE
    # ========================================================

    avg_range = (
        sum(
            c["high"]
            - c["low"]
            for c in closed[-20:]
        )
        / 20.0
    )

    if (
        atr14 > 0
        and rng >= avg_range * 0.75
    ):

        if last["close"] > last["open"]:

            add(
                "CALL",
                5,
                "volatilidade compatível com movimento comprador"
            )

        elif last["close"] < last["open"]:

            add(
                "PUT",
                5,
                "volatilidade compatível com movimento vendedor"
            )

    # ========================================================
    # 8 - SNIPER COMO GATILHO
    # ========================================================

    if sniper_signal == "CALL":

        scores["CALL"] += 12

        reasons.append(
            "Sniper confirmou CALL"
        )

    elif sniper_signal == "PUT":

        scores["PUT"] += 12

        reasons.append(
            "Sniper confirmou PUT"
        )

    # ========================================================
    # DECISÃO
    # ========================================================

    if scores["CALL"] > scores["PUT"]:

        best = "CALL"

    elif scores["PUT"] > scores["CALL"]:

        best = "PUT"

    else:

        best = "NEUTRO"

    total = (
        scores["CALL"]
        + scores["PUT"]
    )

    if (
        best == "NEUTRO"
        or total <= 0
    ):

        confidence = 50.0

    else:

        dominance = (
            max(
                scores["CALL"],
                scores["PUT"]
            )
            / total
        )

        confidence = (
            50.0
            + dominance * 45.0
        )

    # ========================================================
    # CONFIRMAÇÃO FINAL
    # ========================================================

    confirmed = (
        sniper_signal
        in ("CALL", "PUT")
        and best == sniper_signal
        and confidence
        >= AI_MIN_CONFIDENCE
    )

    if not confirmed:

        final_signal = "NEUTRO"

        reason = (
            "IA analisando: "
            "o Sniper e as estratégias "
            "internas ainda não formaram "
            "confirmação suficiente."
        )

    else:

        final_signal = best

        reason = (
            f"IA confirmou {best}. "
            f"Score CALL="
            f"{scores['CALL']:.1f}; "
            f"Score PUT="
            f"{scores['PUT']:.1f}."
        )

    return {
        "signal":
            final_signal,

        "confidence":
            round(
                _clamp(
                    confidence,
                    50.0,
                    97.0
                ),
                1
            ),

        "confirmed":
            confirmed,

        "reason":
            reason,

        "strategies": {

            "tendencia":
                (
                    "ALTA"
                    if e9[-1]
                    > e21[-1]
                    > e50[-1]
                    else "BAIXA"
                    if e9[-1]
                    < e21[-1]
                    < e50[-1]
                    else "LATERAL"
                ),

            "momentum":
                (
                    "CALL"
                    if (
                        r7[-1] > 55
                        and r14[-1] > 52
                    )
                    else "PUT"
                    if (
                        r7[-1] < 45
                        and r14[-1] < 48
                    )
                    else "NEUTRO"
                ),

            "price_action":
                (
                    "CALL"
                    if (
                        last["close"]
                        > last["open"]
                        and body / rng
                        >= 0.55
                    )
                    else "PUT"
                    if (
                        last["close"]
                        < last["open"]
                        and body / rng
                        >= 0.55
                    )
                    else "NEUTRO"
                ),

            "estrutura":
                (
                    "CALL"
                    if last["close"]
                    > recent_high
                    else "PUT"
                    if last["close"]
                    < recent_low
                    else "NEUTRO"
                ),

            "volatilidade":
                (
                    "OK"
                    if (
                        atr14 > 0
                        and rng
                        >= avg_range * 0.75
                    )
                    else "FRACA"
                ),

            "bollinger":
                (
                    "ACIMA_MEDIA"
                    if closes[-1] > mean20
                    else "ABAIXO_MEDIA"
                ),
        },

        "score_call":
            round(
                scores["CALL"],
                1
            ),

        "score_put":
            round(
                scores["PUT"],
                1
            ),

        "non_repaint_reference":
            True,
    }


# ============================================================
# IA FINAL
# ============================================================

async def build_ai_signal(
    symbol: str,
    interval: str
):

    values = await get_candles(
        symbol,
        interval,
        240
    )

    # --------------------------------------------------------
    # PRIMEIRO: RSI + SNIPERS
    # --------------------------------------------------------

    sniper_candidates = []

    for (
        strategy_code,
        internal_name
    ) in STRATEGIES:

        try:

            result = analyze(
                values,
                strategy_code
            )

            if result.get(
                "signal"
            ) in (
                "CALL",
                "PUT"
            ):

                sniper_candidates.append({

                    "code":
                        strategy_code,

                    "name":
                        internal_name,

                    "signal":
                        result["signal"],

                    "confidence":
                        float(
                            result.get(
                                "confidence",
                                50
                            )
                        ),
                })

        except Exception:

            continue

    # --------------------------------------------------------
    # ESCOLHE O MELHOR SNIPER
    # --------------------------------------------------------

    trigger = max(
        sniper_candidates,
        key=lambda x: x["confidence"],
        default=None
    )

    sniper_signal = (
        trigger["signal"]
        if trigger
        else "NEUTRO"
    )

    # --------------------------------------------------------
    # IA TRABALHA JUNTO COM O RSI
    # --------------------------------------------------------

    ai = ai_multi_strategy(
        values,
        sniper_signal
    )

    # --------------------------------------------------------
    # HORÁRIO DA PRÓXIMA VELA
    # --------------------------------------------------------

    minutes = ALLOWED_INTERVALS[
        interval
    ]

    current = now_sp()

    block = minutes * 60

    now_epoch = current.timestamp()

    next_epoch = (
        (
            int(now_epoch)
            // block
        )
        + 1
    ) * block

    entry_dt = datetime.fromtimestamp(
        next_epoch,
        tz=SP_TZ
    )

    expiry_dt = (
        entry_dt
        + timedelta(
            minutes=minutes
        )
    )

    remaining = max(
        0.0,
        next_epoch
        - now_epoch
    )

    in_entry_window = (
        0.0
        < remaining
        <= ENTRY_WINDOW_SECONDS
    )

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

    if ai["confirmed"]:

        if in_entry_window:

            entry_status = "ENTRAR"

            entry_message = (
                f"MOMENTO DE ENTRADA — "
                f"{ai['signal']}"
            )

        else:

            entry_status = "AGUARDE"

            entry_message = (
                f"IA CONFIRMOU "
                f"{ai['signal']}. "
                f"Aguarde a janela "
                f"de entrada às "
                f"{entry_dt.strftime('%H:%M:%S')}."
            )

    elif trigger:

        entry_status = "ANALISANDO"

        entry_message = (
            f"IA ANALISANDO — "
            f"{trigger['name']} deu "
            f"{trigger['signal']}, "
            f"mas a IA ainda não confirmou."
        )

    else:

        entry_status = "ANALISANDO"

        entry_message = (
            "IA ANALISANDO O GRÁFICO — "
            "aguardando um Sniper gerar sinal."
        )

    confidence = float(
        ai.get(
            "confidence",
            0
        )
    )

    if confidence >= 85:

        quality = "MUITO_ALTA"

    elif confidence >= 78:

        quality = "ALTA"

    elif confidence >= 68:

        quality = "MEDIA"

    else:

        quality = "BAIXA"

    return {

        "ok":
            True,

        "source":
            "Twelve Data",

        "engine":
            "ISMAEL TRADE AI + RSI + SNIPERS",

        "version":
            APP_VERSION,

        "symbol":
            symbol,

        "interval":
            interval,

        "reference_candle":
            values[-2]["datetime"],

        "signal":
            ai["signal"],

        "confidence":
            confidence,

        "quality":
            quality,

        "confirmed":
            ai["confirmed"],

        "reason":
            ai["reason"],

        "strategies":
            ai["strategies"],

        "score_call":
            ai["score_call"],

        "score_put":
            ai["score_put"],

        "sniper_trigger":
            trigger,

        "sniper_signal":
            sniper_signal,

        "entry_status":
            entry_status,

        "entry_message":
            entry_message,

        "entry_window_seconds":
            ENTRY_WINDOW_SECONDS,

        "in_entry_window":
            in_entry_window,

        "seconds_to_entry":
            int(remaining),

        "now_epoch":
            int(now_epoch),

        "now_epoch_ms":
            int(now_epoch * 1000),

        "now_sp":
            current.strftime(
                "%d/%m/%Y %H:%M:%S"
            ),

        "entry_epoch":
            int(next_epoch),

        "entry_epoch_ms":
            int(next_epoch * 1000),

        "entry_time":
            entry_dt.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),

        "next_candle":
            entry_dt.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),

        "expiry_time":
            expiry_dt.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),

        "expiry":
            "1 vela do intervalo selecionado",

        "non_repaint_reference":
            True,

        "warning":
            (
                "Análise probabilística. "
                "Não existe garantia de WIN. "
                "A cotação da corretora pode "
                "diferir da Twelve Data."
            ),
    }


# ============================================================
# ENDPOINT PRINCIPAL DA IA
# ============================================================

@app.get("/signal-ai")
async def signal_ai(
    symbol="EUR/USD",
    interval="1min"
):

    require_active_license()

    if symbol not in SYMBOLS:

        raise HTTPException(
            status_code=400,
            detail="Ativo inválido."
        )

    if interval not in ALLOWED_INTERVALS:

        raise HTTPException(
            status_code=400,
            detail="Timeframe inválido."
        )

    return await build_ai_signal(
        symbol,
        interval
    )


# ============================================================
# ENDPOINT SOMENTE IA
# ============================================================

@app.get("/ai-analysis")
async def ai_analysis_endpoint(
    symbol="EUR/USD",
    interval="1min"
):

    require_active_license()

    if symbol not in SYMBOLS:

        raise HTTPException(
            status_code=400,
            detail="Ativo inválido."
        )

    if interval not in ALLOWED_INTERVALS:

        raise HTTPException(
            status_code=400,
            detail="Timeframe inválido."
        )

    values = await get_candles(
        symbol,
        interval,
        240
    )

    analysis = ai_analyze(
        values
    )

    return {
        "ok": True,
        "engine":
            "Trade Sniper AI Hybrid",
        "symbol":
            symbol,
        "interval":
            interval,
        "analysis":
            analysis,
        "updated_at":
            now_sp().strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        "warning":
            (
                "A confiança é uma pontuação "
                "do modelo e não garante o "
                "próximo resultado."
            ),
    }


# ============================================================
# ENDPOINT RSI / SNIPER
# ============================================================

@app.get("/signal")
async def signal(
    symbol="EUR/USD",
    interval="1min",
    strategy="rsi"
):

    require_active_license()

    if strategy not in dict(
        STRATEGIES
    ):

        raise HTTPException(
            status_code=400,
            detail="Estratégia inválida."
        )

    values = await get_candles(
        symbol,
        interval,
        100
    )

    result = analyze(
        values,
        strategy
    )

    minutes = ALLOWED_INTERVALS[
        interval
    ]

    current = now_sp()

    block = minutes * 60

    next_epoch = (
        (
            int(current.timestamp())
            // block
        )
        + 1
    ) * block

    entry_dt = datetime.fromtimestamp(
        next_epoch,
        tz=SP_TZ
    )

    expiry_dt = (
        entry_dt
        + timedelta(
            minutes=minutes
        )
    )

    result.update({

        "source":
            "Twelve Data",

        "symbol":
            symbol,

        "interval":
            interval,

        "entry_time":
            entry_dt.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),

        "next_candle":
            entry_dt.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),

        "expiry_time":
            expiry_dt.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),

        "expiry":
            "1 vela do intervalo selecionado",

        "warning":
            "Sinal probabilístico; não garante WIN.",
    })

    return result


# ============================================================
# VELAS
# ============================================================

@app.get("/candles")
async def candles(
    symbol="EUR/USD",
    interval="1min",
    size=120
):

    require_active_license()

    values = await get_candles(
        symbol,
        interval,
        int(size)
    )

    return {
        "ok": True,
        "source":
            "Twelve Data",
        "symbol":
            symbol,
        "interval":
            interval,
        "values":
            values,
    }


# ============================================================
# RADAR
# ============================================================

def radar_score(
    analysis,
    strategy
):

    if strategy == "rsi":

        r9 = float(
            analysis.get(
                "rsi9",
                50
            )
        )

        r14 = float(
            analysis.get(
                "rsi14",
                50
            )
        )

        bull = max(
            0,
            min(
                100,
                50
                + (
                    r9
                    - 50
                    + r14
                    - 50
                ) * 1.5
            )
        )

        bear = max(
            0,
            min(
                100,
                50
                + (
                    50
                    - r9
                    + 50
                    - r14
                ) * 1.5
            )
        )

    elif strategy == "old_sniper":

        bull = min(
            100,
            50
            + float(
                analysis.get(
                    "bull_score",
                    0
                )
            ) * 10
        )

        bear = min(
            100,
            50
            + float(
                analysis.get(
                    "bear_score",
                    0
                )
            ) * 10
        )

    elif strategy == "sniper_02":

        bull = (
            90
            if analysis.get(
                "call_setup"
            )
            else 50
        )

        bear = (
            90
            if analysis.get(
                "put_setup"
            )
            else 50
        )

    else:

        bull = min(
            99,
            50
            + float(
                analysis.get(
                    "call_score",
                    0
                )
            ) * 5
        )

        bear = min(
            99,
            50
            + float(
                analysis.get(
                    "put_score",
                    0
                )
            ) * 5
        )

    if (
        bull >= bear
        and bull >= 58
    ):

        direction = "CALL"
        proximity = round(bull)

    elif (
        bear > bull
        and bear >= 58
    ):

        direction = "PUT"
        proximity = round(bear)

    else:

        direction = "NEUTRO"
        proximity = round(
            max(
                bull,
                bear
            )
        )

    return {
        "direction":
            direction,
        "proximity":
            max(
                0,
                min(
                    99,
                    proximity
                )
            ),
        "call_proximity":
            round(bull),
        "put_proximity":
            round(bear),
    }


@app.get("/radar")
async def radar(
    interval="1min",
    strategy="rsi"
):

    require_active_license()

    if (
        interval
        not in ALLOWED_INTERVALS
        or strategy
        not in dict(STRATEGIES)
    ):

        raise HTTPException(
            status_code=400,
            detail=(
                "Timeframe ou estratégia inválida."
            )
        )

    cache_key = (
        interval,
        strategy
    )

    cached = RADAR_CACHE.get(
        cache_key
    )

    if (
        cached
        and time.monotonic()
        - cached[0]
        < RADAR_TTL_SECONDS
    ):

        return cached[1]

    results = []
    errors = 0

    radar_symbols = [
        "EUR/USD",
        "GBP/USD",
        "USD/JPY",
        "AUD/USD",
        "USD/CAD",
        "USD/CHF"
    ]

    for symbol_name in radar_symbols:

        try:

            values = await get_candles(
                symbol_name,
                interval,
                100
            )

            a = analyze(
                values,
                strategy
            )

            p = radar_score(
                a,
                strategy
            )

            results.append({

                "symbol":
                    symbol_name,

                "signal":
                    a["signal"],

                "direction":
                    p["direction"],

                "proximity":
                    p["proximity"],

                "call_proximity":
                    p["call_proximity"],

                "put_proximity":
                    p["put_proximity"],

                "confidence":
                    int(
                        a.get(
                            "confidence",
                            50
                        )
                    ),

                "reference_candle":
                    a.get(
                        "reference_candle"
                    ),
            })

        except Exception:

            errors += 1

    results.sort(
        key=lambda x: (
            x["direction"]
            == "NEUTRO",
            -x["proximity"]
        )
    )

    payload = {

        "ok":
            True,

        "interval":
            interval,

        "strategy":
            strategy,

        "symbols_checked":
            len(results),

        "errors":
            errors,

        "results":
            results,

        "warning":
            (
                "Radar probabilístico. "
                "Proximidade não é garantia "
                "de sinal ou WIN."
            ),
    }

    RADAR_CACHE[
        cache_key
    ] = (
        time.monotonic(),
        payload
    )

    return payload


# ============================================================
# HORÁRIO DO SERVIDOR
# ============================================================

@app.get("/server-time")
async def server_time():

    current = now_sp()

    return {

        "brasilia":
            current.isoformat(),

        "utc":
            datetime.now(
                timezone.utc
            ).isoformat(),
    }


# ============================================================
# LICENÇA
# ============================================================

@app.get("/license")
async def license():

    return {
        "ok": True,
        **license_status()
    }


# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
async def health():

    return {

        "ok": True,

        "app":
            APP_NAME,

        "version":
            APP_VERSION,

        "engine":
            "ISMAEL TRADE AI + RSI + SNIPERS",

        "status":
            "online",
    }


# ============================================================
# PÁGINA INICIAL
# ============================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
async def home():

    html = """
<!DOCTYPE html>
<html lang="pt-BR">

<head>

<meta charset="UTF-8">

<meta name="viewport"
      content="width=device-width, initial-scale=1.0">

<title>Ismael Trade AI</title>

<style>

body {
    background: #07111f;
    color: white;
    font-family: Arial, sans-serif;
    margin: 0;
    padding: 20px;
}

.card {
    max-width: 700px;
    margin: auto;
    background: #0d1b2d;
    border-radius: 18px;
    padding: 25px;
    box-shadow: 0 0 30px rgba(0,0,0,.4);
}

h1 {
    text-align: center;
}

.status {
    text-align: center;
    margin: 15px 0;
    padding: 12px;
    border-radius: 10px;
    background: #10253d;
}

.signal {
    text-align: center;
    font-size: 38px;
    font-weight: bold;
    margin: 25px 0;
}

.info {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 10px;
}

.box {
    background: #10253d;
    padding: 15px;
    border-radius: 10px;
}

button {
    width: 100%;
    padding: 15px;
    border: 0;
    border-radius: 10px;
    margin-top: 20px;
    font-weight: bold;
    cursor: pointer;
}

</style>

</head>

<body>

<div class="card">

<h1>🧠 ISMAEL TRADE AI</h1>

<div class="status">
ANÁLISE EM TEMPO REAL
</div>

<div id="clock" class="status">
HORÁRIO DE BRASÍLIA
</div>

<div id="signal"
     class="signal">
AGUARDANDO
</div>

<div class="info">

<div class="box">
<b>ATIVO</b>
<div id="symbol">
EUR/USD
</div>
</div>

<div class="box">
<b>TIMEFRAME</b>
<div id="interval">
M1
</div>
</div>

<div class="box">
<b>CONFIANÇA IA</b>
<div id="confidence">
--
</div>
</div>

<div class="box">
<b>QUALIDADE</b>
<div id="quality">
--
</div>
</div>

<div class="box">
<b>SNIPER</b>
<div id="sniper">
--
</div>
</div>

<div class="box">
<b>STATUS</b>
<div id="entry_status">
--
</div>
</div>

<div class="box">
<b>ENTRADA</b>
<div id="entry">
--
</div>
</div>

<div class="box">
<b>EXPIRAÇÃO</b>
<div id="expiry">
--
</div>
</div>

</div>

<button onclick="updateSignal()">
ATUALIZAR SINAL
</button>

<div class="status"
     id="reason">
IA analisando...
</div>

<div class="status">

Renovação:
<br>

WhatsApp:
55 84 99841-1282
<br>
55 84 99449-9442

<br><br>

Instagram:
@Ismaelartur26

</div>

</div>


<script>

async function updateSignal() {

    try {

        const response =
            await fetch(
                "/signal-ai?symbol=EUR/USD&interval=1min"
            );

        const data =
            await response.json();

        document.getElementById(
            "clock"
        ).innerText =
            data.now_sp || "--";

        document.getElementById(
            "signal"
        ).innerText =
            data.signal || "NEUTRO";

        document.getElementById(
            "confidence"
        ).innerText =
            (data.confidence || 0) + "%";

        document.getElementById(
            "quality"
        ).innerText =
            data.quality || "--";

        document.getElementById(
            "sniper"
        ).innerText =
            data.sniper_signal || "NEUTRO";

        document.getElementById(
            "entry_status"
        ).innerText =
            data.entry_status || "--";

        document.getElementById(
            "entry"
        ).innerText =
            data.entry_time || "--";

        document.getElementById(
            "expiry"
        ).innerText =
            data.expiry_time || "--";

        document.getElementById(
            "reason"
        ).innerText =
            data.entry_message ||
            data.reason ||
            "IA analisando...";

    } catch (error) {

        document.getElementById(
            "reason"
        ).innerText =
            "Erro ao consultar a API.";

    }
}


updateSignal();

setInterval(
    updateSignal,
    5000
);

</script>

</body>

</html>
"""

    return HTMLResponse(
        html
    )


# ============================================================
# EXECUÇÃO
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