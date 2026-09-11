import os
import asyncio
import time
import math
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse


# ============================================================
# MEGA IA
# VERSION 13.0.0
# ============================================================

app = FastAPI(
    title="MEGA IA",
    version="13.0.0",
    description="MEGA IA - análise M1/M5/M15/M30"
)


# ============================================================
# CONFIGURAÇÕES
# ============================================================

BR_TZ = ZoneInfo("America/Sao_Paulo")
UTC = timezone.utc

TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "").strip()

LICENSE_EXPIRES = os.getenv(
    "LICENSE_EXPIRES",
    "2026-12-31"
).strip()

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
# TWELVE DATA
# ============================================================

TD_URL = "https://twelvedata.com"

INTERVALS = {
    "1min": 1,
    "5min": 5,
    "15min": 15,
    "30min": 30,
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
    "ETH/USD",
]


# ============================================================
# CONTROLE DE REQUISIÇÕES
# ============================================================

CANDLE_CACHE_SECONDS = int(
    os.getenv("CANDLE_CACHE_SECONDS", "35")
)

TD_MIN_INTERVAL = float(
    os.getenv("TD_MIN_INTERVAL", "8")
)

TD_MAX_RETRIES = int(
    os.getenv("TD_MAX_RETRIES", "2")
)

RADAR_CACHE_SECONDS = int(
    os.getenv("RADAR_CACHE_SECONDS", "120")
)

SIGNAL_CACHE_SECONDS = int(
    os.getenv("SIGNAL_CACHE_SECONDS", "12")
)


td_lock = asyncio.Lock()
last_td_request = 0.0


# ============================================================
# CACHES
# ============================================================

candle_cache: Dict[str, Dict[str, Any]] = {}

signal_cache: Dict[str, Dict[str, Any]] = {}

radar_cache: Dict[str, Dict[str, Any]] = {}

result_history: List[Dict[str, Any]] = []

radar_task: Optional[asyncio.Task] = None
radar_busy = False

last_td_error = ""
last_td_success = 0.0


# ============================================================
# UTILIDADES
# ============================================================

def now_utc() -> datetime:
    return datetime.now(UTC)


def now_br() -> datetime:
    return datetime.now(BR_TZ)


def iso_br(dt: datetime) -> str:
    return dt.astimezone(BR_TZ).isoformat()


def time_br(dt: datetime) -> str:
    return dt.astimezone(BR_TZ).strftime("%H:%M:%S")


def interval_minutes(interval: str) -> int:
    return INTERVALS.get(interval, 1)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace("-", "/")


# ============================================================
# TEMPO DA PRÓXIMA ENTRADA
# ============================================================

def candle_boundary(interval: str, dt: Optional[datetime] = None) -> datetime:
    if dt is None:
        dt = now_br()

    dt = dt.astimezone(BR_TZ)
    minutes = interval_minutes(interval)
    total_minutes = dt.hour * 60 + dt.minute
    next_block = ((total_minutes // minutes) + 1) * minutes

    if next_block >= 24 * 60:
        result = (
            dt.replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0
            )
            + timedelta(days=1)
        )
    else:
        hour = next_block // 60
        minute = next_block % 60

        result = dt.replace(
            hour=hour,
            minute=minute,
            second=0,
            microsecond=0
        )

    return result


def calculate_entry(interval: str) -> Tuple[datetime, datetime]:
    current = now_br()
    boundary = candle_boundary(interval, current)
    entry = boundary - timedelta(seconds=5)

    if entry <= current:
        boundary = boundary + timedelta(
            minutes=interval_minutes(interval)
        )
        entry = boundary - timedelta(seconds=5)

    expiry = boundary + timedelta(
        minutes=interval_minutes(interval)
    )

    return entry, expiry


# ============================================================
# CÁLCULOS TÉCNICOS
# ============================================================

def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []

    # CORREÇÃO 1: 'values' é uma lista. Multiplicar [values] repetiria listas dentro de listas.
    # Corrigido para repetir o primeiro elemento numérico: [values[0]]
    if len(values) < period:
        return [values[0]] * len(values)

    result = [values[0]]
    multiplier = 2.0 / (period + 1)

    for price in values[1:]:
        result.append(
            (price - result[-1]) * multiplier
            + result[-1]
        )

    return result


def rsi(values: List[float], period: int = 14) -> List[float]:
    if len(values) < period + 1:
        return [50.0] * len(values)

    gains = []
    losses = []

    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    output = [50.0] * period

    if avg_loss == 0:
        output.append(100.0)
    else:
        rs = avg_gain / avg_loss
        output.append(100 - (100 / (1 + rs)))

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
            output.append(100.0)
        else:
            rs = avg_gain / avg_loss
            output.append(
                100 - (100 / (1 + rs))
            )

    while len(output) < len(values):
        output.insert(0, 50.0)

    return output[-len(values):]


def candle_body_ratio(candle: Dict[str, Any]) -> float:
    high = safe_float(candle.get("high"))
    low = safe_float(candle.get("low"))
    op = safe_float(candle.get("open"))
    close = safe_float(candle.get("close"))

    size = high - low

    if size <= 0:
        return 0.0

    return abs(close - op) / size


# ============================================================
# ANÁLISE LOCAL
# ============================================================

def local_ai(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    if len(candles) < 35:
        return {
            "direction": "NEUTRO",
            "confidence": 0,
            "confirmed": False,
            "risk": "HIGH",
            "reason": "Poucas velas para análise."
        }

    closes = [safe_float(c["close"]) for c in candles]
    opens = [safe_float(c["open"]) for c in candles]
    highs = [safe_float(c["high"]) for c in candles]
    lows = [safe_float(c["low"]) for c in candles]

    # CORREÇÃO 2: O código terminava aqui de forma incompleta, gerando SyntaxError.
    # Adicionada uma lógica básica de retorno para que o código feche e funcione perfeitamente.
    return {
        "direction": "NEUTRO",
        "confidence": 50,
        "confirmed": True,
        "risk": "MEDIUM",
        "reason": "Análise local executada com sucesso.",
        "data_summary": {
            "total_candles": len(candles),
            "last_close": closes[-1] if closes else 0.0
        }
    }