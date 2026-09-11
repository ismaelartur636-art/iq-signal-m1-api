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

TD_URL = "https://api.twelvedata.com/time_series"

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

# Cache das velas.
# A ideia é não consultar a Twelve Data a cada atualização da tela.
CANDLE_CACHE_SECONDS = int(
    os.getenv("CANDLE_CACHE_SECONDS", "35")
)

# Intervalo mínimo entre chamadas reais.
# Não reduzir muito em contas com limite baixo.
TD_MIN_INTERVAL = float(
    os.getenv("TD_MIN_INTERVAL", "8")
)

# Quantidade de tentativas.
TD_MAX_RETRIES = int(
    os.getenv("TD_MAX_RETRIES", "2")
)

# Tempo que o radar permanece válido.
RADAR_CACHE_SECONDS = int(
    os.getenv("RADAR_CACHE_SECONDS", "120")
)

# Cache do sinal.
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
    """
    Calcula o início da próxima vela.
    """

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
    """
    A entrada fica 5 segundos antes do fechamento da janela
    seguinte.

    Mantemos a análise baseada em velas fechadas para evitar
    repintura.
    """

    current = now_br()

    boundary = candle_boundary(interval, current)

    entry = boundary - timedelta(seconds=5)

    # Se por algum motivo já passamos da entrada,
    # usamos a próxima janela.
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

    output = [50.0] * (period)

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

    closes = [
        safe_float(c["close"])
        for c in candles
    ]

    opens = [
        safe_float(c["open"])
        for c in candles
    ]

    highs = [
        safe_float(c["high"])
        for c in candles
    ]

    lows = [
        safe_float(c["low"])
        for c in candles
    ]

    ema3 = ema(closes, 3)
    ema7 = ema(closes, 7)
    rsi14 = rsi(closes, 14)

    score_call = 0
    score_put = 0

    reasons = []

    # --------------------------------------------------------
    # EMA
    # --------------------------------------------------------

    if ema3[-1] > ema7[-1]:
        score_call += 2
        reasons.append("EMA3 acima da EMA7")

    elif ema3[-1] < ema7[-1]:
        score_put += 2
        reasons.append("EMA3 abaixo da EMA7")

    # --------------------------------------------------------
    # Inclinação EMA
    # --------------------------------------------------------

    if len(ema3) >= 3:

        if ema3[-1] > ema3[-2] > ema3[-3]:
            score_call += 1
            reasons.append("EMA3 com alta")

        elif ema3[-1] < ema3[-2] < ema3[-3]:
            score_put += 1
            reasons.append("EMA3 com baixa")

    # --------------------------------------------------------
    # RSI
    # --------------------------------------------------------

    current_rsi = rsi14[-1]

    if current_rsi <= 30:
        score_call += 2
        reasons.append("RSI em sobrevenda")

    elif current_rsi >= 70:
        score_put += 2
        reasons.append("RSI em sobrecompra")

    elif current_rsi > 55:
        score_call += 1
        reasons.append("RSI favorece CALL")

    elif current_rsi < 45:
        score_put += 1
        reasons.append("RSI favorece PUT")

    # --------------------------------------------------------
    # Última vela FECHADA
    # --------------------------------------------------------

    last_open = opens[-1]
    last_close = closes[-1]

    if last_close > last_open:
        score_call += 1
        reasons.append("Última vela fechada positiva")

    elif last_close < last_open:
        score_put += 1
        reasons.append("Última vela fechada negativa")

    # --------------------------------------------------------
    # Corpo da vela
    # --------------------------------------------------------

    ratio = candle_body_ratio(candles[-1])

    if ratio >= 0.65:

        if last_close > last_open:
            score_call += 1
            reasons.append("Corpo comprador forte")

        elif last_close < last_open:
            score_put += 1
            reasons.append("Corpo vendedor forte")

    # --------------------------------------------------------
    # Momentum
    # --------------------------------------------------------

    if len(closes) >= 5:

        momentum = closes[-1] - closes[-5]

        if momentum > 0:
            score_call += 1
            reasons.append("Momentum positivo")

        elif momentum < 0:
            score_put += 1
            reasons.append("Momentum negativo")

    total = score_call + score_put

    if total <= 0:
        return {
            "direction": "NEUTRO",
            "confidence": 0,
            "confirmed": False,
            "risk": "HIGH",
            "reason": "Sem confluência suficiente."
        }

    if score_call > score_put:
        direction = "CALL"
        advantage = score_call - score_put
    elif score_put > score_call:
        direction = "PUT"
        advantage = score_put - score_call
    else:
        direction = "NEUTRO"
        advantage = 0

    confidence = 50 + (
        (advantage / total) * 45
    )

    confidence = int(
        clamp(confidence, 0, 95)
    )

    # Para não deixar o sistema praticamente
    # sempre travado em AGUARDANDO.
    confirmed = (
        direction != "NEUTRO"
        and confidence >= 58
    )

    if confidence >= 78:
        risk = "LOW"

    elif confidence >= 65:
        risk = "MEDIUM"

    else:
        risk = "HIGH"

    return {
        "direction": direction,
        "confidence": confidence,
        "confirmed": confirmed,
        "risk": risk,
        "reason": " • ".join(reasons[-6:]),
        "score_call": score_call,
        "score_put": score_put,
        "rsi": round(current_rsi, 2),
        "body_ratio": round(ratio, 3),
        "ema3": ema3[-1],
        "ema7": ema7[-1],
    }


# ============================================================
# OPENAI OPCIONAL
# ============================================================

async def openai_analysis(
    symbol: str,
    interval: str,
    candles: List[Dict[str, Any]],
    local: Dict[str, Any]
) -> Optional[Dict[str, Any]]:

    if not OPENAI_API_KEY:
        return None

    if not OPENAI_MODEL:
        return None

    try:

        compact = []

        for candle in candles[-25:]:
            compact.append({
                "o": round(
                    safe_float(candle.get("open")), 6
                ),
                "h": round(
                    safe_float(candle.get("high")), 6
                ),
                "l": round(
                    safe_float(candle.get("low")), 6
                ),
                "c": round(
                    safe_float(candle.get("close")), 6
                ),
            })

        prompt = f"""
Você é um analisador técnico de curto prazo.

Ativo: {symbol}
Timeframe: {interval}

A análise local encontrou:

Direção: {local.get("direction")}
Confiança: {local.get("confidence")}%
RSI: {local.get("rsi")}
EMA3: {local.get("ema3")}
EMA7: {local.get("ema7")}

Velas fechadas:
{compact}

Analise tendência, momentum, RSI, EMA e comportamento das velas.

Responda SOMENTE JSON neste formato:

{{
  "direction": "CALL" ou "PUT" ou "NEUTRO",
  "confidence": 0,
  "risk": "LOW" ou "MEDIUM" ou "HIGH",
  "confirmed": true ou false,
  "reason": "texto curto"
}}
"""

        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": OPENAI_MODEL,
            "input": prompt,
        }

        timeout = httpx.Timeout(
            connect=5,
            read=12,
            write=5,
            pool=5
        )

        async with httpx.AsyncClient(
            timeout=timeout
        ) as client:

            response = await client.post(
                "https://api.openai.com/v1/responses",
                headers=headers,
                json=payload
            )

        if response.status_code >= 400:
            return None

        data = response.json()

        text = data.get("output_text", "")

        if not text:
            output = data.get("output", [])

            pieces = []

            for item in output:
                for content in item.get(
                    "content", []
                ):
                    if content.get("type") in (
                        "output_text",
                        "text"
                    ):
                        pieces.append(
                            content.get("text", "")
                        )

            text = "".join(pieces)

        if not text:
            return None

        text = text.strip()

        # Remove possíveis cercas Markdown.
        text = text.replace(
            "```json", ""
        ).replace(
            "```", ""
        ).strip()

        import json

        result = json.loads(text)

        direction = str(
            result.get(
                "direction",
                "NEUTRO"
            )
        ).upper()

        confidence = int(
            safe_float(
                result.get("confidence", 0)
            )
        )

        risk = str(
            result.get(
                "risk",
                "HIGH"
            )
        ).upper()

        confirmed = bool(
            result.get(
                "confirmed",
                False
            )
        )

        return {
            "direction": direction,
            "confidence": int(
                clamp(confidence, 0, 100)
            ),
            "risk": risk,
            "confirmed": confirmed,
            "reason": str(
                result.get(
                    "reason",
                    "Análise IA."
                )
            )
        }

    except Exception:
        return None


# ============================================================
# TWELVE DATA
# ============================================================

async def td_request(
    symbol: str,
    interval: str,
    outputsize: int = 80
) -> List[Dict[str, Any]]:

    global last_td_request
    global last_td_error
    global last_td_success

    if not TWELVE_DATA_API_KEY:
        raise RuntimeError(
            "TWELVE_DATA_API_KEY não configurada."
        )

    key = (
        f"{symbol}|{interval}|{outputsize}"
    )

    # --------------------------------------------------------
    # CACHE
    # --------------------------------------------------------

    cached = candle_cache.get(key)

    if cached:

        age = time.time() - cached["timestamp"]

        if age <= CANDLE_CACHE_SECONDS:
            return cached["data"]

    # --------------------------------------------------------
    # LOCK
    # --------------------------------------------------------

    async with td_lock:

        # Outra requisição pode ter atualizado
        # o cache enquanto aguardávamos o lock.

        cached = candle_cache.get(key)

        if cached:

            age = time.time() - cached["timestamp"]

            if age <= CANDLE_CACHE_SECONDS:
                return cached["data"]

        # ----------------------------------------------------
        # RATE LIMIT
        # ----------------------------------------------------

        elapsed = time.monotonic() - last_td_request

        if elapsed < TD_MIN_INTERVAL:
            await asyncio.sleep(
                TD_MIN_INTERVAL - elapsed
            )

        params = {
            "symbol": symbol,
            "interval": interval,
            "outputsize": outputsize,
            "apikey": TWELVE_DATA_API_KEY,
            "format": "JSON",
        }

        last_error = ""

        for attempt in range(TD_MAX_RETRIES):

            try:

                last_td_request = time.monotonic()

                timeout = httpx.Timeout(
                    connect=5,
                    read=12,
                    write=5,
                    pool=5
                )

                async with httpx.AsyncClient(
                    timeout=timeout
                ) as client:

                    response = await client.get(
                        TD_URL,
                        params=params
                    )

                if response.status_code == 429:

                    last_error = (
                        "Twelve Data atingiu o limite "
                        "de requisições."
                    )

                    # NÃO esperar 30 segundos.
                    # Retornaremos cache se existir.
                    break

                if response.status_code >= 400:

                    last_error = (
                        f"Twelve Data HTTP "
                        f"{response.status_code}"
                    )

                    break

                data = response.json()

                if "status" in data:
                    status = str(
                        data.get("status", "")
                    ).lower()

                    if status == "error":
                        last_error = str(
                            data.get(
                                "message",
                                "Erro da Twelve Data."
                            )
                        )
                        break

                values = data.get("values")

                if not values:
                    last_error = (
                        "Twelve Data não retornou velas."
                    )
                    break

                parsed = []

                # Twelve Data normalmente retorna
                # da vela mais recente para a antiga.
                for item in reversed(values):

                    parsed.append({
                        "datetime": item.get(
                            "datetime"
                        ),
                        "open": safe_float(
                            item.get("open")
                        ),
                        "high": safe_float(
                            item.get("high")
                        ),
                        "low": safe_float(
                            item.get("low")
                        ),
                        "close": safe_float(
                            item.get("close")
                        ),
                        "volume": safe_float(
                            item.get("volume")
                        ),
                    })

                if len(parsed) < 2:
                    last_error = (
                        "Dados insuficientes."
                    )
                    break

                candle_cache[key] = {
                    "timestamp": time.time(),
                    "data": parsed
                }

                last_td_success = time.time()
                last_td_error = ""

                return parsed

            except Exception as exc:

                last_error = str(exc)

                if attempt + 1 < TD_MAX_RETRIES:
                    await asyncio.sleep(1)

        # ----------------------------------------------------
        # FALLBACK PARA CACHE ANTIGO
        # ----------------------------------------------------

        cached = candle_cache.get(key)

        if cached:

            last_td_error = last_error

            return cached["data"]

        last_td_error = last_error

        raise RuntimeError(
            last_error or
            "Twelve Data indisponível."
        )


# ============================================================
# OBTÉM SOMENTE VELAS FECHADAS
# ============================================================

def closed_candles(
    candles: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:

    if len(candles) <= 2:
        return candles

    # A última vela recebida pode ser a vela atual.
    # Removemos para impedir repaint.
    return candles[:-1]


# ============================================================
# SINAL
# ============================================================

async def build_signal(
    symbol: str,
    interval: str,
    use_openai: bool = True
) -> Dict[str, Any]:

    symbol = normalize_symbol(symbol)

    if symbol not in SYMBOLS:
        raise HTTPException(
            status_code=400,
            detail="Ativo não suportado."
        )

    if interval not in INTERVALS:
        raise HTTPException(
            status_code=400,
            detail="Timeframe não suportado."
        )

    cache_key = (
        f"{symbol}|{interval}|{use_openai}"
    )

    # --------------------------------------------------------
    # CACHE DO SINAL
    # --------------------------------------------------------

    cached = signal_cache.get(cache_key)

    if cached:

        age = time.time() - cached["timestamp"]

        if age <= SIGNAL_CACHE_SECONDS:
            return cached["data"]

    # --------------------------------------------------------
    # DADOS
    # --------------------------------------------------------

    try:

        raw = await td_request(
            symbol,
            interval,
            80
        )

        candles = closed_candles(raw)

    except Exception as exc:

        data = {
            "ok": False,
            "symbol": symbol,
            "interval": interval,
            "direction": "NEUTRO",
            "confidence": 0,
            "confirmed": False,
            "risk": "HIGH",
            "status": "ERRO NOS DADOS",
            "entry_time": None,
            "expiry_time": None,
            "entry": "--:--:--",
            "expiry": "--:--:--",
            "reason": str(exc),
            "source": "Twelve Data",
            "repaint": False,
            "timestamp": iso_br(now_br()),
        }

        return data

    # --------------------------------------------------------
    # IA LOCAL
    # --------------------------------------------------------

    local = local_ai(candles)

    direction = local["direction"]
    confidence = local["confidence"]
    confirmed = local["confirmed"]
    risk = local["risk"]
    reason = local["reason"]

    source = "IA LOCAL"

    # --------------------------------------------------------
    # IA EXTERNA OPCIONAL
    # --------------------------------------------------------

    external = None

    if (
        use_openai
        and direction != "NEUTRO"
        and confirmed
    ):
        external = await openai_analysis(
            symbol,
            interval,
            candles,
            local
        )

    if external:

        # A IA externa é confirmação.
        # Ela NÃO derruba automaticamente o sinal local.

        if (
            external["direction"] == direction
            and external["confirmed"]
        ):

            confidence = int(
                round(
                    (
                        confidence
                        + external["confidence"]
                    ) / 2
                )
            )

            risk = external["risk"]

            reason = (
                f"{reason} • "
                f"Confirmação IA externa: "
                f"{external['reason']}"
            )

            source = "IA LOCAL + IA EXTERNA"

            confirmed = confidence >= 58

        elif external["direction"] == "NEUTRO":

            reason = (
                f"{reason} • "
                "IA externa neutra"
            )

            source = "IA LOCAL + IA EXTERNA"

        else:

            # Não apaga o sinal local.
            reason = (
                f"{reason} • "
                "IA externa divergiu; "
                "sinal local mantido"
            )

            source = "IA LOCAL"

            risk = "HIGH"

    # --------------------------------------------------------
    # ENTRADA
    # --------------------------------------------------------

    entry_dt, expiry_dt = calculate_entry(
        interval
    )

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

    if direction == "NEUTRO":

        status = "AGUARDANDO"

    elif confirmed:

        status = "SINAL LIBERADO"

    else:

        status = "MONITORANDO"

    data = {
        "ok": True,
        "symbol": symbol,
        "interval": interval,
        "direction": direction,
        "confidence": confidence,
        "confirmed": confirmed,
        "risk": risk,
        "status": status,
        "entry_time": iso_br(entry_dt),
        "expiry_time": iso_br(expiry_dt),
        "entry": time_br(entry_dt),
        "expiry": time_br(expiry_dt),
        "reason": reason,
        "source": source,
        "repaint": False,
        "candles_used": len(candles),
        "local": local,
        "external": external,
        "timestamp": iso_br(now_br()),
    }

    signal_cache[cache_key] = {
        "timestamp": time.time(),
        "data": data
    }

    return data


# ============================================================
# RADAR
# ============================================================

async def radar_worker(interval: str):

    global radar_busy

    if radar_busy:
        return

    radar_busy = True

    try:

        results = []

        # IMPORTANTE:
        # radar NÃO chama OpenAI.
        # Isso reduz muito o tempo e o consumo.

        for symbol in SYMBOLS:

            try:

                result = await build_signal(
                    symbol,
                    interval,
                    use_openai=False
                )

                results.append(result)

            except Exception as exc:

                results.append({
                    "ok": False,
                    "symbol": symbol,
                    "interval": interval,
                    "direction": "NEUTRO",
                    "confidence": 0,
                    "status": "ERRO",
                    "risk": "HIGH",
                    "reason": str(exc)
                })

        radar_cache[interval] = {
            "timestamp": time.time(),
            "data": results
        }

    finally:

        radar_busy = False


# ============================================================
# ENDPOINT RAIZ
# ============================================================

@app.get("/", response_class=HTMLResponse)
async def home():

    return HTMLResponse(
        HTML_PAGE
    )


# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
async def health():

    return {
        "ok": True,
        "app": "MEGA IA",
        "version": "13.0.0",
        "timezone": "America/Sao_Paulo",
        "time_brasilia": now_br().isoformat(),
        "twelve_data_configured": bool(
            TWELVE_DATA_API_KEY
        ),
        "openai_configured": bool(
            OPENAI_API_KEY
        ),
        "openai_model_configured": bool(
            OPENAI_MODEL
        ),
        "cache_seconds": CANDLE_CACHE_SECONDS,
        "signal_cache_seconds": SIGNAL_CACHE_SECONDS,
        "radar_cache_seconds": RADAR_CACHE_SECONDS,
        "last_twelve_data_error": last_td_error,
        "last_twelve_data_success": (
            datetime.fromtimestamp(
                last_td_success,
                UTC
            ).astimezone(BR_TZ).isoformat()
            if last_td_success
            else None
        ),
    }


# ============================================================
# SERVER TIME
# ============================================================

@app.get("/server-time")
async def server_time():

    current = now_br()

    return {
        "timezone": "America/Sao_Paulo",
        "datetime": current.isoformat(),
        "date": current.strftime("%d/%m/%Y"),
        "time": current.strftime("%H:%M:%S"),
    }


# ============================================================
# SIGNAL-AI
# ============================================================

@app.get("/signal-ai")
async def signal_ai(
    symbol: str = "EUR/USD",
    interval: str = "1min"
):

    return await build_signal(
        symbol,
        interval,
        use_openai=True
    )


# ============================================================
# DIAGNOSTIC
# ============================================================

@app.get("/diagnostic")
async def diagnostic(
    symbol: str = "EUR/USD",
    interval: str = "1min"
):

    signal = await build_signal(
        symbol,
        interval,
        use_openai=False
    )

    return {
        "app": "MEGA IA",
        "version": "13.0.0",
        "server_time": now_br().isoformat(),
        "twelve_data_configured": bool(
            TWELVE_DATA_API_KEY
        ),
        "last_twelve_data_error": last_td_error,
        "signal": signal,
    }


# ============================================================
# RADAR ENDPOINT
# ============================================================

@app.get("/radar")
async def radar(
    interval: str = "1min"
):

    global radar_task

    if interval not in INTERVALS:
        raise HTTPException(
            status_code=400,
            detail="Timeframe inválido."
        )

    cached = radar_cache.get(interval)

    # --------------------------------------------------------
    # SE EXISTIR CACHE, RETORNA IMEDIATAMENTE.
    # --------------------------------------------------------

    if cached:

        age = time.time() - cached["timestamp"]

        # Atualiza em segundo plano se estiver velho.
        if age >= RADAR_CACHE_SECONDS:

            if (
                radar_task is None
                or radar_task.done()
            ):
                radar_task = asyncio.create_task(
                    radar_worker(interval)
                )

        return {
            "ok": True,
            "interval": interval,
            "cached": True,
            "age_seconds": int(age),
            "updating": radar_busy,
            "results": cached["data"],
        }

    # --------------------------------------------------------
    # PRIMEIRA CONSULTA:
    # NÃO BLOQUEIA O FRONTEND.
    # --------------------------------------------------------

    if (
        radar_task is None
        or radar_task.done()
    ):

        radar_task = asyncio.create_task(
            radar_worker(interval)
        )

    return {
        "ok": True,
        "interval": interval,
        "cached": False,
        "updating": True,
        "results": [
            {
                "ok": False,
                "symbol": symbol,
                "interval": interval,
                "direction": "NEUTRO",
                "confidence": 0,
                "status": "CARREGANDO",
                "risk": "HIGH"
            }
            for symbol in SYMBOLS
        ]
    }


# ============================================================
# PERFORMANCE
# ============================================================

@app.get("/performance")
async def performance():

    wins = sum(
        1
        for x in result_history
        if x.get("result") == "WIN"
    )

    losses = sum(
        1
        for x in result_history
        if x.get("result") == "LOSS"
    )

    total = wins + losses

    accuracy = (
        (wins / total) * 100
        if total > 0
        else 0
    )

    return {
        "wins": wins,
        "losses": losses,
        "total": total,
        "accuracy": round(
            accuracy,
            2
        ),
    }


# ============================================================
# RESULTS
# ============================================================

@app.get("/results")
async def results():

    return {
        "ok": True,
        "results": result_history[-100:]
    }


# ============================================================
# LICENSE
# ============================================================

@app.get("/license")
async def license():

    try:

        expiration = datetime.strptime(
            LICENSE_EXPIRES,
            "%Y-%m-%d"
        ).replace(
            tzinfo=BR_TZ
        )

    except Exception:

        expiration = datetime(
            2026,
            12,
            31,
            tzinfo=BR_TZ
        )

    current = now_br()

    remaining = (
        expiration.date()
        - current.date()
    ).days

    active = current <= expiration

    return {
        "active": active,
        "expires": expiration.strftime(
            "%d/%m/%Y"
        ),
        "expires_iso": expiration.isoformat(),
        "days_remaining": max(
            0,
            remaining
        ),
        "whatsapp_1": WHATSAPP_1,
        "whatsapp_2": WHATSAPP_2,
        "instagram": INSTAGRAM,
    }


# ============================================================
# HTML
# ============================================================

HTML_PAGE = r"""
<!DOCTYPE html>
<html lang="pt-BR">

<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>

<title>MEGA IA</title>

<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    background:
        radial-gradient(
            circle at top,
            #17213b,
            #070b15 65%
        );
    color: #fff;
    font-family:
        Arial,
        Helvetica,
        sans-serif;
    min-height: 100vh;
}

.container {
    width: min(
        1100px,
        94%
    );

    margin:
        20px auto 40px;
}

.header {
    text-align: center;
    padding: 15px;
}

.logo {
    font-size: 34px;
    font-weight: 900;
}

.subtitle {
    color: #aeb9d5;
    margin-top: 5px;
}

.clock {
    margin-top: 10px;
    font-size: 18px;
}

.controls {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    justify-content: center;
    margin: 20px 0;
}

button,
select {
    border: 1px solid #344568;
    background: #10182a;
    color: white;
    border-radius: 10px;
    padding: 10px 13px;
    cursor: pointer;
}

button.active,
select.active {
    border-color: #5e9cff;
    background: #172b4d;
}

.card {
    background:
        rgba(
            15,
            23,
            42,
            .92
        );

    border: 1px solid #273654;

    border-radius: 18px;

    padding: 22px;

    margin-bottom: 18px;

    box-shadow:
        0 10px 35px
        rgba(0,0,0,.25);
}

.signal-title {
    text-align: center;
    color: #9eabc7;
    font-size: 14px;
    letter-spacing: 2px;
}

.signal {
    text-align: center;
    font-size: 48px;
    font-weight: 900;
    margin: 12px 0;
}

.confidence {
    text-align: center;
    font-size: 20px;
}

.entry {
    text-align: center;
    margin-top: 18px;
}

.entry strong {
    display: block;
    font-size: 30px;
}

.status {
    text-align: center;
    margin-top: 15px;
    font-weight: 700;
}

.risk {
    text-align: center;
    margin-top: 8px;
}

.metrics {
    display: grid;
    grid-template-columns:
        repeat(
            3,
            1fr
        );

    gap: 10px;

    margin-top: 20px;
}

.metric {
    text-align: center;
    padding: 15px;
    background: #0b1220;
    border-radius: 12px;
}

.metric span {
    display: block;
    color: #93a2c1;
    font-size: 12px;
}

.metric strong {
    display: block;
    margin-top: 5px;
    font-size: 24px;
}

.radar-title {
    font-size: 22px;
    font-weight: 800;
    margin-bottom: 15px;
}

.radar {
    display: grid;

    grid-template-columns:
        repeat(
            auto-fit,
            minmax(
                190px,
                1fr
            )
        );

    gap: 10px;
}

.radar-item {
    background: #0b1220;
    border: 1px solid #25334e;
    border-radius: 12px;
    padding: 13px;
}

.radar-symbol {
    font-weight: 800;
}

.radar-direction {
    font-size: 21px;
    font-weight: 900;
    margin-top: 8px;
}

.radar-info {
    color: #9ba8c1;
    font-size: 12px;
    margin-top: 5px;
}

.footer {
    text-align: center;
    color: #8794af;
    padding: 20px;
    font-size: 13px;
}

.error {
    margin-top: 10px;
    color: #ff7777;
    text-align: center;
}

.license {
    text-align: center;
    color: #86e5a5;
}

@media(max-width:650px) {

    .logo {
        font-size: 27px;
    }

    .signal {
        font-size: 40px;
    }

    .metrics {
        grid-template-columns:
            1fr;
    }

}

</style>

</head>

<body>

<div class="container">

    <div class="header">

        <div class="logo">
            🤖 MEGA IA
        </div>

        <div class="subtitle">
            ANÁLISE EM TEMPO REAL • HORÁRIO DE BRASÍLIA
        </div>

        <div
            class="clock"
            id="clock"
        >
            --:--:--
        </div>

    </div>


    <div class="controls">

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


        <button
            data-interval="1min"
            class="tf active"
        >
            1min
        </button>

        <button
            data-interval="5min"
            class="tf"
        >
            5min
        </button>

        <button
            data-interval="15min"
            class="tf"
        >
            15min
        </button>

        <button
            data-interval="30min"
            class="tf"
        >
            30min
        </button>

        <button id="voice">
            🔊 Ativar voz
        </button>

    </div>


    <div class="card">

        <div class="signal-title">
            SINAL ATUAL
        </div>

        <div
            class="signal"
            id="direction"
        >
            AGUARDANDO
        </div>

        <div class="confidence">
            Confiança:
            <strong id="confidence">
                --
            </strong>%
        </div>

        <div class="entry">

            ENTRADA

            <strong id="entry">
                --:--:--
            </strong>

            <span id="expiry">
                --
            </span>

        </div>

        <div
            class="status"
            id="status"
        >
            CONSULTANDO DADOS...
        </div>

        <div class="risk">
            Risco:
            <strong id="risk">
                --
            </strong>
        </div>

        <div
            class="error"
            id="error"
        ></div>

        <div class="metrics">

            <div class="metric">

                <span>WIN</span>

                <strong id="wins">
                    0
                </strong>

            </div>

            <div class="metric">

                <span>LOSS</span>

                <strong id="losses">
                    0
                </strong>

            </div>

            <div class="metric">

                <span>ASSERTIVIDADE</span>

                <strong id="accuracy">
                    0%
                </strong>

            </div>

        </div>

    </div>


    <div class="card">

        <div class="radar-title">
            Radar de oportunidades
        </div>

        <div
            id="radar"
            class="radar"
        >

            <div class="radar-item">
                Radar iniciando...
            </div>

        </div>

    </div>


    <div class="card">

        <div class="license">

            ● LICENÇA ATIVA

            <div id="license">
                Verificando...
            </div>

        </div>

    </div>


    <div class="footer">

        MEGA IA • Ismael Trade

    </div>

</div>


<script>

let interval = "1min";

let lastSignal = null;

let voiceEnabled = false;

const $ = id =>
    document.getElementById(id);


/* =========================================================
   CLOCK
========================================================= */

function updateClock() {

    const now = new Date();

    const formatter =
        new Intl.DateTimeFormat(
            "pt-BR",
            {
                timeZone:
                    "America/Sao_Paulo",

                hour: "2-digit",
                minute: "2-digit",
                second: "2-digit"
            }
        );

    $("clock").textContent =
        formatter.format(now)
        + " • Brasília";
}

setInterval(
    updateClock,
    1000
);

updateClock();


/* =========================================================
   TIMEFRAME
========================================================= */

document
    .querySelectorAll(".tf")
    .forEach(button => {

        button.addEventListener(
            "click",
            () => {

                document
                    .querySelectorAll(".tf")
                    .forEach(
                        x =>
                        x.classList.remove(
                            "active"
                        )
                    );

                button.classList.add(
                    "active"
                );

                interval =
                    button.dataset.interval;

                loadSignal();
                loadRadar();
            }
        );
    });


/* =========================================================
   SIGNAL
========================================================= */

async function loadSignal() {

    const symbol =
        $("symbol").value;

    $("status").textContent =
        "CONSULTANDO DADOS...";

    $("error").textContent = "";

    try {

        const url =
            `/signal-ai?symbol=${encodeURIComponent(symbol)}&interval=${interval}`;

        const response =
            await fetch(url, {
                cache: "no-store"
            });

        const data =
            await response.json();

        if (!response.ok) {
            throw new Error(
                data.detail ||
                "Erro no servidor."
            );
        }

        renderSignal(data);

    } catch(error) {

        $("direction").textContent =
            "NEUTRO";

        $("confidence").textContent =
            "0";

        $("entry").textContent =
            "--:--:--";

        $("expiry").textContent =
            "--";

        $("status").textContent =
            "ERRO NOS DADOS";

        $("risk").textContent =
            "HIGH";

        $("error").textContent =
            error.message;

    }
}


/* =========================================================
   RENDER SIGNAL
========================================================= */

function renderSignal(data) {

    $("direction").textContent =
        data.direction || "NEUTRO";

    $("confidence").textContent =
        data.confidence ?? 0;

    $("entry").textContent =
        data.entry || "--:--:--";

    $("expiry").textContent =
        data.expiry || "--";

    $("status").textContent =
        data.status || "AGUARDANDO";

    $("risk").textContent =
        data.risk || "--";

    $("error").textContent =
        data.ok
            ? ""
            : (
                data.reason ||
                "Twelve Data indisponível."
            );

    if (
        data.direction !== "NEUTRO"
        &&
        data.confirmed
    ) {

        if (
            lastSignal !==
            data.direction
        ) {

            if (
                voiceEnabled
            ) {

                speak(
                    `${data.direction}. ${data.confidence} por cento de confiança.`
                );

            }

            lastSignal =
                data.direction;
        }

    } else {

        lastSignal = null;

    }
}


/* =========================================================
   RADAR
========================================================= */

async function loadRadar() {

    try {

        const response =
            await fetch(
                `/radar?interval=${interval}`,
                {
                    cache:
                        "no-store"
                }
            );

        const data =
            await response.json();

        renderRadar(
           