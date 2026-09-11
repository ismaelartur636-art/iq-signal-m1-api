import os
import asyncio
import time
import json
import re

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse


# ============================================================
# CONFIGURAÇÃO
# ============================================================

app = FastAPI(
    title="MEGA IA",
    version="12.1.0"
)

BR_TZ = ZoneInfo("America/Sao_Paulo")
UTC = timezone.utc

TD_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
OAI_KEY = os.getenv("OPENAI_API_KEY", "").strip()

OAI_MODEL = os.getenv(
    "OPENAI_MODEL",
    "gpt-5.6-luna"
)

OAI_MIN = float(
    os.getenv("OPENAI_MIN_CONFIDENCE", "70")
)

OAI_TIMEOUT = float(
    os.getenv("OPENAI_TIMEOUT", "15")
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

TD_URL = "https://api.twelvedata.com/time_series"

# Intervalos em segundos
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

OAI_URL = "https://api.openai.com/v1/responses"


# ============================================================
# CACHE
# ============================================================

# Cache de candles:
# chave -> {
#   "time": timestamp,
#   "data": candles
# }
candle_cache: Dict[str, Dict[str, Any]] = {}

# Cache dos sinais
signal_cache: Dict[str, Any] = {}

# Cache da IA
oai_cache: Dict[str, Any] = {}

# Resultados
results: Dict[str, Any] = {}


# ============================================================
# CONTROLE DE REQUISIÇÕES TWELVE DATA
# ============================================================

# Tempo mínimo entre chamadas para a Twelve Data.
#
# O valor padrão de 8 segundos foi escolhido para evitar
# excesso de chamadas em planos com limite por minuto.
#
# Pode ser alterado no Render:
#
# TD_MIN_INTERVAL=8
#
TD_MIN_INTERVAL = float(
    os.getenv("TD_MIN_INTERVAL", "8")
)

# Tempo de validade do cache de candles
TD_CACHE_SECONDS = float(
    os.getenv("TD_CACHE_SECONDS", "20")
)

# Número máximo de tentativas
TD_MAX_RETRIES = int(
    os.getenv("TD_MAX_RETRIES", "3")
)

td_lock = asyncio.Lock()

last_td_request = 0.0


# ============================================================
# FUNÇÕES BÁSICAS
# ============================================================

def now():
    return datetime.now(BR_TZ)


def iso(d):
    return d.astimezone(BR_TZ).isoformat()


def parse(s):
    d = datetime.fromisoformat(
        s.replace("Z", "+00:00")
    )

    if d.tzinfo is None:
        d = d.replace(tzinfo=UTC)

    return d.astimezone(BR_TZ)


def clamp(x, a, b):
    return max(a, min(b, x))


# ============================================================
# INDICADORES
# ============================================================

def ema(v, p):

    if len(v) < p:
        return None

    k = 2 / (p + 1)

    e = sum(v[:p]) / p

    for x in v[p:]:
        e = x * k + e * (1 - k)

    return e


def rsi(v, p=14):

    if len(v) < p + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(v)):

        d = v[i] - v[i - 1]

        gains.append(max(d, 0))
        losses.append(max(-d, 0))

    avg_gain = sum(gains[-p:]) / p
    avg_loss = sum(losses[-p:]) / p

    if avg_loss == 0:
        return 100

    return 100 - 100 / (
        1 + avg_gain / avg_loss
    )


# ============================================================
# CONTROLE DE RATE LIMIT
# ============================================================

async def wait_td_slot():

    global last_td_request

    async with td_lock:

        elapsed = time.monotonic() - last_td_request

        if elapsed < TD_MIN_INTERVAL:

            wait_time = (
                TD_MIN_INTERVAL - elapsed
            )

            await asyncio.sleep(wait_time)

        last_td_request = time.monotonic()


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
            detail="TWELVE_DATA_API_KEY não configurada."
        )

    n = int(
        clamp(n, 10, 100)
    )

    cache_key = f"{symbol}|{interval}|{n}"

    # --------------------------------------------------------
    # CACHE
    # --------------------------------------------------------

    cached = candle_cache.get(cache_key)

    if cached:

        age = (
            time.time() - cached["time"]
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


    # --------------------------------------------------------
    # TENTATIVAS
    # --------------------------------------------------------

    last_error = None

    for attempt in range(TD_MAX_RETRIES):

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

                retry_after = response.headers.get(
                    "Retry-After"
                )

                try:
                    wait_seconds = float(
                        retry_after
                    )
                except:

                    wait_seconds = (
                        10 * (attempt + 1)
                    )

                await asyncio.sleep(
                    min(wait_seconds, 30)
                )

                last_error = (
                    "Twelve Data: limite de "
                    "requisições atingido."
                )

                continue


            # ------------------------------------------------
            # OUTROS ERROS HTTP
            # ------------------------------------------------

            if response.status_code >= 400:

                try:
                    error_data = response.json()

                    message = error_data.get(
                        "message",
                        f"HTTP {response.status_code}"
                    )

                except Exception:

                    message = (
                        f"HTTP {response.status_code}"
                    )

                raise HTTPException(
                    status_code=502,
                    detail=message
                )


            data = response.json()


            # ------------------------------------------------
            # ERRO DA TWELVE DATA
            # ------------------------------------------------

            if data.get("status") == "error":

                message = data.get(
                    "message",
                    "Erro Twelve Data."
                )

                if (
                    "limit" in message.lower()
                    or "rate" in message.lower()
                    or "too many" in message.lower()
                ):

                    last_error = message

                    await asyncio.sleep(
                        10 * (attempt + 1)
                    )

                    continue

                raise HTTPException(
                    status_code=502,
                    detail=message
                )


            values = data.get(
                "values",
                []
            )


            if not values:

                raise HTTPException(
                    status_code=502,
                    detail="Nenhum candle recebido."
                )


            # ------------------------------------------------
            # NORMALIZAR
            # ------------------------------------------------

            out = []

            for item in reversed(values):

                try:

                    out.append(
                        {
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
                            )
                        }
                    )

                except Exception:
                    continue


            if not out:

                raise HTTPException(
                    status_code=502,
                    detail="Candles inválidos recebidos."
                )


            # ------------------------------------------------
            # SALVAR CACHE
            # ------------------------------------------------

            candle_cache[cache_key] = {
                "time": time.time(),
                "data": out
            }

            return out


        except HTTPException:
            raise


        except Exception as exc:

            last_error = str(exc)

            if attempt < TD_MAX_RETRIES - 1:

                await asyncio.sleep(
                    2 * (attempt + 1)
                )


    # --------------------------------------------------------
    # SE TIVER CACHE ANTIGO, USA COMO FALLBACK
    # --------------------------------------------------------

    cached = candle_cache.get(
        cache_key
    )

    if cached:

        return cached["data"]


    raise HTTPException(
        status_code=503,
        detail=(
            "Twelve Data temporariamente "
            "indisponível ou limite de requisições "
            "atingido."
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
            "confirmed": False
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
        "CALL": 0,
        "PUT": 0
    }


    # EMA
    if fast is not None and slow is not None:

        if fast > slow:
            votes["CALL"] += 1

        elif fast < slow:
            votes["PUT"] += 1


    # RSI
    if rsi_value is not None:

        if rsi_value <= 35:

            votes["CALL"] += 1.2

        elif rsi_value >= 65:

            votes["PUT"] += 1.2

        elif rsi_value > 50:

            votes["CALL"] += 0.4

        elif rsi_value < 50:

            votes["PUT"] += 0.4


    # Direção do último candle
    if last["close"] > prev["close"]:

        votes["CALL"] += 0.7

    elif last["close"] < prev["close"]:

        votes["PUT"] += 0.7


    # Força do candle
    candle_range = max(
        last["high"] - last["low"],
        1e-12
    )

    body_ratio = abs(
        last["close"] - last["open"]
    ) / candle_range


    if body_ratio >= 0.6:

        if last["close"] > last["open"]:

            votes["CALL"] += 0.7

        else:

            votes["PUT"] += 0.7


    # Resultado
    if votes["CALL"] > votes["PUT"]:

        direction = "CALL"

    elif votes["PUT"] > votes["CALL"]:

        direction = "PUT"

    else:

        direction = "NEUTRO"


    total = sum(
        votes.values()
    )


    if total:

        confidence = (
            50
            + abs(
                votes["CALL"]
                - votes["PUT"]
            ) / total
            * 45
        )

    else:

        confidence = 0


    return {
        "direction": direction,
        "confidence": round(
            clamp(
                confidence,
                0,
                97
            ),
            1
        ),
        "confirmed": confidence >= 65
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
        return json.loads(s)

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
            "available": False
        }


    key = (
        f"{symbol}|"
        f"{interval}|"
        f"{cs[-1]['datetime']}"
    )


    cached = oai_cache.get(key)

    if cached:

        if time.time() - cached[0] < 55:

            return cached[1]


    data = [
        {
            "time": c["datetime"],
            "o": c["open"],
            "h": c["high"],
            "l": c["low"],
            "c": c["close"],
            "v": c["volume"]
        }

        for c in cs[-40:]
    ]


    prompt = f"""
Você é o módulo de confirmação da MEGA IA.

Ativo: {symbol}
Timeframe: {interval}

Use somente candles fechados.

Analise:
- tendência
- momentum
- estrutura
- força
- volatilidade
- possível reversão

Não invente dados futuros.

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
                    "model": OAI_MODEL,
                    "input": prompt
                }
            )


        response.raise_for_status()

        response_data = response.json()

        text = response_data.get(
            "output_text",
            ""
        )


        if not text:

            for item in response_data.get(
                "output",
                []
            ):

                for content in item.get(
                    "content",
                    []
                ):

                    if content.get(
                        "type"
                    ) in (
                        "output_text",
                        "text"
                    ):

                        text += content.get(
                            "text",
                            ""
                        )


        parsed = json_extract(
            text
        )


        if not isinstance(
            parsed,
            dict
        ):

            raise ValueError(
                "Resposta JSON inválida."
            )


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


        out = {
            "available": True,
            "direction": direction,
            "confidence": clamp(
                float(
                    parsed.get(
                        "confidence",
                        0
                    )
                ),
                0,
                100
            ),
            "confirmed": bool(
                parsed.get(
                    "confirmed",
                    False
                )
            ),
            "risk": str(
                parsed.get(
                    "risk",
                    "HIGH"
                )
            ).upper()
        }


        oai_cache[key] = (
            time.time(),
            out
        )


        return out


    except Exception:

        return {
            "available": False
        }


# ============================================================
# PRÓXIMA ENTRADA
# ============================================================

def next_entry(interval):

    seconds = INTERVALS[
        interval
    ]

    timestamp = int(
        now().timestamp()
    )

    next_timestamp = (
        (timestamp // seconds) + 1
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
        f"{symbol}|{interval}"
    )


    # --------------------------------------------------------
    # CACHE DO SINAL
    # --------------------------------------------------------

    cached = signal_cache.get(
        key
    )

    if cached:

        if (
            time.time()
            - cached[0]
            < 8
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

        # Se já houver sinal em cache,
        # mantém o último sinal.

        if cached:

            old = dict(
                cached[1]
            )

            old["status"] = (
                "DADOS TEMPORARIAMENTE "
                "INDISPONÍVEIS"
            )

            return old

        raise exc


    # --------------------------------------------------------
    # REMOVE CANDLE ATUAL
    # --------------------------------------------------------

    if len(cs) > 1:

        cs_closed = cs[:-1]

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
        + timedelta(
            seconds=INTERVALS[interval]
        )
    )


    base = {
        "symbol": symbol,
        "interval": interval,
        "direction": "NEUTRO",
        "confidence": local["confidence"],
        "entry_time": iso(entry),
        "expiry_time": iso(expiry),
        "status": "AGUARDANDO",
        "ai_confirmed": False,
        "risk": "HIGH"
    }


    # --------------------------------------------------------
    # CONFIRMAÇÃO IA
    # --------------------------------------------------------

    if local["confirmed"]:

        ai = await openai_confirm(
            symbol,
            interval,
            cs_closed,
            local
        )


        if (
            ai.get("available")
            and ai["direction"]
            == local["direction"]
            and ai["confirmed"]
            and ai["confidence"]
            >= OAI_MIN
            and ai.get("risk")
            != "HIGH"
        ):

            final_confidence = (
                local["confidence"]
                * 0.45
                +
                ai["confidence"]
                * 0.55
            )


            base.update(
                direction=local["direction"],
                confidence=round(
                    clamp(
                        final_confidence,
                        0,
                        97
                    ),
                    1
                ),
                status="SINAL LIBERADO",
                ai_confirmed=True,
                risk=ai.get(
                    "risk",
                    "MEDIUM"
                )
            )


        elif not ai.get(
            "available"
        ):

            base.update(
                direction=local["direction"],
                status="SINAL LOCAL",
                ai_confirmed=False,
                risk="MEDIUM"
            )


        else:

            base.update(
                confidence=round(
                    min(
                        local["confidence"],
                        ai["confidence"]
                    ),
                    1
                ),
                status=(
                    "AGUARDANDO "
                    "CONFIRMAÇÃO"
                ),
                risk=ai.get(
                    "risk",
                    "HIGH"
                )
            )


    signal_cache[key] = (
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
        "status": "ok",
        "app": "MEGA IA",
        "version": "12.1.0",
        "brasilia_time": iso(
            now()
        ),
        "twelve_data_configured": bool(
            TD_KEY
        ),
        "openai_configured": bool(
            OAI_KEY
        ),
        "td_cache_seconds":
            TD_CACHE_SECONDS,
        "td_min_interval":
            TD_MIN_INTERVAL
    }


# ============================================================
# SERVER TIME
# ============================================================

@app.get("/server-time")
async def server_time():

    return {
        "datetime": iso(
            now()
        ),
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
                - now().date()
            ).days
        )

        active = (
            now().date()
            <= expiration
        )

    except Exception:

        active = False
        days = 0


    return {
        "active": active,
        "expires": LICENSE,
        "days_remaining": days,
        "whatsapp_1": WA1,
        "whatsapp_2": WA2,
        "instagram": IG
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
        "symbol": symbol,
        "interval": interval,
        "candles": await candles(
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
        or interval not in INTERVALS
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
        key: s.get(key)

        for key in (
            "symbol",
            "interval",
            "direction",
            "confidence",
            "status",
            "ai_confirmed",
            "risk"
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


    # --------------------------------------------------------
    # IMPORTANTE:
    #
    # Não fazemos asyncio.gather() aqui.
    #
    # Isso era uma das causas do 429:
    #
    # 12 símbolos = várias requisições simultâneas.
    #
    # Agora cada símbolo é processado separadamente.
    # --------------------------------------------------------

    for symbol in SYMBOLS:

        try:

            value = await signal(
                symbol,
                interval
            )


            output.append(
                {
                    "symbol": symbol,
                    "direction":
                        value["direction"],
                    "confidence":
                        value["confidence"],
                    "status":
                        value["status"]
                }
            )


        except Exception:

            output.append(
                {
                    "symbol": symbol,
                    "direction": "NEUTRO",
                    "confidence": 0,
                    "status":
                        "SEM DADOS"
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
            "name": name,
            "score": 0
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
        if x.get("result") == "WIN"
    )


    losses = sum(
        1
        for x in results.values()
        if x.get("result") == "LOSS"
    )


    total = wins + losses


    accuracy = (
        round(
            wins / total * 100,
            2
        )
        if total
        else 0
    )


    return {
        "wins": wins,
        "losses": losses,
        "total": total,
        "accuracy": accuracy
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

        return results[key]


    if now() < parse(
        expiry_time
    ):

        return {
            "status": "PENDENTE",
            "result": None
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
            "result": None
        }


    target = None


    for c in cs:

        try:

            if parse(
                c["datetime"]
            ) >= parse(
                expiry_time
            ):

                target = c
                break

        except Exception:
            continue


    if not target:

        return {
            "status":
                "AGUARDANDO CANDLE",
            "result": None
        }


    direction = direction.upper()


    if (
        direction == "CALL"
        and target["close"]
        > target["open"]
    ):

        res = "WIN"

    elif (
        direction == "PUT"
        and target["close"]
        < target["open"]
    ):

        res = "WIN"

    else:

        res = "LOSS"


    output = {
        "status": "FINALIZADA",
        "result": res,
        "candle_time":
            target["datetime"]
    }


    results[key] = output


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
font-family:Arial
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

<div id="clock"></div>

<div class="controls">

<select id="symbol"></select>

<select id="interval">

<option>1min</option>
<option>5min</option>
<option>15min</option>
<option>30min</option>

</select>

<button onclick="voice()">
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

const syms=[
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

const S=
document.getElementById('symbol');

syms.forEach(
x=>S.add(
new Option(x,x)
)
);


let cur=null;
let voiceEnabled=false;
let last='';
let five=false;
let entered=false;
let reskey='';


function speak(text){

if(
!voiceEnabled ||
!window.speechSynthesis
)return;

speechSynthesis.cancel();

const u=
new SpeechSynthesisUtterance(
text
);

u.lang='pt-BR';

speechSynthesis.speak(u);

}


function voice(){

voiceEnabled=true;

speak(
'Voz da Mega IA ativada.'
);

}


function ft(x){

return x
?
new Date(x)
.toLocaleTimeString(
'pt-BR',
{
hour12:false
}
)
:
'--:--:--';

}


async function get(url){

const r=
await fetch(
url,
{
cache:'no-store'
}
);

if(!r.ok){

throw new Error(
'HTTP '+r.status
);

}

return r.json();

}


async function sig(){

try{

cur=
await get(
'/signal-ai?symbol='
+
encodeURIComponent(S.value)
+
'&interval='
+
interval.value
);


const d=
document.getElementById(
'direction'
);


d.textContent=
cur.direction;


d.className=
'big '
+
(
cur.direction==='CALL'
?
'call'
:
cur.direction==='PUT'
?
'put'
:
'neutral'
);


confidence.textContent=
'Confiança: '
+
cur.confidence
+
'%';


entry.textContent=
ft(
cur.entry_time
);


status.textContent=
cur.status;


risk.textContent=
'Risco: '
+
cur.risk;


const k=
cur.symbol
+
'|'
+
cur.entry_time
+
'|'
+
cur.direction;


if(
k!==last &&
cur.direction!=='NEUTRO'
){

last=k;

speak(
'Atenção. A Mega IA encontrou uma oportunidade no '
+
cur.symbol.replace('/',' ')
+
'. Sinal '
+
cur.direction
+
'. Entrada programada para '
+
ft(cur.entry_time)
+
'.'
);

}

}catch(e){

status.textContent=
'DADOS TEMPORARIAMENTE INDISPONÍVEIS';

}

}


async function perf(){

try{

const p=
await get(
'/performance'
);

wins.textContent=
p.wins;

losses.textContent=
p.losses;

accuracy.textContent=
p.accuracy
+
'%';

}catch(e){}

}


async function rad(){

try{

const a=
await get(
'/radar?interval='
+
interval.value
);


document.getElementById(
'radar'
).innerHTML=
a.map(
x=>
`
<div>
<b>${x.symbol}</b>
<br>

<span class="${
x.direction==='CALL'
?
'call'
:
x.direction==='PUT'
?
'put'
:
'neutral'
}">
${x.direction}
</span>

•
${x.confidence}%

<br>

<small>
${x.status}
</small>

</div>
`
).join('');

}catch(e){

document.getElementById(
'radar'
).innerHTML=
'<div>Radar aguardando dados...</div>';

}

}


async function lic(){

try{

const x=
await get(
'/license'
);


license.textContent=
x.active
?
`● LICENÇA ATIVA • ${x.expires} • ${x.days_remaining} dias restantes`
:
`● LICENÇA EXPIRADA • ${x.whatsapp_1} / ${x.whatsapp_2} • ${x.instagram}`;

}catch(e){

license.textContent=
'Não foi possível verificar a licença.';

}

}


async function clk(){

try{

const x=
await get(
'/server-time'
);

clock.textContent=
ft(x.datetime)
+
' • Brasília';

}catch(e){}

}


function cd(){

if(!cur)
return;


const n=
Math.ceil(
(
new Date(
cur.entry_time
)
-
Date.now()
) / 1000
);


countdown.textContent=
n>0
?
'Entrada em '
+
n
+
's'
:
'Entrada liberada';


if(
n===5 &&
!five
){

five=true;

speak(
'Atenção. Entrada em 5 segundos.'
);

}


if(
n<=0 &&
n>-2 &&
!entered
){

entered=true;

speak(
'Entrada liberada. '
+
cur.direction
+
' agora.'
);

}

}


async function resultCheck(){

if(
!cur ||
cur.direction==='NEUTRO'
)return;


try{

const x=
await get(
'/result?symbol='
+
encodeURIComponent(
cur.symbol
)
+
'&interval='
+
cur.interval
+
'&direction='
+
cur.direction
+
'&expiry_time='
+
encodeURIComponent(
cur.expiry_time
)
);


if(x.result){

result.textContent=
x.result;


const k=
cur.symbol
+
'|'
+
cur.expiry_time;


if(k!==reskey){

reskey=k;

speak(
'Operação finalizada. Resultado '
+
x.result
+
'.'
);

}

perf();

}

}catch(e){}

}


S.onchange=()=>{

last='';
five=false;
entered=false;

sig();

};


interval.onchange=()=>{

last='';
five=false;
entered=false;

sig();

rad();

};


sig();

perf();

rad();

lic();

clk();


/*
   Não consultar a API a cada segundo.
*/

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
