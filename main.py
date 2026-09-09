import os
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException

app = FastAPI(title="IQ Signal M1 API", version="1.0.0")

TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "")
TWELVE_DATA_URL = "https://api.twelvedata.com/time_series"


@app.get("/")
def root():
    return {
        "ok": True,
        "service": "IQ Signal M1 API",
        "message": "Servidor online.",
        "docs": "/docs"
    }


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/candles")
async def candles(
    symbol: str = "EUR/USD",
    interval: str = "1min",
    outputsize: int = 100
):
    if not TWELVE_DATA_API_KEY:
        raise HTTPException(
            status_code=500,
            detail="TWELVE_DATA_API_KEY não configurada no Render."
        )

    if interval != "1min":
        raise HTTPException(
            status_code=400,
            detail="Esta versão está configurada para candles M1 (1min)."
        )

    outputsize = max(10, min(outputsize, 500))

    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": TWELVE_DATA_API_KEY,
        "timezone": "America/Sao_Paulo",
        "format": "JSON"
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(
                TWELVE_DATA_URL,
                params=params
            )
            response.raise_for_status()
            data: dict[str, Any] = response.json()

    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Erro ao consultar a fonte de dados: {exc}"
        ) from exc

    if data.get("status") == "error":
        raise HTTPException(
            status_code=502,
            detail=data.get(
                "message",
                "Erro retornado pela Twelve Data."
            )
        )

    values = data.get("values", [])

    return {
        "ok": True,
        "source": "Twelve Data",
        "symbol": symbol,
        "interval": interval,
        "count": len(values),
        "values": values
    }


@app.get("/server-time")
def server_time():
    return {
        "utc": datetime.now(timezone.utc).isoformat()
    }
