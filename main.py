import os
from datetime import datetime, timezone
from typing import Any
import asyncio

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

app = FastAPI(
    title="Ismael Trade",
    version="6.0.0"
)

# RECOMENDAÇÃO: Insira sua chave padrão aqui se não usar variáveis de ambiente
KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
URL = "https://api.twelvedata.com/time_series"

ALLOWED_INTERVALS = {
    "1min",
    "5min",
    "15min",
    "30min"
}

ASSETS = [
    "EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD",
    "USD/CAD", "USD/CHF", "NZD/USD", "EUR/JPY",
    "GBP/JPY", "EUR/GBP", "BTC/USD", "ETH/USD"
]

# =========================================================
# LÓGICA DE INDICADORES (Back-end)
# =========================================================

def _cal_ema(prices: list[float], period: int) -> list[float]:
    if len(prices) < period:
        return [0.0] * len(prices)
    
    ema = [0.0] * len(prices)
    # Inicializa com a média aritmética simples (SMA)
    sma = sum(prices[:period]) / period
    ema[period - 1] = sma
    
    multiplier = 2 / (period + 1)
    for i in range(period, len(prices)):
        ema[i] = (prices[i] - ema[i - 1]) * multiplier + ema[i - 1]
        
    return ema

def _cal_rsi(prices: list[float], period: int = 14) -> float:
    if len(prices) <= period:
        return 50.0
        
    gains = []
    losses = []
    
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i - 1]
        if diff > 0:
            gains.append(diff)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(diff))
            
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    
    if avg_loss == 0:
        return 100.0
        
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        
    if avg_loss == 0:
        return 100.0
        
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

def _cal_adx(highs: list[float], lows: list[float], closes: list[float], period: int) -> float:
    if len(closes) <= period * 2:
        return 20.0
        
    tr_list = []
    plus_dm_list = []
    minus_dm_list = []
    
    for i in range(1, len(closes)):
        tr1 = highs[i] - lows[i]
        tr2 = abs(highs[i] - closes[i - 1])
        tr3 = abs(lows[i] - closes[i - 1])
        tr = max(tr1, tr2, tr3)
        tr_list.append(tr)
        
        up_move = highs[i] - highs[i - 1]
        down_move = lows[i - 1] - lows[i]
        
        if up_move > down_move and up_move > 0:
            plus_dm_list.append(up_move)
        else:
            plus_dm_list.append(0.0)
            
        if down_move > up_move and down_move > 0:
            minus_dm_list.append(down_move)
        else:
            minus_dm_list.append(0.0)
            
    # Média suavizada (Wilder's Smoothing)
    atr = sum(tr_list[:period])
    plus_di_sum = sum(plus_dm_list[:period])
    minus_di_sum = sum(minus_dm_list[:period])
    
    dx_list = []
    
    for i in range(period, len(tr_list)):
        atr = atr - (atr / period) + tr_list[i]
        plus_di_sum = plus_di_sum - (plus_di_sum / period) + plus_dm_list[i]
        minus_di_sum = minus_di_sum - (minus_di_sum / period) + minus_dm_list[i]
        
        if atr == 0:
            dx_list.append(0.0)
            continue
            
        p_di = (plus_di_sum / atr) * 100
        m_di = (minus_di_sum / atr) * 100
        
        div = p_di + m_di
        dx = (abs(p_di - m_di) / div * 100) if div != 0 else 0
        dx_list.append(dx)
        
    if len(dx_list) < period:
        return 20.0
        
    return sum(dx_list[-period:]) / period

async def _fetch_asset_data(client: httpx.AsyncClient, symbol: str, interval: str) -> dict[str, Any]:
    """Busca os dados de forma assíncrona na Twelve Data."""
    if not KEY:
        raise HTTPException(status_code=500, detail="Chave de API Twelve Data não configurada no servidor.")
        
    params = {
        "symbol": symbol,
        "interval": interval,
        "apikey": KEY,
        "outputsize": "80"  # Quantidade suficiente para ADX 48
    }
    
    try:
        response = await client.get(URL, params=params, timeout=10.0)
        if response.status_code != 200:
            raise HTTPException(status_code=400, detail=f"Erro na API externa para {symbol}: {response.text}")
            
        data = response.json()
        if data.get("status") == "error":
            raise HTTPException(status_code=400, detail=f"Twelve Data Erro ({symbol}): {data.get('message')}")
            
        return data
    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"Falha na comunicação com o provedor de dados: {str(e)}")

def _process_analysis(data: dict[str, Any], symbol: str, interval: str) -> dict[str, Any]:
    """Processa a estratégia matemática dos indicadores."""
    values = data.get("values", [])
    if not values or len(values) < 50:
        raise HTTPException(status_code=400, detail=f"Dados insuficientes retornados para {symbol}.")
        
    # Inverte a ordem para cronológica (do mais antigo para o mais recente)
    values = values[::-1]
    
    closes = [float(v["close"]) for v in values]
    highs = [float(v["high"]) for v in values]
    lows = [float(v["low"]) for v in values]
    
    # Processamento de Indicadores
    ema3_list = _cal_ema(closes, 3)
    ema7_list = _cal_ema(closes, 7)
    
    rsi_val = _cal_rsi(closes, 14)
    adx21_val = _cal_adx(highs, lows, closes, 21)
    adx48_val = _cal_adx(highs, lows, closes, 48)
    
    current_ema3 = ema3_list[-1]
    current_ema7 = ema7_list[-1]
    
    # Lógica de Direção do Sinal
    signal = "WAIT"
    confidence = 0
    
    if current_ema3 > current_ema7 and rsi_val < 65:
        if adx21_val > 22 or adx48_val > 20:
            signal = "CALL"
            confidence = min(100, int(40 + (adx21_val * 0.8) + (65 - rsi_val)))
    elif current_ema3 < current_ema7 and rsi_val > 35:
        if adx21_val > 22 or adx48_val > 20:
            signal = "PUT"
            confidence = min(100, int(40 + (adx21_val * 0.8) + (rsi_val - 35)))
            
    if signal == "WAIT":
        confidence = 0

    # Define referências textuais de tempo
    ref_time = values[-1]["datetime"]
    
    return {
        "symbol": symbol,
        "interval": interval,
        "signal": signal,
        "confidence": f"{confidence}%" if signal != "WAIT" else "--",
        "ema3": f"{current_ema3:.5f}",
        "ema7": f"{current_ema7:.5f}",
        "rsi": f"{rsi_val:.2f}",
        "adx21": f"{adx21_val:.2f}",
        "adx48": f"{adx48_val:.2f}",
        "reference_candle": ref_time,
        "next_candle": "Próximo minuto" if interval == "1min" else f"Próximos {interval}"
    }

# =========================================================
# ROTAS (Endpoints)
# =========================================================

@app.get("/", response_class=HTMLResponse)
def get_interface():
    return HTML_PAGE

@app.get("/signal")
async def get_signal(symbol: str, interval: str):
    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(status_code=400, detail="Intervalo de tempo gráfico inválido.")
        
    async with httpx.AsyncClient() as client:
        if symbol.upper() == "ALL":
            # Executa requisições em paralelo de forma eficiente e sem travar
            tasks = [_fetch_asset_data(client, s, interval) for s in ASSETS]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            processed_results = []
            for idx, res in enumerate(results):
                asset_name = ASSETS[idx]
                if isinstance(res, Exception) or isinstance(res, HTTPException):
                    continue  # Ignora ativos que falharam na API externa
                try:
                    processed = _process_analysis(res, asset_name, interval)
                    processed_results.append(processed)
                except Exception:
                    continue
                    
            return {"is_all": True, "results": processed_results}
            
        else:
            if symbol.upper() not in ASSETS:
                raise HTTPException(status_code=400, detail="Ativo não suportado ou inválido.")
            
            raw_data = await _fetch_asset_data(client, symbol.upper(), interval)
            processed = _process_analysis(raw_data, symbol.upper(), interval)
            return {"is_all": False, **processed}


# =========================================================
# INTERFACE (HTML Corrigido e Completado)
# =========================================================

# Substitui a string HTML antiga trazendo o JavaScript finalizado
HTML_PAGE = HTML_PAGE.split("async function getSignal")[0] + """async function getSignal(symbol, interval) {
    const response = await fetch("/signal?symbol=" + encodeURIComponent(symbol) + "&interval=" + encodeURIComponent(interval));
    let data;
    try { data = await response.json(); } catch { throw new Error("Resposta inválida da API."); }
    if (!response.ok) { throw new Error(data.detail || "Erro na análise."); }
    return data;
}

function mostrarResultado(data) {
    const signal = document.getElementById("signal");
    const singleCard = document.getElementById("singleCard");
    const details = document.getElementById("details");
    const referenceCard = document.getElementById("referenceCard");
    const allCard = document.getElementById("allCard");
    const allResults = document.getElementById("allResults");

    if (data.is_all) {
        singleCard.style.display = "none";
        details.style.display = "none";
        referenceCard.style.display = "none";
