import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

APP_NAME = "Ismael Trade"
APP_VERSION = "8.1.1"
LICENSE_EXPIRES = os.getenv("LICENSE_EXPIRES", "").strip() or "2026-12-31"
WHATSAPP_1 = "5584998411282"
WHATSAPP_2 = "5584994499442"
INSTAGRAM = "Ismaelartur26"
KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
BASE_URL = "https://api.twelvedata.com/time_series"
SP_TZ = ZoneInfo("America/Sao_Paulo")

ALLOWED_INTERVALS = {"1min": 1, "5min": 5, "15min": 15, "30min": 30}
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

app = FastAPI(title=APP_NAME, version=APP_VERSION)


def now_sp() -> datetime:
    return datetime.now(SP_TZ)


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
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(text, fmt).replace(tzinfo=SP_TZ)
            except ValueError:
                pass
    raise ValueError(f"Timestamp inválido: {value}")


def license_status() -> Dict[str, Any]:
    try:
        expires = datetime.strptime(LICENSE_EXPIRES, "%Y-%m-%d").replace(tzinfo=SP_TZ)
    except ValueError:
        raise HTTPException(500, "LICENSE_EXPIRES inválida. Use AAAA-MM-DD.")
    now = now_sp()
    # A licença vale até o fim do dia configurado em Brasília.
    expires_end = expires.replace(hour=23, minute=59, second=59, microsecond=999999)
    return {
        "active": now <= expires_end,
        "expires": expires.strftime("%d/%m/%Y"),
        "expires_iso": LICENSE_EXPIRES,
        "whatsapp": [WHATSAPP_1, WHATSAPP_2],
        "instagram": INSTAGRAM,
    }


def require_license() -> None:
    status = license_status()
    if not status["active"]:
        raise HTTPException(status_code=403, detail={
            "message": "Licença expirada.",
            "expires": status["expires"],
            "whatsapp": status["whatsapp"],
            "instagram": status["instagram"],
        })


async def get_candles(symbol: str, interval: str, outputsize: int = 120) -> List[Dict[str, Any]]:
    if not KEY:
        raise HTTPException(status_code=500, detail="TWELVE_DATA_API_KEY não configurada no Render.")

    if symbol not in SYMBOLS:
        raise HTTPException(status_code=400, detail="Ativo inválido.")

    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(status_code=400, detail="Timeframe inválido.")

    params = {
        "symbol": SYMBOLS[symbol],
        "interval": interval,
        "outputsize": min(max(int(outputsize), 20), 5000),
        "timezone": "America/Sao_Paulo",
        "apikey": KEY,
        "order": "ASC",
    }

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Erro ao consultar Twelve Data: {exc}") from exc

    if data.get("status") == "error" or "values" not in data:
        message = data.get("message", "Resposta inválida da Twelve Data.")
        raise HTTPException(status_code=502, detail=message)

    candles: List[Dict[str, Any]] = []
    for item in data["values"]:
        try:
            candles.append(
                {
                    "datetime": item["datetime"],
                    "open": float(item["open"]),
                    "high": float(item["high"]),
                    "low": float(item["low"]),
                    "close": float(item["close"]),
                    "volume": float(item.get("volume", 0) or 0),
                }
            )
        except (KeyError, TypeError, ValueError):
            continue

    candles.sort(key=lambda x: parse_time(x["datetime"]))
    if len(candles) < 20:
        raise HTTPException(status_code=502, detail="Poucas velas retornadas pela Twelve Data.")

    return candles


def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1.0)
    result = [values[0]]
    for value in values[1:]:
        result.append((value * alpha) + (result[-1] * (1.0 - alpha)))
    return result


def rsi(values: List[float], period: int = 14) -> List[float]:
    if len(values) < period + 1:
        return [50.0] * len(values)

    gains = [0.0]
    losses = [0.0]
    for i in range(1, len(values)):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))

    result = [50.0] * len(values)
    avg_gain = sum(gains[1 : period + 1]) / period
    avg_loss = sum(losses[1 : period + 1]) / period

    def calc(g: float, l: float) -> float:
        if l == 0:
            return 100.0 if g > 0 else 50.0
        rs = g / l
        return 100.0 - (100.0 / (1.0 + rs))

    result[period] = calc(avg_gain, avg_loss)

    for i in range(period + 1, len(values)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period
        result[i] = calc(avg_gain, avg_loss)

    return result


def true_ranges(candles: List[Dict[str, Any]]) -> List[float]:
    tr: List[float] = []
    for i, candle in enumerate(candles):
        if i == 0:
            tr.append(candle["high"] - candle["low"])
            continue
        previous_close = candles[i - 1]["close"]
        tr.append(
            max(
                candle["high"] - candle["low"],
                abs(candle["high"] - previous_close),
                abs(candle["low"] - previous_close),
            )
        )
    return tr


def adx(candles: List[Dict[str, Any]], period: int) -> List[float]:
    n = len(candles)
    if n < period + 2:
        return [0.0] * n

    tr = true_ranges(candles)
    plus_dm = [0.0] * n
    minus_dm = [0.0] * n

    for i in range(1, n):
        up_move = candles[i]["high"] - candles[i - 1]["high"]
        down_move = candles[i - 1]["low"] - candles[i]["low"]
        if up_move > down_move and up_move > 0:
            plus_dm[i] = up_move
        if down_move > up_move and down_move > 0:
            minus_dm[i] = down_move

    result = [0.0] * n
    atr = sum(tr[1 : period + 1]) / period
    plus = sum(plus_dm[1 : period + 1]) / period
    minus = sum(minus_dm[1 : period + 1]) / period

    dx_values: List[float] = [0.0] * n

    for i in range(period, n):
        if i > period:
            atr = ((atr * (period - 1)) + tr[i]) / period
            plus = ((plus * (period - 1)) + plus_dm[i]) / period
            minus = ((minus * (period - 1)) + minus_dm[i]) / period

        plus_di = 100.0 * plus / atr if atr else 0.0
        minus_di = 100.0 * minus / atr if atr else 0.0
        denom = plus_di + minus_di
        dx_values[i] = 100.0 * abs(plus_di - minus_di) / denom if denom else 0.0

    start = period
    if n > start + period:
        initial = dx_values[start : start + period]
        adx_value = sum(initial) / len(initial)
        result[start + period - 1] = adx_value

        for i in range(start + period, n):
            adx_value = ((adx_value * (period - 1)) + dx_values[i]) / period
            result[i] = adx_value

    return result


def analyze(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    # Somente velas fechadas são usadas na análise.
    # A última vela pode ainda estar em formação.
    if len(candles) < 60:
        raise HTTPException(status_code=422, detail="Dados insuficientes para análise.")

    closed = candles[:-1]
    closes = [c["close"] for c in closed]

    ema3_values = ema(closes, 3)
    ema7_values = ema(closes, 7)
    rsi14_values = rsi(closes, 14)
    adx21_values = adx(closed, 21)
    adx48_values = adx(closed, 48)

    i = len(closed) - 1
    ema3_value = ema3_values[i]
    ema7_value = ema7_values[i]
    rsi14_value = rsi14_values[i]
    adx21_value = adx21_values[i]
    adx48_value = adx48_values[i]

    call_score = 0
    put_score = 0

    if ema3_value > ema7_value:
        call_score += 2
    elif ema3_value < ema7_value:
        put_score += 2

    if rsi14_value >= 50:
        call_score += 1
    if rsi14_value <= 50:
        put_score += 1

    if adx21_value >= 20:
        if ema3_value > ema7_value:
            call_score += 1
        elif ema3_value < ema7_value:
            put_score += 1

    if adx48_value >= 20:
        if ema3_value > ema7_value:
            call_score += 1
        elif ema3_value < ema7_value:
            put_score += 1

    if rsi14_value < 30:
        call_score += 1
    elif rsi14_value > 70:
        put_score += 1

    if call_score >= 4 and call_score > put_score:
        signal = "CALL"
        confidence = min(95, 70 + (call_score - put_score) * 5)
    elif put_score >= 4 and put_score > call_score:
        signal = "PUT"
        confidence = min(95, 70 + (put_score - call_score) * 5)
    else:
        signal = "NEUTRO"
        confidence = 50

    reference = closed[-1]
    next_candle = candles[-1]

    return {
        "signal": signal,
        "confidence": confidence,
        "reference_candle": reference["datetime"],
        "next_candle": next_candle["datetime"],
        "ema3": round(ema3_value, 8),
        "ema7": round(ema7_value, 8),
        "rsi14": round(rsi14_value, 2),
        "adx21": round(adx21_value, 2),
        "adx48": round(adx48_value, 2),
        "call_score": call_score,
        "put_score": put_score,
        "non_repaint_reference": True,
        "ok": True,
    }



def candle_is_closed(candle_time: datetime, interval: str) -> bool:
    minutes = ALLOWED_INTERVALS[interval]
    return now_sp() >= candle_time + timedelta(minutes=minutes)


def body(c: Dict[str, Any]) -> float:
    return abs(float(c["close"]) - float(c["open"]))


def bull(c: Dict[str, Any]) -> bool:
    return float(c["close"]) > float(c["open"])


def bear(c: Dict[str, Any]) -> bool:
    return float(c["close"]) < float(c["open"])


def upper_wick(c: Dict[str, Any]) -> float:
    return float(c["high"]) - max(float(c["open"]), float(c["close"]))


def lower_wick(c: Dict[str, Any]) -> float:
    return min(float(c["open"]), float(c["close"])) - float(c["low"])


def candle_range(c: Dict[str, Any]) -> float:
    return max(float(c["high"]) - float(c["low"]), 1e-12)


def bullish_engulfing(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    return (bear(a) and bull(b) and float(b["open"]) <= float(a["close"])
            and float(b["close"]) >= float(a["open"]) and body(b) > body(a))


def bearish_engulfing(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    return (bull(a) and bear(b) and float(b["open"]) >= float(a["close"])
            and float(b["close"]) <= float(a["open"]) and body(b) > body(a))


def bottom_rejection(c: Dict[str, Any]) -> bool:
    r = candle_range(c)
    return (lower_wick(c) >= body(c) * 1.2 and lower_wick(c) >= upper_wick(c) * 1.5
            and float(c["close"]) > float(c["low"]) + r * 0.55)


def top_rejection(c: Dict[str, Any]) -> bool:
    r = candle_range(c)
    return (upper_wick(c) >= body(c) * 1.2 and upper_wick(c) >= lower_wick(c) * 1.5
            and float(c["close"]) < float(c["high"]) - r * 0.55)


def buyer_strength(c: Dict[str, Any]) -> bool:
    r = candle_range(c)
    return (bull(c) and body(c) / r >= 0.55
            and (float(c["high"]) - float(c["close"])) / r <= 0.25)


def seller_strength(c: Dict[str, Any]) -> bool:
    r = candle_range(c)
    return (bear(c) and body(c) / r >= 0.55
            and (float(c["close"]) - float(c["low"])) / r <= 0.25)


def sniper(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if len(rows) < 3:
        return {"signal": "NEUTRO", "confidence": 0, "call_score": 0, "put_score": 0}
    prev = rows[-2]
    cur = rows[-1]
    call = [
        bullish_engulfing(prev, cur),
        bottom_rejection(cur),
        float(cur["high"]) > float(prev["high"]),
        buyer_strength(cur),
        bull(prev) or float(cur["close"]) > float(prev["close"]),
    ]
    put = [
        bearish_engulfing(prev, cur),
        top_rejection(cur),
        float(cur["low"]) < float(prev["low"]),
        seller_strength(cur),
        bear(prev) or float(cur["close"]) < float(prev["close"]),
    ]
    cs = sum(call)
    ps = sum(put)
    if cs >= 3 and cs > ps:
        sig, score = "CALL", cs
    elif ps >= 3 and ps > cs:
        sig, score = "PUT", ps
    else:
        sig, score = "NEUTRO", max(cs, ps)
    conf = min(95, 55 + score * 8) if sig != "NEUTRO" else 0
    return {
        "signal": sig,
        "confidence": conf,
        "call_score": cs,
        "put_score": ps,
        "reference_candle": cur["datetime"],
    }


@app.get("/", response_class=HTMLResponse)
async def home() -> HTMLResponse:
    html = r"""<!doctype html> <html lang="pt-BR"> <head> <meta charset="utf-8"> <meta name="viewport" content="width=device-width,initial-scale=1"> <title>Ismael Trade</title> <style> *{box-sizing:border-box}body{margin:0;background:#0b1020;color:#f5f7ff;font-family:Arial,sans-serif} .container{max-width:760px;margin:auto;padding:16px}.card{background:#151c30;border:1px solid #29324b; border-radius:16px;padding:16px;margin:12px 0}.row{display:flex;gap:10px;flex-wrap:wrap} .field{flex:1;min-width:170px}label,.small{color:#aeb7cc;font-size:13px}label{display:block;margin-bottom:6px} select,button{width:100%;padding:12px;border-radius:10px;border:1px solid #34405e;background:#0f1526;color:#fff} button{cursor:pointer;font-weight:bold}.signal{text-align:center;padding:20px;border-radius:14px;font-size:38px;font-weight:800} .call{background:#103c2b;color:#52f09d}.put{background:#481d28;color:#ff718b}.neutral{background:#2b3040;color:#d9deeb} .grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}.stat{background:#0f1526;border-radius:12px;padding:13px;text-align:center} .stat b{display:block;font-size:24px;margin-top:5px}.good{color:#52f09d}.bad{color:#ff718b} .params{display:grid;grid-template-columns:1fr 1fr;gap:8px}.param{background:#0f1526;border-radius:10px;padding:10px} .hidden{display:none}.toggle{display:flex;align-items:center;justify-content:space-between;gap:10px} #timer{text-align:center;font-size:34px;font-weight:bold;margin:8px}h1{margin:5px 0}.sub{color:#aeb7cc} .footer{font-size:12px;color:#7f8aa5;line-height:1.5}.license-overlay{position:fixed;inset:0;background:rgba(4,7,15,.96);display:flex;align-items:center;justify-content:center;padding:20px;z-index:9999}.license-box{width:min(460px,100%);background:#151c30;border:1px solid #394563;border-radius:20px;padding:28px;text-align:center;box-shadow:0 20px 60px rgba(0,0,0,.45)}.license-icon{font-size:48px}.license-box h2{margin:10px 0;color:#ff718b}.license-box p{color:#c8cede;line-height:1.5}.license-date{margin:16px 0;color:#aeb7cc}.license-btn{display:block;text-decoration:none;background:#0f1526;color:#fff;border:1px solid #34405e;border-radius:10px;padding:13px;margin:9px 0;font-weight:bold}.license-btn:hover{filter:brightness(1.15)} @media(max-width:520px){.grid{grid-template-columns:1fr 1fr}.params{grid-template-columns:1fr}} </style> </head> <body><div class="container"> <h1>📈 Ismael Trade</h1><div class="sub">SNIPER • entrada em horário de Brasília</div> <div class="card"><div class="row"> <div class="field"><label>ATIVO</label><select id="symbol"> <option>EUR/USD</option><option>GBP/USD</option><option>USD/JPY</option><option>AUD/USD</option> <option>USD/CAD</option><option>USD/CHF</option><option>NZD/USD</option><option>EUR/JPY</option> <option>GBP/JPY</option><option>EUR/GBP</option><option>BTC/USD</option></select></div> <div class="field"><label>TEMPO</label><select id="interval"> <option value="1min">M1</option><option value="5min">M5</option><option value="15min">M15</option> <option value="30min">M30</option></select></div></div> <button id="refresh" style="margin-top:10px">ATUALIZAR SINAL</button></div> <div class="card"><div class="toggle"><div><div class="small">🎯 ESTRATÉGIA</div><b>SNIPER</b></div> <button id="sniperBtn" style="width:auto">ATIVADA</button></div> <div class="small" style="margin-top:10px">Engolfo • rejeição • rompimento • força • estrutura</div></div> <div class="card"><div class="small">CRONÔMETRO DA VELA</div><div id="timer">--:--</div> <div id="entry" class="small">Entrada Brasília: --</div></div> <div class="card"><div class="small">SINAL</div><div id="signal" class="signal neutral">AGUARDANDO</div> <div class="row" style="margin-top:12px"><div class="field"><div class="small">Confiança</div><b id="confidence">--</b></div> <div class="field"><div class="small">Referência</div><b id="reference">--</b></div></div></div> <div class="card"><div class="small">RESULTADOS</div><div class="grid"> <div class="stat">WIN<b id="wins" class="good">0</b></div><div class="stat">LOSS<b id="losses" class="bad">0</b></div> <div class="stat">ASSERT.<b id="accuracy">0%</b></div></div><button id="reset" style="margin-top:10px">ZERAR RESULTADOS</button></div> <div class="card"><div class="toggle"><div><div class="small">⚙️ PARÂMETROS DOS INDICADORES</div> <b id="paramStatus">OCULTO</b></div><button id="toggleParams" style="width:auto">MOSTRAR</button></div> <div id="paramsBox" class="params hidden" style="margin-top:12px"> <div class="param">EMA 3</div><div class="param">EMA 7</div><div class="param">RSI 14</div> <div class="param">ADX 21</div><div class="param">ADX 48</div><div class="param">CALL Score <b id="callScore">--</b></div> <div class="param">PUT Score <b id="putScore">--</b></div></div></div> <div class="card footer">A SNIPER é uma estratégia algorítmica baseada nas regras fornecidas. O sinal é probabilístico, não garante WIN e não executa operações automaticamente. Os dados podem diferir da corretora.</div> </div> <div id="licenseOverlay" class="license-overlay hidden"> <div class="license-box"> <div class="license-icon">🔒</div> <h2>LICENÇA EXPIRADA</h2> <p>Sua licença do <b>Ismael Trade</b> expirou.</p> <p>Para continuar utilizando o aplicativo, entre em contato para renovar sua licença.</p> <div class="license-date" id="licenseDate">Vencimento: --</div> <a id="wa1" class="license-btn" target="_blank" rel="noopener noreferrer" href="https://wa.me/5584998411282?text=Ol%C3%A1%20Ismael%20Trade%2C%20quero%20renovar%20minha%20licen%C3%A7a.">📱 Renovar pelo WhatsApp 1</a> <a id="wa2" class="license-btn" target="_blank" rel="noopener noreferrer" href="https://wa.me/5584994499442?text=Ol%C3%A1%20Ismael%20Trade%2C%20quero%20renovar%20minha%20licen%C3%A7a.">📱 Renovar pelo WhatsApp 2</a> <a id="ig" class="license-btn" target="_blank" rel="noopener noreferrer" href="https://www.instagram.com/Ismaelartur26/">📸 Instagram @Ismaelartur26</a> </div> </div> <script> const $=id=>document.getElementById(id); let sniperOn=localStorage.getItem('sniper_on')!=='0'; let stats=JSON.parse(localStorage.getItem('it_stats')||'{"wins":0,"losses":0}'); let pending=JSON.parse(localStorage.getItem('it_pending')||'null');let left=0; function save(){localStorage.setItem('it_stats',JSON.stringify(stats)); if(pending)localStorage.setItem('it_pending',JSON.stringify(pending));else localStorage.removeItem('it_pending');} function statsUI(){wins.textContent=stats.wins;losses.textContent=stats.losses; let n=stats.wins+stats.losses;accuracy.textContent=n?((stats.wins/n)*100).toFixed(1)+'%':'0%';} function modeUI(){sniperBtn.textContent=sniperOn?'ATIVADA':'DESATIVADA';} function toggleSniper(){sniperOn=!sniperOn;localStorage.setItem('sniper_on',sniperOn?'1':'0');modeUI();loadSignal();} async function checkLicense(){ try{ let r=await fetch('/license?ts='+Date.now(), {cache:'no-store'}); let d=await r.json(); if(!r.ok || !d.active){showExpired(d); return false;} return true; }catch(e){ showExpired({expires:'31/12/2026', whatsapp:['5584998411282','5584994499442'], instagram:'Ismaelartur26', message:'Não foi possível validar a licença no servidor.'}); return false; } } function showExpired(d){ licenseOverlay.classList.remove('hidden'); licenseDate.textContent='Vencimento: '+(d.expires||'31/12/2026'); wa1.href='https://wa.me/'+(d.whatsapp?.[0]||'5584998411282')+'?text='+encodeURIComponent('Olá Ismael Trade, quero renovar minha licença.'); wa2.href='https://wa.me/'+(d.whatsapp?.[1]||'5584994499442')+'?text='+encodeURIComponent('Olá Ismael Trade, quero renovar minha licença.'); ig.href='https://instagram.com/'+(d.instagram||'Ismaelartur26'); } async function loadSignal(){if(!(await checkLicense()))return;try{ let s=symbol.value,i=interval.value,mode=sniperOn?'SNIPER':'OFF'; let r=await fetch('/signal?symbol='+encodeURIComponent(s)+'&interval='+i+'&strategy='+mode);let d=await r.json(); if(!r.ok)throw Error(d.detail||'Erro');left=d.seconds_remaining||0; signal.textContent=d.signal;signal.className='signal '+(d.signal==='CALL'?'call':d.signal==='PUT'?'put':'neutral'); confidence.textContent=(d.confidence||0)+'%';reference.textContent=d.reference_candle||'--'; entry.textContent='Entrada Brasília: '+(d.entry_brasilia||'--');callScore.textContent=d.call_score??'--';putScore.textContent=d.put_score??'--'; if(sniperOn&&d.signal!=='NEUTRO'){pending={symbol:s,interval:i,reference_candle:d.reference_candle,direction:d.signal};save();} }catch(e){signal.textContent='ERRO';signal.className='signal neutral';confidence.textContent=e.message;}} async function checkResult(){if(!pending)return;if(!(await checkLicense()))return;try{ let q=new URLSearchParams(pending);let r=await fetch('/result?'+q.toString());let d=await r.json(); if(d.result==='WIN'){stats.wins++;pending=null;save();statsUI();} else if(d.result==='LOSS'){stats.losses++;pending=null;save();statsUI();} else if(d.result==='DRAW'){pending=null;save();}} catch(e){}} setInterval(()=>{if(left>0)left--;let m=Math.floor(left/60),s=left%60; timer.textContent=String(m).padStart(2,'0')+':'+String(s).padStart(2,'0');},1000); setInterval(loadSignal,60000);setInterval(checkResult,15000); refresh.onclick=loadSignal;sniperBtn.onclick=toggleSniper;symbol.onchange=loadSignal;interval.onchange=loadSignal; let visible=localStorage.getItem('it_params')==='1';function paramsUI(){paramsBox.classList.toggle('hidden',!visible); paramStatus.textContent=visible?'VISÍVEL':'OCULTO';toggleParams.textContent=visible?'OCULTAR':'MOSTRAR';} toggleParams.onclick=()=>{visible=!visible;localStorage.setItem('it_params',visible?'1':'0');paramsUI();}; reset.onclick=()=>{if(confirm('Zerar WIN e LOSS?')){stats={wins:0,losses:0};pending=null;save();statsUI();}}; modeUI();paramsUI();statsUI();loadSignal();checkResult(); </script></body></html>"""
    return HTMLResponse(html)


@app.get("/license")
async def license() -> Dict[str, Any]:
    return license_status()


@app.get("/health")
async def health() -> Dict[str, Any]:
    return {"ok": True, "app": APP_NAME, "version": APP_VERSION}


@app.get("/server-time")
async def server_time() -> Dict[str, str]:
    current = now_sp()
    return {"brasilia": current.isoformat(), "utc": datetime.now(timezone.utc).isoformat()}


@app.get("/candles")
async def candles( symbol: str = "EUR/USD", interval: str = "1min", size: int = 120 ) -> Dict[str, Any]:
    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(400, "Intervalo inválido.")
    values = await get_candles(symbol, interval, int(size))
    return {"ok": True, "source": "Twelve Data", "symbol": symbol,
            "interval": interval, "values": values}


@app.get("/signal")
async def signal( symbol: str = "EUR/USD", interval: str = "1min", strategy: str = "SNIPER" ) -> Dict[str, Any]:
    require_license()
    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(400, "Intervalo inválido.")
    values = await get_candles(symbol, interval, 160)
    closed = values[:-1] if len(values) > 1 else values
    result = sniper(closed) if strategy.upper() == "SNIPER" else {
        "signal": "NEUTRO", "confidence": 0, "call_score": 0, "put_score": 0
    }
    now = now_sp()
    period = ALLOWED_INTERVALS[interval] * 60
    seconds = now.minute * 60 + now.second
    remaining = period - (seconds % period)
    result.update({
        "ok": True, "strategy": strategy.upper(), "symbol": symbol,
        "interval": interval, "entry_brasilia": now.strftime("%d/%m/%Y %H:%M:%S"),
        "seconds_remaining": remaining, "non_repaint_reference": True,
        "source": "Twelve Data",
        "warning": "Sinal probabilístico; não garante WIN e não executa operações.",
    })
    return result


@app.get("/result")
async def result( symbol: str, interval: str, reference_candle: str, direction: str ) -> Dict[str, Any]:
    require_license()
    direction = direction.upper().strip()
    if direction not in {"CALL", "PUT"}:
        raise HTTPException(400, "Direção inválida.")
    values = await get_candles(symbol, interval, 160)
    ref = parse_time(reference_candle)
    idx = None
    for i, c in enumerate(values):
        if parse_time(c["datetime"]) == ref:
            idx = i
            break
    if idx is None or idx + 1 >= len(values):
        return {"ok": True, "status": "PENDING", "result": None}
    nxt = values[idx + 1]
    if not candle_is_closed(parse_time(nxt["datetime"]), interval):
        return {"ok": True, "status": "PENDING", "result": None}
    op, cl = float(nxt["open"]), float(nxt["close"])
    if cl == op:
        out = "DRAW"
    elif direction == "CALL":
        out = "WIN" if cl > op else "LOSS"
    else:
        out = "WIN" if cl < op else "LOSS"
    return {"ok": True, "status": "CLOSED", "result": out,
            "direction": direction, "reference_candle": reference_candle,
            "result_candle": nxt["datetime"], "open": op, "close": cl}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))