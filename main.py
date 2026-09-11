import os
import asyncio
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

try:
    from sklearn.ensemble import RandomForestClassifier, HistGradientBoostingClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler
    SKLEARN_OK = True
except Exception:
    SKLEARN_OK = False

try:
    from xgboost import XGBClassifier
    XGBOOST_OK = True
except Exception:
    XGBOOST_OK = False

try:
    from lightgbm import LGBMClassifier
    LIGHTGBM_OK = True
except Exception:
    LIGHTGBM_OK = False

APP_NAME = "Ismael Trade"
APP_VERSION = "9.2.2"
KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
BASE_URL = "https://api.twelvedata.com/time_series"
SP_TZ = ZoneInfo("America/Sao_Paulo")
LICENSE_EXPIRES = os.getenv("ISMAEL_TRADE_LICENSE_EXPIRES", "2026-12-31").strip()
LICENSE_WHATSAPP_1 = "55 84 99841-1282"
LICENSE_WHATSAPP_2 = "55 84 99449-9442"
LICENSE_INSTAGRAM = "@Ismaelartur26"

ALLOWED_INTERVALS = {"1min": 1, "5min": 5, "15min": 15, "30min": 30}
SYMBOLS = {x: x for x in [
    "EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD", "USD/CHF",
    "NZD/USD", "EUR/JPY", "GBP/JPY", "EUR/GBP", "BTC/USD", "ETH/USD"
]}
RADAR_SYMBOLS = ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD", "USD/CHF"]

app = FastAPI(title=APP_NAME, version=APP_VERSION)
CANDLE_CACHE: Dict[tuple, tuple] = {}
CANDLE_LOCKS: Dict[tuple, asyncio.Lock] = {}
RADAR_CACHE: Dict[tuple, tuple] = {}
AI_MODEL_CACHE: Dict[tuple, tuple] = {}
CACHE_TTL_SECONDS = 60.0
RADAR_TTL_SECONDS = 120.0
AI_MODEL_TTL_SECONDS = 300.0
RATE_LIMIT_UNTIL = 0.0


def now_sp() -> datetime:
    return datetime.now(SP_TZ)


def parse_time(value: str) -> datetime:
    text = str(value).strip().replace("\u00a0", " ")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(text, fmt).replace(tzinfo=SP_TZ)
            except ValueError:
                continue
        raise ValueError(f"Timestamp inválido: {value}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=SP_TZ)
    return dt.astimezone(SP_TZ)


def license_status() -> Dict[str, Any]:
    try:
        expires = datetime.strptime(LICENSE_EXPIRES, "%Y-%m-%d").replace(tzinfo=SP_TZ)
    except ValueError:
        expires = datetime(2026, 12, 31, tzinfo=SP_TZ)
    expires = expires.replace(hour=23, minute=59, second=59)
    active = now_sp() <= expires
    return {"active": active, "expires": expires.strftime("%d/%m/%Y"),
            "expires_iso": expires.isoformat(), "whatsapp_1": LICENSE_WHATSAPP_1,
            "whatsapp_2": LICENSE_WHATSAPP_2, "instagram": LICENSE_INSTAGRAM}


def require_active_license() -> None:
    status = license_status()
    if not status["active"]:
        raise HTTPException(status_code=403, detail={"error": "LICENSE_EXPIRED", **status})


async def get_candles(symbol: str, interval: str, outputsize: int = 100) -> List[Dict[str, Any]]:
    global RATE_LIMIT_UNTIL
    if not KEY:
        raise HTTPException(status_code=500, detail="TWELVE_DATA_API_KEY não configurada no Render.")
    if symbol not in SYMBOLS:
        raise HTTPException(status_code=400, detail="Ativo inválido.")
    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(status_code=400, detail="Timeframe inválido.")

    key = (symbol, interval)
    now = time.monotonic()
    cached = CANDLE_CACHE.get(key)
    if cached and now - cached[0] < CACHE_TTL_SECONDS:
        return cached[1]
    if now < RATE_LIMIT_UNTIL and cached:
        return cached[1]
    if now < RATE_LIMIT_UNTIL:
        remaining = max(1, int(RATE_LIMIT_UNTIL - now))
        raise HTTPException(status_code=429, detail={"error": "RATE_LIMIT", "retry_after": remaining})

    lock = CANDLE_LOCKS.setdefault(key, asyncio.Lock())
    async with lock:
        now = time.monotonic()
        cached = CANDLE_CACHE.get(key)
        if cached and now - cached[0] < CACHE_TTL_SECONDS:
            return cached[1]
        params = {"symbol": SYMBOLS[symbol], "interval": interval,
                  "outputsize": min(max(int(outputsize), 20), 500),
                  "timezone": "America/Sao_Paulo", "order": "ASC"}
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                response = await client.get(BASE_URL, params=params,
                                            headers={"Authorization": f"apikey {KEY}"})
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail="Não foi possível conectar à Twelve Data.") from exc
        if response.status_code == 429:
            RATE_LIMIT_UNTIL = time.monotonic() + 20
            raise HTTPException(status_code=429, detail={"error": "RATE_LIMIT", "retry_after": 20})
        if response.status_code >= 400:
            raise HTTPException(status_code=502, detail=f"Twelve Data retornou HTTP {response.status_code}.")
        try:
            data = response.json()
        except ValueError as exc:
            raise HTTPException(status_code=502, detail="Resposta inválida da Twelve Data.") from exc
        if data.get("status") == "error" or "values" not in data:
            raise HTTPException(status_code=502, detail=data.get("message", "Resposta inválida da Twelve Data."))
        candles = []
        for item in data["values"]:
            try:
                candles.append({"datetime": item["datetime"], "open": float(item["open"]),
                                "high": float(item["high"]), "low": float(item["low"]),
                                "close": float(item["close"]), "volume": float(item.get("volume", 0) or 0)})
            except (KeyError, TypeError, ValueError):
                continue
        candles.sort(key=lambda x: parse_time(x["datetime"]))
        if len(candles) < 20:
            raise HTTPException(status_code=502, detail="Poucas velas retornadas pela Twelve Data.")
        CANDLE_CACHE[key] = (time.monotonic(), candles)
        return candles


def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1.0)
    out = [values[0]]
    for value in values[1:]:
        out.append(value * alpha + out[-1] * (1.0 - alpha))
    return out


def rsi(values: List[float], period: int = 14) -> List[float]:
    if len(values) < period + 1:
        return [50.0] * len(values)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(values)):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0.0)); losses.append(max(-change, 0.0))
    out = [50.0] * len(values)
    ag = sum(gains[1:period + 1]) / period
    al = sum(losses[1:period + 1]) / period
    def calc(g, l):
        if l == 0: return 100.0 if g > 0 else 50.0
        return 100.0 - 100.0 / (1.0 + g / l)
    out[period] = calc(ag, al)
    for i in range(period + 1, len(values)):
        ag = ((ag * (period - 1)) + gains[i]) / period
        al = ((al * (period - 1)) + losses[i]) / period
        out[i] = calc(ag, al)
    return out


def true_ranges(candles):
    out = []
    for i, c in enumerate(candles):
        if i == 0: out.append(c["high"] - c["low"]); continue
        pc = candles[i - 1]["close"]
        out.append(max(c["high"] - c["low"], abs(c["high"] - pc), abs(c["low"] - pc)))
    return out


def body(c): return abs(c["close"] - c["open"])
def rng(c): return max(c["high"] - c["low"], 1e-12)


def analyze(candles, strategy="rsi"):
    if len(candles) < 30:
        raise HTTPException(status_code=422, detail="Dados insuficientes para análise.")
    closed = candles[:-1]
    reference, next_candle, previous = closed[-1], candles[-1], closed[-2]
    strategy = strategy.lower().strip()
    bullish = reference["close"] > reference["open"]
    bearish = reference["close"] < reference["open"]

    if strategy == "old_sniper":
        rb, rr = body(reference), rng(reference)
        lower = min(reference["open"], reference["close"]) - reference["low"]
        upper = reference["high"] - max(reference["open"], reference["close"])
        prev_bull = previous["close"] > previous["open"]
        prev_bear = previous["close"] < previous["open"]
        bull_engulf = bullish and prev_bear and reference["open"] <= previous["close"] and reference["close"] >= previous["open"]
        bear_engulf = bearish and prev_bull and reference["open"] >= previous["close"] and reference["close"] <= previous["open"]
        bull_rej = bullish and lower >= max(rb * 1.2, rr * .30)
        bear_rej = bearish and upper >= max(rb * 1.2, rr * .30)
        bull_break = reference["high"] > previous["high"] and reference["close"] > previous["high"]
        bear_break = reference["low"] < previous["low"] and reference["close"] < previous["low"]
        bull_strength = bullish and rb >= rr * .60 and upper <= rr * .20
        bear_strength = bearish and rb >= rr * .60 and lower <= rr * .20
        bull_score = sum([bull_engulf, bull_rej, bull_break, bull_strength, reference["close"] >= previous["close"]])
        bear_score = sum([bear_engulf, bear_rej, bear_break, bear_strength, reference["close"] <= previous["close"]])
        if bull_score >= 3 and bull_score > bear_score: signal, confidence = "CALL", min(95, 55 + bull_score * 7)
        elif bear_score >= 3 and bear_score > bull_score: signal, confidence = "PUT", min(95, 55 + bear_score * 7)
        else: signal, confidence = "NEUTRO", 50
        return {"signal": signal, "confidence": int(confidence), "strategy": "SNIPER 01", "strategy_code": strategy,
                "bull_score": bull_score, "bear_score": bear_score, "reference_candle": reference["datetime"],
                "next_candle": next_candle["datetime"], "non_repaint_reference": True, "ok": True}

    if strategy == "sniper_02":
        lookback = closed[-22:-4]
        resistance, support = max(c["high"] for c in lookback), min(c["low"] for c in lookback)
        call_setup = put_setup = False
        for j in range(max(2, len(closed) - 5), len(closed) - 2):
            br, retest, conf = closed[j], closed[j + 1], closed[j + 2]
            b, r = body(retest), rng(retest)
            low_wick = min(retest["open"], retest["close"]) - retest["low"]
            high_wick = retest["high"] - max(retest["open"], retest["close"])
            hammer = low_wick >= max(b * 2, r * .45) and high_wick <= r * .20
            star = high_wick >= max(b * 2, r * .45) and low_wick <= r * .20
            if br["close"] > resistance and retest["low"] <= resistance * 1.0015 and (hammer or (retest["close"] > retest["open"])) and conf["close"] > conf["open"] and conf["close"] > retest["high"]:
                call_setup = True
            if br["close"] < support and retest["high"] >= support * .9985 and (star or (retest["close"] < retest["open"])) and conf["close"] < conf["open"] and conf["close"] < retest["low"]:
                put_setup = True
        signal = "CALL" if call_setup and not put_setup else "PUT" if put_setup and not call_setup else "NEUTRO"
        return {"signal": signal, "confidence": 88 if signal != "NEUTRO" else 50, "strategy": "SNIPER 02", "strategy_code": strategy,
                "call_setup": call_setup, "put_setup": put_setup, "support": support, "resistance": resistance,
                "reference_candle": reference["datetime"], "next_candle": next_candle["datetime"], "non_repaint_reference": True, "ok": True}

    closes = [c["close"] for c in closed]
    r9, r14 = rsi(closes, 9)[-1], rsi(closes, 14)[-1]
    if strategy == "sniper_03":
        e20, e50 = ema(closes, 20)[-1], ema(closes, 50)[-1]
        up = e20 > e50 and reference["close"] > e20
        down = e20 < e50 and reference["close"] < e20
        low_wick = min(reference["open"], reference["close"]) - reference["low"]
        high_wick = reference["high"] - max(reference["open"], reference["close"])
        bull = up and (low_wick > body(reference) or reference["close"] >= previous["high"])
        bear = down and (high_wick > body(reference) or reference["close"] <= previous["low"])
        signal = "CALL" if bull else "PUT" if bear else "NEUTRO"
        return {"signal": signal, "confidence": 82 if signal != "NEUTRO" else 50, "strategy": "SNIPER 03", "strategy_code": strategy,
                "trend": "ALTA" if up else "BAIXA" if down else "NEUTRA", "reference_candle": reference["datetime"],
                "next_candle": next_candle["datetime"], "non_repaint_reference": True, "ok": True}

    signal = "CALL" if r9 > 50 and r14 > 50 else "PUT" if r9 < 50 and r14 < 50 else "NEUTRO"
    confidence = 50 if signal == "NEUTRO" else int(min(95, 55 + min(abs(r9 - 50), abs(r14 - 50)) * 1.8))
    return {"signal": signal, "confidence": confidence, "rsi9": round(r9, 2), "rsi14": round(r14, 2),
            "rsi_confluence": signal != "NEUTRO", "strategy": "SNIPER X", "strategy_code": "rsi",
            "reference_candle": reference["datetime"], "next_candle": next_candle["datetime"],
            "non_repaint_reference": True, "ok": True}


def detect_market_trend(candles):
    closed = candles[:-1]
    if len(closed) < 55: return {"trend": "NEUTRA", "trend_text": "Tendência neutra", "trend_score": 0}
    closes = [c["close"] for c in closed]
    e9, e20, e50 = ema(closes, 9), ema(closes, 20), ema(closes, 50)
    slope = e20[-1] - e20[-6]
    bull = int(e9[-1] > e20[-1]) + int(e20[-1] > e50[-1]) + int(closes[-1] > e50[-1]) + int(slope > 0)
    bear = int(e9[-1] < e20[-1]) + int(e20[-1] < e50[-1]) + int(closes[-1] < e50[-1]) + int(slope < 0)
    if bull >= 3 and bull > bear: return {"trend": "ALTA", "trend_text": "Tendência do gráfico é de alta", "trend_score": bull}
    if bear >= 3 and bear > bull: return {"trend": "BAIXA", "trend_text": "Tendência do gráfico é de baixa", "trend_score": bear}
    return {"trend": "NEUTRA", "trend_text": "Tendência neutra", "trend_score": max(bull, bear)}


def ai_feature_vector(candles, i):
    data = candles[:i + 1]
    closes = [c["close"] for c in data]; opens = [c["open"] for c in data]
    highs = [c["high"] for c in data]; lows = [c["low"] for c in data]
    vols = [c.get("volume", 0) or 0 for c in data]
    def ret(n): return closes[-1] / closes[-1-n] - 1 if len(closes) > n else 0.0
    ranges = [max(h-l, 1e-12) for h,l in zip(highs,lows)]
    bodies = [abs(c-o)/r for c,o,r in zip(closes,opens,ranges)]
    tr = true_ranges(data); atr = sum(tr[-14:]) / min(14, len(tr))
    e9, e20, e50 = ema(closes,9)[-1], ema(closes,20)[-1], ema(closes,50)[-1]
    r9, r14 = rsi(closes,9)[-1], rsi(closes,14)[-1]
    mean10 = sum(closes[-10:]) / min(10,len(closes)); vol10 = (sum((x-mean10)**2 for x in closes[-10:]) / min(10,len(closes))) ** .5 / max(mean10,1e-12)
    avg_vol = sum(vols[-20:]) / max(1,min(20,len(vols))); vol_ratio = vols[-1]/avg_vol if avg_vol else 1
    slope = (ema(closes,20)[-1]-ema(closes,20)[-6])/max(closes[-1],1e-12) if len(closes)>=6 else 0
    return [ret(1),ret(2),ret(3),ret(5),ret(10),(closes[-1]-e9)/closes[-1],(e9-e20)/closes[-1],(e20-e50)/closes[-1],slope,r9/100,r14/100,bodies[-1],(highs[-1]-max(opens[-1],closes[-1]))/ranges[-1],(min(opens[-1],closes[-1])-lows[-1])/ranges[-1],atr/closes[-1],vol10,vol_ratio]


def train_ai_ensemble(candles):
    if not SKLEARN_OK: return {"ready": False, "reason": "scikit-learn não instalado"}
    closed = candles[:-1]
    if len(closed) < 100: return {"ready": False, "reason": "Poucas velas para treinar a IA V3"}
    X, y = [], []
    for i in range(50, len(closed)-1):
        X.append(ai_feature_vector(closed, i)); y.append(1 if closed[i+1]["close"] > closed[i]["close"] else 0)
    if len(X) < 60 or len(set(y)) < 2: return {"ready": False, "reason": "Classes insuficientes para treinar a IA V3"}
    split = max(40, int(len(X)*.8)); split = min(split, len(X)-10)
    models = [("LOGISTIC", make_pipeline(StandardScaler(), LogisticRegression(max_iter=700, class_weight="balanced"))),
              ("RANDOM_FOREST", RandomForestClassifier(n_estimators=220,max_depth=7,min_samples_leaf=3,random_state=42,class_weight="balanced_subsample",n_jobs=-1)),
              ("GRADIENT_BOOSTING", HistGradientBoostingClassifier(max_iter=180,learning_rate=.04,max_leaf_nodes=15,l2_regularization=.8,random_state=42))]
    if XGBOOST_OK:
        models.append(("XGBOOST", XGBClassifier(n_estimators=240,max_depth=4,learning_rate=.035,subsample=.85,colsample_bytree=.85,min_child_weight=4,reg_lambda=2.5,reg_alpha=.15,objective="binary:logistic",eval_metric="logloss",random_state=42,n_jobs=2,tree_method="hist")))
    if LIGHTGBM_OK:
        models.append(("LIGHTGBM", LGBMClassifier(n_estimators=200,learning_rate=.035,num_leaves=15,max_depth=5,min_child_samples=8,reg_lambda=2,reg_alpha=.1,objective="binary",random_state=42,n_jobs=2,verbosity=-1)))
    trained, scores = [], []
    for name, model in models:
        try:
            model.fit(X[:split],y[:split]); pred=model.predict(X[split:]); scores.append(sum(int(a==b) for a,b in zip(pred,y[split:]))/len(y[split:])); trained.append((name,model))
        except Exception:
            continue
    if not trained: return {"ready": False, "reason": "Falha ao treinar os modelos"}
    return {"ready":True,"models":trained,"validation_accuracy":round(sum(scores)/len(scores)*100,1) if scores else 0,"samples":split,"validation_samples":len(X)-split,"data_candles":len(closed),"xgboost":XGBOOST_OK,"lightgbm":LIGHTGBM_OK,"training":"temporal_80_20"}


def predict_ai_ensemble(symbol, interval, candles):
    key=(symbol,interval); now=time.monotonic(); cached=AI_MODEL_CACHE.get(key)
    if not cached or now-cached[0] >= AI_MODEL_TTL_SECONDS:
        trained=train_ai_ensemble(candles); AI_MODEL_CACHE[key]=(now,trained)
    else: trained=cached[1]
    if not trained.get("ready"): return {"ready":False,"reason":trained.get("reason","IA indisponível")}
    closed=candles[:-1]; x=ai_feature_vector(closed,len(closed)-1); probs=[]; names=[]
    for name,model in trained["models"]:
        try: probs.append(float(model.predict_proba([x])[0][1])); names.append(name)
        except Exception: pass
    if not probs: return {"ready":False,"reason":"Nenhum modelo conseguiu gerar previsão"}
    weights=[1.25 if n in {"XGBOOST","LIGHTGBM"} else 1.0 for n in names]
    p_up=sum(p*w for p,w in zip(probs,weights))/sum(weights); p_down=1-p_up
    if p_up >= .62: signal="CALL"; probability=p_up
    elif p_up <= .38: signal="PUT"; probability=p_down
    else: signal="NEUTRO"; probability=max(p_up,p_down)
    confidence=50+abs(p_up-.5)*90 if signal=="NEUTRO" else 55+(probability-.5)*100
    agreement=sum((p>=.5)==(p_up>=.5) for p in probs)/len(probs)*100
    return {"ready":True,"signal":signal,"p_up":round(p_up*100,2),"p_down":round(p_down*100,2),"confidence":max(50,min(95,int(round(confidence)))),"agreement":round(agreement,1),"models":names,"validation_accuracy":trained["validation_accuracy"],"samples":trained["samples"],"validation_samples":trained["validation_samples"],"data_candles":trained["data_candles"],"training":trained["training"],"xgboost":trained["xgboost"],"lightgbm":trained["lightgbm"]}


def entry_times(interval):
    minutes=ALLOWED_INTERVALS[interval]; now=now_sp(); block=minutes*60; epoch=int(now.timestamp()); next_epoch=((epoch//block)+1)*block
    entry=datetime.fromtimestamp(next_epoch,tz=SP_TZ); return entry, entry+timedelta(minutes=minutes)


@app.get("/health")
async def health(): return {"ok":True,"app":APP_NAME,"version":APP_VERSION}

@app.get("/server-time")
async def server_time(): return {"brasilia":now_sp().isoformat(),"utc":datetime.utcnow().isoformat()+"+00:00"}

@app.get("/license")
async def license(): return {"ok":True,**license_status()}

@app.get("/candles")
async def candles(symbol="EUR/USD", interval="1min", size=120):
    require_active_license(); values=await get_candles(symbol,interval,int(size)); return {"ok":True,"source":"Twelve Data","symbol":symbol,"interval":interval,"values":values}

@app.get("/ai-analysis")
async def ai_analysis(symbol="EUR/USD", interval="1min"):
    require_active_license(); values=await get_candles(symbol,interval,500); trend=detect_market_trend(values)
    ml=await asyncio.to_thread(predict_ai_ensemble,symbol,interval,values)
    closed=values[:-1]; closes=[c["close"] for c in closed]; r9=rsi(closes,9)[-1]; r14=rsi(closes,14)[-1]
    if ml.get("ready"):
        signal=ml["signal"]; regime=trend["trend"]
        confidence=ml["confidence"]
        if regime=="ALTA" and signal=="PUT" or regime=="BAIXA" and signal=="CALL": confidence=max(50,confidence-8)
        quality="ALTA" if confidence>=78 and ml["agreement"]>=66 else "MÉDIA" if confidence>=65 else "BAIXA"
        analysis={"signal":signal,"confidence":confidence,"quality":quality,"regime":regime,"rsi9":round(r9,2),"rsi14":round(r14,2),"p_up":ml["p_up"],"p_down":ml["p_down"],"agreement":ml["agreement"],"validation_accuracy":ml["validation_accuracy"],"samples":ml["samples"],"models":ml["models"],"data_candles":ml["data_candles"],"training":ml["training"],"xgboost":ml["xgboost"],"lightgbm":ml["lightgbm"],"non_repaint":True,"ml":True,"reason":"Ensemble ML com filtro de regime."}
    else:
        e9,e20,e50=ema(closes,9)[-1],ema(closes,20)[-1],ema(closes,50)[-1]; bull=(e9>e20)+(e20>e50); bear=(e9<e20)+(e20<e50)
        signal="CALL" if bull==2 and r9>50 and r14>50 else "PUT" if bear==2 and r9<50 and r14<50 else "NEUTRO"
        analysis={"signal":signal,"confidence":60 if signal!="NEUTRO" else 50,"quality":"MÉDIA" if signal!="NEUTRO" else "BAIXA","regime":trend["trend"],"rsi9":round(r9,2),"rsi14":round(r14,2),"non_repaint":True,"ml":False,"reason":ml.get("reason","Fallback")}
    return {"ok":True,"source":"Twelve Data","symbol":symbol,"interval":interval,"analysis":analysis,"trend":trend}

@app.get("/signal")
async def signal(symbol="EUR/USD", interval="1min", strategy="rsi"):
    require_active_license()
    if strategy not in {"rsi","old_sniper","sniper_02","sniper_03","ai"}: raise HTTPException(status_code=400,detail="Estratégia inválida.")
    values=await get_candles(symbol,interval,500)
    if strategy=="ai":
        ml=await asyncio.to_thread(predict_ai_ensemble,symbol,interval,values); trend=detect_market_trend(values)
        result={"signal":ml.get("signal","NEUTRO"),"confidence":ml.get("confidence",50),"strategy":"SNIPER IA V3","strategy_code":"ai","p_up":ml.get("p_up",50),"p_down":ml.get("p_down",50),"agreement":ml.get("agreement",0),"models":ml.get("models",[]),"validation_accuracy":ml.get("validation_accuracy",0),"regime":trend["trend"],"non_repaint_reference":True,"ok":True}
    else:
        result=analyze(values,strategy); result.update(detect_market_trend(values))
    entry,expiry=entry_times(interval)
    result.update({"source":"Twelve Data","symbol":symbol,"interval":interval,"entry_time":entry.strftime("%Y-%m-%d %H:%M:%S"),"next_candle":entry.strftime("%Y-%m-%d %H:%M:%S"),"expiry_time":expiry.strftime("%Y-%m-%d %H:%M:%S"),"expiry":"1 vela do intervalo selecionado","warning":"Sinal probabilístico; não garante WIN."})
    return result

@app.get("/result")
async def result(symbol:str, interval:str, reference_candle:str, direction:str, entry_time:Optional[str]=None):
    require_active_license(); direction=direction.upper().strip()
    if direction not in {"CALL","PUT"}: raise HTTPException(status_code=400,detail="Direção inválida.")
    values=await get_candles(symbol,interval,100); ref=parse_time(reference_candle)
    idx=next((i for i,c in enumerate(values) if parse_time(c["datetime"])==ref),None)
    if idx is None: return {"ok":True,"status":"PENDING","result":None}
    if entry_time:
        entry=parse_time(entry_time); eidx=next((i for i,c in enumerate(values) if parse_time(c["datetime"])==entry),None)
        if eidx is None: return {"ok":True,"status":"PENDING","result":None}
        candle=values[eidx]
        if now_sp() < parse_time(candle["datetime"])+timedelta(minutes=ALLOWED_INTERVALS[interval]): return {"ok":True,"status":"PENDING","result":None}
        entry_price=candle["open"]; close=candle["close"]; result_time=candle["datetime"]
    else:
        ridx=idx+1
        if ridx>=len(values): return {"ok":True,"status":"PENDING","result":None}
        candle=values[ridx]
        if now_sp() < parse_time(candle["datetime"])+timedelta(minutes=ALLOWED_INTERVALS[interval]): return {"ok":True,"status":"PENDING","result":None}
        entry_price=values[idx]["close"]; close=candle["close"]; result_time=candle["datetime"]
    tol=max(abs(entry_price)*1e-10,1e-12)
    outcome="DRAW" if abs(close-entry_price)<=tol else ("WIN" if close>entry_price else "LOSS") if direction=="CALL" else ("WIN" if close<entry_price else "LOSS")
    return {"ok":True,"status":"CLOSED","result":outcome,"direction":direction,"reference_candle":reference_candle,"result_candle":result_time,"entry_close":entry_price,"result_close":close}

@app.get("/radar")
async def radar(interval="1min", strategy="rsi"):
    require_active_license()
    if interval not in ALLOWED_INTERVALS or strategy not in {"rsi","old_sniper","sniper_02","sniper_03","ai"}: raise HTTPException(status_code=400,detail="Parâmetros inválidos.")
    key=(interval,strategy); cached=RADAR_CACHE.get(key); now=time.monotonic()
    if cached and now-cached[0]<RADAR_TTL_SECONDS: return cached[1]
    results=[]
    for symbol in RADAR_SYMBOLS:
        try:
            vals=await get_candles(symbol,interval,240 if strategy=="ai" else 100)
            if strategy=="ai": a=await asyncio.to_thread(predict_ai_ensemble,symbol,interval,vals); signal_value=a.get("signal","NEUTRO"); confidence=a.get("confidence",50); up=a.get("p_up",50); down=a.get("p_down",50)
            else: a=analyze(vals,strategy); signal_value=a["signal"]; confidence=a["confidence"]; up=50 if signal_value!="CALL" else confidence; down=50 if signal_value!="PUT" else confidence
            direction="CALL" if up>=down and up>=58 else "PUT" if down>up and down>=58 else "NEUTRO"; proximity=int(max(up,down))
            results.append({"symbol":symbol,"signal":signal_value,"direction":direction,"proximity":min(99,proximity),"call_proximity":int(up),"put_proximity":int(down),"confidence":int(confidence),"reference_candle":a.get("reference_candle")})
        except Exception: continue
    results.sort(key=lambda x:(x["direction"]=="NEUTRO",-x["proximity"]))
    payload={"ok":True,"interval":interval,"strategy":strategy,"symbols_checked":len(results),"results":results,"warning":"Radar probabilístico; não garante WIN."}
    RADAR_CACHE[key]=(time.monotonic(),payload); return payload


@app.get("/", response_class=HTMLResponse)
async def home():
    return HTMLResponse(f'''<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{APP_NAME}</title><style>body{{margin:0;background:#0b1020;color:#fff;font-family:Arial}}.c{{max-width:760px;margin:auto;padding:18px}}.card{{background:#151c30;border:1px solid #29324b;border-radius:14px;padding:16px;margin:12px 0}}select,button{{padding:12px;border-radius:9px;background:#0f1526;color:#fff;border:1px solid #34405e}}select{{width:100%}}button{{cursor:pointer;font-weight:bold}}.row{{display:flex;gap:10px}}.row>*{{flex:1}}#signal{{font-size:42px;text-align:center;font-weight:800;padding:24px;border-radius:14px;margin-top:12px}}.call{{background:#103c2b;color:#52f09d}}.put{{background:#481d28;color:#ff718b}}.neutral{{background:#2b3040;color:#d9deeb}}.grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}}.stat{{background:#0f1526;padding:14px;text-align:center;border-radius:10px}}.stat b{{display:block;font-size:25px;margin-top:5px}}</style></head><body><div class="c"><h1>Ismael Trade</h1><div>SNIPER IA V3 • M1/M5/M15/M30</div><div class="card"><div class="row"><select id="symbol">{''.join(f'<option>{s}</option>' for s in SYMBOLS)}</select><select id="interval"><option value="1min">M1</option><option value="5min">M5</option><option value="15min">M15</option><option value="30min">M30</option></select></div><button style="width:100%;margin-top:10px" onclick="load()">ATUALIZAR SINAL</button></div><div class="card"><div id="signal" class="neutral">AGUARDANDO</div><p id="meta">--</p><p id="ai">IA: --</p></div><div class="card"><div class="grid"><div class="stat">WIN<b id="w">0</b></div><div class="stat">LOSS<b id="l">0</b></div><div class="stat">ASSERTIVIDADE<b id="a">0%</b></div></div></div><div class="card">Licença: {license_status()['expires']}<br>WhatsApp: {LICENSE_WHATSAPP_1} / {LICENSE_WHATSAPP_2}<br>Instagram: {LICENSE_INSTAGRAM}</div></div><script>async function load(){{const s=document.getElementById('symbol').value,i=document.getElementById('interval').value;const r=await fetch(`/signal?symbol=${{encodeURIComponent(s)}}&interval=${{i}}&strategy=ai`);const d=await r.json();const e=document.getElementById('signal');e.textContent=d.signal||'ERRO';e.className=d.signal==='CALL'?'call':d.signal==='PUT'?'put':'neutral';document.getElementById('meta').textContent=`Confiança: ${{d.confidence||0}}% | Entrada: ${{d.entry_time||'--'}} | Expiração: ${{d.expiry_time||'--'}}`;document.getElementById('ai').textContent=`IA: alta ${{d.p_up||50}}% | baixa ${{d.p_down||50}}% | concordância ${{d.agreement||0}}%`}}</script></body></html>''')


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
