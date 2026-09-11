import os
import asyncio
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

APP_NAME = "Trade sniper"
APP_VERSION = "9.1.0"
KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
BASE_URL = "https://api.twelvedata.com/time_series"
SP_TZ = ZoneInfo("America/Sao_Paulo")

LICENSE_EXPIRES = os.getenv("ISMAEL_TRADE_LICENSE_EXPIRES", "2026-12-31").strip()
LICENSE_WHATSAPP_1 = "55 84 99841-1282"
LICENSE_WHATSAPP_2 = "55 84 99449-9442"
LICENSE_INSTAGRAM = "@Ismaelartur26"

ALLOWED_INTERVALS = {"1min": 1, "5min": 5, "15min": 15, "30min": 30}
SYMBOLS = {
    "EUR/USD": "EUR/USD", "GBP/USD": "GBP/USD", "USD/JPY": "USD/JPY",
    "AUD/USD": "AUD/USD", "USD/CAD": "USD/CAD", "USD/CHF": "USD/CHF",
    "NZD/USD": "NZD/USD", "EUR/JPY": "EUR/JPY", "GBP/JPY": "GBP/JPY",
    "EUR/GBP": "EUR/GBP", "BTC/USD": "BTC/USD", "ETH/USD": "ETH/USD",
}
STRATEGIES = [
    ("rsi", "SNIPER X"),
    ("old_sniper", "SNIPER 01"),
    ("sniper_02", "SNIPER 02"),
    ("sniper_03", "SNIPER 03"),
]

app = FastAPI(title=APP_NAME, version=APP_VERSION)

CACHE_TTL_SECONDS = 60.0
RATE_LIMIT_COOLDOWN_SECONDS = 20.0
CANDLE_CACHE: Dict[tuple, tuple] = {}
CANDLE_LOCKS: Dict[tuple, asyncio.Lock] = {}
RATE_LIMIT_UNTIL = 0.0
RADAR_CACHE: Dict[tuple, tuple] = {}
RADAR_TTL_SECONDS = 120.0
RANKING_CACHE: Dict[tuple, tuple] = {}
RANKING_TTL_SECONDS = 120.0


def now_sp() -> datetime:
    return datetime.now(SP_TZ)


def license_status() -> Dict[str, Any]:
    try:
        expires = datetime.strptime(LICENSE_EXPIRES, "%Y-%m-%d").replace(tzinfo=SP_TZ)
    except ValueError:
        expires = datetime(2026, 12, 31, tzinfo=SP_TZ)
    expires = expires.replace(hour=23, minute=59, second=59)
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
        raise HTTPException(status_code=403, detail={"error": "LICENSE_EXPIRED", **status})


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


async def get_candles(symbol: str, interval: str, outputsize: int = 100) -> List[Dict[str, Any]]:
    global RATE_LIMIT_UNTIL
    if not KEY:
        raise HTTPException(status_code=500, detail="TWELVE_DATA_API_KEY não configurada no Render.")
    if symbol not in SYMBOLS:
        raise HTTPException(status_code=400, detail="Ativo inválido.")
    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(status_code=400, detail="Timeframe inválido.")

    requested = min(max(int(outputsize), 20), 240)
    key = (symbol, interval)
    now = time.monotonic()
    cached = CANDLE_CACHE.get(key)

    # O cache agora respeita o tamanho solicitado. Isso evita que o radar
    # com 100 velas impeça o ranking de 3 horas de obter 240 velas no M1.
    if cached and now - cached[0] < CACHE_TTL_SECONDS and len(cached[1]) >= requested:
        return cached[1]

    if now < RATE_LIMIT_UNTIL:
        if cached:
            return cached[1]
        remaining = max(1, int(RATE_LIMIT_UNTIL - now))
        raise HTTPException(status_code=429, detail={
            "error": "RATE_LIMIT",
            "message": f"Limite da Twelve Data atingido. Aguarde {remaining}s.",
            "retry_after": remaining,
        })

    lock = CANDLE_LOCKS.setdefault(key, asyncio.Lock())
    async with lock:
        now = time.monotonic()
        cached = CANDLE_CACHE.get(key)
        if cached and now - cached[0] < CACHE_TTL_SECONDS and len(cached[1]) >= requested:
            return cached[1]

        if now < RATE_LIMIT_UNTIL:
            if cached:
                return cached[1]
            remaining = max(1, int(RATE_LIMIT_UNTIL - now))
            raise HTTPException(status_code=429, detail={
                "error": "RATE_LIMIT",
                "message": f"Limite da Twelve Data atingido. Aguarde {remaining}s.",
                "retry_after": remaining,
            })

        params = {
            "symbol": SYMBOLS[symbol],
            "interval": interval,
            "outputsize": requested,
            "timezone": "America/Sao_Paulo",
            "order": "ASC",
        }

        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                response = await client.get(
                    BASE_URL,
                    params=params,
                    headers={"Authorization": f"apikey {KEY}"},
                )
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail="Não foi possível conectar à Twelve Data.") from exc

        if response.status_code == 429:
            retry_header = response.headers.get("Retry-After", "")
            try:
                retry_after = max(5, min(int(float(retry_header)), 120)) if retry_header else RATE_LIMIT_COOLDOWN_SECONDS
            except ValueError:
                retry_after = RATE_LIMIT_COOLDOWN_SECONDS
            RATE_LIMIT_UNTIL = time.monotonic() + retry_after
            raise HTTPException(status_code=429, detail={
                "error": "RATE_LIMIT",
                "message": f"A Twelve Data informou limite de requisições. Aguarde {int(retry_after)}s.",
                "retry_after": int(retry_after),
            })

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
                candles.append({
                    "datetime": item["datetime"],
                    "open": float(item["open"]),
                    "high": float(item["high"]),
                    "low": float(item["low"]),
                    "close": float(item["close"]),
                    "volume": float(item.get("volume", 0) or 0),
                })
            except (KeyError, TypeError, ValueError):
                continue

        candles.sort(key=lambda x: parse_time(x["datetime"]))
        if len(candles) < 20:
            raise HTTPException(status_code=502, detail="Poucas velas retornadas pela Twelve Data.")

        CANDLE_CACHE[key] = (time.monotonic(), candles)
        return candles


def ema(values, period):
    if not values:
        return []
    alpha = 2.0 / (period + 1.0)
    result = [values[0]]
    for value in values[1:]:
        result.append(value * alpha + result[-1] * (1.0 - alpha))
    return result


def rsi(values, period=14):
    if len(values) < period + 1:
        return [50.0] * len(values)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(values)):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))

    result = [50.0] * len(values)
    avg_gain = sum(gains[1:period + 1]) / period
    avg_loss = sum(losses[1:period + 1]) / period

    def calc(g, l):
        if l == 0:
            return 100.0 if g > 0 else 50.0
        return 100.0 - 100.0 / (1.0 + g / l)

    result[period] = calc(avg_gain, avg_loss)
    for i in range(period + 1, len(values)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        result[i] = calc(avg_gain, avg_loss)
    return result


def true_ranges(candles):
    result = []
    for i, c in enumerate(candles):
        if i == 0:
            result.append(c["high"] - c["low"])
        else:
            pc = candles[i - 1]["close"]
            result.append(max(c["high"] - c["low"], abs(c["high"] - pc), abs(c["low"] - pc)))
    return result


def analyze(candles, strategy="rsi"):
    if len(candles) < 30:
        raise HTTPException(status_code=422, detail="Dados insuficientes para análise.")

    closed = candles[:-1]
    reference = closed[-1]
    previous = closed[-2]
    next_candle = candles[-1]
    strategy = strategy.lower().strip()

    def body(c):
        return abs(c["close"] - c["open"])

    def rng(c):
        return max(c["high"] - c["low"], 1e-12)

    if strategy == "old_sniper":
        rb, rr = body(reference), rng(reference)
        upper = reference["high"] - max(reference["open"], reference["close"])
        lower = min(reference["open"], reference["close"]) - reference["low"]
        bullish = reference["close"] > reference["open"]
        bearish = reference["close"] < reference["open"]
        prev_bearish = previous["close"] < previous["open"]
        prev_bullish = previous["close"] > previous["open"]

        bull_engulf = bullish and prev_bearish and reference["open"] <= previous["close"] and reference["close"] >= previous["open"]
        bear_engulf = bearish and prev_bullish and reference["open"] >= previous["close"] and reference["close"] <= previous["open"]
        bull_rej = bullish and lower >= max(rb * 1.2, rr * .30)
        bear_rej = bearish and upper >= max(rb * 1.2, rr * .30)
        bull_break = reference["high"] > previous["high"] and reference["close"] > previous["high"]
        bear_break = reference["low"] < previous["low"] and reference["close"] < previous["low"]
        bull_strength = bullish and rb >= rr * .60 and upper <= rr * .20
        bear_strength = bearish and rb >= rr * .60 and lower <= rr * .20
        bull_structure = bullish and reference["close"] >= previous["close"]
        bear_structure = bearish and reference["close"] <= previous["close"]

        bull_score = sum([bull_engulf, bull_rej, bull_break, bull_strength, bull_structure])
        bear_score = sum([bear_engulf, bear_rej, bear_break, bear_strength, bear_structure])

        if bull_score >= 3 and bull_score > bear_score:
            signal, confidence = "CALL", min(95, 55 + bull_score * 7)
        elif bear_score >= 3 and bear_score > bull_score:
            signal, confidence = "PUT", min(95, 55 + bear_score * 7)
        else:
            signal, confidence = "NEUTRO", 50

        return {
            "signal": signal, "confidence": int(confidence),
            "reference_candle": reference["datetime"], "next_candle": next_candle["datetime"],
            "strategy": "SNIPER 01", "strategy_code": "old_sniper",
            "bull_score": bull_score, "bear_score": bear_score,
            "bullish_engulfing": bull_engulf, "bearish_engulfing": bear_engulf,
            "bullish_rejection": bull_rej, "bearish_rejection": bear_rej,
            "bullish_breakout": bull_break, "bearish_breakout": bear_break,
            "bullish_strength": bull_strength, "bearish_strength": bear_strength,
            "structure_confirmation": bull_structure if signal == "CALL" else bear_structure if signal == "PUT" else False,
            "non_repaint_reference": True, "ok": True,
        }

    if strategy == "sniper_02":
        if len(closed) < 25:
            raise HTTPException(status_code=422, detail="Dados insuficientes para SNIPER 02.")

        lookback = closed[-22:-4]
        resistance = max(c["high"] for c in lookback)
        support = min(c["low"] for c in lookback)

        def bullish_pin(c):
            b, r = body(c), rng(c)
            lo = min(c["open"], c["close"]) - c["low"]
            up = c["high"] - max(c["open"], c["close"])
            return c["close"] > c["open"] and lo >= max(b * 1.5, r * .40) and up <= r * .25

        def bearish_pin(c):
            b, r = body(c), rng(c)
            up = c["high"] - max(c["open"], c["close"])
            lo = min(c["open"], c["close"]) - c["low"]
            return c["close"] < c["open"] and up >= max(b * 1.5, r * .40) and lo <= r * .25

        def hammer(c):
            b, r = body(c), rng(c)
            lo = min(c["open"], c["close"]) - c["low"]
            up = c["high"] - max(c["open"], c["close"])
            return lo >= max(b * 2, r * .45) and up <= r * .20

        def shooting_star(c):
            b, r = body(c), rng(c)
            up = c["high"] - max(c["open"], c["close"])
            lo = min(c["open"], c["close"]) - c["low"]
            return up >= max(b * 2, r * .45) and lo <= r * .20

        def bull_engulf(a, b):
            return b["close"] > b["open"] and a["close"] < a["open"] and b["open"] <= a["close"] and b["close"] >= a["open"]

        def bear_engulf(a, b):
            return b["close"] < b["open"] and a["close"] > a["open"] and b["open"] >= a["close"] and b["close"] <= a["open"]

        call_setup = put_setup = False
        call_pattern = put_pattern = ""

        for j in range(max(2, len(closed) - 5), len(closed) - 2):
            breakout = closed[j]
            retest = closed[j + 1]
            confirmation = closed[j + 2]

            if breakout["close"] > resistance:
                touched = retest["low"] <= resistance * 1.0015
                rejection = touched and (hammer(retest) or bullish_pin(retest) or bull_engulf(breakout, retest))
                confirmed = confirmation["close"] > confirmation["open"] and confirmation["close"] > retest["high"]
                if rejection and confirmed:
                    call_setup = True
                    call_pattern = "Martelo" if hammer(retest) else "Pin Bar de alta" if bullish_pin(retest) else "Engolfo de alta"

            if breakout["close"] < support:
                touched = retest["high"] >= support * .9985
                rejection = touched and (shooting_star(retest) or bearish_pin(retest) or bear_engulf(breakout, retest))
                confirmed = confirmation["close"] < confirmation["open"] and confirmation["close"] < retest["low"]
                if rejection and confirmed:
                    put_setup = True
                    put_pattern = "Shooting Star" if shooting_star(retest) else "Pin Bar de baixa" if bearish_pin(retest) else "Engolfo de baixa"

        if call_setup and not put_setup:
            signal, confidence = "CALL", 88
        elif put_setup and not call_setup:
            signal, confidence = "PUT", 88
        else:
            signal, confidence = "NEUTRO", 50

        return {
            "signal": signal, "confidence": confidence,
            "reference_candle": reference["datetime"], "next_candle": next_candle["datetime"],
            "strategy": "SNIPER 02", "strategy_code": "sniper_02",
            "resistance": resistance, "support": support,
            "call_setup": call_setup, "put_setup": put_setup,
            "call_pattern": call_pattern, "put_pattern": put_pattern,
            "no_first_breakout": True, "non_repaint_reference": True, "ok": True,
        }

    if strategy == "sniper_03":
        if len(closed) < 45:
            raise HTTPException(status_code=422, detail="Dados insuficientes para SNIPER 03.")

        closes = [c["close"] for c in closed]
        highs = [c["high"] for c in closed]
        lows = [c["low"] for c in closed]
        ema20, ema50 = ema(closes, 20), ema(closes, 50)
        trv = true_ranges(closed)
        atr = sum(trv[-14:]) / 14
        tol = max(atr * .35, abs(reference["close"]) * .0005)

        def pl(i):
            return i >= 2 and i + 2 < len(closed) and lows[i] <= lows[i-1] and lows[i] <= lows[i-2] and lows[i] < lows[i+1] and lows[i] < lows[i+2]

        def ph(i):
            return i >= 2 and i + 2 < len(closed) and highs[i] >= highs[i-1] and highs[i] >= highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]

        lp = [i for i in range(max(2, len(closed)-35), len(closed)-2) if pl(i)]
        hp = [i for i in range(max(2, len(closed)-35), len(closed)-2) if ph(i)]

        lta = ltb = None
        lta_valid = ltb_valid = False

        if len(lp) >= 2:
            a, b = lp[-2], lp[-1]
            if lows[b] > lows[a]:
                slope = (lows[b] - lows[a]) / (b - a)
                lta = lows[b] + slope * (len(closed)-1-b)
                lta_valid = slope > 0 and lta <= reference["high"] + tol

        if len(hp) >= 2:
            a, b = hp[-2], hp[-1]
            if highs[b] < highs[a]:
                slope = (highs[b] - highs[a]) / (b - a)
                ltb = highs[b] + slope * (len(closed)-1-b)
                ltb_valid = slope < 0 and ltb >= reference["low"] - tol

        support = min((lows[i] for i in lp[-5:]), default=None)
        resistance = max((highs[i] for i in hp[-5:]), default=None)

        def zone_score(level, side):
            if level is None:
                return 0
            touches = rejects = 0
            for c in closed[max(0, len(closed)-40):-1]:
                if c["low"] <= level + tol and c["high"] >= level - tol:
                    touches += 1
                    if side == "support" and (c["close"] > c["open"] or min(c["open"], c["close"])-c["low"] > body(c)):
                        rejects += 1
                    if side == "resistance" and (c["close"] < c["open"] or c["high"]-max(c["open"], c["close"]) > body(c)):
                        rejects += 1
            return touches + min(rejects, 3)

        strong_support = support is not None and zone_score(support, "support") >= 4
        strong_resistance = resistance is not None and zone_score(resistance, "resistance") >= 4

        rb, rr = body(reference), rng(reference)
        lw = min(reference["open"], reference["close"]) - reference["low"]
        uw = reference["high"] - max(reference["open"], reference["close"])
        bull_rej = reference["close"] > reference["open"] and lw >= max(rb * 1.2, rr * .30)
        bear_rej = reference["close"] < reference["open"] and uw >= max(rb * 1.2, rr * .30)

        near_lta = lta is not None and abs(reference["low"] - lta) <= tol
        near_ltb = ltb is not None and abs(reference["high"] - ltb) <= tol
        near_sup = support is not None and abs(reference["low"] - support) <= tol
        near_res = resistance is not None and abs(reference["high"] - resistance) <= tol

        prior = closed[-6:-1]
        up = ema20[-1] > ema50[-1] and ema20[-1] > ema20[-4] and reference["close"] > ema50[-1]
        down = ema20[-1] < ema50[-1] and ema20[-1] < ema20[-4] and reference["close"] < ema50[-1]

        pull_call = any(c["close"] < c["open"] for c in prior[-3:]) and (near_lta or near_sup or abs(reference["low"]-ema20[-1]) <= tol)
        pull_put = any(c["close"] > c["open"] for c in prior[-3:]) and (near_ltb or near_res or abs(reference["high"]-ema20[-1]) <= tol)
        conf_call = reference["close"] > reference["open"] and reference["close"] >= previous["high"]
        conf_put = reference["close"] < reference["open"] and reference["close"] <= previous["low"]

        call_score = (2 if up else 0)+(2 if lta_valid else 0)+(2 if strong_support else 0)+(2 if pull_call else 0)+(1 if bull_rej else 0)+(2 if conf_call else 0)+(1 if near_lta else 0)+(1 if near_sup else 0)
        put_score = (2 if down else 0)+(2 if ltb_valid else 0)+(2 if strong_resistance else 0)+(2 if pull_put else 0)+(1 if bear_rej else 0)+(2 if conf_put else 0)+(1 if near_ltb else 0)+(1 if near_res else 0)

        if call_score >= 8 and call_score > put_score:
            signal, confidence = "CALL", min(95, 55 + call_score * 4)
        elif put_score >= 8 and put_score > call_score:
            signal, confidence = "PUT", min(95, 55 + put_score * 4)
        else:
            signal, confidence = "NEUTRO", 50

        return {
            "signal": signal, "confidence": confidence,
            "reference_candle": reference["datetime"], "next_candle": next_candle["datetime"],
            "strategy": "SNIPER 03", "strategy_code": "sniper_03",
            "trend": "ALTA" if up else "BAIXA" if down else "NEUTRA",
            "lta": round(lta, 8) if lta is not None else None,
            "ltb": round(ltb, 8) if ltb is not None else None,
            "support": round(support, 8) if support is not None else None,
            "resistance": round(resistance, 8) if resistance is not None else None,
            "strong_support": strong_support, "strong_resistance": strong_resistance,
            "near_lta": near_lta, "near_ltb": near_ltb, "near_support": near_sup, "near_resistance": near_res,
            "pullback_call": pull_call, "pullback_put": pull_put,
            "bullish_rejection": bull_rej, "bearish_rejection": bear_rej,
            "call_score": call_score, "put_score": put_score,
            "non_repaint_reference": True, "ok": True,
        }

    closes = [float(c["close"]) for c in closed]
    rv9, rv14 = rsi(closes, 9), rsi(closes, 14)
    i = len(closed) - 1
    r9, r14 = rv9[i], rv14[i]

    if r9 > 50 and r14 > 50:
        signal = "CALL"
    elif r9 < 50 and r14 < 50:
        signal = "PUT"
    else:
        signal = "NEUTRO"

    confidence = 50 if signal == "NEUTRO" else int(min(95, 55 + min(abs(r9-50), abs(r14-50)) * 1.8))

    return {
        "signal": signal, "confidence": confidence,
        "reference_candle": reference["datetime"], "next_candle": next_candle["datetime"],
        "rsi9": round(r9, 2), "rsi14": round(r14, 2),
        "rsi_confluence": signal != "NEUTRO",
        "strategy": "SNIPER X", "strategy_code": "rsi",
        "non_repaint_reference": True, "ok": True,
    }


def radar_score(analysis, strategy):
    if strategy == "rsi":
        r9, r14 = float(analysis.get("rsi9", 50)), float(analysis.get("rsi14", 50))
        bull = max(0, min(100, 50 + (r9 - 50 + r14 - 50) * 1.5))
        bear = max(0, min(100, 50 + (50 - r9 + 50 - r14) * 1.5))
    elif strategy == "old_sniper":
        bull = min(100, 50 + float(analysis.get("bull_score", 0)) * 10)
        bear = min(100, 50 + float(analysis.get("bear_score", 0)) * 10)
    elif strategy == "sniper_02":
        bull = 90 if analysis.get("call_setup") else 50
        bear = 90 if analysis.get("put_setup") else 50
    else:
        bull = min(99, 50 + float(analysis.get("call_score", 0)) * 5)
        bear = min(99, 50 + float(analysis.get("put_score", 0)) * 5)

    if bull >= bear and bull >= 58:
        direction, proximity = "CALL", round(bull)
    elif bear > bull and bear >= 58:
        direction, proximity = "PUT", round(bear)
    else:
        direction, proximity = "NEUTRO", round(max(bull, bear))

    return {
        "direction": direction,
        "proximity": max(0, min(99, proximity)),
        "call_proximity": round(bull),
        "put_proximity": round(bear),
    }


def candle_is_closed(candle_time, interval):
    return now_sp() >= candle_time + timedelta(minutes=ALLOWED_INTERVALS[interval])


def result_from_prices(direction, entry_close, result_close):
    tolerance = max(abs(entry_close) * 1e-10, 1e-12)
    if abs(result_close - entry_close) <= tolerance:
        return "DRAW"
    if direction == "CALL":
        return "WIN" if result_close > entry_close else "LOSS"
    return "WIN" if result_close < entry_close else "LOSS"


@app.get("/radar")
async def radar(interval="1min", strategy="rsi"):
    require_active_license()
    if interval not in ALLOWED_INTERVALS or strategy not in dict(STRATEGIES):
        raise HTTPException(status_code=400, detail="Timeframe ou estratégia inválida.")

    cache_key = (interval, strategy)
    cached = RADAR_CACHE.get(cache_key)
    if cached and time.monotonic() - cached[0] < RADAR_TTL_SECONDS:
        return cached[1]

    results, errors = [], 0
    for symbol_name in ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD", "USD/CHF"]:
        try:
            values = await get_candles(symbol_name, interval, 100)
            a = analyze(values, strategy)
            p = radar_score(a, strategy)
            results.append({
                "symbol": symbol_name, "signal": a["signal"], "direction": p["direction"],
                "proximity": p["proximity"], "call_proximity": p["call_proximity"],
                "put_proximity": p["put_proximity"], "confidence": int(a.get("confidence", 50)),
                "reference_candle": a.get("reference_candle"),
            })
        except Exception:
            errors += 1

    results.sort(key=lambda x: (x["direction"] == "NEUTRO", -x["proximity"]))
    payload = {
        "ok": True, "interval": interval, "strategy": strategy,
        "symbols_checked": len(results), "errors": errors, "results": results,
        "warning": "Radar probabilístico: proximidade não é garantia de sinal ou WIN.",
    }
    RADAR_CACHE[cache_key] = (time.monotonic(), payload)
    return payload


@app.get("/sniper-ranking")
async def sniper_ranking(symbol="EUR/USD", interval="1min", hours=3):
    require_active_license()
    if symbol not in SYMBOLS:
        raise HTTPException(status_code=400, detail="Ativo inválido.")
    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(status_code=400, detail="Timeframe inválido.")

    # Mantido em 3 horas conforme solicitado.
    hours = 3
    cache_key = (symbol, interval, hours)
    cached = RANKING_CACHE.get(cache_key)
    if cached and time.monotonic() - cached[0] < RANKING_TTL_SECONDS:
        return cached[1]

    # M1: 180 candles de 3h + margem de aquecimento.
    values = await get_candles(symbol, interval, 240)
    cutoff = now_sp() - timedelta(hours=3)
    eligible = []

    # Cada teste usa somente candles que já existiam naquele momento.
    # values[:i+2] contém a referência i e a vela seguinte usada no resultado.
    for i in range(50, len(values) - 1):
        ref_dt = parse_time(values[i]["datetime"])
        next_dt = parse_time(values[i + 1]["datetime"])
        if ref_dt < cutoff:
            continue
        if not candle_is_closed(next_dt, interval):
            continue
        eligible.append(i)

    ranking = []
    for code, name in STRATEGIES:
        wins = losses = draws = signals = errors = 0

        for i in eligible:
            try:
                historical = values[:i + 2]
                analysis = analyze(historical, code)
                direction = analysis.get("signal", "NEUTRO")

                if direction not in ("CALL", "PUT"):
                    continue

                signals += 1
                outcome = result_from_prices(
                    direction,
                    values[i]["close"],
                    values[i + 1]["close"],
                )

                if outcome == "WIN":
                    wins += 1
                elif outcome == "LOSS":
                    losses += 1
                else:
                    draws += 1
            except Exception:
                errors += 1

        resolved = wins + losses
        accuracy = wins / resolved * 100 if resolved else 0

        ranking.append({
            "strategy_code": code,
            "strategy": name,
            "wins": wins,
            "losses": losses,
            "draws": draws,
            "signals": signals,
            "resolved": resolved,
            "accuracy": round(accuracy, 1),
            "errors": errors,
        })

    ranking.sort(
        key=lambda x: (
            x["resolved"] > 0,
            x["accuracy"],
            x["wins"],
            x["signals"],
        ),
        reverse=True,
    )

    winner = next((x for x in ranking if x["resolved"] > 0), None)

    payload = {
        "ok": True,
        "symbol": symbol,
        "interval": interval,
        "hours": 3,
        "period_label": "ÚLTIMAS 3 HORAS",
        "candles_checked": len(values),
        "signals_checked": sum(x["signals"] for x in ranking),
        "winner": winner,
        "ranking": ranking,
        "updated_at": now_sp().strftime("%Y-%m-%d %H:%M:%S"),
        "warning": "O ranking é histórico e não garante o próximo resultado. A cotação da corretora pode diferir da Twelve Data.",
    }
    RANKING_CACHE[cache_key] = (time.monotonic(), payload)
    return payload


@app.get("/health")
async def health():
    return {"ok": True, "app": APP_NAME, "version": APP_VERSION}


@app.get("/server-time")
async def server_time():
    current = now_sp()
    return {
        "brasilia": current.isoformat(),
        "utc": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/license")
async def license():
    return {"ok": True, **license_status()}


@app.get("/candles")
async def candles(symbol="EUR/USD", interval="1min", size=120):
    require_active_license()
    values = await get_candles(symbol, interval, int(size))
    return {"ok": True, "source": "Twelve Data", "symbol": symbol, "interval": interval, "values": values}


@app.get("/signal")
async def signal(symbol="EUR/USD", interval="1min", strategy="rsi"):
    require_active_license()
    if strategy not in dict(STRATEGIES):
        raise HTTPException(status_code=400, detail="Estratégia inválida.")

    values = await get_candles(symbol, interval, 100)
    result = analyze(values, strategy)

    minutes = ALLOWED_INTERVALS[interval]
    current = now_sp()
    block = minutes * 60
    next_epoch = ((int(current.timestamp()) // block) + 1) * block
    entry_dt = datetime.fromtimestamp(next_epoch, tz=SP_TZ)
    expiry_dt = entry_dt + timedelta(minutes=minutes)

    result.update({
        "source": "Twelve Data",
        "symbol": symbol,
        "interval": interval,
        "entry_time": entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "next_candle": entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "expiry_time": expiry_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "expiry": "1 vela do intervalo selecionado",
        "warning": "Sinal probabilístico; não garante WIN.",
    })
    return result


@app.get("/result")
async def result(
    symbol: str,
    interval: str,
    reference_candle: str,
    direction: str,
    entry_time: Optional[str] = None,
):
    require_active_license()
    direction = direction.upper().strip()
    if direction not in {"CALL", "PUT"}:
        raise HTTPException(status_code=400, detail="Direção inválida.")

    values = await get_candles(symbol, interval, 100)
    ref = parse_time(reference_candle)
    ref_index = next(
        (i for i, c in enumerate(values) if parse_time(c["datetime"]) == ref),
        None,
    )

    if ref_index is None:
        return {"ok": True, "status": "PENDING", "result": None}

    if entry_time:
        entry_dt = parse_time(entry_time)
        entry_index = next(
            (i for i, c in enumerate(values) if parse_time(c["datetime"]) == entry_dt),
            None,
        )
        if entry_index is None:
            return {"ok": True, "status": "PENDING", "result": None}

        result_candle = values[entry_index]
        if not candle_is_closed(parse_time(result_candle["datetime"]), interval):
            return {"ok": True, "status": "PENDING", "result": None}

        entry_close = result_candle["open"]
        result_close = result_candle["close"]
    else:
        ni = ref_index + 1
        if ni >= len(values):
            return {"ok": True, "status": "PENDING", "result": None}

        result_candle = values[ni]
        if not candle_is_closed(parse_time(result_candle["datetime"]), interval):
            return {"ok": True, "status": "PENDING", "result": None}

        entry_close = values[ref_index]["close"]
        result_close = result_candle["close"]

    outcome = result_from_prices(direction, entry_close, result_close)
    return {
        "ok": True,
        "status": "CLOSED",
        "result": outcome,
        "direction": direction,
        "reference_candle": reference_candle,
        "result_candle": result_candle["datetime"],
        "entry_close": entry_close,
        "result_close": result_close,
    }


@app.get("/", response_class=HTMLResponse)
async def home():
    html = r'''<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>📉 SNIPER</title>
<style>
*{box-sizing:border-box}
body{margin:0;font-family:Arial,sans-serif;background:#0b1020;color:#f5f7ff}
.container{max-width:760px;margin:auto;padding:18px}
h1{margin:0 0 4px;font-size:28px}.sub{color:#aeb7cc;margin-bottom:16px}
.card{background:#151c30;border:1px solid #29324b;border-radius:16px;padding:16px;margin:12px 0}
.row{display:flex;gap:10px;flex-wrap:wrap}
label{display:block;color:#aeb7cc;font-size:13px;margin-bottom:6px}
select,button{width:100%;padding:12px;border-radius:10px;border:1px solid #34405e;background:#0f1526;color:#fff}
.field{flex:1;min-width:180px}button{cursor:pointer;font-weight:bold}
.signal{text-align:center;padding:22px;border-radius:14px;font-size:38px;font-weight:800;margin-top:12px}
.call{background:#103c2b;color:#52f09d}.put{background:#481d28;color:#ff718b}.neutral{background:#2b3040;color:#d9deeb}
.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}
.stat{background:#0f1526;border-radius:12px;padding:14px;text-align:center}
.stat b{display:block;font-size:25px;margin-top:4px}
.small{font-size:12px;color:#aeb7cc}.value{font-weight:bold}
.good{color:#52f09d}.bad{color:#ff718b}
.footer{font-size:12px;color:#7f8aa5;line-height:1.5}
.clock{font-size:30px;font-weight:800;letter-spacing:1px}
.clockLabel{font-size:11px;color:#8f9ab2;text-transform:uppercase}
.onlineBtn{border-radius:999px;padding:10px 16px;font-weight:800}
.onlineBtn.online{background:#0d3b2a;border-color:#2ee88a;color:#52f09d}
.onlineBtn.offline{background:#431b25;border-color:#ff718b;color:#ff718b}
.badge{display:inline-block;padding:7px 11px;border-radius:999px;background:#0f1526;border:1px solid #34405e;font-size:12px;font-weight:800}
.sniper{border:1px solid #394765;background:linear-gradient(135deg,#151c30,#10172a)}
.sniperTitle{font-size:20px;font-weight:800;margin:4px 0}
.countdown{font-size:25px;font-weight:800}
.entry{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:12px}
.entryBox{background:#0f1526;border-radius:12px;padding:12px}
.radarRow{display:grid;grid-template-columns:110px 1fr 48px;gap:10px;align-items:center;background:#0f1526;border-radius:12px;padding:10px;margin:7px 0}
.radarMeter{height:9px;background:#252c40;border-radius:999px;overflow:hidden}
.radarFill{height:100%;border-radius:999px}
.radarFill.call{background:#2ee88a}.radarFill.put{background:#ff718b}.radarFill.neutral{background:#8892a8}
.rankWinner{background:#0d3b2a;border:1px solid #2ee88a;border-radius:14px;padding:14px;margin-top:12px}
.rankTable{display:grid;gap:7px;margin-top:12px}
.rankRow{display:grid;grid-template-columns:1.15fr .55fr .55fr .7fr .7fr;gap:7px;align-items:center;background:#0f1526;border-radius:10px;padding:10px;font-size:13px}
.rankHead{color:#8f9ab2;font-size:11px;text-transform:uppercase}
.rankPos{font-weight:800}.rankBar{height:7px;background:#252c40;border-radius:99px;overflow:hidden}
.rankBar>div{height:100%;background:#2ee88a}
@media(max-width:520px){
.entry{grid-template-columns:1fr}.grid{grid-template-columns:1fr 1fr}
.rankRow{grid-template-columns:1.2fr .55fr .55fr .75fr}.rankSignals{display:none}
}
</style>
</head>
<body>
<div class="container">
<h1>📉 SNIPER</h1>
<div class="sub">ANÁLISE EM TEMPO REAL • HORÁRIO DE BRASÍLIA</div>

<div class="card" style="display:flex;justify-content:space-between;align-items:center;gap:12px">
<div><div class="clockLabel">HORÁRIO DE BRASÍLIA</div><div id="clock" class="clock">--:--:--</div><div id="dateBr" class="small">--/--/----</div></div>
<div class="badge">● MERCADO • MONITORANDO</div>
</div>

<div class="card">
<div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap">
<div><div class="small">LICENÇA</div><div id="licenseStatus" class="value">VERIFICANDO...</div></div>
<div><div class="small">VALIDADE</div><div id="licenseExpires" class="value">--</div></div>
</div>
<div class="small" style="margin-top:10px">Renovação: WhatsApp <b>55 84 99841-1282</b> / <b>55 84 99449-9442</b> • Instagram <b>@Ismaelartur26</b></div>
</div>

<div class="card">
<div class="row">
<div class="field"><label>ATIVO</label><select id="symbol">
<option>EUR/USD</option><option>GBP/USD</option><option>USD/JPY</option>
<option>AUD/USD</option><option>USD/CAD</option><option>USD/CHF</option>
<option>NZD/USD</option><option>EUR/JPY</option><option>GBP/JPY</option>
<option>EUR/GBP</option><option>BTC/USD</option><option>ETH/USD</option>
</select></div>
<div class="field"><label>TEMPO</label><select id="interval">
<option value="1min">M1</option><option value="5min">M5</option>
<option value="15min">M15</option><option value="30min">M30</option>
</select></div>
</div>
<button id="refresh" style="margin-top:10px">ATUALIZAR SINAL</button>
</div>

<div class="card sniper"><div style="display:flex;justify-content:space-between;align-items:center"><div class="sniperTitle">SNIPER X</div><button id="rsiToggle" class="onlineBtn online" style="width:auto">● ONLINE</button></div></div>
<div class="card sniper"><div style="display:flex;justify-content:space-between;align-items:center"><div class="sniperTitle">SNIPER 01</div><button id="oldToggle" class="onlineBtn offline" style="width:auto">● OFFLINE</button></div></div>
<div class="card sniper"><div style="display:flex;justify-content:space-between;align-items:center"><div class="sniperTitle">SNIPER 02</div><button id="sniper02Toggle" class="onlineBtn offline" style="width:auto">● OFFLINE</button></div></div>
<div class="card sniper"><div style="display:flex;justify-content:space-between;align-items:center"><div class="sniperTitle">SNIPER 03</div><button id="sniper03Toggle" class="onlineBtn offline" style="width:auto">● OFFLINE</button></div></div>

<div class="card">
<div class="small">SINAL</div>
<div id="signal" class="signal neutral">AGUARDANDO</div>
<div id="signalError" class="small" style="display:none;margin-top:8px"></div>
<div class="entry">
<div class="entryBox"><div class="small">HORÁRIO DE ENTRADA</div><div id="entryTime" class="value">--</div></div>
<div class="entryBox"><div class="small">EXPIRAÇÃO</div><div id="expiry" class="value">--</div></div>
</div>
<div style="margin-top:12px"><div class="small">CRONÔMETRO DE ENTRADA / PRÓXIMA VELA</div><div id="countdown" class="countdown">--:--</div></div>
<div class="row" style="margin-top:12px">
<div class="field"><div class="small">Confiança</div><div id="confidence" class="value">--</div></div>
<div class="field"><div class="small">Referência</div><div id="reference" class="value">--</div></div>
<div class="field"><div class="small">Próxima vela</div><div id="next" class="value">--</div></div>
</div>
</div>

<div class="card">
<div class="small">RESULTADOS DO SINAL ATIVO</div>
<div class="grid">
<div class="stat"><span>WIN</span><b id="wins" class="good">0</b></div>
<div class="stat"><span>LOSS</span><b id="losses" class="bad">0</b></div>
<div class="stat"><span>ASSERTIVIDADE</span><b id="accuracy">0%</b></div>
</div>
<button id="reset" style="margin-top:10px">ZERAR RESULTADOS</button>
</div>

<div class="card">
<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap">
<div><div class="small">RANKING DE DESEMPENHO</div><div class="sniperTitle">🏆 SNIPER MAIS ASSERTIVO — 3 HORAS</div></div>
<div id="rankingStatus" class="badge">AGUARDANDO</div>
</div>
<div id="rankingWinner"></div>
<div id="rankingList" class="rankTable"></div>
<div id="rankingWarning" class="small" style="margin-top:10px"></div>
</div>

<div class="card">
<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap">
<div><div class="small">RADAR DE OPORTUNIDADES</div><div class="sniperTitle">PARES PRÓXIMOS DE SINAL</div></div>
<div id="radarStatus" class="badge">AGUARDANDO</div>
</div>
<div class="small" style="margin-top:8px">Mostra os pares com maior proximidade de CALL ou PUT na estratégia ativa.</div>
<div id="radarList" style="margin-top:12px"></div>
</div>

<div class="card footer">
O sinal é probabilístico e não garante WIN. O ranking mostra o desempenho histórico das últimas 3 horas. A análise usa velas fechadas para reduzir repintura e os preços da Twelve Data podem diferir da corretora.
</div>
</div>

<script>
const $=id=>document.getElementById(id);
let pending=JSON.parse(localStorage.getItem("is_trade_pending")||"null");
let isOnline=true,statsKey="",stats={wins:0,losses:0},resultTimer=null;

function getStatsKey(s,i,t){return `${s}|${i}|${t}`}
function loadStatsFor(s,i,t){
 statsKey=getStatsKey(s,i,t);
 const a=JSON.parse(localStorage.getItem("is_trade_stats_by_key")||"{}");
 stats=a[statsKey]||{wins:0,losses:0};renderStats()
}
function saveStats(){
 const a=JSON.parse(localStorage.getItem("is_trade_stats_by_key")||"{}");
 a[statsKey]=stats;localStorage.setItem("is_trade_stats_by_key",JSON.stringify(a))
}
function save(){
 saveStats();
 if(pending)localStorage.setItem("is_trade_pending",JSON.stringify(pending));
 else localStorage.removeItem("is_trade_pending")
}
function renderStats(){
 $("wins").textContent=stats.wins;$("losses").textContent=stats.losses;
 const t=stats.wins+stats.losses;
 $("accuracy").textContent=t?((stats.wins/t)*100).toFixed(1)+"%":"0%"
}

let rsiOnline=localStorage.getItem("is_trade_rsi_online")!=="off";
let oldOnline=localStorage.getItem("is_trade_old_online")==="on";
let sniper02Online=localStorage.getItem("is_trade_sniper02_online")==="on";
let sniper03Online=localStorage.getItem("is_trade_sniper03_online")==="on";
let selectedStrategy=localStorage.getItem("is_trade_selected_strategy") ||
 (oldOnline?"old_sniper":sniper03Online?"sniper_03":sniper02Online?"sniper_02":"rsi");

function setStrategyButton(id,on){
 const b=$(id);b.textContent=on?"● ONLINE":"● OFFLINE";
 b.className=on?"onlineBtn online":"onlineBtn offline"
}
function activeStrategy(){
 if(selectedStrategy==="rsi"&&rsiOnline)return"rsi";
 if(selectedStrategy==="old_sniper"&&oldOnline)return"old_sniper";
 if(selectedStrategy==="sniper_02"&&sniper02Online)return"sniper_02";
 if(selectedStrategy==="sniper_03"&&sniper03Online)return"sniper_03";
 if(rsiOnline)return"rsi";if(oldOnline)return"old_sniper";
 if(sniper02Online)return"sniper_02";if(sniper03Online)return"sniper_03";
 return null
}
function renderMode(){
 setStrategyButton("rsiToggle",rsiOnline);setStrategyButton("oldToggle",oldOnline);
 setStrategyButton("sniper02Toggle",sniper02Online);setStrategyButton("sniper03Toggle",sniper03Online);
 if(!isOnline||!activeStrategy()){$("signal").textContent="OFFLINE";$("signal").className="signal neutral"}
}

let audioCtx=null,lastSoundSignal=localStorage.getItem("is_trade_last_sound_signal")||"";
function unlockAudio(){
 try{
  if(!audioCtx){const AC=window.AudioContext||window.webkitAudioContext;if(!AC)return;audioCtx=new AC}
  if(audioCtx.state==="suspended")audioCtx.resume()
 }catch(e){}
}
function playSignalSound(d){
 if(d!=="CALL"&&d!=="PUT")return;
 try{
  unlockAudio();if(!audioCtx||audioCtx.state==="suspended")return;
  const n=audioCtx.currentTime,o=audioCtx.createOscillator(),g=audioCtx.createGain();
  o.type="sine";o.frequency.setValueAtTime(d==="CALL"?880:440,n);
  g.gain.setValueAtTime(.0001,n);g.gain.exponentialRampToValueAtTime(.16,n+.03);
  g.gain.setValueAtTime(.16,n+4.7);g.gain.exponentialRampToValueAtTime(.0001,n+5);
  o.connect(g);g.connect(audioCtx.destination);o.start(n);o.stop(n+5.02)
 }catch(e){}
}
document.addEventListener("pointerdown",unlockAudio,{passive:true});

function alertNewSignal(d,r,e){
 if(d!=="CALL"&&d!=="PUT")return;
 const k=`${r||""}|${e||""}|${d}`;
 if(k===lastSoundSignal)return;
 lastSoundSignal=k;localStorage.setItem("is_trade_last_sound_signal",k);playSignalSound(d)
}
function showError(m){
 $("signal").textContent="SEM DADOS";$("signal").className="signal neutral";
 $("confidence").textContent="--";$("signalError").textContent=m||"Erro";
 $("signalError").style.display="block"
}
function updateClock(){
 const n=new Date();
 const p=new Intl.DateTimeFormat("pt-BR",{timeZone:"America/Sao_Paulo",hour:"2-digit",minute:"2-digit",second:"2-digit",hour12:false}).formatToParts(n);
 const g=k=>p.find(x=>x.type===k)?.value||"00";
 $("clock").textContent=`${g("hour")}:${g("minute")}:${g("second")}`;
 $("dateBr").textContent=new Intl.DateTimeFormat("pt-BR",{timeZone:"America/Sao_Paulo",day:"2-digit",month:"2-digit",year:"numeric"}).format(n)
}
let serverOffsetMs=0;
function updateCountdown(){
 const m=({"1min":1,"5min":5,"15min":15,"30min":30})[$("interval").value]||1;
 const n=new Date(Date.now()+serverOffsetMs),b=m*60000,next=Math.ceil(n.getTime()/b)*b;
 const t=Math.max(0,Math.floor((next-n.getTime())/1000));
 $("countdown").textContent=`${String(Math.floor(t/60)).padStart(2,"0")}:${String(t%60).padStart(2,"0")}`
}
async function syncServerClock(){
 try{
  const t0=Date.now(),r=await fetch("/server-time",{cache:"no-store"}),d=await r.json(),t1=Date.now();
  serverOffsetMs=new Date(d.brasilia).getTime()-(t0+t1)/2
 }catch(e){}
}
async function loadLicense(){
 try{
  const r=await fetch("/license",{cache:"no-store"}),d=await r.json();
  $("licenseStatus").textContent=d.active?"● LICENÇA ATIVA":"● LICENÇA EXPIRADA";
  $("licenseStatus").className="value "+(d.active?"good":"bad");
  $("licenseExpires").textContent=d.expires||"--"
 }catch(e){$("licenseStatus").textContent="NÃO VERIFICADA"}
}
function scheduleResultCheck(){
 if(resultTimer)clearTimeout(resultTimer);if(!pending)return;
 const m=({"1min":1,"5min":5,"15min":15,"30min":30})[pending.interval]||1;
 const refMs=new Date(pending.reference_candle.replace(" ","T")).getTime();
 if(Number.isNaN(refMs)){resultTimer=setTimeout(checkResult,m*60000+5000);return}
 resultTimer=setTimeout(checkResult,Math.max(1000,refMs+2*m*60000+3000-Date.now()))
}
async function loadSignal(){
 if(!isOnline){renderMode();return}
 const symbol=$("symbol").value,interval=$("interval").value,strategy=activeStrategy();
 if(!strategy){renderMode();return}
 loadStatsFor(symbol,interval,strategy);
 $("signal").textContent="ANALISANDO...";$("signal").className="signal neutral";
 try{
  const r=await fetch(`/signal?symbol=${encodeURIComponent(symbol)}&interval=${interval}&strategy=${strategy}`,{cache:"no-store"});
  const d=await r.json();
  if(!r.ok)throw new Error(d.detail?.message||d.detail||"Erro ao consultar o sinal.");
  $("signal").textContent=d.signal;
  $("signal").className="signal "+(d.signal==="CALL"?"call":d.signal==="PUT"?"put":"neutral");
  $("confidence").textContent=d.confidence+"%";
  $("signalError").style.display="none";
  $("reference").textContent=d.reference_candle;
  $("next").textContent=d.entry_time||d.next_candle;
  $("entryTime").textContent=(d.entry_time||d.next_candle||"--").split(" ")[1]||"--";
  $("expiry").textContent=(d.expiry_time||"").split(" ")[1]||"--";
  if(d.signal!=="NEUTRO"){
   alertNewSignal(d.signal,d.reference_candle,d.entry_time);
   pending={symbol,interval,reference_candle:d.reference_candle,entry_time:d.entry_time,direction:d.signal};
   save();scheduleResultCheck()
  }
 }catch(e){showError(e.message)}
}
async function checkResult(){
 if(!pending)return;
 try{
  const r=await fetch(`/result?${new URLSearchParams(pending).toString()}`,{cache:"no-store"}),d=await r.json();
  if(!r.ok)return;
  if(d.result==="WIN"){stats.wins++;pending=null;save();renderStats()}
  else if(d.result==="LOSS"){stats.losses++;pending=null;save();renderStats()}
  else if(d.result==="DRAW"){pending=null;save()}
  else scheduleResultCheck()
 }catch(e){scheduleResultCheck()}
}
function radarRow(i){
 const c=i.direction==="CALL"?"call":i.direction==="PUT"?"put":"neutral";
 const l=i.direction==="NEUTRO"?(i.proximity>=75?"PRÓXIMO":"OBSERVAR"):i.direction;
 return `<div class="radarRow"><div><b>${i.symbol}</b><div class="small">${l} • confiança ${i.confidence}%</div></div><div class="radarMeter"><div class="radarFill ${c}" style="width:${i.proximity}%"></div></div><b>${i.proximity}%</b></div>`
}
async function loadRadar(){
 const s=activeStrategy();
 if(!isOnline||!s){$("radarStatus").textContent="OFFLINE";$("radarList").innerHTML="<div class='small'>Ative uma estratégia para iniciar o radar.</div>";return}
 $("radarStatus").textContent="ANALISANDO";
 try{
  const r=await fetch(`/radar?interval=${encodeURIComponent($("interval").value)}&strategy=${s}`,{cache:"no-store"}),d=await r.json();
  if(!r.ok)throw new Error(d.detail?.message||d.detail||"Erro no radar.");
  const a=d.results||[];$("radarStatus").textContent=a.length?`${a.length} PARES`:"SEM DADOS";
  $("radarList").innerHTML=a.length?a.map(radarRow).join(""):"<div class='small'>Nenhum par disponível.</div>"
 }catch(e){$("radarStatus").textContent="AGUARDAR";$("radarList").innerHTML=`<div class='small'>Radar indisponível: ${e.message||"erro"}</div>`}
}
function rankingRow(x,pos){
 const pct=Number(x.accuracy||0);
 return `<div class="rankRow"><div><span class="rankPos">${pos}º ${x.strategy}</span><div class="rankBar"><div style="width:${pct}%"></div></div></div><b class="good">${x.wins}</b><b class="bad">${x.losses}</b><b>${pct.toFixed(1)}%</b><b class="rankSignals">${x.signals}</b></div>`
}
async function loadRanking(){
 const s=$("symbol").value,i=$("interval").value;
 $("rankingStatus").textContent="ANALISANDO";
 try{
  const r=await fetch(`/sniper-ranking?symbol=${encodeURIComponent(s)}&interval=${i}&hours=3`,{cache:"no-store"});
  const d=await r.json();
  if(!r.ok)throw new Error(d.detail?.message||d.detail||"Erro no ranking.");
  const w=d.winner;
  $("rankingStatus").textContent=w?`CAMPEÃO: ${w.strategy}`:"SEM DADOS";
  $("rankingWinner").innerHTML=w?
   `<div class="rankWinner"><div class="small">MAIS ASSERTIVO NAS ÚLTIMAS 3 HORAS</div><div style="font-size:22px;font-weight:800;margin-top:4px">🏆 ${w.strategy}</div><div style="margin-top:5px"><b>${Number(w.accuracy).toFixed(1)}%</b> de assertividade • ${w.wins} WIN • ${w.losses} LOSS • ${w.signals} sinais</div></div>`:
   "<div class='small' style='margin-top:12px'>Ainda não há sinais resolvidos suficientes para apontar um campeão.</div>";
  $("rankingList").innerHTML=`<div class="rankRow rankHead"><div>SNIPER</div><div>WIN</div><div>LOSS</div><div>ACERTO</div><div class="rankSignals">SINAIS</div></div>`+
   (d.ranking||[]).map((x,n)=>rankingRow(x,n+1)).join("");
  $("rankingWarning").textContent=d.warning||""
 }catch(e){
  $("rankingStatus").textContent="AGUARDAR";
  $("rankingWinner").innerHTML=`<div class="small" style="margin-top:12px">Ranking temporariamente indisponível: ${e.message||"erro"}</div>`;
  $("rankingList").innerHTML="";$("rankingWarning").textContent=""
 }
}

$("refresh").onclick=()=>{loadSignal();loadRadar();loadRanking()};
function toggle(name,key,strategy){
 return ()=>{
  window[name]=!window[name];
  const on=window[name];
  if(on)selectedStrategy=strategy;
  localStorage.setItem(key,on?"on":"off");
  localStorage.setItem("is_trade_selected_strategy",selectedStrategy);
  pending=null;save();renderMode();
  if(isOnline&&on){loadStatsFor($("symbol").value,$("interval").value,strategy);loadSignal();loadRadar()}
  loadRanking()
 }
}
$("rsiToggle").onclick=toggle("rsiOnline","is_trade_rsi_online","rsi");
$("oldToggle").onclick=toggle("oldOnline","is_trade_old_online","old_sniper");
$("sniper02Toggle").onclick=toggle("sniper02Online","is_trade_sniper02_online","sniper_02");
$("sniper03Toggle").onclick=toggle("sniper03Online","is_trade_sniper03_online","sniper_03");

$("reset").onclick=()=>{
 if(confirm("Zerar WIN e LOSS deste ATIVO/TEMPO/ESTRATÉGIA?")){
  stats={wins:0,losses:0};pending=null;save();renderStats()
 }
};

$("symbol").addEventListener("change",()=>{pending=null;save();loadSignal();loadRadar();loadRanking()});
$("interval").addEventListener("change",()=>{pending=null;save();loadSignal();loadRadar();loadRanking()});

const initial=activeStrategy();
if(initial)loadStatsFor($("symbol").value,$("interval").value,initial);
else renderStats();

renderMode();updateClock();updateCountdown();
setInterval(updateClock,1000);
setInterval(updateCountdown,250);
syncServerClock();loadLicense();loadSignal();loadRadar();loadRanking();

let lastEntrySlot="";
setInterval(()=>{
 if(pending)return;
 const m=({"1min":1,"5min":5,"15min":15,"30min":30})[$("interval").value]||1;
 const n=new Date(Date.now()+serverOffsetMs);
 const slot=Math.floor(n.getTime()/(m*60000));
 const seconds=Math.floor((n.getTime()%(m*60000))/1000);
 if(seconds<=3&&String(slot)!==lastEntrySlot){
  lastEntrySlot=String(slot);loadSignal()
 }
},1000);

if(pending)scheduleResultCheck();
setInterval(syncServerClock,30000);
setInterval(loadLicense,60000);
setInterval(loadRadar,120000);
setInterval(loadRanking,120000);
</script>
</body>
</html>'''
    return HTMLResponse(html)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
