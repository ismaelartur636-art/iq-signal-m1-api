"""SUPER NOVA CLEAN engine for app integration.

Faithful useful core from SUPER NOVA PRO:
- RSI(3), PRICE_OPEN style
- confirmed fractal support/resistance filtered by LWMA(30)
- Coral/T3-style trend filter (period 20, shift 2)
- preview on current candle; official entry on the next candle
- no martingale, DLL, Telegram or external indicators

Expected candles: oldest -> newest dicts with open/high/low/close and optional time.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Optional


@dataclass
class SuperNovaResult:
    signal: Optional[str]          # "CALL", "PUT", or None
    stage: str                     # "CONFIRMED", "PRE_ALERT", "WAIT"
    confidence: int
    rsi: Optional[float]
    support: Optional[float]
    resistance: Optional[float]
    trend: str                     # "UP", "DOWN", "FLAT"
    entry_mode: str = "NEXT_CANDLE"
    expiry_candles: int = 1
    engine: str = "SUPER NOVA"


def _lwma(values: list[float], end: int, period: int) -> Optional[float]:
    start = end - period + 1
    if start < 0:
        return None
    weights = range(1, period + 1)
    denom = period * (period + 1) / 2
    return sum(values[start + j] * w for j, w in enumerate(weights)) / denom


def _rsi(values: list[float], end: int, period: int) -> Optional[float]:
    if end < period:
        return None
    gains = 0.0
    losses = 0.0
    for j in range(end - period + 1, end + 1):
        d = values[j] - values[j - 1]
        if d > 0:
            gains += d
        else:
            losses -= d
    if losses == 0:
        return 100.0 if gains > 0 else 50.0
    rs = (gains / period) / (losses / period)
    return 100.0 - (100.0 / (1.0 + rs))


def _is_upper_fractal(highs: list[float], i: int) -> bool:
    return 2 <= i <= len(highs) - 3 and highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]


def _is_lower_fractal(lows: list[float], i: int) -> bool:
    return 2 <= i <= len(lows) - 3 and lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] < lows[i+1] and lows[i] < lows[i+2]


def _coral(closes: list[float], period: int = 20, shift: float = 2.0) -> list[float]:
    n = len(closes)
    if not n:
        return []
    s2 = shift * shift
    s3 = s2 * shift
    c1 = -s3
    c2 = 3.0 * (s2 + s3)
    c3 = -3.0 * (2.0 * s2 + shift + s3)
    c4 = 3.0 * shift + 1.0 + s3 + 3.0 * s2
    p = max(1.0, (period - 1.0) / 2.0 + 1.0)
    alpha = 2.0 / (p + 1.0)
    beta = 1.0 - alpha

    e1 = [closes[0]] * n
    e2 = [closes[0]] * n
    e3 = [closes[0]] * n
    e4 = [closes[0]] * n
    e5 = [closes[0]] * n
    e6 = [closes[0]] * n
    out = [closes[0]] * n
    for i in range(1, n):
        e1[i] = alpha * closes[i] + beta * e1[i-1]
        e2[i] = alpha * e1[i] + beta * e2[i-1]
        e3[i] = alpha * e2[i] + beta * e3[i-1]
        e4[i] = alpha * e3[i] + beta * e4[i-1]
        e5[i] = alpha * e4[i] + beta * e5[i-1]
        e6[i] = alpha * e5[i] + beta * e6[i-1]
        out[i] = c1*e6[i] + c2*e5[i] + c3*e4[i] + c4*e3[i]
    return out


def analyze_super_nova(
    candles: Iterable[Mapping[str, float]],
    *,
    rsi_period: int = 3,
    oversold: float = 35.0,
    overbought: float = 65.0,
    lwma_period: int = 30,
    use_trend_filter: bool = True,
    trend_period: int = 20,
    trend_shift: float = 2.0,
    current_candle_closed: bool = False,
) -> SuperNovaResult:
    c = list(candles)
    need = max(lwma_period, trend_period) + 12
    if len(c) < need:
        return SuperNovaResult(None, "WAIT", 0, None, None, None, "FLAT")

    opens = [float(x["open"]) for x in c]
    highs = [float(x["high"]) for x in c]
    lows = [float(x["low"]) for x in c]
    closes = [float(x["close"]) for x in c]
    typical = [(o+h+l+cl)/4.0 for o,h,l,cl in zip(opens,highs,lows,closes)]
    coral = _coral(closes, trend_period, trend_shift)

    # Build S/R as it would have been known in real time.
    # A fractal centered at f is usable only after f+2 has closed.
    support = None
    resistance = None
    support_at: list[Optional[float]] = [None] * len(c)
    resistance_at: list[Optional[float]] = [None] * len(c)

    for t in range(len(c)):
        f = t - 2
        if f >= 2:
            lw_h = _lwma(highs, f, lwma_period)
            lw_l = _lwma(lows, f, lwma_period)
            if lw_h is not None and _is_upper_fractal(highs, f) and typical[f] > lw_h:
                resistance = highs[f]
            if lw_l is not None and _is_lower_fractal(lows, f) and typical[f] < lw_l:
                support = lows[f]
        support_at[t] = support
        resistance_at[t] = resistance

    # For official signal, evaluate last CLOSED setup bar.
    # If newest candle is open, setup is previous candle; if newest is closed, it is newest.
    i = len(c)-1 if current_candle_closed else len(c)-2
    if i < 3:
        return SuperNovaResult(None, "WAIT", 0, None, support_at[i], resistance_at[i], "FLAT")

    rsi = _rsi(opens, i, rsi_period)  # original uses PRICE_OPEN
    trend_up = coral[i] > coral[i-1]
    trend_down = coral[i] < coral[i-1]
    trend = "UP" if trend_up else "DOWN" if trend_down else "FLAT"
    sup = support_at[i]
    res = resistance_at[i]

    call_sr = sup is not None and lows[i] <= sup and opens[i] >= closes[i]
    put_sr = res is not None and highs[i] >= res and opens[i] <= closes[i]
    call = bool(call_sr and rsi is not None and rsi <= oversold and (not use_trend_filter or trend_up))
    put = bool(put_sr and rsi is not None and rsi >= overbought and (not use_trend_filter or trend_down))

    if call ^ put:
        side = "CALL" if call else "PUT"
        # Keep confidence descriptive, not a win probability.
        conf = 70
        if side == "CALL":
            conf += 10 if rsi is not None and rsi <= oversold - 5 else 0
            conf += 10 if trend_up else 0
        else:
            conf += 10 if rsi is not None and rsi >= overbought + 5 else 0
            conf += 10 if trend_down else 0
        return SuperNovaResult(side, "CONFIRMED", min(conf, 90), rsi, sup, res, trend)

    # PRE-ALERT evaluates the newest candle only when it is still open.
    if not current_candle_closed:
        j = len(c)-1
        rsi0 = _rsi(opens, j, rsi_period)
        trend0_up = coral[i] > coral[i-1]       # freeze trend at last closed bar
        trend0_down = coral[i] < coral[i-1]
        sup0 = support_at[i]                    # freeze S/R at last closed bar
        res0 = resistance_at[i]
        pre_call = sup0 is not None and lows[j] <= sup0 and opens[j] >= closes[j] and rsi0 is not None and rsi0 <= oversold and (not use_trend_filter or trend0_up)
        pre_put = res0 is not None and highs[j] >= res0 and opens[j] <= closes[j] and rsi0 is not None and rsi0 >= overbought and (not use_trend_filter or trend0_down)
        if pre_call ^ pre_put:
            side = "CALL" if pre_call else "PUT"
            return SuperNovaResult(side, "PRE_ALERT", 65, rsi0, sup0, res0, "UP" if trend0_up else "DOWN" if trend0_down else "FLAT")

    return SuperNovaResult(None, "WAIT", 0, rsi, sup, res, trend)
