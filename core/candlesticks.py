#!/usr/bin/env python3
# core/candlesticks.py
"""
================================================================================
Candlestick Pattern Detection — v1.0.0
================================================================================

Pure pattern-detection module for the Tadawul Fast Bridge (TFB) project.
Consumes a list of OHLC bars (oldest -> newest) and returns 5 schema fields
that the engine merges into its history patch.

Design principles
-----------------
- Zero I/O, zero engine imports. Fully unit-testable in isolation.
- Pure Python stdlib (math). No numpy / pandas / talib dependency.
- Defensive on input: missing fields, malformed bars, or insufficient
  history return clean empty fields rather than raising.
- Signal labels aligned with engine conventions:
      BULLISH | BEARISH | NEUTRAL | DOJI
- Strength labels: STRONG | MODERATE | WEAK
- Confidence: float, 0-100

Patterns supported (Phase 1)
----------------------------
Single-bar : Doji, Hammer, Inverted Hammer, Shooting Star, Hanging Man,
             Bullish Marubozu, Bearish Marubozu
Two-bar    : Bullish Engulfing, Bearish Engulfing
Three-bar  : Morning Star, Evening Star

Trend context (used to disambiguate Hammer / Hanging Man and
Inverted Hammer / Shooting Star — same shape, opposite meaning):
- A simple percentage change on closes from `idx - DEFAULT_TREND_LOOKBACK`
  through `idx - 1`. Excludes the candle being evaluated to avoid
  self-bias.
- Threshold: |pct| >= DEFAULT_TREND_THRESHOLD (default 1%) -> UP / DOWN,
  otherwise FLAT.

Output schema (the 5 fields the engine adds to the canonical row)
-----------------------------------------------------------------
    candlestick_pattern         "Candle Pattern"         str
    candlestick_signal          "Candle Signal"          str
    candlestick_strength        "Candle Strength"        str
    candlestick_confidence      "Candle Confidence"      float (0-100)
    candlestick_patterns_recent "Recent Patterns (5D)"   str ("|"-separated)

Usage
-----
    from core.candlesticks import detect_patterns

    rows = [
        {"open": 100, "high": 102, "low": 99, "close": 101, "volume": 1000},
        {"open": 101, "high": 103, "low": 100, "close": 102, "volume": 1200},
        # ... oldest -> newest
    ]
    fields = detect_patterns(rows)
    # -> {
    #      "candlestick_pattern": "Hammer",
    #      "candlestick_signal": "BULLISH",
    #      "candlestick_strength": "MODERATE",
    #      "candlestick_confidence": 72.0,
    #      "candlestick_patterns_recent": "Doji | Hammer"
    #    }

Self-test
---------
Run `python -m core.candlesticks` (or `python core/candlesticks.py`)
to execute the bundled synthetic-data assertions.
================================================================================
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

__version__ = "1.0.0"


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SIGNAL_BULLISH = "BULLISH"
SIGNAL_BEARISH = "BEARISH"
SIGNAL_NEUTRAL = "NEUTRAL"
SIGNAL_DOJI = "DOJI"

STRENGTH_STRONG = "STRONG"
STRENGTH_MODERATE = "MODERATE"
STRENGTH_WEAK = "WEAK"

# Pattern names (kept ASCII to avoid encoding surprises on sheet writes).
P_DOJI = "Doji"
P_HAMMER = "Hammer"
P_INVERTED_HAMMER = "Inverted Hammer"
P_SHOOTING_STAR = "Shooting Star"
P_HANGING_MAN = "Hanging Man"
P_MARUBOZU_BULL = "Bullish Marubozu"
P_MARUBOZU_BEAR = "Bearish Marubozu"
P_BULL_ENGULFING = "Bullish Engulfing"
P_BEAR_ENGULFING = "Bearish Engulfing"
P_MORNING_STAR = "Morning Star"
P_EVENING_STAR = "Evening Star"

# Pattern -> signal map.
_PATTERN_SIGNAL: Dict[str, str] = {
    P_DOJI: SIGNAL_DOJI,
    P_HAMMER: SIGNAL_BULLISH,
    P_INVERTED_HAMMER: SIGNAL_BULLISH,
    P_SHOOTING_STAR: SIGNAL_BEARISH,
    P_HANGING_MAN: SIGNAL_BEARISH,
    P_MARUBOZU_BULL: SIGNAL_BULLISH,
    P_MARUBOZU_BEAR: SIGNAL_BEARISH,
    P_BULL_ENGULFING: SIGNAL_BULLISH,
    P_BEAR_ENGULFING: SIGNAL_BEARISH,
    P_MORNING_STAR: SIGNAL_BULLISH,
    P_EVENING_STAR: SIGNAL_BEARISH,
}

# Detection thresholds (tunable, kept conservative for Phase 1).
DOJI_BODY_RATIO_MAX = 0.10           # body / range must be below this for Doji
MARUBOZU_BODY_RATIO_MIN = 0.95       # body / range must exceed this for Marubozu
HAMMER_SHADOW_MULTIPLIER = 2.0       # long shadow must be >= 2 * body
HAMMER_OPP_SHADOW_RATIO_MAX = 0.30   # opposing shadow / range must be <= this
SMALL_BODY_RATIO_MAX = 0.30          # for star middle bar
LONG_BODY_RATIO_MIN = 0.55           # for star outer bars

DEFAULT_TREND_LOOKBACK = 10          # bars to use for trend determination
DEFAULT_TREND_THRESHOLD = 0.01       # 1% slope threshold (fraction)
DEFAULT_RECENT_LOOKBACK = 5          # window for the "Recent Patterns" field


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _as_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


def _coerce_bar(row: Any) -> Optional[Dict[str, float]]:
    """
    Coerce an arbitrary row dict into a normalized bar:
        {"open", "high", "low", "close", "volume"}.
    Accepts the same field-name variants the engine accepts in
    `_coerce_history_rows`. Returns None if no usable close is found.
    """
    if not isinstance(row, dict):
        return None
    o = _as_float(row.get("open") if row.get("open") is not None else row.get("o"))
    h = _as_float(row.get("high") if row.get("high") is not None else row.get("h"))
    low = _as_float(row.get("low") if row.get("low") is not None else row.get("l"))
    c = _as_float(
        row.get("close")
        if row.get("close") is not None
        else (
            row.get("adjusted_close")
            if row.get("adjusted_close") is not None
            else (row.get("adjclose") if row.get("adjclose") is not None else row.get("c"))
        )
    )
    if c is None:
        return None
    if o is None:
        o = c
    if h is None:
        h = max(o, c)
    if low is None:
        low = min(o, c)
    # Sanity: ensure high >= low even if upstream sent inverted bounds.
    if h < low:
        h, low = low, h
    v = _as_float(row.get("volume") if row.get("volume") is not None else row.get("v")) or 0.0
    return {"open": o, "high": h, "low": low, "close": c, "volume": v}


def _coerce_bars(rows: Any) -> List[Dict[str, float]]:
    out: List[Dict[str, float]] = []
    if not rows:
        return out
    try:
        iterable = list(rows)
    except Exception:
        return out
    for row in iterable:
        bar = _coerce_bar(row)
        if bar is not None:
            out.append(bar)
    return out


def _bar_geom(bar: Dict[str, float]) -> Dict[str, float]:
    """Geometry derived from a single bar."""
    o = bar["open"]
    h = bar["high"]
    low = bar["low"]
    c = bar["close"]
    rng = max(h - low, 1e-12)  # guard against flat bars
    body = abs(c - o)
    upper_shadow = max(h - max(o, c), 0.0)
    lower_shadow = max(min(o, c) - low, 0.0)
    return {
        "open": o,
        "high": h,
        "low": low,
        "close": c,
        "range": rng,
        "body": body,
        "body_ratio": body / rng,
        "upper_shadow": upper_shadow,
        "lower_shadow": lower_shadow,
        "upper_shadow_ratio": upper_shadow / rng,
        "lower_shadow_ratio": lower_shadow / rng,
        "is_bullish": c > o,
        "is_bearish": c < o,
        "midpoint": (o + c) / 2.0,
    }


def _trend_at(
    bars: List[Dict[str, float]],
    idx: int,
    lookback: int = DEFAULT_TREND_LOOKBACK,
    threshold: float = DEFAULT_TREND_THRESHOLD,
) -> str:
    """
    Determine trend at bars[idx] using closes from bars[idx - lookback] up to
    bars[idx - 1]. Returns "UP" | "DOWN" | "FLAT".

    Lookback excludes the candle being evaluated so the candle itself does
    not bias the trend.
    """
    start = idx - lookback
    if start < 0:
        return "FLAT"
    window = bars[start:idx]
    if len(window) < 3:
        return "FLAT"
    first = window[0]["close"]
    last = window[-1]["close"]
    if first <= 0:
        return "FLAT"
    pct = (last - first) / first
    if pct >= threshold:
        return "UP"
    if pct <= -threshold:
        return "DOWN"
    return "FLAT"


# ---------------------------------------------------------------------------
# Pattern detectors
# Each returns (pattern_name, confidence_0_100) or None.
# Confidence reflects how comfortably the geometric / contextual criteria
# are met. Intentionally conservative for Phase 1.
# ---------------------------------------------------------------------------
def _detect_marubozu(g: Dict[str, float]) -> Optional[Tuple[str, float]]:
    if g["body_ratio"] < MARUBOZU_BODY_RATIO_MIN:
        return None
    confidence = 50.0 + (g["body_ratio"] - MARUBOZU_BODY_RATIO_MIN) * 1000.0
    confidence = max(50.0, min(100.0, confidence))
    if g["is_bullish"]:
        return P_MARUBOZU_BULL, confidence
    if g["is_bearish"]:
        return P_MARUBOZU_BEAR, confidence
    return None


def _detect_doji(g: Dict[str, float]) -> Optional[Tuple[str, float]]:
    if g["body_ratio"] >= DOJI_BODY_RATIO_MAX:
        return None
    # Tighter body -> higher confidence, clamped to [40, 95].
    raw = 100.0 * (1.0 - g["body_ratio"] / DOJI_BODY_RATIO_MAX)
    confidence = max(40.0, min(95.0, raw))
    return P_DOJI, confidence


def _has_hammer_shape(g: Dict[str, float]) -> bool:
    """Long lower shadow, short upper shadow, real (non-zero) body."""
    if g["body"] <= 0:
        return False
    if g["lower_shadow"] < HAMMER_SHADOW_MULTIPLIER * g["body"]:
        return False
    if g["upper_shadow_ratio"] > HAMMER_OPP_SHADOW_RATIO_MAX:
        return False
    return True


def _has_inverted_hammer_shape(g: Dict[str, float]) -> bool:
    """Long upper shadow, short lower shadow, real (non-zero) body."""
    if g["body"] <= 0:
        return False
    if g["upper_shadow"] < HAMMER_SHADOW_MULTIPLIER * g["body"]:
        return False
    if g["lower_shadow_ratio"] > HAMMER_OPP_SHADOW_RATIO_MAX:
        return False
    return True


def _detect_hammer_family(g: Dict[str, float], trend: str) -> Optional[Tuple[str, float]]:
    """
    Same shape, opposite meaning depending on trend:
      Hammer / Hanging Man            -> long lower shadow
      Inverted Hammer / Shooting Star -> long upper shadow
    """
    # Hammer-shaped bars (long lower shadow).
    if _has_hammer_shape(g):
        ratio = g["lower_shadow"] / max(g["body"], 1e-9)
        base_conf = min(85.0, 40.0 + ratio * 8.0)
        if trend == "DOWN":
            return P_HAMMER, base_conf
        if trend == "UP":
            return P_HANGING_MAN, base_conf * 0.85  # slightly less reliable
        # FLAT trend: still classify as Hammer but with reduced confidence.
        return P_HAMMER, base_conf * 0.65

    # Inverted-hammer-shaped bars (long upper shadow).
    if _has_inverted_hammer_shape(g):
        ratio = g["upper_shadow"] / max(g["body"], 1e-9)
        base_conf = min(80.0, 35.0 + ratio * 8.0)
        if trend == "DOWN":
            return P_INVERTED_HAMMER, base_conf
        if trend == "UP":
            return P_SHOOTING_STAR, base_conf
        return P_INVERTED_HAMMER, base_conf * 0.65

    return None


def _detect_engulfing(
    g_prev: Dict[str, float],
    g_curr: Dict[str, float],
) -> Optional[Tuple[str, float]]:
    """Two-bar engulfing detection."""
    # Both bars need real bodies; skip if either is doji-like.
    if g_prev["body_ratio"] < DOJI_BODY_RATIO_MAX:
        return None
    if g_curr["body_ratio"] < DOJI_BODY_RATIO_MAX:
        return None

    prev_open = g_prev["open"]
    prev_close = g_prev["close"]
    curr_open = g_curr["open"]
    curr_close = g_curr["close"]

    # Bullish engulfing: bearish prior, bullish current, current body covers prior body.
    if g_prev["is_bearish"] and g_curr["is_bullish"]:
        if curr_open <= prev_close and curr_close >= prev_open:
            engulf_factor = g_curr["body"] / max(g_prev["body"], 1e-9)
            confidence = min(90.0, 50.0 + engulf_factor * 10.0)
            return P_BULL_ENGULFING, confidence

    # Bearish engulfing: bullish prior, bearish current, current body covers prior body.
    if g_prev["is_bullish"] and g_curr["is_bearish"]:
        if curr_open >= prev_close and curr_close <= prev_open:
            engulf_factor = g_curr["body"] / max(g_prev["body"], 1e-9)
            confidence = min(90.0, 50.0 + engulf_factor * 10.0)
            return P_BEAR_ENGULFING, confidence

    return None


def _detect_star(
    g_first: Dict[str, float],
    g_star: Dict[str, float],
    g_third: Dict[str, float],
) -> Optional[Tuple[str, float]]:
    """Three-bar Morning Star / Evening Star detection."""
    # Middle bar must have small body.
    if g_star["body_ratio"] >= SMALL_BODY_RATIO_MAX:
        return None
    # First and third bars must have substantial bodies.
    if g_first["body_ratio"] < LONG_BODY_RATIO_MIN:
        return None
    if g_third["body_ratio"] < LONG_BODY_RATIO_MIN:
        return None

    # Morning Star: bearish -> small -> bullish, third closes past midpoint of first.
    if g_first["is_bearish"] and g_third["is_bullish"]:
        if g_third["close"] > g_first["midpoint"]:
            penetration = (g_third["close"] - g_first["midpoint"]) / max(g_first["body"], 1e-9)
            confidence = min(95.0, 60.0 + penetration * 30.0)
            return P_MORNING_STAR, confidence

    # Evening Star: bullish -> small -> bearish, third closes past midpoint of first.
    if g_first["is_bullish"] and g_third["is_bearish"]:
        if g_third["close"] < g_first["midpoint"]:
            penetration = (g_first["midpoint"] - g_third["close"]) / max(g_first["body"], 1e-9)
            confidence = min(95.0, 60.0 + penetration * 30.0)
            return P_EVENING_STAR, confidence

    return None


# ---------------------------------------------------------------------------
# Detection runner — chooses the best pattern at a given index
# ---------------------------------------------------------------------------
def _detect_at_index(bars: List[Dict[str, float]], idx: int) -> Optional[Tuple[str, float]]:
    """
    Detect the best pattern ending at bars[idx]. Returns (pattern, confidence)
    or None. Higher-bar patterns take priority over lower-bar patterns when
    multiple fire.
    """
    if idx < 0 or idx >= len(bars):
        return None

    trend = _trend_at(bars, idx)
    g_curr = _bar_geom(bars[idx])

    # 3-bar patterns (highest priority).
    if idx >= 2:
        g_first = _bar_geom(bars[idx - 2])
        g_star = _bar_geom(bars[idx - 1])
        star = _detect_star(g_first, g_star, g_curr)
        if star is not None:
            return star

    # 2-bar engulfing.
    if idx >= 1:
        g_prev = _bar_geom(bars[idx - 1])
        eng = _detect_engulfing(g_prev, g_curr)
        if eng is not None:
            return eng

    # 1-bar patterns: Marubozu, Hammer family, Doji.
    marubozu = _detect_marubozu(g_curr)
    if marubozu is not None:
        return marubozu

    hammer = _detect_hammer_family(g_curr, trend)
    if hammer is not None:
        return hammer

    doji = _detect_doji(g_curr)
    if doji is not None:
        return doji

    return None


def _confidence_to_strength(confidence: float) -> str:
    if confidence >= 75.0:
        return STRENGTH_STRONG
    if confidence >= 55.0:
        return STRENGTH_MODERATE
    return STRENGTH_WEAK


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def detect_patterns(
    rows: Any,
    *,
    recent_lookback: int = DEFAULT_RECENT_LOOKBACK,
) -> Dict[str, Any]:
    """
    Run candlestick pattern detection on a series of OHLC rows.

    Parameters
    ----------
    rows : iterable of dict
        Each row should be a dict with at least a "close" field. Recognised
        keys (with fallbacks): open/o, high/h, low/l,
        close/adjusted_close/adjclose/c, volume/v.
        Rows must be ordered oldest -> newest.
    recent_lookback : int
        Window for the "Recent Patterns (5D)" field. Defaults to 5.

    Returns
    -------
    dict with keys:
        candlestick_pattern         (str)
        candlestick_signal          (str: BULLISH | BEARISH | NEUTRAL | DOJI)
        candlestick_strength        (str: STRONG | MODERATE | WEAK | "")
        candlestick_confidence      (float, 0-100)
        candlestick_patterns_recent (str, " | "-separated)

    The function is total: it never raises on malformed input. Insufficient
    data yields empty fields rather than an exception.
    """
    empty: Dict[str, Any] = {
        "candlestick_pattern": "",
        "candlestick_signal": SIGNAL_NEUTRAL,
        "candlestick_strength": "",
        "candlestick_confidence": 0.0,
        "candlestick_patterns_recent": "",
    }

    bars = _coerce_bars(rows)
    if not bars:
        return empty

    last_idx = len(bars) - 1

    # Pattern at the latest bar.
    latest = _detect_at_index(bars, last_idx)
    if latest is None:
        out = dict(empty)
    else:
        pattern, confidence = latest
        out = {
            "candlestick_pattern": pattern,
            "candlestick_signal": _PATTERN_SIGNAL.get(pattern, SIGNAL_NEUTRAL),
            "candlestick_strength": _confidence_to_strength(confidence),
            "candlestick_confidence": round(float(confidence), 2),
            "candlestick_patterns_recent": "",
        }

    # Recent-patterns scan over the last `recent_lookback` bars.
    recent_patterns: List[str] = []
    lookback = max(1, int(recent_lookback))
    start = max(0, last_idx - lookback + 1)
    for i in range(start, last_idx + 1):
        det = _detect_at_index(bars, i)
        if det is None:
            continue
        name, _ = det
        recent_patterns.append(name)
    out["candlestick_patterns_recent"] = " | ".join(recent_patterns)

    return out


# ---------------------------------------------------------------------------
# Self-test (synthetic data; run `python core/candlesticks.py`)
# ---------------------------------------------------------------------------
def _self_test() -> None:
    # ---- Test 1: Bullish Engulfing in a downtrend -------------------------
    bars: List[Dict[str, float]] = []
    base = 100.0
    for i in range(10):
        c = base - i * 1.2
        bars.append({"open": c + 0.3, "high": c + 0.7, "low": c - 0.5, "close": c, "volume": 1000})
    # Bar -1: small bearish.
    bars.append({"open": 90.0, "high": 90.5, "low": 88.0, "close": 88.5, "volume": 1100})
    # Bar 0: large bullish, fully engulfs prior body.
    bars.append({"open": 88.0, "high": 92.5, "low": 87.5, "close": 92.0, "volume": 1500})

    res = detect_patterns(bars)
    print("=== Test 1: Bullish Engulfing in downtrend ===")
    for k, v in res.items():
        print("  {}: {}".format(k, v))
    assert res["candlestick_pattern"] == P_BULL_ENGULFING, \
        "expected {}, got {}".format(P_BULL_ENGULFING, res["candlestick_pattern"])
    assert res["candlestick_signal"] == SIGNAL_BULLISH

    # ---- Test 2: Doji ------------------------------------------------------
    bars2 = list(bars[:-2])
    bars2.append({"open": 90.0, "high": 92.0, "low": 88.0, "close": 90.05, "volume": 1000})
    res2 = detect_patterns(bars2)
    print("\n=== Test 2: Doji ===")
    for k, v in res2.items():
        print("  {}: {}".format(k, v))
    assert res2["candlestick_pattern"] == P_DOJI

    # ---- Test 3: Hammer in a downtrend ------------------------------------
    bars3: List[Dict[str, float]] = []
    for i in range(10):
        c = 100.0 - i * 1.2
        bars3.append({"open": c + 0.3, "high": c + 0.7, "low": c - 0.5, "close": c, "volume": 1000})
    # Hammer: small body at top, long lower shadow.
    bars3.append({"open": 89.5, "high": 89.8, "low": 86.0, "close": 89.6, "volume": 1300})
    res3 = detect_patterns(bars3)
    print("\n=== Test 3: Hammer in downtrend ===")
    for k, v in res3.items():
        print("  {}: {}".format(k, v))
    assert res3["candlestick_pattern"] == P_HAMMER, \
        "expected {}, got {}".format(P_HAMMER, res3["candlestick_pattern"])
    assert res3["candlestick_signal"] == SIGNAL_BULLISH

    # ---- Test 4: Shooting Star in an uptrend ------------------------------
    bars4: List[Dict[str, float]] = []
    for i in range(10):
        c = 100.0 + i * 1.2
        bars4.append({"open": c - 0.3, "high": c + 0.5, "low": c - 0.7, "close": c, "volume": 1000})
    # Shooting Star: small body at bottom, long upper shadow.
    bars4.append({"open": 111.0, "high": 115.0, "low": 110.7, "close": 110.9, "volume": 1300})
    res4 = detect_patterns(bars4)
    print("\n=== Test 4: Shooting Star in uptrend ===")
    for k, v in res4.items():
        print("  {}: {}".format(k, v))
    assert res4["candlestick_pattern"] == P_SHOOTING_STAR, \
        "expected {}, got {}".format(P_SHOOTING_STAR, res4["candlestick_pattern"])
    assert res4["candlestick_signal"] == SIGNAL_BEARISH

    # ---- Test 5: Bullish Marubozu -----------------------------------------
    bars5: List[Dict[str, float]] = []
    for i in range(5):
        c = 100.0 + i * 0.1
        bars5.append({"open": c, "high": c + 0.05, "low": c - 0.05, "close": c, "volume": 1000})
    # Bullish marubozu: full body, ~zero shadows.
    bars5.append({"open": 100.0, "high": 105.01, "low": 99.99, "close": 105.0, "volume": 2000})
    res5 = detect_patterns(bars5)
    print("\n=== Test 5: Bullish Marubozu ===")
    for k, v in res5.items():
        print("  {}: {}".format(k, v))
    assert res5["candlestick_pattern"] == P_MARUBOZU_BULL, \
        "expected {}, got {}".format(P_MARUBOZU_BULL, res5["candlestick_pattern"])

    # ---- Test 6: Bearish Engulfing in an uptrend --------------------------
    bars6: List[Dict[str, float]] = []
    for i in range(10):
        c = 100.0 + i * 1.2
        bars6.append({"open": c - 0.3, "high": c + 0.5, "low": c - 0.5, "close": c, "volume": 1000})
    bars6.append({"open": 111.0, "high": 113.0, "low": 110.5, "close": 112.5, "volume": 1100})
    bars6.append({"open": 113.0, "high": 113.5, "low": 109.0, "close": 109.5, "volume": 1500})
    res6 = detect_patterns(bars6)
    print("\n=== Test 6: Bearish Engulfing in uptrend ===")
    for k, v in res6.items():
        print("  {}: {}".format(k, v))
    assert res6["candlestick_pattern"] == P_BEAR_ENGULFING, \
        "expected {}, got {}".format(P_BEAR_ENGULFING, res6["candlestick_pattern"])

    # ---- Test 7: Empty / malformed input ----------------------------------
    res7 = detect_patterns([])
    assert res7["candlestick_pattern"] == ""
    assert res7["candlestick_signal"] == SIGNAL_NEUTRAL
    res8 = detect_patterns(None)  # type: ignore[arg-type]
    assert res8["candlestick_pattern"] == ""
    res9 = detect_patterns([{"foo": "bar"}, "not a dict", 42])  # type: ignore[list-item]
    assert res9["candlestick_pattern"] == ""

    print("\nAll self-tests passed.")


if __name__ == "__main__":
    _self_test()


__all__ = [
    "__version__",
    "detect_patterns",
    # Signal labels
    "SIGNAL_BULLISH",
    "SIGNAL_BEARISH",
    "SIGNAL_NEUTRAL",
    "SIGNAL_DOJI",
    # Strength labels
    "STRENGTH_STRONG",
    "STRENGTH_MODERATE",
    "STRENGTH_WEAK",
    # Pattern names (exposed so the engine and tests can reference by constant)
    "P_DOJI",
    "P_HAMMER",
    "P_INVERTED_HAMMER",
    "P_SHOOTING_STAR",
    "P_HANGING_MAN",
    "P_MARUBOZU_BULL",
    "P_MARUBOZU_BEAR",
    "P_BULL_ENGULFING",
    "P_BEAR_ENGULFING",
    "P_MORNING_STAR",
    "P_EVENING_STAR",
]
