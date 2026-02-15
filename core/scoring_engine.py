# core/scoring_engine.py
"""
core/scoring_engine.py
===========================================================
Advanced Scoring & Forecasting Engine (v1.7.2)
(Emad Bahbah – Financial Leader Edition)

Generates professional-grade quantitative scores:
- Value, Quality, Momentum, Risk, Opportunity (0-100)
- Overall Score & Recommendation (BUY/HOLD/REDUCE/SELL)
- Confidence (Data depth + Source reliability)

v1.7.2 Enhancements:
- ✅ Aligned Forecast Keys: Strict output for forecast_price_* and expected_roi_*.
- ✅ Riyadh Localization: Adds forecast_updated_riyadh (UTC+3).
- ✅ Trend Integration: Uses `trend_signal` to weight Opportunity/Risk.
- ✅ Technical Boost: Uses MACD Histogram and RSI for precise momentum scoring.
- ✅ News Intelligence: Blends `news_score` into scores (if available).
- ✅ Liquidity Guard: Penalizes Risk Score for low-liquidity assets.
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone, timedelta

# Pydantic best-effort (import-safe)
try:
    from pydantic import BaseModel, Field, ConfigDict  # type: ignore

    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    from pydantic import BaseModel, Field  # type: ignore

    ConfigDict = None  # type: ignore
    _PYDANTIC_V2 = False


SCORING_ENGINE_VERSION = "1.7.2"

# Recommendation enum (MUST match routers normalization)
_RECO_ENUM = ("BUY", "HOLD", "REDUCE", "SELL")


def _riyadh_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()


class AssetScores(BaseModel):
    """
    Returned by compute_scores() for internal usage / debugging.
    """
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")  # type: ignore
    else:  # pragma: no cover
        class Config:
            extra = "ignore"

    value_score: float = Field(50.0, ge=0, le=100)
    quality_score: float = Field(50.0, ge=0, le=100)
    momentum_score: float = Field(50.0, ge=0, le=100)
    risk_score: float = Field(50.0, ge=0, le=100)  # higher = MORE risk

    opportunity_score: float = Field(50.0, ge=0, le=100)
    overall_score: float = Field(50.0, ge=0, le=100)
    recommendation: str = Field("HOLD")
    scoring_reason: str = Field("")

    confidence: float = Field(50.0, ge=0, le=100)

    # Optional badges (helpful for sheets/UI)
    rec_badge: Optional[str] = None
    momentum_badge: Optional[str] = None
    opportunity_badge: Optional[str] = None
    risk_badge: Optional[str] = None

    # Forecast outputs (optional, best-effort)
    # Canonical (preferred)
    expected_roi_1m: Optional[float] = None
    expected_roi_3m: Optional[float] = None
    expected_roi_12m: Optional[float] = None

    forecast_price_1m: Optional[float] = None
    forecast_price_3m: Optional[float] = None
    forecast_price_12m: Optional[float] = None

    forecast_confidence: Optional[float] = None
    forecast_method: Optional[str] = None
    forecast_updated_utc: Optional[str] = None
    forecast_updated_riyadh: Optional[str] = None

    # Compat aliases (kept)
    expected_return_1m: Optional[float] = None
    expected_return_3m: Optional[float] = None
    expected_return_12m: Optional[float] = None

    expected_price_1m: Optional[float] = None
    expected_price_3m: Optional[float] = None
    expected_price_12m: Optional[float] = None

    confidence_score: Optional[float] = None  # alias (some routes/providers expect this)

    # gross (unadjusted) returns for audit/debug (NOT mapped to sheets by default)
    expected_return_gross_1m: Optional[float] = None
    expected_return_gross_3m: Optional[float] = None
    expected_return_gross_12m: Optional[float] = None


# ---------------------------------------------------------------------
# Helpers (safe + provider-agnostic)
# ---------------------------------------------------------------------
def _clamp(x: Any, lo: float = 0.0, hi: float = 100.0, default: float = 50.0) -> float:
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return float(default)
        return max(lo, min(hi, v))
    except Exception:
        return float(default)


def _get(obj: Any, name: str, default: Any = None) -> Any:
    """Safe attribute getter that also supports dict-like objects."""
    if obj is None:
        return default
    try:
        if isinstance(obj, dict):
            return obj.get(name, default)
        return getattr(obj, name, default)
    except Exception:
        return default


def _get_any(obj: Any, *names: str) -> Any:
    """Returns the first non-null, non-empty value across candidate names."""
    for n in names:
        v = _get(obj, n, None)
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None


def _strip_percent_str(s: str) -> str:
    t = (s or "").strip()
    if not t:
        return ""
    return t.replace(",", "").replace("%", "").strip()


def _to_float(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        if isinstance(x, str):
            t = _strip_percent_str(x)
            if not t:
                return None
            v = float(t)
        else:
            v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


def _to_percent(x: Any) -> Optional[float]:
    """
    Normalize any percent-like value to 0..100 scale:
    - If abs(val) <= 1.5 -> treat as fraction and multiply by 100
    - Else assume it's already percent
    - Strings like "12%" supported
    """
    v = _to_float(x)
    if v is None:
        return None
    if v == 0.0:
        return 0.0
    if abs(v) <= 1.5:
        return v * 100.0
    return v


def _is_present_number(x: Any) -> bool:
    return _to_float(x) is not None


def _pos52w_frac(cp: Optional[float], low_52w: Optional[float], high_52w: Optional[float]) -> Optional[float]:
    if cp is None or low_52w is None or high_52w is None:
        return None
    rng = high_52w - low_52w
    if rng == 0:
        return None
    return (cp - low_52w) / rng  # 0..1


def _score_band_linear(x: Optional[float], bands: List[Tuple[float, float]]) -> float:
    """
    Piecewise linear score from ordered bands [(x,score),...].
    If x is below first band -> first score; above last -> last score.
    """
    if x is None:
        return 50.0
    try:
        xv = float(x)
    except Exception:
        return 50.0
    if not bands:
        return 50.0
    bands = sorted(bands, key=lambda t: t[0])
    if xv <= bands[0][0]:
        return float(bands[0][1])
    if xv >= bands[-1][0]:
        return float(bands[-1][1])
    for i in range(1, len(bands)):
        x0, s0 = bands[i - 1]
        x1, s1 = bands[i]
        if x0 <= xv <= x1:
            if x1 == x0:
                return float(s1)
            t = (xv - x0) / (x1 - x0)
            return float(s0 + t * (s1 - s0))
    return 50.0


def _normalize_reco(x: Any) -> str:
    """Hard normalize to BUY/HOLD/REDUCE/SELL."""
    if x is None:
        return "HOLD"
    try:
        s = str(x).strip().upper()
    except Exception:
        return "HOLD"
    if s in _RECO_ENUM:
        return s
    if "SELL" in s:
        return "SELL"
    if "REDUCE" in s or "TRIM" in s or "TAKE" in s:
        return "REDUCE"
    if "BUY" in s or "ACCUM" in s or "ADD" in s or "OVERWEIGHT" in s:
        return "BUY"
    return "HOLD"


def _badge_3level(score: Optional[float], *, high: float, low: float, invert: bool = False) -> str:
    """
    Returns one of: Low / Moderate / High
    - invert=True: higher score -> "High" meaning *worse* (used for risk)
    """
    s = _to_float(score)
    if s is None:
        return "Moderate"
    if invert:
        if s >= high:
            return "High"
        if s <= low:
            return "Low"
        return "Moderate"
    else:
        if s >= high:
            return "High"
        if s <= low:
            return "Low"
        return "Moderate"


def _rec_badge_from_reco(reco: str, overall: float, confidence: float) -> str:
    r = _normalize_reco(reco)
    if r == "BUY" and overall >= 82 and confidence >= 65:
        return "STRONG BUY"
    if r == "SELL" and overall <= 22 and confidence >= 45:
        return "STRONG SELL"
    return r


def _forecast_scale(days: int) -> float:
    """
    Conservative horizon scaling for returns using sqrt(time):
      30d  -> sqrt(30/365)
      90d  -> sqrt(90/365)
      365d -> 1.0
    Keeps short-horizon forecasts from being unrealistically large.
    """
    d = max(1, int(days))
    return math.sqrt(min(1.0, d / 365.0))


def _compute_forecast(
    *,
    current_price: Optional[float],
    fair_value: Optional[float],
    upside_percent: Optional[float],
    expected_return_12m_gross_in: Optional[float],
    forecast_method_in: Optional[str],
    forecast_conf_in: Optional[float],
    confidence_fallback: float,
    risk_score: float,
    trend_signal: str,
    forecast_updated_utc: Optional[str],
) -> Dict[str, Any]:
    """
    Best-effort forecast outputs:
    CANONICAL:
      - forecast_price_* (price): derived from gross returns
      - expected_roi_* (%): "Expected ROI %" risk+confidence adjusted (SHEETS)
      - forecast_confidence (0..100), forecast_method, forecast_updated_utc
    """
    out: Dict[str, Any] = {}

    cp = current_price if (current_price is not None and current_price > 0) else None
    fv = fair_value if (fair_value is not None and fair_value > 0) else None

    # 1) Base 12m gross expected return (prefer explicit gross input)
    er12_gross = expected_return_12m_gross_in
    if er12_gross is None:
        if upside_percent is not None:
            er12_gross = upside_percent
        elif cp is not None and fv is not None:
            er12_gross = (fv / cp - 1.0) * 100.0

    if er12_gross is None or cp is None:
        return out

    # 2) Horizon gross returns (percent)
    er1_gross = er12_gross * _forecast_scale(30)
    er3_gross = er12_gross * _forecast_scale(90)

    # 3) Forecast prices from GROSS returns
    fp1 = cp * (1.0 + er1_gross / 100.0)
    fp3 = cp * (1.0 + er3_gross / 100.0)
    fp12 = cp * (1.0 + er12_gross / 100.0)

    # 4) Forecast confidence:
    fc = forecast_conf_in if forecast_conf_in is not None else confidence_fallback
    fc = _clamp(fc, 0.0, 100.0, 50.0)
    
    # Advanced: Boost confidence if trend aligns with forecast direction
    # If forecasting UP and Trend is UPTREND -> Confidence++
    if trend_signal == "UPTREND" and er3_gross > 0:
        fc = min(100.0, fc * 1.1)
    # If forecasting UP but Trend is DOWNTREND -> Confidence--
    elif trend_signal == "DOWNTREND" and er3_gross > 0:
        fc = max(10.0, fc * 0.8)

    # 5) Risk adjustment (higher risk => lower expected ROI)
    # risk_score: 0..100 => multiplier ~ 1.00..0.25 (floored)
    risk_mult = max(0.25, 1.0 - (risk_score / 140.0))
    
    # Advanced: Penalize ROI if fighting the trend
    trend_mult = 1.0
    if trend_signal == "DOWNTREND" and er3_gross > 0:
        trend_mult = 0.8  # dampen bullish forecast in downtrend

    # 6) Expected ROI% (risk + confidence + trend adjusted)
    roi1 = er1_gross * (fc / 100.0) * risk_mult * trend_mult
    roi3 = er3_gross * (fc / 100.0) * risk_mult * trend_mult
    roi12 = er12_gross * (fc / 100.0) * risk_mult * trend_mult

    out.update(
        {
            # gross (audit)
            "expected_return_gross_1m": float(er1_gross),
            "expected_return_gross_3m": float(er3_gross),
            "expected_return_gross_12m": float(er12_gross),

            # CANONICAL prices (Sheets: Forecast Price)
            "forecast_price_1m": float(fp1),
            "forecast_price_3m": float(fp3),
            "forecast_price_12m": float(fp12),

            # CANONICAL ROI (Sheets: Expected ROI %)
            "expected_roi_1m": float(roi1),
            "expected_roi_3m": float(roi3),
            "expected_roi_12m": float(roi12),

            "forecast_confidence": float(fc),
            "forecast_method": str(forecast_method_in or "trend_aware_horizon_scaling"),
            "forecast_updated_utc": str(forecast_updated_utc) if forecast_updated_utc else None,
            "forecast_updated_riyadh": _riyadh_iso(),

            # COMPAT aliases
            "expected_price_1m": float(fp1),
            "expected_price_3m": float(fp3),
            "expected_price_12m": float(fp12),

            "expected_return_1m": float(roi1),
            "expected_return_3m": float(roi3),
            "expected_return_12m": float(roi12),

            "confidence_score": float(fc),
        }
    )
    return out


# ---------------------------------------------------------------------
# Core scoring (deterministic, explainable)
# ---------------------------------------------------------------------
def compute_scores(q: Any) -> AssetScores:
    """
    Computes scores for an EnrichedQuote/UnifiedQuote-like object (duck typing).
    Never raises.
    """
    # -----------------------------
    # Pull common inputs (best-effort + alias tolerant)
    # -----------------------------
    # Valuation
    pe = _to_float(_get_any(q, "pe_ttm", "pe"))
    fpe = _to_float(_get_any(q, "forward_pe", "pe_forward"))
    pb = _to_float(_get_any(q, "pb"))
    ps = _to_float(_get_any(q, "ps"))
    ev = _to_float(_get_any(q, "ev_ebitda", "evebitda"))

    dy = _to_percent(_get_any(q, "dividend_yield", "div_yield", "dividend_yield_pct", "dividend_yield_percent"))
    payout = _to_percent(_get_any(q, "payout_ratio", "payout", "payout_pct"))

    # Forecast inputs (provider-enriched best-effort)
    fair_value = _to_float(_get_any(q, "fair_value", "target_mean_price", "intrinsic_value"))
    upside = _to_percent(_get_any(q, "upside_percent", "upside_pct"))

    # Prefer explicit *gross* 12m hint if provider supplies it (avoid recursion into our own outputs)
    expected_return_12m_gross_in = _to_percent(
        _get_any(
            q,
            "expected_return_gross_12m",
            "expected_return_12m_gross",
            "expected_roi_12m_gross",
            "expected_return_unadjusted_12m",
        )
    )

    forecast_method_in = _get_any(q, "forecast_method", "forecast_model")
    forecast_conf_in = _to_float(_get_any(q, "confidence_score", "forecast_confidence", "confidence"))
    forecast_updated_utc = _get_any(
        q,
        "forecast_updated_utc",
        "forecast_last_utc",
        "forecast_asof_utc",
        "as_of_utc",
        "last_updated_utc",
    )

    # Quality
    roe = _to_percent(_get_any(q, "roe", "return_on_equity"))
    roa = _to_percent(_get_any(q, "roa", "return_on_assets"))
    nm = _to_percent(_get_any(q, "net_margin", "profit_margin"))
    ebitda_m = _to_percent(_get_any(q, "ebitda_margin", "margin_ebitda"))
    rev_g = _to_percent(_get_any(q, "revenue_growth", "rev_growth"))
    ni_g = _to_percent(_get_any(q, "net_income_growth", "ni_growth"))

    dte = _to_float(_get_any(q, "debt_to_equity"))  # optional
    beta = _to_float(_get_any(q, "beta", "beta_5y"))
    vol = _to_percent(_get_any(q, "volatility_30d", "vol_30d_ann", "vol30d", "vol_30d"))
    rsi = _to_float(_get_any(q, "rsi_14", "rsi14"))

    # Advanced Tech: Trend & MACD (v1.7.2)
    trend_sig = str(_get_any(q, "trend_signal") or "NEUTRAL").upper()
    macd_hist = _to_float(_get_any(q, "macd_hist"))

    # Momentum / price context
    pc = _to_percent(_get_any(q, "percent_change", "change_percent", "change_pct", "pct_change"))

    cp = _to_float(_get_any(q, "current_price", "last_price", "price", "close", "last"))
    ma20 = _to_float(_get_any(q, "ma20", "sma20"))
    ma50 = _to_float(_get_any(q, "ma50", "sma50"))

    high_52w = _to_float(_get_any(q, "week_52_high", "high_52w", "52w_high"))
    low_52w = _to_float(_get_any(q, "week_52_low", "low_52w", "52w_low"))
    pos_52w_pct = _to_percent(_get_any(q, "position_52w_percent", "position_52w", "pos_52w_pct"))

    # Liquidity / market structure
    liq = _to_float(_get_any(q, "liquidity_score", "liq_score"))
    avg_vol = _to_float(_get_any(q, "avg_volume_30d", "avg_volume", "avg_vol_30d"))
    turnover = _to_percent(_get_any(q, "turnover_percent", "turnover", "turnover_pct"))
    free_float = _to_percent(_get_any(q, "free_float", "free_float_percent", "free_float_pct"))

    dq = str(_get_any(q, "data_quality", "dq") or "").strip().upper()
    
    # News Intelligence
    news_score = _to_float(_get_any(q, "news_score", "news_boost"))
    
    reasons: List[str] = []

    # -----------------------------
    # VALUE (0..100)
    # -----------------------------
    value_score = 50.0

    pe_like = None
    for cand in (pe, fpe):
        if cand is not None and cand > 0:
            pe_like = cand
            break
    
    # Advanced Valuation: If P/E missing or negative but Growth is high, use P/S
    if pe_like is None and ps is not None and (rev_g or 0) > 15.0:
        ps_val = _score_band_linear(
            ps,
            bands=[(0.7, 75.0), (1.5, 65.0), (3.0, 55.0), (6.0, 45.0), (10.0, 35.0), (20.0, 25.0)],
        )
        value_score = 0.70 * value_score + 0.30 * ps_val
        reasons.append("Valuation: Growth (P/S)")
    elif pe_like is not None:
        pe_val = _score_band_linear(
            pe_like,
            bands=[(5.0, 85.0), (10.0, 75.0), (18.0, 60.0), (25.0, 50.0), (40.0, 35.0), (60.0, 25.0)],
        )
        value_score = 0.70 * value_score + 0.30 * pe_val

    if pb is not None and pb > 0:
        pb_val = _score_band_linear(
            pb,
            bands=[(0.6, 80.0), (1.0, 70.0), (2.0, 60.0), (4.0, 50.0), (6.0, 40.0), (10.0, 30.0)],
        )
        value_score = 0.80 * value_score + 0.20 * pb_val

    if ev is not None and ev > 0:
        ev_val = _score_band_linear(
            ev,
            bands=[(5.0, 80.0), (8.0, 70.0), (12.0, 60.0), (18.0, 45.0), (25.0, 35.0), (40.0, 25.0)],
        )
        value_score = 0.85 * value_score + 0.15 * ev_val

    if dy is not None and dy >= 0:
        dy_val = _score_band_linear(dy, bands=[(0.0, 40.0), (2.0, 65.0), (4.0, 80.0), (6.0, 75.0), (10.0, 55.0), (15.0, 40.0)])
        value_score = 0.85 * value_score + 0.15 * dy_val

    if payout is not None and payout >= 0:
        if payout > 120:
            value_score -= 6
        elif 30 <= payout <= 70:
            value_score += 3

    # Upside: if missing, attempt compute from fair_value and current_price
    if upside is None and cp is not None and fair_value is not None and cp > 0:
        upside = (fair_value / cp - 1.0) * 100.0

    if upside is not None:
        if upside > 0:
            value_score += min(14.0, upside / 2.0)
            if upside > 15: reasons.append("Undervalued")
        elif upside < 0:
            value_score -= min(10.0, abs(upside) / 2.0)

    value_score = _clamp(value_score)

    # -----------------------------
    # QUALITY (0..100)
    # -----------------------------
    quality_score = 50.0

    if roe is not None:
        quality_score = 0.70 * quality_score + 0.30 * _score_band_linear(
            roe, bands=[(-10.0, 20.0), (0.0, 40.0), (10.0, 60.0), (15.0, 72.0), (20.0, 82.0), (30.0, 90.0)]
        )
    if roa is not None:
        quality_score = 0.80 * quality_score + 0.20 * _score_band_linear(
            roa, bands=[(-5.0, 25.0), (0.0, 45.0), (4.0, 60.0), (8.0, 72.0), (12.0, 82.0)]
        )
    if nm is not None:
        quality_score = 0.85 * quality_score + 0.15 * _score_band_linear(
            nm, bands=[(-10.0, 25.0), (0.0, 45.0), (5.0, 58.0), (10.0, 68.0), (20.0, 80.0), (30.0, 88.0)]
        )
    if ebitda_m is not None:
        quality_score = 0.88 * quality_score + 0.12 * _score_band_linear(
            ebitda_m, bands=[(-10.0, 30.0), (0.0, 45.0), (10.0, 60.0), (20.0, 72.0), (30.0, 82.0)]
        )

    # Growth adds small bonus/penalty (bounded)
    if rev_g is not None:
        quality_score += _clamp(
            _score_band_linear(rev_g, bands=[(-30.0, -6.0), (-10.0, -3.0), (0.0, 0.0), (10.0, 2.0), (20.0, 4.0), (40.0, 6.0)]),
            -6.0,
            6.0,
            0.0,
        )
    if ni_g is not None:
        quality_score += _clamp(
            _score_band_linear(ni_g, bands=[(-30.0, -6.0), (-10.0, -3.0), (0.0, 0.0), (10.0, 2.0), (20.0, 4.0), (40.0, 6.0)]),
            -6.0,
            6.0,
            0.0,
        )

    if dte is not None:
        if dte > 2.5:
            quality_score -= 12
        elif dte > 1.5:
            quality_score -= 6
        elif 0 <= dte < 0.5:
            quality_score += 6

    quality_score = _clamp(quality_score)
    if quality_score > 75: reasons.append("High Quality")

    # -----------------------------
    # MOMENTUM (0..100)
    # -----------------------------
    momentum_score = 50.0

    if pc is not None:
        momentum_score = 0.85 * momentum_score + 0.15 * _score_band_linear(
            pc, bands=[(-8.0, 30.0), (-3.0, 42.0), (0.0, 50.0), (3.0, 58.0), (8.0, 70.0)]
        )

    if cp is not None:
        if ma20 is not None:
            momentum_score += 5.0 if cp > ma20 else -5.0
        if ma50 is not None:
            momentum_score += 5.0 if cp > ma50 else -5.0

        pos_frac = None
        if pos_52w_pct is not None:
            pos_frac = pos_52w_pct / 100.0
        else:
            pos_frac = _pos52w_frac(cp, low_52w, high_52w)

        if pos_frac is not None:
            if pos_frac > 0.85:
                momentum_score += 10
            elif pos_frac < 0.15:
                momentum_score -= 8

    if rsi is not None:
        if rsi > 75:
            momentum_score -= 10
        elif rsi < 25:
            momentum_score += 10
        elif 50 < rsi < 70:
            momentum_score += 5

    # Advanced Tech: MACD / Trend Boost
    if macd_hist is not None and macd_hist > 0:
        momentum_score += 5.0  # Bullish momentum building
    
    if trend_sig == "UPTREND":
        momentum_score += 10.0
        reasons.append("Uptrend")
    elif trend_sig == "DOWNTREND":
        momentum_score -= 10.0

    # News Impact on Momentum
    if news_score is not None:
        # news_score is often -5 to +5 boost
        if abs(news_score) <= 10:
            momentum_score += news_score * 2.0 
            if news_score > 2: reasons.append("Positive News")
        else:
            # 0-100 scale? map 50->0, 80->6
            nb = (news_score - 50.0) * 0.2
            momentum_score += nb
            if nb > 2: reasons.append("Positive News")

    momentum_score = _clamp(momentum_score)
    if momentum_score > 70: reasons.append("Strong Momentum")

    # -----------------------------
    # RISK (0..100, higher = more risk)
    # -----------------------------
    risk_score = 50.0

    if vol is not None:
        risk_score = 0.70 * risk_score + 0.30 * _score_band_linear(
            vol, bands=[(10.0, 30.0), (15.0, 38.0), (25.0, 50.0), (35.0, 62.0), (50.0, 75.0), (80.0, 90.0)]
        )

    if beta is not None:
        risk_score = 0.80 * risk_score + 0.20 * _score_band_linear(
            beta, bands=[(0.5, 40.0), (0.8, 45.0), (1.0, 50.0), (1.3, 58.0), (1.7, 68.0), (2.2, 78.0)]
        )

    if dte is not None:
        risk_score = 0.80 * risk_score + 0.20 * _score_band_linear(
            dte, bands=[(0.0, 40.0), (0.5, 45.0), (1.0, 52.0), (1.5, 60.0), (2.5, 75.0), (4.0, 88.0)]
        )
        
    # Advanced: Trend Risk
    if trend_sig == "DOWNTREND":
        risk_score += 10.0 # Fighting the trend is risky

    if liq is not None:
        if liq < 30:
            risk_score += 15
            reasons.append("Low Liquidity")
        elif liq > 70:
            risk_score -= 5

    if avg_vol is not None:
        if avg_vol < 100_000:
            risk_score += 6
        elif avg_vol > 5_000_000:
            risk_score -= 2

    if turnover is not None:
        if turnover < 0.5:
            risk_score += 4
        elif turnover > 3.0:
            risk_score -= 2

    if free_float is not None:
        if free_float < 20:
            risk_score += 3
        elif free_float > 60:
            risk_score -= 1

    if dq in {"BAD", "MISSING"}:
        risk_score += 12
    elif dq in {"PARTIAL"}:
        risk_score += 5

    risk_score = _clamp(risk_score)

    # -----------------------------
    # OPPORTUNITY + OVERALL
    # -----------------------------
    risk_penalty = ((risk_score - 50.0) / 50.0) * 14.0  # -14..+14
    opportunity_score = _clamp(0.46 * value_score + 0.30 * momentum_score + 0.24 * quality_score - risk_penalty)
    overall_score = _clamp(0.36 * value_score + 0.32 * quality_score + 0.22 * momentum_score + 0.10 * opportunity_score)

    # -----------------------------
    # Confidence (0..100)
    # -----------------------------
    groups: List[Tuple[float, List[Any]]] = [
        (0.28, [cp, _get_any(q, "previous_close", "prev_close"), avg_vol, _get_any(q, "market_cap", "mkt_cap"), liq]),
        (0.26, [pe, fpe, pb, ps, ev, dy]),
        (0.26, [roe, roa, nm, ebitda_m, rev_g, ni_g]),
        (0.20, [vol, beta, rsi, high_52w, low_52w]),
    ]

    cov = 0.0
    for w, vals in groups:
        present = sum(1 for v in vals if _is_present_number(v))
        total = len(vals) if vals else 1
        cov += w * (present / total) * 100.0

    confidence = cov

    if dq == "MISSING":
        confidence = 0.0
    elif dq == "BAD":
        confidence = min(confidence, 45.0)
    elif dq == "PARTIAL":
        confidence = min(confidence, 70.0)
    elif dq == "OK":
        confidence = min(confidence, 85.0)

    confidence = _clamp(confidence)

    # Blend in provider forecast confidence if present (best-effort)
    if forecast_conf_in is not None:
        fc0 = _clamp(forecast_conf_in, 0.0, 100.0, 50.0)
        confidence = _clamp(0.65 * confidence + 0.35 * fc0)

    # -----------------------------
    # Forecast outputs (1M/3M/12M)
    # -----------------------------
    forecast_out = _compute_forecast(
        current_price=cp,
        fair_value=fair_value,
        upside_percent=upside,
        expected_return_12m_gross_in=expected_return_12m_gross_in,
        forecast_method_in=str(forecast_method_in) if forecast_method_in is not None else None,
        forecast_conf_in=forecast_conf_in,
        confidence_fallback=confidence,
        risk_score=risk_score,
        trend_signal=trend_sig, # Pass trend signal
        forecast_updated_utc=str(forecast_updated_utc) if forecast_updated_utc else None,
    )

    # -----------------------------
    # Recommendation (enum-only + confidence aware + forecast-aware)
    # -----------------------------
    reco = "HOLD"

    # ROI values from forecast_out are already in percent scale (do NOT re-normalize with _to_percent)
    roi3 = _to_float(forecast_out.get("expected_roi_3m", forecast_out.get("expected_return_3m")))
    roi1 = _to_float(forecast_out.get("expected_roi_1m", forecast_out.get("expected_return_1m")))

    if overall_score >= 74 and confidence >= 55 and risk_score <= 78:
        reco = "BUY"
    elif overall_score <= 30 and confidence >= 40:
        reco = "SELL" if risk_score >= 62 else "REDUCE"
    elif overall_score <= 45:
        reco = "REDUCE"
    else:
        reco = "HOLD"

    # Forecast tilt (conservative)
    if confidence >= 55 and roi3 is not None:
        if roi3 >= 6.0 and reco == "HOLD" and overall_score >= 60 and risk_score <= 78:
            reco = "BUY"
        if roi3 <= -4.0 and reco in ("HOLD", "BUY"):
            reco = "REDUCE"

    if confidence < 40:
        if reco == "BUY":
            reco = "HOLD"
        elif reco == "SELL":
            reco = "REDUCE"

    reco = _normalize_reco(reco)

    # Badges (optional, Sheets-friendly)
    rec_badge = _rec_badge_from_reco(reco, overall_score, confidence)
    momentum_badge = _badge_3level(momentum_score, high=70.0, low=45.0, invert=False)
    opportunity_badge = _badge_3level(opportunity_score, high=70.0, low=45.0, invert=False)
    risk_badge = _badge_3level(risk_score, high=62.0, low=40.0, invert=True)

    return AssetScores(
        value_score=_clamp(value_score),
        quality_score=_clamp(quality_score),
        momentum_score=_clamp(momentum_score),
        risk_score=_clamp(risk_score),
        opportunity_score=_clamp(opportunity_score),
        overall_score=_clamp(overall_score),
        recommendation=reco,
        confidence=float(confidence),
        
        scoring_reason=", ".join(reasons) if reasons else "",

        rec_badge=rec_badge,
        momentum_badge=momentum_badge,
        opportunity_badge=opportunity_badge,
        risk_badge=risk_badge,

        # Canonical forecast outputs
        expected_roi_1m=forecast_out.get("expected_roi_1m"),
        expected_roi_3m=forecast_out.get("expected_roi_3m"),
        expected_roi_12m=forecast_out.get("expected_roi_12m"),

        forecast_price_1m=forecast_out.get("forecast_price_1m"),
        forecast_price_3m=forecast_out.get("forecast_price_3m"),
        forecast_price_12m=forecast_out.get("forecast_price_12m"),

        forecast_confidence=forecast_out.get("forecast_confidence"),
        forecast_method=forecast_out.get("forecast_method"),
        forecast_updated_utc=forecast_out.get("forecast_updated_utc"),
        forecast_updated_riyadh=forecast_out.get("forecast_updated_riyadh"),

        # Compat aliases
        expected_return_1m=forecast_out.get("expected_return_1m"),
        expected_return_3m=forecast_out.get("expected_return_3m"),
        expected_return_12m=forecast_out.get("expected_return_12m"),

        expected_price_1m=forecast_out.get("expected_price_1m"),
        expected_price_3m=forecast_out.get("expected_price_3m"),
        expected_price_12m=forecast_out.get("expected_price_12m"),

        confidence_score=forecast_out.get("confidence_score"),

        # audit/debug
        expected_return_gross_1m=forecast_out.get("expected_return_gross_1m"),
        expected_return_gross_3m=forecast_out.get("expected_return_gross_3m"),
        expected_return_gross_12m=forecast_out.get("expected_return_gross_12m"),
    )


# ---------------------------------------------------------------------
# Enrichment (safe, returns new object when possible)
# ---------------------------------------------------------------------
def enrich_with_scores(q: Any, *, prefer_existing_risk_score: bool = True) -> Any:
    """
    Returns a NEW object with scoring + forecast fields populated when possible.
    Supports:
      - Pydantic v2: model_copy(update=...)
      - Pydantic v1: copy(update=...)
      - dict: returns a new dict
      - otherwise: last resort mutates attributes

    prefer_existing_risk_score:
      - If input already has a non-null risk_score, keep it (useful if upstream risk model exists).
    """
    scores = compute_scores(q)

    existing_risk = _to_float(_get(q, "risk_score"))
    risk_out = existing_risk if (prefer_existing_risk_score and existing_risk is not None) else scores.risk_score

    update_data: Dict[str, Any] = {
        # scores
        "value_score": float(scores.value_score),
        "quality_score": float(scores.quality_score),
        "momentum_score": float(scores.momentum_score),
        "risk_score": float(risk_out),
        "opportunity_score": float(scores.opportunity_score),
        "overall_score": float(scores.overall_score),
        "recommendation": str(_normalize_reco(scores.recommendation)),
        "scoring_reason": scores.scoring_reason,
        "confidence": float(scores.confidence),
        "scoring_version": SCORING_ENGINE_VERSION,

        # badges (optional but useful for Sheets)
        "rec_badge": scores.rec_badge,
        "momentum_badge": scores.momentum_badge,
        "opportunity_badge": scores.opportunity_badge,
        "risk_badge": scores.risk_badge,

        # CANONICAL forecast fields (preferred)
        "forecast_price_1m": scores.forecast_price_1m,
        "forecast_price_3m": scores.forecast_price_3m,
        "forecast_price_12m": scores.forecast_price_12m,

        "expected_roi_1m": scores.expected_roi_1m,
        "expected_roi_3m": scores.expected_roi_3m,
        "expected_roi_12m": scores.expected_roi_12m,

        "forecast_confidence": scores.forecast_confidence,
        "forecast_method": scores.forecast_method,
        "forecast_updated_utc": scores.forecast_updated_utc,
        "forecast_updated_riyadh": scores.forecast_updated_riyadh,

        # COMPAT aliases (kept)
        "expected_price_1m": scores.expected_price_1m,
        "expected_price_3m": scores.expected_price_3m,
        "expected_price_12m": scores.expected_price_12m,

        "expected_return_1m": scores.expected_return_1m,
        "expected_return_3m": scores.expected_return_3m,
        "expected_return_12m": scores.expected_return_12m,

        "confidence_score": scores.confidence_score,

        # audit/debug
        "expected_return_gross_1m": scores.expected_return_gross_1m,
        "expected_return_gross_3m": scores.expected_return_gross_3m,
        "expected_return_gross_12m": scores.expected_return_gross_12m,
    }

    # Pydantic v2
    if hasattr(q, "model_copy"):
        try:
            return q.model_copy(update=update_data)
        except Exception:
            pass

    # Pydantic v1
    if hasattr(q, "copy"):
        try:
            return q.copy(update=update_data)
        except Exception:
            pass

    # dict
    if isinstance(q, dict):
        out = dict(q)
        out.update(update_data)
        return out

    # last resort: set attributes in-place
    for k, v in update_data.items():
        try:
            setattr(q, k, v)
        except Exception:
            pass
    return q


__all__ = ["SCORING_ENGINE_VERSION", "AssetScores", "compute_scores", "enrich_with_scores"]
