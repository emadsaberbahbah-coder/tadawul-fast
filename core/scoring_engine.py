# core/scoring_engine.py
"""
core/scoring_engine.py
===========================================================
Lightweight, transparent scoring engine for the Ultimate Investment Dashboard.

Produces (0–100):
- value_score
- quality_score
- momentum_score
- opportunity_score
- overall_score
- recommendation
- confidence (internal, returned in AssetScores)

Design goals
✅ Deterministic + simple + explainable
✅ Works with partial data (confidence reflects missingness)
✅ Provider-agnostic: normalizes % values that may appear as decimals or percents
✅ Safe imports: does NOT require EnrichedQuote at import time (avoids circular imports)
✅ Works with either EnrichedQuote or UnifiedQuote-like objects (duck-typing)

Usage:
    from core.scoring_engine import enrich_with_scores
    q2 = enrich_with_scores(q)  # returns NEW object when possible
"""

from __future__ import annotations

from typing import Any, Dict, Optional

# Pydantic best-effort
try:
    from pydantic import BaseModel, Field, ConfigDict  # type: ignore
    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    from pydantic import BaseModel, Field  # type: ignore
    ConfigDict = None  # type: ignore
    _PYDANTIC_V2 = False


class AssetScores(BaseModel):
    """
    Returned by compute_scores() for internal usage / debugging.
    """
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")  # type: ignore
    value_score: float = Field(50.0, ge=0, le=100)
    quality_score: float = Field(50.0, ge=0, le=100)
    momentum_score: float = Field(50.0, ge=0, le=100)
    opportunity_score: float = Field(50.0, ge=0, le=100)
    overall_score: float = Field(50.0, ge=0, le=100)
    recommendation: str = Field("HOLD")
    confidence: float = Field(50.0, ge=0, le=100)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    try:
        return max(lo, min(hi, float(x)))
    except Exception:
        return 50.0


def _get(obj: Any, name: str, default: Any = None) -> Any:
    """
    Safe attribute getter that also supports dict-like objects.
    """
    if obj is None:
        return default
    try:
        if isinstance(obj, dict):
            return obj.get(name, default)
        return getattr(obj, name, default)
    except Exception:
        return default


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, bool):
            return None
        v = float(x)
        return v
    except Exception:
        return None


def _to_percent(x: Any) -> Optional[float]:
    """
    Normalize any percent-like value to 0..100 scale:
    - If abs(val) <= 1 -> treat as fraction and multiply by 100
    - Else assume it's already percent
    """
    v = _to_float(x)
    if v is None:
        return None
    if v == 0.0:
        return 0.0
    if abs(v) <= 1.0:
        return v * 100.0
    return v


def _pos52w(cp: Optional[float], low_52w: Optional[float], high_52w: Optional[float]) -> Optional[float]:
    if cp is None or low_52w is None or high_52w is None:
        return None
    rng = high_52w - low_52w
    if rng == 0:
        return None
    return (cp - low_52w) / rng  # 0..1


# ---------------------------------------------------------------------
# Core scoring
# ---------------------------------------------------------------------
def compute_scores(q: Any) -> AssetScores:
    """
    Computes scores for an EnrichedQuote/UnifiedQuote-like object (duck typing).
    Never raises.
    """
    # -----------------------------
    # VALUE
    # -----------------------------
    value_score = 50.0

    pe = _to_float(_get(q, "pe_ttm"))
    if pe is not None and pe > 0:
        if pe < 10:
            value_score += 20
        elif pe < 20:
            value_score += 10
        elif pe > 40:
            value_score -= 15
        elif pe > 25:
            value_score -= 5

    pb = _to_float(_get(q, "pb"))
    if pb is not None and pb > 0:
        if pb < 1.0:
            value_score += 10
        elif pb > 5.0:
            value_score -= 5

    dy = _to_percent(_get(q, "dividend_yield"))
    if dy is not None and dy > 0:
        if 2.0 <= dy <= 6.0:
            value_score += 10
        elif dy > 6.0:
            value_score += 5
        elif dy < 1.0:
            value_score -= 2

    upside = _to_percent(_get(q, "upside_percent"))
    if upside is not None:
        if upside > 0:
            value_score += min(20.0, upside / 2.0)
        elif upside < 0:
            value_score -= min(15.0, abs(upside) / 2.0)

    value_score = _clamp(value_score)

    # -----------------------------
    # QUALITY
    # -----------------------------
    quality_score = 50.0

    roe = _to_percent(_get(q, "roe"))
    if roe is not None:
        if roe > 20:
            quality_score += 20
        elif roe > 15:
            quality_score += 15
        elif roe > 10:
            quality_score += 5
        elif roe < 0:
            quality_score -= 15

    nm = _to_percent(_get(q, "net_margin"))
    if nm is not None:
        if nm > 20:
            quality_score += 10
        elif nm > 10:
            quality_score += 5
        elif nm < 0:
            quality_score -= 10

    dte = _to_float(_get(q, "debt_to_equity"))
    if dte is not None:
        if dte > 2.5:
            quality_score -= 15
        elif dte > 1.5:
            quality_score -= 5
        elif 0 <= dte < 0.5:
            quality_score += 10

    rev_g = _to_percent(_get(q, "revenue_growth"))
    if rev_g is not None:
        if rev_g > 15:
            quality_score += 5
        elif rev_g < 0:
            quality_score -= 5

    ni_g = _to_percent(_get(q, "net_income_growth"))
    if ni_g is not None:
        if ni_g > 15:
            quality_score += 5
        elif ni_g < 0:
            quality_score -= 5

    quality_score = _clamp(quality_score)

    # -----------------------------
    # MOMENTUM
    # -----------------------------
    momentum_score = 50.0

    pc = _to_float(_get(q, "percent_change"))
    if pc is not None:
        if pc > 3:
            momentum_score += 5
        elif pc > 0:
            momentum_score += 2
        elif pc < -3:
            momentum_score -= 5

    cp = _to_float(_get(q, "current_price"))
    ma20 = _to_float(_get(q, "ma20"))
    ma50 = _to_float(_get(q, "ma50"))
    high_52w = _to_float(_get(q, "high_52w"))
    low_52w = _to_float(_get(q, "low_52w"))

    if cp is not None:
        if ma20 is not None:
            momentum_score += 5 if cp > ma20 else -5
        if ma50 is not None:
            momentum_score += 5 if cp > ma50 else -5

        pos = _pos52w(cp, low_52w, high_52w)
        if pos is not None:
            if pos > 0.8:
                momentum_score += 10
            elif pos < 0.2:
                momentum_score -= 10

    rsi = _to_float(_get(q, "rsi_14"))
    if rsi is not None:
        if rsi > 75:
            momentum_score -= 10
        elif rsi < 25:
            momentum_score += 10
        elif 50 < rsi < 70:
            momentum_score += 5

    momentum_score = _clamp(momentum_score)

    # -----------------------------
    # RISK penalty (simple)
    # -----------------------------
    risk_penalty = 0.0
    vol = _to_percent(_get(q, "volatility_30d"))
    if vol is not None:
        if vol > 50:
            risk_penalty = 15.0
        elif vol > 30:
            risk_penalty = 8.0
        elif vol < 15:
            risk_penalty = -5.0

    # Optional explicit risk_score from engine (if present)
    risk_score = _to_float(_get(q, "risk_score"))
    if risk_score is not None:
        # Translate risk_score (0..100 high=more risk) into penalty (-5..+15)
        risk_penalty += _clamp((risk_score - 50.0) / 50.0 * 10.0, -5.0, 15.0)

    # -----------------------------
    # OPPORTUNITY + OVERALL
    # -----------------------------
    opportunity_score = _clamp(0.4 * value_score + 0.4 * momentum_score + 0.2 * quality_score - risk_penalty)
    overall_score = _clamp(0.3 * value_score + 0.3 * quality_score + 0.3 * momentum_score + 0.1 * opportunity_score)

    # -----------------------------
    # Recommendation
    # -----------------------------
    if overall_score >= 80:
        recommendation = "STRONG BUY"
    elif overall_score >= 65:
        recommendation = "BUY"
    elif overall_score <= 35:
        recommendation = "SELL"
    elif overall_score <= 50:
        recommendation = "REDUCE"
    else:
        recommendation = "HOLD"

    # -----------------------------
    # Confidence (data completeness + data_quality)
    # -----------------------------
    confidence = 100.0
    key_fields = [
        _get(q, "current_price"),
        _get(q, "previous_close"),
        _get(q, "market_cap"),
        _get(q, "pe_ttm"),
        _get(q, "roe"),
        _get(q, "volatility_30d"),
    ]
    missing = sum(1 for f in key_fields if f is None)
    confidence -= missing * 12.0

    dq = str(_get(q, "data_quality", "") or "").upper()
    if dq == "MISSING":
        confidence = 0.0
    elif dq == "PARTIAL":
        confidence = min(confidence, 70.0)

    confidence = _clamp(confidence)

    return AssetScores(
        value_score=_clamp(value_score),
        quality_score=_clamp(quality_score),
        momentum_score=_clamp(momentum_score),
        opportunity_score=_clamp(opportunity_score),
        overall_score=_clamp(overall_score),
        recommendation=recommendation,
        confidence=confidence,
    )


# ---------------------------------------------------------------------
# Enrichment (safe, returns new object when possible)
# ---------------------------------------------------------------------
def enrich_with_scores(q: Any) -> Any:
    """
    Returns a NEW object with scoring fields populated when possible.
    Supports Pydantic v2 model_copy, Pydantic v1 copy(update=...), dicts, or falls back to mutating attributes.
    """
    scores = compute_scores(q)
    update_data: Dict[str, Any] = {
        "value_score": scores.value_score,
        "quality_score": scores.quality_score,
        "momentum_score": scores.momentum_score,
        "opportunity_score": scores.opportunity_score,
        "overall_score": scores.overall_score,
        "recommendation": scores.recommendation,
        "confidence": scores.confidence,  # keep if your model supports it
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


__all__ = ["AssetScores", "compute_scores", "enrich_with_scores"]
