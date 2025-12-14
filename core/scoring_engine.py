"""
core/scoring_engine.py
===============================================
Lightweight, transparent scoring engine for the Ultimate Investment Dashboard.

Produces (0–100):
- Value Score
- Quality Score
- Momentum Score
- Opportunity Score
- Overall Score
- Recommendation
- Confidence (internal, returned in AssetScores)

Design goals:
- Deterministic + simple + explainable.
- Works even with partial data (confidence reflects missingness).
- Provider-agnostic: normalizes % values that may appear as decimals or percents.

Usage:
    from core.enriched_quote import EnrichedQuote
    from core.scoring_engine import enrich_with_scores

    q = EnrichedQuote(symbol="1120.SR", current_price=100.0, ...)
    q2 = enrich_with_scores(q)
"""

from __future__ import annotations

from typing import Optional

try:
    from pydantic import BaseModel, Field
except ImportError:  # pragma: no cover
    class BaseModel:  # type: ignore
        pass
    def Field(default, **kwargs):  # type: ignore
        return default

from .enriched_quote import EnrichedQuote


class AssetScores(BaseModel):
    value_score: float = Field(50.0, ge=0, le=100)
    quality_score: float = Field(50.0, ge=0, le=100)
    momentum_score: float = Field(50.0, ge=0, le=100)
    opportunity_score: float = Field(50.0, ge=0, le=100)
    overall_score: float = Field(50.0, ge=0, le=100)
    recommendation: str = Field("HOLD")
    confidence: float = Field(50.0, ge=0, le=100)


def _clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(value)))


def _to_percent(val: Optional[float]) -> Optional[float]:
    """
    Normalize value to percent scale (0–100):
    - If abs(val) <= 1.0, treat as decimal fraction and convert to percent.
    - Else treat as already-percent.
    """
    if val is None:
        return None
    try:
        if val == 0.0:
            return 0.0
        if abs(val) <= 1.0:
            return val * 100.0
        return val
    except Exception:
        return val


def compute_scores(q: EnrichedQuote) -> AssetScores:
    # -----------------------------
    # VALUE
    # -----------------------------
    value_score = 50.0

    pe = getattr(q, "pe_ttm", None)
    if pe is not None and pe > 0:
        if pe < 10:
            value_score += 20
        elif pe < 20:
            value_score += 10
        elif pe > 40:
            value_score -= 15
        elif pe > 25:
            value_score -= 5

    pb = getattr(q, "pb", None)
    if pb is not None and pb > 0:
        if pb < 1.0:
            value_score += 10
        elif pb > 5.0:
            value_score -= 5

    dy = _to_percent(getattr(q, "dividend_yield", None))
    if dy is not None and dy > 0:
        if 2.0 <= dy <= 6.0:
            value_score += 10
        elif dy > 6.0:
            value_score += 5
        elif dy < 1.0:
            value_score -= 2

    up = getattr(q, "upside_percent", None)
    if up is not None:
        up_pct = _to_percent(up) if abs(up) <= 1.0 else up
        if up_pct is not None:
            if up_pct > 0:
                value_score += min(20.0, up_pct / 2.0)
            elif up_pct < 0:
                value_score -= min(15.0, abs(up_pct) / 2.0)

    value_score = _clamp(value_score)

    # -----------------------------
    # QUALITY
    # -----------------------------
    quality_score = 50.0

    roe = _to_percent(getattr(q, "roe", None))
    if roe is not None:
        if roe > 20:
            quality_score += 20
        elif roe > 15:
            quality_score += 15
        elif roe > 10:
            quality_score += 5
        elif roe < 0:
            quality_score -= 15

    nm = _to_percent(getattr(q, "net_margin", None))
    if nm is not None:
        if nm > 20:
            quality_score += 10
        elif nm > 10:
            quality_score += 5
        elif nm < 0:
            quality_score -= 10

    dte = getattr(q, "debt_to_equity", None)
    if dte is not None:
        if dte > 2.5:
            quality_score -= 15
        elif dte > 1.5:
            quality_score -= 5
        elif 0 <= dte < 0.5:
            quality_score += 10

    rev_g = _to_percent(getattr(q, "revenue_growth", None))
    if rev_g is not None:
        if rev_g > 15:
            quality_score += 5
        elif rev_g < 0:
            quality_score -= 5

    ni_g = _to_percent(getattr(q, "net_income_growth", None))
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

    pc = getattr(q, "percent_change", None)
    if pc is not None:
        if pc > 3:
            momentum_score += 5
        elif pc > 0:
            momentum_score += 2
        elif pc < -3:
            momentum_score -= 5

    cp = getattr(q, "current_price", None)
    ma20 = getattr(q, "ma20", None)
    ma50 = getattr(q, "ma50", None)
    high_52w = getattr(q, "high_52w", None)
    low_52w = getattr(q, "low_52w", None)

    if cp is not None:
        if ma20 is not None:
            momentum_score += 5 if cp > ma20 else -5
        if ma50 is not None:
            momentum_score += 5 if cp > ma50 else -5

        if high_52w is not None and low_52w is not None and high_52w != low_52w:
            pos = (cp - low_52w) / (high_52w - low_52w)
            if pos > 0.8:
                momentum_score += 10
            elif pos < 0.2:
                momentum_score -= 10

    rsi = getattr(q, "rsi_14", None)
    if rsi is not None:
        if rsi > 75:
            momentum_score -= 10
        elif rsi < 25:
            momentum_score += 10
        elif 50 < rsi < 70:
            momentum_score += 5

    momentum_score = _clamp(momentum_score)

    # -----------------------------
    # RISK PENALTY
    # -----------------------------
    risk_penalty = 0.0
    vol = _to_percent(getattr(q, "volatility_30d", None))
    if vol is not None:
        if vol > 50:
            risk_penalty = 15.0
        elif vol > 30:
            risk_penalty = 8.0
        elif vol < 15:
            risk_penalty = -5.0

    # -----------------------------
    # OPPORTUNITY + OVERALL
    # -----------------------------
    opportunity_score = _clamp(0.4 * value_score + 0.4 * momentum_score + 0.2 * quality_score - risk_penalty)
    overall_score = _clamp(0.3 * value_score + 0.3 * quality_score + 0.3 * momentum_score + 0.1 * opportunity_score)

    # -----------------------------
    # RECOMMENDATION
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
    # CONFIDENCE
    # -----------------------------
    confidence = 100.0
    missing = 0
    key_fields = [
        getattr(q, "pe_ttm", None),
        getattr(q, "market_cap", None),
        getattr(q, "roe", None),
        getattr(q, "volatility_30d", None),
        getattr(q, "current_price", None),
    ]
    for f in key_fields:
        if f is None:
            missing += 1
    confidence -= (missing * 15.0)

    dq = str(getattr(q, "data_quality", "") or "").upper()
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


def enrich_with_scores(q: EnrichedQuote) -> EnrichedQuote:
    """
    Return a NEW EnrichedQuote with scoring fields filled.
    Uses Pydantic v2 model_copy when available.
    """
    scores = compute_scores(q)
    update_data = {
        "value_score": scores.value_score,
        "quality_score": scores.quality_score,
        "momentum_score": scores.momentum_score,
        "opportunity_score": scores.opportunity_score,
        "overall_score": scores.overall_score,
        "recommendation": scores.recommendation,
    }
    if hasattr(q, "model_copy"):
        return q.model_copy(update=update_data)
    return q.copy(update=update_data)
