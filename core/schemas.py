"""
core/scoring_engine.py
===============================================
Lightweight scoring engine for assets.

Produces:
- Value Score
- Quality Score
- Momentum Score
- Opportunity Score
- Overall Score
- Recommendation
- Confidence

All scores 0–100.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

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
    return max(lo, min(hi, value))


def compute_scores(q: EnrichedQuote) -> AssetScores:
    """
    Basic heuristic scoring.

    This is intentionally simple and transparent. We can refine
    the formulas later based on your preferences, but this
    already gives meaningful differentiation between assets.
    """

    # ----------------------
    # VALUE SCORE
    # ----------------------
    value_score = 50.0

    # Lower P/E tends to be more attractive (but avoid negative)
    if q.pe_ratio is not None and q.pe_ratio > 0:
        if q.pe_ratio < 10:
            value_score += 20
        elif q.pe_ratio < 20:
            value_score += 10
        elif q.pe_ratio > 35:
            value_score -= 10

    # Higher dividend yield (up to ~8%) is attractive
    if q.dividend_yield is not None and q.dividend_yield > 0:
        dy = q.dividend_yield * 100  # if stored as decimal
        if dy > 0 and dy <= 3:
            value_score += 5
        elif dy <= 6:
            value_score += 10
        elif dy <= 8:
            value_score += 5
        else:
            value_score -= 5  # too high can be risk / unsustainable

    # Upside vs fair value; if you supply decimal, you may want to
    # adjust this function accordingly.
    if q.upside_percent is not None:
        up = q.upside_percent * 100  # convert decimal to percentage
        if up > 0:
            value_score += min(20, up / 10)  # +2 points per 10% upside up to +20
        elif up < 0:
            value_score -= min(15, abs(up) / 10)

    value_score = _clamp(value_score)

    # ----------------------
    # QUALITY SCORE
    # ----------------------
    quality_score = 50.0

    # ROE
    if q.roe is not None:
        roe_pct = q.roe * 100
        if roe_pct > 15:
            quality_score += 15
        elif roe_pct > 10:
            quality_score += 8
        elif roe_pct < 5:
            quality_score -= 10

    # Net margin
    if q.net_margin is not None:
        nm_pct = q.net_margin * 100
        if nm_pct > 20:
            quality_score += 10
        elif nm_pct > 10:
            quality_score += 5
        elif nm_pct < 5:
            quality_score -= 5

    # Debt to equity
    if q.debt_to_equity is not None:
        dte = q.debt_to_equity
        if dte > 2.0:
            quality_score -= 10
        elif dte > 1.0:
            quality_score -= 5
        elif 0 < dte < 0.5:
            quality_score += 5

    # Revenue & income growth
    if q.revenue_growth is not None:
        rg_pct = q.revenue_growth * 100
        if rg_pct > 10:
            quality_score += 5
        elif rg_pct < 0:
            quality_score -= 5

    if q.net_income_growth is not None:
        nig_pct = q.net_income_growth * 100
        if nig_pct > 10:
            quality_score += 5
        elif nig_pct < 0:
            quality_score -= 5

    quality_score = _clamp(quality_score)

    # ----------------------
    # MOMENTUM SCORE
    # ----------------------
    momentum_score = 50.0

    # Recent price change
    if q.change_percent is not None:
        cp_pct = q.change_percent * 100
        if cp_pct > 2:
            momentum_score += 5
        elif cp_pct < -2:
            momentum_score -= 5

    # Trend vs moving averages if available
    if q.last_price is not None and q.ma_20d is not None:
        if q.last_price > q.ma_20d:
            momentum_score += 5
        else:
            momentum_score -= 5

    if q.last_price is not None and q.ma_50d is not None:
        if q.last_price > q.ma_50d:
            momentum_score += 5
        else:
            momentum_score -= 5

    # RSI: penalize overbought / reward healthy range
    if q.rsi_14 is not None:
        if q.rsi_14 > 70:
            momentum_score -= 8
        elif q.rsi_14 < 30:
            # Could be oversold opportunity; slight boost
            momentum_score += 5
        elif 40 <= q.rsi_14 <= 60:
            momentum_score += 3

    momentum_score = _clamp(momentum_score)

    # ----------------------
    # RISK & OPPORTUNITY
    # ----------------------
    # Simple risk approximation using volatility
    risk_penalty = 0.0
    if q.volatility_30d is not None:
        vol_pct = q.volatility_30d * 100
        if vol_pct > 50:
            risk_penalty = 15
        elif vol_pct > 30:
            risk_penalty = 8
        elif vol_pct < 15:
            # low volatility = stable
            risk_penalty = -5

    # Opportunity = weighted blend of Value, Momentum minus risk penalty
    opportunity_score = (
        0.4 * value_score
        + 0.4 * momentum_score
        + 0.2 * quality_score
        - risk_penalty
    )
    opportunity_score = _clamp(opportunity_score)

    # Overall score: balanced blend
    overall_score = _clamp(
        0.3 * value_score + 0.3 * quality_score + 0.3 * momentum_score + 0.1 * opportunity_score
    )

    # ----------------------
    # RECOMMENDATION & CONFIDENCE
    # ----------------------
    if overall_score >= 80 and opportunity_score >= 75:
        recommendation = "STRONG BUY"
    elif overall_score >= 70 and opportunity_score >= 65:
        recommendation = "BUY"
    elif overall_score <= 40 and opportunity_score <= 40:
        recommendation = "SELL"
    elif overall_score <= 50 and opportunity_score <= 50:
        recommendation = "REDUCE"
    else:
        recommendation = "HOLD"

    # Confidence based on data completeness
    # (very simple: start 50, adjust if key fields missing)
    confidence = 70.0
    # Penalize missing crucial fields (market_cap, pe_ratio, roe, volatility, etc.)
    missing_penalty = 0
    if q.market_cap is None:
        missing_penalty += 5
    if q.pe_ratio is None:
        missing_penalty += 5
    if q.roe is None:
        missing_penalty += 5
    if q.volatility_30d is None:
        missing_penalty += 5
    if q.data_quality and q.data_quality.upper() == "MISSING":
        missing_penalty += 20

    confidence = _clamp(confidence - missing_penalty)

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
    Return a NEW EnrichedQuote with scores & recommendation fields filled.

    Usage:
        quote = get_raw_quote(...)
        quote = enrich_with_scores(quote)
        # pass to row builder or sheet endpoint
    """
    scores = compute_scores(q)
    return q.copy(
        update=dict(
            value_score=scores.value_score,
            quality_score=scores.quality_score,
            momentum_score=scores.momentum_score,
            opportunity_score=scores.opportunity_score,
            recommendation=scores.recommendation,
        )
    )
