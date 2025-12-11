"""
core/scoring_engine.py
===============================================
Lightweight scoring engine for assets in the
Ultimate Investment Dashboard.

Produces:
- Value Score
- Quality Score
- Momentum Score
- Opportunity Score
- Overall Score
- Recommendation
- Confidence

All scores are 0–100.

Usage
-----
    from core.enriched_quote import EnrichedQuote
    from core.scoring_engine import enrich_with_scores, compute_scores

    q = EnrichedQuote(symbol="1120.SR", last_price=100.0, ...)
    q_with_scores = enrich_with_scores(q)
    # q_with_scores.value_score, q_with_scores.recommendation, ...

This module is intentionally simple & transparent so we
can easily adjust the logic as you see real results.
"""

from __future__ import annotations

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
    """Clamp numeric value into [lo, hi]."""
    return max(lo, min(hi, value))


def compute_scores(q: EnrichedQuote) -> AssetScores:
    """
    Basic heuristic scoring for one asset.

    Assumptions:
    - Most percentage fields in EnrichedQuote are stored as decimals
      (0.10 = 10%). We convert to percent inside this function where needed.
    - Missing fields do NOT crash the scoring; they just reduce the
      ability to adjust the score (keeps everything safe).
    """

    # ------------------------------------------------------------------
    # VALUE SCORE
    # ------------------------------------------------------------------
    value_score = 50.0

    # Lower P/E is typically better (but avoid negative / insane values).
    if q.pe_ratio is not None and q.pe_ratio > 0:
        pe = q.pe_ratio
        if pe < 10:
            value_score += 20
        elif pe < 20:
            value_score += 10
        elif pe > 35:
            value_score -= 10

    # Dividend yield: attractive in moderate ranges.
    if q.dividend_yield is not None and q.dividend_yield > 0:
        dy = q.dividend_yield * 100.0  # convert decimal to %
        if 0 < dy <= 3:
            value_score += 5
        elif dy <= 6:
            value_score += 10
        elif dy <= 8:
            value_score += 5
        else:
            # Very high yield can be risky
            value_score -= 5

    # Upside vs fair value (if provided).
    if q.upside_percent is not None:
        up = q.upside_percent * 100.0
        if up > 0:
            # +2 points per 10% upside, up to +20
            value_score += min(20.0, up / 10.0 * 2.0)
        elif up < 0:
            # -1.5 points per 10% downside, up to -15
            value_score -= min(15.0, abs(up) / 10.0 * 1.5)

    value_score = _clamp(value_score)

    # ------------------------------------------------------------------
    # QUALITY SCORE
    # ------------------------------------------------------------------
    quality_score = 50.0

    # ROE (return on equity)
    if q.roe is not None:
        roe_pct = q.roe * 100.0
        if roe_pct > 20:
            quality_score += 15
        elif roe_pct > 15:
            quality_score += 10
        elif roe_pct > 10:
            quality_score += 5
        elif roe_pct < 5:
            quality_score -= 10

    # Net margin
    if q.net_margin is not None:
        nm_pct = q.net_margin * 100.0
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

    # Revenue growth
    if q.revenue_growth is not None:
        rg_pct = q.revenue_growth * 100.0
        if rg_pct > 15:
            quality_score += 5
        elif rg_pct > 5:
            quality_score += 3
        elif rg_pct < 0:
            quality_score -= 5

    # Net income growth
    if q.net_income_growth is not None:
        nig_pct = q.net_income_growth * 100.0
        if nig_pct > 15:
            quality_score += 5
        elif nig_pct > 5:
            quality_score += 3
        elif nig_pct < 0:
            quality_score -= 5

    quality_score = _clamp(quality_score)

    # ------------------------------------------------------------------
    # MOMENTUM SCORE
    # ------------------------------------------------------------------
    momentum_score = 50.0

    # Recent daily change
    if q.change_percent is not None:
        cp_pct = q.change_percent * 100.0
        if cp_pct > 2:
            momentum_score += 5
        elif cp_pct < -2:
            momentum_score -= 5

    # Price vs moving averages
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

    # RSI behaviour
    if q.rsi_14 is not None:
        rsi = q.rsi_14
        if rsi > 70:
            # Overbought
            momentum_score -= 8
        elif rsi < 30:
            # Oversold: potential opportunity
            momentum_score += 5
        elif 40 <= rsi <= 60:
            momentum_score += 3

    momentum_score = _clamp(momentum_score)

    # ------------------------------------------------------------------
    # RISK & OPPORTUNITY
    # ------------------------------------------------------------------
    # Simple risk penalty using volatility
    risk_penalty = 0.0
    if q.volatility_30d is not None:
        vol_pct = q.volatility_30d * 100.0
        if vol_pct > 50:
            risk_penalty = 15.0
        elif vol_pct > 30:
            risk_penalty = 8.0
        elif vol_pct < 15:
            risk_penalty = -5.0  # low volatility = more stable

    # Opportunity = weighted blend minus risk penalty
    opportunity_score = (
        0.4 * value_score
        + 0.4 * momentum_score
        + 0.2 * quality_score
        - risk_penalty
    )
    opportunity_score = _clamp(opportunity_score)

    # Overall score: balanced combination
    overall_score = _clamp(
        0.3 * value_score
        + 0.3 * quality_score
        + 0.3 * momentum_score
        + 0.1 * opportunity_score
    )

    # ------------------------------------------------------------------
    # RECOMMENDATION & CONFIDENCE
    # ------------------------------------------------------------------
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
    confidence = 70.0
    missing_penalty = 0.0

    # Penalise missing key fields
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
    Return a NEW EnrichedQuote with score fields and recommendation filled.

    This is what DataEngineV2 calls internally so that ALL assets sent
    to Google Sheets already include Value / Quality / Momentum /
    Opportunity / Recommendation.
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
