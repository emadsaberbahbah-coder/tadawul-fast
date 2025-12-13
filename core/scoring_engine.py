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

    q = EnrichedQuote(symbol="1120.SR", current_price=100.0, ...)
    q_with_scores = enrich_with_scores(q)
    # q_with_scores.value_score, q_with_scores.recommendation, ...

This module is intentionally simple & transparent so we
can easily adjust the logic as you see real results.
"""

from __future__ import annotations

from typing import Optional

try:
    from pydantic import BaseModel, Field
except ImportError:
    # Fallback for environments without pydantic installed
    class BaseModel:
        pass
    def Field(default, **kwargs):
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
    """Clamp numeric value into [lo, hi]."""
    return max(lo, min(hi, value))


def _to_percent(val: Optional[float]) -> Optional[float]:
    """
    Normalize value to a percentage (0-100 scale).
    Heuristic:
    - If None, return None.
    - If abs(val) <= 1.0 (e.g. 0.15), treat as decimal -> return 15.0
    - If abs(val) > 1.0 (e.g. 15.0), treat as percent -> return 15.0
    
    This handles inconsistencies between providers (FMP often returns 0.05, EODHD 5.0).
    """
    if val is None:
        return None
    if abs(val) <= 1.0 and val != 0.0:
        return val * 100.0
    return val


def compute_scores(q: EnrichedQuote) -> AssetScores:
    """
    Basic heuristic scoring for one asset.
    Strictly aligned with core.data_engine_v2.UnifiedQuote fields.
    """

    # ------------------------------------------------------------------
    # VALUE SCORE
    # ------------------------------------------------------------------
    value_score = 50.0

    # P/E Ratio (pe_ttm)
    # Lower is typically better (but > 0)
    if q.pe_ttm is not None and q.pe_ttm > 0:
        pe = q.pe_ttm
        if pe < 10:
            value_score += 20
        elif pe < 20:
            value_score += 10
        elif pe > 40:
            value_score -= 15
        elif pe > 25:
            value_score -= 5
    
    # P/B Ratio (pb)
    if q.pb is not None and q.pb > 0:
        if q.pb < 1.0:
            value_score += 10
        elif q.pb > 5.0:
            value_score -= 5

    # Dividend yield
    dy = _to_percent(q.dividend_yield)
    if dy is not None and dy > 0:
        if 2.0 <= dy <= 6.0:
            value_score += 10
        elif dy > 6.0:
            value_score += 5 # High yield, good but maybe risky
        elif dy < 1.0:
            value_score -= 2

    # Upside vs fair value (if provided)
    # Note: upside_percent is usually pre-calculated in data engine V2 if target prices exist
    if q.upside_percent is not None:
        up = q.upside_percent # Assuming this is already a percentage-like number (e.g. 20 for 20%) or decimal
        # Normalize if decimal
        if abs(up) < 1.0 and up != 0: up = up * 100.0
        
        if up > 0:
            value_score += min(20.0, up / 2.0) # Cap contribution
        elif up < 0:
            value_score -= min(15.0, abs(up) / 2.0)

    value_score = _clamp(value_score)

    # ------------------------------------------------------------------
    # QUALITY SCORE
    # ------------------------------------------------------------------
    quality_score = 50.0

    # ROE (Return on Equity)
    roe = _to_percent(q.roe)
    if roe is not None:
        if roe > 20:
            quality_score += 20
        elif roe > 15:
            quality_score += 15
        elif roe > 10:
            quality_score += 5
        elif roe < 0:
            quality_score -= 15

    # Net Margin
    nm = _to_percent(q.net_margin)
    if nm is not None:
        if nm > 20:
            quality_score += 10
        elif nm > 10:
            quality_score += 5
        elif nm < 0:
            quality_score -= 10

    # Debt to Equity
    if q.debt_to_equity is not None:
        dte = q.debt_to_equity
        if dte > 2.5:
            quality_score -= 15
        elif dte > 1.5:
            quality_score -= 5
        elif 0 <= dte < 0.5:
            quality_score += 10

    # Growth (Revenue & Net Income)
    rev_g = _to_percent(q.revenue_growth)
    if rev_g is not None:
        if rev_g > 15: quality_score += 5
        elif rev_g < 0: quality_score -= 5
        
    ni_g = _to_percent(q.net_income_growth)
    if ni_g is not None:
        if ni_g > 15: quality_score += 5
        elif ni_g < 0: quality_score -= 5

    quality_score = _clamp(quality_score)

    # ------------------------------------------------------------------
    # MOMENTUM SCORE
    # ------------------------------------------------------------------
    momentum_score = 50.0

    # Percent Change (Daily)
    # data_engine_v2 uses 'percent_change'
    if q.percent_change is not None:
        pc = q.percent_change
        if pc > 3: momentum_score += 5
        elif pc > 0: momentum_score += 2
        elif pc < -3: momentum_score -= 5

    # Price vs Moving Averages (MA20, MA50)
    cp = q.current_price
    if cp is not None:
        if q.ma20 is not None:
            if cp > q.ma20: momentum_score += 5
            else: momentum_score -= 5
        
        if q.ma50 is not None:
            if cp > q.ma50: momentum_score += 5
            else: momentum_score -= 5
            
        # 52 Week Position
        if q.high_52w is not None and q.low_52w is not None and q.high_52w != q.low_52w:
            pos = (cp - q.low_52w) / (q.high_52w - q.low_52w)
            if pos > 0.8: momentum_score += 10
            elif pos < 0.2: momentum_score -= 10

    # RSI (Relative Strength Index)
    if q.rsi_14 is not None:
        rsi = q.rsi_14
        if rsi > 75:
            momentum_score -= 10 # Overbought
        elif rsi < 25:
            momentum_score += 10 # Oversold (mean reversion potential)
        elif 50 < rsi < 70:
            momentum_score += 5  # Strong trend

    momentum_score = _clamp(momentum_score)

    # ------------------------------------------------------------------
    # RISK & OPPORTUNITY
    # ------------------------------------------------------------------
    # Risk penalty using volatility
    risk_penalty = 0.0
    if q.volatility_30d is not None:
        vol = _to_percent(q.volatility_30d)
        if vol is not None:
            if vol > 50: risk_penalty = 15.0
            elif vol > 30: risk_penalty = 8.0
            elif vol < 15: risk_penalty = -5.0

    # Opportunity = Weighted blend minus risk
    opportunity_score = (
        0.4 * value_score
        + 0.4 * momentum_score
        + 0.2 * quality_score
        - risk_penalty
    )
    opportunity_score = _clamp(opportunity_score)

    # Overall score
    overall_score = _clamp(
        0.3 * value_score
        + 0.3 * quality_score
        + 0.3 * momentum_score
        + 0.1 * opportunity_score
    )

    # ------------------------------------------------------------------
    # RECOMMENDATION & CONFIDENCE
    # ------------------------------------------------------------------
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

    # Confidence based on data completeness
    confidence = 100.0
    missing_count = 0
    # Key fields to check
    check_list = [q.pe_ttm, q.market_cap, q.roe, q.volatility_30d, q.current_price]
    for field in check_list:
        if field is None:
            missing_count += 1
    
    confidence -= (missing_count * 15.0)
    
    if q.data_quality == "MISSING":
        confidence = 0.0
    elif q.data_quality == "PARTIAL":
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
    Return a NEW EnrichedQuote with score fields and recommendation filled.

    This uses Pydantic v2 'model_copy' if available, or 'copy' fallback.
    """
    scores = compute_scores(q)
    
    update_data = {
        "value_score": scores.value_score,
        "quality_score": scores.quality_score,
        "momentum_score": scores.momentum_score,
        "opportunity_score": scores.opportunity_score,
        "overall_score": scores.overall_score,
        "recommendation": scores.recommendation,
        # 'confidence' is typically internal, but could be added to 'error' or 'data_quality' logic if needed
    }
    
    if hasattr(q, "model_copy"):
        return q.model_copy(update=update_data)
    else:
        # Pydantic v1 fallback or dict
        return q.copy(update=update_data)
