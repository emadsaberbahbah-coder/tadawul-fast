"""
core/scoring_engine.py
===============================================
Lightweight scoring engine for assets in the
Ultimate Investment Dashboard.

Produces (0–100):
- Value Score
- Quality Score
- Momentum Score
- Opportunity Score
- Overall Score
- Recommendation
- Confidence (0–100)

Design goals
------------
- Extremely defensive: NEVER raises due to missing fields.
- Provider-agnostic: normalizes decimals vs percents (0.05 vs 5.0).
- Aligned with core.data_engine_v2 / core.enriched_quote field names.
- Transparent heuristics (easy to adjust).

Usage
-----
from core.enriched_quote import EnrichedQuote
from core.scoring_engine import enrich_with_scores

q = EnrichedQuote(symbol="1120.SR", current_price=100.0, pe_ttm=12, roe=0.18)
q2 = enrich_with_scores(q)
"""

from __future__ import annotations

from typing import Any, Dict, Optional

try:
    # Works for pydantic v2 as well
    from pydantic import BaseModel, Field
except Exception:  # pragma: no cover
    # Minimal fallback if pydantic is unavailable (avoid crash)
    class BaseModel:  # type: ignore
        pass

    def Field(default, **kwargs):  # type: ignore
        return default

from .enriched_quote import EnrichedQuote


# =============================================================================
# Output model
# =============================================================================
class AssetScores(BaseModel):
    value_score: float = Field(50.0, ge=0, le=100)
    quality_score: float = Field(50.0, ge=0, le=100)
    momentum_score: float = Field(50.0, ge=0, le=100)
    opportunity_score: float = Field(50.0, ge=0, le=100)
    overall_score: float = Field(50.0, ge=0, le=100)
    recommendation: str = Field("HOLD")
    confidence: float = Field(50.0, ge=0, le=100)


# =============================================================================
# Small helpers (defensive)
# =============================================================================
def _clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    try:
        return max(lo, min(hi, float(value)))
    except Exception:
        return 50.0


def _get(q: Any, field: str, default=None):
    try:
        return getattr(q, field)
    except Exception:
        return default


def _as_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        # handle "5%" / "1,234" etc.
        if isinstance(v, str):
            s = v.strip().replace(",", "").replace("%", "")
            if s == "" or s == "-" or s.lower() == "na":
                return None
            return float(s)
        return float(v)
    except Exception:
        return None


def _to_percent(val: Optional[float]) -> Optional[float]:
    """
    Normalize value into percent scale (0–100).
    Heuristic:
    - None -> None
    - abs(val) <= 1.0 : treat as decimal fraction (0.15 -> 15.0)
    - abs(val) > 1.0  : treat as already percent (15.0 -> 15.0)
    """
    if val is None:
        return None
    try:
        v = float(val)
    except Exception:
        return None
    # keep exact zero as zero
    if v == 0.0:
        return 0.0
    if abs(v) <= 1.0:
        return v * 100.0
    return v


def _normalize_pct_like(val: Optional[float]) -> Optional[float]:
    """
    Same as _to_percent, but allows small daily changes like 0.4 (meaning 0.4%)
    to pass unchanged if you consider it already percent.
    Many feeds give daily % as percent already (e.g., 0.42 means 0.42%).
    We treat <= 1.0 as decimal ONLY if it's clearly a fraction (like 0.05 for 5%).
    So:
      0.05 -> 5.0
      0.4  -> 0.4  (likely already percent)
    """
    if val is None:
        return None
    v = _as_float(val)
    if v is None:
        return None
    if v == 0.0:
        return 0.0
    # if very small, assume fraction (0.01=1%)
    if abs(v) < 0.2:
        return v * 100.0
    return v


def _compute_upside_percent(q: EnrichedQuote) -> Optional[float]:
    """
    If upside_percent is missing but fair_value and current_price exist,
    compute upside%.
    """
    up = _as_float(_get(q, "upside_percent", None))
    if up is not None:
        # normalize fraction if needed
        return _to_percent(up) if abs(up) <= 1.0 else up

    fv = _as_float(_get(q, "fair_value", None))
    cp = _as_float(_get(q, "current_price", None)) or _as_float(_get(q, "last_price", None))
    if fv is None or cp is None or cp == 0:
        return None
    return ((fv - cp) / cp) * 100.0


def _data_quality_to_confidence_cap(label: Optional[str]) -> float:
    """
    Caps confidence based on data_quality label.
    """
    if not label:
        return 80.0
    dq = str(label).strip().upper()
    if dq == "FULL":
        return 100.0
    if dq == "PARTIAL":
        return 70.0
    if dq == "STALE":
        return 55.0
    if dq == "MISSING":
        return 0.0
    return 80.0


# =============================================================================
# Main scoring
# =============================================================================
def compute_scores(q: EnrichedQuote) -> AssetScores:
    """
    Basic heuristic scoring for one asset.
    Uses only fields that may exist; never raises if fields are missing.
    """

    # Pull fields defensively
    pe_ttm = _as_float(_get(q, "pe_ttm", None))
    pb = _as_float(_get(q, "pb", None))
    dividend_yield = _as_float(_get(q, "dividend_yield", None))
    roe = _as_float(_get(q, "roe", None))
    roa = _as_float(_get(q, "roa", None))
    net_margin = _as_float(_get(q, "net_margin", None))
    debt_to_equity = _as_float(_get(q, "debt_to_equity", None))
    revenue_growth = _as_float(_get(q, "revenue_growth", None))
    net_income_growth = _as_float(_get(q, "net_income_growth", None))

    current_price = _as_float(_get(q, "current_price", None)) or _as_float(_get(q, "last_price", None))
    percent_change = _as_float(_get(q, "percent_change", None)) or _as_float(_get(q, "change_percent", None))

    ma20 = _as_float(_get(q, "ma20", None))
    ma50 = _as_float(_get(q, "ma50", None))

    high_52w = _as_float(_get(q, "high_52w", None))
    low_52w = _as_float(_get(q, "low_52w", None))

    rsi_14 = _as_float(_get(q, "rsi_14", None))
    volatility_30d = _as_float(_get(q, "volatility_30d", None))

    data_quality = str(_get(q, "data_quality", "MISSING") or "MISSING").strip().upper()

    upside_percent = _compute_upside_percent(q)

    # ------------------------------------------------------------------
    # VALUE SCORE
    # ------------------------------------------------------------------
    value_score = 50.0

    # P/E
    if pe_ttm is not None and pe_ttm > 0:
        if pe_ttm < 10:
            value_score += 20
        elif pe_ttm < 20:
            value_score += 10
        elif pe_ttm > 40:
            value_score -= 15
        elif pe_ttm > 25:
            value_score -= 5

    # P/B
    if pb is not None and pb > 0:
        if pb < 1.0:
            value_score += 10
        elif pb > 5.0:
            value_score -= 5

    # Dividend yield
    dy = _to_percent(dividend_yield)
    if dy is not None and dy > 0:
        if 2.0 <= dy <= 6.0:
            value_score += 10
        elif dy > 6.0:
            value_score += 5
        elif dy < 1.0:
            value_score -= 2

    # Upside contribution
    if upside_percent is not None:
        up = float(upside_percent)
        if up > 0:
            value_score += min(20.0, up / 2.0)
        elif up < 0:
            value_score -= min(15.0, abs(up) / 2.0)

    value_score = _clamp(value_score)

    # ------------------------------------------------------------------
    # QUALITY SCORE
    # ------------------------------------------------------------------
    quality_score = 50.0

    roe_p = _to_percent(roe)
    if roe_p is not None:
        if roe_p > 20:
            quality_score += 20
        elif roe_p > 15:
            quality_score += 15
        elif roe_p > 10:
            quality_score += 5
        elif roe_p < 0:
            quality_score -= 15

    nm_p = _to_percent(net_margin)
    if nm_p is not None:
        if nm_p > 20:
            quality_score += 10
        elif nm_p > 10:
            quality_score += 5
        elif nm_p < 0:
            quality_score -= 10

    # Debt/Equity
    if debt_to_equity is not None:
        dte = float(debt_to_equity)
        if dte > 2.5:
            quality_score -= 15
        elif dte > 1.5:
            quality_score -= 5
        elif 0 <= dte < 0.5:
            quality_score += 10

    # Growth
    rev_g = _to_percent(revenue_growth)
    if rev_g is not None:
        if rev_g > 15:
            quality_score += 5
        elif rev_g < 0:
            quality_score -= 5

    ni_g = _to_percent(net_income_growth)
    if ni_g is not None:
        if ni_g > 15:
            quality_score += 5
        elif ni_g < 0:
            quality_score -= 5

    # ROA small bonus (optional)
    roa_p = _to_percent(roa)
    if roa_p is not None:
        if roa_p > 8:
            quality_score += 3
        elif roa_p < 0:
            quality_score -= 3

    quality_score = _clamp(quality_score)

    # ------------------------------------------------------------------
    # MOMENTUM SCORE
    # ------------------------------------------------------------------
    momentum_score = 50.0

    pc = _normalize_pct_like(percent_change)
    if pc is not None:
        if pc > 3:
            momentum_score += 5
        elif pc > 0:
            momentum_score += 2
        elif pc < -3:
            momentum_score -= 5

    if current_price is not None:
        cp = float(current_price)

        if ma20 is not None and ma20 != 0:
            momentum_score += 5 if cp > ma20 else -5

        if ma50 is not None and ma50 != 0:
            momentum_score += 5 if cp > ma50 else -5

        # 52w position
        if high_52w is not None and low_52w is not None and high_52w != low_52w:
            pos = (cp - low_52w) / (high_52w - low_52w)
            if pos > 0.8:
                momentum_score += 10
            elif pos < 0.2:
                momentum_score -= 10

    if rsi_14 is not None:
        rsi = float(rsi_14)
        if rsi > 75:
            momentum_score -= 10
        elif rsi < 25:
            momentum_score += 10
        elif 50 < rsi < 70:
            momentum_score += 5

    momentum_score = _clamp(momentum_score)

    # ------------------------------------------------------------------
    # RISK PENALTY (via volatility)
    # ------------------------------------------------------------------
    risk_penalty = 0.0
    vol_p = _to_percent(volatility_30d)
    if vol_p is not None:
        if vol_p > 50:
            risk_penalty = 15.0
        elif vol_p > 30:
            risk_penalty = 8.0
        elif vol_p < 15:
            risk_penalty = -5.0

    # ------------------------------------------------------------------
    # OPPORTUNITY + OVERALL
    # ------------------------------------------------------------------
    opportunity_score = _clamp(
        0.4 * value_score + 0.4 * momentum_score + 0.2 * quality_score - risk_penalty
    )

    overall_score = _clamp(
        0.3 * value_score + 0.3 * quality_score + 0.3 * momentum_score + 0.1 * opportunity_score
    )

    # ------------------------------------------------------------------
    # RECOMMENDATION
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

    # ------------------------------------------------------------------
    # CONFIDENCE (data completeness + data_quality cap)
    # ------------------------------------------------------------------
    # Key fields that make scoring reliable:
    key_fields = {
        "current_price": current_price,
        "market_cap": _as_float(_get(q, "market_cap", None)),
        "pe_ttm": pe_ttm,
        "roe": roe,
        "volatility_30d": volatility_30d,
        "percent_change": percent_change,
    }
    present = sum(1 for _, v in key_fields.items() if v is not None)
    total = len(key_fields)

    # Base confidence: 40–100 depending on completeness
    base_conf = 40.0 + (60.0 * (present / total if total else 0.0))

    # Extra penalty if the quote is explicitly missing
    if data_quality == "MISSING":
        confidence = 0.0
    else:
        # Cap by quality label
        cap = _data_quality_to_confidence_cap(data_quality)
        confidence = min(base_conf, cap)

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

    - Uses Pydantic v2 'model_copy' when available.
    - Falls back to v1 '.copy' when needed.
    - Extra fields are allowed by EnrichedQuote config (extra="allow").
    """
    scores = compute_scores(q)

    update_data: Dict[str, Any] = {
        "value_score": scores.value_score,
        "quality_score": scores.quality_score,
        "momentum_score": scores.momentum_score,
        "opportunity_score": scores.opportunity_score,
        "overall_score": scores.overall_score,
        "recommendation": scores.recommendation,
        # This is useful for routers; EnrichedQuote allows extra fields.
        "confidence": scores.confidence,
    }

    if hasattr(q, "model_copy"):
        return q.model_copy(update=update_data)
    if hasattr(q, "copy"):
        return q.copy(update=update_data)  # type: ignore[attr-defined]

    # last resort: mutate (should rarely happen)
    for k, v in update_data.items():
        try:
            setattr(q, k, v)
        except Exception:
            pass
    return q


__all__ = ["AssetScores", "compute_scores", "enrich_with_scores"]
