# core/scoring_engine.py  (FULL REPLACEMENT)
"""
core/scoring_engine.py
===========================================================
Lightweight, transparent scoring engine for the Ultimate Investment Dashboard.

Produces (0–100):
- value_score
- quality_score
- momentum_score
- risk_score
- opportunity_score
- overall_score
- recommendation
- confidence (internal, returned in AssetScores)

Design goals
✅ Deterministic + simple + explainable
✅ Works with partial data (confidence reflects missingness)
✅ Provider-agnostic: normalizes % values that may appear as decimals, percents, or strings ("12%")
✅ Safe imports: does NOT require EnrichedQuote / DataEngine imports (avoids circular imports)
✅ Works with either EnrichedQuote or UnifiedQuote-like objects (duck-typing)

Usage:
    from core.scoring_engine import enrich_with_scores
    q2 = enrich_with_scores(q)  # returns NEW object when possible
"""

from __future__ import annotations

import math
from typing import Any, Dict, Optional, Tuple

# Pydantic best-effort (import-safe)
try:
    from pydantic import BaseModel, Field, ConfigDict  # type: ignore

    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    from pydantic import BaseModel, Field  # type: ignore

    ConfigDict = None  # type: ignore
    _PYDANTIC_V2 = False


SCORING_ENGINE_VERSION = "1.3.0"


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
    risk_score: float = Field(50.0, ge=0, le=100)

    opportunity_score: float = Field(50.0, ge=0, le=100)
    overall_score: float = Field(50.0, ge=0, le=100)
    recommendation: str = Field("HOLD")

    confidence: float = Field(50.0, ge=0, le=100)


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


def _strip_percent_str(s: str) -> str:
    t = (s or "").strip()
    if not t:
        return ""
    # remove commas and percent sign
    t = t.replace(",", "").replace("%", "").strip()
    return t


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, bool):
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
    - If abs(val) <= 1 -> treat as fraction and multiply by 100
    - Else assume it's already percent
    - Strings like "12%" supported
    """
    v = _to_float(x)
    if v is None:
        return None
    if v == 0.0:
        return 0.0
    if abs(v) <= 1.0:
        return v * 100.0
    return v


def _is_present(x: Any) -> bool:
    v = _to_float(x)
    return v is not None


def _pos52w_frac(cp: Optional[float], low_52w: Optional[float], high_52w: Optional[float]) -> Optional[float]:
    if cp is None or low_52w is None or high_52w is None:
        return None
    rng = high_52w - low_52w
    if rng == 0:
        return None
    return (cp - low_52w) / rng  # 0..1


def _safe_score_from_ratio(x: Optional[float], good_low: float, good_high: float) -> float:
    """
    Score 0..100, peaky around a good interval. Used for dividend yield etc.
    """
    if x is None:
        return 50.0
    try:
        xv = float(x)
    except Exception:
        return 50.0
    if xv < good_low:
        # ramp up to 50
        return _clamp(50.0 * (xv / max(good_low, 1e-9)), 0, 50, 50)
    if xv <= good_high:
        return _clamp(70.0 + (xv - good_low) / max((good_high - good_low), 1e-9) * 30.0, 70, 100, 85)
    # above good_high: gently decline (very high yield can be risky)
    return _clamp(90.0 - min(40.0, (xv - good_high) * 5.0), 20, 90, 60)


# ---------------------------------------------------------------------
# Core scoring (deterministic, explainable)
# ---------------------------------------------------------------------
def compute_scores(q: Any) -> AssetScores:
    """
    Computes scores for an EnrichedQuote/UnifiedQuote-like object (duck typing).
    Never raises.
    """

    # -----------------------------
    # Pull common inputs (best-effort)
    # -----------------------------
    pe = _to_float(_get(q, "pe_ttm"))
    fpe = _to_float(_get(q, "forward_pe"))
    pb = _to_float(_get(q, "pb"))
    ps = _to_float(_get(q, "ps"))
    ev = _to_float(_get(q, "ev_ebitda"))
    dy = _to_percent(_get(q, "dividend_yield"))
    payout = _to_percent(_get(q, "payout_ratio"))

    upside = _to_percent(_get(q, "upside_percent"))

    roe = _to_percent(_get(q, "roe"))
    roa = _to_percent(_get(q, "roa"))
    nm = _to_percent(_get(q, "net_margin"))
    ebitda_m = _to_percent(_get(q, "ebitda_margin"))
    rev_g = _to_percent(_get(q, "revenue_growth"))
    ni_g = _to_percent(_get(q, "net_income_growth"))

    dte = _to_float(_get(q, "debt_to_equity"))  # optional (not always present)
    beta = _to_float(_get(q, "beta"))
    vol = _to_percent(_get(q, "volatility_30d"))
    rsi = _to_float(_get(q, "rsi_14"))

    pc = _to_percent(_get(q, "percent_change"))

    cp = _to_float(_get(q, "current_price"))
    ma20 = _to_float(_get(q, "ma20"))  # optional
    ma50 = _to_float(_get(q, "ma50"))  # optional
    high_52w = _to_float(_get(q, "high_52w"))
    low_52w = _to_float(_get(q, "low_52w"))
    pos_52w_pct = _to_percent(_get(q, "position_52w_percent"))

    liq = _to_float(_get(q, "liquidity_score"))
    avg_vol = _to_float(_get(q, "avg_volume_30d"))
    turnover = _to_percent(_get(q, "turnover_percent"))
    free_float = _to_percent(_get(q, "free_float"))

    dq = str(_get(q, "data_quality", "") or "").strip().upper()

    # -----------------------------
    # VALUE (0..100)
    # -----------------------------
    value_score = 50.0

    # P/E: prefer pe then forward_pe
    pe_like = None
    for cand in (pe, fpe):
        if cand is not None and cand > 0:
            pe_like = cand
            break
    if pe_like is not None:
        if pe_like < 10:
            value_score += 18
        elif pe_like < 20:
            value_score += 8
        elif pe_like > 40:
            value_score -= 14
        elif pe_like > 25:
            value_score -= 6

    # P/B
    if pb is not None and pb > 0:
        if pb < 1.0:
            value_score += 10
        elif pb < 2.0:
            value_score += 5
        elif pb > 6.0:
            value_score -= 7

    # P/S
    if ps is not None and ps > 0:
        if ps < 1.0:
            value_score += 6
        elif ps > 10.0:
            value_score -= 6

    # EV/EBITDA
    if ev is not None and ev > 0:
        if ev < 8:
            value_score += 8
        elif ev > 18:
            value_score -= 6

    # Dividend yield (prefer "healthy" yields)
    if dy is not None and dy > 0:
        value_score += (_safe_score_from_ratio(dy, 2.0, 6.0) - 50.0) * 0.25  # add up to ~+12.5

    # Payout ratio (very high can be risky; moderate okay)
    if payout is not None and payout > 0:
        if payout > 100:
            value_score -= 6
        elif 30 <= payout <= 70:
            value_score += 3

    # Upside %
    if upside is not None:
        if upside > 0:
            value_score += min(18.0, upside / 2.0)
        elif upside < 0:
            value_score -= min(12.0, abs(upside) / 2.0)

    value_score = _clamp(value_score)

    # -----------------------------
    # QUALITY (0..100)
    # -----------------------------
    quality_score = 50.0

    if roe is not None:
        if roe > 20:
            quality_score += 18
        elif roe > 15:
            quality_score += 12
        elif roe > 10:
            quality_score += 6
        elif roe < 0:
            quality_score -= 14

    if roa is not None:
        if roa > 8:
            quality_score += 6
        elif roa < 0:
            quality_score -= 6

    if nm is not None:
        if nm > 20:
            quality_score += 8
        elif nm > 10:
            quality_score += 4
        elif nm < 0:
            quality_score -= 10

    if ebitda_m is not None:
        if ebitda_m > 20:
            quality_score += 5
        elif ebitda_m < 0:
            quality_score -= 5

    if dte is not None:
        if dte > 2.5:
            quality_score -= 14
        elif dte > 1.5:
            quality_score -= 6
        elif 0 <= dte < 0.5:
            quality_score += 8

    if rev_g is not None:
        if rev_g > 15:
            quality_score += 4
        elif rev_g < 0:
            quality_score -= 4

    if ni_g is not None:
        if ni_g > 15:
            quality_score += 4
        elif ni_g < 0:
            quality_score -= 4

    quality_score = _clamp(quality_score)

    # -----------------------------
    # MOMENTUM (0..100)
    # -----------------------------
    momentum_score = 50.0

    # Daily % change
    if pc is not None:
        if pc > 3:
            momentum_score += 6
        elif pc > 0:
            momentum_score += 2
        elif pc < -3:
            momentum_score -= 6
        elif pc < 0:
            momentum_score -= 2

    # MAs (optional)
    if cp is not None:
        if ma20 is not None:
            momentum_score += 5 if cp > ma20 else -5
        if ma50 is not None:
            momentum_score += 5 if cp > ma50 else -5

        # 52w position (prefer provided %, else compute)
        pos_frac = None
        if pos_52w_pct is not None:
            pos_frac = pos_52w_pct / 100.0
        else:
            pos_frac = _pos52w_frac(cp, low_52w, high_52w)

        if pos_frac is not None:
            if pos_frac > 0.8:
                momentum_score += 10
            elif pos_frac < 0.2:
                momentum_score -= 10

    # RSI
    if rsi is not None:
        if rsi > 75:
            momentum_score -= 10
        elif rsi < 25:
            momentum_score += 10
        elif 50 < rsi < 70:
            momentum_score += 5

    momentum_score = _clamp(momentum_score)

    # -----------------------------
    # RISK (0..100, higher = more risk)
    # -----------------------------
    risk_score = 50.0

    # Volatility bands (%)
    if vol is not None:
        if vol <= 15:
            risk_score -= 12
        elif vol <= 30:
            risk_score += 0
        elif vol <= 50:
            risk_score += 12
        else:
            risk_score += 22

    if beta is not None:
        if beta >= 1.7:
            risk_score += 10
        elif beta >= 1.3:
            risk_score += 6
        elif beta <= 0.8:
            risk_score -= 5

    if dte is not None:
        if dte > 2.5:
            risk_score += 15
        elif dte > 1.5:
            risk_score += 7
        elif 0 <= dte < 0.5:
            risk_score -= 6

    # Liquidity score (assume 0..100)
    if liq is not None:
        if liq < 30:
            risk_score += 10
        elif liq > 70:
            risk_score -= 4

    # Avg volume (if available)
    if avg_vol is not None:
        if avg_vol < 100_000:
            risk_score += 6
        elif avg_vol > 5_000_000:
            risk_score -= 2

    # Turnover% (very low turnover adds risk; very high tends to be liquid)
    if turnover is not None:
        if turnover < 0.5:
            risk_score += 4
        elif turnover > 3.0:
            risk_score -= 2

    # Free float% (very low float can be risky)
    if free_float is not None:
        if free_float < 20:
            risk_score += 3
        elif free_float > 60:
            risk_score -= 1

    # Data quality penalty
    if dq in {"BAD", "MISSING"}:
        risk_score += 10
    elif dq in {"PARTIAL"}:
        risk_score += 4

    risk_score = _clamp(risk_score)

    # -----------------------------
    # OPPORTUNITY + OVERALL
    # -----------------------------
    # Translate risk_score into a penalty/bonus applied to opportunity:
    # risk_score 50 => 0
    # risk_score 100 => +15 penalty
    # risk_score 0 => -15 bonus
    risk_penalty = ((risk_score - 50.0) / 50.0) * 15.0  # -15..+15

    opportunity_score = _clamp(0.42 * value_score + 0.33 * momentum_score + 0.25 * quality_score - risk_penalty)
    overall_score = _clamp(0.30 * value_score + 0.30 * quality_score + 0.25 * momentum_score + 0.15 * opportunity_score)

    # -----------------------------
    # Confidence (0..100)
    # -----------------------------
    # coverage score: count how many important fields exist
    key_fields = [
        _get(q, "current_price"),
        _get(q, "previous_close"),
        _get(q, "market_cap"),
        _get(q, "pe_ttm"),
        _get(q, "roe"),
        _get(q, "volatility_30d"),
        _get(q, "avg_volume_30d"),
        _get(q, "high_52w"),
        _get(q, "low_52w"),
    ]
    present = sum(1 for f in key_fields if _is_present(f))
    total = len(key_fields)
    coverage = (present / total) * 100.0 if total else 50.0

    # start from coverage, then adjust by data_quality label
    confidence = coverage

    if dq == "MISSING":
        confidence = 0.0
    elif dq == "BAD":
        confidence = min(confidence, 45.0)
    elif dq == "PARTIAL":
        confidence = min(confidence, 70.0)
    elif dq == "OK":
        confidence = min(confidence, 85.0)
    # FULL => keep as-is (can be up to 100)

    confidence = _clamp(confidence)

    # -----------------------------
    # Recommendation (confidence-aware)
    # -----------------------------
    recommendation = "HOLD"
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

    # If confidence is low, downgrade aggressive recommendations
    if confidence < 40:
        if recommendation in {"STRONG BUY", "BUY"}:
            recommendation = "HOLD"
        elif recommendation == "SELL":
            recommendation = "REDUCE"

    return AssetScores(
        value_score=_clamp(value_score),
        quality_score=_clamp(quality_score),
        momentum_score=_clamp(momentum_score),
        risk_score=_clamp(risk_score),
        opportunity_score=_clamp(opportunity_score),
        overall_score=_clamp(overall_score),
        recommendation=recommendation,
        confidence=confidence,
    )


# ---------------------------------------------------------------------
# Enrichment (safe, returns new object when possible)
# ---------------------------------------------------------------------
def enrich_with_scores(q: Any, *, prefer_existing_risk_score: bool = True) -> Any:
    """
    Returns a NEW object with scoring fields populated when possible.
    Supports:
      - Pydantic v2: model_copy(update=...)
      - Pydantic v1: copy(update=...)
      - dict: returns a new dict
      - otherwise: last resort mutates attributes

    prefer_existing_risk_score:
      - If input already has a non-null risk_score, keep it.
    """
    scores = compute_scores(q)

    existing_risk = _to_float(_get(q, "risk_score"))
    risk_out = existing_risk if (prefer_existing_risk_score and existing_risk is not None) else scores.risk_score

    update_data: Dict[str, Any] = {
        "value_score": float(scores.value_score),
        "quality_score": float(scores.quality_score),
        "momentum_score": float(scores.momentum_score),
        "risk_score": float(risk_out),
        "opportunity_score": float(scores.opportunity_score),
        "overall_score": float(scores.overall_score),
        "recommendation": str(scores.recommendation),
        "confidence": float(scores.confidence),  # keep if your model supports it
        "scoring_version": SCORING_ENGINE_VERSION,
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
