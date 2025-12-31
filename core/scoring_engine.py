# core/scoring_engine.py  (FULL REPLACEMENT)
"""
core/scoring_engine.py
===========================================================
Lightweight, transparent scoring engine for the Ultimate Investment Dashboard.

Produces (0–100):
- value_score
- quality_score
- momentum_score
- risk_score          (higher = MORE risk)
- opportunity_score
- overall_score
- recommendation      (BUY / HOLD / REDUCE / SELL)
- confidence          (0–100, reflects data completeness + data_quality)

Design goals
✅ Deterministic + simple + explainable
✅ Works with partial data (confidence reflects missingness)
✅ Provider-agnostic: normalizes % values that may appear as decimals, percents, or strings ("12%")
✅ Import-safe: does NOT require EnrichedQuote / DataEngine imports (avoids circular imports)
✅ Works with either EnrichedQuote or UnifiedQuote-like objects (duck-typing)

Usage:
    from core.scoring_engine import enrich_with_scores
    q2 = enrich_with_scores(q)  # returns NEW object when possible
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

# Pydantic best-effort (import-safe)
try:
    from pydantic import BaseModel, Field, ConfigDict  # type: ignore

    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    from pydantic import BaseModel, Field  # type: ignore

    ConfigDict = None  # type: ignore
    _PYDANTIC_V2 = False


SCORING_ENGINE_VERSION = "1.4.0"

# Recommendation enum (MUST match routers normalization)
_RECO_ENUM = ("BUY", "HOLD", "REDUCE", "SELL")


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
    risk_score: float = Field(50.0, ge=0, le=100)  # higher = more risk

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


def _get_any(obj: Any, *names: str) -> Any:
    """
    Returns the first non-null, non-empty value across candidate names.
    """
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
    """
    Hard normalize to BUY/HOLD/REDUCE/SELL.
    """
    if x is None:
        return "HOLD"
    try:
        s = str(x).strip().upper()
    except Exception:
        return "HOLD"
    if s in _RECO_ENUM:
        return s
    # tolerate common variants
    if "SELL" in s:
        return "SELL"
    if "REDUCE" in s or "TRIM" in s or "TAKE" in s:
        return "REDUCE"
    if "BUY" in s or "ACCUM" in s or "ADD" in s or "OVERWEIGHT" in s:
        return "BUY"
    return "HOLD"


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

    dy = _to_percent(_get_any(q, "dividend_yield", "div_yield", "dividend_yield_pct"))
    payout = _to_percent(_get_any(q, "payout_ratio", "payout", "payout_pct"))
    upside = _to_percent(_get_any(q, "upside_percent", "upside_pct"))

    # Quality
    roe = _to_percent(_get_any(q, "roe", "return_on_equity"))
    roa = _to_percent(_get_any(q, "roa", "return_on_assets"))
    nm = _to_percent(_get_any(q, "net_margin", "profit_margin"))
    ebitda_m = _to_percent(_get_any(q, "ebitda_margin", "margin_ebitda"))
    rev_g = _to_percent(_get_any(q, "revenue_growth", "rev_growth"))
    ni_g = _to_percent(_get_any(q, "net_income_growth", "ni_growth"))

    dte = _to_float(_get_any(q, "debt_to_equity"))  # optional
    beta = _to_float(_get_any(q, "beta", "beta_5y"))
    vol = _to_percent(_get_any(q, "volatility_30d", "vol_30d_ann", "vol30d"))
    rsi = _to_float(_get_any(q, "rsi_14", "rsi14"))

    # Momentum / price context
    pc = _to_percent(_get_any(q, "percent_change", "change_percent", "change_pct", "pct_change"))

    cp = _to_float(_get_any(q, "current_price", "last_price", "price", "close", "last"))
    ma20 = _to_float(_get_any(q, "ma20"))  # optional
    ma50 = _to_float(_get_any(q, "ma50"))  # optional

    high_52w = _to_float(_get_any(q, "week_52_high", "high_52w", "52w_high"))
    low_52w = _to_float(_get_any(q, "week_52_low", "low_52w", "52w_low"))
    pos_52w_pct = _to_percent(_get_any(q, "position_52w_percent", "position_52w"))

    # Liquidity
    liq = _to_float(_get_any(q, "liquidity_score"))
    avg_vol = _to_float(_get_any(q, "avg_volume_30d", "avg_volume", "avg_vol_30d"))
    turnover = _to_percent(_get_any(q, "turnover_percent", "turnover", "turnover_pct"))
    free_float = _to_percent(_get_any(q, "free_float", "free_float_percent", "free_float_pct"))

    dq = str(_get_any(q, "data_quality", "dq") or "").strip().upper()

    # -----------------------------
    # VALUE (0..100)
    # -----------------------------
    # Start neutral, then adjust using simple, explainable rules.
    value_score = 50.0

    # P/E: lower is better (but ignore <=0)
    pe_like = None
    for cand in (pe, fpe):
        if cand is not None and cand > 0:
            pe_like = cand
            break
    if pe_like is not None:
        # band mapping: (PE -> score contribution around value dimension)
        pe_val = _score_band_linear(
            pe_like,
            bands=[
                (5.0, 85.0),
                (10.0, 75.0),
                (18.0, 60.0),
                (25.0, 50.0),
                (40.0, 35.0),
                (60.0, 25.0),
            ],
        )
        value_score = 0.70 * value_score + 0.30 * pe_val

    # P/B
    if pb is not None and pb > 0:
        pb_val = _score_band_linear(
            pb,
            bands=[
                (0.6, 80.0),
                (1.0, 70.0),
                (2.0, 60.0),
                (4.0, 50.0),
                (6.0, 40.0),
                (10.0, 30.0),
            ],
        )
        value_score = 0.80 * value_score + 0.20 * pb_val

    # P/S
    if ps is not None and ps > 0:
        ps_val = _score_band_linear(
            ps,
            bands=[
                (0.7, 75.0),
                (1.5, 65.0),
                (3.0, 55.0),
                (6.0, 45.0),
                (10.0, 35.0),
                (20.0, 25.0),
            ],
        )
        value_score = 0.85 * value_score + 0.15 * ps_val

    # EV/EBITDA
    if ev is not None and ev > 0:
        ev_val = _score_band_linear(
            ev,
            bands=[
                (5.0, 80.0),
                (8.0, 70.0),
                (12.0, 60.0),
                (18.0, 45.0),
                (25.0, 35.0),
                (40.0, 25.0),
            ],
        )
        value_score = 0.85 * value_score + 0.15 * ev_val

    # Dividend yield: 2%–6% usually "healthy"; too high can be risky
    if dy is not None and dy >= 0:
        dy_val = _score_band_linear(
            dy,
            bands=[
                (0.0, 40.0),
                (2.0, 65.0),
                (4.0, 80.0),
                (6.0, 75.0),
                (10.0, 55.0),
                (15.0, 40.0),
            ],
        )
        value_score = 0.85 * value_score + 0.15 * dy_val

    # Payout ratio: prefer 30–70, penalize >100
    if payout is not None and payout >= 0:
        if payout > 120:
            value_score -= 6
        elif 30 <= payout <= 70:
            value_score += 3

    # Upside %: reward positive, penalize negative (but keep bounded)
    if upside is not None:
        if upside > 0:
            value_score += min(14.0, upside / 2.0)
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

    # Growth (light touch)
    if rev_g is not None:
        quality_score += _clamp(_score_band_linear(rev_g, bands=[(-20.0, -6.0), (0.0, 0.0), (10.0, 3.0), (20.0, 5.0)]), -6, 5, 0)

    if ni_g is not None:
        quality_score += _clamp(_score_band_linear(ni_g, bands=[(-20.0, -6.0), (0.0, 0.0), (10.0, 3.0), (20.0, 5.0)]), -6, 5, 0)

    # Leverage penalty/bonus (if available)
    if dte is not None:
        if dte > 2.5:
            quality_score -= 12
        elif dte > 1.5:
            quality_score -= 6
        elif 0 <= dte < 0.5:
            quality_score += 6

    quality_score = _clamp(quality_score)

    # -----------------------------
    # MOMENTUM (0..100)
    # -----------------------------
    momentum_score = 50.0

    # Daily % change (small weight)
    if pc is not None:
        momentum_score = 0.85 * momentum_score + 0.15 * _score_band_linear(
            pc, bands=[(-8.0, 30.0), (-3.0, 42.0), (0.0, 50.0), (3.0, 58.0), (8.0, 70.0)]
        )

    # MA trend (optional)
    if cp is not None:
        if ma20 is not None:
            momentum_score += 5.0 if cp > ma20 else -5.0
        if ma50 is not None:
            momentum_score += 5.0 if cp > ma50 else -5.0

        # 52w position: use provided percent if exists, else compute
        pos_frac = None
        if pos_52w_pct is not None:
            pos_frac = pos_52w_pct / 100.0
        else:
            pos_frac = _pos52w_frac(cp, low_52w, high_52w)

        if pos_frac is not None:
            # very low position may indicate "value zone" but weak momentum; high indicates strength
            if pos_frac > 0.85:
                momentum_score += 10
            elif pos_frac < 0.15:
                momentum_score -= 8

    # RSI (mean-reversion tilt)
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

    # Volatility (%)
    if vol is not None:
        risk_score = 0.70 * risk_score + 0.30 * _score_band_linear(
            vol, bands=[(10.0, 30.0), (15.0, 38.0), (25.0, 50.0), (35.0, 62.0), (50.0, 75.0), (80.0, 90.0)]
        )

    # Beta
    if beta is not None:
        risk_score = 0.80 * risk_score + 0.20 * _score_band_linear(
            beta, bands=[(0.5, 40.0), (0.8, 45.0), (1.0, 50.0), (1.3, 58.0), (1.7, 68.0), (2.2, 78.0)]
        )

    # Leverage
    if dte is not None:
        risk_score = 0.80 * risk_score + 0.20 * _score_band_linear(
            dte, bands=[(0.0, 40.0), (0.5, 45.0), (1.0, 52.0), (1.5, 60.0), (2.5, 75.0), (4.0, 88.0)]
        )

    # Liquidity (assume 0..100, higher better -> lower risk)
    if liq is not None:
        if liq < 30:
            risk_score += 10
        elif liq > 70:
            risk_score -= 5

    # Avg volume
    if avg_vol is not None:
        if avg_vol < 100_000:
            risk_score += 6
        elif avg_vol > 5_000_000:
            risk_score -= 2

    # Turnover %
    if turnover is not None:
        if turnover < 0.5:
            risk_score += 4
        elif turnover > 3.0:
            risk_score -= 2

    # Free float %
    if free_float is not None:
        if free_float < 20:
            risk_score += 3
        elif free_float > 60:
            risk_score -= 1

    # Data quality penalty to risk
    if dq in {"BAD", "MISSING"}:
        risk_score += 12
    elif dq in {"PARTIAL"}:
        risk_score += 5

    risk_score = _clamp(risk_score)

    # -----------------------------
    # OPPORTUNITY + OVERALL
    # -----------------------------
    # Convert risk_score into a penalty applied to opportunity:
    # risk_score 50 => 0
    # risk_score 100 => +15 penalty
    # risk_score 0 => -15 bonus
    risk_penalty = ((risk_score - 50.0) / 50.0) * 15.0  # -15..+15

    opportunity_score = _clamp(0.44 * value_score + 0.30 * momentum_score + 0.26 * quality_score - risk_penalty)
    overall_score = _clamp(0.34 * value_score + 0.30 * quality_score + 0.22 * momentum_score + 0.14 * opportunity_score)

    # -----------------------------
    # Confidence (0..100)
    # -----------------------------
    # Coverage by groups (weighted)
    # - price/liquidity basics
    # - valuation
    # - quality
    # - risk/technicals
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

    # Adjust by data_quality label
    if dq == "MISSING":
        confidence = 0.0
    elif dq == "BAD":
        confidence = min(confidence, 45.0)
    elif dq == "PARTIAL":
        confidence = min(confidence, 70.0)
    elif dq == "OK":
        confidence = min(confidence, 85.0)
    # FULL or unknown => keep as computed

    confidence = _clamp(confidence)

    # -----------------------------
    # Recommendation (enum-only + confidence aware)
    # -----------------------------
    # Primary driver: overall_score; guardrails by confidence and risk.
    reco = "HOLD"
    if overall_score >= 72 and confidence >= 55 and risk_score <= 75:
        reco = "BUY"
    elif overall_score <= 32 and confidence >= 40:
        reco = "SELL" if risk_score >= 60 else "REDUCE"
    elif overall_score <= 45:
        reco = "REDUCE"
    else:
        reco = "HOLD"

    # If confidence is low, avoid extreme actions.
    if confidence < 40:
        if reco == "BUY":
            reco = "HOLD"
        elif reco == "SELL":
            reco = "REDUCE"

    reco = _normalize_reco(reco)

    return AssetScores(
        value_score=_clamp(value_score),
        quality_score=_clamp(quality_score),
        momentum_score=_clamp(momentum_score),
        risk_score=_clamp(risk_score),
        opportunity_score=_clamp(opportunity_score),
        overall_score=_clamp(overall_score),
        recommendation=reco,
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
      - If input already has a non-null risk_score, keep it (useful if upstream risk model exists).
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
        "recommendation": str(_normalize_reco(scores.recommendation)),
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
