#!/usr/bin/env python3
# core/scoring.py
"""
================================================================================
Scoring Module — v1.0.0 (PHASE 4 / SCHEMA-ALIGNED / SAFE)
================================================================================
Purpose:
- Compute scoring outputs used by Top10 selection and sheet insights:
    risk_score, overall_score, valuation_score, momentum_score, confidence_score
    value_score, quality_score, opportunity_score
    recommendation, recommendation_reason

Integration:
- core/data_engine_v2.py will try:
    compute_scores(row, settings=...)
    score_row(row, settings=...)
    score_quote(row, settings=...)
- Returned dict keys are merged into the engine row.

Conventions:
- All scores are 0..100 (higher is "better") EXCEPT risk_score:
    risk_score: 0..100 where 0 = LOW RISK, 100 = HIGH RISK
- Percent-like inputs are treated as FRACTIONS where possible (0.12 => 12%).
- This file must be startup-safe: no network, no heavy optional ML deps.

================================================================================
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


__version__ = "1.0.0"


# =============================================================================
# Small utilities
# =============================================================================
def _is_nan(x: float) -> bool:
    try:
        return math.isnan(x) or math.isinf(x)
    except Exception:
        return True


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _round(x: Optional[float], nd: int = 2) -> Optional[float]:
    if x is None:
        return None
    try:
        y = float(x)
        if _is_nan(y):
            return None
        return round(y, nd)
    except Exception:
        return None


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            f = float(v)
        else:
            s = str(v).strip().replace(",", "")
            if not s or s.lower() in {"na", "n/a", "none", "null"}:
                return None
            if s.endswith("%"):
                f = float(s[:-1].strip()) / 100.0
            else:
                f = float(s)
        if _is_nan(f):
            return None
        return f
    except Exception:
        return None


def _get(row: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def _getf(row: Dict[str, Any], *keys: str) -> Optional[float]:
    return _safe_float(_get(row, *keys))


def _as_fraction(x: Any) -> Optional[float]:
    """
    Convert percent-like values to fraction:
      "12%" -> 0.12
      12    -> 0.12 (assume percent-points if abs > 1.5)
      0.12  -> 0.12
    """
    f = _safe_float(x)
    if f is None:
        return None
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _sigmoid(x: float) -> float:
    # stable sigmoid
    try:
        if x >= 0:
            z = math.exp(-x)
            return 1 / (1 + z)
        z = math.exp(x)
        return z / (1 + z)
    except Exception:
        return 0.5


# =============================================================================
# Settings/weights (tunable)
# =============================================================================
@dataclass(slots=True)
class ScoreWeights:
    # component weights into overall
    w_valuation: float = 0.30
    w_momentum: float = 0.30
    w_quality: float = 0.20
    w_opportunity: float = 0.20

    # risk penalty strength (overall reduced by this factor)
    risk_penalty_strength: float = 0.55  # 0..1

    # confidence penalty strength (overall reduced by this factor)
    confidence_penalty_strength: float = 0.45  # 0..1


DEFAULT_WEIGHTS = ScoreWeights()


def _weights_from_settings(settings: Any) -> ScoreWeights:
    # Best-effort: allow settings to override weights if present
    w = ScoreWeights(**DEFAULT_WEIGHTS.__dict__)
    if settings is None:
        return w

    def _try(name: str, default: float) -> float:
        try:
            v = getattr(settings, name, None)
            if v is None:
                return default
            f = float(v)
            if _is_nan(f):
                return default
            return f
        except Exception:
            return default

    w.w_valuation = _try("score_w_valuation", w.w_valuation)
    w.w_momentum = _try("score_w_momentum", w.w_momentum)
    w.w_quality = _try("score_w_quality", w.w_quality)
    w.w_opportunity = _try("score_w_opportunity", w.w_opportunity)

    w.risk_penalty_strength = _try("risk_penalty_strength", w.risk_penalty_strength)
    w.confidence_penalty_strength = _try("confidence_penalty_strength", w.confidence_penalty_strength)

    # normalize component weights to sum 1.0
    s = w.w_valuation + w.w_momentum + w.w_quality + w.w_opportunity
    if s > 0:
        w.w_valuation /= s
        w.w_momentum /= s
        w.w_quality /= s
        w.w_opportunity /= s

    w.risk_penalty_strength = _clamp(w.risk_penalty_strength, 0.0, 1.0)
    w.confidence_penalty_strength = _clamp(w.confidence_penalty_strength, 0.0, 1.0)
    return w


# =============================================================================
# Component scoring
# =============================================================================
def _data_quality_factor(row: Dict[str, Any]) -> float:
    dq = str(_get(row, "data_quality") or "").strip().upper()
    # map to 0..1
    return {
        "EXCELLENT": 0.95,
        "GOOD": 0.80,
        "FAIR": 0.60,
        "POOR": 0.40,
        "STALE": 0.45,
        "MISSING": 0.20,
        "ERROR": 0.15,
    }.get(dq, 0.60)


def _completeness_factor(row: Dict[str, Any]) -> float:
    """
    Lightweight completeness estimate (0..1) using a stable set of core fields.
    """
    core_fields = [
        "symbol", "name", "currency", "exchange",
        "current_price", "previous_close",
        "day_high", "day_low", "week_52_high", "week_52_low",
        "volume", "market_cap",
        "pe_ttm", "dividend_yield",
        "rsi_14", "volatility_30d",
        "expected_roi_3m", "forecast_price_3m",
    ]
    present = 0
    for k in core_fields:
        v = row.get(k)
        if v is not None and v != "" and v != [] and v != {}:
            present += 1
    return present / max(1, len(core_fields))


def _quality_score(row: Dict[str, Any]) -> Optional[float]:
    dq = _data_quality_factor(row)
    comp = _completeness_factor(row)
    # quality is a blend of data-quality label + completeness
    q = 100.0 * _clamp(0.55 * dq + 0.45 * comp, 0.0, 1.0)
    return _round(q, 2)


def _confidence_score(row: Dict[str, Any]) -> Optional[float]:
    dq = _data_quality_factor(row)
    comp = _completeness_factor(row)

    # provider signal
    provs = row.get("data_sources") or []
    try:
        pcount = len(provs) if isinstance(provs, list) else 0
    except Exception:
        pcount = 0
    prov_factor = _clamp(pcount / 3.0, 0.0, 1.0)

    # confidence: mostly data quality & completeness, small provider bonus
    conf = 100.0 * _clamp(0.55 * dq + 0.35 * comp + 0.10 * prov_factor, 0.0, 1.0)
    return _round(conf, 2)


def _valuation_score(row: Dict[str, Any]) -> Optional[float]:
    """
    Valuation score (0..100, higher = cheaper/more undervalued).
    Uses:
      - upside (fair value vs price) if available
      - expected ROI (3m/12m) if available
      - PE/PB/EV-EBITDA as weak signals (lower is better)
    """
    price = _getf(row, "current_price", "price")
    if price is None or price <= 0:
        return None

    # upside from fair_value/target/forecast price
    fair = _getf(row, "fair_value", "target_price", "forecast_price_3m", "forecast_price_12m")
    upside = None
    if fair is not None and fair > 0:
        upside = (fair / price) - 1.0  # fraction

    roi3 = _as_fraction(_get(row, "expected_roi_3m"))
    roi12 = _as_fraction(_get(row, "expected_roi_12m"))

    # normalized upside/roi -> 0..1
    def _roi_to_score(frac: Optional[float], cap: float) -> Optional[float]:
        if frac is None:
            return None
        return _clamp((frac + cap) / (2 * cap), 0.0, 1.0)

    upside_n = _roi_to_score(upside, cap=0.50)  # -50%..+50%
    roi3_n = _roi_to_score(roi3, cap=0.35)      # -35%..+35%
    roi12_n = _roi_to_score(roi12, cap=0.80)    # -80%..+80%

    # PE / PB heuristics
    pe = _getf(row, "pe_ttm", "pe_ratio")
    pb = _getf(row, "pb", "price_to_book")
    ev = _getf(row, "ev_ebitda", "enterprise_value_to_ebitda")

    def _low_is_good(x: Optional[float], lo: float, hi: float) -> Optional[float]:
        if x is None or x <= 0:
            return None
        # map [lo..hi] -> [1..0]
        if x <= lo:
            return 1.0
        if x >= hi:
            return 0.0
        return 1.0 - ((x - lo) / (hi - lo))

    pe_n = _low_is_good(pe, lo=8.0, hi=35.0)
    pb_n = _low_is_good(pb, lo=0.8, hi=6.0)
    ev_n = _low_is_good(ev, lo=6.0, hi=25.0)

    parts: List[Tuple[float, float]] = []
    # prioritize direct upside/roi when available
    if upside_n is not None:
        parts.append((0.45, upside_n))
    if roi3_n is not None:
        parts.append((0.30, roi3_n))
    if roi12_n is not None:
        parts.append((0.15, roi12_n))

    # weak fundamental anchors
    anchors = [x for x in [pe_n, pb_n, ev_n] if x is not None]
    if anchors:
        parts.append((0.10, sum(anchors) / len(anchors)))

    if not parts:
        return None

    # weighted average normalize
    wsum = sum(w for w, _ in parts)
    score01 = sum(w * v for w, v in parts) / max(1e-9, wsum)
    return _round(100.0 * _clamp(score01, 0.0, 1.0), 2)


def _momentum_score(row: Dict[str, Any]) -> Optional[float]:
    """
    Momentum score (0..100, higher = stronger trend).
    Uses:
      - percent_change (fraction) if available
      - RSI 14 (target ~55-65)
      - week_52_position_pct (0..1)
    """
    pct = _as_fraction(_get(row, "percent_change", "change_pct"))
    rsi = _getf(row, "rsi_14", "rsi")
    pos = _as_fraction(_get(row, "week_52_position_pct", "position_52w_pct"))

    parts: List[Tuple[float, float]] = []

    if pct is not None:
        # map -10%..+10% daily -> 0..1 (cap)
        p = _clamp((pct + 0.10) / 0.20, 0.0, 1.0)
        parts.append((0.35, p))

    if rsi is not None:
        # 30..70 -> 0..1, center around 55
        # use bell around 55: max at 55, lower at extremes
        x = (rsi - 55.0) / 12.0
        bell = math.exp(-(x * x))
        parts.append((0.35, _clamp(bell, 0.0, 1.0)))

    if pos is not None:
        # if near 1 -> near 52w high => strong; near 0 => weak
        parts.append((0.30, _clamp(pos, 0.0, 1.0)))

    if not parts:
        return None

    wsum = sum(w for w, _ in parts)
    score01 = sum(w * v for w, v in parts) / max(1e-9, wsum)
    return _round(100.0 * _clamp(score01, 0.0, 1.0), 2)


def _risk_score(row: Dict[str, Any]) -> Optional[float]:
    """
    Risk score (0..100) where 0 = LOW RISK, 100 = HIGH RISK.
    Uses:
      - volatility_30d (fraction)
      - beta
      - max_drawdown_30d (fraction) if exists
    """
    vol = _as_fraction(_get(row, "volatility_30d"))
    beta = _getf(row, "beta")
    dd = _as_fraction(_get(row, "max_drawdown_30d", "drawdown_30d"))

    parts: List[Tuple[float, float]] = []

    if vol is not None:
        # map 10%..60% to 0..1 risk
        v = _clamp((vol - 0.10) / (0.60 - 0.10), 0.0, 1.0)
        parts.append((0.50, v))

    if beta is not None:
        # map 0.6..2.0 to 0..1 risk
        b = _clamp((beta - 0.60) / (2.00 - 0.60), 0.0, 1.0)
        parts.append((0.30, b))

    if dd is not None:
        # drawdown 0..50% -> 0..1 risk
        d = _clamp(dd / 0.50, 0.0, 1.0)
        parts.append((0.20, d))

    if not parts:
        return None

    wsum = sum(w for w, _ in parts)
    risk01 = sum(w * v for w, v in parts) / max(1e-9, wsum)
    return _round(100.0 * _clamp(risk01, 0.0, 1.0), 2)


def _opportunity_score(row: Dict[str, Any], valuation: Optional[float], momentum: Optional[float]) -> Optional[float]:
    """
    Opportunity score (0..100) used for ranking when ROI exists.
    Uses ROI primarily; falls back to valuation+momentum blend.
    """
    roi1 = _as_fraction(_get(row, "expected_roi_1m"))
    roi3 = _as_fraction(_get(row, "expected_roi_3m"))
    roi12 = _as_fraction(_get(row, "expected_roi_12m"))

    def roi_norm(frac: Optional[float], cap: float) -> Optional[float]:
        if frac is None:
            return None
        return _clamp((frac + cap) / (2 * cap), 0.0, 1.0)

    r1 = roi_norm(roi1, 0.25)
    r3 = roi_norm(roi3, 0.35)
    r12 = roi_norm(roi12, 0.80)

    parts: List[Tuple[float, float]] = []
    if r3 is not None:
        parts.append((0.55, r3))
    if r12 is not None:
        parts.append((0.30, r12))
    if r1 is not None:
        parts.append((0.15, r1))

    if parts:
        wsum = sum(w for w, _ in parts)
        score01 = sum(w * v for w, v in parts) / max(1e-9, wsum)
        return _round(100.0 * _clamp(score01, 0.0, 1.0), 2)

    # fallback: use valuation & momentum if no ROI
    if valuation is None and momentum is None:
        return None
    v = (valuation or 50.0) / 100.0
    m = (momentum or 50.0) / 100.0
    score01 = _clamp(0.60 * v + 0.40 * m, 0.0, 1.0)
    return _round(100.0 * score01, 2)


def _recommendation(
    overall: Optional[float],
    risk: Optional[float],
    confidence: Optional[float],
    roi3: Optional[float],
) -> Tuple[str, str]:
    """
    Returns (recommendation, reason).
    """
    if overall is None:
        return "HOLD", "Insufficient data to score reliably."

    r = risk if risk is not None else 50.0
    c = confidence if confidence is not None else 50.0
    roi = roi3 if roi3 is not None else 0.0

    # rule-based gating (aligned to your Top10 philosophy)
    # - high confidence helps
    # - high risk penalizes
    if c < 45:
        return "HOLD", f"Low confidence ({_round(c,1)})."

    if r >= 75 and overall < 75:
        return "REDUCE", f"High risk ({_round(r,1)}) and moderate score ({_round(overall,1)})."

    # ROI-aware pushes
    if roi is not None and roi >= 0.25 and c >= 70 and r <= 45 and overall >= 78:
        return "STRONG_BUY", f"High expected ROI (~{_round(roi*100,1)}%) with strong confidence and low/moderate risk."
    if roi is not None and roi >= 0.12 and c >= 60 and r <= 55 and overall >= 70:
        return "BUY", f"Positive expected ROI (~{_round(roi*100,1)}%) with acceptable risk/confidence."

    # score-based fallback
    if overall >= 82 and r <= 55:
        return "BUY", f"Strong overall score ({_round(overall,1)}) with controlled risk ({_round(r,1)})."
    if overall >= 65:
        return "HOLD", f"Moderate overall score ({_round(overall,1)})."
    if overall >= 50:
        return "REDUCE", f"Weak overall score ({_round(overall,1)})."
    return "SELL", f"Very weak overall score ({_round(overall,1)})."


# =============================================================================
# Public API
# =============================================================================
def compute_scores(row: Dict[str, Any], *, settings: Any = None) -> Dict[str, Any]:
    """
    Main entrypoint (preferred).
    Returns a patch dict to merge into row.
    """
    row = row or {}
    w = _weights_from_settings(settings)

    # Component scores
    valuation = _valuation_score(row)
    momentum = _momentum_score(row)
    quality = _quality_score(row)
    confidence = _confidence_score(row)
    risk = _risk_score(row)

    opportunity = _opportunity_score(row, valuation, momentum)

    # Overall score with penalties
    # (higher is better; apply risk/confidence penalties softly)
    base_parts: list[Tuple[float, float]] = []
    if valuation is not None:
        base_parts.append((w.w_valuation, valuation / 100.0))
    if momentum is not None:
        base_parts.append((w.w_momentum, momentum / 100.0))
    if quality is not None:
        base_parts.append((w.w_quality, quality / 100.0))
    if opportunity is not None:
        base_parts.append((w.w_opportunity, opportunity / 100.0))

    overall = None
    if base_parts:
        wsum = sum(x[0] for x in base_parts)
        base01 = sum(weight * value for weight, value in base_parts) / max(1e-9, wsum)

        # penalties
        risk01 = (risk / 100.0) if risk is not None else 0.50
        conf01 = (confidence / 100.0) if confidence is not None else 0.50

        # penalize higher risk
        base01 *= (1.0 - w.risk_penalty_strength * (risk01 * 0.70))
        # penalize low confidence (use (1-conf))
        base01 *= (1.0 - w.confidence_penalty_strength * ((1.0 - conf01) * 0.80))

        overall = _round(100.0 * _clamp(base01, 0.0, 1.0), 2)

    # Value/Quality scores for compatibility with older dashboards
    value_score = valuation
    quality_score = quality

    # ROI for recommendation reasoning
    roi3 = _as_fraction(_get(row, "expected_roi_3m"))
    rec, reason = _recommendation(overall, risk, confidence, roi3)

    patch: Dict[str, Any] = {
        "valuation_score": valuation,
        "momentum_score": momentum,
        "quality_score": quality_score,
        "value_score": value_score,
        "opportunity_score": opportunity,
        "confidence_score": confidence,
        "risk_score": risk,
        "overall_score": overall,
        "recommendation": rec,
        "recommendation_reason": reason,
    }

    # Add some rank helpers if you want (optional, safe)
    # Many pages/sheets like to rank by ROI first.
    if roi3 is not None:
        patch["rank_roi_3m"] = _round(roi3, 6)

    return patch


def score_row(row: Dict[str, Any], *, settings: Any = None) -> Dict[str, Any]:
    return compute_scores(row, settings=settings)


def score_quote(row: Dict[str, Any], *, settings: Any = None) -> Dict[str, Any]:
    return compute_scores(row, settings=settings)


__all__ = [
    "compute_scores",
    "score_row",
    "score_quote",
    "__version__",
]
