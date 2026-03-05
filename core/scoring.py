#!/usr/bin/env python3
# core/scoring.py
"""
================================================================================
Scoring Module — v2.0.0 (PHASE D / SCHEMA-ALIGNED / ENGINE-READY / DETERMINISTIC)
================================================================================
Purpose
- Provide deterministic, schema-aligned scoring outputs so pages never show blank:
    value_score, quality_score, momentum_score, growth_score,
    valuation_score, risk_score, confidence_score,
    overall_score, opportunity_score,
    confidence_bucket, risk_bucket,
    recommendation, recommendation_reason
- Optional helpers:
    rank_overall is a *list-level* concept, provided as rank_rows_by_overall(...)

Integration (engine)
- core/data_engine_v2.py calls (best-effort):
    compute_scores(row, settings=...)
    score_row(row, settings=...)
    score_quote(row, settings=...)
- Returned dict keys are merged into the engine row.

Conventions / Schema alignment
- Scores are 0..100 (higher is better) EXCEPT risk_score:
    risk_score: 0..100 where 0 = LOW RISK, 100 = HIGH RISK
- Percent-like inputs are treated as FRACTIONS when possible:
    0.12 == 12%, "12%" == 0.12, 12 == 0.12 (assume percent-points if abs>1.5)
- Buckets:
    confidence_bucket: High/Medium/Low
    risk_bucket: Low/Moderate/High

Startup-safe
- No network calls
- No heavy optional ML deps

v2.0.0 changes (fix blanks + match Phase D)
- ✅ Adds growth_score, confidence_bucket, risk_bucket (schema-required)
- ✅ Uses risk stats if present: volatility_90d, max_drawdown_1y, var_95_1d, sharpe_1y
- ✅ Maps valuation ratios if present: pe_ttm, pb_ratio, ps_ratio, peg_ratio, ev_ebitda
- ✅ More robust percent parsing + stable fallbacks
- ✅ Adds rank_rows_by_overall(...) helper for list ranking (sets rank_overall)

================================================================================
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

__version__ = "2.0.0"


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


# =============================================================================
# Weights (tunable)
# =============================================================================
@dataclass(slots=True)
class ScoreWeights:
    # component weights into overall
    w_valuation: float = 0.30
    w_momentum: float = 0.25
    w_quality: float = 0.20
    w_growth: float = 0.15
    w_opportunity: float = 0.10

    # penalty strength
    risk_penalty_strength: float = 0.55  # 0..1
    confidence_penalty_strength: float = 0.45  # 0..1


DEFAULT_WEIGHTS = ScoreWeights()


def _weights_from_settings(settings: Any) -> ScoreWeights:
    """
    Best-effort: allow settings to override weights if present.
    Supported setting names:
      score_w_valuation, score_w_momentum, score_w_quality, score_w_growth, score_w_opportunity
      risk_penalty_strength, confidence_penalty_strength
    """
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
    w.w_growth = _try("score_w_growth", w.w_growth)
    w.w_opportunity = _try("score_w_opportunity", w.w_opportunity)

    w.risk_penalty_strength = _try("risk_penalty_strength", w.risk_penalty_strength)
    w.confidence_penalty_strength = _try("confidence_penalty_strength", w.confidence_penalty_strength)

    # normalize component weights to sum 1.0
    s = w.w_valuation + w.w_momentum + w.w_quality + w.w_growth + w.w_opportunity
    if s > 0:
        w.w_valuation /= s
        w.w_momentum /= s
        w.w_quality /= s
        w.w_growth /= s
        w.w_opportunity /= s

    w.risk_penalty_strength = _clamp(w.risk_penalty_strength, 0.0, 1.0)
    w.confidence_penalty_strength = _clamp(w.confidence_penalty_strength, 0.0, 1.0)
    return w


# =============================================================================
# Buckets
# =============================================================================
def _risk_bucket(score: Optional[float]) -> Optional[str]:
    if score is None:
        return None
    s = float(score)
    if s <= 35:
        return "Low"
    if s <= 65:
        return "Moderate"
    return "High"


def _confidence_bucket(conf01: Optional[float]) -> Optional[str]:
    """
    conf01 is fraction 0..1
    """
    if conf01 is None:
        return None
    c = float(conf01)
    if c >= 0.75:
        return "High"
    if c >= 0.50:
        return "Medium"
    return "Low"


# =============================================================================
# Component scoring
# =============================================================================
def _data_quality_factor(row: Dict[str, Any]) -> float:
    dq = str(_get(row, "data_quality") or "").strip().upper()
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
    Lightweight completeness estimate (0..1) using stable core fields.
    """
    core_fields = [
        "symbol",
        "name",
        "currency",
        "exchange",
        "current_price",
        "previous_close",
        "day_high",
        "day_low",
        "week_52_high",
        "week_52_low",
        "volume",
        "market_cap",
        "pe_ttm",
        "pb_ratio",
        "ps_ratio",
        "dividend_yield",
        "rsi_14",
        "volatility_30d",
        "volatility_90d",
        "expected_roi_3m",
        "forecast_price_3m",
        "forecast_confidence",
        "overall_score",
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
    q = 100.0 * _clamp(0.55 * dq + 0.45 * comp, 0.0, 1.0)
    return _round(q, 2)


def _confidence_score(row: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """
    Returns (confidence_score_0_100, forecast_confidence_0_1)
    """
    # Prefer forecast_confidence if supplied (0..1 or 0..100)
    fc = _safe_float(_get(row, "forecast_confidence", "ai_confidence"))
    if fc is not None:
        fc01 = (fc / 100.0) if fc > 1.5 else fc
        fc01 = _clamp(fc01, 0.0, 1.0)
        return _round(fc01 * 100.0, 2), _round(fc01, 4)

    dq = _data_quality_factor(row)
    comp = _completeness_factor(row)

    provs = row.get("data_sources") or []
    try:
        pcount = len(provs) if isinstance(provs, list) else 0
    except Exception:
        pcount = 0
    prov_factor = _clamp(pcount / 3.0, 0.0, 1.0)

    conf01 = _clamp(0.55 * dq + 0.35 * comp + 0.10 * prov_factor, 0.0, 1.0)
    return _round(conf01 * 100.0, 2), _round(conf01, 4)


def _valuation_score(row: Dict[str, Any]) -> Optional[float]:
    """
    Valuation score (0..100, higher = cheaper/more undervalued).

    Uses best-effort:
      - expected ROI (3m/12m) as value proxy
      - forecast vs current price if available
      - valuation multiples as anchors: PE/PB/PS/PEG/EV-EBITDA (lower is better)
    """
    price = _getf(row, "current_price", "price", "last_price", "last")
    if price is None or price <= 0:
        return None

    fair = _getf(row, "intrinsic_value", "fair_value", "target_price", "forecast_price_3m", "forecast_price_12m", "forecast_price_1m")
    upside = None
    if fair is not None and fair > 0:
        upside = (fair / price) - 1.0

    roi3 = _as_fraction(_get(row, "expected_roi_3m", "expected_return_3m"))
    roi12 = _as_fraction(_get(row, "expected_roi_12m", "expected_return_12m"))

    def _roi_to_score(frac: Optional[float], cap: float) -> Optional[float]:
        if frac is None:
            return None
        return _clamp((frac + cap) / (2 * cap), 0.0, 1.0)

    upside_n = _roi_to_score(upside, cap=0.50)
    roi3_n = _roi_to_score(roi3, cap=0.35)
    roi12_n = _roi_to_score(roi12, cap=0.80)

    pe = _getf(row, "pe_ttm", "pe_ratio")
    pb = _getf(row, "pb_ratio", "pb", "price_to_book")
    ps = _getf(row, "ps_ratio", "ps", "price_to_sales")
    peg = _getf(row, "peg_ratio", "peg")
    ev = _getf(row, "ev_ebitda", "ev_to_ebitda")

    def _low_is_good(x: Optional[float], lo: float, hi: float) -> Optional[float]:
        if x is None or x <= 0:
            return None
        if x <= lo:
            return 1.0
        if x >= hi:
            return 0.0
        return 1.0 - ((x - lo) / (hi - lo))

    anchors = [x for x in (
        _low_is_good(pe, lo=8.0, hi=35.0),
        _low_is_good(pb, lo=0.8, hi=6.0),
        _low_is_good(ps, lo=1.0, hi=10.0),
        _low_is_good(peg, lo=0.8, hi=4.0),
        _low_is_good(ev, lo=6.0, hi=25.0),
    ) if x is not None]
    anchor_avg = (sum(anchors) / len(anchors)) if anchors else None

    parts: List[Tuple[float, float]] = []
    if upside_n is not None:
        parts.append((0.40, upside_n))
    if roi3_n is not None:
        parts.append((0.30, roi3_n))
    if roi12_n is not None:
        parts.append((0.20, roi12_n))
    if anchor_avg is not None:
        parts.append((0.10, anchor_avg))

    if not parts:
        return None

    wsum = sum(w for w, _ in parts)
    score01 = sum(w * v for w, v in parts) / max(1e-9, wsum)
    return _round(100.0 * _clamp(score01, 0.0, 1.0), 2)


def _growth_score(row: Dict[str, Any]) -> Optional[float]:
    """
    Growth score (0..100).
    Primarily uses revenue_growth_yoy (fraction), mapping -30%..+30% to 0..100.
    """
    g = _as_fraction(_get(row, "revenue_growth_yoy", "revenue_growth", "growth_yoy"))
    if g is None:
        return None
    return _round(_clamp(((g + 0.30) / 0.60) * 100.0, 0.0, 100.0), 2)


def _momentum_score(row: Dict[str, Any]) -> Optional[float]:
    """
    Momentum score (0..100).
    Uses:
      - percent_change (fraction)
      - RSI 14
      - week_52_position_pct (fraction 0..1)
    """
    pct = _as_fraction(_get(row, "percent_change", "change_pct", "change_percent"))
    rsi = _getf(row, "rsi_14", "rsi", "rsi14")
    pos = _as_fraction(_get(row, "week_52_position_pct", "position_52w_pct", "week52_position_pct"))

    parts: List[Tuple[float, float]] = []

    if pct is not None:
        p = _clamp((pct + 0.10) / 0.20, 0.0, 1.0)  # -10%..+10%
        parts.append((0.40, p))

    if rsi is not None:
        x = (rsi - 55.0) / 12.0
        bell = math.exp(-(x * x))
        parts.append((0.35, _clamp(bell, 0.0, 1.0)))

    if pos is not None:
        parts.append((0.25, _clamp(pos, 0.0, 1.0)))

    if not parts:
        return None

    wsum = sum(w for w, _ in parts)
    score01 = sum(w * v for w, v in parts) / max(1e-9, wsum)
    return _round(100.0 * _clamp(score01, 0.0, 1.0), 2)


def _risk_score(row: Dict[str, Any]) -> Optional[float]:
    """
    Risk score (0..100) where 0 = LOW RISK, 100 = HIGH RISK.

    Prefers Phase D risk stats if available:
      volatility_90d (fraction), max_drawdown_1y (fraction), var_95_1d (fraction)
    Fallback:
      volatility_30d, beta_5y/beta, max_drawdown_30d
    """
    vol90 = _as_fraction(_get(row, "volatility_90d"))
    dd1y = _as_fraction(_get(row, "max_drawdown_1y"))
    var1d = _as_fraction(_get(row, "var_95_1d"))
    sharpe = _getf(row, "sharpe_1y")

    parts: List[Tuple[float, float]] = []

    # Normalize each metric to 0..1 riskiness
    def _scale(x: Optional[float], lo: float, hi: float) -> Optional[float]:
        if x is None:
            return None
        return _clamp((x - lo) / (hi - lo), 0.0, 1.0) if hi > lo else None

    if vol90 is not None:
        parts.append((0.40, _scale(vol90, 0.12, 0.70) or 0.0))
    if dd1y is not None:
        parts.append((0.35, _scale(dd1y, 0.05, 0.55) or 0.0))
    if var1d is not None:
        parts.append((0.20, _scale(var1d, 0.01, 0.08) or 0.0))
    if sharpe is not None:
        # higher Sharpe => lower risk contribution
        s = _clamp((1.0 - _clamp((sharpe + 0.5) / 2.5, 0.0, 1.0)), 0.0, 1.0)
        parts.append((0.05, s))

    if parts:
        wsum = sum(w for w, _ in parts)
        risk01 = sum(w * v for w, v in parts) / max(1e-9, wsum)
        return _round(100.0 * _clamp(risk01, 0.0, 1.0), 2)

    # Fallback metrics
    vol = _as_fraction(_get(row, "volatility_30d", "vol_30d"))
    beta = _getf(row, "beta_5y", "beta")
    dd = _as_fraction(_get(row, "max_drawdown_30d", "drawdown_30d"))

    parts2: List[Tuple[float, float]] = []

    if vol is not None:
        parts2.append((0.50, _scale(vol, 0.10, 0.60) or 0.0))
    if beta is not None:
        parts2.append((0.30, _scale(beta, 0.60, 2.00) or 0.0))
    if dd is not None:
        parts2.append((0.20, _scale(dd, 0.00, 0.50) or 0.0))

    if not parts2:
        return None

    wsum = sum(w for w, _ in parts2)
    risk01 = sum(w * v for w, v in parts2) / max(1e-9, wsum)
    return _round(100.0 * _clamp(risk01, 0.0, 1.0), 2)


def _opportunity_score(row: Dict[str, Any], valuation: Optional[float], momentum: Optional[float]) -> Optional[float]:
    """
    Opportunity score (0..100) used for ranking when ROI exists.
    Uses ROI primarily; falls back to valuation+momentum blend.
    """
    roi1 = _as_fraction(_get(row, "expected_roi_1m", "expected_return_1m"))
    roi3 = _as_fraction(_get(row, "expected_roi_3m", "expected_return_3m"))
    roi12 = _as_fraction(_get(row, "expected_roi_12m", "expected_return_12m"))

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

    if valuation is None and momentum is None:
        return None

    v = (valuation or 50.0) / 100.0
    m = (momentum or 50.0) / 100.0
    score01 = _clamp(0.60 * v + 0.40 * m, 0.0, 1.0)
    return _round(100.0 * score01, 2)


def _recommendation(
    overall: Optional[float],
    risk: Optional[float],
    confidence100: Optional[float],
    roi3: Optional[float],
) -> Tuple[str, str]:
    """
    Returns (recommendation, reason).
    """
    if overall is None:
        return "HOLD", "Insufficient data to score reliably."

    r = risk if risk is not None else 50.0
    c = confidence100 if confidence100 is not None else 55.0
    roi = roi3 if roi3 is not None else 0.0

    if c < 45:
        return "HOLD", f"Low confidence ({_round(c, 1)})."

    if r >= 75 and overall < 75:
        return "REDUCE", f"High risk ({_round(r, 1)}) and moderate score ({_round(overall, 1)})."

    if roi3 is not None and roi >= 0.25 and c >= 70 and r <= 45 and overall >= 78:
        return "STRONG_BUY", f"High expected ROI (~{_round(roi * 100, 1)}%) with strong confidence and controlled risk."
    if roi3 is not None and roi >= 0.12 and c >= 60 and r <= 55 and overall >= 70:
        return "BUY", f"Positive expected ROI (~{_round(roi * 100, 1)}%) with acceptable risk/confidence."

    if overall >= 82 and r <= 55:
        return "BUY", f"Strong overall score ({_round(overall, 1)}) with controlled risk ({_round(r, 1)})."
    if overall >= 65:
        return "HOLD", f"Moderate overall score ({_round(overall, 1)})."
    if overall >= 50:
        return "REDUCE", f"Weak overall score ({_round(overall, 1)})."
    return "SELL", f"Very weak overall score ({_round(overall, 1)})."


# =============================================================================
# Public API
# =============================================================================
def compute_scores(row: Dict[str, Any], *, settings: Any = None) -> Dict[str, Any]:
    """
    Main entrypoint (preferred).
    Returns a patch dict to merge into row.

    Output keys match schema:
      valuation_score, momentum_score, quality_score, growth_score,
      value_score, confidence_score, risk_score, overall_score, opportunity_score,
      confidence_bucket, risk_bucket, recommendation, recommendation_reason
    """
    row = row or {}
    w = _weights_from_settings(settings)

    valuation = _valuation_score(row)
    momentum = _momentum_score(row)
    quality = _quality_score(row)
    growth = _growth_score(row)

    confidence100, conf01 = _confidence_score(row)  # (0..100, 0..1)
    risk = _risk_score(row)
    opportunity = _opportunity_score(row, valuation, momentum)

    # schema: value_score is a component score (0..100)
    value_score = valuation

    # overall base blend (0..1)
    base_parts: List[Tuple[float, float]] = []
    if valuation is not None:
        base_parts.append((w.w_valuation, valuation / 100.0))
    if momentum is not None:
        base_parts.append((w.w_momentum, momentum / 100.0))
    if quality is not None:
        base_parts.append((w.w_quality, quality / 100.0))
    if growth is not None:
        base_parts.append((w.w_growth, growth / 100.0))
    if opportunity is not None:
        base_parts.append((w.w_opportunity, opportunity / 100.0))

    overall: Optional[float] = None
    overall_raw: Optional[float] = None
    penalty_factor: Optional[float] = None

    if base_parts:
        wsum = sum(x[0] for x in base_parts)
        base01 = sum(weight * value for weight, value in base_parts) / max(1e-9, wsum)

        overall_raw = _round(100.0 * _clamp(base01, 0.0, 1.0), 2)

        risk01 = (risk / 100.0) if risk is not None else 0.50
        conf01_used = conf01 if conf01 is not None else 0.55

        # penalties (0..1 multiplicative)
        risk_pen = (1.0 - w.risk_penalty_strength * (risk01 * 0.70))
        conf_pen = (1.0 - w.confidence_penalty_strength * ((1.0 - conf01_used) * 0.80))
        risk_pen = _clamp(risk_pen, 0.0, 1.0)
        conf_pen = _clamp(conf_pen, 0.0, 1.0)
        penalty_factor = _round(risk_pen * conf_pen, 4)

        base01 *= (risk_pen * conf_pen)
        overall = _round(100.0 * _clamp(base01, 0.0, 1.0), 2)

    # buckets
    rb = _risk_bucket(risk)
    cb = _confidence_bucket(conf01)

    # recommendation
    roi3 = _as_fraction(_get(row, "expected_roi_3m", "expected_return_3m"))
    rec, reason = _recommendation(overall, risk, confidence100, roi3)

    patch: Dict[str, Any] = {
        "valuation_score": valuation,
        "momentum_score": momentum,
        "quality_score": quality,
        "growth_score": growth,
        "value_score": value_score,
        "opportunity_score": opportunity,
        "confidence_score": confidence100,       # 0..100 (user-friendly)
        "forecast_confidence": conf01,           # 0..1 (model-friendly)
        "confidence_bucket": cb,
        "risk_score": risk,
        "risk_bucket": rb,
        "overall_score": overall,
        # debug helpers (optional; engine may keep or drop)
        "overall_score_raw": overall_raw,
        "overall_penalty_factor": penalty_factor,
        "recommendation": rec,
        "recommendation_reason": reason,
    }

    return patch


def score_row(row: Dict[str, Any], *, settings: Any = None) -> Dict[str, Any]:
    return compute_scores(row, settings=settings)


def score_quote(row: Dict[str, Any], *, settings: Any = None) -> Dict[str, Any]:
    return compute_scores(row, settings=settings)


def rank_rows_by_overall(rows: List[Dict[str, Any]], *, key_overall: str = "overall_score") -> List[Dict[str, Any]]:
    """
    List-level helper:
      - sorts by overall_score desc (fallback opportunity_score desc)
      - assigns rank_overall starting at 1
    Returns the same list object (mutated), for convenience.
    """
    scored: List[Tuple[int, float, float]] = []
    for i, r in enumerate(rows or []):
        ov = _safe_float(r.get(key_overall))
        op = _safe_float(r.get("opportunity_score"))
        if ov is None and op is None:
            continue
        scored.append((i, ov if ov is not None else -1e9, op if op is not None else -1e9))

    scored.sort(key=lambda t: (t[1], t[2]), reverse=True)

    rank = 1
    for idx, _, _ in scored:
        rows[idx]["rank_overall"] = rank
        rank += 1
    return rows


__all__ = [
    "compute_scores",
    "score_row",
    "score_quote",
    "rank_rows_by_overall",
    "ScoreWeights",
    "DEFAULT_WEIGHTS",
    "__version__",
]
