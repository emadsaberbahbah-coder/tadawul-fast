
#!/usr/bin/env python3
# core/scoring.py
"""
================================================================================
Scoring Module — v2.2.0
(SCHEMA-ALIGNED / ENGINE-READY / CONTRACT-HARDENED / DETERMINISTIC / RANK-AWARE)
================================================================================
"""

from __future__ import annotations

import math
import os
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

__version__ = "2.2.0"
SCORING_VERSION = __version__

_UTC = timezone.utc
_RIYADH = timezone(timedelta(hours=3))


# =============================================================================
# Utilities
# =============================================================================
def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(_UTC)
    if d.tzinfo is None:
        d = d.replace(tzinfo=_UTC)
    return d.astimezone(_UTC).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(_RIYADH)
    if d.tzinfo is None:
        d = d.replace(tzinfo=_RIYADH)
    return d.astimezone(_RIYADH).isoformat()


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


def _safe_str(v: Any, default: str = "") -> str:
    try:
        s = str(v).strip()
        return s if s else default
    except Exception:
        return default


def _get(row: Mapping[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def _getf(row: Mapping[str, Any], *keys: str) -> Optional[float]:
    return _safe_float(_get(row, *keys))


def _as_fraction(x: Any) -> Optional[float]:
    """
    Convert percent-like values to fraction:
      "12%" -> 0.12
      12    -> 0.12
      0.12  -> 0.12
    """
    f = _safe_float(x)
    if f is None:
        return None
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _norm_score_0_100(v: Any) -> Optional[float]:
    f = _safe_float(v)
    if f is None:
        return None
    if 0.0 <= f <= 1.5:
        f *= 100.0
    return _clamp(f, 0.0, 100.0)


def _norm_conf_0_1(v: Any) -> Optional[float]:
    f = _safe_float(v)
    if f is None:
        return None
    if f > 1.5:
        f /= 100.0
    return _clamp(f, 0.0, 1.0)


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except Exception:
        return default


# =============================================================================
# Tunables / types
# =============================================================================
@dataclass(slots=True)
class ScoreWeights:
    w_valuation: float = 0.30
    w_momentum: float = 0.25
    w_quality: float = 0.20
    w_growth: float = 0.15
    w_opportunity: float = 0.10

    risk_penalty_strength: float = 0.55
    confidence_penalty_strength: float = 0.45


@dataclass(slots=True)
class ForecastParameters:
    """
    Stored internally as fractions.
    Env values are accepted as percentage points for convenience.
    """
    min_roi_1m: float = field(default_factory=lambda: _env_float("TFB_TEST_MIN_ROI_1M", -25.0) / 100.0)
    max_roi_1m: float = field(default_factory=lambda: _env_float("TFB_TEST_MAX_ROI_1M", 25.0) / 100.0)
    min_roi_3m: float = field(default_factory=lambda: _env_float("TFB_TEST_MIN_ROI_3M", -35.0) / 100.0)
    max_roi_3m: float = field(default_factory=lambda: _env_float("TFB_TEST_MAX_ROI_3M", 35.0) / 100.0)
    min_roi_12m: float = field(default_factory=lambda: _env_float("TFB_TEST_MIN_ROI_12M", -65.0) / 100.0)
    max_roi_12m: float = field(default_factory=lambda: _env_float("TFB_TEST_MAX_ROI_12M", 65.0) / 100.0)

    ratio_1m_of_12m: float = 0.18
    ratio_3m_of_12m: float = 0.42


@dataclass(slots=True)
class AssetScores:
    valuation_score: Optional[float] = None
    momentum_score: Optional[float] = None
    quality_score: Optional[float] = None
    growth_score: Optional[float] = None
    value_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    confidence_score: Optional[float] = None
    forecast_confidence: Optional[float] = None
    confidence_bucket: Optional[str] = None
    risk_score: Optional[float] = None
    risk_bucket: Optional[str] = None
    overall_score: Optional[float] = None
    overall_score_raw: Optional[float] = None
    overall_penalty_factor: Optional[float] = None
    recommendation: str = "HOLD"
    recommendation_reason: str = "Insufficient data."
    forecast_price_1m: Optional[float] = None
    forecast_price_3m: Optional[float] = None
    forecast_price_12m: Optional[float] = None
    expected_roi_1m: Optional[float] = None
    expected_roi_3m: Optional[float] = None
    expected_roi_12m: Optional[float] = None
    expected_return_1m: Optional[float] = None
    expected_return_3m: Optional[float] = None
    expected_return_12m: Optional[float] = None
    expected_price_1m: Optional[float] = None
    expected_price_3m: Optional[float] = None
    expected_price_12m: Optional[float] = None
    scoring_updated_utc: str = field(default_factory=_utc_iso)
    scoring_updated_riyadh: str = field(default_factory=_riyadh_iso)
    scoring_errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


ScoringWeights = ScoreWeights

DEFAULT_WEIGHTS = ScoreWeights()
DEFAULT_FORECASTS = ForecastParameters()


def _weights_from_settings(settings: Any) -> ScoreWeights:
    w = replace(DEFAULT_WEIGHTS)
    if settings is None:
        return w

    def _try(name: str, default: float) -> float:
        try:
            if isinstance(settings, Mapping):
                v = settings.get(name)
            else:
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
    if score <= 35:
        return "Low"
    if score <= 65:
        return "Moderate"
    return "High"


def _confidence_bucket(conf01: Optional[float]) -> Optional[str]:
    if conf01 is None:
        return None
    if conf01 >= 0.75:
        return "High"
    if conf01 >= 0.50:
        return "Medium"
    return "Low"


# =============================================================================
# Forecast helpers
# =============================================================================
def _forecast_params_from_settings(settings: Any) -> ForecastParameters:
    fp = replace(DEFAULT_FORECASTS)
    if settings is None:
        return fp

    def _try_fraction(name: str, current: float) -> float:
        try:
            if isinstance(settings, Mapping):
                v = settings.get(name)
            else:
                v = getattr(settings, name, None)
            if v is None:
                return current
            f = float(v)
            if _is_nan(f):
                return current
            if abs(f) > 1.5:
                f /= 100.0
            return f
        except Exception:
            return current

    fp.min_roi_1m = _try_fraction("min_roi_1m", fp.min_roi_1m)
    fp.max_roi_1m = _try_fraction("max_roi_1m", fp.max_roi_1m)
    fp.min_roi_3m = _try_fraction("min_roi_3m", fp.min_roi_3m)
    fp.max_roi_3m = _try_fraction("max_roi_3m", fp.max_roi_3m)
    fp.min_roi_12m = _try_fraction("min_roi_12m", fp.min_roi_12m)
    fp.max_roi_12m = _try_fraction("max_roi_12m", fp.max_roi_12m)
    return fp


def _derive_forecast_patch(row: Mapping[str, Any], forecasts: ForecastParameters) -> Tuple[Dict[str, Any], List[str]]:
    patch: Dict[str, Any] = {}
    errors: List[str] = []

    price = _getf(row, "current_price", "price", "last_price", "last")
    fair = _getf(
        row,
        "intrinsic_value",
        "fair_value",
        "target_price",
        "target_mean_price",
        "forecast_price_12m",
        "forecast_price_3m",
        "forecast_price_1m",
    )

    roi1 = _as_fraction(_get(row, "expected_roi_1m", "expected_return_1m"))
    roi3 = _as_fraction(_get(row, "expected_roi_3m", "expected_return_3m"))
    roi12 = _as_fraction(_get(row, "expected_roi_12m", "expected_return_12m"))

    fp1 = _getf(row, "forecast_price_1m", "expected_price_1m")
    fp3 = _getf(row, "forecast_price_3m", "expected_price_3m")
    fp12 = _getf(row, "forecast_price_12m", "expected_price_12m")

    if price is not None and price > 0:
        if roi12 is None and fp12 is not None and fp12 > 0:
            roi12 = (fp12 / price) - 1.0
        if roi3 is None and fp3 is not None and fp3 > 0:
            roi3 = (fp3 / price) - 1.0
        if roi1 is None and fp1 is not None and fp1 > 0:
            roi1 = (fp1 / price) - 1.0

    if price is not None and price > 0 and roi12 is None and fair is not None and fair > 0:
        roi12 = (fair / price) - 1.0

    if roi12 is not None:
        roi12 = _clamp(roi12, forecasts.min_roi_12m, forecasts.max_roi_12m)
    if roi3 is None and roi12 is not None:
        roi3 = _clamp(roi12 * forecasts.ratio_3m_of_12m, forecasts.min_roi_3m, forecasts.max_roi_3m)
    if roi1 is None and roi12 is not None:
        roi1 = _clamp(roi12 * forecasts.ratio_1m_of_12m, forecasts.min_roi_1m, forecasts.max_roi_1m)

    if roi3 is not None:
        roi3 = _clamp(roi3, forecasts.min_roi_3m, forecasts.max_roi_3m)
    if roi1 is not None:
        roi1 = _clamp(roi1, forecasts.min_roi_1m, forecasts.max_roi_1m)

    if price is not None and price > 0:
        if fp12 is None and roi12 is not None:
            fp12 = price * (1.0 + roi12)
        if fp3 is None and roi3 is not None:
            fp3 = price * (1.0 + roi3)
        if fp1 is None and roi1 is not None:
            fp1 = price * (1.0 + roi1)
    else:
        if fair is None:
            errors.append("price_unavailable_for_forecast")

    patch["forecast_price_1m"] = _round(fp1, 4)
    patch["forecast_price_3m"] = _round(fp3, 4)
    patch["forecast_price_12m"] = _round(fp12, 4)

    patch["expected_roi_1m"] = _round(roi1, 6)
    patch["expected_roi_3m"] = _round(roi3, 6)
    patch["expected_roi_12m"] = _round(roi12, 6)

    patch["expected_return_1m"] = patch["expected_roi_1m"]
    patch["expected_return_3m"] = patch["expected_roi_3m"]
    patch["expected_return_12m"] = patch["expected_roi_12m"]

    patch["expected_price_1m"] = patch["forecast_price_1m"]
    patch["expected_price_3m"] = patch["forecast_price_3m"]
    patch["expected_price_12m"] = patch["forecast_price_12m"]

    return patch, errors


# =============================================================================
# Component scoring
# =============================================================================
def _data_quality_factor(row: Mapping[str, Any]) -> float:
    dq = str(_get(row, "data_quality") or "").strip().upper()
    return {
        "EXCELLENT": 0.95,
        "HIGH": 0.85,
        "GOOD": 0.80,
        "MEDIUM": 0.68,
        "FAIR": 0.60,
        "POOR": 0.40,
        "STALE": 0.45,
        "MISSING": 0.20,
        "ERROR": 0.15,
    }.get(dq, 0.60)


def _completeness_factor(row: Mapping[str, Any]) -> float:
    core_fields = [
        "symbol", "name", "currency", "exchange", "current_price", "previous_close",
        "day_high", "day_low", "week_52_high", "week_52_low", "volume", "market_cap",
        "pe_ttm", "pb_ratio", "ps_ratio", "dividend_yield", "rsi_14",
        "volatility_30d", "volatility_90d", "expected_roi_3m", "forecast_price_3m",
        "forecast_confidence",
    ]
    present = 0
    for k in core_fields:
        v = row.get(k)
        if v is not None and v != "" and v != [] and v != {}:
            present += 1
    return present / max(1, len(core_fields))


def _quality_score(row: Mapping[str, Any]) -> Optional[float]:
    dq = _data_quality_factor(row)
    comp = _completeness_factor(row)
    return _round(100.0 * _clamp(0.55 * dq + 0.45 * comp, 0.0, 1.0), 2)


def _confidence_score(row: Mapping[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    fc = _safe_float(_get(row, "forecast_confidence", "ai_confidence", "confidence_score", "confidence"))
    if fc is not None:
        fc01 = (fc / 100.0) if fc > 1.5 else fc
        fc01 = _clamp(fc01, 0.0, 1.0)
        return _round(fc01 * 100.0, 2), _round(fc01, 4)

    dq = _data_quality_factor(row)
    comp = _completeness_factor(row)

    provs = row.get("data_sources") or row.get("providers") or []
    try:
        pcount = len(provs) if isinstance(provs, list) else 0
    except Exception:
        pcount = 0
    prov_factor = _clamp(pcount / 3.0, 0.0, 1.0)

    conf01 = _clamp(0.55 * dq + 0.35 * comp + 0.10 * prov_factor, 0.0, 1.0)
    return _round(conf01 * 100.0, 2), _round(conf01, 4)


def _valuation_score(row: Mapping[str, Any]) -> Optional[float]:
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

    upside_n = _roi_to_score(upside, 0.50)
    roi3_n = _roi_to_score(roi3, 0.35)
    roi12_n = _roi_to_score(roi12, 0.80)

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
        _low_is_good(pe, 8.0, 35.0),
        _low_is_good(pb, 0.8, 6.0),
        _low_is_good(ps, 1.0, 10.0),
        _low_is_good(peg, 0.8, 4.0),
        _low_is_good(ev, 6.0, 25.0),
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


def _growth_score(row: Mapping[str, Any]) -> Optional[float]:
    g = _as_fraction(_get(row, "revenue_growth_yoy", "revenue_growth", "growth_yoy"))
    if g is None:
        return None
    return _round(_clamp(((g + 0.30) / 0.60) * 100.0, 0.0, 100.0), 2)


def _momentum_score(row: Mapping[str, Any]) -> Optional[float]:
    pct = _as_fraction(_get(row, "percent_change", "change_pct", "change_percent"))
    rsi = _getf(row, "rsi_14", "rsi", "rsi14")
    pos = _as_fraction(_get(row, "week_52_position_pct", "position_52w_pct", "week52_position_pct"))

    parts: List[Tuple[float, float]] = []
    if pct is not None:
        parts.append((0.40, _clamp((pct + 0.10) / 0.20, 0.0, 1.0)))
    if rsi is not None:
        x = (rsi - 55.0) / 12.0
        parts.append((0.35, _clamp(math.exp(-(x * x)), 0.0, 1.0)))
    if pos is not None:
        parts.append((0.25, _clamp(pos, 0.0, 1.0)))

    if not parts:
        return None

    wsum = sum(w for w, _ in parts)
    return _round(100.0 * _clamp(sum(w * v for w, v in parts) / max(1e-9, wsum), 0.0, 1.0), 2)


def _risk_score(row: Mapping[str, Any]) -> Optional[float]:
    vol90 = _as_fraction(_get(row, "volatility_90d"))
    dd1y = _as_fraction(_get(row, "max_drawdown_1y"))
    var1d = _as_fraction(_get(row, "var_95_1d"))
    sharpe = _getf(row, "sharpe_1y")

    def _scale(x: Optional[float], lo: float, hi: float) -> Optional[float]:
        if x is None:
            return None
        return _clamp((x - lo) / (hi - lo), 0.0, 1.0) if hi > lo else None

    parts: List[Tuple[float, float]] = []
    if vol90 is not None:
        parts.append((0.40, _scale(vol90, 0.12, 0.70) or 0.0))
    if dd1y is not None:
        parts.append((0.35, _scale(abs(dd1y), 0.05, 0.55) or 0.0))
    if var1d is not None:
        parts.append((0.20, _scale(var1d, 0.01, 0.08) or 0.0))
    if sharpe is not None:
        parts.append((0.05, _clamp(1.0 - _clamp((sharpe + 0.5) / 2.5, 0.0, 1.0), 0.0, 1.0)))

    if not parts:
        vol = _as_fraction(_get(row, "volatility_30d", "vol_30d"))
        beta = _getf(row, "beta_5y", "beta")
        dd = _as_fraction(_get(row, "max_drawdown_30d", "drawdown_30d"))
        if vol is not None:
            parts.append((0.50, _scale(vol, 0.10, 0.60) or 0.0))
        if beta is not None:
            parts.append((0.30, _scale(beta, 0.60, 2.00) or 0.0))
        if dd is not None:
            parts.append((0.20, _scale(abs(dd), 0.00, 0.50) or 0.0))

    if not parts:
        return None

    wsum = sum(w for w, _ in parts)
    return _round(100.0 * _clamp(sum(w * v for w, v in parts) / max(1e-9, wsum), 0.0, 1.0), 2)


def _opportunity_score(row: Mapping[str, Any], valuation: Optional[float], momentum: Optional[float]) -> Optional[float]:
    roi1 = _as_fraction(_get(row, "expected_roi_1m", "expected_return_1m"))
    roi3 = _as_fraction(_get(row, "expected_roi_3m", "expected_return_3m"))
    roi12 = _as_fraction(_get(row, "expected_roi_12m", "expected_return_12m"))

    def _roi_norm(frac: Optional[float], cap: float) -> Optional[float]:
        if frac is None:
            return None
        return _clamp((frac + cap) / (2 * cap), 0.0, 1.0)

    parts: List[Tuple[float, float]] = []
    r3 = _roi_norm(roi3, 0.35)
    r12 = _roi_norm(roi12, 0.80)
    r1 = _roi_norm(roi1, 0.25)

    if r3 is not None:
        parts.append((0.55, r3))
    if r12 is not None:
        parts.append((0.30, r12))
    if r1 is not None:
        parts.append((0.15, r1))

    if parts:
        wsum = sum(w for w, _ in parts)
        return _round(100.0 * _clamp(sum(w * v for w, v in parts) / max(1e-9, wsum), 0.0, 1.0), 2)

    if valuation is None and momentum is None:
        return None

    v = (valuation or 50.0) / 100.0
    m = (momentum or 50.0) / 100.0
    return _round(100.0 * _clamp(0.60 * v + 0.40 * m, 0.0, 1.0), 2)


def _recommendation(overall: Optional[float], risk: Optional[float], confidence100: Optional[float], roi3: Optional[float]) -> Tuple[str, str]:
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
    Main entrypoint.
    Returns a patch dict to merge into row.
    """
    source = dict(row or {})
    scoring_errors: List[str] = []

    forecasts = _forecast_params_from_settings(settings)
    forecast_patch, forecast_errors = _derive_forecast_patch(source, forecasts)
    scoring_errors.extend(forecast_errors)

    working = dict(source)
    working.update({k: v for k, v in forecast_patch.items() if v is not None})

    weights = _weights_from_settings(settings)

    valuation = _valuation_score(working)
    momentum = _momentum_score(working)
    quality = _quality_score(working)
    growth = _growth_score(working)
    confidence100, conf01 = _confidence_score(working)
    risk = _risk_score(working)
    opportunity = _opportunity_score(working, valuation, momentum)
    value_score = valuation

    base_parts: List[Tuple[float, float]] = []
    if valuation is not None:
        base_parts.append((weights.w_valuation, valuation / 100.0))
    if momentum is not None:
        base_parts.append((weights.w_momentum, momentum / 100.0))
    if quality is not None:
        base_parts.append((weights.w_quality, quality / 100.0))
    if growth is not None:
        base_parts.append((weights.w_growth, growth / 100.0))
    if opportunity is not None:
        base_parts.append((weights.w_opportunity, opportunity / 100.0))

    overall: Optional[float] = None
    overall_raw: Optional[float] = None
    penalty_factor: Optional[float] = None

    if base_parts:
        wsum = sum(x[0] for x in base_parts)
        base01 = sum(weight * value for weight, value in base_parts) / max(1e-9, wsum)
        overall_raw = _round(100.0 * _clamp(base01, 0.0, 1.0), 2)

        risk01 = (risk / 100.0) if risk is not None else 0.50
        conf01_used = conf01 if conf01 is not None else 0.55

        risk_pen = _clamp(1.0 - weights.risk_penalty_strength * (risk01 * 0.70), 0.0, 1.0)
        conf_pen = _clamp(1.0 - weights.confidence_penalty_strength * ((1.0 - conf01_used) * 0.80), 0.0, 1.0)
        penalty_factor = _round(risk_pen * conf_pen, 4)

        base01 *= (risk_pen * conf_pen)
        overall = _round(100.0 * _clamp(base01, 0.0, 1.0), 2)
    else:
        overall = 50.0
        overall_raw = 50.0
        penalty_factor = 1.0
        scoring_errors.append("insufficient_scoring_inputs")

    rb = _risk_bucket(risk)
    cb = _confidence_bucket(conf01)

    roi3 = _as_fraction(working.get("expected_roi_3m"))
    rec, reason = _recommendation(overall, risk, confidence100, roi3)

    scores = AssetScores(
        valuation_score=valuation,
        momentum_score=momentum,
        quality_score=quality,
        growth_score=growth,
        value_score=value_score,
        opportunity_score=opportunity,
        confidence_score=confidence100,
        forecast_confidence=conf01,
        confidence_bucket=cb,
        risk_score=risk,
        risk_bucket=rb,
        overall_score=overall,
        overall_score_raw=overall_raw,
        overall_penalty_factor=penalty_factor,
        recommendation=rec,
        recommendation_reason=reason,
        forecast_price_1m=forecast_patch.get("forecast_price_1m"),
        forecast_price_3m=forecast_patch.get("forecast_price_3m"),
        forecast_price_12m=forecast_patch.get("forecast_price_12m"),
        expected_roi_1m=forecast_patch.get("expected_roi_1m"),
        expected_roi_3m=forecast_patch.get("expected_roi_3m"),
        expected_roi_12m=forecast_patch.get("expected_roi_12m"),
        expected_return_1m=forecast_patch.get("expected_return_1m"),
        expected_return_3m=forecast_patch.get("expected_return_3m"),
        expected_return_12m=forecast_patch.get("expected_return_12m"),
        expected_price_1m=forecast_patch.get("expected_price_1m"),
        expected_price_3m=forecast_patch.get("expected_price_3m"),
        expected_price_12m=forecast_patch.get("expected_price_12m"),
        scoring_errors=scoring_errors,
    )
    return scores.to_dict()


def score_row(row: Dict[str, Any], *, settings: Any = None) -> Dict[str, Any]:
    return compute_scores(row, settings=settings)


def score_quote(row: Dict[str, Any], *, settings: Any = None) -> Dict[str, Any]:
    return compute_scores(row, settings=settings)


def enrich_with_scores(row: Dict[str, Any], *, settings: Any = None, in_place: bool = False) -> Dict[str, Any]:
    target = row if in_place else dict(row or {})
    patch = compute_scores(target, settings=settings)
    target.update(patch)
    return target


class ScoringEngine:
    """
    Lightweight wrapper to support object-style callers and later bridge modules.
    """

    version = SCORING_VERSION

    def __init__(self, *, settings: Any = None, weights: Optional[ScoreWeights] = None, forecasts: Optional[ForecastParameters] = None):
        self.settings = settings
        self.weights = weights or replace(DEFAULT_WEIGHTS)
        self.forecasts = forecasts or replace(DEFAULT_FORECASTS)

    def compute_scores(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return compute_scores(row, settings=self.settings)

    def enrich_with_scores(self, row: Dict[str, Any], *, in_place: bool = False) -> Dict[str, Any]:
        return enrich_with_scores(row, settings=self.settings, in_place=in_place)


# =============================================================================
# Ranking helpers
# =============================================================================
def _rank_sort_tuple(row: Dict[str, Any], *, key_overall: str = "overall_score") -> Tuple[float, float, float, float, float, str]:
    overall = _norm_score_0_100(row.get(key_overall))
    opp = _norm_score_0_100(row.get("opportunity_score"))
    conf = _norm_score_0_100(row.get("confidence_score"))
    risk = _norm_score_0_100(row.get("risk_score"))
    roi3 = _as_fraction(row.get("expected_roi_3m"))

    return (
        overall if overall is not None else -1e9,
        opp if opp is not None else -1e9,
        conf if conf is not None else -1e9,
        -(risk if risk is not None else 1e9),
        roi3 if roi3 is not None else -1e9,
        _safe_str(row.get("symbol"), "~"),
    )


def assign_rank_overall(
    rows: Sequence[Dict[str, Any]],
    *,
    key_overall: str = "overall_score",
    inplace: bool = True,
    rank_key: str = "rank_overall",
) -> List[Dict[str, Any]]:
    target = list(rows) if inplace else [dict(r or {}) for r in rows]
    indexed: List[Tuple[int, Dict[str, Any]]] = [(i, r) for i, r in enumerate(target)]

    indexed.sort(key=lambda item: _rank_sort_tuple(item[1], key_overall=key_overall), reverse=True)

    for rank, (_, row) in enumerate(indexed, start=1):
        row[rank_key] = rank

    return target


def rank_rows_by_overall(rows: List[Dict[str, Any]], *, key_overall: str = "overall_score") -> List[Dict[str, Any]]:
    return assign_rank_overall(rows, key_overall=key_overall, inplace=True, rank_key="rank_overall")


def score_and_rank_rows(
    rows: Sequence[Dict[str, Any]],
    *,
    settings: Any = None,
    key_overall: str = "overall_score",
    inplace: bool = False,
) -> List[Dict[str, Any]]:
    prepared = list(rows) if inplace else [dict(r or {}) for r in rows]

    for row in prepared:
        try:
            row.update(compute_scores(row, settings=settings))
        except Exception:
            pass

    assign_rank_overall(prepared, key_overall=key_overall, inplace=True, rank_key="rank_overall")
    return prepared


__all__ = [
    "compute_scores",
    "score_row",
    "score_quote",
    "enrich_with_scores",
    "rank_rows_by_overall",
    "assign_rank_overall",
    "score_and_rank_rows",
    "AssetScores",
    "ScoreWeights",
    "ScoringWeights",
    "ForecastParameters",
    "ScoringEngine",
    "DEFAULT_WEIGHTS",
    "DEFAULT_FORECASTS",
    "SCORING_VERSION",
    "__version__",
]
