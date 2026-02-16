# core/scoring_engine_contract_test.py
"""
core/scoring_engine_contract_test.py
===========================================================
Contract Tests for Scoring Engine (v1.1.0) — PROD SAFE

Purpose
- Validate that core.scoring_engine outputs remain aligned with:
  - core/scoring_engine.py canonical forecast keys
  - core/schemas.py vNext headers / mapping expectations
  - Router normalization expectations (enum reco, ISO timestamps)

Design
- No network calls
- No external data providers
- No pytest required (runs as plain python)
- Returns process exit code 0 on success, 1 on failure

Usage
  python -m core.scoring_engine_contract_test
or
  python core/scoring_engine_contract_test.py
"""

from __future__ import annotations

import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

# Import engine (must exist)
from core.scoring_engine import SCORING_ENGINE_VERSION, compute_scores, enrich_with_scores  # type: ignore


TEST_VERSION = "1.1.0"
_RIYADH_TZ = timezone(timedelta(hours=3))
_RECO_ENUM = {"BUY", "HOLD", "REDUCE", "SELL"}


# -----------------------------
# Minimal local helpers
# -----------------------------
def _is_finite_number(x: Any) -> bool:
    try:
        v = float(x)
        return not (math.isnan(v) or math.isinf(v))
    except Exception:
        return False


def _assert(cond: bool, msg: str, errors: List[str]) -> None:
    if not cond:
        errors.append(msg)


def _parse_iso(dt: Any) -> Optional[datetime]:
    if dt is None:
        return None
    try:
        s = str(dt).strip()
        if not s:
            return None
        # allow "Z"
        s = s.replace("Z", "+00:00")
        d = datetime.fromisoformat(s)
        # if naive, assume UTC
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d
    except Exception:
        return None


def _approx_hours_diff(a: datetime, b: datetime) -> float:
    return abs((a - b).total_seconds()) / 3600.0


def _as_dict(obj: Any) -> Dict[str, Any]:
    if isinstance(obj, dict):
        return obj
    # pydantic v2
    if hasattr(obj, "model_dump"):
        try:
            return obj.model_dump()
        except Exception:
            pass
    # pydantic v1
    if hasattr(obj, "dict"):
        try:
            return obj.dict()
        except Exception:
            pass
    # generic object
    out: Dict[str, Any] = {}
    for k in dir(obj):
        if k.startswith("_"):
            continue
        try:
            v = getattr(obj, k)
        except Exception:
            continue
        if callable(v):
            continue
        out[k] = v
    return out


# -----------------------------
# Test Fixtures (synthetic quotes)
# -----------------------------
@dataclass
class QuoteFixture:
    name: str
    q: Dict[str, Any]
    expect_forecast: bool


def _fixtures() -> List[QuoteFixture]:
    # A healthy equity-like quote (should forecast)
    base_ok = {
        "symbol": "1120.SR",
        "current_price": 100.0,
        "previous_close": 99.0,
        "percent_change": 1.01,
        "volume": 2_000_000,
        "avg_volume_30d": 2_500_000,
        "market_cap": 50_000_000_000,
        "liquidity_score": 75,
        "pe_ttm": 14.0,
        "pb": 1.6,
        "ev_ebitda": 9.0,
        "dividend_yield": 0.035,  # fraction -> 3.5%
        "payout_ratio": "55%",
        "roe": 0.18,  # 18%
        "roa": 0.08,  # 8%
        "net_margin": 0.12,  # 12%
        "ebitda_margin": 0.22,  # 22%
        "revenue_growth": 0.10,  # 10%
        "net_income_growth": 0.12,  # 12%
        "beta": 1.1,
        "volatility_30d": 0.22,  # 22%
        "rsi_14": 58,
        "trend_signal": "UPTREND",
        "macd_hist": 0.4,
        "fair_value": 120.0,
        "data_quality": "OK",
        "news_score": 72,  # 0..100 scale
        "forecast_updated_utc": "2026-02-15T12:00:00+00:00",
    }

    # Missing price => forecast should be absent, scoring still works
    no_price = {
        "symbol": "NOPRICE",
        "current_price": None,
        "fair_value": 120.0,
        "pe_ttm": 10.0,
        "data_quality": "PARTIAL",
    }

    # Zero price => treat same as missing
    zero_price = {
        "symbol": "ZERO_PRICE",
        "current_price": 0.0,
        "fair_value": 100.0,
        "data_quality": "BAD",
    }

    # Downtrend + high risk case
    risky_down = {
        "symbol": "RISKY",
        "current_price": 50.0,
        "previous_close": 53.0,
        "percent_change": -5.66,
        "avg_volume_30d": 60_000,
        "liquidity_score": 20,
        "beta": 2.1,
        "volatility_30d": 0.65,  # 65%
        "debt_to_equity": 3.1,
        "trend_signal": "DOWNTREND",
        "macd_hist": -0.8,
        "rsi_14": 82,
        "fair_value": 45.0,
        "data_quality": "PARTIAL",
        "news_score": -6,  # -10..+10 style
        "forecast_updated_utc": "2026-02-15T10:30:00Z",
    }

    # Provider gives gross 12m expected return; ensure engine uses it as gross
    gross_hint = {
        "symbol": "GROSS",
        "current_price": 20.0,
        "expected_return_gross_12m": 30.0,  # percent
        "trend_signal": "NEUTRAL",
        "liquidity_score": 55,
        "volatility_30d": 0.28,
        "data_quality": "OK",
    }

    # Extreme inputs that should be clamped
    extreme_inputs = {
        "symbol": "EXTREME",
        "current_price": 1000.0,
        "fair_value": 5000.0,  # +400% upside
        "volatility_30d": 5.0, # 500%
        "rsi_14": 150, # invalid > 100
        "beta": 15.0,
        "data_quality": "OK",
    }

    # Non-equity asset (e.g. Commodity/FX) often lacks fundamentals
    non_equity = {
        "symbol": "GC=F",
        "current_price": 2000.0,
        "trend_signal": "UPTREND",
        "rsi_14": 65,
        "volatility_30d": 0.15,
        "data_quality": "OK", # price exists
        # No PE, EPS, etc.
    }

    return [
        QuoteFixture("base_ok", base_ok, True),
        QuoteFixture("no_price", no_price, False),
        QuoteFixture("zero_price", zero_price, False),
        QuoteFixture("risky_down", risky_down, True),
        QuoteFixture("gross_hint", gross_hint, True),
        QuoteFixture("extreme_inputs", extreme_inputs, True), # should clamp forecasts
        QuoteFixture("non_equity", non_equity, False), # likely no forecast without fair_value
    ]


# -----------------------------
# Contract Checks
# -----------------------------
def _check_required_keys(d: Dict[str, Any], errors: List[str], ctx: str) -> None:
    required = [
        "value_score",
        "quality_score",
        "momentum_score",
        "risk_score",
        "opportunity_score",
        "overall_score",
        "recommendation",
        "confidence",
        # canonical forecast fields must exist as keys (may be None)
        "forecast_price_1m",
        "forecast_price_3m",
        "forecast_price_12m",
        "expected_roi_1m",
        "expected_roi_3m",
        "expected_roi_12m",
        "forecast_confidence",
        "forecast_method",
        "forecast_updated_utc",
        "forecast_updated_riyadh",
    ]
    for k in required:
        _assert(k in d, f"[{ctx}] missing key: {k}", errors)


def _check_score_ranges(d: Dict[str, Any], errors: List[str], ctx: str) -> None:
    for k in [
        "value_score",
        "quality_score",
        "momentum_score",
        "risk_score",
        "opportunity_score",
        "overall_score",
        "confidence",
    ]:
        v = d.get(k)
        _assert(_is_finite_number(v), f"[{ctx}] {k} not finite number: {v}", errors)
        if _is_finite_number(v):
            vf = float(v)
            _assert(0.0 <= vf <= 100.0, f"[{ctx}] {k} out of range 0..100: {vf}", errors)


def _check_reco_enum(d: Dict[str, Any], errors: List[str], ctx: str) -> None:
    r = str(d.get("recommendation") or "").strip().upper()
    _assert(r in _RECO_ENUM, f"[{ctx}] recommendation invalid: {r}", errors)


def _check_forecast_contract(
    d: Dict[str, Any], errors: List[str], ctx: str, *, expect_forecast: bool
) -> None:
    # When forecast is expected: canonical prices/roi must be finite and timestamps parseable.
    fp1 = d.get("forecast_price_1m")
    roi1 = d.get("expected_roi_1m")

    has_forecast = _is_finite_number(fp1) and _is_finite_number(roi1)

    if expect_forecast:
        # It's possible extreme inputs might still fail to generate a forecast if filters apply, 
        # but generally we expect it. We'll warn if missing.
        if not has_forecast:
             # Check if it was because of missing critical inputs despite fixture flag
             # For now, treat as error if fixture says True
             _assert(has_forecast, f"[{ctx}] expected forecast but values are missing/invalid", errors)
    else:
        # if no forecast expected, allow None. If present, validate it.
        if not has_forecast:
            return

    # Price forecasts must be > 0
    for k in ("forecast_price_1m", "forecast_price_3m", "forecast_price_12m"):
        v = d.get(k)
        _assert(_is_finite_number(v), f"[{ctx}] {k} not finite: {v}", errors)
        if _is_finite_number(v):
            _assert(float(v) > 0, f"[{ctx}] {k} must be > 0: {v}", errors)

    # ROI forecasts must be finite and bounded (as per engine v1.8.0)
    for k, lo, hi in (
        ("expected_roi_1m", -35.0, 35.0),
        ("expected_roi_3m", -45.0, 45.0),
        ("expected_roi_12m", -60.0, 60.0),
    ):
        v = d.get(k)
        _assert(_is_finite_number(v), f"[{ctx}] {k} not finite: {v}", errors)
        if _is_finite_number(v):
            vf = float(v)
            _assert(lo <= vf <= hi, f"[{ctx}] {k} out of bound {lo}..{hi}: {vf}", errors)

    # Forecast confidence must be 0..100 if present
    fc = d.get("forecast_confidence")
    _assert(_is_finite_number(fc), f"[{ctx}] forecast_confidence not finite: {fc}", errors)
    if _is_finite_number(fc):
        _assert(0.0 <= float(fc) <= 100.0, f"[{ctx}] forecast_confidence out of 0..100: {fc}", errors)

    # ISO timestamps must parse and Riyadh must be around UTC+3 (within tolerance)
    utc_iso = d.get("forecast_updated_utc")
    riy_iso = d.get("forecast_updated_riyadh")
    utc_dt = _parse_iso(utc_iso)
    riy_dt = _parse_iso(riy_iso)

    _assert(utc_dt is not None, f"[{ctx}] forecast_updated_utc not parseable ISO: {utc_iso}", errors)
    _assert(riy_dt is not None, f"[{ctx}] forecast_updated_riyadh not parseable ISO: {riy_iso}", errors)

    if utc_dt and riy_dt:
        # Convert to same tz and check about 3 hours diff (allow DST/format variations ±0.5h)
        utc_as_riy = utc_dt.astimezone(_RIYADH_TZ)
        # Using total_seconds to avoid issues with naive vs aware subtraction if helper failed
        diff_h = _approx_hours_diff(utc_as_riy, riy_dt.astimezone(_RIYADH_TZ))
        # They should be virtually identical time instants, just different offsets. 
        # Wait, the stored strings represent the SAME instant in different TZs? 
        # Yes, usually "2023-01-01T12:00Z" and "2023-01-01T15:00+03:00".
        # So comparing them as datetimes should yield 0 difference.
        
        diff_absolute = abs((utc_dt - riy_dt).total_seconds())
        _assert(diff_absolute <= 60, f"[{ctx}] Riyadh time mismatch (diff={diff_absolute}s)", errors)


def _check_enrich_path(q: Dict[str, Any], errors: List[str], ctx: str) -> None:
    # enrich_with_scores should return dict if dict in, and include scoring_version
    enriched = enrich_with_scores(q)
    _assert(isinstance(enriched, dict), f"[{ctx}] enrich_with_scores(dict) must return dict", errors)
    ed = _as_dict(enriched)
    _assert(ed.get("scoring_version") == SCORING_ENGINE_VERSION, f"[{ctx}] scoring_version mismatch", errors)

    # Enrichment must include canonical keys
    for k in ("forecast_price_1m", "expected_roi_1m", "forecast_updated_utc", "forecast_updated_riyadh"):
        _assert(k in ed, f"[{ctx}] enrich missing key: {k}", errors)
    
    # Original keys preserved
    for k in q.keys():
         _assert(k in ed, f"[{ctx}] original key lost: {k}", errors)


def run_contract_tests(verbose: bool = True) -> Tuple[bool, List[str]]:
    errors: List[str] = []

    fx = _fixtures()
    for f in fx:
        ctx = f"fixture:{f.name}"

        # compute_scores path
        s = compute_scores(f.q)
        d = _as_dict(s)

        _check_required_keys(d, errors, ctx)
        _check_score_ranges(d, errors, ctx)
        _check_reco_enum(d, errors, ctx)
        _check_forecast_contract(d, errors, ctx, expect_forecast=f.expect_forecast)

        # enrich path
        _check_enrich_path(f.q, errors, ctx)

    ok = len(errors) == 0
    if verbose:
        if ok:
            print(f"[OK] scoring engine contract tests passed (engine={SCORING_ENGINE_VERSION}, test={TEST_VERSION})")
        else:
            print(f"[FAIL] scoring engine contract tests failed (engine={SCORING_ENGINE_VERSION}, test={TEST_VERSION})")
            for e in errors:
                print(" -", e)

    return ok, errors


def main() -> int:
    ok, _ = run_contract_tests(verbose=True)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
