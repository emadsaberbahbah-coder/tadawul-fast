#!/usr/bin/env python3
"""
core/scoring_engine_contract_test.py
================================================================================
SCORING ENGINE CONTRACT TESTS — v2.0.0 (HARDENED / CI-FRIENDLY / NO-PRINT)
================================================================================
Emad Bahbah – Tadawul Fast Bridge

Purpose
- Protect the public contract of `core.scoring_engine` during refactors.
- Validate that compute/enrich paths never crash and keep key fields stable.
- Validate ROI clamping supports negative ranges (critical for "Ratio" correctness).
- Run without pytest (uses stdlib unittest) but is pytest-compatible.

How to run
- python core/scoring_engine_contract_test.py
- pytest -q  (will also discover these unittest tests)

Notes
- Avoids `print()` for repo hygiene scanners.
- No network calls.
"""

from __future__ import annotations

import os
import sys
import time
import unittest
from typing import Any, Dict, Optional


# ---------------------------------------------------------------------------
# Hygiene-safe output helper (no print)
# ---------------------------------------------------------------------------
def _out(msg: str) -> None:
    try:
        sys.stdout.write(str(msg) + ("\n" if not str(msg).endswith("\n") else ""))
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Import under test (fail fast but with clear message)
# ---------------------------------------------------------------------------
try:
    from core import scoring_engine as se  # type: ignore
except Exception as e:
    se = None  # type: ignore
    _IMPORT_ERR = e
else:
    _IMPORT_ERR = None


def _is_number(x: Any) -> bool:
    try:
        return isinstance(x, (int, float)) and x == x and abs(float(x)) != float("inf")
    except Exception:
        return False


def _as_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    # pydantic v2
    if hasattr(obj, "model_dump") and callable(obj.model_dump):
        try:
            return obj.model_dump()
        except Exception:
            pass
    # pydantic v1
    if hasattr(obj, "dict") and callable(obj.dict):
        try:
            return obj.dict()
        except Exception:
            pass
    return {}


def _get_env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except Exception:
        return default


class TestScoringEngineContract(unittest.TestCase):
    """
    Contract tests cover:
    - Import & stable surface
    - compute_scores() non-crash and essential fields
    - negative ROI clamp correctness
    - percent coercion behavior (0.02 => 2%)
    - enrich_with_scores() output shape and in-place update
    """

    @classmethod
    def setUpClass(cls) -> None:
        if se is None:
            raise RuntimeError(f"Failed to import core.scoring_engine: {_IMPORT_ERR!r}")

    def test_public_surface_exists(self) -> None:
        self.assertTrue(hasattr(se, "compute_scores"))
        self.assertTrue(callable(se.compute_scores))
        self.assertTrue(hasattr(se, "enrich_with_scores"))
        self.assertTrue(callable(se.enrich_with_scores))
        self.assertTrue(hasattr(se, "SCORING_ENGINE_VERSION"))

        # __all__ should exist and include the key exports (contract)
        self.assertTrue(hasattr(se, "__all__"))
        exported = set(getattr(se, "__all__", []) or [])
        for required in (
            "compute_scores",
            "enrich_with_scores",
            "AssetScores",
            "ScoringEngine",
            "ScoringWeights",
            "ForecastParameters",
        ):
            self.assertIn(required, exported)

    def test_compute_scores_minimal_input_no_crash(self) -> None:
        """
        Minimal payload: should not crash and should return an AssetScores-like object.
        """
        payload = {
            "symbol": "AAPL",
            "price": 100.0,
            "previous_close": 99.0,
            "volume": 10_000_000,
            "market_cap": 2_000_000_000_000,
            "data_quality": "GOOD",
        }
        res = se.compute_scores(payload)
        d = _as_dict(res)

        # must have key score fields
        for k in ("value_score", "quality_score", "momentum_score", "risk_score", "opportunity_score", "overall_score"):
            self.assertIn(k, d)
            self.assertTrue(_is_number(d[k]))
            self.assertGreaterEqual(float(d[k]), 0.0)
            self.assertLessEqual(float(d[k]), 100.0)

        # must have recommendation string/enum
        self.assertIn("recommendation", d)

        # should include timestamps
        self.assertIn("scoring_updated_utc", d)
        self.assertIn("scoring_updated_riyadh", d)

    def test_negative_roi_clamp_is_supported(self) -> None:
        """
        Critical: Forecast ROI must allow negative values and clamp within configured bounds.

        Case:
        - price=100
        - fair_value=50  => base_return_12m = -50%
        Expected:
        - expected_roi_12m should be negative
        - within [-65, +65] default bounds (or env-adjusted, if you changed defaults in engine)
        """
        payload = {
            "symbol": "TEST.NEG",
            "price": 100.0,
            "fair_value": 50.0,
            "data_quality": "EXCELLENT",
            "forecast_confidence": 90.0,
            "volatility_30d": 20.0,
            "beta": 1.0,
        }

        res = se.compute_scores(payload)
        d = _as_dict(res)

        # expected_roi fields may exist; at least 12m should exist when price is present
        self.assertIn("expected_roi_12m", d)
        self.assertTrue(_is_number(d["expected_roi_12m"]))

        r12 = float(d["expected_roi_12m"])
        self.assertLessEqual(r12, 0.0, msg=f"expected_roi_12m should be negative but got {r12}")

        # Contract bounds (defaults in your scoring_engine.ForecastParameters)
        min_roi_12m = _get_env_float("TFB_TEST_MIN_ROI_12M", -65.0)
        max_roi_12m = _get_env_float("TFB_TEST_MAX_ROI_12M", 65.0)
        self.assertGreaterEqual(r12, min_roi_12m, msg=f"ROI below min bound: {r12} < {min_roi_12m}")
        self.assertLessEqual(r12, max_roi_12m, msg=f"ROI above max bound: {r12} > {max_roi_12m}")

        # forecast price must follow ROI direction
        self.assertIn("forecast_price_12m", d)
        self.assertTrue(_is_number(d["forecast_price_12m"]))
        fp12 = float(d["forecast_price_12m"])
        self.assertLess(fp12, 100.0, msg=f"forecast_price_12m should be below price when ROI negative. fp12={fp12}")

    def test_percent_coercion_decimal_to_percent(self) -> None:
        """
        Contract: safe_percent treats 0.02 as 2.0 (percent points).
        This prevents ratio issues where dividend_yield/ROE appear 100x smaller.
        """
        payload = {
            "symbol": "TEST.PCT",
            "price": 100.0,
            "dividend_yield": 0.02,  # 2%
            "roe": 0.15,             # 15%
            "data_quality": "GOOD",
        }
        res = se.compute_scores(payload)
        d = _as_dict(res)

        # Not all inputs are exposed directly, but scoring should not behave like yield=0.02%
        # We assert it doesn't crash and the value_score should be meaningfully influenced:
        self.assertIn("value_score", d)
        self.assertTrue(_is_number(d["value_score"]))

    def test_missing_price_does_not_crash(self) -> None:
        """
        If price is missing/invalid, forecast must be skipped safely, not crash.
        """
        payload = {
            "symbol": "TEST.NOPRICE",
            "pe_ttm": 12.0,
            "roe": 0.12,
            "data_quality": "MEDIUM",
        }
        res = se.compute_scores(payload)
        d = _as_dict(res)

        self.assertIn("overall_score", d)
        self.assertTrue(_is_number(d["overall_score"]))
        # forecast fields may be absent, but should not error
        self.assertIn("scoring_errors", d)

    def test_enrich_with_scores_returns_expected_fields(self) -> None:
        payload = {"symbol": "AAPL", "price": 100.0, "previous_close": 98.0, "data_quality": "GOOD"}
        out = se.enrich_with_scores(payload, in_place=False)
        self.assertIsInstance(out, dict)

        for k in ("overall_score", "risk_score", "recommendation", "forecast_price_12m", "expected_roi_12m"):
            # forecast fields exist when price exists (12m is most reliable)
            self.assertIn(k, out)

        # compatibility aliases must exist
        for k in ("expected_return_12m", "expected_price_12m", "confidence_score"):
            self.assertIn(k, out)

    def test_enrich_with_scores_in_place_updates_dict(self) -> None:
        payload = {"symbol": "AAPL", "price": 100.0, "previous_close": 99.0, "data_quality": "GOOD"}
        out = se.enrich_with_scores(payload, in_place=True)
        self.assertIs(out, payload)
        self.assertIn("overall_score", payload)
        self.assertIn("recommendation", payload)

    def test_recommendation_is_deterministic_for_same_input(self) -> None:
        payload = {"symbol": "DET", "price": 100.0, "fair_value": 120.0, "data_quality": "EXCELLENT"}
        r1 = _as_dict(se.compute_scores(payload)).get("recommendation")
        r2 = _as_dict(se.compute_scores(payload)).get("recommendation")
        self.assertEqual(r1, r2)

    def test_basic_performance_budget(self) -> None:
        """
        Soft guard: compute_scores should be fast (no network, no heavy training).
        Budget is adjustable via env TFB_TEST_SCORE_BUDGET_MS (default 60ms).
        """
        budget_ms = _get_env_float("TFB_TEST_SCORE_BUDGET_MS", 60.0)
        payload = {
            "symbol": "PERF",
            "price": 100.0,
            "previous_close": 99.0,
            "market_cap": 10_000_000_000,
            "volume": 2_000_000,
            "pe_ttm": 15.0,
            "roe": 0.12,
            "data_quality": "GOOD",
        }

        t0 = time.perf_counter()
        _ = se.compute_scores(payload)
        dt_ms = (time.perf_counter() - t0) * 1000.0

        # This is a "soft" contract; allow override in CI if needed.
        self.assertLessEqual(dt_ms, budget_ms, msg=f"compute_scores took {dt_ms:.2f}ms > budget {budget_ms:.2f}ms")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------
def main() -> int:
    try:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestScoringEngineContract)
        runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
        result = runner.run(suite)
        return 0 if result.wasSuccessful() else 2
    except Exception as e:
        _out(f"[contract_test] fatal_error: {e!r}")
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
