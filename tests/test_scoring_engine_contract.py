#!/usr/bin/env python3
"""
test_scoring_engine_contract.py
================================================================================
SCORING ENGINE CONTRACT TESTS -- v2.3.0 (HARDENED / CI-FRIENDLY / NO-PRINT)
================================================================================
Emad Bahbah -- Tadawul Fast Bridge

Aligned with:
  - core.scoring v2.2.0 (the authoritative implementation)
  - core.scoring_engine v2.2.0 (the compatibility bridge that re-exports from
    core.scoring and adds SCORING_ENGINE_VERSION / VERSION / ScoringEngine)

Pytest collection
-----------------
Filename is `test_scoring_engine_contract.py` -- matches pytest's default
`test_*.py` glob, so `pytest` / `pytest core/` picks it up with no extra
config. Also runnable standalone: `python test_scoring_engine_contract.py`.

Why v2.3.0 (bug fixes vs uploaded v2.2.0)
-----------------------------------------
- FIX CRITICAL: `test_forecast_parameters_dataclass` had ALL 14 checked
    fields wrong (min_roi, max_roi, min_pe, max_pe, min_pb, max_pb,
    min_confidence, max_confidence, min_volatility, max_volatility,
    min_dividend_yield, max_dividend_yield, min_price_change_pct,
    max_price_change_pct). Real ForecastParameters fields (8 total) are:
    min_roi_1m, max_roi_1m, min_roi_3m, max_roi_3m, min_roi_12m, max_roi_12m,
    ratio_1m_of_12m, ratio_3m_of_12m. Test would fail on every assertion
    against the canonical module.
- FIX CRITICAL: `test_asset_scores_dataclass_contract` asserted recommendation
    vocab `{"BUY", "HOLD", "SELL", "STRONG_BUY", "STRONG_SELL"}`. The canonical
    vocab is `{STRONG_BUY, BUY, HOLD, REDUCE, SELL}` -- STRONG_SELL does NOT
    exist; the bearish-stronger-than-SELL bucket is REDUCE. Default is "HOLD"
    so current test happened to pass, but any "REDUCE" output would fail it.
- FIX CRITICAL: `test_scoring_engine_class_contract` asserted
    `hasattr(engine, "version") or hasattr(engine, "scoring_version")`.
    ScoringEngine.__init__ only sets `settings`, `weights`, `forecast_parameters`
    -- neither version attribute exists. Test WOULD fail. v2.3.0 checks the
    actual __init__-set attributes and verifies module-level SCORING_ENGINE_VERSION.
- FIX MEDIUM: Header docstring claimed file was `core/scoring_engine_contract_test.py`
    (with `_test` suffix) and warned pytest wouldn't collect it. Actual
    filename matches pytest's default. Stale instruction removed.
- FIX LOW: Removed unused `_get_env_int` helper.

Enhancements in v2.3.0
----------------------
- ADD: test_scoring_weights_alias_identity (ScoringWeights IS ScoreWeights)
- ADD: test_scoring_weights_main_sum_is_one (project canonical: sum=1.0)
- ADD: test_default_weights_and_forecasts_exported (DEFAULT_WEIGHTS,
    DEFAULT_FORECASTS module-level constants)
- ADD: test_bridge_exports_score_row_and_quote
- ADD: test_bridge_exports_ranking_functions
- ADD: test_forecast_parameters_roi_bounds_ordered (|1m| < |3m| < |12m|)
- ADD: test_recommendation_vocab_matches_canonical
- ADD: test_scoring_engine_version_matches_core_scoring
- ADD: test_scoring_engine_instance_attributes
- ADD: test_scoring_engine_has_all_method_delegates

Preserved from v2.2.0
---------------------
- stdlib unittest (no pytest dependency, no network, no prints)
- `_is_number` / `_to_float` / `_as_dict` helpers for pydantic/dataclass/obj
    extraction
- setUpClass skips if core.scoring_engine cannot be imported
- Env-driven performance budget (TFB_TEST_SCORE_BUDGET_MS)
- Env-driven ROI bound overrides (TFB_TEST_MIN_ROI_12M_FRACTION, etc.)
- Direct execution entrypoint `python test_scoring_engine_contract.py`
"""

from __future__ import annotations

import math
import os
import sys
import time
import unittest
from typing import Any


# ---------------------------------------------------------------------------
# Hygiene-safe output helper (no print())
# ---------------------------------------------------------------------------
def _out(msg: str) -> None:
    try:
        sys.stdout.write(
            str(msg) + ("\n" if not str(msg).endswith("\n") else "")
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Import under test (fail fast but with clear message)
# ---------------------------------------------------------------------------
try:
    from core import scoring_engine as se  # type: ignore
except Exception as e:
    se = None  # type: ignore
    _IMPORT_ERR: Any = e
else:
    _IMPORT_ERR = None

# Also try importing core.scoring directly for cross-module version check.
try:
    from core import scoring as _core_scoring  # type: ignore
except Exception:
    _core_scoring = None  # type: ignore


# ---------------------------------------------------------------------------
# Canonical truths (single source for test assertions)
# ---------------------------------------------------------------------------
CANONICAL_RECOMMENDATIONS: frozenset[str] = frozenset({
    "STRONG_BUY", "BUY", "HOLD", "REDUCE", "SELL",
})

CANONICAL_SCOREWEIGHTS_FIELDS: tuple[str, ...] = (
    "w_valuation", "w_momentum", "w_quality", "w_growth", "w_opportunity",
    "risk_penalty_strength", "confidence_penalty_strength",
)
# Only the five "main" weights sum to 1.0; penalty strengths are separate.
CANONICAL_MAIN_WEIGHT_FIELDS: tuple[str, ...] = (
    "w_valuation", "w_momentum", "w_quality", "w_growth", "w_opportunity",
)

CANONICAL_FORECASTPARAMS_FIELDS: tuple[str, ...] = (
    "min_roi_1m", "max_roi_1m",
    "min_roi_3m", "max_roi_3m",
    "min_roi_12m", "max_roi_12m",
    "ratio_1m_of_12m", "ratio_3m_of_12m",
)

CANONICAL_ASSETSCORES_REQUIRED: tuple[str, ...] = (
    "valuation_score", "momentum_score", "quality_score", "growth_score",
    "value_score", "opportunity_score",
    "confidence_score", "forecast_confidence", "confidence_bucket",
    "risk_score", "risk_bucket",
    "overall_score", "overall_score_raw", "overall_penalty_factor",
    "recommendation", "recommendation_reason",
    "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
    "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "expected_return_1m", "expected_return_3m", "expected_return_12m",
    "expected_price_1m", "expected_price_3m", "expected_price_12m",
    "scoring_updated_utc", "scoring_updated_riyadh",
    "scoring_errors",
)

CANONICAL_BRIDGE_EXPORTS: tuple[str, ...] = (
    "compute_scores", "enrich_with_scores",
    "score_row", "score_quote",
    "rank_rows_by_overall", "assign_rank_overall", "score_and_rank_rows",
    "AssetScores", "ScoringEngine", "ScoringWeights",
    "ForecastParameters", "ScoreWeights",
    "SCORING_ENGINE_VERSION", "VERSION",
)


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------
def _is_number(x: Any) -> bool:
    try:
        if isinstance(x, bool):
            return False
        if isinstance(x, (int, float)):
            return x == x and abs(float(x)) != float("inf")
        # tolerate numeric strings
        s = str(x).strip().replace(",", "")
        if not s:
            return False
        f = float(s)
        return f == f and abs(f) != float("inf")
    except Exception:
        return False


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if isinstance(x, bool):
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace(",", "")
        return float(s) if s else default
    except Exception:
        return default


def _as_dict(obj: Any) -> dict[str, Any]:
    """
    Robust object -> dict conversion:
    - dict
    - pydantic v2 (model_dump)
    - pydantic v1 (dict)
    - dataclasses (asdict)
    - AssetScores (project dataclass -- via .to_dict())
    - generic object (__dict__)
    """
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj

    # Try model_dump (pydantic v2)
    dump_fn = getattr(obj, "model_dump", None)
    if callable(dump_fn):
        try:
            d = dump_fn()
            if isinstance(d, dict):
                return d
        except Exception:
            pass

    # Try dict (pydantic v1)
    dict_fn = getattr(obj, "dict", None)
    if callable(dict_fn):
        try:
            d = dict_fn()
            if isinstance(d, dict):
                return d
        except Exception:
            pass

    # Try dataclass asdict
    if hasattr(obj, "__dataclass_fields__"):
        try:
            from dataclasses import asdict

            d = asdict(obj)
            if isinstance(d, dict):
                return d
        except Exception:
            pass

    # Project convention: AssetScores.to_dict()
    to_dict_fn = getattr(obj, "to_dict", None)
    if callable(to_dict_fn):
        try:
            d = to_dict_fn()
            if isinstance(d, dict):
                return d
        except Exception:
            pass

    # Fallback to __dict__
    if hasattr(obj, "__dict__"):
        try:
            d = dict(obj.__dict__)
            d = {k: v for k, v in d.items() if not k.startswith("_")}
            if isinstance(d, dict):
                return d
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


# ---------------------------------------------------------------------------
# Main test class
# ---------------------------------------------------------------------------
class TestScoringEngineContract(unittest.TestCase):
    """
    Contract tests cover:
    - Import & stable public surface
    - compute_scores() no-crash + essential fields
    - negative ROI clamp correctness (fraction-based, not percent points)
    - enrich_with_scores() output shape + in-place update
    - determinism
    - soft performance budget
    - AssetScores / ScoreWeights / ForecastParameters dataclass contracts
    - ScoringEngine class contract (actual attributes, not hypothetical)
    - Bridge module cross-checks (SCORING_ENGINE_VERSION == core.scoring.__version__)
    """

    @classmethod
    def setUpClass(cls) -> None:
        if se is None:
            raise unittest.SkipTest(
                f"core.scoring_engine could not be imported: {_IMPORT_ERR!r}"
            )

    # -------------------- Public surface --------------------

    def test_public_surface_exists(self) -> None:
        """All expected exports are present on the bridge module."""
        for name in CANONICAL_BRIDGE_EXPORTS:
            self.assertTrue(
                hasattr(se, name), msg=f"Bridge missing export: {name}"
            )

        # The two core callables must be callable.
        self.assertTrue(callable(se.compute_scores))
        self.assertTrue(callable(se.enrich_with_scores))

        # __all__ should include the key public names.
        self.assertTrue(hasattr(se, "__all__"))
        exported = set(getattr(se, "__all__", []) or [])
        for required in (
            "compute_scores", "enrich_with_scores",
            "AssetScores", "ScoringEngine",
            "ScoringWeights", "ForecastParameters", "ScoreWeights",
            "SCORING_ENGINE_VERSION", "VERSION",
        ):
            self.assertIn(required, exported, msg=f"__all__ missing: {required}")

    def test_bridge_exports_score_row_and_quote(self) -> None:
        """v2.3.0: bridge re-exports row/quote scoring helpers."""
        for name in ("score_row", "score_quote"):
            self.assertTrue(hasattr(se, name), msg=f"Missing: {name}")
            self.assertTrue(callable(getattr(se, name)), msg=f"Not callable: {name}")

    def test_bridge_exports_ranking_functions(self) -> None:
        """v2.3.0: bridge re-exports ranking helpers."""
        for name in (
            "rank_rows_by_overall", "assign_rank_overall", "score_and_rank_rows",
        ):
            self.assertTrue(hasattr(se, name), msg=f"Missing: {name}")
            self.assertTrue(callable(getattr(se, name)), msg=f"Not callable: {name}")

    def test_scoring_engine_version_matches_core_scoring(self) -> None:
        """v2.3.0: SCORING_ENGINE_VERSION must match core.scoring.__version__."""
        if _core_scoring is None:
            self.skipTest("core.scoring could not be imported directly")
        self.assertEqual(
            getattr(se, "SCORING_ENGINE_VERSION"),
            getattr(_core_scoring, "__version__"),
            msg="Bridge SCORING_ENGINE_VERSION diverged from core.scoring.__version__",
        )
        # VERSION alias must match SCORING_ENGINE_VERSION too.
        self.assertEqual(
            getattr(se, "VERSION"),
            getattr(se, "SCORING_ENGINE_VERSION"),
            msg="VERSION alias diverged from SCORING_ENGINE_VERSION",
        )

    def test_default_weights_and_forecasts_exported(self) -> None:
        """v2.3.0: DEFAULT_WEIGHTS and DEFAULT_FORECASTS module constants."""
        if _core_scoring is None:
            self.skipTest("core.scoring not importable")
        self.assertTrue(hasattr(_core_scoring, "DEFAULT_WEIGHTS"))
        self.assertTrue(hasattr(_core_scoring, "DEFAULT_FORECASTS"))
        # DEFAULT_WEIGHTS should be a ScoreWeights instance (type identity
        # via isinstance of the bridge-exported class).
        self.assertIsInstance(_core_scoring.DEFAULT_WEIGHTS, se.ScoreWeights)
        self.assertIsInstance(_core_scoring.DEFAULT_FORECASTS, se.ForecastParameters)

    # -------------------- compute_scores() --------------------

    def test_compute_scores_minimal_input_no_crash(self) -> None:
        """Minimal input -> complete scores dict with required fields."""
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

        for k in (
            "value_score", "quality_score", "momentum_score",
            "risk_score", "opportunity_score", "overall_score",
        ):
            self.assertIn(k, d, msg=f"Missing key: {k}")
            self.assertTrue(
                _is_number(d[k]), msg=f"Non-numeric score for {k}: {d.get(k)!r}",
            )
            v = _to_float(d[k])
            self.assertGreaterEqual(v, 0.0, msg=f"Score {k} negative: {v}")
            self.assertLessEqual(v, 100.0, msg=f"Score {k} > 100: {v}")

        self.assertIn("recommendation", d)
        self.assertIn(
            d["recommendation"], CANONICAL_RECOMMENDATIONS,
            msg=f"Non-canonical recommendation: {d['recommendation']!r}",
        )
        self.assertIn("scoring_updated_utc", d)
        self.assertIn("scoring_updated_riyadh", d)
        self.assertIn("scoring_errors", d)
        self.assertIsInstance(d["scoring_errors"], list)

    def test_forecast_fields_are_fractions_not_percent_points(self) -> None:
        """
        Critical: expected_roi_* MUST be fractions (0.085 = 8.5%), not
        percent points (8.5 = 850%).
        """
        payload = {
            "symbol": "TEST.ROI",
            "price": 100.0,
            "fair_value": 110.0,
            "forecast_confidence": 0.75,
            "data_quality": "EXCELLENT",
        }
        res = se.compute_scores(payload)
        d = _as_dict(res)

        for field_name in ("expected_roi_1m", "expected_roi_3m", "expected_roi_12m"):
            self.assertIn(field_name, d, msg=f"Missing forecast field: {field_name}")
            val = d[field_name]
            self.assertTrue(
                _is_number(val), msg=f"{field_name} not numeric: {val!r}",
            )
            f = _to_float(val)
            self.assertGreaterEqual(f, -3.0, msg=f"{field_name} below -3.0: {f}")
            self.assertLessEqual(f, 3.0, msg=f"{field_name} above 3.0: {f}")
            if f > 0:
                self.assertLessEqual(
                    f, 1.0,
                    msg=f"{field_name} positive ROI > 100% as fraction: {f}",
                )

    def test_negative_roi_clamp_is_supported(self) -> None:
        """
        Critical: Forecast ROI allows negatives clamped within bounds.
        price=100, fair_value=50 => base_return_12m = -50%.
        """
        payload = {
            "symbol": "TEST.NEG",
            "price": 100.0,
            "fair_value": 50.0,
            "data_quality": "EXCELLENT",
            "forecast_confidence": 0.90,
            "volatility_30d": 0.20,
            "beta": 1.0,
        }
        res = se.compute_scores(payload)
        d = _as_dict(res)

        self.assertIn("expected_roi_12m", d)
        self.assertTrue(_is_number(d["expected_roi_12m"]))

        r12 = _to_float(d["expected_roi_12m"])
        self.assertLessEqual(
            r12, 0.0,
            msg=f"expected_roi_12m should be negative, got {r12}",
        )

        min_roi_12m = _get_env_float("TFB_TEST_MIN_ROI_12M_FRACTION", -0.65)
        max_roi_12m = _get_env_float("TFB_TEST_MAX_ROI_12M_FRACTION", 0.65)
        self.assertGreaterEqual(r12, min_roi_12m)
        self.assertLessEqual(r12, max_roi_12m)

        self.assertIn("forecast_price_12m", d)
        self.assertTrue(_is_number(d["forecast_price_12m"]))
        fp12 = _to_float(d["forecast_price_12m"])
        self.assertLess(
            fp12, 100.0,
            msg=f"forecast_price_12m should be below price when ROI negative. fp12={fp12}",
        )

    def test_missing_price_does_not_crash(self) -> None:
        """Engine should handle missing price gracefully."""
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
        self.assertIn("scoring_errors", d)
        self.assertIsInstance(d["scoring_errors"], list)

    # -------------------- enrich_with_scores() --------------------

    def test_enrich_with_scores_returns_expected_fields(self) -> None:
        """enrich_with_scores returns a dict with all required fields."""
        payload = {
            "symbol": "AAPL", "price": 100.0,
            "previous_close": 98.0, "data_quality": "GOOD",
        }
        out = se.enrich_with_scores(payload, in_place=False)
        self.assertIsInstance(out, dict)

        for k in ("overall_score", "risk_score", "recommendation"):
            self.assertIn(k, out)
        for k in ("expected_roi_12m", "expected_roi_3m", "expected_roi_1m"):
            self.assertIn(k, out)
        for k in ("forecast_price_12m", "confidence_score"):
            self.assertIn(k, out)

    def test_enrich_with_scores_in_place_updates_dict(self) -> None:
        """in_place=True modifies and returns the original dict."""
        payload = {
            "symbol": "AAPL", "price": 100.0,
            "previous_close": 99.0, "data_quality": "GOOD",
        }
        out = se.enrich_with_scores(payload, in_place=True)
        self.assertIs(out, payload)
        self.assertIn("overall_score", payload)
        self.assertIn("recommendation", payload)

    # -------------------- Determinism & performance --------------------

    def test_recommendation_is_deterministic_for_same_input(self) -> None:
        """Same input -> same recommendation."""
        payload = {
            "symbol": "DET", "price": 100.0, "fair_value": 120.0,
            "data_quality": "EXCELLENT",
        }
        r1 = _as_dict(se.compute_scores(payload)).get("recommendation")
        r2 = _as_dict(se.compute_scores(payload)).get("recommendation")
        self.assertEqual(r1, r2)

    def test_basic_performance_budget(self) -> None:
        """compute_scores completes within reasonable time (default 60ms)."""
        budget_ms = _get_env_float("TFB_TEST_SCORE_BUDGET_MS", 60.0)
        payload = {
            "symbol": "PERF", "price": 100.0, "previous_close": 99.0,
            "market_cap": 10_000_000_000, "volume": 2_000_000,
            "pe_ttm": 15.0, "roe": 0.12, "data_quality": "GOOD",
        }
        t0 = time.perf_counter()
        _ = se.compute_scores(payload)
        dt_ms = (time.perf_counter() - t0) * 1000.0
        self.assertLessEqual(
            dt_ms, budget_ms,
            msg=f"compute_scores took {dt_ms:.2f}ms > budget {budget_ms:.2f}ms",
        )

    # -------------------- AssetScores dataclass --------------------

    def test_asset_scores_dataclass_contract(self) -> None:
        """AssetScores dataclass has all canonical fields."""
        self.assertTrue(hasattr(se, "AssetScores"))
        AssetScores = se.AssetScores
        scores = AssetScores()

        for field_name in CANONICAL_ASSETSCORES_REQUIRED:
            self.assertTrue(
                hasattr(scores, field_name),
                msg=f"AssetScores missing field: {field_name}",
            )

        # v2.3.0 FIX: recommendation vocab uses canonical {STRONG_BUY, BUY,
        # HOLD, REDUCE, SELL} -- v2.2.0 incorrectly listed STRONG_SELL.
        self.assertIn(
            scores.recommendation, CANONICAL_RECOMMENDATIONS,
            msg=(
                f"Default recommendation {scores.recommendation!r} not in "
                f"canonical set {sorted(CANONICAL_RECOMMENDATIONS)}"
            ),
        )
        # Concrete default should be "HOLD".
        self.assertEqual(scores.recommendation, "HOLD")
        self.assertIsInstance(scores.scoring_errors, list)

    def test_recommendation_vocab_matches_canonical(self) -> None:
        """v2.3.0: canonical vocab contract (set identity)."""
        expected = frozenset({"STRONG_BUY", "BUY", "HOLD", "REDUCE", "SELL"})
        self.assertEqual(CANONICAL_RECOMMENDATIONS, expected)
        # No STRONG_SELL in canonical vocab.
        self.assertNotIn("STRONG_SELL", CANONICAL_RECOMMENDATIONS)

    # -------------------- ScoringEngine class --------------------

    def test_scoring_engine_class_instantiable_and_callable(self) -> None:
        """
        v2.3.0 FIX: ScoringEngine has compute_scores + enrich_with_scores
        methods. (v2.2.0 asserted nonexistent `version`/`scoring_version`
        attributes -- removed.)
        """
        self.assertTrue(hasattr(se, "ScoringEngine"))
        engine = se.ScoringEngine()
        self.assertIsNotNone(engine)
        self.assertTrue(hasattr(engine, "compute_scores"))
        self.assertTrue(callable(engine.compute_scores))

        payload = {"symbol": "TEST", "price": 100.0, "data_quality": "GOOD"}
        res = engine.compute_scores(payload)
        d = _as_dict(res)
        self.assertIn("overall_score", d)

    def test_scoring_engine_instance_attributes(self) -> None:
        """v2.3.0: ScoringEngine.__init__ sets settings/weights/forecast_parameters."""
        engine = se.ScoringEngine()
        # Defaults are None when kwargs not provided.
        self.assertTrue(hasattr(engine, "settings"))
        self.assertTrue(hasattr(engine, "weights"))
        self.assertTrue(hasattr(engine, "forecast_parameters"))
        self.assertIsNone(engine.settings)
        self.assertIsNone(engine.weights)
        self.assertIsNone(engine.forecast_parameters)

        # Pass custom weights -- must be stored as given.
        custom_weights = se.ScoreWeights()
        engine2 = se.ScoringEngine(weights=custom_weights)
        self.assertIs(engine2.weights, custom_weights)

    def test_scoring_engine_has_all_method_delegates(self) -> None:
        """v2.3.0: bridge's ScoringEngine delegates all 7 public methods."""
        engine = se.ScoringEngine()
        for method_name in (
            "compute_scores", "enrich_with_scores",
            "score_row", "score_quote",
            "rank_rows_by_overall", "assign_rank_overall",
            "score_and_rank_rows",
        ):
            self.assertTrue(
                hasattr(engine, method_name),
                msg=f"ScoringEngine missing method: {method_name}",
            )
            self.assertTrue(
                callable(getattr(engine, method_name)),
                msg=f"ScoringEngine.{method_name} is not callable",
            )

    # -------------------- ScoringWeights / ScoreWeights --------------------

    def test_scoring_weights_dataclass(self) -> None:
        """ScoringWeights has all canonical fields (7 total)."""
        self.assertTrue(hasattr(se, "ScoringWeights"))
        weights = se.ScoringWeights()
        for field_name in CANONICAL_SCOREWEIGHTS_FIELDS:
            self.assertTrue(
                hasattr(weights, field_name),
                msg=f"ScoringWeights missing field: {field_name}",
            )
            self.assertIsInstance(getattr(weights, field_name), (int, float))

    def test_scoring_weights_alias_identity(self) -> None:
        """v2.3.0: ScoringWeights IS ScoreWeights (module-level alias)."""
        self.assertIs(
            se.ScoringWeights, se.ScoreWeights,
            msg="ScoringWeights should be an alias for ScoreWeights",
        )

    def test_scoring_weights_main_sum_is_one(self) -> None:
        """v2.3.0: main 5 weights (excluding penalty strengths) sum to 1.0."""
        weights = se.ScoreWeights()
        total = sum(float(getattr(weights, f)) for f in CANONICAL_MAIN_WEIGHT_FIELDS)
        self.assertTrue(
            math.isclose(total, 1.0, abs_tol=1e-9),
            msg=(
                f"Main weights sum must equal 1.0 (+/-1e-9). Got {total}. "
                + "Fields: "
                + ", ".join(
                    f"{f}={float(getattr(weights, f))}"
                    for f in CANONICAL_MAIN_WEIGHT_FIELDS
                )
            ),
        )

    # -------------------- ForecastParameters --------------------

    def test_forecast_parameters_dataclass(self) -> None:
        """
        v2.3.0 FIX: ForecastParameters has 8 canonical fields (min_roi_1m,
        max_roi_1m, min_roi_3m, max_roi_3m, min_roi_12m, max_roi_12m,
        ratio_1m_of_12m, ratio_3m_of_12m). v2.2.0 checked 14 WRONG fields.
        """
        self.assertTrue(hasattr(se, "ForecastParameters"))
        params = se.ForecastParameters()
        for field_name in CANONICAL_FORECASTPARAMS_FIELDS:
            self.assertTrue(
                hasattr(params, field_name),
                msg=f"ForecastParameters missing field: {field_name}",
            )
            self.assertIsInstance(getattr(params, field_name), (int, float))

    def test_forecast_parameters_roi_bounds_ordered(self) -> None:
        """
        v2.3.0: |min_roi_1m| <= |min_roi_3m| <= |min_roi_12m|
                |max_roi_1m| <= |max_roi_3m| <= |max_roi_12m|
        Physically: longer horizon permits larger swings.
        """
        params = se.ForecastParameters()
        self.assertLessEqual(
            abs(params.min_roi_1m), abs(params.min_roi_3m),
            msg=f"|min_roi_1m| {abs(params.min_roi_1m)} > |min_roi_3m| {abs(params.min_roi_3m)}",
        )
        self.assertLessEqual(
            abs(params.min_roi_3m), abs(params.min_roi_12m),
            msg=f"|min_roi_3m| {abs(params.min_roi_3m)} > |min_roi_12m| {abs(params.min_roi_12m)}",
        )
        self.assertLessEqual(
            abs(params.max_roi_1m), abs(params.max_roi_3m),
            msg=f"|max_roi_1m| {abs(params.max_roi_1m)} > |max_roi_3m| {abs(params.max_roi_3m)}",
        )
        self.assertLessEqual(
            abs(params.max_roi_3m), abs(params.max_roi_12m),
            msg=f"|max_roi_3m| {abs(params.max_roi_3m)} > |max_roi_12m| {abs(params.max_roi_12m)}",
        )
        # min should be negative and max positive (conventional sign discipline).
        self.assertLess(params.min_roi_12m, 0)
        self.assertGreater(params.max_roi_12m, 0)


# ---------------------------------------------------------------------------
# Runner for direct execution
# ---------------------------------------------------------------------------
def main() -> int:
    try:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(
            TestScoringEngineContract,
        )
        verbose_raw = os.getenv("TFB_TEST_VERBOSE", "0").strip()
        try:
            verbosity = 2 if int(float(verbose_raw or "0")) > 0 else 1
        except Exception:
            verbosity = 1
        runner = unittest.TextTestRunner(verbosity=verbosity, stream=sys.stdout)
        result = runner.run(suite)
        return 0 if result.wasSuccessful() else 2
    except Exception as e:
        _out(f"[contract_test] fatal_error: {e!r}")
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
