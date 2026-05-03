#!/usr/bin/env python3
"""
test_scoring_engine_contract.py
================================================================================
SCORING ENGINE CONTRACT TESTS -- v3.0.0 (VIEW-AWARE / FAMILY-ALIGNED)
================================================================================
Emad Bahbah -- Tadawul Fast Bridge

Aligned with the view-aware recommendation family:
  - core.scoring                  v5.0.0  (authoritative implementation;
                                           emits 4 view fields + 5-tier rec
                                           via reco_normalize delegation)
  - core.scoring_engine           v3.4.0  (compatibility bridge; re-exports
                                           derive_*_view + is_view_aware +
                                           get_degradation_report)
  - core.reco_normalize           v7.0.0  (the 5-tier cascade — vetoes,
                                           Recommendation.from_views(),
                                           recommendation_from_views())
  - core.investment_advisor       v5.1.1
  - core.investment_advisor_engine v4.2.0
  - core.sheets.schema_registry   v2.5.0  (4 View columns canonical)
  - core.enriched_quote           v4.2.0  (fallback schema includes views)

Pytest collection
-----------------
Filename matches pytest's default `test_*.py` glob — picked up by
`pytest` / `pytest core/` with no extra config. Also runnable standalone:
`python test_scoring_engine_contract.py`.

================================================================================
Why v3.0.0 (changes from v2.3.0)
================================================================================

NEW CONTRACT TESTS for the view-aware family
--------------------------------------------
- ADD: test_asset_scores_has_view_fields — explicit contract for the 4
    view fields (fundamental_view / technical_view / risk_view / value_view)
    on AssetScores. v2.3.0's CANONICAL_ASSETSCORES_REQUIRED list omitted
    them, so a regression that dropped them would have passed CI silently.
- ADD: test_asset_scores_view_defaults_are_safe — defaults must be None
    or empty string, NEVER a fabricated token. Phantom rows must not get
    fake views.
- ADD: test_view_derivation_functions_exported — derive_fundamental_view,
    derive_technical_view, derive_risk_view, derive_value_view all
    exposed via the bridge (added in scoring_engine v3.4.0).
- ADD: test_view_aware_helpers_exposed — is_view_aware() returns True
    when reco_normalize v7+ is loaded; get_degradation_report() includes
    `view_aware_recommendation_enabled: True`.
- ADD: test_view_tokens_canonical_when_computed — when compute_scores()
    produces view fields, they use canonical tokens
    (BULLISH/NEUTRAL/BEARISH for fundamental+technical, LOW/MODERATE/HIGH
    for risk, CHEAP/FAIR/EXPENSIVE for value).
- ADD: test_recommendation_from_views_direct — the
    `recommendation_from_views` helper from core.reco_normalize is
    callable directly and produces canonical 5-tier output.
- ADD: test_double_bearish_forces_sell — view-aware veto P2: when
    fundamental=BEARISH AND technical=BEARISH, the result is SELL
    regardless of overall score (priority cascade fires).
- ADD: test_expensive_valuation_blocks_strong_buy — view-aware veto
    on the BUY side: bullish-but-EXPENSIVE inputs cannot produce
    STRONG_BUY (the SBK.JSE-style audit case that motivated v7.0.0).
- ADD: test_insufficient_data_returns_hold — when no view tokens and
    no score are provided, the cascade returns HOLD with an
    "Insufficient data" reason marker (priority P1).

UPDATES to existing tests for v5.0.0 stricter gating
----------------------------------------------------
- CHANGE: score-range assertions now tolerate `None` returns. Scoring
    v5.0.0 added a stricter overall-score gate (returns None if fewer
    than 2 components OR total weight < 0.40) and a phantom-row gate
    on quality_score. Existing tests treated all-numeric as the
    contract; that's no longer accurate. None is now an explicitly
    accepted "insufficient data" signal. Numeric values still get the
    full [0, 100] range check.
- CHANGE: test_compute_scores_minimal_input_no_crash now requires the
    minimal payload to be enriched enough that v5.0.0's gate doesn't
    fire (added `pe_ttm`, `roe`). The "no crash" contract is preserved.
- CHANGE: forecast field tests no longer require numeric output for
    every field — they accept None for forecasts that v5.0.0 declined
    to compute, but enforce numeric range when a value IS present.

PRESERVED from v2.3.0
---------------------
- CANONICAL_RECOMMENDATIONS frozenset matches v7.0.0 exactly:
    {STRONG_BUY, BUY, HOLD, REDUCE, SELL}. v3.0.0 keeps
    test_recommendation_vocab_matches_canonical and
    test_asset_scores_dataclass_contract.
- All bridge-export tests (score_row, score_quote, ranking helpers,
    SCORING_ENGINE_VERSION cross-check, DEFAULT_WEIGHTS / DEFAULT_FORECASTS).
- All ForecastParameters tests (8 fields, ROI bounds ordered).
- All ScoringWeights tests (7 fields, alias identity, main 5 sum to 1.0).
- All ScoringEngine class tests (instance attributes, method delegates).
- Determinism + soft performance budget.
- Env-driven overrides (TFB_TEST_SCORE_BUDGET_MS,
    TFB_TEST_MIN_ROI_12M_FRACTION, TFB_TEST_MAX_ROI_12M_FRACTION).
- Stdlib unittest only (no pytest dependency).
- Direct execution entrypoint `python test_scoring_engine_contract.py`.

v2.3.0 bug-fix history (also preserved)
---------------------------------------
- v2.2.0 had ALL 14 ForecastParameters field names wrong (canonical is
    8: min/max_roi_{1m,3m,12m} + ratio_{1m,3m}_of_12m).
- v2.2.0 had recommendation vocab with STRONG_SELL (canonical uses REDUCE).
- v2.2.0 asserted `version`/`scoring_version` attrs on ScoringEngine
    (those don't exist; __init__ sets settings/weights/forecast_parameters).
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

# v3.0.0: also try importing reco_normalize for direct view-aware tests.
# This module is the source of truth for the 5-tier cascade and is what
# scoring v5.0.0's compute_recommendation delegates to.
try:
    from core import reco_normalize as _reco_normalize  # type: ignore
except Exception:
    _reco_normalize = None  # type: ignore


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

# v3.0.0: the four View columns added to AssetScores in scoring v5.0.0.
# Recommendation engine (reco_normalize v7.0.0) consumes these as inputs to
# the 5-tier priority cascade. Schema registry v2.5.0 has matching columns.
CANONICAL_VIEW_FIELDS: tuple[str, ...] = (
    "fundamental_view",
    "technical_view",
    "risk_view",
    "value_view",
)

# v3.0.0: canonical token vocabularies for each View column.
# Mirrors core.reco_normalize VIEW_FUND_*, VIEW_TECH_*, VIEW_RISK_*,
# VIEW_VALUE_* constants.
CANONICAL_FUNDAMENTAL_VIEWS: frozenset[str] = frozenset({
    "BULLISH", "NEUTRAL", "BEARISH",
})
CANONICAL_TECHNICAL_VIEWS: frozenset[str] = frozenset({
    "BULLISH", "NEUTRAL", "BEARISH",
})
CANONICAL_RISK_VIEWS: frozenset[str] = frozenset({
    "LOW", "MODERATE", "HIGH",
})
CANONICAL_VALUE_VIEWS: frozenset[str] = frozenset({
    "CHEAP", "FAIR", "EXPENSIVE",
})

# Per-field allowed token map.
CANONICAL_VIEW_VOCAB: dict[str, frozenset[str]] = {
    "fundamental_view": CANONICAL_FUNDAMENTAL_VIEWS,
    "technical_view": CANONICAL_TECHNICAL_VIEWS,
    "risk_view": CANONICAL_RISK_VIEWS,
    "value_view": CANONICAL_VALUE_VIEWS,
}

# v3.0.0: AssetScores required-field list now includes the 4 view fields.
# (v2.3.0 omitted them; a regression that dropped views would have passed CI.)
CANONICAL_ASSETSCORES_REQUIRED: tuple[str, ...] = (
    # Numeric scores
    "valuation_score", "momentum_score", "quality_score", "growth_score",
    "value_score", "opportunity_score",
    "confidence_score", "forecast_confidence", "confidence_bucket",
    "risk_score", "risk_bucket",
    "overall_score", "overall_score_raw", "overall_penalty_factor",
    # Recommendation
    "recommendation", "recommendation_reason",
    # Forecasts (and aliases)
    "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
    "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "expected_return_1m", "expected_return_3m", "expected_return_12m",
    "expected_price_1m", "expected_price_3m", "expected_price_12m",
    # Provenance
    "scoring_updated_utc", "scoring_updated_riyadh",
    "scoring_errors",
) + CANONICAL_VIEW_FIELDS

# v3.0.0: bridge exports include the 4 derive_* helpers (added in
# scoring_engine v3.4.0) plus is_view_aware / get_degradation_report.
CANONICAL_BRIDGE_EXPORTS: tuple[str, ...] = (
    # Core scoring callables
    "compute_scores", "enrich_with_scores",
    "score_row", "score_quote",
    "rank_rows_by_overall", "assign_rank_overall", "score_and_rank_rows",
    # Classes / dataclasses
    "AssetScores", "ScoringEngine", "ScoringWeights",
    "ForecastParameters", "ScoreWeights",
    # Versions
    "SCORING_ENGINE_VERSION", "VERSION",
    # v3.0.0 — view-aware exports
    "derive_fundamental_view",
    "derive_technical_view",
    "derive_risk_view",
    "derive_value_view",
    "is_view_aware",
    "get_degradation_report",
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


def _check_score_range_or_none(
    test: unittest.TestCase,
    field_name: str,
    value: Any,
    *,
    lo: float = 0.0,
    hi: float = 100.0,
) -> None:
    """
    v3.0.0: tolerant range check.

    scoring v5.0.0 added stricter gating that legitimately returns None
    when there's insufficient data to compute a score (phantom-row gate
    on quality_score, ≥2-components gate on overall_score). None is now
    an explicitly accepted contract value; numeric values still get the
    full range check.
    """
    if value is None:
        return  # legitimate "insufficient data" response
    test.assertTrue(
        _is_number(value),
        msg=f"Field {field_name} not numeric and not None: {value!r}",
    )
    f = _to_float(value)
    test.assertGreaterEqual(f, lo, msg=f"Field {field_name} < {lo}: {f}")
    test.assertLessEqual(f, hi, msg=f"Field {field_name} > {hi}: {f}")


# ---------------------------------------------------------------------------
# Main test class
# ---------------------------------------------------------------------------
class TestScoringEngineContract(unittest.TestCase):
    """
    Contract tests cover:
    - Import & stable public surface (incl. view-aware exports)
    - compute_scores() no-crash + essential fields (numeric OR None)
    - View field contract (4 view columns on AssetScores; canonical tokens)
    - View-aware recommendation cascade (EXPENSIVE veto, double-bearish→SELL)
    - recommendation_from_views direct-call integration
    - negative ROI clamp correctness (fraction-based, not percent points)
    - enrich_with_scores() output shape + in-place update
    - Determinism + soft performance budget
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

    # -------------------- View-aware contract (v3.0.0) --------------------

    def test_view_derivation_functions_exported(self) -> None:
        """v3.0.0: derive_*_view helpers from scoring_engine v3.4.0 bridge."""
        for fn_name in (
            "derive_fundamental_view",
            "derive_technical_view",
            "derive_risk_view",
            "derive_value_view",
        ):
            self.assertTrue(
                hasattr(se, fn_name),
                msg=(
                    f"Bridge missing view derivation function: {fn_name}. "
                    "Expected in scoring_engine v3.4.0+."
                ),
            )
            self.assertTrue(
                callable(getattr(se, fn_name)),
                msg=f"{fn_name} is not callable",
            )

    def test_view_aware_helpers_exposed(self) -> None:
        """v3.0.0: is_view_aware()/get_degradation_report() helpers."""
        self.assertTrue(hasattr(se, "is_view_aware"))
        self.assertTrue(callable(se.is_view_aware))

        # When reco_normalize v7.0.0 is loaded, view-aware should be True.
        if _reco_normalize is not None:
            try:
                self.assertTrue(
                    bool(se.is_view_aware()),
                    msg=(
                        "is_view_aware() returned False even though "
                        "core.reco_normalize is importable. Bridge should "
                        "detect v7.0.0+ and report True."
                    ),
                )
            except Exception as exc:
                self.fail(f"is_view_aware() raised: {exc!r}")

        self.assertTrue(hasattr(se, "get_degradation_report"))
        self.assertTrue(callable(se.get_degradation_report))
        try:
            report = se.get_degradation_report()
        except Exception as exc:
            self.fail(f"get_degradation_report() raised: {exc!r}")
        self.assertIsInstance(report, dict, msg="Degradation report must be a dict")
        # The report should at minimum tell callers whether view-aware mode
        # is active. Field name is fixed per scoring_engine v3.4.0 contract.
        self.assertIn(
            "view_aware_recommendation_enabled", report,
            msg=(
                "Degradation report missing 'view_aware_recommendation_enabled' "
                "key. scoring_engine v3.4.0 contract requires it."
            ),
        )

    def test_asset_scores_has_view_fields(self) -> None:
        """v3.0.0: 4 view fields are part of the AssetScores contract."""
        scores = se.AssetScores()
        for field_name in CANONICAL_VIEW_FIELDS:
            self.assertTrue(
                hasattr(scores, field_name),
                msg=(
                    f"AssetScores missing view field: {field_name}. "
                    "scoring v5.0.0 must populate this for the rec engine."
                ),
            )

    def test_asset_scores_view_defaults_are_safe(self) -> None:
        """
        v3.0.0: view fields default to None or empty string — never a
        fabricated token. Phantom rows must not get fake views.
        """
        scores = se.AssetScores()
        for field_name in CANONICAL_VIEW_FIELDS:
            val = getattr(scores, field_name)
            # Acceptable defaults: None, empty string. Anything else is a
            # fabricated default which would mislead the rec engine.
            self.assertTrue(
                val is None or val == "",
                msg=(
                    f"AssetScores.{field_name} default is fabricated: "
                    f"{val!r}. Should be None or empty string."
                ),
            )

    def test_view_tokens_canonical_when_computed(self) -> None:
        """
        v3.0.0: when compute_scores() produces a view field, the value
        must be a canonical token from the per-field vocabulary.
        """
        # Rich payload so the engine has enough signal to derive views.
        payload = {
            "symbol": "TEST.VIEWS",
            "price": 100.0,
            "previous_close": 99.0,
            "fair_value": 110.0,
            "pe_ttm": 18.0,
            "roe": 0.18,
            "revenue_growth_yoy": 0.12,
            "gross_margin": 0.40,
            "operating_margin": 0.25,
            "rsi_14": 55.0,
            "volatility_30d": 0.20,
            "beta": 1.0,
            "market_cap": 5_000_000_000,
            "volume": 5_000_000,
            "data_quality": "EXCELLENT",
            "forecast_confidence": 0.85,
        }
        d = _as_dict(se.compute_scores(payload))

        for field_name in CANONICAL_VIEW_FIELDS:
            if field_name not in d:
                # Field absent is acceptable in some compute paths; covered
                # by test_asset_scores_has_view_fields. Skip here.
                continue
            val = d[field_name]
            # None / empty string is acceptable (insufficient data path).
            if val is None or val == "":
                continue
            allowed = CANONICAL_VIEW_VOCAB[field_name]
            self.assertIn(
                str(val), allowed,
                msg=(
                    f"{field_name}={val!r} is not in the canonical vocabulary "
                    f"{sorted(allowed)}"
                ),
            )

    def test_recommendation_from_views_direct(self) -> None:
        """
        v3.0.0: recommendation_from_views (the helper the rec engine
        delegates to) is callable and produces canonical 5-tier output.
        """
        if _reco_normalize is None:
            self.skipTest("core.reco_normalize not importable")
        fn = getattr(_reco_normalize, "recommendation_from_views", None)
        self.assertTrue(
            callable(fn),
            msg=(
                "core.reco_normalize.recommendation_from_views() not callable. "
                "Required for view-aware recommendation cascade."
            ),
        )

        # Bullish + cheap + low risk + good score → STRONG_BUY (priority P4).
        reco, reason = fn(
            fundamental="BULLISH", technical="BULLISH",
            risk="LOW", value="CHEAP", score=82.0,
        )
        self.assertIn(
            reco, CANONICAL_RECOMMENDATIONS,
            msg=f"Non-canonical reco: {reco!r}",
        )
        self.assertEqual(
            reco, "STRONG_BUY",
            msg=(
                f"Bullish+cheap+low-risk+82 expected STRONG_BUY, got "
                f"{reco!r}. Reason: {reason!r}"
            ),
        )
        self.assertIsInstance(reason, str)
        self.assertTrue(reason.strip(), msg="Empty reason string")

    def test_double_bearish_forces_sell(self) -> None:
        """
        v3.0.0: priority cascade P2 — when fundamental=BEARISH AND
        technical=BEARISH, the result is SELL regardless of overall
        score. This guards against future regressions that might let
        a high overall_score override the dual-bearish signal.
        """
        if _reco_normalize is None:
            self.skipTest("core.reco_normalize not importable")
        fn = getattr(_reco_normalize, "recommendation_from_views", None)
        if not callable(fn):
            self.skipTest("recommendation_from_views unavailable")

        # Even with overall_score=80 (would normally support BUY/STRONG_BUY),
        # double-bearish must force SELL.
        reco, _reason = fn(
            fundamental="BEARISH", technical="BEARISH",
            risk="MODERATE", value="FAIR", score=80.0,
        )
        self.assertEqual(
            reco, "SELL",
            msg=(
                f"Double-bearish with score=80 should force SELL. Got {reco!r}. "
                "Priority cascade P2 may be broken."
            ),
        )

    def test_expensive_valuation_blocks_strong_buy(self) -> None:
        """
        v3.0.0: priority cascade veto — bullish-but-EXPENSIVE inputs
        cannot produce STRONG_BUY. This is the SBK.JSE-style audit
        case that motivated reco_normalize v7.0.0.
        """
        if _reco_normalize is None:
            self.skipTest("core.reco_normalize not importable")
        fn = getattr(_reco_normalize, "recommendation_from_views", None)
        if not callable(fn):
            self.skipTest("recommendation_from_views unavailable")

        # Strong fundamentals + technicals + a high score, BUT expensive
        # valuation. Must not produce STRONG_BUY (the EXPENSIVE veto).
        reco, _reason = fn(
            fundamental="BULLISH", technical="BULLISH",
            risk="MODERATE", value="EXPENSIVE", score=83.0,
        )
        self.assertNotEqual(
            reco, "STRONG_BUY",
            msg=(
                f"EXPENSIVE valuation should veto STRONG_BUY. Got {reco!r} "
                "(SBK.JSE-style regression)."
            ),
        )
        # Acceptable outputs are HOLD or REDUCE — never STRONG_BUY/BUY when
        # value is EXPENSIVE.
        self.assertIn(
            reco, {"HOLD", "REDUCE"},
            msg=f"Bullish+expensive should be HOLD or REDUCE. Got {reco!r}.",
        )

    def test_insufficient_data_returns_hold(self) -> None:
        """
        v3.0.0: priority cascade P1 — when no view tokens and no score
        are provided, the result is HOLD with a reason indicating
        insufficient data.
        """
        if _reco_normalize is None:
            self.skipTest("core.reco_normalize not importable")
        fn = getattr(_reco_normalize, "recommendation_from_views", None)
        if not callable(fn):
            self.skipTest("recommendation_from_views unavailable")

        reco, reason = fn(
            fundamental=None, technical=None,
            risk=None, value=None, score=None,
        )
        self.assertEqual(
            reco, "HOLD",
            msg=f"Empty inputs should return HOLD. Got {reco!r}.",
        )
        self.assertIn(
            "insufficient", str(reason).lower(),
            msg=(
                f"Empty-inputs reason should mention 'insufficient'. Got "
                f"{reason!r}."
            ),
        )

    # -------------------- compute_scores() --------------------

    def test_compute_scores_minimal_input_no_crash(self) -> None:
        """
        Minimal-but-sufficient input -> complete scores dict.

        v3.0.0: payload extended with `pe_ttm` and `roe` so v5.0.0's
        stricter overall-score gate (need ≥2 components or weight ≥0.40)
        is satisfied. The "no crash" contract is preserved.
        """
        payload = {
            "symbol": "AAPL",
            "price": 100.0,
            "previous_close": 99.0,
            "volume": 10_000_000,
            "market_cap": 2_000_000_000_000,
            "pe_ttm": 28.0,
            "roe": 0.30,
            "data_quality": "GOOD",
        }
        res = se.compute_scores(payload)
        d = _as_dict(res)

        # Range check tolerates None (legitimate "insufficient" signal under
        # v5.0.0 gating, even though this payload is meant to satisfy gates).
        for k in (
            "value_score", "quality_score", "momentum_score",
            "risk_score", "opportunity_score", "overall_score",
        ):
            self.assertIn(k, d, msg=f"Missing key: {k}")
            _check_score_range_or_none(self, k, d[k])

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

        v3.0.0: tolerates None for forecasts the engine declined to
        compute, but enforces the fraction range when a value IS present.
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
            if val is None:
                continue  # v5.0.0: legitimate "couldn't forecast" response
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
        val_12m = d["expected_roi_12m"]
        if val_12m is None:
            self.skipTest("engine returned None for expected_roi_12m")
        self.assertTrue(_is_number(val_12m))

        r12 = _to_float(val_12m)
        self.assertLessEqual(
            r12, 0.0,
            msg=f"expected_roi_12m should be negative, got {r12}",
        )

        min_roi_12m = _get_env_float("TFB_TEST_MIN_ROI_12M_FRACTION", -0.65)
        max_roi_12m = _get_env_float("TFB_TEST_MAX_ROI_12M_FRACTION", 0.65)
        self.assertGreaterEqual(r12, min_roi_12m)
        self.assertLessEqual(r12, max_roi_12m)

        self.assertIn("forecast_price_12m", d)
        fp12_val = d["forecast_price_12m"]
        if fp12_val is None:
            return  # forecast price suppressed, but ROI was computed; OK
        self.assertTrue(_is_number(fp12_val))
        fp12 = _to_float(fp12_val)
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
        # v3.0.0: tolerate None (stricter gating)
        _check_score_range_or_none(self, "overall_score", d["overall_score"])
        self.assertIn("scoring_errors", d)
        self.assertIsInstance(d["scoring_errors"], list)

    # -------------------- enrich_with_scores() --------------------

    def test_enrich_with_scores_returns_expected_fields(self) -> None:
        """enrich_with_scores returns a dict with all required fields."""
        payload = {
            "symbol": "AAPL", "price": 100.0,
            "previous_close": 98.0,
            "pe_ttm": 28.0, "roe": 0.30,
            "data_quality": "GOOD",
        }
        out = se.enrich_with_scores(payload, in_place=False)
        self.assertIsInstance(out, dict)

        for k in ("overall_score", "risk_score", "recommendation"):
            self.assertIn(k, out)
        for k in ("expected_roi_12m", "expected_roi_3m", "expected_roi_1m"):
            self.assertIn(k, out)
        for k in ("forecast_price_12m", "confidence_score"):
            self.assertIn(k, out)
        # v3.0.0: also verify the 4 view fields are emitted into rows
        for k in CANONICAL_VIEW_FIELDS:
            self.assertIn(
                k, out,
                msg=(
                    f"enrich_with_scores omitted view field {k!r}. "
                    "scoring v5.0.0 should write all 4 view columns to the row."
                ),
            )

    def test_enrich_with_scores_in_place_updates_dict(self) -> None:
        """in_place=True modifies and returns the original dict."""
        payload = {
            "symbol": "AAPL", "price": 100.0,
            "previous_close": 99.0,
            "pe_ttm": 28.0, "roe": 0.30,
            "data_quality": "GOOD",
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
            "pe_ttm": 18.0, "roe": 0.20,
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
        """AssetScores dataclass has all canonical fields (incl. 4 views)."""
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
        # v3.0.0: also tolerate None overall_score under stricter gating
        _check_score_range_or_none(self, "overall_score", d["overall_score"])

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
