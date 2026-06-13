#!/usr/bin/env python3
"""
tests/test_opportunity_builder.py
================================================================================
OPPORTUNITY BUILDER CONTRACT TESTS — v1.0.0  (HARDENED / CI-FRIENDLY / NO-PRINT)
================================================================================
Emad Bahbah – Tadawul Fast Bridge

Guards core/analysis/opportunity_builder.py (Plan v5.0 P2, builder v1.0.2+):
the §4.2 hard-gate truth table, the §4.3 score-weight invariant, the §4.4
wealth math + L7 funding identity, the verdict<->gate-trace contract, the
§5 frozen zone payload, and the v1.0.2 conflict-parse fix (regression guard so
"No conflict" can never silently flip back to blocking).

WHY v1.0.0
- stdlib unittest only (no network, no prints, CI-friendly) — matches
  test_scoring_engine_contract.py house style.
- Dual-mode import: package path (core.analysis.opportunity_builder) first,
  flat fallback second, so it runs under pytest from the repo root AND as a
  standalone script next to the module.
- Clear failure diagnostics: import errors and missing §5 keys are surfaced.

WIRING
- Pytest default collects test_*.py, so this file is collected as-is.
- Standalone:  python3 tests/test_opportunity_builder.py   -> exit 0 on pass.
- This file MUST live in tests/ (alongside test_portfolio_actions.py,
  test_scoring_engine_contract.py). A repo-root copy is NOT collected by the
  tests/ pytest run and is the "script not found" symptom.
"""

from __future__ import annotations

import math
import os
import sys
import unittest
from typing import Any, Dict, List

# ---------------------------------------------------------------------------
# Path + tolerant import (package path first, flat fallback). conftest.py in
# tests/ normally puts the repo root on sys.path; we insert it defensively so
# the file also runs standalone.
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_HERE)
for _p in (_REPO_ROOT, _HERE):
    if _p and _p not in sys.path:
        sys.path.insert(0, _p)

_IMPORT_ERR = None
try:  # deployed: repo root ~/project/src, module at core/analysis/
    from core.analysis import opportunity_builder as ob  # type: ignore
except Exception as _e1:
    try:  # standalone: test sitting next to the module
        import opportunity_builder as ob  # type: ignore
        _e1 = None
    except Exception as _e2:
        ob = None  # type: ignore
        _IMPORT_ERR = (_e1, _e2)


# ---------------------------------------------------------------------------
# Hygiene-safe output helper (no print())
# ---------------------------------------------------------------------------
def _out(msg: str) -> None:
    try:
        sys.stdout.write(str(msg) + ("\n" if not str(msg).endswith("\n") else ""))
    except Exception:
        pass


def _row(**kw: Any) -> Dict[str, Any]:
    """A clean, otherwise-investable canonical row (live schema_registry
    v2.13.0 compact keys). Override any field via kwargs."""
    base = {
        "symbol": "1120.SR", "name": "Al Rajhi", "sector": "Banks",
        "market": "Tadawul", "currency": "SAR", "current_price": 100.0,
        "intrinsic_value": 130.0, "forecast_reliability_score": 82.0,
        "data_quality_score": 91.0, "risk_bucket": "Moderate",
        "provider_engine_conflict": "No", "volatility_30d": 4.0,
        "avg_volume_30d": 2500000, "expected_roi_12m": 24.0,
        "recommendation_detailed": "STRONG BUY",
        "investability_status": "INVESTABLE", "block_reason": "",
    }
    base.update(kw)
    return base


def _candidate(payload: Dict[str, Any], idx: int = 0) -> Dict[str, Any]:
    rows = payload.get("candidates_rows") or []
    return rows[idx] if idx < len(rows) else {}


_SECTION5_KEYS = ("version", "status", "kpis", "selected", "near_miss",
                  "alerts", "candidates_rows", "meta")


class TestOpportunityBuilderContract(unittest.TestCase):

    def setUp(self) -> None:
        if ob is None:
            self.fail("opportunity_builder import failed: {!r}".format(_IMPORT_ERR))

    # -- module surface -----------------------------------------------------

    def test_version_and_public_surface(self) -> None:
        self.assertTrue(hasattr(ob, "OPPORTUNITY_BUILDER_VERSION"))
        ver = ob.OPPORTUNITY_BUILDER_VERSION
        self.assertRegex(str(ver), r"^\d+\.\d+\.\d+$")
        for name in ("build_opportunity_payload", "derive_verdict",
                     "normalize_candidate", "confidence_band",
                     "make_criteria", "SCORE_WEIGHTS"):
            self.assertTrue(hasattr(ob, name),
                            "missing public symbol: {}".format(name))

    def test_score_weights_sum_to_100(self) -> None:
        total = sum(float(w) for w in ob.SCORE_WEIGHTS.values())
        self.assertAlmostEqual(total, 100.0, places=6,
                               msg="SCORE_WEIGHTS must sum to 100")

    # -- §5 frozen payload --------------------------------------------------

    def test_empty_input_is_valid_no_candidates(self) -> None:
        p = ob.build_opportunity_payload([])
        for k in _SECTION5_KEYS:
            self.assertIn(k, p, "missing §5 zone key: {}".format(k))
        self.assertEqual(p["status"], "no_candidates")
        self.assertEqual(p["selected"], [])
        self.assertEqual(p["kpis"].get("scanned"), 0)

    def test_disabled_switch_returns_skeleton(self) -> None:
        prev = os.environ.get("TFB_OPP_ENABLED")
        os.environ["TFB_OPP_ENABLED"] = "0"
        try:
            p = ob.build_opportunity_payload([_row()])
            self.assertEqual(p["status"], "disabled")
            for k in _SECTION5_KEYS:
                self.assertIn(k, p)
        finally:
            if prev is None:
                os.environ.pop("TFB_OPP_ENABLED", None)
            else:
                os.environ["TFB_OPP_ENABLED"] = prev

    # -- §4.2 gates / verdicts ---------------------------------------------

    def test_clean_row_is_invest_and_sized(self) -> None:
        p = ob.build_opportunity_payload(
            [_row()], portfolio={"cash_available_sar": 50000})
        a = _candidate(p)
        self.assertEqual(a.get("verdict"), "INVEST",
                         "first_fail={}".format(a.get("first_fail")))
        self.assertTrue(p["selected"], "expected one selected ticket")
        t = p["selected"][0]
        self.assertGreater(t["suggested_shares"], 0)
        self.assertGreater(t["suggested_sar"], 0)

    def test_l7_funding_identity(self) -> None:
        # unallocated == deployable - Σ suggested (L7)
        p = ob.build_opportunity_payload(
            [_row()], portfolio={"cash_available_sar": 50000})
        deployable = p["kpis"]["deployable_sar"]
        total = sum(t["suggested_sar"] for t in p["selected"])
        self.assertAlmostEqual(p["kpis"]["capital_unallocated_sar"],
                               deployable - total, delta=1.0)
        for t in p["selected"]:
            self.assertIn("funds_from", t["detail"])

    def test_missing_fx_is_major_fail(self) -> None:
        a = _candidate(ob.build_opportunity_payload([_row(symbol="Z.XX",
                                                          currency="ZZZ")]))
        self.assertEqual(a.get("verdict"), "DO_NOT_INVEST")
        self.assertEqual((a.get("first_fail") or {}).get("gate"), "FX")

    def test_gbp_subunit_resolves_div_100(self) -> None:
        a = _candidate(ob.build_opportunity_payload(
            [_row(symbol="L.LON", currency="GBp", current_price=500.0)]))
        self.assertEqual(a.get("fx_source"), "static/100")

    def test_tiered_reliability_bands(self) -> None:
        # within (min_reliability - 15) band -> NON_CRITICAL -> WATCH
        a = _candidate(ob.build_opportunity_payload(
            [_row(forecast_reliability_score=60.0)]))
        self.assertEqual(a.get("verdict"), "WATCH")
        # below the band -> MAJOR -> DO_NOT_INVEST
        a = _candidate(ob.build_opportunity_payload(
            [_row(forecast_reliability_score=40.0)]))
        self.assertEqual(a.get("verdict"), "DO_NOT_INVEST")

    def test_max_selected_cap_and_capacity_near_miss(self) -> None:
        rows = [_row(symbol="S{}.SR".format(i), sector="Sec{}".format(i))
                for i in range(6)]
        p = ob.build_opportunity_payload(
            rows, criteria={"max_selected": 3},
            portfolio={"cash_available_sar": 500000})
        self.assertEqual(len(p["selected"]), 3)
        self.assertTrue(any(n.get("failed_gate") == "Capacity"
                            for n in p["near_miss"]),
                        "expected a Capacity near-miss row")

    def test_verdict_matches_gate_trace_contract(self) -> None:
        rows = [_row(symbol="S{}.SR".format(i), sector="Sec{}".format(i))
                for i in range(6)]
        p = ob.build_opportunity_payload(
            rows, portfolio={"cash_available_sar": 500000})
        for a in p["candidates_rows"]:
            self.assertEqual(
                ob.derive_verdict(a["gates"], a["reliability"]),
                a["verdict"],
                "derive_verdict must reproduce the builder verdict for {}"
                .format(a.get("symbol")))

    # -- v1.0.2 conflict-parse regression guard -----------------------------

    def test_free_text_no_conflict_is_not_blocked(self) -> None:
        # v1.0.2: descriptive negations must NOT be read as conflict present.
        for val in ("No conflict", "no provider/engine conflict",
                    "No Conflict Detected", "conflict-free",
                    "without conflict"):
            a = _candidate(ob.build_opportunity_payload(
                [_row(provider_engine_conflict=val)],
                portfolio={"cash_available_sar": 50000}))
            self.assertIs(a.get("conflict"), False,
                          "'{}' must parse as no-conflict".format(val))
            self.assertEqual(a.get("verdict"), "INVEST",
                             "'{}' wrongly blocked: first_fail={}".format(
                                 val, a.get("first_fail")))

    def test_real_conflict_still_blocks(self) -> None:
        for val in ("Yes", "conflict", "provider conflict flagged",
                    "notable conflict in data"):
            a = _candidate(ob.build_opportunity_payload(
                [_row(provider_engine_conflict=val)],
                portfolio={"cash_available_sar": 50000}))
            self.assertIs(a.get("conflict"), True,
                          "'{}' must parse as conflict present".format(val))
            self.assertEqual(a.get("verdict"), "DO_NOT_INVEST")
            self.assertEqual((a.get("first_fail") or {}).get("gate"),
                             "Conflict")

    # -- robustness ---------------------------------------------------------

    def test_nan_input_is_json_safe(self) -> None:
        import json
        p = ob.build_opportunity_payload([_row(current_price=float("nan"))])
        # must not raise — _json_safe coerces NaN/inf to None
        json.dumps(p)

    def test_malformed_rows_never_raise(self) -> None:
        weird: List[Any] = [None, 42, "not-a-dict", {}, {"symbol": None}]
        p = ob.build_opportunity_payload(weird,
                                         portfolio={"cash_available_sar": 1000})
        self.assertIn(p["status"], ("ok", "no_candidates"))
        for k in _SECTION5_KEYS:
            self.assertIn(k, p)


def main() -> int:
    """Runner for direct execution: python3 tests/test_opportunity_builder.py"""
    try:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(
            TestOpportunityBuilderContract)
        verbose = 0
        try:
            verbose = int(float(os.getenv("TFB_TEST_VERBOSE") or "0"))
        except Exception:
            verbose = 0
        runner = unittest.TextTestRunner(
            verbosity=2 if verbose > 0 else 1, stream=sys.stdout)
        result = runner.run(suite)
        return 0 if result.wasSuccessful() else 2
    except Exception as e:  # pragma: no cover
        _out("[opportunity_builder_test] fatal_error: {!r}".format(e))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
