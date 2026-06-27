#!/usr/bin/env python3
"""
tests/test_opportunity_builder.py
================================================================================
OPPORTUNITY BUILDER CONTRACT TESTS — v1.3.0  (HARDENED / CI-FRIENDLY / NO-PRINT)
================================================================================
Emad Bahbah – Tadawul Fast Bridge

Guards core/analysis/opportunity_builder.py (Plan v5.0 P2, builder v1.0.2+):
the §4.2 hard-gate truth table, the §4.3 score-weight invariant, the §4.4
wealth math + L7 funding identity, the verdict<->gate-trace contract, the
§5 frozen zone payload, and the v1.0.2 conflict-parse fix (regression guard so
"No conflict" can never silently flip back to blocking).

v1.1.0 — regression coverage for the funding/selection gates that now run in
production (previously verified only by ad-hoc scripts):
  * v1.0.9 engine-ROI ordering (TFB_OPP_RANK_BY_ENGINE_ROI) — enabling it
    reorders the INVEST pool by the engine 12M forecast; OFF is unchanged.
  * v1.0.9 unfunded-watch (TFB_OPP_UNFUNDED_WATCH) — a 0-SAR (capital-exhausted)
    pick becomes a WATCH near-miss, not a 0-SAR "executable" ticket.
  * v1.0.14 minimum-ticket floor (TFB_OPP_MIN_TICKET_SAR) — a sub-floor ticket
    is deferred (not funded); OFF (floor 0) funds it as before; criteria
    accept + negative-clamp + default-OFF.
  * v1.0.15 floor near-miss labeling — a floor deferral surfaces as a "Funding"
    near-miss, never mislabeled as a diversification cap.

v1.2.0 — coverage for v1.0.16 issuer-level cross-listing dedup
(TFB_OPP_ISSUER_DEDUP): the issuer key collapses same-company listings and keeps
distinct issuers apart; ON defers the duplicate listing ("Duplicate issuer");
OFF funds both (parity); criteria accept + default-OFF.

v1.3.0 — coverage for v1.0.17 duplicate-issuer near-miss labeling: a deferred
cross-listing surfaces in NEAR MISS as the "Duplicate" gate, never mislabeled as
a diversification cap.

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

    # -- v1.0.14 minimum-ticket floor (TFB_OPP_MIN_TICKET_SAR) -------------

    def _floor_scenario(self, min_ticket: float) -> Dict[str, Any]:
        # max_weight 100% so the first name absorbs most cash, leaving a small
        # remainder that the greedy sizer would spend on a 2-share scrap.
        rows = [_row(symbol="AAA.SR", sector="SecA",
                     current_price=300.0, intrinsic_value=390.0),
                _row(symbol="BBB.SR", sector="SecB",
                     current_price=50.0, intrinsic_value=65.0)]
        return ob.build_opportunity_payload(
            rows,
            criteria={"max_weight_pct": 100.0, "max_selected": 5,
                      "min_ticket_sar": min_ticket},
            portfolio={"cash_available_sar": 10300})

    def test_min_ticket_floor_defers_subfloor_ticket(self) -> None:
        p = self._floor_scenario(1000.0)
        sel = {t["symbol"] for t in p["selected"]}
        self.assertIn("AAA.SR", sel, "properly-sized ticket must fund")
        self.assertNotIn("BBB.SR", sel, "sub-floor ticket must NOT fund")
        bbb = next(a for a in p["candidates_rows"] if a["symbol"] == "BBB.SR")
        self.assertIn("minimum ticket floor", (bbb.get("deferral") or ""),
                      "sub-floor name must carry the floor deferral reason")

    def test_min_ticket_floor_off_funds_subfloor(self) -> None:
        # OFF (floor 0): the same scrap IS funded -> parity with pre-floor.
        sel = {t["symbol"] for t in self._floor_scenario(0.0)["selected"]}
        self.assertIn("BBB.SR", sel,
                      "with the floor OFF the sub-floor ticket funds (parity)")

    def test_min_ticket_floor_criteria_accept_and_clamp(self) -> None:
        self.assertEqual(
            ob.make_criteria({"min_ticket_sar": 1500}).get("min_ticket_sar"),
            1500.0)
        self.assertEqual(  # negative clamps to 0 (OFF)
            ob.make_criteria({"min_ticket_sar": -5}).get("min_ticket_sar"),
            0.0)
        self.assertEqual(  # default OFF
            ob.make_criteria().get("min_ticket_sar"), 0.0)

    # -- v1.0.15 floor near-miss labeling ----------------------------------

    def test_floor_near_miss_labeled_funding_not_diversification(self) -> None:
        nm = {n["symbol"]: n for n in self._floor_scenario(1000.0)["near_miss"]}
        self.assertIn("BBB.SR", nm,
                      "sub-floor name should surface in near_miss")
        self.assertEqual(
            nm["BBB.SR"]["failed_gate"], "Funding",
            "a floor deferral must be a Funding near-miss, not Diversification")
        self.assertIn("minimum ticket floor", nm["BBB.SR"]["required"])

    # -- v1.0.9 engine-ROI ordering (TFB_OPP_RANK_BY_ENGINE_ROI) -----------

    def test_engine_roi_ranking_reorders_when_enabled(self) -> None:
        # Identical opportunity_score (equal valuation upside), but ZZZ has the
        # higher engine 12M forecast. OFF -> symbol-tiebreak order (AAA first);
        # ON  -> engine-forecast order (ZZZ first).
        rows = [_row(symbol="AAA.SR", sector="SecA", expected_roi_12m=12.0),
                _row(symbol="ZZZ.SR", sector="SecZ", expected_roi_12m=30.0)]
        port = {"cash_available_sar": 500000}
        off = ob.build_opportunity_payload(
            rows, criteria={"max_selected": 5,
                            "rank_by_engine_roi_enabled": False},
            portfolio=port)
        on = ob.build_opportunity_payload(
            rows, criteria={"max_selected": 5,
                            "rank_by_engine_roi_enabled": True},
            portfolio=port)
        self.assertEqual([t["symbol"] for t in off["selected"]],
                         ["AAA.SR", "ZZZ.SR"], "OFF: opportunity_score order")
        self.assertEqual([t["symbol"] for t in on["selected"]],
                         ["ZZZ.SR", "AAA.SR"], "ON: engine-forecast order")

    def test_engine_roi_ranking_criteria_accept(self) -> None:
        self.assertFalse(
            ob.make_criteria().get("rank_by_engine_roi_enabled"))
        self.assertTrue(
            ob.make_criteria({"rank_by_engine_roi_enabled": True}).get(
                "rank_by_engine_roi_enabled"))

    # -- v1.0.9 unfunded-watch (capital exhausted, TFB_OPP_UNFUNDED_WATCH) --

    def test_unfunded_watch_reclasses_zero_sar_pick_to_watch(self) -> None:
        # First name consumes all cash exactly; the next INVEST name sizes to
        # 0 SAR. ON -> a WATCH near-miss, not a 0-SAR "executable" ticket.
        rows = [_row(symbol="AAA.SR", sector="SecA", current_price=100.0),
                _row(symbol="BBB.SR", sector="SecB", current_price=100.0)]
        p = ob.build_opportunity_payload(
            rows,
            criteria={"max_weight_pct": 100.0, "max_selected": 5,
                      "unfunded_watch_enabled": True},
            portfolio={"cash_available_sar": 10000})
        sel = {t["symbol"] for t in p["selected"]}
        self.assertIn("AAA.SR", sel)
        self.assertNotIn("BBB.SR", sel,
                         "a 0-SAR pick must not be an executable ticket")
        nm = {n["symbol"]: n for n in p["near_miss"]}
        self.assertEqual(nm.get("BBB.SR", {}).get("failed_gate"), "Funding")
        self.assertEqual(nm.get("BBB.SR", {}).get("verdict"), "WATCH")

    # -- v1.0.16 issuer-level cross-listing dedup (TFB_OPP_ISSUER_DEDUP) ----

    def test_issuer_dedup_key_collapses_cross_listings(self) -> None:
        # Same company under two symbols -> same key; distinct companies differ.
        same = ob._issuer_key(
            {"symbol": "4502.T",
             "name": "Takeda Pharmaceutical Company Limited"})
        adr = ob._issuer_key(
            {"symbol": "TAK.US",
             "name": "Takeda Pharmaceutical Company Limited"})
        self.assertEqual(same, adr, "cross-listing must key identically")
        other = ob._issuer_key(
            {"symbol": "OTF.US", "name": "Blue Owl Technology Finance Corp."})
        self.assertNotEqual(same, other, "distinct issuers must not collapse")
        # nameless row keys to its own symbol (never false-merges)
        self.assertEqual(
            ob._issuer_key({"symbol": "5023.SR", "name": "5023.SR"}),
            "sym:5023.SR")

    def _twin_listing_payload(self, dedup: bool) -> Dict[str, Any]:
        rows = [_row(symbol="4502.T",
                     name="Takeda Pharmaceutical Company Limited"),
                _row(symbol="TAK.US",
                     name="Takeda Pharmaceutical Company Limited")]
        return ob.build_opportunity_payload(
            rows,
            criteria={"issuer_dedup_enabled": dedup, "max_selected": 10},
            portfolio={"cash_available_sar": 500000})

    def test_issuer_dedup_defers_duplicate_when_enabled(self) -> None:
        p = self._twin_listing_payload(True)
        self.assertEqual(len(p["selected"]), 1,
                         "only one listing of the issuer may be funded")
        sel = {t["symbol"] for t in p["selected"]}
        dup = next(a for a in p["candidates_rows"] if a["symbol"] not in sel)
        self.assertIn("Duplicate issuer", (dup.get("deferral") or ""),
                      "the other listing must defer as a duplicate issuer")

    def test_issuer_dedup_off_keeps_both(self) -> None:
        # OFF -> both listings funded (parity with pre-dedup v1.0.15).
        self.assertEqual(
            len(self._twin_listing_payload(False)["selected"]), 2)

    def test_issuer_dedup_near_miss_labeled_duplicate(self) -> None:
        # v1.0.17: the deferred cross-listing must surface in NEAR MISS as the
        # "Duplicate" gate, never mislabeled as a diversification cap.
        p = self._twin_listing_payload(True)
        sel = {t["symbol"] for t in p["selected"]}
        nm = {n["symbol"]: n for n in p["near_miss"]
              if n["symbol"] not in sel}
        self.assertTrue(nm, "deferred duplicate should surface in near_miss")
        row = next(iter(nm.values()))
        self.assertEqual(
            row["failed_gate"], "Duplicate",
            "duplicate-issuer near-miss must not be labeled 'Diversification'")
        self.assertEqual(row["required"], "one listing per issuer")

    def test_issuer_dedup_criteria_accept(self) -> None:
        self.assertFalse(ob.make_criteria().get("issuer_dedup_enabled"))
        self.assertTrue(ob.make_criteria(
            {"issuer_dedup_enabled": True}).get("issuer_dedup_enabled"))

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
