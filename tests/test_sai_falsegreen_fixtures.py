#!/usr/bin/env python3
"""tests/test_sai_falsegreen_fixtures.py — IR-092 (Batch 4b, 2026-08-23).

THE FOUR ROWS THAT DIED TODAY, PINNED FOREVER.

On 2026-08-23 the independent morning review found four rows published as
INVESTABLE / INVEST while their own Warnings carried fetch_failed (or, for
the Copper zombie, an inverted day range on a stale literal symbol):

    UNI.MI          GM   Open 185.3 vs range 27.02-28.11, fetch_failed:404
    KE=F            CFX  Open 313.26 vs range 765-780.25, fetch_failed
    ALI=F           CFX  Open 20.87  vs range 3,320-3,416, fetch_failed
    'Copper Futures' CFX High 6.4955 < Low 6.728, stale 08-13, fetch_failed

Re-execution on the primary TSVs confirmed every one to the digit. The fix
was ARMING the already-shipped W1A-2 invariant (surface_blocked 13:09Z,
surface_fetchfail 13:24Z, both verified in engine_gates). These tests make
the class un-regressable: the REAL apply_surface_action_invariants must
demote each fixture whenever the gates are armed, and must be a strict
no-op with the SAME object back when they are not (production's pre-arming
posture — also the reason these four could publish at all).

Pure stdlib + the real module; lean-CI safe. Env is saved/restored around
every test.
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core import surface_action_invariants as sai  # noqa: E402

_GATES = ("TFB_T10_BLOCKED_INVARIANT", "TFB_SURFACE_BLOCKED_INVARIANT",
          "TFB_T10_FETCHFAIL_BLOCKED", "TFB_SURFACE_FETCHFAIL_BLOCKED",
          "TFB_WARN_INVEST_INVARIANT", "TFB_SURFACE_WARN_INVEST",
          "TFB_ROW_SANITY_QUARANTINE", "TFB_SURFACE_ROW_SANITY")


def _clear():
    for k in _GATES:
        os.environ.pop(k, None)


def _fixture_rows():
    """The 2026-08-23 export shapes, condensed to the fields SAI reads."""
    return [
        {  # GM — the dangerous one: DQ 100 / Rel 85.1 sails through T10 gates
            "symbol": "UNI.MI", "name": "UniCredit S.p.A.",
            "current_price": 27.5, "open": 185.3,
            "day_low": 27.02, "day_high": 28.11,
            "data_quality": 100.0, "forecast_reliability": 85.1,
            "investability_status": "INVESTABLE", "final_action": "INVEST",
            "recommendation": "BUY",
            "warnings": "fetch_failed: http 404 not_found",
        },
        {
            "symbol": "KE=F", "name": "KC Wheat",
            "current_price": 770.0, "open": 313.26,
            "day_low": 765.0, "day_high": 780.25,
            "investability_status": "INVESTABLE", "final_action": "INVEST",
            "recommendation": "BUY",
            "warnings": "fetch_failed; intrinsic_soft_ceiling_applied",
        },
        {
            "symbol": "ALI=F", "name": "Aluminum",
            "current_price": 3370.0, "open": 20.87,
            "day_low": 3320.0, "day_high": 3416.0,
            "investability_status": "INVESTABLE", "final_action": "INVEST",
            "recommendation": "BUY",
            "warnings": "fetch_failed",
        },
        {  # the zombie: literal symbol, inverted band, stale since 08-13
            "symbol": "Copper Futures", "name": "Commodity",
            "current_price": 6.6, "open": 6.6345,
            "day_low": 6.728, "day_high": 6.4955,
            "investability_status": "INVESTABLE", "final_action": "INVEST",
            "recommendation": "BUY",
            "warnings": "fetch_failed; stale_row",
        },
    ]


def _is_dead(row):
    """Post-invariant contract: the row can no longer read as an
    executable BUY/INVEST surface."""
    fa = str(row.get("final_action", "")).upper()
    inv = str(row.get("investability_status", "")).upper()
    rec = str(row.get("recommendation", "")).upper()
    return ("INVEST" != fa and "INVESTABLE" not in inv
            and rec not in ("BUY", "STRONG_BUY"))


class TestFalseGreenFixtures(unittest.TestCase):
    def setUp(self):
        _clear()

    def tearDown(self):
        _clear()

    # ------------------------------------------------------------------ #
    def test_gates_off_strict_noop_same_object(self):
        """Pre-arming posture — exactly how the four escaped. Must return
        the SAME object, byte-untouched fields, zero counters."""
        rows = _fixture_rows()
        snap = [dict(r) for r in rows]
        out, n1, n2, n3, n4, errs = sai.apply_surface_action_invariants(
            rows, sheet="Global_Markets")
        self.assertIs(out, rows)
        self.assertEqual((n1, n2, n3, n4, errs), (0, 0, 0, 0, 0))
        self.assertEqual(rows, snap)

    def test_armed_ladder_kills_all_four(self):
        """The 2026-08-23 production arming (#1 13:09Z + #2 13:24Z):
        every fixture must stop reading as an executable INVEST."""
        os.environ["TFB_SURFACE_BLOCKED_INVARIANT"] = "1"
        os.environ["TFB_SURFACE_FETCHFAIL_BLOCKED"] = "1"
        rows = _fixture_rows()
        out, n1, n2, n3, n4, errs = sai.apply_surface_action_invariants(
            rows, sheet="Commodities_FX")
        self.assertEqual(errs, 0)
        self.assertGreaterEqual(n2, 4, "all four carry fetch_failed")
        for row in out:
            self.assertTrue(
                _is_dead(row),
                f"{row['symbol']} still executable: "
                f"fa={row.get('final_action')} inv="
                f"{row.get('investability_status')} rec="
                f"{row.get('recommendation')}")

    def test_fetchfail_gate_alone_suffices(self):
        """W1A-2's own contract (v1.0.1 P0-3): the fetch-failure FACT
        drives the demotion even without ladder #1."""
        os.environ["TFB_SURFACE_FETCHFAIL_BLOCKED"] = "1"
        rows = _fixture_rows()
        out, _, n2, *_ = sai.apply_surface_action_invariants(rows)
        self.assertGreaterEqual(n2, 4)
        self.assertTrue(all(_is_dead(r) for r in out))

    def test_alias_env_names_equivalent(self):
        """Either alias arms the gate — the 2026-08-23 Render fix added
        both rows; neither may silently stop working."""
        os.environ["TFB_T10_FETCHFAIL_BLOCKED"] = "1"
        rows = _fixture_rows()
        out, *_ = sai.apply_surface_action_invariants(rows)
        self.assertTrue(all(_is_dead(r) for r in out))

    def test_healthy_row_untouched_under_full_arming(self):
        """The gates must kill the class, not the page: a clean BUY row
        sails through byte-identical."""
        os.environ["TFB_SURFACE_BLOCKED_INVARIANT"] = "1"
        os.environ["TFB_SURFACE_FETCHFAIL_BLOCKED"] = "1"
        clean = {"symbol": "1050.SR", "name": "Banque Saudi Fransi",
                 "current_price": 20.79, "open": 20.6,
                 "day_low": 20.3, "day_high": 21.0,
                 "investability_status": "INVESTABLE",
                 "final_action": "INVEST", "recommendation": "BUY",
                 "warnings": ""}
        snap = dict(clean)
        out, n1, n2, n3, n4, errs = sai.apply_surface_action_invariants(
            [clean])
        self.assertEqual((n1, n2, n3, n4, errs), (0, 0, 0, 0, 0))
        self.assertEqual(out[0], snap)

    def test_demotion_leaves_audit_trail(self):
        """A killed row must say WHY on the sheet — a warning tag has to
        be appended, never a silent flip."""
        os.environ["TFB_SURFACE_FETCHFAIL_BLOCKED"] = "1"
        rows = _fixture_rows()
        before = [str(r.get("warnings", "")) for r in rows]
        out, *_ = sai.apply_surface_action_invariants(rows)
        for b, row in zip(before, out):
            self.assertGreater(len(str(row.get("warnings", ""))), len(b),
                               f"{row['symbol']}: no audit tag appended")


if __name__ == "__main__":
    unittest.main(verbosity=2)
