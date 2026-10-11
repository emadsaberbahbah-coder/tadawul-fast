"""WP-2b (core/analysis/opportunity_builder.py v1.25.1) - the builder's
Scoring Settlement gate reads the engine's settle_unresolved:enforce receipt.

Under data_engine_v2 5.153.0 TFB_SETTLE_UNRESOLVED=enforce a non-converged
row stops carrying f7_settle_failed:<reason> and publishes its pass-1 score
tagged settle_unresolved:enforce:<reason>:p<n>:ov=..:rc=... Without this
reader such a row would reach the board as an ordinary WATCHLIST row whose
only exclusion is the opt-in Investability gate. v1.25.1 maps the tag to
'unresolved_<reason>', which fails the Scoring Settlement gate MAJOR exactly
like a failure receipt (pattern: tests/test_ob_board_funding.py
test_failed_scoring_settlement_cannot_rank_or_fund_with_policy_gates_off).
A failure receipt still wins; the observe-form tag is not a failure marker.

REAL builder module; synthetic market rows; research-stage builds need no
portfolio figures. Every test pins its own environment and restores it.

Run: python -m pytest -q tests/test_ob_settle_unresolved.py   (or python tests/test_ob_settle_unresolved.py)
"""
from __future__ import annotations

import copy
import functools
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.analysis import opportunity_builder as ob  # noqa: E402
from tests.decision_evidence_fixtures import (build_with_observed_inputs, observed_portfolio,  # noqa: E402
                                              observed_price_fields)

FX = {"SAR": 1.0}
UNRES = "settle_unresolved:enforce:pass_limit:p3:ov=55.02:rc=hold"
CRITERIA = {"investability_gate_enabled": False, "sell_class_gate_enabled": False,
            "max_selected": 2, "board_funding_stage": "research"}
AUTH_ENV_KEYS = ("APP_TOKEN", "TFB_APP_TOKEN", "BACKEND_TOKEN", "BACKUP_APP_TOKEN", "ALLOWED_TOKENS",
                 "TFB_ALLOWED_TOKENS", "APP_TOKENS", "X_APP_TOKEN", "API_KEY", "TFB_TOKEN")


def _isolated(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        saved = dict(os.environ)
        try:
            for key in list(os.environ):
                if key.startswith("TFB_") or key in AUTH_ENV_KEYS or key == "RENDER_GIT_COMMIT":
                    del os.environ[key]
            os.environ["TFB_OPP_ENABLED"] = "1"
            os.environ["TFB_OPP_FUNDING_PLAN"] = "1"
            os.environ["TFB_OPP_INVESTABILITY_GATE"] = "0"
            os.environ["TFB_T10_REL_FLOOR_MODE"] = "display"
            os.environ["APP_TOKEN"] = "synthetic-board-test"
            return fn(*args, **kwargs)
        finally:
            os.environ.clear()
            os.environ.update(saved)
    return wrapper


def _row(symbol, **overrides):
    row = {"symbol": symbol, "name": "Synthetic " + symbol, "sector": "Energy", "market": "Tadawul",
           "currency": "SAR", "current_price": 100.0, "intrinsic_value": 130.0,
           "forecast_reliability_score": 82.0, "data_quality_score": 91.0, "risk_bucket": "Moderate",
           "provider_engine_conflict": "No", "volatility_30d": 4.0, "avg_volume_30d": 2500000,
           "expected_roi_12m": 24.0, "recommendation_detailed": "STRONG BUY",
           "investability_status": "WATCHLIST", "block_reason": ""}
    row.update(observed_price_fields())
    row.update(overrides)
    return row


def _research(rows):
    payload = build_with_observed_inputs(ob, copy.deepcopy(rows), criteria=dict(CRITERIA),
                                         portfolio=observed_portfolio({}, FX), fx_rates=dict(FX))
    assert payload["status"] == "ok", payload.get("message")
    return {c["symbol"]: c for c in payload["candidates_rows"]}


@_isolated
def test_b1_reader_maps_the_enforce_receipt_and_keeps_failure_precedence():
    f = ob._scoring_settlement_failure
    assert f({"warnings": "provider_ok;" + UNRES}) == "unresolved_pass_limit"
    assert f({"Warnings": ["a", "settle_unresolved:enforce:oscillation:p2:ov=48.25:rc=sell"]}) == "unresolved_oscillation"
    assert f({"warnings": "settle_unresolved:enforce:pass_limit"}) == "unresolved_pass_limit"
    # observe-form receipts, substrings and margin receipts are not failures
    for clean in ("settle_unresolved:observe:pass_limit:p3:ov=55.02:rc=hold", "x_settle_unresolved:enforce:pass_limit",
                  "margin_publish:profit_margin:unresolved", "rank_skipped_settle_unresolved", "", None):
        assert f({"warnings": clean}) is None, clean
    # a failure receipt anywhere wins over an unresolved receipt (both scans)
    assert f({"warnings": UNRES, "Warnings": "f7_settle_failed:non_converged"}) == "non_converged"
    assert f({"Warnings": UNRES, "scoring_errors": "f7_settle_failed:error"}) == "error"
    assert tuple(map(int, ob.OPPORTUNITY_BUILDER_VERSION.split("."))) >= (1, 26, 1)


@_isolated
def test_b2_scoring_settlement_gate_fails_major_on_the_candidate():
    crit = ob.make_criteria(dict(CRITERIA))
    cand = ob.normalize_candidate(_row("UNRES.SR", warnings="provider_ok;" + UNRES), dict(FX), crit)
    assert cand["scoring_settlement_failure"] == "unresolved_pass_limit"
    gates = ob.evaluate_gates(cand, crit)
    first = ob.first_failed_gate(gates)
    assert first["gate"] == "Scoring Settlement" and first["fail_class"] == ob.FAIL_MAJOR
    assert first["current"] == "unresolved_pass_limit"
    assert ob.derive_verdict(gates, cand.get("reliability")) == ob.VERDICT_DNI
    clean = ob.normalize_candidate(_row("CLEAN.SR"), dict(FX), crit)
    assert "scoring_settlement_failure" not in clean


@_isolated
def test_b3_research_build_never_ranks_or_scores_an_unresolved_row():
    rows = [_row("UNRES.SR", warnings="provider_ok;" + UNRES), _row("CLEAN.SR")]
    by_symbol = _research(rows)
    failed = by_symbol["UNRES.SR"]
    assert failed["verdict"] == "DO_NOT_INVEST"
    assert failed["opportunity_score"] is None
    assert failed["first_fail"]["gate"] == "Scoring Settlement"
    assert failed["first_fail"]["current"] == "unresolved_pass_limit"
    assert by_symbol["CLEAN.SR"]["verdict"] != "DO_NOT_INVEST"
    # the observe-form receipt is not a failure marker: same verdict / score as clean
    observed = _research([_row("OBS.SR", warnings="provider_ok;settle_unresolved:observe:pass_limit:p3:ov=55:rc=hold")])
    clean = _research([_row("OBS.SR", warnings="provider_ok")])
    for key in ("verdict", "opportunity_score", "first_fail"):
        assert observed["OBS.SR"][key] == clean["OBS.SR"][key], key


TESTS = [test_b1_reader_maps_the_enforce_receipt_and_keeps_failure_precedence,
         test_b2_scoring_settlement_gate_fails_major_on_the_candidate,
         test_b3_research_build_never_ranks_or_scores_an_unresolved_row]

if __name__ == "__main__":
    import traceback
    fails = 0
    for t in TESTS:
        try:
            t()
            print("PASS", t.__name__)
        except Exception:
            fails += 1
            print("FAIL", t.__name__)
            traceback.print_exc()
    print("RESULT", "PASS" if not fails else "FAIL", "%d/%d" % (len(TESTS) - fails, len(TESTS)),
          "builder", ob.OPPORTUNITY_BUILDER_VERSION)
    sys.exit(1 if fails else 0)
