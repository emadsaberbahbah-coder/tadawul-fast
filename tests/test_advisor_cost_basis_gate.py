# -*- coding: utf-8 -*-
"""
Integration test for Wire 1 — the cost-basis gate in
core.investment_advisor._score_recommendation.

Contract:
  * default (TFB_QUALITY_GATES unset): the gate is inactive; an implausible
    cost basis is NOT blocked (behavior unchanged vs pre-wire).
  * TFB_QUALITY_GATES=1: a holding with an implausible cost basis is forced to
    the enum-safe HOLD with a "BLOCKED -- review cost basis" reason, while a
    plausible holding and a cost-basis-less opportunity are untouched.

Skips automatically where the advisor's import chain is unavailable, so it runs
in full environments and is skipped in the lean foundations CI.
"""
import os

import pytest

IA = pytest.importorskip("core.investment_advisor")


def _rec(row):
    return IA._score_recommendation(dict(row))


BBD = {"symbol": "BBD.US", "avg_cost": 250, "current_price": 3.50,
       "position_qty": 4, "overall_score": 78, "risk_score": 25,
       "confidence_score": 80, "expected_roi_3m": 0.12, "opportunity_score": 75}
NORMAL = {"symbol": "AAPL.US", "avg_cost": 180, "current_price": 210,
          "position_qty": 10, "overall_score": 78, "risk_score": 25,
          "confidence_score": 80, "expected_roi_3m": 0.12, "opportunity_score": 75}
OPP = {"symbol": "NEW.US", "current_price": 50, "overall_score": 78,
       "risk_score": 25, "confidence_score": 80, "expected_roi_3m": 0.12,
       "opportunity_score": 75}


def setup_function(_):
    os.environ.pop("TFB_QUALITY_GATES", None)


def teardown_function(_):
    os.environ.pop("TFB_QUALITY_GATES", None)


def test_off_does_not_block_bbd():
    os.environ.pop("TFB_QUALITY_GATES", None)
    _rec_, reason, _c = _rec(BBD)
    assert "BLOCKED" not in reason  # gate inactive by default


def test_on_blocks_bbd():
    os.environ["TFB_QUALITY_GATES"] = "1"
    rec, reason, _c = _rec(BBD)
    assert rec == "HOLD"
    assert "BLOCKED" in reason and "cost basis" in reason.lower()


def test_on_leaves_plausible_holding_untouched():
    os.environ["TFB_QUALITY_GATES"] = "1"
    _rec_, reason, _c = _rec(NORMAL)
    assert "BLOCKED" not in reason


def test_on_skips_opportunity_without_cost_basis():
    os.environ["TFB_QUALITY_GATES"] = "1"
    _rec_, reason, _c = _rec(OPP)
    assert "BLOCKED" not in reason  # scoped to holdings (only fires when avg_cost present)


if __name__ == "__main__":
    import sys
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and callable(v)]
    passed = 0
    for fn in fns:
        setup_function(None)
        try:
            fn()
            print("PASS", fn.__name__)
            passed += 1
        except AssertionError as e:
            print("FAIL", fn.__name__, "->", e)
        finally:
            teardown_function(None)
    print(f"\n{passed}/{len(fns)} passed")
    sys.exit(0 if passed == len(fns) else 1)
