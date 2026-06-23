# -*- coding: utf-8 -*-
"""
Tests for core.quality_gates v1.0.0 — the trust/plausibility decision layer.
Includes the two live failures the module is meant to fix: BBD.US (absurd cost
basis) and 5023.SR (ghost ticker). Pure functions, no I/O.

Run: pytest tests/test_quality_gates.py   (or: python tests/test_quality_gates.py)
"""
import json

try:
    from core import quality_gates as Q
except Exception:
    import quality_gates as Q  # standalone fallback


# ----- cost-basis plausibility: the BBD case ---------------------------------

def test_bbd_ratio_blocked():
    r = Q.cost_basis_plausibility(250.0, 3.50, quantity=4)
    assert r["verdict"] == "BLOCKED"
    assert r["method"] == "ratio"


def test_bbd_mad_blocked_with_history():
    hist = [3.40, 3.50, 3.45, 3.55, 3.50, 3.60]
    r = Q.cost_basis_plausibility(250.0, 3.50, quantity=4, history=hist)
    assert r["verdict"] == "BLOCKED"
    assert r["method"] == "mad"


def test_plausible_cost_basis_ok():
    assert Q.cost_basis_plausibility(3.40, 3.50, quantity=4)["verdict"] == "OK"
    hist = [3.40, 3.50, 3.45, 3.55, 3.50, 3.60]
    assert Q.cost_basis_plausibility(3.52, 3.50, history=hist)["verdict"] == "OK"


def test_cost_basis_presence_failures():
    assert Q.cost_basis_plausibility(0, 3.50, quantity=4)["verdict"] == "BLOCKED"
    assert Q.cost_basis_plausibility(None, 3.50, quantity=4)["verdict"] == "BLOCKED"
    assert Q.cost_basis_plausibility(-5, 3.50, quantity=4)["verdict"] == "BLOCKED"


def test_no_position_no_block():
    # qty 0 -> no holding -> nothing to validate
    assert Q.cost_basis_plausibility(None, 3.50, quantity=0)["verdict"] == "OK"


def test_low_ratio_blocked():
    # cost basis far below price (e.g. stale/penny mis-entry)
    r = Q.cost_basis_plausibility(0.5, 100.0, quantity=10)
    assert r["verdict"] == "BLOCKED" and r["method"] == "ratio"


# ----- trust level: the 5023.SR ghost ----------------------------------------

def test_5023_ghost_low_trust_excluded():
    row = {"symbol": "5023.SR", "name": "", "current_price": 12.3,
           "dq_score": 24, "momentum_only": True}
    t = Q.trust_level(row)
    assert t["level"] == "LOW"
    assert t["exclude_from_ranking"] is True
    assert any("name" in r.lower() for r in t["reasons"])


def test_healthy_row_high_trust():
    row = {"symbol": "AAPL.US", "name": "Apple Inc", "current_price": 210.0,
           "dq_score": 95, "age_hours": 2, "fundamentals_present": True}
    t = Q.trust_level(row)
    assert t["level"] == "HIGH"
    assert t["exclude_from_ranking"] is False


def test_soft_issue_medium_trust():
    # identified and dense, but stale -> MEDIUM (kept, flagged)
    row = {"symbol": "TEST.US", "name": "Test Corp", "current_price": 50.0,
           "dq_score": 80, "age_hours": 120}
    t = Q.trust_level(row)
    assert t["level"] == "MEDIUM"
    assert t["exclude_from_ranking"] is False
    assert any("stale" in r.lower() for r in t["reasons"])


def test_name_equals_symbol_is_identity_fail():
    row = {"symbol": "5023.SR", "name": "5023.SR", "current_price": 12.3,
           "dq_score": 70}
    assert Q.trust_level(row)["level"] == "LOW"


def test_low_dq_hard_fail():
    row = {"symbol": "X.US", "name": "Real Name", "current_price": 10.0,
           "dq_score": 30}
    assert Q.trust_level(row)["level"] == "LOW"


def test_field_alias_resolution():
    # uses alternative header names -> still resolves
    row = {"ticker": "Y.US", "company_name": "", "last_price": 5.0,
           "data_quality": 20, "is_sparse": True}
    t = Q.trust_level(row)
    assert t["level"] == "LOW"


def test_should_exclude_helper():
    low = {"symbol": "5023.SR", "name": "", "current_price": 1.0, "momentum_only": True}
    high = {"symbol": "AAPL.US", "name": "Apple Inc", "current_price": 200.0, "dq_score": 95}
    assert Q.should_exclude_from_ranking(low) is True
    assert Q.should_exclude_from_ranking(high) is False


# ----- combined holding gate --------------------------------------------------

def test_evaluate_holding_bbd_blocks_action():
    # BBD: plausible identity but absurd cost basis -> action blocked, BLOCKED override
    row = {"symbol": "BBD.US", "name": "Banco Bradesco", "current_price": 3.50,
           "avg_cost": 250.0, "quantity": 4, "dq_score": 90}
    res = Q.evaluate_holding(row)
    assert res["action_block"] is True
    assert res["recommendation_override"] == "BLOCKED"
    assert "cost basis" in res["block_reason"].lower()


def test_evaluate_holding_ghost_review():
    row = {"symbol": "5023.SR", "name": "", "current_price": 12.0,
           "avg_cost": 11.5, "quantity": 100, "momentum_only": True, "dq_score": 24}
    res = Q.evaluate_holding(row)
    assert res["action_block"] is True
    assert res["recommendation_override"] == "REVIEW"


def test_evaluate_holding_clean_passes():
    row = {"symbol": "AAPL.US", "name": "Apple Inc", "current_price": 210.0,
           "avg_cost": 180.0, "quantity": 10, "dq_score": 95, "age_hours": 1,
           "fundamentals_present": True}
    res = Q.evaluate_holding(row)
    assert res["action_block"] is False
    assert res["recommendation_override"] is None


def test_outputs_json_serializable():
    row = {"symbol": "BBD.US", "name": "Banco Bradesco", "current_price": 3.5,
           "avg_cost": 250.0, "quantity": 4}
    for obj in [Q.cost_basis_plausibility(250.0, 3.5, quantity=4),
                Q.trust_level(row), Q.evaluate_holding(row)]:
        json.dumps(obj)


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and callable(v)]
    passed = 0
    for fn in fns:
        try:
            fn()
            print("PASS", fn.__name__)
            passed += 1
        except AssertionError as e:
            print("FAIL", fn.__name__, "->", e)
        except Exception as e:
            print("ERROR", fn.__name__, "->", repr(e))
    print(f"\n{passed}/{len(fns)} passed")
