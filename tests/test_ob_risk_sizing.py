#!/usr/bin/env python3
"""tests/test_ob_risk_sizing.py - opportunity_builder v1.29.0:
TFB_OPP_SIZING_MODE=risk sizes each ticket so its loss at the printed stop
is a fixed share of the sizing basis.

WHY (2026-11-08)
----------------
The week of 2-6 Nov lost 4.7% of the account on three stop-outs in one
session: every ticket was sized at the per-seat cap and stopped 5-8% below
entry, so each stop-out cost ~1% and three in a day cost the week. The cap
sizes by capital and knows nothing about the stop distance. The owner asked
for sizing that bounds the loss, not the ticket.

  R1  default (unset / cap / junk) is byte-identical; conviction unchanged
  R2  risk: sizes are inverse to the stop distance, the per-seat cap binds,
      the total never exceeds the budget, meta.sizing discloses the rule
  R3  a seat whose risk-budget ticket is below the minimum ticket is released
      to cash with a Funding near-miss of its own
  R4  pure _risk_targets: budget x risk / stop, cap, floor release, stop
      fallback, bad inputs
  R5  TFB_OPP_RISK_BUDGET_PCT parsing and clamp (0.1..2.0, default 0.5)
  R6  no top-up: what the risk budget does not spend stays in cash
  R7  the research stage is never sized; the allocate replay sizes only the
      eligible seats on the same rule
  R8  the enforced absolute cash floor is honoured under risk
  R8b worst-entry sizing (TFB_OPP_SIZE_AT_ENTRY_HIGH): the loss from the
      booked entry to the stop never exceeds the budget (review on #936)
  R9  the worked example from the WHY block (NAV 100,000, 0.5%, cap 15%)
  R10 the env reads live in the fingerprinted TFB_OPP_ family; version floor

Run: pytest -q tests/test_ob_risk_sizing.py
"""
from __future__ import annotations

import ast
import copy
import inspect
import os
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
sys.path.insert(0, str(_REPO / "tests"))

from core.analysis import opportunity_builder as ob  # noqa: E402
from decision_evidence_fixtures import build_with_observed_inputs, observed_price_fields  # noqa: E402

CRITERIA = {
    "max_selected": 10,
    "max_per_sector": 10,
    "max_per_market": 10,
    "max_weight_pct": 100.0,
    "pf_max_sector_pct": 100.0,
    "min_ticket_sar": 1_000.0,
    "trust_gate_enabled": False,
}
FX = {"SAR": 1.0}
CASH = 30_000.0


def _row(symbol, upside_pct, vol30, *, rel=90.0, sector="Energy", price=100.0):
    """vol30 drives the printed stop: max(8%, 2.5 x vol30) capped at 35%."""
    row = {
        "symbol": symbol, "name": "Synthetic " + symbol,
        "sector": sector, "market": "Tadawul",
        "currency": "SAR", "current_price": price,
        "intrinsic_value": round(price * (1.0 + upside_pct / 100.0), 4),
        "forecast_reliability_score": rel, "data_quality_score": 95.0,
        "risk_bucket": "Low", "provider_engine_conflict": "No",
        "volatility_30d": vol30, "avg_volume_30d": 2_500_000,
        "expected_roi_12m": upside_pct, "recommendation_detailed": "STRONG BUY",
        "investability_status": "INVESTABLE", "block_reason": "",
    }
    row.update(observed_price_fields())
    return row


# stops: ALPHA 10% (2.5 x 4), BRAVO 20% (2.5 x 8), CHARLIE 8% (the floor)
ROWS = [
    _row("ALPHA.SR", 60.0, 4.0, rel=95.0, sector="Energy"),
    _row("BRAVO.SR", 50.0, 8.0, rel=80.0, sector="Financials"),
    _row("CHARLIE.SR", 20.0, 2.0, rel=72.0, sector="Utilities"),
]
STOPS = {"ALPHA.SR": 10.0, "BRAVO.SR": 20.0, "CHARLIE.SR": 8.0}


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    for key in list(os.environ):
        if key.startswith(("TFB_OPP_", "TFB_T10_")):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("RENDER_GIT_COMMIT", raising=False)
    monkeypatch.setenv("TFB_OPP_ENABLED", "1")
    monkeypatch.setenv("APP_TOKEN", "synthetic-risk-sizing-test")


def _build(rows=ROWS, cash=CASH, **criteria):
    return build_with_observed_inputs(
        ob, copy.deepcopy(rows), criteria={**CRITERIA, **criteria},
        portfolio={"cash_available_sar": cash}, fx_rates=dict(FX))


def _sizes(payload):
    return {t["symbol"]: float(t["suggested_sar"]) for t in payload["selected"]}


def test_r1_default_is_cap_and_byte_identical(monkeypatch):
    base = _build()
    assert base["status"] == "ok", base.get("message")
    assert base["meta"]["sizing"] == {"mode": "cap"}
    sizes = _sizes(base)
    assert sizes == {"ALPHA.SR": CASH}            # the legacy greedy cap
    for raw in ("cap", "nonsense", "risky", "risk-budget"):
        monkeypatch.setenv("TFB_OPP_SIZING_MODE", raw)
        again = _build()
        assert _sizes(again) == sizes and again["meta"]["sizing"] == {"mode": "cap"}, raw
    # conviction is untouched: it still spreads, tops up and never releases here
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "conviction")
    conv = _build()
    assert conv["meta"]["sizing"]["mode"] == "conviction"
    assert set(_sizes(conv)) == set(STOPS) and "topup_sar" in conv["meta"]["sizing"]
    assert "risk_pct" not in conv["meta"]["sizing"]


def test_r2_risk_sizes_inverse_to_the_stop(monkeypatch):
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "risk")
    monkeypatch.setenv("TFB_OPP_RISK_BUDGET_PCT", "2")
    payload = _build()
    assert payload["status"] == "ok", payload.get("message")
    sizes = _sizes(payload)
    assert set(sizes) == set(STOPS), payload["near_miss"]
    # basis 30,000 x 2% = 600 at risk per seat -> 600 / stop
    assert sizes == {"ALPHA.SR": 6_000.0, "BRAVO.SR": 3_000.0, "CHARLIE.SR": 7_500.0}
    assert sizes["CHARLIE.SR"] > sizes["ALPHA.SR"] > sizes["BRAVO.SR"]
    assert sum(sizes.values()) <= CASH + 1e-6
    for sym, stop in STOPS.items():
        assert abs(sizes[sym] * stop / 100.0 - 600.0) < 1e-6      # loss at stop == risk budget
    meta = payload["meta"]["sizing"]
    assert meta["mode"] == "risk" and meta["risk_pct"] == 2.0
    assert meta["risk_sar"] == 600.0 and meta["basis_sar"] == CASH and meta["budget_sar"] == CASH
    assert meta["cohort"] == ["ALPHA.SR", "BRAVO.SR", "CHARLIE.SR"]
    assert meta["stop_pct"] == STOPS and meta["released"] == []
    assert meta["targets"] == {"ALPHA.SR": 6_000.0, "BRAVO.SR": 3_000.0, "CHARLIE.SR": 7_500.0}
    assert "topup_sar" not in meta and "weights" not in meta
    # the per-seat cap binds: 15% of 30,000 = 4,500
    capped = _build(max_weight_pct=15.0)
    assert _sizes(capped) == {"ALPHA.SR": 4_500.0, "BRAVO.SR": 3_000.0, "CHARLIE.SR": 4_500.0}
    assert capped["meta"]["sizing"]["cap_sar"] == 4_500.0


def test_r3_a_sub_floor_risk_ticket_is_released_to_cash(monkeypatch):
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "risk")
    # default 0.5%: 150 at risk -> ALPHA 1,500, BRAVO 750 (< 1,000 floor), CHARLIE 1,875
    payload = _build()
    assert payload["status"] == "ok", payload.get("message")
    sizes = _sizes(payload)
    assert sizes == {"ALPHA.SR": 1_500.0, "CHARLIE.SR": 1_800.0}     # 1,875 lot-floored to 18 sh
    meta = payload["meta"]["sizing"]
    assert meta["risk_pct"] == 0.5 and meta["risk_sar"] == 150.0
    assert meta["released"] == ["BRAVO.SR"] and "BRAVO.SR" not in meta["targets"]
    assert meta["stop_pct"]["BRAVO.SR"] == 20.0
    nm = [r for r in payload["near_miss"] if r["symbol"] == "BRAVO.SR"]
    assert nm, payload["near_miss"]
    assert nm[0]["failed_gate"] == "Funding"
    assert nm[0]["current"].startswith("Risk sizing: at a 20.0% stop") and "kept in cash" in nm[0]["current"]
    assert "TFB_OPP_RISK_BUDGET_PCT" in nm[0]["improve_note"]
    assert "minimum ticket floor (SAR 1,000" in nm[0]["required"] or "1,000" in nm[0]["required"]
    # raising the risk budget funds it; dropping the floor funds it too
    monkeypatch.setenv("TFB_OPP_RISK_BUDGET_PCT", "1")
    assert _sizes(_build())["BRAVO.SR"] == 1_500.0
    monkeypatch.setenv("TFB_OPP_RISK_BUDGET_PCT", "0.5")
    assert _sizes(_build(min_ticket_sar=0.0))["BRAVO.SR"] == 700.0        # 750 lot-floored


def test_r4_pure_risk_targets_rules():
    rows = [{"symbol": "A", "stop_pct": 10.0}, {"symbol": "B", "stop_pct": 20.0},
            {"symbol": "C", "stop_pct": 8.0}]
    t, s = ob._risk_targets(rows, 100_000.0, 15_000.0, 0.0, 0.5, 35.0)
    assert t == {"A": 5_000.0, "B": 2_500.0, "C": 6_250.0} and s == {"A": 10.0, "B": 20.0, "C": 8.0}
    # the cap binds
    t, _ = ob._risk_targets(rows, 100_000.0, 4_000.0, 0.0, 0.5, 35.0)
    assert t == {"A": 4_000.0, "B": 2_500.0, "C": 4_000.0}
    # the floor releases B only; the others are untouched (no re-spread)
    t, _ = ob._risk_targets(rows, 100_000.0, 15_000.0, 3_000.0, 0.5, 35.0)
    assert t == {"A": 5_000.0, "C": 6_250.0}
    # a missing / junk / non-positive stop counts as the fallback (the widest stop)
    t, s = ob._risk_targets([{"symbol": "X"}, {"symbol": "Y", "stop_pct": "n/a"},
                             {"symbol": "Z", "stop_pct": -3}], 100_000.0, 15_000.0, 0.0, 0.5, 35.0)
    assert s == {"X": 35.0, "Y": 35.0, "Z": 35.0}
    assert all(abs(v - 1_428.57) < 0.01 for v in t.values())
    t, s = ob._risk_targets([{"symbol": "X"}], 100_000.0, 15_000.0, 0.0, 0.5, "bad-fallback")
    assert t == {} and s == {}
    t, s = ob._risk_targets([{"symbol": "X"}], 100_000.0, 15_000.0, 0.0, 0.5, 0)
    assert s == {"X": 35.0}
    # bad inputs size nothing
    assert ob._risk_targets(rows, 0.0, 15_000.0, 0.0, 0.5, 35.0) == ({}, {})
    assert ob._risk_targets(rows, 100_000.0, 0.0, 0.0, 0.5, 35.0) == ({}, {})
    assert ob._risk_targets(rows, 100_000.0, 15_000.0, 0.0, 0.0, 35.0) == ({}, {})
    assert ob._risk_targets(rows, "bad", 15_000.0, 0.0, 0.5, 35.0) == ({}, {})
    assert ob._risk_targets(None, 100_000.0, 15_000.0, 0.0, 0.5, 35.0) == ({}, {})
    assert ob._risk_targets([{"symbol": ""}, None], 100_000.0, 15_000.0, 0.0, 0.5, 35.0) == ({}, {})


def test_r5_risk_budget_env_parsing(monkeypatch):
    for raw, want in (("", 0.5), ("0.5", 0.5), ("1", 1.0), ("2", 2.0), ("0.1", 0.1),
                      ("0.05", 0.1), ("5", 2.0), ("-1", 0.1), ("abc", 0.5), ("nan", 0.5),
                      ("inf", 0.5), (" 0.75 ", 0.75)):
        monkeypatch.setenv("TFB_OPP_RISK_BUDGET_PCT", raw)
        assert ob._env_risk_budget_pct() == want, raw
    monkeypatch.delenv("TFB_OPP_RISK_BUDGET_PCT", raising=False)
    assert ob._env_risk_budget_pct() == 0.5
    for raw, want in (("risk", "risk"), ("RISK", "risk"), (" Risk ", "risk"),
                      ("conviction", "conviction"), ("cap", "cap"), ("", "cap"), ("x", "cap")):
        monkeypatch.setenv("TFB_OPP_SIZING_MODE", raw)
        assert ob._env_sizing_mode() == want, raw


def test_r6_no_topup_cash_stays_in_cash(monkeypatch):
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "risk")
    monkeypatch.setenv("TFB_OPP_RISK_BUDGET_PCT", "2")
    rows = [_row("ALPHA.SR", 60.0, 4.0, sector="Energy", price=99.7),
            _row("BRAVO.SR", 50.0, 8.0, rel=80.0, sector="Financials", price=99.1),
            _row("CHARLIE.SR", 20.0, 2.0, rel=72.0, sector="Utilities", price=98.3)]
    payload = _build(rows)
    sizes = _sizes(payload)
    targets = payload["meta"]["sizing"]["targets"]
    assert set(sizes) == set(targets) == set(STOPS)
    for sym in sizes:                       # never above the target; lot-rounded below it
        assert sizes[sym] <= targets[sym] + 1e-6 and targets[sym] - sizes[sym] < 100.0
    assert sum(sizes.values()) <= 16_500.0 + 1e-6          # 6,000 + 3,000 + 7,500
    assert CASH - sum(sizes.values()) > 13_000.0           # the rest is cash, by design
    assert "topup_sar" not in payload["meta"]["sizing"]


def test_r7_research_never_sized_and_replay_sizes_eligible_seats(monkeypatch):
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "risk")
    monkeypatch.setenv("TFB_OPP_RISK_BUDGET_PCT", "2")
    research = _build(board_funding_stage="research")
    assert research["status"] == "ok", research.get("message")
    assert research["meta"]["sizing"] == {"mode": "risk"}
    assert all(float(t["suggested_sar"]) == 0.0 for t in research["selected"])
    frozen = copy.deepcopy(research["meta"]["board_funding"]["snapshot"])
    allocated = _build(frozen["rows"], board_funding_stage="allocate",
                       board_funding_symbols=["ALPHA.SR", "CHARLIE.SR"],
                       board_funding_snapshot=frozen)
    assert allocated["status"] == "ok", allocated.get("message")
    assert _sizes(allocated) == {"ALPHA.SR": 6_000.0, "CHARLIE.SR": 7_500.0}
    meta = allocated["meta"]["sizing"]
    assert meta["cohort"] == ["ALPHA.SR", "CHARLIE.SR"] and "BRAVO.SR" not in meta["targets"]
    assert meta["released"] == []


def test_r8_enforced_absolute_cash_floor_is_kept(monkeypatch):
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "risk")
    monkeypatch.setenv("TFB_OPP_RISK_BUDGET_PCT", "2")
    monkeypatch.setenv("TFB_OPP_CASH_FLOOR_SAR", "20000")
    monkeypatch.setenv("TFB_OPP_CASH_FLOOR_MODE", "enforce")
    payload = _build()
    sizes = _sizes(payload)
    assert sum(sizes.values()) <= CASH - 20_000.0 + 1e-6
    assert payload["meta"]["sizing"]["budget_sar"] == 10_000.0
    # the risk basis is NAV + deployable, unchanged by the reserve; the budget is what binds
    assert payload["meta"]["sizing"]["basis_sar"] == CASH
    assert sizes["ALPHA.SR"] == 6_000.0 and sizes["BRAVO.SR"] == 3_000.0
    assert sizes.get("CHARLIE.SR", 0.0) <= 1_000.0 + 1e-6 or "CHARLIE.SR" not in sizes


def test_r8b_worst_entry_sizing_keeps_the_loss_within_the_budget(monkeypatch):
    # review on #936 (P2): with TFB_OPP_SIZE_AT_ENTRY_HIGH the ticket is booked at
    # price x 1.01 while the stop sits at price x (1 - stop); the distance must be
    # measured from the booked entry or the worst fill risks more than the budget
    monkeypatch.setenv("TFB_OPP_SIZING_MODE", "risk")
    monkeypatch.setenv("TFB_OPP_RISK_BUDGET_PCT", "2")
    monkeypatch.setenv("TFB_OPP_SIZE_AT_ENTRY_HIGH", "1")
    payload = _build()
    assert payload["status"] == "ok", payload.get("message")
    meta = payload["meta"]["sizing"]
    assert meta["entry_mult"] == 1.01 and meta["risk_sar"] == 600.0
    for t in payload["selected"]:
        sp = STOPS[t["symbol"]]
        shares = float(t["suggested_shares"])
        worst_fill, stop = 100.0 * 1.01, 100.0 * (1.0 - sp / 100.0)
        assert shares > 0
        assert shares * (worst_fill - stop) <= 600.0 + 1e-6, (t["symbol"], shares * (worst_fill - stop))
        assert float(t["suggested_sar"]) == pytest.approx(shares * worst_fill)
    # CHARLIE (stop 8 %): 600 / (1 - 0.92 / 1.01) = 6,733 -> 66 shares at 101 risk 594, not 74 shares risking 666
    charlie = [t for t in payload["selected"] if t["symbol"] == "CHARLIE.SR"][0]
    assert float(charlie["suggested_shares"]) == 66
    assert meta["targets"]["CHARLIE.SR"] == 6_733.0
    # the pure rule: entry_mult 1.0 is the plain stop distance; 1.01 shortens it; junk -> 1.0
    rows = [{"symbol": "C", "stop_pct": 8.0}]
    assert ob._risk_targets(rows, 30_000.0, 30_000.0, 0.0, 2.0, 35.0)[0] == {"C": 7_500.0}
    assert ob._risk_targets(rows, 30_000.0, 30_000.0, 0.0, 2.0, 35.0, 1.01)[0] == {"C": 6_733.33}
    assert ob._risk_targets(rows, 30_000.0, 30_000.0, 0.0, 2.0, 35.0, 0.5)[0] == {"C": 7_500.0}
    assert ob._risk_targets(rows, 30_000.0, 30_000.0, 0.0, 2.0, 35.0, "x") == ({}, {})
    monkeypatch.delenv("TFB_OPP_SIZE_AT_ENTRY_HIGH", raising=False)
    assert _build()["meta"]["sizing"]["entry_mult"] == 1.0


def test_r9_worked_example_from_the_why_block():
    rows = [{"symbol": "S8", "stop_pct": 8.0}, {"symbol": "S10", "stop_pct": 10.0},
            {"symbol": "S20", "stop_pct": 20.0}, {"symbol": "S35", "stop_pct": 35.0}]
    t, _ = ob._risk_targets(rows, 100_000.0, 15_000.0, 0.0, 0.5, 35.0)
    assert t["S8"] == 6_250.0 and t["S10"] == 5_000.0 and t["S20"] == 2_500.0
    assert abs(t["S35"] - 1_428.57) < 0.01
    # ten seats at 10% stops deploy 50%; a three-stop day costs 1.5%, not 4.7%
    ten = [{"symbol": "N%d" % i, "stop_pct": 10.0} for i in range(10)]
    t, _ = ob._risk_targets(ten, 100_000.0, 15_000.0, 0.0, 0.5, 35.0)
    assert sum(t.values()) == 50_000.0
    assert 3 * 5_000.0 * 0.10 / 100_000.0 == pytest.approx(0.015)


def test_r10_env_reads_are_in_the_fingerprinted_family():
    names = set()
    for node in ast.walk(ast.parse(inspect.getsource(ob))):
        if isinstance(node, ast.Call) and node.args:
            fn = (node.func.attr if isinstance(node.func, ast.Attribute)
                  else node.func.id if isinstance(node.func, ast.Name) else "")
            if (fn == "getenv" or fn.startswith("_env_")) and isinstance(node.args[0], ast.Constant):
                names.add(str(node.args[0].value))
    assert {"TFB_OPP_SIZING_MODE", "TFB_OPP_RISK_BUDGET_PCT"} <= names
    assert all(n.startswith(ob._BOARD_FUNDING_POLICY_PREFIXES)
               for n in ("TFB_OPP_SIZING_MODE", "TFB_OPP_RISK_BUDGET_PCT"))
    assert tuple(map(int, ob.OPPORTUNITY_BUILDER_VERSION.split("."))) >= (1, 29, 0)


if __name__ == "__main__":   # pragma: no cover
    sys.exit(pytest.main([__file__, "-q"]))
