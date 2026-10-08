#!/usr/bin/env python3
"""P-153 fixed-income ladder regression using real code and synthetic evidence.

The compliance classifier is unchanged. Invented holdings and broker captures
exercise the default, legacy display switch and master protection switch;
private sheet exports are never certified by a synthetic account declaration.
"""
from __future__ import annotations

import copy

import pytest

from core.analysis import portfolio_actions as pa
from tests.portfolio_reconciliation_fixtures import build_certified_portfolio_actions, synthetic_quote_receipt

PANEL = {"cash_available_sar": 50000, "target_cash_pct": 10,
         "max_position_pct": 20, "max_sector_pct": 30,
         "min_reliability_add": 70, "min_dq_add": 80,
         "rebalance_mode": "Advisory"}
FX = {"USD": 4, "SAR": 1}
BOND = "SYNTHSUKUK.SR"


@pytest.fixture(autouse=True)
def isolated_policy(monkeypatch):
    for key in ("TFB_PA_SUKUK_LADDER_LEGACY", "TFB_PA_PROTECT_SUKUK", "TFB_FORECAST_BASIS",
                "TFB_PF_CONFIRM_SESSION", "TFB_PF_DD_EXIT"):
        monkeypatch.delenv(key, raising=False)
    for key, value in {"TFB_PF_ENABLED": "1", "TFB_EXIT_BY_RULE_GATE": "0",
                       "TFB_PF_CONFIRM_PERSIST": "0", "TFB_PF_ADD_CONFIRM_DAYS": "2",
                       "TFB_PF_ENGINE_ROI_DISPLAY": "1"}.items():
        monkeypatch.setenv(key, value)
    saved = copy.deepcopy(pa._ADD_CONFIRM_STORE)
    pa._ADD_CONFIRM_STORE.clear()
    yield
    pa._ADD_CONFIRM_STORE.clear()
    pa._ADD_CONFIRM_STORE.update(saved)


def holding(symbol, *, name, quantity, avg_cost, price, currency="USD", roi=25,
            stop=None, tp1=None, tp2=None, reliability=85, quality=100):
    row = {**synthetic_quote_receipt(),
           "Symbol": symbol, "Name": name, "Sector": "Industrials", "Currency": currency,
           "Market": "Synthetic Market", "Quantity": quantity, "Avg Cost": avg_cost,
           "Current Price": price, "Expected ROI 12M": roi,
           "Forecast Reliability Score": reliability, "Data Quality Score": quality,
           "Recommendation": "HOLD", "Investability Status": "INVESTABLE",
           "Final Action": "HOLD", "Target Price": price * (1 + roi / 100)}
    for key, value in (("Stop Loss", stop), ("Take Profit 1", tp1), ("Take Profit 2", tp2)):
        if value is not None:
            row[key] = value
    return row


@pytest.fixture
def rows():
    return [holding("SYNTHSTOCK.US", name="Synthetic Equity", quantity=12,
                    avg_cost=30, price=32, stop=28, tp1=40, tp2=45),
            holding("NOLAD.US", name="Synthetic No Ladder Equity", quantity=8,
                    avg_cost=20, price=21, roi=-3),
            holding(BOND, name="Synthetic Sukuk", quantity=20, avg_cost=50, price=52,
                    currency="SAR", stop=48, tp1=55, tp2=60,
                    reliability=26, quality=70, roi=4.9)]


def build(rows, monkeypatch, *, legacy=None, protect=None, basis="observe"):
    for key, value in {"TFB_PA_SUKUK_LADDER_LEGACY": legacy,
                       "TFB_PA_PROTECT_SUKUK": protect, "TFB_FORECAST_BASIS": basis}.items():
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, str(value))
    pa._ADD_CONFIRM_STORE.clear()
    result = build_certified_portfolio_actions(pa, copy.deepcopy(rows), PANEL, FX)
    assert result["status"] == "ok" and result["meta"]["input_certification"]["funding_eligible"]
    result["meta"].pop("generated_utc", None)
    return result


def actions(payload):
    return {row["symbol"]: row for row in payload["actions"]}


def differences(left, right, path=""):
    if isinstance(left, dict) and isinstance(right, dict):
        return set().union(*(differences(left.get(key), right.get(key), path + "/" + key)
                             for key in set(left) | set(right)))
    if isinstance(left, list) and isinstance(right, list) and len(left) == len(right):
        return set().union(*(differences(a, b, path + "[%d]" % index)
                             for index, (a, b) in enumerate(zip(left, right))))
    return {path} if left != right else set()


@pytest.mark.parametrize("value,expected", [(None, False), ("0", False), ("1", True),
                                            ("true", True), ("on", True), ("yes", True),
                                            ("off", False), (" TRUE ", True)])
def test_legacy_display_switch_reader(monkeypatch, value, expected):
    if value is not None:
        monkeypatch.setenv("TFB_PA_SUKUK_LADDER_LEGACY", value)
    assert pa._env_sukuk_ladder_legacy() is expected
    assert tuple(map(int, pa.PORTFOLIO_ACTIONS_VERSION.split("."))) >= (1, 15, 0)


def test_real_classifier_and_protection_switches(monkeypatch):
    synthetic_sukuk = {"symbol": BOND, "name": "Synthetic Sukuk"}
    assert pa._sukuk_display_active(synthetic_sukuk) is True
    assert pa._sukuk_display_active({"symbol": "5023.SR", "name": ""}) is True
    assert pa._sukuk_display_active({"symbol": "SYNTHSTOCK.US", "name": "Synthetic Equity"}) is False
    assert pa._sukuk_display_active(None) is False
    assert pa._sukuk_display_active({"symbol": None}) is False
    monkeypatch.setenv("TFB_PA_SUKUK_LADDER_LEGACY", "1")
    assert pa._sukuk_display_active(synthetic_sukuk) is False
    monkeypatch.delenv("TFB_PA_SUKUK_LADDER_LEGACY")
    monkeypatch.setenv("TFB_PA_PROTECT_SUKUK", "0")
    assert pa._sukuk_display_active(synthetic_sukuk) is False
    assert "D-9" in pa.SUKUK_LADDER_NOTE and "maturity" in pa.SUKUK_LADDER_NOTE


def test_default_and_legacy_differ_only_in_sukuk_display_fields(rows, monkeypatch):
    default = build(rows, monkeypatch)
    legacy = build(rows, monkeypatch, legacy="1")
    protect_off = build(rows, monkeypatch, protect="0")
    normal, old = actions(default), actions(legacy)
    bond = normal[BOND]
    assert bond["stop_sar"] is bond["tp1_sar"] is bond["tp2_sar"] is None
    assert bond["detail"]["ladder_display"] == "sukuk_na"
    assert pa.SUKUK_LADDER_NOTE in bond["advisor_note"] and " / TP1 " not in bond["advisor_note"]
    assert "stop " not in bond["advisor_note"].split("sukuk / fixed income")[0]
    assert "engine 12M forecast" in bond["advisor_note"]
    assert "[f1-observe] n/a - sukuk / fixed income (D-9)" in bond["action_reason"]
    assert "plan 3M ROI" not in bond["action_reason"] and bond["action_reason"].startswith("Upside")
    assert all(old[BOND][key] is not None for key in ("stop_sar", "tp1_sar", "tp2_sar"))
    assert "plan 3M ROI" in old[BOND]["action_reason"] and " / TP1 " in old[BOND]["advisor_note"]
    for key in ("roi_pct", "engine_roi_pct", "valuation_roi_pct"):
        assert bond[key] == old[BOND][key]
    for symbol in normal.keys() - {BOND}:
        assert normal[symbol] == old[symbol]
        assert normal[symbol]["stop_sar"]
        assert "[f1-observe] plan 3M ROI" in normal[symbol]["action_reason"]
    index = default["actions"].index(bond)
    assert differences(default, legacy) == {
        "/actions[%d]/%s" % (index, field) for field in (
            "stop_sar", "tp1_sar", "tp2_sar", "action_reason", "advisor_note", "detail/ladder_display")}
    assert protect_off == legacy
    assert build(rows, monkeypatch) == default


@pytest.mark.parametrize("basis", ["legacy", "plan3m"])
def test_forecast_modes_preserve_verdict_and_only_change_ladder_display(rows, monkeypatch, basis):
    default = build(rows, monkeypatch, basis=basis)
    legacy = build(rows, monkeypatch, legacy="1", basis=basis)
    normal, old = actions(default)[BOND], actions(legacy)[BOND]
    assert "[f1-observe]" not in normal["action_reason"] and "[f1-observe]" not in old["action_reason"]
    assert normal["action_reason"] == old["action_reason"] and normal["action"] == old["action"]
    assert normal["stop_sar"] is None and old["stop_sar"] is not None
    index = default["actions"].index(normal)
    assert differences(default, legacy) == {
        "/actions[%d]/%s" % (index, field) for field in (
            "stop_sar", "tp1_sar", "tp2_sar", "advisor_note", "detail/ladder_display")}


def test_equity_with_no_target_keeps_the_no_ladder_line(rows, monkeypatch):
    default, legacy = build(rows, monkeypatch), build(rows, monkeypatch, legacy="1")
    row = actions(default)["NOLAD.US"]
    assert row["stop_sar"] and row["tp1_sar"] is None and "no TP ladder" in row["advisor_note"]
    assert row == actions(legacy)["NOLAD.US"]


def test_blocked_sukuk_has_no_advisor_ladder_and_hold_uses_the_switch(monkeypatch):
    entry = {"cand": {"symbol": BOND, "name": "Synthetic Sukuk", "fx_to_sar": 1,
                      "stop": 48, "tp1": 55, "tp2": 60},
             "action": pa.ACTION_BLOCK, "action_reason": "cost basis rejected",
             "confidence_band": "Low", "proceeds_sar": 0}
    controls = pa.make_controls(PANEL)
    blocked = pa._advisor_sentence(entry, controls, "2026-10-30")
    assert "BLOCKED" in blocked and pa.SUKUK_LADDER_NOTE not in blocked and "TP1" not in blocked
    entry["action"] = pa.ACTION_HOLD
    held = pa._advisor_sentence(entry, controls, "2026-10-30")
    monkeypatch.setenv("TFB_PA_SUKUK_LADDER_LEGACY", "1")
    old = pa._advisor_sentence(entry, controls, "2026-10-30")
    assert pa.SUKUK_LADDER_NOTE in held and "TP1" not in held
    assert "stop 48 / TP1 55 / TP2 60 SAR" in old
