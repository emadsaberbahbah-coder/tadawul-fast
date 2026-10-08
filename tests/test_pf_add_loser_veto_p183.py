#!/usr/bin/env python3
"""P-183 loser and stop veto through the real certified portfolio builder.

All holdings, broker account positions, settled funds and FX are synthetic.
The delivered module supplies the off/observe/enforce comparison; historical
sheet exports are not treated as fresh broker evidence.
"""
from __future__ import annotations

import copy
import json

import pytest

from core.analysis import portfolio_actions as pa
from tests.portfolio_reconciliation_fixtures import build_certified_portfolio_actions, synthetic_quote_receipt

PANEL = {"cash_available_sar": 50000, "target_cash_pct": 10,
         "max_position_pct": 20, "max_sector_pct": 30,
         "min_reliability_add": 70, "min_dq_add": 80,
         "rebalance_mode": "Advisory", "add_confirm_days": 1}
FX = {"USD": 4, "SAR": 1}


@pytest.fixture(autouse=True)
def isolated_policy(monkeypatch):
    for key in ("TFB_PF_ADD_LOSER_VETO", "TFB_PF_ADD_LOSER_PCT", "TFB_PF_ADD_STOP_PROX_PCT",
                "TFB_FORECAST_BASIS", "TFB_PF_CONFIRM_SESSION", "TFB_PF_DD_EXIT"):
        monkeypatch.delenv(key, raising=False)
    for key, value in {"TFB_PF_ENABLED": "1", "TFB_EXIT_BY_RULE_GATE": "0",
                       "TFB_PF_CONFIRM_PERSIST": "0", "TFB_PF_ADD_CONFIRM_DAYS": "1",
                       "TFB_PF_ENGINE_ROI_DISPLAY": "1", "TFB_FORECAST_BASIS": "observe",
                       "TFB_PF_CONFIRM_SESSION": "off"}.items():
        monkeypatch.setenv(key, value)
    saved = copy.deepcopy(pa._ADD_CONFIRM_STORE)
    pa._ADD_CONFIRM_STORE.clear()
    yield
    pa._ADD_CONFIRM_STORE.clear()
    pa._ADD_CONFIRM_STORE.update(saved)


def holding(symbol, *, price, quantity, avg_cost, sector, sukuk=False):
    return {**synthetic_quote_receipt(),
            "Symbol": symbol, "Name": "Synthetic Sukuk" if sukuk else "Synthetic Equity",
            "Currency": "USD", "Exchange": "NYSE/NASDAQ", "Sector": sector,
            "Asset Class": "Fixed Income / Sukuk" if sukuk else "Equity",
            "Position Qty": quantity, "Avg Cost": avg_cost, "Current Price": price,
            "Target Price": price * 2, "Expected ROI 12M": 20,
            "Forecast Reliability Score": 85, "Data Quality Score": 100,
            "Risk Bucket": "Low", "Investability Status": "INVESTABLE",
            "Recommendation": "BUY", "Volatility 30D": 0.2,
            "Forecast Source": "provider_target", "Buy Date": "2026-01-01"}


@pytest.fixture
def rows():
    return [holding("LOSER.US", price=40, quantity=20, avg_cost=42, sector="Energy"),
            holding("CLEAN.US", price=50, quantity=15, avg_cost=45, sector="Technology")]


def build(rows, monkeypatch, mode=None, *, loser=None, proximity=None):
    for key, value in {"TFB_PF_ADD_LOSER_VETO": mode, "TFB_PF_ADD_LOSER_PCT": loser,
                       "TFB_PF_ADD_STOP_PROX_PCT": proximity}.items():
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


def alerts(payload):
    return {alert["type"]: alert["count"] for alert in payload["alerts"]}


@pytest.mark.parametrize("value,expected", [(None, "off"), ("Enforce", "enforce"),
                                            ("OBSERVE", "observe"), ("junk", "off")])
def test_mode_reader(monkeypatch, value, expected):
    if value is not None:
        monkeypatch.setenv("TFB_PF_ADD_LOSER_VETO", value)
    assert pa._env_add_loser_veto_mode() == expected
    assert tuple(map(int, pa.PORTFOLIO_ACTIONS_VERSION.split("."))) >= (1, 15, 0)


def test_threshold_reader(monkeypatch):
    assert (pa._env_add_loser_pct(), pa._env_add_stop_prox_pct()) == (2, 2)
    monkeypatch.setenv("TFB_PF_ADD_LOSER_PCT", "-3.5")
    monkeypatch.setenv("TFB_PF_ADD_STOP_PROX_PCT", "1")
    assert (pa._env_add_loser_pct(), pa._env_add_stop_prox_pct()) == (3.5, 1)


@pytest.mark.parametrize("candidate,expected", [
    ({"pnl_sar": -40, "cost_sar": 1000, "price": 48, "stop": 44}, (True, False, False)),
    ({"pnl_sar": 100, "cost_sar": 1000, "price": 55, "stop": 50}, (False, False, False)),
    ({"pnl_sar": 10, "cost_sar": 1000, "price": 60, "stop": 59.2}, (False, True, False)),
    ({"pnl_sar": 10, "cost_sar": 1000, "price": 59, "stop": 59.2}, (False, False, True)),
    ({"pnl_sar": None, "cost_sar": None, "price": None, "stop": None}, (False, False, False)),
])
def test_loser_stop_and_missing_basis_helpers(candidate, expected):
    evaluation = pa._add_loser_eval(candidate, 2, 2)
    assert (evaluation["loser"], evaluation["near_stop"], evaluation["below_stop"]) == expected


def test_observe_preserves_verdicts_funding_and_adds_one_tag_per_site(rows, monkeypatch):
    off, observe = build(rows, monkeypatch), build(rows, monkeypatch, "observe")
    off_rows, observe_rows = actions(off), actions(observe)
    assert all(row["action"] == "ADD" for row in off_rows.values())
    assert off["kpis"] == observe["kpis"]
    assert "addveto" not in json.dumps(off).lower() and "add_loser_veto" not in off["meta"]
    for symbol in off_rows:
        row = observe_rows[symbol]
        assert row["action_reason"].count("[addveto-observe]") == 1
        assert row["advisor_note"].count("[addveto-observe]") == 1
        stripped = dict(row)
        for key in ("action_reason", "advisor_note"):
            stripped[key] = off_rows[symbol][key]
        assert stripped == off_rows[symbol]
    assert "would HOLD under enforce" in observe_rows["LOSER.US"]["action_reason"]
    assert "[addveto-observe] ok" in observe_rows["CLEAN.US"]["action_reason"]
    assert alerts(observe)["add_loser_observe"] == 1
    assert "add_loser_veto" not in alerts(observe)
    assert observe["meta"]["add_loser_veto"] == {
        "mode": "observe", "loser_pct": 2.0, "stop_prox_pct": 2.0, "vetoed": 0, "would_veto": 1}


def test_enforce_vetoes_only_loser_and_preserves_other_add_funding(rows, monkeypatch):
    off, enforce = build(rows, monkeypatch), build(rows, monkeypatch, "enforce")
    old, new = actions(off), actions(enforce)
    loser = new["LOSER.US"]
    assert loser["action"] == "HOLD" and loser["detail"]["capped_from"] == "ADD"
    assert loser["suggested_delta_sar"] in (None, 0) and loser["suggested_delta_shares"] in (None, 0)
    assert loser["funds_from"] is None and loser["action_reason"].startswith("ADD vetoed [P-183]")
    assert "ADD vetoed [P-183]" in loser["advisor_note"]
    assert new["CLEAN.US"] == old["CLEAN.US"]
    assert off["kpis"]["adds_funded_sar"] - enforce["kpis"]["adds_funded_sar"] == old["LOSER.US"]["suggested_delta_sar"]
    assert alerts(enforce)["add_loser_veto"] == 1
    assert alerts(enforce).get("low_confidence_capped") == alerts(off).get("low_confidence_capped")
    assert enforce["meta"]["add_loser_veto"]["vetoed"] == 1
    assert enforce["kpis"]["action_counts"]["ADD"] == off["kpis"]["action_counts"]["ADD"] - 1
    assert enforce["kpis"]["action_counts"]["HOLD"] == off["kpis"]["action_counts"]["HOLD"] + 1
    assert build(rows, monkeypatch) == off
    assert build(rows, monkeypatch, "enforce") == enforce


def test_threshold_and_proximity_controls_apply_to_real_builder(rows, monkeypatch):
    assert actions(build(rows, monkeypatch, "enforce", loser=5))["LOSER.US"]["action"] == "ADD"
    near_rows = [holding("NEAR.US", price=30, quantity=25, avg_cost=29.7, sector="Industrials")]
    off = actions(build(near_rows, monkeypatch))["NEAR.US"]
    stop = off["stop_sar"] / FX["USD"]
    assert stop > 0
    band = (30 / stop - 1) * 100 + 0.5
    near = actions(build(near_rows, monkeypatch, "enforce", proximity=band))["NEAR.US"]
    assert near["action"] == "HOLD" and "above the stop" in near["action_reason"]
    disabled = actions(build(near_rows, monkeypatch, "enforce", proximity=0))["NEAR.US"]
    assert disabled["action"] == "ADD"


def test_sukuk_is_exempt_and_non_add_verdict_is_untouched(monkeypatch):
    fixed_income = [holding("SYNTHBOND.US", price=40, quantity=10, avg_cost=45, sector="Sukuk", sukuk=True)]
    off = actions(build(fixed_income, monkeypatch))["SYNTHBOND.US"]
    enforce = actions(build(fixed_income, monkeypatch, "enforce"))["SYNTHBOND.US"]
    assert off["action"] == "ADD" and enforce == off
    monkeypatch.setenv("TFB_PF_ADD_LOSER_VETO", "enforce")
    assert pa._apply_add_loser_veto({"pnl_sar": -40, "cost_sar": 1000}, pa.ACTION_HOLD, "HOLD", 0, None) == (
        pa.ACTION_HOLD, "HOLD", 0, None)
