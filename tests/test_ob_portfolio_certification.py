"""Actual allocation with custody/cash/valuation uncertainty never funds."""
import copy
from pathlib import Path
import sys

import pytest

from core.analysis import opportunity_builder as ob

sys.path.insert(0, str(Path(__file__).resolve().parent))
from decision_evidence_fixtures import observed_portfolio, observed_price_fields

FX = {"SAR": 1.0, "USD": 3.75}


def stock():
    return {**observed_price_fields(), "symbol": "NEW.US", "name": "Synthetic Energy",
            "currency": "USD", "sector": "Energy", "market": "US",
            "current_price": 100, "intrinsic_value": 125,
            "expected_roi_12m": .25, "forecast_reliability_score": 85,
            "data_quality_score": 95, "risk_bucket": "Low", "volatility_30d": 4,
            "avg_volume_30d": 2_500_000, "recommendation": "BUY", "investability_status": "INVESTABLE"}


def portfolio():
    return observed_portfolio({"cash_available_sar": 50_000,
        "holdings": [{"symbol": "HELD.US", "sector": "Unknown", "currency": "USD",
                      "quantity": 10, "current_price": 100, "value_sar": 3750}]}, FX)


def build(pf, **criteria):
    return ob.build_opportunity_payload([stock()],
        criteria={"trust_gate_enabled": False, "max_weight_pct": 100, "pf_max_sector_pct": 100, **criteria},
        portfolio=pf, fx_rates=FX)


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    monkeypatch.setenv("APP_TOKEN", "synthetic-certification-test")
    monkeypatch.setenv("TFB_OPP_ENABLED", "1")
    monkeypatch.setenv("TFB_T10_PRICE_XCHECK", "off")
    monkeypatch.setenv("TFB_TICKET_FRESHNESS_GATE", "1")


def assert_withheld(payload):
    assert payload["status"] == "ok"  # render an empty allocation, preserve no old money
    assert payload["selected"] == []
    assert not payload["meta"]["execution_ready"]
    assert not payload["meta"]["input_certification"]["funding_eligible"]
    assert payload["kpis"]["deployable_sar"] == 0
    assert payload["kpis"]["expected_gain_12m_sar"] == 0
    assert not any(a["type"] in {"capital_call", "rotation_proposal"} for a in payload["alerts"])
    assert payload["candidates_rows"]  # still reviewable research, not funded tickets


def test_complete_fresh_snapshot_can_allocate_without_leaking_account_data():
    pf = portfolio()
    payload = build(pf)
    assert payload["selected"] and payload["meta"]["execution_ready"]
    assert "synthetic-account" not in str(payload)
    assert "reconciliation_evidence" not in str(payload)


def test_full_sale_blocks_stale_holding_and_money():
    pf = portfolio()
    pf["reconciliation_evidence"]["accounts"][0]["positions"][0]["quantity"] = 0
    before = copy.deepcopy(pf)
    payload = build(pf)
    assert_withheld(payload)
    assert payload["meta"]["input_certification"]["reason_counts"]["position_closed"] == 1
    assert pf == before


def test_cash_rounding_tolerance_cannot_increase_settled_funding_cap():
    pf = portfolio()
    pf["cash_available_sar"] += .01
    payload = build(pf)
    assert payload["meta"]["execution_ready"]
    assert payload["kpis"]["deployable_current_sar"] == 50_000
    assert sum(item["suggested_sar"] for item in payload["selected"]) <= 50_000


@pytest.mark.parametrize("case", ["absent", "stale", "cash_mismatch", "reserved",
                                  "holding_quote", "holding_value", "nav", "proceeds",
                                  "incomplete", "completeness_unknown", "other_custody", "fx"])
def test_uncertain_inputs_cannot_solicit_or_reserve_money(case):
    pf = portfolio()
    evidence = pf["reconciliation_evidence"]
    if case == "absent":
        del pf["reconciliation_evidence"]
    elif case == "stale":
        evidence["captured_at"] = "2001-01-01T00:00:00Z"
    elif case == "cash_mismatch":
        pf["cash_available_sar"] += 1
    elif case == "reserved":
        evidence["accounts"][0]["cash"][0]["reservations_complete"] = False
    elif case == "holding_quote":
        pf["holdings"][0]["warnings"] = "kept_last_good"
    elif case == "holding_value":
        pf["holdings"][0]["value_sar"] += 1
    elif case == "nav":
        pf["portfolio_value_sar"] = 10_000
    elif case == "proceeds":
        pf["pending_proceeds_sar"] = 5000
    elif case == "incomplete":
        pf["holdings_input_incomplete"] = True
    elif case == "completeness_unknown":
        del pf["holdings_input_incomplete"]
    elif case == "other_custody":
        evidence["holding_links"] = []
    elif case == "fx":
        evidence["fx_rates"] = []
    assert_withheld(build(pf))


def test_research_does_not_require_money_or_claim_execution_readiness():
    payload = build({}, board_funding_stage="research")
    assert payload["selected"]
    assert not payload["meta"]["execution_ready"]
    assert not payload["meta"]["input_certification"]["funding_eligible"]
    assert payload["kpis"]["expected_gain_12m_sar"] == 0


def test_signed_replay_rechecks_expired_cash_capture_even_if_signature_valid():
    pf = portfolio()
    pf["reconciliation_evidence"]["captured_at"] = "2001-01-01T00:00:00Z"
    research = build(pf, board_funding_stage="research")
    snapshot = research["meta"]["board_funding"]["snapshot"]
    payload = ob.build_opportunity_payload(snapshot["rows"], criteria={
        **research["meta"]["criteria_snapshot"], "board_funding_stage": "allocate",
        "board_funding_symbols": ["NEW.US"], "board_funding_snapshot": snapshot},
        portfolio=pf, fx_rates=FX)
    assert_withheld(payload)
