"""Real portfolio output keeps unknown quotes and certified quantities honest.

All account/quote packets are synthetic and no provider, broker or Sheet is
contacted. Financial policy functions and the real certifier remain in use.
"""
import socket
from decimal import Decimal

import pytest

from core.analysis import portfolio_actions as pa
from tests.portfolio_reconciliation_fixtures import (
    build_certified_portfolio_actions, synthetic_quote_receipt,
)


@pytest.fixture(autouse=True)
def offline_policy(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("portfolio output regression attempted external I/O")
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setenv("TFB_PF_ENABLED", "1")
    monkeypatch.setenv("TFB_PF_RESEARCH_VIEW", "off")
    monkeypatch.setenv("TFB_EXIT_BY_RULE_GATE", "0")
    monkeypatch.setenv("TFB_PF_ADD_CONFIRM_DAYS", "0")
    monkeypatch.setenv("TFB_PF_CONFIRM_PERSIST", "0")
    monkeypatch.setenv("TFB_PF_TRUST_GATE", "0")
    monkeypatch.setenv("TFB_PF_FEE_FUNDING", "0")


def holding(*, quantity=1, price=10, currency="SAR", **changes):
    row = {**synthetic_quote_receipt(),
           "Symbol": "SYNTHETIC.SR", "Name": "Synthetic holding", "Currency": currency,
           "Quantity": quantity, "Buy Price": 8, "Current Price": price,
           "Intrinsic Value": 8, "Sector": "Synthetic", "Risk Bucket": "Moderate",
           "Forecast Reliability Score": 90, "Data Quality Score": 90,
           "Recommendation": "SELL", "Investability Status": "INVESTABLE"}
    row.update(changes)
    return row


@pytest.mark.parametrize("price", [None, "", "N/A", 0, -1, True])
def test_missing_or_invalid_native_price_never_becomes_known_zero_sar(price):
    result = pa.build_portfolio_actions([holding(price=price)],
                                        {"cash_available_sar": 0}, {"SAR": 1})
    assert result["status"] == "ok" and result["meta"]["execution_ready"] is False
    row = result["actions"][0]
    assert row["action"] == "BLOCK"
    assert row["price"] is None and row["price_sar"] is None
    assert row["market_value_sar"] is None and row["cost_sar"] is None
    assert row["suggested_delta_sar"] == row["suggested_delta_shares"] == 0


@pytest.mark.parametrize("currency,rate,expected", [("SAR", 1, 10), ("USD", 3.75, 37.5)])
def test_known_price_conversion_is_unchanged_on_withheld_research_rows(currency, rate, expected):
    result = pa.build_portfolio_actions([holding(currency=currency)],
                                        {"cash_available_sar": 0}, {currency: rate})
    row = result["actions"][0]
    assert row["price"] == 10 and row["price_sar"] == expected
    assert row["action"] == "BLOCK" and row["detail"]["execution_ready"] is False


def certified(rows):
    return build_certified_portfolio_actions(pa, rows, controls={
        "cash_available_sar": 100, "target_cash_pct": 0,
        "max_position_pct": 100, "max_sector_pct": 100,
        "trust_gate_enabled": False}, fx_rates={"SAR": 1})


@pytest.mark.parametrize("quantity", ["0.4", "0.5", "1.6", "2.4", "2"])
def test_certified_full_exit_represents_exact_custody_without_rounding_or_oversell(quantity):
    result = certified([holding(quantity=quantity)])
    assert result["status"] == "ok" and result["meta"]["execution_ready"] is True
    assert result["meta"]["input_certification"]["funding_eligible"] is True
    row = result["actions"][0]
    assert row["action"] == "EXIT"
    assert Decimal(str(row["suggested_delta_shares"])) == -Decimal(quantity)
    assert row["suggested_delta_shares"] >= -row["quantity"]
    assert row["post_trade_weight_pct"] == 0
    assert row["proceeds_sar"] == float(Decimal(quantity) * 10)
    if quantity == "2":
        assert type(row["suggested_delta_shares"]) is int


@pytest.mark.parametrize("quantity,sold", [("0.4", 0), ("1.6", 1), ("2.4", 2), ("10", 5)])
def test_certified_trim_keeps_whole_share_policy_and_reports_actual_fractional_remainder(quantity, sold):
    result = certified([holding(quantity=quantity, **{"Intrinsic Value": 9.1, "Recommendation": "HOLD"})])
    assert result["meta"]["execution_ready"] is True
    row = result["actions"][0]
    assert row["action"] == "TRIM" and row["suggested_delta_shares"] == -sold
    assert type(row["suggested_delta_shares"]) is int
    q = Decimal(quantity)
    assert sold <= q
    remaining_value = (q - sold) * 10
    expected_weight = round(float(remaining_value / (100 + q * 10) * 100), 1)
    assert row["post_trade_weight_pct"] == expected_weight
    assert row["action_reason"].startswith("Valuation")
    if sold == 0:
        assert "whole-share trim unavailable" in row["advisor_note"]


@pytest.mark.parametrize("quantity", [None, "", 0, -1, True, float("nan"), float("inf")])
@pytest.mark.parametrize("action", [pa.ACTION_EXIT, pa.ACTION_TRIM])
def test_invalid_quantity_never_generates_a_sell_delta_or_post_trade_weight(quantity, action):
    entry = {"cand": {"quantity": quantity, "market_value_sar": 100,
                       "price": 10, "fx_to_sar": 1},
             "action": action, "action_reason": "Synthetic policy result", "proceeds_sar": 50,
             "suggested_delta_shares": None}
    pa._sell_side_deltas([entry], {"max_position_pct": 20}, 100)
    assert entry["suggested_delta_shares"] is None
    assert entry.get("post_trade_weight_pct") is None


def test_true_unit_price_fallback_and_repeated_sizing_are_stable():
    entry = {"cand": {"quantity": 1.6, "market_value_sar": None, "price": 10, "fx_to_sar": 1},
             "action": pa.ACTION_EXIT, "action_reason": "Synthetic full exit", "proceeds_sar": 16,
             "suggested_delta_shares": None}
    pa._sell_side_deltas([entry], {}, 100)
    assert entry["suggested_delta_shares"] == -1.6 and entry["post_trade_weight_pct"] == 0
    first = dict(entry)
    pa._sell_side_deltas([entry], {}, 100)
    assert entry == first


@pytest.mark.parametrize("quantity,requested,credit", [(0.4, 2, 0), (1.6, 16, 10), (2.4, 24, 20),
                                                     (2, 8, 8), (10, 50, 50)])
def test_proceeds_including_helper_never_funds_adds_from_unsized_fractional_trim(quantity, requested, credit):
    trim = {"cand": {"quantity": quantity, "market_value_sar": quantity * 10,
                      "price": 10, "fx_to_sar": 1},
            "action": pa.ACTION_TRIM, "action_reason": "Synthetic trim", "proceeds_sar": requested,
            "suggested_delta_shares": None}
    add = {"cand": {"symbol": "SYNTHETIC_ADD.SR", "sector": "Synthetic", "quantity": 1,
                     "market_value_sar": 1, "price": 1, "fx_to_sar": 1},
           "action": pa.ACTION_ADD, "action_reason": "Synthetic add", "proceeds_sar": 0}
    controls = pa.make_controls({"target_cash_pct": 0, "rebalance_mode": "Advisory Only",
                                 "max_position_pct": 100, "max_sector_pct": 100})
    pa._sell_side_deltas([trim, add], controls, 100)
    deployable, included, funded, _floor = pa.fund_adds([trim, add], controls, 0, 100)
    assert trim["proceeds_sar"] == credit and credit <= requested
    assert included == deployable == credit and funded <= credit
    assert abs(trim["suggested_delta_shares"]) <= quantity
    if not credit:
        assert add["suggested_delta_shares"] == 0 and add["funds_from"] is None


def test_public_certified_advisory_request_cannot_fund_add_from_zero_share_trim():
    rows = [holding(quantity="0.4", **{"Intrinsic Value": 9.1, "Recommendation": "HOLD"}),
            holding(quantity=1, **{"Symbol": "SYNTHADD.SR", "Intrinsic Value": 13,
                                   "Sector": "Other synthetic", "Recommendation": "BUY"})]
    result = build_certified_portfolio_actions(pa, rows, controls={
        "cash_available_sar": 0, "target_cash_pct": 0, "rebalance_mode": "Advisory Only",
        "max_position_pct": 100, "max_sector_pct": 100, "trust_gate_enabled": False},
        fx_rates={"SAR": 1})
    assert result["meta"]["execution_ready"] is True
    trim = next(row for row in result["actions"] if row["action"] == "TRIM")
    add = next(row for row in result["actions"] if row["action"] == "ADD")
    assert trim["suggested_delta_shares"] == trim["proceeds_sar"] == 0
    assert add["suggested_delta_shares"] == 0 and add["funds_from"] is None
    assert result["meta"]["controls_snapshot"]["rebalance_mode"] == pa.REBALANCE_NEW_CASH
    assert result["kpis"]["proceeds_pending_sar"] == result["kpis"]["adds_funded_sar"] == 0


@pytest.mark.parametrize("fee,shares,principal,remaining", [("9", 2, 20, 0),
                                                         ("0", 2, 20, 9),
                                                         ("invalid", 2, 20, 0),
                                                         ("30", 0, 0, 29)])
def test_existing_fee_policy_reports_actual_remaining_capital_without_changing_principal(
    monkeypatch, fee, shares, principal, remaining,
):
    monkeypatch.setenv("TFB_PF_FEE_FUNDING", "1")
    monkeypatch.setenv("TFB_PF_FEE_SAR", fee)
    result = build_certified_portfolio_actions(pa, [holding(**{
        "Intrinsic Value": 13, "Recommendation": "BUY"})], controls={
            "cash_available_sar": 29, "target_cash_pct": 0,
            "max_position_pct": 100, "max_sector_pct": 100, "trust_gate_enabled": False},
        fx_rates={"SAR": 1})
    assert result["meta"]["execution_ready"] is True
    row = result["actions"][0]
    assert row["action"] == "ADD" and row["suggested_delta_shares"] == shares
    assert row["suggested_delta_sar"] == principal
    assert result["kpis"]["adds_funded_sar"] == principal
    assert result["kpis"]["capital_unallocated_sar"] == remaining
    assert "_funding_remaining_sar" not in str(result)


def test_fee_off_remaining_capital_and_ticket_stay_unchanged(monkeypatch):
    row = holding(**{"Intrinsic Value": 13, "Recommendation": "BUY"})
    controls = {"cash_available_sar": 29, "target_cash_pct": 0,
                "max_position_pct": 100, "max_sector_pct": 100, "trust_gate_enabled": False}
    monkeypatch.delenv("TFB_PF_FEE_FUNDING", raising=False)
    absent = build_certified_portfolio_actions(pa, [row], controls, {"SAR": 1})
    monkeypatch.setenv("TFB_PF_FEE_FUNDING", "0")
    explicit_off = build_certified_portfolio_actions(pa, [row], controls, {"SAR": 1})
    assert absent["actions"] == explicit_off["actions"]
    assert absent["kpis"] == explicit_off["kpis"]
    assert absent["actions"][0]["suggested_delta_shares"] == 2
    assert absent["kpis"]["adds_funded_sar"] == 20
    assert absent["kpis"]["capital_unallocated_sar"] == 9


def test_certified_fee_ledger_survives_action_sorting_and_source_order(monkeypatch):
    monkeypatch.setenv("TFB_PF_FEE_FUNDING", "1")
    monkeypatch.setenv("TFB_PF_FEE_SAR", "9")
    held = holding(**{"Intrinsic Value": 10, "Recommendation": "HOLD"})
    add = holding(**{"Symbol": "SYNTHADD.SR", "Intrinsic Value": 13,
                      "Sector": "Other synthetic", "Recommendation": "BUY"})
    controls = {"cash_available_sar": 29, "target_cash_pct": 0,
                "max_position_pct": 100, "max_sector_pct": 100,
                "trust_gate_enabled": False}
    results = []
    for source_rows in ([held, add], [add, held]):
        result = build_certified_portfolio_actions(pa, source_rows, controls, {"SAR": 1})
        assert result["meta"]["execution_ready"] is True
        assert [row["action"] for row in result["actions"]] == ["ADD", "HOLD"]
        assert [row["symbol"] for row in result["actions"]] == ["SYNTHADD.SR", "SYNTHETIC.SR"]
        assert result["actions"][0]["suggested_delta_shares"] == 2
        assert result["actions"][0]["suggested_delta_sar"] == 20
        assert result["kpis"]["adds_funded_sar"] == 20
        assert result["kpis"]["capital_unallocated_sar"] == 0
        assert "_funding_remaining_sar" not in str(result)
        results.append(result)
    # HOLD precedes ADD in the first input, so presentation sorting must move
    # the marker's original host; the ledger and public results stay identical.
    assert held["Symbol"] != results[0]["actions"][0]["symbol"]
    assert results[0]["actions"] == results[1]["actions"]
    assert results[0]["kpis"] == results[1]["kpis"]
