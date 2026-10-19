"""Price-only precision through the real portfolio and Top10 output paths.

Every account and quote is synthetic. No provider, Sheet or broker is called;
the real normalizers, certifier, decisions and allocator remain in use.
"""
import copy
import socket

import pytest

from core.analysis import opportunity_builder as ob, portfolio_actions as pa
from core.price_precision import round_price
from tests.decision_evidence_fixtures import (
    build_with_observed_inputs, observed_price_fields,
)
from tests.portfolio_reconciliation_fixtures import build_certified_portfolio_actions


@pytest.fixture(autouse=True)
def offline_modes(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("decision precision test attempted external I/O")
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    for key, value in {
        "TFB_PRICE_PRECISION": "off", "TFB_PF_ENABLED": "1",
        "TFB_OPP_ENABLED": "1", "TFB_PF_RESEARCH_VIEW": "off",
        "TFB_PF_ADD_CONFIRM_DAYS": "0", "TFB_PF_CONFIRM_PERSIST": "0",
        "TFB_PF_TRUST_GATE": "0", "TFB_EXIT_BY_RULE_GATE": "0",
        "TFB_T10_PRICE_XCHECK": "off", "TFB_PF_FEE_FUNDING": "0",
    }.items():
        monkeypatch.setenv(key, value)


def holding(price, currency="SAR"):
    return {**observed_price_fields(),
            "Symbol": "SYNTHETIC.SR", "Name": "Synthetic precision fixture",
            "Currency": currency, "Exchange": "Tadawul", "Sector": "Synthetic",
            "Quantity": 100000, "Buy Price": price / 1.1 if price else None,
            "Current Price": price, "Target Price": price * 1.3 if price else None,
            "Intrinsic Value": price * 1.3 if price else None,
            "Forecast Reliability Score": 90, "Data Quality Score": 95,
            "Risk Bucket": "Moderate", "Provider/Engine Conflict": "No",
            "Volatility 30D": 4, "Avg Volume 30D": 1e12,
            "Recommendation": "BUY", "Investability Status": "INVESTABLE"}


def portfolio(row, rate=1):
    return build_certified_portfolio_actions(pa, [row], controls={
        "cash_available_sar": 10000, "target_cash_pct": 0,
        "max_position_pct": 100, "max_sector_pct": 100,
    }, fx_rates={row["Currency"]: rate})


def board(rows, rate=1):
    return build_with_observed_inputs(ob, rows,
        portfolio={"cash_available_sar": 10000},
        fx_rates={row["Currency"]: rate for row in rows})


@pytest.mark.parametrize("price", [1.23456789e-12, 5.47e-6, .004321987, 1.23456789])
@pytest.mark.parametrize("currency,rate", [("SAR", 1), ("USD", 3.75)])
def test_armed_public_outputs_preserve_positive_prices_and_levels(monkeypatch, price, currency, rate):
    row = holding(price, currency)
    monkeypatch.setenv("TFB_PRICE_PRECISION", "on")
    pf, top = portfolio(row, rate), board([row], rate)
    assert pf["meta"]["execution_ready"] is True
    assert len(top["selected"]) == 1
    action, ticket = pf["actions"][0], top["selected"][0]
    assert action["price"] == ticket["price"] == round_price(price, 2) > 0
    assert action["price_sar"] == ticket["price_sar"] == round_price(price * rate, 2) > 0
    assert action["avg_cost"] == round_price(price / 1.1, 2) > 0
    assert top["candidates_rows"][0]["price"] == ticket["price"]
    assert top["candidates_rows"][0]["price_sar"] == ticket["price_sar"]
    for result in (action, ticket):
        assert result["stop_sar"] > 0
        assert result["stop_sar"] < result["price_sar"] < result["tp1_sar"] <= result["tp2_sar"]
        for key in ("target_price", "intrinsic_value"):
            assert result["detail"][key] == round_price(price * 1.3, 2) > 0
    assert pf["meta"]["price_precision"] == top["meta"]["price_precision"] == "on"


def test_armed_entry_and_advisor_text_never_reintroduce_zero_prices(monkeypatch):
    monkeypatch.setenv("TFB_PRICE_PRECISION", "true")
    row = holding(1.23456789e-12)
    action = portfolio(row)["actions"][0]
    ticket = board([row])["selected"][0]
    assert action["action"] == "ADD"
    for value in (ticket["entry_zone"].removesuffix(" SAR").split("–")):
        assert float(value.replace(",", "")) > 0
    for result in (action, ticket):
        assert "stop 0 /" not in result["advisor_note"]
        assert "stop 0.0 /" not in result["advisor_note"]
        assert "stop 0.00," not in result["advisor_note"]
        assert "0.000000000001" in result["advisor_note"]


@pytest.mark.parametrize("mode", ["off", "on"])
def test_block_explanations_and_existing_xcheck_receipts_preserve_price_text(monkeypatch, mode):
    monkeypatch.setenv("TFB_PRICE_PRECISION", mode)
    price = 5.47e-6
    row = {**holding(price), "Buy Price": price * 100}
    action = portfolio(row)["actions"][0]
    assert action["action"] == "BLOCK"
    assert action["action_reason"].startswith("BLOCKED — review cost basis")
    if mode == "on":
        assert "current price 0.00000547" in action["action_reason"]
        assert float(ob._xcheck_fmt_px(price)) == price
    else:
        assert "current price 0.0" in action["action_reason"]
        assert ob._xcheck_fmt_px(price) == "0.0000"
    monkeypatch.setenv("TFB_T10_PRICE_XCHECK", "observe")
    calls = []
    def quote(symbol, timeout):
        calls.append((symbol, timeout))
        return price, "2026-10-17T00:00:00+00:00", None
    monkeypatch.setattr(ob, "_XCHECK_FETCH_OVERRIDE", quote)
    ticket = board([holding(price)])["selected"][0]
    receipt = ticket["detail"]["price_xcheck"]
    assert len(calls) == 1 and receipt["verdict"] == "verified"
    assert receipt["delta_pct"] == 0
    if mode == "on":
        assert "eodhd 0.000005470000 vs sheet 0.000005470000" in ticket["advisor_note"]
        assert receipt["secondary"] == price
    else:
        assert "eodhd 0.0000 vs sheet 0.0000" in ticket["advisor_note"]
        assert receipt["secondary"] == 0


@pytest.mark.parametrize("mode", ["off", "on"])
def test_public_wrappers_keep_cyclic_upstream_metadata_fail_soft(monkeypatch, mode):
    monkeypatch.setenv("TFB_PRICE_PRECISION", mode)
    upstream = {}
    upstream["cycle"] = upstream
    upstream["coverage"] = upstream
    row = holding(5.47e-6)
    result = build_certified_portfolio_actions(pa, [row],
        controls={"cash_available_sar": 10000}, fx_rates={"SAR": 1},
        upstream_meta=upstream)
    assert result["status"] == "error" and result["actions"] == []
    top = build_with_observed_inputs(ob, [row],
        portfolio={"cash_available_sar": 10000}, fx_rates={"SAR": 1},
        upstream_meta=upstream)
    assert top["status"] == "error" and top["selected"] == []
    for output in (result, top):
        assert output["meta"].get("price_precision") == ("on" if mode == "on" else None)


@pytest.mark.parametrize("mode", ["", "off", "0", "observe", "invalid"])
def test_disarmed_outputs_match_the_previous_price_boundary(monkeypatch, mode):
    monkeypatch.setenv("TFB_PRICE_PRECISION", mode)
    row = holding(5.47e-6)
    actual_pf, actual_board = portfolio(row), board([row])
    with monkeypatch.context() as legacy:
        legacy.setattr(pa, "_round_price", pa._round)
        legacy.setattr(pa, "_fmt_price", pa._fmt)
        legacy.setattr(ob, "_round_price", ob._round2)
        legacy.setattr(ob, "_fmt_px", lambda value: "?" if value is None else "{:,.2f}".format(value))
        previous_pf, previous_board = portfolio(row), board([row])
    for current, previous in ((actual_pf, previous_pf), (actual_board, previous_board)):
        assert current["actions" if current is actual_pf else "selected"] == previous["actions" if current is actual_pf else "selected"]
        assert current["kpis"] == previous["kpis"]
        assert "price_precision" not in current["meta"]
    assert actual_pf["actions"][0]["price"] == actual_board["selected"][0]["price"] == 0


def test_arming_preserves_decisions_allocation_scores_and_return_precision(monkeypatch):
    rows = [holding(5.47e-6), {**holding(.004321987), "Symbol": "SYNTHETIC2.SR"}]
    off_pf, off_board = portfolio(rows[0]), board(rows)
    monkeypatch.setenv("TFB_PRICE_PRECISION", "yes")
    on_pf, on_board = portfolio(rows[0]), board(rows)
    for current, previous in ((on_pf, off_pf), (on_board, off_board)):
        assert current["kpis"] == previous["kpis"]
    price_keys = {"price", "price_sar", "avg_cost", "stop_sar", "tp1_sar", "tp2_sar", "advisor_note", "entry_zone"}
    def financial_output(result):
        record = copy.deepcopy(result)
        for key in price_keys:
            record.pop(key, None)
        for key in ("target_price", "intrinsic_value"):
            record.get("detail", {}).pop(key, None)
        return record
    assert [financial_output(a) for a in on_pf["actions"]] == [financial_output(a) for a in off_pf["actions"]]
    assert [financial_output(t) for t in on_board["selected"]] == [financial_output(t) for t in off_board["selected"]]
    for current, previous in zip(on_board["candidates_rows"], off_board["candidates_rows"]):
        assert {k: v for k, v in current.items() if k not in {"price", "price_sar"}} == {k: v for k, v in previous.items() if k not in {"price", "price_sar"}}


@pytest.mark.parametrize("mode", ["off", "on"])
def test_unknown_quotes_and_levels_remain_unknown(monkeypatch, mode):
    monkeypatch.setenv("TFB_PRICE_PRECISION", mode)
    result = pa.build_portfolio_actions([holding(None)], {"cash_available_sar": 0}, {"SAR": 1})
    action = result["actions"][0]
    assert action["price"] is action["price_sar"] is None
    assert action["stop_sar"] is action["tp1_sar"] is action["tp2_sar"] is None
    assert result["meta"]["execution_ready"] is False
    audit = board([holding(None)])["candidates_rows"][0]
    assert audit["price"] is audit["price_sar"] is None


@pytest.mark.parametrize("value", [10, 12.3456789, 1000.1234567])
def test_large_price_rounding_contract_is_unchanged(monkeypatch, value):
    monkeypatch.setenv("TFB_PRICE_PRECISION", "on")
    assert pa._round_price(value) == ob._round_price(value) == round(value, 2)
    assert ob._fmt_px(value) == "{:,.2f}".format(value)


@pytest.mark.parametrize("value", [None, float("nan"), float("inf"), -float("inf")])
def test_portfolio_price_formatter_preserves_null_and_nonfinite_behavior(monkeypatch, value):
    monkeypatch.setenv("TFB_PRICE_PRECISION", "on")
    assert pa._round_price(value) is None
