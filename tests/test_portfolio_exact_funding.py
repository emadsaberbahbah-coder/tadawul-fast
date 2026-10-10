"""Certified portfolio share costs must respect exact cash and reserve budgets."""
import datetime as dt
from decimal import Decimal

import pytest

from core.analysis import portfolio_actions as pa


@pytest.fixture(autouse=True)
def qualified_add_policy(monkeypatch):
    # Isolate qualified ADD funding; acquisition/account guards remain real.
    monkeypatch.setenv("TFB_PF_ENABLED", "1")
    monkeypatch.setenv("TFB_EXIT_BY_RULE_GATE", "0")
    monkeypatch.setenv("TFB_PF_ADD_CONFIRM_DAYS", "0")
    monkeypatch.setenv("TFB_PF_CONFIRM_PERSIST", "0")
    monkeypatch.setenv("TFB_PF_TRUST_GATE", "0")
    monkeypatch.setenv("TFB_PF_ADD_LOSER_VETO", "off")
    monkeypatch.setenv("TFB_OPP_FX_SANITY", "0")
    monkeypatch.delenv("TFB_PF_FEE_FUNDING", raising=False)
    monkeypatch.delenv("TFB_PF_FEE_SAR", raising=False)


def certified_build(cash, sectors=("Energy", "Technology"), *, price="1.49",
                    currency="SAR", fx="1", controls=None):
    stamp = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    rows = [{"Symbol": f"SYNTH{i:02d}.{'SR' if currency == 'SAR' else 'US'}",
             "Currency": currency, "Position Qty": 1, "Current Price": price,
             "Buy Price": str(Decimal(price) / Decimal("1.49")),
             "Intrinsic Value": str(Decimal(price) * 2),
             "Sector": sector, "Forecast Reliability Score": 90,
             "Data Quality Score": 90, "Risk Bucket": "Moderate",
             "Recommendation": "BUY", "Investability Status": "INVESTABLE",
             "Data Provider": "EODHD", "Last Updated": stamp,
             "Warnings": f"acquisition_status:success; acquisition_provider:EODHD; "
                         f"acquisition_acquired_at:{stamp}; acquisition_quote_asof:{stamp}"}
            for i, sector in enumerate(sectors)]
    evidence = {"schema_version": 1, "source_ref": "synthetic://exact-funding-test",
        "captured_at": stamp, "accounts": [{"account_id": "synthetic-account",
            "positions_complete": True, "positions_asof": stamp,
            "positions": [{"instrument_id": f"synthetic-{i}", "symbol": row["Symbol"],
                "currency": currency, "quantity": 1} for i, row in enumerate(rows)],
            "cash_complete": True, "cash_asof": stamp,
            "cash": [{"currency": "SAR", "settled_cash": cash, "reserved_cash": 0,
                "reservations_complete": True}]}], "funding_accounts": ["synthetic-account"],
        "holding_links": [{"row_index": i, "account_id": "synthetic-account",
            "instrument_id": f"synthetic-{i}", "position_symbol": row["Symbol"],
            "symbol": row["Symbol"], "currency": currency} for i, row in enumerate(rows)]}
    rates = {"SAR": 1}
    if currency != "SAR":
        rates[currency] = fx
        evidence["fx_rates"] = [{"currency": currency, "rate_to_sar": fx,
            "asof": stamp, "source_ref": "synthetic://fresh-fx-test"}]
    ctl = {"cash_available_sar": cash, "trust_gate_enabled": False, **(controls or {})}
    result = pa.build_portfolio_actions(rows, ctl, rates, {"reconciliation_evidence": evidence})
    assert result["status"] == "ok"
    assert result["meta"]["execution_ready"] is True
    assert result["meta"]["input_certification"]["funding_eligible"] is True
    return result, rows


def share_costs(result, rows, fx="1"):
    prices = {row["Symbol"]: Decimal(row["Current Price"]) * Decimal(fx) for row in rows}
    return [(action, Decimal(action["suggested_delta_shares"] or 0) * prices[action["symbol"]])
            for action in result["actions"] if action["action"] == "ADD"]


NO_RESERVE = {"target_cash_pct": 0, "max_position_pct": 100, "max_sector_pct": 100}


@pytest.mark.parametrize("fee_flag", [None, "0"])
def test_certified_cash_cannot_fund_two_rounded_fractional_tickets(monkeypatch, fee_flag):
    if fee_flag is not None:
        monkeypatch.setenv("TFB_PF_FEE_FUNDING", fee_flag)
    result, rows = certified_build("2.50", controls=NO_RESERVE)
    costs = share_costs(result, rows)
    assert sum(cost for _, cost in costs) == Decimal("1.49")
    assert [action["suggested_delta_shares"] for action, _ in costs] == [1, 0]
    assert result["actions"][0]["suggested_delta_sar"] == 1  # Presentation stays whole SAR.


def test_default_position_sector_and_cash_reserve_limits_remain_satisfied():
    sectors = ("Energy", "Technology", "Financials", "Health Care", "Industrials",
               "Utilities", "Materials", "Consumer Staples", "Consumer Discretionary",
               "Real Estate", "Communication Services", "Unknown")
    result, rows = certified_build("4.77", sectors)
    ctl = result["meta"]["controls_snapshot"]
    assert (ctl["target_cash_pct"], ctl["max_position_pct"], ctl["max_sector_pct"]) == (10, 15, 30)
    total = Decimal(len(rows)) * Decimal("1.49") + Decimal("4.77")
    reserve = total * Decimal("0.10")
    costs = share_costs(result, rows)
    principal = sum(cost for _, cost in costs)
    assert principal == Decimal("1.49")
    assert Decimal("4.77") - principal >= reserve
    assert all(Decimal("1.49") + cost <= total * Decimal("0.15") for _, cost in costs)


@pytest.mark.parametrize("cash,price,currency,fx", [
    ("2.98", "1.49", "SAR", "1"),
    ("2.97", "0.396", "USD", "3.75"),
    ("100", "10", "SAR", "1"),
])
def test_positive_funding_can_use_exact_native_price_fx_boundary(cash, price, currency, fx):
    result, rows = certified_build(cash, price=price, currency=currency, fx=fx, controls=NO_RESERVE)
    costs = share_costs(result, rows, fx)
    assert sum(cost for _, cost in costs) == Decimal(cash)
    assert any(action["suggested_delta_shares"] > 0 for action, _ in costs)
    assert all(action["suggested_delta_sar"] > 0 for action, cost in costs if cost > 0)


def test_shared_sector_budget_deducts_exact_ticket_principal():
    result, rows = certified_build("6", sectors=("Energy", "Energy"), controls={
        "target_cash_pct": 0, "max_position_pct": 100, "max_sector_pct": 50})
    total = Decimal("6") + Decimal("2.98")
    costs = share_costs(result, rows)
    assert sum(cost for _, cost in costs) == Decimal("1.49")
    assert Decimal("2.98") + sum(cost for _, cost in costs) <= total * Decimal("0.5")


def test_optional_fee_policy_reserves_fee_before_exact_principal(monkeypatch):
    monkeypatch.setenv("TFB_PF_FEE_FUNDING", "1")
    monkeypatch.setenv("TFB_PF_FEE_SAR", "0.10")
    result, rows = certified_build("3.18", controls={
        "target_cash_pct": 0, "max_position_pct": 50, "max_sector_pct": 100})
    costs = share_costs(result, rows)
    funded = [(action, cost) for action, cost in costs if action["suggested_delta_shares"] > 0]
    assert len(funded) == 2
    assert sum(cost + Decimal("0.10") for _, cost in funded) == Decimal("3.18")
    assert [action["suggested_delta_shares"] for action, _ in funded] == [1, 1]
