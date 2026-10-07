"""Synthetic native-fee ledger inputs survive the real portfolio rebuild.

Quote projection, runner, final restoration and holdings guards are production
code. Only backend transport and Sheets I/O use the existing boundary fixtures.
"""
import copy
import math

import pytest

from scripts import run_dashboard_sync as sync
from tests.test_portfolio_active_targets import (
    HEADERS, HOLDINGS, PRICES, SYMBOLS, Writer, cell, ledger, quote_row,
    rows_by_symbol, run_portfolio,
)

FEES = {"SYNA.US": 0.375, "SYNB.US": 1.25, "7010.SR": 2.75}


def fee_ledger():
    grid = ledger()
    grid[3].append("Buy Fees")
    for row in grid[4:]:
        row.append(FEES.get(str(row[0]).upper(), 0.0))
    return grid


def effective_basis(symbol):
    hold = HOLDINGS[symbol]
    total = hold["qty"] * hold["cost"] + FEES[symbol]
    return total, total / hold["qty"]


def assert_fee_money(row, symbol, *, price=None):
    hold = HOLDINGS[symbol]
    total, unit = effective_basis(symbol)
    price = PRICES[symbol] if price is None else price
    value = hold["qty"] * price
    assert cell(row, "Position Qty") == hold["qty"]
    assert cell(row, "Avg Cost") == unit
    assert cell(row, "Position Cost") == round(total, 6)
    assert cell(row, "Position Value") == round(value, 6)
    assert cell(row, "Unrealized P/L") == round(value - total, 6)
    assert cell(row, "Unrealized P/L %") == round((value - total) / total * 100, 6)


def test_native_fees_preserve_original_inputs_and_total_in_frozen_snapshot():
    writer = Writer(fee_ledger())
    basis = sync._read_cost_basis(writer, "offline")
    assert set(basis) == set(SYMBOLS)
    assert writer.ledger_reads == [("A1:EZ20050",)]
    for symbol, hold in basis.items():
        total, unit = effective_basis(symbol)
        assert hold["buy_price"] == HOLDINGS[symbol]["cost"]
        assert hold["buy_fees"] == FEES[symbol]
        assert hold["native_cost"] == total
        assert hold["cost"] == unit
        assert hold["currency"] == HOLDINGS[symbol]["currency"]


@pytest.mark.parametrize("strict", ["0", "1"])
def test_actual_producer_runner_uses_same_fee_snapshot_after_restoration(monkeypatch, strict):
    grid = fee_ledger()
    prior = [quote_row(symbol, qty=999, cost=777) for symbol in SYMBOLS]

    def respond(payload, writer):
        # The shared fixture starts in rollout-off mode; arm the actual
        # environment reader before the production membership seam runs.
        monkeypatch.setenv("TFB_SYNC_STRICT_MEMBERSHIP", strict)
        assert payload["symbols"] == SYMBOLS
        # An operator update during the fetch belongs to the next refresh.
        writer.ledger[4][7] += 100
        return [quote_row(symbol) for symbol in SYMBOLS]

    result, writer, payloads = run_portfolio(monkeypatch, grid=grid, prior=prior,
        persistence=True, response_factory=respond)
    assert result.rows_written == len(SYMBOLS)
    assert len(writer.ledger_reads) == 1
    assert len(payloads) == 1 and payloads[0]["symbols"] == SYMBOLS
    assert "buy_fees" not in payloads[0] and "cost_basis" not in payloads[0]
    published = rows_by_symbol(writer.published)
    for symbol in SYMBOLS:
        assert_fee_money(published[symbol], symbol)
        assert cell(published[symbol], "User Notes") == "Synthetic manual note"


def test_repeated_injection_and_price_update_do_not_add_fees_twice():
    basis = sync._read_cost_basis(Writer(fee_ledger()), "offline")
    first, count = sync._inject_portfolio_holdings(
        HEADERS, [quote_row(symbol) for symbol in SYMBOLS], basis)
    again, again_count = sync._inject_portfolio_holdings(HEADERS, first, basis)
    assert count == again_count == len(SYMBOLS) and again == first
    repriced = copy.deepcopy(first)
    for row in repriced:
        row[HEADERS.index("Current Price")] = 50.25
    final, _ = sync._inject_portfolio_holdings(HEADERS, repriced, basis)
    for symbol, row in rows_by_symbol(final).items():
        assert_fee_money(row, symbol, price=50.25)
    assert sync._portfolio_holdings_contract(HEADERS, final, basis, require_complete=True)[0]


def test_explicit_sar_money_uses_conversion_separately_from_native_fees():
    basis = sync._read_cost_basis(Writer(fee_ledger()), "offline")
    headers = ["Symbol", "Currency", "Position Qty", "Avg Cost", "Current Price",
        "Price SAR", "Position Cost", "Position Value", "Unrealized P/L",
        "Unrealized P/L %", "Cost SAR", "MV SAR", "P/L SAR"]
    symbol = "SYNA.US"
    rows, count = sync._inject_portfolio_holdings(headers,
        [[symbol, "USD", 0, 0, 10, 37.5]], basis)
    total, _ = effective_basis(symbol)
    row = dict(zip(headers, rows[0]))
    assert count == 1 and row["Position Cost"] == round(total, 6)
    assert row["Cost SAR"] == round(total * 3.75, 6)
    assert row["MV SAR"] == round(HOLDINGS[symbol]["qty"] * 10 * 3.75, 6)
    assert row["P/L SAR"] == round((HOLDINGS[symbol]["qty"] * 10 - total) * 3.75, 6)


@pytest.mark.parametrize("bad", [None, "", "unknown", True, False, -0.01,
    float("nan"), float("inf"), float("-inf")])
def test_unproven_active_fee_preserves_prior_page_before_any_fetch(monkeypatch, bad):
    grid = fee_ledger()
    grid[4][7] = bad
    assert sync._read_cost_basis(Writer(grid), "offline") == {}
    prior = [quote_row(symbol) for symbol in SYMBOLS]
    result, writer, payloads = run_portfolio(monkeypatch, grid=grid, prior=prior)
    assert not payloads and not writer.writes
    assert result.rows_written == 0 and writer.prior == prior


@pytest.mark.parametrize("mutation", ["duplicate_fee", "foreign_fee_unit",
    "average_cost_with_fees", "overflow_total", "overflow_effective_cost"])
def test_ambiguous_or_nonfinite_fee_basis_rejected(mutation):
    grid = fee_ledger()
    if mutation == "duplicate_fee":
        grid[3].append("Buy Fees")
        for row in grid[4:]:
            row.append(0)
    elif mutation == "foreign_fee_unit":
        grid[3][7] = "Buy Fees SAR"
    elif mutation == "average_cost_with_fees":
        grid[3][5] = "Avg Cost"
    elif mutation == "overflow_total":
        grid[4][5:8] = [1e308, 1.0, 1e308]
    else:
        grid[4][5:8] = [1.0, 1e-308, 1e308]
    assert sync._read_cost_basis(Writer(grid), "offline") == {}


def test_absent_fee_column_retains_established_unit_cost_contract():
    basis = sync._read_cost_basis(Writer(ledger()), "offline")
    for symbol, hold in basis.items():
        assert hold["cost"] == HOLDINGS[symbol]["cost"]
        assert not any(key in hold for key in ("buy_fees", "buy_price", "native_cost"))


def test_zero_fee_and_closed_bad_fee_do_not_change_active_native_basis():
    grid = fee_ledger()
    for row in grid[4:]:
        row[7] = 0 if row[3] == "Active" else "irrelevant closed fee"
    basis = sync._read_cost_basis(Writer(grid), "offline")
    assert set(basis) == set(SYMBOLS)
    assert all(hold["cost"] == HOLDINGS[symbol]["cost"] for symbol, hold in basis.items())


def test_final_guard_rejects_finite_gross_cost_when_snapshot_includes_fees():
    basis = sync._read_cost_basis(Writer(fee_ledger()), "offline")
    final, _ = sync._inject_portfolio_holdings(HEADERS,
        [quote_row(symbol) for symbol in SYMBOLS], basis)
    final[0][HEADERS.index("Position Cost")] -= 0.1
    safe, reason = sync._portfolio_holdings_contract(HEADERS, final, basis, require_complete=True)
    assert not safe and reason == "native position cost differs from ledger snapshot"


def test_actual_sheets_reader_keeps_unformatted_fee_precision():
    grid = fee_ledger()
    calls = []

    class Response:
        def execute(self):
            return {"values": copy.deepcopy(grid)}

    class Service:
        def spreadsheets(self): return self
        def values(self): return self
        def get(self, **kwargs):
            calls.append(kwargs)
            return Response()

    reader = object.__new__(sync.SheetsWriter)
    reader._get_service = lambda: Service()
    basis = sync._read_cost_basis(reader, "offline")
    assert calls[0]["valueRenderOption"] == "UNFORMATTED_VALUE"
    assert basis["SYNA.US"]["buy_fees"] == FEES["SYNA.US"]
    assert math.isfinite(basis["SYNA.US"]["cost"])
