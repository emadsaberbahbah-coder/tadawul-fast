"""A blank native fee requires an exact, independent principal witness.

All amounts and identities are synthetic. Tests exercise the production reader,
full 122-column projection, portfolio runner, injection and publication guard;
only backend transport and Sheets I/O use the existing boundary fixtures.
"""
import copy
import math

import pytest

from scripts import run_dashboard_sync as sync
from tests.test_portfolio_active_targets import (
    HEADERS, HOLDINGS, PRICES, SYMBOLS, Writer, cell, ledger, quote_row,
    rows_by_symbol, run_portfolio,
)


FEE_I = 7
NATIVE_COST_I = 8
EXPLICIT_FEE_SYMBOL = "SYNB.US"
EXPLICIT_FEE = 1.25


def witness_ledger(blank="", *, mixed=False):
    grid = ledger()
    grid[3].extend(["Buy Fees", "Cost Basis"])
    for row in grid[4:]:
        symbol = str(row[0]).upper()
        fee = EXPLICIT_FEE if mixed and symbol == EXPLICIT_FEE_SYMBOL else blank
        principal = row[5] * row[6]
        # Closed rows cannot supply or invalidate an active acquisition input.
        row.extend([fee if row[3] == "Active" else "unknown closed fee",
                    principal + (EXPLICIT_FEE if mixed and
                                 symbol == EXPLICIT_FEE_SYMBOL else 0)])
    return grid


def expected_native_cost(symbol, *, mixed=False):
    hold = HOLDINGS[symbol]
    fee = EXPLICIT_FEE if mixed and symbol == EXPLICIT_FEE_SYMBOL else 0
    return hold["qty"] * hold["cost"] + fee


def assert_native_money(row, symbol, *, mixed=False, price=None):
    hold = HOLDINGS[symbol]
    total = expected_native_cost(symbol, mixed=mixed)
    value = hold["qty"] * (PRICES[symbol] if price is None else price)
    assert len(row) == len(HEADERS) == 122
    assert cell(row, "Position Qty") == hold["qty"]
    assert cell(row, "Avg Cost") == total / hold["qty"]
    assert cell(row, "Position Cost") == round(total, 6)
    assert cell(row, "Position Value") == round(value, 6)
    assert cell(row, "Unrealized P/L") == round(value - total, 6)
    assert cell(row, "Unrealized P/L %") == round((value - total) / total * 100, 6)


def assert_unknown_preserves_page(monkeypatch, grid):
    """Arm the destructive legacy ordering and prove the guard precedes it."""
    assert sync._read_cost_basis(Writer(grid), "offline") == {}
    prior = [quote_row(symbol) for symbol in SYMBOLS]
    clear_calls = []
    original_runner = sync._run_one_task

    async def run_with_clear(*args, **kwargs):
        args = list(args)
        args[4] = True  # clear_before_write, deliberately enabled
        return await original_runner(*args, **kwargs)

    monkeypatch.setenv("TFB_SYNC_WRITE_THEN_TRIM", "0")
    monkeypatch.setattr(sync, "_run_one_task", run_with_clear)
    monkeypatch.setattr(Writer, "clear_from",
        lambda *args, **kwargs: clear_calls.append((args, kwargs)), raising=False)
    result, writer, payloads = run_portfolio(monkeypatch,
        [quote_row(symbol) for symbol in SYMBOLS], grid=grid, prior=prior,
        explicit_symbols=["UNTRUSTED.US"])
    assert result.status == "failed" and result.rows_written == 0
    assert "preserving prior portfolio" in result.error
    assert not payloads and not writer.writes and not clear_calls
    assert writer.prior == prior and writer.published == []
    assert writer.ledger_reads == [("A1:EZ20050",)]
    assert writer.status_rows and "| data=PARTIAL" in writer.status_rows[-1][3]


@pytest.mark.parametrize("blank", [None, "", " \t\n"])
def test_genuine_blank_retains_raw_value_and_zero_delta_provenance(blank):
    writer = Writer(witness_ledger(blank))
    basis = sync._read_cost_basis(writer, "offline")
    assert set(basis) == set(SYMBOLS)
    assert writer.ledger_reads == [("A1:EZ20050",)]
    for symbol, hold in basis.items():
        assert hold["buy_fees_raw"] == blank
        assert hold["buy_fees_basis"] == "native_cost_basis_zero_delta"
        assert hold["buy_fees"] == 0.0
        assert hold["buy_price"] == HOLDINGS[symbol]["cost"]
        assert hold["native_cost"] == expected_native_cost(symbol)
        assert hold["cost"] == HOLDINGS[symbol]["cost"]
        assert hold["currency"] == HOLDINGS[symbol]["currency"]


@pytest.mark.parametrize("strict", ["0", "1"])
def test_runner_uses_one_frozen_mixed_witness_for_cohort_and_final_math(monkeypatch, strict):
    grid = witness_ledger(None, mixed=True)
    prior = [quote_row(symbol, qty=999, cost=777) for symbol in SYMBOLS]
    fetched = [quote_row(symbol) for symbol in reversed(SYMBOLS)]
    unchanged_response = copy.deepcopy(fetched)

    def respond(payload, writer):
        monkeypatch.setenv("TFB_SYNC_STRICT_MEMBERSHIP", strict)
        assert payload["symbols"] == payload["tickers"] == SYMBOLS
        # An operator edit after the fetch begins belongs to the next run.
        writer.ledger[4][5] += 10
        writer.ledger[4][6] += 3
        writer.ledger[4][FEE_I] = 4
        writer.ledger[4][NATIVE_COST_I] += 100
        return fetched

    result, writer, payloads = run_portfolio(monkeypatch, grid=grid, prior=prior,
        persistence=True, response_factory=respond)
    assert result.status == "success" and result.rows_written == len(SYMBOLS)
    assert writer.ledger_reads == [("A1:EZ20050",)] and len(payloads) == 1
    assert payloads[0]["limit"] == len(SYMBOLS)
    assert not any(key in payloads[0] for key in
        ("buy_fees", "buy_fees_raw", "buy_fees_basis", "cost_basis", "native_cost"))
    assert fetched == unchanged_response
    published = rows_by_symbol(writer.published)
    assert set(published) == set(SYMBOLS) and len(published) == len(writer.published)
    for symbol, row in published.items():
        assert_native_money(row, symbol, mixed=True)
        assert cell(row, "User Notes") == "Synthetic manual note"
        assert "acquisition_status:success" in cell(row, "Warnings")
    assert sync._page_fresh_fetch_metrics(result) == (3, 3, 100.0)
    assert "acquired=3/3 acquisition=COMPLETE" in writer.status_rows[-1][3]


def test_repeated_injection_and_repricing_keep_zero_witness_and_explicit_fee_once():
    basis = sync._read_cost_basis(Writer(witness_ledger("", mixed=True)), "offline")
    before = copy.deepcopy(basis)
    first, count = sync._inject_portfolio_holdings(HEADERS,
        [quote_row(symbol) for symbol in SYMBOLS], basis)
    repeated, repeated_count = sync._inject_portfolio_holdings(HEADERS, first, basis)
    assert repeated == first and count == repeated_count == len(SYMBOLS)
    repriced = copy.deepcopy(repeated)
    for row in repriced:
        row[HEADERS.index("Current Price")] = 50.25
    final, _ = sync._inject_portfolio_holdings(HEADERS, repriced, basis)
    assert basis == before
    for symbol, row in rows_by_symbol(final).items():
        assert_native_money(row, symbol, mixed=True, price=50.25)
    assert sync._portfolio_holdings_contract(HEADERS, final, basis,
        require_complete=True) == (True, "")
    for symbol in SYMBOLS:
        if symbol != EXPLICIT_FEE_SYMBOL:
            assert basis[symbol]["buy_fees_basis"] == "native_cost_basis_zero_delta"
    assert "buy_fees_basis" not in basis[EXPLICIT_FEE_SYMBOL]
    assert "buy_fees_raw" not in basis[EXPLICIT_FEE_SYMBOL]


def test_restored_row_gets_frozen_witness_cost_but_keeps_preserved_acquisition(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    restored_symbol = "SYNA.US"
    grid = witness_ledger(None, mixed=True)
    predecessor = quote_row(restored_symbol, stamp="2026-10-07T11:00:00Z",
        qty=1, cost=1, note="Preserved synthetic operator note")
    unchanged = copy.deepcopy(predecessor)

    def respond(_payload, writer):
        writer.ledger[4][5:7] = [999, 333]
        writer.ledger[4][FEE_I] = "unknown after snapshot"
        writer.ledger[4][NATIVE_COST_I] = 100
        return [quote_row(symbol) for symbol in SYMBOLS if symbol != restored_symbol]

    result, writer, payloads = run_portfolio(monkeypatch, grid=grid,
        prior=[predecessor], persistence=True, response_factory=respond)
    assert payloads[0]["symbols"] == SYMBOLS and result.rows_written == len(SYMBOLS)
    assert writer.ledger_reads == [("A1:EZ20050",)]
    assert result._stamp_meta["persist_restored"] == 1
    restored = rows_by_symbol(writer.published)[restored_symbol]
    assert_native_money(restored, restored_symbol, mixed=True)
    assert cell(restored, "User Notes") == "Preserved synthetic operator note"
    assert cell(restored, "Last Updated (UTC)") == cell(unchanged, "Last Updated (UTC)")
    assert "acquisition_status:preserved" in cell(restored, "Warnings")
    assert "acquired=2/3 acquisition=PARTIAL" in writer.status_rows[-1][3]
    assert sync._page_fresh_fetch_metrics(result) == (2, 3, pytest.approx(200 / 3))
    assert predecessor == unchanged


@pytest.mark.parametrize("field,delta,reason", [
    ("Avg Cost", 0.1, "holding quantity or unit cost differs from ledger snapshot"),
    ("Position Cost", 0.1, "native position cost differs from ledger snapshot"),
    ("Position Cost", EXPLICIT_FEE, "native position cost differs from ledger snapshot"),
])
def test_final_guard_rejects_added_or_doubled_fee_candidates(field, delta, reason):
    basis = sync._read_cost_basis(Writer(witness_ledger(mixed=True)), "offline")
    rows, _ = sync._inject_portfolio_holdings(HEADERS,
        [quote_row(symbol) for symbol in SYMBOLS], basis)
    candidate = rows_by_symbol(rows)[EXPLICIT_FEE_SYMBOL]
    candidate[HEADERS.index(field)] += delta
    assert sync._portfolio_holdings_contract(HEADERS, rows, basis,
        require_complete=True) == (False, reason)


@pytest.mark.parametrize("symbol,extra_fee", [("SYNA.US", 0.1),
    (EXPLICIT_FEE_SYMBOL, EXPLICIT_FEE)])
def test_final_runner_guard_stops_added_fee_before_publication(monkeypatch, symbol, extra_fee):
    """The final guard catches an intervening restoration/injection defect."""
    original_injector = sync._inject_portfolio_holdings

    def intervening_bad_cost(headers, rows, basis, *, include_ledger_name=False):
        output, count = original_injector(headers, rows, basis,
            include_ledger_name=include_ledger_name)
        if include_ledger_name:  # The final, post-restoration production pass.
            row = rows_by_symbol(output)[symbol]
            row[HEADERS.index("Position Cost")] += extra_fee
        return output, count

    monkeypatch.setattr(sync, "_inject_portfolio_holdings", intervening_bad_cost)
    prior = [quote_row(item) for item in SYMBOLS]
    result, writer, payloads = run_portfolio(monkeypatch,
        [quote_row(item) for item in SYMBOLS], grid=witness_ledger(mixed=True), prior=prior)
    assert payloads[0]["symbols"] == SYMBOLS
    assert result.status == "failed" and result.rows_written == 0
    assert "Final portfolio contract failed" in result.error
    assert "native position cost differs from ledger snapshot" in result.error
    assert not writer.writes and writer.prior == prior


@pytest.mark.parametrize("bad", [True, False, -0.01, "-", "unknown", "null",
    "None", "0junk", [], {}, float("nan"), float("inf"), float("-inf")])
def test_nonblank_malformed_fee_cannot_borrow_even_an_exact_witness(monkeypatch, bad):
    grid = witness_ledger()
    grid[4][FEE_I] = bad
    assert_unknown_preserves_page(monkeypatch, grid)


@pytest.mark.parametrize("kind", [
    "missing_native_witness", "sar_only_witness", "duplicate_native_witness",
    "avg_cost_alias", "average_cost_alias", "qty_alias", "quantity_alias",
    "units_alias", "position_qty_alias", "missing_currency", "invalid_currency",
    "witness_missing_cell", "witness_blank", "witness_bool", "witness_unknown",
    "witness_zero", "witness_negative", "witness_nan", "witness_inf",
    "witness_contains_fee", "witness_sar_value", "witness_one_ulp_high",
    "witness_one_ulp_low",
])
def test_blank_fee_without_unique_exact_native_principal_stops_before_fetch_or_clear(
        monkeypatch, kind):
    grid = witness_ledger(None)
    row = grid[4]
    if kind == "missing_native_witness":
        grid[3][NATIVE_COST_I] = "Purchase memo"
    elif kind == "sar_only_witness":
        grid[3][NATIVE_COST_I] = "Cost Basis SAR"
    elif kind == "duplicate_native_witness":
        grid[3].append("Cost_Basis")
        for position in grid[4:]:
            position.append(position[NATIVE_COST_I])
    elif kind in {"avg_cost_alias", "average_cost_alias"}:
        grid[3][5] = "Avg Cost" if kind == "avg_cost_alias" else "Average Cost"
    elif kind in {"qty_alias", "quantity_alias", "units_alias", "position_qty_alias"}:
        grid[3][6] = {"qty_alias": "Qty", "quantity_alias": "Quantity",
            "units_alias": "Units", "position_qty_alias": "Position Qty"}[kind]
    elif kind in {"missing_currency", "invalid_currency"}:
        row[2] = "" if kind == "missing_currency" else "US dollars"
    elif kind == "witness_missing_cell":
        del row[NATIVE_COST_I:]
    elif kind.startswith("witness_"):
        principal = row[5] * row[6]
        row[NATIVE_COST_I] = {
            "witness_blank": None, "witness_bool": True,
            "witness_unknown": "unknown", "witness_zero": 0,
            "witness_negative": -principal, "witness_nan": float("nan"),
            "witness_inf": float("inf"), "witness_contains_fee": principal + 0.01,
            "witness_sar_value": principal * 3.75,
            "witness_one_ulp_high": math.nextafter(principal, math.inf),
            "witness_one_ulp_low": math.nextafter(principal, -math.inf),
        }[kind]
    else:
        raise AssertionError(kind)
    assert_unknown_preserves_page(monkeypatch, grid)


@pytest.mark.parametrize("currency_header", ["Ccy", "Currency"])
def test_native_witness_may_coexist_with_separate_sar_totals(currency_header):
    grid = witness_ledger()
    grid[3][2] = currency_header
    grid[3].append("Cost Basis SAR")
    for row in grid[4:]:
        row.append(row[NATIVE_COST_I] * (3.75 if row[2].upper() == "USD" else 1))
    basis = sync._read_cost_basis(Writer(grid), "offline")
    assert set(basis) == set(SYMBOLS)
    for symbol, hold in basis.items():
        assert hold["native_cost"] == expected_native_cost(symbol)
        assert hold["buy_fees_basis"] == "native_cost_basis_zero_delta"


@pytest.mark.parametrize("witness", [None, "unknown", 9999])
def test_explicit_fees_keep_established_contract_without_using_native_witness(witness):
    grid = witness_ledger()
    grid[3][6] = "Quantity"  # Established aliases remain valid for explicit fees.
    for row in grid[4:]:
        row[FEE_I] = EXPLICIT_FEE if str(row[0]).upper() == EXPLICIT_FEE_SYMBOL else 0
        row[NATIVE_COST_I] = witness
    basis = sync._read_cost_basis(Writer(grid), "offline")
    assert set(basis) == set(SYMBOLS)
    for symbol, hold in basis.items():
        assert hold["native_cost"] == expected_native_cost(symbol, mixed=True)
        assert "buy_fees_basis" not in hold and "buy_fees_raw" not in hold


@pytest.mark.parametrize("quote_currency", ["SAR", "GBP", "GBp", "GBX"])
def test_blank_witness_never_authorizes_a_foreign_or_minor_unit_quote(monkeypatch, quote_currency):
    monkeypatch.setenv("TFB_PF_MINOR_UNIT_CCY_GUARD", "1")
    grid = witness_ledger()
    prior = [quote_row(symbol) for symbol in SYMBOLS]
    rows = [quote_row(symbol, currency=quote_currency if symbol == "SYNA.US" else ...)
            for symbol in SYMBOLS]
    result, writer, payloads = run_portfolio(monkeypatch, rows, grid=grid, prior=prior)
    assert payloads and payloads[0]["symbols"] == SYMBOLS
    assert result.status == "failed" and result.rows_written == 0
    assert "holding quote currency does not match native ledger currency" in result.error
    assert not writer.writes and writer.prior == prior


def test_explicit_sar_outputs_use_fx_separately_from_blank_native_fee_witness():
    basis = sync._read_cost_basis(Writer(witness_ledger()), "offline")
    headers = ["Symbol", "Currency", "Position Qty", "Avg Cost", "Current Price",
        "Price SAR", "Position Cost", "Position Value", "Unrealized P/L",
        "Unrealized P/L %", "Cost SAR", "MV SAR", "P/L SAR"]
    symbol = "SYNA.US"
    rows, count = sync._inject_portfolio_holdings(headers,
        [[symbol, "USD", 0, 0, 10, 37.5]], basis)
    row = dict(zip(headers, rows[0]))
    total = expected_native_cost(symbol)
    assert count == 1 and row["Position Cost"] == round(total, 6)
    assert row["Cost SAR"] == round(total * 3.75, 6)
    assert row["MV SAR"] == round(HOLDINGS[symbol]["qty"] * 10 * 3.75, 6)
    assert row["P/L SAR"] == round((HOLDINGS[symbol]["qty"] * 10 - total) * 3.75, 6)


def test_sheets_boundary_requests_unformatted_native_witness_precision():
    grid = witness_ledger()
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
    assert len(calls) == 1 and calls[0]["valueRenderOption"] == "UNFORMATTED_VALUE"
    assert set(basis) == set(SYMBOLS)
    assert basis["SYNA.US"]["native_cost"] == expected_native_cost("SYNA.US")
