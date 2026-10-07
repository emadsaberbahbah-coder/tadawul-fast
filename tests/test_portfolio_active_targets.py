"""Active cost-basis holdings drive real portfolio requests and native money math.

All rows are synthetic. The producer, 122-column projection, runner, status stamp,
and acquisition audit are real; only backend transport and Sheets I/O are fake.
"""
import asyncio
import copy
import logging
from datetime import datetime, timezone

import pytest

from core import data_engine_v2 as engine
from core.data_validity import acquisition_census
from core.sheets.schema_registry import get_sheet_headers, get_sheet_keys
from scripts import run_dashboard_sync as sync
from scripts.audit_full_refresh_coverage import Rule, audit_grid, ledger_symbols


NOW = datetime(2026, 10, 7, 12, 30, tzinfo=timezone.utc)
STAMP = "2026-10-07T12:29:00Z"
HEADERS = get_sheet_headers("My_Portfolio")
KEYS = get_sheet_keys("My_Portfolio")
HOLDINGS = {
    "SYNA.US": {"qty": 2.5, "cost": 8.125, "currency": "USD"},
    "7010.SR": {"qty": 7.0, "cost": 20.0, "currency": "SAR"},
    "SYNB.US": {"qty": 3.0, "cost": 31.0, "currency": "USD"},
}
SYMBOLS = sorted(HOLDINGS)
PRICES = {"SYNA.US": 10.875, "7010.SR": 25.2, "SYNB.US": 27.125}
LEDGER_HEADERS = ["Symbol", "Name", "Ccy", "Status", "Buy Date", "Buy Price", "Shares"]


def ledger():
    # The actual shape has three title/metadata rows before its header.
    return [["Synthetic cost-basis ledger"], [], ["Synthetic metadata"],
        LEDGER_HEADERS[:],
        *[[symbol.lower(), "Synthetic holding", hold["currency"].lower(), "Active",
            "2026-01-01", hold["cost"], hold["qty"]] for symbol, hold in HOLDINGS.items()],
        *[["OLD" + str(i) + ".US", "Historical lot", "USD", "Inactive", "", 12, 3]
            for i in range(34)],
        ["SOLD.US", "Historical lot", "USD", "Sold", "", 12, 3],
        ["CLOSED.US", "Historical lot", "USD", "Closed", "", 12, 3],
        ["ZERO.US", "Zero position", "USD", "Active", "", 12, 0]]


def quote_row(symbol, *, price=..., currency=..., stamp=STAMP, failure="",
              qty=999, cost=777, note="Synthetic manual note"):
    if price is ...:
        price = PRICES.get(symbol, 50.0)
    if currency is ...:
        currency = HOLDINGS.get(symbol, {"currency": "USD"})["currency"]
    provider = "yahoo_chart" if symbol.endswith(".SR") else "eodhd"
    row = {"symbol": symbol, "name": "Synthetic holding " + symbol,
        "currency": currency, "current_price": price, "data_provider": provider,
        "last_updated_utc": stamp, "last_updated_riyadh": "2026-10-07T15:29:00+03:00",
        "warnings": "synthetic_quote_note" + ("; " + failure if failure else ""),
        "position_qty": qty, "avg_cost": cost, "position_cost": 9999,
        "position_value": 8888, "unrealized_pl": 7777, "unrealized_pl_pct": 6666,
        "buy_date": "2026-01-01", "decision": "HOLD", "recommendation": "HOLD",
        "target_weight": 12.5, "user_notes": note}
    engine._publish_price_acquisition(row, live_priced=True, fallback_source="",
        acquired_at=stamp, provider=provider, quote_asof="2026-10-07T12:28:00Z")
    projected = engine._strict_project_row(KEYS, row)
    display = engine._strict_project_row_display(HEADERS, KEYS, projected)
    return [display[header] for header in HEADERS]


def cell(row, header):
    return row[HEADERS.index(header)]


def rows_by_symbol(rows):
    return {cell(row, "Symbol"): row for row in rows}


class Response:
    def __init__(self, value):
        self.value = value

    def execute(self):
        return self.value


class Writer:
    def __init__(self, grid, prior=None):
        self.ledger = grid
        self.prior = [] if prior is None else copy.deepcopy(prior)
        self.ledger_reads = []
        self.published = []
        self.writes = []
        self.status_rows = []

    def _get_service(self):
        return self

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def get(self, **_kwargs):
        return Response({"values": [["Page"], ["My_Portfolio"]]})

    def update(self, **kwargs):
        self.status_rows.extend(copy.deepcopy(kwargs["body"]["values"]))
        return Response({})

    def read_values(self, _sid, page, *args, **_kwargs):
        if page == "_Portfolio_CostBasis":
            self.ledger_reads.append(args)
            if isinstance(self.ledger, Exception):
                raise self.ledger
            return copy.deepcopy(self.ledger)
        return [HEADERS] + copy.deepcopy(self.prior)

    def write_table(self, _sid, page, _start, headers, rows):
        assert page == "My_Portfolio" and headers == HEADERS and len(headers) == 122
        self.published = copy.deepcopy(rows)
        self.writes.append(copy.deepcopy(rows))
        return len(rows)


def run_portfolio(monkeypatch, rows=None, *, grid=None, prior=None, rebuild=True,
                  explicit_symbols=None, persistence=False, response_factory=None):
    writer = Writer(ledger() if grid is None else grid, prior)
    payloads = []

    class Backend:
        async def post_json(self, _endpoint, payload):
            payloads.append(copy.deepcopy(payload))
            selected = response_factory(payload, writer) if response_factory else rows
            return {"headers": HEADERS, "rows_matrix": copy.deepcopy(selected)}, None, 200

    for name, value in {
        "TFB_PORTFOLIO_REBUILD": "1" if rebuild else "0",
        "TFB_SYNC_STATUS_STAMP": "1", "TFB_SYNC_STATUS_STAMP_PAGES": "",
        "TFB_SYNC_STATUS_TRUTH": "0", "TFB_SYNC_WRITE_SENTINEL": "0",
        "TFB_SYNC_NAME_DEDUP_MODE": "off", "TFB_SYNC_ROW_ID_FIREWALL": "0",
        "TFB_SYNC_OHLC_LAKE": "0", "TFB_SYNC_OHLC_PREWRITE": "0",
        "TFB_SYNC_OHLC_READBACK": "0", "TFB_SYNC_FALSE_GREEN_SCREEN": "0",
        "TFB_SYNC_SYMBOL_BATCHING": "0", "TFB_SYNC_MARKET_SYMBOL_READBACK": "0",
        "TFB_SYNC_SYMBOL_PERSISTENCE": "1" if persistence else "0",
        "TFB_SYNC_KEEP_LAST_GOOD": "0", "TFB_SYNC_MIN_COVERAGE_PCT": "0",
        "TFB_SYNC_STRICT_MEMBERSHIP": "0", "TFB_SYNC_MANUAL_GUARD": "1",
        "TFB_SYNC_STALE_SKIP_RED": "1", "GITHUB_RUN_ID": "synthetic-active-holdings",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(sync, "_read_symbols", lambda *_args: list(explicit_symbols or []))
    monkeypatch.setattr(sync, "_utc_now", lambda: NOW)
    result = asyncio.run(sync._run_one_task(
        sync.TaskSpec("MY_PORTFOLIO", "My_Portfolio", "enriched", allow_empty_symbols=True),
        "offline", "A1", -1, False, False, Backend(), writer))
    return result, writer, payloads


def full_audit(rows):
    active, warnings = ledger_symbols(ledger())
    assert not warnings and set(active) == set(SYMBOLS)
    return audit_grid([HEADERS] + rows,
        Rule("My_Portfolio", len(SYMBOLS), 8, 100, 100, 100, True, True),
        HEADERS, NOW, active)


def assert_money(row, symbol, *, priced=True):
    hold = HOLDINGS[symbol]
    assert cell(row, "Position Qty") == hold["qty"]
    assert cell(row, "Avg Cost") == hold["cost"]
    position_cost = round(hold["qty"] * hold["cost"], 6)
    assert cell(row, "Position Cost") == position_cost
    if not priced:
        assert all(cell(row, name) in (None, "") for name in
            ("Position Value", "Unrealized P/L", "Unrealized P/L %"))
        return
    native_value = round(hold["qty"] * PRICES[symbol], 6)
    native_pl = round(native_value - position_cost, 6)
    assert cell(row, "Position Value") == native_value
    assert cell(row, "Unrealized P/L") == native_pl
    assert cell(row, "Unrealized P/L %") == round(native_pl / position_cost * 100, 6)


def test_row_four_ledger_uses_only_unique_active_positive_positions():
    writer = Writer(ledger())
    assert sync._read_cost_basis(writer, "offline") == HOLDINGS
    assert writer.ledger_reads == [("A1:EZ20050",)]


@pytest.mark.parametrize("cost_header,currency_header", [
    ("Buy Price", "Ccy"), ("Avg Cost", "Currency"), ("Average Cost", "Currency")])
def test_unambiguous_native_cost_and_currency_aliases(cost_header, currency_header):
    writer = Writer([["Synthetic title"], [],
        ["Symbol", "Status", "Shares", cost_header, currency_header],
        ["syn.us", "Active", "2.5", "8.125", "usd"]])
    assert sync._read_cost_basis(writer, "offline") == {
        "SYN.US": {"qty": 2.5, "cost": 8.125, "currency": "USD"}}
    assert writer.ledger_reads == [("A1:EZ20050",)]


def malformed_ledger(kind):
    grid = ledger()
    row = grid[4]
    if kind == "unreadable":
        return RuntimeError("synthetic unavailable ledger")
    if kind == "empty":
        return []
    if kind == "bounded":
        return grid + [[]] * (20050 - len(grid))
    if kind == "no_active":
        for position in grid[4:]:
            position[3] = "Closed"
        return grid
    if kind == "duplicate_active":
        grid.append(copy.deepcopy(row))
    elif kind == "bad_status":
        row[3] = "Unrecognized"
    elif kind == "blank_status":
        row[3] = ""
    elif kind == "missing_currency":
        row[2] = ""
    elif kind == "non_iso_currency":
        row[2] = "US dollars"
    elif kind == "negative_qty":
        row[6] = -1
    elif kind in {"nan_qty", "inf_qty"}:
        row[6] = float("nan" if kind == "nan_qty" else "inf")
    elif kind in {"zero_cost", "negative_cost", "nan_cost", "inf_cost"}:
        row[5] = {"zero_cost": 0, "negative_cost": -1,
            "nan_cost": float("nan"), "inf_cost": float("inf")}[kind]
    elif kind.startswith("ambiguous_"):
        alias, value = {
            "ambiguous_symbol": ("Ticker", "OTHER.US"),
            "ambiguous_qty": ("Quantity", 99),
            "ambiguous_cost": ("Avg Cost", 99),
            "ambiguous_currency": ("Currency", "SAR"),
            "ambiguous_status": ("Status", "Inactive"),
        }[kind]
        grid[3].append(alias)
        for position in grid[4:]:
            position.append(value)
    elif kind == "missing_cost_column":
        grid[3][5] = "Purchase memo"
    elif kind == "missing_currency_column":
        grid[3][2] = "Currency memo"
    else:
        raise AssertionError(kind)
    return grid


@pytest.mark.parametrize("kind", ["unreadable", "empty", "bounded", "no_active",
    "duplicate_active", "bad_status", "blank_status", "missing_currency",
    "non_iso_currency", "negative_qty", "nan_qty", "inf_qty", "zero_cost",
    "negative_cost", "nan_cost", "inf_cost", "ambiguous_symbol", "ambiguous_qty",
    "ambiguous_cost", "ambiguous_currency", "ambiguous_status",
    "missing_cost_column", "missing_currency_column"])
def test_invalid_or_empty_ledger_fails_before_any_backend_fetch_or_table_write(kind, monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    grid = malformed_ledger(kind)
    parser_writer = Writer(grid)
    assert sync._read_cost_basis(parser_writer, "offline") == {}
    assert parser_writer.ledger_reads == [("A1:EZ20050",)]
    result, writer, payloads = run_portfolio(monkeypatch,
        [quote_row(symbol) for symbol in SYMBOLS], grid=grid,
        explicit_symbols=["UNTRUSTED.US"])
    assert not payloads and not writer.writes and result.rows_written == 0
    assert writer.ledger_reads == [("A1:EZ20050",)]
    assert writer.status_rows and "| data=PARTIAL" in writer.status_rows[-1][3]


@pytest.mark.parametrize("mode", ["off", "observe", "enforce"])
def test_real_122_runner_requests_active_ledger_and_keeps_native_units(mode, monkeypatch, caplog):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", mode)
    caplog.set_level(logging.INFO, logger=sync.logger.name)
    defaults = ["2222.SR", "AAPL", "MSFT", "QQQ", "GC=F"]
    inputs = [quote_row(symbol) for symbol in reversed(SYMBOLS)]
    unchanged = copy.deepcopy(inputs)

    def backend_response(payload, _writer):
        # Reproduces the route's old emergency-default behavior on symbols=[].
        return inputs if payload["symbols"] else [quote_row(symbol) for symbol in defaults]

    result, writer, payloads = run_portfolio(monkeypatch,
        response_factory=backend_response, explicit_symbols=[])
    assert result.status == "success" and result.rows_written == len(SYMBOLS)
    assert payloads and all(payload["symbols"] == SYMBOLS and
        payload["tickers"] == SYMBOLS and payload["limit"] == len(SYMBOLS)
        for payload in payloads)
    assert result.symbols_requested == len(SYMBOLS)
    assert writer.ledger_reads == [("A1:EZ20050",)]
    assert inputs == unchanged  # The shared backend matrix was not mutated.
    output = rows_by_symbol(writer.published)
    assert set(output) == set(SYMBOLS) and len(writer.published) == len(SYMBOLS)
    for original in inputs:
        symbol = cell(original, "Symbol")
        assert_money(output[symbol], symbol)
        for name in ("Current Price", "Currency", "Data Provider", "Last Updated (UTC)",
                "Warnings", "Investor Decision", "User Notes", "Buy Date", "Target Weight %"):
            assert cell(output[symbol], name) == cell(original, name)
    assert sync._page_fresh_fetch_metrics(result) == (3, 3, 100.0)
    stamp = writer.status_rows[-1]
    assert "acquired=3/3 acquisition=COMPLETE" in stamp[3]
    assert "| data=COMPLETE" in stamp[3] and stamp[6] == 3
    sync._apply_stale_skip_escalation([result], writer, "offline")
    assert "fresh_rows=3 requested_rows=3 fresh_pct=100.0000" in caplog.text
    audit = full_audit(writer.published)
    assert audit.status == "PASS" and audit.fresh == 3
    assert not audit.missing_qty and not audit.missing_cost


@pytest.mark.parametrize("bad_response", ["defaults", "foreign_extra", "duplicate", "missing"])
def test_final_portfolio_identity_is_complete_exact_and_unique_even_strict_membership_off(
        bad_response, monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    rows = [quote_row(symbol) for symbol in SYMBOLS]
    if bad_response == "defaults":
        rows = [quote_row(symbol) for symbol in ["2222.SR", "AAPL", "MSFT", "QQQ", "GC=F"]]
    elif bad_response == "foreign_extra":
        rows.append(quote_row("FOREIGN.US"))
    elif bad_response == "duplicate":
        rows.append(copy.deepcopy(rows[0]))
    else:
        rows.pop()
    result, writer, payloads = run_portfolio(monkeypatch, rows)
    assert payloads and payloads[0]["symbols"] == SYMBOLS
    assert not writer.writes and result.rows_written == 0
    assert writer.ledger_reads == [("A1:EZ20050",)]


@pytest.mark.parametrize("currency", ["SAR", "", None, "US dollars"])
def test_quote_native_currency_must_match_ledger_before_any_publication(currency, monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    inputs = [quote_row(symbol, currency=currency if symbol == "SYNA.US" else ...)
        for symbol in SYMBOLS]
    original = copy.deepcopy(inputs)
    result, writer, payloads = run_portfolio(monkeypatch, inputs)
    assert payloads and not writer.writes and result.rows_written == 0
    assert inputs == original


@pytest.mark.parametrize("price", [None, 0, -1])
def test_missing_or_invalid_price_preserves_holdings_but_clears_derived_values(price, monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    rows = [quote_row(symbol, price=price if symbol == "SYNA.US" else ...,
        failure="fetch_failed:synthetic" if symbol == "SYNA.US" else "")
        for symbol in SYMBOLS]
    result, writer, payloads = run_portfolio(monkeypatch, rows)
    assert payloads and result.rows_written == 3
    output = rows_by_symbol(writer.published)
    assert_money(output["SYNA.US"], "SYNA.US", priced=False)
    assert cell(output["SYNA.US"], "Current Price") == price
    assert "fetch_failed:synthetic" in cell(output["SYNA.US"], "Warnings")
    assert sync._page_fresh_fetch_metrics(result) == (2, 3, pytest.approx(200 / 3))
    assert "acquired=2/3 acquisition=PARTIAL" in writer.status_rows[-1][3]
    audit = full_audit(writer.published)
    assert audit.fresh == 2 and audit.status == "FAIL"


@pytest.mark.parametrize("price", [float("nan"), float("inf"), float("-inf")])
def test_injector_does_not_form_money_values_from_nonfinite_price(price):
    rows = [quote_row("SYNA.US")]
    rows[0][HEADERS.index("Current Price")] = price
    result, count = sync._inject_portfolio_holdings(HEADERS, rows, HOLDINGS)
    assert count == 1
    assert_money(result[0], "SYNA.US", priced=False)


def test_actual_persistence_restoration_gets_current_holdings_from_same_snapshot(monkeypatch, caplog):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    caplog.set_level(logging.INFO, logger=sync.logger.name)
    restored_symbol = "SYNA.US"
    predecessor = quote_row(restored_symbol, stamp="2026-10-07T11:00:00Z",
        qty=1, cost=1, note="Preserved synthetic operator note")
    old = copy.deepcopy(predecessor)
    rows = [quote_row(symbol) for symbol in SYMBOLS if symbol != restored_symbol]

    def backend_response(_payload, writer):
        # A concurrent ledger edit after the verified read must not supply a
        # different quantity to final reinjection or a different denominator.
        writer.ledger[4][6] = 12345
        return rows

    result, writer, payloads = run_portfolio(monkeypatch, prior=[predecessor],
        persistence=True, response_factory=backend_response)
    assert payloads[0]["symbols"] == SYMBOLS and result.rows_written == 3
    assert writer.ledger_reads == [("A1:EZ20050",)]
    assert result._stamp_meta["persist_restored"] == 1
    restored = rows_by_symbol(writer.published)[restored_symbol]
    assert_money(restored, restored_symbol)
    for name in ("Current Price", "Currency", "Data Provider", "Last Updated (UTC)",
            "Investor Decision", "User Notes", "Buy Date", "Target Weight %"):
        assert cell(restored, name) == cell(old, name)
    assert "synthetic_quote_note" in cell(restored, "Warnings")
    assert "acquisition_status:preserved" in cell(restored, "Warnings")
    assert predecessor == old
    assert sync._page_fresh_fetch_metrics(result) == (2, 3, pytest.approx(200 / 3))
    census = acquisition_census(HEADERS, writer.published, now=NOW,
        max_age_seconds=8 * 3600, requested=SYMBOLS, symbol_key=sync.canonicalize_symbol)
    assert census.successful == set(SYMBOLS) - {restored_symbol}
    assert "acquired=2/3 acquisition=PARTIAL" in writer.status_rows[-1][3]
    sync._apply_stale_skip_escalation([result], writer, "offline")
    assert "fresh_rows=2 requested_rows=3 fresh_pct=66.6667" in caplog.text
    audit = full_audit(writer.published)
    assert audit.fresh == 2 and audit.status == "FAIL"
    assert not audit.missing_portfolio and not audit.missing_qty and not audit.missing_cost


def test_rebuild_off_preserves_explicit_caller_roster_and_manual_positions(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    requested = ["EXPLICIT.US"]
    inputs = [quote_row(requested[0], qty=9, cost=4)]
    result, writer, payloads = run_portfolio(monkeypatch, inputs,
        grid=RuntimeError("Must not consult ledger for explicit rebuild-off caller"),
        rebuild=False, explicit_symbols=requested)
    assert payloads[0]["symbols"] == requested and result.rows_written == 1
    assert not writer.ledger_reads and writer.published == inputs


@pytest.mark.parametrize("strict", ["0", "1"])
def test_noncanonical_known_registry_identity_is_unproven_before_fetch(strict, monkeypatch):
    grid = ledger()
    grid[4][0] = "BNY"  # Existing approved registry maps this to BNY.US.
    assert sync.canonicalize_symbol("BNY") == "BNY.US"
    monkeypatch.setattr(sync, "_strict_membership_enabled", lambda: strict == "1")
    result, writer, payloads = run_portfolio(monkeypatch,
        [quote_row(symbol) for symbol in SYMBOLS], grid=grid)
    assert not payloads and not writer.writes and result.status == "failed"
    assert writer.ledger_reads == [("A1:EZ20050",)]


def test_canonical_active_cohort_survives_production_strict_membership(monkeypatch):
    monkeypatch.setattr(sync, "_strict_membership_enabled", lambda: True)
    result, writer, payloads = run_portfolio(monkeypatch,
        [quote_row(symbol) for symbol in SYMBOLS])
    assert result.status == "success" and payloads[0]["symbols"] == SYMBOLS
    assert set(rows_by_symbol(writer.published)) == set(SYMBOLS)
    assert sync._page_fresh_fetch_metrics(result) == (3, 3, 100.0)
    for symbol, row in rows_by_symbol(writer.published).items():
        assert_money(row, symbol)


def test_generic_page_readback_cannot_replace_verified_active_fetch_cohort(monkeypatch):
    # The market readback scope is configurable. Its existing rows must not
    # outrank the authoritative active portfolio snapshot even if PF is added.
    monkeypatch.setattr(sync, "_market_symbol_readback_enabled", lambda: True)
    monkeypatch.setattr(sync, "_market_readback_pages", lambda: {sync._guard_norm("My_Portfolio")})
    result, writer, payloads = run_portfolio(monkeypatch,
        [quote_row(symbol) for symbol in SYMBOLS], prior=[quote_row("DEFAULT.US")])
    assert payloads[0]["symbols"] == SYMBOLS and payloads[0]["tickers"] == SYMBOLS
    assert result.status == "success" and result.rows_written == len(SYMBOLS)
    assert set(rows_by_symbol(writer.published)) == set(SYMBOLS)


@pytest.mark.parametrize("sar_price", [40.78125, None, 0, float("inf")])
def test_explicit_sar_columns_are_separate_from_native_positions_and_need_price_fx(sar_price):
    headers = HEADERS + ["Price SAR", "MV SAR", "Cost SAR", "P/L SAR"]
    rows = [quote_row("SYNA.US") + [sar_price, 999, 999, 999]]
    original = copy.deepcopy(rows)
    output, count = sync._inject_portfolio_holdings(headers, rows, HOLDINGS)
    assert count == 1 and rows == original
    assert_money(output[0], "SYNA.US")
    assert output[0][headers.index("Price SAR")] == sar_price
    explicit = [output[0][headers.index(name)] for name in ("MV SAR", "Cost SAR", "P/L SAR")]
    if sar_price == 40.78125:
        native = [cell(output[0], name) for name in
            ("Position Value", "Position Cost", "Unrealized P/L")]
        assert explicit == [round(value * 3.75, 6) for value in native]
    else:
        assert explicit == ["", "", ""]


def test_ledger_reader_does_not_truncate_later_active_holding_to_old_200_row_cap():
    grid = ledger()
    last = grid.pop(4)
    grid.extend([[]] * (450 - len(grid)))
    grid.append(last)
    writer = Writer(grid)
    assert sync._read_cost_basis(writer, "offline") == HOLDINGS
    assert writer.ledger_reads == [("A1:EZ20050",)]


def test_rebuild_off_without_explicit_cohort_never_fetches_emergency_defaults(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    result, writer, payloads = run_portfolio(monkeypatch,
        [quote_row("DEFAULT.US")], rebuild=False, explicit_symbols=[])
    assert not payloads and not writer.writes and result.rows_written == 0


def test_active_ledger_larger_than_request_ceiling_is_not_partially_requested(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    monkeypatch.setattr(sync, "_request_limit_ceiling", lambda: 2)
    result, writer, payloads = run_portfolio(monkeypatch,
        [quote_row(symbol) for symbol in SYMBOLS])
    assert writer.ledger_reads == [("A1:EZ20050",)]
    assert not payloads and not writer.writes and result.rows_written == 0


@pytest.mark.parametrize("qty,cost", [(1e308, 2.0), (1e-300, 1e-300)])
def test_native_position_cost_overflow_or_underflow_cannot_prove_ledger(qty, cost, monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    grid = ledger()
    grid[4][6], grid[4][5] = qty, cost
    assert sync._read_cost_basis(Writer(grid), "offline") == {}
    result, writer, payloads = run_portfolio(monkeypatch,
        [quote_row(symbol) for symbol in SYMBOLS], grid=grid)
    assert not payloads and not writer.writes and result.rows_written == 0


@pytest.mark.parametrize("qty,cost,price", [
    (2.5, 8.125, 1e308),  # finite quote overflows native position value
    (1.0, 1e-300, 1e308),  # value remains finite; percentage points overflow
])
def test_finite_quote_cannot_publish_nonfinite_position_money(qty, cost, price, monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    grid = ledger()
    grid[4][6], grid[4][5] = qty, cost
    rows = [quote_row(symbol, price=price if symbol == "SYNA.US" else ...)
        for symbol in SYMBOLS]
    result, writer, payloads = run_portfolio(monkeypatch, rows, grid=grid)
    assert payloads and payloads[0]["symbols"] == SYMBOLS
    assert not writer.writes and result.rows_written == 0


def test_header_offset_native_unit_cost_ignores_aggregate_totals_and_inactive_duplicate():
    grid = ledger()
    grid[0] = ["Symbol", "Shares"]  # earlier metadata cannot replace the real header
    grid[1] = ["DECOY.US", 99]
    grid[3].extend(["Aggregate Cost Basis", "Cost SAR", "Position Cost SAR"])
    for position in grid[4:]:
        position.extend([123456, 987654, 111111])
    inactive_duplicate = copy.deepcopy(grid[4])
    inactive_duplicate[3] = "Inactive"
    inactive_duplicate[5], inactive_duplicate[6] = 99999, 99999
    grid.append(inactive_duplicate)
    writer = Writer(grid)
    assert sync._read_cost_basis(writer, "offline") == HOLDINGS
    assert writer.ledger_reads == [("A1:EZ20050",)]


def test_unpriced_response_still_requires_native_quote_currency(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    rows = [quote_row(symbol, price=None if symbol == "SYNA.US" else ...,
        currency=None if symbol == "SYNA.US" else ...,
        failure="fetch_failed:synthetic" if symbol == "SYNA.US" else "")
        for symbol in SYMBOLS]
    result, writer, payloads = run_portfolio(monkeypatch, rows)
    assert payloads and not writer.writes and result.rows_written == 0


@pytest.mark.parametrize("misleading_cost_header", ["Cost Basis", "Aggregate Cost Basis", "Price"])
def test_aggregate_or_ambiguous_price_column_cannot_replace_native_unit_cost(misleading_cost_header):
    grid = ledger()
    grid[3][5] = misleading_cost_header
    assert sync._read_cost_basis(Writer(grid), "offline") == {}
