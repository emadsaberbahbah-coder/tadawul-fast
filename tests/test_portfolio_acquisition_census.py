"""Portfolio acquisitions use a proven active ledger, never written count."""
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
SYMBOLS = ["SYN" + letter + ".US" for letter in "ABCDE"]
HEADERS = get_sheet_headers("My_Portfolio")
KEYS = get_sheet_keys("My_Portfolio")


def ledger():
    # Real ledger layout: title/metadata precede row-four header. No private data.
    return [["Synthetic ledger"], [], ["Synthetic metadata"],
            ["Symbol", "Name", "Ccy", "Status", "Buy Date", "Buy Price", "Shares"],
            *[[symbol, "Synthetic holding", "USD", "Active", "2026-01-01", 22, 11]
              for symbol in SYMBOLS],
            [SYMBOLS[0], "Synthetic closed lot", "USD", "Inactive", "2026-01-01", 23, 2],
            *[["OLD" + str(i) + ".US", "Closed synthetic lot", "USD", "Inactive", "", 20, 1]
              for i in range(34)],
            ["ZERO.US", "Zero quantity", "USD", "Active", "", 20, 0],
            ["SOLD.US", "Sold lot", "USD", "Sold", "", 20, 3],
            ["CLOSED.US", "Closed lot", "USD", "Closed", "", 20, 3]]


def provider_row(symbol, failure="", index=0):
    provider = "eodhd" if index < 3 else "yahoo_chart"
    row = {"symbol": symbol, "name": "Synthetic holding", "currency": "USD", "current_price": 30 + index,
           "data_provider": provider, "last_updated_utc": STAMP,
           "last_updated_riyadh": "2026-10-07T15:29:00+03:00", "warnings": "",
           "position_qty": 11, "avg_cost": 22, "decision": "HOLD",
           "user_notes": "Synthetic manual note", "recommendation": "HOLD"}
    if failure == "failed":
        row["warnings"] = "fetch_failed:HTTP402"
    elif failure == "unknown":
        row["last_updated_utc"] = "unavailable"
        row["last_updated_riyadh"] = "unavailable"
    elif failure == "stale":
        row["last_updated_utc"] = "2026-10-01T12:29:00Z"
    engine._publish_price_acquisition(row, live_priced=failure != "preserved",
        fallback_source="snapshot" if failure == "preserved" else "",
        acquired_at=row["last_updated_utc"], provider=provider, quote_asof="")
    projected = engine._strict_project_row(KEYS, row)
    display = engine._strict_project_row_display(HEADERS, KEYS, projected)
    return [display[header] for header in HEADERS]


class Response:
    def __init__(self, value):
        self.value = value

    def execute(self):
        return self.value


class Writer:
    def __init__(self, grid, prior=None):
        self.ledger = grid
        self.prior = prior or [provider_row(s, index=i) for i, s in enumerate(SYMBOLS)]
        self.published = []
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

    def read_values(self, _sid, page, *_args, **_kwargs):
        if page == "_Portfolio_CostBasis":
            if isinstance(self.ledger, Exception):
                raise self.ledger
            return copy.deepcopy(self.ledger)
        return [HEADERS] + copy.deepcopy(self.prior)

    def write_table(self, _sid, _page, _start, headers, rows):
        assert headers == HEADERS and len(headers) == 122
        self.published = copy.deepcopy(rows)
        return len(rows)


def run_portfolio(monkeypatch, rows, *, grid=None, backend_error=False, prior=None,
                  explicit_symbols=None):
    writer = Writer(ledger() if grid is None else grid, prior)
    payloads = []

    class Backend:
        async def post_json(self, _endpoint, payload):
            payloads.append(copy.deepcopy(payload))
            if backend_error:
                return None, "synthetic outage", 503
            return {"headers": HEADERS, "rows_matrix": copy.deepcopy(rows)}, None, 200

    # Exercise the repaired page-driven entry and real manual-cell guard.
    monkeypatch.setenv("TFB_PORTFOLIO_REBUILD", "0" if explicit_symbols else "1")
    monkeypatch.setenv("TFB_SYNC_STATUS_STAMP", "1")
    monkeypatch.setenv("TFB_SYNC_STATUS_STAMP_PAGES", "")
    monkeypatch.setenv("TFB_SYNC_STATUS_TRUTH", "0")
    monkeypatch.setenv("TFB_SYNC_WRITE_SENTINEL", "0")
    monkeypatch.setenv("TFB_SYNC_NAME_DEDUP_MODE", "off")
    monkeypatch.setenv("TFB_SYNC_OHLC_LAKE", "0")
    monkeypatch.setenv("TFB_SYNC_OHLC_PREWRITE", "0")
    monkeypatch.setenv("TFB_SYNC_OHLC_READBACK", "0")
    monkeypatch.setenv("TFB_SYNC_STALE_SKIP_RED", "1")
    monkeypatch.setenv("GITHUB_RUN_ID", "synthetic-portfolio-run")
    monkeypatch.setattr(sync, "_read_symbols", lambda *_args: explicit_symbols or [])
    monkeypatch.setattr(sync, "_utc_now", lambda: NOW)
    result = asyncio.run(sync._run_one_task(
        sync.TaskSpec("MY_PORTFOLIO", "My_Portfolio", "enriched", allow_empty_symbols=True),
        "offline", "A1", -1, False, False, Backend(), writer))
    expected = explicit_symbols or SYMBOLS
    assert all(payload["symbols"] == expected for payload in payloads)
    return result, writer


@pytest.mark.parametrize("mode", ["off", "observe", "enforce"])
@pytest.mark.parametrize("failure,fresh", [("", 5), ("failed", 4),
    ("preserved", 4), ("unknown", 4), ("stale", 4), ("missing", 4)])
def test_real_page_driven_portfolio_census_status_and_audit(mode, failure, fresh,
                                                         monkeypatch, caplog):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", mode)
    rows = [provider_row(symbol, failure if i == 0 else "", i)
            for i, symbol in enumerate(SYMBOLS)]
    if failure == "missing":
        rows = rows[1:]
    incoming = copy.deepcopy(rows)
    caplog.set_level(logging.INFO, logger=sync.logger.name)
    result, writer = run_portfolio(monkeypatch, rows)
    assert result.status == "success" and result.symbols_requested == 5
    published = {row[HEADERS.index("Symbol")]: row for row in writer.published}
    for original in incoming:
        output = published[original[HEADERS.index("Symbol")]]
        for name in ("Current Price", "Currency", "Position Qty", "Avg Cost",
                     "Data Provider", "Last Updated (UTC)", "Investor Decision", "User Notes"):
            assert output[HEADERS.index(name)] == original[HEADERS.index(name)]
    assert sync._page_fresh_fetch_metrics(result) == (fresh, 5, fresh * 20.0)
    census = acquisition_census(HEADERS, writer.published, now=NOW,
        max_age_seconds=8 * 3600, requested=SYMBOLS, symbol_key=sync.canonicalize_symbol)
    assert len(census.requested) == 5 and len(census.successful) == fresh
    assert result._stamp_meta["acquisition_unknown"] == (1 if failure == "unknown" else 0)
    if failure == "missing":
        assert result._stamp_meta["persist_restored"] == 1
    stamp = writer.status_rows[-1]
    assert stamp[0] == "My_Portfolio" and stamp[6] == 5
    assert f"[STATUS-STAMP v{sync.SCRIPT_VERSION}]" in stamp[3]
    assert "run=synthetic-portfolio-run" in stamp[3]
    assert f"acquired={fresh}/5" in stamp[3]
    assert "acquisition=" + ("COMPLETE" if fresh == 5 else "PARTIAL") in stamp[3]
    assert "| data=" + ("COMPLETE" if fresh == 5 else "PARTIAL") in stamp[3]
    if mode == "enforce":
        assert "policy_data=" + ("COMPLETE" if fresh == 5 else "PARTIAL") in stamp[3]
        assert sync._uv_page_state(result) == ("OK" if fresh == 5 else "STALE_COV", fresh * 20.0)
    else:
        # Existing policy modes still expose incomplete response coverage;
        # priced failure truth is factual regardless of the rollout mode.
        assert stamp[2] == ("PARTIAL_FRESH" if failure == "missing" else "SUCCESS")
        assert "policy_data=" + ("PARTIAL" if failure == "missing" else "COMPLETE") in stamp[3]
        assert sync._uv_page_state(result) == (
            "STALE_COV" if failure == "missing" else "OK",
            80.0 if failure == "missing" else 100.0)
    sync._apply_stale_skip_escalation([result], writer, "offline")
    assert f"fresh_rows={fresh} requested_rows=5 fresh_pct={fresh * 20.0:.4f}" in caplog.text
    active, warnings = ledger_symbols(ledger())
    assert not warnings and active == SYMBOLS
    audit = audit_grid([HEADERS] + writer.published,
        Rule("My_Portfolio", 5, 8, 100, 100, 100, True, True), HEADERS, NOW, active)
    assert audit.fresh == fresh
    assert audit.status == ("PASS" if fresh == 5 else "FAIL")
    if failure == "missing":
        assert not audit.missing_portfolio  # Real persistence retains the failed acquisition as preserved.
        assert "acquisition_status:preserved" in published[SYMBOLS[0]][HEADERS.index("Warnings")]
    else:
        assert audit.unique == 5 and audit.fresh_pct == fresh * 20.0


@pytest.mark.parametrize("bad_ledger", [[], [["Symbol", "Shares"], ["SYN.US", 1]],
    [["Symbol", "Status"], ["SYN.US", "unrecognized"]],
    [["Symbol", "Status"], ["SYN.US", ""]],
    [["Symbol", "Symbol", "Status"], ["FIRST.US", "SECOND.US", "Active"]],
    [["Symbol", "Status", "Shares", "Quantity"], ["SYN.US", "Active", 0, 1]],
    [["Symbol", "Status", "Quantity", "Shares"], ["SYN.US", "Active", 1, 0]],
    [["Symbol", "Status", "Shares", "Shares"], ["SYN.US", "Active", 0, 1]],
    [["Symbol", "Status", "Shares", "Shares"], ["SYN.US", "Active", 1, 0]],
    [["Symbol", "Status", "Qty"], ["SYN.US", "Active", 0]],
    RuntimeError("synthetic unavailable ledger")])
def test_unproven_ledger_never_uses_successful_response_denominator(bad_ledger, monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    result, writer = run_portfolio(monkeypatch,
        [provider_row(symbol, index=i) for i, symbol in enumerate(SYMBOLS)], grid=bad_ledger)
    assert result.status == "failed" and result.rows_written == 0 and not writer.published
    assert sync._page_fresh_fetch_metrics(result) == (None, 0, None)
    assert "acquired=unknown/0 acquisition=UNKNOWN" in writer.status_rows[-1][3]
    assert "| data=PARTIAL" in writer.status_rows[-1][3]


def test_missing_fetch_retains_proven_ledger_denominator_without_publication(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    result, writer = run_portfolio(monkeypatch, [], backend_error=True)
    assert result.status == "failed" and not writer.published
    assert sync._page_fresh_fetch_metrics(result) == (None, 5, None)
    assert "acquired=unknown/5 acquisition=UNKNOWN" in writer.status_rows[-1][3]
    assert "| data=PARTIAL" in writer.status_rows[-1][3]


def test_proven_empty_active_ledger_cannot_certify_existing_rows(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    result, writer = run_portfolio(monkeypatch,
        [provider_row(symbol, index=i) for i, symbol in enumerate(SYMBOLS)],
        grid=[["Symbol", "Status", "Shares"], ["OLD.US", "Inactive", 1]])
    assert result.status == "failed" and not writer.published
    assert sync._page_fresh_fetch_metrics(result) == (None, 0, None)
    assert "acquired=unknown/0 acquisition=UNKNOWN" in writer.status_rows[-1][3]


def test_bounded_out_ledger_does_not_prove_a_partial_cohort():
    assert sync._read_portfolio_acquisition_symbols(Writer([[]] * 20050), "offline") is None


def test_foreign_success_cannot_inflate_active_portfolio_acquisition(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    result, _writer = run_portfolio(monkeypatch,
        [provider_row(symbol, index=i) for i, symbol in enumerate(SYMBOLS[1:])]
        + [provider_row("FOREIGN.US")])
    assert result.status == "failed" and result.rows_written == 0
    assert sync._page_fresh_fetch_metrics(result) == (None, 5, None)


def test_validated_ledger_header_cannot_be_replaced_by_earlier_partial_header():
    writer = Writer([["Symbol", "Shares"], ["DECOY.US", 1], [],
        ["Symbol", "Shares", "Average Cost", "Status"],
        ["ACTIVE.US", 1, 100, "Active"], ["OLD.US", 1, 100, "Inactive"]])
    assert sync._read_portfolio_acquisition_symbols(writer, "offline") == ["ACTIVE.US"]


def test_explicit_portfolio_request_keeps_existing_requested_cohort(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    result, writer = run_portfolio(monkeypatch,
        [provider_row(symbol, index=i) for i, symbol in enumerate(SYMBOLS[:2])],
        explicit_symbols=SYMBOLS[:2])
    assert result.symbols_requested == 2
    assert sync._page_fresh_fetch_metrics(result) == (2, 2, 100.0)
    assert "acquired=2/2 acquisition=COMPLETE" in writer.status_rows[-1][3]


def test_trusted_active_ledger_repairs_blank_backend_holdings_before_manual_guard(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "observe")
    rows = [provider_row(symbol, index=i) for i, symbol in enumerate(SYMBOLS)]
    rows[0][HEADERS.index("Position Qty")] = ""
    result, writer = run_portfolio(monkeypatch, rows)
    assert result.status == "success" and result.rows_written == 5
    assert writer.published[0][HEADERS.index("Position Qty")] == 11
    assert "leg=success written=5" in writer.status_rows[-1][3]
    assert "| data=COMPLETE" in writer.status_rows[-1][3]
