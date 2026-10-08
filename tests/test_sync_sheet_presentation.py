"""Synthetic regressions at the sync runner's final Sheets publication boundary."""
from __future__ import annotations

import asyncio
import copy
from datetime import datetime, timezone

import pytest

from core.data_validity import row_acquisition
from core.sheets.schema_registry import get_sheet_headers
from integrations import google_sheets_service as sheets_sdk
from scripts import run_dashboard_sync as sync


NOW = datetime(2026, 10, 9, tzinfo=timezone.utc)
MARKET_PAGES = ("Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX")
HEADERS = get_sheet_headers("Commodities_FX")


def source_row(symbol="SI=F", *, proven=False):
    margin = 0.009 if proven else 0.9
    warnings = (
        "prior_nonacquisition_note; margin_publish:profit_margin:pts:observe; "
        "acquisition_status:success; acquisition_provider:yahoo_chart; "
        "acquisition_acquired_at:2026-10-08T23:59:00Z; "
        "acquisition_quote_asof:2026-10-08T20:00:00Z"
    )
    if proven:
        warnings += "; sheet_margin_unit:profit_margin:fraction:0.009"
    return {
        "Symbol": symbol, "Name": "Synthetic " + symbol, "Asset Class": "Commodity",
        "Current Price": "2600.12500", "Currency": "USD", "Profit Margin": margin,
        "Horizon Days": 365, "Invest Period Label": "3M",
        "Forecast Price 12M": "3120.15", "Expected ROI 12M": -0.2,
        "Overall Score": 79.125, "Quality Score": 73.5,
        "Data Provider": "yahoo_chart", "Last Updated (UTC)": "2026-10-08T23:59:00Z",
        "Last Updated (Riyadh)": "2026-10-09T02:59:00+03:00", "Warnings": warnings,
    }


def matrix(rows, headers=HEADERS):
    return [[row.get(header, "") for header in headers] for row in rows]


class FakeSheetsAPI:
    """Record the real direct SDK values.update arguments, without credentials."""

    def __init__(self, seeded_grid=None):
        self.requests = []
        self.grid = copy.deepcopy(seeded_grid or [])

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def update(self, **kwargs):
        self.requests.append(copy.deepcopy(kwargs))
        return self

    def batchUpdate(self, **kwargs):
        self.requests.append(copy.deepcopy(kwargs))
        return self

    def _apply_values(self, values):
        for row_i, row in enumerate(values):
            while len(self.grid) <= row_i:
                self.grid.append([])
            while len(self.grid[row_i]) < len(row):
                self.grid[row_i].append("")
            for column_i, value in enumerate(row):
                # Sheets RAW values.update skips nulls; only empty strings
                # remove a prior on-sheet quantity.
                if value is not None:
                    self.grid[row_i][column_i] = value
        return {"updatedRows": len(values), "updatedCells": sum(len(row) for row in values)}

    def execute(self):
        body = self.requests[-1]["body"]
        if "data" in body:
            return {"responses": [self._apply_values(item["values"]) for item in body["data"]]}
        return self._apply_values(body["values"])


@pytest.fixture(autouse=True)
def offline_modes(monkeypatch):
    modes = {
        "TFB_MARKET_SYMBOL_READBACK": "0", "TFB_SYNC_SYMBOL_BATCH_SIZE": "0",
        "TFB_SYNC_SYMBOL_PERSISTENCE": "1", "TFB_SYNC_KEEP_LAST_GOOD": "1",
        "TFB_SYNC_PERSISTENCE_HARD": "0", "TFB_SYNC_ROW_ID_FIREWALL": "0",
        "TFB_SYNC_NAME_DEDUP_MODE": "off", "TFB_SYNC_OHLC_LAKE": "0",
        "TFB_SYNC_FALSE_GREEN_SCREEN": "0", "TFB_SYNC_STATUS_STAMP": "0",
        "TFB_SYNC_FETCHFAIL_TRUTH": "off", "TFB_SYNC_IDENTITY_TRIPWIRE": "0",
        "TFB_SYNC_COHERENCE_TRIPWIRE": "0", "TFB_SYNC_OHLC_PREWRITE": "0",
        "TFB_SYNC_OHLC_FILLGUARD": "off", "TFB_SYNC_OHLC_READBACK": "0",
        "TFB_SYNC_IDFW_RUNLOG": "0",
        "TFB_SVC_CAPACITY_GUARD": "0",
    }
    for name, value in modes.items():
        monkeypatch.setenv(name, value)


def assert_presented(displayed, original, *, proven=False):
    assert displayed["Profit Margin"] == (0.009 if proven else "")
    assert displayed["Invest Period Label"] == "1Y"
    assert displayed["Expected ROI 12M"] == ""
    assert "sheet_tuple_conflict:expected_roi_12m" in displayed["Warnings"]
    if proven:
        assert "sheet_margin_unit:profit_margin:fraction:0.009" in displayed["Warnings"]
    else:
        assert "sheet_margin_unknown:profit_margin" in displayed["Warnings"]
    for field in (
        "Symbol", "Current Price", "Currency", "Forecast Price 12M", "Overall Score",
        "Quality Score", "Data Provider", "Last Updated (UTC)", "Last Updated (Riyadh)",
    ):
        assert displayed[field] == original[field]
    assert "acquisition_acquired_at:2026-10-08T23:59:00Z" in displayed["Warnings"]
    assert "acquisition_quote_asof:2026-10-08T20:00:00Z" in displayed["Warnings"]
    assert "prior_nonacquisition_note" in displayed["Warnings"]


@pytest.mark.parametrize("page", MARKET_PAGES)
@pytest.mark.parametrize("proven", [False, True])
def test_direct_sdk_market_payload_is_presented_copy_only_and_idempotent(page, proven):
    headers = get_sheet_headers(page)
    source = source_row(proven=proven)
    rows = matrix([source], headers)
    original = copy.deepcopy((headers, rows, source))
    api = FakeSheetsAPI([headers] + rows)
    writer = sync.SheetsWriter()
    writer._service = api
    assert writer.write_table("SYNTHETIC_BOOK", page, "A5", headers, rows) == 1
    request = api.requests[-1]
    assert request["valueInputOption"] == "RAW"
    assert request["range"] == page + "!A5"
    payload = request["body"]["values"]
    assert payload[0] == headers and len(payload[1]) == len(headers) == 115
    assert_presented(dict(zip(headers, payload[1])), source, proven=proven)
    assert_presented(dict(zip(headers, api.grid[1])), source, proven=proven)
    assert (headers, rows, source) == original
    writer.write_table("SYNTHETIC_BOOK", page, "A5", payload[0], payload[1:])
    assert api.requests[-1]["body"]["values"] == payload
    assert api.grid == payload


def test_direct_sdk_presentation_runs_after_fillguard_restoration(monkeypatch):
    source = source_row()
    restored = matrix([source])
    original = copy.deepcopy(restored)
    incoming = matrix([dict(source, **{"Profit Margin": None, "Invest Period Label": "1Y"})])
    incoming_original = copy.deepcopy(incoming)
    calls = []

    def restore_after_guard(headers, rows):
        calls.append((headers, copy.deepcopy(rows)))
        return copy.deepcopy(restored), None

    monkeypatch.setattr(sync, "_ohlc_fill_guard_apply", restore_after_guard)
    api = FakeSheetsAPI()
    writer = sync.SheetsWriter()
    writer._service = api
    writer.write_table("SYNTHETIC_BOOK", "Commodities_FX", "A5", HEADERS, incoming)
    assert len(calls) == 1
    assert_presented(dict(zip(HEADERS, api.requests[-1]["body"]["values"][1])), source)
    assert incoming == incoming_original and restored == original


@pytest.mark.parametrize("use_batch_update", [False, True])
@pytest.mark.parametrize("proven", [False, True])
def test_shared_sdk_canonical115_clears_prior_bad_cells_and_keeps_receipts(
        use_batch_update, proven, monkeypatch):
    source = source_row(proven=proven)
    grid = [HEADERS] + matrix([source])
    original = copy.deepcopy(grid)
    api = FakeSheetsAPI(grid)
    monkeypatch.setattr(sheets_sdk, "get_sheets_service", lambda: api)
    monkeypatch.setattr(sheets_sdk._CONFIG, "use_batch_update", use_batch_update)
    assert sheets_sdk.write_grid_chunked("SYNTHETIC_BOOK", "Commodities_FX", "A5", grid) == 230
    request = api.requests[-1]
    if use_batch_update:
        assert request["body"]["valueInputOption"] == "RAW"
        payload = request["body"]["data"][0]["values"]
    else:
        assert request["valueInputOption"] == "RAW"
        payload = request["body"]["values"]
    assert len(payload[0]) == len(payload[1]) == 115
    assert_presented(dict(zip(HEADERS, payload[1])), source, proven=proven)
    assert_presented(dict(zip(HEADERS, api.grid[1])), source, proven=proven)
    assert grid == original
    sheets_sdk.write_grid_chunked("SYNTHETIC_BOOK", "Commodities_FX", "A5", payload)
    assert api.grid == payload


@pytest.mark.parametrize("malformation", ["short", "reordered", "duplicate", "alias"])
def test_direct_sdk_malformed_market_headers_fail_before_write(malformation):
    headers = list(HEADERS)
    if malformation == "short":
        headers.pop()
    elif malformation == "reordered":
        headers[0], headers[1] = headers[1], headers[0]
    elif malformation == "duplicate":
        headers[headers.index("Gross Margin")] = "Profit Margin"
    else:
        headers[headers.index("Profit Margin")] = "profit_margin"
    rows = matrix([source_row()], headers)
    if malformation == "duplicate":
        indices = [i for i, header in enumerate(headers) if header == "Profit Margin"]
        rows[0][indices[0]], rows[0][indices[1]] = 0.009, 0.5
    original = copy.deepcopy((headers, rows))
    api = FakeSheetsAPI()
    writer = sync.SheetsWriter()
    writer._service = api
    with pytest.raises(ValueError):
        writer.write_table("SYNTHETIC_BOOK", "Commodities_FX", "A5", headers, rows)
    assert api.requests == []
    assert (headers, rows) == original


@pytest.mark.parametrize("width", [114, 116])
def test_direct_sdk_malformed_market_row_width_fails_before_write(width):
    rows = matrix([source_row()])
    rows[0] = rows[0][:width] if width < 115 else rows[0] + ["extra-alias-value"]
    original = copy.deepcopy(rows)
    api = FakeSheetsAPI()
    writer = sync.SheetsWriter()
    writer._service = api
    with pytest.raises(ValueError):
        writer.write_table("SYNTHETIC_BOOK", "Commodities_FX", "A5", HEADERS, rows)
    assert api.requests == [] and rows == original


@pytest.mark.parametrize("page", ["My_Portfolio", "Insights_Analysis", "_Refresh_History", "Account", "Cash", "Ledger"])
def test_direct_sdk_other_pages_keep_existing_values(page):
    rows = matrix([source_row()])
    original = copy.deepcopy(rows)
    api = FakeSheetsAPI()
    writer = sync.SheetsWriter()
    writer._service = api
    writer.write_table("SYNTHETIC_BOOK", page, "A5", HEADERS, rows)
    assert api.requests[-1]["body"]["values"] == [HEADERS] + original
    assert rows == original


def test_direct_sdk_native_portfolio122_keeps_manual_and_presentation_cells():
    headers = get_sheet_headers("My_Portfolio")
    assert len(headers) == 122
    source = dict(source_row(), **{"Position Qty": 11.5, "Avg Cost": 2410.625,
                                 "Investor Decision": "HOLD", "User Notes": "Synthetic note"})
    rows = matrix([source], headers)
    original = copy.deepcopy(rows)
    api = FakeSheetsAPI()
    writer = sync.SheetsWriter()
    writer._service = api
    writer.write_table("SYNTHETIC_BOOK", "My_Portfolio", "A5", headers, rows)
    assert api.requests[-1]["body"]["values"] == [headers] + original
    assert rows == original


@pytest.mark.parametrize("mechanism", ["klg", "pv2"])
@pytest.mark.parametrize("proven", [False, True])
@pytest.mark.parametrize("actual_writer", [False, True])
def test_actual_runner_presents_late_restored_rows_before_writer(mechanism, proven, actual_writer, monkeypatch):
    prior = source_row(proven=proven)
    fresh = [source_row(symbol) for symbol in ("GC=F", "HG=F", "CL=F")]
    failed = dict(prior, **{"Current Price": "", "Data Provider": "fallback_error",
                            "Warnings": "fetch_failed:timeout"})
    incoming = matrix(fresh + [failed if mechanism == "klg" else source_row()])
    originals = copy.deepcopy((incoming, prior, fresh))

    class Backend:
        async def post_json(self, _endpoint, _payload):
            return {"headers": HEADERS, "rows_matrix": incoming}, None, 200

    class RecordingWriter(sync.SheetsWriter):
        published = None
        clears = 0

        def __init__(self):
            super().__init__()
            self._service = FakeSheetsAPI([HEADERS] + matrix([prior]))

        def _get_service(self):
            return self._service

        def read_values(self, *_args, **_kwargs):
            return [HEADERS] + matrix([prior])

        def clear_from(self, *_args, **_kwargs):
            self.clears += 1

        def write_table(self, _book, _page, _start, headers, rows):
            if actual_writer:
                written = super().write_table(_book, _page, _start, headers, rows)
                self.published = copy.deepcopy(self._service.grid[1:])
                return written
            # This recorder deliberately performs no presentation itself.
            self.published = copy.deepcopy(rows)
            return len(rows)

    if mechanism == "pv2":
        monkeypatch.setattr(sync, "_row_firewall_enabled", lambda: True)
        monkeypatch.setattr(sync, "_persistence_hard_enabled", lambda: True)
        monkeypatch.setattr(sync, "_persist_v2_enabled", lambda: True)
        monkeypatch.setattr(sync, "_persist_sanity_enabled", lambda: False)

        def drop_after_first_persistence(headers, rows):
            symbol_i = headers.index("Symbol")
            return [row for row in rows if row[symbol_i] != "SI=F"], []

        monkeypatch.setattr(sync, "_row_identity_firewall", drop_after_first_persistence)
    monkeypatch.setattr(sync, "_utc_now", lambda: NOW)
    monkeypatch.setattr(sync, "_read_symbols", lambda *_args, **_kwargs: ["GC=F", "SI=F", "HG=F", "CL=F"])
    writer = RecordingWriter()
    result = asyncio.run(sync._run_one_task(
        sync.TaskSpec("COMMODITIES_FX", "Commodities_FX", "analysis"),
        "SYNTHETIC_BOOK", "A5", -1, False, False, Backend(), writer,
    ))
    assert result.status == "success" and result.rows_written == 4
    assert result._stamp_meta["klg_kept" if mechanism == "klg" else "pv2_restored"] == 1
    published = {row[HEADERS.index("Symbol")]: dict(zip(HEADERS, row)) for row in writer.published}
    restored = published["SI=F"]
    assert_presented(restored, prior, proven=proven)
    assert "acquisition_status:preserved" in restored["Warnings"]
    assert "acquisition_status:success" not in restored["Warnings"]
    assert row_acquisition(restored, NOW, 86400).status == "INVALID"
    assert (incoming, prior, fresh) == originals


def test_actual_direct_writer_preserves_accepted_market_tuple_shape():
    row = tuple(matrix([source_row()])[0])
    api = FakeSheetsAPI()
    writer = sync.SheetsWriter()
    writer._service = api
    writer.write_table("SYNTHETIC_BOOK", "Commodities_FX", "A5", HEADERS, [row])
    published = api.requests[-1]["body"]["values"][1]
    assert len(published) == 115 and published[0] == "SI=F"
    assert_presented(dict(zip(HEADERS, published)), source_row())
    assert row == tuple(matrix([source_row()])[0])


def test_actual_runner_presentation_failure_precedes_clear_hold_and_update(monkeypatch):
    source = source_row()
    incoming = matrix([source])
    api = FakeSheetsAPI([HEADERS] + incoming)
    events = []
    class Backend:
        async def post_json(self, *_args):
            return {"headers": HEADERS, "rows_matrix": incoming}, None, 200
    class Writer(sync.SheetsWriter):
        def __init__(self):
            super().__init__()
            self._service = api
        def read_values(self, *_args, **_kwargs):
            return [HEADERS] + copy.deepcopy(incoming)
        def clear_from(self, *_args, **_kwargs):
            events.append("clear")
    def failed_presentation(*_args):
        raise ValueError("synthetic presentation guard failure")
    monkeypatch.setattr(sync, "_present_market_sheet_rows", failed_presentation)
    monkeypatch.setattr(sync, "_sync_hold_publish", lambda *_args: events.append("hold"))
    monkeypatch.setattr(sync, "_read_symbols", lambda *_args, **_kwargs: ["SI=F"])
    monkeypatch.setattr(sync, "_utc_now", lambda: NOW)
    monkeypatch.setenv("TFB_SYNC_WRITE_THEN_TRIM", "0")
    result = asyncio.run(sync._run_one_task(
        sync.TaskSpec("COMMODITIES_FX", "Commodities_FX", "analysis"),
        "SYNTHETIC_BOOK", "A5", -1, True, False, Backend(), Writer()))
    assert result.status == "failed" and result.rows_written == 0
    assert events == [] and api.requests == []
    assert api.grid == [HEADERS] + incoming

