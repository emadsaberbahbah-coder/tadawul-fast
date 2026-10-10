"""Real shared calendar parser, tracker joins and historical persistence."""
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from core import calendar_evidence as evidence
from scripts import track_performance as tracker

STAMP = "2026-10-09T08:15:00Z"


def event_row(symbol="ACME.US", earnings="2026-10-12", exdiv="2026-10-15"):
    return [symbol, earnings, 999, exdiv, 999, "2026-10-10 11:15", "summary only",
            "yahoo", STAMP, "estimated", "eodhd", STAMP, "reported"]


def context():
    return evidence.parse_calendar_values([evidence.CALENDAR_HEADERS, event_row()])["ACME.US"]


@pytest.fixture(autouse=True)
def frozen_clock(monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            now = cls(2026, 10, 10, 8, 0, tzinfo=timezone.utc)
            return now.astimezone(tz) if tz else now.replace(tzinfo=None)

    monkeypatch.setattr(evidence, "datetime", Clock)
    monkeypatch.setattr(tracker.RiyadhTime, "now", lambda: Clock.now(timezone.utc))
    monkeypatch.setenv("TRACK_EVENT_CONTEXT", "1")


def test_actual_parser_binds_each_provider_to_its_own_event():
    assert context() == {
        "next_earnings_date": "2026-10-12", "earnings_source": "yahoo",
        "earnings_observed_at": STAMP, "earnings_status": "estimated",
        "next_ex_div_date": "2026-10-15", "exdiv_source": "eodhd",
        "exdiv_observed_at": STAMP, "exdiv_status": "reported",
    }


@pytest.mark.parametrize("column,value", [
    (7, "issuer confirmed"), (8, "2026-10-09"), (8, "2026-10-09T08:15:00"),
    (8, "2099-10-09T08:15:00Z"), (8, "2026-02-31T08:15:00Z"),
    (8, "2026-10-09T08:15:00+03:00"), (9, "confirmed"),
    (7, "eodhd"),  # estimated EODHD claim is unsupported
])
def test_incomplete_or_unsupported_claims_keep_date_with_unknown_evidence(column, value):
    row = event_row()
    row[column] = value
    parsed = evidence.parse_calendar_values([evidence.CALENDAR_HEADERS, row])["ACME.US"]
    assert parsed["next_earnings_date"] == "2026-10-12"
    assert parsed["earnings_source"] == parsed["earnings_status"] == "unknown"
    assert parsed["earnings_observed_at"] == ""
    assert parsed["exdiv_status"] == "reported"


@pytest.mark.parametrize("invalid", ["", "2026-02-31", "2026-10-12junk", "12/10/2026"])
def test_invalid_date_cannot_be_replaced_with_static_countdown_or_typed_receipt(invalid):
    row = event_row(earnings=invalid)
    parsed = evidence.parse_calendar_values([evidence.CALENDAR_HEADERS, row])["ACME.US"]
    assert parsed["next_earnings_date"] is None and parsed["earnings_status"] == "unknown"
    assert parsed["earnings_observed_at"] == ""


def test_legacy_row_does_not_turn_summary_clock_into_field_provenance():
    row = event_row()[:7]
    row[6] = "issuer confirmed / eodhd / yahoo"
    parsed = evidence.parse_calendar_values([evidence.CALENDAR_HEADERS[:7], row])["ACME.US"]
    assert parsed["next_earnings_date"] == "2026-10-12"
    assert parsed["earnings_status"] == parsed["exdiv_status"] == "unknown"
    assert parsed["earnings_observed_at"] == parsed["exdiv_observed_at"] == ""


@pytest.mark.parametrize("column,value", [(1, "2026-10-13"), (7, "eodhd"), (8, "2026-10-08T08:15:00Z")])
def test_conflicting_duplicate_symbol_is_excluded_independent_of_row_order(column, value):
    original = event_row()
    conflict = deepcopy(original)
    conflict[column] = value
    for rows in ([original, conflict, original], [conflict, original, conflict]):
        assert evidence.parse_calendar_values([evidence.CALENDAR_HEADERS, *rows]) == {}
    assert list(evidence.parse_calendar_values([evidence.CALENDAR_HEADERS, original, original])) == ["ACME.US"]


def test_duplicate_header_and_oversize_table_are_explicitly_rejected():
    with pytest.raises(evidence.CalendarEvidenceError, match="duplicate"):
        evidence.parse_calendar_values([evidence.CALENDAR_HEADERS + ["Earnings Source"]])
    with pytest.raises(evidence.CalendarEvidenceError, match="capacity"):
        evidence.parse_calendar_values([evidence.CALENDAR_HEADERS] + [[""]] * 5001)


class CalendarSheet:
    def __init__(self, rows, row_count=None, col_count=13):
        self.values = rows
        self.row_count = row_count or len(rows)
        self.col_count = col_count
        self.reads = []

    def get(self, a1):
        self.reads.append(a1)
        return self.values


def app_with(sheet):
    app = object.__new__(tracker.PerformanceTrackerApp)
    app.signal_store = SimpleNamespace(sheet=SimpleNamespace(worksheet=lambda _: sheet))
    return app


def test_actual_tracker_reads_and_retains_event_after_old_2000_row_cap():
    values = [evidence.CALENDAR_HEADERS] + [[""] * 13 for _ in range(2099)] + [event_row()]
    sheet = CalendarSheet(values)
    assert app_with(sheet)._load_calendar_context()["ACME.US"] == context()
    assert sheet.reads == ["A1:M2101"]


def test_tracker_clamps_to_legacy_grid_and_keeps_unknown_evidence():
    sheet = CalendarSheet([evidence.CALENDAR_HEADERS[:7], event_row()[:7]], row_count=1000, col_count=7)
    parsed = app_with(sheet)._load_calendar_context()["ACME.US"]
    assert sheet.reads == ["A1:G1000"] and parsed["earnings_status"] == "unknown"


def test_tracker_refuses_overbound_grid_before_reading():
    sheet = CalendarSheet([evidence.CALENDAR_HEADERS, event_row()], row_count=5002)
    assert app_with(sheet)._load_calendar_context() == {} and sheet.reads == []


def test_real_join_never_attaches_evidence_to_conflicting_backend_date():
    rows = [{"symbol": "ACME.US", "next_earnings_date": "2026-10-14"}]
    tracker._merge_calendar_context(rows, {"ACME.US": context()})
    assert rows[0]["next_earnings_date"] == "2026-10-14"
    assert "earnings_source" not in rows[0]
    assert rows[0]["next_ex_div_date"] == "2026-10-15" and rows[0]["exdiv_source"] == "eodhd"


def test_equal_date_can_receive_only_a_whole_missing_tuple():
    rows = [{"symbol": "ACME.US", "next_earnings_date": "2026-10-12", "earnings_source": "other"}]
    tracker._merge_calendar_context(rows, {"ACME.US": context()})
    assert rows[0]["earnings_source"] == "other" and "earnings_observed_at" not in rows[0]
    rows[0].pop("earnings_source")
    tracker._merge_calendar_context(rows, {"ACME.US": context()})
    assert rows[0]["earnings_source"] == "yahoo" and rows[0]["earnings_observed_at"] == STAMP


def test_sheet_read_join_snapshot_and_history_roundtrip_keep_date_and_evidence():
    app = app_with(CalendarSheet([evidence.CALENDAR_HEADERS, event_row()]))
    rows = [{"symbol": "ACME.US", "current_price": 10, "recommendation": "HOLD"}]
    assert tracker._merge_calendar_context(rows, app._load_calendar_context()) == 1
    snapshot = app._build_signal_snapshots(rows)[0]
    assert snapshot.days_to_earnings == 2 and snapshot.days_to_exdiv == 5
    store = object.__new__(tracker.SignalHistoryStore)
    row = store._snapshot_to_row(snapshot)
    assert len(row) == len(store.HEADERS) == 26
    restored = tracker.SignalSnapshot.from_sheet_row(row, store.HEADERS)
    for key, value in context().items():
        assert restored.to_dict()[key] == value
    assert restored.key == snapshot.key


def test_legacy_historical_rows_load_without_backfilling_modern_provenance():
    app = app_with(CalendarSheet([]))
    snapshot = app._build_signal_snapshots([{"symbol": "ACME.US", "current_price": 10}])[0]
    store = object.__new__(tracker.SignalHistoryStore)
    legacy_row = store._snapshot_to_row(snapshot)[:18]
    loaded = tracker.SignalSnapshot.from_sheet_row(legacy_row, store.HEADERS[:18])
    assert loaded.earnings_source == loaded.earnings_status == "unknown"
    assert loaded.earnings_observed_at == loaded.next_earnings_date == ""


def test_context_switch_disables_dates_and_evidence_capture(monkeypatch):
    monkeypatch.setenv("TRACK_EVENT_CONTEXT", "0")
    app = app_with(CalendarSheet([]))
    snapshot = app._build_signal_snapshots([dict(context(), symbol="ACME.US")])[0]
    assert snapshot.days_to_earnings is None and snapshot.days_to_exdiv is None
    assert snapshot.earnings_status == "unknown" and snapshot.next_earnings_date == ""


class HistorySheet:
    def __init__(self, headers, error=None, cols=18):
        self.headers, self.error, self.col_count = headers, error, cols
        self.row_count, self.body = 4000, []
        self.resizes, self.updates = [], []

    def row_values(self, _):
        if self.error:
            raise self.error
        return self.headers

    def resize(self, **kwargs):
        self.resizes.append(kwargs)

    def update(self, **kwargs):
        self.updates.append(kwargs)

    def freeze(self, **kwargs):
        pass

    def get(self, a1):
        assert a1 in {"A2:R4000", "S2:Z4000"}
        return self.body


def test_history_header_migration_only_grows_columns_and_leaves_old_fields_in_place():
    store = object.__new__(tracker.SignalHistoryStore)
    store.ws = HistorySheet(store.HEADERS[:18])
    store._ensure_headers()
    assert store.ws.resizes == [{"cols": 26}]
    assert store.ws.updates == [{"values": [store.HEADERS], "range_name": "A1:Z1", "value_input_option": "RAW"}]


@pytest.mark.parametrize("variant", ["reordered", "duplicate", "read_failure"])
def test_history_migration_preserves_ambiguous_or_unreadable_table(variant):
    store = object.__new__(tracker.SignalHistoryStore)
    hdr = list(store.HEADERS[:18])
    if variant == "reordered":
        hdr[2], hdr[3] = hdr[3], hdr[2]
    if variant == "duplicate":
        hdr[3] = hdr[2]
    store.ws = HistorySheet(hdr, RuntimeError("read failed") if variant == "read_failure" else None)
    with pytest.raises((ValueError, RuntimeError)):
        store._ensure_headers()
    assert store.ws.updates == store.ws.resizes == []


@pytest.mark.parametrize("value", ["old evidence", 0, False])
def test_missing_header_above_any_existing_body_value_is_not_initialized(value):
    store = object.__new__(tracker.SignalHistoryStore)
    store.ws = HistorySheet([])
    store.ws.body = [[""]] * 2100 + [[value]]
    with pytest.raises(ValueError, match="existing data"):
        store._ensure_headers()
    assert store.ws.updates == store.ws.resizes == []


def test_verified_empty_history_can_initialize_additive_header():
    store = object.__new__(tracker.SignalHistoryStore)
    store.ws = HistorySheet([])
    store._ensure_headers()
    assert store.ws.resizes == [{"cols": 26}]
    assert len(store.ws.updates) == 1


def test_legacy_header_cannot_relabel_preexisting_unheaded_extension_cells():
    store = object.__new__(tracker.SignalHistoryStore)
    store.ws = HistorySheet(store.HEADERS[:18], cols=40)
    store.ws.body = [[""]] * 2000 + [["2026-10-12", "yahoo", STAMP, "estimated"]]
    with pytest.raises(ValueError, match="extension contains existing data"):
        store._ensure_headers()
    assert store.ws.updates == store.ws.resizes == []


def test_empty_extension_allows_legacy_40column_grid_migration_without_resize():
    store = object.__new__(tracker.SignalHistoryStore)
    store.ws = HistorySheet(store.HEADERS[:18], cols=40)
    store._ensure_headers()
    assert store.ws.resizes == [] and len(store.ws.updates) == 1
