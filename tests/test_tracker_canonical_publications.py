"""Price-corroborated targets and immutable publication replay boundaries."""
from __future__ import annotations

import asyncio
import copy
import json
import random
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from scripts import track_performance as tracker

RIYADH = timezone(timedelta(hours=3))
NOW = datetime(2026, 10, 11, 19, tzinfo=RIYADH)


class Worksheet:
    def __init__(self, fail_read=False, lose_response=False):
        self.rows = []
        self.row_count = 1000
        self.fail_read = fail_read
        self.lose_response = lose_response
        self.append_calls = 0

    def get(self, range_name):
        if self.fail_read:
            raise RuntimeError("identity index unavailable")
        col = tracker.SignalHistoryStore.HEADERS.index("Publication Key")
        assert range_name == "S2:S"
        return [[row[col]] for row in self.rows]

    def append_rows(self, rows, value_input_option="RAW"):
        self.append_calls += 1
        self.rows.extend(copy.deepcopy(rows))
        if self.lose_response:
            self.lose_response = False
            raise RuntimeError("server committed but response lost")


class ImmediateRetry:
    def execute_sync(self, operation):
        try:
            return operation()
        except RuntimeError:
            return operation()


def app_with_store(worksheet):
    store = object.__new__(tracker.SignalHistoryStore)
    store.ws, store.sheet = worksheet, None
    store.spreadsheet_id = "test"
    store.backoff = ImmediateRetry()
    store.cache_keys, store.cache_lock = set(), tracker.Lock()
    app = object.__new__(tracker.PerformanceTrackerApp)
    app.signal_store = store
    app.signal_history_enabled = True
    app.args = SimpleNamespace(max_records=1000, horizons=["1M", "3M"])
    return app


def source_row():
    return {"symbol": "AAA.US", "current_price": 100.0,
            "forecast_price_1m": 110.0, "expected_roi_1m": 0.10,
            "forecast_price_3m": 125.0, "expected_roi_3m": 0.25,
            "recommendation": "BUY", "final_action": "INVEST"}


@pytest.fixture(autouse=True)
def reset_rollout_flags(monkeypatch):
    old_failures = tracker._evidence_append_failures()
    for name in ("TRACK_CANONICAL_TARGETS", "TRACK_SIGNAL_PUBLICATION_MODE",
                 "TRACK_PUBLICATION_ID", "TRACK_DEDUP_SIGNAL_HISTORY",
                 "TRACK_LOG_UNFORMATTED_READ"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(tracker.RiyadhTime, "now", classmethod(lambda cls: NOW))
    yield
    with tracker._EVIDENCE_APPEND_LOCK:
        tracker._EVIDENCE_APPEND_FAILURES[:] = old_failures


@pytest.mark.parametrize("horizon,expected", ((tracker.HorizonType.WEEK_1, 7 / 30 * 10),
                                             (tracker.HorizonType.WEEK_2, 14 / 30 * 10),
                                             (tracker.HorizonType.MONTH_1, 10),
                                             (tracker.HorizonType.MONTH_3, 25)))
def test_canonical_new_targets_use_price_witness_for_every_horizon(monkeypatch, horizon, expected):
    monkeypatch.setenv("TRACK_CANONICAL_TARGETS", "1")
    app = app_with_store(Worksheet())
    row = source_row()
    row["expected_roi_1m"] = 12345  # explicit price witness wins; no magnitude inference
    price, roi = app._derive_target(row, horizon, 100)
    assert roi == pytest.approx(expected)
    assert (price / 100 - 1) * 100 == pytest.approx(expected)
    assert app._derive_target({"expected_roi_1m": 0.1}, tracker.HorizonType.MONTH_1, 100) == (0, 0)


def test_schema_survives_real_sheet_roundtrip_and_win_can_miss_target(monkeypatch):
    monkeypatch.setenv("TRACK_CANONICAL_TARGETS", "1")
    app = app_with_store(Worksheet())
    records = asyncio.run(app.record_from_top10([], [source_row()]))
    record = records[0]
    assert tracker._target_schema(record.notes)["horizon"] == "1M"
    record.status = tracker.PerformanceStatus.MATURED
    record.realized_roi, record.outcome = 2.0, "WIN"
    store = object.__new__(tracker.PerformanceStore)
    loaded = tracker.PerformanceRecord.from_sheet_row(store._record_to_row(record), store.HEADERS)
    assert tracker._target_schema(loaded.notes)["roi_unit"] == "percent_points"
    result = tracker.canonical_performance_report([loaded])["records"][0]
    assert result["directional_outcome"] == "WIN"
    assert result["target_achieved_at_horizon"] is False
    assert result["forecast_error_pp"] == pytest.approx(-8)
    assert result["intrahorizon_target_hit"] is result["stop_hit"] is result["net_realized_roi_pp"] is None


def legacy_record(roi, target_price, horizon=tracker.HorizonType.MONTH_1, record_id="old"):
    return tracker.PerformanceRecord(
        record_id=record_id, symbol="AAA.US", horizon=horizon, date_recorded=NOW,
        entry_price=100, entry_recommendation=tracker.RecommendationType.BUY,
        entry_score=70, entry_risk_bucket="LOW", entry_confidence="HIGH", origin_tab="test",
        target_price=target_price, target_roi=roi, target_date=NOW + timedelta(days=horizon.days),
        status=tracker.PerformanceStatus.MATURED, realized_roi=2.0, outcome="WIN")


@pytest.mark.parametrize("roi,price,candidate,reason", ((0.25, 125, 25, "unproven_forecast_provenance"),
                                                     (0.0001, 100.01, None, "ambiguous_units"),
                                                     (2, 102, 2, "unproven_forecast_provenance"),
                                                     (4, 102, None, "target_price_mismatch")))
def test_historical_audit_corroborates_without_mutating_or_guessing(roi, price, candidate, reason):
    record = legacy_record(roi, price)
    before = copy.deepcopy(record.to_dict())
    result = tracker.canonical_performance_report([record])["records"][0]
    assert result["unit_evidence"] == "unresolved"
    assert result["canonical_target_roi_pp"] is None
    assert result["price_consistent_candidate_pp"] == candidate
    assert result["reason"] == reason
    assert record.to_dict() == before


def test_checkpoint_requires_proven_unique_same_day_thesis_and_duplicate_maturities_do_not_count(monkeypatch):
    checkpoint = legacy_record(7 / 30 * 0.1, 100.023333, tracker.HorizonType.WEEK_1)
    assert tracker.canonical_performance_report([checkpoint])["by_horizon"] == {}
    sibling = legacy_record(0.1, 110)
    result = tracker.canonical_performance_report([checkpoint, sibling])
    assert result["records"][0]["reason"] == "unproven_forecast_provenance"
    monkeypatch.setenv("TRACK_CANONICAL_TARGETS", "1")
    app = app_with_store(Worksheet())
    sibling = asyncio.run(app.record_from_top10([], [source_row()]))[0]
    sibling.status = tracker.PerformanceStatus.MATURED
    sibling.realized_roi = 5
    result = tracker.canonical_performance_report([checkpoint, sibling])
    assert result["records"][0]["canonical_target_roi_pp"] == pytest.approx(7 / 30 * 10)
    duplicate = copy.deepcopy(sibling)
    duplicate.record_id = "dup"
    result = tracker.canonical_performance_report([checkpoint, sibling, duplicate])
    assert result["by_horizon"] == {}
    assert result["unit_counts"]["duplicate_matured_key"] == 2


def test_immutable_real_writer_replay_preserves_first_and_later_daily_state(monkeypatch):
    monkeypatch.setenv("TRACK_SIGNAL_PUBLICATION_MODE", "immutable")
    monkeypatch.setenv("TRACK_PUBLICATION_ID", "run-1")
    worksheet = Worksheet()
    app = app_with_store(worksheet)
    assert asyncio.run(app.record_signal_snapshots([source_row()])) == 1
    # A fresh process has an empty cache; publication-index I/O still skips replay.
    app = app_with_store(worksheet)
    assert asyncio.run(app.record_signal_snapshots([source_row()])) == 0
    first = copy.deepcopy(worksheet.rows[0])
    monkeypatch.setenv("TRACK_PUBLICATION_ID", "run-2")
    row = source_row() | {"overall_score": 90, "final_action": "HOLD"}
    assert asyncio.run(app.record_signal_snapshots([row])) == 1
    assert worksheet.rows[0] == first
    snapshots = [tracker.SignalSnapshot.from_sheet_row(row, tracker.SignalHistoryStore.HEADERS)
                 for row in worksheet.rows]
    states = tracker.signal_daily_publications(snapshots)
    assert states[0]["publication_count"] == 2
    assert states[0]["first_publication"]["publication_id"] == "run-1"
    assert states[0]["latest_daily_state"]["publication_id"] == "run-2"
    assert {snap.publication_id for snap in snapshots} == {"run-1", "run-2"}
    assert snapshots[0].key == snapshots[1].key


def test_publication_replay_crossing_midnight_keeps_source_identity(monkeypatch):
    monkeypatch.setenv("TRACK_SIGNAL_PUBLICATION_MODE", "immutable")
    monkeypatch.setenv("TRACK_PUBLICATION_ID", "source-42")
    worksheet = Worksheet()
    app = app_with_store(worksheet)
    assert asyncio.run(app.record_signal_snapshots([source_row()])) == 1
    monkeypatch.setattr(tracker.RiyadhTime, "now", classmethod(lambda cls: NOW + timedelta(days=1)))
    assert asyncio.run(app.record_signal_snapshots([source_row()])) == 0
    assert len(worksheet.rows) == 1


@pytest.mark.parametrize("failure", ("read", "missing_id"))
def test_immutable_writer_refuses_unverifiable_idempotence(monkeypatch, failure):
    monkeypatch.setenv("TRACK_SIGNAL_PUBLICATION_MODE", "immutable")
    if failure != "missing_id":
        monkeypatch.setenv("TRACK_PUBLICATION_ID", "run-1")
    worksheet = Worksheet(fail_read=failure == "read")
    app = app_with_store(worksheet)
    assert asyncio.run(app.record_signal_snapshots([source_row()])) == 0
    assert worksheet.rows == []


def test_lost_append_response_rechecks_identity_before_retry(monkeypatch):
    monkeypatch.setenv("TRACK_SIGNAL_PUBLICATION_MODE", "immutable")
    monkeypatch.setenv("TRACK_PUBLICATION_ID", "run-1")
    worksheet = Worksheet(lose_response=True)
    app = app_with_store(worksheet)
    # The response was lost, so zero newly confirmed writes is honest; the
    # committed publication is observed on retry and never appended twice.
    assert asyncio.run(app.record_signal_snapshots([source_row()])) == 0
    assert worksheet.append_calls == 1
    assert len(worksheet.rows) == 1


def test_canonical_receipt_invalid_horizon_is_quarantined_and_mixed_csv_exports(monkeypatch, tmp_path):
    monkeypatch.setenv("TRACK_CANONICAL_TARGETS", "1")
    app = app_with_store(Worksheet())
    new = asyncio.run(app.record_from_top10([], [source_row()]))[0]
    old = legacy_record(0.1, 110)
    old.symbol = "OLD.US"
    report = tracker.ReportGenerator(tracker.PerformanceAnalyzer())
    report.generate_csv([old, new], str(tmp_path / "records.csv"))
    new.notes = new.notes.replace('"1M"', '"3M"')
    result = tracker.canonical_performance_report([new])["records"][0]
    assert result["reason"] == "unsupported_schema"


def test_legacy_synthesized_target_is_not_unit_provenance():
    app = app_with_store(Worksheet())
    price, roi = app._derive_target({"expected_roi_1m": 0.2}, tracker.HorizonType.MONTH_1, 100)
    assert (price, roi) == (100.2, 0.2)
    result = tracker.canonical_performance_report([legacy_record(roi, price)])["records"][0]
    assert result["reason"] == "unproven_forecast_provenance"
    assert result["canonical_target_roi_pp"] is None


def test_witnessed_flat_forecast_has_error_even_without_directional_target(monkeypatch):
    monkeypatch.setenv("TRACK_CANONICAL_TARGETS", "1")
    app = app_with_store(Worksheet())
    row = source_row() | {"forecast_price_1m": 100, "expected_roi_1m": 0}
    record = asyncio.run(app.record_from_top10([], [row]))[0]
    record.status, record.realized_roi = tracker.PerformanceStatus.MATURED, 5
    report = tracker.canonical_performance_report([record])
    assert report["records"][0]["forecast_error_pp"] == 5
    assert report["records"][0]["target_achieved_at_horizon"] is None
    assert report["by_horizon"]["1M"]["n"] == 1
    assert report["by_horizon"]["1M"]["target_achievement_n"] == 0


def test_distinct_publications_for_one_symbol_in_one_batch_are_retained(monkeypatch):
    monkeypatch.setenv("TRACK_SIGNAL_PUBLICATION_MODE", "immutable")
    app = app_with_store(Worksheet())
    rows = [source_row() | {"publication_id": value} for value in ("pub-1", "pub-2")]
    assert asyncio.run(app.record_signal_snapshots(rows)) == 2
    assert len(app.signal_store.ws.rows) == 2


def test_invalid_index_response_refuses_append(monkeypatch):
    monkeypatch.setenv("TRACK_SIGNAL_PUBLICATION_MODE", "immutable")
    monkeypatch.setenv("TRACK_PUBLICATION_ID", "run-1")
    app = app_with_store(Worksheet())
    app.signal_store.ws.get = lambda range_name: None
    assert asyncio.run(app.record_signal_snapshots([source_row()])) == 0
    assert app.signal_store.ws.rows == []


def test_lost_response_beyond_cached_grid_extent_still_skips(monkeypatch):
    monkeypatch.setenv("TRACK_SIGNAL_PUBLICATION_MODE", "immutable")
    monkeypatch.setenv("TRACK_PUBLICATION_ID", "run-1")
    worksheet = Worksheet(lose_response=True)
    worksheet.row_count = 1  # cached metadata remains stale after append grows the grid
    app = app_with_store(worksheet)
    assert asyncio.run(app.record_signal_snapshots([source_row()])) == 0
    assert worksheet.append_calls == 1
    assert len(worksheet.rows) == 1


@pytest.mark.parametrize("raw,valid", (("2.5%", True), ("0", True), ("bad", False),
                                      ("N/A", False), ("", False), ("NaN", False)))
def test_invalid_stored_outcomes_are_not_canonical_zero_samples(monkeypatch, raw, valid):
    monkeypatch.setenv("TRACK_CANONICAL_TARGETS", "1")
    app = app_with_store(Worksheet())
    record = asyncio.run(app.record_from_top10([], [source_row()]))[0]
    record.status = tracker.PerformanceStatus.MATURED
    store = object.__new__(tracker.PerformanceStore)
    row = store._record_to_row(record)
    row[store.HEADERS.index("Realized ROI %")] = raw
    loaded = tracker.PerformanceRecord.from_sheet_row(row, store.HEADERS)
    assert loaded.realized_roi_source_valid is valid
    result = tracker.canonical_performance_report([loaded])
    assert bool(result["by_horizon"]) is valid


# --- v6.50.0 [LOG-READ]: the Performance_Log read-back (full harness:
# tests/test_tracker_log_read_precision.py; these two guard the blocking lane).
class FormattedLog:
    """Rows as the tracker writes them (RAW); numbers come back as their
    displayed '.10g' text unless UNFORMATTED_VALUE is requested, as in Sheets."""

    def __init__(self, rows):
        self.rows, self.row_count, self.calls = copy.deepcopy(rows), 1000, []

    def get(self, range_name, **kw):
        self.calls.append(kw)
        if kw.get("value_render_option") == "UNFORMATTED_VALUE":
            return copy.deepcopy(self.rows)
        return [[format(v, ".10g") if isinstance(v, (int, float)) and not isinstance(v, bool) else v
                 for v in row] for row in self.rows]


def written_log_rows(monkeypatch):
    monkeypatch.setenv("TRACK_CANONICAL_TARGETS", "1")
    app = app_with_store(Worksheet())
    app.args.horizons = ["1W", "2W", "1M", "3M"]
    rnd, rows = random.Random(20261025), []
    for i in range(12):
        price, r1 = round(rnd.uniform(5, 500), 2), rnd.uniform(-0.12, 0.25)
        r3 = r1 * rnd.uniform(1.4, 2.6)
        row = source_row()
        row.update(symbol="S%02d.US" % i, current_price=price, expected_roi_1m=r1, expected_roi_3m=r3,
                   forecast_price_1m=round(price * (1 + r1), 2), forecast_price_3m=round(price * (1 + r3), 2))
        rows.append(row)
    records = asyncio.run(app.record_from_top10([], rows))
    store = object.__new__(tracker.PerformanceStore)
    return [store._record_to_row(r) for r in records]


def reload_log(rows):
    store = object.__new__(tracker.PerformanceStore)
    store.ws, store.backoff = FormattedLog(rows), ImmediateRetry()
    store.cache, store.cache_lock = {}, tracker.Lock()
    return store.load_records(1000), store


def schema_mismatches(records):
    return sum(1 for item in tracker.canonical_performance_report(records)["records"]
               if item["reason"] == "schema_price_mismatch")


@pytest.mark.parametrize("mode", (None, "off", "1"))
def test_log_read_off_keeps_the_formatted_read_that_loses_canonical_targets(monkeypatch, mode):
    rows = written_log_rows(monkeypatch)
    if mode is not None:
        monkeypatch.setenv("TRACK_LOG_UNFORMATTED_READ", mode)
    loaded, store = reload_log(rows)
    assert store.ws.calls == [{}]
    assert len(loaded) == 48 and schema_mismatches(loaded) > 0   # the defect, pinned


def test_log_read_enforce_reads_back_exactly_what_was_written(monkeypatch):
    rows = written_log_rows(monkeypatch)
    monkeypatch.setenv("TRACK_LOG_UNFORMATTED_READ", "enforce")
    loaded, store = reload_log(rows)
    assert store.ws.calls == [{"value_render_option": "UNFORMATTED_VALUE",
                               "date_time_render_option": "FORMATTED_STRING"}]
    assert schema_mismatches(loaded) == 0
    assert [store._record_to_row(r) for r in loaded] == rows
