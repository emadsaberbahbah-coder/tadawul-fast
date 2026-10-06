"""Offline outcome cohort, capture-time, and append idempotency regressions.

Use the actual tracker and writer methods; only backend/Sheets I/O is replaced.
No credentials, network requests, or worksheet schema changes are needed.
"""
from __future__ import annotations

import asyncio
import os
import unittest
from datetime import date, datetime, timedelta, timezone
from threading import Lock
from unittest import mock

from scripts import track_performance as tp


def _instant(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def _row() -> dict:
    return {
        "symbol": "AAPL.US", "current_price": 100.0,
        "recommendation": "BUY", "final_action": "BUY",
        "investability_status": "INVESTABLE", "overall_score": 80.0,
        "forecast_reliability_score": 90.0, "data_quality_score": 90.0,
        "provider": "yahoo_chart", "forecast_price_1m": 110.0,
        "expected_roi_1m": 10.0, "next_earnings_date": "2026-10-08",
        "next_ex_div_date": "2026-10-09",
    }


class _MemoryWorksheet:
    def __init__(self):
        self.rows = []
        self.append_calls = 0
        self.lose_first_response = False

    def col_values(self, column):
        assert column == 2
        return ["Key"] + [row[1] for row in self.rows]

    def append_rows(self, rows, *, value_input_option):
        assert value_input_option == "RAW"
        self.append_calls += 1
        self.rows.extend([list(row) for row in rows])
        if self.lose_first_response and self.append_calls == 1:
            raise TimeoutError("committed, but response was lost")


def _signal_store(worksheet):
    # Bypass authentication only; exercise real refresh/filter/append methods.
    store = object.__new__(tp.SignalHistoryStore)
    store.ws = worksheet
    store.sheet = None
    store.spreadsheet_id = "offline"
    store.cache_keys = set()
    store.cache_lock = Lock()
    store.append_lock = Lock()
    store.backoff = tp.FullJitterBackoff(max_retries=2, base_delay=0, max_delay=0)
    return store


def _app():
    app = object.__new__(tp.PerformanceTrackerApp)
    app.args = tp.create_parser().parse_args(["--record", "--horizons", "1M"])
    # Ensure host environment cannot accidentally turn on unrelated I/O legs.
    for name in ("audit", "analyze", "export", "calibrate", "simulate"):
        setattr(app.args, name, False)
    app.spreadsheet_id = "offline"
    app.calibration_enabled = False
    app.signal_history_enabled = True
    app._load_calendar_context = lambda: {}
    app._track_selftest_ = lambda: True
    return app


class EvidenceDayTests(unittest.TestCase):
    def setUp(self):
        values = {
            "TRACK_DAY_KEY_MODE": "", "TRACK_SLOT_UTC": "",
            "TRACK_RUN_CREATED_AT": "", "TRACK_EVIDENCE_DAY": "",
            "TFB_TRACK_FORCE_DECISION_SYMBOLS": "0",
            "TFB_TRACK_SHADOW_COHORTS": "0", "TRACK_EVENT_CONTEXT": "1",
            "TFB_TREND_DAY_KEYED": "1",
        }
        self.environment = mock.patch.dict(os.environ, values)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.failures = mock.patch.object(tp, "_EVIDENCE_APPEND_FAILURES", [])
        self.failures.start()
        self.addCleanup(self.failures.stop)

    def test_manual_default_has_no_provenance_warnings(self):
        day, details = tp.resolve_track_evidence_day(_instant("2026-10-06T21:01:00Z"))
        self.assertEqual(day, date(2026, 10, 7))
        self.assertEqual(details["source"], "current-run")
        self.assertEqual(details["warnings"], ())

    def test_delayed_schedule_and_rerun_use_fixed_original_reference(self):
        for current in ("2026-10-06T22:30:00Z", "2026-10-09T22:30:00Z"):
            with self.subTest(current=current):
                day, details = tp.resolve_track_evidence_day(
                    _instant(current), mode="slot", slot_utc="20:17",
                    run_created_at="2026-10-06T21:30:00Z",
                )
                self.assertEqual(day, date(2026, 10, 6))
                self.assertEqual(details["slot_instant_utc"], "2026-10-06T20:17:00Z")
                self.assertEqual(details["source"], "scheduled-slot")
                self.assertTrue(details["drift"])

    def test_midnight_utc_slot_maps_to_riyadh_calendar_day(self):
        day, details = tp.resolve_track_evidence_day(
            _instant("2026-10-07T01:00:00Z"), mode="slot", slot_utc="23:47",
            run_created_at="2026-10-07T00:05:00Z",
        )
        self.assertEqual(day, date(2026, 10, 7))
        self.assertEqual(details["slot_instant_utc"], "2026-10-06T23:47:00Z")

    def test_observe_and_wallclock_report_candidate_without_changing_day(self):
        for mode in ("observe", "wallclock"):
            with self.subTest(mode=mode):
                day, details = tp.resolve_track_evidence_day(
                    _instant("2026-10-06T22:30:00Z"), mode=mode, slot_utc="20:17",
                    run_created_at="2026-10-06T21:30:00Z",
                )
                self.assertEqual(day, date(2026, 10, 7))
                self.assertEqual(details["candidate_day"], "2026-10-06")
                self.assertTrue(details["candidate_drift"])
                self.assertFalse(details["drift"])

    def test_explicit_day_is_authoritative(self):
        day, details = tp.resolve_track_evidence_day(
            _instant("2026-10-09T22:30:00Z"), mode="wallclock", slot_utc="20:17",
            run_created_at="2026-10-06T21:30:00Z", evidence_day="2026-10-05",
        )
        self.assertEqual(day, date(2026, 10, 5))
        self.assertEqual(details["source"], "explicit-day")

    def test_malformed_schedule_provenance_falls_back_to_current_run(self):
        for reference in (
            "bad", "2026-10-06", "2026-10-06T21:30:00",
            "0001-01-01T00:00:00+14:00", "9999-12-31T23:59:59-12:00",
        ):
            with self.subTest(reference=reference):
                day, details = tp.resolve_track_evidence_day(
                    _instant("2026-10-06T22:30:00Z"), mode="slot", slot_utc="20:17",
                    run_created_at=reference,
                )
                self.assertEqual(day, date(2026, 10, 7))
                self.assertIn("invalid TRACK_RUN_CREATED_AT", details["warnings"])
                self.assertEqual(details["source"], "current-run")
        for slot in ("24:00", "12:60", "no-slot"):
            with self.subTest(slot=slot):
                day, details = tp.resolve_track_evidence_day(
                    _instant("2026-10-06T22:30:00Z"), mode="slot", slot_utc=slot,
                    run_created_at="2026-10-06T21:30:00Z",
                )
                self.assertEqual(day, date(2026, 10, 7))
                self.assertIn("invalid TRACK_SLOT_UTC", details["warnings"])

    def test_invalid_explicit_day_and_future_reference_are_not_applied(self):
        for override in ("2026-1-05", "2026-02-30"):
            with self.subTest(override=override):
                day, details = tp.resolve_track_evidence_day(
                    _instant("2026-10-06T22:30:00Z"), mode="slot", slot_utc="20:17",
                    run_created_at="2026-10-10T21:30:00Z", evidence_day=override,
                )
                self.assertEqual(day, date(2026, 10, 7))
                self.assertIn("invalid TRACK_EVIDENCE_DAY", details["warnings"])
                self.assertIn("TRACK_RUN_CREATED_AT is after current run time", details["warnings"])

    def test_cohort_changes_keys_only_not_capture_maturity_or_events(self):
        app = _app()
        captured = _instant("2026-10-06T22:30:00Z").astimezone(tp._RIYADH_TZ)
        cohort = date(2026, 10, 6)
        with mock.patch.object(tp.RiyadhTime, "now", side_effect=AssertionError("extra wall clock")):
            records = asyncio.run(app.record_from_top10(
                [], rows=[_row()], evidence_day=cohort, captured_at=captured
            ))
            snapshots = app._build_signal_snapshots(
                [_row()], evidence_day=cohort, captured_at=captured
            )
        record, snapshot = records[0], snapshots[0]
        self.assertEqual(record.key, "AAPL.US|1M|20261006")
        self.assertEqual(snapshot.key, "AAPL.US|20261006")
        self.assertEqual(record.date_recorded, captured)
        self.assertEqual(snapshot.date_recorded, captured)
        self.assertEqual(snapshot.recorded_at, captured.astimezone(timezone.utc))
        self.assertEqual(record.last_updated, captured.astimezone(timezone.utc))
        self.assertEqual(record.target_date, captured + timedelta(days=record.horizon.days))
        self.assertEqual(record.status, tp.PerformanceStatus.ACTIVE)
        self.assertEqual(snapshot.days_to_earnings, 1)
        self.assertEqual(snapshot.days_to_exdiv, 2)
        self.assertEqual(record.to_dict()["date_recorded_riyadh"], "2026-10-07 01:30:00")
        self.assertEqual(snapshot.to_dict()["date_riyadh"], "2026-10-07")
        self.assertEqual(asyncio.run(app.record_from_top10(
            records, rows=[_row()], evidence_day=cohort,
            captured_at=captured + timedelta(hours=1),
        )), [])

    def test_sheet_roundtrip_preserves_existing_key_suffix(self):
        app = _app()
        captured = _instant("2026-10-06T22:30:00Z")
        cohort = date(2026, 10, 6)
        record = asyncio.run(app.record_from_top10(
            [], rows=[_row()], evidence_day=cohort, captured_at=captured
        ))[0]
        snapshot = app._build_signal_snapshots(
            [_row()], evidence_day=cohort, captured_at=captured
        )[0]
        perf_store = object.__new__(tp.PerformanceStore)
        signal_store = _signal_store(_MemoryWorksheet())
        for item, store, headers, model, serializer in (
            (record, perf_store, tp.PerformanceStore.HEADERS, tp.PerformanceRecord, "_record_to_row"),
            (snapshot, signal_store, tp.SignalHistoryStore.HEADERS, tp.SignalSnapshot, "_snapshot_to_row"),
        ):
            with self.subTest(model=model.__name__):
                row = getattr(store, serializer)(item)
                self.assertEqual(len(row), len(headers))
                restored = model.from_sheet_row(row, headers)
                self.assertEqual(restored.key, item.key)
                self.assertEqual(restored.evidence_date, cohort)
                self.assertEqual(restored.date_recorded.date(), date(2026, 10, 7))
                # Old rows lacking a Key still use their real recorded date.
                row[headers.index("Key")] = ""
                old = model.from_sheet_row(row, headers)
                self.assertEqual(old.evidence_date, date(2026, 10, 7))

    def test_day_keyed_trends_use_cohorts_for_delayed_capture(self):
        app = _app()
        previous = app._build_signal_snapshots(
            [_row()], evidence_day=date(2026, 10, 6),
            captured_at=_instant("2026-10-07T03:00:00Z"),
        )[0]
        current_row = _row() | {"recommendation": "SELL", "overall_score": 40.0}
        current = app._build_signal_snapshots(
            [current_row], evidence_day=date(2026, 10, 7),
            captured_at=_instant("2026-10-07T00:00:00Z"),
        )[0]
        trend = tp.SignalTrendAnalyzer().analyze([previous, current])[0]
        self.assertEqual(trend.latest_date, "2026-10-07")
        self.assertEqual(trend.current_action, "SELL")
        self.assertEqual(trend.flip_count, 1)

    def test_sequential_fresh_processes_and_backstop_do_not_duplicate(self):
        worksheet = _MemoryWorksheet()
        app = _app()
        snapshot = app._build_signal_snapshots(
            [_row()], evidence_day=date(2026, 10, 6),
            captured_at=_instant("2026-10-06T22:30:00Z"),
        )[0]
        with mock.patch.object(tp, "_capacity_block_reason", return_value=""):
            self.assertEqual(_signal_store(worksheet).append_snapshots([snapshot, snapshot]), 1)
            # Empty cache in a second process must still consult existing keys.
            self.assertEqual(_signal_store(worksheet).append_snapshots([snapshot]), 0)
        self.assertEqual(worksheet.append_calls, 1)
        self.assertEqual([row[1] for row in worksheet.rows], [snapshot.key])

    def test_lost_append_response_is_reconciled_before_retry(self):
        worksheet = _MemoryWorksheet()
        worksheet.lose_first_response = True
        snapshot = _app()._build_signal_snapshots(
            [_row()], evidence_day=date(2026, 10, 6),
            captured_at=_instant("2026-10-06T22:30:00Z"),
        )[0]
        with mock.patch.object(tp, "_capacity_block_reason", return_value=""):
            self.assertEqual(_signal_store(worksheet).append_snapshots([snapshot]), 1)
        self.assertEqual(worksheet.append_calls, 1)
        self.assertEqual([row[1] for row in worksheet.rows], [snapshot.key])

    def test_unreadable_or_malformed_key_column_refuses_append(self):
        snapshot = _app()._build_signal_snapshots([_row()])[0]
        for values in (None, [], [snapshot.key], ["Wrong Header", snapshot.key]):
            with self.subTest(values=values):
                worksheet = _MemoryWorksheet()
                worksheet.col_values = mock.Mock(return_value=values)
                self.assertEqual(_signal_store(worksheet).append_snapshots([snapshot]), 0)
                self.assertEqual(worksheet.append_calls, 0)
        worksheet = _MemoryWorksheet()
        worksheet.col_values = mock.Mock(side_effect=TimeoutError("read unavailable"))
        self.assertEqual(_signal_store(worksheet).append_snapshots([snapshot]), 0)
        self.assertEqual(worksheet.append_calls, 0)
        self.assertTrue(tp._evidence_append_failures())

    def test_run_once_resolves_before_io_and_shares_post_fetch_capture(self):
        for signals_enabled in (True, False):
            with self.subTest(signals_enabled=signals_enabled):
                events = []
                app = _app()
                app.signal_history_enabled = signals_enabled
                worksheet = _MemoryWorksheet()
                app.signal_store = _signal_store(worksheet)
                records = []

                class Store:
                    @staticmethod
                    def is_available():
                        return True

                    @staticmethod
                    def load_records(limit):
                        events.append("load")
                        return []

                    @staticmethod
                    def append_records(new):
                        events.append("append")
                        records.extend(new)
                        return True

                class Backend:
                    base_url = "https://offline.invalid"

                    @staticmethod
                    async def get_top10_rows(criteria_overrides):
                        events.append("fetch")
                        return [_row()], {}

                app.store = Store()
                app.backend = Backend()
                clocks = iter([
                    _instant("2026-10-06T20:59:59Z"),
                    _instant("2026-10-06T21:00:01Z"),
                ])

                def now():
                    events.append("clock")
                    return next(clocks).astimezone(tp._RIYADH_TZ)

                with mock.patch.object(tp.RiyadhTime, "now", side_effect=now) as clock, \
                     mock.patch.object(tp, "_capacity_block_reason", return_value=""), \
                     mock.patch.object(tp, "_shadow_cohorts_enabled", return_value=False):
                    self.assertEqual(asyncio.run(app.run_once()), 0)
                self.assertEqual(events[:4], ["clock", "load", "fetch", "clock"])
                self.assertEqual(clock.call_count, 2)
                self.assertEqual(events.count("fetch"), 1)
                self.assertEqual(records[0].key, "AAPL.US|1M|20261006")
                self.assertEqual(records[0].date_recorded.date(), date(2026, 10, 7))
                if signals_enabled:
                    snapshot = tp.SignalSnapshot.from_sheet_row(
                        worksheet.rows[0], tp.SignalHistoryStore.HEADERS
                    )
                    self.assertEqual(snapshot.key, "AAPL.US|20261006")
                    self.assertEqual(snapshot.recorded_at, records[0].date_recorded)
                    self.assertEqual(snapshot.date_recorded.date(), date(2026, 10, 7))
                else:
                    self.assertEqual(worksheet.rows, [])


if __name__ == "__main__":
    unittest.main()
