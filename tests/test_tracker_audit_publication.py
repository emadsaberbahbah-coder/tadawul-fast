"""An audited calibration may describe only acknowledged stored outcomes.

The real run_once orchestration is exercised with an in-process executor and
synthetic stores. No workbook, quote provider or backend is contacted.
"""
import asyncio
from concurrent.futures import Executor, Future
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from scripts import track_performance as track


PRIVATE_ERROR = "synthetic private URL https://example.invalid/?api_token=do-not-log"


class InlineExecutor(Executor):
    def submit(self, fn, /, *args, **kwargs):
        future = Future()
        try:
            future.set_result(fn(*args, **kwargs))
        except BaseException as exc:
            future.set_exception(exc)
        return future


@pytest.fixture
def audit_app(monkeypatch):
    now = datetime(2026, 10, 19, 21, tzinfo=timezone.utc)
    record = track.PerformanceRecord(
        record_id="synthetic-audit", symbol="SYNTH.US",
        horizon=track.HorizonType.WEEK_1, date_recorded=now-timedelta(days=7),
        entry_price=100, entry_recommendation=track.RecommendationType.BUY,
        entry_score=70, entry_risk_bucket="MEDIUM", entry_confidence="HIGH",
        origin_tab="synthetic", target_price=110, target_roi=10, target_date=now,
        status=track.PerformanceStatus.ACTIVE,
    )
    events = []
    state = {"available": True, "save": True, "s1": True,
             "reliability": {"published": True, "env": "", "changed": False}}

    def operation(name, result=None):
        def call(*_args, **_kwargs):
            events.append(name)
            value = state[name] if name in state else result
            if isinstance(value, Exception):
                raise value
            return value
        return call

    store = SimpleNamespace(
        is_available=lambda: state["available"],
        _last_load_succeeded=True,
        _last_load_complete=True,
        load_records=Mock(side_effect=operation("load", [record])),
        save_records=Mock(side_effect=operation("save")),
        update_summary=Mock(side_effect=operation("summary", True)),
    )
    app = object.__new__(track.PerformanceTrackerApp)
    app.store = store
    app.args = SimpleNamespace(record=False, audit=True, analyze=True, export=False,
                               calibrate=False, simulate=False, max_records=100, verbose=False)
    app.signal_history_enabled = False
    app.calibration_enabled = True
    app.analyzer = track.PerformanceAnalyzer()
    app._track_selftest_ = Mock()
    app._append_runlog_verdict = Mock(side_effect=operation("verdict"))
    app._publish_s1_calibration = Mock(side_effect=operation("s1"))
    app._compute_calibration = Mock(return_value=SimpleNamespace())
    app._publish_reliability_calibration = Mock(side_effect=operation("reliability"))

    async def audit(records):
        events.append("audit")
        records[0].status = track.PerformanceStatus.MATURED
        records[0].realized_roi = 10
        records[0].outcome = "WIN"
        records[0].maturity_date = now
        return records

    app.audit_active_records = audit
    monkeypatch.setattr(track, "_get_executor", lambda: InlineExecutor())
    monkeypatch.setattr(track, "_canonical_targets_enabled", lambda: False)
    monkeypatch.setattr(track, "_evidence_append_failures", lambda: [])
    monkeypatch.setattr(track, "render_calibration_report", Mock())
    monkeypatch.setenv("TFB_S1_CALIBRATION", "1")
    monkeypatch.setenv("TRACK_PUBLISH_CALIBRATION", "1")
    return app, state, events


def test_outcomes_are_acknowledged_before_either_calibration_publication(audit_app):
    app, _state, events = audit_app
    assert asyncio.run(app.run_once()) == 0
    assert events.index("audit") < events.index("save") < events.index("s1")
    assert events.index("save") < events.index("reliability")


@pytest.mark.parametrize("failure", [False, None, PermissionError(PRIVATE_ERROR)])
def test_failed_outcome_persistence_blocks_all_derived_publication(audit_app, caplog, failure):
    app, state, events = audit_app
    state["save"] = failure
    assert asyncio.run(app.run_once()) == 1
    assert events == ["load", "audit", "save"]
    app._publish_s1_calibration.assert_not_called()
    app._publish_reliability_calibration.assert_not_called()
    app._compute_calibration.assert_not_called()
    assert "Performance_Log" in caplog.text and "partial writes possible" in caplog.text
    assert PRIVATE_ERROR not in caplog.text and "api_token=" not in caplog.text


def test_unavailable_outcome_store_blocks_audit_before_price_requests(audit_app):
    app, state, events = audit_app
    state["available"] = False
    assert asyncio.run(app.run_once()) == 1
    assert events == []


def test_unacknowledged_history_read_blocks_recording_audit_save_and_calibration(audit_app):
    app, _state, events = audit_app
    app.args.record = True
    app.store.load_records.side_effect = lambda *_args: []
    app.store._last_load_succeeded = False
    assert asyncio.run(app.run_once()) == 1
    assert events == []
    app.store.save_records.assert_not_called()
    app._publish_s1_calibration.assert_not_called()
    app._publish_reliability_calibration.assert_not_called()


def test_analyze_without_audit_cannot_publish_empty_calibration_after_failed_read(audit_app):
    app, _state, _events = audit_app
    app.args.audit = False
    app.store.load_records.side_effect = lambda *_args: []
    app.store._last_load_succeeded = False
    assert asyncio.run(app.run_once()) == 1
    app.store.update_summary.assert_not_called()
    app._compute_calibration.assert_not_called()
    app._publish_reliability_calibration.assert_not_called()


def test_capped_history_read_cannot_authorize_full_grid_rewrite(audit_app):
    app, _state, events = audit_app
    app.store._last_load_complete = False
    assert asyncio.run(app.run_once()) == 1
    assert events == ["load"]
    app.store.save_records.assert_not_called()


@pytest.mark.parametrize("stage", ["s1", "reliability"])
@pytest.mark.parametrize("exception", [False, True])
def test_required_calibration_write_failure_is_nonzero_and_sanitized(
    audit_app, caplog, stage, exception,
):
    app, state, events = audit_app
    state[stage] = (PermissionError(PRIVATE_ERROR) if exception else
                    False if stage == "s1" else {"published": False})
    assert asyncio.run(app.run_once()) == 1
    assert "save" in events and stage in events
    assert PRIVATE_ERROR not in caplog.text and "api_token=" not in caplog.text


def test_explicitly_disabled_calibration_publishers_remain_zero_write(audit_app, monkeypatch):
    app, _state, events = audit_app
    monkeypatch.setenv("TFB_S1_CALIBRATION", "0")
    monkeypatch.setenv("TRACK_PUBLISH_CALIBRATION", "0")
    assert asyncio.run(app.run_once()) == 0
    assert "save" in events
    app._publish_s1_calibration.assert_not_called()
    app._publish_reliability_calibration.assert_not_called()


def test_real_store_save_failure_is_sanitized_and_acknowledges_partial_io(caplog):
    store = object.__new__(track.PerformanceStore)
    writes = []

    class Worksheet:
        row_count = 100

        def batch_clear(self, ranges):
            writes.append(("clear", ranges))
            raise PermissionError(PRIVATE_ERROR)

    store.ws = Worksheet()
    store.backoff = SimpleNamespace(execute_sync=lambda fn: fn())
    assert store.save_records([]) is False
    assert writes
    assert "PermissionError" in caplog.text and "partial writes possible" in caplog.text
    assert PRIVATE_ERROR not in caplog.text and "api_token=" not in caplog.text


def test_real_failed_read_differs_from_acknowledged_empty_history(caplog):
    store = object.__new__(track.PerformanceStore)
    store.ws = SimpleNamespace(row_count=100, get=Mock(side_effect=PermissionError(PRIVATE_ERROR)))
    store.cache = {}
    store.cache_lock = track.Lock()
    store.backoff = SimpleNamespace(execute_sync=lambda fn: fn())
    assert store.load_records() == []
    assert store._last_load_succeeded is False
    assert PRIVATE_ERROR not in caplog.text and "api_token=" not in caplog.text
    store.ws.get.side_effect = None
    store.ws.get.return_value = []
    assert store.load_records() == []
    assert store._last_load_succeeded is True
    # A later failed read must reset an earlier successful acknowledgement.
    store.ws.get.side_effect = PermissionError(PRIVATE_ERROR)
    assert store.load_records() == []
    assert store._last_load_succeeded is False


@pytest.mark.parametrize("response", [None, {}, "malformed"])
def test_malformed_read_is_never_acknowledged_as_empty_history(response):
    store = object.__new__(track.PerformanceStore)
    store.ws = SimpleNamespace(row_count=100, get=Mock(return_value=response))
    store.backoff = SimpleNamespace(execute_sync=lambda fn: fn())
    assert store.load_records() == []
    assert store._last_load_succeeded is False
    assert store._last_load_complete is False


@pytest.mark.parametrize("malformed_row", [
    None, False, 0, 7, "", "12X45678901234567890123456789012", {},
    {0: "synthetic-id", 1: "synthetic-key", 2: "SYNTH.US"},
])
def test_malformed_row_container_blocks_entire_read_before_cache_or_extent_mutation(
    audit_app, malformed_row,
):
    app, _state, _events = audit_app
    record = app.store.load_records()[0]
    store = object.__new__(track.PerformanceStore)
    valid_row = store._record_to_row(record)
    # A valid first row must not partially replace the cache before a later
    # malformed row is rejected. Position loss is unsafe for the full saver.
    store.ws = SimpleNamespace(row_count=100, get=Mock(return_value=[valid_row, malformed_row]))
    sentinel = object()
    store.cache = {"previous-record": sentinel}
    store.cache_lock = track.Lock()
    store.backoff = SimpleNamespace(execute_sync=lambda fn: fn())
    store._loaded_extent = 99
    assert store.load_records() == []
    assert store._last_load_succeeded is False
    assert store._last_load_complete is False
    assert store.cache == {"previous-record": sentinel}
    assert store._loaded_extent == 99


@pytest.mark.parametrize("container", [list, tuple])
def test_valid_row_containers_and_blank_rows_keep_normal_parsing(audit_app, container):
    app, _state, _events = audit_app
    record = app.store.load_records()[0]
    store = object.__new__(track.PerformanceStore)
    store.ws = SimpleNamespace(row_count=100, get=Mock(return_value=[
        [], container(store._record_to_row(record)), (), ["", ""],
    ]))
    store.cache = {}
    store.cache_lock = track.Lock()
    store.backoff = SimpleNamespace(execute_sync=lambda fn: fn())
    result = store.load_records()
    assert len(result) == 1
    assert result[0].record_id == record.record_id
    assert result[0].symbol == record.symbol
    assert result[0].entry_price == record.entry_price
    assert result[0].status == record.status
    assert store._last_load_succeeded is True
    assert store._last_load_complete is True


def test_successful_but_capped_read_is_explicitly_incomplete():
    store = object.__new__(track.PerformanceStore)
    store.ws = SimpleNamespace(row_count=50000, get=Mock(return_value=[]))
    store.cache = {}
    store.cache_lock = track.Lock()
    store.backoff = SimpleNamespace(execute_sync=lambda fn: fn())
    assert store.load_records(max_records=100) == []
    assert store._last_load_succeeded is True
    assert store._last_load_complete is False
    assert store.load_records() == []
    assert store._last_load_succeeded is True
    assert store._last_load_complete is True


@pytest.mark.parametrize("failed", ["status_read", "status_write", "calibration_read", "calibration_write"])
def test_real_reliability_publisher_cannot_acknowledge_partial_failure(monkeypatch, caplog, failed):
    monkeypatch.setenv("TRACK_PUBLISH_CALIBRATION", "1")
    writes = []

    class Worksheet:
        def __init__(self, name):
            self.name = name

        def get(self, _range):
            if failed == "status_read":
                raise PermissionError(PRIVATE_ERROR)
            return []

        def update(self, **kwargs):
            writes.append((self.name, kwargs["range_name"]))
            if failed == self.name + "_write":
                raise PermissionError(PRIVATE_ERROR)

        def append_row(self, *_args, **_kwargs):
            pytest.fail("incomplete publication must not claim factors changed")

    class Workbook:
        def worksheet(self, title):
            if title == "_Status":
                return Worksheet("status")
            if title == track.S1_CAL_TAB:
                if failed == "calibration_read":
                    raise PermissionError(PRIVATE_ERROR)
                return Worksheet("calibration")
            return Worksheet("runlog")

    app = object.__new__(track.PerformanceTrackerApp)
    app.store = SimpleNamespace(sheet=Workbook())
    report = SimpleNamespace(by_investability=[], min_sample=10, brier_score=None)
    assert app._publish_reliability_calibration(report)["published"] is False
    if failed.startswith("calibration_"):
        assert writes[0] == ("status", "L1:M1")  # real partial I/O occurred
    assert PRIVATE_ERROR not in caplog.text and "api_token=" not in caplog.text


@pytest.mark.parametrize("has_checkpoint_tab", [True, False])
def test_reliability_insufficient_evidence_is_valid_publication_and_missing_optional_tab_stays_optional(
    monkeypatch, has_checkpoint_tab,
):
    monkeypatch.setenv("TRACK_PUBLISH_CALIBRATION", "1")
    # Missing worksheet is a distinct provider exception, never a generic
    # permission/transport error. This fixture preserves the established
    # analyze-only behavior when the optional S-1 tab has never been created.
    class MissingWorksheet(Exception):
        pass

    monkeypatch.setattr(track, "gspread", SimpleNamespace(WorksheetNotFound=MissingWorksheet))
    writes = []

    class Worksheet:
        def __init__(self, title):
            self.title = title

        def get(self, _range):
            return []

        def update(self, **kwargs):
            writes.append((self.title, kwargs["range_name"], kwargs["values"]))

        def append_row(self, *_args, **_kwargs):
            return None

    class Workbook:
        def worksheet(self, title):
            if title == track.S1_CAL_TAB and not has_checkpoint_tab:
                raise MissingWorksheet()
            return Worksheet(title)

    app = object.__new__(track.PerformanceTrackerApp)
    app.store = SimpleNamespace(sheet=Workbook())
    report = track.ReliabilityCalibrator(10, 20).calibrate([])
    assert report.global_sufficient is False
    result = app._publish_reliability_calibration(report)
    assert result["published"] is True and result["env"] == ""
    assert writes[0][:2] == ("_Status", "L1:M1")
    assert "NONE" in writes[0][2][0][1]
    assert len(writes) == (2 if has_checkpoint_tab else 1)


def test_pending_checkpoint_is_successful_acknowledged_measurement(monkeypatch):
    monkeypatch.setenv("TFB_S1_CALIBRATION", "1")
    writes = []
    worksheet = SimpleNamespace(update=lambda *args, **kwargs: writes.append((args, kwargs)))
    app = object.__new__(track.PerformanceTrackerApp)
    app.store = SimpleNamespace(
        is_available=lambda: True,
        sheet=SimpleNamespace(worksheet=lambda _title: worksheet),
        backoff=SimpleNamespace(execute_sync=lambda fn, *args: fn(*args)),
    )
    assert app._publish_s1_calibration([]) is True
    assert writes[0][0][1][1][1] == "PENDING"
