"""P-170c: run_dashboard_sync v6.69.1 writes the run's provenance into each
_Run_Log Details JSON, and tfb_export_audit v1.3.0 reads it back."""
import datetime as dt
import json

import pytest

from scripts import run_dashboard_sync as rds

ENV = ("GITHUB_EVENT_NAME", "GITHUB_EVENT_PATH", "GITHUB_WORKFLOW_REF", "GITHUB_RUN_ID", "TFB_SYNC_IDFW_RUNLOG")


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.setattr(rds, "_RUN_SCHEDULE_CACHE", [])
    for k in ENV:
        monkeypatch.delenv(k, raising=False)


def actions(monkeypatch, tmp_path, event, workflow, payload=None):
    path = tmp_path / "event.json"
    path.write_text(json.dumps(payload or {}), encoding="utf-8")
    monkeypatch.setenv("GITHUB_EVENT_NAME", event)
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(path))
    monkeypatch.setenv("GITHUB_WORKFLOW_REF", f"owner/repo/.github/workflows/{workflow}@refs/heads/main")
    monkeypatch.setenv("GITHUB_RUN_ID", "4242")


def test_rows_written_outside_actions_are_unchanged():
    d = json.loads(rds._runlog_meta_json('{"a": 1}'))
    assert set(d) == {"a", "run_id", "ts_utc"}


def test_schedule_run_records_event_cron_line_and_workflow(monkeypatch, tmp_path):
    actions(monkeypatch, tmp_path, "schedule", "daily_sync.yml", {"schedule": "17 12,20 * * *"})
    d = json.loads(rds._runlog_meta_json("{}"))
    assert (d["run_id"], d["event"], d["schedule"], d["workflow"]) == ("4242", "schedule", "17 12,20 * * *",
                                                                       "daily_sync.yml")


def test_dispatch_run_has_no_cron_line(monkeypatch, tmp_path):
    actions(monkeypatch, tmp_path, "workflow_dispatch", "page_refresh_recovery.yml", {"schedule": "ignored"})
    d = json.loads(rds._runlog_meta_json("{}"))
    assert d["event"] == "workflow_dispatch" and d["workflow"] == "page_refresh_recovery.yml"
    assert "schedule" not in d


def test_unreadable_payload_and_caller_keys_are_fail_open(monkeypatch, tmp_path):
    actions(monkeypatch, tmp_path, "schedule", "daily_sync.yml")
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(tmp_path / "missing.json"))
    d = json.loads(rds._runlog_meta_json('{"event": "caller", "workflow": "x"}'))
    assert d["event"] == "caller" and d["workflow"] == "x" and "schedule" not in d
    assert rds._runlog_meta_json("not json") == "not json"


class _Recorder:
    def __init__(self):
        self.rows = []

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def append(self, **kw):
        self.rows.extend(kw["body"]["values"])
        return self

    def execute(self):
        return {}


class _Sheets:
    def __init__(self, svc):
        self.svc = svc

    def _get_service(self):
        return self.svc


def test_id_firewall_row_carries_the_run_provenance(monkeypatch, tmp_path):
    # The [ID-FIREWALL] row is the first row of each market page; before v6.69.1 it had no run id.
    actions(monkeypatch, tmp_path, "workflow_dispatch", "daily_sync.yml")
    rec = _Recorder()
    rds._append_runlog_idfirewall(_Sheets(rec), "sid", "Market_Leaders", 3, [], [])
    (row,) = rec.rows
    d = json.loads(row[9])
    assert row[2] == "run_dashboard_sync" and d["klg_kept"] == 3
    assert (d["run_id"], d["event"], d["workflow"]) == ("4242", "workflow_dispatch", "daily_sync.yml")


def test_export_audit_reads_what_the_sync_writes(monkeypatch, tmp_path):
    pytest.importorskip("openpyxl")
    from scripts import tfb_export_audit as audit

    actions(monkeypatch, tmp_path, "workflow_dispatch", "daily_sync.yml")
    details = rds._runlog_meta_json("{}")
    header = ["Timestamp", "Level", "Action", "Page", "Status", "Message", "Endpoint", "HTTP", "ms", "Details JSON"]
    rows = [header,
            ["2026-10-03 06:14:00", "INFO", "tfbDispatchDailySync", "daily_sync.yml", "OK", "", "", 204, "", "{}"],
            ["2026-10-03 06:16:00", "INFO", "run_dashboard_sync", "Market_Leaders", "OK", "leg", "", "", "", details]]

    class Book:
        def rows(self, name):
            return rows if name == "_Run_Log" else None

    now = dt.datetime(2026, 10, 3, 11, tzinfo=dt.timezone.utc)
    fs, metrics = audit.check_runlog(Book(), 3, 7, 90000, "17 12,20", now, "2026-10-03")
    finding = {f["check"]: f for f in fs}["schedule_lateness"]
    assert finding["status"] == "PASS" and metrics["schedule_dispatched_runs"] == 1
    assert finding["examples"] == ["run 4242: dispatch 10-03 03:14Z started 03:16Z, +0h02m"]
