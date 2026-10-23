"""Merged backup boundaries: rejected exports cannot start resumable storage.

Valid containers use the real Drive/GCS upload helpers and retain their resume
behavior; failed transfers never advance into retention. All I/O is synthetic.
"""
from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import pytest

from scripts import backup_workbook as backup
from tests.workbook_backup_fixtures import workbook_export


class _Response:
    def __init__(self, status, *, headers=None, body=None):
        self.status_code = status
        self.headers = headers or {}
        self._body = body or {}

    def raise_for_status(self):
        assert self.status_code < 400

    def json(self):
        return self._body


@pytest.mark.parametrize("dest", ("drive", "gcs"))
@pytest.mark.parametrize("outcome", ("invalid", "resumed", "exhausted"))
def test_validation_precedes_resume_and_retention_requires_success(monkeypatch, dest, outcome):
    events, calls, receipts = [], [], []
    payload = workbook_export() if outcome != "invalid" else b"<html>failed export</html>"
    fault_type = backup._transport_exception_types()[0]
    today = dt.date(2026, 10, 24)

    class Session:
        def post(self, _url, **_kwargs):
            events.append("open_upload")
            return _Response(200, headers={"Location": "https://synthetic.invalid/upload"})

        def put(self, _url, *, data, headers, timeout):
            calls.append((data, dict(headers)))
            if len(calls) == 1:
                events.append("transfer_fault")
                raise fault_type("synthetic interruption")
            if len(calls) == 2:
                events.append("status_probe")
                return _Response(308, headers={"Range": "bytes=0-63"})
            events.append("resume")
            if outcome == "exhausted":
                raise fault_type("synthetic final interruption")
            return _Response(200, body={"id": "new", "name": backup.daily_name(today)})

    session = Session()
    for key, value in {
        "DEFAULT_SPREADSHEET_ID": "synthetic-sheet",
        "TFB_BACKUP_FOLDER_ID": "synthetic-folder",
        "TFB_BACKUP_GCS_BUCKET": "synthetic-bucket",
        "TFB_BACKUP_DEST": dest,
        "TFB_BACKUP_MIN_BYTES": "1",
        "TFB_BACKUP_KEEP_DAILY": "1",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("TFB_BACKUP_LOCAL_COPY", raising=False)
    monkeypatch.setattr(backup, "_credentials", lambda: SimpleNamespace(_tfb_client_email="offline"))
    monkeypatch.setattr(backup, "_session", lambda _: session)
    monkeypatch.setattr(backup, "_now_riyadh", lambda: dt.datetime(2026, 10, 24, tzinfo=backup._RIYADH))
    monkeypatch.setattr(backup, "export_xlsx", lambda *_: events.append("export") or payload)
    validate = backup._validate_export_xlsx

    def validated(data):
        events.append("validate")
        return validate(data)

    monkeypatch.setattr(backup, "_validate_export_xlsx", validated)
    monkeypatch.setattr(backup, "_write_local_copy", lambda *_: events.append("local_copy"))
    resume = backup._resumable_put_with_resume
    monkeypatch.setattr(backup, "_resumable_put_with_resume", lambda sess, loc, data:
                        resume(sess, loc, data, attempts=2, sleep=lambda _: None))
    monkeypatch.setattr(backup, "_store_list", lambda *_: events.append("list") or [
        {"id": "old", "name": backup.daily_name(dt.date(2026, 10, 23))},
        {"id": "new", "name": backup.daily_name(today)},
        {"id": "monthly", "name": backup.monthly_name(today)},
    ])
    monkeypatch.setattr(backup, "_store_delete", lambda *_: events.append("delete"))
    monkeypatch.setattr(backup, "_store_copy", lambda *_: pytest.fail("existing monthly must remain"))
    monkeypatch.setattr(backup, "append_run_log", lambda *args: receipts.append(args))

    assert backup.run_backup(dry_run=False) == (0 if outcome == "resumed" else 3)
    assert events[:2] == ["export", "validate"]
    if outcome == "invalid":
        assert events == ["export", "validate"]
        assert calls == []
        assert receipts[-1][2]["stage"] == "export_validation"
        return

    assert events[2:7] == ["local_copy", "open_upload", "transfer_fault", "status_probe", "resume"]
    assert calls[0][0] == payload
    assert calls[1] == (b"", {"Content-Range": f"bytes */{len(payload)}"})
    assert calls[2][0] == payload[64:]
    assert calls[2][1]["Content-Range"] == f"bytes 64-{len(payload) - 1}/{len(payload)}"
    if outcome == "exhausted":
        assert len(events) == 7
        assert receipts[-1][2]["stage"] == "upload"
    else:
        assert events[7:] == ["list", "delete"]
        assert receipts[-1][0] == "OK"
        assert receipts[-1][2]["pruned"] == 1
