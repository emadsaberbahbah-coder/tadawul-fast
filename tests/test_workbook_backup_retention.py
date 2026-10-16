"""Real backup retention with repeated Drive names; all storage I/O is fake."""
from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import pytest

from scripts import backup_workbook as backup


def test_duplicate_names_do_not_consume_dated_retention_slots():
    daily = [backup.daily_name(dt.date(2026, 10, day)) for day in (14, 15, 16)]
    monthly = [backup.monthly_name(dt.date(2026, month, 1)) for month in (8, 9, 10)]
    keep, delete = backup.plan_pruning(daily + [daily[-1]] * 40 + monthly * 3 + ["notes.xlsx"], 2, 2)
    assert set(keep) == set(daily[1:] + monthly[1:])
    assert set(delete) == {daily[0], monthly[0]}
    assert set(keep).isdisjoint(delete)
    assert len(keep) == 4
    assert len(delete) == 2


@pytest.mark.parametrize("dest", ("drive", "oauth", "gcs"))
@pytest.mark.parametrize("duplicates", (False, True))
def test_real_backup_prunes_every_expired_id_once_and_preserves_kept_copies(monkeypatch, capsys, dest, duplicates):
    today = dt.date(2026, 10, 16)
    old = backup.daily_name(dt.date(2026, 10, 14))
    prior = backup.daily_name(dt.date(2026, 10, 15))
    current = backup.daily_name(today)
    month = backup.monthly_name(today)
    listing = [
        {"id": "expired-a", "name": old},
        {"id": "keep-prior", "name": prior},
        {"id": "keep-current-b", "name": current},
        {"id": "keep-month", "name": month},
        {"id": "foreign", "name": "operator-notes.xlsx"},
        {"id": "old-month-a", "name": backup.monthly_name(dt.date(2026, 8, 1))},
        {"id": "old-month-b", "name": backup.monthly_name(dt.date(2026, 9, 1))},
    ]
    if duplicates:
        listing += [{"id": "expired-b", "name": old},
                    {"id": "expired-a", "name": old},
                    {"id": "keep-current-a", "name": current}]
    monkeypatch.setenv("DEFAULT_SPREADSHEET_ID", "test-sheet")
    monkeypatch.setenv("TFB_BACKUP_FOLDER_ID", "test-folder")
    monkeypatch.setenv("TFB_BACKUP_GCS_BUCKET", "test-bucket")
    monkeypatch.setenv("TFB_BACKUP_DEST", dest)
    monkeypatch.setenv("TFB_BACKUP_MIN_BYTES", "1")
    monkeypatch.setenv("TFB_BACKUP_KEEP_DAILY", "2")
    monkeypatch.setenv("TFB_BACKUP_KEEP_MONTHLY", "1")
    monkeypatch.delenv("TFB_BACKUP_LOCAL_COPY", raising=False)
    monkeypatch.setattr(backup, "_now_riyadh", lambda: dt.datetime.combine(today, dt.time()))
    monkeypatch.setattr(backup, "_credentials", lambda: SimpleNamespace(_tfb_client_email="test"))
    monkeypatch.setattr(backup, "_session", lambda _: object())
    monkeypatch.setattr(backup, "_oauth_session", lambda: object())
    monkeypatch.setattr(backup, "drive_ensure_app_folder", lambda *_: "test-folder")
    monkeypatch.setattr(backup, "export_xlsx", lambda *_: b"offline-export")
    monkeypatch.setattr(backup, "_store_upload", lambda *_: {"id": "keep-current-b", "name": current})
    monkeypatch.setattr(backup, "_store_list", lambda *_: listing)
    monkeypatch.setattr(backup, "_store_copy", lambda *_: pytest.fail("existing month must not be recopied"))
    removed = []
    monkeypatch.setattr(backup, "_store_delete", lambda *args: removed.append(args[-1]))
    receipts = []
    monkeypatch.setattr(backup, "append_run_log", lambda *args: receipts.append(args))

    assert backup.run_backup(dry_run=False) == 0
    expected = ["expired-a", "old-month-a", "old-month-b"]
    if duplicates:
        expected.append("expired-b")
    assert removed == expected
    assert receipts[-1][0] == "OK"
    assert receipts[-1][2]["pruned"] == len(expected)
    assert "daily~2/2 monthly~1/1" in capsys.readouterr().err
