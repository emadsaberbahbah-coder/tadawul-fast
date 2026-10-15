"""backup_workbook.py v1.6.0 --attach: the DQ reports go beside the backups.

Runs the real module against an in-memory Drive (no network, no credentials):
upload-only, dated names, missing paths skipped, same-day replace, per-kind
pruning, refusal of the export / credentials, the unchanged default modes,
and the workflow step that calls it.
"""
from __future__ import annotations

import datetime as dt
import itertools
import json
import os
import re
from pathlib import Path

import pytest
import yaml

from scripts import backup_workbook as bw

ROOT = Path(__file__).resolve().parents[1]
TODAY = dt.datetime(2026, 10, 15, 2, 40, tzinfo=bw._RIYADH)
FOLDER = "FOLDER123"


class Resp:
    def __init__(self, code=200, payload=None, headers=None):
        self.status_code, self._payload, self.headers = code, payload or {}, headers or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            err = RuntimeError(f"HTTP {self.status_code}")
            err.response = self
            raise err


class FakeDrive:
    """Just enough of the Drive v3 + Docs export surface the script uses."""

    def __init__(self):
        self.files, self.calls, self.pending = {}, [], {}
        self.ids = (f"id{n}" for n in itertools.count(1))

    def add(self, name, parent=FOLDER, folder=False):
        fid = next(self.ids)
        self.files[fid] = {"id": fid, "name": name, "parents": [parent], "folder": folder,
                           "mime": "", "data": b""}
        return fid

    def in_folder(self, parent=FOLDER):
        return sorted(f["name"] for f in self.files.values() if parent in f["parents"] and not f["folder"])

    def get(self, url, params=None, timeout=None):
        self.calls.append(("GET", url))
        if "/export?format=xlsx" in url:
            return _Bytes(b"PK\x03\x04" + b"x" * 2_000_000)
        assert url == "https://www.googleapis.com/drive/v3/files", url
        q = params["q"]
        if "google-apps.folder" in q:
            name = re.search(r"name='(.*)'", q).group(1)
            hits = [f for f in self.files.values() if f["folder"] and f["name"] == name]
            return Resp(200, {"files": [{"id": f["id"], "name": f["name"]} for f in hits]})
        parent = re.match(r"'(.+)' in parents", q).group(1)
        return Resp(200, {"files": [{"id": f["id"], "name": f["name"]} for f in self.files.values()
                                    if parent in f["parents"] and not f["folder"]]})

    def post(self, url, json=None, params=None, headers=None, timeout=None):
        self.calls.append(("POST", url))
        if url.startswith("https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable"):
            loc = f"loc{len(self.pending)}"
            self.pending[loc] = (json, headers)
            return Resp(200, headers={"Location": loc})
        if "/copy?" in url:                                  # monthly copy
            src = url.split("/files/", 1)[1].split("/copy", 1)[0]
            fid = self.add(json["name"], parent=json["parents"][0])
            self.files[fid].update(mime=self.files[src]["mime"], data=self.files[src]["data"])
            return Resp(200, {"id": fid, "name": json["name"]})
        assert url == "https://www.googleapis.com/drive/v3/files" and json["mimeType"].endswith("folder")
        return Resp(200, {"id": self.add(json["name"], parent="root", folder=True)})

    def put(self, loc, data=None, headers=None, timeout=None):
        meta, start_headers = self.pending.pop(loc)
        fid = self.add(meta["name"], parent=meta["parents"][0])
        self.files[fid].update(mime=headers["Content-Type"], data=data,
                               announced=start_headers["X-Upload-Content-Type"])
        return Resp(200, {"id": fid, "name": meta["name"]})

    def delete(self, url, timeout=None):
        fid = url.split("/files/", 1)[1].split("?", 1)[0]
        self.files.pop(fid)
        return Resp(204)


class _Bytes(Resp):
    def __init__(self, content):
        super().__init__(200)
        self.content = content


class Creds:
    _tfb_client_email = "svc@example.iam.gserviceaccount.com"


@pytest.fixture
def drive(monkeypatch):
    fake = FakeDrive()
    log = []
    monkeypatch.setattr(bw, "_credentials", lambda: Creds())
    monkeypatch.setattr(bw, "_session", lambda creds: fake)
    monkeypatch.setattr(bw, "_now_riyadh", lambda: TODAY)
    monkeypatch.setattr(bw, "append_run_log", lambda status, msg, det: log.append((status, msg, det)))
    for var in ("TFB_BACKUP_DEST", "TFB_BACKUP_LOCAL_COPY", "GOOGLE_APPLICATION_CREDENTIALS",
                "TFB_BACKUP_KEEP_DAILY", "TFB_BACKUP_KEEP_MONTHLY"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("TFB_BACKUP_FOLDER_ID", FOLDER)
    monkeypatch.setenv("DEFAULT_SPREADSHEET_ID", "SHEET1")
    fake.log = log
    return fake


def reports(tmp_path, *names):
    out = []
    for name in names:
        path = tmp_path / name
        path.write_text("# DQ scorecard\n" if name.endswith(".md") else '{"checks": 75}\n')
        out.append(str(path))
    return out


def test_two_reports_land_dated_and_a_missing_one_is_skipped(drive, tmp_path, capsys):
    md, js = reports(tmp_path, "dq_scorecard.md", "dq_report.json")
    missing = str(tmp_path / "dq_activation_receipt.json")
    assert bw.main(["--attach", md, js, missing]) == 0
    assert drive.in_folder() == ["DAILY_TFB_DQ_Report_2026-10-15.json", "DAILY_TFB_DQ_Scorecard_2026-10-15.md"]
    by_name = {f["name"]: f for f in drive.files.values()}
    assert by_name["DAILY_TFB_DQ_Scorecard_2026-10-15.md"]["mime"] == "text/markdown"
    assert by_name["DAILY_TFB_DQ_Report_2026-10-15.json"]["data"] == b'{"checks": 75}\n'
    assert by_name["DAILY_TFB_DQ_Report_2026-10-15.json"]["announced"] == "application/json"
    err = capsys.readouterr().err
    assert f"NOTE: attach: {missing} not found — skipped" in err
    # Upload-only: no workbook export, no workbook upload.
    assert not any("/export?format=xlsx" in url for _, url in drive.calls)
    assert not any(n.startswith("DAILY_TFB_Workbook_") for n in drive.in_folder())
    [(status, msg, det)] = drive.log
    assert status == "OK" and msg.startswith("[BACKUP-ATTACH v1.6.0] uploaded=2/3 skipped=1")
    assert "[BACKUP v" not in msg and "[RESTORE-TEST" not in msg
    # tfb_export_audit counts any row containing REFUS as an identity-guard refusal.
    assert "REFUS" not in msg.upper() and "REFUS" not in json.dumps(det).upper()


def test_nothing_present_exits_zero_before_any_credential(monkeypatch, tmp_path):
    def boom():
        raise AssertionError("credentials must not be touched")
    monkeypatch.setattr(bw, "_credentials", boom)
    assert bw.main(["--attach", str(tmp_path / "a.md"), str(tmp_path / "b.json")]) == 0


def test_same_day_rerun_replaces_and_pruning_keeps_30_per_kind(drive, tmp_path, monkeypatch):
    for i in range(1, 32):
        drive.add(bw.attach_name("dq_report.json", TODAY.date() - dt.timedelta(days=i)))
    for i in range(3):
        drive.add(bw.daily_name(TODAY.date() - dt.timedelta(days=i)))
    drive.add("operator_notes.txt")
    md, js = reports(tmp_path, "dq_scorecard.md", "dq_report.json")
    assert bw.main(["--attach", md, js]) == 0
    assert bw.main(["--attach", md, js]) == 0        # a re-run the same night
    names = drive.in_folder()
    dq_reports = [n for n in names if n.startswith("DAILY_TFB_DQ_Report_")]
    assert len(dq_reports) == 30 and dq_reports[-1] == "DAILY_TFB_DQ_Report_2026-10-15.json"
    assert names.count("DAILY_TFB_DQ_Scorecard_2026-10-15.md") == 1
    assert sum(n.startswith("DAILY_TFB_Workbook_") for n in names) == 3
    assert "operator_notes.txt" in names
    monkeypatch.setenv("TFB_BACKUP_KEEP_DAILY", "5")
    assert bw.main(["--attach", js]) == 0
    assert len([n for n in drive.in_folder() if n.startswith("DAILY_TFB_DQ_Report_")]) == 5


def test_export_and_credentials_are_refused(drive, tmp_path, monkeypatch, capsys):
    export = tmp_path / "tfb_export.xlsx"
    export.write_bytes(b"PK\x03\x04workbook")
    os.utime(export, (TODAY.timestamp(), TODAY.timestamp()))   # the reports take the export's date
    disguised = tmp_path / "export.json"
    disguised.write_bytes(b"PK\x03\x04workbook")
    key = tmp_path / "gcreds.json"
    key.write_text('{"type": "service_account", "private_key": "-----BEGIN PRIVATE KEY-----"}')
    token = tmp_path / "token.json"
    token.write_text('{"refresh_token": "1//x"}')
    monkeypatch.setenv("TFB_BACKUP_LOCAL_COPY", str(export))
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(key))
    [md] = reports(tmp_path, "dq_scorecard.md")
    assert bw.main(["--attach", str(export), str(disguised), str(key), str(token), md]) == 2
    assert drive.in_folder() == ["DAILY_TFB_DQ_Scorecard_2026-10-15.md"]
    err = capsys.readouterr().err
    assert err.count("blocked —") == 4 and "private_key" not in "".join(f["name"] for f in drive.files.values())
    assert drive.log[-1][0] == "FAIL" and "REFUS" not in drive.log[-1][1].upper()


def test_oauth_destination_uses_the_app_folder(drive, tmp_path, monkeypatch):
    monkeypatch.setenv("TFB_BACKUP_DEST", "oauth")
    monkeypatch.setattr(bw, "_oauth_session", lambda: drive)
    [md] = reports(tmp_path, "dq_scorecard.md")
    assert bw.main(["--attach", md]) == 0
    [folder] = [f for f in drive.files.values() if f["folder"]]
    assert folder["name"] == "TFB Workbook Backups"
    assert drive.in_folder(folder["id"]) == ["DAILY_TFB_DQ_Scorecard_2026-10-15.md"]
    assert drive.in_folder(FOLDER) == []


def test_upload_failure_exits_3_and_setup_failure_is_tagged(drive, tmp_path, monkeypatch):
    [md] = reports(tmp_path, "dq_scorecard.md")
    monkeypatch.setattr(drive, "put", lambda *a, **k: Resp(500))
    assert bw.main(["--attach", md]) == 3
    monkeypatch.setattr(bw, "_credentials", lambda: (_ for _ in ()).throw(RuntimeError("no Google credentials in env")))
    assert bw.main(["--attach", md]) == 2
    assert drive.log[-1][1].startswith("[BACKUP-ATTACH v1.6.0] FAIL credentials setup")


def test_attach_is_exclusive_with_the_other_modes():
    for other in ("--dry-run", "--restore-test", "--selftest"):
        with pytest.raises(SystemExit):
            bw.main([other, "--attach", "x.md"])


def test_default_modes_unchanged_dry_run_and_workbook_upload_mime(drive, capsys):
    assert bw.main(["--dry-run"]) == 0
    captured = capsys.readouterr()
    assert captured.out == ""
    lines = [re.sub(r"\d+\.\d+s", "Ts", line) for line in captured.err.splitlines()]
    assert lines == [
        "[workbook_backup v1.6.0] preflight: export_account=svc@example.iam.gserviceaccount.com dest=drive "
        "sheet_id=SHEET1 folder_id=FOLDER123 dry_run=True",
        "[workbook_backup v1.6.0] export: 2,000,004 bytes in Ts sha256=" + bw.sha256_hex(b"PK\x03\x04" + b"x" * 2_000_000)[:12],
        "[workbook_backup v1.6.0] dry-run — export verified, nothing uploaded",
    ]
    assert drive.in_folder() == []
    assert bw.main([]) == 0
    assert drive.in_folder() == ["DAILY_TFB_Workbook_2026-10-15.xlsx", "MONTHLY_TFB_Workbook_2026-10.xlsx"]
    wb = next(f for f in drive.files.values() if f["name"].startswith("DAILY_"))
    assert wb["mime"] == wb["announced"] == bw._XLSX_MIME


def test_workflow_publishes_after_the_artifact_and_before_cleanup():
    wf = yaml.safe_load((ROOT / ".github/workflows/workbook_backup.yml").read_text(encoding="utf-8"))
    assert wf["permissions"] == {"contents": "read"}
    steps = wf["jobs"]["backup"]["steps"]
    names = [s.get("name") for s in steps]
    i = names.index("Publish DQ report to Drive")
    assert names[i - 1] == "Upload DQ report" and names[i + 1] == "Remove Google credentials"
    publish, backup = steps[i], steps[names.index("Run backup")]
    assert publish["continue-on-error"] is True
    assert publish["env"] == backup["env"]
    assert publish["run"].strip().splitlines()[-1] == (
        "python scripts/backup_workbook.py --attach dq_scorecard.md dq_report.json dq_activation_receipt.json")
    assert ".xlsx" not in publish["run"]

    def runs(mode="observe", event="schedule", dry_run="", restore_test=""):
        expr = publish["if"].removeprefix("${{").removesuffix("}}")
        for token, value in (("always()", "True"), ("steps.dq_mode.outputs.mode", repr(mode)),
                             ("github.event.inputs.restore_test", repr(restore_test)),
                             ("github.event.inputs.dry_run", repr(dry_run))):
            expr = expr.replace(token, value)
        expr = expr.replace("&&", " and ").replace("||", " or ")
        return eval(expr, {"__builtins__": {}}, {})   # noqa: S307 - literal-only expression

    assert runs() and runs(mode="enforce") and runs(event="workflow_dispatch", dry_run="false")
    assert not runs(dry_run="true")                  # a dry_run dispatch publishes nothing
    assert not runs(restore_test="true")
    assert not runs(mode="off")


def test_two_inputs_with_one_destination_name_keep_the_first(drive, tmp_path, capsys):
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    first, second = tmp_path / "a" / "dq_report.json", tmp_path / "b" / "dq_report.json"
    first.write_text('{"run": 1}')
    second.write_text('{"run": 2}')
    assert bw.main(["--attach", str(first), str(second)]) == 0
    [f] = [f for f in drive.files.values() if not f["folder"]]
    assert f["name"] == "DAILY_TFB_DQ_Report_2026-10-15.json" and f["data"] == b'{"run": 1}'
    assert "duplicates an earlier input — skipped" in capsys.readouterr().err


def test_upload_errors_never_print_the_session_url(drive, tmp_path, monkeypatch, capsys):
    class SessionError(Exception):
        response = Resp(503)

    def fail(*a, **k):
        raise SessionError("503 Server Error: for url: https://www.googleapis.com/upload/drive/v3/files"
                           "?uploadType=resumable&upload_id=SECRET_SESSION")
    monkeypatch.setattr(drive, "put", fail)
    [md] = reports(tmp_path, "dq_scorecard.md")
    assert bw.main(["--attach", md]) == 3
    err = capsys.readouterr().err
    assert "upload_id" not in err and "SECRET_SESSION" not in err
    assert "SessionError HTTP 503" in err


def test_reports_take_the_date_of_the_export_they_describe(drive, tmp_path, monkeypatch):
    export = tmp_path / "tfb_export.xlsx"
    export.write_bytes(b"PK\x03\x04")
    stamp = dt.datetime(2026, 10, 14, 23, 50, tzinfo=bw._RIYADH).timestamp()   # before Riyadh midnight
    os.utime(export, (stamp, stamp))
    monkeypatch.setenv("TFB_BACKUP_LOCAL_COPY", str(export))
    [md] = reports(tmp_path, "dq_scorecard.md")
    assert bw.main(["--attach", md]) == 0                 # the clock says 2026-10-15
    assert drive.in_folder() == ["DAILY_TFB_DQ_Scorecard_2026-10-14.md"]

