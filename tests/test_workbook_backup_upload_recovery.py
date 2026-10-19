"""Drive resumable recovery and real backup gates; all HTTP/storage is synthetic."""
from __future__ import annotations

import datetime as dt
import hashlib
import ssl
from types import SimpleNamespace

import pytest
import requests

from scripts import backup_workbook as backup
from tests.workbook_backup_fixtures import workbook_export


NAME = "DAILY_TFB_Workbook_2026-10-19.xlsx"
SESSION_URL = "https://www.googleapis.com/upload/drive/v3/files?upload_id=SYNTHETIC_SESSION_SECRET"


def response(status, *, headers=None, payload=None):
    result = requests.Response()
    result.status_code = status
    result.url = SESSION_URL
    result.headers.update(headers or {})
    if isinstance(payload, Exception):
        result.json = lambda: (_ for _ in ()).throw(payload)
    else:
        result.json = lambda: payload
    return result


def acknowledgement(data, **overrides):
    return {"id": "synthetic-new-file", "name": NAME, "size": str(len(data)),
            "md5Checksum": hashlib.md5(data, usedforsecurity=False).hexdigest(), **overrides}


def ssl_eof():
    return requests.exceptions.SSLError(
        "SYNTHETIC_SESSION_SECRET " + str(ssl.SSLEOFError(8, "synthetic EOF")))


class Session:
    """Queue every HTTP boundary, including an interrupted acknowledgement."""

    def __init__(self, outcomes, *, initiation=None):
        self.outcomes = list(outcomes)
        self.initiation = (initiation if initiation is not None
                           else response(200, headers={"Location": SESSION_URL}))
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append(("POST", url, kwargs))
        return self.initiation

    def put(self, url, **kwargs):
        self.calls.append(("PUT", url, kwargs))
        assert self.outcomes, "unexpected extra request (retry must be bounded)"
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


@pytest.fixture(autouse=True)
def no_backoff_wait(monkeypatch):
    sleeps = []
    monkeypatch.setattr(backup.time, "sleep", sleeps.append)
    return sleeps


def upload(sess, data):
    return backup.drive_upload_resumable(sess, "synthetic-folder", NAME, data)


def assert_single_session(sess):
    assert sum(method == "POST" for method, *_ in sess.calls) == 1
    assert all(url == SESSION_URL for method, url, _ in sess.calls if method == "PUT")


def test_tls_eof_queries_status_and_resumes_only_acknowledged_remaining_bytes():
    data = b"synthetic workbook bytes"
    sess = Session([ssl_eof(), response(308, headers={"Range": "bytes=0-8"}),
                    response(200, payload=acknowledgement(data))])

    assert upload(sess, data) == acknowledgement(data)
    assert_single_session(sess)
    first, query, resumed = [kwargs for method, _, kwargs in sess.calls if method == "PUT"]
    assert first["data"] == data
    assert query["data"] == b""
    assert query["headers"]["Content-Range"] == f"bytes */{len(data)}"
    assert query["headers"]["Content-Length"] == "0"
    assert resumed["data"] == data[9:]
    assert resumed["headers"]["Content-Range"] == f"bytes 9-{len(data)-1}/{len(data)}"
    assert resumed["headers"]["Content-Length"] == str(len(data) - 9)
    assert all(not kwargs["allow_redirects"] for method, _, kwargs in sess.calls if method == "PUT")


def test_lost_final_response_accepts_completed_status_without_resending_or_creating():
    data = b"synthetic workbook bytes"
    sess = Session([ssl_eof(), response(201, payload=acknowledgement(data))])

    assert upload(sess, data)["id"] == "synthetic-new-file"
    assert_single_session(sess)
    assert [kwargs["data"] for method, _, kwargs in sess.calls if method == "PUT"] == [data, b""]


@pytest.mark.parametrize("failure", (
    requests.exceptions.ReadTimeout("synthetic timeout"),
    requests.exceptions.ConnectionError("synthetic reset"),
    requests.exceptions.ChunkedEncodingError("synthetic lost response"),
    response(500), response(502), response(503), response(504), response(429),
    response(403, payload={"error": {"errors": [{"reason": "rateLimitExceeded"}]}}),
    response(403, payload={"error": {"errors": [{"reason": "userRateLimitExceeded"}]}}),
))
def test_transient_failures_query_same_session_then_recover(failure):
    data = b"synthetic workbook bytes"
    sess = Session([failure, response(200, payload=acknowledgement(data))])
    assert upload(sess, data)["id"] == "synthetic-new-file"
    assert_single_session(sess)
    assert sess.calls[-1][2]["data"] == b""


def test_status_failure_retries_status_without_replaying_uncertain_data(no_backoff_wait):
    data = b"synthetic workbook bytes"
    sess = Session([ssl_eof(), response(503), ssl_eof(),
                    response(200, payload=acknowledgement(data))])
    assert upload(sess, data)["id"] == "synthetic-new-file"
    assert [kwargs["data"] for method, _, kwargs in sess.calls if method == "PUT"] == [data, b"", b"", b""]
    assert no_backoff_wait == [1, 2, 4]


def test_no_range_means_no_bytes_acknowledged_so_whole_payload_can_resume():
    data = b"synthetic workbook bytes"
    sess = Session([ssl_eof(), response(308), response(200, payload=acknowledgement(data))])
    assert upload(sess, data)["id"] == "synthetic-new-file"
    assert [kwargs["data"] for method, _, kwargs in sess.calls if method == "PUT"] == [data, b"", data]
    assert_single_session(sess)


def test_incomplete_response_uses_server_offset_not_length_sent():
    data = b"synthetic workbook bytes"
    sess = Session([response(308, headers={"Range": "bytes=0-8"}),
                    response(200, payload=acknowledgement(data))])
    assert upload(sess, data)["id"] == "synthetic-new-file"
    assert sess.calls[-1][2]["data"] == data[9:]


def test_all_bytes_received_is_not_success_until_completed_metadata_is_acknowledged():
    data = b"synthetic workbook bytes"
    sess = Session([response(308, headers={"Range": f"bytes=0-{len(data)-1}"}),
                    response(200, payload=acknowledgement(data))])
    assert upload(sess, data)["id"] == "synthetic-new-file"
    assert sess.calls[-1][2]["data"] == b""
    assert sess.calls[-1][2]["headers"]["Content-Range"] == f"bytes */{len(data)}"


@pytest.mark.parametrize("header", ("garbage", "bytes=1-8", "bytes=0-999999", "bytes=0--1"))
def test_malformed_or_out_of_bounds_server_ranges_fail_without_resending(header):
    sess = Session([response(308, headers={"Range": header})])
    with pytest.raises(backup.DriveUploadError, match="range"):
        upload(sess, b"synthetic workbook bytes")
    assert len(sess.calls) == 2


def test_server_progress_cannot_regress_to_replay_already_acknowledged_bytes():
    sess = Session([response(308, headers={"Range": "bytes=0-8"}), response(308)])
    with pytest.raises(backup.DriveUploadError, match="range"):
        upload(sess, b"synthetic workbook bytes")
    assert len(sess.calls) == 3


@pytest.mark.parametrize("status, reason", (
    (400, "badRequest"), (401, "authError"), (403, "insufficientFilePermissions"),
    (403, "storageQuotaExceeded"), (404, "notFound"), (410, "gone"),
))
def test_permanent_http_failure_has_no_retry_or_new_session(status, reason, no_backoff_wait):
    sess = Session([response(status, payload={"error": {"errors": [{"reason": reason}]}})])
    with pytest.raises(backup.DriveUploadError, match=f"HTTP {status}"):
        upload(sess, b"synthetic workbook bytes")
    assert len(sess.calls) == 2
    assert not no_backoff_wait


def test_failed_status_session_is_not_restarted_as_another_backup():
    sess = Session([ssl_eof(), response(404)])
    with pytest.raises(backup.DriveUploadError, match="HTTP 404"):
        upload(sess, b"synthetic workbook bytes")
    assert_single_session(sess)
    assert len(sess.calls) == 3


@pytest.mark.parametrize("initiation", (response(503), response(403), response(200)))
def test_unacknowledged_session_initiation_is_not_blindly_repeated(initiation):
    sess = Session([], initiation=initiation)
    with pytest.raises(backup.DriveUploadError):
        upload(sess, b"synthetic workbook bytes")
    assert len(sess.calls) == 1


def test_upload_explicitly_requests_content_receipt_fields():
    data = b"synthetic workbook bytes"
    sess = Session([response(200, payload=acknowledgement(data))])
    upload(sess, data)
    assert sess.calls[0][2]["params"]["fields"] == "id,name,size,md5Checksum"


@pytest.mark.parametrize("override", (
    {"id": ""}, {"id": None}, {"name": "another-file.xlsx"}, {"size": "1"},
    {"size": None}, {"md5Checksum": "0" * 32}, {"md5Checksum": None},
))
def test_completed_http_status_requires_matching_file_and_content_receipt(override):
    data = b"synthetic workbook bytes"
    sess = Session([response(200, payload=acknowledgement(data, **override))])
    with pytest.raises(backup.DriveUploadError, match="acknowledgement"):
        upload(sess, data)
    assert len(sess.calls) == 2


@pytest.mark.parametrize("payload", (None, [], {}, ValueError("SYNTHETIC_PRIVATE_RESPONSE")))
def test_unreadable_or_empty_completion_receipt_is_not_success(payload):
    sess = Session([response(200, payload=payload)])
    with pytest.raises(backup.DriveUploadError, match="acknowledgement"):
        upload(sess, b"synthetic workbook bytes")


@pytest.mark.parametrize("status", (202, 204, 302, 307))
def test_noncompletion_http_status_is_not_success(status):
    sess = Session([response(status)])
    with pytest.raises(backup.DriveUploadError):
        upload(sess, b"synthetic workbook bytes")


def test_recovery_request_limit_is_finite_even_when_status_never_answers(no_backoff_wait):
    sess = Session([ssl_eof() for _ in range(backup._DRIVE_UPLOAD_MAX_REQUESTS)])
    with pytest.raises(backup.DriveUploadError, match="exhausted"):
        upload(sess, b"synthetic workbook bytes")
    assert_single_session(sess)
    assert len(sess.calls) == 1 + backup._DRIVE_UPLOAD_MAX_REQUESTS
    assert no_backoff_wait == [1, 2, 4, 8, 16, 16, 16]


def test_repeated_no_progress_cannot_loop_without_bound():
    sess = Session([response(308) for _ in range(backup._DRIVE_UPLOAD_MAX_REQUESTS)])
    with pytest.raises(backup.DriveUploadError, match="exhausted"):
        upload(sess, b"synthetic workbook bytes")
    assert len(sess.calls) == 1 + backup._DRIVE_UPLOAD_MAX_REQUESTS


def test_recovery_deadline_stops_requests_after_budget_expires(monkeypatch):
    clock = {"now": 0.0}
    monkeypatch.setattr(backup.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(backup.time, "sleep", lambda _: clock.update(now=601.0))
    sess = Session([ssl_eof()])
    with pytest.raises(backup.DriveUploadError, match="exhausted"):
        upload(sess, b"synthetic workbook bytes")
    assert len(sess.calls) == 2


@pytest.fixture
def run_context(monkeypatch):
    data = workbook_export()
    events, receipts = [], []
    monkeypatch.setenv("DEFAULT_SPREADSHEET_ID", "synthetic-sheet")
    monkeypatch.setenv("TFB_BACKUP_DEST", "oauth")
    monkeypatch.setenv("TFB_BACKUP_MIN_BYTES", "1")
    monkeypatch.setenv("TFB_BACKUP_KEEP_DAILY", "1")
    monkeypatch.delenv("TFB_BACKUP_LOCAL_COPY", raising=False)
    monkeypatch.setattr(backup, "_credentials", lambda: SimpleNamespace(_tfb_client_email="synthetic"))
    monkeypatch.setattr(backup, "_session", lambda _: object())
    monkeypatch.setattr(backup, "export_xlsx", lambda *_: data)
    monkeypatch.setattr(backup, "_now_riyadh", lambda: dt.datetime(2026, 10, 19, tzinfo=backup._RIYADH))
    monkeypatch.setattr(backup, "drive_ensure_app_folder", lambda *_: "synthetic-folder")
    monkeypatch.setattr(backup, "_store_list", lambda *_: events.append("list") or [
        {"id": "expired", "name": "DAILY_TFB_Workbook_2026-10-17.xlsx"},
        {"id": "synthetic-new-file", "name": NAME},
    ])
    monkeypatch.setattr(backup, "_store_copy", lambda *_: events.append("copy"))
    monkeypatch.setattr(backup, "_store_delete", lambda *_: events.append("delete"))
    monkeypatch.setattr(backup, "append_run_log", lambda *args: receipts.append(args))
    return data, events, receipts


def test_recovered_upload_allows_real_backup_retention_only_after_completion(monkeypatch, run_context):
    data, events, receipts = run_context
    sess = Session([ssl_eof(), response(201, payload=acknowledgement(data))])
    monkeypatch.setattr(backup, "_oauth_session", lambda: sess)
    assert backup.run_backup(False) == 0
    assert events == ["list", "copy", "delete"]
    assert receipts[-1][0] == "OK"
    assert receipts[-1][2]["pruned"] == 1
    assert_single_session(sess)


@pytest.mark.parametrize("case", ("exhaustion", "permission", "bad_ack", "expired_session"))
def test_unacknowledged_upload_fails_before_retention_and_never_logs_session_details(
        monkeypatch, run_context, capsys, case):
    data, events, receipts = run_context
    outcomes = {
        "exhaustion": [ssl_eof() for _ in range(backup._DRIVE_UPLOAD_MAX_REQUESTS)],
        "permission": [response(403, payload={"error": {
            "message": "SYNTHETIC_PRIVATE_RESPONSE " + SESSION_URL,
            "errors": [{"reason": "insufficientFilePermissions"}]}})],
        "bad_ack": [response(200, payload=acknowledgement(data, size="1"))],
        "expired_session": [ssl_eof(), response(404)],
    }
    sess = Session(outcomes[case])
    monkeypatch.setattr(backup, "_oauth_session", lambda: sess)
    assert backup.run_backup(False) == 3
    assert not events
    assert receipts[-1][0] == "FAIL"
    assert receipts[-1][2]["stage"] == "upload"
    assert all(row[0] != "OK" for row in receipts)
    diagnostic = capsys.readouterr().err + repr(receipts)
    assert "SYNTHETIC_SESSION_SECRET" not in diagnostic
    assert "SYNTHETIC_PRIVATE_RESPONSE" not in diagnostic
    assert "upload_id=" not in diagnostic
    if case == "exhaustion":
        assert "SSLError" in diagnostic
        assert "EDITOR" not in diagnostic
    assert_single_session(sess)
