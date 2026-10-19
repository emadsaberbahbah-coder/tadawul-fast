#!/usr/bin/env python3
"""tests/test_backup_upload_resume.py - backup_workbook v1.5.1: the resumable
upload resumes instead of losing the day.

WHY (2026-10-19)
----------------
Scheduled run #102 exported 24,273,892 bytes and then the single PUT to the
Drive resumable session died with SSLEOFError mid-stream. v1.5.0 made one PUT
and no status query, so the whole backup failed (exit 3) although the session
was still open on Google's side. These tests drive the new
_resumable_put_with_resume() helper with a scripted fake session and pin the
protocol it must follow:

  T1  clean path is byte-identical to v1.5.0 (one PUT, no Content-Range, json)
  T2  transport fault -> sleep -> status probe (bytes */N) -> resend ONLY the
      bytes after the server's Range end, with the matching Content-Range
  T3  status probe answering 200 means the server already finished: return it
  T4  308 to a data PUT is followed without sleeping and without burning an
      attempt
  T5  a definite 4xx raises the same HTTPError on the first answer (no retry)
  T6  attempts are bounded: the last transport fault is re-raised unchanged
  T7  transient 5xx counts as a fault and is retried the same way
  T8  TFB_BACKUP_UPLOAD_RETRIES=1 disables retries (first fault propagates)
  T9  both backends (Drive, GCS) route through the helper
  T10 the probe itself failing keeps the loop alive and the offset known

No network, no credentials, no Google client objects: the fake session only
records calls. Run: pytest -q tests/test_backup_upload_resume.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from requests import exceptions as rx  # noqa: E402

from scripts import backup_workbook as bw  # noqa: E402

DATA = bytes(range(256)) * 400          # 102,400 bytes, non-trivial content
TOTAL = len(DATA)
LOC = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&upload_id=SECRET"


class _Resp:
    def __init__(self, status, headers=None, body=None):
        self.status_code = status
        self.headers = dict(headers or {})
        self._body = body if body is not None else {"id": "f1", "name": "DAILY"}

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            err = rx.HTTPError(f"{self.status_code} Client Error")
            err.response = self
            raise err


class FakeSession:
    """Scripted PUT answers. An entry is either a _Resp or an Exception
    instance (raised instead of returned). Every call is recorded."""

    def __init__(self, script):
        self.script = list(script)
        self.calls = []

    def put(self, url, data=b"", headers=None, timeout=None):
        self.calls.append({"url": url, "data": data,
                           "headers": dict(headers or {}), "timeout": timeout})
        if not self.script:
            raise AssertionError("unexpected PUT beyond the script")
        nxt = self.script.pop(0)
        if isinstance(nxt, BaseException):
            raise nxt
        return nxt

    def post(self, url, **kw):            # only the session-open call uses it
        self.calls.append({"url": url, "post": True, **kw})
        return _Resp(200, {"Location": LOC})


def _no_sleep():
    slept = []
    return slept, (lambda s: slept.append(s))


def test_t1_clean_path_is_single_put_without_content_range():
    sess = FakeSession([_Resp(200)])
    slept, sl = _no_sleep()
    out = bw._resumable_put_with_resume(sess, LOC, DATA, sleep=sl)
    assert out == {"id": "f1", "name": "DAILY"}
    assert len(sess.calls) == 1
    c = sess.calls[0]
    assert c["data"] == DATA and c["data"] is DATA          # no copy on the clean path
    assert c["headers"] == {"Content-Type": bw._XLSX_MIME}
    assert c["timeout"] == bw._UPLOAD_PUT_TIMEOUT
    assert slept == []


def test_t2_transport_fault_resumes_from_server_range():
    persisted_end = 65_535                                   # server holds 0..65535
    sess = FakeSession([
        rx.SSLError("EOF occurred in violation of protocol"),  # data PUT #1
        _Resp(308, {"Range": f"bytes=0-{persisted_end}"}),     # status probe
        _Resp(200),                                            # data PUT #2
    ])
    slept, sl = _no_sleep()
    out = bw._resumable_put_with_resume(sess, LOC, DATA, attempts=4, sleep=sl)
    assert out["id"] == "f1"
    assert slept == [2.0]
    probe = sess.calls[1]
    assert probe["data"] == b""
    assert probe["headers"] == {"Content-Range": f"bytes */{TOTAL}"}
    resend = sess.calls[2]
    assert resend["data"] == DATA[persisted_end + 1:]
    assert resend["headers"]["Content-Range"] == \
        f"bytes {persisted_end + 1}-{TOTAL - 1}/{TOTAL}"
    assert resend["headers"]["Content-Type"] == bw._XLSX_MIME


def test_t2b_probe_without_range_header_restarts_from_zero():
    sess = FakeSession([
        rx.ConnectionError("reset"),
        _Resp(308),                       # 308 but no Range -> nothing persisted
        _Resp(201),
    ])
    _, sl = _no_sleep()
    bw._resumable_put_with_resume(sess, LOC, DATA, attempts=3, sleep=sl)
    resend = sess.calls[2]
    assert resend["data"] == DATA
    assert "Content-Range" not in resend["headers"]


def test_t3_probe_reporting_complete_returns_final_resource():
    final = {"id": "already", "name": "DAILY_done"}
    sess = FakeSession([
        rx.ChunkedEncodingError("cut"),
        _Resp(200, body=final),           # probe: server finished anyway
    ])
    _, sl = _no_sleep()
    out = bw._resumable_put_with_resume(sess, LOC, DATA, attempts=4, sleep=sl)
    assert out == final
    assert len(sess.calls) == 2


def test_t4_308_to_data_put_is_followed_without_sleep_or_attempt():
    sess = FakeSession([
        _Resp(308, {"Range": "bytes=0-9999"}),
        _Resp(308, {"Range": "bytes=0-49999"}),
        _Resp(200),
    ])
    slept, sl = _no_sleep()
    # attempts=1 would fail immediately if a 308 counted as a failed attempt
    out = bw._resumable_put_with_resume(sess, LOC, DATA, attempts=1, sleep=sl)
    assert out["id"] == "f1"
    assert slept == []
    assert sess.calls[1]["data"] == DATA[10_000:]
    assert sess.calls[1]["headers"]["Content-Range"] == f"bytes 10000-{TOTAL - 1}/{TOTAL}"
    assert sess.calls[2]["data"] == DATA[50_000:]


@pytest.mark.parametrize("status", [401, 403, 404, 412])
def test_t5_definite_4xx_raises_first_time_without_retry(status):
    sess = FakeSession([_Resp(status, body={"error": {"message": "nope"}})])
    slept, sl = _no_sleep()
    with pytest.raises(rx.HTTPError) as ei:
        bw._resumable_put_with_resume(sess, LOC, DATA, attempts=4, sleep=sl)
    assert ei.value.response.status_code == status      # _api_reason() still works
    assert len(sess.calls) == 1 and slept == []


def test_t6_attempts_are_bounded_and_last_fault_is_reraised_unchanged():
    faults = [rx.SSLError("a"), rx.SSLError("b"), rx.SSLError("c")]
    sess = FakeSession([
        faults[0], _Resp(308, {"Range": "bytes=0-99"}),
        faults[1], _Resp(308, {"Range": "bytes=0-199"}),
        faults[2],
    ])
    slept, sl = _no_sleep()
    with pytest.raises(rx.SSLError) as ei:
        bw._resumable_put_with_resume(sess, LOC, DATA, attempts=3, sleep=sl)
    assert ei.value is faults[2]
    assert slept == [2.0, 4.0]
    assert sess.calls[4]["headers"]["Content-Range"] == f"bytes 200-{TOTAL - 1}/{TOTAL}"


@pytest.mark.parametrize("status", sorted(bw._UPLOAD_TRANSIENT_STATUS))
def test_t7_transient_status_is_retried_like_a_transport_fault(status):
    sess = FakeSession([
        _Resp(status, body={"error": {"message": "try later"}}),
        _Resp(308, {"Range": "bytes=0-999"}),
        _Resp(200),
    ])
    slept, sl = _no_sleep()
    out = bw._resumable_put_with_resume(sess, LOC, DATA, attempts=2, sleep=sl)
    assert out["id"] == "f1" and slept == [2.0]
    assert sess.calls[2]["data"] == DATA[1000:]


def test_t7b_transient_status_on_last_attempt_raises_runtime_error():
    sess = FakeSession([_Resp(503, body={})])
    _, sl = _no_sleep()
    with pytest.raises(RuntimeError, match="HTTP 503 after 1 attempt"):
        bw._resumable_put_with_resume(sess, LOC, DATA, attempts=1, sleep=sl)


def test_t8_env_retries_one_disables_retry(monkeypatch):
    monkeypatch.setenv("TFB_BACKUP_UPLOAD_RETRIES", "1")
    assert bw._upload_retry_attempts() == 1
    sess = FakeSession([rx.SSLError("first")])
    slept, sl = _no_sleep()
    with pytest.raises(rx.SSLError):
        bw._resumable_put_with_resume(sess, LOC, DATA, sleep=sl)
    assert slept == [] and len(sess.calls) == 1


def test_t8b_env_default_and_garbage_fall_back_to_four(monkeypatch):
    monkeypatch.delenv("TFB_BACKUP_UPLOAD_RETRIES", raising=False)
    assert bw._upload_retry_attempts() == 4
    monkeypatch.setenv("TFB_BACKUP_UPLOAD_RETRIES", "zero")
    assert bw._upload_retry_attempts() == 4
    monkeypatch.setenv("TFB_BACKUP_UPLOAD_RETRIES", "0")
    assert bw._upload_retry_attempts() == 4


def test_t8c_backoff_is_bounded():
    assert [bw._upload_backoff_s(a) for a in (1, 2, 3, 4, 5, 9)] == \
        [2.0, 4.0, 8.0, 16.0, 30.0, 30.0]


def test_t9_drive_and_gcs_backends_route_through_the_helper(monkeypatch):
    seen = []

    def fake_put(sess, loc, data, mime=bw._XLSX_MIME, attempts=None, sleep=None):
        seen.append((loc, len(data)))
        return {"id": "x", "name": "tfb-backups/DAILY_x.xlsx"}

    monkeypatch.setattr(bw, "_resumable_put_with_resume", fake_put)
    d = bw.drive_upload_resumable(FakeSession([]), "folder", "DAILY_x.xlsx", DATA)
    g = bw.gcs_upload_resumable(FakeSession([]), "bucket", "DAILY_x.xlsx", DATA)
    assert d == {"id": "x", "name": "tfb-backups/DAILY_x.xlsx"}
    assert g == {"id": "tfb-backups/DAILY_x.xlsx", "name": "DAILY_x.xlsx"}
    assert seen == [(LOC, TOTAL), (LOC, TOTAL)]


def test_t10_probe_failure_keeps_loop_alive(capsys):
    sess = FakeSession([
        rx.SSLError("data"),                        # attempt 1 fails
        rx.ConnectionError("probe down"),           # probe fails too
        _Resp(200),                                 # attempt 2 succeeds from byte 0
    ])
    slept, sl = _no_sleep()
    out = bw._resumable_put_with_resume(sess, LOC, DATA, attempts=3, sleep=sl)
    assert out["id"] == "f1"
    assert sess.calls[2]["data"] == DATA and "Content-Range" not in sess.calls[2]["headers"]
    err = capsys.readouterr().err
    assert "status probe failed" in err
    assert "upload_id=SECRET" not in err            # the session URL is never printed


def test_parse_range_end_is_total():
    assert bw._parse_range_end("bytes=0-1048575") == 1_048_575
    assert bw._parse_range_end(None) == -1
    assert bw._parse_range_end("garbage") == -1
    assert bw._parse_range_end("bytes=5-") == -1


def test_version_and_manifest_pin():
    assert bw.__version__ == "1.5.1"
    src = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("backup_workbook", "1.5.1", "workbook backup")' in src


if __name__ == "__main__":   # pragma: no cover
    sys.exit(pytest.main([__file__, "-q"]))
