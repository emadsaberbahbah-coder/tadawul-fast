#!/usr/bin/env python3
"""tests/test_sync_status_stamp_retry.py - run_dashboard_sync v6.70.2 STAMP-RETRY.

WHY (2026-10-21)
----------------
Run 39513589733 (#4998, 20 Oct 23:13 UTC) wrote Global_Markets and then
annotated "[STATUS-STAMP v6.70.0] could not read _Status key column for
Global_Markets - TimeoutError: The read operation timed out". The stamp made
one read and returned, so Global_Markets kept the previous run ID in _Status,
the Top_10 cockpit saw two runs and withheld, and it could not build the pool
receipt that explains the Mutual_Funds pool. v6.70.2 retries the read + write
as one unit on transient errors and re-reads column A on every attempt.

Pinned here:
  S1 a read timeout heals on a later attempt; the row is updated in place
  S2 a read that times out on every attempt gives up after the configured
     attempts with the v6.70.0 '::warning:: ... stamp FAILED' annotation, now
     naming the attempts and the cohort consequence; nothing written
  S3 a non-transient error is not retried (one attempt, no sleep)
  S4 an append whose response was lost is NOT appended twice: the next
     attempt re-reads column A, finds the row and updates it
  S5 TFB_SYNC_STATUS_STAMP_RETRIES=1 restores the v6.70.0 single attempt
  S6 a transient write failure is retried and the payload is unchanged
  S7 attempts are clamped to 1..6; garbage falls back to 4; the 90 s stamp
     budget (TFB_SYNC_STATUS_STAMP_DEADLINE_S, 10..600) stops further retries
  S8 the gate (TFB_SYNC_STATUS_STAMP) still means zero API calls when off

Run: pytest -q tests/test_sync_status_stamp_retry.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from scripts import run_dashboard_sync as sync  # noqa: E402

PAYLOAD = ["Global_Markets", "2026-10-21 02:30:00+03:00", "SUCCESS",
           "[STATUS-STAMP v6.70.2] leg=success run=39513589733", "backend:run_dashboard_sync",
           "", 6613, 122, 1000, 0]


class _Exec:
    def __init__(self, fn):
        self._fn = fn

    def execute(self):
        return self._fn()


class FakeSheets:
    """Minimal googleapiclient double: values().get/update/append."""

    def __init__(self, grid, *, read_errors=(), update_errors=(), append_errors=(),
                 append_lands_before_error=False):
        self.grid = [list(r) for r in grid]
        self.read_errors = list(read_errors)
        self.update_errors = list(update_errors)
        self.append_errors = list(append_errors)
        self.append_lands_before_error = append_lands_before_error
        self.calls = []

    # SheetsWriter surface used by _stamp_page_status
    def _get_service(self):
        return self

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def get(self, **kw):
        def run():
            self.calls.append(("get", kw["range"]))
            if self.read_errors:
                raise self.read_errors.pop(0)
            return {"values": [list(r[:1]) for r in self.grid]}
        return _Exec(run)

    def update(self, **kw):
        def run():
            self.calls.append(("update", kw["range"]))
            if self.update_errors:
                raise self.update_errors.pop(0)
            row = int(kw["range"].split("!A", 1)[1].split(":", 1)[0])
            self.grid[row - 1] = list(kw["body"]["values"][0])
            return {}
        return _Exec(run)

    def append(self, **kw):
        def run():
            self.calls.append(("append", kw["range"]))
            if self.append_errors:
                if self.append_lands_before_error:
                    self.grid.append(list(kw["body"]["values"][0]))
                raise self.append_errors.pop(0)
            self.grid.append(list(kw["body"]["values"][0]))
            return {}
        return _Exec(run)


@pytest.fixture
def armed(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_STATUS_STAMP", "1")
    monkeypatch.setenv("TFB_SYNC_STATUS_STAMP_PAGES", "")
    monkeypatch.delenv("TFB_SYNC_STATUS_STAMP_RETRIES", raising=False)
    monkeypatch.delenv("TFB_SYNC_STATUS_STAMP_DEADLINE_S", raising=False)
    monkeypatch.setattr(sync, "_status_stamp_row", lambda page, res, n: list(PAYLOAD))
    sleeps = []
    monkeypatch.setattr(sync.time, "sleep", lambda s: sleeps.append(s))
    return sleeps


RES = object()  # _status_stamp_should_skip inspects attributes; a bare object stamps
GRID = [["Page"], ["Market_Leaders"], ["Global_Markets"], ["Mutual_Funds"]]


def _timeout():
    return TimeoutError("The read operation timed out")


def test_s1_read_timeout_heals_on_retry(armed, capsys):
    api = FakeSheets(GRID, read_errors=[_timeout(), _timeout()])
    sync._stamp_page_status(api, "sid", "Global_Markets", RES, 122)
    assert [c[0] for c in api.calls] == ["get", "get", "get", "update"]
    assert ("update", "'_Status'!A3:J3") in api.calls
    assert api.grid[2] == PAYLOAD
    assert armed == [3.0, 8.0]
    out = capsys.readouterr().out
    assert out.count("::warning::") == 2 and "stamp FAILED" not in out
    assert "attempt 1/4" in out and "attempt 2/4" in out


def test_s2_exhausted_reports_error_with_consequence(armed, capsys):
    api = FakeSheets(GRID, read_errors=[_timeout() for _ in range(4)])
    sync._stamp_page_status(api, "sid", "Global_Markets", RES, 122)
    assert [c[0] for c in api.calls] == ["get"] * 4
    assert api.grid[2] == ["Global_Markets"]          # untouched
    assert armed == [3.0, 8.0, 15.0]
    out = capsys.readouterr().out
    assert out.count("stamp FAILED") == 1 and "::error::" not in out
    # The tag carries SCRIPT_VERSION: v6.71.0 since 2026-10-29 (FW2-STALE-PE);
    # v6.72.0 on the #778 bundle (FW-2 SCOPE, 2026-10-31), read from the module.
    assert f"::warning::[STATUS-STAMP v{sync.SCRIPT_VERSION}] stamp FAILED for Global_Markets after 4 attempt(s)" in out
    assert "cohort mismatch" in out and "could not read _Status key column" in out


def test_s3_non_transient_is_not_retried(armed, capsys):
    api = FakeSheets(GRID, read_errors=[ValueError("Unable to parse range: '_Status'!A:A")])
    sync._stamp_page_status(api, "sid", "Global_Markets", RES, 122)
    assert [c[0] for c in api.calls] == ["get"]
    assert armed == []
    out = capsys.readouterr().out
    assert "stamp FAILED for Global_Markets after 1 attempt(s)" in out


def test_s4_lost_append_response_is_not_duplicated(armed):
    grid = [["Page"], ["Market_Leaders"]]
    api = FakeSheets(grid, append_errors=[_timeout()], append_lands_before_error=True)
    sync._stamp_page_status(api, "sid", "Global_Markets", RES, 122)
    kinds = [c[0] for c in api.calls]
    assert kinds == ["get", "append", "get", "update"]
    assert sum(1 for r in api.grid if r and r[0] == "Global_Markets") == 1
    assert api.grid[2] == PAYLOAD and armed == [3.0]


def test_s5_single_attempt_rollback(armed, monkeypatch, capsys):
    monkeypatch.setenv("TFB_SYNC_STATUS_STAMP_RETRIES", "1")
    api = FakeSheets(GRID, read_errors=[_timeout()])
    sync._stamp_page_status(api, "sid", "Global_Markets", RES, 122)
    assert [c[0] for c in api.calls] == ["get"] and armed == []
    assert "after 1 attempt(s)" in capsys.readouterr().out


def test_s6_transient_write_failure_retried(armed, capsys):
    api = FakeSheets(GRID, update_errors=[OSError("[SSL] record layer failure")])
    sync._stamp_page_status(api, "sid", "Global_Markets", RES, 122)
    assert [c[0] for c in api.calls] == ["get", "update", "get", "update"]
    assert api.grid[2] == PAYLOAD and armed == [3.0]
    out = capsys.readouterr().out
    assert "could not write the _Status row for Global_Markets (attempt 1/4)" in out
    assert "stamp FAILED" not in out


@pytest.mark.parametrize("raw,want", [("", 4), ("0", 1), ("1", 1), ("3", 3), ("6", 6),
                                      ("9", 6), ("x", 4), ("-2", 1)])
def test_s7_attempts_clamped(monkeypatch, raw, want):
    monkeypatch.setenv("TFB_SYNC_STATUS_STAMP_RETRIES", raw)
    assert sync._status_stamp_attempts() == want


def test_s7b_deadline_stops_retries(armed, monkeypatch, capsys):
    # backoff 3 s, 8 s, 15 s; a 10 s budget allows the first two waits only
    monkeypatch.setenv("TFB_SYNC_STATUS_STAMP_DEADLINE_S", "10")
    api = FakeSheets(GRID, read_errors=[_timeout() for _ in range(4)])
    sync._stamp_page_status(api, "sid", "Global_Markets", RES, 122)
    assert [c[0] for c in api.calls] == ["get"] * 3 and armed == [3.0, 8.0]
    out = capsys.readouterr().out
    assert "after 3 attempt(s) (10 s stamp budget reached)" in out


@pytest.mark.parametrize("raw,want", [("", 90.0), ("5", 10.0), ("45", 45.0), ("9999", 600.0), ("x", 90.0)])
def test_s7c_deadline_clamped(monkeypatch, raw, want):
    monkeypatch.setenv("TFB_SYNC_STATUS_STAMP_DEADLINE_S", raw)
    assert sync._status_stamp_deadline_s() == want


def test_s8_gate_off_means_no_calls(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_STATUS_STAMP", "0")
    api = FakeSheets(GRID)
    sync._stamp_page_status(api, "sid", "Global_Markets", RES, 122)
    assert api.calls == []


def test_version_pin():
    # 6.71.0 (2026-10-29): FW2-STALE-PE (default off); STAMP-RETRY unchanged.
    # merge note (2026-10-31, #778): FW-2 SCOPE is 6.72.0 over it; floor kept.
    # merge note (2026-10-21, #778): FW-2 SCOPE builds on this as 6.71.0, so this is a
    # version floor and the manifest pin is read from the source constant.
    assert tuple(int(x) for x in sync.SCRIPT_VERSION.split(".")) >= (6, 70, 2)
    src = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert f'("run_dashboard_sync", "{sync.SCRIPT_VERSION}", "dashboard sync")' in src
