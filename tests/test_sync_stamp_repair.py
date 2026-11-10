#!/usr/bin/env python3
"""tests/test_sync_stamp_repair.py - run_dashboard_sync v6.72.0 STAMP-REPAIR.

WHY (2026-11-10)
----------------
Run 42621566901 (9 Nov, 15:59 Riyadh) wrote all 6,601 Global_Markets rows,
then logged "[STATUS-STAMP v6.71.0] stamp FAILED for Global_Markets after 4
attempt(s) (could not read _Status key column - TimeoutError: The read
operation timed out)" between 17:01 and 17:05 Riyadh. _Status kept the morning
run 42556077749, the Top_10 cockpit withheld every render that evening
("Global_Markets source run 42556077749 differs from cohort 42621566901"),
wrote no receipt, and the final publication audit failed. Reads of the same
workbook worked again from about 17:11. v6.72.0 queues the payload of a stamp
that failed on a transient error and writes it once more at the end of the run.

Pinned here (the Google API double is the one tests/test_sync_status_stamp_retry
uses; the functions under test are the real module functions):
  R1 gate off: an exhausted stamp queues nothing; the end-of-run pass makes no
     call and no sleep (v6.71.0 behaviour)
  R2 gate on: the exhausted stamp is queued with the exact payload it built and
     a ::notice:: says so; the "stamp FAILED" warning text is unchanged
  R3 the repair pass writes that same payload to the page's row after the
     waits 20 s then 40 s, and reports "stamp written"
  R4 a non-transient stamp error is not queued
  R5 the repair budget ends the pass with "still NOT written" and nothing written
  R6 a non-transient error inside the repair stops that page at once
  R7 the queue is emptied by the pass (a second pass does nothing), and one page
     keeps one entry (latest payload wins)
  R8 the budget is clamped 60..900, garbage falls back to 480
  R9 the end-of-run call sits after the escalation pass and before the
     upstream verdict in main_async
  R10 version pins

Run: pytest -q tests/test_sync_stamp_repair.py
"""
from __future__ import annotations

import inspect
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from scripts import run_dashboard_sync as sync  # noqa: E402
from tests.test_sync_status_stamp_retry import FakeSheets  # noqa: E402

PAYLOAD = ["Global_Markets", "2026-11-09 17:01:00+03:00", "SUCCESS",
           "[STATUS-STAMP v6.72.0] leg=success run=42621566901", "backend:run_dashboard_sync",
           "", 6601, 115, 3885711, 0]
GRID = [["Page"], ["Market_Leaders"], ["Global_Markets"], ["Mutual_Funds"]]
RES = object()


def _timeout():
    return TimeoutError("The read operation timed out")


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_STATUS_STAMP", "1")
    monkeypatch.setenv("TFB_SYNC_STATUS_STAMP_PAGES", "")
    monkeypatch.delenv("TFB_SYNC_STATUS_STAMP_RETRIES", raising=False)
    monkeypatch.delenv("TFB_SYNC_STATUS_STAMP_DEADLINE_S", raising=False)
    monkeypatch.delenv("TFB_SYNC_STAMP_REPAIR", raising=False)
    monkeypatch.delenv("TFB_SYNC_STAMP_REPAIR_BUDGET_S", raising=False)
    monkeypatch.setattr(sync, "_status_stamp_row", lambda page, res, n: list(PAYLOAD))
    clock = {"t": 1000.0, "sleeps": []}

    def _sleep(s):
        clock["sleeps"].append(s)
        clock["t"] += s

    monkeypatch.setattr(sync.time, "sleep", _sleep)
    monkeypatch.setattr(sync.time, "monotonic", lambda: clock["t"])
    del sync._STAMP_REPAIR_QUEUE[:]
    yield clock
    del sync._STAMP_REPAIR_QUEUE[:]


def _exhaust(api):
    """The in-place stamp gives up after its 4 attempts (all read timeouts)."""
    sync._stamp_page_status(api, "sid", "Global_Markets", RES, 115)


def test_r1_gate_off_queues_nothing_and_repair_is_inert(env, capsys):
    api = FakeSheets(GRID, read_errors=[_timeout() for _ in range(4)])
    _exhaust(api)
    assert sync._STAMP_REPAIR_QUEUE == []
    calls_before, sleeps_before = list(api.calls), list(env["sleeps"])
    assert sync._repair_failed_stamps(api, "sid") == {}
    assert api.calls == calls_before and env["sleeps"] == sleeps_before
    out = capsys.readouterr().out
    assert "STAMP-REPAIR" not in out and out.count("stamp FAILED") == 1


def test_r2_gate_on_queues_the_exact_payload(env, monkeypatch, capsys):
    monkeypatch.setenv("TFB_SYNC_STAMP_REPAIR", "1")
    api = FakeSheets(GRID, read_errors=[_timeout() for _ in range(4)])
    _exhaust(api)
    assert sync._STAMP_REPAIR_QUEUE == [("Global_Markets", PAYLOAD)]
    out = capsys.readouterr().out
    assert ("::warning::[STATUS-STAMP v6.72.0] stamp FAILED for Global_Markets after 4 attempt(s)"
            in out)
    assert "::notice::[STAMP-REPAIR v6.72.0] Global_Markets stamp queued for the end-of-run repair" in out


def test_r3_repair_writes_the_same_payload_after_waits(env, monkeypatch, capsys):
    monkeypatch.setenv("TFB_SYNC_STAMP_REPAIR", "1")
    api = FakeSheets(GRID, read_errors=[_timeout() for _ in range(4)])
    _exhaust(api)
    env["sleeps"].clear()
    api.calls.clear()
    api.read_errors = [_timeout()]                    # first repair read still times out
    out_map = sync._repair_failed_stamps(api, "sid")
    assert [c[0] for c in api.calls] == ["get", "get", "update"]
    assert ("update", "'_Status'!A3:J3") in api.calls
    assert api.grid[2] == PAYLOAD
    assert env["sleeps"] == [20.0, 40.0]
    assert out_map["Global_Markets"]["repaired"] is True
    assert out_map["Global_Markets"]["attempts"] == 2
    assert out_map["Global_Markets"]["where"] == "row 3"
    out = capsys.readouterr().out
    assert ("::notice::[STAMP-REPAIR v6.72.0] Global_Markets _Status stamp written (row 3) "
            "after 2 repair attempt(s)") in out
    assert "still NOT written" not in out
    assert sync._STAMP_REPAIR_QUEUE == []


def test_r4_non_transient_stamp_error_is_not_queued(env, monkeypatch):
    monkeypatch.setenv("TFB_SYNC_STAMP_REPAIR", "1")
    api = FakeSheets(GRID, read_errors=[ValueError("Unable to parse range: '_Status'!A:A")])
    _exhaust(api)
    assert sync._STAMP_REPAIR_QUEUE == []


def test_r5_budget_ends_the_pass_with_nothing_written(env, monkeypatch, capsys):
    monkeypatch.setenv("TFB_SYNC_STAMP_REPAIR", "1")
    monkeypatch.setenv("TFB_SYNC_STAMP_REPAIR_BUDGET_S", "60")   # waits 20 + 40 fit, 60 more does not
    api = FakeSheets(GRID, read_errors=[_timeout() for _ in range(4)])
    _exhaust(api)
    env["sleeps"].clear()
    api.calls.clear()
    api.read_errors = [_timeout() for _ in range(9)]
    out_map = sync._repair_failed_stamps(api, "sid")
    assert env["sleeps"] == [20.0, 40.0]
    assert [c[0] for c in api.calls] == ["get", "get"]
    assert api.grid[2] == ["Global_Markets"]           # untouched
    assert out_map["Global_Markets"]["repaired"] is False
    out = capsys.readouterr().out
    assert ("::warning::[STAMP-REPAIR v6.72.0] Global_Markets _Status stamp still NOT written "
            "after 2 repair attempt(s)") in out
    assert "cohort mismatch" in out


def test_r6_non_transient_repair_error_stops_at_once(env, monkeypatch):
    monkeypatch.setenv("TFB_SYNC_STAMP_REPAIR", "1")
    sync._stamp_repair_enqueue("Global_Markets", PAYLOAD)
    api = FakeSheets(GRID, read_errors=[PermissionError("The caller does not have permission")])
    out_map = sync._repair_failed_stamps(api, "sid")
    assert [c[0] for c in api.calls] == ["get"]
    assert env["sleeps"] == [20.0]
    assert out_map["Global_Markets"]["repaired"] is False
    assert "PermissionError" in out_map["Global_Markets"]["last_error"]


def test_r7_queue_is_emptied_and_one_entry_per_page(env, monkeypatch):
    monkeypatch.setenv("TFB_SYNC_STAMP_REPAIR", "1")
    first = list(PAYLOAD)
    later = list(PAYLOAD)
    later[2] = "PARTIAL"
    assert sync._stamp_repair_enqueue("Global_Markets", first) is True
    assert sync._stamp_repair_enqueue("Global_Markets", later) is True
    assert sync._STAMP_REPAIR_QUEUE == [("Global_Markets", later)]
    api = FakeSheets(GRID)
    sync._repair_failed_stamps(api, "sid")
    assert api.grid[2] == later and sync._STAMP_REPAIR_QUEUE == []
    api.calls.clear()
    assert sync._repair_failed_stamps(api, "sid") == {} and api.calls == []


def test_r7b_enqueue_refuses_while_off(env):
    assert sync._stamp_repair_enqueue("Global_Markets", PAYLOAD) is False
    assert sync._STAMP_REPAIR_QUEUE == []


@pytest.mark.parametrize("raw,want", [("", 480.0), ("30", 60.0), ("300", 300.0),
                                      ("5000", 900.0), ("x", 480.0), ("nan", 480.0)])
def test_r8_budget_clamped(monkeypatch, raw, want):
    monkeypatch.setenv("TFB_SYNC_STAMP_REPAIR_BUDGET_S", raw)
    assert sync._stamp_repair_budget_s() == want


@pytest.mark.parametrize("raw,want", [("", False), ("0", False), ("off", False), ("1", True),
                                      ("true", True), ("ON", True), ("yes", True)])
def test_r8b_gate_values(monkeypatch, raw, want):
    monkeypatch.setenv("TFB_SYNC_STAMP_REPAIR", raw)
    assert sync._stamp_repair_enabled() is want


def test_r9_end_of_run_call_order():
    src = inspect.getsource(sync.main_async)
    i_esc = src.index("_apply_stale_skip_escalation(results, sheets, spreadsheet_id)")
    i_rep = src.index("_repair_failed_stamps(sheets, spreadsheet_id)")
    i_up = src.index("_write_upstream_verdict(sheets, spreadsheet_id, results)")
    assert i_esc < i_rep < i_up


def test_r10_version_pins():
    assert sync.SCRIPT_VERSION == "6.72.0"
    src = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("run_dashboard_sync", "6.72.0", "dashboard sync")' in src
    head = (_REPO / "scripts" / "run_dashboard_sync.py").read_text(encoding="utf-8")[:4000]
    assert "v6.72.0 - a _Status stamp that times out is written again at the end of the" in head
    assert "v6.71.0 - FW-2 keeps a witnessed row whose P/E is only stale (2026-10-29)" in head
