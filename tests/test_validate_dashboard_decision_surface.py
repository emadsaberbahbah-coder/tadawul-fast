"""tests/test_validate_dashboard_decision_surface.py - validate_dashboard v1.3.1.

WHY (2026-10-21)
----------------
Sync run 39513589733 failed all three legs on "cockpit data-grid header
(Rank|Symbol) not found in first 16 rows": the Top_10 cockpit had written
"status: running…" and not yet drawn its board, and the FAIL did not say so.
v1.3.1 re-reads while the cockpit reports a render in progress and quotes the
cockpit status line in a header FAIL. Checks and statuses are unchanged.

Cases (read_range is a scripted fake; asyncio.sleep is patched to no-op):
  V1 running, then the board appears  -> header PASS after 1 re-read
  V2 running throughout              -> FAIL after the configured re-reads,
     detail quotes the status line and the re-read count
  V3 no grid and a non-running status (e.g. FAILED) -> FAIL at once, no
     re-read, detail quotes the status line
  V4 VALIDATE_T10_INPROGRESS_READS=0 -> one read only (v1.3.0)
  V5 a normal published board -> one read; banner/sizing checks unchanged
  V6 version 1.3.1

Run:  python -m pytest -q tests/test_validate_dashboard_decision_surface.py
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import validate_dashboard as vd  # noqa: E402

PAGE = "Top_10_Investments"
RUNNING = [["TOP 10 INVESTMENTS"], ["", "Last run 2026-10-20 19:20:00 | status: running…"]]
FAILED = [["TOP 10 INVESTMENTS"],
          ["", "Last run 2026-10-21 08:10:00 | status: FAILED | failed_at_utc=2026-10-21T05:10:02Z | timeout"]]
BOARD = [["TOP 10 INVESTMENTS"],
         ["", "Last run 2026-10-21 08:10:00 | status: OK"],
         ["FEED: NOT ACTIONABLE (source) | run=39513589733"],
         ["Rank", "Symbol", "Name", "Price", "Ticket SAR", "Shares"],
         [1, "AAA.US", "Aaa Inc", 10.5, "", ""],
         [2, "BBB.US", "Bbb Inc", 20.0, "", ""]]


def _reader(script):
    calls = []

    def read_range(sid, rng):
        calls.append(rng)
        return script[min(len(calls) - 1, len(script) - 1)]
    return read_range, calls


def _run(script, monkeypatch, reads=None):
    monkeypatch.delenv("VALIDATE_T10_INPROGRESS_READS", raising=False)
    monkeypatch.setenv("VALIDATE_T10_INPROGRESS_WAIT_S", "0")
    if reads is not None:
        monkeypatch.setenv("VALIDATE_T10_INPROGRESS_READS", str(reads))
    slept = []

    async def fake_sleep(s):
        slept.append(s)
    monkeypatch.setattr(vd.asyncio, "sleep", fake_sleep)
    read_range, calls = _reader(script)
    res = asyncio.run(vd._run_decision_surface(PAGE, "sid", read_range))
    return {r.name: r for r in res}, calls, slept


def test_v1_running_then_board(monkeypatch):
    res, calls, slept = _run([RUNNING, BOARD], monkeypatch)
    assert res["decision.header_found"].status == "PASS"
    assert len(calls) == 2 and len(slept) == 1
    assert res["decision.feed_banner_present"].status == "PASS"


def test_v2_running_throughout_fails_with_reason(monkeypatch):
    res, calls, slept = _run([RUNNING], monkeypatch)
    r = res["decision.header_found"]
    assert r.status == "FAIL" and len(calls) == 4 and len(slept) == 3
    assert "status: running" in r.detail and "after 3 in-progress re-read(s)" in r.detail
    assert "not found in first 2 rows" in r.detail


def test_v3_failed_status_fails_at_once(monkeypatch):
    res, calls, slept = _run([FAILED, BOARD], monkeypatch)
    r = res["decision.header_found"]
    assert r.status == "FAIL" and len(calls) == 1 and slept == []
    assert "status: FAILED" in r.detail and "re-read" not in r.detail


def test_v4_zero_rereads_is_v130(monkeypatch):
    res, calls, slept = _run([RUNNING, BOARD], monkeypatch, reads=0)
    assert res["decision.header_found"].status == "FAIL" and len(calls) == 1 and slept == []


def test_v5_published_board_single_read(monkeypatch):
    res, calls, slept = _run([BOARD], monkeypatch)
    assert len(calls) == 1 and slept == []
    assert res["decision.header_found"].status == "PASS"
    assert res["decision.sizing_withheld_when_blocked"].status == "PASS"


def test_v6_version():
    assert vd.SCRIPT_VERSION == "1.3.1"
