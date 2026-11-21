"""validate_dashboard v1.3.5 [TRANSIENT-READ]: a passing Sheets fault is waited out, not scored as a failed audit.

Sync run 44801530434 (20 Nov 2026, 22:22 Riyadh) wrote all 6,602 Global_Markets rows. The validator's literal read
then got one "HttpError 500 ... Internal error encountered", fell back to the logical rows, and failed
scope.coverage with "UNKNOWN literal-sheet extent" (exit 2). Since v1.3.5 both literal reads (every page and the
decision surface) wait out a quota answer (429: 65 s, 65 s) or a passing server fault (500/502/503/504: 15 s, then
30 s), as scripts/sheets_quota.py reads them, at most twice. Any other error, or the last try, goes on as in
v1.3.4. The tests use fakes only: no network, and no real wait.
"""
from __future__ import annotations

import asyncio
import re

import pytest

from scripts import sheets_quota as sq
from scripts import validate_dashboard as vd


class _Resp:
    def __init__(self, status):
        self.status = status


class HttpError(Exception):
    """The shape of googleapiclient's HttpError: the status on e.resp.status."""

    def __init__(self, status):
        super().__init__("<HttpError %d when requesting https://sheets.googleapis.com/v4/spreadsheets/x/values/"
                         "Global_Markets returned \"Internal error encountered.\">" % status)
        self.resp = _Resp(status)


class Registry:
    version = "test"

    def normalize(self, page):
        return page

    def headers(self, page):
        return ["Symbol", "Current Price", "Expected ROI 12M"]

    def keys(self, page):
        return ["symbol", "current_price", "expected_roi_12m"]


def _validate(monkeypatch, faults, size=6602):
    """Run the real validate() on Global_Markets; read_range raises `faults` in order, then returns the grid."""
    reg = Registry()
    grid = [reg.headers("Global_Markets")] + [[f"S{i}", 10, 0.1] for i in range(size)]
    calls, waits = [], []
    todo = list(faults)

    def read_range(sid, area):
        calls.append(area)
        if todo:
            raise todo.pop(0)
        end = int(re.search(r"DZ(\d+)$", area).group(1))
        return grid[:end]

    async def no_logical_reader():
        return None

    async def fake_sleep(s):
        waits.append(s)

    monkeypatch.setattr(vd, "_load_registry", lambda: reg)
    monkeypatch.setattr(vd, "_engine_version", lambda: "test")
    monkeypatch.setattr(vd, "_resolve_read_range", lambda: read_range)
    monkeypatch.setattr(vd, "_resolve_get_sheet_rows", no_logical_reader)
    monkeypatch.setattr(vd, "_SLEEP", fake_sleep)
    monkeypatch.delenv("VALIDATE_MAX_ROWS", raising=False)
    cap = vd.create_parser().parse_args([]).max_rows
    results, _ = asyncio.run(vd.validate(pages=["Global_Markets"], sid="test", max_rows=cap))
    return {r.name: r for r in results}, calls, waits, results


def test_t1_one_server_fault_is_waited_out_and_the_audit_passes(monkeypatch):
    checks, calls, waits, results = _validate(monkeypatch, [HttpError(500)])
    assert checks["scope.coverage"].status == "PASS" and checks["scope.coverage"].count == 6602
    assert waits == [15.0] and len(calls) == 2 and calls[0] == calls[1]        # the same range, read again
    assert vd._exit_code(results) == 0


@pytest.mark.parametrize("status,want", [(502, [15.0]), (503, [15.0]), (504, [15.0]), (429, [65.0])])
def test_t2_each_transient_answer_is_waited_out(monkeypatch, status, want):
    checks, calls, waits, _ = _validate(monkeypatch, [HttpError(status)])
    assert checks["scope.coverage"].status == "PASS" and waits == want and len(calls) == 2


def test_t3_two_faults_wait_15_then_30_and_two_quota_answers_65_twice(monkeypatch):
    checks, calls, waits, _ = _validate(monkeypatch, [HttpError(500), HttpError(503)])
    assert checks["scope.coverage"].status == "PASS" and waits == [15.0, 30.0] and len(calls) == 3
    checks, calls, waits, _ = _validate(monkeypatch, [HttpError(429), HttpError(429)])
    assert checks["scope.coverage"].status == "PASS" and waits == [65.0, 65.0] and len(calls) == 3


def test_t4_a_fault_that_lasts_still_fails_as_in_v134(monkeypatch):
    checks, calls, waits, results = _validate(monkeypatch, [HttpError(500)] * 3)
    assert waits == [15.0, 30.0] and len(calls) == 3                          # at most READ_RETRIES more tries
    assert vd.READ_RETRIES == 2 and vd._exit_code(results) == 2               # never certified on a fault that lasts


@pytest.mark.parametrize("exc", [HttpError(400), HttpError(403), HttpError(404), ValueError("bad range"),
                                 KeyError("x")])
def test_t5_any_other_error_is_not_retried(monkeypatch, exc):
    checks, calls, waits, results = _validate(monkeypatch, [exc])
    assert waits == [] and len(calls) == 1 and vd._exit_code(results) == 2


def test_t6_the_decision_surface_read_waits_too(monkeypatch):
    board = [["", "Last run 2026-11-21 08:10:00 | status: OK"],
             ["Rank", "Symbol", "Name", "Price", "Ticket SAR", "Shares"],
             [1, "AAA.US", "Aaa Inc", 10.5, "", ""]]
    calls, waits = [], []
    todo = [HttpError(503)]

    def read_range(sid, rng):
        calls.append(rng)
        if todo:
            raise todo.pop(0)
        return board

    async def fake_sleep(s):
        waits.append(s)

    monkeypatch.setattr(vd, "_SLEEP", fake_sleep)
    monkeypatch.setenv("VALIDATE_T10_INPROGRESS_WAIT_S", "0")
    res = {r.name: r for r in asyncio.run(vd._run_decision_surface("Top_10_Investments", "sid", read_range))}
    assert "decision.read" not in res and res["decision.header_found"].status == "PASS"
    assert waits == [15.0] and len(calls) == 2
    # a fault that lasts is still decision.read FAIL, with the error kept
    todo[:] = [HttpError(503)] * 3
    calls.clear(); waits.clear()
    res = {r.name: r for r in asyncio.run(vd._run_decision_surface("Top_10_Investments", "sid", read_range))}
    assert res["decision.read"].status == "FAIL" and "HttpError 503" in res["decision.read"].detail
    assert waits == [15.0, 30.0] and len(calls) == 3


def test_t7_the_waits_are_the_shared_helpers_and_none_without_it(monkeypatch):
    assert vd._transient_wait(HttpError(500), 1) == sq.SERVER_WAITS[0] == 15.0
    assert vd._transient_wait(HttpError(500), 2) == sq.SERVER_WAITS[1] == 30.0
    assert vd._transient_wait(HttpError(429), 1) == sq.WAITS[0]
    assert vd._transient_wait(HttpError(400), 1) is None and vd._transient_wait(ValueError("x"), 1) is None
    import builtins
    real = builtins.__import__

    def no_helper(name, *a, **kw):
        if name == "scripts.sheets_quota":
            raise ImportError("not here")
        return real(name, *a, **kw)
    monkeypatch.setattr(builtins, "__import__", no_helper)
    assert vd._transient_wait(HttpError(500), 1) is None                      # v1.3.4 behaviour, never a crash
