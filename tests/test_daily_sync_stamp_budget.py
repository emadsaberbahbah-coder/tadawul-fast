#!/usr/bin/env python3
"""tests/test_daily_sync_stamp_budget.py - daily_sync.yml v1.0.7 STAMP BUDGET.

WHY (2026-10-28)
----------------
Run 40556442682 (27 Oct 2026, 18:03 UTC) wrote all 6,611 Global_Markets rows,
then annotated
  [STATUS-STAMP v6.70.2] could not read _Status key column for Global_Markets
  (attempt 1/4) - TimeoutError: The read operation timed out; retrying in 3s
  [STATUS-STAMP v6.70.2] stamp FAILED for Global_Markets after 2 attempt(s)
  (90 s stamp budget reached)
Each read waited the client's full 60 s socket timeout, so the 90 s retry
budget (run_dashboard_sync v6.70.2 default) left room for two of the four
attempts. Global_Markets kept run 40484148059 in _Status, the other pages
carried 40556442682, and the Top-10 cockpit withheld the board ("source
readiness WITHHELD: Global_Markets source run ... differs from cohort ...").
daily_sync.yml v1.0.7 maps TFB_SYNC_STATUS_STAMP_DEADLINE_S with a default of
300 in both page-writing jobs.

Pinned here:
  B1 every job that maps TFB_SYNC_STATUS_STAMP also maps
     TFB_SYNC_STATUS_STAMP_DEADLINE_S, exactly as
     ${{ vars.TFB_SYNC_STATUS_STAMP_DEADLINE_S || '300' }} (two jobs today)
  B2 the REAL _stamp_page_status, with reads that each take 60 s and time
     out: budget 90 -> 2 attempts and the run-40556442682 annotation; budget
     300 -> all 4 attempts; budget 300 with a read that heals on attempt 3 ->
     the row is written (the stamp the run lost)
  B3 the script accepts 300 (clamp 10..600) and the default stays 90
  B4 yaml(v1.0.7) minus the two added keys == yaml(the untouched v1.0.4 at
     21dc13e9) - env mapping only; and B1 fails on that base (golden
     negative). Both need the base commit locally (git show); SKIP otherwise.

Run: python tests/test_daily_sync_stamp_budget.py   or   pytest -q <this file>
"""
from __future__ import annotations

import copy
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from scripts import run_dashboard_sync as sync  # noqa: E402

WORKFLOW = _REPO / ".github" / "workflows" / "daily_sync.yml"
BASE_SHA = "21dc13e9"  # main before v1.0.7 (daily_sync.yml v1.0.4)
KEY = "TFB_SYNC_STATUS_STAMP_DEADLINE_S"
EXPR = "${{ vars.TFB_SYNC_STATUS_STAMP_DEADLINE_S || '300' }}"
# daily_sync.yml 1.0.8 (2026-10-29) maps one more key in the same two jobs
# (run_dashboard_sync 6.71.0 FW2-STALE-PE; pinned in tests/test_fw2_stale_pe.py).
# B4 strips it too, so it still proves "env mapping only" back to v1.0.4.
LATER_KEYS = ("TFB_SYNC_FW2_STALE_PE",)


def _jobs(doc):
    return doc.get("jobs") or {}


def _env_of(job):
    env = dict(job.get("env") or {})
    for step in job.get("steps") or []:
        env.update(step.get("env") or {})
    return env


def _mapping_problems(doc):
    """B1 on a parsed workflow: [] when every stamp job maps the budget."""
    problems, stamp_jobs = [], 0
    for name, job in _jobs(doc).items():
        for where, env in [("job", job.get("env") or {})] + [
                ("step %r" % (s.get("name") or i), s.get("env") or {})
                for i, s in enumerate(job.get("steps") or [])]:
            if "TFB_SYNC_STATUS_STAMP" not in env:
                continue
            stamp_jobs += 1
            if env.get(KEY) != EXPR:
                problems.append("%s %s: %s=%r" % (name, where, KEY, env.get(KEY)))
    if stamp_jobs == 0:
        problems.append("no job maps TFB_SYNC_STATUS_STAMP")
    return problems, stamp_jobs


def _load(text):
    return yaml.safe_load(text)


# ---------------------------------------------------------------- B1 -------
def test_b1_every_stamp_job_maps_the_budget():
    doc = _load(WORKFLOW.read_text(encoding="utf-8"))
    problems, stamp_jobs = _mapping_problems(doc)
    assert problems == []
    assert stamp_jobs == 2  # sync-dashboard + recover-missing-market-pages


def test_b1_header_names_the_version():
    head = WORKFLOW.read_text(encoding="utf-8").splitlines()[:6]
    assert any(l.startswith("# VERSION 1.0.7 (2026-10-28)") for l in head)


# ---------------------------------------------------------------- B2 -------
class _Exec:
    def __init__(self, fn):
        self._fn = fn

    def execute(self):
        return self._fn()


class SlowSheets:
    """values().get takes `read_s` seconds on the fake clock; the first
    `fail_reads` reads then raise the socket timeout."""

    def __init__(self, clock, grid, fail_reads, read_s=60.0):
        self.clock, self.grid = clock, [list(r) for r in grid]
        self.fail_reads, self.read_s, self.calls = fail_reads, read_s, []

    def _get_service(self):
        return self

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def get(self, **kw):
        def run():
            self.calls.append("get")
            self.clock[0] += self.read_s
            if self.fail_reads > 0:
                self.fail_reads -= 1
                raise TimeoutError("The read operation timed out")
            return {"values": [list(r[:1]) for r in self.grid]}
        return _Exec(run)

    def update(self, **kw):
        def run():
            self.calls.append("update")
            row = int(kw["range"].split("!A", 1)[1].split(":", 1)[0])
            self.grid[row - 1] = list(kw["body"]["values"][0])
            return {}
        return _Exec(run)


PAYLOAD = ["Global_Markets", "2026-10-27 22:09:55+03:00", "SUCCESS",
           "[STATUS-STAMP v6.70.2] leg=success run=40556442682", "backend:run_dashboard_sync",
           "", 6611, 122, 1000, 0]
GRID = [["Page"], ["Market_Leaders"], ["Global_Markets"], ["Mutual_Funds"]]


@pytest.fixture
def clocked(monkeypatch):
    clock = [1000.0]
    monkeypatch.setenv("TFB_SYNC_STATUS_STAMP", "1")
    monkeypatch.setenv("TFB_SYNC_STATUS_STAMP_PAGES", "")
    monkeypatch.delenv("TFB_SYNC_STATUS_STAMP_RETRIES", raising=False)
    monkeypatch.setattr(sync, "_status_stamp_row", lambda page, res, n: list(PAYLOAD))
    monkeypatch.setattr(sync.time, "monotonic", lambda: clock[0])

    def _sleep(s):
        clock[0] += s
    monkeypatch.setattr(sync.time, "sleep", _sleep)
    return clock


def test_b2_budget_90_reproduces_the_lost_stamp(clocked, monkeypatch, capsys):
    monkeypatch.setenv(KEY, "90")
    api = SlowSheets(clocked, GRID, fail_reads=4)
    sync._stamp_page_status(api, "sid", "Global_Markets", object(), 122)
    assert api.calls == ["get", "get"]
    assert api.grid[2] == ["Global_Markets"]
    out = capsys.readouterr().out
    assert "stamp FAILED for Global_Markets after 2 attempt(s) (90 s stamp budget reached)" in out


def test_b2_budget_300_runs_all_attempts(clocked, monkeypatch, capsys):
    monkeypatch.setenv(KEY, "300")
    api = SlowSheets(clocked, GRID, fail_reads=4)
    sync._stamp_page_status(api, "sid", "Global_Markets", object(), 122)
    assert api.calls == ["get"] * 4
    assert clocked[0] - 1000.0 == pytest.approx(4 * 60 + 3 + 8 + 15)  # 266 s
    out = capsys.readouterr().out
    assert "stamp FAILED for Global_Markets after 4 attempt(s)" in out
    assert "stamp budget reached" not in out


def test_b2_budget_300_writes_the_stamp_the_run_lost(clocked, monkeypatch, capsys):
    monkeypatch.setenv(KEY, "300")
    api = SlowSheets(clocked, GRID, fail_reads=2)
    sync._stamp_page_status(api, "sid", "Global_Markets", object(), 122)
    assert api.calls == ["get", "get", "get", "update"]
    assert api.grid[2] == PAYLOAD
    assert "stamp FAILED" not in capsys.readouterr().out


# ---------------------------------------------------------------- B3 -------
@pytest.mark.parametrize("raw,want", [("300", 300.0), ("", 90.0), ("90", 90.0)])
def test_b3_script_accepts_the_budget(monkeypatch, raw, want):
    monkeypatch.setenv(KEY, raw)
    assert sync._status_stamp_deadline_s() == want


# ---------------------------------------------------------------- B4 -------
def _base_text():
    try:
        return subprocess.run(
            ["git", "-C", str(_REPO), "show", BASE_SHA + ":.github/workflows/daily_sync.yml"],
            check=True, capture_output=True, text=True).stdout
    except Exception:
        return None


def _strip_added(doc):
    out = copy.deepcopy(doc)
    for job in _jobs(out).values():
        for holder in [job] + list(job.get("steps") or []):
            env = holder.get("env")
            if isinstance(env, dict):
                env.pop(KEY, None)
                for later in LATER_KEYS:
                    env.pop(later, None)
    return out


def test_b4_env_mapping_only_against_base():
    base = _base_text()
    if base is None:
        pytest.skip("base %s not available (shallow checkout)" % BASE_SHA)
    assert _strip_added(_load(WORKFLOW.read_text(encoding="utf-8"))) == _load(base)


def test_b4_golden_negative_base_lacks_the_mapping():
    base = _base_text()
    if base is None:
        pytest.skip("base %s not available (shallow checkout)" % BASE_SHA)
    problems, stamp_jobs = _mapping_problems(_load(base))
    assert stamp_jobs == 2 and len(problems) == 2


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
