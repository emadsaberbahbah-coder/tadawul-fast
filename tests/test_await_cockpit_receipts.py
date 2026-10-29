#!/usr/bin/env python3
"""tests/test_await_cockpit_receipts.py - await_cockpit_receipts v1.0.0 and
release_after_sync.yml v1.0.0: the audited release is dispatched as soon as
both cockpits consumed a completed production sync (TFB_RELEASE_AFTER_SYNC,
default off).

WHY (2026-10-29)
----------------
release_decision_feed.yml promotes only on fixed slots or by hand. On 28 Oct
GitHub ran the slots 1-6 h late; sync 40744567865 completed 19:31Z and was
released by hand at 21:07Z. The new waiter decides only WHEN: it reads (never
writes) the three ranges the strict audit reads, and dispatches the unchanged
release workflow, which keeps every gate, audit, probe and compare-and-set.

  W1  gate off: exit 0 before ANY I/O (no provenance, probe, read, dispatch)
  W2  identity and bounds refusals: exit 3, zero I/O
  W3  provenance with the release script's own rule: not releasable -> stop,
      no read, no dispatch; transport faults retry, the third fails (exit 3)
  W4  ready on the first poll: ONE probe (writers + release runs) and ONE
      dispatch for SOURCE; the ranges are the strict audit's own
  W5  receipts landing over several polls: GitHub is not called until the
      ready poll; first-seen seconds recorded per cockpit
  W6  every feed-evidence stop ends the wait without a probe or a dispatch
  W7  receipts the strict audit would reject never dispatch: timeout, bounded
      poll count, exit 0
  W8  read / layout / probe faults: one is retried, three in a row exit 3
  W9  dispatch faults: a 4xx fails at once; a 5xx or transport fault is never
      blindly re-POSTed: a later ready poll re-probes first
  W10 github_dispatch request: POST, exact path and body, https only, token
      required, 200/204 only; github_release_active listing rules
  W11 the evidence and the printed line carry no status-line text
  W12 receipt parsing agrees with the REAL strict audit (audit_surfaces)
  W13 CLI: JSON written for gate-off and refusals; parse errors exit 3
  W14 workflow contract: workflow_run of the Advanced Sync only, JOB-level
      private concurrency (a skipped run never enters the group), least
      privilege, inputs via env, steps copied from the release workflow
  W15 the dispatch matches the release workflow's dispatch contract, and the
      manifest pins this script
  W16 the job condition, evaluated over branch x event x conclusion x gate word
  W17 a busy production writer or an active release run (a pending kill, a
      running slot) never ends the attempt and never gets a dispatch queued
      behind it; probes back off; the GitHub call budget is bounded

Runs standalone (python tests/test_await_cockpit_receipts.py -> PASS k/k)
and under pytest. Offline: fakes only, no network, no credentials.
"""
from __future__ import annotations

import ast
import copy
import inspect
import io
import json
import re
import sys
import tempfile
import traceback
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import audit_decision_surface_freshness as freshness  # noqa: E402
from scripts import await_cockpit_receipts as W  # noqa: E402
from scripts import finalize_decision_publication as final  # noqa: E402

SOURCE = "40744567865"
NEWER = "40800000001"
OLDER = "40700000001"
AFTER_YML = ROOT / ".github" / "workflows" / "release_after_sync.yml"
RELEASE_YML = ROOT / ".github" / "workflows" / "release_decision_feed.yml"
DAILY_YML = ROOT / ".github" / "workflows" / "daily_sync.yml"
VERIFIER = ROOT / "scripts" / "verify_deployment.py"
FINAL_JOB = chr(0x1F6E1) + chr(0xFE0F) + " Verify final decision publication"
NOW = datetime(2026, 10, 28, 19, 40, 0, tzinfo=timezone.utc)
PF_TAIL = "Last run 2026-10-28 22:40:00 | status: ok | holdings 10 | cash 1234.56 SAR"
T10_TAIL = ("Last run 2026-10-28 22:45:00 | status: ok | sheets pool 9694 rows "
            "[Market_Leaders 255/255, Global_Markets 6512/6512, Commodities_FX 453/453, "
            "Mutual_Funds 2474/2474] (full universe) | held=10 sent")
BUSY_WRITER = [{"workflow": "page_refresh_recovery.yml", "id": 41, "event": "workflow_dispatch",
                "status": "in_progress"}]
BUSY_RELEASE = [{"id": 42, "event": "workflow_dispatch", "status": "pending"}]
_ENV = ("TFB_RELEASE_AFTER_SYNC", "TFB_RELEASE_AFTER_SYNC_WAIT_MIN", "GITHUB_TOKEN",
        "GITHUB_REPOSITORY", "GITHUB_API_URL", "GITHUB_RUN_ID", "DEFAULT_SPREADSHEET_ID")


# --------------------------------------------------------------------------- fixtures
def gm(run=SOURCE):
    return "PARTIAL_FRESH | cov=99.6 | run=" + run + " | 2026-10-28 22:28:44+03:00"


def veto(run=SOURCE):
    return ("NOT_ACTIONABLE(final_publication) | run=" + run +
            " | 2026-10-28 22:30:58+03:00 | decision_failed")


def live(run=SOURCE):
    return ("EXECUTABLE | run=" + run + " | 2026-10-28 22:28:44+03:00"
            " | issued=2026-10-28T22:50:00+03:00 | expires=2026-10-29T06:28:44+03:00"
            " | ttl_min=480 | release=777001 | attempt=1 | receipts=T10:" + run + ",PF:" + run +
            " | t10_at=2026-10-28T22:45:00+03:00 | pf_at=2026-10-28T22:40:00+03:00"
            " | mp_at=2026-10-28T22:28:44+03:00 | audits=0,0")


def key_grid(*, gm_value="default", feed="default", extra=()):
    rows = [["Backend URL", "https://synthetic.invalid"], ["Last Global Update", "stamp"],
            ["TFB Feed Market_Leaders", "OK | run=" + SOURCE + " | 2026-10-28 21:32:39+03:00"]]
    gm_value = gm() if gm_value == "default" else gm_value
    feed = veto() if feed == "default" else feed
    if gm_value is not None:
        rows.append(["TFB Feed Global_Markets", gm_value])
    if feed is not None:
        rows.append([final.KEY, feed])
    rows.extend(copy.deepcopy(list(extra)))
    return rows


def surface(title, tail, receipt):
    return [[title], ["Status:", tail + receipt]]


def verified(run=SOURCE):
    return " | source readiness VERIFIED (run " + run + ")"


def pf(receipt=None):
    return surface("MY PORTFOLIO - DECISION", PF_TAIL, verified() if receipt is None else receipt)


def t10(receipt=None):
    return surface("TOP 10 INVESTMENTS - DECISION", T10_TAIL, verified() if receipt is None else receipt)


def state(*, key=None, pf_grid=None, t10_grid=None):
    return (key_grid() if key is None else key, pf() if pf_grid is None else pf_grid,
            t10() if t10_grid is None else t10_grid)


def provenance_ok(source=SOURCE, **run_changes):
    run = {"id": int(source), "path": ".github/workflows/daily_sync.yml", "head_branch": "main",
           "event": "schedule", "status": "completed", "conclusion": "failure", "run_attempt": 1,
           "head_sha": "0" * 40, "updated_at": "2026-10-28T19:31:05Z"}
    run.update(run_changes)
    return {"run": run, "jobs": [{"name": "sync-dashboard", "status": "completed",
                                  "conclusion": "success"},
                                 {"name": FINAL_JOB, "status": "completed", "conclusion": "failure"}]}


class _Req:
    def __init__(self, fn):
        self._fn = fn

    def execute(self):
        return self._fn()


class Sheets:
    """batchGet over a scripted sequence of (key, pf, t10) states; the last
    state repeats. Any other call (get/update/batchUpdate) is a test failure."""

    def __init__(self, states, faults=()):
        self.states = [copy.deepcopy(item) for item in states]
        self.faults = list(faults)          # per-call: an exception to raise, or None
        self.calls = []

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def batchGet(self, **kw):  # noqa: N802 - the Google client's spelling
        self.calls.append(copy.deepcopy(kw))
        index = len(self.calls) - 1

        def read():
            fault = self.faults[index] if index < len(self.faults) else None
            if fault is not None:
                raise fault
            grids = self.states[min(index, len(self.states) - 1)]
            return {"valueRanges": [{"range": r, "values": copy.deepcopy(g)}
                                    for r, g in zip(kw["ranges"], grids)]}
        return _Req(read)

    def __getattr__(self, name):
        raise AssertionError("unexpected Sheets call: " + name)


class Clock:
    def __init__(self):
        self.t = 5000.0
        self.sleeps = []

    def monotonic(self):
        return self.t

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.t += seconds


class Script:
    """Per-call scripted answers: a list (busy runs) or an exception; the last
    answer repeats; an empty script answers [] (nothing busy)."""

    def __init__(self, answers=None):
        self.answers = [copy.deepcopy(item) for item in (answers or [])]
        self.calls = 0

    def __call__(self):
        index = self.calls
        self.calls += 1
        if not self.answers:
            return []
        answer = self.answers[min(index, len(self.answers) - 1)]
        if isinstance(answer, BaseException):
            raise answer
        return copy.deepcopy(answer)


class Harness:
    def __init__(self, states=None, *, writers=None, releases=None, prov=None, prov_faults=(),
                 read_faults=(), dispatch_results=None):
        self.sheets = Sheets(states if states is not None else [state()], read_faults)
        self.writers = Script(writers)
        self.releases = Script(releases)
        self.prov = provenance_ok() if prov is None else prov
        self.prov_faults = list(prov_faults)
        self.dispatch_results = list(dispatch_results or [204])
        self.clock = Clock()
        self.calls = {"provenance": 0, "dispatch": []}
        self.dispatch_times = []

    def provenance(self, source):
        self.calls["provenance"] += 1
        if self.prov_faults:
            fault = self.prov_faults.pop(0)
            if fault is not None:
                raise fault
        return copy.deepcopy(self.prov)

    def dispatch(self, source):
        self.calls["dispatch"].append(source)
        self.dispatch_times.append(self.clock.t)
        result = self.dispatch_results.pop(0) if self.dispatch_results else 204
        if isinstance(result, BaseException):
            raise result
        return result

    def run(self, source=SOURCE, **kw):
        kw.setdefault("wait_min", 45)
        kw.setdefault("poll_s", 90)
        return W.run("synthetic-sheet", source, service=self.sheets, inflight=self.writers,
                     release_active=self.releases, provenance=self.provenance,
                     dispatch=self.dispatch, sleep=self.clock.sleep,
                     monotonic=self.clock.monotonic, now_utc=lambda: NOW, **kw)

    def github_calls(self):
        return self.calls["provenance"] + self.writers.calls + self.releases.calls

    def io_count(self):
        return self.github_calls() + len(self.sheets.calls) + len(self.calls["dispatch"])


def http(code):
    return urllib.error.HTTPError("https://api.github.com/x", code, "err", {}, io.BytesIO(b""))


def arm(mp):
    mp.setenv("TFB_RELEASE_AFTER_SYNC", "1")


def _clean(mp):
    for name in _ENV:
        mp.delenv(name, raising=False)
    mp.setenv("GITHUB_RUN_ID", "777123")


def _yaml(path):
    data = __import__("yaml").safe_load(Path(path).read_text(encoding="utf-8"))
    data["on"] = data.pop(True, data.get("on"))
    return data


# --------------------------------------------------------------------------- cases
def case_w1_gate_off_is_zero_io(mp, tmp_path):
    for word in (None, "", "0", "off", "false", "no", "enabled", "observe"):
        _clean(mp)
        if word is not None:
            mp.setenv("TFB_RELEASE_AFTER_SYNC", word)
        h = Harness()
        result = h.run()
        assert result["exit_code"] == 0 and result["outcome"] == "gate_off", word
        assert result["gate"] is False and h.io_count() == 0 and h.clock.sleeps == [], word
        assert result["dispatched"] is False
    for word in ("1", "true", "YES", " on "):
        _clean(mp)
        mp.setenv("TFB_RELEASE_AFTER_SYNC", word)
        assert W.gate_armed(), word


def case_w2_identity_and_bounds_refusals_are_zero_io(mp, tmp_path):
    _clean(mp)
    arm(mp)
    for source, reason in ((None, "source_run_invalid"), ("", "source_run_invalid"),
                           ("auto", "source_run_invalid"), ("0", "source_run_invalid"),
                           ("12a", "source_run_invalid"), (123, "source_run_invalid"),
                           ("777123", "source_is_this_run")):
        h = Harness()
        result = h.run(source)
        assert result["exit_code"] == 3 and reason in result["reasons"], (source, result)
        assert h.io_count() == 0, source
    h = Harness()
    result = W.run("  ", SOURCE, service=h.sheets, inflight=h.writers, release_active=h.releases,
                   provenance=h.provenance, dispatch=h.dispatch, sleep=h.clock.sleep,
                   monotonic=h.clock.monotonic)
    assert result["exit_code"] == 3 and "sheet_id_missing" in result["reasons"] and h.io_count() == 0
    for wait in ("0", "56", "x", "-1", "4.5", ""):
        h = Harness()
        result = h.run(wait_min=wait)
        assert result["exit_code"] == 3 and "wait_min_invalid" in result["reasons"], wait
        assert h.io_count() == 0, wait
    for poll in ("29", "601", "ninety"):
        h = Harness()
        result = h.run(poll_s=poll)
        assert result["exit_code"] == 3 and "poll_s_invalid" in result["reasons"], poll
        assert h.io_count() == 0, poll
    assert W.MAX_WAIT_MIN == 55 and W.DEFAULT_WAIT_MIN == 45 and W.MIN_POLL_S == 30
    assert W.PROBE_MIN_S == 270 and W.MAX_DISPATCH_FAILURES == 2


def case_w3_provenance_rule_and_bounded_retries(mp, tmp_path):
    _clean(mp)
    arm(mp)
    for changes, reason in (({"head_branch": "fw2-stale-pe"}, "not_main"),
                            ({"event": "pull_request"}, "event_not_production"),
                            ({"status": "in_progress"}, "source_not_completed"),
                            ({"path": ".github/workflows/ci.yml"}, "not_daily_sync")):
        h = Harness(prov=provenance_ok(**changes))
        result = h.run()
        assert result["exit_code"] == 0 and result["outcome"] == "stopped", changes
        assert result["reasons"][:2] == ["source_not_releasable", reason], result["reasons"]
        assert h.sheets.calls == [] and h.writers.calls == 0 and h.calls["dispatch"] == []
        assert result["provenance"]["ok"] is False
    no_final = provenance_ok()
    no_final["jobs"] = no_final["jobs"][:1]
    h = Harness(prov=no_final)
    result = h.run()
    assert result["outcome"] == "stopped" and "final_job_not_executed" in result["reasons"]
    assert h.calls["dispatch"] == []
    # transport faults: two are retried, the third fails closed
    h = Harness(prov_faults=[TimeoutError("api"), urllib.error.URLError("reset"), None])
    result = h.run()
    assert result["outcome"] == "dispatched" and h.calls["provenance"] == 3
    assert h.clock.sleeps[:2] == [90, 90] and result["faults"] == 2
    h = Harness(prov_faults=[TimeoutError("a"), TimeoutError("b"), TimeoutError("c")])
    result = h.run()
    assert result["exit_code"] == 3 and result["reasons"] == ["provenance_unavailable"]
    assert h.sheets.calls == [] and h.calls["dispatch"] == []
    assert result["source_completed_utc"] is None


def case_w4_ready_on_first_poll_dispatches_once(mp, tmp_path):
    _clean(mp)
    arm(mp)
    h = Harness()
    result = h.run()
    assert result["exit_code"] == 0 and result["outcome"] == "dispatched"
    assert result["reasons"] == ["both_receipts_verified"]
    assert h.calls["dispatch"] == [SOURCE] and result["dispatch_status"] == 204
    assert result["polls"] == 1 and result["probes"] == 1 and h.clock.sleeps == []
    assert h.writers.calls == 1 and h.releases.calls == 1 and len(h.sheets.calls) == 1
    call = h.sheets.calls[0]
    assert call["ranges"] == [final.KEY_RANGE, "Portfolio_Decision!A1:B3", "Top_10_Investments!A1:B3"]
    assert call["spreadsheetId"] == "synthetic-sheet"
    assert call["valueRenderOption"] == "UNFORMATTED_VALUE"
    # the two cockpit ranges are the ones the strict audit's live reader reads
    reader = inspect.getsource(freshness.run_live)
    assert '"Portfolio_Decision!A1:B3"' in reader and '"Top_10_Investments!A1:B3"' in reader
    assert "_cell(portfolio_grid, 1, 1)" in inspect.getsource(freshness.audit_surfaces)
    assert "_cell(top10_grid, 1, 1)" in inspect.getsource(freshness.audit_surfaces)
    assert result["first_seen_s"] == {"pf": 0, "t10": 0, "ready": 0}
    assert result["source_completed_utc"] == "2026-10-28T19:31:05Z"
    assert result["provenance"]["ok"] is True and result["last"]["state"] == "ready"
    assert result["last"]["gm_run"] == SOURCE and result["last"]["decision_run"] == SOURCE
    assert result["last"]["in_flight"] == 0 and result["last"]["release_runs_active"] == 0


def case_w5_receipts_landing_over_several_polls(mp, tmp_path):
    _clean(mp)
    arm(mp)
    older_pf = pf(verified(OLDER))
    older_t10 = t10(verified(OLDER))
    states = [state(pf_grid=older_pf, t10_grid=older_t10),        # 0 s: neither consumed
              state(t10_grid=older_t10),                           # 90 s: PF landed
              state(t10_grid=t10(" | source readiness WITHHELD: run in flight")),
              state()]                                             # 270 s: both
    h = Harness(states)
    result = h.run()
    assert result["outcome"] == "dispatched" and result["polls"] == 4
    assert h.clock.sleeps == [90, 90, 90] and h.calls["dispatch"] == [SOURCE]
    assert result["first_seen_s"] == {"pf": 90, "t10": 270, "ready": 270}
    # GitHub is not called while the workbook is not ready
    assert h.writers.calls == 1 and h.releases.calls == 1 and len(h.sheets.calls) == 4


def case_w6_every_feed_stop_ends_without_probe_or_dispatch(mp, tmp_path):
    _clean(mp)
    arm(mp)
    stops = [
        (state(key=key_grid(gm_value=gm(NEWER))), "superseded_by_newer_cohort"),
        (state(key=key_grid(gm_value=gm(OLDER))), "gm_feed_not_this_run"),
        (state(key=key_grid(gm_value=None)), "source_auto_key_missing"),
        (state(key=key_grid(gm_value="PARTIAL_FRESH | no run token")), "source_auto_run_unreadable"),
        (state(key=key_grid(feed=veto(NEWER))), "superseded_by_newer_cohort"),
        (state(key=key_grid(feed=veto(OLDER))), "decision_feed_not_this_run"),
        (state(key=key_grid(feed=None)), "decision_feed_unreadable"),
        (state(key=key_grid(feed="NOT_ACTIONABLE(owner_hold) | 2026-10-28 22:00:00+03:00")),
         "decision_feed_unreadable"),
        (state(key=key_grid(feed=live())), "already_executable"),
        (state(key=key_grid(feed="NOT_ACTIONABLE(final_publication) | run=" + OLDER +
                            " | 2026-10-28 21:00:00+03:00 | superseded_by_run=" + SOURCE + ".1")),
         "decision_feed_not_this_run"),
    ]
    for grids, reason in stops:
        h = Harness([grids])
        result = h.run()
        assert result["exit_code"] == 0 and result["outcome"] == "stopped", (reason, result)
        assert result["reasons"] == [reason], (reason, result["reasons"])
        assert h.calls["dispatch"] == [] and result["polls"] == 1 and h.clock.sleeps == []
        assert h.writers.calls == 0 and h.releases.calls == 0, reason
    # a cohort that moves mid-wait stops the wait at the next poll
    h = Harness([state(t10_grid=t10("")), state(t10_grid=t10("")),
                 state(key=key_grid(gm_value=gm(NEWER), feed=veto(NEWER)), t10_grid=t10(""))])
    result = h.run()
    assert result["reasons"] == ["superseded_by_newer_cohort"] and result["polls"] == 3
    assert h.calls["dispatch"] == []


def case_w7_unproven_receipts_time_out_bounded(mp, tmp_path):
    _clean(mp)
    arm(mp)
    variants = [
        ("", "none"),
        (verified(OLDER), "verified"),
        (verified() + verified(), "unproven"),
        (verified() + verified(OLDER), "unproven"),
        (verified() + " | source readiness WITHHELD: source stale", "unproven"),
        (" | source readiness WITHHELD: Commodities_FX acquired 430/453 below 95%", "withheld"),
        (" | source readiness VERIFIED (run local)", "unproven"),
        (" | run=" + SOURCE + " sha=0123abcd", "none"),
    ]
    for receipt, kind in variants:
        h = Harness([state(t10_grid=t10(receipt))])
        result = h.run(wait_min=10, poll_s=60)
        assert result["exit_code"] == 0 and result["outcome"] == "timeout", receipt
        assert result["reasons"] == ["receipts_timeout"] and h.calls["dispatch"] == []
        assert result["polls"] == 11 and h.clock.sleeps == [60] * 10, (receipt, result["polls"])
        assert result["last"]["t10"]["declaration"] == kind, (receipt, result["last"]["t10"])
        assert result["last"]["pf"]["consumed_run"] == SOURCE
        assert result["first_seen_s"]["t10"] is None and h.github_calls() == 1
    h = Harness([state(pf_grid=pf(""), t10_grid=t10(""))])
    result = h.run()                      # defaults: 45 min, 90 s
    assert result["polls"] == 31 and sum(h.clock.sleeps) == 2700 and result["waited_s"] == 2700


def case_w8_read_layout_and_probe_faults(mp, tmp_path):
    _clean(mp)
    arm(mp)
    h = Harness([state()], read_faults=[TimeoutError("read timed out"), None])
    result = h.run()
    assert result["outcome"] == "dispatched" and result["polls"] == 2 and result["faults"] == 1
    assert h.clock.sleeps == [90]
    h = Harness([state()], read_faults=[TimeoutError("1"), TimeoutError("2"), TimeoutError("3")])
    result = h.run()
    assert result["exit_code"] == 3 and result["reasons"] == ["read_faults"]
    assert h.calls["dispatch"] == [] and result["polls"] == 3
    # a non-consecutive pattern never trips the bound
    h = Harness([state(t10_grid=t10(""))] * 4 + [state()],
                read_faults=[TimeoutError("1"), TimeoutError("2"), None, TimeoutError("3"), None])
    result = h.run()
    assert result["outcome"] == "dispatched" and result["faults"] == 3 and result["polls"] == 5
    for layout in (key_grid(extra=[["", "orphan"]]),
                   key_grid(extra=[["TFB Feed Global_Markets", gm()]]),
                   [["unknown", "x"]]):
        h = Harness([state(key=layout)])
        result = h.run()
        assert result["exit_code"] == 3 and result["reasons"] == ["read_faults"], layout
        assert h.calls["dispatch"] == []
    # probe faults: retried on the probe cadence, the third in a row fails
    h = Harness(writers=[RuntimeError("GitHub API 502")])
    result = h.run()
    assert result["exit_code"] == 3 and result["reasons"] == ["probe_faults"]
    assert result["probes"] == 3 and h.calls["dispatch"] == []
    assert sum(h.clock.sleeps) == 2 * W.PROBE_MIN_S
    for writers, releases in (([RuntimeError("502"), []], None),
                              (None, [ValueError("release run listing is malformed"), []]),
                              (None, [{"malformed": True}, []]),
                              ([("not", "a", "list"), []], None)):
        h = Harness(writers=writers, releases=releases)
        result = h.run()
        assert result["outcome"] == "dispatched", (writers, releases, result)
        assert result["probes"] == 2 and result["faults"] == 1 and len(h.calls["dispatch"]) == 1
    listing = {"workflow_runs": [{"id": 1, "status": "completed", "event": "schedule"},
                                 {"id": 2, "status": "queued", "event": "workflow_dispatch"},
                                 {"id": 3, "status": "waiting", "event": "schedule"},
                                 {"id": 4, "status": "in_progress", "event": "workflow_dispatch"}]}
    seen = []
    active = W.github_release_active(http_get=lambda path: seen.append(path) or listing)
    assert [item["id"] for item in active] == [2, 3, 4]
    assert seen == ["/repos/{repo}/actions/workflows/release_decision_feed.yml/runs?per_page=30"]
    for bad in ({}, {"workflow_runs": None}, {"workflow_runs": ["x"]}, []):
        with pytest.raises((ValueError, TypeError)):
            W.github_release_active(http_get=lambda path, bad=bad: bad)


def case_w9_dispatch_faults_never_blind_retry(mp, tmp_path):
    _clean(mp)
    arm(mp)
    h = Harness(dispatch_results=[http(403)])
    result = h.run()
    assert result["exit_code"] == 3 and result["outcome"] == "dispatch_failed"
    assert len(h.calls["dispatch"]) == 1 and result["dispatched"] is False
    # 502: no immediate re-POST; the next due probe finds nothing -> second POST
    h = Harness(dispatch_results=[http(502), 204])
    result = h.run()
    assert result["outcome"] == "dispatched" and len(h.calls["dispatch"]) == 2
    assert h.dispatch_times[1] - h.dispatch_times[0] >= W.PROBE_MIN_S
    assert result["probes"] == 2 and result["dispatch_attempts"] == 2
    assert result["dispatch_error"] is None
    # 502 that did create the run: the next probe sees it, nothing is re-POSTed,
    # and once it released the feed reads EXECUTABLE -> stop
    states = [state()] * 4 + [state(key=key_grid(feed=live()))]
    h = Harness(states, releases=[[], BUSY_RELEASE, []], dispatch_results=[http(502)])
    result = h.run()
    assert result["outcome"] == "stopped" and result["reasons"] == ["already_executable"]
    assert len(h.calls["dispatch"]) == 1 and result["dispatched"] is False
    assert result["dispatch_error"] is not None
    # two transport faults fail; a non-HTTP contract fault fails at once
    h = Harness(dispatch_results=[urllib.error.URLError("reset"), urllib.error.URLError("reset")])
    result = h.run()
    assert result["exit_code"] == 3 and len(h.calls["dispatch"]) == 2
    for fault in (ValueError("bad"), RuntimeError("dispatch returned HTTP 201"), http(404)):
        h = Harness(dispatch_results=[fault])
        result = h.run()
        assert result["exit_code"] == 3 and len(h.calls["dispatch"]) == 1, fault


def case_w10_github_dispatch_request(mp, tmp_path):
    _clean(mp)
    seen = []

    class Response:
        def __init__(self, status):
            self.status = status

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def opener(status):
        def _open(request, timeout):
            seen.append((request, timeout))
            return Response(status)
        return _open
    with pytest.raises(RuntimeError):
        W.github_dispatch(SOURCE, opener=opener(204))
    mp.setenv("GITHUB_TOKEN", "ghs_synthetic")
    mp.setenv("GITHUB_REPOSITORY", "owner/repo")
    assert W.github_dispatch(SOURCE, opener=opener(204)) == 204
    request, timeout = seen[-1]
    assert request.get_method() == "POST" and timeout == 20
    assert request.full_url == ("https://api.github.com/repos/owner/repo/actions/workflows/"
                                "release_decision_feed.yml/dispatches")
    assert json.loads(request.data.decode("ascii")) == {
        "ref": "main", "inputs": {"source_run_id": SOURCE, "mode": "release"}}
    headers = {key.lower(): value for key, value in request.header_items()}
    assert headers["authorization"] == "Bearer ghs_synthetic"
    assert headers["x-github-api-version"] == "2022-11-28"
    assert headers["content-type"] == "application/json"
    assert W.github_dispatch(SOURCE, opener=opener(200)) == 200
    with pytest.raises(RuntimeError):
        W.github_dispatch(SOURCE, opener=opener(201))
    with pytest.raises(ValueError):
        W.github_dispatch("auto", opener=opener(204))
    mp.setenv("GITHUB_API_URL", "http://api.github.com")
    with pytest.raises(RuntimeError):
        W.github_dispatch(SOURCE, opener=opener(204))
    mp.delenv("GITHUB_API_URL")
    mp.setenv("GITHUB_REPOSITORY", "owner/repo/../x")
    with pytest.raises(RuntimeError):
        W.github_dispatch(SOURCE, opener=opener(204))


def case_w11_evidence_carries_no_status_text(mp, tmp_path):
    _clean(mp)
    arm(mp)
    for grids in ([state()], [state(t10_grid=t10(""))]):
        h = Harness(grids, writers=[BUSY_WRITER])
        result = h.run(wait_min=2, poll_s=60)
        text = json.dumps(result)
        for fragment in ("holdings", "cash", "1234.56", "sheets pool", "Market_Leaders",
                         "Status:", "source readiness", "PARTIAL_FRESH", "decision_failed",
                         "synthetic.invalid", "page_refresh_recovery"):
            assert fragment not in text, fragment
    out = tmp_path / "out.json"
    h, original = Harness(), W.run
    mp.setattr(W, "run", lambda sheet, source, **kw: original(
        sheet, source, wait_min=kw.get("wait_min"), poll_s=kw.get("poll_s"), service=h.sheets,
        inflight=h.writers, release_active=h.releases, provenance=h.provenance,
        dispatch=h.dispatch, sleep=h.clock.sleep, monotonic=h.clock.monotonic,
        now_utc=lambda: NOW))
    buffer = io.StringIO()
    mp.setattr(sys, "stdout", buffer)
    assert W.main(["--source-run-id", SOURCE, "--sheet-id", "s", "--json-out", str(out)]) == 0
    printed = json.loads(buffer.getvalue().strip().splitlines()[-1])
    assert set(printed) == set(W._PRINT_KEYS) and printed["outcome"] == "dispatched"
    assert "last" not in printed and "provenance" not in printed


def _audit_findings(pf_grid, t10_grid):
    """The REAL strict decision audit on minimal grids (My_Portfolio published
    by SOURCE); only its two consumption findings matter here."""
    status = [["Page", "Last Updated", "Status", "Message", "Endpoint", "HTTP Code", "Rows",
               "Columns"],
              ["My_Portfolio", "2026-10-28 22:28:44", "VALID",
               "complete acquired=10/10 acquisition=COMPLETE data=COMPLETE run=" + SOURCE,
               "/read-only", 200, 10, 122]]
    report = freshness.audit_surfaces(status, pf_grid, t10_grid, now_utc=NOW,
                                      min_rows={"Market_Leaders": 255}, expected_source_run=SOURCE)
    return {finding.code for finding in report.findings}


def case_w12_receipt_parsing_agrees_with_the_real_audit(mp, tmp_path):
    receipts = ["", verified(), verified(OLDER), verified() + verified(),
                verified() + verified(OLDER), verified() + " | source readiness WITHHELD: x",
                " | source readiness VERIFIED (run local)", " | source readiness VERIFIED (run 0)",
                " | source readiness WITHHELD: stale", " | Source Readiness verified (run " + SOURCE + ")",
                " | run=" + SOURCE, " | source readiness VERIFIED (run " + SOURCE + "9)"]
    agreed = 0
    for receipt in receipts:
        for name, grid_pf, grid_t10, code in (
                ("pf", pf(receipt), t10(), "PF_SOURCE_CONSUMPTION_UNPROVEN"),
                ("t10", pf(), t10(receipt), "T10_SOURCE_CONSUMPTION_UNPROVEN")):
            waiter_ready = W.surface_receipt(grid_pf if name == "pf" else grid_t10)["consumed_run"] == SOURCE
            audit_ready = code not in _audit_findings(grid_pf, grid_t10)
            assert waiter_ready is audit_ready, (name, receipt, waiter_ready, audit_ready)
            agreed += 1
    assert agreed == 2 * len(receipts)
    parsed = W.surface_receipt(pf())
    assert parsed == {"consumed_run": SOURCE, "declaration": "verified",
                      "run_riyadh": "2026-10-28 22:40:00", "state": "OK"}
    for broken in (None, [], [["only title"]], [["t"], ["Status:"]], "text", [[None], [None, None]]):
        parsed = W.surface_receipt(broken)
        assert parsed["consumed_run"] is None and parsed["declaration"] == "none", broken


def case_w13_cli(mp, tmp_path):
    _clean(mp)
    mp.setattr(sys, "stdout", io.StringIO())
    out = tmp_path / "gate.json"
    assert W.main(["--source-run-id", SOURCE, "--sheet-id", "s", "--json-out", str(out)]) == 0
    assert json.loads(out.read_text())["outcome"] == "gate_off"
    arm(mp)
    out = tmp_path / "missing.json"
    assert W.main(["--sheet-id", "s", "--json-out", str(out)]) == 3
    assert "source_run_invalid" in json.loads(out.read_text())["reasons"]
    out = tmp_path / "wait.json"
    assert W.main(["--source-run-id", SOURCE, "--sheet-id", "s", "--wait-min", "90",
                   "--json-out", str(out)]) == 3
    assert "wait_min_invalid" in json.loads(out.read_text())["reasons"]
    mp.setenv("TFB_RELEASE_AFTER_SYNC_WAIT_MIN", "70")
    out = tmp_path / "envwait.json"
    assert W.main(["--source-run-id", SOURCE, "--sheet-id", "s", "--json-out", str(out)]) == 3
    assert "wait_min_invalid" in json.loads(out.read_text())["reasons"]
    mp.setattr(sys, "stderr", io.StringIO())
    assert W.main(["--bogus"]) == 3
    mp.setattr(W, "run", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    out = tmp_path / "fatal.json"
    assert W.main(["--source-run-id", SOURCE, "--json-out", str(out)]) == 3
    assert json.loads(out.read_text())["outcome"] == "fatal"


EXPECTED_IF = (
    "${{\n"
    "  github.event.workflow_run.head_branch == 'main' &&\n"
    "  (\n"
    "    github.event.workflow_run.event == 'schedule' ||\n"
    "    github.event.workflow_run.event == 'workflow_dispatch'\n"
    "  ) &&\n"
    "  (\n"
    "    github.event.workflow_run.conclusion == 'success' ||\n"
    "    github.event.workflow_run.conclusion == 'failure'\n"
    "  ) &&\n"
    "  contains(fromJSON('[\"1\", \"true\", \"yes\", \"on\"]'), vars.TFB_RELEASE_AFTER_SYNC)\n"
    "}}")


def case_w14_workflow_contract(mp, tmp_path):
    text = AFTER_YML.read_text(encoding="utf-8")
    sync_name = _yaml(DAILY_YML)["name"]
    for number, line in enumerate(text.splitlines(), 1):
        if any(ord(ch) > 127 for ch in line):
            # the only non-ASCII text is the Advanced Sync's own name in the trigger
            assert line.strip() == 'workflows: ["' + sync_name + '"]', number
    doc = _yaml(AFTER_YML)
    assert doc["on"] == {"workflow_run": {"workflows": [sync_name], "types": ["completed"]}}
    assert doc["permissions"] == {"contents": "read", "actions": "write"}
    # JOB-level group: a run whose job is skipped (every PR/push run of the
    # sync) never enters it, so it can never cancel a pending real attempt
    assert "concurrency" not in doc
    assert list(doc["jobs"]) == ["await-receipts-then-release"]
    job = doc["jobs"]["await-receipts-then-release"]
    assert job["concurrency"] == {"group": "tfb-release-after-sync", "cancel-in-progress": False}
    release_doc = _yaml(RELEASE_YML)
    assert job["concurrency"]["group"] != release_doc["concurrency"]["group"]
    for path in sorted((ROOT / ".github" / "workflows").glob("*.yml")):
        if path == AFTER_YML:
            continue
        other = _yaml(path)
        groups = [other.get("concurrency")] + [item.get("concurrency") for item in
                                               (other.get("jobs") or {}).values()]
        names = [group.get("group") if isinstance(group, dict) else group for group in groups]
        assert "tfb-release-after-sync" not in names, path.name
    assert job["if"] == EXPECTED_IF
    assert job["runs-on"] == "ubuntu-24.04" and job["environment"] == "production"
    assert job["timeout-minutes"] >= W.MAX_WAIT_MIN + 5
    assert job["env"] == {
        "PYTHONPATH": "${{ github.workspace }}",
        "TARGET_SHEET_ID": "${{ vars.DEFAULT_SPREADSHEET_ID }}",
        "TFB_RELEASE_AFTER_SYNC": "${{ vars.TFB_RELEASE_AFTER_SYNC || '0' }}",
        "TFB_RELEASE_AFTER_SYNC_WAIT_MIN": "${{ vars.TFB_RELEASE_AFTER_SYNC_WAIT_MIN || '%d' }}"
                                           % W.DEFAULT_WAIT_MIN}
    steps = {step["name"]: step for step in job["steps"]}
    for step in job["steps"]:
        assert "${{" not in step.get("run", ""), step["name"]
    await_step = steps["Await cockpit receipts and dispatch the release"]
    assert await_step["env"] == {"SOURCE_RUN_ID": "${{ github.event.workflow_run.id }}",
                                 "GITHUB_TOKEN": "${{ github.token }}"}
    assert "python scripts/await_cockpit_receipts.py" in await_step["run"]
    for flag in ('--sheet-id "$TARGET_SHEET_ID"', '--source-run-id "$SOURCE_RUN_ID"',
                 '--wait-min "$TFB_RELEASE_AFTER_SYNC_WAIT_MIN"',
                 '--json-out await_cockpit_receipts.json'):
        assert flag in await_step["run"], flag
    assert 'exit "$code"' in await_step["run"]
    rel_job = release_doc["jobs"]["release-decision-feed"]
    rel_steps = {step["name"]: step for step in rel_job["steps"]}
    creds = steps["Configure read-only audit credentials"]
    assert creds["run"] == rel_steps["Configure release-audit credentials"]["run"]
    assert creds["env"] == rel_steps["Configure release-audit credentials"]["env"]
    assert steps["Secure credential cleanup"]["run"] == rel_steps["Secure credential cleanup"]["run"]
    assert steps["Install dependencies"]["run"] == rel_steps["Install dependencies"]["run"]
    assert steps["Setup Python 3.11"]["with"] == rel_steps["Setup Python 3.11"]["with"]
    assert steps["Checkout this workflow's ref"]["with"] == {
        "ref": "${{ github.sha }}", "fetch-depth": 1, "persist-credentials": False}
    upload = steps["Upload after-sync evidence"]
    assert upload["uses"] == rel_steps["Upload release evidence"]["uses"]
    assert upload["with"]["path"] == "await_cockpit_receipts.json"
    # the release workflow stays the only production entry and is not edited here
    assert "workflow_run" not in release_doc["on"]
    assert "await_cockpit_receipts" not in RELEASE_YML.read_text(encoding="utf-8")


def case_w15_dispatch_matches_the_release_contract_and_manifest(mp, tmp_path):
    release_doc = _yaml(RELEASE_YML)
    inputs = release_doc["on"]["workflow_dispatch"]["inputs"]
    assert set(inputs) == {"source_run_id", "mode"} and "release" in inputs["mode"]["options"]
    assert inputs["source_run_id"]["type"] == "string"
    validate = {step["name"]: step for step in
                release_doc["jobs"]["release-decision-feed"]["steps"]}["Validate dispatch inputs"]
    assert '"$GITHUB_REF" != "refs/heads/main"' in validate["run"] and W.RELEASE_REF == "main"
    pattern = re.search(r'"\$SOURCE_RUN_ID" =~ (\S+) \]\]', validate["run"]).group(1)
    assert re.fullmatch(pattern, SOURCE) and not re.fullmatch(pattern, "0")
    assert (ROOT / ".github" / "workflows" / W.RELEASE_WORKFLOW).is_file()
    pins = re.findall(r'\("await_cockpit_receipts",\s*"([0-9.]+)"', VERIFIER.read_text(encoding="utf-8"))
    assert pins == [W.SCRIPT_VERSION]
    for path in (ROOT / "scripts" / "await_cockpit_receipts.py", Path(__file__)):
        added = [line for line in path.read_text(encoding="utf-8").splitlines()
                 if any(ord(ch) > 127 for ch in line)]
        assert added == [], path.name


class _CI(str):
    """A GitHub expression string: == / != compare case-insensitively."""
    def __eq__(self, other):
        return str(self).casefold() == str(other).casefold()

    def __ne__(self, other):
        return not self.__eq__(other)

    __hash__ = str.__hash__


def evaluate_job_if(expression, *, branch, event, conclusion, variable):
    """Evaluate the Actions subset the job condition uses (contains, fromJSON,
    &&, ||, ==) with GitHub's case-insensitive string semantics."""
    assert expression.startswith("${{") and expression.endswith("}}"), expression
    source = expression[3:-2].strip().replace("&&", " and ").replace("||", " or ")
    tree = ast.parse("(" + source + ")", mode="eval")     # multi-line: one parenthesised expression
    allowed = (ast.Expression, ast.BoolOp, ast.And, ast.Or, ast.Compare, ast.Eq,
               ast.Constant, ast.Name, ast.Load, ast.Attribute, ast.Call)
    for node in ast.walk(tree):
        assert isinstance(node, allowed), type(node).__name__
        if isinstance(node, ast.Call):
            assert isinstance(node.func, ast.Name) and node.func.id in {"contains", "fromJSON"}
    run = SimpleNamespace(head_branch=_CI(branch), event=_CI(event), conclusion=_CI(conclusion))
    scope = {
        "vars": SimpleNamespace(TFB_RELEASE_AFTER_SYNC=_CI(variable)),
        "github": SimpleNamespace(event=SimpleNamespace(workflow_run=run)),
        "fromJSON": lambda text: [_CI(item) for item in json.loads(text)],
        "contains": lambda items, item: any(_CI(x) == item for x in items),
    }
    return bool(eval(compile(tree, "<job if>", "eval"), {"__builtins__": {}}, scope))


def case_w16_job_condition_matrix(mp, tmp_path):
    expression = _yaml(AFTER_YML)["jobs"]["await-receipts-then-release"]["if"]
    seen = 0
    for branch in ("main", "fw2-stale-pe", "refs/heads/main", ""):
        for event in ("schedule", "workflow_dispatch", "pull_request", "push", "workflow_run"):
            for conclusion in ("success", "failure", "cancelled", "skipped", "timed_out", ""):
                for variable in ("1", "true", "Yes", "ON", "", "0", "off", "observe", " 1"):
                    want = (branch == "main" and event in ("schedule", "workflow_dispatch")
                            and conclusion in ("success", "failure")
                            and variable.lower() in ("1", "true", "yes", "on"))
                    got = evaluate_job_if(expression, branch=branch, event=event,
                                          conclusion=conclusion, variable=variable)
                    assert got is want, (branch, event, conclusion, variable)
                    seen += want
    assert seen == 1 * 2 * 2 * 4


def case_w17_busy_writers_and_release_runs_block_the_dispatch_only(mp, tmp_path):
    _clean(mp)
    arm(mp)
    for writers, releases, reason in (([BUSY_WRITER, BUSY_WRITER, []], None, "production_run_in_flight"),
                                      (None, [BUSY_RELEASE, BUSY_RELEASE, []], "release_run_active")):
        h = Harness(writers=writers, releases=releases)
        result = h.run()
        assert result["outcome"] == "dispatched", (reason, result)
        assert result["probes"] == 3 and len(h.calls["dispatch"]) == 1
        # probes every PROBE_MIN_S (polls 1, 4, 7 at 90 s), never one per poll
        assert result["polls"] == 7 and h.dispatch_times[0] - 5000.0 == 2 * W.PROBE_MIN_S
    # an active release run whose release lands: stop, nothing is queued behind it
    h = Harness([state()] * 4 + [state(key=key_grid(feed=live()))], releases=[BUSY_RELEASE])
    result = h.run()
    assert result["outcome"] == "stopped" and result["reasons"] == ["already_executable"]
    assert h.calls["dispatch"] == []
    # a writer busy for the whole window: timeout, the attempt never ends early,
    # and the GitHub budget stays bounded (probes <= 1 + 2700 / 270)
    h = Harness(writers=[BUSY_WRITER])
    result = h.run()
    assert result["outcome"] == "timeout"
    assert result["reasons"] == ["ready_but_blocked", "production_run_in_flight"]
    assert result["polls"] == 31 and result["probes"] == 11 and h.calls["dispatch"] == []
    assert h.writers.calls == 11 and h.releases.calls == 11
    assert result["last"]["state"] == "wait" and result["last"]["in_flight"] is not None


# --------------------------------------------------------------------------- runner
CASES = [value for _name, value in sorted(
    ((name, value) for name, value in globals().items() if name.startswith("case_w") and callable(value)),
    key=lambda kv: int(re.sub(r"\D", "", kv[0].split("_")[1]) or 0))]


@pytest.mark.parametrize("case", CASES, ids=[case.__name__[5:] for case in CASES])
def test_await_cockpit_receipts(case, monkeypatch, tmp_path):
    case(monkeypatch, tmp_path)


def _main() -> int:
    passed = failed = 0
    for case in CASES:
        with pytest.MonkeyPatch.context() as mp, tempfile.TemporaryDirectory() as tmp:
            try:
                case(mp, Path(tmp))
            except Exception:  # noqa: BLE001
                failed += 1
                print("FAIL " + case.__name__)
                traceback.print_exc()
                continue
        passed += 1
    total = passed + failed
    print(("PASS" if failed == 0 else "FAIL") + " %d/%d" % (passed, total))
    return 0 if failed == 0 and total == len(CASES) else 1


if __name__ == "__main__":
    sys.exit(_main())
