#!/usr/bin/env python3
"""tests/test_release_auto_source.py - release_decision_feed v1.2.0: the
scheduled release discovers its source cohort from the bounded _Status block.

WHY (2026-10-19)
----------------
The Top_10 board rendered 'FEED NOT ACTIONABLE ... 0 EXECUTABLE' on every
run since 15 Oct while 11-12 names qualified. The only promotion path was a
manual dispatch carrying the sync run id, so nothing was ever released. The
workflow now also runs on a schedule with --source-run-id auto. These tests
pin the discovery rule and prove that everything after it is the unchanged
v1.1.0 path (gates, provenance, probes, strict audits).

  A1  discover_source: exactly one 'TFB Feed Global_Markets' key with exactly
      one run= token resolves; missing, repeated, multi-token or layout faults
      are unresolved with a distinct reason
  A2  run(source='auto'): resolves from the key block, records source_mode,
      and continues into provenance with the discovered run (not GITHUB_RUN_ID)
  A3  run(source='auto') unresolved: refused, zero writes, source_run_id None
  A4  mode=kill with an unresolved auto cohort still retracts a live
      EXECUTABLE, using the run token of the value it retracts
  A5  an explicit numeric source behaves exactly as before (source_mode explicit)
  A6  CLI accepts 'auto' and still refuses a missing source
  A7  manifest pin and the workflow invariants the schedule relies on

Run: pytest -q tests/test_release_auto_source.py
"""
from __future__ import annotations

import asyncio
import copy
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from scripts import finalize_decision_publication as final  # noqa: E402
from scripts import release_decision_feed as R  # noqa: E402

RUN = "39172358424"
GM = "PARTIAL_FRESH | cov=99.6 | run=" + RUN + " | 2026-10-19 02:28:44+03:00"
VETO = ("NOT_ACTIONABLE(final_publication) | run=" + RUN +
        " | 2026-10-19 02:32:29+03:00 | coverage_failed,decision_failed")
LIVE = ("EXECUTABLE | run=" + RUN + " | 2026-10-19 02:28:44+03:00 | issued=2026-10-19T03:50:00+03:00"
        " | expires=2026-10-19T10:28:44+03:00 | ttl_min=480 | release=777001 | attempt=1"
        " | receipts=T10:" + RUN + ",PF:" + RUN + " | t10_at=2026-10-19T03:40:00+03:00"
        " | pf_at=2026-10-19T02:35:00+03:00 | mp_at=2026-10-19T02:28:44+03:00 | audits=0,0")


def grid(*, gm=GM, feed=VETO, extra=()):
    rows = [["Backend URL", "https://synthetic.invalid"], ["Last Global Update", "stamp"],
            ["TFB Feed Market_Leaders", "OK | run=" + RUN + " | 2026-10-19 01:32:39+03:00"]]
    if gm is not None:
        rows.append(["TFB Feed Global_Markets", gm])
    if feed is not None:
        rows.append([final.KEY, feed])
    rows.extend(extra)
    return rows


class _Req:
    def __init__(self, fn):
        self._fn = fn
        self.num_retries = None

    def execute(self, num_retries=None):
        # v1.2.1: the real googleapiclient HttpRequest.execute takes
        # num_retries; the armed slot deferral passes it on the key read.
        self.num_retries = num_retries
        return self._fn()


class Service:
    """The values get/update surface over one in-memory key block."""

    def __init__(self, rows):
        self.rows = [list(r) for r in rows]
        self.calls = []

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def get(self, **kw):
        self.calls.append(("get", copy.deepcopy(kw)))

        def read():
            if kw["range"] == final.KEY_RANGE:
                return {"values": copy.deepcopy(self.rows)}
            slot = int(kw["range"].split("!L")[1].split(":")[0])
            return {"values": [copy.deepcopy(self.rows[slot - 1])]}
        return _Req(read)

    def update(self, **kw):
        self.calls.append(("update", copy.deepcopy(kw)))

        def write():
            slot = int(kw["range"].split("!L")[1].split(":")[0])
            self.rows[slot - 1] = copy.deepcopy(kw["body"]["values"][0])
            return {"updatedCells": 2}
        return _Req(write)

    def updates(self):
        return [kw for kind, kw in self.calls if kind == "update"]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in ("TFB_DECISION_RELEASE", "TFB_DECISION_RELEASE_KILL", "GITHUB_TOKEN",
                 "GITHUB_REPOSITORY", "TFB_RELEASE_INFLIGHT_DEFER", "GITHUB_EVENT_NAME"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_RUN_ID", "777002")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")


def _run(service, source, *, mode="release", provenance=None, inflight=None, env_kill=False):
    seen = {"provenance": []}

    def prov(run_id):
        seen["provenance"].append(run_id)
        if provenance is not None:
            return provenance(run_id)
        raise RuntimeError("provenance unavailable (offline)")

    out = asyncio.run(R.run("synthetic-sheet", source, mode=mode, service=service,
                            inflight=inflight or (lambda: []), provenance=prov))
    return out, seen


def test_a1_discover_source_rule():
    assert R.discover_source(grid()) == (RUN, None)
    assert R.discover_source(grid(gm=None)) == (None, "source_auto_key_missing")
    assert R.discover_source(grid(extra=[["TFB Feed Global_Markets", GM]])) == \
        (None, "source_auto_key_duplicated")
    assert R.discover_source(grid(gm="PARTIAL_FRESH | run=1 | run=2")) == (None, "source_auto_run_unreadable")
    assert R.discover_source(grid(gm="PARTIAL_FRESH | no cohort")) == (None, "source_auto_run_unreadable")
    assert R.discover_source(grid(gm="OK | rerun=12 | x")) == (None, "source_auto_run_unreadable")
    assert R.discover_source("not-a-grid") == (None, "source_auto_layout_invalid")
    # key match is case-insensitive and whitespace-tolerant, like publication_slot
    assert R.discover_source(grid(gm=None, extra=[["  tfb feed global_markets ", GM]])) == (RUN, None)


def test_a2_auto_resolves_and_binds_provenance_to_the_discovered_run():
    svc = Service(grid())
    out, seen = _run(svc, R.SOURCE_AUTO)
    assert out["source_mode"] == "auto"
    assert out["source_run_id"] == RUN
    # the discovered run, never the release run id, reached the provenance probe
    assert seen["provenance"] == [RUN]
    # offline provenance => the unchanged v1.1.0 deferral, zero writes
    assert out["outcome"] == "deferred" and out["exit_code"] == 1
    assert "provenance_unavailable" in out["reasons"]
    assert svc.updates() == []


@pytest.mark.parametrize("rows, reason", [
    (grid(gm=None), "source_auto_key_missing"),
    (grid(gm="PARTIAL_FRESH | run=1 | run=2"), "source_auto_run_unreadable"),
])
def test_a3_auto_unresolved_is_refused_without_writes(rows, reason):
    svc = Service(rows)
    out, seen = _run(svc, R.SOURCE_AUTO)
    assert out["outcome"] == "refused" and out["exit_code"] == 3
    assert reason in out["reasons"]
    assert out["source_run_id"] is None and out["source_mode"] == "auto"
    assert seen["provenance"] == [] and svc.updates() == []


def test_a4_kill_with_unresolved_auto_still_retracts_a_live_release():
    svc = Service(grid(gm=None, feed=LIVE))
    out, _seen = _run(svc, R.SOURCE_AUTO, mode="kill")
    assert out["outcome"] == "killed" and out["exit_code"] == 0
    assert out["source_run_id"] == RUN              # taken from the retracted value
    assert out["blocker_published"] is True
    assert len(svc.updates()) == 1
    written = svc.updates()[0]["body"]["values"][0][1]
    assert written.startswith(R.DOWNGRADE_PREFIX + "run=" + RUN)
    assert "release_killed" in written and not R.reads_executable(written)


def test_a4b_sticky_kill_variable_on_a_scheduled_auto_run(monkeypatch):
    monkeypatch.setenv("TFB_DECISION_RELEASE_KILL", "1")
    svc = Service(grid(feed=LIVE))
    out, _seen = _run(svc, R.SOURCE_AUTO)        # mode=release, as the schedule sends
    assert out["kill_active"] is True and out["outcome"] == "killed"
    assert out["source_run_id"] == RUN and len(svc.updates()) == 1


def test_a5_explicit_source_is_unchanged():
    svc = Service(grid())
    out, seen = _run(svc, RUN)
    assert out["source_mode"] == "explicit" and out["source_run_id"] == RUN
    assert seen["provenance"] == [RUN]
    bad, seen_bad = _run(Service(grid()), "not-a-run")
    assert bad["outcome"] == "refused" and "source_run_invalid" in bad["reasons"]
    assert seen_bad["provenance"] == []


def test_a6_cli_accepts_auto_and_refuses_a_missing_source(monkeypatch, tmp_path):
    captured = {}

    async def fake_run(sheet_id, source, **kwargs):
        captured["source"] = source
        return {"exit_code": 0, "outcome": "dry_run_accepted", "mode": kwargs.get("mode")}

    monkeypatch.setattr(R, "run", fake_run)
    out = tmp_path / "r.json"
    code = R.main(["--sheet-id", "sheet", "--source-run-id", "auto", "--json-out", str(out)])
    assert code == 0 and captured["source"] == "auto" and out.exists()
    assert R.main(["--sheet-id", "sheet", "--json-out", str(tmp_path / "x.json")]) == 3


def test_a7_pin_and_workflow_invariants():
    # 2026-10-24 (v1.2.1): a version floor plus the verifier pin of the same value.
    assert tuple(int(x) for x in R.SCRIPT_VERSION.split(".")) >= (1, 2, 0)
    verifier = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("release_decision_feed", "%s"' % R.SCRIPT_VERSION in verifier
    text = (_REPO / ".github" / "workflows" / "release_decision_feed.yml").read_text(encoding="utf-8")
    assert "schedule:" in text and "source_run_id || 'auto'" in text
    # the schedule never kills or sweeps by itself; the sticky variable does
    assert "a scheduled run is always mode=release with source_run_id=auto" in text
    # exits 1 and 2 are tolerated only on the schedule
    assert '"$EVENT_NAME" == "schedule" && ( "$code" -eq 1 || "$code" -eq 2 )' in text
    assert R.GM_FEED_KEY == "TFB Feed Global_Markets"



def test_a8_scheduled_slot_inside_the_superseding_sync(monkeypatch):
    """v1.2.1 (blocking-lane guard; the full battery is
    tests/test_release_inflight_defer.py): off refuses as v1.2.0; armed on a
    schedule with the superseding sync in flight defers with zero writes."""
    sup = ("NOT_ACTIONABLE(final_publication) | run=" + RUN +
           " | 2026-10-24 12:00:00+03:00 | superseded_by_run=40079965055.1")
    busy = [{"workflow": "daily_sync.yml", "id": 40079965055, "event": "schedule",
             "status": "in_progress"}]
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    off, _ = _run(Service(grid(feed=sup)), R.SOURCE_AUTO, inflight=lambda: busy)
    assert (off["exit_code"], off["outcome"], off["reasons"]) == (3, "refused", ["superseded_by_run"])
    assert "inflight_defer" not in off
    monkeypatch.setenv("TFB_RELEASE_INFLIGHT_DEFER", "1")
    svc = Service(grid(feed=sup))
    on, seen = _run(svc, R.SOURCE_AUTO, inflight=lambda: busy)
    assert (on["exit_code"], on["outcome"]) == (1, "deferred")
    assert on["reasons"] == ["superseded_by_run", "production_run_in_flight"]
    assert svc.updates() == [] and seen["provenance"] == []


if __name__ == "__main__":   # pragma: no cover
    sys.exit(pytest.main([__file__, "-q"]))
