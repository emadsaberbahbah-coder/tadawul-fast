"""scripts/sync_watchdog.py v1.0.0 and the cadence change of daily_sync 1.0.13.

WHY (7-10 Nov 2026): GitHub delivered daily_sync's cron 3-9 hours late; a sync
started 15:59 Riyadh on 9 Nov (board dark through the US open) and two ran
back to back on 10 Nov. The dispatcher is the clock now; this watchdog only
covers a dispatcher that stops.
  W1 decide: a queued or running sync, or a recent one, means skip
  W2 decide: an overdue newest run means dispatch; CI events never count
  W3 decide: nothing readable is 'unknown' (never a dispatch)
  W4 main: off reads nothing; observe never posts; on posts the dispatcher's
     request exactly once; an API fault or 'unknown' exits 1 without a post
  W5 knobs: mode and gap clamps
  W6 workflows: daily_sync has no schedule trigger and keeps its dispatch;
     the watchdog maps on / 10 and has actions: write; the outcome audit
     accepts dispatched sync runs
"""
import datetime as dt
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import sync_watchdog as wd  # noqa: E402

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 10, 12, 0, tzinfo=UTC)


def run(rid, event, status, created, conclusion="success"):
    return {"id": rid, "event": event, "status": status, "conclusion": conclusion, "created_at": created}


def test_w1_active_or_recent_means_skip():
    recent = [run(1, "workflow_dispatch", "completed", "2026-11-10T03:22:53Z")]      # 8.6 h ago
    assert wd.decide(recent, NOW, 10.0)[0] == "skip"
    for status in ("queued", "in_progress", "pending", "waiting", "requested"):
        active = [run(2, "workflow_dispatch", status, "2026-11-09T01:00:00Z", None)]
        verdict, why = wd.decide(active, NOW, 10.0)
        assert verdict == "skip" and "run 2" in why
    # an old finished run plus a running one: still skip
    mixed = [run(3, "schedule", "completed", "2026-11-08T00:00:00Z"), run(4, "workflow_dispatch", "in_progress", "2026-11-10T11:50:00Z", None)]
    assert wd.decide(mixed, NOW, 10.0)[0] == "skip"


def test_w2_overdue_means_dispatch_and_ci_runs_do_not_count():
    old = [run(1, "workflow_dispatch", "completed", "2026-11-10T01:00:00Z")]           # 11 h ago
    verdict, why = wd.decide(old, NOW, 10.0)
    assert verdict == "dispatch" and "11.0 h" in why
    # a push / pull_request run of the same workflow is not a sync
    with_ci = old + [run(9, "push", "completed", "2026-11-10T11:59:00Z"), run(8, "pull_request", "in_progress", "2026-11-10T11:58:00Z", None)]
    assert wd.decide(with_ci, NOW, 10.0)[0] == "dispatch"
    # a failed sync still counts as a start: the next slot or a human decides, not a loop
    failed = [run(1, "schedule", "completed", "2026-11-10T09:00:00Z", "failure")]
    assert wd.decide(failed, NOW, 10.0)[0] == "skip"
    # exactly at the limit is not overdue
    edge = [run(1, "workflow_dispatch", "completed", "2026-11-10T02:00:00Z")]
    assert wd.decide(edge, NOW, 10.0)[0] == "skip"


def test_w3_unreadable_is_unknown():
    assert wd.decide([], NOW, 10.0)[0] == "unknown"
    assert wd.decide([run(1, "push", "completed", "2026-11-10T01:00:00Z")], NOW, 10.0)[0] == "unknown"
    assert wd.decide([run(1, "schedule", "completed", "not a time")], NOW, 10.0)[0] == "unknown"
    assert wd.decide([run(1, "schedule", "completed", "2026-11-11T01:00:00Z")], NOW, 10.0)[0] == "unknown"


class Api:
    def __init__(self, runs=None, fail_get=False, fail_post=False):
        self.runs, self.fail_get, self.fail_post, self.calls = runs or [], fail_get, fail_post, []

    def __call__(self, method, path, token, body=None):
        self.calls.append((method, path, body))
        if method == "GET":
            if self.fail_get:
                raise TimeoutError("read timed out")
            ev = path.split("event=")[1].split("&")[0]
            return 200, {"workflow_runs": [r for r in self.runs if r["event"] == ev]}
        if self.fail_post:
            raise OSError("boom")
        return 204, None


@pytest.fixture
def env(monkeypatch):
    for k in ("TFB_SYNC_WATCHDOG", "TFB_SYNC_WATCHDOG_MAX_GAP_H", "TFB_SYNC_WATCHDOG_REF"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setenv("GITHUB_TOKEN", "t")
    return monkeypatch


OLD = [run(1, "workflow_dispatch", "completed", "2026-11-10T01:00:00Z")]
NEW = [run(1, "workflow_dispatch", "completed", "2026-11-10T09:00:00Z")]


def test_w4_main_modes(env, capsys):
    api = Api(OLD)
    assert wd.main([], api=api, now=NOW) == 0 and api.calls == []          # default off
    env.setenv("TFB_SYNC_WATCHDOG", "observe")
    api = Api(OLD)
    assert wd.main([], api=api, now=NOW) == 0
    assert [c[0] for c in api.calls] == ["GET", "GET"] and "would dispatch" in capsys.readouterr().out
    assert [c[1].split("?")[1] for c in api.calls] == ["event=schedule&per_page=10", "event=workflow_dispatch&per_page=10"]
    env.setenv("TFB_SYNC_WATCHDOG", "on")
    api = Api(OLD)
    assert wd.main([], api=api, now=NOW) == 0
    assert [c[0] for c in api.calls] == ["GET", "GET", "POST"]
    assert api.calls[2][1] == "/repos/owner/repo/actions/workflows/daily_sync.yml/dispatches"
    assert api.calls[2][2] == {"ref": "main", "inputs": {"run_mode": "full_sync", "include_portfolio": "true"}}
    api = Api(NEW)
    assert wd.main([], api=api, now=NOW) == 0 and [c[0] for c in api.calls] == ["GET", "GET"]
    # 30 newer CI runs do not hide the sync (review on PR #950): the reads are filtered by event
    flood = NEW + [run(100 + i, "pull_request", "completed", "2026-11-10T11:%02d:00Z" % i) for i in range(30)]
    api = Api(flood)
    assert wd.main([], api=api, now=NOW) == 0 and [c[0] for c in api.calls] == ["GET", "GET"]
    api = Api(OLD + flood[1:])
    assert wd.main([], api=api, now=NOW) == 0 and [c[0] for c in api.calls] == ["GET", "GET", "POST"]
    # faults: never a post, exit 1
    api = Api(OLD, fail_get=True)
    assert wd.main([], api=api, now=NOW) == 1 and [c[0] for c in api.calls] == ["GET"]
    api = Api([])
    assert wd.main([], api=api, now=NOW) == 1 and [c[0] for c in api.calls] == ["GET", "GET"]
    api = Api(OLD, fail_post=True)
    assert wd.main([], api=api, now=NOW) == 1
    env.delenv("GITHUB_TOKEN")
    api = Api(OLD)
    assert wd.main([], api=api, now=NOW) == 1 and api.calls == []


def test_w5_knobs(env):
    for raw, want in (("", "off"), ("on", "on"), ("OBSERVE", "observe"), ("1", "off"), ("yes", "off")):
        env.setenv("TFB_SYNC_WATCHDOG", raw)
        assert wd.mode() == want
    for raw, want in (("", 10.0), ("2", 4.0), ("12", 12.0), ("99", 48.0), ("x", 10.0), ("nan", 10.0)):
        env.setenv("TFB_SYNC_WATCHDOG_MAX_GAP_H", raw)
        assert wd.max_gap_h() == want


def _wf(name):
    doc = yaml.safe_load((ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8"))
    return doc, (doc.get("on") or doc.get(True))


def test_w6_workflows():
    doc, on = _wf("daily_sync.yml")
    assert "schedule" not in on and "workflow_dispatch" in on
    assert on["workflow_dispatch"]["inputs"]["run_mode"]["default"] == "full_sync"
    assert on["workflow_dispatch"]["inputs"]["include_portfolio"]["default"] is True
    text = (ROOT / ".github" / "workflows" / "daily_sync.yml").read_text(encoding="utf-8")
    assert "# VERSION 1.0.13 (2026-11-10) -- THE DISPATCHER IS THE CLOCK" in text
    assert '#     - cron: "17 4,12,20 * * *"' in text                              # the rollback line
    wdoc, won = _wf("sync_watchdog.yml")
    assert won["schedule"] == [{"cron": "37 */2 * * *"}] and "workflow_dispatch" in won
    assert wdoc["permissions"] == {"contents": "read", "actions": "write"}
    step = [s for s in wdoc["jobs"]["sync_watchdog"]["steps"] if "sync_watchdog.py" in (s.get("run") or "")][0]
    assert step["env"]["TFB_SYNC_WATCHDOG"] == "${{ vars.TFB_SYNC_WATCHDOG || 'on' }}"
    assert step["env"]["TFB_SYNC_WATCHDOG_MAX_GAP_H"] == "${{ vars.TFB_SYNC_WATCHDOG_MAX_GAP_H || '10' }}"
    assert step["env"]["GITHUB_TOKEN"] == "${{ github.token }}"
    adoc, _ = _wf("sync_outcome_audit.yml")
    cond = adoc["jobs"]["audit-scheduled-run"]["if"]
    assert "github.event.workflow_run.event == 'schedule' || github.event.workflow_run.event == 'workflow_dispatch'" in cond
    assert wd.__version__ == "1.0.0"
