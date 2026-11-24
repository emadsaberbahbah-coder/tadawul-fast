"""scripts/collector_catchup.py v1.0.0 - start a nightly data collector that missed its slot.

WHY (24 Nov 2026): GitHub delivered the BigQuery collectors' crons hours late or not at all; the archive, SPY and
one news slot were missing at the morning check. The catch-up guard dispatches ONE write run per missed slot.
  C1 due_slot: the latest slot past its grace, weekdays honoured, lookback bounded
  C2 decide: active -> skip; success since the slot -> ok; nothing since -> dispatch
  C3 decide: one failure -> one retry; two runs not succeeding -> stuck (never a loop); old runs never count
  C4 decide: only production events count (a CI or workflow_run run is not a collection)
  C5 main: off reads nothing; observe never posts; on posts one write dispatch per missed collector with its inputs
  C6 main: an unreadable list skips that collector, still checks the rest, exits 1
  C7 the workflow: observe by default, actions: write, runs after Pre-Open Check and on its own cron
  C8 every collector it dispatches exists and accepts the inputs it sends
  C9 the 24 Nov morning, replayed from the run list read that day
"""
import datetime as dt
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import collector_catchup as cc  # noqa: E402

UTC = dt.timezone.utc
SPEC = {c["file"]: c for c in cc.COLLECTORS}


def run(rid, event, status, created, conclusion="success", title=None):
    if title is None:
        title = "wf (dispatch, write=true)" if event == "workflow_dispatch" else "wf"
    return {"id": rid, "event": event, "status": status, "conclusion": conclusion, "created_at": created,
            "display_title": title}


def at(s):
    return dt.datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=UTC)


# ---------------------------------------------------------------- C1
def test_c1_due_slot():
    arc = SPEC["bq_archive.yml"]                     # 23:05 slot, 5 h grace
    assert cc.due_slot(arc, at("2026-11-24 03:00")) == at("2026-11-22 23:05")   # the 23 Nov slot is still in grace
    assert cc.due_slot(arc, at("2026-11-24 04:05")) == at("2026-11-23 23:05")
    opt = SPEC["bq_options.yml"]                     # Mon-Fri 23:30, 4 h grace
    assert cc.due_slot(opt, at("2026-11-22 12:00")) is None                     # Sunday: Saturday has no slot, Friday's is > 32 h old
    assert cc.due_slot(opt, at("2026-11-24 04:00")) == at("2026-11-23 23:30")   # Monday night
    news = SPEC["news_archive.yml"]                  # 14:00 and 20:00, 3 h grace
    assert cc.due_slot(news, at("2026-11-23 18:00")) == at("2026-11-23 14:00")
    assert cc.due_slot(news, at("2026-11-23 23:30")) == at("2026-11-23 20:00")


# ---------------------------------------------------------------- C2
def test_c2_active_ok_dispatch():
    arc, now = SPEC["bq_archive.yml"], at("2026-11-24 06:10")
    assert cc.decide(arc, [run(1, "schedule", "in_progress", "2026-11-24T06:00:00Z", None)], now)[0] == "skip"
    assert cc.decide(arc, [run(2, "schedule", "completed", "2026-11-24T03:40:00Z")], now)[0] == "ok"
    assert cc.decide(arc, [run(3, "schedule", "completed", "2026-11-23T03:40:00Z")], now)[0] == "dispatch"
    assert cc.decide(arc, [], now)[0] == "dispatch"
    assert cc.decide(arc, [], at("2026-11-24 03:00"))[0] == "dispatch"           # the 22 Nov slot, nothing since
    assert cc.decide(arc, [run(4, "schedule", "completed", "2026-11-23T03:40:00Z")], at("2026-11-24 03:00"))[0] == "ok"


# ---------------------------------------------------------------- C3
def test_c3_one_retry_then_stuck():
    mk, now = SPEC["bq_markets.yml"], at("2026-11-24 06:00")                     # slot 23 Nov 22:35
    one_fail = [run(1, "schedule", "completed", "2026-11-24T01:00:00Z", "failure")]
    v, why = cc.decide(mk, one_fail, now)
    assert v == "dispatch" and "one retry" in why
    two_fail = one_fail + [run(2, "workflow_dispatch", "completed", "2026-11-24T02:00:00Z", "failure")]
    assert cc.decide(mk, two_fail, now)[0] == "stuck"
    fail_then_ok = one_fail + [run(3, "workflow_dispatch", "completed", "2026-11-24T02:00:00Z")]
    assert cc.decide(mk, fail_then_ok, now)[0] == "ok"
    before_slot_only = [run(4, "schedule", "completed", "2026-11-23T20:00:00Z")]
    assert cc.decide(mk, before_slot_only, now)[0] == "dispatch"


# ---------------------------------------------------------------- C4
def test_c4_only_production_events():
    arc, now = SPEC["bq_archive.yml"], at("2026-11-24 06:10")
    others = [run(1, "workflow_run", "completed", "2026-11-24T05:31:00Z", "skipped"),
              run(2, "push", "completed", "2026-11-24T05:00:00Z"),
              run(3, "workflow_run", "in_progress", "2026-11-24T06:00:00Z", None)]
    assert cc.decide(arc, others, now)[0] == "dispatch"


def test_c4b_a_manual_dry_run_is_not_a_collection():
    """Codex P2 on #1070: a default manual dispatch of a BigQuery collector is a dry run."""
    mk, now = SPEC["bq_markets.yml"], at("2026-11-24 06:00")
    dry = [run(1, "workflow_dispatch", "completed", "2026-11-24T01:00:00Z", title="🌍 BigQuery Market Series (dispatch, write=false)")]
    assert cc.decide(mk, dry, now)[0] == "dispatch"
    wet = [run(2, "workflow_dispatch", "completed", "2026-11-24T01:00:00Z", title="🌍 BigQuery Market Series (dispatch, write=true)")]
    assert cc.decide(mk, wet, now)[0] == "ok"
    active_dry = [run(3, "workflow_dispatch", "in_progress", "2026-11-24T05:59:00Z", None, title="x (dispatch, write=false)")]
    assert cc.decide(mk, active_dry, now)[0] == "dispatch"                       # a dry run in flight is not coming data
    news = SPEC["news_archive.yml"]
    assert cc.decide(news, [run(4, "workflow_dispatch", "completed", "2026-11-24T04:00:00Z", title="news-archive (dispatch, dry_run=true)")], now)[0] == "dispatch"
    assert cc.decide(news, [run(5, "workflow_dispatch", "completed", "2026-11-24T04:00:00Z", title="news-archive (dispatch, dry_run=false)")], now)[0] == "ok"


def test_c4c_each_collector_names_its_write_mode():
    for spec in cc.COLLECTORS:
        wf = yaml.safe_load((ROOT / ".github" / "workflows" / spec["file"]).read_text(encoding="utf-8"))
        rn = wf.get("run-name") or ""
        key, val = spec["write_mark"].split("=")
        assert "github.event_name == 'workflow_dispatch'" in rn and "inputs.%s" % key in rn, spec["file"]
        assert "(dispatch, %s={1})" % key in rn and rn.endswith("|| github.workflow }}"), spec["file"]
        assert spec["inputs"] == {key: val}                                         # the guard's own dispatch carries the mark


# ---------------------------------------------------------------- C5 / C6
class FakeAPI:
    def __init__(self, runs_by_file, fail_files=()):
        self.runs_by_file, self.fail_files, self.posts, self.gets = runs_by_file, set(fail_files), [], []

    def __call__(self, method, path, token, body=None):
        if method == "GET":
            self.gets.append(path)
            wf = path.split("/workflows/")[1].split("/")[0]
            if wf in self.fail_files:
                raise OSError("boom")
            ev = path.split("event=")[1].split("&")[0]
            return 200, {"workflow_runs": [r for r in self.runs_by_file.get(wf, []) if r["event"] == ev]}
        self.posts.append((path, body))
        return 204, None


@pytest.fixture
def env(monkeypatch, tmp_path):
    monkeypatch.setenv("GITHUB_REPOSITORY", "o/r")
    monkeypatch.setenv("GITHUB_TOKEN", "t")
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "summary.md"))
    return tmp_path


FRESH = {f: [run(9, "schedule", "completed", "2026-11-24T06:05:00Z")] for f in SPEC}
NOW = at("2026-11-24 06:10")


def test_c5_modes(env, monkeypatch, capsys):
    stale = dict(FRESH, **{"bq_archive.yml": [run(1, "schedule", "completed", "2026-11-23T03:40:00Z")]})
    monkeypatch.setenv("TFB_COLLECTOR_CATCHUP", "off")
    api = FakeAPI(stale)
    assert cc.main(api=api, now=NOW) == 0 and not api.gets and not api.posts
    monkeypatch.setenv("TFB_COLLECTOR_CATCHUP", "observe")
    api = FakeAPI(stale)
    assert cc.main(api=api, now=NOW) == 0 and api.gets and not api.posts
    assert "bq_archive.yml: would dispatch" in capsys.readouterr().out
    monkeypatch.setenv("TFB_COLLECTOR_CATCHUP", "on")
    api = FakeAPI(stale)
    assert cc.main(api=api, now=NOW) == 0
    assert api.posts == [("/repos/o/r/actions/workflows/bq_archive.yml/dispatches",
                          {"ref": "main", "inputs": {"write": "true"}})]
    assert "| bq_archive.yml | dispatched |" in (env / "summary.md").read_text()
    for junk in ("", "yes", "ON "):
        monkeypatch.setenv("TFB_COLLECTOR_CATCHUP", junk)
        assert cc.mode() == ("on" if junk == "ON " else "off")


def test_c6_unreadable_list(env, monkeypatch, capsys):
    monkeypatch.setenv("TFB_COLLECTOR_CATCHUP", "on")
    stale = dict(FRESH, **{"bq_markets.yml": []})
    api = FakeAPI(stale, fail_files={"bq_archive.yml"})
    assert cc.main(api=api, now=NOW) == 1
    assert [p for p, _ in api.posts] == ["/repos/o/r/actions/workflows/bq_markets.yml/dispatches"]
    assert "bq_archive.yml: ERROR cannot list runs" in capsys.readouterr().out
    monkeypatch.delenv("GITHUB_TOKEN")
    assert cc.main(api=FakeAPI(FRESH), now=NOW) == 1


# ---------------------------------------------------------------- C7 / C8
def test_c7_workflow():
    wf = yaml.safe_load((ROOT / ".github" / "workflows" / "collector_catchup.yml").read_text(encoding="utf-8"))
    on = wf.get(True) or wf.get("on")
    assert on["workflow_run"]["workflows"] == ["⏰ Pre-Open Check"] and on["schedule"][0]["cron"] == "11 */2 * * *"
    assert "workflow_dispatch" in on and wf["permissions"] == {"contents": "read", "actions": "write"}
    step = wf["jobs"]["catchup"]["steps"][-1]
    assert step["env"]["TFB_COLLECTOR_CATCHUP"] == "${{ vars.TFB_COLLECTOR_CATCHUP || 'observe' }}"
    assert "python scripts/collector_catchup.py" in step["run"]
    names = {yaml.safe_load(p.read_text(encoding="utf-8")).get("name") for p in (ROOT / ".github" / "workflows").glob("*.yml")}
    assert "⏰ Pre-Open Check" in names


def test_c8_collectors_accept_the_inputs():
    for spec in cc.COLLECTORS:
        wf = yaml.safe_load((ROOT / ".github" / "workflows" / spec["file"]).read_text(encoding="utf-8"))
        on = wf.get(True) or wf.get("on")
        inputs = (on.get("workflow_dispatch") or {}).get("inputs") or {}
        assert "workflow_dispatch" in on, spec["file"]
        assert set(spec["inputs"]) <= set(inputs), spec["file"]
        for k, v in spec["inputs"].items():                                        # the input is a boolean the run-name prints
            assert inputs[k]["type"] == "boolean" and v in ("true", "false"), spec["file"]
        hours = set()
        for c in on.get("schedule") or []:
            mm, hh = c["cron"].split()[:2]
            hours |= {(int(h), int(mm)) for h in hh.split(",")}
        assert set(spec["slots"]) == hours, (spec["file"], hours)            # the slots are the collector's own cron


# ---------------------------------------------------------------- C9
def test_c9_24_nov_morning_replayed():
    """The run lists read at 06:10 UTC on 24 Nov: archive, markets and options missed; today's deep run is still in
    its grace (06:20), and its 23 Nov run covers the slot before."""
    runs = {
        "bq_markets.yml": [run(1, "schedule", "completed", "2026-11-23T03:03:00Z"),
                           run(2, "workflow_dispatch", "completed", "2026-11-22T20:35:00Z")],
        "bq_archive.yml": [run(3, "schedule", "completed", "2026-11-23T03:40:21Z")],
        "bq_options.yml": [run(4, "workflow_dispatch", "completed", "2026-11-22T21:33:00Z")],
        "bq_symbol_deep.yml": [run(5, "schedule", "completed", "2026-11-23T07:49:00Z")],
        "news_archive.yml": [run(6, "schedule", "completed", "2026-11-24T01:17:00Z")],
    }
    got = {f: cc.decide(SPEC[f], runs[f], NOW)[0] for f in runs}
    assert got == {"bq_markets.yml": "dispatch", "bq_archive.yml": "dispatch", "bq_options.yml": "dispatch",
                   "bq_symbol_deep.yml": "ok", "news_archive.yml": "ok"}
    # at 04:00 UTC the archive and deep data are still in grace; the market series and options are already late
    early = {f: cc.decide(SPEC[f], runs[f], at("2026-11-24 04:00"))[0] for f in runs}
    assert early["bq_symbol_deep.yml"] == "ok" and early["bq_archive.yml"] == "ok" and early["bq_markets.yml"] == "dispatch"
