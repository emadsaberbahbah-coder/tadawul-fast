"""The pages GitHub's schedule leaves behind now follow the hourly Exit Plan (21 Nov 2026, CHAIN).

The workflows are news_board.yml 1.1.0, trial_labels.yml 1.1.0 and pre_open_check.yml v1.2.0. Each also runs after
the Exit Plan workflow, which the workbook dispatches every hour (apps_script/29_Page_Refresh_Dispatch.gs), but only
in the UTC hours of its repository Variable. This is the rule of News & Macro Gate 1.1.0, which 1.1.1 aligns.

Why: since 19 Nov every scheduled News Board run started about 8 hours late. Since 9 Nov every scheduled Pre-Open
Check started after the US close and wrote nothing.

The hour gate is its own job ('due'), and the page job holds the concurrency group (Codex P2 on PR 1043). So a
chained run that is not due never enters the group, and cannot cancel or replace a run that publishes.

What is tested:
  C1  the wiring: the trigger, the crons kept, the two jobs, the concurrency on the page job only
  C2  the gate step, run in bash with a fixed clock
  C3  the pre-open window in both DST regimes, read by the script's own window function
"""
from __future__ import annotations

import datetime as dt
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest
import yaml

from scripts import pre_open_check as pc

ROOT = Path(__file__).resolve().parents[1]
WF = ROOT / ".github" / "workflows"
EXIT_PLAN = "\U0001F3AF Exit Plan"
JOB_IF = "needs.due.outputs.run == 'true'"
# workflow: (page job, repository Variable, default UTC hours, weekdays only, the crons that stay, its concurrency)
CHAINS = {
    "news_board.yml": ("board", "TFB_NEWS_BOARD_CHAIN_HOURS", "4 13", False, ["55 4,13 * * *"],
                       {"group": "tfb-news-board", "cancel-in-progress": False}),
    "trial_labels.yml": ("labels", "TFB_TRIAL_LABELS_CHAIN_HOURS", "5 9 14", False, ["5 5,9,14 * * *"],
                         {"group": "tfb-trial-labels", "cancel-in-progress": False}),
    "pre_open_check.yml": ("pre_open_check", "TFB_PREOPEN_CHAIN_HOURS", "12 13", True, ["0 13,14 * * 1-5"],
                           {"group": "tfb-pre-open-check", "cancel-in-progress": True}),
    "news_macro_gate.yml": ("gate", "TFB_NEWS_GATE_CHAIN_HOURS", "6 9 12 13 16 19", False,
                            ["41 6 * * 0-4", "47 12 * * 1-5", "23 19 * * 1-5"],
                            {"group": "${{ github.workflow }}-${{ github.ref }}", "cancel-in-progress": False}),
}


def _wf(name):
    return yaml.safe_load((WF / name).read_text(encoding="utf-8"))


def _on(wf):
    return wf[True] if True in wf else wf["on"]


# ----------------------------------------------------------------------------- C1 wiring
def test_c1_the_pages_follow_the_exit_plan_keep_their_crons_and_gate_in_their_own_job():
    assert _wf("exit_plan.yml")["name"] == EXIT_PLAN                            # the name the chains wait for
    for name, (job, var, hours, weekdays, crons, conc) in CHAINS.items():
        wf = _wf(name)
        on = _on(wf)
        assert set(on) == {"schedule", "workflow_run", "workflow_dispatch"}, name
        assert on["workflow_run"] == {"workflows": [EXIT_PLAN], "types": ["completed"]}, name
        assert [c["cron"] for c in on["schedule"]] == crons, name                 # the cron stays as the backstop
        assert set(wf["jobs"]) == {"due", job}, name
        due = wf["jobs"]["due"]
        assert due["runs-on"] == "ubuntu-24.04" and due["timeout-minutes"] == 5 and "concurrency" not in due, name
        assert due["outputs"] == {"run": "${{ steps.due.outputs.run }}"} and len(due["steps"]) == 1, name
        gate = due["steps"][0]
        assert gate["id"] == "due" and "if" not in gate and gate["name"].endswith("Is a chained run due this hour?"), name
        assert gate["env"] == {"EVENT_NAME": "${{ github.event_name }}",
                               "CHAIN_HOURS": "${{ vars.%s || '%s' }}" % (var, hours)}, name
        assert ("DOW=$((10#$(date -u +%u)))" in gate["run"]) is weekdays, name
        page = wf["jobs"][job]
        assert page["needs"] == "due" and page["if"] == JOB_IF, name
        # Codex P2 (PR 1043): the group is the page job's, never the workflow's, so a run that is not due stays out of it
        assert "concurrency" not in wf and page["concurrency"] == conc, name
        assert len(page["steps"]) >= 4 and not any("steps.due" in str(s.get("if", "")) for s in page["steps"]), name
        head = (WF / name).read_text(encoding="utf-8").split("\nname:", 1)[0]
        assert var in head and "29_Page_Refresh_Dispatch" in head and "The cron" in head, name
    # a chained run writes like a scheduled one: only a dispatch can ask for a dry run
    for name, script in (("news_board.yml", "news_board.py"), ("trial_labels.yml", "trial_labels.py")):
        step = [s for s in _wf(name)["jobs"][CHAINS[name][0]]["steps"] if script in str(s.get("run", ""))][0]
        assert 'if [[ "$EVENT_NAME" == "workflow_dispatch" && "$IN_DRY_RUN" == "true" ]]; then' in step["run"], name
    pre = [s for s in _wf("pre_open_check.yml")["jobs"]["pre_open_check"]["steps"] if "pre_open_check.py" in str(s.get("run", ""))][0]
    assert 'MODE="--write"' in pre["run"] and 'if [[ "${{ github.event_name }}" == "workflow_dispatch" ]]; then' in pre["run"]
    assert "tests/test_page_chains.py" in (WF / "ci.yml").read_text(encoding="utf-8")


# ----------------------------------------------------------------------------- C2 the gate, run
def _gate(name, event, utc, hours=None):
    """Run the gate step of `name` in bash with `date` fixed at `utc`; return (run, stdout)."""
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash is not installed")
    step = _wf(name)["jobs"]["due"]["steps"][0]
    with tempfile.TemporaryDirectory() as d:
        out = Path(d) / "out"
        stub = ('date() { case "$2" in "+%%H") echo %s ;; "+%%u") echo %d ;; *) command date "$@" ;; esac; }\n'
                % (utc.strftime("%H"), utc.isoweekday()))
        env = {"PATH": os.environ.get("PATH", ""), "GITHUB_OUTPUT": str(out), "EVENT_NAME": event,
               "CHAIN_HOURS": CHAINS[name][2] if hours is None else hours}
        r = subprocess.run([bash, "-e", "-c", stub + step["run"]], env=env, capture_output=True, text=True, timeout=30)
        assert r.returncode == 0, r.stderr
        return out.read_text(encoding="utf-8").strip(), r.stdout


def _at(day, hour, minute=40):
    return dt.datetime(2026, 11, day, hour, minute, tzinfo=dt.timezone.utc)   # 23 Nov 2026 is a Monday


@pytest.mark.parametrize("name,hour,want", [
    ("news_board.yml", 4, True), ("news_board.yml", 13, True), ("news_board.yml", 5, False), ("news_board.yml", 14, False),
    ("trial_labels.yml", 5, True), ("trial_labels.yml", 9, True), ("trial_labels.yml", 14, True),
    ("trial_labels.yml", 4, False), ("trial_labels.yml", 13, False),
    ("pre_open_check.yml", 12, True), ("pre_open_check.yml", 13, True), ("pre_open_check.yml", 14, False),
    ("pre_open_check.yml", 11, False),
    ("news_macro_gate.yml", 6, True), ("news_macro_gate.yml", 19, True), ("news_macro_gate.yml", 7, False)])
def test_c2_a_chained_run_works_only_in_its_hours(name, hour, want):
    run, log = _gate(name, "workflow_run", _at(23, hour))
    assert run == "run=%s" % ("true" if want else "false"), (name, hour, run)
    assert "Chained run at UTC hour %d (hours: %s): run=%s" % (hour, CHAINS[name][2], str(want).lower()) in log


def test_c2_schedule_dispatch_off_and_weekends():
    for name in CHAINS:
        for event in ("schedule", "workflow_dispatch"):                        # never held back by the gate
            assert _gate(name, event, _at(21, 2))[0] == "run=true", (name, event)
            assert _gate(name, event, _at(22, 7), hours="off")[0] == "run=true", (name, event)
        due = int(CHAINS[name][2].split()[0])
        assert _gate(name, "workflow_run", _at(23, due), hours="off")[0] == "run=false", name    # off = cron only
        assert _gate(name, "workflow_run", _at(23, due), hours="%d,99" % due)[0] == "run=false", name   # spaces only
        assert _gate(name, "workflow_run", _at(23, due), hours=" 0%d " % due)[0] == "run=true", name   # 04 reads as 4
        assert _gate(name, "workflow_run", _at(23, 8), hours="08 09")[0] == "run=true", name      # base 10, not octal
        assert _gate(name, "workflow_run", _at(23, 0), hours="off")[0] == "run=false", name       # "off" is not hour 0
    # the pre-open check chains on weekdays only (UTC): Sat 21 / Sun 22 Nov never, Fri 20 / Mon 23 Nov in its hours
    assert _gate("pre_open_check.yml", "workflow_run", _at(21, 13))[0] == "run=false"
    assert _gate("pre_open_check.yml", "workflow_run", _at(22, 12))[0] == "run=false"
    assert _gate("pre_open_check.yml", "workflow_run", _at(20, 13))[0] == "run=true"
    assert _gate("pre_open_check.yml", "workflow_run", _at(23, 12))[0] == "run=true"
    # the other two chain every day
    assert _gate("news_board.yml", "workflow_run", _at(21, 4))[0] == "run=true"
    assert _gate("trial_labels.yml", "workflow_run", _at(22, 9))[0] == "run=true"


# ----------------------------------------------------------------------------- C3 the pre-open window
class _Cal:
    def __init__(self, open_utc):
        self.open_utc = open_utc

    def is_session(self, day):
        return True

    def session_open(self, day):
        return self.open_utc


@pytest.mark.parametrize("open_hm,hour_inside", [((14, 30), 13), ((13, 30), 12)])     # winter (EST), summer (EDT)
def test_c3_one_chain_hour_lands_inside_the_window_before_the_open(open_hm, hour_inside):
    op = dt.datetime(2026, 11, 23, open_hm[0], open_hm[1], tzinfo=dt.timezone.utc)
    cal = _Cal(op)
    hours = [int(h) for h in CHAINS["pre_open_check.yml"][2].split()]
    assert hour_inside in hours
    # the Exit Plan starts at :35 - :45 and runs a few minutes; the chained check reads the clock at :40 - :58
    for minute in (40, 45, 50, 55, 58):
        now = op.replace(hour=hour_inside, minute=minute)
        when, inside, why = pc.preopen_window(now, cal)
        assert inside and now < op and when == op, (now, why)
    # the other chain hour: in winter 12:4x is before the window ("nothing to do"); in summer 13:4x is after the
    # open - inside the window's 15 minutes after it at :40 - :45 (a post-open read), outside it from :50
    other = [h for h in hours if h != hour_inside][0]
    reads = [pc.preopen_window(op.replace(hour=other, minute=m), cal)[1] for m in (40, 45, 50, 55, 58)]
    assert reads == ([False] * 5 if other < hour_inside else [True, True, False, False, False]), reads
    assert pc.WINDOW_BEFORE_MIN == 75 and pc.WINDOW_AFTER_MIN == 15
