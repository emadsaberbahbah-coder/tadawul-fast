"""P-170b: the yml crons must not duplicate the Apps Script dispatcher's slots.

The dispatcher defaults are read from apps_script/24_Sync_Dispatch.gs (Riyadh,
UTC+3), so a change on either side that re-creates a double run fails here.
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WF = ROOT / ".github/workflows"
GAS = (ROOT / "apps_script/24_Sync_Dispatch.gs").read_text(encoding="utf-8")


def workflow(name):
    data = yaml.safe_load((WF / name).read_text(encoding="utf-8"))
    data["on"] = data.pop(True, data.get("on"))
    return data


def cron_hours(name):
    sched = workflow(name)["on"].get("schedule") or []
    hours = set()
    for entry in sched:
        _minute, hour = entry["cron"].split()[:2]
        hours.update(int(h) for h in hour.split(","))
    return hours


def utc_hours(riyadh_hours):
    return {(int(h) - 3) % 24 for h in riyadh_hours.split(",") if h.strip()}


def sync_lane_utc_hours():
    return utc_hours(re.search(r"DEFAULT_HOURS:\s*'([\d,]+)'", GAS).group(1))


def s1_lane_utc_hours(lane):
    block = re.search(lane + r":\s*\{(.*?)\n    \}", GAS, re.S).group(1)
    return utc_hours(re.search(r"defaultHours:\s*'([\d,]+)'", block).group(1))


def test_daily_sync_cron_leaves_the_dispatched_morning_slot_to_the_dispatcher():
    dispatched = sync_lane_utc_hours()
    assert dispatched == {3}
    crons = cron_hours("daily_sync.yml")
    assert crons == {12, 20}
    # No cron within an hour of the dispatched run (the removed 04:17Z slot).
    assert not {(h + d) % 24 for h in dispatched for d in (-1, 0, 1)} & crons


def test_shadow_board_has_no_schedule_only_dispatch():
    assert s1_lane_utc_hours("shadow_board") == {5, 14}
    on = workflow("shadow_board.yml")["on"]
    assert list(on) == ["workflow_dispatch"]
    assert on["workflow_dispatch"]["inputs"]["dry_run"]["options"] == ["true", "false"]


def test_shadow_scorer_cron_and_dispatch_share_one_non_cancelling_lease():
    assert s1_lane_utc_hours("shadow_scorer") == {15}
    assert cron_hours("shadow_scorer.yml") == {15}
    wf = workflow("shadow_scorer.yml")
    assert wf["concurrency"] == {"group": "tfb-shadow-scorer", "cancel-in-progress": False}
    assert all("concurrency" not in job for job in wf["jobs"].values())
