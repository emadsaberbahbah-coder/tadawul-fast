"""D-L1 (2026-10-17): the scheduled maturity audit lane of track_performance.yml
v1.1.0 and the mirror guard in daily_sync.yml's tracker step.

What is pinned
--------------
* The audit cron "47 20 * * *" appears exactly once in `on.schedule`, as the
  🚦 step's AUDIT_LANE_CRON and in the job timeout expression; the two record
  crons of v1.0.2 are untouched.
* The 🚦 step's real shell, run with a stubbed `curl` (real `jq`): on the audit
  cron it WAITS while Daily Sync is queued/running, grants `audit=true` only on
  a zero reading, and fails CLOSED (skip, no grant) on an unreadable state or
  an expired deadline. On the record crons the v1.0.2 behaviour is byte-for-
  byte: skip on in-flight, proceed (fail-open) on an unreadable state.
* The run step's real shell: the granted schedule audits WITHOUT --record and
  exports TRACK_AUDIT=1 / TRACK_PUBLISH_CALIBRATION=1 over an inherited 0; the
  14:47 / 18:47 slots still export the record-only 0s (golden negative).
* daily_sync.yml's tracker shell: with the Actions API reachable it skips its
  record while a Performance Clock run is in progress, records on zero, and
  records (fail-open) on an unreadable state; offline (no GITHUB_API_URL) it is
  unchanged.

Offline: the stub `curl` answers from a response file; no network, no Sheets.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import subprocess

import pytest

from tests.test_workflow_activation_observe import step, workflow
from tests.test_workflow_activation_observe import wrapper_repo as wrapper_repo  # noqa: PLC0414 (pytest fixture)

ROOT = Path(__file__).resolve().parents[1]
AUDIT_CRON = "47 20 * * *"
RECORD_CRONS = ("47 11 * * *", "47 15 * * *")


def _runs(*statuses):
    return json.dumps({"workflow_runs": [{"status": s} for s in statuses]})


def _curl_stub(tmp_path, responses):
    """A `curl` that pops one line per call from a response file.
    A line of FAIL exits 7 with no output (the real `|| echo ERR` path)."""
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    responses_file = tmp_path / "curl_responses.txt"
    responses_file.write_text("\n".join(responses) + "\n")
    calls = tmp_path / "curl_calls.txt"
    calls.write_text("")
    script = bindir / "curl"
    script.write_text(f'''#!/usr/bin/env bash
# record the URL (last argument) and pop the first response line
printf '%s\\n' "${{@: -1}}" >> "{calls}"
line="$(head -n 1 "{responses_file}")"
tail -n +2 "{responses_file}" > "{responses_file}.tmp" && mv "{responses_file}.tmp" "{responses_file}"
if [[ "$line" == "FAIL" ]]; then exit 7; fi
printf '%s\\n' "$line"
''')
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return bindir, calls


def _outputs(path):
    return dict(line.split("=", 1) for line in path.read_text().splitlines() if "=" in line)


def _overlap_env(tmp_path, bindir, event, schedule, **extra):
    item = step("track_performance.yml", "Skip if a production write is in flight")
    env = {k: v for k, v in item["env"].items() if not v.startswith("${{")}
    env.update({
        "PATH": str(bindir) + os.pathsep + os.environ["PATH"],
        "GH_TOKEN": "NEVER_EXPORT_TOKEN", "GITHUB_API_URL": "https://api.invalid",
        "GITHUB_REPOSITORY": "owner/repo", "GITHUB_EVENT_NAME": event, "EVENT_SCHEDULE": schedule,
        "GITHUB_OUTPUT": str(tmp_path / "outputs"), "AUDIT_POLL_SECONDS": "0",
    })
    env.update(extra)
    (tmp_path / "outputs").write_text("")
    return item["run"], env


# ---------------------------------------------------------------------------
# Static pins
# ---------------------------------------------------------------------------
def test_audit_cron_pinned_in_schedule_guard_env_and_job_timeout():
    clock = workflow("track_performance.yml")
    crons = [entry["cron"] for entry in clock["on"]["schedule"]]
    assert crons == [*RECORD_CRONS, AUDIT_CRON]
    guard = step("track_performance.yml", "Skip if a production write is in flight")
    assert guard["env"]["AUDIT_LANE_CRON"] == AUDIT_CRON
    assert guard["env"]["EVENT_SCHEDULE"] == "${{ github.event.schedule }}"
    assert guard["env"]["GH_TOKEN"] == "${{ github.token }}"
    assert int(guard["env"]["AUDIT_WAIT_MINUTES"]) >= 60
    job = clock["jobs"]["performance-clock"]
    assert job["timeout-minutes"] == "${{ github.event.schedule == '%s' && 180 || 45 }}" % AUDIT_CRON
    # the lane is granted by the guard and read by the run step; nothing else
    run = step("track_performance.yml", "Run performance clock")
    assert run["env"]["AUDIT_LANE"] == "${{ steps.overlap.outputs.audit }}"
    assert run["if"] == "steps.overlap.outputs.skip != 'true'"
    # the private, non-cancelling concurrency group of 2026-10-06 is unchanged
    assert clock["concurrency"] == {"group": "tfb-performance-clock", "cancel-in-progress": "false"}


def test_header_carries_version_rollback_and_the_no_record_reason():
    text = (ROOT / ".github/workflows/track_performance.yml").read_text(encoding="utf-8")
    assert "# VERSION 1.1.0 (2026-10-17)" in text
    assert 'ROLLBACK: delete the "47 20 * * *" cron line' in text
    assert "does NOT --record" in text
    assert text.count('"47 20 * * *"') == 5, text.count('"47 20 * * *"')  # header, cron, timeout, guard env, lane comment


def test_daily_sync_tracker_step_maps_the_token_for_the_mirror_guard():
    daily = workflow("daily_sync.yml")
    assert daily["permissions"]["actions"] == "read"
    item = step("daily_sync.yml", "Track Performance (record + audit)")
    assert item["env"]["GH_TOKEN"] == "${{ github.token }}"
    assert "actions/workflows/track_performance.yml/runs?status=in_progress" in item["run"]
    # the record-only contract text of #759 is still the fallback branch
    assert "export TRACK_AUDIT=0" in item["run"] and "export TRACK_PUBLISH_CALIBRATION=0" in item["run"]


# ---------------------------------------------------------------------------
# 🚦 step: audit lane waits, grants on zero, fails closed
# ---------------------------------------------------------------------------
def test_audit_lane_waits_for_daily_sync_then_grants(tmp_path):
    bindir, calls = _curl_stub(tmp_path, [
        _runs("in_progress", "completed"), _runs("queued"), _runs("completed", "completed")])
    block, env = _overlap_env(tmp_path, bindir, "schedule", AUDIT_CRON)
    run = subprocess.run(["bash", "-c", block], cwd=tmp_path, env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stdout + run.stderr
    assert _outputs(tmp_path / "outputs") == {"skip": "false", "audit": "true"}
    assert len(calls.read_text().splitlines()) == 3
    assert run.stdout.count("audit lane waiting") == 2
    assert "audit lane granted" in run.stdout
    assert "NEVER_EXPORT_TOKEN" not in run.stdout + run.stderr


@pytest.mark.parametrize("statuses", (("in_progress",), ("queued",), ("waiting",), ("pending",)))
def test_audit_lane_treats_every_pending_state_as_in_flight(tmp_path, statuses):
    bindir, calls = _curl_stub(tmp_path, [_runs(*statuses), _runs()])
    block, env = _overlap_env(tmp_path, bindir, "schedule", AUDIT_CRON)
    run = subprocess.run(["bash", "-c", block], cwd=tmp_path, env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stdout + run.stderr
    assert _outputs(tmp_path / "outputs") == {"skip": "false", "audit": "true"}
    assert len(calls.read_text().splitlines()) == 2


def test_audit_lane_fails_closed_when_sync_state_is_unreadable(tmp_path):
    bindir, calls = _curl_stub(tmp_path, ["FAIL", _runs()])
    block, env = _overlap_env(tmp_path, bindir, "schedule", AUDIT_CRON)
    run = subprocess.run(["bash", "-c", block], cwd=tmp_path, env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stdout + run.stderr
    assert _outputs(tmp_path / "outputs") == {"skip": "true", "audit": "false"}
    assert len(calls.read_text().splitlines()) == 1  # no retry on a blind read
    assert "::warning::D-L1 audit lane: could not read Daily Sync state" in run.stdout
    assert "fail-closed" in run.stdout


def test_audit_lane_fails_closed_when_the_wait_deadline_passes(tmp_path):
    bindir, calls = _curl_stub(tmp_path, [_runs("in_progress"), _runs("in_progress"), _runs()])
    block, env = _overlap_env(tmp_path, bindir, "schedule", AUDIT_CRON, AUDIT_WAIT_MINUTES="0")
    run = subprocess.run(["bash", "-c", block], cwd=tmp_path, env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stdout + run.stderr
    assert _outputs(tmp_path / "outputs") == {"skip": "true", "audit": "false"}
    assert len(calls.read_text().splitlines()) == 1
    assert "still queued or in progress after 0 min" in run.stdout


@pytest.mark.parametrize("schedule", RECORD_CRONS)
def test_record_crons_keep_the_fail_open_skip_semantics(tmp_path, schedule):
    # unreadable -> proceed (fail-open), never a grant
    bindir, calls = _curl_stub(tmp_path, ["FAIL"])
    block, env = _overlap_env(tmp_path, bindir, "schedule", schedule)
    run = subprocess.run(["bash", "-c", block], cwd=tmp_path, env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stdout + run.stderr
    assert _outputs(tmp_path / "outputs") == {"audit": "false", "skip": "false"}
    assert "proceeding (fail-open)" in run.stdout
    # in flight -> skip, never a grant, exactly one read (no waiting)
    bindir, calls = _curl_stub(tmp_path, [_runs("in_progress"), _runs()])
    block, env = _overlap_env(tmp_path, bindir, "schedule", schedule)
    run = subprocess.run(["bash", "-c", block], cwd=tmp_path, env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stdout + run.stderr
    assert _outputs(tmp_path / "outputs") == {"audit": "false", "skip": "true"}
    assert len(calls.read_text().splitlines()) == 1
    # clear -> proceed, never a grant
    bindir, calls = _curl_stub(tmp_path, [_runs("completed")])
    block, env = _overlap_env(tmp_path, bindir, "schedule", schedule)
    run = subprocess.run(["bash", "-c", block], cwd=tmp_path, env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stdout + run.stderr
    assert _outputs(tmp_path / "outputs") == {"audit": "false", "skip": "false"}


@pytest.mark.parametrize("event,schedule", (
    ("workflow_dispatch", ""), ("workflow_dispatch", AUDIT_CRON), ("push", AUDIT_CRON), ("", AUDIT_CRON)))
def test_only_a_schedule_event_on_the_audit_cron_can_be_granted(tmp_path, event, schedule):
    bindir, _calls = _curl_stub(tmp_path, [_runs()])
    block, env = _overlap_env(tmp_path, bindir, event, schedule)
    run = subprocess.run(["bash", "-c", block], cwd=tmp_path, env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stdout + run.stderr
    assert _outputs(tmp_path / "outputs") == {"audit": "false", "skip": "false"}


# ---------------------------------------------------------------------------
# Run step: the granted lane audits without --record, explicit 1s
# ---------------------------------------------------------------------------
def _run_clock(root, env, **overrides):
    item = step("track_performance.yml", "Run performance clock")
    block = item["run"].replace("${{ github.event.inputs.enable_debug }}", "false")
    env = env | {"INPUT_SIGNAL_HISTORY_MODE": "observe", "TRACK_DEDUP_SIGNAL_HISTORY": "observe",
                 "INPUT_RUN_AUDIT": "", "GITHUB_ENV": str(root / "github_env"),
                 # inherited 0s: the lane must OVERRIDE them, not rely on defaults
                 "TRACK_AUDIT": "0", "TRACK_PUBLISH_CALIBRATION": "0", "TFB_SIGNAL_TRENDS_TAB": "1",
                 "TFB_TRACK_SUMMARY_SIDE": "0"}
    env.update(overrides)
    env.pop("GITHUB_API_URL", None)
    (root / "github_env").write_text("")
    run = subprocess.run(["bash", "-c", block], cwd=root, env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stdout + run.stderr
    actual = json.loads((root / "actual_tracker_call.json").read_text())
    receipt = json.loads((root / "tracker_activation_receipt.json").read_text())
    return actual, receipt, _outputs(root / "github_env"), run.stdout


def test_granted_schedule_audits_without_record_and_exports_explicit_ones(wrapper_repo):
    root, env = wrapper_repo
    actual, receipt, github_env, stdout = _run_clock(root, env, GITHUB_EVENT_NAME="schedule", AUDIT_LANE="true")
    assert actual["args"] == ["--sheet-id", "offline-sheet", "--audit", "--analyze", "--export"]
    assert actual["audit_env"] == actual["calibration_publish"] == "1"
    assert actual["trends"] == "1" and actual["summary_side"] == "0"
    assert receipt["audit_enabled"] is receipt["historical_grid_save_requested"] is True
    assert receipt["execution_evidence_complete"] is True
    assert receipt["signal_snapshot_append_acknowledged"] == 2  # stub log line; the real lane records nothing
    assert github_env == {"TRACKER_LANE": "scheduled-audit"}
    assert "🧭 lane=scheduled-audit" in stdout


@pytest.mark.parametrize("grant", ("", "false", "TRUE", "1"))
def test_record_slots_still_write_no_calibration_row(wrapper_repo, grant):
    """Golden negative for the 14:47 / 18:47 slots: the #759 contract holds."""
    root, env = wrapper_repo
    actual, receipt, github_env, stdout = _run_clock(root, env, GITHUB_EVENT_NAME="schedule", AUDIT_LANE=grant)
    assert actual["args"] == ["--sheet-id", "offline-sheet", "--record", "--analyze", "--export"]
    assert actual["audit_env"] == actual["calibration_publish"] == actual["trends"] == "0"
    assert actual["summary_side"] == "1"
    assert receipt["audit_enabled"] is receipt["historical_grid_save_requested"] is False
    assert github_env == {"TRACKER_LANE": "record-only"}
    assert "🧭 lane=record-only" in stdout


def test_manual_dispatch_grant_is_ignored_and_manual_audit_still_records(wrapper_repo):
    root, env = wrapper_repo
    actual, *_ = _run_clock(root, env, GITHUB_EVENT_NAME="workflow_dispatch", AUDIT_LANE="true")
    assert actual["args"] == ["--sheet-id", "offline-sheet", "--record", "--analyze", "--export"]
    assert actual["calibration_publish"] == "0"
    actual, receipt, github_env, _ = _run_clock(root, env, GITHUB_EVENT_NAME="workflow_dispatch",
                                                INPUT_RUN_AUDIT="true", TRACK_AUDIT="1", TRACK_PUBLISH_CALIBRATION="1")
    assert actual["args"] == ["--sheet-id", "offline-sheet", "--record", "--audit", "--analyze", "--export"]
    assert receipt["audit_enabled"] is True
    assert github_env == {"TRACKER_LANE": "manual-audit"}


# ---------------------------------------------------------------------------
# daily_sync tracker step: mirror guard
# ---------------------------------------------------------------------------
def _run_daily(root, env, bindir=None, **overrides):
    item = step("daily_sync.yml", "Track Performance (record + audit)")
    block = item["run"].replace("/tmp/track_performance.log", str(root / "daily_tracker.log"))
    env = env | {"GITHUB_EVENT_NAME": "schedule", "INPUT_RUN_TRACKER": "", "GOOGLE_APPLICATION_CREDENTIALS": "",
                 "GITHUB_REPOSITORY": "owner/repo", "TRACK_DEDUP_SIGNAL_HISTORY": "observe"}
    env.pop("GITHUB_API_URL", None)
    if bindir is not None:
        env["PATH"] = str(bindir) + os.pathsep + env["PATH"]
    env.update(overrides)
    for stale in ("actual_tracker_call.json",):
        (root / stale).unlink(missing_ok=True)
    run = subprocess.run(["bash", "-c", block], cwd=root, env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stdout + run.stderr
    return (root / "actual_tracker_call.json").exists(), run.stdout


def test_daily_tracker_skips_its_record_while_a_clock_run_is_in_progress(wrapper_repo, tmp_path):
    root, env = wrapper_repo
    bindir, calls = _curl_stub(tmp_path, [_runs("in_progress")])
    recorded, stdout = _run_daily(root, env, bindir, GITHUB_API_URL="https://api.invalid", GH_TOKEN="NEVER_EXPORT_TOKEN")
    assert recorded is False
    assert "D-L1 mirror guard" in stdout and "::notice::1 Performance Clock run(s) in progress" in stdout
    assert "track_performance.yml/runs?status=in_progress" in calls.read_text()
    assert "NEVER_EXPORT_TOKEN" not in stdout


@pytest.mark.parametrize("response,expect_warning", ((_runs(), False), ("FAIL", True), ("not json", True)))
def test_daily_tracker_records_when_no_clock_runs_or_state_is_unreadable(wrapper_repo, tmp_path, response, expect_warning):
    root, env = wrapper_repo
    bindir, _calls = _curl_stub(tmp_path, [response])
    recorded, stdout = _run_daily(root, env, bindir, GITHUB_API_URL="https://api.invalid", GH_TOKEN="x")
    assert recorded is True
    assert ("recording as before (fail-open)" in stdout) is expect_warning
    actual = json.loads((root / "actual_tracker_call.json").read_text())
    assert "--record" in actual["args"] and "--audit" not in actual["args"]
    assert actual["calibration_publish"] == "0"


def test_daily_tracker_offline_path_is_unchanged_without_an_api_url(wrapper_repo, tmp_path):
    root, env = wrapper_repo
    bindir, calls = _curl_stub(tmp_path, [_runs("in_progress")])
    recorded, stdout = _run_daily(root, env, bindir, GH_TOKEN="x")  # no GITHUB_API_URL
    assert recorded is True
    assert calls.read_text() == ""  # the guard never called curl
    assert "mirror guard" not in stdout
