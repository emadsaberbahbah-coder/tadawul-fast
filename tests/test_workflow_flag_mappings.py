"""Keep operator controls reachable in every workflow writer lane.

WHY (2026-10-11, WP-02/WP-05): the 2026-10-10 export still contained 27
duplicate matured keys, all-zero unavailable risk columns, a vacuous S-1
corporate-actions PASS and 198 repeatedly fetched unavailable symbols. Their
existing guards were unreachable from Actions Variables. Mapping one writer
alone lets the next writer restore the old behavior. Parse the actual YAML,
check default-off mappings and compare every shared effective env value.
No credentials, provider requests or Google Sheet writes are involved.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml


WORKFLOWS = Path(__file__).resolve().parents[1] / ".github" / "workflows"
TRACKER_DEFAULTS = {"TRACK_DEDUP_MATURED": "off", "TRACK_RISK_BLANK": "0",
                    "TRACK_CANONICAL_TARGETS": "0", "TRACK_SIGNAL_PUBLICATION_MODE": "off"}
SYNC_DEFAULTS = {
    "TFB_SYNC_RETIRE_TAB": "off",
    "TFB_SYNC_RETIRE_TAB_NAME": "_Retired_Symbols",
    "TFB_SYNC_RETIRE_MAX_PCT": "20",
    "TFB_SYNC_KLG_CRYPTO_NAME_GATE": "off",
    "TFB_OPP_FX_USX_SUBUNIT": "0",
    "TFB_SYNC_FW2_SCOPE": "off",  # v6.71.0 FW-2 SCOPE (2026-10-14)
}
SYNC_JOBS = ("sync-dashboard", "recover-missing-market-pages")


def _workflow(name):
    return yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))


def _tracker_step(workflow, job_name):
    steps = [
        step for step in workflow["jobs"][job_name]["steps"]
        if "track_performance.py" in step.get("run", "")
    ]
    assert len(steps) == 1, f"Expected one tracker step in {job_name}"
    return steps[0]


def _effective_env(workflow, job_name, step=None):
    """Actions env precedence is workflow < job < step."""
    values = workflow.get("env", {}) | workflow["jobs"][job_name].get("env", {})
    if step is not None:
        values |= step.get("env", {})
    # YAML scalar spelling can differ while the runner sees the same string.
    return {name: str(value) for name, value in values.items()}


def _mapping(name, default):
    return "${{ vars." + name + " || '" + default + "' }}"


@pytest.mark.parametrize("name,default", TRACKER_DEFAULTS.items())
def test_both_tracker_steps_map_the_same_default_off_variable(name, default):
    for workflow_name, job_name in (
        ("track_performance.yml", "performance-clock"),
        ("daily_sync.yml", "sync-dashboard"),
    ):
        workflow = _workflow(workflow_name)
        step = _tracker_step(workflow, job_name)
        expected = _mapping(name, default)
        assert step["env"][name] == expected
        assert _effective_env(workflow, job_name, step)[name] == expected


def test_tracker_lanes_have_parity_for_every_shared_effective_env_key():
    clock = _workflow("track_performance.yml")
    sync = _workflow("daily_sync.yml")
    clock_env = _effective_env(clock, "performance-clock", _tracker_step(clock, "performance-clock"))
    sync_env = _effective_env(sync, "sync-dashboard", _tracker_step(sync, "sync-dashboard"))
    shared = clock_env.keys() & sync_env.keys()
    # Include inherited cohort settings, not just the newly added mappings.
    assert {"TRACK_HORIZONS", "TFB_TRACK_SHADOW_COHORTS",
            "TFB_PERF_TARGET_UNIT_SENTRY", *TRACKER_DEFAULTS} <= shared
    mismatches = {key: (clock_env[key], sync_env[key])
                  for key in sorted(shared) if clock_env[key] != sync_env[key]}
    assert not mismatches, mismatches


def test_tracker_publication_identity_survives_workflow_reruns():
    for name, job in (("track_performance.yml", "performance-clock"),
                      ("daily_sync.yml", "sync-dashboard")):
        step = _tracker_step(_workflow(name), job)
        assert step["env"]["TRACK_PUBLICATION_ID"] == "${{ github.run_id }}"


@pytest.mark.parametrize("name,default", SYNC_DEFAULTS.items())
def test_primary_and_recovery_sync_jobs_map_identical_operator_controls(name, default):
    workflow = _workflow("daily_sync.yml")
    for job_name in SYNC_JOBS:
        assert workflow["jobs"][job_name]["env"][name] == _mapping(name, default)


def test_sync_writer_lanes_have_parity_for_every_shared_effective_env_key():
    workflow = _workflow("daily_sync.yml")
    primary = _effective_env(workflow, SYNC_JOBS[0])
    recovery = _effective_env(workflow, SYNC_JOBS[1])
    shared = primary.keys() & recovery.keys()
    assert SYNC_DEFAULTS.keys() <= shared
    mismatches = {key: (primary[key], recovery[key])
                  for key in sorted(shared) if primary[key] != recovery[key]}
    assert not mismatches, mismatches


def test_s1_v2_requires_an_operator_variable_and_defaults_off():
    workflow = _workflow("shadow_scorer.yml")
    assert workflow["jobs"]["score"]["env"]["TFB_S1_CRITERIA_V2"] == _mapping("TFB_S1_CRITERIA_V2", "off")


def test_behavior_flags_are_scoped_to_writer_lanes_and_do_not_leak_into_ci():
    names = TRACKER_DEFAULTS.keys() | SYNC_DEFAULTS.keys() | {"TFB_S1_CRITERIA_V2"}
    for workflow_name in ("track_performance.yml", "daily_sync.yml", "shadow_scorer.yml"):
        workflow = _workflow(workflow_name)
        assert not names.intersection(workflow.get("env", {}))
    daily = _workflow("daily_sync.yml")
    for step in daily["jobs"]["ci-tests"]["steps"]:
        assert not names.intersection(_effective_env(daily, "ci-tests", step))
