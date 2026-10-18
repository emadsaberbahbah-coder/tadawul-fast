"""Run both intraday dispatch wrappers with an inert Python command boundary."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/intraday_refresh.yml"
GUARD = "Guard - production apply only from main"
REFRESH = "Refresh decision-symbol quotes"


def workflow_steps():
    workflow = yaml.safe_load(WORKFLOW.read_text())
    return {step["name"]: step for step in workflow["jobs"]["refresh"]["steps"]}


@pytest.fixture
def run_wrappers(tmp_path):
    binary_dir = tmp_path / "bin"
    binary_dir.mkdir()
    calls_path = tmp_path / "calls.jsonl"
    python = binary_dir / "python"
    python.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "with open(os.environ['ARGV_CAPTURE'], 'a') as output:\n"
        "    output.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "raise SystemExit(int(os.environ['COMMAND_EXIT']))\n"
    )
    python.chmod(0o755)

    def run(apply=None, *, event="workflow_dispatch", ref="refs/heads/main", command_exit=0):
        env = {"PATH": str(binary_dir) + os.pathsep + os.defpath,
               "ARGV_CAPTURE": str(calls_path), "COMMAND_EXIT": str(command_exit),
               "GITHUB_EVENT_NAME": event, "GITHUB_REF": ref}
        if apply is not None:
            env["INPUT_APPLY"] = apply
        steps = workflow_steps()
        names = [GUARD, REFRESH] if event == "workflow_dispatch" else [REFRESH]
        results = []
        for name in names:
            result = subprocess.run(
                ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", steps[name]["run"]],
                cwd=tmp_path, env=env, text=True, capture_output=True, timeout=10,
            )
            results.append(result)
            if result.returncode:
                break
        calls = [json.loads(line) for line in calls_path.read_text().splitlines()] \
            if calls_path.exists() else []
        return results, calls

    return run


def test_free_form_apply_input_is_environment_data_in_both_wrappers():
    steps = workflow_steps()
    assert steps[GUARD]["if"] == "github.event_name == 'workflow_dispatch'"
    for name in (GUARD, REFRESH):
        assert steps[name]["env"]["INPUT_APPLY"] == "${{ github.event.inputs.apply }}"
        assert "${{" not in steps[name]["run"]


@pytest.mark.parametrize("event,apply,ref,expected", [
    ("workflow_dispatch", "true", "refs/heads/main", "--apply"),
    ("workflow_dispatch", "false", "refs/heads/main", "--scan"),
    ("workflow_dispatch", None, "refs/heads/main", "--scan"),
    ("workflow_dispatch", "TRUE", "refs/heads/main", "--scan"),
    ("workflow_dispatch", "false", "refs/heads/feature", "--scan"),
    ("schedule", None, "refs/heads/main", "--apply"),
])
def test_existing_dispatch_defaults_and_schedule_routing_are_preserved(
    run_wrappers, event, apply, ref, expected,
):
    results, calls = run_wrappers(apply, event=event, ref=ref)
    assert all(result.returncode == 0 for result in results)
    assert calls == [["scripts/intraday_quote_refresh.py", expected]]


def test_manual_apply_from_another_branch_stops_before_python(run_wrappers):
    results, calls = run_wrappers("true", ref="refs/heads/feature")
    assert len(results) == 1 and results[0].returncode == 1
    assert "Production apply is allowed only from main" in results[0].stdout
    assert calls == []


@pytest.mark.parametrize("apply", [
    "$(touch injected-command)",
    "`touch injected-backtick`",
    'true"; touch injected-quote; #',
    "true\n$(touch injected-newline)",
])
def test_metacharacters_are_literal_in_guard_and_refresh(run_wrappers, tmp_path, apply):
    results, calls = run_wrappers(apply, ref="refs/heads/feature")
    assert len(results) == 2 and all(result.returncode == 0 for result in results)
    assert calls == [["scripts/intraday_quote_refresh.py", "--scan"]]
    assert list(tmp_path.glob("injected-*")) == []


def test_branch_ref_diagnostic_does_not_execute_substitution(run_wrappers, tmp_path):
    results, calls = run_wrappers("true", ref="refs/heads/$(touch injected-ref)")
    assert len(results) == 1 and results[0].returncode == 1
    assert calls == [] and not (tmp_path / "injected-ref").exists()


@pytest.mark.parametrize("apply", ["true", "false"])
def test_refresh_failure_propagates_for_both_modes(run_wrappers, apply):
    results, calls = run_wrappers(apply, command_exit=2)
    assert results[0].returncode == 0 and results[-1].returncode == 2
    assert len(calls) == 1
