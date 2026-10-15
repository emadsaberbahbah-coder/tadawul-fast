"""P-170c: every input the Apps Script dispatcher sends must be a declared
workflow_dispatch input of the workflow it targets.

GitHub answers HTTP 422 "Unexpected inputs provided" to an undeclared input, so
renaming or dropping one of these in a workflow silently stops that dispatch
lane (since v1.2.0 the trigger then fails loudly, but only after the fact).
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
GAS = (ROOT / "apps_script/24_Sync_Dispatch.gs").read_text(encoding="utf-8")


def dispatch_inputs(name):
    data = yaml.safe_load((ROOT / ".github/workflows" / name).read_text(encoding="utf-8"))
    on = data.get(True, data.get("on"))
    return on["workflow_dispatch"]["inputs"]


def object_keys(literal):
    return set(re.findall(r"(\w+)\s*:", literal))


def test_daily_sync_dispatch_inputs_are_declared_and_boolean_flags_are_true():
    body = re.search(r"function tfbSdDispatchInputs_\(\) \{(.*?)\n\}", GAS, re.S).group(1)
    sent = object_keys(re.search(r"return \{(.*?)\};", body, re.S).group(1))
    assert sent == {"run_mode", "include_portfolio", "record_tracker"}
    declared = dispatch_inputs("daily_sync.yml")
    assert sent <= set(declared)
    assert "full_sync" in declared["run_mode"]["options"]
    for flag in ("include_portfolio", "record_tracker"):
        assert declared[flag]["type"] == "boolean"
        assert re.search(r"%s:\s*'true'" % flag.upper(), GAS)
    # The historical audit stays a manual, explicit opt-in.
    assert "run_tracker" not in sent


def test_s1_lane_inputs_are_declared_by_their_workflows():
    lanes = re.findall(r"workflow: '([\w.]+)',.*?inputs: \{([^}]*)\}", GAS, re.S)
    assert {wf for wf, _ in lanes} == {"shadow_board.yml", "shadow_scorer.yml"}
    for workflow, literal in lanes:
        assert object_keys(literal) <= set(dispatch_inputs(workflow)), workflow
