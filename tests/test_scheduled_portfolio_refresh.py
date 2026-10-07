"""Run the real Actions configuration shell offline with synthetic inputs."""
from __future__ import annotations

import ast
import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
DAILY = ROOT / ".github/workflows/daily_sync.yml"
RECOVERY = ROOT / ".github/workflows/page_refresh_recovery.yml"
LEGACY_KEYS = "MARKET_LEADERS GLOBAL_MARKETS COMMODITIES_FX MUTUAL_FUNDS DATA_DICTIONARY"


def step_script(name: str, substitutions=None) -> str:
    lines = DAILY.read_text(encoding="utf-8").splitlines()
    start = next(i for i, line in enumerate(lines) if line.strip() == "- name: " + name)
    run = next(i for i in range(start + 1, len(lines)) if lines[i].strip() == "run: |")
    body, indent = [], None
    for line in lines[run + 1:]:
        if not line.strip():
            body.append("")
            continue
        current = len(line) - len(line.lstrip())
        if indent is None:
            indent = current
        if current < indent:
            break
        body.append(line[indent:])
    script = "\n".join(body)
    for token, value in (substitutions or {}).items():
        script = script.replace("${{ " + token + " }}", value)
    assert "${{" not in script
    return script


def resolve(tmp_path, *, event="schedule", mode="", keys=LEGACY_KEYS, single=""):
    output = tmp_path / "github_env"
    output.write_text("", encoding="utf-8")
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "GITHUB_EVENT_NAME": event,
        "GITHUB_ENV": str(output),
        "RUNNER_TEMP": str(tmp_path),
        "INPUT_URL": "https://backend.example.invalid",
        "INPUT_SHEET": "synthetic-spreadsheet-id",
        "INPUT_MODE": mode,
        "INPUT_KEY": single,
        "VAR_DEFAULT_KEYS": keys,
        "SECRET_CREDS": json.dumps({"client_email": "fixture@example.invalid"}),
    }
    assert shutil.which("bash") and shutil.which("jq"), "Actions shell tools are required"
    proc = subprocess.run(
        ["bash", "-c", step_script("⚙️ Smart Configuration Resolution (robust for schedule + manual)")],
        env=env, text=True, capture_output=True, timeout=20,
    )
    values = dict(line.split("=", 1) for line in output.read_text().splitlines() if "=" in line)
    return proc, values


@pytest.mark.parametrize("keys", [
    LEGACY_KEYS,
    LEGACY_KEYS.lower().replace(" ", ","),
    json.dumps(LEGACY_KEYS.split()),
    "MARKET_LEADERS\nGLOBAL_MARKETS\tCOMMODITIES_FX;MUTUAL_FUNDS|DATA_DICTIONARY",
])
def test_real_scheduled_override_includes_portfolio(tmp_path, keys):
    proc, values = resolve(tmp_path, keys=keys)
    assert proc.returncode == 0, proc.stderr
    assert values["RUN_MODE"] == "full_sync"
    assert values["SYNC_KEYS"].split() == LEGACY_KEYS.split() + ["MY_PORTFOLIO"]


@pytest.mark.parametrize("keys", ["MY_PORTFOLIO MARKET_LEADERS", "MY_PORTFOLIO,MY_PORTFOLIO,GLOBAL_MARKETS", ""])
def test_existing_portfolio_is_not_duplicated_or_reordered(tmp_path, keys):
    proc, values = resolve(tmp_path, keys=keys)
    assert proc.returncode == 0, proc.stderr
    actual = values["SYNC_KEYS"].split()
    assert actual.count("MY_PORTFOLIO") == 1
    if keys:
        assert actual[0] == "MY_PORTFOLIO"


@pytest.mark.parametrize("event", ["workflow_dispatch", "push", "pull_request"])
def test_manual_and_ci_subset_requests_are_preserved(tmp_path, event):
    proc, values = resolve(tmp_path, event=event, mode="full_sync", keys="GLOBAL_MARKETS COMMODITIES_FX")
    assert proc.returncode == 0, proc.stderr
    assert values["SYNC_KEYS"] == "GLOBAL_MARKETS COMMODITIES_FX"


@pytest.mark.parametrize("event", ["schedule", "workflow_dispatch"])
@pytest.mark.parametrize("key", ["GLOBAL_MARKETS", "MY_PORTFOLIO"])
def test_single_key_is_never_expanded(tmp_path, event, key):
    proc, values = resolve(tmp_path, event=event, mode="single_key", single=key)
    assert proc.returncode == 0, proc.stderr
    assert values["SYNC_KEYS"] == key


def test_health_only_is_not_expanded(tmp_path):
    proc, values = resolve(tmp_path, mode="health_only", keys="COMMODITIES_FX")
    assert proc.returncode == 0, proc.stderr
    assert values["RUN_MODE"] == "health_only"
    assert values["SYNC_KEYS"] == "COMMODITIES_FX"


@pytest.mark.parametrize("keys", ["UNKNOWN_PAGE", "KSA_TADAWUL ADVISOR_CRITERIA", "GLOBAL_MARKETS WRONG_PAGE"])
def test_invalid_or_empty_sanitized_override_still_fails_before_publish(tmp_path, keys):
    proc, values = resolve(tmp_path, keys=keys)
    assert proc.returncode != 0
    assert "SYNC_KEYS" not in values


@pytest.mark.parametrize("path,expected_jobs", [(DAILY, 2), (RECOVERY, 1)])
def test_all_pf_writers_default_to_minor_unit_rejection_with_explicit_opt_out(path, expected_jobs):
    text = path.read_text(encoding="utf-8")
    declarations = re.findall(r"^\s+TFB_PF_MINOR_UNIT_CCY_GUARD:\s*(.+)$", text, re.M)
    assert declarations == ["${{ vars.TFB_PF_MINOR_UNIT_CCY_GUARD || '1' }}"] * expected_jobs
    # GitHub's string-valued repository variable 0 remains truthy, so the
    # explicit opt-out reaches the already tested runtime guard unchanged.
    assert "TFB_PORTFOLIO_REBUILD:" in text


def step_condition(name):
    lines = DAILY.read_text().splitlines()
    start = next(i for i, line in enumerate(lines) if line.strip() == "- name: " + name)
    for line in lines[start + 1:]:
        if line.strip().startswith("- name:"):
            break
        if line.strip().startswith("if:"):
            return line.strip()[3:].strip().removeprefix("${{").removesuffix("}}").strip()
    raise AssertionError("post-sync step needs an explicit condition")


def accepts(condition, *, success=True, group="global-markets", has_keys="true", mode="full_sync"):
    # Evaluate only the small boolean Actions expression actually in the file.
    # Unknown variables/operators fail this offline boundary rather than being
    # silently treated as an allowed write.
    values = {"success()": success, "matrix.group": group,
              "steps.scope_keys.outputs.has_keys": has_keys, "env.RUN_MODE": mode}
    for token, value in values.items():
        condition = condition.replace(token, repr(value))
    condition = condition.replace("&&", " and ").replace("||", " or ")
    tree = ast.parse(condition, mode="eval")
    allowed = (ast.Expression, ast.BoolOp, ast.And, ast.Or, ast.Compare, ast.Eq, ast.NotEq, ast.Constant)
    assert all(isinstance(node, allowed) for node in ast.walk(tree))
    return bool(eval(compile(tree, "workflow-condition", "eval"), {"__builtins__": {}}, {}))


@pytest.mark.parametrize("scope,group,keys,expected", [
    ("core", "core-pages", "MY_PORTFOLIO", ["MY_PORTFOLIO"]),
    ("only-gm", "global-markets", "MY_PORTFOLIO", []),
    ("only-mf", "mutual-funds", "MY_PORTFOLIO", []),
    ("only-gm", "global-markets", "GLOBAL_MARKETS", ["GLOBAL_MARKETS"]),
    ("only-mf", "mutual-funds", "MUTUAL_FUNDS", ["MUTUAL_FUNDS"]),
    ("core", "core-pages", LEGACY_KEYS + " MY_PORTFOLIO",
     ["MARKET_LEADERS", "COMMODITIES_FX", "DATA_DICTIONARY", "MY_PORTFOLIO"]),
])
def test_actual_matrix_scope_publishes_ownership_receipt(tmp_path, scope, group, keys, expected):
    output, env_file = tmp_path / "output", tmp_path / "env"
    output.write_text("")
    env_file.write_text("")
    script = step_script("🎯 Scope Keys To Matrix Leg", {
        "matrix.keys_scope": scope, "matrix.group": group, "matrix.stagger": "0",
    })
    proc = subprocess.run(["bash", "-c", script], text=True, capture_output=True, timeout=5,
        env={"PATH": os.environ["PATH"], "SYNC_KEYS": keys,
             "GITHUB_OUTPUT": str(output), "GITHUB_ENV": str(env_file)})
    assert proc.returncode == 0, proc.stderr
    actual = dict(line.split("=", 1) for line in env_file.read_text().splitlines())["SYNC_KEYS"].split()
    assert actual == expected
    assert output.read_text().strip() == "has_keys=" + ("true" if expected else "false")
    assert "id: scope_keys" in DAILY.read_text()


@pytest.mark.parametrize("success,group,has_keys,mode,validate,track", [
    (True, "global-markets", "true", "full_sync", True, True),
    (True, "core-pages", "true", "full_sync", True, False),
    (True, "mutual-funds", "true", "full_sync", True, False),
    (True, "global-markets", "false", "full_sync", False, False),
    (True, "global-markets", "false", "single_key", False, False),
    (True, "global-markets", "true", "single_key", True, False),
    (True, "core-pages", "true", "single_key", True, False),
    (True, "global-markets", "true", "health_only", False, False),
    (True, "global-markets", "false", "health_only", False, False),
    (False, "global-markets", "true", "full_sync", False, False),
])
def test_post_sync_writes_respect_actual_scope_and_mode(success, group, has_keys, mode, validate, track):
    state = dict(success=success, group=group, has_keys=has_keys, mode=mode)
    assert accepts(step_condition("✅ Validate Dashboard (contract + gate integrity)"), **state) is validate
    assert accepts(step_condition("📈 Track Performance (record + audit)"), **state) is track
