"""Run the real Actions configuration shell offline with synthetic inputs."""
from __future__ import annotations

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


def step_script(name: str) -> str:
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
