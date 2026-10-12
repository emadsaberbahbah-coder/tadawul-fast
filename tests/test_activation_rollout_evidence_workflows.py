"""Execute real rollout/runner wrappers without provider or workbook writes."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

import pytest

from tests.test_approved_universe_workflow import workflow
from tests.test_scheduled_portfolio_refresh import accepts
from tests.test_workflow_activation_observe import workflow_variable_value

LANES = (("daily_sync.yml", "sync-dashboard"),
         ("daily_sync.yml", "recover-missing-market-pages"),
         ("page_refresh_recovery.yml", "recover-page"))


def step(lane, name):
    matches = [item for item in workflow(lane[0])["jobs"][lane[1]]["steps"] if item.get("name") == name]
    assert len(matches) == 1
    return matches[0]


def resolve(lane, tmp_path, **settings):
    env = dict(os.environ, GITHUB_ENV=str(tmp_path / "github-env"), GITHUB_RUN_ID="70001",
               GITHUB_SHA="tested-release-sha", GITHUB_EVENT_NAME="workflow_dispatch",
               TFB_RANK_OVERALL_PAGE="off", TFB_WARNINGS_COMPACT="off")
    env.pop("RANK_MODE_OVERRIDE", None)
    env.pop("WARNINGS_MODE_OVERRIDE", None)
    env.update(settings)
    result = subprocess.run(["bash", "-c", step(lane, "Resolve publication rollout modes")["run"]],
                            cwd=tmp_path, env=env, text=True, capture_output=True)
    return result


@pytest.mark.parametrize("file", ("daily_sync.yml", "page_refresh_recovery.yml"))
def test_dispatch_controls_allow_rank_enforce_but_no_warning_enforce(file):
    inputs = workflow(file)["on"]["workflow_dispatch"]["inputs"]
    assert inputs["rank_mode"]["default"] == inputs["warnings_mode"]["default"] == "inherit"
    assert inputs["rank_mode"]["options"] == ["inherit", "off", "observe", "enforce"]
    assert inputs["warnings_mode"]["options"] == ["inherit", "off", "observe"]
    assert not any("creds" in name or "token" in name for name in inputs)


@pytest.mark.parametrize("event,choice,group,mode,expected", [
    ("workflow_dispatch", "false", "global-markets", "full_sync", False),
    ("workflow_dispatch", "true", "global-markets", "full_sync", True),
    ("workflow_dispatch", "", "global-markets", "full_sync", True),
    ("schedule", "false", "global-markets", "full_sync", True),
    ("workflow_dispatch", "true", "core", "full_sync", False),
    ("workflow_dispatch", "true", "global-markets", "single_key", False),
])
def test_activation_can_refresh_all_sources_without_bundled_historical_save(event, choice, group, mode, expected):
    daily = workflow("daily_sync.yml")
    control = daily["on"]["workflow_dispatch"]["inputs"]["run_tracker"]
    assert control["type"] == "boolean" and str(control["default"]).lower() == "true"
    expression = step(LANES[0], "📈 Track Performance (record + audit)")["if"]
    expression = expression.removeprefix("${{").removesuffix("}}").strip()
    assert accepts(expression, group=group, mode=mode, event=event, run_tracker=choice) is expected
    # Source recovery and final acceptance must still run in this full-sync mode.
    for job in ("recover-missing-market-pages", "verify-final-decision-publication"):
        assert "run_tracker" not in daily["jobs"][job]["if"]


@pytest.mark.parametrize("lane", LANES)
@pytest.mark.parametrize("rank,warnings,expected", [
    ("inherit", "inherit", {"rank": "off", "warnings": "off"}),
    ("observe", "observe", {"rank": "observe", "warnings": "observe"}),
    ("enforce", "observe", {"rank": "enforce", "warnings": "observe"}),
    ("off", "off", {"rank": "off", "warnings": "off"}),
])
def test_actual_resolver_records_same_modes_in_all_writer_lanes(lane, rank, warnings, expected, tmp_path):
    result = resolve(lane, tmp_path, RANK_MODE_OVERRIDE=rank, WARNINGS_MODE_OVERRIDE=warnings)
    assert result.returncode == 0, result.stdout + result.stderr
    evidence = json.loads((tmp_path / "rollout_modes.json").read_text())
    assert evidence["effective"] == expected and evidence["source_sha"] == "tested-release-sha"
    assert evidence["run_id"] == "70001" and evidence["warnings_enforcement"] == "blocked_no_prewrite_external_archive"
    assert (tmp_path / "github-env").read_text() == (
        "TFB_RANK_OVERALL_PAGE=" + expected["rank"] + "\nTFB_WARNINGS_COMPACT=" + expected["warnings"] + "\n")


@pytest.mark.parametrize("lane", LANES)
@pytest.mark.parametrize("rank_raw,warnings_raw,expected_rank", [(" enforce ", " Observe ", "enforce"),
                                                                ("1", "true", "off")])
def test_scheduled_inherit_preserves_source_normalization_and_repository_defaults(lane, rank_raw, warnings_raw, expected_rank, tmp_path):
    result = resolve(lane, tmp_path, GITHUB_EVENT_NAME="schedule", TFB_RANK_OVERALL_PAGE=rank_raw,
                     TFB_WARNINGS_COMPACT=warnings_raw)
    assert result.returncode == 0, result.stdout + result.stderr
    evidence = json.loads((tmp_path / "rollout_modes.json").read_text())
    assert evidence["requested"] == {"rank": "inherit", "warnings": "inherit"}
    assert evidence["effective"] == {"rank": expected_rank, "warnings": "observe" if warnings_raw.strip().lower() == "observe" else "off"}
    job = workflow(lane[0])["jobs"][lane[1]]
    assert job["env"]["TFB_RANK_OVERALL_PAGE"] == "${{ vars.TFB_RANK_OVERALL_PAGE || 'observe' }}"
    assert job["env"]["TFB_WARNINGS_COMPACT"] == "${{ vars.TFB_WARNINGS_COMPACT || 'observe' }}"


@pytest.mark.parametrize("lane", LANES)
@pytest.mark.parametrize("event", ("schedule", "workflow_dispatch"))
@pytest.mark.parametrize("variable,override,expected", (
    (None, "inherit", "observe"), ("", "inherit", "observe"),
    ("off", "inherit", "off"), ("false", "inherit", "off"),
    ("invalid", "inherit", "off"), ("observe", "inherit", "observe"),
    (None, "off", "off"), ("observe", "off", "off"),
))
def test_persistent_observe_defaults_preserve_explicit_disable_in_all_writer_lanes(lane, event, variable, override, expected, tmp_path):
    job = workflow(lane[0])["jobs"][lane[1]]
    configured = {}
    for name in ("TFB_RANK_OVERALL_PAGE", "TFB_WARNINGS_COMPACT"):
        assert job["env"][name] == "${{ vars." + name + " || 'observe' }}"
        configured[name] = workflow_variable_value(job["env"][name], name, variable)
    result = resolve(lane, tmp_path, GITHUB_EVENT_NAME=event,
                     RANK_MODE_OVERRIDE=override, WARNINGS_MODE_OVERRIDE=override, **configured)
    assert result.returncode == 0, result.stdout + result.stderr
    value = json.loads((tmp_path / "rollout_modes.json").read_text())
    assert value["requested"] == {"rank":override, "warnings":override}
    assert value["configured"] == {"rank":configured["TFB_RANK_OVERALL_PAGE"],
                                    "warnings":configured["TFB_WARNINGS_COMPACT"]}
    assert value["effective"] == {"rank":expected, "warnings":expected}
    assert value["warnings_enforcement"] == "blocked_no_prewrite_external_archive"
    assert (tmp_path / "github-env").read_text() == (
        "TFB_RANK_OVERALL_PAGE=" + expected + "\nTFB_WARNINGS_COMPACT=" + expected + "\n")


@pytest.mark.parametrize("lane", LANES)
@pytest.mark.parametrize("settings", [
    {"WARNINGS_MODE_OVERRIDE": "enforce"},
    {"TFB_WARNINGS_COMPACT": " Enforce "},
    {"RANK_MODE_OVERRIDE": "true"},
    {"WARNINGS_MODE_OVERRIDE": "observe\nTFB_WARNINGS_COMPACT=enforce"},
])
def test_invalid_override_or_warning_enforce_blocks_before_any_writer(lane, settings, tmp_path):
    result = resolve(lane, tmp_path, **settings)
    assert result.returncode != 0 and "::error::" in result.stderr
    assert not (tmp_path / "github-env").exists() and not (tmp_path / "rollout_modes.json").exists()


def install_runner(tmp_path):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "run_dashboard_sync.py").write_text('''import json, os, sys
from pathlib import Path
args = sys.argv[1:]
report = Path(args[args.index('--json-out') + 1])
call = {'args': args, 'report': str(report), 'rank': os.getenv('TFB_RANK_OVERALL_PAGE'),
        'warnings': os.getenv('TFB_WARNINGS_COMPACT'), 'source_sha': os.getenv('GITHUB_SHA')}
with open(os.environ['OFFLINE_CALLS'], 'a') as out:
    out.write(json.dumps(call) + '\\n')
report.write_text(json.dumps({'summary': {'version': '6.67.0'}, 'results': []}))
print('SYNC DONE | failed=0')
sys.exit(int(os.getenv('OFFLINE_RUNNER_EXIT', '0')))
''')


@pytest.mark.parametrize("code", (0, 2))
def test_actual_primary_wrapper_retains_separate_portfolio_and_market_reports(code, tmp_path):
    install_runner(tmp_path)
    env = dict(os.environ, TARGET_URL="https://offline.invalid", TARGET_SHEET_ID="offline-sheet",
               SYNC_KEYS="MY_PORTFOLIO MARKET_LEADERS", GITHUB_RUN_ID="70001", GITHUB_RUN_ATTEMPT="2",
               GITHUB_SHA="tested-release-sha", GITHUB_OUTPUT=str(tmp_path / "output"),
               OFFLINE_CALLS=str(tmp_path / "calls"), OFFLINE_RUNNER_EXIT=str(code),
               TFB_RANK_OVERALL_PAGE="enforce", TFB_WARNINGS_COMPACT="observe", RUN_MODE="full_sync")
    body = step(LANES[0], "🚀 Execute Dashboard Sync")["run"].replace("${{ matrix.group }}", "offline-core")
    result = subprocess.run(["bash", "-c", body], cwd=tmp_path, env=env, text=True, capture_output=True)
    assert result.returncode == code, result.stdout + result.stderr
    calls = [json.loads(line) for line in (tmp_path / "calls").read_text().splitlines()]
    assert len(calls) == 2 and len({call["report"] for call in calls}) == 2
    assert all(Path(tmp_path / call["report"]).is_file() for call in calls)
    assert "--no-clear" in calls[0]["args"] and "--no-clear" not in calls[1]["args"]
    assert all(call["rank"] == "enforce" and call["warnings"] == "observe" for call in calls)
    assert all(call["source_sha"] == "tested-release-sha" for call in calls)


@pytest.mark.parametrize("code", (0, 2))
def test_actual_manual_page_wrapper_retains_runner_report_and_exit_grade(code, tmp_path):
    install_runner(tmp_path)
    env = dict(os.environ, TARGET_URL="https://offline.invalid", TARGET_SHEET_ID="offline-sheet",
               GITHUB_RUN_ID="70002", GITHUB_RUN_ATTEMPT="1", GITHUB_SHA="tested-release-sha",
               GITHUB_OUTPUT=str(tmp_path / "output"), OFFLINE_CALLS=str(tmp_path / "calls"),
               OFFLINE_RUNNER_EXIT=str(code), TFB_RANK_OVERALL_PAGE="enforce", TFB_WARNINGS_COMPACT="observe")
    body = step(LANES[2], "Refresh one page with an independent budget")["run"]
    body = body.replace("${{ matrix.key }}", "MARKET_LEADERS").replace("${{ matrix.group }}", "market-leaders").replace(
        "${{ matrix.page }}", "Market_Leaders")
    result = subprocess.run(["bash", "-c", body], cwd=tmp_path, env=env, text=True, capture_output=True)
    assert result.returncode == code, result.stdout + result.stderr
    calls = [json.loads(line) for line in (tmp_path / "calls").read_text().splitlines()]
    assert len(calls) == 1 and (tmp_path / calls[0]["report"]).is_file()
    assert "--json-out" in calls[0]["args"] and calls[0]["source_sha"] == "tested-release-sha"
    assert (tmp_path / "output").read_text().strip() == f"runner_exit={code}"


def test_receipts_are_uploaded_in_every_lane_without_source_sha_hops_or_lease_changes():
    for lane in (LANES[0], LANES[2]):
        job = workflow(lane[0])["jobs"][lane[1]]
        uploads = [item for item in job["steps"] if item.get("uses", "").startswith("actions/upload-artifact@")]
        paths = "\n".join(item["with"]["path"] for item in uploads)
        assert "sync_runner_*.json" in paths and "rollout_modes.json" in paths
        checkouts = [item for item in job["steps"] if item.get("uses", "").startswith("actions/checkout@")]
        assert all(item.get("with", {}).get("ref") in (None, "${{ github.sha }}") for item in checkouts)
    inline = workflow("daily_sync.yml")["jobs"]["recover-missing-market-pages"]
    upload_paths = "\n".join(item["with"]["path"] for item in inline["steps"]
                             if item.get("uses", "").startswith("actions/upload-artifact@"))
    assert "rollout_modes.json" in upload_paths and "inline-recovery-evidence/market-leaders" in upload_paths
    for file in ("daily_sync.yml", "page_refresh_recovery.yml"):
        concurrency = workflow(file)["concurrency"]
        assert "tadawul-production-write" in concurrency["group"]
        assert "github.event_name == 'push'" in concurrency["cancel-in-progress"]
        assert "github.event_name == 'pull_request'" in concurrency["cancel-in-progress"]
