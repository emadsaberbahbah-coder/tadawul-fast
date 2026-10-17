"""Manual recovery plans consume same-run inline evidence before another write."""
from __future__ import annotations

import fnmatch
import io
import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest
import yaml

from scripts.plan_sync_recovery import build_recovery_plan

ROOT = Path(__file__).resolve().parents[1]
SOURCE_ID = "12345"
SOURCE_EXPRESSION = "${{ steps.source.outputs.run_id }}"
PRIMARY_PREFIX = f"tadawul-sync-logs-{SOURCE_ID}"
RECOVERY_PREFIX = f"page-refresh-{SOURCE_ID}"


def workflow():
    # BaseLoader retains GitHub's `on` key and renders literal booleans as
    # strings, avoiding YAML 1.1's interpretation of `on` as True.
    return yaml.load((ROOT / ".github/workflows/page_refresh_recovery.yml").read_text(), Loader=yaml.BaseLoader)


def verdict(page, fresh=100, requested=100, status="success"):
    return (f"[PAGE-VERDICT v6.26.0] page={page} status={status} rows_written={requested} "
            f"fresh_rows={fresh} requested_rows={requested} fresh_pct={100*fresh/requested:.4f} reason=synthetic\n")


def primary_artifacts():
    return {
        PRIMARY_PREFIX + "-global-markets": {"sync_execution.log": verdict("Global_Markets", 90)},
        PRIMARY_PREFIX + "-core-pages": {"sync_execution.log": verdict("Market_Leaders") + verdict("Commodities_FX")},
        PRIMARY_PREFIX + "-mutual-funds": {"sync_execution.log": verdict("Mutual_Funds")},
    }


def stage_downloads(tmp_path, artifacts):
    """Extract synthetic ZIPs using the workflow's actual patterns and paths.

    download-artifact v8 extracts one match directly into `path`; multiple
    matches with merge-multiple=false get separate artifact-name directories.
    Both layouts occur in existing source-run evidence.
    """
    steps = workflow()["jobs"]["plan"]["steps"]
    for step in steps:
        if not step.get("uses", "").startswith("actions/download-artifact@"):
            continue
        settings = step["with"]
        pattern = settings["pattern"].replace(SOURCE_EXPRESSION, SOURCE_ID)
        matching = {name: files for name, files in artifacts.items() if fnmatch.fnmatchcase(name, pattern)}
        if not matching:
            assert step.get("continue-on-error") == "true"
            continue
        base = tmp_path / settings["path"]
        for name, files in matching.items():
            target = base / name if len(matching) > 1 and settings.get("merge-multiple") == "false" else base
            archive_bytes = io.BytesIO()
            with zipfile.ZipFile(archive_bytes, "w") as archive:
                for filename, contents in files.items():
                    archive.writestr(filename, contents)
            archive_bytes.seek(0)
            with zipfile.ZipFile(archive_bytes) as archive:
                archive.extractall(target)
    return tmp_path / "source-sync-artifacts"


@pytest.fixture(autouse=True)
def full_fetch_gate(monkeypatch):
    monkeypatch.setenv("TFB_AUDIT_REQUIRE_FULL_FETCH", "1")
    monkeypatch.setenv("TFB_AUDIT_MIN_FRESH_PCT", "95")


def test_workflow_downloads_only_both_evidence_families_from_same_resolved_run_before_plan():
    config = workflow()
    plan = config["jobs"]["plan"]
    downloads = [(index, step) for index, step in enumerate(plan["steps"])
                 if step.get("uses", "").startswith("actions/download-artifact@")]
    assert len(downloads) == 2
    primary, recovery = [step for _, step in downloads]
    assert primary["with"]["pattern"] == "tadawul-sync-logs-" + SOURCE_EXPRESSION + "-*"
    assert primary["with"]["path"] == "source-sync-artifacts"
    assert primary.get("continue-on-error", "false") == "false"
    assert recovery["with"]["pattern"] == "page-refresh-" + SOURCE_EXPRESSION + "-*"
    assert recovery["with"]["path"] == "source-sync-artifacts/zz-recovery"
    assert recovery["continue-on-error"] == "true"
    for _, step in downloads:
        assert step["with"]["run-id"] == SOURCE_EXPRESSION
        assert step["with"]["github-token"] == "${{ github.token }}"
        assert step["with"]["merge-multiple"] == "false"
    planner_index = next(index for index, step in enumerate(plan["steps"]) if step.get("id") == "plan")
    assert downloads[-1][0] < planner_index
    assert "--root source-sync-artifacts" in plan["steps"][planner_index]["run"]
    assert plan["env"]["TFB_AUDIT_REQUIRE_FULL_FETCH"] == "1"
    assert plan["env"]["TFB_AUDIT_MIN_FRESH_PCT"] == "95"
    assert "tests/test_manual_recovery_evidence_workflow.py" in config["on"]["pull_request"]["paths"]
    assert "tests/test_manual_recovery_evidence_workflow.py" in config["on"]["push"]["paths"]
    assert any("tests/test_manual_recovery_evidence_workflow.py" in step.get("run", "")
               for step in config["jobs"]["regression-tests"]["steps"])


def test_single_flattened_inline_success_prevents_duplicate_full_page_replay(tmp_path):
    artifacts = primary_artifacts()
    artifacts[RECOVERY_PREFIX + "-global-markets"] = {"sync_execution.log": verdict("Global_Markets")}
    source = stage_downloads(tmp_path, artifacts)
    assert (source / "zz-recovery/sync_execution.log").is_file()
    plan = build_recovery_plan(source)
    assert plan["retry_pages"] == [] and not plan["needs_recovery"]


def test_multiple_inline_artifacts_and_later_cycles_follow_actual_audit_ordering(tmp_path):
    artifacts = primary_artifacts()
    artifacts[RECOVERY_PREFIX + "-global-markets"] = {
        "sync_execution.log": verdict("Global_Markets", 90),
        "cycle2/sync_execution.log": verdict("Global_Markets", 100),
    }
    artifacts[RECOVERY_PREFIX + "-commodities-fx"] = {"sync_execution.log": verdict("Commodities_FX")}
    source = stage_downloads(tmp_path, artifacts)
    assert (source / "zz-recovery" / (RECOVERY_PREFIX + "-global-markets") / "cycle2/sync_execution.log").is_file()
    assert build_recovery_plan(source)["retry_pages"] == []


@pytest.mark.parametrize("fresh,status", [(93, "success"), (100, "partial"), (100, "failed")])
def test_later_failed_or_incomplete_inline_attempt_still_requires_recovery(tmp_path, fresh, status):
    artifacts = primary_artifacts()
    artifacts[RECOVERY_PREFIX + "-global-markets"] = {
        "sync_execution.log": verdict("Global_Markets", 100),
        "cycle2/sync_execution.log": verdict("Global_Markets", fresh, status=status),
    }
    plan = build_recovery_plan(stage_downloads(tmp_path, artifacts))
    assert plan["retry_pages"] == ["Global_Markets"] and plan["needs_recovery"]


def test_old_source_run_without_inline_artifact_keeps_primary_only_retry(tmp_path):
    plan = build_recovery_plan(stage_downloads(tmp_path, primary_artifacts()))
    assert plan["retry_pages"] == ["Global_Markets"] and plan["needs_recovery"]


def test_other_run_recovery_and_other_artifact_families_cannot_certify_source_run(tmp_path):
    artifacts = primary_artifacts()
    artifacts["page-refresh-99999-global-markets"] = {"sync_execution.log": verdict("Global_Markets")}
    artifacts[f"inline-recovery-{SOURCE_ID}"] = {"sync_execution.log": verdict("Global_Markets")}
    artifacts[f"tfb-workbook-backup-{SOURCE_ID}"] = {"sync_execution.log": verdict("Global_Markets")}
    source = stage_downloads(tmp_path, artifacts)
    assert not (source / "zz-recovery").exists()
    assert build_recovery_plan(source)["retry_pages"] == ["Global_Markets"]


@pytest.mark.parametrize("download_outcome", ["success", "failure"])
def test_actual_planning_wrapper_discloses_absent_optional_evidence_and_keeps_retry(tmp_path, download_outcome):
    stage_downloads(tmp_path, primary_artifacts())
    (tmp_path / "scripts").symlink_to(ROOT / "scripts", target_is_directory=True)
    step = next(step for step in workflow()["jobs"]["plan"]["steps"] if step.get("id") == "plan")
    assert step["env"]["SOURCE_INLINE_DOWNLOAD_OUTCOME"] == "${{ steps.source_inline_evidence.outcome }}"
    env = dict(os.environ, SOURCE_INLINE_DOWNLOAD_OUTCOME=download_outcome,
               GITHUB_OUTPUT=str(tmp_path / "github-output"),
               PATH=str(Path(sys.executable).parent) + os.pathsep + os.environ.get("PATH", ""))
    result = subprocess.run(["bash", "-e", "-c", step["run"]], cwd=tmp_path, env=env,
                            capture_output=True, text=True, check=False, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "No source inline recovery logs are available" in result.stdout
    if download_outcome == "failure":
        assert "no recovery success is inferred" in result.stdout
    plan = json.loads((tmp_path / "sync-recovery-plan.json").read_text())
    assert plan["needs_recovery"] and plan["retry_pages"] == ["Global_Markets"]
