"""Execute source-target workflow bindings without credentials or live audits."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest
import yaml

from tests.test_sync_target_receipt import REPOSITORY, source_environment, source_event, write_matrix


ROOT = Path(__file__).resolve().parents[1]
CASES = (
    ("decision_surface_freshness.yml", "audit-live-surfaces", "audit_decision_surface_freshness.py"),
    ("full_refresh_coverage.yml", "audit-live-workbook", "audit_full_refresh_coverage.py"),
)


def workflow(name):
    return yaml.load((ROOT / ".github/workflows" / name).read_text(), Loader=yaml.BaseLoader)


def named_step(job, name):
    return next(step for step in job["steps"] if step.get("name") == name)


@pytest.mark.parametrize("name,job_name,script", CASES)
def test_followups_require_verified_target_before_dependencies_credentials_or_audit(name, job_name, script):
    source = workflow(name)
    job = source["jobs"][job_name]
    target = named_step(job, "Resolve verified audit target and mode")
    download = named_step(job, "Download source target receipts")
    assert job["permissions"] == {"contents": "read", "actions": "read"}
    assert "TARGET_SHEET_ID" not in job["env"]
    assert job["env"]["DEFAULT_AUDIT_SHEET_ID"] == "${{ vars.DEFAULT_SPREADSHEET_ID }}"
    assert target["id"] == "target" and "continue-on-error" not in target
    assert "if" not in target  # GitHub's default success() blocks failed downloads.
    assert download["if"] == "${{ github.event_name == 'workflow_run' }}"
    assert "continue-on-error" not in download
    assert download["with"]["run-id"] == "${{ github.event.workflow_run.id }}"
    assert download["with"]["pattern"] == "tadawul-sync-logs-${{ github.event.workflow_run.id }}-*"
    assert download["with"]["merge-multiple"] == "false"
    assert download["with"]["github-token"] == "${{ github.token }}"
    assert job["steps"].index(download) < job["steps"].index(target)
    for step in job["steps"]:
        if (step.get("name") == "Install dependencies" or "SECRET_CREDS" in step.get("env", {})
                or f"scripts/{script}" in step.get("run", "")):
            assert job["steps"].index(target) < job["steps"].index(step)
            assert step["if"] == "${{ steps.target.outputs.audit_required == 'true' }}"
    upload = next(step for step in job["steps"] if step.get("uses") == "actions/upload-artifact@v7")
    assert "sync_audit_target.json" in upload["with"]["path"]
    for event in ("push", "pull_request"):
        paths = source["on"][event]["paths"]
        for path in ("scripts/sync_target_receipt.py", "tests/test_sync_target_receipt.py",
                     "tests/test_followup_audit_target_workflows.py"):
            assert path in paths
    commands = "\n".join(step.get("run", "") for step in source["jobs"]["regression-tests"]["steps"])
    assert "tests/test_sync_target_receipt.py" in commands
    assert "tests/test_followup_audit_target_workflows.py" in commands


@pytest.mark.parametrize("name,job_name,script", CASES)
@pytest.mark.parametrize("mode", ["full_sync", "health_only", "single_key", "historical", "standalone"])
def test_actual_followup_resolver_and_audit_shell_use_only_the_verified_target(tmp_path, name, job_name, script, mode):
    job = workflow(name)["jobs"][job_name]
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    shutil.copyfile(ROOT / "scripts/sync_target_receipt.py", scripts / "sync_target_receipt.py")
    # An offline stand-in captures the exact --sheet-id passed by the real
    # audit wrapper. No Google modules, credentials or network calls are used.
    (scripts / script).write_text(
        "import json, sys\nfrom pathlib import Path\n"
        "Path('captured_audit.json').write_text(json.dumps({'sheet_id': "
        "sys.argv[sys.argv.index('--sheet-id') + 1]}))\n"
    )
    (tmp_path / "event.json").write_text(json.dumps(source_event()))
    if mode not in {"historical", "standalone"}:
        write_matrix(tmp_path / "source-sync-targets", RUN_MODE=mode)
    env_file = tmp_path / "github_env"
    output_file = tmp_path / "github_output"
    env_file.touch()
    output_file.touch()
    env = {
        "PATH": os.environ.get("PATH", os.defpath),
        "GITHUB_EVENT_NAME": "workflow_dispatch" if mode == "standalone" else "workflow_run",
        "GITHUB_EVENT_PATH": str(tmp_path / "event.json"), "GITHUB_REPOSITORY": REPOSITORY,
        "GITHUB_ENV": str(env_file), "GITHUB_OUTPUT": str(output_file),
        "DEFAULT_AUDIT_SHEET_ID": "current-default", "TFB_REFRESH_AUDIT_MAX_ROWS": "20000",
    }
    target = named_step(job, "Resolve verified audit target and mode")
    resolved = subprocess.run(["bash", "-c", target["run"]], cwd=tmp_path, env=env,
                              text=True, capture_output=True, timeout=10)
    report = json.loads((tmp_path / "sync_audit_target.json").read_text())
    if mode == "historical":
        assert resolved.returncode == 2
        assert report["status"] == "blocked" and env_file.read_text() == ""
        return
    assert resolved.returncode == 0, resolved.stdout + resolved.stderr
    if mode in {"health_only", "single_key"}:
        assert output_file.read_text() == "audit_required=false\n"
        assert report["status"] == "skipped" and env_file.read_text() == ""
        return
    assert output_file.read_text() == "audit_required=true\n"
    env.update(dict(line.split("=", 1) for line in env_file.read_text().splitlines()))
    env["GITHUB_OUTPUT"] = str(tmp_path / "audit_output")
    audit = next(step for step in job["steps"] if f"scripts/{script}" in step.get("run", ""))
    audited = subprocess.run(["bash", "-c", audit["run"]], cwd=tmp_path, env=env,
                             text=True, capture_output=True, timeout=10)
    assert audited.returncode == 0, audited.stdout + audited.stderr
    expected = "current-default" if mode == "standalone" else "source-workbook"
    assert json.loads((tmp_path / "captured_audit.json").read_text())["sheet_id"] == expected


def test_primary_receipt_is_recorded_after_resolution_before_any_page_write_and_uploaded(tmp_path):
    job = workflow("daily_sync.yml")["jobs"]["sync-dashboard"]
    steps = job["steps"]
    producer = next(step for step in steps if "scripts/sync_target_receipt.py write" in step.get("run", ""))
    smart = next(step for step in steps if step.get("id") == "smart_config")
    supersede = next(step for step in steps if step.get("name") == "Supersede released decision feed (P0-1)")
    assert steps.index(smart) < steps.index(producer) < steps.index(supersede)
    assert producer["env"]["SYNC_GROUP"] == "${{ matrix.group }}"
    assert "continue-on-error" not in producer
    upload = next(step for step in steps if step.get("name") == "📤 Upload Logs")
    assert "sync_target.json" in upload["with"]["path"].splitlines()
    (tmp_path / "scripts").mkdir()
    shutil.copyfile(ROOT / "scripts/sync_target_receipt.py", tmp_path / "scripts/sync_target_receipt.py")
    env = dict(source_environment(), PATH=os.environ.get("PATH", os.defpath), SYNC_GROUP="core-pages")
    result = subprocess.run(["bash", "-c", producer["run"]], cwd=tmp_path, env=env,
                            text=True, capture_output=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr
    receipt = json.loads((tmp_path / "sync_target.json").read_text())
    assert receipt["sheet_id"] == "source-workbook" and receipt["group"] == "core-pages"
