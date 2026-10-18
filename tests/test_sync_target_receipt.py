"""Offline target provenance checks; no Sheets access or provider requests."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from scripts import sync_target_receipt as receipt


ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = "emadsaberbahbah-coder/tadawul-fast"
SOURCE_SHA = "a" * 40


def source_environment(**overrides):
    values = {
        "GITHUB_REPOSITORY": REPOSITORY,
        "GITHUB_RUN_ID": "12345",
        "GITHUB_RUN_ATTEMPT": "2",
        "GITHUB_SHA": SOURCE_SHA,
        "GITHUB_REF": "refs/heads/main",
        "GITHUB_EVENT_NAME": "workflow_dispatch",
        "TARGET_URL": "https://source-backend.invalid/api",
        "TARGET_SHEET_ID": "source-workbook",
        "RUN_MODE": "full_sync",
    }
    return dict(values, **overrides)


def source_event(**overrides):
    source = {
        "id": 12345, "run_attempt": 2, "head_sha": SOURCE_SHA,
        "head_branch": "main", "event": "workflow_dispatch",
        "status": "completed", "conclusion": "failure", "path": receipt.WORKFLOW_PATH,
        "repository": {"full_name": REPOSITORY},
        "head_repository": {"full_name": REPOSITORY},
    }
    source.update(overrides)
    return {"action": "completed", "repository": {"full_name": REPOSITORY}, "workflow_run": source}


def write_matrix(root, **overrides):
    for group in receipt.GROUPS:
        path = root / f"tadawul-sync-logs-12345-{group}" / "sync_target.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(receipt.build_receipt(source_environment(**overrides), group)))


def resolve_cli(tmp_path, *, event_name="workflow_run", event=None, default_sheet="current-default"):
    event_path = tmp_path / "event.json"
    event_path.write_text(json.dumps(source_event() if event is None else event))
    env_file = tmp_path / "github_env"
    output_file = tmp_path / "github_output"
    env_file.write_text("PREEXISTING=keep\n")
    output_file.write_text("")
    result = subprocess.run([
        sys.executable, str(ROOT / "scripts/sync_target_receipt.py"), "resolve",
        "--event-name", event_name, "--event-path", str(event_path),
        "--repository", REPOSITORY, "--receipts-root", str(tmp_path / "artifacts"),
        "--default-sheet-id", default_sheet, "--json-out", str(tmp_path / "resolved.json"),
        "--github-env", str(env_file), "--github-output", str(output_file),
    ], env={"PATH": os.environ.get("PATH", os.defpath)}, text=True, capture_output=True, timeout=10)
    report = json.loads((tmp_path / "resolved.json").read_text())
    return result, report, env_file.read_text(), output_file.read_text()


def test_real_writer_cli_records_only_resolved_target_and_source_identity(tmp_path):
    path = tmp_path / "sync_target.json"
    env = dict(source_environment(), PATH=os.environ.get("PATH", os.defpath),
               GOOGLE_SHEETS_CREDENTIALS="must-never-be-persisted", VAR_SHEET="wrong-default")
    result = subprocess.run([
        sys.executable, str(ROOT / "scripts/sync_target_receipt.py"), "write",
        "--group", "core-pages", "--output", str(path),
    ], env=env, text=True, capture_output=True, timeout=10)
    assert result.returncode == 0, result.stderr
    data = json.loads(path.read_text())
    assert data["sheet_id"] == "source-workbook"
    assert data["backend_url"] == "https://source-backend.invalid/api"
    assert data["run_attempt"] == 2 and data["source_sha"] == SOURCE_SHA
    assert set(data) == receipt.RECEIPT_FIELDS
    assert "must-never-be-persisted" not in path.read_text() + result.stdout + result.stderr


def test_failed_source_full_sync_still_audits_its_override_and_exact_attempt(tmp_path):
    write_matrix(tmp_path / "artifacts")
    result, report, env, outputs = resolve_cli(tmp_path)
    assert result.returncode == 0, result.stderr
    assert report["status"] == "ready" and report["audit_required"] is True
    assert report["source"]["run_attempt"] == 2 and report["source"]["source_sha"] == SOURCE_SHA
    assert report["scope"] == "live_workbook"
    assert set(report["validated_groups"]) == receipt.GROUPS
    assert env == "PREEXISTING=keep\nTARGET_SHEET_ID=source-workbook\n"
    assert outputs == "audit_required=true\n"
    assert "current-default" not in json.dumps(report) + env


@pytest.mark.parametrize("mode", ["health_only", "single_key"])
def test_nonfull_source_modes_skip_full_audits_without_exporting_a_target(tmp_path, mode):
    write_matrix(tmp_path / "artifacts", RUN_MODE=mode)
    result, report, env, outputs = resolve_cli(tmp_path)
    assert result.returncode == 0, result.stderr
    assert report["status"] == "skipped" and report["audit_required"] is False
    assert mode in report["reason"] and report["target"]["run_mode"] == mode
    assert env == "PREEXISTING=keep\n" and outputs == "audit_required=false\n"


@pytest.mark.parametrize("event_name", ["schedule", "workflow_dispatch"])
def test_historical_source_without_receipts_never_falls_back_to_current_default(tmp_path, event_name):
    result, report, env, outputs = resolve_cli(tmp_path, event=source_event(event=event_name))
    assert result.returncode == 2
    assert report["status"] == "blocked" and report["audit_required"] is False
    assert "fallback" in report["reason"]
    assert env == "PREEXISTING=keep\n" and outputs == ""


@pytest.mark.parametrize("field,value", [
    ("run_id", "54321"), ("run_attempt", 1), ("source_sha", "b" * 40),
    ("repository", "someone/else"), ("source_ref", "refs/heads/feature"),
    ("source_event", "schedule"), ("sheet_id", "different-workbook"),
    ("backend_url", "https://different.invalid"), ("run_mode", "single_key"),
])
def test_mixed_matrix_target_or_source_identity_blocks_before_env_export(tmp_path, field, value):
    root = tmp_path / "artifacts"
    write_matrix(root)
    path = root / "tadawul-sync-logs-12345-core-pages" / "sync_target.json"
    data = json.loads(path.read_text())
    data[field] = value
    path.write_text(json.dumps(data))
    result, report, env, outputs = resolve_cli(tmp_path)
    assert result.returncode == 2
    assert report["status"] == "blocked"
    assert env == "PREEXISTING=keep\n" and outputs == ""


@pytest.mark.parametrize("changes", [
    {"id": 54321}, {"run_attempt": 3}, {"head_sha": "b" * 40},
    {"head_branch": "feature"}, {"event": "push"}, {"status": "in_progress"},
    {"path": ".github/workflows/other.yml"}, {"run_attempt": None},
    {"repository": {"full_name": "someone/else"}},
    {"head_repository": {"full_name": "someone/else"}},
    {"repository": {"full_name": None}}, {"event": []},
])
def test_source_metadata_cannot_relabel_other_run_artifacts(tmp_path, changes):
    root = tmp_path / "artifacts"
    write_matrix(root)
    with pytest.raises(receipt.ReceiptError):
        receipt.resolve_followup(source_event(**changes), root, REPOSITORY)


def test_all_three_groups_and_matching_artifact_families_are_required(tmp_path):
    write_matrix(tmp_path)
    path = tmp_path / "tadawul-sync-logs-12345-mutual-funds" / "sync_target.json"
    data = path.read_text()
    path.unlink()
    with pytest.raises(receipt.ReceiptError, match="all three"):
        receipt.resolve_followup(source_event(), tmp_path, REPOSITORY)
    (tmp_path / "sync_target.json").write_text(data)
    with pytest.raises(receipt.ReceiptError, match="matching primary"):
        receipt.resolve_followup(source_event(), tmp_path, REPOSITORY)


def test_receipt_symlink_outside_download_directory_is_rejected(tmp_path):
    root = tmp_path / "artifacts"
    write_matrix(root)
    path = root / "tadawul-sync-logs-12345-core-pages" / "sync_target.json"
    outside = tmp_path / "outside.json"
    path.rename(outside)
    path.symlink_to(outside)
    with pytest.raises(receipt.ReceiptError, match="escapes"):
        receipt.resolve_followup(source_event(), root, REPOSITORY)


@pytest.mark.parametrize("replacement", ["{", '{"schema":"first","schema":"second"}'])
def test_unreadable_or_ambiguous_receipt_json_is_rejected(tmp_path, replacement):
    write_matrix(tmp_path)
    path = tmp_path / "tadawul-sync-logs-12345-core-pages" / "sync_target.json"
    path.write_text(replacement)
    with pytest.raises(receipt.ReceiptError):
        receipt.resolve_followup(source_event(), tmp_path, REPOSITORY)


@pytest.mark.parametrize("field,value", [
    ("TARGET_SHEET_ID", "source\nINJECTED=value"), ("TARGET_SHEET_ID", "source\rbad"),
    ("TARGET_SHEET_ID", " "), ("TARGET_SHEET_ID", "null"),
    ("TARGET_URL", "https://source.invalid\nINJECTED=value"),
    ("TARGET_URL", "https://user:password@source.invalid"),
    ("TARGET_URL", "https://source.invalid?api_key=secret"),
    ("TARGET_URL", "https://source.invalid#secret"),
    ("TARGET_URL", "file:///tmp/book"), ("TARGET_URL", "https://"),
    ("GITHUB_RUN_ATTEMPT", "0"), ("GITHUB_SHA", "short"), ("RUN_MODE", "bogus"),
])
def test_writer_rejects_unresolved_unsafe_or_credential_bearing_values(field, value):
    with pytest.raises(receipt.ReceiptError):
        receipt.build_receipt(source_environment(**{field: value}), "core-pages")


def test_scheduled_receipts_require_full_sync_mode(tmp_path):
    write_matrix(tmp_path, GITHUB_EVENT_NAME="schedule")
    result = receipt.resolve_followup(source_event(event="schedule"), tmp_path, REPOSITORY)
    assert result["audit_required"] is True
    with pytest.raises(receipt.ReceiptError, match="full_sync"):
        receipt.build_receipt(source_environment(GITHUB_EVENT_NAME="schedule", RUN_MODE="health_only"), "core-pages")


def test_standalone_manual_audit_is_explicit_current_default_health_check(tmp_path):
    result, report, env, outputs = resolve_cli(tmp_path, event_name="workflow_dispatch")
    assert result.returncode == 0, result.stderr
    assert report["source"] is None
    assert report["target"]["sheet_id"] == "current-default"
    assert "standalone" in report["reason"]
    assert env.endswith("TARGET_SHEET_ID=current-default\n") and outputs == "audit_required=true\n"


def test_empty_standalone_default_fails_closed(tmp_path):
    result, report, env, outputs = resolve_cli(tmp_path, event_name="workflow_dispatch", default_sheet="")
    assert result.returncode == 2 and report["status"] == "blocked"
    assert env == "PREEXISTING=keep\n" and outputs == ""
