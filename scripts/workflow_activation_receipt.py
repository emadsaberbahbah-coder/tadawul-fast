#!/usr/bin/env python3
"""Non-secret execution evidence for the existing DQ and tracker workflows.

Select explicit metadata only. These receipts prove execution/configuration;
they never certify Sheet readback, data quality or historical preservation.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
from typing import Mapping

SCRIPT_VERSION = "1.0.0"
REPOSITORY = Path(__file__).resolve().parents[1]
_VERSION = re.compile(r"\d+\.\d+\.\d+\Z")
_SHA = re.compile(r"[0-9a-f]{40}\Z")


def _version(path: Path) -> str | None:
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id in {"SCRIPT_VERSION", "__version__"}
            for target in node.targets
        ):
            if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                if _VERSION.fullmatch(node.value.value):
                    return node.value.value
    return None


def _accepted(raw: str | None, words: tuple[str, ...], default: str = "off") -> str:
    value = str(raw or "").strip().lower()
    return value if value in words else default


def _identity(env: Mapping[str, str], root: Path) -> dict:
    source_sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    if not _SHA.fullmatch(source_sha):
        raise ValueError("Source checkout does not have a valid commit identity")
    event_sha = str(env.get("GITHUB_SHA", ""))
    event_sha = event_sha if _SHA.fullmatch(event_sha) else None
    run_id, attempt = str(env.get("GITHUB_RUN_ID", "")), str(env.get("GITHUB_RUN_ATTEMPT", ""))
    return {"schema": "tfb.workflow-activation/v1", "receipt_version": SCRIPT_VERSION,
            "workflow_run_id": int(run_id) if run_id.isdecimal() else None,
            "run_attempt": int(attempt) if attempt.isdecimal() else None,
            "source_sha": source_sha, "event_sha": event_sha,
            "source_matches_event": source_sha == event_sha,
            "sheet_readback_verified": None, "historical_prefix_verified": None}


def dq_receipt(exit_code: int, *, root: Path = REPOSITORY,
               env: Mapping[str, str] | None = None) -> dict:
    values = os.environ if env is None else env
    receipt = _identity(values, root)
    selected = _accepted(values.get("TFB_DQ_AUDIT_MODE"), ("observe", "enforce"))
    expected_version = _version(root / "scripts/tfb_workbook_dq_audit.py")
    report_path = root / "dq_report.json"
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        report = {}
    if not isinstance(report, dict):
        report = {}
    report_version = report.get("version")
    report_version = report_version if isinstance(report_version, str) and _VERSION.fullmatch(report_version) else None
    report_mode = report.get("mode") if report.get("mode") in {"off", "observe", "enforce"} else None
    export_path = Path(values.get("RUNNER_TEMP", "")) / "tfb_export.xlsx"
    digest_matches = False
    if export_path.is_file():
        digest_matches = hashlib.sha256(export_path.read_bytes()).hexdigest() == report.get("export_sha256")
    verified = (receipt["source_matches_event"] and selected in {"observe", "enforce"} and report_mode == selected
                and report_version == expected_version and expected_version is not None and digest_matches)
    receipt.update(workflow="workbook_backup", audit_exit_code=exit_code,
                   source_versions={"backup_workbook": _version(root / "scripts/backup_workbook.py"),
                                    "tfb_workbook_dq_audit": expected_version},
                   selected_mode=selected, reported_mode=report_mode, reported_version=report_version,
                   report_present=report_path.is_file(), export_digest_matches_report=digest_matches,
                   execution_evidence_complete=verified, data_quality_accepted=None)
    return receipt


def tracker_receipt(exit_code: int, log_path: Path, help_path: Path, audit_enabled: bool, *,
                    root: Path = REPOSITORY, env: Mapping[str, str] | None = None) -> dict:
    values = os.environ if env is None else env
    receipt = _identity(values, root)
    source_version = _version(root / "scripts/track_performance.py")
    help_text = help_path.read_text(encoding="utf-8") if help_path.is_file() else ""
    witness = re.search(r"Performance Tracker v(\d+\.\d+\.\d+)", help_text)
    cli_version = witness.group(1) if witness else None
    log = log_path.read_text(encoding="utf-8", errors="replace") if log_path.is_file() else ""
    mode = _accepted(values.get("TRACK_DEDUP_SIGNAL_HISTORY"), ("observe", "enforce"))
    measurements = re.findall(
        r"DEDUP-SIGNAL\] mode=(observe|enforce) would_skip=(\d+)/(\d+) new=(\d+)"
        r" \| sheet rows=(\d+) keys=(\d+) backlog=(\d+) max_per_key=(\d+)", log)
    measurement = None
    if measurements:
        last = measurements[-1]
        measurement = {"mode": last[0], **dict(zip(
            ("would_skip", "built", "new", "sheet_rows", "sheet_keys", "backlog", "max_per_key"),
            (int(value) for value in last[1:]))) }
    measurement_matches = bool(measurement and measurement["mode"] == mode)
    writes = re.findall(r"record_signal_snapshots: wrote (\d+) new daily snapshot\(s\)", log)
    append_failures = any(token in log for token in (
        "[EVIDENCE-APPEND", "Failed to append", "init failed:", "record_signal_snapshots failed:"))
    publication = _accepted(values.get("TRACK_SIGNAL_PUBLICATION_MODE"), ("immutable",))
    canonical = str(values.get("TRACK_CANONICAL_TARGETS", "0")).strip().lower() in {"1", "true", "yes", "on"}
    unit = _accepted(values.get("TFB_PERF_TARGET_UNIT_SENTRY"), ("observe", "enforce"))
    verified = (receipt["source_matches_event"] and exit_code == 0
                and cli_version == source_version and source_version is not None
                and (mode == "off" or measurement_matches) and not append_failures)
    receipt.update(workflow="track_performance", tracker_exit_code=exit_code,
                   source_versions={"track_performance": source_version}, cli_version=cli_version,
                   signal_history_mode=mode, signal_history_measurement=measurement,
                   signal_history_measurement_only=True, same_day_deduplication_applied=False,
                   signal_snapshot_append_acknowledged=sum(int(value) for value in writes),
                   append_failure_logged=append_failures, audit_enabled=audit_enabled,
                   historical_grid_save_requested=audit_enabled,
                   prospective_controls={"canonical_targets": canonical, "target_unit_sentry": unit,
                                         "signal_publication_mode": publication},
                   execution_evidence_complete=verified)
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("dq", "tracker"))
    parser.add_argument("--exit-code", type=int, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--log", type=Path)
    parser.add_argument("--help-witness", type=Path)
    parser.add_argument("--audit-enabled", choices=("true", "false"), default="true")
    args = parser.parse_args()
    if args.kind == "dq":
        receipt = dq_receipt(args.exit_code)
    else:
        if args.log is None or args.help_witness is None:
            parser.error("tracker requires --log and --help-witness")
        receipt = tracker_receipt(args.exit_code, args.log, args.help_witness, args.audit_enabled == "true")
    args.receipt.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if receipt["execution_evidence_complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
