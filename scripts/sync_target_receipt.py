#!/usr/bin/env python3
"""Bind follow-up live audits to the exact source sync's resolved target.

This receipt proves which workbook was requested, not that its rows are fresh.
The existing coverage and decision audits retain that responsibility. Every
primary matrix leg must agree, including the source run's exact rerun attempt.
Missing historical receipts never imply the current default workbook.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Mapping
from urllib.parse import urlsplit


SCHEMA = "tadawul.sync-target.v1"
GROUPS = frozenset({"core-pages", "global-markets", "mutual-funds"})
MODES = frozenset({"full_sync", "health_only", "single_key"})
PRODUCTION_EVENTS = frozenset({"schedule", "workflow_dispatch"})
WORKFLOW_PATH = ".github/workflows/daily_sync.yml"
RECEIPT_FIELDS = frozenset({
    "schema", "repository", "run_id", "run_attempt", "source_sha",
    "source_ref", "source_event", "run_mode", "group", "backend_url", "sheet_id",
})


class ReceiptError(ValueError):
    """The source target cannot safely be established."""


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ReceiptError(f"{field} must be a nonempty, trimmed string")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ReceiptError(f"{field} must not contain control characters")
    return value


def _positive_id(value: Any, field: str) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ReceiptError(f"{field} must be a positive integer")
    value = str(value)
    if not re.fullmatch(r"[1-9][0-9]*", value):
        raise ReceiptError(f"{field} must be a positive integer")
    return value


def _repository(value: Any) -> str:
    value = _text(value, "repository")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", value):
        raise ReceiptError("repository must identify one GitHub repository")
    return value


def _sha(value: Any) -> str:
    value = _text(value, "source_sha")
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise ReceiptError("source_sha must be an exact lowercase 40-character commit SHA")
    return value


def _sheet(value: Any) -> str:
    value = _text(value, "sheet_id")
    if value in {"null", "undefined"} or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ReceiptError("sheet_id must be a resolved spreadsheet ID")
    return value


def _backend(value: Any) -> str:
    value = _text(value, "backend_url")
    try:
        parsed = urlsplit(value)
        valid = (parsed.scheme in {"https", "http"} and bool(parsed.hostname)
                 and not any(char.isspace() for char in value)
                 and parsed.username is None and parsed.password is None
                 and not parsed.query and not parsed.fragment)
        parsed.port  # Invalid ports must not become durable target evidence.
    except ValueError:
        valid = False
    if not valid:
        raise ReceiptError("backend_url must be an HTTP(S) base URL without credentials, query or fragment")
    return value


def validate_receipt(receipt: Any) -> dict[str, Any]:
    if not isinstance(receipt, dict) or set(receipt) != RECEIPT_FIELDS:
        raise ReceiptError("sync target receipt has missing or unexpected fields")
    if receipt["schema"] != SCHEMA:
        raise ReceiptError("unsupported sync target receipt schema")
    result = dict(receipt)
    result["repository"] = _repository(receipt["repository"])
    result["run_id"] = _positive_id(receipt["run_id"], "run_id")
    result["run_attempt"] = int(_positive_id(receipt["run_attempt"], "run_attempt"))
    result["source_sha"] = _sha(receipt["source_sha"])
    result["source_ref"] = _text(receipt["source_ref"], "source_ref")
    if not result["source_ref"].startswith("refs/heads/") or result["source_ref"] == "refs/heads/":
        raise ReceiptError("source_ref must identify a branch")
    for field, allowed in (("source_event", PRODUCTION_EVENTS), ("run_mode", MODES), ("group", GROUPS)):
        if not isinstance(receipt[field], str) or receipt[field] not in allowed:
            raise ReceiptError(f"unsupported {field}")
    if result["source_event"] == "schedule" and result["run_mode"] != "full_sync":
        raise ReceiptError("scheduled sync receipts must have full_sync mode")
    result["sheet_id"] = _sheet(receipt["sheet_id"])
    result["backend_url"] = _backend(receipt["backend_url"])
    return result


def build_receipt(environ: Mapping[str, str], group: str) -> dict[str, Any]:
    """Record the values already selected by Smart Configuration Resolution."""
    return validate_receipt({
        "schema": SCHEMA,
        "repository": environ.get("GITHUB_REPOSITORY"),
        "run_id": environ.get("GITHUB_RUN_ID"),
        "run_attempt": environ.get("GITHUB_RUN_ATTEMPT"),
        "source_sha": environ.get("GITHUB_SHA"),
        "source_ref": environ.get("GITHUB_REF"),
        "source_event": environ.get("GITHUB_EVENT_NAME"),
        "run_mode": environ.get("RUN_MODE"),
        "group": group,
        "backend_url": environ.get("TARGET_URL"),
        "sheet_id": environ.get("TARGET_SHEET_ID"),
    })


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ReceiptError("JSON evidence contains a duplicate field")
        result[key] = value
    return result


def _read_json(path: Path, *, max_bytes: int = 32768) -> Any:
    if path.stat().st_size > max_bytes:
        raise ReceiptError("JSON evidence exceeds its size limit")
    try:
        return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ReceiptError("JSON evidence could not be decoded") from exc


def _source_identity(event: Any, repository: str) -> dict[str, Any]:
    if not isinstance(event, dict) or event.get("action") != "completed":
        raise ReceiptError("follow-up requires a completed workflow_run event")
    source = event.get("workflow_run")
    if not isinstance(source, dict):
        raise ReceiptError("workflow_run source metadata is missing")
    for owner in (event.get("repository"), source.get("repository"), source.get("head_repository")):
        if (not isinstance(owner, dict) or not isinstance(owner.get("full_name"), str)
                or owner["full_name"].casefold() != repository.casefold()):
            raise ReceiptError("source run repository does not match this repository")
    if (source.get("path") != WORKFLOW_PATH or source.get("head_branch") != "main"
            or not isinstance(source.get("event"), str) or source["event"] not in PRODUCTION_EVENTS
            or source.get("status") != "completed"):
        raise ReceiptError("source must be a completed main Advanced Sync production run")
    return {
        "repository": repository,
        "run_id": _positive_id(source.get("id"), "source run_id"),
        "run_attempt": int(_positive_id(source.get("run_attempt"), "source run_attempt")),
        "source_sha": _sha(source.get("head_sha")),
        "source_ref": "refs/heads/main",
        "source_event": source["event"],
    }


def resolve_followup(event: Any, root: Path, repository: str) -> dict[str, Any]:
    repository = _repository(repository)
    identity = _source_identity(event, repository)
    if not root.is_dir():
        raise ReceiptError("source target artifacts are missing; no default-target fallback is permitted")
    receipts = sorted(root.rglob("sync_target.json"))
    if len(receipts) != len(GROUPS):
        raise ReceiptError("all three primary matrix target receipts are required")
    found: dict[str, dict[str, Any]] = {}
    target: dict[str, Any] | None = None
    for path in receipts:
        if not path.resolve().is_relative_to(root.resolve()):
            raise ReceiptError("target receipt escapes the artifact directory")
        receipt = validate_receipt(_read_json(path))
        group = receipt["group"]
        expected_parent = f"tadawul-sync-logs-{identity['run_id']}-{group}"
        if path.relative_to(root).parts != (expected_parent, "sync_target.json"):
            raise ReceiptError("target receipt is not in its matching primary matrix artifact")
        if group in found:
            raise ReceiptError("duplicate primary matrix target receipt")
        for field, expected in identity.items():
            actual = receipt[field]
            if field == "repository":
                actual, expected = actual.casefold(), expected.casefold()
            if actual != expected:
                raise ReceiptError(f"target receipt does not match source {field}")
        current_target = {field: receipt[field] for field in ("sheet_id", "backend_url", "run_mode")}
        if target is not None and current_target != target:
            raise ReceiptError("primary matrix receipts disagree on the target or run mode")
        target = current_target
        found[group] = receipt
    if set(found) != GROUPS or target is None:
        raise ReceiptError("all three distinct primary matrix target receipts are required")
    required = target["run_mode"] == "full_sync"
    return {
        "status": "ready" if required else "skipped",
        "audit_required": required,
        "reason": "Validated full_sync source target" if required else
                  f"Source mode {target['run_mode']} does not request a full-refresh audit",
        "scope": "live_workbook",
        "source": identity,
        "target": target,
        "validated_groups": sorted(found),
    }


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    writer = commands.add_parser("write", help="record the resolved primary sync target")
    writer.add_argument("--group", required=True, choices=sorted(GROUPS))
    writer.add_argument("--output", type=Path, default=Path("sync_target.json"))
    resolver = commands.add_parser("resolve", help="validate a follow-up source or explicit standalone audit")
    resolver.add_argument("--event-name", required=True)
    resolver.add_argument("--event-path", type=Path, required=True)
    resolver.add_argument("--repository", required=True)
    resolver.add_argument("--receipts-root", type=Path, required=True)
    resolver.add_argument("--default-sheet-id", default="")
    resolver.add_argument("--json-out", type=Path, required=True)
    resolver.add_argument("--github-env", type=Path, required=True)
    resolver.add_argument("--github-output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "write":
            _write_json(args.output, build_receipt(os.environ, args.group))
            print(f"Resolved sync target receipt recorded for {args.group}.")
            return 0
        if args.event_name == "workflow_run":
            report = resolve_followup(_read_json(args.event_path, max_bytes=2_000_000),
                                      args.receipts_root, args.repository)
        elif args.event_name == "workflow_dispatch":
            # This audit workflow's own explicit dispatch requests current
            # production health. It is not a substitute for a source receipt.
            _repository(args.repository)
            report = {"status": "ready", "audit_required": True,
                      "reason": "Explicit standalone audit of the current default workbook",
                      "scope": "live_workbook", "source": None,
                      "target": {"sheet_id": _sheet(args.default_sheet_id)}}
        else:
            raise ReceiptError("target resolution is only for workflow_run or explicit workflow_dispatch")
        _write_json(args.json_out, report)
        if report["audit_required"]:
            with args.github_env.open("a", encoding="utf-8") as output:
                output.write(f"TARGET_SHEET_ID={report['target']['sheet_id']}\n")
        with args.github_output.open("a", encoding="utf-8") as output:
            output.write(f"audit_required={str(report['audit_required']).lower()}\n")
        print(report["reason"])
        return 0
    except (ReceiptError, OSError) as exc:
        message = str(exc) if isinstance(exc, ReceiptError) else "target evidence could not be read or written"
        if args.command == "resolve":
            try:
                _write_json(args.json_out, {"status": "blocked", "audit_required": False, "reason": message})
            except OSError:
                pass
        print(f"::error::{message}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
