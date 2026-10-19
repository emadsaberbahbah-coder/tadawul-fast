#!/usr/bin/env python3
"""Verify pip-audit 2.9 JSON against a complete pip-list JSON inventory.

Usage: python scripts/verify_dependency_audit.py --installed INVENTORY.json
       --audit AUDIT.json [--output RECEIPT.json]

This verifier uses only the standard library. It reads metadata files; it
never imports audited distributions, executes package code, or uses network.
Names use Python project-name normalization; versions must match exactly.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
from typing import Any

__version__ = "1.0.0"
_NAME = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?")
_VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9.!+_-]*")


class VerificationError(ValueError):
    """Only controlled codes and aggregate counts belong in the receipt."""

    def __init__(self, code: str, **details: Any):
        super().__init__(code)
        self.code = code
        self.details = details


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise VerificationError("duplicate_json_key")
        result[key] = value
    return result


def _reject_nonfinite(_value):
    raise VerificationError("invalid_json_constant")


def _read_json(path: str):
    try:
        with Path(path).open(encoding="utf-8") as stream:
            return json.load(stream, object_pairs_hook=_unique_object,
                             parse_constant=_reject_nonfinite)
    except OSError as exc:
        raise VerificationError("file_read_error", error_type=type(exc).__name__) from None
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise VerificationError("json_parse_error", error_type=type(exc).__name__) from None


def _identity(entry, source):
    name, version = entry.get("name"), entry.get("version")
    if (isinstance(name, str) and name.lower() == "unknown") or \
            (isinstance(version, str) and version.lower() in {"unknown", "none", "null", "n/a"}):
        raise VerificationError("unknown_dependency")
    if not isinstance(name, str) or not _NAME.fullmatch(name) or \
            not isinstance(version, str) or not _VERSION.fullmatch(version) or \
            not any(char.isdigit() for char in version):
        raise VerificationError(source + "_schema_error")
    return re.sub(r"[-_.]+", "-", name).lower(), version


def _index(entries, *, source):
    result, vulnerabilities = {}, 0
    for entry in entries:
        if not isinstance(entry, dict):
            raise VerificationError(source + "_schema_error")
        if "skip_reason" in entry:
            raise VerificationError("skipped_dependency")
        if source == "audit":
            if set(entry) != {"name", "version", "vulns"} or not isinstance(entry["vulns"], list):
                raise VerificationError("audit_schema_error")
            vulnerabilities += len(entry["vulns"])
        elif not {"name", "version"}.issubset(entry) or \
                not set(entry).issubset({"name", "version", "editable_project_location"}) or \
                ("editable_project_location" in entry and not isinstance(entry["editable_project_location"], str)):
            raise VerificationError("installed_schema_error")
        name, version = _identity(entry, source)
        if name in result:
            raise VerificationError(source + "_duplicate_dependency")
        result[name] = version
    return result, vulnerabilities


def verify(installed, audit) -> dict[str, Any]:
    """Return a passing receipt only for exact, complete, vulnerability-free coverage."""
    if not isinstance(installed, list) or not installed:
        raise VerificationError("installed_schema_error")
    if not isinstance(audit, dict) or set(audit) != {"dependencies", "fixes"} or \
            not isinstance(audit["dependencies"], list) or not isinstance(audit["fixes"], list):
        raise VerificationError("audit_schema_error")
    if audit["fixes"]:
        raise VerificationError("unexpected_fixes")
    installed_index, _ = _index(installed, source="installed")
    audited_index, vulnerabilities = _index(audit["dependencies"], source="audit")
    installed_names, audited_names = set(installed_index), set(audited_index)
    counts = {
        "installed_count": len(installed_index), "audited_count": len(audited_index),
        "missing_count": len(installed_names - audited_names),
        "unexpected_count": len(audited_names - installed_names),
        "version_mismatch_count": sum(installed_index[name] != audited_index[name]
                                      for name in installed_names & audited_names),
        "vulnerability_count": vulnerabilities,
    }
    if counts["missing_count"] or counts["unexpected_count"] or counts["version_mismatch_count"]:
        raise VerificationError("coverage_mismatch", **counts)
    if vulnerabilities:
        raise VerificationError("vulnerabilities_present", **counts)
    return {"schema_version": 1, "status": "pass", "code": "complete", **counts}


class _Parser(argparse.ArgumentParser):
    def error(self, _message):
        raise VerificationError("argument_error")


def main(argv=None) -> int:
    parser = _Parser(description=__doc__)
    parser.add_argument("--installed", required=True)
    parser.add_argument("--audit", required=True)
    parser.add_argument("--output")
    args = None
    try:
        args = parser.parse_args(argv)
        receipt = verify(_read_json(args.installed), _read_json(args.audit))
    except VerificationError as exc:
        receipt = {"schema_version": 1, "status": "fail", "code": exc.code, **exc.details}
    if args is not None and args.output:
        try:
            Path(args.output).write_text(json.dumps(receipt, sort_keys=True) + "\n", encoding="utf-8")
        except OSError as exc:
            receipt = {"schema_version": 1, "status": "fail", "code": "output_write_error",
                       "error_type": type(exc).__name__}
    print(json.dumps(receipt, sort_keys=True, separators=(",", ":")))
    return 0 if receipt["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
