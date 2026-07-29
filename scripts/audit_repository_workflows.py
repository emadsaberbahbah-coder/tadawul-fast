#!/usr/bin/env python3
"""Audit GitHub workflow configuration for high-risk repository mistakes.

The scanner is intentionally offline and read-only. It blocks only conditions
that are clearly unsafe or impossible today; maintainable but old action majors
are warnings so existing production automation is not disabled unexpectedly.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Iterable, Sequence


CURRENT_ACTION_MAJORS = {
    "actions/checkout": 6,
    "actions/setup-python": 6,
    "actions/upload-artifact": 7,
}

OIDC_CONSUMERS = (
    "google-github-actions/auth@",
    "aws-actions/configure-aws-credentials@",
    "azure/login@",
    "hashicorp/vault-action@",
)


@dataclass(frozen=True)
class Finding:
    severity: str
    code: str
    path: str
    line: int
    detail: str


def _line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def audit_workflow_text(path: str, text: str) -> list[Finding]:
    findings: list[Finding] = []

    action_pattern = re.compile(
        r"(?m)^\s*uses:\s*"
        r"(actions/(?:checkout|setup-python|upload-artifact))@v(\d+)\s*$"
    )
    for match in action_pattern.finditer(text):
        action = match.group(1)
        major = int(match.group(2))
        current = CURRENT_ACTION_MAJORS[action]
        line = _line_number(text, match.start())
        if major > current:
            findings.append(Finding(
                "ERROR",
                "UNSUPPORTED_ACTION_MAJOR",
                path,
                line,
                f"{action}@v{major} is newer than the verified current major v{current}",
            ))
        elif major < current:
            findings.append(Finding(
                "WARN",
                "OUTDATED_ACTION_MAJOR",
                path,
                line,
                f"{action}@v{major} is behind verified current major v{current}",
            ))

    for match in re.finditer(r"(?m)^\s*pull_request_target\s*:", text):
        findings.append(Finding(
            "ERROR",
            "PULL_REQUEST_TARGET",
            path,
            _line_number(text, match.start()),
            "pull_request_target can expose repository secrets to untrusted PR code",
        ))

    for match in re.finditer(r"(?m)^\s*permissions\s*:\s*write-all\s*$", text):
        findings.append(Finding(
            "ERROR",
            "WRITE_ALL_PERMISSIONS",
            path,
            _line_number(text, match.start()),
            "workflow grants write-all instead of least-privilege permissions",
        ))

    sensitive_dispatch = re.compile(
        r"(?mi)^\s{6,}(?:token|secret|password|private_key|api_key)\s*:\s*$"
    )
    for match in sensitive_dispatch.finditer(text):
        findings.append(Finding(
            "ERROR",
            "SENSITIVE_WORKFLOW_INPUT",
            path,
            _line_number(text, match.start()),
            "sensitive values must be GitHub Secrets, not workflow_dispatch inputs",
        ))

    for match in re.finditer(
        r"(?m)^\s*TFB_SYNC_FORCE_REFETCH_SYMBOLS\s*:\s*.+$", text
    ):
        findings.append(Finding(
            "WARN",
            "TEMPORARY_OVERRIDE_ACTIVE",
            path,
            _line_number(text, match.start()),
            "one-run force-refetch override remains active; verify and remove after the repair run",
        ))

    id_token = re.search(r"(?m)^\s*id-token\s*:\s*write\s*$", text)
    if id_token and not any(consumer in text for consumer in OIDC_CONSUMERS):
        findings.append(Finding(
            "WARN",
            "UNUSED_ID_TOKEN_PERMISSION",
            path,
            _line_number(text, id_token.start()),
            "id-token: write is granted but no recognized OIDC authentication action is used",
        ))

    return findings


def audit_paths(paths: Iterable[Path]) -> list[Finding]:
    findings: list[Finding] = []
    for path in sorted(paths):
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            findings.append(Finding(
                "ERROR", "READ_ERROR", str(path), 0, f"cannot read workflow: {exc}"
            ))
            continue
        findings.extend(audit_workflow_text(str(path), text))
    return findings


def _default_paths(root: Path) -> Sequence[Path]:
    workflows = root / ".github" / "workflows"
    return tuple(workflows.glob("*.yml")) + tuple(workflows.glob("*.yaml"))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".", help="repository root")
    parser.add_argument(
        "--fail-on-warnings",
        action="store_true",
        help="also return non-zero for warnings",
    )
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    findings = audit_paths(_default_paths(root))
    for finding in findings:
        print(
            f"::{finding.severity.lower()} file={finding.path},line={finding.line}::"
            f"{finding.code}: {finding.detail}"
        )

    errors = sum(item.severity == "ERROR" for item in findings)
    warnings = sum(item.severity == "WARN" for item in findings)
    print(f"workflow audit: errors={errors} warnings={warnings}")
    if errors or (warnings and args.fail_on_warnings):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
