#!/usr/bin/env python3
"""Fail closed when scheduled market sync artifacts show skipped core pages.

The dashboard sync runner intentionally preserves last-good sheet data when a
provider fetch is incomplete. That safety behavior is correct, but the runner
can still exit zero after writing no rows for a required market page. This
read-only auditor parses the uploaded sync logs and separates process success
from data-refresh success.

Exit codes
----------
0  every required market page was observed and wrote rows
2  a required page was missing, skipped, failed, or wrote zero rows
3  artifacts could not be read
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Sequence


SCRIPT_VERSION = "1.0.0"
CRITICAL_MARKET_PAGES = (
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
)

_PAGE_VERDICT_RE = re.compile(
    r"\[PAGE-VERDICT[^\]]*\]\s+"
    r"page=(?P<page>\S+)\s+"
    r"status=(?P<status>\S+)\s+"
    r"rows_written=(?P<rows>\d+)"
)
_FORCE_REFETCH_RE = re.compile(r"\[FORCE-REFETCH[^\]]*\]", re.IGNORECASE)


@dataclass(frozen=True)
class PageVerdict:
    page: str
    status: str
    rows_written: int
    source: str
    line: int

    @property
    def passed(self) -> bool:
        return self.status.lower() in {"success", "partial"} and self.rows_written > 0


@dataclass(frozen=True)
class AuditResult:
    status: str
    required_pages: tuple[str, ...]
    observed_pages: tuple[str, ...]
    missing_pages: tuple[str, ...]
    failed_pages: tuple[str, ...]
    force_refetch_evidence_lines: int
    log_files: tuple[str, ...]
    verdicts: tuple[PageVerdict, ...]

    @property
    def exit_code(self) -> int:
        return 0 if self.status == "ok" else 2

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "1.0",
            "script_version": SCRIPT_VERSION,
            "status": self.status,
            "required_pages": list(self.required_pages),
            "observed_pages": list(self.observed_pages),
            "missing_pages": list(self.missing_pages),
            "failed_pages": list(self.failed_pages),
            "force_refetch_evidence_lines": self.force_refetch_evidence_lines,
            "log_files": list(self.log_files),
            "verdicts": [asdict(item) | {"passed": item.passed} for item in self.verdicts],
        }


def _candidate_logs(root: Path) -> list[Path]:
    """Prefer one canonical execution log per artifact to avoid duplicates."""
    canonical = sorted(path for path in root.rglob("sync_execution.log") if path.is_file())
    if canonical:
        return canonical
    return sorted(path for path in root.rglob("sync_*.log") if path.is_file())


def _read_logs(paths: Iterable[Path]) -> tuple[list[PageVerdict], int]:
    verdicts: list[PageVerdict] = []
    force_lines = 0
    for path in paths:
        text = path.read_text(encoding="utf-8", errors="replace")
        for line_number, line in enumerate(text.splitlines(), start=1):
            if _FORCE_REFETCH_RE.search(line):
                force_lines += 1
            match = _PAGE_VERDICT_RE.search(line)
            if not match:
                continue
            verdicts.append(
                PageVerdict(
                    page=match.group("page"),
                    status=match.group("status").lower(),
                    rows_written=int(match.group("rows")),
                    source=str(path),
                    line=line_number,
                )
            )
    return verdicts, force_lines


def audit_artifacts(
    root: Path,
    *,
    required_pages: Sequence[str] = CRITICAL_MARKET_PAGES,
) -> AuditResult:
    if not root.exists() or not root.is_dir():
        raise OSError(f"artifact root is not a directory: {root}")

    logs = _candidate_logs(root)
    if not logs:
        raise OSError(f"no sync log files found below: {root}")

    parsed, force_lines = _read_logs(logs)
    required = tuple(required_pages)
    latest_by_page: dict[str, PageVerdict] = {}
    for verdict in parsed:
        if verdict.page in required:
            latest_by_page[verdict.page] = verdict

    observed = tuple(page for page in required if page in latest_by_page)
    missing = tuple(page for page in required if page not in latest_by_page)
    failed = tuple(
        page for page in required
        if page in latest_by_page and not latest_by_page[page].passed
    )
    status = "ok" if not missing and not failed else "blocked"

    return AuditResult(
        status=status,
        required_pages=required,
        observed_pages=observed,
        missing_pages=missing,
        failed_pages=failed,
        force_refetch_evidence_lines=force_lines,
        log_files=tuple(str(path) for path in logs),
        verdicts=tuple(latest_by_page[page] for page in required if page in latest_by_page),
    )


def _parse_pages(raw: str) -> tuple[str, ...]:
    pages = tuple(part.strip() for part in raw.split(",") if part.strip())
    if not pages:
        raise argparse.ArgumentTypeError("at least one required page is needed")
    return pages


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, help="downloaded artifact directory")
    parser.add_argument(
        "--required-pages",
        type=_parse_pages,
        default=CRITICAL_MARKET_PAGES,
        help="comma-separated page names; defaults to all four market universes",
    )
    parser.add_argument("--json-out", help="optional JSON report path")
    args = parser.parse_args(argv)

    try:
        result = audit_artifacts(Path(args.root), required_pages=args.required_pages)
    except OSError as exc:
        print(f"::error::SYNC_ARTIFACT_READ_ERROR: {exc}")
        return 3

    payload = result.to_dict()
    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    print(rendered)
    if args.json_out:
        Path(args.json_out).write_text(rendered + "\n", encoding="utf-8")

    for verdict in result.verdicts:
        annotation = "notice" if verdict.passed else "error"
        print(
            f"::{annotation} file={verdict.source},line={verdict.line}::"
            f"{verdict.page}: status={verdict.status}, rows_written={verdict.rows_written}"
        )
    if result.missing_pages:
        print(f"::error::Missing required page verdicts: {', '.join(result.missing_pages)}")
    if result.failed_pages:
        print(f"::error::Required pages did not refresh: {', '.join(result.failed_pages)}")

    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
