#!/usr/bin/env python3
"""Audit scheduled market-sync artifacts using Freshness Verdict v2.

The dashboard sync intentionally preserves last-good rows during provider
failures. That availability protection must not be mistaken for fresh data.
This auditor understands both the legacy rows-written verdict and the v2
freshness contract. Legacy evidence remains temporarily accepted in shadow mode,
but is reported as incomplete and can be blocked with ``--enforce-v2``.

Exit codes
----------
0  required pages pass the active policy
2  a required page is missing, failed, stale/under-covered, identity-broken, or
   uses legacy/incomplete evidence while v2 enforcement is enabled
3  artifacts could not be read
"""
from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence

from scripts.freshness_verdict_v2 import (
    FreshnessEvidence,
    assess_evidence,
    parse_verdict_line,
)

SCRIPT_VERSION = "2.0.0"
CRITICAL_MARKET_PAGES = (
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
)

_LEGACY_PAGE_VERDICT_RE = re.compile(
    r"\[PAGE-VERDICT(?! v2\.0)[^\]]*\]\s+"
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
    evidence_version: str
    evidence_complete: bool
    enforcement_ready: bool
    passed: bool
    failure_reasons: tuple[str, ...]
    requested: Optional[int] = None
    fresh: Optional[int] = None
    preserved: Optional[int] = None
    stale: Optional[int] = None
    stubs: Optional[int] = None
    identity_failures: Optional[int] = None
    provider_failures: Optional[int] = None
    coverage_pct: Optional[float] = None
    oldest_source_age_h: Optional[float] = None
    newest_source_age_h: Optional[float] = None
    api_units: Optional[float] = None
    api_units_known: bool = False


@dataclass(frozen=True)
class AuditResult:
    status: str
    required_pages: tuple[str, ...]
    observed_pages: tuple[str, ...]
    missing_pages: tuple[str, ...]
    failed_pages: tuple[str, ...]
    incomplete_pages: tuple[str, ...]
    v2_pages: tuple[str, ...]
    enforcement_ready: bool
    force_refetch_evidence_lines: int
    log_files: tuple[str, ...]
    verdicts: tuple[PageVerdict, ...]
    min_fresh_coverage_pct: float
    max_stubs: int
    enforce_v2: bool

    @property
    def exit_code(self) -> int:
        return 0 if self.status == "ok" else 2

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "2.0",
            "script_version": SCRIPT_VERSION,
            "status": self.status,
            "required_pages": list(self.required_pages),
            "observed_pages": list(self.observed_pages),
            "missing_pages": list(self.missing_pages),
            "failed_pages": list(self.failed_pages),
            "incomplete_pages": list(self.incomplete_pages),
            "v2_pages": list(self.v2_pages),
            "enforcement_ready": self.enforcement_ready,
            "force_refetch_evidence_lines": self.force_refetch_evidence_lines,
            "log_files": list(self.log_files),
            "min_fresh_coverage_pct": self.min_fresh_coverage_pct,
            "max_stubs": self.max_stubs,
            "enforce_v2": self.enforce_v2,
            "verdicts": [asdict(item) for item in self.verdicts],
        }


def _candidate_logs(root: Path) -> list[Path]:
    """Prefer one canonical execution log per artifact to avoid duplicates."""
    canonical = sorted(path for path in root.rglob("sync_execution.log") if path.is_file())
    if canonical:
        return canonical
    return sorted(path for path in root.rglob("sync_*.log") if path.is_file())


def _v2_page_verdict(
    evidence: FreshnessEvidence,
    *,
    source: Path,
    line_number: int,
    min_fresh_coverage_pct: float,
    max_stubs: int,
) -> PageVerdict:
    assessment = assess_evidence(
        evidence,
        min_fresh_coverage_pct=min_fresh_coverage_pct,
        max_stubs=max_stubs,
    )
    return PageVerdict(
        page=evidence.page,
        status=evidence.status,
        rows_written=int(evidence.rows_written or 0),
        source=str(source),
        line=line_number,
        evidence_version=evidence.evidence_version,
        evidence_complete=evidence.evidence_complete,
        enforcement_ready=evidence.evidence_complete,
        passed=assessment.passed,
        failure_reasons=assessment.failure_reasons,
        requested=evidence.requested,
        fresh=evidence.fresh,
        preserved=evidence.preserved,
        stale=evidence.stale,
        stubs=evidence.stubs,
        identity_failures=evidence.identity_failures,
        provider_failures=evidence.provider_failures,
        coverage_pct=evidence.coverage_pct,
        oldest_source_age_h=evidence.oldest_source_age_h,
        newest_source_age_h=evidence.newest_source_age_h,
        api_units=evidence.api_units,
        api_units_known=evidence.api_units_known,
    )


def _legacy_page_verdict(
    match: re.Match[str],
    *,
    source: Path,
    line_number: int,
    enforce_v2: bool,
) -> PageVerdict:
    status = match.group("status").lower()
    rows = int(match.group("rows"))
    legacy_pass = status in {"success", "partial"} and rows > 0
    failures: tuple[str, ...]
    if enforce_v2:
        failures = ("legacy_verdict_not_allowed",)
        passed = False
    elif legacy_pass:
        failures = ()
        passed = True
    else:
        failures = ("legacy_status_or_rows_failed",)
        passed = False
    return PageVerdict(
        page=match.group("page"),
        status=status,
        rows_written=rows,
        source=str(source),
        line=line_number,
        evidence_version="1.x",
        evidence_complete=False,
        enforcement_ready=False,
        passed=passed,
        failure_reasons=failures,
    )


def _read_logs(
    paths: Iterable[Path],
    *,
    min_fresh_coverage_pct: float,
    max_stubs: int,
    enforce_v2: bool,
) -> tuple[list[PageVerdict], int]:
    verdicts: list[PageVerdict] = []
    force_lines = 0
    for path in paths:
        text = path.read_text(encoding="utf-8", errors="replace")
        for line_number, line in enumerate(text.splitlines(), start=1):
            if _FORCE_REFETCH_RE.search(line):
                force_lines += 1
            v2 = parse_verdict_line(line)
            if v2 is not None:
                verdicts.append(
                    _v2_page_verdict(
                        v2,
                        source=path,
                        line_number=line_number,
                        min_fresh_coverage_pct=min_fresh_coverage_pct,
                        max_stubs=max_stubs,
                    )
                )
                continue
            legacy = _LEGACY_PAGE_VERDICT_RE.search(line)
            if legacy:
                verdicts.append(
                    _legacy_page_verdict(
                        legacy,
                        source=path,
                        line_number=line_number,
                        enforce_v2=enforce_v2,
                    )
                )
    return verdicts, force_lines


def audit_artifacts(
    root: Path,
    *,
    required_pages: Sequence[str] = CRITICAL_MARKET_PAGES,
    min_fresh_coverage_pct: float = 95.0,
    max_stubs: int = 0,
    enforce_v2: bool = False,
) -> AuditResult:
    if not root.exists() or not root.is_dir():
        raise OSError(f"artifact root is not a directory: {root}")

    logs = _candidate_logs(root)
    if not logs:
        raise OSError(f"no sync log files found below: {root}")

    parsed, force_lines = _read_logs(
        logs,
        min_fresh_coverage_pct=min_fresh_coverage_pct,
        max_stubs=max_stubs,
        enforce_v2=enforce_v2,
    )
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
    incomplete = tuple(
        page for page in required
        if page in latest_by_page and not latest_by_page[page].evidence_complete
    )
    v2_pages = tuple(
        page for page in required
        if page in latest_by_page and latest_by_page[page].evidence_version == "2.0"
    )
    enforcement_ready = not missing and not incomplete and len(v2_pages) == len(required)
    status = "ok" if not missing and not failed else "blocked"

    return AuditResult(
        status=status,
        required_pages=required,
        observed_pages=observed,
        missing_pages=missing,
        failed_pages=failed,
        incomplete_pages=incomplete,
        v2_pages=v2_pages,
        enforcement_ready=enforcement_ready,
        force_refetch_evidence_lines=force_lines,
        log_files=tuple(str(path) for path in logs),
        verdicts=tuple(latest_by_page[page] for page in required if page in latest_by_page),
        min_fresh_coverage_pct=max(0.0, min(100.0, float(min_fresh_coverage_pct))),
        max_stubs=max(0, int(max_stubs)),
        enforce_v2=bool(enforce_v2),
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
    parser.add_argument(
        "--min-fresh-coverage",
        type=float,
        default=95.0,
        help="minimum v2 fresh coverage percentage; default 95",
    )
    parser.add_argument(
        "--max-stubs",
        type=int,
        default=0,
        help="maximum published stub rows allowed by v2; default 0",
    )
    parser.add_argument(
        "--enforce-v2",
        action="store_true",
        help="block legacy PAGE-VERDICT lines; omit while v2 is in shadow rollout",
    )
    parser.add_argument("--json-out", help="optional JSON report path")
    args = parser.parse_args(argv)

    try:
        result = audit_artifacts(
            Path(args.root),
            required_pages=args.required_pages,
            min_fresh_coverage_pct=args.min_fresh_coverage,
            max_stubs=args.max_stubs,
            enforce_v2=args.enforce_v2,
        )
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
        detail = (
            f"status={verdict.status}, rows_written={verdict.rows_written}, "
            f"evidence={verdict.evidence_version}, coverage={verdict.coverage_pct}, "
            f"fresh={verdict.fresh}, preserved={verdict.preserved}, "
            f"stubs={verdict.stubs}, identity_failures={verdict.identity_failures}"
        )
        print(
            f"::{annotation} file={verdict.source},line={verdict.line}::"
            f"{verdict.page}: {detail}"
        )
    if result.incomplete_pages:
        print(
            "::warning::Freshness Verdict v2 shadow evidence is incomplete for: "
            + ", ".join(result.incomplete_pages)
        )
    if result.missing_pages:
        print(f"::error::Missing required page verdicts: {', '.join(result.missing_pages)}")
    if result.failed_pages:
        print(f"::error::Required pages did not refresh: {', '.join(result.failed_pages)}")

    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
