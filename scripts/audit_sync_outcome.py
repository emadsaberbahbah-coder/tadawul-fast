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


SCRIPT_VERSION = "1.1.0"
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

# --- v1.1.0 FULL-FETCH CRITERION (kill-switch: TFB_AUDIT_REQUIRE_FULL_FETCH) --
# WHY (run 30891848376, 2026-08-04): Global_Markets exhausted its 3600s budget
# at 73/239 batches (30% fresh coverage), the persistence merge restored 4,162
# last-good rows, PAGE-VERDICT reported status=success rows_written=5943, and
# this audit therefore declared the page refreshed — inline recovery never
# fired. Process success and DATA-COMPLETE success are different facts. These
# patterns read the runner's own budget/coverage warnings so the audit can,
# when armed, treat an incomplete fetch as a failed page and hand it to the
# existing recovery matrix. Default OFF => v1.0.0 verdicts byte-identical.
_TIME_BUDGET_RE = re.compile(
    r"\[v[\d.]+ TIME-BUDGET\]\s+(?P<page>\S+): budget \d+s exhausted after "
    r"(?P<done>\d+)/(?P<total>\d+) batch"
)
_FLOOR_MERGE_RE = re.compile(
    r"\[v[\d.]+ FLOOR-MERGE\] Partial fetch on '(?P<page>[^']+)': "
    r"(?P<fresh>\d+) fresh row\(s\) for (?P<req>\d+) requested "
    r"\((?P<pct>\d+)% coverage"
)
_UNRECOVERED_RE = re.compile(
    r"\[v[\d.]+ BATCH-RETRY\]\s+(?P<page>\S+): budget ended with "
    r"(?P<n>\d+) failed batch\(es\) still unrecovered"
)
import os as _os


def _require_full_fetch() -> bool:
    """v1.1.0 master switch. Default OFF; TFB_AUDIT_REQUIRE_FULL_FETCH=1/true/
    on/yes arms the data-complete criterion. OFF => v1.0.0 byte-identical."""
    return (_os.getenv("TFB_AUDIT_REQUIRE_FULL_FETCH") or "0").strip().lower() in {"1", "true", "on", "yes"}


def _min_fresh_pct() -> int:
    """v1.1.0: minimum acceptable fresh-fetch coverage percent (default 95)."""
    try:
        return max(0, min(100, int((_os.getenv("TFB_AUDIT_MIN_FRESH_PCT") or "95").strip())))
    except ValueError:
        return 95


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
    # v1.1.0 (defaulted => constructor-compatible with v1.0.0 callers):
    incomplete_pages: tuple[str, ...] = ()
    fetch_evidence: tuple = ()
    full_fetch_gate: bool = False
    min_fresh_pct: int = 95

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
            "incomplete_pages": list(self.incomplete_pages),
            "fetch_evidence": [dict(item) for item in self.fetch_evidence],
            "full_fetch_gate": self.full_fetch_gate,
            "min_fresh_pct": self.min_fresh_pct,
            "log_files": list(self.log_files),
            "verdicts": [asdict(item) | {"passed": item.passed} for item in self.verdicts],
        }


def _candidate_logs(root: Path) -> list[Path]:
    """Prefer one canonical execution log per artifact to avoid duplicates."""
    canonical = sorted(path for path in root.rglob("sync_execution.log") if path.is_file())
    if canonical:
        return canonical
    return sorted(path for path in root.rglob("sync_*.log") if path.is_file())


def _read_logs(paths: Iterable[Path]) -> tuple[list[PageVerdict], int, dict]:
    verdicts: list[PageVerdict] = []
    force_lines = 0
    fetch_evidence: dict[str, dict] = {}
    for path in paths:
        text = path.read_text(encoding="utf-8", errors="replace")
        for line_number, line in enumerate(text.splitlines(), start=1):
            if _FORCE_REFETCH_RE.search(line):
                force_lines += 1
            tb = _TIME_BUDGET_RE.search(line)
            if tb:
                ev = fetch_evidence.setdefault(tb.group("page"), {"page": tb.group("page")})
                ev["batches_done"] = int(tb.group("done"))
                ev["batches_total"] = int(tb.group("total"))
            fm = _FLOOR_MERGE_RE.search(line)
            if fm:
                ev = fetch_evidence.setdefault(fm.group("page"), {"page": fm.group("page")})
                ev["fresh_rows"] = int(fm.group("fresh"))
                ev["requested"] = int(fm.group("req"))
                ev["fresh_pct"] = int(fm.group("pct"))
            ur = _UNRECOVERED_RE.search(line)
            if ur:
                ev = fetch_evidence.setdefault(ur.group("page"), {"page": ur.group("page")})
                ev["unrecovered_batches"] = int(ur.group("n"))
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
    return verdicts, force_lines, fetch_evidence


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

    parsed, force_lines, fetch_evidence = _read_logs(logs)
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
    # v1.1.0: pages whose fetch demonstrably did not complete this run.
    gate = _require_full_fetch()
    min_pct = _min_fresh_pct()
    incomplete: list[str] = []
    for page in required:
        ev = fetch_evidence.get(page)
        if not ev or page not in latest_by_page:
            continue
        short_budget = (
            "batches_total" in ev and ev.get("batches_done", 0) < ev["batches_total"]
        )
        low_fresh = ("fresh_pct" in ev and ev["fresh_pct"] < min_pct)
        unrecovered = ev.get("unrecovered_batches", 0) > 0
        if short_budget or low_fresh or unrecovered:
            incomplete.append(page)
    if gate:
        failed = tuple(dict.fromkeys(tuple(failed) + tuple(incomplete)))
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
        incomplete_pages=tuple(incomplete),
        fetch_evidence=tuple(fetch_evidence[k] for k in sorted(fetch_evidence)),
        full_fetch_gate=gate,
        min_fresh_pct=min_pct,
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
    if result.incomplete_pages:
        level = "error" if result.full_fetch_gate else "warning"
        print(
            f"::{level}::Pages with incomplete fresh fetch "
            f"(gate={'on' if result.full_fetch_gate else 'off'}, "
            f"min {result.min_fresh_pct}%): {', '.join(result.incomplete_pages)}"
        )

    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
