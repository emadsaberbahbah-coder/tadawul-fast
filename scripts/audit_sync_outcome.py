#!/usr/bin/env python3
"""Fail closed when scheduled market sync artifacts show skipped core pages.

The dashboard sync runner intentionally preserves last-good sheet data when a
provider fetch is incomplete. That safety behavior is correct, but the runner
can still exit zero after writing no rows for a required market page. This
read-only auditor parses the uploaded sync logs and separates process success
from data-refresh success.

Exit codes
----------
0  every required market page was observed and passed the armed criteria
2  a required page was missing, skipped, failed, wrote zero rows, or could
   not certify the configured fresh-coverage minimum while the criterion was armed
3  artifacts could not be read
"""
from __future__ import annotations

import argparse
import json
import re
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

SCRIPT_VERSION = "1.1.3"
# v1.1.3 (2026-10-16): an empty/truncated later recovery attempt must not
# reuse an earlier successful verdict. Scope actual attempt logs/receipts to
# their own artifact/page; absent artifacts and legacy primary logs retain
# their existing behavior. Conflicting or unavailable recovery scope is a
# read error, never guessed coverage. No freshness thresholds change.
# v1.1.2 (2026-10-06): full-fetch mode now rejects partial verdicts and
# explicit unknown exact coverage, while legacy no-coverage logs remain
# compatible. Canonical-log preference is local to each artifact directory.
# v1.1.1 (2026-10-06): parse exact PAGE-VERDICT coverage, compare the integer
# ratio without display rounding, and make recovery verdict/evidence ordering
# attempt-aware with later clean attempts clearing stale failure evidence.
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
_PAGE_FRESH_RE = re.compile(
    r"\bfresh_rows=(?P<fresh>\d+|NA)\s+"
    r"requested_rows=(?P<requested>\d+|NA)\s+"
    r"fresh_pct=(?P<pct>\d+(?:\.\d+)?|NA)\b",
    re.IGNORECASE,
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


def _verdict_passed(verdict: PageVerdict, full_fetch_gate: bool) -> bool:
    """Apply the gate-specific status contract to one parsed verdict.

    Legacy/unarmed audits retain the original soft-pass treatment for a
    positive-row ``partial`` result.  Once the full-fetch criterion is armed,
    only ``success`` can certify a data-complete refresh.
    """
    return verdict.passed and (
        not full_fetch_gate or verdict.status.lower() == "success"
    )


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
            "verdicts": [
                asdict(item)
                | {"passed": _verdict_passed(item, self.full_fetch_gate)}
                for item in self.verdicts
            ],
        }


def _candidate_logs(root: Path) -> list[Path]:
    """Prefer the canonical execution log within each artifact directory.

    Recovery cycle directories represent ordered attempts. Strip ``cycleN``
    from the logical path for ordering, then sort the base attempt before
    cycle 2, cycle 3, and so on. Plain lexical sorting puts ``cycle2`` before
    ``sync_execution.log`` and can let an older verdict overwrite recovery.

    Canonical preference is deliberately local to a directory.  A different
    artifact may have been interrupted before creating ``sync_execution.log``
    while still carrying its usable timestamped ``sync_*.log``.
    """

    def order_key(
        path: Path,
    ) -> tuple[tuple[str, ...], int, str, tuple[str, ...]]:
        try:
            parts = path.relative_to(root).parts
        except ValueError:
            parts = path.parts
        logical_parent: list[str] = []
        cycle = 1
        for part in parts[:-1]:
            match = re.fullmatch(r"cycle(\d+)", part, re.IGNORECASE)
            if match:
                cycle = max(cycle, int(match.group(1)))
            else:
                logical_parent.append(part)
        filename = parts[-1] if parts else ""
        return tuple(logical_parent), cycle, filename, tuple(parts)

    by_parent: dict[Path, list[Path]] = {}
    for path in root.rglob("sync_*.log"):
        if path.is_file():
            by_parent.setdefault(path.parent, []).append(path)
    # These per-attempt files are emitted only after launching the child, not
    # for quota skips. Their presence without a log proves missing evidence;
    # a success claim in JSON cannot substitute for a terminal log verdict.
    for filename in ("sync_runner_receipt.json", "page-audit.json"):
        for path in root.rglob(filename):
            if path.is_file() and path.parent not in by_parent:
                by_parent[path.parent] = [path.parent / "sync_execution.log"]

    selected: list[Path] = []
    for paths in by_parent.values():
        canonical = [p for p in paths if p.name == "sync_execution.log"]
        selected.extend(canonical or paths)
    return sorted(selected, key=order_key)


_RECOVERY_GROUP_PAGES = {
    "global-markets": "Global_Markets", "commodities-fx": "Commodities_FX",
    "mutual-funds": "Mutual_Funds", "market-leaders": "Market_Leaders",
}
_RECOVERY_GROUP_PAGES.update({page: page for page in CRITICAL_MARKET_PAGES})
_RECOVERY_ARTIFACT_RE = re.compile(r"^page-refresh-(\d+)-([A-Za-z0-9_-]+)$")
_PRIMARY_ARTIFACT_RE = re.compile(r"^tadawul-sync-logs-(\d+)-.+$")
_CYCLE_DIRECTORY_RE = re.compile(r"^cycle\d+$", re.IGNORECASE)
_ATTEMPT_RECEIPTS = ("sync_runner_receipt.json", "page-audit.json")
_MAX_ATTEMPT_RECEIPT_BYTES = 4 * 1024 * 1024


def _attempt_family(path: Path, root: Path) -> Path:
    """The exact logical artifact parent, stripping only cycle directories."""
    parent = path.parent
    while parent != root and _CYCLE_DIRECTORY_RE.fullmatch(parent.name):
        parent = parent.parent
    return parent


def _inside_artifact_root(path: Path, root: Path) -> None:
    if not path.resolve().is_relative_to(root.resolve()):
        raise OSError("sync attempt evidence escapes the artifact root")


def _attempt_receipt_pages(directory: Path, root: Path) -> set[str]:
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate receipt field")
            result[key] = value
        return result

    pages: set[str] = set()
    for filename in _ATTEMPT_RECEIPTS:
        path = directory / filename
        if not path.is_file():
            continue
        _inside_artifact_root(path, root)
        try:
            if path.stat().st_size > _MAX_ATTEMPT_RECEIPT_BYTES:
                raise ValueError("oversized receipt")
            raw = path.read_bytes()
            if len(raw) > _MAX_ATTEMPT_RECEIPT_BYTES:
                raise ValueError("oversized receipt")
            payload = json.loads(raw, object_pairs_hook=unique_object)
            if not isinstance(payload, dict):
                raise TypeError("invalid receipt")
            if filename == "sync_runner_receipt.json":
                results = payload.get("results")
                if not isinstance(results, list) or not results or any(not isinstance(item, dict) for item in results):
                    raise ValueError("receipt does not scope one child page")
                declared = [item.get("sheet_name") for item in results]
            else:
                required = payload.get("required_pages")
                if not isinstance(required, list) or not required:
                    raise ValueError("receipt does not scope one audited page")
                declared = required
            if any(not isinstance(page, str) or page not in CRITICAL_MARKET_PAGES for page in declared):
                raise ValueError("receipt page is invalid")
            pages.update(declared)
        except (OSError, ValueError, TypeError, UnicodeError, RecursionError):
            # Optional diagnostics are not a completion criterion. Malformed
            # JSON supplies no page authority; current path/terminal evidence
            # may still bind the attempt. Valid contrary claims remain in
            # `pages` and are rejected by the scope comparison below.
            continue
    return pages


def _validate_artifact_run_ids(paths: Sequence[Path], root: Path) -> None:
    primary_ids, recovery_ids = set(), set()
    for path in paths:
        for part in (root.name, *path.relative_to(root).parts[:-1]):
            primary = _PRIMARY_ARTIFACT_RE.fullmatch(part)
            recovery = _RECOVERY_ARTIFACT_RE.fullmatch(part)
            if primary:
                primary_ids.add(primary.group(1))
            if recovery:
                recovery_ids.add(recovery.group(1))
    if len(primary_ids | recovery_ids) > 1:
        raise OSError("sync artifact source run IDs conflict")


def _recovery_attempt_scope(path: Path, matches: Sequence[re.Match],
                            family_pages: dict[Path, str], required_pages: Sequence[str], root: Path) -> str | None:
    family = _attempt_family(path, root)
    artifact = _RECOVERY_ARTIFACT_RE.fullmatch(family.name)
    group_page = _RECOVERY_GROUP_PAGES.get(family.name)
    has_receipt = any((directory / name).is_file()
                      for directory in {family, path.parent} for name in _ATTEMPT_RECEIPTS)
    scoped_group = group_page and (
        tuple(required_pages) == (group_page,)
        or family.parent.name == "inline-recovery-evidence"
        or family.parent.name == "zz-recovery"
    )
    if not (artifact or family.name == "zz-recovery" or scoped_group or has_receipt
            or _CYCLE_DIRECTORY_RE.fullmatch(root.name)):
        return None  # An unrelated legacy primary layout is not a recovery.
    candidates = set()
    if artifact:
        mapped_page = _RECOVERY_GROUP_PAGES.get(artifact.group(2))
        if mapped_page:
            candidates.add(mapped_page)
    if scoped_group:
        candidates.add(group_page)
    if family in family_pages:
        candidates.add(family_pages[family])
    candidates.update(_attempt_receipt_pages(path.parent, root))
    # A flattened single-artifact download can establish its own scope from
    # its cycle-one log. No verdict from another artifact is consulted.
    candidates.update(match.group("page") for match in matches)
    if not candidates and family != path.parent:
        # Consult older files only when the current attempt cannot bind
        # itself, and only within this audit's own artifact root.
        candidates.update(_attempt_receipt_pages(family, root))
        base_log = family / "sync_execution.log"
        if base_log.is_file():
            _inside_artifact_root(base_log, root)
            candidates.update(match.group("page") for line in base_log.read_text(
                encoding="utf-8", errors="replace").splitlines()
                if (match := _PAGE_VERDICT_RE.search(line)))
    if len(candidates) != 1 or not candidates <= set(CRITICAL_MARKET_PAGES):
        raise OSError("recovery attempt page scope is unavailable or conflicting")
    page = next(iter(candidates))
    family_pages[family] = page
    return page


def _read_logs(paths: Iterable[Path], *, root: Path, required_pages: Sequence[str] = CRITICAL_MARKET_PAGES
               ) -> tuple[list[PageVerdict], int, dict]:
    verdicts: list[PageVerdict] = []
    force_lines = 0
    # v1.1.1: completeness evidence follows the same last-attempt-wins
    # contract as PAGE-VERDICT. Recovery artifacts sort after primary logs;
    # a clean recovery verdict must therefore clear an earlier attempt's
    # TIME-BUDGET/FLOOR-MERGE evidence instead of inheriting it.
    fetch_evidence: dict[str, dict] = {}
    family_pages: dict[Path, str] = {}
    for path in paths:
        pending_evidence: dict[str, dict] = {}
        _inside_artifact_root(path, root)
        text = path.read_text(encoding="utf-8", errors="replace") if path.is_file() else ""
        lines = text.splitlines()
        matches = [match for line in lines if (match := _PAGE_VERDICT_RE.search(line))]
        expected_page = _recovery_attempt_scope(path, matches, family_pages, required_pages, root)
        for line_number, line in enumerate(lines, start=1):
            if _FORCE_REFETCH_RE.search(line):
                force_lines += 1
            tb = _TIME_BUDGET_RE.search(line)
            if tb:
                ev = pending_evidence.setdefault(tb.group("page"), {"page": tb.group("page")})
                ev["batches_done"] = int(tb.group("done"))
                ev["batches_total"] = int(tb.group("total"))
            fm = _FLOOR_MERGE_RE.search(line)
            if fm:
                ev = pending_evidence.setdefault(fm.group("page"), {"page": fm.group("page")})
                ev["fresh_rows"] = int(fm.group("fresh"))
                ev["requested"] = int(fm.group("req"))
                ev["fresh_pct"] = int(fm.group("pct"))
            ur = _UNRECOVERED_RE.search(line)
            if ur:
                ev = pending_evidence.setdefault(ur.group("page"), {"page": ur.group("page")})
                ev["unrecovered_batches"] = int(ur.group("n"))
            match = _PAGE_VERDICT_RE.search(line)
            if not match:
                continue
            page = match.group("page")
            exact = _PAGE_FRESH_RE.search(line)
            if exact:
                values = exact.groupdict()
                ev = pending_evidence.setdefault(page, {"page": page})
                ev["coverage_source"] = "page_verdict"
                ev["coverage_unknown"] = any(
                    values[key].upper() == "NA"
                    for key in ("fresh", "requested", "pct")
                )
                if values["fresh"].upper() != "NA":
                    ev["fresh_rows"] = int(values["fresh"])
                if values["requested"].upper() != "NA":
                    ev["requested"] = int(values["requested"])
                if values["pct"].upper() != "NA":
                    ev["fresh_pct"] = float(values["pct"])
            verdicts.append(
                PageVerdict(
                    page=page,
                    status=match.group("status").lower(),
                    rows_written=int(match.group("rows")),
                    source=str(path),
                    line=line_number,
                )
            )
            evidence = pending_evidence.pop(page, None)
            if evidence:
                fetch_evidence[page] = {
                    **evidence,
                    "source": str(path),
                    "verdict_line": line_number,
                }
            else:
                # A later clean attempt supersedes incomplete evidence from
                # the original leg. Absence of these warnings means only
                # "not demonstrably incomplete", matching the gate's scope.
                fetch_evidence.pop(page, None)
        if expected_page is not None and not matches:
            # Positive attempt evidence without a terminal verdict is unknown,
            # even if the previous cycle passed or its JSON says success.
            # This diagnostic replaces prior coverage as well as its verdict.
            evidence_source = path
            if not path.is_file():
                evidence_source = next((path.parent / name for name in _ATTEMPT_RECEIPTS
                                        if (path.parent / name).is_file()), path)
            line = max(1, len(text.splitlines()))
            verdicts.append(PageVerdict(expected_page, "unverified_attempt", 0, str(evidence_source), line))
            fetch_evidence[expected_page] = {
                "page": expected_page, "source": str(evidence_source), "verdict_line": line,
                "coverage_source": "recovery_attempt", "coverage_unknown": True,
                "attempt_verdict_missing": True,
                "attempt_log_missing": not path.is_file(),
            }
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

    _validate_artifact_run_ids(logs, root)
    parsed, force_lines, fetch_evidence = _read_logs(logs, root=root, required_pages=required_pages)
    required = tuple(required_pages)
    latest_by_page: dict[str, PageVerdict] = {}
    for verdict in parsed:
        if verdict.page in required:
            latest_by_page[verdict.page] = verdict

    observed = tuple(page for page in required if page in latest_by_page)
    missing = tuple(page for page in required if page not in latest_by_page)
    gate = _require_full_fetch()
    failed = tuple(
        page for page in required
        if page in latest_by_page
        and not _verdict_passed(latest_by_page[page], gate)
    )
    # v1.1.0: pages whose fetch demonstrably did not complete this run.
    min_pct = _min_fresh_pct()
    incomplete: list[str] = []
    for page in required:
        ev = fetch_evidence.get(page)
        if not ev or page not in latest_by_page:
            continue
        short_budget = (
            "batches_total" in ev and ev.get("batches_done", 0) < ev["batches_total"]
        )
        if ev.get("requested", 0) > 0 and "fresh_rows" in ev:
            # Compare the exact ratio. PAGE-VERDICT carries four-decimal
            # display telemetry, while the integer numerator/denominator keep
            # the threshold decision free of rounding artifacts.
            low_fresh = (
                int(ev["fresh_rows"]) * 100
                < min_pct * int(ev["requested"])
            )
        else:
            low_fresh = (
                "fresh_pct" in ev and float(ev["fresh_pct"]) < min_pct
            )
        unrecovered = ev.get("unrecovered_batches", 0) > 0
        # Legacy PAGE-VERDICT lines carried no exact coverage fields and remain
        # backward-compatible.  A modern line that explicitly reports any
        # exact field as NA has supplied evidence that completeness is unknown;
        # the armed full-fetch contract cannot certify it as complete.
        unknown_exact = (
            ev.get("coverage_source") == "page_verdict"
            and bool(ev.get("coverage_unknown"))
        )
        if short_budget or low_fresh or unrecovered or unknown_exact:
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
        log_files=tuple(str(path) for path in logs if path.is_file()),
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
        annotation = (
            "notice"
            if _verdict_passed(verdict, result.full_fetch_gate)
            else "error"
        )
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
