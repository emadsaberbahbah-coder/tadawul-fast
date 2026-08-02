#!/usr/bin/env python3
"""Derive Freshness Verdict v2 evidence from existing sync artifacts.

This is a transitional, read-only adapter for the shadow rollout. It does not
call providers, write Google Sheets, or alter the dashboard sync exit code. It
reads the runner's existing telemetry and appends one v2 verdict line per page
when ``--append`` is supplied.

Evidence that the legacy runner cannot prove remains ``NA``. In particular,
row-level stale counts, oldest source age, and weighted API units are not
invented. They will become complete when the operational store records them.
"""
from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict
from pathlib import Path
from typing import Iterable, Optional, Sequence

try:  # Supports both ``python -m scripts...`` and ``python scripts/...py``.
    from scripts.freshness_verdict_v2 import (
        FreshnessEvidence,
        TAG_PREFIX,
        format_verdict_line,
    )
except ModuleNotFoundError:  # pragma: no cover - exercised by workflow invocation
    from freshness_verdict_v2 import (  # type: ignore
        FreshnessEvidence,
        TAG_PREFIX,
        format_verdict_line,
    )

SCRIPT_VERSION = "1.0.0"
_MARKET_PAGES = {
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
}

_LEGACY_RE = re.compile(
    r"\[PAGE-VERDICT(?! v2\.0)[^\]]*\]\s+"
    r"page=(?P<page>\S+)\s+status=(?P<status>\S+)\s+"
    r"rows_written=(?P<rows>\d+)\s+"
    r"newest_stamp_age_h=(?P<newest>\S+)\s+reason=(?P<reason>.*)$"
)
_FLOOR_RE = re.compile(
    r"FLOOR-MERGE.*?Partial fetch on ['\"](?P<page>[^'\"]+)['\"]:\s*"
    r"(?P<fresh>\d+) fresh row\(s\) for (?P<requested>\d+) requested",
    re.IGNORECASE,
)
_PERSIST_RE = re.compile(
    r"PERSISTENCE[^\]]*\].*?preserved (?P<count>\d+) last-good row\(s\).*?"
    r"on ['\"](?P<page>[^'\"]+)['\"]",
    re.IGNORECASE,
)
_KLG_RE = re.compile(
    r"KEEP-LAST-GOOD[^\]]*\].*?substituted (?P<count>\d+).*?"
    r"on ['\"](?P<page>[^'\"]+)['\"]",
    re.IGNORECASE,
)
_FW_KEEP_RE = re.compile(
    r"FW-KEEP[^\]]*\].*?['\"](?P<page>[^'\"]+)['\"]:\s*restored "
    r"(?P<restored>\d+)/(?P<total>\d+).*?"
    r"(?:(?P<unrestored>\d+) had no last-good \(left as stub\))?",
    re.IGNORECASE,
)
_IDFW_RE = re.compile(
    r"ID-FIREWALL[^\]]*\].*?quarantined (?P<count>\d+).*?"
    r"on ['\"](?P<page>[^'\"]+)['\"]",
    re.IGNORECASE,
)
_CRITICAL_RE = re.compile(
    r"CRITICAL-IDENTITY[^\]]*\].*?quarantined (?P<count>\d+).*?"
    r"on ['\"](?P<page>[^'\"]+)['\"]",
    re.IGNORECASE,
)
_BATCH_REASON_RE = re.compile(
    r"\[SYMBOL-BATCH[^\]]*\]\s*fetched (?P<requested>\d+) symbol\(s\) in "
    r"(?P<ok>\d+)/(?P<total>\d+) batch\(es\)",
    re.IGNORECASE,
)
_TIME_BUDGET_REASON_RE = re.compile(
    r"TIME-BUDGET[^\]]*\]\s*[^:]+:\s*budget .*? exhausted after "
    r"(?P<ok>\d+)/(?P<total>\d+) batch\(es\)",
    re.IGNORECASE,
)


def _candidate_logs(root: Path) -> list[Path]:
    canonical = sorted(path for path in root.rglob("sync_execution.log") if path.is_file())
    if canonical:
        return canonical
    return sorted(path for path in root.rglob("sync_*.log") if path.is_file())


def _number_or_none(value: object) -> Optional[float]:
    text = str(value or "").strip().upper()
    if text in {"", "NA", "NONE", "NULL", "UNKNOWN"}:
        return None
    try:
        parsed = float(text)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


def _sum_by_page(pattern: re.Pattern[str], text: str, field: str = "count") -> dict[str, int]:
    out: dict[str, int] = {}
    for match in pattern.finditer(text):
        page = match.group("page")
        out[page] = out.get(page, 0) + int(match.group(field))
    return out


def derive_from_text(text: str) -> list[FreshnessEvidence]:
    """Return the latest derived v2 evidence for each market page in one log."""
    legacy: dict[str, re.Match[str]] = {}
    for line in str(text or "").splitlines():
        match = _LEGACY_RE.search(line)
        if match and match.group("page") in _MARKET_PAGES:
            legacy[match.group("page")] = match

    floor: dict[str, tuple[int, int]] = {}
    for match in _FLOOR_RE.finditer(text):
        floor[match.group("page")] = (
            int(match.group("fresh")),
            int(match.group("requested")),
        )

    persisted = _sum_by_page(_PERSIST_RE, text)
    kept_last_good = _sum_by_page(_KLG_RE, text)
    quarantined = _sum_by_page(_IDFW_RE, text)
    critical = _sum_by_page(_CRITICAL_RE, text)

    restored: dict[str, int] = {}
    unresolved_stubs: dict[str, int] = {}
    for match in _FW_KEEP_RE.finditer(text):
        page = match.group("page")
        restored[page] = restored.get(page, 0) + int(match.group("restored"))
        unresolved = match.group("unrestored")
        if unresolved is None:
            unresolved = str(max(0, int(match.group("total")) - int(match.group("restored"))))
        unresolved_stubs[page] = unresolved_stubs.get(page, 0) + int(unresolved)

    output: list[FreshnessEvidence] = []
    for page, match in legacy.items():
        status = match.group("status").lower()
        rows_written = int(match.group("rows"))
        newest_age = _number_or_none(match.group("newest"))
        reason = match.group("reason") or ""

        requested: Optional[int] = None
        fresh_hint: Optional[int] = None
        if page in floor:
            fresh_hint, requested = floor[page]

        batch_match = _BATCH_REASON_RE.search(reason)
        if batch_match:
            requested = int(batch_match.group("requested"))
            provider_failures: Optional[int] = max(
                0, int(batch_match.group("total")) - int(batch_match.group("ok"))
            )
        else:
            time_match = _TIME_BUDGET_REASON_RE.search(reason)
            provider_failures = (
                max(0, int(time_match.group("total")) - int(time_match.group("ok")))
                if time_match
                else None
            )

        preserved = persisted.get(page, 0) + kept_last_good.get(page, 0) + restored.get(page, 0)
        stubs = max(unresolved_stubs.get(page, 0), critical.get(page, 0))
        identity_failures = max(quarantined.get(page, 0), critical.get(page, 0), stubs)

        if requested is None:
            requested = max(rows_written, preserved + stubs)
        requested = max(0, requested)
        preserved = min(max(0, preserved), requested)
        stubs = min(max(0, stubs), max(0, requested - preserved))

        if fresh_hint is not None:
            fresh = max(0, min(fresh_hint, requested - preserved - stubs))
        else:
            # rows_written is the final matrix after persistence/KLG substitutions.
            fresh = max(0, min(rows_written - preserved - stubs, requested - preserved - stubs))

        coverage = (100.0 * fresh / requested) if requested else None
        output.append(
            FreshnessEvidence(
                page=page,
                status=status,
                requested=requested,
                fresh=fresh,
                preserved=preserved,
                stale=None,  # Legacy logs do not prove row-level stale age.
                stubs=stubs,
                identity_failures=identity_failures,
                provider_failures=provider_failures,
                rows_written=rows_written,
                coverage_pct=coverage,
                oldest_source_age_h=None,
                newest_source_age_h=newest_age,
                api_units=None,
            )
        )
    return output


def _replace_v2_lines(text: str, evidences: Sequence[FreshnessEvidence]) -> str:
    """Replace existing v2 lines so repeated workflow runs are idempotent."""
    kept = [line for line in str(text or "").splitlines() if TAG_PREFIX not in line]
    kept.extend(format_verdict_line(item) for item in evidences)
    return "\n".join(kept).rstrip() + "\n"


def derive_artifacts(root: Path, *, append: bool = False) -> tuple[list[FreshnessEvidence], list[str]]:
    if not root.exists() or not root.is_dir():
        raise OSError(f"artifact root is not a directory: {root}")
    logs = _candidate_logs(root)
    if not logs:
        raise OSError(f"no sync log files found below: {root}")

    all_evidence: list[FreshnessEvidence] = []
    touched: list[str] = []
    for path in logs:
        text = path.read_text(encoding="utf-8", errors="replace")
        evidence = derive_from_text(text)
        all_evidence.extend(evidence)
        if append and evidence:
            path.write_text(_replace_v2_lines(text, evidence), encoding="utf-8")
            touched.append(str(path))
    return all_evidence, touched


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, help="downloaded sync artifact directory")
    parser.add_argument("--append", action="store_true", help="append/replace v2 lines in logs")
    parser.add_argument("--json-out", help="optional JSON evidence report")
    args = parser.parse_args(argv)

    try:
        evidences, touched = derive_artifacts(Path(args.root), append=args.append)
    except OSError as exc:
        print(f"::error::FRESHNESS_V2_DERIVATION_ERROR: {exc}")
        return 3

    payload = {
        "schema_version": "2.0-shadow",
        "script_version": SCRIPT_VERSION,
        "append": bool(args.append),
        "files_touched": touched,
        "evidence": [
            item.to_dict() | {"verdict_line": format_verdict_line(item)}
            for item in evidences
        ],
    }
    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    print(rendered)
    if args.json_out:
        Path(args.json_out).write_text(rendered + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
