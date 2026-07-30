#!/usr/bin/env python3
"""Freshness Verdict v2 contract and evaluator.

This module is deliberately side-effect free. It does not call providers, write
Google Sheets, or change recommendation logic. The dashboard sync runner can
populate the counters and emit one grep-stable verdict line per page; the
artifact auditor parses the same line and applies the same acceptance rules.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Iterable, Optional

SCHEMA_VERSION = "2.0"
TAG_PREFIX = "[PAGE-VERDICT v2.0]"
_ALLOWED_STATUSES = {"success", "partial", "failed", "skipped"}
_INT_FIELDS = (
    "requested",
    "fresh",
    "preserved",
    "stale",
    "stubs",
    "identity_failures",
    "provider_failures",
    "rows_written",
)
_OPTIONAL_NUMBER_FIELDS = (
    "coverage_pct",
    "oldest_source_age_h",
    "newest_source_age_h",
    "api_units",
)
_TOKEN_RE = re.compile(r"(?P<key>[a-zA-Z_][a-zA-Z0-9_]*)=(?P<value>[^\s]+)")


def _none_token(value: object) -> bool:
    return str(value or "").strip().upper() in {"", "NA", "NONE", "NULL", "UNKNOWN"}


def _parse_int(value: object) -> Optional[int]:
    if _none_token(value):
        return None
    try:
        parsed = int(float(str(value)))
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _parse_float(value: object) -> Optional[float]:
    if _none_token(value):
        return None
    try:
        parsed = float(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _fmt_number(value: Optional[float], digits: int = 2) -> str:
    if value is None:
        return "NA"
    return f"{value:.{digits}f}"


@dataclass(frozen=True)
class FreshnessEvidence:
    page: str
    status: str
    requested: Optional[int]
    fresh: Optional[int]
    preserved: Optional[int]
    stale: Optional[int]
    stubs: Optional[int]
    identity_failures: Optional[int]
    provider_failures: Optional[int]
    rows_written: Optional[int]
    coverage_pct: Optional[float]
    oldest_source_age_h: Optional[float]
    newest_source_age_h: Optional[float]
    api_units: Optional[float]
    evidence_version: str = SCHEMA_VERSION

    @property
    def api_units_known(self) -> bool:
        return self.api_units is not None

    @property
    def evidence_complete(self) -> bool:
        required = (
            self.requested,
            self.fresh,
            self.preserved,
            self.stale,
            self.stubs,
            self.identity_failures,
            self.provider_failures,
            self.rows_written,
            self.coverage_pct,
        )
        return bool(self.page.strip()) and self.status in _ALLOWED_STATUSES and all(
            value is not None for value in required
        )

    def to_dict(self) -> dict[str, object]:
        return asdict(self) | {
            "evidence_complete": self.evidence_complete,
            "api_units_known": self.api_units_known,
        }


@dataclass(frozen=True)
class FreshnessAssessment:
    passed: bool
    failure_reasons: tuple[str, ...]
    min_fresh_coverage_pct: float
    max_stubs: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def build_evidence(
    *,
    page: str,
    status: str,
    requested: int,
    fresh: int,
    preserved: int = 0,
    stale: int = 0,
    stubs: int = 0,
    identity_failures: int = 0,
    provider_failures: int = 0,
    rows_written: int = 0,
    oldest_source_age_h: Optional[float] = None,
    newest_source_age_h: Optional[float] = None,
    api_units: Optional[float] = None,
) -> FreshnessEvidence:
    """Construct evidence and calculate coverage from fresh/requested.

    The four row-state buckets are mutually exclusive by contract. Identity and
    provider failures are event counters and may overlap those row buckets.
    """
    requested_i = max(0, int(requested))
    fresh_i = max(0, int(fresh))
    preserved_i = max(0, int(preserved))
    stale_i = max(0, int(stale))
    stubs_i = max(0, int(stubs))
    if fresh_i + preserved_i + stale_i + stubs_i > requested_i:
        raise ValueError("fresh + preserved + stale + stubs cannot exceed requested")
    coverage = (100.0 * fresh_i / requested_i) if requested_i else None
    return FreshnessEvidence(
        page=str(page or "").strip(),
        status=str(status or "").strip().lower(),
        requested=requested_i,
        fresh=fresh_i,
        preserved=preserved_i,
        stale=stale_i,
        stubs=stubs_i,
        identity_failures=max(0, int(identity_failures)),
        provider_failures=max(0, int(provider_failures)),
        rows_written=max(0, int(rows_written)),
        coverage_pct=coverage,
        oldest_source_age_h=oldest_source_age_h,
        newest_source_age_h=newest_source_age_h,
        api_units=api_units,
    )


def assess_evidence(
    evidence: FreshnessEvidence,
    *,
    min_fresh_coverage_pct: float = 95.0,
    max_stubs: int = 0,
) -> FreshnessAssessment:
    failures: list[str] = []
    threshold = max(0.0, min(100.0, float(min_fresh_coverage_pct)))
    stub_limit = max(0, int(max_stubs))

    if not evidence.evidence_complete:
        failures.append("incomplete_v2_evidence")
    if evidence.status not in {"success", "partial"}:
        failures.append(f"status_{evidence.status or 'unknown'}")
    if not evidence.rows_written:
        failures.append("rows_written_zero_or_unknown")
    if evidence.coverage_pct is None or evidence.coverage_pct < threshold:
        failures.append("fresh_coverage_below_threshold")
    if evidence.identity_failures is None or evidence.identity_failures != 0:
        failures.append("identity_failures_present_or_unknown")
    if evidence.stubs is None or evidence.stubs > stub_limit:
        failures.append("stub_count_above_limit_or_unknown")

    return FreshnessAssessment(
        passed=not failures,
        failure_reasons=tuple(dict.fromkeys(failures)),
        min_fresh_coverage_pct=threshold,
        max_stubs=stub_limit,
    )


def format_verdict_line(evidence: FreshnessEvidence) -> str:
    def iv(value: Optional[int]) -> str:
        return "NA" if value is None else str(value)

    return (
        f"{TAG_PREFIX} page={evidence.page or 'UNKNOWN'} status={evidence.status or 'unknown'} "
        f"requested={iv(evidence.requested)} fresh={iv(evidence.fresh)} "
        f"preserved={iv(evidence.preserved)} stale={iv(evidence.stale)} "
        f"stubs={iv(evidence.stubs)} identity_failures={iv(evidence.identity_failures)} "
        f"provider_failures={iv(evidence.provider_failures)} "
        f"coverage_pct={_fmt_number(evidence.coverage_pct)} "
        f"oldest_source_age_h={_fmt_number(evidence.oldest_source_age_h, 1)} "
        f"newest_source_age_h={_fmt_number(evidence.newest_source_age_h, 1)} "
        f"api_units={_fmt_number(evidence.api_units, 0)} "
        f"rows_written={iv(evidence.rows_written)}"
    )


def parse_verdict_line(line: str) -> Optional[FreshnessEvidence]:
    if TAG_PREFIX not in str(line or ""):
        return None
    tokens = {m.group("key"): m.group("value") for m in _TOKEN_RE.finditer(line)}
    if not tokens:
        return None
    ints = {name: _parse_int(tokens.get(name)) for name in _INT_FIELDS}
    floats = {name: _parse_float(tokens.get(name)) for name in _OPTIONAL_NUMBER_FIELDS}
    return FreshnessEvidence(
        page=str(tokens.get("page") or "").strip(),
        status=str(tokens.get("status") or "").strip().lower(),
        requested=ints["requested"],
        fresh=ints["fresh"],
        preserved=ints["preserved"],
        stale=ints["stale"],
        stubs=ints["stubs"],
        identity_failures=ints["identity_failures"],
        provider_failures=ints["provider_failures"],
        rows_written=ints["rows_written"],
        coverage_pct=floats["coverage_pct"],
        oldest_source_age_h=floats["oldest_source_age_h"],
        newest_source_age_h=floats["newest_source_age_h"],
        api_units=floats["api_units"],
    )


def count_unknown_metrics(evidences: Iterable[FreshnessEvidence]) -> int:
    return sum(1 for item in evidences if not item.api_units_known)
