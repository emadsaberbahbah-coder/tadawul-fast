"""Typed calendar evidence shared by the off-request-path sheet consumers.

Dates remain useful for conservative proximity warnings when provenance is
unknown. Provider reports never establish issuer confirmation. Observation
times describe acquisition, separately from event dates and publication time.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
import re
from typing import Any

CALENDAR_EVIDENCE_VERSION = "1.0.0"
MAX_CALENDAR_BODY_ROWS = 5000
CALENDAR_HEADERS = [
    "Symbol", "Next Earnings Date", "Days To Earnings", "Next Ex-Div Date",
    "Days To ExDiv", "Updated At (Riyadh)", "Source",
    "Earnings Source", "Earnings Observed At (UTC)", "Earnings Evidence Status",
    "ExDiv Source", "ExDiv Observed At (UTC)", "ExDiv Evidence Status",
]
EVENT_FIELDS = (
    ("next_earnings_date", "earnings", "Next Earnings Date"),
    ("next_ex_div_date", "exdiv", "Next Ex-Div Date"),
)


class CalendarEvidenceError(ValueError):
    """The complete calendar table could not be interpreted unambiguously."""


def calendar_date(value: Any) -> str | None:
    text = str(value or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return None
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return None


def normalize_event_evidence(context: dict[str, Any], prefix: str,
                             event_date: Any) -> dict[str, str]:
    """Keep only a complete provider observation bound to a valid event date.

    Incomplete, legacy or unsupported claims retain unknown provenance. A
    free-form Source cell, fetch time or 'confirmed' label is not an attestation.
    """
    keys = [prefix + suffix for suffix in ("_source", "_observed_at", "_status")]
    unknown = dict(zip(keys, ("unknown", "", "unknown")))
    source, observed, status = [str(context.get(k) or "").strip() for k in keys]
    source, status = source.lower(), status.lower()
    if not calendar_date(event_date) or source not in {"eodhd", "yahoo"}:
        return unknown
    if status not in {"reported", "estimated"}:
        return unknown
    # Estimates are supported for Yahoo earnings, never asserted as a declared
    # ex-dividend date or manufactured for EODHD's report-date endpoint.
    if status == "estimated" and (source != "yahoo" or prefix != "earnings"):
        return unknown
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)", observed):
        return unknown
    try:
        dt = datetime.fromisoformat(observed.replace("Z", "+00:00"))
    except ValueError:
        return unknown
    if (dt.tzinfo is None or dt.utcoffset() != timezone.utc.utcoffset(dt)
            or dt > datetime.now(timezone.utc)):
        return unknown
    return dict(zip(keys, (source, observed, status)))


def parse_calendar_values(values: list[list[Any]]) -> dict[str, dict[str, Any]]:
    """Parse one bounded complete table, including legacy seven-column rows.

    Invalid dates do not fall back to static countdowns. Conflicting duplicate
    symbols are excluded rather than choosing an arbitrary source generation.
    """
    if not values:
        return {}
    if len(values) > MAX_CALENDAR_BODY_ROWS + 1:
        raise CalendarEvidenceError("calendar exceeds full-table capacity")
    headers = [str(v or "").strip() for v in values[0]]
    named = [h for h in headers if h]
    if len(named) != len({h.casefold() for h in named}):
        raise CalendarEvidenceError("duplicate calendar headers")
    if any(h not in headers for h in ("Symbol", "Next Earnings Date", "Next Ex-Div Date")):
        raise CalendarEvidenceError("missing calendar date headers")
    hmap = {h: i for i, h in enumerate(headers) if h}

    def cell(row: list[Any], name: str) -> Any:
        index = hmap.get(name)
        return row[index] if index is not None and index < len(row) else ""

    out: dict[str, dict[str, Any]] = {}
    conflicts: set[str] = set()
    for row in values[1:]:
        sym = str(cell(row, "Symbol") or "").strip().upper()
        if not re.fullmatch(r"[A-Z0-9]{1,8}(?:[-.][A-Z0-9]{1,3})?\.[A-Z]{1,4}", sym):
            continue
        ctx: dict[str, Any] = {}
        for key, prefix, heading in EVENT_FIELDS:
            ctx[key] = calendar_date(cell(row, heading))
            title = "Earnings" if prefix == "earnings" else "ExDiv"
            ctx.update(normalize_event_evidence({
                prefix + "_source": cell(row, title + " Source"),
                prefix + "_observed_at": cell(row, title + " Observed At (UTC)"),
                prefix + "_status": cell(row, title + " Evidence Status"),
            }, prefix, ctx[key]))
        if sym in out and out[sym] != ctx:
            conflicts.add(sym)
        elif sym not in conflicts:
            out[sym] = ctx
    for sym in conflicts:
        out.pop(sym, None)
    return out
