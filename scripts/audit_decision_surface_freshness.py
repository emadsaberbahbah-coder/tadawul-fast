#!/usr/bin/env python3
"""Read-only, fail-closed audit of investment decision surfaces.

The workbook can contain individually valid pages built from different
snapshots. This audit prevents an old ``Portfolio_Decision`` or a Top-10 page
built from partial/stale source universes from looking executable merely
because its own status text says ``ok``.

No provider call and no Google Sheet write is performed.
"""
from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import math
import os
import re
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

for _path in (Path(__file__).resolve().parent, Path(__file__).resolve().parent.parent):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from scripts.audit_full_refresh_coverage import parse_dt, resolve_reader, s  # noqa: E402

VERSION = "1.0.0"
GOOD_FULL_PAGE_STATUSES = {"OK", "SUCCESS", "VALID", "PASS", "COMPLETE"}
RUN_RE = re.compile(
    r"Last\s+run\s+(?P<stamp>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})"
    r"\s*\|\s*status:\s*(?P<status>[A-Za-z_]+)",
    re.I,
)
POOL_RE = re.compile(r"(?P<page>[A-Za-z][A-Za-z0-9_]+)\s+(?P<used>\d+)\/(?P<total>\d+)")


@dataclass(frozen=True)
class StatusRow:
    page: str
    updated: Optional[datetime]
    status: str
    message: str
    rows: Optional[int]
    columns: Optional[int]


@dataclass
class Finding:
    severity: str
    code: str
    surface: str
    message: str


@dataclass
class DecisionSurfaceReport:
    generated_at_utc: str
    spreadsheet: str
    executable: bool = False
    portfolio_run_riyadh: Optional[str] = None
    top10_run_riyadh: Optional[str] = None
    my_portfolio_updated_riyadh: Optional[str] = None
    source_status: dict[str, dict[str, Any]] = field(default_factory=dict)
    top10_pool_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    findings: list[Finding] = field(default_factory=list)
    fatal: str = ""

    @property
    def exit_code(self) -> int:
        if self.fatal:
            return 3
        if any(item.severity == "FAIL" for item in self.findings):
            return 2
        if self.findings:
            return 1
        return 0

    def payload(self) -> dict[str, Any]:
        return {
            "script_version": VERSION,
            "generated_at_utc": self.generated_at_utc,
            "spreadsheet": self.spreadsheet,
            "executable": self.executable,
            "portfolio_run_riyadh": self.portfolio_run_riyadh,
            "top10_run_riyadh": self.top10_run_riyadh,
            "my_portfolio_updated_riyadh": self.my_portfolio_updated_riyadh,
            "source_status": self.source_status,
            "top10_pool_counts": self.top10_pool_counts,
            "summary": {
                "failures": sum(item.severity == "FAIL" for item in self.findings),
                "warnings": sum(item.severity == "WARN" for item in self.findings),
                "exit_code": self.exit_code,
                "fatal": self.fatal,
            },
            "findings": [asdict(item) for item in self.findings],
        }


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, "") or default))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, "") or default)
        return default if math.isnan(value) or math.isinf(value) else value
    except Exception:
        return default


def _number(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(float(str(value).replace(",", "").strip()))
    except Exception:
        return None


def _cell(grid: Sequence[Sequence[Any]], row: int, col: int) -> Any:
    if row < 0 or row >= len(grid):
        return None
    current = grid[row]
    if not isinstance(current, (list, tuple)) or col < 0 or col >= len(current):
        return None
    return current[col]


def parse_surface_status(text: Any) -> tuple[Optional[datetime], str]:
    match = RUN_RE.search(s(text))
    if not match:
        return None, ""
    return parse_dt(match.group("stamp")), match.group("status").strip().upper()


def parse_pool_counts(text: Any) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    for match in POOL_RE.finditer(s(text)):
        counts[match.group("page")] = {
            "used": int(match.group("used")),
            "total": int(match.group("total")),
        }
    return counts


def parse_status_grid(grid: Sequence[Sequence[Any]]) -> dict[str, StatusRow]:
    if not grid:
        return {}
    headers = [s(value).casefold() for value in grid[0]]
    index = {header: position for position, header in enumerate(headers) if header}
    required = ("page", "last updated", "status", "message", "rows", "columns")
    if any(name not in index for name in required):
        return {}

    rows: dict[str, StatusRow] = {}
    for raw in grid[1:]:
        if not isinstance(raw, (list, tuple)):
            continue
        page = s(raw[index["page"]] if index["page"] < len(raw) else "")
        if not page:
            continue
        rows[page] = StatusRow(
            page=page,
            updated=parse_dt(raw[index["last updated"]] if index["last updated"] < len(raw) else None),
            status=s(raw[index["status"]] if index["status"] < len(raw) else "").upper(),
            message=s(raw[index["message"]] if index["message"] < len(raw) else ""),
            rows=_number(raw[index["rows"]] if index["rows"] < len(raw) else None),
            columns=_number(raw[index["columns"]] if index["columns"] < len(raw) else None),
        )
    return rows


def _age_hours(stamp: Optional[datetime], now_riyadh: datetime) -> Optional[float]:
    if stamp is None:
        return None
    local = stamp.replace(tzinfo=None)
    return max(0.0, (now_riyadh.replace(tzinfo=None) - local).total_seconds() / 3600.0)


def _iso(stamp: Optional[datetime]) -> Optional[str]:
    return stamp.isoformat(sep=" ", timespec="seconds") if stamp else None


def audit_surfaces(
    status_grid: Sequence[Sequence[Any]],
    portfolio_grid: Sequence[Sequence[Any]],
    top10_grid: Sequence[Sequence[Any]],
    *,
    spreadsheet: str = "***",
    now_utc: Optional[datetime] = None,
    market_max_age_h: float = 30.0,
    decision_max_age_h: float = 8.0,
    min_rows: Optional[Mapping[str, int]] = None,
) -> DecisionSurfaceReport:
    now_utc = now_utc or datetime.now(timezone.utc)
    now_riyadh = now_utc.astimezone(timezone(timedelta(hours=3))).replace(tzinfo=None)
    report = DecisionSurfaceReport(now_utc.isoformat(), spreadsheet)
    status_rows = parse_status_grid(status_grid)
    if not status_rows:
        report.fatal = "_Status header or rows could not be parsed"
        return report

    floors = dict(
        min_rows
        or {
            "Market_Leaders": _env_int("TFB_EXPECTED_MIN_ROWS_MARKET_LEADERS", 1025),
            "Global_Markets": _env_int("TFB_EXPECTED_MIN_ROWS_GLOBAL_MARKETS", 6512),
            "Commodities_FX": _env_int("TFB_EXPECTED_MIN_ROWS_COMMODITIES_FX", 453),
            "Mutual_Funds": _env_int("TFB_EXPECTED_MIN_ROWS_MUTUAL_FUNDS", 4496),
        }
    )

    portfolio_text = _cell(portfolio_grid, 1, 1)
    portfolio_run, portfolio_state = parse_surface_status(portfolio_text)
    report.portfolio_run_riyadh = _iso(portfolio_run)
    if portfolio_run is None:
        report.findings.append(Finding("FAIL", "PF_RUN_MISSING", "Portfolio_Decision", "Last-run timestamp is missing or unparseable."))
    elif _age_hours(portfolio_run, now_riyadh) > decision_max_age_h:
        report.findings.append(Finding("FAIL", "PF_RUN_STALE", "Portfolio_Decision", f"Decision surface age exceeds {decision_max_age_h:g} hours."))
    if portfolio_state != "OK":
        report.findings.append(Finding("FAIL", "PF_STATUS_NOT_OK", "Portfolio_Decision", f"Embedded status is {portfolio_state or 'unknown'}, not OK."))

    my_portfolio = status_rows.get("My_Portfolio")
    if my_portfolio is None:
        report.findings.append(Finding("FAIL", "PF_SOURCE_STATUS_MISSING", "Portfolio_Decision", "My_Portfolio is absent from _Status."))
    else:
        report.my_portfolio_updated_riyadh = _iso(my_portfolio.updated)
        report.source_status["My_Portfolio"] = asdict(my_portfolio)
        if my_portfolio.status not in GOOD_FULL_PAGE_STATUSES:
            report.findings.append(Finding("FAIL", "PF_SOURCE_NOT_VALID", "Portfolio_Decision", f"My_Portfolio status is {my_portfolio.status or 'unknown'}."))
        source_age = _age_hours(my_portfolio.updated, now_riyadh)
        if source_age is None or source_age > decision_max_age_h:
            report.findings.append(Finding("FAIL", "PF_SOURCE_STALE", "Portfolio_Decision", f"My_Portfolio source age exceeds {decision_max_age_h:g} hours or is unknown."))
        if portfolio_run and my_portfolio.updated and portfolio_run < my_portfolio.updated:
            report.findings.append(Finding("FAIL", "PF_OLDER_THAN_SOURCE", "Portfolio_Decision", "Portfolio_Decision predates the latest My_Portfolio refresh."))

    top10_text = _cell(top10_grid, 1, 1)
    top10_run, top10_state = parse_surface_status(top10_text)
    report.top10_run_riyadh = _iso(top10_run)
    report.top10_pool_counts = parse_pool_counts(top10_text)
    if top10_run is None:
        report.findings.append(Finding("FAIL", "T10_RUN_MISSING", "Top_10_Investments", "Last-run timestamp is missing or unparseable."))
    elif _age_hours(top10_run, now_riyadh) > decision_max_age_h:
        report.findings.append(Finding("FAIL", "T10_RUN_STALE", "Top_10_Investments", f"Top-10 surface age exceeds {decision_max_age_h:g} hours."))
    if top10_state != "OK":
        report.findings.append(Finding("FAIL", "T10_STATUS_NOT_OK", "Top_10_Investments", f"Embedded status is {top10_state or 'unknown'}, not OK."))

    incomplete_sources: list[str] = []
    for page, floor in floors.items():
        item = status_rows.get(page)
        if item is None:
            incomplete_sources.append(page)
            report.findings.append(Finding("FAIL", "SOURCE_STATUS_MISSING", "Top_10_Investments", f"{page} is absent from _Status."))
            continue
        report.source_status[page] = asdict(item)
        if item.status not in GOOD_FULL_PAGE_STATUSES:
            incomplete_sources.append(page)
            report.findings.append(Finding("FAIL", "SOURCE_NOT_COMPLETE", "Top_10_Investments", f"{page} status is {item.status or 'unknown'}: {item.message or 'no message'}."))
        age = _age_hours(item.updated, now_riyadh)
        if age is None or age > market_max_age_h:
            incomplete_sources.append(page)
            report.findings.append(Finding("FAIL", "SOURCE_STALE", "Top_10_Investments", f"{page} exceeds {market_max_age_h:g} hours or has no valid timestamp."))
        if item.rows is None or item.rows < floor:
            incomplete_sources.append(page)
            report.findings.append(Finding("FAIL", "SOURCE_ROW_FLOOR", "Top_10_Investments", f"{page} rows {item.rows if item.rows is not None else 'unknown'} are below approved minimum {floor}."))
        if top10_run and item.updated and top10_run < item.updated:
            incomplete_sources.append(page)
            report.findings.append(Finding("FAIL", "T10_OLDER_THAN_SOURCE", "Top_10_Investments", f"Top-10 predates the latest {page} status timestamp."))

        pool = report.top10_pool_counts.get(page)
        if pool is None:
            incomplete_sources.append(page)
            report.findings.append(Finding("FAIL", "T10_POOL_COUNT_MISSING", "Top_10_Investments", f"Top-10 status does not disclose the {page} pool count."))
        elif pool["total"] < floor:
            incomplete_sources.append(page)
            report.findings.append(Finding("FAIL", "T10_POOL_BELOW_FLOOR", "Top_10_Investments", f"Top-10 used {pool['total']} {page} rows, below approved minimum {floor}."))

    claims_full = "(full universe)" in s(top10_text).casefold()
    if claims_full and incomplete_sources:
        report.findings.append(Finding("FAIL", "FALSE_FULL_UNIVERSE_CLAIM", "Top_10_Investments", "Status claims a full universe while one or more required source pages are partial, stale, or below their approved row floors."))

    report.executable = report.exit_code == 0
    return report


async def _read_range(reader: Callable[..., Any], spreadsheet_id: str, a1: str) -> list[list[Any]]:
    loop = asyncio.get_running_loop()
    value = await loop.run_in_executor(None, lambda: reader(spreadsheet_id, a1))
    if inspect.isawaitable(value):
        value = await value
    if not isinstance(value, list):
        raise TypeError(f"read_range for {a1} did not return a list")
    return [list(row) if isinstance(row, (list, tuple)) else [row] for row in value]


async def run_live(spreadsheet_id: str, reader: Optional[Callable[..., Any]] = None) -> DecisionSurfaceReport:
    if not spreadsheet_id:
        report = DecisionSurfaceReport(datetime.now(timezone.utc).isoformat(), "***")
        report.fatal = "spreadsheet ID missing"
        return report
    reader = reader or resolve_reader()
    if not reader:
        report = DecisionSurfaceReport(datetime.now(timezone.utc).isoformat(), "***")
        report.fatal = "read_range unavailable"
        return report
    masked = spreadsheet_id[:5] + "..." + spreadsheet_id[-5:] if len(spreadsheet_id) > 10 else "***"
    try:
        status_grid, portfolio_grid, top10_grid = await asyncio.gather(
            _read_range(reader, spreadsheet_id, "_Status!A1:J100"),
            _read_range(reader, spreadsheet_id, "Portfolio_Decision!A1:B3"),
            _read_range(reader, spreadsheet_id, "Top_10_Investments!A1:B3"),
        )
    except Exception as exc:
        report = DecisionSurfaceReport(datetime.now(timezone.utc).isoformat(), masked)
        report.fatal = f"live read failed: {type(exc).__name__}: {exc}"
        return report
    return audit_surfaces(
        status_grid,
        portfolio_grid,
        top10_grid,
        spreadsheet=masked,
        market_max_age_h=_env_float("TFB_DECISION_SOURCE_MAX_AGE_H", 30.0),
        decision_max_age_h=_env_float("TFB_DECISION_SURFACE_MAX_AGE_H", 8.0),
    )


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sheet-id", default=os.getenv("DEFAULT_SPREADSHEET_ID", ""))
    parser.add_argument("--json-out", default="decision_surface_freshness.json")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = create_parser().parse_args(argv)
    report = asyncio.run(run_live(args.sheet_id))
    rendered = json.dumps(report.payload(), ensure_ascii=False, indent=2, default=str)
    print(rendered)
    for finding in report.findings:
        annotation = "error" if finding.severity == "FAIL" else "warning"
        print(f"::{annotation}::{finding.surface} [{finding.code}] {finding.message}")
    if args.json_out:
        Path(args.json_out).write_text(rendered + "\n", encoding="utf-8")
    return report.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
