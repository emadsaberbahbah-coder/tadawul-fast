#!/usr/bin/env python3
"""Read-only, fail-closed audit of investment decision surfaces.

The workbook can contain individually valid pages built from different
snapshots. This audit prevents an old ``Portfolio_Decision`` or a Top-10 page
built from partial/stale source universes from looking executable merely
because its own status text says ``ok``.

No provider call and no Google Sheet write is performed.

VERSION 1.1.0 (2026-09-08) — CLOCK TRUTH: NO INVENTED AGES (P-104)
WHY v1.1.0: the 2026-09-08 run (34189823244) failed PF_SOURCE_STALE on a
date-only "9/8/2026" stamp read as midnight (fabricated 8.24h age), while
every "+03:00" _Status stamp aged +3h exactly (shared parse_dt returned
naive UTC; _age_hours subtracted it from naive Riyadh — reproduced against
the live 07:07:28+03:00 stamp: true 1.11h, computed 4.11h). FIX: (1)
parse_dt is now uniformly Riyadh-naive at its source (coverage script
v1.1.0); ages and run-vs-source orderings become same-basis. (2) precision
truth — a date-only stamp yields *_TIME_PRECISION (still FAIL, fail-closed)
instead of a fabricated midnight age, and is excluded from run-vs-source
ordering checks. (3) _age_hours returns SIGNED age; stamps more than
FUTURE_SKEW_H in the future yield explicit *_FUTURE findings instead of
silently clamping to fresh. (4) --selftest pins the golden cases (T05
equivalence of Z/+00:00/+03:00/naive forms; T06 date-only; future). Floors,
universe contract, exit-code semantics: UNTOUCHED.
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

from scripts.audit_full_refresh_coverage import parse_dt, parse_dt_precision, resolve_reader, s  # noqa: E402

VERSION = "1.1.0"
FUTURE_SKEW_H = 0.25                              # v1.1.0 P-104: allowed clock skew
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
    updated_precision: str                            # v1.1.0 P-104: "datetime" | "date" | "none"
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
        _upd, _prec = parse_dt_precision(              # v1.1.0 P-104
            raw[index["last updated"]] if index["last updated"] < len(raw) else None)
        rows[page] = StatusRow(
            page=page,
            updated=_upd,
            updated_precision=_prec,
            status=s(raw[index["status"]] if index["status"] < len(raw) else "").upper(),
            message=s(raw[index["message"]] if index["message"] < len(raw) else ""),
            rows=_number(raw[index["rows"]] if index["rows"] < len(raw) else None),
            columns=_number(raw[index["columns"]] if index["columns"] < len(raw) else None),
        )
    return rows


def _age_hours(stamp: Optional[datetime], now_riyadh: datetime) -> Optional[float]:
    """v1.1.0 P-104: SIGNED age in hours (negative = stamp in the future).
    The old max(0.0, ...) clamp silently certified future timestamps as
    perfectly fresh; callers now detect them explicitly."""
    if stamp is None:
        return None
    local = stamp.replace(tzinfo=None)
    return (now_riyadh.replace(tzinfo=None) - local).total_seconds() / 3600.0


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
    else:                                              # v1.1.0 P-104 signed age
        _pf_age = _age_hours(portfolio_run, now_riyadh)
        if _pf_age < -FUTURE_SKEW_H:
            report.findings.append(Finding("FAIL", "PF_RUN_FUTURE", "Portfolio_Decision", f"Last-run timestamp is {-_pf_age:.2f} hours in the future."))
        elif _pf_age > decision_max_age_h:
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
        if my_portfolio.updated is not None and my_portfolio.updated_precision == "date":
            # v1.1.0 P-104 precision truth: a date-only stamp cannot certify
            # intraday freshness and must not fabricate a midnight age.
            report.findings.append(Finding("FAIL", "PF_SOURCE_TIME_PRECISION", "Portfolio_Decision", "My_Portfolio Last Updated is date-only; intraday freshness cannot be certified from it."))
        elif source_age is not None and source_age < -FUTURE_SKEW_H:
            report.findings.append(Finding("FAIL", "PF_SOURCE_FUTURE", "Portfolio_Decision", f"My_Portfolio timestamp is {-source_age:.2f} hours in the future."))
        elif source_age is None or source_age > decision_max_age_h:
            report.findings.append(Finding("FAIL", "PF_SOURCE_STALE", "Portfolio_Decision", f"My_Portfolio source age exceeds {decision_max_age_h:g} hours or is unknown."))
        if (portfolio_run and my_portfolio.updated
                and my_portfolio.updated_precision == "datetime"    # v1.1.0 P-104
                and portfolio_run < my_portfolio.updated):
            report.findings.append(Finding("FAIL", "PF_OLDER_THAN_SOURCE", "Portfolio_Decision", "Portfolio_Decision predates the latest My_Portfolio refresh."))

    top10_text = _cell(top10_grid, 1, 1)
    top10_run, top10_state = parse_surface_status(top10_text)
    report.top10_run_riyadh = _iso(top10_run)
    report.top10_pool_counts = parse_pool_counts(top10_text)
    if top10_run is None:
        report.findings.append(Finding("FAIL", "T10_RUN_MISSING", "Top_10_Investments", "Last-run timestamp is missing or unparseable."))
    else:                                              # v1.1.0 P-104 signed age
        _t10_age = _age_hours(top10_run, now_riyadh)
        if _t10_age < -FUTURE_SKEW_H:
            report.findings.append(Finding("FAIL", "T10_RUN_FUTURE", "Top_10_Investments", f"Top-10 last-run timestamp is {-_t10_age:.2f} hours in the future."))
        elif _t10_age > decision_max_age_h:
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
        if item.updated is not None and item.updated_precision == "date":
            incomplete_sources.append(page)            # v1.1.0 P-104 precision truth
            report.findings.append(Finding("FAIL", "SOURCE_TIME_PRECISION", "Top_10_Investments", f"{page} Last Updated is date-only; intraday freshness cannot be certified from it."))
        elif age is not None and age < -FUTURE_SKEW_H:
            incomplete_sources.append(page)
            report.findings.append(Finding("FAIL", "SOURCE_FUTURE", "Top_10_Investments", f"{page} timestamp is {-age:.2f} hours in the future."))
        elif age is None or age > market_max_age_h:
            incomplete_sources.append(page)
            report.findings.append(Finding("FAIL", "SOURCE_STALE", "Top_10_Investments", f"{page} exceeds {market_max_age_h:g} hours or has no valid timestamp."))
        if item.rows is None or item.rows < floor:
            incomplete_sources.append(page)
            report.findings.append(Finding("FAIL", "SOURCE_ROW_FLOOR", "Top_10_Investments", f"{page} rows {item.rows if item.rows is not None else 'unknown'} are below approved minimum {floor}."))
        if (top10_run and item.updated
                and item.updated_precision == "datetime"            # v1.1.0 P-104
                and top10_run < item.updated):
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


def _selftest() -> int:
    """v1.1.0 P-104 golden fixtures — offline, no network, no sheets.
    Pins acceptance tests T05 (equivalent instants), T06 (missing time
    precision) and the future-timestamp rule against the REAL functions."""
    checks: list[tuple[str, bool]] = []
    riyadh_naive = datetime(2026, 9, 8, 7, 7, 28)

    forms = ["2026-09-08 07:07:28+03:00", "2026-09-08T04:07:28Z",
             "2026-09-08T04:07:28+00:00", "2026-09-08 07:07:28"]
    parsed = [parse_dt(x) for x in forms]
    checks.append(("T05: Z / +00:00 / +03:00 / naive forms -> one Riyadh instant",
                   all(p == riyadh_naive for p in parsed)))

    now_utc = datetime(2026, 9, 8, 5, 14, 20, tzinfo=timezone.utc)
    now_riyadh = now_utc.astimezone(timezone(timedelta(hours=3))).replace(tzinfo=None)
    age = _age_hours(parse_dt("2026-09-08 07:07:28+03:00"), now_riyadh)
    checks.append(("T05: reproduced 3h-inflation case now reads true 1.1144h",
                   age is not None and abs(age - 1.114444) < 1e-3))

    checks.append(("T06: date-only forms report precision 'date'",
                   parse_dt_precision("9/8/2026")[1] == "date"
                   and parse_dt_precision("2026-09-08")[1] == "date"
                   and parse_dt_precision(46252)[1] == "date"
                   and parse_dt_precision(46252.5)[1] == "datetime"))

    def _grids(mp_updated: str, gm_updated: str):
        status = [["Page", "Last Updated", "Status", "Message", "Rows", "Columns"],
                  ["My_Portfolio", mp_updated, "VALID", "ok", "7", "122"],
                  ["Market_Leaders", gm_updated, "SUCCESS", "ok", "255", "115"],
                  ["Global_Markets", gm_updated, "SUCCESS", "ok", "6609", "115"],
                  ["Commodities_FX", gm_updated, "SUCCESS", "ok", "453", "115"],
                  ["Mutual_Funds", gm_updated, "SUCCESS", "ok", "2474", "115"]]
        surface = [["x"], ["Status:", "Last run 2026-09-08 08:08:08 | status: ok | "
                   "Market_Leaders 255/255, Global_Markets 6609/6609, "
                   "Commodities_FX 453/453, Mutual_Funds 2469/2474"]]
        return status, surface, surface

    floors1 = {"Market_Leaders": 1, "Global_Markets": 1,
               "Commodities_FX": 1, "Mutual_Funds": 1}
    rep = audit_surfaces(*_grids("2026-09-08T05:05:24+00:00",
                                 "2026-09-08 07:07:28+03:00"),
                         now_utc=now_utc, min_rows=floors1)
    codes = {x.code for x in rep.findings}
    checks.append(("T05: fresh +03:00 / +00:00 stamps raise no STALE finding",
                   not ({"SOURCE_STALE", "PF_SOURCE_STALE"} & codes)))

    rep2 = audit_surfaces(*_grids("9/8/2026", "2026-09-08 07:07:28+03:00"),
                          now_utc=now_utc, min_rows=floors1)
    codes2 = {x.code for x in rep2.findings}
    checks.append(("T06: date-only My_Portfolio -> PRECISION finding, never a "
                   "fabricated-midnight STALE",
                   "PF_SOURCE_TIME_PRECISION" in codes2
                   and "PF_SOURCE_STALE" not in codes2))

    rep3 = audit_surfaces(*_grids("2026-09-08T09:30:00+03:00",
                                  "2026-09-08 07:07:28+03:00"),
                          now_utc=now_utc, min_rows=floors1)
    codes3 = {x.code for x in rep3.findings}
    checks.append(("FUTURE: stamp beyond skew -> explicit *_FUTURE, not "
                   "silent freshness",
                   "PF_SOURCE_FUTURE" in codes3
                   and "PF_SOURCE_STALE" not in codes3))

    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"[decision_surface_freshness v{VERSION}] SELFTEST {passed}/{len(checks)}")
    return 0 if passed == len(checks) else 1


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sheet-id", default=os.getenv("DEFAULT_SPREADSHEET_ID", ""))
    parser.add_argument("--json-out", default="decision_surface_freshness.json")
    parser.add_argument("--selftest", action="store_true",
                        help="offline P-104 golden fixtures, no network")  # v1.1.0
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = create_parser().parse_args(argv)
    if args.selftest:                                  # v1.1.0 P-104
        return _selftest()
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
