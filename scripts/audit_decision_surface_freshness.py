#!/usr/bin/env python3
"""Read-only, fail-closed audit of investment decision surfaces.

The workbook can contain individually valid pages built from different
snapshots. This audit prevents an old ``Portfolio_Decision`` or a Top-10 page
built from partial/stale source universes from looking executable merely
because its own status text says ``ok``.

No provider call and no Google Sheet write is performed.

VERSION 1.3.2 — an opportunity-pool shortfall is accepted only when a native
per-symbol exclusion receipt agrees with independently crossbound complete
source grids, source cohort and first-page ownership. Physical membership
remains unchanged; missing rows and unverifiable deductions fail closed.

VERSION 1.3.1 — independent full-column Symbol reads prove there is no unseen
tail beyond the wide audit bound and agree by physical row with that read.

VERSION 1.3.0 — approved Market_Leaders and Mutual_Funds membership is checked
against physical Symbol cells, independently of _Status counts. The current
255 Saudi-only / 2474 retained-fund approval supersedes historical row floors
only for those two pages; a missing or invalid approval fails closed. Explicit
in-memory min_rows fixtures can still exercise the generic audit policy.

VERSION 1.2.3 — final publication requires current source publication runs and
explicit consumed-source receipts on both decision surfaces. The receipt schema
is ``source readiness VERIFIED (run <id>)``. Portfolio_Decision must emit this
only after consuming the matching My_Portfolio publication; its current producer
does not yet provide that proof, so final publication remains blocked until it
does. A source checksum or warning archive receipt does not prove consumption,
and these receipts do not authenticate custody. Standalone audits without an
expected source run retain their existing behavior and approved row floors.

VERSION 1.2.2 — explicit final-publication policy and optional source-run cohort
proof. Defaults retain the standalone audit's existing policy and behavior.

VERSION 1.2.1 — consumes the corrected price-acquisition facts: a margin-only
fundamentals quarantine remains a fundamentals flag, not a price failure.
Configured feed policy and decision-readiness checks remain unchanged.

VERSION 1.2.0 — acquisition truth is required independently of the rollout
status cell. Missing/partial acquisition evidence and policy certification
errors cannot certify decision readiness. Duplicate proofs remain unknown.

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
import hashlib
import inspect
import json
import math
import os
import re
import sys
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Mapping, Optional, Sequence

for _path in (Path(__file__).resolve().parent, Path(__file__).resolve().parent.parent):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from scripts.audit_full_refresh_coverage import parse_dt, parse_dt_precision, resolve_reader, s  # noqa: E402
from core.data_validity import coverage_validity  # noqa: E402

if TYPE_CHECKING:
    from scripts.approved_universe_scope import ScopePolicy

VERSION = "1.3.2"
SOURCE_SCOPE_BOUND = 20050
SOURCE_SCOPE_RANGE = f"A1:EZ{SOURCE_SCOPE_BOUND}"
POOL_PAGES = ("Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds")
FUTURE_SKEW_H = 0.25                              # v1.1.0 P-104: allowed clock skew
GOOD_FULL_PAGE_STATUSES = {"OK", "SUCCESS", "VALID", "PASS", "COMPLETE"}
RUN_RE = re.compile(
    r"Last\s+run\s+(?P<stamp>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})"
    r"\s*\|\s*status:\s*(?P<status>[A-Za-z_]+)",
    re.I,
)
POOL_RE = re.compile(r"(?P<page>[A-Za-z][A-Za-z0-9_]+)\s+(?P<used>\d+)\/(?P<total>\d+)")
SOURCE_RUN_RE = re.compile(r"(?<![a-z0-9_])run\s*=\s*([^\s;|,]*)", re.I)
SOURCE_READINESS_RE = re.compile(r"(?<![a-z0-9_])source\s+readiness\b", re.I)
CONSUMED_SOURCE_RE = re.compile(
    r"(?<![a-z0-9_])source readiness VERIFIED \(run ([1-9][0-9]*)\)", re.I)


@dataclass(frozen=True)
class StatusRow:
    page: str
    updated: Optional[datetime]
    updated_precision: str                            # v1.1.0 P-104: "datetime" | "date" | "none"
    status: str
    message: str
    rows: Optional[int]
    columns: Optional[int]
    acquired_fresh: Optional[int] = None
    acquired_requested: Optional[int] = None
    acquisition: str = "UNKNOWN"
    data: str = "UNKNOWN"
    fetchfail_requested: str = "unknown"
    fetchfail_effective: str = "unknown"


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
    approved_scope: dict[str, Any] = field(default_factory=dict)
    top10_pool_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    top10_pool_reconciliation: dict[str, Any] = field(default_factory=dict)
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
            "approved_scope": self.approved_scope,
            "top10_pool_counts": self.top10_pool_counts,
            "top10_pool_reconciliation": self.top10_pool_reconciliation,
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
        message = s(raw[index["message"]] if index["message"] < len(raw) else "")
        acquired_values = re.findall(r"(?:^|\s)acquired=([^\s]+)",message,re.I)
        acquired = re.fullmatch(r"(\d+|unknown)/(\d+)",acquired_values[0],re.I) if len(acquired_values)==1 else None
        def token(name, default):
            values = re.findall(r"(?:^|\s)" + re.escape(name) + r"=([A-Za-z_]+)(?:\s|$)",message,re.I)
            return values[0] if len(values)==1 else default
        if page in rows:
            rows[page] = replace(rows[page],acquired_fresh=None,acquired_requested=None,
                                 acquisition="UNKNOWN",data="UNKNOWN")
            continue
        rows[page] = StatusRow(
            page=page,
            updated=_upd,
            updated_precision=_prec,
            status=s(raw[index["status"]] if index["status"] < len(raw) else "").upper(),
            message=message,
            rows=_number(raw[index["rows"]] if index["rows"] < len(raw) else None),
            columns=_number(raw[index["columns"]] if index["columns"] < len(raw) else None),
            acquired_fresh=_number(acquired.group(1)) if acquired else None,
            acquired_requested=_number(acquired.group(2)) if acquired else None,
            acquisition=token("acquisition","UNKNOWN").upper(),
            data=token("data","UNKNOWN").upper(),
            fetchfail_requested=token("fetchfail_requested","unknown").lower(),
            fetchfail_effective=token("fetchfail_effective","unknown").lower(),
        )
    return rows


def _source_publication_runs(grid: Sequence[Sequence[Any]]) -> dict[str, Optional[str]]:
    """Only one page row and one positive run token establish its publication."""
    headers = [s(value).casefold() for value in grid[0]] if grid else []
    if headers.count("page") != 1 or headers.count("message") != 1:
        return {}
    pi, mi = headers.index("page"), headers.index("message")
    messages: dict[str, list[str]] = {}
    for row in grid[1:]:
        if not isinstance(row, (list, tuple)) or max(pi, mi) >= len(row):
            continue
        page = s(row[pi])
        if page:
            messages.setdefault(page, []).append(s(row[mi]))
    runs = {}
    for page, values in messages.items():
        tokens = SOURCE_RUN_RE.findall(values[0]) if len(values) == 1 else []
        runs[page] = (tokens[0] if len(tokens) == 1 and
                      re.fullmatch(r"[1-9][0-9]*", tokens[0]) else None)
    return runs


def _consumed_source_run(text: Any) -> Optional[str]:
    """A unique producer receipt proves the consumed cohort, never wall time.

    Count every source-readiness declaration, including withheld/malformed
    declarations, so a valid marker cannot mask a contradictory one.
    """
    value = s(text)
    matches = CONSUMED_SOURCE_RE.findall(value)
    if len(SOURCE_READINESS_RE.findall(value)) != 1 or len(matches) != 1:
        return None
    return matches[0]


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


def _acquisition_findings(item: StatusRow, surface: str, prefix: str,
                          floor: int, minimum: float) -> list[Finding]:
    """Read acquisition truth even when the status cell reflects observe policy."""
    findings: list[Finding] = []
    validity = coverage_validity(item.acquired_requested, item.acquired_fresh, minimum)
    if item.acquired_fresh is None or item.acquired_requested is None:
        findings.append(Finding("FAIL",prefix+"_ACQUISITION_UNKNOWN",surface,
            f"{item.page} successful-acquisition evidence is missing; a recent publication stamp cannot certify acquisition."))
    elif not validity.valid or item.acquisition != "COMPLETE":
        findings.append(Finding("FAIL",prefix+"_ACQUISITION_INCOMPLETE",surface,
            f"{item.page} acquired {item.acquired_fresh}/{item.acquired_requested} ({validity.percent}%), acquisition={item.acquisition}; minimum {minimum:g}%."))
    if item.acquired_requested is not None and item.acquired_requested < floor:
        findings.append(Finding("FAIL",prefix+"_ACQUISITION_UNIVERSE",surface,
            f"{item.page} acquisition denominator {item.acquired_requested} is below approved minimum {floor}."))
    if item.data != "COMPLETE":
        findings.append(Finding("FAIL",prefix+"_DATA_NOT_COMPLETE",surface,
            f"{item.page} factual data verdict is {item.data} despite status {item.status}."))
    if item.fetchfail_effective == "error":
        findings.append(Finding("FAIL",prefix+"_POLICY_ERROR",surface,
            f"{item.page} requested {item.fetchfail_requested} but effective policy is error; readiness is blocked."))
    return findings


def _physical_source(grid: Sequence[Sequence[Any]]) -> tuple[list[Any], int, int]:
    """Read the literal Symbol column; neither ticker aliases nor row counts
    establish the approved physical membership.
    """
    candidates = []
    for row_number, row in enumerate(grid[:45]):
        if not isinstance(row, (list, tuple)):
            continue
        columns = [i for i, value in enumerate(row) if s(value).casefold() == "symbol"]
        if columns:
            if len(columns) != 1:
                raise ValueError("physical source has repeated Symbol header columns")
            candidates.append((row_number, columns[0]))
    if len(candidates) != 1:
        raise ValueError("physical source must have one unambiguous Symbol header")
    header_row, symbol_column = candidates[0]
    symbols = []
    for row in grid[header_row + 1:]:
        if not isinstance(row, (list, tuple)):
            raise ValueError("physical source contains a malformed row")
        if any(s(value) for value in row):
            symbols.append(row[symbol_column] if symbol_column < len(row) else None)
    return symbols, header_row, symbol_column


def _column_cell(grid: Sequence[Sequence[Any]], row_number: int) -> str:
    row = grid[row_number] if row_number < len(grid) else ()
    return s(row[0] if row else None).upper()


def _scope_membership(page: str, wide_grid: Sequence[Sequence[Any]],
                      column_grid: Sequence[Sequence[Any]], scope: "ScopePolicy",
                      receipt: dict[str, Any]) -> Any:
    """Bind complete-column evidence to the bounded wide snapshot before using
    either read to certify membership. A set hash alone cannot detect a reorder.
    """
    from scripts.approved_universe_scope import validate_symbols
    proof = {"range": f"{page}!A:A", "returned_rows": len(column_grid),
             "bound": SOURCE_SCOPE_BOUND, "symbol_column": 1,
             "valid": False, "crossbound": False}
    receipt["complete_column_proof"] = proof
    if len(column_grid) > SOURCE_SCOPE_BOUND or len(wide_grid) > SOURCE_SCOPE_BOUND:
        raise ValueError(f"complete Symbol column extends beyond row {SOURCE_SCOPE_BOUND}")
    if any(not isinstance(row, (list, tuple)) or len(row) > 1 for row in column_grid):
        raise ValueError("complete Symbol column has malformed or multiple-column rows")
    wide_symbols, wide_header, wide_column = _physical_source(wide_grid)
    complete_symbols, complete_header, complete_column = _physical_source(column_grid)
    proof["header_row"] = complete_header + 1
    if wide_column != 0 or complete_column != 0 or wide_header != complete_header:
        raise ValueError("complete Symbol column and wide source must share their column-A header row")
    if any(_column_cell(wide_grid, row) != _column_cell(column_grid, row)
           for row in range(max(len(wide_grid), len(column_grid)))):
        raise ValueError("complete Symbol column and wide source changed between reads")
    wide_validation = validate_symbols(page, wide_symbols, policy=scope)
    complete_validation = validate_symbols(page, complete_symbols, policy=scope)
    wide_receipt, complete_receipt = wide_validation.to_dict(), complete_validation.to_dict()
    receipt.update(wide_receipt)
    for key in ("actual_membership_sha256", "observed_symbol_set_sha256",
                "actual_count", "actual_unique_count", "duplicate_count", "bad_symbol_count"):
        if wide_receipt[key] != complete_receipt[key]:
            raise ValueError("complete Symbol column and wide source membership evidence differs")
    proof.update({"observed_symbol_set_sha256": complete_receipt["observed_symbol_set_sha256"],
                  "actual_membership_sha256": complete_receipt["actual_membership_sha256"],
                  "bounded_membership_sha256": wide_receipt["actual_membership_sha256"],
                  "bounded_symbol_set_sha256": wide_receipt["observed_symbol_set_sha256"],
                  "valid": wide_validation.valid and complete_validation.valid,
                  "crossbound": True})
    return complete_validation


def _complete_pool_source(page: str, wide_grid: Sequence[Sequence[Any]],
                          column_grid: Sequence[Sequence[Any]]) -> tuple[list[str], str]:
    """Complete raw identity evidence for cross-page ownership, with no aliases."""
    from scripts.approved_universe_scope import symbol_set_sha256
    if len(column_grid) > SOURCE_SCOPE_BOUND or len(wide_grid) > SOURCE_SCOPE_BOUND:
        raise ValueError(f"{page} complete Symbol column exceeds audit bound")
    if any(not isinstance(row, (list, tuple)) or len(row) > 1 for row in column_grid):
        raise ValueError(f"{page} complete Symbol column is malformed")
    wide_symbols, wide_header, wide_column = _physical_source(wide_grid)
    complete_symbols, complete_header, complete_column = _physical_source(column_grid)
    if wide_column != 0 or complete_column != 0 or wide_header != complete_header:
        raise ValueError(f"{page} complete and wide Symbol headers differ")
    if any(_column_cell(wide_grid, row) != _column_cell(column_grid, row)
           for row in range(max(len(wide_grid), len(column_grid)))):
        raise ValueError(f"{page} complete and wide Symbol rows changed between reads")
    # The shared hash contract rejects numeric, blank, malformed and duplicate
    # identities. A within-page duplicate is never an authorized exclusion.
    digest = symbol_set_sha256(wide_symbols)
    if digest != symbol_set_sha256(complete_symbols) or len(wide_symbols) != len(complete_symbols):
        raise ValueError(f"{page} complete and wide membership evidence differs")
    return [identity.strip().upper() for identity in complete_symbols], digest


def _pool_receipt(text: Any) -> Optional[dict[str, Any]]:
    """The native producer emits one canonical, hashed JSON field in B2."""
    value = s(text)
    occurrences = re.findall(r"(?<![A-Za-z0-9_])pool_reconciliation_v1\s*=", value)
    if not occurrences:
        return None
    matches = re.findall(r"(?:^|\|)\s*pool_reconciliation_v1=([^|]*)", value)
    if len(occurrences) != 1 or len(matches) != 1:
        raise ValueError("native pool reconciliation receipt must occur exactly once")
    raw = matches[0].strip()
    if len(raw) > 30000:
        raise ValueError("native pool reconciliation exceeds producer receipt bound")
    def unique_object(pairs):
        result = {}
        for key, item in pairs:
            if key in result:
                raise ValueError("native pool receipt contains duplicate JSON keys")
            result[key] = item
        return result
    receipt = json.loads(raw, object_pairs_hook=unique_object,
                         parse_constant=lambda _value: (_ for _ in ()).throw(
                             ValueError("native pool receipt contains non-finite numbers")))
    if not isinstance(receipt, dict) or list(receipt) != [
            "version", "run", "pages", "excluded", "physical_symbol_sha256", "sha256"]:
        raise ValueError("native pool reconciliation schema/order differs")
    if type(receipt["version"]) is not int or receipt["version"] != 1:
        raise ValueError("native pool reconciliation version unsupported")
    if not isinstance(receipt["run"], str) or not re.fullmatch(r"[1-9][0-9]*", receipt["run"]):
        raise ValueError("native pool reconciliation run invalid")
    claimed = receipt["sha256"]
    if not isinstance(claimed, str) or not re.fullmatch(r"[0-9a-f]{64}", claimed):
        raise ValueError("native pool reconciliation checksum invalid")
    canonical = json.dumps(receipt, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    unsigned = {key: value for key, value in receipt.items() if key != "sha256"}
    digest = hashlib.sha256(json.dumps(unsigned, separators=(",", ":"), ensure_ascii=False,
                                      allow_nan=False).encode("utf-8")).hexdigest()
    if raw != canonical or digest != claimed:
        raise ValueError("native pool reconciliation checksum/canonical bytes differ")
    return receipt


def _pool_reconciliation(text: Any, pools: Mapping[str, Mapping[str, int]],
                         status_rows: Mapping[str, StatusRow],
                         publication_runs: Mapping[str, Optional[str]],
                         expected_run: Optional[str],
                         source_grids: Optional[Mapping[str, Sequence[Sequence[Any]]]],
                         source_columns: Optional[Mapping[str, Sequence[Sequence[Any]]]]) -> dict[str, Any]:
    """Count equality alone cannot authorize loss from a physical source roster.

    Independently derive every duplicate's earlier-page owner. Native included
    counts describe the actual capped pool; physical totals describe the source
    pages. Neither assertion authenticates custody or provider acquisition.
    """
    receipt = _pool_receipt(text)
    if not source_grids or not source_columns:
        raise ValueError("complete source grids required for opportunity-pool reconciliation")
    physical, digests, excluded, owned_counts, owners = {}, {}, [], {}, {}
    for page in POOL_PAGES:
        if page not in source_grids or page not in source_columns:
            raise ValueError(f"{page} independent complete source evidence missing")
        symbols, digest = _complete_pool_source(page, source_grids[page], source_columns[page])
        physical[page], digests[page] = len(symbols), digest
        item = status_rows.get(page)
        if item is None or item.rows != len(symbols) or item.acquired_requested != len(symbols):
            raise ValueError(f"{page} status/acquisition counters differ from complete physical source")
        owned_counts[page] = 0
        for identity in sorted(symbols):
            if identity in owners:
                excluded.append({"symbol": identity, "page": page, "owner": owners[identity]})
            else:
                owners[identity] = page
                owned_counts[page] += 1
    if receipt is None:
        if excluded:
            raise ValueError("cross-page exclusions require individual native producer receipts")
        for page in POOL_PAGES:
            pool = pools.get(page)
            if not pool or pool["total"] != physical[page] or pool["used"] > physical[page]:
                raise ValueError(f"{page} pool counts differ from complete physical source")
            if pool["used"] != physical[page]:
                raise ValueError(f"{page} capped or incomplete pool cannot certify full publication")
        return {"valid": True, "receipt_required": False, "physical_counts": physical,
                "unique_counts": owned_counts, "excluded": []}
    if expected_run is not None and receipt["run"] != expected_run:
        raise ValueError("native pool reconciliation does not match expected source run")
    if _consumed_source_run(text) != receipt["run"] or any(
            publication_runs.get(page) != receipt["run"] for page in POOL_PAGES):
        raise ValueError("native pool reconciliation does not match one consumed/current source cohort")
    if not isinstance(receipt["pages"], dict) or tuple(receipt["pages"]) != POOL_PAGES:
        raise ValueError("native pool reconciliation page schema/order differs")
    if not isinstance(receipt["physical_symbol_sha256"], dict) or \
            tuple(receipt["physical_symbol_sha256"]) != POOL_PAGES or \
            receipt["physical_symbol_sha256"] != digests:
        raise ValueError("native pool reconciliation physical Symbol hashes differ")
    if not isinstance(receipt["excluded"], list) or receipt["excluded"] != excluded or any(
            not isinstance(record, dict) or list(record) != ["symbol", "page", "owner"]
            for record in receipt["excluded"]):
        raise ValueError("native pool exclusions differ from exact independent first-page ownership")
    for page in POOL_PAGES:
        claimed = receipt["pages"][page]
        if not isinstance(claimed, dict) or list(claimed) != ["physical", "unique", "included"] or any(
                type(value) is not int or value < 0 for value in claimed.values()):
            raise ValueError(f"{page} native pool counters invalid")
        if claimed["physical"] != physical[page] or claimed["unique"] != owned_counts[page] or \
                claimed["included"] > owned_counts[page]:
            raise ValueError(f"{page} native pool counters do not reconcile with physical ownership")
        if claimed["included"] != owned_counts[page]:
            raise ValueError(f"{page} explicit pool cap omitted unique rows; a truncated scan cannot certify full readiness")
        pool_matches = [match for match in POOL_RE.finditer(s(text)) if match.group("page") == page]
        if len(pool_matches) != 1 or pools.get(page) != {
                "used": claimed["included"], "total": physical[page]}:
            raise ValueError(f"{page} native status must disclose included/physical counts exactly once")
    totals = re.findall(r"(?<![A-Za-z0-9_])sheets pool ([0-9]+) rows\b", s(text))
    if len(totals) != 1 or int(totals[0]) != sum(owned_counts.values()):
        raise ValueError("native submitted-pool total differs from the complete unique ownership count")
    return {"valid": True, "receipt_required": True, "run": receipt["run"],
            "sha256": receipt["sha256"], "physical_counts": physical,
            "unique_counts": owned_counts, "excluded": excluded, "receipt": receipt}


def _market_floor(page: str, default: int) -> int:
    """G1 (2026-10-17): config/market_floors_v1.json, else the literal default."""
    try:
        from scripts.approved_universe_scope import market_floor
        return market_floor(page, default)
    except Exception:
        return default


def _configured_floors(min_rows: Optional[Mapping[str, int]]) -> dict[str, int]:
    return dict(min_rows or {
        "Market_Leaders": _env_int("TFB_EXPECTED_MIN_ROWS_MARKET_LEADERS", 1025),
        "Global_Markets": _env_int("TFB_EXPECTED_MIN_ROWS_GLOBAL_MARKETS", _market_floor("Global_Markets", 6512)),
        "Commodities_FX": _env_int("TFB_EXPECTED_MIN_ROWS_COMMODITIES_FX", _market_floor("Commodities_FX", 453)),
        "Mutual_Funds": _env_int("TFB_EXPECTED_MIN_ROWS_MUTUAL_FUNDS", 4496),
    })


def _load_approved_scope() -> "ScopePolicy":
    # Deliberately no environment or command-line policy bypass.
    from scripts.approved_universe_scope import load_scope
    return load_scope()


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
    min_fresh_percent: Optional[float | Mapping[str, float]] = None,
    expected_source_run: Optional[str] = None,
    approved_scope: Optional["ScopePolicy"] = None,
    source_grids: Optional[Mapping[str, Sequence[Sequence[Any]]]] = None,
    source_columns: Optional[Mapping[str, Sequence[Sequence[Any]]]] = None,
) -> DecisionSurfaceReport:
    now_utc = now_utc or datetime.now(timezone.utc)
    now_riyadh = now_utc.astimezone(timezone(timedelta(hours=3))).replace(tzinfo=None)
    report = DecisionSurfaceReport(now_utc.isoformat(), spreadsheet)
    status_rows = parse_status_grid(status_grid)
    if not status_rows:
        report.fatal = "_Status header or rows could not be parsed"
        return report
    strict_run = expected_source_run is not None
    expected_run = str(expected_source_run) if strict_run else None
    publication_runs = _source_publication_runs(status_grid)
    if strict_run and not re.fullmatch(r"[1-9][0-9]*", expected_run):
        report.findings.append(Finding("FAIL", "EXPECTED_SOURCE_RUN_INVALID", "Final_Publication",
            "Expected source run must be one positive publication run ID; final readiness cannot use an unknown/local run."))

    configured_floors = _configured_floors(min_rows)
    floors = dict(configured_floors)
    scope_counts: dict[str, int] = {}
    if approved_scope is not None or min_rows is None:
        try:
            from scripts.approved_universe_scope import approved_floors
            approved_scope = approved_scope or _load_approved_scope()
            scope_counts = approved_floors(policy=approved_scope)
            floors.update(scope_counts)
            floors["Global_Markets"] = max(floors.get("Global_Markets", 0), _market_floor("Global_Markets", 6512))
            floors["Commodities_FX"] = max(floors.get("Commodities_FX", 0), _market_floor("Commodities_FX", 453))
            market_max_age_h = min(30.0, market_max_age_h)
            decision_max_age_h = min(8.0, decision_max_age_h)
            report.approved_scope = approved_scope.receipt()
        except Exception as exc:
            report.fatal = f"approved universe scope unavailable: {type(exc).__name__}: {exc}"
            return report

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
    if strict_run:
        portfolio_source_run = publication_runs.get("My_Portfolio")
        if portfolio_source_run is None or portfolio_source_run != expected_run:
            report.findings.append(Finding("FAIL", "PF_SOURCE_PUBLICATION_COHORT", "Portfolio_Decision",
                f"My_Portfolio must have one _Status row with a unique run={expected_run} publication receipt; missing, repeated or different runs cannot certify this final publication."))
        consumed = _consumed_source_run(portfolio_text)
        if consumed is None or consumed != expected_run or consumed != portfolio_source_run:
            report.findings.append(Finding("FAIL", "PF_SOURCE_CONSUMPTION_UNPROVEN", "Portfolio_Decision",
                f"Portfolio_Decision must publish exactly one 'source readiness VERIFIED (run {expected_run})' receipt after consuming that My_Portfolio run. Its timestamp, holdings count and source checksum do not prove consumption; update the authenticated producer before final publication."))

    my_portfolio = status_rows.get("My_Portfolio")
    if my_portfolio is None:
        report.findings.append(Finding("FAIL", "PF_SOURCE_STATUS_MISSING", "Portfolio_Decision", "My_Portfolio is absent from _Status."))
    else:
        report.my_portfolio_updated_riyadh = _iso(my_portfolio.updated)
        report.source_status["My_Portfolio"] = asdict(my_portfolio)
        report.findings.extend(_acquisition_findings(my_portfolio,"Portfolio_Decision","PF_SOURCE",1,100))
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
    if strict_run and _consumed_source_run(top10_text) != expected_run:
        report.findings.append(Finding("FAIL", "T10_SOURCE_CONSUMPTION_UNPROVEN", "Top_10_Investments",
            f"Top-10 must publish exactly one 'source readiness VERIFIED (run {expected_run})' receipt for its consumed source cohort; refresh after that run completes. A recent Last run stamp does not prove current source consumption."))

    incomplete_sources: list[str] = []
    for page, floor in floors.items():
        item = status_rows.get(page)
        if item is None:
            incomplete_sources.append(page)
            report.findings.append(Finding("FAIL", "SOURCE_STATUS_MISSING", "Top_10_Investments", f"{page} is absent from _Status."))
            continue
        report.source_status[page] = asdict(item)
        if page in scope_counts:
            scope_receipt = {
                "applied_min_rows": floor,
                "superseded_configured_floor": {
                    "environment": "TFB_EXPECTED_MIN_ROWS_" + page.upper(),
                    "value": os.getenv("TFB_EXPECTED_MIN_ROWS_" + page.upper()),
                    "legacy_effective_min_rows": _env_int(
                        "TFB_EXPECTED_MIN_ROWS_" + page.upper(),
                        1025 if page == "Market_Leaders" else 4496),
                },
            }
            if min_rows is not None:
                scope_receipt["supplied_min_rows"] = min_rows.get(page)
            report.source_status[page]["approved_scope"] = scope_receipt
            try:
                if source_grids is None or page not in source_grids:
                    raise ValueError("physical source grid was not supplied")
                if source_columns is None or page not in source_columns:
                    raise ValueError("independent complete Symbol column was not supplied")
                validation = _scope_membership(page, source_grids[page], source_columns[page],
                                                approved_scope, scope_receipt)
                scope_receipt.update(validation.to_dict())
                if not validation.valid:
                    incomplete_sources.append(page)
                    report.findings.append(Finding("FAIL", "SOURCE_APPROVED_MEMBERSHIP", "Top_10_Investments",
                        f"{page} physical Symbol membership does not match the approved scope; missing, substituted, extra, duplicate or invalid symbols cannot certify a full universe."))
            except Exception as exc:
                incomplete_sources.append(page)
                scope_receipt["valid"] = False
                scope_receipt["error"] = f"{type(exc).__name__}: {exc}"
                report.findings.append(Finding("FAIL", "SOURCE_APPROVED_MEMBERSHIP_UNPROVEN", "Top_10_Investments",
                    f"{page} physical Symbol membership cannot be verified: {type(exc).__name__}: {exc}"))
            if item.rows != floor:
                incomplete_sources.append(page)
                report.findings.append(Finding("FAIL", "SOURCE_APPROVED_ROW_COUNT", "Top_10_Investments",
                    f"{page} _Status rows {item.rows if item.rows is not None else 'unknown'} do not equal the approved physical membership count {floor}."))
            if item.acquired_requested != floor:
                incomplete_sources.append(page)
                report.findings.append(Finding("FAIL", "SOURCE_APPROVED_ACQUISITION_DENOMINATOR", "Top_10_Investments",
                    f"{page} acquisition denominator {item.acquired_requested if item.acquired_requested is not None else 'unknown'} does not equal the approved count {floor}."))
        if strict_run:
            if publication_runs.get(page) != expected_run:
                report.findings.append(Finding("FAIL", "SOURCE_PUBLICATION_COHORT", "Top_10_Investments",
                    f"{page} must have one _Status row with a unique run={expected_run} receipt for this final-publication source cohort."))
        fresh_minimum = (_env_float("TFB_REFRESH_MIN_FRESH_PCT_"+page.upper(),95.0)
                         if min_fresh_percent is None else
                         min_fresh_percent.get(page,95.0) if isinstance(min_fresh_percent,Mapping)
                         else min_fresh_percent)
        if approved_scope is not None:
            fresh_minimum = max(95.0, fresh_minimum)
        acquisition_findings = _acquisition_findings(item,"Top_10_Investments","SOURCE",floor,fresh_minimum)
        if acquisition_findings:
            incomplete_sources.append(page)
            report.findings.extend(acquisition_findings)
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
        elif page in scope_counts and pool["total"] != floor:
            incomplete_sources.append(page)
            report.findings.append(Finding("FAIL", "T10_POOL_APPROVED_COUNT", "Top_10_Investments", f"Top-10 declares {pool['total']} {page} source rows; the approved scope contains exactly {floor}."))
        elif pool["total"] < floor:
            incomplete_sources.append(page)
            report.findings.append(Finding("FAIL", "T10_POOL_BELOW_FLOOR", "Top_10_Investments", f"Top-10 used {pool['total']} {page} rows, below approved minimum {floor}."))

    if approved_scope is not None or "pool_reconciliation_v1" in s(top10_text):
        try:
            report.top10_pool_reconciliation = _pool_reconciliation(
                top10_text, report.top10_pool_counts, status_rows, publication_runs,
                expected_run, source_grids, source_columns)
        except Exception as exc:
            incomplete_sources.append("opportunity_pool")
            report.top10_pool_reconciliation = {"valid": False,
                "error": f"{type(exc).__name__}: {exc}"}
            report.findings.append(Finding("FAIL", "T10_POOL_RECONCILIATION_UNPROVEN",
                "Top_10_Investments",
                f"Physical source membership and deduplicated opportunity pool cannot be reconciled: {type(exc).__name__}: {exc}"))

    claims_full = "(full universe)" in s(top10_text).casefold()
    if claims_full and incomplete_sources:
        report.findings.append(Finding("FAIL", "FALSE_FULL_UNIVERSE_CLAIM", "Top_10_Investments", "Status claims a full universe while required source pages are partial, stale, below their approved row floors, or physical-to-pool reconciliation is unproven."))

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


async def run_live(spreadsheet_id: str, reader: Optional[Callable[..., Any]] = None, *,
                   min_rows: Optional[Mapping[str, int]] = None,
                   market_max_age_h: Optional[float] = None,
                   decision_max_age_h: Optional[float] = None,
                   min_fresh_percent: Optional[float | Mapping[str, float]] = None,
                   expected_source_run: Optional[str] = None,
                   approved_scope: Optional["ScopePolicy"] = None) -> DecisionSurfaceReport:
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
    source_grids = {}
    source_columns = {}
    if approved_scope is not None or min_rows is None:
        try:
            approved_scope = approved_scope or _load_approved_scope()
        except Exception as exc:
            report = DecisionSurfaceReport(datetime.now(timezone.utc).isoformat(), masked)
            report.fatal = f"approved universe scope unavailable: {type(exc).__name__}: {exc}"
            return report
    try:
        reads = [
            _read_range(reader, spreadsheet_id, "_Status!A1:J100"),
            _read_range(reader, spreadsheet_id, "Portfolio_Decision!A1:B3"),
            _read_range(reader, spreadsheet_id, "Top_10_Investments!A1:B3"),
        ]
        # Ownership exclusions must be proved from every source page, rather
        # than treating the two approved-page membership hashes as proof that
        # the removed symbols exist on a different complete source page.
        scope_pages = POOL_PAGES if approved_scope is not None else ()
        reads.extend(_read_range(reader, spreadsheet_id, f"{page}!{SOURCE_SCOPE_RANGE}")
                     for page in scope_pages)
        reads.extend(_read_range(reader, spreadsheet_id, f"{page}!A:A")
                     for page in scope_pages)
        values = await asyncio.gather(*reads)
        status_grid, portfolio_grid, top10_grid = values[:3]
        source_grids = dict(zip(scope_pages, values[3:3 + len(scope_pages)]))
        source_columns = dict(zip(scope_pages, values[3 + len(scope_pages):]))
    except Exception as exc:
        report = DecisionSurfaceReport(datetime.now(timezone.utc).isoformat(), masked)
        report.fatal = f"live read failed: {type(exc).__name__}: {exc}"
        return report
    return audit_surfaces(
        status_grid,
        portfolio_grid,
        top10_grid,
        spreadsheet=masked,
        market_max_age_h=(_env_float("TFB_DECISION_SOURCE_MAX_AGE_H", 30.0)
                          if market_max_age_h is None else market_max_age_h),
        decision_max_age_h=(_env_float("TFB_DECISION_SURFACE_MAX_AGE_H", 8.0)
                            if decision_max_age_h is None else decision_max_age_h),
        min_rows=min_rows,
        min_fresh_percent=min_fresh_percent,
        expected_source_run=expected_source_run,
        approved_scope=approved_scope,
        source_grids=source_grids,
        source_columns=source_columns,
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
