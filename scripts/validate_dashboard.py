#!/usr/bin/env python3
# scripts/validate_dashboard.py
"""
================================================================================
TADAWUL FAST BRIDGE — DASHBOARD CONTRACT & GATE-INTEGRITY VALIDATOR (v1.0.0)
================================================================================
Post-refresh "System_Validation" gate. Reads the LIVE rendered sheet and checks
it against the DEPLOYED core.sheets.schema_registry, then asserts the
investability-gate verdicts are internally consistent. Designed to run in CI /
daily_sync with a meaningful exit code so a broken deploy fails loudly instead
of silently shipping a truncated or inconsistent sheet.

WHY THIS SCRIPT (and why it is NOT audit_data_quality.py)
---------------------------------------------------------
audit_data_quality.py audits each symbol's ENRICHED QUOTE (engine per-symbol
output) for freshness / unit-drift / provider-repair warnings. drift_detection.py
measures ML feature-distribution drift. Neither inspects the RENDERED SHEET for
structural correctness or gate-verdict consistency -- and the recurring failure
mode in this project is exactly the deploy gap where the engine emits the full
schema (e.g. 115 canonical keys) but the sheet header was never re-widened, so
the writer silently drops the extra columns. Auditing the engine's quote output
cannot see that, because it never looks at what actually landed on the sheet.

This validator therefore reads the LITERAL sheet cells (read_range) and compares
the actual header row to schema_registry.get_sheet_headers(page). It pulls the
EXPECTED width/order/headers from whatever registry is deployed at runtime, so:
  - sheet narrower than registry  -> CONTRACT FAIL (the classic deploy gap)
  - registry narrower than current code -> still surfaced (registry didn't deploy)
  - header order/content mismatch -> CONTRACT FAIL with the first divergence

It does NOT predict anything and assigns no reliability score. It checks that the
output is STRUCTURALLY INTACT and the gate's own fields agree with each other.
The forward-return question lives in track_performance.py; data freshness lives
in audit_data_quality.py. This is the third, orthogonal leg: output integrity.

WHAT IT CHECKS (per page)
-------------------------
CONTRACT (hard fail):
  - header count == registry width
  - last header == registry last header (e.g. "Block Reason")
  - header order/content == registry headers (reports first divergence,
    missing headers, and extra headers)

GATE INTEGRITY (hard fail; SKIPPED if the required columns are absent -- which
the CONTRACT check will already have flagged):
  - INVESTABLE rows with no current price OR no 12M forecast
  - final_action == INVEST on a REDUCE / SELL / STRONG_SELL / AVOID reco
  - BUY-family reco (STRONG_BUY / BUY / ACCUMULATE) carrying a non-empty
    block_reason
  - provider_engine_conflict == TRUE with a blank conflict_type

SANITY (warn):
  - duplicate symbols
  - current price outside [day_low, day_high]
  - current price outside [week_52_low, week_52_high]
  - expected_roi_12m beyond the v5.79.3 soft-cap ceiling (lenient; the cap
    asymptotes near +35%, this flags well past it)

TOP-10 (hard fail; only on the Top_10_Investments page):
  - any REDUCE / SELL / STRONG_SELL / AVOID reco
  - any row with a missing current price

READING THE SHEET (lesson from drift_detection.py v4.3.0)
---------------------------------------------------------
drift_detection.py v4.2.0 assumed `google_sheets_service.get_rows_for_sheet`,
which is NOT in that module's public API, collected zero rows, and failed
silently. This validator does not guess: it probes the documented surface and
degrades gracefully.
  PRIMARY:  google_sheets_service.read_range(spreadsheet_id, "<page>!A1:..N")
            -> raw rendered cells -> auto-detected header row -> dict rows.
            This is the literal sheet and is required for a TRUE contract check.
  FALLBACK: core.data_engine.get_sheet_rows / core.data_engine_v2.get_sheet_rows
            -> logical rows. The contract check is DEGRADED on this path (no
            literal header row), so it is reported as WARN with a note; gate /
            sanity checks still run on the logical rows.
Row-field resolution is dual: row.get(<canonical_key>) then row.get(<header>),
so the gate checks work whether rows are snake_case-keyed or header-keyed.

EXIT CODES
----------
  0  clean (all PASS / SKIP)
  1  WARN only (sanity flags, no structural or gate failure)
  2  FAIL (contract / gate-integrity / top-10 failure)
  3  could not run (registry unimportable, or no sheet reader available)

ENVIRONMENT
-----------
  VALIDATE_SHEET_ID / DEFAULT_SPREADSHEET_ID   spreadsheet id
  VALIDATE_PAGES                               comma-separated page list
  VALIDATE_MAX_ROWS                            max data rows per page (default 1500)
  VALIDATE_JSON_OUT                            write JSON report to this path
  VALIDATE_WRITE_SHEET                         truthy = also write Dashboard_Audit tab
  VALIDATE_AUDIT_TAB                           audit tab name (default Dashboard_Audit)
  GOOGLE_SHEETS_CREDENTIALS / GOOGLE_CREDENTIALS   service account (JSON or b64)
  LOG_LEVEL                                    logger level (default INFO)
================================================================================
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import importlib
import inspect
import json
import logging
import math
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
SCRIPT_VERSION = "1.0.0"
SERVICE_VERSION = SCRIPT_VERSION
SCRIPT_NAME = "DashboardValidator"

# ---------------------------------------------------------------------------
# Project-wide truthy/falsy vocabulary (matches main._TRUTHY / _FALSY)
# ---------------------------------------------------------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}


def _env_bool(name: str, default: bool = False) -> bool:
    try:
        raw = (os.getenv(name, "") or "").strip().lower()
    except Exception:
        return bool(default)
    if not raw:
        return bool(default)
    if raw in _TRUTHY:
        return True
    if raw in _FALSY:
        return False
    return bool(default)


def _env_int(name: str, default: int, *, lo: Optional[int] = None) -> int:
    try:
        raw = (os.getenv(name, "") or "").strip()
        v = int(float(raw)) if raw else default
    except Exception:
        return default
    if lo is not None and v < lo:
        v = lo
    return v


def _env_csv(name: str, default: List[str]) -> List[str]:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return list(default)
    items = [x.strip() for x in raw.split(",") if x.strip()]
    return items or list(default)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("DashboardValidator")

_RIYADH_TZ = timezone(timedelta(hours=3))


def _out(s: str) -> None:
    sys.stdout.write(s + "\n")


def _riyadh_now_str() -> str:
    return datetime.now(_RIYADH_TZ).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Safe coercion
# ---------------------------------------------------------------------------
def _safe_str(x: Any) -> str:
    try:
        if x is None:
            return ""
        return str(x).strip()
    except Exception:
        return ""


def _safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, bool):
            return None
        if isinstance(x, (int, float)):
            f = float(x)
            return None if (math.isnan(f) or math.isinf(f)) else f
        s = _safe_str(x)
        if not s or s.lower() in {"na", "n/a", "null", "none", "-", "—"}:
            return None
        s = s.replace(",", "").replace("%", "")
        f = float(s)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return None


def _norm_token(x: Any) -> str:
    """Upper-case, strip, collapse separators -> matches RecommendationType
    value strings ('STRONG_BUY' -> 'STRONG BUY')."""
    s = _safe_str(x).upper().replace("_", " ").replace("-", " ").replace("/", " ")
    while "  " in s:
        s = s.replace("  ", " ")
    return s.strip()


# Canonical 8-tier vocabulary (engine schemas.py authority).
_BUY_FAMILY = {"STRONG BUY", "BUY", "ACCUMULATE"}
_SELL_FAMILY = {"REDUCE", "SELL", "STRONG SELL", "AVOID"}

# Canonical engine keys the gate/sanity checks reference.
_K_SYMBOL = "symbol"
_K_PRICE = "current_price"
_K_F12 = "forecast_price_12m"
_K_ROI12 = "expected_roi_12m"
_K_RECO = "recommendation"
_K_INVEST = "investability_status"
_K_ACTION = "final_action"
_K_BLOCK = "block_reason"
_K_CONFLICT = "provider_engine_conflict"
_K_CTYPE = "conflict_type"
_K_DAY_HI = "day_high"
_K_DAY_LO = "day_low"
_K_W52_HI = "week_52_high"
_K_W52_LO = "week_52_low"


# ---------------------------------------------------------------------------
# Result model
# ---------------------------------------------------------------------------
@dataclass
class CheckResult:
    page: str
    name: str
    status: str  # PASS | FAIL | WARN | SKIP
    count: int = 0
    examples: List[str] = field(default_factory=list)
    detail: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "page": self.page,
            "name": self.name,
            "status": self.status,
            "count": self.count,
            "examples": list(self.examples[:10]),
            "detail": self.detail,
        }


# ---------------------------------------------------------------------------
# Registry loader (probe documented module paths; no guessing)
# ---------------------------------------------------------------------------
@dataclass
class _Registry:
    module: Any
    version: str

    def headers(self, page: str) -> List[str]:
        return list(self.module.get_sheet_headers(page))

    def keys(self, page: str) -> List[str]:
        return list(self.module.get_sheet_keys(page))

    def normalize(self, page: str) -> str:
        fn = getattr(self.module, "normalize_sheet_name", None)
        if callable(fn):
            try:
                return fn(page)
            except Exception:
                return page
        return page


def _load_registry() -> Optional[_Registry]:
    for modpath in (
        "core.sheets.schema_registry",
        "schema_registry",
        "core.schema_registry",
        "sheets.schema_registry",
    ):
        try:
            mod = importlib.import_module(modpath)
        except Exception:
            continue
        if all(callable(getattr(mod, fn, None)) for fn in ("get_sheet_headers", "get_sheet_keys")):
            ver = _safe_str(getattr(mod, "SCHEMA_VERSION", "")) or _safe_str(getattr(mod, "__version__", "")) or "unknown"
            logger.info("schema_registry loaded from %s (SCHEMA_VERSION=%s)", modpath, ver)
            return _Registry(module=mod, version=ver)
    return None


def _engine_version() -> str:
    for modpath in ("core.data_engine_v2", "core.data_engine"):
        try:
            mod = importlib.import_module(modpath)
        except Exception:
            continue
        v = _safe_str(getattr(mod, "__version__", ""))
        if v:
            return f"{modpath}={v}"
    return "unknown"


# ---------------------------------------------------------------------------
# Sheet reader: read_range (literal cells) primary; get_sheet_rows fallback
# ---------------------------------------------------------------------------
def _project_root_on_path() -> None:
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        root = os.path.dirname(here)
        for p in (here, root):
            if p and p not in sys.path:
                sys.path.insert(0, p)
    except Exception:
        pass


_project_root_on_path()

# A1 end column wide enough to catch over-width sheets (DZ = 130 columns).
_READ_END_COL = "DZ"


def _resolve_read_range() -> Optional[Callable]:
    for modpath in (
        "integrations.google_sheets_service",
        "core.integrations.google_sheets_service",
        "google_sheets_service",
        "core.google_sheets_service",
    ):
        try:
            mod = importlib.import_module(modpath)
        except Exception:
            continue
        fn = getattr(mod, "read_range", None)
        if callable(fn):
            logger.info("sheet reader: %s.read_range", modpath)
            return fn
    return None


async def _resolve_get_sheet_rows() -> Optional[Tuple[Callable, bool, str]]:
    for modpath in ("core.data_engine", "core.data_engine_v2"):
        try:
            mod = importlib.import_module(modpath)
        except Exception:
            continue
        fn = getattr(mod, "get_sheet_rows", None)
        if callable(fn):
            return fn, inspect.iscoroutinefunction(fn), modpath
    return None


def _detect_header_row(
    grid: List[List[Any]], expected_tokens: set, scan: int = 14
) -> int:
    """Return the index of the row most resembling the header (max overlap
    with expected headers/keys). -1 if nothing plausible."""
    best_idx, best_overlap = -1, 0
    for i, row in enumerate(grid[:scan]):
        if not isinstance(row, list):
            continue
        cells = {_safe_str(c) for c in row if _safe_str(c)}
        if not cells:
            continue
        overlap = len(cells & expected_tokens)
        if overlap > best_overlap:
            best_overlap, best_idx = overlap, i
    return best_idx if best_overlap >= 3 else -1


def _grid_to_rows(
    header_cells: List[str], data_grid: List[List[Any]]
) -> List[Dict[str, Any]]:
    headers = [_safe_str(h) for h in header_cells]
    rows: List[Dict[str, Any]] = []
    for raw in data_grid:
        if not isinstance(raw, list):
            continue
        if not any(_safe_str(c) for c in raw):
            continue  # skip fully blank rows
        padded = list(raw) + [None] * max(0, len(headers) - len(raw))
        d: Dict[str, Any] = {}
        for i, h in enumerate(headers):
            if h:
                d[h] = padded[i]
        rows.append(d)
    return rows


@dataclass
class _PageData:
    page: str
    source: str            # "read_range" | "get_sheet_rows" | "none"
    header_cells: List[str]  # literal header row (empty on get_sheet_rows path)
    rows: List[Dict[str, Any]]
    error: str = ""


async def _read_page(
    page: str,
    sid: str,
    reg: _Registry,
    read_range: Optional[Callable],
    rows_reader: Optional[Tuple[Callable, bool, str]],
    max_rows: int,
) -> _PageData:
    expected_tokens = set(reg.headers(page)) | set(reg.keys(page))

    # PRIMARY: literal cells via read_range
    if read_range is not None and sid:
        rng = f"{page}!A1:{_READ_END_COL}{max(2, max_rows + 25)}"
        grid: Any = None
        try:
            loop = asyncio.get_running_loop()
            grid = await loop.run_in_executor(None, lambda: read_range(sid, rng))
            grid = await _maybe_await(grid)
        except Exception as e:
            logger.warning("read_range failed for %s: %s", page, e)
            grid = None
        if isinstance(grid, list) and len(grid) >= 1:
            hr = _detect_header_row(grid, expected_tokens)
            if hr >= 0:
                header_cells = [_safe_str(c) for c in grid[hr]]
                rows = _grid_to_rows(header_cells, grid[hr + 1 : hr + 1 + max_rows])
                return _PageData(page, "read_range", header_cells, rows)
            logger.warning("read_range: header row not detected for %s", page)

    # FALLBACK: logical rows via engine get_sheet_rows (no literal header row)
    if rows_reader is not None:
        fn, is_async, modpath = rows_reader
        attempts: Tuple[Tuple[Tuple, Dict[str, Any]], ...] = (
            ((), {"sheet": page, "limit": max_rows}),
            ((), {"sheet_name": page, "limit": max_rows}),
            ((), {"page": page, "limit": max_rows}),
            ((page,), {"limit": max_rows}),
            ((page,), {}),
        )
        for args, kwargs in attempts:
            try:
                res = fn(*args, **kwargs) if is_async else fn(*args, **kwargs)
                res = await res if is_async else await _maybe_await(res)
                rows = _extract_rows(res)
                return _PageData(page, "get_sheet_rows", [], rows)
            except TypeError:
                continue
            except Exception as e:
                return _PageData(page, "none", [], [], error=f"{modpath}.get_sheet_rows: {e}")

    return _PageData(page, "none", [], [], error="no reader produced rows")


async def _maybe_await(x: Any) -> Any:
    return await x if inspect.isawaitable(x) else x


def _extract_rows(payload: Any) -> List[Dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        for k in ("rows", "row_objects", "items", "records", "data", "quotes", "results"):
            seq = payload.get(k)
            if isinstance(seq, list) and seq and isinstance(seq[0], dict):
                return [r for r in seq if isinstance(r, dict)]
    return []


# ---------------------------------------------------------------------------
# Row-field resolution (dual: canonical key, then header name)
# ---------------------------------------------------------------------------
def _resolve(row: Dict[str, Any], k2h: Dict[str, str], key: str) -> Any:
    if key in row:
        return row.get(key)
    h = k2h.get(key)
    if h and h in row:
        return row.get(h)
    return None


def _present(row: Dict[str, Any], k2h: Dict[str, str], key: str) -> bool:
    return (key in row) or (k2h.get(key, "") in row)


def _has_column(actual_set: set, k2h: Dict[str, str], key: str) -> bool:
    """True if the page can carry this canonical key (its header is on the
    sheet, or the registry maps it)."""
    h = k2h.get(key, "")
    return bool(h) and (h in actual_set or not actual_set)


# ---------------------------------------------------------------------------
# CONTRACT check
# ---------------------------------------------------------------------------
def check_contract(page: str, expected_headers: List[str], actual_headers: List[str]) -> CheckResult:
    exp = [_safe_str(h) for h in expected_headers]
    # trim trailing empties from the actual header row
    act = [_safe_str(h) for h in actual_headers]
    while act and act[-1] == "":
        act.pop()

    if act == exp:
        return CheckResult(page, "contract.header_match", "PASS",
                           detail=f"{len(act)} columns, ends with '{act[-1] if act else ''}'")

    detail_bits: List[str] = [f"expected {len(exp)} cols, found {len(act)}"]
    if exp:
        exp_last = exp[-1]
        act_last = act[-1] if act else "(none)"
        if act_last != exp_last:
            detail_bits.append(f"last col '{act_last}' != expected '{exp_last}'")
    missing = [h for h in exp if h not in act]
    extra = [h for h in act if h not in exp]
    if missing:
        detail_bits.append("missing: " + ", ".join(missing[:8]) + (" ..." if len(missing) > 8 else ""))
    if extra:
        detail_bits.append("extra: " + ", ".join(extra[:8]) + (" ..." if len(extra) > 8 else ""))
    if not missing and not extra:
        # same set, different order -> find first divergence
        for i in range(min(len(exp), len(act))):
            if exp[i] != act[i]:
                detail_bits.append(f"order diverges at col {i + 1}: '{act[i]}' vs '{exp[i]}'")
                break
    examples = (missing[:5] or extra[:5])
    return CheckResult(page, "contract.header_match", "FAIL",
                       count=len(missing) + len(extra), examples=examples,
                       detail="; ".join(detail_bits))


def check_contract_logical(page: str, expected_keys: List[str], rows: List[Dict[str, Any]]) -> CheckResult:
    """Degraded contract check for the get_sheet_rows path (no literal header
    row). Verifies the logical rows expose the expected canonical keys."""
    if not rows:
        return CheckResult(page, "contract.keys_present", "WARN", detail="no rows to inspect (logical path)")
    seen = set()
    for r in rows[:50]:
        seen |= set(r.keys())
    missing = [k for k in expected_keys if k not in seen]
    if not missing:
        return CheckResult(page, "contract.keys_present", "WARN",
                           detail="logical rows expose all expected keys, but the LITERAL sheet header was not read (read_range unavailable) -- run with a reachable google_sheets_service for a true contract check")
    return CheckResult(page, "contract.keys_present", "FAIL", count=len(missing),
                       examples=missing[:8],
                       detail="logical rows missing expected keys: " + ", ".join(missing[:8]))


# ---------------------------------------------------------------------------
# GATE INTEGRITY checks
# ---------------------------------------------------------------------------
def check_gate(page: str, k2h: Dict[str, str], actual_set: set, rows: List[Dict[str, Any]]) -> List[CheckResult]:
    out: List[CheckResult] = []

    def sym(r: Dict[str, Any]) -> str:
        return _safe_str(_resolve(r, k2h, _K_SYMBOL)) or "?"

    # 1) INVESTABLE with no price or no 12M forecast
    if _has_column(actual_set, k2h, _K_INVEST):
        bad: List[str] = []
        for r in rows:
            if _norm_token(_resolve(r, k2h, _K_INVEST)) != "INVESTABLE":
                continue
            price = _safe_float(_resolve(r, k2h, _K_PRICE))
            f12 = _safe_float(_resolve(r, k2h, _K_F12))
            if price is None or price <= 0 or f12 is None or f12 <= 0:
                bad.append(sym(r))
        out.append(CheckResult(page, "gate.investable_has_price_and_forecast",
                               "FAIL" if bad else "PASS", count=len(bad), examples=bad,
                               detail="INVESTABLE rows missing current price or 12M forecast" if bad else ""))
    else:
        out.append(CheckResult(page, "gate.investable_has_price_and_forecast", "SKIP",
                               detail="investability_status column not present (see contract)"))

    # 2) final_action == INVEST on a sell-family reco
    if _has_column(actual_set, k2h, _K_ACTION) and _has_column(actual_set, k2h, _K_RECO):
        bad = []
        for r in rows:
            if _norm_token(_resolve(r, k2h, _K_ACTION)) != "INVEST":
                continue
            if _norm_token(_resolve(r, k2h, _K_RECO)) in _SELL_FAMILY:
                bad.append(sym(r))
        out.append(CheckResult(page, "gate.no_invest_on_sell_reco",
                               "FAIL" if bad else "PASS", count=len(bad), examples=bad,
                               detail="final_action=INVEST on a REDUCE/SELL/STRONG_SELL/AVOID reco" if bad else ""))
    else:
        out.append(CheckResult(page, "gate.no_invest_on_sell_reco", "SKIP",
                               detail="final_action or recommendation column not present"))

    # 3) BUY-family reco carrying a non-empty block_reason
    if _has_column(actual_set, k2h, _K_BLOCK) and _has_column(actual_set, k2h, _K_RECO):
        bad = []
        for r in rows:
            if _norm_token(_resolve(r, k2h, _K_RECO)) not in _BUY_FAMILY:
                continue
            if _safe_str(_resolve(r, k2h, _K_BLOCK)):
                bad.append(sym(r))
        out.append(CheckResult(page, "gate.buy_has_no_block_reason",
                               "FAIL" if bad else "PASS", count=len(bad), examples=bad,
                               detail="BUY-family reco with a non-empty block_reason" if bad else ""))
    else:
        out.append(CheckResult(page, "gate.buy_has_no_block_reason", "SKIP",
                               detail="block_reason or recommendation column not present"))

    # 4) provider_engine_conflict TRUE with blank conflict_type
    if _has_column(actual_set, k2h, _K_CONFLICT) and _has_column(actual_set, k2h, _K_CTYPE):
        bad = []
        for r in rows:
            if _norm_token(_resolve(r, k2h, _K_CONFLICT)) not in {"TRUE", "YES", "1"}:
                continue
            if not _safe_str(_resolve(r, k2h, _K_CTYPE)):
                bad.append(sym(r))
        out.append(CheckResult(page, "gate.conflict_has_type",
                               "FAIL" if bad else "PASS", count=len(bad), examples=bad,
                               detail="provider_engine_conflict=TRUE with blank conflict_type" if bad else ""))
    else:
        out.append(CheckResult(page, "gate.conflict_has_type", "SKIP",
                               detail="provider_engine_conflict or conflict_type column not present"))

    return out


# ---------------------------------------------------------------------------
# SANITY checks (warn)
# ---------------------------------------------------------------------------
def check_sanity(page: str, k2h: Dict[str, str], actual_set: set, rows: List[Dict[str, Any]]) -> List[CheckResult]:
    out: List[CheckResult] = []

    def sym(r: Dict[str, Any]) -> str:
        return _safe_str(_resolve(r, k2h, _K_SYMBOL)) or "?"

    # duplicate symbols
    seen: Dict[str, int] = {}
    for r in rows:
        s = sym(r)
        if s and s != "?":
            seen[s] = seen.get(s, 0) + 1
    dups = [f"{s}x{n}" for s, n in seen.items() if n > 1]
    out.append(CheckResult(page, "sanity.no_duplicate_symbols",
                           "WARN" if dups else "PASS", count=len(dups), examples=dups,
                           detail="duplicate symbols on page" if dups else ""))

    # price outside day range
    if _has_column(actual_set, k2h, _K_DAY_HI) and _has_column(actual_set, k2h, _K_DAY_LO):
        bad: List[str] = []
        for r in rows:
            p = _safe_float(_resolve(r, k2h, _K_PRICE))
            hi = _safe_float(_resolve(r, k2h, _K_DAY_HI))
            lo = _safe_float(_resolve(r, k2h, _K_DAY_LO))
            if p is None or hi is None or lo is None or hi <= 0 or lo <= 0:
                continue
            if p < lo or p > hi:
                bad.append(sym(r))
        out.append(CheckResult(page, "sanity.price_in_day_range",
                               "WARN" if bad else "PASS", count=len(bad), examples=bad,
                               detail="current price outside [day_low, day_high]" if bad else ""))

    # price outside 52w range
    if _has_column(actual_set, k2h, _K_W52_HI) and _has_column(actual_set, k2h, _K_W52_LO):
        bad = []
        for r in rows:
            p = _safe_float(_resolve(r, k2h, _K_PRICE))
            hi = _safe_float(_resolve(r, k2h, _K_W52_HI))
            lo = _safe_float(_resolve(r, k2h, _K_W52_LO))
            if p is None or hi is None or lo is None or hi <= 0 or lo <= 0:
                continue
            if p < lo or p > hi:
                bad.append(sym(r))
        out.append(CheckResult(page, "sanity.price_in_52w_range",
                               "WARN" if bad else "PASS", count=len(bad), examples=bad,
                               detail="current price outside [week_52_low, week_52_high]" if bad else ""))

    # expected_roi_12m beyond soft-cap ceiling (lenient; cap asymptotes ~+0.35)
    if _has_column(actual_set, k2h, _K_ROI12):
        bad = []
        for r in rows:
            v = _safe_float(_resolve(r, k2h, _K_ROI12))
            if v is None:
                continue
            # value may be fraction (0.35) or points (35); use a lenient ceiling either way
            over = (abs(v) <= 1.5 and abs(v) > 0.40) or (abs(v) > 1.5 and abs(v) > 40.0)
            if over:
                bad.append(f"{sym(r)}={v}")
        out.append(CheckResult(page, "sanity.roi12_within_softcap",
                               "WARN" if bad else "PASS", count=len(bad), examples=bad,
                               detail="expected_roi_12m beyond soft-cap ceiling (review)" if bad else ""))

    return out


# ---------------------------------------------------------------------------
# TOP-10 checks (hard fail)
# ---------------------------------------------------------------------------
def check_top10(page: str, k2h: Dict[str, str], actual_set: set, rows: List[Dict[str, Any]]) -> List[CheckResult]:
    out: List[CheckResult] = []

    def sym(r: Dict[str, Any]) -> str:
        return _safe_str(_resolve(r, k2h, _K_SYMBOL)) or "?"

    if _has_column(actual_set, k2h, _K_RECO):
        bad = [sym(r) for r in rows if _norm_token(_resolve(r, k2h, _K_RECO)) in _SELL_FAMILY]
        out.append(CheckResult(page, "top10.no_sell_family",
                               "FAIL" if bad else "PASS", count=len(bad), examples=bad,
                               detail="Top 10 contains a REDUCE/SELL/STRONG_SELL/AVOID reco" if bad else ""))
    else:
        out.append(CheckResult(page, "top10.no_sell_family", "SKIP",
                               detail="recommendation column not present"))

    bad = []
    for r in rows:
        p = _safe_float(_resolve(r, k2h, _K_PRICE))
        if p is None or p <= 0:
            bad.append(sym(r))
    out.append(CheckResult(page, "top10.no_missing_price",
                           "FAIL" if bad else "PASS", count=len(bad), examples=bad,
                           detail="Top 10 row with missing current price" if bad else ""))
    return out


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
_DEFAULT_PAGES = [
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
    "Top_10_Investments",
]


async def validate(
    *, pages: List[str], sid: str, max_rows: int
) -> Tuple[List[CheckResult], Dict[str, Any]]:
    reg = _load_registry()
    if reg is None:
        return [], {"fatal": "schema_registry not importable (tried core.sheets.schema_registry and fallbacks)"}

    read_range = _resolve_read_range()
    rows_reader = await _resolve_get_sheet_rows()
    if read_range is None and rows_reader is None:
        return [], {"fatal": "no sheet reader available (neither google_sheets_service.read_range nor get_sheet_rows)"}

    meta = {
        "registry_version": reg.version,
        "engine_version": _engine_version(),
        "reader": "read_range" if read_range is not None else "get_sheet_rows",
        "generated_riyadh": _riyadh_now_str(),
    }

    results: List[CheckResult] = []
    for raw_page in pages:
        page = reg.normalize(raw_page) or raw_page
        try:
            expected_headers = reg.headers(page)
            expected_keys = reg.keys(page)
        except Exception as e:
            results.append(CheckResult(page, "contract.header_match", "FAIL",
                                       detail=f"registry has no spec for page: {e}"))
            continue

        k2h = dict(zip(expected_keys, expected_headers))
        pdata = await _read_page(page, sid, reg, read_range, rows_reader, max_rows)

        if pdata.error and not pdata.rows:
            results.append(CheckResult(page, "read", "FAIL", detail=pdata.error))
            continue

        # CONTRACT
        if pdata.source == "read_range":
            results.append(check_contract(page, expected_headers, pdata.header_cells))
            actual_set = {_safe_str(h) for h in pdata.header_cells if _safe_str(h)}
        else:
            results.append(check_contract_logical(page, expected_keys, pdata.rows))
            actual_set = set()  # empty => _has_column relies on registry mapping

        logger.info("Page=%s | source=%s | rows=%d", page, pdata.source, len(pdata.rows))

        # GATE + SANITY
        results.extend(check_gate(page, k2h, actual_set, pdata.rows))
        results.extend(check_sanity(page, k2h, actual_set, pdata.rows))

        # TOP-10 (only on the Top10 page)
        if reg.normalize("Top_10_Investments") in (page, reg.normalize(page)):
            results.extend(check_top10(page, k2h, actual_set, pdata.rows))

    return results, meta


# ---------------------------------------------------------------------------
# Optional: write a Dashboard_Audit tab (best-effort, gspread)
# ---------------------------------------------------------------------------
def _write_audit_tab(sid: str, tab: str, results: List[CheckResult], meta: Dict[str, Any]) -> bool:
    try:
        import gspread  # type: ignore
        from google.oauth2 import service_account  # type: ignore
    except Exception:
        logger.warning("Dashboard_Audit write skipped: gspread/google-auth not installed")
        return False

    raw = (os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS") or "").strip()
    gc = None
    try:
        if raw:
            s = raw
            if not s.startswith("{"):
                try:
                    dec = base64.b64decode(s).decode("utf-8", errors="replace").strip()
                    if dec.startswith("{"):
                        s = dec
                except Exception:
                    pass
            info = json.loads(s)
            creds = service_account.Credentials.from_service_account_info(
                info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
            )
            gc = gspread.authorize(creds)
        else:
            gc = gspread.service_account()
    except Exception as e:
        logger.warning("Dashboard_Audit write skipped: credential load failed: %s", e)
        return False

    try:
        sh = gc.open_by_key(sid)
        try:
            ws = sh.worksheet(tab)
        except Exception:
            ws = sh.add_worksheet(title=tab, rows=400, cols=8)
        block: List[List[Any]] = [
            ["Dashboard Validation", f"Generated: {meta.get('generated_riyadh','')}", "", "", ""],
            [f"registry={meta.get('registry_version','')}", f"engine={meta.get('engine_version','')}",
             f"reader={meta.get('reader','')}", "", ""],
            ["Page", "Check", "Status", "Count", "Examples"],
        ]
        for r in results:
            block.append([r.page, r.name, r.status, r.count, ", ".join(r.examples[:6])])
        # pad to a stable height is unnecessary; clear then write
        try:
            ws.batch_clear(["A1:E10000"])
        except Exception:
            pass
        ws.update("A1", block)
        logger.info("Wrote %d check rows to '%s'", len(results), tab)
        return True
    except Exception as e:
        logger.warning("Dashboard_Audit write failed: %s", e)
        return False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _exit_code(results: List[CheckResult]) -> int:
    has_fail = any(r.status == "FAIL" for r in results)
    has_warn = any(r.status == "WARN" for r in results)
    if has_fail:
        return 2
    if has_warn:
        return 1
    return 0


def _print_report(results: List[CheckResult], meta: Dict[str, Any]) -> None:
    _out("=" * 72)
    _out("DASHBOARD VALIDATION  |  registry=%s  engine=%s  reader=%s" % (
        meta.get("registry_version", "?"), meta.get("engine_version", "?"), meta.get("reader", "?")))
    _out("=" * 72)
    by_page: Dict[str, List[CheckResult]] = {}
    for r in results:
        by_page.setdefault(r.page, []).append(r)
    for page, checks in by_page.items():
        _out(f"\n[{page}]")
        for c in checks:
            line = f"  {c.status:<4} | {c.name}"
            if c.count:
                line += f"  (n={c.count})"
            if c.detail:
                line += f"  — {c.detail}"
            _out(line)
            if c.examples and c.status in {"FAIL", "WARN"}:
                _out(f"         e.g. {', '.join(c.examples[:8])}")
    fails = [r for r in results if r.status == "FAIL"]
    warns = [r for r in results if r.status == "WARN"]
    _out("\n" + "-" * 72)
    _out(f"RESULT: {len(fails)} FAIL, {len(warns)} WARN, "
         f"{sum(1 for r in results if r.status == 'PASS')} PASS, "
         f"{sum(1 for r in results if r.status == 'SKIP')} SKIP")


def create_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=f"TFB Dashboard Validator v{SCRIPT_VERSION}")
    p.add_argument("--sheet-id", default=os.getenv("VALIDATE_SHEET_ID") or None,
                   help="Spreadsheet ID (also VALIDATE_SHEET_ID / DEFAULT_SPREADSHEET_ID env).")
    p.add_argument("--pages", nargs="+", default=_env_csv("VALIDATE_PAGES", _DEFAULT_PAGES),
                   help="Pages to validate (also VALIDATE_PAGES env as CSV).")
    p.add_argument("--max-rows", type=int, default=_env_int("VALIDATE_MAX_ROWS", 1500, lo=1),
                   help="Max data rows per page (also VALIDATE_MAX_ROWS env).")
    p.add_argument("--json-out", default=os.getenv("VALIDATE_JSON_OUT") or "",
                   help="Write JSON report to this path (also VALIDATE_JSON_OUT env).")
    p.add_argument("--write-sheet", type=int, default=(1 if _env_bool("VALIDATE_WRITE_SHEET", False) else 0),
                   help="1 = also write the Dashboard_Audit tab (also VALIDATE_WRITE_SHEET env).")
    p.add_argument("--audit-tab", default=os.getenv("VALIDATE_AUDIT_TAB", "Dashboard_Audit"),
                   help="Audit tab name (also VALIDATE_AUDIT_TAB env).")
    return p


def _resolve_sid(args: argparse.Namespace) -> str:
    for v in (args.sheet_id, os.getenv("VALIDATE_SHEET_ID"), os.getenv("DEFAULT_SPREADSHEET_ID"),
              os.getenv("SPREADSHEET_ID")):
        s = _safe_str(v)
        if s:
            return s
    return ""


async def async_main() -> int:
    args = create_parser().parse_args()
    sid = _resolve_sid(args)
    if not sid:
        logger.error("No spreadsheet ID. Use --sheet-id or set VALIDATE_SHEET_ID / DEFAULT_SPREADSHEET_ID.")
        return 3

    results, meta = await validate(pages=list(args.pages or _DEFAULT_PAGES), sid=sid, max_rows=int(args.max_rows))

    if meta.get("fatal"):
        logger.error("Cannot validate: %s", meta["fatal"])
        return 3

    _print_report(results, meta)

    if args.json_out:
        try:
            payload = {"meta": meta, "results": [r.to_dict() for r in results]}
            from pathlib import Path
            Path(args.json_out).parent.mkdir(parents=True, exist_ok=True)
            Path(args.json_out).write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
            logger.info("JSON report written to %s", args.json_out)
        except Exception as e:
            logger.warning("Failed to write JSON report: %s", e)

    if int(args.write_sheet) == 1:
        _write_audit_tab(sid, args.audit_tab, results, meta)

    code = _exit_code(results)
    logger.info("Validation complete | exit=%d", code)
    return code


def main() -> int:
    try:
        return asyncio.run(async_main())
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        logger.exception("Fatal error: %s", e)
        return 3


__all__ = [
    "SCRIPT_VERSION",
    "SERVICE_VERSION",
    "CheckResult",
    "check_contract",
    "check_contract_logical",
    "check_gate",
    "check_sanity",
    "check_top10",
    "validate",
    "create_parser",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
