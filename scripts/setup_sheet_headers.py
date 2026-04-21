#!/usr/bin/env python3
# scripts/setup_sheet_headers.py
"""
================================================================================
TADAWUL FAST BRIDGE — SHEET HEADER SETUP — v7.1.0
(SCHEMA-REGISTRY FIRST / PHASE-2 ALIGNED / SAFE / EXACT CONTRACTS)
================================================================================

What this script does
---------------------
- Uses the authoritative schema contract from:
    1) core.sheets.schema_registry   (preferred)
    2) core.schemas                  (fallback)
- Creates missing canonical tabs when requested
- Writes EXACT canonical headers for each page
- Aligns start cells with the live project:
    - Data_Dictionary   -> A1
    - all other pages   -> A5
- Seeds Insights_Analysis criteria block at A1:D4 if empty
- Applies stable formatting:
    - freeze panes
    - header styling
    - filter
    - per-column widths
    - per-column number formats from schema metadata
    - row banding
- Blocks forbidden/removed pages:
    - KSA_Tadawul
    - Advisor_Criteria

Why this revision (v7.1.0 vs v7.0.0)
-------------------------------------
- FIX: Removed dead imports (`Iterable` from typing, `HttpError` from
     googleapiclient.errors) — neither was referenced in the body.
- FIX: Added project-standard `_TRUTHY`/`_FALSY` vocabulary (matches
     `main._TRUTHY`/`_FALSY`) plus `_env_bool`, `_env_int`, `_env_csv`
     helpers.
- FIX: Added `SERVICE_VERSION = SCRIPT_VERSION` alias (cross-script
     convention with `run_dashboard_sync v6.5.0` / `run_market_scan v5.3.0`
     / `refresh_data v5.2.0` / `repo_hygiene_check v5.1.0` / etc.).
- FIX: Added `SETUP_*` env var defaults for every CLI flag so the runner
     can be driven purely from the environment in cron/CI.
- 🔑 FIX HIGH correctness: When schema resolution completely fails,
     `get_column_specs_for_page` used to silently fall back to FIVE hard-
     coded headers (`Symbol, Name, Current Price, Recommendation, Error`).
     Writing 5 headers onto a sheet whose canonical width is 80 columns
     corrupts column alignment for every downstream write. v7.1.0:
       - Elevates the fallback log from WARNING to ERROR.
       - Adds a new `--require-schema` flag (SETUP_REQUIRE_SCHEMA=true)
         that hard-fails with exit code 1 instead of falling back.
       - Adds a new `--min-header-count N` safety check: if the resolved
         header count falls below N for a page NOT in DEFAULT_CANONICAL_
         NARROW_PAGES (which are Data_Dictionary/Insights_Analysis/
         Top_10_Investments), the tab is skipped with a clear error.
- FIX: Added `--json` flag for structured stdout output (useful for CI
     pipelines that parse script results).
- FIX: Fully documents exit codes: 0/1/2 (+ 130 SIGINT).

Important project alignment
---------------------------
- This script is schema-first and contract-safe.
- It does NOT write bilingual headers because that would break exact schema
  matching used by the backend and Google Sheets workflows.
- Percent formatting defaults to "points" mode, which matches the current
  backend convention in this project:
      1.23 means 1.23%  (NOT 123%)
  If your backend later emits fractional percentages (0.0123 = 1.23%),
  use:
      --percent-mode fraction

Environment
-----------
  DEFAULT_SPREADSHEET_ID   spreadsheet id (also SETUP_SHEET_ID)
  SETUP_SHEET_ID           spreadsheet id override (preferred)
  TFB_PERCENT_MODE         points | fraction (also SETUP_PERCENT_MODE)
  SETUP_PERCENT_MODE       percent mode override
  SETUP_TABS               comma-separated tabs (overrides --tabs)
  SETUP_ALL_EXISTING       process all existing tabs (truthy)
  SETUP_CREATE_MISSING     create missing tabs (truthy)
  SETUP_FORCE              force rewrite (truthy)
  SETUP_CLEAR              clear header row before writing (truthy)
  SETUP_NO_STYLE           skip formatting (truthy)
  SETUP_DRY_RUN            preview only (truthy)
  SETUP_PARALLEL           parallel processing (truthy)
  SETUP_RESIZE_GRID        grow grid to fit schema (truthy)
  SETUP_GRID_ROWS          target grid rows
  SETUP_GRID_COLS          target grid cols
  SETUP_REQUIRE_SCHEMA     hard-fail if schema module not found (truthy)
  SETUP_MIN_HEADER_COUNT   minimum accepted header count (default 7)
  SETUP_JSON               print JSON result to stdout (truthy)
  SETUP_SCHEMA_VERSION     schema version (default "vNext")
  SETUP_START_CELL         override start cell
  SETUP_ROW                override header row number
  LOG_LEVEL                logger level (default INFO)

Exit codes
----------
  0 = all targets succeeded (or skipped harmlessly)
  1 = config/bootstrap error (no sheet id, no service module, no targets,
      or --require-schema set and schema module unavailable)
  2 = at least one target failed during processing
  130 = interrupted (SIGINT)

Typical usage
-------------
1) Initialize all canonical tabs, create missing, apply formatting:
    python scripts/setup_sheet_headers.py --create-missing

2) Re-apply exact canonical headers + formatting:
    python scripts/setup_sheet_headers.py --force

3) Process selected tabs only:
    python scripts/setup_sheet_headers.py --tabs Market_Leaders My_Portfolio

4) Dry run:
    python scripts/setup_sheet_headers.py --dry-run

5) If backend ever switches to fractional percent values:
    python scripts/setup_sheet_headers.py --percent-mode fraction

6) Cron/CI (no CLI args needed):
    SETUP_SHEET_ID=abc SETUP_CREATE_MISSING=true SETUP_FORCE=true \
    SETUP_JSON=true python scripts/setup_sheet_headers.py
================================================================================
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json as _json
import logging
import os
import random
import re
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

# =============================================================================
# Version + Logging
# =============================================================================

SCRIPT_VERSION = "7.1.0"
SERVICE_VERSION = SCRIPT_VERSION  # v7.1.0: cross-script alias

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("TFB.SetupSheetHeaders")


# =============================================================================
# Project-wide truthy/falsy vocabulary (matches main._TRUTHY / _FALSY)
# =============================================================================
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


def _env_int(
    name: str, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None
) -> int:
    try:
        raw = (os.getenv(name, "") or "").strip()
        if not raw:
            return default
        v = int(float(raw))
    except Exception:
        return default
    if lo is not None and v < lo:
        v = lo
    if hi is not None and v > hi:
        v = hi
    return v


def _env_csv(name: str, default: Optional[List[str]] = None) -> Optional[List[str]]:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    items = [x.strip() for x in raw.split(",") if x.strip()]
    return items or default


# =============================================================================
# Constants
# =============================================================================

DEFAULT_FORBIDDEN_PAGES = {"KSA_Tadawul", "Advisor_Criteria"}
DEFAULT_CANONICAL_PAGES = [
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
    "Insights_Analysis",
    "Top_10_Investments",
    "Data_Dictionary",
]

# Pages that are legitimately narrow (canonical widths < 80):
#   Data_Dictionary     = 9 cols
#   Insights_Analysis   = 7 cols
# For any other page, the min-header-count guard applies.
DEFAULT_NARROW_PAGES = {"Data_Dictionary", "Insights_Analysis"}

INSIGHTS_CRITERIA_BLOCK = [
    ["Advisor Criteria", "", "", ""],
    ["Risk Level", "Moderate", "Confidence Level", "High"],
    ["Invest Period (days)", "90", "Min Expected ROI %", "3"],
    [
        "Max Risk",
        "Moderate",
        "Pages Selected",
        "Market_Leaders,Global_Markets,Commodities_FX,Mutual_Funds,My_Portfolio",
    ],
]

# Default min accepted header count for full-width pages. Anything below
# this for a non-narrow page is treated as a schema-resolution failure.
DEFAULT_MIN_HEADER_COUNT = 7

# =============================================================================
# Basic helpers
# =============================================================================

def _strip(value: Any) -> str:
    try:
        return str(value).strip()
    except Exception:
        return ""


def _normalize_header_key(value: Any) -> str:
    s = _strip(value).lower()
    return re.sub(r"[^a-z0-9]", "", s)


def _normalize_page_token(value: Any) -> str:
    s = _strip(value).lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


def _is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def index_to_col(idx_1_based: int) -> str:
    idx = max(1, int(idx_1_based))
    out = ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        out = chr(rem + ord("A")) + out
    return out or "A"


def col_to_index(col: str) -> int:
    s = (_strip(col) or "A").upper()
    n = 0
    for ch in s:
        if "A" <= ch <= "Z":
            n = n * 26 + (ord(ch) - ord("A") + 1)
    return max(1, n)


_A1_RE = re.compile(r"^\$?([A-Za-z]+)\$?(\d+)$")


def parse_a1_cell(cell: str) -> Tuple[str, int]:
    s = (_strip(cell) or "A1").strip()
    if ":" in s:
        s = s.split(":", 1)[0].strip()
    m = _A1_RE.match(s)
    if not m:
        return ("A", 1)
    return (m.group(1).upper(), max(1, int(m.group(2))))


def a1(col: str, row: int) -> str:
    return f"{(_strip(col) or 'A').upper()}{max(1, int(row))}"


def safe_sheet_name(name: str) -> str:
    n = (_strip(name) or "Sheet1").replace("'", "''")
    return f"'{n}'"


def default_start_cell_for_page(page: str) -> str:
    return "A1" if _strip(page) == "Data_Dictionary" else "A5"


# =============================================================================
# Retry / backoff
# =============================================================================

def _http_status_from_error(err: Exception) -> Optional[int]:
    try:
        if hasattr(err, "resp") and hasattr(err.resp, "status"):
            return int(err.resp.status)
    except Exception:
        pass
    try:
        if hasattr(err, "response") and hasattr(err.response, "status_code"):
            return int(err.response.status_code)
    except Exception:
        pass
    return None


class FullJitterBackoff:
    """
    Retry for Google Sheets API calls.
    Retries on 408 / 409 / 429 / 5xx and unknown transient errors.
    """

    def __init__(
        self, max_retries: int = 4, base_delay: float = 1.0, max_delay: float = 30.0
    ) -> None:
        self.max_retries = max(0, int(max_retries))
        self.base_delay = max(0.1, float(base_delay))
        self.max_delay = max(1.0, float(max_delay))

    def call(self, op_name: str, fn: Callable[[], Any]) -> Any:
        last_exc: Optional[Exception] = None

        for attempt in range(self.max_retries + 1):
            try:
                return fn()
            except Exception as exc:
                last_exc = exc
                status = _http_status_from_error(exc)

                retryable = (
                    status is None
                    or status in {408, 409, 429}
                    or (500 <= int(status) <= 599)
                )

                if attempt >= self.max_retries or not retryable:
                    raise

                delay_cap = min(self.max_delay, self.base_delay * (2 ** attempt))
                sleep_s = random.uniform(0.0, delay_cap)
                logger.warning(
                    "%s failed (status=%s). retry %s/%s in %.2fs: %s",
                    op_name,
                    status,
                    attempt + 1,
                    self.max_retries,
                    sleep_s,
                    repr(exc),
                )
                time.sleep(sleep_s)

        raise last_exc or RuntimeError(f"{op_name} failed after retries")


# =============================================================================
# Schema loading
# =============================================================================

def import_schema_module() -> Optional[Any]:
    paths = [
        "core.sheets.schema_registry",
        "core.schemas",
        "schemas",
    ]
    for path in paths:
        try:
            module = __import__(path, fromlist=["*"])
            logger.debug("Loaded schema module: %s", path)
            return module
        except Exception:
            continue
    return None


def import_sheets_service_module() -> Optional[Any]:
    paths = [
        "integrations.google_sheets_service",
        "core.integrations.google_sheets_service",
        "google_sheets_service",
    ]
    for path in paths:
        try:
            module = __import__(path, fromlist=["*"])
            if hasattr(module, "get_sheets_service"):
                logger.debug("Loaded sheets service module: %s", path)
                return module
        except Exception:
            continue
    return None


def get_supported_pages(schema_mod: Optional[Any]) -> List[str]:
    if schema_mod is not None:
        for name in ("get_supported_pages", "list_pages", "list_sheets"):
            fn = getattr(schema_mod, name, None)
            if callable(fn):
                try:
                    pages = list(fn())
                    if pages:
                        return pages
                except Exception:
                    pass
    return list(DEFAULT_CANONICAL_PAGES)


def get_forbidden_pages(sheets_mod: Optional[Any]) -> set:
    fn = getattr(sheets_mod, "core_forbidden_pages", None) if sheets_mod is not None else None
    if callable(fn):
        try:
            pages = set(fn() or set())
            if pages:
                return pages
        except Exception:
            pass
    return set(DEFAULT_FORBIDDEN_PAGES)


def resolve_page_name(
    page: str, schema_mod: Optional[Any], sheets_mod: Optional[Any]
) -> str:
    raw = _strip(page)
    if not raw:
        return ""

    for fn_name in ("normalize_page_name", "resolve_page_name", "resolve_sheet_key"):
        fn = getattr(schema_mod, fn_name, None) if schema_mod is not None else None
        if callable(fn):
            try:
                resolved = _strip(fn(raw))
                if resolved:
                    return resolved
            except Exception:
                pass

    fn = getattr(sheets_mod, "core_normalize_page", None) if sheets_mod is not None else None
    if callable(fn):
        try:
            resolved = _strip(fn(raw, allow_output_pages=True))
            if resolved:
                return resolved
        except Exception:
            pass

    return raw


# =============================================================================
# Column spec abstraction
# =============================================================================

@dataclass(frozen=True)
class ColumnSpec:
    key: str
    header: str
    data_type: str = "string"
    format_hint: str = ""
    group: str = ""
    required: bool = False
    width: int = 140
    align: str = "LEFT"


def _smart_width(key: str, header: str, group: str) -> int:
    hk = _normalize_header_key(header)
    kk = _normalize_header_key(key)
    gk = _normalize_page_token(group)

    if hk in {"symbol", "ticker"} or kk == "symbol":
        return 110
    if hk in {"name", "companyname"} or kk == "name":
        return 240
    if hk in {"reason", "selectionreason", "commentary", "notes", "criteriasnapshot"}:
        return 280
    if hk in {"provider", "providersecondary", "rowsource", "recommendation"}:
        return 170
    if hk in {"updatedatutc", "updatedatriyadh", "asofutc", "asofriyadh", "updatedat"}:
        return 170
    if "portfolio" in gk:
        return 130
    if "provenance" in gk:
        return 165
    if "insight" in gk:
        return 150
    if "dictionary" in gk:
        return 150
    if hk in {"error", "warning"}:
        return 260
    if "signal" in hk or "bucket" in hk or "rating" in hk:
        return 135
    if "price" in hk or "value" in hk or "score" in hk or "roi" in hk:
        return 125
    return 140


def _smart_align(data_type: str, key: str, header: str) -> str:
    dt = (_strip(data_type) or "string").lower()
    hk = _normalize_header_key(header)
    kk = _normalize_header_key(key)

    if hk in {"symbol", "name"} or kk in {"symbol", "name"}:
        return "LEFT"

    if dt in {"number", "percent", "integer", "int", "float"}:
        return "RIGHT"

    if dt in {"date", "datetime", "timestamp"}:
        return "CENTER"

    return "LEFT"


def _map_number_format(
    data_type: str, format_hint: str, percent_mode: str
) -> Optional[Dict[str, str]]:
    dt = (_strip(data_type) or "string").lower()
    hint = _strip(format_hint)

    if dt in {"boolean", "string"}:
        return None

    if dt in {"datetime", "timestamp"}:
        return {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}

    if dt == "date":
        return {"type": "DATE", "pattern": "yyyy-mm-dd"}

    if dt == "percent":
        if percent_mode == "fraction":
            return {"type": "PERCENT", "pattern": hint or "0.00%"}
        # backend currently emits percent points, so do NOT use Sheets percent
        return {"type": "NUMBER", "pattern": "0.00"}

    if dt in {"integer", "int"}:
        return {"type": "NUMBER", "pattern": "#,##0"}

    if dt in {"number", "float"}:
        if "%" in hint:
            if percent_mode == "fraction":
                return {"type": "PERCENT", "pattern": hint}
            return {"type": "NUMBER", "pattern": "0.00"}
        if hint:
            return {"type": "NUMBER", "pattern": hint}
        return {"type": "NUMBER", "pattern": "#,##0.00"}

    if hint:
        if "%" in hint:
            if percent_mode == "fraction":
                return {"type": "PERCENT", "pattern": hint}
            return {"type": "NUMBER", "pattern": "0.00"}
        return {"type": "NUMBER", "pattern": hint}

    return None


# Sentinel returned by the 5-header fallback so callers can detect it and
# apply the min-header-count guard at a page-aware level.
_FALLBACK_HEADERS = ["Symbol", "Name", "Current Price", "Recommendation", "Error"]


def get_column_specs_for_page(
    page: str,
    schema_mod: Optional[Any],
    schema_version: str,
) -> Tuple[List[ColumnSpec], bool]:
    """
    Returns (specs, used_fallback).

    v7.1.0: the `used_fallback` bool lets the caller decide whether to
    accept the 5-header emergency fallback or skip the tab. v7.0.0 silently
    returned the fallback with no signal, causing silent sheet corruption
    for wide pages.
    """
    page = _strip(page)

    # Preferred: core.sheets.schema_registry / core.schemas full schema object
    for fn_name in ("get_schema", "get_page_schema", "get_sheet_schema"):
        fn = getattr(schema_mod, fn_name, None) if schema_mod is not None else None
        if callable(fn):
            try:
                schema = fn(page)
                fields = getattr(schema, "fields", None) or getattr(
                    schema, "columns", None
                )
                if fields:
                    specs: List[ColumnSpec] = []
                    for f_ in fields:
                        key = _strip(getattr(f_, "key", "")) or _normalize_header_key(
                            getattr(f_, "header", "")
                        )
                        header = _strip(getattr(f_, "header", "")) or key
                        data_type = (
                            _strip(getattr(f_, "data_type", ""))
                            or _strip(getattr(f_, "dtype", ""))
                            or "string"
                        )
                        format_hint = (
                            _strip(getattr(f_, "format_hint", ""))
                            or _strip(getattr(f_, "fmt", ""))
                        )
                        group = _strip(getattr(f_, "group", ""))
                        required = bool(getattr(f_, "required", False))
                        specs.append(
                            ColumnSpec(
                                key=key,
                                header=header,
                                data_type=data_type,
                                format_hint=format_hint,
                                group=group,
                                required=required,
                                width=_smart_width(key, header, group),
                                align=_smart_align(data_type, key, header),
                            )
                        )
                    if specs:
                        return specs, False
            except Exception:
                pass

    # Fallback: explicit field-spec dictionaries if exposed
    fn = getattr(schema_mod, "get_field_specs", None) if schema_mod is not None else None
    if callable(fn):
        try:
            raw_specs = fn(page)
            specs2: List[ColumnSpec] = []
            for f_ in raw_specs or []:
                if not isinstance(f_, dict):
                    continue
                key = _strip(f_.get("key")) or _normalize_header_key(f_.get("header"))
                header = _strip(f_.get("header")) or key
                data_type = _strip(f_.get("data_type") or f_.get("dtype") or "string")
                format_hint = _strip(f_.get("format_hint") or f_.get("fmt") or "")
                group = _strip(f_.get("group") or "")
                required = bool(f_.get("required", False))
                specs2.append(
                    ColumnSpec(
                        key=key,
                        header=header,
                        data_type=data_type,
                        format_hint=format_hint,
                        group=group,
                        required=required,
                        width=_smart_width(key, header, group),
                        align=_smart_align(data_type, key, header),
                    )
                )
            if specs2:
                return specs2, False
        except Exception:
            pass

    # Fallback: headers only
    headers: List[str] = []
    if schema_mod is not None:
        for fn_name in ("get_headers_for_sheet", "get_headers", "get_sheet_headers"):
            fn = getattr(schema_mod, fn_name, None)
            if callable(fn):
                try:
                    if fn_name == "get_headers_for_sheet":
                        headers = list(fn(page, version=schema_version))
                    else:
                        headers = list(fn(page))
                    if headers:
                        break
                except Exception:
                    continue

    used_fallback = False
    if not headers:
        headers = list(_FALLBACK_HEADERS)
        used_fallback = True

    specs3 = [
        ColumnSpec(
            key=_normalize_header_key(h) or f"col_{i+1}",
            header=_strip(h),
            data_type="string",
            format_hint="",
            group="",
            required=(i == 0),
            width=_smart_width(_normalize_header_key(h), _strip(h), ""),
            align="LEFT",
        )
        for i, h in enumerate(headers)
        if _strip(h)
    ]
    return specs3, used_fallback


# =============================================================================
# Header comparisons
# =============================================================================

def _headers_non_empty(headers: Sequence[Any], min_filled: int = 3) -> bool:
    if not headers:
        return False
    filled = 0
    for value in headers:
        if _strip(value):
            filled += 1
            if filled >= min_filled:
                return True
    return False


def _headers_match(existing: Sequence[Any], expected: Sequence[str]) -> bool:
    ex = [_strip(x) for x in existing if _strip(x)]
    nw = [_strip(x) for x in expected if _strip(x)]
    if not ex or not nw:
        return False
    return ex == nw


# =============================================================================
# Sheet manager
# =============================================================================

class SheetManager:
    def __init__(
        self, spreadsheet_id: str, service: Any, backoff: FullJitterBackoff
    ) -> None:
        self.spreadsheet_id = spreadsheet_id
        self.service = service
        self.backoff = backoff
        self._lock = threading.RLock()
        self.sheet_meta: Dict[str, Dict[str, Any]] = {}

    def refresh_meta(self) -> None:
        fields = (
            "sheets("
            "properties(sheetId,title,gridProperties(rowCount,columnCount,frozenRowCount,frozenColumnCount)),"
            "bandedRanges(bandedRangeId)"
            ")"
        )

        def _get() -> Dict[str, Any]:
            return self.service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id,
                fields=fields,
            ).execute()

        meta = self.backoff.call("spreadsheets.get.meta", _get)
        out: Dict[str, Dict[str, Any]] = {}
        for sheet in meta.get("sheets", []) or []:
            props = (sheet or {}).get("properties") or {}
            title = _strip(props.get("title"))
            if not title:
                continue
            out[title] = {
                "sheetId": props.get("sheetId"),
                "rowCount": ((props.get("gridProperties") or {}).get("rowCount") or 1000),
                "columnCount": ((props.get("gridProperties") or {}).get("columnCount") or 26),
                "bandedRangeIds": [
                    int(br.get("bandedRangeId"))
                    for br in (sheet.get("bandedRanges") or [])
                    if isinstance(br, dict) and br.get("bandedRangeId") is not None
                ],
            }
        with self._lock:
            self.sheet_meta = out

    def tab_exists(self, title: str) -> bool:
        with self._lock:
            return _strip(title) in self.sheet_meta

    def get_sheet_id(self, title: str) -> Optional[int]:
        with self._lock:
            return self.sheet_meta.get(_strip(title), {}).get("sheetId")

    def get_grid_size(self, title: str) -> Tuple[int, int]:
        with self._lock:
            meta = self.sheet_meta.get(_strip(title), {})
            return int(meta.get("rowCount") or 1000), int(meta.get("columnCount") or 26)

    def get_banded_range_ids(self, title: str) -> List[int]:
        with self._lock:
            return list(self.sheet_meta.get(_strip(title), {}).get("bandedRangeIds") or [])

    def create_tab(
        self, title: str, row_count: int = 2000, column_count: int = 120
    ) -> int:
        title = _strip(title)
        if not title:
            raise ValueError("Tab title is empty")

        def _create() -> Dict[str, Any]:
            return self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={
                    "requests": [
                        {
                            "addSheet": {
                                "properties": {
                                    "title": title,
                                    "gridProperties": {
                                        "rowCount": int(row_count),
                                        "columnCount": int(column_count),
                                    },
                                }
                            }
                        }
                    ]
                },
            ).execute()

        resp = self.backoff.call("addSheet", _create)
        sheet_id = int(resp["replies"][0]["addSheet"]["properties"]["sheetId"])
        self.refresh_meta()
        return sheet_id

    def read_row(
        self, sheet_name: str, row_number: int, start_col: str = "A", end_col: str = "ZZ"
    ) -> List[str]:
        range_name = f"{safe_sheet_name(sheet_name)}!{a1(start_col, row_number)}:{end_col}{row_number}"

        def _read() -> Dict[str, Any]:
            return self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
            ).execute()

        res = self.backoff.call("values.get.row", _read)
        values = res.get("values", []) or []
        if not values:
            return []
        return [str(x).strip() for x in values[0] if str(x).strip()]

    def write_row(self, sheet_name: str, start_cell: str, values: List[Any]) -> None:
        range_name = f"{safe_sheet_name(sheet_name)}!{_strip(start_cell)}"

        def _write() -> Dict[str, Any]:
            return self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption="RAW",
                body={"values": [values]},
            ).execute()

        self.backoff.call("values.update.row", _write)

    def clear_range(self, range_name: str) -> None:
        def _clear() -> Dict[str, Any]:
            return self.service.spreadsheets().values().clear(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                body={},
            ).execute()

        self.backoff.call("values.clear", _clear)

    def batch_update(self, requests: List[Dict[str, Any]]) -> None:
        if not requests:
            return

        chunk_size = 250
        for i in range(0, len(requests), chunk_size):
            part = requests[i:i + chunk_size]

            def _update() -> Dict[str, Any]:
                return self.service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body={"requests": part},
                ).execute()

            self.backoff.call("batchUpdate", _update)

    def ensure_insights_criteria_if_empty(self, sheet_name: str) -> None:
        if _strip(sheet_name) != "Insights_Analysis":
            return

        read_range = f"{safe_sheet_name(sheet_name)}!A1:D4"

        def _read() -> Dict[str, Any]:
            return self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=read_range,
            ).execute()

        res = self.backoff.call("values.get.insights.criteria", _read)
        values = res.get("values", []) or []

        has_any = False
        for row in values:
            for cell in row or []:
                if _strip(cell):
                    has_any = True
                    break
            if has_any:
                break

        if has_any:
            return

        def _write() -> Dict[str, Any]:
            return self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{safe_sheet_name(sheet_name)}!A1",
                valueInputOption="RAW",
                body={"values": INSIGHTS_CRITERIA_BLOCK},
            ).execute()

        self.backoff.call("values.update.insights.criteria", _write)


# =============================================================================
# Styling
# =============================================================================

def apply_sheet_style(
    manager: SheetManager,
    page: str,
    start_cell: str,
    specs: List[ColumnSpec],
    percent_mode: str,
    resize_grid: bool,
    grid_rows: int,
    grid_cols: int,
) -> None:
    page = _strip(page)
    sheet_id = manager.get_sheet_id(page)
    if sheet_id is None:
        manager.refresh_meta()
        sheet_id = manager.get_sheet_id(page)
    if sheet_id is None:
        raise RuntimeError(f"Could not resolve sheet id for '{page}'")

    start_col, header_row = parse_a1_cell(start_cell)
    start_col_idx = col_to_index(start_col) - 1
    end_col_idx = start_col_idx + len(specs)

    current_rows, current_cols = manager.get_grid_size(page)
    target_rows = max(int(grid_rows), int(current_rows), header_row + 500)
    target_cols = max(int(grid_cols), int(current_cols), end_col_idx + 5)

    requests: List[Dict[str, Any]] = []

    if resize_grid:
        requests.append(
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {
                            "rowCount": target_rows,
                            "columnCount": target_cols,
                        },
                    },
                    "fields": "gridProperties.rowCount,gridProperties.columnCount",
                }
            }
        )

    # Freeze all rows above and including the header row.
    requests.append(
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "frozenRowCount": header_row,
                        "frozenColumnCount": 1,
                    },
                },
                "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
            }
        }
    )

    # Clear then set filter.
    requests.append({"clearBasicFilter": {"sheetId": sheet_id}})
    requests.append(
        {
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": header_row - 1,
                        "endRowIndex": target_rows,
                        "startColumnIndex": start_col_idx,
                        "endColumnIndex": end_col_idx,
                    }
                }
            }
        }
    )

    # Remove existing banding so the script is idempotent.
    for banding_id in manager.get_banded_range_ids(page):
        requests.append({"deleteBanding": {"bandedRangeId": int(banding_id)}})

    # Header style.
    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": header_row - 1,
                    "endRowIndex": header_row,
                    "startColumnIndex": start_col_idx,
                    "endColumnIndex": end_col_idx,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.11, "green": 0.23, "blue": 0.38},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "wrapStrategy": "WRAP",
                        "textFormat": {
                            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                            "fontFamily": "Nunito",
                            "fontSize": 10,
                            "bold": True,
                        },
                    }
                },
                "fields": (
                    "userEnteredFormat("
                    "backgroundColor,horizontalAlignment,verticalAlignment,"
                    "wrapStrategy,textFormat)"
                ),
            }
        }
    )

    requests.append(
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": header_row - 1,
                    "endIndex": header_row,
                },
                "properties": {"pixelSize": 32},
                "fields": "pixelSize",
            }
        }
    )

    # Data row font / alignment / number formats.
    for i, spec in enumerate(specs):
        col_index = start_col_idx + i
        fmt: Dict[str, Any] = {
            "textFormat": {"fontFamily": "Nunito", "fontSize": 10},
            "horizontalAlignment": spec.align,
        }

        number_format = _map_number_format(spec.data_type, spec.format_hint, percent_mode)
        if number_format:
            fmt["numberFormat"] = number_format

        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": header_row,
                        "endRowIndex": target_rows,
                        "startColumnIndex": col_index,
                        "endColumnIndex": col_index + 1,
                    },
                    "cell": {"userEnteredFormat": fmt},
                    "fields": "userEnteredFormat(textFormat,horizontalAlignment,numberFormat)",
                }
            }
        )

        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": col_index,
                        "endIndex": col_index + 1,
                    },
                    "properties": {"pixelSize": int(spec.width)},
                    "fields": "pixelSize",
                }
            }
        )

    # Row banding for data area.
    requests.append(
        {
            "addBanding": {
                "bandedRange": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": header_row,
                        "endRowIndex": target_rows,
                        "startColumnIndex": start_col_idx,
                        "endColumnIndex": end_col_idx,
                    },
                    "rowProperties": {
                        "firstBandColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                        "secondBandColor": {"red": 0.97, "green": 0.97, "blue": 0.97},
                    },
                }
            }
        }
    )

    manager.batch_update(requests)
    manager.refresh_meta()


# =============================================================================
# Per-tab processing
# =============================================================================

@dataclass(frozen=True)
class RunOptions:
    spreadsheet_id: str
    schema_version: str
    start_cell: Optional[str]
    row: Optional[int]
    create_missing: bool
    force: bool
    clear: bool
    no_style: bool
    dry_run: bool
    percent_mode: str
    resize_grid: bool
    grid_rows: int
    grid_cols: int
    require_schema: bool = False           # v7.1.0
    min_header_count: int = DEFAULT_MIN_HEADER_COUNT  # v7.1.0


def resolve_start_cell(
    page: str, cli_start_cell: Optional[str], cli_row: Optional[int]
) -> str:
    if _strip(cli_start_cell):
        return _strip(cli_start_cell)
    if cli_row is not None and int(cli_row) > 0:
        return f"A{int(cli_row)}"
    return default_start_cell_for_page(page)


def process_single_tab(
    page_input: str,
    options: RunOptions,
    schema_mod: Optional[Any],
    sheets_mod: Optional[Any],
) -> Tuple[str, str, str]:
    """
    Returns: (page, status, detail)
    status in: success / skipped / failed
    """
    page = resolve_page_name(page_input, schema_mod, sheets_mod)
    forbidden_pages = get_forbidden_pages(sheets_mod)

    if not page:
        return page_input, "failed", "Empty or unresolved page name"

    if page in forbidden_pages:
        return page, "failed", f"Forbidden/removed page: {page}"

    specs, used_fallback = get_column_specs_for_page(
        page, schema_mod, options.schema_version
    )
    headers = [spec.header for spec in specs]

    # v7.1.0: hard-fail on fallback if required
    if used_fallback:
        if options.require_schema:
            return page, "failed", (
                "Schema resolution fell back to emergency 5-header set; "
                "--require-schema prevents silent narrow-header writes."
            )
        # v7.1.0: elevate warning — a 5-header write on an 80-col page
        # silently corrupts column alignment for every downstream write.
        logger.error(
            "Page '%s' fell back to emergency 5-header set. If this page is "
            "supposed to be wide (canonical 80+ cols), downstream writes "
            "will misalign. Rerun with --require-schema to fail fast, or "
            "ensure core.sheets.schema_registry is importable.",
            page,
        )

    # v7.1.0: min-header-count guard for non-narrow pages
    if page not in DEFAULT_NARROW_PAGES and len(headers) < options.min_header_count:
        return page, "failed", (
            f"Resolved only {len(headers)} headers (< min_header_count="
            f"{options.min_header_count}) for wide page '{page}'. "
            f"Refusing to write a narrow-header block to a wide page. "
            f"Check schema module or override with --min-header-count."
        )

    if not headers:
        return page, "failed", "No headers resolved from schema"

    start_cell = resolve_start_cell(page, options.start_cell, options.row)
    start_col, header_row = parse_a1_cell(start_cell)

    if options.dry_run:
        fallback_note = " [FALLBACK 5-HEADERS]" if used_fallback else ""
        return page, "success", (
            f"[DRY RUN] start_cell={start_cell}, headers={len(headers)}{fallback_note}, "
            f"create_missing={options.create_missing}, style={not options.no_style}"
        )

    if sheets_mod is None:
        return page, "failed", "Could not import integrations.google_sheets_service"

    try:
        service = sheets_mod.get_sheets_service()
    except Exception as exc:
        return page, "failed", f"Failed to build Sheets service: {exc}"

    manager = SheetManager(
        spreadsheet_id=options.spreadsheet_id,
        service=service,
        backoff=FullJitterBackoff(max_retries=4, base_delay=1.0, max_delay=30.0),
    )

    try:
        manager.refresh_meta()
    except Exception as exc:
        return page, "failed", f"Failed reading spreadsheet metadata: {exc}"

    # Create missing tab if requested.
    if not manager.tab_exists(page):
        if not options.create_missing:
            return page, "skipped", "Tab missing (use --create-missing)"
        try:
            manager.create_tab(
                page,
                row_count=max(options.grid_rows, header_row + 500),
                column_count=max(options.grid_cols, len(headers) + 10),
            )
        except Exception as exc:
            return page, "failed", f"Failed creating tab: {exc}"

    # Read current header row at the effective start cell.
    try:
        current_headers = manager.read_row(page, header_row, start_col=start_col, end_col="ZZ")
    except Exception:
        current_headers = []

    # Decide whether to write.
    needs_write = options.force or not _headers_match(current_headers, headers)

    try:
        if needs_write:
            end_col = index_to_col(col_to_index(start_col) + len(headers) - 1)
            if options.clear:
                manager.clear_range(
                    f"{safe_sheet_name(page)}!{a1(start_col, header_row)}:"
                    f"{end_col}{header_row}"
                )
            manager.write_row(page, start_cell, headers)
    except Exception as exc:
        return page, "failed", f"Failed writing headers: {exc}"

    # Align with project service-level rules first.
    if hasattr(sheets_mod, "ensure_headers_and_formatting"):
        try:
            sheets_mod.ensure_headers_and_formatting(
                options.spreadsheet_id, page, start_cell
            )
        except Exception as exc:
            logger.warning("Service-level ensure failed for %s: %s", page, exc)

    # Seed Insights criteria block if needed.
    try:
        manager.ensure_insights_criteria_if_empty(page)
    except Exception as exc:
        logger.warning("Insights criteria seed skipped for %s: %s", page, exc)

    # Apply richer formatting.
    if not options.no_style:
        try:
            apply_sheet_style(
                manager=manager,
                page=page,
                start_cell=start_cell,
                specs=specs,
                percent_mode=options.percent_mode,
                resize_grid=options.resize_grid,
                grid_rows=options.grid_rows,
                grid_cols=options.grid_cols,
            )
        except Exception as exc:
            return page, "failed", f"Headers written but styling failed: {exc}"

    if needs_write:
        return page, "success", f"Headers set to exact canonical schema at {start_cell}"
    return page, "success", f"Already aligned; formatting refreshed at {start_cell}"


# =============================================================================
# Main
# =============================================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            f"Set up exact canonical Google Sheets headers for Tadawul Fast "
            f"Bridge (v{SCRIPT_VERSION})."
        )
    )
    p.add_argument(
        "--sheet-id",
        default=os.getenv("SETUP_SHEET_ID") or None,
        help="Spreadsheet ID (also SETUP_SHEET_ID / DEFAULT_SPREADSHEET_ID env).",
    )
    p.add_argument(
        "--tabs",
        nargs="*",
        default=_env_csv("SETUP_TABS"),
        help="Process selected tabs only (also SETUP_TABS env, comma-separated).",
    )
    p.add_argument(
        "--all-existing",
        action="store_true",
        default=_env_bool("SETUP_ALL_EXISTING", False),
        help="Process all existing tabs in the spreadsheet (also SETUP_ALL_EXISTING env).",
    )
    p.add_argument(
        "--create-missing",
        action="store_true",
        default=_env_bool("SETUP_CREATE_MISSING", False),
        help="Create missing tabs (also SETUP_CREATE_MISSING env).",
    )
    p.add_argument(
        "--schema-version",
        default=os.getenv("SETUP_SCHEMA_VERSION", "vNext"),
        help="vNext | legacy (kept for compatibility; also SETUP_SCHEMA_VERSION env).",
    )
    p.add_argument(
        "--start-cell",
        default=os.getenv("SETUP_START_CELL") or None,
        help="Override header start cell (also SETUP_START_CELL env; default: smart per page).",
    )
    p.add_argument(
        "--row",
        type=int,
        default=(_env_int("SETUP_ROW", 0) or None),
        help="Override header row as A{row} (also SETUP_ROW env).",
    )
    p.add_argument(
        "--force",
        action="store_true",
        default=_env_bool("SETUP_FORCE", False),
        help="Force header rewrite even if already aligned (also SETUP_FORCE env).",
    )
    p.add_argument(
        "--clear",
        action="store_true",
        default=_env_bool("SETUP_CLEAR", False),
        help="Clear the target header row range before writing (also SETUP_CLEAR env).",
    )
    p.add_argument(
        "--no-style",
        action="store_true",
        default=_env_bool("SETUP_NO_STYLE", False),
        help="Write headers only; skip formatting (also SETUP_NO_STYLE env).",
    )
    p.add_argument(
        "--percent-mode",
        default=os.getenv("SETUP_PERCENT_MODE") or None,
        help="points | fraction (also SETUP_PERCENT_MODE / TFB_PERCENT_MODE env; "
             "default 'points').",
    )
    p.add_argument(
        "--parallel",
        action="store_true",
        default=_env_bool("SETUP_PARALLEL", False),
        help="Process tabs in parallel with separate clients (also SETUP_PARALLEL env).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=_env_bool("SETUP_DRY_RUN", False),
        help="Preview actions without writing (also SETUP_DRY_RUN env).",
    )
    p.add_argument(
        "--resize-grid",
        action="store_true",
        default=_env_bool("SETUP_RESIZE_GRID", False),
        help="Resize grid rows/cols to fit schema width (grow-only; also SETUP_RESIZE_GRID env).",
    )
    p.add_argument(
        "--grid-rows",
        type=int,
        default=_env_int("SETUP_GRID_ROWS", 3000, lo=100),
        help="Target grid rows when creating/resizing (also SETUP_GRID_ROWS env).",
    )
    p.add_argument(
        "--grid-cols",
        type=int,
        default=_env_int("SETUP_GRID_COLS", 140, lo=10),
        help="Target grid cols when creating/resizing (also SETUP_GRID_COLS env).",
    )

    # v7.1.0 NEW: schema-safety flags
    p.add_argument(
        "--require-schema",
        action="store_true",
        default=_env_bool("SETUP_REQUIRE_SCHEMA", False),
        help="Hard-fail if schema module unavailable or resolution falls back "
             "to emergency 5-header set (also SETUP_REQUIRE_SCHEMA env).",
    )
    p.add_argument(
        "--min-header-count",
        type=int,
        default=_env_int("SETUP_MIN_HEADER_COUNT", DEFAULT_MIN_HEADER_COUNT, lo=1),
        help=(
            f"Minimum accepted header count for wide pages (pages not in "
            f"{sorted(DEFAULT_NARROW_PAGES)}). Tabs resolving fewer headers "
            f"are failed to prevent misaligned writes. "
            f"Default {DEFAULT_MIN_HEADER_COUNT}. Also SETUP_MIN_HEADER_COUNT env."
        ),
    )

    p.add_argument(
        "--json",
        action="store_true",
        default=_env_bool("SETUP_JSON", False),
        help="Print JSON-structured result to stdout at end (also SETUP_JSON env).",
    )
    p.add_argument(
        "--verbose", "-v",
        action="store_true",
        default=_env_bool("SETUP_VERBOSE", False),
        help="Verbose logging (also SETUP_VERBOSE env).",
    )
    return p


def resolve_percent_mode(cli_value: Optional[str]) -> str:
    raw = (
        _strip(cli_value)
        or _strip(os.getenv("SETUP_PERCENT_MODE", ""))
        or _strip(os.getenv("TFB_PERCENT_MODE", ""))
        or "points"
    ).lower()
    return "fraction" if raw in {"fraction", "decimal", "dec"} else "points"


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    spreadsheet_id = _strip(
        args.sheet_id
        or os.getenv("SETUP_SHEET_ID", "")
        or os.getenv("DEFAULT_SPREADSHEET_ID", "")
    )
    if not spreadsheet_id:
        logger.error(
            "No spreadsheet id. Use --sheet-id or set SETUP_SHEET_ID / "
            "DEFAULT_SPREADSHEET_ID."
        )
        return 1

    schema_mod = import_schema_module()
    sheets_mod = import_sheets_service_module()

    if schema_mod is None:
        if args.require_schema:
            logger.error(
                "Schema module not found (tried core.sheets.schema_registry, "
                "core.schemas, schemas) and --require-schema is set. Refusing to "
                "run with emergency fallback headers."
            )
            return 1
        logger.warning(
            "Schema module not found. Falling back to minimal header behavior. "
            "Use --require-schema to fail fast instead."
        )
    if sheets_mod is None:
        logger.error("Could not import Google Sheets service module.")
        return 1

    percent_mode = resolve_percent_mode(args.percent_mode)
    forbidden_pages = get_forbidden_pages(sheets_mod)

    # Determine target pages.
    if args.all_existing:
        try:
            service = sheets_mod.get_sheets_service()
            manager = SheetManager(
                spreadsheet_id=spreadsheet_id,
                service=service,
                backoff=FullJitterBackoff(max_retries=4, base_delay=1.0, max_delay=30.0),
            )
            manager.refresh_meta()
            target_pages = [
                p for p in manager.sheet_meta.keys() if p not in forbidden_pages
            ]
        except Exception as exc:
            logger.error("Could not read existing tabs: %s", exc)
            return 1
    elif args.tabs:
        target_pages = [resolve_page_name(p, schema_mod, sheets_mod) for p in args.tabs]
        target_pages = [p for p in target_pages if p]
    else:
        target_pages = [
            p for p in get_supported_pages(schema_mod) if p not in forbidden_pages
        ]

    if not target_pages:
        logger.error("No target tabs to process.")
        return 1

    options = RunOptions(
        spreadsheet_id=spreadsheet_id,
        schema_version=(_strip(args.schema_version) or "vNext"),
        start_cell=args.start_cell,
        row=args.row,
        create_missing=bool(args.create_missing),
        force=bool(args.force),
        clear=bool(args.clear),
        no_style=bool(args.no_style),
        dry_run=bool(args.dry_run),
        percent_mode=percent_mode,
        resize_grid=bool(args.resize_grid),
        grid_rows=int(args.grid_rows),
        grid_cols=int(args.grid_cols),
        require_schema=bool(args.require_schema),
        min_header_count=int(args.min_header_count),
    )

    logger.info("=" * 76)
    logger.info("TFB Sheet Header Setup v%s", SCRIPT_VERSION)
    logger.info("Spreadsheet: %s", spreadsheet_id)
    logger.info("Percent mode: %s", percent_mode)
    logger.info("Require schema: %s", options.require_schema)
    logger.info("Min header count (wide pages): %s", options.min_header_count)
    logger.info("Targets: %s", ", ".join(target_pages))
    logger.info("=" * 76)

    results: List[Tuple[str, str, str]] = []

    if args.parallel and len(target_pages) > 1:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(6, len(target_pages)),
            thread_name_prefix="TFB-SetupHeaders",
        ) as executor:
            futures = [
                executor.submit(process_single_tab, page, options, schema_mod, sheets_mod)
                for page in target_pages
            ]
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())
    else:
        for page in target_pages:
            results.append(process_single_tab(page, options, schema_mod, sheets_mod))

    success = 0
    skipped = 0
    failed = 0

    for page, status, detail in results:
        if status == "success":
            success += 1
            logger.info("SUCCESS | %s | %s", page, detail)
        elif status == "skipped":
            skipped += 1
            logger.warning("SKIPPED | %s | %s", page, detail)
        else:
            failed += 1
            logger.error("FAILED  | %s | %s", page, detail)

    logger.info("=" * 76)
    logger.info("SUMMARY")
    logger.info("Success: %d", success)
    logger.info("Skipped: %d", skipped)
    logger.info("Failed : %d", failed)
    logger.info("=" * 76)

    # v7.1.0: JSON stdout output for CI/pipelines
    if args.json:
        report = {
            "version": SCRIPT_VERSION,
            "spreadsheet_id": spreadsheet_id,
            "percent_mode": percent_mode,
            "require_schema": options.require_schema,
            "min_header_count": options.min_header_count,
            "summary": {
                "success": success,
                "skipped": skipped,
                "failed": failed,
                "total": len(results),
            },
            "results": [
                {"page": p, "status": s, "detail": d} for (p, s, d) in results
            ],
        }
        sys.stdout.write(
            _json.dumps(report, indent=2, ensure_ascii=False, default=str) + "\n"
        )

    return 0 if failed == 0 else 2


__all__ = [
    "SCRIPT_VERSION",
    "SERVICE_VERSION",
    "DEFAULT_CANONICAL_PAGES",
    "DEFAULT_FORBIDDEN_PAGES",
    "DEFAULT_NARROW_PAGES",
    "DEFAULT_MIN_HEADER_COUNT",
    "ColumnSpec",
    "RunOptions",
    "SheetManager",
    "FullJitterBackoff",
    "get_column_specs_for_page",
    "process_single_tab",
    "build_parser",
    "main",
]


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        logger.warning("Interrupted.")
        sys.exit(130)
