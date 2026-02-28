#!/usr/bin/env python3
# scripts/run_sheet_init.py
"""
================================================================================
TADAWUL FAST BRIDGE — SHEET INITIALIZER — v6.1.0 (STABLE / ALIGNED / FAST)
================================================================================

Primary goals
- ✅ Create/initialize tabs with the *correct headers* (aligned with core.schemas + router output)
- ✅ Apply consistent formatting (Nunito, frozen panes, filter, banding, widths, number formats)
- ✅ Safe retries with Full-Jitter backoff for Google API throttling/conflicts
- ✅ Parallel tab initialization (optional) without breaking thread-safety
- ✅ No hard dependency on openpyxl / gspread (uses Google Sheets API service when available)

Major fixes vs your pasted script
- ✅ Removed duplicated FullJitterBackoff (it was defined twice)
- ✅ Fixed TraceContext usage (proper start_as_current_span + safe fallbacks)
- ✅ Fixed invalid header read range patterns and safer "headers already exist" detection
- ✅ Fixed percent formatting pitfall:
     Your backend often returns percent *points* (e.g., 1.23 = 1.23%),
     so applying Google PERCENT format would show 123%.
     We use NUMBER format "0.00" for percent columns by default.
- ✅ Removed openpyxl range parsing; replaced with lightweight A1 helpers
- ✅ BatchUpdate request chunking (avoids hitting request limits)
- ✅ “Core schemas first”:
     Pulls headers from core.schemas.get_headers_for_sheet(version=legacy|vNext)
     and falls back to ENRICHED_HEADERS_61 if core.schemas is unavailable.

Usage examples
- Initialize standard tabs (create missing):
    python scripts/run_sheet_init.py --create-missing --parallel

- Force overwrite headers + restyle:
    python scripts/run_sheet_init.py --force --clear --parallel

- Use legacy (router/enriched) headers:
    python scripts/run_sheet_init.py --schema-version legacy --force

Env
- DEFAULT_SPREADSHEET_ID (recommended)
- CORE_TRACING_ENABLED (optional)
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import logging
import os
import random
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union


# =============================================================================
# Logging
# =============================================================================

SCRIPT_VERSION = "6.1.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("TFB.SheetInit")


# =============================================================================
# Optional deps (safe)
# =============================================================================

try:
    from googleapiclient.errors import HttpError  # type: ignore
except Exception:  # pragma: no cover
    HttpError = Exception  # type: ignore

try:
    from prometheus_client import Counter, Histogram  # type: ignore
    _PROM_AVAILABLE = True
except Exception:
    _PROM_AVAILABLE = False

try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore
    _OTEL_AVAILABLE = True
    _TRACER = trace.get_tracer(__name__)
except Exception:
    _OTEL_AVAILABLE = False
    _TRACER = None
    Status = None  # type: ignore
    StatusCode = None  # type: ignore


# =============================================================================
# Metrics (safe)
# =============================================================================

class _DummyMetric:
    def labels(self, *args, **kwargs):  # noqa
        return self
    def inc(self, *args, **kwargs):  # noqa
        return None
    def observe(self, *args, **kwargs):  # noqa
        return None

if _PROM_AVAILABLE:
    sheet_ops_total = Counter("tfb_sheet_ops_total", "Sheet operations", ["op", "status"])
    sheet_ops_seconds = Histogram("tfb_sheet_ops_seconds", "Sheet operation duration", ["op"])
else:
    sheet_ops_total = _DummyMetric()
    sheet_ops_seconds = _DummyMetric()


# =============================================================================
# Time helpers
# =============================================================================

UTC = timezone.utc
RIYADH = timezone(timedelta(hours=3))

def utc_now() -> datetime:
    return datetime.now(UTC)

def riyadh_now() -> datetime:
    return datetime.now(RIYADH)

def utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or utc_now()
    if d.tzinfo is None:
        d = d.replace(tzinfo=UTC)
    return d.astimezone(UTC).isoformat()


# =============================================================================
# Tracing (safe)
# =============================================================================

_TRACING_ENABLED = (os.getenv("CORE_TRACING_ENABLED", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}

class TraceContext:
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._span_cm = None
        self._span = None

    def __enter__(self):
        if _OTEL_AVAILABLE and _TRACING_ENABLED and _TRACER is not None:
            try:
                self._span_cm = _TRACER.start_as_current_span(self.name)
                self._span = self._span_cm.__enter__()
                try:
                    for k, v in (self.attributes or {}).items():
                        self._span.set_attribute(str(k), v)
                except Exception:
                    pass
            except Exception:
                self._span_cm = None
                self._span = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self._span is not None and exc_val is not None and Status is not None and StatusCode is not None:
                try:
                    self._span.record_exception(exc_val)
                except Exception:
                    pass
                try:
                    self._span.set_status(Status(StatusCode.ERROR, str(exc_val)))
                except Exception:
                    pass
        finally:
            if self._span_cm is not None:
                try:
                    return self._span_cm.__exit__(exc_type, exc_val, exc_tb)
                except Exception:
                    return False
        return False


# =============================================================================
# Full Jitter Backoff (sync)
# =============================================================================

def _http_status_from_error(err: Exception) -> Optional[int]:
    # googleapiclient.errors.HttpError usually has .resp.status
    try:
        if hasattr(err, "resp") and hasattr(err.resp, "status"):
            return int(err.resp.status)
    except Exception:
        pass

    # gspread APIError sometimes contains response status
    try:
        if hasattr(err, "response") and hasattr(err.response, "status_code"):
            return int(err.response.status_code)
    except Exception:
        pass

    return None


class FullJitterBackoff:
    """
    AWS-style Full Jitter backoff for Google API calls.
    """

    def __init__(self, max_retries: int = 5, base_delay: float = 1.0, max_delay: float = 60.0):
        self.max_retries = max(0, int(max_retries))
        self.base_delay = max(0.1, float(base_delay))
        self.max_delay = max(1.0, float(max_delay))

    def call(self, op_name: str, fn: Callable[[], Any]) -> Any:
        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                t0 = time.time()
                out = fn()
                sheet_ops_total.labels(op=op_name, status="ok").inc()
                sheet_ops_seconds.labels(op=op_name).observe(time.time() - t0)
                return out
            except Exception as e:
                last_err = e
                status = _http_status_from_error(e)

                retryable = False
                if status is None:
                    # unknown error: allow limited retries
                    retryable = True
                else:
                    # common retryable statuses
                    retryable = status in (429, 409, 408) or (500 <= status <= 599)

                if attempt >= self.max_retries or not retryable:
                    sheet_ops_total.labels(op=op_name, status="fail").inc()
                    raise

                temp = min(self.max_delay, self.base_delay * (2 ** attempt))
                sleep_s = random.uniform(0, temp)
                logger.warning(
                    "%s failed (status=%s). retry %s/%s in %.2fs: %s",
                    op_name, status, attempt + 1, self.max_retries, sleep_s, repr(e),
                )
                time.sleep(sleep_s)

        # should not reach
        raise last_err or RuntimeError("Backoff exhausted")


# =============================================================================
# A1 helpers (no openpyxl)
# =============================================================================

_A1_COL_RE = re.compile(r"^[A-Z]+$", re.IGNORECASE)
_A1_CELL_RE = re.compile(r"^([A-Z]+)(\d+)$", re.IGNORECASE)

def col_to_index(col: str) -> int:
    c = (col or "").strip().upper()
    if not c or not _A1_COL_RE.match(c):
        raise ValueError(f"Invalid column: {col!r}")
    n = 0
    for ch in c:
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return n - 1

def index_to_col(idx: int) -> str:
    if idx < 0:
        return "A"
    x = idx + 1
    out = ""
    while x > 0:
        x, rem = divmod(x - 1, 26)
        out = chr(rem + ord("A")) + out
    return out

def safe_tab_title(title: str) -> str:
    t = (title or "").strip()
    if not t:
        return "Sheet1"
    # Google sheets doesn't allow some chars; keep it simple
    t = re.sub(r"[\[\]\:\*\?\/\\]", " ", t).strip()
    return t[:99] if len(t) > 99 else t


# =============================================================================
# Field typing / formatting inference
# =============================================================================

class FieldType(str, Enum):
    STRING = "string"
    NUMBER = "number"
    INT = "int"
    PERCENT_POINTS = "percent_points"   # IMPORTANT: values are percent points (1.23 means 1.23%)
    DATE = "date"
    DATETIME = "datetime"
    SCORE = "score"

@dataclass(slots=True)
class FieldDef:
    header: str
    field_type: FieldType
    width: int
    align: str
    number_format: Optional[Dict[str, str]] = None

def infer_field_type(header: str) -> FieldType:
    h = (header or "").strip().lower()

    if any(x in h for x in ("last updated", "updated", "timestamp")):
        return FieldType.DATETIME
    if "date" in h:
        return FieldType.DATE

    # percent-like columns in your system are usually percent POINTS
    if "%" in h or "percent" in h or "yield" in h or "margin" in h or "growth" in h or "turnover" in h or "upside" in h:
        return FieldType.PERCENT_POINTS

    if any(x in h for x in ("score", "rsi", "rating", "confidence")):
        return FieldType.SCORE

    if any(x in h for x in ("volume", "shares", "count")):
        return FieldType.INT

    if any(x in h for x in ("price", "close", "open", "high", "low", "cap", "eps", "p/e", "p/b", "p/s", "ev/ebitda", "value")):
        return FieldType.NUMBER

    return FieldType.STRING

def default_format(ft: FieldType) -> Tuple[int, str, Optional[Dict[str, str]]]:
    # width, alignment, number_format
    if ft == FieldType.STRING:
        return 220, "LEFT", None
    if ft == FieldType.INT:
        return 120, "RIGHT", {"type": "NUMBER", "pattern": "#,##0"}
    if ft == FieldType.NUMBER:
        return 120, "RIGHT", {"type": "NUMBER", "pattern": "#,##0.00"}
    if ft == FieldType.PERCENT_POINTS:
        # IMPORTANT: keep as NUMBER, not PERCENT (backend sends percent points)
        return 100, "RIGHT", {"type": "NUMBER", "pattern": "0.00"}
    if ft == FieldType.DATE:
        return 120, "CENTER", {"type": "DATE", "pattern": "yyyy-mm-dd"}
    if ft == FieldType.DATETIME:
        return 180, "CENTER", {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}
    if ft == FieldType.SCORE:
        return 100, "CENTER", {"type": "NUMBER", "pattern": "0.0"}
    return 140, "LEFT", None

def build_field_defs(headers: Sequence[str]) -> List[FieldDef]:
    out: List[FieldDef] = []
    for h in headers:
        ft = infer_field_type(h)
        w, a, nf = default_format(ft)
        # compact some known headers
        hl = h.lower()
        if hl in ("rank", "#"):
            w, a, nf = 60, "CENTER", {"type": "NUMBER", "pattern": "0"}
        if hl in ("symbol",):
            w, a, nf = 110, "LEFT", None
        if hl in ("name", "company name"):
            w, a, nf = 260, "LEFT", None
        out.append(FieldDef(header=h, field_type=ft, width=w, align=a, number_format=nf))
    return out


# =============================================================================
# Core schemas integration (preferred)
# =============================================================================

def _core_schemas_headers(tab_name: str, schema_version: str) -> Optional[List[str]]:
    """
    Tries to pull headers from core.schemas.
    schema_version: "vNext" or "legacy"
    """
    candidates = [
        "core.schemas",
        "schemas",
    ]
    for mod in candidates:
        try:
            m = __import__(mod, fromlist=["get_headers_for_sheet", "resolve_sheet_key"])
            get_headers_for_sheet = getattr(m, "get_headers_for_sheet", None)
            resolve_sheet_key = getattr(m, "resolve_sheet_key", None)
            if callable(get_headers_for_sheet):
                sheet_key = tab_name
                if callable(resolve_sheet_key):
                    try:
                        sheet_key = resolve_sheet_key(tab_name)
                    except Exception:
                        sheet_key = tab_name
                return list(get_headers_for_sheet(sheet_key, version=schema_version))
        except Exception:
            continue
    return None


# fallback aligned to your router (enriched headers = 61)
ENRICHED_HEADERS_61_FALLBACK: List[str] = [
    "Rank", "Symbol", "Origin", "Name", "Sector", "Sub Sector", "Market",
    "Currency", "Listing Date", "Price", "Prev Close", "Change", "Change %", "Day High", "Day Low",
    "52W High", "52W Low", "52W Position %",
    "Volume", "Avg Vol 30D", "Value Traded", "Turnover %", "Shares Outstanding", "Free Float %", "Market Cap", "Free Float Mkt Cap",
    "Liquidity Score", "EPS (TTM)", "Forward EPS", "P/E (TTM)", "Forward P/E", "P/B", "P/S", "EV/EBITDA",
    "Dividend Yield", "Dividend Rate", "Payout Ratio", "Beta",
    "ROE", "ROA", "Net Margin", "EBITDA Margin", "Revenue Growth", "Net Income Growth", "Volatility 30D", "RSI 14",
    "Fair Value", "Upside %", "Valuation Label", "Value Score", "Quality Score", "Momentum Score", "Opportunity Score", "Risk Score",
    "Overall Score", "Error", "Recommendation", "Data Source", "Data Quality",
    "Last Updated (UTC)", "Last Updated (Riyadh)",
]


# =============================================================================
# Google Sheets service import (aligned with your repo)
# =============================================================================

def import_sheets_service_module() -> Optional[Any]:
    """
    Tries multiple import paths so this script works across repo layouts.
    Expects module to expose get_sheets_service().
    """
    paths = [
        "integrations.google_sheets_service",
        "core.integrations.google_sheets_service",
        "google_sheets_service",
        "core.google_sheets_service",
    ]
    for p in paths:
        try:
            m = __import__(p, fromlist=["get_sheets_service"])
            if hasattr(m, "get_sheets_service"):
                return m
        except Exception:
            continue
    return None


# =============================================================================
# Styling builder (Google Sheets API batchUpdate)
# =============================================================================

@dataclass(slots=True)
class StyleConfig:
    font_family: str = "Nunito"
    header_font_size: int = 10
    data_font_size: int = 10
    header_row_height: int = 32
    header_bg: Dict[str, float] = field(default_factory=lambda: {"red": 0.17, "green": 0.24, "blue": 0.31})
    header_fg: Dict[str, float] = field(default_factory=lambda: {"red": 1.0, "green": 1.0, "blue": 1.0})
    band_light: Dict[str, float] = field(default_factory=lambda: {"red": 1.0, "green": 1.0, "blue": 1.0})
    band_dark: Dict[str, float] = field(default_factory=lambda: {"red": 0.96, "green": 0.96, "blue": 0.96})

class StyleBuilder:
    def __init__(self, spreadsheet_id: str, service: Any, config: Optional[StyleConfig] = None):
        self.spreadsheet_id = spreadsheet_id
        self.service = service
        self.config = config or StyleConfig()
        self.requests: List[Dict[str, Any]] = []

    def _chunked_batch_update(self, requests: List[Dict[str, Any]], backoff: FullJitterBackoff) -> None:
        # Google API has practical limits; keep chunks moderate.
        CHUNK = 350
        for i in range(0, len(requests), CHUNK):
            part = requests[i:i + CHUNK]
            backoff.call(
                "batchUpdate",
                lambda p=part: self.service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body={"requests": p},
                ).execute(),
            )

    def add_freeze(self, sheet_id: int, frozen_rows: int, frozen_cols: int) -> None:
        self.requests.append({
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "frozenRowCount": max(0, int(frozen_rows)),
                        "frozenColumnCount": max(0, int(frozen_cols)),
                    }
                },
                "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"
            }
        })

    def add_header_format(self, sheet_id: int, header_row_1based: int, col_count: int) -> None:
        r0 = max(0, header_row_1based - 1)
        self.requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": r0, "endRowIndex": r0 + 1, "startColumnIndex": 0, "endColumnIndex": col_count},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": self.config.header_bg,
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "wrapStrategy": "WRAP",
                        "textFormat": {
                            "foregroundColor": self.config.header_fg,
                            "fontFamily": self.config.font_family,
                            "fontSize": self.config.header_font_size,
                            "bold": True,
                        },
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)"
            }
        })
        self.requests.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": r0, "endIndex": r0 + 1},
                "properties": {"pixelSize": self.config.header_row_height},
                "fields": "pixelSize",
            }
        })

    def add_filter(self, sheet_id: int, header_row_1based: int, col_count: int) -> None:
        r0 = max(0, header_row_1based - 1)
        self.requests.append({
            "setBasicFilter": {
                "filter": {
                    "range": {"sheetId": sheet_id, "startRowIndex": r0, "startColumnIndex": 0, "endColumnIndex": col_count}
                }
            }
        })

    def add_banding(self, sheet_id: int, data_start_row_1based: int, col_count: int) -> None:
        # startRowIndex is 0-based; banding should begin at data start row (not header row)
        start = max(0, data_start_row_1based - 1)
        self.requests.append({
            "addBanding": {
                "bandedRange": {
                    "range": {"sheetId": sheet_id, "startRowIndex": start, "startColumnIndex": 0, "endColumnIndex": col_count},
                    "rowProperties": {"firstBandColor": self.config.band_light, "secondBandColor": self.config.band_dark},
                }
            }
        })

    def add_column_widths(self, sheet_id: int, field_defs: List[FieldDef]) -> None:
        for idx, fd in enumerate(field_defs):
            self.requests.append({
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": idx, "endIndex": idx + 1},
                    "properties": {"pixelSize": int(fd.width)},
                    "fields": "pixelSize",
                }
            })

    def add_column_formats(self, sheet_id: int, field_defs: List[FieldDef], data_start_row_1based: int) -> None:
        start_row = max(0, data_start_row_1based - 1)
        for idx, fd in enumerate(field_defs):
            cell_fmt: Dict[str, Any] = {
                "textFormat": {"fontFamily": self.config.font_family, "fontSize": self.config.data_font_size},
                "horizontalAlignment": fd.align,
            }
            if fd.number_format:
                cell_fmt["numberFormat"] = fd.number_format

            self.requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row,
                        "startColumnIndex": idx,
                        "endColumnIndex": idx + 1,
                    },
                    "cell": {"userEnteredFormat": cell_fmt},
                    "fields": "userEnteredFormat(numberFormat,horizontalAlignment,textFormat)",
                }
            })

    def add_conditional_formats(self, sheet_id: int, headers: List[str], header_row_1based: int) -> None:
        """
        Minimal but effective:
        - Change %: green if >=0, red if <0
        - Upside %: green if >=10, red if <-10
        - Error: red background if not blank
        """
        data_start = header_row_1based + 1
        start_row = max(0, data_start - 1)

        def add_num_rule(col_idx: int, op: str, value: Union[int, float], rgb: Dict[str, float], text_only: bool = True):
            fmt = {"textFormat": {"foregroundColor": rgb}} if text_only else {"backgroundColor": rgb}
            self.requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": sheet_id,
                            "startRowIndex": start_row,
                            "startColumnIndex": col_idx,
                            "endColumnIndex": col_idx + 1,
                        }],
                        "booleanRule": {
                            "condition": {"type": op, "values": [{"userEnteredValue": str(value)}]},
                            "format": fmt
                        }
                    },
                    "index": 0
                }
            })

        def add_text_not_blank(col_idx: int, rgb: Dict[str, float]):
            self.requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": sheet_id,
                            "startRowIndex": start_row,
                            "startColumnIndex": col_idx,
                            "endColumnIndex": col_idx + 1,
                        }],
                        "booleanRule": {
                            "condition": {"type": "TEXT_NOT_CONTAINS", "values": [{"userEnteredValue": ""}]},
                            "format": {"backgroundColor": rgb}
                        }
                    },
                    "index": 0
                }
            })

        # locate columns by header name
        norm = {h.strip().lower(): i for i, h in enumerate(headers)}

        if "change %" in norm:
            i = norm["change %"]
            add_num_rule(i, "NUMBER_GREATER_THAN_EQ", 0, {"red": 0.0, "green": 0.6, "blue": 0.0}, text_only=True)
            add_num_rule(i, "NUMBER_LESS_THAN", 0, {"red": 0.8, "green": 0.0, "blue": 0.0}, text_only=True)

        if "upside %" in norm:
            i = norm["upside %"]
            add_num_rule(i, "NUMBER_GREATER_THAN_EQ", 10, {"red": 0.0, "green": 0.6, "blue": 0.0}, text_only=True)
            add_num_rule(i, "NUMBER_LESS_THAN_EQ", -10, {"red": 0.8, "green": 0.0, "blue": 0.0}, text_only=True)

        if "error" in norm:
            i = norm["error"]
            add_text_not_blank(i, {"red": 1.0, "green": 0.85, "blue": 0.85})

    def execute(self, backoff: FullJitterBackoff) -> None:
        if not self.requests:
            return
        self._chunked_batch_update(self.requests, backoff)
        self.requests = []


# =============================================================================
# Sheet manager
# =============================================================================

class SheetManager:
    def __init__(self, spreadsheet_id: str, service: Any, backoff: FullJitterBackoff):
        self.spreadsheet_id = spreadsheet_id
        self.service = service
        self.backoff = backoff
        self.sheet_id_by_title: Dict[str, int] = {}

    def refresh_meta(self) -> None:
        def _get():
            return self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
        ss = self.backoff.call("spreadsheets.get", _get)
        self.sheet_id_by_title = {}
        for sh in ss.get("sheets", []):
            props = sh.get("properties", {}) or {}
            title = props.get("title")
            sid = props.get("sheetId")
            if title and sid is not None:
                self.sheet_id_by_title[str(title)] = int(sid)

    def tab_exists(self, title: str) -> bool:
        return title in self.sheet_id_by_title

    def get_sheet_id(self, title: str) -> Optional[int]:
        return self.sheet_id_by_title.get(title)

    def create_tab(self, title: str, rows: int = 2000, cols: int = 80) -> int:
        title = safe_tab_title(title)

        def _create():
            return self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={"requests": [{
                    "addSheet": {
                        "properties": {
                            "title": title,
                            "gridProperties": {"rowCount": int(rows), "columnCount": int(cols)},
                        }
                    }
                }]}
            ).execute()

        resp = self.backoff.call("addSheet", _create)
        sheet_id = int(resp["replies"][0]["addSheet"]["properties"]["sheetId"])
        self.sheet_id_by_title[title] = sheet_id
        return sheet_id

    def read_header_row(self, title: str, header_row_1based: int) -> List[str]:
        title = safe_tab_title(title)
        rng = f"'{title}'!{header_row_1based}:{header_row_1based}"

        def _get():
            return self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=rng,
            ).execute()

        try:
            res = self.backoff.call("values.get.headers", _get)
            values = res.get("values", [])
            return list(values[0]) if values else []
        except Exception:
            return []

    def write_header_row(self, title: str, headers: List[str], header_row_1based: int, clear_first: bool) -> None:
        title = safe_tab_title(title)
        start_cell = f"'{title}'!A{header_row_1based}"

        if clear_first:
            end_col = index_to_col(max(0, len(headers) - 1))
            clear_range = f"'{title}'!A{header_row_1based}:{end_col}{header_row_1based}"

            def _clear():
                return self.service.spreadsheets().values().clear(
                    spreadsheetId=self.spreadsheet_id,
                    range=clear_range,
                    body={},
                ).execute()

            self.backoff.call("values.clear.headers", _clear)

        def _update():
            return self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=start_cell,
                valueInputOption="RAW",
                body={"values": [headers]},
            ).execute()

        self.backoff.call("values.update.headers", _update)


# =============================================================================
# Tabs + schema resolution
# =============================================================================

DEFAULT_TABS = [
    "Market_Leaders",
    "KSA_Tadawul",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
    "My_Portfolio",
    "Insights_Analysis",
]

def normalize_tab_key(tab: str) -> str:
    t = (tab or "").strip().lower().replace(" ", "_")
    return t

def pick_headers_for_tab(tab_name: str, schema_version: str) -> List[str]:
    # Core schemas first
    headers = _core_schemas_headers(tab_name, schema_version)
    if headers and len(headers) >= 10:
        return headers

    # fallback: use enriched headers for all
    return list(ENRICHED_HEADERS_61_FALLBACK)

def make_bilingual_headers(headers: List[str]) -> List[str]:
    # Minimal Arabic mapping for common headers; others stay English.
    ar = {
        "Rank": "الترتيب",
        "Symbol": "الرمز",
        "Name": "الاسم",
        "Sector": "القطاع",
        "Sub Sector": "القطاع الفرعي",
        "Market": "السوق",
        "Currency": "العملة",
        "Listing Date": "تاريخ الإدراج",
        "Price": "السعر",
        "Prev Close": "إغلاق سابق",
        "Change": "التغير",
        "Change %": "٪ التغير",
        "Day High": "أعلى اليوم",
        "Day Low": "أدنى اليوم",
        "Volume": "الكمية",
        "Market Cap": "القيمة السوقية",
        "Liquidity Score": "درجة السيولة",
        "Recommendation": "التوصية",
        "Data Source": "مصدر البيانات",
        "Data Quality": "جودة البيانات",
        "Last Updated (UTC)": "آخر تحديث (UTC)",
        "Last Updated (Riyadh)": "آخر تحديث (الرياض)",
        "Error": "خطأ",
    }
    out = []
    for h in headers:
        out.append(f"{h} | {ar.get(h, '')}".rstrip(" |"))
    return out


# =============================================================================
# Worker
# =============================================================================

_IO_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=5, thread_name_prefix="TFB-SheetWorker")

def _headers_non_empty(headers: Sequence[Any]) -> bool:
    if not headers:
        return False
    for x in headers:
        if x is None:
            continue
        s = str(x).strip()
        if s:
            return True
    return False

def process_tab_sync(
    manager: SheetManager,
    tab_name: str,
    *,
    schema_version: str,
    header_row: int,
    force: bool,
    clear: bool,
    create_missing: bool,
    no_style: bool,
    bilingual: bool,
) -> Tuple[str, str]:
    """
    Returns: (tab_name, status) where status in {"success","skipped","failed"}
    """
    tab_name = safe_tab_title(tab_name)

    with TraceContext("process_tab_sync", {"tab": tab_name, "schema_version": schema_version}):
        logger.info("—" * 72)
        logger.info("TAB: %s", tab_name)

        if not manager.tab_exists(tab_name):
            if create_missing:
                try:
                    sid = manager.create_tab(tab_name)
                    logger.info("✅ Created tab '%s' (sheetId=%s)", tab_name, sid)
                except Exception as e:
                    logger.error("❌ Failed creating tab '%s': %s", tab_name, e)
                    return tab_name, "failed"
            else:
                logger.warning("⏭️  Tab '%s' missing (use --create-missing)", tab_name)
                return tab_name, "skipped"

        headers = pick_headers_for_tab(tab_name, schema_version)

        if bilingual:
            headers_to_write = make_bilingual_headers(headers)
        else:
            headers_to_write = list(headers)

        existing = manager.read_header_row(tab_name, header_row)
        if _headers_non_empty(existing) and not force:
            logger.info("⏭️  Headers already exist (use --force to overwrite)")
            return tab_name, "skipped"

        # Write headers
        try:
            manager.write_header_row(tab_name, headers_to_write, header_row, clear_first=clear)
            logger.info("✅ Wrote %d headers to row %d", len(headers_to_write), header_row)
        except Exception as e:
            logger.error("❌ Failed writing headers: %s", e)
            return tab_name, "failed"

        # Style (optional)
        if not no_style:
            try:
                sheet_id = manager.get_sheet_id(tab_name)
                if sheet_id is None:
                    manager.refresh_meta()
                    sheet_id = manager.get_sheet_id(tab_name)
                if sheet_id is None:
                    raise RuntimeError("sheet_id not found after refresh")

                field_defs = build_field_defs(headers)  # IMPORTANT: infer from canonical headers (not bilingual)
                sb = StyleBuilder(manager.spreadsheet_id, manager.service, StyleConfig())
                sb.add_freeze(sheet_id, frozen_rows=header_row, frozen_cols=2)
                sb.add_header_format(sheet_id, header_row, col_count=len(headers))
                sb.add_filter(sheet_id, header_row, col_count=len(headers))
                sb.add_banding(sheet_id, data_start_row_1based=header_row + 1, col_count=len(headers))
                sb.add_column_widths(sheet_id, field_defs)
                sb.add_column_formats(sheet_id, field_defs, data_start_row_1based=header_row + 1)
                sb.add_conditional_formats(sheet_id, headers, header_row_1based=header_row)
                sb.execute(manager.backoff)

                logger.info("✨ Styled '%s' (%d columns)", tab_name, len(headers))
            except Exception as e:
                logger.error("❌ Styling failed for '%s': %s", tab_name, e)
                return tab_name, "failed"

        return tab_name, "success"


async def process_tab_async(*args, **kwargs) -> Tuple[str, str]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_IO_EXECUTOR, lambda: process_tab_sync(*args, **kwargs))


# =============================================================================
# Main
# =============================================================================

async def main_async(args: argparse.Namespace) -> int:
    spreadsheet_id = (args.sheet_id or os.getenv("DEFAULT_SPREADSHEET_ID") or "").strip()
    if not spreadsheet_id:
        logger.error("❌ No spreadsheet id. Provide --sheet-id or set DEFAULT_SPREADSHEET_ID.")
        return 1

    schema_version = (args.schema_version or "vNext").strip()
    if schema_version.lower() in {"legacy", "router", "enriched"}:
        schema_version = "legacy"
    else:
        schema_version = "vNext"

    sheets_mod = import_sheets_service_module()
    if sheets_mod is None:
        logger.error("❌ Cannot import google_sheets_service module. Expected get_sheets_service().")
        logger.error("   Tried: integrations.google_sheets_service, core.integrations.google_sheets_service, google_sheets_service")
        return 1

    try:
        service = sheets_mod.get_sheets_service()
    except Exception as e:
        logger.error("❌ Failed to build Sheets service: %s", e)
        return 1

    backoff = FullJitterBackoff(max_retries=int(args.retry), base_delay=1.0, max_delay=60.0)
    manager = SheetManager(spreadsheet_id, service, backoff)

    try:
        manager.refresh_meta()
    except Exception as e:
        logger.error("❌ Cannot access spreadsheet metadata: %s", e)
        return 1

    if args.all_existing:
        target_tabs = list(manager.sheet_id_by_title.keys())
    else:
        target_tabs = args.tabs or list(DEFAULT_TABS)

    logger.info("=" * 72)
    logger.info("TFB Sheet Initializer v%s", SCRIPT_VERSION)
    logger.info("Spreadsheet: %s", spreadsheet_id)
    logger.info("Schema version: %s", schema_version)
    logger.info("Tabs: %s", ", ".join(target_tabs))
    logger.info("=" * 72)

    stats = {"success": 0, "skipped": 0, "failed": 0}

    async def _run_one(tab: str) -> None:
        try:
            name, status = await process_tab_async(
                manager,
                tab,
                schema_version=schema_version,
                header_row=int(args.row),
                force=bool(args.force),
                clear=bool(args.clear),
                create_missing=bool(args.create_missing),
                no_style=bool(args.no_style),
                bilingual=bool(args.bilingual),
            )
            stats[status] = stats.get(status, 0) + 1
            logger.info("RESULT: %s => %s", name, status.upper())
        except Exception as e:
            stats["failed"] += 1
            logger.error("RESULT: %s => FAILED (%s)", tab, e)

    if args.parallel and len(target_tabs) > 1:
        await asyncio.gather(*[_run_one(t) for t in target_tabs])
    else:
        for t in target_tabs:
            await _run_one(t)

    logger.info("=" * 72)
    logger.info("SUMMARY")
    logger.info("✅ success: %d", stats["success"])
    logger.info("⏭️  skipped: %d", stats["skipped"])
    logger.info("❌ failed : %d", stats["failed"])
    logger.info("=" * 72)

    _IO_EXECUTOR.shutdown(wait=False)
    return 0 if stats["failed"] == 0 else 2


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="TFB Sheet Initializer (aligned with core.schemas)")
    p.add_argument("--sheet-id", help="Spreadsheet ID (or set DEFAULT_SPREADSHEET_ID)")
    p.add_argument("--tabs", nargs="*", help="Tabs to initialize")
    p.add_argument("--all-existing", action="store_true", help="Initialize ALL existing tabs in the spreadsheet")
    p.add_argument("--create-missing", action="store_true", help="Create tabs that do not exist")
    p.add_argument("--schema-version", default="vNext", help="vNext | legacy (router/enriched)")
    p.add_argument("--row", type=int, default=5, help="Header row (1-based), default=5")
    p.add_argument("--force", action="store_true", help="Overwrite if headers already exist")
    p.add_argument("--clear", action="store_true", help="Clear header range before writing")
    p.add_argument("--no-style", action="store_true", help="Write headers only (skip styling)")
    p.add_argument("--bilingual", action="store_true", help="Write headers as 'EN | AR' (single row)")
    p.add_argument("--retry", type=int, default=3, help="Retry attempts for Google API calls")
    p.add_argument("--parallel", action="store_true", help="Process tabs in parallel")
    p.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = p.parse_args(argv)
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
        _IO_EXECUTOR.shutdown(wait=False)
        return 130
    except Exception as e:
        logger.error("Fatal error: %s", e)
        _IO_EXECUTOR.shutdown(wait=False)
        return 1


if __name__ == "__main__":
    sys.exit(main())
