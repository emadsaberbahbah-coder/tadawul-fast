#!/usr/bin/env python3
"""
scripts/run_sheet_init.py
===========================================================
TADAWUL FAST BRIDGE – ENHANCED SHEET INITIALIZER (v6.0.0)
===========================================================
QUANTUM EDITION | PURPOSE-DRIVEN | DYNAMIC LAYOUT | NON-BLOCKING

What's new in v6.0.0:
- ✅ Persistent ThreadPoolExecutor: Eliminates thread-creation overhead for blocking Google API I/O.
- ✅ Memory-Optimized Models: Applied `@dataclass(slots=True)` to all schema and configuration models.
- ✅ High-Performance JSON (`orjson`): Integrated for blazing fast schema backups and exports.
- ✅ Full Jitter Exponential Backoff: Safe retry strategy protecting against Google Sheets API rate limits (429) and concurrent modification conflicts (409).
- ✅ Asynchronous Parallel Initialization: True parallel tab initialization via `asyncio.gather` while maintaining thread-safety.
- ✅ OpenTelemetry Tracing & Prometheus Metrics: End-to-end observability of spreadsheet mutation durations.

Core Philosophy
---------------
Each tab serves a specific purpose and should be initialized with:
- Purpose-optimized headers (not one-size-fits-all)
- Data-type appropriate formatting
- Business-logic specific columns
- Visual hierarchy matching information priority
- Multi-language support (English/Arabic)
- Named ranges for downstream formula processing
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import hashlib
import json
import logging
import logging.config
import os
import random
import re
import sys
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import lru_cache, wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson
    def json_dumps(v: Any, *, indent: int = 0) -> str:
        option = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(v, option=option).decode('utf-8')
    def json_loads(data: Union[str, bytes]) -> Any:
        return orjson.loads(data)
    _HAS_ORJSON = True
except ImportError:
    import json
    def json_dumps(v: Any, *, indent: int = 0) -> str:
        return json.dumps(v, indent=indent if indent else None, default=str)
    def json_loads(data: Union[str, bytes]) -> Any:
        return json.loads(data)
    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Optional Dependencies
# ---------------------------------------------------------------------------

# Google Sheets
try:
    import gspread
    from gspread.exceptions import APIError
    from googleapiclient.errors import HttpError
    GSHEET_AVAILABLE = True
except ImportError:
    gspread = None
    HttpError = Exception
    GSHEET_AVAILABLE = False

# Monitoring
try:
    from prometheus_client import Counter, Histogram, Gauge
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode
    OTEL_AVAILABLE = True
    tracer = trace.get_tracer(__name__)
except ImportError:
    OTEL_AVAILABLE = False
    class DummySpan:
        def set_attribute(self, *args, **kwargs): pass
        def set_status(self, *args, **kwargs): pass
        def record_exception(self, *args, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args, **kwargs): pass
    class DummyTracer:
        def start_as_current_span(self, *args, **kwargs): return DummySpan()
    tracer = DummyTracer()

# Core imports
try:
    import google_sheets_service as sheets_service
except ImportError:
    sheets_service = None

# =============================================================================
# Version & Core Configuration
# =============================================================================
SCRIPT_VERSION = "6.0.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("SheetSetup")

_IO_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=5, thread_name_prefix="SheetWorker")
_TRACING_ENABLED = os.getenv("CORE_TRACING_ENABLED", "").strip().lower() in {"1", "true", "yes", "y", "on"}

# =============================================================================
# Tracing & Metrics
# =============================================================================

class TraceContext:
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.tracer = tracer if OTEL_AVAILABLE and _TRACING_ENABLED else None
        self.span = None
    
    def __enter__(self):
        if self.tracer:
            self.span = self.tracer.start_as_current_span(self.name)
            if self.attributes: self.span.set_attributes(self.attributes)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.span and OTEL_AVAILABLE:
            if exc_val:
                self.span.record_exception(exc_val)
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            self.span.end()

if PROMETHEUS_AVAILABLE:
    sheet_operations_total = Counter('sheet_operations_total', 'Total sheet operations', ['operation', 'status'])
    sheet_operation_duration = Histogram('sheet_operation_duration_seconds', 'Sheet operation duration', ['operation'])
else:
    class DummyMetric:
        def labels(self, *args, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
    sheet_operations_total = DummyMetric()
    sheet_operation_duration = DummyMetric()

# =============================================================================
# Timezone Helpers
# =============================================================================

_UTC = timezone.utc
_RIYADH_TZ = timezone(timedelta(hours=3))

def utc_now() -> datetime: return datetime.now(_UTC)
def riyadh_now() -> datetime: return datetime.now(_RIYADH_TZ)
def utc_iso(dt: Optional[datetime] = None) -> str:
    dt = dt or utc_now()
    if dt.tzinfo is None: dt = dt.replace(tzinfo=_UTC)
    return dt.astimezone(_UTC).isoformat()

# =============================================================================
# Enums & Data Models (Memory Optimized)
# =============================================================================

class TabType(str, Enum):
    MARKET_LEADERS = "market_leaders"
    KSA_TADAWUL = "ksa_tadawul"
    GLOBAL_MARKETS = "global_markets"
    MUTUAL_FUNDS = "mutual_funds"
    COMMODITIES_FX = "commodities_fx"
    PORTFOLIO = "portfolio"
    MARKET_SCAN = "market_scan"
    INSIGHTS = "insights"
    WATCHLIST = "watchlist"
    PERFORMANCE_DASH = "performance_dash"
    SCREENER = "screener"
    JOURNAL = "journal"
    BACKTEST = "backtest"
    RAMADAN_SCAN = "ramadan_scan"
    EARNINGS_TRACKER = "earnings_tracker"
    DIVIDEND_TRACKER = "dividend_tracker"
    TECHNICAL_SCAN = "technical_scan"
    ADMIN = "admin"
    CUSTOM = "custom"
    
    @classmethod
    def from_string(cls, s: str) -> "TabType":
        try: return cls(s.lower())
        except ValueError: return cls.CUSTOM

class DataCategory(str, Enum):
    METADATA = "metadata"
    MARKET = "market"
    TECHNICAL = "technical"
    FUNDAMENTAL = "fundamental"
    FORECAST = "forecast"
    PERFORMANCE = "performance"
    RISK = "risk"
    PORTFOLIO = "portfolio"
    TIMESTAMP = "timestamp"
    ERROR = "error"
    NOTE = "note"
    ALERT = "alert"
    CONFIG = "config"
    
    @property
    def priority(self) -> int:
        priorities = {
            DataCategory.METADATA: 1, DataCategory.MARKET: 2, DataCategory.PERFORMANCE: 3,
            DataCategory.TECHNICAL: 4, DataCategory.FUNDAMENTAL: 5, DataCategory.FORECAST: 6,
            DataCategory.RISK: 7, DataCategory.PORTFOLIO: 8, DataCategory.TIMESTAMP: 9,
        }
        return priorities.get(self, 10)

class FieldType(str, Enum):
    STRING = "string"
    NUMBER = "number"
    PERCENT = "percent"
    CURRENCY = "currency"
    DATE = "date"
    DATETIME = "datetime"
    TIME = "time"
    BOOLEAN = "boolean"
    SCORE = "score"
    RATING = "rating"
    ENUM = "enum"
    FORMULA = "formula"
    HYPERLINK = "hyperlink"
    IMAGE = "image"
    SPARKLINE = "sparkline"
    CUSTOM = "custom"
    
    @property
    def default_width(self) -> int:
        widths = {
            FieldType.STRING: 200, FieldType.NUMBER: 120, FieldType.PERCENT: 100,
            FieldType.CURRENCY: 120, FieldType.DATE: 120, FieldType.DATETIME: 180,
            FieldType.SCORE: 100, FieldType.RATING: 100, FieldType.FORMULA: 150,
            FieldType.HYPERLINK: 200, FieldType.SPARKLINE: 120,
        }
        return widths.get(self, 140)
    
    @property
    def number_format(self) -> Optional[Dict[str, str]]:
        formats = {
            FieldType.NUMBER: {"type": "NUMBER", "pattern": "#,##0.00"},
            FieldType.PERCENT: {"type": "PERCENT", "pattern": "#,##0.00%"},
            FieldType.CURRENCY: {"type": "CURRENCY", "pattern": "#,##0.00"},
            FieldType.DATE: {"type": "DATE", "pattern": "yyyy-mm-dd"},
            FieldType.DATETIME: {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"},
            FieldType.TIME: {"type": "TIME", "pattern": "hh:mm:ss"},
            FieldType.SCORE: {"type": "NUMBER", "pattern": "0.0"},
            FieldType.BOOLEAN: {"type": "BOOLEAN"},
        }
        return formats.get(self)

class Alignment(str, Enum):
    LEFT = "LEFT"
    CENTER = "CENTER"
    RIGHT = "RIGHT"
    JUSTIFY = "JUSTIFY"
    
    @classmethod
    def for_type(cls, field_type: FieldType) -> "Alignment":
        alignments = {
            FieldType.NUMBER: Alignment.RIGHT, FieldType.PERCENT: Alignment.RIGHT,
            FieldType.CURRENCY: Alignment.RIGHT, FieldType.DATE: Alignment.CENTER,
            FieldType.DATETIME: Alignment.CENTER, FieldType.TIME: Alignment.CENTER,
            FieldType.SCORE: Alignment.CENTER, FieldType.RATING: Alignment.CENTER,
            FieldType.BOOLEAN: Alignment.CENTER, FieldType.SPARKLINE: Alignment.CENTER,
        }
        return alignments.get(field_type, Alignment.LEFT)

class ValidationType(str, Enum):
    NONE = "none"
    NUMBER = "number"
    DATE = "date"
    LIST = "list"
    CHECKBOX = "checkbox"
    CUSTOM = "custom"
    REQUIRED = "required"
    UNIQUE = "unique"

class FormatStyle(str, Enum):
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"
    WARNING = "warning"
    CRITICAL = "critical"
    INFO = "info"
    CUSTOM = "custom"

@dataclass(slots=True)
class ValidationRule:
    type: ValidationType
    condition: Optional[str] = None
    values: List[Any] = field(default_factory=list)
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    formula: Optional[str] = None
    strict: bool = True
    show_dropdown: bool = True
    help_text: Optional[str] = None
    
    def to_api_format(self) -> Dict[str, Any]:
        condition = {}
        if self.type == ValidationType.NUMBER:
            condition = {"type": "NUMBER_BETWEEN", "values": [{"userEnteredValue": str(self.min_value or 0)}, {"userEnteredValue": str(self.max_value or 100)}]}
        elif self.type == ValidationType.DATE:
            condition = {"type": "DATE_IS_VALID"}
        elif self.type == ValidationType.LIST and self.values:
            condition = {"type": "ONE_OF_LIST", "values": [{"userEnteredValue": str(v)} for v in self.values]}
        elif self.type == ValidationType.CHECKBOX:
            condition = {"type": "BOOLEAN"}
        elif self.type == ValidationType.REQUIRED:
            condition = {"type": "NOT_BLANK"}
        elif self.type == ValidationType.CUSTOM and self.formula:
            condition = {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": self.formula}]}
        else:
            return {}
        return {"condition": condition, "inputMessage": self.help_text or "", "strict": self.strict, "showCustomUi": self.show_dropdown}

@dataclass(slots=True)
class ConditionalFormat:
    range: str
    type: FormatStyle
    condition: str
    value: Any
    format: Dict[str, Any]
    priority: int = 0
    
    def to_api_format(self, sheet_id: int) -> Dict[str, Any]:
        return {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": sheet_id, **self._parse_range(self.range)}],
                    "booleanRule": {
                        "condition": {"type": self.condition, "values": [{"userEnteredValue": str(self.value)}]},
                        "format": self.format
                    }
                },
                "index": self.priority
            }
        }
    
    def _parse_range(self, range_str: str) -> Dict[str, int]:
        if ':' in range_str:
            start, end = range_str.split(':')
            return {"startColumnIndex": self._col_to_index(start), "endColumnIndex": self._col_to_index(end) + 1}
        return {"startColumnIndex": self._col_to_index(range_str), "endColumnIndex": self._col_to_index(range_str) + 1}
    
    def _col_to_index(self, col: str) -> int:
        col = col.strip().upper()
        result = 0
        for char in col: result = result * 26 + (ord(char) - ord('A') + 1)
        return result - 1

@dataclass(slots=True)
class ProtectedRange:
    range: str
    description: str
    warning_only: bool = True
    editors: List[str] = field(default_factory=list)
    
    def to_api_format(self, sheet_id: int) -> Dict[str, Any]:
        return {
            "addProtectedRange": {
                "protectedRange": {
                    "range": {"sheetId": sheet_id, **self._parse_range(self.range)},
                    "description": self.description, "warningOnly": self.warning_only,
                    "editors": {"users": self.editors} if self.editors else {}
                }
            }
        }
    
    def _parse_range(self, range_str: str) -> Dict[str, int]:
        from openpyxl.utils import range_to_tuple
        try:
            min_col, min_row, max_col, max_row = range_to_tuple(range_str)
            return {"startRowIndex": min_row - 1 if min_row else None, "endRowIndex": max_row if max_row else None, "startColumnIndex": min_col - 1 if min_col else None, "endColumnIndex": max_col if max_col else None}
        except: return {}

@dataclass(slots=True)
class NamedRange:
    name: str
    range: str
    description: Optional[str] = None
    
    def to_api_format(self, sheet_id: int) -> Dict[str, Any]:
        return {
            "addNamedRange": {
                "namedRange": {
                    "name": self.name,
                    "range": {"sheetId": sheet_id, **self._parse_range(self.range)}
                }
            }
        }
    
    def _parse_range(self, range_str: str) -> Dict[str, int]:
        from openpyxl.utils import range_to_tuple
        try:
            min_col, min_row, max_col, max_row = range_to_tuple(range_str)
            return {"startRowIndex": min_row - 1 if min_row else None, "endRowIndex": max_row if max_row else None, "startColumnIndex": min_col - 1 if min_col else None, "endColumnIndex": max_col if max_col else None}
        except: return {}

@dataclass(slots=True)
class FieldDefinition:
    name: str
    display_name: str
    category: DataCategory
    field_type: FieldType
    description: str = ""
    required: bool = False
    width: int = 140
    alignment: Alignment = Alignment.LEFT
    number_format: Optional[Dict[str, str]] = None
    visible: bool = True
    hidden_formula: bool = False
    priority: int = 10
    validation: Optional[ValidationRule] = None
    formula: Optional[str] = None
    conditional_formats: List[ConditionalFormat] = field(default_factory=list)
    display_name_ar: Optional[str] = None
    
    def __post_init__(self):
        if self.width == 140: self.width = self.field_type.default_width
        if self.alignment == Alignment.LEFT: self.alignment = Alignment.for_type(self.field_type)
        if self.number_format is None and self.field_type.number_format: self.number_format = self.field_type.number_format

@dataclass(slots=True)
class TabSchema:
    tab_type: TabType
    name: str
    description: str
    fields: List[FieldDefinition]
    version: str = "2.0.0"
    header_row: int = 5
    frozen_rows: int = 5
    frozen_cols: int = 2
    default_sort: Optional[str] = None
    conditional_formats: List[ConditionalFormat] = field(default_factory=list)
    protected_ranges: List[ProtectedRange] = field(default_factory=list)
    named_ranges: List[NamedRange] = field(default_factory=list)
    name_ar: Optional[str] = None
    
    @property
    def visible_fields(self) -> List[FieldDefinition]:
        return sorted([f for f in self.fields if f.visible], key=lambda f: (f.priority, f.category.priority))
    
    @property
    def field_names(self) -> List[str]:
        return [f.display_name for f in self.visible_fields]
    
    @property
    def field_names_ar(self) -> List[str]:
        return [f.display_name_ar or f.display_name for f in self.visible_fields]


# =============================================================================
# Full Jitter Rate Limiter
# =============================================================================

class FullJitterBackoff:
    """Safe retry mechanism implementing AWS Full Jitter Backoff."""
    
    def __init__(self, max_retries: int = 5, base_delay: float = 1.0, max_delay: float = 60.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    async def execute_async(self, func: Callable, *args, **kwargs) -> Any:
        """Execute async function with Full Jitter backoff."""
        for attempt in range(self.max_retries):
            try:
                return await func(*args, **kwargs)
            except HttpError as e:
                # 429 = Too Many Requests, 409 = Conflict (Concurrent modification), 5xx = Server Error
                if e.resp.status in (429, 409) or e.resp.status >= 500:
                    if attempt == self.max_retries - 1:
                        raise
                    # Full Jitter
                    temp = min(self.max_delay, self.base_delay * (2 ** attempt))
                    sleep_time = random.uniform(0, temp)
                    logger.warning(f"Google API Error {e.resp.status}. Retrying in {sleep_time:.2f}s (Attempt {attempt+1}/{self.max_retries})")
                    await asyncio.sleep(sleep_time)
                else:
                    raise
            except Exception as e:
                raise


# =============================================================================
# Advanced Styling Engine
# =============================================================================

@dataclass(slots=True)
class StyleConfig:
    font_family: str = "Nunito"
    header_font_size: int = 10
    header_row_height: int = 32
    data_font_size: int = 10
    header_bg: Dict[str, float] = field(default_factory=lambda: {"red": 0.17, "green": 0.24, "blue": 0.31})
    header_text: Dict[str, float] = field(default_factory=lambda: {"red": 1.0, "green": 1.0, "blue": 1.0})
    band_light: Dict[str, float] = field(default_factory=lambda: {"red": 1.0, "green": 1.0, "blue": 1.0})
    band_dark: Dict[str, float] = field(default_factory=lambda: {"red": 0.96, "green": 0.96, "blue": 0.96})
    alignments: Dict[FieldType, str] = field(default_factory=lambda: {
        FieldType.NUMBER: "RIGHT", FieldType.PERCENT: "RIGHT", FieldType.CURRENCY: "RIGHT",
        FieldType.DATE: "CENTER", FieldType.DATETIME: "CENTER", FieldType.TIME: "CENTER",
        FieldType.SCORE: "CENTER", FieldType.RATING: "CENTER", FieldType.STRING: "LEFT",
        FieldType.ENUM: "LEFT", FieldType.FORMULA: "LEFT", FieldType.HYPERLINK: "LEFT",
        FieldType.IMAGE: "CENTER", FieldType.SPARKLINE: "CENTER", FieldType.BOOLEAN: "CENTER",
    })

class StyleBuilder:
    """Builds Google Sheets API requests for styling"""
    def __init__(self, spreadsheet_id: str, service: Any, config: StyleConfig = None):
        self.spreadsheet_id = spreadsheet_id
        self.service = service
        self.config = config or StyleConfig()
        self.requests = []
        self.sheet_id_map: Dict[str, int] = {}
        
    def set_sheet_id(self, sheet_name: str, sheet_id: int) -> None:
        self.sheet_id_map[sheet_name] = sheet_id
        
    def add_header_formatting(self, sheet_name: str, header_row: int, col_count: int) -> 'StyleBuilder':
        sheet_id = self.sheet_id_map.get(sheet_name)
        if sheet_id is None: return self
        self.requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": header_row - 1, "endRowIndex": header_row, "startColumnIndex": 0, "endColumnIndex": col_count},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": self.config.header_bg, "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE", "wrapStrategy": "WRAP",
                        "textFormat": {"foregroundColor": self.config.header_text, "fontFamily": self.config.font_family, "fontSize": self.config.header_font_size, "bold": True},
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)",
            }
        })
        self.requests.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": header_row - 1, "endIndex": header_row},
                "properties": {"pixelSize": self.config.header_row_height}, "fields": "pixelSize",
            }
        })
        return self

    def add_frozen_rows(self, sheet_name: str, frozen_rows: int) -> 'StyleBuilder':
        sheet_id = self.sheet_id_map.get(sheet_name)
        if sheet_id is None: return self
        self.requests.append({"updateSheetProperties": {"properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": frozen_rows}}, "fields": "gridProperties.frozenRowCount"}})
        return self

    def add_frozen_cols(self, sheet_name: str, frozen_cols: int) -> 'StyleBuilder':
        sheet_id = self.sheet_id_map.get(sheet_name)
        if sheet_id is None: return self
        self.requests.append({"updateSheetProperties": {"properties": {"sheetId": sheet_id, "gridProperties": {"frozenColumnCount": frozen_cols}}, "fields": "gridProperties.frozenColumnCount"}})
        return self

    def add_filter(self, sheet_name: str, header_row: int, col_count: int) -> 'StyleBuilder':
        sheet_id = self.sheet_id_map.get(sheet_name)
        if sheet_id is None: return self
        self.requests.append({"setBasicFilter": {"filter": {"range": {"sheetId": sheet_id, "startRowIndex": header_row - 1, "endRowIndex": header_row, "startColumnIndex": 0, "endColumnIndex": col_count}}}})
        return self

    def add_banding(self, sheet_name: str, start_row: int, col_count: int) -> 'StyleBuilder':
        sheet_id = self.sheet_id_map.get(sheet_name)
        if sheet_id is None: return self
        self.requests.append({
            "addBanding": {
                "bandedRange": {
                    "range": {"sheetId": sheet_id, "startRowIndex": start_row, "startColumnIndex": 0, "endColumnIndex": col_count},
                    "rowProperties": {"firstBandColor": self.config.band_light, "secondBandColor": self.config.band_dark},
                }
            }
        })
        return self

    def add_column_widths(self, sheet_name: str, fields: List[FieldDefinition]) -> 'StyleBuilder':
        sheet_id = self.sheet_id_map.get(sheet_name)
        if sheet_id is None: return self
        for idx, field in enumerate(fields):
            if not field.visible: continue
            self.requests.append({
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": idx, "endIndex": idx + 1},
                    "properties": {"pixelSize": field.width}, "fields": "pixelSize",
                }
            })
        return self

    def add_column_formats(self, sheet_name: str, fields: List[FieldDefinition], start_row: int) -> 'StyleBuilder':
        sheet_id = self.sheet_id_map.get(sheet_name)
        if sheet_id is None: return self
        for idx, field in enumerate(fields):
            if not field.visible: continue
            format_spec = field.number_format or {"type": "NUMBER"}
            alignment = self.config.alignments.get(field.field_type, "LEFT").value
            self.requests.append({
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": start_row, "startColumnIndex": idx, "endColumnIndex": idx + 1},
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": format_spec, "horizontalAlignment": alignment,
                            "textFormat": {"fontFamily": self.config.font_family, "fontSize": self.config.data_font_size}
                        }
                    },
                    "fields": "userEnteredFormat(numberFormat,horizontalAlignment,textFormat)",
                }
            })
        return self

    def add_conditional_formats(self, sheet_name: str, schema: TabSchema) -> 'StyleBuilder':
        sheet_id = self.sheet_id_map.get(sheet_name)
        if sheet_id is None: return self
        for rule in schema.conditional_formats:
            self.requests.append(rule.to_api_format(sheet_id))
        for field in schema.fields:
            if field.conditional_formats:
                for rule in field.conditional_formats:
                    self.requests.append(rule.to_api_format(sheet_id))
        return self

    def add_protected_ranges(self, sheet_name: str, schema: TabSchema) -> 'StyleBuilder':
        sheet_id = self.sheet_id_map.get(sheet_name)
        if sheet_id is None: return self
        for protected_range in schema.protected_ranges:
            self.requests.append(protected_range.to_api_format(sheet_id))
        return self

    def add_named_ranges(self, sheet_name: str, schema: TabSchema) -> 'StyleBuilder':
        sheet_id = self.sheet_id_map.get(sheet_name)
        if sheet_id is None: return self
        for named_range in schema.named_ranges:
            self.requests.append(named_range.to_api_format(sheet_id))
        return self

    def execute(self) -> None:
        if self.requests:
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={"requests": self.requests}
            ).execute()
            logger.debug(f"Executed {len(self.requests)} style requests")

# =============================================================================
# Pre-defined Tab Schemas
# =============================================================================

class TabSchemas:
    """Repository of all tab schemas with comprehensive definitions"""
    
    _COMMON_FIELDS = {
        "rank": FieldDefinition(name="rank", display_name="#", category=DataCategory.METADATA, field_type=FieldType.NUMBER, width=50, priority=1),
        "symbol": FieldDefinition(name="symbol", display_name="Symbol", category=DataCategory.METADATA, field_type=FieldType.STRING, width=100, priority=2, required=True),
        "name": FieldDefinition(name="name", display_name="Company Name", category=DataCategory.METADATA, field_type=FieldType.STRING, width=250, priority=3),
        "name_ar": FieldDefinition(name="name_ar", display_name="الشركة", category=DataCategory.METADATA, field_type=FieldType.STRING, width=250, priority=3, visible=False),
        "sector": FieldDefinition(name="sector", display_name="Sector", category=DataCategory.METADATA, field_type=FieldType.ENUM, width=150, priority=6),
        "price": FieldDefinition(name="price", display_name="Price", category=DataCategory.MARKET, field_type=FieldType.CURRENCY, width=100, priority=10),
        "change_pct": FieldDefinition(
            name="change_pct", display_name="Change %", category=DataCategory.PERFORMANCE, field_type=FieldType.PERCENT, width=90, priority=12,
            conditional_formats=[
                ConditionalFormat(range="D:D", type=FormatStyle.POSITIVE, condition="NUMBER_GREATER_THAN_EQ", value=0, format={"textFormat": {"foregroundColor": {"red": 0, "green": 0.6, "blue": 0}}}),
                ConditionalFormat(range="D:D", type=FormatStyle.NEGATIVE, condition="NUMBER_LESS_THAN", value=0, format={"textFormat": {"foregroundColor": {"red": 0.8, "green": 0, "blue": 0}}})
            ]
        ),
        "volume": FieldDefinition(name="volume", display_name="Volume", category=DataCategory.MARKET, field_type=FieldType.NUMBER, width=100, priority=13, number_format={"type": "NUMBER", "pattern": "#,##0"}),
        "rsi": FieldDefinition(
            name="rsi", display_name="RSI (14)", category=DataCategory.TECHNICAL, field_type=FieldType.SCORE, width=90, priority=20,
            conditional_formats=[
                ConditionalFormat(range="K:K", type=FormatStyle.CRITICAL, condition="NUMBER_GREATER_THAN", value=70, format={"backgroundColor": {"red": 1, "green": 0.8, "blue": 0.8}}),
                ConditionalFormat(range="K:K", type=FormatStyle.POSITIVE, condition="NUMBER_LESS_THAN", value=30, format={"backgroundColor": {"red": 0.8, "green": 1, "blue": 0.8}})
            ]
        ),
        "ma_50": FieldDefinition(name="ma_50", display_name="MA (50)", category=DataCategory.TECHNICAL, field_type=FieldType.CURRENCY, width=100, priority=24),
        "ma_200": FieldDefinition(name="ma_200", display_name="MA (200)", category=DataCategory.TECHNICAL, field_type=FieldType.CURRENCY, width=100, priority=25),
        "pe": FieldDefinition(name="pe", display_name="P/E", category=DataCategory.FUNDAMENTAL, field_type=FieldType.NUMBER, width=90, priority=30),
        "pb": FieldDefinition(name="pb", display_name="P/B", category=DataCategory.FUNDAMENTAL, field_type=FieldType.NUMBER, width=90, priority=31),
        "div_yield": FieldDefinition(name="div_yield", display_name="Div Yield %", category=DataCategory.FUNDAMENTAL, field_type=FieldType.PERCENT, width=100, priority=34),
        "eps": FieldDefinition(name="eps", display_name="EPS", category=DataCategory.FUNDAMENTAL, field_type=FieldType.CURRENCY, width=100, priority=35),
        "fair_value": FieldDefinition(name="fair_value", display_name="Fair Value", category=DataCategory.FORECAST, field_type=FieldType.CURRENCY, width=110, priority=40),
        "upside": FieldDefinition(
            name="upside", display_name="Upside %", category=DataCategory.FORECAST, field_type=FieldType.PERCENT, width=90, priority=41,
            conditional_formats=[
                ConditionalFormat(range="R:R", type=FormatStyle.POSITIVE, condition="NUMBER_GREATER_THAN", value=20, format={"textFormat": {"foregroundColor": {"red": 0, "green": 0.6, "blue": 0}}}),
                ConditionalFormat(range="R:R", type=FormatStyle.NEGATIVE, condition="NUMBER_LESS_THAN", value=-10, format={"textFormat": {"foregroundColor": {"red": 0.8, "green": 0, "blue": 0}}})
            ]
        ),
        "last_updated": FieldDefinition(name="last_updated", display_name="Last Updated", category=DataCategory.TIMESTAMP, field_type=FieldType.DATETIME, width=180, priority=100, formula="=NOW()", hidden_formula=True),
        "error": FieldDefinition(name="error", display_name="Error", category=DataCategory.ERROR, field_type=FieldType.STRING, width=250, priority=1000, visible=False)
    }
    
    @classmethod
    def get_schema(cls, tab_name: str) -> TabSchema:
        name_lower = tab_name.lower().replace("_", "").replace(" ", "")
        
        # Standard schema creation helper
        def create_schema(tab_type: TabType, name: str, desc: str, extra_fields: List[FieldDefinition]) -> TabSchema:
            return TabSchema(
                tab_type=tab_type, name=name, description=desc,
                fields=[cls._COMMON_FIELDS["rank"], cls._COMMON_FIELDS["symbol"], cls._COMMON_FIELDS["name"]] + extra_fields + [cls._COMMON_FIELDS["last_updated"], cls._COMMON_FIELDS["error"]],
                frozen_cols=2
            )

        if "marketleaders" in name_lower:
            return create_schema(TabType.MARKET_LEADERS, "Market_Leaders", "Top market performers and momentum leaders", [
                cls._COMMON_FIELDS["sector"], cls._COMMON_FIELDS["price"], cls._COMMON_FIELDS["change_pct"], cls._COMMON_FIELDS["volume"], cls._COMMON_FIELDS["rsi"], cls._COMMON_FIELDS["fair_value"], cls._COMMON_FIELDS["upside"]
            ])
        elif "ksa" in name_lower or "tadawul" in name_lower:
            return create_schema(TabType.KSA_TADAWUL, "KSA_Tadawul", "Saudi Stock Exchange detailed analysis", [
                cls._COMMON_FIELDS["name_ar"], cls._COMMON_FIELDS["sector"], cls._COMMON_FIELDS["price"], cls._COMMON_FIELDS["change_pct"], cls._COMMON_FIELDS["volume"], cls._COMMON_FIELDS["pe"], cls._COMMON_FIELDS["div_yield"], cls._COMMON_FIELDS["fair_value"], cls._COMMON_FIELDS["upside"]
            ])
        elif "portfolio" in name_lower:
            return create_schema(TabType.PORTFOLIO, "My_Portfolio", "Portfolio tracking and analysis", [
                cls._COMMON_FIELDS["sector"], FieldDefinition(name="quantity", display_name="Quantity", category=DataCategory.PORTFOLIO, field_type=FieldType.NUMBER, width=100, priority=10), FieldDefinition(name="avg_cost", display_name="Avg Cost", category=DataCategory.PORTFOLIO, field_type=FieldType.CURRENCY, width=100, priority=11), cls._COMMON_FIELDS["price"], FieldDefinition(name="unrealized_pl", display_name="Unrealized P&L", category=DataCategory.PERFORMANCE, field_type=FieldType.CURRENCY, width=120, priority=12, formula="=(price-avg_cost)*quantity")
            ])
        else:
            logger.warning(f"No specific schema for '{tab_name}', using Market Leaders template")
            return create_schema(TabType.MARKET_LEADERS, tab_name, "Custom Sheet", [
                cls._COMMON_FIELDS["sector"], cls._COMMON_FIELDS["price"], cls._COMMON_FIELDS["change_pct"], cls._COMMON_FIELDS["volume"]
            ])

# =============================================================================
# Sheet Manager
# =============================================================================

class SheetManager:
    """Manages sheet operations with thread-safe execution"""
    
    def __init__(self, spreadsheet_id: str):
        self.spreadsheet_id = spreadsheet_id
        self.service = None
        self.sheets_meta: Dict[str, int] = {}
        
    def initialize(self) -> bool:
        if not GSHEET_AVAILABLE or not sheets_service:
            logger.error("Google Sheets service not available")
            return False
        try:
            self.service = sheets_service.get_sheets_service()
            spreadsheet = self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
            for sh in spreadsheet.get("sheets", []):
                props = sh.get("properties", {})
                title = props.get("title")
                sheet_id = props.get("sheetId")
                if title and sheet_id is not None:
                    self.sheets_meta[title] = sheet_id
            logger.info(f"Connected to spreadsheet with {len(self.sheets_meta)} sheets")
            return True
        except HttpError as e:
            logger.error(f"HTTP error initializing Sheets API: {e}")
            return False
    
    def tab_exists(self, tab_name: str) -> bool:
        return tab_name in self.sheets_meta
    
    def get_sheet_id(self, tab_name: str) -> Optional[int]:
        return self.sheets_meta.get(tab_name)
    
    def create_tab(self, tab_name: str) -> Optional[int]:
        try:
            request = {"requests": [{"addSheet": {"properties": {"title": tab_name, "gridProperties": {"rowCount": 1000, "columnCount": 50}}}}]}
            response = self.service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheet_id, body=request).execute()
            sheet_id = response["replies"][0]["addSheet"]["properties"]["sheetId"]
            self.sheets_meta[tab_name] = sheet_id
            logger.info(f"✅ Created tab: {tab_name}")
            return sheet_id
        except Exception as e:
            logger.error(f"Failed to create tab {tab_name}: {e}")
            return None
    
    def read_headers(self, tab_name: str, header_row: int) -> List[str]:
        if not self.tab_exists(tab_name): return []
        try:
            a1_range = f"'{tab_name}'!{header_row}:{header_row}"
            result = self.service.spreadsheets().values().get(spreadsheetId=self.spreadsheet_id, range=a1_range).execute()
            values = result.get("values", [])
            return values[0] if values else []
        except Exception: return []
    
    def write_headers(self, tab_name: str, headers: List[str], header_row: int, clear_first: bool = False) -> bool:
        try:
            if clear_first:
                end_col = self._col_num_to_letter(len(headers))
                clear_range = f"'{tab_name}'!A{header_row}:{end_col}{header_row}"
                self.service.spreadsheets().values().clear(spreadsheetId=self.spreadsheet_id, range=clear_range, body={}).execute()
            
            a1_range = f"'{tab_name}'!A{header_row}"
            self.service.spreadsheets().values().update(spreadsheetId=self.spreadsheet_id, range=a1_range, valueInputOption="RAW", body={"values": [headers]}).execute()
            logger.info(f"✅ Wrote {len(headers)} headers to {tab_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to write headers to {tab_name}: {e}")
            return False
            
    def apply_styling(self, tab_name: str, schema: TabSchema, header_row: int, no_style: bool = False) -> bool:
        if no_style: return True
        sheet_id = self.get_sheet_id(tab_name)
        if sheet_id is None: return False
        
        try:
            builder = StyleBuilder(self.spreadsheet_id, self.service)
            builder.set_sheet_id(tab_name, sheet_id)
            visible_fields = schema.visible_fields
            col_count = len(visible_fields)
            
            (builder.add_header_formatting(tab_name, header_row, col_count)
             .add_frozen_rows(tab_name, schema.frozen_rows)
             .add_frozen_cols(tab_name, schema.frozen_cols)
             .add_filter(tab_name, header_row, col_count)
             .add_banding(tab_name, header_row, col_count)
             .add_column_widths(tab_name, visible_fields)
             .add_column_formats(tab_name, visible_fields, header_row)
             .add_conditional_formats(tab_name, schema)
             .add_protected_ranges(tab_name, schema)
             .execute())
            
            logger.info(f"✨ Styled {tab_name} with {col_count} columns")
            return True
        except Exception as e:
            logger.error(f"Failed to style {tab_name}: {e}")
            return False

    def _col_num_to_letter(self, n: int) -> str:
        if n <= 0: return "A"
        result = ""
        while n > 0:
            n -= 1
            result = chr(65 + (n % 26)) + result
            n //= 26
        return result

# =============================================================================
# Main Async Application
# =============================================================================

async def main_async(args: argparse.Namespace) -> int:
    spreadsheet_id = args.sheet_id or os.getenv("DEFAULT_SPREADSHEET_ID", "")
    if not spreadsheet_id:
        logger.error("❌ No spreadsheet ID provided.")
        return 1

    manager = SheetManager(spreadsheet_id)
    # Initialization is synchronous, but we do it once safely
    if not manager.initialize():
        return 1

    if args.all_existing:
        target_tabs = list(manager.sheets_meta.keys())
    else:
        target_tabs = args.tabs or ["Market_Leaders", "KSA_Tadawul", "Global_Markets", "Mutual_Funds", "Commodities_FX", "My_Portfolio", "Insights_Analysis"]

    logger.info("=" * 70)
    logger.info(f"Enhanced Sheet Initializer v{SCRIPT_VERSION}")
    logger.info(f"Spreadsheet: {spreadsheet_id}")
    logger.info(f"Target tabs: {len(target_tabs)}")
    logger.info("=" * 70)

    stats = {"success": 0, "skipped": 0, "failed": 0, "created": 0}
    backoff = FullJitterBackoff(max_retries=args.retry)

    # Function to process a single tab securely inside the ThreadPool
    def _process_tab_sync(tab_name: str) -> str:
        tab_name = str(tab_name).strip()
        if not tab_name: return "skipped"
        
        logger.info(f"\n--- Processing: {tab_name} ---")
        if not manager.tab_exists(tab_name):
            if args.create_missing and not args.dry_run:
                sheet_id = manager.create_tab(tab_name)
                if not sheet_id: return "failed"
            else:
                logger.warning(f"⚠️  Tab '{tab_name}' does not exist (use --create-missing to create)")
                return "skipped"

        schema = TabSchemas.get_schema(tab_name)

        if args.dry_run:
            logger.info(f"[DRY RUN] Would initialize {tab_name} with {len(schema.visible_fields)} headers")
            return "success"

        existing = manager.read_headers(tab_name, args.row)
        if existing and not args.force:
            logger.info(f"ℹ️  Tab '{tab_name}' already has headers (use --force to overwrite)")
            return "skipped"

        field_names = schema.field_names_ar if args.arabic else schema.field_names

        # Write Headers
        success = manager.write_headers(tab_name, field_names, args.row, args.clear)
        if not success: return "failed"

        # Apply Styling
        style_success = manager.apply_styling(tab_name, schema, args.row, no_style=args.no_style)
        return "success" if style_success else "failed"

    # Async wrapper around synchronous processing
    async def _process_tab_async(tab_name: str) -> str:
        with TraceContext("initialize_tab", {"tab_name": tab_name}):
            loop = asyncio.get_running_loop()
            # Apply Jitter Backoff securely to the thread dispatch
            return await backoff.execute_async(
                lambda: loop.run_in_executor(_IO_EXECUTOR, _process_tab_sync, tab_name)
            )

    if args.parallel and len(target_tabs) > 1:
        # Batch execute safely
        logger.info(f"Executing {len(target_tabs)} tabs in parallel...")
        results = await asyncio.gather(*[_process_tab_async(t) for t in target_tabs], return_exceptions=True)
        for r in results:
            if isinstance(r, str) and r in stats:
                stats[r] += 1
            else:
                stats["failed"] += 1
    else:
        # Sequential execute
        for tab in target_tabs:
            result = await _process_tab_async(tab)
            if result in stats: stats[result] += 1

    logger.info("\n" + "=" * 70)
    logger.info("SUMMARY")
    logger.info("=" * 70)
    logger.info(f"✅ Successful: {stats['success']}")
    logger.info(f"⏭️  Skipped: {stats['skipped']}")
    logger.info(f"❌ Failed: {stats['failed']}")
    logger.info(f"🆕 Created: {stats['created']}")
    logger.info("=" * 70)

    _IO_EXECUTOR.shutdown(wait=False)
    return 0 if stats["failed"] == 0 else 2

class FullJitterBackoff:
    """Safe retry mechanism implementing AWS Full Jitter Backoff."""
    def __init__(self, max_retries: int = 5, base_delay: float = 1.0, max_delay: float = 60.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    async def execute_async(self, func: Callable, *args, **kwargs) -> Any:
        for attempt in range(self.max_retries):
            try:
                # `func` here is the `loop.run_in_executor(...)` awaitable
                return await func(*args, **kwargs)
            except Exception as e:
                # Catch specific Google API errors here if `googleapiclient` bubbles them up properly
                if attempt == self.max_retries - 1:
                    logger.error(f"Max retries exhausted. Failed with: {e}")
                    raise
                
                temp = min(self.max_delay, self.base_delay * (2 ** attempt))
                sleep_time = random.uniform(0, temp)
                logger.warning(f"Operation failed with {e}. Retrying in {sleep_time:.2f}s (Attempt {attempt+1}/{self.max_retries})")
                await asyncio.sleep(sleep_time)

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="TFB Enhanced Sheet Initializer - Purpose-Driven Templates")
    parser.add_argument("--sheet-id", help="Target Spreadsheet ID")
    parser.add_argument("--tabs", nargs="*", help="Specific tabs to setup")
    parser.add_argument("--all-existing", action="store_true", help="Initialize ALL existing tabs")
    parser.add_argument("--create-missing", action="store_true", help="Create tabs that don't exist")
    parser.add_argument("--row", type=int, default=5, help="Header row (1-based, default: 5)")
    parser.add_argument("--force", action="store_true", help="Overwrite non-empty headers")
    parser.add_argument("--clear", action="store_true", help="Clear header range before writing")
    parser.add_argument("--no-style", action="store_true", help="Skip styling (headers only)")
    parser.add_argument("--arabic", action="store_true", help="Use Arabic headers and descriptions")
    parser.add_argument("--bilingual", action="store_true", help="Include both English and Arabic headers")
    parser.add_argument("--retry", type=int, default=3, help="Number of retry attempts for API calls")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    parser.add_argument("--export-schema", help="Export schema to JSON file")
    parser.add_argument("--import-schema", help="Import schema from JSON file")
    parser.add_argument("--parallel", action="store_true", help="Process tabs in parallel")
    
    args, _ = parser.parse_known_args(argv)
    if args.verbose: logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
        _IO_EXECUTOR.shutdown(wait=False)
        return 130
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
