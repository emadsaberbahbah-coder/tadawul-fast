#!/usr/bin/env python3
"""
core/symbols_reader.py
================================================================================
TADAWUL FAST BRIDGE – ENTERPRISE SYMBOLS READER (v8.4.0)
================================================================================
RENDER-ALIGNED | ENGINE-COMPATIBLE | SETTINGS-AWARE | SCHEMA-AWARE
ASYNC-SAFE | CACHE-SAFE

Why this revision (v8.4.0)
- ✅ FIX: Imports Sequence (was used but missing).
- ✅ FIX: Reader is now settings-aware:
    - spreadsheet_id can come from explicit arg, settings, or env
    - SymbolsReader(settings=...) now works properly with lazy engine discovery
- ✅ FIX: Factory/class exports preserve settings and default spreadsheet context.
- ✅ FIX: Stronger actual-tab resolution with canonical matching.
- ✅ FIX: Better fallback diagnostics when no spreadsheet ID or no tabs are available.
- ✅ FIX: Supports direct sheet-name lookup even when not passed as a canonical page key.
- ✅ HARDEN: No import-time network I/O.
- ✅ HARDEN: Safe fallbacks when Google/Redis/OTEL/ML libs are unavailable.
- ✅ PERF: SingleFlight + dual executors + O(1) LRU + optional Redis L2.

Canonical public APIs
---------------------
- get_symbols_for_sheet(...)
- read_symbols_for_sheet(...)
- get_sheet_symbols(...)
- get_symbols_for_page(...)
- list_symbols_for_page(...)
- get_symbols(...)
- list_symbols(...)
- get_page_symbols(...)
- get_universe(...)
- SymbolsReader
- get_reader()
- create_reader()
- build_reader()
"""

from __future__ import annotations

import asyncio
import base64
import concurrent.futures
import hashlib
import logging
import os
import pickle
import random
import re
import sys
import threading
import time
import zlib
from collections import defaultdict, OrderedDict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
SCRIPT_VERSION = "8.4.0"


# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_dumps(v: Any, *, default: Optional[Callable] = None) -> str:
        return orjson.dumps(v, default=default).decode("utf-8")

    def json_loads(v: Union[str, bytes]) -> Any:
        if isinstance(v, str):
            v = v.encode("utf-8")
        return orjson.loads(v)

except Exception:
    import json

    def json_dumps(v: Any, *, default: Optional[Callable] = None) -> str:
        return json.dumps(v, default=default, ensure_ascii=False)

    def json_loads(v: Union[str, bytes]) -> Any:
        if isinstance(v, bytes):
            v = v.decode("utf-8", errors="replace")
        return json.loads(v)


# =============================================================================
# Optional Dependencies with Graceful Degradation
# =============================================================================

try:
    from google.oauth2.service_account import Credentials  # type: ignore
    from googleapiclient.discovery import build  # type: ignore

    GOOGLE_API_AVAILABLE = True
except Exception:
    Credentials = None  # type: ignore
    build = None  # type: ignore
    GOOGLE_API_AVAILABLE = False

try:
    from sklearn.feature_extraction.text import TfidfVectorizer  # type: ignore
    from sklearn.ensemble import RandomForestClassifier  # type: ignore

    ML_AVAILABLE = True
except Exception:
    TfidfVectorizer = None  # type: ignore
    RandomForestClassifier = None  # type: ignore
    ML_AVAILABLE = False

try:
    import redis.asyncio as aioredis  # type: ignore
    from redis.asyncio import Redis  # type: ignore

    REDIS_AVAILABLE = True
except Exception:
    aioredis = None  # type: ignore
    Redis = None  # type: ignore
    REDIS_AVAILABLE = False

try:
    from prometheus_client import Counter, Histogram  # type: ignore

    PROMETHEUS_AVAILABLE = True
except Exception:
    Counter = None  # type: ignore
    Histogram = None  # type: ignore
    PROMETHEUS_AVAILABLE = False

try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore

    OTEL_AVAILABLE = True
except Exception:
    trace = None  # type: ignore
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    OTEL_AVAILABLE = False


# =============================================================================
# Logging & Executors
# =============================================================================
LOG_FORMAT = "%(asctime)s | %(levelname)8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _env_truthy(name: str) -> bool:
    return (os.getenv(name, "") or "").strip().lower() in {
        "1", "true", "yes", "y", "on", "t", "enabled", "active"
    }


logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger("core.symbols_reader")

_IO_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=10,
    thread_name_prefix="SymReadIO",
)
_CPU_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=4,
    thread_name_prefix="SymReadCPU",
)


# =============================================================================
# Metrics & Tracing
# =============================================================================
_TRACING_ENABLED = (
    _env_truthy("ENABLE_TRACING")
    or _env_truthy("CORE_TRACING_ENABLED")
    or _env_truthy("TRACING_ENABLED")
)


class TraceContext:
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._span_cm = None
        self._span = None

    def __enter__(self):
        if OTEL_AVAILABLE and _TRACING_ENABLED and trace is not None:
            tracer = trace.get_tracer(__name__)
            self._span_cm = tracer.start_as_current_span(self.name)
            self._span = self._span_cm.__enter__()
            try:
                for k, v in self.attributes.items():
                    self._span.set_attribute(str(k), v)
            except Exception:
                pass
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._span is not None and OTEL_AVAILABLE and _TRACING_ENABLED:
            try:
                if exc_val is not None and Status is not None and StatusCode is not None:
                    self._span.record_exception(exc_val)
                    self._span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            except Exception:
                pass
        if self._span_cm is not None:
            try:
                return self._span_cm.__exit__(exc_type, exc_val, exc_tb)
            except Exception:
                return False
        return False

    async def __aenter__(self):
        return self.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.__exit__(exc_type, exc_val, exc_tb)


if PROMETHEUS_AVAILABLE and Counter is not None and Histogram is not None:
    reader_requests_total = Counter(
        "symbols_reader_requests_total",
        "Total read requests",
        ["page", "status"],
    )
    reader_discovery_duration = Histogram(
        "symbols_reader_discovery_duration_seconds",
        "Discovery duration",
        ["strategy"],
    )
    reader_cache_hits = Counter(
        "symbols_reader_cache_hits_total",
        "Cache hits",
        ["level"],
    )
    reader_cache_misses = Counter(
        "symbols_reader_cache_misses_total",
        "Cache misses",
        ["level"],
    )
else:
    class _DummyMetric:
        def labels(self, *args, **kwargs):
            return self

        def inc(self, *args, **kwargs):
            return None

        def observe(self, *args, **kwargs):
            return None

    reader_requests_total = _DummyMetric()
    reader_discovery_duration = _DummyMetric()
    reader_cache_hits = _DummyMetric()
    reader_cache_misses = _DummyMetric()


# =============================================================================
# Enums & Types
# =============================================================================
class SymbolType(str, Enum):
    KSA = "ksa"
    GLOBAL = "global"
    INDEX = "index"
    ETF = "etf"
    MUTUAL_FUND = "mutual_fund"
    CURRENCY = "currency"
    CRYPTO = "crypto"
    COMMODITY = "commodity"
    UNKNOWN = "unknown"


class DiscoveryStrategy(str, Enum):
    HEADER = "header"
    DATA_SCAN = "data_scan"
    ML = "ml"
    EXPLICIT = "explicit"
    DEFAULT = "default"


class ConfidenceLevel(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    NONE = "none"


@dataclass(slots=True)
class SymbolMetadata:
    symbol: str
    normalized: str
    symbol_type: SymbolType
    origin: str
    discovery_strategy: DiscoveryStrategy
    confidence: ConfidenceLevel
    sheet_name: Optional[str] = None
    column: Optional[str] = None
    row: Optional[int] = None
    raw_value: Optional[str] = None
    is_active: bool = True
    discovered_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    tags: List[str] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PageSpec:
    key: str
    sheet_names: Tuple[str, ...]
    header_row: int = 5
    start_row: int = 6
    max_rows: int = 5000
    header_candidates: Tuple[str, ...] = ("SYMBOL", "TICKER", "CODE", "STOCK")
    symbol_type: SymbolType = SymbolType.GLOBAL
    confidence_threshold: float = 0.7
    allowed_types: Tuple[SymbolType, ...] = tuple()
    cache_ttl: Optional[int] = None
    required: bool = False
    description: str = ""


# =============================================================================
# Utility Functions
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok", "active"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled", "inactive"}

_BLOCKLIST_EXACT: Set[str] = {
    "SYMBOL", "TICKER", "CODE", "STOCK", "NAME", "RANK", "MARKET", "CURRENCY",
    "PRICE", "LAST", "UPDATED", "LASTUPDATED", "DATE", "TIME", "NOTES", "COMMENT",
    "DESCRIPTION", "TITLE", "HEADER", "COLUMN", "ROW", "VALUE", "DATA", "INFO",
}

_TICKER_PATTERNS: Dict[str, List[re.Pattern[str]]] = {
    "ksa": [
        re.compile(r"^\d{4}$"),
        re.compile(r"^\d{4}\.SR$", re.IGNORECASE),
        re.compile(r"^\d{4}SR$", re.IGNORECASE),
        re.compile(r"^[0-9]{4}\.(SR|TADAWUL)$", re.IGNORECASE),
    ],
    "global": [
        re.compile(r"^[A-Z]{1,5}$"),
        re.compile(r"^[A-Z]{1,4}\.[A-Z]{1,2}$"),
        re.compile(r"^[A-Z]{1,5}-[A-Z]{1,5}$"),
    ],
    "index": [
        re.compile(r"^\^[A-Z]{2,10}$"),
        re.compile(r"^\.[A-Z]{2,10}$"),
        re.compile(r"^[A-Z]{2,10}\.INDX$", re.IGNORECASE),
    ],
    "currency": [
        re.compile(r"^[A-Z]{6}=X$"),
        re.compile(r"^[A-Z]{3}/[A-Z]{3}$"),
        re.compile(r"^[A-Z]{6}$"),
    ],
    "crypto": [
        re.compile(r"^[A-Z0-9]{2,10}-USD$"),
        re.compile(r"^[A-Z0-9]{2,10}/USD$"),
    ],
    "commodity": [
        re.compile(r"^[A-Z]{1,4}=F$"),
        re.compile(r"^[A-Z]{1,4}\d{2}=F$"),
    ],
}

_DEFAULT_SPREADSHEET_ID_ATTRS: Tuple[str, ...] = (
    "default_spreadsheet_id",
    "DEFAULT_SPREADSHEET_ID",
    "spreadsheet_id",
    "SPREADSHEET_ID",
    "google_spreadsheet_id",
    "GOOGLE_SPREADSHEET_ID",
    "google_sheet_id",
    "GOOGLE_SHEET_ID",
    "spreadsheetId",
    "sheet_id",
    "SHEET_ID",
)


def strip_value(v: Any) -> str:
    try:
        return str(v).strip()
    except Exception:
        return ""


def coerce_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    s = strip_value(v).lower()
    if not s:
        return default
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def get_env_str(key: str, default: str = "") -> str:
    return strip_value(os.getenv(key)) or default


def get_env_int(key: str, default: int, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        x = int(float(strip_value(os.getenv(key, str(default)))))
    except Exception:
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


def get_env_float(key: str, default: float, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    try:
        x = float(strip_value(os.getenv(key, str(default))))
    except Exception:
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


def split_cell(cell: Any) -> List[str]:
    raw = strip_value(cell)
    if not raw:
        return []
    parts = re.split(r"[\s,;|\t\n\r]+", raw)
    return [p.strip() for p in parts if p and p.strip()]


def normalize_name_key(name: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", strip_value(name).upper())


def _get_setting_attr(settings: Any, *names: str) -> Any:
    if settings is None:
        return None
    for name in names:
        try:
            if hasattr(settings, name):
                value = getattr(settings, name)
                if value is not None and strip_value(value):
                    return value
            if isinstance(settings, dict) and name in settings:
                value = settings.get(name)
                if value is not None and strip_value(value):
                    return value
        except Exception:
            continue
    return None


def _spreadsheet_id_from_settings(settings: Any) -> str:
    value = _get_setting_attr(settings, *_DEFAULT_SPREADSHEET_ID_ATTRS)
    return strip_value(value)


def _default_spreadsheet_id(settings: Any = None) -> str:
    return (
        _spreadsheet_id_from_settings(settings)
        or strip_value(os.getenv("DEFAULT_SPREADSHEET_ID"))
        or strip_value(os.getenv("SPREADSHEET_ID"))
        or strip_value(os.getenv("GOOGLE_SPREADSHEET_ID"))
        or strip_value(os.getenv("GOOGLE_SHEET_ID"))
        or ""
    )


# =============================================================================
# Full Jitter Backoff & SingleFlight
# =============================================================================
class FullJitterBackoff:
    def __init__(self, max_retries: int = 4, base_delay: float = 0.8, max_delay: float = 20.0):
        self.max_retries = max(1, int(max_retries))
        self.base_delay = max(0.05, float(base_delay))
        self.max_delay = max(self.base_delay, float(max_delay))

    def _retryable_error(self, e: Exception) -> bool:
        s = str(e).lower()
        return any(
            x in s for x in (
                "rate limit", "quota", "429", "500", "502", "503",
                "timeout", "deadline", "temporarily"
            )
        )

    def execute_sync(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_err = e
                if attempt >= self.max_retries - 1 or not self._retryable_error(e):
                    raise
                cap = min(self.max_delay, self.base_delay * (2 ** attempt))
                time.sleep(random.uniform(0, cap))
        if last_err:
            raise last_err
        raise RuntimeError("Backoff failed unexpectedly.")


class SingleFlight:
    def __init__(self):
        self._calls: Dict[str, asyncio.Future[Any]] = {}
        self._lock = asyncio.Lock()

    async def execute(self, key: str, coro_func: Callable[[], Awaitable[Any]]) -> Any:
        async with self._lock:
            if key in self._calls:
                return await self._calls[key]
            fut = asyncio.get_running_loop().create_future()
            self._calls[key] = fut

        try:
            result = await coro_func()
            if not fut.done():
                fut.set_result(result)
            return result
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
            raise
        finally:
            async with self._lock:
                self._calls.pop(key, None)


_SINGLE_FLIGHT = SingleFlight()


# =============================================================================
# Configuration
# =============================================================================
class Config:
    DEFAULT_HEADER_ROW = get_env_int("TFB_SYMBOL_HEADER_ROW", 5, lo=1, hi=200)
    DEFAULT_START_ROW = get_env_int("TFB_SYMBOL_START_ROW", 6, lo=1, hi=500)
    DEFAULT_MAX_ROWS = get_env_int("TFB_SYMBOL_MAX_ROWS", 5000, lo=1, hi=200000)

    CACHE_TTL_SEC = get_env_float(
        "TFB_SYMBOLS_CACHE_TTL_SEC",
        get_env_float("CACHE_TTL_SEC", 300.0),
        lo=1.0,
        hi=86400.0,
    )
    CACHE_DISABLE = coerce_bool(os.getenv("TFB_SYMBOLS_CACHE_DISABLE"), False) or (
        not coerce_bool(os.getenv("CACHE_ENABLED"), True)
    )
    CACHE_BACKEND = get_env_str(
        "TFB_SYMBOLS_CACHE_BACKEND",
        get_env_str("CACHE_BACKEND", "redis" if REDIS_AVAILABLE else "memory"),
    ).lower()
    CACHE_MAX_SIZE = get_env_int(
        "TFB_SYMBOLS_CACHE_MAX_SIZE",
        get_env_int("CACHE_MAX_SIZE", 20000),
        lo=100,
        hi=5_000_000,
    )
    CACHE_COMPRESSION = coerce_bool(
        os.getenv("TFB_SYMBOLS_CACHE_COMPRESSION"),
        coerce_bool(os.getenv("CACHE_COMPRESSION"), True),
    )

    MAX_RETRIES = get_env_int("TFB_SYMBOLS_MAX_RETRIES", 4, lo=1, hi=20)
    REQUEST_TIMEOUT = get_env_float("TFB_SYMBOLS_TIMEOUT", 30.0, lo=3.0, hi=180.0)

    ML_DETECTION_ENABLED = coerce_bool(os.getenv("TFB_SYMBOLS_ML_DETECTION"), True) and ML_AVAILABLE
    ML_CONFIDENCE_THRESHOLD = get_env_float("TFB_SYMBOLS_ML_CONFIDENCE", 0.8, lo=0.0, hi=1.0)

    LOG_DETECTION = coerce_bool(os.getenv("TFB_SYMBOLS_LOG_DETECTION"), True)
    LOG_PERFORMANCE = coerce_bool(os.getenv("TFB_SYMBOLS_LOG_PERFORMANCE"), True)


config = Config()


# =============================================================================
# Page Registry
# =============================================================================
PAGE_REGISTRY: Dict[str, PageSpec] = {
    "MARKET_LEADERS": PageSpec(
        key="MARKET_LEADERS",
        sheet_names=("Market_Leaders", "Market Leaders", "Leaders"),
        header_candidates=("SYMBOL", "TICKER", "CODE", "TRADING SYMBOL", "SECURITY CODE"),
        symbol_type=SymbolType.GLOBAL,
        description="Market leaders list (single source of truth for symbols).",
        required=True,
    ),
    "GLOBAL_MARKETS": PageSpec(
        key="GLOBAL_MARKETS",
        sheet_names=("Global_Markets", "Global Markets", "World Markets"),
        header_candidates=("SYMBOL", "TICKER", "CODE", "PAIR"),
        symbol_type=SymbolType.GLOBAL,
        description="Global markets / indices / shares.",
    ),
    "COMMODITIES_FX": PageSpec(
        key="COMMODITIES_FX",
        sheet_names=("Commodities_FX", "Commodities & FX", "FX & Commodities"),
        header_candidates=("SYMBOL", "TICKER", "PAIR", "COMMODITY", "INSTRUMENT"),
        symbol_type=SymbolType.COMMODITY,
        description="Commodities and FX symbols.",
    ),
    "MUTUAL_FUNDS": PageSpec(
        key="MUTUAL_FUNDS",
        sheet_names=("Mutual_Funds", "Mutual Funds", "Funds"),
        header_candidates=("SYMBOL", "TICKER", "FUND", "CODE"),
        symbol_type=SymbolType.MUTUAL_FUND,
        description="Mutual fund tickers/symbols.",
    ),
    "MY_PORTFOLIO": PageSpec(
        key="MY_PORTFOLIO",
        sheet_names=("My_Portfolio", "My Portfolio", "Portfolio"),
        header_candidates=("SYMBOL", "TICKER", "CODE", "ASSET", "HOLDING"),
        symbol_type=SymbolType.GLOBAL,
        description="User portfolio holdings (optional).",
        required=False,
    ),
    "INSIGHTS_ANALYSIS": PageSpec(
        key="INSIGHTS_ANALYSIS",
        sheet_names=("Insights_Analysis", "Insights Analysis", "Analysis"),
        header_candidates=("SYMBOL", "TICKER", "CODE", "WATCHLIST"),
        symbol_type=SymbolType.GLOBAL,
        description="Derived insights/analysis (may include watchlists).",
        required=False,
    ),
    "TOP_10_INVESTMENTS": PageSpec(
        key="TOP_10_INVESTMENTS",
        sheet_names=("Top_10_Investments", "Top 10 Investments", "Top_10"),
        header_candidates=("SYMBOL", "TICKER", "CODE", "ASSET"),
        symbol_type=SymbolType.GLOBAL,
        description="Top 10 investments (new schema page).",
        required=False,
    ),
    "KSA": PageSpec(
        key="KSA",
        sheet_names=("Market_Leaders", "My_Portfolio", "Insights_Analysis"),
        header_candidates=("SYMBOL", "TICKER", "CODE", "ISIN"),
        symbol_type=SymbolType.KSA,
        allowed_types=(SymbolType.KSA,),
        description="Backward alias (no dedicated KSA_Tadawul sheet required).",
        required=False,
    ),
}

_SHEETNAME_TO_PAGEKEY: Dict[str, str] = {}
for _pk, _spec in PAGE_REGISTRY.items():
    _SHEETNAME_TO_PAGEKEY[normalize_name_key(_pk)] = _pk
    for _nm in _spec.sheet_names:
        _SHEETNAME_TO_PAGEKEY[normalize_name_key(_nm)] = _pk


def resolve_key(key: str) -> str:
    raw = strip_value(key)
    nk = normalize_name_key(raw)
    if nk in _SHEETNAME_TO_PAGEKEY:
        return _SHEETNAME_TO_PAGEKEY[nk]

    k = raw.upper().replace("-", "_").replace(" ", "_")
    aliases = {
        "LEADERS": "MARKET_LEADERS",
        "MARKET": "MARKET_LEADERS",
        "GLOBAL": "GLOBAL_MARKETS",
        "WORLD": "GLOBAL_MARKETS",
        "FX": "COMMODITIES_FX",
        "COMMODITIES": "COMMODITIES_FX",
        "FUNDS": "MUTUAL_FUNDS",
        "PORTFOLIO": "MY_PORTFOLIO",
        "MYPORTFOLIO": "MY_PORTFOLIO",
        "INSIGHTS": "INSIGHTS_ANALYSIS",
        "ANALYSIS": "INSIGHTS_ANALYSIS",
        "TOP10": "TOP_10_INVESTMENTS",
        "TOP_10": "TOP_10_INVESTMENTS",
        "TOP_10_INVESTMENT": "TOP_10_INVESTMENTS",
        "TOP_10_INVESTMENTS": "TOP_10_INVESTMENTS",
        "TADAWUL": "KSA",
        "SAUDI": "KSA",
    }
    return aliases.get(k, k)


# =============================================================================
# Google Sheets Authentication & Service
# =============================================================================
def _read_file_text(path: str) -> str:
    try:
        p = Path(path)
        if p.exists() and p.is_file():
            return p.read_text(encoding="utf-8", errors="replace")
    except Exception:
        pass
    return ""


def _repair_private_key(creds: Dict[str, Any]) -> Dict[str, Any]:
    try:
        pk = creds.get("private_key")
        if isinstance(pk, str) and "\\n" in pk:
            creds = dict(creds)
            creds["private_key"] = pk.replace("\\n", "\n")
    except Exception:
        pass
    return creds


def _load_google_creds_dict_from_env() -> Optional[Dict[str, Any]]:
    raw = (
        os.getenv("GOOGLE_SHEETS_CREDENTIALS")
        or os.getenv("GOOGLE_CREDENTIALS")
        or ""
    ).strip()

    b64 = (
        os.getenv("GOOGLE_SHEETS_CREDENTIALS_B64")
        or os.getenv("GOOGLE_CREDENTIALS_B64")
        or ""
    ).strip()

    file_path = (
        os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
        or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        or ""
    ).strip()

    if file_path:
        raw = raw or _read_file_text(file_path)

    if b64 and not raw:
        raw = b64

    if not raw:
        return None

    if raw.startswith("{") and raw.endswith("}"):
        try:
            obj = json_loads(raw)
            if isinstance(obj, dict):
                return _repair_private_key(obj)
        except Exception:
            return None

    try:
        decoded = base64.b64decode(raw.encode("utf-8"), validate=True)
        obj = json_loads(decoded)
        if isinstance(obj, dict):
            return _repair_private_key(obj)
    except Exception:
        return None

    return None


class GoogleSheetsReader:
    def __init__(self):
        self._service = None
        self._lock = threading.Lock()
        self.backoff = FullJitterBackoff(
            max_retries=config.MAX_RETRIES,
            base_delay=0.8,
            max_delay=20.0,
        )

    def _get_service(self):
        if self._service is not None:
            return self._service

        with self._lock:
            if self._service is not None:
                return self._service

            if not GOOGLE_API_AVAILABLE:
                logger.error("Google API libraries not available")
                return None

            try:
                creds_dict = _load_google_creds_dict_from_env()
                if not creds_dict:
                    raise ValueError("No Google credentials found in environment")

                scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
                credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)  # type: ignore
                self._service = build("sheets", "v4", credentials=credentials, cache_discovery=False)  # type: ignore
                logger.info("Google Sheets service initialized")
                return self._service
            except Exception as e:
                logger.error(f"Failed to initialize Google Sheets service: {e}")
                return None

    @staticmethod
    def safe_sheet_name(name: str) -> str:
        n = strip_value(name) or "Sheet1"
        if any(c in n for c in ("'", " ", "-", "!", ":", "/", "\\", ".", ",")):
            return "'" + n.replace("'", "''") + "'"
        return n

    @staticmethod
    def col_to_letter(col: int) -> str:
        letters = ""
        c = max(1, int(col))
        while c > 0:
            c -= 1
            letters = chr(65 + (c % 26)) + letters
            c //= 26
        return letters or "A"

    def read_range(self, spreadsheet_id: str, range_a1: str) -> List[List[Any]]:
        service = self._get_service()
        if not service:
            return []

        def _execute():
            result = service.spreadsheets().values().get(  # type: ignore
                spreadsheetId=spreadsheet_id,
                range=range_a1,
                valueRenderOption="UNFORMATTED_VALUE",
                dateTimeRenderOption="SERIAL_NUMBER",
            ).execute()
            return result.get("values", []) or []

        try:
            with TraceContext("sheets_read_range", {"range": range_a1}):
                t0 = time.perf_counter()
                out = self.backoff.execute_sync(_execute)
                if config.LOG_PERFORMANCE:
                    dt_ms = (time.perf_counter() - t0) * 1000
                    logger.debug("Sheet read %s: %.1fms", range_a1, dt_ms)
                return out
        except Exception as e:
            logger.error("Failed to read range %s: %s", range_a1, e)
            return []

    async def read_range_async(self, spreadsheet_id: str, range_a1: str) -> List[List[Any]]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(_IO_EXECUTOR, self.read_range, spreadsheet_id, range_a1)

    def list_sheets(self, spreadsheet_id: str) -> List[str]:
        service = self._get_service()
        if not service:
            return []

        def _execute():
            metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()  # type: ignore
            sheets = metadata.get("sheets", []) or []
            out: List[str] = []
            for s in sheets:
                title = (s.get("properties") or {}).get("title")
                if title:
                    out.append(str(title))
            return out

        try:
            return self.backoff.execute_sync(_execute)
        except Exception as e:
            logger.error("Failed to list sheets: %s", e)
            return []


sheets_reader = GoogleSheetsReader()


# =============================================================================
# Cache Manager
# =============================================================================
class AdvancedCache:
    def __init__(self):
        self._memory_cache: OrderedDict[str, Tuple[float, bytes]] = OrderedDict()
        self._memory_lock = asyncio.Lock()
        self._redis_client: Optional["Redis"] = None

        if (not config.CACHE_DISABLE) and config.CACHE_BACKEND == "redis" and REDIS_AVAILABLE:
            redis_url = strip_value(os.getenv("REDIS_URL") or "")
            if redis_url:
                try:
                    self._redis_client = aioredis.from_url(  # type: ignore
                        redis_url,
                        decode_responses=False,
                        max_connections=20,
                    )
                    logger.info("Redis L2 cache initialized")
                except Exception as e:
                    logger.warning("Redis init failed, using L1 only: %s", e)

    @staticmethod
    def _make_key(*parts: Any) -> str:
        key = ":".join(str(p) for p in parts)
        if len(key) > 160:
            key = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return f"sym_reader:{key}"

    @staticmethod
    def _compress_obj(obj: Any) -> bytes:
        raw = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
        if config.CACHE_COMPRESSION:
            return zlib.compress(raw, level=6)
        return raw

    @staticmethod
    def _decompress_obj(blob: bytes) -> Any:
        if config.CACHE_COMPRESSION:
            blob = zlib.decompress(blob)
        return pickle.loads(blob)

    def _ttl_for(self, spec: Optional[PageSpec]) -> float:
        if spec and spec.cache_ttl is not None:
            return float(max(1, int(spec.cache_ttl)))
        return float(config.CACHE_TTL_SEC)

    async def get(self, *key_parts: Any, spec: Optional[PageSpec] = None) -> Optional[Any]:
        if config.CACHE_DISABLE:
            return None

        key = self._make_key(*key_parts)
        now = time.time()

        async with self._memory_lock:
            if key in self._memory_cache:
                expiry, data = self._memory_cache[key]
                if now < expiry:
                    self._memory_cache.move_to_end(key)
                    reader_cache_hits.labels(level="L1").inc()
                    loop = asyncio.get_running_loop()
                    return await loop.run_in_executor(_CPU_EXECUTOR, self._decompress_obj, data)
                del self._memory_cache[key]

        if self._redis_client is not None:
            try:
                data = await self._redis_client.get(key)  # type: ignore
                if data:
                    reader_cache_hits.labels(level="L2").inc()
                    ttl = self._ttl_for(spec)
                    async with self._memory_lock:
                        if len(self._memory_cache) >= config.CACHE_MAX_SIZE:
                            self._memory_cache.popitem(last=False)
                        self._memory_cache[key] = (now + ttl, data)
                        self._memory_cache.move_to_end(key)

                    loop = asyncio.get_running_loop()
                    return await loop.run_in_executor(_CPU_EXECUTOR, self._decompress_obj, data)
            except Exception as e:
                logger.debug("Redis get failed: %s", e)

        reader_cache_misses.labels(level="ALL").inc()
        return None

    async def set(self, value: Any, *key_parts: Any, spec: Optional[PageSpec] = None) -> None:
        if config.CACHE_DISABLE:
            return

        key = self._make_key(*key_parts)
        ttl = self._ttl_for(spec)
        expiry = time.time() + ttl

        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(_CPU_EXECUTOR, self._compress_obj, value)

        async with self._memory_lock:
            if key not in self._memory_cache and len(self._memory_cache) >= config.CACHE_MAX_SIZE:
                self._memory_cache.popitem(last=False)
            self._memory_cache[key] = (expiry, data)
            self._memory_cache.move_to_end(key)

        if self._redis_client is not None:
            try:
                await self._redis_client.setex(key, int(ttl), data)  # type: ignore
            except Exception as e:
                logger.debug("Redis set failed: %s", e)


cache = AdvancedCache()


# =============================================================================
# Symbol Normalizer
# =============================================================================
class SymbolNormalizer:
    @staticmethod
    def _try_core_import() -> Optional[Callable[[str], str]]:
        try:
            from core.symbols.normalize import normalize_symbol  # type: ignore
            return normalize_symbol
        except Exception:
            return None

    def __init__(self):
        self._core_norm = self._try_core_import()

    @lru_cache(maxsize=8192)
    def normalize(self, raw: str) -> Tuple[str, SymbolType]:
        s = strip_value(raw).upper()
        if not s:
            return "", SymbolType.UNKNOWN

        if self._core_norm:
            try:
                core_res = self._core_norm(raw)
                if core_res:
                    s = strip_value(core_res).upper()
            except Exception:
                pass

        for p in ("$", "#", "NYSE:", "NASDAQ:", "TADAWUL:", "SR:", "SAUDI:"):
            if s.startswith(p):
                s = s[len(p):]

        for suf in (".NYSE", ".NASDAQ"):
            if s.endswith(suf):
                s = s[:-len(suf)]

        sym_type = SymbolType.UNKNOWN
        raw_up = strip_value(raw).upper()

        if ".SR" in raw_up or "TADAWUL" in raw_up or "SAUDI" in raw_up:
            sym_type = SymbolType.KSA
        else:
            for pat in _TICKER_PATTERNS["ksa"]:
                if pat.match(s) or pat.match(raw_up):
                    sym_type = SymbolType.KSA
                    break
            if sym_type == SymbolType.UNKNOWN:
                for cat, pats in _TICKER_PATTERNS.items():
                    if cat == "ksa":
                        continue
                    for pat in pats:
                        if pat.match(s) or pat.match(raw_up):
                            try:
                                sym_type = SymbolType(cat)
                            except Exception:
                                sym_type = SymbolType.UNKNOWN
                            break
                    if sym_type != SymbolType.UNKNOWN:
                        break

        if sym_type == SymbolType.KSA:
            if re.match(r"^\d{4}$", s):
                s = f"{s}.SR"
            elif re.match(r"^\d{4}SR$", s):
                s = f"{s[:4]}.SR"
            elif re.match(r"^\d{4}\.SR$", s, re.IGNORECASE):
                s = s[:4] + ".SR"

        if sym_type == SymbolType.UNKNOWN:
            sym_type = SymbolType.GLOBAL

        return s, sym_type

    def is_valid(self, symbol: str) -> bool:
        sym = strip_value(symbol).upper()
        if not sym:
            return False
        if sym in _BLOCKLIST_EXACT:
            return False
        if len(sym) > 40:
            return False
        return True


normalizer = SymbolNormalizer()


# =============================================================================
# ML Detection Engine (optional)
# =============================================================================
class MLDetectionEngine:
    def __init__(self):
        self.model = None
        self.vectorizer = None
        self.trained = False
        self._lock = asyncio.Lock()

    async def train(self, positive_samples: List[str], negative_samples: List[str]) -> None:
        if not ML_AVAILABLE or TfidfVectorizer is None or RandomForestClassifier is None:
            return
        async with self._lock:
            loop = asyncio.get_running_loop()

            def _train():
                texts = positive_samples + negative_samples
                labels = [1] * len(positive_samples) + [0] * len(negative_samples)
                vec = TfidfVectorizer(analyzer="char", ngram_range=(2, 5), max_features=1200)
                X = vec.fit_transform(texts)
                mdl = RandomForestClassifier(n_estimators=120, n_jobs=2, random_state=42)
                mdl.fit(X, labels)
                self.vectorizer = vec
                self.model = mdl
                self.trained = True

            await loop.run_in_executor(_CPU_EXECUTOR, _train)

    async def predict(self, text: str) -> Tuple[bool, float]:
        if not self.trained or not ML_AVAILABLE or self.model is None or self.vectorizer is None:
            return False, 0.0

        loop = asyncio.get_running_loop()

        def _predict():
            X = self.vectorizer.transform([text])
            prob = float(self.model.predict_proba(X)[0][1])
            return prob

        prob = await loop.run_in_executor(_CPU_EXECUTOR, _predict)
        return prob >= config.ML_CONFIDENCE_THRESHOLD, prob


ml_detector = MLDetectionEngine() if config.ML_DETECTION_ENABLED else None


# =============================================================================
# Column Detection Engine
# =============================================================================
class ColumnDetectionEngine:
    async def detect(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        spec: PageSpec,
    ) -> Tuple[Optional[str], DiscoveryStrategy, float, Dict[str, Any]]:
        with TraceContext("detect_symbol_column", {"sheet": sheet_name, "page": spec.key}):
            t0 = time.perf_counter()

            col, conf, meta = await self._detect_by_header(spreadsheet_id, sheet_name, spec)
            if col and conf >= spec.confidence_threshold:
                reader_discovery_duration.labels(strategy=DiscoveryStrategy.HEADER.value).observe(time.perf_counter() - t0)
                return col, DiscoveryStrategy.HEADER, conf, meta

            col, conf, meta = await self._detect_by_data(spreadsheet_id, sheet_name, spec)
            if col and conf >= spec.confidence_threshold:
                reader_discovery_duration.labels(strategy=DiscoveryStrategy.DATA_SCAN.value).observe(time.perf_counter() - t0)
                return col, DiscoveryStrategy.DATA_SCAN, conf, meta

            reader_discovery_duration.labels(strategy=DiscoveryStrategy.DEFAULT.value).observe(time.perf_counter() - t0)
            return "B", DiscoveryStrategy.DEFAULT, 0.5, {"reason": "fallback_to_B"}

    async def _detect_by_header(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        spec: PageSpec,
    ) -> Tuple[Optional[str], float, Dict[str, Any]]:
        range_a1 = f"{GoogleSheetsReader.safe_sheet_name(sheet_name)}!A{spec.header_row}:ZZ{spec.header_row}"
        values = await sheets_reader.read_range_async(spreadsheet_id, range_a1)
        if not values or not values[0]:
            return None, 0.0, {}

        headers = [strip_value(h).upper() for h in values[0]]
        candidates = tuple(strip_value(c).upper() for c in spec.header_candidates)

        for idx, header in enumerate(headers, 1):
            if header in candidates:
                return GoogleSheetsReader.col_to_letter(idx), 1.0, {
                    "matched_header": header,
                    "match": "exact",
                }

        best_score, best_col, best_match = 0.0, None, ""
        for idx, header in enumerate(headers, 1):
            if not header:
                continue
            for cand in candidates:
                if cand and cand in header:
                    score = len(cand) / max(1, len(header))
                    if score > best_score:
                        best_score = score
                        best_col = GoogleSheetsReader.col_to_letter(idx)
                        best_match = cand

        if best_col and best_score >= 0.5:
            return best_col, float(best_score), {
                "matched_header": best_match,
                "match": "partial",
                "score": best_score,
            }

        return None, 0.0, {}

    async def _detect_by_data(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        spec: PageSpec,
    ) -> Tuple[Optional[str], float, Dict[str, Any]]:
        sample_rows = min(250, max(50, spec.max_rows // 20))
        end_row = spec.start_row + sample_rows - 1
        range_a1 = f"{GoogleSheetsReader.safe_sheet_name(sheet_name)}!A{spec.start_row}:ZZ{end_row}"
        values = await sheets_reader.read_range_async(spreadsheet_id, range_a1)
        if not values:
            return None, 0.0, {}

        max_cols = max((len(r) for r in values), default=0)
        if max_cols <= 0:
            return None, 0.0, {}

        col_stats: Dict[int, Dict[str, float]] = defaultdict(
            lambda: {"hits": 0.0, "total": 0.0, "cells": 0.0}
        )

        for row in values:
            for col_idx in range(min(max_cols, 702)):
                if col_idx >= len(row):
                    continue
                cell = row[col_idx]
                if cell is None:
                    continue
                tokens = split_cell(cell)
                if not tokens:
                    continue
                col_stats[col_idx]["cells"] += 1.0
                col_stats[col_idx]["total"] += float(len(tokens))
                for token in tokens:
                    norm, sym_type = normalizer.normalize(token)
                    if not normalizer.is_valid(norm):
                        continue
                    if spec.allowed_types and sym_type not in spec.allowed_types:
                        continue
                    col_stats[col_idx]["hits"] += 1.0

        best_col, best_score, best_meta = None, 0.0, {}
        total_rows = float(len(values)) if values else 1.0

        for col_idx, st in col_stats.items():
            if st["total"] < 8:
                continue
            hit_rate = st["hits"] / max(1.0, st["total"])
            coverage = st["cells"] / total_rows
            score = (hit_rate * 0.75) + (coverage * 0.25)
            if score > best_score:
                best_score = score
                best_col = GoogleSheetsReader.col_to_letter(col_idx + 1)
                best_meta = {
                    "hit_rate": hit_rate,
                    "coverage": coverage,
                    "sample_rows": int(total_rows),
                }

        if best_col and best_score >= 0.3:
            return best_col, float(best_score), best_meta

        return None, 0.0, {}


detector = ColumnDetectionEngine()


# =============================================================================
# Symbol Extractor
# =============================================================================
class SymbolExtractor:
    @staticmethod
    def _letter_to_col(letter: str) -> int:
        col = 0
        for ch in strip_value(letter).upper():
            if "A" <= ch <= "Z":
                col = col * 26 + (ord(ch) - ord("A") + 1)
        return max(1, col)

    def extract_from_column(
        self,
        values: List[List[Any]],
        column_letter: str,
        *,
        start_row: int,
        origin: str,
        sheet_name: str,
        strategy: DiscoveryStrategy,
        confidence_score: float,
        allowed_types: Tuple[SymbolType, ...] = tuple(),
    ) -> List[SymbolMetadata]:
        col_idx = self._letter_to_col(column_letter) - 1
        out: List[SymbolMetadata] = []

        conf_level = (
            ConfidenceLevel.HIGH if confidence_score >= 0.9
            else ConfidenceLevel.MEDIUM if confidence_score >= 0.7
            else ConfidenceLevel.LOW
        )

        for i, row in enumerate(values):
            row_num = start_row + i
            if not row or col_idx >= len(row) or row[col_idx] is None:
                continue
            raw_cell = row[col_idx]
            for token in split_cell(raw_cell):
                norm, sym_type = normalizer.normalize(token)
                if not norm or not normalizer.is_valid(norm):
                    continue
                if allowed_types and sym_type not in allowed_types:
                    continue
                out.append(
                    SymbolMetadata(
                        symbol=norm,
                        normalized=norm,
                        symbol_type=sym_type,
                        origin=origin,
                        discovery_strategy=strategy,
                        confidence=conf_level,
                        sheet_name=sheet_name,
                        column=column_letter,
                        row=row_num,
                        raw_value=strip_value(token)[:200],
                    )
                )
        return out


extractor = SymbolExtractor()


# =============================================================================
# Main discovery helpers
# =============================================================================
def _build_adhoc_spec(sheet_name: str) -> PageSpec:
    safe_name = strip_value(sheet_name) or "Sheet1"
    return PageSpec(
        key=resolve_key(safe_name),
        sheet_names=(safe_name,),
        header_row=config.DEFAULT_HEADER_ROW,
        start_row=config.DEFAULT_START_ROW,
        max_rows=config.DEFAULT_MAX_ROWS,
        header_candidates=("SYMBOL", "TICKER", "CODE", "STOCK", "ASSET", "PAIR", "FUND"),
        symbol_type=SymbolType.GLOBAL,
        description="Ad-hoc direct sheet lookup.",
        required=False,
    )


async def _list_tabs_cached(spreadsheet_id: str) -> List[str]:
    cache_key = ("tabs", spreadsheet_id)
    cached = await cache.get(*cache_key)
    if isinstance(cached, list):
        return [strip_value(x) for x in cached if strip_value(x)]

    loop = asyncio.get_running_loop()
    tabs = await loop.run_in_executor(_IO_EXECUTOR, sheets_reader.list_sheets, spreadsheet_id)
    tabs = [strip_value(x) for x in tabs if strip_value(x)]
    await cache.set(tabs, *cache_key)
    return tabs


async def _resolve_actual_sheet_names(spreadsheet_id: str, preferred_names: Sequence[str]) -> List[str]:
    tabs = await _list_tabs_cached(spreadsheet_id)
    if not tabs:
        return [strip_value(x) for x in preferred_names if strip_value(x)]

    by_norm: Dict[str, List[str]] = defaultdict(list)
    for t in tabs:
        by_norm[normalize_name_key(t)].append(t)

    resolved: List[str] = []
    seen: Set[str] = set()

    for pref in preferred_names:
        p = strip_value(pref)
        if not p:
            continue
        np = normalize_name_key(p)
        matches = by_norm.get(np, [])
        if matches:
            for m in matches:
                if m not in seen:
                    resolved.append(m)
                    seen.add(m)
        else:
            if p not in seen:
                resolved.append(p)
                seen.add(p)

    return resolved


async def _discover_symbols_for_spec(
    spreadsheet_id: str,
    canonical_key: str,
    spec: PageSpec,
) -> Dict[str, Any]:
    start_time = time.perf_counter()

    cache_key = ("page", spreadsheet_id, canonical_key, spec.header_row, spec.start_row, spec.max_rows)
    cached = await cache.get(*cache_key, spec=spec)
    if cached is not None and isinstance(cached, dict):
        cached = dict(cached)
        cached["cache_hit"] = True
        return cached

    actual_sheet_names = await _resolve_actual_sheet_names(spreadsheet_id, spec.sheet_names)

    best_sheet: Optional[str] = None
    best_col: Optional[str] = None
    best_strategy: DiscoveryStrategy = DiscoveryStrategy.DEFAULT
    best_confidence: float = 0.0
    best_detect_meta: Dict[str, Any] = {}
    best_symbols: List[SymbolMetadata] = []
    best_count: int = -1

    for sheet_name in actual_sheet_names:
        col, strategy, confidence, detect_meta = await detector.detect(spreadsheet_id, sheet_name, spec)
        if not col:
            continue

        end_row = spec.start_row + max(1, spec.max_rows) - 1
        range_a1 = f"{GoogleSheetsReader.safe_sheet_name(sheet_name)}!{col}{spec.start_row}:{col}{end_row}"

        values = await sheets_reader.read_range_async(spreadsheet_id, range_a1)
        if not values:
            continue

        symbols = extractor.extract_from_column(
            values,
            col,
            start_row=spec.start_row,
            origin=canonical_key,
            sheet_name=sheet_name,
            strategy=strategy,
            confidence_score=confidence,
            allowed_types=spec.allowed_types,
        )

        cnt = len(symbols)
        if cnt > best_count or (cnt == best_count and confidence > best_confidence):
            best_count = cnt
            best_sheet = sheet_name
            best_col = col
            best_strategy = strategy
            best_confidence = confidence
            best_detect_meta = detect_meta
            best_symbols = symbols

    elapsed_ms = (time.perf_counter() - start_time) * 1000.0
    ts = datetime.now(timezone.utc).isoformat()

    if best_sheet and best_col and best_symbols:
        all_syms: List[str] = []
        by_type: Dict[str, List[str]] = defaultdict(list)
        ksa_syms: List[str] = []
        global_syms: List[str] = []

        seen: Set[str] = set()
        for sm in best_symbols:
            if sm.symbol in seen:
                continue
            seen.add(sm.symbol)
            all_syms.append(sm.symbol)
            by_type[sm.symbol_type.value].append(sm.symbol)
            if sm.symbol_type == SymbolType.KSA:
                ksa_syms.append(sm.symbol)
            else:
                global_syms.append(sm.symbol)

        result: Dict[str, Any] = {
            "all": all_syms,
            "symbols": all_syms,
            "ksa": ksa_syms,
            "global": global_syms,
            "by_type": dict(by_type),
            "metadata": [asdict(x) for x in best_symbols],
            "origin": canonical_key,
            "discovery": {
                "sheet": best_sheet,
                "column": best_col,
                "strategy": best_strategy.value,
                "confidence": best_confidence,
                "detection": best_detect_meta,
                "candidate_sheets": actual_sheet_names,
            },
            "performance": {
                "elapsed_ms": round(elapsed_ms, 2),
                "symbol_count": len(best_symbols),
                "unique_count": len(all_syms),
            },
            "cache_hit": False,
            "timestamp": ts,
            "version": SCRIPT_VERSION,
            "status": "success",
        }

        await cache.set(result, *cache_key, spec=spec)
        reader_requests_total.labels(page=canonical_key, status="success").inc()
        return result

    result = {
        "all": [],
        "symbols": [],
        "ksa": [],
        "global": [],
        "by_type": {},
        "metadata": [],
        "origin": canonical_key,
        "discovery": {
            "candidate_sheets": actual_sheet_names,
        },
        "performance": {
            "elapsed_ms": round(elapsed_ms, 2),
            "symbol_count": 0,
            "unique_count": 0,
        },
        "cache_hit": False,
        "timestamp": ts,
        "version": SCRIPT_VERSION,
        "status": "empty",
        "warning": "No symbols found",
    }
    await cache.set(result, *cache_key, spec=spec)
    reader_requests_total.labels(page=canonical_key, status="empty").inc()
    return result


async def get_page_symbols_async(
    key: str,
    spreadsheet_id: Optional[str] = None,
    settings: Any = None,
) -> Dict[str, Any]:
    sid = strip_value(spreadsheet_id) or _default_spreadsheet_id(settings)
    if not sid:
        return {
            "all": [],
            "symbols": [],
            "error": "No spreadsheet ID (explicit/settings/env missing)",
            "status": "error",
            "version": SCRIPT_VERSION,
        }

    canonical_key = resolve_key(key)
    spec = PAGE_REGISTRY.get(canonical_key)
    if not spec:
        spec = _build_adhoc_spec(key)
        canonical_key = spec.key

    flight_key = f"page:{sid}:{canonical_key}:{spec.header_row}:{spec.start_row}:{spec.max_rows}"
    return await _SINGLE_FLIGHT.execute(
        flight_key,
        lambda: _discover_symbols_for_spec(sid, canonical_key, spec),
    )


async def get_universe_async(
    keys: List[str],
    spreadsheet_id: Optional[str] = None,
    settings: Any = None,
) -> Dict[str, Any]:
    start_time = time.perf_counter()
    sid = strip_value(spreadsheet_id) or _default_spreadsheet_id(settings)
    if not sid:
        return {
            "symbols": [],
            "error": "No spreadsheet ID (explicit/settings/env missing)",
            "status": "error",
            "version": SCRIPT_VERSION,
        }

    canonical_keys = [resolve_key(k) for k in keys]
    tasks = [get_page_symbols_async(k, sid, settings=settings) for k in canonical_keys]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_symbols: List[str] = []
    origin_map: Dict[str, str] = {}
    by_origin: Dict[str, List[str]] = defaultdict(list)
    by_type: Dict[str, List[str]] = defaultdict(list)
    page_meta: Dict[str, Any] = {}

    for req_key, res in zip(canonical_keys, results):
        if isinstance(res, Exception):
            page_meta[req_key] = {"status": "error", "error": str(res)}
            continue
        if not isinstance(res, dict):
            page_meta[req_key] = {"status": "error", "error": "invalid_result"}
            continue

        status = res.get("status")
        page_meta[req_key] = {
            "status": status,
            "elapsed_ms": (res.get("performance") or {}).get("elapsed_ms"),
            "discovery": res.get("discovery"),
            "count": len(res.get("all") or []),
        }

        if status != "success":
            continue

        syms = res.get("all") or []
        metas = res.get("metadata") or []
        for s in syms:
            if s not in origin_map:
                origin_map[s] = req_key
                all_symbols.append(s)
                by_origin[req_key].append(s)

        for m in metas:
            try:
                t = strip_value(m.get("symbol_type"))
                sym = strip_value(m.get("symbol"))
                if t and sym and origin_map.get(sym) == req_key:
                    by_type[t].append(sym)
            except Exception:
                continue

    by_type = {k: sorted(list(set(v))) for k, v in by_type.items()}
    all_symbols = sorted(list(dict.fromkeys(all_symbols)))

    elapsed_ms = (time.perf_counter() - start_time) * 1000.0
    return {
        "symbols": all_symbols,
        "by_origin": {k: v for k, v in by_origin.items()},
        "by_type": by_type,
        "origin_map": origin_map,
        "metadata": page_meta,
        "performance": {
            "elapsed_ms": round(elapsed_ms, 2),
            "pages_requested": len(keys),
            "pages_processed": len(page_meta),
            "total_symbols": len(all_symbols),
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": SCRIPT_VERSION,
        "status": "success",
    }


# =============================================================================
# Engine-compatible wrappers
# =============================================================================
async def get_symbols_for_sheet_async(
    sheet: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    page: Optional[str] = None,
    sheet_name: Optional[str] = None,
    tab: Optional[str] = None,
    name: Optional[str] = None,
    worksheet: Optional[str] = None,
    settings: Any = None,
    **kwargs: Any,
) -> List[str]:
    target = sheet or page or sheet_name or tab or name or worksheet or ""
    result = await get_page_symbols_async(
        target,
        spreadsheet_id=spreadsheet_id,
        settings=settings,
    )
    syms = result.get("all") or result.get("symbols") or []
    return list(syms)[: max(1, int(limit or 5000))]


async def read_symbols_for_sheet_async(
    sheet: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    settings: Any = None,
    **kwargs: Any,
) -> List[str]:
    return await get_symbols_for_sheet_async(
        sheet=sheet,
        spreadsheet_id=spreadsheet_id,
        limit=limit,
        settings=settings,
        **kwargs,
    )


async def get_sheet_symbols_async(
    sheet: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    settings: Any = None,
    **kwargs: Any,
) -> List[str]:
    return await get_symbols_for_sheet_async(
        sheet=sheet,
        spreadsheet_id=spreadsheet_id,
        limit=limit,
        settings=settings,
        **kwargs,
    )


async def get_symbols_for_page_async(
    page: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    sheet: Optional[str] = None,
    settings: Any = None,
    **kwargs: Any,
) -> List[str]:
    return await get_symbols_for_sheet_async(
        sheet=page or sheet,
        spreadsheet_id=spreadsheet_id,
        limit=limit,
        settings=settings,
        **kwargs,
    )


async def list_symbols_for_page_async(
    page: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    sheet: Optional[str] = None,
    settings: Any = None,
    **kwargs: Any,
) -> List[str]:
    return await get_symbols_for_page_async(
        page=page,
        sheet=sheet,
        spreadsheet_id=spreadsheet_id,
        limit=limit,
        settings=settings,
        **kwargs,
    )


async def get_symbols_async(
    sheet: Optional[str] = None,
    page: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    settings: Any = None,
    **kwargs: Any,
) -> List[str]:
    return await get_symbols_for_sheet_async(
        sheet=sheet or page,
        spreadsheet_id=spreadsheet_id,
        limit=limit,
        settings=settings,
        **kwargs,
    )


async def list_symbols_async(
    sheet: Optional[str] = None,
    page: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    settings: Any = None,
    **kwargs: Any,
) -> List[str]:
    return await get_symbols_async(
        sheet=sheet,
        page=page,
        spreadsheet_id=spreadsheet_id,
        limit=limit,
        settings=settings,
        **kwargs,
    )


# =============================================================================
# Synchronous wrappers
# =============================================================================
def _run_coro_sync(coro: Awaitable[Any]) -> Any:
    try:
        asyncio.get_running_loop()
        box: Dict[str, Any] = {"value": None, "error": None}

        def _worker():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                box["value"] = loop.run_until_complete(coro)
            except Exception as e:
                box["error"] = e
            finally:
                loop.close()

        t = threading.Thread(target=_worker, daemon=True)
        t.start()
        t.join()
        if box["error"] is not None:
            raise box["error"]
        return box["value"]
    except RuntimeError:
        return asyncio.run(coro)


def get_page_symbols(
    key: str,
    spreadsheet_id: Optional[str] = None,
    settings: Any = None,
) -> Dict[str, Any]:
    return _run_coro_sync(get_page_symbols_async(key, spreadsheet_id, settings=settings))


def get_universe(
    keys: List[str],
    spreadsheet_id: Optional[str] = None,
    settings: Any = None,
) -> Dict[str, Any]:
    return _run_coro_sync(get_universe_async(keys, spreadsheet_id, settings=settings))


def get_symbols_for_sheet(
    sheet: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    settings: Any = None,
    **kwargs: Any,
) -> List[str]:
    return _run_coro_sync(
        get_symbols_for_sheet_async(
            sheet=sheet,
            spreadsheet_id=spreadsheet_id,
            limit=limit,
            settings=settings,
            **kwargs,
        )
    )


def read_symbols_for_sheet(
    sheet: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    settings: Any = None,
    **kwargs: Any,
) -> List[str]:
    return _run_coro_sync(
        read_symbols_for_sheet_async(
            sheet=sheet,
            spreadsheet_id=spreadsheet_id,
            limit=limit,
            settings=settings,
            **kwargs,
        )
    )


def get_sheet_symbols(
    sheet: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    settings: Any = None,
    **kwargs: Any,
) -> List[str]:
    return _run_coro_sync(
        get_sheet_symbols_async(
            sheet=sheet,
            spreadsheet_id=spreadsheet_id,
            limit=limit,
            settings=settings,
            **kwargs,
        )
    )


def get_symbols_for_page(
    page: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    settings: Any = None,
    **kwargs: Any,
) -> List[str]:
    return _run_coro_sync(
        get_symbols_for_page_async(
            page=page,
            spreadsheet_id=spreadsheet_id,
            limit=limit,
            settings=settings,
            **kwargs,
        )
    )


def list_symbols_for_page(
    page: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    settings: Any = None,
    **kwargs: Any,
) -> List[str]:
    return _run_coro_sync(
        list_symbols_for_page_async(
            page=page,
            spreadsheet_id=spreadsheet_id,
            limit=limit,
            settings=settings,
            **kwargs,
        )
    )


def get_symbols(
    sheet: Optional[str] = None,
    page: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    settings: Any = None,
    **kwargs: Any,
) -> List[str]:
    return _run_coro_sync(
        get_symbols_async(
            sheet=sheet,
            page=page,
            spreadsheet_id=spreadsheet_id,
            limit=limit,
            settings=settings,
            **kwargs,
        )
    )


def list_symbols(
    sheet: Optional[str] = None,
    page: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    limit: int = 5000,
    settings: Any = None,
    **kwargs: Any,
) -> List[str]:
    return _run_coro_sync(
        list_symbols_async(
            sheet=sheet,
            page=page,
            spreadsheet_id=spreadsheet_id,
            limit=limit,
            settings=settings,
            **kwargs,
        )
    )


def list_tabs(spreadsheet_id: Optional[str] = None, settings: Any = None) -> List[str]:
    sid = strip_value(spreadsheet_id) or _default_spreadsheet_id(settings)
    if not sid:
        return []
    return sheets_reader.list_sheets(sid)


def supported_pages() -> List[str]:
    return sorted(PAGE_REGISTRY.keys())


# =============================================================================
# Reader object + factories for engine lazy discovery
# =============================================================================
class SymbolsReader:
    def __init__(self, settings: Any = None, spreadsheet_id: Optional[str] = None):
        self.settings = settings
        self.spreadsheet_id = strip_value(spreadsheet_id) or _default_spreadsheet_id(settings)

    async def get_symbols_for_sheet(self, sheet: Optional[str] = None, limit: int = 5000, **kwargs: Any) -> List[str]:
        return await get_symbols_for_sheet_async(
            sheet=sheet,
            spreadsheet_id=self.spreadsheet_id,
            limit=limit,
            settings=self.settings,
            **kwargs,
        )

    async def read_symbols_for_sheet(self, sheet: Optional[str] = None, limit: int = 5000, **kwargs: Any) -> List[str]:
        return await read_symbols_for_sheet_async(
            sheet=sheet,
            spreadsheet_id=self.spreadsheet_id,
            limit=limit,
            settings=self.settings,
            **kwargs,
        )

    async def get_sheet_symbols(self, sheet: Optional[str] = None, limit: int = 5000, **kwargs: Any) -> List[str]:
        return await get_sheet_symbols_async(
            sheet=sheet,
            spreadsheet_id=self.spreadsheet_id,
            limit=limit,
            settings=self.settings,
            **kwargs,
        )

    async def get_symbols_for_page(self, page: Optional[str] = None, limit: int = 5000, **kwargs: Any) -> List[str]:
        return await get_symbols_for_page_async(
            page=page,
            spreadsheet_id=self.spreadsheet_id,
            limit=limit,
            settings=self.settings,
            **kwargs,
        )

    async def list_symbols_for_page(self, page: Optional[str] = None, limit: int = 5000, **kwargs: Any) -> List[str]:
        return await list_symbols_for_page_async(
            page=page,
            spreadsheet_id=self.spreadsheet_id,
            limit=limit,
            settings=self.settings,
            **kwargs,
        )

    async def get_symbols(self, sheet: Optional[str] = None, page: Optional[str] = None, limit: int = 5000, **kwargs: Any) -> List[str]:
        return await get_symbols_async(
            sheet=sheet,
            page=page,
            spreadsheet_id=self.spreadsheet_id,
            limit=limit,
            settings=self.settings,
            **kwargs,
        )

    async def list_symbols(self, sheet: Optional[str] = None, page: Optional[str] = None, limit: int = 5000, **kwargs: Any) -> List[str]:
        return await list_symbols_async(
            sheet=sheet,
            page=page,
            spreadsheet_id=self.spreadsheet_id,
            limit=limit,
            settings=self.settings,
            **kwargs,
        )


def get_reader(*args: Any, **kwargs: Any) -> SymbolsReader:
    return SymbolsReader(*args, **kwargs)


def create_reader(*args: Any, **kwargs: Any) -> SymbolsReader:
    return SymbolsReader(*args, **kwargs)


def build_reader(*args: Any, **kwargs: Any) -> SymbolsReader:
    return SymbolsReader(*args, **kwargs)


# =============================================================================
# CLI Entry Point
# =============================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=f"TFB Symbols Reader v{SCRIPT_VERSION}")
    parser.add_argument("--sheet-id", help="Spreadsheet ID override")
    parser.add_argument("--key", default="MARKET_LEADERS", help="Page key or sheet name")
    parser.add_argument("--keys", nargs="+", help="Multiple keys for universe")
    parser.add_argument("--list-tabs", action="store_true", help="List tabs and exit")
    parser.add_argument("--list-pages", action="store_true", help="List supported page keys and exit")
    parser.add_argument("--output", choices=["text", "json"], default="text", help="Output format")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--no-cache", action="store_true", help="Disable cache")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    if args.no_cache:
        config.CACHE_DISABLE = True

    if args.list_pages:
        pages = supported_pages()
        sys.stdout.write((json_dumps({"pages": pages}) if args.output == "json" else "\n".join(pages)) + "\n")
        raise SystemExit(0)

    if args.list_tabs:
        tabs = list_tabs(args.sheet_id)
        sys.stdout.write((json_dumps({"tabs": tabs}) if args.output == "json" else "\n".join(tabs) if tabs else "No tabs found") + "\n")
        raise SystemExit(0)

    if args.keys:
        result = get_universe(args.keys, spreadsheet_id=args.sheet_id)
    else:
        result = get_page_symbols(args.key, spreadsheet_id=args.sheet_id)

    if args.output == "json":
        sys.stdout.write(json_dumps(result) + "\n")
    else:
        syms = result.get("symbols", []) if isinstance(result, dict) else []
        sys.stdout.write(f"Symbols Reader v{SCRIPT_VERSION}\n")
        sys.stdout.write(f"Status: {result.get('status', 'unknown')}\n")
        sys.stdout.write(f"Count: {len(syms)}\n")
        perf = result.get("performance") or {}
        if isinstance(perf, dict) and "elapsed_ms" in perf:
            sys.stdout.write(f"Time: {float(perf.get('elapsed_ms') or 0.0):.1f}ms\n")
        bt = result.get("by_type") or {}
        if isinstance(bt, dict) and bt:
            sys.stdout.write("\nBy Type:\n")
            for t_name, lst in bt.items():
                try:
                    sys.stdout.write(f"  {t_name}: {len(lst)}\n")
                except Exception:
                    continue
        if syms:
            sys.stdout.write("\nPreview:\n  " + ", ".join(list(syms)[:20]) + "\n")


__all__ = [
    "SCRIPT_VERSION",
    "SymbolType",
    "DiscoveryStrategy",
    "ConfidenceLevel",
    "SymbolMetadata",
    "PageSpec",
    "PAGE_REGISTRY",
    "SymbolsReader",
    "get_reader",
    "create_reader",
    "build_reader",
    "get_page_symbols",
    "get_page_symbols_async",
    "get_universe",
    "get_universe_async",
    "get_symbols_for_sheet",
    "get_symbols_for_sheet_async",
    "read_symbols_for_sheet",
    "read_symbols_for_sheet_async",
    "get_sheet_symbols",
    "get_sheet_symbols_async",
    "get_symbols_for_page",
    "get_symbols_for_page_async",
    "list_symbols_for_page",
    "list_symbols_for_page_async",
    "get_symbols",
    "get_symbols_async",
    "list_symbols",
    "list_symbols_async",
    "list_tabs",
    "supported_pages",
    "normalizer",
    "extractor",
    "detector",
]
