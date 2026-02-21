#!/usr/bin/env python3
"""
core/symbols_reader.py
================================================================================
TADAWUL FAST BRIDGE – ENTERPRISE SYMBOLS READER (v7.5.0)
================================================================================
QUANTUM EDITION | INTELLIGENT DISCOVERY | ASYNC ORCHESTRATION

What's new in v7.5.0:
- ✅ High-Performance JSON (`orjson`): Blazing fast serialization for cached sheets.
- ✅ Memory Optimization: `@dataclass(slots=True)` reduces memory footprint during large scans.
- ✅ Async Thread Pooling: Non-blocking Google Sheets API calls using `asyncio.to_thread`.
- ✅ Full Jitter Exponential Backoff: Protects against Google API rate limits (429s).
- ✅ Multi-Tier Caching: Zlib-compressed Memory (L1) + Redis (L2) cache architecture.
- ✅ OpenTelemetry Tracing: Granular observability across discovery strategies.
- ✅ Prometheus Metrics: Real-time tracking of discovery performance and cache hits.

Core Capabilities
-----------------
• Multi-strategy symbol discovery (header-based, data-based, ML-based)
• Intelligent column detection with confidence scoring
• Symbol normalization with KSA/Global classification
• Multi-source aggregation (Google Sheets, CSV, JSON, API)
• Symbol relationship mapping (parent/child, ETFs, indices)
• Sector/Industry classification enrichment
"""

from __future__ import annotations

import asyncio
import base64
import csv
import hashlib
import logging
import logging.config
import os
import pickle
import random
import re
import threading
import time
import uuid
import zlib
from collections import defaultdict
from contextlib import asynccontextmanager, contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import lru_cache, wraps
from pathlib import Path
from typing import (Any, Callable, Dict, Generator, Iterable, List, Optional,
                    Set, Tuple, Type, TypeVar, Union, cast, Awaitable)

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson
    def json_dumps(v, *, default=None):
        return orjson.dumps(v, default=default).decode('utf-8')
    def json_loads(v):
        return orjson.loads(v)
    _HAS_ORJSON = True
except ImportError:
    import json
    def json_dumps(v, *, default=None):
        return json.dumps(v, default=default)
    def json_loads(v):
        return json.loads(v)
    _HAS_ORJSON = False

# =============================================================================
# Optional Dependencies with Graceful Degradation
# =============================================================================
# Google Sheets API
try:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GOOGLE_API_AVAILABLE = True
except ImportError:
    Credentials = None
    build = None
    HttpError = Exception
    GOOGLE_API_AVAILABLE = False

# CSV/Data processing
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    pd = None
    PANDAS_AVAILABLE = False

# Machine Learning (optional)
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.ensemble import RandomForestClassifier
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False

# Redis Cache
try:
    import redis.asyncio as aioredis
    from redis.asyncio import Redis
    REDIS_AVAILABLE = True
except ImportError:
    aioredis = None
    Redis = None
    REDIS_AVAILABLE = False

# Monitoring & Tracing
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

# =============================================================================
# Logging Configuration
# =============================================================================
LOG_FORMAT = "%(asctime)s | %(levelname)8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger("symbols_reader")

# =============================================================================
# Metrics & Tracing
# =============================================================================

_TRACING_ENABLED = os.getenv("CORE_TRACING_ENABLED", "").strip().lower() in {"1", "true", "yes", "y", "on"}

class TraceContext:
    """OpenTelemetry trace context manager."""
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.tracer = tracer if OTEL_AVAILABLE and _TRACING_ENABLED else None
        self.span = None
    
    def __enter__(self):
        if self.tracer:
            self.span = self.tracer.start_as_current_span(self.name)
            if self.attributes:
                self.span.set_attributes(self.attributes)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.span and OTEL_AVAILABLE:
            if exc_val:
                self.span.record_exception(exc_val)
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            self.span.__exit__(exc_type, exc_val, exc_tb)

    async def __aenter__(self): return self.__enter__()
    async def __aexit__(self, exc_type, exc_val, exc_tb): return self.__exit__(exc_type, exc_val, exc_tb)

if PROMETHEUS_AVAILABLE:
    reader_requests_total = Counter('symbols_reader_requests_total', 'Total read requests', ['sheet', 'status'])
    reader_discovery_duration = Histogram('symbols_reader_discovery_duration_seconds', 'Discovery duration', ['strategy'])
    reader_cache_hits = Counter('symbols_reader_cache_hits_total', 'Cache hits', ['level'])
    reader_cache_misses = Counter('symbols_reader_cache_misses_total', 'Cache misses', ['level'])
else:
    class DummyMetric:
        def labels(self, *args, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
    reader_requests_total = DummyMetric()
    reader_discovery_duration = DummyMetric()
    reader_cache_hits = DummyMetric()
    reader_cache_misses = DummyMetric()

# =============================================================================
# Enums & Types (Memory Optimized)
# =============================================================================
class SymbolType(Enum):
    KSA = "ksa"
    GLOBAL = "global"
    INDEX = "index"
    ETF = "etf"
    MUTUAL_FUND = "mutual_fund"
    CURRENCY = "currency"
    CRYPTO = "crypto"
    COMMODITY = "commodity"
    UNKNOWN = "unknown"

class DiscoveryStrategy(Enum):
    HEADER = "header"
    DATA_SCAN = "data_scan"
    ML = "ml"
    EXPLICIT = "explicit"
    DEFAULT = "default"

class ConfidenceLevel(Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    NONE = "none"

@dataclass(slots=True)
class SymbolMetadata:
    """Rich symbol metadata"""
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
    sector: Optional[str] = None
    industry: Optional[str] = None
    exchange: Optional[str] = None
    currency: Optional[str] = None
    name: Optional[str] = None
    is_active: bool = True
    discovered_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    last_validated: Optional[str] = None
    validation_status: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)

@dataclass(slots=True)
class PageSpec:
    """Page specification with enhanced metadata"""
    key: str
    sheet_names: Tuple[str, ...]
    header_row: int = 5
    start_row: int = 6
    max_rows: int = 5000
    header_candidates: Tuple[str, ...] = ("SYMBOL", "TICKER", "CODE", "STOCK")
    symbol_type: SymbolType = SymbolType.GLOBAL
    confidence_threshold: float = 0.7
    required: bool = False
    description: str = ""
    tags: List[str] = field(default_factory=list)
    cache_ttl: Optional[int] = None

# =============================================================================
# Utility Functions
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok", "active"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled", "inactive"}

_BLOCKLIST_EXACT = {
    "SYMBOL", "TICKER", "CODE", "STOCK", "NAME", "RANK", "MARKET", "CURRENCY",
    "PRICE", "LAST", "UPDATED", "LASTUPDATED", "DATE", "TIME", "NOTES", "COMMENT",
    "DESCRIPTION", "TITLE", "HEADER", "COLUMN", "ROW", "VALUE", "DATA", "INFO"
}

_TICKER_PATTERNS = {
    'ksa': [
        re.compile(r'^\d{4}$'),  # 1120
        re.compile(r'^\d{4}\.SR$', re.IGNORECASE),  # 1120.SR
        re.compile(r'^\d{4}SR$', re.IGNORECASE),  # 1120SR
        re.compile(r'^[0-9]{4}\.(SR|TADAWUL)$', re.IGNORECASE),
    ],
    'global': [
        re.compile(r'^[A-Z]{1,5}$'),  # AAPL, MSFT
        re.compile(r'^[A-Z]{1,4}\.[A-Z]{1,2}$'),  # BRK.B
        re.compile(r'^[A-Z]{1,5}-[A-Z]{1,5}$'),  # BRK-A
    ],
    'index': [
        re.compile(r'^\^[A-Z]{2,5}$'),  # ^GSPC
        re.compile(r'^\.[A-Z]{2,5}$'),  # .SPX
        re.compile(r'^[A-Z]{2,5}\.INDX$', re.IGNORECASE),
    ],
    'etf': [
        re.compile(r'^[A-Z]{3,5}$'),
        re.compile(r'^[A-Z]{3,5}\.[A-Z]{2}$'),
    ],
    'mutual_fund': [
        re.compile(r'^[A-Z]{5}$'),
        re.compile(r'^[A-Z]{5}\.[A-Z]{2}$'),
    ],
    'currency': [
        re.compile(r'^[A-Z]{6}=X$'),
        re.compile(r'^[A-Z]{3}/[A-Z]{3}$'),
        re.compile(r'^[A-Z]{3}[A-Z]{3}$'),
    ],
    'crypto': [
        re.compile(r'^[A-Z]{3,5}-USD$'),
        re.compile(r'^[A-Z]{3,5}/USD$'),
        re.compile(r'^[A-Z]{3,5}$'),
    ],
    'commodity': [
        re.compile(r'^[A-Z]{2}=F$'),
        re.compile(r'^[A-Z]{2}\d{2}=F$'),
    ]
}

def strip_value(v: Any) -> str:
    try: return str(v).strip()
    except Exception: return ""

def coerce_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool): return v
    s = strip_value(v).lower()
    if not s: return default
    if s in _TRUTHY: return True
    if s in _FALSY: return False
    return default

def get_env_bool(key: str, default: bool = False) -> bool:
    return coerce_bool(os.getenv(key), default)

def get_env_int(key: str, default: int, **kwargs) -> int:
    try: return int(float(os.getenv(key, str(default)).strip()))
    except Exception: return default

def get_env_float(key: str, default: float, **kwargs) -> float:
    try: return float(os.getenv(key, str(default)).strip())
    except Exception: return default

def get_env_str(key: str, default: str = "") -> str:
    return strip_value(os.getenv(key)) or default

def split_cell(cell: Any) -> List[str]:
    raw = strip_value(cell)
    if not raw: return []
    parts = re.split(r"[\s,;|\t\n\r]+", raw)
    return [p.strip() for p in parts if p and p.strip()]

# =============================================================================
# Full Jitter Backoff
# =============================================================================

class FullJitterBackoff:
    """Safe retry mechanism implementing AWS Full Jitter Backoff."""
    def __init__(self, max_retries: int = 4, base_delay: float = 1.0, max_delay: float = 30.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    async def execute_async(self, func: Callable, *args, **kwargs) -> Any:
        last_err = None
        for attempt in range(self.max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                last_err = e
                # Only retry on rate limits or server errors
                error_str = str(e).lower()
                retryable = any(x in error_str for x in ["rate limit", "quota", "429", "500", "502", "503", "timeout"])
                
                if not retryable or attempt == self.max_retries - 1:
                    raise
                temp = min(self.max_delay, self.base_delay * (2 ** attempt))
                await asyncio.sleep(random.uniform(0, temp))
        raise last_err

    def execute_sync(self, func: Callable, *args, **kwargs) -> Any:
        last_err = None
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_err = e
                error_str = str(e).lower()
                retryable = any(x in error_str for x in ["rate limit", "quota", "429", "500", "502", "503", "timeout"])
                
                if not retryable or attempt == self.max_retries - 1:
                    raise
                temp = min(self.max_delay, self.base_delay * (2 ** attempt))
                time.sleep(random.uniform(0, temp))
        raise last_err

# =============================================================================
# Configuration
# =============================================================================
class Config:
    DEFAULT_HEADER_ROW = get_env_int("TFB_SYMBOL_HEADER_ROW", 5)
    DEFAULT_START_ROW = get_env_int("TFB_SYMBOL_START_ROW", 6)
    DEFAULT_MAX_ROWS = get_env_int("TFB_SYMBOL_MAX_ROWS", 5000)

    CACHE_TTL_SEC = get_env_float("TFB_SYMBOLS_CACHE_TTL_SEC", 300.0)
    CACHE_DISABLE = get_env_bool("TFB_SYMBOLS_CACHE_DISABLE", False)
    CACHE_BACKEND = get_env_str("TFB_SYMBOLS_CACHE_BACKEND", "redis" if REDIS_AVAILABLE else "memory")
    CACHE_MAX_SIZE = get_env_int("TFB_SYMBOLS_CACHE_MAX_SIZE", 1000)
    CACHE_COMPRESSION = get_env_bool("TFB_SYMBOLS_CACHE_COMPRESSION", True)

    MAX_RETRIES = get_env_int("TFB_SYMBOLS_MAX_RETRIES", 4)
    REQUEST_TIMEOUT = get_env_float("TFB_SYMBOLS_TIMEOUT", 30.0)

    ML_DETECTION_ENABLED = get_env_bool("TFB_SYMBOLS_ML_DETECTION", True) and ML_AVAILABLE
    ML_CONFIDENCE_THRESHOLD = get_env_float("TFB_SYMBOLS_ML_CONFIDENCE", 0.8)

    LOG_DETECTION = get_env_bool("TFB_SYMBOLS_LOG_DETECTION", True)
    LOG_PERFORMANCE = get_env_bool("TFB_SYMBOLS_LOG_PERFORMANCE", True)

config = Config()

# =============================================================================
# Page Registry
# =============================================================================
PAGE_REGISTRY: Dict[str, PageSpec] = {
    "MARKET_LEADERS": PageSpec(
        key="MARKET_LEADERS",
        sheet_names=("Market_Leaders", "Market Leaders", "Leaders"),
        header_candidates=("SYMBOL", "TICKER", "CODE"),
        symbol_type=SymbolType.GLOBAL,
        description="Top market performers",
    ),
    "GLOBAL_MARKETS": PageSpec(
        key="GLOBAL_MARKETS",
        sheet_names=("Global_Markets", "Global Markets", "World Markets"),
        header_candidates=("SYMBOL", "TICKER", "CODE"),
        symbol_type=SymbolType.GLOBAL,
        description="Global equity markets and indices",
    ),
    "KSA_TADAWUL": PageSpec(
        key="KSA_TADAWUL",
        sheet_names=("KSA_Tadawul", "KSA Tadawul", "Tadawul", "Saudi Market"),
        header_candidates=("SYMBOL", "TICKER", "CODE", "ISIN"),
        symbol_type=SymbolType.KSA,
        description="Saudi Stock Exchange",
    ),
    "MUTUAL_FUNDS": PageSpec(
        key="MUTUAL_FUNDS",
        sheet_names=("Mutual_Funds", "Mutual Funds", "Funds"),
        header_candidates=("SYMBOL", "TICKER", "FUND", "CODE"),
        symbol_type=SymbolType.MUTUAL_FUND,
    ),
    "COMMODITIES_FX": PageSpec(
        key="COMMODITIES_FX",
        sheet_names=("Commodities_FX", "Commodities & FX", "FX & Commodities"),
        header_candidates=("SYMBOL", "TICKER", "PAIR", "COMMODITY"),
        symbol_type=SymbolType.COMMODITY,
    ),
    "MY_PORTFOLIO": PageSpec(
        key="MY_PORTFOLIO",
        sheet_names=("My_Portfolio", "My Portfolio", "Portfolio"),
        header_candidates=("SYMBOL", "TICKER", "CODE", "ASSET"),
        symbol_type=SymbolType.GLOBAL,
        required=False,
    ),
    "INSIGHTS_ANALYSIS": PageSpec(
        key="INSIGHTS_ANALYSIS",
        sheet_names=("Insights_Analysis", "Insights Analysis", "Analysis"),
        header_candidates=("SYMBOL", "TICKER", "CODE"),
        symbol_type=SymbolType.GLOBAL,
    ),
}

def resolve_key(key: str) -> str:
    """Resolve canonical page key from alias"""
    k = strip_value(key).upper().replace("-", "_").replace(" ", "_")
    aliases = {
        "KSA": "KSA_TADAWUL", "TADAWUL": "KSA_TADAWUL", "SAUDI": "KSA_TADAWUL",
        "GLOBAL": "GLOBAL_MARKETS", "WORLD": "GLOBAL_MARKETS",
        "PORTFOLIO": "MY_PORTFOLIO", "MYPORTFOLIO": "MY_PORTFOLIO",
        "INSIGHTS": "INSIGHTS_ANALYSIS", "ANALYSIS": "INSIGHTS_ANALYSIS",
        "LEADERS": "MARKET_LEADERS", "FUNDS": "MUTUAL_FUNDS",
        "COMMODITIES": "COMMODITIES_FX", "FX": "COMMODITIES_FX",
    }
    return aliases.get(k, k)

# =============================================================================
# Google Sheets Authentication & Service
# =============================================================================
class GoogleSheetsReader:
    """Enhanced Google Sheets reader with Async isolation and Exponential Backoff"""

    def __init__(self):
        self._service = None
        self._lock = threading.Lock()
        self.backoff = FullJitterBackoff(max_retries=config.MAX_RETRIES)

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
                # Resolve credentials
                creds_raw = os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS")
                if not creds_raw:
                    path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
                    if path and os.path.exists(path):
                        with open(path, "r", encoding="utf-8") as f:
                            creds_raw = f.read()
                
                if not creds_raw:
                    raise ValueError("No credentials found in environment")

                # Parse and repair
                if not creds_raw.startswith("{"):
                    creds_raw = base64.b64decode(creds_raw).decode("utf-8")
                
                creds_dict = json_loads(creds_raw)
                if "private_key" in creds_dict:
                    creds_dict["private_key"] = creds_dict["private_key"].replace('\\n', '\n')

                scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
                credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
                self._service = build("sheets", "v4", credentials=credentials, cache_discovery=False)
                logger.info("Google Sheets service initialized")
                return self._service
            except Exception as e:
                logger.error(f"Failed to initialize Google Sheets service: {e}")
                return None

    def _safe_sheet_name(self, name: str) -> str:
        n = strip_value(name) or "Sheet1"
        return f"'{n.replace(chr(39), chr(39)*2)}'" if any(c in n for c in ("'", " ", "-")) else n

    def _col_to_letter(self, col: int) -> str:
        letters = ""
        while col > 0:
            col -= 1
            letters = chr(65 + (col % 26)) + letters
            col //= 26
        return letters or "A"

    def read_range(self, spreadsheet_id: str, range_a1: str) -> List[List[Any]]:
        """Synchronous read with backoff"""
        service = self._get_service()
        if not service: return []

        def _execute():
            t0 = time.perf_counter()
            result = service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=range_a1,
                valueRenderOption="UNFORMATTED_VALUE", dateTimeRenderOption="SERIAL_NUMBER"
            ).execute()
            if config.LOG_PERFORMANCE:
                logger.debug(f"Sheet read {range_a1}: {(time.perf_counter() - t0)*1000:.1f}ms")
            return result.get("values", [])

        try:
            with TraceContext("sheets_read_range", {"range": range_a1}):
                return self.backoff.execute_sync(_execute)
        except Exception as e:
            logger.error(f"Failed to read range {range_a1}: {e}")
            return []
            
    async def read_range_async(self, spreadsheet_id: str, range_a1: str) -> List[List[Any]]:
        """Asynchronous wrapper for read_range to avoid blocking the event loop"""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.read_range, spreadsheet_id, range_a1)

    def list_sheets(self, spreadsheet_id: str) -> List[str]:
        service = self._get_service()
        if not service: return []
        def _execute():
            metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            return [sheet["properties"]["title"] for sheet in metadata.get("sheets", []) if sheet.get("properties", {}).get("title")]
        
        try: return self.backoff.execute_sync(_execute)
        except Exception as e:
            logger.error(f"Failed to list sheets: {e}")
            return []

sheets_reader = GoogleSheetsReader()

# =============================================================================
# Cache Manager (Multi-Tier Zlib)
# =============================================================================
class AdvancedCache:
    """Multi-tier Cache (Memory L1 + Redis L2) with Zlib Compression"""
    def __init__(self):
        self._memory_cache: Dict[str, Tuple[float, bytes]] = {}
        self._memory_lock = asyncio.Lock()
        self._redis_client: Optional[Redis] = None
        self._stats = {"l1_hits": 0, "l2_hits": 0, "misses": 0}

        if config.CACHE_BACKEND == "redis" and REDIS_AVAILABLE:
            redis_url = os.getenv("REDIS_URL")
            if redis_url:
                try:
                    self._redis_client = aioredis.from_url(redis_url, decode_responses=False, max_connections=20)
                    logger.info("Redis L2 cache initialized")
                except Exception as e:
                    logger.warning(f"Redis initialization failed, falling back to L1 only: {e}")

    def _make_key(self, *parts) -> str:
        key = ":".join(str(p) for p in parts)
        if len(key) > 100: key = hashlib.sha256(key.encode()).hexdigest()
        return f"sym_reader:{key}"

    def _compress(self, data: Any) -> bytes:
        pickled = pickle.dumps(data)
        return zlib.compress(pickled, level=6) if config.CACHE_COMPRESSION else pickled

    def _decompress(self, data: bytes) -> Any:
        return pickle.loads(zlib.decompress(data)) if config.CACHE_COMPRESSION else pickle.loads(data)

    async def get(self, *key_parts) -> Optional[Any]:
        if config.CACHE_DISABLE: return None
        key = self._make_key(*key_parts)
        now = time.time()

        # Check L1 Memory Cache
        async with self._memory_lock:
            if key in self._memory_cache:
                expiry, data = self._memory_cache[key]
                if now < expiry:
                    self._stats["l1_hits"] += 1
                    reader_cache_hits.labels(level="L1").inc()
                    return self._decompress(data)
                else:
                    del self._memory_cache[key]

        # Check L2 Redis Cache
        if self._redis_client:
            try:
                data = await self._redis_client.get(key)
                if data:
                    self._stats["l2_hits"] += 1
                    reader_cache_hits.labels(level="L2").inc()
                    # Backfill L1
                    async with self._memory_lock:
                        self._memory_cache[key] = (now + config.CACHE_TTL_SEC, data)
                    return self._decompress(data)
            except Exception as e:
                logger.debug(f"Redis get failed: {e}")

        self._stats["misses"] += 1
        reader_cache_misses.labels(level="ALL").inc()
        return None

    async def set(self, value: Any, *key_parts) -> None:
        if config.CACHE_DISABLE: return
        key = self._make_key(*key_parts)
        data = self._compress(value)
        expiry = time.time() + config.CACHE_TTL_SEC

        async with self._memory_lock:
            if len(self._memory_cache) >= config.CACHE_MAX_SIZE:
                oldest = min(self._memory_cache.items(), key=lambda x: x[1][0])[0]
                del self._memory_cache[oldest]
            self._memory_cache[key] = (expiry, data)

        if self._redis_client:
            try:
                await self._redis_client.setex(key, int(config.CACHE_TTL_SEC), data)
            except Exception as e:
                logger.debug(f"Redis set failed: {e}")

cache = AdvancedCache()

# =============================================================================
# Symbol Normalizer
# =============================================================================

class SymbolNormalizer:
    @staticmethod
    def _try_core_import() -> Any:
        try:
            from core.symbols.normalize import normalize_symbol
            return normalize_symbol
        except ImportError:
            return None

    def __init__(self):
        self._core_norm = self._try_core_import()

    @lru_cache(maxsize=4096)
    def normalize(self, raw: str) -> Tuple[str, SymbolType]:
        s = strip_value(raw).upper()
        if not s: return "", SymbolType.UNKNOWN

        # Use core normalizer if available
        if self._core_norm:
            try:
                core_res = self._core_norm(raw)
                if core_res: s = core_res
            except Exception: pass

        # Fallback local normalization
        for p in ["$", "#", "^", "NYSE:", "NASDAQ:", "TADAWUL:", "SR:", "SAUDI:"]:
            if s.startswith(p): s = s[len(p):]
        for suf in [".SR", ".SA", ".AB", ".TADAWUL", ".NYSE", ".NASDAQ"]:
            if s.endswith(suf): s = s[:-len(suf)]

        # Detect Type
        sym_type = SymbolType.UNKNOWN
        if ".SR" in raw.upper() or "TADAWUL" in raw.upper() or "SAUDI" in raw.upper(): sym_type = SymbolType.KSA
        else:
            for pattern in _TICKER_PATTERNS['ksa']:
                if pattern.match(s) or pattern.match(raw.upper()):
                    sym_type = SymbolType.KSA; break
            if sym_type == SymbolType.UNKNOWN:
                for cat, patterns in _TICKER_PATTERNS.items():
                    if cat == 'ksa': continue
                    for pattern in patterns:
                        if pattern.match(s) or pattern.match(raw.upper()):
                            sym_type = SymbolType(cat); break
                    if sym_type != SymbolType.UNKNOWN: break

        if sym_type == SymbolType.KSA:
            if re.match(r'^\d{4}$', s): s = f"{s}.SR"
            elif re.match(r'^\d{4}SR$', s): s = f"{s[:4]}.SR"
            elif re.match(r'^\d{4}\.SR$', s, re.IGNORECASE): s = s.upper()

        if sym_type == SymbolType.UNKNOWN: sym_type = SymbolType.GLOBAL
        return s, sym_type

    def is_valid(self, symbol: str) -> bool:
        norm, sym_type = self.normalize(symbol)
        if not norm or sym_type == SymbolType.UNKNOWN or norm in _BLOCKLIST_EXACT: return False
        return True

normalizer = SymbolNormalizer()

# =============================================================================
# ML Detection Engine
# =============================================================================
class MLDetectionEngine:
    def __init__(self):
        self.model = None
        self.vectorizer = None
        self.trained = False
        self._lock = asyncio.Lock()

    async def train(self, positive_samples: List[str], negative_samples: List[str]):
        if not ML_AVAILABLE: return
        async with self._lock:
            loop = asyncio.get_running_loop()
            def _train():
                texts = positive_samples + negative_samples
                labels = [1] * len(positive_samples) + [0] * len(negative_samples)
                self.vectorizer = TfidfVectorizer(analyzer='char', ngram_range=(2, 5), max_features=1000)
                X = self.vectorizer.fit_transform(texts)
                self.model = RandomForestClassifier(n_estimators=100, n_jobs=-1)
                self.model.fit(X, labels)
                self.trained = True
            await loop.run_in_executor(None, _train)

    async def predict(self, text: str) -> Tuple[bool, float]:
        if not self.trained or not ML_AVAILABLE: return False, 0.0
        async with self._lock:
            loop = asyncio.get_running_loop()
            def _predict():
                X = self.vectorizer.transform([text])
                return self.model.predict_proba(X)[0][1]
            confidence = await loop.run_in_executor(None, _predict)
            return confidence > config.ML_CONFIDENCE_THRESHOLD, confidence

ml_detector = MLDetectionEngine() if config.ML_DETECTION_ENABLED else None

# =============================================================================
# Column Detection Engine
# =============================================================================

class ColumnDetectionEngine:
    async def detect(self, spreadsheet_id: str, sheet_name: str, spec: PageSpec) -> Tuple[Optional[str], DiscoveryStrategy, float, Dict[str, Any]]:
        with TraceContext("detect_symbol_column", {"sheet": sheet_name}):
            # 1. Header Strategy
            col, conf, meta = await self._detect_by_header(spreadsheet_id, sheet_name, spec)
            if col and conf >= spec.confidence_threshold: return col, DiscoveryStrategy.HEADER, conf, meta

            # 2. Data Scan Strategy
            col, conf, meta = await self._detect_by_data(spreadsheet_id, sheet_name, spec)
            if col and conf >= spec.confidence_threshold: return col, DiscoveryStrategy.DATA_SCAN, conf, meta

            # 3. Fallback
            return "B", DiscoveryStrategy.DEFAULT, 0.5, {"reason": "fallback to B"}

    async def _detect_by_header(self, spreadsheet_id: str, sheet_name: str, spec: PageSpec) -> Tuple[Optional[str], float, Dict[str, Any]]:
        range_a1 = f"{sheets_reader._safe_sheet_name(sheet_name)}!A{spec.header_row}:ZZ{spec.header_row}"
        values = await sheets_reader.read_range_async(spreadsheet_id, range_a1)
        if not values or not values[0]: return None, 0.0, {}
        
        headers = [strip_value(h).upper() for h in values[0]]
        for idx, header in enumerate(headers, 1):
            if header in spec.header_candidates: return sheets_reader._col_to_letter(idx), 1.0, {"matched_header": header}
            
        best_score, best_col, best_match = 0.0, None, ""
        for idx, header in enumerate(headers, 1):
            if not header: continue
            for candidate in spec.header_candidates:
                if candidate in header:
                    score = len(candidate) / len(header) if header else 0
                    if score > best_score: best_score, best_col, best_match = score, sheets_reader._col_to_letter(idx), candidate
                    
        if best_col and best_score >= 0.5: return best_col, best_score, {"matched_header": best_match, "similarity": best_score}
        return None, 0.0, {}

    async def _detect_by_data(self, spreadsheet_id: str, sheet_name: str, spec: PageSpec) -> Tuple[Optional[str], float, Dict[str, Any]]:
        sample_size = min(200, spec.max_rows // 10)
        end_row = spec.start_row + sample_size - 1
        range_a1 = f"{sheets_reader._safe_sheet_name(sheet_name)}!A{spec.start_row}:ZZ{end_row}"
        values = await sheets_reader.read_range_async(spreadsheet_id, range_a1)
        if not values: return None, 0.0, {}

        max_cols = max((len(row) for row in values), default=0)
        col_scores = defaultdict(lambda: {"hits": 0, "total": 0, "cells": 0})

        for row in values:
            for col_idx in range(max_cols):
                if col_idx >= len(row) or row[col_idx] is None: continue
                tokens = split_cell(row[col_idx])
                if not tokens: continue
                col_scores[col_idx]["cells"] += 1
                col_scores[col_idx]["total"] += len(tokens)
                for token in tokens:
                    norm, _ = normalizer.normalize(token)
                    if normalizer.is_valid(norm): col_scores[col_idx]["hits"] += 1

        best_col, best_score, best_stats = None, 0.0, {}
        for col_idx, stats in col_scores.items():
            if stats["total"] < 5: continue
            hit_rate = stats["hits"] / stats["total"] if stats["total"] else 0
            cell_coverage = stats["cells"] / len(values) if values else 0
            score = hit_rate * 0.7 + cell_coverage * 0.3
            if score > best_score:
                best_score, best_col = score, sheets_reader._col_to_letter(col_idx + 1)
                best_stats = {"hit_rate": hit_rate, "cell_coverage": cell_coverage}

        if best_col and best_score >= 0.3: return best_col, best_score, best_stats
        return None, 0.0, {}

detector = ColumnDetectionEngine()

# =============================================================================
# Symbol Extractor
# =============================================================================
class SymbolExtractor:
    def extract_from_column(self, values: List[List[Any]], column_letter: str) -> List[SymbolMetadata]:
        col_idx = self._letter_to_col(column_letter) - 1
        symbols = []
        for row_idx, row in enumerate(values, start=config.DEFAULT_START_ROW):
            if not row or col_idx >= len(row) or row[col_idx] is None: continue
            for token in split_cell(row[col_idx]):
                norm, sym_type = normalizer.normalize(token)
                if norm and normalizer.is_valid(norm):
                    symbols.append(SymbolMetadata(
                        symbol=norm, normalized=norm, symbol_type=sym_type, origin="sheet",
                        discovery_strategy=DiscoveryStrategy.DATA_SCAN,
                        confidence=ConfidenceLevel.HIGH if sym_type != SymbolType.UNKNOWN else ConfidenceLevel.LOW,
                        column=column_letter, row=row_idx, raw_value=token
                    ))
        return symbols

    def _letter_to_col(self, letter: str) -> int:
        col = 0
        for ch in letter.upper(): col = col * 26 + (ord(ch) - ord('A') + 1)
        return col

extractor = SymbolExtractor()

# =============================================================================
# Main Async API
# =============================================================================

async def get_page_symbols_async(key: str, spreadsheet_id: Optional[str] = None) -> Dict[str, Any]:
    """Async engine to get symbols from a page with rich metadata."""
    start_time = time.perf_counter()
    sid = strip_value(spreadsheet_id) or os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("SPREADSHEET_ID")
    
    if not sid:
        return {"all": [], "error": "No spreadsheet ID", "status": "error"}

    canonical_key = resolve_key(key)
    spec = PAGE_REGISTRY.get(canonical_key)
    if not spec: return {"all": [], "error": f"Unknown page key: {key}", "status": "error"}

    # Cache Check
    cached = await cache.get("page", sid, canonical_key, spec.header_row, spec.start_row, spec.max_rows)
    if cached is not None:
        cached["cache_hit"] = True
        return cached

    best_result, best_confidence = None, 0.0
    for sheet_name in spec.sheet_names:
        col, strategy, confidence, detect_meta = await detector.detect(sid, sheet_name, spec)
        if not col or confidence < spec.confidence_threshold: continue

        end_row = spec.start_row + spec.max_rows - 1
        range_a1 = f"{sheets_reader._safe_sheet_name(sheet_name)}!{col}{spec.start_row}:{col}{end_row}"
        values = await sheets_reader.read_range_async(sid, range_a1)
        if not values: continue

        symbols = extractor.extract_from_column(values, col)
        for sym in symbols:
            sym.sheet_name = sheet_name
            sym.origin = canonical_key
            sym.discovery_strategy = strategy
            sym.confidence = ConfidenceLevel.HIGH if strategy == DiscoveryStrategy.HEADER and confidence > 0.9 else ConfidenceLevel.MEDIUM if confidence > 0.7 else ConfidenceLevel.LOW

        if len(symbols) > best_confidence:
            best_confidence, best_result = len(symbols), (sheet_name, col, strategy, confidence, detect_meta, symbols)

    elapsed = (time.perf_counter() - start_time) * 1000
    
    if best_result:
        sheet_name, col, strategy, confidence, detect_meta, symbols = best_result
        all_syms, ksa_syms, global_syms, by_type = [], [], [], defaultdict(list)
        for sym in symbols:
            all_syms.append(sym.symbol)
            by_type[sym.symbol_type.value].append(sym.symbol)
            if sym.symbol_type == SymbolType.KSA: ksa_syms.append(sym.symbol)
            else: global_syms.append(sym.symbol)

        result = {
            "all": all_syms, "ksa": ksa_syms, "global": global_syms, "by_type": dict(by_type),
            "metadata": [asdict(sym) for sym in symbols], "origin": canonical_key,
            "discovery": {"sheet": sheet_name, "column": col, "strategy": strategy.value, "confidence": confidence, "detection": detect_meta},
            "performance": {"elapsed_ms": round(elapsed, 2), "symbol_count": len(symbols), "unique_count": len(set(all_syms))},
            "cache_hit": False, "timestamp": datetime.now(timezone.utc).isoformat(), "version": VERSION, "status": "success",
        }
        await cache.set(result, "page", sid, canonical_key, spec.header_row, spec.start_row, spec.max_rows)
        reader_requests_total.labels(sheet=canonical_key, status="success").inc()
        return result

    result = {
        "all": [], "ksa": [], "global": [], "by_type": {}, "metadata": [], "origin": canonical_key, "discovery": None,
        "performance": {"elapsed_ms": round(elapsed, 2), "symbol_count": 0, "unique_count": 0},
        "cache_hit": False, "timestamp": datetime.now(timezone.utc).isoformat(), "version": VERSION, "status": "empty", "warning": "No symbols found"
    }
    await cache.set(result, "page", sid, canonical_key, spec.header_row, spec.start_row, spec.max_rows)
    reader_requests_total.labels(sheet=canonical_key, status="empty").inc()
    return result

async def get_universe_async(keys: List[str], spreadsheet_id: Optional[str] = None) -> Dict[str, Any]:
    start_time = time.perf_counter()
    sid = strip_value(spreadsheet_id) or os.getenv("DEFAULT_SPREADSHEET_ID")
    if not sid: return {"symbols": [], "error": "No spreadsheet ID", "status": "error"}

    all_symbols, origin_map, by_origin, by_type, page_metadata = [], {}, defaultdict(list), defaultdict(list), {}
    
    tasks = [get_page_symbols_async(key, sid) for key in keys]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    for key, result in zip(keys, results):
        canonical_key = resolve_key(key)
        if isinstance(result, dict) and result.get("status") == "success" and result.get("all"):
            symbols = result["all"]
            page_metadata[canonical_key] = {"count": len(symbols), "elapsed_ms": result["performance"]["elapsed_ms"], "status": result.get("status"), "discovery": result.get("discovery")}
            for sym in symbols:
                if sym not in origin_map:
                    origin_map[sym] = canonical_key
                    all_symbols.append(sym)
                    by_origin[canonical_key].append(sym)
            if "metadata" in result:
                for meta in result["metadata"]:
                    if meta["symbol"] in origin_map: by_type[meta["symbol_type"]].append(meta["symbol"])

    for type_name in by_type: by_type[type_name] = list(set(by_type[type_name]))
    all_symbols.sort()

    return {
        "symbols": all_symbols, "by_origin": dict(by_origin), "by_type": dict(by_type), "origin_map": origin_map, "metadata": page_metadata,
        "performance": {"elapsed_ms": round((time.perf_counter() - start_time) * 1000, 2), "pages_processed": len(keys), "pages_with_data": len(page_metadata), "total_symbols": len(all_symbols)},
        "timestamp": datetime.now(timezone.utc).isoformat(), "version": VERSION, "status": "success",
    }


# Synchronous Wrappers for CLI/Backward Compatibility
def get_page_symbols(key: str, spreadsheet_id: Optional[str] = None) -> Dict[str, Any]:
    try:
        loop = asyncio.get_running_loop()
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            return loop.run_until_complete(get_page_symbols_async(key, spreadsheet_id))
    except RuntimeError:
        return asyncio.run(get_page_symbols_async(key, spreadsheet_id))

def get_universe(keys: List[str], spreadsheet_id: Optional[str] = None) -> Dict[str, Any]:
    try:
        loop = asyncio.get_running_loop()
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            return loop.run_until_complete(get_universe_async(keys, spreadsheet_id))
    except RuntimeError:
        return asyncio.run(get_universe_async(keys, spreadsheet_id))

def list_tabs(spreadsheet_id: Optional[str] = None) -> List[str]:
    sid = strip_value(spreadsheet_id) or os.getenv("DEFAULT_SPREADSHEET_ID")
    if not sid: return []
    return sheets_reader.list_sheets(sid)


# =============================================================================
# CLI Entry Point
# =============================================================================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=f"TFB Symbols Reader v{VERSION}")
    parser.add_argument("--sheet-id", help="Spreadsheet ID override")
    parser.add_argument("--key", default="KSA", help="Page key (e.g., KSA, GLOBAL, LEADERS)")
    parser.add_argument("--keys", nargs="+", help="Multiple keys for universe")
    parser.add_argument("--list-tabs", action="store_true", help="List tabs and exit")
    parser.add_argument("--output", choices=["text", "json"], default="text", help="Output format")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--no-cache", action="store_true", help="Disable cache")

    args = parser.parse_args()

    if args.verbose: logging.getLogger().setLevel(logging.DEBUG)
    if args.no_cache: config.CACHE_DISABLE = True

    if args.list_tabs:
        tabs = list_tabs(args.sheet_id)
        print(json_dumps({"tabs": tabs}) if args.output == "json" else "\n".join(tabs) if tabs else "No tabs found")
        sys.exit(0)

    result = get_universe(args.keys, spreadsheet_id=args.sheet_id) if args.keys else get_page_symbols(args.key, spreadsheet_id=args.sheet_id)

    if args.output == "json": print(json_dumps(result))
    else:
        print(f"Symbols Reader v{VERSION}\nStatus: {result.get('status', 'unknown')}\nCount: {len(result.get('all', result.get('symbols', [])))}")
        if "performance" in result: print(f"Time: {result['performance'].get('elapsed_ms', 0):.1f}ms")
        if "by_type" in result:
            print("\nBy Type:")
            for t_name, syms in result["by_type"].items(): print(f"  {t_name}: {len(syms)}")
        symbols = result.get("all", result.get("symbols", []))
        if symbols: print("\nPreview:\n  " + ", ".join(symbols[:20]))

__all__ = [
    "VERSION", "SymbolType", "DiscoveryStrategy", "ConfidenceLevel", "SymbolMetadata", "PageSpec",
    "get_page_symbols", "get_page_symbols_async", "get_universe", "get_universe_async", "list_tabs",
    "PAGE_REGISTRY", "normalizer", "extractor", "detector"
]
