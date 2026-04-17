#!/usr/bin/env python3
"""
integrations/google_sheets_service.py
===========================================================
GOOGLE SHEETS SERVICE FOR TADAWUL FAST BRIDGE -- v5.5.3 (PHASE 2 ALIGNED)

Phase 2 Alignment Goals
- ✅ Ensure canonical tabs exist (Top_10_Investments + Data_Dictionary + all schema pages)
- ✅ Ensure headers EXACTLY match schema_registry (always full 60+ columns; no drift)
- ✅ Insights_Analysis: seed Advisor Criteria block at top (A1:D4) if empty + table headers at A5
- ✅ NEVER create/write forbidden pages (KSA_Tadawul, Advisor_Criteria)
- ✅ Keep no-data-loss preserve columns logic (only fill blanks from existing sheet)
- ✅ Startup-safe: no network work at import-time (all Sheets calls occur only inside refresh calls)
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
import random
import re
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache, wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

# ---------------------------------------------------------------------------
# Optional high-performance libs (safe)
# ---------------------------------------------------------------------------
try:
    import httpx  # type: ignore

    _HAS_HTTPX = True
except Exception:
    httpx = None  # type: ignore
    _HAS_HTTPX = False

try:
    import orjson  # type: ignore

    def json_dumps_str(v: Any, *, default: Optional[Callable] = None) -> str:
        return orjson.dumps(v, default=default or str).decode("utf-8")

    def json_dumps_bytes(v: Any, *, default: Optional[Callable] = None) -> bytes:
        return orjson.dumps(v, default=default or str)

    def json_loads(v: Any) -> Any:
        if isinstance(v, (bytes, bytearray)):
            return orjson.loads(v)
        if isinstance(v, str):
            return orjson.loads(v.encode("utf-8"))
        return orjson.loads(str(v).encode("utf-8"))

    _HAS_ORJSON = True
except Exception:
    import json  # type: ignore

    def json_dumps_str(v: Any, *, default: Optional[Callable] = None) -> str:
        return json.dumps(v, default=default or str, ensure_ascii=False)

    def json_dumps_bytes(v: Any, *, default: Optional[Callable] = None) -> bytes:
        return json.dumps(v, default=default or str, ensure_ascii=False).encode("utf-8")

    def json_loads(v: Any) -> Any:
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", errors="replace")
        return json.loads(v)

    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Monitoring & Tracing (Optional; safe fallback)
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Histogram  # type: ignore

    _PROMETHEUS_AVAILABLE = True
    sheets_operations_total = Counter("sheets_operations_total", "Total Sheets operations", ["operation", "status"])
    sheets_operation_duration = Histogram(
        "sheets_operation_duration_seconds", "Sheets operation duration", ["operation"]
    )
except Exception:
    _PROMETHEUS_AVAILABLE = False

    class DummyMetric:
        def labels(self, *args, **kwargs):
            return self

        def inc(self, *args, **kwargs):
            return None

        def observe(self, *args, **kwargs):
            return None

    sheets_operations_total = DummyMetric()
    sheets_operation_duration = DummyMetric()

try:
    from opentelemetry import trace as otel_trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore

    _OTEL_AVAILABLE = True
    _TRACER = otel_trace.get_tracer(__name__)
except Exception:
    otel_trace = None  # type: ignore
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    _OTEL_AVAILABLE = False
    _TRACER = None

_TRACING_ENABLED = (os.getenv("CORE_TRACING_ENABLED", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}


class TraceContext:
    """OpenTelemetry context manager (sync + async). Never raises."""

    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._cm = None
        self._span = None

    def __enter__(self):
        if not (_OTEL_AVAILABLE and _TRACING_ENABLED and _TRACER is not None):
            return self
        try:
            self._cm = _TRACER.start_as_current_span(self.name)
            self._span = self._cm.__enter__()
            if self._span is not None and self.attributes:
                for k, v in self.attributes.items():
                    try:
                        self._span.set_attribute(str(k), v)
                    except Exception:
                        pass
        except Exception:
            self._cm = None
            self._span = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self._span is not None and exc_val is not None:
                try:
                    if hasattr(self._span, "record_exception"):
                        self._span.record_exception(exc_val)
                except Exception:
                    pass
                try:
                    if Status is not None and StatusCode is not None and hasattr(self._span, "set_status"):
                        self._span.set_status(Status(StatusCode.ERROR, str(exc_val)))
                except Exception:
                    pass
        finally:
            if self._cm is not None:
                try:
                    return self._cm.__exit__(exc_type, exc_val, exc_tb)
                except Exception:
                    return False
        return False

    async def __aenter__(self):
        return self.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.__exit__(exc_type, exc_val, exc_tb)


# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
SERVICE_VERSION = "5.5.3"
MIN_CORE_VERSION = "5.0.0"

logger = logging.getLogger("integrations.google_sheets_service")
logger.addHandler(logging.NullHandler())

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}


def _strip(v: Any) -> str:
    try:
        return str(v).strip()
    except Exception:
        return ""


def coerce_bool(v: Any, default: bool) -> bool:
    s = _strip(v).lower()
    if not s:
        return default
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def coerce_int(v: Any, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        x = int(float(_strip(v)))
    except Exception:
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


def coerce_float(v: Any, default: float, *, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    try:
        x = float(_strip(v))
    except Exception:
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


def coerce_list(v: Any, default: Optional[List[str]] = None) -> List[str]:
    if default is None:
        default = []
    if v is None:
        return list(default)
    if isinstance(v, list):
        return [_strip(x) for x in v if _strip(x)]
    s = _strip(v)
    if not s:
        return list(default)
    if s.startswith("[") and s.endswith("]"):
        try:
            import json as _json
            parsed = _json.loads(s)
            if isinstance(parsed, list):
                return [_strip(x) for x in parsed if _strip(x)]
        except Exception:
            pass
    parts = re.split(r"[\s,;|]+", s)
    return [_strip(x) for x in parts if _strip(x)]


def normalize_list_lower(items: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for x in items or []:
        s = _strip(x).lower()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def parse_csv_env(name: str, default: str = "") -> List[str]:
    raw = _strip(os.getenv(name, "") or default)
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class DataQuality(str, Enum):
    EXCELLENT = "EXCELLENT"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    POOR = "POOR"
    STALE = "STALE"
    ERROR = "ERROR"
    MISSING = "MISSING"


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class TokenTransport(str, Enum):
    HEADER = "header"
    BEARER = "bearer"
    QUERY = "query"
    BODY = "body"  # reserved


class SafeModeLevel(str, Enum):
    OFF = "off"
    BASIC = "basic"
    STRICT = "strict"
    PARANOID = "paranoid"


# ---------------------------------------------------------------------------
# Core schema + page catalog integration (Phase 2) — schema_registry driven
# ---------------------------------------------------------------------------
try:
    from core.config import (  # type: ignore
        canonical_pages as core_canonical_pages,
        forbidden_pages as core_forbidden_pages,
        normalize_page as core_normalize_page,
        schema_headers as core_schema_headers,
    )
    from core.symbols.normalize import normalize_symbol as core_normalize_symbol  # type: ignore

    _HAS_CORE = True
except Exception:
    _HAS_CORE = False

    def core_canonical_pages() -> List[str]:
        return [
            "Market_Leaders",
            "Global_Markets",
            "Commodities_FX",
            "Mutual_Funds",
            "My_Portfolio",
            "Insights_Analysis",
            "Top_10_Investments",
            "Data_Dictionary",
        ]

    def core_forbidden_pages() -> Set[str]:
        return {"KSA_Tadawul", "Advisor_Criteria"}

    def core_normalize_page(page: str, *, allow_output_pages: bool = True) -> str:
        p = (page or "").strip()
        if not p:
            raise ValueError("Page name is empty")
        if p in core_forbidden_pages():
            raise ValueError(f"Forbidden/removed page: {p}")
        return p

    def core_schema_headers(name: str) -> List[str]:
        return []

    def core_normalize_symbol(sym: str) -> str:
        return (sym or "").strip().upper()


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class SheetsServiceConfig:
    # Google Sheets
    default_spreadsheet_id: str = ""
    sheets_api_retries: int = 4
    sheets_api_retry_base_sleep: float = 1.0
    sheets_api_timeout_sec: float = 60.0

    # Backend API
    backend_base_url: str = ""
    backend_timeout_sec: float = 120.0
    backend_retries: int = 3
    backend_retry_sleep: float = 1.0
    backend_max_symbols_per_call: int = 200
    backend_token_transport: Set[TokenTransport] = field(
        default_factory=lambda: {TokenTransport.HEADER, TokenTransport.BEARER}
    )
    backend_token_param_name: str = "token"
    backend_circuit_breaker_threshold: int = 5
    backend_circuit_breaker_timeout: int = 60

    # Write operations
    max_rows_per_write: int = 500
    use_batch_update: bool = True
    max_batch_ranges: int = 25
    clear_end_col: str = "ZZ"
    clear_end_row: int = 100000
    smart_clear: bool = True  # used for post-clear (tail + right-side header)

    # SAFE MODE
    safe_mode: SafeModeLevel = SafeModeLevel.STRICT
    block_on_empty_headers: bool = True
    block_on_empty_rows: bool = True
    block_on_data_mismatch: bool = True
    validate_row_count: bool = True
    require_symbol_header: bool = True

    # Caching
    cache_ttl_seconds: int = 300
    enable_header_cache: bool = True

    # httpx perf
    connection_pool_size: int = 20
    keep_alive_seconds: int = 30

    # Security
    verify_ssl: bool = True
    ssl_cert_path: Optional[str] = None

    # Telemetry
    enable_telemetry: bool = True

    # No-data-loss
    preserve_columns: List[str] = field(default_factory=list)
    preserve_max_rows_scan: int = 20000
    preserve_read_end_col: str = "ZZ"

    # Phase 2 — Schema enforcement / tab management
    ensure_tabs_exist: bool = True
    ensure_headers_match_schema: bool = True
    allow_extra_columns: bool = False  # strict by default
    headers_start_cell_default: str = "A5"

    # Phase 2 — Insights_Analysis criteria block (A1:D4)
    insights_criteria_enabled: bool = True
    insights_criteria_range: str = "A1:D4"

    # Env
    environment: str = "prod"
    user_agent: str = f"TadawulFastBridge-SheetsService/{SERVICE_VERSION}"

    @classmethod
    def from_env(cls) -> "SheetsServiceConfig":
        env = (_strip(os.getenv("APP_ENV", "prod")) or "prod").lower()

        safe_mode_str = (_strip(os.getenv("SHEETS_SAFE_MODE", "strict")) or "strict").lower()
        if safe_mode_str in ("off", "false", "0", "disabled"):
            safe_mode = SafeModeLevel.OFF
        elif safe_mode_str in ("basic",):
            safe_mode = SafeModeLevel.BASIC
        elif safe_mode_str in ("paranoid", "maximum", "max"):
            safe_mode = SafeModeLevel.PARANOID
        else:
            safe_mode = SafeModeLevel.STRICT

        token_transport: Set[TokenTransport] = set()
        transport_str = (_strip(os.getenv("SHEETS_BACKEND_TOKEN_TRANSPORT", "header,bearer")) or "header,bearer").lower()
        for part in transport_str.split(","):
            p = part.strip()
            if p == "header":
                token_transport.add(TokenTransport.HEADER)
            elif p == "bearer":
                token_transport.add(TokenTransport.BEARER)
            elif p == "query":
                token_transport.add(TokenTransport.QUERY)
            elif p == "body":
                token_transport.add(TokenTransport.BODY)

        preserve_cols = parse_csv_env("SHEETS_PRESERVE_COLUMNS", default="")
        preserve_max_rows_scan = coerce_int(
            os.getenv("SHEETS_PRESERVE_MAX_ROWS_SCAN", "20000"), 20000, lo=1000, hi=200000
        )
        preserve_end_col = (_strip(os.getenv("SHEETS_PRESERVE_READ_END_COL", "ZZ")) or "ZZ").upper()

        return cls(
            default_spreadsheet_id=_strip(os.getenv("DEFAULT_SPREADSHEET_ID", "")),
            sheets_api_retries=coerce_int(os.getenv("SHEETS_API_RETRIES", "4"), 4, lo=1, hi=12),
            sheets_api_retry_base_sleep=coerce_float(
                os.getenv("SHEETS_API_RETRY_BASE_SLEEP", "1.0"), 1.0, lo=0.1, hi=30.0
            ),
            sheets_api_timeout_sec=coerce_float(os.getenv("SHEETS_API_TIMEOUT_SEC", "60"), 60.0, lo=5.0, hi=600.0),
            backend_base_url=_strip(os.getenv("BACKEND_BASE_URL", "")).rstrip("/"),
            backend_timeout_sec=coerce_float(os.getenv("SHEETS_BACKEND_TIMEOUT_SEC", "120"), 120.0, lo=5.0, hi=600.0),
            backend_retries=coerce_int(os.getenv("SHEETS_BACKEND_RETRIES", "3"), 3, lo=0, hi=10),
            backend_retry_sleep=coerce_float(os.getenv("SHEETS_BACKEND_RETRY_SLEEP", "1.0"), 1.0, lo=0.1, hi=30.0),
            backend_max_symbols_per_call=coerce_int(
                os.getenv("SHEETS_BACKEND_MAX_SYMBOLS_PER_CALL", "200"), 200, lo=10, hi=5000
            ),
            backend_token_transport=token_transport or {TokenTransport.HEADER, TokenTransport.BEARER},
            backend_token_param_name=_strip(os.getenv("SHEETS_BACKEND_TOKEN_QUERY_PARAM", "token")) or "token",
            backend_circuit_breaker_threshold=coerce_int(os.getenv("BACKEND_CIRCUIT_BREAKER_THRESHOLD", "5"), 5, lo=1, hi=100),
            backend_circuit_breaker_timeout=coerce_int(os.getenv("BACKEND_CIRCUIT_BREAKER_TIMEOUT", "60"), 60, lo=5, hi=3600),
            max_rows_per_write=coerce_int(os.getenv("SHEETS_MAX_ROWS_PER_WRITE", "500"), 500, lo=50, hi=5000),
            use_batch_update=coerce_bool(os.getenv("SHEETS_USE_BATCH_UPDATE", "true"), True),
            max_batch_ranges=coerce_int(os.getenv("SHEETS_MAX_BATCH_RANGES", "25"), 25, lo=1, hi=200),
            clear_end_col=(_strip(os.getenv("SHEETS_CLEAR_END_COL", "ZZ")) or "ZZ").upper(),
            clear_end_row=coerce_int(os.getenv("SHEETS_CLEAR_END_ROW", "100000"), 100000, lo=1000, hi=2000000),
            smart_clear=coerce_bool(os.getenv("SHEETS_SMART_CLEAR", "true"), True),
            safe_mode=safe_mode,
            block_on_empty_headers=coerce_bool(os.getenv("SHEETS_BLOCK_ON_EMPTY_HEADERS", "true"), True),
            block_on_empty_rows=coerce_bool(os.getenv("SHEETS_BLOCK_ON_EMPTY_ROWS", "true"), True),
            block_on_data_mismatch=coerce_bool(os.getenv("SHEETS_BLOCK_ON_DATA_MISMATCH", "true"), True),
            validate_row_count=coerce_bool(os.getenv("SHEETS_VALIDATE_ROW_COUNT", "true"), True),
            require_symbol_header=coerce_bool(os.getenv("SHEETS_REQUIRE_SYMBOL_HEADER", "true"), True),
            cache_ttl_seconds=coerce_int(os.getenv("SHEETS_CACHE_TTL_SECONDS", "300"), 300, lo=5, hi=86400),
            enable_header_cache=coerce_bool(os.getenv("SHEETS_ENABLE_HEADER_CACHE", "true"), True),
            connection_pool_size=coerce_int(os.getenv("SHEETS_CONNECTION_POOL_SIZE", "20"), 20, lo=5, hi=200),
            keep_alive_seconds=coerce_int(os.getenv("SHEETS_KEEP_ALIVE_SECONDS", "30"), 30, lo=5, hi=300),
            verify_ssl=coerce_bool(os.getenv("SHEETS_VERIFY_SSL", "true"), True),
            ssl_cert_path=_strip(os.getenv("SHEETS_SSL_CERT_PATH", "")) or None,
            enable_telemetry=coerce_bool(os.getenv("SHEETS_ENABLE_TELEMETRY", "true"), True),
            preserve_columns=preserve_cols,
            preserve_max_rows_scan=preserve_max_rows_scan,
            preserve_read_end_col=preserve_end_col,
            ensure_tabs_exist=coerce_bool(os.getenv("SHEETS_ENSURE_TABS_EXIST", "true"), True),
            ensure_headers_match_schema=coerce_bool(os.getenv("SHEETS_ENSURE_HEADERS_MATCH_SCHEMA", "true"), True),
            allow_extra_columns=coerce_bool(os.getenv("SHEETS_ALLOW_EXTRA_COLUMNS", "false"), False),
            headers_start_cell_default=_strip(os.getenv("SHEETS_HEADERS_START_CELL", "A5")) or "A5",
            insights_criteria_enabled=coerce_bool(os.getenv("SHEETS_INSIGHTS_CRITERIA_ENABLED", "true"), True),
            insights_criteria_range=_strip(os.getenv("SHEETS_INSIGHTS_CRITERIA_RANGE", "A1:D4")) or "A1:D4",
            environment=env,
            user_agent=_strip(os.getenv("SHEETS_USER_AGENT", f"TadawulFastBridge-SheetsService/{SERVICE_VERSION}"))
            or f"TadawulFastBridge-SheetsService/{SERVICE_VERSION}",
        )


_CONFIG = SheetsServiceConfig.from_env()

# ---------------------------------------------------------------------------
# Telemetry
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class OperationMetrics:
    operation: str
    start_time: float
    end_time: float
    success: bool
    rows_processed: int = 0
    cells_updated: int = 0
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    sheet_name: Optional[str] = None
    request_id: Optional[str] = None

    @property
    def duration_ms(self) -> float:
        return (self.end_time - self.start_time) * 1000.0


class TelemetryCollector:
    def __init__(self):
        self._metrics: List[OperationMetrics] = []
        self._lock = threading.RLock()
        self._counters: Dict[str, int] = {}
        self._latencies: Dict[str, List[float]] = {}

    def record_operation(self, metrics: OperationMetrics) -> None:
        if not _CONFIG.enable_telemetry:
            return
        with self._lock:
            self._metrics.append(metrics)
            if len(self._metrics) > 10000:
                self._metrics = self._metrics[-10000:]

            key = f"{metrics.operation}:{'success' if metrics.success else 'failure'}"
            self._counters[key] = self._counters.get(key, 0) + 1

            if metrics.success:
                self._latencies.setdefault(metrics.operation, []).append(metrics.duration_ms)
                if len(self._latencies[metrics.operation]) > 200:
                    self._latencies[metrics.operation] = self._latencies[metrics.operation][-200:]

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            out: Dict[str, Any] = {
                "total_operations": len(self._metrics),
                "counters": dict(self._counters),
                "latency_by_operation": {},
            }
            for op, lat in self._latencies.items():
                if not lat:
                    continue
                s = sorted(lat)
                n = len(s)
                out["latency_by_operation"][op] = {
                    "avg_ms": sum(lat) / n,
                    "p50_ms": s[n // 2],
                    "p95_ms": s[min(n - 1, int(n * 0.95))],
                    "p99_ms": s[min(n - 1, int(n * 0.99))],
                    "max_ms": max(lat),
                }
            return out


_telemetry = TelemetryCollector()


def track_operation(operation: str):
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            request_id = uuid.uuid4().hex[:12]
            sheet_name = kwargs.get("sheet_name") or (args[1] if len(args) >= 2 else None)

            try:
                with TraceContext(f"sheets_{operation}", {"sheet_name": sheet_name or "", "request_id": request_id}):
                    result = func(*args, **kwargs)

                rows_written = int(result.get("rows_written", 0) or 0) if isinstance(result, dict) else 0
                cells_updated = int(result.get("cells_updated", 0) or 0) if isinstance(result, dict) else 0

                _telemetry.record_operation(
                    OperationMetrics(
                        operation=operation,
                        start_time=start,
                        end_time=time.time(),
                        success=True,
                        rows_processed=rows_written,
                        cells_updated=cells_updated,
                        sheet_name=str(sheet_name) if sheet_name else None,
                        request_id=request_id,
                    )
                )
                sheets_operations_total.labels(operation=operation, status="success").inc()
                sheets_operation_duration.labels(operation=operation).observe(time.time() - start)
                if isinstance(result, dict):
                    result.setdefault("request_id", request_id)
                return result
            except Exception as e:
                _telemetry.record_operation(
                    OperationMetrics(
                        operation=operation,
                        start_time=start,
                        end_time=time.time(),
                        success=False,
                        error_type=e.__class__.__name__,
                        error_message=str(e),
                        sheet_name=str(sheet_name) if sheet_name else None,
                        request_id=request_id,
                    )
                )
                sheets_operations_total.labels(operation=operation, status="error").inc()
                sheets_operation_duration.labels(operation=operation).observe(time.time() - start)
                raise

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# Circuit Breaker (backend)
# ---------------------------------------------------------------------------
class BackendCircuitBreaker:
    def __init__(self, threshold: int, timeout: int):
        self.threshold = max(1, int(threshold))
        self.timeout = max(5, int(timeout))
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self._lock = threading.RLock()

    def can_execute(self) -> bool:
        with self._lock:
            if self.state == CircuitState.CLOSED:
                return True
            if self.state == CircuitState.OPEN:
                if self._should_attempt_recovery():
                    self.state = CircuitState.HALF_OPEN
                    return True
                return False
            return True  # HALF_OPEN

    def record_success(self) -> None:
        with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.CLOSED
            self.failure_count = 0

    def record_failure(self) -> None:
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitState.CLOSED and self.failure_count >= self.threshold:
                self.state = CircuitState.OPEN
            elif self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN

    def _should_attempt_recovery(self) -> bool:
        if not self.last_failure_time:
            return True
        return (time.time() - self.last_failure_time) > self.timeout

    def get_state(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "state": self.state.value,
                "failure_count": self.failure_count,
                "threshold": self.threshold,
                "timeout": self.timeout,
                "last_failure": self.last_failure_time,
            }


# ---------------------------------------------------------------------------
# Credentials + Google Sheets client (lazy; startup-safe)
# ---------------------------------------------------------------------------
def _maybe_b64_decode(s: str) -> Optional[str]:
    t = _strip(s)
    if not t:
        return None
    if t.lower().startswith("b64:"):
        t = t.split(":", 1)[1].strip()
    try:
        return base64.b64decode(t.encode("utf-8"), validate=False).decode("utf-8", errors="replace")
    except Exception:
        return None


def _read_text_file_if_exists(p: str) -> Optional[str]:
    try:
        path = Path(_strip(p))
        if path.exists() and path.is_file():
            return path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None
    return None


def _repair_private_key(pk: str) -> str:
    pk2 = (pk or "").replace("\\n", "\n").replace("\\r\\n", "\n")
    if "-----BEGIN PRIVATE KEY-----" not in pk2:
        pk2 = "-----BEGIN PRIVATE KEY-----\n" + pk2.strip()
    if "-----END PRIVATE KEY-----" not in pk2:
        pk2 = pk2.strip() + "\n-----END PRIVATE KEY-----"
    return pk2 + ("\n" if not pk2.endswith("\n") else "")


class CredentialsManager:
    def __init__(self):
        self._creds_info: Optional[Dict[str, Any]] = None
        self._lock = threading.RLock()

    def _from_env_manager_settings(self) -> Optional[Dict[str, Any]]:
        """Try env.py global settings manager."""
        try:
            from env import get_settings as get_env_settings  # type: ignore
            s = get_env_settings()
            raw = getattr(s, "GOOGLE_SHEETS_CREDENTIALS", None)
            if isinstance(raw, str) and raw.strip().startswith("{"):
                obj = json_loads(raw)
                return obj if isinstance(obj, dict) else None
        except Exception:
            pass
        return None

    def reset_cache(self) -> None:
        """Flush cached credentials so next get_credentials() re-reads from env/file."""
        with self._lock:
            self._creds_info = None

    def get_credentials(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            if self._creds_info is not None:
                return dict(self._creds_info)

            for src in (self._from_env_dict, self._from_env_json, self._from_env_base64, self._from_env_file, self._from_settings, self._from_env_manager_settings):
                creds = src()
                if creds:
                    self._creds_info = self._normalize_credentials(creds)
                    return dict(self._creds_info)

            logger.error("No valid Google credentials found (needed only when Sheets operations are called).")
            return None

    def _normalize_credentials(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(creds)
        pk = out.get("private_key")
        if isinstance(pk, str) and pk.strip():
            out["private_key"] = _repair_private_key(pk)
        return out

    def _from_env_dict(self) -> Optional[Dict[str, Any]]:
        """Try GOOGLE_CREDENTIALS_DICT (raw JSON dict string)."""
        raw = _strip(os.getenv("GOOGLE_CREDENTIALS_DICT", ""))
        if raw.startswith("{") and raw.endswith("}"):
            try:
                obj = json_loads(raw)
                return obj if isinstance(obj, dict) else None
            except Exception:
                return None
        return None

    def _from_env_json(self) -> Optional[Dict[str, Any]]:
        raw = _strip(os.getenv("GOOGLE_SHEETS_CREDENTIALS", "") or os.getenv("GOOGLE_CREDENTIALS", ""))
        if raw.startswith("{"):
            try:
                obj = json_loads(raw)
                return obj if isinstance(obj, dict) else None
            except Exception:
                return None
        return None

    def _from_env_base64(self) -> Optional[Dict[str, Any]]:
        raw = _strip(os.getenv("GOOGLE_SHEETS_CREDENTIALS_B64", "") or os.getenv("GOOGLE_CREDENTIALS_B64", ""))
        if not raw:
            return None
        decoded = _maybe_b64_decode(raw)
        if not decoded:
            return None
        try:
            obj = json_loads(decoded)
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None

    def _from_env_file(self) -> Optional[Dict[str, Any]]:
        path = _strip(
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
            or os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", "")
            or os.getenv("GOOGLE_CREDENTIALS_FILE", "")
        )
        if not path:
            return None
        txt = _read_text_file_if_exists(path)
        if not txt:
            return None
        try:
            obj = json_loads(txt)
            return obj if isinstance(obj, dict) else None
        except Exception as e:
            logger.warning("Failed to load credentials from file %s: %s", path, e)
            return None

    def _from_settings(self) -> Optional[Dict[str, Any]]:
        try:
            from core.config import get_settings  # type: ignore

            s = get_settings()
            raw = getattr(s, "google_sheets_credentials_json", None)
            if isinstance(raw, str) and raw.strip().startswith("{"):
                try:
                    obj = json_loads(raw)
                    return obj if isinstance(obj, dict) else None
                except Exception:
                    pass
            d = getattr(s, "google_credentials_dict", None)
            if isinstance(d, dict) and d:
                return dict(d)
        except Exception:
            pass
        return None


_credentials_manager = CredentialsManager()

# FIX v5.5.3: Retry cooldown prevents retry storms when credentials are broken.
# Without this, every sheet write retried on failure, flooding logs with auth errors.
_SHEETS_INIT_RETRY_COOLDOWN_SEC: float = max(
    5.0,
    float((os.getenv("SHEETS_INIT_RETRY_COOLDOWN_SEC") or "30").strip() or "30"),
)


class SheetsAPIClient:
    """
    Lazy-initialized Google Sheets API client with full recovery support.

    v5.5.3 fixes vs v5.4.0:
    - FIX CRITICAL: _failed flag no longer sticks permanently after one error.
      v5.4.0 had `if self._failed: raise RuntimeError(...)` with NO reset path,
      meaning a single transient credential error would brick ALL Sheets writes
      for the entire process lifetime — requiring a full Render redeploy to recover.
    - FIX: Added reset() method: clears _failed + _service + flushes credential
      cache so updated credentials can be picked up without a restart.
    - FIX: Added retry cooldown (_SHEETS_INIT_RETRY_COOLDOWN_SEC, default 30s).
    - FIX: Added _mark_failed() and status() for operational visibility.
    - FIX: close() now properly tears down service + clears state.
    """

    def __init__(self) -> None:
        self._service = None
        self._lock = threading.RLock()
        self._failed: bool = False
        self._last_error: str = ""
        self._last_failure_ts: Optional[float] = None
        self._retry_cooldown_sec: float = _SHEETS_INIT_RETRY_COOLDOWN_SEC

    def get_service(self, *, retry_initialize: bool = True, reset_failed: bool = False):
        with self._lock:
            if reset_failed:
                self.reset(reason="manual_reset_before_get")

            if self._service is not None:
                return self._service

            if self._failed and not retry_initialize:
                raise RuntimeError(
                    self._last_error or "Sheets API client initialization failed previously"
                )

            if self._failed and retry_initialize:
                elapsed = time.time() - (self._last_failure_ts or 0.0)
                if elapsed < self._retry_cooldown_sec:
                    remaining = self._retry_cooldown_sec - elapsed
                    raise RuntimeError(
                        f"Sheets API init cooldown active — retry in {remaining:.0f}s. "
                        f"Last error: {self._last_error}"
                    )
                self.reset(reason=f"auto_retry_after_cooldown_{elapsed:.0f}s")

            try:
                self._service = self._create_service()
                self._failed = False
                self._last_error = ""
                self._last_failure_ts = None
                logger.info("SheetsAPIClient: initialized successfully")
                return self._service
            except Exception as exc:
                self._mark_failed(exc)
                raise

    def _mark_failed(self, exc: Exception) -> None:
        self._service = None
        self._failed = True
        self._last_error = f"{type(exc).__name__}: {exc}"
        self._last_failure_ts = time.time()
        logger.error(
            "SheetsAPIClient init failed (cooldown=%.0fs): %s",
            self._retry_cooldown_sec,
            self._last_error,
        )

    def reset(self, *, reason: str = "manual_reset") -> None:
        with self._lock:
            try:
                transport = getattr(self._service, "_http", None)
                if transport is not None and hasattr(transport, "close"):
                    transport.close()
            except Exception:
                pass
            self._service = None
            self._failed = False
            self._last_error = ""
            self._last_failure_ts = None
            _credentials_manager.reset_cache()
            logger.info("SheetsAPIClient reset: %s", reason)

    def _create_service(self):
        try:
            from google.oauth2.service_account import Credentials  # type: ignore
            from googleapiclient.discovery import build  # type: ignore
        except Exception as e:
            raise RuntimeError(
                "Missing Google API libs. Install: google-api-python-client google-auth"
            ) from e

        creds_info = _credentials_manager.get_credentials()
        if not creds_info:
            raise RuntimeError("Missing Google service account credentials")

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
        return build(
            "sheets", "v4", credentials=creds,
            cache_discovery=False, num_retries=_CONFIG.sheets_api_retries,
        )

    def close(self) -> None:
        """Tear down and clear state. Safe to call multiple times."""
        self.reset(reason="close")

    def status(self) -> Dict[str, Any]:
        """Return operational status dict."""
        with self._lock:
            elapsed_since_failure = (
                round(time.time() - self._last_failure_ts, 1)
                if self._last_failure_ts else None
            )
            cooldown_remaining = None
            if self._failed and self._last_failure_ts:
                elapsed = time.time() - self._last_failure_ts
                cooldown_remaining = max(0.0, round(self._retry_cooldown_sec - elapsed, 1))
            return {
                "failed": self._failed,
                "has_service": self._service is not None,
                "last_error": self._last_error,
                "last_failure_ts": self._last_failure_ts,
                "elapsed_since_failure_sec": elapsed_since_failure,
                "cooldown_remaining_sec": cooldown_remaining,
                "retry_cooldown_sec": self._retry_cooldown_sec,
            }


_sheets_client = SheetsAPIClient()


def get_sheets_service(*, retry_initialize: bool = True, reset_failed: bool = False):
    """
    Public getter for the authenticated Google Sheets service object.
    FIX v5.5.3: Added retry_initialize + reset_failed kwargs.
    """
    return _sheets_client.get_service(
        retry_initialize=retry_initialize, reset_failed=reset_failed
    )


def reset_sheets_service(
    *,
    reason: str = "manual_reset",
    clear_header_cache: bool = True,
    close_backend_client: bool = False,
) -> Dict[str, Any]:
    """
    Reset the Google Sheets service to a clean state.
    FIX v5.5.3: public helper for credential hot-reload.
    Useful after updating GOOGLE_SHEETS_CREDENTIALS in Render env vars.
    """
    _sheets_client.reset(reason=reason)
    if clear_header_cache:
        try:
            _header_cache.clear()
        except Exception:
            pass
    if close_backend_client:
        try:
            _backend_client.close()
        except Exception:
            pass
    return {
        "status": "ok",
        "reason": reason,
        "clear_header_cache": bool(clear_header_cache),
        "close_backend_client": bool(close_backend_client),
        "service_version": SERVICE_VERSION,
    }


# ---------------------------------------------------------------------------
# Header cache + canonical headers (schema_registry)
# ---------------------------------------------------------------------------
class HeaderCache:
    def __init__(self, ttl: int):
        self.ttl = max(5, int(ttl))
        self._cache: Dict[str, Tuple[List[str], float]] = {}
        self._lock = threading.RLock()

    def get(self, sheet_name: str) -> Optional[List[str]]:
        if not _CONFIG.enable_header_cache:
            return None
        key = (_strip(sheet_name) or "").upper()
        with self._lock:
            item = self._cache.get(key)
            if not item:
                return None
            headers, ts = item
            if (time.time() - ts) < self.ttl:
                return list(headers)
            self._cache.pop(key, None)
            return None

    def set(self, sheet_name: str, headers: List[str]) -> None:
        if not _CONFIG.enable_header_cache:
            return
        key = (_strip(sheet_name) or "").upper()
        with self._lock:
            self._cache[key] = (list(headers), time.time())

    def invalidate(self, sheet_name: str) -> None:
        key = (_strip(sheet_name) or "").upper()
        with self._lock:
            self._cache.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()


_header_cache = HeaderCache(_CONFIG.cache_ttl_seconds)


def get_canonical_headers(sheet_name: str) -> List[str]:
    """
    Phase 2: canonical headers come from schema_registry via core.config.schema_headers().
    Must return FULL schema headers (60+).
    """
    try:
        normalized = core_normalize_page(sheet_name, allow_output_pages=True) if _HAS_CORE else _strip(sheet_name)
    except Exception:
        normalized = _strip(sheet_name)

    cached = _header_cache.get(normalized)
    if cached is not None:
        return cached

    headers: List[str] = []
    if _HAS_CORE:
        try:
            headers = core_schema_headers(normalized) or []
        except Exception as e:
            logger.warning("Failed to get headers from schema_registry (core.config.schema_headers): %s", e)

    headers = ensure_symbol_header([h for h in (headers or []) if _strip(h)])

    if headers:
        _header_cache.set(normalized, headers)
    return headers


# ---------------------------------------------------------------------------
# A1 utilities
# ---------------------------------------------------------------------------
_A1_RE = re.compile(r"^\$?([A-Za-z]+)\$?(\d+)$")


@lru_cache(maxsize=1024)
def parse_a1_cell(cell: str) -> Tuple[str, int]:
    s = (_strip(cell) or "A1").strip()
    if ":" in s:
        s = s.split(":", 1)[0].strip()
    m = _A1_RE.match(s)
    if not m:
        return ("A", 1)
    col = m.group(1).upper()
    row = int(m.group(2))
    return (col, max(1, row))


@lru_cache(maxsize=1024)
def a1(col: str, row: int) -> str:
    return f"{col.upper()}{int(row)}"


@lru_cache(maxsize=1024)
def col_to_index(col: str) -> int:
    col = (_strip(col) or "A").upper()
    n = 0
    for ch in col:
        if "A" <= ch <= "Z":
            n = n * 26 + (ord(ch) - ord("A") + 1)
    return max(1, n)


@lru_cache(maxsize=1024)
def index_to_col(idx: int) -> str:
    idx = max(1, int(idx))
    s = ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        s = chr(rem + ord("A")) + s
    return s or "A"


def safe_sheet_name(name: str) -> str:
    name = (_strip(name) or "Sheet1").replace("'", "''")
    return f"'{name}'"


def compute_clear_end_col(start_col: str, num_cols: int) -> str:
    if num_cols <= 0:
        return _CONFIG.clear_end_col
    start_idx = col_to_index(start_col)
    end_idx = start_idx + num_cols - 1
    return index_to_col(end_idx)


# ---------------------------------------------------------------------------
# Symbol normalization
# ---------------------------------------------------------------------------
class SymbolNormalizer:
    @staticmethod
    @lru_cache(maxsize=4096)
    def normalize(symbol: str) -> str:
        s = _strip(symbol)
        if not s:
            return ""
        try:
            if _HAS_CORE:
                return core_normalize_symbol(s)
        except Exception:
            pass
        return s.upper()


_normalizer = SymbolNormalizer()


def normalize_tickers(tickers: Sequence[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for t in tickers or []:
        if t is None:
            continue
        norm = _normalizer.normalize(str(t))
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out


# ---------------------------------------------------------------------------
# Header & row processing helpers
# ---------------------------------------------------------------------------
@lru_cache(maxsize=2048)
def _normalize_header_key(header: str) -> str:
    s = _strip(header).lower()
    return re.sub(r"[^a-z0-9]", "", s)


# IMPORTANT: include both snake_case and "pretty" field names
_HEADER_ALIAS_MAP: Dict[str, str] = {
    # identity
    "symbol": "Symbol",
    "ticker": "Symbol",
    "requestedsymbol": "Symbol",
    # errors
    "error": "Error",
    "errormessage": "Error",
    # scoring_engine aligned
    "value_score": "Value Score",
    "quality_score": "Quality Score",
    "momentum_score": "Momentum Score",
    "risk_score": "Risk Score",
    "opportunity_score": "Opportunity Score",
    "overall_score": "Overall Score",
    "recommendation": "Recommendation",
    "rec": "Recommendation",
    # forecast aligned
    "forecast_price_1m": "Forecast Price (1M)",
    "forecast_price_3m": "Forecast Price (3M)",
    "forecast_price_6m": "Forecast Price (6M)",
    "forecast_price_12m": "Forecast Price (12M)",
    "expected_roi_1m": "Expected ROI % (1M)",
    "expected_roi_3m": "Expected ROI % (3M)",
    "expected_roi_6m": "Expected ROI % (6M)",
    "expected_roi_12m": "Expected ROI % (12M)",
    "expected_return_1m": "Expected ROI % (1M)",
    "expected_return_3m": "Expected ROI % (3M)",
    "expected_return_6m": "Expected ROI % (6M)",
    "expected_return_12m": "Expected ROI % (12M)",
    "forecast_updated_utc": "Forecast Updated (UTC)",
    "forecast_updated_riyadh": "Forecast Updated (Riyadh)",
    # badges
    "rec_badge": "Rec Badge",
    "momentum_badge": "Momentum Badge",
    "opportunity_badge": "Opportunity Badge",
    "risk_badge": "Risk Badge",
    "value_badge": "Value Badge",
    "quality_badge": "Quality Badge",
}
_HEADER_ALIAS_MAP_NORM = {_normalize_header_key(k): v for k, v in _HEADER_ALIAS_MAP.items()}


def append_missing_headers(headers: List[str], extra: Sequence[str]) -> List[str]:
    seen = {_normalize_header_key(h) for h in headers}
    out = list(headers)
    for h in extra:
        hh = _strip(h)
        if not hh:
            continue
        nh = _normalize_header_key(hh)
        if nh not in seen:
            seen.add(nh)
            out.append(hh)
    return out


def find_symbol_col(headers: List[str]) -> int:
    for i, h in enumerate(headers):
        if _normalize_header_key(h) in ("symbol", "ticker", "requestedsymbol"):
            return i
    return -1


def find_error_col(headers: List[str]) -> int:
    for i, h in enumerate(headers):
        if _normalize_header_key(h) == "error":
            return i
    return -1


def ensure_symbol_header(headers: List[str]) -> List[str]:
    return headers if find_symbol_col(headers) >= 0 else ["Symbol"] + list(headers)


def rows_to_grid(headers: List[str], rows: Any) -> Tuple[List[str], List[List[Any]]]:
    headers = [(_strip(h) or "") for h in (headers or []) if _strip(h)]
    headers = ensure_symbol_header(headers or ["Symbol", "Error"])

    fixed_rows: List[List[Any]] = []
    if not rows:
        return headers, fixed_rows

    # dict rows
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        header_lookup = {_normalize_header_key(h): i for i, h in enumerate(headers)}
        for row_dict in rows:
            if not isinstance(row_dict, dict):
                continue
            row_data = [None] * len(headers)
            for key, value in row_dict.items():
                nk = _normalize_header_key(key)
                if nk in header_lookup:
                    row_data[header_lookup[nk]] = value
                    continue
                alias = _HEADER_ALIAS_MAP_NORM.get(nk)
                if alias:
                    na = _normalize_header_key(alias)
                    if na in header_lookup:
                        row_data[header_lookup[na]] = value
            fixed_rows.append(row_data)
        return headers, fixed_rows

    # list rows
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, (list, tuple)):
                row = [row]
            row_list = list(row)
            if len(row_list) < len(headers):
                row_list += [None] * (len(headers) - len(row_list))
            elif len(row_list) > len(headers):
                row_list = row_list[: len(headers)]
            fixed_rows.append(row_list)
        return headers, fixed_rows

    fixed_rows.append([rows] + [None] * (len(headers) - 1))
    return headers, fixed_rows


def reorder_to_canonical(sheet_name: str, headers: List[str], rows: List[List[Any]]) -> Tuple[List[str], List[List[Any]]]:
    canonical = get_canonical_headers(sheet_name)
    if not canonical:
        return headers, rows

    canonical = [h for h in canonical if _strip(h)]
    if not canonical:
        return headers, rows

    src_index = {_normalize_header_key(h): i for i, h in enumerate(headers)}

    # Strict schema by default (Phase 2)
    new_headers = ensure_symbol_header(list(canonical))

    if _CONFIG.allow_extra_columns:
        canonical_norm = {_normalize_header_key(h) for h in canonical}
        extra_headers = [h for h in headers if _normalize_header_key(h) not in canonical_norm]
        new_headers = ensure_symbol_header(list(canonical) + extra_headers)

    new_rows: List[List[Any]] = []
    for row in rows:
        new_row = [None] * len(new_headers)
        for i, h in enumerate(new_headers):
            nh = _normalize_header_key(h)
            if nh in src_index:
                j = src_index[nh]
                if j < len(row):
                    new_row[i] = row[j]
        new_rows.append(new_row)

    return new_headers, new_rows


def remap_rows_to_headers(src_headers: List[str], src_rows: Any, dst_headers: List[str]) -> List[List[Any]]:
    """
    Convert (src_headers, src_rows) into rows aligned to dst_headers order,
    using normalized header keys + alias map.
    """
    sh, sr = rows_to_grid(src_headers, src_rows)
    dst_headers = ensure_symbol_header(list(dst_headers))

    dst_key_to_idx = {_normalize_header_key(h): i for i, h in enumerate(dst_headers)}
    src_key_to_idx = {_normalize_header_key(h): i for i, h in enumerate(sh)}

    out: List[List[Any]] = []
    for row in sr:
        new_row = [None] * len(dst_headers)
        for src_k_norm, si in src_key_to_idx.items():
            if si >= len(row):
                continue
            if src_k_norm in dst_key_to_idx:
                new_row[dst_key_to_idx[src_k_norm]] = row[si]
                continue
            alias_dst = _HEADER_ALIAS_MAP_NORM.get(src_k_norm)
            if alias_dst:
                alias_dst_norm = _normalize_header_key(alias_dst)
                if alias_dst_norm in dst_key_to_idx:
                    new_row[dst_key_to_idx[alias_dst_norm]] = row[si]
        out.append(new_row)
    return out


def pad_rows(rows: List[List[Any]], new_len: int) -> List[List[Any]]:
    if new_len <= 0:
        return rows
    out: List[List[Any]] = []
    for r in rows:
        rr = list(r)
        if len(rr) < new_len:
            rr += [None] * (new_len - len(rr))
        elif len(rr) > new_len:
            rr = rr[:new_len]
        out.append(rr)
    return out


# ---------------------------------------------------------------------------
# Google Sheets operations (Retry + Full Jitter)
# ---------------------------------------------------------------------------
def _sleep_full_jitter(base_sleep: float, cap: float = 30.0) -> None:
    time.sleep(random.uniform(0.0, min(cap, base_sleep)))


def _retry_sheet_op(operation_name: str, func: Callable, *args, **kwargs):
    last_exc: Optional[Exception] = None
    retries = max(1, int(_CONFIG.sheets_api_retries))
    for attempt in range(retries):
        try:
            with TraceContext(f"sheets_api_{operation_name.lower().replace(' ', '_')}"):
                return func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            if attempt >= retries - 1:
                raise
            base = float(_CONFIG.sheets_api_retry_base_sleep) * (2**attempt)
            logger.warning("Sheets API %s retry %s/%s (base=%.2fs)", operation_name, attempt + 1, retries, base)
            _sleep_full_jitter(base, cap=15.0)
    raise last_exc  # type: ignore


def read_range(spreadsheet_id: str, range_name: str) -> List[List[Any]]:
    service = get_sheets_service()

    def _read():
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_name, majorDimension="ROWS")
            .execute()
        )
        return result.get("values", []) or []

    return _retry_sheet_op("Read Range", _read)


def read_ranges_batch(spreadsheet_id: str, ranges: List[str], major_dimension: str = "COLUMNS") -> List[List[List[Any]]]:
    """
    Returns list of "values" per range. Each is a list-of-lists per majorDimension.
    major_dimension="COLUMNS" makes each range return: [ [col_values...] ].
    """
    service = get_sheets_service()

    def _batch_get():
        result = (
            service.spreadsheets()
            .values()
            .batchGet(spreadsheetId=spreadsheet_id, ranges=ranges, majorDimension=major_dimension)
            .execute()
        )
        vals = result.get("valueRanges", []) or []
        out = []
        for vr in vals:
            out.append(vr.get("values", []) or [])
        return out

    return _retry_sheet_op("Batch Get", _batch_get)


def write_range(spreadsheet_id: str, range_name: str, values: List[List[Any]], value_input: str = "RAW") -> int:
    service = get_sheets_service()
    body = {"values": values or [[]]}

    def _write():
        result = (
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption=value_input,
                body=body,
            )
            .execute()
        )
        return int(result.get("updatedCells", 0) or 0)

    return _retry_sheet_op("Write Range", _write)


def clear_range(spreadsheet_id: str, range_name: str) -> None:
    service = get_sheets_service()

    def _clear():
        service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=range_name).execute()

    _retry_sheet_op("Clear Range", _clear)


def write_grid_chunked(
    spreadsheet_id: str,
    sheet_name: str,
    start_cell: str,
    grid: List[List[Any]],
    value_input: str = "RAW",
) -> int:
    if not grid:
        return 0

    start_col, start_row = parse_a1_cell(start_cell)
    sheet_a1 = safe_sheet_name(sheet_name)

    header = list(grid[0] if grid else [])
    data_rows = grid[1:] if len(grid) > 1 else []

    header_len = len(header)
    fixed_rows: List[List[Any]] = []
    for row in data_rows:
        if not isinstance(row, (list, tuple)):
            row = [row]
        row_list = list(row)
        if len(row_list) < header_len:
            row_list += [None] * (header_len - len(row_list))
        elif len(row_list) > header_len:
            row_list = row_list[:header_len]
        fixed_rows.append(row_list)

    chunks = [
        fixed_rows[i : i + _CONFIG.max_rows_per_write]
        for i in range(0, len(fixed_rows), _CONFIG.max_rows_per_write)
    ]
    total_cells = 0

    if _CONFIG.use_batch_update:
        try:
            service = get_sheets_service()
            batch_data = []

            rng0 = f"{sheet_a1}!{a1(start_col, start_row)}"
            batch_data.append({"range": rng0, "values": [header] + (chunks[0] if chunks else [])})

            current_row = start_row + 1 + (len(chunks[0]) if chunks else 0)
            for chunk in chunks[1:]:
                rng = f"{sheet_a1}!{a1(start_col, current_row)}"
                batch_data.append({"range": rng, "values": chunk})
                current_row += len(chunk)

            for i in range(0, len(batch_data), _CONFIG.max_batch_ranges):
                batch = batch_data[i : i + _CONFIG.max_batch_ranges]

                def _batch_update():
                    body = {"valueInputOption": value_input, "data": batch}
                    result = (
                        service.spreadsheets()
                        .values()
                        .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
                        .execute()
                    )
                    responses = result.get("responses", []) or []
                    return sum(int(resp.get("updatedCells", 0) or 0) for resp in responses)

                total_cells += _retry_sheet_op("Batch Write", _batch_update)

            return total_cells
        except Exception as e:
            logger.warning("Batch update failed, falling back to sequential: %s", e)

    rng0 = f"{sheet_a1}!{a1(start_col, start_row)}"
    total_cells += write_range(spreadsheet_id, rng0, [header] + (chunks[0] if chunks else []), value_input=value_input)

    current_row = start_row + 1 + (len(chunks[0]) if chunks else 0)
    for chunk in chunks[1:]:
        rng = f"{sheet_a1}!{a1(start_col, current_row)}"
        total_cells += write_range(spreadsheet_id, rng, chunk, value_input=value_input)
        current_row += len(chunk)

    return total_cells


# ---------------------------------------------------------------------------
# Phase 2 — Ensure tabs exist + seed headers + minimal formatting
# ---------------------------------------------------------------------------
def _get_sheet_title_to_id(spreadsheet_id: str) -> Dict[str, int]:
    service = get_sheets_service()

    def _get():
        meta = (
            service.spreadsheets()
            .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title,gridProperties))")
            .execute()
        )
        out: Dict[str, int] = {}
        for s in (meta.get("sheets") or []):
            props = (s or {}).get("properties") or {}
            title = _strip(props.get("title"))
            sid = props.get("sheetId")
            if title and isinstance(sid, int):
                out[title] = sid
        return out

    return _retry_sheet_op("Get Spreadsheet Meta", _get)


def _batch_update(spreadsheet_id: str, requests: List[Dict[str, Any]]) -> None:
    if not requests:
        return
    service = get_sheets_service()

    def _do():
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute()

    _retry_sheet_op("BatchUpdate", _do)


def ensure_canonical_tabs_exist(spreadsheet_id: str) -> None:
    """
    Creates missing canonical tabs from page_catalog.
    NEVER creates forbidden pages (KSA_Tadawul / Advisor_Criteria).
    """
    spreadsheet_id = _strip(spreadsheet_id or _CONFIG.default_spreadsheet_id)
    if not spreadsheet_id:
        return

    existing = _get_sheet_title_to_id(spreadsheet_id)
    canonical = list(core_canonical_pages() or [])
    forbidden = set(core_forbidden_pages() or set())

    reqs: List[Dict[str, Any]] = []
    for title in canonical:
        if title in forbidden:
            continue
        if title not in existing:
            reqs.append({"addSheet": {"properties": {"title": title}}})

    if reqs:
        _batch_update(spreadsheet_id, reqs)


def _read_header_row(spreadsheet_id: str, sheet_name: str, start_cell: str, end_col: str = "ZZ") -> List[str]:
    start_col, start_row = parse_a1_cell(start_cell)
    rng = f"{safe_sheet_name(sheet_name)}!{a1(start_col, start_row)}:{end_col}{start_row}"
    vals = read_range(spreadsheet_id, rng)
    if not vals or not vals[0]:
        return []
    return [str(x).strip() for x in vals[0] if str(x).strip()]


def _seed_insights_criteria_if_empty(spreadsheet_id: str, sheet_name: str) -> None:
    if not _CONFIG.insights_criteria_enabled:
        return
    if sheet_name != "Insights_Analysis":
        return
    try:
        existing = read_range(spreadsheet_id, f"{safe_sheet_name(sheet_name)}!{_CONFIG.insights_criteria_range}")
        has_any = False
        for row in existing or []:
            for v in row or []:
                if _strip(v):
                    has_any = True
                    break
            if has_any:
                break
        if has_any:
            return

        block = [
            ["Advisor Criteria", "", "", ""],
            ["Risk Level", "Moderate", "Confidence Level", "High"],
            ["Invest Period (days)", "90", "Min Expected ROI %", "3"],
            ["Max Risk", "Moderate", "Pages Selected", "Market_Leaders,Global_Markets,Commodities_FX,Mutual_Funds,My_Portfolio"],
        ]
        write_range(spreadsheet_id, f"{safe_sheet_name(sheet_name)}!A1", block, value_input="RAW")
    except Exception as e:
        logger.warning("Insights criteria seed skipped (non-fatal): %s", e)


def ensure_headers_and_formatting(spreadsheet_id: str, sheet_name: str, start_cell: str) -> None:
    """
    Ensures:
    - tab exists (if enabled)
    - header row EXACTLY matches schema headers
    - Insights_Analysis criteria block seeded (optional)
    - basic freeze + filter + baseline column widths (best-effort)
    """
    spreadsheet_id = _strip(spreadsheet_id or _CONFIG.default_spreadsheet_id)
    if not spreadsheet_id:
        return

    # Normalize + block forbidden
    sheet_name = core_normalize_page(sheet_name, allow_output_pages=True)
    if sheet_name in set(core_forbidden_pages() or set()):
        raise ValueError(f"Forbidden/removed sheet: {sheet_name}")

    if _CONFIG.ensure_tabs_exist:
        ensure_canonical_tabs_exist(spreadsheet_id)

    # Data_Dictionary headers recommended at A1 (not A5)
    effective_start_cell = start_cell
    if sheet_name == "Data_Dictionary" and _strip(start_cell).upper() == "A5":
        effective_start_cell = "A1"

    canonical = get_canonical_headers(sheet_name) or []
    canonical = ensure_symbol_header([h for h in canonical if _strip(h)])

    current = _read_header_row(spreadsheet_id, sheet_name, effective_start_cell)
    current_norm = [_normalize_header_key(h) for h in current]
    canon_norm = [_normalize_header_key(h) for h in canonical]

    if current_norm != canon_norm:
        write_range(spreadsheet_id, f"{safe_sheet_name(sheet_name)}!{effective_start_cell}", [canonical], value_input="RAW")
        _header_cache.invalidate(sheet_name)

    _seed_insights_criteria_if_empty(spreadsheet_id, sheet_name)

    # Minimal formatting (best-effort)
    try:
        title_to_id = _get_sheet_title_to_id(spreadsheet_id)
        sid = title_to_id.get(sheet_name)
        if sid is None:
            return

        start_col, start_row = parse_a1_cell(effective_start_cell)
        frozen_rows = max(1, int(start_row))
        ncols = max(1, len(canonical))
        end_col_idx = col_to_index(start_col) + ncols - 1

        reqs: List[Dict[str, Any]] = []

        # Freeze rows
        reqs.append(
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": sid, "gridProperties": {"frozenRowCount": frozen_rows}},
                    "fields": "gridProperties.frozenRowCount",
                }
            }
        )

        # Clear previous filter if exists (best effort)
        reqs.append({"clearBasicFilter": {"sheetId": sid}})

        # Filter on header row
        reqs.append(
            {
                "setBasicFilter": {
                    "filter": {
                        "range": {
                            "sheetId": sid,
                            "startRowIndex": frozen_rows - 1,
                            "endRowIndex": min(_CONFIG.clear_end_row, frozen_rows - 1 + 50000),
                            "startColumnIndex": col_to_index(start_col) - 1,
                            "endColumnIndex": end_col_idx,
                        }
                    }
                }
            }
        )

        # Baseline column width
        reqs.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sid,
                        "dimension": "COLUMNS",
                        "startIndex": col_to_index(start_col) - 1,
                        "endIndex": end_col_idx,
                    },
                    "properties": {"pixelSize": 140},
                    "fields": "pixelSize",
                }
            }
        )

        _batch_update(spreadsheet_id, reqs)
    except Exception as e:
        logger.warning("Formatting skipped (non-fatal): %s", e)


# ---------------------------------------------------------------------------
# SAFE MODE validation
# ---------------------------------------------------------------------------
class SafeModeValidator:
    def __init__(self, config: SheetsServiceConfig):
        self.config = config

    def validate_backend_response(
        self,
        response: Dict[str, Any],
        requested_symbols: List[str],
        sheet_name: str,
        *,
        allow_empty_headers: bool = False,
        allow_empty_rows: bool = False,
    ) -> Optional[str]:
        if not isinstance(response, dict):
            return "SAFE MODE: Backend response is not a dict"

        headers = response.get("headers", []) or []
        rows = response.get("rows", []) or []

        if self.config.safe_mode == SafeModeLevel.OFF:
            return None

        if self.config.block_on_empty_headers and (not headers) and (not allow_empty_headers):
            return "SAFE MODE: Backend returned empty headers"

        if self.config.block_on_empty_rows and (not rows) and (not allow_empty_rows):
            return "SAFE MODE: Backend returned empty rows"

        if headers and self.config.require_symbol_header and find_symbol_col(list(headers)) < 0:
            return "SAFE MODE: Backend headers missing 'Symbol' column"

        if self.config.safe_mode == SafeModeLevel.BASIC:
            return None

        if self.config.block_on_data_mismatch and headers and rows:
            for i, row in enumerate(rows[:10]):
                if isinstance(row, list) and len(row) != len(headers):
                    return f"SAFE MODE: Row {i+1} length ({len(row)}) doesn't match headers ({len(headers)})"

        if self.config.safe_mode == SafeModeLevel.STRICT:
            return None

        if self.config.validate_row_count and headers and requested_symbols:
            symbol_col = find_symbol_col(list(headers))
            if symbol_col >= 0:
                returned: Set[str] = set()
                for row in rows:
                    if isinstance(row, list) and symbol_col < len(row):
                        sym = _strip(row[symbol_col]).upper()
                        if sym:
                            returned.add(sym)
                req = set([_strip(x).upper() for x in requested_symbols if _strip(x)])
                missing = req - returned
                if missing and len(missing) > max(1, int(len(req) * 0.5)):
                    return f"SAFE MODE: Missing >50% of requested symbols ({len(missing)}/{len(req)}) for {sheet_name}"

        return None


_safe_mode_validator = SafeModeValidator(_CONFIG)

# ---------------------------------------------------------------------------
# Backend API client (httpx preferred, urllib fallback) — LAZY httpx init
# ---------------------------------------------------------------------------
class BackendAPIClient:
    def __init__(self):
        self.circuit_breaker = BackendCircuitBreaker(
            _CONFIG.backend_circuit_breaker_threshold, _CONFIG.backend_circuit_breaker_timeout
        )
        self._ssl_ctx = self._create_ssl_context()
        self._httpx_client = None
        self._httpx_lock = threading.RLock()

    def _create_ssl_context(self) -> ssl.SSLContext:
        ctx = ssl.create_default_context()
        if not _CONFIG.verify_ssl:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        if _CONFIG.ssl_cert_path:
            try:
                ctx.load_verify_locations(cafile=_CONFIG.ssl_cert_path)
            except Exception as e:
                logger.warning("Failed to load SSL cert from %s: %s", _CONFIG.ssl_cert_path, e)
        return ctx

    def _get_httpx_client(self):
        if not (_HAS_HTTPX and httpx is not None):
            return None
        with self._httpx_lock:
            if self._httpx_client is not None:
                return self._httpx_client
            try:
                limits = httpx.Limits(
                    max_connections=max(10, int(_CONFIG.connection_pool_size)),
                    max_keepalive_connections=max(5, int(_CONFIG.connection_pool_size // 2)),
                    keepalive_expiry=float(max(5, int(_CONFIG.keep_alive_seconds))),
                )
                verify = _CONFIG.ssl_cert_path if _CONFIG.ssl_cert_path else bool(_CONFIG.verify_ssl)
                self._httpx_client = httpx.Client(
                    limits=limits,
                    timeout=float(_CONFIG.backend_timeout_sec),
                    verify=verify,
                    headers={"User-Agent": _CONFIG.user_agent, "Accept": "application/json"},
                )
            except Exception as e:
                logger.warning("httpx client init failed, falling back to urllib: %s", e)
                self._httpx_client = None
            return self._httpx_client

    def close(self) -> None:
        try:
            with self._httpx_lock:
                if self._httpx_client is not None:
                    self._httpx_client.close()
                self._httpx_client = None
        except Exception:
            pass

    def _resolve_token_candidates(self) -> List[str]:
        toks: List[str] = []
        for name in ("ALLOWED_TOKENS", "TFB_ALLOWED_TOKENS", "APP_TOKENS"):
            toks.extend(parse_csv_env(name, default=""))
        for name in ("APP_TOKEN", "BACKEND_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
            v = _strip(os.getenv(name, ""))
            if v:
                toks.append(v)
        out: List[str] = []
        seen: Set[str] = set()
        for t in toks:
            if not t or t in seen:
                continue
            seen.add(t)
            out.append(t)
        return out

    def _get_token(self) -> str:
        toks = self._resolve_token_candidates()
        return toks[0] if toks else ""

    def _build_headers(self, token: str, request_id: str) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": _CONFIG.user_agent,
            "X-Request-Id": request_id,
        }
        if not token:
            return headers
        if TokenTransport.HEADER in _CONFIG.backend_token_transport:
            headers["X-APP-TOKEN"] = token
        if TokenTransport.BEARER in _CONFIG.backend_token_transport:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _compose_url_and_params(
        self, endpoint: str, token: str, query_params: Optional[Dict[str, Any]]
    ) -> Tuple[str, Dict[str, Any]]:
        base_url = _CONFIG.backend_base_url.rstrip("/")
        url = f"{base_url}{endpoint}"
        params: Dict[str, Any] = dict(query_params or {})
        if token and TokenTransport.QUERY in _CONFIG.backend_token_transport:
            params[_CONFIG.backend_token_param_name] = token
        return url, params

    def _error_payload(self, symbols: List[str], error: str, request_id: str) -> Dict[str, Any]:
        return {
            "status": "error",
            "error": error,
            "headers": ["Symbol", "Error"],
            "rows": [[s, error] for s in (symbols or [])],
            "request_id": request_id,
        }

    def call_api(self, endpoint: str, payload: Dict[str, Any], query_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        request_id = uuid.uuid4().hex[:12]
        with TraceContext("backend_api_call", {"endpoint": endpoint, "request_id": request_id}):
            symbols = payload.get("symbols", []) or payload.get("tickers", []) or []
            if not self.circuit_breaker.can_execute():
                st = self.circuit_breaker.get_state()
                return self._error_payload(symbols, f"Circuit breaker {st['state']}", request_id)

            base_url = _CONFIG.backend_base_url.rstrip("/")
            if not base_url:
                return self._error_payload(symbols, "No backend URL configured", request_id)

            token = self._get_token()
            url, params = self._compose_url_and_params(endpoint, token, query_params)

            try:
                body_bytes = json_dumps_bytes(payload, default=str)
            except Exception as e:
                self.circuit_breaker.record_failure()
                return self._error_payload(symbols, f"JSON encode error: {e}", request_id)

            last_error: Optional[str] = None
            retries = max(0, int(_CONFIG.backend_retries))

            for attempt in range(retries + 1):
                try:
                    client = self._get_httpx_client()
                    if client is not None:
                        resp = client.post(
                            url,
                            params=params or None,
                            content=body_bytes,
                            headers=self._build_headers(token, request_id),
                        )
                        status = int(resp.status_code)
                        if 200 <= status < 300:
                            try:
                                data = resp.json()
                            except Exception:
                                try:
                                    data = json_loads(resp.content)
                                except Exception:
                                    data = {}
                            self.circuit_breaker.record_success()
                            if not isinstance(data, dict):
                                data = {}
                            data.setdefault("status", "success")
                            data.setdefault("headers", [])
                            data.setdefault("rows", [])
                            data.setdefault("request_id", request_id)
                            return data

                        snip = ""
                        try:
                            snip = (resp.text or "")[:400]
                        except Exception:
                            snip = ""
                        last_error = f"HTTP {status}: {snip}"
                        if status in (429, 500, 502, 503, 504) and attempt < retries:
                            _sleep_full_jitter(float(_CONFIG.backend_retry_sleep) * (2**attempt), cap=30.0)
                            continue
                        self.circuit_breaker.record_failure()
                        return self._error_payload(symbols, last_error, request_id)

                    # urllib fallback
                    request_url = url
                    if params:
                        parsed = urllib.parse.urlsplit(request_url)
                        q = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
                        q.update(params)
                        request_url = urllib.parse.urlunsplit(
                            (
                                parsed.scheme,
                                parsed.netloc,
                                parsed.path,
                                urllib.parse.urlencode(q, doseq=True),
                                parsed.fragment,
                            )
                        )

                    req = urllib.request.Request(
                        request_url,
                        data=body_bytes,
                        headers=self._build_headers(token, request_id),
                        method="POST",
                    )
                    try:
                        with urllib.request.urlopen(
                            req,
                            timeout=float(_CONFIG.backend_timeout_sec),
                            context=self._ssl_ctx,
                        ) as resp2:
                            raw2 = resp2.read()
                            status2 = int(getattr(resp2, "status", 200) or 200)
                    except urllib.error.HTTPError as he:
                        raw2 = he.read() if hasattr(he, "read") else b""
                        status2 = int(getattr(he, "code", 500) or 500)

                    if 200 <= status2 < 300:
                        try:
                            data2 = json_loads(raw2)
                        except Exception as e:
                            self.circuit_breaker.record_failure()
                            return self._error_payload(symbols, f"Invalid JSON: {e}", request_id)

                        self.circuit_breaker.record_success()
                        if not isinstance(data2, dict):
                            data2 = {}
                        data2.setdefault("status", "success")
                        data2.setdefault("headers", [])
                        data2.setdefault("rows", [])
                        data2.setdefault("request_id", request_id)
                        return data2

                    try:
                        snip2 = raw2.decode("utf-8", errors="replace")[:400]
                    except Exception:
                        snip2 = ""
                    last_error = f"HTTP {status2}: {snip2}"

                    if status2 in (429, 500, 502, 503, 504) and attempt < retries:
                        _sleep_full_jitter(float(_CONFIG.backend_retry_sleep) * (2**attempt), cap=30.0)
                        continue

                    self.circuit_breaker.record_failure()
                    return self._error_payload(symbols, last_error, request_id)

                except Exception as e:
                    last_error = f"Request error: {e}"
                    if attempt < retries:
                        _sleep_full_jitter(float(_CONFIG.backend_retry_sleep) * (2**attempt), cap=30.0)
                        continue
                    self.circuit_breaker.record_failure()
                    return self._error_payload(symbols, last_error, request_id)

            self.circuit_breaker.record_failure()
            return self._error_payload(symbols, last_error or "Max retries exceeded", request_id)

    def call_api_chunked(
        self,
        endpoint: str,
        symbols: List[str],
        base_payload: Dict[str, Any],
        query_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if len(symbols) <= _CONFIG.backend_max_symbols_per_call:
            payload = dict(base_payload)
            payload["symbols"] = symbols
            payload["tickers"] = symbols
            return self.call_api(endpoint, payload, query_params)

        chunks = [
            symbols[i : i + _CONFIG.backend_max_symbols_per_call]
            for i in range(0, len(symbols), _CONFIG.backend_max_symbols_per_call)
        ]
        responses: List[Dict[str, Any]] = []
        first_headers: List[str] = []

        for chunk in chunks:
            payload = dict(base_payload)
            payload["symbols"] = chunk
            payload["tickers"] = chunk
            resp = self.call_api(endpoint, payload, query_params)
            responses.append(resp)
            if not first_headers:
                first_headers = list(resp.get("headers") or [])

        return self._merge_responses(symbols, first_headers, responses)

    def _merge_responses(self, requested_symbols: List[str], first_headers: List[str], responses: List[Dict[str, Any]]) -> Dict[str, Any]:
        status = "success"
        error_msg: Optional[str] = None

        all_headers = list(first_headers)
        for resp in responses:
            all_headers = append_missing_headers(all_headers, resp.get("headers", []) or [])
        all_headers = ensure_symbol_header(all_headers or ["Symbol", "Error"])

        sym_idx = find_symbol_col(all_headers)
        err_idx = find_error_col(all_headers)
        row_map: Dict[str, List[Any]] = {}

        for resp in responses:
            resp_status = (_strip(resp.get("status")) or "success").lower()
            if resp_status in ("error", "partial"):
                status = "partial"
            if resp_status == "error" and not error_msg:
                error_msg = _strip(resp.get("error"))

            h = resp.get("headers") or []
            r = resp.get("rows") or []
            if not h or not r:
                continue

            remapped_rows = remap_rows_to_headers(list(h), r, all_headers)
            if sym_idx < 0:
                continue

            for row in remapped_rows:
                if sym_idx < len(row):
                    sym = _strip(row[sym_idx]).upper()
                    if sym:
                        row_map[sym] = row

        final_rows: List[List[Any]] = []
        for sym in requested_symbols:
            sym_u = _strip(sym).upper()
            if sym_u and sym_u in row_map:
                row = list(row_map[sym_u])
                row = pad_rows([row], len(all_headers))[0]
                final_rows.append(row)
            else:
                placeholder = [None] * len(all_headers)
                if sym_idx >= 0:
                    placeholder[sym_idx] = sym_u or sym
                if err_idx >= 0:
                    placeholder[err_idx] = "No data from backend"
                final_rows.append(placeholder)
                status = "partial"

        return {"status": status, "headers": all_headers, "rows": final_rows, "error": error_msg}


_backend_client = BackendAPIClient()

# ---------------------------------------------------------------------------
# Preserve Columns (No Data Loss) — by Symbol (batchGet minimal columns)
# ---------------------------------------------------------------------------
def _is_blank(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and not v.strip():
        return True
    return False


def _build_preserve_map(
    spreadsheet_id: str,
    sheet_name: str,
    start_cell: str,
    preserve_cols: List[str],
    *,
    max_rows_scan: int,
    read_end_col: str,
) -> Dict[str, Dict[str, Any]]:
    preserve_norm = {_normalize_header_key(c) for c in preserve_cols if _strip(c)}
    if not preserve_norm:
        return {}

    start_col, start_row = parse_a1_cell(start_cell)
    hdr_row = start_row
    data_start = hdr_row + 1
    data_end = data_start + max(1, int(max_rows_scan)) - 1

    header_range = f"{safe_sheet_name(sheet_name)}!{a1(start_col, hdr_row)}:{read_end_col}{hdr_row}"
    header_vals = read_range(spreadsheet_id, header_range)
    if not header_vals or not header_vals[0]:
        return {}

    headers = [str(x).strip() for x in header_vals[0] if str(x).strip()]
    if not headers:
        return {}

    sym_idx = find_symbol_col(headers)
    if sym_idx < 0:
        return {}

    preserve_indices: List[Tuple[int, str]] = []
    for i, h in enumerate(headers):
        nh = _normalize_header_key(h)
        if nh in preserve_norm:
            preserve_indices.append((i, nh))

    if not preserve_indices:
        return {}

    start_col_idx = col_to_index(start_col)
    sym_col_letter = index_to_col(start_col_idx + sym_idx)
    ranges = [f"{safe_sheet_name(sheet_name)}!{sym_col_letter}{data_start}:{sym_col_letter}{data_end}"]

    col_letter_by_norm: Dict[str, str] = {}
    for i, nh in preserve_indices:
        col_letter = index_to_col(start_col_idx + i)
        col_letter_by_norm[nh] = col_letter
        ranges.append(f"{safe_sheet_name(sheet_name)}!{col_letter}{data_start}:{col_letter}{data_end}")

    values_by_range = read_ranges_batch(spreadsheet_id, ranges, major_dimension="COLUMNS")
    if not values_by_range or len(values_by_range) != len(ranges):
        return {}

    sym_col_values = values_by_range[0][0] if values_by_range[0] else []
    preserve_values_map: Dict[str, List[Any]] = {}
    items = list(col_letter_by_norm.items())
    for idx, (nh, _) in enumerate(items, start=1):
        preserve_values_map[nh] = values_by_range[idx][0] if values_by_range[idx] else []

    out: Dict[str, Dict[str, Any]] = {}
    row_count = max(len(sym_col_values), max((len(v) for v in preserve_values_map.values()), default=0))
    for r in range(row_count):
        sym = ""
        if r < len(sym_col_values):
            sym = str(sym_col_values[r] or "").strip().upper()
        if not sym:
            continue
        d: Dict[str, Any] = {}
        for nh, col_vals in preserve_values_map.items():
            if r < len(col_vals):
                v = col_vals[r]
                if not _is_blank(v):
                    d[nh] = v
        if d:
            out[sym] = d

    return out


def _ensure_preserve_headers(headers: List[str], preserve_cols: List[str]) -> List[str]:
    if not preserve_cols:
        return headers
    return append_missing_headers(list(headers), list(preserve_cols))


def _apply_preserve_map(
    headers: List[str],
    rows: List[List[Any]],
    preserve_map: Dict[str, Dict[str, Any]],
    preserve_cols: List[str],
) -> List[List[Any]]:
    if not preserve_map or not preserve_cols:
        return rows

    headers = ensure_symbol_header(headers)
    sym_idx = find_symbol_col(headers)
    if sym_idx < 0:
        return rows

    tgt_idx_by_norm = {_normalize_header_key(h): i for i, h in enumerate(headers)}
    preserve_norm = [_normalize_header_key(c) for c in preserve_cols if _strip(c)]
    preserve_norm = [nh for nh in preserve_norm if nh in tgt_idx_by_norm]

    if not preserve_norm:
        return rows

    out_rows: List[List[Any]] = []
    for row in rows:
        r = list(row)
        if len(r) < len(headers):
            r += [None] * (len(headers) - len(r))
        if len(r) > len(headers):
            r = r[: len(headers)]

        sym = _strip(r[sym_idx]).upper() if sym_idx < len(r) else ""
        if sym and sym in preserve_map:
            saved = preserve_map[sym]
            for nh in preserve_norm:
                ti = tgt_idx_by_norm[nh]
                if ti < len(r) and _is_blank(r[ti]) and nh in saved:
                    r[ti] = saved[nh]
        out_rows.append(r)

    return out_rows


# ---------------------------------------------------------------------------
# Refresh core logic
# ---------------------------------------------------------------------------
def _payload_for_endpoint(symbols: List[str], sheet_name: str, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "symbols": symbols,
        "tickers": symbols,
        "sheet_name": sheet_name,
        "sheetName": sheet_name,
        "meta": {"service_version": SERVICE_VERSION, "timestamp_utc": datetime.now(timezone.utc).isoformat()},
    }
    if extra:
        payload.update(extra)
        payload["symbols"] = symbols
        payload["tickers"] = symbols
        payload["sheet_name"] = sheet_name
        payload["sheetName"] = sheet_name
    return payload


def _post_clear_trailing(
    spreadsheet_id: str,
    sheet_name: str,
    start_cell: str,
    headers_len: int,
    rows_written: int,
) -> None:
    try:
        start_col, start_row = parse_a1_cell(start_cell)
        end_col = compute_clear_end_col(start_col, headers_len) if _CONFIG.smart_clear else _CONFIG.clear_end_col

        last_written_row = start_row + rows_written
        tail_start = last_written_row + 1
        if tail_start <= _CONFIG.clear_end_row:
            tail_range = f"{safe_sheet_name(sheet_name)}!{a1(start_col, tail_start)}:{end_col}{_CONFIG.clear_end_row}"
            clear_range(spreadsheet_id, tail_range)

        if _CONFIG.smart_clear:
            try:
                end_idx = col_to_index(end_col)
                cfg_end_idx = col_to_index(_CONFIG.clear_end_col)
                if cfg_end_idx > end_idx:
                    right_start = index_to_col(end_idx + 1)
                    hdr_right_range = f"{safe_sheet_name(sheet_name)}!{a1(right_start, start_row)}:{_CONFIG.clear_end_col}{start_row}"
                    clear_range(spreadsheet_id, hdr_right_range)
            except Exception:
                pass
    except Exception as e:
        logger.warning("Post-clear failed (continuing): %s", e)


def _refresh_logic(
    endpoint: str,
    spreadsheet_id: str,
    sheet_name: str,
    tickers: Sequence[str],
    start_cell: str = "A5",
    clear: bool = False,
    value_input: str = "RAW",
    *,
    mode: Optional[str] = None,
    backend_query_params: Optional[Dict[str, Any]] = None,
    backend_payload_extra: Optional[Dict[str, Any]] = None,
    allow_empty_headers: bool = False,
    allow_empty_rows: bool = False,
) -> Dict[str, Any]:
    t0 = time.time()
    request_id = uuid.uuid4().hex[:12]
    spreadsheet_id = _strip(spreadsheet_id or _CONFIG.default_spreadsheet_id)
    sheet_name = _strip(sheet_name)
    start_cell = _strip(start_cell or _CONFIG.headers_start_cell_default) or "A5"

    try:
        if not spreadsheet_id:
            return {"status": "error", "error": "No spreadsheet ID provided", "service_version": SERVICE_VERSION, "request_id": request_id}
        if not sheet_name:
            return {"status": "error", "error": "No sheet name provided", "service_version": SERVICE_VERSION, "request_id": request_id}

        # Phase 2: normalize & block forbidden
        try:
            sheet_name = core_normalize_page(sheet_name, allow_output_pages=True)
            if sheet_name in set(core_forbidden_pages() or set()):
                return {"status": "blocked", "reason": f"Forbidden/removed page: {sheet_name}", "service_version": SERVICE_VERSION, "request_id": request_id}
        except Exception as e:
            return {"status": "blocked", "reason": f"Invalid sheet name: {e}", "service_version": SERVICE_VERSION, "request_id": request_id}

        # Phase 2: ensure tab exists + headers exactly match schema
        if _CONFIG.ensure_headers_match_schema:
            try:
                ensure_headers_and_formatting(spreadsheet_id, sheet_name, start_cell)
            except Exception as e:
                return {"status": "blocked", "reason": f"Schema/tab/header ensure failed: {e}", "sheet": sheet_name, "service_version": SERVICE_VERSION, "request_id": request_id}

        symbols = normalize_tickers(tickers)
        if not symbols:
            return {"status": "skipped", "reason": "No valid tickers provided", "endpoint": endpoint, "sheet": sheet_name, "service_version": SERVICE_VERSION, "request_id": request_id}

        query_params = dict(backend_query_params or {})
        if mode:
            query_params.setdefault("mode", mode)

        payload = _payload_for_endpoint(symbols, sheet_name, backend_payload_extra)
        response = _backend_client.call_api_chunked(endpoint, symbols, payload, query_params=query_params or None)

        validation_error = _safe_mode_validator.validate_backend_response(
            response, symbols, sheet_name, allow_empty_headers=allow_empty_headers, allow_empty_rows=allow_empty_rows
        )
        if validation_error:
            return {
                "status": "blocked",
                "reason": validation_error,
                "endpoint": endpoint,
                "sheet": sheet_name,
                "safe_mode": _CONFIG.safe_mode.value,
                "backend_status": response.get("status"),
                "backend_error": response.get("error"),
                "service_version": SERVICE_VERSION,
                "request_id": request_id,
            }

        headers = response.get("headers", []) or []
        rows = response.get("rows", []) or []

        # If backend headers missing, fall back to schema headers
        if not headers:
            headers = get_canonical_headers(sheet_name)

        headers2, rows2 = rows_to_grid(list(headers), rows)
        headers3, rows3 = reorder_to_canonical(sheet_name, headers2, rows2)

        # Ensure preserve headers exist (appended, then padded)
        headers3 = _ensure_preserve_headers(headers3, _CONFIG.preserve_columns)
        headers3 = ensure_symbol_header(headers3 or ["Symbol", "Error"])
        rows3 = pad_rows(rows3, len(headers3))

        preserve_map: Dict[str, Dict[str, Any]] = {}
        if _CONFIG.preserve_columns:
            try:
                preserve_map = _build_preserve_map(
                    spreadsheet_id,
                    sheet_name,
                    start_cell,
                    _CONFIG.preserve_columns,
                    max_rows_scan=_CONFIG.preserve_max_rows_scan,
                    read_end_col=_CONFIG.preserve_read_end_col,
                )
            except Exception as e:
                logger.warning("Preserve scan failed (continuing): %s", e)

        if preserve_map:
            rows3 = _apply_preserve_map(headers3, rows3, preserve_map, _CONFIG.preserve_columns)

        grid = [headers3] + rows3

        cells_updated = write_grid_chunked(spreadsheet_id, sheet_name, start_cell, grid, value_input=value_input)

        if clear:
            _post_clear_trailing(spreadsheet_id, sheet_name, start_cell, headers_len=len(headers3), rows_written=len(rows3))

        backend_status = (_strip(response.get("status")) or "success").lower()
        final_status = "partial" if backend_status in ("error", "partial") else "skipped" if backend_status == "skipped" else "success"

        return {
            "status": final_status,
            "sheet": sheet_name,
            "endpoint": endpoint,
            "mode": mode or "",
            "rows_written": len(rows3),
            "headers_count": len(headers3),
            "cells_updated": int(cells_updated or 0),
            "backend_status": backend_status,
            "backend_error": response.get("error"),
            "backend_chunk_size": _CONFIG.backend_max_symbols_per_call,
            "safe_mode": _CONFIG.safe_mode.value,
            "preserve_columns": list(_CONFIG.preserve_columns),
            "elapsed_ms": int((time.time() - t0) * 1000),
            "service_version": SERVICE_VERSION,
            "request_id": request_id,
        }

    except Exception as e:
        logger.exception("Refresh failed: %s", e)
        return {
            "status": "error",
            "error": str(e),
            "endpoint": endpoint,
            "sheet": sheet_name,
            "elapsed_ms": int((time.time() - t0) * 1000),
            "service_version": SERVICE_VERSION,
            "request_id": request_id,
        }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
@track_operation("refresh_enriched")
def refresh_sheet_with_enriched_quotes(
    spreadsheet_id: str = "", sheet_name: str = "", tickers: Sequence[str] = (), sid: str = "", **kwargs
) -> Dict[str, Any]:
    spreadsheet_id = _strip(spreadsheet_id or sid or _CONFIG.default_spreadsheet_id)
    return _refresh_logic("/v1/enriched/sheet-rows", spreadsheet_id, sheet_name, tickers, **kwargs)


@track_operation("refresh_ai")
def refresh_sheet_with_ai_analysis(
    spreadsheet_id: str = "", sheet_name: str = "", tickers: Sequence[str] = (), sid: str = "", **kwargs
) -> Dict[str, Any]:
    spreadsheet_id = _strip(spreadsheet_id or sid or _CONFIG.default_spreadsheet_id)
    return _refresh_logic("/v1/analysis/sheet-rows", spreadsheet_id, sheet_name, tickers, **kwargs)


@track_operation("refresh_advanced")
def refresh_sheet_with_advanced_analysis(
    spreadsheet_id: str = "", sheet_name: str = "", tickers: Sequence[str] = (), sid: str = "", **kwargs
) -> Dict[str, Any]:
    spreadsheet_id = _strip(spreadsheet_id or sid or _CONFIG.default_spreadsheet_id)
    return _refresh_logic("/v1/advanced/sheet-rows", spreadsheet_id, sheet_name, tickers, **kwargs)


# Thread-isolated async execution (reused pool)
_ASYNC_EXECUTOR = None
_ASYNC_EXECUTOR_LOCK = threading.Lock()


def _get_async_executor():
    global _ASYNC_EXECUTOR
    with _ASYNC_EXECUTOR_LOCK:
        if _ASYNC_EXECUTOR is None:
            import concurrent.futures

            _ASYNC_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
                max_workers=coerce_int(os.getenv("SHEETS_ASYNC_MAX_WORKERS", "5"), 5, lo=1, hi=50)
            )
        return _ASYNC_EXECUTOR


async def _run_async(func: Callable, *args, **kwargs) -> Any:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_get_async_executor(), lambda: func(*args, **kwargs))


async def refresh_sheet_with_enriched_quotes_async(*args, **kwargs) -> Any:
    return await _run_async(refresh_sheet_with_enriched_quotes, *args, **kwargs)


async def refresh_sheet_with_ai_analysis_async(*args, **kwargs) -> Any:
    return await _run_async(refresh_sheet_with_ai_analysis, *args, **kwargs)


async def refresh_sheet_with_advanced_analysis_async(*args, **kwargs) -> Any:
    return await _run_async(refresh_sheet_with_advanced_analysis, *args, **kwargs)



def get_rows_for_sheet(
    sheet_name: str,
    spreadsheet_id: str = "",
    *,
    limit: int = 5000,
    start_cell: str = "A5",
    sheet: Optional[str] = None,
    page: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Read data rows from a Google Sheets tab and return as a list of dicts.
    Discoverable by data_engine_v2.py external rows reader.
    """
    effective_name = _strip(sheet_name or sheet or page or "")
    if not effective_name:
        return []

    spreadsheet_id = _strip(spreadsheet_id or _CONFIG.default_spreadsheet_id)
    if not spreadsheet_id:
        logger.debug("get_rows_for_sheet: no spreadsheet_id configured")
        return []

    try:
        effective_name = core_normalize_page(effective_name, allow_output_pages=True)
    except Exception:
        pass

    try:
        start_col, start_row = parse_a1_cell(_strip(start_cell) or "A5")
        sheet_a1 = safe_sheet_name(effective_name)

        header_range = f"{sheet_a1}!{a1(start_col, start_row)}:{_CONFIG.clear_end_col}{start_row}"
        header_vals = read_range(spreadsheet_id, header_range)
        if not header_vals or not header_vals[0]:
            return []
        headers = [str(x).strip() for x in header_vals[0] if str(x).strip()]
        if not headers:
            return []

        data_start = start_row + 1
        data_end = data_start + max(1, min(int(limit or 5000), 5000)) - 1
        data_range = f"{sheet_a1}!{a1(start_col, data_start)}:{_CONFIG.clear_end_col}{data_end}"
        data_vals = read_range(spreadsheet_id, data_range)

        rows_out: List[Dict[str, Any]] = []
        for row_cells in (data_vals or []):
            if not any(str(x).strip() for x in (row_cells or [])):
                continue
            row_dict: Dict[str, Any] = {}
            for i, h in enumerate(headers):
                val = row_cells[i] if i < len(row_cells) else None
                if val is not None and str(val).strip():
                    row_dict[h] = val
            if row_dict:
                rows_out.append(row_dict)
            if len(rows_out) >= int(limit or 5000):
                break

        logger.debug("get_rows_for_sheet: %s -> %d rows", effective_name, len(rows_out))
        return rows_out

    except Exception as e:
        logger.warning("get_rows_for_sheet failed for '%s': %s", effective_name, e)
        return []


# Aliases for data_engine_v2.py rows reader discovery
read_rows_for_sheet = get_rows_for_sheet
get_sheet_rows = get_rows_for_sheet


def get_service_status() -> Dict[str, Any]:
    client = None
    try:
        client = _backend_client._get_httpx_client()
    except Exception:
        client = None

    return {
        "service_version": SERVICE_VERSION,
        "config": {
            "environment": _CONFIG.environment,
            "safe_mode": _CONFIG.safe_mode.value,
            "backend_url_configured": bool(_CONFIG.backend_base_url),
            "spreadsheet_id_configured": bool(_CONFIG.default_spreadsheet_id),
            "max_rows_per_write": _CONFIG.max_rows_per_write,
            "use_batch_update": _CONFIG.use_batch_update,
            "httpx_enabled": bool(client is not None),
            "preserve_columns": list(_CONFIG.preserve_columns),
            "preserve_max_rows_scan": _CONFIG.preserve_max_rows_scan,
            "ensure_tabs_exist": _CONFIG.ensure_tabs_exist,
            "ensure_headers_match_schema": _CONFIG.ensure_headers_match_schema,
            "init_retry_cooldown_sec": _SHEETS_INIT_RETRY_COOLDOWN_SEC,
            "allow_extra_columns": _CONFIG.allow_extra_columns,
            "insights_criteria_enabled": _CONFIG.insights_criteria_enabled,
        },
        "circuit_breaker": _backend_client.circuit_breaker.get_state(),
        "sheets_client": _sheets_client.status(),   # FIX v5.5.3: expose failure/cooldown state
        "telemetry": _telemetry.get_stats() if _CONFIG.enable_telemetry else {},
        "has_orjson": _HAS_ORJSON,
        "has_httpx": _HAS_HTTPX,
        "has_prometheus": _PROMETHEUS_AVAILABLE,
        "has_otel": _OTEL_AVAILABLE,
        "has_schema_core": _HAS_CORE,
    }


def invalidate_header_cache(sheet_name: Optional[str] = None) -> None:
    if sheet_name:
        _header_cache.invalidate(sheet_name)
    else:
        _header_cache.clear()


def close() -> None:
    try:
        _backend_client.close()
    except Exception:
        pass
    try:
        _sheets_client.close()
    except Exception:
        pass


__all__ = [
    "SERVICE_VERSION",
    "MIN_CORE_VERSION",
    "DataQuality",
    "CircuitState",
    "TokenTransport",
    "SafeModeLevel",
    "SheetsServiceConfig",
    "get_sheets_service",
    "reset_sheets_service",       # v5.5.3
    "get_rows_for_sheet",         # v5.5.3
    "read_rows_for_sheet",        # alias
    "get_sheet_rows",             # alias
    "read_range",
    "read_ranges_batch",
    "write_range",
    "clear_range",
    "write_grid_chunked",
    "ensure_canonical_tabs_exist",
    "ensure_headers_and_formatting",
    "refresh_sheet_with_enriched_quotes",
    "refresh_sheet_with_ai_analysis",
    "refresh_sheet_with_advanced_analysis",
    "refresh_sheet_with_enriched_quotes_async",
    "refresh_sheet_with_ai_analysis_async",
    "refresh_sheet_with_advanced_analysis_async",
    "get_service_status",
    "invalidate_header_cache",
    "close",
    "_refresh_logic",
]
