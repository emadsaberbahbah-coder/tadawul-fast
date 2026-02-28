#!/usr/bin/env python3
"""
integrations/google_sheets_service.py
===========================================================
GOOGLE SHEETS SERVICE FOR TADAWUL FAST BRIDGE — v5.2.0
(Emad Bahbah – Production Integration, Safety + Alignment)

Goals (v5.2.0)
- ✅ Strong alignment with scoring_engine/data_engine payloads + canonical headers
- ✅ No data loss option: preserve selected columns by Symbol (Notes, User Notes, etc.)
- ✅ SAFE MODE: blocks destructive writes on empty/mismatched backend payloads (configurable)
- ✅ httpx sync client (preferred) + urllib fallback (no hard dependency)
- ✅ orjson fast JSON (safe default=str) + json fallback
- ✅ OpenTelemetry + Prometheus optional (safe fallbacks; never raises)
- ✅ AWS Full-Jitter exponential backoff (Sheets + Backend)
- ✅ Chunked backend calls + correct merge (maps per-chunk rows into final header schema)
- ✅ Chunked sheet writes + batchUpdate (with safe fallback to sequential updates)
- ✅ Hygiene-friendly (no print usage in library paths; only logger)

Public API
- get_sheets_service()
- refresh_sheet_with_enriched_quotes / _async
- refresh_sheet_with_ai_analysis / _async
- refresh_sheet_with_advanced_analysis / _async
- get_service_status(), invalidate_header_cache(), close()

Environment (main)
- DEFAULT_SPREADSHEET_ID
- BACKEND_BASE_URL
- APP_TOKEN or BACKEND_TOKEN
- GOOGLE_SHEETS_CREDENTIALS / GOOGLE_CREDENTIALS (JSON)
- GOOGLE_SHEETS_CREDENTIALS_B64 / GOOGLE_CREDENTIALS_B64 (base64 JSON)
- GOOGLE_APPLICATION_CREDENTIALS / GOOGLE_SHEETS_CREDENTIALS_FILE (file)
- SHEETS_PRESERVE_COLUMNS="Notes,User Notes"
- SHEETS_SAFE_MODE=off|basic|strict|paranoid
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
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache, wraps
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

# ---------------------------------------------------------------------------
# Optional high-performance libs
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
        return orjson.loads(v)

    _HAS_ORJSON = True
except Exception:
    import json  # type: ignore

    def json_dumps_str(v: Any, *, default: Optional[Callable] = None) -> str:
        return json.dumps(v, default=default or str, ensure_ascii=False)

    def json_dumps_bytes(v: Any, *, default: Optional[Callable] = None) -> bytes:
        return json.dumps(v, default=default or str, ensure_ascii=False).encode("utf-8")

    def json_loads(v: Any) -> Any:
        return json.loads(v)

    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Monitoring & Tracing (Optional, safe fallback)
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Histogram  # type: ignore

    _PROMETHEUS_AVAILABLE = True
    sheets_operations_total = Counter("sheets_operations_total", "Total Sheets operations", ["operation", "status"])
    sheets_operation_duration = Histogram("sheets_operation_duration_seconds", "Sheets operation duration", ["operation"])
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
                try:
                    if hasattr(self._span, "set_attributes"):
                        self._span.set_attributes(self.attributes)
                    else:
                        for k, v in self.attributes.items():
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
SERVICE_VERSION = "5.2.0"
MIN_CORE_VERSION = "5.0.0"

logger = logging.getLogger("integrations.google_sheets_service")
logger.addHandler(logging.NullHandler())

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}

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
# Core schema integration (optional)
# ---------------------------------------------------------------------------
try:
    from core.schemas import get_headers_for_sheet  # type: ignore
    from core.symbols.normalize import normalize_symbol as core_normalize_symbol  # type: ignore

    _HAS_CORE = True
except Exception:
    _HAS_CORE = False

    def get_headers_for_sheet(name: str) -> List[str]:
        return []

    def core_normalize_symbol(sym: str) -> str:
        return (sym or "").strip().upper()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def _parse_csv_env(name: str, default: str = "") -> List[str]:
    raw = (os.getenv(name, "") or default).strip()
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]

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
    backend_token_transport: Set[TokenTransport] = field(default_factory=lambda: {TokenTransport.HEADER, TokenTransport.BEARER})
    backend_token_param_name: str = "token"
    backend_circuit_breaker_threshold: int = 5
    backend_circuit_breaker_timeout: int = 60

    # Write operations
    max_rows_per_write: int = 500
    use_batch_update: bool = True
    max_batch_ranges: int = 25
    clear_end_col: str = "ZZ"
    clear_end_row: int = 100000
    smart_clear: bool = True

    # SAFE MODE
    safe_mode: SafeModeLevel = SafeModeLevel.STRICT
    block_on_empty_headers: bool = True
    block_on_empty_rows: bool = False
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

    # Zero-data-loss
    preserve_columns: List[str] = field(default_factory=list)

    # Env
    environment: str = "prod"
    user_agent: str = f"TadawulFastBridge-SheetsService/{SERVICE_VERSION}"

    @classmethod
    def from_env(cls) -> "SheetsServiceConfig":
        env = (os.getenv("APP_ENV", "prod") or "prod").lower().strip()

        safe_mode_str = (os.getenv("SHEETS_SAFE_MODE", "strict") or "strict").lower().strip()
        if safe_mode_str in ("off", "false", "0"):
            safe_mode = SafeModeLevel.OFF
        elif safe_mode_str in ("basic",):
            safe_mode = SafeModeLevel.BASIC
        elif safe_mode_str in ("paranoid", "maximum"):
            safe_mode = SafeModeLevel.PARANOID
        else:
            safe_mode = SafeModeLevel.STRICT

        token_transport: Set[TokenTransport] = set()
        transport_str = (os.getenv("SHEETS_BACKEND_TOKEN_TRANSPORT", "header,bearer") or "header,bearer").lower()
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

        preserve_cols = _parse_csv_env("SHEETS_PRESERVE_COLUMNS", default="")

        return cls(
            default_spreadsheet_id=(os.getenv("DEFAULT_SPREADSHEET_ID", "") or "").strip(),
            sheets_api_retries=int(os.getenv("SHEETS_API_RETRIES", "4") or "4"),
            sheets_api_retry_base_sleep=float(os.getenv("SHEETS_API_RETRY_BASE_SLEEP", "1.0") or "1.0"),
            sheets_api_timeout_sec=float(os.getenv("SHEETS_API_TIMEOUT_SEC", "60") or "60"),
            backend_base_url=(os.getenv("BACKEND_BASE_URL", "") or "").rstrip("/"),
            backend_timeout_sec=float(os.getenv("SHEETS_BACKEND_TIMEOUT_SEC", "120") or "120"),
            backend_retries=int(os.getenv("SHEETS_BACKEND_RETRIES", "3") or "3"),
            backend_retry_sleep=float(os.getenv("SHEETS_BACKEND_RETRY_SLEEP", "1.0") or "1.0"),
            backend_max_symbols_per_call=int(os.getenv("SHEETS_BACKEND_MAX_SYMBOLS_PER_CALL", "200") or "200"),
            backend_token_transport=token_transport or {TokenTransport.HEADER, TokenTransport.BEARER},
            backend_token_param_name=(os.getenv("SHEETS_BACKEND_TOKEN_QUERY_PARAM", "token") or "token").strip(),
            backend_circuit_breaker_threshold=int(os.getenv("BACKEND_CIRCUIT_BREAKER_THRESHOLD", "5") or "5"),
            backend_circuit_breaker_timeout=int(os.getenv("BACKEND_CIRCUIT_BREAKER_TIMEOUT", "60") or "60"),
            max_rows_per_write=int(os.getenv("SHEETS_MAX_ROWS_PER_WRITE", "500") or "500"),
            use_batch_update=(os.getenv("SHEETS_USE_BATCH_UPDATE", "true") or "true").lower() in _TRUTHY,
            max_batch_ranges=int(os.getenv("SHEETS_MAX_BATCH_RANGES", "25") or "25"),
            clear_end_col=(os.getenv("SHEETS_CLEAR_END_COL", "ZZ") or "ZZ").strip().upper(),
            clear_end_row=int(os.getenv("SHEETS_CLEAR_END_ROW", "100000") or "100000"),
            smart_clear=(os.getenv("SHEETS_SMART_CLEAR", "true") or "true").lower() in _TRUTHY,
            safe_mode=safe_mode,
            block_on_empty_headers=(os.getenv("SHEETS_BLOCK_ON_EMPTY_HEADERS", "true") or "true").lower() in _TRUTHY,
            block_on_empty_rows=(os.getenv("SHEETS_BLOCK_ON_EMPTY_ROWS", "false") or "false").lower() in _TRUTHY,
            block_on_data_mismatch=(os.getenv("SHEETS_BLOCK_ON_DATA_MISMATCH", "true") or "true").lower() in _TRUTHY,
            validate_row_count=(os.getenv("SHEETS_VALIDATE_ROW_COUNT", "true") or "true").lower() in _TRUTHY,
            require_symbol_header=(os.getenv("SHEETS_REQUIRE_SYMBOL_HEADER", "true") or "true").lower() in _TRUTHY,
            cache_ttl_seconds=int(os.getenv("SHEETS_CACHE_TTL_SECONDS", "300") or "300"),
            enable_header_cache=(os.getenv("SHEETS_ENABLE_HEADER_CACHE", "true") or "true").lower() in _TRUTHY,
            connection_pool_size=int(os.getenv("SHEETS_CONNECTION_POOL_SIZE", "20") or "20"),
            keep_alive_seconds=int(os.getenv("SHEETS_KEEP_ALIVE_SECONDS", "30") or "30"),
            verify_ssl=(os.getenv("SHEETS_VERIFY_SSL", "true") or "true").lower() in _TRUTHY,
            ssl_cert_path=os.getenv("SHEETS_SSL_CERT_PATH", None),
            enable_telemetry=(os.getenv("SHEETS_ENABLE_TELEMETRY", "true") or "true").lower() in _TRUTHY,
            preserve_columns=preserve_cols,
            environment=env,
            user_agent=(os.getenv("SHEETS_USER_AGENT", f"TadawulFastBridge-SheetsService/{SERVICE_VERSION}") or f"TadawulFastBridge-SheetsService/{SERVICE_VERSION}"),
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

    def reset(self) -> None:
        with self._lock:
            self._metrics.clear()
            self._counters.clear()
            self._latencies.clear()

_telemetry = TelemetryCollector()

def track_operation(operation: str):
    """
    Decorator for sync public API calls.
    Captures sheet name from kwargs or positional args (2nd arg in our public refresh signatures).
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            sheet_name = kwargs.get("sheet_name")
            if not sheet_name and len(args) >= 2:
                try:
                    sheet_name = args[1]
                except Exception:
                    sheet_name = None

            try:
                with TraceContext(f"sheets_{operation}", {"sheet_name": sheet_name or ""}):
                    result = func(*args, **kwargs)

                rows_written = (result.get("rows_written", 0) if isinstance(result, dict) else 0)
                cells_updated = (result.get("cells_updated", 0) if isinstance(result, dict) else 0)
                _telemetry.record_operation(
                    OperationMetrics(
                        operation=operation,
                        start_time=start,
                        end_time=time.time(),
                        success=True,
                        rows_processed=int(rows_written or 0),
                        cells_updated=int(cells_updated or 0),
                        sheet_name=sheet_name,
                    )
                )
                sheets_operations_total.labels(operation=operation, status="success").inc()
                sheets_operation_duration.labels(operation=operation).observe(time.time() - start)
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
                        sheet_name=sheet_name,
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
    def __init__(self, threshold: int = _CONFIG.backend_circuit_breaker_threshold, timeout: int = _CONFIG.backend_circuit_breaker_timeout):
        self.threshold = max(1, int(threshold))
        self.timeout = max(1, int(timeout))
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
# Credentials + Google Sheets client
# ---------------------------------------------------------------------------
class CredentialsManager:
    def __init__(self):
        self._creds_info: Optional[Dict[str, Any]] = None
        self._lock = threading.RLock()

    def get_credentials(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            if self._creds_info is not None:
                return dict(self._creds_info)

            for src in (self._from_env_json, self._from_env_base64, self._from_env_file, self._from_settings):
                creds = src()
                if creds:
                    self._creds_info = self._normalize_credentials(creds)
                    return dict(self._creds_info)

            logger.error("No valid Google credentials found")
            return None

    def _normalize_credentials(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(creds)
        pk = out.get("private_key")
        if isinstance(pk, str) and pk:
            pk2 = pk.replace("\\n", "\n")
            if "-----BEGIN PRIVATE KEY-----" not in pk2:
                pk2 = "-----BEGIN PRIVATE KEY-----\n" + pk2.strip() + "\n-----END PRIVATE KEY-----\n"
            out["private_key"] = pk2
        return out

    def _from_env_json(self) -> Optional[Dict[str, Any]]:
        raw = (os.getenv("GOOGLE_SHEETS_CREDENTIALS", "") or os.getenv("GOOGLE_CREDENTIALS", "") or "").strip()
        if raw.startswith("{"):
            try:
                obj = json_loads(raw)
                return obj if isinstance(obj, dict) else None
            except Exception:
                return None
        return None

    def _from_env_base64(self) -> Optional[Dict[str, Any]]:
        raw = (os.getenv("GOOGLE_SHEETS_CREDENTIALS_B64", "") or os.getenv("GOOGLE_CREDENTIALS_B64", "") or "").strip()
        if not raw:
            return None
        try:
            decoded = base64.b64decode(raw).decode("utf-8", errors="replace")
            obj = json_loads(decoded)
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None

    def _from_env_file(self) -> Optional[Dict[str, Any]]:
        path = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "") or os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", "") or "").strip()
        if not path:
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                obj = json_loads(f.read())
                return obj if isinstance(obj, dict) else None
        except Exception as e:
            logger.warning("Failed to load credentials from file %s: %s", path, e)
            return None

    def _from_settings(self) -> Optional[Dict[str, Any]]:
        try:
            from core.config import get_settings  # type: ignore

            settings = get_settings()
            raw = getattr(settings, "google_sheets_credentials_json", None)
            if isinstance(raw, str) and raw.strip().startswith("{"):
                try:
                    obj = json_loads(raw.strip())
                    if isinstance(obj, dict):
                        return obj
                except Exception:
                    pass

            d = getattr(settings, "google_credentials_dict", None)
            if isinstance(d, dict):
                return dict(d)
        except Exception:
            pass
        return None

_credentials_manager = CredentialsManager()

class SheetsAPIClient:
    def __init__(self):
        self._service = None
        self._lock = threading.RLock()
        self._failed = False

    def get_service(self):
        with self._lock:
            if self._service is not None:
                return self._service
            if self._failed:
                raise RuntimeError("Sheets API client initialization failed previously")
            self._service = self._create_service()
            return self._service

    def _create_service(self):
        try:
            from google.oauth2.service_account import Credentials  # type: ignore
            from googleapiclient.discovery import build  # type: ignore
        except Exception as e:
            self._failed = True
            raise RuntimeError("Missing Google API libs. Install: google-api-python-client google-auth") from e

        creds_info = _credentials_manager.get_credentials()
        if not creds_info:
            self._failed = True
            raise RuntimeError("Missing Google service account credentials")

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(creds_info, scopes=scopes)

        service = build(
            "sheets",
            "v4",
            credentials=creds,
            cache_discovery=False,
            num_retries=_CONFIG.sheets_api_retries,
        )
        return service

    def close(self):
        # googleapiclient doesn't require explicit close in most cases
        return None

_sheets_client = SheetsAPIClient()

def get_sheets_service():
    """Public getter (exported + used)."""
    return _sheets_client.get_service()

# ---------------------------------------------------------------------------
# Header cache + canonical headers
# ---------------------------------------------------------------------------
class HeaderCache:
    def __init__(self, ttl: int = _CONFIG.cache_ttl_seconds):
        self.ttl = int(ttl)
        self._cache: Dict[str, Tuple[List[str], float]] = {}
        self._lock = threading.RLock()

    def get(self, sheet_name: str) -> Optional[List[str]]:
        if not _CONFIG.enable_header_cache:
            return None
        key = (sheet_name or "").strip().upper()
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
        key = (sheet_name or "").strip().upper()
        with self._lock:
            self._cache[key] = (list(headers), time.time())

    def invalidate(self, sheet_name: str) -> None:
        key = (sheet_name or "").strip().upper()
        with self._lock:
            self._cache.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

_header_cache = HeaderCache()

def get_canonical_headers(sheet_name: str) -> List[str]:
    cached = _header_cache.get(sheet_name)
    if cached is not None:
        return cached
    headers: List[str] = []
    if _HAS_CORE:
        try:
            headers = get_headers_for_sheet(sheet_name) or []
        except Exception as e:
            logger.warning("Failed to get headers from core.schemas: %s", e)
    if headers:
        _header_cache.set(sheet_name, headers)
    return headers

# ---------------------------------------------------------------------------
# A1 utilities
# ---------------------------------------------------------------------------
_A1_RE = re.compile(r"^\$?([A-Za-z]+)\$?(\d+)$")

@lru_cache(maxsize=1024)
def parse_a1_cell(cell: str) -> Tuple[str, int]:
    s = (cell or "").strip()
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
    col = (col or "").strip().upper()
    if not col:
        return 1
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
    name = (name or "").strip() or "Sheet1"
    name = name.replace("'", "''")
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
        s = str(symbol or "").strip()
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
        if not t or not isinstance(t, str):
            continue
        norm = _normalizer.normalize(t)
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
    s = str(header or "").strip().lower()
    return re.sub(r"[^a-z0-9]", "", s)

_HEADER_ALIAS_MAP: Dict[str, str] = {
    # identity
    "symbol": "Symbol",
    "ticker": "Symbol",
    "requestedsymbol": "Symbol",

    # scoring_engine aligned (snake_case -> pretty)
    "value_score": "Value Score",
    "quality_score": "Quality Score",
    "momentum_score": "Momentum Score",
    "risk_score": "Risk Score",
    "opportunity_score": "Opportunity Score",
    "overall_score": "Overall Score",
    "recommendation": "Recommendation",

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
    "value_badge": "Value Badge",
    "quality_badge": "Quality Badge",
    "momentum_badge": "Momentum Badge",
    "risk_badge": "Risk Badge",
    "opportunity_badge": "Opportunity Badge",
}
_HEADER_ALIAS_MAP_NORM = {_normalize_header_key(k): v for k, v in _HEADER_ALIAS_MAP.items()}

def append_missing_headers(headers: List[str], extra: Sequence[str]) -> List[str]:
    seen = {_normalize_header_key(h) for h in headers}
    out = list(headers)
    for h in extra:
        if _normalize_header_key(h) not in seen:
            seen.add(_normalize_header_key(h))
            out.append(h)
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

def rows_to_grid(headers: List[str], rows: Any) -> Tuple[List[str], List[List[Any]]]:
    if not headers:
        headers = ["Symbol", "Error"]
    fixed_headers = [str(h).strip() for h in headers if str(h).strip()]
    fixed_rows: List[List[Any]] = []

    if not rows:
        return fixed_headers, fixed_rows

    # dict rows
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        header_lookup = {_normalize_header_key(h): i for i, h in enumerate(fixed_headers)}
        for row_dict in rows:
            if not isinstance(row_dict, dict):
                continue
            row_data = [None] * len(fixed_headers)
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
        return fixed_headers, fixed_rows

    # list rows
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, (list, tuple)):
                row = [row]
            row_list = list(row)
            if len(row_list) < len(fixed_headers):
                row_list += [None] * (len(fixed_headers) - len(row_list))
            elif len(row_list) > len(fixed_headers):
                row_list = row_list[: len(fixed_headers)]
            fixed_rows.append(row_list)
        return fixed_headers, fixed_rows

    fixed_rows.append([rows] + [None] * (len(fixed_headers) - 1))
    return fixed_headers, fixed_rows

def reorder_to_canonical(sheet_name: str, headers: List[str], rows: List[List[Any]]) -> Tuple[List[str], List[List[Any]]]:
    canonical = get_canonical_headers(sheet_name)
    if not canonical:
        return headers, rows

    src_index = {_normalize_header_key(h): i for i, h in enumerate(headers)}
    canonical_norm = {_normalize_header_key(h) for h in canonical}
    extra_headers = [h for h in headers if _normalize_header_key(h) not in canonical_norm]

    new_headers = list(canonical) + extra_headers
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

    dst_key_to_idx = {_normalize_header_key(h): i for i, h in enumerate(dst_headers)}
    src_key_to_idx = {_normalize_header_key(h): i for i, h in enumerate(sh)}

    # also allow alias resolution from src->dst
    for src_k_norm, dst_name in _HEADER_ALIAS_MAP_NORM.items():
        dst_k_norm = _normalize_header_key(dst_name)
        if src_k_norm in src_key_to_idx and dst_k_norm in dst_key_to_idx:
            # already covered by key mapping via normalization, but keep for robustness
            pass

    out: List[List[Any]] = []
    for row in sr:
        new_row = [None] * len(dst_headers)
        for src_k_norm, si in src_key_to_idx.items():
            if si >= len(row):
                continue
            # direct same-key mapping
            if src_k_norm in dst_key_to_idx:
                new_row[dst_key_to_idx[src_k_norm]] = row[si]
                continue
            # alias mapping
            alias_dst = _HEADER_ALIAS_MAP_NORM.get(src_k_norm)
            if alias_dst:
                alias_dst_norm = _normalize_header_key(alias_dst)
                if alias_dst_norm in dst_key_to_idx:
                    new_row[dst_key_to_idx[alias_dst_norm]] = row[si]
        out.append(new_row)
    return out

# ---------------------------------------------------------------------------
# Google Sheets operations (Retry + Full Jitter)
# ---------------------------------------------------------------------------
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
            base = float(_CONFIG.sheets_api_retry_base_sleep) * (2 ** attempt)
            sleep_time = random.uniform(0.0, min(15.0, base))
            logger.warning("Sheets API %s retry %s/%s in %.2fs: %s", operation_name, attempt + 1, retries, sleep_time, e)
            time.sleep(sleep_time)
    raise last_exc  # type: ignore

def read_range(spreadsheet_id: str, range_name: str) -> List[List[Any]]:
    service = get_sheets_service()

    def _read():
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            majorDimension="ROWS",
        ).execute()
        return result.get("values", []) or []

    return _retry_sheet_op("Read Range", _read)

def write_range(spreadsheet_id: str, range_name: str, values: List[List[Any]], value_input: str = "RAW") -> int:
    service = get_sheets_service()
    body = {"values": values or [[]]}

    def _write():
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption=value_input,
            body=body,
        ).execute()
        return int(result.get("updatedCells", 0) or 0)

    return _retry_sheet_op("Write Range", _write)

def clear_range(spreadsheet_id: str, range_name: str) -> None:
    service = get_sheets_service()

    def _clear():
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=range_name,
        ).execute()

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

    chunks = [fixed_rows[i : i + _CONFIG.max_rows_per_write] for i in range(0, len(fixed_rows), _CONFIG.max_rows_per_write)]
    total_cells = 0

    # Batch update path
    if _CONFIG.use_batch_update:
        try:
            service = get_sheets_service()
            batch_data = []

            # header + first chunk in one request
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
                    result = service.spreadsheets().values().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body=body,
                    ).execute()
                    responses = result.get("responses", []) or []
                    return sum(int(resp.get("updatedCells", 0) or 0) for resp in responses)

                total_cells += _retry_sheet_op("Batch Write", _batch_update)

            return total_cells
        except Exception as e:
            logger.warning("Batch update failed, falling back to sequential: %s", e)

    # Sequential fallback
    rng0 = f"{sheet_a1}!{a1(start_col, start_row)}"
    total_cells += write_range(spreadsheet_id, rng0, [header] + (chunks[0] if chunks else []), value_input=value_input)

    current_row = start_row + 1 + (len(chunks[0]) if chunks else 0)
    for chunk in chunks[1:]:
        rng = f"{sheet_a1}!{a1(start_col, current_row)}"
        total_cells += write_range(spreadsheet_id, rng, chunk, value_input=value_input)
        current_row += len(chunk)

    return total_cells

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
        headers = response.get("headers", []) or []
        rows = response.get("rows", []) or []

        if self.config.safe_mode == SafeModeLevel.OFF:
            return None

        if self.config.block_on_empty_headers and (not headers) and (not allow_empty_headers):
            return "SAFE MODE: Backend returned empty headers"

        if self.config.block_on_empty_rows and (not rows) and (not allow_empty_rows):
            return "SAFE MODE: Backend returned empty rows"

        if headers and self.config.require_symbol_header:
            if find_symbol_col(headers) < 0:
                return "SAFE MODE: Backend headers missing 'Symbol' column"

        if self.config.safe_mode == SafeModeLevel.BASIC:
            return None

        if self.config.block_on_data_mismatch and headers and rows:
            for i, row in enumerate(rows[:5]):
                try:
                    if isinstance(row, list) and len(row) != len(headers):
                        return f"SAFE MODE: Row {i+1} length ({len(row)}) doesn't match headers ({len(headers)})"
                except Exception:
                    continue

        if self.config.safe_mode == SafeModeLevel.STRICT:
            return None

        # PARANOID checks
        if self.config.validate_row_count and headers and requested_symbols:
            symbol_col = find_symbol_col(headers)
            if symbol_col >= 0:
                returned: Set[str] = set()
                for row in rows:
                    if isinstance(row, list) and symbol_col < len(row):
                        sym = str(row[symbol_col] or "").strip().upper()
                        if sym:
                            returned.add(sym)
                req = set(requested_symbols)
                missing = req - returned
                if missing and len(missing) > len(req) * 0.5:
                    return f"SAFE MODE: Missing >50% of requested symbols ({len(missing)}/{len(req)})"

        return None

_safe_mode_validator = SafeModeValidator(_CONFIG)

# ---------------------------------------------------------------------------
# Backend API client (httpx preferred, urllib fallback)
# ---------------------------------------------------------------------------
class BackendAPIClient:
    def __init__(self):
        self.circuit_breaker = BackendCircuitBreaker()
        self._ssl_ctx = self._create_ssl_context()

        self._httpx_client = None
        if _HAS_HTTPX and httpx is not None:
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

    def close(self) -> None:
        try:
            if self._httpx_client is not None:
                self._httpx_client.close()
        except Exception:
            pass

    def _get_token(self) -> str:
        return (os.getenv("APP_TOKEN", "") or os.getenv("BACKEND_TOKEN", "") or "").strip()

    def _build_headers(self, token: str) -> Dict[str, str]:
        headers = {"Content-Type": "application/json", "Accept": "application/json", "User-Agent": _CONFIG.user_agent}
        if not token:
            return headers
        if TokenTransport.HEADER in _CONFIG.backend_token_transport:
            headers["X-APP-TOKEN"] = token
        if TokenTransport.BEARER in _CONFIG.backend_token_transport:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _compose_url_and_params(self, endpoint: str, token: str, query_params: Optional[Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
        base_url = _CONFIG.backend_base_url.rstrip("/")
        url = f"{base_url}{endpoint}"
        params: Dict[str, Any] = dict(query_params or {})
        if token and TokenTransport.QUERY in _CONFIG.backend_token_transport:
            params[_CONFIG.backend_token_param_name] = token
        return url, params

    @staticmethod
    def _sleep_full_jitter(base_sleep: float, cap: float = 30.0) -> None:
        time.sleep(random.uniform(0.0, min(cap, base_sleep)))

    def _error_payload(self, symbols: List[str], error: str) -> Dict[str, Any]:
        return {
            "status": "error",
            "error": error,
            "headers": ["Symbol", "Error"],
            "rows": [[s, error] for s in (symbols or [])],
        }

    def call_api(self, endpoint: str, payload: Dict[str, Any], query_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        with TraceContext("backend_api_call", {"endpoint": endpoint}):
            if not self.circuit_breaker.can_execute():
                st = self.circuit_breaker.get_state()
                return self._error_payload(payload.get("symbols", []) or payload.get("tickers", []) or [], f"Circuit breaker {st['state']}")

            base_url = _CONFIG.backend_base_url.rstrip("/")
            if not base_url:
                return self._error_payload(payload.get("symbols", []) or payload.get("tickers", []) or [], "No backend URL configured")

            symbols = payload.get("symbols", []) or payload.get("tickers", []) or []
            token = self._get_token()
            url, params = self._compose_url_and_params(endpoint, token, query_params)

            try:
                body_bytes = json_dumps_bytes(payload, default=str)
            except Exception as e:
                self.circuit_breaker.record_failure()
                return self._error_payload(symbols, f"JSON encode error: {e}")

            last_error: Optional[str] = None
            retries = max(0, int(_CONFIG.backend_retries))

            for attempt in range(retries + 1):
                try:
                    # httpx path
                    if self._httpx_client is not None:
                        resp = self._httpx_client.post(
                            url,
                            params=params or None,
                            content=body_bytes,
                            headers=self._build_headers(token),
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
                            return data

                        raw_snip = ""
                        try:
                            raw_snip = (resp.text or "")[:200]
                        except Exception:
                            raw_snip = ""
                        last_error = f"HTTP {status}: {raw_snip}"

                        if status in (429, 500, 502, 503, 504) and attempt < retries:
                            self._sleep_full_jitter(float(_CONFIG.backend_retry_sleep) * (2 ** attempt), cap=30.0)
                            continue

                        self.circuit_breaker.record_failure()
                        return self._error_payload(symbols, last_error)

                    # urllib fallback
                    request_url = url
                    if params:
                        parsed = urllib.parse.urlsplit(request_url)
                        q = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
                        q.update(params)
                        request_url = urllib.parse.urlunsplit(
                            (parsed.scheme, parsed.netloc, parsed.path, urllib.parse.urlencode(q, doseq=True), parsed.fragment)
                        )

                    req = urllib.request.Request(request_url, data=body_bytes, headers=self._build_headers(token), method="POST")
                    with urllib.request.urlopen(req, timeout=float(_CONFIG.backend_timeout_sec), context=self._ssl_ctx) as resp2:
                        raw2 = resp2.read()
                        status2 = int(getattr(resp2, "status", 200) or 200)

                        if 200 <= status2 < 300:
                            try:
                                data2 = json_loads(raw2)
                            except Exception as e:
                                self.circuit_breaker.record_failure()
                                return self._error_payload(symbols, f"Invalid JSON: {e}")

                            self.circuit_breaker.record_success()
                            if not isinstance(data2, dict):
                                data2 = {}
                            data2.setdefault("status", "success")
                            data2.setdefault("headers", [])
                            data2.setdefault("rows", [])
                            return data2

                        try:
                            snip = raw2.decode("utf-8", errors="replace")[:200]
                        except Exception:
                            snip = ""
                        last_error = f"HTTP {status2}: {snip}"

                        if status2 in (429, 500, 502, 503, 504) and attempt < retries:
                            self._sleep_full_jitter(float(_CONFIG.backend_retry_sleep) * (2 ** attempt), cap=30.0)
                            continue

                        self.circuit_breaker.record_failure()
                        return self._error_payload(symbols, last_error)

                except Exception as e:
                    last_error = f"Request error: {e}"
                    if attempt < retries:
                        self._sleep_full_jitter(float(_CONFIG.backend_retry_sleep) * (2 ** attempt), cap=30.0)
                        continue
                    self.circuit_breaker.record_failure()
                    return self._error_payload(symbols, last_error)

            self.circuit_breaker.record_failure()
            return self._error_payload(symbols, last_error or "Max retries exceeded")

    def call_api_chunked(self, endpoint: str, symbols: List[str], base_payload: Dict[str, Any], query_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if len(symbols) <= _CONFIG.backend_max_symbols_per_call:
            payload = dict(base_payload)
            payload["symbols"] = symbols
            payload["tickers"] = symbols
            return self.call_api(endpoint, payload, query_params)

        chunks = [symbols[i : i + _CONFIG.backend_max_symbols_per_call] for i in range(0, len(symbols), _CONFIG.backend_max_symbols_per_call)]
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
        if not all_headers:
            all_headers = ["Symbol", "Error"]

        sym_idx = find_symbol_col(all_headers)
        err_idx = find_error_col(all_headers)
        row_map: Dict[str, List[Any]] = {}

        for resp in responses:
            resp_status = (resp.get("status") or "success").lower()
            if resp_status in ("error", "partial"):
                status = "partial"
            if resp_status == "error" and not error_msg:
                error_msg = resp.get("error")

            h = resp.get("headers") or []
            r = resp.get("rows") or []
            if not h or not r:
                continue

            # Remap chunk rows to all_headers order BEFORE storing
            remapped_rows = remap_rows_to_headers(h, r, all_headers)
            chunk_sym_idx = find_symbol_col(all_headers)
            if chunk_sym_idx < 0:
                continue

            for row in remapped_rows:
                if chunk_sym_idx < len(row):
                    sym = str(row[chunk_sym_idx] or "").strip().upper()
                    if sym:
                        row_map[sym] = row

        final_rows: List[List[Any]] = []
        for sym in requested_symbols:
            if sym in row_map:
                row = list(row_map[sym])
                if len(row) < len(all_headers):
                    row += [None] * (len(all_headers) - len(row))
                elif len(row) > len(all_headers):
                    row = row[: len(all_headers)]
                final_rows.append(row)
            else:
                placeholder = [None] * len(all_headers)
                if sym_idx >= 0:
                    placeholder[sym_idx] = sym
                if err_idx >= 0:
                    placeholder[err_idx] = "No data from backend"
                final_rows.append(placeholder)
                status = "partial"

        return {"status": status, "headers": all_headers, "rows": final_rows, "error": error_msg}

_backend_client = BackendAPIClient()

# ---------------------------------------------------------------------------
# Preserve Columns (Zero Data Loss) — by Symbol
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
    max_rows_scan: int = 20000,
) -> Dict[str, Dict[str, Any]]:
    """
    Reads existing grid at start_cell and builds:
    { SYMBOL: { normalized_header_key: value, ... } }
    Uses EXISTING header row from sheet (not assumed).
    """
    preserve_norm = {_normalize_header_key(c) for c in preserve_cols if c}
    if not preserve_norm:
        return {}

    start_col, start_row = parse_a1_cell(start_cell)
    # Read a bounded window
    end_col = _CONFIG.clear_end_col
    end_row = start_row + max_rows_scan
    rng = f"{safe_sheet_name(sheet_name)}!{a1(start_col, start_row)}:{end_col}{end_row}"

    existing = read_range(spreadsheet_id, rng)
    if not existing or len(existing) < 2:
        return {}

    existing_headers = [str(x).strip() for x in (existing[0] or []) if str(x).strip()]
    if not existing_headers:
        return {}

    sym_idx = find_symbol_col(existing_headers)
    if sym_idx < 0:
        return {}

    col_indices: List[Tuple[int, str]] = []
    for i, h in enumerate(existing_headers):
        nh = _normalize_header_key(h)
        if nh in preserve_norm:
            col_indices.append((i, nh))

    if not col_indices:
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for row in existing[1:]:
        if not isinstance(row, list):
            continue
        if sym_idx >= len(row):
            continue
        sym = str(row[sym_idx] or "").strip().upper()
        if not sym:
            continue
        d: Dict[str, Any] = {}
        for i, nh in col_indices:
            if i < len(row):
                v = row[i]
                if not _is_blank(v):
                    d[nh] = v
        if d:
            out[sym] = d

    return out

def _apply_preserve_map(
    headers: List[str],
    rows: List[List[Any]],
    preserve_map: Dict[str, Dict[str, Any]],
    preserve_cols: List[str],
) -> List[List[Any]]:
    if not preserve_map or not preserve_cols:
        return rows

    sym_idx = find_symbol_col(headers)
    if sym_idx < 0:
        return rows

    # Map target header normalized keys -> target index
    tgt_idx_by_norm = {_normalize_header_key(h): i for i, h in enumerate(headers)}
    # Determine which normalized preserve keys exist in target
    preserve_norm = {_normalize_header_key(c) for c in preserve_cols if c}
    active_preserve = [nh for nh in preserve_norm if nh in tgt_idx_by_norm]

    if not active_preserve:
        return rows

    out_rows: List[List[Any]] = []
    for row in rows:
        r = list(row)
        sym = ""
        if sym_idx < len(r):
            sym = str(r[sym_idx] or "").strip().upper()

        if sym and sym in preserve_map:
            saved = preserve_map[sym]
            for nh in active_preserve:
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
    spreadsheet_id = (spreadsheet_id or _CONFIG.default_spreadsheet_id).strip()
    sheet_name = (sheet_name or "").strip()

    try:
        if not spreadsheet_id:
            return {"status": "error", "error": "No spreadsheet ID provided", "service_version": SERVICE_VERSION}
        if not sheet_name:
            return {"status": "error", "error": "No sheet name provided", "service_version": SERVICE_VERSION}

        symbols = normalize_tickers(tickers)
        if not symbols:
            return {
                "status": "skipped",
                "reason": "No valid tickers provided",
                "endpoint": endpoint,
                "sheet": sheet_name,
                "service_version": SERVICE_VERSION,
            }

        query_params = dict(backend_query_params or {})
        if mode:
            query_params.setdefault("mode", mode)

        payload = _payload_for_endpoint(symbols, sheet_name, backend_payload_extra)
        response = _backend_client.call_api_chunked(endpoint, symbols, payload, query_params=query_params or None)

        validation_error = _safe_mode_validator.validate_backend_response(
            response,
            symbols,
            sheet_name,
            allow_empty_headers=allow_empty_headers,
            allow_empty_rows=allow_empty_rows,
        )
        if validation_error:
            return {
                "status": "blocked",
                "reason": validation_error,
                "endpoint": endpoint,
                "sheet": sheet_name,
                "service_version": SERVICE_VERSION,
            }

        headers = response.get("headers", []) or []
        rows = response.get("rows", []) or []

        if not headers:
            headers = get_canonical_headers(sheet_name)

        headers2, rows2 = rows_to_grid(headers, rows)
        headers3, rows3 = reorder_to_canonical(sheet_name, headers2, rows2)
        if not headers3:
            headers3 = ["Symbol", "Error"]

        # Preserve columns (zero-data-loss)
        preserve_map: Dict[str, Dict[str, Any]] = {}
        if _CONFIG.preserve_columns:
            try:
                preserve_map = _build_preserve_map(
                    spreadsheet_id,
                    sheet_name,
                    start_cell,
                    _CONFIG.preserve_columns,
                )
            except Exception as e:
                logger.warning("Preserve scan failed (continuing): %s", e)

        if preserve_map:
            rows3 = _apply_preserve_map(headers3, rows3, preserve_map, _CONFIG.preserve_columns)

        grid = [headers3] + rows3

        # Optional clear
        if clear:
            try:
                start_col, start_row = parse_a1_cell(start_cell)
                end_col = compute_clear_end_col(start_col, len(headers3)) if _CONFIG.smart_clear else _CONFIG.clear_end_col
                clear_range(
                    spreadsheet_id,
                    f"{safe_sheet_name(sheet_name)}!{a1(start_col, start_row)}:{end_col}{_CONFIG.clear_end_row}",
                )
            except Exception as e:
                logger.warning("Clear failed (continuing): %s", e)

        # Write
        cells_updated = write_grid_chunked(spreadsheet_id, sheet_name, start_cell, grid, value_input=value_input)

        backend_status = (response.get("status") or "success").lower()
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
        }

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
@track_operation("refresh_enriched")
def refresh_sheet_with_enriched_quotes(
    spreadsheet_id: str = "",
    sheet_name: str = "",
    tickers: Sequence[str] = (),
    sid: str = "",
    **kwargs,
) -> Dict[str, Any]:
    spreadsheet_id = (spreadsheet_id or sid or _CONFIG.default_spreadsheet_id).strip()
    return _refresh_logic("/v1/enriched/sheet-rows", spreadsheet_id, sheet_name, tickers, **kwargs)

@track_operation("refresh_ai")
def refresh_sheet_with_ai_analysis(
    spreadsheet_id: str = "",
    sheet_name: str = "",
    tickers: Sequence[str] = (),
    sid: str = "",
    **kwargs,
) -> Dict[str, Any]:
    spreadsheet_id = (spreadsheet_id or sid or _CONFIG.default_spreadsheet_id).strip()
    return _refresh_logic("/v1/analysis/sheet-rows", spreadsheet_id, sheet_name, tickers, **kwargs)

@track_operation("refresh_advanced")
def refresh_sheet_with_advanced_analysis(
    spreadsheet_id: str = "",
    sheet_name: str = "",
    tickers: Sequence[str] = (),
    sid: str = "",
    **kwargs,
) -> Dict[str, Any]:
    spreadsheet_id = (spreadsheet_id or sid or _CONFIG.default_spreadsheet_id).strip()
    return _refresh_logic("/v1/advanced/sheet-rows", spreadsheet_id, sheet_name, tickers, **kwargs)

# Thread-isolated async execution (reused pool)
_ASYNC_EXECUTOR = None
_ASYNC_EXECUTOR_LOCK = threading.Lock()

def _get_async_executor():
    global _ASYNC_EXECUTOR
    with _ASYNC_EXECUTOR_LOCK:
        if _ASYNC_EXECUTOR is None:
            import concurrent.futures

            _ASYNC_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=int(os.getenv("SHEETS_ASYNC_MAX_WORKERS", "5") or "5"))
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

def get_service_status() -> Dict[str, Any]:
    return {
        "service_version": SERVICE_VERSION,
        "config": {
            "environment": _CONFIG.environment,
            "safe_mode": _CONFIG.safe_mode.value,
            "backend_url_configured": bool(_CONFIG.backend_base_url),
            "spreadsheet_id_configured": bool(_CONFIG.default_spreadsheet_id),
            "max_rows_per_write": _CONFIG.max_rows_per_write,
            "use_batch_update": _CONFIG.use_batch_update,
            "httpx_enabled": bool(_backend_client._httpx_client is not None),
            "preserve_columns": list(_CONFIG.preserve_columns),
        },
        "circuit_breaker": _backend_client.circuit_breaker.get_state(),
        "telemetry": _telemetry.get_stats() if _CONFIG.enable_telemetry else {},
        "has_core_schemas": _HAS_CORE,
        "has_orjson": _HAS_ORJSON,
        "has_httpx": _HAS_HTTPX,
        "has_prometheus": _PROMETHEUS_AVAILABLE,
        "has_otel": _OTEL_AVAILABLE,
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
    "read_range",
    "write_range",
    "clear_range",
    "write_grid_chunked",
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
