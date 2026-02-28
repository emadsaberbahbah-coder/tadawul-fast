#!/usr/bin/env python3
"""
integrations/google_sheets_service.py
===========================================================
ADVANCED GOOGLE SHEETS SERVICE FOR TADAWUL FAST BRIDGE — v5.2.0
(Emad Bahbah – Enterprise Integration Architecture)

INSTITUTIONAL GRADE · STARTUP-SAFE · ZERO DATA LOSS · COMPLETE ALIGNMENT

v5.2.0 (Enhanced / Render-safe / Production hardened)
- ✅ STARTUP-SAFE: no Google client / httpx client / network work at import-time
- ✅ ZERO-DATA-LOSS clear strategy:
     - never clears before a successful write
     - clears ONLY trailing old rows AFTER successful write (optional)
- ✅ Stronger env parsing (coerce_int/float/bool) — avoids crash if env values are invalid
- ✅ Expanded token resolution (APP_TOKEN / BACKEND_TOKEN / BACKUP_APP_TOKEN / ALLOWED_TOKENS)
- ✅ Safer json_loads/json_dumps for orjson + bytes/str inputs
- ✅ Improved response merge across chunks (header-safe mapping; no column loss)
- ✅ Safe Mode upgraded (STRICT/PARANOID semantics clearer; Symbol column guaranteed)
- ✅ Optional deps remain optional (httpx/orjson/otel/prometheus/core.schemas) with safe fallback
- ✅ Exposes get_sheets_service() and keeps stable __all__
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
# Optional High-Performance Dependencies (safe)
# ---------------------------------------------------------------------------
try:
    import httpx  # type: ignore

    _HAS_HTTPX = True
except Exception:
    httpx = None  # type: ignore
    _HAS_HTTPX = False

try:
    import orjson  # type: ignore

    def json_dumps(v: Any, *, default: Optional[Callable] = None) -> str:
        return orjson.dumps(v, default=default or str).decode("utf-8")

    def json_loads(v: Any) -> Any:
        if isinstance(v, (bytes, bytearray)):
            return orjson.loads(v)
        if isinstance(v, str):
            return orjson.loads(v.encode("utf-8"))
        return orjson.loads(str(v).encode("utf-8"))

    _HAS_ORJSON = True
except Exception:
    import json  # noqa: F401

    def json_dumps(v: Any, *, default: Optional[Callable] = None) -> str:
        return json.dumps(v, default=default or str, ensure_ascii=False)

    def json_loads(v: Any) -> Any:
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", errors="replace")
        return json.loads(v)

    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Monitoring & Tracing (Optional, Safe Fallback)
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
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore

    _OTEL_AVAILABLE = True
    _TRACER = trace.get_tracer(__name__)
except Exception:
    _OTEL_AVAILABLE = False
    _TRACER = None
    Status = None  # type: ignore
    StatusCode = None  # type: ignore

_TRACING_ENABLED = os.getenv("CORE_TRACING_ENABLED", "").strip().lower() in {"1", "true", "yes", "y", "on"}


class TraceContext:
    """OpenTelemetry trace context manager (sync + async). Never raises."""

    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._cm = None
        self.span = None

    def __enter__(self):
        if _OTEL_AVAILABLE and _TRACING_ENABLED and _TRACER is not None:
            try:
                self._cm = _TRACER.start_as_current_span(self.name)
                self.span = self._cm.__enter__()
                if self.span and self.attributes:
                    for k, v in self.attributes.items():
                        try:
                            self.span.set_attribute(k, v)
                        except Exception:
                            pass
            except Exception:
                self._cm = None
                self.span = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.span is not None and exc_val is not None:
                try:
                    if hasattr(self.span, "record_exception"):
                        self.span.record_exception(exc_val)
                except Exception:
                    pass
                try:
                    if Status and StatusCode and hasattr(self.span, "set_status"):
                        self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
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
# Module meta
# ---------------------------------------------------------------------------
SERVICE_VERSION = "5.2.0"
MIN_CORE_VERSION = "5.0.0"

logger = logging.getLogger("google_sheets_service")
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
            parsed = json_loads(s)
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
# Core Schema Integration (Optional)
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
# Configuration
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
    smart_clear: bool = True  # applied as POST-CLEAR only (zero-data-loss)

    # SAFE MODE
    safe_mode: SafeModeLevel = SafeModeLevel.STRICT
    block_on_empty_headers: bool = True
    block_on_empty_rows: bool = False
    block_on_data_mismatch: bool = True
    validate_row_count: bool = True

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

    # Env
    environment: str = "prod"
    user_agent: str = f"TadawulFastBridge-SheetsService/{SERVICE_VERSION}"

    @classmethod
    def from_env(cls) -> "SheetsServiceConfig":
        env = _strip(os.getenv("APP_ENV", "prod")).lower()

        safe_mode_str = _strip(os.getenv("SHEETS_SAFE_MODE", "strict")).lower()
        if safe_mode_str in ("off", "false", "0", "disabled"):
            safe_mode = SafeModeLevel.OFF
        elif safe_mode_str in ("basic",):
            safe_mode = SafeModeLevel.BASIC
        elif safe_mode_str in ("paranoid", "maximum", "max"):
            safe_mode = SafeModeLevel.PARANOID
        else:
            safe_mode = SafeModeLevel.STRICT

        token_transport: Set[TokenTransport] = set()
        transport_str = _strip(os.getenv("SHEETS_BACKEND_TOKEN_TRANSPORT", "header,bearer")).lower()
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

        return cls(
            default_spreadsheet_id=_strip(os.getenv("DEFAULT_SPREADSHEET_ID", "")),
            sheets_api_retries=coerce_int(os.getenv("SHEETS_API_RETRIES", "4"), 4, lo=1, hi=12),
            sheets_api_retry_base_sleep=coerce_float(os.getenv("SHEETS_API_RETRY_BASE_SLEEP", "1.0"), 1.0, lo=0.1, hi=30.0),
            sheets_api_timeout_sec=coerce_float(os.getenv("SHEETS_API_TIMEOUT_SEC", "60"), 60.0, lo=5.0, hi=600.0),
            backend_base_url=_strip(os.getenv("BACKEND_BASE_URL", "")).rstrip("/"),
            backend_timeout_sec=coerce_float(os.getenv("SHEETS_BACKEND_TIMEOUT_SEC", "120"), 120.0, lo=5.0, hi=600.0),
            backend_retries=coerce_int(os.getenv("SHEETS_BACKEND_RETRIES", "3"), 3, lo=0, hi=10),
            backend_retry_sleep=coerce_float(os.getenv("SHEETS_BACKEND_RETRY_SLEEP", "1.0"), 1.0, lo=0.1, hi=30.0),
            backend_max_symbols_per_call=coerce_int(os.getenv("SHEETS_BACKEND_MAX_SYMBOLS_PER_CALL", "200"), 200, lo=10, hi=2000),
            backend_token_transport=token_transport or {TokenTransport.HEADER, TokenTransport.BEARER},
            backend_token_param_name=_strip(os.getenv("SHEETS_BACKEND_TOKEN_QUERY_PARAM", "token")) or "token",
            backend_circuit_breaker_threshold=coerce_int(os.getenv("BACKEND_CIRCUIT_BREAKER_THRESHOLD", "5"), 5, lo=1, hi=100),
            backend_circuit_breaker_timeout=coerce_int(os.getenv("BACKEND_CIRCUIT_BREAKER_TIMEOUT", "60"), 60, lo=5, hi=3600),
            max_rows_per_write=coerce_int(os.getenv("SHEETS_MAX_ROWS_PER_WRITE", "500"), 500, lo=50, hi=5000),
            use_batch_update=coerce_bool(os.getenv("SHEETS_USE_BATCH_UPDATE", "true"), True),
            max_batch_ranges=coerce_int(os.getenv("SHEETS_MAX_BATCH_RANGES", "25"), 25, lo=1, hi=200),
            clear_end_col=_strip(os.getenv("SHEETS_CLEAR_END_COL", "ZZ")).upper() or "ZZ",
            clear_end_row=coerce_int(os.getenv("SHEETS_CLEAR_END_ROW", "100000"), 100000, lo=1000, hi=2000000),
            smart_clear=coerce_bool(os.getenv("SHEETS_SMART_CLEAR", "true"), True),
            safe_mode=safe_mode,
            block_on_empty_headers=coerce_bool(os.getenv("SHEETS_BLOCK_ON_EMPTY_HEADERS", "true"), True),
            block_on_empty_rows=coerce_bool(os.getenv("SHEETS_BLOCK_ON_EMPTY_ROWS", "false"), False),
            block_on_data_mismatch=coerce_bool(os.getenv("SHEETS_BLOCK_ON_DATA_MISMATCH", "true"), True),
            validate_row_count=coerce_bool(os.getenv("SHEETS_VALIDATE_ROW_COUNT", "true"), True),
            cache_ttl_seconds=coerce_int(os.getenv("SHEETS_CACHE_TTL_SECONDS", "300"), 300, lo=5, hi=86400),
            enable_header_cache=coerce_bool(os.getenv("SHEETS_ENABLE_HEADER_CACHE", "true"), True),
            connection_pool_size=coerce_int(os.getenv("SHEETS_CONNECTION_POOL_SIZE", "20"), 20, lo=5, hi=200),
            keep_alive_seconds=coerce_int(os.getenv("SHEETS_KEEP_ALIVE_SECONDS", "30"), 30, lo=5, hi=300),
            verify_ssl=coerce_bool(os.getenv("SHEETS_VERIFY_SSL", "true"), True),
            ssl_cert_path=_strip(os.getenv("SHEETS_SSL_CERT_PATH", "")) or None,
            enable_telemetry=coerce_bool(os.getenv("SHEETS_ENABLE_TELEMETRY", "true"), True),
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
    sheet_name: Optional[str] = None
    endpoint: Optional[str] = None
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
                    "avg": sum(lat) / n,
                    "p50": s[n // 2],
                    "p95": s[min(n - 1, int(n * 0.95))],
                    "p99": s[min(n - 1, int(n * 0.99))],
                    "max": max(lat),
                }
            return out

    def reset(self) -> None:
        with self._lock:
            self._metrics.clear()
            self._counters.clear()
            self._latencies.clear()


_telemetry = TelemetryCollector()


def _best_effort_sheet_name_from_call(args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Optional[str]:
    # Prefer keyword
    sn = kwargs.get("sheet_name")
    if isinstance(sn, str) and sn.strip():
        return sn.strip()
    # Try positional conventions: (spreadsheet_id, sheet_name, tickers, ...)
    if len(args) >= 2 and isinstance(args[1], str) and args[1].strip():
        return args[1].strip()
    return None


def track_operation(operation: str):
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            rid = uuid.uuid4().hex[:12]
            sheet_name = _best_effort_sheet_name_from_call(args, kwargs)
            endpoint = kwargs.get("endpoint") if isinstance(kwargs.get("endpoint"), str) else None

            try:
                with TraceContext(
                    f"sheets_{operation}",
                    {"sheet_name": sheet_name or "", "request_id": rid, "endpoint": endpoint or ""},
                ):
                    result = func(*args, **kwargs)

                rows_written = 0
                cells_updated = 0
                if isinstance(result, dict):
                    rows_written = int(result.get("rows_written", 0) or 0)
                    cells_updated = int(result.get("cells_updated", 0) or 0)

                metrics = OperationMetrics(
                    operation=operation,
                    start_time=start,
                    end_time=time.time(),
                    success=True,
                    rows_processed=rows_written,
                    cells_updated=cells_updated,
                    sheet_name=sheet_name,
                    endpoint=endpoint,
                    request_id=rid,
                )
                _telemetry.record_operation(metrics)
                sheets_operations_total.labels(operation=operation, status="success").inc()
                sheets_operation_duration.labels(operation=operation).observe(time.time() - start)
                return result
            except Exception as e:
                metrics = OperationMetrics(
                    operation=operation,
                    start_time=start,
                    end_time=time.time(),
                    success=False,
                    error_type=e.__class__.__name__,
                    sheet_name=sheet_name,
                    endpoint=endpoint,
                    request_id=rid,
                )
                _telemetry.record_operation(metrics)
                sheets_operations_total.labels(operation=operation, status="error").inc()
                sheets_operation_duration.labels(operation=operation).observe(time.time() - start)
                raise

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# Circuit Breaker
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
                    logger.info("Circuit breaker HALF_OPEN (testing recovery)")
                    return True
                return False
            return True  # HALF_OPEN

    def record_success(self) -> None:
        with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.CLOSED
                logger.info("Circuit breaker CLOSED after recovery")
            self.failure_count = 0

    def record_failure(self) -> None:
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitState.CLOSED and self.failure_count >= self.threshold:
                self.state = CircuitState.OPEN
                logger.warning("Circuit breaker OPEN after %s failures", self.failure_count)
            elif self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                logger.warning("Circuit breaker re-OPEN after HALF_OPEN failure")

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
# Credentials + Google Sheets Client (LAZY; never at import-time)
# ---------------------------------------------------------------------------
def _repair_private_key(pk: str) -> str:
    pk2 = (pk or "").replace("\\n", "\n").replace("\\r\\n", "\n")
    if "-----BEGIN PRIVATE KEY-----" not in pk2:
        pk2 = "-----BEGIN PRIVATE KEY-----\n" + pk2.strip()
    if "-----END PRIVATE KEY-----" not in pk2:
        pk2 = pk2.strip() + "\n-----END PRIVATE KEY-----"
    return pk2 + ("\n" if not pk2.endswith("\n") else "")


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


class CredentialsManager:
    """
    Resolves service account credentials from:
      1) GOOGLE_CREDENTIALS_DICT (JSON dict string)
      2) GOOGLE_SHEETS_CREDENTIALS / GOOGLE_CREDENTIALS (JSON)
      3) GOOGLE_*_B64 (base64 JSON)
      4) GOOGLE_APPLICATION_CREDENTIALS / GOOGLE_*_FILE (file path)
      5) core.config.get_settings() if available
      6) env.py.get_settings() if available
    """

    def __init__(self):
        self._creds_info: Optional[Dict[str, Any]] = None
        self._lock = threading.RLock()

    def get_credentials(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            if self._creds_info is not None:
                return dict(self._creds_info)

            for src in (
                self._from_env_dict,
                self._from_env_json,
                self._from_env_base64,
                self._from_env_file,
                self._from_core_settings,
                self._from_env_manager_settings,
            ):
                creds = src()
                if creds:
                    self._creds_info = self._normalize_credentials(creds)
                    logger.info("Google creds loaded from %s", src.__name__)
                    return dict(self._creds_info)

            logger.warning("Google creds not configured (this is OK unless Sheets operations are invoked)")
            return None

    def _normalize_credentials(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(creds)
        pk = out.get("private_key")
        if isinstance(pk, str) and pk.strip():
            out["private_key"] = _repair_private_key(pk)
        return out

    def _from_env_dict(self) -> Optional[Dict[str, Any]]:
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
        path = _strip(os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "") or os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", "") or os.getenv("GOOGLE_CREDENTIALS_FILE", ""))
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

    def _from_core_settings(self) -> Optional[Dict[str, Any]]:
        try:
            from core.config import get_settings  # type: ignore

            settings = get_settings()
            raw = getattr(settings, "google_sheets_credentials_json", None)
            if isinstance(raw, str) and raw.strip().startswith("{"):
                obj = json_loads(raw)
                return obj if isinstance(obj, dict) else None
            d = getattr(settings, "google_credentials_dict", None)
            if isinstance(d, dict) and d:
                return dict(d)
        except Exception:
            pass
        return None

    def _from_env_manager_settings(self) -> Optional[Dict[str, Any]]:
        # If you use env.py as global settings manager
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


_credentials_manager = CredentialsManager()


class SheetsAPIClient:
    """
    LAZY Google Sheets API service:
    - No imports / no build until first use.
    """

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
            raise RuntimeError(
                "Google API client libraries not installed. Install: google-api-python-client google-auth"
            ) from e

        creds_info = _credentials_manager.get_credentials()
        if not creds_info:
            self._failed = True
            raise RuntimeError("Missing Google service account credentials")

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(creds_info, scopes=scopes)

        # NOTE: this may fetch discovery doc; that is OK at runtime, never at import-time.
        service = build("sheets", "v4", credentials=creds, cache_discovery=False, num_retries=_CONFIG.sheets_api_retries)
        logger.info("Google Sheets API client initialized")
        return service

    def close(self):
        # discovery/build doesn't expose a standard close; keep for API symmetry
        return None


_sheets_client = SheetsAPIClient()


def get_sheets_service():
    """Public getter (stable)."""
    return _sheets_client.get_service()


# ---------------------------------------------------------------------------
# Header Cache & Canonical Headers
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
            if time.time() - ts < self.ttl:
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
# A1 Notation Utilities
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
# Symbol Normalization
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
# Header & Row Processing
# ---------------------------------------------------------------------------
@lru_cache(maxsize=2048)
def _normalize_header_key(header: str) -> str:
    s = (_strip(header) or "").lower()
    return re.sub(r"[^a-z0-9]", "", s)


_HEADER_ALIAS_MAP: Dict[str, str] = {
    "symbol": "Symbol",
    "ticker": "Symbol",
    "requestedsymbol": "Symbol",
    "expectedroi1m": "Expected ROI % (1M)",
    "expectedroi3m": "Expected ROI % (3M)",
    "expectedroi12m": "Expected ROI % (12M)",
    "forecastprice1m": "Forecast Price (1M)",
    "forecastprice3m": "Forecast Price (3M)",
    "forecastprice12m": "Forecast Price (12M)",
    "forecastupdatedutc": "Forecast Updated (UTC)",
    "forecastupdatedriyadh": "Forecast Updated (Riyadh)",
    "valuescore": "Value Score",
    "qualityscore": "Quality Score",
    "momentumscore": "Momentum Score",
    "riskscore": "Risk Score",
    "opportunityscore": "Opportunity Score",
    "overallscore": "Overall Score",
    "recommendation": "Recommendation",
    "rec": "Recommendation",
    "recbadge": "Rec Badge",
    "momentumbadge": "Momentum Badge",
    "opportunitybadge": "Opportunity Badge",
    "riskbadge": "Risk Badge",
    "error": "Error",
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
    if find_symbol_col(headers) >= 0:
        return headers
    return ["Symbol"] + list(headers)


def rows_to_grid(headers: List[str], rows: Any) -> Tuple[List[str], List[List[Any]]]:
    headers = [(_strip(h) or "") for h in (headers or []) if _strip(h)]
    headers = ensure_symbol_header(headers or ["Symbol", "Error"])

    fixed_rows: List[List[Any]] = []
    if not rows:
        return headers, fixed_rows

    # rows is list of dicts
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

    # rows is list of lists
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


# ---------------------------------------------------------------------------
# Google Sheets Operations (Retry + Full Jitter)
# ---------------------------------------------------------------------------
def _sleep_full_jitter(base: float, cap: float = 15.0) -> None:
    t = random.uniform(0.0, min(cap, base))
    time.sleep(max(0.0, t))


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
            logger.warning("Sheets API %s retry %s/%s in jitter (base=%.2fs): %s", operation_name, attempt + 1, retries, base, e)
            _sleep_full_jitter(base, cap=15.0)
    raise last_exc  # type: ignore


def read_range(spreadsheet_id: str, range_name: str) -> List[List[Any]]:
    service = get_sheets_service()

    def _read():
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name, majorDimension="ROWS").execute()
        return result.get("values", []) or []

    return _retry_sheet_op("Read Range", _read)


def write_range(spreadsheet_id: str, range_name: str, values: List[List[Any]], value_input: str = "RAW") -> int:
    service = get_sheets_service()
    body = {"values": values or [[]]}

    def _write():
        result = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=range_name, valueInputOption=value_input, body=body).execute()
        return int(result.get("updatedCells", 0) or 0)

    return _retry_sheet_op("Write Range", _write)


def clear_range(spreadsheet_id: str, range_name: str) -> None:
    service = get_sheets_service()

    def _clear():
        service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=range_name).execute()

    _retry_sheet_op("Clear Range", _clear)


def write_grid_chunked(spreadsheet_id: str, sheet_name: str, start_cell: str, grid: List[List[Any]], value_input: str = "RAW") -> int:
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
    if _CONFIG.use_batch_update and chunks:
        try:
            service = get_sheets_service()
            batch_data = []

            rng0 = f"{sheet_a1}!{a1(start_col, start_row)}"
            batch_data.append({"range": rng0, "values": [header] + chunks[0]})

            current_row = start_row + 1 + len(chunks[0])
            for chunk in chunks[1:]:
                rng = f"{sheet_a1}!{a1(start_col, current_row)}"
                batch_data.append({"range": rng, "values": chunk})
                current_row += len(chunk)

            for i in range(0, len(batch_data), _CONFIG.max_batch_ranges):
                batch = batch_data[i : i + _CONFIG.max_batch_ranges]

                def _batch_update():
                    body = {"valueInputOption": value_input, "data": batch}
                    result = service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
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
# SAFE MODE Validation
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

        if self.config.safe_mode == SafeModeLevel.BASIC:
            return None

        # STRICT+: minimal structural checks
        if self.config.block_on_data_mismatch and headers and rows:
            for i, row in enumerate(rows[:10]):
                try:
                    if isinstance(row, list) and len(row) != len(headers):
                        return f"SAFE MODE: Row {i+1} length ({len(row)}) doesn't match headers ({len(headers)})"
                except Exception:
                    continue

        if self.config.safe_mode == SafeModeLevel.STRICT:
            return None

        # PARANOID: symbol coverage heuristic
        if self.config.validate_row_count and headers and requested_symbols:
            h2, r2 = rows_to_grid(headers, rows)
            sym_idx = find_symbol_col(h2)
            if sym_idx >= 0:
                returned: Set[str] = set()
                for row in r2:
                    if isinstance(row, list) and sym_idx < len(row):
                        sym = _strip(row[sym_idx]).upper()
                        if sym:
                            returned.add(sym)
                req = set(requested_symbols)
                missing = req - returned
                if missing and len(missing) > max(1, int(len(req) * 0.5)):
                    return f"SAFE MODE: Missing >50% of requested symbols ({len(missing)}/{len(req)}) for {sheet_name}"

        return None


_safe_mode_validator = SafeModeValidator(_CONFIG)


# ---------------------------------------------------------------------------
# Backend API Client (httpx preferred, urllib fallback) — LAZY httpx init
# ---------------------------------------------------------------------------
class BackendAPIClient:
    def __init__(self):
        self.circuit_breaker = BackendCircuitBreaker(
            threshold=_CONFIG.backend_circuit_breaker_threshold,
            timeout=_CONFIG.backend_circuit_breaker_timeout,
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
        """
        Resolve tokens in priority order.
        - ALLOWED_TOKENS / TFB_ALLOWED_TOKENS / APP_TOKENS (list)
        - APP_TOKEN / BACKEND_TOKEN / BACKUP_APP_TOKEN
        """
        toks = normalize_list_lower(coerce_list(os.getenv("ALLOWED_TOKENS") or os.getenv("TFB_ALLOWED_TOKENS") or os.getenv("APP_TOKENS") or "", []))
        # These list sources are not lowercased tokens; re-pull original list safely:
        toks_raw = coerce_list(os.getenv("ALLOWED_TOKENS") or os.getenv("TFB_ALLOWED_TOKENS") or os.getenv("APP_TOKENS") or "", [])
        toks_out = [_strip(t) for t in toks_raw if _strip(t)]

        for k in ("APP_TOKEN", "BACKEND_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
            v = _strip(os.getenv(k, ""))
            if v and v not in toks_out:
                toks_out.append(v)
        return toks_out

    def _get_token(self) -> str:
        toks = self._resolve_token_candidates()
        return toks[0] if toks else ""

    def _build_headers(self, token: str, request_id: str) -> Dict[str, str]:
        headers = {"Content-Type": "application/json", "Accept": "application/json", "User-Agent": _CONFIG.user_agent, "X-Request-Id": request_id}
        if not token:
            return headers
        if TokenTransport.HEADER in _CONFIG.backend_token_transport:
            headers["X-APP-TOKEN"] = token
        if TokenTransport.BEARER in _CONFIG.backend_token_transport:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _compose_url_and_params(
        self,
        endpoint: str,
        token: str,
        query_params: Optional[Dict[str, Any]],
    ) -> Tuple[str, Dict[str, Any]]:
        base_url = _CONFIG.backend_base_url.rstrip("/")
        url = f"{base_url}{endpoint}"
        params: Dict[str, Any] = dict(query_params or {})
        if token and TokenTransport.QUERY in _CONFIG.backend_token_transport:
            params[_CONFIG.backend_token_param_name] = token
        return url, params

    def call_api(self, endpoint: str, payload: Dict[str, Any], query_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        request_id = uuid.uuid4().hex[:12]
        with TraceContext("backend_api_call", {"endpoint": endpoint, "request_id": request_id}):
            if not self.circuit_breaker.can_execute():
                return {"status": "error", "error": f"Circuit breaker {self.circuit_breaker.get_state()['state']}", "headers": ["Symbol", "Error"], "rows": []}

            base_url = _CONFIG.backend_base_url.rstrip("/")
            if not base_url:
                return {"status": "error", "error": "No backend URL configured", "headers": ["Symbol", "Error"], "rows": []}

            symbols = payload.get("symbols", []) or payload.get("tickers", []) or []
            token = self._get_token()
            url, params = self._compose_url_and_params(endpoint, token, query_params)

            try:
                body_bytes = json_dumps(payload, default=str).encode("utf-8")
            except Exception as e:
                self.circuit_breaker.record_failure()
                err = f"JSON encode error: {e}"
                return {"status": "error", "error": err, "headers": ["Symbol", "Error"], "rows": [[s, err] for s in symbols]}

            last_error: Optional[str] = None
            max_attempts = max(1, int(_CONFIG.backend_retries) + 1)

            for attempt in range(max_attempts):
                try:
                    client = self._get_httpx_client()

                    # Preferred: httpx
                    if client is not None:
                        resp = client.post(
                            url,
                            params=params or None,
                            content=body_bytes,
                            headers=self._build_headers(token, request_id),
                        )
                        status = int(resp.status_code)
                        raw = resp.text

                        if 200 <= status < 300:
                            try:
                                data = resp.json()
                            except Exception:
                                data = json_loads(raw)

                            self.circuit_breaker.record_success()
                            if not isinstance(data, dict):
                                data = {}
                            data.setdefault("status", "success")
                            data.setdefault("headers", [])
                            data.setdefault("rows", [])
                            data.setdefault("request_id", request_id)
                            return data

                        last_error = f"HTTP {status}: {raw[:400]}"
                        if status in (429, 500, 502, 503, 504) and attempt < max_attempts - 1:
                            base_sleep = float(_CONFIG.backend_retry_sleep) * (2 ** attempt)
                            _sleep_full_jitter(base_sleep, cap=30.0)
                            continue

                        self.circuit_breaker.record_failure()
                        return {"status": "error", "error": last_error, "headers": ["Symbol", "Error"], "rows": [[s, last_error] for s in symbols], "request_id": request_id}

                    # Fallback: urllib
                    request_url = url
                    if params:
                        parsed = urllib.parse.urlsplit(request_url)
                        q = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
                        q.update(params)
                        request_url = urllib.parse.urlunsplit(
                            (parsed.scheme, parsed.netloc, parsed.path, urllib.parse.urlencode(q, doseq=True), parsed.fragment)
                        )

                    req = urllib.request.Request(
                        request_url,
                        data=body_bytes,
                        headers=self._build_headers(token, request_id),
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=float(_CONFIG.backend_timeout_sec), context=self._ssl_ctx) as resp2:
                        raw2 = resp2.read().decode("utf-8", errors="replace")
                        status2 = int(getattr(resp2, "status", 200) or 200)

                        if 200 <= status2 < 300:
                            try:
                                data2 = json_loads(raw2)
                            except Exception as e:
                                self.circuit_breaker.record_failure()
                                err = f"Invalid JSON: {e}"
                                return {"status": "error", "error": err, "headers": ["Symbol", "Error"], "rows": [[s, err] for s in symbols], "request_id": request_id}

                            self.circuit_breaker.record_success()
                            if not isinstance(data2, dict):
                                data2 = {}
                            data2.setdefault("status", "success")
                            data2.setdefault("headers", [])
                            data2.setdefault("rows", [])
                            data2.setdefault("request_id", request_id)
                            return data2

                        last_error = f"HTTP {status2}: {raw2[:400]}"
                        if status2 in (429, 500, 502, 503, 504) and attempt < max_attempts - 1:
                            base_sleep = float(_CONFIG.backend_retry_sleep) * (2 ** attempt)
                            _sleep_full_jitter(base_sleep, cap=30.0)
                            continue

                        self.circuit_breaker.record_failure()
                        return {"status": "error", "error": last_error, "headers": ["Symbol", "Error"], "rows": [[s, last_error] for s in symbols], "request_id": request_id}

                except Exception as e:
                    last_error = f"Request error: {e}"
                    if attempt < max_attempts - 1:
                        base_sleep = float(_CONFIG.backend_retry_sleep) * (2 ** attempt)
                        _sleep_full_jitter(base_sleep, cap=30.0)
                        continue
                    self.circuit_breaker.record_failure()
                    return {"status": "error", "error": last_error, "headers": ["Symbol", "Error"], "rows": [[s, last_error] for s in symbols], "request_id": request_id}

            self.circuit_breaker.record_failure()
            return {"status": "error", "error": last_error or "Max retries exceeded", "headers": ["Symbol", "Error"], "rows": [[s, last_error or "Max retries exceeded"] for s in symbols], "request_id": request_id}

    def call_api_chunked(self, endpoint: str, symbols: List[str], base_payload: Dict[str, Any], query_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        symbols = list(symbols or [])
        if len(symbols) <= _CONFIG.backend_max_symbols_per_call:
            payload = dict(base_payload)
            payload["symbols"] = symbols
            payload["tickers"] = symbols
            return self.call_api(endpoint, payload, query_params)

        chunks = [symbols[i : i + _CONFIG.backend_max_symbols_per_call] for i in range(0, len(symbols), _CONFIG.backend_max_symbols_per_call)]
        responses: List[Dict[str, Any]] = []

        for chunk in chunks:
            payload = dict(base_payload)
            payload["symbols"] = chunk
            payload["tickers"] = chunk
            resp = self.call_api(endpoint, payload, query_params)
            responses.append(resp)

        return self._merge_responses(symbols, responses)

    def _merge_responses(self, requested_symbols: List[str], responses: List[Dict[str, Any]]) -> Dict[str, Any]:
        # Merge headers union, then map each response row into final header positions.
        status = "success"
        error_msg: Optional[str] = None

        all_headers: List[str] = []
        for resp in responses:
            hs = resp.get("headers", []) or []
            all_headers = append_missing_headers(all_headers, hs)

        all_headers = ensure_symbol_header(all_headers or ["Symbol", "Error"])
        all_hdr_idx = {_normalize_header_key(h): i for i, h in enumerate(all_headers)}
        sym_col_final = find_symbol_col(all_headers)
        err_col_final = find_error_col(all_headers)

        # Build row map by symbol
        row_map: Dict[str, List[Any]] = {}

        for resp in responses:
            resp_status = (_strip(resp.get("status", "success")) or "success").lower()
            if resp_status in ("error", "partial"):
                status = "partial"
            if resp_status == "error" and not error_msg:
                error_msg = _strip(resp.get("error"))

            h = resp.get("headers") or []
            r = resp.get("rows") or []
            if not h or not r:
                continue

            h2, r2 = rows_to_grid(list(h), r)
            h2 = ensure_symbol_header(h2)
            src_idx = {_normalize_header_key(x): i for i, x in enumerate(h2)}
            sym_idx = find_symbol_col(h2)

            for row in r2:
                if sym_idx < 0 or sym_idx >= len(row):
                    continue
                sym = _strip(row[sym_idx]).upper()
                if not sym:
                    continue

                out_row = [None] * len(all_headers)
                for key_norm, src_pos in src_idx.items():
                    if src_pos >= len(row):
                        continue
                    # direct mapping
                    if key_norm in all_hdr_idx:
                        out_row[all_hdr_idx[key_norm]] = row[src_pos]
                        continue
                    # alias mapping
                    alias = _HEADER_ALIAS_MAP_NORM.get(key_norm)
                    if alias:
                        a_norm = _normalize_header_key(alias)
                        if a_norm in all_hdr_idx:
                            out_row[all_hdr_idx[a_norm]] = row[src_pos]

                # ensure Symbol column
                if sym_col_final >= 0:
                    out_row[sym_col_final] = sym
                row_map[sym] = out_row

        # Final ordered rows aligned to requested_symbols
        final_rows: List[List[Any]] = []
        for sym in requested_symbols:
            sym_u = _strip(sym).upper()
            if sym_u and sym_u in row_map:
                final_rows.append(row_map[sym_u])
            else:
                status = "partial"
                placeholder = [None] * len(all_headers)
                if sym_col_final >= 0:
                    placeholder[sym_col_final] = sym_u or sym
                if err_col_final >= 0:
                    placeholder[err_col_final] = "No data from backend"
                final_rows.append(placeholder)

        return {"status": status, "headers": all_headers, "rows": final_rows, "error": error_msg}


_backend_client = BackendAPIClient()

# ---------------------------------------------------------------------------
# Refresh Logic (ZERO-DATA-LOSS clear)
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
    start_time = time.time()
    rid = uuid.uuid4().hex[:12]

    try:
        spreadsheet_id = _strip(spreadsheet_id or _CONFIG.default_spreadsheet_id)
        sheet_name = _strip(sheet_name)

        if not spreadsheet_id:
            return {"status": "error", "error": "No spreadsheet ID provided", "service_version": SERVICE_VERSION, "request_id": rid}
        if not sheet_name:
            return {"status": "error", "error": "No sheet name provided", "service_version": SERVICE_VERSION, "request_id": rid}

        symbols = normalize_tickers(tickers)
        if not symbols:
            return {
                "status": "skipped",
                "reason": "No valid tickers provided",
                "endpoint": endpoint,
                "sheet": sheet_name,
                "service_version": SERVICE_VERSION,
                "request_id": rid,
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
                "request_id": rid,
                "backend_status": response.get("status"),
                "backend_error": response.get("error"),
            }

        headers = response.get("headers", []) or []
        rows = response.get("rows", []) or []

        if not headers:
            headers = get_canonical_headers(sheet_name)

        headers2, rows2 = rows_to_grid(headers, rows)
        headers3, rows3 = reorder_to_canonical(sheet_name, headers2, rows2)
        headers3 = ensure_symbol_header(headers3 or ["Symbol", "Error"])

        grid = [headers3] + rows3

        # 1) WRITE first (no clear before write)
        try:
            cells_updated = write_grid_chunked(spreadsheet_id, sheet_name, start_cell, grid, value_input=value_input)
        except Exception as e:
            return {
                "status": "error",
                "error": f"Write failed: {e}",
                "sheet": sheet_name,
                "endpoint": endpoint,
                "rows": len(rows3),
                "headers": len(headers3),
                "service_version": SERVICE_VERSION,
                "request_id": rid,
            }

        # 2) POST-CLEAR trailing rows ONLY after successful write (zero-data-loss)
        if clear:
            try:
                start_col, start_row = parse_a1_cell(start_cell)
                end_col = compute_clear_end_col(start_col, len(headers3)) if _CONFIG.smart_clear else _CONFIG.clear_end_col

                # Clear only AFTER last written row to remove old leftovers (never clears new data).
                last_written_row = start_row + len(grid) - 1  # includes header row
                tail_start_row = last_written_row + 1
                if tail_start_row <= _CONFIG.clear_end_row:
                    tail_range = f"{safe_sheet_name(sheet_name)}!{a1(start_col, tail_start_row)}:{end_col}{_CONFIG.clear_end_row}"
                    clear_range(spreadsheet_id, tail_range)

                # Also clear header row extra columns to the right (optional, conservative)
                # (We only clear from end_col+1 to configured clear_end_col if smart_clear is ON)
                if _CONFIG.smart_clear:
                    try:
                        end_idx = col_to_index(end_col)
                        cfg_end_idx = col_to_index(_CONFIG.clear_end_col)
                        if cfg_end_idx > end_idx:
                            right_start_col = index_to_col(end_idx + 1)
                            hdr_right_range = f"{safe_sheet_name(sheet_name)}!{a1(right_start_col, start_row)}:{_CONFIG.clear_end_col}{start_row}"
                            clear_range(spreadsheet_id, hdr_right_range)
                    except Exception:
                        pass

            except Exception as e:
                logger.warning("Post-clear failed (continuing): %s", e)

        backend_status = (_strip(response.get("status")) or "success").lower()
        final_status = "partial" if backend_status in ("error", "partial") else "skipped" if backend_status == "skipped" else "success"

        return {
            "status": final_status,
            "sheet": sheet_name,
            "endpoint": endpoint,
            "mode": mode or "",
            "rows_written": len(rows3),
            "headers_count": len(headers3),
            "cells_updated": cells_updated,
            "backend_status": backend_status,
            "backend_error": response.get("error"),
            "backend_chunk_size": _CONFIG.backend_max_symbols_per_call,
            "safe_mode": _CONFIG.safe_mode.value,
            "elapsed_ms": int((time.time() - start_time) * 1000),
            "service_version": SERVICE_VERSION,
            "request_id": rid,
        }

    except Exception as e:
        logger.exception("Refresh logic failed: %s", e)
        return {
            "status": "error",
            "error": str(e),
            "endpoint": endpoint,
            "sheet": sheet_name,
            "elapsed_ms": int((time.time() - start_time) * 1000),
            "service_version": SERVICE_VERSION,
            "request_id": rid,
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
    spreadsheet_id = _strip(spreadsheet_id or sid or _CONFIG.default_spreadsheet_id)
    return _refresh_logic("/v1/enriched/sheet-rows", spreadsheet_id, sheet_name, tickers, **kwargs)


@track_operation("refresh_ai")
def refresh_sheet_with_ai_analysis(
    spreadsheet_id: str = "",
    sheet_name: str = "",
    tickers: Sequence[str] = (),
    sid: str = "",
    **kwargs,
) -> Dict[str, Any]:
    spreadsheet_id = _strip(spreadsheet_id or sid or _CONFIG.default_spreadsheet_id)
    return _refresh_logic("/v1/analysis/sheet-rows", spreadsheet_id, sheet_name, tickers, **kwargs)


@track_operation("refresh_advanced")
def refresh_sheet_with_advanced_analysis(
    spreadsheet_id: str = "",
    sheet_name: str = "",
    tickers: Sequence[str] = (),
    sid: str = "",
    **kwargs,
) -> Dict[str, Any]:
    spreadsheet_id = _strip(spreadsheet_id or sid or _CONFIG.default_spreadsheet_id)
    return _refresh_logic("/v1/advanced/sheet-rows", spreadsheet_id, sheet_name, tickers, **kwargs)


# Thread-isolated async execution (reused pool; no per-call pool creation)
_ASYNC_EXECUTOR = None
_ASYNC_EXECUTOR_LOCK = threading.Lock()


def _get_async_executor():
    global _ASYNC_EXECUTOR
    with _ASYNC_EXECUTOR_LOCK:
        if _ASYNC_EXECUTOR is None:
            import concurrent.futures

            _ASYNC_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=coerce_int(os.getenv("SHEETS_ASYNC_MAX_WORKERS", "5"), 5, lo=1, hi=50))
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
            "orjson_enabled": bool(_HAS_ORJSON),
        },
        "circuit_breaker": _backend_client.circuit_breaker.get_state(),
        "telemetry": _telemetry.get_stats() if _CONFIG.enable_telemetry else {},
        "has_core_schemas": _HAS_CORE,
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
    # Executor intentionally kept alive for process lifetime


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
