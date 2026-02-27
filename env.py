#!/usr/bin/env python3
"""
env.py
================================================================================
TADAWUL FAST BRIDGE – ENTERPRISE ENVIRONMENT MANAGER (v7.5.1)
================================================================================
STABLE EDITION | DISTRIBUTED CONFIG | HOT-RELOAD | SECRETS MANAGEMENT

Fixes vs v7.5.0 draft:
- ✅ Always imports `json` (even if orjson exists) to avoid NameError
- ✅ Correct OpenTelemetry span usage (context manager) + safe attribute setting
- ✅ Adds missing `coerce_dict` utility (exported in __all__)
- ✅ Keeps your public API + backward compatibility exports
"""

from __future__ import annotations

import base64
import logging
import os
import random
import re
import socket
import sys
import threading
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

import json  # ✅ ALWAYS available, even if orjson is installed

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

    def json_dumps(obj: Any, *, indent: int = 0) -> str:
        option = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(obj, option=option).decode("utf-8")

    _HAS_ORJSON = True
except Exception:
    def json_loads(data: Union[str, bytes]) -> Any:
        return json.loads(data)

    def json_dumps(obj: Any, *, indent: int = 0) -> str:
        return json.dumps(obj, indent=indent if indent else None, default=str, ensure_ascii=False)

    _HAS_ORJSON = False


# =============================================================================
# Version & Core Configuration
# =============================================================================
ENV_VERSION = "7.5.1"
ENV_SCHEMA_VERSION = "2.1"
MIN_PYTHON = (3, 8)

if sys.version_info < MIN_PYTHON:
    sys.exit(f"❌ Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ required")


# =============================================================================
# Optional Dependencies with Graceful Degradation
# =============================================================================

# YAML support
try:
    import yaml  # type: ignore
    YAML_AVAILABLE = True
except Exception:
    yaml = None  # type: ignore
    YAML_AVAILABLE = False

# Monitoring & Tracing
try:
    from prometheus_client import Counter, Histogram  # type: ignore
    PROMETHEUS_AVAILABLE = True
except Exception:
    PROMETHEUS_AVAILABLE = False
    Counter = Histogram = None  # type: ignore

_TRACING_ENABLED = os.getenv("CORE_TRACING_ENABLED", "").strip().lower() in {"1", "true", "yes", "y", "on"}

try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore
    OTEL_AVAILABLE = True
    _TRACER = trace.get_tracer(__name__)
except Exception:
    OTEL_AVAILABLE = False
    Status = StatusCode = None  # type: ignore

    class _DummyCM:
        def __enter__(self): return None
        def __exit__(self, *args, **kwargs): return False

    class _DummyTracer:
        def start_as_current_span(self, *args, **kwargs): return _DummyCM()

    _TRACER = _DummyTracer()  # type: ignore


# =============================================================================
# Metrics (safe)
# =============================================================================
if PROMETHEUS_AVAILABLE:
    env_config_loads_total = Counter(
        "env_config_loads_total",
        "Total configuration loads",
        ["source", "status"],
    )
    env_config_errors_total = Counter(
        "env_config_errors_total",
        "Total configuration errors",
        ["type"],
    )
    env_config_reload_duration = Histogram(
        "env_config_reload_duration_seconds",
        "Configuration reload duration (seconds)",
        buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
    )
else:
    class _DummyMetric:
        def labels(self, *args, **kwargs): return self
        def inc(self, *args, **kwargs): return None
        def observe(self, *args, **kwargs): return None

    env_config_loads_total = _DummyMetric()
    env_config_errors_total = _DummyMetric()
    env_config_reload_duration = _DummyMetric()


# =============================================================================
# Logging Configuration
# =============================================================================
LOG_FORMAT = "%(asctime)s | %(levelname)8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger("env")


# =============================================================================
# Tracing (fixed)
# =============================================================================
class TraceContext:
    """
    Correct OTEL usage:
    - start_as_current_span() returns a context manager
    - __enter__ returns the span (or None)
    """
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._cm = None
        self.span = None

    def __enter__(self):
        if OTEL_AVAILABLE and _TRACING_ENABLED:
            self._cm = _TRACER.start_as_current_span(self.name)
            self.span = self._cm.__enter__()
            if self.span and self.attributes:
                for k, v in self.attributes.items():
                    try:
                        self.span.set_attribute(k, v)
                    except Exception:
                        pass
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if OTEL_AVAILABLE and _TRACING_ENABLED and self.span and exc_val and Status and StatusCode:
            try:
                self.span.record_exception(exc_val)
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            except Exception:
                pass
        if self._cm:
            return self._cm.__exit__(exc_type, exc_val, exc_tb)
        return False


# =============================================================================
# Enums & Types
# =============================================================================
class Environment(str, Enum):
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"
    LOCAL = "local"


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class ProviderType(str, Enum):
    EODHD = "eodhd"
    FINNHUB = "finnhub"
    ALPHAVANTAGE = "alphavantage"
    YAHOO = "yahoo"
    YAHOO_CHART = "yahoo_chart"
    ARGAAM = "argaam"
    TADAWUL = "tadawul"
    FMP = "fmp"
    TWELVEDATA = "twelvedata"
    MARKETSTACK = "marketstack"
    POLYGON = "polygon"
    IEXCLOUD = "iexcloud"
    TRADIER = "tradier"
    CUSTOM = "custom"


class CacheBackend(str, Enum):
    MEMORY = "memory"
    REDIS = "redis"
    MEMCACHED = "memcached"
    NONE = "none"


class ConfigSource(str, Enum):
    ENV_VAR = "env_var"
    ENV_FILE = "env_file"
    YAML_FILE = "yaml_file"
    JSON_FILE = "json_file"
    DEFAULT = "default"


class AuthMethod(str, Enum):
    TOKEN = "token"
    JWT = "jwt"
    API_KEY = "api_key"
    OAUTH2 = "oauth2"
    NONE = "none"


# =============================================================================
# Utility Functions
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok", "active", "prod"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled", "inactive", "dev"}

_URL_RE = re.compile(r"^https?://", re.IGNORECASE)

def strip_value(v: Any) -> str:
    try:
        return str(v).strip()
    except Exception:
        return ""

def strip_wrapping_quotes(s: str) -> str:
    t = strip_value(s)
    if len(t) >= 2 and ((t[0] == t[-1] == '"') or (t[0] == t[-1] == "'")):
        return t[1:-1].strip()
    return t

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

def coerce_int(v: Any, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        x = int(float(strip_value(v)))
    except Exception:
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x

def coerce_float(v: Any, default: float, *, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    try:
        x = float(strip_value(v))
    except Exception:
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x

def coerce_list(v: Any, default: Optional[List[str]] = None) -> List[str]:
    if v is None:
        return default or []
    if isinstance(v, list):
        return [strip_value(x) for x in v if strip_value(x)]
    s = strip_value(v)
    if not s:
        return default or []
    if s.startswith(("[", "(")):
        try:
            parsed = json_loads(s.replace("'", '"'))
            if isinstance(parsed, list):
                return [strip_value(x) for x in parsed if strip_value(x)]
        except Exception:
            pass
    parts = re.split(r"[\s,;|]+", s)
    return [strip_value(x) for x in parts if strip_value(x)]

def coerce_dict(v: Any, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    ✅ Missing in your v7.5.0 draft.
    Accepts dict or JSON string. Returns dict or default.
    """
    if isinstance(v, dict):
        return v
    s = strip_value(v)
    if not s:
        return default or {}
    try:
        obj = json_loads(s)
        return obj if isinstance(obj, dict) else (default or {})
    except Exception:
        return default or {}

def get_first_env(*keys: str) -> Optional[str]:
    for k in keys:
        v = os.getenv(k)
        if v is not None and strip_value(v):
            return strip_value(v)
    return None

def get_first_env_int(*keys: str, default: int, **kwargs) -> int:
    v = get_first_env(*keys)
    if v is None:
        return default
    return coerce_int(v, default, **kwargs)

def get_first_env_float(*keys: str, default: float, **kwargs) -> float:
    v = get_first_env(*keys)
    if v is None:
        return default
    return coerce_float(v, default, **kwargs)

def get_first_env_list(*keys: str, default: Optional[List[str]] = None) -> List[str]:
    v = get_first_env(*keys)
    if v is None:
        return default or []
    return coerce_list(v, default)

def mask_secret(s: Optional[str], reveal_first: int = 3, reveal_last: int = 3) -> str:
    if s is None:
        return "MISSING"
    x = strip_value(s)
    if not x:
        return "EMPTY"
    if len(x) < reveal_first + reveal_last + 4:
        return "***"
    return f"{x[:reveal_first]}...{x[-reveal_last:]}"

def is_valid_url(url: str) -> bool:
    u = strip_value(url)
    if not u:
        return False
    return bool(_URL_RE.match(u))

def repair_private_key(key: str) -> str:
    if not key:
        return key
    key = key.replace("\\n", "\n").replace("\\r\\n", "\n")
    if "-----BEGIN PRIVATE KEY-----" not in key:
        key = "-----BEGIN PRIVATE KEY-----\n" + key.lstrip()
    if "-----END PRIVATE KEY-----" not in key:
        key = key.rstrip() + "\n-----END PRIVATE KEY-----"
    return key

def repair_json_creds(raw: str) -> Optional[Dict[str, Any]]:
    t = strip_wrapping_quotes(raw or "")
    if not t:
        return None

    # Try base64 decode if it doesn't look like JSON
    if not t.startswith("{") and len(t) > 50:
        try:
            decoded = base64.b64decode(t, validate=False).decode("utf-8", errors="replace").strip()
            if decoded.startswith("{"):
                t = decoded
        except Exception:
            pass

    try:
        obj = json_loads(t)
        if isinstance(obj, dict):
            pk = obj.get("private_key")
            if isinstance(pk, str) and pk:
                obj["private_key"] = repair_private_key(pk)
            return obj
    except Exception as e:
        logger.debug("Failed to parse Google credentials: %s", e)
    return None

def get_system_info() -> Dict[str, Any]:
    info: Dict[str, Any] = {
        "python_version": sys.version.split()[0],
        "platform": sys.platform,
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "cwd": os.getcwd(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    try:
        info["cpu_cores"] = os.cpu_count() or 1
    except Exception:
        info["cpu_cores"] = 1
    return info


# =============================================================================
# Configuration Sources
# =============================================================================
class ConfigSourceLoader:
    @staticmethod
    def from_env_file(path: Union[str, Path]) -> Dict[str, str]:
        p = Path(path)
        if not p.exists():
            return {}
        cfg: Dict[str, str] = {}
        for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            v = strip_wrapping_quotes(v.strip())
            cfg[k.strip()] = v
        return cfg

    @staticmethod
    def from_yaml_file(path: Union[str, Path]) -> Dict[str, Any]:
        if not YAML_AVAILABLE or yaml is None:
            raise ImportError("PyYAML required for YAML config files")
        p = Path(path)
        if not p.exists():
            return {}
        return yaml.safe_load(p.read_text(encoding="utf-8", errors="replace")) or {}

    @staticmethod
    def from_json_file(path: Union[str, Path]) -> Dict[str, Any]:
        """
        ✅ fixed: always uses json_loads (works with orjson or std json)
        """
        p = Path(path)
        if not p.exists():
            return {}
        return json_loads(p.read_text(encoding="utf-8", errors="replace")) or {}


# =============================================================================
# Settings
# =============================================================================
@dataclass(frozen=True)
class Settings:
    env_version: str = ENV_VERSION
    schema_version: str = ENV_SCHEMA_VERSION

    APP_NAME: str = "Tadawul Fast Bridge"
    APP_VERSION: str = "dev"
    APP_ENV: str = "production"

    LOG_LEVEL: str = "INFO"
    LOG_JSON: bool = False
    LOG_FORMAT: str = ""

    TIMEZONE_DEFAULT: str = "Asia/Riyadh"

    AUTH_HEADER_NAME: str = "X-APP-TOKEN"
    APP_TOKEN: Optional[str] = None
    BACKUP_APP_TOKEN: Optional[str] = None
    REQUIRE_AUTH: bool = True
    ALLOW_QUERY_TOKEN: bool = False
    OPEN_MODE: bool = False

    AI_ANALYSIS_ENABLED: bool = True
    ADVANCED_ANALYSIS_ENABLED: bool = True
    ADVISOR_ENABLED: bool = True
    RATE_LIMIT_ENABLED: bool = True

    MAX_REQUESTS_PER_MINUTE: int = 240
    MAX_RETRIES: int = 2
    RETRY_DELAY: float = 0.6

    BACKEND_BASE_URL: str = "http://127.0.0.1:8000"
    ENGINE_CACHE_TTL_SEC: int = 20
    CACHE_MAX_SIZE: int = 5000
    CACHE_SAVE_INTERVAL: int = 300
    CACHE_BACKUP_ENABLED: bool = True

    HTTP_TIMEOUT_SEC: float = 45.0

    TFB_SYMBOL_HEADER_ROW: int = 5
    TFB_SYMBOL_START_ROW: int = 6

    DEFAULT_SPREADSHEET_ID: Optional[str] = None
    GOOGLE_SHEETS_CREDENTIALS: Optional[str] = None
    GOOGLE_APPS_SCRIPT_URL: Optional[str] = None
    GOOGLE_APPS_SCRIPT_BACKUP_URL: Optional[str] = None

    EODHD_API_KEY: Optional[str] = None
    EODHD_BASE_URL: Optional[str] = None
    FINNHUB_API_KEY: Optional[str] = None
    FMP_API_KEY: Optional[str] = None
    FMP_BASE_URL: Optional[str] = None
    ALPHA_VANTAGE_API_KEY: Optional[str] = None
    TWELVEDATA_API_KEY: Optional[str] = None
    MARKETSTACK_API_KEY: Optional[str] = None

    ARGAAM_QUOTE_URL: Optional[str] = None
    ARGAAM_API_KEY: Optional[str] = None
    TADAWUL_QUOTE_URL: Optional[str] = None
    TADAWUL_MARKET_ENABLED: bool = True
    TADAWUL_MAX_SYMBOLS: int = 1500
    TADAWUL_REFRESH_INTERVAL: int = 60
    KSA_DISALLOW_EODHD: bool = True

    ENABLED_PROVIDERS: List[str] = field(default_factory=lambda: ["eodhd", "finnhub"])
    KSA_PROVIDERS: List[str] = field(default_factory=lambda: ["yahoo_chart", "argaam"])
    PRIMARY_PROVIDER: str = "eodhd"

    AI_BATCH_SIZE: int = 20
    AI_MAX_TICKERS: int = 500
    ADV_BATCH_SIZE: int = 25
    BATCH_CONCURRENCY: int = 5

    RENDER_API_KEY: Optional[str] = None
    RENDER_SERVICE_ID: Optional[str] = None
    RENDER_SERVICE_NAME: Optional[str] = None

    ENABLE_CORS_ALL_ORIGINS: bool = False
    CORS_ORIGINS: str = ""

    boot_errors: Tuple[str, ...] = field(default_factory=tuple)
    boot_warnings: Tuple[str, ...] = field(default_factory=tuple)
    config_sources: Dict[str, ConfigSource] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "Settings":
        env_name = strip_value(get_first_env("APP_ENV", "ENVIRONMENT") or "production").lower()

        auto_global: List[str] = []
        if get_first_env("EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN"):
            auto_global.append("eodhd")
        if get_first_env("FINNHUB_API_KEY", "FINNHUB_TOKEN"):
            auto_global.append("finnhub")
        if get_first_env("FMP_API_KEY"):
            auto_global.append("fmp")
        if get_first_env("ALPHA_VANTAGE_API_KEY"):
            auto_global.append("alphavantage")
        if get_first_env("TWELVEDATA_API_KEY"):
            auto_global.append("twelvedata")
        if get_first_env("MARKETSTACK_API_KEY"):
            auto_global.append("marketstack")
        if not auto_global:
            auto_global = ["eodhd", "finnhub"]

        auto_ksa: List[str] = []
        if get_first_env("ARGAAM_QUOTE_URL"):
            auto_ksa.append("argaam")
        if get_first_env("TADAWUL_QUOTE_URL"):
            auto_ksa.append("tadawul")
        if coerce_bool(get_first_env("ENABLE_YAHOO_CHART_KSA", "ENABLE_YFINANCE_KSA"), True):
            auto_ksa.insert(0, "yahoo_chart")
        if not auto_ksa:
            auto_ksa = ["yahoo_chart", "argaam"]

        google_creds_raw = get_first_env("GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS") or ""
        google_creds_obj = repair_json_creds(google_creds_raw)
        google_creds_str = json_dumps(google_creds_obj) if google_creds_obj else None

        s = cls(
            APP_NAME=get_first_env("APP_NAME", "SERVICE_NAME", "APP_TITLE") or "Tadawul Fast Bridge",
            APP_VERSION=get_first_env("APP_VERSION", "SERVICE_VERSION", "VERSION") or "dev",
            APP_ENV=env_name,
            LOG_LEVEL=get_first_env("LOG_LEVEL") or "INFO",
            LOG_JSON=coerce_bool(get_first_env("LOG_JSON"), False),
            LOG_FORMAT=get_first_env("LOG_FORMAT") or "",
            TIMEZONE_DEFAULT=get_first_env("APP_TIMEZONE", "TIMEZONE_DEFAULT", "TZ") or "Asia/Riyadh",
            AUTH_HEADER_NAME=get_first_env("AUTH_HEADER_NAME", "TOKEN_HEADER_NAME") or "X-APP-TOKEN",
            APP_TOKEN=get_first_env("APP_TOKEN", "TFB_APP_TOKEN"),
            BACKUP_APP_TOKEN=get_first_env("BACKUP_APP_TOKEN"),
            REQUIRE_AUTH=coerce_bool(get_first_env("REQUIRE_AUTH"), True),
            ALLOW_QUERY_TOKEN=coerce_bool(get_first_env("ALLOW_QUERY_TOKEN"), False),
            OPEN_MODE=coerce_bool(get_first_env("OPEN_MODE"), False),
            AI_ANALYSIS_ENABLED=coerce_bool(get_first_env("AI_ANALYSIS_ENABLED"), True),
            ADVANCED_ANALYSIS_ENABLED=coerce_bool(get_first_env("ADVANCED_ANALYSIS_ENABLED", "ADVANCED_ENABLED"), True),
            ADVISOR_ENABLED=coerce_bool(get_first_env("ADVISOR_ENABLED"), True),
            RATE_LIMIT_ENABLED=coerce_bool(get_first_env("ENABLE_RATE_LIMITING", "RATE_LIMIT_ENABLED"), True),
            MAX_REQUESTS_PER_MINUTE=get_first_env_int("MAX_REQUESTS_PER_MINUTE", default=240, lo=30, hi=5000),
            MAX_RETRIES=get_first_env_int("MAX_RETRIES", default=2, lo=0, hi=10),
            RETRY_DELAY=get_first_env_float("RETRY_DELAY", default=0.6, lo=0.0, hi=30.0),
            BACKEND_BASE_URL=get_first_env("BACKEND_BASE_URL", "TFB_BASE_URL") or "http://127.0.0.1:8000",
            ENGINE_CACHE_TTL_SEC=get_first_env_int("ENGINE_CACHE_TTL_SEC", "CACHE_DEFAULT_TTL", default=20, lo=5, hi=3600),
            CACHE_MAX_SIZE=get_first_env_int("CACHE_MAX_SIZE", default=5000, lo=100, hi=500000),
            CACHE_SAVE_INTERVAL=get_first_env_int("CACHE_SAVE_INTERVAL", default=300, lo=30, hi=86400),
            CACHE_BACKUP_ENABLED=coerce_bool(get_first_env("CACHE_BACKUP_ENABLED"), True),
            HTTP_TIMEOUT_SEC=get_first_env_float("HTTP_TIMEOUT_SEC", "HTTP_TIMEOUT", default=45.0, lo=5.0, hi=180.0),
            TFB_SYMBOL_HEADER_ROW=get_first_env_int("TFB_SYMBOL_HEADER_ROW", default=5, lo=1, hi=1000),
            TFB_SYMBOL_START_ROW=get_first_env_int("TFB_SYMBOL_START_ROW", default=6, lo=1, hi=1000),
            DEFAULT_SPREADSHEET_ID=get_first_env("DEFAULT_SPREADSHEET_ID", "SPREADSHEET_ID", "GOOGLE_SHEETS_ID"),
            GOOGLE_SHEETS_CREDENTIALS=google_creds_str,
            GOOGLE_APPS_SCRIPT_URL=get_first_env("GOOGLE_APPS_SCRIPT_URL"),
            GOOGLE_APPS_SCRIPT_BACKUP_URL=get_first_env("GOOGLE_APPS_SCRIPT_BACKUP_URL"),
            EODHD_API_KEY=get_first_env("EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN"),
            EODHD_BASE_URL=get_first_env("EODHD_BASE_URL"),
            FINNHUB_API_KEY=get_first_env("FINNHUB_API_KEY", "FINNHUB_TOKEN"),
            FMP_API_KEY=get_first_env("FMP_API_KEY"),
            FMP_BASE_URL=get_first_env("FMP_BASE_URL"),
            ALPHA_VANTAGE_API_KEY=get_first_env("ALPHA_VANTAGE_API_KEY"),
            TWELVEDATA_API_KEY=get_first_env("TWELVEDATA_API_KEY"),
            MARKETSTACK_API_KEY=get_first_env("MARKETSTACK_API_KEY"),
            ARGAAM_QUOTE_URL=get_first_env("ARGAAM_QUOTE_URL"),
            ARGAAM_API_KEY=get_first_env("ARGAAM_API_KEY"),
            TADAWUL_QUOTE_URL=get_first_env("TADAWUL_QUOTE_URL"),
            TADAWUL_MARKET_ENABLED=coerce_bool(get_first_env("TADAWUL_MARKET_ENABLED"), True),
            TADAWUL_MAX_SYMBOLS=get_first_env_int("TADAWUL_MAX_SYMBOLS", default=1500, lo=10, hi=50000),
            TADAWUL_REFRESH_INTERVAL=get_first_env_int("TADAWUL_REFRESH_INTERVAL", default=60, lo=5, hi=3600),
            KSA_DISALLOW_EODHD=coerce_bool(get_first_env("KSA_DISALLOW_EODHD"), True),
            ENABLED_PROVIDERS=get_first_env_list("ENABLED_PROVIDERS", "PROVIDERS", default=auto_global),
            KSA_PROVIDERS=get_first_env_list("KSA_PROVIDERS", default=auto_ksa),
            PRIMARY_PROVIDER=get_first_env("PRIMARY_PROVIDER") or (auto_global[0] if auto_global else "eodhd"),
            AI_BATCH_SIZE=get_first_env_int("AI_BATCH_SIZE", default=20, lo=1, hi=200),
            AI_MAX_TICKERS=get_first_env_int("AI_MAX_TICKERS", default=500, lo=1, hi=20000),
            ADV_BATCH_SIZE=get_first_env_int("ADV_BATCH_SIZE", default=25, lo=1, hi=500),
            BATCH_CONCURRENCY=get_first_env_int("BATCH_CONCURRENCY", default=5, lo=1, hi=50),
            RENDER_API_KEY=get_first_env("RENDER_API_KEY"),
            RENDER_SERVICE_ID=get_first_env("RENDER_SERVICE_ID"),
            RENDER_SERVICE_NAME=get_first_env("RENDER_SERVICE_NAME"),
            ENABLE_CORS_ALL_ORIGINS=coerce_bool(get_first_env("ENABLE_CORS_ALL_ORIGINS", "CORS_ALL_ORIGINS"), False),
            CORS_ORIGINS=get_first_env("CORS_ORIGINS") or "",
            config_sources={"env": ConfigSource.ENV_VAR},
        )

        errors, warns = s.validate()
        return replace(s, boot_errors=tuple(errors), boot_warnings=tuple(warns))

    def validate(self) -> Tuple[List[str], List[str]]:
        errors: List[str] = []
        warns: List[str] = []

        if not self.OPEN_MODE and not self.APP_TOKEN:
            errors.append("Authentication required but no APP_TOKEN configured.")
        if self.OPEN_MODE and self.APP_TOKEN:
            warns.append("OPEN_MODE enabled while APP_TOKEN exists. Endpoints exposed publicly.")
        if "eodhd" in self.ENABLED_PROVIDERS and not self.EODHD_API_KEY:
            warns.append("Provider 'eodhd' enabled but EODHD_API_KEY missing.")
        if "finnhub" in self.ENABLED_PROVIDERS and not self.FINNHUB_API_KEY:
            warns.append("Provider 'finnhub' enabled but FINNHUB_API_KEY missing.")

        return errors, warns

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["APP_TOKEN"] = mask_secret(self.APP_TOKEN)
        d["BACKUP_APP_TOKEN"] = mask_secret(self.BACKUP_APP_TOKEN)
        d["EODHD_API_KEY"] = mask_secret(self.EODHD_API_KEY)
        d["FINNHUB_API_KEY"] = mask_secret(self.FINNHUB_API_KEY)
        d["GOOGLE_SHEETS_CREDENTIALS"] = "PRESENT" if self.GOOGLE_SHEETS_CREDENTIALS else "MISSING"
        d["config_sources"] = {k: v.value for k, v in self.config_sources.items()}
        return d

    def compliance_report(self) -> Dict[str, Any]:
        errors, warns = self.validate()
        return {
            "version": self.env_version,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "environment": self.APP_ENV,
            "security": {
                "auth_enabled": self.REQUIRE_AUTH and not self.OPEN_MODE,
                "rate_limiting": self.RATE_LIMIT_ENABLED,
                "tokens_masked": {"app_token": mask_secret(self.APP_TOKEN)},
            },
            "validation": {"errors": errors, "warnings": warns, "is_valid": len(errors) == 0},
        }

    def feature_enabled(self, feature: str) -> bool:
        flags = {
            "ai_analysis": self.AI_ANALYSIS_ENABLED,
            "advanced_analysis": self.ADVANCED_ANALYSIS_ENABLED,
            "advisor": self.ADVISOR_ENABLED,
            "rate_limiting": self.RATE_LIMIT_ENABLED,
            "open_mode": self.OPEN_MODE,
        }
        return bool(flags.get(feature, False))


# =============================================================================
# Dynamic Configuration Manager
# =============================================================================
class DynamicConfig:
    def __init__(self, initial_settings: Settings):
        self._settings = initial_settings
        self._lock = threading.RLock()
        self._observers: List[Callable[[Settings], None]] = []
        self._last_reload = datetime.now(timezone.utc)
        self._reload_count = 0

    @property
    def settings(self) -> Settings:
        with self._lock:
            return self._settings

    def reload(self) -> bool:
        with TraceContext("env_reload_config"):
            start = time.time()
            try:
                new_settings = Settings.from_env()
                with self._lock:
                    self._settings = new_settings
                    self._last_reload = datetime.now(timezone.utc)
                    self._reload_count += 1

                env_config_loads_total.labels(source="dynamic_reload", status="success").inc()
                env_config_reload_duration.observe(max(0.0, time.time() - start))
                logger.info("Configuration reloaded (count=%s)", self._reload_count)

                for cb in list(self._observers):
                    try:
                        cb(new_settings)
                    except Exception as e:
                        logger.error("Observer callback failed: %s", e)

                return True
            except Exception as e:
                env_config_loads_total.labels(source="dynamic_reload", status="error").inc()
                env_config_errors_total.labels(type=type(e).__name__).inc()
                logger.error("Configuration reload failed: %s", e)
                return False


# =============================================================================
# Singleton Instance
# =============================================================================
_settings_instance: Optional[Settings] = None
_dynamic_config: Optional[DynamicConfig] = None
_init_lock = threading.Lock()

def get_settings() -> Settings:
    global _settings_instance
    if _settings_instance is None:
        with _init_lock:
            if _settings_instance is None:
                _settings_instance = Settings.from_env()
    return _settings_instance

def get_dynamic_config() -> DynamicConfig:
    global _dynamic_config
    if _dynamic_config is None:
        with _init_lock:
            if _dynamic_config is None:
                _dynamic_config = DynamicConfig(get_settings())
    return _dynamic_config

def reload_config() -> bool:
    return get_dynamic_config().reload()

def compliance_report() -> Dict[str, Any]:
    return get_settings().compliance_report()

def feature_enabled(feature: str) -> bool:
    return get_settings().feature_enabled(feature)

class CompatibilitySettings:
    def __init__(self, settings: Settings):
        object.__setattr__(self, "_settings", settings)

    def __getattr__(self, name: str):
        return getattr(self._settings, name)

    def __setattr__(self, name: str, value: Any):
        raise AttributeError("Settings are immutable")

settings = CompatibilitySettings(get_settings())

# Backward compatibility exports
APP_NAME = settings.APP_NAME
APP_VERSION = settings.APP_VERSION
APP_ENV = settings.APP_ENV
LOG_LEVEL = settings.LOG_LEVEL
TIMEZONE_DEFAULT = settings.TIMEZONE_DEFAULT
AUTH_HEADER_NAME = settings.AUTH_HEADER_NAME
TFB_SYMBOL_HEADER_ROW = settings.TFB_SYMBOL_HEADER_ROW
TFB_SYMBOL_START_ROW = settings.TFB_SYMBOL_START_ROW
APP_TOKEN = settings.APP_TOKEN
BACKUP_APP_TOKEN = settings.BACKUP_APP_TOKEN
REQUIRE_AUTH = settings.REQUIRE_AUTH
ALLOW_QUERY_TOKEN = settings.ALLOW_QUERY_TOKEN
BACKEND_BASE_URL = settings.BACKEND_BASE_URL
HTTP_TIMEOUT_SEC = settings.HTTP_TIMEOUT_SEC
ENGINE_CACHE_TTL_SEC = settings.ENGINE_CACHE_TTL_SEC
AI_ANALYSIS_ENABLED = settings.AI_ANALYSIS_ENABLED
ADVANCED_ANALYSIS_ENABLED = settings.ADVANCED_ANALYSIS_ENABLED
ADVISOR_ENABLED = settings.ADVISOR_ENABLED
RATE_LIMIT_ENABLED = settings.RATE_LIMIT_ENABLED
MAX_REQUESTS_PER_MINUTE = settings.MAX_REQUESTS_PER_MINUTE
DEFAULT_SPREADSHEET_ID = settings.DEFAULT_SPREADSHEET_ID
GOOGLE_SHEETS_CREDENTIALS = settings.GOOGLE_SHEETS_CREDENTIALS
ENABLED_PROVIDERS = settings.ENABLED_PROVIDERS
KSA_PROVIDERS = settings.KSA_PROVIDERS
PRIMARY_PROVIDER = settings.PRIMARY_PROVIDER

__all__ = [
    "ENV_VERSION",
    "Environment",
    "LogLevel",
    "ProviderType",
    "CacheBackend",
    "ConfigSource",
    "AuthMethod",
    "Settings",
    "DynamicConfig",
    "get_settings",
    "get_dynamic_config",
    "reload_config",
    "compliance_report",
    "feature_enabled",
    "settings",
    "strip_value",
    "strip_wrapping_quotes",
    "coerce_bool",
    "coerce_int",
    "coerce_float",
    "coerce_list",
    "coerce_dict",
    "get_first_env",
    "mask_secret",
    "is_valid_url",
    "repair_private_key",
    "repair_json_creds",
    "get_system_info",
    "APP_NAME",
    "APP_VERSION",
    "APP_ENV",
    "LOG_LEVEL",
    "TIMEZONE_DEFAULT",
    "AUTH_HEADER_NAME",
    "TFB_SYMBOL_HEADER_ROW",
    "TFB_SYMBOL_START_ROW",
    "APP_TOKEN",
    "BACKUP_APP_TOKEN",
    "REQUIRE_AUTH",
    "ALLOW_QUERY_TOKEN",
    "BACKEND_BASE_URL",
    "HTTP_TIMEOUT_SEC",
    "ENGINE_CACHE_TTL_SEC",
    "AI_ANALYSIS_ENABLED",
    "ADVANCED_ANALYSIS_ENABLED",
    "ADVISOR_ENABLED",
    "RATE_LIMIT_ENABLED",
    "MAX_REQUESTS_PER_MINUTE",
    "DEFAULT_SPREADSHEET_ID",
    "GOOGLE_SHEETS_CREDENTIALS",
    "ENABLED_PROVIDERS",
    "KSA_PROVIDERS",
    "PRIMARY_PROVIDER",
]
