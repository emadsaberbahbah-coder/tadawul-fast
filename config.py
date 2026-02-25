#!/usr/bin/env python3
# core/config.py
"""
================================================================================
Core Configuration Module — v6.0.1 (QUANTUM EDITION)
================================================================================
TADAWUL FAST BRIDGE – Enterprise Configuration Management

Design Goals
- ✅ Never breaks startup (all optional dependencies are safe)
- ✅ Central source of truth for env parsing + defaults
- ✅ Deterministic behavior (no random failures; safe fallbacks)
- ✅ Compatible with integrations/google_sheets_service.py + setup_credentials.py

Key Fixes vs your pasted draft
- ✅ Added missing utilities: TraceContext, _dbg, deep_merge, load_file_content
- ✅ Removed broken @TraceContext decorator usage (now works as context manager + decorator)
- ✅ Fixed name conflicts: custom ValidationError renamed to ConfigValidationError
- ✅ Fixed __all__ exporting undefined names (kept compatibility aliases)
- ✅ Added Settings fields used by Google Sheets integrations:
    - google_sheets_credentials_json
    - google_credentials_dict
    - default_spreadsheet_id

Note
This file intentionally does NOT force-connect to Consul/etcd/ZK unless env vars are present.
"""

from __future__ import annotations

import base64
import copy
import json
import logging
import os
import random
import re
import threading
import time
import uuid
import zlib
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from threading import RLock
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse

# Version
CONFIG_VERSION = "6.0.1"
CONFIG_SCHEMA_VERSION = "2.2"
CONFIG_BUILD_TIMESTAMP = datetime.now(timezone.utc).isoformat()

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# =============================================================================
# High-Performance JSON fallback
# =============================================================================
try:
    import orjson  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

    def json_dumps(obj: Any, *, default=str) -> str:
        return orjson.dumps(obj, default=default).decode("utf-8")

    _HAS_ORJSON = True
except Exception:
    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    def json_dumps(obj: Any, *, default=str) -> str:
        return json.dumps(obj, default=default, ensure_ascii=False)

    _HAS_ORJSON = False

# =============================================================================
# Optional Dependencies with Graceful Degradation
# =============================================================================

# YAML support
try:
    import yaml  # type: ignore

    YAML_AVAILABLE = True
except Exception:
    yaml = None
    YAML_AVAILABLE = False

# TOML support
try:
    import toml  # type: ignore

    TOML_AVAILABLE = True
except Exception:
    toml = None
    TOML_AVAILABLE = False

# Pydantic support (optional)
try:
    from pydantic import BaseModel, Field  # type: ignore
    from pydantic import ConfigDict  # type: ignore

    PYDANTIC_AVAILABLE = True
    PYDANTIC_V2 = True
except Exception:
    try:
        from pydantic import BaseModel, Field  # type: ignore

        PYDANTIC_AVAILABLE = True
        PYDANTIC_V2 = False
        ConfigDict = None  # type: ignore
    except Exception:
        BaseModel = object  # type: ignore
        Field = lambda default=None, **kwargs: default  # type: ignore
        PYDANTIC_AVAILABLE = False
        PYDANTIC_V2 = False
        ConfigDict = None  # type: ignore

# OpenTelemetry (optional)
try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore

    OTEL_AVAILABLE = True
except Exception:
    trace = None  # type: ignore
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    OTEL_AVAILABLE = False

# Consul / etcd / ZooKeeper (optional)
try:
    import consul  # type: ignore

    CONSUL_AVAILABLE = True
except Exception:
    consul = None  # type: ignore
    CONSUL_AVAILABLE = False

try:
    import etcd3  # type: ignore

    ETCD_AVAILABLE = True
except Exception:
    etcd3 = None  # type: ignore
    ETCD_AVAILABLE = False

try:
    from kazoo.client import KazooClient  # type: ignore

    ZOOKEEPER_AVAILABLE = True
except Exception:
    KazooClient = None  # type: ignore
    ZOOKEEPER_AVAILABLE = False

# =============================================================================
# Tracing Context (Sync + Decorator)
# =============================================================================

_TRACING_ENABLED = (os.getenv("TRACING_ENABLED", "") or os.getenv("CORE_TRACING_ENABLED", "")).strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}


class TraceContext:
    """
    Lightweight OpenTelemetry wrapper.
    - Usable as context manager: with TraceContext("name"): ...
    - Usable as decorator: @TraceContext("name")
    """

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
                for k, v in (self.attributes or {}).items():
                    self._span.set_attribute(k, v)
            except Exception:
                pass
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._span is not None and OTEL_AVAILABLE and _TRACING_ENABLED:
            try:
                if exc is not None and Status is not None and StatusCode is not None:
                    self._span.record_exception(exc)
                    self._span.set_status(Status(StatusCode.ERROR, str(exc)))
            except Exception:
                pass
        if self._span_cm is not None:
            try:
                return self._span_cm.__exit__(exc_type, exc, tb)
            except Exception:
                return False
        return False

    def __call__(self, fn: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            with TraceContext(self.name, self.attributes):
                return fn(*args, **kwargs)

        wrapper.__name__ = getattr(fn, "__name__", "wrapped")
        wrapper.__doc__ = getattr(fn, "__doc__", None)
        return wrapper


# =============================================================================
# Utility Functions
# =============================================================================

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "active"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "inactive"}

_URL_RE = re.compile(r"^https?://", re.IGNORECASE)


def get_correlation_id() -> str:
    return str(uuid.uuid4())


def strip_value(v: Any) -> str:
    if v is None:
        return ""
    try:
        return str(v).strip()
    except Exception:
        return ""


def coerce_bool(v: Any, default: bool) -> bool:
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
        x = int(float(v)) if isinstance(v, (int, float)) else int(float(strip_value(v)))
    except Exception:
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


def coerce_float(v: Any, default: float, *, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    try:
        x = float(v) if isinstance(v, (int, float)) else float(strip_value(v))
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
        return default
    if isinstance(v, list):
        return [strip_value(x) for x in v if strip_value(x)]
    s = strip_value(v)
    if not s:
        return default
    # Try JSON list
    if s.startswith("[") and s.endswith("]"):
        try:
            parsed = json_loads(s)
            if isinstance(parsed, list):
                return [strip_value(x) for x in parsed if strip_value(x)]
        except Exception:
            pass
    return [x.strip() for x in s.split(",") if x.strip()]


def coerce_dict(v: Any, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if default is None:
        default = {}
    if v is None:
        return default
    if isinstance(v, dict):
        return v
    s = strip_value(v)
    if not s:
        return default
    try:
        parsed = json_loads(s)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    return default


def is_valid_url(url: str) -> bool:
    return bool(_URL_RE.match(strip_value(url)))


SENSITIVE_KEYS = {
    "token",
    "secret",
    "key",
    "api_key",
    "apikey",
    "password",
    "credential",
    "authorization",
    "bearer",
    "jwt",
    "private_key",
    "client_secret",
    "encryption_key",
}


def mask_secret(s: Optional[str], reveal_first: int = 2, reveal_last: int = 4) -> Optional[str]:
    if not s:
        return None
    s = strip_value(s)
    if len(s) <= reveal_first + reveal_last + 3:
        return "***"
    return s[:reveal_first] + "..." + s[-reveal_last:]


def mask_secret_dict(d: Dict[str, Any], sensitive_keys: Optional[Set[str]] = None) -> Dict[str, Any]:
    if sensitive_keys is None:
        sensitive_keys = set(SENSITIVE_KEYS)
    out: Dict[str, Any] = {}
    for k, v in (d or {}).items():
        lk = str(k).lower()
        if isinstance(v, dict):
            out[k] = mask_secret_dict(v, sensitive_keys)
        elif isinstance(v, list):
            out[k] = [mask_secret_dict(x, sensitive_keys) if isinstance(x, dict) else x for x in v]
        elif any(sk in lk for sk in sensitive_keys) and isinstance(v, (str, bytes)):
            out[k] = mask_secret(str(v))
        else:
            out[k] = v
    return out


def _dbg(message: str, level: str = "info", **kwargs) -> None:
    """
    Internal logging helper. Never throws.
    """
    try:
        payload = {"message": message, **kwargs}
        if level.lower() == "error":
            logger.error(json_dumps(payload))
        elif level.lower() in ("warn", "warning"):
            logger.warning(json_dumps(payload))
        elif level.lower() == "debug":
            logger.debug(json_dumps(payload))
        else:
            logger.info(json_dumps(payload))
    except Exception:
        # absolute last resort
        pass


def deep_merge(base: Dict[str, Any], override: Dict[str, Any], *, overwrite: bool = True) -> Dict[str, Any]:
    """
    Deep merge dictionaries.
    - If overwrite=True: override wins.
    - Lists are replaced (not concatenated) to keep deterministic configuration.
    """
    if not isinstance(base, dict):
        base = {}
    if not isinstance(override, dict):
        return dict(base)

    out = copy.deepcopy(base)
    for k, v in override.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = deep_merge(out[k], v, overwrite=overwrite)
        else:
            if overwrite or k not in out:
                out[k] = copy.deepcopy(v)
    return out


def load_file_content(path: Path) -> Dict[str, Any]:
    """
    Load config from JSON/YAML/TOML. Never throws; returns {} on failure.
    """
    try:
        if not path.exists() or not path.is_file():
            return {}
        raw = path.read_text(encoding="utf-8", errors="replace").strip()
        if not raw:
            return {}
        suffix = path.suffix.lower()

        if suffix in (".json",):
            obj = json_loads(raw)
            return obj if isinstance(obj, dict) else {}

        if suffix in (".yml", ".yaml") and YAML_AVAILABLE and yaml is not None:
            obj = yaml.safe_load(raw)
            return obj if isinstance(obj, dict) else {}

        if suffix in (".toml",) and TOML_AVAILABLE and toml is not None:
            obj = toml.loads(raw)
            return obj if isinstance(obj, dict) else {}

        # Fallback: try JSON
        obj = json_loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception as e:
        _dbg(f"load_file_content failed: {e}", "warning", path=str(path))
        return {}


# =============================================================================
# Enums & Types
# =============================================================================

class Environment(str, Enum):
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"
    LOCAL = "local"
    DOCKER = "docker"
    KUBERNETES = "kubernetes"
    CANARY = "canary"
    BLUE = "blue"
    GREEN = "green"
    DR = "dr"

    @classmethod
    def from_string(cls, value: str) -> "Environment":
        v = strip_value(value).lower()
        try:
            return cls(v)
        except Exception:
            return cls.PRODUCTION


class LogLevel(str, Enum):
    TRACE = "TRACE"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    AUDIT = "AUDIT"

    def to_int(self) -> int:
        return {
            "TRACE": 5,
            "DEBUG": 10,
            "INFO": 20,
            "WARNING": 30,
            "ERROR": 40,
            "CRITICAL": 50,
            "AUDIT": 60,
        }[self.value]


class ProviderType(str, Enum):
    EODHD = "eodhd"
    FINNHUB = "finnhub"
    ALPHAVANTAGE = "alphavantage"
    YAHOO = "yahoo"
    YAHOO_CHART = "yahoo_chart"
    YAHOO_FUNDAMENTALS = "yahoo_fundamentals"
    ARGAAM = "argaam"
    CUSTOM = "custom"

    @classmethod
    def requires_api_key(cls, provider: str) -> bool:
        p = strip_value(provider).lower()
        return p not in {"yahoo", "yahoo_chart", "yahoo_fundamentals", "custom", "argaam"}


class CacheBackend(str, Enum):
    MEMORY = "memory"
    REDIS = "redis"
    MEMCACHED = "memcached"
    NONE = "none"

    @property
    def is_distributed(self) -> bool:
        return self in {CacheBackend.REDIS, CacheBackend.MEMCACHED}


# Compatibility aliases (to avoid breaking older imports)
CacheStrategy = CacheBackend  # alias
ProviderStatus = Enum("ProviderStatus", {"ENABLED": "enabled", "DISABLED": "disabled"})  # minimal


class ConfigSource(str, Enum):
    ENV_VAR = "env_var"
    ENV_FILE = "env_file"
    YAML_FILE = "yaml_file"
    JSON_FILE = "json_file"
    CONSUL = "consul"
    ETCD = "etcd"
    ZOOKEEPER = "zookeeper"
    DEFAULT = "default"
    RUNTIME = "runtime"

    @property
    def priority(self) -> int:
        return {
            ConfigSource.RUNTIME: 100,
            ConfigSource.ENV_VAR: 80,
            ConfigSource.CONSUL: 65,
            ConfigSource.ETCD: 64,
            ConfigSource.ZOOKEEPER: 63,
            ConfigSource.ENV_FILE: 50,
            ConfigSource.JSON_FILE: 40,
            ConfigSource.YAML_FILE: 40,
            ConfigSource.DEFAULT: 10,
        }.get(self, 0)


class ConfigStatus(str, Enum):
    ACTIVE = "active"
    PENDING = "pending"
    ROLLED_BACK = "rolled_back"
    ARCHIVED = "archived"
    CORRUPT = "corrupt"


class EncryptionMethod(str, Enum):
    NONE = "none"
    BASE64 = "base64"
    AES = "aes"
    FERNET = "fernet"


class AuthType(str, Enum):
    NONE = "none"
    TOKEN = "token"
    BEARER = "bearer"
    API_KEY = "api_key"
    JWT = "jwt"


class ConfigEncryptionAlgorithm(str, Enum):
    FERNET = "fernet"
    AES_GCM = "aes_gcm"
    NONE = "none"


# =============================================================================
# Exceptions
# =============================================================================
class ConfigurationError(Exception):
    pass


class ConfigValidationError(ConfigurationError):
    pass


class SourceError(ConfigurationError):
    pass


class ReloadError(ConfigurationError):
    pass


# =============================================================================
# Full Jitter Exponential Backoff
# =============================================================================
class FullJitterBackoff:
    """AWS-style Full Jitter backoff. Safe and deterministic bounds."""
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 10.0):
        self.max_retries = max(1, int(max_retries))
        self.base_delay = max(0.01, float(base_delay))
        self.max_delay = max(self.base_delay, float(max_delay))

    def execute_sync(self, func: Callable, *args, **kwargs) -> Any:
        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_err = e
                if attempt >= self.max_retries - 1:
                    raise
                cap = min(self.max_delay, self.base_delay * (2 ** attempt))
                time.sleep(random.uniform(0, cap))
        if last_err:
            raise last_err
        raise RuntimeError("Backoff failed without error (unexpected).")


# =============================================================================
# Optional Pydantic Models (kept small + safe)
# =============================================================================
if PYDANTIC_AVAILABLE:
    class SettingsBaseModel(BaseModel):
        if PYDANTIC_V2:
            model_config = ConfigDict(
                validate_assignment=True,
                arbitrary_types_allowed=True,
                use_enum_values=True,
                extra="ignore",
            )
        else:
            class Config:
                validate_assignment = True
                arbitrary_types_allowed = True
                use_enum_values = True
                extra = "ignore"

    class RateLimitConfig(SettingsBaseModel):
        enabled: bool = True
        rate_per_sec: float = 10.0
        burst_size: int = 20
        retry_after: int = 60

    class CircuitBreakerConfig(SettingsBaseModel):
        enabled: bool = True
        failure_threshold: int = 5
        timeout_seconds: int = 60

    class CacheConfig(SettingsBaseModel):
        backend: CacheBackend = Field(CacheBackend.MEMORY)
        default_ttl: int = Field(300, ge=1, le=86400)
        prefix: str = Field("tfb:")

    class ProviderConfig(SettingsBaseModel):
        name: ProviderType
        api_key: Optional[str] = None
        base_url: Optional[str] = None
        timeout_seconds: float = 30.0
        retry_attempts: int = 3
        enabled: bool = True

    class AuthConfig(SettingsBaseModel):
        type: AuthType = AuthType.NONE
        tokens: List[str] = Field(default_factory=list)

    class MetricsConfig(SettingsBaseModel):
        enabled: bool = True
        port: int = 9090
        path: str = "/metrics"

    class LoggingConfig(SettingsBaseModel):
        level: LogLevel = LogLevel.INFO
        format: str = "json"
else:
    # Fallback placeholders
    RateLimitConfig = None  # type: ignore
    CircuitBreakerConfig = None  # type: ignore
    CacheConfig = None  # type: ignore
    ProviderConfig = None  # type: ignore
    AuthConfig = None  # type: ignore
    MetricsConfig = None  # type: ignore
    LoggingConfig = None  # type: ignore


# =============================================================================
# Distributed Configuration Sources (Optional)
# =============================================================================
class ConfigSourceManager:
    def __init__(self):
        self.sources: List[Tuple[ConfigSource, Callable[[], Optional[Dict[str, Any]]]]] = []
        self._lock = threading.RLock()

    def register_source(self, source: ConfigSource, loader: Callable[[], Optional[Dict[str, Any]]]) -> None:
        with self._lock:
            self.sources.append((source, loader))

    def load_all(self) -> Dict[ConfigSource, Dict[str, Any]]:
        out: Dict[ConfigSource, Dict[str, Any]] = {}
        with self._lock:
            for src, loader in self.sources:
                try:
                    data = loader()
                    if isinstance(data, dict) and data:
                        out[src] = data
                except Exception as e:
                    _dbg(f"Failed to load from {src.value}: {e}", "warning")
        return out

    def load_merged(self, priority: Optional[List[ConfigSource]] = None) -> Dict[str, Any]:
        loaded = self.load_all()
        if not loaded:
            return {}
        if priority is None:
            priority = [ConfigSource.ENV_VAR, ConfigSource.CONSUL, ConfigSource.ETCD, ConfigSource.ZOOKEEPER, ConfigSource.DEFAULT]
        merged: Dict[str, Any] = {}
        for src in priority:
            if src in loaded:
                merged = deep_merge(merged, loaded[src], overwrite=True)
        return merged


_CONFIG_SOURCES = ConfigSourceManager()


class ConsulConfigSource:
    def __init__(self, host: str = "localhost", port: int = 8500, token: Optional[str] = None, prefix: str = "config/tadawul"):
        self.prefix = prefix
        self.client = None
        self._connected = False

        if CONSUL_AVAILABLE and consul is not None:
            backoff = FullJitterBackoff(max_retries=2, base_delay=0.5, max_delay=2.0)

            def _connect():
                c = consul.Consul(host=host, port=port, token=token)
                c.status.leader()
                return c

            try:
                self.client = backoff.execute_sync(_connect)
                self._connected = True
                _dbg(f"Connected to Consul {host}:{port}", "info")
            except Exception as e:
                _dbg(f"Consul connect failed: {e}", "warning")

    def load(self) -> Optional[Dict[str, Any]]:
        if not self._connected or not self.client:
            return None
        try:
            _, data = self.client.kv.get(self.prefix, recurse=True)
            if not data:
                return None
            out: Dict[str, Any] = {}
            for item in data:
                k = item.get("Key") or ""
                if not k:
                    continue
                rel = k[len(self.prefix) + 1 :] if k.startswith(self.prefix) else k
                raw = item.get("Value")
                if raw is None:
                    continue
                try:
                    val = json_loads(raw.decode("utf-8", errors="replace"))
                except Exception:
                    val = raw.decode("utf-8", errors="replace")

                parts = [p for p in rel.split("/") if p]
                cur = out
                for p in parts[:-1]:
                    if p not in cur or not isinstance(cur[p], dict):
                        cur[p] = {}
                    cur = cur[p]
                if parts:
                    cur[parts[-1]] = val
            return out or None
        except Exception as e:
            _dbg(f"Consul load failed: {e}", "warning")
            return None


class EtcdConfigSource:
    def __init__(self, host: str = "localhost", port: int = 2379, prefix: str = "/config/tadawul"):
        self.prefix = prefix
        self.client = None
        self._connected = False

        if ETCD_AVAILABLE and etcd3 is not None:
            backoff = FullJitterBackoff(max_retries=2, base_delay=0.5, max_delay=2.0)

            def _connect():
                c = etcd3.client(host=host, port=port)
                try:
                    c.status()
                except Exception:
                    pass
                return c

            try:
                self.client = backoff.execute_sync(_connect)
                self._connected = True
                _dbg(f"Connected to etcd {host}:{port}", "info")
            except Exception as e:
                _dbg(f"etcd connect failed: {e}", "warning")

    def load(self) -> Optional[Dict[str, Any]]:
        if not self._connected or not self.client:
            return None
        try:
            out: Dict[str, Any] = {}
            for value, meta in self.client.get_prefix(self.prefix):
                key = (meta.key or b"").decode("utf-8", errors="replace")
                rel = key[len(self.prefix) :].lstrip("/")
                try:
                    val = json_loads(value.decode("utf-8", errors="replace"))
                except Exception:
                    val = value.decode("utf-8", errors="replace")
                parts = [p for p in rel.split("/") if p]
                cur = out
                for p in parts[:-1]:
                    if p not in cur or not isinstance(cur[p], dict):
                        cur[p] = {}
                    cur = cur[p]
                if parts:
                    cur[parts[-1]] = val
            return out or None
        except Exception as e:
            _dbg(f"etcd load failed: {e}", "warning")
            return None


class ZooKeeperConfigSource:
    def __init__(self, hosts: str, prefix: str = "/config/tadawul"):
        self.prefix = prefix
        self.client = None
        self._connected = False

        if ZOOKEEPER_AVAILABLE and KazooClient is not None:
            backoff = FullJitterBackoff(max_retries=2, base_delay=0.5, max_delay=2.0)

            def _connect():
                c = KazooClient(hosts=hosts)
                c.start(timeout=5)
                return c

            try:
                self.client = backoff.execute_sync(_connect)
                self._connected = True
                _dbg(f"Connected to ZooKeeper {hosts}", "info")
            except Exception as e:
                _dbg(f"ZooKeeper connect failed: {e}", "warning")

    def load(self) -> Optional[Dict[str, Any]]:
        if not self._connected or not self.client:
            return None
        try:
            out: Dict[str, Any] = {}

            def walk(path: str, cur: Dict[str, Any]) -> None:
                if not self.client.exists(path):
                    return
                children = self.client.get_children(path)
                for child in children:
                    child_path = f"{path}/{child}"
                    data, _ = self.client.get(child_path)
                    sub_children = self.client.get_children(child_path)
                    if sub_children:
                        cur.setdefault(child, {})
                        if isinstance(cur[child], dict):
                            walk(child_path, cur[child])
                    else:
                        if data:
                            raw = data.decode("utf-8", errors="replace")
                            try:
                                cur[child] = json_loads(raw)
                            except Exception:
                                cur[child] = raw

            walk(self.prefix, out)
            return out or None
        except Exception as e:
            _dbg(f"ZooKeeper load failed: {e}", "warning")
            return None


# =============================================================================
# Main Settings (Dataclass)
# =============================================================================
@dataclass(frozen=True)
class Settings:
    """Main application settings (core)."""

    # Meta
    config_version: str = CONFIG_VERSION
    schema_version: str = CONFIG_SCHEMA_VERSION
    build_timestamp: str = CONFIG_BUILD_TIMESTAMP

    # App
    environment: Environment = Environment.PRODUCTION
    app_version: str = "dev"
    service_name: str = "Tadawul Fast Bridge"
    timezone: str = "Asia/Riyadh"

    # Logging
    log_level: LogLevel = LogLevel.INFO
    log_format: str = "json"

    # Backend URLs
    backend_base_url: str = "http://localhost:8000"
    internal_base_url: Optional[str] = None
    public_base_url: Optional[str] = None

    # Auth
    auth_header_name: str = "X-APP-TOKEN"
    app_token: Optional[str] = None
    open_mode: bool = False  # if True: endpoints may allow anonymous access

    # Feature flags
    ai_analysis_enabled: bool = True
    advisor_enabled: bool = True
    rate_limit_enabled: bool = True
    cache_enabled: bool = True
    circuit_breaker_enabled: bool = True
    metrics_enabled: bool = True
    tracing_enabled: bool = False

    # Providers
    enabled_providers: List[str] = field(default_factory=lambda: ["eodhd", "finnhub"])
    ksa_providers: List[str] = field(default_factory=lambda: ["yahoo_chart", "argaam"])
    primary_provider: str = "eodhd"

    # Secrets (providers)
    eodhd_api_key: Optional[str] = None
    finnhub_api_key: Optional[str] = None
    alphavantage_api_key: Optional[str] = None

    # Performance
    http_timeout_sec: float = 45.0
    cache_ttl_sec: int = 20
    batch_concurrency: int = 5
    ai_batch_size: int = 20
    quote_batch_size: int = 50

    # Google Sheets integration (used by integrations/google_sheets_service.py)
    default_spreadsheet_id: Optional[str] = None
    google_sheets_credentials_json: Optional[str] = None
    google_credentials_dict: Optional[Dict[str, Any]] = None

    @classmethod
    def from_env(cls, env_prefix: str = "TFB_") -> "Settings":
        env_name = strip_value(os.getenv(f"{env_prefix}ENV") or os.getenv("APP_ENV") or "production")

        token = strip_value(os.getenv(f"{env_prefix}APP_TOKEN") or os.getenv("APP_TOKEN") or os.getenv("BACKEND_TOKEN"))
        is_open = coerce_bool(os.getenv("OPEN_MODE"), not bool(token))

        eodhd_key = strip_value(os.getenv("EODHD_API_TOKEN") or os.getenv("EODHD_API_KEY"))
        finnhub_key = strip_value(os.getenv("FINNHUB_API_KEY"))
        alpha_key = strip_value(os.getenv("ALPHAVANTAGE_API_KEY"))

        providers_env = coerce_list(os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS"))
        if not providers_env:
            providers_env = []
            if eodhd_key:
                providers_env.append("eodhd")
            if finnhub_key:
                providers_env.append("finnhub")
            if not providers_env:
                providers_env = ["eodhd", "finnhub"]

        ksa_env = coerce_list(os.getenv("KSA_PROVIDERS") or "yahoo_chart,argaam")
        primary = strip_value(os.getenv("PRIMARY_PROVIDER") or "eodhd")

        log_level_name = strip_value(os.getenv("LOG_LEVEL") or "INFO").upper()
        log_level = LogLevel.INFO
        for ll in LogLevel:
            if ll.value == log_level_name:
                log_level = ll
                break

        backend_base_url = strip_value(os.getenv("BACKEND_BASE_URL") or os.getenv("DEFAULT_BACKEND_URL") or "http://localhost:8000")
        default_spreadsheet_id = strip_value(os.getenv("DEFAULT_SPREADSHEET_ID") or "")

        # Allow storing credentials in settings (used by google_sheets_service CredentialsManager)
        gs_creds_json = strip_value(os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS") or "")
        gs_creds_dict = None
        if gs_creds_json.startswith("{") and gs_creds_json.endswith("}"):
            # keep raw JSON; google_sheets_service will parse as needed
            pass

        # A dict form can be injected via an env var as JSON too
        gs_creds_dict_env = strip_value(os.getenv("GOOGLE_CREDENTIALS_DICT") or "")
        if gs_creds_dict_env.startswith("{") and gs_creds_dict_env.endswith("}"):
            try:
                parsed = json_loads(gs_creds_dict_env)
                if isinstance(parsed, dict):
                    gs_creds_dict = parsed
            except Exception:
                gs_creds_dict = None

        return cls(
            environment=Environment.from_string(env_name),
            app_token=token or None,
            open_mode=is_open,
            backend_base_url=backend_base_url,
            eodhd_api_key=eodhd_key or None,
            finnhub_api_key=finnhub_key or None,
            alphavantage_api_key=alpha_key or None,
            enabled_providers=providers_env,
            ksa_providers=ksa_env,
            primary_provider=primary,
            log_level=log_level,
            http_timeout_sec=coerce_float(os.getenv("HTTP_TIMEOUT_SEC"), 45.0, lo=5.0, hi=300.0),
            cache_ttl_sec=coerce_int(os.getenv("CACHE_TTL_SEC"), 20, lo=1, hi=3600),
            batch_concurrency=coerce_int(os.getenv("BATCH_CONCURRENCY"), 5, lo=1, hi=50),
            ai_batch_size=coerce_int(os.getenv("AI_BATCH_SIZE"), 20, lo=1, hi=500),
            quote_batch_size=coerce_int(os.getenv("QUOTE_BATCH_SIZE"), 50, lo=1, hi=500),
            metrics_enabled=coerce_bool(os.getenv("METRICS_ENABLED"), True),
            tracing_enabled=coerce_bool(os.getenv("TRACING_ENABLED"), False),
            ai_analysis_enabled=coerce_bool(os.getenv("AI_ANALYSIS_ENABLED"), True),
            advisor_enabled=coerce_bool(os.getenv("ADVISOR_ENABLED"), True),
            default_spreadsheet_id=default_spreadsheet_id or None,
            google_sheets_credentials_json=gs_creds_json or None,
            google_credentials_dict=gs_creds_dict,
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def validate(self) -> Tuple[List[str], List[str]]:
        errors: List[str] = []
        warnings_list: List[str] = []

        # Auth sanity
        if not self.open_mode and not self.app_token:
            errors.append("Authentication required but no app_token configured (OPEN_MODE=false).")
        if self.open_mode and self.app_token:
            warnings_list.append("OPEN_MODE enabled while app_token exists. Endpoints may be publicly exposed.")

        # Backend URL sanity
        if self.backend_base_url and not is_valid_url(self.backend_base_url):
            warnings_list.append(f"backend_base_url does not look like a URL: {self.backend_base_url!r}")

        # Providers
        if "eodhd" in [p.lower() for p in self.enabled_providers] and not self.eodhd_api_key:
            errors.append("Provider 'eodhd' is enabled but no API key provided.")
        if "finnhub" in [p.lower() for p in self.enabled_providers] and not self.finnhub_api_key:
            errors.append("Provider 'finnhub' is enabled but no API key provided.")

        return errors, warnings_list

    @classmethod
    def from_file(cls, filepath: str) -> "Settings":
        path = Path(filepath)
        content = load_file_content(path)
        base = cls.from_env()
        merged = deep_merge(asdict(base), content, overwrite=True)
        # Keep only known fields
        allowed = set(cls.__dataclass_fields__.keys())
        cleaned = {k: v for k, v in merged.items() if k in allowed}
        return cls(**cleaned)


# =============================================================================
# Settings Cache + Public API
# =============================================================================
class SettingsCache:
    def __init__(self, ttl_seconds: int = 30):
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._lock = RLock()
        self._ttl = max(1, int(ttl_seconds))
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key in self._cache:
                value, expiry = self._cache[key]
                if time.time() < expiry:
                    self._hits += 1
                    return value
                del self._cache[key]
            self._misses += 1
            return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        with self._lock:
            t = self._ttl if ttl is None else max(1, int(ttl))
            self._cache[key] = (value, time.time() + t)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._hits = 0
            self._misses = 0

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            total = self._hits + self._misses
            return {
                "size": len(self._cache),
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": (self._hits / total) if total > 0 else 0.0,
            }


_SETTINGS_CACHE = SettingsCache(ttl_seconds=30)


def get_settings() -> Settings:
    return Settings.from_env()


def get_settings_cached(force_reload: bool = False) -> Settings:
    with TraceContext("get_settings_cached"):
        if force_reload:
            _SETTINGS_CACHE.clear()
        cached = _SETTINGS_CACHE.get("settings")
        if cached is not None:
            return cached
        s = get_settings()
        _SETTINGS_CACHE.set("settings", s)
        return s


def reload_settings() -> Settings:
    with TraceContext("reload_settings"):
        _SETTINGS_CACHE.clear()
        merged = _CONFIG_SOURCES.load_merged()
        base = asdict(Settings.from_env())
        if merged:
            base = deep_merge(base, merged, overwrite=True)
        allowed = set(Settings.__dataclass_fields__.keys())
        cleaned = {k: v for k, v in base.items() if k in allowed}
        new_settings = Settings(**cleaned)
        _SETTINGS_CACHE.set("settings", new_settings)
        return new_settings


def config_health_check() -> Dict[str, Any]:
    health: Dict[str, Any] = {"status": "healthy", "checks": {}, "warnings": [], "errors": []}
    try:
        settings = get_settings_cached()
        health["checks"]["settings_accessible"] = True
        errors, warnings_list = settings.validate()
        if errors:
            health["checks"]["settings_valid"] = False
            health["errors"].extend(errors)
            health["status"] = "degraded"
        else:
            health["checks"]["settings_valid"] = True
        health["warnings"].extend(warnings_list)
    except Exception as e:
        health["checks"]["settings_accessible"] = False
        health["errors"].append(f"Cannot access settings: {e}")
        health["status"] = "unhealthy"

    cache_stats = _SETTINGS_CACHE.get_stats()
    health["checks"]["cache_size"] = cache_stats["size"]
    health["checks"]["cache_hit_rate"] = cache_stats["hit_rate"]
    return health


# =============================================================================
# Distributed config init (safe + optional)
# =============================================================================
def init_distributed_config() -> None:
    """
    Register distributed sources if corresponding env vars exist.
    Does not force-connect unless enabled by environment.
    """
    with TraceContext("init_distributed_config"):
        if os.getenv("CONSUL_HOST"):
            host = os.getenv("CONSUL_HOST", "localhost")
            port = coerce_int(os.getenv("CONSUL_PORT"), 8500, lo=1, hi=65535)
            token = strip_value(os.getenv("CONSUL_TOKEN"))
            prefix = strip_value(os.getenv("CONSUL_PREFIX") or "config/tadawul")
            consul_src = ConsulConfigSource(host=host, port=port, token=token or None, prefix=prefix)
            _CONFIG_SOURCES.register_source(ConfigSource.CONSUL, consul_src.load)

        if os.getenv("ETCD_HOST"):
            host = os.getenv("ETCD_HOST", "localhost")
            port = coerce_int(os.getenv("ETCD_PORT"), 2379, lo=1, hi=65535)
            prefix = strip_value(os.getenv("ETCD_PREFIX") or "/config/tadawul")
            etcd_src = EtcdConfigSource(host=host, port=port, prefix=prefix)
            _CONFIG_SOURCES.register_source(ConfigSource.ETCD, etcd_src.load)

        if os.getenv("ZOOKEEPER_HOSTS"):
            hosts = os.getenv("ZOOKEEPER_HOSTS", "localhost:2181")
            prefix = strip_value(os.getenv("ZOOKEEPER_PREFIX") or "/config/tadawul")
            zk_src = ZooKeeperConfigSource(hosts=hosts, prefix=prefix)
            _CONFIG_SOURCES.register_source(ConfigSource.ZOOKEEPER, zk_src.load)


# Initialize (safe)
init_distributed_config()


# =============================================================================
# Module Exports
# =============================================================================
__all__ = [
    # meta
    "CONFIG_VERSION",
    "CONFIG_SCHEMA_VERSION",
    "CONFIG_BUILD_TIMESTAMP",
    # enums
    "Environment",
    "AuthType",
    "CacheBackend",
    "CacheStrategy",
    "ProviderType",
    "ProviderStatus",
    "ConfigSource",
    "ConfigStatus",
    "EncryptionMethod",
    "ConfigEncryptionAlgorithm",
    "LogLevel",
    # optional models
    "RateLimitConfig",
    "CircuitBreakerConfig",
    "CacheConfig",
    "ProviderConfig",
    "AuthConfig",
    "MetricsConfig",
    "LoggingConfig",
    # settings + API
    "Settings",
    "get_settings",
    "get_settings_cached",
    "reload_settings",
    "config_health_check",
    # utils
    "mask_secret",
    "mask_secret_dict",
    "deep_merge",
    "load_file_content",
    "coerce_int",
    "coerce_float",
    "coerce_bool",
    "coerce_list",
    "coerce_dict",
    "strip_value",
    "TraceContext",
    # distributed sources
    "ConsulConfigSource",
    "EtcdConfigSource",
    "ZooKeeperConfigSource",
    "init_distributed_config",
]
