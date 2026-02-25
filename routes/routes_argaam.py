#!/usr/bin/env python3
# core/config.py
"""
================================================================================
Core Configuration Module — v6.1.1 (QUANTUM EDITION / PROJECT-ALIGNED)
================================================================================
TADAWUL FAST BRIDGE – Enterprise Configuration Management

PRIMARY GOALS (Project-Aligned)
- ✅ Never breaks startup (all optional deps are safe; no hard failures)
- ✅ Single source of truth for env parsing + defaults
- ✅ Strong alignment with:
    - integrations/google_sheets_service.py (CredentialsManager hooks)
    - integrations/setup_credentials.py (credentials normalization + validation)
- ✅ Deterministic + safe (no hidden network calls unless explicitly enabled)
- ✅ Production-friendly: masked public config, health checks, strict validation

WHAT'S NEW vs v6.1.0 pasted draft
- ✅ Fixed naming conflicts & undefined exports (CacheStrategy/ProviderStatus aliases safe)
- ✅ Safer TraceContext implementation (never throws; decorator supported)
- ✅ Env-file (.env) loader integrated into Settings.from_env without mutating os.environ
- ✅ Unified Google credential resolution returns BOTH:
      - google_credentials_dict (normalized)
      - google_sheets_credentials_json (string JSON) for compatibility
- ✅ Runtime overrides support (thread-safe) + included in reload_settings
- ✅ Distributed config sources are lazy: only connect when reload_settings calls load_merged
- ✅ Added auth helpers (allowed_tokens/is_open_mode/auth_ok) used by backend routes
- ✅ Public config export includes masking for system/info endpoints

NOTE
- This module does NOT force-connect to Consul/etcd/ZK unless you set:
    CONSUL_HOST / ETCD_HOST / ZOOKEEPER_HOSTS
- Even then, connection attempts happen ONLY when load_merged() is called (during reload_settings).
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
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from threading import RLock
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

# =============================================================================
# Version
# =============================================================================
CONFIG_VERSION = "6.1.1"
CONFIG_SCHEMA_VERSION = "2.2"
CONFIG_BUILD_TIMESTAMP = os.getenv("BUILD_TIMESTAMP") or datetime.now(timezone.utc).isoformat()

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# =============================================================================
# High-Performance JSON (orjson) fallback
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
# Optional deps (safe)
# =============================================================================
try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore

    OTEL_AVAILABLE = True
except Exception:
    trace = None  # type: ignore
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    OTEL_AVAILABLE = False

try:
    import yaml  # type: ignore

    YAML_AVAILABLE = True
except Exception:
    yaml = None  # type: ignore
    YAML_AVAILABLE = False

try:
    import toml  # type: ignore

    TOML_AVAILABLE = True
except Exception:
    toml = None  # type: ignore
    TOML_AVAILABLE = False

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
# Tracing Context (sync + decorator)
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
    - Use as context manager: with TraceContext("name"): ...
    - Use as decorator: @TraceContext("name")
    Never throws; always safe.
    """

    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._span_cm = None
        self._span = None

    def __enter__(self):
        if OTEL_AVAILABLE and _TRACING_ENABLED and trace is not None:
            try:
                tracer = trace.get_tracer(__name__)
                self._span_cm = tracer.start_as_current_span(self.name)
                self._span = self._span_cm.__enter__()
                try:
                    for k, v in (self.attributes or {}).items():
                        self._span.set_attribute(k, v)
                except Exception:
                    pass
            except Exception:
                self._span_cm = None
                self._span = None
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self._span is not None and exc is not None and Status is not None and StatusCode is not None:
                try:
                    self._span.record_exception(exc)
                except Exception:
                    pass
                try:
                    self._span.set_status(Status(StatusCode.ERROR, str(exc)))
                except Exception:
                    pass
        finally:
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
# Utilities
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "active"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "inactive"}

_URL_RE = re.compile(r"^https?://", re.IGNORECASE)
_HOSTPORT_RE = re.compile(r"^[a-zA-Z0-9.\-]+(:\d+)?$", re.IGNORECASE)


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


def normalize_base_url(url: str, *, default_scheme: str = "https") -> str:
    """
    Normalize URLs safely:
    - "example.com:8000" -> "https://example.com:8000"
    - "http://x" -> "http://x"
    - "" -> ""
    """
    u = strip_value(url)
    if not u:
        return ""
    if _URL_RE.match(u):
        return u.rstrip("/")
    if _HOSTPORT_RE.match(u):
        return f"{default_scheme}://{u}".rstrip("/")
    return u.rstrip("/")


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
    """Internal logger. Never throws."""
    try:
        payload = {"message": message, "ts": datetime.now(timezone.utc).isoformat(), **kwargs}
        msg = json_dumps(payload)
        lvl = (level or "info").lower()
        if lvl == "error":
            logger.error(msg)
        elif lvl in ("warn", "warning"):
            logger.warning(msg)
        elif lvl == "debug":
            logger.debug(msg)
        else:
            logger.info(msg)
    except Exception:
        pass


def deep_merge(base: Dict[str, Any], override: Dict[str, Any], *, overwrite: bool = True) -> Dict[str, Any]:
    """
    Deep merge dicts.
    - overwrite=True: override wins.
    - lists are replaced (deterministic behavior).
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
    """Load JSON/YAML/TOML safely. Returns {} on failure."""
    try:
        if not path.exists() or not path.is_file():
            return {}
        raw = path.read_text(encoding="utf-8", errors="replace").strip()
        if not raw:
            return {}
        suffix = path.suffix.lower()

        if suffix == ".json":
            obj = json_loads(raw)
            return obj if isinstance(obj, dict) else {}

        if suffix in (".yml", ".yaml") and YAML_AVAILABLE and yaml is not None:
            obj = yaml.safe_load(raw)
            return obj if isinstance(obj, dict) else {}

        if suffix == ".toml" and TOML_AVAILABLE and toml is not None:
            obj = toml.loads(raw)
            return obj if isinstance(obj, dict) else {}

        # fallback: try JSON anyway
        obj = json_loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception as e:
        _dbg("load_file_content failed", "warning", path=str(path), error=str(e))
        return {}


def load_dotenv_simple(path: Path) -> Dict[str, str]:
    """
    Minimal .env parser (no dependency).
    Supports KEY=VALUE, quotes, comments.
    Does not mutate os.environ; returns dict.
    """
    out: Dict[str, str] = {}
    try:
        if not path.exists() or not path.is_file():
            return out
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if "=" not in s:
                continue
            k, v = s.split("=", 1)
            k = k.strip()
            v = v.strip()
            if v.startswith(("'", '"')) and v.endswith(("'", '"')) and len(v) >= 2:
                v = v[1:-1]
            if k:
                out[k] = v
    except Exception as e:
        _dbg("load_dotenv_simple failed", "warning", path=str(path), error=str(e))
    return out


# =============================================================================
# Enums
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


# Compatibility aliases (avoid breaking old imports)
CacheStrategy = CacheBackend
ProviderStatus = Enum("ProviderStatus", {"ENABLED": "enabled", "DISABLED": "disabled"})


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


class ConfigStatus(str, Enum):
    ACTIVE = "active"
    PENDING = "pending"
    ROLLED_BACK = "rolled_back"
    ARCHIVED = "archived"
    CORRUPT = "corrupt"


class EncryptionMethod(str, Enum):
    NONE = "none"
    BASE64 = "base64"


class AuthType(str, Enum):
    NONE = "none"
    TOKEN = "token"
    BEARER = "bearer"
    API_KEY = "api_key"
    JWT = "jwt"


# =============================================================================
# Backoff
# =============================================================================
class FullJitterBackoff:
    """AWS Full-Jitter backoff (bounded)."""

    def __init__(self, max_retries: int = 3, base_delay: float = 0.5, max_delay: float = 5.0):
        self.max_retries = max(1, int(max_retries))
        self.base_delay = max(0.01, float(base_delay))
        self.max_delay = max(self.base_delay, float(max_delay))

    def execute_sync(self, fn: Callable, *args, **kwargs) -> Any:
        last: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                last = e
                if attempt >= self.max_retries - 1:
                    raise
                cap = min(self.max_delay, self.base_delay * (2**attempt))
                time.sleep(random.uniform(0, cap))
        if last:
            raise last
        raise RuntimeError("Backoff failed unexpectedly")


# =============================================================================
# Runtime Overrides (optional)
# =============================================================================
class RuntimeOverrides:
    """Thread-safe runtime overrides for config values."""

    def __init__(self):
        self._lock = RLock()
        self._data: Dict[str, Any] = {}

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._data[str(key)] = value

    def get_all(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._data)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


_RUNTIME = RuntimeOverrides()

# =============================================================================
# Google Credentials Helpers (Aligned with google_sheets_service + setup_credentials)
# =============================================================================
def _parse_json_maybe(raw: str) -> Optional[Dict[str, Any]]:
    raw = strip_value(raw)
    if not raw:
        return None
    if not raw.startswith("{"):
        return None
    try:
        obj = json_loads(raw)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _parse_b64_json(raw: str) -> Optional[Dict[str, Any]]:
    raw = strip_value(raw)
    if not raw:
        return None
    try:
        decoded = base64.b64decode(raw).decode("utf-8", errors="replace")
        obj = json_loads(decoded)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _normalize_google_creds_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize private_key newlines (\\n -> \n) and add PEM wrapper if missing.
    """
    out = dict(d)
    pk = out.get("private_key")
    if isinstance(pk, str) and pk:
        if "\\n" in pk:
            pk = pk.replace("\\n", "\n")
        if "-----BEGIN PRIVATE KEY-----" not in pk and "-----END PRIVATE KEY-----" not in pk:
            pk = "-----BEGIN PRIVATE KEY-----\n" + pk.strip() + "\n-----END PRIVATE KEY-----\n"
        out["private_key"] = pk
    return out


def get_google_credentials_dict(env: Optional[Dict[str, str]] = None) -> Optional[Dict[str, Any]]:
    """
    Unified Google service account credential loader.
    Supported sources:
      - GOOGLE_CREDENTIALS_DICT (JSON dict)
      - GOOGLE_SHEETS_CREDENTIALS / GOOGLE_CREDENTIALS (JSON)
      - GOOGLE_*_B64 (base64 JSON)
      - GOOGLE_APPLICATION_CREDENTIALS / GOOGLE_SHEETS_CREDENTIALS_FILE (file path)
    Returns dict or None. Never throws.
    """
    try:
        e = env if env is not None else os.environ

        # 1) explicit dict env (JSON)
        d = _parse_json_maybe(e.get("GOOGLE_CREDENTIALS_DICT", ""))
        if d:
            return _normalize_google_creds_dict(d)

        # 2) JSON env
        d = _parse_json_maybe(e.get("GOOGLE_SHEETS_CREDENTIALS", "") or e.get("GOOGLE_CREDENTIALS", ""))
        if d:
            return _normalize_google_creds_dict(d)

        # 3) base64 JSON env
        d = _parse_b64_json(e.get("GOOGLE_SHEETS_CREDENTIALS_B64", "") or e.get("GOOGLE_CREDENTIALS_B64", ""))
        if d:
            return _normalize_google_creds_dict(d)

        # 4) file path
        p = strip_value(e.get("GOOGLE_APPLICATION_CREDENTIALS", "") or e.get("GOOGLE_SHEETS_CREDENTIALS_FILE", ""))
        if p:
            path = Path(p)
            if path.exists() and path.is_file():
                raw = path.read_text(encoding="utf-8", errors="replace")
                d2 = _parse_json_maybe(raw)
                if d2:
                    return _normalize_google_creds_dict(d2)

        return None
    except Exception:
        return None


# =============================================================================
# Distributed config sources (lazy connect; only used during load_merged)
# =============================================================================
class ConfigSourceManager:
    def __init__(self):
        self._lock = RLock()
        self._sources: List[Tuple[ConfigSource, Callable[[], Optional[Dict[str, Any]]]]] = []

    def register_source(self, source: ConfigSource, loader: Callable[[], Optional[Dict[str, Any]]]) -> None:
        with self._lock:
            self._sources.append((source, loader))

    def load_all(self) -> Dict[ConfigSource, Dict[str, Any]]:
        out: Dict[ConfigSource, Dict[str, Any]] = {}
        with self._lock:
            for src, loader in self._sources:
                try:
                    data = loader()
                    if isinstance(data, dict) and data:
                        out[src] = data
                except Exception as e:
                    _dbg("Config source load failed", "warning", source=src.value, error=str(e))
        return out

    def load_merged(self, priority: Optional[List[ConfigSource]] = None) -> Dict[str, Any]:
        loaded = self.load_all()
        if not loaded:
            return {}
        if priority is None:
            priority = [
                ConfigSource.ENV_VAR,
                ConfigSource.ENV_FILE,
                ConfigSource.CONSUL,
                ConfigSource.ETCD,
                ConfigSource.ZOOKEEPER,
                ConfigSource.RUNTIME,
            ]
        merged: Dict[str, Any] = {}
        for src in priority:
            if src in loaded:
                merged = deep_merge(merged, loaded[src], overwrite=True)
        return merged

    @property
    def sources_count(self) -> int:
        with self._lock:
            return len(self._sources)


_CONFIG_SOURCES = ConfigSourceManager()


class ConsulConfigSource:
    def __init__(self, host: str, port: int, token: Optional[str], prefix: str):
        self.host = host
        self.port = port
        self.token = token
        self.prefix = prefix
        self._lock = RLock()
        self._client = None
        self._connected = False

    def _connect(self):
        if not CONSUL_AVAILABLE or consul is None:
            return None
        c = consul.Consul(host=self.host, port=self.port, token=self.token)
        c.status.leader()
        return c

    def load(self) -> Optional[Dict[str, Any]]:
        if not CONSUL_AVAILABLE:
            return None
        with self._lock:
            if not self._connected:
                backoff = FullJitterBackoff(max_retries=2, base_delay=0.5, max_delay=2.0)
                try:
                    self._client = backoff.execute_sync(self._connect)
                    self._connected = bool(self._client)
                except Exception as e:
                    _dbg("Consul connect failed", "warning", error=str(e))
                    self._connected = False
                    return None

            if not self._client:
                return None

            try:
                _, data = self._client.kv.get(self.prefix, recurse=True)
                if not data:
                    return None
                out: Dict[str, Any] = {}
                for item in data:
                    k = item.get("Key") or ""
                    raw = item.get("Value")
                    if not k or raw is None:
                        continue
                    rel = k[len(self.prefix) + 1 :] if k.startswith(self.prefix) else k
                    try:
                        val = json_loads(raw.decode("utf-8", errors="replace"))
                    except Exception:
                        val = raw.decode("utf-8", errors="replace")

                    parts = [p for p in rel.split("/") if p]
                    cur = out
                    for p in parts[:-1]:
                        cur = cur.setdefault(p, {})
                    cur[parts[-1]] = val
                return out or None
            except Exception as e:
                _dbg("Consul load failed", "warning", error=str(e))
                return None


class EtcdConfigSource:
    def __init__(self, host: str, port: int, prefix: str):
        self.host = host
        self.port = port
        self.prefix = prefix
        self._lock = RLock()
        self._client = None
        self._connected = False

    def _connect(self):
        if not ETCD_AVAILABLE or etcd3 is None:
            return None
        c = etcd3.client(host=self.host, port=self.port)
        try:
            c.status()
        except Exception:
            pass
        return c

    def load(self) -> Optional[Dict[str, Any]]:
        if not ETCD_AVAILABLE:
            return None
        with self._lock:
            if not self._connected:
                backoff = FullJitterBackoff(max_retries=2, base_delay=0.5, max_delay=2.0)
                try:
                    self._client = backoff.execute_sync(self._connect)
                    self._connected = bool(self._client)
                except Exception as e:
                    _dbg("etcd connect failed", "warning", error=str(e))
                    self._connected = False
                    return None

            if not self._client:
                return None

            try:
                out: Dict[str, Any] = {}
                for value, meta in self._client.get_prefix(self.prefix):
                    key = (meta.key or b"").decode("utf-8", errors="replace")
                    rel = key[len(self.prefix) :].lstrip("/")
                    try:
                        val = json_loads(value.decode("utf-8", errors="replace"))
                    except Exception:
                        val = value.decode("utf-8", errors="replace")

                    parts = [p for p in rel.split("/") if p]
                    cur = out
                    for p in parts[:-1]:
                        cur = cur.setdefault(p, {})
                    cur[parts[-1]] = val
                return out or None
            except Exception as e:
                _dbg("etcd load failed", "warning", error=str(e))
                return None


class ZooKeeperConfigSource:
    def __init__(self, hosts: str, prefix: str):
        self.hosts = hosts
        self.prefix = prefix
        self._lock = RLock()
        self._client = None
        self._connected = False

    def _connect(self):
        if not ZOOKEEPER_AVAILABLE or KazooClient is None:
            return None
        c = KazooClient(hosts=self.hosts)
        c.start(timeout=5)
        return c

    def load(self) -> Optional[Dict[str, Any]]:
        if not ZOOKEEPER_AVAILABLE:
            return None
        with self._lock:
            if not self._connected:
                backoff = FullJitterBackoff(max_retries=2, base_delay=0.5, max_delay=2.0)
                try:
                    self._client = backoff.execute_sync(self._connect)
                    self._connected = bool(self._client)
                except Exception as e:
                    _dbg("ZooKeeper connect failed", "warning", error=str(e))
                    self._connected = False
                    return None

            if not self._client:
                return None

            try:
                out: Dict[str, Any] = {}

                def walk(path: str, cur: Dict[str, Any]) -> None:
                    if not self._client.exists(path):
                        return
                    children = self._client.get_children(path)
                    for child in children:
                        child_path = f"{path}/{child}"
                        data, _ = self._client.get(child_path)
                        sub_children = self._client.get_children(child_path)
                        if sub_children:
                            nxt = cur.setdefault(child, {})
                            if isinstance(nxt, dict):
                                walk(child_path, nxt)
                        else:
                            raw = data.decode("utf-8", errors="replace") if data else ""
                            if raw:
                                try:
                                    cur[child] = json_loads(raw)
                                except Exception:
                                    cur[child] = raw

                walk(self.prefix, out)
                return out or None
            except Exception as e:
                _dbg("ZooKeeper load failed", "warning", error=str(e))
                return None


# =============================================================================
# Settings (aligned with backend + google sheets)
# =============================================================================
@dataclass(frozen=True, slots=True)
class Settings:
    # meta
    config_version: str = CONFIG_VERSION
    schema_version: str = CONFIG_SCHEMA_VERSION
    build_timestamp: str = CONFIG_BUILD_TIMESTAMP

    # app
    environment: Environment = Environment.PRODUCTION
    app_version: str = "dev"
    service_name: str = "Tadawul Fast Bridge"
    timezone: str = "Asia/Riyadh"

    # logging
    log_level: LogLevel = LogLevel.INFO
    log_format: str = "json"

    # urls
    backend_base_url: str = "http://localhost:8000"
    internal_base_url: Optional[str] = None
    public_base_url: Optional[str] = None

    # auth
    auth_header_name: str = "X-APP-TOKEN"
    app_token: Optional[str] = None
    open_mode: bool = False

    # feature flags
    ai_analysis_enabled: bool = True
    advisor_enabled: bool = True
    rate_limit_enabled: bool = True
    cache_enabled: bool = True
    circuit_breaker_enabled: bool = True
    metrics_enabled: bool = True
    tracing_enabled: bool = False

    # providers
    enabled_providers: List[str] = field(default_factory=lambda: ["eodhd", "finnhub"])
    ksa_providers: List[str] = field(default_factory=lambda: ["yahoo_chart", "argaam"])
    primary_provider: str = "eodhd"

    # provider secrets
    eodhd_api_key: Optional[str] = None
    finnhub_api_key: Optional[str] = None
    alphavantage_api_key: Optional[str] = None

    # performance
    http_timeout_sec: float = 45.0
    cache_ttl_sec: int = 20
    batch_concurrency: int = 5
    ai_batch_size: int = 20
    quote_batch_size: int = 50

    # google sheets integration
    default_spreadsheet_id: Optional[str] = None
    google_sheets_credentials_json: Optional[str] = None
    google_credentials_dict: Optional[Dict[str, Any]] = None

    @classmethod
    def from_env(cls, env_prefix: str = "TFB_") -> "Settings":
        # Optional .env read (does NOT mutate os.environ)
        env_file = strip_value(os.getenv("ENV_FILE") or os.getenv("DOTENV_PATH") or "")
        dotenv_map: Dict[str, str] = load_dotenv_simple(Path(env_file)) if env_file else {}

        def env_get(key: str, fallback: str = "") -> str:
            return strip_value(os.getenv(key) or dotenv_map.get(key) or fallback)

        env_name = env_get(f"{env_prefix}ENV", env_get("APP_ENV", "production"))

        token = env_get(f"{env_prefix}APP_TOKEN", env_get("APP_TOKEN", env_get("BACKEND_TOKEN", ""))) or ""
        open_mode = coerce_bool(env_get("OPEN_MODE", ""), not bool(token))

        backend_url = env_get("BACKEND_BASE_URL", env_get("DEFAULT_BACKEND_URL", "http://localhost:8000"))
        backend_url = normalize_base_url(backend_url, default_scheme=env_get("DEFAULT_SCHEME", "https") or "https")

        # Provider keys
        eodhd_key = env_get("EODHD_API_TOKEN", env_get("EODHD_API_KEY", ""))
        finnhub_key = env_get("FINNHUB_API_KEY", "")
        alpha_key = env_get("ALPHAVANTAGE_API_KEY", "")

        # providers lists
        providers_env = coerce_list(env_get("ENABLED_PROVIDERS", env_get("PROVIDERS", "")))
        if not providers_env:
            providers_env = []
            if eodhd_key:
                providers_env.append("eodhd")
            if finnhub_key:
                providers_env.append("finnhub")
            if not providers_env:
                providers_env = ["eodhd", "finnhub"]

        ksa_env = coerce_list(env_get("KSA_PROVIDERS", "yahoo_chart,argaam"))
        primary = env_get("PRIMARY_PROVIDER", "eodhd")

        # log level
        log_level_name = env_get("LOG_LEVEL", "INFO").upper()
        log_level = LogLevel.INFO
        for ll in LogLevel:
            if ll.value == log_level_name:
                log_level = ll
                break

        # google sheets
        default_spreadsheet_id = env_get("DEFAULT_SPREADSHEET_ID", "") or None

        # Gather creds from combined env+dotenv for stability
        combined_env = dict(os.environ)
        combined_env.update(dotenv_map)

        gs_dict = get_google_credentials_dict(combined_env)
        gs_creds_json = env_get("GOOGLE_SHEETS_CREDENTIALS", env_get("GOOGLE_CREDENTIALS", "")) or None
        if gs_dict and not gs_creds_json:
            try:
                gs_creds_json = json_dumps(gs_dict)
            except Exception:
                gs_creds_json = None

        return cls(
            environment=Environment.from_string(env_name),
            app_version=env_get("APP_VERSION", "dev"),
            service_name=env_get("SERVICE_NAME", "Tadawul Fast Bridge"),
            timezone=env_get("TIMEZONE", env_get("TZ", "Asia/Riyadh")),
            log_level=log_level,
            log_format=env_get("LOG_FORMAT", "json"),
            backend_base_url=backend_url or "http://localhost:8000",
            internal_base_url=env_get("INTERNAL_BASE_URL", "") or None,
            public_base_url=env_get("PUBLIC_BASE_URL", "") or None,
            auth_header_name=env_get("AUTH_HEADER_NAME", "X-APP-TOKEN"),
            app_token=token or None,
            open_mode=open_mode,
            eodhd_api_key=eodhd_key or None,
            finnhub_api_key=finnhub_key or None,
            alphavantage_api_key=alpha_key or None,
            enabled_providers=providers_env,
            ksa_providers=ksa_env,
            primary_provider=primary,
            http_timeout_sec=coerce_float(env_get("HTTP_TIMEOUT_SEC", ""), 45.0, lo=5.0, hi=300.0),
            cache_ttl_sec=coerce_int(env_get("CACHE_TTL_SEC", ""), 20, lo=1, hi=3600),
            batch_concurrency=coerce_int(env_get("BATCH_CONCURRENCY", ""), 5, lo=1, hi=50),
            ai_batch_size=coerce_int(env_get("AI_BATCH_SIZE", ""), 20, lo=1, hi=500),
            quote_batch_size=coerce_int(env_get("QUOTE_BATCH_SIZE", ""), 50, lo=1, hi=500),
            metrics_enabled=coerce_bool(env_get("METRICS_ENABLED", ""), True),
            tracing_enabled=coerce_bool(env_get("TRACING_ENABLED", env_get("CORE_TRACING_ENABLED", "")), False),
            ai_analysis_enabled=coerce_bool(env_get("AI_ANALYSIS_ENABLED", ""), True),
            advisor_enabled=coerce_bool(env_get("ADVISOR_ENABLED", ""), True),
            rate_limit_enabled=coerce_bool(env_get("RATE_LIMIT_ENABLED", ""), True),
            cache_enabled=coerce_bool(env_get("CACHE_ENABLED", ""), True),
            circuit_breaker_enabled=coerce_bool(env_get("CIRCUIT_BREAKER_ENABLED", ""), True),
            default_spreadsheet_id=default_spreadsheet_id,
            google_sheets_credentials_json=gs_creds_json,
            google_credentials_dict=gs_dict,
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def public_dict(self) -> Dict[str, Any]:
        """Safe-to-expose config snapshot (masked)."""
        return mask_secret_dict(self.to_dict())

    def validate(self) -> Tuple[List[str], List[str]]:
        errors: List[str] = []
        warnings: List[str] = []

        # backend url
        if not self.backend_base_url:
            errors.append("backend_base_url is empty.")
        elif not is_valid_url(self.backend_base_url):
            warnings.append(f"backend_base_url does not look like a URL: {self.backend_base_url!r}")

        # auth
        if not self.open_mode and not self.app_token:
            errors.append("OPEN_MODE=false but app_token is missing.")
        if self.open_mode and self.app_token:
            warnings.append("OPEN_MODE=true while app_token exists (public exposure risk).")

        # providers
        enabled = [p.lower() for p in (self.enabled_providers or [])]
        if "eodhd" in enabled and not self.eodhd_api_key:
            errors.append("Provider 'eodhd' enabled but EODHD API key missing.")
        if "finnhub" in enabled and not self.finnhub_api_key:
            errors.append("Provider 'finnhub' enabled but FINNHUB API key missing.")

        # google sheets (warnings only)
        if self.default_spreadsheet_id and not self.google_credentials_dict:
            warnings.append("DEFAULT_SPREADSHEET_ID is set but Google credentials are missing/invalid.")
        if self.google_credentials_dict and not self.default_spreadsheet_id:
            warnings.append("Google credentials are set but DEFAULT_SPREADSHEET_ID is missing.")

        # bounds
        if self.batch_concurrency < 1:
            errors.append("batch_concurrency must be >= 1")
        if self.http_timeout_sec < 5:
            warnings.append("http_timeout_sec < 5 may cause timeouts under load.")

        return errors, warnings

    @classmethod
    def from_file(cls, filepath: str) -> "Settings":
        content = load_file_content(Path(filepath))
        base = asdict(cls.from_env())
        merged = deep_merge(base, content, overwrite=True)
        allowed = set(cls.__dataclass_fields__.keys())
        cleaned = {k: v for k, v in merged.items() if k in allowed}
        if "backend_base_url" in cleaned:
            cleaned["backend_base_url"] = normalize_base_url(str(cleaned["backend_base_url"]), default_scheme="https")
        return cls(**cleaned)


# =============================================================================
# Cache + public API
# =============================================================================
class SettingsCache:
    def __init__(self, ttl_seconds: int = 30):
        self._lock = RLock()
        self._ttl = max(1, int(ttl_seconds))
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            item = self._cache.get(key)
            if not item:
                self._misses += 1
                return None
            value, exp = item
            if time.time() < exp:
                self._hits += 1
                return value
            self._cache.pop(key, None)
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

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            total = self._hits + self._misses
            return {
                "size": len(self._cache),
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": (self._hits / total) if total else 0.0,
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
    """
    Reload order:
    - base env settings
    - distributed sources merge (if registered)
    - runtime overrides merge
    """
    with TraceContext("reload_settings"):
        _SETTINGS_CACHE.clear()

        base = asdict(Settings.from_env())

        merged = _CONFIG_SOURCES.load_merged()
        if merged:
            base = deep_merge(base, merged, overwrite=True)

        runtime = _RUNTIME.get_all()
        if runtime:
            base = deep_merge(base, runtime, overwrite=True)

        allowed = set(Settings.__dataclass_fields__.keys())
        cleaned = {k: v for k, v in base.items() if k in allowed}

        if "backend_base_url" in cleaned:
            cleaned["backend_base_url"] = normalize_base_url(str(cleaned["backend_base_url"]), default_scheme="https")

        s = Settings(**cleaned)
        _SETTINGS_CACHE.set("settings", s)
        return s


def config_health_check() -> Dict[str, Any]:
    health: Dict[str, Any] = {"status": "healthy", "checks": {}, "warnings": [], "errors": []}
    try:
        s = get_settings_cached()
        errors, warnings = s.validate()
        health["checks"]["settings_accessible"] = True
        health["checks"]["settings_valid"] = not bool(errors)
        health["warnings"].extend(warnings)
        health["errors"].extend(errors)
        if errors:
            health["status"] = "degraded"
    except Exception as e:
        health["checks"]["settings_accessible"] = False
        health["errors"].append(f"Cannot access settings: {e}")
        health["status"] = "unhealthy"

    health["checks"]["cache"] = _SETTINGS_CACHE.stats()
    health["checks"]["distributed_sources_registered"] = _CONFIG_SOURCES.sources_count
    health["checks"]["has_orjson"] = bool(_HAS_ORJSON)
    health["checks"]["tracing_enabled"] = bool(_TRACING_ENABLED and OTEL_AVAILABLE)
    return health


# =============================================================================
# Distributed sources registration (lazy)
# =============================================================================
def init_distributed_config() -> None:
    """
    Registers sources if env vars exist.
    No network work unless reload_settings calls load_merged().
    """
    with TraceContext("init_distributed_config"):
        if os.getenv("CONSUL_HOST"):
            host = strip_value(os.getenv("CONSUL_HOST", "localhost"))
            port = coerce_int(os.getenv("CONSUL_PORT"), 8500, lo=1, hi=65535)
            token = strip_value(os.getenv("CONSUL_TOKEN")) or None
            prefix = strip_value(os.getenv("CONSUL_PREFIX") or "config/tadawul")
            src = ConsulConfigSource(host=host, port=port, token=token, prefix=prefix)
            _CONFIG_SOURCES.register_source(ConfigSource.CONSUL, src.load)

        if os.getenv("ETCD_HOST"):
            host = strip_value(os.getenv("ETCD_HOST", "localhost"))
            port = coerce_int(os.getenv("ETCD_PORT"), 2379, lo=1, hi=65535)
            prefix = strip_value(os.getenv("ETCD_PREFIX") or "/config/tadawul")
            src = EtcdConfigSource(host=host, port=port, prefix=prefix)
            _CONFIG_SOURCES.register_source(ConfigSource.ETCD, src.load)

        if os.getenv("ZOOKEEPER_HOSTS"):
            hosts = strip_value(os.getenv("ZOOKEEPER_HOSTS", "localhost:2181"))
            prefix = strip_value(os.getenv("ZOOKEEPER_PREFIX") or "/config/tadawul")
            src = ZooKeeperConfigSource(hosts=hosts, prefix=prefix)
            _CONFIG_SOURCES.register_source(ConfigSource.ZOOKEEPER, src.load)

        # ENV_FILE also registered as dict override (only selected keys)
        env_file = strip_value(os.getenv("ENV_FILE") or os.getenv("DOTENV_PATH") or "")
        if env_file:
            p = Path(env_file)

            def _load_env_file() -> Optional[Dict[str, Any]]:
                m = load_dotenv_simple(p)
                if not m:
                    return None
                return {
                    "backend_base_url": normalize_base_url(m.get("BACKEND_BASE_URL") or m.get("DEFAULT_BACKEND_URL") or "", default_scheme="https"),
                    "app_token": strip_value(m.get("APP_TOKEN") or m.get("BACKEND_TOKEN") or ""),
                    "default_spreadsheet_id": strip_value(m.get("DEFAULT_SPREADSHEET_ID") or "") or None,
                }

            _CONFIG_SOURCES.register_source(ConfigSource.ENV_FILE, _load_env_file)


# Run registration
init_distributed_config()

# =============================================================================
# Auth helpers (used by routes)
# =============================================================================
def allowed_tokens() -> List[str]:
    """
    Allowed tokens list.
    Priority:
      - ALLOWED_TOKENS (comma or JSON list)
      - APP_TOKEN (single)
    """
    toks = coerce_list(os.getenv("ALLOWED_TOKENS"))
    if toks:
        return toks
    single = strip_value(os.getenv("APP_TOKEN") or os.getenv("BACKEND_TOKEN") or "")
    return [single] if single else []


def is_open_mode() -> bool:
    """
    Open mode if OPEN_MODE=true,
    else open only when no tokens exist.
    """
    if os.getenv("OPEN_MODE") is not None:
        return coerce_bool(os.getenv("OPEN_MODE"), False)
    return len(allowed_tokens()) == 0


def _norm_headers(headers: Any) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        if headers is None:
            return out
        if isinstance(headers, dict):
            for k, v in headers.items():
                out[str(k).lower()] = strip_value(v)
            return out
        if hasattr(headers, "items"):
            for k, v in headers.items():
                out[str(k).lower()] = strip_value(v)
    except Exception:
        pass
    return out


def _extract_bearer(authorization: Optional[str]) -> Optional[str]:
    a = strip_value(authorization)
    if not a:
        return None
    if a.lower().startswith("bearer "):
        return strip_value(a.split(" ", 1)[1])
    return a


def auth_ok(
    token: Optional[str] = None,
    authorization: Optional[str] = None,
    headers: Optional[Any] = None,
    api_key: Optional[str] = None,
    **_: Any,
) -> bool:
    """
    Standard auth check:
    - token param
    - Authorization: Bearer <token>
    - X-APP-TOKEN header
    - X-API-KEY header
    """
    if is_open_mode():
        return True

    allowed = set(allowed_tokens())
    if not allowed:
        return False

    if token and token in allowed:
        return True

    h = _norm_headers(headers)
    bearer = _extract_bearer(authorization or h.get("authorization"))
    if bearer and bearer in allowed:
        return True

    app_token = h.get("x-app-token") or h.get("x_app_token")
    if app_token and app_token in allowed:
        return True

    api_hdr = h.get("x-api-key") or h.get("x_api_key") or h.get("apikey")
    if api_hdr and api_hdr in allowed:
        return True

    if api_key and api_key in allowed:
        return True

    return False


# =============================================================================
# Module exports
# =============================================================================
__all__ = [
    # meta
    "CONFIG_VERSION",
    "CONFIG_SCHEMA_VERSION",
    "CONFIG_BUILD_TIMESTAMP",
    # enums
    "Environment",
    "LogLevel",
    "ProviderType",
    "CacheBackend",
    "CacheStrategy",
    "ProviderStatus",
    "ConfigSource",
    "ConfigStatus",
    "EncryptionMethod",
    "AuthType",
    # core
    "Settings",
    "get_settings",
    "get_settings_cached",
    "reload_settings",
    "config_health_check",
    # tracing + utils
    "TraceContext",
    "strip_value",
    "coerce_bool",
    "coerce_int",
    "coerce_float",
    "coerce_list",
    "coerce_dict",
    "deep_merge",
    "load_file_content",
    "load_dotenv_simple",
    "normalize_base_url",
    "mask_secret",
    "mask_secret_dict",
    # google creds helpers
    "get_google_credentials_dict",
    # distributed sources
    "ConsulConfigSource",
    "EtcdConfigSource",
    "ZooKeeperConfigSource",
    "init_distributed_config",
    # runtime overrides
    "_RUNTIME",
    # auth helpers
    "allowed_tokens",
    "is_open_mode",
    "auth_ok",
]
