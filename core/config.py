#!/usr/bin/env python3
# core/config.py
"""
================================================================================
Core Configuration Module — v5.4.0 (RENDER-SAFE / STARTUP-SAFE / GLOBAL-FIRST)
================================================================================
TADAWUL FAST BRIDGE – Enterprise Configuration Management

Primary goal of this revision:
- ✅ Avoid deploy/startup failures (NO network calls, NO heavy init at import-time)
- ✅ Keep configuration surface complete and explicit (GLOBAL-first, EODHD primary)
- ✅ Defensive env parsing + safe optional dependencies
- ✅ Google credentials normalization (JSON / base64 / file path)
- ✅ Distributed config sources are LAZY and opt-in (won’t block Render port binding)

What changed vs v5.3.0 you pasted:
- ✅ IMPORTANT: Distributed config sources no longer connect at import-time
  (Consul/ETCD/ZooKeeper clients are created only when `reload_settings()` is called)
- ✅ Added explicit DISTRIBUTED_CONFIG_ENABLED flag (default: off unless host vars present)
- ✅ Settings cache TTL configurable (CORE_SETTINGS_CACHE_TTL)
- ✅ More defensive auth defaults (never throws, validation is non-fatal unless you call it)
- ✅ Safer URL normalization + masking utilities kept

Safe-by-default philosophy:
- Importing this module must NEVER crash, NEVER hang, NEVER do network I/O.
"""

from __future__ import annotations

import base64
import copy
import json
import logging
import os
import re
import time
import uuid
import zlib
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from threading import RLock
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

# =============================================================================
# Version
# =============================================================================
__version__ = "5.4.0"
CONFIG_VERSION = __version__
CONFIG_BUILD_TIMESTAMP = datetime.now(timezone.utc).isoformat()

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# =============================================================================
# Fast JSON (orjson optional)
# =============================================================================
try:
    import orjson  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

    def json_dumps(obj: Any, *, default: Callable = str) -> str:
        return orjson.dumps(obj, default=default).decode("utf-8")

    _HAS_ORJSON = True
except Exception:

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    def json_dumps(obj: Any, *, default: Callable = str) -> str:
        return json.dumps(obj, default=default, ensure_ascii=False)

    _HAS_ORJSON = False

# =============================================================================
# Optional dependencies (safe)
# =============================================================================
try:
    import yaml  # type: ignore

    _HAS_YAML = True
except Exception:
    yaml = None  # type: ignore
    _HAS_YAML = False

try:
    import toml  # type: ignore

    _HAS_TOML = True
except Exception:
    toml = None  # type: ignore
    _HAS_TOML = False

# Encryption (optional)
try:
    from cryptography.fernet import Fernet, InvalidToken  # type: ignore

    _HAS_CRYPTO = True
except Exception:
    Fernet = None  # type: ignore
    InvalidToken = Exception  # type: ignore
    _HAS_CRYPTO = False

# OpenTelemetry (optional)
try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore

    _OTEL_AVAILABLE = True
except Exception:
    trace = None  # type: ignore
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    _OTEL_AVAILABLE = False

# Distributed config libs (optional) — IMPORT ONLY, do not connect at import time
try:
    import consul  # type: ignore

    _CONSUL_AVAILABLE = True
except Exception:
    consul = None  # type: ignore
    _CONSUL_AVAILABLE = False

try:
    import etcd3  # type: ignore

    _ETCD_AVAILABLE = True
except Exception:
    etcd3 = None  # type: ignore
    _ETCD_AVAILABLE = False

try:
    from kazoo.client import KazooClient  # type: ignore

    _ZOOKEEPER_AVAILABLE = True
except Exception:
    KazooClient = None  # type: ignore
    _ZOOKEEPER_AVAILABLE = False

# =============================================================================
# Debug logging helper (never throws)
# =============================================================================
_DEBUG = (os.getenv("CORE_CONFIG_DEBUG", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _dbg(message: str, level: str = "info", **kwargs: Any) -> None:
    if not _DEBUG:
        return
    try:
        payload = {"message": message, **kwargs}
        line = json_dumps(payload, default=str)
        lvl = (level or "info").lower()
        if lvl == "error":
            logger.error(line)
        elif lvl in ("warn", "warning"):
            logger.warning(line)
        elif lvl == "debug":
            logger.debug(line)
        else:
            logger.info(line)
    except Exception:
        pass


# =============================================================================
# TraceContext (safe)
# =============================================================================
_TRACING_ENABLED = (os.getenv("CORE_TRACING_ENABLED", "") or os.getenv("TRACING_ENABLED", "")).strip().lower() in {
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
    """

    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._cm = None
        self._span = None

    def __enter__(self):
        if _OTEL_AVAILABLE and _TRACING_ENABLED and trace is not None:
            try:
                tracer = trace.get_tracer(__name__)
                self._cm = tracer.start_as_current_span(self.name)
                self._span = self._cm.__enter__()
                try:
                    for k, v in (self.attributes or {}).items():
                        self._span.set_attribute(k, v)
                except Exception:
                    pass
            except Exception:
                self._cm = None
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
            if self._cm is not None:
                try:
                    return self._cm.__exit__(exc_type, exc, tb)
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
# Coercion helpers
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "active"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "inactive"}
_URL_RE = re.compile(r"^https?://", re.IGNORECASE)


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
    # JSON list
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


# =============================================================================
# Sensitive masking helpers
# =============================================================================
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


# =============================================================================
# Deep merge + file loader (safe)
# =============================================================================
def deep_merge(base: Dict[str, Any], override: Dict[str, Any], *, overwrite: bool = True) -> Dict[str, Any]:
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
    """Load config from JSON/YAML/TOML. Never throws; returns {} on failure."""
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

        if suffix in (".yml", ".yaml") and _HAS_YAML and yaml is not None:
            obj = yaml.safe_load(raw)
            return obj if isinstance(obj, dict) else {}

        if suffix == ".toml" and _HAS_TOML and toml is not None:
            obj = toml.loads(raw)
            return obj if isinstance(obj, dict) else {}

        # fallback: try JSON
        obj = json_loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception as e:
        _dbg("load_file_content failed", "warning", path=str(path), error=str(e))
        return {}


# =============================================================================
# Enums
# =============================================================================
class Environment(str, Enum):
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"

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


class AuthType(str, Enum):
    NONE = "none"
    TOKEN = "token"
    BEARER = "bearer"
    API_KEY = "api_key"
    JWT = "jwt"
    OAUTH2 = "oauth2"
    BASIC = "basic"
    MULTI = "multi"


class CacheStrategy(str, Enum):
    MEMORY = "memory"
    REDIS = "redis"
    MEMCACHED = "memcached"
    NONE = "none"


class ConfigSource(str, Enum):
    ENV = "env"
    FILE = "file"
    CONSUL = "consul"
    ETCD = "etcd"
    ZOOKEEPER = "zookeeper"
    DEFAULT = "default"
    RUNTIME = "runtime"


class ConfigStatus(str, Enum):
    ACTIVE = "active"
    ROLLED_BACK = "rolled_back"
    ARCHIVED = "archived"


class EncryptionMethod(str, Enum):
    NONE = "none"
    BASE64 = "base64"
    AES = "aes"
    FERNET = "fernet"


# =============================================================================
# Encryption helper (optional crypto)
# =============================================================================
class ConfigEncryption:
    def __init__(self, *, method: EncryptionMethod = EncryptionMethod.NONE, key: Optional[str] = None):
        self.method = method
        self.key = key
        self._fernet = None
        if self.method == EncryptionMethod.FERNET and _HAS_CRYPTO and key and Fernet is not None:
            try:
                self._fernet = Fernet(key.encode("utf-8"))
            except Exception:
                self._fernet = None

    def encrypt(self, value: str) -> str:
        if not value:
            return value
        try:
            if self.method == EncryptionMethod.FERNET and self._fernet is not None:
                return self._fernet.encrypt(value.encode("utf-8")).decode("utf-8")
            if self.method == EncryptionMethod.BASE64:
                return base64.b64encode(value.encode("utf-8")).decode("utf-8")
            if self.method == EncryptionMethod.AES:
                # transport-safe placeholder (compress + base64)
                return base64.b64encode(zlib.compress(value.encode("utf-8"))).decode("utf-8")
            return value
        except Exception:
            return value

    def decrypt(self, value: str) -> str:
        if not value:
            return value
        try:
            if self.method == EncryptionMethod.FERNET and self._fernet is not None:
                return self._fernet.decrypt(value.encode("utf-8")).decode("utf-8")
            if self.method == EncryptionMethod.BASE64:
                return base64.b64decode(value.encode("utf-8")).decode("utf-8")
            if self.method == EncryptionMethod.AES:
                return zlib.decompress(base64.b64decode(value.encode("utf-8"))).decode("utf-8")
            return value
        except Exception:
            return value


# =============================================================================
# Versioning (minimal, safe)
# =============================================================================
@dataclass(slots=True)
class ConfigVersion:
    version_id: str
    timestamp_utc: str
    source: ConfigSource
    changes: Dict[str, Tuple[Any, Any]]
    author: Optional[str] = None
    comment: Optional[str] = None
    status: ConfigStatus = ConfigStatus.ACTIVE

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version_id": self.version_id,
            "timestamp_utc": self.timestamp_utc,
            "source": self.source.value,
            "changes": {k: [str(v[0]), str(v[1])] for k, v in (self.changes or {}).items()},
            "author": self.author,
            "comment": self.comment,
            "status": self.status.value,
        }


class ConfigVersionManager:
    def __init__(self, max_versions: int = 100):
        self.max_versions = max(10, int(max_versions))
        self._lock = RLock()
        self._versions: List[ConfigVersion] = []

    def add(self, v: ConfigVersion) -> None:
        with self._lock:
            self._versions.append(v)
            if len(self._versions) > self.max_versions:
                self._versions = self._versions[-self.max_versions :]

    def history(self, limit: int = 20) -> List[ConfigVersion]:
        with self._lock:
            return list(reversed(self._versions[-max(1, int(limit)) :]))


_VERSION_MANAGER = ConfigVersionManager(max_versions=100)


def get_version_manager() -> ConfigVersionManager:
    return _VERSION_MANAGER


def save_config_version(
    changes: Dict[str, Tuple[Any, Any]],
    *,
    author: Optional[str] = None,
    comment: Optional[str] = None,
    source: ConfigSource = ConfigSource.RUNTIME,
) -> None:
    try:
        v = ConfigVersion(
            version_id=str(uuid.uuid4()),
            timestamp_utc=datetime.now(timezone.utc).isoformat(),
            source=source,
            changes=changes or {},
            author=author,
            comment=comment,
            status=ConfigStatus.ACTIVE,
        )
        _VERSION_MANAGER.add(v)
    except Exception:
        pass


# =============================================================================
# Google credentials normalization
# =============================================================================
def _maybe_b64_decode(s: str) -> Optional[str]:
    s2 = strip_value(s)
    if not s2:
        return None
    if s2.lower().startswith("b64:"):
        s2 = s2.split(":", 1)[1].strip()
    try:
        return base64.b64decode(s2.encode("utf-8")).decode("utf-8", errors="replace")
    except Exception:
        return None


def _read_text_file_if_exists(p: str) -> Optional[str]:
    try:
        path = Path(strip_value(p))
        if path.exists() and path.is_file():
            return path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None
    return None


def normalize_google_credentials() -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """
    Returns (credentials_json_string, credentials_dict).
    Accepts:
      - GOOGLE_SHEETS_CREDENTIALS / GOOGLE_CREDENTIALS as JSON string
      - GOOGLE_*_B64 (base64 JSON)
      - GOOGLE_APPLICATION_CREDENTIALS / GOOGLE_*_FILE as file path
      - GOOGLE_CREDENTIALS_DICT as JSON dict string
    """
    # 1) dict env
    dict_env = strip_value(os.getenv("GOOGLE_CREDENTIALS_DICT") or "")
    if dict_env.startswith("{") and dict_env.endswith("}"):
        try:
            d = json_loads(dict_env)
            if isinstance(d, dict):
                return (json_dumps(d), d)
        except Exception:
            pass

    # 2) JSON string
    raw = strip_value(os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS") or "")
    if raw.startswith("{") and raw.endswith("}"):
        try:
            d = json_loads(raw)
            if isinstance(d, dict):
                return (raw, d)
        except Exception:
            return (raw, None)

    # 3) base64 variants
    b64 = strip_value(os.getenv("GOOGLE_SHEETS_CREDENTIALS_B64") or os.getenv("GOOGLE_CREDENTIALS_B64") or "")
    if b64:
        decoded = _maybe_b64_decode(b64)
        if decoded:
            decoded = decoded.strip()
            if decoded.startswith("{") and decoded.endswith("}"):
                try:
                    d = json_loads(decoded)
                    if isinstance(d, dict):
                        return (decoded, d)
                except Exception:
                    return (decoded, None)

    # 4) file path
    fp = strip_value(
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        or os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
        or os.getenv("GOOGLE_CREDENTIALS_FILE")
        or ""
    )
    if fp:
        txt = _read_text_file_if_exists(fp)
        if txt:
            t = txt.strip()
            if t.startswith("{") and t.endswith("}"):
                try:
                    d = json_loads(t)
                    if isinstance(d, dict):
                        return (t, d)
                except Exception:
                    return (t, None)

    return (None, None)


# =============================================================================
# Settings (core)
# =============================================================================
@dataclass(frozen=True)
class Settings:
    # Meta
    config_version: str = CONFIG_VERSION
    build_timestamp: str = CONFIG_BUILD_TIMESTAMP

    # App
    environment: Environment = Environment.PRODUCTION
    service_name: str = "Tadawul Fast Bridge"
    timezone: str = "Asia/Riyadh"
    app_name: str = "TFB"
    app_version: str = __version__

    # Logging
    log_level: LogLevel = LogLevel.INFO
    log_format: str = "json"
    log_json: bool = True

    # Auth / Security
    auth_header_name: str = "X-APP-TOKEN"
    require_auth: bool = True
    open_mode: bool = False
    app_token: Optional[str] = None
    backup_app_token: Optional[str] = None

    # Backend (internal integrations)
    backend_base_url: str = "http://localhost:8000"

    # Providers (GLOBAL + KSA)
    enabled_providers: List[str] = field(default_factory=lambda: ["eodhd"])
    ksa_providers: List[str] = field(default_factory=lambda: ["yahoo_chart", "argaam"])
    primary_provider: str = "eodhd"

    # Provider keys + base URLs
    eodhd_api_key: Optional[str] = None
    eodhd_base_url: str = "https://eodhd.com/api"

    finnhub_api_key: Optional[str] = None
    fmp_api_key: Optional[str] = None
    alphavantage_api_key: Optional[str] = None
    marketstack_api_key: Optional[str] = None
    twelvedata_api_key: Optional[str] = None

    fmp_base_url: Optional[str] = None

    # EODHD global defaults
    eodhd_symbol_suffix_default: str = "US"
    eodhd_append_exchange_suffix: bool = True  # AAPL -> AAPL.US when needed

    # Market feature flags
    tadawul_market_enabled: bool = True
    ksa_disallow_eodhd: bool = False

    # API feature flags
    advanced_analysis_enabled: bool = True
    ai_analysis_enabled: bool = True

    # HTTP / Retry
    http_timeout_sec: float = 45.0
    max_retries: int = 2
    retry_delay_sec: float = 1.0

    # Cache
    cache_ttl_sec: int = 20
    engine_cache_ttl_sec: int = 60
    cache_max_size: int = 2048

    # Concurrency / batch sizes
    batch_concurrency: int = 5
    ai_batch_size: int = 20
    quote_batch_size: int = 50

    # Engine lifecycle
    init_engine_on_boot: bool = True
    defer_router_mount: bool = False

    # Google Sheets integration
    default_spreadsheet_id: Optional[str] = None
    google_sheets_credentials_json: Optional[str] = None
    google_credentials_dict: Optional[Dict[str, Any]] = None
    google_apps_script_url: Optional[str] = None
    google_apps_script_backup_url: Optional[str] = None

    @classmethod
    def from_env(cls) -> "Settings":
        env_name = strip_value(os.getenv("APP_ENV") or os.getenv("TFB_ENV") or "production")

        # Render-style meta
        app_name = strip_value(os.getenv("APP_NAME") or "TFB")
        app_version = strip_value(os.getenv("APP_VERSION") or __version__)

        # --- auth ---
        token = strip_value(os.getenv("APP_TOKEN") or os.getenv("TFB_APP_TOKEN") or os.getenv("BACKEND_TOKEN") or "")
        backup_token = strip_value(os.getenv("BACKUP_APP_TOKEN") or "")

        # Default: require auth if you provided a token, else respect REQUIRE_AUTH or default True
        require_auth = coerce_bool(os.getenv("REQUIRE_AUTH"), bool(token or backup_token) or True)

        # OPEN_MODE explicit wins; else open only when require_auth is false AND no tokens exist
        if os.getenv("OPEN_MODE") is not None:
            open_mode = coerce_bool(os.getenv("OPEN_MODE"), False)
        else:
            open_mode = (not require_auth) and (not bool(token or backup_token))

        # --- providers keys / base urls ---
        eodhd_key = strip_value(os.getenv("EODHD_API_TOKEN") or os.getenv("EODHD_API_KEY") or os.getenv("EODHD_KEY") or "")
        finnhub_key = strip_value(os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_KEY") or "")
        fmp_key = strip_value(os.getenv("FMP_API_KEY") or "")
        alpha_key = strip_value(
            os.getenv("ALPHA_VANTAGE_API_KEY")
            or os.getenv("ALPHAVANTAGE_API_KEY")
            or os.getenv("ALPHAVANTAGE_KEY")
            or ""
        )
        marketstack_key = strip_value(os.getenv("MARKETSTACK_API_KEY") or "")
        twelvedata_key = strip_value(os.getenv("TWELVEDATA_API_KEY") or "")

        eodhd_base_url = strip_value(os.getenv("EODHD_BASE_URL") or "https://eodhd.com/api")
        fmp_base_url = strip_value(os.getenv("FMP_BASE_URL") or "") or None

        eodhd_suffix_default = strip_value(os.getenv("EODHD_DEFAULT_EXCHANGE") or os.getenv("EODHD_SYMBOL_SUFFIX_DEFAULT") or "US")
        eodhd_append_suffix = coerce_bool(os.getenv("EODHD_APPEND_EXCHANGE_SUFFIX"), True)

        # Providers list
        enabled_providers = [p.lower() for p in coerce_list(os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS"))]
        if not enabled_providers:
            enabled_providers = []
            if eodhd_key:
                enabled_providers.append("eodhd")
            if finnhub_key:
                enabled_providers.append("finnhub")
            if fmp_key:
                enabled_providers.append("fmp")
            if not enabled_providers:
                enabled_providers = ["eodhd"]

        ksa_providers = [p.lower() for p in coerce_list(os.getenv("KSA_PROVIDERS") or "yahoo_chart,argaam")]
        primary_provider = strip_value(os.getenv("PRIMARY_PROVIDER") or "eodhd").lower()

        # Logging
        ll = strip_value(os.getenv("LOG_LEVEL") or "INFO").upper()
        log_level = LogLevel.INFO
        for x in LogLevel:
            if x.value == ll:
                log_level = x
                break
        log_format = strip_value(os.getenv("LOG_FORMAT") or "json").lower()
        log_json = coerce_bool(os.getenv("LOG_JSON"), True)

        # Backend base URL
        backend_base_url = strip_value(
            os.getenv("BACKEND_BASE_URL") or os.getenv("DEFAULT_BACKEND_URL") or "http://localhost:8000"
        )

        # Feature flags
        advanced_analysis_enabled = coerce_bool(os.getenv("ADVANCED_ANALYSIS_ENABLED"), True)
        ai_analysis_enabled = coerce_bool(os.getenv("AI_ANALYSIS_ENABLED"), True)
        tadawul_market_enabled = coerce_bool(os.getenv("TADAWUL_MARKET_ENABLED"), True)
        ksa_disallow_eodhd = coerce_bool(os.getenv("KSA_DISALLOW_EODHD"), False)

        # HTTP / Retry / Cache / Engine
        http_timeout_sec = coerce_float(os.getenv("HTTP_TIMEOUT_SEC"), 45.0, lo=5.0, hi=300.0)
        max_retries = coerce_int(os.getenv("MAX_RETRIES"), 2, lo=0, hi=20)
        retry_delay_sec = coerce_float(os.getenv("RETRY_DELAY") or os.getenv("RETRY_DELAY_SEC"), 1.0, lo=0.0, hi=30.0)

        cache_ttl_sec = coerce_int(os.getenv("CACHE_TTL_SEC") or os.getenv("CACHE_DEFAULT_TTL"), 20, lo=1, hi=3600)
        engine_cache_ttl_sec = coerce_int(os.getenv("ENGINE_CACHE_TTL_SEC"), 60, lo=1, hi=86400)
        cache_max_size = coerce_int(os.getenv("CACHE_MAX_SIZE"), 2048, lo=128, hi=200000)

        batch_concurrency = coerce_int(os.getenv("BATCH_CONCURRENCY") or os.getenv("WEB_CONCURRENCY"), 5, lo=1, hi=50)
        ai_batch_size = coerce_int(os.getenv("AI_BATCH_SIZE"), 20, lo=1, hi=500)
        quote_batch_size = coerce_int(os.getenv("QUOTE_BATCH_SIZE") or os.getenv("TADAWUL_MAX_SYMBOLS"), 50, lo=1, hi=500)

        init_engine_on_boot = coerce_bool(os.getenv("INIT_ENGINE_ON_BOOT"), True)
        defer_router_mount = coerce_bool(os.getenv("DEFER_ROUTER_MOUNT"), False)

        # Sheets
        default_spreadsheet_id = strip_value(os.getenv("DEFAULT_SPREADSHEET_ID") or "") or None
        gs_json, gs_dict = normalize_google_credentials()
        google_apps_script_url = strip_value(os.getenv("GOOGLE_APPS_SCRIPT_URL") or "") or None
        google_apps_script_backup_url = strip_value(os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL") or "") or None

        tz = strip_value(os.getenv("TZ") or "Asia/Riyadh")

        return cls(
            environment=Environment.from_string(env_name),
            service_name=strip_value(os.getenv("SERVICE_NAME") or "Tadawul Fast Bridge"),
            timezone=tz,
            app_name=app_name,
            app_version=app_version,
            log_level=log_level,
            log_format=log_format,
            log_json=log_json,
            auth_header_name=strip_value(os.getenv("AUTH_HEADER_NAME") or "X-APP-TOKEN"),
            require_auth=require_auth,
            open_mode=open_mode,
            app_token=token or None,
            backup_app_token=backup_token or None,
            backend_base_url=backend_base_url,
            enabled_providers=enabled_providers,
            ksa_providers=ksa_providers,
            primary_provider=primary_provider,
            eodhd_api_key=eodhd_key or None,
            eodhd_base_url=eodhd_base_url,
            finnhub_api_key=finnhub_key or None,
            fmp_api_key=fmp_key or None,
            alphavantage_api_key=alpha_key or None,
            marketstack_api_key=marketstack_key or None,
            twelvedata_api_key=twelvedata_key or None,
            fmp_base_url=fmp_base_url,
            eodhd_symbol_suffix_default=eodhd_suffix_default or "US",
            eodhd_append_exchange_suffix=eodhd_append_suffix,
            tadawul_market_enabled=tadawul_market_enabled,
            ksa_disallow_eodhd=ksa_disallow_eodhd,
            advanced_analysis_enabled=advanced_analysis_enabled,
            ai_analysis_enabled=ai_analysis_enabled,
            http_timeout_sec=http_timeout_sec,
            max_retries=max_retries,
            retry_delay_sec=retry_delay_sec,
            cache_ttl_sec=cache_ttl_sec,
            engine_cache_ttl_sec=engine_cache_ttl_sec,
            cache_max_size=cache_max_size,
            batch_concurrency=batch_concurrency,
            ai_batch_size=ai_batch_size,
            quote_batch_size=quote_batch_size,
            init_engine_on_boot=init_engine_on_boot,
            defer_router_mount=defer_router_mount,
            default_spreadsheet_id=default_spreadsheet_id,
            google_sheets_credentials_json=gs_json,
            google_credentials_dict=gs_dict,
            google_apps_script_url=google_apps_script_url,
            google_apps_script_backup_url=google_apps_script_backup_url,
        )

    def validate(self) -> Tuple[List[str], List[str]]:
        """
        Validation is NON-FATAL by design.
        Your app should decide how to treat errors (health endpoint may degrade).
        """
        errors: List[str] = []
        warnings_list: List[str] = []

        if self.backend_base_url and not is_valid_url(self.backend_base_url):
            warnings_list.append(f"backend_base_url does not look like a URL: {self.backend_base_url!r}")

        enabled_lower = [p.lower() for p in (self.enabled_providers or [])]

        if "eodhd" in enabled_lower and not self.eodhd_api_key:
            errors.append("Provider 'eodhd' is enabled but EODHD_API_KEY is missing.")

        if self.primary_provider and self.primary_provider.lower() not in enabled_lower:
            warnings_list.append(
                f"PRIMARY_PROVIDER={self.primary_provider!r} not included in ENABLED_PROVIDERS={self.enabled_providers!r}."
            )

        if self.require_auth and not self.open_mode:
            if not (self.app_token or self.backup_app_token or allowed_tokens()):
                errors.append("REQUIRE_AUTH=true but no APP_TOKEN/BACKUP_APP_TOKEN/ALLOWED_TOKENS configured (and OPEN_MODE=false).")

        if self.open_mode and (self.app_token or self.backup_app_token):
            warnings_list.append("OPEN_MODE=true while tokens exist. Service may be publicly exposed unintentionally.")

        return errors, warnings_list

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_file(cls, filepath: str) -> "Settings":
        path = Path(filepath)
        content = load_file_content(path)
        base = cls.from_env()
        merged = deep_merge(asdict(base), content, overwrite=True)
        allowed = set(cls.__dataclass_fields__.keys())
        cleaned = {k: v for k, v in merged.items() if k in allowed}
        return cls(**cleaned)


# =============================================================================
# Settings cache + API
# =============================================================================
class SettingsCache:
    def __init__(self, ttl_seconds: int = 30):
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._lock = RLock()
        self._ttl = max(1, int(ttl_seconds))

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            item = self._cache.get(key)
            if not item:
                return None
            value, exp = item
            if time.time() < exp:
                return value
            self._cache.pop(key, None)
            return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        with self._lock:
            t = self._ttl if ttl is None else max(1, int(ttl))
            self._cache[key] = (value, time.time() + t)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {"size": len(self._cache), "ttl": self._ttl}


_SETTINGS_CACHE_TTL = coerce_int(os.getenv("CORE_SETTINGS_CACHE_TTL"), 30, lo=1, hi=3600)
_SETTINGS_CACHE = SettingsCache(ttl_seconds=_SETTINGS_CACHE_TTL)


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


# =============================================================================
# Distributed sources (optional, LAZY, safe, env-driven)
# =============================================================================
class ConfigSourceManager:
    def __init__(self):
        self.sources: List[Tuple[ConfigSource, Callable[[], Optional[Dict[str, Any]]]]] = []
        self._lock = RLock()

    def register_source(self, source: ConfigSource, loader: Callable[[], Optional[Dict[str, Any]]]) -> None:
        with self._lock:
            self.sources.append((source, loader))

    def load_merged(self) -> Dict[str, Any]:
        merged: Dict[str, Any] = {}
        with self._lock:
            for src, loader in self.sources:
                try:
                    data = loader()
                    if isinstance(data, dict) and data:
                        merged = deep_merge(merged, data, overwrite=True)
                except Exception as e:
                    _dbg("distributed source load failed", "warning", source=src.value, error=str(e))
        return merged


_CONFIG_SOURCES = ConfigSourceManager()
_DISTRIBUTED_INIT_DONE = False
_DISTRIBUTED_INIT_LOCK = RLock()


def _distributed_enabled() -> bool:
    # Explicit flag OR presence of a host var (but still lazy, no connection at import time)
    if os.getenv("DISTRIBUTED_CONFIG_ENABLED") is not None:
        return coerce_bool(os.getenv("DISTRIBUTED_CONFIG_ENABLED"), False)
    return bool(os.getenv("CONSUL_HOST") or os.getenv("ETCD_HOST") or os.getenv("ZOOKEEPER_HOSTS"))


class ConsulConfigSource:
    """
    LAZY: does NOT connect in __init__.
    """

    def __init__(self, host: str, port: int, token: Optional[str], prefix: str):
        self.host = host
        self.port = port
        self.token = token
        self.prefix = prefix

    def load(self) -> Optional[Dict[str, Any]]:
        if not (_CONSUL_AVAILABLE and consul is not None):
            return None
        try:
            client = consul.Consul(host=self.host, port=self.port, token=self.token)  # type: ignore
            _, data = client.kv.get(self.prefix, recurse=True)  # type: ignore
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
                    txt = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
                    try:
                        val = json_loads(txt)
                    except Exception:
                        val = txt
                except Exception:
                    continue

                parts = [p for p in rel.split("/") if p]
                cur = out
                for p in parts[:-1]:
                    cur.setdefault(p, {})
                    if not isinstance(cur[p], dict):
                        cur[p] = {}
                    cur = cur[p]
                if parts:
                    cur[parts[-1]] = val

            return out or None
        except Exception as e:
            _dbg("consul load failed", "warning", error=str(e))
            return None


class EtcdConfigSource:
    """
    LAZY: does NOT connect in __init__.
    """

    def __init__(self, host: str, port: int, prefix: str):
        self.host = host
        self.port = port
        self.prefix = prefix

    def load(self) -> Optional[Dict[str, Any]]:
        if not (_ETCD_AVAILABLE and etcd3 is not None):
            return None
        try:
            client = etcd3.client(host=self.host, port=self.port)  # type: ignore
            out: Dict[str, Any] = {}
            for value, meta in client.get_prefix(self.prefix):  # type: ignore
                key = (meta.key or b"").decode("utf-8", errors="replace")
                rel = key[len(self.prefix) :].lstrip("/")
                try:
                    txt = value.decode("utf-8", errors="replace") if isinstance(value, (bytes, bytearray)) else str(value)
                    try:
                        val = json_loads(txt)
                    except Exception:
                        val = txt
                except Exception:
                    continue

                parts = [p for p in rel.split("/") if p]
                cur = out
                for p in parts[:-1]:
                    cur.setdefault(p, {})
                    if not isinstance(cur[p], dict):
                        cur[p] = {}
                    cur = cur[p]
                if parts:
                    cur[parts[-1]] = val

            return out or None
        except Exception as e:
            _dbg("etcd load failed", "warning", error=str(e))
            return None


class ZooKeeperConfigSource:
    """
    LAZY: client is created inside load() with a small timeout.
    """

    def __init__(self, hosts: str, prefix: str, start_timeout_sec: int = 3):
        self.hosts = hosts
        self.prefix = prefix
        self.start_timeout_sec = max(1, int(start_timeout_sec))

    def load(self) -> Optional[Dict[str, Any]]:
        if not (_ZOOKEEPER_AVAILABLE and KazooClient is not None):
            return None
        client = None
        try:
            client = KazooClient(hosts=self.hosts)  # type: ignore
            client.start(timeout=self.start_timeout_sec)

            out: Dict[str, Any] = {}

            def walk(path: str, cur: Dict[str, Any]) -> None:
                if not client.exists(path):
                    return
                children = client.get_children(path)
                for child in children:
                    child_path = f"{path}/{child}"
                    data, _ = client.get(child_path)
                    sub = client.get_children(child_path)
                    if sub:
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
            _dbg("zookeeper load failed", "warning", error=str(e))
            return None
        finally:
            try:
                if client is not None:
                    client.stop()
                    client.close()
            except Exception:
                pass


def init_distributed_config() -> None:
    """
    Register distributed sources only if env vars exist and feature is enabled.
    STARTUP-SAFE: registers loaders only; no connections here.
    """
    global _DISTRIBUTED_INIT_DONE
    with _DISTRIBUTED_INIT_LOCK:
        if _DISTRIBUTED_INIT_DONE:
            return
        _DISTRIBUTED_INIT_DONE = True

        if not _distributed_enabled():
            return

        with TraceContext("init_distributed_config"):
            if os.getenv("CONSUL_HOST"):
                host = strip_value(os.getenv("CONSUL_HOST") or "localhost")
                port = coerce_int(os.getenv("CONSUL_PORT"), 8500, lo=1, hi=65535)
                token = strip_value(os.getenv("CONSUL_TOKEN")) or None
                prefix = strip_value(os.getenv("CONSUL_PREFIX") or "config/tadawul")
                src = ConsulConfigSource(host=host, port=port, token=token, prefix=prefix)
                _CONFIG_SOURCES.register_source(ConfigSource.CONSUL, src.load)

            if os.getenv("ETCD_HOST"):
                host = strip_value(os.getenv("ETCD_HOST") or "localhost")
                port = coerce_int(os.getenv("ETCD_PORT"), 2379, lo=1, hi=65535)
                prefix = strip_value(os.getenv("ETCD_PREFIX") or "/config/tadawul")
                src = EtcdConfigSource(host=host, port=port, prefix=prefix)
                _CONFIG_SOURCES.register_source(ConfigSource.ETCD, src.load)

            if os.getenv("ZOOKEEPER_HOSTS"):
                hosts = strip_value(os.getenv("ZOOKEEPER_HOSTS") or "localhost:2181")
                prefix = strip_value(os.getenv("ZOOKEEPER_PREFIX") or "/config/tadawul")
                zkt = coerce_int(os.getenv("ZOOKEEPER_START_TIMEOUT_SEC"), 3, lo=1, hi=10)
                src = ZooKeeperConfigSource(hosts=hosts, prefix=prefix, start_timeout_sec=zkt)
                _CONFIG_SOURCES.register_source(ConfigSource.ZOOKEEPER, src.load)


def reload_settings() -> Settings:
    """
    Reload settings by merging distributed overrides into env-derived settings.
    - distributed init is LAZY and safe
    - overrides keys must match Settings dataclass fields
    """
    with TraceContext("reload_settings"):
        init_distributed_config()
        _SETTINGS_CACHE.clear()

        base = asdict(Settings.from_env())
        merged = _CONFIG_SOURCES.load_merged()
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
        init_distributed_config()
        s = get_settings_cached()
        health["checks"]["settings_accessible"] = True
        errs, warns = s.validate()
        if errs:
            health["status"] = "degraded"
            health["checks"]["settings_valid"] = False
            health["errors"].extend(errs)
        else:
            health["checks"]["settings_valid"] = True
        health["warnings"].extend(warns)
    except Exception as e:
        health["status"] = "unhealthy"
        health["checks"]["settings_accessible"] = False
        health["errors"].append(str(e))

    health["checks"]["cache"] = _SETTINGS_CACHE.stats()
    health["checks"]["distributed_sources"] = len(_CONFIG_SOURCES.sources)
    health["checks"]["has_orjson"] = bool(_HAS_ORJSON)
    health["checks"]["tracing_enabled"] = bool(_TRACING_ENABLED and _OTEL_AVAILABLE)
    return health


# =============================================================================
# Auth helpers (used by API)
# =============================================================================
def allowed_tokens() -> List[str]:
    """
    Allowed tokens list (comma-separated).
    Priority:
      1) ALLOWED_TOKENS
      2) TFB_ALLOWED_TOKENS
      3) APP_TOKENS
      4) APP_TOKEN + BACKUP_APP_TOKEN
    """
    toks = coerce_list(os.getenv("ALLOWED_TOKENS") or os.getenv("TFB_ALLOWED_TOKENS") or os.getenv("APP_TOKENS"))
    toks = [t for t in toks if t]
    if toks:
        return toks
    t1 = strip_value(os.getenv("APP_TOKEN") or os.getenv("TFB_APP_TOKEN") or "")
    t2 = strip_value(os.getenv("BACKUP_APP_TOKEN") or "")
    out: List[str] = []
    if t1:
        out.append(t1)
    if t2 and t2 not in out:
        out.append(t2)
    return out


def is_open_mode() -> bool:
    """
    If OPEN_MODE is set => honor it.
    Else open only when REQUIRE_AUTH is false AND no tokens exist.
    """
    env_flag = os.getenv("OPEN_MODE")
    if env_flag is not None:
        return coerce_bool(env_flag, False)

    req = coerce_bool(os.getenv("REQUIRE_AUTH"), False)
    return (not req) and (len(allowed_tokens()) == 0)


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
    Standard auth check used by routes.
    Supports:
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


def mask_settings(settings: Optional[Settings] = None) -> Dict[str, Any]:
    s = settings or get_settings_cached()
    d = s.to_dict()
    d = mask_secret_dict(d, set(SENSITIVE_KEYS))
    d["open_mode_effective"] = bool(is_open_mode())
    d["token_count"] = len(allowed_tokens())
    d["config_version"] = CONFIG_VERSION
    return d


# =============================================================================
# Exports
# =============================================================================
__all__ = [
    "__version__",
    "CONFIG_VERSION",
    "CONFIG_BUILD_TIMESTAMP",
    # core types
    "Environment",
    "LogLevel",
    "AuthType",
    "CacheStrategy",
    "ConfigSource",
    "ConfigStatus",
    "EncryptionMethod",
    # utilities
    "TraceContext",
    "_dbg",
    "strip_value",
    "coerce_bool",
    "coerce_int",
    "coerce_float",
    "coerce_list",
    "coerce_dict",
    "is_valid_url",
    "deep_merge",
    "load_file_content",
    "mask_secret",
    "mask_secret_dict",
    # versioning
    "ConfigVersion",
    "ConfigVersionManager",
    "get_version_manager",
    "save_config_version",
    # encryption
    "ConfigEncryption",
    # settings
    "Settings",
    "get_settings",
    "get_settings_cached",
    "reload_settings",
    "config_health_check",
    # auth
    "allowed_tokens",
    "is_open_mode",
    "auth_ok",
    "mask_settings",
    # distributed init
    "init_distributed_config",
]
