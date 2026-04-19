#!/usr/bin/env python3
# core/config.py
"""
================================================================================
Core Configuration Module -- v7.0.0
(RENDER-SAFE / STARTUP-SAFE / SCHEMA-AWARE / ROUTE-AUTH-CONTROLLED)
================================================================================
TADAWUL FAST BRIDGE -- Enterprise Configuration Management

Primary goals:
- ✅ Avoid deploy/startup failures (NO network calls, NO heavy init at import-time)
- ✅ Defensive env parsing + safe optional dependencies
- ✅ Google credentials normalization (JSON / base64 / file path)
- ✅ Distributed config sources are LAZY and opt-in (won't block Render port binding)
- ✅ Schema/page alignment via core.sheets.page_catalog + core.sheets.schema_registry
- ✅ CRITICAL: headers/columns MUST ALWAYS exist even if computations are disabled
- ✅ Granular route auth controls

v7.0.0 Changes (from v6.0.0)
----------------------------
Bug fixes:
  - `mask_secret_dict` previously leaked string/bytes values inside lists when
    the enclosing key was sensitive (e.g. `{"api_keys": ["sk_live_...", ...]}`
    produced unmasked output). Fixed: when the key matches a sensitive pattern
    and the value is a list of primitives, each primitive is masked.
  - `normalize_google_credentials` previously returned `(raw_string, None)`
    when a `{...}`-looking env var failed to JSON-parse, AND short-circuited
    further fallback paths (base64 / file). Callers then received malformed
    JSON strings as "valid credentials". Fixed: parse failures are logged and
    we continue to the next source; the function only returns a raw string
    when it maps to a valid credentials dict.
  - `coerce_dict(value, default=some_dict)` returned the caller's `default`
    by reference, so later mutations on the returned dict leaked back into
    the caller's default. Fixed: default is shallow-copied before return.
  - `Environment.from_string("dev")` resolved to PRODUCTION because the enum
    only accepted exact values. Added an alias table covering common forms
    (`dev`, `prod`, `stage`, `test`, `qa`, `uat`, `ci`) before falling back
    to the configured default (still PRODUCTION for safety).

Cleanup:
  - Added `ConfigError`, `ConfigValidationError`, and `ConfigSourceError` to
    `__all__` — they're catchable exceptions callers need access to.
  - `TraceContext.__call__` now uses `functools.wraps` so decorated functions
    preserve `__qualname__`, `__module__`, `__wrapped__`, and annotations
    instead of only `__name__` and `__doc__`.
  - Removed unused `cast` import from typing.
  - `LogLevel` lookup in `Settings.from_env` uses direct member lookup via
    `LogLevel.__members__` instead of a linear scan.
  - `Settings.from_env` no longer double-reads `OPEN_MODE` from the environment.

Preserved:
  - Full `__all__` surface (60+ public names; exception exports added).
  - Every public function signature and behavior for valid inputs.
  - orjson fast-path with stdlib fallback.
  - Entire distributed config-source stack (Consul / Etcd / ZooKeeper),
    encryption helper, version management.
  - `Settings` dataclass field set, defaults, and `from_env` / `from_file`
    contracts.
  - Auth helpers: `auth_ok`, `is_public_path`, `is_open_mode`,
    `allowed_tokens`.
  - Schema import fallback chain and page-catalog shims.
================================================================================
"""

from __future__ import annotations

import base64
import copy
import functools
import json
import logging
import os
import re
import time
import uuid
import zlib
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from threading import RLock
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

# =============================================================================
# Version
# =============================================================================

__version__ = "7.0.0"
CONFIG_VERSION = __version__
CONFIG_BUILD_TIMESTAMP = datetime.now(timezone.utc).isoformat()

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# =============================================================================
# Fast JSON (orjson optional)
# =============================================================================

try:
    import orjson  # type: ignore

    def _json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

    def _json_dumps(obj: Any, *, default: Callable = str) -> str:
        return orjson.dumps(obj, default=default).decode("utf-8")

    _HAS_ORJSON = True
except ImportError:
    def _json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    def _json_dumps(obj: Any, *, default: Callable = str) -> str:
        return json.dumps(obj, default=default, ensure_ascii=False)

    _HAS_ORJSON = False

# =============================================================================
# Optional Dependencies (Safe)
# =============================================================================

try:
    import yaml  # type: ignore
    _HAS_YAML = True
except ImportError:
    yaml = None  # type: ignore
    _HAS_YAML = False

try:
    import toml  # type: ignore
    _HAS_TOML = True
except ImportError:
    toml = None  # type: ignore
    _HAS_TOML = False

try:
    from cryptography.fernet import Fernet  # type: ignore
    _HAS_CRYPTO = True
except ImportError:
    Fernet = None  # type: ignore
    _HAS_CRYPTO = False

try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore
    _OTEL_AVAILABLE = True
except ImportError:
    trace = None  # type: ignore
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    _OTEL_AVAILABLE = False

try:
    import consul  # type: ignore
    _CONSUL_AVAILABLE = True
except ImportError:
    consul = None  # type: ignore
    _CONSUL_AVAILABLE = False

try:
    import etcd3  # type: ignore
    _ETCD_AVAILABLE = True
except ImportError:
    etcd3 = None  # type: ignore
    _ETCD_AVAILABLE = False

try:
    from kazoo.client import KazooClient  # type: ignore
    _ZOOKEEPER_AVAILABLE = True
except ImportError:
    KazooClient = None  # type: ignore
    _ZOOKEEPER_AVAILABLE = False

# =============================================================================
# Custom Exceptions
# =============================================================================

class ConfigError(Exception):
    """Base exception for configuration errors."""
    pass


class ConfigValidationError(ConfigError):
    """Raised when configuration validation fails."""
    pass


class ConfigSourceError(ConfigError):
    """Raised when a config source fails to load."""
    pass


# =============================================================================
# Debug Logging Helper
# =============================================================================

_DEBUG = (os.getenv("CORE_CONFIG_DEBUG", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _dbg(message: str, level: str = "info", **kwargs: Any) -> None:
    """Debug logging for configuration module."""
    if not _DEBUG:
        return
    try:
        payload = {"message": message, **kwargs}
        line = _json_dumps(payload, default=str)
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
# TraceContext (Safe Wrapper)
# =============================================================================

_TRACING_ENABLED = (
    os.getenv("CORE_TRACING_ENABLED", "") or os.getenv("TRACING_ENABLED", "")
).strip().lower() in {"1", "true", "yes", "y", "on"}


class TraceContext:
    """
    Lightweight OpenTelemetry wrapper.

    Use as context manager: with TraceContext("name"): ...
    Use as decorator: @TraceContext("name")
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
                for k, v in self.attributes.items():
                    self._span.set_attribute(k, v)
            except Exception:
                self._cm = None
                self._span = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self._span is not None and exc_val is not None and Status is not None and StatusCode is not None:
                try:
                    self._span.record_exception(exc_val)
                except Exception:
                    pass
                try:
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

    def __call__(self, fn: Callable) -> Callable:
        # v7.0.0: use functools.wraps so the decorated function preserves
        # __qualname__, __module__, __wrapped__, annotations, etc.
        name = self.name
        attributes = self.attributes

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            with TraceContext(name, attributes):
                return fn(*args, **kwargs)

        return wrapper


# =============================================================================
# Coercion Helpers
# =============================================================================

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "active"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "inactive"}
_URL_RE = re.compile(r"^https?://", re.IGNORECASE)


def strip_value(value: Any) -> str:
    """Strip value to string, return empty string for None."""
    if value is None:
        return ""
    try:
        return str(value).strip()
    except Exception:
        return ""


def coerce_bool(value: Any, default: bool) -> bool:
    """Coerce value to boolean."""
    s = strip_value(value).lower()
    if not s:
        return default
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def coerce_int(value: Any, default: int, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    """Coerce value to integer with optional bounds."""
    try:
        if isinstance(value, (int, float)):
            x = int(float(value))
        else:
            x = int(float(strip_value(value)))
    except Exception:
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


def coerce_float(value: Any, default: float, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    """Coerce value to float with optional bounds."""
    try:
        if isinstance(value, (int, float)):
            x = float(value)
        else:
            x = float(strip_value(value))
    except Exception:
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


def coerce_list(value: Any, default: Optional[List[str]] = None) -> List[str]:
    """Coerce value to list of strings."""
    if default is None:
        default = []
    if value is None:
        return list(default)  # return a fresh list, not the caller's default
    if isinstance(value, (list, tuple)):
        return [strip_value(x) for x in value if strip_value(x)]
    s = strip_value(value)
    if not s:
        return list(default)
    # Try JSON list
    if s.startswith("[") and s.endswith("]"):
        try:
            parsed = _json_loads(s)
            if isinstance(parsed, list):
                return [strip_value(x) for x in parsed if strip_value(x)]
        except Exception:
            pass
    # Comma-separated
    return [x.strip() for x in s.split(",") if x.strip()]


def coerce_dict(value: Any, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Coerce value to dictionary.

    v7.0.0: returns a shallow copy of the default on miss, so mutations on
    the returned dict don't leak back into the caller's supplied default.
    """
    if default is None:
        default = {}
    if value is None:
        return dict(default)
    if isinstance(value, dict):
        return value
    s = strip_value(value)
    if not s:
        return dict(default)
    try:
        parsed = _json_loads(s)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    return dict(default)


def is_valid_url(url: str) -> bool:
    """Check if string is a valid HTTP/HTTPS URL."""
    return bool(_URL_RE.match(strip_value(url)))


# =============================================================================
# Sensitive Masking Helpers
# =============================================================================

SENSITIVE_KEYS = {
    "token", "secret", "key", "api_key", "apikey", "password",
    "credential", "authorization", "bearer", "jwt", "private_key",
    "client_secret", "encryption_key",
}


def mask_secret(s: Optional[str], reveal_first: int = 2, reveal_last: int = 4) -> Optional[str]:
    """Mask sensitive string, showing only first/last characters."""
    if not s:
        return None
    s = strip_value(s)
    if len(s) <= reveal_first + reveal_last + 3:
        return "***"
    return s[:reveal_first] + "..." + s[-reveal_last:]


def _key_is_sensitive(key: str, sensitive_keys: Set[str]) -> bool:
    """Return True if the (lower-cased) key matches any sensitive pattern."""
    lk = str(key).lower()
    return any(sk in lk for sk in sensitive_keys)


def mask_secret_dict(d: Dict[str, Any], sensitive_keys: Optional[Set[str]] = None) -> Dict[str, Any]:
    """Recursively mask sensitive values in dictionary.

    v7.0.0: string/bytes items inside a list are now masked when the
    enclosing key is sensitive (e.g. ``{"api_keys": ["sk_live_...", ...]}``).
    Previously only dict items inside the list were recursed, so raw
    credential strings leaked through unmasked.
    """
    if sensitive_keys is None:
        sensitive_keys = set(SENSITIVE_KEYS)
    result: Dict[str, Any] = {}
    for k, v in (d or {}).items():
        is_sensitive = _key_is_sensitive(k, sensitive_keys)
        if isinstance(v, dict):
            result[k] = mask_secret_dict(v, sensitive_keys)
        elif isinstance(v, list):
            new_list: List[Any] = []
            for x in v:
                if isinstance(x, dict):
                    new_list.append(mask_secret_dict(x, sensitive_keys))
                elif is_sensitive and isinstance(x, (str, bytes)):
                    new_list.append(mask_secret(str(x)))
                else:
                    new_list.append(x)
            result[k] = new_list
        elif is_sensitive and isinstance(v, (str, bytes)):
            result[k] = mask_secret(str(v))
        else:
            result[k] = v
    return result


# =============================================================================
# Deep Merge + File Loader
# =============================================================================

def deep_merge(base: Dict[str, Any], override: Dict[str, Any], overwrite: bool = True) -> Dict[str, Any]:
    """
    Deep merge two dictionaries.

    Args:
        base: Base dictionary
        override: Override dictionary
        overwrite: If True, override existing keys; if False, only add missing keys

    Returns:
        Merged dictionary
    """
    if not isinstance(base, dict):
        base = {}
    if not isinstance(override, dict):
        return dict(base)
    result = copy.deepcopy(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = deep_merge(result[k], v, overwrite=overwrite)
        else:
            if overwrite or k not in result:
                result[k] = copy.deepcopy(v)
    return result


def load_file_content(path: Path) -> Dict[str, Any]:
    """
    Load configuration from file (JSON/YAML/TOML).

    Returns empty dict on failure.
    """
    try:
        if not path.exists() or not path.is_file():
            return {}
        raw = path.read_text(encoding="utf-8", errors="replace").strip()
        if not raw:
            return {}
        suffix = path.suffix.lower()
        if suffix == ".json":
            obj = _json_loads(raw)
            return obj if isinstance(obj, dict) else {}
        if suffix in (".yml", ".yaml") and _HAS_YAML and yaml is not None:
            obj = yaml.safe_load(raw)
            return obj if isinstance(obj, dict) else {}
        if suffix == ".toml" and _HAS_TOML and toml is not None:
            obj = toml.loads(raw)
            return obj if isinstance(obj, dict) else {}
        obj = _json_loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception as e:
        _dbg("load_file_content failed", "warning", path=str(path), error=str(e))
        return {}


# =============================================================================
# Enums
# =============================================================================

# v7.0.0: common abbreviations used in deployment configs that should map
# to canonical Environment values.
_ENV_ALIASES: Dict[str, str] = {
    "dev": "development",
    "develop": "development",
    "local": "development",
    "prod": "production",
    "live": "production",
    "stage": "staging",
    "stg": "staging",
    "test": "testing",
    "tst": "testing",
    "qa": "testing",
    "uat": "staging",
    "ci": "testing",
}


class Environment(str, Enum):
    """Deployment environment."""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"

    @classmethod
    def from_string(cls, value: str, default: "Optional[Environment]" = None) -> "Environment":
        """Resolve a string to an Environment, with alias support.

        v7.0.0: handles common abbreviations like ``dev``, ``prod``,
        ``stage``, ``test`` before falling back to ``default`` (which is
        ``PRODUCTION`` when unspecified — fail-closed for security).
        """
        v = strip_value(value).lower()
        if not v:
            return default if default is not None else cls.PRODUCTION
        # Exact match first
        try:
            return cls(v)
        except ValueError:
            pass
        # Alias match
        aliased = _ENV_ALIASES.get(v)
        if aliased is not None:
            try:
                return cls(aliased)
            except ValueError:
                pass
        return default if default is not None else cls.PRODUCTION


class LogLevel(str, Enum):
    """Log levels."""
    TRACE = "TRACE"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class AuthType(str, Enum):
    """Authentication types."""
    NONE = "none"
    TOKEN = "token"
    BEARER = "bearer"
    API_KEY = "api_key"
    JWT = "jwt"
    OAUTH2 = "oauth2"
    BASIC = "basic"
    MULTI = "multi"


class CacheStrategy(str, Enum):
    """Cache strategies."""
    MEMORY = "memory"
    REDIS = "redis"
    MEMCACHED = "memcached"
    NONE = "none"


class ConfigSource(str, Enum):
    """Configuration sources."""
    ENV = "env"
    FILE = "file"
    CONSUL = "consul"
    ETCD = "etcd"
    ZOOKEEPER = "zookeeper"
    DEFAULT = "default"
    RUNTIME = "runtime"


class ConfigStatus(str, Enum):
    """Configuration version status."""
    ACTIVE = "active"
    ROLLED_BACK = "rolled_back"
    ARCHIVED = "archived"


class EncryptionMethod(str, Enum):
    """Encryption methods."""
    NONE = "none"
    BASE64 = "base64"
    AES = "aes"
    FERNET = "fernet"


# =============================================================================
# Encryption Helper (Optional Crypto)
# =============================================================================

class ConfigEncryption:
    """Configuration encryption helper."""

    def __init__(self, method: EncryptionMethod = EncryptionMethod.NONE, key: Optional[str] = None):
        self.method = method
        self.key = key
        self._fernet = None

        if self.method == EncryptionMethod.FERNET and _HAS_CRYPTO and key and Fernet is not None:
            try:
                self._fernet = Fernet(key.encode("utf-8"))
            except Exception:
                self._fernet = None

    def encrypt(self, value: str) -> str:
        """Encrypt a value."""
        if not value:
            return value
        try:
            if self.method == EncryptionMethod.FERNET and self._fernet is not None:
                return self._fernet.encrypt(value.encode("utf-8")).decode("utf-8")
            if self.method == EncryptionMethod.BASE64:
                return base64.b64encode(value.encode("utf-8")).decode("utf-8")
            if self.method == EncryptionMethod.AES:
                return base64.b64encode(zlib.compress(value.encode("utf-8"))).decode("utf-8")
            return value
        except Exception:
            return value

    def decrypt(self, value: str) -> str:
        """Decrypt a value."""
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
# Schema / Pages (Fallback + Safe Import)
# =============================================================================

_HAS_SCHEMA = False
_SCHEMA_VERSION: Optional[str] = None
_SREG: Dict[str, Any] = {}

# NOTE: the _FALLBACK suffix reflects initial values; these globals are
# reassigned in `_safe_import_schema()` if the real modules import cleanly,
# at which point they hold the authoritative live data.
_CANONICAL_PAGES_FALLBACK = [
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
    "Insights_Analysis",
    "Top_10_Investments",
    "Data_Dictionary",
]

_TOP10_FEED_PAGES_FALLBACK = [
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
]

_FORBIDDEN_PAGES_FALLBACK: Set[str] = {"KSA_Tadawul", "Advisor_Criteria"}


def _fallback_normalize_page_name(page: str, allow_output_pages: bool = True) -> str:
    """Fallback page name normalization."""
    p = (page or "").strip()
    if not p:
        raise ValueError("Page name is empty.")
    if p in _FORBIDDEN_PAGES_FALLBACK:
        raise ValueError(f"Page '{p}' is forbidden/removed.")
    if p not in _CANONICAL_PAGES_FALLBACK:
        raise ValueError(f"Unknown page '{p}'. Allowed: {', '.join(sorted(_CANONICAL_PAGES_FALLBACK))}")
    if not allow_output_pages and p in {"Top_10_Investments", "Data_Dictionary"}:
        raise ValueError(f"Page '{p}' is output/meta and not allowed for this operation.")
    return p


def _fallback_get_top10_feed_pages(pages_override: Optional[List[str]] = None) -> List[str]:
    """Fallback Top10 feed pages."""
    base = pages_override or list(_TOP10_FEED_PAGES_FALLBACK)
    result: List[str] = []
    seen: Set[str] = set()
    for p in base:
        try:
            cp = _fallback_normalize_page_name(p, allow_output_pages=False)
        except Exception:
            continue
        if cp not in seen:
            seen.add(cp)
            result.append(cp)
    return result


_normalize_page_name: Callable[..., str] = _fallback_normalize_page_name
_get_top10_feed_pages: Callable[..., List[str]] = _fallback_get_top10_feed_pages


def _try_import_module(module_path: str) -> Optional[Any]:
    """Try to import a module; return None on failure."""
    try:
        import importlib
        return importlib.import_module(module_path)
    except ImportError:
        return None


def _safe_import_schema() -> None:
    """Safely import schema and page catalog modules."""
    global _HAS_SCHEMA, _SCHEMA_VERSION, _SREG
    global _CANONICAL_PAGES_FALLBACK, _TOP10_FEED_PAGES_FALLBACK, _FORBIDDEN_PAGES_FALLBACK
    global _normalize_page_name, _get_top10_feed_pages

    # Try schema_registry
    sreg_mod = None
    for path in ("core.sheets.schema_registry", "core.schema_registry", "schema_registry"):
        sreg_mod = _try_import_module(path)
        if sreg_mod is not None:
            break

    # Try page_catalog
    pcat_mod = None
    for path in ("core.sheets.page_catalog", "core.page_catalog", "page_catalog"):
        pcat_mod = _try_import_module(path)
        if pcat_mod is not None:
            break

    if sreg_mod is None or pcat_mod is None:
        _HAS_SCHEMA = False
        _SREG = {}
        _normalize_page_name = _fallback_normalize_page_name
        _get_top10_feed_pages = _fallback_get_top10_feed_pages
        return

    try:
        _SREG = getattr(sreg_mod, "SCHEMA_REGISTRY", {})
        _SCHEMA_VERSION = getattr(sreg_mod, "SCHEMA_VERSION", None)
        _CANONICAL_PAGES_FALLBACK = list(getattr(pcat_mod, "CANONICAL_PAGES", _CANONICAL_PAGES_FALLBACK))
        _FORBIDDEN_PAGES_FALLBACK = set(getattr(pcat_mod, "FORBIDDEN_PAGES", _FORBIDDEN_PAGES_FALLBACK))
        _TOP10_FEED_PAGES_FALLBACK = list(getattr(pcat_mod, "TOP10_FEED_PAGES_DEFAULT", _TOP10_FEED_PAGES_FALLBACK))

        norm_fn = getattr(pcat_mod, "normalize_page_name", None)
        if callable(norm_fn):
            _normalize_page_name = norm_fn

        t10_fn = getattr(pcat_mod, "get_top10_feed_pages", None)
        if callable(t10_fn):
            _get_top10_feed_pages = t10_fn

        _HAS_SCHEMA = True
    except Exception as e:
        _dbg("Schema import failed", "warning", error=str(e))
        _HAS_SCHEMA = False
        _SREG = {}


_safe_import_schema()


# =============================================================================
# Page + Schema Helpers
# =============================================================================

_OUTPUT_PAGES = {"Top_10_Investments", "Data_Dictionary"}


def canonical_pages() -> List[str]:
    """Get list of canonical pages."""
    return list(_CANONICAL_PAGES_FALLBACK)


def forbidden_pages() -> Set[str]:
    """Get set of forbidden pages."""
    return set(_FORBIDDEN_PAGES_FALLBACK)


def is_output_page(page: str) -> bool:
    """Check if page is an output page."""
    try:
        return strip_value(page) in _OUTPUT_PAGES
    except Exception:
        return False


def normalize_page(page: str, allow_output_pages: bool = True) -> str:
    """Normalize page name."""
    return _normalize_page_name(page, allow_output_pages=allow_output_pages)


def top10_feed_pages_default() -> List[str]:
    """Get default Top10 feed pages."""
    return _get_top10_feed_pages(None)


def schema_available() -> bool:
    """Check if schema is available."""
    return bool(_HAS_SCHEMA and isinstance(_SREG, dict) and _SREG)


def schema_known(sheet: str) -> bool:
    """Check if sheet is known in schema."""
    try:
        return strip_value(sheet) in _SREG
    except Exception:
        return False


def schema_headers(sheet: str) -> List[str]:
    """Get headers for a sheet."""
    try:
        spec = _SREG.get(strip_value(sheet))
        if not spec:
            return []
        if hasattr(spec, "columns"):
            return [getattr(c, "header", "") for c in spec.columns]
        return []
    except Exception:
        return []


def schema_keys(sheet: str) -> List[str]:
    """Get keys for a sheet."""
    try:
        spec = _SREG.get(strip_value(sheet))
        if not spec:
            return []
        if hasattr(spec, "columns"):
            return [getattr(c, "key", "") for c in spec.columns]
        return []
    except Exception:
        return []


def schema_columns(sheet: str) -> List[Dict[str, Any]]:
    """Get column specifications for a sheet."""
    try:
        spec = _SREG.get(strip_value(sheet))
        if not spec:
            return []
        if hasattr(spec, "columns"):
            result: List[Dict[str, Any]] = []
            for c in spec.columns:
                result.append({
                    "group": getattr(c, "group", None),
                    "header": getattr(c, "header", None),
                    "key": getattr(c, "key", None),
                    "dtype": getattr(c, "dtype", None),
                    "fmt": getattr(c, "fmt", None),
                    "required": bool(getattr(c, "required", False)),
                    "source": getattr(c, "source", None),
                    "notes": getattr(c, "notes", None),
                })
            return result
        return []
    except Exception:
        return []


def schema_row_template(sheet: str, fill_value: Any = None) -> Dict[str, Any]:
    """Get row template for a sheet."""
    try:
        return {k: fill_value for k in schema_keys(sheet)}
    except Exception:
        return {}


def normalize_row_to_schema(
    sheet: str,
    row: Dict[str, Any],
    fill_missing_with_null: bool = True,
) -> Dict[str, Any]:
    """Normalize row to schema."""
    try:
        sheet = strip_value(sheet)
        row = row or {}
        keys = schema_keys(sheet)
        if not keys:
            return dict(row)
        fill = None if fill_missing_with_null else ""
        result: Dict[str, Any] = {}
        for k in keys:
            result[k] = row.get(k, fill)
        for k, v in row.items():
            if k not in result:
                result[k] = v
        return result
    except Exception:
        return dict(row or {})


# =============================================================================
# Path Normalization for Auth
# =============================================================================

def _clean_paths(values: Sequence[str]) -> List[str]:
    """Clean and deduplicate path strings."""
    result: List[str] = []
    seen: Set[str] = set()
    for v in values:
        p = strip_value(v)
        if not p:
            continue
        if not p.startswith("/"):
            p = "/" + p
        p = re.sub(r"/{2,}", "/", p).rstrip("/") or "/"
        if p not in seen:
            seen.add(p)
            result.append(p)
    return result


def normalize_request_path(path: Optional[str]) -> str:
    """Normalize request path for auth matching."""
    p = strip_value(path)
    if not p:
        return ""
    p = p.split("?", 1)[0].strip()
    if not p.startswith("/"):
        p = "/" + p
    return re.sub(r"/{2,}", "/", p).rstrip("/") or "/"


# =============================================================================
# Version Management
# =============================================================================

@dataclass(slots=True)
class ConfigVersion:
    """Configuration version record."""
    version_id: str
    timestamp_utc: str
    source: ConfigSource
    changes: Dict[str, Tuple[Any, Any]]
    author: Optional[str] = None
    comment: Optional[str] = None
    status: ConfigStatus = ConfigStatus.ACTIVE

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
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
    """Manages configuration version history."""

    def __init__(self, max_versions: int = 100):
        self.max_versions = max(10, int(max_versions))
        self._lock = RLock()
        self._versions: List[ConfigVersion] = []

    def add(self, version: ConfigVersion) -> None:
        """Add a version to history."""
        with self._lock:
            self._versions.append(version)
            if len(self._versions) > self.max_versions:
                self._versions = self._versions[-self.max_versions:]

    def history(self, limit: int = 20) -> List[ConfigVersion]:
        """Get version history."""
        with self._lock:
            return list(reversed(self._versions[-max(1, int(limit)):]))


_VERSION_MANAGER = ConfigVersionManager(max_versions=100)


def get_version_manager() -> ConfigVersionManager:
    """Get the global version manager."""
    return _VERSION_MANAGER


def save_config_version(
    changes: Dict[str, Tuple[Any, Any]],
    author: Optional[str] = None,
    comment: Optional[str] = None,
    source: ConfigSource = ConfigSource.RUNTIME,
) -> None:
    """Save a configuration version."""
    try:
        version = ConfigVersion(
            version_id=str(uuid.uuid4()),
            timestamp_utc=datetime.now(timezone.utc).isoformat(),
            source=source,
            changes=changes or {},
            author=author,
            comment=comment,
            status=ConfigStatus.ACTIVE,
        )
        _VERSION_MANAGER.add(version)
    except Exception:
        pass


# =============================================================================
# Google Credentials Normalization
# =============================================================================

def _maybe_b64_decode(s: str) -> Optional[str]:
    """Attempt base64 decode."""
    s2 = strip_value(s)
    if not s2:
        return None
    if s2.lower().startswith("b64:"):
        s2 = s2.split(":", 1)[1].strip()
    try:
        return base64.b64decode(s2.encode("utf-8")).decode("utf-8", errors="replace")
    except Exception:
        return None


def _read_text_file_if_exists(filepath: str) -> Optional[str]:
    """Read text file if it exists."""
    try:
        path = Path(strip_value(filepath))
        if path.exists() and path.is_file():
            return path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None
    return None


def _parse_credentials_json(raw: str, source_label: str) -> Optional[Tuple[str, Dict[str, Any]]]:
    """Parse a JSON-looking credentials string.

    v7.0.0 helper: returns (raw, dict) only on a successful parse to a
    dict. On any parse failure, returns None and logs — callers continue
    to the next credential source rather than receiving a malformed raw
    string (which v6 did).
    """
    s = strip_value(raw)
    if not (s.startswith("{") and s.endswith("}")):
        return None
    try:
        d = _json_loads(s)
    except Exception as e:
        _dbg(
            "normalize_google_credentials: JSON parse failed",
            "warning",
            source=source_label,
            error=str(e),
        )
        return None
    if isinstance(d, dict):
        return (s, d)
    _dbg(
        "normalize_google_credentials: JSON is not a dict",
        "warning",
        source=source_label,
        parsed_type=type(d).__name__,
    )
    return None


def normalize_google_credentials() -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """
    Normalize Google credentials from various sources.

    Probes, in order:
      1. ``GOOGLE_CREDENTIALS_DICT`` (JSON inline).
      2. ``GOOGLE_SHEETS_CREDENTIALS`` / ``GOOGLE_CREDENTIALS`` (JSON inline).
      3. ``GOOGLE_SHEETS_CREDENTIALS_B64`` / ``GOOGLE_CREDENTIALS_B64``
         (base64-encoded JSON).
      4. ``GOOGLE_APPLICATION_CREDENTIALS`` / ``GOOGLE_SHEETS_CREDENTIALS_FILE``
         / ``GOOGLE_CREDENTIALS_FILE`` (path to JSON file).

    v7.0.0: if a JSON-looking value fails to parse, the function now logs
    a warning and continues to the next source. Previously it returned a
    malformed raw string paired with ``None`` as the dict — poisoning
    downstream callers AND short-circuiting the remaining sources.

    Returns:
        Tuple of (credentials_json_string, credentials_dict). Both are
        ``None`` when no source yields valid credentials.
    """
    # 1) GOOGLE_CREDENTIALS_DICT
    parsed = _parse_credentials_json(
        os.getenv("GOOGLE_CREDENTIALS_DICT") or "",
        "GOOGLE_CREDENTIALS_DICT",
    )
    if parsed is not None:
        # Re-encode via _json_dumps to normalize formatting.
        _raw, d = parsed
        try:
            return (_json_dumps(d), d)
        except Exception:
            return parsed

    # 2) GOOGLE_SHEETS_CREDENTIALS / GOOGLE_CREDENTIALS (direct JSON)
    for env_name in ("GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS"):
        raw = os.getenv(env_name) or ""
        if raw.strip():
            parsed = _parse_credentials_json(raw, env_name)
            if parsed is not None:
                return parsed
            # else: malformed; try next source

    # 3) Base64-encoded JSON
    for env_name in ("GOOGLE_SHEETS_CREDENTIALS_B64", "GOOGLE_CREDENTIALS_B64"):
        b64 = strip_value(os.getenv(env_name) or "")
        if not b64:
            continue
        decoded = _maybe_b64_decode(b64)
        if decoded is None:
            _dbg(
                "normalize_google_credentials: base64 decode failed",
                "warning",
                source=env_name,
            )
            continue
        parsed = _parse_credentials_json(decoded, f"{env_name} (b64-decoded)")
        if parsed is not None:
            return parsed
        # else: decoded but not valid JSON — try next source

    # 4) File path
    filepath = strip_value(
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        or os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
        or os.getenv("GOOGLE_CREDENTIALS_FILE")
        or ""
    )
    if filepath:
        content = _read_text_file_if_exists(filepath)
        if content is not None:
            parsed = _parse_credentials_json(content, f"file:{filepath}")
            if parsed is not None:
                return parsed

    return (None, None)


# Alias for backward compatibility
resolve_google_credentials = normalize_google_credentials


# =============================================================================
# Settings Class
# =============================================================================

@dataclass(frozen=True)
class Settings:
    """Application configuration settings."""

    # Meta
    config_version: str = CONFIG_VERSION
    build_timestamp: str = CONFIG_BUILD_TIMESTAMP

    # Schema
    schema_enabled: bool = True
    schema_version: str = (_SCHEMA_VERSION or "unknown")
    schema_headers_always: bool = True
    schema_fill_missing_with_null: bool = True

    # Pages / Catalog
    allowed_pages: List[str] = field(default_factory=lambda: canonical_pages())
    forbidden_pages: List[str] = field(default_factory=lambda: sorted(list(forbidden_pages())))
    default_data_pages: List[str] = field(default_factory=lambda: [
        "Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds", "My_Portfolio",
    ])
    top10_feed_pages: List[str] = field(default_factory=lambda: top10_feed_pages_default())

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
    public_exact_paths: List[str] = field(default_factory=list)
    public_path_prefixes: List[str] = field(default_factory=list)
    protected_exact_paths: List[str] = field(default_factory=list)
    protected_path_prefixes: List[str] = field(default_factory=list)

    # Backend
    backend_base_url: str = "http://localhost:8000"

    # Providers
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
    eodhd_append_exchange_suffix: bool = True

    # Market feature flags
    tadawul_market_enabled: bool = True
    ksa_disallow_eodhd: bool = False

    # Computation feature flags
    computations_enabled: bool = True
    fundamentals_enabled: bool = True
    technicals_enabled: bool = True
    forecasting_enabled: bool = True
    scoring_enabled: bool = True

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
        """Create settings from environment variables."""
        # Environment
        env_name = strip_value(os.getenv("APP_ENV") or os.getenv("TFB_ENV") or "production")
        app_name = strip_value(os.getenv("APP_NAME") or "TFB")
        app_version = strip_value(os.getenv("APP_VERSION") or __version__)

        # Auth
        token = strip_value(os.getenv("APP_TOKEN") or os.getenv("TFB_APP_TOKEN") or os.getenv("BACKEND_TOKEN") or "")
        backup_token = strip_value(os.getenv("BACKUP_APP_TOKEN") or "")
        require_auth = coerce_bool(os.getenv("REQUIRE_AUTH"), True)

        # v7.0.0: read OPEN_MODE only once.
        open_mode_raw = os.getenv("OPEN_MODE")
        if open_mode_raw is not None:
            open_mode = coerce_bool(open_mode_raw, False)
        else:
            open_mode = (not require_auth) and (not bool(token or backup_token))

        # Provider keys
        eodhd_key = strip_value(os.getenv("EODHD_API_TOKEN") or os.getenv("EODHD_API_KEY") or os.getenv("EODHD_KEY") or "")
        finnhub_key = strip_value(os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_KEY") or "")
        fmp_key = strip_value(os.getenv("FMP_API_KEY") or "")
        alpha_key = strip_value(os.getenv("ALPHA_VANTAGE_API_KEY") or os.getenv("ALPHAVANTAGE_API_KEY") or "")
        marketstack_key = strip_value(os.getenv("MARKETSTACK_API_KEY") or "")
        twelvedata_key = strip_value(os.getenv("TWELVEDATA_API_KEY") or "")

        # Provider URLs
        eodhd_base_url = strip_value(os.getenv("EODHD_BASE_URL") or "https://eodhd.com/api")
        fmp_base_url = strip_value(os.getenv("FMP_BASE_URL") or "") or None

        # EODHD defaults
        eodhd_suffix_default = strip_value(os.getenv("EODHD_DEFAULT_EXCHANGE") or os.getenv("EODHD_SYMBOL_SUFFIX_DEFAULT") or "US")
        eodhd_append_suffix = coerce_bool(os.getenv("EODHD_APPEND_EXCHANGE_SUFFIX"), True)

        # Enabled providers
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

        # Logging — v7.0.0: direct member lookup instead of linear scan
        log_level_str = strip_value(os.getenv("LOG_LEVEL") or "INFO").upper()
        log_level = LogLevel.__members__.get(log_level_str, LogLevel.INFO)
        log_format = strip_value(os.getenv("LOG_FORMAT") or "json").lower()
        log_json = coerce_bool(os.getenv("LOG_JSON"), True)

        # Backend
        backend_base_url = strip_value(os.getenv("BACKEND_BASE_URL") or os.getenv("DEFAULT_BACKEND_URL") or "http://localhost:8000")

        # Feature flags
        advanced_analysis_enabled = coerce_bool(os.getenv("ADVANCED_ANALYSIS_ENABLED"), True)
        ai_analysis_enabled = coerce_bool(os.getenv("AI_ANALYSIS_ENABLED"), True)
        tadawul_market_enabled = coerce_bool(os.getenv("TADAWUL_MARKET_ENABLED"), True)
        ksa_disallow_eodhd = coerce_bool(os.getenv("KSA_DISALLOW_EODHD"), False)
        computations_enabled = coerce_bool(os.getenv("COMPUTATIONS_ENABLED"), True)
        fundamentals_enabled = coerce_bool(os.getenv("FUNDAMENTALS_ENABLED"), True)
        technicals_enabled = coerce_bool(os.getenv("TECHNICALS_ENABLED"), True)
        forecasting_enabled = coerce_bool(os.getenv("FORECASTING_ENABLED"), True)
        scoring_enabled = coerce_bool(os.getenv("SCORING_ENABLED"), True)
        schema_enabled = coerce_bool(os.getenv("SCHEMA_ENABLED"), True)
        schema_headers_always = coerce_bool(os.getenv("SCHEMA_HEADERS_ALWAYS"), True)
        schema_fill_missing_with_null = coerce_bool(os.getenv("SCHEMA_FILL_MISSING_WITH_NULL"), True)

        # Page lists
        allowed_pages_env = coerce_list(os.getenv("ALLOWED_PAGES"))
        if allowed_pages_env:
            allowed = []
            for p in allowed_pages_env:
                try:
                    allowed.append(normalize_page(p, allow_output_pages=True))
                except Exception:
                    continue
            allowed_pages = allowed or canonical_pages()
        else:
            allowed_pages = canonical_pages()

        default_data_pages_env = coerce_list(os.getenv("DEFAULT_DATA_PAGES"))
        if default_data_pages_env:
            ddp = []
            for p in default_data_pages_env:
                try:
                    ddp.append(normalize_page(p, allow_output_pages=False))
                except Exception:
                    continue
            default_data_pages = ddp or ["Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds", "My_Portfolio"]
        else:
            default_data_pages = ["Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds", "My_Portfolio"]

        top10_feed_pages_env = coerce_list(os.getenv("TOP10_FEED_PAGES"))
        if top10_feed_pages_env:
            top10_feed_pages = _get_top10_feed_pages(top10_feed_pages_env)
        else:
            top10_feed_pages = top10_feed_pages_default()

        # HTTP settings
        http_timeout_sec = coerce_float(os.getenv("HTTP_TIMEOUT_SEC"), 45.0, lo=5.0, hi=300.0)
        max_retries = coerce_int(os.getenv("MAX_RETRIES"), 2, lo=0, hi=20)
        retry_delay_sec = coerce_float(os.getenv("RETRY_DELAY") or os.getenv("RETRY_DELAY_SEC"), 1.0, lo=0.0, hi=30.0)

        # Cache settings
        cache_ttl_sec = coerce_int(os.getenv("CACHE_TTL_SEC") or os.getenv("CACHE_DEFAULT_TTL"), 20, lo=1, hi=3600)
        engine_cache_ttl_sec = coerce_int(os.getenv("ENGINE_CACHE_TTL_SEC"), 60, lo=1, hi=86400)
        cache_max_size = coerce_int(os.getenv("CACHE_MAX_SIZE"), 2048, lo=128, hi=200000)

        # Concurrency (PRESERVED: reads ONLY BATCH_CONCURRENCY, never WEB_CONCURRENCY)
        batch_concurrency = coerce_int(os.getenv("BATCH_CONCURRENCY"), 5, lo=1, hi=50)
        ai_batch_size = coerce_int(os.getenv("AI_BATCH_SIZE"), 20, lo=1, hi=500)
        quote_batch_size = coerce_int(os.getenv("QUOTE_BATCH_SIZE") or os.getenv("TADAWUL_MAX_SYMBOLS"), 50, lo=1, hi=500)

        # Engine lifecycle
        init_engine_on_boot = coerce_bool(os.getenv("INIT_ENGINE_ON_BOOT"), True)
        defer_router_mount = coerce_bool(os.getenv("DEFER_ROUTER_MOUNT"), False)

        # Auth paths
        public_exact_paths = _clean_paths(coerce_list(os.getenv("PUBLIC_EXACT_PATHS"), []))
        public_path_prefixes = _clean_paths(coerce_list(os.getenv("PUBLIC_PATH_PREFIXES"), []))
        protected_exact_paths = _clean_paths(coerce_list(os.getenv("PROTECTED_EXACT_PATHS"), []))
        protected_path_prefixes = _clean_paths(coerce_list(os.getenv("PROTECTED_PATH_PREFIXES"), []))

        # Google Sheets
        default_spreadsheet_id = strip_value(os.getenv("DEFAULT_SPREADSHEET_ID") or "") or None
        gs_json, gs_dict = normalize_google_credentials()
        google_apps_script_url = strip_value(os.getenv("GOOGLE_APPS_SCRIPT_URL") or "") or None
        google_apps_script_backup_url = strip_value(os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL") or "") or None
        timezone_str = strip_value(os.getenv("TZ") or "Asia/Riyadh")

        # Deep copy to prevent external mutation
        safe_gs_dict = copy.deepcopy(gs_dict) if isinstance(gs_dict, dict) else gs_dict

        return cls(
            schema_enabled=schema_enabled,
            schema_version=(_SCHEMA_VERSION or "unknown"),
            schema_headers_always=schema_headers_always,
            schema_fill_missing_with_null=schema_fill_missing_with_null,
            allowed_pages=allowed_pages,
            forbidden_pages=sorted(list(forbidden_pages())),
            default_data_pages=default_data_pages,
            top10_feed_pages=top10_feed_pages,
            environment=Environment.from_string(env_name),
            service_name=strip_value(os.getenv("SERVICE_NAME") or "Tadawul Fast Bridge"),
            timezone=timezone_str,
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
            public_exact_paths=public_exact_paths,
            public_path_prefixes=public_path_prefixes,
            protected_exact_paths=protected_exact_paths,
            protected_path_prefixes=protected_path_prefixes,
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
            eodhd_symbol_suffix_default=eodhd_suffix_default,
            eodhd_append_exchange_suffix=eodhd_append_suffix,
            tadawul_market_enabled=tadawul_market_enabled,
            ksa_disallow_eodhd=ksa_disallow_eodhd,
            computations_enabled=computations_enabled,
            fundamentals_enabled=fundamentals_enabled,
            technicals_enabled=technicals_enabled,
            forecasting_enabled=forecasting_enabled,
            scoring_enabled=scoring_enabled,
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
            google_credentials_dict=safe_gs_dict,
            google_apps_script_url=google_apps_script_url,
            google_apps_script_backup_url=google_apps_script_backup_url,
        )

    def validate(self) -> Tuple[List[str], List[str]]:
        """Validate settings and return (errors, warnings)."""
        errors: List[str] = []
        warnings: List[str] = []

        # Forbidden pages check
        for fp in self.forbidden_pages or []:
            if fp in (self.allowed_pages or []):
                errors.append(f"Forbidden page '{fp}' appears in allowed_pages. Remove it.")

        # Default data pages check
        for p in self.default_data_pages or []:
            if p not in (self.allowed_pages or []):
                warnings.append(f"default_data_pages contains '{p}' which is not in allowed_pages.")

        # Top10 feed pages check
        for p in self.top10_feed_pages or []:
            if p not in (self.allowed_pages or []):
                warnings.append(f"top10_feed_pages contains '{p}' which is not in allowed_pages.")

        # Backend URL
        if self.backend_base_url and not is_valid_url(self.backend_base_url):
            warnings.append(f"backend_base_url does not look like a URL: {self.backend_base_url!r}")

        # Provider validation
        enabled_lower = [p.lower() for p in (self.enabled_providers or [])]
        if "eodhd" in enabled_lower and not self.eodhd_api_key:
            errors.append("Provider 'eodhd' is enabled but EODHD_API_KEY is missing.")

        if self.primary_provider and self.primary_provider.lower() not in enabled_lower:
            warnings.append(
                f"PRIMARY_PROVIDER={self.primary_provider!r} not included in "
                f"ENABLED_PROVIDERS={self.enabled_providers!r}."
            )

        # Auth validation
        if self.require_auth and not self.open_mode:
            if not (self.app_token or self.backup_app_token or allowed_tokens()):
                errors.append(
                    "REQUIRE_AUTH=true but no APP_TOKEN/BACKUP_APP_TOKEN/ALLOWED_TOKENS "
                    "configured (and OPEN_MODE=false)."
                )

        if self.open_mode and (self.app_token or self.backup_app_token):
            warnings.append(
                "OPEN_MODE=true while tokens exist. Service may be publicly exposed unintentionally."
            )

        # Schema validation
        if self.schema_enabled and not self.schema_headers_always:
            warnings.append(
                "SCHEMA_ENABLED=true but SCHEMA_HEADERS_ALWAYS is not true. "
                "This can break column guarantees."
            )

        if self.schema_enabled and not schema_available():
            warnings.append(
                "Schema modules not available/importable yet. Falling back to static page list."
            )

        # Path overlap check
        if self.public_exact_paths and self.protected_exact_paths:
            overlap = set(self.public_exact_paths) & set(self.protected_exact_paths)
            if overlap:
                warnings.append(
                    f"Paths appear in both public_exact_paths and protected_exact_paths: {sorted(overlap)}"
                )

        return errors, warnings

    def to_dict(self) -> Dict[str, Any]:
        """Convert settings to dictionary."""
        return asdict(self)

    @classmethod
    def from_file(cls, filepath: str) -> "Settings":
        """Create settings from file (JSON/YAML/TOML)."""
        path = Path(filepath)
        content = load_file_content(path)
        base = cls.from_env()
        merged = deep_merge(asdict(base), content, overwrite=True)
        allowed = set(cls.__dataclass_fields__.keys())
        cleaned = {k: v for k, v in merged.items() if k in allowed}
        return cls(**cleaned)


# =============================================================================
# Settings Cache
# =============================================================================

class SettingsCache:
    """Thread-safe settings cache with TTL."""

    def __init__(self, ttl_seconds: int = 30):
        self._ttl = max(1, int(ttl_seconds))
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._lock = RLock()

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        with self._lock:
            item = self._cache.get(key)
            if not item:
                return None
            value, expires_at = item
            if time.time() < expires_at:
                return value
            self._cache.pop(key, None)
            return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache."""
        with self._lock:
            t = self._ttl if ttl is None else max(1, int(ttl))
            self._cache[key] = (value, time.time() + t)

    def clear(self) -> None:
        """Clear cache."""
        with self._lock:
            self._cache.clear()

    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            return {"size": len(self._cache), "ttl": self._ttl}


_SETTINGS_CACHE_TTL = coerce_int(os.getenv("CORE_SETTINGS_CACHE_TTL"), 30, lo=1, hi=3600)
_SETTINGS_CACHE = SettingsCache(ttl_seconds=_SETTINGS_CACHE_TTL)


def get_settings() -> Settings:
    """Get fresh settings from environment."""
    return Settings.from_env()


def get_settings_cached(force_reload: bool = False) -> Settings:
    """Get cached settings."""
    with TraceContext("get_settings_cached"):
        if force_reload:
            _SETTINGS_CACHE.clear()
        cached = _SETTINGS_CACHE.get("settings")
        if cached is not None:
            return cached
        settings = get_settings()
        _SETTINGS_CACHE.set("settings", settings)
        return settings


# =============================================================================
# Distributed Config Sources
# =============================================================================

class ConfigSourceManager:
    """Manages distributed configuration sources."""

    def __init__(self):
        self.sources: List[Tuple[ConfigSource, Callable[[], Optional[Dict[str, Any]]]]] = []
        self._lock = RLock()

    def register_source(self, source: ConfigSource, loader: Callable[[], Optional[Dict[str, Any]]]) -> None:
        """Register a configuration source."""
        with self._lock:
            self.sources.append((source, loader))

    def load_merged(self) -> Dict[str, Any]:
        """Load and merge all registered sources."""
        merged: Dict[str, Any] = {}
        with self._lock:
            for source, loader in self.sources:
                try:
                    data = loader()
                    if isinstance(data, dict) and data:
                        merged = deep_merge(merged, data, overwrite=True)
                except Exception as e:
                    _dbg("distributed source load failed", "warning", source=source.value, error=str(e))
        return merged


_CONFIG_SOURCES = ConfigSourceManager()
_DISTRIBUTED_INIT_DONE = False
_DISTRIBUTED_INIT_LOCK = RLock()


def _distributed_enabled() -> bool:
    """Check if distributed config is enabled."""
    if os.getenv("DISTRIBUTED_CONFIG_ENABLED") is not None:
        return coerce_bool(os.getenv("DISTRIBUTED_CONFIG_ENABLED"), False)
    return bool(os.getenv("CONSUL_HOST") or os.getenv("ETCD_HOST") or os.getenv("ZOOKEEPER_HOSTS"))


class ConsulConfigSource:
    """Consul configuration source."""

    def __init__(self, host: str, port: int, token: Optional[str], prefix: str):
        self.host = host
        self.port = port
        self.token = token
        self.prefix = prefix

    def load(self) -> Optional[Dict[str, Any]]:
        """Load configuration from Consul."""
        if not (_CONSUL_AVAILABLE and consul is not None):
            return None
        try:
            client = consul.Consul(host=self.host, port=self.port, token=self.token)
            _, data = client.kv.get(self.prefix, recurse=True)
            if not data:
                return None
            result: Dict[str, Any] = {}
            for item in data:
                key = item.get("Key") or ""
                raw = item.get("Value")
                if not key or raw is None:
                    continue
                rel = key[len(self.prefix) + 1:] if key.startswith(self.prefix) else key
                try:
                    text = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
                    try:
                        value = _json_loads(text)
                    except Exception:
                        value = text
                except Exception:
                    continue
                parts = [p for p in rel.split("/") if p]
                cur = result
                for part in parts[:-1]:
                    cur.setdefault(part, {})
                    if not isinstance(cur[part], dict):
                        cur[part] = {}
                    cur = cur[part]
                if parts:
                    cur[parts[-1]] = value
            return result or None
        except Exception as e:
            _dbg("consul load failed", "warning", error=str(e))
            return None


class EtcdConfigSource:
    """Etcd configuration source."""

    def __init__(self, host: str, port: int, prefix: str):
        self.host = host
        self.port = port
        self.prefix = prefix

    def load(self) -> Optional[Dict[str, Any]]:
        """Load configuration from Etcd."""
        if not (_ETCD_AVAILABLE and etcd3 is not None):
            return None
        try:
            client = etcd3.client(host=self.host, port=self.port)
            result: Dict[str, Any] = {}
            for value, meta in client.get_prefix(self.prefix):
                key = (meta.key or b"").decode("utf-8", errors="replace")
                rel = key[len(self.prefix):].lstrip("/")
                try:
                    text = value.decode("utf-8", errors="replace") if isinstance(value, (bytes, bytearray)) else str(value)
                    try:
                        val = _json_loads(text)
                    except Exception:
                        val = text
                except Exception:
                    continue
                parts = [p for p in rel.split("/") if p]
                cur = result
                for part in parts[:-1]:
                    cur.setdefault(part, {})
                    cur = cur[part]
                if parts:
                    cur[parts[-1]] = val
            return result or None
        except Exception as e:
            _dbg("etcd load failed", "warning", error=str(e))
            return None


class ZooKeeperConfigSource:
    """ZooKeeper configuration source."""

    def __init__(self, hosts: str, prefix: str, start_timeout_sec: int = 3):
        self.hosts = hosts
        self.prefix = prefix
        self.start_timeout_sec = max(1, int(start_timeout_sec))

    def load(self) -> Optional[Dict[str, Any]]:
        """Load configuration from ZooKeeper."""
        if not (_ZOOKEEPER_AVAILABLE and KazooClient is not None):
            return None
        client = None
        try:
            client = KazooClient(hosts=self.hosts)
            client.start(timeout=self.start_timeout_sec)
            result: Dict[str, Any] = {}

            def walk(path: str, cur: Dict[str, Any]) -> None:
                if not client.exists(path):
                    return
                for child in client.get_children(path):
                    child_path = f"{path}/{child}"
                    data, _ = client.get(child_path)
                    if client.get_children(child_path):
                        cur.setdefault(child, {})
                        if isinstance(cur[child], dict):
                            walk(child_path, cur[child])
                    else:
                        if data:
                            raw = data.decode("utf-8", errors="replace")
                            try:
                                cur[child] = _json_loads(raw)
                            except Exception:
                                cur[child] = raw

            walk(self.prefix, result)
            return result or None
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
    """Initialize distributed configuration sources."""
    global _DISTRIBUTED_INIT_DONE
    with _DISTRIBUTED_INIT_LOCK:
        if _DISTRIBUTED_INIT_DONE:
            return
        _DISTRIBUTED_INIT_DONE = True

        if not _distributed_enabled():
            return

        with TraceContext("init_distributed_config"):
            # Consul
            if os.getenv("CONSUL_HOST"):
                consul_source = ConsulConfigSource(
                    host=strip_value(os.getenv("CONSUL_HOST") or "localhost"),
                    port=coerce_int(os.getenv("CONSUL_PORT"), 8500, lo=1, hi=65535),
                    token=strip_value(os.getenv("CONSUL_TOKEN")) or None,
                    prefix=strip_value(os.getenv("CONSUL_PREFIX") or "config/tadawul"),
                )
                _CONFIG_SOURCES.register_source(ConfigSource.CONSUL, consul_source.load)

            # Etcd
            if os.getenv("ETCD_HOST"):
                etcd_source = EtcdConfigSource(
                    host=strip_value(os.getenv("ETCD_HOST") or "localhost"),
                    port=coerce_int(os.getenv("ETCD_PORT"), 2379, lo=1, hi=65535),
                    prefix=strip_value(os.getenv("ETCD_PREFIX") or "/config/tadawul"),
                )
                _CONFIG_SOURCES.register_source(ConfigSource.ETCD, etcd_source.load)

            # ZooKeeper
            if os.getenv("ZOOKEEPER_HOSTS"):
                zk_source = ZooKeeperConfigSource(
                    hosts=strip_value(os.getenv("ZOOKEEPER_HOSTS") or "localhost:2181"),
                    prefix=strip_value(os.getenv("ZOOKEEPER_PREFIX") or "/config/tadawul"),
                    start_timeout_sec=coerce_int(os.getenv("ZOOKEEPER_START_TIMEOUT_SEC"), 3, lo=1, hi=10),
                )
                _CONFIG_SOURCES.register_source(ConfigSource.ZOOKEEPER, zk_source.load)


def reload_settings() -> Settings:
    """Reload settings from all sources."""
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


# =============================================================================
# Auth Helpers
# =============================================================================

def allowed_tokens() -> List[str]:
    """Get list of allowed authentication tokens."""
    tokens = coerce_list(os.getenv("ALLOWED_TOKENS") or os.getenv("TFB_ALLOWED_TOKENS") or os.getenv("APP_TOKENS"))
    tokens = [t for t in tokens if t]
    if tokens:
        return tokens
    t1 = strip_value(os.getenv("APP_TOKEN") or os.getenv("TFB_APP_TOKEN") or os.getenv("BACKEND_TOKEN") or "")
    t2 = strip_value(os.getenv("BACKUP_APP_TOKEN") or "")
    result: List[str] = []
    if t1:
        result.append(t1)
    if t2 and t2 not in result:
        result.append(t2)
    return result


def is_open_mode() -> bool:
    """Check if open mode is enabled (no authentication required)."""
    env_flag = os.getenv("OPEN_MODE")
    if env_flag is not None:
        return coerce_bool(env_flag, False)
    require_auth = coerce_bool(os.getenv("REQUIRE_AUTH"), True)
    return (not require_auth) and (len(allowed_tokens()) == 0)


def _norm_headers(headers: Any) -> Dict[str, str]:
    """Normalize headers to lowercase dict."""
    result: Dict[str, str] = {}
    try:
        if headers is None:
            return result
        if isinstance(headers, dict):
            for k, v in headers.items():
                result[str(k).lower()] = strip_value(v)
            return result
        if hasattr(headers, "items"):
            for k, v in headers.items():
                result[str(k).lower()] = strip_value(v)
    except Exception:
        pass
    return result


def _extract_bearer(authorization: Optional[str]) -> Optional[str]:
    """Extract Bearer token from Authorization header."""
    auth = strip_value(authorization)
    if not auth:
        return None
    if auth.lower().startswith("bearer "):
        return strip_value(auth.split(" ", 1)[1])
    return auth


def is_public_path(path: Optional[str], settings: Optional[Settings] = None) -> bool:
    """Check if path is public (no authentication required)."""
    s = settings or get_settings_cached()
    p = normalize_request_path(path)
    if not p:
        return False

    # Check protected exact paths
    protected_exact = set(_clean_paths(s.protected_exact_paths or []))
    if p in protected_exact:
        return False

    # Check protected path prefixes
    for prefix in _clean_paths(s.protected_path_prefixes or []):
        if prefix != "/" and p.startswith(prefix):
            return False

    # Check public exact paths
    public_exact = set(_clean_paths(s.public_exact_paths or []))
    if p in public_exact:
        return True

    # Check public path prefixes
    for prefix in _clean_paths(s.public_path_prefixes or []):
        if prefix == "/" or p.startswith(prefix):
            return True

    return False


def auth_ok(
    token: Optional[str] = None,
    authorization: Optional[str] = None,
    headers: Optional[Any] = None,
    api_key: Optional[str] = None,
    path: Optional[str] = None,
    request: Optional[Any] = None,
    settings: Optional[Settings] = None,
    **_: Any,
) -> bool:
    """
    Check if request is authenticated.

    Args:
        token: Direct token value
        authorization: Authorization header value
        headers: Request headers dict
        api_key: API key value
        path: Request path
        request: Request object (FastAPI/Starlette)
        settings: Settings instance

    Returns:
        True if authenticated, False otherwise
    """
    s = settings or get_settings_cached()

    # Extract path and headers from request if provided
    req_path = path
    req_headers = headers
    if request is not None:
        try:
            if not req_path:
                req_path = (
                    getattr(getattr(request, "url", None), "path", None)
                    or getattr(request, "scope", {}).get("path")
                )
        except Exception:
            pass
        try:
            if req_headers is None:
                req_headers = getattr(request, "headers", None)
        except Exception:
            pass

    # Check open mode
    if is_open_mode():
        return True
    if not s.require_auth:
        return True

    # Check public path
    if is_public_path(req_path, settings=s):
        return True

    # Check tokens
    allowed = set(allowed_tokens())
    if not allowed:
        return False

    if token and token in allowed:
        return True

    # Check Authorization header
    headers_norm = _norm_headers(req_headers)
    bearer = _extract_bearer(authorization or headers_norm.get("authorization"))
    if bearer and bearer in allowed:
        return True

    # Check app token header
    configured_header = strip_value(s.auth_header_name or "X-APP-TOKEN").lower().replace("_", "-")
    configured_header_alt = configured_header.replace("-", "_")
    app_token = (
        headers_norm.get(configured_header)
        or headers_norm.get(configured_header_alt)
        or headers_norm.get("x-app-token")
        or headers_norm.get("x_app_token")
    )
    if app_token and app_token in allowed:
        return True

    # Check API key
    api_key_header = headers_norm.get("x-api-key") or headers_norm.get("x_api_key") or headers_norm.get("apikey")
    if api_key_header and api_key_header in allowed:
        return True
    if api_key and api_key in allowed:
        return True

    return False


# =============================================================================
# Health Check
# =============================================================================

def config_health_check() -> Dict[str, Any]:
    """Run configuration health check."""
    health: Dict[str, Any] = {
        "status": "healthy",
        "checks": {},
        "warnings": [],
        "errors": [],
    }

    try:
        init_distributed_config()
        s = get_settings_cached()
        health["checks"]["settings_accessible"] = True

        errors, warnings = s.validate()
        if errors:
            health["status"] = "degraded"
            health["checks"]["settings_valid"] = False
            health["errors"].extend(errors)
        else:
            health["checks"]["settings_valid"] = True
        health["warnings"].extend(warnings)
    except Exception as e:
        health["status"] = "unhealthy"
        health["checks"]["settings_accessible"] = False
        health["errors"].append(str(e))

    s = get_settings_cached()
    health["checks"]["cache"] = _SETTINGS_CACHE.stats()
    health["checks"]["distributed_sources"] = len(_CONFIG_SOURCES.sources)
    health["checks"]["has_orjson"] = bool(_HAS_ORJSON)
    health["checks"]["tracing_enabled"] = bool(_TRACING_ENABLED and _OTEL_AVAILABLE)
    health["checks"]["schema_available"] = bool(schema_available())
    health["checks"]["schema_version"] = _SCHEMA_VERSION or "unknown"
    health["checks"]["open_mode_effective"] = bool(is_open_mode())
    health["checks"]["token_count"] = len(allowed_tokens())
    health["checks"]["public_exact_paths"] = list(s.public_exact_paths or [])
    health["checks"]["public_path_prefixes"] = list(s.public_path_prefixes or [])

    return health


# =============================================================================
# Mask Settings for Output
# =============================================================================

def mask_settings(settings: Optional[Settings] = None) -> Dict[str, Any]:
    """Get masked settings dictionary for safe output."""
    s = settings or get_settings_cached()
    d = s.to_dict()
    d = mask_secret_dict(d, set(SENSITIVE_KEYS))
    d["open_mode_effective"] = bool(is_open_mode())
    d["token_count"] = len(allowed_tokens())
    d["config_version"] = CONFIG_VERSION
    d["schema_available"] = bool(schema_available())
    d["schema_version"] = _SCHEMA_VERSION or "unknown"
    return d


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    "__version__",
    "CONFIG_VERSION",
    "CONFIG_BUILD_TIMESTAMP",
    # Exceptions (v7.0.0: exported)
    "ConfigError",
    "ConfigValidationError",
    "ConfigSourceError",
    # Enums
    "Environment",
    "LogLevel",
    "AuthType",
    "CacheStrategy",
    "ConfigSource",
    "ConfigStatus",
    "EncryptionMethod",
    # Tracing
    "TraceContext",
    # Coercion
    "strip_value",
    "coerce_bool",
    "coerce_int",
    "coerce_float",
    "coerce_list",
    "coerce_dict",
    "is_valid_url",
    # Merge / files
    "deep_merge",
    "load_file_content",
    # Masking
    "mask_secret",
    "mask_secret_dict",
    # Pages / Schema
    "canonical_pages",
    "forbidden_pages",
    "is_output_page",
    "normalize_page",
    "top10_feed_pages_default",
    "schema_available",
    "schema_known",
    "schema_headers",
    "schema_keys",
    "schema_columns",
    "schema_row_template",
    "normalize_row_to_schema",
    # Paths / auth
    "normalize_request_path",
    "is_public_path",
    # Versions
    "ConfigVersion",
    "ConfigVersionManager",
    "get_version_manager",
    "save_config_version",
    # Encryption
    "ConfigEncryption",
    # Settings
    "Settings",
    "get_settings",
    "get_settings_cached",
    "reload_settings",
    "config_health_check",
    "allowed_tokens",
    "is_open_mode",
    "auth_ok",
    "mask_settings",
    "init_distributed_config",
    "normalize_google_credentials",
    "resolve_google_credentials",
]
