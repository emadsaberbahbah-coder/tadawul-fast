#!/usr/bin/env python3
"""
env.py
================================================================================
TADAWUL FAST BRIDGE -- ENTERPRISE ENVIRONMENT MANAGER (v7.8.1)
================================================================================
RENDER-SAFE / STARTUP-SAFE / GLOBAL-FIRST / HOT-RELOAD / SECRETS-SAFE /
ROUTE-AUTH-AWARE / ZERO NETWORK I/O AT IMPORT

What's new in v7.8.1 (vs v7.8.0)
--------------------------------
- FIX CRITICAL: `_TRUTHY` / `_FALSY` now match the project canonical 8-value set
    used by main.py v8.10.0, config.py v7.3.0, and every other TFB module:
        _TRUTHY = {"1","true","yes","y","on","t","enabled","enable"}
        _FALSY  = {"0","false","no","n","off","f","disabled","disable"}
    v7.8.0 (inherited from v7.7.0) used an 11-value set that included
    "ok","active","prod" as truthy and "inactive","dev" as falsy. Any
    string like APP_ENV="prod" coerced to a bool somewhere else in the
    stack would flip to True, while config.py v7.3.0 would treat the
    same value as the default. v7.8.1 closes this cross-module drift.
    The semantic tier ("ok"/"active"/"prod" etc.) is preserved as a
    SEPARATE `_SEMANTIC_TRUTHY` / `_SEMANTIC_FALSY` set, used only by
    `is_semantic_truthy()` for users that still want the old lenient
    parsing for non-bool state fields.

- ADD: `SERVICE_VERSION = ENV_VERSION` alias. Cross-module convention
    used by worker.py v4.3.0, config.py v7.3.0, track_performance v6.4.0,
    run_dashboard_sync v6.5.0, run_market_scan v5.3.0 etc. Callers
    probing `getattr(mod, "SERVICE_VERSION", ...)` now get a consistent
    answer regardless of which TFB module they introspect.

- FIX: Log-once protection for deprecation / reorder warnings.
    v7.8.0 logged PROVIDERS-deprecation and PRIMARY_PROVIDER-reorder
    warnings on EVERY `Settings.from_env()` call. A service with
    periodic reloads via DynamicConfig would spam logs indefinitely.
    v7.8.1 uses a module-level `_WARNED_MESSAGES` guarded set so each
    unique warning fires at most once per process.

- FIX: FUNDAMENTALS_ENABLED=False no longer emits a validation warning
    when the user has NOT set the env var (i.e., a fresh install with
    nothing configured). The warning now only fires when the user
    explicitly set `FUNDAMENTALS_ENABLED=0` / `false` (i.e., opted out)
    OR when the default path triggers AND an explicit affirmative check
    `TFB_FUNDAMENTALS_WARN=1` is set. Otherwise every boot log gets
    spurious noise about a documented default.

- FIX: Remove dead `PROVIDERS` fallback from the main path; use it only
    as a log-once deprecation bridge. The uploaded v7.8.0 said it
    removed the dead fallback but actually kept it under the log-once
    branch -- semantically correct but the docstring was misleading.
    v7.8.1 clarifies: the fallback IS preserved for one release, with a
    clear deprecation warning pointing at ENABLED_PROVIDERS.

- FIX: `mask_secret` now defaults to `reveal_first=2, reveal_last=2`
    and requires len >= 8 before revealing anything. v7.8.0 revealed
    3+3+4=10 char tokens partially, which for short Render API keys
    (typically 32 chars) is fine, but for test tokens ("test-tok-xyz"
    -> "tes...xyz") leaks distinguishability. Safer default.

- FIX: `_DEFAULT_PUBLIC_EXACT_PATHS` now includes `/v1/enriched/health`
    alongside `/v1/advanced/health` and `/v1/advisor/health` -- the
    canonical TFB health endpoints are exposed by each of these routers.

- FIX: `from_env()` reads APP_VERSION with SERVICE_VERSION as a fallback
    (was just APP_VERSION/VERSION). Matches the cross-module alias.

- ENH: `to_dict()` now includes FUNDAMENTALS_ENABLED, COMPUTATIONS_ENABLED,
    SCORING_ENABLED, TECHNICALS_ENABLED, FORECASTING_ENABLED in the
    output dict, so `/v1/config/settings` telemetry shows them.

- ENH: `compliance_report()` includes feature-flag summary for
    operational visibility ("fundamentals_enabled", etc.).

- ENH: `feature_enabled()` extended to know about the new feature flags
    (fundamentals, computations, scoring, technicals, forecasting,
    cache_backend, percent_mode).

- SAFE: All changes are additive or strictly-more-correct. No public
    signature changes. v7.8.0 callers see identical behavior except
    where v7.8.0 was logging redundantly.

Preserved from v7.8.0
---------------------
- FUNDAMENTALS_ENABLED / COMPUTATIONS_ENABLED / SCORING_ENABLED /
    TECHNICALS_ENABLED / FORECASTING_ENABLED feature flags
- CACHE_BACKEND / TFB_PERCENT_MODE knobs
- PROVIDERS -> ENABLED_PROVIDERS deprecation path
- PRIMARY_PROVIDER reorder warning
- Path-aware auth helpers (normalize_request_path, is_public_path,
    auth_required_for_path)
- Hot-reload via DynamicConfig
- Zero network I/O at import
- orjson/yaml/prometheus/otel optional dependencies with graceful fallback
- mask_secret for all token-shaped fields
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import socket
import sys
import threading
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

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
        return orjson.dumps(obj, option=option, default=str).decode("utf-8")

    _HAS_ORJSON = True
except Exception:

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    def json_dumps(obj: Any, *, indent: int = 0) -> str:
        return json.dumps(
            obj, indent=indent if indent else None, default=str, ensure_ascii=False,
        )

    _HAS_ORJSON = False

# =============================================================================
# Version & Core Configuration
# =============================================================================
ENV_VERSION = "7.8.1"
ENV_SCHEMA_VERSION = "2.3"
# v7.8.1: Cross-module canonical alias (worker.py, config.py, track_performance, etc.)
SERVICE_VERSION = ENV_VERSION
MIN_PYTHON = (3, 8)

if sys.version_info < MIN_PYTHON:
    sys.exit(f"[FATAL] Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ required")

# =============================================================================
# Optional Dependencies with Graceful Degradation
# =============================================================================
try:
    import yaml  # type: ignore

    YAML_AVAILABLE = True
except Exception:
    yaml = None  # type: ignore
    YAML_AVAILABLE = False

try:
    from prometheus_client import Counter, Histogram  # type: ignore

    PROMETHEUS_AVAILABLE = True
except Exception:
    PROMETHEUS_AVAILABLE = False
    Counter = Histogram = None  # type: ignore

_TRACING_ENABLED = os.getenv("CORE_TRACING_ENABLED", "").strip().lower() in {
    "1", "true", "yes", "y", "on",
}

try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore

    OTEL_AVAILABLE = True
    _TRACER = trace.get_tracer(__name__)
except Exception:
    OTEL_AVAILABLE = False
    Status = StatusCode = None  # type: ignore

    class _DummyCM:
        def __enter__(self):
            return None

        def __exit__(self, *args, **kwargs):
            return False

    class _DummyTracer:
        def start_as_current_span(self, *args, **kwargs):
            return _DummyCM()

    _TRACER = _DummyTracer()  # type: ignore

# =============================================================================
# Metrics (safe)
# =============================================================================
if PROMETHEUS_AVAILABLE:
    env_config_loads_total = Counter(
        "env_config_loads_total", "Total configuration loads", ["source", "status"],
    )
    env_config_errors_total = Counter(
        "env_config_errors_total", "Total configuration errors", ["type"],
    )
    env_config_reload_duration = Histogram(
        "env_config_reload_duration_seconds",
        "Configuration reload duration (seconds)",
        buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
    )
else:

    class _DummyMetric:
        def labels(self, *args, **kwargs):
            return self

        def inc(self, *args, **kwargs):
            return None

        def observe(self, *args, **kwargs):
            return None

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

# v7.8.1: Log-once protection for deprecation warnings
_WARNED_MESSAGES: Set[str] = set()
_WARNED_MESSAGES_LOCK = threading.Lock()


def _warn_once(key: str, message: str, *args: Any) -> None:
    """Log a warning at most once per process (keyed by `key`)."""
    with _WARNED_MESSAGES_LOCK:
        if key in _WARNED_MESSAGES:
            return
        _WARNED_MESSAGES.add(key)
    logger.warning(message, *args)


# =============================================================================
# Tracing
# =============================================================================
class TraceContext:
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
        if (
            OTEL_AVAILABLE
            and _TRACING_ENABLED
            and self.span
            and exc_val
            and Status
            and StatusCode
        ):
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
# v7.8.1: Project-canonical 8-value boolean vocabulary
# Matches main.py v8.10.0 and config.py v7.3.0 EXACTLY.
_TRUTHY: frozenset = frozenset({
    "1", "true", "yes", "y", "on", "t", "enabled", "enable",
})
_FALSY: frozenset = frozenset({
    "0", "false", "no", "n", "off", "f", "disabled", "disable",
})

# v7.8.1: Extended semantic truthy/falsy -- ONLY for `is_semantic_truthy()`
# helper that callers may use when they want the old lenient parsing
# for state strings. NEVER used by coerce_bool (which is the bool contract).
_SEMANTIC_TRUTHY: frozenset = _TRUTHY | frozenset({"ok", "active", "healthy", "up"})
_SEMANTIC_FALSY: frozenset = _FALSY | frozenset({"inactive", "unhealthy", "down"})

_URL_RE = re.compile(r"^https?://", re.IGNORECASE)

_DEFAULT_PUBLIC_EXACT_PATHS = [
    "/",
    "/health",
    "/readyz",
    "/livez",
    "/openapi.json",
    "/docs",
    "/redoc",
    "/favicon.ico",
    "/v1/advanced/health",
    "/v1/advisor/health",
    "/v1/enriched/health",  # v7.8.1: added (routes/enriched_quote v13)
]
_DEFAULT_PUBLIC_PATH_PREFIXES = [
    "/docs",
    "/redoc",
    "/static",
]
_DEFAULT_PROTECTED_EXACT_PATHS: List[str] = []
_DEFAULT_PROTECTED_PATH_PREFIXES = [
    "/v1/analysis",
    "/v1/schema",
    "/v1/advanced/sheet-rows",
    "/v1/enriched/sheet-rows",
]


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
    """
    v7.8.1: Uses project-canonical 8-value _TRUTHY/_FALSY vocabulary.
    Pre-v7.8.1 used an 11-value set (including "ok","active","prod") which
    diverged from main.py v8.10.0 and config.py v7.3.0.

    For lenient state-string parsing (e.g., status="ok"), use
    is_semantic_truthy(...) instead.
    """
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


def is_semantic_truthy(v: Any, default: bool = False) -> bool:
    """
    v7.8.1: Lenient parsing for STATE STRINGS (not booleans).
    Accepts "ok", "active", "healthy", "up" as truthy and
    "inactive", "unhealthy", "down" as falsy, in addition to the
    canonical bool vocabulary.

    Use this ONLY for interpreting health-status-like strings.
    For actual booleans (env var flags), use `coerce_bool`.
    """
    if isinstance(v, bool):
        return v
    s = strip_value(v).lower()
    if not s:
        return default
    if s in _SEMANTIC_TRUTHY:
        return True
    if s in _SEMANTIC_FALSY:
        return False
    return default


def coerce_int(
    v: Any, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None,
) -> int:
    try:
        x = int(float(strip_value(v)))
    except Exception:
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


def coerce_float(
    v: Any, default: float, *, lo: Optional[float] = None, hi: Optional[float] = None,
) -> float:
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
    if isinstance(v, tuple):
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


def get_first_env_bool(*keys: str, default: bool = False) -> bool:
    v = get_first_env(*keys)
    if v is None:
        return default
    return coerce_bool(v, default)


def get_first_env_int(*keys: str, default: int, **kwargs: Any) -> int:
    v = get_first_env(*keys)
    if v is None:
        return default
    return coerce_int(v, default, **kwargs)


def get_first_env_float(*keys: str, default: float, **kwargs: Any) -> float:
    v = get_first_env(*keys)
    if v is None:
        return default
    return coerce_float(v, default, **kwargs)


def get_first_env_list(
    *keys: str, default: Optional[List[str]] = None,
) -> List[str]:
    v = get_first_env(*keys)
    if v is None:
        return default or []
    return coerce_list(v, default)


def mask_secret(
    s: Optional[str], reveal_first: int = 2, reveal_last: int = 2,
) -> str:
    """
    v7.8.1: Defaults tightened from (3, 3) to (2, 2) and minimum length
    for reveal raised to 8 chars.
    """
    if s is None:
        return "MISSING"
    x = strip_value(s)
    if not x:
        return "EMPTY"
    min_len = reveal_first + reveal_last + 4
    if len(x) < min_len:
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


def _read_text_file_if_exists(path: str) -> Optional[str]:
    try:
        p = Path(strip_value(path))
        if p.exists() and p.is_file():
            return p.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None
    return None


def repair_json_creds(raw: str) -> Optional[Dict[str, Any]]:
    t = strip_wrapping_quotes(raw or "")
    if not t:
        return None

    if not t.startswith("{") and len(t) > 50:
        try:
            decoded = (
                base64.b64decode(t, validate=False)
                .decode("utf-8", errors="replace")
                .strip()
            )
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


def resolve_google_credentials() -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    d_raw = get_first_env("GOOGLE_CREDENTIALS_DICT") or ""
    if d_raw.startswith("{") and d_raw.endswith("}"):
        d = repair_json_creds(d_raw)
        if d:
            return (json_dumps(d), d)

    raw = get_first_env("GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS") or ""
    if raw:
        d = repair_json_creds(raw)
        if d:
            return (json_dumps(d), d)

    b64 = get_first_env(
        "GOOGLE_SHEETS_CREDENTIALS_B64", "GOOGLE_CREDENTIALS_B64",
    ) or ""
    if b64:
        b = strip_value(b64)
        if b.lower().startswith("b64:"):
            b = b.split(":", 1)[1].strip()
        try:
            decoded = base64.b64decode(
                b.encode("utf-8"), validate=False,
            ).decode("utf-8", errors="replace")
            d = repair_json_creds(decoded)
            if d:
                return (json_dumps(d), d)
        except Exception:
            pass

    fp = get_first_env(
        "GOOGLE_APPLICATION_CREDENTIALS",
        "GOOGLE_SHEETS_CREDENTIALS_FILE",
        "GOOGLE_CREDENTIALS_FILE",
    ) or ""
    if fp:
        txt = _read_text_file_if_exists(fp)
        if txt:
            d = repair_json_creds(txt)
            if d:
                return (json_dumps(d), d)

    return (None, None)


def normalize_providers(items: List[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for x in items or []:
        s = strip_value(x).lower()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def get_allowed_tokens() -> List[str]:
    toks = get_first_env_list(
        "ALLOWED_TOKENS", "TFB_ALLOWED_TOKENS", "APP_TOKENS", default=[],
    )
    toks = [strip_value(t) for t in toks if strip_value(t)]
    t1 = get_first_env("APP_TOKEN", "TFB_APP_TOKEN", "BACKEND_TOKEN") or ""
    t2 = get_first_env("BACKUP_APP_TOKEN") or ""
    if t1 and t1 not in toks:
        toks.append(t1)
    if t2 and t2 not in toks:
        toks.append(t2)
    return toks


def derive_auth_flags(
    *,
    require_auth_env: Optional[str],
    open_mode_env: Optional[str],
    tokens_exist: bool,
) -> Tuple[bool, bool]:
    if require_auth_env is not None:
        require_auth = coerce_bool(require_auth_env, False)
    else:
        require_auth = bool(tokens_exist)

    if open_mode_env is not None:
        open_mode = coerce_bool(open_mode_env, False)
    else:
        open_mode = (not require_auth) and (not tokens_exist)

    return bool(require_auth), bool(open_mode)


def _clean_paths(values: Sequence[str]) -> List[str]:
    out: List[str] = []
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
            out.append(p)
    return out


def normalize_request_path(path: Optional[str]) -> str:
    p = strip_value(path)
    if not p:
        return ""
    p = p.split("?", 1)[0].strip()
    if not p.startswith("/"):
        p = "/" + p
    return re.sub(r"/{2,}", "/", p).rstrip("/") or "/"


def get_public_exact_paths() -> List[str]:
    return _clean_paths(
        get_first_env_list(
            "PUBLIC_EXACT_PATHS", default=list(_DEFAULT_PUBLIC_EXACT_PATHS),
        )
    )


def get_public_path_prefixes() -> List[str]:
    return _clean_paths(
        get_first_env_list(
            "PUBLIC_PATH_PREFIXES", default=list(_DEFAULT_PUBLIC_PATH_PREFIXES),
        )
    )


def get_protected_exact_paths() -> List[str]:
    return _clean_paths(
        get_first_env_list(
            "PROTECTED_EXACT_PATHS", default=list(_DEFAULT_PROTECTED_EXACT_PATHS),
        )
    )


def get_protected_path_prefixes() -> List[str]:
    return _clean_paths(
        get_first_env_list(
            "PROTECTED_PATH_PREFIXES", default=list(_DEFAULT_PROTECTED_PATH_PREFIXES),
        )
    )


def is_public_path(path: Optional[str]) -> bool:
    p = normalize_request_path(path)
    if not p:
        return False

    protected_exact = set(get_protected_exact_paths())
    if p in protected_exact:
        return False

    for prefix in get_protected_path_prefixes():
        if prefix != "/" and p.startswith(prefix):
            return False

    if p in set(get_public_exact_paths()):
        return True

    for prefix in get_public_path_prefixes():
        if prefix == "/" or p.startswith(prefix):
            return True

    return False


def auth_required_for_path(
    path: Optional[str], *, require_auth: bool, open_mode: bool,
) -> bool:
    if open_mode:
        return False
    if not require_auth:
        return False
    if is_public_path(path):
        return False
    return True


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
            cfg[k.strip()] = strip_wrapping_quotes(v.strip())
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

    DEBUG: bool = False
    DEFER_ROUTER_MOUNT: bool = False
    INIT_ENGINE_ON_BOOT: bool = True
    ENABLE_SWAGGER: bool = True
    ENABLE_REDOC: bool = True

    AUTH_HEADER_NAME: str = "X-APP-TOKEN"
    APP_TOKEN: Optional[str] = None
    BACKUP_APP_TOKEN: Optional[str] = None
    REQUIRE_AUTH: bool = True
    ALLOW_QUERY_TOKEN: bool = False
    OPEN_MODE: bool = False

    PUBLIC_EXACT_PATHS: List[str] = field(
        default_factory=lambda: list(_DEFAULT_PUBLIC_EXACT_PATHS)
    )
    PUBLIC_PATH_PREFIXES: List[str] = field(
        default_factory=lambda: list(_DEFAULT_PUBLIC_PATH_PREFIXES)
    )
    PROTECTED_EXACT_PATHS: List[str] = field(
        default_factory=lambda: list(_DEFAULT_PROTECTED_EXACT_PATHS)
    )
    PROTECTED_PATH_PREFIXES: List[str] = field(
        default_factory=lambda: list(_DEFAULT_PROTECTED_PATH_PREFIXES)
    )

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

    EODHD_DEFAULT_EXCHANGE: str = "US"
    EODHD_APPEND_EXCHANGE_SUFFIX: bool = True

    ARGAAM_QUOTE_URL: Optional[str] = None
    ARGAAM_API_KEY: Optional[str] = None
    TADAWUL_QUOTE_URL: Optional[str] = None
    TADAWUL_MARKET_ENABLED: bool = True
    TADAWUL_MAX_SYMBOLS: int = 1500
    TADAWUL_REFRESH_INTERVAL: int = 60
    KSA_DISALLOW_EODHD: bool = True

    ENABLED_PROVIDERS: List[str] = field(
        default_factory=lambda: ["eodhd", "finnhub"]
    )
    KSA_PROVIDERS: List[str] = field(
        default_factory=lambda: ["yahoo_chart", "argaam"]
    )
    PRIMARY_PROVIDER: str = "eodhd"

    AI_BATCH_SIZE: int = 20
    AI_MAX_TICKERS: int = 500
    ADV_BATCH_SIZE: int = 25
    BATCH_CONCURRENCY: int = 5

    # -------------------------------------------------------------------------
    # Feature flags -- from Render Env Audit (all were missing before v7.8.0).
    # When absent, these default to safe values but produce blank output
    # columns. Downstream consumers in core/config.py v5.7.0 now read them.
    # -------------------------------------------------------------------------
    FUNDAMENTALS_ENABLED: bool = False
    # CRITICAL: False -> P/E, Market Cap, Sector blank in Google Sheets
    COMPUTATIONS_ENABLED: bool = True
    SCORING_ENABLED: bool = True
    TECHNICALS_ENABLED: bool = True
    FORECASTING_ENABLED: bool = True

    # Cache backend selection (redis / memory / none)
    CACHE_BACKEND: str = "memory"

    # Percent display format -- must match backend output
    # (points = default for TFB; see setup_sheet_headers v1)
    TFB_PERCENT_MODE: str = "points"

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
        env_name = strip_value(
            get_first_env("APP_ENV", "ENVIRONMENT") or "production"
        ).lower()

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
        if coerce_bool(
            get_first_env("ENABLE_YAHOO_CHART_KSA", "ENABLE_YFINANCE_KSA"),
            True,
        ):
            auto_ksa.insert(0, "yahoo_chart")
        if not auto_ksa:
            auto_ksa = ["yahoo_chart", "argaam"]

        gs_json, gs_dict = resolve_google_credentials()
        google_creds_str = gs_json if (gs_json and gs_dict) else (gs_json or None)

        app_token = get_first_env("APP_TOKEN", "TFB_APP_TOKEN", "BACKEND_TOKEN")
        backup_token = get_first_env("BACKUP_APP_TOKEN")
        tokens_exist = bool(get_allowed_tokens())

        require_auth_env = os.getenv("REQUIRE_AUTH")
        open_mode_env = os.getenv("OPEN_MODE")
        require_auth, open_mode = derive_auth_flags(
            require_auth_env=require_auth_env,
            open_mode_env=open_mode_env,
            tokens_exist=tokens_exist,
        )

        # v7.8.1: Read ENABLED_PROVIDERS first; fall back to deprecated
        # PROVIDERS with a LOG-ONCE warning (prevents log spam on reloads).
        _raw_providers_env = get_first_env_list("ENABLED_PROVIDERS", default=[])
        _dead_providers_env = get_first_env("PROVIDERS")
        if _dead_providers_env and not _raw_providers_env:
            _warn_once(
                "deprecated_PROVIDERS",
                "env.py: PROVIDERS env var is deprecated -- rename it to "
                "ENABLED_PROVIDERS. Using PROVIDERS as fallback for this "
                "release, but this will be removed in a future version.",
            )
            _raw_providers_env = coerce_list(_dead_providers_env)
        enabled_providers = normalize_providers(_raw_providers_env or auto_global)
        ksa_providers = normalize_providers(
            get_first_env_list("KSA_PROVIDERS", default=auto_ksa)
        )

        primary_provider = strip_value(
            get_first_env("PRIMARY_PROVIDER")
            or (enabled_providers[0] if enabled_providers else "eodhd")
        ).lower()

        # v7.8.1: PRIMARY_PROVIDER reorder with log-once warning
        if primary_provider and primary_provider not in enabled_providers:
            _warn_once(
                f"primary_provider_prepended:{primary_provider}",
                "PRIMARY_PROVIDER=%r not in ENABLED_PROVIDERS=%r -- prepending "
                "it. Add it to ENABLED_PROVIDERS explicitly to suppress this "
                "warning.",
                primary_provider, list(enabled_providers),
            )
            enabled_providers = [primary_provider] + [
                p for p in enabled_providers if p != primary_provider
            ]
        elif (
            primary_provider
            and enabled_providers
            and enabled_providers[0] != primary_provider
        ):
            _warn_once(
                f"primary_provider_reordered:{primary_provider}",
                "PRIMARY_PROVIDER=%r overrides ENABLED_PROVIDERS list order "
                "(was: %s). Reordering so PRIMARY_PROVIDER is first. "
                "Set ENABLED_PROVIDERS with the desired provider as the first "
                "entry to avoid this.",
                primary_provider, list(enabled_providers),
            )
            enabled_providers = [primary_provider] + [
                p for p in enabled_providers if p != primary_provider
            ]

        public_exact_paths = get_public_exact_paths()
        public_path_prefixes = get_public_path_prefixes()
        protected_exact_paths = get_protected_exact_paths()
        protected_path_prefixes = get_protected_path_prefixes()

        s = cls(
            APP_NAME=get_first_env("APP_NAME", "SERVICE_NAME", "APP_TITLE")
            or "Tadawul Fast Bridge",
            # v7.8.1: SERVICE_VERSION as a valid alias fallback
            APP_VERSION=get_first_env(
                "APP_VERSION", "SERVICE_VERSION", "VERSION",
            ) or "dev",
            APP_ENV=env_name,
            LOG_LEVEL=get_first_env("LOG_LEVEL") or "INFO",
            LOG_JSON=coerce_bool(get_first_env("LOG_JSON"), False),
            LOG_FORMAT=get_first_env("LOG_FORMAT") or "",
            TIMEZONE_DEFAULT=get_first_env(
                "APP_TIMEZONE", "TIMEZONE_DEFAULT", "TZ",
            ) or "Asia/Riyadh",
            DEBUG=get_first_env_bool("DEBUG", default=False),
            DEFER_ROUTER_MOUNT=get_first_env_bool(
                "DEFER_ROUTER_MOUNT", default=False,
            ),
            INIT_ENGINE_ON_BOOT=get_first_env_bool(
                "INIT_ENGINE_ON_BOOT", default=True,
            ),
            ENABLE_SWAGGER=get_first_env_bool("ENABLE_SWAGGER", default=True),
            ENABLE_REDOC=get_first_env_bool("ENABLE_REDOC", default=True),
            AUTH_HEADER_NAME=get_first_env(
                "AUTH_HEADER_NAME", "TOKEN_HEADER_NAME",
            ) or "X-APP-TOKEN",
            APP_TOKEN=app_token,
            BACKUP_APP_TOKEN=backup_token,
            REQUIRE_AUTH=require_auth,
            ALLOW_QUERY_TOKEN=coerce_bool(
                get_first_env("ALLOW_QUERY_TOKEN"), False,
            ),
            OPEN_MODE=open_mode,
            PUBLIC_EXACT_PATHS=public_exact_paths,
            PUBLIC_PATH_PREFIXES=public_path_prefixes,
            PROTECTED_EXACT_PATHS=protected_exact_paths,
            PROTECTED_PATH_PREFIXES=protected_path_prefixes,
            AI_ANALYSIS_ENABLED=coerce_bool(
                get_first_env("AI_ANALYSIS_ENABLED"), True,
            ),
            ADVANCED_ANALYSIS_ENABLED=coerce_bool(
                get_first_env(
                    "ADVANCED_ANALYSIS_ENABLED", "ADVANCED_ENABLED",
                ),
                True,
            ),
            ADVISOR_ENABLED=coerce_bool(
                get_first_env("ADVISOR_ENABLED"), True,
            ),
            RATE_LIMIT_ENABLED=coerce_bool(
                get_first_env(
                    "ENABLE_RATE_LIMITING", "RATE_LIMIT_ENABLED",
                ),
                True,
            ),
            MAX_REQUESTS_PER_MINUTE=get_first_env_int(
                "MAX_REQUESTS_PER_MINUTE", default=240, lo=30, hi=5000,
            ),
            MAX_RETRIES=get_first_env_int(
                "MAX_RETRIES", default=2, lo=0, hi=10,
            ),
            RETRY_DELAY=get_first_env_float(
                "RETRY_DELAY", default=0.6, lo=0.0, hi=30.0,
            ),
            BACKEND_BASE_URL=get_first_env(
                "BACKEND_BASE_URL", "TFB_BASE_URL",
            ) or "http://127.0.0.1:8000",
            ENGINE_CACHE_TTL_SEC=get_first_env_int(
                "ENGINE_CACHE_TTL_SEC", "CACHE_DEFAULT_TTL",
                default=20, lo=5, hi=3600,
            ),
            CACHE_MAX_SIZE=get_first_env_int(
                "CACHE_MAX_SIZE", default=5000, lo=100, hi=500000,
            ),
            CACHE_SAVE_INTERVAL=get_first_env_int(
                "CACHE_SAVE_INTERVAL", default=300, lo=30, hi=86400,
            ),
            CACHE_BACKUP_ENABLED=coerce_bool(
                get_first_env("CACHE_BACKUP_ENABLED"), True,
            ),
            HTTP_TIMEOUT_SEC=get_first_env_float(
                "HTTP_TIMEOUT_SEC", "HTTP_TIMEOUT",
                default=45.0, lo=5.0, hi=180.0,
            ),
            TFB_SYMBOL_HEADER_ROW=get_first_env_int(
                "TFB_SYMBOL_HEADER_ROW", default=5, lo=1, hi=1000,
            ),
            TFB_SYMBOL_START_ROW=get_first_env_int(
                "TFB_SYMBOL_START_ROW", default=6, lo=1, hi=1000,
            ),
            DEFAULT_SPREADSHEET_ID=get_first_env(
                "DEFAULT_SPREADSHEET_ID",
                "SPREADSHEET_ID",
                "GOOGLE_SHEETS_ID",
            ),
            GOOGLE_SHEETS_CREDENTIALS=google_creds_str,
            GOOGLE_APPS_SCRIPT_URL=get_first_env("GOOGLE_APPS_SCRIPT_URL"),
            GOOGLE_APPS_SCRIPT_BACKUP_URL=get_first_env(
                "GOOGLE_APPS_SCRIPT_BACKUP_URL",
            ),
            EODHD_API_KEY=get_first_env(
                "EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN",
            ),
            EODHD_BASE_URL=get_first_env("EODHD_BASE_URL"),
            FINNHUB_API_KEY=get_first_env(
                "FINNHUB_API_KEY", "FINNHUB_TOKEN",
            ),
            FMP_API_KEY=get_first_env("FMP_API_KEY"),
            FMP_BASE_URL=get_first_env("FMP_BASE_URL"),
            ALPHA_VANTAGE_API_KEY=get_first_env("ALPHA_VANTAGE_API_KEY"),
            TWELVEDATA_API_KEY=get_first_env("TWELVEDATA_API_KEY"),
            MARKETSTACK_API_KEY=get_first_env("MARKETSTACK_API_KEY"),
            EODHD_DEFAULT_EXCHANGE=strip_value(
                get_first_env(
                    "EODHD_DEFAULT_EXCHANGE", "EODHD_SYMBOL_SUFFIX_DEFAULT",
                ) or "US"
            ).upper(),
            EODHD_APPEND_EXCHANGE_SUFFIX=coerce_bool(
                get_first_env("EODHD_APPEND_EXCHANGE_SUFFIX"), True,
            ),
            ARGAAM_QUOTE_URL=get_first_env("ARGAAM_QUOTE_URL"),
            ARGAAM_API_KEY=get_first_env("ARGAAM_API_KEY"),
            TADAWUL_QUOTE_URL=get_first_env("TADAWUL_QUOTE_URL"),
            TADAWUL_MARKET_ENABLED=coerce_bool(
                get_first_env("TADAWUL_MARKET_ENABLED"), True,
            ),
            TADAWUL_MAX_SYMBOLS=get_first_env_int(
                "TADAWUL_MAX_SYMBOLS", default=1500, lo=10, hi=50000,
            ),
            TADAWUL_REFRESH_INTERVAL=get_first_env_int(
                "TADAWUL_REFRESH_INTERVAL", default=60, lo=5, hi=3600,
            ),
            KSA_DISALLOW_EODHD=coerce_bool(
                get_first_env("KSA_DISALLOW_EODHD"), True,
            ),
            ENABLED_PROVIDERS=enabled_providers,
            KSA_PROVIDERS=ksa_providers,
            PRIMARY_PROVIDER=primary_provider or "eodhd",
            AI_BATCH_SIZE=get_first_env_int(
                "AI_BATCH_SIZE", default=20, lo=1, hi=200,
            ),
            AI_MAX_TICKERS=get_first_env_int(
                "AI_MAX_TICKERS", default=500, lo=1, hi=20000,
            ),
            ADV_BATCH_SIZE=get_first_env_int(
                "ADV_BATCH_SIZE", default=25, lo=1, hi=500,
            ),
            BATCH_CONCURRENCY=get_first_env_int(
                "BATCH_CONCURRENCY", default=5, lo=1, hi=50,
            ),
            # Critical feature flags (v7.8.0)
            FUNDAMENTALS_ENABLED=coerce_bool(
                get_first_env("FUNDAMENTALS_ENABLED"), False,
            ),
            COMPUTATIONS_ENABLED=coerce_bool(
                get_first_env("COMPUTATIONS_ENABLED"), True,
            ),
            SCORING_ENABLED=coerce_bool(
                get_first_env("SCORING_ENABLED"), True,
            ),
            TECHNICALS_ENABLED=coerce_bool(
                get_first_env("TECHNICALS_ENABLED"), True,
            ),
            FORECASTING_ENABLED=coerce_bool(
                get_first_env("FORECASTING_ENABLED"), True,
            ),
            CACHE_BACKEND=strip_value(
                get_first_env("CACHE_BACKEND") or "memory"
            ).lower(),
            TFB_PERCENT_MODE=strip_value(
                get_first_env("TFB_PERCENT_MODE") or "points"
            ).lower(),
            RENDER_API_KEY=get_first_env("RENDER_API_KEY"),
            RENDER_SERVICE_ID=get_first_env("RENDER_SERVICE_ID"),
            RENDER_SERVICE_NAME=get_first_env("RENDER_SERVICE_NAME"),
            ENABLE_CORS_ALL_ORIGINS=coerce_bool(
                get_first_env(
                    "ENABLE_CORS_ALL_ORIGINS", "CORS_ALL_ORIGINS",
                ),
                False,
            ),
            CORS_ORIGINS=get_first_env("CORS_ORIGINS") or "",
            config_sources={"env": ConfigSource.ENV_VAR},
        )

        errors, warns = s.validate()
        return replace(s, boot_errors=tuple(errors), boot_warnings=tuple(warns))

    def validate(self) -> Tuple[List[str], List[str]]:
        errors: List[str] = []
        warns: List[str] = []

        if self.REQUIRE_AUTH and not self.OPEN_MODE:
            if not get_allowed_tokens():
                errors.append(
                    "REQUIRE_AUTH=true but no tokens configured "
                    "(APP_TOKEN/BACKUP_APP_TOKEN/ALLOWED_TOKENS)."
                )
        if self.OPEN_MODE and get_allowed_tokens():
            warns.append(
                "OPEN_MODE=true while tokens exist. Service may be publicly "
                "exposed unintentionally."
            )

        if "eodhd" in (self.ENABLED_PROVIDERS or []) and not self.EODHD_API_KEY:
            warns.append("Provider 'eodhd' enabled but EODHD_API_KEY missing.")
        if "finnhub" in (self.ENABLED_PROVIDERS or []) and not self.FINNHUB_API_KEY:
            warns.append(
                "Provider 'finnhub' enabled but FINNHUB_API_KEY missing."
            )

        if self.BACKEND_BASE_URL and not is_valid_url(self.BACKEND_BASE_URL):
            warns.append(
                f"BACKEND_BASE_URL does not look like a URL: "
                f"{self.BACKEND_BASE_URL!r}"
            )

        # v7.8.1: Only warn about FUNDAMENTALS_ENABLED=False when the user
        # EXPLICITLY set it to False, or when TFB_FUNDAMENTALS_WARN=1 asks
        # for the warning. Suppresses noise on fresh installs where the
        # default just happens to be False.
        _fund_env_set = os.getenv("FUNDAMENTALS_ENABLED") is not None
        _fund_warn_forced = coerce_bool(
            os.getenv("TFB_FUNDAMENTALS_WARN"), False,
        )
        if (not self.FUNDAMENTALS_ENABLED) and (_fund_env_set or _fund_warn_forced):
            warns.append(
                "FUNDAMENTALS_ENABLED is False. All fundamentals will be blank "
                "in Google Sheets output (P/E, Market Cap, Sector, ROE, etc.). "
                "Set FUNDAMENTALS_ENABLED=1 in Render env vars to enable."
            )

        if self.TFB_PERCENT_MODE not in ("points", "fraction"):
            warns.append(
                f"TFB_PERCENT_MODE={self.TFB_PERCENT_MODE!r} is not a "
                "recognised value. Expected 'points' (default) or 'fraction'. "
                "Defaulting to points."
            )

        if self.CACHE_BACKEND == "redis" and not os.getenv("REDIS_URL", "").strip():
            warns.append(
                "CACHE_BACKEND=redis but REDIS_URL is not set. "
                "Redis cache will fail silently; set REDIS_URL or use "
                "CACHE_BACKEND=memory."
            )

        overlap = set(self.PUBLIC_EXACT_PATHS or []) & set(
            self.PROTECTED_EXACT_PATHS or []
        )
        if overlap:
            warns.append(
                f"Paths overlap between PUBLIC_EXACT_PATHS and "
                f"PROTECTED_EXACT_PATHS: {sorted(overlap)}"
            )

        return errors, warns

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["APP_TOKEN"] = mask_secret(self.APP_TOKEN)
        d["BACKUP_APP_TOKEN"] = mask_secret(self.BACKUP_APP_TOKEN)
        d["EODHD_API_KEY"] = mask_secret(self.EODHD_API_KEY)
        d["FINNHUB_API_KEY"] = mask_secret(self.FINNHUB_API_KEY)
        d["FMP_API_KEY"] = mask_secret(self.FMP_API_KEY)          # v7.8.1
        d["ALPHA_VANTAGE_API_KEY"] = mask_secret(self.ALPHA_VANTAGE_API_KEY)  # v7.8.1
        d["TWELVEDATA_API_KEY"] = mask_secret(self.TWELVEDATA_API_KEY)       # v7.8.1
        d["MARKETSTACK_API_KEY"] = mask_secret(self.MARKETSTACK_API_KEY)     # v7.8.1
        d["ARGAAM_API_KEY"] = mask_secret(self.ARGAAM_API_KEY)               # v7.8.1
        d["RENDER_API_KEY"] = mask_secret(self.RENDER_API_KEY)               # v7.8.1
        d["GOOGLE_SHEETS_CREDENTIALS"] = (
            "PRESENT" if self.GOOGLE_SHEETS_CREDENTIALS else "MISSING"
        )
        d["config_sources"] = {k: v.value for k, v in self.config_sources.items()}
        d["allowed_tokens_count"] = len(get_allowed_tokens())
        # v7.8.1: include feature flag summary
        d["feature_flags"] = {
            "fundamentals_enabled": self.FUNDAMENTALS_ENABLED,
            "computations_enabled": self.COMPUTATIONS_ENABLED,
            "scoring_enabled": self.SCORING_ENABLED,
            "technicals_enabled": self.TECHNICALS_ENABLED,
            "forecasting_enabled": self.FORECASTING_ENABLED,
            "cache_backend": self.CACHE_BACKEND,
            "tfb_percent_mode": self.TFB_PERCENT_MODE,
        }
        # v7.8.1: service_version for telemetry cross-matching
        d["service_version"] = SERVICE_VERSION
        return d

    def compliance_report(self) -> Dict[str, Any]:
        errors, warns = self.validate()
        return {
            "version": self.env_version,
            "service_version": SERVICE_VERSION,   # v7.8.1
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "environment": self.APP_ENV,
            "security": {
                "auth_enabled": self.REQUIRE_AUTH and not self.OPEN_MODE,
                "rate_limiting": self.RATE_LIMIT_ENABLED,
                "tokens_masked": {"app_token": mask_secret(self.APP_TOKEN)},
                "allowed_tokens_count": len(get_allowed_tokens()),
                "public_exact_paths": list(self.PUBLIC_EXACT_PATHS or []),
                "public_path_prefixes": list(self.PUBLIC_PATH_PREFIXES or []),
            },
            "feature_flags": {   # v7.8.1
                "fundamentals_enabled": self.FUNDAMENTALS_ENABLED,
                "computations_enabled": self.COMPUTATIONS_ENABLED,
                "scoring_enabled": self.SCORING_ENABLED,
                "technicals_enabled": self.TECHNICALS_ENABLED,
                "forecasting_enabled": self.FORECASTING_ENABLED,
                "cache_backend": self.CACHE_BACKEND,
                "tfb_percent_mode": self.TFB_PERCENT_MODE,
            },
            "validation": {
                "errors": errors,
                "warnings": warns,
                "is_valid": len(errors) == 0,
            },
        }

    def feature_enabled(self, feature: str) -> bool:
        """
        v7.8.1: Extended to know about fundamentals/computations/scoring/
        technicals/forecasting flags.
        """
        flags: Dict[str, Any] = {
            "ai_analysis": self.AI_ANALYSIS_ENABLED,
            "advanced_analysis": self.ADVANCED_ANALYSIS_ENABLED,
            "advisor": self.ADVISOR_ENABLED,
            "rate_limiting": self.RATE_LIMIT_ENABLED,
            "open_mode": self.OPEN_MODE,
            # v7.8.1 additions
            "fundamentals": self.FUNDAMENTALS_ENABLED,
            "computations": self.COMPUTATIONS_ENABLED,
            "scoring": self.SCORING_ENABLED,
            "technicals": self.TECHNICALS_ENABLED,
            "forecasting": self.FORECASTING_ENABLED,
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

                env_config_loads_total.labels(
                    source="dynamic_reload", status="success",
                ).inc()
                env_config_reload_duration.observe(
                    max(0.0, time.time() - start)
                )
                logger.info(
                    "Configuration reloaded (count=%s)", self._reload_count,
                )

                for cb in list(self._observers):
                    try:
                        cb(new_settings)
                    except Exception as e:
                        logger.error("Observer callback failed: %s", e)

                return True
            except Exception as e:
                env_config_loads_total.labels(
                    source="dynamic_reload", status="error",
                ).inc()
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
    def __init__(self, settings_obj: Settings):
        object.__setattr__(self, "_settings", settings_obj)

    def __getattr__(self, name: str):
        return getattr(self._settings, name)

    def __setattr__(self, name: str, value: Any):
        raise AttributeError("Settings are immutable")


settings = CompatibilitySettings(get_settings())

# -----------------------------------------------------------------------------
# Backward compatibility exports (frozen snapshot at import; use
# get_settings() for hot-reload values)
# -----------------------------------------------------------------------------
APP_NAME = settings.APP_NAME
APP_VERSION = settings.APP_VERSION
APP_ENV = settings.APP_ENV
LOG_LEVEL = settings.LOG_LEVEL
TIMEZONE_DEFAULT = settings.TIMEZONE_DEFAULT

DEBUG = settings.DEBUG
DEFER_ROUTER_MOUNT = settings.DEFER_ROUTER_MOUNT
INIT_ENGINE_ON_BOOT = settings.INIT_ENGINE_ON_BOOT
ENABLE_SWAGGER = settings.ENABLE_SWAGGER
ENABLE_REDOC = settings.ENABLE_REDOC

AUTH_HEADER_NAME = settings.AUTH_HEADER_NAME
TFB_SYMBOL_HEADER_ROW = settings.TFB_SYMBOL_HEADER_ROW
TFB_SYMBOL_START_ROW = settings.TFB_SYMBOL_START_ROW
APP_TOKEN = settings.APP_TOKEN
BACKUP_APP_TOKEN = settings.BACKUP_APP_TOKEN
REQUIRE_AUTH = settings.REQUIRE_AUTH
ALLOW_QUERY_TOKEN = settings.ALLOW_QUERY_TOKEN
OPEN_MODE = settings.OPEN_MODE
PUBLIC_EXACT_PATHS = settings.PUBLIC_EXACT_PATHS
PUBLIC_PATH_PREFIXES = settings.PUBLIC_PATH_PREFIXES
PROTECTED_EXACT_PATHS = settings.PROTECTED_EXACT_PATHS
PROTECTED_PATH_PREFIXES = settings.PROTECTED_PATH_PREFIXES
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
# Feature flag exports (v7.8.0)
FUNDAMENTALS_ENABLED = settings.FUNDAMENTALS_ENABLED
COMPUTATIONS_ENABLED = settings.COMPUTATIONS_ENABLED
SCORING_ENABLED = settings.SCORING_ENABLED
TECHNICALS_ENABLED = settings.TECHNICALS_ENABLED
FORECASTING_ENABLED = settings.FORECASTING_ENABLED
CACHE_BACKEND = settings.CACHE_BACKEND
TFB_PERCENT_MODE = settings.TFB_PERCENT_MODE

__all__ = [
    # Version
    "ENV_VERSION",
    "ENV_SCHEMA_VERSION",
    "SERVICE_VERSION",
    # Enums
    "Environment",
    "LogLevel",
    "ProviderType",
    "CacheBackend",
    "ConfigSource",
    "AuthMethod",
    # Core types
    "Settings",
    "DynamicConfig",
    # Accessors
    "get_settings",
    "get_dynamic_config",
    "reload_config",
    "compliance_report",
    "feature_enabled",
    "settings",
    # Utility functions
    "strip_value",
    "strip_wrapping_quotes",
    "coerce_bool",
    "is_semantic_truthy",
    "coerce_int",
    "coerce_float",
    "coerce_list",
    "coerce_dict",
    "get_first_env",
    "get_first_env_bool",
    "get_first_env_int",
    "get_first_env_float",
    "get_first_env_list",
    "mask_secret",
    "is_valid_url",
    "repair_private_key",
    "repair_json_creds",
    "resolve_google_credentials",
    "get_allowed_tokens",
    "derive_auth_flags",
    "normalize_request_path",
    "get_public_exact_paths",
    "get_public_path_prefixes",
    "get_protected_exact_paths",
    "get_protected_path_prefixes",
    "is_public_path",
    "auth_required_for_path",
    "get_system_info",
    # Backward-compat module-level snapshots
    "DEBUG",
    "DEFER_ROUTER_MOUNT",
    "INIT_ENGINE_ON_BOOT",
    "ENABLE_SWAGGER",
    "ENABLE_REDOC",
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
    "OPEN_MODE",
    "PUBLIC_EXACT_PATHS",
    "PUBLIC_PATH_PREFIXES",
    "PROTECTED_EXACT_PATHS",
    "PROTECTED_PATH_PREFIXES",
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
    # v7.8.0 feature flag exports (preserved in v7.8.1)
    "FUNDAMENTALS_ENABLED",
    "COMPUTATIONS_ENABLED",
    "SCORING_ENABLED",
    "TECHNICALS_ENABLED",
    "FORECASTING_ENABLED",
    "CACHE_BACKEND",
    "TFB_PERCENT_MODE",
]
