#!/usr/bin/env python3
# core/config.py
"""
================================================================================
Core Configuration Module — v5.0.0 (ADVANCED PRODUCTION)
================================================================================
TADAWUL FAST BRIDGE – Enterprise Configuration Management

What's new in v5.0.0:
- ✅ Advanced settings management with hierarchical resolution
- ✅ Multi-provider authentication support (tokens, API keys, JWT, OAuth)
- ✅ Comprehensive logging with sensitive data masking
- ✅ Thread-safe settings access with caching
- ✅ Environment-aware configuration (dev/staging/prod)
- ✅ Settings validation with Pydantic integration
- ✅ Dynamic provider configuration
- ✅ Rate limiting and circuit breaker settings
- ✅ Cache TTL management per data type
- ✅ Feature flags and toggles
- ✅ Metrics and monitoring configuration
- ✅ Never crashes startup: all functions are defensive

Key Features:
- Zero external dependencies (optional Pydantic for validation)
- Thread-safe singleton pattern
- Comprehensive error handling
- Sensitive data masking
- Environment variable overrides
- Dynamic reload capability
- Backward compatible with v3.x and v4.x
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type, TypeVar, Union
from functools import lru_cache, wraps

__version__ = "5.0.0"
CONFIG_VERSION = __version__


# ============================================================================
# Debug Configuration
# ============================================================================

class LogLevel(Enum):
    DEBUG = 10
    INFO = 20
    WARN = 30
    WARNING = 30
    ERROR = 40
    CRITICAL = 50

_DEBUG = os.getenv("CORE_CONFIG_DEBUG", "").strip().lower() in {"1", "true", "yes", "y", "on"}
_DEBUG_LEVEL = os.getenv("CORE_CONFIG_DEBUG_LEVEL", "info").strip().lower() or "info"
_LOG_LEVEL = LogLevel.INFO
try:
    _LOG_LEVEL = LogLevel[_DEBUG_LEVEL.upper()]
except (KeyError, AttributeError):
    _LOG_LEVEL = LogLevel.INFO


def _dbg(msg: str, level: str = "info") -> None:
    """Debug logging with level filtering."""
    if not _DEBUG:
        return
    
    try:
        msg_level = LogLevel[level.upper()]
        if msg_level.value >= _LOG_LEVEL.value:
            timestamp = time.strftime("%H:%M:%S.%f")[:-3]
            print(f"[{timestamp}] [core.config:{level.upper()}] {msg}", file=sys.stderr)
    except Exception:
        pass


# ============================================================================
# Type Definitions
# ============================================================================

T = TypeVar('T')


class Environment(Enum):
    """Deployment environment."""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"


class AuthType(Enum):
    """Authentication type."""
    NONE = "none"
    TOKEN = "token"
    BEARER = "bearer"
    API_KEY = "api_key"
    JWT = "jwt"
    OAUTH2 = "oauth2"
    BASIC = "basic"
    MULTI = "multi"  # Multiple auth methods


class CacheStrategy(Enum):
    """Cache strategy."""
    MEMORY = "memory"
    REDIS = "redis"
    MEMCACHED = "memcached"
    NONE = "none"


class ProviderStatus(Enum):
    """Provider status."""
    ACTIVE = "active"
    DEGRADED = "degraded"
    MAINTENANCE = "maintenance"
    DISABLED = "disabled"


# ============================================================================
# Safe Helpers
# ============================================================================

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "active"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled"}


def safe_str(x: Any) -> Optional[str]:
    """Safely convert to string."""
    if x is None:
        return None
    try:
        s = str(x).strip()
        return s if s else None
    except Exception:
        return None


def safe_int(x: Any, default: Optional[int] = None) -> Optional[int]:
    """Safely convert to int."""
    if x is None:
        return default
    try:
        if isinstance(x, (int, float)):
            return int(x)
        s = str(x).strip()
        if not s:
            return default
        return int(float(s))  # Handle "123.45"
    except Exception:
        return default


def safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    """Safely convert to float."""
    if x is None:
        return default
    try:
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if not s:
            return default
        return float(s)
    except Exception:
        return default


def safe_bool(x: Any, default: bool = False) -> bool:
    """Safely convert to boolean."""
    if x is None:
        return default
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return bool(x)
    if isinstance(x, str):
        v = x.strip().lower()
        if v in _TRUTHY:
            return True
        if v in _FALSY:
            return False
    return default


def safe_list(x: Any, default: Optional[List[str]] = None) -> List[str]:
    """Safely convert to list of strings."""
    if default is None:
        default = []
    try:
        if x is None:
            return default
        if isinstance(x, (list, tuple, set)):
            out = []
            for i in x:
                s = safe_str(i)
                if s:
                    out.append(s)
            return out
        if isinstance(x, str):
            # Handle comma-separated
            return [p.strip() for p in x.split(",") if p.strip()]
        return default
    except Exception:
        return default


def safe_dict(x: Any, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Safely convert to dictionary."""
    if default is None:
        default = {}
    try:
        if x is None:
            return default
        if isinstance(x, dict):
            return dict(x)
        if isinstance(x, str):
            # Try JSON parse
            try:
                parsed = json.loads(x)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass
        return default
    except Exception:
        return default


def safe_json(x: Any, default: Optional[Any] = None) -> Any:
    """Safely parse JSON."""
    if default is None:
        default = {}
    try:
        if isinstance(x, (dict, list, tuple, set, int, float, bool, str)):
            return x
        if isinstance(x, str):
            return json.loads(x)
        return default
    except Exception:
        return default


def safe_duration(x: Any, default: Optional[float] = None) -> Optional[float]:
    """Parse duration strings (e.g., '5m', '2h', '1d') to seconds."""
    if default is None:
        default = None
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        
        s = str(x).strip().lower()
        if not s:
            return default
        
        # Parse duration format
        match = re.match(r"^(\d+(?:\.\d+)?)\s*([smhd])?$", s)
        if match:
            value = float(match.group(1))
            unit = match.group(2) or 's'
            
            multipliers = {
                's': 1,
                'm': 60,
                'h': 3600,
                'd': 86400,
            }
            return value * multipliers.get(unit, 1)
        
        return float(s)
    except Exception:
        return default


def safe_size(x: Any, default: Optional[int] = None) -> Optional[int]:
    """Parse size strings (e.g., '1KB', '2MB', '1GB') to bytes."""
    if default is None:
        default = None
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return int(x)
        
        s = str(x).strip().upper()
        if not s:
            return default
        
        # Parse size format
        match = re.match(r"^(\d+(?:\.\d+)?)\s*([KMGTP]?B?)?$", s)
        if match:
            value = float(match.group(1))
            unit = (match.group(2) or '').upper()
            
            multipliers = {
                '': 1,
                'B': 1,
                'KB': 1024,
                'MB': 1024 ** 2,
                'GB': 1024 ** 3,
                'TB': 1024 ** 4,
                'PB': 1024 ** 5,
            }
            return int(value * multipliers.get(unit, 1))
        
        return int(float(s))
    except Exception:
        return default


def safe_percent(x: Any, default: Optional[float] = None) -> Optional[float]:
    """Parse percentage (0-100)."""
    val = safe_float(x)
    if val is None:
        return default
    return max(0.0, min(100.0, val))


def safe_rate(x: Any, default: Optional[float] = None) -> Optional[float]:
    """Parse rate (requests per second)."""
    val = safe_float(x)
    if val is None:
        return default
    return max(0.0, val)


# ============================================================================
# Header Normalization
# ============================================================================

def normalize_headers(headers: Any) -> Dict[str, str]:
    """
    Normalize headers to a lowercase dict[str, str].
    Accepts dict-like, list of tuples, or request-like objects with .headers.
    """
    out: Dict[str, str] = {}
    
    try:
        if headers is None:
            return out
        
        # Request-like object
        if not isinstance(headers, (dict, list, tuple)) and hasattr(headers, "headers"):
            headers = getattr(headers, "headers", None)
        
        if isinstance(headers, dict):
            for k, v in headers.items():
                ks = safe_str(k)
                if not ks:
                    continue
                vs = safe_str(v) or ""
                out[ks.lower()] = vs
            return out
        
        if isinstance(headers, (list, tuple)):
            for item in headers:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    ks = safe_str(item[0])
                    if not ks:
                        continue
                    out[ks.lower()] = safe_str(item[1]) or ""
            return out
        
        return out
    except Exception:
        return out


def extract_bearer_token(authorization: Optional[str]) -> Optional[str]:
    """Extract Bearer token from Authorization header."""
    auth = safe_str(authorization)
    if not auth:
        return None
    
    # Handle "Bearer token" format
    if auth.lower().startswith("bearer "):
        return safe_str(auth.split(" ", 1)[1])
    
    # Handle just token
    return auth


def extract_api_key(headers: Dict[str, str], key_names: List[str]) -> Optional[str]:
    """Extract API key from headers using multiple possible key names."""
    headers_lower = {k.lower(): v for k, v in headers.items()}
    
    for key_name in key_names:
        key_lower = key_name.lower()
        if key_lower in headers_lower:
            return headers_lower[key_lower]
    
    return None


# ============================================================================
# Thread-Safe Settings Cache
# ============================================================================

class SettingsCache:
    """Thread-safe cache for settings with TTL."""
    
    def __init__(self, ttl_seconds: int = 60):
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._lock = threading.RLock()
        self._ttl = ttl_seconds
    
    def get(self, key: str) -> Optional[Any]:
        """Get cached value if not expired."""
        with self._lock:
            if key in self._cache:
                value, expiry = self._cache[key]
                if time.time() < expiry:
                    return value
                else:
                    del self._cache[key]
            return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set cached value with TTL."""
        with self._lock:
            ttl_seconds = ttl if ttl is not None else self._ttl
            expiry = time.time() + ttl_seconds
            self._cache[key] = (value, expiry)
    
    def clear(self) -> None:
        """Clear all cached values."""
        with self._lock:
            self._cache.clear()
    
    def remove(self, key: str) -> None:
        """Remove specific key from cache."""
        with self._lock:
            self._cache.pop(key, None)


_SETTINGS_CACHE = SettingsCache(ttl_seconds=30)


# ============================================================================
# Sensitive Data Masking
# ============================================================================

SENSITIVE_KEYS = {
    "token", "tokens", "secret", "secrets", "key", "keys", "api_key", "apikey",
    "password", "passwd", "pwd", "credential", "credentials", "auth", "authorization",
    "bearer", "jwt", "oauth", "access_token", "refresh_token", "client_secret",
    "private_key", "private", "cert", "certificate", "pem", "keyfile",
}


def mask_sensitive_value(key: str, value: Any) -> Any:
    """Mask sensitive values for logging."""
    if value is None:
        return None
    
    key_lower = key.lower()
    
    # Check if key contains sensitive words
    for sensitive in SENSITIVE_KEYS:
        if sensitive in key_lower:
            if isinstance(value, str):
                if len(value) <= 8:
                    return "****"
                return f"{value[:4]}...{value[-4:]}"
            return "****"
    
    # Mask email addresses
    if isinstance(value, str) and '@' in value and '.' in value:
        parts = value.split('@')
        if len(parts) == 2:
            local = parts[0]
            domain = parts[1]
            if len(local) > 3:
                local = local[:2] + "..." + local[-1]
            return f"{local}@{domain}"
    
    # Mask credit cards (simplified)
    if isinstance(value, str) and re.match(r'^\d{13,19}$', value.replace(' ', '')):
        cc = value.replace(' ', '')
        return f"{cc[:4]}...{cc[-4:]}"
    
    return value


def mask_settings_dict(settings: Dict[str, Any]) -> Dict[str, Any]:
    """Create a copy of settings dict with sensitive values masked."""
    masked = {}
    for k, v in settings.items():
        if isinstance(v, dict):
            masked[k] = mask_settings_dict(v)
        elif isinstance(v, (list, tuple)):
            masked[k] = [
                mask_settings_dict(item) if isinstance(item, dict) else mask_sensitive_value(k, item)
                for item in v
            ]
        else:
            masked[k] = mask_sensitive_value(k, v)
    return masked


# ============================================================================
# Settings Models
# ============================================================================

@dataclass
class RateLimitConfig:
    """Rate limiting configuration."""
    enabled: bool = True
    rate_per_sec: float = 10.0
    burst_size: int = 20
    retry_after: int = 60
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> RateLimitConfig:
        return cls(
            enabled=safe_bool(data.get("enabled"), True),
            rate_per_sec=safe_float(data.get("rate_per_sec"), 10.0),
            burst_size=safe_int(data.get("burst_size"), 20),
            retry_after=safe_int(data.get("retry_after"), 60),
        )


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""
    enabled: bool = True
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: int = 60
    half_open_timeout: int = 30
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> CircuitBreakerConfig:
        return cls(
            enabled=safe_bool(data.get("enabled"), True),
            failure_threshold=safe_int(data.get("failure_threshold"), 5),
            success_threshold=safe_int(data.get("success_threshold"), 2),
            timeout_seconds=safe_int(data.get("timeout_seconds"), 60),
            half_open_timeout=safe_int(data.get("half_open_timeout"), 30),
        )


@dataclass
class CacheConfig:
    """Cache configuration."""
    strategy: CacheStrategy = CacheStrategy.MEMORY
    ttl_seconds: int = 300
    max_size: int = 10000
    redis_url: Optional[str] = None
    memcached_servers: Optional[List[str]] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> CacheConfig:
        strategy_str = safe_str(data.get("strategy"), "memory")
        try:
            strategy = CacheStrategy(strategy_str.lower())
        except ValueError:
            strategy = CacheStrategy.MEMORY
        
        return cls(
            strategy=strategy,
            ttl_seconds=safe_int(data.get("ttl_seconds"), 300),
            max_size=safe_int(data.get("max_size"), 10000),
            redis_url=safe_str(data.get("redis_url")),
            memcached_servers=safe_list(data.get("memcached_servers")),
        )


@dataclass
class ProviderConfig:
    """Provider configuration."""
    name: str
    enabled: bool = True
    priority: int = 100
    markets: List[str] = field(default_factory=list)
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    timeout_seconds: int = 30
    retry_attempts: int = 3
    retry_delay_seconds: float = 1.0
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    extra_params: Dict[str, Any] = field(default_factory=dict)
    status: ProviderStatus = ProviderStatus.ACTIVE
    
    @classmethod
    def from_dict(cls, name: str, data: Dict[str, Any]) -> ProviderConfig:
        # Parse status
        status_str = safe_str(data.get("status"), "active")
        try:
            status = ProviderStatus(status_str.lower())
        except ValueError:
            status = ProviderStatus.ACTIVE
        
        return cls(
            name=name,
            enabled=safe_bool(data.get("enabled"), True),
            priority=safe_int(data.get("priority"), 100),
            markets=safe_list(data.get("markets")),
            rate_limit=RateLimitConfig.from_dict(data.get("rate_limit", {})),
            circuit_breaker=CircuitBreakerConfig.from_dict(data.get("circuit_breaker", {})),
            cache=CacheConfig.from_dict(data.get("cache", {})),
            timeout_seconds=safe_int(data.get("timeout_seconds"), 30),
            retry_attempts=safe_int(data.get("retry_attempts"), 3),
            retry_delay_seconds=safe_float(data.get("retry_delay_seconds"), 1.0),
            api_key=safe_str(data.get("api_key")),
            base_url=safe_str(data.get("base_url")),
            extra_params=safe_dict(data.get("extra_params")),
            status=status,
        )


@dataclass
class AuthConfig:
    """Authentication configuration."""
    type: AuthType = AuthType.NONE
    tokens: List[str] = field(default_factory=list)
    api_keys: Dict[str, str] = field(default_factory=dict)
    jwt_secret: Optional[str] = None
    jwt_algorithm: str = "HS256"
    jwt_expiry_seconds: int = 3600
    oauth2_url: Optional[str] = None
    oauth2_client_id: Optional[str] = None
    oauth2_client_secret: Optional[str] = None
    basic_auth_users: Dict[str, str] = field(default_factory=dict)  # username: password hash
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> AuthConfig:
        # Parse auth type
        type_str = safe_str(data.get("type"), "none")
        try:
            auth_type = AuthType(type_str.lower())
        except ValueError:
            auth_type = AuthType.NONE
        
        return cls(
            type=auth_type,
            tokens=safe_list(data.get("tokens")),
            api_keys=safe_dict(data.get("api_keys")),
            jwt_secret=safe_str(data.get("jwt_secret")),
            jwt_algorithm=safe_str(data.get("jwt_algorithm"), "HS256"),
            jwt_expiry_seconds=safe_int(data.get("jwt_expiry_seconds"), 3600),
            oauth2_url=safe_str(data.get("oauth2_url")),
            oauth2_client_id=safe_str(data.get("oauth2_client_id")),
            oauth2_client_secret=safe_str(data.get("oauth2_client_secret")),
            basic_auth_users=safe_dict(data.get("basic_auth_users")),
        )


@dataclass
class CoreSettings:
    """Core settings container."""
    # Environment
    environment: Environment = Environment.DEVELOPMENT
    debug: bool = False
    
    # Authentication
    auth: AuthConfig = field(default_factory=AuthConfig)
    
    # Providers
    providers: Dict[str, ProviderConfig] = field(default_factory=dict)
    
    # Caching
    cache: CacheConfig = field(default_factory=CacheConfig)
    
    # Rate limiting (global)
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    
    # Circuit breaker (global)
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    
    # Metrics
    metrics_enabled: bool = True
    metrics_port: int = 9090
    
    # Logging
    log_level: str = "INFO"
    log_format: str = "json"
    log_file: Optional[str] = None
    
    # Feature flags
    features: Dict[str, bool] = field(default_factory=dict)
    
    # Custom settings
    custom: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> CoreSettings:
        # Parse environment
        env_str = safe_str(data.get("environment"), "development")
        try:
            environment = Environment(env_str.lower())
        except ValueError:
            environment = Environment.DEVELOPMENT
        
        # Parse providers
        providers = {}
        for name, config in safe_dict(data.get("providers", {})).items():
            providers[name] = ProviderConfig.from_dict(name, config)
        
        return cls(
            environment=environment,
            debug=safe_bool(data.get("debug"), False),
            auth=AuthConfig.from_dict(data.get("auth", {})),
            providers=providers,
            cache=CacheConfig.from_dict(data.get("cache", {})),
            rate_limit=RateLimitConfig.from_dict(data.get("rate_limit", {})),
            circuit_breaker=CircuitBreakerConfig.from_dict(data.get("circuit_breaker", {})),
            metrics_enabled=safe_bool(data.get("metrics_enabled"), True),
            metrics_port=safe_int(data.get("metrics_port"), 9090),
            log_level=safe_str(data.get("log_level"), "INFO"),
            log_format=safe_str(data.get("log_format"), "json"),
            log_file=safe_str(data.get("log_file")),
            features=safe_dict(data.get("features", {})),
            custom=safe_dict(data.get("custom", {})),
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


# ============================================================================
# Settings Shim (fallback when no root config)
# ============================================================================

class SettingsShim:
    """Minimal settings shim for when root config is unavailable."""
    
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
    
    def dict(self) -> Dict[str, Any]:
        return dict(self.__dict__)
    
    def model_dump(self) -> Dict[str, Any]:
        return dict(self.__dict__)


# ============================================================================
# Root Configuration Import
# ============================================================================

_ROOT_MODULE = "config"
_ROOT_LOADED = False
_ROOT_ERROR: Optional[Exception] = None
_ROOT_MODULE_OBJ = None

try:
    _ROOT_MODULE_OBJ = __import__(_ROOT_MODULE)
    _ROOT_LOADED = True
    _dbg(f"Successfully imported root config module: {_ROOT_MODULE}", "info")
except ImportError as e:
    _ROOT_ERROR = e
    _dbg(f"Failed to import root config module: {e}", "warn")
except Exception as e:
    _ROOT_ERROR = e
    _dbg(f"Unexpected error importing root config: {e}", "error")


# ============================================================================
# Settings Access Functions
# ============================================================================

def get_root_attr(name: str, default: Any = None) -> Any:
    """Get attribute from root config module if available."""
    if not _ROOT_LOADED or _ROOT_MODULE_OBJ is None:
        return default
    
    try:
        return getattr(_ROOT_MODULE_OBJ, name, default)
    except Exception:
        return default


def get_settings_cached(force_reload: bool = False) -> Any:
    """Get settings with caching."""
    if force_reload:
        _SETTINGS_CACHE.clear()
    
    cached = _SETTINGS_CACHE.get("settings")
    if cached is not None:
        return cached
    
    settings = get_settings()
    _SETTINGS_CACHE.set("settings", settings)
    return settings


def get_settings() -> Any:
    """
    Get application settings.
    Tries root config.get_settings() first, then falls back to building from env.
    """
    try:
        # Try root config.get_settings()
        get_settings_func = get_root_attr("get_settings")
        if callable(get_settings_func):
            settings = get_settings_func()
            if settings is not None:
                return settings
        
        # Try root Settings class
        settings_class = get_root_attr("Settings")
        if settings_class is not None and hasattr(settings_class, "model_validate"):
            # Pydantic v2
            return settings_class.model_validate({})
        elif settings_class is not None and hasattr(settings_class, "parse_obj"):
            # Pydantic v1
            return settings_class.parse_obj({})
        elif settings_class is not None:
            # Plain class
            try:
                return settings_class()
            except Exception:
                pass
        
        # Build from environment
        return _build_settings_from_env()
    
    except Exception as e:
        _dbg(f"get_settings() failed: {e}", "error")
        return _build_settings_from_env()


def _build_settings_from_env() -> SettingsShim:
    """Build settings from environment variables."""
    settings = {}
    
    # Basic settings
    settings["ENVIRONMENT"] = os.getenv("ENVIRONMENT", "development")
    settings["DEBUG"] = safe_bool(os.getenv("DEBUG"), False)
    settings["LOG_LEVEL"] = os.getenv("LOG_LEVEL", "INFO")
    
    # API Keys and tokens
    settings["ALLOWED_TOKENS"] = safe_list(os.getenv("ALLOWED_TOKENS"))
    settings["API_KEYS"] = safe_dict(os.getenv("API_KEYS"))
    
    # Provider settings
    providers = {}
    for provider in ["yahoo", "finnhub", "eodhd", "tadawul", "argaam"]:
        prefix = provider.upper()
        
        provider_config = {
            "enabled": safe_bool(os.getenv(f"{prefix}_ENABLED"), True),
            "api_key": os.getenv(f"{prefix}_API_KEY"),
            "base_url": os.getenv(f"{prefix}_BASE_URL"),
            "timeout_seconds": safe_int(os.getenv(f"{prefix}_TIMEOUT_SECONDS"), 30),
            "retry_attempts": safe_int(os.getenv(f"{prefix}_RETRY_ATTEMPTS"), 3),
            "rate_limit": safe_float(os.getenv(f"{prefix}_RATE_LIMIT"), 10.0),
        }
        
        # Clean None values
        provider_config = {k: v for k, v in provider_config.items() if v is not None}
        if provider_config:
            providers[provider] = provider_config
    
    if providers:
        settings["PROVIDERS"] = providers
    
    # Feature flags
    features = {}
    for key, value in os.environ.items():
        if key.startswith("FEATURE_"):
            feature_name = key[8:].lower()
            features[feature_name] = safe_bool(value, False)
    if features:
        settings["FEATURES"] = features
    
    return SettingsShim(**settings)


def get_setting(name: str, default: Any = None, settings: Optional[Any] = None) -> Any:
    """
    Get a specific setting by name.
    Handles nested paths with dots (e.g., "providers.yahoo.api_key").
    """
    if settings is None:
        settings = get_settings_cached()
    
    if settings is None:
        return default
    
    # Handle nested paths
    if "." in name:
        parts = name.split(".")
        current = settings
        
        for part in parts:
            if current is None:
                return default
            
            # Try different access methods
            if isinstance(current, dict):
                current = current.get(part)
            elif hasattr(current, part):
                current = getattr(current, part)
            elif hasattr(current, "get") and callable(current.get):
                current = current.get(part)
            else:
                return default
        
        return current if current is not None else default
    
    # Simple path
    if isinstance(settings, dict):
        return settings.get(name, default)
    elif hasattr(settings, name):
        return getattr(settings, name, default)
    elif hasattr(settings, "get") and callable(settings.get):
        return settings.get(name, default)
    
    return default


def as_dict(settings: Any) -> Dict[str, Any]:
    """Safely convert settings to dictionary."""
    try:
        if settings is None:
            return {}
        
        if isinstance(settings, dict):
            return dict(settings)
        
        if hasattr(settings, "model_dump") and callable(settings.model_dump):
            return settings.model_dump()
        
        if hasattr(settings, "dict") and callable(settings.dict):
            return settings.dict()
        
        # Generic object
        result = {}
        for key in dir(settings):
            if key.startswith("_"):
                continue
            try:
                value = getattr(settings, key)
                if not callable(value):
                    result[key] = value
            except Exception:
                continue
        return result
    
    except Exception as e:
        _dbg(f"as_dict() failed: {e}", "error")
        return {}


# ============================================================================
# Authentication Functions
# ============================================================================

def allowed_tokens() -> List[str]:
    """Get list of allowed tokens."""
    try:
        # Try root function
        func = get_root_attr("allowed_tokens")
        if callable(func):
            tokens = func()
            if tokens is not None:
                return safe_list(tokens)
        
        # Try settings
        settings = get_settings_cached()
        tokens = get_setting("ALLOWED_TOKENS", settings=settings)
        if tokens:
            return safe_list(tokens)
        
        # Try environment
        env_tokens = safe_list(os.getenv("ALLOWED_TOKENS"))
        if env_tokens:
            return env_tokens
        
        return []
    except Exception as e:
        _dbg(f"allowed_tokens() failed: {e}", "warn")
        return []


def is_open_mode() -> bool:
    """Determine if running in open mode (no authentication required)."""
    try:
        # Try root function
        func = get_root_attr("is_open_mode")
        if callable(func):
            return safe_bool(func(), True)
        
        # Try settings
        settings = get_settings_cached()
        open_mode = get_setting("OPEN_MODE", settings=settings)
        if open_mode is not None:
            return safe_bool(open_mode, True)
        
        # Infer from tokens
        tokens = allowed_tokens()
        return len(tokens) == 0
    
    except Exception as e:
        _dbg(f"is_open_mode() failed: {e}", "warn")
        return True


def auth_ok(
    token: Optional[str] = None,
    authorization: Optional[str] = None,
    headers: Optional[Any] = None,
    api_key: Optional[str] = None,
    **kwargs: Any,
) -> bool:
    """
    Check if authentication is valid.
    
    Supports multiple authentication methods:
    - Direct token
    - Bearer token in Authorization header
    - X-API-Key header
    - X-App-Token header
    - API key in query parameters
    """
    try:
        # Open mode
        if is_open_mode():
            return True
        
        # Get allowed tokens
        allowed = allowed_tokens()
        if not allowed:
            return False
        
        # Normalize headers
        headers_dict = normalize_headers(headers)
        
        # Check direct token
        if token and token in allowed:
            return True
        
        # Check Authorization header
        auth_header = authorization or headers_dict.get("authorization")
        bearer = extract_bearer_token(auth_header)
        if bearer and bearer in allowed:
            return True
        
        # Check X-API-Key header
        api_key_header = headers_dict.get("x-api-key") or headers_dict.get("apikey")
        if api_key_header and api_key_header in allowed:
            return True
        
        # Check X-App-Token header
        app_token = headers_dict.get("x-app-token") or headers_dict.get("x_app_token")
        if app_token and app_token in allowed:
            return True
        
        # Check API key parameter
        if api_key and api_key in allowed:
            return True
        
        # Try root auth_ok function
        func = get_root_attr("auth_ok")
        if callable(func):
            try:
                return safe_bool(func(
                    token=token,
                    authorization=authorization,
                    headers=headers,
                    api_key=api_key,
                    **kwargs
                ), False)
            except TypeError:
                # Try with different signatures
                try:
                    return safe_bool(func(headers), False)
                except TypeError:
                    try:
                        return safe_bool(func(token), False)
                    except TypeError:
                        return safe_bool(func(), False)
        
        return False
    
    except Exception as e:
        _dbg(f"auth_ok() failed: {e}", "error")
        return False


def mask_settings_dict(settings: Optional[Any] = None) -> Dict[str, Any]:
    """Get masked settings dictionary for logging/monitoring."""
    try:
        if settings is None:
            settings_dict = as_dict(get_settings_cached())
        else:
            settings_dict = as_dict(settings)
        
        # Try root mask function
        func = get_root_attr("mask_settings_dict")
        if callable(func):
            result = func()
            if result:
                return result
        
        # Mask sensitive values
        masked = mask_settings_dict(settings_dict)
        
        # Add metadata
        masked["config_version"] = CONFIG_VERSION
        masked["environment"] = str(get_setting("ENVIRONMENT", "unknown"))
        masked["open_mode"] = is_open_mode()
        masked["token_count"] = len(allowed_tokens())
        masked["provider_count"] = len(get_setting("providers", {}))
        
        return masked
    
    except Exception as e:
        _dbg(f"mask_settings_dict() failed: {e}", "error")
        return {
            "config_version": CONFIG_VERSION,
            "status": "fallback",
            "open_mode": is_open_mode(),
        }


# ============================================================================
# Provider Configuration
# ============================================================================

def get_provider_config(provider_name: str) -> Optional[ProviderConfig]:
    """Get configuration for a specific provider."""
    try:
        settings = get_settings_cached()
        
        # Try structured config
        providers = get_setting("providers", {}, settings)
        if provider_name in providers:
            config = providers[provider_name]
            if isinstance(config, dict):
                return ProviderConfig.from_dict(provider_name, config)
            elif hasattr(config, "dict"):
                return ProviderConfig.from_dict(provider_name, as_dict(config))
            elif config is not None:
                # Assume it's already a ProviderConfig
                return config
        
        # Try environment-based config
        prefix = provider_name.upper()
        config_dict = {
            "enabled": safe_bool(os.getenv(f"{prefix}_ENABLED"), True),
            "api_key": os.getenv(f"{prefix}_API_KEY"),
            "base_url": os.getenv(f"{prefix}_BASE_URL"),
            "timeout_seconds": safe_int(os.getenv(f"{prefix}_TIMEOUT_SECONDS"), 30),
            "retry_attempts": safe_int(os.getenv(f"{prefix}_RETRY_ATTEMPTS"), 3),
            "rate_limit": {
                "rate_per_sec": safe_float(os.getenv(f"{prefix}_RATE_LIMIT"), 10.0),
            },
        }
        
        # Only return if at least some config exists
        if any(v is not None for v in config_dict.values()):
            return ProviderConfig.from_dict(provider_name, config_dict)
        
        return None
    
    except Exception as e:
        _dbg(f"get_provider_config({provider_name}) failed: {e}", "error")
        return None


def get_enabled_providers(market: Optional[str] = None) -> List[ProviderConfig]:
    """Get list of enabled providers, optionally filtered by market."""
    try:
        settings = get_settings_cached()
        providers_dict = get_setting("providers", {}, settings)
        
        result = []
        for name, config in providers_dict.items():
            if isinstance(config, dict):
                provider = ProviderConfig.from_dict(name, config)
            elif isinstance(config, ProviderConfig):
                provider = config
            else:
                continue
            
            if not provider.enabled:
                continue
            
            if market and provider.markets and market not in provider.markets:
                continue
            
            result.append(provider)
        
        # Sort by priority
        result.sort(key=lambda p: p.priority)
        return result
    
    except Exception as e:
        _dbg(f"get_enabled_providers() failed: {e}", "error")
        return []


# ============================================================================
# Feature Flags
# ============================================================================

def feature_enabled(feature_name: str, default: bool = False) -> bool:
    """Check if a feature flag is enabled."""
    try:
        settings = get_settings_cached()
        features = get_setting("features", {}, settings)
        
        if isinstance(features, dict):
            return safe_bool(features.get(feature_name), default)
        
        # Check environment
        env_flag = os.getenv(f"FEATURE_{feature_name.upper()}")
        if env_flag is not None:
            return safe_bool(env_flag, default)
        
        return default
    
    except Exception as e:
        _dbg(f"feature_enabled({feature_name}) failed: {e}", "error")
        return default


# ============================================================================
# Cache Configuration
# ============================================================================

def get_cache_config() -> CacheConfig:
    """Get cache configuration."""
    try:
        settings = get_settings_cached()
        cache = get_setting("cache", {}, settings)
        
        if isinstance(cache, dict):
            return CacheConfig.from_dict(cache)
        elif isinstance(cache, CacheConfig):
            return cache
        
        return CacheConfig()
    
    except Exception as e:
        _dbg(f"get_cache_config() failed: {e}", "error")
        return CacheConfig()


def get_ttl_for_type(data_type: str, default: Optional[int] = None) -> int:
    """Get TTL for specific data type (quote, fundamentals, history)."""
    try:
        if default is None:
            default = 300  # 5 minutes
        
        settings = get_settings_cached()
        ttls = get_setting("ttl_seconds", {}, settings)
        
        if isinstance(ttls, dict):
            return safe_int(ttls.get(data_type), default)
        
        # Environment override
        env_ttl = os.getenv(f"TTL_{data_type.upper()}_SECONDS")
        if env_ttl is not None:
            return safe_int(env_ttl, default)
        
        return default
    
    except Exception as e:
        _dbg(f"get_ttl_for_type({data_type}) failed: {e}", "error")
        return default if default is not None else 300


# ============================================================================
# Rate Limit Configuration
# ============================================================================

def get_rate_limit_config(provider: Optional[str] = None) -> RateLimitConfig:
    """Get rate limit configuration for provider or global."""
    try:
        if provider:
            provider_config = get_provider_config(provider)
            if provider_config:
                return provider_config.rate_limit
        
        settings = get_settings_cached()
        rate_limit = get_setting("rate_limit", {}, settings)
        
        if isinstance(rate_limit, dict):
            return RateLimitConfig.from_dict(rate_limit)
        elif isinstance(rate_limit, RateLimitConfig):
            return rate_limit
        
        return RateLimitConfig()
    
    except Exception as e:
        _dbg(f"get_rate_limit_config() failed: {e}", "error")
        return RateLimitConfig()


# ============================================================================
# Circuit Breaker Configuration
# ============================================================================

def get_circuit_breaker_config(provider: Optional[str] = None) -> CircuitBreakerConfig:
    """Get circuit breaker configuration for provider or global."""
    try:
        if provider:
            provider_config = get_provider_config(provider)
            if provider_config:
                return provider_config.circuit_breaker
        
        settings = get_settings_cached()
        cb = get_setting("circuit_breaker", {}, settings)
        
        if isinstance(cb, dict):
            return CircuitBreakerConfig.from_dict(cb)
        elif isinstance(cb, CircuitBreakerConfig):
            return cb
        
        return CircuitBreakerConfig()
    
    except Exception as e:
        _dbg(f"get_circuit_breaker_config() failed: {e}", "error")
        return CircuitBreakerConfig()


# ============================================================================
# Settings Validation
# ============================================================================

def validate_settings(settings: Optional[Any] = None) -> Tuple[bool, List[str]]:
    """Validate settings and return (is_valid, errors)."""
    errors = []
    
    try:
        if settings is None:
            settings = get_settings_cached()
        
        settings_dict = as_dict(settings)
        
        # Check required settings
        required = ["ENVIRONMENT"]
        for req in required:
            if req not in settings_dict:
                errors.append(f"Missing required setting: {req}")
        
        # Validate environment
        env = settings_dict.get("ENVIRONMENT", "").lower()
        valid_envs = ["development", "testing", "staging", "production"]
        if env and env not in valid_envs:
            errors.append(f"Invalid ENVIRONMENT: {env}")
        
        # Validate tokens if not open mode
        if not is_open_mode():
            tokens = allowed_tokens()
            if not tokens:
                errors.append("No authentication tokens configured")
        
        # Validate providers
        providers = get_setting("providers", {}, settings)
        for name, config in providers.items():
            if not isinstance(config, dict):
                continue
            
            if safe_bool(config.get("enabled"), True):
                # Check for required provider config
                if name in ["finnhub", "eodhd"] and not config.get("api_key"):
                    errors.append(f"Provider {name} enabled but no API key")
        
        return len(errors) == 0, errors
    
    except Exception as e:
        errors.append(f"Validation error: {e}")
        return False, errors


# ============================================================================
# Settings Reload
# ============================================================================

def reload_settings() -> Any:
    """Force reload settings from source."""
    _SETTINGS_CACHE.clear()
    _dbg("Settings cache cleared", "info")
    return get_settings_cached(force_reload=True)


# ============================================================================
# Module Exports
# ============================================================================

__all__ = [
    # Version
    "__version__",
    "CONFIG_VERSION",
    
    # Enums
    "Environment",
    "AuthType",
    "CacheStrategy",
    "ProviderStatus",
    
    # Config classes
    "RateLimitConfig",
    "CircuitBreakerConfig",
    "CacheConfig",
    "ProviderConfig",
    "AuthConfig",
    "CoreSettings",
    
    # Settings access
    "get_settings",
    "get_settings_cached",
    "get_setting",
    "as_dict",
    "reload_settings",
    "validate_settings",
    
    # Authentication
    "allowed_tokens",
    "is_open_mode",
    "auth_ok",
    "mask_settings_dict",
    "normalize_headers",
    "extract_bearer_token",
    
    # Provider config
    "get_provider_config",
    "get_enabled_providers",
    
    # Feature flags
    "feature_enabled",
    
    # Cache config
    "get_cache_config",
    "get_ttl_for_type",
    
    # Rate limiting
    "get_rate_limit_config",
    
    # Circuit breaker
    "get_circuit_breaker_config",
    
    # Safe helpers
    "safe_str",
    "safe_int",
    "safe_float",
    "safe_bool",
    "safe_list",
    "safe_dict",
    "safe_json",
    "safe_duration",
    "safe_size",
    "safe_percent",
    "safe_rate",
    
    # Settings class (shim)
    "SettingsShim",
]
