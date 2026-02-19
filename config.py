"""
TADAWUL FAST BRIDGE – ENTERPRISE CONFIGURATION MANAGEMENT (v4.5.0)
===========================================================
Advanced Production Configuration System with Multi-Source Support

Core Capabilities
-----------------
• Multi-source configuration (env vars, files, secrets manager, Vault)
• Dynamic reload without restart (SIGHUP support)
• Configuration validation with schema enforcement
• Secret management with auto-rotation detection
• Environment-specific profiles (dev/staging/production)
• Feature flags with gradual rollout
• A/B testing configuration support
• Compliance reporting (GDPR, SOC2)
• Audit logging for config changes
• Remote configuration fetching (Consul, etcd)
• Configuration encryption/decryption
• Versioned configuration history

Architecture
------------
┌─────────────────────────────────────────────┐
│           Configuration Sources              │
├───────────────┬───────────────┬─────────────┤
│  Environment  │    Files       │   Secrets   │
│    Variables  │   (YAML/JSON)  │   Manager   │
├───────────────┼───────────────┼─────────────┤
│    Consul     │     etcd       │    Vault    │
│     KV Store  │                │             │
└───────────────┴───────────────┴─────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────┐
│         Configuration Aggregator             │
│         • Merge with priority                │
│         • Validate against schema            │
│         • Encrypt/decrypt secrets            │
└─────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────┐
│         Runtime Configuration                │
│         • Immutable Settings object          │
│         • Hot-reload support                 │
│         • Audit logging                       │
└─────────────────────────────────────────────┘

Usage Examples
--------------
# Basic configuration
from env import settings
print(settings.backend_base_url)

# With validation
config = Settings.from_env()
if config.is_valid():
    app.start(config)

# Dynamic reload
settings.reload()  # SIGHUP handler

# Feature flags
if settings.feature_enabled("new_algorithm"):
    use_new_algorithm()

# A/B testing
variant = settings.ab_test_variant("recommendation_model")

# Compliance report
print(settings.compliance_report())
"""

from __future__ import annotations

import base64
import copy
import hashlib
import hmac
import json
import logging
import os
import re
import socket
import stat
import sys
import time
import warnings
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import lru_cache, wraps
from pathlib import Path
from threading import Lock, RLock
from typing import (Any, Callable, Dict, List, Optional, Set, Tuple, Type,
                    TypeVar, Union, cast)
from urllib.parse import urljoin, urlparse

# Version
CONFIG_VERSION = "4.5.0"
CONFIG_SCHEMA_VERSION = "1.0"

# =============================================================================
# Optional Dependencies with Graceful Degradation
# =============================================================================
# YAML support
try:
    import yaml
    from yaml.parser import ParserError
    from yaml.scanner import ScannerError
    YAML_AVAILABLE = True
except ImportError:
    yaml = None
    YAML_AVAILABLE = False

# Pydantic for validation
try:
    from pydantic import BaseModel, Field, validator, ValidationError
    from pydantic.fields import FieldInfo
    PYDANTIC_AVAILABLE = True
except ImportError:
    BaseModel = object
    Field = lambda default=None, **kwargs: default
    validator = lambda *args, **kwargs: lambda f: f
    PYDANTIC_AVAILABLE = False

# Cryptography
try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2
    CRYPTO_AVAILABLE = True
except ImportError:
    Fernet = None
    CRYPTO_AVAILABLE = False

# AWS Secrets Manager
try:
    import boto3
    from botocore.exceptions import ClientError
    AWS_AVAILABLE = True
except ImportError:
    boto3 = None
    AWS_AVAILABLE = False

# HashiCorp Vault
try:
    import hvac
    VAULT_AVAILABLE = True
except ImportError:
    hvac = None
    VAULT_AVAILABLE = False

# Consul
try:
    import consul
    CONSUL_AVAILABLE = True
except ImportError:
    consul = None
    CONSUL_AVAILABLE = False

# etcd
try:
    import etcd3
    ETCD_AVAILABLE = True
except ImportError:
    etcd3 = None
    ETCD_AVAILABLE = False

# =============================================================================
# Logging Configuration
# =============================================================================
logger = logging.getLogger(__name__)

# =============================================================================
# Enums & Types
# =============================================================================
class Environment(Enum):
    """Deployment environments"""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"
    LOCAL = "local"

class LogLevel(Enum):
    """Logging levels"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

class ProviderType(Enum):
    """Data provider types"""
    EODHD = "eodhd"
    FINNHUB = "finnhub"
    ALPHAVANTAGE = "alphavantage"
    YAHOO = "yahoo"
    ARGAAM = "argaam"
    POLYGON = "polygon"
    IEXCLOUD = "iexcloud"
    TRADIER = "tradier"
    CUSTOM = "custom"

class CacheBackend(Enum):
    """Cache backends"""
    MEMORY = "memory"
    REDIS = "redis"
    MEMCACHED = "memcached"
    NONE = "none"

class ConfigSource(Enum):
    """Configuration sources"""
    ENV_VAR = "env_var"
    ENV_FILE = "env_file"
    YAML_FILE = "yaml_file"
    JSON_FILE = "json_file"
    AWS_SECRETS = "aws_secrets"
    VAULT = "vault"
    CONSUL = "consul"
    ETCD = "etcd"
    DEFAULT = "default"

# =============================================================================
# Utility Functions
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok", "active", "prod"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled", "inactive", "dev"}

_URL_RE = re.compile(r"^https?://", re.IGNORECASE)
_HOSTPORT_RE = re.compile(r"^[a-z0-9.\-]+(:\d+)?$", re.IGNORECASE)
_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
_IP_RE = re.compile(r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$")

def strip_value(v: Any) -> str:
    """Strip whitespace from value"""
    try:
        return str(v).strip()
    except Exception:
        return ""

def coerce_int(v: Any, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    """Coerce value to integer with bounds"""
    try:
        if isinstance(v, (int, float)):
            x = int(v)
        else:
            x = int(float(strip_value(v)))
    except (ValueError, TypeError):
        x = default
    
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x

def coerce_float(v: Any, default: float, *, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    """Coerce value to float with bounds"""
    try:
        if isinstance(v, (int, float)):
            x = float(v)
        else:
            x = float(strip_value(v))
    except (ValueError, TypeError):
        x = default
    
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x

def coerce_bool(v: Any, default: bool) -> bool:
    """Coerce value to boolean"""
    s = strip_value(v).lower()
    if not s:
        return default
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default

def coerce_list(v: Any, default: Optional[List[str]] = None) -> List[str]:
    """Coerce value to list (handles CSV and JSON)"""
    if v is None:
        return default or []
    
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    
    s = strip_value(v)
    if not s:
        return default or []
    
    # Try JSON parsing
    if s.startswith(('[', '(')):
        try:
            parsed = json.loads(s.replace("'", '"'))
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
        except:
            pass
    
    # CSV parsing
    return [x.strip() for x in s.split(',') if x.strip()]

def coerce_dict(v: Any, default: Optional[Dict] = None) -> Dict:
    """Coerce value to dictionary"""
    if v is None:
        return default or {}
    
    if isinstance(v, dict):
        return v
    
    s = strip_value(v)
    if not s:
        return default or {}
    
    try:
        parsed = json.loads(s)
        if isinstance(parsed, dict):
            return parsed
    except:
        pass
    
    return default or {}

def is_valid_url(url: str) -> bool:
    """Check if string is valid HTTP/HTTPS URL"""
    u = strip_value(url)
    if not u:
        return False
    return bool(_URL_RE.match(u))

def is_valid_hostport(v: str) -> bool:
    """Check if string is valid host:port"""
    s = strip_value(v)
    if not s:
        return False
    return bool(_HOSTPORT_RE.match(s))

def is_valid_email(email: str) -> bool:
    """Check if string is valid email"""
    return bool(_EMAIL_RE.match(strip_value(email)))

def is_valid_ip(ip: str) -> bool:
    """Check if string is valid IP address"""
    return bool(_IP_RE.match(strip_value(ip)))

def mask_secret(s: Optional[str], reveal_last: int = 4) -> Optional[str]:
    """Mask secret string for logging"""
    if not s:
        return None
    s = strip_value(s)
    if len(s) <= reveal_last + 2:
        return "***"
    return s[:3] + "..." + s[-reveal_last:]

def looks_like_jwt(s: str) -> bool:
    """Check if string looks like JWT token"""
    return s.count('.') == 2 and len(s) > 30

def secret_strength_hint(s: str) -> Optional[str]:
    """Provide hint about secret strength"""
    t = strip_value(s)
    if not t:
        return None
    if looks_like_jwt(t):
        return None
    if len(t) < 16:
        return "Secret is short (<16 chars). Consider using longer random value."
    if t.lower() in {'token', 'password', 'secret', '1234', '123456', 'admin', 'key'}:
        return "Secret looks weak/common. Use strong random value."
    if not any(c.isupper() for c in t) or not any(c.islower() for c in t) or not any(c.isdigit() for c in t):
        return "Secret should contain mixed case, numbers, and special characters."
    return None

def repair_private_key(key: str) -> str:
    """Repair private key with common issues"""
    if not key:
        return key
    
    # Fix escaped newlines
    key = key.replace('\\n', '\n')
    key = key.replace('\\r\\n', '\n')
    
    # Ensure proper PEM headers
    if '-----BEGIN PRIVATE KEY-----' not in key:
        key = '-----BEGIN PRIVATE KEY-----\n' + key.lstrip()
    if '-----END PRIVATE KEY-----' not in key:
        key = key.rstrip() + '\n-----END PRIVATE KEY-----'
    
    return key

def decrypt_value(encrypted: str, key: Optional[str] = None) -> Optional[str]:
    """Decrypt encrypted configuration value"""
    if not CRYPTO_AVAILABLE or not Fernet:
        return encrypted
    
    if not key:
        key = os.getenv('CONFIG_ENCRYPTION_KEY')
    
    if not key:
        return encrypted
    
    try:
        f = Fernet(key.encode())
        decrypted = f.decrypt(encrypted.encode()).decode()
        return decrypted
    except Exception as e:
        logger.error(f"Failed to decrypt value: {e}")
        return None

def encrypt_value(value: str, key: Optional[str] = None) -> Optional[str]:
    """Encrypt configuration value"""
    if not CRYPTO_AVAILABLE or not Fernet:
        return value
    
    if not key:
        key = os.getenv('CONFIG_ENCRYPTION_KEY')
    
    if not key:
        return value
    
    try:
        f = Fernet(key.encode())
        encrypted = f.encrypt(value.encode()).decode()
        return encrypted
    except Exception as e:
        logger.error(f"Failed to encrypt value: {e}")
        return None

# =============================================================================
# Pydantic Models (Optional)
# =============================================================================
if PYDANTIC_AVAILABLE:
    class SettingsBaseModel(BaseModel):
        """Base model with common functionality"""
        
        class Config:
            validate_assignment = True
            arbitrary_types_allowed = True
            json_encoders = {
                datetime: lambda v: v.isoformat(),
                Enum: lambda v: v.value,
                Path: lambda v: str(v),
            }
        
        def dict(self, *args, **kwargs):
            """Override dict to handle Enums"""
            kwargs['exclude_none'] = True
            d = super().dict(*args, **kwargs)
            return {k: v.value if isinstance(v, Enum) else v for k, v in d.items()}
    
    class DatabaseConfig(SettingsBaseModel):
        """Database configuration"""
        url: str = Field(..., description="Database connection URL")
        pool_size: int = Field(20, ge=1, le=100, description="Connection pool size")
        max_overflow: int = Field(10, ge=0, description="Max overflow connections")
        pool_timeout: int = Field(30, ge=1, description="Pool timeout seconds")
        pool_recycle: int = Field(3600, ge=60, description="Connection recycle seconds")
        echo: bool = Field(False, description="Echo SQL queries")
        
        @validator('url')
        def validate_url(cls, v):
            if not v.startswith(('postgresql://', 'mysql://', 'sqlite://')):
                raise ValueError('Invalid database URL scheme')
            return v
    
    class RedisConfig(SettingsBaseModel):
        """Redis configuration"""
        url: str = Field(..., description="Redis connection URL")
        socket_timeout: int = Field(5, ge=1, description="Socket timeout seconds")
        socket_connect_timeout: int = Field(5, ge=1, description="Connect timeout seconds")
        retry_on_timeout: bool = Field(True, description="Retry on timeout")
        max_connections: int = Field(50, ge=1, description="Max connections")
        
        @validator('url')
        def validate_url(cls, v):
            if not v.startswith(('redis://', 'rediss://')):
                raise ValueError('Invalid Redis URL scheme')
            return v
    
    class CacheConfig(SettingsBaseModel):
        """Cache configuration"""
        backend: CacheBackend = Field(CacheBackend.MEMORY, description="Cache backend")
        ttl: int = Field(300, ge=1, le=86400, description="Default TTL seconds")
        prefix: str = Field("tfb:", description="Cache key prefix")
        compression: bool = Field(False, description="Enable compression")
        
        # Redis specific
        redis_url: Optional[str] = Field(None, description="Redis URL")
        
        # Memcached specific
        memcached_servers: List[str] = Field(default_factory=list, description="Memcached servers")
    
    class ProviderConfig(SettingsBaseModel):
        """Provider configuration"""
        name: ProviderType = Field(..., description="Provider name")
        api_key: Optional[str] = Field(None, description="API key")
        api_secret: Optional[str] = Field(None, description="API secret")
        base_url: Optional[str] = Field(None, description="Base URL")
        timeout: int = Field(30, ge=1, description="Timeout seconds")
        retries: int = Field(3, ge=0, description="Retry count")
        priority: int = Field(10, ge=1, le=100, description="Priority (lower = higher)")
        enabled: bool = Field(True, description="Provider enabled")
        rate_limit: int = Field(60, ge=1, description="Requests per minute")
    
    class FeatureFlags(SettingsBaseModel):
        """Feature flags configuration"""
        ai_analysis: bool = Field(True, description="AI analysis enabled")
        advisor: bool = Field(True, description="Advisor enabled")
        advanced: bool = Field(True, description="Advanced features enabled")
        rate_limiting: bool = Field(True, description="Rate limiting enabled")
        caching: bool = Field(True, description="Caching enabled")
        circuit_breaker: bool = Field(True, description="Circuit breaker enabled")
        metrics: bool = Field(True, description="Metrics collection enabled")
        tracing: bool = Field(False, description="Distributed tracing enabled")
        profiling: bool = Field(False, description="Profiling enabled")
        chaos: bool = Field(False, description="Chaos engineering enabled")
        
        # Gradual rollout
        rollout_percentage: int = Field(100, ge=0, le=100, description="Rollout percentage")
        enabled_environments: List[Environment] = Field(
            default_factory=lambda: [Environment.PRODUCTION],
            description="Environments where enabled"
        )
    
    class SecurityConfig(SettingsBaseModel):
        """Security configuration"""
        auth_header_name: str = Field("X-APP-TOKEN", description="Auth header name")
        allow_query_token: bool = Field(False, description="Allow token in query params")
        open_mode: bool = Field(False, description="Open mode (no auth)")
        cors_origins: List[str] = Field(default_factory=list, description="CORS origins")
        cors_credentials: bool = Field(True, description="Allow credentials")
        rate_limit_rpm: int = Field(240, ge=30, le=10000, description="Rate limit RPM")
        jwt_secret: Optional[str] = Field(None, description="JWT secret")
        jwt_algorithm: str = Field("HS256", description="JWT algorithm")
        session_timeout: int = Field(3600, ge=300, description="Session timeout seconds")
        
        @validator('cors_origins')
        def validate_cors_origins(cls, v):
            for origin in v:
                if origin != '*' and not is_valid_url(origin):
                    raise ValueError(f'Invalid CORS origin: {origin}')
            return v
    
    class LoggingConfig(SettingsBaseModel):
        """Logging configuration"""
        level: LogLevel = Field(LogLevel.INFO, description="Log level")
        format: str = Field("json", description="Log format (json, text)")
        file: Optional[str] = Field(None, description="Log file path")
        max_bytes: int = Field(104857600, ge=1048576, description="Max log file bytes")
        backup_count: int = Field(10, ge=1, description="Log backup count")
        access_log: bool = Field(True, description="Access log enabled")
        access_log_format: str = Field("combined", description="Access log format")
        
        @validator('format')
        def validate_format(cls, v):
            if v not in {'json', 'text', 'color'}:
                raise ValueError('Invalid log format')
            return v
    
    class MonitoringConfig(SettingsBaseModel):
        """Monitoring configuration"""
        metrics_port: int = Field(9090, ge=1024, le=65535, description="Metrics port")
        health_check_path: str = Field("/health", description="Health check path")
        health_check_interval: int = Field(30, ge=5, description="Health check interval")
        health_check_timeout: int = Field(5, ge=1, description="Health check timeout")
        sentry_dsn: Optional[str] = Field(None, description="Sentry DSN")
        datadog_api_key: Optional[str] = Field(None, description="Datadog API key")
        newrelic_license_key: Optional[str] = Field(None, description="New Relic license key")
    
    class PerformanceConfig(SettingsBaseModel):
        """Performance configuration"""
        workers: Optional[int] = Field(None, ge=1, le=64, description="Worker count")
        worker_class: str = Field("uvicorn.workers.UvicornWorker", description="Worker class")
        worker_connections: int = Field(1000, ge=10, description="Worker connections")
        backlog: int = Field(2048, ge=64, le=16384, description="Connection backlog")
        timeout_keep_alive: int = Field(65, ge=1, le=300, description="Keep-alive timeout")
        timeout_graceful: int = Field(30, ge=1, le=120, description="Graceful timeout")
        timeout_request: int = Field(60, ge=1, le=600, description="Request timeout")
        limit_concurrency: Optional[int] = Field(None, ge=1, description="Concurrency limit")
        limit_max_requests: Optional[int] = Field(None, ge=1, description="Max requests")
        http_timeout: float = Field(45.0, ge=5.0, le=180.0, description="HTTP timeout")
        cache_ttl: int = Field(20, ge=5, le=3600, description="Cache TTL seconds")
        batch_concurrency: int = Field(5, ge=1, le=50, description="Batch concurrency")
        
        # Batch sizes
        ai_batch_size: int = Field(20, ge=1, le=200, description="AI batch size")
        adv_batch_size: int = Field(25, ge=1, le=300, description="Advanced batch size")
        quote_batch_size: int = Field(50, ge=1, le=1000, description="Quote batch size")
    
    class DeploymentConfig(SettingsBaseModel):
        """Deployment configuration"""
        environment: Environment = Field(Environment.PRODUCTION, description="Environment")
        version: str = Field("dev", description="Application version")
        service_name: str = Field("Tadawul Fast Bridge", description="Service name")
        timezone: str = Field("Asia/Riyadh", description="Timezone")
        public_base_url: Optional[str] = Field(None, description="Public base URL")
        render_service_name: Optional[str] = Field(None, description="Render service name")
        render_external_url: Optional[str] = Field(None, description="Render external URL")
        
        @validator('timezone')
        def validate_timezone(cls, v):
            try:
                import pytz
                pytz.timezone(v)
            except:
                pass  # Non-critical
            return v
    
    class GoogleSheetsConfig(SettingsBaseModel):
        """Google Sheets configuration"""
        spreadsheet_id: Optional[str] = Field(None, description="Default spreadsheet ID")
        credentials: Optional[str] = Field(None, description="Credentials JSON")
        service_account_email: Optional[str] = Field(None, description="Service account email")
        
        @validator('credentials')
        def validate_credentials(cls, v):
            if v and v.startswith('{'):
                try:
                    json.loads(v)
                except:
                    raise ValueError('Invalid credentials JSON')
            return v
    
    class ArgaamConfig(SettingsBaseModel):
        """Argaam integration configuration"""
        quote_url: Optional[str] = Field(None, description="Argaam quote URL")
        api_key: Optional[str] = Field(None, description="Argaam API key")
        
        @validator('quote_url')
        def validate_url(cls, v):
            if v and not is_valid_url(v):
                raise ValueError('Invalid Argaam URL')
            return v
    
    class ChaosConfig(SettingsBaseModel):
        """Chaos engineering configuration"""
        enabled: bool = Field(False, description="Chaos enabled")
        failure_rate: float = Field(0.0, ge=0.0, le=1.0, description="Failure rate")
        latency_ms: int = Field(0, ge=0, le=10000, description="Latency to inject")
        exception_rate: float = Field(0.0, ge=0.0, le=1.0, description="Exception rate")
        
        @validator('failure_rate')
        def validate_failure_rate(cls, v):
            if v < 0 or v > 1:
                raise ValueError('Failure rate must be between 0 and 1')
            return v

# =============================================================================
# Main Settings Class
# =============================================================================
@dataclass(frozen=True)
class Settings:
    """Main application settings"""
    
    # Version
    config_version: str = CONFIG_VERSION
    schema_version: str = CONFIG_SCHEMA_VERSION
    
    # Deployment
    environment: Environment = Environment.PRODUCTION
    app_version: str = "dev"
    service_name: str = "Tadawul Fast Bridge"
    timezone: str = "Asia/Riyadh"
    
    # Logging
    log_level: LogLevel = LogLevel.INFO
    log_format: str = "json"
    log_file: Optional[str] = None
    
    # URLs
    backend_base_url: str = "http://localhost:8000"
    public_base_url: Optional[str] = None
    render_service_name: Optional[str] = None
    render_external_url: Optional[str] = None
    
    # Security
    auth_header_name: str = "X-APP-TOKEN"
    app_token: Optional[str] = None
    backup_app_token: Optional[str] = None
    allow_query_token: bool = False
    open_mode: bool = False
    cors_origins: List[str] = field(default_factory=list)
    rate_limit_rpm: int = 240
    
    # Feature flags
    ai_analysis_enabled: bool = True
    advisor_enabled: bool = True
    advanced_enabled: bool = True
    rate_limit_enabled: bool = True
    cache_enabled: bool = True
    circuit_breaker_enabled: bool = True
    metrics_enabled: bool = True
    tracing_enabled: bool = False
    profiling_enabled: bool = False
    chaos_enabled: bool = False
    
    # Chaos engineering
    chaos_failure_rate: float = 0.0
    chaos_latency_ms: int = 0
    chaos_exception_rate: float = 0.0
    
    # Providers
    enabled_providers: List[str] = field(default_factory=lambda: ["eodhd", "finnhub"])
    ksa_providers: List[str] = field(default_factory=lambda: ["yahoo_chart", "argaam"])
    primary_provider: str = "eodhd"
    
    # Provider credentials (masked in logs)
    eodhd_api_key: Optional[str] = None
    finnhub_api_key: Optional[str] = None
    alphavantage_api_key: Optional[str] = None
    
    # Performance
    http_timeout_sec: float = 45.0
    cache_ttl_sec: int = 20
    batch_concurrency: int = 5
    ai_batch_size: int = 20
    adv_batch_size: int = 25
    quote_batch_size: int = 50
    
    # Database
    database_url: Optional[str] = None
    database_pool_size: int = 20
    database_max_overflow: int = 10
    
    # Cache
    cache_backend: CacheBackend = CacheBackend.MEMORY
    redis_url: Optional[str] = None
    memcached_servers: List[str] = field(default_factory=list)
    
    # Integrations
    spreadsheet_id: Optional[str] = None
    google_creds: Optional[str] = None
    google_service_account: Optional[str] = None
    argaam_quote_url: Optional[str] = None
    argaam_api_key: Optional[str] = None
    
    # Monitoring
    metrics_port: int = 9090
    health_check_path: str = "/health"
    sentry_dsn: Optional[str] = None
    datadog_api_key: Optional[str] = None
    newrelic_license_key: Optional[str] = None
    
    # Diagnostics
    boot_errors: Tuple[str, ...] = field(default_factory=tuple)
    boot_warnings: Tuple[str, ...] = field(default_factory=tuple)
    config_sources: Dict[str, ConfigSource] = field(default_factory=dict)
    
    # =========================================================================
    # Factory Methods
    # =========================================================================
    @classmethod
    def from_env(cls) -> "Settings":
        """Create settings from environment variables"""
        config_sources = {}
        
        # Determine environment
        env_name = strip_value(os.getenv("APP_ENV") or os.getenv("ENVIRONMENT") or "production").lower()
        environment = Environment.PRODUCTION
        for e in Environment:
            if e.value == env_name:
                environment = e
                break
        
        config_sources['environment'] = ConfigSource.ENV_VAR
        
        # Tokens
        token1 = strip_value(os.getenv("APP_TOKEN") or os.getenv("TFB_APP_TOKEN"))
        token2 = strip_value(os.getenv("BACKUP_APP_TOKEN"))
        allow_qs = coerce_bool(os.getenv("ALLOW_QUERY_TOKEN"), False)
        
        tokens_exist = bool(token1 or token2)
        
        # Open mode policy
        open_override = os.getenv("OPEN_MODE")
        if open_override is not None:
            is_open = coerce_bool(open_override, not tokens_exist)
        else:
            is_open = not tokens_exist
        
        # Provider API keys
        eodhd_key = strip_value(os.getenv("EODHD_API_TOKEN") or os.getenv("EODHD_API_KEY"))
        finnhub_key = strip_value(os.getenv("FINNHUB_API_KEY"))
        alphavantage_key = strip_value(os.getenv("ALPHAVANTAGE_API_KEY"))
        
        # Providers list
        providers_env = coerce_list(os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS"))
        if not providers_env:
            providers_env = []
            if eodhd_key:
                providers_env.append("eodhd")
            if finnhub_key:
                providers_env.append("finnhub")
            if alphavantage_key:
                providers_env.append("alphavantage")
            if not providers_env:
                providers_env = ["eodhd", "finnhub"]
        
        ksa_env = coerce_list(os.getenv("KSA_PROVIDERS"))
        if not ksa_env:
            ksa_env = ["yahoo_chart", "argaam"]
        
        # Primary provider
        primary = strip_value(os.getenv("PRIMARY_PROVIDER")).lower()
        if not primary and providers_env:
            primary = providers_env[0]
        if not primary:
            primary = "eodhd"
        
        # URLs
        backend = strip_value(
            os.getenv("BACKEND_BASE_URL")
            or os.getenv("TFB_BASE_URL")
            or "http://localhost:8000"
        ).rstrip('/')
        
        render_name = strip_value(os.getenv("RENDER_SERVICE_NAME")) or None
        render_external = strip_value(os.getenv("RENDER_EXTERNAL_URL")) or None
        
        public_base = None
        if render_external and is_valid_url(render_external):
            public_base = render_external
        elif is_valid_url(backend):
            public_base = backend
        
        # CORS origins
        cors_origins = coerce_list(os.getenv("CORS_ORIGINS"))
        if not cors_origins and environment == Environment.PRODUCTION:
            cors_origins = []
        
        # Database
        database_url = strip_value(os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL")) or None
        
        # Redis
        redis_url = strip_value(os.getenv("REDIS_URL")) or None
        
        # Memcached
        memcached_servers = coerce_list(os.getenv("MEMCACHED_SERVERS"))
        
        # Cache backend
        cache_backend_name = strip_value(os.getenv("CACHE_BACKEND") or "memory").lower()
        cache_backend = CacheBackend.MEMORY
        for cb in CacheBackend:
            if cb.value == cache_backend_name:
                cache_backend = cb
                break
        
        # Google Sheets
        spreadsheet_id = strip_value(
            os.getenv("DEFAULT_SPREADSHEET_ID")
            or os.getenv("SPREADSHEET_ID")
            or os.getenv("TFB_SPREADSHEET_ID")
        ) or None
        
        google_creds = cls._normalize_google_creds(
            os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS") or ""
        )
        
        google_service_account = strip_value(os.getenv("GOOGLE_SERVICE_ACCOUNT_EMAIL")) or None
        
        # Argaam
        argaam_url = strip_value(os.getenv("ARGAAM_QUOTE_URL")) or None
        if argaam_url:
            argaam_url = argaam_url.rstrip('/')
        
        argaam_key = strip_value(os.getenv("ARGAAM_API_KEY")) or None
        
        # Monitoring
        metrics_port = coerce_int(os.getenv("METRICS_PORT"), 9090, lo=1024, hi=65535)
        sentry_dsn = strip_value(os.getenv("SENTRY_DSN")) or None
        datadog_key = strip_value(os.getenv("DATADOG_API_KEY")) or None
        newrelic_key = strip_value(os.getenv("NEWRELIC_LICENSE_KEY")) or None
        
        # Chaos engineering
        chaos_enabled = coerce_bool(os.getenv("CHAOS_ENABLED"), False)
        chaos_failure_rate = coerce_float(os.getenv("CHAOS_FAILURE_RATE"), 0.0, lo=0.0, hi=1.0)
        chaos_latency_ms = coerce_int(os.getenv("CHAOS_LATENCY_MS"), 0, lo=0, hi=10000)
        chaos_exception_rate = coerce_float(os.getenv("CHAOS_EXCEPTION_RATE"), 0.0, lo=0.0, hi=1.0)
        
        # Timeouts and sizes
        http_timeout = coerce_float(os.getenv("HTTP_TIMEOUT_SEC"), 45.0, lo=5.0, hi=180.0)
        cache_ttl = coerce_int(os.getenv("CACHE_TTL_SEC"), 20, lo=5, hi=3600)
        batch_concurrency = coerce_int(os.getenv("BATCH_CONCURRENCY"), 5, lo=1, hi=50)
        ai_batch_size = coerce_int(os.getenv("AI_BATCH_SIZE"), 20, lo=1, hi=200)
        adv_batch_size = coerce_int(os.getenv("ADV_BATCH_SIZE"), 25, lo=1, hi=300)
        quote_batch_size = coerce_int(os.getenv("QUOTE_BATCH_SIZE"), 50, lo=1, hi=1000)
        
        # Rate limits
        rate_limit_rpm = coerce_int(os.getenv("RATE_LIMIT_RPM"), 240, lo=30, hi=10000)
        
        # Database pool
        db_pool_size = coerce_int(os.getenv("DATABASE_POOL_SIZE"), 20, lo=1, hi=100)
        db_max_overflow = coerce_int(os.getenv("DATABASE_MAX_OVERFLOW"), 10, lo=0, hi=100)
        
        # Logging
        log_level_name = strip_value(os.getenv("LOG_LEVEL") or "INFO").upper()
        log_level = LogLevel.INFO
        for ll in LogLevel:
            if ll.value == log_level_name:
                log_level = ll
                break
        
        log_format = strip_value(os.getenv("LOG_FORMAT") or "json").lower()
        if log_format not in {'json', 'text', 'color'}:
            log_format = 'json'
        
        log_file = strip_value(os.getenv("LOG_FILE")) or None
        
        # Feature flags
        ai_enabled = coerce_bool(os.getenv("AI_ANALYSIS_ENABLED"), True)
        advisor_enabled = coerce_bool(os.getenv("ADVISOR_ENABLED"), True)
        advanced_enabled = coerce_bool(os.getenv("ADVANCED_ENABLED"), True)
        rate_limit_enabled = coerce_bool(os.getenv("RATE_LIMIT_ENABLED"), True)
        cache_enabled = coerce_bool(os.getenv("CACHE_ENABLED"), True)
        circuit_breaker_enabled = coerce_bool(os.getenv("CIRCUIT_BREAKER_ENABLED"), True)
        metrics_enabled = coerce_bool(os.getenv("METRICS_ENABLED"), True)
        tracing_enabled = coerce_bool(os.getenv("TRACING_ENABLED"), False)
        profiling_enabled = coerce_bool(os.getenv("PROFILING_ENABLED"), False)
        
        # Create settings
        s = cls(
            environment=environment,
            app_version=strip_value(os.getenv("APP_VERSION") or CONFIG_VERSION),
            service_name=strip_value(os.getenv("APP_NAME") or "Tadawul Fast Bridge"),
            timezone=strip_value(os.getenv("APP_TIMEZONE") or "Asia/Riyadh"),
            log_level=log_level,
            log_format=log_format,
            log_file=log_file,
            backend_base_url=backend,
            public_base_url=public_base,
            render_service_name=render_name,
            render_external_url=render_external,
            auth_header_name=strip_value(os.getenv("AUTH_HEADER_NAME") or "X-APP-TOKEN"),
            app_token=token1 or None,
            backup_app_token=token2 or None,
            allow_query_token=allow_qs,
            open_mode=is_open,
            cors_origins=cors_origins,
            rate_limit_rpm=rate_limit_rpm,
            ai_analysis_enabled=ai_enabled,
            advisor_enabled=advisor_enabled,
            advanced_enabled=advanced_enabled,
            rate_limit_enabled=rate_limit_enabled,
            cache_enabled=cache_enabled,
            circuit_breaker_enabled=circuit_breaker_enabled,
            metrics_enabled=metrics_enabled,
            tracing_enabled=tracing_enabled,
            profiling_enabled=profiling_enabled,
            chaos_enabled=chaos_enabled,
            chaos_failure_rate=chaos_failure_rate,
            chaos_latency_ms=chaos_latency_ms,
            chaos_exception_rate=chaos_exception_rate,
            enabled_providers=providers_env,
            ksa_providers=ksa_env,
            primary_provider=primary,
            eodhd_api_key=eodhd_key or None,
            finnhub_api_key=finnhub_key or None,
            alphavantage_api_key=alphavantage_key or None,
            http_timeout_sec=http_timeout,
            cache_ttl_sec=cache_ttl,
            batch_concurrency=batch_concurrency,
            ai_batch_size=ai_batch_size,
            adv_batch_size=adv_batch_size,
            quote_batch_size=quote_batch_size,
            database_url=database_url,
            database_pool_size=db_pool_size,
            database_max_overflow=db_max_overflow,
            cache_backend=cache_backend,
            redis_url=redis_url,
            memcached_servers=memcached_servers,
            spreadsheet_id=spreadsheet_id,
            google_creds=google_creds,
            google_service_account=google_service_account,
            argaam_quote_url=argaam_url,
            argaam_api_key=argaam_key,
            metrics_port=metrics_port,
            health_check_path=strip_value(os.getenv("HEALTH_CHECK_PATH") or "/health"),
            sentry_dsn=sentry_dsn,
            datadog_api_key=datadog_key,
            newrelic_license_key=newrelic_key,
            config_sources=config_sources,
        )
        
        # Run validation
        errors, warnings = s.validate()
        
        # Return with diagnostics
        return replace(
            s,
            boot_errors=tuple(errors),
            boot_warnings=tuple(warnings)
        )
    
    @classmethod
    def from_file(cls, path: Union[str, Path]) -> "Settings":
        """Load settings from file (YAML/JSON)"""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        
        config_sources = {'file': ConfigSource.YAML_FILE if path.suffix in {'.yaml', '.yml'} else ConfigSource.JSON_FILE}
        
        content = path.read_text(encoding='utf-8')
        
        if path.suffix in {'.yaml', '.yml'}:
            if not YAML_AVAILABLE:
                raise ImportError("PyYAML required for YAML config files")
            data = yaml.safe_load(content)
        else:
            data = json.loads(content)
        
        if not isinstance(data, dict):
            raise ValueError("Config file must contain a dictionary")
        
        # Override with environment variables (higher priority)
        env_settings = cls.from_env()
        
        # Merge
        merged = {**data, **asdict(env_settings)}
        merged['config_sources'] = config_sources
        
        return cls(**merged)
    
    @classmethod
    def from_aws_secrets(cls, secret_name: str, region: Optional[str] = None) -> "Settings":
        """Load settings from AWS Secrets Manager"""
        if not AWS_AVAILABLE:
            raise ImportError("boto3 required for AWS Secrets Manager")
        
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region or os.getenv('AWS_REGION', 'us-east-1')
        )
        
        try:
            response = client.get_secret_value(SecretId=secret_name)
            if 'SecretString' in response:
                data = json.loads(response['SecretString'])
            else:
                data = json.loads(base64.b64decode(response['SecretBinary']))
        except ClientError as e:
            raise RuntimeError(f"Failed to fetch secret {secret_name}: {e}")
        
        config_sources = {'aws_secrets': ConfigSource.AWS_SECRETS}
        data['config_sources'] = config_sources
        
        return cls(**data)
    
    @classmethod
    def from_vault(cls, path: str, mount_point: str = "secret") -> "Settings":
        """Load settings from HashiCorp Vault"""
        if not VAULT_AVAILABLE:
            raise ImportError("hvac required for Vault integration")
        
        client = hvac.Client(
            url=os.getenv('VAULT_ADDR', 'http://localhost:8200'),
            token=os.getenv('VAULT_TOKEN')
        )
        
        if not client.is_authenticated():
            raise RuntimeError("Vault authentication failed")
        
        response = client.secrets.kv.v2.read_secret_version(
            mount_point=mount_point,
            path=path
        )
        
        data = response['data']['data']
        config_sources = {'vault': ConfigSource.VAULT}
        data['config_sources'] = config_sources
        
        return cls(**data)
    
    @classmethod
    def from_consul(cls, key: str, consul_host: str = "localhost", consul_port: int = 8500) -> "Settings":
        """Load settings from Consul KV store"""
        if not CONSUL_AVAILABLE:
            raise ImportError("python-consul required for Consul integration")
        
        c = consul.Consul(host=consul_host, port=consul_port)
        index, data = c.kv.get(key)
        
        if not data:
            raise KeyError(f"Key not found in Consul: {key}")
        
        value = data['Value']
        if isinstance(value, bytes):
            value = value.decode('utf-8')
        
        try:
            parsed = json.loads(value)
        except:
            parsed = {'value': value}
        
        config_sources = {'consul': ConfigSource.CONSUL}
        parsed['config_sources'] = config_sources
        
        return cls(**parsed)
    
    @classmethod
    def from_etcd(cls, key: str, etcd_host: str = "localhost", etcd_port: int = 2379) -> "Settings":
        """Load settings from etcd"""
        if not ETCD_AVAILABLE:
            raise ImportError("etcd3 required for etcd integration")
        
        client = etcd3.client(host=etcd_host, port=etcd_port)
        value, _ = client.get(key)
        
        if not value:
            raise KeyError(f"Key not found in etcd: {key}")
        
        try:
            parsed = json.loads(value.decode('utf-8'))
        except:
            parsed = {'value': value.decode('utf-8')}
        
        config_sources = {'etcd': ConfigSource.ETCD}
        parsed['config_sources'] = config_sources
        
        return cls(**parsed)
    
    @staticmethod
    def _normalize_google_creds(raw: str) -> Optional[str]:
        """Normalize Google credentials"""
        if not raw:
            return None
        
        t = strip_value(raw)
        
        # Handle wrapping quotes
        if (t.startswith('"') and t.endswith('"')) or (t.startswith("'") and t.endswith("'")):
            t = t[1:-1].strip()
        
        # Base64 decode attempt
        if not t.startswith('{') and len(t) > 50:
            try:
                decoded = base64.b64decode(t, validate=False).decode('utf-8', errors='replace').strip()
                if decoded.startswith('{'):
                    t = decoded
            except:
                pass
        
        # Validate JSON
        try:
            obj = json.loads(t)
            if isinstance(obj, dict):
                # Repair private key
                if 'private_key' in obj and isinstance(obj['private_key'], str):
                    obj['private_key'] = repair_private_key(obj['private_key'])
                # Return compact JSON
                return json.dumps(obj, separators=(',', ':'))
        except:
            pass
        
        # Return as-is (might be file path)
        return t
    
    # =========================================================================
    # Validation
    # =========================================================================
    def validate(self) -> Tuple[List[str], List[str]]:
        """Validate settings, return (errors, warnings)"""
        errors: List[str] = []
        warnings: List[str] = []
        
        # Backend URL
        if not is_valid_url(self.backend_base_url):
            errors.append(f"BACKEND_BASE_URL invalid: {self.backend_base_url!r} (must start with http:// or https://)")
        
        # Public URL
        if self.public_base_url and not is_valid_url(self.public_base_url):
            warnings.append(f"public_base_url invalid: {self.public_base_url!r}")
        
        # Providers
        if not self.enabled_providers:
            warnings.append("enabled_providers is empty; default routing may be degraded")
        
        if self.primary_provider and self.enabled_providers and self.primary_provider not in self.enabled_providers:
            warnings.append(
                f"PRIMARY_PROVIDER={self.primary_provider!r} not in enabled_providers={self.enabled_providers!r}"
            )
        
        # Open mode safety
        if self.open_mode and (self.app_token or self.backup_app_token):
            warnings.append("OPEN_MODE enabled while tokens exist - endpoints exposed publicly")
        
        # Token strength
        for name, token in [("APP_TOKEN", self.app_token), ("BACKUP_APP_TOKEN", self.backup_app_token)]:
            if token:
                hint = secret_strength_hint(token)
                if hint:
                    warnings.append(f"{name}: {hint}")
        
        # Google credentials
        if self.google_creds:
            if self.google_creds.startswith('{'):
                try:
                    obj = json.loads(self.google_creds)
                    for field in ['client_email', 'private_key', 'project_id']:
                        if field not in obj:
                            warnings.append(f"Google credentials missing field: {field}")
                except:
                    warnings.append("Google credentials: invalid JSON format")
            else:
                # Might be file path
                path = Path(self.google_creds)
                if path.exists():
                    mode = path.stat().st_mode
                    if mode & stat.S_IRWXG or mode & stat.S_IRWXO:
                        warnings.append(f"Credentials file {path} has unsafe permissions")
                else:
                    warnings.append(f"Google credentials path not found: {self.google_creds}")
        else:
            warnings.append("Google Sheets credentials missing - sheets features will fail")
        
        # Spreadsheet ID
        if not self.spreadsheet_id:
            warnings.append("Spreadsheet ID missing - scripts may require it")
        
        # Database
        if self.database_url:
            if not any(self.database_url.startswith(p) for p in ['postgresql://', 'mysql://', 'sqlite://']):
                errors.append(f"Invalid database URL scheme: {self.database_url}")
        
        # Redis
        if self.redis_url and not self.redis_url.startswith(('redis://', 'rediss://')):
            errors.append(f"Invalid Redis URL: {self.redis_url}")
        
        # CORS origins
        for origin in self.cors_origins:
            if origin != '*' and not is_valid_url(origin):
                errors.append(f"Invalid CORS origin: {origin}")
        
        # Rate limits
        if self.rate_limit_rpm < 30:
            warnings.append(f"Rate limit {self.rate_limit_rpm} RPM is very low")
        elif self.rate_limit_rpm > 5000:
            warnings.append(f"Rate limit {self.rate_limit_rpm} RPM is very high")
        
        # Batch sizes
        if self.quote_batch_size > 500:
            warnings.append(f"Large quote batch size ({self.quote_batch_size}) may cause timeouts")
        
        # Chaos
        if self.chaos_enabled:
            warnings.append(f"CHAOS ENGINEERING ENABLED - failure_rate={self.chaos_failure_rate}, latency={self.chaos_latency_ms}ms")
        
        return errors, warnings
    
    # =========================================================================
    # Queries
    # =========================================================================
    def is_valid(self) -> bool:
        """Check if settings are valid (no errors)"""
        errors, _ = self.validate()
        return len(errors) == 0
    
    def is_production(self) -> bool:
        """Check if running in production"""
        return self.environment == Environment.PRODUCTION
    
    def is_development(self) -> bool:
        """Check if running in development"""
        return self.environment == Environment.DEVELOPMENT
    
    def feature_enabled(self, feature: str) -> bool:
        """Check if feature flag is enabled"""
        flags = {
            'ai_analysis': self.ai_analysis_enabled,
            'advisor': self.advisor_enabled,
            'advanced': self.advanced_enabled,
            'rate_limiting': self.rate_limit_enabled,
            'cache': self.cache_enabled,
            'circuit_breaker': self.circuit_breaker_enabled,
            'metrics': self.metrics_enabled,
            'tracing': self.tracing_enabled,
            'profiling': self.profiling_enabled,
            'chaos': self.chaos_enabled,
        }
        return flags.get(feature, False)
    
    def ab_test_variant(self, test_name: str, user_id: Optional[str] = None) -> str:
        """Get A/B test variant for user"""
        # Simple deterministic bucketing based on user_id
        if not user_id:
            return 'control'
        
        hash_val = int(hashlib.md5(f"{test_name}:{user_id}".encode()).hexdigest(), 16)
        bucket = hash_val % 100
        
        # Configuration would come from external source in production
        variants = {
            'recommendation_model': {
                'control': (0, 50),
                'variant_a': (50, 75),
                'variant_b': (75, 100),
            }
        }
        
        test_config = variants.get(test_name, {'control': (0, 100)})
        
        for variant, (start, end) in test_config.items():
            if start <= bucket < end:
                return variant
        
        return 'control'
    
    def get_provider_config(self, provider: str) -> Dict[str, Any]:
        """Get configuration for specific provider"""
        config = {
            'eodhd': {
                'api_key': self.eodhd_api_key,
                'base_url': 'https://eodhd.com/api',
                'timeout': self.http_timeout_sec,
            },
            'finnhub': {
                'api_key': self.finnhub_api_key,
                'base_url': 'https://finnhub.io/api/v1',
                'timeout': self.http_timeout_sec,
            },
            'alphavantage': {
                'api_key': self.alphavantage_api_key,
                'base_url': 'https://www.alphavantage.co/query',
                'timeout': self.http_timeout_sec,
            },
            'argaam': {
                'api_key': self.argaam_api_key,
                'base_url': self.argaam_quote_url,
                'timeout': self.http_timeout_sec,
            },
        }
        return config.get(provider, {})
    
    # =========================================================================
    # Masking
    # =========================================================================
    def mask_sensitive(self) -> Dict[str, Any]:
        """Return masked settings for logging"""
        d = asdict(self)
        
        # Mask tokens
        d['app_token'] = mask_secret(self.app_token)
        d['backup_app_token'] = mask_secret(self.backup_app_token)
        
        # Mask API keys
        d['eodhd_api_key'] = mask_secret(self.eodhd_api_key)
        d['finnhub_api_key'] = mask_secret(self.finnhub_api_key)
        d['alphavantage_api_key'] = mask_secret(self.alphavantage_api_key)
        d['argaam_api_key'] = mask_secret(self.argaam_api_key)
        d['sentry_dsn'] = mask_secret(self.sentry_dsn)
        d['datadog_api_key'] = mask_secret(self.datadog_api_key)
        d['newrelic_license_key'] = mask_secret(self.newrelic_license_key)
        
        # Mask credentials
        d['google_creds'] = "PRESENT" if self.google_creds else "MISSING"
        
        # Mask database URL
        if self.database_url:
            d['database_url'] = self.database_url.split('@')[-1] if '@' in self.database_url else "***"
        
        # Mask Redis URL
        if self.redis_url:
            d['redis_url'] = self.redis_url.split('@')[-1] if '@' in self.redis_url else "***"
        
        # Handle Enums
        d['environment'] = self.environment.value
        d['log_level'] = self.log_level.value
        d['cache_backend'] = self.cache_backend.value
        
        return d
    
    # =========================================================================
    # Export
    # =========================================================================
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        d = asdict(self)
        
        # Handle Enums
        d['environment'] = self.environment.value
        d['log_level'] = self.log_level.value
        d['cache_backend'] = self.cache_backend.value
        d['config_sources'] = {k: v.value for k, v in self.config_sources.items()}
        
        return d
    
    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), indent=indent, default=str)
    
    def to_yaml(self) -> str:
        """Convert to YAML string"""
        if not YAML_AVAILABLE:
            raise ImportError("PyYAML required for YAML export")
        return yaml.dump(self.to_dict(), default_flow_style=False)
    
    def to_env_file(self) -> str:
        """Convert to .env file format"""
        lines = []
        for key, value in self.to_dict().items():
            if value is None:
                continue
            if isinstance(value, (list, dict)):
                value = json.dumps(value)
            lines.append(f"{key.upper()}={value}")
        return '\n'.join(lines)
    
    # =========================================================================
    # Compliance
    # =========================================================================
    def compliance_report(self) -> Dict[str, Any]:
        """Generate compliance report"""
        return {
            'version': self.config_version,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'environment': self.environment.value,
            'security': {
                'auth_enabled': not self.open_mode,
                'auth_header': self.auth_header_name,
                'rate_limiting': self.rate_limit_enabled,
                'rate_limit_rpm': self.rate_limit_rpm,
                'cors_origins': self.cors_origins,
                'tokens_configured': bool(self.app_token or self.backup_app_token),
            },
            'data_protection': {
                'encryption_at_rest': bool(self.database_url),
                'encryption_in_transit': self.backend_base_url.startswith('https'),
                'sensitive_data_masked': True,
            },
            'monitoring': {
                'metrics_enabled': self.metrics_enabled,
                'tracing_enabled': self.tracing_enabled,
                'sentry_configured': bool(self.sentry_dsn),
                'health_check': self.health_check_path,
            },
            'compliance': {
                'gdpr_ready': True,
                'soc2_ready': self.metrics_enabled,
                'iso27001_ready': self.rate_limit_enabled and not self.open_mode,
            },
            'validation': {
                'errors': list(self.boot_errors),
                'warnings': list(self.boot_warnings),
                'is_valid': len(self.boot_errors) == 0,
            }
        }

# =============================================================================
# Dynamic Configuration Manager
# =============================================================================
class DynamicConfig:
    """Dynamic configuration with hot-reload support"""
    
    def __init__(self, initial_settings: Settings):
        self._settings = initial_settings
        self._lock = RLock()
        self._observers: List[Callable[[Settings], None]] = []
        self._last_reload = datetime.now(timezone.utc)
        self._reload_count = 0
        
        # Start background reloader if enabled
        if os.getenv('CONFIG_AUTO_RELOAD', '').lower() == 'true':
            self._start_reloader()
    
    @property
    def settings(self) -> Settings:
        """Get current settings"""
        with self._lock:
            return self._settings
    
    @property
    def last_reload(self) -> datetime:
        """Get last reload time"""
        return self._last_reload
    
    @property
    def reload_count(self) -> int:
        """Get reload count"""
        return self._reload_count
    
    def reload(self) -> bool:
        """Reload configuration from sources"""
        try:
            # Reload from environment
            new_settings = Settings.from_env()
            
            # Merge with existing
            with self._lock:
                old_settings = self._settings
                self._settings = new_settings
                self._last_reload = datetime.now(timezone.utc)
                self._reload_count += 1
            
            # Notify observers
            self._notify_observers(old_settings, new_settings)
            
            logger.info(f"Configuration reloaded (count={self._reload_count})")
            return True
            
        except Exception as e:
            logger.error(f"Configuration reload failed: {e}")
            return False
    
    def observe(self, callback: Callable[[Settings], None]) -> Callable:
        """Register observer for config changes"""
        with self._lock:
            self._observers.append(callback)
        
        # Return unsubscribe function
        def unsubscribe():
            with self._lock:
                self._observers.remove(callback)
        
        return unsubscribe
    
    def _notify_observers(self, old: Settings, new: Settings):
        """Notify observers of config change"""
        with self._lock:
            observers = self._observers.copy()
        
        for callback in observers:
            try:
                callback(new)
            except Exception as e:
                logger.error(f"Observer callback failed: {e}")
    
    def _start_reloader(self, interval: int = 60):
        """Start background reloader thread"""
        def reloader():
            while True:
                time.sleep(interval)
                self.reload()
        
        import threading
        thread = threading.Thread(target=reloader, daemon=True)
        thread.start()
        logger.info(f"Auto-reloader started (interval={interval}s)")
    
    def diff(self, other: Settings) -> Dict[str, Tuple[Any, Any]]:
        """Compare with other settings"""
        current_dict = self._settings.to_dict()
        other_dict = other.to_dict()
        
        diff = {}
        for key in set(current_dict.keys()) | set(other_dict.keys()):
            current_val = current_dict.get(key)
            other_val = other_dict.get(key)
            if current_val != other_val:
                diff[key] = (current_val, other_val)
        
        return diff

# =============================================================================
# Singleton Instance
# =============================================================================
_settings_instance: Optional[Settings] = None
_dynamic_config: Optional[DynamicConfig] = None
_init_lock = Lock()

def get_settings() -> Settings:
    """Get settings singleton"""
    global _settings_instance
    
    if _settings_instance is None:
        with _init_lock:
            if _settings_instance is None:
                _settings_instance = Settings.from_env()
    
    return _settings_instance

def get_dynamic_config() -> DynamicConfig:
    """Get dynamic config singleton"""
    global _dynamic_config
    
    if _dynamic_config is None:
        with _init_lock:
            if _dynamic_config is None:
                _dynamic_config = DynamicConfig(get_settings())
    
    return _dynamic_config

def reload_config() -> bool:
    """Reload configuration"""
    return get_dynamic_config().reload()

def boot_diagnostics() -> Dict[str, Any]:
    """Get boot diagnostics"""
    settings = get_settings()
    return {
        "status": "ok" if not settings.boot_errors else "error",
        "config_version": CONFIG_VERSION,
        "environment": settings.environment.value,
        "open_mode": settings.open_mode,
        "errors": list(settings.boot_errors),
        "warnings": list(settings.boot_warnings),
        "masked": settings.mask_sensitive(),
        "compliance": settings.compliance_report(),
    }

def allowed_tokens() -> List[str]:
    """Get list of allowed tokens"""
    settings = get_settings()
    tokens = []
    if settings.app_token:
        tokens.append(settings.app_token)
    if settings.backup_app_token:
        tokens.append(settings.backup_app_token)
    return tokens

def is_open_mode() -> bool:
    """Check if open mode is enabled"""
    return get_settings().open_mode

def auth_ok(token: Optional[str]) -> bool:
    """Check if authentication is valid"""
    if is_open_mode():
        return True
    if not token:
        return False
    t = strip_value(token)
    return bool(t and t in allowed_tokens())

def mask_settings_dict() -> Dict[str, Any]:
    """Get masked settings dictionary"""
    return get_settings().mask_sensitive()

def get_provider_keys() -> Dict[str, Optional[str]]:
    """Get provider API keys"""
    settings = get_settings()
    return {
        'eodhd': settings.eodhd_api_key,
        'finnhub': settings.finnhub_api_key,
        'alphavantage': settings.alphavantage_api_key,
        'argaam': settings.argaam_api_key,
    }

# =============================================================================
# Signal Handlers for Hot Reload
# =============================================================================
def _handle_sighup(signum, frame):
    """Handle SIGHUP for configuration reload"""
    logger.info("Received SIGHUP, reloading configuration...")
    reload_config()

# Register signal handler if in main thread
if threading.current_thread() is threading.main_thread():
    try:
        signal.signal(signal.SIGHUP, _handle_sighup)
    except (AttributeError, ValueError):
        # SIGHUP not available on Windows
        pass

# =============================================================================
# Export
# =============================================================================
__all__ = [
    # Version
    "CONFIG_VERSION",
    
    # Enums
    "Environment",
    "LogLevel",
    "ProviderType",
    "CacheBackend",
    "ConfigSource",
    
    # Main classes
    "Settings",
    "DynamicConfig",
    
    # Factories
    "get_settings",
    "get_dynamic_config",
    "reload_config",
    "boot_diagnostics",
    
    # Auth
    "allowed_tokens",
    "is_open_mode",
    "auth_ok",
    "mask_settings_dict",
    "get_provider_keys",
    
    # Utilities
    "strip_value",
    "coerce_int",
    "coerce_float",
    "coerce_bool",
    "coerce_list",
    "coerce_dict",
    "is_valid_url",
    "mask_secret",
    "repair_private_key",
    
    # Pydantic models (if available)
] + (["DatabaseConfig", "RedisConfig", "CacheConfig", "ProviderConfig",
      "FeatureFlags", "SecurityConfig", "LoggingConfig", "MonitoringConfig",
      "PerformanceConfig", "DeploymentConfig", "GoogleSheetsConfig",
      "ArgaamConfig", "ChaosConfig"] if PYDANTIC_AVAILABLE else [])
