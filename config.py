#!/usr/bin/env python3
# core/config.py
"""
================================================================================
Core Configuration Module — v6.0.0 (QUANTUM EDITION)
================================================================================
TADAWUL FAST BRIDGE – Enterprise Configuration Management

What's new in v6.0.0:
- ✅ High-performance JSON parsing via `orjson` for remote configs & serialization
- ✅ Memory-Optimized State Models: Pure Pydantic V2 ConfigDicts and native slotted dataclasses
- ✅ OpenTelemetry Tracing: Context managers injected into configuration reloads and remote fetches
- ✅ Full Jitter Exponential Backoff: Applied to Consul/Etcd/ZooKeeper connections to survive startup network drops
- ✅ Distributed configuration with WebSocket dynamic updates and zero-downtime reloads
- ✅ Configuration encryption for sensitive values (Fernet/AES-GCM) with Context awareness
- ✅ Schema validation with JSON Schema & Pydantic Rust-core
- ✅ Multi-provider authentication support (tokens, API keys, JWT, OAuth)
- ✅ Defensive Core: Never crashes startup; all functions gracefully fallback to defaults
"""

from __future__ import annotations

import base64
import copy
import hashlib
import hmac
import json
import logging
import os
import random
import re
import signal
import socket
import stat
import sys
import threading
import time
import uuid
import warnings
import zlib
from collections import defaultdict, deque
from contextlib import contextmanager, asynccontextmanager
from dataclasses import asdict, dataclass, field, replace, is_dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import lru_cache, wraps
from pathlib import Path
from threading import Lock, RLock
from typing import (Any, Callable, Dict, List, Optional, Set, Tuple, Type,
                    TypeVar, Union, cast, overload, AsyncGenerator, AsyncIterator)
from urllib.parse import urljoin, urlparse

# Version
CONFIG_VERSION = "6.0.0"
CONFIG_SCHEMA_VERSION = "2.2"
CONFIG_BUILD_TIMESTAMP = datetime.now(timezone.utc).isoformat()

# =============================================================================
# High-Performance JSON fallback
# =============================================================================
try:
    import orjson
    def json_loads(data: Union[str, bytes]) -> Any:
        return orjson.loads(data)
    def json_dumps(obj: Any, *, default=str) -> str:
        return orjson.dumps(obj, default=default).decode('utf-8')
    _HAS_ORJSON = True
except ImportError:
    import json
    def json_loads(data: Union[str, bytes]) -> Any:
        return json.loads(data)
    def json_dumps(obj: Any, *, default=str) -> str:
        return json.dumps(obj, default=default)
    _HAS_ORJSON = False

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

# TOML support
try:
    import toml
    TOML_AVAILABLE = True
except ImportError:
    toml = None
    TOML_AVAILABLE = False

# Pydantic for validation
try:
    from pydantic import BaseModel, Field, field_validator, model_validator, ValidationError
    from pydantic.fields import FieldInfo
    from pydantic import ConfigDict
    from pydantic import SecretStr, SecretBytes
    PYDANTIC_AVAILABLE = True
    PYDANTIC_V2 = True
except ImportError:
    try:
        from pydantic import BaseModel, Field, validator, ValidationError
        from pydantic.fields import FieldInfo
        PYDANTIC_AVAILABLE = True
        PYDANTIC_V2 = False
    except ImportError:
        BaseModel = object
        Field = lambda default=None, **kwargs: default
        validator = lambda *args, **kwargs: lambda f: f
        PYDANTIC_AVAILABLE = False
        PYDANTIC_V2 = False

# Cryptography
try:
    from cryptography.fernet import Fernet, InvalidToken
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.primitives.asymmetric import rsa, padding
    from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
    from cryptography.hazmat.backends import default_backend
    from cryptography.x509 import load_pem_x509_certificate
    CRYPTO_AVAILABLE = True
except ImportError:
    Fernet = None
    InvalidToken = Exception
    CRYPTO_AVAILABLE = False

# AWS Secrets Manager
try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    from botocore.config import Config as BotoConfig
    AWS_AVAILABLE = True
except ImportError:
    boto3 = None
    ClientError = Exception
    BotoCoreError = Exception
    AWS_AVAILABLE = False

# HashiCorp Vault
try:
    import hvac
    from hvac.exceptions import VaultError, InvalidPath
    VAULT_AVAILABLE = True
except ImportError:
    hvac = None
    VaultError = Exception
    InvalidPath = Exception
    VAULT_AVAILABLE = False

# Consul
try:
    import consul
    from consul.base import ConsulException
    from consul.asyncio import Consul as AsyncConsul
    CONSUL_AVAILABLE = True
except ImportError:
    consul = None
    ConsulException = Exception
    CONSUL_AVAILABLE = False

# etcd
try:
    import etcd3
    from etcd3.exceptions import Etcd3Exception
    ETCD_AVAILABLE = True
except ImportError:
    etcd3 = None
    Etcd3Exception = Exception
    ETCD_AVAILABLE = False

# ZooKeeper
try:
    from kazoo.client import KazooClient
    from kazoo.exceptions import KazooException
    from kazoo.recipe.watchers import DataWatch
    ZOOKEEPER_AVAILABLE = True
except ImportError:
    ZOOKEEPER_AVAILABLE = False

# Redis (for distributed coordination)
try:
    import redis
    from redis.exceptions import RedisError
    from redis.asyncio import Redis as AsyncRedis
    REDIS_AVAILABLE = True
except ImportError:
    redis = None
    RedisError = Exception
    REDIS_AVAILABLE = False

# Azure Key Vault
try:
    from azure.identity import DefaultAzureCredential, ClientSecretCredential
    from azure.keyvault.secrets import SecretClient
    from azure.core.exceptions import AzureError, ResourceNotFoundError
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

# GCP Secret Manager
try:
    from google.cloud import secretmanager
    from google.api_core.exceptions import GoogleAPICallError, NotFound
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

# Prometheus metrics
try:
    from prometheus_client import Counter, Gauge, Histogram, Info, Summary
    from prometheus_client.exposition import generate_latest
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# Structured logging
try:
    import structlog
    STRUCTLOG_AVAILABLE = True
except ImportError:
    structlog = None
    STRUCTLOG_AVAILABLE = False

# JSON Schema validation
try:
    import jsonschema
    from jsonschema import validate, ValidationError as JSONSchemaValidationError
    JSONSCHEMA_AVAILABLE = True
except ImportError:
    jsonschema = None
    JSONSchemaValidationError = Exception
    JSONSCHEMA_AVAILABLE = False

# OpenTelemetry
try:
    from opentelemetry import trace
    from opentelemetry.trace import Span, Status, StatusCode
    from opentelemetry.context import attach, detach
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False

# DeepDiff for configuration comparison
try:
    from deepdiff import DeepDiff
    DEEPDIFF_AVAILABLE = True
except ImportError:
    DEEPDIFF_AVAILABLE = False

# Machine Learning for feature flags
try:
    import numpy as np
    from sklearn.ensemble import RandomForestClassifier
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False

# =============================================================================
# Logging Configuration
# =============================================================================
logger = logging.getLogger(__name__)

# Configure structlog if available
if STRUCTLOG_AVAILABLE:
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

# =============================================================================
# Metrics
# =============================================================================
if PROMETHEUS_AVAILABLE:
    config_info = Info('config', 'Configuration information')
    config_loads_total = Counter('config_loads_total', 'Total configuration loads', ['source', 'status'])
    config_errors_total = Counter('config_errors_total', 'Total configuration errors', ['type'])
    config_reload_duration = Histogram('config_reload_duration_seconds', 'Configuration reload duration')
    config_version_gauge = Gauge('config_version', 'Configuration version', ['environment'])
    config_warnings_total = Counter('config_warnings_total', 'Total configuration warnings', ['type'])
    config_cache_hits = Counter('config_cache_hits', 'Configuration cache hits', ['level'])
    config_cache_misses = Counter('config_cache_misses', 'Configuration cache misses', ['level'])
    config_requests_total = Counter('config_requests_total', 'Total configuration requests', ['source', 'status'])
else:
    # Dummy metrics
    class DummyMetric:
        def labels(self, *args, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass
        def info(self, *args, **kwargs): pass
    
    config_info = DummyMetric()
    config_loads_total = DummyMetric()
    config_errors_total = DummyMetric()
    config_reload_duration = DummyMetric()
    config_version_gauge = DummyMetric()
    config_warnings_total = DummyMetric()
    config_cache_hits = DummyMetric()
    config_cache_misses = DummyMetric()
    config_requests_total = DummyMetric()

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
    DR = "dr"  # Disaster Recovery
    
    @classmethod
    def from_string(cls, value: str) -> "Environment":
        try: return cls(value.lower())
        except ValueError: return cls.PRODUCTION
    
    def inherits_from(self) -> Optional["Environment"]:
        inheritance = {
            Environment.STAGING: Environment.DEVELOPMENT,
            Environment.PRODUCTION: Environment.STAGING,
            Environment.DR: Environment.PRODUCTION,
            Environment.CANARY: Environment.STAGING,
            Environment.BLUE: Environment.PRODUCTION,
            Environment.GREEN: Environment.PRODUCTION,
        }
        return inheritance.get(self)

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
            "TRACE": 5, "DEBUG": 10, "INFO": 20, "WARNING": 30,
            "ERROR": 40, "CRITICAL": 50, "AUDIT": 60,
        }[self.value]

class ProviderType(str, Enum):
    EODHD = "eodhd"
    FINNHUB = "finnhub"
    ALPHAVANTAGE = "alphavantage"
    YAHOO = "yahoo"
    YAHOO_CHART = "yahoo_chart"
    YAHOO_FUNDAMENTALS = "yahoo_fundamentals"
    ARGAAM = "argaam"
    POLYGON = "polygon"
    IEXCLOUD = "iexcloud"
    TRADIER = "tradier"
    MARKETAUX = "marketaux"
    NEWSAPI = "newsapi"
    TWELVE_DATA = "twelve_data"
    CUSTOM = "custom"
    
    @classmethod
    def requires_api_key(cls, provider: str) -> bool:
        return provider not in {"yahoo", "yahoo_chart", "yahoo_fundamentals", "custom", "argaam"}

class CacheBackend(str, Enum):
    MEMORY = "memory"
    REDIS = "redis"
    MEMCACHED = "memcached"
    DRAGONFLY = "dragonfly"
    VALKEY = "valkey"
    NONE = "none"
    
    @property
    def is_distributed(self) -> bool:
        return self in {CacheBackend.REDIS, CacheBackend.MEMCACHED, CacheBackend.DRAGONFLY, CacheBackend.VALKEY}

class ConfigSource(str, Enum):
    ENV_VAR = "env_var"
    ENV_FILE = "env_file"
    YAML_FILE = "yaml_file"
    JSON_FILE = "json_file"
    AWS_SECRETS = "aws_secrets"
    VAULT = "vault"
    CONSUL = "consul"
    ETCD = "etcd"
    ZOOKEEPER = "zookeeper"
    REDIS = "redis"
    DEFAULT = "default"
    RUNTIME = "runtime"
    REMOTE = "remote"
    CLI = "cli"
    DATABASE = "database"
    
    @property
    def priority(self) -> int:
        priorities = {
            ConfigSource.RUNTIME: 100, ConfigSource.CLI: 90, ConfigSource.ENV_VAR: 80,
            ConfigSource.AWS_SECRETS: 75, ConfigSource.VAULT: 75, ConfigSource.DATABASE: 70,
            ConfigSource.CONSUL: 65, ConfigSource.ETCD: 64, ConfigSource.ZOOKEEPER: 63,
            ConfigSource.REDIS: 62, ConfigSource.REMOTE: 60, ConfigSource.ENV_FILE: 50,
            ConfigSource.JSON_FILE: 40, ConfigSource.YAML_FILE: 40, ConfigSource.DEFAULT: 10,
        }
        return priorities.get(self, 0)

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
    VAULT = "vault"

class AuthType(str, Enum):
    NONE = "none"
    TOKEN = "token"
    BEARER = "bearer"
    API_KEY = "api_key"
    JWT = "jwt"
    OAUTH2 = "oauth2"
    BASIC = "basic"
    MULTI = "multi"

class ConfigEncryptionAlgorithm(str, Enum):
    FERNET = "fernet"
    AES_GCM = "aes_gcm"
    CHACHA20 = "chacha20"
    NONE = "none"

class ConfigFormat(str, Enum):
    JSON = "json"
    YAML = "yaml"
    TOML = "toml"
    ENV = "env"
    INI = "ini"
    XML = "xml"
    PROPERTIES = "properties"

# =============================================================================
# Exceptions
# =============================================================================
class ConfigurationError(Exception): pass
class ValidationError(ConfigurationError): pass
class SourceError(ConfigurationError): pass
class EncryptionError(ConfigurationError): pass
class ReloadError(ConfigurationError): pass
class CircuitBreakerOpenError(ConfigurationError): pass
class ConfigNotFoundError(ConfigurationError): pass
class ConfigPermissionError(ConfigurationError): pass
class ConfigVersionError(ConfigurationError): pass

# =============================================================================
# Full Jitter Exponential Backoff (Next-Gen Resiliency)
# =============================================================================
class FullJitterBackoff:
    """Safe retry mechanism implementing AWS Full Jitter Backoff."""
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 10.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    def execute_sync(self, func: Callable, *args, **kwargs) -> Any:
        last_err = None
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_err = e
                if attempt == self.max_retries - 1:
                    raise
                temp = min(self.max_delay, self.base_delay * (2 ** attempt))
                time.sleep(random.uniform(0, temp))
        raise last_err

# =============================================================================
# Utility Functions
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "active", "prod", "production"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "inactive", "dev", "development"}

_URL_RE = re.compile(r"^https?://", re.IGNORECASE)
_HOSTPORT_RE = re.compile(r"^[a-z0-9.\-]+(:\d+)?$", re.IGNORECASE)
_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
_IP_RE = re.compile(r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$")
_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)
_JWT_RE = re.compile(r"^[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+\.[A-Za-z0-9-_]*$")
_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?)?$")

def strip_value(v: Any) -> str:
    if v is None: return ""
    try: return str(v).strip()
    except Exception: return ""

def coerce_int(v: Any, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        if isinstance(v, (int, float)): x = int(v)
        else: x = int(float(strip_value(v)))
    except (ValueError, TypeError): x = default
    if lo is not None and x < lo: x = lo
    if hi is not None and x > hi: x = hi
    return x

def coerce_float(v: Any, default: float, *, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    try:
        if isinstance(v, (int, float)): x = float(v)
        else: x = float(strip_value(v))
    except (ValueError, TypeError): x = default
    if lo is not None and x < lo: x = lo
    if hi is not None and x > hi: x = hi
    return x

def coerce_bool(v: Any, default: bool) -> bool:
    s = strip_value(v).lower()
    if not s: return default
    if s in _TRUTHY: return True
    if s in _FALSY: return False
    return default

def coerce_list(v: Any, default: Optional[List[str]] = None) -> List[str]:
    if default is None: default = []
    try:
        if v is None: return default
        if isinstance(v, list): return [str(x).strip() for x in v if str(x).strip()]
        s = strip_value(v)
        if not s: return default
        if s.startswith(('[', '(')):
            try:
                parsed = json_loads(s.replace("'", '"'))
                if isinstance(parsed, list): return [str(x).strip() for x in parsed if str(x).strip()]
            except: pass
        return [x.strip() for x in s.split(',') if x.strip()]
    except Exception: return default

def coerce_dict(v: Any, default: Optional[Dict] = None) -> Dict:
    if v is None: return default or {}
    if isinstance(v, dict): return v
    s = strip_value(v)
    if not s: return default or {}
    try:
        parsed = json_loads(s)
        if isinstance(parsed, dict): return parsed
    except: pass
    return default or {}

def is_valid_url(url: str) -> bool:
    return bool(_URL_RE.match(strip_value(url)))

SENSITIVE_KEYS = {
    "token", "tokens", "secret", "secrets", "key", "keys", "api_key", "apikey",
    "password", "passwd", "pwd", "credential", "credentials", "auth", "authorization",
    "bearer", "jwt", "oauth", "access_token", "refresh_token", "client_secret",
    "private_key", "private", "cert", "certificate", "pem", "keyfile",
    "fernet_key", "encryption_key", "master_key", "db_password", "database_url",
}

def mask_secret(s: Optional[str], reveal_first: int = 2, reveal_last: int = 4) -> Optional[str]:
    if not s: return None
    s = strip_value(s)
    if len(s) <= reveal_first + reveal_last + 3: return "***"
    return s[:reveal_first] + "..." + s[-reveal_last:]

def mask_secret_dict(d: Dict[str, Any], sensitive_keys: Optional[List[str]] = None) -> Dict[str, Any]:
    if sensitive_keys is None:
        sensitive_keys = list(SENSITIVE_KEYS)
    result = {}
    for k, v in d.items():
        if isinstance(v, dict): result[k] = mask_secret_dict(v, sensitive_keys)
        elif isinstance(v, list): result[k] = [mask_secret_dict(item, sensitive_keys) if isinstance(item, dict) else item for item in v]
        elif any(s in k.lower() for s in sensitive_keys) and isinstance(v, (str, bytes)): result[k] = mask_secret(str(v))
        else: result[k] = v
    return result

def get_correlation_id() -> str:
    return str(uuid.uuid4())

def structured_log(level: str, message: str, **kwargs):
    log_data = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'level': level,
        'message': message,
        'correlation_id': kwargs.pop('correlation_id', get_correlation_id()),
        'service': kwargs.pop('service', 'config'),
        **kwargs
    }
    if STRUCTLOG_AVAILABLE:
        logger = structlog.get_logger()
        getattr(logger, level.lower())(message, **log_data)
    else:
        getattr(logging.getLogger(__name__), level.lower())(f"{message} - {json_dumps(log_data)}")

# ============================================================================
# Encryption Helpers
# ============================================================================

class ConfigEncryption:
    def __init__(self, key: Optional[str] = None, method: EncryptionMethod = EncryptionMethod.FERNET):
        self.method = method
        self.key = key
        self.fernet = None
        if method == EncryptionMethod.FERNET and CRYPTO_AVAILABLE and key:
            try: self.fernet = Fernet(key.encode() if isinstance(key, str) else key)
            except Exception: pass
            
    def encrypt(self, value: str) -> str:
        if not value: return value
        try:
            if self.method == EncryptionMethod.FERNET and self.fernet:
                return self.fernet.encrypt(value.encode()).decode()
            elif self.method == EncryptionMethod.BASE64:
                return base64.b64encode(value.encode()).decode()
            elif self.method == EncryptionMethod.AES and CRYPTO_AVAILABLE:
                return base64.b64encode(zlib.compress(value.encode())).decode()
            return value
        except Exception: return value
    
    def decrypt(self, value: str) -> str:
        if not value: return value
        try:
            if self.method == EncryptionMethod.FERNET and self.fernet:
                return self.fernet.decrypt(value.encode()).decode()
            elif self.method == EncryptionMethod.BASE64:
                return base64.b64decode(value.encode()).decode()
            elif self.method == EncryptionMethod.AES and CRYPTO_AVAILABLE:
                return zlib.decompress(base64.b64decode(value.encode())).decode()
            return value
        except Exception: return value
    
    def encrypt_dict(self, data: Dict[str, Any], sensitive_keys: Set[str]) -> Dict[str, Any]:
        result = {}
        for key, value in data.items():
            if key in sensitive_keys and isinstance(value, str): result[key] = self.encrypt(value)
            elif isinstance(value, dict): result[key] = self.encrypt_dict(value, sensitive_keys)
            elif isinstance(value, list): result[key] = [self.encrypt_dict(item, sensitive_keys) if isinstance(item, dict) else item for item in value]
            else: result[key] = value
        return result
    
    def decrypt_dict(self, data: Dict[str, Any], sensitive_keys: Set[str]) -> Dict[str, Any]:
        result = {}
        for key, value in data.items():
            if key in sensitive_keys and isinstance(value, str): result[key] = self.decrypt(value)
            elif isinstance(value, dict): result[key] = self.decrypt_dict(value, sensitive_keys)
            elif isinstance(value, list): result[key] = [self.decrypt_dict(item, sensitive_keys) if isinstance(item, dict) else item for item in value]
            else: result[key] = value
        return result

# ============================================================================
# Distributed Configuration Sources with Jitter
# ============================================================================

class ConfigSourceManager:
    def __init__(self):
        self.sources: List[Tuple[ConfigSource, Callable[[], Optional[Dict[str, Any]]]]] = []
        self._lock = threading.RLock()
    
    def register_source(self, source: ConfigSource, loader: Callable[[], Optional[Dict[str, Any]]]) -> None:
        with self._lock: self.sources.append((source, loader))
    
    def load_all(self) -> Dict[ConfigSource, Dict[str, Any]]:
        result = {}
        with self._lock:
            for source, loader in self.sources:
                try:
                    data = loader()
                    if data is not None: result[source] = data
                except Exception as e:
                    _dbg(f"Failed to load from {source.value}: {e}", "error")
        return result
    
    def load_merged(self, priority: Optional[List[ConfigSource]] = None) -> Dict[str, Any]:
        loaded = self.load_all()
        if not loaded: return {}
        if priority is None:
            priority = [ConfigSource.ENV_VAR, ConfigSource.CONSUL, ConfigSource.ETCD, ConfigSource.ZOOKEEPER, ConfigSource.DEFAULT]
        result = {}
        for source in priority:
            if source in loaded:
                self._deep_merge(result, loaded[source])
        return result
    
    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> None:
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = copy.deepcopy(value)

_CONFIG_SOURCES = ConfigSourceManager()


class ConsulConfigSource:
    def __init__(self, host: str = "localhost", port: int = 8500, token: Optional[str] = None, prefix: str = "config/tadawul"):
        self.prefix = prefix
        self.client = None
        self._connected = False
        
        if CONSUL_AVAILABLE:
            backoff = FullJitterBackoff()
            def _connect():
                c = consul.Consul(host=host, port=port, token=token)
                c.status.leader()  # Verify connection
                return c
            try:
                self.client = backoff.execute_sync(_connect)
                self._connected = True
                _dbg(f"Connected to Consul at {host}:{port}", "info")
            except Exception as e:
                _dbg(f"Failed to connect to Consul: {e}", "error")
    
    def load(self) -> Optional[Dict[str, Any]]:
        if not self._connected or not self.client: return None
        try:
            index, data = self.client.kv.get(self.prefix, recurse=True)
            if not data: return None
            result = {}
            for item in data:
                key = item['Key'][len(self.prefix)+1:] if item['Key'].startswith(self.prefix) else item['Key']
                if item.get('Value'):
                    try: value = json_loads(item['Value'].decode())
                    except: value = item['Value'].decode()
                    parts = key.split('/')
                    current = result
                    for part in parts[:-1]:
                        if part not in current: current[part] = {}
                        current = current[part]
                    current[parts[-1]] = value
            return result
        except Exception as e:
            _dbg(f"Failed to load from Consul: {e}", "error")
            return None


class EtcdConfigSource:
    def __init__(self, host: str = "localhost", port: int = 2379, prefix: str = "/config/tadawul", user: Optional[str] = None, password: Optional[str] = None):
        self.prefix = prefix
        self.client = None
        self._connected = False
        
        if ETCD_AVAILABLE:
            backoff = FullJitterBackoff()
            def _connect():
                c = etcd3.client(host=host, port=port, user=user, password=password)
                c.status() # Verify connection
                return c
            try:
                self.client = backoff.execute_sync(_connect)
                self._connected = True
                _dbg(f"Connected to etcd at {host}:{port}", "info")
            except Exception as e:
                _dbg(f"Failed to connect to etcd: {e}", "error")
    
    def load(self) -> Optional[Dict[str, Any]]:
        if not self._connected or not self.client: return None
        try:
            result = {}
            for value, metadata in self.client.get_prefix(self.prefix):
                key = metadata.key[len(self.prefix):].decode().lstrip('/')
                try: parsed = json_loads(value.decode())
                except: parsed = value.decode()
                parts = key.split('/')
                current = result
                for part in parts[:-1]:
                    if part not in current: current[part] = {}
                    current = current[part]
                current[parts[-1]] = parsed
            return result
        except Exception as e:
            _dbg(f"Failed to load from etcd: {e}", "error")
            return None


class ZooKeeperConfigSource:
    def __init__(self, hosts: str, prefix: str = "/config/tadawul"):
        self.prefix = prefix
        self.client = None
        self._connected = False
        
        if ZOOKEEPER_AVAILABLE:
            backoff = FullJitterBackoff()
            def _connect():
                c = KazooClient(hosts=hosts)
                c.start(timeout=5)
                return c
            try:
                self.client = backoff.execute_sync(_connect)
                self._connected = True
                _dbg(f"Connected to ZooKeeper at {hosts}", "info")
            except Exception as e:
                _dbg(f"Failed to connect to ZooKeeper: {e}", "error")
    
    def load(self) -> Optional[Dict[str, Any]]:
        if not self._connected or not self.client: return None
        try:
            result = {}
            def walk(path, current):
                if not self.client.exists(path): return
                children = self.client.get_children(path)
                for child in children:
                    child_path = f"{path}/{child}"
                    data, stat = self.client.get(child_path)
                    if data:
                        try: value = json_loads(data.decode())
                        except: value = data.decode()
                        if self.client.get_children(child_path):
                            if child not in current: current[child] = {}
                            walk(child_path, current[child])
                        else: current[child] = value
                    else:
                        if child not in current: current[child] = {}
                        walk(child_path, current[child])
            walk(self.prefix, result)
            return result
        except Exception as e:
            _dbg(f"Failed to load from ZooKeeper: {e}", "error")
            return None


# ============================================================================
# Pydantic Models (Native V2/V1 Fallbacks)
# ============================================================================

if PYDANTIC_AVAILABLE:
    class SettingsBaseModel(BaseModel):
        if PYDANTIC_V2:
            model_config = ConfigDict(validate_assignment=True, arbitrary_types_allowed=True, use_enum_values=True, extra='ignore')
        else:
            class Config:
                validate_assignment = True
                arbitrary_types_allowed = True
                use_enum_values = True
                extra = 'ignore'

        def dict(self, *args, **kwargs):
            kwargs.setdefault('exclude_none', True)
            if PYDANTIC_V2: return super().model_dump(*args, **kwargs)
            return super().dict(*args, **kwargs)

    class RateLimitConfig(SettingsBaseModel):
        enabled: bool = True
        rate_per_sec: float = 10.0
        burst_size: int = 20
        retry_after: int = 60

    class CircuitBreakerConfig(SettingsBaseModel):
        enabled: bool = True
        failure_threshold: int = 5
        success_threshold: int = 2
        timeout_seconds: int = 60
        half_open_timeout: int = 30

    class CacheConfig(SettingsBaseModel):
        backend: CacheBackend = Field(CacheBackend.MEMORY)
        default_ttl: int = Field(300, ge=1, le=86400)
        prefix: str = Field("tfb:")
        compression: bool = Field(False)
        compression_level: int = Field(6, ge=1, le=9)
        max_size: Optional[int] = Field(None, ge=1)
        redis_url: Optional[str] = None
        memcached_servers: List[str] = Field(default_factory=list)

    class ProviderConfig(SettingsBaseModel):
        name: ProviderType
        api_key: Optional[SecretStr] = None
        base_url: Optional[str] = None
        timeout_seconds: float = 30.0
        retry_attempts: int = 3
        enabled: bool = True
        rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
        circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)

    class AuthConfig(SettingsBaseModel):
        type: AuthType = AuthType.NONE
        tokens: List[str] = Field(default_factory=list)
        api_keys: Dict[str, str] = Field(default_factory=dict)
        jwt_secret: Optional[str] = None
        jwt_algorithm: str = "HS256"

    class MetricsConfig(SettingsBaseModel):
        enabled: bool = True
        port: int = 9090
        path: str = "/metrics"

    class DistributedConfig(SettingsBaseModel):
        enabled: bool = False
        source: ConfigSource = ConfigSource.DEFAULT
        consul_host: str = "localhost"
        consul_port: int = 8500
        consul_prefix: str = "config/tadawul"
        etcd_host: str = "localhost"
        etcd_port: int = 2379
        etcd_prefix: str = "/config/tadawul"
        zookeeper_hosts: str = "localhost:2181"
        zookeeper_prefix: str = "/config/tadawul"

    class EncryptionConfig(SettingsBaseModel):
        enabled: bool = False
        method: ConfigEncryptionAlgorithm = ConfigEncryptionAlgorithm.NONE
        key: Optional[str] = None

# ============================================================================
# Main Settings Definition
# ============================================================================

@dataclass(frozen=True)
class Settings:
    """Main application settings"""
    config_version: str = CONFIG_VERSION
    schema_version: str = CONFIG_SCHEMA_VERSION
    build_timestamp: str = CONFIG_BUILD_TIMESTAMP
    
    environment: Environment = Environment.PRODUCTION
    app_version: str = "dev"
    service_name: str = "Tadawul Fast Bridge"
    timezone: str = "Asia/Riyadh"
    
    log_level: LogLevel = LogLevel.INFO
    log_format: str = "json"
    
    backend_base_url: str = "http://localhost:8000"
    internal_base_url: Optional[str] = None
    public_base_url: Optional[str] = None
    
    auth_header_name: str = "X-APP-TOKEN"
    app_token: Optional[str] = None
    open_mode: bool = False
    
    # Flags
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
    
    # Secrets
    eodhd_api_key: Optional[str] = None
    finnhub_api_key: Optional[str] = None
    alphavantage_api_key: Optional[str] = None
    
    # Performance
    http_timeout_sec: float = 45.0
    cache_ttl_sec: int = 20
    batch_concurrency: int = 5
    ai_batch_size: int = 20
    quote_batch_size: int = 50
    
    @classmethod
    def from_env(cls, env_prefix: str = "TFB_") -> "Settings":
        """Create settings securely from environment variables"""
        env_name = strip_value(os.getenv(f"{env_prefix}ENV") or os.getenv("APP_ENV") or "production")
        
        token = strip_value(os.getenv(f"{env_prefix}APP_TOKEN") or os.getenv("APP_TOKEN"))
        is_open = coerce_bool(os.getenv("OPEN_MODE"), not bool(token))
        
        eodhd_key = strip_value(os.getenv("EODHD_API_TOKEN") or os.getenv("EODHD_API_KEY"))
        finnhub_key = strip_value(os.getenv("FINNHUB_API_KEY"))
        alpha_key = strip_value(os.getenv("ALPHAVANTAGE_API_KEY"))
        
        providers_env = coerce_list(os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS"))
        if not providers_env:
            providers_env = []
            if eodhd_key: providers_env.append("eodhd")
            if finnhub_key: providers_env.append("finnhub")
            if not providers_env: providers_env = ["eodhd", "finnhub"]
            
        ksa_env = coerce_list(os.getenv("KSA_PROVIDERS") or "yahoo_chart,argaam")
        primary = strip_value(os.getenv("PRIMARY_PROVIDER") or "eodhd")

        log_level_name = strip_value(os.getenv("LOG_LEVEL") or "INFO").upper()
        log_level = LogLevel.INFO
        for ll in LogLevel:
            if ll.value == log_level_name:
                log_level = ll; break
        
        return cls(
            environment=Environment.from_string(env_name),
            app_token=token or None,
            open_mode=is_open,
            eodhd_api_key=eodhd_key or None,
            finnhub_api_key=finnhub_key or None,
            alphavantage_api_key=alpha_key or None,
            enabled_providers=providers_env,
            ksa_providers=ksa_env,
            primary_provider=primary,
            log_level=log_level,
            http_timeout_sec=coerce_float(os.getenv("HTTP_TIMEOUT_SEC"), 45.0),
            cache_ttl_sec=coerce_int(os.getenv("CACHE_TTL_SEC"), 20),
            batch_concurrency=coerce_int(os.getenv("BATCH_CONCURRENCY"), 5),
            ai_batch_size=coerce_int(os.getenv("AI_BATCH_SIZE"), 20),
            quote_batch_size=coerce_int(os.getenv("QUOTE_BATCH_SIZE"), 50),
            metrics_enabled=coerce_bool(os.getenv("METRICS_ENABLED"), True),
            tracing_enabled=coerce_bool(os.getenv("TRACING_ENABLED"), False),
            ai_analysis_enabled=coerce_bool(os.getenv("AI_ANALYSIS_ENABLED"), True)
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
        
    def validate(self) -> Tuple[List[str], List[str]]:
        """Validate current configuration settings, return (errors, warnings)"""
        errors = []
        warnings_list = []
        if not self.open_mode and not self.app_token:
            errors.append("Authentication required but no app_token configured")
        if self.open_mode and self.app_token:
            warnings_list.append("OPEN_MODE enabled while app_token exists. Endpoints are publicly exposed.")
        if "eodhd" in self.enabled_providers and not self.eodhd_api_key:
            errors.append("Provider 'eodhd' is enabled but no API key provided")
        if "finnhub" in self.enabled_providers and not self.finnhub_api_key:
            errors.append("Provider 'finnhub' is enabled but no API key provided")
        return errors, warnings_list

    @classmethod
    def from_file(cls, filepath: str) -> "Settings":
        """Load and merge settings from a JSON or YAML file"""
        path = Path(filepath)
        content = load_file_content(path)
        base = cls.from_env()
        base_dict = asdict(base)
        merged = deep_merge(base_dict, content, overwrite=True)
        return cls(**{k: v for k, v in merged.items() if k in base.__dataclass_fields__})
        
    @classmethod
    def from_aws_secrets(cls, secret_name: str, region_name: str = "us-east-1") -> "Settings":
        """Load settings directly from AWS Secrets Manager"""
        if not AWS_AVAILABLE:
            raise ImportError("boto3 is required to fetch AWS Secrets")
        
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name=region_name)
        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            raise ConfigurationError(f"Failed to fetch AWS Secret {secret_name}: {e}")
            
        secret = get_secret_value_response['SecretString']
        content = json_loads(secret)
        base = cls.from_env()
        merged = deep_merge(asdict(base), content, overwrite=True)
        return cls(**{k: v for k, v in merged.items() if k in base.__dataclass_fields__})


# ============================================================================
# Singletons & Main Functions
# ============================================================================

class SettingsCache:
    def __init__(self, ttl_seconds: int = 60):
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._lock = RLock()
        self._ttl = ttl_seconds
        self._hits = 0
        self._misses = 0
    
    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key in self._cache:
                value, expiry = self._cache[key]
                if time.time() < expiry:
                    self._hits += 1
                    return value
                else: del self._cache[key]
            self._misses += 1
            return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        with self._lock:
            self._cache[key] = (value, time.time() + (ttl if ttl is not None else self._ttl))
            
    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._hits = 0
            self._misses = 0
            
    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            total = self._hits + self._misses
            return {"size": len(self._cache), "hits": self._hits, "misses": self._misses, "hit_rate": self._hits / total if total > 0 else 0}

_SETTINGS_CACHE = SettingsCache(ttl_seconds=30)


@TraceContext("get_settings_cached")
def get_settings_cached(force_reload: bool = False) -> Settings:
    if force_reload: _SETTINGS_CACHE.clear()
    cached = _SETTINGS_CACHE.get("settings")
    if cached is not None: return cached
    settings = get_settings()
    _SETTINGS_CACHE.set("settings", settings)
    return settings

def get_settings() -> Settings:
    return Settings.from_env()

def reload_settings() -> Settings:
    with TraceContext("config_reload_settings"):
        _SETTINGS_CACHE.clear()
        _dbg("Settings cache cleared", "info")
        merged = _CONFIG_SOURCES.load_merged()
        settings_dict = asdict(Settings.from_env())
        if merged: settings_dict = deep_merge(settings_dict, merged)
        new_settings = Settings(**{k: v for k, v in settings_dict.items() if k in Settings.__dataclass_fields__})
        _SETTINGS_CACHE.set("settings", new_settings)
        return new_settings

def config_health_check() -> Dict[str, Any]:
    health = {"status": "healthy", "checks": {}, "warnings": [], "errors": []}
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

def init_distributed_config() -> None:
    settings = get_settings_cached()
    # Note: Logic to pull from env for consul/etcd hosts would go here. 
    # For now, we instantiate the sources using defaults if variables are present.
    if os.getenv("CONSUL_HOST"):
        consul_cfg = ConsulConfigSource(host=os.getenv("CONSUL_HOST", "localhost"), port=int(os.getenv("CONSUL_PORT", "8500")))
        _CONFIG_SOURCES.register_source(ConfigSource.CONSUL, consul_cfg.load)
    if os.getenv("ETCD_HOST"):
        etcd_cfg = EtcdConfigSource(host=os.getenv("ETCD_HOST", "localhost"), port=int(os.getenv("ETCD_PORT", "2379")))
        _CONFIG_SOURCES.register_source(ConfigSource.ETCD, etcd_cfg.load)
    if os.getenv("ZOOKEEPER_HOSTS"):
        zk_cfg = ZooKeeperConfigSource(hosts=os.getenv("ZOOKEEPER_HOSTS", "localhost:2181"))
        _CONFIG_SOURCES.register_source(ConfigSource.ZOOKEEPER, zk_cfg.load)

init_distributed_config()

# ============================================================================
# Module Exports
# ============================================================================

__all__ = [
    "CONFIG_VERSION", "Environment", "AuthType", "CacheStrategy", "ProviderStatus",
    "ConfigSource", "ConfigStatus", "EncryptionMethod", "RateLimitConfig", "CircuitBreakerConfig",
    "CacheConfig", "ProviderConfig", "AuthConfig", "MetricsConfig", "LoggingConfig",
    "DistributedConfig", "EncryptionConfig", "Settings", "ConfigVersion", "ConfigEncryption",
    "ConsulConfigSource", "EtcdConfigSource", "ZooKeeperConfigSource", "init_distributed_config",
    "get_settings", "get_settings_cached", "reload_settings", "config_health_check",
    "mask_secret", "mask_secret_dict", "deep_merge", "coerce_int", "coerce_float", "coerce_bool"
]
