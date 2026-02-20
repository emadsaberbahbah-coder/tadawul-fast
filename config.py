#!/usr/bin/env python3
"""
TADAWUL FAST BRIDGE – ENTERPRISE CONFIGURATION MANAGEMENT (v5.1.0)
==================================================================
Advanced Production Configuration System with Multi-Source Support
SAMA Compliant | Distributed Config | Secrets Management | Dynamic Updates | Zero-Trust Security

[Documentation remains the same...]
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
import signal
import socket
import stat
import sys
import threading
import time
import warnings
import uuid
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
CONFIG_VERSION = "5.1.0"
CONFIG_SCHEMA_VERSION = "2.1"
CONFIG_BUILD_TIMESTAMP = datetime.now(timezone.utc).isoformat()

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
        def labels(self, *args, **kwargs):
            return self
        def inc(self, *args, **kwargs):
            pass
        def observe(self, *args, **kwargs):
            pass
        def set(self, *args, **kwargs):
            pass
        def info(self, *args, **kwargs):
            pass
    
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

class Environment(Enum):
    """Deployment environments with inheritance"""
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
        """Create from string with fallback"""
        try:
            return cls(value.lower())
        except ValueError:
            return cls.PRODUCTION
    
    def inherits_from(self) -> Optional["Environment"]:
        """Get parent environment for inheritance"""
        inheritance = {
            Environment.STAGING: Environment.DEVELOPMENT,
            Environment.PRODUCTION: Environment.STAGING,
            Environment.DR: Environment.PRODUCTION,
            Environment.CANARY: Environment.STAGING,
            Environment.BLUE: Environment.PRODUCTION,
            Environment.GREEN: Environment.PRODUCTION,
        }
        return inheritance.get(self)

class LogLevel(Enum):
    """Logging levels"""
    TRACE = "TRACE"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    AUDIT = "AUDIT"
    
    def to_int(self) -> int:
        """Convert to numeric level"""
        return {
            "TRACE": 5,
            "DEBUG": 10,
            "INFO": 20,
            "WARNING": 30,
            "ERROR": 40,
            "CRITICAL": 50,
            "AUDIT": 60,
        }[self.value]

class ProviderType(Enum):
    """Data provider types"""
    EODHD = "eodhd"
    FINNHUB = "finnhub"
    ALPHAVANTAGE = "alphavantage"
    YAHOO = "yahoo"
    YAHOO_CHART = "yahoo_chart"
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
        """Check if provider requires API key"""
        return provider not in {"yahoo", "yahoo_chart", "custom"}

class CacheBackend(Enum):
    """Cache backends"""
    MEMORY = "memory"
    REDIS = "redis"
    MEMCACHED = "memcached"
    DRAGONFLY = "dragonfly"
    VALKEY = "valkey"
    NONE = "none"
    
    @property
    def is_distributed(self) -> bool:
        """Check if backend is distributed"""
        return self in {CacheBackend.REDIS, CacheBackend.MEMCACHED, 
                        CacheBackend.DRAGONFLY, CacheBackend.VALKEY}

class ConfigSource(Enum):
    """Configuration sources with priority"""
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
        """Get source priority (higher = more important)"""
        priorities = {
            ConfigSource.RUNTIME: 100,
            ConfigSource.CLI: 90,
            ConfigSource.ENV_VAR: 80,
            ConfigSource.AWS_SECRETS: 75,
            ConfigSource.VAULT: 75,
            ConfigSource.DATABASE: 70,
            ConfigSource.CONSUL: 65,
            ConfigSource.ETCD: 64,
            ConfigSource.ZOOKEEPER: 63,
            ConfigSource.REDIS: 62,
            ConfigSource.REMOTE: 60,
            ConfigSource.ENV_FILE: 50,
            ConfigSource.JSON_FILE: 40,
            ConfigSource.YAML_FILE: 40,
            ConfigSource.DEFAULT: 10,
        }
        return priorities.get(self, 0)

class ConfigChangeType(Enum):
    """Type of configuration change"""
    ADDED = "added"
    MODIFIED = "modified"
    DELETED = "deleted"
    UNCHANGED = "unchanged"

class ComplianceStandard(Enum):
    """Compliance standards"""
    GDPR = "gdpr"
    SOC2 = "soc2"
    HIPAA = "hipaa"
    PCI_DSS = "pci_dss"
    ISO27001 = "iso27001"
    ISO27017 = "iso27017"
    ISO27018 = "iso27018"
    NIST = "nist"
    NIST_CSF = "nist_csf"
    CCPA = "ccpa"
    SAMA = "sama"      # Saudi Central Bank
    NCA = "nca"        # National Cybersecurity Authority
    CMA = "cma"        # Capital Market Authority

class CircuitBreakerState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class FeatureStage(Enum):
    """Feature rollout stages"""
    ALPHA = "alpha"
    BETA = "beta"
    GA = "ga"
    DEPRECATED = "deprecated"
    REMOVED = "removed"
    CANARY = "canary"
    EXPERIMENTAL = "experimental"
    PRIVATE = "private"
    PUBLIC = "public"

class ValidationLevel(Enum):
    """Validation levels"""
    OFF = "off"
    BASIC = "basic"
    STRICT = "strict"
    SCHEMA = "schema"
    BUSINESS = "business"

class ConfigEncryptionAlgorithm(Enum):
    """Encryption algorithms for config values"""
    FERNET = "fernet"
    AES_GCM = "aes_gcm"
    CHACHA20 = "chacha20"
    NONE = "none"

class ConfigFormat(Enum):
    """Configuration file formats"""
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
class ConfigurationError(Exception):
    """Base configuration error"""
    pass

class ValidationError(ConfigurationError):
    """Configuration validation error"""
    pass

class SourceError(ConfigurationError):
    """Configuration source error"""
    pass

class EncryptionError(ConfigurationError):
    """Encryption/decryption error"""
    pass

class ReloadError(ConfigurationError):
    """Configuration reload error"""
    pass

class CircuitBreakerOpenError(ConfigurationError):
    """Circuit breaker is open"""
    pass

class ConfigNotFoundError(ConfigurationError):
    """Configuration not found"""
    pass

class ConfigPermissionError(ConfigurationError):
    """Permission denied for configuration"""
    pass

class ConfigVersionError(ConfigurationError):
    """Configuration version mismatch"""
    pass

# =============================================================================
# Utility Functions
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok", "active", "prod", "production"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled", "inactive", "dev", "development"}

_URL_RE = re.compile(r"^https?://", re.IGNORECASE)
_HOSTPORT_RE = re.compile(r"^[a-z0-9.\-]+(:\d+)?$", re.IGNORECASE)
_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
_IP_RE = re.compile(r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$")
_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)
_JWT_RE = re.compile(r"^[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+\.[A-Za-z0-9-_]*$")
_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?)?$")

def strip_value(v: Any) -> str:
    """Strip whitespace from value"""
    if v is None:
        return ""
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

def coerce_path(v: Any, default: Optional[Path] = None) -> Optional[Path]:
    """Coerce value to Path"""
    if v is None:
        return default
    
    try:
        return Path(strip_value(v))
    except:
        return default

def coerce_timedelta(v: Any, default: timedelta) -> timedelta:
    """Coerce value to timedelta"""
    try:
        if isinstance(v, timedelta):
            return v
        seconds = coerce_float(v, default.total_seconds())
        return timedelta(seconds=seconds)
    except:
        return default

def coerce_bytes(v: Any, default: Optional[bytes] = None, encoding: str = 'utf-8') -> Optional[bytes]:
    """Coerce value to bytes"""
    if v is None:
        return default
    
    if isinstance(v, bytes):
        return v
    
    try:
        return str(v).encode(encoding)
    except:
        return default

def coerce_datetime(v: Any, default: Optional[datetime] = None) -> Optional[datetime]:
    """Coerce value to datetime"""
    if v is None:
        return default
    
    if isinstance(v, datetime):
        return v
    
    s = strip_value(v)
    if not s:
        return default
    
    # Try ISO format
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00'))
    except:
        pass
    
    # Try timestamp
    try:
        return datetime.fromtimestamp(float(s), tz=timezone.utc)
    except:
        pass
    
    return default

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

def is_valid_uuid(uuid_str: str) -> bool:
    """Check if string is valid UUID"""
    return bool(_UUID_RE.match(strip_value(uuid_str)))

def is_valid_jwt(token: str) -> bool:
    """Check if string looks like JWT"""
    return bool(_JWT_RE.match(strip_value(token)))

def is_valid_datetime(dt_str: str) -> bool:
    """Check if string is valid datetime"""
    return bool(_DATETIME_RE.match(strip_value(dt_str)))

def mask_secret(s: Optional[str], reveal_first: int = 2, reveal_last: int = 4) -> Optional[str]:
    """Mask secret string for logging"""
    if not s:
        return None
    s = strip_value(s)
    if len(s) <= reveal_first + reveal_last + 3:
        return "***"
    return s[:reveal_first] + "..." + s[-reveal_last:]

def mask_secret_dict(d: Dict[str, Any], sensitive_keys: Optional[List[str]] = None) -> Dict[str, Any]:
    """Mask sensitive values in dictionary"""
    if sensitive_keys is None:
        sensitive_keys = ['token', 'password', 'secret', 'key', 'credential', 'auth', 'api_key', 'api_secret']
    
    result = {}
    for k, v in d.items():
        if isinstance(v, dict):
            result[k] = mask_secret_dict(v, sensitive_keys)
        elif isinstance(v, list):
            result[k] = [mask_secret_dict(item, sensitive_keys) if isinstance(item, dict) else item for item in v]
        elif any(s in k.lower() for s in sensitive_keys) and isinstance(v, (str, bytes)):
            result[k] = mask_secret(str(v))
        else:
            result[k] = v
    return result

def looks_like_jwt(s: str) -> bool:
    """Check if string looks like JWT token"""
    return s.count('.') == 2 and len(s) > 30 and is_valid_jwt(s)

def looks_like_api_key(s: str) -> bool:
    """Check if string looks like API key"""
    s = strip_value(s)
    patterns = [
        r'^[A-Za-z0-9]{20,}$',  # Random alphanumeric
        r'^sk-[A-Za-z0-9]{20,}$',  # OpenAI-style
        r'^[A-Za-z0-9_-]{20,}$',  # With underscores/dashes
        r'^[A-F0-9]{32,}$',  # Hex
    ]
    return any(re.match(p, s) for p in patterns)

def secret_strength_hint(s: str) -> Optional[str]:
    """Provide hint about secret strength"""
    t = strip_value(s)
    if not t:
        return None
    
    if looks_like_jwt(t) or looks_like_api_key(t):
        return None
    
    if len(t) < 16:
        return f"Secret is short ({len(t)} chars). Consider using longer random value (min 16)."
    
    if t.lower() in {'token', 'password', 'secret', '1234', '123456', 'admin', 'key', 'changeme'}:
        return "Secret looks weak/common. Use strong random value."
    
    has_upper = any(c.isupper() for c in t)
    has_lower = any(c.islower() for c in t)
    has_digit = any(c.isdigit() for c in t)
    has_special = any(not c.isalnum() for c in t)
    
    if not (has_upper and has_lower and has_digit and has_special):
        missing = []
        if not has_upper:
            missing.append("uppercase")
        if not has_lower:
            missing.append("lowercase")
        if not has_digit:
            missing.append("digit")
        if not has_special:
            missing.append("special char")
        return f"Secret missing: {', '.join(missing)}. Use strong random value with all types."
    
    return None

def repair_private_key(key: str) -> str:
    """Repair private key with common issues"""
    if not key:
        return key
    
    # Fix escaped newlines
    key = key.replace('\\n', '\n')
    key = key.replace('\\r\\n', '\n')
    key = key.replace('\\r', '\n')
    
    # Remove surrounding quotes
    if (key.startswith('"') and key.endswith('"')) or (key.startswith("'") and key.endswith("'")):
        key = key[1:-1]
    
    # Normalize line endings
    key = '\n'.join(line.rstrip() for line in key.split('\n') if line.rstrip())
    
    # Ensure proper PEM headers
    if '-----BEGIN PRIVATE KEY-----' not in key:
        key = '-----BEGIN PRIVATE KEY-----\n' + key
    if '-----END PRIVATE KEY-----' not in key:
        key = key + '\n-----END PRIVATE KEY-----'
    
    return key

def decrypt_value(encrypted: str, key: Optional[str] = None, 
                  context: Optional[Dict[str, Any]] = None,
                  algorithm: ConfigEncryptionAlgorithm = ConfigEncryptionAlgorithm.FERNET) -> Optional[str]:
    """Decrypt encrypted configuration value with context"""
    if not CRYPTO_AVAILABLE:
        logger.warning("Cryptography not available, returning original value")
        return encrypted
    
    if not key:
        key = os.getenv('CONFIG_ENCRYPTION_KEY') or os.getenv('ENCRYPTION_KEY')
    
    if not key:
        logger.warning("No encryption key found, returning original value")
        return encrypted
    
    try:
        if algorithm == ConfigEncryptionAlgorithm.FERNET:
            if not Fernet:
                raise EncryptionError("Fernet not available")
            
            # Handle Fernet key format
            if len(key) != 32 and not key.endswith('='):
                # Derive key
                kdf = PBKDF2HMAC(
                    algorithm=hashes.SHA256(),
                    length=32,
                    salt=b'salt_' + str(uuid.uuid4()).encode()[:8],
                    iterations=100000,
                    backend=default_backend()
                )
                key_bytes = kdf.derive(key.encode())
                key = base64.urlsafe_b64encode(key_bytes).decode()
            
            f = Fernet(key.encode() if isinstance(key, str) else key)
            decrypted = f.decrypt(encrypted.encode() if isinstance(encrypted, str) else encrypted)
            result = decrypted.decode()
            
            # Verify context if present
            if context:
                try:
                    payload = json.loads(result)
                    if isinstance(payload, dict) and "data" in payload and "context" in payload:
                        if payload["context"] != context:
                            logger.warning("Context mismatch in decryption")
                        return payload["data"]
                except:
                    pass
            
            return result
            
        elif algorithm == ConfigEncryptionAlgorithm.AES_GCM:
            # AES-GCM implementation
            import hashlib
            from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
            
            # Derive key
            key_bytes = hashlib.sha256(key.encode()).digest()
            
            # Decode encrypted data
            data = base64.b64decode(encrypted)
            nonce = data[:12]
            ciphertext = data[12:-16]
            tag = data[-16:]
            
            # Decrypt
            cipher = Cipher(algorithms.AES(key_bytes), modes.GCM(nonce, tag), backend=default_backend())
            decryptor = cipher.decryptor()
            result = decryptor.update(ciphertext) + decryptor.finalize()
            
            return result.decode()
            
        else:
            logger.warning(f"Unsupported encryption algorithm: {algorithm}")
            return encrypted
            
    except InvalidToken:
        logger.error("Invalid encryption token")
        return None
    except Exception as e:
        logger.error(f"Failed to decrypt value: {e}")
        return None

def encrypt_value(value: str, key: Optional[str] = None, 
                  context: Optional[Dict[str, Any]] = None,
                  algorithm: ConfigEncryptionAlgorithm = ConfigEncryptionAlgorithm.FERNET) -> Optional[str]:
    """Encrypt configuration value with context"""
    if not CRYPTO_AVAILABLE:
        logger.warning("Cryptography not available, returning original value")
        return value
    
    if not key:
        key = os.getenv('CONFIG_ENCRYPTION_KEY') or os.getenv('ENCRYPTION_KEY')
    
    if not key:
        logger.warning("No encryption key found, returning original value")
        return value
    
    try:
        if algorithm == ConfigEncryptionAlgorithm.FERNET:
            if not Fernet:
                raise EncryptionError("Fernet not available")
            
            # Add context to encryption (prevents replay attacks)
            if context:
                value = json.dumps({"data": value, "context": context})
            
            # Handle Fernet key format
            if len(key) != 32 and not key.endswith('='):
                # Derive key
                kdf = PBKDF2HMAC(
                    algorithm=hashes.SHA256(),
                    length=32,
                    salt=b'salt_' + str(uuid.uuid4()).encode()[:8],
                    iterations=100000,
                    backend=default_backend()
                )
                key_bytes = kdf.derive(key.encode())
                key = base64.urlsafe_b64encode(key_bytes).decode()
            
            f = Fernet(key.encode() if isinstance(key, str) else key)
            encrypted = f.encrypt(value.encode())
            return encrypted.decode()
            
        elif algorithm == ConfigEncryptionAlgorithm.AES_GCM:
            # AES-GCM implementation
            import hashlib
            from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
            
            # Derive key
            key_bytes = hashlib.sha256(key.encode()).digest()
            
            # Generate random nonce
            nonce = os.urandom(12)
            
            # Encrypt
            cipher = Cipher(algorithms.AES(key_bytes), modes.GCM(nonce), backend=default_backend())
            encryptor = cipher.encryptor()
            ciphertext = encryptor.update(value.encode()) + encryptor.finalize()
            
            # Combine nonce + ciphertext + tag
            result = base64.b64encode(nonce + ciphertext + encryptor.tag).decode()
            return result
            
        else:
            logger.warning(f"Unsupported encryption algorithm: {algorithm}")
            return value
            
    except Exception as e:
        logger.error(f"Failed to encrypt value: {e}")
        return None

def compute_hash(obj: Any) -> str:
    """Compute hash of object for change detection"""
    try:
        if isinstance(obj, (dict, list)):
            content = json.dumps(obj, sort_keys=True, default=str)
        elif hasattr(obj, '__dict__'):
            content = json.dumps(obj.__dict__, sort_keys=True, default=str)
        else:
            content = str(obj)
        return hashlib.sha256(content.encode()).hexdigest()
    except:
        return str(id(obj))

def deep_merge(dict1: Dict, dict2: Dict, overwrite: bool = True, 
               merge_lists: bool = False, deduplicate: bool = True) -> Dict:
    """Deep merge two dictionaries with advanced options"""
    result = dict1.copy()
    
    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value, overwrite, merge_lists, deduplicate)
        elif key in result and isinstance(result[key], list) and isinstance(value, list) and merge_lists:
            # Merge lists
            if deduplicate:
                # Remove duplicates while preserving order
                combined = result[key] + value
                seen = set()
                result[key] = []
                for item in combined:
                    item_str = json.dumps(item, sort_keys=True) if isinstance(item, (dict, list)) else str(item)
                    if item_str not in seen:
                        seen.add(item_str)
                        result[key].append(item)
            else:
                result[key] = result[key] + value
        elif key in result and overwrite:
            result[key] = value
        elif key not in result:
            result[key] = value
    
    return result

def get_correlation_id() -> str:
    """Get or generate correlation ID"""
    # Try to get from context (OpenTelemetry, etc.)
    return str(uuid.uuid4())

def structured_log(level: str, message: str, **kwargs):
    """Emit structured log"""
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
        getattr(logger, level.lower())(f"{message} - {json.dumps(log_data)}")

def parse_file_format(path: Union[str, Path]) -> ConfigFormat:
    """Parse file format from extension"""
    path = Path(path)
    ext = path.suffix.lower()
    
    if ext in {'.json'}:
        return ConfigFormat.JSON
    elif ext in {'.yaml', '.yml'}:
        return ConfigFormat.YAML
    elif ext in {'.toml'}:
        return ConfigFormat.TOML
    elif ext in {'.env', '.envrc'}:
        return ConfigFormat.ENV
    elif ext in {'.ini', '.cfg', '.conf'}:
        return ConfigFormat.INI
    elif ext in {'.xml'}:
        return ConfigFormat.XML
    elif ext in {'.properties'}:
        return ConfigFormat.PROPERTIES
    else:
        return ConfigFormat.JSON

def load_file_content(path: Path, format: Optional[ConfigFormat] = None) -> Dict[str, Any]:
    """Load configuration file with appropriate parser"""
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    
    content = path.read_text(encoding='utf-8')
    format = format or parse_file_format(path)
    
    if format == ConfigFormat.JSON:
        return json.loads(content)
    elif format == ConfigFormat.YAML:
        if not YAML_AVAILABLE:
            raise ImportError("PyYAML required for YAML config files")
        return yaml.safe_load(content)
    elif format == ConfigFormat.TOML:
        if not TOML_AVAILABLE:
            raise ImportError("toml required for TOML config files")
        return toml.loads(content)
    elif format == ConfigFormat.ENV:
        # Parse .env format
        result = {}
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, value = line.split('=', 1)
                result[key.strip()] = value.strip()
        return result
    else:
        raise ValueError(f"Unsupported file format: {format}")

# =============================================================================
# Timezone Helpers
# =============================================================================

RIYADH_TZ = timezone(timedelta(hours=3))

def utc_now() -> datetime:
    """Get current UTC datetime"""
    return datetime.now(timezone.utc)

def riyadh_now() -> datetime:
    """Get current Riyadh datetime"""
    return datetime.now(RIYADH_TZ)

def utc_iso(dt: Optional[datetime] = None) -> str:
    """Get UTC ISO string"""
    dt = dt or utc_now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def riyadh_iso(dt: Optional[datetime] = None) -> str:
    """Get Riyadh ISO string"""
    dt = dt or riyadh_now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(RIYADH_TZ).isoformat()

def to_riyadh(dt: Optional[datetime]) -> Optional[datetime]:
    """Convert to Riyadh timezone"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(RIYADH_TZ)

def to_riyadh_iso(dt: Optional[datetime]) -> Optional[str]:
    """Convert to Riyadh ISO string"""
    rd = to_riyadh(dt)
    return rd.isoformat() if rd else None

# =============================================================================
# Circuit Breaker
# =============================================================================

class CircuitBreaker:
    """Circuit breaker for external service calls with exponential backoff"""
    
    def __init__(self, name: str, threshold: int = 5, timeout: float = 60.0, 
                 half_open_requests: int = 3, success_threshold: int = 2):
        self.name = name
        self.threshold = threshold
        self.base_timeout = timeout
        self.half_open_requests = half_open_requests
        self.success_threshold = success_threshold
        
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0.0
        self.consecutive_failures = 0
        self.total_calls = 0
        self.total_failures = 0
        self.total_successes = 0
        self.half_open_calls = 0
        self.last_state_change = time.time()
        self._lock = RLock()
    
    def _get_timeout(self) -> float:
        """Calculate timeout with exponential backoff"""
        if self.consecutive_failures <= self.threshold:
            return self.base_timeout
        # Exponential backoff: base * 2^(failures-threshold)
        backoff_factor = 2 ** (self.consecutive_failures - self.threshold)
        return min(self.base_timeout * backoff_factor, 300.0)  # Cap at 5 minutes
    
    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            with self._lock:
                self.total_calls += 1
                
                if self.state == CircuitBreakerState.OPEN:
                    if time.time() - self.last_failure_time > self._get_timeout():
                        self.state = CircuitBreakerState.HALF_OPEN
                        self.half_open_calls = 0
                        self.success_count = 0
                        self.last_state_change = time.time()
                        logger.info(f"Circuit {self.name} entering HALF_OPEN")
                    else:
                        raise CircuitBreakerOpenError(f"Circuit {self.name} is OPEN")
                
                if self.state == CircuitBreakerState.HALF_OPEN:
                    if self.half_open_calls >= self.half_open_requests:
                        raise CircuitBreakerOpenError(f"Circuit {self.name} HALF_OPEN at capacity")
                    self.half_open_calls += 1
            
            try:
                result = func(*args, **kwargs)
                
                with self._lock:
                    self.total_successes += 1
                    self.success_count += 1
                    self.consecutive_failures = 0
                    
                    if self.state == CircuitBreakerState.HALF_OPEN:
                        if self.success_count >= self.success_threshold:
                            self.state = CircuitBreakerState.CLOSED
                            self.failure_count = 0
                            self.last_state_change = time.time()
                            logger.info(f"Circuit {self.name} CLOSED")
                    
                    config_requests_total.labels(source=self.name, status='success').inc()
                
                return result
                
            except Exception as e:
                with self._lock:
                    self.total_failures += 1
                    self.failure_count += 1
                    self.consecutive_failures += 1
                    self.last_failure_time = time.time()
                    
                    if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.threshold:
                        self.state = CircuitBreakerState.OPEN
                        self.last_state_change = time.time()
                        logger.warning(f"Circuit {self.name} OPEN after {self.failure_count} failures")
                    elif self.state == CircuitBreakerState.HALF_OPEN:
                        self.state = CircuitBreakerState.OPEN
                        self.last_state_change = time.time()
                        logger.warning(f"Circuit {self.name} re-OPEN from HALF_OPEN")
                    
                    config_errors_total.labels(type=self.name).inc()
                
                raise e
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            with self._lock:
                self.total_calls += 1
                
                if self.state == CircuitBreakerState.OPEN:
                    if time.time() - self.last_failure_time > self._get_timeout():
                        self.state = CircuitBreakerState.HALF_OPEN
                        self.half_open_calls = 0
                        self.success_count = 0
                        self.last_state_change = time.time()
                        logger.info(f"Circuit {self.name} entering HALF_OPEN")
                    else:
                        raise CircuitBreakerOpenError(f"Circuit {self.name} is OPEN")
                
                if self.state == CircuitBreakerState.HALF_OPEN:
                    if self.half_open_calls >= self.half_open_requests:
                        raise CircuitBreakerOpenError(f"Circuit {self.name} HALF_OPEN at capacity")
                    self.half_open_calls += 1
            
            try:
                result = await func(*args, **kwargs)
                
                with self._lock:
                    self.total_successes += 1
                    self.success_count += 1
                    self.consecutive_failures = 0
                    
                    if self.state == CircuitBreakerState.HALF_OPEN:
                        if self.success_count >= self.success_threshold:
                            self.state = CircuitBreakerState.CLOSED
                            self.failure_count = 0
                            self.last_state_change = time.time()
                            logger.info(f"Circuit {self.name} CLOSED")
                    
                    config_loads_total.labels(source=self.name, status='success').inc()
                
                return result
                
            except Exception as e:
                with self._lock:
                    self.total_failures += 1
                    self.failure_count += 1
                    self.consecutive_failures += 1
                    self.last_failure_time = time.time()
                    
                    if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.threshold:
                        self.state = CircuitBreakerState.OPEN
                        self.last_state_change = time.time()
                        logger.warning(f"Circuit {self.name} OPEN after {self.failure_count} failures")
                    elif self.state == CircuitBreakerState.HALF_OPEN:
                        self.state = CircuitBreakerState.OPEN
                        self.last_state_change = time.time()
                        logger.warning(f"Circuit {self.name} re-OPEN from HALF_OPEN")
                    
                    config_errors_total.labels(type=self.name).inc()
                
                raise e
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return wrapper
    
    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics"""
        with self._lock:
            return {
                "name": self.name,
                "state": self.state.value,
                "total_calls": self.total_calls,
                "total_successes": self.total_successes,
                "total_failures": self.total_failures,
                "success_rate": self.total_successes / self.total_calls if self.total_calls > 0 else 1.0,
                "current_failure_count": self.failure_count,
                "current_success_count": self.success_count,
                "consecutive_failures": self.consecutive_failures,
                "timeout_sec": self._get_timeout(),
                "half_open_requests": self.half_open_requests,
                "success_threshold": self.success_threshold,
                "last_state_change": datetime.fromtimestamp(self.last_state_change).isoformat(),
                "state_duration_sec": time.time() - self.last_state_change,
            }
    
    def reset(self) -> None:
        """Reset circuit breaker to closed state"""
        with self._lock:
            self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0
            self.success_count = 0
            self.consecutive_failures = 0
            self.last_state_change = time.time()
            logger.info(f"Circuit {self.name} manually reset to CLOSED")

# =============================================================================
# Configuration Cache
# =============================================================================

class ConfigCache:
    """Multi-level cache for configuration values"""
    
    def __init__(self, maxsize: int = 1000, ttl: int = 300, 
                 enable_compression: bool = False,
                 compression_level: int = 6):
        self.maxsize = maxsize
        self.ttl = ttl
        self.enable_compression = enable_compression
        self.compression_level = compression_level
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._access_times: Dict[str, float] = {}
        self._lock = RLock()
        self.hits = 0
        self.misses = 0
        self.evictions = 0
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        with self._lock:
            now = time.time()
            if key in self._cache:
                value, expiry = self._cache[key]
                if now < expiry:
                    self._access_times[key] = now
                    self.hits += 1
                    config_cache_hits.labels(level='memory').inc()
                    
                    # Decompress if needed
                    if self.enable_compression and isinstance(value, bytes):
                        try:
                            value = zlib.decompress(value).decode('utf-8')
                            value = json.loads(value)
                        except:
                            pass
                    
                    return value
                else:
                    del self._cache[key]
                    del self._access_times[key]
                    self.evictions += 1
            self.misses += 1
            config_cache_misses.labels(level='memory').inc()
            return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache"""
        with self._lock:
            # Compress if enabled
            if self.enable_compression:
                try:
                    if isinstance(value, (dict, list)):
                        value = json.dumps(value)
                    if isinstance(value, str):
                        value = zlib.compress(value.encode('utf-8'), level=self.compression_level)
                except:
                    pass
            
            # Evict if full
            if len(self._cache) >= self.maxsize and key not in self._cache:
                # Remove oldest
                if self._access_times:
                    oldest_key = min(self._access_times.items(), key=lambda x: x[1])[0]
                    del self._cache[oldest_key]
                    del self._access_times[oldest_key]
                    self.evictions += 1
            
            self._cache[key] = (value, time.time() + (ttl or self.ttl))
            self._access_times[key] = time.time()
    
    def delete(self, key: str) -> None:
        """Delete from cache"""
        with self._lock:
            self._cache.pop(key, None)
            self._access_times.pop(key, None)
    
    def clear(self) -> None:
        """Clear cache"""
        with self._lock:
            self._cache.clear()
            self._access_times.clear()
            self.hits = 0
            self.misses = 0
            self.evictions = 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        with self._lock:
            total = self.hits + self.misses
            return {
                "size": len(self._cache),
                "maxsize": self.maxsize,
                "ttl": self.ttl,
                "hits": self.hits,
                "misses": self.misses,
                "evictions": self.evictions,
                "hit_rate": self.hits / total if total > 0 else 0,
                "compression_enabled": self.enable_compression,
                "compression_level": self.compression_level if self.enable_compression else None,
            }
    
    def get_or_set(self, key: str, generator: Callable[[], Any], ttl: Optional[int] = None) -> Any:
        """Get from cache or generate and set"""
        value = self.get(key)
        if value is None:
            value = generator()
            self.set(key, value, ttl)
        return value

# =============================================================================
# Pydantic Models (Optional)
# =============================================================================

if PYDANTIC_AVAILABLE:
    class SettingsBaseModel(BaseModel):
        """Base model with common functionality"""
        
        if PYDANTIC_V2:
            model_config = ConfigDict(
                validate_assignment=True,
                arbitrary_types_allowed=True,
                json_encoders={
                    datetime: lambda v: v.isoformat(),
                    Enum: lambda v: v.value,
                    Path: lambda v: str(v),
                    timedelta: lambda v: v.total_seconds(),
                    bytes: lambda v: base64.b64encode(v).decode(),
                },
                use_enum_values=True,
                extra='forbid',
            )
        else:
            class Config:
                validate_assignment = True
                arbitrary_types_allowed = True
                json_encoders = {
                    datetime: lambda v: v.isoformat(),
                    Enum: lambda v: v.value,
                    Path: lambda v: str(v),
                    timedelta: lambda v: v.total_seconds(),
                    bytes: lambda v: base64.b64encode(v).decode(),
                }
                use_enum_values = True
                extra = 'forbid'
        
        def dict(self, *args, **kwargs):
            """Override dict to handle Enums"""
            kwargs.setdefault('exclude_none', True)
            if PYDANTIC_V2:
                return super().model_dump(*args, **kwargs)
            return super().dict(*args, **kwargs)
        
        def mask_sensitive(self) -> Dict[str, Any]:
            """Mask sensitive fields"""
            data = self.dict()
            return mask_secret_dict(data)
    
    class DatabaseConfig(SettingsBaseModel):
        """Database configuration"""
        url: str = Field(..., description="Database connection URL")
        pool_size: int = Field(20, ge=1, le=200, description="Connection pool size")
        max_overflow: int = Field(10, ge=0, le=100, description="Max overflow connections")
        pool_timeout: int = Field(30, ge=1, le=300, description="Pool timeout seconds")
        pool_recycle: int = Field(3600, ge=60, le=86400, description="Connection recycle seconds")
        echo: bool = Field(False, description="Echo SQL queries")
        ssl_mode: Optional[str] = Field(None, description="SSL mode")
        statement_timeout: int = Field(30000, ge=1000, le=300000, description="Statement timeout ms")
        lock_timeout: int = Field(5000, ge=100, le=60000, description="Lock timeout ms")
        application_name: str = Field("tfb", description="Application name")
        
        if PYDANTIC_V2:
            @field_validator('url')
            @classmethod
            def validate_url(cls, v):
                valid_schemes = {'postgresql', 'mysql', 'sqlite', 'oracle', 'mssql'}
                scheme = v.split('://')[0] if '://' in v else None
                if scheme not in valid_schemes:
                    raise ValueError(f'Invalid database URL scheme: {scheme}. Must be one of {valid_schemes}')
                return v
        else:
            @validator('url')
            def validate_url(cls, v):
                valid_schemes = {'postgresql', 'mysql', 'sqlite', 'oracle', 'mssql'}
                scheme = v.split('://')[0] if '://' in v else None
                if scheme not in valid_schemes:
                    raise ValueError(f'Invalid database URL scheme: {scheme}. Must be one of {valid_schemes}')
                return v
    
    class RedisConfig(SettingsBaseModel):
        """Redis configuration"""
        url: str = Field(..., description="Redis connection URL")
        socket_timeout: float = Field(5.0, ge=0.5, le=30.0, description="Socket timeout seconds")
        socket_connect_timeout: float = Field(5.0, ge=0.5, le=30.0, description="Connect timeout seconds")
        retry_on_timeout: bool = Field(True, description="Retry on timeout")
        max_connections: int = Field(50, ge=1, le=500, description="Max connections")
        ssl: bool = Field(False, description="Use SSL")
        ssl_cert_reqs: Optional[str] = Field(None, description="SSL certificate requirements")
        health_check_interval: int = Field(30, ge=5, le=300, description="Health check interval")
        decode_responses: bool = Field(True, description="Decode responses")
        
        if PYDANTIC_V2:
            @field_validator('url')
            @classmethod
            def validate_url(cls, v):
                if not v.startswith(('redis://', 'rediss://')):
                    raise ValueError('Invalid Redis URL scheme')
                return v
        else:
            @validator('url')
            def validate_url(cls, v):
                if not v.startswith(('redis://', 'rediss://')):
                    raise ValueError('Invalid Redis URL scheme')
                return v
    
    class CacheConfig(SettingsBaseModel):
        """Cache configuration"""
        backend: CacheBackend = Field(CacheBackend.MEMORY, description="Cache backend")
        default_ttl: int = Field(300, ge=1, le=86400, description="Default TTL seconds")
        prefix: str = Field("tfb:", description="Cache key prefix")
        compression: bool = Field(False, description="Enable compression")
        compression_level: int = Field(6, ge=1, le=9, description="Compression level")
        max_size: Optional[int] = Field(None, ge=1, description="Max cache size (items)")
        max_memory: Optional[str] = Field(None, description="Max memory (e.g., '1gb')")
        
        # Redis specific
        redis_url: Optional[str] = Field(None, description="Redis URL")
        
        # Memcached specific
        memcached_servers: List[str] = Field(default_factory=list, description="Memcached servers")
        
        if PYDANTIC_V2:
            @field_validator('redis_url')
            @classmethod
            def validate_redis_url(cls, v, info):
                backend = info.data.get('backend')
                if backend == CacheBackend.REDIS and not v:
                    raise ValueError('Redis URL required for Redis backend')
                return v
        else:
            @validator('redis_url')
            def validate_redis_url(cls, v, values):
                if values.get('backend') == CacheBackend.REDIS and not v:
                    raise ValueError('Redis URL required for Redis backend')
                return v
    
    class ProviderConfig(SettingsBaseModel):
        """Provider configuration"""
        name: ProviderType = Field(..., description="Provider name")
        api_key: Optional[SecretStr] = Field(None, description="API key")
        api_secret: Optional[SecretStr] = Field(None, description="API secret")
        base_url: Optional[str] = Field(None, description="Base URL")
        timeout: float = Field(30.0, ge=1.0, le=300.0, description="Timeout seconds")
        connect_timeout: float = Field(10.0, ge=1.0, le=60.0, description="Connect timeout")
        read_timeout: float = Field(30.0, ge=1.0, le=300.0, description="Read timeout")
        retries: int = Field(3, ge=0, le=10, description="Retry count")
        retry_backoff: float = Field(1.0, ge=0.1, le=60.0, description="Retry backoff factor")
        priority: int = Field(10, ge=1, le=100, description="Priority (lower = higher)")
        enabled: bool = Field(True, description="Provider enabled")
        rate_limit: int = Field(60, ge=1, le=10000, description="Requests per minute")
        rate_limit_burst: int = Field(10, ge=1, le=100, description="Rate limit burst")
        circuit_breaker_threshold: int = Field(5, ge=1, le=100, description="Circuit breaker threshold")
        circuit_breaker_timeout: int = Field(60, ge=5, le=3600, description="Circuit breaker timeout")
        weight: float = Field(1.0, ge=0.1, le=10.0, description="Provider weight")
        tags: List[str] = Field(default_factory=list, description="Provider tags")
        
        if PYDANTIC_V2:
            @field_validator('base_url')
            @classmethod
            def validate_url(cls, v):
                if v and not is_valid_url(v):
                    raise ValueError('Invalid base URL')
                return v
            
            def dict(self, *args, **kwargs):
                d = super().dict(*args, **kwargs)
                if 'api_key' in d and d['api_key']:
                    d['api_key'] = '***'
                if 'api_secret' in d and d['api_secret']:
                    d['api_secret'] = '***'
                return d
        else:
            @validator('base_url')
            def validate_url(cls, v):
                if v and not is_valid_url(v):
                    raise ValueError('Invalid base URL')
                return v
    
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
        webhook: bool = Field(True, description="Webhook enabled")
        websocket: bool = Field(True, description="WebSocket enabled")
        streaming: bool = Field(False, description="Streaming enabled")
        batch_processing: bool = Field(True, description="Batch processing enabled")
        
        # Gradual rollout
        rollout_percentage: int = Field(100, ge=0, le=100, description="Rollout percentage")
        enabled_environments: List[Environment] = Field(
            default_factory=lambda: [Environment.PRODUCTION, Environment.STAGING],
            description="Environments where enabled"
        )
        
        # Feature flags dictionary
        flags: Dict[str, Any] = Field(default_factory=dict, description="Custom feature flags")
        
        def is_enabled(self, feature: str, environment: Optional[Environment] = None) -> bool:
            """Check if feature is enabled"""
            # Check environment
            if environment and environment not in self.enabled_environments:
                return False
            
            # Check standard flags
            if hasattr(self, feature):
                return getattr(self, feature)
            
            # Check custom flags
            return self.flags.get(feature, False)
        
        def is_enabled_for_user(self, feature: str, user_id: str, environment: Optional[Environment] = None) -> bool:
            """Check if feature is enabled for specific user (consistent hashing)"""
            if not self.is_enabled(feature, environment):
                return False
            
            # Use consistent hashing for rollout percentage
            feature_config = getattr(self, feature, None)
            if feature_config is None and feature in self.flags:
                feature_config = self.flags[feature]
            
            if isinstance(feature_config, dict) and 'rollout_percentage' in feature_config:
                percentage = feature_config['rollout_percentage']
                if percentage < 100:
                    # Consistent hashing
                    hash_val = int(hashlib.md5(f"{feature}:{user_id}".encode()).hexdigest(), 16) % 100
                    return hash_val < percentage
            
            return True
    
    class SecurityConfig(SettingsBaseModel):
        """Security configuration"""
        auth_header_name: str = Field("X-APP-TOKEN", description="Auth header name")
        allow_query_token: bool = Field(False, description="Allow token in query params")
        allow_cookie_token: bool = Field(False, description="Allow token in cookies")
        open_mode: bool = Field(False, description="Open mode (no auth)")
        cors_origins: List[str] = Field(default_factory=list, description="CORS origins")
        cors_allow_credentials: bool = Field(True, description="Allow credentials")
        cors_max_age: int = Field(3600, ge=0, le=86400, description="CORS max age")
        cors_allow_headers: List[str] = Field(default_factory=list, description="CORS allowed headers")
        cors_expose_headers: List[str] = Field(default_factory=list, description="CORS exposed headers")
        rate_limit_rpm: int = Field(240, ge=30, le=10000, description="Rate limit RPM")
        rate_limit_global: bool = Field(True, description="Global rate limit")
        jwt_secret: Optional[SecretStr] = Field(None, description="JWT secret")
        jwt_algorithm: str = Field("HS256", description="JWT algorithm")
        jwt_expiry: int = Field(3600, ge=300, le=86400, description="JWT expiry seconds")
        session_timeout: int = Field(3600, ge=300, le=86400, description="Session timeout seconds")
        session_cookie_name: str = Field("session", description="Session cookie name")
        session_cookie_secure: bool = Field(True, description="Secure cookie flag")
        session_cookie_httponly: bool = Field(True, description="HttpOnly cookie flag")
        session_cookie_samesite: str = Field("lax", description="SameSite cookie attribute")
        api_key_header: Optional[str] = Field(None, description="API key header")
        api_key_param: Optional[str] = Field(None, description="API key query parameter")
        basic_auth_realm: str = Field("Tadawul Fast Bridge", description="Basic auth realm")
        
        if PYDANTIC_V2:
            @field_validator('cors_origins')
            @classmethod
            def validate_cors_origins(cls, v):
                for origin in v:
                    if origin != '*' and not is_valid_url(origin):
                        raise ValueError(f'Invalid CORS origin: {origin}')
                return v
        else:
            @validator('cors_origins')
            def validate_cors_origins(cls, v):
                for origin in v:
                    if origin != '*' and not is_valid_url(origin):
                        raise ValueError(f'Invalid CORS origin: {origin}')
                return v
    
    class LoggingConfig(SettingsBaseModel):
        """Logging configuration"""
        level: LogLevel = Field(LogLevel.INFO, description="Log level")
        format: str = Field("json", description="Log format (json, text, color)")
        file: Optional[Path] = Field(None, description="Log file path")
        max_bytes: int = Field(104857600, ge=1048576, le=1073741824, description="Max log file bytes")
        backup_count: int = Field(10, ge=1, le=100, description="Log backup count")
        access_log: bool = Field(True, description="Access log enabled")
        access_log_format: str = Field("combined", description="Access log format")
        error_log: bool = Field(True, description="Error log enabled")
        slow_query_threshold: int = Field(1000, ge=100, le=30000, description="Slow query threshold ms")
        structured: bool = Field(True, description="Structured logging")
        correlation_id_header: str = Field("X-Correlation-ID", description="Correlation ID header")
        
        if PYDANTIC_V2:
            @field_validator('format')
            @classmethod
            def validate_format(cls, v):
                if v not in {'json', 'text', 'color', 'console'}:
                    raise ValueError('Invalid log format')
                return v
        else:
            @validator('format')
            def validate_format(cls, v):
                if v not in {'json', 'text', 'color', 'console'}:
                    raise ValueError('Invalid log format')
                return v
    
    class MonitoringConfig(SettingsBaseModel):
        """Monitoring configuration"""
        metrics_port: int = Field(9090, ge=1024, le=65535, description="Metrics port")
        metrics_path: str = Field("/metrics", description="Metrics path")
        health_check_path: str = Field("/health", description="Health check path")
        health_check_interval: int = Field(30, ge=5, le=300, description="Health check interval")
        health_check_timeout: int = Field(5, ge=1, le=30, description="Health check timeout")
        health_check_fail_fast: bool = Field(True, description="Fail fast on health check")
        readiness_path: str = Field("/ready", description="Readiness path")
        liveness_path: str = Field("/live", description="Liveness path")
        sentry_dsn: Optional[str] = Field(None, description="Sentry DSN")
        sentry_environment: Optional[str] = Field(None, description="Sentry environment")
        sentry_sample_rate: float = Field(1.0, ge=0.0, le=1.0, description="Sentry sample rate")
        datadog_api_key: Optional[SecretStr] = Field(None, description="Datadog API key")
        datadog_app_key: Optional[SecretStr] = Field(None, description="Datadog app key")
        datadog_site: str = Field("datadoghq.com", description="Datadog site")
        newrelic_license_key: Optional[SecretStr] = Field(None, description="New Relic license key")
        newrelic_app_name: str = Field("Tadawul Fast Bridge", description="New Relic app name")
        prometheus_enabled: bool = Field(True, description="Prometheus metrics enabled")
        opentelemetry_enabled: bool = Field(False, description="OpenTelemetry enabled")
        opentelemetry_endpoint: Optional[str] = Field(None, description="OpenTelemetry endpoint")
    
    class PerformanceConfig(SettingsBaseModel):
        """Performance configuration"""
        workers: Optional[int] = Field(None, ge=1, le=64, description="Worker count")
        worker_class: str = Field("uvicorn.workers.UvicornWorker", description="Worker class")
        worker_connections: int = Field(1000, ge=10, le=10000, description="Worker connections")
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
        historical_batch_size: int = Field(10, ge=1, le=100, description="Historical batch size")
        news_batch_size: int = Field(20, ge=1, le=200, description="News batch size")
        
        # Connection pools
        connection_pool_size: int = Field(10, ge=1, le=100, description="Connection pool size")
        connection_pool_max_overflow: int = Field(20, ge=0, le=200, description="Connection pool max overflow")
        connection_pool_recycle: int = Field(3600, ge=60, le=86400, description="Connection pool recycle")
        
        # Async
        async_thread_pool_size: int = Field(20, ge=1, le=200, description="Async thread pool size")
        async_queue_size: int = Field(100, ge=10, le=1000, description="Async queue size")
        async_task_timeout: int = Field(60, ge=5, le=600, description="Async task timeout")
        
        # Rate limiting
        rate_limit_enabled: bool = Field(True, description="Rate limiting enabled")
        rate_limit_strategy: str = Field("token_bucket", description="Rate limit strategy")
        rate_limit_redis_url: Optional[str] = Field(None, description="Rate limit Redis URL")
    
    class DeploymentConfig(SettingsBaseModel):
        """Deployment configuration"""
        environment: Environment = Field(Environment.PRODUCTION, description="Environment")
        version: str = Field(CONFIG_VERSION, description="Application version")
        service_name: str = Field("Tadawul Fast Bridge", description="Service name")
        timezone: str = Field("Asia/Riyadh", description="Timezone")
        public_base_url: Optional[str] = Field(None, description="Public base URL")
        internal_base_url: Optional[str] = Field(None, description="Internal base URL")
        render_service_name: Optional[str] = Field(None, description="Render service name")
        render_external_url: Optional[str] = Field(None, description="Render external URL")
        kubernetes_namespace: Optional[str] = Field(None, description="Kubernetes namespace")
        kubernetes_pod_name: Optional[str] = Field(None, description="Kubernetes pod name")
        docker_container_id: Optional[str] = Field(None, description="Docker container ID")
        region: str = Field("us-east-1", description="AWS region")
        availability_zone: Optional[str] = Field(None, description="Availability zone")
        instance_id: Optional[str] = Field(None, description="Instance ID")
        hostname: str = Field(socket.gethostname(), description="Hostname")
        
        if PYDANTIC_V2:
            @field_validator('timezone')
            @classmethod
            def validate_timezone(cls, v):
                try:
                    import pytz
                    pytz.timezone(v)
                except:
                    pass  # Non-critical
                return v
        else:
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
        credentials: Optional[SecretStr] = Field(None, description="Credentials JSON")
        service_account_email: Optional[str] = Field(None, description="Service account email")
        token_uri: str = Field("https://oauth2.googleapis.com/token", description="Token URI")
        auth_uri: str = Field("https://accounts.google.com/o/oauth2/auth", description="Auth URI")
        scopes: List[str] = Field(
            default_factory=lambda: ["https://www.googleapis.com/auth/spreadsheets"],
            description="OAuth scopes"
        )
        
        if PYDANTIC_V2:
            @field_validator('credentials')
            @classmethod
            def validate_credentials(cls, v):
                if v and v.get_secret_value().startswith('{'):
                    try:
                        json.loads(v.get_secret_value())
                    except:
                        raise ValueError('Invalid credentials JSON')
                return v
        else:
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
        api_key: Optional[SecretStr] = Field(None, description="Argaam API key")
        timeout: int = Field(30, ge=5, le=120, description="Timeout seconds")
        retries: int = Field(3, ge=0, le=10, description="Retry count")
        
        if PYDANTIC_V2:
            @field_validator('quote_url')
            @classmethod
            def validate_url(cls, v):
                if v and not is_valid_url(v):
                    raise ValueError('Invalid Argaam URL')
                return v
        else:
            @validator('quote_url')
            def validate_url(cls, v):
                if v and not is_valid_url(v):
                    raise ValueError('Invalid Argaam URL')
                return v
    
    class ChaosConfig(SettingsBaseModel):
        """Chaos engineering configuration"""
        enabled: bool = Field(False, description="Chaos enabled")
        failure_rate: float = Field(0.0, ge=0.0, le=1.0, description="Failure rate")
        latency_ms: int = Field(0, ge=0, le=30000, description="Latency to inject")
        exception_rate: float = Field(0.0, ge=0.0, le=1.0, description="Exception rate")
        error_codes: List[int] = Field(default_factory=list, description="HTTP error codes")
        endpoints: List[str] = Field(default_factory=list, description="Target endpoints")
        providers: List[str] = Field(default_factory=list, description="Target providers")
        schedule: Optional[str] = Field(None, description="Cron schedule")
        
        if PYDANTIC_V2:
            @field_validator('failure_rate')
            @classmethod
            def validate_failure_rate(cls, v):
                if v < 0 or v > 1:
                    raise ValueError('Failure rate must be between 0 and 1')
                return v
            
            @field_validator('exception_rate')
            @classmethod
            def validate_exception_rate(cls, v):
                if v < 0 or v > 1:
                    raise ValueError('Exception rate must be between 0 and 1')
                return v
        else:
            @validator('failure_rate')
            def validate_failure_rate(cls, v):
                if v < 0 or v > 1:
                    raise ValueError('Failure rate must be between 0 and 1')
                return v
            
            @validator('exception_rate')
            def validate_exception_rate(cls, v):
                if v < 0 or v > 1:
                    raise ValueError('Exception rate must be between 0 and 1')
                return v
    
    class AITestConfig(SettingsBaseModel):
        """AI/ML test configuration"""
        enabled: bool = Field(False, description="AI testing enabled")
        model_path: Optional[Path] = Field(None, description="Model path")
        model_version: str = Field("latest", description="Model version")
        confidence_threshold: float = Field(0.7, ge=0.0, le=1.0, description="Confidence threshold")
        batch_size: int = Field(32, ge=1, le=256, description="Inference batch size")
        max_tokens: int = Field(2048, ge=64, le=8192, description="Max tokens")
        temperature: float = Field(0.7, ge=0.0, le=2.0, description="Temperature")
        top_p: float = Field(0.9, ge=0.0, le=1.0, description="Top p")
        top_k: int = Field(40, ge=1, le=100, description="Top k")
        seed: Optional[int] = Field(None, description="Random seed")
        cache_responses: bool = Field(True, description="Cache responses")
        cache_ttl: int = Field(3600, ge=60, le=86400, description="Cache TTL")
        
        if PYDANTIC_V2:
            @field_validator('model_path')
            @classmethod
            def validate_path(cls, v):
                if v and not v.exists():
                    raise ValueError(f'Model path does not exist: {v}')
                return v
        else:
            @validator('model_path')
            def validate_path(cls, v):
                if v and not v.exists():
                    raise ValueError(f'Model path does not exist: {v}')
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
    build_timestamp: str = CONFIG_BUILD_TIMESTAMP
    
    # Deployment
    environment: Environment = Environment.PRODUCTION
    app_version: str = "dev"
    service_name: str = "Tadawul Fast Bridge"
    service_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timezone: str = "Asia/Riyadh"
    
    # Logging
    log_level: LogLevel = LogLevel.INFO
    log_format: str = "json"
    log_file: Optional[Path] = None
    log_correlation_id: bool = True
    
    # URLs
    backend_base_url: str = "http://localhost:8000"
    public_base_url: Optional[str] = None
    internal_base_url: Optional[str] = None
    render_service_name: Optional[str] = None
    render_external_url: Optional[str] = None
    
    # Security
    auth_header_name: str = "X-APP-TOKEN"
    app_token: Optional[str] = None
    backup_app_token: Optional[str] = None
    allow_query_token: bool = False
    allow_cookie_token: bool = False
    open_mode: bool = False
    cors_origins: List[str] = field(default_factory=list)
    cors_allow_credentials: bool = True
    cors_max_age: int = 3600
    rate_limit_rpm: int = 240
    rate_limit_global: bool = True
    rate_limit_redis_url: Optional[str] = None
    
    # Feature flags
    ai_analysis_enabled: bool = True
    advisor_enabled: bool = True
    advanced_enabled: bool = True
    rate_limit_enabled: bool = True
    cache_enabled: bool = True
    circuit_breaker_enabled: bool = True
    metrics_enabled: bool = True  # This is the canonical definition
    tracing_enabled: bool = False
    profiling_enabled: bool = False
    chaos_enabled: bool = False
    webhook_enabled: bool = True
    websocket_enabled: bool = True
    streaming_enabled: bool = False
    batch_processing_enabled: bool = True
    
    # Feature flag dictionary
    feature_flags: Dict[str, Any] = field(default_factory=dict)
    
    # Chaos engineering
    chaos_failure_rate: float = 0.0
    chaos_latency_ms: int = 0
    chaos_exception_rate: float = 0.0
    chaos_error_codes: List[int] = field(default_factory=list)
    chaos_endpoints: List[str] = field(default_factory=list)
    chaos_providers: List[str] = field(default_factory=list)
    
    # Providers
    enabled_providers: List[str] = field(default_factory=lambda: ["eodhd", "finnhub"])
    ksa_providers: List[str] = field(default_factory=lambda: ["yahoo_chart", "argaam"])
    primary_provider: str = "eodhd"
    provider_weights: Dict[str, float] = field(default_factory=dict)
    provider_timeouts: Dict[str, float] = field(default_factory=dict)
    provider_retries: Dict[str, int] = field(default_factory=dict)
    
    # Provider credentials (masked in logs)
    eodhd_api_key: Optional[str] = None
    finnhub_api_key: Optional[str] = None
    alphavantage_api_key: Optional[str] = None
    polygon_api_key: Optional[str] = None
    iexcloud_api_key: Optional[str] = None
    tradier_api_key: Optional[str] = None
    marketaux_api_key: Optional[str] = None
    newsapi_api_key: Optional[str] = None
    twelve_data_api_key: Optional[str] = None
    argaam_api_key: Optional[str] = None
    
    # Provider configurations
    provider_configs: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    # Performance
    http_timeout_sec: float = 45.0
    http_connect_timeout: float = 10.0
    http_read_timeout: float = 30.0
    cache_ttl_sec: int = 20
    batch_concurrency: int = 5
    ai_batch_size: int = 20
    adv_batch_size: int = 25
    quote_batch_size: int = 50
    historical_batch_size: int = 10
    news_batch_size: int = 20
    
    # Connection pools
    connection_pool_size: int = 10
    connection_pool_max_overflow: int = 20
    connection_pool_recycle: int = 3600
    async_thread_pool_size: int = 20
    async_queue_size: int = 100
    async_task_timeout: int = 60
    
    # Database
    database_url: Optional[str] = None
    database_pool_size: int = 20
    database_max_overflow: int = 10
    database_pool_timeout: int = 30
    database_pool_recycle: int = 3600
    database_echo: bool = False
    database_ssl_mode: Optional[str] = None
    database_statement_timeout: int = 30000
    database_lock_timeout: int = 5000
    database_application_name: str = "tfb"
    
    # Cache
    cache_backend: CacheBackend = CacheBackend.MEMORY
    cache_default_ttl: int = 300
    cache_prefix: str = "tfb:"
    cache_compression: bool = False
    cache_compression_level: int = 6
    cache_max_size: Optional[int] = None
    cache_max_memory: Optional[str] = None
    
    # Redis
    redis_url: Optional[str] = None
    redis_socket_timeout: float = 5.0
    redis_socket_connect_timeout: float = 5.0
    redis_retry_on_timeout: bool = True
    redis_max_connections: int = 50
    redis_ssl: bool = False
    redis_health_check_interval: int = 30
    
    # Memcached
    memcached_servers: List[str] = field(default_factory=list)
    
    # Integrations
    spreadsheet_id: Optional[str] = None
    google_creds: Optional[str] = None
    google_service_account: Optional[str] = None
    google_token_uri: str = "https://oauth2.googleapis.com/token"
    google_auth_uri: str = "https://accounts.google.com/o/oauth2/auth"
    google_scopes: List[str] = field(
        default_factory=lambda: ["https://www.googleapis.com/auth/spreadsheets"]
    )
    argaam_quote_url: Optional[str] = None
    argaam_timeout: int = 30
    argaam_retries: int = 3
    
    # Monitoring
    metrics_port: int = 9090
    metrics_path: str = "/metrics"
    health_check_path: str = "/health"
    health_check_interval: int = 30
    health_check_timeout: int = 5
    health_check_fail_fast: bool = True
    readiness_path: str = "/ready"
    liveness_path: str = "/live"
    sentry_dsn: Optional[str] = None
    sentry_environment: Optional[str] = None
    sentry_sample_rate: float = 1.0
    datadog_api_key: Optional[str] = None
    datadog_app_key: Optional[str] = None
    datadog_site: str = "datadoghq.com"
    newrelic_license_key: Optional[str] = None
    newrelic_app_name: str = "Tadawul Fast Bridge"
    prometheus_enabled: bool = True
    opentelemetry_enabled: bool = False
    opentelemetry_endpoint: Optional[str] = None
    
    # AI/ML Testing
    ai_testing_enabled: bool = False
    ai_model_path: Optional[Path] = None
    ai_model_version: str = "latest"
    ai_confidence_threshold: float = 0.7
    ai_batch_size: int = 32
    ai_max_tokens: int = 2048
    ai_temperature: float = 0.7
    ai_top_p: float = 0.9
    ai_top_k: int = 40
    ai_seed: Optional[int] = None
    ai_cache_responses: bool = True
    ai_cache_ttl: int = 3600
    
    # Encryption
    encryption_algorithm: ConfigEncryptionAlgorithm = ConfigEncryptionAlgorithm.FERNET
    encryption_key: Optional[str] = None
    
    # Diagnostics
    boot_errors: Tuple[str, ...] = field(default_factory=tuple)
    boot_warnings: Tuple[str, ...] = field(default_factory=tuple)
    config_sources: Dict[str, ConfigSource] = field(default_factory=dict)
    config_history: List[Dict[str, Any]] = field(default_factory=list)
    
    # =========================================================================
    # Factory Methods
    # =========================================================================
    @classmethod
    def from_env(cls, env_prefix: str = "TFB_") -> "Settings":
        """Create settings from environment variables"""
        start_time = time.time()
        config_sources = {}
        warnings_list = []
        
        # Determine environment
        env_name = strip_value(
            os.getenv(f"{env_prefix}ENV") or 
            os.getenv("APP_ENV") or 
            os.getenv("ENVIRONMENT") or 
            "production"
        ).lower()
        environment = Environment.from_string(env_name)
        config_sources['environment'] = ConfigSource.ENV_VAR
        
        # Service identification
        service_id = strip_value(os.getenv(f"{env_prefix}SERVICE_ID") or str(uuid.uuid4()))
        
        # Tokens
        token1 = strip_value(os.getenv(f"{env_prefix}APP_TOKEN") or os.getenv("APP_TOKEN"))
        token2 = strip_value(os.getenv(f"{env_prefix}BACKUP_APP_TOKEN") or os.getenv("BACKUP_APP_TOKEN"))
        allow_qs = coerce_bool(os.getenv(f"{env_prefix}ALLOW_QUERY_TOKEN") or os.getenv("ALLOW_QUERY_TOKEN"), False)
        allow_cookie = coerce_bool(os.getenv(f"{env_prefix}ALLOW_COOKIE_TOKEN") or os.getenv("ALLOW_COOKIE_TOKEN"), False)
        
        tokens_exist = bool(token1 or token2)
        
        # Open mode policy
        open_override = os.getenv(f"{env_prefix}OPEN_MODE") or os.getenv("OPEN_MODE")
        if open_override is not None:
            is_open = coerce_bool(open_override, not tokens_exist)
        else:
            is_open = not tokens_exist
        
        if is_open and tokens_exist:
            warnings_list.append("OPEN_MODE enabled while tokens exist - endpoints exposed publicly")
        
        # Provider API keys - support multiple env var names
        eodhd_key = strip_value(
            os.getenv(f"{env_prefix}EODHD_API_TOKEN") or 
            os.getenv("EODHD_API_TOKEN") or 
            os.getenv(f"{env_prefix}EODHD_API_KEY") or 
            os.getenv("EODHD_API_KEY")
        )
        
        finnhub_key = strip_value(
            os.getenv(f"{env_prefix}FINNHUB_API_KEY") or 
            os.getenv("FINNHUB_API_KEY")
        )
        
        alphavantage_key = strip_value(
            os.getenv(f"{env_prefix}ALPHAVANTAGE_API_KEY") or 
            os.getenv("ALPHAVANTAGE_API_KEY")
        )
        
        polygon_key = strip_value(
            os.getenv(f"{env_prefix}POLYGON_API_KEY") or 
            os.getenv("POLYGON_API_KEY")
        )
        
        iexcloud_key = strip_value(
            os.getenv(f"{env_prefix}IEXCLOUD_API_KEY") or 
            os.getenv("IEXCLOUD_API_KEY")
        )
        
        tradier_key = strip_value(
            os.getenv(f"{env_prefix}TRADIER_API_KEY") or 
            os.getenv("TRADIER_API_KEY")
        )
        
        marketaux_key = strip_value(
            os.getenv(f"{env_prefix}MARKETAUX_API_KEY") or 
            os.getenv("MARKETAUX_API_KEY")
        )
        
        newsapi_key = strip_value(
            os.getenv(f"{env_prefix}NEWSAPI_API_KEY") or 
            os.getenv("NEWSAPI_API_KEY")
        )
        
        twelve_data_key = strip_value(
            os.getenv(f"{env_prefix}TWELVE_DATA_API_KEY") or 
            os.getenv("TWELVE_DATA_API_KEY")
        )
        
        argaam_key = strip_value(
            os.getenv(f"{env_prefix}ARGAAM_API_KEY") or 
            os.getenv("ARGAAM_API_KEY")
        )
        
        # Provider weights
        provider_weights = {}
        weights_str = os.getenv(f"{env_prefix}PROVIDER_WEIGHTS") or os.getenv("PROVIDER_WEIGHTS")
        if weights_str:
            try:
                provider_weights = json.loads(weights_str)
            except:
                pass
        
        # Provider timeouts
        provider_timeouts = {}
        timeouts_str = os.getenv(f"{env_prefix}PROVIDER_TIMEOUTS") or os.getenv("PROVIDER_TIMEOUTS")
        if timeouts_str:
            try:
                provider_timeouts = json.loads(timeouts_str)
            except:
                pass
        
        # Provider retries
        provider_retries = {}
        retries_str = os.getenv(f"{env_prefix}PROVIDER_RETRIES") or os.getenv("PROVIDER_RETRIES")
        if retries_str:
            try:
                provider_retries = json.loads(retries_str)
            except:
                pass
        
        # Providers list
        providers_env = coerce_list(
            os.getenv(f"{env_prefix}ENABLED_PROVIDERS") or 
            os.getenv("ENABLED_PROVIDERS") or 
            os.getenv(f"{env_prefix}PROVIDERS") or 
            os.getenv("PROVIDERS")
        )
        
        if not providers_env:
            providers_env = []
            if eodhd_key:
                providers_env.append("eodhd")
            if finnhub_key:
                providers_env.append("finnhub")
            if alphavantage_key:
                providers_env.append("alphavantage")
            if polygon_key:
                providers_env.append("polygon")
            if iexcloud_key:
                providers_env.append("iexcloud")
            if tradier_key:
                providers_env.append("tradier")
            if not providers_env:
                providers_env = ["eodhd", "finnhub"]
                warnings_list.append("No providers configured with API keys, using defaults")
        
        ksa_env = coerce_list(
            os.getenv(f"{env_prefix}KSA_PROVIDERS") or 
            os.getenv("KSA_PROVIDERS") or 
            "yahoo_chart,argaam"
        )
        
        # Primary provider
        primary = strip_value(
            os.getenv(f"{env_prefix}PRIMARY_PROVIDER") or 
            os.getenv("PRIMARY_PROVIDER")
        ).lower()
        if not primary and providers_env:
            primary = providers_env[0]
        if not primary:
            primary = "eodhd"
        
        if primary not in providers_env:
            warnings_list.append(f"PRIMARY_PROVIDER={primary!r} not in enabled_providers={providers_env!r}")
        
        # URLs
        backend = strip_value(
            os.getenv(f"{env_prefix}BACKEND_BASE_URL") or
            os.getenv("BACKEND_BASE_URL") or
            os.getenv(f"{env_prefix}TFB_BASE_URL") or
            os.getenv("TFB_BASE_URL") or
            "http://localhost:8000"
        ).rstrip('/')
        
        internal = strip_value(
            os.getenv(f"{env_prefix}INTERNAL_BASE_URL") or 
            os.getenv("INTERNAL_BASE_URL") or 
            backend
        ).rstrip('/')
        
        render_name = strip_value(
            os.getenv(f"{env_prefix}RENDER_SERVICE_NAME") or 
            os.getenv("RENDER_SERVICE_NAME")
        ) or None
        
        render_external = strip_value(
            os.getenv(f"{env_prefix}RENDER_EXTERNAL_URL") or 
            os.getenv("RENDER_EXTERNAL_URL")
        ) or None
        
        public_base = None
        if render_external and is_valid_url(render_external):
            public_base = render_external
        elif is_valid_url(backend):
            public_base = backend
        
        # Kubernetes
        k8s_namespace = strip_value(os.getenv("KUBERNETES_NAMESPACE")) or None
        k8s_pod_name = strip_value(os.getenv("HOSTNAME")) or None
        
        # CORS origins
        cors_origins = coerce_list(
            os.getenv(f"{env_prefix}CORS_ORIGINS") or 
            os.getenv("CORS_ORIGINS") or 
            ""
        )
        cors_allow_creds = coerce_bool(
            os.getenv(f"{env_prefix}CORS_ALLOW_CREDENTIALS") or 
            os.getenv("CORS_ALLOW_CREDENTIALS"), 
            True
        )
        cors_max_age = coerce_int(
            os.getenv(f"{env_prefix}CORS_MAX_AGE") or 
            os.getenv("CORS_MAX_AGE"), 
            3600, lo=0, hi=86400
        )
        
        # Rate limiting
        rate_limit_rpm = coerce_int(
            os.getenv(f"{env_prefix}RATE_LIMIT_RPM") or 
            os.getenv("RATE_LIMIT_RPM"), 
            240, lo=30, hi=10000
        )
        rate_limit_global = coerce_bool(
            os.getenv(f"{env_prefix}RATE_LIMIT_GLOBAL") or 
            os.getenv("RATE_LIMIT_GLOBAL"), 
            True
        )
        rate_limit_redis = strip_value(
            os.getenv(f"{env_prefix}RATE_LIMIT_REDIS_URL") or 
            os.getenv("RATE_LIMIT_REDIS_URL")
        ) or None
        
        # Database
        database_url = strip_value(
            os.getenv(f"{env_prefix}DATABASE_URL") or 
            os.getenv("DATABASE_URL") or 
            os.getenv(f"{env_prefix}POSTGRES_URL") or 
            os.getenv("POSTGRES_URL")
        ) or None
        
        # Database pool
        db_pool_size = coerce_int(
            os.getenv(f"{env_prefix}DATABASE_POOL_SIZE") or 
            os.getenv("DATABASE_POOL_SIZE"), 
            20, lo=1, hi=200
        )
        db_max_overflow = coerce_int(
            os.getenv(f"{env_prefix}DATABASE_MAX_OVERFLOW") or 
            os.getenv("DATABASE_MAX_OVERFLOW"), 
            10, lo=0, hi=100
        )
        db_pool_timeout = coerce_int(
            os.getenv(f"{env_prefix}DATABASE_POOL_TIMEOUT") or 
            os.getenv("DATABASE_POOL_TIMEOUT"), 
            30, lo=1, hi=300
        )
        db_pool_recycle = coerce_int(
            os.getenv(f"{env_prefix}DATABASE_POOL_RECYCLE") or 
            os.getenv("DATABASE_POOL_RECYCLE"), 
            3600, lo=60, hi=86400
        )
        db_echo = coerce_bool(
            os.getenv(f"{env_prefix}DATABASE_ECHO") or 
            os.getenv("DATABASE_ECHO"), 
            False
        )
        db_ssl_mode = strip_value(
            os.getenv(f"{env_prefix}DATABASE_SSL_MODE") or 
            os.getenv("DATABASE_SSL_MODE")
        ) or None
        db_stmt_timeout = coerce_int(
            os.getenv(f"{env_prefix}DATABASE_STATEMENT_TIMEOUT") or 
            os.getenv("DATABASE_STATEMENT_TIMEOUT"), 
            30000, lo=1000, hi=300000
        )
        db_lock_timeout = coerce_int(
            os.getenv(f"{env_prefix}DATABASE_LOCK_TIMEOUT") or 
            os.getenv("DATABASE_LOCK_TIMEOUT"), 
            5000, lo=100, hi=60000
        )
        
        # Redis
        redis_url = strip_value(
            os.getenv(f"{env_prefix}REDIS_URL") or 
            os.getenv("REDIS_URL")
        ) or None
        
        redis_socket_timeout = coerce_float(
            os.getenv(f"{env_prefix}REDIS_SOCKET_TIMEOUT") or 
            os.getenv("REDIS_SOCKET_TIMEOUT"), 
            5.0, lo=0.5, hi=30.0
        )
        redis_connect_timeout = coerce_float(
            os.getenv(f"{env_prefix}REDIS_SOCKET_CONNECT_TIMEOUT") or 
            os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT"), 
            5.0, lo=0.5, hi=30.0
        )
        redis_retry_on_timeout = coerce_bool(
            os.getenv(f"{env_prefix}REDIS_RETRY_ON_TIMEOUT") or 
            os.getenv("REDIS_RETRY_ON_TIMEOUT"), 
            True
        )
        redis_max_connections = coerce_int(
            os.getenv(f"{env_prefix}REDIS_MAX_CONNECTIONS") or 
            os.getenv("REDIS_MAX_CONNECTIONS"), 
            50, lo=1, hi=500
        )
        redis_ssl = coerce_bool(
            os.getenv(f"{env_prefix}REDIS_SSL") or 
            os.getenv("REDIS_SSL"), 
            False
        )
        redis_health_interval = coerce_int(
            os.getenv(f"{env_prefix}REDIS_HEALTH_CHECK_INTERVAL") or 
            os.getenv("REDIS_HEALTH_CHECK_INTERVAL"), 
            30, lo=5, hi=300
        )
        
        # Memcached
        memcached_servers = coerce_list(
            os.getenv(f"{env_prefix}MEMCACHED_SERVERS") or 
            os.getenv("MEMCACHED_SERVERS")
        )
        
        # Cache
        cache_backend_name = strip_value(
            os.getenv(f"{env_prefix}CACHE_BACKEND") or 
            os.getenv("CACHE_BACKEND") or 
            "memory"
        ).lower()
        cache_backend = CacheBackend.MEMORY
        for cb in CacheBackend:
            if cb.value == cache_backend_name:
                cache_backend = cb
                break
        
        cache_default_ttl = coerce_int(
            os.getenv(f"{env_prefix}CACHE_DEFAULT_TTL") or 
            os.getenv("CACHE_DEFAULT_TTL"), 
            300, lo=1, hi=86400
        )
        cache_prefix = strip_value(
            os.getenv(f"{env_prefix}CACHE_PREFIX") or 
            os.getenv("CACHE_PREFIX") or 
            "tfb:"
        )
        cache_compression = coerce_bool(
            os.getenv(f"{env_prefix}CACHE_COMPRESSION") or 
            os.getenv("CACHE_COMPRESSION"), 
            False
        )
        cache_compression_level = coerce_int(
            os.getenv(f"{env_prefix}CACHE_COMPRESSION_LEVEL") or 
            os.getenv("CACHE_COMPRESSION_LEVEL"), 
            6, lo=1, hi=9
        )
        cache_max_size = coerce_int(
            os.getenv(f"{env_prefix}CACHE_MAX_SIZE") or 
            os.getenv("CACHE_MAX_SIZE"), 
            0, lo=0, hi=10000000
        )
        cache_max_size = cache_max_size if cache_max_size > 0 else None
        cache_max_memory = strip_value(
            os.getenv(f"{env_prefix}CACHE_MAX_MEMORY") or 
            os.getenv("CACHE_MAX_MEMORY")
        ) or None
        
        # Google Sheets
        spreadsheet_id = strip_value(
            os.getenv(f"{env_prefix}DEFAULT_SPREADSHEET_ID") or
            os.getenv("DEFAULT_SPREADSHEET_ID") or
            os.getenv(f"{env_prefix}SPREADSHEET_ID") or
            os.getenv("SPREADSHEET_ID") or
            os.getenv(f"{env_prefix}TFB_SPREADSHEET_ID") or
            os.getenv("TFB_SPREADSHEET_ID")
        ) or None
        
        google_creds = cls._normalize_google_creds(
            os.getenv(f"{env_prefix}GOOGLE_SHEETS_CREDENTIALS") or 
            os.getenv("GOOGLE_SHEETS_CREDENTIALS") or 
            os.getenv(f"{env_prefix}GOOGLE_CREDENTIALS") or 
            os.getenv("GOOGLE_CREDENTIALS") or 
            ""
        )
        
        google_service_account = strip_value(
            os.getenv(f"{env_prefix}GOOGLE_SERVICE_ACCOUNT_EMAIL") or 
            os.getenv("GOOGLE_SERVICE_ACCOUNT_EMAIL")
        ) or None
        
        # Argaam
        argaam_url = strip_value(
            os.getenv(f"{env_prefix}ARGAAM_QUOTE_URL") or 
            os.getenv("ARGAAM_QUOTE_URL")
        ) or None
        if argaam_url:
            argaam_url = argaam_url.rstrip('/')
        
        argaam_timeout = coerce_int(
            os.getenv(f"{env_prefix}ARGAAM_TIMEOUT") or 
            os.getenv("ARGAAM_TIMEOUT"), 
            30, lo=5, hi=120
        )
        argaam_retries = coerce_int(
            os.getenv(f"{env_prefix}ARGAAM_RETRIES") or 
            os.getenv("ARGAAM_RETRIES"), 
            3, lo=0, hi=10
        )
        
        # Monitoring
        metrics_enabled_val = coerce_bool(
            os.getenv(f"{env_prefix}METRICS_ENABLED") or 
            os.getenv("METRICS_ENABLED"), 
            True
        )
        metrics_port = coerce_int(
            os.getenv(f"{env_prefix}METRICS_PORT") or 
            os.getenv("METRICS_PORT"), 
            9090, lo=1024, hi=65535
        )
        metrics_path = strip_value(
            os.getenv(f"{env_prefix}METRICS_PATH") or 
            os.getenv("METRICS_PATH") or 
            "/metrics"
        )
        health_check_path = strip_value(
            os.getenv(f"{env_prefix}HEALTH_CHECK_PATH") or 
            os.getenv("HEALTH_CHECK_PATH") or 
            "/health"
        )
        readiness_path = strip_value(
            os.getenv(f"{env_prefix}READINESS_PATH") or 
            os.getenv("READINESS_PATH") or 
            "/ready"
        )
        liveness_path = strip_value(
            os.getenv(f"{env_prefix}LIVENESS_PATH") or 
            os.getenv("LIVENESS_PATH") or 
            "/live"
        )
        sentry_dsn = strip_value(
            os.getenv(f"{env_prefix}SENTRY_DSN") or 
            os.getenv("SENTRY_DSN")
        ) or None
        sentry_environment = strip_value(
            os.getenv(f"{env_prefix}SENTRY_ENVIRONMENT") or 
            os.getenv("SENTRY_ENVIRONMENT") or 
            environment.value
        )
        sentry_sample_rate = coerce_float(
            os.getenv(f"{env_prefix}SENTRY_SAMPLE_RATE") or 
            os.getenv("SENTRY_SAMPLE_RATE"), 
            1.0, lo=0.0, hi=1.0
        )
        datadog_key = strip_value(
            os.getenv(f"{env_prefix}DATADOG_API_KEY") or 
            os.getenv("DATADOG_API_KEY")
        ) or None
        datadog_app_key = strip_value(
            os.getenv(f"{env_prefix}DATADOG_APP_KEY") or 
            os.getenv("DATADOG_APP_KEY")
        ) or None
        datadog_site = strip_value(
            os.getenv(f"{env_prefix}DATADOG_SITE") or 
            os.getenv("DATADOG_SITE") or 
            "datadoghq.com"
        )
        newrelic_key = strip_value(
            os.getenv(f"{env_prefix}NEWRELIC_LICENSE_KEY") or 
            os.getenv("NEWRELIC_LICENSE_KEY")
        ) or None
        newrelic_app = strip_value(
            os.getenv(f"{env_prefix}NEWRELIC_APP_NAME") or 
            os.getenv("NEWRELIC_APP_NAME") or 
            "Tadawul Fast Bridge"
        )
        
        # Chaos engineering
        chaos_enabled = coerce_bool(
            os.getenv(f"{env_prefix}CHAOS_ENABLED") or 
            os.getenv("CHAOS_ENABLED"), 
            False
        )
        if chaos_enabled:
            warnings_list.append(f"CHAOS ENGINEERING ENABLED")
        
        chaos_failure_rate = coerce_float(
            os.getenv(f"{env_prefix}CHAOS_FAILURE_RATE") or 
            os.getenv("CHAOS_FAILURE_RATE"), 
            0.0, lo=0.0, hi=1.0
        )
        chaos_latency_ms = coerce_int(
            os.getenv(f"{env_prefix}CHAOS_LATENCY_MS") or 
            os.getenv("CHAOS_LATENCY_MS"), 
            0, lo=0, hi=30000
        )
        chaos_exception_rate = coerce_float(
            os.getenv(f"{env_prefix}CHAOS_EXCEPTION_RATE") or 
            os.getenv("CHAOS_EXCEPTION_RATE"), 
            0.0, lo=0.0, hi=1.0
        )
        chaos_error_codes = coerce_list(
            os.getenv(f"{env_prefix}CHAOS_ERROR_CODES") or 
            os.getenv("CHAOS_ERROR_CODES")
        )
        chaos_error_codes = [int(c) for c in chaos_error_codes if c.isdigit()]
        
        chaos_endpoints = coerce_list(
            os.getenv(f"{env_prefix}CHAOS_ENDPOINTS") or 
            os.getenv("CHAOS_ENDPOINTS")
        )
        
        chaos_providers = coerce_list(
            os.getenv(f"{env_prefix}CHAOS_PROVIDERS") or 
            os.getenv("CHAOS_PROVIDERS")
        )
        
        # Timeouts and sizes
        http_timeout = coerce_float(
            os.getenv(f"{env_prefix}HTTP_TIMEOUT_SEC") or 
            os.getenv("HTTP_TIMEOUT_SEC"), 
            45.0, lo=5.0, hi=180.0
        )
        http_connect_timeout = coerce_float(
            os.getenv(f"{env_prefix}HTTP_CONNECT_TIMEOUT") or 
            os.getenv("HTTP_CONNECT_TIMEOUT"), 
            10.0, lo=1.0, hi=60.0
        )
        http_read_timeout = coerce_float(
            os.getenv(f"{env_prefix}HTTP_READ_TIMEOUT") or 
            os.getenv("HTTP_READ_TIMEOUT"), 
            30.0, lo=1.0, hi=300.0
        )
        
        cache_ttl = coerce_int(
            os.getenv(f"{env_prefix}CACHE_TTL_SEC") or 
            os.getenv("CACHE_TTL_SEC"), 
            20, lo=5, hi=3600
        )
        batch_concurrency = coerce_int(
            os.getenv(f"{env_prefix}BATCH_CONCURRENCY") or 
            os.getenv("BATCH_CONCURRENCY"), 
            5, lo=1, hi=50
        )
        ai_batch_size = coerce_int(
            os.getenv(f"{env_prefix}AI_BATCH_SIZE") or 
            os.getenv("AI_BATCH_SIZE"), 
            20, lo=1, hi=200
        )
        adv_batch_size = coerce_int(
            os.getenv(f"{env_prefix}ADV_BATCH_SIZE") or 
            os.getenv("ADV_BATCH_SIZE"), 
            25, lo=1, hi=300
        )
        quote_batch_size = coerce_int(
            os.getenv(f"{env_prefix}QUOTE_BATCH_SIZE") or 
            os.getenv("QUOTE_BATCH_SIZE"), 
            50, lo=1, hi=1000
        )
        if quote_batch_size > 500:
            warnings_list.append(f"Large quote batch size ({quote_batch_size}) may cause timeouts")
        
        historical_batch_size = coerce_int(
            os.getenv(f"{env_prefix}HISTORICAL_BATCH_SIZE") or 
            os.getenv("HISTORICAL_BATCH_SIZE"), 
            10, lo=1, hi=100
        )
        news_batch_size = coerce_int(
            os.getenv(f"{env_prefix}NEWS_BATCH_SIZE") or 
            os.getenv("NEWS_BATCH_SIZE"), 
            20, lo=1, hi=200
        )
        
        # Connection pools
        conn_pool_size = coerce_int(
            os.getenv(f"{env_prefix}CONNECTION_POOL_SIZE") or 
            os.getenv("CONNECTION_POOL_SIZE"), 
            10, lo=1, hi=100
        )
        conn_pool_max_overflow = coerce_int(
            os.getenv(f"{env_prefix}CONNECTION_POOL_MAX_OVERFLOW") or 
            os.getenv("CONNECTION_POOL_MAX_OVERFLOW"), 
            20, lo=0, hi=200
        )
        conn_pool_recycle = coerce_int(
            os.getenv(f"{env_prefix}CONNECTION_POOL_RECYCLE") or 
            os.getenv("CONNECTION_POOL_RECYCLE"), 
            3600, lo=60, hi=86400
        )
        
        async_thread_pool = coerce_int(
            os.getenv(f"{env_prefix}ASYNC_THREAD_POOL_SIZE") or 
            os.getenv("ASYNC_THREAD_POOL_SIZE"), 
            20, lo=1, hi=200
        )
        async_queue = coerce_int(
            os.getenv(f"{env_prefix}ASYNC_QUEUE_SIZE") or 
            os.getenv("ASYNC_QUEUE_SIZE"), 
            100, lo=10, hi=1000
        )
        async_task_timeout = coerce_int(
            os.getenv(f"{env_prefix}ASYNC_TASK_TIMEOUT") or 
            os.getenv("ASYNC_TASK_TIMEOUT"), 
            60, lo=5, hi=600
        )
        
        # Logging
        log_level_name = strip_value(
            os.getenv(f"{env_prefix}LOG_LEVEL") or 
            os.getenv("LOG_LEVEL") or 
            "INFO"
        ).upper()
        log_level = LogLevel.INFO
        for ll in LogLevel:
            if ll.value == log_level_name:
                log_level = ll
                break
        
        log_format = strip_value(
            os.getenv(f"{env_prefix}LOG_FORMAT") or 
            os.getenv("LOG_FORMAT") or 
            "json"
        ).lower()
        if log_format not in {'json', 'text', 'color', 'console'}:
            log_format = 'json'
        
        log_file_str = strip_value(
            os.getenv(f"{env_prefix}LOG_FILE") or 
            os.getenv("LOG_FILE")
        )
        log_file = Path(log_file_str) if log_file_str else None
        
        log_correlation_id = coerce_bool(
            os.getenv(f"{env_prefix}LOG_CORRELATION_ID") or 
            os.getenv("LOG_CORRELATION_ID"), 
            True
        )
        
        # Feature flags
        ai_enabled = coerce_bool(
            os.getenv(f"{env_prefix}AI_ANALYSIS_ENABLED") or 
            os.getenv("AI_ANALYSIS_ENABLED"), 
            True
        )
        advisor_enabled = coerce_bool(
            os.getenv(f"{env_prefix}ADVISOR_ENABLED") or 
            os.getenv("ADVISOR_ENABLED"), 
            True
        )
        advanced_enabled = coerce_bool(
            os.getenv(f"{env_prefix}ADVANCED_ENABLED") or 
            os.getenv("ADVANCED_ENABLED"), 
            True
        )
        rate_limit_enabled = coerce_bool(
            os.getenv(f"{env_prefix}RATE_LIMIT_ENABLED") or 
            os.getenv("RATE_LIMIT_ENABLED"), 
            True
        )
        cache_enabled = coerce_bool(
            os.getenv(f"{env_prefix}CACHE_ENABLED") or 
            os.getenv("CACHE_ENABLED"), 
            True
        )
        circuit_breaker_enabled = coerce_bool(
            os.getenv(f"{env_prefix}CIRCUIT_BREAKER_ENABLED") or 
            os.getenv("CIRCUIT_BREAKER_ENABLED"), 
            True
        )
        metrics_enabled = coerce_bool(
            os.getenv(f"{env_prefix}METRICS_ENABLED") or 
            os.getenv("METRICS_ENABLED"), 
            True
        )
        tracing_enabled = coerce_bool(
            os.getenv(f"{env_prefix}TRACING_ENABLED") or 
            os.getenv("TRACING_ENABLED"), 
            False
        )
        profiling_enabled = coerce_bool(
            os.getenv(f"{env_prefix}PROFILING_ENABLED") or 
            os.getenv("PROFILING_ENABLED"), 
            False
        )
        webhook_enabled = coerce_bool(
            os.getenv(f"{env_prefix}WEBHOOK_ENABLED") or 
            os.getenv("WEBHOOK_ENABLED"), 
            True
        )
        websocket_enabled = coerce_bool(
            os.getenv(f"{env_prefix}WEBSOCKET_ENABLED") or 
            os.getenv("WEBSOCKET_ENABLED"), 
            True
        )
        streaming_enabled = coerce_bool(
            os.getenv(f"{env_prefix}STREAMING_ENABLED") or 
            os.getenv("STREAMING_ENABLED"), 
            False
        )
        batch_processing_enabled = coerce_bool(
            os.getenv(f"{env_prefix}BATCH_PROCESSING_ENABLED") or 
            os.getenv("BATCH_PROCESSING_ENABLED"), 
            True
        )
        
        # Feature flags dictionary
        feature_flags_str = os.getenv(f"{env_prefix}FEATURE_FLAGS") or os.getenv("FEATURE_FLAGS")
        feature_flags = {}
        if feature_flags_str:
            try:
                feature_flags = json.loads(feature_flags_str)
            except:
                pass
        
        # AI/ML Testing
        ai_testing_enabled = coerce_bool(
            os.getenv(f"{env_prefix}AI_TESTING_ENABLED") or 
            os.getenv("AI_TESTING_ENABLED"), 
            False
        )
        
        ai_model_path_str = strip_value(
            os.getenv(f"{env_prefix}AI_MODEL_PATH") or 
            os.getenv("AI_MODEL_PATH")
        )
        ai_model_path = Path(ai_model_path_str) if ai_model_path_str else None
        
        ai_model_version = strip_value(
            os.getenv(f"{env_prefix}AI_MODEL_VERSION") or 
            os.getenv("AI_MODEL_VERSION") or 
            "latest"
        )
        ai_confidence = coerce_float(
            os.getenv(f"{env_prefix}AI_CONFIDENCE_THRESHOLD") or 
            os.getenv("AI_CONFIDENCE_THRESHOLD"), 
            0.7, lo=0.0, hi=1.0
        )
        ai_batch = coerce_int(
            os.getenv(f"{env_prefix}AI_BATCH_SIZE") or 
            os.getenv("AI_BATCH_SIZE"), 
            32, lo=1, hi=256
        )
        ai_max_tokens = coerce_int(
            os.getenv(f"{env_prefix}AI_MAX_TOKENS") or 
            os.getenv("AI_MAX_TOKENS"), 
            2048, lo=64, hi=8192
        )
        ai_temperature = coerce_float(
            os.getenv(f"{env_prefix}AI_TEMPERATURE") or 
            os.getenv("AI_TEMPERATURE"), 
            0.7, lo=0.0, hi=2.0
        )
        ai_top_p = coerce_float(
            os.getenv(f"{env_prefix}AI_TOP_P") or 
            os.getenv("AI_TOP_P"), 
            0.9, lo=0.0, hi=1.0
        )
        ai_top_k = coerce_int(
            os.getenv(f"{env_prefix}AI_TOP_K") or 
            os.getenv("AI_TOP_K"), 
            40, lo=1, hi=100
        )
        ai_seed_str = strip_value(
            os.getenv(f"{env_prefix}AI_SEED") or 
            os.getenv("AI_SEED")
        )
        ai_seed = int(ai_seed_str) if ai_seed_str and ai_seed_str.isdigit() else None
        ai_cache_responses = coerce_bool(
            os.getenv(f"{env_prefix}AI_CACHE_RESPONSES") or 
            os.getenv("AI_CACHE_RESPONSES"), 
            True
        )
        ai_cache_ttl = coerce_int(
            os.getenv(f"{env_prefix}AI_CACHE_TTL") or 
            os.getenv("AI_CACHE_TTL"), 
            3600, lo=60, hi=86400
        )
        
        # Encryption
        encryption_algorithm_name = strip_value(
            os.getenv(f"{env_prefix}ENCRYPTION_ALGORITHM") or 
            os.getenv("ENCRYPTION_ALGORITHM") or 
            "fernet"
        ).lower()
        encryption_algorithm = ConfigEncryptionAlgorithm.FERNET
        for algo in ConfigEncryptionAlgorithm:
            if algo.value == encryption_algorithm_name:
                encryption_algorithm = algo
                break
        
        encryption_key = strip_value(
            os.getenv(f"{env_prefix}ENCRYPTION_KEY") or 
            os.getenv("ENCRYPTION_KEY")
        ) or None
        
        # Version
        app_version = strip_value(
            os.getenv(f"{env_prefix}APP_VERSION") or 
            os.getenv("APP_VERSION") or 
            CONFIG_VERSION
        )
        
        service_name = strip_value(
            os.getenv(f"{env_prefix}APP_NAME") or 
            os.getenv("APP_NAME") or 
            "Tadawul Fast Bridge"
        )
        
        timezone = strip_value(
            os.getenv(f"{env_prefix}APP_TIMEZONE") or 
            os.getenv("APP_TIMEZONE") or 
            "Asia/Riyadh"
        )
        
        # Auth header
        auth_header = strip_value(
            os.getenv(f"{env_prefix}AUTH_HEADER_NAME") or 
            os.getenv("AUTH_HEADER_NAME") or 
            "X-APP-TOKEN"
        )
        
        # Create settings - FIXED: Remove duplicate argaam_api_key
        s = cls(
            environment=environment,
            app_version=app_version,
            service_name=service_name,
            service_id=service_id,
            timezone=timezone,
            log_level=log_level,
            log_format=log_format,
            log_file=log_file,
            log_correlation_id=log_correlation_id,
            backend_base_url=backend,
            internal_base_url=internal,
            public_base_url=public_base,
            render_service_name=render_name,
            render_external_url=render_external,
            auth_header_name=auth_header,
            app_token=token1 or None,
            backup_app_token=token2 or None,
            allow_query_token=allow_qs,
            allow_cookie_token=allow_cookie,
            open_mode=is_open,
            cors_origins=cors_origins,
            cors_allow_credentials=cors_allow_creds,
            cors_max_age=cors_max_age,
            rate_limit_rpm=rate_limit_rpm,
            rate_limit_global=rate_limit_global,
            rate_limit_redis_url=rate_limit_redis,
            ai_analysis_enabled=ai_enabled,
            advisor_enabled=advisor_enabled,
            advanced_enabled=advanced_enabled,
            rate_limit_enabled=rate_limit_enabled,
            cache_enabled=cache_enabled,
            circuit_breaker_enabled=circuit_breaker_enabled,
            metrics_enabled=metrics_enabled,  # This is from feature flags above
            tracing_enabled=tracing_enabled,
            profiling_enabled=profiling_enabled,
            chaos_enabled=chaos_enabled,
            webhook_enabled=webhook_enabled,
            websocket_enabled=websocket_enabled,
            streaming_enabled=streaming_enabled,
            batch_processing_enabled=batch_processing_enabled,
            feature_flags=feature_flags,
            chaos_failure_rate=chaos_failure_rate,
            chaos_latency_ms=chaos_latency_ms,
            chaos_exception_rate=chaos_exception_rate,
            chaos_error_codes=chaos_error_codes,
            chaos_endpoints=chaos_endpoints,
            chaos_providers=chaos_providers,
            enabled_providers=providers_env,
            ksa_providers=ksa_env,
            primary_provider=primary,
            provider_weights=provider_weights,
            provider_timeouts=provider_timeouts,
            provider_retries=provider_retries,
            eodhd_api_key=eodhd_key or None,
            finnhub_api_key=finnhub_key or None,
            alphavantage_api_key=alphavantage_key or None,
            polygon_api_key=polygon_key or None,
            iexcloud_api_key=iexcloud_key or None,
            tradier_api_key=tradier_key or None,
            marketaux_api_key=marketaux_key or None,
            newsapi_api_key=newsapi_key or None,
            twelve_data_api_key=twelve_data_key or None,
            argaam_api_key=argaam_key or None,  # Keep this one (from provider credentials)
            http_timeout_sec=http_timeout,
            http_connect_timeout=http_connect_timeout,
            http_read_timeout=http_read_timeout,
            cache_ttl_sec=cache_ttl,
            batch_concurrency=batch_concurrency,
            ai_batch_size=ai_batch_size,
            adv_batch_size=adv_batch_size,
            quote_batch_size=quote_batch_size,
            historical_batch_size=historical_batch_size,
            news_batch_size=news_batch_size,
            connection_pool_size=conn_pool_size,
            connection_pool_max_overflow=conn_pool_max_overflow,
            connection_pool_recycle=conn_pool_recycle,
            async_thread_pool_size=async_thread_pool,
            async_queue_size=async_queue,
            async_task_timeout=async_task_timeout,
            database_url=database_url,
            database_pool_size=db_pool_size,
            database_max_overflow=db_max_overflow,
            database_pool_timeout=db_pool_timeout,
            database_pool_recycle=db_pool_recycle,
            database_echo=db_echo,
            database_ssl_mode=db_ssl_mode,
            database_statement_timeout=db_stmt_timeout,
            database_lock_timeout=db_lock_timeout,
            database_application_name=service_name,
            cache_backend=cache_backend,
            cache_default_ttl=cache_default_ttl,
            cache_prefix=cache_prefix,
            cache_compression=cache_compression,
            cache_compression_level=cache_compression_level,
            cache_max_size=cache_max_size,
            cache_max_memory=cache_max_memory,
            redis_url=redis_url,
            redis_socket_timeout=redis_socket_timeout,
            redis_socket_connect_timeout=redis_connect_timeout,
            redis_retry_on_timeout=redis_retry_on_timeout,
            redis_max_connections=redis_max_connections,
            redis_ssl=redis_ssl,
            redis_health_check_interval=redis_health_interval,
            memcached_servers=memcached_servers,
            spreadsheet_id=spreadsheet_id,
            google_creds=google_creds,
            google_service_account=google_service_account,
            google_token_uri="https://oauth2.googleapis.com/token",
            google_auth_uri="https://accounts.google.com/o/oauth2/auth",
            google_scopes=["https://www.googleapis.com/auth/spreadsheets"],
            argaam_quote_url=argaam_url,
            # REMOVED: argaam_api_key=argaam_key,  <-- This was the duplicate
            argaam_timeout=argaam_timeout,
            argaam_retries=argaam_retries,
            metrics_port=metrics_port,
            metrics_path=metrics_path,
            health_check_path=health_check_path,
            health_check_interval=30,
            health_check_timeout=5,
            health_check_fail_fast=True,
            readiness_path=readiness_path,
            liveness_path=liveness_path,
            sentry_dsn=sentry_dsn,
            sentry_environment=sentry_environment,
            sentry_sample_rate=sentry_sample_rate,
            datadog_api_key=datadog_key,
            datadog_app_key=datadog_app_key,
            datadog_site=datadog_site,
            newrelic_license_key=newrelic_key,
            newrelic_app_name=newrelic_app,
            prometheus_enabled=True,
            opentelemetry_enabled=False,
            opentelemetry_endpoint=None,
            ai_testing_enabled=ai_testing_enabled,
            ai_model_path=ai_model_path,
            ai_model_version=ai_model_version,
            ai_confidence_threshold=ai_confidence,
            ai_batch_size=ai_batch,
            ai_max_tokens=ai_max_tokens,
            ai_temperature=ai_temperature,
            ai_top_p=ai_top_p,
            ai_top_k=ai_top_k,
            ai_seed=ai_seed,
            ai_cache_responses=ai_cache_responses,
            ai_cache_ttl=ai_cache_ttl,
            encryption_algorithm=encryption_algorithm,
            encryption_key=encryption_key,
            config_sources=config_sources,
        )
        
        # Run validation
        errors, warnings = s.validate()
        all_warnings = warnings_list + list(warnings)
        
        # Record metrics
        load_time = time.time() - start_time
        config_loads_total.labels(source='env', status='success' if not errors else 'error').inc()
        
        if errors:
            structured_log('ERROR', 'Configuration errors found', errors=errors, count=len(errors))
        
        if all_warnings:
            for warning in all_warnings[:5]:  # Log first 5 warnings
                structured_log('WARNING', 'Configuration warning', warning=warning)
            config_warnings_total.labels(type='validation').inc(len(all_warnings))
        
        # Return with diagnostics
        return replace(
            s,
            boot_errors=tuple(errors),
            boot_warnings=tuple(all_warnings)
        )
    
    @classmethod
    def from_file(cls, path: Union[str, Path], env_prefix: str = "TFB_") -> "Settings":
        """Load settings from file (YAML/JSON) and override with env vars"""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        
        start_time = time.time()
        
        config_sources = {}
        source_type = ConfigSource.YAML_FILE if path.suffix in {'.yaml', '.yml'} else ConfigSource.JSON_FILE
        config_sources['file'] = source_type
        
        try:
            data = load_file_content(path)
        except Exception as e:
            raise ValueError(f"Failed to parse config file: {e}")
        
        if not isinstance(data, dict):
            raise ValueError("Config file must contain a dictionary")
        
        # Get environment settings (higher priority)
        env_settings = cls.from_env(env_prefix)
        
        # Merge with env settings taking precedence
        merged_data = deep_merge(data, asdict(env_settings), overwrite=True)
        merged_data['config_sources'] = {
            **config_sources,
            **env_settings.config_sources
        }
        
        # Create settings
        s = cls(**merged_data)
        
        # Run validation
        errors, warnings = s.validate()
        
        # Record metrics
        load_time = time.time() - start_time
        config_loads_total.labels(source='file', status='success' if not errors else 'error').inc()
        
        return replace(
            s,
            boot_errors=tuple(errors),
            boot_warnings=tuple(warnings)
        )
    
    @classmethod
    def from_aws_secrets(cls, secret_name: str, region: Optional[str] = None, 
                         env_prefix: str = "TFB_") -> "Settings":
        """Load settings from AWS Secrets Manager"""
        if not AWS_AVAILABLE:
            raise ImportError("boto3 required for AWS Secrets Manager")
        
        start_time = time.time()
        
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region or os.getenv('AWS_REGION', 'us-east-1'),
            config=BotoConfig(retries={'max_attempts': 3, 'mode': 'adaptive'})
        )
        
        try:
            response = client.get_secret_value(SecretId=secret_name)
            if 'SecretString' in response:
                data = json.loads(response['SecretString'])
            else:
                data = json.loads(base64.b64decode(response['SecretBinary']))
        except ClientError as e:
            config_errors_total.labels(type='aws_secrets').inc()
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                raise SourceError(f"Secret not found: {secret_name}") from e
            elif e.response['Error']['Code'] == 'AccessDeniedException':
                raise SourceError(f"Access denied to secret: {secret_name}") from e
            else:
                raise SourceError(f"AWS Secrets Manager error: {e}") from e
        except BotoCoreError as e:
            config_errors_total.labels(type='aws_secrets').inc()
            raise SourceError(f"AWS SDK error: {e}") from e
        
        config_sources = {'aws_secrets': ConfigSource.AWS_SECRETS}
        data['config_sources'] = config_sources
        
        # Get environment settings for override
        env_settings = cls.from_env(env_prefix)
        
        # Merge
        merged_data = deep_merge(data, asdict(env_settings), overwrite=True)
        
        # Create settings
        s = cls(**merged_data)
        
        # Run validation
        errors, warnings = s.validate()
        
        # Record metrics
        load_time = time.time() - start_time
        config_loads_total.labels(source='aws_secrets', status='success' if not errors else 'error').inc()
        
        return replace(
            s,
            boot_errors=tuple(errors),
            boot_warnings=tuple(warnings)
        )
    
    @classmethod
    def from_vault(cls, path: str, mount_point: str = "secret", 
                   env_prefix: str = "TFB_") -> "Settings":
        """Load settings from HashiCorp Vault"""
        if not VAULT_AVAILABLE:
            raise ImportError("hvac required for Vault integration")
        
        start_time = time.time()
        
        client = hvac.Client(
            url=os.getenv('VAULT_ADDR', 'http://localhost:8200'),
            token=os.getenv('VAULT_TOKEN'),
            namespace=os.getenv('VAULT_NAMESPACE')
        )
        
        if not client.is_authenticated():
            config_errors_total.labels(type='vault_auth').inc()
            raise SourceError("Vault authentication failed")
        
        try:
            response = client.secrets.kv.v2.read_secret_version(
                mount_point=mount_point,
                path=path
            )
            data = response['data']['data']
        except InvalidPath:
            config_errors_total.labels(type='vault_path').inc()
            raise SourceError(f"Secret path not found: {path}")
        except VaultError as e:
            config_errors_total.labels(type='vault').inc()
            raise SourceError(f"Vault error: {e}") from e
        
        config_sources = {'vault': ConfigSource.VAULT}
        data['config_sources'] = config_sources
        
        # Get environment settings for override
        env_settings = cls.from_env(env_prefix)
        
        # Merge
        merged_data = deep_merge(data, asdict(env_settings), overwrite=True)
        
        # Create settings
        s = cls(**merged_data)
        
        # Run validation
        errors, warnings = s.validate()
        
        # Record metrics
        load_time = time.time() - start_time
        config_loads_total.labels(source='vault', status='success' if not errors else 'error').inc()
        
        return replace(
            s,
            boot_errors=tuple(errors),
            boot_warnings=tuple(warnings)
        )
    
    @classmethod
    def from_consul(cls, key: str, consul_host: str = "localhost", consul_port: int = 8500,
                    env_prefix: str = "TFB_") -> "Settings":
        """Load settings from Consul KV store"""
        if not CONSUL_AVAILABLE:
            raise ImportError("python-consul required for Consul integration")
        
        start_time = time.time()
        
        try:
            c = consul.Consul(host=consul_host, port=consul_port)
            index, data = c.kv.get(key)
            
            if not data or 'Value' not in data:
                raise KeyError(f"Key not found in Consul: {key}")
            
            value = data['Value']
            if isinstance(value, bytes):
                value = value.decode('utf-8')
            
            try:
                parsed = json.loads(value)
            except:
                parsed = {'value': value}
        except ConsulException as e:
            config_errors_total.labels(type='consul').inc()
            raise SourceError(f"Consul error: {e}") from e
        
        config_sources = {'consul': ConfigSource.CONSUL}
        parsed['config_sources'] = config_sources
        
        # Get environment settings for override
        env_settings = cls.from_env(env_prefix)
        
        # Merge
        merged_data = deep_merge(parsed, asdict(env_settings), overwrite=True)
        
        # Create settings
        s = cls(**merged_data)
        
        # Run validation
        errors, warnings = s.validate()
        
        # Record metrics
        load_time = time.time() - start_time
        config_loads_total.labels(source='consul', status='success' if not errors else 'error').inc()
        
        return replace(
            s,
            boot_errors=tuple(errors),
            boot_warnings=tuple(warnings)
        )
    
    @classmethod
    def from_etcd(cls, key: str, etcd_host: str = "localhost", etcd_port: int = 2379,
                  env_prefix: str = "TFB_") -> "Settings":
        """Load settings from etcd"""
        if not ETCD_AVAILABLE:
            raise ImportError("etcd3 required for etcd integration")
        
        start_time = time.time()
        
        try:
            client = etcd3.client(host=etcd_host, port=etcd_port)
            value, metadata = client.get(key)
            
            if not value:
                raise KeyError(f"Key not found in etcd: {key}")
            
            try:
                parsed = json.loads(value.decode('utf-8'))
            except:
                parsed = {'value': value.decode('utf-8')}
        except Etcd3Exception as e:
            config_errors_total.labels(type='etcd').inc()
            raise SourceError(f"etcd error: {e}") from e
        
        config_sources = {'etcd': ConfigSource.ETCD}
        parsed['config_sources'] = config_sources
        
        # Get environment settings for override
        env_settings = cls.from_env(env_prefix)
        
        # Merge
        merged_data = deep_merge(parsed, asdict(env_settings), overwrite=True)
        
        # Create settings
        s = cls(**merged_data)
        
        # Run validation
        errors, warnings = s.validate()
        
        # Record metrics
        load_time = time.time() - start_time
        config_loads_total.labels(source='etcd', status='success' if not errors else 'error').inc()
        
        return replace(
            s,
            boot_errors=tuple(errors),
            boot_warnings=tuple(warnings)
        )
    
    @classmethod
    def from_redis(cls, key: str, redis_url: str, env_prefix: str = "TFB_") -> "Settings":
        """Load settings from Redis"""
        if not REDIS_AVAILABLE:
            raise ImportError("redis required for Redis integration")
        
        start_time = time.time()
        
        try:
            client = redis.from_url(redis_url, decode_responses=True)
            value = client.get(key)
            
            if not value:
                raise KeyError(f"Key not found in Redis: {key}")
            
            try:
                parsed = json.loads(value)
            except:
                parsed = {'value': value}
        except RedisError as e:
            config_errors_total.labels(type='redis').inc()
            raise SourceError(f"Redis error: {e}") from e
        
        config_sources = {'redis': ConfigSource.REMOTE}
        parsed['config_sources'] = config_sources
        
        # Get environment settings for override
        env_settings = cls.from_env(env_prefix)
        
        # Merge
        merged_data = deep_merge(parsed, asdict(env_settings), overwrite=True)
        
        # Create settings
        s = cls(**merged_data)
        
        # Run validation
        errors, warnings = s.validate()
        
        # Record metrics
        load_time = time.time() - start_time
        config_loads_total.labels(source='redis', status='success' if not errors else 'error').inc()
        
        return replace(
            s,
            boot_errors=tuple(errors),
            boot_warnings=tuple(warnings)
        )
    
    @classmethod
    def from_zookeeper(cls, path: str, hosts: List[str], env_prefix: str = "TFB_") -> "Settings":
        """Load settings from ZooKeeper"""
        if not ZOOKEEPER_AVAILABLE:
            raise ImportError("kazoo required for ZooKeeper integration")
        
        start_time = time.time()
        
        try:
            zk = KazooClient(hosts=','.join(hosts))
            zk.start()
            
            if zk.exists(path):
                data, stat = zk.get(path)
                if data:
                    try:
                        parsed = json.loads(data.decode('utf-8'))
                    except:
                        parsed = {'value': data.decode('utf-8')}
                else:
                    parsed = {}
            else:
                raise KeyError(f"Path not found in ZooKeeper: {path}")
            
            zk.stop()
        except KazooException as e:
            config_errors_total.labels(type='zookeeper').inc()
            raise SourceError(f"ZooKeeper error: {e}") from e
        
        config_sources = {'zookeeper': ConfigSource.REMOTE}
        parsed['config_sources'] = config_sources
        
        # Get environment settings for override
        env_settings = cls.from_env(env_prefix)
        
        # Merge
        merged_data = deep_merge(parsed, asdict(env_settings), overwrite=True)
        
        # Create settings
        s = cls(**merged_data)
        
        # Run validation
        errors, warnings = s.validate()
        
        # Record metrics
        load_time = time.time() - start_time
        config_loads_total.labels(source='zookeeper', status='success' if not errors else 'error').inc()
        
        return replace(
            s,
            boot_errors=tuple(errors),
            boot_warnings=tuple(warnings)
        )
    
    @classmethod
    def from_multiple_sources(cls, sources: List[Dict[str, Any]], 
                              env_prefix: str = "TFB_") -> "Settings":
        """Load settings from multiple sources with priority
        
        Args:
            sources: List of source configurations, each with:
                - type: 'env', 'file', 'aws', 'vault', 'consul', 'etcd', 'redis', 'zookeeper'
                - config: Source-specific configuration
                - priority: Optional priority (higher = more important)
        
        Example:
            sources = [
                {'type': 'env', 'priority': 80},
                {'type': 'file', 'config': {'path': 'config.yaml'}, 'priority': 40},
                {'type': 'aws', 'config': {'secret_name': 'myapp/config'}, 'priority': 70},
            ]
        """
        start_time = time.time()
        
        # Sort by priority (highest first)
        sorted_sources = sorted(sources, key=lambda x: x.get('priority', 50), reverse=True)
        
        merged_data = {}
        config_sources = {}
        errors = []
        warnings = []
        
        for source in sorted_sources:
            source_type = source['type']
            source_config = source.get('config', {})
            
            try:
                if source_type == 'env':
                    settings = cls.from_env(env_prefix)
                    merged_data = deep_merge(merged_data, asdict(settings), overwrite=True)
                    config_sources.update(settings.config_sources)
                
                elif source_type == 'file':
                    path = source_config.get('path')
                    if not path:
                        raise ValueError("File source requires 'path'")
                    settings = cls.from_file(path, env_prefix)
                    merged_data = deep_merge(merged_data, asdict(settings), overwrite=True)
                    config_sources.update(settings.config_sources)
                
                elif source_type == 'aws':
                    secret_name = source_config.get('secret_name')
                    if not secret_name:
                        raise ValueError("AWS source requires 'secret_name'")
                    region = source_config.get('region')
                    settings = cls.from_aws_secrets(secret_name, region, env_prefix)
                    merged_data = deep_merge(merged_data, asdict(settings), overwrite=True)
                    config_sources.update(settings.config_sources)
                
                elif source_type == 'vault':
                    path = source_config.get('path')
                    if not path:
                        raise ValueError("Vault source requires 'path'")
                    mount_point = source_config.get('mount_point', 'secret')
                    settings = cls.from_vault(path, mount_point, env_prefix)
                    merged_data = deep_merge(merged_data, asdict(settings), overwrite=True)
                    config_sources.update(settings.config_sources)
                
                elif source_type == 'consul':
                    key = source_config.get('key')
                    if not key:
                        raise ValueError("Consul source requires 'key'")
                    host = source_config.get('host', 'localhost')
                    port = source_config.get('port', 8500)
                    settings = cls.from_consul(key, host, port, env_prefix)
                    merged_data = deep_merge(merged_data, asdict(settings), overwrite=True)
                    config_sources.update(settings.config_sources)
                
                elif source_type == 'etcd':
                    key = source_config.get('key')
                    if not key:
                        raise ValueError("etcd source requires 'key'")
                    host = source_config.get('host', 'localhost')
                    port = source_config.get('port', 2379)
                    settings = cls.from_etcd(key, host, port, env_prefix)
                    merged_data = deep_merge(merged_data, asdict(settings), overwrite=True)
                    config_sources.update(settings.config_sources)
                
                elif source_type == 'redis':
                    key = source_config.get('key')
                    if not key:
                        raise ValueError("Redis source requires 'key'")
                    redis_url = source_config.get('url')
                    if not redis_url:
                        raise ValueError("Redis source requires 'url'")
                    settings = cls.from_redis(key, redis_url, env_prefix)
                    merged_data = deep_merge(merged_data, asdict(settings), overwrite=True)
                    config_sources.update(settings.config_sources)
                
                elif source_type == 'zookeeper':
                    path = source_config.get('path')
                    if not path:
                        raise ValueError("ZooKeeper source requires 'path'")
                    hosts = source_config.get('hosts', ['localhost:2181'])
                    settings = cls.from_zookeeper(path, hosts, env_prefix)
                    merged_data = deep_merge(merged_data, asdict(settings), overwrite=True)
                    config_sources.update(settings.config_sources)
                
                else:
                    warnings.append(f"Unknown source type: {source_type}")
                
            except Exception as e:
                errors.append(f"Source {source_type} failed: {e}")
                config_errors_total.labels(type=source_type).inc()
        
        merged_data['config_sources'] = config_sources
        
        # Create settings
        s = cls(**merged_data)
        
        # Run validation
        validation_errors, validation_warnings = s.validate()
        all_errors = errors + list(validation_errors)
        all_warnings = warnings + list(validation_warnings)
        
        # Record metrics
        load_time = time.time() - start_time
        config_loads_total.labels(source='multiple', status='success' if not all_errors else 'error').inc()
        
        return replace(
            s,
            boot_errors=tuple(all_errors),
            boot_warnings=tuple(all_warnings)
        )
    
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
    def validate(self, schema: Optional[Dict] = None) -> Tuple[List[str], List[str]]:
        """Validate settings, return (errors, warnings)
        
        Args:
            schema: Optional JSON Schema for validation
        """
        errors: List[str] = []
        warnings: List[str] = []
        
        # Backend URL
        if not is_valid_url(self.backend_base_url):
            errors.append(f"BACKEND_BASE_URL invalid: {self.backend_base_url!r} (must start with http:// or https://)")
        
        # Internal URL
        if self.internal_base_url and not is_valid_url(self.internal_base_url):
            warnings.append(f"internal_base_url invalid: {self.internal_base_url!r}")
        
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
        
        # Provider API keys
        provider_key_map = {
            'eodhd': self.eodhd_api_key,
            'finnhub': self.finnhub_api_key,
            'alphavantage': self.alphavantage_api_key,
            'polygon': self.polygon_api_key,
            'iexcloud': self.iexcloud_api_key,
            'tradier': self.tradier_api_key,
            'marketaux': self.marketaux_api_key,
            'newsapi': self.newsapi_api_key,
            'twelve_data': self.twelve_data_api_key,
            'argaam': self.argaam_api_key,
        }
        
        for provider in self.enabled_providers:
            if ProviderType.requires_api_key(provider):
                key = provider_key_map.get(provider)
                if not key:
                    warnings.append(f"Provider {provider} enabled but API key missing")
        
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
            if self.spreadsheet_id:
                warnings.append("Google Sheets credentials missing - sheets features will fail")
        
        # Spreadsheet ID
        if not self.spreadsheet_id and self.google_creds:
            warnings.append("Spreadsheet ID missing - scripts may require it")
        
        # Database
        if self.database_url:
            valid_schemes = ['postgresql://', 'mysql://', 'sqlite://', 'oracle://', 'mssql://']
            if not any(self.database_url.startswith(p) for p in valid_schemes):
                errors.append(f"Invalid database URL scheme: {self.database_url}")
        
        # Redis
        if self.redis_url and not self.redis_url.startswith(('redis://', 'rediss://')):
            errors.append(f"Invalid Redis URL: {self.redis_url}")
        
        # Rate limit Redis
        if self.rate_limit_redis_url and not self.rate_limit_redis_url.startswith(('redis://', 'rediss://')):
            errors.append(f"Invalid rate limit Redis URL: {self.rate_limit_redis_url}")
        
        # CORS origins
        for origin in self.cors_origins:
            if origin != '*' and not is_valid_url(origin):
                errors.append(f"Invalid CORS origin: {origin}")
        
        # Rate limits
        if self.rate_limit_rpm < 30:
            warnings.append(f"Rate limit {self.rate_limit_rpm} RPM is very low")
        elif self.rate_limit_rpm > 5000:
            warnings.append(f"Rate limit {self.rate_limit_rpm} RPM is very high")
        
        # Chaos
        if self.chaos_enabled:
            if self.chaos_failure_rate > 0.5:
                warnings.append(f"High chaos failure rate: {self.chaos_failure_rate}")
            if self.chaos_latency_ms > 5000:
                warnings.append(f"High chaos latency: {self.chaos_latency_ms}ms")
        
        # AI model path
        if self.ai_testing_enabled and self.ai_model_path and not self.ai_model_path.exists():
            warnings.append(f"AI model path does not exist: {self.ai_model_path}")
        
        # Timeouts
        if self.http_timeout_sec < 10:
            warnings.append(f"HTTP timeout {self.http_timeout_sec}s is very low")
        elif self.http_timeout_sec > 120:
            warnings.append(f"HTTP timeout {self.http_timeout_sec}s is very high")
        
        # Batch sizes
        if self.quote_batch_size > 500:
            warnings.append(f"Large quote batch size ({self.quote_batch_size}) may cause timeouts")
        if self.historical_batch_size > 50:
            warnings.append(f"Large historical batch size ({self.historical_batch_size}) may cause timeouts")
        
        # Encryption
        if self.encryption_key and len(self.encryption_key) < 16:
            warnings.append(f"Encryption key is short ({len(self.encryption_key)} chars). Use at least 16 chars.")
        
        # JSON Schema validation
        if schema and JSONSCHEMA_AVAILABLE:
            try:
                validate(instance=self.to_dict(), schema=schema)
            except JSONSchemaValidationError as e:
                errors.append(f"Schema validation failed: {e}")
        
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
    
    def is_testing(self) -> bool:
        """Check if running in testing"""
        return self.environment == Environment.TESTING
    
    def is_staging(self) -> bool:
        """Check if running in staging"""
        return self.environment == Environment.STAGING
    
    def is_local(self) -> bool:
        """Check if running locally"""
        return self.environment == Environment.LOCAL
    
    def is_kubernetes(self) -> bool:
        """Check if running in Kubernetes"""
        return self.environment == Environment.KUBERNETES
    
    def feature_enabled(self, feature: str, environment: Optional[Environment] = None) -> bool:
        """Check if feature flag is enabled"""
        # Check environment
        if environment and environment != self.environment:
            return False
        
        # Standard feature flags
        standard_flags = {
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
            'webhook': self.webhook_enabled,
            'websocket': self.websocket_enabled,
            'streaming': self.streaming_enabled,
            'batch_processing': self.batch_processing_enabled,
        }
        
        if feature in standard_flags:
            return standard_flags[feature]
        
        # Custom feature flags
        return self.feature_flags.get(feature, False)
    
    def feature_enabled_for_user(self, feature: str, user_id: str, environment: Optional[Environment] = None) -> bool:
        """Check if feature flag is enabled for specific user (consistent rollout)"""
        if not self.feature_enabled(feature, environment):
            return False
        
        # Check if feature has custom rollout percentage
        feature_config = self.feature_flags.get(feature, {})
        if isinstance(feature_config, dict) and 'rollout_percentage' in feature_config:
            percentage = feature_config['rollout_percentage']
            if percentage < 100:
                # Consistent hashing
                hash_val = int(hashlib.md5(f"{feature}:{user_id}".encode()).hexdigest(), 16) % 100
                return hash_val < percentage
        
        return True
    
    def ab_test_variant(self, test_name: str, user_id: Optional[str] = None, 
                        buckets: Optional[Dict[str, int]] = None) -> str:
        """Get A/B test variant for user
        
        Args:
            test_name: Name of the A/B test
            user_id: User identifier for consistent bucketing
            buckets: Custom bucket distribution (default: 50/25/25)
        
        Returns:
            Variant name (control, variant_a, variant_b, etc.)
        """
        if not user_id:
            return 'control'
        
        # Default bucket distribution if not provided
        if not buckets:
            buckets = {
                'control': 50,
                'variant_a': 25,
                'variant_b': 25,
            }
        
        # Ensure percentages sum to 100
        total = sum(buckets.values())
        if total != 100:
            # Normalize
            buckets = {k: int(v * 100 / total) for k, v in buckets.items()}
        
        # Deterministic hashing
        hash_input = f"{test_name}:{user_id}".encode()
        hash_val = int(hashlib.md5(hash_input).hexdigest(), 16)
        bucket = hash_val % 100
        
        # Find variant
        cumulative = 0
        for variant, percentage in buckets.items():
            cumulative += percentage
            if bucket < cumulative:
                return variant
        
        return 'control'
    
    def get_provider_config(self, provider: str) -> Dict[str, Any]:
        """Get configuration for specific provider"""
        # Base config
        config = {
            'eodhd': {
                'api_key': self.eodhd_api_key,
                'base_url': 'https://eodhd.com/api',
                'timeout': self.http_timeout_sec,
                'connect_timeout': self.http_connect_timeout,
                'read_timeout': self.http_read_timeout,
                'retries': 3,
                'weight': 1.0,
            },
            'finnhub': {
                'api_key': self.finnhub_api_key,
                'base_url': 'https://finnhub.io/api/v1',
                'timeout': self.http_timeout_sec,
                'connect_timeout': self.http_connect_timeout,
                'read_timeout': self.http_read_timeout,
                'retries': 3,
                'weight': 1.0,
            },
            'alphavantage': {
                'api_key': self.alphavantage_api_key,
                'base_url': 'https://www.alphavantage.co/query',
                'timeout': self.http_timeout_sec,
                'connect_timeout': self.http_connect_timeout,
                'read_timeout': self.http_read_timeout,
                'retries': 3,
                'weight': 1.0,
            },
            'polygon': {
                'api_key': self.polygon_api_key,
                'base_url': 'https://api.polygon.io',
                'timeout': self.http_timeout_sec,
                'connect_timeout': self.http_connect_timeout,
                'read_timeout': self.http_read_timeout,
                'retries': 3,
                'weight': 1.0,
            },
            'iexcloud': {
                'api_key': self.iexcloud_api_key,
                'base_url': 'https://cloud.iexapis.com/stable',
                'timeout': self.http_timeout_sec,
                'connect_timeout': self.http_connect_timeout,
                'read_timeout': self.http_read_timeout,
                'retries': 3,
                'weight': 1.0,
            },
            'tradier': {
                'api_key': self.tradier_api_key,
                'base_url': 'https://api.tradier.com/v1',
                'timeout': self.http_timeout_sec,
                'connect_timeout': self.http_connect_timeout,
                'read_timeout': self.http_read_timeout,
                'retries': 3,
                'weight': 1.0,
            },
            'marketaux': {
                'api_key': self.marketaux_api_key,
                'base_url': 'https://api.marketaux.com/v1',
                'timeout': self.http_timeout_sec,
                'connect_timeout': self.http_connect_timeout,
                'read_timeout': self.http_read_timeout,
                'retries': 3,
                'weight': 1.0,
            },
            'newsapi': {
                'api_key': self.newsapi_api_key,
                'base_url': 'https://newsapi.org/v2',
                'timeout': self.http_timeout_sec,
                'connect_timeout': self.http_connect_timeout,
                'read_timeout': self.http_read_timeout,
                'retries': 3,
                'weight': 1.0,
            },
            'twelve_data': {
                'api_key': self.twelve_data_api_key,
                'base_url': 'https://api.twelvedata.com',
                'timeout': self.http_timeout_sec,
                'connect_timeout': self.http_connect_timeout,
                'read_timeout': self.http_read_timeout,
                'retries': 3,
                'weight': 1.0,
            },
            'yahoo': {
                'api_key': None,
                'base_url': 'https://query1.finance.yahoo.com',
                'timeout': self.http_timeout_sec,
                'connect_timeout': self.http_connect_timeout,
                'read_timeout': self.http_read_timeout,
                'retries': 3,
                'weight': 1.0,
            },
            'yahoo_chart': {
                'api_key': None,
                'base_url': 'https://query1.finance.yahoo.com',
                'timeout': self.http_timeout_sec,
                'connect_timeout': self.http_connect_timeout,
                'read_timeout': self.http_read_timeout,
                'retries': 3,
                'weight': 1.0,
            },
            'argaam': {
                'api_key': self.argaam_api_key,
                'base_url': self.argaam_quote_url,
                'timeout': self.argaam_timeout,
                'connect_timeout': self.http_connect_timeout,
                'read_timeout': self.argaam_timeout,
                'retries': self.argaam_retries,
                'weight': 1.0,
            },
        }
        
        # Get base config
        result = config.get(provider, {}).copy()
        
        # Apply overrides
        if provider in self.provider_weights:
            result['weight'] = self.provider_weights[provider]
        if provider in self.provider_timeouts:
            result['timeout'] = self.provider_timeouts[provider]
        if provider in self.provider_retries:
            result['retries'] = self.provider_retries[provider]
        
        # Apply custom config
        if provider in self.provider_configs:
            result.update(self.provider_configs[provider])
        
        return result
    
    def get_providers_by_priority(self) -> List[str]:
        """Get providers sorted by priority/weight"""
        providers_with_weight = []
        
        for provider in self.enabled_providers:
            config = self.get_provider_config(provider)
            weight = config.get('weight', 1.0)
            providers_with_weight.append((provider, weight))
        
        # Sort by weight descending (higher weight = higher priority)
        providers_with_weight.sort(key=lambda x: x[1], reverse=True)
        
        return [p for p, _ in providers_with_weight]
    
    def get_ksa_providers(self) -> List[str]:
        """Get KSA-specific providers"""
        return [p for p in self.ksa_providers if p in self.enabled_providers]
    
    def get_provider_api_key(self, provider: str) -> Optional[str]:
        """Get API key for provider"""
        key_map = {
            'eodhd': self.eodhd_api_key,
            'finnhub': self.finnhub_api_key,
            'alphavantage': self.alphavantage_api_key,
            'polygon': self.polygon_api_key,
            'iexcloud': self.iexcloud_api_key,
            'tradier': self.tradier_api_key,
            'marketaux': self.marketaux_api_key,
            'newsapi': self.newsapi_api_key,
            'twelve_data': self.twelve_data_api_key,
            'argaam': self.argaam_api_key,
        }
        return key_map.get(provider)
    
    def decrypt_secret(self, encrypted_value: str, context: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Decrypt a secret using configured encryption"""
        return decrypt_value(
            encrypted_value, 
            key=self.encryption_key,
            context=context,
            algorithm=self.encryption_algorithm
        )
    
    def encrypt_secret(self, value: str, context: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Encrypt a secret using configured encryption"""
        return encrypt_value(
            value,
            key=self.encryption_key,
            context=context,
            algorithm=self.encryption_algorithm
        )
    
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
        d['polygon_api_key'] = mask_secret(self.polygon_api_key)
        d['iexcloud_api_key'] = mask_secret(self.iexcloud_api_key)
        d['tradier_api_key'] = mask_secret(self.tradier_api_key)
        d['marketaux_api_key'] = mask_secret(self.marketaux_api_key)
        d['newsapi_api_key'] = mask_secret(self.newsapi_api_key)
        d['twelve_data_api_key'] = mask_secret(self.twelve_data_api_key)
        d['argaam_api_key'] = mask_secret(self.argaam_api_key)
        d['sentry_dsn'] = mask_secret(self.sentry_dsn)
        d['datadog_api_key'] = mask_secret(self.datadog_api_key)
        d['datadog_app_key'] = mask_secret(self.datadog_app_key)
        d['newrelic_license_key'] = mask_secret(self.newrelic_license_key)
        
        # Mask encryption key
        d['encryption_key'] = mask_secret(self.encryption_key)
        
        # Mask credentials
        d['google_creds'] = "PRESENT" if self.google_creds else "MISSING"
        
        # Mask database URL
        if self.database_url:
            if '@' in self.database_url:
                # postgresql://user:pass@host/db -> postgresql://***@host/db
                parts = self.database_url.split('@')
                d['database_url'] = f"***@{parts[-1]}"
            else:
                d['database_url'] = "***"
        
        # Mask Redis URL
        if self.redis_url:
            if '@' in self.redis_url:
                parts = self.redis_url.split('@')
                d['redis_url'] = f"***@{parts[-1]}"
            else:
                d['redis_url'] = "***"
        
        # Mask rate limit Redis
        if self.rate_limit_redis_url:
            if '@' in self.rate_limit_redis_url:
                parts = self.rate_limit_redis_url.split('@')
                d['rate_limit_redis_url'] = f"***@{parts[-1]}"
            else:
                d['rate_limit_redis_url'] = "***"
        
        # Handle Enums
        d['environment'] = self.environment.value
        d['log_level'] = self.log_level.value
        d['cache_backend'] = self.cache_backend.value
        d['encryption_algorithm'] = self.encryption_algorithm.value
        
        # Mask provider configs
        masked_provider_configs = {}
        for provider, config in self.provider_configs.items():
            masked_config = config.copy()
            for key in masked_config:
                if any(s in key.lower() for s in ['key', 'token', 'secret', 'password']):
                    masked_config[key] = mask_secret(str(masked_config[key]))
            masked_provider_configs[provider] = masked_config
        d['provider_configs'] = masked_provider_configs
        
        return d
    
    # =========================================================================
    # Export
    # =========================================================================
    def to_dict(self, mask_secrets: bool = False) -> Dict[str, Any]:
        """Convert to dictionary"""
        if mask_secrets:
            return self.mask_sensitive()
        
        d = asdict(self)
        
        # Handle Enums
        d['environment'] = self.environment.value
        d['log_level'] = self.log_level.value
        d['cache_backend'] = self.cache_backend.value
        d['encryption_algorithm'] = self.encryption_algorithm.value
        d['config_sources'] = {k: v.value for k, v in self.config_sources.items()}
        
        # Handle Path
        if d['log_file']:
            d['log_file'] = str(d['log_file'])
        if d['ai_model_path']:
            d['ai_model_path'] = str(d['ai_model_path'])
        
        return d
    
    def to_json(self, indent: int = 2, mask_secrets: bool = False) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(mask_secrets), indent=indent, default=str)
    
    def to_yaml(self, mask_secrets: bool = False) -> str:
        """Convert to YAML string"""
        if not YAML_AVAILABLE:
            raise ImportError("PyYAML required for YAML export")
        return yaml.dump(self.to_dict(mask_secrets), default_flow_style=False, sort_keys=False)
    
    def to_toml(self, mask_secrets: bool = False) -> str:
        """Convert to TOML format"""
        if not TOML_AVAILABLE:
            raise ImportError("toml required for TOML export")
        return toml.dumps(self.to_dict(mask_secrets))
    
    def to_env_file(self, mask_secrets: bool = False) -> str:
        """Convert to .env file format"""
        lines = []
        for key, value in self.to_dict(mask_secrets).items():
            if value is None:
                continue
            if isinstance(value, (list, dict)):
                value = json.dumps(value)
            lines.append(f"{key.upper()}={value}")
        return '\n'.join(lines)
    
    # =========================================================================
    # Comparison
    # =========================================================================
    def diff(self, other: "Settings") -> Dict[str, Tuple[Any, Any, ConfigChangeType]]:
        """Compare with another settings object
        
        Returns:
            Dictionary of changed fields with (old_value, new_value, change_type)
        """
        current_dict = self.to_dict()
        other_dict = other.to_dict()
        
        diff = {}
        all_keys = set(current_dict.keys()) | set(other_dict.keys())
        
        for key in all_keys:
            current_val = current_dict.get(key)
            other_val = other_dict.get(key)
            
            if key not in current_dict:
                diff[key] = (None, other_val, ConfigChangeType.ADDED)
            elif key not in other_dict:
                diff[key] = (current_val, None, ConfigChangeType.DELETED)
            elif current_val != other_val:
                diff[key] = (current_val, other_val, ConfigChangeType.MODIFIED)
        
        return diff
    
    def deep_diff(self, other: "Settings") -> Dict[str, Any]:
        """Deep compare with another settings object using DeepDiff"""
        if DEEPDIFF_AVAILABLE:
            return DeepDiff(self.to_dict(), other.to_dict(), ignore_order=True)
        return self.diff(other)
    
    def hash(self) -> str:
        """Compute hash of settings for change detection"""
        return compute_hash(self.to_dict())
    
    # =========================================================================
    # Compliance
    # =========================================================================
    def compliance_report(self, standards: Optional[List[ComplianceStandard]] = None) -> Dict[str, Any]:
        """Generate compliance report
        
        Args:
            standards: List of compliance standards to check
        """
        if not standards:
            standards = [ComplianceStandard.GDPR, ComplianceStandard.SOC2, ComplianceStandard.SAMA]
        
        report = {
            'version': self.config_version,
            'schema_version': self.schema_version,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'timestamp_riyadh': riyadh_iso(),
            'environment': self.environment.value,
            'service': self.service_name,
            'service_id': self.service_id,
            'standards': {},
            'security': {
                'auth_enabled': not self.open_mode,
                'auth_header': self.auth_header_name,
                'rate_limiting': self.rate_limit_enabled,
                'rate_limit_rpm': self.rate_limit_rpm,
                'cors_origins': self.cors_origins,
                'cors_credentials': self.cors_allow_credentials,
                'tokens_configured': bool(self.app_token or self.backup_app_token),
                'tokens_count': len([t for t in [self.app_token, self.backup_app_token] if t]),
                'session_timeout': 3600,
                'jwt_configured': False,
                'encryption_enabled': bool(self.encryption_key),
                'encryption_algorithm': self.encryption_algorithm.value if self.encryption_key else None,
            },
            'data_protection': {
                'encryption_at_rest': bool(self.database_url),
                'encryption_in_transit': self.backend_base_url.startswith('https'),
                'sensitive_data_masked': True,
                'secret_rotation': False,
                'private_keys_secured': bool(self.google_creds and 'private_key' in str(self.google_creds)),
            },
            'monitoring': {
                'metrics_enabled': self.metrics_enabled,
                'tracing_enabled': self.tracing_enabled,
                'sentry_configured': bool(self.sentry_dsn),
                'datadog_configured': bool(self.datadog_api_key),
                'newrelic_configured': bool(self.newrelic_license_key),
                'health_check': self.health_check_path,
                'readiness_check': self.readiness_path,
                'liveness_check': self.liveness_path,
                'metrics_port': self.metrics_port,
            },
            'logging': {
                'structured': self.log_format == 'json',
                'level': self.log_level.value,
                'correlation_ids': self.log_correlation_id,
                'access_log': True,
                'audit_log': False,
            },
            'performance': {
                'timeouts_configured': True,
                'batch_sizes_configured': True,
                'connection_pools_configured': self.connection_pool_size > 0,
                'cache_configured': self.cache_backend != CacheBackend.NONE,
            },
            'compliance': {},
            'validation': {
                'errors': list(self.boot_errors),
                'warnings': list(self.boot_warnings),
                'is_valid': len(self.boot_errors) == 0,
                'error_count': len(self.boot_errors),
                'warning_count': len(self.boot_warnings),
            }
        }
        
        # Check each standard
        for standard in standards:
            if standard == ComplianceStandard.GDPR:
                report['standards']['gdpr'] = {
                    'compliant': self._check_gdpr_compliance(),
                    'requirements': {
                        'data_minimization': True,
                        'purpose_limitation': True,
                        'storage_limitation': bool(self.database_url),
                        'integrity_confidentiality': self.backend_base_url.startswith('https'),
                        'accountability': self.log_correlation_id,
                    }
                }
            
            elif standard == ComplianceStandard.SOC2:
                report['standards']['soc2'] = {
                    'compliant': self._check_soc2_compliance(),
                    'requirements': {
                        'security': not self.open_mode,
                        'availability': self.metrics_enabled,
                        'processing_integrity': self.circuit_breaker_enabled,
                        'confidentiality': bool(self.app_token),
                        'privacy': self.log_correlation_id,
                    }
                }
            
            elif standard == ComplianceStandard.HIPAA:
                report['standards']['hipaa'] = {
                    'compliant': self._check_hipaa_compliance(),
                    'requirements': {
                        'access_control': not self.open_mode,
                        'audit_controls': self.log_correlation_id,
                        'integrity': self.circuit_breaker_enabled,
                        'person_or_entity_auth': bool(self.app_token),
                        'transmission_security': self.backend_base_url.startswith('https'),
                    }
                }
            
            elif standard == ComplianceStandard.PCI_DSS:
                report['standards']['pci_dss'] = {
                    'compliant': self._check_pci_compliance(),
                    'requirements': {
                        'firewall': True,
                        'secure_config': self.is_valid(),
                        'protect_stored_data': bool(self.database_url and 'postgresql' in self.database_url),
                        'encrypt_transmission': self.backend_base_url.startswith('https'),
                        'access_control': not self.open_mode,
                        'monitoring': self.metrics_enabled,
                        'policy': True,
                    }
                }
            
            elif standard == ComplianceStandard.ISO27001:
                report['standards']['iso27001'] = {
                    'compliant': self._check_iso_compliance(),
                    'requirements': {
                        'information_security_policy': True,
                        'asset_management': True,
                        'access_control': not self.open_mode,
                        'cryptography': self.backend_base_url.startswith('https'),
                        'physical_security': True,
                        'operations_security': self.metrics_enabled,
                        'communications_security': self.cors_origins is not None,
                    }
                }
            
            elif standard == ComplianceStandard.SAMA:
                report['standards']['sama'] = {
                    'compliant': self._check_sama_compliance(),
                    'requirements': {
                        'data_residency': self._check_data_residency(),
                        'audit_trail': self.log_correlation_id,
                        'risk_management': self.circuit_breaker_enabled,
                        'business_continuity': True,
                        'incident_response': True,
                    }
                }
        
        return report
    
    def _check_gdpr_compliance(self) -> bool:
        """Check GDPR compliance"""
        # Basic checks
        checks = [
            self.backend_base_url.startswith('https'),  # Encryption in transit
            self.log_correlation_id,  # Audit logging
            not self.open_mode,  # Access control
            bool(self.database_url),  # Data storage (for data minimization)
        ]
        return all(checks)
    
    def _check_soc2_compliance(self) -> bool:
        """Check SOC2 compliance"""
        checks = [
            not self.open_mode,  # Security
            self.metrics_enabled,  # Availability monitoring
            self.circuit_breaker_enabled,  # Processing integrity
            bool(self.app_token),  # Confidentiality
            self.log_correlation_id,  # Privacy
        ]
        return all(checks)
    
    def _check_hipaa_compliance(self) -> bool:
        """Check HIPAA compliance"""
        checks = [
            not self.open_mode,  # Access control
            self.log_correlation_id,  # Audit controls
            self.circuit_breaker_enabled,  # Integrity
            bool(self.app_token),  # Authentication
            self.backend_base_url.startswith('https'),  # Transmission security
        ]
        return all(checks)
    
    def _check_pci_compliance(self) -> bool:
        """Check PCI DSS compliance"""
        checks = [
            self.is_valid(),  # Secure configuration
            bool(self.database_url),  # Protect stored data
            self.backend_base_url.startswith('https'),  # Encrypt transmission
            not self.open_mode,  # Access control
            self.metrics_enabled,  # Monitoring
        ]
        return all(checks)
    
    def _check_iso_compliance(self) -> bool:
        """Check ISO 27001 compliance"""
        checks = [
            not self.open_mode,  # Access control
            self.backend_base_url.startswith('https'),  # Cryptography
            self.metrics_enabled,  # Operations security
            bool(self.cors_origins),  # Communications security
        ]
        return all(checks)
    
    def _check_sama_compliance(self) -> bool:
        """Check SAMA compliance"""
        checks = [
            not self.open_mode,  # Access control
            self.log_correlation_id,  # Audit trail
            self.circuit_breaker_enabled,  # Risk management
            self.backend_base_url.startswith('https'),  # Encryption
            self.timezone == "Asia/Riyadh",  # Local timezone
        ]
        return all(checks)
    
    def _check_data_residency(self) -> bool:
        """Check data residency requirements"""
        # Check if database is in Saudi Arabia (simplified)
        if self.database_url:
            # Check for Saudi regions
            saudi_regions = ['me-south-1', 'sa-central-1', 'sa-west-1', 'riyadh', 'jeddah']
            return any(region in self.database_url.lower() for region in saudi_regions)
        return True
    
    # =========================================================================
    # History
    # =========================================================================
    def snapshot(self) -> Dict[str, Any]:
        """Create a snapshot of current configuration"""
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'version': self.config_version,
            'hash': self.hash(),
            'settings': self.to_dict(),
        }
    
    def add_to_history(self) -> "Settings":
        """Add current state to history"""
        history_entry = self.snapshot()
        new_history = list(self.config_history) + [history_entry]
        # Keep last 100 entries
        if len(new_history) > 100:
            new_history = new_history[-100:]
        return replace(self, config_history=tuple(new_history))
    
    def rollback(self, index: int = -2) -> "Settings":
        """Rollback to previous configuration
        
        Args:
            index: History index (-1 for last, -2 for previous, etc.)
        """
        if not self.config_history:
            raise ValueError("No history available")
        
        if abs(index) > len(self.config_history):
            raise ValueError(f"Index {index} out of range")
        
        snapshot = self.config_history[index]
        settings_dict = snapshot['settings']
        
        # Remove history from the dict to avoid circular reference
        settings_dict.pop('config_history', None)
        
        return Settings(**settings_dict)

# =============================================================================
# Dynamic Configuration Manager
# =============================================================================
class DynamicConfig:
    """Dynamic configuration with hot-reload support"""
    
    def __init__(self, initial_settings: Settings):
        self._settings = initial_settings
        self._lock = RLock()
        self._observers: List[Callable[[Settings, Settings], None]] = []
        self._last_reload = datetime.now(timezone.utc)
        self._reload_count = 0
        self._history: List[Dict[str, Any]] = []
        self._sources: List[Dict[str, Any]] = []
        self._auto_reload = False
        self._reload_interval = 60
        self._reload_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._cache = ConfigCache(maxsize=100, ttl=30)
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        
        # Add initial snapshot
        self._add_to_history(initial_settings)
        
        # Start auto-reloader if enabled
        if os.getenv('CONFIG_AUTO_RELOAD', '').lower() in {'true', '1', 'yes'}:
            interval = int(os.getenv('CONFIG_RELOAD_INTERVAL', '60'))
            self.start_auto_reload(interval)
    
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
    
    @property
    def history(self) -> List[Dict[str, Any]]:
        """Get configuration history"""
        with self._lock:
            return self._history.copy()
    
    def get_circuit_breaker(self, name: str) -> CircuitBreaker:
        """Get or create circuit breaker"""
        with self._lock:
            if name not in self._circuit_breakers:
                self._circuit_breakers[name] = CircuitBreaker(name)
            return self._circuit_breakers[name]
    
    def reload(self, source: str = "manual") -> bool:
        """Reload configuration from sources
        
        Args:
            source: Reload source for audit trail
        
        Returns:
            True if reload was successful
        """
        start_time = time.time()
        
        try:
            # Reload from environment
            new_settings = Settings.from_env()
            
            # Merge with existing
            with self._lock:
                old_settings = self._settings
                
                # Check for changes
                if old_settings.hash() == new_settings.hash():
                    structured_log('INFO', 'Configuration reloaded - no changes', 
                                  source=source, duration=time.time() - start_time)
                    return True
                
                # Update settings
                self._settings = new_settings.add_to_history()
                self._last_reload = datetime.now(timezone.utc)
                self._reload_count += 1
                
                # Add to history
                self._add_to_history(new_settings)
            
            # Notify observers
            self._notify_observers(old_settings, new_settings)
            
            # Record metrics
            duration = time.time() - start_time
            config_reload_duration.observe(duration)
            config_loads_total.labels(source=source, status='success').inc()
            config_version_gauge.labels(environment=self._settings.environment.value).set(self._reload_count)
            
            # Update info metric
            if PROMETHEUS_AVAILABLE:
                config_info.info({
                    'version': self._settings.config_version,
                    'environment': self._settings.environment.value,
                    'reload_count': str(self._reload_count),
                    'last_reload': self._last_reload.isoformat(),
                })
            
            structured_log('INFO', 'Configuration reloaded successfully', 
                          source=source, reload_count=self._reload_count,
                          duration=duration, changes=len(self.diff(old_settings, new_settings)))
            
            return True
            
        except Exception as e:
            duration = time.time() - start_time
            config_errors_total.labels(type='reload').inc()
            config_loads_total.labels(source=source, status='error').inc()
            
            structured_log('ERROR', 'Configuration reload failed', 
                          source=source, error=str(e), duration=duration)
            
            logger.error(f"Configuration reload failed: {e}")
            return False
    
    def reload_from_file(self, path: Union[str, Path]) -> bool:
        """Reload configuration from file"""
        start_time = time.time()
        
        try:
            new_settings = Settings.from_file(path)
            
            with self._lock:
                old_settings = self._settings
                
                if old_settings.hash() == new_settings.hash():
                    return True
                
                self._settings = new_settings.add_to_history()
                self._last_reload = datetime.now(timezone.utc)
                self._reload_count += 1
                self._add_to_history(new_settings)
            
            self._notify_observers(old_settings, new_settings)
            
            duration = time.time() - start_time
            config_reload_duration.observe(duration)
            config_loads_total.labels(source='file', status='success').inc()
            
            structured_log('INFO', 'Configuration reloaded from file', 
                          path=str(path), reload_count=self._reload_count, duration=duration)
            
            return True
            
        except Exception as e:
            duration = time.time() - start_time
            config_errors_total.labels(type='file_reload').inc()
            config_loads_total.labels(source='file', status='error').inc()
            
            structured_log('ERROR', 'Configuration reload from file failed', 
                          path=str(path), error=str(e), duration=duration)
            
            logger.error(f"Configuration reload from file failed: {e}")
            return False
    
    def reload_from_source(self, source_type: str, source_config: Dict[str, Any]) -> bool:
        """Reload from specific source"""
        try:
            if source_type == 'env':
                new_settings = Settings.from_env()
            elif source_type == 'file':
                path = source_config.get('path')
                if not path:
                    raise ValueError("File source requires 'path'")
                new_settings = Settings.from_file(path)
            elif source_type == 'aws':
                secret_name = source_config.get('secret_name')
                if not secret_name:
                    raise ValueError("AWS source requires 'secret_name'")
                region = source_config.get('region')
                new_settings = Settings.from_aws_secrets(secret_name, region)
            elif source_type == 'vault':
                path = source_config.get('path')
                if not path:
                    raise ValueError("Vault source requires 'path'")
                mount_point = source_config.get('mount_point', 'secret')
                new_settings = Settings.from_vault(path, mount_point)
            elif source_type == 'consul':
                key = source_config.get('key')
                if not key:
                    raise ValueError("Consul source requires 'key'")
                host = source_config.get('host', 'localhost')
                port = source_config.get('port', 8500)
                new_settings = Settings.from_consul(key, host, port)
            elif source_type == 'etcd':
                key = source_config.get('key')
                if not key:
                    raise ValueError("etcd source requires 'key'")
                host = source_config.get('host', 'localhost')
                port = source_config.get('port', 2379)
                new_settings = Settings.from_etcd(key, host, port)
            elif source_type == 'redis':
                key = source_config.get('key')
                if not key:
                    raise ValueError("Redis source requires 'key'")
                redis_url = source_config.get('url')
                if not redis_url:
                    raise ValueError("Redis source requires 'url'")
                new_settings = Settings.from_redis(key, redis_url)
            else:
                raise ValueError(f"Unknown source type: {source_type}")
            
            with self._lock:
                old_settings = self._settings
                
                if old_settings.hash() == new_settings.hash():
                    return True
                
                self._settings = new_settings.add_to_history()
                self._last_reload = datetime.now(timezone.utc)
                self._reload_count += 1
                self._add_to_history(new_settings)
            
            self._notify_observers(old_settings, new_settings)
            
            config_loads_total.labels(source=source_type, status='success').inc()
            
            return True
            
        except Exception as e:
            config_errors_total.labels(type=f'{source_type}_reload').inc()
            config_loads_total.labels(source=source_type, status='error').inc()
            logger.error(f"Configuration reload from {source_type} failed: {e}")
            return False
    
    def watch(self, callback: Callable[[Settings, Settings], None]) -> Callable:
        """Register observer for config changes
        
        Args:
            callback: Function that receives (old_settings, new_settings)
        
        Returns:
            Unsubscribe function
        """
        with self._lock:
            self._observers.append(callback)
        
        # Return unsubscribe function
        def unsubscribe():
            with self._lock:
                if callback in self._observers:
                    self._observers.remove(callback)
        
        return unsubscribe
    
    def _notify_observers(self, old: Settings, new: Settings):
        """Notify observers of config change"""
        with self._lock:
            observers = self._observers.copy()
        
        for callback in observers:
            try:
                callback(old, new)
            except Exception as e:
                logger.error(f"Observer callback failed: {e}")
    
    def _add_to_history(self, settings: Settings):
        """Add settings to history"""
        self._history.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'hash': settings.hash(),
            'reload_count': self._reload_count,
            'source': 'auto' if self._auto_reload else 'manual',
        })
        
        # Keep last 100 entries
        if len(self._history) > 100:
            self._history = self._history[-100:]
    
    def start_auto_reload(self, interval: int = 60):
        """Start background reloader thread"""
        if self._reload_thread and self._reload_thread.is_alive():
            self.stop_auto_reload()
        
        self._auto_reload = True
        self._reload_interval = interval
        self._stop_event.clear()
        
        self._reload_thread = threading.Thread(
            target=self._auto_reloader,
            daemon=True,
            name="ConfigAutoReloader"
        )
        self._reload_thread.start()
        
        structured_log('INFO', 'Auto-reloader started', interval=interval)
    
    def stop_auto_reload(self):
        """Stop background reloader thread"""
        self._auto_reload = False
        self._stop_event.set()
        
        if self._reload_thread:
            self._reload_thread.join(timeout=5)
            self._reload_thread = None
        
        structured_log('INFO', 'Auto-reloader stopped')
    
    def _auto_reloader(self):
        """Auto-reloader thread function"""
        while not self._stop_event.is_set():
            time.sleep(self._reload_interval)
            
            if self._stop_event.is_set():
                break
            
            try:
                self.reload(source="auto")
            except Exception as e:
                logger.error(f"Auto-reload failed: {e}")
    
    def diff(self, old: Optional[Settings] = None, new: Optional[Settings] = None) -> Dict[str, Tuple[Any, Any, ConfigChangeType]]:
        """Compare settings
        
        Args:
            old: Old settings (defaults to previous version from history)
            new: New settings (defaults to current)
        
        Returns:
            Dictionary of changes
        """
        with self._lock:
            current = new or self._settings
            previous = old or (self._history[-2]['settings'] if len(self._history) >= 2 else None)
            
            if not previous:
                return {}
            
            # Convert dict to Settings if needed
            if isinstance(previous, dict):
                previous = Settings(**previous)
            
            return previous.diff(current)
    
    def rollback(self, steps: int = 1) -> bool:
        """Rollback to previous configuration
        
        Args:
            steps: Number of steps to rollback (1 = previous, 2 = before that, etc.)
        
        Returns:
            True if rollback was successful
        """
        with self._lock:
            if len(self._history) <= steps:
                logger.error(f"Cannot rollback {steps} steps, only {len(self._history)} history entries")
                return False
            
            target_idx = -1 - steps
            snapshot = self._history[target_idx]
            
            try:
                settings_dict = snapshot['settings'].copy()
                settings_dict.pop('config_history', None)
                
                old_settings = self._settings
                new_settings = Settings(**settings_dict)
                
                self._settings = new_settings.add_to_history()
                self._last_reload = datetime.now(timezone.utc)
                self._reload_count += 1
                
                self._notify_observers(old_settings, new_settings)
                
                structured_log('INFO', 'Configuration rolled back', 
                              steps=steps, hash=snapshot['hash'])
                
                return True
                
            except Exception as e:
                logger.error(f"Rollback failed: {e}")
                return False
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get configuration metrics"""
        with self._lock:
            return {
                'reload_count': self._reload_count,
                'last_reload': self._last_reload.isoformat(),
                'history_size': len(self._history),
                'observer_count': len(self._observers),
                'auto_reload': self._auto_reload,
                'reload_interval': self._reload_interval if self._auto_reload else None,
                'current_hash': self._settings.hash(),
                'is_valid': self._settings.is_valid(),
                'error_count': len(self._settings.boot_errors),
                'warning_count': len(self._settings.boot_warnings),
                'circuit_breakers': {name: cb.get_stats() for name, cb in self._circuit_breakers.items()},
                'cache_stats': self._cache.get_stats(),
            }

# =============================================================================
# Configuration Manager with Distributed Coordination
# =============================================================================
class DistributedConfigManager:
    """Configuration manager with distributed coordination"""
    
    def __init__(self, settings: Settings, redis_url: Optional[str] = None):
        self._settings = settings
        self._lock = RLock()
        self._configs: Dict[str, Settings] = {}
        self._last_update: Dict[str, datetime] = {}
        self._leader = False
        self._leader_key = "config:leader"
        self._leader_ttl = 30
        
        # Redis for coordination
        self._redis_client = None
        if REDIS_AVAILABLE and (redis_url or settings.redis_url):
            try:
                redis_url = redis_url or settings.redis_url
                if redis_url:
                    self._redis_client = redis.from_url(
                        redis_url, 
                        decode_responses=True,
                        socket_timeout=5
                    )
            except Exception as e:
                logger.warning(f"Failed to connect to Redis for distributed config: {e}")
    
    def register_service(self, service_id: str, settings: Optional[Settings] = None):
        """Register service configuration"""
        with self._lock:
            self._configs[service_id] = settings or self._settings
            self._last_update[service_id] = datetime.now(timezone.utc)
            
            # Publish to Redis if available
            if self._redis_client:
                try:
                    self._redis_client.hset(
                        f"config:services:{service_id}",
                        mapping={
                            'timestamp': datetime.now(timezone.utc).isoformat(),
                            'hash': (settings or self._settings).hash(),
                            'environment': (settings or self._settings).environment.value,
                        }
                    )
                    self._redis_client.expire(f"config:services:{service_id}", 300)  # 5 min TTL
                except Exception as e:
                    logger.warning(f"Failed to publish to Redis: {e}")
    
    def get_service_config(self, service_id: str) -> Optional[Settings]:
        """Get configuration for a service"""
        with self._lock:
            return self._configs.get(service_id)
    
    def get_all_services(self) -> Dict[str, Settings]:
        """Get all registered service configurations"""
        with self._lock:
            return self._configs.copy()
    
    def get_active_services(self, max_age_seconds: int = 300) -> List[str]:
        """Get services that have updated recently"""
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=max_age_seconds)
        
        active = []
        with self._lock:
            for service_id, last_update in self._last_update.items():
                if last_update > cutoff:
                    active.append(service_id)
        
        return active
    
    def sync_from_redis(self, service_pattern: str = "config:services:*"):
        """Sync configurations from Redis"""
        if not self._redis_client:
            return
        
        try:
            keys = self._redis_client.keys(service_pattern)
            for key in keys:
                data = self._redis_client.hgetall(key)
                if data:
                    service_id = key.replace("config:services:", "")
                    self._last_update[service_id] = datetime.fromisoformat(data['timestamp'])
                    # We don't have the full settings, just metadata
        except Exception as e:
            logger.warning(f"Failed to sync from Redis: {e}")
    
    def broadcast_change(self, service_id: str, settings: Settings):
        """Broadcast configuration change to other services"""
        if not self._redis_client:
            return
        
        try:
            channel = f"config:changes"
            self._redis_client.publish(channel, json.dumps({
                'service_id': service_id,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'hash': settings.hash(),
                'environment': settings.environment.value,
            }))
        except Exception as e:
            logger.warning(f"Failed to broadcast change: {e}")
    
    def subscribe_to_changes(self, callback: Callable[[str, str], None]):
        """Subscribe to configuration changes"""
        if not self._redis_client:
            return
        
        def listener():
            pubsub = self._redis_client.pubsub()
            pubsub.subscribe('config:changes')
            
            for message in pubsub.listen():
                if message['type'] == 'message':
                    try:
                        data = json.loads(message['data'])
                        callback(data['service_id'], data['hash'])
                    except Exception as e:
                        logger.error(f"Failed to process change message: {e}")
        
        thread = threading.Thread(target=listener, daemon=True)
        thread.start()
    
    def try_acquire_leadership(self) -> bool:
        """Try to acquire leadership using Redis"""
        if not self._redis_client:
            return False
        
        try:
            # Use SET NX with expiration for leader election
            acquired = self._redis_client.set(
                self._leader_key,
                socket.gethostname(),
                nx=True,
                ex=self._leader_ttl
            )
            self._leader = bool(acquired)
            return self._leader
        except Exception as e:
            logger.error(f"Failed to acquire leadership: {e}")
            return False
    
    def renew_leadership(self) -> bool:
        """Renew leadership lease"""
        if not self._leader or not self._redis_client:
            return False
        
        try:
            # Renew the lease by updating expiration
            self._redis_client.expire(self._leader_key, self._leader_ttl)
            return True
        except Exception as e:
            logger.error(f"Failed to renew leadership: {e}")
            self._leader = False
            return False

# =============================================================================
# Configuration Middleware for Web Frameworks
# =============================================================================
class ConfigMiddleware:
    """WSGI/ASGI middleware to inject configuration into request context"""
    
    def __init__(self, app, config: DynamicConfig):
        self.app = app
        self.config = config
    
    def __call__(self, environ, start_response):
        # WSGI
        environ['config'] = self.config.settings
        environ['config_manager'] = self.config
        return self.app(environ, start_response)
    
    async def __call__(self, scope, receive, send):
        # ASGI
        if scope['type'] == 'http':
            scope['config'] = self.config.settings
            scope['config_manager'] = self.config
        await self.app(scope, receive, send)

# =============================================================================
# Singleton Instance
# =============================================================================
_settings_instance: Optional[Settings] = None
_dynamic_config: Optional[DynamicConfig] = None
_distributed_manager: Optional[DistributedConfigManager] = None
_init_lock = Lock()

def get_settings() -> Settings:
    """Get settings singleton"""
    global _settings_instance
    
    if _settings_instance is None:
        with _init_lock:
            if _settings_instance is None:
                _settings_instance = Settings.from_env()
                
                # Update metrics
                if PROMETHEUS_AVAILABLE:
                    config_info.info({
                        'version': _settings_instance.config_version,
                        'environment': _settings_instance.environment.value,
                    })
                    config_version_gauge.labels(
                        environment=_settings_instance.environment.value
                    ).set(1)
    
    return _settings_instance

def get_dynamic_config() -> DynamicConfig:
    """Get dynamic config singleton"""
    global _dynamic_config
    
    if _dynamic_config is None:
        with _init_lock:
            if _dynamic_config is None:
                _dynamic_config = DynamicConfig(get_settings())
    
    return _dynamic_config

def get_distributed_manager(redis_url: Optional[str] = None) -> DistributedConfigManager:
    """Get distributed config manager singleton"""
    global _distributed_manager
    
    if _distributed_manager is None:
        with _init_lock:
            if _distributed_manager is None:
                _distributed_manager = DistributedConfigManager(
                    get_settings(), 
                    redis_url
                )
    
    return _distributed_manager

def reload_config(source: str = "manual") -> bool:
    """Reload configuration"""
    return get_dynamic_config().reload(source)

def boot_diagnostics() -> Dict[str, Any]:
    """Get boot diagnostics"""
    settings = get_settings()
    return {
        "status": "ok" if not settings.boot_errors else "error",
        "config_version": CONFIG_VERSION,
        "schema_version": CONFIG_SCHEMA_VERSION,
        "build_timestamp": CONFIG_BUILD_TIMESTAMP,
        "environment": settings.environment.value,
        "service_id": settings.service_id,
        "open_mode": settings.open_mode,
        "errors": list(settings.boot_errors),
        "warnings": list(settings.boot_warnings),
        "error_count": len(settings.boot_errors),
        "warning_count": len(settings.boot_warnings),
        "is_valid": len(settings.boot_errors) == 0,
        "masked": settings.mask_sensitive(),
        "compliance": settings.compliance_report(),
        "sources": {k: v.value for k, v in settings.config_sources.items()},
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
        'polygon': settings.polygon_api_key,
        'iexcloud': settings.iexcloud_api_key,
        'tradier': settings.tradier_api_key,
        'marketaux': settings.marketaux_api_key,
        'newsapi': settings.newsapi_api_key,
        'twelve_data': settings.twelve_data_api_key,
        'argaam': settings.argaam_api_key,
    }

def create_config_endpoints(app, path_prefix: str = "/-/config"):
    """Create configuration endpoints for web frameworks"""
    
    @app.get(f"{path_prefix}")
    def get_config(request):
        """Get current configuration (masked)"""
        return get_settings().mask_sensitive()
    
    @app.get(f"{path_prefix}/raw")
    def get_raw_config(request):
        """Get raw configuration (admin only)"""
        # Add authentication check here
        return get_settings().to_dict()
    
    @app.post(f"{path_prefix}/reload")
    def reload_config_endpoint(request):
        """Reload configuration"""
        source = request.headers.get('X-Reload-Source', 'http')
        success = reload_config(source)
        return {"success": success, "timestamp": datetime.now(timezone.utc).isoformat()}
    
    @app.get(f"{path_prefix}/diff")
    def get_config_diff(request):
        """Get configuration diff from previous version"""
        config = get_dynamic_config()
        diff = config.diff()
        return {"diff": diff, "current_hash": config.settings.hash()}
    
    @app.post(f"{path_prefix}/rollback")
    def rollback_config(request):
        """Rollback configuration"""
        steps = int(request.query_params.get('steps', 1))
        success = get_dynamic_config().rollback(steps)
        return {"success": success, "steps": steps}
    
    @app.get(f"{path_prefix}/history")
    def get_config_history(request):
        """Get configuration history"""
        return {"history": get_dynamic_config().history}
    
    @app.get(f"{path_prefix}/metrics")
    def get_config_metrics(request):
        """Get configuration metrics"""
        return get_dynamic_config().get_metrics()
    
    @app.get(f"{path_prefix}/compliance")
    def get_compliance_report(request):
        """Get compliance report"""
        standard = request.query_params.get('standard')
        standards = [ComplianceStandard(standard)] if standard else None
        return get_settings().compliance_report(standards)
    
    @app.get(f"{path_prefix}/health")
    def config_health(request):
        """Configuration health check"""
        settings = get_settings()
        return {
            "status": "healthy" if settings.is_valid() else "unhealthy",
            "errors": list(settings.boot_errors),
            "warnings": list(settings.boot_warnings),
            "last_reload": get_dynamic_config().last_reload.isoformat(),
            "reload_count": get_dynamic_config().reload_count,
        }

# =============================================================================
# Signal Handlers for Hot Reload
# =============================================================================
def _handle_sighup(signum, frame):
    """Handle SIGHUP for configuration reload"""
    structured_log('INFO', 'Received SIGHUP, reloading configuration...', signal='SIGHUP')
    reload_config(source='sighup')

def _handle_sigusr1(signum, frame):
    """Handle SIGUSR1 for configuration dump"""
    structured_log('INFO', 'Received SIGUSR1, dumping configuration...', signal='SIGUSR1')
    settings = get_settings()
    masked = settings.mask_sensitive()
    print(json.dumps(masked, indent=2))

# Register signal handlers if in main thread
try:
    if threading.current_thread() is threading.main_thread():
        if hasattr(signal, 'SIGHUP'):
            signal.signal(signal.SIGHUP, _handle_sighup)
        if hasattr(signal, 'SIGUSR1'):
            signal.signal(signal.SIGUSR1, _handle_sigusr1)
except (AttributeError, ValueError):
    # Signals not available on Windows
    pass

# =============================================================================
# Context Managers
# =============================================================================
@contextmanager
def override_settings(**kwargs):
    """Temporarily override settings for testing"""
    original = get_settings()
    
    # Create temporary settings
    temp_settings = replace(original, **kwargs)
    
    # Override singleton
    global _settings_instance
    _settings_instance = temp_settings
    
    try:
        yield temp_settings
    finally:
        # Restore
        _settings_instance = original

@contextmanager
def config_source(source_type: str, source_config: Dict[str, Any]):
    """Temporarily use a different configuration source"""
    original = get_settings()
    
    try:
        if source_type == 'file':
            new_settings = Settings.from_file(source_config['path'])
        elif source_type == 'env':
            new_settings = Settings.from_env()
        elif source_type == 'aws':
            new_settings = Settings.from_aws_secrets(
                source_config['secret_name'],
                source_config.get('region')
            )
        else:
            raise ValueError(f"Unknown source type: {source_type}")
        
        global _settings_instance
        _settings_instance = new_settings
        
        yield new_settings
    finally:
        _settings_instance = original

# =============================================================================
# Initialization
# =============================================================================
def init_config(env_prefix: str = "TFB_", auto_reload: bool = False, 
                reload_interval: int = 60) -> DynamicConfig:
    """Initialize configuration system
    
    Args:
        env_prefix: Environment variable prefix
        auto_reload: Enable auto-reloading
        reload_interval: Auto-reload interval in seconds
    
    Returns:
        DynamicConfig instance
    """
    global _settings_instance, _dynamic_config
    
    with _init_lock:
        # Load settings
        _settings_instance = Settings.from_env(env_prefix)
        
        # Create dynamic config
        _dynamic_config = DynamicConfig(_settings_instance)
        
        # Start auto-reload if requested
        if auto_reload:
            _dynamic_config.start_auto_reload(reload_interval)
        
        # Log initialization
        structured_log('INFO', 'Configuration initialized',
                      version=CONFIG_VERSION,
                      environment=_settings_instance.environment.value,
                      auto_reload=auto_reload,
                      errors=len(_settings_instance.boot_errors),
                      warnings=len(_settings_instance.boot_warnings))
        
        # Warn about errors
        if _settings_instance.boot_errors:
            for error in _settings_instance.boot_errors:
                structured_log('ERROR', 'Configuration error', error=error)
        
        # Warn about warnings
        if _settings_instance.boot_warnings:
            for warning in _settings_instance.boot_warnings:
                structured_log('WARNING', 'Configuration warning', warning=warning)
        
        return _dynamic_config

# =============================================================================
# Export
# =============================================================================
__all__ = [
    # Version
    "CONFIG_VERSION",
    "CONFIG_SCHEMA_VERSION",
    
    # Enums
    "Environment",
    "LogLevel",
    "ProviderType",
    "CacheBackend",
    "ConfigSource",
    "ConfigChangeType",
    "ComplianceStandard",
    "CircuitBreakerState",
    "FeatureStage",
    "ValidationLevel",
    "ConfigEncryptionAlgorithm",
    "ConfigFormat",
    
    # Exceptions
    "ConfigurationError",
    "ValidationError",
    "SourceError",
    "EncryptionError",
    "ReloadError",
    "CircuitBreakerOpenError",
    "ConfigNotFoundError",
    "ConfigPermissionError",
    "ConfigVersionError",
    
    # Main classes
    "Settings",
    "DynamicConfig",
    "DistributedConfigManager",
    "ConfigMiddleware",
    "CircuitBreaker",
    "ConfigCache",
    
    # Factories
    "get_settings",
    "get_dynamic_config",
    "get_distributed_manager",
    "reload_config",
    "boot_diagnostics",
    "init_config",
    "create_config_endpoints",
    
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
    "coerce_path",
    "coerce_timedelta",
    "coerce_bytes",
    "coerce_datetime",
    "is_valid_url",
    "is_valid_hostport",
    "is_valid_email",
    "is_valid_ip",
    "is_valid_uuid",
    "is_valid_jwt",
    "is_valid_datetime",
    "mask_secret",
    "mask_secret_dict",
    "looks_like_jwt",
    "looks_like_api_key",
    "secret_strength_hint",
    "repair_private_key",
    "decrypt_value",
    "encrypt_value",
    "compute_hash",
    "deep_merge",
    "get_correlation_id",
    "structured_log",
    "utc_now",
    "riyadh_now",
    "utc_iso",
    "riyadh_iso",
    "parse_file_format",
    "load_file_content",
    
    # Context managers
    "override_settings",
    "config_source",
    
    # Pydantic models (if available)
] + (["SettingsBaseModel", "DatabaseConfig", "RedisConfig", "CacheConfig", 
      "ProviderConfig", "FeatureFlags", "SecurityConfig", "LoggingConfig", 
      "MonitoringConfig", "PerformanceConfig", "DeploymentConfig", 
      "GoogleSheetsConfig", "ArgaamConfig", "ChaosConfig", "AITestConfig"] 
     if PYDANTIC_AVAILABLE else [])
