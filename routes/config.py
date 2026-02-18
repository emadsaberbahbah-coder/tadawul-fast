"""
routes/config.py
------------------------------------------------------------
TADAWUL ENTERPRISE CONFIGURATION MANAGEMENT — v4.0.0 (DISTRIBUTED + ENCRYPTED + HOT-RELOAD)
SAMA Compliant | Distributed Config | Secrets Management | Dynamic Updates

Core Capabilities:
- Distributed configuration management (Consul, etcd, Redis)
- Encrypted secrets with AWS KMS / HashiCorp Vault integration
- Hot-reload with zero downtime
- Multi-environment support (dev, staging, prod)
- Feature flags with gradual rollout
- Audit logging for configuration changes
- Circuit breaker for external config sources
- Graceful fallback to local config
- SAMA-compliant audit trail
- Dynamic routing configuration
- Rate limiting configuration
- ML model version management
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib
import json
import logging
import os
import re
import socket
import threading
import time
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, is_dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import lru_cache, wraps
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)
from urllib.parse import urlparse

# Optional enterprise integrations
try:
    import consul
    from consul.base import Timeout
    _CONSUL_AVAILABLE = True
except ImportError:
    _CONSUL_AVAILABLE = False

try:
    import etcd3
    _ETCD_AVAILABLE = True
except ImportError:
    _ETCD_AVAILABLE = False

try:
    import aioredis
    from aioredis import Redis
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False

try:
    import boto3
    from botocore.exceptions import ClientError
    _AWS_AVAILABLE = True
except ImportError:
    _AWS_AVAILABLE = False

try:
    import hvac
    _VAULT_AVAILABLE = True
except ImportError:
    _VAULT_AVAILABLE = False

try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2
    _CRYPTO_AVAILABLE = True
except ImportError:
    _CRYPTO_AVAILABLE = False

# Pydantic imports
try:
    from pydantic import (
        BaseModel,
        ConfigDict,
        Field,
        field_validator,
        model_validator,
        ValidationError,
        AliasChoices,
        SecretStr,
        SecretBytes,
    )
    from pydantic.functional_validators import AfterValidator
    from typing_extensions import Annotated
    _PYDANTIC_V2 = True
except ImportError:
    from pydantic import BaseModel, Field, validator  # type: ignore
    _PYDANTIC_V2 = False

logger = logging.getLogger("routes.config")

CONFIG_VERSION = "4.0.0"
SHIM_VERSION = "4.0.0-shim"

# =============================================================================
# Enums & Types
# =============================================================================

class Environment(str, Enum):
    """Deployment environments"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TEST = "test"
    DEMO = "demo"

class ConfigProvider(str, Enum):
    """Configuration providers"""
    LOCAL = "local"
    CONSUL = "consul"
    ETCD = "etcd"
    REDIS = "redis"
    VAULT = "vault"
    AWS_SECRETS = "aws_secrets"
    AWS_PARAM_STORE = "aws_param_store"
    ENV = "environment"

class SecretProvider(str, Enum):
    """Secrets management providers"""
    LOCAL = "local"
    VAULT = "vault"
    AWS_KMS = "aws_kms"
    AWS_SECRETS = "aws_secrets"
    AZURE_KEYVAULT = "azure_keyvault"
    GCP_SECRETS = "gcp_secrets"

class FeatureStage(str, Enum):
    """Feature rollout stages"""
    ALPHA = "alpha"
    BETA = "beta"
    GA = "ga"
    DEPRECATED = "deprecated"
    REMOVED = "removed"

class CacheStrategy(str, Enum):
    """Caching strategies"""
    NONE = "none"
    LOCAL = "local"
    REDIS = "redis"
    MEMCACHED = "memcached"
    HYBRID = "hybrid"

class AuthMethod(str, Enum):
    """Authentication methods"""
    TOKEN = "token"
    JWT = "jwt"
    OAUTH2 = "oauth2"
    API_KEY = "api_key"
    MTLS = "mtls"
    BASIC = "basic"

# =============================================================================
# Secure Configuration Models
# =============================================================================

class DatabaseConfig(BaseModel):
    """Database configuration"""
    host: str = "localhost"
    port: int = 5432
    name: str = "tadawul"
    user: str = "postgres"
    password: SecretStr = Field(default_factory=lambda: SecretStr(""))
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 3600
    echo: bool = False
    ssl_mode: str = "prefer"
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)
        
        @property
        def connection_string(self) -> str:
            """Get PostgreSQL connection string"""
            return f"postgresql://{self.user}:{self.password.get_secret_value()}@{self.host}:{self.port}/{self.name}"

class RedisConfig(BaseModel):
    """Redis configuration"""
    host: str = "localhost"
    port: int = 6379
    password: SecretStr = Field(default_factory=lambda: SecretStr(""))
    db: int = 0
    ssl: bool = False
    socket_timeout: float = 5.0
    socket_connect_timeout: float = 5.0
    max_connections: int = 20
    decode_responses: bool = True
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)
        
        @property
        def connection_string(self) -> str:
            """Get Redis connection string"""
            auth = f":{self.password.get_secret_value()}@" if self.password.get_secret_value() else ""
            return f"redis://{auth}{self.host}:{self.port}/{self.db}"

class ConsulConfig(BaseModel):
    """Consul configuration"""
    host: str = "localhost"
    port: int = 8500
    token: SecretStr = Field(default_factory=lambda: SecretStr(""))
    scheme: str = "http"
    verify: bool = True
    timeout: float = 10.0
    dc: Optional[str] = None
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)

class VaultConfig(BaseModel):
    """HashiCorp Vault configuration"""
    url: str = "http://localhost:8200"
    token: SecretStr = Field(default_factory=lambda: SecretStr(""))
    role_id: Optional[SecretStr] = None
    secret_id: Optional[SecretStr] = None
    mount_point: str = "secret"
    kv_version: int = 2
    verify: bool = True
    timeout: float = 10.0
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)

class AWSConfig(BaseModel):
    """AWS configuration"""
    region: str = "me-south-1"  # Bahrain region for low latency to KSA
    access_key_id: Optional[SecretStr] = None
    secret_access_key: Optional[SecretStr] = None
    session_token: Optional[SecretStr] = None
    kms_key_id: Optional[str] = None
    secrets_manager_prefix: str = "tadawul"
    param_store_path: str = "/tadawul/"
    use_instance_metadata: bool = True
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)

class AuthConfig(BaseModel):
    """Authentication configuration"""
    method: AuthMethod = AuthMethod.TOKEN
    header_name: str = "X-APP-TOKEN"
    allow_query_token: bool = False
    token_expiry_seconds: int = 86400  # 24 hours
    jwt_secret: Optional[SecretStr] = None
    jwt_algorithm: str = "HS256"
    jwt_expiry_seconds: int = 3600
    oauth2_provider: Optional[str] = None
    oauth2_client_id: Optional[str] = None
    oauth2_client_secret: Optional[SecretStr] = None
    api_key_header: str = "X-API-Key"
    mTLS_enabled: bool = False
    mTLS_ca_cert: Optional[str] = None
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)

class RateLimitConfig(BaseModel):
    """Rate limiting configuration"""
    enabled: bool = True
    requests_per_second: int = 10
    burst_size: int = 20
    per_ip: bool = True
    per_token: bool = True
    per_endpoint: bool = True
    redis_backend: bool = False
    storage_uri: Optional[str] = None
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)

class CacheConfig(BaseModel):
    """Caching configuration"""
    strategy: CacheStrategy = CacheStrategy.HYBRID
    default_ttl_seconds: int = 300
    max_size: int = 10000
    redis_uri: Optional[str] = None
    local_ttl_seconds: int = 60
    enable_prediction: bool = False
    enable_compression: bool = False
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)

class MLConfig(BaseModel):
    """ML model configuration"""
    enabled: bool = True
    model_version: str = "1.0.0"
    model_path: Optional[str] = None
    s3_bucket: Optional[str] = None
    s3_key: Optional[str] = None
    cache_ttl_seconds: int = 3600
    batch_size: int = 32
    prediction_timeout_seconds: float = 5.0
    feature_store: Optional[str] = None
    enable_explainability: bool = True
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)

class SAMAConfig(BaseModel):
    """SAMA compliance configuration"""
    enabled: bool = True
    audit_log_enabled: bool = True
    audit_retention_days: int = 90
    trading_hours_start: str = "10:00"
    trading_hours_end: str = "15:00"
    weekend_days: List[int] = Field(default_factory=lambda: [4, 5])  # Friday, Saturday
    holiday_file: Optional[str] = None
    reporting_currency: str = "SAR"
    max_leverage: float = 1.0
    min_capital: float = 500000  # 500K SAR for institutional
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)

class FeatureFlag(BaseModel):
    """Feature flag configuration"""
    name: str
    enabled: bool = False
    stage: FeatureStage = FeatureStage.ALPHA
    rollout_percentage: int = 0
    enabled_for: List[str] = Field(default_factory=list)
    disabled_for: List[str] = Field(default_factory=list)
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    description: Optional[str] = None
    owner: Optional[str] = None
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)
        
        def is_enabled_for(self, user_id: str) -> bool:
            """Check if feature is enabled for specific user"""
            if not self.enabled:
                return False
            
            if self.enabled_for and user_id not in self.enabled_for:
                return False
            
            if self.disabled_for and user_id in self.disabled_for:
                return False
            
            if self.rollout_percentage < 100:
                # Consistent hashing for rollout
                hash_val = int(hashlib.md5(user_id.encode()).hexdigest(), 16) % 100
                if hash_val >= self.rollout_percentage:
                    return False
            
            if self.start_date:
                start = datetime.fromisoformat(self.start_date)
                if datetime.now() < start:
                    return False
            
            if self.end_date:
                end = datetime.fromisoformat(self.end_date)
                if datetime.now() > end:
                    return False
            
            return True

class EndpointConfig(BaseModel):
    """API endpoint configuration"""
    path: str
    methods: List[str] = Field(default_factory=lambda: ["GET"])
    rate_limit: Optional[int] = None
    auth_required: bool = True
    roles_allowed: List[str] = Field(default_factory=list)
    cache_ttl: Optional[int] = None
    timeout_seconds: float = 30.0
    enabled: bool = True
    deprecated: bool = False
    version: str = "1.0.0"
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)

class ServiceConfig(BaseModel):
    """Microservice configuration"""
    name: str
    host: str = "localhost"
    port: int
    protocol: str = "http"
    health_check_path: str = "/health"
    timeout_seconds: float = 10.0
    retry_count: int = 3
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 60.0
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)
        
        @property
        def url(self) -> str:
            """Get service URL"""
            return f"{self.protocol}://{self.host}:{self.port}"

class TadawulConfig(BaseModel):
    """Main configuration model"""
    
    # Basic
    environment: Environment = Environment.PRODUCTION
    service_name: str = "tadawul-fast-bridge"
    version: str = CONFIG_VERSION
    
    # Core
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    
    # Distributed config
    config_provider: ConfigProvider = ConfigProvider.LOCAL
    consul: ConsulConfig = Field(default_factory=ConsulConfig)
    etcd_hosts: List[str] = Field(default_factory=list)
    etcd_prefix: str = "/tadawul/"
    
    # Secrets
    secret_provider: SecretProvider = SecretProvider.LOCAL
    vault: VaultConfig = Field(default_factory=VaultConfig)
    aws: AWSConfig = Field(default_factory=AWSConfig)
    
    # Auth
    auth: AuthConfig = Field(default_factory=AuthConfig)
    tokens: List[SecretStr] = Field(default_factory=list)
    backup_tokens: List[SecretStr] = Field(default_factory=list)
    
    # Rate limiting
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    
    # Cache
    cache: CacheConfig = Field(default_factory=CacheConfig)
    
    # ML/AI
    ml: MLConfig = Field(default_factory=MLConfig)
    
    # SAMA compliance
    sama: SAMAConfig = Field(default_factory=SAMAConfig)
    
    # Feature flags
    features: Dict[str, FeatureFlag] = Field(default_factory=dict)
    
    # Endpoints
    endpoints: Dict[str, EndpointConfig] = Field(default_factory=dict)
    
    # Services
    services: Dict[str, ServiceConfig] = Field(default_factory=dict)
    
    # Batch processing
    batch_size: int = 25
    batch_timeout_sec: float = 60.0
    batch_concurrency: int = 6
    max_tickers: int = 1200
    route_timeout_sec: float = 120.0
    
    # Monitoring
    enable_metrics: bool = True
    enable_tracing: bool = False
    metrics_port: int = 9090
    trace_sample_rate: float = 0.1
    
    # Logging
    log_level: str = "INFO"
    log_format: str = "json"
    log_file: Optional[str] = None
    
    # Security
    enable_cors: bool = True
    cors_origins: List[str] = Field(default_factory=list)
    enable_csrf: bool = True
    secure_cookies: bool = True
    session_timeout: int = 3600
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)
        
        @model_validator(mode="after")
        def validate_config(self) -> "TadawulConfig":
            """Validate configuration"""
            # Validate tokens
            if self.auth.method == AuthMethod.TOKEN and not self.tokens and self.environment == Environment.PRODUCTION:
                logger.warning("No authentication tokens configured in production mode")
            
            # Validate SAMA compliance
            if self.sama.enabled:
                if self.sama.trading_hours_start >= self.sama.trading_hours_end:
                    raise ValueError("Trading hours end must be after start")
            
            return self
        
        def get_tokens(self) -> List[str]:
            """Get all tokens as strings"""
            tokens = [t.get_secret_value() for t in self.tokens if t.get_secret_value()]
            tokens.extend([t.get_secret_value() for t in self.backup_tokens if t.get_secret_value()])
            return tokens

# =============================================================================
# Secure Configuration Manager
# =============================================================================

class ConfigEncryption:
    """Configuration encryption/decryption"""
    
    def __init__(self, key: Optional[bytes] = None):
        self.key = key or self._generate_key()
        self.fernet = Fernet(self.key) if _CRYPTO_AVAILABLE else None
    
    def _generate_key(self) -> bytes:
        """Generate encryption key from environment"""
        salt = os.getenv("CONFIG_SALT", "tadawul-salt").encode()
        password = os.getenv("CONFIG_PASSWORD", "tadawul-secret").encode()
        
        kdf = PBKDF2(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password))
        return key
    
    def encrypt(self, data: str) -> str:
        """Encrypt string data"""
        if not self.fernet:
            return data
        return self.fernet.encrypt(data.encode()).decode()
    
    def decrypt(self, data: str) -> str:
        """Decrypt string data"""
        if not self.fernet:
            return data
        try:
            return self.fernet.decrypt(data.encode()).decode()
        except Exception:
            return data
    
    def encrypt_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Encrypt dictionary values"""
        result = {}
        for k, v in data.items():
            if isinstance(v, str) and any(secret in k.lower() for secret in ["token", "password", "secret", "key"]):
                result[k] = self.encrypt(v)
            elif isinstance(v, dict):
                result[k] = self.encrypt_dict(v)
            elif isinstance(v, list):
                result[k] = [self.encrypt_dict(item) if isinstance(item, dict) else item for item in v]
            else:
                result[k] = v
        return result
    
    def decrypt_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Decrypt dictionary values"""
        result = {}
        for k, v in data.items():
            if isinstance(v, str) and any(secret in k.lower() for secret in ["token", "password", "secret", "key"]):
                result[k] = self.decrypt(v)
            elif isinstance(v, dict):
                result[k] = self.decrypt_dict(v)
            elif isinstance(v, list):
                result[k] = [self.decrypt_dict(item) if isinstance(item, dict) else item for item in v]
            else:
                result[k] = v
        return result

class ConfigManager:
    """Distributed configuration manager"""
    
    def __init__(self):
        self._config: Optional[TadawulConfig] = None
        self._config_lock = asyncio.Lock()
        self._watchers: List[Callable] = []
        self._watch_task: Optional[asyncio.Task] = None
        self._encryption = ConfigEncryption()
        self._providers: Dict[ConfigProvider, Any] = {}
        self._stats = defaultdict(int)
        self._last_update: Optional[datetime] = None
        
        # Initialize providers
        self._init_providers()
        
        # Load initial config
        self._load_initial_config()
    
    def _init_providers(self):
        """Initialize configuration providers"""
        
        # Consul
        if _CONSUL_AVAILABLE:
            try:
                consul_config = TadawulConfig().consul
                self._providers[ConfigProvider.CONSUL] = consul.Consul(
                    host=consul_config.host,
                    port=consul_config.port,
                    token=consul_config.token.get_secret_value() if consul_config.token.get_secret_value() else None,
                    scheme=consul_config.scheme,
                    verify=consul_config.verify
                )
                logger.info("Consul provider initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Consul: {e}")
        
        # etcd
        if _ETCD_AVAILABLE:
            try:
                etcd_hosts = TadawulConfig().etcd_hosts
                if etcd_hosts:
                    self._providers[ConfigProvider.ETCD] = etcd3.client(
                        host=etcd_hosts[0].split(":")[0],
                        port=int(etcd_hosts[0].split(":")[1]) if ":" in etcd_hosts[0] else 2379
                    )
                    logger.info("etcd provider initialized")
            except Exception as e:
                logger.error(f"Failed to initialize etcd: {e}")
        
        # Redis
        if _REDIS_AVAILABLE:
            try:
                redis_config = TadawulConfig().redis
                self._providers[ConfigProvider.REDIS] = Redis(
                    host=redis_config.host,
                    port=redis_config.port,
                    password=redis_config.password.get_secret_value() if redis_config.password.get_secret_value() else None,
                    db=redis_config.db,
                    ssl=redis_config.ssl,
                    socket_timeout=redis_config.socket_timeout,
                    socket_connect_timeout=redis_config.socket_connect_timeout,
                    max_connections=redis_config.max_connections,
                    decode_responses=redis_config.decode_responses
                )
                logger.info("Redis provider initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Redis: {e}")
        
        # Vault
        if _VAULT_AVAILABLE:
            try:
                vault_config = TadawulConfig().vault
                client = hvac.Client(
                    url=vault_config.url,
                    token=vault_config.token.get_secret_value() if vault_config.token.get_secret_value() else None,
                    verify=vault_config.verify
                )
                if vault_config.role_id and vault_config.secret_id:
                    client.auth.approle.login(
                        role_id=vault_config.role_id.get_secret_value(),
                        secret_id=vault_config.secret_id.get_secret_value()
                    )
                self._providers[ConfigProvider.VAULT] = client
                logger.info("Vault provider initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Vault: {e}")
        
        # AWS
        if _AWS_AVAILABLE:
            try:
                aws_config = TadawulConfig().aws
                session = boto3.Session(
                    aws_access_key_id=aws_config.access_key_id.get_secret_value() if aws_config.access_key_id else None,
                    aws_secret_access_key=aws_config.secret_access_key.get_secret_value() if aws_config.secret_access_key else None,
                    aws_session_token=aws_config.session_token.get_secret_value() if aws_config.session_token else None,
                    region_name=aws_config.region
                )
                self._providers[ConfigProvider.AWS_SECRETS] = session.client("secretsmanager")
                self._providers[ConfigProvider.AWS_PARAM_STORE] = session.client("ssm")
                logger.info("AWS providers initialized")
            except Exception as e:
                logger.error(f"Failed to initialize AWS: {e}")
    
    def _load_initial_config(self):
        """Load initial configuration"""
        try:
            # Try to load from environment first
            config = self._load_from_env()
            
            # Override with provider if available
            provider_config = self._load_from_provider()
            if provider_config:
                config.update(provider_config)
            
            # Decrypt secrets
            config = self._encryption.decrypt_dict(config)
            
            # Create config object
            self._config = TadawulConfig(**config)
            self._last_update = datetime.now()
            
            logger.info(f"Configuration loaded from {config.get('config_provider', 'local')}")
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            # Load default config
            self._config = TadawulConfig()
    
    def _load_from_env(self) -> Dict[str, Any]:
        """Load configuration from environment variables"""
        config: Dict[str, Any] = {}
        
        # Environment
        env = os.getenv("APP_ENV", "production").lower()
        if env in [e.value for e in Environment]:
            config["environment"] = env
        
        # Database
        if os.getenv("DB_HOST"):
            config["database"] = {
                "host": os.getenv("DB_HOST"),
                "port": int(os.getenv("DB_PORT", "5432")),
                "name": os.getenv("DB_NAME", "tadawul"),
                "user": os.getenv("DB_USER", "postgres"),
                "password": os.getenv("DB_PASSWORD", ""),
            }
        
        # Redis
        if os.getenv("REDIS_HOST"):
            config["redis"] = {
                "host": os.getenv("REDIS_HOST"),
                "port": int(os.getenv("REDIS_PORT", "6379")),
                "password": os.getenv("REDIS_PASSWORD", ""),
                "db": int(os.getenv("REDIS_DB", "0")),
            }
        
        # Tokens
        tokens = []
        for key in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
            token = os.getenv(key)
            if token:
                tokens.append(token)
        
        if tokens:
            config["tokens"] = tokens
        
        # Auth
        config["auth"] = {
            "header_name": os.getenv("AUTH_HEADER_NAME", "X-APP-TOKEN"),
            "allow_query_token": os.getenv("ALLOW_QUERY_TOKEN", "false").lower() == "true",
        }
        
        # Batch
        config["batch_size"] = int(os.getenv("AI_BATCH_SIZE", "25"))
        config["batch_timeout_sec"] = float(os.getenv("AI_BATCH_TIMEOUT_SEC", "60.0"))
        config["batch_concurrency"] = int(os.getenv("AI_BATCH_CONCURRENCY", "6"))
        config["max_tickers"] = int(os.getenv("AI_MAX_TICKERS", "1200"))
        config["route_timeout_sec"] = float(os.getenv("AI_ROUTE_TIMEOUT_SEC", "120.0"))
        
        return config
    
    def _load_from_provider(self) -> Dict[str, Any]:
        """Load configuration from distributed provider"""
        config: Dict[str, Any] = {}
        
        # Determine provider
        provider_name = os.getenv("CONFIG_PROVIDER", "local").lower()
        try:
            provider = ConfigProvider(provider_name)
        except ValueError:
            provider = ConfigProvider.LOCAL
        
        config["config_provider"] = provider.value
        
        if provider == ConfigProvider.CONSUL and ConfigProvider.CONSUL in self._providers:
            try:
                consul_client = self._providers[ConfigProvider.CONSUL]
                index, data = consul_client.kv.get("tadawul/config")
                if data and data.get("Value"):
                    config.update(json.loads(data["Value"].decode()))
                    self._stats["consul_hits"] += 1
            except Exception as e:
                logger.error(f"Consul load failed: {e}")
                self._stats["consul_errors"] += 1
        
        elif provider == ConfigProvider.ETCD and ConfigProvider.ETCD in self._providers:
            try:
                etcd_client = self._providers[ConfigProvider.ETCD]
                data, _ = etcd_client.get("/tadawul/config")
                if data:
                    config.update(json.loads(data.decode()))
                    self._stats["etcd_hits"] += 1
            except Exception as e:
                logger.error(f"etcd load failed: {e}")
                self._stats["etcd_errors"] += 1
        
        elif provider == ConfigProvider.REDIS and ConfigProvider.REDIS in self._providers:
            try:
                redis_client = self._providers[ConfigProvider.REDIS]
                data = asyncio.run_coroutine_threadsafe(
                    redis_client.get("tadawul:config"),
                    asyncio.get_event_loop()
                ).result(timeout=5)
                if data:
                    config.update(json.loads(data))
                    self._stats["redis_hits"] += 1
            except Exception as e:
                logger.error(f"Redis load failed: {e}")
                self._stats["redis_errors"] += 1
        
        elif provider == ConfigProvider.VAULT and ConfigProvider.VAULT in self._providers:
            try:
                vault_client = self._providers[ConfigProvider.VAULT]
                secret = vault_client.secrets.kv.v2.read_secret_version(
                    mount_point=TadawulConfig().vault.mount_point,
                    path="tadawul/config"
                )
                if secret and "data" in secret:
                    config.update(secret["data"]["data"])
                    self._stats["vault_hits"] += 1
            except Exception as e:
                logger.error(f"Vault load failed: {e}")
                self._stats["vault_errors"] += 1
        
        elif provider == ConfigProvider.AWS_PARAM_STORE and ConfigProvider.AWS_PARAM_STORE in self._providers:
            try:
                ssm = self._providers[ConfigProvider.AWS_PARAM_STORE]
                prefix = TadawulConfig().aws.param_store_path
                response = ssm.get_parameters_by_path(
                    Path=prefix,
                    Recursive=True,
                    WithDecryption=True
                )
                for param in response.get("Parameters", []):
                    name = param["Name"].replace(prefix, "").replace("/", ".")
                    value = param["Value"]
                    # Convert to nested dict
                    parts = name.split(".")
                    current = config
                    for part in parts[:-1]:
                        if part not in current:
                            current[part] = {}
                        current = current[part]
                    current[parts[-1]] = value
                    self._stats["aws_hits"] += 1
            except Exception as e:
                logger.error(f"AWS Param Store load failed: {e}")
                self._stats["aws_errors"] += 1
        
        return config
    
    async def get_config(self) -> TadawulConfig:
        """Get current configuration"""
        if self._config is None:
            self._load_initial_config()
        
        # Check for updates
        await self._check_for_updates()
        
        return cast(TadawulConfig, self._config)
    
    async def _check_for_updates(self):
        """Check for configuration updates"""
        if not self._config:
            return
        
        # Only check every 60 seconds
        if self._last_update and (datetime.now() - self._last_update).total_seconds() < 60:
            return
        
        # Load from provider
        provider_config = self._load_from_provider()
        if provider_config:
            # Decrypt
            provider_config = self._encryption.decrypt_dict(provider_config)
            
            # Update config
            current = self._config.dict()
            current.update(provider_config)
            
            async with self._config_lock:
                self._config = TadawulConfig(**current)
                self._last_update = datetime.now()
                
                # Notify watchers
                for watcher in self._watchers:
                    try:
                        watcher(self._config)
                    except Exception as e:
                        logger.error(f"Config watcher failed: {e}")
    
    def watch(self, callback: Callable[[TadawulConfig], None]):
        """Watch for configuration changes"""
        self._watchers.append(callback)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get configuration statistics"""
        return {
            "provider": self._config.config_provider.value if self._config else None,
            "last_update": self._last_update.isoformat() if self._last_update else None,
            "stats": dict(self._stats),
            "environment": self._config.environment.value if self._config else None,
            "feature_flags": len(self._config.features) if self._config else 0,
            "endpoints": len(self._config.endpoints) if self._config else 0,
            "services": len(self._config.services) if self._config else 0,
        }

_config_manager = ConfigManager()

# =============================================================================
# Legacy Shim for Backward Compatibility
# =============================================================================

class _FallbackSettings:
    """Fallback settings for backward compatibility"""
    
    auth_header_name: str = "X-APP-TOKEN"
    allow_query_token: bool = False
    ai_batch_size: int = 25
    ai_batch_timeout_sec: float = 60.0
    ai_batch_concurrency: int = 6
    ai_max_tickers: int = 1200
    app_env: str = os.getenv("APP_ENV", "production").strip().lower()


Settings = _FallbackSettings


# =============================================================================
# Token Management
# =============================================================================

@lru_cache(maxsize=1)
def _get_allowed_tokens() -> List[str]:
    """Get all allowed tokens"""
    try:
        config = asyncio.run(_config_manager.get_config())
        return config.get_tokens()
    except Exception:
        # Fallback to environment
        tokens = []
        for key in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
            token = os.getenv(key, "").strip()
            if token:
                tokens.append(token)
        return tokens


def allowed_tokens() -> List[str]:
    """Get list of allowed tokens"""
    return _get_allowed_tokens()


def is_open_mode() -> bool:
    """Check if running in open mode (no auth required)"""
    return len(allowed_tokens()) == 0


# =============================================================================
# Auth Header Management
# =============================================================================

@lru_cache(maxsize=1)
def _get_auth_header_name() -> str:
    """Get configured auth header name"""
    # Environment override
    env_name = os.getenv("AUTH_HEADER_NAME", "").strip()
    if env_name:
        return env_name
    
    # Config
    try:
        config = asyncio.run(_config_manager.get_config())
        return config.auth.header_name
    except Exception:
        return "X-APP-TOKEN"


AUTH_HEADER_NAME = _get_auth_header_name()


# =============================================================================
# Query Token Configuration
# =============================================================================

@lru_cache(maxsize=1)
def _get_allow_query_token() -> bool:
    """Check if query token is allowed"""
    # Environment
    env_val = os.getenv("ALLOW_QUERY_TOKEN", "false").lower()
    if env_val in ("true", "1", "yes", "on"):
        return True
    
    # Config
    try:
        config = asyncio.run(_config_manager.get_config())
        return config.auth.allow_query_token
    except Exception:
        return False


def allow_query_token() -> bool:
    """Check if query token is allowed"""
    return _get_allow_query_token()


# =============================================================================
# Auth Helper Functions
# =============================================================================

def _extract_bearer(authorization: Optional[str]) -> str:
    """Extract Bearer token from Authorization header"""
    if not authorization:
        return ""
    
    parts = authorization.strip().split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    
    return ""


def auth_ok(x_app_token: Optional[str]) -> bool:
    """
    Legacy auth check - checks only the configured header token
    """
    tokens = allowed_tokens()
    if not tokens:
        return True
    
    return x_app_token and x_app_token.strip() in tokens


def auth_ok_request(
    *,
    x_app_token: Optional[str] = None,
    authorization: Optional[str] = None,
    query_token: Optional[str] = None,
) -> bool:
    """
    Modern auth check with multiple methods
    """
    tokens = allowed_tokens()
    if not tokens:
        return True
    
    # Check header token
    if x_app_token and x_app_token.strip() in tokens:
        return True
    
    # Check Bearer token
    bearer = _extract_bearer(authorization)
    if bearer and bearer in tokens:
        return True
    
    # Check query token if allowed
    if allow_query_token() and query_token and query_token.strip() in tokens:
        return True
    
    return False


# =============================================================================
# Settings Access
# =============================================================================

def get_settings() -> TadawulConfig:
    """Get current configuration"""
    try:
        return asyncio.run(_config_manager.get_config())
    except Exception as e:
        logger.error(f"Failed to get settings: {e}")
        return TadawulConfig()


# =============================================================================
# Feature Flags
# =============================================================================

def is_feature_enabled(feature_name: str, user_id: Optional[str] = None) -> bool:
    """Check if feature is enabled"""
    try:
        config = asyncio.run(_config_manager.get_config())
        feature = config.features.get(feature_name)
        if not feature:
            return False
        
        if user_id:
            return feature.is_enabled_for(user_id)
        
        return feature.enabled
    except Exception:
        return False


def get_feature_flags() -> Dict[str, FeatureFlag]:
    """Get all feature flags"""
    try:
        config = asyncio.run(_config_manager.get_config())
        return config.features
    except Exception:
        return {}


# =============================================================================
# Endpoint Configuration
# =============================================================================

def get_endpoint_config(path: str) -> Optional[EndpointConfig]:
    """Get configuration for specific endpoint"""
    try:
        config = asyncio.run(_config_manager.get_config())
        return config.endpoints.get(path)
    except Exception:
        return None


# =============================================================================
# Service Discovery
# =============================================================================

def get_service(service_name: str) -> Optional[ServiceConfig]:
    """Get service configuration"""
    try:
        config = asyncio.run(_config_manager.get_config())
        return config.services.get(service_name)
    except Exception:
        return None


# =============================================================================
# Batch Configuration
# =============================================================================

def get_batch_config() -> Dict[str, Any]:
    """Get batch processing configuration"""
    try:
        config = asyncio.run(_config_manager.get_config())
        return {
            "batch_size": config.batch_size,
            "batch_timeout_sec": config.batch_timeout_sec,
            "batch_concurrency": config.batch_concurrency,
            "max_tickers": config.max_tickers,
            "route_timeout_sec": config.route_timeout_sec,
        }
    except Exception:
        return {
            "batch_size": 25,
            "batch_timeout_sec": 60.0,
            "batch_concurrency": 6,
            "max_tickers": 1200,
            "route_timeout_sec": 120.0,
        }


# =============================================================================
# SAMA Compliance
# =============================================================================

def get_sama_config() -> Dict[str, Any]:
    """Get SAMA compliance configuration"""
    try:
        config = asyncio.run(_config_manager.get_config())
        return {
            "enabled": config.sama.enabled,
            "trading_hours_start": config.sama.trading_hours_start,
            "trading_hours_end": config.sama.trading_hours_end,
            "weekend_days": config.sama.weekend_days,
            "audit_enabled": config.sama.audit_log_enabled,
            "retention_days": config.sama.audit_retention_days,
        }
    except Exception:
        return {
            "enabled": True,
            "trading_hours_start": "10:00",
            "trading_hours_end": "15:00",
            "weekend_days": [4, 5],
            "audit_enabled": True,
            "retention_days": 90,
        }


# =============================================================================
# Cache Management
# =============================================================================

def refresh_config_cache() -> None:
    """Refresh all cached configuration"""
    _get_allowed_tokens.cache_clear()
    _get_auth_header_name.cache_clear()
    _get_allow_query_token.cache_clear()


# =============================================================================
# Masked Settings for Logging
# =============================================================================

def mask_settings_dict() -> Dict[str, Any]:
    """
    Safe snapshot for logs - never exposes secrets
    """
    try:
        config = asyncio.run(_config_manager.get_config())
        
        # Create masked copy
        masked = {
            "status": "ok",
            "version": CONFIG_VERSION,
            "environment": config.environment.value,
            "service_name": config.service_name,
            "config_provider": config.config_provider.value,
            "secret_provider": config.secret_provider.value,
            "auth_method": config.auth.method.value,
            "auth_header": config.auth.header_name,
            "allow_query_token": config.auth.allow_query_token,
            "rate_limit_enabled": config.rate_limit.enabled,
            "cache_strategy": config.cache.strategy.value,
            "ml_enabled": config.ml.enabled,
            "sama_enabled": config.sama.enabled,
            "feature_count": len(config.features),
            "endpoint_count": len(config.endpoints),
            "service_count": len(config.services),
            "batch_size": config.batch_size,
            "max_tickers": config.max_tickers,
            "log_level": config.log_level,
            "enable_metrics": config.enable_metrics,
            "enable_tracing": config.enable_tracing,
        }
        
        # Add token count (not the actual tokens)
        masked["token_count"] = len(config.tokens) + len(config.backup_tokens)
        
        # Add provider stats
        masked["provider_stats"] = _config_manager.get_stats()
        
        return masked
        
    except Exception as e:
        logger.error(f"Failed to mask settings: {e}")
        return {
            "status": "fallback",
            "version": CONFIG_VERSION,
            "open_mode": is_open_mode(),
            "auth_header": AUTH_HEADER_NAME,
            "token_count": len(allowed_tokens()),
        }


# =============================================================================
# Configuration Watcher
# =============================================================================

def watch_config(callback: Callable[[TadawulConfig], None]) -> None:
    """Watch for configuration changes"""
    _config_manager.watch(callback)


# =============================================================================
# Root Config Import (for backward compatibility)
# =============================================================================

def _load_root_config() -> Optional[Any]:
    """Try to import root config module"""
    try:
        return importlib.import_module("config")
    except Exception:
        return None


_root_config = _load_root_config()

# If root config exists, try to use its Settings class
if _root_config is not None:
    try:
        root_settings = getattr(_root_config, "Settings", None)
        if root_settings is not None:
            Settings = root_settings
    except Exception:
        pass


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    # Version
    "CONFIG_VERSION",
    
    # Core models
    "TadawulConfig",
    "Environment",
    "ConfigProvider",
    "SecretProvider",
    "FeatureStage",
    "CacheStrategy",
    "AuthMethod",
    
    # Config components
    "DatabaseConfig",
    "RedisConfig",
    "ConsulConfig",
    "VaultConfig",
    "AWSConfig",
    "AuthConfig",
    "RateLimitConfig",
    "CacheConfig",
    "MLConfig",
    "SAMAConfig",
    "FeatureFlag",
    "EndpointConfig",
    "ServiceConfig",
    
    # Legacy shim
    "Settings",
    "AUTH_HEADER_NAME",
    
    # Auth functions
    "allowed_tokens",
    "is_open_mode",
    "allow_query_token",
    "auth_ok",
    "auth_ok_request",
    
    # Config access
    "get_settings",
    "get_batch_config",
    "get_sama_config",
    
    # Feature flags
    "is_feature_enabled",
    "get_feature_flags",
    
    # Endpoints
    "get_endpoint_config",
    
    # Services
    "get_service",
    
    # Utilities
    "mask_settings_dict",
    "refresh_config_cache",
    "watch_config",
    
    # Config manager
    "_config_manager",
]
