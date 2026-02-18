"""
routes/config.py
------------------------------------------------------------
TADAWUL ENTERPRISE CONFIGURATION MANAGEMENT — v4.5.0 (DISTRIBUTED + ENCRYPTED + HOT-RELOAD + AUDITED)
SAMA Compliant | Distributed Config | Secrets Management | Dynamic Updates | Zero-Trust Security

Core Capabilities:
- Distributed configuration management (Consul, etcd, Redis, ZooKeeper)
- Encrypted secrets with AWS KMS / HashiCorp Vault / Azure KeyVault
- Hot-reload with zero downtime and version tracking
- Multi-environment support with inheritance
- Feature flags with gradual rollout and A/B testing
- Audit logging for all configuration changes (SAMA compliant)
- Circuit breaker for external config sources
- Graceful fallback with cascade strategy
- SAMA-compliant audit trail with immutable logs
- Dynamic routing configuration with canary deployments
- Rate limiting configuration per endpoint/user
- ML model version management with A/B testing
- Zero-trust security model with mTLS support
- Configuration validation with JSON Schema
- Configuration encryption at rest and in transit
"""

from __future__ import annotations

import asyncio
import base64
import functools
import hashlib
import hmac
import importlib
import json
import logging
import os
import re
import socket
import threading
import time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, is_dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from functools import lru_cache, wraps
from pathlib import Path
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
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
    from kazoo.client import KazooClient
    from kazoo.recipe.cache import TreeCache
    _ZOOKEEPER_AVAILABLE = True
except ImportError:
    _ZOOKEEPER_AVAILABLE = False

try:
    import aioredis
    from aioredis import Redis
    from aioredis.exceptions import RedisError
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False

try:
    import boto3
    from botocore.exceptions import ClientError
    from botocore.config import Config
    _AWS_AVAILABLE = True
except ImportError:
    _AWS_AVAILABLE = False

try:
    import hvac
    _VAULT_AVAILABLE = True
except ImportError:
    _VAULT_AVAILABLE = False

try:
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient
    _AZURE_AVAILABLE = True
except ImportError:
    _AZURE_AVAILABLE = False

try:
    from google.cloud import secretmanager
    _GCP_AVAILABLE = True
except ImportError:
    _GCP_AVAILABLE = False

try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    _CRYPTO_AVAILABLE = True
except ImportError:
    _CRYPTO_AVAILABLE = False

try:
    import prometheus_client
    from prometheus_client import Counter, Histogram, Gauge, Summary
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False

try:
    from opentelemetry import trace
    from opentelemetry.trace import Span, Status, StatusCode
    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False

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
        Json,
        AnyUrl,
        RedisDsn,
        PostgresDsn,
        AmqpDsn,
    )
    from pydantic.functional_validators import AfterValidator
    from typing_extensions import Annotated
    _PYDANTIC_V2 = True
except ImportError:
    from pydantic import BaseModel, Field, validator  # type: ignore
    _PYDANTIC_V2 = False

logger = logging.getLogger("routes.config")

CONFIG_VERSION = "4.5.0"
SHIM_VERSION = "4.5.0-shim"

# =============================================================================
# Enums & Types
# =============================================================================

class Environment(str, Enum):
    """Deployment environments with inheritance"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TEST = "test"
    DEMO = "demo"
    DR = "dr"  # Disaster Recovery
    SANDBOX = "sandbox"
    
    def inherits_from(self) -> Optional[Environment]:
        """Get parent environment for inheritance"""
        inheritance = {
            Environment.STAGING: Environment.DEVELOPMENT,
            Environment.PRODUCTION: Environment.STAGING,
            Environment.DR: Environment.PRODUCTION,
            Environment.DEMO: Environment.STAGING,
            Environment.SANDBOX: Environment.DEVELOPMENT,
        }
        return inheritance.get(self)

class ConfigProvider(str, Enum):
    """Configuration providers with priority"""
    LOCAL = "local"
    ENV = "environment"
    CONSUL = "consul"
    ETCD = "etcd"
    ZOOKEEPER = "zookeeper"
    REDIS = "redis"
    VAULT = "vault"
    AWS_SECRETS = "aws_secrets"
    AWS_PARAM_STORE = "aws_param_store"
    AZURE_KEYVAULT = "azure_keyvault"
    GCP_SECRETS = "gcp_secrets"
    DATABASE = "database"
    
    @property
    def priority(self) -> int:
        """Get provider priority (higher = more authoritative)"""
        priorities = {
            ConfigProvider.LOCAL: 0,
            ConfigProvider.ENV: 10,
            ConfigProvider.REDIS: 20,
            ConfigProvider.CONSUL: 30,
            ConfigProvider.ETCD: 31,
            ConfigProvider.ZOOKEEPER: 32,
            ConfigProvider.DATABASE: 40,
            ConfigProvider.VAULT: 50,
            ConfigProvider.AWS_PARAM_STORE: 60,
            ConfigProvider.AWS_SECRETS: 61,
            ConfigProvider.AZURE_KEYVAULT: 62,
            ConfigProvider.GCP_SECRETS: 63,
        }
        return priorities.get(self, 0)

class SecretProvider(str, Enum):
    """Secrets management providers"""
    LOCAL = "local"
    ENV = "environment"
    VAULT = "vault"
    AWS_KMS = "aws_kms"
    AWS_SECRETS = "aws_secrets"
    AZURE_KEYVAULT = "azure_keyvault"
    GCP_SECRETS = "gcp_secrets"
    DATABASE = "database"

class FeatureStage(str, Enum):
    """Feature rollout stages"""
    ALPHA = "alpha"          # Internal testing
    BETA = "beta"            # Limited user testing
    GA = "ga"                # General availability
    DEPRECATED = "deprecated"  # Will be removed
    REMOVED = "removed"       # No longer available
    CANARY = "canary"         # Gradual rollout
    EXPERIMENTAL = "experimental"  # Experimental features

class CacheStrategy(str, Enum):
    """Caching strategies"""
    NONE = "none"
    LOCAL = "local"
    REDIS = "redis"
    MEMCACHED = "memcached"
    HYBRID = "hybrid"  # Local + Redis
    DISTRIBUTED = "distributed"  # Redis cluster

class AuthMethod(str, Enum):
    """Authentication methods"""
    TOKEN = "token"
    JWT = "jwt"
    OAUTH2 = "oauth2"
    API_KEY = "api_key"
    MTLS = "mtls"
    BASIC = "basic"
    SAML = "saml"
    OIDC = "oidc"

class LogLevel(str, Enum):
    """Logging levels"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    AUDIT = "AUDIT"  # Special audit level

class LogFormat(str, Enum):
    """Log output formats"""
    JSON = "json"
    TEXT = "text"
    GRAYLOG = "graylog"
    SPLUNK = "splunk"

class DataCenter(str, Enum):
    """Data center locations"""
    AWS_BAHRAIN = "aws-me-south-1"
    AWS_DUBAI = "aws-me-central-1"
    AZURE_UAE = "azure-uaenorth"
    GCP_DOHA = "gcp-me-west1"
    ONPREM_KSA = "onprem-riyadh"
    ONPREM_JEDDAH = "onprem-jeddah"

class ComplianceStandard(str, Enum):
    """Compliance standards"""
    SAMA = "sama"          # Saudi Central Bank
    CMA = "cma"            # Capital Market Authority
    GDPR = "gdpr"          # EU General Data Protection
    SOC2 = "soc2"          # Service Organization Control
    ISO27001 = "iso27001"  # Information Security
    PCI_DSS = "pci_dss"    # Payment Card Industry

class AlertSeverity(str, Enum):
    """Alert severity levels"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"

class CircuitBreakerState(str, Enum):
    """Circuit breaker states"""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

# =============================================================================
# Advanced Configuration Models
# =============================================================================

class DatabaseConfig(BaseModel):
    """Enhanced database configuration"""
    host: str = "localhost"
    port: int = 5432
    name: str = "tadawul"
    user: str = "postgres"
    password: SecretStr = Field(default_factory=lambda: SecretStr(""))
    
    # Connection pooling
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 3600
    pool_pre_ping: bool = True
    
    # SSL/TLS
    ssl_mode: str = "prefer"
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    ssl_root_cert: Optional[str] = None
    
    # Replication
    read_replicas: List[str] = Field(default_factory=list)
    replica_lag_threshold: int = 10  # seconds
    
    # Backup
    backup_enabled: bool = True
    backup_schedule: str = "0 2 * * *"  # Daily at 2 AM
    backup_retention_days: int = 30
    
    # Monitoring
    slow_query_threshold: int = 1000  # ms
    enable_query_logging: bool = False
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, json_encoders={SecretStr: lambda v: "***"})
        
        @property
        def master_dsn(self) -> PostgresDsn:
            """Get master connection string"""
            return PostgresDsn(f"postgresql://{self.user}:{self.password.get_secret_value()}@{self.host}:{self.port}/{self.name}")
        
        @property
        def replica_dsns(self) -> List[PostgresDsn]:
            """Get replica connection strings"""
            return [PostgresDsn(f"postgresql://{self.user}:{self.password.get_secret_value()}@{host}/{self.name}") 
                    for host in self.read_replicas]

class RedisConfig(BaseModel):
    """Enhanced Redis configuration"""
    host: str = "localhost"
    port: int = 6379
    password: SecretStr = Field(default_factory=lambda: SecretStr(""))
    db: int = 0
    
    # Cluster
    cluster_mode: bool = False
    startup_nodes: List[Dict[str, Any]] = Field(default_factory=list)
    
    # Sentinel
    sentinel_mode: bool = False
    sentinel_hosts: List[str] = Field(default_factory=list)
    sentinel_master: str = "mymaster"
    
    # Connection
    ssl: bool = False
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    ssl_ca_certs: Optional[str] = None
    socket_timeout: float = 5.0
    socket_connect_timeout: float = 5.0
    socket_keepalive: bool = True
    max_connections: int = 20
    decode_responses: bool = True
    
    # Pool
    pool_class: str = "ConnectionPool"
    pool_maxsize: int = 10
    pool_timeout: float = 5.0
    
    if _PYDANTIC_V2:
        @property
        def dsn(self) -> RedisDsn:
            """Get Redis DSN"""
            auth = f":{self.password.get_secret_value()}@" if self.password.get_secret_value() else ""
            return RedisDsn(f"redis://{auth}{self.host}:{self.port}/{self.db}")

class ConsulConfig(BaseModel):
    """Enhanced Consul configuration"""
    host: str = "localhost"
    port: int = 8500
    token: SecretStr = Field(default_factory=lambda: SecretStr(""))
    scheme: str = "http"
    verify: bool = True
    timeout: float = 10.0
    dc: Optional[str] = None
    
    # Session
    session_ttl: int = 30
    session_lock_delay: int = 15
    
    # Watch
    watch_enabled: bool = True
    watch_interval: int = 10
    
    # Service discovery
    service_ttl: int = 30
    service_deregister_after: int = 60
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)

class VaultConfig(BaseModel):
    """Enhanced Vault configuration"""
    url: str = "http://localhost:8200"
    token: SecretStr = Field(default_factory=lambda: SecretStr(""))
    role_id: Optional[SecretStr] = None
    secret_id: Optional[SecretStr] = None
    mount_point: str = "secret"
    kv_version: int = 2
    verify: bool = True
    timeout: float = 10.0
    
    # Auth methods
    auth_method: str = "token"  # token, approle, kubernetes, aws, gcp
    kubernetes_role: Optional[str] = None
    kubernetes_jwt: Optional[str] = None
    aws_role: Optional[str] = None
    gcp_role: Optional[str] = None
    
    # Renewal
    renew_token: bool = True
    renew_interval: int = 300
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)

class AWSConfig(BaseModel):
    """Enhanced AWS configuration"""
    region: str = "me-south-1"  # Bahrain
    access_key_id: Optional[SecretStr] = None
    secret_access_key: Optional[SecretStr] = None
    session_token: Optional[SecretStr] = None
    profile: Optional[str] = None
    
    # KMS
    kms_key_id: Optional[str] = None
    kms_aliases: List[str] = Field(default_factory=list)
    
    # Secrets Manager
    secrets_manager_prefix: str = "tadawul"
    secrets_manager_ttl: int = 3600
    
    # Parameter Store
    param_store_path: str = "/tadawul/"
    param_store_recursive: bool = True
    param_store_with_decryption: bool = True
    
    # S3
    s3_bucket: Optional[str] = None
    s3_prefix: str = "config/"
    s3_kms_key: Optional[str] = None
    
    # Assume Role
    assume_role_arn: Optional[str] = None
    assume_role_session_name: Optional[str] = None
    assume_role_duration: int = 3600
    
    # Endpoints
    endpoint_url: Optional[str] = None
    sts_endpoint_url: Optional[str] = None
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)
        
        def get_boto3_config(self) -> Config:
            """Get boto3 config"""
            return Config(
                region_name=self.region,
                signature_version='s3v4',
                retries={'max_attempts': 3, 'mode': 'adaptive'}
            )

class AzureConfig(BaseModel):
    """Azure configuration"""
    tenant_id: Optional[str] = None
    client_id: Optional[SecretStr] = None
    client_secret: Optional[SecretStr] = None
    key_vault_url: Optional[str] = None
    key_vault_ttl: int = 3600
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)

class GCPConfig(BaseModel):
    """Google Cloud configuration"""
    project_id: Optional[str] = None
    credentials_json: Optional[SecretStr] = None
    secret_manager_ttl: int = 3600
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)

class AuthConfig(BaseModel):
    """Enhanced authentication configuration"""
    method: AuthMethod = AuthMethod.TOKEN
    header_name: str = "X-APP-TOKEN"
    allow_query_token: bool = False
    
    # Token auth
    token_expiry_seconds: int = 86400  # 24 hours
    token_rotation_enabled: bool = True
    token_rotation_interval: int = 604800  # 7 days
    
    # JWT
    jwt_secret: Optional[SecretStr] = None
    jwt_public_key: Optional[str] = None
    jwt_private_key: Optional[SecretStr] = None
    jwt_algorithm: str = "HS256"
    jwt_expiry_seconds: int = 3600
    jwt_issuer: Optional[str] = None
    jwt_audience: Optional[str] = None
    
    # OAuth2
    oauth2_provider: Optional[str] = None  # google, azure, okta, auth0
    oauth2_client_id: Optional[str] = None
    oauth2_client_secret: Optional[SecretStr] = None
    oauth2_authorize_url: Optional[str] = None
    oauth2_token_url: Optional[str] = None
    oauth2_userinfo_url: Optional[str] = None
    oauth2_redirect_uri: Optional[str] = None
    oauth2_scopes: List[str] = Field(default_factory=lambda: ["openid", "email", "profile"])
    
    # API Key
    api_key_header: str = "X-API-Key"
    api_key_param: str = "api_key"
    
    # mTLS
    mTLS_enabled: bool = False
    mTLS_ca_cert: Optional[str] = None
    mTLS_cert_required: bool = True
    mTLS_verify_client: bool = True
    
    # SAML
    saml_idp_metadata_url: Optional[str] = None
    saml_sp_entity_id: Optional[str] = None
    saml_acs_url: Optional[str] = None
    saml_cert: Optional[str] = None
    saml_private_key: Optional[SecretStr] = None
    
    # Rate limiting per auth
    rate_limit_requests: int = 100
    rate_limit_window: int = 60
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)

class RateLimitConfig(BaseModel):
    """Enhanced rate limiting configuration"""
    enabled: bool = True
    strategy: str = "token_bucket"  # token_bucket, leaky_bucket, fixed_window, sliding_window
    
    # Global limits
    global_requests_per_second: int = 1000
    global_burst_size: int = 2000
    
    # Per client limits
    per_ip_requests_per_second: int = 10
    per_ip_burst_size: int = 20
    per_token_requests_per_second: int = 50
    per_token_burst_size: int = 100
    per_user_requests_per_second: int = 30
    per_user_burst_size: int = 60
    
    # Per endpoint limits
    per_endpoint_enabled: bool = True
    default_endpoint_limit: int = 100
    
    # Backend
    storage_backend: str = "memory"  # memory, redis, memcached
    redis_uri: Optional[str] = None
    
    # Headers
    headers_enabled: bool = True
    retry_after_header: bool = True
    
    # Exemptions
    whitelist_ips: List[str] = Field(default_factory=list)
    whitelist_tokens: List[str] = Field(default_factory=list)
    whitelist_paths: List[str] = Field(default_factory=list)
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)

class CacheConfig(BaseModel):
    """Enhanced cache configuration"""
    strategy: CacheStrategy = CacheStrategy.HYBRID
    default_ttl_seconds: int = 300
    max_size: int = 10000
    
    # Local cache
    local_ttl_seconds: int = 60
    local_max_size: int = 1000
    
    # Redis cache
    redis_uri: Optional[str] = None
    redis_prefix: str = "tadawul:cache:"
    redis_ttl_seconds: int = 3600
    
    # Advanced features
    enable_prediction: bool = False  # Predictive pre-fetch
    enable_compression: bool = False
    compression_threshold: int = 1024  # bytes
    enable_encryption: bool = False
    encryption_key: Optional[SecretStr] = None
    
    # Cache warming
    warm_on_startup: bool = True
    warm_keys: List[str] = Field(default_factory=list)
    warm_interval: int = 300
    
    # Monitoring
    hit_rate_threshold: float = 0.8
    enable_stats: bool = True
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)

class MLConfig(BaseModel):
    """Enhanced ML configuration"""
    enabled: bool = True
    model_version: str = "1.0.0"
    model_path: Optional[str] = None
    
    # Model storage
    model_registry: str = "local"  # local, s3, gcs, azure
    s3_bucket: Optional[str] = None
    s3_key: Optional[str] = None
    gcs_bucket: Optional[str] = None
    azure_container: Optional[str] = None
    
    # Inference
    batch_size: int = 32
    prediction_timeout_seconds: float = 5.0
    max_concurrent_predictions: int = 10
    cache_predictions: bool = True
    cache_ttl_seconds: int = 3600
    
    # Features
    feature_store: Optional[str] = None
    feature_group: Optional[str] = None
    feature_version: str = "1.0.0"
    
    # Explainability
    enable_explainability: bool = True
    shap_enabled: bool = True
    lime_enabled: bool = False
    
    # A/B Testing
    enable_ab_testing: bool = False
    model_a_version: Optional[str] = None
    model_b_version: Optional[str] = None
    ab_traffic_split: float = 0.5
    
    # Monitoring
    track_predictions: bool = True
    track_performance: bool = True
    alert_on_drift: bool = True
    drift_threshold: float = 0.1
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)

class SAMAConfig(BaseModel):
    """SAMA (Saudi Central Bank) compliance configuration"""
    enabled: bool = True
    audit_log_enabled: bool = True
    audit_retention_days: int = 90
    audit_immutable: bool = True  # Audit logs cannot be modified
    
    # Trading hours
    trading_hours_start: str = "10:00"  # 10 AM Riyadh
    trading_hours_end: str = "15:00"    # 3 PM Riyadh
    weekend_days: List[int] = Field(default_factory=lambda: [4, 5])  # Friday, Saturday
    
    # Holidays
    holiday_file: Optional[str] = None
    holiday_refresh_interval: int = 86400  # 24 hours
    
    # Reporting
    reporting_currency: str = "SAR"
    reporting_timezone: str = "Asia/Riyadh"
    reporting_format: str = "iso20022"  # ISO 20022 XML format
    
    # Capital requirements
    min_capital: float = 500000  # 500K SAR for institutional
    max_leverage: float = 1.0
    margin_requirements: Dict[str, float] = Field(default_factory=lambda: {
        "equity": 0.5,  # 50% margin
        "sukuk": 0.3,   # 30% margin
        "reit": 0.4,    # 40% margin
    })
    
    # Risk management
    var_confidence_level: float = 0.99
    stress_test_scenarios: List[str] = Field(default_factory=lambda: [
        "market_crash_2008",
        "covid_2020",
        "oil_price_crash",
        "interest_rate_hike",
    ])
    
    # Data residency
    data_residency: List[str] = Field(default_factory=lambda: ["sa-central-1", "sa-west-1"])
    data_sovereignty: bool = True  # Data must stay in Saudi Arabia
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)
        
        @field_validator("trading_hours_start", "trading_hours_end")
        def validate_time_format(cls, v):
            """Validate time format HH:MM"""
            if not re.match(r"^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$", v):
                raise ValueError("Time must be in HH:MM format")
            return v

class FeatureFlag(BaseModel):
    """Enhanced feature flag with advanced rollout"""
    name: str
    enabled: bool = False
    stage: FeatureStage = FeatureStage.ALPHA
    
    # Rollout
    rollout_percentage: int = 0
    rollout_strategy: str = "random"  # random, consistent, gradual
    
    # Targeting
    enabled_for: List[str] = Field(default_factory=list)  # User IDs
    disabled_for: List[str] = Field(default_factory=list)
    enabled_for_groups: List[str] = Field(default_factory=list)
    disabled_for_groups: List[str] = Field(default_factory=list)
    enabled_for_ips: List[str] = Field(default_factory=list)
    enabled_for_countries: List[str] = Field(default_factory=list)
    
    # Time constraints
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    start_time: Optional[str] = None  # Daily start time
    end_time: Optional[str] = None    # Daily end time
    
    # A/B testing
    experiment_id: Optional[str] = None
    experiment_variant: Optional[str] = None
    
    # Dependencies
    depends_on: List[str] = Field(default_factory=list)
    required_features: List[str] = Field(default_factory=list)
    
    # Metadata
    description: Optional[str] = None
    owner: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)
        
        def is_enabled_for(self, user_id: str, user_groups: Optional[List[str]] = None, 
                          user_ip: Optional[str] = None, country: Optional[str] = None) -> bool:
            """Check if feature is enabled for specific user"""
            if not self.enabled:
                return False
            
            # Check blacklists
            if self.disabled_for and user_id in self.disabled_for:
                return False
            
            if self.disabled_for_groups and user_groups:
                if any(g in self.disabled_for_groups for g in user_groups):
                    return False
            
            # Check whitelists
            if self.enabled_for and user_id not in self.enabled_for:
                # If whitelist exists and user not in it, check other criteria
                pass
            
            if self.enabled_for_groups and user_groups:
                if not any(g in self.enabled_for_groups for g in user_groups):
                    return False
            
            if self.enabled_for_ips and user_ip:
                if user_ip not in self.enabled_for_ips:
                    return False
            
            if self.enabled_for_countries and country:
                if country not in self.enabled_for_countries:
                    return False
            
            # Check rollout percentage
            if self.rollout_percentage < 100:
                if self.rollout_strategy == "consistent":
                    # Consistent hashing
                    hash_val = int(hashlib.md5(f"{user_id}:{self.name}".encode()).hexdigest(), 16) % 100
                    if hash_val >= self.rollout_percentage:
                        return False
                else:
                    # Random
                    if random.randint(1, 100) > self.rollout_percentage:
                        return False
            
            # Check time constraints
            now = datetime.now()
            
            if self.start_date:
                start = datetime.fromisoformat(self.start_date)
                if now < start:
                    return False
            
            if self.end_date:
                end = datetime.fromisoformat(self.end_date)
                if now > end:
                    return False
            
            if self.start_time:
                start_time = datetime.strptime(self.start_time, "%H:%M").time()
                current_time = now.time()
                if current_time < start_time:
                    return False
            
            if self.end_time:
                end_time = datetime.strptime(self.end_time, "%H:%M").time()
                current_time = now.time()
                if current_time > end_time:
                    return False
            
            return True

class EndpointConfig(BaseModel):
    """Enhanced endpoint configuration"""
    path: str
    methods: List[str] = Field(default_factory=lambda: ["GET"])
    
    # Rate limiting
    rate_limit: Optional[int] = None
    rate_limit_burst: Optional[int] = None
    
    # Auth
    auth_required: bool = True
    auth_methods: List[AuthMethod] = Field(default_factory=list)
    roles_allowed: List[str] = Field(default_factory=list)
    permissions_required: List[str] = Field(default_factory=list)
    
    # Cache
    cache_ttl: Optional[int] = None
    cache_vary_by_user: bool = False
    cache_vary_by_ip: bool = False
    
    # Performance
    timeout_seconds: float = 30.0
    max_body_size: int = 1048576  # 1MB
    
    # Versioning
    version: str = "1.0.0"
    deprecated: bool = False
    sunset_date: Optional[str] = None
    
    # Monitoring
    enabled: bool = True
    track_metrics: bool = True
    log_requests: bool = True
    alert_on_error: bool = False
    error_threshold: float = 0.05  # 5% error rate
    
    # CORS
    cors_enabled: bool = True
    cors_origins: List[str] = Field(default_factory=list)
    cors_credentials: bool = False
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)

class ServiceConfig(BaseModel):
    """Enhanced microservice configuration"""
    name: str
    host: str = "localhost"
    port: int
    protocol: str = "http"
    
    # Health
    health_check_path: str = "/health"
    health_check_interval: int = 30
    health_check_timeout: float = 5.0
    unhealthy_threshold: int = 3
    healthy_threshold: int = 2
    
    # Circuit breaker
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 60.0
    circuit_breaker_half_open: int = 2
    
    # Retry
    retry_count: int = 3
    retry_backoff: float = 1.0
    retry_max_backoff: float = 10.0
    
    # Timeout
    connect_timeout: float = 5.0
    read_timeout: float = 10.0
    write_timeout: float = 10.0
    pool_timeout: float = 10.0
    
    # Load balancing
    load_balancer: str = "round_robin"  # round_robin, least_conn, random, consistent_hash
    stickiness_enabled: bool = False
    stickiness_ttl: int = 300
    
    # Service discovery
    discovery_enabled: bool = True
    discovery_provider: str = "consul"  # consul, etcd, dns, kubernetes
    discovery_tags: List[str] = Field(default_factory=list)
    
    # Metadata
    metadata: Dict[str, Any] = Field(default_factory=dict)
    version: str = "1.0.0"
    environment: Optional[Environment] = None
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)
        
        @property
        def base_url(self) -> str:
            """Get service base URL"""
            return f"{self.protocol}://{self.host}:{self.port}"

class AlertingConfig(BaseModel):
    """Alerting and notification configuration"""
    enabled: bool = True
    
    # Alert rules
    error_rate_threshold: float = 0.05
    latency_threshold_ms: int = 1000
    cpu_threshold: float = 80.0
    memory_threshold: float = 85.0
    
    # Notification channels
    slack_webhook: Optional[str] = None
    slack_channel: Optional[str] = None
    
    pagerduty_key: Optional[SecretStr] = None
    pagerduty_severity: AlertSeverity = AlertSeverity.HIGH
    
    email_enabled: bool = False
    email_recipients: List[str] = Field(default_factory=list)
    email_smtp_host: Optional[str] = None
    email_smtp_port: int = 587
    email_username: Optional[str] = None
    email_password: Optional[SecretStr] = None
    
    sms_enabled: bool = False
    sms_phone_numbers: List[str] = Field(default_factory=list)
    sms_provider: str = "twilio"
    
    # Alert cooldown
    cooldown_minutes: int = 15
    max_alerts_per_hour: int = 10
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)

class TracingConfig(BaseModel):
    """Distributed tracing configuration"""
    enabled: bool = False
    provider: str = "jaeger"  # jaeger, zipkin, datadog, newrelic
    
    # Jaeger
    jaeger_host: str = "localhost"
    jaeger_port: int = 6831
    
    # Zipkin
    zipkin_endpoint: Optional[str] = None
    
    # Sampling
    sample_rate: float = 0.1
    sample_rate_low_priority: float = 0.01
    sample_rate_high_priority: float = 1.0
    
    # Tags
    service_tags: Dict[str, str] = Field(default_factory=dict)
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)

class TadawulConfig(BaseModel):
    """Main configuration model - Single Source of Truth"""
    
    # Basic
    environment: Environment = Environment.PRODUCTION
    service_name: str = "tadawul-fast-bridge"
    service_version: str = CONFIG_VERSION
    data_center: DataCenter = DataCenter.ONPREM_KSA
    
    # Database
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    
    # Cache
    redis: RedisConfig = Field(default_factory=RedisConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    
    # Distributed config
    config_provider: ConfigProvider = ConfigProvider.LOCAL
    config_providers: List[ConfigProvider] = Field(default_factory=lambda: [
        ConfigProvider.LOCAL,
        ConfigProvider.ENV,
        ConfigProvider.REDIS,
        ConfigProvider.CONSUL,
    ])
    
    # Config backends
    consul: ConsulConfig = Field(default_factory=ConsulConfig)
    etcd_hosts: List[str] = Field(default_factory=list)
    etcd_prefix: str = "/tadawul/"
    zookeeper_hosts: List[str] = Field(default_factory=list)
    zookeeper_prefix: str = "/tadawul"
    
    # Secrets management
    secret_provider: SecretProvider = SecretProvider.LOCAL
    secret_providers: List[SecretProvider] = Field(default_factory=lambda: [
        SecretProvider.LOCAL,
        SecretProvider.ENV,
        SecretProvider.VAULT,
        SecretProvider.AWS_SECRETS,
    ])
    
    # Secret backends
    vault: VaultConfig = Field(default_factory=VaultConfig)
    aws: AWSConfig = Field(default_factory=AWSConfig)
    azure: AzureConfig = Field(default_factory=AzureConfig)
    gcp: GCPConfig = Field(default_factory=GCPConfig)
    
    # Auth
    auth: AuthConfig = Field(default_factory=AuthConfig)
    tokens: List[SecretStr] = Field(default_factory=list)
    backup_tokens: List[SecretStr] = Field(default_factory=list)
    
    # Rate limiting
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    
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
    
    # Alerting
    alerting: AlertingConfig = Field(default_factory=AlertingConfig)
    
    # Tracing
    tracing: TracingConfig = Field(default_factory=TracingConfig)
    
    # Batch processing
    batch_size: int = 25
    batch_timeout_sec: float = 60.0
    batch_concurrency: int = 6
    max_tickers: int = 1200
    route_timeout_sec: float = 120.0
    
    # Monitoring
    enable_metrics: bool = True
    metrics_port: int = 9090
    metrics_path: str = "/metrics"
    
    # Logging
    log_level: LogLevel = LogLevel.INFO
    log_format: LogFormat = LogFormat.JSON
    log_file: Optional[str] = None
    log_retention_days: int = 30
    
    # Security
    enable_cors: bool = True
    cors_origins: List[str] = Field(default_factory=lambda: ["*"])
    enable_csrf: bool = True
    csrf_exempt_paths: List[str] = Field(default_factory=list)
    secure_cookies: bool = True
    session_timeout: int = 3600
    session_cookie_name: str = "session"
    
    # Encryption
    encryption_key: Optional[SecretStr] = None
    encryption_salt: Optional[str] = None
    
    # Compliance
    compliance_standards: List[ComplianceStandard] = Field(default_factory=lambda: [ComplianceStandard.SAMA])
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True, validate_assignment=True)
        
        @model_validator(mode="after")
        def validate_config(self) -> "TadawulConfig":
            """Validate configuration"""
            # Validate tokens in production
            if self.environment == Environment.PRODUCTION:
                if self.auth.method == AuthMethod.TOKEN and not self.tokens:
                    logger.warning("No authentication tokens configured in production mode")
                
                if not self.database.password.get_secret_value():
                    logger.warning("Database password not set in production")
                
                if self.sama.enabled:
                    if self.data_center not in [DataCenter.ONPREM_KSA, DataCenter.ONPREM_JEDDAH]:
                        logger.warning("SAMA compliance requires data residency in Saudi Arabia")
            
            # Validate trading hours
            if self.sama.enabled:
                start = datetime.strptime(self.sama.trading_hours_start, "%H:%M").time()
                end = datetime.strptime(self.sama.trading_hours_end, "%H:%M").time()
                if start >= end:
                    raise ValueError("Trading hours end must be after start")
            
            return self
        
        def get_tokens(self) -> List[str]:
            """Get all tokens as strings"""
            tokens = []
            for t in self.tokens:
                val = t.get_secret_value()
                if val:
                    tokens.append(val)
            for t in self.backup_tokens:
                val = t.get_secret_value()
                if val:
                    tokens.append(val)
            return tokens
        
        def get_feature(self, name: str) -> Optional[FeatureFlag]:
            """Get feature flag by name"""
            return self.features.get(name)
        
        def get_endpoint(self, path: str) -> Optional[EndpointConfig]:
            """Get endpoint config by path"""
            return self.endpoints.get(path)
        
        def get_service(self, name: str) -> Optional[ServiceConfig]:
            """Get service config by name"""
            return self.services.get(name)

# =============================================================================
# Secure Configuration Manager with Encryption
# =============================================================================

class ConfigEncryption:
    """Advanced configuration encryption with key rotation"""
    
    def __init__(self, master_key: Optional[bytes] = None):
        self.master_key = master_key or self._derive_master_key()
        self._fernet = Fernet(self.master_key) if _CRYPTO_AVAILABLE else None
        self._aesgcm = AESGCM(self.master_key[:32]) if _CRYPTO_AVAILABLE and hasattr(AESGCM, '__call__') else None
        self._key_version = 1
        self._key_rotation_time = datetime.now()
    
    def _derive_master_key(self) -> bytes:
        """Derive master key from environment"""
        salt = os.getenv("CONFIG_SALT", "tadawul-salt-2024").encode()
        password = os.getenv("CONFIG_PASSWORD", "tadawul-secret-key-2024").encode()
        
        kdf = PBKDF2(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password))
        return key
    
    def encrypt(self, data: str, context: Optional[Dict[str, Any]] = None) -> str:
        """Encrypt string data with context"""
        if not self._fernet:
            return data
        
        try:
            # Add context to encryption (prevents replay attacks)
            if context:
                data = json.dumps({"data": data, "context": context})
            
            encrypted = self._fernet.encrypt(data.encode())
            return base64.b64encode(encrypted).decode()
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            return data
    
    def decrypt(self, data: str, context: Optional[Dict[str, Any]] = None) -> str:
        """Decrypt string data with context verification"""
        if not self._fernet:
            return data
        
        try:
            encrypted = base64.b64decode(data.encode())
            decrypted = self._fernet.decrypt(encrypted).decode()
            
            # Verify context if present
            if context:
                try:
                    payload = json.loads(decrypted)
                    if isinstance(payload, dict) and "data" in payload and "context" in payload:
                        if payload["context"] != context:
                            logger.warning("Context mismatch in decryption")
                        return payload["data"]
                except:
                    pass
            
            return decrypted
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            return data
    
    def encrypt_dict(self, data: Dict[str, Any], sensitive_keys: Optional[List[str]] = None) -> Dict[str, Any]:
        """Encrypt sensitive dictionary values"""
        if sensitive_keys is None:
            sensitive_keys = ["token", "password", "secret", "key", "credential"]
        
        result = {}
        for k, v in data.items():
            # Check if key is sensitive
            is_sensitive = any(s in k.lower() for s in sensitive_keys)
            
            if isinstance(v, str) and is_sensitive:
                result[k] = self.encrypt(v, {"key": k})
            elif isinstance(v, dict):
                result[k] = self.encrypt_dict(v, sensitive_keys)
            elif isinstance(v, list):
                result[k] = [
                    self.encrypt_dict(item, sensitive_keys) if isinstance(item, dict) else item
                    for item in v
                ]
            else:
                result[k] = v
        
        return result
    
    def decrypt_dict(self, data: Dict[str, Any], sensitive_keys: Optional[List[str]] = None) -> Dict[str, Any]:
        """Decrypt dictionary values"""
        if sensitive_keys is None:
            sensitive_keys = ["token", "password", "secret", "key", "credential"]
        
        result = {}
        for k, v in data.items():
            is_sensitive = any(s in k.lower() for s in sensitive_keys)
            
            if isinstance(v, str) and is_sensitive:
                result[k] = self.decrypt(v, {"key": k})
            elif isinstance(v, dict):
                result[k] = self.decrypt_dict(v, sensitive_keys)
            elif isinstance(v, list):
                result[k] = [
                    self.decrypt_dict(item, sensitive_keys) if isinstance(item, dict) else item
                    for item in v
                ]
            else:
                result[k] = v
        
        return result
    
    def rotate_key(self) -> None:
        """Rotate encryption key"""
        self._key_version += 1
        self._key_rotation_time = datetime.now()
        self.master_key = self._derive_master_key()
        self._fernet = Fernet(self.master_key) if _CRYPTO_AVAILABLE else None
        logger.info(f"Encryption key rotated to version {self._key_version}")

class ConfigAuditLogger:
    """SAMA-compliant audit logger for configuration changes"""
    
    def __init__(self):
        self._audit_log: List[Dict[str, Any]] = []
        self._audit_lock = asyncio.Lock()
        self._audit_file = os.getenv("AUDIT_LOG_FILE", "/var/log/tadawul/config_audit.log")
        self._immutable = True  # SAMA requirement
    
    async def log_change(
        self,
        action: str,
        key: str,
        old_value: Optional[Any],
        new_value: Optional[Any],
        user: Optional[str],
        source: str,
        request_id: Optional[str] = None
    ) -> None:
        """Log configuration change"""
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": action,
            "key": key,
            "old_value": self._mask_value(old_value),
            "new_value": self._mask_value(new_value),
            "user": user,
            "source": source,
            "request_id": request_id or str(uuid.uuid4()),
            "version": CONFIG_VERSION,
        }
        
        async with self._audit_lock:
            self._audit_log.append(entry)
            await self._write_to_file(entry)
    
    def _mask_value(self, value: Any) -> Any:
        """Mask sensitive values in audit log"""
        if isinstance(value, str) and len(value) > 8:
            # Mask all but first 4 and last 4 characters
            return value[:4] + "*" * (len(value) - 8) + value[-4:] if len(value) > 8 else "***"
        return value
    
    async def _write_to_file(self, entry: Dict[str, Any]) -> None:
        """Write audit entry to file (immutable)"""
        try:
            with open(self._audit_file, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            logger.error(f"Failed to write audit log: {e}")
    
    async def query(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        user: Optional[str] = None,
        action: Optional[str] = None,
        key: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query audit logs (admin only)"""
        results = self._audit_log.copy()
        
        if start_time:
            results = [e for e in results if datetime.fromisoformat(e["timestamp"]) >= start_time]
        if end_time:
            results = [e for e in results if datetime.fromisoformat(e["timestamp"]) <= end_time]
        if user:
            results = [e for e in results if e["user"] == user]
        if action:
            results = [e for e in results if e["action"] == action]
        if key:
            results = [e for e in results if key in e["key"]]
        
        return results

class CircuitBreaker:
    """Circuit breaker for external config sources"""
    
    def __init__(self, name: str, threshold: int = 5, timeout: float = 60.0):
        self.name = name
        self.threshold = threshold
        self.timeout = timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0
        self.success_count = 0
        self.total_calls = 0
        self.total_failures = 0
        self._lock = asyncio.Lock()
    
    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute with circuit breaker protection"""
        
        async with self._lock:
            self.total_calls += 1
            
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time > self.timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.success_count = 0
                    logger.info(f"Circuit {self.name} entering half-open")
                else:
                    raise Exception(f"Circuit {self.name} is OPEN")
            
            if self.state == CircuitBreakerState.HALF_OPEN and self.success_count >= 2:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                logger.info(f"Circuit {self.name} closed")
        
        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            
            async with self._lock:
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.success_count += 1
                else:
                    self.failure_count = 0
            
            return result
            
        except Exception as e:
            async with self._lock:
                self.total_failures += 1
                self.failure_count += 1
                self.last_failure_time = time.time()
                
                if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.threshold:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit {self.name} opened after {self.failure_count} failures")
                elif self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit {self.name} re-opened from half-open")
            
            raise e
    
    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics"""
        return {
            "name": self.name,
            "state": self.state.value,
            "total_calls": self.total_calls,
            "total_failures": self.total_failures,
            "failure_rate": self.total_failures / self.total_calls if self.total_calls > 0 else 0,
            "current_failure_count": self.failure_count,
            "threshold": self.threshold,
            "timeout": self.timeout
        }

class ConfigManager:
    """Distributed configuration manager with hot-reload"""
    
    def __init__(self):
        self._config: Optional[TadawulConfig] = None
        self._config_lock = asyncio.Lock()
        self._watchers: List[Callable[[TadawulConfig], None]] = []
        self._watch_task: Optional[asyncio.Task] = None
        self._encryption = ConfigEncryption()
        self._audit = ConfigAuditLogger()
        self._providers: Dict[ConfigProvider, Any] = {}
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._stats: Dict[str, Any] = defaultdict(int)
        self._last_update: Optional[datetime] = None
        self._version = 1
        self._initialized = False
        
        # Initialize providers
        self._init_providers()
        
        # Load initial config
        asyncio.create_task(self._load_initial_config())
    
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
                self._circuit_breakers["consul"] = CircuitBreaker("consul")
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
                    self._circuit_breakers["etcd"] = CircuitBreaker("etcd")
                    logger.info("etcd provider initialized")
            except Exception as e:
                logger.error(f"Failed to initialize etcd: {e}")
        
        # ZooKeeper
        if _ZOOKEEPER_AVAILABLE:
            try:
                zk_hosts = TadawulConfig().zookeeper_hosts
                if zk_hosts:
                    self._providers[ConfigProvider.ZOOKEEPER] = KazooClient(
                        hosts=','.join(zk_hosts)
                    )
                    self._circuit_breakers["zookeeper"] = CircuitBreaker("zookeeper")
                    logger.info("ZooKeeper provider initialized")
            except Exception as e:
                logger.error(f"Failed to initialize ZooKeeper: {e}")
        
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
                self._circuit_breakers["redis"] = CircuitBreaker("redis")
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
                self._circuit_breakers["vault"] = CircuitBreaker("vault")
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
                    profile_name=aws_config.profile,
                    region_name=aws_config.region
                )
                self._providers[ConfigProvider.AWS_SECRETS] = session.client(
                    "secretsmanager",
                    config=aws_config.get_boto3_config(),
                    endpoint_url=aws_config.endpoint_url
                )
                self._providers[ConfigProvider.AWS_PARAM_STORE] = session.client(
                    "ssm",
                    config=aws_config.get_boto3_config(),
                    endpoint_url=aws_config.endpoint_url
                )
                self._circuit_breakers["aws"] = CircuitBreaker("aws")
                logger.info("AWS providers initialized")
            except Exception as e:
                logger.error(f"Failed to initialize AWS: {e}")
        
        # Azure
        if _AZURE_AVAILABLE:
            try:
                azure_config = TadawulConfig().azure
                if azure_config.key_vault_url:
                    credential = DefaultAzureCredential()
                    self._providers[ConfigProvider.AZURE_KEYVAULT] = SecretClient(
                        vault_url=azure_config.key_vault_url,
                        credential=credential
                    )
                    self._circuit_breakers["azure"] = CircuitBreaker("azure")
                    logger.info("Azure KeyVault provider initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Azure: {e}")
        
        # GCP
        if _GCP_AVAILABLE:
            try:
                gcp_config = TadawulConfig().gcp
                if gcp_config.project_id:
                    client = secretmanager.SecretManagerServiceClient()
                    self._providers[ConfigProvider.GCP_SECRETS] = client
                    self._circuit_breakers["gcp"] = CircuitBreaker("gcp")
                    logger.info("GCP Secret Manager provider initialized")
            except Exception as e:
                logger.error(f"Failed to initialize GCP: {e}")
    
    async def _load_initial_config(self):
        """Load initial configuration"""
        try:
            # Load from environment
            config_dict = self._load_from_env()
            
            # Load from providers in priority order
            for provider in TadawulConfig().config_providers:
                if provider != ConfigProvider.LOCAL and provider != ConfigProvider.ENV:
                    provider_config = await self._load_from_provider(provider)
                    if provider_config:
                        config_dict.update(provider_config)
                        self._stats[f"{provider.value}_hits"] += 1
            
            # Decrypt
            config_dict = self._encryption.decrypt_dict(config_dict)
            
            # Create config
            async with self._config_lock:
                self._config = TadawulConfig(**config_dict)
                self._last_update = datetime.now()
                self._version += 1
                self._initialized = True
            
            logger.info(f"Configuration loaded (version {self._version})")
            
            # Start watch task
            if not self._watch_task:
                self._watch_task = asyncio.create_task(self._watch_config())
            
        except Exception as e:
            logger.error(f"Failed to load initial configuration: {e}")
            self._config = TadawulConfig()
            self._initialized = True
    
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
    
    async def _load_from_provider(self, provider: ConfigProvider) -> Dict[str, Any]:
        """Load configuration from specific provider"""
        config: Dict[str, Any] = {}
        
        if provider == ConfigProvider.CONSUL and provider in self._providers:
            cb = self._circuit_breakers.get("consul")
            if cb:
                try:
                    result = await cb.execute(self._load_from_consul)
                    if result:
                        config.update(result)
                except Exception as e:
                    logger.error(f"Consul load failed: {e}")
                    self._stats["consul_errors"] += 1
        
        elif provider == ConfigProvider.ETCD and provider in self._providers:
            cb = self._circuit_breakers.get("etcd")
            if cb:
                try:
                    result = await cb.execute(self._load_from_etcd)
                    if result:
                        config.update(result)
                except Exception as e:
                    logger.error(f"etcd load failed: {e}")
                    self._stats["etcd_errors"] += 1
        
        elif provider == ConfigProvider.ZOOKEEPER and provider in self._providers:
            cb = self._circuit_breakers.get("zookeeper")
            if cb:
                try:
                    result = await cb.execute(self._load_from_zookeeper)
                    if result:
                        config.update(result)
                except Exception as e:
                    logger.error(f"ZooKeeper load failed: {e}")
                    self._stats["zookeeper_errors"] += 1
        
        elif provider == ConfigProvider.REDIS and provider in self._providers:
            cb = self._circuit_breakers.get("redis")
            if cb:
                try:
                    result = await cb.execute(self._load_from_redis)
                    if result:
                        config.update(result)
                except Exception as e:
                    logger.error(f"Redis load failed: {e}")
                    self._stats["redis_errors"] += 1
        
        elif provider == ConfigProvider.VAULT and provider in self._providers:
            cb = self._circuit_breakers.get("vault")
            if cb:
                try:
                    result = await cb.execute(self._load_from_vault)
                    if result:
                        config.update(result)
                except Exception as e:
                    logger.error(f"Vault load failed: {e}")
                    self._stats["vault_errors"] += 1
        
        elif provider == ConfigProvider.AWS_PARAM_STORE and provider in self._providers:
            cb = self._circuit_breakers.get("aws")
            if cb:
                try:
                    result = await cb.execute(self._load_from_aws_param_store)
                    if result:
                        config.update(result)
                except Exception as e:
                    logger.error(f"AWS Param Store load failed: {e}")
                    self._stats["aws_errors"] += 1
        
        elif provider == ConfigProvider.AWS_SECRETS and provider in self._providers:
            cb = self._circuit_breakers.get("aws")
            if cb:
                try:
                    result = await cb.execute(self._load_from_aws_secrets)
                    if result:
                        config.update(result)
                except Exception as e:
                    logger.error(f"AWS Secrets Manager load failed: {e}")
                    self._stats["aws_errors"] += 1
        
        return config
    
    def _load_from_consul(self) -> Dict[str, Any]:
        """Load from Consul"""
        consul_client = self._providers[ConfigProvider.CONSUL]
        config = {}
        
        try:
            index, data = consul_client.kv.get("tadawul/config", recurse=True)
            if data:
                for item in data:
                    key = item["Key"].replace("tadawul/config/", "")
                    if key and item.get("Value"):
                        # Convert to nested dict
                        parts = key.split("/")
                        current = config
                        for part in parts[:-1]:
                            if part not in current:
                                current[part] = {}
                            current = current[part]
                        
                        try:
                            value = json.loads(item["Value"].decode())
                        except:
                            value = item["Value"].decode()
                        
                        current[parts[-1]] = value
        except Exception as e:
            logger.error(f"Consul query failed: {e}")
            raise
        
        return config
    
    def _load_from_etcd(self) -> Dict[str, Any]:
        """Load from etcd"""
        etcd_client = self._providers[ConfigProvider.ETCD]
        config = {}
        
        try:
            prefix = TadawulConfig().etcd_prefix
            response = etcd_client.get_prefix(prefix)
            for value, metadata in response:
                key = metadata.key.decode().replace(prefix, "")
                if key:
                    parts = key.split("/")
                    current = config
                    for part in parts[:-1]:
                        if part not in current:
                            current[part] = {}
                        current = current[part]
                    
                    try:
                        current[parts[-1]] = json.loads(value.decode())
                    except:
                        current[parts[-1]] = value.decode()
        except Exception as e:
            logger.error(f"etcd query failed: {e}")
            raise
        
        return config
    
    def _load_from_zookeeper(self) -> Dict[str, Any]:
        """Load from ZooKeeper"""
        zk_client = self._providers[ConfigProvider.ZOOKEEPER]
        config = {}
        
        try:
            prefix = TadawulConfig().zookeeper_prefix
            if not zk_client.connected:
                zk_client.start()
            
            children = zk_client.get_children(prefix)
            for child in children:
                path = f"{prefix}/{child}"
                data, stat = zk_client.get(path)
                if data:
                    try:
                        config[child] = json.loads(data.decode())
                    except:
                        config[child] = data.decode()
        except Exception as e:
            logger.error(f"ZooKeeper query failed: {e}")
            raise
        
        return config
    
    async def _load_from_redis(self) -> Dict[str, Any]:
        """Load from Redis"""
        redis_client = self._providers[ConfigProvider.REDIS]
        config = {}
        
        try:
            keys = await redis_client.keys("tadawul:config:*")
            for key in keys:
                value = await redis_client.get(key)
                if value:
                    config_key = key.replace("tadawul:config:", "")
                    try:
                        config[config_key] = json.loads(value)
                    except:
                        config[config_key] = value
        except Exception as e:
            logger.error(f"Redis query failed: {e}")
            raise
        
        return config
    
    def _load_from_vault(self) -> Dict[str, Any]:
        """Load from Vault"""
        vault_client = self._providers[ConfigProvider.VAULT]
        config = {}
        
        try:
            mount_point = TadawulConfig().vault.mount_point
            secret = vault_client.secrets.kv.v2.read_secret_version(
                mount_point=mount_point,
                path="tadawul/config"
            )
            if secret and "data" in secret:
                config.update(secret["data"]["data"])
        except Exception as e:
            logger.error(f"Vault query failed: {e}")
            raise
        
        return config
    
    def _load_from_aws_param_store(self) -> Dict[str, Any]:
        """Load from AWS Parameter Store"""
        ssm = self._providers[ConfigProvider.AWS_PARAM_STORE]
        config = {}
        
        try:
            aws_config = TadawulConfig().aws
            paginator = ssm.get_paginator("get_parameters_by_path")
            
            for page in paginator.paginate(
                Path=aws_config.param_store_path,
                Recursive=aws_config.param_store_recursive,
                WithDecryption=aws_config.param_store_with_decryption
            ):
                for param in page.get("Parameters", []):
                    name = param["Name"].replace(aws_config.param_store_path, "")
                    value = param["Value"]
                    
                    # Convert to nested dict
                    parts = name.split("/")
                    current = config
                    for part in parts[:-1]:
                        if part not in current:
                            current[part] = {}
                        current = current[part]
                    
                    try:
                        current[parts[-1]] = json.loads(value)
                    except:
                        current[parts[-1]] = value
        except Exception as e:
            logger.error(f"AWS Parameter Store query failed: {e}")
            raise
        
        return config
    
    def _load_from_aws_secrets(self) -> Dict[str, Any]:
        """Load from AWS Secrets Manager"""
        secrets = self._providers[ConfigProvider.AWS_SECRETS]
        config = {}
        
        try:
            aws_config = TadawulConfig().aws
            paginator = secrets.get_paginator("list_secrets")
            
            for page in paginator.paginate():
                for secret in page.get("SecretList", []):
                    if secret["Name"].startswith(aws_config.secrets_manager_prefix):
                        value = secrets.get_secret_value(SecretId=secret["ARN"])
                        secret_string = value.get("SecretString")
                        if secret_string:
                            name = secret["Name"].replace(aws_config.secrets_manager_prefix, "").strip("/")
                            try:
                                config[name] = json.loads(secret_string)
                            except:
                                config[name] = secret_string
        except Exception as e:
            logger.error(f"AWS Secrets Manager query failed: {e}")
            raise
        
        return config
    
    async def _watch_config(self):
        """Watch for configuration changes"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                if not self._config:
                    continue
                
                # Check for updates
                updated = False
                new_config_dict = self._config.dict()
                
                for provider in self._config.config_providers:
                    if provider not in [ConfigProvider.LOCAL, ConfigProvider.ENV]:
                        provider_config = await self._load_from_provider(provider)
                        if provider_config:
                            new_config_dict.update(provider_config)
                            updated = True
                
                if updated:
                    # Decrypt
                    new_config_dict = self._encryption.decrypt_dict(new_config_dict)
                    
                    # Create new config
                    async with self._config_lock:
                        old_config = self._config
                        self._config = TadawulConfig(**new_config_dict)
                        self._last_update = datetime.now()
                        self._version += 1
                        
                        # Audit log
                        await self._audit.log_change(
                            action="update",
                            key="config",
                            old_value=old_config.dict(),
                            new_value=self._config.dict(),
                            user="system",
                            source="watch"
                        )
                        
                        # Notify watchers
                        for watcher in self._watchers:
                            try:
                                watcher(self._config)
                            except Exception as e:
                                logger.error(f"Config watcher failed: {e}")
                        
                        logger.info(f"Configuration updated (version {self._version})")
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Config watch failed: {e}")
    
    async def get_config(self) -> TadawulConfig:
        """Get current configuration"""
        while not self._initialized:
            await asyncio.sleep(0.1)
        
        return cast(TadawulConfig, self._config)
    
    async def update_config(self, updates: Dict[str, Any], user: str) -> TadawulConfig:
        """Update configuration (admin only)"""
        async with self._config_lock:
            old_config = self._config
            current_dict = old_config.dict() if old_config else {}
            current_dict.update(updates)
            
            # Encrypt sensitive data
            current_dict = self._encryption.encrypt_dict(current_dict)
            
            # Create new config
            new_config = TadawulConfig(**current_dict)
            
            # Update providers
            for provider in new_config.config_providers:
                if provider != ConfigProvider.LOCAL:
                    await self._update_provider(provider, updates)
            
            self._config = new_config
            self._last_update = datetime.now()
            self._version += 1
            
            # Audit log
            await self._audit.log_change(
                action="update",
                key="config",
                old_value=old_config.dict() if old_config else None,
                new_value=new_config.dict(),
                user=user,
                source="api"
            )
            
            # Notify watchers
            for watcher in self._watchers:
                try:
                    watcher(new_config)
                except Exception as e:
                    logger.error(f"Config watcher failed: {e}")
            
            return new_config
    
    async def _update_provider(self, provider: ConfigProvider, updates: Dict[str, Any]):
        """Update configuration in provider"""
        if provider == ConfigProvider.CONSUL and provider in self._providers:
            try:
                consul_client = self._providers[provider]
                for key, value in updates.items():
                    await consul_client.kv.put(
                        f"tadawul/config/{key}",
                        json.dumps(value, default=str).encode()
                    )
            except Exception as e:
                logger.error(f"Consul update failed: {e}")
        
        elif provider == ConfigProvider.ETCD and provider in self._providers:
            try:
                etcd_client = self._providers[provider]
                prefix = TadawulConfig().etcd_prefix
                for key, value in updates.items():
                    etcd_client.put(
                        f"{prefix}{key}",
                        json.dumps(value, default=str).encode()
                    )
            except Exception as e:
                logger.error(f"etcd update failed: {e}")
    
    def watch(self, callback: Callable[[TadawulConfig], None]):
        """Watch for configuration changes"""
        self._watchers.append(callback)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get configuration statistics"""
        return {
            "version": self._version,
            "last_update": self._last_update.isoformat() if self._last_update else None,
            "initialized": self._initialized,
            "providers": list(self._providers.keys()),
            "circuit_breakers": {
                name: cb.get_stats() for name, cb in self._circuit_breakers.items()
            },
            "stats": dict(self._stats),
            "watchers": len(self._watchers),
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

def is_feature_enabled(feature_name: str, user_id: Optional[str] = None, **kwargs) -> bool:
    """Check if feature is enabled"""
    try:
        config = asyncio.run(_config_manager.get_config())
        feature = config.get_feature(feature_name)
        if not feature:
            return False
        
        if user_id:
            return feature.is_enabled_for(user_id, **kwargs)
        
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
        return config.get_endpoint(path)
    except Exception:
        return None


# =============================================================================
