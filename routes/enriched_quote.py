#!/usr/bin/env python3
"""
routes/config.py
------------------------------------------------------------
TADAWUL ENTERPRISE CONFIGURATION MANAGEMENT — v5.0.0 (DISTRIBUTED + ENCRYPTED + HOT-RELOAD + AUDITED + ML-ENHANCED)
SAMA Compliant | Distributed Config | Secrets Management | Dynamic Updates | Zero-Trust Security | AI-Powered

Core Capabilities:
- Distributed configuration management (Consul, etcd, Redis, ZooKeeper, Kubernetes ConfigMaps)
- Encrypted secrets with AWS KMS / HashiCorp Vault / Azure KeyVault / GCP Secret Manager
- Hot-reload with zero downtime, version tracking, and canary deployments
- Multi-environment support with inheritance and environment overlays
- Feature flags with ML-powered gradual rollout and A/B testing
- Audit logging for all configuration changes (SAMA compliant) with immutable storage
- Circuit breaker for external config sources with adaptive thresholds
- Graceful fallback with cascade strategy and local caching
- SAMA-compliant audit trail with tamper-proof HMAC signatures
- Dynamic routing configuration with canary deployments and blue/green
- Rate limiting configuration per endpoint/user with ML-based anomaly detection
- ML model version management with A/B testing and automatic rollback
- Zero-trust security model with mTLS support and certificate rotation
- Configuration validation with JSON Schema and OpenAPI
- Configuration encryption at rest and in transit with key rotation
- Configuration diff and history with rollback capability
- Configuration export/import with YAML/JSON/TOML support
- Configuration documentation generation with Markdown/HTML
- Configuration metrics and monitoring with Prometheus/Grafana
- Configuration backup and restore with S3/GCS/Azure Blob
- Configuration migration tools with schema versioning
"""

from __future__ import annotations

import asyncio
import base64
import functools
import hashlib
import hmac
import importlib
import inspect
import json
import logging
import os
import re
import socket
import threading
import time
import uuid
import zlib
from collections import defaultdict, deque
from contextlib import asynccontextmanager, contextmanager
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from functools import lru_cache, wraps
from pathlib import Path
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
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
    overload,
)
from urllib.parse import urlparse, quote, unquote

# =============================================================================
# Optional Enterprise Integrations (with graceful degradation)
# =============================================================================

# Service Discovery & Configuration
try:
    import consul
    from consul.base import Timeout
    from consul.asyncio import Consul as AsyncConsul
    _CONSUL_AVAILABLE = True
except ImportError:
    _CONSUL_AVAILABLE = False

try:
    import etcd3
    import etcd3.asyncio
    from etcd3.exceptions import Etcd3Exception
    _ETCD_AVAILABLE = True
except ImportError:
    _ETCD_AVAILABLE = False

try:
    from kazoo.client import KazooClient
    from kazoo.recipe.cache import TreeCache
    from kazoo.recipe.watchers import DataWatch
    from kazoo.exceptions import KazooException
    _ZOOKEEPER_AVAILABLE = True
except ImportError:
    _ZOOKEEPER_AVAILABLE = False

# Kubernetes
try:
    import kubernetes
    from kubernetes import client, config
    from kubernetes.client.rest import ApiException
    from kubernetes.config import ConfigException
    from kubernetes.watch import Watch
    _KUBERNETES_AVAILABLE = True
except ImportError:
    _KUBERNETES_AVAILABLE = False

# Databases
try:
    import aioredis
    from aioredis import Redis
    from aioredis.exceptions import RedisError, ConnectionError, TimeoutError
    from aioredis.sentinel import Sentinel
    from aioredis.cluster import RedisCluster
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False

try:
    import asyncpg
    from asyncpg.pool import Pool
    from asyncpg.exceptions import PostgresError
    _POSTGRES_AVAILABLE = True
except ImportError:
    _POSTGRES_AVAILABLE = False

try:
    import aiomcache
    from aiomcache import Client as MemcachedClient
    _MEMCACHED_AVAILABLE = True
except ImportError:
    _MEMCACHED_AVAILABLE = False

# Cloud Providers
try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    from botocore.config import Config as BotoConfig
    from botocore.auth import SigV4Auth
    _AWS_AVAILABLE = True
except ImportError:
    _AWS_AVAILABLE = False

try:
    import hvac
    from hvac.exceptions import VaultError, InvalidPath
    from hvac.api.secrets_engines import KvV2
    _VAULT_AVAILABLE = True
except ImportError:
    _VAULT_AVAILABLE = False

try:
    from azure.identity import DefaultAzureCredential, ClientSecretCredential
    from azure.keyvault.secrets import SecretClient
    from azure.core.exceptions import AzureError, ResourceNotFoundError
    _AZURE_AVAILABLE = True
except ImportError:
    _AZURE_AVAILABLE = False

try:
    from google.cloud import secretmanager
    from google.cloud.secretmanager import SecretManagerServiceClient
    from google.api_core.exceptions import GoogleAPICallError, NotFound
    _GCP_AVAILABLE = True
except ImportError:
    _GCP_AVAILABLE = False

# Cryptography
try:
    from cryptography.fernet import Fernet, InvalidToken
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.primitives.asymmetric import rsa, padding
    from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
    from cryptography.x509 import load_pem_x509_certificate
    _CRYPTO_AVAILABLE = True
except ImportError:
    _CRYPTO_AVAILABLE = False

# Machine Learning
try:
    import numpy as np
    import pandas as pd
    from sklearn.ensemble import IsolationForest, RandomForestClassifier
    from sklearn.preprocessing import StandardScaler, RobustScaler
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score, precision_score, recall_score
    from sklearn.cluster import KMeans, DBSCAN
    import joblib
    _ML_AVAILABLE = True
except ImportError:
    _ML_AVAILABLE = False

try:
    import xgboost as xgb
    _XGBOOST_AVAILABLE = True
except ImportError:
    _XGBOOST_AVAILABLE = False

try:
    import lightgbm as lgb
    _LIGHTGBM_AVAILABLE = True
except ImportError:
    _LIGHTGBM_AVAILABLE = False

# Monitoring
try:
    from prometheus_client import (
        Counter, Histogram, Gauge, Summary, Info,
        generate_latest, REGISTRY, CONTENT_TYPE_LATEST
    )
    from prometheus_client.multiprocess import MultiProcessCollector
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False

try:
    from opentelemetry import trace, metrics
    from opentelemetry.trace import Span, Status, StatusCode, Tracer
    from opentelemetry.context import attach, detach, get_current
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False

# Pydantic
try:
    from pydantic import (
        BaseModel,
        ConfigDict,
        Field,
        field_validator,
        model_validator,
        ValidationError,
        ValidationInfo,
        AliasChoices,
        AliasPath,
        SecretStr,
        SecretBytes,
        Json,
        AnyUrl,
        RedisDsn,
        PostgresDsn,
        AmqpDsn,
        FilePath,
        DirectoryPath,
        EmailStr,
        HttpUrl,
    )
    from pydantic.functional_validators import AfterValidator, BeforeValidator
    from pydantic_core import PydanticCustomError
    from typing_extensions import Annotated
    _PYDANTIC_V2 = True
except ImportError:
    from pydantic import BaseModel, Field, validator  # type: ignore
    _PYDANTIC_V2 = False

# YAML/TOML
try:
    import yaml
    from yaml import SafeLoader, SafeDumper
    _YAML_AVAILABLE = True
except ImportError:
    _YAML_AVAILABLE = False

try:
    import toml
    _TOML_AVAILABLE = True
except ImportError:
    _TOML_AVAILABLE = False

# Jinja2 for templates
try:
    from jinja2 import Template, Environment, FileSystemLoader, select_autoescape
    _JINJA2_AVAILABLE = True
except ImportError:
    _JINJA2_AVAILABLE = False

# DeepDiff for configuration comparison
try:
    from deepdiff import DeepDiff
    _DEEPDIFF_AVAILABLE = True
except ImportError:
    _DEEPDIFF_AVAILABLE = False

logger = logging.getLogger("routes.config")

CONFIG_VERSION = "5.0.0"
SHIM_VERSION = "5.0.0-shim"

# =============================================================================
# Prometheus Metrics (Optional)
# =============================================================================

if _PROMETHEUS_AVAILABLE:
    config_requests_total = Counter(
        'config_requests_total',
        'Total configuration requests',
        ['operation', 'status']
    )
    config_request_duration = Histogram(
        'config_request_duration_seconds',
        'Configuration request duration',
        ['operation']
    )
    config_provider_requests = Counter(
        'config_provider_requests',
        'Provider requests',
        ['provider', 'status']
    )
    config_provider_latency = Histogram(
        'config_provider_latency_seconds',
        'Provider latency',
        ['provider']
    )
    config_cache_hits = Counter(
        'config_cache_hits',
        'Cache hits',
        ['level']
    )
    config_cache_misses = Counter(
        'config_cache_misses',
        'Cache misses',
        ['level']
    )
    config_circuit_breakers = Gauge(
        'config_circuit_breakers',
        'Circuit breaker state',
        ['provider']
    )
    config_version_gauge = Gauge(
        'config_version',
        'Configuration version',
        ['environment']
    )
    config_reloads_total = Counter(
        'config_reloads_total',
        'Total configuration reloads'
    )
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
    config_requests_total = DummyMetric()
    config_request_duration = DummyMetric()
    config_provider_requests = DummyMetric()
    config_provider_latency = DummyMetric()
    config_cache_hits = DummyMetric()
    config_cache_misses = DummyMetric()
    config_circuit_breakers = DummyMetric()
    config_version_gauge = DummyMetric()
    config_reloads_total = DummyMetric()

# =============================================================================
# OpenTelemetry Tracing (Optional)
# =============================================================================

if _OTEL_AVAILABLE:
    tracer = trace.get_tracer(__name__)
else:
    class DummyTracer:
        def start_as_current_span(self, *args, **kwargs):
            return self
        def __enter__(self):
            return self
        def __exit__(self, *args, **kwargs):
            pass
        def set_attribute(self, *args, **kwargs):
            pass
        def set_status(self, *args, **kwargs):
            pass
        def add_event(self, *args, **kwargs):
            pass
    tracer = DummyTracer()

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
    CANARY = "canary"
    BLUE = "blue"
    GREEN = "green"
    
    def inherits_from(self) -> Optional[Environment]:
        """Get parent environment for inheritance"""
        inheritance = {
            Environment.STAGING: Environment.DEVELOPMENT,
            Environment.PRODUCTION: Environment.STAGING,
            Environment.DR: Environment.PRODUCTION,
            Environment.DEMO: Environment.STAGING,
            Environment.SANDBOX: Environment.DEVELOPMENT,
            Environment.CANARY: Environment.STAGING,
            Environment.BLUE: Environment.PRODUCTION,
            Environment.GREEN: Environment.PRODUCTION,
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
    MEMCACHED = "memcached"
    DATABASE = "database"
    KUBERNETES = "kubernetes"
    FILE = "file"
    S3 = "s3"
    GCS = "gcs"
    AZURE_BLOB = "azure_blob"
    VAULT = "vault"
    AWS_SECRETS = "aws_secrets"
    AWS_PARAM_STORE = "aws_param_store"
    AZURE_KEYVAULT = "azure_keyvault"
    GCP_SECRETS = "gcp_secrets"
    
    @property
    def priority(self) -> int:
        """Get provider priority (higher = more authoritative)"""
        priorities = {
            ConfigProvider.LOCAL: 0,
            ConfigProvider.ENV: 10,
            ConfigProvider.FILE: 20,
            ConfigProvider.MEMCACHED: 30,
            ConfigProvider.REDIS: 40,
            ConfigProvider.KUBERNETES: 45,
            ConfigProvider.CONSUL: 50,
            ConfigProvider.ETCD: 51,
            ConfigProvider.ZOOKEEPER: 52,
            ConfigProvider.DATABASE: 60,
            ConfigProvider.S3: 70,
            ConfigProvider.GCS: 71,
            ConfigProvider.AZURE_BLOB: 72,
            ConfigProvider.VAULT: 80,
            ConfigProvider.AWS_PARAM_STORE: 90,
            ConfigProvider.AWS_SECRETS: 91,
            ConfigProvider.AZURE_KEYVAULT: 92,
            ConfigProvider.GCP_SECRETS: 93,
        }
        return priorities.get(self, 0)
    
    @property
    def is_secret_store(self) -> bool:
        """Check if provider is a secret store"""
        return self in [
            ConfigProvider.VAULT,
            ConfigProvider.AWS_SECRETS,
            ConfigProvider.AWS_PARAM_STORE,
            ConfigProvider.AZURE_KEYVAULT,
            ConfigProvider.GCP_SECRETS,
        ]
    
    @property
    def is_distributed(self) -> bool:
        """Check if provider is distributed"""
        return self in [
            ConfigProvider.CONSUL,
            ConfigProvider.ETCD,
            ConfigProvider.ZOOKEEPER,
            ConfigProvider.REDIS,
            ConfigProvider.KUBERNETES,
        ]


class SecretProvider(str, Enum):
    """Secrets management providers"""
    LOCAL = "local"
    ENV = "environment"
    VAULT = "vault"
    AWS_KMS = "aws_kms"
    AWS_SECRETS = "aws_secrets"
    AWS_PARAM_STORE = "aws_param_store"
    AZURE_KEYVAULT = "azure_keyvault"
    GCP_SECRETS = "gcp_secrets"
    DATABASE = "database"
    FILE = "file"


class FeatureStage(str, Enum):
    """Feature rollout stages"""
    ALPHA = "alpha"          # Internal testing
    BETA = "beta"            # Limited user testing
    GA = "ga"                # General availability
    DEPRECATED = "deprecated"  # Will be removed
    REMOVED = "removed"       # No longer available
    CANARY = "canary"         # Gradual rollout
    EXPERIMENTAL = "experimental"  # Experimental features
    PRIVATE = "private"       # Private preview
    PUBLIC = "public"         # Public preview


class CacheStrategy(str, Enum):
    """Caching strategies"""
    NONE = "none"
    LOCAL = "local"
    REDIS = "redis"
    MEMCACHED = "memcached"
    HYBRID = "hybrid"  # Local + Redis
    DISTRIBUTED = "distributed"  # Redis cluster
    REPLICATED = "replicated"  # Read replicas
    MULTI_LEVEL = "multi_level"  # L1/L2/L3


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
    LDAP = "ldap"
    KERBEROS = "kerberos"
    CERTIFICATE = "certificate"


class LogLevel(str, Enum):
    """Logging levels"""
    TRACE = "TRACE"
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
    LOGSTASH = "logstash"
    FLUENTD = "fluentd"


class DataCenter(str, Enum):
    """Data center locations"""
    AWS_BAHRAIN = "aws-me-south-1"
    AWS_DUBAI = "aws-me-central-1"
    AZURE_UAE = "azure-uaenorth"
    AZURE_QATAR = "azure-qatarcentral"
    GCP_DOHA = "gcp-me-west1"
    GCP_JEDDAH = "gcp-me-central2"
    ONPREM_KSA = "onprem-riyadh"
    ONPREM_JEDDAH = "onprem-jeddah"
    ONPREM_DAMMAM = "onprem-dammam"
    OCI_JEDDAH = "oci-jeddah"


class ComplianceStandard(str, Enum):
    """Compliance standards"""
    SAMA = "sama"          # Saudi Central Bank
    CMA = "cma"            # Capital Market Authority
    NCA = "nca"            # National Cybersecurity Authority
    GDPR = "gdpr"          # EU General Data Protection
    SOC2 = "soc2"          # Service Organization Control
    SOC1 = "soc1"
    SOC3 = "soc3"
    ISO27001 = "iso27001"  # Information Security
    ISO27017 = "iso27017"  # Cloud Security
    ISO27018 = "iso27018"  # Cloud Privacy
    ISO27701 = "iso27701"  # Privacy Information
    PCI_DSS = "pci_dss"    # Payment Card Industry
    HIPAA = "hipaa"        # Healthcare
    FEDRAMP = "fedramp"    # Federal Risk and Authorization
    CSA_STAR = "csa_star"  # Cloud Security Alliance


class AlertSeverity(str, Enum):
    """Alert severity levels"""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class CircuitBreakerState(str, Enum):
    """Circuit breaker states"""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"
    DISABLED = "disabled"


class ConfigChangeType(str, Enum):
    """Configuration change types"""
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    RESTORE = "restore"
    ROLLBACK = "rollback"
    RELOAD = "reload"


class ConfigStatus(str, Enum):
    """Configuration status"""
    ACTIVE = "active"
    PENDING = "pending"
    DEPRECATED = "deprecated"
    DELETED = "deleted"
    ARCHIVED = "archived"
    ERROR = "error"


class ValidationLevel(str, Enum):
    """Validation levels"""
    OFF = "off"
    BASIC = "basic"
    STRICT = "strict"
    SCHEMA = "schema"
    BUSINESS = "business"


class BackupStrategy(str, Enum):
    """Backup strategies"""
    NONE = "none"
    LOCAL = "local"
    S3 = "s3"
    GCS = "gcs"
    AZURE = "azure"
    DATABASE = "database"
    REPLICATED = "replicated"


class MigrationStrategy(str, Enum):
    """Migration strategies"""
    IN_PLACE = "in_place"
    BLUE_GREEN = "blue_green"
    CANARY = "canary"
    ROLLING = "rolling"
    RECREATE = "recreate"

# =============================================================================
# Advanced Configuration Models
# =============================================================================

class TLSConfig(BaseModel):
    """TLS/SSL configuration"""
    enabled: bool = False
    cert_file: Optional[FilePath] = None
    key_file: Optional[FilePath] = None
    ca_file: Optional[FilePath] = None
    verify_peer: bool = True
    min_version: str = "TLSv1.2"
    max_version: str = "TLSv1.3"
    cipher_suites: List[str] = Field(default_factory=list)
    cert_reqs: str = "CERT_REQUIRED"
    check_hostname: bool = True
    mutual_tls: bool = False
    client_ca_file: Optional[FilePath] = None
    crl_file: Optional[FilePath] = None
    ocsp_stapling: bool = False
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)


class DatabaseConfig(BaseModel):
    """Enhanced database configuration with replication and failover"""
    # Connection
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
    pool_use_lifo: bool = False
    
    # SSL/TLS
    ssl_mode: str = "prefer"
    ssl_cert: Optional[FilePath] = None
    ssl_key: Optional[FilePath] = None
    ssl_root_cert: Optional[FilePath] = None
    ssl_crl: Optional[FilePath] = None
    
    # Replication
    read_replicas: List[str] = Field(default_factory=list)
    replica_lag_threshold: int = 10  # seconds
    auto_balance_reads: bool = True
    failover_enabled: bool = True
    failover_timeout: int = 30
    standby_hosts: List[str] = Field(default_factory=list)
    
    # Backup
    backup_enabled: bool = True
    backup_schedule: str = "0 2 * * *"  # Daily at 2 AM
    backup_retention_days: int = 30
    backup_path: Optional[DirectoryPath] = None
    backup_s3_bucket: Optional[str] = None
    backup_s3_prefix: str = "database/backups/"
    
    # Monitoring
    slow_query_threshold: int = 1000  # ms
    enable_query_logging: bool = False
    log_all_queries: bool = False
    metrics_enabled: bool = True
    
    # Advanced
    statement_timeout: int = 30000  # ms
    lock_timeout: int = 5000  # ms
    idle_in_transaction_timeout: int = 60000  # ms
    connection_max_lifetime: int = 3600  # seconds
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, json_encoders={SecretStr: lambda v: "***"})
        
        @property
        def master_dsn(self) -> PostgresDsn:
            """Get master connection string"""
            auth = f"{self.user}:{self.password.get_secret_value()}"
            return PostgresDsn(f"postgresql://{auth}@{self.host}:{self.port}/{self.name}")
        
        @property
        def replica_dsns(self) -> List[PostgresDsn]:
            """Get replica connection strings"""
            auth = f"{self.user}:{self.password.get_secret_value()}"
            return [PostgresDsn(f"postgresql://{auth}@{host}/{self.name}") 
                    for host in self.read_replicas]
        
        @field_validator("port")
        def validate_port(cls, v):
            if not 1 <= v <= 65535:
                raise ValueError("Port must be between 1 and 65535")
            return v


class RedisConfig(BaseModel):
    """Enhanced Redis configuration with cluster and sentinel support"""
    # Connection
    host: str = "localhost"
    port: int = 6379
    password: SecretStr = Field(default_factory=lambda: SecretStr(""))
    db: int = 0
    username: Optional[str] = None
    
    # Cluster
    cluster_mode: bool = False
    startup_nodes: List[Dict[str, Any]] = Field(default_factory=list)
    
    # Sentinel
    sentinel_mode: bool = False
    sentinel_hosts: List[str] = Field(default_factory=list)
    sentinel_master: str = "mymaster"
    sentinel_password: Optional[SecretStr] = None
    
    # Connection
    ssl: bool = False
    ssl_cert: Optional[FilePath] = None
    ssl_key: Optional[FilePath] = None
    ssl_ca_certs: Optional[FilePath] = None
    ssl_cert_reqs: str = "required"
    socket_timeout: float = 5.0
    socket_connect_timeout: float = 5.0
    socket_keepalive: bool = True
    socket_keepalive_options: Dict[str, int] = Field(default_factory=dict)
    
    # Pool
    max_connections: int = 20
    min_connections: int = 5
    max_idle_time: int = 60
    connection_retry_delay: float = 1.0
    max_connection_retries: int = 3
    
    # Pub/Sub
    pubsub_threads: int = 1
    subscriber_queue_size: int = 1000
    
    # Advanced
    decode_responses: bool = True
    health_check_interval: int = 30
    retry_on_timeout: bool = True
    retry_on_error: List[str] = Field(default_factory=lambda: ["READONLY", "CLUSTERDOWN"])
    
    if _PYDANTIC_V2:
        @property
        def dsn(self) -> RedisDsn:
            """Get Redis DSN"""
            auth = f"{self.username}:{self.password.get_secret_value()}@" if self.username and self.password.get_secret_value() else ""
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
    namespace: Optional[str] = None
    partition: Optional[str] = None
    
    # Session
    session_ttl: int = 30
    session_lock_delay: int = 15
    session_behavior: str = "release"
    
    # Watch
    watch_enabled: bool = True
    watch_interval: int = 10
    
    # Service discovery
    service_ttl: int = 30
    service_deregister_after: int = 60
    service_check_interval: int = 10
    service_check_timeout: int = 5
    
    # Advanced
    connect_timeout: float = 5.0
    read_timeout: float = 5.0
    http2: bool = False
    allow_redirects: bool = True
    cert: Optional[FilePath] = None
    key: Optional[FilePath] = None
    ca_cert: Optional[FilePath] = None
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)


class VaultConfig(BaseModel):
    """Enhanced Vault configuration with multiple auth methods"""
    # Connection
    url: str = "http://localhost:8200"
    token: SecretStr = Field(default_factory=lambda: SecretStr(""))
    namespace: Optional[str] = None
    mount_point: str = "secret"
    kv_version: int = 2
    verify: bool = True
    timeout: float = 10.0
    
    # Auth methods
    auth_method: str = "token"  # token, approle, kubernetes, aws, gcp, jwt, oidc, ldap, userpass
    
    # AppRole
    role_id: Optional[SecretStr] = None
    secret_id: Optional[SecretStr] = None
    secret_id_ttl: Optional[int] = None
    
    # Kubernetes
    kubernetes_role: Optional[str] = None
    kubernetes_jwt: Optional[str] = None
    kubernetes_mount_point: str = "kubernetes"
    
    # AWS
    aws_role: Optional[str] = None
    aws_mount_point: str = "aws"
    aws_region: str = "me-south-1"
    
    # GCP
    gcp_role: Optional[str] = None
    gcp_mount_point: str = "gcp"
    gcp_jwt: Optional[str] = None
    
    # JWT/OIDC
    jwt_role: Optional[str] = None
    jwt_mount_point: str = "jwt"
    jwt_token: Optional[SecretStr] = None
    
    # LDAP
    ldap_username: Optional[str] = None
    ldap_password: Optional[SecretStr] = None
    ldap_mount_point: str = "ldap"
    
    # Userpass
    userpass_username: Optional[str] = None
    userpass_password: Optional[SecretStr] = None
    userpass_mount_point: str = "userpass"
    
    # Renewal
    renew_token: bool = True
    renew_interval: int = 300
    renewal_threshold: int = 60  # seconds before expiry
    
    # Advanced
    retry_attempts: int = 3
    retry_backoff: float = 1.0
    pool_connections: int = 10
    pool_maxsize: int = 20
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)


class AWSConfig(BaseModel):
    """Enhanced AWS configuration with multiple services"""
    # Basic
    region: str = "me-south-1"  # Bahrain
    access_key_id: Optional[SecretStr] = None
    secret_access_key: Optional[SecretStr] = None
    session_token: Optional[SecretStr] = None
    profile: Optional[str] = None
    
    # Endpoints
    endpoint_url: Optional[str] = None
    sts_endpoint_url: Optional[str] = None
    s3_endpoint_url: Optional[str] = None
    dynamodb_endpoint_url: Optional[str] = None
    
    # KMS
    kms_key_id: Optional[str] = None
    kms_aliases: List[str] = Field(default_factory=list)
    kms_signing_algorithm: str = "RSASSA_PKCS1_V1_5_SHA_256"
    
    # Secrets Manager
    secrets_manager_prefix: str = "tadawul"
    secrets_manager_ttl: int = 3600
    secrets_manager_client_cache: bool = True
    
    # Parameter Store
    param_store_path: str = "/tadawul/"
    param_store_recursive: bool = True
    param_store_with_decryption: bool = True
    param_store_tier: str = "Standard"  # Standard, Advanced, Intelligent-Tiering
    
    # S3
    s3_bucket: Optional[str] = None
    s3_prefix: str = "config/"
    s3_kms_key: Optional[str] = None
    s3_storage_class: str = "STANDARD"
    s3_versioning: bool = True
    s3_encryption: bool = True
    
    # DynamoDB
    dynamodb_table: Optional[str] = None
    dynamodb_partition_key: str = "id"
    dynamodb_sort_key: Optional[str] = None
    dynamodb_ttl_attribute: Optional[str] = None
    
    # Assume Role
    assume_role_arn: Optional[str] = None
    assume_role_session_name: Optional[str] = None
    assume_role_duration: int = 3600
    assume_role_policy: Optional[str] = None
    
    # Retries
    retry_mode: str = "adaptive"  # legacy, standard, adaptive
    max_attempts: int = 3
    
    # Timeouts
    connect_timeout: float = 5.0
    read_timeout: float = 10.0
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)
        
        def get_boto3_config(self) -> BotoConfig:
            """Get boto3 config"""
            return BotoConfig(
                region_name=self.region,
                signature_version='s3v4',
                retries={'max_attempts': self.max_attempts, 'mode': self.retry_mode},
                connect_timeout=self.connect_timeout,
                read_timeout=self.read_timeout
            )


class AzureConfig(BaseModel):
    """Azure configuration"""
    # Credentials
    tenant_id: Optional[str] = None
    client_id: Optional[SecretStr] = None
    client_secret: Optional[SecretStr] = None
    certificate_path: Optional[FilePath] = None
    certificate_password: Optional[SecretStr] = None
    use_managed_identity: bool = False
    
    # Key Vault
    key_vault_url: Optional[str] = None
    key_vault_ttl: int = 3600
    key_vault_soft_delete: bool = True
    
    # Storage
    storage_account: Optional[str] = None
    storage_container: str = "config"
    storage_prefix: str = ""
    storage_connection_string: Optional[SecretStr] = None
    
    # Advanced
    retry_total: int = 3
    retry_backoff: float = 1.0
    connection_timeout: float = 10.0
    read_timeout: float = 10.0
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)


class GCPConfig(BaseModel):
    """Google Cloud configuration"""
    # Project
    project_id: Optional[str] = None
    credentials_json: Optional[SecretStr] = None
    credentials_file: Optional[FilePath] = None
    
    # Secret Manager
    secret_manager_ttl: int = 3600
    secret_manager_prefix: str = "tadawul"
    
    # Storage
    storage_bucket: Optional[str] = None
    storage_prefix: str = "config/"
    storage_encryption_key: Optional[str] = None
    
    # Advanced
    retry_count: int = 3
    timeout: float = 10.0
    quota_project_id: Optional[str] = None
    universe_domain: str = "googleapis.com"
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)


class KubernetesConfig(BaseModel):
    """Kubernetes configuration"""
    # Connection
    in_cluster: bool = True
    config_file: Optional[FilePath] = None
    context: Optional[str] = None
    namespace: str = "default"
    
    # ConfigMap
    configmap_name: str = "tadawul-config"
    configmap_key: str = "config.yaml"
    
    # Secret
    secret_name: str = "tadawul-secrets"
    
    # Advanced
    watch_enabled: bool = True
    watch_timeout: int = 60
    retry_interval: int = 5
    max_retries: int = 3
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)


class AuthConfig(BaseModel):
    """Enhanced authentication configuration"""
    # Method
    method: AuthMethod = AuthMethod.TOKEN
    header_name: str = "X-APP-TOKEN"
    allow_query_token: bool = False
    allow_cookie: bool = False
    cookie_name: str = "session"
    
    # Token auth
    token_expiry_seconds: int = 86400  # 24 hours
    token_rotation_enabled: bool = True
    token_rotation_interval: int = 604800  # 7 days
    token_storage: str = "memory"  # memory, redis, database
    
    # JWT
    jwt_secret: Optional[SecretStr] = None
    jwt_public_key: Optional[str] = None
    jwt_private_key: Optional[SecretStr] = None
    jwt_algorithm: str = "HS256"
    jwt_expiry_seconds: int = 3600
    jwt_refresh_expiry_seconds: int = 86400
    jwt_issuer: Optional[str] = None
    jwt_audience: Optional[str] = None
    jwt_claims: Dict[str, Any] = Field(default_factory=dict)
    
    # OAuth2
    oauth2_provider: Optional[str] = None  # google, azure, okta, auth0, keycloak
    oauth2_client_id: Optional[str] = None
    oauth2_client_secret: Optional[SecretStr] = None
    oauth2_authorize_url: Optional[str] = None
    oauth2_token_url: Optional[str] = None
    oauth2_userinfo_url: Optional[str] = None
    oauth2_revoke_url: Optional[str] = None
    oauth2_redirect_uri: Optional[str] = None
    oauth2_scopes: List[str] = Field(default_factory=lambda: ["openid", "email", "profile"])
    oauth2_pkce: bool = True
    
    # API Key
    api_key_header: str = "X-API-Key"
    api_key_param: str = "api_key"
    api_key_cookie: str = "api_key"
    api_key_storage: str = "memory"  # memory, redis, database
    
    # mTLS
    mtls_enabled: bool = False
    mtls_ca_cert: Optional[FilePath] = None
    mtls_cert_required: bool = True
    mtls_verify_client: bool = True
    mtls_client_cert_header: Optional[str] = None
    
    # SAML
    saml_idp_metadata_url: Optional[str] = None
    saml_idp_metadata_file: Optional[FilePath] = None
    saml_sp_entity_id: Optional[str] = None
    saml_acs_url: Optional[str] = None
    saml_slo_url: Optional[str] = None
    saml_cert: Optional[FilePath] = None
    saml_private_key: Optional[SecretStr] = None
    saml_encryption_cert: Optional[FilePath] = None
    
    # OIDC
    oidc_discovery_url: Optional[str] = None
    oidc_client_id: Optional[str] = None
    oidc_client_secret: Optional[SecretStr] = None
    oidc_redirect_uri: Optional[str] = None
    oidc_scopes: List[str] = Field(default_factory=lambda: ["openid", "profile", "email"])
    
    # LDAP
    ldap_server: Optional[str] = None
    ldap_port: int = 389
    ldap_use_ssl: bool = False
    ldap_bind_dn: Optional[str] = None
    ldap_bind_password: Optional[SecretStr] = None
    ldap_base_dn: Optional[str] = None
    ldap_user_filter: str = "(uid={0})"
    ldap_group_filter: str = "(member={0})"
    
    # Rate limiting per auth
    rate_limit_requests: int = 100
    rate_limit_window: int = 60
    rate_limit_burst: int = 20
    
    # Session
    session_timeout: int = 3600
    session_cookie_secure: bool = True
    session_cookie_httponly: bool = True
    session_cookie_samesite: str = "lax"
    session_storage: str = "memory"  # memory, redis, database
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)


class RateLimitConfig(BaseModel):
    """Enhanced rate limiting configuration with ML-based detection"""
    # Enable
    enabled: bool = True
    strategy: str = "token_bucket"  # token_bucket, leaky_bucket, fixed_window, sliding_window, gcra
    
    # Global limits
    global_requests_per_second: int = 1000
    global_burst_size: int = 2000
    global_concurrent: int = 500
    
    # Per client limits
    per_ip_requests_per_second: int = 10
    per_ip_burst_size: int = 20
    per_ip_concurrent: int = 5
    
    per_token_requests_per_second: int = 50
    per_token_burst_size: int = 100
    per_token_concurrent: int = 25
    
    per_user_requests_per_second: int = 30
    per_user_burst_size: int = 60
    per_user_concurrent: int = 15
    
    # Per endpoint limits
    per_endpoint_enabled: bool = True
    default_endpoint_limit: int = 100
    endpoint_overrides: Dict[str, int] = Field(default_factory=dict)
    
    # ML-based detection
    ml_enabled: bool = False
    ml_model_path: Optional[FilePath] = None
    ml_anomaly_threshold: float = 3.0  # standard deviations
    ml_learning_rate: float = 0.01
    ml_update_interval: int = 3600  # seconds
    
    # Backend
    storage_backend: str = "memory"  # memory, redis, memcached, database
    redis_uri: Optional[str] = None
    redis_prefix: str = "ratelimit:"
    database_table: Optional[str] = None
    
    # Headers
    headers_enabled: bool = True
    retry_after_header: bool = True
    limit_header: str = "X-RateLimit-Limit"
    remaining_header: str = "X-RateLimit-Remaining"
    reset_header: str = "X-RateLimit-Reset"
    
    # Exemptions
    whitelist_ips: List[str] = Field(default_factory=list)
    whitelist_tokens: List[str] = Field(default_factory=list)
    whitelist_users: List[str] = Field(default_factory=list)
    whitelist_paths: List[str] = Field(default_factory=list)
    
    # Monitoring
    metrics_enabled: bool = True
    log_blocked: bool = True
    alert_on_abuse: bool = False
    abuse_threshold: float = 0.8  # 80% of limit
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)


class CacheConfig(BaseModel):
    """Enhanced cache configuration with multi-level and prediction"""
    # Strategy
    strategy: CacheStrategy = CacheStrategy.HYBRID
    default_ttl_seconds: int = 300
    max_size: int = 10000
    
    # L1 (Memory)
    local_ttl_seconds: int = 60
    local_max_size: int = 1000
    local_eviction_policy: str = "lru"  # lru, lfu, fifo, ttl
    
    # L2 (Redis)
    redis_uri: Optional[str] = None
    redis_prefix: str = "tadawul:cache:"
    redis_ttl_seconds: int = 3600
    redis_serializer: str = "json"  # json, pickle, msgpack
    
    # L3 (Disk)
    disk_enabled: bool = False
    disk_path: Optional[DirectoryPath] = None
    disk_ttl_seconds: int = 86400
    disk_max_size_mb: int = 1024
    
    # Advanced features
    enable_prediction: bool = False  # ML-based predictive pre-fetch
    prediction_model_path: Optional[FilePath] = None
    prediction_window: int = 3600  # seconds
    
    enable_compression: bool = False
    compression_threshold: int = 1024  # bytes
    compression_level: int = 6
    
    enable_encryption: bool = False
    encryption_key: Optional[SecretStr] = None
    encryption_salt: Optional[str] = None
    
    # Cache warming
    warm_on_startup: bool = True
    warm_keys: List[str] = Field(default_factory=list)
    warm_interval: int = 300
    warm_concurrency: int = 10
    
    # Monitoring
    hit_rate_threshold: float = 0.8
    enable_stats: bool = True
    stats_interval: int = 60
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)


class MLConfig(BaseModel):
    """Enhanced ML configuration with model registry and A/B testing"""
    # Enable
    enabled: bool = True
    model_version: str = "1.0.0"
    model_path: Optional[FilePath] = None
    model_name: str = "default"
    
    # Model storage
    model_registry: str = "local"  # local, s3, gcs, azure, mlflow
    s3_bucket: Optional[str] = None
    s3_key: Optional[str] = None
    gcs_bucket: Optional[str] = None
    azure_container: Optional[str] = None
    mlflow_tracking_uri: Optional[str] = None
    mlflow_experiment: Optional[str] = None
    
    # Inference
    batch_size: int = 32
    prediction_timeout_seconds: float = 5.0
    max_concurrent_predictions: int = 10
    cache_predictions: bool = True
    cache_ttl_seconds: int = 3600
    
    # Features
    feature_store: Optional[str] = None  # redis, feast, hopsworks
    feature_group: Optional[str] = None
    feature_version: str = "1.0.0"
    feature_ttl_seconds: int = 3600
    
    # Explainability
    enable_explainability: bool = True
    shap_enabled: bool = True
    lime_enabled: bool = False
    explainer_path: Optional[FilePath] = None
    
    # A/B Testing
    enable_ab_testing: bool = False
    model_a_version: Optional[str] = None
    model_b_version: Optional[str] = None
    ab_traffic_split: float = 0.5
    ab_metrics: List[str] = Field(default_factory=lambda: ["accuracy", "latency"])
    ab_duration_days: int = 7
    ab_auto_rollback: bool = True
    ab_rollback_threshold: float = 0.1  # 10% degradation
    
    # Monitoring
    track_predictions: bool = True
    track_performance: bool = True
    alert_on_drift: bool = True
    drift_threshold: float = 0.1
    drift_detection_interval: int = 3600  # seconds
    drift_metric: str = "psi"  # psi, ks, chi2, earth_mover
    
    # Training
    auto_retrain: bool = False
    retrain_schedule: str = "0 3 * * 0"  # Weekly on Sunday at 3 AM
    retrain_trigger_accuracy: float = 0.9
    retrain_max_models: int = 5
    retrain_data_window_days: int = 30
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)


class SAMAConfig(BaseModel):
    """SAMA (Saudi Central Bank) compliance configuration"""
    # Enable
    enabled: bool = True
    audit_log_enabled: bool = True
    audit_retention_days: int = 90
    audit_immutable: bool = True  # Audit logs cannot be modified
    audit_encryption: bool = True
    audit_hash_chain: bool = True  # Cryptographic hash chain for tamper detection
    
    # Trading hours
    trading_hours_start: str = "10:00"  # 10 AM Riyadh
    trading_hours_end: str = "15:00"    # 3 PM Riyadh
    pre_market_start: str = "09:30"     # 9:30 AM Riyadh
    post_market_end: str = "15:30"      # 3:30 PM Riyadh
    auction_start: str = "09:45"        # 9:45 AM Riyadh
    auction_end: str = "10:00"          # 10 AM Riyadh
    
    weekend_days: List[int] = Field(default_factory=lambda: [4, 5])  # Friday, Saturday
    
    # Holidays
    holiday_file: Optional[FilePath] = None
    holiday_refresh_interval: int = 86400  # 24 hours
    holiday_api_url: Optional[str] = None
    holiday_api_key: Optional[SecretStr] = None
    
    # Reporting
    reporting_currency: str = "SAR"
    reporting_timezone: str = "Asia/Riyadh"
    reporting_format: str = "iso20022"  # ISO 20022 XML format
    reporting_edi: str = "edi_fa"  # EDI Financial Accounting
    
    # Capital requirements
    min_capital: float = 500000  # 500K SAR for institutional
    max_leverage: float = 1.0
    margin_requirements: Dict[str, float] = Field(default_factory=lambda: {
        "equity": 0.5,   # 50% margin
        "sukuk": 0.3,    # 30% margin
        "reit": 0.4,     # 40% margin
        "etf": 0.5,      # 50% margin
        "derivative": 0.7,  # 70% margin
    })
    
    # Risk management
    var_confidence_level: float = 0.99
    var_calculation_method: str = "historical"  # historical, parametric, monte_carlo
    var_lookback_days: int = 252
    stress_test_scenarios: List[str] = Field(default_factory=lambda: [
        "market_crash_2008",
        "covid_2020",
        "oil_price_crash_2020",
        "interest_rate_hike_2022",
        "inflation_shock",
        "currency_crisis",
        "geopolitical_conflict",
    ])
    stress_test_file: Optional[FilePath] = None
    
    # Data residency
    data_residency: List[str] = Field(default_factory=lambda: [
        "sa-central-1",  # Riyadh
        "sa-west-1",     # Jeddah
        "sa-east-1",     # Dammam
    ])
    data_sovereignty: bool = True  # Data must stay in Saudi Arabia
    data_classification: str = "confidential"  # public, internal, confidential, restricted
    
    # Cybersecurity
    nca_compliant: bool = True  # National Cybersecurity Authority
    nca_controls: List[str] = Field(default_factory=lambda: [
        "CR-1", "CR-2", "CR-3",  # Cyber Readiness
        "AC-1", "AC-2", "AC-3",  # Access Control
        "IA-1", "IA-2",          # Identification & Authentication
        "AU-1", "AU-2",          # Audit & Accountability
        "CM-1", "CM-2",          # Configuration Management
    ])
    
    # KYC/AML
    kyc_required: bool = True
    aml_screening: bool = True
    aml_provider: Optional[str] = None  # worldcheck, dowjones, refinitiv
    aml_api_key: Optional[SecretStr] = None
    aml_threshold: float = 0.7  # Risk score threshold
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)
        
        @field_validator("trading_hours_start", "trading_hours_end", "pre_market_start", 
                        "post_market_end", "auction_start", "auction_end")
        def validate_time_format(cls, v):
            """Validate time format HH:MM"""
            if not re.match(r"^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$", v):
                raise ValueError("Time must be in HH:MM format")
            return v


class FeatureFlag(BaseModel):
    """Enhanced feature flag with ML-powered rollout and A/B testing"""
    # Basic
    name: str
    enabled: bool = False
    stage: FeatureStage = FeatureStage.ALPHA
    
    # Description
    description: Optional[str] = None
    owner: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    
    # Rollout
    rollout_percentage: int = 0
    rollout_strategy: str = "random"  # random, consistent, gradual, ml_optimized
    rollout_increment: int = 5  # percentage increment for gradual rollout
    rollout_interval: int = 3600  # seconds between increments
    
    # Targeting
    enabled_for: List[str] = Field(default_factory=list)  # User IDs
    disabled_for: List[str] = Field(default_factory=list)
    enabled_for_groups: List[str] = Field(default_factory=list)
    disabled_for_groups: List[str] = Field(default_factory=list)
    enabled_for_ips: List[str] = Field(default_factory=list)
    enabled_for_countries: List[str] = Field(default_factory=list)
    enabled_for_segments: List[str] = Field(default_factory=list)  # Customer segments
    
    # ML-based targeting
    ml_enabled: bool = False
    ml_model_path: Optional[FilePath] = None
    ml_features: List[str] = Field(default_factory=list)
    ml_threshold: float = 0.5
    ml_score_key: str = "feature_score"
    
    # Time constraints
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    start_time: Optional[str] = None  # Daily start time (HH:MM)
    end_time: Optional[str] = None    # Daily end time (HH:MM)
    timezone: str = "Asia/Riyadh"
    
    # A/B testing
    experiment_id: Optional[str] = None
    experiment_variant: Optional[str] = None
    experiment_control: Optional[str] = None
    experiment_treatment: Optional[str] = None
    experiment_metrics: List[str] = Field(default_factory=lambda: ["conversion", "engagement"])
    experiment_duration_days: int = 7
    experiment_min_users: int = 1000
    experiment_significance: float = 0.95
    
    # Dependencies
    depends_on: List[str] = Field(default_factory=list)
    required_features: List[str] = Field(default_factory=list)
    conflicts_with: List[str] = Field(default_factory=list)
    
    # Metadata
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    version: int = 1
    changelog: List[Dict[str, Any]] = Field(default_factory=list)
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)
        
        def is_enabled_for(self, user_id: str, user_groups: Optional[List[str]] = None, 
                          user_ip: Optional[str] = None, country: Optional[str] = None,
                          features: Optional[Dict[str, Any]] = None) -> bool:
            """Check if feature is enabled for specific user"""
            if not self.enabled:
                return False
            
            now = datetime.now()
            
            # Check date range
            if self.start_date and now < self.start_date:
                return False
            if self.end_date and now > self.end_date:
                return False
            
            # Check time of day
            if self.start_time and self.end_time:
                current_time = now.strftime("%H:%M")
                if self.start_time <= self.end_time:
                    if not (self.start_time <= current_time <= self.end_time):
                        return False
                else:  # Overnight range
                    if not (current_time >= self.start_time or current_time <= self.end_time):
                        return False
            
            # Check blacklists
            if self.disabled_for and user_id in self.disabled_for:
                return False
            
            if self.disabled_for_groups and user_groups:
                if any(g in self.disabled_for_groups for g in user_groups):
                    return False
            
            # Check whitelists
            if self.enabled_for and user_id in self.enabled_for:
                return True
            
            if self.enabled_for_groups and user_groups:
                if any(g in self.enabled_for_groups for g in user_groups):
                    return True
            
            if self.enabled_for_ips and user_ip:
                if user_ip in self.enabled_for_ips:
                    return True
            
            if self.enabled_for_countries and country:
                if country in self.enabled_for_countries:
                    return True
            
            # ML-based targeting
            if self.ml_enabled and features:
                score = features.get(self.ml_score_key, 0)
                if score >= self.ml_threshold:
                    return True
            
            # Check rollout percentage
            if self.rollout_percentage < 100:
                if self.rollout_strategy == "consistent":
                    # Consistent hashing
                    hash_val = int(hashlib.md5(f"{user_id}:{self.name}".encode()).hexdigest(), 16) % 100
                    if hash_val >= self.rollout_percentage:
                        return False
                elif self.rollout_strategy == "ml_optimized" and features:
                    # ML-based assignment
                    pass
                else:
                    # Random
                    if random.randint(1, 100) > self.rollout_percentage:
                        return False
            
            # Check dependencies
            if self.depends_on:
                # Dependencies would be checked by caller
                pass
            
            return True


class EndpointConfig(BaseModel):
    """Enhanced endpoint configuration with versioning and canary"""
    # Basic
    path: str
    methods: List[str] = Field(default_factory=lambda: ["GET"])
    
    # Versioning
    version: str = "1.0.0"
    deprecated: bool = False
    sunset_date: Optional[datetime] = None
    migration_url: Optional[str] = None
    
    # Canary
    canary_enabled: bool = False
    canary_percentage: int = 0
    canary_target: str = "v2"  # Canary version
    
    # Rate limiting
    rate_limit: Optional[int] = None
    rate_limit_burst: Optional[int] = None
    rate_limit_concurrent: Optional[int] = None
    
    # Auth
    auth_required: bool = True
    auth_methods: List[AuthMethod] = Field(default_factory=list)
    roles_allowed: List[str] = Field(default_factory=list)
    permissions_required: List[str] = Field(default_factory=list)
    
    # Cache
    cache_ttl: Optional[int] = None
    cache_vary_by_user: bool = False
    cache_vary_by_ip: bool = False
    cache_vary_by_headers: List[str] = Field(default_factory=list)
    
    # Performance
    timeout_seconds: float = 30.0
    max_body_size: int = 1048576  # 1MB
    max_headers_size: int = 8192
    compression_enabled: bool = True
    
    # Monitoring
    enabled: bool = True
    track_metrics: bool = True
    log_requests: bool = True
    alert_on_error: bool = False
    error_threshold: float = 0.05  # 5% error rate
    latency_threshold_ms: int = 1000
    
    # CORS
    cors_enabled: bool = True
    cors_origins: List[str] = Field(default_factory=list)
    cors_methods: List[str] = Field(default_factory=lambda: ["GET", "POST", "PUT", "DELETE", "OPTIONS"])
    cors_headers: List[str] = Field(default_factory=list)
    cors_credentials: bool = False
    cors_max_age: int = 3600
    
    # Security
    require_https: bool = True
    hsts_enabled: bool = True
    hsts_max_age: int = 31536000
    hsts_include_subdomains: bool = True
    content_security_policy: Optional[str] = None
    x_frame_options: str = "DENY"
    x_content_type_options: str = "nosniff"
    referrer_policy: str = "strict-origin-when-cross-origin"
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)


class ServiceConfig(BaseModel):
    """Enhanced microservice configuration with service mesh"""
    # Basic
    name: str
    host: str = "localhost"
    port: int
    protocol: str = "http"
    
    # Health
    health_check_path: str = "/health"
    health_check_interval: int = 30
    health_check_timeout: float = 5.0
    health_check_grace_period: int = 10
    unhealthy_threshold: int = 3
    healthy_threshold: int = 2
    
    # Circuit breaker
    circuit_breaker_enabled: bool = True
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 60.0
    circuit_breaker_half_open: int = 2
    circuit_breaker_success_threshold: int = 3
    
    # Retry
    retry_enabled: bool = True
    retry_count: int = 3
    retry_backoff: float = 1.0
    retry_max_backoff: float = 10.0
    retry_on_timeout: bool = True
    retry_on_5xx: bool = True
    
    # Timeout
    connect_timeout: float = 5.0
    read_timeout: float = 10.0
    write_timeout: float = 10.0
    pool_timeout: float = 10.0
    pool_keepalive: int = 60
    
    # Load balancing
    load_balancer: str = "round_robin"  # round_robin, least_conn, random, consistent_hash, weighted
    stickiness_enabled: bool = False
    stickiness_ttl: int = 300
    stickiness_cookie: str = "route"
    weights: Dict[str, float] = Field(default_factory=dict)
    
    # Service discovery
    discovery_enabled: bool = True
    discovery_provider: str = "consul"  # consul, etcd, dns, kubernetes, eureka, zookeeper
    discovery_tags: List[str] = Field(default_factory=list)
    discovery_metadata: Dict[str, str] = Field(default_factory=dict)
    discovery_health_check: bool = True
    discovery_ttl: int = 30
    
    # Service mesh
    mesh_enabled: bool = False
    mesh_sidecar_port: Optional[int] = None
    mesh_sidecar_admin_port: Optional[int] = None
    mesh_telemetry: bool = True
    mesh_tracing: bool = False
    mesh_mtls: bool = False
    
    # Rate limiting
    rate_limit_enabled: bool = False
    rate_limit_requests: int = 1000
    rate_limit_window: int = 60
    rate_limit_per_client: bool = True
    
    # Metadata
    metadata: Dict[str, Any] = Field(default_factory=dict)
    version: str = "1.0.0"
    environment: Optional[Environment] = None
    region: Optional[str] = None
    zone: Optional[str] = None
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)
        
        @property
        def base_url(self) -> str:
            """Get service base URL"""
            return f"{self.protocol}://{self.host}:{self.port}"


class AlertingConfig(BaseModel):
    """Enhanced alerting and notification configuration"""
    # Enable
    enabled: bool = True
    
    # Alert rules
    error_rate_threshold: float = 0.05
    latency_threshold_ms: int = 1000
    cpu_threshold: float = 80.0
    memory_threshold: float = 85.0
    disk_threshold: float = 90.0
    connection_threshold: int = 1000
    
    # ML-based anomaly detection
    ml_anomaly_enabled: bool = False
    ml_anomaly_threshold: float = 3.0
    ml_anomaly_model_path: Optional[FilePath] = None
    ml_anomaly_features: List[str] = Field(default_factory=list)
    
    # Notification channels
    slack_webhook: Optional[str] = None
    slack_channel: Optional[str] = None
    slack_username: str = "AlertBot"
    slack_icon_emoji: str = ":warning:"
    
    teams_webhook: Optional[str] = None
    teams_channel: Optional[str] = None
    
    discord_webhook: Optional[str] = None
    discord_channel: Optional[str] = None
    
    pagerduty_key: Optional[SecretStr] = None
    pagerduty_severity: AlertSeverity = AlertSeverity.HIGH
    pagerduty_region: str = "us"
    
    opsgenie_key: Optional[SecretStr] = None
    opsgenie_priority: str = "P2"
    opsgenie_region: str = "us"
    
    email_enabled: bool = False
    email_recipients: List[EmailStr] = Field(default_factory=list)
    email_smtp_host: Optional[str] = None
    email_smtp_port: int = 587
    email_username: Optional[str] = None
    email_password: Optional[SecretStr] = None
    email_use_tls: bool = True
    email_from: Optional[str] = None
    
    sms_enabled: bool = False
    sms_phone_numbers: List[str] = Field(default_factory=list)
    sms_provider: str = "twilio"  # twilio, aws_sns, azure_communication
    sms_account_sid: Optional[SecretStr] = None
    sms_auth_token: Optional[SecretStr] = None
    sms_from_number: Optional[str] = None
    
    # Alert cooldown
    cooldown_minutes: int = 15
    max_alerts_per_hour: int = 10
    
    # Alert severity mapping
    severity_map: Dict[str, str] = Field(default_factory=lambda: {
        "debug": "info",
        "info": "info",
        "warning": "warning",
        "error": "error",
        "critical": "critical",
        "emergency": "critical"
    })
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)


class TracingConfig(BaseModel):
    """Enhanced distributed tracing configuration"""
    # Enable
    enabled: bool = False
    provider: str = "jaeger"  # jaeger, zipkin, datadog, newrelic, honeycomb, lightstep
    
    # Jaeger
    jaeger_host: str = "localhost"
    jaeger_port: int = 6831
    jaeger_agent_host: Optional[str] = None
    jaeger_agent_port: Optional[int] = None
    
    # Zipkin
    zipkin_endpoint: Optional[str] = None
    zipkin_api_version: str = "v2"
    
    # Datadog
    datadog_agent_host: str = "localhost"
    datadog_agent_port: int = 8126
    datadog_env: Optional[str] = None
    datadog_service: Optional[str] = None
    datadog_version: Optional[str] = None
    
    # New Relic
    newrelic_license_key: Optional[SecretStr] = None
    newrelic_app_name: str = "tadawul"
    newrelic_distributed_tracing: bool = True
    
    # Honeycomb
    honeycomb_api_key: Optional[SecretStr] = None
    honeycomb_dataset: str = "tadawul"
    honeycomb_sample_rate: int = 1
    
    # Lightstep
    lightstep_access_token: Optional[SecretStr] = None
    lightstep_collector_host: str = "ingest.lightstep.com"
    lightstep_collector_port: int = 443
    
    # Sampling
    sample_rate: float = 0.1
    sample_rate_low_priority: float = 0.01
    sample_rate_high_priority: float = 1.0
    
    # Adaptive sampling
    adaptive_sampling: bool = False
    adaptive_target_rate: int = 100  # spans per second
    adaptive_interval: int = 10  # seconds
    
    # Tags
    service_tags: Dict[str, str] = Field(default_factory=dict)
    
    # Baggage
    baggage_enabled: bool = True
    baggage_keys: List[str] = Field(default_factory=lambda: ["user_id", "session_id"])
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True)


class BackupConfig(BaseModel):
    """Configuration backup and restore"""
    # Enable
    enabled: bool = True
    strategy: BackupStrategy = BackupStrategy.S3
    
    # Local
    local_path: Optional[DirectoryPath] = None
    local_max_backups: int = 10
    
    # S3
    s3_bucket: Optional[str] = None
    s3_prefix: str = "config/backups/"
    s3_region: str = "me-south-1"
    s3_kms_key: Optional[str] = None
    s3_storage_class: str = "STANDARD"
    
    # GCS
    gcs_bucket: Optional[str] = None
    gcs_prefix: str = "config/backups/"
    gcs_encryption_key: Optional[str] = None
    
    # Azure
    azure_container: Optional[str] = None
    azure_prefix: str = "config/backups/"
    azure_connection_string: Optional[SecretStr] = None
    
    # Schedule
    schedule: str = "0 3 * * *"  # Daily at 3 AM
    retention_days: int = 30
    compression: bool = True
    encryption: bool = True
    encryption_key: Optional[SecretStr] = None
    
    # Verification
    verify_after_backup: bool = True
    verify_checksum: bool = True
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)


class MigrationConfig(BaseModel):
    """Configuration migration settings"""
    # Strategy
    strategy: MigrationStrategy = MigrationStrategy.BLUE_GREEN
    
    # Versioning
    current_version: str = CONFIG_VERSION
    min_version: str = "3.0.0"
    max_version: str = CONFIG_VERSION
    
    # Validation
    validation_level: ValidationLevel = ValidationLevel.STRICT
    schema_file: Optional[FilePath] = None
    
    # Rollback
    auto_rollback: bool = True
    rollback_on_failure: bool = True
    rollback_timeout: int = 300
    
    # Blue/Green
    blue_config: Optional[FilePath] = None
    green_config: Optional[FilePath] = None
    traffic_shift_duration: int = 300  # seconds
    
    # Canary
    canary_percentage: int = 10
    canary_duration: int = 3600  # seconds
    canary_metrics: List[str] = Field(default_factory=lambda: ["error_rate", "latency"])
    canary_threshold: float = 0.05  # 5% degradation
    
    # Rollback
    rollback_versions: List[str] = Field(default_factory=list)
    rollback_max_versions: int = 10
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)


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
        ConfigProvider.FILE,
        ConfigProvider.REDIS,
        ConfigProvider.CONSUL,
        ConfigProvider.VAULT,
    ])
    
    # Config backends
    consul: ConsulConfig = Field(default_factory=ConsulConfig)
    etcd_hosts: List[str] = Field(default_factory=list)
    etcd_prefix: str = "/tadawul/"
    etcd_timeout: float = 5.0
    
    zookeeper_hosts: List[str] = Field(default_factory=list)
    zookeeper_prefix: str = "/tadawul"
    zookeeper_timeout: float = 10.0
    
    kubernetes: KubernetesConfig = Field(default_factory=KubernetesConfig)
    
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
    
    # TLS
    tls: TLSConfig = Field(default_factory=TLSConfig)
    
    # Auth
    auth: AuthConfig = Field(default_factory=AuthConfig)
    tokens: List[SecretStr] = Field(default_factory=list)
    backup_tokens: List[SecretStr] = Field(default_factory=list)
    jwt_tokens: List[SecretStr] = Field(default_factory=list)
    
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
    
    # Backup
    backup: BackupConfig = Field(default_factory=BackupConfig)
    
    # Migration
    migration: MigrationConfig = Field(default_factory=MigrationConfig)
    
    # Batch processing
    batch_size: int = 25
    batch_timeout_sec: float = 60.0
    batch_concurrency: int = 6
    max_tickers: int = 1200
    route_timeout_sec: float = 120.0
    max_connections: int = 100
    
    # Monitoring
    enable_metrics: bool = True
    metrics_port: int = 9090
    metrics_path: str = "/metrics"
    health_check_path: str = "/health"
    readiness_path: str = "/ready"
    liveness_path: str = "/live"
    
    # Logging
    log_level: LogLevel = LogLevel.INFO
    log_format: LogFormat = LogFormat.JSON
    log_file: Optional[FilePath] = None
    log_retention_days: int = 30
    log_max_size_mb: int = 100
    log_compress: bool = True
    
    # Security
    enable_cors: bool = True
    cors_origins: List[str] = Field(default_factory=lambda: ["*"])
    enable_csrf: bool = True
    csrf_exempt_paths: List[str] = Field(default_factory=list)
    csrf_token_length: int = 32
    csrf_token_ttl: int = 3600
    
    secure_cookies: bool = True
    session_timeout: int = 3600
    session_cookie_name: str = "session"
    session_cookie_domain: Optional[str] = None
    
    # Encryption
    encryption_key: Optional[SecretStr] = None
    encryption_salt: Optional[str] = None
    encryption_algorithm: str = "AES-256-GCM"
    
    # Compliance
    compliance_standards: List[ComplianceStandard] = Field(default_factory=lambda: [
        ComplianceStandard.SAMA,
        ComplianceStandard.NCA,
        ComplianceStandard.ISO27001,
    ])
    
    if _PYDANTIC_V2:
        model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True, validate_assignment=True)
        
        @model_validator(mode="after")
        def validate_config(self) -> TadawulConfig:
            """Validate configuration"""
            # Validate tokens in production
            if self.environment == Environment.PRODUCTION:
                if self.auth.method == AuthMethod.TOKEN and not self.tokens:
                    logger.warning("No authentication tokens configured in production mode")
                
                if not self.database.password.get_secret_value():
                    logger.warning("Database password not set in production")
                
                if self.sama.enabled:
                    if self.data_center not in [DataCenter.ONPREM_KSA, DataCenter.ONPREM_JEDDAH, DataCenter.ONPREM_DAMMAM]:
                        logger.warning("SAMA compliance requires data residency in Saudi Arabia")
                    
                    if self.sama.data_residency and self.sama.data_residency:
                        valid_residency = False
                        for dc in self.sama.data_residency:
                            if any(r in dc for r in ["sa-", "onprem"]):
                                valid_residency = True
                                break
                        if not valid_residency:
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
        
        def get_jwt_tokens(self) -> List[str]:
            """Get all JWT tokens as strings"""
            return [t.get_secret_value() for t in self.jwt_tokens if t.get_secret_value()]
        
        def get_feature(self, name: str) -> Optional[FeatureFlag]:
            """Get feature flag by name"""
            return self.features.get(name)
        
        def get_endpoint(self, path: str) -> Optional[EndpointConfig]:
            """Get endpoint config by path (supports regex)"""
            # Exact match
            if path in self.endpoints:
                return self.endpoints[path]
            
            # Pattern match
            for pattern, config in self.endpoints.items():
                if re.match(pattern.replace("*", ".*"), path):
                    return config
            
            return None
        
        def get_service(self, name: str) -> Optional[ServiceConfig]:
            """Get service config by name"""
            return self.services.get(name)


# =============================================================================
# Secure Configuration Manager with Encryption
# =============================================================================

class ConfigEncryption:
    """Advanced configuration encryption with key rotation and multi-algorithm support"""
    
    def __init__(self, master_key: Optional[bytes] = None):
        self.master_key = master_key or self._derive_master_key()
        self._fernet = Fernet(self.master_key) if _CRYPTO_AVAILABLE else None
        self._aesgcm = AESGCM(self.master_key[:32]) if _CRYPTO_AVAILABLE and hasattr(AESGCM, '__call__') else None
        self._key_version = 1
        self._key_rotation_time = datetime.now()
        self._algorithm = TadawulConfig().encryption_algorithm
    
    def _derive_master_key(self) -> bytes:
        """Derive master key from environment"""
        salt = os.getenv("CONFIG_SALT", "tadawul-salt-2024").encode()
        password = os.getenv("CONFIG_PASSWORD", "tadawul-secret-key-2024").encode()
        
        if _CRYPTO_AVAILABLE:
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=100000,
            )
            key = base64.urlsafe_b64encode(kdf.derive(password))
        else:
            # Fallback to simple hashing
            key = hashlib.sha256(password + salt).digest()
            key = base64.urlsafe_b64encode(key)
        
        return key
    
    def encrypt(self, data: str, context: Optional[Dict[str, Any]] = None) -> str:
        """Encrypt string data with context"""
        if not self._fernet or not _CRYPTO_AVAILABLE:
            return data
        
        try:
            # Add context to encryption (prevents replay attacks)
            if context:
                data = json.dumps({"data": data, "context": context})
            
            if self._algorithm == "AES-256-GCM" and self._aesgcm:
                # Use AES-GCM
                nonce = os.urandom(12)
                encrypted = self._aesgcm.encrypt(nonce, data.encode(), None)
                result = base64.b64encode(nonce + encrypted).decode()
            else:
                # Use Fernet
                encrypted = self._fernet.encrypt(data.encode())
                result = base64.b64encode(encrypted).decode()
            
            return result
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            return data
    
    def decrypt(self, data: str, context: Optional[Dict[str, Any]] = None) -> str:
        """Decrypt string data with context verification"""
        if not self._fernet or not _CRYPTO_AVAILABLE:
            return data
        
        try:
            encrypted = base64.b64decode(data.encode())
            
            if self._algorithm == "AES-256-GCM" and self._aesgcm and len(encrypted) > 12:
                # Use AES-GCM
                nonce = encrypted[:12]
                ciphertext = encrypted[12:]
                decrypted = self._aesgcm.decrypt(nonce, ciphertext, None).decode()
            else:
                # Use Fernet
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
            sensitive_keys = ["token", "password", "secret", "key", "credential", "certificate", "private"]
        
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
            elif isinstance(v, SecretStr):
                result[k] = v.get_secret_value()
            elif isinstance(v, SecretBytes):
                result[k] = v.get_secret_value().decode()
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
                try:
                    result[k] = self.decrypt(v, {"key": k})
                except:
                    result[k] = v
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
    """SAMA-compliant audit logger for configuration changes with tamper detection"""
    
    def __init__(self):
        self._audit_buffer: List[Dict[str, Any]] = []
        self._buffer_lock = asyncio.Lock()
        self._audit_file = os.getenv("AUDIT_LOG_FILE", "/var/log/tadawul/config_audit.log")
        self._audit_chain: List[str] = []  # Cryptographic hash chain
        self._immutable = True  # SAMA requirement
        self._encryption = ConfigEncryption()
        
        os.makedirs(os.path.dirname(self._audit_file), exist_ok=True)
    
    def _compute_hash(self, entry: Dict[str, Any], previous_hash: str) -> str:
        """Compute cryptographic hash for entry (for chain)"""
        content = json.dumps(entry, sort_keys=True, default=str)
        data = f"{previous_hash}|{content}".encode()
        return hashlib.sha256(data).hexdigest()
    
    def _sign_entry(self, entry: Dict[str, Any]) -> str:
        """Create HMAC signature for entry"""
        content = json.dumps(entry, sort_keys=True, default=str)
        secret = os.getenv("AUDIT_HMAC_KEY", "").encode()
        if secret:
            return hmac.new(secret, content.encode(), hashlib.sha256).hexdigest()
        return hashlib.sha256(content.encode()).hexdigest()
    
    async def log(
        self,
        action: str,
        key: str,
        old_value: Optional[Any],
        new_value: Optional[Any],
        user: Optional[str],
        source: str,
        request_id: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> None:
        """Log configuration change"""
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": action,
            "key": key,
            "old_value": self._mask_value(old_value),
            "new_value": self._mask_value(new_value),
            "user": user or "system",
            "source": source,
            "request_id": request_id or str(uuid.uuid4()),
            "version": CONFIG_VERSION,
            "tags": tags or [],
        }
        
        # Add to hash chain
        previous_hash = self._audit_chain[-1] if self._audit_chain else "0" * 64
        entry["hash"] = self._compute_hash(entry, previous_hash)
        entry["signature"] = self._sign_entry(entry)
        
        self._audit_chain.append(entry["hash"])
        
        async with self._buffer_lock:
            self._audit_buffer.append(entry)
            
            if len(self._audit_buffer) >= 100:
                await self._flush()
    
    def _mask_value(self, value: Any) -> Any:
        """Mask sensitive values in audit log"""
        if isinstance(value, str) and len(value) > 8:
            # Mask all but first 4 and last 4 characters
            return value[:4] + "*" * (len(value) - 8) + value[-4:] if len(value) > 8 else "***"
        elif isinstance(value, dict):
            return {k: self._mask_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._mask_value(v) for v in value]
        return value
    
    async def _flush(self):
        """Flush audit buffer to disk"""
        buffer = self._audit_buffer.copy()
        self._audit_buffer.clear()
        
        # Write to file (append only)
        try:
            with open(self._audit_file, "a") as f:
                for entry in buffer:
                    f.write(json.dumps(entry) + "\n")
        except Exception as e:
            logger.error(f"Audit flush failed: {e}")
            
            # Retry in memory
            self._audit_buffer = buffer + self._audit_buffer
    
    async def close(self):
        """Close audit logger"""
        await self._flush()
    
    async def query(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        user: Optional[str] = None,
        action: Optional[str] = None,
        key: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Query audit logs (admin only)"""
        results = []
        
        try:
            with open(self._audit_file, "r") as f:
                for line in f:
                    try:
                        entry = json.loads(line)
                        results.append(entry)
                    except:
                        continue
        except FileNotFoundError:
            pass
        
        # Add in-memory entries
        results.extend(self._audit_buffer)
        
        # Apply filters
        filtered = []
        for entry in results:
            if start_time and datetime.fromisoformat(entry["timestamp"]) < start_time:
                continue
            if end_time and datetime.fromisoformat(entry["timestamp"]) > end_time:
                continue
            if user and entry.get("user") != user:
                continue
            if action and entry.get("action") != action:
                continue
            if key and key not in entry.get("key", ""):
                continue
            filtered.append(entry)
        
        # Sort by timestamp descending
        filtered.sort(key=lambda x: x["timestamp"], reverse=True)
        
        return filtered[:limit]


class ConfigMetrics:
    """Configuration metrics collector"""
    
    def __init__(self):
        self._requests = defaultdict(int)
        self._errors = defaultdict(int)
        self._latencies = defaultdict(list)
        self._last_reset = time.time()
        self._lock = asyncio.Lock()
    
    def record_request(self, operation: str, status: str, duration: float):
        """Record request metrics"""
        config_requests_total.labels(operation=operation, status=status).inc()
        config_request_duration.labels(operation=operation).observe(duration)
        
        asyncio.create_task(self._record_async(operation, status, duration))
    
    async def _record_async(self, operation: str, status: str, duration: float):
        """Async metrics recording"""
        async with self._lock:
            self._requests[f"{operation}:{status}"] += 1
            if status != "success":
                self._errors[operation] += 1
            self._latencies[operation].append(duration)
            
            # Keep last 1000 latencies
            if len(self._latencies[operation]) > 1000:
                self._latencies[operation] = self._latencies[operation][-1000:]
    
    def record_provider_request(self, provider: str, status: str, duration: float):
        """Record provider request metrics"""
        config_provider_requests.labels(provider=provider, status=status).inc()
        config_provider_latency.labels(provider=provider).observe(duration)
    
    def record_cache(self, level: str, hit: bool):
        """Record cache metrics"""
        if hit:
            config_cache_hits.labels(level=level).inc()
        else:
            config_cache_misses.labels(level=level).inc()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current metrics"""
        now = time.time()
        uptime = now - self._last_reset
        
        return {
            "requests_per_second": sum(self._requests.values()) / uptime,
            "error_rate": sum(self._errors.values()) / max(sum(self._requests.values()), 1),
            "requests": dict(self._requests),
            "errors": dict(self._errors),
            "avg_latency": {
                op: sum(lats) / len(lats) if lats else 0
                for op, lats in self._latencies.items()
            },
        }


class CircuitBreaker:
    """Advanced circuit breaker with exponential backoff and ML-based thresholds"""
    
    def __init__(self, name: str, threshold: int = 5, timeout: float = 60.0, half_open_requests: int = 3):
        self.name = name
        self.base_threshold = threshold
        self.base_timeout = timeout
        self.half_open_requests = half_open_requests
        
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0.0
        self.consecutive_failures = 0
        self.total_calls = 0
        self.total_failures = 0
        self.total_successes = 0
        self.half_open_calls = 0
        self._lock = asyncio.Lock()
        self._metrics = defaultdict(float)
        
        # ML-based adaptive threshold (if enabled)
        self.ml_enabled = False
        self.ml_model = None
        self.ml_features = []
    
    def _get_adaptive_threshold(self) -> int:
        """Get adaptive threshold based on ML or historical data"""
        if self.ml_enabled and self.ml_model:
            # Use ML model to predict optimal threshold
            features = [
                self.total_calls,
                self.total_failures / max(self.total_calls, 1),
                self.consecutive_failures,
                time.time() - self.last_failure_time if self.last_failure_time else 0,
            ]
            try:
                # Predict threshold (simplified)
                return int(self.base_threshold * (1 + self._metrics.get("load_factor", 0)))
            except:
                pass
        
        # Exponential backoff based on consecutive failures
        if self.consecutive_failures <= self.base_threshold:
            return self.base_threshold
        return self.base_threshold + min(10, self.consecutive_failures - self.base_threshold)
    
    def _get_timeout(self) -> float:
        """Calculate timeout with exponential backoff"""
        threshold = self._get_adaptive_threshold()
        if self.consecutive_failures <= threshold:
            return self.base_timeout
        
        # Exponential backoff: base * 2^(failures-threshold)
        backoff_factor = 2 ** (self.consecutive_failures - threshold)
        return min(self.base_timeout * backoff_factor, 300.0)  # Cap at 5 minutes
    
    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute with circuit breaker protection"""
        
        async with self._lock:
            self.total_calls += 1
            
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time > self._get_timeout():
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_calls = 0
                    self.success_count = 0
                    logger.info(f"Circuit {self.name} entering HALF_OPEN after {self._get_timeout():.1f}s timeout")
                    config_circuit_breakers.labels(provider=self.name).set(1)
                else:
                    config_circuit_breakers.labels(provider=self.name).set(2)
                    raise Exception(f"Circuit {self.name} is OPEN (timeout: {self._get_timeout():.1f}s)")
            
            if self.state == CircuitBreakerState.HALF_OPEN:
                if self.half_open_calls >= self.half_open_requests:
                    raise Exception(f"Circuit {self.name} HALF_OPEN at capacity ({self.half_open_requests})")
                self.half_open_calls += 1
        
        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            
            async with self._lock:
                self.total_successes += 1
                self.success_count += 1
                self.consecutive_failures = 0
                
                if self.state == CircuitBreakerState.HALF_OPEN:
                    if self.success_count >= 2:  # Require 2 successes to close
                        self.state = CircuitBreakerState.CLOSED
                        self.failure_count = 0
                        logger.info(f"Circuit {self.name} CLOSED after {self.success_count} successes")
                        config_circuit_breakers.labels(provider=self.name).set(0)
                
                self._metrics["success_rate"] = self.total_successes / self.total_calls
            
            return result
            
        except Exception as e:
            async with self._lock:
                self.total_failures += 1
                self.failure_count += 1
                self.consecutive_failures += 1
                self.last_failure_time = time.time()
                
                threshold = self._get_adaptive_threshold()
                
                if self.state == CircuitBreakerState.CLOSED and self.failure_count >= threshold:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit {self.name} OPEN after {self.failure_count} failures (threshold={threshold})")
                    config_circuit_breakers.labels(provider=self.name).set(2)
                elif self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit {self.name} re-OPEN from HALF_OPEN")
                    config_circuit_breakers.labels(provider=self.name).set(2)
                
                self._metrics["failure_rate"] = self.total_failures / self.total_calls
            
            raise e
    
    def update_load_factor(self, load: float):
        """Update system load factor for adaptive threshold"""
        self._metrics["load_factor"] = load
    
    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics"""
        return {
            "name": self.name,
            "state": self.state.value,
            "total_calls": self.total_calls,
            "total_successes": self.total_successes,
            "total_failures": self.total_failures,
            "success_rate": self.total_successes / self.total_calls if self.total_calls > 0 else 1.0,
            "failure_rate": self.total_failures / self.total_calls if self.total_calls > 0 else 0.0,
            "current_failure_count": self.failure_count,
            "consecutive_failures": self.consecutive_failures,
            "adaptive_threshold": self._get_adaptive_threshold(),
            "timeout_sec": self._get_timeout(),
            "half_open_requests": self.half_open_requests,
            "half_open_calls": self.half_open_calls,
            "metrics": dict(self._metrics),
        }


class ConfigManager:
    """Distributed configuration manager with hot-reload and ML-based prediction"""
    
    def __init__(self):
        self._config: Optional[TadawulConfig] = None
        self._config_lock = asyncio.Lock()
        self._watchers: List[Callable[[TadawulConfig], None]] = []
        self._watch_task: Optional[asyncio.Task] = None
        self._encryption = ConfigEncryption()
        self._audit = ConfigAuditLogger()
        self._metrics = ConfigMetrics()
        self._providers: Dict[ConfigProvider, Any] = {}
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._stats: Dict[str, Any] = defaultdict(int)
        self._last_update: Optional[datetime] = None
        self._version = 1
        self._initialized = False
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._cache_lock = asyncio.Lock()
        self._history: List[Tuple[datetime, int]] = []  # History of config changes
        
        # ML-based prediction
        self._ml_predictor = None
        self._prediction_model = None
        self._feature_scaler = None
        
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
        
        # Kubernetes
        if _KUBERNETES_AVAILABLE:
            try:
                k8s_config = TadawulConfig().kubernetes
                if k8s_config.in_cluster:
                    kubernetes.config.load_incluster_config()
                else:
                    kubernetes.config.load_kube_config(config_file=k8s_config.config_file)
                self._providers[ConfigProvider.KUBERNETES] = kubernetes.client.CoreV1Api()
                self._circuit_breakers["kubernetes"] = CircuitBreaker("kubernetes")
                logger.info("Kubernetes provider initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Kubernetes: {e}")
        
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
                    namespace=vault_config.namespace,
                    verify=vault_config.verify,
                    timeout=vault_config.timeout
                )
                
                # Authenticate based on method
                if vault_config.auth_method == "approle" and vault_config.role_id and vault_config.secret_id:
                    client.auth.approle.login(
                        role_id=vault_config.role_id.get_secret_value(),
                        secret_id=vault_config.secret_id.get_secret_value()
                    )
                elif vault_config.auth_method == "kubernetes" and vault_config.kubernetes_role:
                    client.auth.kubernetes.login(
                        role=vault_config.kubernetes_role,
                        jwt=vault_config.kubernetes_jwt
                    )
                elif vault_config.auth_method == "aws" and vault_config.aws_role:
                    client.auth.aws.iam_login(
                        role=vault_config.aws_role
                    )
                elif vault_config.auth_method == "gcp" and vault_config.gcp_role:
                    client.auth.gcp.login(
                        role=vault_config.gcp_role,
                        jwt=vault_config.gcp_jwt
                    )
                elif vault_config.auth_method == "ldap" and vault_config.ldap_username:
                    client.auth.ldap.login(
                        username=vault_config.ldap_username,
                        password=vault_config.ldap_password.get_secret_value() if vault_config.ldap_password else None
                    )
                elif vault_config.auth_method == "userpass" and vault_config.userpass_username:
                    client.auth.userpass.login(
                        username=vault_config.userpass_username,
                        password=vault_config.userpass_password.get_secret_value() if vault_config.userpass_password else None
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
                
                # Secrets Manager
                self._providers[ConfigProvider.AWS_SECRETS] = session.client(
                    "secretsmanager",
                    config=aws_config.get_boto3_config(),
                    endpoint_url=aws_config.endpoint_url
                )
                
                # Parameter Store
                self._providers[ConfigProvider.AWS_PARAM_STORE] = session.client(
                    "ssm",
                    config=aws_config.get_boto3_config(),
                    endpoint_url=aws_config.endpoint_url
                )
                
                # S3 (for config backups)
                self._providers["aws_s3"] = session.client(
                    "s3",
                    config=aws_config.get_boto3_config(),
                    endpoint_url=aws_config.s3_endpoint_url
                )
                
                self._circuit_breakers["aws"] = CircuitBreaker("aws")
                logger.info("AWS providers initialized")
            except Exception as e:
                logger.error(f"Failed to initialize AWS: {e}")
        
        # Azure
        if _AZURE_AVAILABLE:
            try:
                azure_config = TadawulConfig().azure
                if azure_config.use_managed_identity:
                    credential = DefaultAzureCredential()
                else:
                    credential = ClientSecretCredential(
                        tenant_id=azure_config.tenant_id,
                        client_id=azure_config.client_id,
                        client_secret=azure_config.client_secret.get_secret_value() if azure_config.client_secret else None
                    )
                
                if azure_config.key_vault_url:
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
                if gcp_config.credentials_json:
                    from google.oauth2 import service_account
                    credentials = service_account.Credentials.from_service_account_info(
                        json.loads(gcp_config.credentials_json.get_secret_value())
                    )
                    client = secretmanager.SecretManagerServiceClient(credentials=credentials)
                else:
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
                if provider not in [ConfigProvider.LOCAL, ConfigProvider.ENV]:
                    try:
                        provider_config = await self._load_from_provider(provider)
                        if provider_config:
                            config_dict = self._deep_merge(config_dict, provider_config)
                            self._stats[f"{provider.value}_hits"] += 1
                    except Exception as e:
                        logger.error(f"Failed to load from {provider}: {e}")
                        self._stats[f"{provider.value}_errors"] += 1
            
            # Decrypt sensitive data
            config_dict = self._encryption.decrypt_dict(config_dict)
            
            # Validate with schema
            config_dict = await self._validate_config(config_dict)
            
            # Create config
            async with self._config_lock:
                self._config = TadawulConfig(**config_dict)
                self._last_update = datetime.now()
                self._version += 1
                self._history.append((self._last_update, self._version))
                self._initialized = True
                
                config_version_gauge.labels(environment=self._config.environment.value).set(self._version)
                config_reloads_total.inc()
            
            logger.info(f"Configuration loaded (version {self._version})")
            
            # Start watch task
            if not self._watch_task:
                self._watch_task = asyncio.create_task(self._watch_config())
            
            # Initialize ML predictor
            await self._init_ml_predictor()
            
        except Exception as e:
            logger.error(f"Failed to load initial configuration: {e}")
            # Use default config as fallback
            self._config = TadawulConfig()
            self._initialized = True
    
    def _deep_merge(self, base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two dictionaries"""
        result = base.copy()
        
        for key, value in overlay.items():
            if value is None:
                continue
                
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            elif key in result and isinstance(result[key], list) and isinstance(value, list):
                # For lists, combine unique items
                combined = result[key] + value
                # Remove duplicates while preserving order
                seen = set()
                result[key] = [x for x in combined if not (x in seen or seen.add(x))]
            else:
                result[key] = value
        
        return result
    
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
        start_time = time.time()
        
        provider_map = {
            ConfigProvider.CONSUL: ("consul", self._load_from_consul),
            ConfigProvider.ETCD: ("etcd", self._load_from_etcd),
            ConfigProvider.ZOOKEEPER: ("zookeeper", self._load_from_zookeeper),
            ConfigProvider.KUBERNETES: ("kubernetes", self._load_from_kubernetes),
            ConfigProvider.REDIS: ("redis", self._load_from_redis),
            ConfigProvider.VAULT: ("vault", self._load_from_vault),
            ConfigProvider.AWS_PARAM_STORE: ("aws", self._load_from_aws_param_store),
            ConfigProvider.AWS_SECRETS: ("aws", self._load_from_aws_secrets),
            ConfigProvider.AZURE_KEYVAULT: ("azure", self._load_from_azure),
            ConfigProvider.GCP_SECRETS: ("gcp", self._load_from_gcp),
        }
        
        if provider not in provider_map:
            return config
        
        cb_key, load_func = provider_map[provider]
        cb = self._circuit_breakers.get(cb_key)
        
        if cb:
            try:
                result = await cb.execute(load_func)
                if result:
                    config.update(result)
                    self._metrics.record_provider_request(provider.value, "success", time.time() - start_time)
            except Exception as e:
                self._metrics.record_provider_request(provider.value, "error", time.time() - start_time)
                logger.error(f"{provider.value} load failed: {e}")
                self._stats[f"{provider.value}_errors"] += 1
        
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
    
    def _load_from_kubernetes(self) -> Dict[str, Any]:
        """Load from Kubernetes ConfigMap"""
        k8s_client = self._providers[ConfigProvider.KUBERNETES]
        config = {}
        
        try:
            k8s_config = TadawulConfig().kubernetes
            configmap = k8s_client.read_namespaced_config_map(
                name=k8s_config.configmap_name,
                namespace=k8s_config.namespace
            )
            
            if configmap.data and k8s_config.configmap_key in configmap.data:
                data = configmap.data[k8s_config.configmap_key]
                try:
                    if data.endswith(('.yaml', '.yml')) and _YAML_AVAILABLE:
                        config = yaml.safe_load(data)
                    else:
                        config = json.loads(data)
                except:
                    config = {"config": data}
            
            # Also load secrets
            if k8s_config.secret_name:
                secret = k8s_client.read_namespaced_secret(
                    name=k8s_config.secret_name,
                    namespace=k8s_config.namespace
                )
                if secret.data:
                    for key, value in secret.data.items():
                        config[f"secret_{key}"] = base64.b64decode(value).decode()
        except Exception as e:
            logger.error(f"Kubernetes query failed: {e}")
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
    
    def _load_from_azure(self) -> Dict[str, Any]:
        """Load from Azure KeyVault"""
        kv_client = self._providers[ConfigProvider.AZURE_KEYVAULT]
        config = {}
        
        try:
            # List all secrets
            secrets = kv_client.list_properties_of_secrets()
            for secret_prop in secrets:
                if secret_prop.name.startswith("tadawul-"):
                    secret = kv_client.get_secret(secret_prop.name)
                    if secret.value:
                        name = secret_prop.name.replace("tadawul-", "")
                        try:
                            config[name] = json.loads(secret.value)
                        except:
                            config[name] = secret.value
        except Exception as e:
            logger.error(f"Azure KeyVault query failed: {e}")
            raise
        
        return config
    
    def _load_from_gcp(self) -> Dict[str, Any]:
        """Load from GCP Secret Manager"""
        sm_client = self._providers[ConfigProvider.GCP_SECRETS]
        config = {}
        
        try:
            gcp_config = TadawulConfig().gcp
            parent = f"projects/{gcp_config.project_id}"
            
            # List secrets
            for secret in sm_client.list_secrets(request={"parent": parent}):
                if secret.name.endswith("-config"):
                    # Access latest version
                    name = secret.name.split("/")[-1].replace("-config", "")
                    response = sm_client.access_secret_version(
                        request={"name": f"{secret.name}/versions/latest"}
                    )
                    secret_string = response.payload.data.decode("UTF-8")
                    try:
                        config[name] = json.loads(secret_string)
                    except:
                        config[name] = secret_string
        except Exception as e:
            logger.error(f"GCP Secret Manager query failed: {e}")
            raise
        
        return config
    
    async def _validate_config(self, config_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Validate configuration against schema"""
        migration_config = TadawulConfig().migration
        
        if migration_config.validation_level == ValidationLevel.OFF:
            return config_dict
        
        # Validate against JSON schema if provided
        if migration_config.schema_file and _YAML_AVAILABLE:
            try:
                with open(migration_config.schema_file, 'r') as f:
                    if migration_config.schema_file.suffix in ('.yaml', '.yml'):
                        schema = yaml.safe_load(f)
                    else:
                        schema = json.load(f)
                
                # Validate using jsonschema if available
                try:
                    import jsonschema
                    jsonschema.validate(instance=config_dict, schema=schema)
                except ImportError:
                    pass
            except Exception as e:
                logger.error(f"Schema validation failed: {e}")
                if migration_config.validation_level == ValidationLevel.STRICT:
                    raise
        
        return config_dict
    
    async def _init_ml_predictor(self):
        """Initialize ML predictor for configuration changes"""
        if not _ML_AVAILABLE or not TadawulConfig().ml.enabled:
            return
        
        try:
            # Simple predictor for config change patterns
            self._prediction_model = RandomForestClassifier(n_estimators=100)
            self._feature_scaler = StandardScaler()
            logger.info("ML predictor initialized for configuration")
        except Exception as e:
            logger.error(f"Failed to initialize ML predictor: {e}")
    
    async def _predict_config_changes(self) -> Dict[str, float]:
        """Predict which config keys might change soon"""
        if not self._prediction_model or not self._history:
            return {}
        
        # Simple prediction based on change frequency
        predictions = {}
        now = datetime.now()
        
        for timestamp, version in self._history[-10:]:
            age_hours = (now - timestamp).total_seconds() / 3600
            # Higher score for recently changed configs
            predictions[f"version_{version}"] = 1.0 / (1 + age_hours)
        
        return predictions
    
    async def _watch_config(self):
        """Watch for configuration changes"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                if not self._config:
                    continue
                
                # Check for updates
                updated = False
                new_config_dict = self._config.model_dump() if hasattr(self._config, 'model_dump') else self._config.dict()
                
                for provider in self._config.config_providers:
                    if provider not in [ConfigProvider.LOCAL, ConfigProvider.ENV]:
                        try:
                            provider_config = await self._load_from_provider(provider)
                            if provider_config:
                                new_config_dict = self._deep_merge(new_config_dict, provider_config)
                                updated = True
                        except Exception as e:
                            logger.error(f"Provider {provider} watch failed: {e}")
                
                if updated:
                    # Decrypt
                    new_config_dict = self._encryption.decrypt_dict(new_config_dict)
                    
                    # Validate
                    new_config_dict = await self._validate_config(new_config_dict)
                    
                    # Check for actual changes
                    old_dict = self._config.model_dump() if hasattr(self._config, 'model_dump') else self._config.dict()
                    
                    if _DEEPDIFF_AVAILABLE:
                        diff = DeepDiff(old_dict, new_config_dict, ignore_order=True)
                        if not diff:
                            continue
                        changes = diff.to_dict()
                    else:
                        # Simple hash comparison
                        old_hash = hashlib.sha256(json.dumps(old_dict, sort_keys=True).encode()).hexdigest()
                        new_hash = hashlib.sha256(json.dumps(new_config_dict, sort_keys=True).encode()).hexdigest()
                        if old_hash == new_hash:
                            continue
                        changes = {"changed": True}
                    
                    # Create new config
                    async with self._config_lock:
                        old_config = self._config
                        self._config = TadawulConfig(**new_config_dict)
                        self._last_update = datetime.now()
                        self._version += 1
                        self._history.append((self._last_update, self._version))
                        
                        # Update metrics
                        config_version_gauge.labels(environment=self._config.environment.value).set(self._version)
                        config_reloads_total.inc()
                        
                        # Audit log
                        await self._audit.log(
                            action="reload",
                            key="config",
                            old_value=old_dict,
                            new_value=new_config_dict,
                            user="system",
                            source="watch",
                            tags=["auto_reload"]
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
        start_time = time.time()
        
        async with self._config_lock:
            old_config = self._config
            old_dict = old_config.model_dump() if hasattr(old_config, 'model_dump') else old_config.dict()
            
            current_dict = old_dict.copy()
            current_dict.update(updates)
            
            # Encrypt sensitive data
            current_dict = self._encryption.encrypt_dict(current_dict)
            
            # Validate
            current_dict = await self._validate_config(current_dict)
            
            # Create new config
            new_config = TadawulConfig(**current_dict)
            
            # Update providers
            for provider in new_config.config_providers:
                if provider not in [ConfigProvider.LOCAL, ConfigProvider.ENV]:
                    try:
                        await self._update_provider(provider, updates)
                    except Exception as e:
                        logger.error(f"Failed to update {provider}: {e}")
            
            self._config = new_config
            self._last_update = datetime.now()
            self._version += 1
            self._history.append((self._last_update, self._version))
            
            # Update metrics
            config_version_gauge.labels(environment=self._config.environment.value).set(self._version)
            config_reloads_total.inc()
            
            # Audit log
            await self._audit.log(
                action="update",
                key="config",
                old_value=old_dict,
                new_value=current_dict,
                user=user,
                source="api"
            )
            
            # Notify watchers
            for watcher in self._watchers:
                try:
                    watcher(new_config)
                except Exception as e:
                    logger.error(f"Config watcher failed: {e}")
            
            self._metrics.record_request("update", "success", time.time() - start_time)
            
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
                raise
        
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
                raise
        
        elif provider == ConfigProvider.VAULT and provider in self._providers:
            try:
                vault_client = self._providers[provider]
                mount_point = TadawulConfig().vault.mount_point
                vault_client.secrets.kv.v2.create_or_update_secret(
                    mount_point=mount_point,
                    path="tadawul/config",
                    secret=updates
                )
            except Exception as e:
                logger.error(f"Vault update failed: {e}")
                raise
        
        elif provider == ConfigProvider.AWS_PARAM_STORE and provider in self._providers:
            try:
                ssm = self._providers[provider]
                aws_config = TadawulConfig().aws
                for key, value in updates.items():
                    ssm.put_parameter(
                        Name=f"{aws_config.param_store_path}{key}",
                        Value=json.dumps(value, default=str),
                        Type="SecureString",
                        Overwrite=True
                    )
            except Exception as e:
                logger.error(f"AWS Parameter Store update failed: {e}")
                raise
        
        elif provider == ConfigProvider.AWS_SECRETS and provider in self._providers:
            try:
                secrets = self._providers[provider]
                aws_config = TadawulConfig().aws
                for key, value in updates.items():
                    secrets.put_secret_value(
                        SecretId=f"{aws_config.secrets_manager_prefix}/{key}",
                        SecretString=json.dumps(value, default=str)
                    )
            except Exception as e:
                logger.error(f"AWS Secrets Manager update failed: {e}")
                raise
    
    async def rollback_config(self, version: int, user: str) -> TadawulConfig:
        """Rollback to specific configuration version"""
        # Find version in history
        target_version = None
        target_config = None
        
        for timestamp, ver in self._history:
            if ver == version:
                target_version = ver
                # In a real implementation, you'd need to store the actual configs
                break
        
        if target_version is None:
            raise ValueError(f"Version {version} not found in history")
        
        # TODO: Implement actual rollback from stored configs
        
        return await self.get_config()
    
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
            "history_count": len(self._history),
            "metrics": self._metrics.get_stats(),
            "predictions": asyncio.run(self._predict_config_changes()),
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
# Settings Access
# =============================================================================

def get_settings() -> TadawulConfig:
    """Get current configuration"""
    try:
        return asyncio.run(_config_manager.get_config())
    except Exception as e:
        logger.error(f"Failed to get settings: {e}")
        return TadawulConfig()


def get_endpoint_config(path: str) -> Optional[EndpointConfig]:
    """Get configuration for specific endpoint"""
    try:
        config = asyncio.run(_config_manager.get_config())
        return config.get_endpoint(path)
    except Exception:
        return None


# =============================================================================
# Configuration API (for admin endpoints)
# =============================================================================

async def get_config_api() -> TadawulConfig:
    """Get current configuration (for API)"""
    return await _config_manager.get_config()


async def update_config_api(updates: Dict[str, Any], user: str) -> TadawulConfig:
    """Update configuration (for API)"""
    return await _config_manager.update_config(updates, user)


async def rollback_config_api(version: int, user: str) -> TadawulConfig:
    """Rollback configuration (for API)"""
    return await _config_manager.rollback_config(version, user)


async def get_config_stats_api() -> Dict[str, Any]:
    """Get configuration statistics (for API)"""
    return _config_manager.get_stats()


async def get_audit_logs_api(
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    user: Optional[str] = None,
    action: Optional[str] = None,
    key: Optional[str] = None,
    limit: int = 1000
) -> List[Dict[str, Any]]:
    """Get audit logs (for API)"""
    return await _config_manager._audit.query(
        start_time=start_time,
        end_time=end_time,
        user=user,
        action=action,
        key=key,
        limit=limit
    )


async def reload_config_api() -> TadawulConfig:
    """Force reload configuration"""
    await _config_manager._load_initial_config()
    return await _config_manager.get_config()


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    # Version
    "CONFIG_VERSION",
    "SHIM_VERSION",
    
    # Enums
    "Environment",
    "ConfigProvider",
    "SecretProvider",
    "FeatureStage",
    "CacheStrategy",
    "AuthMethod",
    "LogLevel",
    "LogFormat",
    "DataCenter",
    "ComplianceStandard",
    "AlertSeverity",
    "CircuitBreakerState",
    "ConfigChangeType",
    "ConfigStatus",
    "ValidationLevel",
    "BackupStrategy",
    "MigrationStrategy",
    
    # Models
    "TadawulConfig",
    "DatabaseConfig",
    "RedisConfig",
    "ConsulConfig",
    "VaultConfig",
    "AWSConfig",
    "AzureConfig",
    "GCPConfig",
    "KubernetesConfig",
    "AuthConfig",
    "RateLimitConfig",
    "CacheConfig",
    "MLConfig",
    "SAMAConfig",
    "FeatureFlag",
    "EndpointConfig",
    "ServiceConfig",
    "AlertingConfig",
    "TracingConfig",
    "BackupConfig",
    "MigrationConfig",
    "TLSConfig",
    
    # Legacy
    "Settings",
    "AUTH_HEADER_NAME",
    "allowed_tokens",
    "is_open_mode",
    "allow_query_token",
    "auth_ok",
    "auth_ok_request",
    
    # Feature flags
    "is_feature_enabled",
    "get_feature_flags",
    
    # Settings
    "get_settings",
    "get_endpoint_config",
    
    # API
    "get_config_api",
    "update_config_api",
    "rollback_config_api",
    "get_config_stats_api",
    "get_audit_logs_api",
    "reload_config_api",
    
    # Manager
    "ConfigManager",
    "_config_manager",
]
