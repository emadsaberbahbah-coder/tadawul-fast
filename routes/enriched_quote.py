#!/usr/bin/env python3
"""
routes/config.py
------------------------------------------------------------
TADAWUL ENTERPRISE CONFIGURATION MANAGEMENT — v5.4.1 (PROD PATCH)
SAMA Compliant | Distributed Config | Secrets Management | Dynamic Updates

v5.4.1 Fixes (production blockers / correctness)
- ✅ FIX: OpenTelemetry TraceContext corrected (start_as_current_span is a context manager; no attach/detach misuse).
- ✅ FIX: Works on Pydantic v2 AND v1 (SecretStr & types always available; no NameError).
- ✅ FIX: Redis Pub/Sub uses redis.asyncio (preferred) with aioredis fallback.
- ✅ FIX: REQUIRE_AUTH respected (no accidental “open mode” public access when auth is required but tokens missing).
- ✅ IMPROVE: REDIS_URL parsing supported for Render Key Value.
- ✅ IMPROVE: Encryption key handling corrected (raw 32-byte key for AESGCM + Fernet key derived from it).
"""

from __future__ import annotations

import asyncio
import base64
import concurrent.futures
import hashlib
import logging
import os
import random
import time
import zlib
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union, cast
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson

    def json_dumps(v: Any, *, default: Optional[Callable] = None) -> str:
        return orjson.dumps(v, default=default).decode("utf-8")

    def json_loads(v: Union[str, bytes]) -> Any:
        return orjson.loads(v)

    _HAS_ORJSON = True
except ImportError:
    import json

    def json_dumps(v: Any, *, default: Optional[Callable] = None) -> str:
        return json.dumps(v, default=default, ensure_ascii=False)

    def json_loads(v: Union[str, bytes]) -> Any:
        return json.loads(v)

    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Optional Enterprise Integrations
# ---------------------------------------------------------------------------
try:
    import hvac  # Vault
    _VAULT_AVAILABLE = True
except ImportError:
    _VAULT_AVAILABLE = False

try:
    import boto3
    from botocore.config import Config as BotoConfig
    _AWS_AVAILABLE = True
except ImportError:
    _AWS_AVAILABLE = False
    BotoConfig = None  # type: ignore

try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    _CRYPTO_AVAILABLE = True
except ImportError:
    _CRYPTO_AVAILABLE = False
    Fernet = None  # type: ignore
    AESGCM = None  # type: ignore
    PBKDF2HMAC = None  # type: ignore
    hashes = None  # type: ignore

# OpenTelemetry (optional)
try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode

    _OTEL_AVAILABLE = True
    tracer = trace.get_tracer(__name__)
except ImportError:
    _OTEL_AVAILABLE = False

    class DummySpan:
        def set_attribute(self, *args, **kwargs):  # noqa
            return None

        def record_exception(self, *args, **kwargs):  # noqa
            return None

        def set_status(self, *args, **kwargs):  # noqa
            return None

    class DummyContextManager:
        def __init__(self, span):
            self._span = span

        def __enter__(self):
            return self._span

        def __exit__(self, *args, **kwargs):
            return False

    class DummyTracer:
        def start_as_current_span(self, *args, **kwargs):
            return DummyContextManager(DummySpan())

    tracer = DummyTracer()

# Prometheus (optional)
try:
    from prometheus_client import Counter, Histogram, Gauge  # noqa

    _PROMETHEUS_AVAILABLE = True
    config_requests_total = Counter("config_requests_total", "Total config requests", ["operation", "status"])
    config_request_duration = Histogram("config_request_duration_seconds", "Config request duration", ["operation"])
    config_version_gauge = Gauge("config_version", "Config version", ["environment"])
    config_reloads_total = Counter("config_reloads_total", "Total config reloads")
    config_circuit_breakers = Gauge("config_circuit_breakers", "CB state", ["provider"])
except ImportError:
    _PROMETHEUS_AVAILABLE = False

    class DummyMetric:
        def labels(self, *args, **kwargs):
            return self

        def inc(self, *args, **kwargs):
            return None

        def set(self, *args, **kwargs):
            return None

        def observe(self, *args, **kwargs):
            return None

    config_requests_total = DummyMetric()
    config_request_duration = DummyMetric()
    config_version_gauge = DummyMetric()
    config_reloads_total = DummyMetric()
    config_circuit_breakers = DummyMetric()

# Pydantic Compatibility (v2 preferred; v1 safe)
try:
    from pydantic import BaseModel, ConfigDict, Field, SecretStr  # type: ignore
    _PYDANTIC_V2 = True
except ImportError:
    from pydantic import BaseModel, Field, SecretStr  # type: ignore
    ConfigDict = None  # type: ignore
    _PYDANTIC_V2 = False

# Redis async client (prefer redis.asyncio)
try:
    import redis.asyncio as redis_async  # type: ignore
    _REDIS_ASYNC_AVAILABLE = True
except Exception:
    redis_async = None  # type: ignore
    _REDIS_ASYNC_AVAILABLE = False

# aioredis fallback
try:
    import aioredis  # type: ignore
    _AIOREDIS_AVAILABLE = True
except Exception:
    aioredis = None  # type: ignore
    _AIOREDIS_AVAILABLE = False


logger = logging.getLogger("routes.config")

CONFIG_VERSION = "5.4.1"
_TRACING_ENABLED = os.getenv("CORE_TRACING_ENABLED", "").strip().lower() in {"1", "true", "yes", "y", "on"}


# =============================================================================
# Trace Context (FIXED)
# =============================================================================
class TraceContext:
    """
    Correct OTel usage:
      tracer.start_as_current_span(...) -> context manager
      enter to get span, set attributes via set_attribute
      exit closes span
    """

    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._cm = None
        self.span = None
        self._enabled = _OTEL_AVAILABLE and _TRACING_ENABLED

    async def __aenter__(self):
        if self._enabled:
            try:
                self._cm = tracer.start_as_current_span(self.name)
                self.span = self._cm.__enter__()
                for k, v in self.attributes.items():
                    try:
                        if hasattr(self.span, "set_attribute"):
                            self.span.set_attribute(k, v)
                    except Exception:
                        pass
            except Exception:
                self._cm = None
                self.span = None
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._cm and self.span:
            try:
                if exc_val and hasattr(self.span, "record_exception"):
                    self.span.record_exception(exc_val)
                if exc_val and hasattr(self.span, "set_status") and _OTEL_AVAILABLE:
                    self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            except Exception:
                pass
            try:
                return self._cm.__exit__(exc_type, exc_val, exc_tb)
            except Exception:
                return False
        return False


# =============================================================================
# Enums
# =============================================================================
class Environment(str, Enum):
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    DR = "dr"
    SANDBOX = "sandbox"
    CANARY = "canary"


class ConfigProvider(str, Enum):
    LOCAL = "local"
    ENV = "environment"
    FILE = "file"
    REDIS = "redis"
    VAULT = "vault"
    AWS_SECRETS = "aws_secrets"
    AWS_PARAM_STORE = "aws_param_store"


class AuthMethod(str, Enum):
    TOKEN = "token"
    JWT = "jwt"
    API_KEY = "api_key"
    MTLS = "mtls"


class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# =============================================================================
# Pydantic Models (v2 + v1 safe)
# =============================================================================
class TLSConfig(BaseModel):
    enabled: bool = False
    verify_peer: bool = True
    min_version: str = "TLSv1.2"
    mutual_tls: bool = False

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")  # type: ignore


class DatabaseConfig(BaseModel):
    host: str = "localhost"
    port: int = 5432
    name: str = "tadawul"
    user: str = "postgres"
    password: SecretStr = Field(default_factory=lambda: SecretStr(""))
    pool_size: int = 20

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")  # type: ignore


class RedisConfig(BaseModel):
    host: str = "localhost"
    port: int = 6379
    password: SecretStr = Field(default_factory=lambda: SecretStr(""))
    db: int = 0
    max_connections: int = 50

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")  # type: ignore


class VaultConfig(BaseModel):
    url: str = "http://localhost:8200"
    token: SecretStr = Field(default_factory=lambda: SecretStr(""))
    mount_point: str = "secret"
    kv_version: int = 2
    auth_method: str = "token"

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")  # type: ignore


class AWSConfig(BaseModel):
    region: str = "me-south-1"
    access_key_id: Optional[SecretStr] = None
    secret_access_key: Optional[SecretStr] = None
    kms_key_id: Optional[str] = None
    secrets_manager_prefix: str = "tadawul"
    param_store_path: str = "/tadawul/"

    def get_boto3_config(self):
        if not _AWS_AVAILABLE or BotoConfig is None:
            return None
        return BotoConfig(region_name=self.region, retries={"max_attempts": 3, "mode": "adaptive"})

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")  # type: ignore


class AuthConfig(BaseModel):
    method: AuthMethod = AuthMethod.TOKEN
    header_name: str = "X-APP-TOKEN"
    allow_query_token: bool = False
    token_expiry_seconds: int = 86400

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore", use_enum_values=True)  # type: ignore


class FeatureFlag(BaseModel):
    name: str
    enabled: bool = False
    rollout_percentage: int = 100
    rollout_strategy: str = "consistent"

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")  # type: ignore


class TadawulConfig(BaseModel):
    environment: Environment = Environment.PRODUCTION
    service_name: str = "tadawul-fast-bridge"
    service_version: str = CONFIG_VERSION

    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    vault: VaultConfig = Field(default_factory=VaultConfig)
    aws: AWSConfig = Field(default_factory=AWSConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    tls: TLSConfig = Field(default_factory=TLSConfig)

    config_providers: List[ConfigProvider] = Field(
        default_factory=lambda: [
            ConfigProvider.LOCAL,
            ConfigProvider.ENV,
            ConfigProvider.REDIS,
            ConfigProvider.VAULT,
        ]
    )

    tokens: List[SecretStr] = Field(default_factory=list)
    features: Dict[str, FeatureFlag] = Field(default_factory=dict)

    batch_size: int = 25
    batch_concurrency: int = 6
    route_timeout_sec: float = 120.0

    encryption_algorithm: str = "AES-256-GCM"

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore", use_enum_values=True, validate_assignment=True)  # type: ignore

    def get_tokens(self) -> List[str]:
        out: List[str] = []
        for t in self.tokens:
            try:
                v = t.get_secret_value()
            except Exception:
                v = str(t)
            if v:
                out.append(v)
        return out


# =============================================================================
# Utilities
# =============================================================================
def dict_diff(old: Dict[str, Any], new: Dict[str, Any], path: str = "") -> Dict[str, Any]:
    diffs: Dict[str, Any] = {}
    for k in old:
        current_path = f"{path}.{k}" if path else k
        if k not in new:
            diffs[current_path] = {"old": old[k], "new": None, "action": "removed"}
        elif isinstance(old[k], dict) and isinstance(new[k], dict):
            diffs.update(dict_diff(old[k], new[k], current_path))
        elif old[k] != new[k]:
            diffs[current_path] = {"old": old[k], "new": new[k], "action": "updated"}
    for k in new:
        current_path = f"{path}.{k}" if path else k
        if k not in old:
            diffs[current_path] = {"old": None, "new": new[k], "action": "added"}
    return diffs


class ConfigEncryption:
    """
    AESGCM uses a raw 32-byte key.
    Fernet uses a urlsafe base64-encoded 32-byte key.
    """

    def __init__(self, raw_key: Optional[bytes] = None):
        self._raw_key = raw_key or self._derive_raw_key()
        self._fernet_key = base64.urlsafe_b64encode(self._raw_key)

        self._aesgcm = AESGCM(self._raw_key) if _CRYPTO_AVAILABLE and AESGCM is not None else None
        self._fernet = Fernet(self._fernet_key) if _CRYPTO_AVAILABLE and Fernet is not None else None

    def _derive_raw_key(self) -> bytes:
        salt = os.getenv("CONFIG_SALT", "tadawul-5.4-salt").encode("utf-8")
        password = os.getenv("CONFIG_PASSWORD", "tadawul-ultra-secret").encode("utf-8")
        if _CRYPTO_AVAILABLE and PBKDF2HMAC is not None and hashes is not None:
            kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100_000)
            return kdf.derive(password)
        # fallback (not ideal, but deterministic)
        return hashlib.sha256(password + salt).digest()

    def decrypt_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        sensitive = {"token", "password", "secret", "key", "credential"}
        out: Dict[str, Any] = {}
        for k, v in data.items():
            if isinstance(v, dict):
                out[k] = self.decrypt_dict(v)
                continue

            if isinstance(v, str) and any(s in k.lower() for s in sensitive) and len(v) > 32:
                # Try AESGCM payload: base64(nonce[12] + ciphertext)
                try:
                    raw = base64.b64decode(v.encode("utf-8"))
                    if self._aesgcm and len(raw) > 12:
                        nonce, ct = raw[:12], raw[12:]
                        out[k] = self._aesgcm.decrypt(nonce, ct, None).decode("utf-8")
                        continue
                except Exception:
                    pass

                # Try Fernet token (base64-urlsafe)
                try:
                    if self._fernet:
                        out[k] = self._fernet.decrypt(v.encode("utf-8")).decode("utf-8")
                        continue
                except Exception:
                    pass

            out[k] = v
        return out

    def encrypt_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        sensitive = {"token", "password", "secret", "key", "credential"}
        out: Dict[str, Any] = {}
        for k, v in data.items():
            if isinstance(v, dict):
                out[k] = self.encrypt_dict(v)
                continue

            if isinstance(v, str) and any(s in k.lower() for s in sensitive) and v:
                if self._aesgcm:
                    nonce = os.urandom(12)
                    ct = self._aesgcm.encrypt(nonce, v.encode("utf-8"), None)
                    out[k] = base64.b64encode(nonce + ct).decode("utf-8")
                    continue
                if self._fernet:
                    out[k] = self._fernet.encrypt(v.encode("utf-8")).decode("utf-8")
                    continue

            out[k] = v
        return out


class CircuitBreaker:
    def __init__(self, name: str, threshold: int = 5, timeout: float = 60.0):
        self.name = name
        self.threshold = threshold
        self.timeout = timeout
        self.state = CircuitBreakerState.CLOSED
        self.failures = 0
        self.last_failure = 0.0
        self._lock = asyncio.Lock()

    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure > self.timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    logger.info("Circuit %s entering HALF_OPEN", self.name)
                else:
                    raise Exception(f"Circuit {self.name} is OPEN")

        try:
            res = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            async with self._lock:
                self.state = CircuitBreakerState.CLOSED
                self.failures = 0
                config_circuit_breakers.labels(provider=self.name).set(0)
            return res
        except Exception as e:
            async with self._lock:
                self.failures += 1
                self.last_failure = time.time()
                if self.failures >= self.threshold:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning("Circuit %s OPEN after %s failures", self.name, self.failures)
                    config_circuit_breakers.labels(provider=self.name).set(2)
            raise e


def _parse_redis_url(redis_url: str) -> Tuple[str, int, Optional[str], int]:
    """
    Returns: host, port, password, db
    """
    u = urlparse(redis_url)
    host = u.hostname or "localhost"
    port = int(u.port or 6379)
    password = u.password
    db = int((u.path or "/0").lstrip("/") or 0)
    return host, port, password, db


# =============================================================================
# Config Manager
# =============================================================================
class ConfigManager:
    def __init__(self):
        self._config: Optional[TadawulConfig] = None
        self._config_lock = asyncio.Lock()
        self._encryption = ConfigEncryption()
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._version = 0
        self._initialized = False
        self._history = deque(maxlen=20)
        self._last_update: Optional[datetime] = None

        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="ConfigWorker")
        self._pubsub_task: Optional[asyncio.Task] = None
        self._watch_task: Optional[asyncio.Task] = None

    async def initialize(self) -> None:
        if self._initialized:
            return

        async with self._config_lock:
            if self._initialized:
                return

            try:
                default_model = TadawulConfig()
                base_dict = self._load_from_env()

                # fetch external providers in parallel
                tasks = []
                for p_type in default_model.config_providers:
                    if p_type in {ConfigProvider.LOCAL, ConfigProvider.ENV}:
                        continue
                    tasks.append(self._load_from_provider(p_type, default_model))

                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for r in results:
                        if isinstance(r, dict) and r:
                            base_dict = self._deep_merge(base_dict, r)

                # decrypt in threadpool (may be expensive)
                loop = asyncio.get_running_loop()
                decrypted = await loop.run_in_executor(self._executor, self._encryption.decrypt_dict, base_dict)

                self._config = TadawulConfig.model_validate(decrypted) if _PYDANTIC_V2 else TadawulConfig(**decrypted)
                self._version += 1
                self._last_update = datetime.now(timezone.utc)
                self._initialized = True

                config_version_gauge.labels(environment=self._config.environment.value).set(self._version)
                config_reloads_total.inc()

                logger.info("Configuration v%s initialized.", self._version)

                self._start_background_tasks()

            except Exception as e:
                logger.error("Config initialization failure: %s", e)
                self._config = TadawulConfig()
                self._initialized = True

    def _start_background_tasks(self) -> None:
        if not self._watch_task or self._watch_task.done():
            self._watch_task = asyncio.create_task(self._watch_config())

        # Redis pubsub only if we can create a client
        if not self._pubsub_task or self._pubsub_task.done():
            if _REDIS_ASYNC_AVAILABLE or _AIOREDIS_AVAILABLE:
                self._pubsub_task = asyncio.create_task(self._listen_redis_pubsub())

    def _load_from_env(self) -> Dict[str, Any]:
        cfg: Dict[str, Any] = {}

        env = os.getenv("APP_ENV", "production").lower()
        if env in {e.value for e in Environment}:
            cfg["environment"] = env

        # database
        if os.getenv("DB_HOST"):
            cfg["database"] = {
                "host": os.getenv("DB_HOST", "localhost"),
                "port": int(os.getenv("DB_PORT", "5432")),
                "name": os.getenv("DB_NAME", "tadawul"),
                "user": os.getenv("DB_USER", "postgres"),
                "password": os.getenv("DB_PASSWORD", ""),
            }

        # redis (Render provides REDIS_URL)
        redis_url = os.getenv("REDIS_URL", "").strip()
        if redis_url:
            host, port, password, db = _parse_redis_url(redis_url)
            cfg["redis"] = {
                "host": host,
                "port": port,
                "password": password or "",
                "db": db,
            }

        # auth config
        cfg.setdefault("auth", {})
        cfg["auth"]["header_name"] = os.getenv("AUTH_HEADER_NAME", "X-APP-TOKEN")
        cfg["auth"]["allow_query_token"] = os.getenv("ALLOW_QUERY_TOKEN", "0").strip().lower() in {"1", "true", "yes", "on"}

        # tokens: accept APP_TOKEN / BACKUP_APP_TOKEN / APP_TOKENS
        tokens: List[str] = []
        for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
            v = os.getenv(k, "").strip()
            if v:
                tokens.append(v)
        if os.getenv("APP_TOKENS"):
            tokens.extend([t.strip() for t in os.getenv("APP_TOKENS", "").split(",") if t.strip()])
        if tokens:
            # store as list of strings; pydantic will coerce to SecretStr
            cfg["tokens"] = tokens

        # aws env pass-through (optional)
        aws_cfg: Dict[str, Any] = {}
        if os.getenv("AWS_ACCESS_KEY_ID"):
            aws_cfg["access_key_id"] = os.getenv("AWS_ACCESS_KEY_ID", "")
        if os.getenv("AWS_SECRET_ACCESS_KEY"):
            aws_cfg["secret_access_key"] = os.getenv("AWS_SECRET_ACCESS_KEY", "")
        if os.getenv("AWS_REGION"):
            aws_cfg["region"] = os.getenv("AWS_REGION", "")
        if aws_cfg:
            cfg["aws"] = aws_cfg

        return cfg

    async def _load_from_provider(self, provider: ConfigProvider, default_model: TadawulConfig) -> Dict[str, Any]:
        cb = self._circuit_breakers.setdefault(provider.value, CircuitBreaker(provider.value))

        async def _fetch_blocking_in_executor() -> Dict[str, Any]:
            def blocking() -> Dict[str, Any]:
                if provider == ConfigProvider.VAULT and _VAULT_AVAILABLE:
                    vc = default_model.vault
                    token = vc.token.get_secret_value() if vc.token else ""
                    client = hvac.Client(url=vc.url, token=token)
                    if client.is_authenticated():
                        # example path (adjust to your vault structure)
                        res = client.secrets.kv.v2.read_secret_version(path="tadawul/config")
                        return res.get("data", {}).get("data", {}) or {}
                    return {}

                if provider == ConfigProvider.AWS_SECRETS and _AWS_AVAILABLE:
                    ac = default_model.aws
                    session = boto3.Session(
                        aws_access_key_id=ac.access_key_id.get_secret_value() if ac.access_key_id else None,
                        aws_secret_access_key=ac.secret_access_key.get_secret_value() if ac.secret_access_key else None,
                        region_name=ac.region,
                    )
                    client = session.client("secretsmanager")
                    secret_id = f"{ac.secrets_manager_prefix}/config"
                    val = client.get_secret_value(SecretId=secret_id)
                    return json_loads(val.get("SecretString", "{}"))

                return {}

            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(self._executor, blocking)

        async def _fetch_with_retries() -> Dict[str, Any]:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    return await _fetch_blocking_in_executor()
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise e
                    base = 2 ** attempt
                    jitter = random.uniform(0, base)
                    await asyncio.sleep(min(5.0, base + jitter))
            return {}

        async with TraceContext(f"config_fetch_{provider.value}"):
            try:
                res = await cb.execute(_fetch_with_retries)
                config_requests_total.labels(operation=f"fetch_{provider.value}", status="success").inc()
                return res if isinstance(res, dict) else {}
            except Exception as e:
                logger.debug("Provider %s fetch failed: %s", provider.value, e)
                config_requests_total.labels(operation=f"fetch_{provider.value}", status="failure").inc()
                return {}

    def _deep_merge(self, base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
        res = dict(base)
        for k, v in overlay.items():
            if isinstance(v, dict) and isinstance(res.get(k), dict):
                res[k] = self._deep_merge(cast(Dict[str, Any], res[k]), v)
            elif isinstance(v, list) and isinstance(res.get(k), list):
                # stable unique merge (preserve order)
                seen = set()
                out: List[Any] = []
                for item in list(res[k]) + v:  # type: ignore
                    key = json_dumps(item, default=str) if isinstance(item, (dict, list)) else str(item)
                    if key not in seen:
                        seen.add(key)
                        out.append(item)
                res[k] = out
            else:
                res[k] = v
        return res

    async def _listen_redis_pubsub(self) -> None:
        """
        PubSub channel: 'tadawul_config_updates'
        Works with redis.asyncio (preferred) and aioredis fallback.
        """
        channel_name = "tadawul_config_updates"
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

        try:
            if _REDIS_ASYNC_AVAILABLE and redis_async is not None:
                client = redis_async.from_url(redis_url, decode_responses=True)
                pubsub = client.pubsub()
                await pubsub.subscribe(channel_name)
                async for msg in pubsub.listen():
                    if msg and msg.get("type") == "message":
                        logger.info("Received config update signal: %s", msg.get("data"))
                        await self._force_reload()
            elif _AIOREDIS_AVAILABLE and aioredis is not None:
                client = aioredis.from_url(redis_url, decode_responses=True)  # type: ignore
                pubsub = client.pubsub()
                await pubsub.subscribe(channel_name)
                async for msg in pubsub.listen():
                    if msg and msg.get("type") == "message":
                        logger.info("Received config update signal: %s", msg.get("data"))
                        await self._force_reload()
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.warning("Config PubSub listener died: %s", e)

    async def _force_reload(self) -> None:
        self._initialized = False
        await self.initialize()

    async def _watch_config(self) -> None:
        """
        Poll fallback (slow) — PubSub should handle most reloads.
        """
        while True:
            try:
                await asyncio.sleep(120)
                if not self._config:
                    continue

                new_dict = self._load_from_env()
                # If you later add providers in config_providers, merge them here as well.

                loop = asyncio.get_running_loop()
                decrypted = await loop.run_in_executor(self._executor, self._encryption.decrypt_dict, new_dict)

                old_dict = self._config.model_dump(mode="python") if _PYDANTIC_V2 else self._config.dict()
                diff = dict_diff(old_dict, decrypted)
                if diff:
                    logger.info("Config changes detected via polling: %s", diff)
                    async with self._config_lock:
                        self._config = TadawulConfig.model_validate(decrypted) if _PYDANTIC_V2 else TadawulConfig(**decrypted)
                        self._version += 1
                        self._last_update = datetime.now(timezone.utc)
                        config_reloads_total.inc()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Config polling watcher failed: %s", e)

    async def get_config(self) -> TadawulConfig:
        if not self._initialized:
            await self.initialize()
        return cast(TadawulConfig, self._config or TadawulConfig())

    async def close(self) -> None:
        if self._pubsub_task:
            self._pubsub_task.cancel()
        if self._watch_task:
            self._watch_task.cancel()
        self._executor.shutdown(wait=False)


_config_manager = ConfigManager()

# =============================================================================
# Public Interface (Legacy Shim)
# =============================================================================

def get_settings() -> TadawulConfig:
    """
    Thread-safe settings getter.
    If called inside a running event loop before init, returns defaults (non-blocking).
    """
    if _config_manager._initialized and _config_manager._config is not None:
        return _config_manager._config

    try:
        loop = asyncio.get_running_loop()
        if loop.is_running():
            logger.warning("get_settings called inside running loop before initialization; returning defaults.")
            return TadawulConfig()
    except RuntimeError:
        pass

    return asyncio.run(_config_manager.get_config())

async def get_config_api() -> TadawulConfig:
    return await _config_manager.get_config()

def allowed_tokens() -> List[str]:
    return get_settings().get_tokens()

def require_auth() -> bool:
    return os.getenv("REQUIRE_AUTH", "0").strip().lower() in {"1", "true", "yes", "on"}

def is_open_mode() -> bool:
    """
    ✅ FIX:
    - If REQUIRE_AUTH=true and no tokens configured => NOT open mode (deny by default).
    - If REQUIRE_AUTH=false and no tokens => open mode.
    """
    toks = allowed_tokens()
    if not toks and require_auth():
        return False
    return len(toks) == 0

def auth_ok_request(*, x_app_token: Optional[str] = None, authorization: Optional[str] = None, query_token: Optional[str] = None) -> bool:
    tokens = set(allowed_tokens())

    # if auth required but no tokens -> deny
    if require_auth() and not tokens:
        return False

    # if tokens list is empty and auth not required -> allow
    if not tokens:
        return True

    if x_app_token and x_app_token.strip() in tokens:
        return True
    if authorization and authorization.startswith("Bearer ") and authorization[7:].strip() in tokens:
        return True

    # allow query token only if explicitly enabled
    allow_query = os.getenv("ALLOW_QUERY_TOKEN", "0").strip().lower() in {"1", "true", "yes", "on"}
    if allow_query and query_token and query_token.strip() in tokens:
        return True

    return False

def is_feature_enabled(name: str) -> bool:
    cfg = get_settings()
    feature = cfg.features.get(name)
    return bool(feature.enabled) if feature else False

__all__ = [
    "CONFIG_VERSION",
    "Environment",
    "ConfigProvider",
    "AuthMethod",
    "CircuitBreakerState",
    "TadawulConfig",
    "DatabaseConfig",
    "RedisConfig",
    "VaultConfig",
    "AWSConfig",
    "AuthConfig",
    "get_settings",
    "get_config_api",
    "allowed_tokens",
    "require_auth",
    "is_open_mode",
    "auth_ok_request",
    "is_feature_enabled",
    "ConfigManager",
    "_config_manager",
    "dict_diff",
]
