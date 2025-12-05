"""
google_apps_script_client.py
Enhanced Async Google Apps Script Client for Tadawul Fast Bridge
Version: 4.0.0 - Enhanced configuration and connection management
Aligned with main.py v4.0.0 and environment patterns
"""

import asyncio
import logging
import time
import random
import re
import json
import uuid
from typing import Dict, List, Optional, Any, Union, Tuple
from enum import Enum
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import aiohttp
from pydantic import BaseModel, Field, validator
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

# Configure logger
logger = logging.getLogger(__name__)

# ======================================================================
# Configuration Models (Aligned with main.py)
# ======================================================================


class ClientConfig(BaseModel):
    """Client configuration aligned with main application settings and env vars."""

    base_url: str = Field(..., description="Primary Google Apps Script URL")
    backup_url: Optional[str] = Field(None, description="Backup URL for failover")
    timeout: int = Field(30, ge=5, le=120, description="Request timeout in seconds")
    max_retries: int = Field(3, ge=0, le=10, description="Maximum retry attempts")
    rate_limit_rpm: int = Field(60, ge=10, le=300, description="Requests per minute limit")
    circuit_breaker_threshold: int = Field(
        5, ge=1, le=20, description="Circuit breaker failure threshold"
    )
    app_token: Optional[str] = Field(None, description="Authentication token")
    user_agent: str = Field("TadawulFastBridge/4.0.0", description="User agent string")
    pool_size: int = Field(20, ge=5, le=100, description="Connection pool size")
    keepalive_timeout: int = Field(30, ge=10, le=120, description="Keepalive timeout")
    max_redirects: int = Field(5, ge=0, le=10, description="Maximum redirects")

    @validator("base_url")
    def validate_base_url(cls, v: str) -> str:
        """Validate Google Apps Script URL pattern."""
        pattern = re.compile(
            r"^https://script\.google\.com/macros/s/[A-Za-z0-9_-]+/exec$"
        )
        if not pattern.match(v):
            raise ValueError("Invalid Google Apps Script URL format")
        return v

    @classmethod
    def from_env(cls) -> "ClientConfig":
        """Create configuration from environment variables aligned with main.py."""
        import os

        timeout = int(os.getenv("HTTP_TIMEOUT", "30"))
        max_retries = int(os.getenv("HTTP_MAX_RETRIES", "3"))
        rate_limit_rpm = int(os.getenv("RATE_LIMIT_RPM", "60"))
        cb_threshold = int(os.getenv("CIRCUIT_BREAKER_THRESHOLD", "5"))

        # Token priority aligned with main.py AppSettings
        app_token = (
            os.getenv("APP_TOKEN")
            or os.getenv("BACKUP_APP_TOKEN")
            or os.getenv("TFB_APP_TOKEN")
        )

        return cls(
            base_url=os.getenv("GOOGLE_APPS_SCRIPT_URL", ""),
            backup_url=os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL"),
            timeout=timeout,
            max_retries=max_retries,
            rate_limit_rpm=rate_limit_rpm,
            circuit_breaker_threshold=cb_threshold,
            app_token=app_token,
            user_agent=os.getenv("USER_AGENT", "TadawulFastBridge/4.0.0"),
            pool_size=int(os.getenv("CONNECTION_POOL_SIZE", "20")),
            keepalive_timeout=int(os.getenv("KEEPALIVE_TIMEOUT", "30")),
            max_redirects=int(os.getenv("MAX_REDIRECTS", "5")),
        )


# ======================================================================
# Data Models (Enhanced)
# ======================================================================


class RequestAction(str, Enum):
    """Standardized actions for Google Apps Script requests."""

    GET = "get"
    UPDATE = "update"
    PING = "ping"
    LIST = "list"
    BATCH = "batch"
    HEALTH = "health"
    REFRESH = "refresh"
    METADATA = "metadata"
    VALIDATE = "validate"
    SEARCH = "search"


class ErrorType(str, Enum):
    """Enhanced error classification."""

    NETWORK = "network_error"
    TIMEOUT = "timeout_error"
    AUTH = "authentication_error"
    RATE_LIMIT = "rate_limit_error"
    SERVER = "server_error"
    CLIENT = "client_error"
    PARSING = "parsing_error"
    CIRCUIT_BREAKER = "circuit_breaker_error"
    VALIDATION = "validation_error"
    CONNECTION = "connection_error"
    DNS = "dns_error"
    SSL = "ssl_error"
    UNKNOWN = "unknown_error"


class HealthState(str, Enum):
    """Health states for circuit breaker."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    CIRCUIT_OPEN = "circuit_open"
    RECOVERING = "recovering"


@dataclass
class RequestMetrics:
    """Metrics for a single request."""

    request_id: str
    start_time: float
    end_time: Optional[float] = None
    attempt_count: int = 0
    url_used: Optional[str] = None
    response_size: Optional[int] = None
    success: Optional[bool] = None

    @property
    def duration(self) -> Optional[float]:
        """Calculate request duration."""
        if self.end_time is not None:
            return self.end_time - self.start_time
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "request_id": self.request_id,
            "duration": self.duration,
            "attempt_count": self.attempt_count,
            "url_used": self.url_used,
            "response_size": self.response_size,
            "success": self.success,
        }


@dataclass
class GoogleAppsScriptResponse:
    """Enhanced response wrapper with diagnostics."""

    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    error_type: Optional[ErrorType] = None
    execution_time: Optional[float] = None
    status_code: Optional[int] = None
    retries: int = 0
    request_id: Optional[str] = None
    timestamp: Optional[str] = None
    health_state: Optional[HealthState] = None
    source_url: Optional[str] = None
    metrics: Optional[RequestMetrics] = None
    cache_hit: bool = False
    version: str = "4.0.0"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        if self.error_type:
            result["error_type"] = self.error_type.value
        if self.health_state:
            result["health_state"] = self.health_state.value
        if self.metrics:
            result["metrics"] = self.metrics.to_dict()
        # Remove None values
        return {k: v for k, v in result.items() if v is not None}

    @property
    def is_from_cache(self) -> bool:
        """Check if response is from cache."""
        return self.cache_hit

    def raise_for_status(self):
        """Raise an exception if request was not successful."""
        if not self.success:
            error_msg = self.error or "Request failed"
            error_type = self.error_type or ErrorType.UNKNOWN
            raise RequestError(
                message=error_msg,
                error_type=error_type,
                status_code=self.status_code,
                request_id=self.request_id,
            )


class RequestError(Exception):
    """Custom exception for Google Apps Script requests."""

    def __init__(
        self,
        message: str,
        error_type: ErrorType,
        status_code: Optional[int] = None,
        request_id: Optional[str] = None,
        url: Optional[str] = None,
    ):
        self.message = message
        self.error_type = error_type
        self.status_code = status_code
        self.request_id = request_id
        self.url = url
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format error message."""
        parts = [f"{self.error_type.value}: {self.message}"]
        if self.status_code:
            parts.append(f"Status: {self.status_code}")
        if self.request_id:
            parts.append(f"Request ID: {self.request_id}")
        if self.url:
            parts.append(f"URL: {self.url}")
        return " | ".join(parts)


# ======================================================================
# Async Circuit Breaker (Enhanced)
# ======================================================================


class AsyncCircuitBreaker:
    """Enhanced async circuit breaker with metrics and recovery strategies."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        half_open_max_requests: int = 3,
        metrics_window: int = 100,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_requests = half_open_max_requests

        self.state = HealthState.HEALTHY
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self.last_success_time: Optional[float] = None
        self.half_open_attempts = 0

        # Metrics tracking
        self.metrics_window = metrics_window
        self.recent_failures: List[float] = []
        self.recent_successes: List[float] = []

        self._lock = asyncio.Lock()

        logger.info(
            "AsyncCircuitBreaker initialized: "
            f"threshold={failure_threshold}, recovery={recovery_timeout}s"
        )

    async def can_execute(self) -> Tuple[bool, HealthState]:
        """Check if request can be executed based on current circuit state."""
        async with self._lock:
            now = time.time()

            if self.state == HealthState.CIRCUIT_OPEN:
                if self.last_failure_time and (now - self.last_failure_time) > self.recovery_timeout:
                    # Transition to half-open (recovering) state
                    self.state = HealthState.RECOVERING
                    self.half_open_attempts = 0
                    logger.info("Circuit transitioning to RECOVERING state")
                    return True, self.state
                return False, self.state

            if self.state == HealthState.RECOVERING:
                if self.half_open_attempts >= self.half_open_max_requests:
                    # Too many failures in recovery, open circuit
                    self.state = HealthState.CIRCUIT_OPEN
                    self.last_failure_time = now
                    logger.warning("Circuit re-opened due to recovery failures")
                    return False, self.state
                return True, self.state

            if self.state == HealthState.UNHEALTHY:
                # Allow execution but with degraded state
                return True, self.state

            # HEALTHY
            return True, self.state

    async def on_success(self) -> None:
        """Record successful execution."""
        async with self._lock:
            now = time.time()
            self.success_count += 1
            self.last_success_time = now
            self.recent_successes.append(now)

            self._trim_metrics()

            if self.state == HealthState.RECOVERING:
                self.half_open_attempts += 1
                if self.half_open_attempts >= self.half_open_max_requests:
                    self.state = HealthState.HEALTHY
                    self.failure_count = 0
                    self.half_open_attempts = 0
                    logger.info("Circuit recovered to HEALTHY state")

            elif self.state == HealthState.UNHEALTHY:
                # Gradual recovery
                self.failure_count = max(0, self.failure_count - 2)
                if self.failure_count < self.failure_threshold // 2:
                    self.state = HealthState.HEALTHY
                    logger.info("Circuit improved to HEALTHY state")

    async def on_failure(self) -> None:
        """Record failed execution."""
        async with self._lock:
            now = time.time()
            self.failure_count += 1
            self.last_failure_time = now
            self.recent_failures.append(now)

            self._trim_metrics()

            if self.state == HealthState.RECOVERING:
                self.half_open_attempts += 1
                if self.half_open_attempts >= self.half_open_max_requests:
                    self.state = HealthState.CIRCUIT_OPEN
                    logger.error("Circuit opened during recovery")

            elif self.state == HealthState.HEALTHY and self.failure_count >= self.failure_threshold:
                self.state = HealthState.UNHEALTHY
                logger.warning(f"Circuit degraded: {self.failure_count} failures")

            elif self.state == HealthState.UNHEALTHY and self.failure_count >= (
                self.failure_threshold * 2
            ):
                self.state = HealthState.CIRCUIT_OPEN
                logger.error(f"Circuit opened: {self.failure_count} failures")

    def _trim_metrics(self) -> None:
        """Trim old metrics to keep window size."""
        window_start = time.time() - 3600  # 1 hour window

        self.recent_failures = [f for f in self.recent_failures if f > window_start]
        self.recent_successes = [s for s in self.recent_successes if s > window_start]

        if len(self.recent_failures) > self.metrics_window:
            self.recent_failures = self.recent_failures[-self.metrics_window :]
        if len(self.recent_successes) > self.metrics_window:
            self.recent_successes = self.recent_successes[-self.metrics_window :]

    async def get_state(self) -> Dict[str, Any]:
        """Get detailed circuit breaker state."""
        async with self._lock:
            now = time.time()
            recent_failure_rate = (
                len(self.recent_failures) / self.metrics_window if self.metrics_window > 0 else 0
            )
            recent_success_rate = (
                len(self.recent_successes) / self.metrics_window if self.metrics_window > 0 else 0
            )

            can_execute = True
            if self.state == HealthState.CIRCUIT_OPEN:
                can_execute = False
            elif self.state == HealthState.RECOVERING:
                can_execute = self.half_open_attempts < self.half_open_max_requests

            return {
                "state": self.state.value,
                "failure_count": self.failure_count,
                "success_count": self.success_count,
                "half_open_attempts": self.half_open_attempts,
                "last_failure_time": self.last_failure_time,
                "last_success_time": self.last_success_time,
                "recent_failure_rate": round(recent_failure_rate, 3),
                "recent_success_rate": round(recent_success_rate, 3),
                "can_execute": can_execute,
                "recovery_timeout": self.recovery_timeout,
                "failure_threshold": self.failure_threshold,
                "timestamp": now,
            }

    async def reset(self) -> None:
        """Reset circuit breaker to initial state."""
        async with self._lock:
            self.state = HealthState.HEALTHY
            self.failure_count = 0
            self.success_count = 0
            self.last_failure_time = None
            self.last_success_time = None
            self.half_open_attempts = 0
            self.recent_failures.clear()
            self.recent_successes.clear()
            logger.info("Circuit breaker reset")


# ======================================================================
# Enhanced Async Google Apps Script Client
# ======================================================================


class GoogleAppsScriptClient:
    """
    Enhanced async HTTP client for Google Apps Script endpoints.
    Aligned with main.py v4.0.0 and environment configuration.
    """

    GAS_URL_PATTERN = re.compile(
        r"^https://script\.google\.com/macros/s/[A-Za-z0-9_-]+/exec$"
    )

    def __init__(self, config: Optional[ClientConfig] = None) -> None:
        # Load configuration
        self.config = config or ClientConfig.from_env()

        # Validate URLs
        self._validate_urls()

        # Rate limiting
        self.request_timestamps: List[float] = []
        self._rate_limit_lock = asyncio.Lock()

        # Circuit breaker
        self.circuit_breaker = AsyncCircuitBreaker(
            failure_threshold=self.config.circuit_breaker_threshold,
            recovery_timeout=60,
            half_open_max_requests=2,
            metrics_window=100,
        )

        # Statistics
        self.request_counter = 0
        self.error_counter = 0
        self.success_counter = 0
        self.cache_hits = 0
        self._stats_lock = asyncio.Lock()

        # Simple in-memory cache
        self._cache: Dict[str, Tuple[GoogleAppsScriptResponse, float]] = {}
        self._cache_lock = asyncio.Lock()

        # HTTP session
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

        # Request history
        self._request_history: List[RequestMetrics] = []
        self._max_history = 1000

        logger.info(
            "GoogleAppsScriptClient initialized: "
            f"base_url={self._mask_url(self.config.base_url)}, "
            f"timeout={self.config.timeout}s, retries={self.config.max_retries}"
        )

    def _validate_urls(self) -> None:
        """Validate configured URLs."""
        if not self.config.base_url:
            raise ValueError("GOOGLE_APPS_SCRIPT_URL is required")

        if not self.GAS_URL_PATTERN.match(self.config.base_url):
            logger.warning(
                f"Base URL may not be valid: {self._mask_url(self.config.base_url)}"
            )

        if self.config.backup_url and not self.GAS_URL_PATTERN.match(
            self.config.backup_url
        ):
            logger.warning(
                f"Backup URL may not be valid: {self._mask_url(self.config.backup_url)}"
            )

    def _mask_url(self, url: Optional[str]) -> str:
        """Mask URL for secure logging."""
        if not url:
            return "***"

        try:
            from urllib.parse import urlparse

            parsed = urlparse(url)
            if parsed.hostname and "google.com" in parsed.hostname:
                path_parts = parsed.path.split("/")
                if len(path_parts) > 3:
                    script_id = path_parts[3]
                    if script_id:
                        masked_id = (
                            script_id[:4] + "..." + script_id[-4:]
                            if len(script_id) > 8
                            else script_id
                        )
                        return (
                            f"https://script.google.com/macros/s/"
                            f"{masked_id}/exec"
                        )
        except Exception:
            pass

        return "https://script.google.com/macros/s/.../exec"

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session with optimized configuration."""
        async with self._session_lock:
            if self._session is None or self._session.closed:
                timeout = aiohttp.ClientTimeout(
                    total=self.config.timeout,
                    connect=10,
                    sock_read=30,
                )

                connector = aiohttp.TCPConnector(
                    limit=self.config.pool_size,
                    limit_per_host=max(1, self.config.pool_size // 2),
                    ssl=True,
                    keepalive_timeout=self.config.keepalive_timeout,
                    enable_cleanup_closed=True,
                )

                headers = {
                    "User-Agent": self.config.user_agent,
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip, deflate",
                    "Content-Type": "application/json",
                    "Cache-Control": "no-cache",
                }

                # App token → multiple headers (more flexible)
                if self.config.app_token:
                    token = self.config.app_token.strip()
                    headers["Authorization"] = f"Bearer {token}"
                    headers["X-APP-TOKEN"] = token
                    headers["X-TFB-Token"] = token

                self._session = aiohttp.ClientSession(
                    timeout=timeout,
                    connector=connector,
                    headers=headers,
                    auto_decompress=True,
                )

                logger.debug("Created new HTTP session for GoogleAppsScriptClient")

            return self._session

    async def _enforce_rate_limit(self) -> None:
        """Enhanced async rate limiting with adaptive window."""
        async with self._rate_limit_lock:
            now = time.time()
            window_start = now - 60  # 1 minute

            self.request_timestamps = [
                ts for ts in self.request_timestamps if ts > window_start
            ]

            if len(self.request_timestamps) >= self.config.rate_limit_rpm:
                oldest_timestamp = self.request_timestamps[0]
                sleep_time = 60 - (now - oldest_timestamp)

                if sleep_time > 0:
                    over_limit = (
                        len(self.request_timestamps) - self.config.rate_limit_rpm
                    )
                    jitter = random.uniform(0.1, 0.5) * (over_limit + 1)

                    logger.warning(
                        "Rate limit exceeded by %s requests, sleeping for %.2fs",
                        over_limit,
                        sleep_time + jitter,
                    )

                    await asyncio.sleep(sleep_time + jitter)

                    now = time.time()
                    window_start = now - 60
                    self.request_timestamps = [
                        ts for ts in self.request_timestamps if ts > window_start
                    ]

            # Small jitter and record this request
            await asyncio.sleep(random.uniform(0.01, 0.05))
            self.request_timestamps.append(now)

    def _classify_error(
        self, error: Exception, status_code: Optional[int] = None
    ) -> ErrorType:
        """Enhanced error classification."""
        if isinstance(error, (asyncio.TimeoutError, aiohttp.ServerTimeoutError)):
            return ErrorType.TIMEOUT
        if isinstance(error, aiohttp.ClientConnectionError):
            return ErrorType.CONNECTION
        if isinstance(error, aiohttp.ClientConnectorError):
            if "SSL" in str(error):
                return ErrorType.SSL
            return ErrorType.NETWORK
        if isinstance(error, aiohttp.ClientResponseError):
            if status_code == 429:
                return ErrorType.RATE_LIMIT
            if status_code in (401, 403):
                return ErrorType.AUTH
            if status_code and 500 <= status_code < 600:
                return ErrorType.SERVER
            if status_code and 400 <= status_code < 500:
                return ErrorType.CLIENT
        if isinstance(error, aiohttp.ClientOSError):
            return ErrorType.NETWORK
        if isinstance(error, json.JSONDecodeError):
            return ErrorType.PARSING
        if isinstance(error, ValueError):
            return ErrorType.VALIDATION

        return ErrorType.UNKNOWN

    def _generate_request_id(self) -> str:
        """Generate unique request ID with timestamp and counter."""
        self.request_counter += 1
        timestamp = int(time.time() * 1000)
        unique_id = str(uuid.uuid4())[:8]
        return f"gas_{timestamp}_{self.request_counter:06d}_{unique_id}"

    def _add_to_history(self, metrics: RequestMetrics) -> None:
        """Add request metrics to history."""
        self._request_history.append(metrics)
        if len(self._request_history) > self._max_history:
            self._request_history = self._request_history[-self._max_history :]

    async def _get_cached_response(
        self, cache_key: str, ttl: int = 30
    ) -> Optional[GoogleAppsScriptResponse]:
        """Get cached response if available and not expired."""
        async with self._cache_lock:
            if cache_key in self._cache:
                response, timestamp = self._cache[cache_key]
                if time.time() - timestamp < ttl:
                    self.cache_hits += 1
                    response.cache_hit = True
                    logger.debug("Cache hit for key: %s", cache_key)
                    return response
                # expired
                del self._cache[cache_key]
        return None

    async def _cache_response(
        self, cache_key: str, response: GoogleAppsScriptResponse, ttl: int = 30
    ) -> None:
        """Cache response with TTL."""
        async with self._cache_lock:
            self._cache[cache_key] = (response, time.time())
            if len(self._cache) > 1000:
                sorted_items = sorted(self._cache.items(), key=lambda x: x[1][1])
                for key, _ in sorted_items[:100]:
                    del self._cache[key]

    @retry(
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    async def _make_request_attempt(
        self,
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        metrics: RequestMetrics = None,
    ) -> Tuple[Optional[int], Optional[str], Optional[Exception]]:
        """Make a single request attempt with retry decorator."""
        try:
            metrics.attempt_count += 1

            if method.upper() == "GET":
                async with session.get(url, params=params) as response:
                    status_code = response.status
                    response_text = await response.text()
                    metrics.response_size = len(response_text)
                    return status_code, response_text, None
            else:
                async with session.post(url, json=json_data) as response:
                    status_code = response.status
                    response_text = await response.text()
                    metrics.response_size = len(response_text)
                    return status_code, response_text, None

        except Exception as e:
            return None, None, e

    async def _make_request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        action: str = "unknown",
        use_cache: bool = False,
        cache_ttl: int = 30,
    ) -> GoogleAppsScriptResponse:
        """
        Internal method to make async HTTP requests with comprehensive error handling.
        """
        cache_key = None
        if use_cache and method.upper() == "GET":
            import hashlib

            cache_dict = {"url": url, "params": params, "action": action}
            cache_key = hashlib.md5(
                json.dumps(cache_dict, sort_keys=True).encode()
            ).hexdigest()

            cached_response = await self._get_cached_response(cache_key, cache_ttl)
            if cached_response:
                return cached_response

        can_execute, circuit_state = await self.circuit_breaker.can_execute()
        if not can_execute:
            logger.warning("Request blocked by circuit breaker (state: %s)", circuit_state.value)
            return GoogleAppsScriptResponse(
                success=False,
                error=f"Service temporarily unavailable (circuit {circuit_state.value})",
                error_type=ErrorType.CIRCUIT_BREAKER,
                health_state=circuit_state,
                request_id=self._generate_request_id(),
                timestamp=datetime.now(timezone.utc).isoformat(),
                source_url=self._mask_url(url),
            )

        request_id = self._generate_request_id()
        metrics = RequestMetrics(
            request_id=request_id,
            start_time=time.perf_counter(),
        )

        urls_to_try = [url]
        if self.config.backup_url and url == self.config.base_url:
            urls_to_try.append(self.config.backup_url)

        last_exception: Optional[Exception] = None
        last_status_code: Optional[int] = None
        last_response_text: Optional[str] = None

        for current_url in urls_to_try:
            metrics.url_used = current_url

            for attempt in range(self.config.max_retries + 1):
                try:
                    await self._enforce_rate_limit()
                    session = await self._get_session()

                    logger.debug(
                        "[%s] Attempt %d: %s to %s",
                        request_id,
                        attempt + 1,
                        action,
                        self._mask_url(current_url),
                    )

                    status_code, response_text, error = await self._make_request_attempt(
                        session=session,
                        method=method,
                        url=current_url,
                        params=params,
                        json_data=json_data,
                        metrics=metrics,
                    )

                    if error:
                        last_exception = error
                        continue

                    last_status_code = status_code
                    last_response_text = response_text
                    metrics.end_time = time.perf_counter()

                    if status_code != 200:
                        error_msg = f"HTTP {status_code}: {response_text[:200]}"
                        logger.warning("[%s] HTTP error: %s", request_id, error_msg)

                        await self.circuit_breaker.on_failure()
                        metrics.success = False
                        self._add_to_history(metrics)

                        return GoogleAppsScriptResponse(
                            success=False,
                            error=error_msg,
                            error_type=self._classify_error(
                                aiohttp.ClientResponseError(None, None, status=status_code),
                                status_code,
                            ),
                            status_code=status_code,
                            retries=attempt,
                            request_id=request_id,
                            execution_time=metrics.duration,
                            timestamp=datetime.now(timezone.utc).isoformat(),
                            health_state=circuit_state,
                            source_url=self._mask_url(current_url),
                            metrics=metrics,
                        )

                    try:
                        data = json.loads(response_text)
                    except json.JSONDecodeError as e:
                        logger.warning("[%s] JSON parse error: %s", request_id, e)

                        await self.circuit_breaker.on_failure()
                        metrics.success = False
                        self._add_to_history(metrics)

                        return GoogleAppsScriptResponse(
                            success=False,
                            error=f"Invalid JSON response: {e}",
                            error_type=ErrorType.PARSING,
                            status_code=status_code,
                            retries=attempt,
                            request_id=request_id,
                            execution_time=metrics.duration,
                            timestamp=datetime.now(timezone.utc).isoformat(),
                            health_state=circuit_state,
                            source_url=self._mask_url(current_url),
                            metrics=metrics,
                        )

                    success = data.get("success", True)
                    error_msg = data.get("error") or data.get("message")

                    if success:
                        await self.circuit_breaker.on_success()
                        await self._update_stats(True)
                        metrics.success = True
                        logger.debug("[%s] Request successful", request_id)
                    else:
                        await self.circuit_breaker.on_failure()
                        await self._update_stats(False)
                        metrics.success = False
                        logger.warning(
                            "[%s] Apps Script error: %s", request_id, error_msg
                        )

                    response = GoogleAppsScriptResponse(
                        success=success,
                        data=data if success else None,
                        error=error_msg,
                        error_type=ErrorType.SERVER if not success else None,
                        execution_time=metrics.duration,
                        status_code=status_code,
                        retries=attempt,
                        request_id=request_id,
                        timestamp=datetime.now(timezone.utc).isoformat(),
                        health_state=circuit_state,
                        source_url=self._mask_url(current_url),
                        metrics=metrics,
                    )

                    if use_cache and cache_key and success and method.upper() == "GET":
                        await self._cache_response(cache_key, response, cache_ttl)

                    self._add_to_history(metrics)
                    return response

                except Exception as e:
                    last_exception = e
                    logger.warning(
                        "[%s] Attempt %d failed: %s: %s",
                        request_id,
                        attempt + 1,
                        type(e).__name__,
                        str(e),
                    )

                    error_type = self._classify_error(e, last_status_code)
                    if error_type == ErrorType.CLIENT and last_status_code != 429:
                        break

                    if attempt < self.config.max_retries:
                        sleep_time = (2**attempt) + random.uniform(0.1, 0.5)
                        logger.debug(
                            "[%s] Retrying in %.2fs...", request_id, sleep_time
                        )
                        await asyncio.sleep(sleep_time)

        metrics.end_time = time.perf_counter()
        metrics.success = False
        self._add_to_history(metrics)

        await self.circuit_breaker.on_failure()
        await self._update_stats(False)

        error_type = (
            self._classify_error(last_exception, last_status_code)
            if last_exception
            else ErrorType.UNKNOWN
        )

        return GoogleAppsScriptResponse(
            success=False,
            error=(
                f"All requests failed: {str(last_exception)}"
                if last_exception
                else "All requests failed: Unknown error"
            ),
            error_type=error_type,
            status_code=last_status_code,
            retries=self.config.max_retries,
            request_id=request_id,
            execution_time=metrics.duration,
            timestamp=datetime.now(timezone.utc).isoformat(),
            health_state=circuit_state,
            source_url=self._mask_url(url),
            metrics=metrics,
        )

    async def _update_stats(self, success: bool) -> None:
        """Update success/error statistics."""
        async with self._stats_lock:
            if success:
                self.success_counter += 1
            else:
                self.error_counter += 1

    # ==================================================================
    # Public Async API Methods
    # ==================================================================

    async def get_symbols_data(
        self,
        symbol: Optional[Union[str, List[str]]] = None,
        action: str = "get",
        include_metadata: bool = False,
        use_cache: bool = True,
        cache_ttl: int = 30,
    ) -> GoogleAppsScriptResponse:
        """
        Async symbols data retrieval with caching support.
        """
        if symbol and isinstance(symbol, list) and len(symbol) > 100:
            return GoogleAppsScriptResponse(
                success=False,
                error="Too many symbols (maximum 100 per request)",
                error_type=ErrorType.VALIDATION,
                timestamp=datetime.now(timezone.utc).isoformat(),
            )

        params: Dict[str, Any] = {"action": action}

        if symbol:
            if isinstance(symbol, (list, tuple)):
                params["symbol"] = ",".join(
                    str(s).upper().strip() for s in symbol
                )
            else:
                params["symbol"] = str(symbol).upper().strip()

        if include_metadata:
            params["metadata"] = "true"
            params["source"] = "tadawul_api_v4"

        logger.info(
            "Fetching symbols data: symbol=%s, action=%s, cache=%s",
            symbol if isinstance(symbol, str) else (f"list({len(symbol)})" if symbol else "None"),
            action,
            use_cache,
        )

        return await self._make_request(
            method="GET",
            url=self.config.base_url,
            params=params,
            action=f"get_symbols_{action}",
            use_cache=use_cache,
            cache_ttl=cache_ttl,
        )

    async def update_symbol_data(
        self,
        symbol: str,
        data: Dict[str, Any],
        action: str = "update",
        validate: bool = True,
    ) -> GoogleAppsScriptResponse:
        """
        Async symbol data update with validation.
        """
        if not symbol or not symbol.strip():
            return GoogleAppsScriptResponse(
                success=False,
                error="Symbol cannot be empty",
                error_type=ErrorType.VALIDATION,
                timestamp=datetime.now(timezone.utc).isoformat(),
            )

        if validate:
            validation_error = self._validate_update_data(symbol, data)
            if validation_error:
                return GoogleAppsScriptResponse(
                    success=False,
                    error=validation_error,
                    error_type=ErrorType.VALIDATION,
                    timestamp=datetime.now(timezone.utc).isoformat(),
                )

        payload = {
            "action": action,
            "symbol": symbol.upper().strip(),
            "data": data,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "tadawul_api_v4",
            "version": "4.0.0",
        }

        logger.info(
            "Updating symbol data: %s, data_keys=%s",
            symbol,
            list(data.keys()),
        )

        return await self._make_request(
            method="POST",
            url=self.config.base_url,
            json_data=payload,
            action=f"update_symbol_{action}",
        )

    def _validate_update_data(self, symbol: str, data: Dict[str, Any]) -> Optional[str]:
        """Validate update data for common issues."""
        if not data:
            return "Update data cannot be empty"

        try:
            data_size = len(json.dumps(data))
            if data_size > 50000:
                return f"Data payload too large ({data_size} bytes, max 50000)"
        except Exception:
            pass

        required_fields = ["symbol", "timestamp"]
        for field in required_fields:
            if field not in data:
                data[field] = (
                    symbol.upper().strip()
                    if field == "symbol"
                    else datetime.now(timezone.utc).isoformat()
                )

        return None

    async def batch_update_symbols(
        self,
        updates: List[Dict[str, Any]],
        action: str = "batch_update",
        chunk_size: int = 25,
    ) -> List[GoogleAppsScriptResponse]:
        """
        Async batch update multiple symbols with chunking.
        """
        if not updates:
            return [
                GoogleAppsScriptResponse(
                    success=False,
                    error="Empty updates list",
                    error_type=ErrorType.VALIDATION,
                    timestamp=datetime.now(timezone.utc).isoformat(),
                )
            ]

        if len(updates) > 100:
            logger.warning(
                "Large batch update: %d symbols, will be chunked", len(updates)
            )

        chunks = []
        for i in range(0, len(updates), chunk_size):
            chunk = updates[i : i + chunk_size]

            payload = {
                "action": action,
                "updates": chunk,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "tadawul_api_v4",
                "count": len(chunk),
                "chunk_index": i // chunk_size,
                "total_chunks": (len(updates) + chunk_size - 1) // chunk_size,
            }

            chunks.append(payload)

        tasks = []
        for chunk_payload in chunks:
            task = self._make_request(
                method="POST",
                url=self.config.base_url,
                json_data=chunk_payload,
                action="batch_update_symbols",
            )
            tasks.append(task)

        logger.info(
            "Batch updating %d symbols in %d chunks", len(updates), len(chunks)
        )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Ensure return type is List[GoogleAppsScriptResponse]
        cleaned_results: List[GoogleAppsScriptResponse] = []
        for r in results:
            if isinstance(r, GoogleAppsScriptResponse):
                cleaned_results.append(r)
            elif isinstance(r, Exception):
                cleaned_results.append(
                    GoogleAppsScriptResponse(
                        success=False,
                        error=str(r),
                        error_type=ErrorType.UNKNOWN,
                        timestamp=datetime.now(timezone.utc).isoformat(),
                    )
                )
        return cleaned_results

    async def health_check(
        self,
        detailed: bool = False,
        include_system: bool = False,
    ) -> GoogleAppsScriptResponse:
        """
        Async health check with detailed diagnostics.
        """
        params: Dict[str, Any] = {
            "action": "health",
            "ping": "1",
            "detailed": "true" if detailed else "false",
        }

        logger.debug("Performing async health check")

        response = await self._make_request(
            method="GET",
            url=self.config.base_url,
            params=params,
            action="health_check",
            use_cache=False,
        )

        if response.data is None:
            response.data = {}

        response.data["client"] = {
            "stats": await self.get_statistics(),
            "circuit_breaker": await self.circuit_breaker.get_state(),
            "config": self.config.dict(),
            "version": "4.0.0",
        }

        if include_system:
            response.data["system"] = await self._get_system_metrics()

        return response

    async def _get_system_metrics(self) -> Dict[str, Any]:
        """Get system metrics for monitoring."""
        import psutil
        import platform

        try:
            return {
                "platform": platform.platform(),
                "python_version": platform.python_version(),
                "cpu_percent": psutil.cpu_percent(interval=0.1),
                "memory_percent": psutil.virtual_memory().percent,
                "disk_usage": psutil.disk_usage("/").percent,
                "process": {
                    "memory_mb": psutil.Process().memory_info().rss
                    / 1024
                    / 1024,
                    "cpu_percent": psutil.Process().cpu_percent(interval=0.1),
                    "threads": psutil.Process().num_threads(),
                },
            }
        except Exception as e:
            logger.warning("Failed to get system metrics: %s", e)
            return {"error": str(e)}

    async def test_connection(
        self,
        timeout: Optional[int] = None,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Enhanced async connection test.
        """
        original_timeout = self.config.timeout
        if timeout:
            self.config.timeout = timeout

        try:
            response = await self.health_check(detailed=True)

            diagnostics: Dict[str, Any] = {
                "base_url_accessible": response.success,
                "backup_url_configured": bool(self.config.backup_url),
                "timeout_configured": self.config.timeout,
                "app_token_configured": bool(self.config.app_token),
                "client_stats": await self.get_statistics(),
                "circuit_breaker": await self.circuit_breaker.get_state(),
                "last_health_check": response.timestamp,
                "response_time": response.execution_time,
                "config": {
                    "pool_size": self.config.pool_size,
                    "rate_limit_rpm": self.config.rate_limit_rpm,
                    "max_retries": self.config.max_retries,
                },
            }

            if not response.success:
                diagnostics["error"] = response.error
                diagnostics["error_type"] = (
                    response.error_type.value if response.error_type else "unknown"
                )
                diagnostics["health_state"] = (
                    response.health_state.value if response.health_state else "unknown"
                )

            return response.success, diagnostics

        finally:
            if timeout:
                self.config.timeout = original_timeout

    @asynccontextmanager
    async def request_context(self, action: str = "custom"):
        """Context manager for request with automatic cleanup."""
        request_id = self._generate_request_id()
        metrics = RequestMetrics(
            request_id=request_id,
            start_time=time.perf_counter(),
        )
        logger.debug("[%s] Starting request context: %s", request_id, action)
        try:
            yield {
                "request_id": request_id,
                "metrics": metrics,
                "client": self,
            }
        finally:
            metrics.end_time = time.perf_counter()
            self._add_to_history(metrics)
            logger.debug("[%s] Request context completed: %s", request_id, action)

    # ==================================================================
    # Monitoring and Statistics
    # ==================================================================

    async def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive client usage statistics and health metrics."""
        async with self._stats_lock:
            total_requests = self.success_counter + self.error_counter
            success_rate = (
                self.success_counter / total_requests * 100
                if total_requests > 0
                else 0
            )

            now = time.time()
            window_start = now - 60
            current_window = len(
                [ts for ts in self.request_timestamps if ts > window_start]
            )

            recent_history = (
                self._request_history[-100:]
                if len(self._request_history) > 100
                else self._request_history
            )
            avg_response_time = (
                sum(m.duration or 0 for m in recent_history) / len(recent_history)
                if recent_history
                else 0
            )

            circuit_state = await self.circuit_breaker.get_state()

            return {
                "total_requests": total_requests,
                "successful_requests": self.success_counter,
                "failed_requests": self.error_counter,
                "cache_hits": self.cache_hits,
                "success_rate_percent": round(success_rate, 2),
                "rate_limit": {
                    "rpm": self.config.rate_limit_rpm,
                    "current_window": current_window,
                    "window_utilization": round(
                        current_window / self.config.rate_limit_rpm * 100, 1
                    )
                    if self.config.rate_limit_rpm
                    else 0,
                },
                "performance": {
                    "avg_response_time_ms": round(avg_response_time * 1000, 2)
                    if avg_response_time
                    else 0,
                    "recent_requests": len(recent_history),
                    "max_history": self._max_history,
                },
                "configuration": {
                    "max_retries": self.config.max_retries,
                    "timeout_seconds": self.config.timeout,
                    "pool_size": self.config.pool_size,
                    "keepalive_timeout": self.config.keepalive_timeout,
                },
                "circuit_breaker": circuit_state,
                "urls": {
                    "base_url": self._mask_url(self.config.base_url),
                    "backup_url": self._mask_url(self.config.backup_url)
                    if self.config.backup_url
                    else None,
                },
                "cache": {
                    "size": len(self._cache),
                    "hits": self.cache_hits,
                    "hit_rate": round(
                        self.cache_hits / total_requests * 100, 2
                    )
                    if total_requests > 0
                    else 0,
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

    async def get_request_history(
        self,
        limit: int = 50,
        filter_success: Optional[bool] = None,
    ) -> List[Dict[str, Any]]:
        """Get recent request history with optional filtering."""
        history = (
            self._request_history[-limit:]
            if limit > 0
            else self._request_history
        )

        if filter_success is not None:
            history = [m for m in history if m.success == filter_success]

        return [m.to_dict() for m in history]

    async def reset_statistics(self) -> None:
        """Reset client statistics counters and cache."""
        async with self._stats_lock:
            self.request_counter = 0
            self.error_counter = 0
            self.success_counter = 0
            self.cache_hits = 0
            self.request_timestamps.clear()
            self._request_history.clear()
            self._cache.clear()

            await self.circuit_breaker.reset()

            logger.info("Client statistics, cache, and circuit breaker reset")

    async def clear_cache(self) -> None:
        """Clear response cache."""
        async with self._cache_lock:
            cache_size = len(self._cache)
            self._cache.clear()
            logger.info("Cache cleared (%d entries removed)", cache_size)

    async def close(self) -> None:
        """Close the session and cleanup resources."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            logger.info("Google Apps Script client session closed")

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()


# ======================================================================
# Global Async Instance Management
# ======================================================================

_google_apps_script_client: Optional[GoogleAppsScriptClient] = None
_client_lock = asyncio.Lock()


async def get_google_apps_script_client(
    config: Optional[ClientConfig] = None,
    refresh: bool = False,
) -> GoogleAppsScriptClient:
    """
    Get or create global Google Apps Script client instance.
    """
    global _google_apps_script_client

    async with _client_lock:
        if refresh and _google_apps_script_client:
            await _google_apps_script_client.close()
            _google_apps_script_client = None

        if _google_apps_script_client is None:
            _google_apps_script_client = GoogleAppsScriptClient(config=config)
            logger.info(
                "Global GoogleAppsScriptClient instance created successfully"
            )

        return _google_apps_script_client


async def close_google_apps_script_client() -> None:
    """Close global Google Apps Script client."""
    global _google_apps_script_client
    if _google_apps_script_client:
        await _google_apps_script_client.close()
        _google_apps_script_client = None
        logger.info("Global GoogleAppsScriptClient closed")


# ======================================================================
# Convenience Async Functions
# ======================================================================


async def get_symbols_data_async(
    symbol: Optional[Union[str, List[str]]] = None,
    action: str = "get",
    include_metadata: bool = False,
    use_cache: bool = True,
    cache_ttl: int = 30,
) -> GoogleAppsScriptResponse:
    """Async convenience function for symbol data retrieval."""
    client = await get_google_apps_script_client()
    return await client.get_symbols_data(
        symbol=symbol,
        action=action,
        include_metadata=include_metadata,
        use_cache=use_cache,
        cache_ttl=cache_ttl,
    )


async def update_symbol_data_async(
    symbol: str,
    data: Dict[str, Any],
    action: str = "update",
    validate: bool = True,
) -> GoogleAppsScriptResponse:
    """Async convenience function for symbol data update."""
    client = await get_google_apps_script_client()
    return await client.update_symbol_data(
        symbol=symbol,
        data=data,
        action=action,
        validate=validate,
    )


async def health_check_async(
    detailed: bool = False,
    include_system: bool = False,
) -> GoogleAppsScriptResponse:
    """Async convenience function for health checks."""
    client = await get_google_apps_script_client()
    return await client.health_check(
        detailed=detailed,
        include_system=include_system,
    )


async def get_client_statistics_async() -> Dict[str, Any]:
    """Async convenience function to get client statistics."""
    client = await get_google_apps_script_client()
    return await client.get_statistics()


# ======================================================================
# Enhanced Test Function
# ======================================================================


async def test_client():
    """Enhanced test for the async Google Apps Script client."""
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    try:
        client = await get_google_apps_script_client()

        print("=" * 60)
        print("Testing Google Apps Script Client (Async) v4.0.0")
        print("=" * 60)

        print("\n=== Configuration ===")
        print(f"Base URL: {client._mask_url(client.config.base_url)}")
        print(f"Timeout: {client.config.timeout}s")
        print(f"Max Retries: {client.config.max_retries}")
        print(f"Pool Size: {client.config.pool_size}")
        print(f"Rate Limit: {client.config.rate_limit_rpm} RPM")

        print("\n=== Health Check ===")
        health_response = await client.health_check(detailed=True)
        print(f"Success: {health_response.success}")
        print(f"Status Code: {health_response.status_code}")
        print(f"Response Time: {health_response.execution_time:.3f}s")

        if health_response.data and "client" in health_response.data:
            stats = health_response.data["client"]["stats"]
            print(f"Total Requests: {stats['total_requests']}")
            print(f"Success Rate: {stats['success_rate_percent']}%")

        print("\n=== Circuit Breaker ===")
        cb_state = await client.circuit_breaker.get_state()
        print(f"State: {cb_state['state']}")
        print(f"Can Execute: {cb_state['can_execute']}")
        print(f"Failure Count: {cb_state['failure_count']}")

        print("\n=== Statistics ===")
        stats = await client.get_statistics()
        print(f"Cache Hits: {stats.get('cache_hits', 0)}")
        print(f"Cache Size: {stats.get('cache', {}).get('size', 0)}")

        print("\n=== Request History ===")
        history = await client.get_request_history(limit=5)
        print(f"Recent Requests: {len(history)}")
        for req in history[:3]:
            print(
                f"  - {req.get('request_id')}: {req.get('duration', 0) or 0:.3f}s"
            )

        print("\n=== Test Completed Successfully ===")
        print("=" * 60)

    except Exception as e:
        print(f"\n!!! Test Failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)

    finally:
        await close_google_apps_script_client()


if __name__ == "__main__":
    asyncio.run(test_client())
