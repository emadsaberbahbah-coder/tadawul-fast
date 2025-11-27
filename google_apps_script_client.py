# google_apps_script_client.py
# Enhanced Google Apps Script Client - Production Ready for Render
# Version: 3.1.0 - Memory and Performance Optimizations
# Optimized for FastAPI and Render deployment

from __future__ import annotations

import os
import logging
import time
import random
import re
import asyncio
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any, Union, Tuple
from enum import Enum
from contextlib import contextmanager
from collections import deque
import threading

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class RequestAction(str, Enum):
    """Standardized actions for Google Apps Script requests."""
    GET = "get"
    UPDATE = "update"
    PING = "ping"
    LIST = "list"
    BATCH = "batch"
    HEALTH = "health"


class ErrorType(str, Enum):
    """Classification of error types for better handling."""
    NETWORK = "network_error"
    TIMEOUT = "timeout_error"
    AUTH = "authentication_error"
    RATE_LIMIT = "rate_limit_error"
    SERVER = "server_error"
    CLIENT = "client_error"
    PARSING = "parsing_error"
    CIRCUIT_BREAKER = "circuit_breaker_error"
    VALIDATION = "validation_error"
    UNKNOWN = "unknown_error"


class HealthState(str, Enum):
    """System health states for circuit breaker pattern."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    CIRCUIT_OPEN = "circuit_open"


@dataclass
class GoogleAppsScriptResponse:
    """
    Enhanced response wrapper for Google Apps Script calls with detailed diagnostics.
    """
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

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        if self.error_type:
            result['error_type'] = self.error_type.value
        if self.health_state:
            result['health_state'] = self.health_state.value
        # Remove None values for cleaner output
        return {k: v for k, v in result.items() if v is not None}


class CircuitBreaker:
    """
    Enhanced circuit breaker pattern to prevent cascading failures.
    """
    
    def __init__(
        self, 
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        half_open_max_requests: int = 3
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_requests = half_open_max_requests
        
        self.state = HealthState.HEALTHY
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.half_open_attempts = 0
        self._lock = threading.RLock()
        
        logger.info(
            f"CircuitBreaker initialized: threshold={failure_threshold}, "
            f"recovery_timeout={recovery_timeout}s"
        )
    
    def can_execute(self) -> Tuple[bool, Optional[HealthState]]:
        """
        Check if request can be executed based on current circuit state.
        Returns (can_execute, current_state)
        """
        with self._lock:
            now = time.time()
            
            if self.state == HealthState.CIRCUIT_OPEN:
                if self.last_failure_time and (now - self.last_failure_time) > self.recovery_timeout:
                    # Transition to half-open state
                    self.state = HealthState.DEGRADED
                    self.half_open_attempts = 0
                    logger.info("Circuit breaker transitioning to HALF-OPEN state")
                    return True, self.state
                return False, self.state
            
            elif self.state == HealthState.DEGRADED:
                if self.half_open_attempts >= self.half_open_max_requests:
                    self.state = HealthState.CIRCUIT_OPEN
                    self.last_failure_time = now
                    logger.warning("Circuit breaker re-opened due to half-open failures")
                    return False, self.state
                return True, self.state
            
            return True, self.state
    
    def on_success(self) -> None:
        """Record successful execution and recover circuit state."""
        with self._lock:
            if self.state == HealthState.DEGRADED:
                self.half_open_attempts += 1
                # If we have enough successful attempts in half-open, close the circuit
                if self.half_open_attempts >= self.half_open_max_requests:
                    self.state = HealthState.HEALTHY
                    self.failure_count = 0
                    self.half_open_attempts = 0
                    logger.info("Circuit breaker closed - service recovered")
            
            elif self.state == HealthState.UNHEALTHY:
                # Gradual recovery from unhealthy state
                self.failure_count = max(0, self.failure_count - 2)
                if self.failure_count < self.failure_threshold:
                    self.state = HealthState.HEALTHY
                    logger.info("Circuit breaker recovered to HEALTHY state")
    
    def on_failure(self) -> None:
        """Record failed execution and update circuit state."""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.state == HealthState.DEGRADED:
                self.half_open_attempts += 1
                # If half-open requests fail, open circuit
                if self.half_open_attempts >= self.half_open_max_requests:
                    self.state = HealthState.CIRCUIT_OPEN
                    logger.error("Circuit breaker opened due to half-open failures")
            
            elif self.state == HealthState.HEALTHY and self.failure_count >= self.failure_threshold:
                self.state = HealthState.UNHEALTHY
                logger.warning(f"Circuit breaker degraded due to {self.failure_count} consecutive failures")
            
            elif self.state == HealthState.UNHEALTHY and self.failure_count >= self.failure_threshold * 2:
                self.state = HealthState.CIRCUIT_OPEN
                logger.error(f"Circuit breaker opened due to {self.failure_count} consecutive failures")
    
    def get_state(self) -> Dict[str, Any]:
        """Get current circuit breaker state."""
        with self._lock:
            can_execute, _ = self.can_execute()
            return {
                "state": self.state.value,
                "failure_count": self.failure_count,
                "last_failure_time": self.last_failure_time,
                "half_open_attempts": self.half_open_attempts,
                "can_execute": can_execute
            }


class GoogleAppsScriptClient:
    """
    Enhanced HTTP client for interacting with Google Apps Script endpoints.

    Features:
    - Circuit breaker pattern for failure prevention
    - Retry mechanism with exponential backoff
    - Rate limiting and request throttling
    - Comprehensive error handling and classification
    - Connection pooling with optimized settings
    - Request/response logging for debugging
    - Health checks and graceful degradation
    - Async-compatible operation
    - Memory-efficient timestamp tracking

    Environment Variables:
        GOOGLE_APPS_SCRIPT_URL (required) - Base URL for the Apps Script
        GOOGLE_APPS_SCRIPT_BACKUP_URL (optional) - Fallback URL
        GOOGLE_APPS_SCRIPT_APP_TOKEN (optional) - Authentication token
        GOOGLE_APPS_SCRIPT_TIMEOUT (optional) - Request timeout in seconds
        GOOGLE_APPS_SCRIPT_MAX_RETRIES (optional) - Maximum retry attempts
        GOOGLE_APPS_SCRIPT_RATE_LIMIT_RPM (optional) - Rate limit (requests per minute)
        GOOGLE_APPS_SCRIPT_CIRCUIT_BREAKER_THRESHOLD (optional) - Failure threshold for circuit breaker
    """

    # Google Apps Script URL pattern for validation
    GAS_URL_PATTERN = re.compile(
        r'^https://script\.google\.com/macros/s/[A-Za-z0-9_-]+/exec$'
    )
    
    # Default Apps Script URL (should be replaced with actual URL)
    DEFAULT_GAS_URL = "https://script.google.com/macros/s/AKfycbw.../exec"

    def __init__(
        self,
        base_url: Optional[str] = None,
        backup_url: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
        rate_limit_rpm: int = 60,
        circuit_breaker_threshold: int = 5,
        app_token: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> None:
        # URLs configuration with validation
        self.base_url: str = self._get_and_validate_url(base_url, "GOOGLE_APPS_SCRIPT_URL")
        self.backup_url: Optional[str] = self._get_and_validate_url(backup_url, "GOOGLE_APPS_SCRIPT_BACKUP_URL", required=False)
        
        # Request configuration
        self.timeout: int = self._get_timeout_config(timeout)
        self.max_retries: int = max_retries
        self.rate_limit_rpm: int = rate_limit_rpm
        
        # Enhanced rate limiting with memory optimization
        self.request_timestamps: deque = deque(maxlen=rate_limit_rpm + 10)  # Small buffer
        self.last_request_time: float = 0.0
        self._rate_limit_lock = threading.RLock()
        
        # Circuit breaker for failure prevention
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=circuit_breaker_threshold,
            recovery_timeout=60,  # 1 minute recovery
            half_open_max_requests=2
        )
        
        # Security / identity
        self.app_token: Optional[str] = app_token or os.getenv("GOOGLE_APPS_SCRIPT_APP_TOKEN") or os.getenv("APP_TOKEN")
        
        self.user_agent: str = (
            user_agent
            or os.getenv("GOOGLE_APPS_SCRIPT_USER_AGENT")
            or os.getenv("USER_AGENT")
            or f"TadawulStockAPI/3.1.0 (+Python-Requests)"
        )

        # Request tracking
        self.request_counter: int = 0
        self.error_counter: int = 0
        self.success_counter: int = 0
        self._stats_lock = threading.RLock()

        # Initialize session with enhanced configuration
        self.session = self._create_session()

        logger.info(
            "GoogleAppsScriptClient initialized: "
            f"base_url={self._mask_url(self.base_url)}, "
            f"backup_url={self._mask_url(self.backup_url) if self.backup_url else 'None'}, "
            f"timeout={self.timeout}s, max_retries={self.max_retries}, "
            f"rate_limit={self.rate_limit_rpm}/min, "
            f"circuit_breaker_threshold={circuit_breaker_threshold}"
        )

    def _get_and_validate_url(self, provided_url: Optional[str], env_var: str, required: bool = True) -> str:
        """Get and validate Google Apps Script URL."""
        url = provided_url or os.getenv(env_var)
        
        if required and not url:
            logger.warning(f"{env_var} not configured, using default")
            url = self.DEFAULT_GAS_URL
        
        if url:
            url = url.strip()
            if not self._validate_gas_url(url):
                logger.warning(f"URL for {env_var} does not match Google Apps Script pattern: {self._mask_url(url)}")
            return url
        
        if required:
            raise ValueError(f"Required configuration missing: {env_var}")
        
        return ""

    def _validate_gas_url(self, url: str) -> bool:
        """Validate that URL matches Google Apps Script pattern."""
        return bool(self.GAS_URL_PATTERN.match(url))

    def _get_timeout_config(self, default_timeout: int) -> int:
        """Get timeout configuration from environment with validation."""
        try:
            env_timeout = os.getenv("GOOGLE_APPS_SCRIPT_TIMEOUT")
            if env_timeout:
                timeout_val = max(5, int(env_timeout))  # Minimum 5 seconds
                logger.info(f"Using configured timeout: {timeout_val}s")
                return timeout_val
        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid GOOGLE_APPS_SCRIPT_TIMEOUT: {e}, using default: {default_timeout}s")
        
        return max(5, default_timeout)

    def _create_session(self) -> requests.Session:
        """Create configured requests session with retry strategy and connection pooling."""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            status_forcelist=[408, 429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            backoff_factor=1.5,
            raise_on_status=False,
        )

        # Configure adapter with connection pooling
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=20,
            pool_maxsize=30,
            pool_block=False,
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set default headers
        session.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

        if self.app_token:
            session.headers["Authorization"] = f"Bearer {self.app_token}"
            logger.debug("App token configured for Google Apps Script requests")

        return session

    def _mask_url(self, url: str) -> str:
        """Mask URL for secure logging."""
        if not url or "/" not in url:
            return "***"
        
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            if parsed.hostname and "google.com" in parsed.hostname:
                path_parts = parsed.path.split('/')
                if len(path_parts) > 4:
                    script_id = path_parts[3] if len(path_parts) > 3 else "***"
                    return f"https://script.google.com/macros/s/{script_id[:8]}.../exec"
        except Exception:
            pass
        
        return "https://script.google.com/macros/s/.../exec"

    def _generate_request_id(self) -> str:
        """Generate unique request ID for tracing."""
        with self._stats_lock:
            self.request_counter += 1
            timestamp = int(time.time() * 1000)
            return f"gas_req_{timestamp}_{self.request_counter:06d}"

    def _enforce_rate_limit(self) -> None:
        """Enhanced rate limiting with optimized memory usage."""
        with self._rate_limit_lock:
            now = time.time()
            window_start = now - 60  # 1 minute window
            
            # Efficiently clean old timestamps using deque maxlen property
            # The deque will automatically maintain size due to maxlen
            # We just need to ensure we're not blocking on cleanup
            
            # Check if we're over the limit
            current_count = len(self.request_timestamps)
            if current_count >= self.rate_limit_rpm:
                # Find the oldest timestamp that's still in the window
                while self.request_timestamps and self.request_timestamps[0] < window_start:
                    self.request_timestamps.popleft()
                
                current_count = len(self.request_timestamps)
                if current_count >= self.rate_limit_rpm:
                    oldest_timestamp = self.request_timestamps[0]
                    sleep_time = 60 - (now - oldest_timestamp)
                    if sleep_time > 0:
                        logger.warning(f"Rate limit exceeded, sleeping for {sleep_time:.2f}s")
                        time.sleep(sleep_time + random.uniform(0.1, 0.3))
            
            # Add jitter to avoid synchronized requests
            jitter = random.uniform(0.05, 0.15)
            time.sleep(jitter)
            
            self.request_timestamps.append(now)
            self.last_request_time = now

    def _classify_error(self, error: Exception, status_code: Optional[int] = None) -> ErrorType:
        """Enhanced error classification with better coverage."""
        if isinstance(error, requests.exceptions.Timeout):
            return ErrorType.TIMEOUT
        elif isinstance(error, requests.exceptions.ConnectionError):
            return ErrorType.NETWORK
        elif isinstance(error, requests.exceptions.HTTPError):
            if status_code == 429:
                return ErrorType.RATE_LIMIT
            elif status_code in [401, 403]:
                return ErrorType.AUTH
            elif 500 <= status_code < 600:
                return ErrorType.SERVER
            elif 400 <= status_code < 500:
                return ErrorType.CLIENT
        elif isinstance(error, (requests.exceptions.JSONDecodeError, ValueError)):
            return ErrorType.PARSING
        
        return ErrorType.UNKNOWN

    def _update_stats(self, success: bool) -> None:
        """Update success/error statistics."""
        with self._stats_lock:
            if success:
                self.success_counter += 1
            else:
                self.error_counter += 1

    @contextmanager
    def _request_context(self, request_id: str, action: str, url: str):
        """Context manager for request logging and timing."""
        start_time = time.perf_counter()
        logger.debug(f"[{request_id}] Starting {action} request to {self._mask_url(url)}")
        
        try:
            yield
            execution_time = time.perf_counter() - start_time
            logger.debug(f"[{request_id}] Request completed in {execution_time:.3f}s")
            
        except Exception as e:
            execution_time = time.perf_counter() - start_time
            logger.error(f"[{request_id}] Request failed after {execution_time:.3f}s: {e}")
            raise

    def _make_request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        action: str = "unknown"
    ) -> GoogleAppsScriptResponse:
        """
        Internal method to make HTTP requests with comprehensive error handling and circuit breaker.
        """
        # Check circuit breaker first
        can_execute, health_state = self.circuit_breaker.can_execute()
        if not can_execute:
            logger.warning(f"Request blocked by circuit breaker (state: {health_state.value})")
            return GoogleAppsScriptResponse(
                success=False,
                error=f"Service temporarily unavailable (circuit {health_state.value})",
                error_type=ErrorType.CIRCUIT_BREAKER,
                health_state=health_state,
                request_id=self._generate_request_id(),
                timestamp=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            )

        request_id = self._generate_request_id()
        retries = 0
        last_exception = None
        last_status_code = None
        
        # Try primary and backup URLs
        urls_to_try = [url]
        if self.backup_url and url == self.base_url:
            urls_to_try.append(self.backup_url)

        for current_url in urls_to_try:
            for attempt in range(self.max_retries + 1):
                try:
                    self._enforce_rate_limit()
                    
                    with self._request_context(request_id, action, current_url):
                        if method.upper() == "GET":
                            response = self.session.get(
                                current_url,
                                params=params,
                                timeout=self.timeout,
                            )
                        else:  # POST
                            response = self.session.post(
                                current_url,
                                json=json_data,
                                timeout=self.timeout,
                            )
                    
                    last_status_code = response.status_code
                    
                    # Log response for debugging
                    logger.debug(
                        f"[{request_id}] Response: status={last_status_code}, "
                        f"size={len(response.content)} bytes, url={self._mask_url(current_url)}"
                    )
                    
                    response.raise_for_status()
                    
                    # Parse JSON response
                    try:
                        data = response.json()
                    except ValueError as e:
                        logger.warning(f"[{request_id}] JSON parse error: {e}")
                        # Try to extract error from text
                        error_text = response.text[:500]
                        self._update_stats(False)
                        self.circuit_breaker.on_failure()
                        
                        return GoogleAppsScriptResponse(
                            success=False,
                            error=f"Invalid JSON response: {error_text}",
                            error_type=ErrorType.PARSING,
                            status_code=last_status_code,
                            retries=retries,
                            request_id=request_id,
                            timestamp=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                            health_state=health_state,
                        )
                    
                    # Check for success flag in response
                    success = data.get('success', True)
                    error_msg = data.get('error') or data.get('message')
                    
                    execution_time = time.perf_counter() - (self.last_request_time or time.time())
                    
                    # Update statistics and circuit breaker
                    self._update_stats(success)
                    if success:
                        self.circuit_breaker.on_success()
                        logger.debug(f"[{request_id}] Request successful")
                    else:
                        self.circuit_breaker.on_failure()
                        logger.warning(f"[{request_id}] Apps Script reported error: {error_msg}")
                    
                    return GoogleAppsScriptResponse(
                        success=success,
                        data=data if success else None,
                        error=error_msg,
                        error_type=ErrorType.SERVER if not success else None,
                        execution_time=execution_time,
                        status_code=last_status_code,
                        retries=retries,
                        request_id=request_id,
                        timestamp=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                        health_state=health_state,
                    )
                    
                except requests.exceptions.RequestException as e:
                    last_exception = e
                    retries = attempt
                    last_status_code = getattr(e.response, 'status_code', None)
                    error_type = self._classify_error(e, last_status_code)
                    
                    logger.warning(
                        f"[{request_id}] Attempt {attempt + 1}/{self.max_retries + 1} failed "
                        f"({error_type.value}): {e}, url={self._mask_url(current_url)}"
                    )
                    
                    # Don't retry on client errors (except 429)
                    if (error_type == ErrorType.CLIENT and last_status_code != 429):
                        break
                    
                    if attempt < self.max_retries:
                        # Exponential backoff with jitter
                        sleep_time = (2 ** attempt) + random.uniform(0.1, 0.5)
                        logger.debug(f"[{request_id}] Retrying in {sleep_time:.2f}s...")
                        time.sleep(sleep_time)
                    else:
                        logger.error(f"[{request_id}] All retries exhausted for {self._mask_url(current_url)}")
        
        # All attempts failed
        self._update_stats(False)
        self.circuit_breaker.on_failure()
        error_type = self._classify_error(last_exception, last_status_code) if last_exception else ErrorType.UNKNOWN
        
        return GoogleAppsScriptResponse(
            success=False,
            error=f"All requests failed: {str(last_exception) if last_exception else 'Unknown error'}",
            error_type=error_type,
            status_code=last_status_code,
            retries=retries,
            request_id=request_id,
            timestamp=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            health_state=health_state,
        )

    # ------------------------------------------------------------------
    # Public API Methods (Synchronous)
    # ------------------------------------------------------------------

    def get_symbols_data(
        self,
        symbol: Optional[Union[str, List[str]]] = None,
        action: str = "get",
        include_metadata: bool = False
    ) -> GoogleAppsScriptResponse:
        """
        Enhanced symbols data retrieval with comprehensive error handling.
        
        Args:
            symbol: Single symbol string or list of symbols
            action: Action type for the Apps Script
            include_metadata: Whether to include additional metadata in request
            
        Returns:
            GoogleAppsScriptResponse with results
        """
        # Validate input
        if symbol and isinstance(symbol, list) and len(symbol) > 100:
            return GoogleAppsScriptResponse(
                success=False,
                error="Too many symbols (maximum 100 per request)",
                error_type=ErrorType.VALIDATION
            )

        params = {"action": action}
        
        if symbol:
            if isinstance(symbol, (list, tuple)):
                params["symbol"] = ",".join(str(s).upper().strip() for s in symbol)
            else:
                params["symbol"] = str(symbol).upper().strip()
        
        if include_metadata:
            params["metadata"] = "true"
            params["source"] = "tadawul_api_v3"

        logger.info(
            f"Fetching symbols data: symbol={symbol if isinstance(symbol, str) else f'list({len(symbol)})' if symbol else 'None'}, "
            f"action={action}, include_metadata={include_metadata}"
        )

        return self._make_request(
            method="GET",
            url=self.base_url,
            params=params,
            action=f"get_symbols_{action}"
        )

    def update_symbol_data(
        self,
        symbol: str,
        data: Dict[str, Any],
        action: str = "update"
    ) -> GoogleAppsScriptResponse:
        """
        Enhanced symbol data update with validation.
        
        Args:
            symbol: Stock symbol to update
            data: Dictionary of data to update
            action: Action type for the Apps Script
            
        Returns:
            GoogleAppsScriptResponse with update results
        """
        if not symbol or not symbol.strip():
            return GoogleAppsScriptResponse(
                success=False,
                error="Symbol cannot be empty",
                error_type=ErrorType.VALIDATION
            )

        # Validate data size
        if len(str(data)) > 10000:
            return GoogleAppsScriptResponse(
                success=False,
                error="Data payload too large",
                error_type=ErrorType.VALIDATION
            )

        payload = {
            "action": action,
            "symbol": symbol.upper().strip(),
            "data": data,
            "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            "source": "tadawul_api_v3"
        }

        logger.info(f"Updating symbol data: {symbol}, data_keys={list(data.keys())}")

        return self._make_request(
            method="POST",
            url=self.base_url,
            json_data=payload,
            action=f"update_symbol_{action}"
        )

    def batch_update_symbols(
        self,
        updates: List[Dict[str, Any]],
        action: str = "batch_update"
    ) -> GoogleAppsScriptResponse:
        """
        Batch update multiple symbols in a single request.
        
        Args:
            updates: List of update objects, each containing 'symbol' and 'data'
            action: Action type for the Apps Script
            
        Returns:
            GoogleAppsScriptResponse with batch results
        """
        if not updates:
            return GoogleAppsScriptResponse(
                success=False,
                error="Empty updates list",
                error_type=ErrorType.VALIDATION
            )

        if len(updates) > 50:
            return GoogleAppsScriptResponse(
                success=False,
                error="Too many updates (maximum 50 per batch)",
                error_type=ErrorType.VALIDATION
            )

        payload = {
            "action": action,
            "updates": updates,
            "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            "source": "tadawul_api_v3",
            "count": len(updates)
        }

        logger.info(f"Batch updating {len(updates)} symbols")

        return self._make_request(
            method="POST",
            url=self.base_url,
            json_data=payload,
            action="batch_update_symbols"
        )

    def health_check(self, detailed: bool = False) -> GoogleAppsScriptResponse:
        """
        Enhanced health check with detailed diagnostics.
        
        Args:
            detailed: Whether to include detailed system information
            
        Returns:
            GoogleAppsScriptResponse with health status
        """
        params = {"action": "health", "ping": "1"}
        if detailed:
            params["detailed"] = "true"

        logger.debug("Performing health check")

        response = self._make_request(
            method="GET",
            url=self.base_url,
            params=params,
            action="health_check"
        )

        # Add client statistics and circuit breaker state to health response
        if response.data is None:
            response.data = {}
        
        response.data["client_stats"] = self.get_statistics()
        response.data["circuit_breaker"] = self.circuit_breaker.get_state()

        return response

    def test_connection(self) -> Tuple[bool, Dict[str, Any]]:
        """
        Simple connection test that returns basic information.
        
        Returns:
            Tuple of (success, diagnostics_dict)
        """
        response = self.health_check()
        
        diagnostics = {
            "base_url_accessible": response.success,
            "backup_url_configured": bool(self.backup_url),
            "timeout_configured": self.timeout,
            "app_token_configured": bool(self.app_token),
            "client_stats": self.get_statistics(),
            "circuit_breaker": self.circuit_breaker.get_state(),
            "last_health_check": response.timestamp,
        }
        
        if not response.success:
            diagnostics["error"] = response.error
            diagnostics["error_type"] = response.error_type.value if response.error_type else "unknown"
            diagnostics["health_state"] = response.health_state.value if response.health_state else "unknown"
        
        return response.success, diagnostics

    # ------------------------------------------------------------------
    # Async-Compatible Methods
    # ------------------------------------------------------------------

    async def get_symbols_data_async(
        self,
        symbol: Optional[Union[str, List[str]]] = None,
        action: str = "get",
        include_metadata: bool = False
    ) -> GoogleAppsScriptResponse:
        """
        Async-compatible version of get_symbols_data.
        Uses thread pool to avoid blocking the event loop.
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, 
            self.get_symbols_data, 
            symbol, action, include_metadata
        )

    async def health_check_async(self, detailed: bool = False) -> GoogleAppsScriptResponse:
        """
        Async-compatible version of health_check.
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.health_check, detailed)

    # ------------------------------------------------------------------
    # Monitoring and Statistics
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        """Get client usage statistics and health metrics."""
        with self._stats_lock:
            total_requests = self.success_counter + self.error_counter
            success_rate = (self.success_counter / total_requests * 100) if total_requests > 0 else 0
            
            return {
                "total_requests": total_requests,
                "successful_requests": self.success_counter,
                "failed_requests": self.error_counter,
                "success_rate_percent": round(success_rate, 2),
                "rate_limit_rpm": self.rate_limit_rpm,
                "current_window_requests": len(self.request_timestamps),
                "max_retries": self.max_retries,
                "timeout_seconds": self.timeout,
                "circuit_breaker_state": self.circuit_breaker.get_state(),
            }

    def reset_statistics(self) -> None:
        """Reset client statistics counters."""
        with self._stats_lock:
            self.request_counter = 0
            self.error_counter = 0
            self.success_counter = 0
            self.request_timestamps.clear()
            # Reset circuit breaker but keep configuration
            self.circuit_breaker = CircuitBreaker(
                failure_threshold=self.circuit_breaker.failure_threshold,
                recovery_timeout=self.circuit_breaker.recovery_timeout,
                half_open_max_requests=self.circuit_breaker.half_open_max_requests
            )
            logger.info("Client statistics and circuit breaker reset")

    def get_detailed_status(self) -> Dict[str, Any]:
        """Get detailed status including configuration and health."""
        stats = self.get_statistics()
        config = {
            "base_url": self._mask_url(self.base_url),
            "backup_url": self._mask_url(self.backup_url) if self.backup_url else None,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "rate_limit_rpm": self.rate_limit_rpm,
            "app_token_configured": bool(self.app_token),
            "user_agent": self.user_agent,
        }
        
        return {
            "configuration": config,
            "statistics": stats,
            "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        }

    def close(self) -> None:
        """Close the session and cleanup resources."""
        if hasattr(self, 'session'):
            self.session.close()
            logger.info("Google Apps Script client session closed")


# Global instance with enhanced error handling
try:
    # Get circuit breaker threshold from environment
    circuit_breaker_threshold = int(os.getenv("GOOGLE_APPS_SCRIPT_CIRCUIT_BREAKER_THRESHOLD", "5"))
    
    google_apps_script_client = GoogleAppsScriptClient(
        circuit_breaker_threshold=circuit_breaker_threshold
    )
    logger.info("Global GoogleAppsScriptClient instance created successfully")
except Exception as e:
    logger.error(f"Failed to create global GoogleAppsScriptClient: {e}")
    google_apps_script_client = None


# Convenience functions for backward compatibility and async usage
def get_symbols_data(symbol: Optional[Union[str, List[str]]] = None) -> GoogleAppsScriptResponse:
    """Convenience function for simple symbol data retrieval."""
    if google_apps_script_client is None:
        return GoogleAppsScriptResponse(
            success=False,
            error="Google Apps Script client not initialized",
            error_type=ErrorType.CLIENT
        )
    return google_apps_script_client.get_symbols_data(symbol)


async def get_symbols_data_async(symbol: Optional[Union[str, List[str]]] = None) -> GoogleAppsScriptResponse:
    """Async convenience function for symbol data retrieval."""
    if google_apps_script_client is None:
        return GoogleAppsScriptResponse(
            success=False,
            error="Google Apps Script client not initialized",
            error_type=ErrorType.CLIENT
        )
    return await google_apps_script_client.get_symbols_data_async(symbol)


def health_check() -> GoogleAppsScriptResponse:
    """Convenience function for health checks."""
    if google_apps_script_client is None:
        return GoogleAppsScriptResponse(
            success=False,
            error="Google Apps Script client not initialized",
            error_type=ErrorType.CLIENT
        )
    return google_apps_script_client.health_check()


async def health_check_async() -> GoogleAppsScriptResponse:
    """Async convenience function for health checks."""
    if google_apps_script_client is None:
        return GoogleAppsScriptResponse(
            success=False,
            error="Google Apps Script client not initialized",
            error_type=ErrorType.CLIENT
        )
    return await google_apps_script_client.health_check_async()


if __name__ == "__main__":
    # Enhanced test functionality
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    client = GoogleAppsScriptClient()
    
    try:
        # Test health check
        print("=== Health Check ===")
        health_response = client.health_check()
        print(f"Health Check: {health_response.success}")
        if health_response.data:
            print(f"Data: {health_response.data}")
        
        # Test statistics
        print("\n=== Statistics ===")
        stats = client.get_statistics()
        for key, value in stats.items():
            print(f"{key}: {value}")
        
        # Test detailed status
        print("\n=== Detailed Status ===")
        status = client.get_detailed_status()
        print(f"Configuration: {status['configuration']}")
        print(f"Statistics: {status['statistics']}")
        
        # Test circuit breaker state
        print("\n=== Circuit Breaker ===")
        cb_state = client.circuit_breaker.get_state()
        print(f"Circuit Breaker: {cb_state}")
        
    finally:
        client.close()
        print("\n=== Client Closed ===")
