"""
google_apps_script_client.py
Enhanced Async Google Apps Script Client for Tadawul Fast Bridge
Version: 3.7.0 - Fully async with aiohttp, aligned with main.py
"""

import asyncio
import logging
import time
import random
import re
import json
from typing import Dict, List, Optional, Any, Union, Tuple
from enum import Enum
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

import aiohttp
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# ======================================================================
# Data Models
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


class ErrorType(str, Enum):
    """Classification of error types."""
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
    """Health states for circuit breaker."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    CIRCUIT_OPEN = "circuit_open"


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

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        if self.error_type:
            result["error_type"] = self.error_type.value
        if self.health_state:
            result["health_state"] = self.health_state.value
        return {k: v for k, v in result.items() if v is not None}


# ======================================================================
# Async Circuit Breaker
# ======================================================================

class AsyncCircuitBreaker:
    """Async circuit breaker pattern to prevent cascading failures."""
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        half_open_max_requests: int = 3,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_requests = half_open_max_requests
        
        self.state = HealthState.HEALTHY
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.half_open_attempts = 0
        self._lock = asyncio.Lock()
        
        logger.info(
            f"AsyncCircuitBreaker initialized: threshold={failure_threshold}, "
            f"recovery_timeout={recovery_timeout}s"
        )
    
    async def can_execute(self) -> Tuple[bool, HealthState]:
        """Check if request can be executed based on current circuit state."""
        async with self._lock:
            now = time.time()
            
            if self.state == HealthState.CIRCUIT_OPEN:
                if self.last_failure_time and (now - self.last_failure_time) > self.recovery_timeout:
                    # Transition to half-open state
                    self.state = HealthState.DEGRADED
                    self.half_open_attempts = 0
                    logger.info("Circuit breaker transitioning to HALF-OPEN (DEGRADED) state")
                    return True, self.state
                return False, self.state
            
            elif self.state == HealthState.DEGRADED:
                if self.half_open_attempts >= self.half_open_max_requests:
                    self.state = HealthState.CIRCUIT_OPEN
                    self.last_failure_time = now
                    logger.warning("Circuit breaker re-opened due to half-open failures")
                    return False, self.state
                return True, self.state
            
            # HEALTHY or UNHEALTHY states still allow execution
            return True, self.state
    
    async def on_success(self) -> None:
        """Record successful execution and recover circuit state."""
        async with self._lock:
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
    
    async def on_failure(self) -> None:
        """Record failed execution and update circuit state."""
        async with self._lock:
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
    
    async def get_state(self) -> Dict[str, Any]:
        """Get current circuit breaker state."""
        async with self._lock:
            now = time.time()
            can_execute = True
            
            if self.state == HealthState.CIRCUIT_OPEN:
                can_execute = False
            elif self.state == HealthState.DEGRADED:
                can_execute = self.half_open_attempts < self.half_open_max_requests
            
            return {
                "state": self.state.value,
                "failure_count": self.failure_count,
                "last_failure_time": self.last_failure_time,
                "half_open_attempts": self.half_open_attempts,
                "can_execute": can_execute,
                "recovery_timeout": self.recovery_timeout,
                "now": now,
            }


# ======================================================================
# Enhanced Async Google Apps Script Client
# ======================================================================

class GoogleAppsScriptClient:
    """
    Async HTTP client for Google Apps Script endpoints.
    Uses aiohttp for async operations, aligned with main.py configuration.
    
    Environment Variables (from main.py Settings):
        GOOGLE_APPS_SCRIPT_URL (required)
        GOOGLE_APPS_SCRIPT_BACKUP_URL (optional)
        APP_TOKEN (for authentication)
        TFB_APP_TOKEN (alternative authentication)
        HTTP_TIMEOUT (from settings, default: 30)
        MAX_RETRIES (from settings, default: 3)
    """
    
    # Google Apps Script URL pattern
    GAS_URL_PATTERN = re.compile(r"^https://script\.google\.com/macros/s/[A-Za-z0-9_-]+/exec$")
    
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
        import os
        
        # URLs from environment or parameters
        self.base_url = base_url or os.getenv("GOOGLE_APPS_SCRIPT_URL", "")
        self.backup_url = backup_url or os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL", "")
        
        # Validate URLs
        if not self.base_url:
            raise ValueError("GOOGLE_APPS_SCRIPT_URL is required")
        if not self._validate_gas_url(self.base_url):
            logger.warning(f"Base URL may not be valid Google Apps Script URL: {self._mask_url(self.base_url)}")
        if self.backup_url and not self._validate_gas_url(self.backup_url):
            logger.warning(f"Backup URL may not be valid: {self._mask_url(self.backup_url)}")
        
        # Configuration from main.py settings
        self.timeout = timeout
        self.max_retries = max_retries
        self.rate_limit_rpm = rate_limit_rpm
        
        # Rate limiting tracking
        self.request_timestamps: List[float] = []
        self._rate_limit_lock = asyncio.Lock()
        
        # Circuit breaker
        self.circuit_breaker = AsyncCircuitBreaker(
            failure_threshold=circuit_breaker_threshold,
            recovery_timeout=60,
            half_open_max_requests=2,
        )
        
        # Authentication - use APP_TOKEN or TFB_APP_TOKEN from main.py
        self.app_token = app_token or os.getenv("APP_TOKEN") or os.getenv("TFB_APP_TOKEN")
        
        # User agent matching main.py
        self.user_agent = user_agent or os.getenv("USER_AGENT", "TadawulStockAPI/3.7.0")
        
        # Statistics
        self.request_counter = 0
        self.error_counter = 0
        self.success_counter = 0
        self._stats_lock = asyncio.Lock()
        
        # HTTP session (will be created on demand)
        self._session: Optional[aiohttp.ClientSession] = None
        
        logger.info(
            f"GoogleAppsScriptClient initialized: "
            f"base_url={self._mask_url(self.base_url)}, "
            f"timeout={self.timeout}s, max_retries={self.max_retries}"
        )
    
    def _validate_gas_url(self, url: str) -> bool:
        """Validate that URL matches Google Apps Script pattern."""
        return bool(self.GAS_URL_PATTERN.match(url))
    
    def _mask_url(self, url: str) -> str:
        """Mask URL for secure logging."""
        if not url:
            return "***"
        
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            if parsed.hostname and "google.com" in parsed.hostname:
                path_parts = parsed.path.split("/")
                if len(path_parts) > 4:
                    script_id = path_parts[3] if len(path_parts) > 3 else "***"
                    return f"https://script.google.com/macros/s/{script_id[:8]}.../exec"
        except Exception:
            pass
        
        return "https://script.google.com/macros/s/.../exec"
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session with proper configuration."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            connector = aiohttp.TCPConnector(
                limit=100,
                limit_per_host=20,
                ssl=True,
            )
            
            headers = {
                "User-Agent": self.user_agent,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            
            if self.app_token:
                headers["Authorization"] = f"Bearer {self.app_token}"
            
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers=headers,
            )
        
        return self._session
    
    async def _enforce_rate_limit(self) -> None:
        """Async rate limiting with sliding window."""
        async with self._rate_limit_lock:
            now = time.time()
            window_start = now - 60  # 1 minute window
            
            # Remove old timestamps
            self.request_timestamps = [ts for ts in self.request_timestamps if ts > window_start]
            
            # Check if we're at limit
            if len(self.request_timestamps) >= self.rate_limit_rpm:
                oldest_timestamp = self.request_timestamps[0]
                sleep_time = 60 - (now - oldest_timestamp)
                if sleep_time > 0:
                    logger.warning(f"Rate limit exceeded, sleeping for {sleep_time:.2f}s")
                    await asyncio.sleep(sleep_time + random.uniform(0.1, 0.3))
            
            # Add jitter and record this request
            await asyncio.sleep(random.uniform(0.05, 0.15))
            self.request_timestamps.append(time.time())
    
    def _classify_error(self, error: Exception, status_code: Optional[int] = None) -> ErrorType:
        """Enhanced error classification."""
        if isinstance(error, asyncio.TimeoutError):
            return ErrorType.TIMEOUT
        elif isinstance(error, aiohttp.ClientConnectionError):
            return ErrorType.NETWORK
        elif isinstance(error, aiohttp.ClientResponseError):
            if status_code == 429:
                return ErrorType.RATE_LIMIT
            elif status_code in [401, 403]:
                return ErrorType.AUTH
            elif status_code and 500 <= status_code < 600:
                return ErrorType.SERVER
            elif status_code and 400 <= status_code < 500:
                return ErrorType.CLIENT
        elif isinstance(error, json.JSONDecodeError):
            return ErrorType.PARSING
        
        return ErrorType.UNKNOWN
    
    async def _update_stats(self, success: bool) -> None:
        """Update success/error statistics."""
        async with self._stats_lock:
            if success:
                self.success_counter += 1
            else:
                self.error_counter += 1
    
    def _generate_request_id(self) -> str:
        """Generate unique request ID for tracing."""
        self.request_counter += 1
        timestamp = int(time.time() * 1000)
        return f"gas_async_{timestamp}_{self.request_counter:06d}"
    
    async def _make_request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        action: str = "unknown",
    ) -> GoogleAppsScriptResponse:
        """
        Internal method to make async HTTP requests with comprehensive error handling.
        """
        # Check circuit breaker first
        can_execute, circuit_state = await self.circuit_breaker.can_execute()
        if not can_execute:
            logger.warning(f"Request blocked by circuit breaker (state: {circuit_state.value})")
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
        retries = 0
        last_exception: Optional[Exception] = None
        last_status_code: Optional[int] = None
        
        # Try primary and backup URLs
        urls_to_try = [url]
        if self.backup_url and url == self.base_url:
            urls_to_try.append(self.backup_url)
        
        for current_url in urls_to_try:
            for attempt in range(self.max_retries + 1):
                try:
                    await self._enforce_rate_limit()
                    attempt_start = time.perf_counter()
                    
                    session = await self._get_session()
                    
                    logger.debug(f"[{request_id}] Starting {action} to {self._mask_url(current_url)}")
                    
                    if method.upper() == "GET":
                        async with session.get(current_url, params=params) as response:
                            last_status_code = response.status
                            response_text = await response.text()
                    else:  # POST
                        async with session.post(current_url, json=json_data) as response:
                            last_status_code = response.status
                            response_text = await response.text()
                    
                    execution_time = time.perf_counter() - attempt_start
                    
                    logger.debug(
                        f"[{request_id}] Response: status={last_status_code}, "
                        f"time={execution_time:.3f}s, size={len(response_text)} bytes"
                    )
                    
                    # Check HTTP status
                    if last_status_code != 200:
                        error_msg = f"HTTP {last_status_code}: {response_text[:200]}"
                        logger.warning(f"[{request_id}] HTTP error: {error_msg}")
                        
                        await self._update_stats(False)
                        await self.circuit_breaker.on_failure()
                        
                        return GoogleAppsScriptResponse(
                            success=False,
                            error=error_msg,
                            error_type=self._classify_error(
                                aiohttp.ClientResponseError(None, None, status=last_status_code),
                                last_status_code
                            ),
                            status_code=last_status_code,
                            retries=retries,
                            request_id=request_id,
                            execution_time=execution_time,
                            timestamp=datetime.now(timezone.utc).isoformat(),
                            health_state=circuit_state,
                            source_url=self._mask_url(current_url),
                        )
                    
                    # Parse JSON response
                    try:
                        data = json.loads(response_text)
                    except json.JSONDecodeError as e:
                        logger.warning(f"[{request_id}] JSON parse error: {e}, response: {response_text[:500]}")
                        
                        await self._update_stats(False)
                        await self.circuit_breaker.on_failure()
                        
                        return GoogleAppsScriptResponse(
                            success=False,
                            error=f"Invalid JSON response: {response_text[:200]}",
                            error_type=ErrorType.PARSING,
                            status_code=last_status_code,
                            retries=retries,
                            request_id=request_id,
                            execution_time=execution_time,
                            timestamp=datetime.now(timezone.utc).isoformat(),
                            health_state=circuit_state,
                            source_url=self._mask_url(current_url),
                        )
                    
                    # Check for success flag in response
                    success = data.get("success", True)
                    error_msg = data.get("error") or data.get("message")
                    
                    # Update statistics and circuit breaker
                    await self._update_stats(success)
                    if success:
                        await self.circuit_breaker.on_success()
                        logger.debug(f"[{request_id}] Request successful")
                    else:
                        await self.circuit_breaker.on_failure()
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
                        timestamp=datetime.now(timezone.utc).isoformat(),
                        health_state=circuit_state,
                        source_url=self._mask_url(current_url),
                    )
                    
                except Exception as e:
                    last_exception = e
                    retries = attempt
                    error_type = self._classify_error(e, last_status_code)
                    
                    logger.warning(
                        f"[{request_id}] Attempt {attempt + 1}/{self.max_retries + 1} "
                        f"failed ({error_type.value}): {e}, "
                        f"url={self._mask_url(current_url)}"
                    )
                    
                    # Don't retry on client errors (except 429)
                    if error_type == ErrorType.CLIENT and last_status_code != 429:
                        break
                    
                    if attempt < self.max_retries:
                        # Exponential backoff with jitter
                        sleep_time = (2 ** attempt) + random.uniform(0.1, 0.5)
                        logger.debug(f"[{request_id}] Retrying in {sleep_time:.2f}s...")
                        await asyncio.sleep(sleep_time)
                    else:
                        logger.error(f"[{request_id}] All retries exhausted")
        
        # All attempts failed
        await self._update_stats(False)
        await self.circuit_breaker.on_failure()
        
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
            retries=retries,
            request_id=request_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            health_state=circuit_state,
            source_url=self._mask_url(url),
        )
    
    # ==================================================================
    # Public Async API Methods
    # ==================================================================
    
    async def get_symbols_data(
        self,
        symbol: Optional[Union[str, List[str]]] = None,
        action: str = "get",
        include_metadata: bool = False,
    ) -> GoogleAppsScriptResponse:
        """
        Async symbols data retrieval.
        
        Args:
            symbol: Single symbol string or list of symbols
            action: Action type for the Apps Script
            include_metadata: Whether to include additional metadata
        
        Returns:
            GoogleAppsScriptResponse with results
        """
        # Validate input
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
                params["symbol"] = ",".join(str(s).upper().strip() for s in symbol)
            else:
                params["symbol"] = str(symbol).upper().strip()
        
        if include_metadata:
            params["metadata"] = "true"
            params["source"] = "tadawul_api_v3"
        
        logger.info(
            f"Fetching symbols data: "
            f"symbol={symbol if isinstance(symbol, str) else f'list({len(symbol)})' if symbol else 'None'}, "
            f"action={action}"
        )
        
        return await self._make_request(
            method="GET",
            url=self.base_url,
            params=params,
            action=f"get_symbols_{action}",
        )
    
    async def update_symbol_data(
        self,
        symbol: str,
        data: Dict[str, Any],
        action: str = "update",
    ) -> GoogleAppsScriptResponse:
        """
        Async symbol data update.
        
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
                error_type=ErrorType.VALIDATION,
                timestamp=datetime.now(timezone.utc).isoformat(),
            )
        
        # Validate data size
        if len(json.dumps(data)) > 10000:
            return GoogleAppsScriptResponse(
                success=False,
                error="Data payload too large",
                error_type=ErrorType.VALIDATION,
                timestamp=datetime.now(timezone.utc).isoformat(),
            )
        
        payload = {
            "action": action,
            "symbol": symbol.upper().strip(),
            "data": data,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "tadawul_api_v3",
        }
        
        logger.info(f"Updating symbol data: {symbol}, data_keys={list(data.keys())}")
        
        return await self._make_request(
            method="POST",
            url=self.base_url,
            json_data=payload,
            action=f"update_symbol_{action}",
        )
    
    async def batch_update_symbols(
        self,
        updates: List[Dict[str, Any]],
        action: str = "batch_update",
    ) -> GoogleAppsScriptResponse:
        """
        Async batch update multiple symbols.
        
        Args:
            updates: List of update objects
            action: Action type for the Apps Script
        
        Returns:
            GoogleAppsScriptResponse with batch results
        """
        if not updates:
            return GoogleAppsScriptResponse(
                success=False,
                error="Empty updates list",
                error_type=ErrorType.VALIDATION,
                timestamp=datetime.now(timezone.utc).isoformat(),
            )
        
        if len(updates) > 50:
            return GoogleAppsScriptResponse(
                success=False,
                error="Too many updates (maximum 50 per batch)",
                error_type=ErrorType.VALIDATION,
                timestamp=datetime.now(timezone.utc).isoformat(),
            )
        
        payload = {
            "action": action,
            "updates": updates,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "tadawul_api_v3",
            "count": len(updates),
        }
        
        logger.info(f"Batch updating {len(updates)} symbols")
        
        return await self._make_request(
            method="POST",
            url=self.base_url,
            json_data=payload,
            action="batch_update_symbols",
        )
    
    async def health_check(self, detailed: bool = False) -> GoogleAppsScriptResponse:
        """
        Async health check with detailed diagnostics.
        
        Args:
            detailed: Whether to include detailed system information
        
        Returns:
            GoogleAppsScriptResponse with health status
        """
        params: Dict[str, Any] = {"action": "health", "ping": "1"}
        if detailed:
            params["detailed"] = "true"
        
        logger.debug("Performing async health check")
        
        response = await self._make_request(
            method="GET",
            url=self.base_url,
            params=params,
            action="health_check",
        )
        
        # Add client statistics and circuit breaker state
        if response.data is None:
            response.data = {}
        
        response.data["client_stats"] = await self.get_statistics()
        response.data["circuit_breaker"] = await self.circuit_breaker.get_state()
        
        return response
    
    async def test_connection(self) -> Tuple[bool, Dict[str, Any]]:
        """
        Simple async connection test.
        
        Returns:
            Tuple of (success, diagnostics_dict)
        """
        response = await self.health_check()
        
        diagnostics: Dict[str, Any] = {
            "base_url_accessible": response.success,
            "backup_url_configured": bool(self.backup_url),
            "timeout_configured": self.timeout,
            "app_token_configured": bool(self.app_token),
            "client_stats": await self.get_statistics(),
            "circuit_breaker": await self.circuit_breaker.get_state(),
            "last_health_check": response.timestamp,
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
    
    # ==================================================================
    # Monitoring and Statistics
    # ==================================================================
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get client usage statistics and health metrics."""
        async with self._stats_lock:
            total_requests = self.success_counter + self.error_counter
            success_rate = (
                self.success_counter / total_requests * 100
                if total_requests > 0
                else 0
            )
            
            # Get current rate limit window usage
            now = time.time()
            window_start = now - 60
            current_window = len([ts for ts in self.request_timestamps if ts > window_start])
            
            circuit_state = await self.circuit_breaker.get_state()
            
            return {
                "total_requests": total_requests,
                "successful_requests": self.success_counter,
                "failed_requests": self.error_counter,
                "success_rate_percent": round(success_rate, 2),
                "rate_limit_rpm": self.rate_limit_rpm,
                "current_window_requests": current_window,
                "max_retries": self.max_retries,
                "timeout_seconds": self.timeout,
                "circuit_breaker_state": circuit_state,
                "base_url": self._mask_url(self.base_url),
                "backup_url": self._mask_url(self.backup_url) if self.backup_url else None,
            }
    
    async def reset_statistics(self) -> None:
        """Reset client statistics counters."""
        async with self._stats_lock:
            self.request_counter = 0
            self.error_counter = 0
            self.success_counter = 0
            self.request_timestamps.clear()
            
            # Reset circuit breaker but keep configuration
            self.circuit_breaker = AsyncCircuitBreaker(
                failure_threshold=self.circuit_breaker.failure_threshold,
                recovery_timeout=self.circuit_breaker.recovery_timeout,
                half_open_max_requests=self.circuit_breaker.half_open_max_requests,
            )
            
            logger.info("Client statistics and circuit breaker reset")
    
    async def close(self) -> None:
        """Close the session and cleanup resources."""
        if self._session and not self._session.closed:
            await self._session.close()
            logger.info("Google Apps Script client session closed")
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()


# ======================================================================
# Global Async Instance
# ======================================================================

# Create global async instance
google_apps_script_client: Optional[GoogleAppsScriptClient] = None

async def get_google_apps_script_client() -> GoogleAppsScriptClient:
    """Get or create global Google Apps Script client instance."""
    global google_apps_script_client
    
    if google_apps_script_client is None:
        import os
        
        # Get configuration from environment (aligned with main.py)
        base_url = os.getenv("GOOGLE_APPS_SCRIPT_URL")
        backup_url = os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL")
        timeout = int(os.getenv("HTTP_TIMEOUT", "30"))
        max_retries = int(os.getenv("MAX_RETRIES", "3"))
        app_token = os.getenv("APP_TOKEN") or os.getenv("TFB_APP_TOKEN")
        
        if not base_url:
            raise ValueError("GOOGLE_APPS_SCRIPT_URL environment variable is required")
        
        google_apps_script_client = GoogleAppsScriptClient(
            base_url=base_url,
            backup_url=backup_url,
            timeout=timeout,
            max_retries=max_retries,
            rate_limit_rpm=60,
            circuit_breaker_threshold=5,
            app_token=app_token,
            user_agent="TadawulStockAPI/3.7.0",
        )
        
        logger.info("Global GoogleAppsScriptClient instance created successfully")
    
    return google_apps_script_client


async def close_google_apps_script_client() -> None:
    """Close global Google Apps Script client."""
    global google_apps_script_client
    if google_apps_script_client:
        await google_apps_script_client.close()
        google_apps_script_client = None
        logger.info("Global GoogleAppsScriptClient closed")


# ======================================================================
# Convenience Async Functions
# ======================================================================

async def get_symbols_data_async(
    symbol: Optional[Union[str, List[str]]] = None,
    action: str = "get",
    include_metadata: bool = False,
) -> GoogleAppsScriptResponse:
    """Async convenience function for symbol data retrieval."""
    client = await get_google_apps_script_client()
    return await client.get_symbols_data(symbol, action, include_metadata)


async def update_symbol_data_async(
    symbol: str,
    data: Dict[str, Any],
    action: str = "update",
) -> GoogleAppsScriptResponse:
    """Async convenience function for symbol data update."""
    client = await get_google_apps_script_client()
    return await client.update_symbol_data(symbol, data, action)


async def health_check_async(detailed: bool = False) -> GoogleAppsScriptResponse:
    """Async convenience function for health checks."""
    client = await get_google_apps_script_client()
    return await client.health_check(detailed)


# ======================================================================
# Test Function
# ======================================================================

async def test_client():
    """Test the async Google Apps Script client."""
    import sys
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    try:
        client = await get_google_apps_script_client()
        
        print("=== Testing Google Apps Script Client (Async) ===")
        print(f"Base URL: {client._mask_url(client.base_url)}")
        print(f"Timeout: {client.timeout}s")
        print(f"Max Retries: {client.max_retries}")
        
        # Test health check
        print("\n=== Health Check ===")
        health_response = await client.health_check()
        print(f"Success: {health_response.success}")
        print(f"Status Code: {health_response.status_code}")
        print(f"Execution Time: {health_response.execution_time:.3f}s")
        
        if health_response.data:
            print(f"Response Keys: {list(health_response.data.keys())}")
        
        # Test statistics
        print("\n=== Statistics ===")
        stats = await client.get_statistics()
        for key, value in stats.items():
            print(f"{key}: {value}")
        
        # Test circuit breaker
        print("\n=== Circuit Breaker ===")
        cb_state = await client.circuit_breaker.get_state()
        print(f"State: {cb_state['state']}")
        print(f"Can Execute: {cb_state['can_execute']}")
        print(f"Failure Count: {cb_state['failure_count']}")
        
        print("\n=== Test Completed Successfully ===")
        
    except Exception as e:
        print(f"\n!!! Test Failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        await close_google_apps_script_client()


if __name__ == "__main__":
    # Run test if executed directly
    asyncio.run(test_client())
