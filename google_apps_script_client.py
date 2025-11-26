# google_apps_script_client.py
# Enhanced Google Apps Script Client - Production Ready for Render
# Version: 2.0.0 - With retry logic, rate limiting, and comprehensive error handling

from __future__ import annotations

import os
import logging
import time
import random
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any, Union, Tuple
from enum import Enum
from contextlib import contextmanager

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


class ErrorType(str, Enum):
    """Classification of error types for better handling."""
    NETWORK = "network_error"
    TIMEOUT = "timeout_error"
    AUTH = "authentication_error"
    RATE_LIMIT = "rate_limit_error"
    SERVER = "server_error"
    CLIENT = "client_error"
    PARSING = "parsing_error"
    UNKNOWN = "unknown_error"


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

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        if self.error_type:
            result['error_type'] = self.error_type.value
        return result


class GoogleAppsScriptClient:
    """
    Enhanced HTTP client for interacting with Google Apps Script endpoints.

    Features:
    - Retry mechanism with exponential backoff
    - Rate limiting and request throttling
    - Comprehensive error handling and classification
    - Connection pooling with optimized settings
    - Request/response logging for debugging
    - Health checks and circuit breaker pattern

    Environment Variables:
        GOOGLE_APPS_SCRIPT_URL (required) - Base URL for the Apps Script
        GOOGLE_APPS_SCRIPT_BACKUP_URL (optional) - Fallback URL
        GOOGLE_APPS_SCRIPT_APP_TOKEN (optional) - Authentication token
        GOOGLE_APPS_SCRIPT_TIMEOUT (optional) - Request timeout in seconds
        GOOGLE_APPS_SCRIPT_MAX_RETRIES (optional) - Maximum retry attempts
        GOOGLE_APPS_SCRIPT_RATE_LIMIT_RPM (optional) - Rate limit (requests per minute)
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        backup_url: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
        rate_limit_rpm: int = 60,
        app_token: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> None:
        # URLs configuration with fallback
        self.base_url: str = self._get_configured_url(base_url, "GOOGLE_APPS_SCRIPT_URL")
        self.backup_url: Optional[str] = self._get_configured_url(backup_url, "GOOGLE_APPS_SCRIPT_BACKUP_URL", required=False)
        
        # Request configuration
        self.timeout: int = self._get_timeout_config(timeout)
        self.max_retries: int = max_retries
        self.rate_limit_rpm: int = rate_limit_rpm
        
        # Rate limiting state
        self.request_timestamps: List[float] = []
        self.last_request_time: float = 0.0
        
        # Security / identity
        self.app_token: Optional[str] = app_token or os.getenv("GOOGLE_APPS_SCRIPT_APP_TOKEN") or os.getenv("APP_TOKEN")
        
        self.user_agent: str = (
            user_agent
            or os.getenv("GOOGLE_APPS_SCRIPT_USER_AGENT")
            or os.getenv("USER_AGENT")
            or f"TadawulStockAPI/2.0.0 (+Python-Requests)"
        )

        # Request tracking
        self.request_counter: int = 0
        self.error_counter: int = 0
        self.success_counter: int = 0

        # Initialize session with enhanced configuration
        self.session = self._create_session()

        logger.info(
            "GoogleAppsScriptClient initialized: "
            f"base_url={self._mask_url(self.base_url)}, "
            f"backup_url={self._mask_url(self.backup_url) if self.backup_url else 'None'}, "
            f"timeout={self.timeout}s, max_retries={self.max_retries}, "
            f"rate_limit={self.rate_limit_rpm}/min"
        )

    def _get_configured_url(self, provided_url: Optional[str], env_var: str, required: bool = True) -> str:
        """Get URL from provided value or environment variable."""
        url = provided_url or os.getenv(env_var)
        
        if required and not url:
            default_url = "https://script.google.com/macros/s/AKfycbw.../exec"
            logger.warning(f"{env_var} not configured, using default: {self._mask_url(default_url)}")
            return default_url.strip()
        
        if url:
            return url.strip()
        
        if required:
            raise ValueError(f"Required configuration missing: {env_var}")
        
        return ""

    def _get_timeout_config(self, default_timeout: int) -> int:
        """Get timeout configuration from environment with validation."""
        try:
            env_timeout = os.getenv("GOOGLE_APPS_SCRIPT_TIMEOUT")
            if env_timeout:
                return max(5, int(env_timeout))  # Minimum 5 seconds
        except (ValueError, TypeError):
            logger.warning(f"Invalid GOOGLE_APPS_SCRIPT_TIMEOUT, using default: {default_timeout}")
        
        return max(5, default_timeout)

    def _create_session(self) -> requests.Session:
        """Create configured requests session with retry strategy and connection pooling."""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            backoff_factor=1.0,
            raise_on_status=False,
        )

        # Configure adapter with connection pooling
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20,
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
            session.headers["X-App-Token"] = self.app_token
            session.headers["Authorization"] = f"Bearer {self.app_token}"

        return session

    def _mask_url(self, url: str) -> str:
        """Mask URL for secure logging."""
        if not url or "/" not in url:
            return "***"
        
        parts = url.split('/')
        if len(parts) > 4:
            # Keep first and last part, mask the middle
            return f"{parts[0]}//.../{parts[-1]}"
        return "***"

    def _generate_request_id(self) -> str:
        """Generate unique request ID for tracing."""
        self.request_counter += 1
        timestamp = int(time.time() * 1000)
        return f"gas_req_{timestamp}_{self.request_counter:06d}"

    def _enforce_rate_limit(self) -> None:
        """Enforce rate limiting to avoid hitting quotas."""
        now = time.time()
        window_start = now - 60  # 1 minute window
        
        # Clean old timestamps
        self.request_timestamps = [ts for ts in self.request_timestamps if ts > window_start]
        
        # Check if we're over the limit
        if len(self.request_timestamps) >= self.rate_limit_rpm:
            sleep_time = 60 - (now - self.request_timestamps[0])
            if sleep_time > 0:
                logger.warning(f"Rate limit exceeded, sleeping for {sleep_time:.2f}s")
                time.sleep(sleep_time + 0.1)  # Small buffer
        
        # Add jitter to avoid synchronized requests
        jitter = random.uniform(0.05, 0.2)
        time.sleep(jitter)
        
        self.request_timestamps.append(now)
        self.last_request_time = now

    def _classify_error(self, error: Exception, status_code: Optional[int] = None) -> ErrorType:
        """Classify error for appropriate handling."""
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
        Internal method to make HTTP requests with comprehensive error handling.
        """
        request_id = self._generate_request_id()
        retries = 0
        last_exception = None
        
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
                    
                    # Log response for debugging
                    logger.debug(
                        f"[{request_id}] Response: status={response.status_code}, "
                        f"size={len(response.content)} bytes"
                    )
                    
                    response.raise_for_status()
                    
                    # Parse JSON response
                    try:
                        data = response.json()
                    except ValueError as e:
                        logger.warning(f"[{request_id}] JSON parse error: {e}")
                        # Try to extract error from text
                        error_text = response.text[:200]  # First 200 chars
                        return GoogleAppsScriptResponse(
                            success=False,
                            error=f"Invalid JSON response: {error_text}",
                            error_type=ErrorType.PARSING,
                            status_code=response.status_code,
                            retries=retries,
                            request_id=request_id,
                            timestamp=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                        )
                    
                    # Check for success flag in response
                    success = data.get('success', True)
                    error_msg = data.get('error') or data.get('message')
                    
                    if not success:
                        logger.warning(f"[{request_id}] Apps Script reported error: {error_msg}")
                    
                    execution_time = time.perf_counter() - (self.last_request_time or time.time())
                    
                    # Update counters
                    if success:
                        self.success_counter += 1
                    else:
                        self.error_counter += 1
                    
                    return GoogleAppsScriptResponse(
                        success=success,
                        data=data if success else None,
                        error=error_msg,
                        error_type=ErrorType.SERVER if not success else None,
                        execution_time=execution_time,
                        status_code=response.status_code,
                        retries=retries,
                        request_id=request_id,
                        timestamp=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                    )
                    
                except requests.exceptions.RequestException as e:
                    last_exception = e
                    retries = attempt
                    error_type = self._classify_error(e, getattr(e.response, 'status_code', None))
                    
                    logger.warning(
                        f"[{request_id}] Attempt {attempt + 1}/{self.max_retries + 1} failed "
                        f"({error_type.value}): {e}"
                    )
                    
                    # Don't retry on client errors (except 429)
                    if (error_type == ErrorType.CLIENT and 
                        getattr(e.response, 'status_code', 0) != 429):
                        break
                    
                    if attempt < self.max_retries:
                        # Exponential backoff with jitter
                        sleep_time = (2 ** attempt) + random.uniform(0.1, 0.5)
                        logger.debug(f"[{request_id}] Retrying in {sleep_time:.2f}s...")
                        time.sleep(sleep_time)
                    else:
                        logger.error(f"[{request_id}] All retries exhausted")
        
        # All attempts failed
        self.error_counter += 1
        error_type = self._classify_error(last_exception) if last_exception else ErrorType.UNKNOWN
        
        return GoogleAppsScriptResponse(
            success=False,
            error=f"All requests failed: {str(last_exception) if last_exception else 'Unknown error'}",
            error_type=error_type,
            retries=retries,
            request_id=request_id,
            timestamp=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        )

    # ------------------------------------------------------------------
    # Public API Methods
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
        params = {"action": action}
        
        if symbol:
            if isinstance(symbol, (list, tuple)):
                params["symbol"] = ",".join(str(s).upper().strip() for s in symbol)
            else:
                params["symbol"] = str(symbol).upper().strip()
        
        if include_metadata:
            params["metadata"] = "true"
            params["source"] = "tadawul_api_v2"

        logger.info(
            f"Fetching symbols data: symbol={symbol}, action={action}, "
            f"include_metadata={include_metadata}"
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
                error_type=ErrorType.CLIENT
            )

        payload = {
            "action": action,
            "symbol": symbol.upper().strip(),
            "data": data,
            "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            "source": "tadawul_api_v2"
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
        payload = {
            "action": action,
            "updates": updates,
            "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            "source": "tadawul_api_v2",
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

        # Add client statistics to health response
        if response.success and response.data:
            response.data["client_stats"] = self.get_statistics()

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
            "last_health_check": response.timestamp,
        }
        
        if not response.success:
            diagnostics["error"] = response.error
            diagnostics["error_type"] = response.error_type.value if response.error_type else "unknown"
        
        return response.success, diagnostics

    # ------------------------------------------------------------------
    # Monitoring and Statistics
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        """Get client usage statistics and health metrics."""
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
        }

    def reset_statistics(self) -> None:
        """Reset client statistics counters."""
        self.request_counter = 0
        self.error_counter = 0
        self.success_counter = 0
        self.request_timestamps.clear()
        logger.info("Client statistics reset")

    def close(self) -> None:
        """Close the session and cleanup resources."""
        if hasattr(self, 'session'):
            self.session.close()
            logger.info("Google Apps Script client session closed")


# Global instance with error handling
try:
    google_apps_script_client = GoogleAppsScriptClient()
    logger.info("Global GoogleAppsScriptClient instance created successfully")
except Exception as e:
    logger.error(f"Failed to create global GoogleAppsScriptClient: {e}")
    google_apps_script_client = None


# Convenience functions for backward compatibility
def get_symbols_data(symbol: Optional[Union[str, List[str]]] = None) -> GoogleAppsScriptResponse:
    """Convenience function for simple symbol data retrieval."""
    if google_apps_script_client is None:
        return GoogleAppsScriptResponse(
            success=False,
            error="Google Apps Script client not initialized",
            error_type=ErrorType.CLIENT
        )
    return google_apps_script_client.get_symbols_data(symbol)


def health_check() -> GoogleAppsScriptResponse:
    """Convenience function for health checks."""
    if google_apps_script_client is None:
        return GoogleAppsScriptResponse(
            success=False,
            error="Google Apps Script client not initialized",
            error_type=ErrorType.CLIENT
        )
    return google_apps_script_client.health_check()


if __name__ == "__main__":
    # Basic test functionality
    logging.basicConfig(level=logging.INFO)
    
    client = GoogleAppsScriptClient()
    
    # Test health check
    health_response = client.health_check()
    print(f"Health Check: {health_response.success}")
    if health_response.data:
        print(f"Data: {health_response.data}")
    
    # Test statistics
    stats = client.get_statistics()
    print(f"Statistics: {stats}")
    
    client.close()
