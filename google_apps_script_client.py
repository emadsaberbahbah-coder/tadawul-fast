# google_apps_script_client.py
from __future__ import annotations

import os
import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Union

import requests

logger = logging.getLogger(__name__)


@dataclass
class GoogleAppsScriptResponse:
    """
    Standardized response wrapper for Google Apps Script calls.
    """
    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    execution_time: Optional[float] = None
    status_code: Optional[int] = None


class GoogleAppsScriptClient:
    """
    Thin HTTP client for interacting with your Google Apps Script endpoint.

    - Uses env vars:
        GOOGLE_APPS_SCRIPT_URL          (base URL – required/with default)
        GOOGLE_APPS_SCRIPT_APP_TOKEN    (optional app token)
        GOOGLE_APPS_SCRIPT_USER_AGENT   (optional user agent override)
        USER_AGENT                      (fallback user agent)
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: int = 30,
        app_token: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> None:
        # Base URL
        self.base_url: str = base_url or os.getenv(
            "GOOGLE_APPS_SCRIPT_URL",
            "https://script.google.com/macros/s/AKfycbwnIX0hIaffDJVnHZUxej4zoLPQZgpdMMpkA9YP1xPQVxqwvEAXuIHWcF7qBIVsntnLkg/exec",  # your default
        ).strip()

        # Timeout
        self.timeout: int = timeout

        # Security / identity
        self.app_token: Optional[str] = app_token or os.getenv(
            "GOOGLE_APPS_SCRIPT_APP_TOKEN"
        ) or os.getenv("APP_TOKEN")

        self.user_agent: str = (
            user_agent
            or os.getenv("GOOGLE_APPS_SCRIPT_USER_AGENT")
            or os.getenv("USER_AGENT")
            or "StockMarketHub/GoogleAppsScriptClient (+python-requests)"
        )

        # Use a session for connection pooling & shared headers
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": self.user_agent,
            }
        )
        if self.app_token:
            # Align with the rest of the project – common header name
            self.session.headers["X-App-Token"] = self.app_token

        logger.info(
            "GoogleAppsScriptClient initialized: base_url=%s, timeout=%s",
            self.base_url,
            self.timeout,
        )

    # ------------------------------------------------------------------
    # Helper
    # ------------------------------------------------------------------
    def _build_params(
        self, extra: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Build querystring params for GET requests, keeping space for future
        additions (e.g., 'action', 'source', etc.).
        """
        params: Dict[str, Any] = {}
        if extra:
            params.update(extra)
        return params

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def get_symbols_data(
        self,
        symbol: Optional[Union[str, List[str]]] = None,
        action: str = "get",
    ) -> GoogleAppsScriptResponse:
        """
        Get symbols data from Google Apps Script.

        - symbol=None      → fetch all symbols (depending on Apps Script logic)
        - symbol="1120.SR" → fetch single symbol
        - symbol=[...]     → fetch multiple symbols (joined by commas)
        """
        try:
            # Prepare params
            params: Dict[str, Any] = {"action": action}

            if symbol:
                if isinstance(symbol, (list, tuple)):
                    joined = ",".join(s.upper() for s in symbol)
                    params["symbol"] = joined
                else:
                    params["symbol"] = str(symbol).upper()

            query_params = self._build_params(params)

            logger.debug(
                "Requesting symbols data from Google Apps Script: url=%s, params=%s",
                self.base_url,
                query_params,
            )

            start = time.perf_counter()
            response = self.session.get(
                self.base_url,
                params=query_params,
                timeout=self.timeout,
            )
            elapsed = time.perf_counter() - start

            status_code = response.status_code
            logger.debug(
                "Google Apps Script GET response: status=%s, elapsed=%.3fs",
                status_code,
                elapsed,
            )

            # Raise for 4xx/5xx
            response.raise_for_status()

            data = response.json()

            # If Apps Script already returns a "success" flag, respect it;
            # otherwise assume success since HTTP was 2xx.
            success = bool(data.get("success", True))

            # If the script encodes an error message in JSON
            error_msg: Optional[str] = None
            if not success:
                error_msg = data.get("error") or data.get("message")

            return GoogleAppsScriptResponse(
                success=success,
                data=data if success else None,
                error=error_msg,
                execution_time=elapsed,
                status_code=status_code,
            )

        except requests.exceptions.RequestException as e:
            logger.error("Google Apps Script GET request failed: %s", e, exc_info=True)
            return GoogleAppsScriptResponse(
                success=False,
                error=f"Request failed: {e}",
            )
        except ValueError as e:
            # JSON parsing error
            logger.error("Google Apps Script JSON parse failed: %s", e, exc_info=True)
            return GoogleAppsScriptResponse(
                success=False,
                error=f"JSON parse failed: {e}",
            )

    def update_symbol_data(
        self,
        symbol: str,
        data: Dict[str, Any],
        action: str = "update",
    ) -> GoogleAppsScriptResponse:
        """
        Update symbol data via Google Apps Script (POST).

        The Apps Script side is expected to:
        - read JSON body: { action, symbol, data }
        - return JSON with a `success` flag and optional `error` field.
        """
        try:
            payload: Dict[str, Any] = {
                "action": action,
                "symbol": symbol.upper(),
                "data": data,
            }

            logger.debug(
                "Sending symbol update to Google Apps Script: url=%s, payload=%s",
                self.base_url,
                payload,
            )

            start = time.perf_counter()
            response = self.session.post(
                self.base_url,
                json=payload,
                timeout=self.timeout,
            )
            elapsed = time.perf_counter() - start
            status_code = response.status_code

            logger.debug(
                "Google Apps Script POST response: status=%s, elapsed=%.3fs",
                status_code,
                elapsed,
            )

            response.raise_for_status()

            result = response.json()
            success = bool(result.get("success", True))
            error_msg: Optional[str] = None
            if not success:
                error_msg = result.get("error") or result.get("message")

            return GoogleAppsScriptResponse(
                success=success,
                data=result if success else None,
                error=error_msg,
                execution_time=elapsed,
                status_code=status_code,
            )

        except requests.exceptions.RequestException as e:
            logger.error("Google Apps Script update failed: %s", e, exc_info=True)
            return GoogleAppsScriptResponse(
                success=False,
                error=f"Update failed: {e}",
            )
        except ValueError as e:
            logger.error("Google Apps Script JSON parse failed (update): %s", e, exc_info=True)
            return GoogleAppsScriptResponse(
                success=False,
                error=f"JSON parse failed: {e}",
            )

    def health_check(self) -> GoogleAppsScriptResponse:
        """
        Lightweight health check for the Apps Script endpoint.
        Useful for diagnostics / /health endpoint in FastAPI.
        """
        try:
            params = self._build_params({"ping": "1"})
            start = time.perf_counter()
            response = self.session.get(
                self.base_url,
                params=params,
                timeout=self.timeout,
            )
            elapsed = time.perf_counter() - start
            status_code = response.status_code

            logger.debug(
                "Google Apps Script health check: status=%s, elapsed=%.3fs",
                status_code,
                elapsed,
            )

            ok = response.ok
            data: Dict[str, Any] = {}
            try:
                data = response.json()
            except ValueError:
                # Not critical for health check
                data = {"raw_text": response.text}

            return GoogleAppsScriptResponse(
                success=ok,
                data=data if ok else None,
                error=None if ok else "Health check failed",
                execution_time=elapsed,
                status_code=status_code,
            )

        except requests.exceptions.RequestException as e:
            logger.error("Google Apps Script health check failed: %s", e, exc_info=True)
            return GoogleAppsScriptResponse(
                success=False,
                error=f"Health check failed: {e}",
            )


# Global instance – used by FastAPI routes and other modules
google_apps_script_client = GoogleAppsScriptClient()
