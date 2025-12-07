"""
google_apps_script_client.py
------------------------------------------------------------
Google Apps Script client for Tadawul Fast Bridge

GOAL
- Provide a robust way for the backend (or management scripts) to talk
  to your deployed Google Apps Script WebApp.
- Support live data integration with Google Sheets as a backup or helper
  to direct Google Sheets API usage (google_sheets_service.py).
- Explicitly handle KSA tickers (e.g. 1120.SR) separately from GLOBAL
  tickers because EODHD does NOT work reliably for KSA financial markets.

KEY IDEAS
- Central client using:
    • GOOGLE_APPS_SCRIPT_BACKUP_URL  (from env)
    • APP_TOKEN (sent as X-APP-TOKEN header if set)
- Safe helpers:
    • split_tickers_by_market() -> (ksa, global)
    • build_quotes_payload(...)  # designed for KSA + Global tickers
- Generic call methods:
    • call_script(...)           # sync
    • call_script_async(...)     # async wrapper
    • sync_quotes_to_sheet(...)  # opinionated helper for quotes sheets

USAGE EXAMPLES
--------------
    from google_apps_script_client import (
        apps_script_client,
        sync_quotes_to_sheet,
    )

    # Simple sync call (e.g. from CLI script)
    result = apps_script_client.call_script(
        mode="healthCheck",
        payload={"ping": "hello from backend"},
    )

    # Async usage inside FastAPI background task
    await sync_quotes_to_sheet(
        sheet_id="your-sheet-id",
        sheet_name="KSA_Tadawul",
        tickers=["1120.SR", "1180.SR", "AAPL", "MSFT"],
        mode="refresh_quotes",
    )
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from env import (
    GOOGLE_APPS_SCRIPT_BACKUP_URL,
    APP_TOKEN,
    BACKEND_BASE_URL,
)

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Data structures
# ----------------------------------------------------------------------


@dataclass
class AppsScriptResult:
    """
    Normalized response from Google Apps Script WebApp.
    """

    ok: bool
    status_code: int
    data: Any
    error: Optional[str] = None
    raw_text: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "status_code": self.status_code,
            "data": self.data,
            "error": self.error,
            "raw_text": self.raw_text,
        }


# ----------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------


def split_tickers_by_market(tickers: List[str]) -> Tuple[List[str], List[str]]:
    """
    Split a list of tickers into KSA vs Global buckets.

    Assumptions:
    - KSA tickers end with '.SR' (e.g. 1120.SR, 1180.SR, 1211.SR)
    - EVERYTHING ELSE is treated as 'global' (AAPL, MSFT, NVDA,...)

    This is important because:
    - EODHD is NOT working for KSA.
    - KSA tickers should be routed to Tadawul / Argaam providers through
      your KSA-specific endpoints (e.g. routes_argaam.py) on the Apps Script side.
    """
    ksa: List[str] = []
    global_: List[str] = []

    for t in tickers or []:
        t_clean = (t or "").strip()
        if not t_clean:
            continue
        if t_clean.upper().endswith(".SR"):
            ksa.append(t_clean.upper())
        else:
            global_.append(t_clean.upper())

    return ksa, global_


def _default_ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    return ctx


# ----------------------------------------------------------------------
# Client class
# ----------------------------------------------------------------------


class GoogleAppsScriptClient:
    """
    Lightweight client to call a deployed Google Apps Script WebApp.

    - Uses only the standard library (urllib) to avoid new dependencies.
    - Automatically includes APP_TOKEN via X-APP-TOKEN header if available.
    - Provides sync + async wrappers.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        app_token: Optional[str] = None,
        timeout: float = 30.0,
    ) -> None:
        self.base_url: str = (base_url or GOOGLE_APPS_SCRIPT_BACKUP_URL or "").strip()
        self.app_token: Optional[str] = app_token or APP_TOKEN
        self.timeout: float = timeout
        self._ssl_context = _default_ssl_context()

        if not self.base_url:
            logger.warning(
                "[GoogleAppsScriptClient] GOOGLE_APPS_SCRIPT_BACKUP_URL is not set. "
                "Client will not be able to call Apps Script."
            )
        else:
            logger.info(
                "[GoogleAppsScriptClient] Configured with base_url=%s (backend=%s)",
                self.base_url,
                BACKEND_BASE_URL,
            )

    # ------------------------------------------------------------------
    # Core HTTP logic (sync)
    # ------------------------------------------------------------------

    def call_script(
        self,
        mode: str,
        payload: Optional[Dict[str, Any]] = None,
        method: str = "POST",
        extra_query: Optional[Dict[str, str]] = None,
    ) -> AppsScriptResult:
        """
        Perform a call to the Apps Script WebApp.

        Parameters
        ----------
        mode: str
            Logical mode / operation. Will be appended as a query parameter
            'mode=...' so your Apps Script can switch on it.
        payload: dict | None
            JSON body for POST/PUT calls; ignored for GET.
        method: str
            'GET' or 'POST'. POST is recommended for non-trivial payloads.
        extra_query: dict | None
            Additional query parameters to append to the URL.

        RETURNS
        -------
        AppsScriptResult
        """
        if not self.base_url:
            return AppsScriptResult(
                ok=False,
                status_code=0,
                data=None,
                error="GOOGLE_APPS_SCRIPT_BACKUP_URL is not configured",
            )

        method = (method or "POST").upper()
        query_params: Dict[str, str] = {"mode": mode}
        if extra_query:
            query_params.update({k: str(v) for k, v in extra_query.items()})

        # Build URL with query
        url_parts = list(urllib.parse.urlparse(self.base_url))
        # Merge or extend existing query
        existing_qs = dict(urllib.parse.parse_qsl(url_parts[4]))
        existing_qs.update(query_params)
        url_parts[4] = urllib.parse.urlencode(existing_qs)
        url = urllib.parse.urlunparse(url_parts)

        headers: Dict[str, str] = {
            "Content-Type": "application/json; charset=utf-8",
        }
        if self.app_token:
            headers["X-APP-TOKEN"] = self.app_token

        data_bytes: Optional[bytes] = None
        if method != "GET" and payload is not None:
            try:
                data_bytes = json.dumps(payload).encode("utf-8")
            except Exception as exc:
                logger.error("Failed to JSON-encode payload for Apps Script: %s", exc)
                return AppsScriptResult(
                    ok=False,
                    status_code=0,
                    data=None,
                    error=f"JSON encode error: {exc}",
                )

        req = urllib.request.Request(url, data=data_bytes, headers=headers, method=method)

        try:
            with urllib.request.urlopen(
                req, timeout=self.timeout, context=self._ssl_context
            ) as resp:
                status_code = getattr(resp, "status", resp.getcode())
                raw = resp.read()
                text = raw.decode("utf-8", errors="replace")
                try:
                    parsed = json.loads(text)
                except Exception:
                    parsed = {"raw": text}

                ok = 200 <= status_code < 300
                return AppsScriptResult(
                    ok=ok,
                    status_code=status_code,
                    data=parsed,
                    raw_text=text,
                    error=None if ok else f"HTTP {status_code}",
                )

        except urllib.error.HTTPError as exc:
            text = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
            logger.warning(
                "HTTPError from Apps Script: status=%s, body=%s",
                exc.code,
                text[:300],
            )
            try:
                parsed = json.loads(text)
            except Exception:
                parsed = {"raw": text}
            return AppsScriptResult(
                ok=False,
                status_code=exc.code,
                data=parsed,
                raw_text=text,
                error=f"HTTPError {exc.code}: {exc.reason}",
            )

        except urllib.error.URLError as exc:
            logger.error("URLError calling Apps Script: %s", exc)
            return AppsScriptResult(
                ok=False,
                status_code=0,
                data=None,
                error=f"URLError: {exc}",
            )

        except Exception as exc:
            logger.exception("Unexpected error calling Apps Script")
            return AppsScriptResult(
                ok=False,
                status_code=0,
                data=None,
                error=f"Unexpected error: {exc}",
            )

    # ------------------------------------------------------------------
    # Async wrapper (optional, for use inside FastAPI)
    # ------------------------------------------------------------------

    async def call_script_async(
        self,
        mode: str,
        payload: Optional[Dict[str, Any]] = None,
        method: str = "POST",
        extra_query: Optional[Dict[str, str]] = None,
    ) -> AppsScriptResult:
        """
        Async wrapper around call_script() using a thread executor.
        Safe to use inside async FastAPI endpoints or background tasks.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, lambda: self.call_script(mode, payload, method, extra_query)
        )

    # ------------------------------------------------------------------
    # Opinionated helper for quote sheets
    # ------------------------------------------------------------------

    def build_quotes_payload(
        self,
        sheet_id: str,
        sheet_name: str,
        tickers: List[str],
        extra_meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Build a standardized payload for 'quotes to sheet' operations.

        Important:
        - Automatically splits tickers into:
            • ksa_tickers   -> for Tadawul/Argaam routes (EODHD not used)
            • global_tickers -> for EODHD/FMP/etc.
        - Includes app/backend metadata so Apps Script can decide how to call
          /v1/quote, /v1/enriched, /v1/argaam, etc.

        Apps Script can read:
            payload["ksa_tickers"]
            payload["global_tickers"]
            payload["all_tickers"]
            payload["sheet"]["id"]
            payload["sheet"]["name"]
            payload["backend"]["base_url"]
        """
        ksa_tickers, global_tickers = split_tickers_by_market(tickers)

        sheet_info = {
            "id": sheet_id,
            "name": sheet_name,
        }
        backend_info = {
            "base_url": BACKEND_BASE_URL,
        }

        base_payload: Dict[str, Any] = {
            "ksa_tickers": ksa_tickers,
            "global_tickers": global_tickers,
            "all_tickers": ksa_tickers + global_tickers,
            "sheet": sheet_info,
            "backend": backend_info,
        }

        if extra_meta:
            base_payload["meta"] = extra_meta

        return base_payload

    def sync_quotes_to_sheet(
        self,
        sheet_id: str,
        sheet_name: str,
        tickers: List[str],
        mode: str = "refresh_quotes",
        extra_meta: Optional[Dict[str, Any]] = None,
    ) -> AppsScriptResult:
        """
        High-level helper: instruct Apps Script to refresh a quotes sheet.

        Parameters
        ----------
        sheet_id: str
            The Spreadsheet ID (or an alias your Apps Script understands).
        sheet_name: str
            Target sheet/tab name (e.g. 'KSA_Tadawul', 'Global_Markets').
        tickers: list[str]
            List of symbols to refresh (KSA + global mixed).
        mode: str
            Logical operation name. Defaults to 'refresh_quotes'.
            Apps Script will receive it as ?mode=refresh_quotes.
        extra_meta: dict | None
            Extra metadata to send (e.g. range info, layout version).

        Returns
        -------
        AppsScriptResult
        """
        payload = self.build_quotes_payload(
            sheet_id=sheet_id,
            sheet_name=sheet_name,
            tickers=tickers,
            extra_meta=extra_meta,
        )
        return self.call_script(mode=mode, payload=payload, method="POST")

    async def sync_quotes_to_sheet_async(
        self,
        sheet_id: str,
        sheet_name: str,
        tickers: List[str],
        mode: str = "refresh_quotes",
        extra_meta: Optional[Dict[str, Any]] = None,
    ) -> AppsScriptResult:
        """
        Async version of sync_quotes_to_sheet().
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: self.sync_quotes_to_sheet(
                sheet_id=sheet_id,
                sheet_name=sheet_name,
                tickers=tickers,
                mode=mode,
                extra_meta=extra_meta,
            ),
        )


# ----------------------------------------------------------------------
# Module-level singleton and convenience functions
# ----------------------------------------------------------------------

apps_script_client = GoogleAppsScriptClient()


def get_apps_script_client() -> GoogleAppsScriptClient:
    """
    Convenience accessor, in case you want to inject/mocks later.
    """
    return apps_script_client


# Shortcuts for callers that prefer function-style API -----------------


def call_script(
    mode: str,
    payload: Optional[Dict[str, Any]] = None,
    method: str = "POST",
    extra_query: Optional[Dict[str, str]] = None,
) -> AppsScriptResult:
    """
    Function-style shortcut to apps_script_client.call_script(...)
    """
    return apps_script_client.call_script(
        mode=mode,
        payload=payload,
        method=method,
        extra_query=extra_query,
    )


async def call_script_async(
    mode: str,
    payload: Optional[Dict[str, Any]] = None,
    method: str = "POST",
    extra_query: Optional[Dict[str, str]] = None,
) -> AppsScriptResult:
    """
    Async function-style shortcut to apps_script_client.call_script_async(...)
    """
    return await apps_script_client.call_script_async(
        mode=mode,
        payload=payload,
        method=method,
        extra_query=extra_query,
    )


def sync_quotes_to_sheet(
    sheet_id: str,
    sheet_name: str,
    tickers: List[str],
    mode: str = "refresh_quotes",
    extra_meta: Optional[Dict[str, Any]] = None,
) -> AppsScriptResult:
    """
    Function-style shortcut for quotes sheet refresh.
    """
    return apps_script_client.sync_quotes_to_sheet(
        sheet_id=sheet_id,
        sheet_name=sheet_name,
        tickers=tickers,
        mode=mode,
        extra_meta=extra_meta,
    )


async def sync_quotes_to_sheet_async(
    sheet_id: str,
    sheet_name: str,
    tickers: List[str],
    mode: str = "refresh_quotes",
    extra_meta: Optional[Dict[str, Any]] = None,
) -> AppsScriptResult:
    """
    Async function-style shortcut for quotes sheet refresh.
    """
    return await apps_script_client.sync_quotes_to_sheet_async(
        sheet_id=sheet_id,
        sheet_name=sheet_name,
        tickers=tickers,
        mode=mode,
        extra_meta=extra_meta,
    )
