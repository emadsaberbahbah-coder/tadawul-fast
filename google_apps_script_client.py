"""
google_apps_script_client.py
------------------------------------------------------------
Google Apps Script client for Tadawul Fast Bridge – v2.2

GOALS
- Provide a robust way for backend/tools to talk to your deployed
  Google Apps Script WebApp (backup / helper, not the primary path).
- KSA tickers (.SR) are explicitly separated from GLOBAL tickers in
  the payload so Apps Script can route:
      • KSA    → Tadawul / Argaam / KSA-safe backend endpoints
      • Global → other providers (EODHD/FMP/etc., but NOT from Python).

CONFIGURATION (env.py or OS env vars)
- GOOGLE_APPS_SCRIPT_BACKUP_URL   (full WebApp URL)
- APP_TOKEN                       (sent as X-APP-TOKEN)
- BACKEND_BASE_URL                (optional; passed as metadata)

IMPORTANT
- This module NEVER calls any market data provider directly.
- It only calls your Apps Script WebApp.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("google_apps_script_client")
if not logger.handlers:
    logger.setLevel(logging.INFO)

# ----------------------------------------------------------------------
# env.py + environment integration (v2-style, KSA-safe)
# ----------------------------------------------------------------------

# We support BOTH patterns:
#   1) env.settings   (pydantic Settings instance, attributes in lowercase)
#   2) env.<VARS>     (legacy uppercase module-level attributes)
#   3) OS env vars    as a final fallback
try:  # pragma: no cover - env.py optional
    from env import settings as _settings  # type: ignore[attr-defined]
except Exception:  # pragma: no cover - defensive
    _settings = None  # type: ignore[assignment]

try:  # pragma: no cover - env.py optional
    import env as _env_mod  # type: ignore
    logger.info("[AppsScriptClient] env.py detected for configuration.")
except Exception:  # pragma: no cover
    _env_mod = None  # type: ignore[assignment]
    logger.warning(
        "[AppsScriptClient] env.py not available. Falling back to OS env vars only."
    )


def _get_env(name: str, default: str = "") -> str:
    """
    Unified config reader:

    Resolution order:
      1) env.settings.<lowercase_name>   (pydantic Settings)
      2) env.<NAME>                      (legacy uppercase attributes)
      3) os.getenv(NAME)

    Always returns a string (or default if nothing set).
    """
    # 1) pydantic-style settings (lowercase attributes)
    if _settings is not None and hasattr(_settings, name.lower()):
        val = getattr(_settings, name.lower())
        if val is not None:
            try:
                return str(val)
            except Exception:
                pass

    # 2) legacy env module uppercase attributes
    if _env_mod is not None and hasattr(_env_mod, name):
        val = getattr(_env_mod, name)
        if val is not None:
            try:
                return str(val)
            except Exception:
                pass

    # 3) OS environment
    return os.getenv(name, default)


GOOGLE_APPS_SCRIPT_BACKUP_URL: str = _get_env("GOOGLE_APPS_SCRIPT_BACKUP_URL", "").strip()
APP_TOKEN: str = _get_env("APP_TOKEN", "").strip()
BACKEND_BASE_URL: str = _get_env("BACKEND_BASE_URL", "").strip()

_SSL_CONTEXT = ssl.create_default_context()

# ----------------------------------------------------------------------
# Data structures
# ----------------------------------------------------------------------


@dataclass
class AppsScriptResult:
    """
    Normalized response from Google Apps Script WebApp.

    ok          : True if HTTP status is 2xx, else False
    status_code : HTTP status code (0 if no response)
    data        : Parsed JSON (dict/list) or raw content
    error       : Short error description (if any)
    raw_text    : Original response body as text (if available)
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

    Python NEVER calls EODHD or any provider here.
    This split is just to help Apps Script / backend route correctly.
    """
    ksa: List[str] = []
    global_: List[str] = []

    for t in tickers or []:
        t_clean = (t or "").strip()
        if not t_clean:
            continue
        up = t_clean.upper()
        if up.endswith(".SR"):
            ksa.append(up)
        else:
            global_.append(up)

    return ksa, global_


# ----------------------------------------------------------------------
# Client class
# ----------------------------------------------------------------------


class GoogleAppsScriptClient:
    """
    Lightweight client to call a deployed Google Apps Script WebApp.

    - Uses only stdlib (urllib) – no extra dependencies.
    - Automatically includes APP_TOKEN via X-APP-TOKEN header if set.
    - Provides sync + async wrappers.

    Typical usage:
        client = GoogleAppsScriptClient()
        res = client.call_script(
            mode="refresh_quotes",
            payload={...},
        )
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        app_token: Optional[str] = None,
        timeout: float = 30.0,
    ) -> None:
        raw_url = (base_url or GOOGLE_APPS_SCRIPT_BACKUP_URL or "").strip()

        # If configured without scheme, assume https://
        if raw_url and not raw_url.startswith(("http://", "https://")):
            logger.warning(
                "[AppsScriptClient] GOOGLE_APPS_SCRIPT_BACKUP_URL has no scheme, "
                "assuming https://%s",
                raw_url,
            )
            raw_url = "https://" + raw_url

        self.base_url: str = raw_url.rstrip("/")
        self.app_token: Optional[str] = (app_token or APP_TOKEN or "").strip()
        self.timeout: float = timeout
        self._ssl_context = _SSL_CONTEXT

        if not self.base_url:
            logger.warning(
                "[AppsScriptClient] GOOGLE_APPS_SCRIPT_BACKUP_URL is not configured. "
                "Client will not be able to call Apps Script."
            )
        else:
            logger.info(
                "[AppsScriptClient] Initialized with base_url=%s (backend=%s)",
                self.base_url,
                BACKEND_BASE_URL or "(not set)",
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
            Logical mode / operation. Will be appended as query parameter
            'mode=...' so Apps Script can switch on it.
        payload: dict | None
            JSON body for POST/PUT calls; ignored for GET.
        method: str
            'GET' or 'POST'. POST is recommended for non-trivial payloads.
        extra_query: dict | None
            Additional query parameters to append to the URL.

        Returns
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

        # Build URL with merged query
        url_parts = list(urllib.parse.urlparse(self.base_url))
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
            logger.error("URLError calling Apps Script (%s): %s", url, exc)
            return AppsScriptResult(
                ok=False,
                status_code=0,
                data=None,
                error=f"URLError: {exc}",
            )

        except Exception as exc:
            logger.exception("Unexpected error calling Apps Script (%s)", url)
            return AppsScriptResult(
                ok=False,
                status_code=0,
                data=None,
                error=f"Unexpected error: {exc}",
            )

    # ------------------------------------------------------------------
    # Async wrapper (for FastAPI / async workers)
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
    # Opinionated helpers for quote sheets
    # ------------------------------------------------------------------

    def build_quotes_payload(
        self,
        sheet_id: str,
        sheet_name: str,
        tickers: List[str],
        extra_meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Build a standardized payload for "quotes to sheet" operations.

        - Automatically splits tickers into:
              • ksa_tickers    (.SR only)
              • global_tickers (everything else)
        - Includes backend metadata so Apps Script can, if needed, call
          your FastAPI backend (/v1/enriched, /v1/argaam, etc.).
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

        Apps Script receives:
        - ?mode=<mode> in query
        - JSON body from build_quotes_payload(...)
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


def call_script(
    mode: str,
    payload: Optional[Dict[str, Any]] = None,
    method: str = "POST",
    extra_query: Optional[Dict[str, str]] = None,
) -> AppsScriptResult:
    """
    Function-style shortcut to apps_script_client.call_script(...).
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
    Async function-style shortcut to apps_script_client.call_script_async(...).
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


__all__ = [
    "AppsScriptResult",
    "GoogleAppsScriptClient",
    "apps_script_client",
    "get_apps_script_client",
    "split_tickers_by_market",
    "call_script",
    "call_script_async",
    "sync_quotes_to_sheet",
    "sync_quotes_to_sheet_async",
]
