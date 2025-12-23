"""
google_apps_script_client.py
------------------------------------------------------------
Google Apps Script client for Tadawul Fast Bridge – v2.3.0

GOALS
- Provide a robust way for backend/tools to talk to your deployed
  Google Apps Script WebApp (backup / helper).
- Routes tickers intelligently:
      • KSA (.SR or numeric) -> Apps Script KSA logic (Argaam/Tadawul)
      • Global               -> Apps Script Global logic (EODHD/FMP)

CONFIGURATION
- Reads directly from `env.py` settings.

IMPORTANT
- This module NEVER calls market providers directly.
- It is a bridge to the Google ecosystem.
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

# --- CONFIGURATION IMPORT ---
try:
    from env import settings
    # Fallback constants if needed for legacy imports
    _DEFAULT_URL = settings.google_apps_script_backup_url
    _DEFAULT_TOKEN = settings.app_token
    _BACKEND_URL = settings.backend_base_url
except ImportError:
    # Fallback for environments without the full app stack
    import os
    settings = None # type: ignore
    _DEFAULT_URL = os.getenv("GOOGLE_APPS_SCRIPT_BACKUP_URL", "")
    _DEFAULT_TOKEN = os.getenv("APP_TOKEN", "")
    _BACKEND_URL = os.getenv("BACKEND_BASE_URL", "")

logger = logging.getLogger("google_apps_script_client")
_SSL_CONTEXT = ssl.create_default_context()

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

    Logic:
    - KSA: Ends with '.SR' OR is purely numeric (e.g., '1120', '2222')
    - Global: Everything else (e.g., 'AAPL', 'BTC-USD', 'EURUSD')
    """
    ksa: List[str] = []
    global_: List[str] = []

    for t in tickers or []:
        t_clean = (t or "").strip().upper()
        if not t_clean:
            continue
        
        # Check KSA criteria
        is_ksa = False
        if t_clean.endswith(".SR"):
            is_ksa = True
        elif t_clean.endswith(".TADAWUL"):
            is_ksa = True
        elif t_clean.isdigit() and 3 <= len(t_clean) <= 5:
            # Numeric tickers are invariably Tadawul in this context
            is_ksa = True
            
        if is_ksa:
            ksa.append(t_clean)
        else:
            global_.append(t_clean)

    return ksa, global_


# ----------------------------------------------------------------------
# Client class
# ----------------------------------------------------------------------

class GoogleAppsScriptClient:
    """
    Lightweight client to call a deployed Google Apps Script WebApp.
    Uses urllib standard library for maximum portability.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        app_token: Optional[str] = None,
        timeout: float = 45.0,
    ) -> None:
        raw_url = (base_url or _DEFAULT_URL or "").strip()

        # Fix missing scheme
        if raw_url and not raw_url.startswith(("http://", "https://")):
            raw_url = "https://" + raw_url

        self.base_url: str = raw_url.rstrip("/")
        self.app_token: Optional[str] = (app_token or _DEFAULT_TOKEN or "").strip()
        self.timeout: float = float(timeout)
        self._ssl_context = _SSL_CONTEXT

        if not self.base_url:
            logger.warning("[AppsScriptClient] No URL configured. Calls will fail.")

    def call_script(
        self,
        mode: str,
        payload: Optional[Dict[str, Any]] = None,
        method: str = "POST",
        extra_query: Optional[Dict[str, str]] = None,
        retries: int = 2
    ) -> AppsScriptResult:
        """
        Perform a call to the Apps Script WebApp with retry logic.
        """
        if not self.base_url:
            return AppsScriptResult(False, 0, None, "GOOGLE_APPS_SCRIPT_BACKUP_URL not configured")

        method = (method or "POST").upper()
        
        # 1. Build Query Params
        query_params: Dict[str, str] = {"mode": mode}
        if extra_query:
            query_params.update({k: str(v) for k, v in extra_query.items()})

        # 2. Construct URL
        url_parts = list(urllib.parse.urlparse(self.base_url))
        existing_qs = dict(urllib.parse.parse_qsl(url_parts[4]))
        existing_qs.update(query_params)
        url_parts[4] = urllib.parse.urlencode(existing_qs)
        url = urllib.parse.urlunparse(url_parts)

        # 3. Build Headers & Body
        headers: Dict[str, str] = {"Content-Type": "application/json; charset=utf-8"}
        if self.app_token:
            headers["X-APP-TOKEN"] = self.app_token

        data_bytes: Optional[bytes] = None
        if method != "GET" and payload is not None:
            try:
                data_bytes = json.dumps(payload).encode("utf-8")
            except Exception as exc:
                return AppsScriptResult(False, 0, None, f"JSON Encode Error: {exc}")

        # 4. Execute with Retry
        last_error = None
        
        for attempt in range(retries + 1):
            try:
                req = urllib.request.Request(url, data=data_bytes, headers=headers, method=method)
                
                with urllib.request.urlopen(req, timeout=self.timeout, context=self._ssl_context) as resp:
                    status_code = getattr(resp, "status", resp.getcode())
                    raw = resp.read()
                    text = raw.decode("utf-8", errors="replace")
                    
                    try:
                        parsed = json.loads(text)
                    except Exception:
                        parsed = {"raw": text}

                    ok = 200 <= status_code < 300
                    return AppsScriptResult(ok, status_code, parsed, None if ok else f"HTTP {status_code}", text)

            except urllib.error.HTTPError as exc:
                # HTTP errors (4xx, 5xx) are "success" in terms of networking, but failures for logic
                text = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
                try:
                    parsed = json.loads(text)
                except Exception:
                    parsed = {"raw": text}
                
                # Don't retry 4xx errors (client fault), do retry 5xx (server fault)
                if exc.code < 500:
                    return AppsScriptResult(False, exc.code, parsed, f"HTTP {exc.code}: {exc.reason}", text)
                
                last_error = f"HTTP {exc.code}"
                
            except Exception as exc:
                last_error = str(exc)
            
            # If we are here, an exception occurred. Wait before retry.
            if attempt < retries:
                sleep_time = (attempt + 1) * 2
                logger.warning(f"[AppsScriptClient] Attempt {attempt+1} failed ({last_error}). Retrying in {sleep_time}s...")
                time.sleep(sleep_time)

        return AppsScriptResult(False, 0, None, f"Max retries exceeded. Last error: {last_error}")

    # ------------------------------------------------------------------
    # Async wrapper
    # ------------------------------------------------------------------

    async def call_script_async(
        self,
        mode: str,
        payload: Optional[Dict[str, Any]] = None,
        method: str = "POST",
        extra_query: Optional[Dict[str, str]] = None,
    ) -> AppsScriptResult:
        """Async wrapper using thread executor."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, lambda: self.call_script(mode, payload, method, extra_query)
        )

    # ------------------------------------------------------------------
    # Specific Operations
    # ------------------------------------------------------------------

    def build_quotes_payload(
        self,
        sheet_id: str,
        sheet_name: str,
        tickers: List[str],
        extra_meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Construct the payload expected by the GAS `doPost` function.
        Splits tickers so GAS knows which function (Argaam vs EOD) to call.
        """
        ksa_tickers, global_tickers = split_tickers_by_market(tickers)

        base_payload: Dict[str, Any] = {
            "ksa_tickers": ksa_tickers,
            "global_tickers": global_tickers,
            "all_tickers": ksa_tickers + global_tickers,
            "sheet": {
                "id": sheet_id,
                "name": sheet_name,
            },
            "backend": {
                "base_url": _BACKEND_URL,
            },
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
    ) -> AppsScriptResult:
        """Sync quotes logic wrapper."""
        payload = self.build_quotes_payload(sheet_id, sheet_name, tickers)
        return self.call_script(mode=mode, payload=payload)


# ----------------------------------------------------------------------
# Singleton
# ----------------------------------------------------------------------

apps_script_client = GoogleAppsScriptClient()

def get_apps_script_client() -> GoogleAppsScriptClient:
    return apps_script_client

__all__ = [
    "AppsScriptResult",
    "GoogleAppsScriptClient",
    "apps_script_client",
    "split_tickers_by_market",
]
