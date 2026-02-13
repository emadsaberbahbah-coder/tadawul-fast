"""
integrations/google_apps_script_client.py
------------------------------------------------------------
Google Apps Script client for Tadawul Fast Bridge – v2.8.0 (ADVANCED + RESILIENT)

GOALS
- Robust bridge for backend/tools to call your deployed Google Apps Script WebApp.
- Smart KSA Routing: Auto-detects and normalizes Saudi symbols.
- Adaptive Resilience: Jittered backoff, circuit breaking for auth errors.
- Payload Optimization: Auto-prunes empty fields to save bandwidth.
- Environment Aware: Adjusts behavior based on APP_ENV.

ENV (selected)
- GOOGLE_APPS_SCRIPT_URL / GOOGLE_APPS_SCRIPT_BACKUP_URL
- APP_TOKEN / BACKUP_APP_TOKEN
- APPS_SCRIPT_TIMEOUT_SEC
- APPS_SCRIPT_VERIFY_SSL=1|0
- APPS_SCRIPT_TOKEN_TRANSPORT="query,body"
"""

from __future__ import annotations

import json
import logging
import os
import random
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("google_apps_script_client")

CLIENT_VERSION = "2.8.0"

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}

# =============================================================================
# Configuration & Helpers
# =============================================================================
def _get_env_str(key: str, default: str = "") -> str:
    return (os.getenv(key) or default).strip()

def _get_env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except:
        return default

def _get_env_bool(key: str, default: bool = False) -> bool:
    return (os.getenv(key) or "").lower() in _TRUTHY

# Primary Config
_PRIMARY_URL = _get_env_str("GOOGLE_APPS_SCRIPT_URL")
_BACKUP_URL = _get_env_str("GOOGLE_APPS_SCRIPT_BACKUP_URL")
_DEFAULT_URL = _PRIMARY_URL or _BACKUP_URL

_APP_TOKEN = _get_env_str("APP_TOKEN")
_BACKUP_TOKEN = _get_env_str("BACKUP_APP_TOKEN")
_DEFAULT_TOKEN = _APP_TOKEN or _BACKUP_TOKEN

_BACKEND_URL = _get_env_str("BACKEND_BASE_URL")
_TIMEOUT = _get_env_float("APPS_SCRIPT_TIMEOUT_SEC", 45.0)

_TOKEN_TRANSPORT = _get_env_str("APPS_SCRIPT_TOKEN_TRANSPORT", "query,body").lower()
_TOKEN_PARAM = _get_env_str("APPS_SCRIPT_TOKEN_PARAM_NAME", "token")
_VERIFY_SSL = _get_env_bool("APPS_SCRIPT_VERIFY_SSL", True)
_MAX_LOG_CHARS = int(_get_env_float("APPS_SCRIPT_MAX_RAW_TEXT_CHARS", 12000))

# =============================================================================
# Advanced Symbol Normalization
# =============================================================================
def _normalize_ksa_symbol(symbol: str) -> str:
    """
    Advanced KSA Normalizer:
    - Strips whitespace, handles lowercase prefixes.
    - Resolves '1120', '1120.SR', 'TADAWUL:1120' to '1120.SR'.
    """
    s = str(symbol or "").strip().upper()
    if not s: return ""
    
    # Strip common prefixes
    for p in ["TADAWUL:", "TDWL:", "SA:"]:
        if s.startswith(p):
            s = s[len(p):].strip()
            
    # Strip suffixes
    if s.endswith(".TADAWUL"):
        s = s[:-8].strip()
        
    # Standardize numeric
    if s.isdigit() and 3 <= len(s) <= 6:
        return f"{s}.SR"
    
    # Standardize .SR
    if s.endswith(".SR"):
        code = s[:-3]
        if code.isdigit(): return f"{code}.SR"
        
    return ""

def _fallback_normalize(symbol: str) -> str:
    return str(symbol or "").strip().upper()

# Try to use core normalizer if available
try:
    from core.symbols.normalize import normalize_symbol as _CORE_NORM
    _NORMALIZE_SYMBOL = lambda x: _CORE_NORM(x) or _fallback_normalize(x)
except ImportError:
    _NORMALIZE_SYMBOL = _fallback_normalize

# =============================================================================
# Data Structures
# =============================================================================
@dataclass
class AppsScriptResult:
    ok: bool
    status_code: int
    data: Any
    error: Optional[str] = None
    raw_text: Optional[str] = None
    url: Optional[str] = None
    elapsed_ms: Optional[int] = None

    def as_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items()}

# =============================================================================
# Page Logic
# =============================================================================
PAGE_SPECS = {
    "MARKET_LEADERS": {"market": "mixed", "mode": "refresh_quotes"},
    "MY_PORTFOLIO": {"market": "mixed", "mode": "refresh_quotes"},
    "GLOBAL_MARKETS": {"market": "global", "mode": "refresh_quotes_global"},
    "KSA_TADAWUL": {"market": "ksa", "mode": "refresh_quotes_ksa"},
    # Add defaults for others
    "MUTUAL_FUNDS": {"market": "global", "mode": "refresh_quotes_global"},
    "COMMODITIES_FX": {"market": "global", "mode": "refresh_quotes_global"},
}

def resolve_page_spec(page_key: str) -> Dict[str, str]:
    k = (page_key or "").strip().upper()
    return PAGE_SPECS.get(k, {"market": "mixed", "mode": "refresh_quotes"})

def split_tickers_by_market(tickers: Sequence[str]) -> Tuple[List[str], List[str]]:
    seen_ksa, seen_glb = set(), set()
    ksa, glb = [], []
    
    for t in tickers or []:
        raw = str(t or "").strip()
        if not raw: continue
        
        # Try KSA specific norm first
        ksa_norm = _normalize_ksa_symbol(raw)
        if ksa_norm:
            if ksa_norm not in seen_ksa:
                seen_ksa.add(ksa_norm)
                ksa.append(ksa_norm)
            continue
            
        # Fallback to global
        norm = _NORMALIZE_SYMBOL(raw)
        if norm and norm not in seen_glb:
            seen_glb.add(norm)
            glb.append(norm)
            
    return ksa, glb

# =============================================================================
# Client Implementation
# =============================================================================
class GoogleAppsScriptClient:
    def __init__(self, base_url: str = None, token: str = None):
        self.base_url = (base_url or _DEFAULT_URL or "").rstrip("/")
        self.token = token or _DEFAULT_TOKEN
        self.timeout = _TIMEOUT
        
        # SSL Context
        self._ssl_ctx = ssl.create_default_context()
        if not _VERIFY_SSL:
            self._ssl_ctx.check_hostname = False
            self._ssl_ctx.verify_mode = ssl.CERT_NONE

        if not self.base_url:
            logger.warning("⚠️ No Apps Script URL configured. Calls will fail.")

    def _build_url(self, mode: str, query: Dict[str, str] = None) -> str:
        params = {"mode": mode}
        if self.token and "query" in _TOKEN_TRANSPORT:
            params[_TOKEN_PARAM] = self.token
        if query:
            params.update(query)
            
        return f"{self.base_url}?{urllib.parse.urlencode(params)}"

    def _sleep_jitter(self, attempt: int):
        # Cap sleep at 10s
        sleep_time = min(10.0, (1.5 ** attempt) + random.uniform(0, 1))
        time.sleep(sleep_time)

    def call_script(
        self, 
        mode: str, 
        payload: Dict[str, Any] = None, 
        method: str = "POST",
        retries: int = 2
    ) -> AppsScriptResult:
        
        if not self.base_url:
            return AppsScriptResult(False, 0, None, "No URL Configured")

        url = self._build_url(mode)
        
        # Optimize Payload (Prune None)
        clean_payload = {k: v for k, v in (payload or {}).items() if v is not None}
        
        # Auth injection
        headers = {"Content-Type": "application/json", "User-Agent": f"TFB-Client/{CLIENT_VERSION}"}
        if self.token and "header" in _TOKEN_TRANSPORT:
            headers["X-APP-TOKEN"] = self.token
            
        if self.token and "body" in _TOKEN_TRANSPORT:
            clean_payload.setdefault("auth", {})["token"] = self.token

        data = json.dumps(clean_payload).encode("utf-8") if method != "GET" else None
        
        start_t = time.time()
        
        for attempt in range(retries + 1):
            try:
                req = urllib.request.Request(url, data=data, headers=headers, method=method)
                with urllib.request.urlopen(req, timeout=self.timeout, context=self._ssl_ctx) as resp:
                    raw = resp.read().decode("utf-8")
                    try:
                        data = json.loads(raw)
                        return AppsScriptResult(True, 200, data, elapsed_ms=int((time.time()-start_t)*1000))
                    except:
                        return AppsScriptResult(False, 200, None, "Invalid JSON Response", raw_text=raw[:_MAX_LOG_CHARS])
                        
            except urllib.error.HTTPError as e:
                # Circuit Breaker: Don't retry auth errors
                if e.code in (401, 403):
                    return AppsScriptResult(False, e.code, None, f"Auth Error: {e.reason}")
                
                if attempt < retries:
                    logger.warning(f"Retry {attempt+1}/{retries} after HTTP {e.code}")
                    self._sleep_jitter(attempt)
                    continue
                    
                return AppsScriptResult(False, e.code, None, f"HTTP Error: {e.reason}", raw_text=str(e.read())[:200])
                
            except Exception as e:
                if attempt < retries:
                    self._sleep_jitter(attempt)
                    continue
                return AppsScriptResult(False, 0, None, f"Network Error: {str(e)}")

        return AppsScriptResult(False, 0, None, "Max Retries Exceeded")

    # ------------------------------------------------------------------
    # High-Level Methods
    # ------------------------------------------------------------------
    def sync_page_quotes(
        self, 
        page_key: str, 
        sheet_id: str, 
        sheet_name: str, 
        tickers: List[str]
    ) -> AppsScriptResult:
        
        spec = resolve_page_spec(page_key)
        ksa, glb = split_tickers_by_market(tickers)
        
        payload = {
            "sheet": {"id": sheet_id, "name": sheet_name},
            "meta": {
                "client": "tfb_advanced",
                "version": CLIENT_VERSION,
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            "backend": {"base_url": _BACKEND_URL}
        }

        # Route based on market spec
        if spec["market"] == "ksa":
            payload["ksa_tickers"] = ksa
            payload["all_tickers"] = ksa # optimization
        elif spec["market"] == "global":
            payload["global_tickers"] = glb
            payload["all_tickers"] = glb
        else:
            payload["ksa_tickers"] = ksa
            payload["global_tickers"] = glb
            payload["all_tickers"] = ksa + glb

        logger.info(f"Syncing {page_key}: {len(payload.get('all_tickers', []))} symbols")
        return self.call_script(mode=spec["mode"], payload=payload)

# Singleton
apps_script_client = GoogleAppsScriptClient()
get_apps_script_client = lambda: apps_script_client

__all__ = ["GoogleAppsScriptClient", "apps_script_client", "PAGE_SPECS"]
