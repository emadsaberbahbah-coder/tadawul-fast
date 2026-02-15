# routes/routes_argaam.py (FULL REPLACEMENT)
"""
routes/routes_argaam.py
===============================================================
Argaam Router (delegate) — v3.9.0
(PROD SAFE + SCORING AWARE + SCHEMA MAPPING + RIYADH TIME)

v3.9.0 Improvements:
- ✅ **Full Schema Mapping**: Uses EnrichedQuote to support dynamic Sheet headers (59 cols).
- ✅ **Scoring Integration**: Applies core.scoring_engine to generate metrics on the fly.
- ✅ **Riyadh Localization**: Injects 'last_updated_riyadh' (UTC+3).
- ✅ **Robust Batching**: Handles mixed success/failure in sheet-rows without crashing.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import re
import time
import traceback
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple, Union

import httpx
from cachetools import TTLCache
from fastapi import APIRouter, Header, Query, Request
from pydantic import BaseModel

logger = logging.getLogger("routes.routes_argaam")

ROUTE_VERSION = "3.9.0"
router = APIRouter(prefix="/v1/argaam", tags=["KSA / Argaam"])

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT_SEC = 25.0
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_SHEET_MAX = 500
DEFAULT_CONCURRENCY = 12

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}

# =============================================================================
# Lazy Imports (Safety First)
# =============================================================================
@lru_cache(maxsize=1)
def _get_ksa_normalizer():
    try:
        from core.symbols.normalize import normalize_ksa_symbol
        return normalize_ksa_symbol
    except ImportError:
        return None

@lru_cache(maxsize=1)
def _get_headers_helper():
    try:
        from core.schemas import get_headers_for_sheet, DEFAULT_HEADERS_59
        return get_headers_for_sheet, DEFAULT_HEADERS_59
    except ImportError:
        return None, None

@lru_cache(maxsize=1)
def _get_scoring_enricher():
    try:
        from core.scoring_engine import enrich_with_scores
        return enrich_with_scores
    except ImportError:
        return None

@lru_cache(maxsize=1)
def _get_enriched_quote_class():
    try:
        from core.enriched_quote import EnrichedQuote
        return EnrichedQuote
    except ImportError:
        return None

# =============================================================================
# Helpers
# =============================================================================
def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY

def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _riyadh_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()

def _utc_to_riyadh_iso(utc_iso: Optional[str]) -> Optional[str]:
    if not utc_iso: return _riyadh_iso()
    try:
        dt = datetime.fromisoformat(utc_iso.replace("Z", "+00:00"))
        tz = timezone(timedelta(hours=3))
        return dt.astimezone(tz).isoformat()
    except: return _riyadh_iso()

def _safe_str(x: Any) -> Optional[str]:
    if x is None: return None
    s = str(x).strip()
    return s if s else None

def _normalize_ksa_symbol(symbol: str) -> str:
    # Use central normalizer if available
    norm_fn = _get_ksa_normalizer()
    if norm_fn:
        return norm_fn(symbol)
    
    # Fallback local logic
    s = (symbol or "").strip().upper()
    if not s: return ""
    if s.startswith("TADAWUL:"): s = s.split(":", 1)[1]
    if s.endswith(".TADAWUL"): s = s.replace(".TADAWUL", "")
    if s.isdigit(): return f"{s}.SR"
    if s.endswith(".SR"): return s
    return ""

def _format_url(template: str, symbol: str) -> str:
    code = symbol.split(".")[0] if "." in symbol else symbol
    return (template or "").replace("{code}", code).replace("{symbol}", symbol)

async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x): return await x
    return x

# =============================================================================
# Enrichment Logic
# =============================================================================
def _enrich_item_scores(item: Dict[str, Any]) -> Dict[str, Any]:
    """Applies scoring engine to raw Argaam data."""
    enricher = _get_scoring_enricher()
    if enricher and item.get("current_price"):
        try:
            return enricher(item)
        except Exception:
            pass
    return item

# =============================================================================
# Auth
# =============================================================================
def _extract_token(x_token: Optional[str], auth_header: Optional[str], query_token: Optional[str]) -> Optional[str]:
    if x_token: return x_token
    if auth_header and auth_header.lower().startswith("bearer "):
        return auth_header.split(" ", 1)[1].strip()
    if query_token and _truthy(os.getenv("ALLOW_QUERY_TOKEN", "0")):
        return query_token
    return None

def _auth_ok(provided: Optional[str]) -> bool:
    required = (os.getenv("APP_TOKEN") or "").strip()
    backup = (os.getenv("BACKUP_APP_TOKEN") or "").strip()
    if not required and not backup: return True
    if not provided: return False
    return provided == required or provided == backup

# =============================================================================
# Argaam Specifics
# =============================================================================
ARGAAM_QUOTE_URL = _safe_str(os.getenv("ARGAAM_QUOTE_URL"))
# Optional fallbacks if env not set
if not ARGAAM_QUOTE_URL:
    # Placeholder to prevent crash, though it won't fetch data
    pass 

_quote_cache = TTLCache(maxsize=4000, ttl=20.0)

async def _fetch_url(url: str) -> Tuple[Optional[Dict], Optional[str]]:
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT_SEC, follow_redirects=True) as client:
        try:
            r = await client.get(url, headers={"User-Agent": USER_AGENT})
            if r.status_code == 200:
                return r.json(), None
            return None, f"HTTP {r.status_code}"
        except Exception as e:
            return None, str(e)

async def _get_quote_payload(request: Request, symbol: str, debug: int = 0) -> Dict[str, Any]:
    sym = _normalize_ksa_symbol(symbol)
    if not sym: return {"status": "error", "error": "Invalid KSA Symbol"}

    if not ARGAAM_QUOTE_URL:
        return {"status": "error", "error": "Argaam not configured", "symbol": sym}

    ck = f"q::{sym}"
    if ck in _quote_cache: return _quote_cache[ck]

    url = _format_url(ARGAAM_QUOTE_URL, sym)
    data, err = await _fetch_url(url)
    
    if not data:
        return {"status": "error", "error": err or "No data", "symbol": sym}

    # Normalize response to UnifiedQuote-like shape
    now_utc = _utc_iso()
    out = {
        "status": "ok",
        "symbol": sym,
        "symbol_normalized": sym,
        "market": "KSA",
        "currency": "SAR",
        # Map fields (adjust keys based on actual Argaam API response)
        "current_price": data.get("last") or data.get("price") or data.get("close"),
        "previous_close": data.get("previous_close") or data.get("prev_close"),
        "price_change": data.get("change"),
        "percent_change": data.get("change_pct") or data.get("percentage_change"),
        "volume": data.get("volume"),
        "name": data.get("companyName") or data.get("nameAr") or data.get("nameEn"),
        "sector": data.get("sectorName"),
        
        "data_source": "argaam",
        "data_quality": "OK" if data.get("last") else "PARTIAL",
        "last_updated_utc": now_utc,
        "last_updated_riyadh": _utc_to_riyadh_iso(now_utc)
    }
    
    # Enrich with Scores
    out = _enrich_item_scores(out)
    
    _quote_cache[ck] = out
    return out

# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
async def health(request: Request) -> Dict[str, Any]:
    return {
        "status": "ok",
        "module": "routes.routes_argaam",
        "version": ROUTE_VERSION,
        "configured": bool(ARGAAM_QUOTE_URL),
        "time_riyadh": _riyadh_iso()
    }

@router.get("/quote")
async def quote(
    request: Request,
    symbol: str = Query(..., description="KSA symbol"),
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
    debug: int = 0
):
    if not _auth_ok(_extract_token(x_app_token, authorization, token)):
        return {"status": "error", "error": "Unauthorized"}
    
    return await _get_quote_payload(request, symbol, debug)

# --------------------------------------------------------------------------
# Sheet Rows (Schema Aware)
# --------------------------------------------------------------------------
class SheetRowsIn(BaseModel):
    tickers: Optional[List[str]] = None
    symbols: Optional[List[str]] = None
    sheet_name: Optional[str] = None

@router.post("/sheet-rows")
async def sheet_rows(
    request: Request,
    payload: SheetRowsIn,
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None),
    token: Optional[str] = Query(None)
):
    if not _auth_ok(_extract_token(x_app_token, authorization, token)):
        return {"status": "error", "error": "Unauthorized"}

    syms = payload.tickers or payload.symbols or []
    normed = [_normalize_ksa_symbol(s) for s in syms if s]
    normed = [s for s in normed if s][:DEFAULT_SHEET_MAX]

    if not normed:
        return {"status": "skipped", "rows": [], "count": 0}

    # Fetch concurrently
    sem = asyncio.Semaphore(DEFAULT_CONCURRENCY)
    async def one(s):
        async with sem:
            return await _get_quote_payload(request, s)
            
    items = await asyncio.gather(*[one(s) for s in normed])
    
    # Determine Headers
    get_headers, default_headers = _get_headers_helper()
    headers = default_headers if default_headers else ["Symbol", "Price", "Change %", "Last Updated"]
    
    if get_headers and payload.sheet_name:
        h = get_headers(payload.sheet_name)
        if h: headers = h

    # Map Rows
    EnrichedQuote = _get_enriched_quote_class()
    rows = []
    
    for it in items:
        if EnrichedQuote and isinstance(it, dict) and "error" not in it:
            try:
                # Use powerful EnrichedQuote mapper to align with any schema (59 cols etc)
                eq = EnrichedQuote.from_unified(it)
                rows.append(eq.to_row(headers))
            except Exception:
                # Fallback if mapping fails
                rows.append([it.get("symbol"), "Map Error"] + [""] * (len(headers)-2))
        else:
            # Basic fallback for errors or missing EnrichedQuote
            row = [it.get("symbol", "UNKNOWN")] + [""] * (len(headers)-1)
            # Try to place error message if "Error" header exists
            try:
                err_idx = headers.index("Error")
                row[err_idx] = it.get("error", "")
            except ValueError:
                pass
            rows.append(row)

    return {
        "status": "ok",
        "headers": headers,
        "rows": rows,
        "count": len(rows),
        "source": "argaam_direct"
    }

__all__ = ["router"]
