# routes/enriched_quote.py
"""
routes/enriched_quote.py
------------------------------------------------------------
Enriched Quote Router — PROD SAFE (v5.8.0)
(SCORING INTEGRATED + NORMALIZATION AWARE + RIYADH TIME)

v5.8.0 Advanced Features:
- ✅ **Native Scoring**: Automatically applies `core.scoring_engine` to raw quotes.
- ✅ **Canonical Normalization**: Uses `core.symbols.normalize` for robust TADAWUL/Arabic support.
- ✅ **Riyadh Localization**: Injects `last_updated_riyadh` (UTC+3) into all responses.
- ✅ **Forecast Alignment**: Standardizes ROI/Target keys for the v12.2 Dashboard.
- ✅ **Resilient Batching**: Groups by market (KSA/Global) to optimize provider routing.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
import os
import re
import time
import traceback
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple, Union

from fastapi import APIRouter, Header, Query, Request
from fastapi.responses import JSONResponse

router = APIRouter(tags=["enriched"])

ENRICHED_ROUTE_VERSION = "5.8.0"

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}

logger = logging.getLogger("routes.enriched_quote")

# =============================================================================
# Lazy Imports (Safety First)
# =============================================================================
@lru_cache(maxsize=1)
def _get_scoring_enricher():
    try:
        from core.scoring_engine import enrich_with_scores
        return enrich_with_scores
    except ImportError:
        return None

@lru_cache(maxsize=1)
def _get_normalizer():
    try:
        from core.symbols.normalize import normalize_symbol, is_ksa
        return normalize_symbol, is_ksa
    except ImportError:
        return None, None

# =============================================================================
# Helpers
# =============================================================================
def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY

def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _riyadh_iso(utc_iso: Optional[str] = None) -> str:
    try:
        dt = datetime.fromisoformat(utc_iso.replace("Z", "+00:00")) if utc_iso else datetime.now(timezone.utc)
        ksa_tz = timezone(timedelta(hours=3))
        return dt.astimezone(ksa_tz).isoformat()
    except Exception:
        return ""

def _clamp(s: Any, n: int = 2000) -> str:
    t = (str(s) if s is not None else "").strip()
    if not t: return ""
    return t if len(t) <= n else (t[: n - 12] + " ...TRUNC...")

async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x

def _unwrap_tuple_payload(x: Any) -> Any:
    if isinstance(x, tuple) and len(x) == 2:
        return x[0]
    return x

def _debug_enabled(debug_q: int) -> bool:
    if int(debug_q or 0): return True
    v = (os.getenv("DEBUG_ERRORS") or "").strip().lower()
    return v in ("1", "true", "yes", "on")

def _norm_str(x: Any) -> str:
    try:
        return (str(x) if x is not None else "").strip()
    except Exception:
        return ""

# =============================================================================
# Normalization (Hybrid: Core > Local Fallback)
# =============================================================================
def _fallback_normalize(raw: str) -> str:
    s = (raw or "").strip().upper()
    if not s: return ""
    if s.startswith("TADAWUL:"): s = s.split(":", 1)[1].strip().upper()
    if s.endswith(".TADAWUL"): s = s.replace(".TADAWUL", "")
    if s.isdigit(): return f"{s}.SR"
    return s

def _normalize_symbol(sym: str) -> str:
    norm_fn, _ = _get_normalizer()
    if norm_fn:
        try:
            return norm_fn(sym)
        except Exception:
            pass
    return _fallback_normalize(sym)

def _is_ksa_symbol(sym: str) -> bool:
    _, is_ksa_fn = _get_normalizer()
    if is_ksa_fn:
        try:
            return is_ksa_fn(sym)
        except Exception:
            pass
    return sym.endswith(".SR") or sym.isdigit()

def _market_hint_for(sym: str) -> str:
    u = sym.upper()
    if "^" in u or "=" in u: return "INDEXFX"
    if _is_ksa_symbol(u): return "KSA"
    return "GLOBAL"

def _parse_symbols_list(raw: str) -> List[str]:
    s = (raw or "").strip()
    if not s: return []
    parts = re.split(r"[\s,]+", s)
    out = []
    seen = set()
    for p in parts:
        if not p.strip(): continue
        norm = _normalize_symbol(p)
        if norm and norm not in seen:
            seen.add(norm)
            out.append(norm)
    return out

def _resolve_symbols_from_query(
    symbols: Any, tickers: Any, ticker: Optional[str], symbol: Optional[str]
) -> List[str]:
    # Flatten varied inputs
    def flatten(v):
        if isinstance(v, (list, tuple)): return ",".join([str(x) for x in v if x])
        return str(v or "")
        
    multi = flatten(symbols) or flatten(tickers)
    if multi: return _parse_symbols_list(multi)
    
    single = str(ticker or symbol or "").strip()
    return [_normalize_symbol(single)] if single else []

# =============================================================================
# Auth
# =============================================================================
def _auth_ok(token: Optional[str]) -> bool:
    # 1. Env vars
    valid = {
        (os.getenv("APP_TOKEN") or "").strip(),
        (os.getenv("BACKUP_APP_TOKEN") or "").strip()
    }
    valid.discard("")
    # If no tokens configured => OPEN MODE
    if not valid: return True
    
    return (token or "").strip() in valid

def _extract_token(x_token: Optional[str], auth_header: Optional[str], query_token: Optional[str]) -> Optional[str]:
    if x_token: return x_token
    if auth_header and auth_header.lower().startswith("bearer "):
        return auth_header.split(" ", 1)[1].strip()
    # Query token allowed only if enabled explicitly
    if query_token and os.getenv("ALLOW_QUERY_TOKEN") == "1":
        return query_token
    return None

# =============================================================================
# Enrichment & Scoring Logic
# =============================================================================
def _enrich_item(item: Dict[str, Any], requested_symbol: str) -> Dict[str, Any]:
    """
    Applies scoring, timestamp localization, and provenance to a raw item.
    """
    if not item: return item
    
    # 1. Scoring Engine (Apply Value/Quality/Momentum/Risk scores)
    enricher = _get_scoring_enricher()
    if enricher:
        try:
            # enrich_with_scores modifies in-place or returns new dict
            item = enricher(item)
        except Exception as e:
            logger.warning(f"Scoring failed for {requested_symbol}: {e}")

    # 2. Recommendation Normalization
    reco = str(item.get("recommendation") or "HOLD").upper()
    if reco not in ("BUY", "HOLD", "REDUCE", "SELL"):
        item["recommendation"] = "HOLD"
    else:
        item["recommendation"] = reco

    # 3. Timestamps (UTC & Riyadh)
    utc = item.get("last_updated_utc") or _utc_iso()
    item["last_updated_utc"] = utc
    item["last_updated_riyadh"] = _riyadh_iso(utc)

    # 4. Metadata
    item.setdefault("symbol", requested_symbol)
    item.setdefault("data_quality", "MISSING" if not item.get("current_price") else "OK")
    
    # 5. Forecast Aliases (Sheets Compatibility)
    if "expected_roi_1m" in item:
        item["expected_return_1m"] = item["expected_roi_1m"]
    if "forecast_price_1m" in item:
        item["expected_price_1m"] = item["forecast_price_1m"]

    return item

def _shape_batch_result(syms: List[str], res: Any, eng_src: str) -> Dict[str, Dict[str, Any]]:
    """Aligns batch results to requested symbols."""
    out = {}
    
    # List Result (Order-based)
    if isinstance(res, list):
        for i, req_sym in enumerate(syms):
            raw = res[i] if i < len(res) else None
            # Extract dict if tuple (data, error)
            if isinstance(raw, tuple): raw = raw[0]
            
            item = dict(raw) if isinstance(raw, dict) else {}
            item.setdefault("engine_source", eng_src)
            out[req_sym] = _enrich_item(item, req_sym)
            
    # Dict Result (Key-based)
    elif isinstance(res, dict):
        # Normalize keys for lookup
        lookup = {k.upper(): v for k, v in res.items()}
        for req_sym in syms:
            # Try exact, normalized, or fallback
            raw = lookup.get(req_sym) or lookup.get(_normalize_symbol(req_sym))
            if isinstance(raw, tuple): raw = raw[0]
            
            item = dict(raw) if isinstance(raw, dict) else {}
            item.setdefault("engine_source", eng_src)
            out[req_sym] = _enrich_item(item, req_sym)
            
    # Fallback (Empty)
    else:
        for req_sym in syms:
            out[req_sym] = _enrich_item({"error": "Invalid batch format"}, req_sym)
            
    return out

# =============================================================================
# Engine Interaction
# =============================================================================
async def _get_engine(request: Request) -> Any:
    # 1. Try App State
    try:
        if request.app.state.engine:
            return request.app.state.engine
    except: pass
    
    # 2. Try Singleton
    try:
        from core.data_engine_v2 import get_engine
        return await _maybe_await(get_engine())
    except: return None

async def _call_engine_batch(eng: Any, symbols: List[str], refresh: bool) -> Tuple[Any, str]:
    # Try enriched batch
    if hasattr(eng, "get_enriched_quotes"):
        return await _maybe_await(eng.get_enriched_quotes(symbols, refresh=refresh)), "batch_enriched"
    # Try standard batch
    if hasattr(eng, "get_quotes"):
        return await _maybe_await(eng.get_quotes(symbols, refresh=refresh)), "batch_standard"
    return None, "none"

async def _call_engine_single(eng: Any, symbol: str, refresh: bool) -> Any:
    if hasattr(eng, "get_enriched_quote"):
        return await _maybe_await(eng.get_enriched_quote(symbol, refresh=refresh))
    if hasattr(eng, "get_quote"):
        return await _maybe_await(eng.get_quote(symbol, refresh=refresh))
    return None

# =============================================================================
# Routes
# =============================================================================
@router.get("/v1/enriched/health")
async def enriched_health(request: Request):
    eng = await _get_engine(request)
    return {
        "status": "ok",
        "module": "routes.enriched_quote",
        "version": ENRICHED_ROUTE_VERSION,
        "engine": type(eng).__name__ if eng else "none",
        "time": _utc_iso()
    }

@router.get("/v1/enriched/quote")
async def get_single_quote(
    request: Request,
    symbol: str = Query(..., description="Symbol (e.g., 1120.SR)"),
    refresh: int = 0,
    debug: int = 0,
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None)
):
    if not _auth_ok(_extract_token(x_app_token, authorization, None)):
        return JSONResponse({"status": "error", "error": "Unauthorized"}, status_code=401)

    eng = await _get_engine(request)
    if not eng:
        return {"status": "error", "error": "Engine Unavailable"}

    norm_sym = _normalize_symbol(symbol)
    if not norm_sym:
        return {"status": "error", "error": "Invalid Symbol"}

    try:
        raw = await _call_engine_single(eng, norm_sym, bool(refresh))
        if isinstance(raw, tuple): raw = raw[0]
        
        data = dict(raw) if isinstance(raw, dict) else {}
        data = _enrich_item(data, norm_sym)
        data["status"] = "ok"
        
        return data
    except Exception as e:
        err = {"status": "error", "error": str(e), "symbol": norm_sym}
        if debug: err["trace"] = traceback.format_exc()
        return err

@router.get("/v1/enriched/quotes")
async def get_batch_quotes(
    request: Request,
    symbols: Optional[str] = None,
    tickers: Optional[str] = None,
    refresh: int = 0,
    x_app_token: Optional[str] = Header(None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(None)
):
    if not _auth_ok(_extract_token(x_app_token, authorization, None)):
        return JSONResponse({"status": "error", "error": "Unauthorized"}, status_code=401)

    target_list = _resolve_symbols_from_query(symbols, tickers, None, None)
    if not target_list:
        return {"status": "error", "error": "No symbols provided", "items": []}

    eng = await _get_engine(request)
    if not eng:
        return {"status": "error", "error": "Engine Unavailable"}

    # Group by Market Hint for Optimization
    groups = {}
    for s in target_list:
        h = _market_hint_for(s)
        groups.setdefault(h, []).append(s)

    final_map = {}

    # Process Groups
    for hint, grp_syms in groups.items():
        # Hint can be passed if engine supports it, otherwise generic batch
        res, src = await _call_engine_batch(eng, grp_syms, bool(refresh))
        
        if res:
            shaped = _shape_batch_result(grp_syms, res, src)
            final_map.update(shaped)
        else:
            # Fallback to serial if batch fails/unsupported
            for s in grp_syms:
                one = await _call_engine_single(eng, s, bool(refresh))
                if isinstance(one, tuple): one = one[0]
                item = dict(one) if isinstance(one, dict) else {}
                final_map[s] = _enrich_item(item, s)

    # Reassemble in requested order
    items = []
    for t in target_list:
        item = final_map.get(t) or _enrich_item({}, t)
        items.append(item)

    return {
        "status": "ok",
        "count": len(items),
        "items": items,
        "version": ENRICHED_ROUTE_VERSION,
        "time_utc": _utc_iso()
    }
