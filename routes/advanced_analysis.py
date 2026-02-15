# routes/advanced_analysis.py
"""
TADAWUL FAST BRIDGE – ADVANCED ANALYSIS ROUTES (v3.18.0)
PROD SAFE + ALIGNED ROI KEYS + RIYADH LOCALIZATION + SCORING INTEGRATION

v3.18.0 Enhancements:
- ✅ **Strict Key Alignment**: Primary keys 'expected_roi_1m/3m/12m' match Sheet Controller v5.8.0.
- ✅ **Riyadh Localization**: Injects 'last_updated_riyadh' and 'forecast_updated_riyadh' (UTC+3).
- ✅ **Scoring Integration**: Uses core.scoring_engine for consistent risk/opportunity metrics.
- ✅ **Resilient Mapping**: Safe-row fallback ensures one bad ticker doesn't break a 50-row batch.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import re
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, Body, Header, Query, Request

# pydantic v2 preferred, v1 fallback
try:
    from pydantic import BaseModel, ConfigDict, Field  # type: ignore
    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    from pydantic import BaseModel, Field  # type: ignore
    ConfigDict = None  # type: ignore
    _PYDANTIC_V2 = False

logger = logging.getLogger("routes.advanced_analysis")

ADVANCED_ANALYSIS_VERSION = "3.18.0"
router = APIRouter(prefix="/v1/advanced", tags=["Advanced Analysis"])


# =============================================================================
# Core Shims (Safe Imports)
# =============================================================================
try:
    from core.config import get_settings
except Exception:
    def get_settings(): return None

try:
    from core.schemas import DEFAULT_HEADERS_59 as _SCHEMAS_DEFAULT_59
    from core.schemas import get_headers_for_sheet as _get_headers_for_sheet
    _SCHEMAS_OK = True
except Exception:
    _SCHEMAS_DEFAULT_59 = None
    _get_headers_for_sheet = None
    _SCHEMAS_OK = False


# =============================================================================
# Normalization & Localization Utilities
# =============================================================================
def _fallback_normalize(raw: str) -> str:
    s = (raw or "").strip().upper()
    if not s: return ""
    if s.startswith("TADAWUL:"): s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"): s = s.replace(".TADAWUL", "")
    if any(ch in s for ch in ("^", "=")): return s
    if s.isdigit(): return f"{s}.SR"
    return s

@lru_cache(maxsize=1)
def _try_import_v2_normalizer() -> Any:
    try:
        from core.data_engine_v2 import normalize_symbol as _NS
        return _NS
    except Exception:
        return _fallback_normalize

def _normalize_any(raw: str) -> str:
    try:
        ns = _try_import_v2_normalizer()
        return (ns(raw) or "").strip().upper()
    except Exception:
        return _fallback_normalize(raw)

def _riyadh_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()

def _to_riyadh_iso(utc_iso: Optional[str]) -> str:
    if not utc_iso: return ""
    try:
        # Standardize Z to offset for fromisoformat compatibility
        dt = datetime.fromisoformat(utc_iso.replace("Z", "+00:00"))
        tz = timezone(timedelta(hours=3))
        return dt.astimezone(tz).isoformat()
    except: return ""


# =============================================================================
# Scoring & Row Mapping (Lazy Imports)
# =============================================================================
@lru_cache(maxsize=1)
def _try_import_enriched_quote() -> Optional[Any]:
    try:
        from core.enriched_quote import EnrichedQuote
        return EnrichedQuote
    except Exception: return None

@lru_cache(maxsize=1)
def _try_import_scoring_enricher() -> Optional[Any]:
    try:
        from core.scoring_engine import enrich_with_scores
        return enrich_with_scores
    except Exception: return None

def _enrich_scores_best_effort(uq: Any) -> Any:
    fn = _try_import_scoring_enricher()
    if callable(fn):
        try: return fn(uq)
        except Exception: return uq
    return uq

async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x): return await x
    return x


# =============================================================================
# Auth (X-APP-TOKEN)
# =============================================================================
@lru_cache(maxsize=1)
def _allowed_tokens() -> List[str]:
    tokens: List[str] = []
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v: tokens.append(v)
    return list(set(tokens))

def _auth_ok(x_app_token: Optional[str]) -> bool:
    allowed = _allowed_tokens()
    if not allowed: return True
    return bool(x_app_token and x_app_token.strip() in allowed)


# =============================================================================
# Engine Resolution
# =============================================================================
_ENGINE: Optional[Any] = None
_ENGINE_LOCK = asyncio.Lock()

async def _resolve_engine(request: Request) -> Optional[Any]:
    st = getattr(request.app, "state", None)
    if st:
        eng = getattr(st, "engine", None)
        if eng: return eng

    global _ENGINE
    if _ENGINE is not None: return _ENGINE
    async with _ENGINE_LOCK:
        if _ENGINE is not None: return _ENGINE
        try:
            from core.data_engine_v2 import get_engine
            _ENGINE = await _maybe_await(get_engine())
            return _ENGINE
        except Exception: return None


# =============================================================================
# Request/Response Models
# =============================================================================
class _ExtraIgnore(BaseModel):
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")
    else:
        class Config: extra = "ignore"

class PushSheetItem(_ExtraIgnore):
    sheet: str
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)

class PushSheetRowsRequest(_ExtraIgnore):
    items: List[PushSheetItem] = Field(default_factory=list)
    universe_rows: Optional[int] = None
    source: Optional[str] = None

class AdvancedSheetRequest(_ExtraIgnore):
    tickers: List[str] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)
    top_n: Optional[int] = Field(default=50, ge=1, le=500)
    sheet_name: Optional[str] = None

class AdvancedSheetResponse(_ExtraIgnore):
    status: str = "success"
    error: Optional[str] = None
    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)


# =============================================================================
# Helper: Placeholder
# =============================================================================
def _make_placeholder(symbol: str, err: str) -> Dict[str, Any]:
    return {
        "symbol": symbol.strip().upper(),
        "error": err,
        "data_quality": "MISSING",
        "last_updated_utc": datetime.now(timezone.utc).isoformat(),
        "last_updated_riyadh": _riyadh_iso()
    }


# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
async def advanced_health(request: Request):
    eng = await _resolve_engine(request)
    return {
        "status": "ok",
        "version": ADVANCED_ANALYSIS_VERSION,
        "engine": type(eng).__name__ if eng else "none",
        "riyadh_time": _riyadh_iso()
    }

@router.post("/sheet-rows")
async def advanced_sheet_rows(
    request: Request,
    body: Dict[str, Any] = Body(...),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Union[AdvancedSheetResponse, Dict[str, Any]]:
    
    if not _auth_ok(x_app_token):
        return {"status": "error", "error": "Unauthorized", "version": ADVANCED_ANALYSIS_VERSION}

    engine = await _resolve_engine(request)

    # --------------------------------------------------------
    # 1. PUSH MODE (Writing to engine cache from Sheets)
    # --------------------------------------------------------
    if "items" in body and isinstance(body["items"], list):
        try:
            push = PushSheetRowsRequest.model_validate(body) if _PYDANTIC_V2 else PushSheetRowsRequest.parse_obj(body)
            written = []
            for it in push.items:
                # Engine specific snapshot setter (DataEngine v2.15+)
                fn = getattr(engine, "set_cached_sheet_snapshot", None)
                if callable(fn):
                    await _maybe_await(fn(it.sheet, it.headers, it.rows))
                    written.append({"sheet": it.sheet, "rows": len(it.rows), "status": "cached"})
            return {"status": "success", "written": written, "version": ADVANCED_ANALYSIS_VERSION}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    # --------------------------------------------------------
    # 2. COMPUTE MODE (Fetching data + Scoring + Mapping)
    # --------------------------------------------------------
    try:
        req = AdvancedSheetRequest.model_validate(body) if _PYDANTIC_V2 else AdvancedSheetRequest.parse_obj(body)
        raw_tickers = (req.tickers or []) + (req.symbols or [])
        clean_tickers = []
        seen = set()
        for t in raw_tickers:
            if t and t not in seen:
                clean_tickers.append(t)
                seen.add(t)

        if not clean_tickers:
            return {"status": "error", "error": "No tickers provided"}

        # Execution Batch
        results = []
        if engine:
            limit = min(req.top_n or 50, 500)
            tasks = [engine.get_enriched_quote(t) for t in clean_tickers[:limit]]
            raw_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for i, r in enumerate(raw_results):
                symbol_in = clean_tickers[i]
                if isinstance(r, Exception):
                    results.append(_make_placeholder(symbol_in, str(r)))
                else:
                    # ✅ SCORING & LOCALIZATION INTEGRATION
                    r = _enrich_scores_best_effort(r)
                    if isinstance(r, dict):
                        # Ensure Riyadh timestamps are present for Sheet Mappers
                        r["last_updated_riyadh"] = _to_riyadh_iso(r.get("last_updated_utc"))
                        r["forecast_updated_riyadh"] = _riyadh_iso()
                        # Ensure recommendation enum consistency
                        if "recommendation" not in r: r["recommendation"] = "HOLD"
                    results.append(r)
        else:
            return {"status": "error", "error": "Engine unavailable"}

        # Map results to Rows using EnrichedQuote helper
        EnrichedQuote = _try_import_enriched_quote()
        headers = _get_headers_for_sheet(req.sheet_name) if _get_headers_for_sheet else []
        if not headers:
            headers = list(_SCHEMAS_DEFAULT_59) if _SCHEMAS_DEFAULT_59 else ["Symbol", "Price", "Error"]

        rows = []
        for res in results:
            try:
                if EnrichedQuote and isinstance(res, dict):
                    # EnrichedQuote handles logic for 59-column or custom schema alignment
                    eq = EnrichedQuote.from_unified(res)
                    mapped_row = eq.to_row(headers)
                    rows.append(mapped_row)
                else:
                    # Basic Fallback
                    fallback = [res.get("symbol", "UNKNOWN")] + [None] * (len(headers) - 1)
                    if "error" in res:
                        # Put error in the last column if "Error" is in headers
                        err_idx = next((idx for idx, h in enumerate(headers) if "Error" in h), -1)
                        if err_idx != -1: fallback[err_idx] = res["error"]
                    rows.append(fallback)
            except Exception as row_err:
                logger.warning(f"Failed to map row for {res.get('symbol')}: {row_err}")
                rows.append([res.get("symbol", "ERROR")] + [None] * (len(headers) - 1))

        return AdvancedSheetResponse(status="success", headers=headers, rows=rows)

    except Exception as e:
        logger.exception("Advanced Route Failure")
        return {"status": "error", "error": f"Computation failure: {str(e)}"}

__all__ = ["router"]
