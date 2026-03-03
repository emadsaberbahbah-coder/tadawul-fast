#!/usr/bin/env python3
# routes/advanced_sheet_rows.py
"""
================================================================================
Advanced Sheet-Rows Router — v1.0.0 (PHASE 3 / SCHEMA-DRIVEN)
================================================================================
Endpoint:
  POST /v1/advanced/sheet-rows

Contract:
- Accept page aliases via page_catalog.normalize_page_name()
- Return FULL schema headers from schema_registry (not partial columns)
- For each row: emit dict with ALL schema keys (missing => null)
- Stable ordering:
    - keys/headers follow schema_registry order
    - rows follow request symbols order

Also returns optional rows_matrix for legacy clients.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import is_dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

from core.sheets.schema_registry import get_sheet_spec
from core.sheets.page_catalog import normalize_page_name, CANONICAL_PAGES
from core.sheets.data_dictionary import build_data_dictionary_rows

# core.config is preferred for auth + flags, but router must be safe if unavailable
try:
    from core.config import auth_ok, get_settings_cached  # type: ignore
except Exception:
    auth_ok = None  # type: ignore

    def get_settings_cached(*args, **kwargs):  # type: ignore
        return None


ADVANCED_SHEET_ROWS_VERSION = "1.0.0"
router = APIRouter(prefix="/v1/advanced", tags=["Advanced Sheet Rows"])


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def _to_plain_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    try:
        if hasattr(obj, "model_dump"):
            return obj.model_dump(mode="python")
        if hasattr(obj, "dict"):
            return obj.dict()
        if is_dataclass(obj):
            return {k: getattr(obj, k) for k in obj.__dict__.keys() if not str(k).startswith("_")}
    except Exception:
        return {}
    return {}


_CANONICAL_ALIASES: Dict[str, Tuple[str, ...]] = {
    "symbol": ("symbol", "ticker"),
    "name": ("name", "company_name"),
    "asset_class": ("asset_class", "type", "instrument_type"),
    "exchange": ("exchange",),
    "currency": ("currency",),

    "current_price": ("current_price", "price", "last_price"),
    "previous_close": ("previous_close", "prev_close"),
    "open_price": ("open_price", "open"),
    "day_high": ("day_high", "high"),
    "day_low": ("day_low", "low"),
    "week_52_high": ("week_52_high", "52w_high", "high_52w"),
    "week_52_low": ("week_52_low", "52w_low", "low_52w"),
    "price_change": ("price_change", "change"),
    "percent_change": ("percent_change", "change_pct", "change_percent"),

    "volume": ("volume",),
    "market_cap": ("market_cap",),

    "rsi_14": ("rsi_14", "rsi14", "rsi_14d", "rsi"),
    "volatility_30d": ("volatility_30d", "vol_30d"),
    "risk_score": ("risk_score",),

    "forecast_price_1m": ("forecast_price_1m",),
    "forecast_price_3m": ("forecast_price_3m",),
    "forecast_price_12m": ("forecast_price_12m",),
    "expected_roi_1m": ("expected_roi_1m",),
    "expected_roi_3m": ("expected_roi_3m",),
    "expected_roi_12m": ("expected_roi_12m",),
    "forecast_confidence": ("forecast_confidence",),

    "overall_score": ("overall_score",),
    "confidence_score": ("confidence_score",),
    "recommendation": ("recommendation",),
    "recommendation_reason": ("recommendation_reason",),

    "last_updated_utc": ("last_updated_utc",),
    "last_updated_riyadh": ("last_updated_riyadh",),
    "warnings": ("warnings",),
    "data_provider": ("data_provider",),
}


def normalize_row_to_schema(schema_keys: Sequence[str], raw: Dict[str, Any], *, symbol_fallback: str) -> Dict[str, Any]:
    raw = raw or {}
    raw_lc = {str(k).lower(): v for k, v in raw.items()}
    out: Dict[str, Any] = {}

    for k in schema_keys:
        v = None
        if k in raw:
            v = raw.get(k)
        elif k.lower() in raw_lc:
            v = raw_lc.get(k.lower())
        else:
            aliases = _CANONICAL_ALIASES.get(k, ())
            for a in aliases:
                if a in raw:
                    v = raw.get(a)
                    break
                if a.lower() in raw_lc:
                    v = raw_lc.get(a.lower())
                    break
        out[k] = v

    if "symbol" in out and not out.get("symbol"):
        out["symbol"] = symbol_fallback

    return out


# -----------------------------------------------------------------------------
# Request model (minimal; no pydantic dependency required)
# -----------------------------------------------------------------------------
def _get_list(body: Dict[str, Any], *keys: str) -> List[str]:
    for k in keys:
        v = body.get(k)
        if isinstance(v, list):
            return [str(x).strip() for x in v if str(x).strip()]
    return []


def _get_bool(body: Dict[str, Any], key: str, default: bool) -> bool:
    v = body.get(key)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


# -----------------------------------------------------------------------------
# Engine accessor (lazy)
# -----------------------------------------------------------------------------
async def _get_engine(request: Request) -> Optional[Any]:
    # prefer app.state.engine
    try:
        st = getattr(request.app, "state", None)
        if st and getattr(st, "engine", None):
            return st.engine
    except Exception:
        pass

    # fallback to core.data_engine_v2.get_engine()
    try:
        from core.data_engine_v2 import get_engine  # type: ignore
        eng = get_engine()
        if hasattr(eng, "__await__"):
            eng = await eng
        return eng
    except Exception:
        return None


async def _fetch_quotes(engine: Any, symbols: List[str], mode: str = "") -> Dict[str, Any]:
    """
    Best-effort enriched quote fetcher.
    """
    if not symbols:
        return {}

    # batch methods first
    for method in ("get_enriched_quotes_batch", "get_enriched_quotes", "get_quotes_batch"):
        fn = getattr(engine, method, None)
        if callable(fn):
            try:
                res = await fn(symbols, mode=mode) if mode else await fn(symbols)
                if isinstance(res, dict):
                    return res
                if isinstance(res, list):
                    return {s: r for s, r in zip(symbols, res)}
            except Exception:
                pass

    # per-symbol fallback
    out: Dict[str, Any] = {}
    for s in symbols:
        try:
            fn = getattr(engine, "get_enriched_quote", None)
            if callable(fn):
                r = await fn(s, mode=mode) if mode else await fn(s)
                out[s] = r
            else:
                out[s] = {"symbol": s, "error": "engine_missing_get_enriched_quote"}
        except Exception as e:
            out[s] = {"symbol": s, "error": str(e)}
    return out


# -----------------------------------------------------------------------------
# Route
# -----------------------------------------------------------------------------
@router.post("/sheet-rows")
async def advanced_sheet_rows(
    request: Request,
    body: Dict[str, Any] = Body(...),
    mode: str = Query(default="", description="Optional mode hint for engine/provider"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    start = time.time()
    request_id = x_request_id or str(uuid.uuid4())

    # --- auth ---
    auth_token = (x_app_token or "").strip()
    if authorization and authorization.strip().lower().startswith("bearer "):
        auth_token = authorization.strip().split(" ", 1)[1].strip()
    if token and not auth_token:
        # query token allowed only if configured in core.config or env; best effort:
        allow_query = False
        try:
            settings = get_settings_cached()
            allow_query = bool(getattr(settings, "allow_query_token", False))
        except Exception:
            allow_query = False
        if allow_query:
            auth_token = token.strip()

    if auth_ok is not None:
        if not auth_ok(token=auth_token, authorization=authorization, headers={"X-APP-TOKEN": x_app_token, "Authorization": authorization}):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    # --- parse page + symbols ---
    page_raw = str(body.get("page") or body.get("sheet_name") or "Market_Leaders")
    try:
        page = normalize_page_name(page_raw, allow_output_pages=True)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": f"Invalid page: {str(e)}", "allowed_pages": list(CANONICAL_PAGES)},
        )

    symbols = _get_list(body, "symbols", "tickers")
    top_n = int(body.get("top_n") or 50)
    top_n = max(1, min(2000, top_n))
    symbols = symbols[:top_n]
    include_matrix = _get_bool(body, "include_matrix", True)

    # --- schema ---
    spec = get_sheet_spec("Data_Dictionary" if page == "Data_Dictionary" else page)
    headers = [c.header for c in spec.columns]
    keys = [c.key for c in spec.columns]

    # --- Data_Dictionary is computed locally ---
    if page == "Data_Dictionary":
        rows_dict = build_data_dictionary_rows(include_meta_sheet=True)
        rows = [normalize_row_to_schema(keys, r, symbol_fallback="") for r in rows_dict]
        return {
            "status": "success",
            "page": page,
            "headers": headers,
            "keys": keys,
            "rows": rows,
            "rows_matrix": [[row.get(k) for k in keys] for row in rows] if include_matrix else None,
            "version": ADVANCED_SHEET_ROWS_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - start) * 1000,
                "count": len(rows),
                "schema_mode": "data_dictionary",
            },
        }

    # --- engine fetch ---
    engine = await _get_engine(request)
    if not engine:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Data engine unavailable")

    quotes = await _fetch_quotes(engine, symbols, mode=mode or "")
    normalized_rows: List[Dict[str, Any]] = []
    errors = 0

    for sym in symbols:
        raw = _to_plain_dict(quotes.get(sym))
        if isinstance(raw, dict) and raw.get("error"):
            errors += 1
        normalized_rows.append(normalize_row_to_schema(keys, raw, symbol_fallback=sym))

    status_out = "success" if errors == 0 else ("partial" if errors < len(symbols) else "error")

    return {
        "status": status_out,
        "page": page,
        "headers": headers,
        "keys": keys,
        "rows": normalized_rows,
        "rows_matrix": [[row.get(k) for k in keys] for row in normalized_rows] if include_matrix else None,
        "error": f"{errors} errors" if errors else None,
        "version": ADVANCED_SHEET_ROWS_VERSION,
        "request_id": request_id,
        "meta": {
            "duration_ms": (time.time() - start) * 1000,
            "requested": len(symbols),
            "errors": errors,
            "mode": mode,
        },
    }


__all__ = ["router"]
