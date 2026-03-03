#!/usr/bin/env python3
# routes/analysis_sheet_rows.py
"""
================================================================================
Analysis Sheet-Rows Router — v1.0.0 (PHASE 3 / SCHEMA-DRIVEN)
================================================================================
Endpoint:
  POST /v1/analysis/sheet-rows

Contract (Phase 3):
- Accept page aliases via page_catalog.normalize_page_name()
- Return FULL schema headers from schema_registry (not partial columns)
- For each row: emit dict with ALL schema keys (missing => null)
- Stable ordering:
    - keys/headers follow schema_registry order
    - rows follow request symbols order

Extra (Analysis rules):
- Supports computed/analysis fields when enabled (but headers always returned)
- Never fails on naming / alias mismatch (clean 400 with allowed pages)
- Optional rows_matrix for legacy clients

Also supports:
- page="Data_Dictionary" -> returns schema dictionary rows (no engine needed)
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

# core.config preferred: auth + flags (schema_headers_always, computations_enabled, etc.)
try:
    from core.config import auth_ok, get_settings_cached  # type: ignore
except Exception:
    auth_ok = None  # type: ignore

    def get_settings_cached(*args, **kwargs):  # type: ignore
        return None


ANALYSIS_SHEET_ROWS_VERSION = "1.0.0"
router = APIRouter(prefix="/v1/analysis", tags=["Analysis Sheet Rows"])


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
    # identity
    "symbol": ("symbol", "ticker"),
    "name": ("name", "company_name"),
    "asset_class": ("asset_class", "type", "instrument_type"),
    "exchange": ("exchange",),
    "currency": ("currency",),

    # price
    "current_price": ("current_price", "price", "last_price"),
    "previous_close": ("previous_close", "prev_close"),
    "open_price": ("open_price", "open"),
    "day_high": ("day_high", "high"),
    "day_low": ("day_low", "low"),
    "week_52_high": ("week_52_high", "52w_high", "high_52w"),
    "week_52_low": ("week_52_low", "52w_low", "low_52w"),
    "price_change": ("price_change", "change"),
    "percent_change": ("percent_change", "change_pct", "change_percent"),

    # liquidity
    "volume": ("volume",),
    "market_cap": ("market_cap",),

    # fundamentals
    "pe_ttm": ("pe_ttm", "pe_ratio", "pe"),
    "dividend_yield": ("dividend_yield",),

    # technical/risk
    "rsi_14": ("rsi_14", "rsi14", "rsi_14d", "rsi"),
    "volatility_30d": ("volatility_30d", "vol_30d"),
    "risk_score": ("risk_score",),

    # forecast/roi
    "forecast_price_1m": ("forecast_price_1m",),
    "forecast_price_3m": ("forecast_price_3m",),
    "forecast_price_12m": ("forecast_price_12m",),
    "expected_roi_1m": ("expected_roi_1m",),
    "expected_roi_3m": ("expected_roi_3m",),
    "expected_roi_12m": ("expected_roi_12m",),
    "forecast_confidence": ("forecast_confidence",),

    # scores
    "overall_score": ("overall_score",),
    "confidence_score": ("confidence_score",),
    "recommendation": ("recommendation",),
    "recommendation_reason": ("recommendation_reason",),

    # provenance
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

    # fallback to v2 or legacy
    for modpath in ("core.data_engine_v2", "core.data_engine"):
        try:
            mod = __import__(modpath, fromlist=["get_engine"])
            get_engine = getattr(mod, "get_engine", None)
            if callable(get_engine):
                eng = get_engine()
                if hasattr(eng, "__await__"):
                    eng = await eng
                return eng
        except Exception:
            continue

    return None


async def _fetch_analysis_rows(engine: Any, symbols: List[str], mode: str, settings: Any) -> Dict[str, Any]:
    """
    Analysis flavor fetch:
    - If computations enabled, prefer "analysis/enriched" style methods when present
    - Otherwise fall back to enriched quote fetch
    """
    if not symbols:
        return {}

    computations_enabled = bool(getattr(settings, "computations_enabled", True)) if settings is not None else True
    forecasting_enabled = bool(getattr(settings, "forecasting_enabled", True)) if settings is not None else True
    scoring_enabled = bool(getattr(settings, "scoring_enabled", True)) if settings is not None else True

    # When computations are disabled, we still return FULL schema keys,
    # but values may remain None if provider/engine doesn't supply them.
    # We still fetch basic enriched quotes.
    preferred_methods: List[str] = []
    if computations_enabled and (forecasting_enabled or scoring_enabled):
        preferred_methods += [
            "get_analysis_rows_batch",
            "get_analysis_quotes_batch",
            "get_enriched_quotes_batch",
            "get_enriched_quotes",
        ]
    else:
        preferred_methods += [
            "get_enriched_quotes_batch",
            "get_enriched_quotes",
            "get_quotes_batch",
        ]

    for method in preferred_methods:
        fn = getattr(engine, method, None)
        if callable(fn):
            try:
                res = await fn(symbols, mode=mode) if mode else await fn(symbols)
                if isinstance(res, dict):
                    return res
                if isinstance(res, list):
                    return {s: r for s, r in zip(symbols, res)}
            except Exception:
                continue

    # Per-symbol fallback
    out: Dict[str, Any] = {}
    per_methods = ["get_analysis_row", "get_analysis_quote", "get_enriched_quote", "quote", "get_quote"]
    for s in symbols:
        last_err = None
        for m in per_methods:
            fn = getattr(engine, m, None)
            if callable(fn):
                try:
                    r = await fn(s, mode=mode) if mode else await fn(s)
                    out[s] = r
                    last_err = None
                    break
                except Exception as e:
                    last_err = str(e)
        if last_err is not None and s not in out:
            out[s] = {"symbol": s, "error": last_err}
    return out


# -----------------------------------------------------------------------------
# Route
# -----------------------------------------------------------------------------
@router.post("/sheet-rows")
async def analysis_sheet_rows(
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
        # clean 400 (never raw crash)
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
            "version": ANALYSIS_SHEET_ROWS_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - start) * 1000,
                "count": len(rows),
                "schema_mode": "data_dictionary",
            },
        }

    # --- symbols can be empty (still return schema) ---
    if not symbols:
        return {
            "status": "skipped",
            "page": page,
            "headers": headers,
            "keys": keys,
            "rows": [],
            "rows_matrix": [] if include_matrix else None,
            "error": "No symbols provided",
            "version": ANALYSIS_SHEET_ROWS_VERSION,
            "request_id": request_id,
            "meta": {"duration_ms": (time.time() - start) * 1000},
        }

    # --- engine fetch ---
    engine = await _get_engine(request)
    if not engine:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Data engine unavailable")

    settings = None
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    data_map = await _fetch_analysis_rows(engine, symbols, mode=mode or "", settings=settings)

    normalized_rows: List[Dict[str, Any]] = []
    errors = 0

    for sym in symbols:
        raw = _to_plain_dict(data_map.get(sym))
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
        "version": ANALYSIS_SHEET_ROWS_VERSION,
        "request_id": request_id,
        "meta": {
            "duration_ms": (time.time() - start) * 1000,
            "requested": len(symbols),
            "errors": errors,
            "mode": mode,
            "computations_enabled": bool(getattr(settings, "computations_enabled", True)) if settings else True,
            "forecasting_enabled": bool(getattr(settings, "forecasting_enabled", True)) if settings else True,
            "scoring_enabled": bool(getattr(settings, "scoring_enabled", True)) if settings else True,
        },
    }


__all__ = ["router"]
