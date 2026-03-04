#!/usr/bin/env python3
# routes/analysis_sheet_rows.py
"""
================================================================================
Analysis Sheet-Rows Router — v1.2.0 (PHASE 3 / SCHEMA-DRIVEN / FULL COLUMNS)
================================================================================

Endpoint:
  POST /v1/analysis/sheet-rows

Contract (Phase 3):
- Accept page aliases via page_catalog.normalize_page_name()
- Reject forbidden/removed pages (e.g., KSA_Tadawul, Advisor_Criteria)
- Return FULL schema headers + keys from schema_registry (never partial)
- For each requested symbol: emit dict with ALL schema keys (missing => null)
- Stable ordering:
    - keys/headers follow schema_registry order
    - rows follow request symbols order
- Optional rows_matrix for legacy clients (default true)

Analysis rules:
- Computations/forecast/scoring are engine-driven (if enabled); route never breaks if disabled
- Returns schema even when symbols list is empty
- Clean 400 on invalid page name and includes allowed_pages
- page="Data_Dictionary" -> returns schema dictionary rows (no engine needed)

Startup-safe:
- No network I/O at import-time
- All heavy work is inside request handler/engine calls
================================================================================
"""

from __future__ import annotations

import inspect
import os
import time
import uuid
from dataclasses import is_dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

# -----------------------------------------------------------------------------
# Schema + pages (safe imports)
# -----------------------------------------------------------------------------
try:
    from core.sheets.schema_registry import get_sheet_spec  # type: ignore
except Exception as e:  # pragma: no cover
    get_sheet_spec = None  # type: ignore
    _SCHEMA_IMPORT_ERROR = repr(e)
else:
    _SCHEMA_IMPORT_ERROR = None

try:
    from core.sheets.page_catalog import normalize_page_name, CANONICAL_PAGES  # type: ignore
except Exception:  # pragma: no cover
    CANONICAL_PAGES = set()  # type: ignore

    def normalize_page_name(name: str, allow_output_pages: bool = True) -> str:  # type: ignore
        return (name or "").strip()

# Forbidden pages (optional, enforce safe defaults)
try:
    from core.sheets.page_catalog import FORBIDDEN_PAGES  # type: ignore
except Exception:
    FORBIDDEN_PAGES = {"KSA_Tadawul", "Advisor_Criteria"}  # type: ignore

# Data Dictionary builder
try:
    from core.sheets.data_dictionary import build_data_dictionary_rows  # type: ignore
except Exception:  # pragma: no cover
    build_data_dictionary_rows = None  # type: ignore

# core.config preferred: auth + flags (schema_headers_always, computations_enabled, etc.)
try:
    from core.config import auth_ok, get_settings_cached  # type: ignore
except Exception:  # pragma: no cover
    auth_ok = None  # type: ignore

    def get_settings_cached(*args, **kwargs):  # type: ignore
        return None


ANALYSIS_SHEET_ROWS_VERSION = "1.2.0"
router = APIRouter(prefix="/v1/analysis", tags=["Analysis Sheet Rows"])

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def _strip(v: Any) -> str:
    try:
        return str(v).strip()
    except Exception:
        return ""


def _to_plain_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    try:
        # pydantic v2
        if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
            return obj.model_dump(mode="python")  # type: ignore
        # pydantic v1
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            return obj.dict()  # type: ignore
        # dataclass
        if is_dataclass(obj):
            try:
                return {k: getattr(obj, k) for k in obj.__dict__.keys() if not str(k).startswith("_")}
            except Exception:
                return {}
        # plain object
        if hasattr(obj, "__dict__"):
            try:
                return dict(obj.__dict__)
            except Exception:
                return {}
    except Exception:
        return {}
    return {}


async def _maybe_await(x: Any) -> Any:
    try:
        if inspect.isawaitable(x):
            return await x
    except Exception:
        pass
    return x


def _get_list(body: Dict[str, Any], *keys: str) -> List[str]:
    for k in keys:
        v = body.get(k)
        if isinstance(v, list):
            out: List[str] = []
            for item in v:
                s = _strip(item)
                if s:
                    out.append(s)
            return out
    return []


def _get_int(body: Dict[str, Any], key: str, default: int) -> int:
    v = body.get(key)
    try:
        if isinstance(v, bool):
            return default
        if isinstance(v, (int, float)):
            return int(v)
        s = _strip(v)
        if s:
            return int(float(s))
    except Exception:
        pass
    return default


def _get_bool(body: Dict[str, Any], key: str, default: bool) -> bool:
    v = body.get(key)
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        try:
            return bool(int(v))
        except Exception:
            return default
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


def _allow_query_token(settings: Any, request: Request) -> bool:
    # Settings-first
    try:
        if settings is not None:
            return bool(getattr(settings, "allow_query_token", False))
    except Exception:
        pass

    # Env fallback
    v = (os.getenv("ALLOW_QUERY_TOKEN", "") or "").strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True

    # Optional header override (debug only)
    try:
        hv = _strip(request.headers.get("X-Allow-Query-Token"))
        if hv.lower() in {"1", "true", "yes"}:
            return True
    except Exception:
        pass

    return False


def _extract_auth_token(
    *,
    token_query: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
    settings: Any,
    request: Request,
) -> str:
    # Header token
    auth_token = _strip(x_app_token)

    # Bearer token
    if authorization and authorization.strip().lower().startswith("bearer "):
        auth_token = authorization.strip().split(" ", 1)[1].strip()

    # Query token only if allowed
    if token_query and not auth_token and _allow_query_token(settings, request):
        auth_token = _strip(token_query)

    return auth_token


# -----------------------------------------------------------------------------
# Row normalization: ensure ALL schema keys exist (missing => None)
# -----------------------------------------------------------------------------
_CANONICAL_ALIASES: Dict[str, Tuple[str, ...]] = {
    # identity
    "symbol": ("symbol", "ticker", "requested_symbol", "requestedsymbol"),
    "name": ("name", "company_name", "company", "short_name", "shortname", "longname"),
    "asset_class": ("asset_class", "type", "instrument_type", "assettype"),
    "exchange": ("exchange", "fullExchangeName", "full_exchange"),
    "currency": ("currency",),

    # price
    "current_price": ("current_price", "price", "last_price", "last", "regularMarketPrice"),
    "previous_close": ("previous_close", "prev_close", "regularMarketPreviousClose"),
    "open_price": ("open_price", "open", "regularMarketOpen"),
    "day_high": ("day_high", "high", "dayHigh"),
    "day_low": ("day_low", "low", "dayLow"),
    "week_52_high": ("week_52_high", "52w_high", "high_52w", "high52w", "fiftyTwoWeekHigh"),
    "week_52_low": ("week_52_low", "52w_low", "low_52w", "low52w", "fiftyTwoWeekLow"),
    "price_change": ("price_change", "change"),
    "percent_change": ("percent_change", "change_pct", "change_percent"),

    # liquidity
    "volume": ("volume", "regularMarketVolume"),
    "market_cap": ("market_cap", "marketcap", "marketCap"),

    # fundamentals
    "pe_ttm": ("pe_ttm", "pe_ratio", "pe"),
    "dividend_yield": ("dividend_yield", "div_yield"),

    # technical/risk
    "rsi_14": ("rsi_14", "rsi14", "rsi_14d", "rsi"),
    "volatility_30d": ("volatility_30d", "vol_30d", "volatility"),
    "risk_score": ("risk_score",),

    # forecast/roi
    "forecast_price_1m": ("forecast_price_1m",),
    "forecast_price_3m": ("forecast_price_3m",),
    "forecast_price_12m": ("forecast_price_12m",),
    "expected_roi_1m": ("expected_roi_1m", "expected_return_1m"),
    "expected_roi_3m": ("expected_roi_3m", "expected_return_3m"),
    "expected_roi_12m": ("expected_roi_12m", "expected_return_12m"),
    "forecast_confidence": ("forecast_confidence", "ai_confidence"),

    # scores
    "overall_score": ("overall_score",),
    "confidence_score": ("confidence_score",),
    "recommendation": ("recommendation", "rec"),
    "recommendation_reason": ("recommendation_reason", "reason"),

    # provenance / meta
    "last_updated_utc": ("last_updated_utc",),
    "last_updated_riyadh": ("last_updated_riyadh",),
    "warnings": ("warnings", "warning", "warning_message"),
    "data_provider": ("data_provider", "provider", "data_source"),
    "error": ("error", "error_message", "errormessage"),
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
                al = a.lower()
                if al in raw_lc:
                    v = raw_lc.get(al)
                    break

        out[k] = v

    if "symbol" in out and not out.get("symbol"):
        out["symbol"] = symbol_fallback

    return out


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
                eng = await _maybe_await(eng)
                return eng
        except Exception:
            continue

    return None


async def _call_engine(fn: Any, *args: Any, **kwargs: Any) -> Any:
    res = fn(*args, **kwargs)
    return await _maybe_await(res)


def _dict_is_symbol_map(d: Dict[str, Any], symbols: List[str]) -> bool:
    if not isinstance(d, dict) or not symbols:
        return False
    symset = set(symbols)
    keys = [k for k in d.keys() if isinstance(k, str)]
    if not keys:
        return False
    hit = sum(1 for k in keys if k in symset)
    if hit == len(symset) and len(symset) > 0:
        return True
    if hit >= max(1, min(3, len(symset) // 3)):
        return True
    return False


# -----------------------------------------------------------------------------
# Analysis fetch (best-effort)
# -----------------------------------------------------------------------------
async def _fetch_analysis_rows(
    engine: Any,
    symbols: List[str],
    *,
    mode: str,
    settings: Any,
    schema: Any = None,
) -> Dict[str, Any]:
    """
    Analysis flavor fetcher.

    Prefer dict-by-symbol batch method if available:
      - get_enriched_quotes_batch (DataEngineV5)  ✅ recommended
      - get_analysis_rows_batch / get_analysis_quotes_batch (if you add later)
      - get_enriched_quotes / get_quotes_batch (list results)
    Fallback:
      - per-symbol get_enriched_quote_dict / get_enriched_quote / get_quote
    """
    if not symbols:
        return {}

    computations_enabled = bool(getattr(settings, "computations_enabled", True)) if settings is not None else True
    forecasting_enabled = bool(getattr(settings, "forecasting_enabled", True)) if settings is not None else True
    scoring_enabled = bool(getattr(settings, "scoring_enabled", True)) if settings is not None else True

    # If computations disabled, still fetch enriched quotes (basic),
    # but schema output is always enforced by this router.
    want_analysis = computations_enabled and (forecasting_enabled or scoring_enabled)

    # Batch first
    preferred_methods: List[str] = []
    if want_analysis:
        preferred_methods += ["get_analysis_rows_batch", "get_analysis_quotes_batch", "get_enriched_quotes_batch"]
    else:
        preferred_methods += ["get_enriched_quotes_batch"]

    # Then other batch shapes
    preferred_methods += ["get_enriched_quotes", "get_quotes_batch", "quotes_batch"]

    for method in preferred_methods:
        fn = getattr(engine, method, None)
        if not callable(fn):
            continue
        try:
            # Try (symbols, mode=?, schema=?), then relax.
            try:
                res = await _call_engine(fn, symbols, mode=mode, schema=schema) if (mode or schema is not None) else await _call_engine(fn, symbols)
            except TypeError:
                try:
                    res = await _call_engine(fn, symbols, schema=schema) if schema is not None else await _call_engine(fn, symbols, mode=mode) if mode else await _call_engine(fn, symbols)
                except TypeError:
                    res = await _call_engine(fn, symbols)

            # Shapes
            if isinstance(res, dict):
                if _dict_is_symbol_map(res, symbols):
                    return res
                data = res.get("data") or res.get("rows") or res.get("items")
                if isinstance(data, dict) and _dict_is_symbol_map(data, symbols):
                    return data
                if isinstance(data, list):
                    return {s: r for s, r in zip(symbols, data)}
                # If it doesn't match, ignore and continue.
            elif isinstance(res, list):
                return {s: r for s, r in zip(symbols, res)}
        except Exception:
            continue

    # Per-symbol fallback (prefer dict-returning method if available)
    out: Dict[str, Any] = {}
    per_dict_fn = getattr(engine, "get_enriched_quote_dict", None) or getattr(engine, "get_analysis_row_dict", None)
    per_fn = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_analysis_row", None) or getattr(engine, "get_quote", None)

    for s in symbols:
        try:
            if callable(per_dict_fn):
                try:
                    r = await _call_engine(per_dict_fn, s, mode=mode, schema=schema) if (mode or schema is not None) else await _call_engine(per_dict_fn, s)
                except TypeError:
                    try:
                        r = await _call_engine(per_dict_fn, s, schema=schema) if schema is not None else await _call_engine(per_dict_fn, s, mode=mode) if mode else await _call_engine(per_dict_fn, s)
                    except TypeError:
                        r = await _call_engine(per_dict_fn, s)
                out[s] = r
            elif callable(per_fn):
                try:
                    r = await _call_engine(per_fn, s, mode=mode, schema=schema) if (mode or schema is not None) else await _call_engine(per_fn, s)
                except TypeError:
                    try:
                        r = await _call_engine(per_fn, s, schema=schema) if schema is not None else await _call_engine(per_fn, s, mode=mode) if mode else await _call_engine(per_fn, s)
                    except TypeError:
                        r = await _call_engine(per_fn, s)
                out[s] = r
            else:
                out[s] = {"symbol": s, "error": "engine_missing_quote_method"}
        except Exception as e:
            out[s] = {"symbol": s, "error": str(e)}

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

    # --- schema registry availability ---
    if get_sheet_spec is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "Schema registry unavailable",
                "module": "routes.analysis_sheet_rows",
                "schema_import_error": _SCHEMA_IMPORT_ERROR,
            },
        )

    # --- settings (optional) ---
    settings = None
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    # --- auth ---
    auth_token = _extract_auth_token(
        token_query=token,
        x_app_token=x_app_token,
        authorization=authorization,
        settings=settings,
        request=request,
    )

    if auth_ok is not None:
        if not auth_ok(
            token=auth_token,
            authorization=authorization,
            headers={"X-APP-TOKEN": x_app_token, "Authorization": authorization},
        ):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    # --- parse page + symbols ---
    page_raw = _strip(body.get("page") or body.get("sheet_name") or body.get("sheetName") or "Market_Leaders")
    try:
        page = normalize_page_name(page_raw, allow_output_pages=True)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": f"Invalid page: {str(e)}", "allowed_pages": list(CANONICAL_PAGES) if CANONICAL_PAGES else None},
        )

    forbidden = set(FORBIDDEN_PAGES or set())
    if page in forbidden:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": f"Forbidden/removed page: {page}", "forbidden_pages": sorted(list(forbidden))},
        )

    symbols = _get_list(body, "symbols", "tickers")
    top_n = max(1, min(2000, int(_get_int(body, "top_n", 50))))
    symbols = symbols[:top_n]
    include_matrix = _get_bool(body, "include_matrix", True)

    # --- schema ---
    try:
        spec = get_sheet_spec("Data_Dictionary" if page == "Data_Dictionary" else page)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": f"Unknown page schema: {page}",
                "detail": str(e),
                "allowed_pages": list(CANONICAL_PAGES) if CANONICAL_PAGES else None,
            },
        )

    headers = [c.header for c in getattr(spec, "columns", [])]
    keys = [c.key for c in getattr(spec, "columns", [])]

    if not headers or not keys:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": f"Schema for page '{page}' is empty", "page": page},
        )

    # --- Data_Dictionary is computed locally ---
    if page == "Data_Dictionary":
        if build_data_dictionary_rows is None:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail={"error": "Data_Dictionary builder unavailable"})

        rows_dict = build_data_dictionary_rows(include_meta_sheet=True)
        rows_norm = [normalize_row_to_schema(keys, r, symbol_fallback="") for r in (rows_dict or [])]

        return {
            "status": "success",
            "page": page,
            "headers": headers,
            "keys": keys,
            "rows": rows_norm,
            "rows_matrix": [[row.get(k) for k in keys] for row in rows_norm] if include_matrix else None,
            "version": ANALYSIS_SHEET_ROWS_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - start) * 1000.0,
                "count": len(rows_norm),
                "schema_mode": "data_dictionary",
            },
        }

    # --- symbols can be empty (still return schema) ---
    if not symbols:
        return {
            "status": "success",
            "page": page,
            "headers": headers,
            "keys": keys,
            "rows": [],
            "rows_matrix": [] if include_matrix else None,
            "version": ANALYSIS_SHEET_ROWS_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - start) * 1000.0,
                "requested": 0,
                "errors": 0,
                "mode": mode,
            },
        }

    # --- engine fetch ---
    engine = await _get_engine(request)
    if not engine:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Data engine unavailable")

    # Some engines can use schema to pre-normalize; we pass the spec (best-effort)
    data_map = await _fetch_analysis_rows(engine, symbols, mode=mode or "", settings=settings, schema=spec)

    normalized_rows: List[Dict[str, Any]] = []
    errors = 0

    for sym in symbols:
        raw_obj = data_map.get(sym)
        raw = _to_plain_dict(raw_obj)
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
            "duration_ms": (time.time() - start) * 1000.0,
            "requested": len(symbols),
            "errors": errors,
            "mode": mode,
            "schema_headers_always": bool(getattr(settings, "schema_headers_always", True)) if settings else True,
            "computations_enabled": bool(getattr(settings, "computations_enabled", True)) if settings else True,
            "forecasting_enabled": bool(getattr(settings, "forecasting_enabled", True)) if settings else True,
            "scoring_enabled": bool(getattr(settings, "scoring_enabled", True)) if settings else True,
        },
    }


__all__ = ["router", "ANALYSIS_SHEET_ROWS_VERSION"]
