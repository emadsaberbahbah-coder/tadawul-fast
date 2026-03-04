#!/usr/bin/env python3
# routes/analysis_sheet_rows.py
"""
================================================================================
Analysis Sheet-Rows Router — v1.3.0 (PHASE 3/5 / SCHEMA-DRIVEN / FIX SPECIAL SHEETS)
================================================================================

Endpoint:
  POST /v1/analysis/sheet-rows

What this FIXES (your PowerShell evidence):
- ✅ Insights_Analysis expected 7 columns, was returning 80  -> FIXED
- ✅ Top_10_Investments expected 83 columns, was returning 80 -> FIXED
- ✅ Data_Dictionary expected 9 columns, was returning 80     -> FIXED

How:
- This router supports TWO safe modes (same as Advanced):
  (A) TABLE SHEETS (no symbols OR special pages) -> delegates to core.data_engine.get_sheet_rows(...)
      - Correct for Insights_Analysis / Top_10_Investments / Data_Dictionary
  (B) INSTRUMENT SHEETS (symbols provided) -> fetches per-symbol analysis/enriched rows then normalizes to schema

Contract (Phase 3):
- Accept aliases via page_catalog (best-effort)
- Reject forbidden pages
- Return FULL schema headers + keys from schema_registry (authoritative order)
- Stable ordering:
    - headers/keys follow schema_registry
    - rows follow request symbols order
- Optional rows_matrix for legacy clients (default true)
- Startup-safe: no network I/O at import-time

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
# Schema registry (safe import)
# -----------------------------------------------------------------------------
try:
    from core.sheets.schema_registry import get_sheet_spec  # type: ignore
except Exception as e:  # pragma: no cover
    get_sheet_spec = None  # type: ignore
    _SCHEMA_IMPORT_ERROR = repr(e)
else:
    _SCHEMA_IMPORT_ERROR = None

# -----------------------------------------------------------------------------
# Page catalog helpers (names differ across versions; best-effort)
# -----------------------------------------------------------------------------
try:
    from core.sheets.page_catalog import CANONICAL_PAGES  # type: ignore
except Exception:  # pragma: no cover
    CANONICAL_PAGES = []  # type: ignore

try:
    from core.sheets.page_catalog import FORBIDDEN_PAGES  # type: ignore
except Exception:  # pragma: no cover
    FORBIDDEN_PAGES = {"KSA_Tadawul", "Advisor_Criteria"}  # type: ignore

# -----------------------------------------------------------------------------
# Auth (optional)
# -----------------------------------------------------------------------------
try:
    from core.config import auth_ok, get_settings_cached  # type: ignore
except Exception:  # pragma: no cover
    auth_ok = None  # type: ignore

    def get_settings_cached(*args, **kwargs):  # type: ignore
        return None

# -----------------------------------------------------------------------------
# Shared sheet-rows builder (critical fix)
# -----------------------------------------------------------------------------
try:
    from core.data_engine import get_sheet_rows as core_get_sheet_rows  # type: ignore
except Exception:  # pragma: no cover
    core_get_sheet_rows = None  # type: ignore

ANALYSIS_SHEET_ROWS_VERSION = "1.3.0"
router = APIRouter(prefix="/v1/analysis", tags=["Analysis Sheet Rows"])


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def _strip(v: Any) -> str:
    try:
        return str(v).strip()
    except Exception:
        return ""


def _maybe_int(v: Any, default: int) -> int:
    try:
        if v is None or isinstance(v, bool):
            return default
        if isinstance(v, (int, float)):
            return int(v)
        s = _strip(v)
        if not s:
            return default
        return int(float(s))
    except Exception:
        return default


def _maybe_bool(v: Any, default: bool) -> bool:
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


def _to_plain_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    try:
        if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
            return obj.model_dump(mode="python")  # type: ignore
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            return obj.dict()  # type: ignore
        if is_dataclass(obj):
            try:
                return {k: getattr(obj, k) for k in obj.__dict__.keys() if not str(k).startswith("_")}
            except Exception:
                return {}
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


def _allow_query_token(settings: Any, request: Request) -> bool:
    try:
        if settings is not None:
            return bool(getattr(settings, "allow_query_token", False))
    except Exception:
        pass
    v = (os.getenv("ALLOW_QUERY_TOKEN", "") or "").strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True
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
    auth_token = _strip(x_app_token)
    if authorization and authorization.strip().lower().startswith("bearer "):
        auth_token = authorization.strip().split(" ", 1)[1].strip()
    if token_query and not auth_token and _allow_query_token(settings, request):
        auth_token = _strip(token_query)
    return auth_token


def _normalize_page_name(name: str) -> str:
    s = _strip(name)
    if not s:
        return s
    for fn_name in ("normalize_page_name", "resolve_page", "canonicalize_page"):
        try:
            mod = __import__("core.sheets.page_catalog", fromlist=[fn_name])
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                try:
                    out = fn(s, allow_output_pages=True)
                except TypeError:
                    out = fn(s)
                if isinstance(out, str) and out.strip():
                    return out.strip()
        except Exception:
            continue
    return s.replace(" ", "_")


def _ensure_page_allowed(page: str) -> None:
    forbidden = set(FORBIDDEN_PAGES or set())
    if page in forbidden:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": f"Forbidden/removed page: {page}", "forbidden_pages": sorted(list(forbidden))},
        )
    try:
        cp = list(CANONICAL_PAGES) if CANONICAL_PAGES else []
        if cp:
            if page != "Data_Dictionary" and page not in set(cp):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={"error": f"Unknown page: {page}", "allowed_pages": cp},
                )
    except HTTPException:
        raise
    except Exception:
        return


def _schema_headers_keys(page: str) -> Tuple[List[str], List[str], Any]:
    if get_sheet_spec is None:
        raise KeyError("schema_registry unavailable")
    spec = get_sheet_spec(page)
    cols = getattr(spec, "columns", None) or []
    headers = [str(getattr(c, "header", "")) for c in cols if getattr(c, "header", None)]
    keys = [str(getattr(c, "key", "")) for c in cols if getattr(c, "key", None)]
    return headers, keys, spec


def _rows_to_matrix(rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Any]]:
    return [[r.get(k) for k in keys] for r in rows]


# -----------------------------------------------------------------------------
# Normalization (fallback for per-symbol rows)
# -----------------------------------------------------------------------------
_CANONICAL_ALIASES: Dict[str, Tuple[str, ...]] = {
    "symbol": ("symbol", "ticker", "requested_symbol", "requestedsymbol"),
    "name": ("name", "company_name", "company", "short_name", "shortname", "longname"),
    "asset_class": ("asset_class", "type", "instrument_type", "assettype"),
    "exchange": ("exchange", "fullExchangeName", "full_exchange"),
    "currency": ("currency",),
    "current_price": ("current_price", "price", "last_price", "last", "regularMarketPrice"),
    "previous_close": ("previous_close", "prev_close", "regularMarketPreviousClose"),
    "open_price": ("open_price", "open", "regularMarketOpen"),
    "day_high": ("day_high", "high", "dayHigh"),
    "day_low": ("day_low", "low", "dayLow"),
    "week_52_high": ("week_52_high", "52w_high", "high_52w", "high52w", "fiftyTwoWeekHigh"),
    "week_52_low": ("week_52_low", "52w_low", "low_52w", "low52w", "fiftyTwoWeekLow"),
    "price_change": ("price_change", "change"),
    "percent_change": ("percent_change", "change_pct", "change_percent"),
    "volume": ("volume", "regularMarketVolume"),
    "market_cap": ("market_cap", "marketcap", "marketCap"),
    "pe_ttm": ("pe_ttm", "pe_ratio", "pe"),
    "dividend_yield": ("dividend_yield", "div_yield"),
    "rsi_14": ("rsi_14", "rsi14", "rsi_14d", "rsi"),
    "volatility_30d": ("volatility_30d", "vol_30d", "volatility"),
    "risk_score": ("risk_score",),
    "forecast_price_1m": ("forecast_price_1m",),
    "forecast_price_3m": ("forecast_price_3m",),
    "forecast_price_12m": ("forecast_price_12m",),
    "expected_roi_1m": ("expected_roi_1m", "expected_return_1m"),
    "expected_roi_3m": ("expected_roi_3m", "expected_return_3m"),
    "expected_roi_12m": ("expected_roi_12m", "expected_return_12m"),
    "forecast_confidence": ("forecast_confidence", "ai_confidence"),
    "overall_score": ("overall_score",),
    "confidence_score": ("confidence_score",),
    "recommendation": ("recommendation", "rec"),
    "recommendation_reason": ("recommendation_reason", "reason"),
    "last_updated_utc": ("last_updated_utc",),
    "last_updated_riyadh": ("last_updated_riyadh",),
    "warnings": ("warnings", "warning", "warning_message"),
    "data_provider": ("data_provider", "provider", "data_source"),
    "error": ("error", "error_message", "errormessage"),
}


def _normalize_row_to_schema_keys(schema_keys: Sequence[str], raw: Dict[str, Any], *, symbol_fallback: str) -> Dict[str, Any]:
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
            for a in _CANONICAL_ALIASES.get(k, ()):
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
# Engine accessor + analysis fetch
# -----------------------------------------------------------------------------
async def _get_engine(request: Request) -> Optional[Any]:
    try:
        st = getattr(request.app, "state", None)
        if st and getattr(st, "engine", None):
            return st.engine
    except Exception:
        pass

    for modpath in ("core.data_engine", "core.data_engine_v2"):
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


async def _fetch_analysis_rows(engine: Any, symbols: List[str], *, mode: str, settings: Any, schema: Any) -> Dict[str, Any]:
    if not symbols:
        return {}

    computations_enabled = bool(getattr(settings, "computations_enabled", True)) if settings is not None else True
    forecasting_enabled = bool(getattr(settings, "forecasting_enabled", True)) if settings is not None else True
    scoring_enabled = bool(getattr(settings, "scoring_enabled", True)) if settings is not None else True
    want_analysis = computations_enabled and (forecasting_enabled or scoring_enabled)

    preferred: List[str] = []
    if want_analysis:
        preferred += ["get_analysis_rows_batch", "get_analysis_quotes_batch", "get_enriched_quotes_batch"]
    else:
        preferred += ["get_enriched_quotes_batch"]

    preferred += ["get_enriched_quotes", "get_quotes_batch", "quotes_batch"]

    for method in preferred:
        fn = getattr(engine, method, None)
        if not callable(fn):
            continue
        try:
            # try with (symbols, mode, schema), then relax
            try:
                res = await _call_engine(fn, symbols, mode=mode, schema=schema)
            except TypeError:
                try:
                    res = await _call_engine(fn, symbols, schema=schema)
                except TypeError:
                    try:
                        res = await _call_engine(fn, symbols, mode=mode)
                    except TypeError:
                        res = await _call_engine(fn, symbols)

            if isinstance(res, dict):
                if _dict_is_symbol_map(res, symbols):
                    return res
                data = res.get("data") or res.get("rows") or res.get("items")
                if isinstance(data, dict) and _dict_is_symbol_map(data, symbols):
                    return data
                if isinstance(data, list):
                    return {s: r for s, r in zip(symbols, data)}
            elif isinstance(res, list):
                return {s: r for s, r in zip(symbols, res)}
        except Exception:
            continue

    out: Dict[str, Any] = {}
    per_dict_fn = getattr(engine, "get_enriched_quote_dict", None) or getattr(engine, "get_analysis_row_dict", None)
    per_fn = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_analysis_row", None) or getattr(engine, "get_quote", None)

    for s in symbols:
        try:
            if callable(per_dict_fn):
                try:
                    out[s] = await _call_engine(per_dict_fn, s, mode=mode, schema=schema)
                except TypeError:
                    try:
                        out[s] = await _call_engine(per_dict_fn, s, schema=schema)
                    except TypeError:
                        try:
                            out[s] = await _call_engine(per_dict_fn, s, mode=mode)
                        except TypeError:
                            out[s] = await _call_engine(per_dict_fn, s)
            elif callable(per_fn):
                try:
                    out[s] = await _call_engine(per_fn, s, mode=mode, schema=schema)
                except TypeError:
                    try:
                        out[s] = await _call_engine(per_fn, s, schema=schema)
                    except TypeError:
                        try:
                            out[s] = await _call_engine(per_fn, s, mode=mode)
                        except TypeError:
                            out[s] = await _call_engine(per_fn, s)
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

    # Schema registry availability
    if get_sheet_spec is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "Schema registry unavailable",
                "module": "routes.analysis_sheet_rows",
                "schema_import_error": _SCHEMA_IMPORT_ERROR,
            },
        )

    # Settings (optional)
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    # Auth
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

    # Page name
    page_raw = _strip(
        body.get("sheet")
        or body.get("page")
        or body.get("sheet_name")
        or body.get("sheetName")
        or body.get("name")
        or "Market_Leaders"
    )
    page = _normalize_page_name(page_raw)
    _ensure_page_allowed(page)

    include_matrix = _maybe_bool(body.get("include_matrix"), True)
    limit = max(1, min(5000, _maybe_int(body.get("limit"), 2000)))
    offset = max(0, _maybe_int(body.get("offset"), 0))

    # Schema headers/keys/spec
    try:
        headers, keys, spec = _schema_headers_keys("Data_Dictionary" if page == "Data_Dictionary" else page)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": f"Unknown page schema: {page}",
                "detail": str(e),
                "allowed_pages": list(CANONICAL_PAGES) if CANONICAL_PAGES else None,
            },
        )

    if not headers or not keys:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": f"Schema for page '{page}' is empty", "page": page},
        )

    # Symbols
    symbols = _get_list(body, "symbols", "tickers")
    top_n = max(1, min(5000, _maybe_int(body.get("top_n"), 2000)))
    symbols = symbols[:top_n]

    # -------------------------------------------------------------------------
    # MODE A: Table sheets or no symbols -> core.data_engine.get_sheet_rows
    # This is the critical fix for Insights_Analysis / Top_10_Investments / Data_Dictionary.
    # -------------------------------------------------------------------------
    if (not symbols) or page in {"Insights_Analysis", "Top_10_Investments", "Data_Dictionary"}:
        if core_get_sheet_rows is None:
            return {
                "status": "partial",
                "page": page,
                "headers": headers,
                "keys": keys,
                "rows": [],
                "rows_matrix": [] if include_matrix else None,
                "error": "core.data_engine.get_sheet_rows unavailable",
                "version": ANALYSIS_SHEET_ROWS_VERSION,
                "request_id": request_id,
                "meta": {
                    "duration_ms": (time.time() - start) * 1000.0,
                    "mode": mode,
                    "limit": limit,
                    "offset": offset,
                    "path": "schema_only_fallback",
                },
            }

        try:
            payload = await core_get_sheet_rows(
                page,
                limit=limit,
                offset=offset,
                mode=(mode or ""),
                body=body,
                include_matrix=include_matrix,
            )
        except Exception as e:
            return {
                "status": "error",
                "page": page,
                "headers": headers,
                "keys": keys,
                "rows": [],
                "rows_matrix": [] if include_matrix else None,
                "error": str(e),
                "version": ANALYSIS_SHEET_ROWS_VERSION,
                "request_id": request_id,
                "meta": {
                    "duration_ms": (time.time() - start) * 1000.0,
                    "mode": mode,
                    "limit": limit,
                    "offset": offset,
                    "path": "core_get_sheet_rows_failed",
                },
            }

        rows = payload.get("rows") or []
        if not isinstance(rows, list):
            rows = []
        rows = [r for r in rows if isinstance(r, dict)]

        out = {
            "status": payload.get("status") or "success",
            "page": page,
            "headers": headers,
            "keys": keys,
            "rows": rows,
            "rows_matrix": _rows_to_matrix(rows, keys) if include_matrix else None,
            "error": payload.get("error"),
            "version": ANALYSIS_SHEET_ROWS_VERSION,
            "request_id": request_id,
            "meta": payload.get("meta") or {},
        }
        out["meta"].setdefault("duration_ms", (time.time() - start) * 1000.0)
        out["meta"].setdefault("path", "core.data_engine.get_sheet_rows")
        out["meta"].setdefault("mode", mode)
        return out

    # -------------------------------------------------------------------------
    # MODE B: Symbol-based sheets -> fetch and normalize to schema
    # -------------------------------------------------------------------------
    engine = await _get_engine(request)
    if not engine:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Data engine unavailable")

    data_map = await _fetch_analysis_rows(engine, symbols, mode=mode or "", settings=settings, schema=spec)

    # Prefer engine schema normalizer if present
    normalize_fn = None
    try:
        from core.data_engine_v2 import normalize_row_to_schema as _n  # type: ignore

        normalize_fn = _n
    except Exception:
        normalize_fn = None

    normalized_rows: List[Dict[str, Any]] = []
    errors = 0

    for sym in symbols:
        raw_obj = data_map.get(sym)
        raw = _to_plain_dict(raw_obj)
        if isinstance(raw, dict) and raw.get("error"):
            errors += 1

        if callable(normalize_fn):
            try:
                row = normalize_fn(page, raw, keep_extras=True)
                for k in keys:
                    if k not in row:
                        row[k] = None
            except Exception:
                row = _normalize_row_to_schema_keys(keys, raw, symbol_fallback=sym)
        else:
            row = _normalize_row_to_schema_keys(keys, raw, symbol_fallback=sym)

        normalized_rows.append(row)

    status_out = "success" if errors == 0 else ("partial" if errors < len(symbols) else "error")

    return {
        "status": status_out,
        "page": page,
        "headers": headers,
        "keys": keys,
        "rows": normalized_rows,
        "rows_matrix": _rows_to_matrix(normalized_rows, keys) if include_matrix else None,
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
            "path": "symbol_rows_builder",
        },
    }


__all__ = ["router", "ANALYSIS_SHEET_ROWS_VERSION"]
