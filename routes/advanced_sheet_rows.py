#!/usr/bin/env python3
# routes/advanced_sheet_rows.py
"""
================================================================================
Advanced Sheet-Rows Router — v1.1.0 (PHASE 3 / SCHEMA-DRIVEN / FULL COLUMNS)
================================================================================

Endpoint:
  POST /v1/advanced/sheet-rows

Contract (Phase 3):
- Accept page aliases via page_catalog.normalize_page_name()
- Reject forbidden/removed pages (e.g., KSA_Tadawul, Advisor_Criteria)
- Return FULL schema headers + keys from schema_registry for the requested page
- For each requested symbol: return a dict with ALL schema keys (missing => null)
- Stable ordering:
    - headers/keys follow schema_registry order
    - rows follow request symbols order
- Optional rows_matrix for legacy clients (default true)

Special:
- Data_Dictionary is computed locally from schema_registry (no engine call)
"""

from __future__ import annotations

import inspect
import time
import uuid
from dataclasses import is_dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

from core.sheets.schema_registry import get_sheet_spec
from core.sheets.page_catalog import CANONICAL_PAGES, normalize_page_name

# Optional (if present in your page_catalog)
try:
    from core.sheets.page_catalog import FORBIDDEN_PAGES  # type: ignore
except Exception:
    FORBIDDEN_PAGES = {"KSA_Tadawul", "Advisor_Criteria"}  # type: ignore

from core.sheets.data_dictionary import build_data_dictionary_rows

# core.config is preferred for auth + flags, but router must be safe if unavailable
try:
    from core.config import auth_ok, get_settings_cached  # type: ignore
except Exception:
    auth_ok = None  # type: ignore

    def get_settings_cached(*args, **kwargs):  # type: ignore
        return None


ADVANCED_SHEET_ROWS_VERSION = "1.1.0"
router = APIRouter(prefix="/v1/advanced", tags=["Advanced Sheet Rows"])


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
        if hasattr(obj, "model_dump"):
            return obj.model_dump(mode="python")
        if hasattr(obj, "dict"):
            return obj.dict()
        if is_dataclass(obj):
            # dataclass: best-effort
            try:
                return {k: getattr(obj, k) for k in obj.__dict__.keys() if not str(k).startswith("_")}
            except Exception:
                pass
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
        return bool(int(v))
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


# -----------------------------------------------------------------------------
# Row normalization: ensure ALL schema keys exist (missing => None)
# -----------------------------------------------------------------------------
_CANONICAL_ALIASES: Dict[str, Tuple[str, ...]] = {
    # identity
    "symbol": ("symbol", "ticker", "requested_symbol", "requestedsymbol"),
    "name": ("name", "company_name", "company", "short_name"),
    "asset_class": ("asset_class", "type", "instrument_type"),
    "exchange": ("exchange",),
    "currency": ("currency",),
    # prices
    "current_price": ("current_price", "price", "last_price", "last"),
    "previous_close": ("previous_close", "prev_close"),
    "open_price": ("open_price", "open"),
    "day_high": ("day_high", "high"),
    "day_low": ("day_low", "low"),
    "week_52_high": ("week_52_high", "52w_high", "high_52w", "high52w"),
    "week_52_low": ("week_52_low", "52w_low", "low_52w", "low52w"),
    "price_change": ("price_change", "change"),
    "percent_change": ("percent_change", "change_pct", "change_percent"),
    # volume
    "volume": ("volume",),
    "market_cap": ("market_cap", "marketcap"),
    # technicals / risk
    "rsi_14": ("rsi_14", "rsi14", "rsi_14d", "rsi"),
    "volatility_30d": ("volatility_30d", "vol_30d", "volatility"),
    "risk_score": ("risk_score",),
    # forecasts / ROI
    "forecast_price_1m": ("forecast_price_1m",),
    "forecast_price_3m": ("forecast_price_3m",),
    "forecast_price_12m": ("forecast_price_12m",),
    "expected_roi_1m": ("expected_roi_1m", "expected_return_1m"),
    "expected_roi_3m": ("expected_roi_3m", "expected_return_3m"),
    "expected_roi_12m": ("expected_roi_12m", "expected_return_12m"),
    "forecast_confidence": ("forecast_confidence", "ai_confidence"),
    # scoring
    "overall_score": ("overall_score",),
    "confidence_score": ("confidence_score",),
    "recommendation": ("recommendation", "rec"),
    "recommendation_reason": ("recommendation_reason", "reason"),
    # meta
    "last_updated_utc": ("last_updated_utc",),
    "last_updated_riyadh": ("last_updated_riyadh",),
    "warnings": ("warnings",),
    "data_provider": ("data_provider", "provider"),
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
                if a.lower() in raw_lc:
                    v = raw_lc.get(a.lower())
                    break
        out[k] = v

    # Guarantee symbol
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

    # fallback to core.data_engine_v2.get_engine()
    try:
        from core.data_engine_v2 import get_engine  # type: ignore

        eng = get_engine()
        eng = await _maybe_await(eng)
        return eng
    except Exception:
        return None


async def _call_engine(fn: Any, *args: Any, **kwargs: Any) -> Any:
    try:
        res = fn(*args, **kwargs)
        return await _maybe_await(res)
    except Exception:
        raise


async def _fetch_quotes(engine: Any, symbols: List[str], mode: str = "") -> Dict[str, Any]:
    """
    Best-effort enriched quote fetcher.
    Returns: {symbol: row_obj_or_dict}
    """
    if not symbols:
        return {}

    # batch methods first (async or sync)
    candidates = (
        "get_enriched_quotes_batch",
        "get_enriched_quotes",
        "get_quotes_batch",
        "quotes_batch",
    )
    for method in candidates:
        fn = getattr(engine, method, None)
        if callable(fn):
            try:
                if mode:
                    res = await _call_engine(fn, symbols, mode=mode)
                else:
                    res = await _call_engine(fn, symbols)

                # common shapes
                if isinstance(res, dict):
                    # could be {symbol: row} OR {"rows": ..., "data": ...}
                    if all(isinstance(k, str) for k in res.keys()):
                        return res
                    data = res.get("data") or res.get("rows") or res.get("items")
                    if isinstance(data, dict):
                        return data
                    if isinstance(data, list):
                        return {s: r for s, r in zip(symbols, data)}
                if isinstance(res, list):
                    return {s: r for s, r in zip(symbols, res)}
            except Exception:
                pass

    # per-symbol fallback
    out: Dict[str, Any] = {}
    per_fn = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_quote", None)
    for s in symbols:
        if callable(per_fn):
            try:
                if mode:
                    r = await _call_engine(per_fn, s, mode=mode)
                else:
                    r = await _call_engine(per_fn, s)
                out[s] = r
            except Exception as e:
                out[s] = {"symbol": s, "error": str(e)}
        else:
            out[s] = {"symbol": s, "error": "engine_missing_get_enriched_quote"}
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
    auth_token = _strip(x_app_token)
    if authorization and authorization.strip().lower().startswith("bearer "):
        auth_token = authorization.strip().split(" ", 1)[1].strip()

    # query token only if explicitly allowed (settings or env)
    if token and not auth_token:
        allow_query = False
        try:
            settings = get_settings_cached()
            allow_query = bool(getattr(settings, "allow_query_token", False))
        except Exception:
            allow_query = False
        if not allow_query:
            # env fallback
            allow_query = (_strip(request.headers.get("X-Allow-Query-Token")) or "").lower() in {"1", "true", "yes"} or (
                (_strip(__import__("os").getenv("ALLOW_QUERY_TOKEN", "")) or "").lower() in {"1", "true", "yes"}
            )
        if allow_query:
            auth_token = _strip(token)

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
            detail={"error": f"Invalid page: {str(e)}", "allowed_pages": list(CANONICAL_PAGES)},
        )

    if page in set(FORBIDDEN_PAGES or set()):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": f"Forbidden/removed page: {page}", "forbidden_pages": list(FORBIDDEN_PAGES)},
        )

    symbols = _get_list(body, "symbols", "tickers")
    top_n = _get_int(body, "top_n", 50)
    top_n = max(1, min(2000, int(top_n)))
    symbols = symbols[:top_n]
    include_matrix = _get_bool(body, "include_matrix", True)

    # --- schema ---
    spec = get_sheet_spec("Data_Dictionary" if page == "Data_Dictionary" else page)
    headers = [c.header for c in spec.columns]
    keys = [c.key for c in spec.columns]

    # Always return FULL schema headers/keys even if no symbols
    if page != "Data_Dictionary" and not symbols:
        return {
            "status": "success",
            "page": page,
            "headers": headers,
            "keys": keys,
            "rows": [],
            "rows_matrix": [] if include_matrix else None,
            "version": ADVANCED_SHEET_ROWS_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - start) * 1000,
                "requested": 0,
                "errors": 0,
                "mode": mode,
            },
        }

    # --- Data_Dictionary is computed locally ---
    if page == "Data_Dictionary":
        rows_dict = build_data_dictionary_rows(include_meta_sheet=True)
        rows_norm = [normalize_row_to_schema(keys, r, symbol_fallback="") for r in (rows_dict or [])]
        return {
            "status": "success",
            "page": page,
            "headers": headers,
            "keys": keys,
            "rows": rows_norm,
            "rows_matrix": [[row.get(k) for k in keys] for row in rows_norm] if include_matrix else None,
            "version": ADVANCED_SHEET_ROWS_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - start) * 1000,
                "count": len(rows_norm),
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

    # Ensure rows follow requested symbols order
    for sym in symbols:
        raw_obj = quotes.get(sym)
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
