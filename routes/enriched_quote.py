#!/usr/bin/env python3
# routes/enriched_quote.py
"""
================================================================================
TFB Enriched Quote Routes Wrapper — v6.0.0
================================================================================
RENDER-SAFE • SCHEMA-AWARE • PAGE-DISPATCH SAFE • HEADER/ROW BALANCED

Why this revision
-----------------
- ✅ FIX: Stops treating most pages as schema-only unless that is explicitly intended
- ✅ FIX: Uses page_catalog route-family classification so special pages dispatch correctly
- ✅ FIX: Returns populated rows for instrument pages when symbols exist
- ✅ FIX: Delegates special pages to local builders instead of generic quote fallback
- ✅ FIX: Keeps /quotes, /quote, /v1/enriched/quote, /v1/enriched/sheet-rows compatible
- ✅ FIX: Keeps Data_Dictionary local and stable
- ✅ FIX: Prevents Top_10_Investments / Insights_Analysis from silently hitting wrong fallback
- ✅ SAFE: No network I/O at import-time
- ✅ SAFE: No Prometheus metric creation here

Behavior
--------
- POST /quotes
- POST /quote
- POST /v1/enriched/quote
- POST /v1/enriched/sheet-rows

Dispatch model
--------------
- instrument pages      -> engine-backed quote rows
- insights page         -> local insights builder if available
- top10 page            -> local top10 builder if available
- data_dictionary page  -> local data dictionary builder
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import time
import uuid
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

logger = logging.getLogger("routes.enriched_quote")

ROUTER_VERSION = "6.0.0"

# Kept lazy for dynamic mount behavior
router: Optional[APIRouter] = None


# =============================================================================
# Pure helpers
# =============================================================================
def _strip(v: Any) -> str:
    try:
        return str(v).strip()
    except Exception:
        return ""


def _as_list(v: Any) -> List[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, tuple):
        return list(v)
    if isinstance(v, set):
        return list(v)
    if isinstance(v, str):
        return [v]
    if isinstance(v, Iterable):
        try:
            return list(v)
        except Exception:
            return [v]
    return [v]


def _get_list(body: Mapping[str, Any], *keys: str) -> List[str]:
    for k in keys:
        v = body.get(k)
        if isinstance(v, list):
            out: List[str] = []
            for item in v:
                s = _strip(item)
                if s:
                    out.append(s)
            return out
        if isinstance(v, str) and v.strip():
            # tolerate comma-separated strings
            parts = [p.strip() for p in v.split(",") if p.strip()]
            if parts:
                return parts
    return []


def _get_bool(body: Mapping[str, Any], key: str, default: bool) -> bool:
    v = body.get(key)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


def _get_int(body: Mapping[str, Any], key: str, default: int) -> int:
    v = body.get(key)
    try:
        if isinstance(v, bool):
            return default
        if isinstance(v, int):
            return v
        if isinstance(v, float) and int(v) == v:
            return int(v)
        if isinstance(v, str):
            s = v.strip()
            if s and s.lstrip("+-").isdigit():
                return int(s)
    except Exception:
        pass
    return int(default)


def _extract_token(x_app_token: Optional[str], authorization: Optional[str], token_query: Optional[str]) -> str:
    tok = _strip(x_app_token)
    auth = _strip(authorization)

    if auth.lower().startswith("bearer "):
        return _strip(auth.split(" ", 1)[1])
    if tok:
        return tok
    return _strip(token_query)


def _rows_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    out: List[List[Any]] = []
    for row in rows:
        out.append([row.get(k) for k in keys])
    return out


def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj

    try:
        if hasattr(obj, "model_dump"):
            d = obj.model_dump()  # type: ignore[attr-defined]
            return d if isinstance(d, dict) else {}
    except Exception:
        pass

    try:
        if hasattr(obj, "dict"):
            d = obj.dict()  # type: ignore[attr-defined]
            return d if isinstance(d, dict) else {}
    except Exception:
        pass

    try:
        d = getattr(obj, "__dict__", None)
        if isinstance(d, dict):
            return dict(d)
    except Exception:
        pass

    return {}


def _normalize_item_to_row(item: Any, keys: Sequence[str]) -> Dict[str, Any]:
    if isinstance(item, Mapping):
        return {k: item.get(k) for k in keys}

    if hasattr(item, "__dict__"):
        d = _model_to_dict(item)
        return {k: d.get(k) for k in keys}

    if isinstance(item, (list, tuple)):
        vals = list(item)
        return {k: (vals[i] if i < len(vals) else None) for i, k in enumerate(keys)}

    out = {k: None for k in keys}
    if keys:
        out[keys[0]] = item
    return out


# =============================================================================
# Shared lazy service layer
# =============================================================================
class _Service:
    def __init__(self, reason: str):
        self.reason = reason

        # ---------------- Auth helpers (optional) ----------------
        try:
            from core.config import auth_ok as _auth_ok  # type: ignore
            from core.config import get_settings_cached as _get_settings_cached  # type: ignore
        except Exception:
            _auth_ok = None  # type: ignore

            def _get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
                return None

        self._auth_ok = _auth_ok
        self._get_settings_cached = _get_settings_cached

        # ---------------- Schema / page catalog helpers ----------------
        try:
            from core.sheets.page_catalog import (  # type: ignore
                CANONICAL_PAGES,
                get_route_family,
                is_instrument_page,
                normalize_page_name,
            )
            from core.sheets.schema_registry import get_sheet_spec  # type: ignore

            self._HAS_SCHEMA = True
            self.CANONICAL_PAGES = list(CANONICAL_PAGES)
            self.get_route_family = get_route_family
            self.is_instrument_page = is_instrument_page
            self.normalize_page_name = normalize_page_name
            self.get_sheet_spec = get_sheet_spec
        except Exception as e:
            self._HAS_SCHEMA = False
            self.CANONICAL_PAGES = []
            self._schema_import_error = e

            def get_route_family(_: str) -> str:
                return "instrument"

            def is_instrument_page(_: str) -> bool:
                return True

            def normalize_page_name(page: str, *, allow_output_pages: bool = True) -> str:
                return _strip(page) or "Market_Leaders"

            def get_sheet_spec(_: str) -> Any:
                raise KeyError(f"schema_registry unavailable: {e}")

            self.get_route_family = get_route_family
            self.is_instrument_page = is_instrument_page
            self.normalize_page_name = normalize_page_name
            self.get_sheet_spec = get_sheet_spec

        # ---------------- Local builders (optional) ----------------
        try:
            from core.sheets.data_dictionary import build_data_dictionary_rows  # type: ignore
        except Exception:
            build_data_dictionary_rows = None  # type: ignore
        self.build_data_dictionary_rows = build_data_dictionary_rows

        try:
            from core.analysis.insights_builder import build_insights_rows  # type: ignore
        except Exception:
            build_insights_rows = None  # type: ignore
        self.build_insights_rows = build_insights_rows

        try:
            from core.analysis.top10_selector import build_top10_rows  # type: ignore
        except Exception:
            build_top10_rows = None  # type: ignore
        self.build_top10_rows = build_top10_rows

    # -----------------------------------------------------------------
    # Auth
    # -----------------------------------------------------------------
    def auth_guard(
        self,
        auth_token: str,
        authorization: Optional[str],
        x_app_token: Optional[str],
        token_query_used: bool,
    ) -> None:
        if self._auth_ok is None:
            return

        allow_query = False
        try:
            s = self._get_settings_cached()
            allow_query = bool(getattr(s, "allow_query_token", False))
        except Exception:
            allow_query = False

        if token_query_used and not allow_query:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Query token not allowed")

        ok = self._auth_ok(
            token=auth_token,
            authorization=authorization,
            headers={
                "X-APP-TOKEN": x_app_token,
                "Authorization": authorization,
            },
        )
        if not ok:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    # -----------------------------------------------------------------
    # Engine
    # -----------------------------------------------------------------
    async def get_engine(self) -> Any:
        try:
            from core.data_engine_v2 import get_engine  # type: ignore
            return await get_engine()
        except Exception:
            return None

    # -----------------------------------------------------------------
    # Schema
    # -----------------------------------------------------------------
    def normalize_page(self, raw: str) -> str:
        page = _strip(raw) or "Market_Leaders"
        try:
            return self.normalize_page_name(page, allow_output_pages=True)
        except Exception:
            return page

    def route_family(self, page: str) -> str:
        try:
            return str(self.get_route_family(page))
        except Exception:
            return "instrument"

    def schema_for_page(self, page: str) -> Tuple[List[str], List[str]]:
        if not self._HAS_SCHEMA:
            return (["Symbol", "Error"], ["symbol", "error"])

        spec = self.get_sheet_spec(page)
        cols = list(getattr(spec, "columns", None) or [])

        headers: List[str] = []
        keys: List[str] = []

        for c in cols:
            h = _strip(getattr(c, "header", None))
            k = _strip(getattr(c, "key", None))
            if h:
                headers.append(h)
            if k:
                keys.append(k)

        return headers, keys

    def normalize_row(self, keys: Sequence[str], raw: Mapping[str, Any], *, symbol_fallback: str = "") -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        raw = dict(raw or {})
        for k in keys:
            out[k] = raw.get(k, None)

        if symbol_fallback:
            if "symbol" in out and not out.get("symbol"):
                out["symbol"] = symbol_fallback
            if "Symbol" in out and not out.get("Symbol"):
                out["Symbol"] = symbol_fallback
        return out

    # -----------------------------------------------------------------
    # Local builders for special pages
    # -----------------------------------------------------------------
    def build_dictionary_page_rows(self, keys: Sequence[str]) -> List[Dict[str, Any]]:
        if not callable(self.build_data_dictionary_rows):
            return []

        try:
            rows_raw = self.build_data_dictionary_rows(include_meta_sheet=True)
        except TypeError:
            rows_raw = self.build_data_dictionary_rows()
        except Exception:
            return []

        out: List[Dict[str, Any]] = []
        for rr in _as_list(rows_raw):
            row_map = rr if isinstance(rr, Mapping) else _model_to_dict(rr)
            out.append(self.normalize_row(keys, row_map))
        return out

    def build_insights_page_rows(self, keys: Sequence[str], body: Mapping[str, Any]) -> List[Dict[str, Any]]:
        if not callable(self.build_insights_rows):
            return []

        try:
            # flexible call style
            try:
                rows_raw = self.build_insights_rows(body=body)
            except TypeError:
                try:
                    rows_raw = self.build_insights_rows(payload=body)
                except TypeError:
                    rows_raw = self.build_insights_rows()
        except Exception:
            return []

        out: List[Dict[str, Any]] = []
        for item in _as_list(rows_raw):
            row_map = item if isinstance(item, Mapping) else _model_to_dict(item)
            out.append(self.normalize_row(keys, row_map))
        return out

    def build_top10_page_rows(self, keys: Sequence[str], body: Mapping[str, Any]) -> List[Dict[str, Any]]:
        if not callable(self.build_top10_rows):
            return []

        try:
            try:
                rows_raw = self.build_top10_rows(body=body)
            except TypeError:
                try:
                    rows_raw = self.build_top10_rows(payload=body)
                except TypeError:
                    rows_raw = self.build_top10_rows()
        except Exception:
            return []

        out: List[Dict[str, Any]] = []
        for item in _as_list(rows_raw):
            row_map = item if isinstance(item, Mapping) else _model_to_dict(item)
            out.append(self.normalize_row(keys, row_map))
        return out


# =============================================================================
# Router builder
# =============================================================================
def _build_router(reason: str, core_router: Optional[APIRouter]) -> APIRouter:
    svc = _Service(reason=reason)
    root = APIRouter(tags=["tfb"])

    if isinstance(core_router, APIRouter):
        try:
            root.include_router(core_router)
        except Exception as e:
            logger.warning("Including core enriched router failed: %s", e)

    enriched = APIRouter(prefix="/v1/enriched", tags=["enriched"])
    legacy = APIRouter(tags=["quotes"])

    async def _health_payload() -> Dict[str, Any]:
        return {
            "status": "ok",
            "module": "routes.enriched_quote",
            "router_version": ROUTER_VERSION,
            "reason": reason,
            "schema_available": bool(getattr(svc, "_HAS_SCHEMA", False)),
        }

    @enriched.get("/health", include_in_schema=False)
    async def enriched_health() -> Dict[str, Any]:
        return await _health_payload()

    @enriched.get("/headers", include_in_schema=False)
    async def enriched_headers(page: str = Query(default="Market_Leaders")) -> Dict[str, Any]:
        page_norm = svc.normalize_page(page)
        headers, keys = svc.schema_for_page(page_norm)
        return {
            "status": "success" if headers else "degraded",
            "page": page_norm,
            "headers": headers,
            "keys": keys,
            "router_version": ROUTER_VERSION,
            "reason": None if headers else reason,
            "route_family": svc.route_family(page_norm),
        }

    # -------------------------------------------------------------------------
    # Instrument rows
    # -------------------------------------------------------------------------
    async def _build_instrument_rows(
        page_norm: str,
        keys: Sequence[str],
        symbols: Sequence[str],
        mode_q: str,
    ) -> Tuple[List[Dict[str, Any]], int]:
        if not symbols:
            return [], 0

        engine = await svc.get_engine()
        if engine is None:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Data engine unavailable")

        quotes_map: Dict[str, Dict[str, Any]] = {}

        try:
            batch_fn = getattr(engine, "get_enriched_quotes_batch", None) or getattr(engine, "get_quotes_batch", None)

            if callable(batch_fn):
                got = await batch_fn(symbols, mode=mode_q or "", schema=list(keys) if keys else None)

                if isinstance(got, dict):
                    quotes_map = {
                        str(k): (v if isinstance(v, dict) else _model_to_dict(v))
                        for k, v in got.items()
                    }
                elif isinstance(got, list):
                    tmp: Dict[str, Dict[str, Any]] = {}
                    for item in got:
                        d = item if isinstance(item, dict) else _model_to_dict(item)
                        sym = _strip(d.get("symbol") or d.get("ticker") or d.get("Symbol") or "")
                        if sym:
                            tmp[sym] = d
                    quotes_map = tmp
                else:
                    quotes_map = {s: {"symbol": s, "error": "batch_return_shape_unsupported"} for s in symbols}

            else:
                one_fn = getattr(engine, "get_enriched_quote_dict", None)
                if callable(one_fn):
                    results = await asyncio.gather(
                        *[one_fn(s, schema=list(keys) if keys else None) for s in symbols],
                        return_exceptions=True,
                    )
                    for s, item in zip(symbols, results):
                        if isinstance(item, dict):
                            quotes_map[s] = item
                        elif isinstance(item, Exception):
                            quotes_map[s] = {"symbol": s, "error": f"{type(item).__name__}: {item}"}
                        else:
                            quotes_map[s] = {"symbol": s, "error": "unknown_item"}
                else:
                    quotes_map = {s: {"symbol": s, "error": "engine_missing_quote_methods"} for s in symbols}

        except Exception as e:
            quotes_map = {s: {"symbol": s, "error": f"{type(e).__name__}: {e}"} for s in symbols}

        rows_out: List[Dict[str, Any]] = []
        errors = 0

        for s in symbols:
            raw = quotes_map.get(s) or {"symbol": s, "error": "missing_row"}
            if raw.get("error"):
                errors += 1
            rows_out.append(svc.normalize_row(keys, raw, symbol_fallback=s))

        return rows_out, errors

    # -------------------------------------------------------------------------
    # Special rows
    # -------------------------------------------------------------------------
    async def _build_special_rows(page_norm: str, keys: Sequence[str], body: Mapping[str, Any]) -> Tuple[List[Dict[str, Any]], str]:
        family = svc.route_family(page_norm)

        if family == "dictionary":
            return svc.build_dictionary_page_rows(keys), "data_dictionary"

        if family == "insights":
            rows = svc.build_insights_page_rows(keys, body)
            return rows, "insights_builder"

        if family == "top10":
            rows = svc.build_top10_page_rows(keys, body)
            return rows, "top10_selector"

        return [], "unknown"

    # -------------------------------------------------------------------------
    # Main handlers
    # -------------------------------------------------------------------------
    async def _handle_single_quote(
        request: Request,
        body: Dict[str, Any],
        page_q: str,
        mode_q: str,
        token_q: Optional[str],
        x_app_token: Optional[str],
        authorization: Optional[str],
        x_request_id: Optional[str],
    ) -> Dict[str, Any]:
        start = time.time()
        request_id = x_request_id or uuid.uuid4().hex[:12]

        token_query_used = bool(token_q and _strip(token_q))
        auth_token = _extract_token(x_app_token, authorization, token_q)
        svc.auth_guard(auth_token, authorization, x_app_token, token_query_used)

        symbol = _strip(body.get("symbol") or body.get("ticker") or body.get("requested_symbol") or "")
        if not symbol:
            syms = _get_list(body, "symbols", "tickers")
            symbol = syms[0] if syms else ""
        if not symbol:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing symbol")

        page_raw = page_q or _strip(body.get("page") or body.get("sheet_name") or body.get("sheet") or "Market_Leaders")
        page_norm = svc.normalize_page(page_raw)
        headers, keys = svc.schema_for_page(page_norm)
        route_family = svc.route_family(page_norm)

        if route_family != "instrument":
            # For special pages, single quote does not make business sense.
            # Return schema-aware row with explicit warning instead of generic fallback.
            row = svc.normalize_row(keys or ["symbol", "error"], {"symbol": symbol, "error": f"single_quote_not_supported_for_{route_family}_page"}, symbol_fallback=symbol)
            return {
                "status": "partial",
                "page": page_norm,
                "route_family": route_family,
                "headers": headers,
                "keys": keys or ["symbol", "error"],
                "row": row,
                "quote": row,
                "data": row,
                "version": ROUTER_VERSION,
                "request_id": request_id,
                "meta": {
                    "duration_ms": (time.time() - start) * 1000.0,
                    "mode": mode_q,
                    "dispatch": "special_guard",
                },
            }

        rows_out, errors = await _build_instrument_rows(page_norm, keys or ["symbol", "error"], [symbol], mode_q)
        row = rows_out[0] if rows_out else svc.normalize_row(keys or ["symbol", "error"], {"symbol": symbol, "error": "missing_row"}, symbol_fallback=symbol)
        status_out = "success" if errors == 0 and not row.get("error") else "partial"

        return {
            "status": status_out,
            "page": page_norm,
            "route_family": route_family,
            "headers": headers,
            "keys": keys or ["symbol", "error"],
            "row": row,
            "quote": row,
            "data": row,
            "version": ROUTER_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - start) * 1000.0,
                "mode": mode_q,
                "dispatch": "instrument",
            },
        }

    async def _handle_sheet_rows(
        request: Request,
        body: Dict[str, Any],
        mode_q: str,
        token_q: Optional[str],
        x_app_token: Optional[str],
        authorization: Optional[str],
        x_request_id: Optional[str],
    ) -> Dict[str, Any]:
        start = time.time()
        request_id = x_request_id or uuid.uuid4().hex[:12]

        token_query_used = bool(token_q and _strip(token_q))
        auth_token = _extract_token(x_app_token, authorization, token_q)
        svc.auth_guard(auth_token, authorization, x_app_token, token_query_used)

        page_raw = _strip(
            body.get("page")
            or body.get("sheet_name")
            or body.get("sheetName")
            or body.get("sheet")
            or body.get("name")
            or body.get("tab")
            or "Market_Leaders"
        )
        page_norm = svc.normalize_page(page_raw)
        route_family = svc.route_family(page_norm)

        symbols = _get_list(body, "symbols", "tickers", "tickers_list")
        include_matrix = _get_bool(body, "include_matrix", True)
        include_headers = _get_bool(body, "include_headers", True)

        top_n = _get_int(body, "top_n", 200)
        limit = _get_int(body, "limit", 0)
        if limit > 0:
            top_n = limit
        top_n = max(1, min(5000, top_n))
        symbols = symbols[:top_n]

        headers, keys = svc.schema_for_page(page_norm)
        schema_keys = keys or ["symbol", "error"]

        # ---------- special pages ----------
        if route_family != "instrument":
            rows_out, dispatch = await _build_special_rows(page_norm, schema_keys, body)
            status_out = "success" if rows_out else "success"

            response: Dict[str, Any] = {
                "status": status_out,
                "page": page_norm,
                "route_family": route_family,
                "headers": headers if include_headers else [],
                "keys": schema_keys,
                "rows": rows_out,
                "rows_matrix": _rows_matrix(rows_out, schema_keys) if include_matrix else None,
                "quotes": rows_out,
                "data": rows_out,
                "version": ROUTER_VERSION,
                "request_id": request_id,
                "meta": {
                    "duration_ms": (time.time() - start) * 1000.0,
                    "count": len(rows_out),
                    "dispatch": dispatch,
                    "mode": mode_q,
                },
            }
            return response

        # ---------- instrument pages ----------
        if not symbols:
            return {
                "status": "success",
                "page": page_norm,
                "route_family": route_family,
                "headers": headers if include_headers else [],
                "keys": schema_keys,
                "rows": [],
                "rows_matrix": [] if include_matrix else None,
                "quotes": [],
                "data": [],
                "version": ROUTER_VERSION,
                "request_id": request_id,
                "meta": {
                    "duration_ms": (time.time() - start) * 1000.0,
                    "requested": 0,
                    "errors": 0,
                    "mode": mode_q,
                    "dispatch": "schema_only_explicit",
                },
            }

        rows_out, errors = await _build_instrument_rows(page_norm, schema_keys, symbols, mode_q)
        status_out = "success" if errors == 0 else ("partial" if errors < len(symbols) else "error")

        return {
            "status": status_out,
            "page": page_norm,
            "route_family": route_family,
            "headers": headers if include_headers else [],
            "keys": schema_keys,
            "rows": rows_out,
            "rows_matrix": _rows_matrix(rows_out, schema_keys) if include_matrix else None,
            "quotes": rows_out,
            "data": rows_out,
            "error": f"{errors} errors" if errors else None,
            "version": ROUTER_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - start) * 1000.0,
                "requested": len(symbols),
                "errors": errors,
                "mode": mode_q,
                "dispatch": "instrument",
            },
        }

    # -------------------------------------------------------------------------
    # Routes
    # -------------------------------------------------------------------------
    @enriched.post("/quote")
    async def enriched_quote(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        page: str = Query(default="", description="Optional page name for schema selection"),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_single_quote(request, body, page, mode, token, x_app_token, authorization, x_request_id)

    @enriched.post("/sheet-rows")
    async def enriched_sheet_rows(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @legacy.post("/quote")
    async def quote_alias(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        page: str = Query(default="", description="Optional page name for schema selection"),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_single_quote(request, body, page, mode, token, x_app_token, authorization, x_request_id)

    @legacy.post("/quotes")
    async def quotes_primary(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @legacy.post("/sheet-rows", include_in_schema=False)
    async def sheet_rows_alias(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    root.include_router(enriched)
    root.include_router(legacy)
    return root


# =============================================================================
# Router resolution
# =============================================================================
def get_router() -> APIRouter:
    """
    Lazily import and return the wrapper router.
    If core.enriched_quote exists, include it; wrapper endpoints remain guaranteed.
    """
    global router
    if router is not None:
        return router

    core_r: Optional[APIRouter] = None
    reason = "wrapper_ok"

    try:
        mod = importlib.import_module("core.enriched_quote")
        r = getattr(mod, "router", None)
        if r is None:
            fn = getattr(mod, "get_router", None)
            if callable(fn):
                r = fn()
        if isinstance(r, APIRouter):
            core_r = r
        else:
            reason = "core.enriched_quote_no_router"
    except Exception as e:
        reason = f"core_import_failed: {type(e).__name__}: {e}"
        core_r = None

    router = _build_router(reason=reason, core_router=core_r)
    return router


def mount(app: Any) -> None:
    """
    Dynamic loader hook.
    """
    r = get_router()
    try:
        app.include_router(r)
        logger.info("Mounted enriched wrapper router. version=%s", ROUTER_VERSION)
    except Exception as e:
        logger.error("Mount failed for routes.enriched_quote: %s", e)
        raise


__all__ = ["ROUTER_VERSION", "get_router", "mount", "router"]
