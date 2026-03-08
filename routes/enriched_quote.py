#!/usr/bin/env python3
# routes/enriched_quote.py
"""
================================================================================
TFB Enriched Quote Routes Wrapper — v6.2.0
================================================================================
RENDER-SAFE • SCHEMA-AWARE • PAGE-DISPATCH SAFE • HEADER/ROW BALANCED
SPECIAL-BUILDER PRESERVING • ASYNC-SAFE • GET+POST COMPATIBLE
QUERY/BODY MERGE • LEGACY ALIAS SAFE • NULL-ROW AVOIDANCE

Why this revision
-----------------
- ✅ FIX: Adds GET support for /sheet-rows (your live checker uses GET, not POST)
- ✅ FIX: Adds /v1/enriched_quote compatibility aliases alongside /v1/enriched
- ✅ FIX: Accepts page/sheet/sheet_name/name/tab and tickers/symbols from query params
- ✅ FIX: Preserves payload-style builder output instead of collapsing to null rows
- ✅ FIX: Keeps Top_10_Investments / Insights / Data_Dictionary dispatch working even
        when schema/page helpers are partially unavailable
- ✅ FIX: Adds stronger fallback route-family detection for special pages
- ✅ FIX: Can derive schema from Data_Dictionary builder if schema registry is partial
- ✅ FIX: Keeps instrument pages engine-backed and schema-aligned
- ✅ SAFE: No network I/O at import-time
- ✅ SAFE: No Prometheus metric creation here

Behavior
--------
Supported route families:
- instrument pages      -> engine-backed quote rows
- insights page         -> local insights builder if available
- top10 page            -> local top10 builder if available
- data_dictionary page  -> local data dictionary builder

Compatibility routes
--------------------
- POST /quote
- POST /quotes
- GET  /sheet-rows
- POST /sheet-rows
- POST /v1/enriched/quote
- POST /v1/enriched/sheet-rows
- GET  /v1/enriched/sheet-rows
- POST /v1/enriched_quote/quote
- POST /v1/enriched_quote/sheet-rows
- GET  /v1/enriched_quote/sheet-rows
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
import time
import uuid
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

logger = logging.getLogger("routes.enriched_quote")

ROUTER_VERSION = "6.2.0"

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
            if out:
                return out
        if isinstance(v, str) and v.strip():
            parts = [p.strip() for p in v.split(",") if p.strip()]
            if parts:
                return parts
    return []


def _get_bool(body: Mapping[str, Any], key: str, default: bool) -> bool:
    v = body.get(key)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False
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


def _extract_token(
    x_app_token: Optional[str],
    authorization: Optional[str],
    token_query: Optional[str],
) -> str:
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
        return dict(obj)

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


async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


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


def _dict_or_empty(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _first_list(*vals: Any) -> List[Any]:
    for v in vals:
        if isinstance(v, list):
            return v
    return []


def _merge_non_empty(base: Dict[str, Any], extra: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in extra.items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out[k] = v
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
    # Page normalization / routing
    # -----------------------------------------------------------------
    def _fallback_normalize_page(self, raw: str) -> str:
        s = _strip(raw)
        if not s:
            return "Market_Leaders"

        compact = s.replace("-", "_").replace(" ", "_").lower()

        mapping = {
            "market_leaders": "Market_Leaders",
            "global_markets": "Global_Markets",
            "commodities_fx": "Commodities_FX",
            "commodities___fx": "Commodities_FX",
            "mutual_funds": "Mutual_Funds",
            "my_portfolio": "My_Portfolio",
            "insights_analysis": "Insights_Analysis",
            "insights___analysis": "Insights_Analysis",
            "top_10_investments": "Top_10_Investments",
            "data_dictionary": "Data_Dictionary",
        }
        return mapping.get(compact, s)

    def normalize_page(self, raw: str) -> str:
        page = _strip(raw) or "Market_Leaders"
        try:
            out = self.normalize_page_name(page, allow_output_pages=True)
            if _strip(out):
                return str(out)
        except Exception:
            pass
        return self._fallback_normalize_page(page)

    def _fallback_route_family(self, page: str) -> str:
        p = self._fallback_normalize_page(page)
        if p == "Insights_Analysis":
            return "insights"
        if p == "Top_10_Investments":
            return "top10"
        if p == "Data_Dictionary":
            return "dictionary"
        return "instrument"

    def route_family(self, page: str) -> str:
        try:
            fam = str(self.get_route_family(page))
            if _strip(fam):
                return fam
        except Exception:
            pass
        return self._fallback_route_family(page)

    # -----------------------------------------------------------------
    # Schema
    # -----------------------------------------------------------------
    def _schema_from_data_dictionary(self, page: str) -> Tuple[List[str], List[str]]:
        if not callable(self.build_data_dictionary_rows):
            return [], []

        headers: List[str] = []
        keys: List[str] = []

        try:
            try:
                rows = self.build_data_dictionary_rows(include_meta_sheet=True)
            except TypeError:
                rows = self.build_data_dictionary_rows()

            page_norm = self.normalize_page(page)

            for row in _as_list(rows):
                if isinstance(row, Mapping):
                    sheet_name = _strip(row.get("sheet") or row.get("Sheet") or row.get("page") or "")
                    if sheet_name != page_norm:
                        continue
                    h = _strip(row.get("header") or row.get("Header"))
                    k = _strip(row.get("key") or row.get("Key"))
                else:
                    d = _model_to_dict(row)
                    sheet_name = _strip(d.get("sheet") or d.get("Sheet") or d.get("page") or "")
                    if sheet_name != page_norm:
                        continue
                    h = _strip(d.get("header") or d.get("Header"))
                    k = _strip(d.get("key") or d.get("Key"))

                if h:
                    headers.append(h)
                if k:
                    keys.append(k)

        except Exception:
            return [], []

        return headers, keys

    def schema_for_page(self, page: str) -> Tuple[List[str], List[str]]:
        headers: List[str] = []
        keys: List[str] = []

        if self._HAS_SCHEMA:
            try:
                spec = self.get_sheet_spec(page)
                cols = list(getattr(spec, "columns", None) or [])

                if not cols and isinstance(spec, Mapping):
                    cols = list(spec.get("columns") or [])
                    if not cols and (spec.get("headers") or spec.get("keys")):
                        hs = list(spec.get("headers") or [])
                        ks = list(spec.get("keys") or [])
                        if hs:
                            headers = [_strip(h) for h in hs if _strip(h)]
                        if ks:
                            keys = [_strip(k) for k in ks if _strip(k)]

                if cols:
                    for c in cols:
                        if isinstance(c, Mapping):
                            h = _strip(c.get("header"))
                            k = _strip(c.get("key"))
                        else:
                            h = _strip(getattr(c, "header", None))
                            k = _strip(getattr(c, "key", None))
                        if h:
                            headers.append(h)
                        if k:
                            keys.append(k)
            except Exception:
                headers, keys = [], []

        if not headers or not keys:
            dd_headers, dd_keys = self._schema_from_data_dictionary(page)
            if dd_headers:
                headers = dd_headers
            if dd_keys:
                keys = dd_keys

        page_norm = self.normalize_page(page)

        if page_norm == "Data_Dictionary" and (not headers or not keys):
            headers = [
                "sheet",
                "group",
                "header",
                "key",
                "dtype",
                "fmt",
                "required",
                "source",
                "notes",
            ]
            keys = list(headers)

        if not headers and keys:
            headers = list(keys)
        if not keys and headers:
            keys = [h.lower().replace(" ", "_") for h in headers]

        if not headers or not keys:
            return (["Symbol", "Error"], ["symbol", "error"])

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
            if "ticker" in out and not out.get("ticker"):
                out["ticker"] = symbol_fallback
        return out

    # -----------------------------------------------------------------
    # Flexible builder calling
    # -----------------------------------------------------------------
    async def _call_builder_flexible(
        self,
        builder: Any,
        *,
        request: Request,
        engine: Any,
        body: Mapping[str, Any],
        limit: int,
        mode: str,
    ) -> Any:
        if not callable(builder):
            return None

        settings = None
        try:
            settings = self._get_settings_cached()
        except Exception:
            settings = None

        criteria = body.get("criteria") if isinstance(body.get("criteria"), dict) else None

        attempts: List[Dict[str, Any]] = [
            {
                "engine": engine,
                "request": request,
                "settings": settings,
                "body": dict(body),
                "criteria": criteria,
                "limit": limit,
                "mode": mode,
            },
            {
                "request": request,
                "settings": settings,
                "body": dict(body),
                "criteria": criteria,
                "limit": limit,
                "mode": mode,
            },
            {
                "engine": engine,
                "body": dict(body),
                "criteria": criteria,
                "limit": limit,
                "mode": mode,
            },
            {
                "body": dict(body),
                "criteria": criteria,
                "limit": limit,
                "mode": mode,
            },
            {
                "payload": dict(body),
                "limit": limit,
                "mode": mode,
            },
            {
                "data": dict(body),
                "limit": limit,
                "mode": mode,
            },
            {},
        ]

        last_exc: Optional[Exception] = None

        for kwargs in attempts:
            try:
                result = builder(**kwargs)
                return await _maybe_await(result)
            except TypeError as e:
                last_exc = e
                continue
            except Exception as e:
                last_exc = e
                break

        if last_exc:
            raise last_exc
        return None

    def _normalize_rows_from_any(self, rows_raw: Any, keys: Sequence[str]) -> List[Dict[str, Any]]:
        rows_out: List[Dict[str, Any]] = []
        for item in _as_list(rows_raw):
            row_map = item if isinstance(item, Mapping) else _model_to_dict(item)
            if row_map:
                rows_out.append(self.normalize_row(keys, row_map))
            else:
                rows_out.append(_normalize_item_to_row(item, keys))
        return rows_out

    async def build_dictionary_page_payload(
        self,
        *,
        page_norm: str,
        headers: Sequence[str],
        keys: Sequence[str],
        include_headers: bool,
        include_matrix: bool,
        request_id: str,
        started_at: float,
        mode: str,
    ) -> Dict[str, Any]:
        rows_out: List[Dict[str, Any]] = []

        if callable(self.build_data_dictionary_rows):
            try:
                try:
                    rows_raw = self.build_data_dictionary_rows(include_meta_sheet=False)
                except TypeError:
                    rows_raw = self.build_data_dictionary_rows()
                rows_out = self._normalize_rows_from_any(rows_raw, keys)
            except Exception as e:
                logger.warning("Data dictionary builder failed: %s", e)
                rows_out = []

        return {
            "status": "success",
            "page": page_norm,
            "route_family": "dictionary",
            "headers": list(headers) if include_headers else [],
            "keys": list(keys),
            "rows": rows_out,
            "rows_matrix": _rows_matrix(rows_out, keys) if include_matrix else None,
            "quotes": rows_out,
            "data": rows_out,
            "version": ROUTER_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - started_at) * 1000.0,
                "count": len(rows_out),
                "dispatch": "data_dictionary",
                "mode": mode,
            },
        }

    async def build_special_builder_payload(
        self,
        *,
        builder: Any,
        dispatch_name: str,
        route_family: str,
        request: Request,
        engine: Any,
        page_norm: str,
        headers: Sequence[str],
        keys: Sequence[str],
        body: Mapping[str, Any],
        limit: int,
        include_headers: bool,
        include_matrix: bool,
        request_id: str,
        started_at: float,
        mode: str,
    ) -> Dict[str, Any]:
        if not callable(builder):
            return {
                "status": "success",
                "page": page_norm,
                "route_family": route_family,
                "headers": list(headers) if include_headers else [],
                "keys": list(keys),
                "rows": [],
                "rows_matrix": [] if include_matrix else None,
                "quotes": [],
                "data": [],
                "version": ROUTER_VERSION,
                "request_id": request_id,
                "meta": {
                    "duration_ms": (time.time() - started_at) * 1000.0,
                    "count": 0,
                    "dispatch": dispatch_name,
                    "mode": mode,
                    "warning": f"{dispatch_name}_builder_unavailable",
                },
            }

        try:
            result = await self._call_builder_flexible(
                builder,
                request=request,
                engine=engine,
                body=body,
                limit=limit,
                mode=mode,
            )
        except Exception as e:
            logger.warning("%s builder failed: %s", dispatch_name, e)
            return {
                "status": "partial",
                "page": page_norm,
                "route_family": route_family,
                "headers": list(headers) if include_headers else [],
                "keys": list(keys),
                "rows": [],
                "rows_matrix": [] if include_matrix else None,
                "quotes": [],
                "data": [],
                "error": f"{type(e).__name__}: {e}",
                "version": ROUTER_VERSION,
                "request_id": request_id,
                "meta": {
                    "duration_ms": (time.time() - started_at) * 1000.0,
                    "count": 0,
                    "dispatch": dispatch_name,
                    "mode": mode,
                    "warning": f"{dispatch_name}_builder_failed",
                },
            }

        if isinstance(result, dict):
            result_headers = _first_list(
                result.get("headers"),
                result.get("display_headers"),
                result.get("sheet_headers"),
                result.get("column_headers"),
            )
            result_keys = _first_list(result.get("keys"))
            keys_out = [str(k) for k in (result_keys or list(keys)) if _strip(k)]
            headers_out = [str(h) for h in (result_headers or list(headers)) if _strip(h)]

            if not keys_out:
                keys_out = list(keys)

            rows_raw = result.get("rows")
            rows_matrix_raw = result.get("rows_matrix")

            if rows_raw is None:
                data_candidate = result.get("data")
                if isinstance(data_candidate, list):
                    rows_raw = data_candidate
                elif isinstance(result.get("quotes"), list):
                    rows_raw = result.get("quotes")
                else:
                    rows_raw = []

            rows_out: List[Dict[str, Any]] = []

            if isinstance(rows_raw, list) and rows_raw:
                rows_out = self._normalize_rows_from_any(rows_raw, keys_out)
            elif isinstance(rows_matrix_raw, list) and rows_matrix_raw:
                for item in rows_matrix_raw:
                    rows_out.append(_normalize_item_to_row(item, keys_out))

            rows_matrix_out = rows_matrix_raw if (include_matrix and isinstance(rows_matrix_raw, list)) else None
            if include_matrix and rows_matrix_out is None:
                rows_matrix_out = _rows_matrix(rows_out, keys_out)

            status_out = _strip(result.get("status")) or "success"
            meta_in = _dict_or_empty(result.get("meta"))
            error_out = result.get("error")

            return {
                "status": status_out,
                "page": _strip(result.get("page")) or page_norm,
                "route_family": _strip(result.get("route_family")) or route_family,
                "headers": headers_out if include_headers else [],
                "keys": keys_out,
                "rows": rows_out,
                "rows_matrix": rows_matrix_out if include_matrix else None,
                "quotes": rows_out,
                "data": rows_out,
                "error": error_out,
                "version": ROUTER_VERSION,
                "request_id": request_id,
                "meta": {
                    **meta_in,
                    "duration_ms": (time.time() - started_at) * 1000.0,
                    "count": len(rows_out),
                    "dispatch": dispatch_name,
                    "mode": mode,
                    "builder_payload_preserved": True,
                },
            }

        rows_out = self._normalize_rows_from_any(result, keys)

        return {
            "status": "success",
            "page": page_norm,
            "route_family": route_family,
            "headers": list(headers) if include_headers else [],
            "keys": list(keys),
            "rows": rows_out,
            "rows_matrix": _rows_matrix(rows_out, keys) if include_matrix else None,
            "quotes": rows_out,
            "data": rows_out,
            "version": ROUTER_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - started_at) * 1000.0,
                "count": len(rows_out),
                "dispatch": dispatch_name,
                "mode": mode,
                "builder_payload_preserved": False,
            },
        }


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
    enriched_quote_alias = APIRouter(prefix="/v1/enriched_quote", tags=["enriched"])
    legacy = APIRouter(tags=["quotes"])

    async def _health_payload() -> Dict[str, Any]:
        return {
            "status": "ok",
            "module": "routes.enriched_quote",
            "router_version": ROUTER_VERSION,
            "reason": reason,
            "schema_available": bool(getattr(svc, "_HAS_SCHEMA", False)),
        }

    def _sheet_rows_body_from_query(
        *,
        page: Optional[str],
        sheet_name: Optional[str],
        sheet: Optional[str],
        name: Optional[str],
        tab: Optional[str],
        symbols: Optional[str],
        tickers: Optional[str],
        include_headers: Optional[str],
        include_matrix: Optional[str],
        limit: Optional[int],
        top_n: Optional[int],
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {}
        if page is not None:
            body["page"] = page
        if sheet_name is not None:
            body["sheet_name"] = sheet_name
        if sheet is not None:
            body["sheet"] = sheet
        if name is not None:
            body["name"] = name
        if tab is not None:
            body["tab"] = tab
        if symbols is not None:
            body["symbols"] = symbols
        if tickers is not None:
            body["tickers"] = tickers
        if include_headers is not None:
            body["include_headers"] = include_headers
        if include_matrix is not None:
            body["include_matrix"] = include_matrix
        if limit is not None:
            body["limit"] = limit
        if top_n is not None:
            body["top_n"] = top_n
        return body

    def _quote_body_from_query(
        *,
        symbol: Optional[str],
        ticker: Optional[str],
        page: Optional[str],
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {}
        if symbol is not None:
            body["symbol"] = symbol
        if ticker is not None:
            body["ticker"] = ticker
        if page is not None:
            body["page"] = page
        return body

    @enriched.get("/health", include_in_schema=False)
    async def enriched_health() -> Dict[str, Any]:
        return await _health_payload()

    @enriched_quote_alias.get("/health", include_in_schema=False)
    async def enriched_quote_alias_health() -> Dict[str, Any]:
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

    @enriched_quote_alias.get("/headers", include_in_schema=False)
    async def enriched_quote_alias_headers(page: str = Query(default="Market_Leaders")) -> Dict[str, Any]:
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
                try:
                    got = batch_fn(symbols, mode=mode_q or "", schema=list(keys) if keys else None)
                    got = await _maybe_await(got)
                except TypeError:
                    got = batch_fn(symbols)
                    got = await _maybe_await(got)

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
                    async def _one(sym: str) -> Any:
                        try:
                            try:
                                res = one_fn(sym, schema=list(keys) if keys else None)
                            except TypeError:
                                res = one_fn(sym)
                            return await _maybe_await(res)
                        except Exception as e:
                            return e

                    results = await asyncio.gather(*[_one(s) for s in symbols], return_exceptions=False)

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
            row = svc.normalize_row(
                keys or ["symbol", "error"],
                {"symbol": symbol, "error": f"single_quote_not_supported_for_{route_family}_page"},
                symbol_fallback=symbol,
            )
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
        row = rows_out[0] if rows_out else svc.normalize_row(
            keys or ["symbol", "error"],
            {"symbol": symbol, "error": "missing_row"},
            symbol_fallback=symbol,
        )
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

        if route_family != "instrument":
            engine = await svc.get_engine()

            if route_family == "dictionary":
                return await svc.build_dictionary_page_payload(
                    page_norm=page_norm,
                    headers=headers,
                    keys=schema_keys,
                    include_headers=include_headers,
                    include_matrix=include_matrix,
                    request_id=request_id,
                    started_at=start,
                    mode=mode_q,
                )

            if route_family == "insights":
                return await svc.build_special_builder_payload(
                    builder=svc.build_insights_rows,
                    dispatch_name="insights_builder",
                    route_family=route_family,
                    request=request,
                    engine=engine,
                    page_norm=page_norm,
                    headers=headers,
                    keys=schema_keys,
                    body=body,
                    limit=top_n,
                    include_headers=include_headers,
                    include_matrix=include_matrix,
                    request_id=request_id,
                    started_at=start,
                    mode=mode_q,
                )

            if route_family == "top10":
                return await svc.build_special_builder_payload(
                    builder=svc.build_top10_rows,
                    dispatch_name="top10_selector",
                    route_family=route_family,
                    request=request,
                    engine=engine,
                    page_norm=page_norm,
                    headers=headers,
                    keys=schema_keys,
                    body=body,
                    limit=top_n,
                    include_headers=include_headers,
                    include_matrix=include_matrix,
                    request_id=request_id,
                    started_at=start,
                    mode=mode_q,
                )

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
                    "count": 0,
                    "dispatch": "special_unknown",
                    "mode": mode_q,
                },
            }

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
    # Routes: POST quote
    # -------------------------------------------------------------------------
    @enriched.post("/quote")
    async def enriched_quote_post(
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

    @enriched_quote_alias.post("/quote")
    async def enriched_quote_alias_post(
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

    # -------------------------------------------------------------------------
    # Routes: GET quote (optional compatibility)
    # -------------------------------------------------------------------------
    @enriched.get("/quote", include_in_schema=False)
    async def enriched_quote_get(
        request: Request,
        symbol: Optional[str] = Query(default=None),
        ticker: Optional[str] = Query(default=None),
        page: str = Query(default="", description="Optional page name for schema selection"),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _quote_body_from_query(symbol=symbol, ticker=ticker, page=page)
        return await _handle_single_quote(request, body, page, mode, token, x_app_token, authorization, x_request_id)

    @enriched_quote_alias.get("/quote", include_in_schema=False)
    async def enriched_quote_alias_get(
        request: Request,
        symbol: Optional[str] = Query(default=None),
        ticker: Optional[str] = Query(default=None),
        page: str = Query(default="", description="Optional page name for schema selection"),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _quote_body_from_query(symbol=symbol, ticker=ticker, page=page)
        return await _handle_single_quote(request, body, page, mode, token, x_app_token, authorization, x_request_id)

    # -------------------------------------------------------------------------
    # Routes: POST sheet-rows
    # -------------------------------------------------------------------------
    @enriched.post("/sheet-rows")
    async def enriched_sheet_rows_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @enriched_quote_alias.post("/sheet-rows")
    async def enriched_quote_alias_sheet_rows_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    # -------------------------------------------------------------------------
    # Routes: GET sheet-rows (critical for your live checker)
    # -------------------------------------------------------------------------
    @enriched.get("/sheet-rows")
    async def enriched_sheet_rows_get(
        request: Request,
        page: Optional[str] = Query(default=None),
        sheet_name: Optional[str] = Query(default=None),
        sheet: Optional[str] = Query(default=None),
        name: Optional[str] = Query(default=None),
        tab: Optional[str] = Query(default=None),
        symbols: Optional[str] = Query(default=None),
        tickers: Optional[str] = Query(default=None),
        include_headers: Optional[str] = Query(default=None),
        include_matrix: Optional[str] = Query(default=None),
        limit: Optional[int] = Query(default=None),
        top_n: Optional[int] = Query(default=None),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _sheet_rows_body_from_query(
            page=page,
            sheet_name=sheet_name,
            sheet=sheet,
            name=name,
            tab=tab,
            symbols=symbols,
            tickers=tickers,
            include_headers=include_headers,
            include_matrix=include_matrix,
            limit=limit,
            top_n=top_n,
        )
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @enriched_quote_alias.get("/sheet-rows")
    async def enriched_quote_alias_sheet_rows_get(
        request: Request,
        page: Optional[str] = Query(default=None),
        sheet_name: Optional[str] = Query(default=None),
        sheet: Optional[str] = Query(default=None),
        name: Optional[str] = Query(default=None),
        tab: Optional[str] = Query(default=None),
        symbols: Optional[str] = Query(default=None),
        tickers: Optional[str] = Query(default=None),
        include_headers: Optional[str] = Query(default=None),
        include_matrix: Optional[str] = Query(default=None),
        limit: Optional[int] = Query(default=None),
        top_n: Optional[int] = Query(default=None),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _sheet_rows_body_from_query(
            page=page,
            sheet_name=sheet_name,
            sheet=sheet,
            name=name,
            tab=tab,
            symbols=symbols,
            tickers=tickers,
            include_headers=include_headers,
            include_matrix=include_matrix,
            limit=limit,
            top_n=top_n,
        )
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    # -------------------------------------------------------------------------
    # Legacy aliases
    # -------------------------------------------------------------------------
    @legacy.post("/quote")
    async def quote_alias_post(
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

    @legacy.get("/quote", include_in_schema=False)
    async def quote_alias_get(
        request: Request,
        symbol: Optional[str] = Query(default=None),
        ticker: Optional[str] = Query(default=None),
        page: str = Query(default="", description="Optional page name for schema selection"),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _quote_body_from_query(symbol=symbol, ticker=ticker, page=page)
        return await _handle_single_quote(request, body, page, mode, token, x_app_token, authorization, x_request_id)

    @legacy.post("/quotes")
    async def quotes_primary_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @legacy.get("/quotes", include_in_schema=False)
    async def quotes_primary_get(
        request: Request,
        page: Optional[str] = Query(default=None),
        sheet_name: Optional[str] = Query(default=None),
        sheet: Optional[str] = Query(default=None),
        name: Optional[str] = Query(default=None),
        tab: Optional[str] = Query(default=None),
        symbols: Optional[str] = Query(default=None),
        tickers: Optional[str] = Query(default=None),
        include_headers: Optional[str] = Query(default=None),
        include_matrix: Optional[str] = Query(default=None),
        limit: Optional[int] = Query(default=None),
        top_n: Optional[int] = Query(default=None),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _sheet_rows_body_from_query(
            page=page,
            sheet_name=sheet_name,
            sheet=sheet,
            name=name,
            tab=tab,
            symbols=symbols,
            tickers=tickers,
            include_headers=include_headers,
            include_matrix=include_matrix,
            limit=limit,
            top_n=top_n,
        )
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @legacy.post("/sheet-rows", include_in_schema=False)
    async def sheet_rows_alias_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @legacy.get("/sheet-rows", include_in_schema=False)
    async def sheet_rows_alias_get(
        request: Request,
        page: Optional[str] = Query(default=None),
        sheet_name: Optional[str] = Query(default=None),
        sheet: Optional[str] = Query(default=None),
        name: Optional[str] = Query(default=None),
        tab: Optional[str] = Query(default=None),
        symbols: Optional[str] = Query(default=None),
        tickers: Optional[str] = Query(default=None),
        include_headers: Optional[str] = Query(default=None),
        include_matrix: Optional[str] = Query(default=None),
        limit: Optional[int] = Query(default=None),
        top_n: Optional[int] = Query(default=None),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _sheet_rows_body_from_query(
            page=page,
            sheet_name=sheet_name,
            sheet=sheet,
            name=name,
            tab=tab,
            symbols=symbols,
            tickers=tickers,
            include_headers=include_headers,
            include_matrix=include_matrix,
            limit=limit,
            top_n=top_n,
        )
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    root.include_router(enriched)
    root.include_router(enriched_quote_alias)
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
