#!/usr/bin/env python3
"""
routes/enriched_quote.py
================================================================================
TFB Enriched Quote Routes Wrapper — v5.5.0 (RENDER-SAFE / SCHEMA-AWARE / PROMETHEUS-SAFE)
================================================================================

What this revision fixes (your current prod symptom: /quotes = 404)
- ✅ Adds a FIRST-CLASS POST /quotes endpoint (Google Sheets + runner compatibility)
- ✅ Keeps existing /v1/enriched/quote and /v1/enriched/sheet-rows endpoints
- ✅ Ensures router mounting works with dynamic loaders (router stays lazy; mount(app) supported)
- ✅ Fixes missing imports/bugs in the previous wrapper (asyncio + _model_to_dict)
- ✅ Never does network I/O at import-time (engine + schema loaded lazily inside handlers)
- ✅ Prometheus-safe: no metrics created here

Behavior (compatibility-first)
- POST /quotes            -> schema-aligned response (headers/keys/rows/rows_matrix + aliases)
- POST /quote             -> single-symbol schema-aligned response
- POST /v1/enriched/quote -> same as /quote
- POST /v1/enriched/sheet-rows -> same as /quotes
- If core.enriched_quote exists, it is included (but /quotes is guaranteed by this wrapper)

Notes
- This wrapper returns a “sheet-rows style” payload even for /quotes to maximize
  compatibility with your runner’s robust response parsing.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import time
import uuid
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

logger = logging.getLogger("routes.enriched_quote")

ROUTER_VERSION = "5.5.0"

# IMPORTANT:
# Do NOT create APIRouter at import-time for dynamic loader behavior.
router: Optional[APIRouter] = None


# =============================================================================
# Small helpers (pure; safe)
# =============================================================================
def _strip(v: Any) -> str:
    try:
        return str(v).strip()
    except Exception:
        return ""


def _get_list(body: Dict[str, Any], *keys: str) -> List[str]:
    for k in keys:
        v = body.get(k)
        if isinstance(v, list):
            out: List[str] = []
            for x in v:
                s = _strip(x)
                if s:
                    out.append(s)
            return out
    return []


def _get_bool(body: Dict[str, Any], key: str, default: bool) -> bool:
    v = body.get(key)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


def _get_int(body: Dict[str, Any], key: str, default: int) -> int:
    v = body.get(key)
    try:
        if isinstance(v, (int, float)) and int(v) == v:
            return int(v)
        if isinstance(v, str) and v.strip().isdigit():
            return int(v.strip())
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

    # query token is only used if core.config allows it (checked later)
    return _strip(token_query)


def _rows_matrix(rows: List[Dict[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    out: List[List[Any]] = []
    for r in rows:
        out.append([r.get(k) for k in keys])
    return out


def _model_to_dict(obj: Any) -> Dict[str, Any]:
    """
    Best-effort conversion to dict for engine responses.
    Never raises.
    """
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    # pydantic v2
    try:
        if hasattr(obj, "model_dump"):
            d = obj.model_dump()  # type: ignore[attr-defined]
            return d if isinstance(d, dict) else {}
    except Exception:
        pass
    # pydantic v1
    try:
        if hasattr(obj, "dict"):
            d = obj.dict()  # type: ignore[attr-defined]
            return d if isinstance(d, dict) else {}
    except Exception:
        pass
    # dataclass / objects
    try:
        d = getattr(obj, "__dict__", None)
        if isinstance(d, dict):
            return dict(d)
    except Exception:
        pass
    return {}


# =============================================================================
# Shared service layer (lazy imports; schema-aware)
# =============================================================================
class _Service:
    def __init__(self, reason: str):
        self.reason = reason

        # Auth hooks (optional)
        try:
            from core.config import auth_ok as _auth_ok  # type: ignore
            from core.config import get_settings_cached as _get_settings_cached  # type: ignore
        except Exception:
            _auth_ok = None  # type: ignore

            def _get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
                return None

        self._auth_ok = _auth_ok
        self._get_settings_cached = _get_settings_cached

        # Schema hooks (optional)
        try:
            from core.sheets.page_catalog import CANONICAL_PAGES, normalize_page_name  # type: ignore
            from core.sheets.schema_registry import get_sheet_spec  # type: ignore
            from core.sheets.data_dictionary import build_data_dictionary_rows  # type: ignore

            self._HAS_SCHEMA = True
            self.CANONICAL_PAGES = list(CANONICAL_PAGES)  # type: ignore
            self.normalize_page_name = normalize_page_name  # type: ignore
            self.get_sheet_spec = get_sheet_spec  # type: ignore
            self.build_data_dictionary_rows = build_data_dictionary_rows  # type: ignore
        except Exception as e:
            self._HAS_SCHEMA = False
            self.CANONICAL_PAGES = []
            self._schema_import_error = e

            def normalize_page_name(page: str, *, allow_output_pages: bool = True) -> str:  # type: ignore
                p = _strip(page) or "Market_Leaders"
                return p

            def get_sheet_spec(_: str) -> Any:  # type: ignore
                raise KeyError(f"schema_registry unavailable: {e}")

            def build_data_dictionary_rows(*args: Any, **kwargs: Any) -> List[Dict[str, Any]]:  # type: ignore
                return []

            self.normalize_page_name = normalize_page_name
            self.get_sheet_spec = get_sheet_spec
            self.build_data_dictionary_rows = build_data_dictionary_rows

    async def get_engine(self) -> Any:
        try:
            from core.data_engine_v2 import get_engine  # type: ignore

            return await get_engine()
        except Exception:
            return None

    def auth_guard(self, auth_token: str, authorization: Optional[str], x_app_token: Optional[str], token_query_used: bool) -> None:
        """
        Enforces auth if core.config.auth_ok exists.
        Query token is only accepted if allow_query_token=True in settings.
        """
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
            headers={"X-APP-TOKEN": x_app_token, "Authorization": authorization},
        )
        if not ok:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    def schema_for_page(self, page: str) -> Tuple[List[str], List[str]]:
        """
        Returns (headers, keys) for the requested page. Always stable order.
        """
        if not self._HAS_SCHEMA:
            return (["Symbol", "Error"], ["symbol", "error"])

        spec = self.get_sheet_spec("Data_Dictionary" if page == "Data_Dictionary" else page)
        cols = list(getattr(spec, "columns", None) or [])
        headers = [_strip(getattr(c, "header", "")) for c in cols]
        keys = [_strip(getattr(c, "key", "")) for c in cols]
        headers = [h for h in headers if h]
        keys = [k for k in keys if k]
        return headers, keys

    def normalize_row(self, keys: Sequence[str], raw: Dict[str, Any], *, symbol_fallback: str = "") -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        raw = raw or {}
        for k in keys:
            out[k] = raw.get(k, None)

        if symbol_fallback:
            if "symbol" in out and not out.get("symbol"):
                out["symbol"] = symbol_fallback
            if "Symbol" in out and not out.get("Symbol"):
                out["Symbol"] = symbol_fallback
        return out

    def normalize_page(self, raw: str) -> str:
        p = _strip(raw) or "Market_Leaders"
        try:
            return self.normalize_page_name(p, allow_output_pages=True)
        except Exception:
            return p


# =============================================================================
# Router builder (guarantees /quotes)
# =============================================================================
def _build_router(reason: str, core_router: Optional[APIRouter]) -> APIRouter:
    svc = _Service(reason=reason)
    root = APIRouter(tags=["tfb"])

    # Include the "core" router if present (keeps any additional endpoints)
    if isinstance(core_router, APIRouter):
        try:
            root.include_router(core_router)
        except Exception as e:
            logger.warning("Including core enriched router failed: %s", e)

    # Always include our guaranteed endpoints:
    enriched = APIRouter(prefix="/v1/enriched", tags=["enriched"])
    legacy = APIRouter(tags=["quotes"])  # no prefix

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
        p = svc.normalize_page(page)
        h, k = svc.schema_for_page(p)
        return {
            "status": "success" if h else "degraded",
            "page": p,
            "headers": h,
            "keys": k,
            "router_version": ROUTER_VERSION,
            "reason": None if h else reason,
        }

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

        # token source
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
        engine = await svc.get_engine()
        if engine is None:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Data engine unavailable")

        raw_row: Dict[str, Any] = {}
        try:
            fn = getattr(engine, "get_enriched_quote_dict", None)
            if callable(fn):
                raw_row = await fn(symbol, schema=list(keys) if keys else None)
            else:
                fn2 = getattr(engine, "get_enriched_quote", None)
                if callable(fn2):
                    q = await fn2(symbol, schema=list(keys) if keys else None)
                    raw_row = _model_to_dict(q)
                else:
                    raw_row = {"symbol": symbol, "error": "engine_missing_quote_methods"}
        except Exception as e:
            raw_row = {"symbol": symbol, "error": f"{type(e).__name__}: {e}"}

        row = svc.normalize_row(keys or ["symbol", "error"], raw_row, symbol_fallback=symbol)
        status_out = "success" if not row.get("error") else "partial"

        return {
            "status": status_out,
            "page": page_norm,
            "headers": headers,
            "keys": keys,
            "row": row,
            "quote": row,   # alias
            "data": row,    # alias
            "version": ROUTER_VERSION,
            "request_id": request_id,
            "meta": {"duration_ms": (time.time() - start) * 1000.0, "mode": mode_q},
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

        # token source
        token_query_used = bool(token_q and _strip(token_q))
        auth_token = _extract_token(x_app_token, authorization, token_q)
        svc.auth_guard(auth_token, authorization, x_app_token, token_query_used)

        # page selection (support the many names your runner sends)
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

        symbols = _get_list(body, "symbols", "tickers", "tickers_list")
        include_matrix = _get_bool(body, "include_matrix", True)

        # limit/top_n safety (never allow 0)
        top_n = _get_int(body, "top_n", 200)
        limit = _get_int(body, "limit", 0)
        if limit > 0:
            top_n = limit
        top_n = max(1, min(5000, top_n))
        symbols = symbols[:top_n]

        headers, keys = svc.schema_for_page(page_norm)

        # Data_Dictionary computed locally (if available)
        if page_norm == "Data_Dictionary":
            rows_raw = svc.build_data_dictionary_rows(include_meta_sheet=True)
            rows_out = [svc.normalize_row(keys, rr, symbol_fallback="") for rr in rows_raw]
            return {
                "status": "success",
                "page": page_norm,
                "headers": headers,
                "keys": keys,
                "rows": rows_out,
                "rows_matrix": _rows_matrix(rows_out, keys) if include_matrix else None,
                "quotes": rows_out,  # alias
                "data": rows_out,    # alias
                "version": ROUTER_VERSION,
                "request_id": request_id,
                "meta": {
                    "duration_ms": (time.time() - start) * 1000.0,
                    "count": len(rows_out),
                    "schema_mode": "data_dictionary",
                },
            }

        # If no symbols, still return schema (schema-only response)
        if not symbols:
            return {
                "status": "success",
                "page": page_norm,
                "headers": headers,
                "keys": keys,
                "rows": [],
                "rows_matrix": [] if include_matrix else None,
                "quotes": [],  # alias
                "data": [],    # alias
                "version": ROUTER_VERSION,
                "request_id": request_id,
                "meta": {"duration_ms": (time.time() - start) * 1000.0, "requested": 0, "errors": 0, "mode": mode_q},
            }

        engine = await svc.get_engine()
        if engine is None:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Data engine unavailable")

        # Prefer batch method
        quotes_map: Dict[str, Dict[str, Any]] = {}
        try:
            fn = getattr(engine, "get_enriched_quotes_batch", None) or getattr(engine, "get_quotes_batch", None)
            if callable(fn):
                got = await fn(symbols, mode=mode_q or "", schema=list(keys) if keys else None)

                # Accept both mapping and list shapes
                if isinstance(got, dict):
                    quotes_map = {str(k): (v if isinstance(v, dict) else _model_to_dict(v)) for k, v in got.items()}
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
                # per-symbol fallback
                fn2 = getattr(engine, "get_enriched_quote_dict", None)
                if callable(fn2):
                    results = await asyncio.gather(
                        *[fn2(s, schema=list(keys) if keys else None) for s in symbols],
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
        schema_keys = keys or ["symbol", "error"]

        for s in symbols:
            raw = quotes_map.get(s) or {"symbol": s, "error": "missing_row"}
            if raw.get("error"):
                errors += 1
            rows_out.append(svc.normalize_row(schema_keys, raw, symbol_fallback=s))

        status_out = "success" if errors == 0 else ("partial" if errors < len(symbols) else "error")

        return {
            "status": status_out,
            "page": page_norm,
            "headers": headers,
            "keys": schema_keys,
            "rows": rows_out,
            "rows_matrix": _rows_matrix(rows_out, schema_keys) if include_matrix else None,
            "quotes": rows_out,  # alias
            "data": rows_out,    # alias
            "error": f"{errors} errors" if errors else None,
            "version": ROUTER_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - start) * 1000.0,
                "requested": len(symbols),
                "errors": errors,
                "mode": mode_q,
            },
        }

    # -------- Route registrations (same handlers, multiple paths) --------

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

    # Legacy/top-level compatibility routes expected by your runner / Sheets
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

    # If you ever want /sheet-rows at root too (handy for debugging)
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

    # Assemble
    root.include_router(enriched)
    root.include_router(legacy)
    return root


# =============================================================================
# Router resolution (preferred core.enriched_quote, but /quotes guaranteed here)
# =============================================================================
def get_router() -> APIRouter:
    """
    Lazily imports and returns the wrapper router.
    If core.enriched_quote is available, it is included; otherwise wrapper still works.
    """
    global router
    if router is not None:
        return router

    core_r: Optional[APIRouter] = None
    reason = "wrapper_ok"

    # Try core.enriched_quote (preferred)
    try:
        mod = importlib.import_module("core.enriched_quote")
        # Preferred: core.enriched_quote exports `router`
        r = getattr(mod, "router", None)
        # Fallback: core.enriched_quote exports get_router()
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
        logger.info("Mounted enriched wrapper router (includes /quotes). version=%s", ROUTER_VERSION)
    except Exception as e:
        logger.error("Mount failed for routes.enriched_quote: %s", e)
        raise


__all__ = ["ROUTER_VERSION", "get_router", "mount", "router"]
