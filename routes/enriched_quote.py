#!/usr/bin/env python3
"""
routes/enriched_quote.py
================================================================================
TFB Enriched Quote Routes Wrapper — v5.4.0 (RENDER-SAFE / SCHEMA-AWARE / PROMETHEUS-SAFE)
================================================================================

Why this wrapper exists:
- ✅ Render/Prod safe: no heavy imports / no network I/O at import-time
- ✅ Prometheus-safe: do NOT create metrics here (avoids duplicate-timeseries crashes)
- ✅ Dynamic-loader friendly: exposes mount(app) + get_router()
- ✅ Schema-aligned fallback router (Phase 4):
    - /v1/enriched/quote emits schema-aligned keys (page-aware)
    - /v1/enriched/sheet-rows returns FULL schema headers/keys and rows normalized to schema

Preferred implementation:
- Imports and mounts: core.enriched_quote (if present and healthy)

Fallback implementation:
- Self-contained schema-driven router that uses:
    - core.data_engine_v2 (engine)
    - core.sheets.schema_registry + core.sheets.page_catalog
    - core.sheets.data_dictionary (for Data_Dictionary)
"""

from __future__ import annotations

import importlib
import logging
import time
import uuid
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

logger = logging.getLogger("routes.enriched_quote")

ROUTER_VERSION = "5.4.0"

# IMPORTANT:
# Do NOT define `router = APIRouter(...)` at import-time, because your dynamic loader
# may prefer `router` over `mount()`. We keep lazy import/creation.
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


# =============================================================================
# Fallback router (schema-driven)
# =============================================================================
def _build_fallback_router(reason: str) -> APIRouter:
    """
    Schema-driven fallback router used only if core.enriched_quote cannot be imported.
    """

    r = APIRouter(prefix="/v1/enriched", tags=["enriched"])

    # --- optional auth from core.config (safe) ---
    try:
        from core.config import auth_ok as _auth_ok  # type: ignore
        from core.config import get_settings_cached as _get_settings_cached  # type: ignore
    except Exception:  # pragma: no cover
        _auth_ok = None  # type: ignore

        def _get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
            return None

    # --- schema/catalog imports (safe; local) ---
    try:
        from core.sheets.page_catalog import CANONICAL_PAGES, normalize_page_name  # type: ignore
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore
        from core.sheets.data_dictionary import build_data_dictionary_rows  # type: ignore

        _HAS_SCHEMA = True
    except Exception as e:  # pragma: no cover
        _HAS_SCHEMA = False
        CANONICAL_PAGES = []  # type: ignore

        def normalize_page_name(page: str, *, allow_output_pages: bool = True) -> str:  # type: ignore
            p = _strip(page) or "Market_Leaders"
            return p

        def get_sheet_spec(_: str) -> Any:  # type: ignore
            raise KeyError(f"schema_registry unavailable: {e}")

        def build_data_dictionary_rows(*args: Any, **kwargs: Any) -> List[Dict[str, Any]]:  # type: ignore
            return []

    # --- engine getter (lazy; async) ---
    async def _get_engine() -> Any:
        try:
            from core.data_engine_v2 import get_engine  # type: ignore

            return await get_engine()
        except Exception:
            return None

    def _auth_guard(auth_token: str, authorization: Optional[str], x_app_token: Optional[str]) -> None:
        if _auth_ok is None:
            return

        # query token allowed only if explicitly enabled (best-effort)
        allow_query = False
        try:
            s = _get_settings_cached()
            allow_query = bool(getattr(s, "allow_query_token", False))
        except Exception:
            allow_query = False

        # If the only token source was query and it's not allowed, blank it
        if auth_token and auth_token == _strip(x_app_token) == "" and allow_query is False:
            auth_token = ""

        ok = _auth_ok(
            token=auth_token,
            authorization=authorization,
            headers={"X-APP-TOKEN": x_app_token, "Authorization": authorization},
        )
        if not ok:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    def _schema_for_page(page: str) -> Tuple[List[str], List[str]]:
        """
        Returns (headers, keys) for the requested page. Always stable order.
        """
        if not _HAS_SCHEMA:
            # minimal fallback
            headers = ["Symbol", "Error"]
            keys = ["symbol", "error"]
            return headers, keys

        spec = get_sheet_spec("Data_Dictionary" if page == "Data_Dictionary" else page)
        cols = list(getattr(spec, "columns", None) or [])
        headers = [getattr(c, "header", "") for c in cols]
        keys = [getattr(c, "key", "") for c in cols]
        headers = [_strip(h) for h in headers if _strip(h)]
        keys = [_strip(k) for k in keys if _strip(k)]
        return headers, keys

    def _normalize_row(keys: Sequence[str], raw: Dict[str, Any], *, symbol_fallback: str = "") -> Dict[str, Any]:
        """
        Guarantee all schema keys exist in returned row dict.
        Missing => None.
        """
        out: Dict[str, Any] = {}
        raw = raw or {}
        for k in keys:
            out[k] = raw.get(k, None)
        if symbol_fallback and (("symbol" in out and not out.get("symbol")) or ("Symbol" in out and not out.get("Symbol"))):
            if "symbol" in out:
                out["symbol"] = symbol_fallback
            if "Symbol" in out:
                out["Symbol"] = symbol_fallback
        return out

    @r.get("/health", include_in_schema=False)
    async def health() -> Dict[str, Any]:
        return {
            "status": "degraded",
            "module": "routes.enriched_quote",
            "router_version": ROUTER_VERSION,
            "reason": reason,
            "schema_available": bool(_HAS_SCHEMA),
        }

    @r.get("/headers", include_in_schema=False)
    async def headers(
        page: str = Query(default="Market_Leaders"),
    ) -> Dict[str, Any]:
        try:
            p = normalize_page_name(page, allow_output_pages=True)
        except Exception:
            p = _strip(page) or "Market_Leaders"
        h, k = _schema_for_page(p)
        return {
            "status": "success" if h else "degraded",
            "page": p,
            "headers": h,
            "keys": k,
            "router_version": ROUTER_VERSION,
            "reason": None if h else reason,
        }

    @r.post("/quote")
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
        start = time.time()
        request_id = x_request_id or uuid.uuid4().hex[:12]

        auth_token = _extract_token(x_app_token, authorization, token)
        _auth_guard(auth_token, authorization, x_app_token)

        symbol = _strip(body.get("symbol") or body.get("ticker") or body.get("requested_symbol") or "")
        if not symbol:
            syms = _get_list(body, "symbols", "tickers")
            symbol = syms[0] if syms else ""

        if not symbol:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing symbol")

        page_raw = page or _strip(body.get("page") or body.get("sheet_name") or "Market_Leaders")
        try:
            page_norm = normalize_page_name(page_raw, allow_output_pages=True)
        except Exception:
            page_norm = _strip(page_raw) or "Market_Leaders"

        headers, keys = _schema_for_page(page_norm)

        engine = await _get_engine()
        if engine is None:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Data engine unavailable")

        # Prefer schema-aware dict from engine (normalized to provided schema keys)
        raw_row: Dict[str, Any] = {}
        try:
            fn = getattr(engine, "get_enriched_quote_dict", None)
            if callable(fn):
                raw_row = await fn(symbol, schema=list(keys) if keys else None)
            else:
                # fallback: model then to dict
                fn2 = getattr(engine, "get_enriched_quote", None)
                if callable(fn2):
                    q = await fn2(symbol, schema=list(keys) if keys else None)
                    raw_row = _model_to_dict(q)
        except Exception as e:
            raw_row = {"symbol": symbol, "error": f"{type(e).__name__}: {e}"}

        row = _normalize_row(keys or ["symbol", "error"], raw_row, symbol_fallback=symbol)

        return {
            "status": "success" if not row.get("error") else "partial",
            "page": page_norm,
            "headers": headers,
            "keys": keys,
            "row": row,
            "version": ROUTER_VERSION,
            "request_id": request_id,
            "meta": {"duration_ms": (time.time() - start) * 1000.0, "mode": mode},
        }

    @r.post("/sheet-rows")
    async def enriched_sheet_rows(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        start = time.time()
        request_id = x_request_id or uuid.uuid4().hex[:12]

        auth_token = _extract_token(x_app_token, authorization, token)
        _auth_guard(auth_token, authorization, x_app_token)

        page_raw = _strip(body.get("page") or body.get("sheet_name") or body.get("sheetName") or "Market_Leaders")
        try:
            page_norm = normalize_page_name(page_raw, allow_output_pages=True)
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"error": f"Invalid page: {e}", "allowed_pages": list(CANONICAL_PAGES) if CANONICAL_PAGES else []},
            )

        symbols = _get_list(body, "symbols", "tickers")
        top_n = int(body.get("top_n") or 200)
        top_n = max(1, min(5000, top_n))
        symbols = symbols[:top_n]
        include_matrix = _get_bool(body, "include_matrix", True)

        headers, keys = _schema_for_page(page_norm)

        # Data_Dictionary computed locally
        if page_norm == "Data_Dictionary":
            rows_raw = build_data_dictionary_rows(include_meta_sheet=True)
            rows_out = [_normalize_row(keys, rr, symbol_fallback="") for rr in rows_raw]
            return {
                "status": "success",
                "page": page_norm,
                "headers": headers,
                "keys": keys,
                "rows": rows_out,
                "rows_matrix": _rows_matrix(rows_out, keys) if include_matrix else None,
                "version": ROUTER_VERSION,
                "request_id": request_id,
                "meta": {
                    "duration_ms": (time.time() - start) * 1000.0,
                    "count": len(rows_out),
                    "schema_mode": "data_dictionary",
                },
            }

        if not symbols:
            # Always return full schema even if no symbols
            return {
                "status": "success",
                "page": page_norm,
                "headers": headers,
                "keys": keys,
                "rows": [],
                "rows_matrix": [] if include_matrix else None,
                "version": ROUTER_VERSION,
                "request_id": request_id,
                "meta": {"duration_ms": (time.time() - start) * 1000.0, "requested": 0, "errors": 0, "mode": mode},
            }

        engine = await _get_engine()
        if engine is None:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Data engine unavailable")

        # Prefer batch method (stable, per-input symbol)
        quotes_map: Dict[str, Dict[str, Any]] = {}
        try:
            fn = getattr(engine, "get_enriched_quotes_batch", None) or getattr(engine, "get_quotes_batch", None)
            if callable(fn):
                quotes_map = await fn(symbols, mode=mode or "", schema=list(keys) if keys else None)
            else:
                # per-symbol fallback
                fn2 = getattr(engine, "get_enriched_quote_dict", None)
                if callable(fn2):
                    got = await asyncio.gather(*[fn2(s, schema=list(keys) if keys else None) for s in symbols], return_exceptions=True)
                    for s, item in zip(symbols, got):
                        if isinstance(item, dict):
                            quotes_map[s] = item
                        else:
                            quotes_map[s] = {"symbol": s, "error": f"{type(item).__name__}"}
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
            rows_out.append(_normalize_row(keys or ["symbol", "error"], raw, symbol_fallback=s))

        status_out = "success" if errors == 0 else ("partial" if errors < len(symbols) else "error")

        return {
            "status": status_out,
            "page": page_norm,
            "headers": headers,
            "keys": keys,
            "rows": rows_out,
            "rows_matrix": _rows_matrix(rows_out, keys) if include_matrix else None,
            "error": f"{errors} errors" if errors else None,
            "version": ROUTER_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - start) * 1000.0,
                "requested": len(symbols),
                "errors": errors,
                "mode": mode,
            },
        }

    return r


# =============================================================================
# Router resolution (preferred core.enriched_quote)
# =============================================================================
def get_router() -> APIRouter:
    """
    Lazily imports and returns the real enriched router.
    If not available, returns a schema-driven fallback router.
    """
    global router
    if router is not None:
        return router

    # Preferred implementation
    try:
        mod = importlib.import_module("core.enriched_quote")
    except Exception as e:
        logger.warning("Failed to import core.enriched_quote: %s", e)
        router = _build_fallback_router(f"import_failed: {type(e).__name__}: {e}")
        return router

    # Preferred: core.enriched_quote exports `router`
    r = getattr(mod, "router", None)

    # Fallback: core.enriched_quote exports get_router()
    if r is None:
        fn = getattr(mod, "get_router", None)
        if callable(fn):
            try:
                r = fn()
            except Exception as e:
                logger.warning("core.enriched_quote.get_router() failed: %s", e)
                r = None

    if not isinstance(r, APIRouter):
        router = _build_fallback_router("core.enriched_quote has no APIRouter export")
        return router

    router = r
    return router


def mount(app: Any) -> None:
    """
    Dynamic loader hook.
    """
    r = get_router()
    try:
        app.include_router(r)
        logger.info("Mounted enriched router: %s", getattr(r, "prefix", "/v1/enriched"))
    except Exception as e:
        logger.warning("Mount failed; using fallback router. err=%s", e)
        app.include_router(_build_fallback_router(f"mount_failed: {type(e).__name__}: {e}"))


__all__ = ["ROUTER_VERSION", "get_router", "mount", "router"]
