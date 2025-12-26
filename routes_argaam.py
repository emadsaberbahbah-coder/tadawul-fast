# routes_argaam.py
"""
routes_argaam.py (repo root shim)
===============================================================
Argaam Router Shim – v1.4.1 (PROD SAFE + QUIET BOOT)

This module MUST be valid Python. Do NOT include Markdown fences (```).

Purpose
- Backward compatibility for imports: `import routes_argaam; routes_argaam.router`
- Prefer real router modules if present (in order):
    1) routes.routes_argaam
    2) core.routes_argaam
- If none load, provide a fallback router that always returns HTTP 200.

No network calls at import time.
"""

from __future__ import annotations

import importlib
import logging
import os
from typing import Any, Optional, Tuple

logger = logging.getLogger("routes.argaam_shim")

SHIM_VERSION = "1.4.1"

DEFAULT_DELEGATES = ("routes.routes_argaam", "core.routes_argaam")
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _delegate_list() -> Tuple[str, ...]:
    raw = (os.getenv("ARGAAM_DELEGATE_MODULES") or "").strip()
    if raw:
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        if parts:
            return tuple(parts)
    return DEFAULT_DELEGATES


def _try_load_router(mod_name: str) -> Tuple[Optional[Any], Optional[str]]:
    """
    Return (router_obj, error). Never raises.
    Accepts:
      - module.router
      - module.get_router() -> router
    """
    try:
        m = importlib.import_module(mod_name)

        r = getattr(m, "router", None)
        if r is not None:
            return r, None

        gr = getattr(m, "get_router", None)
        if callable(gr):
            try:
                r2 = gr()
                if r2 is not None:
                    return r2, None
                return None, f"{mod_name}.get_router() returned None"
            except Exception as exc:
                return None, f"{mod_name}.get_router() failed: {exc.__class__.__name__}: {exc}"

        return None, f"{mod_name} has no router/get_router"
    except Exception as exc:
        return None, f"{mod_name} import failed: {exc.__class__.__name__}: {exc}"


def _import_delegate() -> Tuple[Optional[Any], Optional[str], Optional[str]]:
    """
    Return (router_obj, loaded_from, error). Never raises.
    """
    dbg = _truthy(os.getenv("DEBUG_ERRORS", "0"))
    last_err = None

    for mod in _delegate_list():
        r, err = _try_load_router(mod)
        if r is not None:
            return r, mod, None
        last_err = err
        if dbg:
            logger.exception("[argaam_shim] delegate not usable: %s | %s", mod, err)
        else:
            logger.warning("[argaam_shim] delegate not usable: %s | %s", mod, err)

    return None, None, (last_err or "No delegate modules could be loaded")


_delegate_router, _delegate_loaded_from, _delegate_error = _import_delegate()

# ---------------------------------------------------------------------
# Export router
# ---------------------------------------------------------------------
if _delegate_router is not None:
    router = _delegate_router  # type: ignore[assignment]
    DELEGATE_MODULE = _delegate_loaded_from or ""
else:
    from fastapi import APIRouter, Header, Query

    DELEGATE_MODULE = _delegate_loaded_from or ",".join(_delegate_list())

    router = APIRouter(prefix="/v1/argaam", tags=["KSA / Argaam (shim fallback)"])

    def _app_token_required() -> bool:
        return bool((os.getenv("APP_TOKEN") or "").strip())

    def _auth_ok(x_app_token: Optional[str]) -> bool:
        required = (os.getenv("APP_TOKEN") or "").strip()
        if not required:
            return True
        provided = (x_app_token or "").strip()
        return bool(provided) and provided == required

    def _base(symbol: str = "") -> dict:
        return {
            "status": "error",
            "symbol": (symbol or "").strip().upper(),
            "market": "KSA",
            "currency": "SAR",
            "data_quality": "MISSING",
            "data_source": "argaam_shim_fallback",
            "error": _delegate_error
            or "Delegate router import failed. Ensure routes/routes_argaam.py exists and exports `router`.",
            "shim_version": SHIM_VERSION,
            "delegate_module": DELEGATE_MODULE,
        }

    @router.get("/health")
    async def health() -> dict:
        out = _base("")
        out["module"] = "routes_argaam (repo root shim fallback)"
        out["delegate_loaded"] = False
        out["app_token_required"] = _app_token_required()
        return out

    @router.get("/quote")
    async def quote(
        symbol: str = Query(..., description="KSA symbol (e.g., 1120.SR or 1120)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        debug: int = Query(default=0),
    ) -> dict:
        out = _base(symbol)
        out["delegate_loaded"] = False

        if _app_token_required() and not _auth_ok(x_app_token):
            out["error"] = "Unauthorized: invalid or missing X-APP-TOKEN"
            out["auth_ok"] = False
        else:
            out["auth_ok"] = True

        if int(debug or 0) == 1:
            out["debug"] = {
                "token_provided": bool((x_app_token or "").strip()),
                "delegate_modules_tried": list(_delegate_list()),
                "hint": "Create routes/routes_argaam.py exporting `router` (or `get_router()`).",
            }
        return out


__all__ = ["router", "SHIM_VERSION", "DELEGATE_MODULE"]
