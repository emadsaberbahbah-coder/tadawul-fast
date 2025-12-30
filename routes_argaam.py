# routes_argaam.py  (FULL REPLACEMENT)
"""
routes_argaam.py (repo root shim)
===============================================================
Argaam Router Shim – v1.5.2 (PROD SAFE + QUIET BOOT + AUTH-COMPAT)

IMPORTANT
- This module MUST be valid Python.
- Do NOT include Markdown code fences (three backticks) anywhere in this file,
  even inside docstrings/comments, because repo_hygiene_check blocks deploys.

Purpose
- Backward compatibility for imports: import routes_argaam; routes_argaam.router
- Prefer real router modules if present (in order):
    1) routes.routes_argaam
    2) core.routes_argaam
- If none load, provide a fallback router that always returns HTTP 200.

Quiet boot policy
- By default, NO warning/error logs during import.
- If DEBUG_ERRORS=1 or ARGAAM_SHIM_DEBUG=1, logs become verbose.

Auth compatibility (fallback router)
- Accepts token via:
    • X-APP-TOKEN
    • Authorization: Bearer <token>
- Uses APP_TOKEN / BACKUP_APP_TOKEN / REQUIRE_AUTH env vars.

No network calls at import time.
"""

from __future__ import annotations

import importlib
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("routes.argaam_shim")

SHIM_VERSION = "1.5.2"

DEFAULT_DELEGATES = ("routes.routes_argaam", "core.routes_argaam")
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _debug_enabled() -> bool:
    return _truthy(os.getenv("DEBUG_ERRORS", "0")) or _truthy(os.getenv("ARGAAM_SHIM_DEBUG", "0"))


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
    Quiet by default; verbose only when debug is enabled.
    """
    dbg = _debug_enabled()
    last_err: Optional[str] = None

    for mod in _delegate_list():
        r, err = _try_load_router(mod)
        if r is not None:
            if dbg:
                logger.info("[argaam_shim] delegate loaded: %s", mod)
            return r, mod, None

        last_err = err
        if dbg:
            logger.info("[argaam_shim] delegate not usable: %s | %s", mod, err)

    if dbg:
        logger.warning("[argaam_shim] no delegate loaded. last_err=%s", last_err)

    return None, None, (last_err or "No delegate modules could be loaded")


_delegate_router, _delegate_loaded_from, _delegate_error = _import_delegate()


# ---------------------------------------------------------------------
# Export router (delegate if available, else fallback)
# ---------------------------------------------------------------------
if _delegate_router is not None:
    router = _delegate_router  # type: ignore[assignment]
    DELEGATE_MODULE = _delegate_loaded_from or ""

else:
    from fastapi import APIRouter, Header, Query

    DELEGATE_MODULE = _delegate_loaded_from or ",".join(_delegate_list())

    router = APIRouter(prefix="/v1/argaam", tags=["KSA / Argaam (shim fallback)"])

    def _token_required() -> bool:
        # If REQUIRE_AUTH=true, require token even if APP_TOKEN empty (explicit)
        if _truthy(os.getenv("REQUIRE_AUTH", "0")):
            return True
        # Otherwise require only if at least one token exists
        return bool((os.getenv("APP_TOKEN") or "").strip() or (os.getenv("BACKUP_APP_TOKEN") or "").strip())

    def _extract_token(x_app_token: Optional[str], authorization: Optional[str]) -> Optional[str]:
        t = (x_app_token or "").strip()
        if t:
            return t

        auth = (authorization or "").strip()
        if not auth:
            return None

        low = auth.lower()
        if low.startswith("bearer "):
            tok = auth.split(" ", 1)[1].strip()
            return tok or None

        # Raw token (rare but allow)
        return auth or None

    def _auth_ok(provided_token: Optional[str]) -> bool:
        """
        Accept APP_TOKEN or BACKUP_APP_TOKEN if set.
        If none set and REQUIRE_AUTH is false -> allow.
        """
        required_primary = (os.getenv("APP_TOKEN") or "").strip()
        required_backup = (os.getenv("BACKUP_APP_TOKEN") or "").strip()
        require_auth = _truthy(os.getenv("REQUIRE_AUTH", "0"))

        if not required_primary and not required_backup and not require_auth:
            return True

        pt = (provided_token or "").strip()
        if not pt:
            return False

        if required_primary and pt == required_primary:
            return True
        if required_backup and pt == required_backup:
            return True
        return False

    def _clamp(s: str, n: int = 1500) -> str:
        t = (s or "").strip()
        if len(t) <= n:
            return t
        return t[: max(0, n - 12)] + " ...TRUNC..."

    def _base(symbol: str = "") -> Dict[str, Any]:
        return {
            "status": "error",
            "symbol": (symbol or "").strip().upper(),
            "market": "KSA",
            "currency": "SAR",
            "data_quality": "MISSING",
            "data_source": "argaam_shim_fallback",
            "error": _clamp(
                _delegate_error
                or "Delegate router import failed. Ensure routes/routes_argaam.py exists and exports router (or get_router())."
            ),
            "shim_version": SHIM_VERSION,
            "delegate_module": DELEGATE_MODULE,
        }

    @router.get("/health", include_in_schema=False)
    async def health() -> Dict[str, Any]:
        out = _base("")
        out["status"] = "ok"
        out["module"] = "routes_argaam (repo root shim fallback)"
        out["delegate_loaded"] = False
        out["auth_required"] = _token_required()
        out["debug_enabled"] = _debug_enabled()
        return out

    @router.get("/quote", include_in_schema=False)
    async def quote(
        symbol: str = Query(..., description="KSA symbol (e.g., 1120.SR or 1120)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        debug: int = Query(default=0),
    ) -> Dict[str, Any]:
        out = _base(symbol)
        out["delegate_loaded"] = False

        token = _extract_token(x_app_token, authorization)
        if _token_required() and not _auth_ok(token):
            out["error"] = "Unauthorized: invalid or missing token (X-APP-TOKEN or Authorization: Bearer)"
            out["auth_ok"] = False
        else:
            out["auth_ok"] = True

        if int(debug or 0) == 1 or _debug_enabled():
            out["debug"] = {
                "token_provided": bool((token or "").strip()),
                "delegate_modules_tried": list(_delegate_list()),
                "hint": "Create routes/routes_argaam.py exporting router (or get_router()).",
                "env": {
                    "REQUIRE_AUTH": os.getenv("REQUIRE_AUTH", ""),
                    "APP_TOKEN_SET": bool((os.getenv("APP_TOKEN") or "").strip()),
                    "BACKUP_APP_TOKEN_SET": bool((os.getenv("BACKUP_APP_TOKEN") or "").strip()),
                },
            }
        return out

    @router.get("/quotes", include_in_schema=False)
    async def quotes(
        symbols: str = Query(..., description="Comma-separated KSA symbols e.g. 1120.SR,2222.SR"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        debug: int = Query(default=0),
    ) -> Dict[str, Any]:
        arr = [s.strip() for s in (symbols or "").split(",") if s.strip()][:50]

        items: List[Dict[str, Any]] = []
        for s in arr:
            items.append(
                await quote(
                    symbol=s,
                    x_app_token=x_app_token,
                    authorization=authorization,
                    debug=debug,
                )
            )

        return {"status": "ok", "count": len(items), "items": items, "shim_version": SHIM_VERSION}


__all__ = ["router", "SHIM_VERSION", "DELEGATE_MODULE"]
