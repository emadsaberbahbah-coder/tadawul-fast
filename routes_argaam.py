Here is a **FULL REPLACEMENT** for **`routes_argaam.py` (repo root shim)** — hardened, quieter on boot, and more flexible (tries multiple delegate modules + supports `router` or `get_router()`).

> **Important:** when you paste into the file, **do NOT include** the leading/trailing ``` fences inside the file content.

```python
# routes_argaam.py
"""
routes_argaam.py (repo root shim)
===============================================================
Argaam Router Shim – v1.4.0 (PROD SAFE + QUIET BOOT)

Purpose
- Backward compatibility for deployments importing `routes_argaam.router` from repo root.
- Prefer a real router from delegate modules (in order):
    1) routes.routes_argaam
    2) core.routes_argaam
- If delegation fails, expose a minimal fallback router that:
    • never crashes app startup
    • always returns HTTP 200 with status/error fields
    • keeps stable keys for Sheets/client code

Rules
- No engine init, no network, no heavy side effects at import time.
- Avoid noisy stacktraces on every worker boot unless DEBUG_ERRORS=1.
"""

from __future__ import annotations

import importlib
import logging
import os
from typing import Any, Callable, Optional, Tuple

logger = logging.getLogger("routes.argaam_shim")

SHIM_VERSION = "1.4.0"

# You can override delegate search order:
#   ARGAAM_DELEGATE_MODULES="routes.routes_argaam,core.routes_argaam"
DEFAULT_DELEGATE_MODULES = ("routes.routes_argaam", "core.routes_argaam")

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _parse_delegate_modules() -> Tuple[str, ...]:
    raw = (os.getenv("ARGAAM_DELEGATE_MODULES") or "").strip()
    if not raw:
        return DEFAULT_DELEGATE_MODULES
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return tuple(parts) if parts else DEFAULT_DELEGATE_MODULES


def _try_load_router_from_module(mod_name: str) -> Tuple[Optional[Any], Optional[str]]:
    """
    Return (router_obj, error_message). Never raises.
    Accepts either:
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

        return None, f"{mod_name} imported but no `router` or `get_router()` found"
    except Exception as exc:
        return None, f"{mod_name} import failed: {exc.__class__.__name__}: {exc}"


def _try_import_delegate_router() -> Tuple[Optional[Any], Optional[str], Optional[str]]:
    """
    Return (router_obj, loaded_from, error_message). Never raises.
    """
    dbg = _truthy(os.getenv("DEBUG_ERRORS", "0"))
    last_err = None
    for mod_name in _parse_delegate_modules():
        r, err = _try_load_router_from_module(mod_name)
        if r is not None:
            return r, mod_name, None
        last_err = err
        if dbg:
            logger.exception("[argaam_shim] Delegate not usable: %s | %s", mod_name, err)
        else:
            logger.warning("[argaam_shim] Delegate not usable: %s | %s", mod_name, err)

    return None, None, (last_err or "No delegate modules could be loaded")


_delegate_router, _delegate_loaded_from, _delegate_error = _try_import_delegate_router()

# Fast path: delegate router exists -> export directly
if _delegate_router is not None:
    router = _delegate_router  # type: ignore[assignment]
    DELEGATE_MODULE = _delegate_loaded_from or ""
else:
    # Fallback router (minimal + stable schema)
    from fastapi import APIRouter, Header, Query

    DELEGATE_MODULE = _delegate_loaded_from or ",".join(_parse_delegate_modules())

    router = APIRouter(prefix="/v1/argaam", tags=["KSA / Argaam (shim fallback)"])

    def _app_token_required() -> bool:
        return bool((os.getenv("APP_TOKEN") or "").strip())

    def _auth_ok(x_app_token: Optional[str]) -> bool:
        required = (os.getenv("APP_TOKEN") or "").strip()
        if not required:
            return True
        provided = (x_app_token or "").strip()
        return bool(provided) and provided == required

    def _base_payload(symbol: str = "") -> dict:
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
        out = _base_payload("")
        out["module"] = "routes_argaam (repo root shim fallback)"
        out["delegate_loaded"] = False
        out["app_token_required"] = _app_token_required()
        return out

    @router.get("/quote")
    async def quote(
        symbol: str = Query(..., description="KSA symbol (e.g., 1120.SR or 1120)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        debug: int = Query(default=0, description="Set 1 to include debug hints"),
    ) -> dict:
        out = _base_payload(symbol)
        out["delegate_loaded"] = False

        # Optional auth behavior (still HTTP 200)
        if _app_token_required() and not _auth_ok(x_app_token):
            out["error"] = "Unauthorized: invalid or missing X-APP-TOKEN"
            out["auth_ok"] = False
        else:
            out["auth_ok"] = True

        if int(debug or 0) == 1:
            out["debug"] = {
                "token_provided": bool((x_app_token or "").strip()),
                "hint": "Fix delegate module: create routes/routes_argaam.py exporting `router` (or `get_router()`).",
                "delegate_modules_tried": list(_parse_delegate_modules()),
            }
        return out

    @router.get("/fundamentals")
    async def fundamentals(
        symbol: str = Query(..., description="KSA symbol (e.g., 1120.SR or 1120)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        debug: int = Query(default=0),
    ) -> dict:
        # Same stable behavior for clients that call /fundamentals
        out = _base_payload(symbol)
        out["endpoint"] = "fundamentals"
        if _app_token_required() and not _auth_ok(x_app_token):
            out["error"] = "Unauthorized: invalid or missing X-APP-TOKEN"
            out["auth_ok"] = False
        else:
            out["auth_ok"] = True
        if int(debug or 0) == 1:
            out["debug"] = {
                "hint": "Delegate missing; implement routes/routes_argaam.py + core.providers.argaam_provider.",
            }
        return out


__all__ = ["router", "SHIM_VERSION", "DELEGATE_MODULE"]
```
