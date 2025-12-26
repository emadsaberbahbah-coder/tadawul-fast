```python
# routes_argaam.py  (REPO ROOT SHIM)  — FULL REPLACEMENT
"""
routes_argaam.py (repo root shim)
------------------------------------------------------------
Argaam Router Shim – v1.3.0 (PROD SAFE + QUIET BOOT)

Purpose
- Backward compatibility for deployments importing `routes_argaam.router`
  from repo root.
- Prefer the real router: `routes.routes_argaam.router`.
- If delegation fails, expose a minimal fallback router that:
    • never crashes app startup
    • always returns HTTP 200 with status/error fields
    • provides stable keys for Sheets/client code

Rules
- No engine init, no network, no heavy side effects at import time.
- Avoid noisy stacktraces on every worker boot unless DEBUG_ERRORS=1.
"""

from __future__ import annotations

import importlib
import logging
import os
from typing import Any, Optional, Tuple

logger = logging.getLogger("routes.argaam_shim")

SHIM_VERSION = "1.3.0"
DELEGATE_MODULE = "routes.routes_argaam"

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _try_import_delegate() -> Tuple[Optional[Any], Optional[str]]:
    """
    Return (delegate_router, error_message). Never raises.
    """
    try:
        m = importlib.import_module(DELEGATE_MODULE)
        r = getattr(m, "router", None)
        if r is None:
            return None, f"{DELEGATE_MODULE}.router not found"
        return r, None
    except Exception as exc:
        dbg = _truthy(os.getenv("DEBUG_ERRORS", "0"))
        msg = f"Failed to import delegate router from {DELEGATE_MODULE}: {exc.__class__.__name__}: {exc}"
        if dbg:
            logger.exception("[argaam_shim] %s", msg)
        else:
            logger.warning("[argaam_shim] %s (set DEBUG_ERRORS=1 for traceback)", msg)
        return None, msg


_delegate_router, _delegate_error = _try_import_delegate()

# If delegate router exists, export it directly (fast path).
# Otherwise, export a minimal fallback router with stable responses.
if _delegate_router is not None:
    router = _delegate_router  # type: ignore

else:
    # Import FastAPI only for fallback (still no network/heavy work)
    from fastapi import APIRouter, Header, Query

    router = APIRouter(prefix="/v1/argaam", tags=["KSA / Argaam (shim fallback)"])

    def _base_error_payload(symbol: str = "") -> dict:
        return {
            "status": "error",
            "symbol": (symbol or "").strip().upper(),
            "market": "KSA",
            "currency": "SAR",
            "data_quality": "MISSING",
            "data_source": "shim_fallback",
            "error": _delegate_error
            or "Delegate router import failed. Ensure routes/routes_argaam.py exists and exports `router`.",
            "shim_version": SHIM_VERSION,
            "delegate_module": DELEGATE_MODULE,
        }

    @router.get("/health")
    async def _shim_health() -> dict:
        """
        Always 200. Indicates shim is active because delegate import failed.
        """
        out = _base_error_payload("")
        # keep health payload compact + consistent
        out["module"] = "routes_argaam (repo root shim fallback)"
        return out

    @router.get("/quote")
    async def _shim_quote(
        symbol: str = Query(..., description="KSA symbol (e.g., 1120.SR or 1120)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        debug: int = Query(default=0, description="Set 1 to include debug hints"),
    ) -> dict:
        """
        Always 200. Stable schema so Sheets never breaks.
        """
        out = _base_error_payload(symbol)
        if int(debug or 0) == 1:
            out["debug"] = {
                "token_provided": bool((x_app_token or "").strip()),
                "hint": "Delegate router missing. Fix package path: routes/routes_argaam.py exporting `router`.",
            }
        return out


__all__ = ["router", "SHIM_VERSION", "DELEGATE_MODULE"]
```
