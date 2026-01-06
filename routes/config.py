# routes/config.py  (FULL REPLACEMENT)
"""
TADAWUL FAST BRIDGE – CONFIG ROUTES (v1.3.0) – PROD SAFE

Purpose
- Provide safe config diagnostics endpoints (health + masked settings).
- Token-aware: if APP_TOKEN is configured, sensitive endpoints require X-APP-TOKEN.
- Never imports heavy engine modules at import-time.

Endpoints
- GET /v1/config/health      -> always open, basic info
- GET /v1/config/settings    -> masked settings (requires token if configured)
- GET /v1/config/env         -> presence-only env diagnostics (requires token if configured)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Request

try:
    # Preferred: your main config module (same folder as main.py)
    from config import get_settings, is_open_mode, mask_settings_dict, auth_ok  # type: ignore
except Exception:  # pragma: no cover
    # Fallback if user renamed/moved it
    def get_settings():  # type: ignore
        return None

    def is_open_mode() -> bool:  # type: ignore
        return True

    def mask_settings_dict() -> Dict[str, Any]:  # type: ignore
        return {"error": "config.get_settings not available"}

    def auth_ok(_x: Optional[str]) -> bool:  # type: ignore
        return True


logger = logging.getLogger("routes.config")

CONFIG_ROUTES_VERSION = "1.3.0"
router = APIRouter(prefix="/v1/config", tags=["Config"])


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_token_if_configured(x_app_token: Optional[str]) -> None:
    """
    If app is in token mode, require a valid X-APP-TOKEN.
    If app is open mode (no tokens configured), allow.
    """
    if is_open_mode():
        return
    if not auth_ok(x_app_token):
        raise HTTPException(status_code=401, detail="Unauthorized (invalid or missing X-APP-TOKEN).")


@router.get("/health")
async def config_health(_request: Request) -> Dict[str, Any]:
    s = None
    try:
        s = get_settings()
    except Exception:
        s = None

    return {
        "status": "ok",
        "module": "routes.config",
        "version": CONFIG_ROUTES_VERSION,
        "token_mode": "open" if is_open_mode() else "token",
        "service_name": getattr(s, "service_name", None) if s else None,
        "environment": getattr(s, "environment", None) if s else None,
        "timestamp_utc": _now_utc_iso(),
    }


@router.get("/settings")
async def config_settings(
    _request: Request,
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    _require_token_if_configured(x_app_token)
    return {
        "status": "ok",
        "token_mode": "open" if is_open_mode() else "token",
        "settings": mask_settings_dict(),
        "timestamp_utc": _now_utc_iso(),
    }


@router.get("/env")
async def config_env(
    _request: Request,
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    """
    Presence-only environment diagnostics (does NOT return secrets).
    """
    _require_token_if_configured(x_app_token)

    s = None
    try:
        s = get_settings()
    except Exception:
        s = None

    def _present(name: str) -> bool:
        try:
            v = getattr(s, name, None) if s else None
            return bool(v)
        except Exception:
            return False

    return {
        "status": "ok",
        "token_mode": "open" if is_open_mode() else "token",
        "presence": {
            # auth
            "app_token_configured": not is_open_mode(),
            "backup_app_token_present": _present("backup_app_token"),
            # google sheets
            "google_sheets_credentials_present": _present("google_sheets_credentials"),
            "google_application_credentials_present": _present("google_application_credentials"),
            "default_spreadsheet_id_present": _present("default_spreadsheet_id"),
            # provider urls (presence only)
            "tadawul_quote_url_present": _present("tadawul_quote_url"),
            "tadawul_fundamentals_url_present": _present("tadawul_fundamentals_url"),
            "argaam_base_url_present": _present("argaam_base_url"),
            "yahoo_enabled": bool(getattr(s, "enable_yahoo", True)) if s else True,
        },
        "timestamp_utc": _now_utc_iso(),
    }


__all__ = ["router"]
