# routes/config.py  (FULL REPLACEMENT)
"""
TADAWUL FAST BRIDGE – CONFIG ROUTES (v1.1.0) – PROD SAFE

- Exposes safe config snapshot (masked) for diagnostics.
- Never returns secrets (tokens / service account JSON).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import APIRouter

router = APIRouter(prefix="/v1/config", tags=["Config"])

try:
    from core.config import CONFIG_VERSION, get_settings, is_open_mode, mask_settings_dict  # type: ignore
except Exception:  # pragma: no cover
    CONFIG_VERSION = "unknown"

    def get_settings():  # type: ignore
        return None

    def is_open_mode() -> bool:  # type: ignore
        return True

    def mask_settings_dict() -> Dict[str, Any]:  # type: ignore
        return {"status": "unknown"}


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@router.get("/health")
def config_health() -> Dict[str, Any]:
    s = get_settings()
    return {
        "status": "ok",
        "module": "routes.config",
        "config_version": CONFIG_VERSION,
        "environment": getattr(s, "environment", None),
        "service_name": getattr(s, "service_name", None),
        "auth": "open" if is_open_mode() else "token",
        "ai_limits": {
            "ai_batch_size": getattr(s, "ai_batch_size", None),
            "ai_batch_timeout_sec": getattr(s, "ai_batch_timeout_sec", None),
            "ai_batch_concurrency": getattr(s, "ai_batch_concurrency", None),
            "ai_max_tickers": getattr(s, "ai_max_tickers", None),
        },
        "timestamp_utc": _now_utc_iso(),
    }


@router.get("/public")
def config_public() -> Dict[str, Any]:
    d = mask_settings_dict()
    d["timestamp_utc"] = _now_utc_iso()
    return d


__all__ = ["router"]
