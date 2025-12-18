from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter
from config import get_settings

router = APIRouter(tags=["system"])


@router.get("/health")
async def health() -> dict:
    settings = get_settings()
    return {
        "status": "ok",
        "app": settings.app_name,
        "version": settings.version,
        "env": settings.env,
        "time_utc": datetime.now(timezone.utc).isoformat(),
    }
