from __future__ import annotations

from fastapi import Header, HTTPException
from config import get_settings


async def require_app_token(
    x_app_token: str | None = Header(default=None, alias="X-APP-TOKEN"),
) -> None:
    """
    Simple API protection using header: X-APP-TOKEN

    - If APP_TOKEN is not configured on the server -> 503 (service not ready)
    - If missing/invalid token -> 401
    """
    settings = get_settings()

    if not settings.app_token:
        raise HTTPException(
            status_code=503,
            detail="APP_TOKEN is not configured on the server.",
        )

    if not x_app_token or x_app_token != settings.app_token:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing X-APP-TOKEN.",
        )
