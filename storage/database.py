from __future__ import annotations

from typing import AsyncGenerator

from fastapi import HTTPException


async def init_db() -> None:
    """
    DB-FREE mode:
    No database initialization.
    """
    return


async def get_session() -> AsyncGenerator[None, None]:
    """
    DB-FREE mode:
    If any old route still depends on DB session, fail clearly.
    """
    raise HTTPException(
        status_code=501,
        detail="Database storage is disabled. Using Google Sheets for history instead.",
    )
    yield  # pragma: no cover
