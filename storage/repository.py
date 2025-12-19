from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import HTTPException


async def append_snapshot(
    *_,
    **__,
) -> int:
    raise HTTPException(
        status_code=501,
        detail="Database storage is disabled. Use Google Sheets history (Apps Script) instead.",
    )


async def recent_snapshots(
    *_,
    **__,
) -> List[Dict[str, Any]]:
    raise HTTPException(
        status_code=501,
        detail="Database storage is disabled. History is stored in Google Sheets instead.",
    )
