from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from core.security import require_app_token
from storage.database import get_session
from storage.repository import recent_snapshots

router = APIRouter(tags=["history"], dependencies=[Depends(require_app_token)])


@router.get("/api/v1/history/recent/{region}/{page_id}")
async def get_recent_history(
    region: str,
    page_id: str,
    limit: int = Query(default=5, ge=1, le=50),
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """
    Debug/verification endpoint:
      GET /api/v1/history/recent/{region}/{page_id}?limit=5

    region must be: ksa | global
    """
    region_norm = (region or "").lower().strip()
    if region_norm not in {"ksa", "global"}:
        raise HTTPException(status_code=400, detail="region must be 'ksa' or 'global'.")

    items: List[Dict[str, Any]] = await recent_snapshots(
        session,
        page_id=page_id,
        region=region_norm,
        limit=int(limit),
    )

    return {
        "ok": True,
        "page_id": page_id,
        "region": region_norm,
        "count": len(items),
        "items": items,
    }
