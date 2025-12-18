from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from storage.database import DataHistory


async def append_snapshot(
    session: AsyncSession,
    *,
    page_id: str,
    region: str,
    schema_hash: str,
    payload: Any,
    metadata: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Append-only insert. Returns inserted row id.
    """
    row = DataHistory(
        page_id=page_id,
        region=region,
        schema_hash=schema_hash,
        payload=payload,
        metadata=metadata or {},
    )
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return int(row.id)


async def recent_snapshots(
    session: AsyncSession,
    *,
    page_id: str,
    region: str,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """
    Returns latest snapshots (for diagnostics / future learning).
    """
    q = (
        select(DataHistory)
        .where(DataHistory.page_id == page_id, DataHistory.region == region)
        .order_by(desc(DataHistory.ingested_at))
        .limit(limit)
    )
    res = await session.execute(q)
    rows = res.scalars().all()

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r.id),
                "page_id": r.page_id,
                "region": r.region,
                "ingested_at": r.ingested_at.isoformat(),
                "schema_hash": r.schema_hash,
                "payload": r.payload,
                "metadata": r.metadata,
            }
        )
    return out
