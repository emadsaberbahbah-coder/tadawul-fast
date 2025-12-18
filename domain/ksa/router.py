from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from core.security import require_app_token
from storage.database import get_session
from domain.ksa.ingestion import KsaIngestor

router = APIRouter(prefix="/api/v1/ksa", tags=["ksa"], dependencies=[Depends(require_app_token)])


@router.post("/ingest/{page_id}")
async def ingest_ksa(
    page_id: str,
    rows: List[Dict[str, Any]],
    session: AsyncSession = Depends(get_session),
    client_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    POST /api/v1/ksa/ingest/{page_id}
    Body: list of row objects (JSON)

    Validates using YAML-driven dynamic Pydantic model, then saves snapshot to Postgres JSONB.
    """
    ingestor = KsaIngestor()
    try:
        result = await ingestor.ingest(
            page_id=page_id,
            rows=rows,
            session=session,
            client_metadata=client_metadata,
        )
        return {
            "ok": True,
            "result": {
                "page_id": result.page_id,
                "region": result.region,
                "schema_hash": result.schema_hash,
                "accepted_count": result.accepted_count,
                "rejected_count": result.rejected_count,
                "snapshot_ids": result.snapshot_ids,
                "errors": [e.__dict__ for e in result.errors],
            },
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"KSA ingestion failed: {e}")
