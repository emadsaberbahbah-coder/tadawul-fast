from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException

from core.security import require_app_token
from dynamic.registry import get_registered_page

router = APIRouter(
    prefix="/api/v1/global",
    tags=["global"],
    dependencies=[Depends(require_app_token)],
)


@router.post("/ingest/{page_id}")
async def ingest_global(page_id: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    DB-FREE Global ingestion:
    - Validates rows using YAML-driven dynamic Pydantic model
    - Does NOT write to Postgres
    - Returns validated_rows so Apps Script can append them to Google Sheets history
    """
    try:
        reg = get_registered_page(page_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid page config: {e}")

    if reg.page.region != "global":
        raise HTTPException(
            status_code=400,
            detail=f"Page '{page_id}' is region='{reg.page.region}', not 'global'.",
        )

    model = reg.row_model
    accepted: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for i, row in enumerate(rows):
        try:
            obj = model.model_validate(row)
            accepted.append(obj.model_dump())
        except Exception as e:
            errors.append({"index": i, "field": "*", "message": str(e)})

    return {
        "ok": True,
        "storage": "google_sheets",
        "page_id": page_id,
        "region": "global",
        "schema_hash": reg.page.schema_hash,
        "accepted_count": len(accepted),
        "rejected_count": len(errors),
        "validated_rows": accepted,   # <-- Apps Script will append this to History sheet
        "errors": errors[:50],
    }
