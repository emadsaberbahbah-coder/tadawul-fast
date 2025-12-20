from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Body, Depends, HTTPException

from core.security import require_app_token
from dynamic.registry import get_registered_page

router = APIRouter(
    prefix="/api/v1/ksa",
    tags=["ksa"],
    dependencies=[Depends(require_app_token)],
)


@router.post("/ingest/{page_id}")
async def ingest_ksa(page_id: str, payload: Any = Body(...)) -> Dict[str, Any]:
    """
    Accept ANY JSON body to avoid 422:
      - [ {...}, {...} ]
      - { "rows": [ {...}, {...} ] }
    """
    # Normalize payload -> rows
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict) and isinstance(payload.get("rows"), list):
        rows = payload["rows"]
    else:
        raise HTTPException(
            status_code=400,
            detail="Invalid body. Send either a JSON array of rows, or {\"rows\": [...]}",
        )

    try:
        reg = get_registered_page(page_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid page config: {e}")

    if reg.page.region != "ksa":
        raise HTTPException(status_code=400, detail=f"Page '{page_id}' is region='{reg.page.region}', not 'ksa'.")

    model = reg.row_model
    accepted: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            errors.append({"index": i, "field": "*", "message": "Row must be an object/dict"})
            continue
        try:
            obj = model.model_validate(row)
            accepted.append(obj.model_dump())
        except Exception as e:
            errors.append({"index": i, "field": "*", "message": str(e)})

    return {
        "ok": True,
        "storage": "google_sheets",
        "page_id": page_id,
        "region": "ksa",
        "schema_hash": reg.page.schema_hash,
        "accepted_count": len(accepted),
        "rejected_count": len(errors),
        "validated_rows": accepted,
        "errors": errors[:50],
    }
