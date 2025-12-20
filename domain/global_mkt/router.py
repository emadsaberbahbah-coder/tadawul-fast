from __future__ import annotations

import json
from typing import Any, Dict, List

from fastapi import APIRouter, Body, Depends, HTTPException

from core.security import require_app_token
from dynamic.registry import get_registered_page

router = APIRouter(
    prefix="/api/v1/global",
    tags=["global"],
    dependencies=[Depends(require_app_token)],
)


def _decode_if_string(payload: Any) -> Any:
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except Exception:
            return payload
    return payload


def _normalize_rows(payload: Any) -> List[Dict[str, Any]]:
    payload = _decode_if_string(payload)

    if isinstance(payload, list):
        return payload  # type: ignore[return-value]

    if isinstance(payload, dict) and isinstance(payload.get("rows"), list):
        return payload["rows"]  # type: ignore[return-value]

    if isinstance(payload, dict):
        return [payload]  # type: ignore[return-value]

    raise HTTPException(
        status_code=400,
        detail='Invalid body. Send JSON array [..] or {"rows":[..]} or single row object.',
    )


@router.post("/ingest/{page_id}")
async def ingest_global(page_id: str, payload: Any = Body(...)) -> Dict[str, Any]:
    rows = _normalize_rows(payload)

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
        "region": "global",
        "schema_hash": reg.page.schema_hash,
        "accepted_count": len(accepted),
        "rejected_count": len(errors),
        "validated_rows": accepted,
        "errors": errors[:50],
    }
