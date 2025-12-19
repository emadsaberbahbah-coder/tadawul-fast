from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from core.security import require_app_token
from dynamic.registry import get_registered_page

router = APIRouter(tags=["validation"], dependencies=[Depends(require_app_token)])


class ValidateRequest(BaseModel):
    sample_row: Optional[Dict[str, Any]] = None


@router.post("/api/v1/validate/{page_id}")
async def validate_page(page_id: str, body: ValidateRequest) -> Dict[str, Any]:
    """
    Validates that:
    - YAML exists and is readable
    - Dynamic Pydantic model can be built
    - Optionally validates one sample_row against the model
    """
    try:
        reg = get_registered_page(page_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML/model: {e}")

    model = reg.row_model
    fields = list(model.model_fields.keys())

    sample_result = None
    if body.sample_row is not None:
        try:
            obj = model.model_validate(body.sample_row)
            sample_result = {"ok": True, "validated": obj.model_dump()}
        except Exception as e:
            sample_result = {"ok": False, "error": str(e)}

    return {
        "ok": True,
        "page_id": page_id,
        "region": reg.page.region,
        "schema_hash": reg.page.schema_hash,
        "field_count": len(fields),
        "fields": fields,
        "sample": sample_result,
    }
