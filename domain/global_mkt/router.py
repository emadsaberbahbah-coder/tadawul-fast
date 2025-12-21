# domain/global_mkt/router.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

from core.security import require_app_token
from domain.global_mkt.eodhd_enricher import enrich_many

router = APIRouter(prefix="/api/v1/global", tags=["global"])


class EnrichRequest(BaseModel):
    symbols: List[str] = Field(default_factory=list)


@router.post("/enrich", dependencies=[Depends(require_app_token)])
def global_enrich(req: EnrichRequest) -> Dict[str, Any]:
    if not req.symbols:
        raise HTTPException(status_code=400, detail="symbols is required")
    rows, errors = enrich_many(req.symbols)
    return {
        "ok": True,
        "count": len(rows),
        "rows": rows,
        "errors": errors,
    }


@router.post("/top7", dependencies=[Depends(require_app_token)])
def global_top7(req: EnrichRequest) -> Dict[str, Any]:
    if not req.symbols:
        raise HTTPException(status_code=400, detail="symbols is required")
    rows, errors = enrich_many(req.symbols)
    rows_sorted = sorted(rows, key=lambda r: (r.get("opportunity_score") or -1e9), reverse=True)
    top = rows_sorted[:7]
    return {
        "ok": True,
        "count": len(top),
        "rows": top,
        "errors": errors,
    }
