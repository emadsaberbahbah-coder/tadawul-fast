from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from fastapi import APIRouter, Depends
from config import get_settings
from core.security import require_app_token

router = APIRouter(tags=["config"], dependencies=[Depends(require_app_token)])


@router.get("/api/v1/pages")
async def list_pages() -> Dict[str, Any]:
    """
    Lists available dynamic page YAML configs (page_ids).
    Useful for Apps Script to discover what pages exist.
    """
    settings = get_settings()
    pages_dir = Path(settings.dynamic_pages_dir).resolve()

    yaml_files = sorted(list(pages_dir.glob("*.yaml")) + list(pages_dir.glob("*.yml")))
    page_ids: List[str] = [f.stem for f in yaml_files]

    return {
        "ok": True,
        "dynamic_pages_dir": str(pages_dir),
        "count": len(page_ids),
        "page_ids": page_ids,
    }
