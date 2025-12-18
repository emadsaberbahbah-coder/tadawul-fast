from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any, Dict

import yaml
from fastapi import APIRouter, Depends, HTTPException

from config import get_settings
from core.security import require_app_token

router = APIRouter(tags=["config"])

_PAGE_ID_RE = re.compile(r"^[a-zA-Z0-9_\-]+$")


def _resolve_yaml_path(pages_dir: Path, page_id: str) -> Path:
    p1 = pages_dir / f"{page_id}.yaml"
    if p1.exists():
        return p1
    p2 = pages_dir / f"{page_id}.yml"
    if p2.exists():
        return p2
    raise FileNotFoundError(f"Page config not found for page_id='{page_id}' in {pages_dir}")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


@router.get("/api/v1/config/{page_id}", dependencies=[Depends(require_app_token)])
async def get_page_config(page_id: str) -> Dict[str, Any]:
    """
    Returns YAML-driven page config (columns, types, enums) for Google Sheets.

    Endpoint:
      GET /api/v1/config/{page_id}
    """
    if not _PAGE_ID_RE.match(page_id):
        raise HTTPException(
            status_code=400,
            detail="Invalid page_id. Allowed: letters, numbers, underscore, dash.",
        )

    settings = get_settings()
    pages_dir = Path(settings.dynamic_pages_dir).resolve()

    try:
        path = _resolve_yaml_path(pages_dir, page_id)
        raw_bytes = path.read_bytes()
        raw = yaml.safe_load(raw_bytes.decode("utf-8")) or {}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load YAML: {e}")

    columns = raw.get("columns") or []
    if not isinstance(columns, list):
        raise HTTPException(status_code=400, detail="YAML 'columns' must be a list.")

    schema_hash = _sha256_bytes(raw_bytes)

    return {
        "page_id": page_id,
        "title": str(raw.get("title") or page_id),
        "region": str(raw.get("region") or "unknown").lower().strip(),
        "schema_hash": schema_hash,
        "source": {
            "path": str(path),
            "mtime_utc": float(path.stat().st_mtime),
        },
        "columns": columns,
    }
