from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from config import get_settings
from dynamic.schema_hash import sha256_file


_PAGE_ID_RE = re.compile(r"^[a-zA-Z0-9_\-]+$")


@dataclass(frozen=True)
class PageConfig:
    page_id: str
    title: str
    region: str  # "ksa" | "global" (Phase-1 strict isolation)
    columns: List[Dict[str, Any]]
    schema_hash: str
    source_path: str
    source_mtime_utc: float
    raw: Dict[str, Any]


def _resolve_yaml_path(pages_dir: Path, page_id: str) -> Optional[Path]:
    p1 = pages_dir / f"{page_id}.yaml"
    if p1.exists():
        return p1
    p2 = pages_dir / f"{page_id}.yml"
    if p2.exists():
        return p2
    return None


def load_page_config(page_id: str) -> PageConfig:
    """
    Loads a YAML config from DYNAMIC_PAGES_DIR/{page_id}.yaml (or .yml)
    """
    if not _PAGE_ID_RE.match(page_id):
        raise ValueError("Invalid page_id. Allowed: letters, numbers, underscore, dash.")

    settings = get_settings()
    pages_dir = Path(settings.dynamic_pages_dir).resolve()

    path = _resolve_yaml_path(pages_dir, page_id)
    if not path:
        raise FileNotFoundError(f"Page config not found for page_id='{page_id}' in {pages_dir}")

    raw_text = path.read_text(encoding="utf-8")
    raw = yaml.safe_load(raw_text) or {}

    title = str(raw.get("title") or page_id)
    region = str(raw.get("region") or "unknown").lower().strip()
    columns = raw.get("columns") or []
    if not isinstance(columns, list):
        raise ValueError("YAML 'columns' must be a list.")

    schema_hash = sha256_file(path)
    stat = path.stat()

    return PageConfig(
        page_id=page_id,
        title=title,
        region=region,
        columns=columns,
        schema_hash=schema_hash,
        source_path=str(path),
        source_mtime_utc=float(stat.st_mtime),
        raw=raw,
    )
