# domain/global_mkt/router.py
from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel, Field, create_model

from config import get_settings
from core.security import require_app_token

router = APIRouter(prefix="/api/v1/global", tags=["global"])

# ---------------------------------------------------------------------
# In-memory latest cache (needed for /top7 even if storage is Sheets)
# ---------------------------------------------------------------------
_MEM_LATEST: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}  # key: (region, page_id)

# Simple model cache to avoid rebuilding on every request
_MODEL_CACHE: Dict[Tuple[str, str], Any] = {}  # key: (page_id, schema_hash) -> pydantic model


# ---------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------
class IngestResponse(BaseModel):
    ok: bool = True
    storage: str
    page_id: str
    region: str
    schema_hash: str
    accepted_count: int
    rejected_count: int
    validated_rows: List[Dict[str, Any]]
    errors: List[Dict[str, Any]]


class Top7Response(BaseModel):
    ok: bool = True
    page_id: str
    region: str
    count_source_rows: int
    returned: int
    top: List[Dict[str, Any]]


# ---------------------------------------------------------------------
# Helpers: YAML load + schema hash + dynamic Pydantic model
# ---------------------------------------------------------------------
def _yaml_path(page_id: str) -> str:
    settings = get_settings()
    base_dir = getattr(settings, "dynamic_pages_dir", "config/dynamic_pages")
    return os.path.join(base_dir, f"{page_id}.yaml")


def _read_yaml_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _schema_hash_from_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _load_yaml_config(page_id: str) -> Tuple[Dict[str, Any], str, str]:
    """
    Returns: (config_dict, schema_hash, path)
    """
    try:
        import yaml  # PyYAML
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"PyYAML not available: {e}")

    path = _yaml_path(page_id)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"YAML not found for page_id={page_id}: {path}")

    text = _read_yaml_text(path)
    schema_hash = _schema_hash_from_text(text)

    try:
        cfg = yaml.safe_load(text) or {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Invalid YAML for {page_id}: {e}")

    return cfg, schema_hash, path


def _type_for(col: Dict[str, Any]) -> Any:
    t = str(col.get("type", "string")).lower()

    if t in ("string", "text"):
        return str
    if t in ("number", "float", "decimal"):
        return float
    if t in ("int", "integer"):
        return int
    if t in ("bool", "boolean"):
        return bool
    if t in ("datetime",):
        return datetime
    if t in ("date",):
        # keep date as string in Phase-1 unless you have strict date parsing
        return str

    # default fallback
    return str


def _enum_for(page_id: str, col_name: str, values: List[Any]) -> Any:
    # Make safe enum keys
    members = {}
    for v in values:
        key = str(v).strip().upper()
        key = "".join(ch if ch.isalnum() else "_" for ch in key)
        if not key:
            key = "VALUE"
        if key[0].isdigit():
            key = "_" + key
        # Avoid duplicates
        base = key
        i = 2
        while key in members:
            key = f"{base}_{i}"
            i += 1
        members[key] = v

    return Enum(f"Enum_{page_id}_{col_name}", members)  # type: ignore


def _build_model_from_cfg(page_id: str, cfg: Dict[str, Any], schema_hash: str):
    cache_key = (page_id, schema_hash)
    if cache_key in _MODEL_CACHE:
        return _MODEL_CACHE[cache_key]

    cols = cfg.get("columns") or []
    if not isinstance(cols, list) or not cols:
        raise HTTPException(status_code=500, detail=f"YAML columns missing/empty for {page_id}")

    fields: Dict[str, Any] = {}

    for c in cols:
        if not isinstance(c, dict):
            continue

        name = str(c.get("name") or "").strip()
        if not name:
            continue

        required = bool(c.get("required", False))
        max_length = c.get("max_length")
        ge = c.get("ge")
        le = c.get("le")
        enum_vals = c.get("enum")

        base_type = _type_for(c)
        field_type = base_type

        # Enum support
        if isinstance(enum_vals, list) and enum_vals:
            field_type = _enum_for(page_id, name, enum_vals)

        default = ... if required else None

        # Pydantic constraints
        field_kwargs: Dict[str, Any] = {}
        if isinstance(max_length, int) and max_length > 0:
            field_kwargs["max_length"] = max_length
        if ge is not None:
            field_kwargs["ge"] = ge
        if le is not None:
            field_kwargs["le"] = le

        fields[name] = (Optional[field_type] if not required else field_type, Field(default, **field_kwargs))

    Model = create_model(f"Row_{page_id}_{schema_hash[:8]}", **fields)  # type: ignore
    _MODEL_CACHE[cache_key] = Model
    return Model


def _get_model(page_id: str) -> Tuple[Any, str, Dict[str, Any]]:
    """
    Uses dynamic registry if present, otherwise builds from YAML directly.
    Returns: (Model, schema_hash, cfg)
    """
    # Prefer your dynamic registry if it exists
    try:
        from dynamic.registry import get_model_for_page  # type: ignore

        Model, schema_hash, cfg = get_model_for_page(page_id)  # expected signature
        return Model, schema_hash, cfg
    except Exception:
        pass

    # Fallback: load YAML and build locally
    cfg, schema_hash, _path = _load_yaml_config(page_id)
    Model = _build_model_from_cfg(page_id, cfg, schema_hash)
    return Model, schema_hash, cfg


def _parse_rows(body: Any) -> List[Dict[str, Any]]:
    if isinstance(body, list):
        if all(isinstance(x, dict) for x in body):
            return body
        raise HTTPException(status_code=422, detail="Invalid body. Rows must be objects.")
    if isinstance(body, dict):
        rows = body.get("rows")
        if isinstance(rows, list) and all(isinstance(x, dict) for x in rows):
            return rows
    raise HTTPException(status_code=422, detail='Invalid body. Send either a JSON array of rows, or {"rows": [...]}')  # exact msg


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _append_snapshot_safe(page_id: str, region: str, schema_hash: str, rows: List[Dict[str, Any]]) -> str:
    """
    Always store in memory for /top7, and also write to repository if available.
    """
    # ✅ Always keep latest in memory (so /top7 works immediately)
    _MEM_LATEST[(region, page_id)] = rows

    # Try project repository (google sheets / postgres / etc.)
    try:
        from storage.repository import append_snapshot  # type: ignore

        res = append_snapshot(
            page_id=page_id,
            region=region,
            schema_hash=schema_hash,
            payload=rows,
            metadata={"ingested_at": _now_utc_iso()},
        )
        # If repository returns a storage name, pass it through
        if isinstance(res, str) and res.strip():
            return res.strip()
        return "repository"
    except Exception:
        return "memory"


def _score_row(r: Dict[str, Any]) -> float:
    """
    Ranking score for Top7.
    - Prefer opportunity_score if present.
    - Otherwise compute from value/quality/momentum minus risk,
      and add upside_to_target_percent if available.
    """
    def f(x) -> Optional[float]:
        try:
            if x is None:
                return None
            return float(x)
        except Exception:
            return None

    opp = f(r.get("opportunity_score"))
    if opp is not None:
        return opp

    value = f(r.get("value_score")) or 0.0
    quality = f(r.get("quality_score")) or 0.0
    momentum = f(r.get("momentum_score")) or 0.0
    risk = f(r.get("risk_score")) or 0.0

    upside = f(r.get("upside_to_target_percent")) or 0.0
    exp30 = f(r.get("expected_roi_30d")) or 0.0
    exp90 = f(r.get("expected_roi_90d")) or 0.0

    # Simple balanced formula (Phase-1)
    return (0.28 * value) + (0.28 * quality) + (0.24 * momentum) + (0.10 * upside) + (0.10 * exp30) + (0.10 * exp90) - (0.30 * risk)


# ---------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------
@router.post("/ingest/{page_id}", response_model=IngestResponse, dependencies=[Depends(require_app_token)])
def ingest_global(page_id: str, body: Any = Body(...)) -> IngestResponse:
    Model, schema_hash, cfg = _get_model(page_id)

    # Enforce region isolation (global only)
    region_in_yaml = str((cfg or {}).get("region") or "global").lower()
    if region_in_yaml not in ("global", "global_mkt", "world"):
        raise HTTPException(status_code=400, detail=f"Page {page_id} is not global region (yaml region={region_in_yaml}).")

    rows = _parse_rows(body)

    validated: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for idx, row in enumerate(rows):
        try:
            obj = Model.model_validate(row)  # pydantic v2
            validated.append(obj.model_dump())
        except Exception as e:
            errors.append({"index": idx, "error": str(e)})

    storage = _append_snapshot_safe(page_id=page_id, region="global", schema_hash=schema_hash, rows=validated)

    return IngestResponse(
        ok=True,
        storage=storage,
        page_id=page_id,
        region="global",
        schema_hash=schema_hash,
        accepted_count=len(validated),
        rejected_count=len(errors),
        validated_rows=validated,
        errors=errors,
    )


@router.get("/top7", response_model=Top7Response, dependencies=[Depends(require_app_token)])
def top7_global(
    page_id: str = Query(..., description="Dynamic page id, e.g. page_02_market_summary_global"),
    limit: int = Query(7, ge=1, le=50),
) -> Top7Response:
    rows = _MEM_LATEST.get(("global", page_id), []) or []

    ranked = sorted(rows, key=_score_row, reverse=True)[: int(limit)]

    return Top7Response(
        ok=True,
        page_id=page_id,
        region="global",
        count_source_rows=len(rows),
        returned=len(ranked),
        top=ranked,
    )
