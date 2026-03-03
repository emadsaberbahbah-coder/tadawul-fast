#!/usr/bin/env python3
# routes/data_dictionary.py
"""
================================================================================
Schema Data Dictionary Router — v1.1.0 (HARDENED / APPS SCRIPT FRIENDLY)
================================================================================
Endpoint:
  GET /v1/schema/data-dictionary

Returns Data_Dictionary as JSON (schema-derived), useful for Apps Script debugging.

Features:
- Optional auth via core.config.auth_ok (if present)
- Stable metadata: schema_version, count, generated_at_utc
- format=rows (default) or format=values (2D array + header row)
- include_meta_sheet=true|false (default true)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Query

from core.sheets.schema_registry import get_sheet_spec

try:
    from core.sheets.schema_registry import SCHEMA_VERSION  # type: ignore
except Exception:
    SCHEMA_VERSION = "unknown"  # type: ignore

from core.sheets.data_dictionary import build_data_dictionary_rows, build_data_dictionary_values

# Optional auth (safe)
try:
    from core.config import auth_ok  # type: ignore
except Exception:
    auth_ok = None  # type: ignore


DATA_DICTIONARY_ROUTE_VERSION = "1.1.0"

router = APIRouter(prefix="/v1/schema", tags=["Schema"])


def _bool_q(v: Optional[str], default: bool) -> bool:
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


@router.get("/data-dictionary")
def get_data_dictionary(
    format: str = Query(default="rows", description="rows (default) or values (2D array including header row)"),
    include_meta_sheet: Optional[str] = Query(default="true", description="Include Data_Dictionary sheet itself in rows"),
    token: Optional[str] = Query(default=None, description="Optional token (if your deployment enforces auth)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Dict[str, Any]:
    # Optional auth enforcement (only if core.config.auth_ok exists)
    if auth_ok is not None:
        # Prefer Bearer; otherwise X-APP-TOKEN; otherwise token query (auth_ok decides)
        auth_token = (x_app_token or "").strip()
        if authorization and authorization.strip().lower().startswith("bearer "):
            auth_token = authorization.strip().split(" ", 1)[1].strip()
        if token and not auth_token:
            auth_token = token.strip()

        if not auth_ok(token=auth_token, authorization=authorization, headers={"X-APP-TOKEN": x_app_token, "Authorization": authorization}):
            raise HTTPException(status_code=401, detail="Invalid token")

    include_meta = _bool_q(include_meta_sheet, True)
    fmt = (format or "rows").strip().lower()

    spec = get_sheet_spec("Data_Dictionary")
    headers = [c.header for c in spec.columns]
    keys = [c.key for c in spec.columns]

    generated_at_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")

    if fmt == "values":
        values = build_data_dictionary_values(include_header_row=True, include_meta_sheet=include_meta)
        return {
            "status": "success",
            "format": "values",
            "headers": headers,
            "keys": keys,
            "values": values,
            "version": DATA_DICTIONARY_ROUTE_VERSION,
            "schema_version": SCHEMA_VERSION,
            "generated_at_utc": generated_at_utc,
            "count": max(0, len(values) - 1),  # excluding header row
        }

    # default fmt == "rows"
    rows = build_data_dictionary_rows(include_meta_sheet=include_meta)
    return {
        "status": "success",
        "format": "rows",
        "headers": headers,
        "keys": keys,
        "rows": rows,
        "version": DATA_DICTIONARY_ROUTE_VERSION,
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at_utc,
        "count": len(rows),
        "include_meta_sheet": include_meta,
    }


__all__ = ["router"]
