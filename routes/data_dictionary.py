#!/usr/bin/env python3
# routes/data_dictionary.py
"""
================================================================================
Schema Data Dictionary Router — v1.0.0
================================================================================
Endpoint:
  GET /v1/schema/data-dictionary

Returns Data_Dictionary as JSON (schema-derived), useful for Apps Script debugging.
"""

from __future__ import annotations

from fastapi import APIRouter

from core.sheets.schema_registry import get_sheet_spec
from core.sheets.data_dictionary import build_data_dictionary_rows

DATA_DICTIONARY_ROUTE_VERSION = "1.0.0"

router = APIRouter(prefix="/v1/schema", tags=["Schema"])

@router.get("/data-dictionary")
def get_data_dictionary():
    spec = get_sheet_spec("Data_Dictionary")
    headers = [c.header for c in spec.columns]
    keys = [c.key for c in spec.columns]
    rows = build_data_dictionary_rows(include_meta_sheet=True)
    return {
        "status": "success",
        "headers": headers,
        "keys": keys,
        "rows": rows,
        "version": DATA_DICTIONARY_ROUTE_VERSION,
    }

__all__ = ["router"]
