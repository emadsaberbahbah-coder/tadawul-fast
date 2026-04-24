#!/usr/bin/env python3
# core/sheets/__init__.py
"""
core.sheets — Sheet contract + page catalog + data dictionary (v1.0.0)

This package holds the authoritative schema definitions used across the
backend and the Google Apps Script frontend:

    - schema_registry  : single source of truth for column contracts
                         (80-col instrument, 83-col Top_10,
                         7-col Insights, 9-col Data_Dictionary)
    - page_catalog     : canonical page names + normalization
    - data_dictionary  : derived reference rows exposed on the
                         Data_Dictionary sheet

This __init__ is intentionally empty of runtime imports so nothing
unexpected runs at boot. Consumers import directly:

    from core.sheets.schema_registry import SCHEMA_REGISTRY, get_sheet_spec
    from core.sheets.page_catalog import normalize_page_name
"""

from __future__ import annotations

__version__ = "1.0.0"
__all__: list[str] = []
