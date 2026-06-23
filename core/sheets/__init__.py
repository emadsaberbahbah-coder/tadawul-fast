#!/usr/bin/env python3
# core/sheets/__init__.py
"""
core.sheets — Sheet contract + page catalog + data dictionary (v1.0.1)

This package holds the authoritative schema definitions used across the
backend and the Google Apps Script frontend:

    - schema_registry  : single source of truth for column contracts
                         (115-col instrument market pages, 122-col
                         My_Portfolio, 118-col Top_10_Investments,
                         7-col Insights_Analysis, 9-col Data_Dictionary)
    - page_catalog     : canonical page names + normalization
    - data_dictionary  : derived reference rows exposed on the
                         Data_Dictionary sheet

The column counts above are descriptive only — schema_registry
(SCHEMA_REGISTRY / get_sheet_spec) remains the single live authority. If
the contract ever changes there, treat that as truth and refresh this
note; do not infer widths from this docstring.

This __init__ is intentionally empty of runtime imports so nothing
unexpected runs at boot. Consumers import directly:

    from core.sheets.schema_registry import SCHEMA_REGISTRY, get_sheet_spec
    from core.sheets.page_catalog import normalize_page_name

--------------------------------------------------------------------------
CHANGELOG
--------------------------------------------------------------------------
v1.0.1
    - DOC: corrected the column-count summary to match schema_registry
      v2.15.0. The prior note read "80-col instrument, 83-col Top_10",
      which described an earlier schema generation. The live contract is
      115-col instrument market pages (Market_Leaders / Global_Markets /
      Commodities_FX / Mutual_Funds), 122-col My_Portfolio, and 118-col
      Top_10_Investments; the 7-col Insights_Analysis and 9-col
      Data_Dictionary counts were already correct and are unchanged.
      Docstring-only change — no runtime import, behavior, or public API
      impact (__version__ string bumped; __all__ unchanged).
v1.0.0
    - Initial package initializer (empty-by-design; lazy consumer imports
      so a broken submodule never blocks boot).
"""

from __future__ import annotations

__version__ = "1.0.1"
__all__: list[str] = []
