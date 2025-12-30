# integrations/symbols_reader.py
"""
integrations/symbols_reader.py
===========================================================
Compatibility + Repo-Hygiene Shim — v0.1.0 (PROD SAFE)

Why this file exists
- Some older modules may import: `from integrations.symbols_reader import ...`
- The canonical implementation lives in repo root: `symbols_reader.py`
- This shim ensures:
  1) This module is ALWAYS valid Python (no Markdown fences)
  2) Imports remain backward-compatible
  3) No duplicate business logic (single source of truth)

Behavior
- Preferred: re-export symbols from root `symbols_reader.py`
- Fallback: return safe empty outputs with error metadata (never crash import)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger("symbols_reader_shim")

SHIM_VERSION = "0.1.0"

try:
    # ✅ Canonical module (repo root)
    from symbols_reader import (  # type: ignore
        PageConfig,
        PAGE_REGISTRY,
        split_tickers_by_market,
        get_symbols_from_sheet,
        get_page_symbols,
        get_all_pages_symbols,
    )

    # pass-through version (if defined upstream)
    try:
        from symbols_reader import SYMBOLS_READER_VERSION as SYMBOLS_READER_VERSION  # type: ignore
    except Exception:
        SYMBOLS_READER_VERSION = "unknown"

except Exception as e:  # pragma: no cover
    # -------------------------------------------------------------------------
    # Safe fallback: never crash app startup because of optional module
    # -------------------------------------------------------------------------
    PageConfig = object  # type: ignore
    PAGE_REGISTRY = {}  # type: ignore
    SYMBOLS_READER_VERSION = "fallback"

    def split_tickers_by_market(tickers):  # type: ignore
        ksa, glob = [], []
        for t in tickers or []:
            u = str(t or "").upper()
            (ksa if u.endswith(".SR") else glob).append(str(t))
        return {"ksa": ksa, "global": glob}

    def _err(meta_error: str) -> Dict[str, Any]:
        return {"status": "error", "error": meta_error, "shim_version": SHIM_VERSION}

    def get_symbols_from_sheet(  # type: ignore
        spreadsheet_id: str,
        sheet_name: str,
        header_row: int = 5,
        max_cols: int = 52,
        scan_rows: int = 2000,
    ) -> Dict[str, Any]:
        return {"all": [], "ksa": [], "global": [], "meta": _err(f"symbols_reader import failed: {e}")}

    def get_page_symbols(page_key: str, spreadsheet_id: Optional[str] = None) -> Dict[str, Any]:  # type: ignore
        return {"all": [], "ksa": [], "global": [], "meta": _err(f"symbols_reader import failed: {e}")}

    def get_all_pages_symbols(spreadsheet_id: Optional[str] = None) -> Dict[str, Dict[str, Any]]:  # type: ignore
        return {}

__all__ = [
    "PageConfig",
    "PAGE_REGISTRY",
    "SYMBOLS_READER_VERSION",
    "split_tickers_by_market",
    "get_symbols_from_sheet",
    "get_page_symbols",
    "get_all_pages_symbols",
]
