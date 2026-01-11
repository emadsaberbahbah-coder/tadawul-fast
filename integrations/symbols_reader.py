# integrations/symbols_reader.py
"""
integrations/symbols_reader.py
===========================================================
Compatibility + Repo-Hygiene Shim — v0.2.0 (PROD SAFE)

Why this file exists
- Some older modules may import: `from integrations.symbols_reader import ...`
- The canonical implementation lives in repo root: `symbols_reader.py`
- This shim ensures:
  1) This module is ALWAYS valid Python (no markdown fences / no runtime crashes)
  2) Imports remain backward-compatible
  3) No duplicate business logic (single source of truth)

Behavior
- Preferred: re-export symbols from root `symbols_reader.py`
- Fallback: provide safe empty outputs with error metadata (never crash import)

Notes
- Fallback functions return a consistent shape:
    {
      "all": [...],
      "ksa": [...],
      "global": [...],
      "tickers": [...],   # alias of "all"
      "symbols": [...],   # alias of "all"
      "meta": {...}
    }
  This helps downstream code that expects either "tickers" or "symbols".
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Sequence, Tuple

logger = logging.getLogger("integrations.symbols_reader_shim")

SHIM_VERSION = "0.2.0"


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

    # Pass-through version (if defined upstream)
    try:
        from symbols_reader import SYMBOLS_READER_VERSION as SYMBOLS_READER_VERSION  # type: ignore
    except Exception:
        SYMBOLS_READER_VERSION = "unknown"

except Exception as _import_exc:  # pragma: no cover
    # -------------------------------------------------------------------------
    # Safe fallback: never crash app startup because of optional module
    # -------------------------------------------------------------------------
    logger.warning(
        "[integrations.symbols_reader] Fallback active (root symbols_reader import failed): %s",
        _import_exc,
    )

    PageConfig = object  # type: ignore
    PAGE_REGISTRY = {}  # type: ignore
    SYMBOLS_READER_VERSION = "fallback"

    def _meta_err(msg: str) -> Dict[str, Any]:
        return {"status": "error", "error": msg, "shim_version": SHIM_VERSION}

    def _bundle(all_list: Sequence[str], ksa: Sequence[str], glob: Sequence[str], meta: Dict[str, Any]) -> Dict[str, Any]:
        a = list(all_list or [])
        return {
            "all": a,
            "ksa": list(ksa or []),
            "global": list(glob or []),
            # aliases (some callers expect these keys)
            "tickers": a,
            "symbols": a,
            "meta": meta,
        }

    def split_tickers_by_market(tickers: Sequence[str]) -> Tuple[list, list]:  # type: ignore
        """
        Fallback minimal split:
        - KSA: endswith .SR OR numeric 3-6 digits (treated as Tadawul)
        - Global: everything else
        Returns (ksa, global) tuple (to match canonical contract).
        """
        seen_ksa = set()
        seen_glb = set()
        ksa: list = []
        glob: list = []

        for t in (tickers or []):
            s = str(t or "").strip()
            if not s:
                continue
            u = s.upper()

            if u.startswith("TADAWUL:"):
                u = u.split(":", 1)[1].strip().upper()
            if u.endswith(".TADAWUL"):
                u = u[: -len(".TADAWUL")].strip()

            if u.endswith(".SR") or (u.isdigit() and 3 <= len(u) <= 6):
                sym = u if u.endswith(".SR") else f"{u}.SR"
                if sym not in seen_ksa:
                    seen_ksa.add(sym)
                    ksa.append(sym)
            else:
                if u not in seen_glb:
                    seen_glb.add(u)
                    glob.append(u)

        return ksa, glob

    def get_symbols_from_sheet(  # type: ignore
        spreadsheet_id: str,
        sheet_name: str,
        header_row: int = 5,
        max_cols: int = 52,
        scan_rows: int = 2000,
    ) -> Dict[str, Any]:
        meta = _meta_err(f"symbols_reader import failed: {_import_exc}")
        return _bundle([], [], [], meta)

    def get_page_symbols(page_key: str, spreadsheet_id: Optional[str] = None) -> Dict[str, Any]:  # type: ignore
        meta = _meta_err(f"symbols_reader import failed: {_import_exc}")
        return _bundle([], [], [], meta)

    def get_all_pages_symbols(spreadsheet_id: Optional[str] = None) -> Dict[str, Dict[str, Any]]:  # type: ignore
        # Keep shape consistent: dict of page_key -> symbols bundle
        # (No page keys available in fallback)
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
