# integrations/symbols_reader.py  (FULL REPLACEMENT)
"""
integrations/symbols_reader.py
===========================================================
Compatibility Shim — v0.1.0 (PROD SAFE)

Why this exists
- Some older code paths may try: `from integrations.symbols_reader import ...`
- The canonical implementation in this repo is: `symbols_reader.py` (project root)
- This shim prevents SyntaxError "time bombs" caused by accidental Markdown fences
  and ensures imports never crash app startup.

Behavior
- Re-exports the canonical symbols_reader symbols if available
- If canonical import fails, returns safe error-shaped responses (never raises)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger("integrations.symbols_reader")

SHIM_VERSION = "0.1.0"
CANONICAL_MODULE = "symbols_reader"

_IMPORT_ERROR: Optional[Exception] = None

try:
    # Canonical module (used by scripts/run_dashboard_sync.py etc.)
    from symbols_reader import (  # type: ignore
        PageConfig,
        PAGE_REGISTRY,
        split_tickers_by_market,
        get_symbols_from_sheet,
        get_page_symbols,
        get_all_pages_symbols,
    )

except Exception as e:  # pragma: no cover
    _IMPORT_ERROR = e
    logger.exception("Failed to import canonical symbols_reader: %s", e)

    # Safe fallbacks (never raise)
    PageConfig = object  # type: ignore
    PAGE_REGISTRY: Dict[str, Any] = {}

    def split_tickers_by_market(tickers: List[str]) -> Dict[str, List[str]]:  # type: ignore
        # best-effort: treat .SR numeric as KSA
        ksa: List[str] = []
        glob: List[str] = []
        for t in tickers or []:
            u = (t or "").upper().strip()
            if u.endswith(".SR") and u[:-3].isdigit():
                ksa.append(t)
            else:
                glob.append(t)
        return {"ksa": ksa, "global": glob}

    def _err_meta() -> Dict[str, Any]:
        return {
            "status": "error",
            "shim_version": SHIM_VERSION,
            "canonical": CANONICAL_MODULE,
            "error": f"canonical symbols_reader import failed: {_IMPORT_ERROR}",
        }

    def get_symbols_from_sheet(*args: Any, **kwargs: Any) -> Dict[str, Any]:  # type: ignore
        return {"all": [], "ksa": [], "global": [], "meta": _err_meta()}

    def get_page_symbols(*args: Any, **kwargs: Any) -> Dict[str, Any]:  # type: ignore
        return {"all": [], "ksa": [], "global": [], "meta": _err_meta()}

    def get_all_pages_symbols(*args: Any, **kwargs: Any) -> Dict[str, Any]:  # type: ignore
        return {"meta": _err_meta(), "pages": {}}


__all__ = [
    "SHIM_VERSION",
    "CANONICAL_MODULE",
    "PageConfig",
    "PAGE_REGISTRY",
    "split_tickers_by_market",
    "get_symbols_from_sheet",
    "get_page_symbols",
    "get_all_pages_symbols",
]
