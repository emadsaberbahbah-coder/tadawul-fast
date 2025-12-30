# core/yahoo_chart_provider.py
"""
core/yahoo_chart_provider.py
===========================================================
Compatibility SHIM (PROD SAFE)

Why this exists
- Some modules historically imported: `core.yahoo_chart_provider`
- The real implementation should live in: `core.providers.yahoo_chart_provider`
- This file must NEVER contain Markdown fences or heavy side effects.

Behavior
- Re-exports the real provider if available
- If import fails, returns a safe MISSING payload (never crashes importers)

(v1.0.0) — Repo Hygiene Fix
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger("yahoo_chart_provider_shim")

SHIM_VERSION = "1.0.0"

# Default constant (used only if the real provider cannot be imported)
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
PROVIDER_VERSION = "missing"

_IMPORT_ERROR: Optional[str] = None

try:
    # ✅ Canonical provider location
    from core.providers.yahoo_chart_provider import (  # type: ignore
        yahoo_chart_quote,
        YAHOO_CHART_URL as _REAL_YAHOO_CHART_URL,
        PROVIDER_VERSION as _REAL_PROVIDER_VERSION,
    )

    # Re-export canonical constants
    YAHOO_CHART_URL = _REAL_YAHOO_CHART_URL
    PROVIDER_VERSION = _REAL_PROVIDER_VERSION

except Exception as e:  # pragma: no cover
    _IMPORT_ERROR = f"{type(e).__name__}: {e}"

    async def yahoo_chart_quote(  # type: ignore
        symbol: str,
        *args: Any,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        sym = (symbol or "").strip().upper()
        market = "KSA" if sym.endswith(".SR") else "GLOBAL"
        currency = "SAR" if sym.endswith(".SR") else None
        return {
            "symbol": sym,
            "market": market,
            "currency": currency,
            "data_source": "yahoo_chart",
            "provider_version": PROVIDER_VERSION,
            "data_quality": "MISSING",
            "error": f"yahoo_chart provider import failed: {_IMPORT_ERROR}",
        }


__all__ = ["yahoo_chart_quote", "YAHOO_CHART_URL", "PROVIDER_VERSION", "SHIM_VERSION"]
