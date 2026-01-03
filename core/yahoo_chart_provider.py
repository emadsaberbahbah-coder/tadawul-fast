# core/yahoo_chart_provider.py  (FULL REPLACEMENT)
"""
core/yahoo_chart_provider.py
===========================================================
Compatibility + Repo-Hygiene Shim — v0.2.1 (PROD SAFE)

Problem this fixes
- This file previously contained Markdown code fences which makes it INVALID Python.
- Even if the engine uses core.providers.yahoo_chart_provider, an invalid file here is a
  "time bomb" if anything imports core.yahoo_chart_provider later.

Solution
- Keep ONE canonical implementation:
    core/providers/yahoo_chart_provider.py
- This module becomes a small, import-safe re-export shim.

Exports preserved (best-effort)
- YahooChartProvider
- fetch_quote / get_quote / get_quote_patch
- yahoo_chart_quote (alias to get_quote for backward compatibility)
- PROVIDER_VERSION
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger("yahoo_chart_provider_shim")

SHIM_VERSION = "0.2.1"

# Backward-compat constant (not necessarily used by canonical provider)
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v7/finance/quote"

try:
    # Canonical provider
    from core.providers.yahoo_chart_provider import (  # type: ignore
        YahooChartProvider,
        fetch_quote,
        get_quote,
        fetch_quote_patch as get_quote_patch,  # map new name to old name if needed
        PROVIDER_VERSION,
    )

    # Backward compatible alias (older code may call yahoo_chart_quote)
    async def yahoo_chart_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await get_quote(symbol, *args, **kwargs)

except Exception as e:  # pragma: no cover
    # Do not log at import-time by default (quiet boot).
    # If you want verbose diagnostics, enable DEBUG_ERRORS=1 in env and log elsewhere.
    PROVIDER_VERSION = "fallback"

    class YahooChartProvider:  # type: ignore
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._error = f"{e.__class__.__name__}: {e}"

        async def get_quote_patch(
            self, symbol: str, base: Optional[Dict[str, Any]] = None
        ) -> Dict[str, Any]:
            out: Dict[str, Any] = dict(base or {})
            out.update(
                {
                    "status": "error",
                    "symbol": (symbol or "").strip().upper(),
                    "data_source": "yahoo_chart",
                    "data_quality": "MISSING",
                    "error": self._error,
                    "shim_version": SHIM_VERSION,
                }
            )
            return out

    async def fetch_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return {
            "status": "error",
            "symbol": (symbol or "").strip().upper(),
            "data_source": "yahoo_chart",
            "data_quality": "MISSING",
            "error": f"{e.__class__.__name__}: {e}",
            "shim_version": SHIM_VERSION,
        }

    async def get_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await fetch_quote(symbol, *args, **kwargs)

    async def get_quote_patch(
        symbol: str,
        base: Optional[Dict[str, Any]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        out: Dict[str, Any] = dict(base or {})
        out.update(
            {
                "status": "error",
                "symbol": (symbol or "").strip().upper(),
                "data_source": "yahoo_chart",
                "data_quality": "MISSING",
                "error": f"{e.__class__.__name__}: {e}",
                "shim_version": SHIM_VERSION,
            }
        )
        return out

    async def yahoo_chart_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await get_quote(symbol, *args, **kwargs)


__all__ = [
    "YAHOO_CHART_URL",
    "PROVIDER_VERSION",
    "YahooChartProvider",
    "fetch_quote",
    "get_quote",
    "get_quote_patch",
    "yahoo_chart_quote",
]
