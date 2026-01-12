# core/yahoo_chart_provider.py  (FULL REPLACEMENT) — v0.3.2
"""
core/yahoo_chart_provider.py
===========================================================
Compatibility + Repo-Hygiene Shim — v0.3.2 (PROD SAFE)

Why this exists
- The canonical Yahoo Chart provider lives here:
    core/providers/yahoo_chart_provider.py
- This top-level module must remain VALID Python forever (no markdown fences),
  because older imports may still do:
    import core.yahoo_chart_provider

What this shim guarantees
- ✅ Import-safe (never crashes app startup)
- ✅ Re-exports canonical provider symbols when available
- ✅ Provides backward-compatible function names:
    - fetch_quote, get_quote
    - get_quote_patch / fetch_quote_patch
    - yahoo_chart_quote (older code)
    - history function names (best-effort pass-through)
    - aclose_yahoo_chart_client (best-effort)

If canonical import fails
- Returns safe error-shaped dicts (never raises)
"""

from __future__ import annotations

import inspect
import logging
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger("core.yahoo_chart_provider_shim")

SHIM_VERSION = "0.3.2"

# Backward-compat constant (not necessarily used by canonical provider)
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v7/finance/quote"


def _is_awaitable(x: Any) -> bool:
    try:
        return inspect.isawaitable(x)
    except Exception:
        try:
            return hasattr(x, "__await__")
        except Exception:
            return False


async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    out = fn(*args, **kwargs)
    if _is_awaitable(out):
        return await out
    return out


async def _call_with_optional_kw(
    fn: Callable[..., Any],
    *,
    kw_name: str,
    kw_value: Any,
    args: tuple,
    kwargs: dict,
) -> Any:
    """
    Call fn(*args, **kwargs) but with optional kw_name:
      - try with kw
      - if TypeError (unexpected kw), retry without it
    Never raises TypeError outward.
    """
    try:
        k2 = dict(kwargs)
        k2[kw_name] = kw_value
        return await _call_maybe_async(fn, *args, **k2)
    except TypeError:
        return await _call_maybe_async(fn, *args, **kwargs)


def _norm_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper()


def _err_payload(symbol: str, err: str, *, base: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = dict(base or {})
    out.update(
        {
            "status": "error",
            "symbol": _norm_symbol(symbol),
            "data_source": "yahoo_chart",
            "data_quality": "MISSING",
            "error": err,
            "shim_version": SHIM_VERSION,
        }
    )
    return out


def _ensure_dict(symbol: str, r: Any, *, err: str = "unexpected return type") -> Dict[str, Any]:
    if isinstance(r, dict):
        return r
    return _err_payload(symbol, err)


try:
    # Canonical provider module
    import core.providers.yahoo_chart_provider as _canon  # type: ignore

    # Prefer canonical constants if present
    PROVIDER_VERSION = str(getattr(_canon, "PROVIDER_VERSION", "unknown") or "unknown")

    # Provider class (optional; do not force failure if missing)
    YahooChartProvider = getattr(_canon, "YahooChartProvider", None)  # type: ignore

    # -------- Quote helpers (best-effort mapping) --------
    _get_quote = getattr(_canon, "get_quote", None)
    _fetch_quote = getattr(_canon, "fetch_quote", None)

    _fetch_quote_patch = getattr(_canon, "fetch_quote_patch", None)
    _get_quote_patch = getattr(_canon, "get_quote_patch", None)

    _fetch_enriched_quote_patch = getattr(_canon, "fetch_enriched_quote_patch", None)
    _fetch_quote_and_enrichment_patch = getattr(_canon, "fetch_quote_and_enrichment_patch", None)

    # -------- History helpers (optional pass-through) --------
    _fetch_price_history = getattr(_canon, "fetch_price_history", None)
    _fetch_history = getattr(_canon, "fetch_history", None)
    _fetch_ohlc_history = getattr(_canon, "fetch_ohlc_history", None)
    _fetch_history_patch = getattr(_canon, "fetch_history_patch", None)
    _fetch_prices = getattr(_canon, "fetch_prices", None)

    # -------- Client closer (optional) --------
    _aclose = getattr(_canon, "aclose_yahoo_chart_client", None)

    # If canonical provider class is missing, provide a thin adapter that uses patch funcs.
    if YahooChartProvider is None:

        class YahooChartProvider:  # type: ignore
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                pass

            async def get_quote_patch(self, symbol: str, base: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
                return await get_quote_patch(symbol, base)

    async def fetch_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        try:
            if callable(_fetch_quote):
                r = await _call_maybe_async(_fetch_quote, symbol, *args, **kwargs)
                return _ensure_dict(symbol, r)
            if callable(_get_quote):
                r = await _call_maybe_async(_get_quote, symbol, *args, **kwargs)
                return _ensure_dict(symbol, r)
            return _err_payload(symbol, "canonical provider missing get_quote/fetch_quote")
        except Exception as ex:
            return _err_payload(symbol, f"{ex.__class__.__name__}: {ex}")

    async def get_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        # Prefer canonical get_quote, else fallback to fetch_quote
        try:
            if callable(_get_quote):
                r = await _call_maybe_async(_get_quote, symbol, *args, **kwargs)
                return _ensure_dict(symbol, r)
            return await fetch_quote(symbol, *args, **kwargs)
        except Exception as ex:
            return _err_payload(symbol, f"{ex.__class__.__name__}: {ex}")

    async def get_quote_patch(
        symbol: str,
        base: Optional[Dict[str, Any]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Returns a PATCH dict suitable for merging into an existing quote dict.
        When canonical patch exists, prefer it. Else merge full quote into base.
        """
        try:
            if callable(_get_quote_patch):
                r = await _call_maybe_async(_get_quote_patch, symbol, base, *args, **kwargs)
                return _ensure_dict(symbol, r, err="unexpected return type from get_quote_patch")

            if callable(_fetch_quote_patch):
                r = await _call_maybe_async(_fetch_quote_patch, symbol, *args, **kwargs)
                if isinstance(r, dict):
                    out = dict(base or {})
                    out.update(r)
                    return out
                return _err_payload(symbol, "unexpected return type from fetch_quote_patch", base=base)

            # Fallback: merge get_quote (full quote) into base
            q = await get_quote(symbol, *args, **kwargs)
            out = dict(base or {})
            out.update(q if isinstance(q, dict) else {})
            return out
        except Exception as ex:
            return _err_payload(symbol, f"{ex.__class__.__name__}: {ex}", base=base)

    # Engine-friendly alias (many engines call this exact name)
    async def fetch_quote_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """
        Returns a patch dict. Tries passing debug=... if canonical accepts it; otherwise retries without.
        """
        try:
            if callable(_fetch_quote_patch):
                r = await _call_with_optional_kw(
                    _fetch_quote_patch,
                    kw_name="debug",
                    kw_value=debug,
                    args=(symbol,) + tuple(args),
                    kwargs=dict(kwargs),
                )
                return _ensure_dict(symbol, r, err="unexpected return type from fetch_quote_patch")
            return await get_quote_patch(symbol, None, *args, **kwargs)
        except Exception as ex:
            return _err_payload(symbol, f"{ex.__class__.__name__}: {ex}")

    async def fetch_enriched_quote_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        try:
            if callable(_fetch_enriched_quote_patch):
                r = await _call_with_optional_kw(
                    _fetch_enriched_quote_patch,
                    kw_name="debug",
                    kw_value=debug,
                    args=(symbol,) + tuple(args),
                    kwargs=dict(kwargs),
                )
                return _ensure_dict(symbol, r, err="unexpected return type from fetch_enriched_quote_patch")
            return await fetch_quote_patch(symbol, debug=debug, *args, **kwargs)
        except Exception as ex:
            return _err_payload(symbol, f"{ex.__class__.__name__}: {ex}")

    async def fetch_quote_and_enrichment_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        try:
            if callable(_fetch_quote_and_enrichment_patch):
                r = await _call_with_optional_kw(
                    _fetch_quote_and_enrichment_patch,
                    kw_name="debug",
                    kw_value=debug,
                    args=(symbol,) + tuple(args),
                    kwargs=dict(kwargs),
                )
                return _ensure_dict(symbol, r, err="unexpected return type from fetch_quote_and_enrichment_patch")
            return await fetch_quote_patch(symbol, debug=debug, *args, **kwargs)
        except Exception as ex:
            return _err_payload(symbol, f"{ex.__class__.__name__}: {ex}")

    # Backward compatible alias (older code may call yahoo_chart_quote)
    async def yahoo_chart_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await get_quote(symbol, *args, **kwargs)

    # -------- History pass-throughs (optional) --------
    async def fetch_price_history(symbol: str, *args: Any, **kwargs: Any) -> Any:
        try:
            if callable(_fetch_price_history):
                return await _call_maybe_async(_fetch_price_history, symbol, *args, **kwargs)
            if callable(_fetch_history):
                return await _call_maybe_async(_fetch_history, symbol, *args, **kwargs)
            if callable(_fetch_ohlc_history):
                return await _call_maybe_async(_fetch_ohlc_history, symbol, *args, **kwargs)
            if callable(_fetch_history_patch):
                return await _call_maybe_async(_fetch_history_patch, symbol, *args, **kwargs)
            if callable(_fetch_prices):
                return await _call_maybe_async(_fetch_prices, symbol, *args, **kwargs)
            return {}
        except Exception:
            return {}

    async def fetch_history(symbol: str, *args: Any, **kwargs: Any) -> Any:
        return await fetch_price_history(symbol, *args, **kwargs)

    async def fetch_ohlc_history(symbol: str, *args: Any, **kwargs: Any) -> Any:
        return await fetch_price_history(symbol, *args, **kwargs)

    async def fetch_history_patch(symbol: str, *args: Any, **kwargs: Any) -> Any:
        return await fetch_price_history(symbol, *args, **kwargs)

    async def fetch_prices(symbol: str, *args: Any, **kwargs: Any) -> Any:
        return await fetch_price_history(symbol, *args, **kwargs)

    async def aclose_yahoo_chart_client() -> None:
        if callable(_aclose):
            try:
                await _call_maybe_async(_aclose)
            except Exception:
                pass

except Exception as _import_exc:  # pragma: no cover
    # Quiet boot: do not log at import-time
    _IMPORT_ERROR = f"{_import_exc.__class__.__name__}: {_import_exc}"
    PROVIDER_VERSION = "fallback"

    class YahooChartProvider:  # type: ignore
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._error = _IMPORT_ERROR

        async def get_quote_patch(self, symbol: str, base: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
            return _err_payload(symbol, self._error, base=base)

    async def fetch_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _err_payload(symbol, _IMPORT_ERROR)

    async def get_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await fetch_quote(symbol, *args, **kwargs)

    async def get_quote_patch(
        symbol: str,
        base: Optional[Dict[str, Any]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        return _err_payload(symbol, _IMPORT_ERROR, base=base)

    async def fetch_quote_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _err_payload(symbol, _IMPORT_ERROR)

    async def fetch_enriched_quote_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _err_payload(symbol, _IMPORT_ERROR)

    async def fetch_quote_and_enrichment_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _err_payload(symbol, _IMPORT_ERROR)

    async def yahoo_chart_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await get_quote(symbol, *args, **kwargs)

    # History fallbacks
    async def fetch_price_history(symbol: str, *args: Any, **kwargs: Any) -> Any:
        return {}

    async def fetch_history(symbol: str, *args: Any, **kwargs: Any) -> Any:
        return {}

    async def fetch_ohlc_history(symbol: str, *args: Any, **kwargs: Any) -> Any:
        return {}

    async def fetch_history_patch(symbol: str, *args: Any, **kwargs: Any) -> Any:
        return {}

    async def fetch_prices(symbol: str, *args: Any, **kwargs: Any) -> Any:
        return {}

    async def aclose_yahoo_chart_client() -> None:
        return None


__all__ = [
    "YAHOO_CHART_URL",
    "PROVIDER_VERSION",
    "YahooChartProvider",
    # Quote API
    "fetch_quote",
    "get_quote",
    "get_quote_patch",
    "fetch_quote_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "yahoo_chart_quote",
    # History API (best-effort)
    "fetch_price_history",
    "fetch_history",
    "fetch_ohlc_history",
    "fetch_history_patch",
    "fetch_prices",
    # Client close
    "aclose_yahoo_chart_client",
]
