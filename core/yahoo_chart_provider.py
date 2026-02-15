# core/yahoo_chart_provider.py  (FULL REPLACEMENT) — v0.4.0
"""
core/yahoo_chart_provider.py
===========================================================
Compatibility + Repo-Hygiene Shim — v0.4.0 (PROD SAFE + HARDENED)

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
    - fetch_enriched_quote_patch
    - fetch_quote_and_enrichment_patch
    - fetch_quote_and_fundamentals_patch (alias if missing)
    - yahoo_chart_quote (older code)
    - history function names (best-effort pass-through)
    - aclose_yahoo_chart_client (best-effort)
- ✅ Adds minimal normalization to returned dicts:
    - ensures symbol + status + data_source/data_quality when missing
- ✅ Never raises to caller (always returns safe dict or empty history)

If canonical import fails
- Returns safe error-shaped dicts (never raises)

Notes
- This shim does NOT do any network calls directly.
- It only delegates to the canonical provider if it exists.
"""

from __future__ import annotations

import inspect
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Tuple

logger = logging.getLogger("core.yahoo_chart_provider_shim")

SHIM_VERSION = "0.4.0"

# Backward-compat constant (not necessarily used by canonical provider)
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v7/finance/quote"

# Canonical-ish provenance label used across this repo
_DATA_SOURCE = "yahoo_chart"

# Conservative default qualities (matches your app convention)
_DQ_OK = "OK"
_DQ_PARTIAL = "PARTIAL"
_DQ_MISSING = "MISSING"


# -----------------------------------------------------------------------------
# Utilities (import-safe)
# -----------------------------------------------------------------------------
def _now_utc_iso() -> str:
    try:
        return datetime.now(timezone.utc).isoformat()
    except Exception:
        return ""


def _norm_symbol(symbol: Any) -> str:
    try:
        return (str(symbol) if symbol is not None else "").strip().upper()
    except Exception:
        return ""


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


def _safe_sig(fn: Any) -> Optional[inspect.Signature]:
    try:
        return inspect.signature(fn)
    except Exception:
        return None


def _supports_kw(fn: Any, kw: str) -> bool:
    """
    Best-effort: true if function signature accepts kw or has **kwargs.
    If signature cannot be inspected, returns False (we then avoid injecting).
    """
    sig = _safe_sig(fn)
    if sig is None:
        return False
    try:
        for p in sig.parameters.values():
            if p.kind == p.VAR_KEYWORD:
                return True
        return kw in sig.parameters
    except Exception:
        return False


async def _call_with_optional_kws(
    fn: Callable[..., Any],
    *args: Any,
    _optional_kws: Tuple[Tuple[str, Any], ...] = (),
    **kwargs: Any,
) -> Any:
    """
    Call fn(*args, **kwargs) while injecting optional kws ONLY if supported by signature.
    Never raises TypeError for unexpected kws (we avoid injecting those).
    If signature can't be inspected, we don't inject and call directly.
    """
    try:
        k2 = dict(kwargs)
        for k, v in _optional_kws:
            if _supports_kw(fn, k):
                k2[k] = v
        return await _call_maybe_async(fn, *args, **k2)
    except Exception:
        # Let outer layer convert to error payload
        raise


def _err_payload(
    symbol: Any,
    err: str,
    *,
    base: Optional[Dict[str, Any]] = None,
    where: str = "",
) -> Dict[str, Any]:
    out: Dict[str, Any] = dict(base or {})
    out.update(
        {
            "status": "error",
            "symbol": _norm_symbol(symbol),
            "data_source": _DATA_SOURCE,
            "data_quality": _DQ_MISSING,
            "error": (str(err) if err else "unknown error"),
            "where": where or "yahoo_chart_provider_shim",
            "shim_version": SHIM_VERSION,
            "last_updated_utc": out.get("last_updated_utc") or _now_utc_iso(),
        }
    )
    return out


def _ensure_quote_shape(symbol: Any, r: Any) -> Dict[str, Any]:
    """
    Ensure a dict quote-like output with minimal fields.
    We do not overwrite canonical provider fields if already present.
    """
    if not isinstance(r, dict):
        return _err_payload(symbol, "unexpected return type (not dict)", where="ensure_quote_shape")

    out = dict(r)
    out.setdefault("symbol", _norm_symbol(out.get("symbol") or symbol))
    out.setdefault("status", "ok")
    out.setdefault("data_source", _DATA_SOURCE)
    out.setdefault("data_quality", _DQ_OK if out.get("error") in (None, "", False) else _DQ_PARTIAL)
    out.setdefault("shim_version", SHIM_VERSION)

    # Many routes rely on these fields existing; keep canonical values if set.
    out.setdefault("last_updated_utc", out.get("as_of_utc") or out.get("quote_time_utc") or _now_utc_iso())

    return out


def _merge_patch(base: Optional[Dict[str, Any]], patch: Any, symbol: Any, *, where: str) -> Dict[str, Any]:
    """
    Always returns a dict safe to merge into a quote.
    """
    b = dict(base or {})
    if isinstance(patch, dict):
        b.update(patch)
        return b
    return _err_payload(symbol, "unexpected patch type (not dict)", base=b, where=where)


# -----------------------------------------------------------------------------
# Canonical import (must never crash import-time)
# -----------------------------------------------------------------------------
try:
    import core.providers.yahoo_chart_provider as _canon  # type: ignore

    PROVIDER_VERSION = str(getattr(_canon, "PROVIDER_VERSION", "unknown") or "unknown")
    YahooChartProvider = getattr(_canon, "YahooChartProvider", None)  # type: ignore

    # Quote APIs (optional, best-effort)
    _get_quote = getattr(_canon, "get_quote", None)
    _fetch_quote = getattr(_canon, "fetch_quote", None)

    _fetch_quote_patch = getattr(_canon, "fetch_quote_patch", None)
    _get_quote_patch = getattr(_canon, "get_quote_patch", None)

    _fetch_enriched_quote_patch = getattr(_canon, "fetch_enriched_quote_patch", None)
    _fetch_quote_and_enrichment_patch = getattr(_canon, "fetch_quote_and_enrichment_patch", None)

    # "fundamentals" sometimes existed in older branches; alias to enriched if absent
    _fetch_quote_and_fundamentals_patch = getattr(_canon, "fetch_quote_and_fundamentals_patch", None) or _fetch_enriched_quote_patch

    # History APIs (optional)
    _fetch_price_history = getattr(_canon, "fetch_price_history", None)
    _fetch_history = getattr(_canon, "fetch_history", None)
    _fetch_ohlc_history = getattr(_canon, "fetch_ohlc_history", None)
    _fetch_history_patch = getattr(_canon, "fetch_history_patch", None)
    _fetch_prices = getattr(_canon, "fetch_prices", None)

    # Client close (optional)
    _aclose = getattr(_canon, "aclose_yahoo_client", None) or getattr(_canon, "aclose_yahoo_chart_client", None)

    # If canonical class missing, provide thin adapter
    if YahooChartProvider is None:

        class YahooChartProvider:  # type: ignore
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                pass

            async def get_quote_patch(self, symbol: str, base: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
                return await get_quote_patch(symbol, base)

            async def fetch_quote(self, symbol: str, debug: bool = False) -> Dict[str, Any]:
                return await fetch_quote(symbol, debug=debug)

    async def fetch_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """
        Backward-compatible quote fetcher.
        Prefers canonical fetch_quote, then get_quote.
        """
        try:
            if callable(_fetch_quote):
                r = await _call_maybe_async(_fetch_quote, symbol, *args, **kwargs)
                return _ensure_quote_shape(symbol, r)
            if callable(_get_quote):
                r = await _call_maybe_async(_get_quote, symbol, *args, **kwargs)
                return _ensure_quote_shape(symbol, r)
            return _err_payload(symbol, "canonical provider missing get_quote/fetch_quote", where="fetch_quote")
        except Exception as ex:
            return _err_payload(symbol, f"{ex.__class__.__name__}: {ex}", where="fetch_quote")

    async def get_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """
        Backward-compatible quote getter.
        Prefers canonical get_quote, else falls back to fetch_quote.
        """
        try:
            if callable(_get_quote):
                r = await _call_maybe_async(_get_quote, symbol, *args, **kwargs)
                return _ensure_quote_shape(symbol, r)
            return await fetch_quote(symbol, *args, **kwargs)
        except Exception as ex:
            return _err_payload(symbol, f"{ex.__class__.__name__}: {ex}", where="get_quote")

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
                if isinstance(r, dict):
                    # canonical might already return merged; we don't force merge
                    return dict(r)
                return _err_payload(symbol, "unexpected return type from get_quote_patch", base=base, where="get_quote_patch")

            if callable(_fetch_quote_patch):
                r = await _call_maybe_async(_fetch_quote_patch, symbol, *args, **kwargs)
                return _merge_patch(base, r, symbol, where="get_quote_patch:fetch_quote_patch")

            # Fallback: merge full quote into base
            q = await get_quote(symbol, *args, **kwargs)
            return _merge_patch(base, q, symbol, where="get_quote_patch:fallback_merge_quote")
        except Exception as ex:
            return _err_payload(symbol, f"{ex.__class__.__name__}: {ex}", base=base, where="get_quote_patch")

    async def fetch_quote_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """
        Engine-friendly alias (many engines call this exact name).
        Tries passing debug=... ONLY if canonical accepts it; otherwise calls without it.
        """
        try:
            if callable(_fetch_quote_patch):
                r = await _call_with_optional_kws(
                    _fetch_quote_patch,
                    symbol,
                    *args,
                    _optional_kws=(("debug", debug),),
                    **kwargs,
                )
                if isinstance(r, dict):
                    return dict(r)
                return _err_payload(symbol, "unexpected return type from fetch_quote_patch", where="fetch_quote_patch")
            return await get_quote_patch(symbol, None, *args, **kwargs)
        except Exception as ex:
            return _err_payload(symbol, f"{ex.__class__.__name__}: {ex}", where="fetch_quote_patch")

    async def fetch_enriched_quote_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """
        Best-effort: prefer canonical enriched patch, else fall back to quote patch.
        """
        try:
            if callable(_fetch_enriched_quote_patch):
                r = await _call_with_optional_kws(
                    _fetch_enriched_quote_patch,
                    symbol,
                    *args,
                    _optional_kws=(("debug", debug),),
                    **kwargs,
                )
                if isinstance(r, dict):
                    return dict(r)
                return _err_payload(symbol, "unexpected return type from fetch_enriched_quote_patch", where="fetch_enriched_quote_patch")
            return await fetch_quote_patch(symbol, debug=debug, *args, **kwargs)
        except Exception as ex:
            return _err_payload(symbol, f"{ex.__class__.__name__}: {ex}", where="fetch_enriched_quote_patch")

    async def fetch_quote_and_enrichment_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """
        Some codepaths expect a combined patch.
        Prefer canonical, else fall back to quote patch.
        """
        try:
            if callable(_fetch_quote_and_enrichment_patch):
                r = await _call_with_optional_kws(
                    _fetch_quote_and_enrichment_patch,
                    symbol,
                    *args,
                    _optional_kws=(("debug", debug),),
                    **kwargs,
                )
                if isinstance(r, dict):
                    return dict(r)
                return _err_payload(symbol, "unexpected return type from fetch_quote_and_enrichment_patch", where="fetch_quote_and_enrichment_patch")
            return await fetch_quote_patch(symbol, debug=debug, *args, **kwargs)
        except Exception as ex:
            return _err_payload(symbol, f"{ex.__class__.__name__}: {ex}", where="fetch_quote_and_enrichment_patch")

    async def fetch_quote_and_fundamentals_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """
        Older naming: 'fundamentals'. If canonical doesn't implement it, we alias to enriched patch.
        """
        try:
            if callable(_fetch_quote_and_fundamentals_patch):
                r = await _call_with_optional_kws(
                    _fetch_quote_and_fundamentals_patch,
                    symbol,
                    *args,
                    _optional_kws=(("debug", debug),),
                    **kwargs,
                )
                if isinstance(r, dict):
                    return dict(r)
                return _err_payload(symbol, "unexpected return type from fetch_quote_and_fundamentals_patch", where="fetch_quote_and_fundamentals_patch")
            return await fetch_enriched_quote_patch(symbol, debug=debug, *args, **kwargs)
        except Exception as ex:
            return _err_payload(symbol, f"{ex.__class__.__name__}: {ex}", where="fetch_quote_and_fundamentals_patch")

    # Backward compatible alias (older code may call yahoo_chart_quote)
    async def yahoo_chart_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await get_quote(symbol, *args, **kwargs)

    # ---------------- History pass-throughs ----------------
    async def fetch_price_history(symbol: str, *args: Any, **kwargs: Any) -> Any:
        """
        Best-effort history delegate. Returns {} on failure.
        """
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

    # ---------------- Client close ----------------
    async def aclose_yahoo_chart_client() -> None:
        if callable(_aclose):
            try:
                await _call_maybe_async(_aclose)
            except Exception:
                pass

    # Extra backward alias (some older modules used this name)
    async def aclose_yahoo_client() -> None:
        return await aclose_yahoo_chart_client()

except Exception as _import_exc:  # pragma: no cover
    _IMPORT_ERROR = f"{_import_exc.__class__.__name__}: {_import_exc}"
    PROVIDER_VERSION = "fallback"

    class YahooChartProvider:  # type: ignore
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._error = _IMPORT_ERROR

        async def get_quote_patch(self, symbol: str, base: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
            return _err_payload(symbol, self._error, base=base, where="YahooChartProvider.get_quote_patch")

        async def fetch_quote(self, symbol: str, debug: bool = False) -> Dict[str, Any]:
            return _err_payload(symbol, self._error, where="YahooChartProvider.fetch_quote")

    async def fetch_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _err_payload(symbol, _IMPORT_ERROR, where="fetch_quote(import_fail)")

    async def get_quote(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await fetch_quote(symbol, *args, **kwargs)

    async def get_quote_patch(
        symbol: str,
        base: Optional[Dict[str, Any]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        return _err_payload(symbol, _IMPORT_ERROR, base=base, where="get_quote_patch(import_fail)")

    async def fetch_quote_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _err_payload(symbol, _IMPORT_ERROR, where="fetch_quote_patch(import_fail)")

    async def fetch_enriched_quote_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _err_payload(symbol, _IMPORT_ERROR, where="fetch_enriched_quote_patch(import_fail)")

    async def fetch_quote_and_enrichment_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _err_payload(symbol, _IMPORT_ERROR, where="fetch_quote_and_enrichment_patch(import_fail)")

    async def fetch_quote_and_fundamentals_patch(symbol: str, debug: bool = False, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _err_payload(symbol, _IMPORT_ERROR, where="fetch_quote_and_fundamentals_patch(import_fail)")

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

    async def aclose_yahoo_client() -> None:
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
    "fetch_quote_and_fundamentals_patch",
    "yahoo_chart_quote",
    # History API (best-effort)
    "fetch_price_history",
    "fetch_history",
    "fetch_ohlc_history",
    "fetch_history_patch",
    "fetch_prices",
    # Client close
    "aclose_yahoo_chart_client",
    "aclose_yahoo_client",
]
