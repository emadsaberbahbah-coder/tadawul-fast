from __future__ import annotations

"""
Yahoo Fundamentals Provider (NO yfinance) — v1.2.0 (PROD SAFE)

Best-effort fundamentals for symbols (works for KSA .SR + GLOBAL) using ONLY Yahoo
public endpoints (quoteSummary + v7 quote), with defensive retries, throttling,
and safe parsing.

Fields returned (best-effort):
- market_cap, pe_ttm, pb, dividend_yield, roe, roa, currency

Design goals
- Never crash the app on import (safe defaults).
- Async-friendly, re-usable httpx client support.
- Robust parsing across Yahoo field variants (raw/fmt and alternative keys).
- Bounded jitter backoff to reduce transient 429/5xx issues.
- Returns structured output with timestamps and error details (no exceptions leak).

Notes
- dividend_yield is a FRACTION (e.g., 0.035). Downstream can convert to %.
- roe/roa are usually FRACTION (e.g., 0.12). Downstream can convert to %.
"""

import asyncio
import random
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import httpx

PROVIDER_VERSION = "1.2.0"

YAHOO_QUOTE_SUMMARY_URL = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Reasonable defaults (can be overridden by caller)
DEFAULT_TIMEOUT_SEC = 12.0
DEFAULT_RETRY_ATTEMPTS = 3

# Some symbols are passed as AAPL.US in your system; Yahoo expects AAPL
# KSA usually already uses .SR which Yahoo expects unchanged.
def _yahoo_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if s.endswith(".US"):
        return s[:-3]
    return s


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(x: Any) -> Optional[float]:
    """
    Parses numbers from:
    - float/int
    - strings with commas
    - dicts like {"raw": ..., "fmt": ...}
    Returns None for NaN, blanks, or non-numeric.
    """
    try:
        if x is None or isinstance(x, bool):
            return None
        if isinstance(x, (int, float)):
            v = float(x)
            return None if v != v else v  # NaN check
        if isinstance(x, dict):
            # Prefer raw, then fmt if raw missing
            if "raw" in x:
                return _safe_float(x.get("raw"))
            if "fmt" in x:
                return _safe_float(x.get("fmt"))
            # any other shape
            return None
        s = str(x).strip().replace(",", "")
        if not s or s in {"-", "—", "N/A", "NA", "null", "None"}:
            return None
        v = float(s)
        return None if v != v else v
    except Exception:
        return None


def _get_raw(obj: Any) -> Optional[float]:
    """
    Yahoo often returns objects like {"raw": 123, "fmt": "123"}.
    This helper extracts numeric values safely.
    """
    if obj is None:
        return None
    if isinstance(obj, (int, float)):
        return _safe_float(obj)
    if isinstance(obj, dict):
        return _safe_float(obj.get("raw")) or _safe_float(obj.get("fmt"))
    return _safe_float(obj)


def _sleep_backoff(attempt: int) -> float:
    """
    Compute bounded exponential backoff with jitter.
    Returns the seconds to sleep.
    """
    base = 0.25 * (2**attempt)
    jitter = random.random() * 0.35
    return min(2.5, base + jitter)


def _base(symbol: str) -> Dict[str, Any]:
    return {
        "symbol": symbol,
        "provider": "yahoo_fundamentals",
        "provider_version": PROVIDER_VERSION,
        "last_updated_utc": _now_utc_iso(),
        "market_cap": None,
        "pe_ttm": None,
        "pb": None,
        "dividend_yield": None,  # fraction
        "roe": None,             # fraction
        "roa": None,             # fraction
        "currency": None,
        "error": None,
        "status_code": None,
        "source": None,  # "quoteSummary" | "quoteV7" | "quoteSummary+quoteV7"
    }


def _is_transient_status(code: Optional[int]) -> bool:
    if code is None:
        return True
    return code in (408, 425, 429, 500, 502, 503, 504)


def _client_defaults(timeout: float) -> Dict[str, Any]:
    return {
        "headers": {
            "User-Agent": USER_AGENT,
            "Accept": "application/json,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.8,ar;q=0.6",
            "Referer": "https://finance.yahoo.com/",
            "Connection": "keep-alive",
        },
        "follow_redirects": True,
        "timeout": httpx.Timeout(timeout),
        "limits": httpx.Limits(max_keepalive_connections=10, max_connections=20),
    }


def _merge_best_effort(out: Dict[str, Any], patch: Dict[str, Any]) -> None:
    """
    Only fill missing values in `out` from `patch`.
    """
    for k, v in patch.items():
        if v is None:
            continue
        if out.get(k) is None:
            out[k] = v


def _summarize_error(prefix: str, exc: Exception, status_code: Optional[int] = None) -> str:
    msg = f"{prefix}: {type(exc).__name__}: {exc}"
    if status_code is not None:
        msg = f"{prefix} HTTP {status_code}: {type(exc).__name__}: {exc}"
    # Keep error short-ish for Sheets
    return msg[:5000]


async def _fetch_json(
    client: httpx.AsyncClient,
    url: str,
    params: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[int], Optional[str]]:
    """
    Returns (json, status_code, error_str)
    """
    try:
        r = await client.get(url, params=params)
        code = r.status_code
        if code >= 400:
            return None, code, f"HTTP {code}"
        j = r.json() or {}
        if not isinstance(j, dict):
            return None, code, "Non-dict JSON"
        return j, code, None
    except Exception as exc:
        return None, None, _summarize_error("request failed", exc)


async def _try_quote_summary(client: httpx.AsyncClient, sym: str) -> Tuple[Dict[str, Any], Optional[int], Optional[str]]:
    url = YAHOO_QUOTE_SUMMARY_URL.format(symbol=sym)
    params = {"modules": "price,summaryDetail,defaultKeyStatistics,financialData"}
    j, code, err = await _fetch_json(client, url, params=params)
    if err:
        return {}, code, f"quoteSummary {err}"

    res = (((j.get("quoteSummary") or {}).get("result")) or [])
    if not res:
        return {}, code, "quoteSummary empty result"

    data = res[0] or {}
    price = data.get("price") or {}
    summ = data.get("summaryDetail") or {}
    stats = data.get("defaultKeyStatistics") or {}
    fin = data.get("financialData") or {}

    patch: Dict[str, Any] = {}

    # currency
    patch["currency"] = price.get("currency")

    # market cap
    patch["market_cap"] = _get_raw(price.get("marketCap")) or _get_raw(stats.get("marketCap"))

    # P/E (TTM)
    patch["pe_ttm"] = (
        _get_raw(summ.get("trailingPE"))
        or _get_raw(stats.get("trailingPE"))
        or _get_raw(stats.get("peRatio"))  # sometimes appears in variants
    )

    # P/B
    patch["pb"] = _get_raw(stats.get("priceToBook"))

    # dividend yield (fraction)
    patch["dividend_yield"] = (
        _get_raw(summ.get("dividendYield"))
        or _get_raw(summ.get("trailingAnnualDividendYield"))
    )

    # ROE / ROA (fractions)
    patch["roe"] = _get_raw(fin.get("returnOnEquity")) or _get_raw(stats.get("returnOnEquity"))
    patch["roa"] = _get_raw(fin.get("returnOnAssets")) or _get_raw(stats.get("returnOnAssets"))

    return patch, code, None


async def _try_quote_v7(client: httpx.AsyncClient, sym: str) -> Tuple[Dict[str, Any], Optional[int], Optional[str]]:
    j, code, err = await _fetch_json(client, YAHOO_QUOTE_URL, params={"symbols": sym})
    if err:
        return {}, code, f"quoteV7 {err}"

    q = (((j.get("quoteResponse") or {}).get("result")) or [])
    if not q:
        return {}, code, "quoteV7 empty result"

    q0 = q[0] or {}
    patch: Dict[str, Any] = {}

    patch["currency"] = q0.get("currency")

    patch["market_cap"] = _safe_float(q0.get("marketCap"))
    patch["pe_ttm"] = _safe_float(q0.get("trailingPE")) or _safe_float(q0.get("peRatio"))
    patch["pb"] = _safe_float(q0.get("priceToBook"))

    # Yahoo sometimes provides these in quoteV7:
    patch["dividend_yield"] = (
        _safe_float(q0.get("trailingAnnualDividendYield"))
        or _safe_float(q0.get("dividendYield"))
    )

    # ROE/ROA are not reliably present in v7 quote; keep only if present
    patch["roe"] = _safe_float(q0.get("returnOnEquity"))
    patch["roa"] = _safe_float(q0.get("returnOnAssets"))

    return patch, code, None


async def yahoo_fundamentals(
    symbol: str,
    *,
    timeout: float = DEFAULT_TIMEOUT_SEC,
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
    client: Optional[httpx.AsyncClient] = None,
) -> Dict[str, Any]:
    """
    Main provider function.

    Always returns a dict with:
      {symbol, provider, provider_version, last_updated_utc, fields..., error, status_code, source}
    Never raises exceptions out.
    """
    sym = _yahoo_symbol(symbol)
    out = _base(sym)

    if not sym:
        out["error"] = "Empty symbol"
        return out

    close_client = False
    if client is None:
        client = httpx.AsyncClient(**_client_defaults(timeout))
        close_client = True

    # Attempt quoteSummary first, then fallback to quoteV7 if still missing core fields
    last_err: Optional[str] = None
    last_code: Optional[int] = None
    used_sources: List[str] = []

    try:
        attempts = max(1, int(retry_attempts))

        # 1) quoteSummary
        for attempt in range(attempts):
            try:
                patch, code, err = await _try_quote_summary(client, sym)
                last_code = code
                if err:
                    last_err = err
                    if _is_transient_status(code) and attempt < attempts - 1:
                        await asyncio.sleep(_sleep_backoff(attempt))
                        continue
                _merge_best_effort(out, patch)
                used_sources.append("quoteSummary")
                last_err = None
                break
            except Exception as exc:
                last_err = _summarize_error("quoteSummary failed", exc, last_code)
                if attempt < attempts - 1:
                    await asyncio.sleep(_sleep_backoff(attempt))

        # 2) If key fields are still missing, try quoteV7
        core_missing = (
            out.get("market_cap") is None
            and out.get("pe_ttm") is None
            and out.get("pb") is None
            and out.get("dividend_yield") is None
        )

        if core_missing:
            for attempt in range(attempts):
                try:
                    patch, code, err = await _try_quote_v7(client, sym)
                    last_code = code
                    if err:
                        last_err = err
                        if _is_transient_status(code) and attempt < attempts - 1:
                            await asyncio.sleep(_sleep_backoff(attempt))
                            continue
                    _merge_best_effort(out, patch)
                    used_sources.append("quoteV7")
                    last_err = None
                    break
                except Exception as exc:
                    last_err = _summarize_error("quoteV7 failed", exc, last_code)
                    if attempt < attempts - 1:
                        await asyncio.sleep(_sleep_backoff(attempt))

        out["status_code"] = last_code
        out["source"] = "+".join(used_sources) if used_sources else None
        if last_err:
            out["error"] = last_err

        return out

    finally:
        if close_client:
            try:
                await client.aclose()
            except Exception:
                pass


# --- Engine adapter (what DataEngineV2 calls) --------------------------------
async def fetch_fundamentals_patch(symbol: str) -> Dict[str, Any]:
    """
    Returns a patch aligned to UnifiedQuote keys.
    Only non-None values are returned (safe to .update()).
    """
    d = await yahoo_fundamentals(symbol)
    patch = {
        "currency": d.get("currency"),
        "market_cap": d.get("market_cap"),
        "pe_ttm": d.get("pe_ttm"),
        "pb": d.get("pb"),
        "dividend_yield": d.get("dividend_yield"),
        "roe": d.get("roe"),
        "roa": d.get("roa"),
    }
    return {k: v for k, v in patch.items() if v is not None}
