```python
# core/providers/finnhub_provider.py  (FULL REPLACEMENT)
"""
core/providers/finnhub_provider.py
===============================================================
Finnhub Provider / Client (v1.5.0) — PROD SAFE + Async + Engine-Aligned (HARDENED)

What’s improved vs v1.4.0 (avoid errors + missing data)
- ✅ Import-safe settings: NO get_settings() at import-time (prevents startup crashes).
- ✅ All patches ALWAYS include: data_source, provider_version, last_updated_utc (even on partial/error).
- ✅ Better HTTP diagnostics: provider_status_code + provider_latency_ms + compact error strings.
- ✅ Quote parsing hardened + computes change/% when missing.
- ✅ Profile + Metrics parsing expanded (more field aliases, safe merges).
- ✅ Micro-caches preserved but configured lazily via env/settings (async).

KSA-SAFE:
- Refuses .SR and numeric-only symbols by default.
- allow_ksa=True must be explicitly passed to override (engine should not do this).

Endpoints used
- /quote
- /stock/profile2
- /stock/metric?metric=all
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, Union

import httpx
from cachetools import TTLCache

logger = logging.getLogger("core.providers.finnhub_provider")

PROVIDER_VERSION = "1.5.0"

DEFAULT_BASE_URL = "https://finnhub.io/api/v1"
DEFAULT_TIMEOUT_SEC = 25.0
DEFAULT_RETRY_ATTEMPTS = 3

QUOTE_PATH = "/quote"
PROFILE_PATH = "/stock/profile2"
METRIC_PATH = "/stock/metric"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_TRUTHY = {"1", "true", "yes", "on", "y", "t"}
_FALSY = {"0", "false", "no", "off", "n", "f"}

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


# -----------------------------------------------------------------------------
# Lazy settings shim (PROD SAFE) — no import-time get_settings()
# -----------------------------------------------------------------------------
_SETTINGS_OBJ: Optional[Any] = None
_SETTINGS_LOCK = asyncio.Lock()


async def _get_settings_obj() -> Optional[Any]:
    global _SETTINGS_OBJ
    if _SETTINGS_OBJ is not None:
        return _SETTINGS_OBJ
    async with _SETTINGS_LOCK:
        if _SETTINGS_OBJ is not None:
            return _SETTINGS_OBJ
        try:
            from core.config import get_settings  # type: ignore

            _SETTINGS_OBJ = get_settings()
            return _SETTINGS_OBJ
        except Exception:
            _SETTINGS_OBJ = None
            return None


def _get_env_any(name: str) -> Optional[str]:
    v = os.getenv(name)
    if v is not None:
        return v
    v = os.getenv(name.upper())
    if v is not None:
        return v
    v = os.getenv(name.lower())
    if v is not None:
        return v
    return None


async def _get_attr_or_env(name: str, default: Any = None) -> Any:
    s = await _get_settings_obj()
    if s is not None:
        try:
            if hasattr(s, name):
                v = getattr(s, name)
                if v is not None:
                    return v
        except Exception:
            pass
    v = _get_env_any(name)
    return v if v is not None else default


async def _get_int(name: str, default: int) -> int:
    raw = await _get_attr_or_env(name, None)
    if raw is None:
        return default
    try:
        x = int(str(raw).strip())
        return x if x > 0 else default
    except Exception:
        return default


async def _get_float(name: str, default: float) -> float:
    raw = await _get_attr_or_env(name, None)
    if raw is None:
        return default
    try:
        x = float(str(raw).strip())
        return x if x > 0 else default
    except Exception:
        return default


async def _get_bool(name: str, default: bool) -> bool:
    raw = await _get_attr_or_env(name, None)
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return default
    s = str(raw).strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clamp(s: Any, n: int = 300) -> str:
    try:
        t = str(s or "")
        return t if len(t) <= n else (t[:n] + "…")
    except Exception:
        return ""


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            f = float(val)
            if math.isnan(f) or math.isinf(f):
                return None
            return f

        s = str(val).strip()
        if not s or s in {"-", "—", "N/A", "NA", "null", "None"}:
            return None

        s = s.translate(_ARABIC_DIGITS)
        s = s.replace("٬", ",").replace("٫", ".")
        s = (
            s.replace("SAR", "")
            .replace("USD", "")
            .replace("ريال", "")
            .replace("$", "")
            .replace("﷼", "")
            .replace("%", "")
            .replace(",", "")
            .strip()
        )

        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        m = re.match(r"^(-?\d+(\.\d+)?)([KMB])$", s, re.IGNORECASE)
        mult = 1.0
        if m:
            num = m.group(1)
            suf = m.group(3).upper()
            mult = 1_000.0 if suf == "K" else 1_000_000.0 if suf == "M" else 1_000_000_000.0
            s = num

        f = float(s) * mult
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _pct_if_fraction(x: Any) -> Optional[float]:
    v = _safe_float(x)
    if v is None:
        return None
    return v * 100.0 if abs(v) <= 1.0 else v


def _is_ksa_symbol(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    return s.endswith(".SR") or s.isdigit()


def _finnhub_symbol(symbol: str) -> str:
    """
    Finnhub uses plain tickers for US (AAPL), not AAPL.US.
    Keep special symbols unchanged. Keep .SR unchanged (but Finnhub generally won't support).
    """
    s = (symbol or "").strip().upper()
    if not s:
        return s
    if any(ch in s for ch in ("=", "^")):
        return s
    if s.endswith(".US"):
        return s[:-3]
    return s


def _base_patch(provider_symbol: Optional[str] = None) -> Dict[str, Any]:
    p: Dict[str, Any] = {
        "data_source": "finnhub",
        "provider_version": PROVIDER_VERSION,
        "last_updated_utc": _utc_iso(),
    }
    if provider_symbol:
        p["provider_symbol"] = provider_symbol
    return p


# -----------------------------------------------------------------------------
# Client
# -----------------------------------------------------------------------------
class FinnhubClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout_sec: float = DEFAULT_TIMEOUT_SEC,
        retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
        *,
        quote_ttl_sec: float = 15.0,
        meta_ttl_sec: float = 21600.0,
    ) -> None:
        self.api_key = (api_key or "").strip() or None
        self.base_url = (base_url or DEFAULT_BASE_URL).rstrip("/")
        self.timeout_sec = float(timeout_sec) if timeout_sec and timeout_sec > 0 else DEFAULT_TIMEOUT_SEC
        self.retry_attempts = int(retry_attempts) if retry_attempts and retry_attempts > 0 else DEFAULT_RETRY_ATTEMPTS

        timeout = httpx.Timeout(self.timeout_sec, connect=min(10.0, self.timeout_sec))
        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.8,ar;q=0.6",
            },
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=40),
        )

        self._quote_cache: TTLCache = TTLCache(maxsize=6000, ttl=max(5.0, float(quote_ttl_sec)))
        self._meta_cache: TTLCache = TTLCache(maxsize=4000, ttl=max(120.0, float(meta_ttl_sec)))

    async def aclose(self) -> None:
        await self._client.aclose()

    def _token(self, api_key: Optional[str]) -> Optional[str]:
        k = (api_key or "").strip() or (self.api_key or "").strip()
        return k or None

    async def _get_json(
        self,
        path: str,
        params: Dict[str, Any],
    ) -> Tuple[Optional[Union[Dict[str, Any], list]], Optional[str], int, int]:
        """
        Returns: (data, error, status_code, latency_ms)
        Never raises.
        """
        url = f"{self.base_url}{path}"
        last_status = 0
        last_err: Optional[str] = None

        for attempt in range(max(1, self.retry_attempts)):
            t0 = time.perf_counter()
            try:
                r = await self._client.get(url, params=params)
                dt_ms = int((time.perf_counter() - t0) * 1000)
                last_status = int(getattr(r, "status_code", 0) or 0)

                # Retry on rate limit / transient server errors
                if last_status == 429 or (500 <= last_status < 600):
                    if attempt < (self.retry_attempts - 1):
                        ra = (r.headers.get("Retry-After") or "").strip()
                        if ra.isdigit():
                            await asyncio.sleep(min(3.0, float(ra)))
                        else:
                            await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.35)
                        continue
                    return None, f"http {last_status}", last_status, dt_ms

                if last_status >= 400:
                    return None, f"http {last_status}", last_status, dt_ms

                try:
                    data = r.json()
                except Exception:
                    txt = (r.text or "").strip()
                    if txt.startswith("{") or txt.startswith("["):
                        try:
                            data = json.loads(txt)
                        except Exception:
                            return None, "invalid json", last_status, dt_ms
                    else:
                        return None, "empty/invalid body", last_status, dt_ms

                # Finnhub sometimes returns { "error": "..." } with 200
                if isinstance(data, dict) and data.get("error"):
                    return data, _clamp(data.get("error")), last_status, dt_ms

                return data, None, last_status, dt_ms

            except Exception as exc:
                dt_ms = int((time.perf_counter() - t0) * 1000)
                last_err = str(exc)
                if attempt < (self.retry_attempts - 1):
                    await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.35)
                    continue
                return None, _clamp(last_err, 500), last_status, dt_ms

        return None, last_err or "request failed", last_status, 0

    # -------------------------------------------------------------------------
    # Quote
    # -------------------------------------------------------------------------
    async def fetch_quote_patch(
        self,
        symbol: str,
        api_key: Optional[str] = None,
        *,
        allow_ksa: bool = False,
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        sym_in = (symbol or "").strip()
        if not sym_in:
            return _base_patch(), "Finnhub: empty symbol"

        if _is_ksa_symbol(sym_in) and not allow_ksa:
            return _base_patch(_finnhub_symbol(sym_in)), "Finnhub: KSA symbol refused (.SR/numeric not supported)"

        sym = _finnhub_symbol(sym_in)
        token = self._token(api_key)
        if not token:
            return _base_patch(sym), "Finnhub: missing api key"

        cache_key = f"q::{sym}"
        hit = self._quote_cache.get(cache_key)
        if isinstance(hit, dict) and hit:
            return dict(hit), None

        params = {"symbol": sym, "token": token}
        data, ferr, status, dt_ms = await self._get_json(QUOTE_PATH, params=params)

        patch = _base_patch(sym)
        patch["provider_status_code"] = status
        patch["provider_latency_ms"] = dt_ms

        if not data or not isinstance(data, dict):
            return patch, f"Finnhub: empty response ({ferr or 'no data'}) ({dt_ms}ms)"

        # Finnhub quote schema:
        # c=current, pc=prev close, o=open, h=high, l=low, d=change, dp=percent change, t=timestamp
        cp = _safe_float(data.get("c"))
        pc = _safe_float(data.get("pc"))
        opn = _safe_float(data.get("o"))
        high = _safe_float(data.get("h"))
        low = _safe_float(data.get("l"))

        chg = _safe_float(data.get("d"))
        pct = _safe_float(data.get("dp"))

        # Compute change/% if missing but have cp & pc
        if chg is None and cp is not None and pc is not None:
            chg = cp - pc
        if pct is None and cp is not None and pc not in (None, 0.0):
            try:
                pct = (cp - pc) / pc * 100.0
            except Exception:
                pct = None

        patch.update(
            {
                "current_price": cp,
                "previous_close": pc,
                "open": opn,
                "day_high": high,
                "day_low": low,
                "price_change": chg,
                "percent_change": pct,
            }
        )

        ts = _safe_float(data.get("t"))
        if ts:
            try:
                patch["finnhub_timestamp_utc"] = datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
            except Exception:
                pass

        # Cache only if we got a usable price
        if patch.get("current_price") is not None:
            self._quote_cache[cache_key] = dict(patch)
            return patch, None

        # return partial patch with an error (do NOT return empty {})
        return patch, f"Finnhub: no price in response ({dt_ms}ms)"

    # -------------------------------------------------------------------------
    # Profile
    # -------------------------------------------------------------------------
    async def fetch_profile_patch(
        self,
        symbol: str,
        api_key: Optional[str] = None,
        *,
        allow_ksa: bool = False,
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        sym_in = (symbol or "").strip()
        if not sym_in:
            return _base_patch(), "Finnhub: empty symbol"

        if _is_ksa_symbol(sym_in) and not allow_ksa:
            return _base_patch(_finnhub_symbol(sym_in)), "Finnhub: KSA symbol refused (.SR/numeric not supported)"

        sym = _finnhub_symbol(sym_in)
        token = self._token(api_key)
        if not token:
            return _base_patch(sym), "Finnhub: missing api key"

        cache_key = f"profile::{sym}"
        hit = self._meta_cache.get(cache_key)
        if isinstance(hit, dict) and hit:
            return dict(hit), None

        params = {"symbol": sym, "token": token}
        data, ferr, status, dt_ms = await self._get_json(PROFILE_PATH, params=params)

        patch = _base_patch(sym)
        patch["provider_status_code"] = status
        patch["provider_latency_ms"] = dt_ms

        if not data or not isinstance(data, dict):
            return patch, f"Finnhub: profile empty ({ferr or 'no data'})"

        # Finnhub profile2 commonly:
        # name, ticker, exchange, ipo, marketCapitalization, shareOutstanding, currency, finnhubIndustry, weburl, logo, country
        patch.update(
            {
                "name": _safe_str(data.get("name")),
                "ticker": _safe_str(data.get("ticker")) or sym,
                "exchange": _safe_str(data.get("exchange")),
                "country": _safe_str(data.get("country")),
                "currency": _safe_str(data.get("currency")),
                "industry": _safe_str(data.get("finnhubIndustry") or data.get("industry")),
                "listing_date": _safe_str(data.get("ipo")),
                "market_cap": _safe_float(data.get("marketCapitalization")),
                "shares_outstanding": _safe_float(data.get("shareOutstanding")),
                "website": _safe_str(data.get("weburl")),
                "logo": _safe_str(data.get("logo")),
            }
        )

        # Cache profile even if partial
        self._meta_cache[cache_key] = dict(patch)
        return patch, None

    # -------------------------------------------------------------------------
    # Metrics
    # -------------------------------------------------------------------------
    async def fetch_metrics_patch(
        self,
        symbol: str,
        api_key: Optional[str] = None,
        *,
        allow_ksa: bool = False,
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        sym_in = (symbol or "").strip()
        if not sym_in:
            return _base_patch(), "Finnhub: empty symbol"

        if _is_ksa_symbol(sym_in) and not allow_ksa:
            return _base_patch(_finnhub_symbol(sym_in)), "Finnhub: KSA symbol refused (.SR/numeric not supported)"

        sym = _finnhub_symbol(sym_in)
        token = self._token(api_key)
        if not token:
            return _base_patch(sym), "Finnhub: missing api key"

        cache_key = f"metrics::{sym}"
        hit = self._meta_cache.get(cache_key)
        if isinstance(hit, dict) and hit:
            return dict(hit), None

        params = {"symbol": sym, "metric": "all", "token": token}
        data, ferr, status, dt_ms = await self._get_json(METRIC_PATH, params=params)

        patch = _base_patch(sym)
        patch["provider_status_code"] = status
        patch["provider_latency_ms"] = dt_ms

        if not data or not isinstance(data, dict):
            return patch, f"Finnhub: metrics empty ({ferr or 'no data'})"

        m = data.get("metric") or {}
        if not isinstance(m, dict) or not m:
            return patch, "Finnhub: metrics missing"

        # Map common Finnhub metric keys to UnifiedQuote-friendly names
        patch.update(
            {
                "high_52w": _safe_float(m.get("52WeekHigh")),
                "low_52w": _safe_float(m.get("52WeekLow")),
                "pe_ttm": _safe_float(m.get("peTTM") or m.get("peAnnual") or m.get("peBasicExclExtraTTM")),
                "pb": _safe_float(m.get("pbAnnual") or m.get("pbQuarterly") or m.get("pbTTM")),
                "ps": _safe_float(m.get("psTTM") or m.get("psAnnual")),
                "eps_ttm": _safe_float(m.get("epsTTM") or m.get("epsBasicExclExtraItemsTTM")),
                "beta": _safe_float(m.get("beta")),
                "dividend_yield": _pct_if_fraction(
                    m.get("dividendYieldIndicatedAnnual")
                    or m.get("dividendYieldAnnual")
                    or m.get("dividendYieldTTM")
                ),
                "roe": _pct_if_fraction(m.get("roeTTM") or m.get("roeAnnual")),
                "roa": _pct_if_fraction(m.get("roaTTM") or m.get("roaAnnual")),
                "net_margin": _pct_if_fraction(m.get("netMarginTTM") or m.get("netMarginAnnual")),
                "operating_margin": _pct_if_fraction(m.get("operatingMarginTTM") or m.get("operatingMarginAnnual")),
                "gross_margin": _pct_if_fraction(m.get("grossMarginTTM") or m.get("grossMarginAnnual")),
                "debt_to_equity": _safe_float(
                    m.get("totalDebt/totalEquityAnnual")
                    or m.get("totalDebt/totalEquityQuarterly")
                    or m.get("totalDebt/totalEquityTTM")
                ),
                "current_ratio": _safe_float(m.get("currentRatioAnnual") or m.get("currentRatioQuarterly")),
                "quick_ratio": _safe_float(m.get("quickRatioAnnual") or m.get("quickRatioQuarterly")),
                "free_cash_flow": _safe_float(m.get("freeCashFlowTTM")),
            }
        )

        # Cache metrics even if partial
        self._meta_cache[cache_key] = dict(patch)
        return patch, None

    # -------------------------------------------------------------------------
    # Combined helpers
    # -------------------------------------------------------------------------
    async def fetch_profile_and_metrics_patch(
        self,
        symbol: str,
        api_key: Optional[str] = None,
        *,
        allow_ksa: bool = False,
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        """
        Concurrently fetch profile+metrics and merge into one PATCH dict.
        Always returns a patch with base fields; error only if both fail hard.
        """
        prof_res, met_res = await asyncio.gather(
            self.fetch_profile_patch(symbol, api_key=api_key, allow_ksa=allow_ksa),
            self.fetch_metrics_patch(symbol, api_key=api_key, allow_ksa=allow_ksa),
            return_exceptions=True,
        )

        patch: Dict[str, Any] = _base_patch(_finnhub_symbol(symbol))
        errs = []

        if isinstance(prof_res, Exception):
            errs.append(f"profile:{_clamp(prof_res)}")
        else:
            p_patch, p_err = prof_res
            if isinstance(p_patch, dict) and p_patch:
                patch.update({k: v for k, v in p_patch.items() if v is not None})
            if p_err:
                errs.append(p_err)

        if isinstance(met_res, Exception):
            errs.append(f"metrics:{_clamp(met_res)}")
        else:
            m_patch, m_err = met_res
            if isinstance(m_patch, dict) and m_patch:
                patch.update({k: v for k, v in m_patch.items() if v is not None})
            if m_err:
                errs.append(m_err)

        # If patch has anything beyond base keys, treat as success
        if _has_payload(patch):
            return patch, None

        return patch, ("Finnhub: " + " | ".join([e for e in errs if e])) if errs else "Finnhub: meta unknown error"

    async def fetch_quote_and_enrichment_patch(
        self,
        symbol: str,
        api_key: Optional[str] = None,
        *,
        allow_ksa: bool = False,
        enrich: bool = True,
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        """
        One-call helper:
        - Quote patch always attempted
        - If enrich=True, profile+metrics are also fetched and merged
        """
        if not enrich:
            return await self.fetch_quote_patch(symbol, api_key=api_key, allow_ksa=allow_ksa)

        q_res, meta_res = await asyncio.gather(
            self.fetch_quote_patch(symbol, api_key=api_key, allow_ksa=allow_ksa),
            self.fetch_profile_and_metrics_patch(symbol, api_key=api_key, allow_ksa=allow_ksa),
            return_exceptions=True,
        )

        patch: Dict[str, Any] = _base_patch(_finnhub_symbol(symbol))
        errs = []

        if isinstance(q_res, Exception):
            errs.append(f"quote:{_clamp(q_res)}")
        else:
            q_patch, q_err = q_res
            if isinstance(q_patch, dict) and q_patch:
                patch.update({k: v for k, v in q_patch.items() if v is not None})
            if q_err:
                errs.append(q_err)

        if isinstance(meta_res, Exception):
            errs.append(f"meta:{_clamp(meta_res)}")
        else:
            m_patch, m_err = meta_res
            if isinstance(m_patch, dict) and m_patch:
                patch.update({k: v for k, v in m_patch.items() if v is not None})
            if m_err:
                errs.append(m_err)

        if _has_payload(patch):
            return patch, None

        return patch, ("Finnhub: " + " | ".join([e for e in errs if e])) if errs else "Finnhub: unknown error"


def _has_payload(patch: Dict[str, Any]) -> bool:
    """
    Detect if we have real data beyond base fields.
    """
    base = {"data_source", "provider_version", "last_updated_utc", "provider_symbol", "provider_status_code", "provider_latency_ms"}
    for k, v in (patch or {}).items():
        if k in base:
            continue
        if v is not None and v != "":
            return True
    return False


# -----------------------------------------------------------------------------
# Lazy singleton (PROD SAFE)
# -----------------------------------------------------------------------------
_CLIENT_SINGLETON: Optional[FinnhubClient] = None
_CLIENT_LOCK = asyncio.Lock()


async def get_finnhub_client(api_key: Optional[str] = None) -> FinnhubClient:
    global _CLIENT_SINGLETON
    if _CLIENT_SINGLETON is not None:
        return _CLIENT_SINGLETON

    async with _CLIENT_LOCK:
        if _CLIENT_SINGLETON is not None:
            return _CLIENT_SINGLETON

        base_url = (await _get_attr_or_env("FINNHUB_BASE_URL", DEFAULT_BASE_URL)) or DEFAULT_BASE_URL
        base_url = str(base_url).strip() or DEFAULT_BASE_URL

        timeout_sec = await _get_float("HTTP_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC)
        retries = await _get_int("HTTP_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS)

        quote_ttl = await _get_float("FINNHUB_QUOTE_TTL_SEC", 15.0)
        meta_ttl = await _get_float("FINNHUB_META_TTL_SEC", 21600.0)

        _CLIENT_SINGLETON = FinnhubClient(
            api_key=_safe_str(api_key),
            base_url=_safe_str(base_url) or DEFAULT_BASE_URL,
            timeout_sec=float(timeout_sec),
            retry_attempts=int(retries),
            quote_ttl_sec=float(quote_ttl),
            meta_ttl_sec=float(meta_ttl),
        )

        logger.info(
            "Finnhub client init v%s | base=%s | timeout=%.1fs | retries=%s | quote_ttl=%.1fs | meta_ttl=%.1fs",
            PROVIDER_VERSION,
            base_url,
            float(timeout_sec),
            int(retries),
            float(quote_ttl),
            float(meta_ttl),
        )

    return _CLIENT_SINGLETON


async def aclose_finnhub_client() -> None:
    global _CLIENT_SINGLETON
    if _CLIENT_SINGLETON is None:
        return
    try:
        await _CLIENT_SINGLETON.aclose()
    finally:
        _CLIENT_SINGLETON = None


# -----------------------------------------------------------------------------
# Convenience functions (engine-friendly)
# -----------------------------------------------------------------------------
async def fetch_quote_patch(symbol: str, api_key: Optional[str]) -> Tuple[Dict[str, Any], Optional[str]]:
    c = await get_finnhub_client()
    return await c.fetch_quote_patch(symbol, api_key=api_key, allow_ksa=False)


async def fetch_profile_and_metrics_patch(symbol: str, api_key: Optional[str]) -> Tuple[Dict[str, Any], Optional[str]]:
    c = await get_finnhub_client()
    return await c.fetch_profile_and_metrics_patch(symbol, api_key=api_key, allow_ksa=False)


async def fetch_quote_and_enrichment_patch(symbol: str, api_key: Optional[str]) -> Tuple[Dict[str, Any], Optional[str]]:
    c = await get_finnhub_client()
    return await c.fetch_quote_and_enrichment_patch(symbol, api_key=api_key, allow_ksa=False, enrich=True)


__all__ = [
    "FinnhubClient",
    "get_finnhub_client",
    "aclose_finnhub_client",
    "fetch_quote_patch",
    "fetch_profile_and_metrics_patch",
    "fetch_quote_and_enrichment_patch",
    "PROVIDER_VERSION",
]
```
