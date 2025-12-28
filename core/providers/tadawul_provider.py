# core/providers/tadawul_provider.py  (FULL REPLACEMENT)
"""
core/providers/tadawul_provider.py
===============================================================
Tadawul Provider / Client (KSA quote + fundamentals) — v1.10.0
PROD SAFE + Async + ENGINE-ALIGNED (DICT RETURNS)

What this revision guarantees
- ✅ Engine-aligned exports:
    fetch_quote_patch
    fetch_enriched_quote_patch
    fetch_quote_and_enrichment_patch
  (all return Dict[str, Any] — NO tuples)
- ✅ Never crashes import; never does network at import-time
- ✅ Clean "not configured" if TADAWUL_* URLs are missing
- ✅ Strict KSA-only guard (prevents misrouting GLOBAL tickers into Tadawul)
- ✅ Defensive parsing + bounded deep-find (prevents recursion blowups)
- ✅ Retry/backoff on 429 + transient 5xx
- ✅ Cache for quote + fundamentals (TTL from env)
- ✅ Avoids misleading values: only sets fields we can parse

Supported env vars (optional)
- TADAWUL_MARKET_ENABLED          true/false (default true)
- TADAWUL_QUOTE_URL               template with {symbol} and/or {code}
- TADAWUL_FUNDAMENTALS_URL        template with {symbol} and/or {code}
  (also accepts TADAWUL_PROFILE_URL as fallback for fundamentals URL)
- TADAWUL_HEADERS_JSON            JSON dict of headers (optional)
- TADAWUL_TIMEOUT_SEC             default falls back to HTTP_TIMEOUT then 25
- TADAWUL_RETRY_ATTEMPTS          default 3 (min 1)
- TADAWUL_REFRESH_INTERVAL        used as quote cache TTL if > 0 (seconds)
- TADAWUL_QUOTE_TTL_SEC           explicit quote TTL override
- TADAWUL_FUND_TTL_SEC            fundamentals TTL (default 6 hours)

Also reads (if present)
- HTTP_TIMEOUT                    (Render env list)
- HTTP_TIMEOUT_SEC                (legacy)
- MAX_RETRIES / RETRY_DELAY       (tolerated)

Notes
- This provider expects you to supply your own Tadawul endpoints via env URLs.
- The JSON schema of those endpoints can vary; parsing is best-effort via key search.
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
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import httpx

logger = logging.getLogger("core.providers.tadawul_provider")

PROVIDER_NAME = "tadawul"
PROVIDER_VERSION = "1.10.0"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT_SEC = 25.0
DEFAULT_RETRY_ATTEMPTS = 3

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}

# KSA symbol: 3-6 digits, optionally .SR
_KSA_CODE_RE = re.compile(r"^\d{3,6}$")
_KSA_SYMBOL_RE = re.compile(r"^\d{3,6}(\.SR)?$", re.IGNORECASE)


# -----------------------------------------------------------------------------
# Safe helpers
# -----------------------------------------------------------------------------
def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def _to_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip()
    return s if s else default


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return float(str(v).strip())
    except Exception:
        return default


def _timeout_sec() -> float:
    t = _env_float("TADAWUL_TIMEOUT_SEC", 0.0)
    if t > 0:
        return t
    t = _env_float("HTTP_TIMEOUT", 0.0)
    if t > 0:
        return t
    t = _env_float("HTTP_TIMEOUT_SEC", 0.0)
    if t > 0:
        return t
    return DEFAULT_TIMEOUT_SEC


def _retry_attempts() -> int:
    r = _env_int("TADAWUL_RETRY_ATTEMPTS", 0)
    if r > 0:
        return r
    r = _env_int("HTTP_RETRY_ATTEMPTS", 0)
    if r > 0:
        return r
    r = _env_int("MAX_RETRIES", 0)
    if r > 0:
        return r
    return DEFAULT_RETRY_ATTEMPTS


def _retry_delay_sec() -> float:
    d = _env_float("TADAWUL_RETRY_DELAY_SEC", _env_float("RETRY_DELAY", _env_float("RETRY_DELAY_SEC", 0.35)))
    return d if d > 0 else 0.35


def _quote_ttl_sec() -> float:
    ttl = _env_float("TADAWUL_QUOTE_TTL_SEC", 0.0)
    if ttl > 0:
        return max(5.0, ttl)
    ri = _env_float("TADAWUL_REFRESH_INTERVAL", 0.0)
    if ri > 0:
        return max(5.0, ri)
    return 15.0


def _fund_ttl_sec() -> float:
    ttl = _env_float("TADAWUL_FUND_TTL_SEC", 0.0)
    if ttl > 0:
        return max(120.0, ttl)
    return 21600.0  # 6 hours


def _safe_float(val: Any) -> Optional[float]:
    """
    Robust numeric parser:
    - supports Arabic digits
    - strips %, +, commas
    - supports suffix K/M/B
    - supports (1.23) negatives
    """
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
        s = s.replace("SAR", "").replace("ريال", "").replace("USD", "").strip()
        s = s.replace("+", "").strip()

        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        s = s.replace("%", "").strip()
        s = s.replace(",", "")

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
    # If provider returns 0.034 => treat as 3.4%
    return v * 100.0 if abs(v) <= 1.0 else v


def _is_ksa_symbol(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    if not s:
        return False
    return bool(_KSA_SYMBOL_RE.match(s) or _KSA_CODE_RE.match(s))


def _ksa_code(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if s.endswith(".SR"):
        s = s[:-3]
    if "." in s:
        s = s.split(".", 1)[0]
    return s


def _normalize_ksa_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if s.endswith(".SR"):
        code = s[:-3]
        return f"{code}.SR" if _KSA_CODE_RE.match(code) else ""
    if _KSA_CODE_RE.match(s):
        return f"{s}.SR"
    return ""  # STRICT: prevent GLOBAL tickers


def _format_url(template: str, symbol: str) -> str:
    code = _ksa_code(symbol)
    return (template or "").replace("{code}", code).replace("{symbol}", symbol)


def _coerce_dict(data: Any) -> Dict[str, Any]:
    if isinstance(data, dict):
        return data
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0]
    return {}


# -----------------------------------------------------------------------------
# Bounded deep-find helpers (safe for unknown JSON shapes)
# -----------------------------------------------------------------------------
def _iter_children(x: Any) -> Iterable[Any]:
    if isinstance(x, dict):
        return x.values()
    if isinstance(x, list):
        return x
    return []


def _find_first_value(obj: Any, keys: Sequence[str], *, max_depth: int = 6, max_nodes: int = 2500) -> Any:
    """
    Breadth-first bounded search over dict/list nodes.
    Never raises; returns first matching value for any key in `keys`.
    """
    if obj is None:
        return None
    keyset = {str(k).strip().lower() for k in keys if k}
    if not keyset:
        return None

    q: List[Tuple[Any, int]] = [(obj, 0)]
    seen: set[int] = set()
    nodes = 0

    while q:
        x, d = q.pop(0)
        if x is None:
            continue

        xid = id(x)
        if xid in seen:
            continue
        seen.add(xid)

        nodes += 1
        if nodes > max_nodes:
            return None
        if d > max_depth:
            continue

        if isinstance(x, dict):
            for k, v in x.items():
                if str(k).strip().lower() in keyset:
                    return v
            for child in x.values():
                q.append((child, d + 1))
            continue

        if isinstance(x, list):
            for child in x:
                q.append((child, d + 1))
            continue

    return None


def _pick_num(obj: Any, *keys: str, max_depth: int = 6) -> Optional[float]:
    v = _find_first_value(obj, keys, max_depth=max_depth)
    return _safe_float(v)


def _pick_pct(obj: Any, *keys: str, max_depth: int = 6) -> Optional[float]:
    v = _find_first_value(obj, keys, max_depth=max_depth)
    return _pct_if_fraction(v)


def _pick_str(obj: Any, *keys: str, max_depth: int = 6) -> Optional[str]:
    v = _find_first_value(obj, keys, max_depth=max_depth)
    return _safe_str(v)


# -----------------------------------------------------------------------------
# Minimal TTL cache (fallback if cachetools isn't installed)
# -----------------------------------------------------------------------------
try:
    from cachetools import TTLCache  # type: ignore

    _HAS_CACHETOOLS = True
except Exception:  # pragma: no cover
    _HAS_CACHETOOLS = False

    class TTLCache(dict):  # type: ignore
        def __init__(self, maxsize: int = 1024, ttl: float = 60.0) -> None:
            super().__init__()
            self._maxsize = max(1, int(maxsize))
            self._ttl = max(1.0, float(ttl))
            self._exp: Dict[str, float] = {}

        def get(self, key: str, default: Any = None) -> Any:  # type: ignore
            now = time.time()
            exp = self._exp.get(key)
            if exp is not None and exp < now:
                try:
                    super().pop(key, None)
                except Exception:
                    pass
                self._exp.pop(key, None)
                return default
            return super().get(key, default)

        def __setitem__(self, key: str, value: Any) -> None:  # type: ignore
            # naive eviction
            if len(self) >= self._maxsize:
                try:
                    oldest_key = next(iter(self.keys()))
                    super().pop(oldest_key, None)
                    self._exp.pop(oldest_key, None)
                except Exception:
                    pass
            super().__setitem__(key, value)
            self._exp[key] = time.time() + self._ttl


# -----------------------------------------------------------------------------
# Output schema (subset; engine merge remains safe)
# -----------------------------------------------------------------------------
def _base(symbol: str) -> Dict[str, Any]:
    return {
        "status": "success",
        "symbol": symbol,
        "market": "KSA",
        "currency": "SAR",
        "data_source": PROVIDER_NAME,
        "data_quality": "OK",
        "error": "",
        "provider_version": PROVIDER_VERSION,
        "last_updated_utc": _utc_iso(),
        # identity / fundamentals
        "name": "",
        "sector": "",
        "industry": "",
        "sub_sector": "",
        "listing_date": "",
        "shares_outstanding": None,
        "free_float": None,
        "market_cap": None,
        "free_float_market_cap": None,
        # price / liquidity
        "current_price": None,
        "previous_close": None,
        "open": None,
        "day_high": None,
        "day_low": None,
        "high_52w": None,
        "low_52w": None,
        "position_52w_percent": None,
        "volume": None,
        "avg_volume_30d": None,
        "value_traded": None,
        "turnover_percent": None,
        # valuation / ratios (best-effort)
        "eps_ttm": None,
        "forward_eps": None,
        "pe_ttm": None,
        "forward_pe": None,
        "pb": None,
        "ps": None,
        "ev_ebitda": None,
        "dividend_yield": None,
        "dividend_rate": None,
        "payout_ratio": None,
        "roe": None,
        "roa": None,
        "net_margin": None,
        "ebitda_margin": None,
        "revenue_growth": None,
        "net_income_growth": None,
        "beta": None,
        "debt_to_equity": None,
        "current_ratio": None,
        "quick_ratio": None,
        # technicals (best-effort)
        "ma20": None,
        "ma50": None,
        # derived
        "price_change": None,
        "percent_change": None,
    }


def _pos_52w(cur: Optional[float], lo: Optional[float], hi: Optional[float]) -> Optional[float]:
    if cur is None or lo is None or hi is None:
        return None
    if hi == lo:
        return None
    return (cur - lo) / (hi - lo) * 100.0


def _fill_derived(out: Dict[str, Any]) -> None:
    cur = _safe_float(out.get("current_price"))
    prev = _safe_float(out.get("previous_close"))
    vol = _safe_float(out.get("volume"))

    if out.get("price_change") is None and cur is not None and prev is not None:
        out["price_change"] = cur - prev

    if out.get("percent_change") is None and cur is not None and prev not in (None, 0.0):
        out["percent_change"] = (cur - prev) / prev * 100.0

    if out.get("value_traded") is None and cur is not None and vol is not None:
        out["value_traded"] = cur * vol

    if out.get("position_52w_percent") is None:
        out["position_52w_percent"] = _pos_52w(cur, _safe_float(out.get("low_52w")), _safe_float(out.get("high_52w")))

    mc = _safe_float(out.get("market_cap"))
    ff = _safe_float(out.get("free_float"))
    if out.get("free_float_market_cap") is None and mc is not None and ff is not None:
        out["free_float_market_cap"] = mc * (ff / 100.0)


def _quality(out: Dict[str, Any]) -> None:
    has_price = _safe_float(out.get("current_price")) is not None
    rich = bool((out.get("name") or "").strip()) or (_safe_float(out.get("market_cap")) is not None)

    if not has_price:
        out["data_quality"] = "BAD"
    else:
        out["data_quality"] = "FULL" if rich else "OK"

    if (out.get("error") or "").strip():
        out["status"] = "error"


# -----------------------------------------------------------------------------
# Client
# -----------------------------------------------------------------------------
class TadawulClient:
    def __init__(
        self,
        quote_url: Optional[str] = None,
        fundamentals_url: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout_sec: Optional[float] = None,
        retry_attempts: Optional[int] = None,
    ) -> None:
        self.quote_url = _safe_str(quote_url or _env_str("TADAWUL_QUOTE_URL", ""))
        self.fundamentals_url = _safe_str(
            fundamentals_url
            or _env_str("TADAWUL_FUNDAMENTALS_URL", "")
            or _env_str("TADAWUL_PROFILE_URL", "")
        )

        self.timeout_sec = float(timeout_sec) if (timeout_sec and timeout_sec > 0) else _timeout_sec()
        self.retry_attempts = int(retry_attempts) if (retry_attempts and retry_attempts > 0) else _retry_attempts()

        timeout = httpx.Timeout(self.timeout_sec, connect=min(10.0, self.timeout_sec))
        base_headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/plain,text/html;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.8,ar;q=0.6",
        }

        # Optional custom headers from env JSON
        hdrs: Dict[str, str] = {}
        raw = _safe_str(_env_str("TADAWUL_HEADERS_JSON", ""))
        if raw:
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    hdrs.update({str(k): str(v) for k, v in obj.items()})
            except Exception:
                pass

        if headers:
            hdrs.update({str(k): str(v) for k, v in headers.items()})

        base_headers.update(hdrs)
        self._headers = base_headers

        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers=self._headers,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=40),
        )

        self._quote_cache: TTLCache = TTLCache(maxsize=5000, ttl=_quote_ttl_sec())
        self._fund_cache: TTLCache = TTLCache(maxsize=3000, ttl=_fund_ttl_sec())

        logger.info(
            "Tadawul client init v%s | quote_url_set=%s | fund_url_set=%s | timeout=%.1fs | retries=%s | cachetools=%s",
            PROVIDER_VERSION,
            bool(self.quote_url),
            bool(self.fundamentals_url),
            self.timeout_sec,
            self.retry_attempts,
            _HAS_CACHETOOLS,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _get_json(self, url: str) -> Tuple[Optional[Union[Dict[str, Any], List[Any]]], Optional[str]]:
        retries = max(1, int(self.retry_attempts))
        base_delay = _retry_delay_sec()

        for attempt in range(retries):
            try:
                r = await self._client.get(url)

                # Retry on 429 + 5xx
                if r.status_code == 429 or 500 <= r.status_code < 600:
                    if attempt < (retries - 1):
                        await asyncio.sleep(base_delay * (2**attempt) + random.random() * 0.35)
                        continue
                    return None, f"HTTP {r.status_code}"

                if r.status_code >= 400:
                    return None, f"HTTP {r.status_code}"

                # Prefer json() but tolerate JSON-in-text
                try:
                    return r.json(), None
                except Exception:
                    txt = (r.text or "").strip()
                    if txt.startswith("{") or txt.startswith("["):
                        try:
                            return json.loads(txt), None
                        except Exception:
                            return None, "invalid JSON"
                    return None, "non-JSON response"

            except Exception as e:
                if attempt < (retries - 1):
                    await asyncio.sleep(base_delay * (2**attempt) + random.random() * 0.35)
                    continue
                return None, f"{e.__class__.__name__}: {e}"

        return None, "unknown error"

    def _enabled(self) -> bool:
        v = os.getenv("TADAWUL_MARKET_ENABLED")
        if v is None or str(v).strip() == "":
            return True
        return _to_bool(v, True)

    async def fetch_quote_patch(self, symbol: str) -> Dict[str, Any]:
        out = _base(symbol)

        if not self._enabled():
            out["status"] = "error"
            out["data_quality"] = "BAD"
            out["error"] = "tadawul: disabled (TADAWUL_MARKET_ENABLED=false)"
            return out

        sym = _normalize_ksa_symbol(symbol)
        if not sym:
            out["status"] = "error"
            out["data_quality"] = "BAD"
            out["error"] = "tadawul: not a KSA symbol (expected 3-6 digits or .SR)"
            return out

        if not self.quote_url:
            out["status"] = "error"
            out["data_quality"] = "BAD"
            out["error"] = "tadawul: not configured (TADAWUL_QUOTE_URL)"
            return out

        ck = f"quote::{sym}"
        hit = self._quote_cache.get(ck)
        if isinstance(hit, dict) and hit:
            return dict(hit)

        url = _format_url(self.quote_url, sym)
        t0 = time.perf_counter()
        data, err = await self._get_json(url)
        dt_ms = int((time.perf_counter() - t0) * 1000)

        if not data:
            out["status"] = "error"
            out["data_quality"] = "BAD"
            out["error"] = f"tadawul: quote empty ({err or 'no data'}) ({dt_ms}ms)"
            return out

        root = _coerce_dict(data)

        # Quote fields (best-effort)
        price = _pick_num(
            root,
            "price", "last", "last_price", "lastPrice", "tradingPrice", "close", "currentPrice", "c",
            "trading_price", "lastTradePrice",
        )
        prev = _pick_num(root, "previous_close", "previousClose", "prevClose", "prev_close", "pc", "previousPrice")
        opn = _pick_num(root, "open", "openPrice", "o")
        hi = _pick_num(root, "high", "day_high", "dayHigh", "h", "sessionHigh")
        lo = _pick_num(root, "low", "day_low", "dayLow", "l", "sessionLow")
        vol = _pick_num(root, "volume", "tradedVolume", "qty", "quantity", "volumeTraded", "v", "tradedQty")
        val_traded = _pick_num(root, "valueTraded", "tradingValue", "turnoverValue", "tradedValue", "value")

        chg = _pick_num(root, "change", "price_change", "diff", "delta", "d")
        chg_p = _pick_pct(root, "change_percent", "percent_change", "changePercent", "pctChange", "dp")

        name = _pick_str(root, "name", "company", "companyName", "company_name", "securityName", "issuerName")
        currency = _pick_str(root, "currency") or "SAR"

        if price is None:
            out["status"] = "error"
            out["data_quality"] = "BAD"
            out["error"] = f"tadawul: quote missing price ({dt_ms}ms)"
            return out

        out.update(
            {
                "symbol": sym,
                "market": "KSA",
                "currency": currency,
                "name": name or "",
                "current_price": price,
                "previous_close": prev,
                "open": opn,
                "day_high": hi,
                "day_low": lo,
                "volume": vol,
                "value_traded": val_traded,
                "price_change": chg,
                "percent_change": chg_p,
                "last_updated_utc": _utc_iso(),
            }
        )

        _fill_derived(out)
        _quality(out)

        self._quote_cache[ck] = dict(out)
        return out

    async def fetch_fundamentals_patch(self, symbol: str) -> Dict[str, Any]:
        out = _base(symbol)

        if not self._enabled():
            out["status"] = "error"
            out["data_quality"] = "BAD"
            out["error"] = "tadawul: disabled (TADAWUL_MARKET_ENABLED=false)"
            return out

        sym = _normalize_ksa_symbol(symbol)
        if not sym:
            out["status"] = "error"
            out["data_quality"] = "BAD"
            out["error"] = "tadawul: not a KSA symbol (expected 3-6 digits or .SR)"
            return out

        if not self.fundamentals_url:
            out["status"] = "error"
            out["data_quality"] = "BAD"
            out["error"] = "tadawul: not configured (TADAWUL_FUNDAMENTALS_URL)"
            return out

        ck = f"fund::{sym}"
        hit = self._fund_cache.get(ck)
        if isinstance(hit, dict) and hit:
            return dict(hit)

        url = _format_url(self.fundamentals_url, sym)
        data, err = await self._get_json(url)
        if not data:
            out["status"] = "error"
            out["data_quality"] = "BAD"
            out["error"] = f"tadawul: fundamentals empty ({err or 'no data'})"
            return out

        root = _coerce_dict(data)

        # Identity
        name = _pick_str(root, "name", "companyName", "company_name", "securityName", "issuerName", "shortName", "longName")
        sector = _pick_str(root, "sector", "sectorName", "sector_name")
        industry = _pick_str(root, "industry", "industryName", "industry_name")
        sub_sector = _pick_str(root, "sub_sector", "subSector", "subSectorName", "sub_sector_name", "subIndustry")
        listing_date = _pick_str(root, "listing_date", "ipoDate", "IPODate", "listingDate", "ListingDate")

        # Core fundamentals
        market_cap = _pick_num(root, "market_cap", "marketCap", "MarketCap", "mcap", "marketCapitalization")
        shares_out = _pick_num(root, "shares_outstanding", "sharesOutstanding", "SharesOutstanding", "outstandingShares", "issuedShares")
        free_float_pct = _pick_pct(root, "free_float", "freeFloat", "freeFloatPercent", "freeFloatPct", "freeFloatPercentage")

        # Ratios
        eps_ttm = _pick_num(root, "eps", "eps_ttm", "epsTTM", "earningsPerShare", "EarningsPerShare")
        forward_eps = _pick_num(root, "forward_eps", "epsForward", "forwardEPS")
        pe_ttm = _pick_num(root, "pe", "pe_ttm", "peTTM", "priceEarnings", "PERatio")
        forward_pe = _pick_num(root, "forward_pe", "forwardPE", "peForward")
        pb = _pick_num(root, "pb", "priceToBook", "PBRatio")
        ps = _pick_num(root, "ps", "priceToSales", "PSRatio")
        ev_ebitda = _pick_num(root, "ev_ebitda", "evEbitda", "EVEBITDA")

        dividend_yield = _pick_pct(root, "dividend_yield", "dividendYield", "DividendYield")
        dividend_rate = _pick_num(root, "dividend_rate", "dividendRate", "DividendRate", "dividendPerShare")
        payout_ratio = _pick_pct(root, "payout_ratio", "payoutRatio", "PayoutRatio")

        roe = _pick_pct(root, "roe", "returnOnEquity", "ROE")
        roa = _pick_pct(root, "roa", "returnOnAssets", "ROA")
        net_margin = _pick_pct(root, "net_margin", "netMargin", "NetMargin")
        ebitda_margin = _pick_pct(root, "ebitda_margin", "ebitdaMargin", "EBITDAMargin")
        revenue_growth = _pick_pct(root, "revenue_growth", "revenueGrowth", "RevenueGrowth")
        net_income_growth = _pick_pct(root, "net_income_growth", "netIncomeGrowth", "NetIncomeGrowth")

        beta = _pick_num(root, "beta", "Beta")
        debt_to_equity = _pick_num(root, "debt_to_equity", "debtToEquity", "DebtToEquity")
        current_ratio = _pick_num(root, "current_ratio", "currentRatio", "CurrentRatio")
        quick_ratio = _pick_num(root, "quick_ratio", "quickRatio", "QuickRatio")

        high_52w = _pick_num(root, "high_52w", "52WeekHigh", "fiftyTwoWeekHigh", "yearHigh")
        low_52w = _pick_num(root, "low_52w", "52WeekLow", "fiftyTwoWeekLow", "yearLow")
        ma20 = _pick_num(root, "ma20", "MA20", "20DayMA", "movingAverage20")
        ma50 = _pick_num(root, "ma50", "MA50", "50DayMA", "movingAverage50")

        out.update(
            {
                "symbol": sym,
                "market": "KSA",
                "currency": "SAR",
                "name": name or "",
                "sector": sector or "",
                "industry": industry or "",
                "sub_sector": sub_sector or "",
                "listing_date": listing_date or "",
                "market_cap": market_cap,
                "shares_outstanding": shares_out,
                "free_float": free_float_pct,
                "free_float_market_cap": (market_cap * (free_float_pct / 100.0)) if (market_cap is not None and free_float_pct is not None) else None,
                "eps_ttm": eps_ttm,
                "forward_eps": forward_eps,
                "pe_ttm": pe_ttm,
                "forward_pe": forward_pe,
                "pb": pb,
                "ps": ps,
                "ev_ebitda": ev_ebitda,
                "dividend_yield": dividend_yield,
                "dividend_rate": dividend_rate,
                "payout_ratio": payout_ratio,
                "roe": roe,
                "roa": roa,
                "net_margin": net_margin,
                "ebitda_margin": ebitda_margin,
                "revenue_growth": revenue_growth,
                "net_income_growth": net_income_growth,
                "beta": beta,
                "debt_to_equity": debt_to_equity,
                "current_ratio": current_ratio,
                "quick_ratio": quick_ratio,
                "high_52w": high_52w,
                "low_52w": low_52w,
                "ma20": ma20,
                "ma50": ma50,
                "last_updated_utc": _utc_iso(),
            }
        )

        useful = bool((out.get("name") or "").strip()) or (_safe_float(out.get("market_cap")) is not None) or (_safe_float(out.get("shares_outstanding")) is not None)
        if not useful:
            out["status"] = "error"
            out["data_quality"] = "BAD"
            out["error"] = "tadawul: fundamentals parsed but no identity/mcap/shares found"
            return out

        _fill_derived(out)
        _quality(out)

        self._fund_cache[ck] = dict(out)
        return out

    async def fetch_quote_and_fundamentals_patch(self, symbol: str) -> Dict[str, Any]:
        """
        Merge fundamentals first, then quote overwrites quote fields.
        If quote succeeds but fundamentals fails → overall success with _warn.
        If both fail → error.
        """
        f_task = self.fetch_fundamentals_patch(symbol)
        q_task = self.fetch_quote_patch(symbol)
        f, q = await asyncio.gather(f_task, q_task)

        out = _base(symbol)

        # Merge only meaningful values
        for src in (f, q):
            if not isinstance(src, dict):
                continue
            for k, v in src.items():
                if v is None:
                    continue
                if isinstance(v, str) and not v.strip():
                    continue
                out[k] = v

        f_err = (f.get("error") or "").strip() if isinstance(f, dict) else ""
        q_err = (q.get("error") or "").strip() if isinstance(q, dict) else ""

        q_ok = isinstance(q, dict) and q.get("status") != "error" and (_safe_float(q.get("current_price")) is not None)
        f_ok = isinstance(f, dict) and f.get("status") != "error" and (bool((f.get("name") or "").strip()) or (_safe_float(f.get("market_cap")) is not None))

        if not q_ok and not f_ok:
            out["status"] = "error"
            out["data_quality"] = "BAD"
            joined = " | ".join([e for e in (q_err, f_err) if e])
            out["error"] = joined or "tadawul: unknown error"
            return out

        # Non-fatal warnings (do NOT populate `error`, because that flips status)
        warns = " | ".join([e for e in (q_err, f_err) if e])
        if warns:
            out["_warn"] = warns

        _fill_derived(out)
        _quality(out)
        return out


# -----------------------------------------------------------------------------
# Lazy singleton
# -----------------------------------------------------------------------------
_CLIENT_SINGLETON: Optional[TadawulClient] = None
_CLIENT_LOCK = asyncio.Lock()


async def get_tadawul_client() -> TadawulClient:
    global _CLIENT_SINGLETON
    if _CLIENT_SINGLETON is None:
        async with _CLIENT_LOCK:
            if _CLIENT_SINGLETON is None:
                _CLIENT_SINGLETON = TadawulClient()
    return _CLIENT_SINGLETON


async def aclose_tadawul_client() -> None:
    global _CLIENT_SINGLETON
    if _CLIENT_SINGLETON is None:
        return
    try:
        await _CLIENT_SINGLETON.aclose()
    finally:
        _CLIENT_SINGLETON = None


# -----------------------------------------------------------------------------
# Engine-compatible exported callables (DICT RETURNS)
# -----------------------------------------------------------------------------
async def fetch_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    c = await get_tadawul_client()
    return await c.fetch_quote_patch(symbol)


async def fetch_enriched_quote_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    c = await get_tadawul_client()
    return await c.fetch_quote_and_fundamentals_patch(symbol)


async def fetch_quote_and_enrichment_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    c = await get_tadawul_client()
    return await c.fetch_quote_and_fundamentals_patch(symbol)


# Backwards-compatible aliases (older code may call these)
async def fetch_fundamentals_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    c = await get_tadawul_client()
    return await c.fetch_fundamentals_patch(symbol)


async def fetch_quote_and_fundamentals_patch(symbol: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    c = await get_tadawul_client()
    return await c.fetch_quote_and_fundamentals_patch(symbol)


__all__ = [
    "TadawulClient",
    "get_tadawul_client",
    "aclose_tadawul_client",
    "fetch_quote_patch",
    "fetch_fundamentals_patch",
    "fetch_enriched_quote_patch",
    "fetch_quote_and_enrichment_patch",
    "fetch_quote_and_fundamentals_patch",
    "PROVIDER_VERSION",
]
