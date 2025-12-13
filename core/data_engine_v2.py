"""
core/data_engine_v2.py
===============================================
Core Data & Analysis Engine - v2.4 (KSA-SAFE + Argaam fallback)

Author: Emad Bahbah (with GPT)

Goals
-----
- Provide a single, defensive, async engine that returns a UnifiedQuote for any symbol.
- Global symbols: prefer EODHD (fast + rich), then FMP, then optional Yahoo (yfinance).
- KSA symbols (.SR or numeric): NEVER depend on EODHD by default; use Argaam public market pages
  as primary source, with optional fallbacks (FMP / yfinance) if enabled.
- Always return a result object (data_quality FULL/PARTIAL/MISSING), never raise in normal flow.
- Include cache + sensible timeouts so Google Apps Script (UrlFetchApp ~60s) can succeed.

Notes
-----
- This module intentionally avoids tight coupling with FastAPI routes; routes should call:
    • await engine.get_enriched_quote(symbol)
    • await engine.get_enriched_quotes([symbols...])
- If BeautifulSoup is not installed, Argaam parsing will fall back to a regex-based stripper
  (less robust). For production: add `beautifulsoup4` and `lxml` to requirements.txt.
"""

from __future__ import annotations

import asyncio
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import httpx

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

try:
    from pydantic import BaseModel, Field
except Exception:  # pragma: no cover
    BaseModel = object  # type: ignore
    Field = lambda default=None, **kwargs: default  # type: ignore

# Optional Yahoo Finance fallback (may be blocked depending on Yahoo policy)
try:
    import yfinance as yf  # type: ignore
except Exception:
    yf = None  # type: ignore

# Optional HTML parsing for Argaam
try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:
    BeautifulSoup = None  # type: ignore


# =============================================================================
# Helpers
# =============================================================================

_RIYADH_TZ = ZoneInfo("Asia/Riyadh") if ZoneInfo else None


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_riyadh_iso() -> Optional[str]:
    if not _RIYADH_TZ:
        return None
    return datetime.now(timezone.utc).astimezone(_RIYADH_TZ).isoformat()


def _safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, str):
            s = x.strip()
            if not s:
                return None
            # (1.23) => -1.23
            if s.startswith("(") and s.endswith(")"):
                s = "-" + s[1:-1]
            s = s.replace(",", "")
            return float(s)
        return float(x)
    except Exception:
        return None


def _safe_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        if isinstance(x, str):
            s = x.strip()
            if not s:
                return None
            s = s.replace(",", "")
            return int(float(s))
        return int(x)
    except Exception:
        return None


def _coalesce(*vals):
    for v in vals:
        if v is not None:
            return v
    return None


def _strip_html_fallback(html: str) -> str:
    # Basic fallback for environments without bs4.
    # Not perfect, but good enough to extract Argaam table text in many cases.
    html = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html)
    html = re.sub(r"(?is)<br\s*/?>", "\n", html)
    html = re.sub(r"(?is)</(p|div|tr|li|h\d)>", "\n", html)
    html = re.sub(r"(?is)<.*?>", " ", html)
    html = re.sub(r"[\t\r]+", " ", html)
    return html


def is_ksa_symbol(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    if not s:
        return False
    if s.endswith(".SR") or s.endswith(".TADAWUL"):
        return True
    # Tadawul numeric (4 digits) is also accepted in some inputs
    if re.fullmatch(r"\d{3,5}", s):
        return True
    return False


def normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip()
    if not s:
        return s
    s = s.replace(" ", "")
    if s.upper().endswith(".TADAWUL"):
        s = s[:-7] + ".SR"
    return s


def ksa_numeric(symbol: str) -> Optional[str]:
    s = normalize_symbol(symbol).upper()
    if s.endswith(".SR"):
        s = s[:-3]
    if re.fullmatch(r"\d{3,5}", s):
        return s
    return None


def normalize_eodhd_symbol(symbol: str) -> str:
    """
    EODHD uses exchange suffixes (e.g., AAPL.US). In the dashboard, users often use AAPL.
    Heuristic:
      - If already contains '.', keep as-is.
      - If looks like a plain equity ticker, default to .US
    """
    s = normalize_symbol(symbol).upper()
    if not s:
        return s
    if "." in s or "=" in s or "^" in s:
        return s
    if re.fullmatch(r"[A-Z]{1,6}", s):
        return s + ".US"
    return s


# =============================================================================
# UnifiedQuote model (fields aligned to the 59-column dashboard template)
# =============================================================================

class UnifiedQuote(BaseModel):
    # Identity
    symbol: str = Field(...)
    name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    market: Optional[str] = None
    currency: Optional[str] = None

    # Prices
    current_price: Optional[float] = None
    previous_close: Optional[float] = None
    price_change: Optional[float] = None
    percent_change: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    high_52w: Optional[float] = None
    low_52w: Optional[float] = None

    # Volume & liquidity
    volume: Optional[float] = None
    avg_volume_30d: Optional[float] = None
    value_traded: Optional[float] = None

    # Shares / market cap
    shares_outstanding: Optional[float] = None
    free_float: Optional[float] = None
    market_cap: Optional[float] = None

    # Fundamentals
    eps_ttm: Optional[float] = None
    forward_eps: Optional[float] = None
    pe_ttm: Optional[float] = None
    forward_pe: Optional[float] = None
    pb: Optional[float] = None
    ps: Optional[float] = None
    ev_ebitda: Optional[float] = None
    dividend_yield: Optional[float] = None
    payout_ratio: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    net_margin: Optional[float] = None
    ebitda_margin: Optional[float] = None
    revenue_growth: Optional[float] = None
    net_income_growth: Optional[float] = None

    # Risk
    debt_to_equity: Optional[float] = None
    current_ratio: Optional[float] = None
    quick_ratio: Optional[float] = None
    beta: Optional[float] = None
    volatility_30d: Optional[float] = None

    # Technicals
    rsi_14: Optional[float] = None
    macd: Optional[float] = None
    ma20: Optional[float] = None
    ma50: Optional[float] = None

    # Valuation & scoring
    fair_value: Optional[float] = None
    upside_percent: Optional[float] = None
    valuation_label: Optional[str] = None

    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    overall_score: Optional[float] = None

    target_price: Optional[float] = None
    recommendation: Optional[str] = None

    # Meta
    data_source: Optional[str] = None
    data_quality: str = "MISSING"  # FULL / PARTIAL / MISSING
    last_updated_utc: Optional[str] = None
    last_updated_riyadh: Optional[str] = None
    error: Optional[str] = None

    def _default_scores_if_missing(self) -> None:
        if self.value_score is None:
            self.value_score = 50.0
        if self.quality_score is None:
            self.quality_score = 50.0
        if self.momentum_score is None:
            self.momentum_score = 50.0
        if self.overall_score is None:
            self.overall_score = float((self.value_score + self.quality_score + self.momentum_score) / 3.0)
        if self.opportunity_score is None:
            self.opportunity_score = float(_coalesce(self.overall_score, 50.0))

        if not self.recommendation:
            s = float(self.overall_score or 50.0)
            if s >= 80:
                self.recommendation = "STRONG BUY"
            elif s >= 65:
                self.recommendation = "BUY"
            elif s >= 45:
                self.recommendation = "HOLD"
            elif s >= 30:
                self.recommendation = "REDUCE"
            else:
                self.recommendation = "SELL"

    def finalize(self) -> "UnifiedQuote":
        if not self.last_updated_utc:
            self.last_updated_utc = _now_utc().isoformat()
        if not self.last_updated_riyadh:
            self.last_updated_riyadh = _now_riyadh_iso()
        self._default_scores_if_missing()
        return self

    def to_sheet_row(self, headers: List[str]) -> List[Any]:
        m = {
            "Symbol": self.symbol,
            "Company Name": self.name,
            "Sector": self.sector,
            "Industry": self.industry,
            "Market": self.market,
            "Currency": self.currency,
            "Current Price": self.current_price,
            "Previous Close": self.previous_close,
            "Price Change": self.price_change,
            "Percent Change": self.percent_change,
            "Day High": self.day_high,
            "Day Low": self.day_low,
            "52W High": self.high_52w,
            "52W Low": self.low_52w,
            "Volume": self.volume,
            "Avg Volume (30D)": self.avg_volume_30d,
            "Value Traded": self.value_traded,
            "Shares Outstanding": self.shares_outstanding,
            "Free Float %": self.free_float,
            "Market Cap": self.market_cap,
            "EPS (TTM)": self.eps_ttm,
            "Forward EPS": self.forward_eps,
            "P/E (TTM)": self.pe_ttm,
            "Forward P/E": self.forward_pe,
            "P/B": self.pb,
            "P/S": self.ps,
            "EV/EBITDA": self.ev_ebitda,
            "Dividend Yield %": self.dividend_yield,
            "Payout Ratio %": self.payout_ratio,
            "ROE %": self.roe,
            "ROA %": self.roa,
            "Net Margin %": self.net_margin,
            "EBITDA Margin %": self.ebitda_margin,
            "Revenue Growth %": self.revenue_growth,
            "Net Income Growth %": self.net_income_growth,
            "Debt/Equity": self.debt_to_equity,
            "Current Ratio": self.current_ratio,
            "Quick Ratio": self.quick_ratio,
            "Beta": self.beta,
            "Volatility (30D)": self.volatility_30d,
            "RSI (14)": self.rsi_14,
            "MACD": self.macd,
            "MA20": self.ma20,
            "MA50": self.ma50,
            "Fair Value": self.fair_value,
            "Upside %": self.upside_percent,
            "Valuation Label": self.valuation_label,
            "Value Score": self.value_score,
            "Quality Score": self.quality_score,
            "Momentum Score": self.momentum_score,
            "Opportunity Score": self.opportunity_score,
            "Overall Score": self.overall_score,
            "Target Price": self.target_price,
            "Recommendation": self.recommendation,
            "Data Source": self.data_source,
            "Data Quality": self.data_quality,
            "Last Updated (UTC)": self.last_updated_utc,
            "Last Updated (Riyadh)": self.last_updated_riyadh,
            "Error": self.error,
        }
        return [m.get(h) for h in headers]


# =============================================================================
# Lightweight TTL Cache
# =============================================================================

@dataclass
class _CacheEntry:
    value: Any
    expires_at: float


class TTLCache:
    def __init__(self, default_ttl_sec: float = 30.0, max_items: int = 2048):
        self.default_ttl = float(default_ttl_sec)
        self.max_items = int(max_items)
        self._data: Dict[str, _CacheEntry] = {}

    def get(self, key: str) -> Any:
        e = self._data.get(key)
        if not e:
            return None
        if e.expires_at < time.time():
            self._data.pop(key, None)
            return None
        return e.value

    def set(self, key: str, value: Any, ttl_sec: Optional[float] = None) -> None:
        if len(self._data) >= self.max_items:
            self._data.clear()
        ttl = self.default_ttl if ttl_sec is None else float(ttl_sec)
        self._data[key] = _CacheEntry(value=value, expires_at=time.time() + ttl)

    def clear(self) -> None:
        self._data.clear()


# =============================================================================
# Providers
# =============================================================================

class ArgaamProvider:
    """
    Primary KSA provider using Argaam public market pages (no API key).
    We scrape the market "companies-volume" page and extract a row per symbol.
    """
    BASE = "https://www.argaam.com/en/company/companies-volume/{market_id}"

    def __init__(self, client: httpx.AsyncClient, cache: TTLCache, ttl_sec: float = 30.0):
        self._client = client
        self._cache = cache
        self._ttl = float(ttl_sec)

    async def get_snapshot(self, market_id: int = 3) -> Dict[str, Dict[str, Any]]:
        cache_key = f"argaam:snapshot:{market_id}"
        cached = self._cache.get(cache_key)
        if isinstance(cached, dict) and cached:
            return cached

        url = self.BASE.format(market_id=market_id)
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; TadawulFastBridge/1.0; +https://example.com)",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        resp = await self._client.get(url, headers=headers)
        resp.raise_for_status()
        html = resp.text

        if BeautifulSoup:
            soup = BeautifulSoup(html, "lxml" if "lxml" in (os.getenv("BS4_PARSER", "lxml").lower()) else "html.parser")
            text = soup.get_text("\n")
        else:
            text = _strip_html_fallback(html)

        lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines()]
        lines = [ln for ln in lines if ln and re.match(r"^\d{3,5}\s+", ln)]

        out: Dict[str, Dict[str, Any]] = {}
        for ln in lines:
            parts = ln.split(" ")
            if len(parts) < 7:
                continue

            sym = parts[0]
            if not re.fullmatch(r"\d{3,5}", sym):
                continue

            avg_vol = parts[-3]
            vol = parts[-2]
            vol_chg = parts[-1]
            price = parts[-5]
            chg = parts[-4]

            price_f = _safe_float(price)
            chg_f = _safe_float(chg)
            avg_vol_f = _safe_float(avg_vol)
            vol_i = _safe_int(vol)
            vol_chg_f = _safe_float(vol_chg)

            mid = parts[1:-5]
            short_name = None
            company = " ".join(mid).strip() or None
            if mid:
                last = mid[-1]
                if re.fullmatch(r"[A-Z0-9\.\-]{2,}", last):
                    short_name = last
                    company = " ".join(mid[:-1]).strip() or company

            out[sym] = {
                "symbol": sym,
                "company": company,
                "short_name": short_name,
                "price": price_f,
                "change_percent": chg_f,
                "avg_volume_3m": avg_vol_f,
                "volume": vol_i,
                "volume_change_percent": vol_chg_f,
            }

        self._cache.set(cache_key, out, ttl_sec=self._ttl)
        return out

    async def quote_ksa(self, symbol: str) -> Optional[UnifiedQuote]:
        code = ksa_numeric(symbol)
        if not code:
            return None

        snap = await self.get_snapshot(market_id=3)
        row = snap.get(code)
        if not row:
            return None

        q = UnifiedQuote(
            symbol=normalize_symbol(symbol),
            name=row.get("company"),
            market="KSA",
            currency="SAR",
            current_price=row.get("price"),
            percent_change=row.get("change_percent"),
            volume=row.get("volume"),
            avg_volume_30d=row.get("avg_volume_3m"),
            data_source="argaam",
            data_quality="PARTIAL",
        ).finalize()
        return q


class EODHDProvider:
    BASE = "https://eodhd.com/api"

    def __init__(self, client: httpx.AsyncClient, api_key: Optional[str], cache: TTLCache):
        self._client = client
        self._key = api_key
        self._cache = cache

    def enabled(self) -> bool:
        return bool(self._key)

    async def realtime(self, symbol: str) -> Dict[str, Any]:
        if not self._key:
            return {}
        sym = normalize_eodhd_symbol(symbol)
        url = f"{self.BASE}/real-time/{sym}"
        params = {"api_token": self._key, "fmt": "json"}
        r = await self._client.get(url, params=params)
        r.raise_for_status()
        return r.json()

    async def fundamentals(self, symbol: str) -> Dict[str, Any]:
        if not self._key:
            return {}
        sym = normalize_eodhd_symbol(symbol)
        cache_key = f"eodhd:fund:{sym}"
        cached = self._cache.get(cache_key)
        if cached:
            return cached
        url = f"{self.BASE}/fundamentals/{sym}"
        params = {"api_token": self._key, "fmt": "json"}
        r = await self._client.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        self._cache.set(cache_key, data, ttl_sec=float(os.getenv("FUNDAMENTALS_TTL_SEC", "21600")))
        return data


class FMPProvider:
    BASE = "https://financialmodelingprep.com/api/v3"

    def __init__(self, client: httpx.AsyncClient, api_key: Optional[str], cache: TTLCache):
        self._client = client
        self._key = api_key
        self._cache = cache

    def enabled(self) -> bool:
        return bool(self._key)

    async def quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self._key:
            return None
        sym = normalize_symbol(symbol).upper()
        url = f"{self.BASE}/quote/{sym}"
        r = await self._client.get(url, params={"apikey": self._key})
        if r.status_code >= 400:
            return None
        js = r.json()
        if isinstance(js, list) and js:
            return js[0]
        return None


# =============================================================================
# DataEngine
# =============================================================================

class DataEngine:
    """
    Unified engine.
    """

    def __init__(self):
        timeout = float(os.getenv("HTTP_TIMEOUT_SEC", "8"))
        limits = httpx.Limits(
            max_connections=int(os.getenv("HTTP_MAX_CONNECTIONS", "50")),
            max_keepalive_connections=int(os.getenv("HTTP_MAX_KEEPALIVE", "20")),
        )
        self._client = httpx.AsyncClient(timeout=timeout, limits=limits, follow_redirects=True)

        self._cache = TTLCache(default_ttl_sec=float(os.getenv("CACHE_TTL_SEC", "20")), max_items=4096)

        self._argaam = ArgaamProvider(self._client, self._cache, ttl_sec=float(os.getenv("ARGAAM_SNAPSHOT_TTL_SEC", "30")))
        self._eodhd = EODHDProvider(self._client, os.getenv("EODHD_API_KEY") or os.getenv("EODHD_KEY"), self._cache)
        self._fmp = FMPProvider(self._client, os.getenv("FMP_API_KEY") or os.getenv("FMP_KEY"), self._cache)

        self._allow_eodhd_ksa = (os.getenv("ALLOW_EODHD_KSA", "0").strip() == "1")
        self._sem = asyncio.Semaphore(int(os.getenv("ENGINE_MAX_CONCURRENCY", "20")))

    async def aclose(self) -> None:
        await self._client.aclose()

    async def get_enriched_quote(self, symbol: str) -> UnifiedQuote:
        sym = normalize_symbol(symbol)
        if not sym:
            return UnifiedQuote(symbol=symbol or "", data_quality="MISSING", error="Empty symbol").finalize()

        cache_key = f"quote:{sym.upper()}"
        cached = self._cache.get(cache_key)
        if isinstance(cached, UnifiedQuote):
            return cached

        async with self._sem:
            q = await self._build_quote(sym)

        self._cache.set(cache_key, q, ttl_sec=float(os.getenv("QUOTE_TTL_SEC", "20")))
        return q

    async def get_enriched_quotes(self, symbols: Iterable[str]) -> List[UnifiedQuote]:
        syms = [normalize_symbol(s) for s in (symbols or []) if (s or "").strip()]
        if not syms:
            return []
        tasks = [asyncio.create_task(self.get_enriched_quote(s)) for s in syms]
        return await asyncio.gather(*tasks)

    async def _build_quote(self, symbol: str) -> UnifiedQuote:
        if is_ksa_symbol(symbol):
            q = await self._build_ksa_quote(symbol)
            return q.finalize()
        q = await self._build_global_quote(symbol)
        return q.finalize()

    async def _build_ksa_quote(self, symbol: str) -> UnifiedQuote:
        argaam_err = None
        try:
            q = await self._argaam.quote_ksa(symbol)
            if q and q.current_price is not None:
                return q
        except Exception as e:
            argaam_err = f"Argaam error: {type(e).__name__}: {e}"

        if self._allow_eodhd_ksa and self._eodhd.enabled():
            try:
                rt = await self._eodhd.realtime(symbol)
                q = UnifiedQuote(
                    symbol=normalize_symbol(symbol),
                    market="KSA",
                    currency="SAR",
                    current_price=_safe_float(rt.get("close")),
                    previous_close=_safe_float(rt.get("previousClose")),
                    price_change=_safe_float(rt.get("change")),
                    percent_change=_safe_float(rt.get("change_p")),
                    day_high=_safe_float(rt.get("high")),
                    day_low=_safe_float(rt.get("low")),
                    volume=_safe_float(rt.get("volume")),
                    data_source="eodhd",
                    data_quality="PARTIAL",
                    error=argaam_err,
                )
                return q
            except Exception as e:
                argaam_err = (argaam_err or "") + f" | EODHD-KSA error: {type(e).__name__}: {e}"

        if self._fmp.enabled():
            try:
                fq = await self._fmp.quote(symbol)
                if fq:
                    q = UnifiedQuote(
                        symbol=symbol,
                        name=fq.get("name"),
                        market="KSA",
                        currency="SAR",
                        current_price=_safe_float(fq.get("price")),
                        previous_close=_safe_float(fq.get("previousClose")),
                        price_change=_safe_float(fq.get("change")),
                        percent_change=_safe_float(fq.get("changesPercentage")),
                        day_high=_safe_float(fq.get("dayHigh")),
                        day_low=_safe_float(fq.get("dayLow")),
                        volume=_safe_float(fq.get("volume")),
                        market_cap=_safe_float(fq.get("marketCap")),
                        data_source="fmp",
                        data_quality="PARTIAL",
                        error=argaam_err,
                    )
                    return q
            except Exception as e:
                argaam_err = (argaam_err or "") + f" | FMP error: {type(e).__name__}: {e}"

        if yf is not None and os.getenv("ENABLE_YFINANCE", "0").strip() == "1":
            try:
                y = yf.Ticker(symbol)
                info = getattr(y, "fast_info", None) or {}
                price = _safe_float(getattr(info, "last_price", None) or info.get("lastPrice"))
                q = UnifiedQuote(
                    symbol=symbol,
                    market="KSA",
                    currency="SAR",
                    current_price=price,
                    data_source="yfinance",
                    data_quality="PARTIAL" if price is not None else "MISSING",
                    error=argaam_err,
                )
                return q
            except Exception as e:
                argaam_err = (argaam_err or "") + f" | yfinance error: {type(e).__name__}: {e}"

        return UnifiedQuote(
            symbol=symbol,
            market="KSA",
            currency="SAR",
            data_source="argaam",
            data_quality="MISSING",
            error=argaam_err or "No KSA provider returned data",
        )

    async def _build_global_quote(self, symbol: str) -> UnifiedQuote:
        eod_err = None
        if self._eodhd.enabled():
            try:
                rt = await self._eodhd.realtime(symbol)
                q = UnifiedQuote(
                    symbol=normalize_symbol(symbol),
                    market="GLOBAL",
                    currency="USD",
                    current_price=_safe_float(rt.get("close")),
                    previous_close=_safe_float(rt.get("previousClose")),
                    price_change=_safe_float(rt.get("change")),
                    percent_change=_safe_float(rt.get("change_p")),
                    day_high=_safe_float(rt.get("high")),
                    day_low=_safe_float(rt.get("low")),
                    volume=_safe_float(rt.get("volume")),
                    data_source="eodhd",
                    data_quality="PARTIAL",
                )
                if os.getenv("EODHD_FETCH_FUNDAMENTALS", "1").strip() == "1":
                    try:
                        f = await self._eodhd.fundamentals(symbol)
                        self._apply_eodhd_fundamentals(q, f)
                        q.data_quality = "FULL" if q.current_price is not None else "MISSING"
                    except Exception as e:
                        q.error = f"EODHD fundamentals error: {type(e).__name__}: {e}"
                return q
            except Exception as e:
                eod_err = f"EODHD error: {type(e).__name__}: {e}"
        else:
            eod_err = "EODHD not configured"

        if self._fmp.enabled():
            try:
                fq = await self._fmp.quote(symbol)
                if fq:
                    q = UnifiedQuote(
                        symbol=symbol,
                        name=fq.get("name"),
                        market="GLOBAL",
                        currency="USD",
                        current_price=_safe_float(fq.get("price")),
                        previous_close=_safe_float(fq.get("previousClose")),
                        price_change=_safe_float(fq.get("change")),
                        percent_change=_safe_float(fq.get("changesPercentage")),
                        day_high=_safe_float(fq.get("dayHigh")),
                        day_low=_safe_float(fq.get("dayLow")),
                        high_52w=_safe_float(fq.get("yearHigh")),
                        low_52w=_safe_float(fq.get("yearLow")),
                        volume=_safe_float(fq.get("volume")),
                        market_cap=_safe_float(fq.get("marketCap")),
                        pe_ttm=_safe_float(fq.get("pe")),
                        eps_ttm=_safe_float(fq.get("eps")),
                        data_source="fmp",
                        data_quality="PARTIAL",
                        error=eod_err,
                    )
                    return q
            except Exception as e:
                eod_err = (eod_err or "") + f" | FMP error: {type(e).__name__}: {e}"

        return UnifiedQuote(symbol=symbol, market="GLOBAL", data_source="none", data_quality="MISSING", error=eod_err)

    def _apply_eodhd_fundamentals(self, q: UnifiedQuote, f: Dict[str, Any]) -> None:
        general = (f or {}).get("General") or {}
        highlights = (f or {}).get("Highlights") or {}
        valuation = (f or {}).get("Valuation") or {}
        shares = (f or {}).get("SharesStats") or {}
        tech = (f or {}).get("Technicals") or {}

        q.name = q.name or general.get("Name")
        q.sector = q.sector or general.get("Sector")
        q.industry = q.industry or general.get("Industry")
        q.currency = q.currency or general.get("CurrencyCode")

        q.high_52w = _safe_float(tech.get("52WeekHigh")) or _safe_float(highlights.get("52WeekHigh")) or q.high_52w
        q.low_52w = _safe_float(tech.get("52WeekLow")) or _safe_float(highlights.get("52WeekLow")) or q.low_52w

        q.market_cap = _safe_float(highlights.get("MarketCapitalization")) or q.market_cap
        q.shares_outstanding = _safe_float(shares.get("SharesOutstanding")) or q.shares_outstanding
        q.free_float = _safe_float(shares.get("FloatShares")) or q.free_float

        q.eps_ttm = _safe_float(highlights.get("EarningsShare")) or q.eps_ttm
        q.forward_eps = _safe_float(highlights.get("EPSEstimateNextYear")) or q.forward_eps
        q.pe_ttm = _safe_float(highlights.get("PERatio")) or q.pe_ttm
        q.forward_pe = _safe_float(highlights.get("ForwardPE")) or q.forward_pe
        q.pb = _safe_float(valuation.get("PriceBookMRQ")) or q.pb
        q.ps = _safe_float(valuation.get("PriceSalesTTM")) or q.ps
        q.ev_ebitda = _safe_float(valuation.get("EnterpriseValueEbitda")) or q.ev_ebitda
