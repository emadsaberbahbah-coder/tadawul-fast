from __future__ import annotations

import asyncio
import logging
import math
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

import httpx
from cachetools import TTLCache
from pydantic import BaseModel, ConfigDict, Field

from core.config import get_settings

# Optional deps (graceful fallback)
try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover
    BeautifulSoup = None

try:
    import yfinance as yf
except Exception:  # pragma: no cover
    yf = None

settings = get_settings()
logger = logging.getLogger("core.data_engine_v2")

# -----------------------------------------------------------------------------
# Provider URLs
# -----------------------------------------------------------------------------
EODHD_RT_URL = "https://eodhd.com/api/real-time/{symbol}"
EODHD_FUND_URL = "https://eodhd.com/api/fundamentals/{symbol}"
FMP_QUOTE_URL = "https://financialmodelingprep.com/api/v3/quote/{symbol}"

# Argaam (best-effort; HTML can change)
ARGAAM_PRICES_URLS = [
    "https://www.argaam.com/ar/company/companies-prices/3",  # TASI (Arabic)
    "https://www.argaam.com/en/company/companies-prices/3",  # TASI (English)
]
ARGAAM_VOLUME_URLS = [
    "https://www.argaam.com/ar/company/companies-volume/14",  # often "Most Active"
    "https://www.argaam.com/en/company/companies-volume/14",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Riyadh is UTC+3 (no DST)
RIYADH_TZ = timezone(timedelta(hours=3))

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _now_riyadh_iso() -> str:
    return datetime.now(RIYADH_TZ).isoformat()

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

        # Normalize Arabic digits and separators
        s = s.translate(_ARABIC_DIGITS)
        s = s.replace("٬", ",").replace("٫", ".")  # Arabic thousands/decimal
        s = s.replace(",", "")
        s = s.replace("%", "")
        s = s.replace("+", "")
        s = s.replace("SAR", "").replace("USD", "").strip()

        # Handle parentheses negatives: (1.23)
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        f = float(s)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None

def normalize_symbol(raw: str) -> str:
    """
    Standardizes input to a provider-friendly format:

    - KSA numeric => 1120.SR
    - If already contains a dot suffix => keep as-is (upper)
    - Special Yahoo-style symbols (^GSPC, GC=F, EURUSD=X) => keep as-is (upper)
    - Otherwise => default to .US (AAPL => AAPL.US)
    """
    s = (raw or "").strip().upper()
    if not s:
        return ""

    # Common prefixes
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "")

    # Yahoo-style symbols or pairs
    if any(ch in s for ch in ("=", "^")):
        return s

    # Saudi numeric
    if s.isdigit():
        return f"{s}.SR"

    # Already has suffix (.US, .SR, .L, BRK.B, etc.)
    if "." in s:
        return s

    return f"{s}.US"

def is_ksa_symbol(symbol: str) -> bool:
    s = (symbol or "").upper().strip()
    return s.endswith(".SR") or s.isdigit()

def _fmp_symbol(symbol: str) -> str:
    """
    FMP often expects tickers without '.US' if we auto-added it.
    Keep other dots (e.g. BRK.B) untouched.
    """
    s = (symbol or "").strip().upper()
    return s[:-3] if s.endswith(".US") else s

# -----------------------------------------------------------------------------
# Data model (Pydantic v2)
# -----------------------------------------------------------------------------
class UnifiedQuote(BaseModel):
    """
    UnifiedQuote: master model aligned with core.enriched_quote.EnrichedQuote
    and the 9-page Google Sheets dashboard.
    """
    model_config = ConfigDict(populate_by_name=True, from_attributes=True, validate_assignment=True)

    # Identity
    symbol: str
    name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    sub_sector: Optional[str] = None
    market: str = "UNKNOWN"         # KSA / GLOBAL / UNKNOWN
    currency: Optional[str] = None
    listing_date: Optional[str] = None

    # Shares / Float / Cap
    shares_outstanding: Optional[float] = None
    free_float: Optional[float] = None
    market_cap: Optional[float] = None
    free_float_market_cap: Optional[float] = None

    # Prices
    current_price: Optional[float] = None
    previous_close: Optional[float] = None
    open: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    high_52w: Optional[float] = None
    low_52w: Optional[float] = None
    position_52w_percent: Optional[float] = None
    price_change: Optional[float] = None
    percent_change: Optional[float] = None

    # Volume / Liquidity
    volume: Optional[float] = None
    avg_volume_30d: Optional[float] = None
    value_traded: Optional[float] = None
    turnover_percent: Optional[float] = None
    liquidity_score: Optional[float] = None

    # Fundamentals
    eps_ttm: Optional[float] = None
    forward_eps: Optional[float] = None
    pe_ttm: Optional[float] = None
    forward_pe: Optional[float] = None
    pb: Optional[float] = None
    ps: Optional[float] = None
    ev_ebitda: Optional[float] = None
    dividend_yield: Optional[float] = None
    dividend_rate: Optional[float] = None
    payout_ratio: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    net_margin: Optional[float] = None
    ebitda_margin: Optional[float] = None
    revenue_growth: Optional[float] = None
    net_income_growth: Optional[float] = None
    beta: Optional[float] = None
    debt_to_equity: Optional[float] = None
    current_ratio: Optional[float] = None
    quick_ratio: Optional[float] = None

    # Technicals
    rsi_14: Optional[float] = None
    volatility_30d: Optional[float] = None
    macd: Optional[float] = None
    ma20: Optional[float] = None
    ma50: Optional[float] = None

    # Valuation / Targets
    fair_value: Optional[float] = None
    target_price: Optional[float] = None
    upside_percent: Optional[float] = None
    valuation_label: Optional[str] = None
    analyst_rating: Optional[str] = None

    # Scores / Recommendation
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    overall_score: Optional[float] = None
    recommendation: Optional[str] = None
    confidence: Optional[float] = None
    risk_score: Optional[float] = None

    # Meta
    data_source: str = "none"
    data_quality: str = "MISSING"   # FULL / PARTIAL / MISSING
    last_updated_utc: str = Field(default_factory=_now_utc_iso)
    last_updated_riyadh: str = Field(default_factory=_now_riyadh_iso)
    error: Optional[str] = None

    # -------------------------------------------------------------------------
    # Finalization helpers
    # -------------------------------------------------------------------------
    def finalize(self) -> "UnifiedQuote":
        # Timestamps
        if not self.last_updated_utc:
            self.last_updated_utc = _now_utc_iso()
        if not self.last_updated_riyadh:
            self.last_updated_riyadh = _now_riyadh_iso()

        # Derive market/currency defaults
        if not self.market:
            self.market = "KSA" if is_ksa_symbol(self.symbol) else "GLOBAL"
        if not self.currency:
            self.currency = "SAR" if self.market == "KSA" else "USD"

        # Derive change fields
        if self.current_price is not None and self.previous_close is not None:
            if self.price_change is None:
                self.price_change = _safe_float(self.current_price - self.previous_close)
            if self.percent_change is None and self.previous_close not in (0, None):
                self.percent_change = _safe_float((self.current_price - self.previous_close) / self.previous_close * 100.0)

        # 52w position
        if (
            self.position_52w_percent is None
            and self.current_price is not None
            and self.high_52w is not None
            and self.low_52w is not None
        ):
            rng = self.high_52w - self.low_52w
            if rng and rng > 0:
                self.position_52w_percent = _safe_float((self.current_price - self.low_52w) / rng * 100.0)

        # Value traded
        if self.value_traded is None and self.current_price is not None and self.volume is not None:
            self.value_traded = _safe_float(self.current_price * self.volume)

        # Data quality heuristic
        if self.current_price is None:
            self.data_quality = "MISSING"
        else:
            have_core = all(x is not None for x in (self.current_price, self.previous_close, self.day_high, self.day_low))
            self.data_quality = "FULL" if have_core else "PARTIAL"

        # Ensure recommendation exists (basic fallback if scoring isn't applied)
        if self.overall_score is None:
            self._calculate_simple_scores()

        return self

    def _calculate_simple_scores(self) -> None:
        """
        Very simple fallback score so Sheets always has a value even when
        providers return only partial data. This does NOT replace scoring_engine.
        """
        score = 50.0
        if self.percent_change is not None:
            score += 0.8 * max(-10.0, min(10.0, self.percent_change))
        if self.pe_ttm is not None:
            if 0 < self.pe_ttm < 15:
                score += 8
            elif self.pe_ttm > 35:
                score -= 8
        if self.dividend_yield is not None and self.dividend_yield > 0:
            score += min(6.0, self.dividend_yield * 2.0)

        score = max(0.0, min(100.0, score))
        self.overall_score = score

        if self.recommendation:
            return
        if score >= 80:
            self.recommendation = "STRONG BUY"
        elif score >= 65:
            self.recommendation = "BUY"
        elif score <= 35:
            self.recommendation = "SELL"
        else:
            self.recommendation = "HOLD"

# -----------------------------------------------------------------------------
# Engine
# -----------------------------------------------------------------------------
class DataEngine:
    """
    Async multi-provider engine with KSA-safe routing:
      - KSA (.SR): Argaam snapshot (best-effort). No EODHD for KSA.
      - Global   : EODHD (real-time + optional fundamentals) -> FMP -> yfinance
    """

    def __init__(self) -> None:
        self.enabled_providers: List[str] = [
            p.strip().lower() for p in (settings.ENABLED_PROVIDERS or []) if str(p).strip()
        ]
        if isinstance(settings.ENABLED_PROVIDERS, str):
            self.enabled_providers = [p.strip().lower() for p in settings.ENABLED_PROVIDERS.split(",") if p.strip()]

        self.client = httpx.AsyncClient(
            timeout=settings.HTTP_TIMEOUT_SEC,
            follow_redirects=True,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.8,ar;q=0.6",
            },
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=40),
        )

        # Caches
        self.quote_cache: TTLCache = TTLCache(maxsize=3000, ttl=20)     # live-ish
        self.stale_cache: TTLCache = TTLCache(maxsize=5000, ttl=300)    # fallback for outages
        self.fund_cache: TTLCache = TTLCache(maxsize=1500, ttl=21600)   # 6 hours
        self.snapshot_cache: TTLCache = TTLCache(maxsize=50, ttl=25)    # argaam snapshots

        # Concurrency guard
        self._sem = asyncio.Semaphore(10)

    async def aclose(self) -> None:
        await self.client.aclose()

    # -------------------------------------------------------------------------
    # Public API (expected by routers)
    # -------------------------------------------------------------------------
    async def get_quote(self, symbol: str) -> UnifiedQuote:
        s = normalize_symbol(symbol)
        if not s:
            return UnifiedQuote(symbol=str(symbol or ""), data_quality="MISSING", error="Empty symbol").finalize()

        # Hot cache
        hit = self.quote_cache.get(s)
        if hit is not None:
            return hit

        # If we fail live, use stale
        stale = self.stale_cache.get(s)

        try:
            async with self._sem:
                if is_ksa_symbol(s):
                    q = await self._fetch_ksa(s)
                else:
                    q = await self._fetch_global(s)

            q = q.finalize()

            # Optional: apply scoring (no hard dependency)
            try:
                from core.scoring_engine import enrich_with_scores  # type: ignore
                q = enrich_with_scores(q)
            except Exception:
                pass

            self.quote_cache[s] = q
            self.stale_cache[s] = q
            return q

        except Exception as exc:
            logger.exception("get_quote failed for %s", s, exc_info=exc)
            if stale is not None:
                stale2 = stale.model_copy(update={"error": f"Live fetch failed; returned stale. {exc}"})
                return stale2.finalize()
            return UnifiedQuote(symbol=s, data_quality="MISSING", error=str(exc)).finalize()

    async def get_quotes(self, symbols: Sequence[str]) -> List[UnifiedQuote]:
        tasks = [self.get_quote(s) for s in (symbols or []) if s and str(s).strip()]
        if not tasks:
            return []
        return await asyncio.gather(*tasks)

    # Backward-compatible names used across routers
    async def get_enriched_quote(self, symbol: str) -> UnifiedQuote:
        return await self.get_quote(symbol)

    async def get_enriched_quotes(self, symbols: Sequence[str]) -> List[UnifiedQuote]:
        return await self.get_quotes(symbols)

    # -------------------------------------------------------------------------
    # KSA: Argaam
    # -------------------------------------------------------------------------
    async def _fetch_ksa(self, symbol: str) -> UnifiedQuote:
        # IMPORTANT: never use EODHD for KSA (EODHD doesn't reliably support .SR here)
        base = symbol.split(".", 1)[0].strip()
        snap = await self._argaam_snapshot()
        row = snap.get(base)

        if row:
            return UnifiedQuote(
                symbol=symbol,
                name=row.get("name"),
                market="KSA",
                currency="SAR",
                current_price=row.get("price"),
                price_change=row.get("change"),
                percent_change=row.get("change_percent"),
                volume=row.get("volume"),
                value_traded=row.get("value_traded"),
                data_source="argaam",
                data_quality="PARTIAL",
            )

        # Optional last resort: yfinance (often unreliable for .SR)
        if settings.ENABLE_YFINANCE and "yfinance" in self.enabled_providers and yf:
            q = await self._fetch_yfinance(symbol, market="KSA")
            if q.current_price is not None:
                q.data_source = q.data_source or "yfinance"
                q.data_quality = "PARTIAL"
                return q

        return UnifiedQuote(symbol=symbol, market="KSA", currency="SAR", data_quality="MISSING", error="No KSA provider data").finalize()

    async def _argaam_snapshot(self) -> Dict[str, Dict[str, Any]]:
        """
        Returns cached map: code -> {name, price, change, change_percent, volume, value_traded}
        """
        key = "argaam_snapshot_v1"
        cached = self.snapshot_cache.get(key)
        if isinstance(cached, dict) and cached:
            return cached

        merged: Dict[str, Dict[str, Any]] = {}

        for url in ARGAAM_PRICES_URLS:
            html = await self._http_get_text(url)
            if not html:
                continue
            part = self._parse_argaam_table(html)
            if part:
                for k, v in part.items():
                    merged.setdefault(k, {}).update(v)
                break

        for url in ARGAAM_VOLUME_URLS:
            html = await self._http_get_text(url)
            if not html:
                continue
            part = self._parse_argaam_table(html)
            if part:
                for k, v in part.items():
                    merged.setdefault(k, {}).update(v)
                break

        self.snapshot_cache[key] = merged
        return merged

    def _parse_argaam_table(self, html: str) -> Dict[str, Dict[str, Any]]:
        """
        Best-effort parser that finds rows where the first cell is a numeric code.
        """
        out: Dict[str, Dict[str, Any]] = {}
        if not BeautifulSoup or not html:
            return out

        soup = BeautifulSoup(html, "lxml")
        rows = soup.find_all("tr")
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 5:
                continue

            txt = [c.get_text(" ", strip=True) for c in cols]
            code = (txt[0] or "").strip().translate(_ARABIC_DIGITS)
            if not re.match(r"^\d{3,6}$", code):
                continue

            name = txt[1].strip() if len(txt) > 1 else None
            price = _safe_float(txt[2]) if len(txt) > 2 else None
            change = _safe_float(txt[3]) if len(txt) > 3 else None
            chg_p = _safe_float(txt[4]) if len(txt) > 4 else None

            volume = None
            value_traded = None
            if len(txt) >= 9:
                volume = _safe_float(txt[-2]) or _safe_float(txt[-1])
                value_traded = _safe_float(txt[-3]) if len(txt) >= 3 else None

            out[code] = {
                "name": name,
                "price": price,
                "change": change,
                "change_percent": chg_p,
                "volume": volume,
                "value_traded": value_traded,
            }

        return out

    # -------------------------------------------------------------------------
    # GLOBAL: EODHD -> FMP -> yfinance
    # -------------------------------------------------------------------------
    async def _fetch_global(self, symbol: str) -> UnifiedQuote:
        # Yahoo-style special symbols: skip EODHD/FMP and go yfinance
        if any(ch in symbol for ch in ("=", "^")):
            if settings.ENABLE_YFINANCE and "yfinance" in self.enabled_providers and yf:
                q = await self._fetch_yfinance(symbol, market="GLOBAL")
                if q.current_price is not None:
                    q.data_source = "yfinance"
                    q.data_quality = "PARTIAL"
                    return q
            return UnifiedQuote(symbol=symbol, market="GLOBAL", currency="USD", data_quality="MISSING", error="Unsupported symbol for providers").finalize()

        # 1) EODHD (real-time)
        if "eodhd" in self.enabled_providers and settings.EODHD_API_KEY:
            q = await self._fetch_eodhd_realtime(symbol)
            if q.current_price is not None:
                if settings.EODHD_FETCH_FUNDAMENTALS:
                    fund = await self._fetch_eodhd_fundamentals(symbol)
                    q = q.model_copy(update={k: v for k, v in fund.items() if v is not None})
                q.data_source = "eodhd"
                q.market = q.market or "GLOBAL"
                q.currency = q.currency or "USD"
                q.data_quality = "FULL" if q.previous_close is not None else "PARTIAL"
                return q.finalize()

        # 2) FMP
        if "fmp" in self.enabled_providers and settings.FMP_API_KEY:
            q = await self._fetch_fmp_quote(symbol)
            if q.current_price is not None:
                q.data_source = "fmp"
                q.market = "GLOBAL"
                q.currency = q.currency or "USD"
                q.data_quality = "FULL" if q.previous_close is not None else "PARTIAL"
                return q.finalize()

        # 3) yfinance fallback
        if settings.ENABLE_YFINANCE and "yfinance" in self.enabled_providers and yf:
            q = await self._fetch_yfinance(symbol, market="GLOBAL")
            if q.current_price is not None:
                q.data_source = "yfinance"
                q.data_quality = "PARTIAL"
                return q.finalize()

        return UnifiedQuote(symbol=symbol, market="GLOBAL", currency="USD", data_quality="MISSING", error="No provider data / missing keys").finalize()

    # -------------------------------------------------------------------------
    # HTTP helpers
    # -------------------------------------------------------------------------
    async def _http_get_text(self, url: str, params: Optional[Dict[str, Any]] = None) -> Optional[str]:
        try:
            r = await self.client.get(url, params=params)
            if r.status_code >= 400:
                return None
            return r.text
        except Exception:
            return None

    async def _http_get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        try:
            r = await self.client.get(url, params=params)
            if r.status_code >= 400:
                return None
            return r.json()
        except Exception:
            return None

    # -------------------------------------------------------------------------
    # Provider: EODHD
    # -------------------------------------------------------------------------
    async def _fetch_eodhd_realtime(self, symbol: str) -> UnifiedQuote:
        url = EODHD_RT_URL.format(symbol=symbol)
        params = {"api_token": settings.EODHD_API_KEY, "fmt": "json"}
        t0 = time.perf_counter()
        try:
            data = await self._http_get_json(url, params=params)
            if not data:
                return UnifiedQuote(symbol=symbol, market="GLOBAL", currency="USD", error="EODHD empty response")
            return UnifiedQuote(
                symbol=symbol,
                market="GLOBAL",
                currency="USD",
                current_price=_safe_float(data.get("close") or data.get("price") or data.get("previousClose")),
                previous_close=_safe_float(data.get("previousClose")),
                open=_safe_float(data.get("open")),
                day_high=_safe_float(data.get("high")),
                day_low=_safe_float(data.get("low")),
                volume=_safe_float(data.get("volume")),
                price_change=_safe_float(data.get("change")),
                percent_change=_safe_float(data.get("change_p")),
                data_source="eodhd",
                data_quality="PARTIAL",
            )
        except Exception as exc:
            dt = int((time.perf_counter() - t0) * 1000)
            return UnifiedQuote(symbol=symbol, market="GLOBAL", currency="USD", error=f"EODHD error ({dt}ms): {exc}")

    async def _fetch_eodhd_fundamentals(self, symbol: str) -> Dict[str, Any]:
        cache_key = f"eodhd_fund::{symbol}"
        hit = self.fund_cache.get(cache_key)
        if isinstance(hit, dict) and hit:
            return hit

        url = EODHD_FUND_URL.format(symbol=symbol)
        params = {"api_token": settings.EODHD_API_KEY, "fmt": "json"}
        data = await self._http_get_json(url, params=params)
        if not data:
            self.fund_cache[cache_key] = {}
            return {}

        general = data.get("General") or {}
        highlights = data.get("Highlights") or {}
        valuation = data.get("Valuation") or {}
        technicals = data.get("Technicals") or {}
        shares = data.get("SharesStats") or {}

        def _pct(x: Any) -> Optional[float]:
            v = _safe_float(x)
            if v is None:
                return None
            return v * 100.0 if v <= 1.0 else v

        out = {
            "name": _safe_str(general.get("Name")),
            "sector": _safe_str(general.get("Sector")),
            "industry": _safe_str(general.get("Industry")),
            "market_cap": _safe_float(highlights.get("MarketCapitalization") or general.get("MarketCapitalization")),
            "shares_outstanding": _safe_float(shares.get("SharesOutstanding") or general.get("SharesOutstanding")),
            "eps_ttm": _safe_float(highlights.get("EarningsShare")),
            "pe_ttm": _safe_float(highlights.get("PERatio")),
            "pb": _safe_float(valuation.get("PriceBookMRQ") or highlights.get("PriceToBookMRQ")),
            "dividend_yield": _pct(highlights.get("DividendYield")),
            "beta": _safe_float(highlights.get("Beta")),
            "net_margin": _pct(highlights.get("ProfitMargin")),
            "roe": _pct(highlights.get("ReturnOnEquityTTM")),
            "roa": _pct(highlights.get("ReturnOnAssetsTTM")),
            "high_52w": _safe_float(technicals.get("52WeekHigh")),
            "low_52w": _safe_float(technicals.get("52WeekLow")),
            "ma50": _safe_float(technicals.get("50DayMA")),
            "ma20": _safe_float(technicals.get("20DayMA")) or _safe_float(technicals.get("200DayMA")),
        }

        self.fund_cache[cache_key] = out
        return out

    # -------------------------------------------------------------------------
    # Provider: FMP
    # -------------------------------------------------------------------------
    async def _fetch_fmp_quote(self, symbol: str) -> UnifiedQuote:
        sym = _fmp_symbol(symbol)
        url = FMP_QUOTE_URL.format(symbol=sym)
        params = {"apikey": settings.FMP_API_KEY}
        try:
            data = await self._http_get_json(url, params=params)
            if not data or not isinstance(data, list):
                return UnifiedQuote(symbol=symbol, market="GLOBAL", currency="USD", error="FMP empty response")

            item = data[0] if data else {}
            current = _safe_float(item.get("price"))
            prev = _safe_float(item.get("previousClose"))
            return UnifiedQuote(
                symbol=symbol,
                name=_safe_str(item.get("name")),
                market="GLOBAL",
                currency=_safe_str(item.get("currency")) or "USD",
                current_price=current,
                previous_close=prev,
                price_change=_safe_float(item.get("change")),
                percent_change=_safe_float(item.get("changesPercentage")),
                day_high=_safe_float(item.get("dayHigh")),
                day_low=_safe_float(item.get("dayLow")),
                high_52w=_safe_float(item.get("yearHigh")),
                low_52w=_safe_float(item.get("yearLow")),
                volume=_safe_float(item.get("volume")),
                avg_volume_30d=_safe_float(item.get("avgVolume")),
                market_cap=_safe_float(item.get("marketCap")),
                pe_ttm=_safe_float(item.get("pe")),
                eps_ttm=_safe_float(item.get("eps")),
                data_source="fmp",
                data_quality="PARTIAL",
            )
        except Exception as exc:
            return UnifiedQuote(symbol=symbol, market="GLOBAL", currency="USD", error=f"FMP error: {exc}")

    # -------------------------------------------------------------------------
    # Provider: yfinance
    # -------------------------------------------------------------------------
    async def _fetch_yfinance(self, symbol: str, market: str) -> UnifiedQuote:
        if not yf:
            return UnifiedQuote(symbol=symbol, market=market, error="yfinance not installed")

        def _sync() -> Dict[str, Any]:
            t = yf.Ticker(symbol)
            try:
                fi = t.fast_info
                if isinstance(fi, dict):
                    return fi
                return {
                    "last_price": getattr(fi, "last_price", None),
                    "previous_close": getattr(fi, "previous_close", None),
                    "open": getattr(fi, "open", None),
                    "day_high": getattr(fi, "day_high", None),
                    "day_low": getattr(fi, "day_low", None),
                    "last_volume": getattr(fi, "last_volume", None),
                    "currency": getattr(fi, "currency", None),
                }
            except Exception:
                hist = t.history(period="5d", interval="1d")
                if hist is None or hist.empty:
                    return {}
                last = hist.iloc[-1]
                prev = hist.iloc[-2] if len(hist) >= 2 else last
                return {
                    "last_price": float(last.get("Close")) if "Close" in last else None,
                    "previous_close": float(prev.get("Close")) if "Close" in prev else None,
                    "open": float(last.get("Open")) if "Open" in last else None,
                    "day_high": float(last.get("High")) if "High" in last else None,
                    "day_low": float(last.get("Low")) if "Low" in last else None,
                    "last_volume": float(last.get("Volume")) if "Volume" in last else None,
                    "currency": None,
                }

        try:
            info = await asyncio.to_thread(_sync)
            if not info:
                return UnifiedQuote(symbol=symbol, market=market, error="yfinance empty response")

            cp = _safe_float(info.get("last_price"))
            pc = _safe_float(info.get("previous_close"))
            return UnifiedQuote(
                symbol=symbol,
                market=market,
                currency=_safe_str(info.get("currency")) or ("SAR" if market == "KSA" else "USD"),
                current_price=cp,
                previous_close=pc,
                open=_safe_float(info.get("open")),
                day_high=_safe_float(info.get("day_high")),
                day_low=_safe_float(info.get("day_low")),
                volume=_safe_float(info.get("last_volume")),
                data_source="yfinance",
                data_quality="PARTIAL" if cp is not None else "MISSING",
            )
        except Exception as exc:
            return UnifiedQuote(symbol=symbol, market=market, error=f"yfinance error: {exc}", data_quality="MISSING")

__all__ = ["UnifiedQuote", "DataEngine", "normalize_symbol", "is_ksa_symbol"]
