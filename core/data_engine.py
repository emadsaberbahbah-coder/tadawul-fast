"""
core/data_engine.py
----------------------------------------------------------------------
UNIFIED DATA & ANALYSIS ENGINE – LEGACY v2.3 (MULTI-PROVIDER)

Role in Architecture
--------------------
- This module is the "legacy" but still fully functional data engine.
- Many routes still fall back to `core.data_engine` when
  `core.data_engine_v2.DataEngine` is missing or disabled.
- It exposes both:
    • Module-level async functions:
        - get_enriched_quote(symbol)
        - get_enriched_quotes(symbols)
    • A thin async class wrapper:
        - DataEngine.get_enriched_quote(symbol)
        - DataEngine.get_enriched_quotes(symbols)

Key Behaviors
-------------
- Uses multiple providers:
    • EODHD (GLOBAL ONLY – **never** for .SR / KSA)
    • FMP
    • Yahoo Finance (yfinance) as universal fallback
- Merges provider outputs into a single `UnifiedQuote` Pydantic model.
- Provides a simple `opportunity_score` and `data_quality` signal.
- Exposes alias fields via `UnifiedQuote.model_dump()` so that
  Google Sheets endpoints (via routes/enriched_quote.py, legacy_service)
  see dashboard-friendly keys like:

    • last_price         -> from price
    • previous_close     -> from prev_close
    • high_52w / low_52w -> from fifty_two_week_high / fifty_two_week_low
    • change_percent     -> from change_pct
    • as_of_utc          -> from last_updated_utc
    • timezone           -> inferred from market_region

Global vs KSA
-------------
- EODHD is used **only** for GLOBAL symbols (no .SR suffix).
- For KSA (.SR) tickers this engine relies on FMP + Yahoo Finance.
- KSA-specific Tadawul/Argaam routing is handled in v2 and /v1/argaam
  routes and can be used as the preferred source for production KSA.

EODHD Fundamentals – ENHANCED
-----------------------------
- For GLOBAL symbols, this engine now calls:
    1) /real-time/{TICKER}
    2) /fundamentals/{TICKER}
  and merges fields from:
    • General
    • Highlights
    • Technicals
    • Valuation

  into a richer quote:

    • sector / industry / listing_date
    • market_cap
    • eps_ttm / pe_ttm / pb
    • dividend_yield
    • beta
    • 52-week high / low
    • roe / roa
    • profit_margin / operating_margin
    • revenue_growth_yoy / net_income_growth_yoy

- This behaviour can be toggled via:
    ENABLE_EODHD_FUNDAMENTALS = "0" / "1" (env var, default = "1")

NOTE
----
- This engine is deliberately **defensive**:
    • No direct EODHD calls for .SR / KSA.
    • Yahoo is a safety fallback when API providers fail.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple

import aiohttp  # type: ignore

# yfinance is optional but strongly recommended
try:  # pragma: no cover - import guard
    import yfinance as yf  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    yf = None  # type: ignore

from pydantic import BaseModel, Field  # type: ignore

# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------

# Legacy / future backend integration (kept for compatibility)
BACKEND_BASE_URL = os.getenv(
    "BACKEND_BASE_URL", "https://tadawul-fast-bridge.onrender.com"
)
APP_TOKEN = os.getenv("APP_TOKEN", "")
BACKUP_APP_TOKEN = os.getenv("BACKUP_APP_TOKEN", "")

# Enabled providers list (lowercase)
_enabled_raw = os.getenv("ENABLED_PROVIDERS", "eodhd,fmp,yfinance")
ENABLED_PROVIDERS: List[str] = [
    p.strip().lower() for p in _enabled_raw.split(",") if p.strip()
]

# Primary provider preference (usually "eodhd" for your paid plan)
PRIMARY_PROVIDER = os.getenv("PRIMARY_PROVIDER", "eodhd").lower()

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "25"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "2"))  # reserved for future use

# Provider-specific config
EODHD_API_KEY = os.getenv("EODHD_API_KEY", "")
EODHD_BASE_URL = os.getenv("EODHD_BASE_URL", "https://eodhd.com/api")

FMP_API_KEY = os.getenv("FMP_API_KEY", "")
FMP_BASE_URL = os.getenv(
    "FMP_BASE_URL", "https://financialmodelingprep.com/api/v3"
)

# Toggle for extra EODHD fundamentals call
_ENABLE_EODHD_FUNDAMENTALS_RAW = os.getenv("ENABLE_EODHD_FUNDAMENTALS", "1")
ENABLE_EODHD_FUNDAMENTALS: bool = (
    _ENABLE_EODHD_FUNDAMENTALS_RAW.strip().lower()
    not in {"0", "false", "no", "off"}
)

# Types
DataQualityLevel = Literal["EXCELLENT", "GOOD", "FAIR", "POOR", "MISSING"]
MarketRegion = Literal["KSA", "GLOBAL", "UNKNOWN"]


# ----------------------------------------------------------------------
# MODELS
# ----------------------------------------------------------------------


class QuoteSourceInfo(BaseModel):
    """Details about a single provider source used in the merged quote."""

    provider: str
    timestamp: datetime
    fields: List[str] = Field(default_factory=list)


class UnifiedQuote(BaseModel):
    # Identity
    symbol: str
    name: Optional[str] = None
    exchange: Optional[str] = None
    currency: Optional[str] = None
    market_region: MarketRegion = "UNKNOWN"
    sector: Optional[str] = None
    industry: Optional[str] = None
    listing_date: Optional[str] = None  # ISO string (e.g. "1980-12-12")

    # Intraday price snapshot
    price: Optional[float] = None
    prev_close: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    volume: Optional[float] = None

    # Derived price info
    change: Optional[float] = None
    change_pct: Optional[float] = None
    fifty_two_week_high: Optional[float] = None
    fifty_two_week_low: Optional[float] = None

    # Fundamentals
    market_cap: Optional[float] = None
    eps_ttm: Optional[float] = None
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None
    dividend_yield: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    debt_to_equity: Optional[float] = None
    profit_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    revenue_growth_yoy: Optional[float] = None
    net_income_growth_yoy: Optional[float] = None
    beta: Optional[float] = None

    # Meta
    last_updated_utc: Optional[datetime] = None
    data_quality: DataQualityLevel = "MISSING"
    data_gaps: List[str] = Field(default_factory=list)
    sources: List[QuoteSourceInfo] = Field(default_factory=list)

    # Analysis
    opportunity_score: Optional[float] = None
    risk_flag: Optional[str] = None
    notes: Optional[str] = None

    # ------------------------------------------------------------------
    # Compatibility helpers (keep old attribute names / keys working)
    # ------------------------------------------------------------------
    @property
    def last_updated(self) -> Optional[datetime]:
        return self.last_updated_utc

    @property
    def pe_ratio(self) -> Optional[float]:
        return self.pe_ttm

    @property
    def price_change(self) -> Optional[float]:
        return self.change

    @property
    def provider_sources(self) -> List[str]:
        return [s.provider for s in self.sources]

    def model_dump(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """
        Extend BaseModel.model_dump with extra alias keys so that
        downstream routers (especially /v1/enriched endpoints) can
        use consistent names without changing older code.

        Aliases added:
            - last_price       (from price)
            - previous_close   (from prev_close)
            - high_52w         (from fifty_two_week_high)
            - low_52w          (from fifty_two_week_low)
            - change_percent   (from change_pct)
            - as_of_utc        (from last_updated_utc)
            - as_of_local      (same as as_of_utc for now)
            - timezone         ('Asia/Riyadh' for KSA, else 'UTC')
        """
        data = super().model_dump(*args, **kwargs)

        # Price aliases
        if "price" in data and "last_price" not in data:
            data["last_price"] = data["price"]
        if "prev_close" in data and "previous_close" not in data:
            data["previous_close"] = data["prev_close"]

        # 52W aliases
        if "fifty_two_week_high" in data and "high_52w" not in data:
            data["high_52w"] = data["fifty_two_week_high"]
        if "fifty_two_week_low" in data and "low_52w" not in data:
            data["low_52w"] = data["fifty_two_week_low"]

        # Change aliases
        if "change_pct" in data and "change_percent" not in data:
            data["change_percent"] = data["change_pct"]

        # Timestamp / timezone aliases
        dt = data.get("last_updated_utc")
        if "as_of_utc" not in data:
            data["as_of_utc"] = dt
        if "as_of_local" not in data:
            data["as_of_local"] = dt

        if "timezone" not in data:
            mr = data.get("market_region", "UNKNOWN")
            if mr == "KSA":
                data["timezone"] = "Asia/Riyadh"
            else:
                data["timezone"] = "UTC"

        return data


# ----------------------------------------------------------------------
# PROVIDER ADAPTERS
# ----------------------------------------------------------------------


async def fetch_from_yahoo(symbol: str) -> Dict[str, Any]:
    """
    Fetch data from Yahoo Finance via yfinance.

    Used as a universal fallback when API providers do not return data.
    Handles both global and KSA (.SR) symbols.
    """
    if not yf:
        logger.error("yfinance module not found. Please add it to requirements.txt")
        return {}

    try:
        loop = asyncio.get_event_loop()
        ticker = await loop.run_in_executor(None, yf.Ticker, symbol)
        info = await loop.run_in_executor(None, lambda: ticker.info)

        # If price is missing, fallback to a 1-day history snapshot
        if not info or (
            "currentPrice" not in info and "regularMarketPrice" not in info
        ):
            hist = await loop.run_in_executor(
                None, lambda: ticker.history(period="1d")
            )
            if not hist.empty:
                info = info or {}
                info["currentPrice"] = float(hist["Close"].iloc[-1])
                info["previousClose"] = float(hist["Open"].iloc[-1])
                info["regularMarketVolume"] = int(hist["Volume"].iloc[-1])
            else:
                logger.warning("Yahoo Finance returned no data for %s", symbol)
                return {}

        return _normalize_yahoo_quote(info, symbol)
    except Exception as exc:  # pragma: no cover - defensive
        logger.error("Yahoo Finance fetch failed for %s: %s", symbol, exc)
        return {}


async def fetch_from_eodhd(symbol: str, session: aiohttp.ClientSession) -> Dict[str, Any]:
    """
    Fetch real-time quote (and optional fundamentals) from EODHD.

    Behaviour:
    - Uses /real-time/{code} for price snapshot.
    - If ENABLE_EODHD_FUNDAMENTALS is true, also calls
      /fundamentals/{code} and merges extra fields (sector, industry,
      listing_date, eps_ttm, pe_ttm, pb, dividend_yield, beta, 52w high/low,
      market_cap, roe, roa, profit_margin, operating_margin,
      revenue_growth_yoy, net_income_growth_yoy).

    NOTE:
    - **Not used for KSA (.SR)**; see _gather_provider_payloads which
      skips EODHD for Tadawul tickers.
    """
    if not EODHD_API_KEY:
        logger.debug("EODHD_API_KEY not configured – skipping EODHD for %s", symbol)
        return {}

    t = symbol.strip().upper()
    # If no suffix, assume US equity for EODHD
    code = t if "." in t else f"{t}.US"
    base = EODHD_BASE_URL.rstrip("/")

    async def _request_json(url: str) -> Dict[str, Any]:
        params = {"api_token": EODHD_API_KEY, "fmt": "json"}
        try:
            async with session.get(url, params=params, timeout=HTTP_TIMEOUT) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.warning(
                        "EODHD HTTP %s for %s (%s) [%s]: %s",
                        resp.status,
                        symbol,
                        code,
                        url,
                        text,
                    )
                    return {}
                try:
                    return await resp.json()
                except Exception as exc:
                    logger.warning(
                        "EODHD JSON decode failed for %s (%s) [%s]: %s",
                        symbol,
                        code,
                        url,
                        exc,
                    )
                    return {}
        except Exception as exc:  # pragma: no cover - network
            logger.error(
                "EODHD request failed for %s (%s) [%s]: %s",
                symbol,
                code,
                url,
                exc,
            )
            return {}

    realtime_url = f"{base}/real-time/{code}"
    fundamentals_data: Dict[str, Any] = {}

    if ENABLE_EODHD_FUNDAMENTALS:
        fundamentals_url = f"{base}/fundamentals/{code}"
        realtime_data, fundamentals_data = await asyncio.gather(
            _request_json(realtime_url),
            _request_json(fundamentals_url),
        )
    else:
        realtime_data = await _request_json(realtime_url)

    if not isinstance(realtime_data, dict) or "code" not in realtime_data:
        logger.warning(
            "EODHD payload invalid for %s (%s): %s", symbol, code, realtime_data
        )
        return {}

    if not isinstance(fundamentals_data, dict):
        fundamentals_data = {}

    return _normalize_eodhd_quote(realtime_data, symbol, fundamentals_data)


async def fetch_from_fmp(symbol: str, session: aiohttp.ClientSession) -> Dict[str, Any]:
    """
    Fetch quote & basic fundamentals from FinancialModelingPrep.

    Uses /quote/{symbol}, which returns a list with a single item.
    """
    if not FMP_API_KEY:
        logger.debug("FMP_API_KEY not configured – skipping FMP for %s", symbol)
        return {}

    url = f"{FMP_BASE_URL.rstrip('/')}/quote/{symbol.upper()}"
    params = {"apikey": FMP_API_KEY}

    try:
        async with session.get(url, params=params, timeout=HTTP_TIMEOUT) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.warning("FMP HTTP %s for %s: %s", resp.status, symbol, text)
                return {}
            data = await resp.json()
    except Exception as exc:  # pragma: no cover - network
        logger.error("FMP request failed for %s: %s", symbol, exc)
        return {}

    if not isinstance(data, list) or not data:
        logger.warning("FMP payload invalid for %s: %s", symbol, data)
        return {}

    return _normalize_fmp_quote(data[0], symbol)


async def fetch_from_backend_api(symbol: str) -> Dict[str, Any]:
    """
    (Optional) Fetch from an external backend API.

    NOTE:
    - Disabled by default to avoid self-calling loops inside the same container.
    - You can safely extend this later if you expose a dedicated data microservice.
    """
    if "tadawul-fast-bridge" in BACKEND_BASE_URL:
        logger.debug(
            "Skipping backend API fetch for %s to avoid possible infinite loop",
            symbol,
        )
        return {}

    # Placeholder – currently unused
    return {}


# ----------------------------------------------------------------------
# NORMALIZATION HELPERS
# ----------------------------------------------------------------------


def _normalize_yahoo_quote(data: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    """Normalize Yahoo Finance dict to UnifiedQuote-compatible payload."""
    now = datetime.now(timezone.utc)

    price = (
        data.get("currentPrice")
        or data.get("regularMarketPrice")
        or data.get("previousClose")
    )
    prev_close = data.get("previousClose") or data.get("regularMarketPreviousClose")

    change = data.get("change")
    change_pct = data.get("changePercent") or data.get("changePercentRaw")

    if change is None and price is not None and prev_close:
        change = price - prev_close
        change_pct = (change / prev_close) * 100.0

    payload: Dict[str, Any] = {
        "symbol": symbol,
        "name": data.get("longName") or data.get("shortName"),
        "price": price,
        "prev_close": prev_close,
        "open": data.get("open") or data.get("regularMarketOpen"),
        "high": data.get("dayHigh") or data.get("regularMarketDayHigh"),
        "low": data.get("dayLow") or data.get("regularMarketDayLow"),
        "volume": data.get("volume") or data.get("regularMarketVolume"),
        "market_cap": data.get("marketCap"),
        "currency": data.get("currency"),
        "exchange": data.get("exchange") or data.get("exchangeTimezoneName"),
        "fifty_two_week_high": data.get("fiftyTwoWeekHigh"),
        "fifty_two_week_low": data.get("fiftyTwoWeekLow"),
        "change": change,
        "change_pct": change_pct,
        # Extra identity & risk where available
        "sector": data.get("sector"),
        "industry": data.get("industry"),
        "beta": data.get("beta"),
        # Fundamentals
        "eps_ttm": data.get("trailingEps"),
        "pe_ttm": data.get("trailingPE"),
        "pb": data.get("priceToBook"),
        "dividend_yield": data.get("dividendYield"),
        "roe": data.get("returnOnEquity"),
        "roa": data.get("returnOnAssets"),
        "debt_to_equity": data.get("debtToEquity"),
        "profit_margin": data.get("profitMargins"),
        "operating_margin": data.get("operatingMargins"),
        "revenue_growth_yoy": data.get("revenueGrowth"),
        "net_income_growth_yoy": data.get("earningsGrowth"),
        "last_updated_utc": now,
        "__source__": QuoteSourceInfo(
            provider="yfinance", timestamp=now, fields=list(data.keys())
        ),
    }
    return payload


def _normalize_eodhd_quote(
    data: Dict[str, Any],
    symbol: str,
    fundamentals: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Normalize EODHD real-time payload (+ optional fundamentals)
    to UnifiedQuote-compatible dict.

    This is where most of the GLOBAL fundamentals are mapped.
    """
    now = datetime.now(timezone.utc)

    price = data.get("close")
    prev_close = data.get("previousClose")
    change = data.get("change")
    change_pct = data.get("change_p")

    if change is None and price is not None and prev_close:
        change = price - prev_close
        change_pct = (change / prev_close) * 100.0

    ts = data.get("timestamp")
    if isinstance(ts, (int, float)):
        try:
            last_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            last_dt = now
    else:
        last_dt = now

    general: Dict[str, Any] = {}
    highlights: Dict[str, Any] = {}
    technicals: Dict[str, Any] = {}
    valuation: Dict[str, Any] = {}

    if isinstance(fundamentals, dict) and fundamentals:
        g = fundamentals.get("General")
        if isinstance(g, dict):
            general = g
        h = fundamentals.get("Highlights")
        if isinstance(h, dict):
            highlights = h
        t = fundamentals.get("Technicals")
        if isinstance(t, dict):
            technicals = t
        v = fundamentals.get("Valuation")
        if isinstance(v, dict):
            valuation = v

    # Identity & static attributes
    currency = general.get("CurrencyCode") or _infer_currency_from_symbol(symbol)
    sector = general.get("Sector")
    industry = general.get("Industry") or general.get("GicIndustry")
    listing_date = general.get("IPODate")

    # Fundamentals from highlights/valuation/technicals
    market_cap = (
        data.get("market_cap")
        or data.get("marketCap")
        or general.get("MarketCapitalization")
        or highlights.get("MarketCapitalization")
    )

    # EPS
    eps_ttm = (
        highlights.get("EpsTTM")
        or highlights.get("EPS")
        or highlights.get("EarningsShare")
    )

    # P/E
    pe_ttm = (
        highlights.get("PERatio")
        or valuation.get("TrailingPE")
        or data.get("pe")
    )

    # Dividend yield
    dividend_yield = highlights.get("DividendYield")

    # PB ratio
    pb = (
        valuation.get("PriceBookMRQ")
        or valuation.get("PriceBookTTM")
        or valuation.get("PriceBook")
    )

    # Risk & profitability
    beta = highlights.get("Beta") or technicals.get("Beta")
    roe = highlights.get("ReturnOnEquityTTM")
    roa = highlights.get("ReturnOnAssetsTTM")
    profit_margin = highlights.get("ProfitMargin")
    operating_margin = (
        highlights.get("OperatingMarginTTM") or highlights.get("OperatingMargin5Y")
    )

    # Growth
    revenue_growth_yoy = highlights.get("QuarterlyRevenueGrowthYOY")
    net_income_growth_yoy = highlights.get("QuarterlyEarningsGrowthYOY")

    fifty_two_week_high = (
        technicals.get("52WeekHigh") or highlights.get("Week52High")
    )
    fifty_two_week_low = (
        technicals.get("52WeekLow") or highlights.get("Week52Low")
    )

    # Merge field names for provider source info
    fields = list(data.keys())
    for section in (general, highlights, technicals, valuation):
        if isinstance(section, dict):
            fields.extend(list(section.keys()))

    payload: Dict[str, Any] = {
        "symbol": symbol,
        "name": general.get("Name") or data.get("name"),
        "exchange": general.get("Exchange")
        or data.get("exchange_short_name")
        or data.get("exchange"),
        "currency": currency,
        "sector": sector,
        "industry": industry,
        "listing_date": listing_date,
        "price": price,
        "prev_close": prev_close,
        "open": data.get("open"),
        "high": data.get("high"),
        "low": data.get("low"),
        "volume": data.get("volume"),
        "market_cap": market_cap,
        "change": change,
        "change_pct": change_pct,
        "fifty_two_week_high": fifty_two_week_high,
        "fifty_two_week_low": fifty_two_week_low,
        "eps_ttm": eps_ttm,
        "pe_ttm": pe_ttm,
        "pb": pb,
        "dividend_yield": dividend_yield,
        "beta": beta,
        "roe": roe,
        "roa": roa,
        "profit_margin": profit_margin,
        "operating_margin": operating_margin,
        "revenue_growth_yoy": revenue_growth_yoy,
        "net_income_growth_yoy": net_income_growth_yoy,
        "last_updated_utc": last_dt,
        "__source__": QuoteSourceInfo(
            provider="eodhd", timestamp=last_dt, fields=fields
        ),
    }
    return payload


def _normalize_fmp_quote(data: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    """Normalize FMP /quote payload to UnifiedQuote-compatible dict."""
    now = datetime.now(timezone.utc)

    price = data.get("price")
    prev_close = data.get("previousClose")
    change = data.get("change")
    change_pct = data.get("changesPercentage")

    if change is None and price is not None and prev_close:
        change = price - prev_close
        change_pct = (change / prev_close) * 100.0

    payload: Dict[str, Any] = {
        "symbol": symbol,
        "name": data.get("name"),
        "exchange": data.get("exchange"),
        "currency": data.get("currency"),
        "price": price,
        "prev_close": prev_close,
        "open": data.get("open"),
        "high": data.get("dayHigh"),
        "low": data.get("dayLow"),
        "volume": data.get("volume"),
        "market_cap": data.get("marketCap"),
        "fifty_two_week_high": data.get("yearHigh"),
        "fifty_two_week_low": data.get("yearLow"),
        # Extra where available
        "sector": data.get("sector"),
        "industry": data.get("industry"),
        "beta": data.get("beta"),
        # Fundamentals (depending on your FMP plan)
        "eps_ttm": data.get("eps"),
        "pe_ttm": data.get("pe"),
        "pb": data.get("priceToBook"),
        "dividend_yield": data.get("yield"),
        "last_updated_utc": now,
        "change": change,
        "change_pct": change_pct,
        "__source__": QuoteSourceInfo(
            provider="fmp", timestamp=now, fields=list(data.keys())
        ),
    }
    return payload


def _infer_currency_from_symbol(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith(".SR"):
        return "SAR"
    # Default assumption for global tickers when fundamentals are missing
    return "USD"


def _calculate_change_fields(data: Dict[str, Any]) -> None:
    price = data.get("price")
    prev_close = data.get("prev_close")
    if price is not None and prev_close:
        data["change"] = price - prev_close
        data["change_pct"] = (price - prev_close) / prev_close * 100.0


def _infer_market_region(symbol: str, exchange: Optional[str]) -> MarketRegion:
    if symbol.upper().endswith(".SR") or (
        exchange and "SAUDI" in str(exchange).upper()
    ):
        return "KSA"
    return "GLOBAL"


def _assess_data_quality(data: Dict[str, Any]) -> Tuple[DataQualityLevel, List[str]]:
    required = ["price", "prev_close", "volume"]
    missing = [f for f in required if data.get(f) is None]
    if len(missing) == len(required):
        return "MISSING", missing
    if not missing:
        return "EXCELLENT", missing
    return "FAIR", missing


def _compute_opportunity_score(data: Dict[str, Any]) -> Optional[float]:
    """
    Very simple opportunity score based mainly on P/E and dividend yield.
    0–100 scale.
    """
    pe = data.get("pe_ttm")
    dy = data.get("dividend_yield")
    if pe is None and dy is None:
        return None

    score = 50.0

    # Value tilt via P/E
    if isinstance(pe, (int, float)):
        if 0 < pe < 12:
            score += 25
        elif 12 <= pe <= 20:
            score += 10
        elif pe > 35:
            score -= 15

    # Income tilt via dividend yield
    if isinstance(dy, (int, float)):
        if 0.02 <= dy <= 0.06:
            score += 10
        elif dy > 0.08:
            score -= 5

    return max(0.0, min(100.0, score))


# ----------------------------------------------------------------------
# PROVIDER MERGING
# ----------------------------------------------------------------------


async def _gather_provider_payloads(symbol: str) -> Dict[str, Any]:
    """
    Call all enabled providers (EODHD, FMP, Yahoo) and merge results.

    Priority order:
        1) PRIMARY_PROVIDER (if available and allowed)
        2) Remaining providers from ENABLED_PROVIDERS

    IMPORTANT (KSA rule):
        - If symbol ends with '.SR', EODHD is **skipped**, since it
          does not work reliably for Tadawul.
    """
    symbol = symbol.strip().upper()
    is_ksa = symbol.endswith(".SR")

    # Restrict to known providers and apply KSA rule for EODHD
    providers_in_env = [
        p for p in ENABLED_PROVIDERS if p in {"eodhd", "fmp", "yfinance"}
    ]
    if is_ksa and "eodhd" in providers_in_env:
        providers_in_env.remove("eodhd")
        logger.debug("Skipping EODHD for KSA symbol %s", symbol)

    # Build order with primary first
    order: List[str] = []
    primary = PRIMARY_PROVIDER
    if primary in providers_in_env:
        order.append(primary)
    for p in providers_in_env:
        if p not in order:
            order.append(p)

    if not order:
        logger.warning("No providers enabled – falling back to Yahoo only.")
        order = ["yfinance"]

    merged: Dict[str, Any] = {"symbol": symbol}
    sources: List[QuoteSourceInfo] = []

    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks: Dict[str, asyncio.Task] = {}

        for provider in order:
            if provider == "eodhd":
                tasks["eodhd"] = asyncio.create_task(
                    fetch_from_eodhd(symbol, session)
                )
            elif provider == "fmp":
                tasks["fmp"] = asyncio.create_task(fetch_from_fmp(symbol, session))
            elif provider == "yfinance":
                tasks["yfinance"] = asyncio.create_task(fetch_from_yahoo(symbol))

        if not tasks:
            return {}

        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        for provider_name, result in zip(tasks.keys(), results):
            if isinstance(result, Exception):  # pragma: no cover - defensive
                logger.error(
                    "Provider %s raised exception for %s: %s",
                    provider_name,
                    symbol,
                    result,
                )
                continue

            data = result or {}
            if not data:
                continue

            src = data.pop("__source__", None)
            if isinstance(src, QuoteSourceInfo):
                sources.append(src)

            for key, value in data.items():
                if value is None:
                    continue
                if key not in merged or merged.get(key) is None:
                    merged[key] = value

    merged["sources"] = sources
    if merged.get("last_updated_utc") is None:
        merged["last_updated_utc"] = datetime.now(timezone.utc)

    _calculate_change_fields(merged)
    return merged


# ----------------------------------------------------------------------
# PUBLIC ENTRYPOINTS (MODULE-LEVEL)
# ----------------------------------------------------------------------


async def get_enriched_quote(symbol: str) -> UnifiedQuote:
    """
    Public enriched quote function (module-level).

    This is the main legacy entrypoint used by older parts of the
    system. Newer code may use `core.data_engine_v2.DataEngine`,
    but this remains the safe fallback.
    """
    symbol = symbol.strip().upper()
    logger.info("[DataEngine v1] Fetching enriched quote for: %s", symbol)

    raw_data = await _gather_provider_payloads(symbol)

    if not raw_data:
        logger.warning("No data found for %s", symbol)
        return UnifiedQuote(
            symbol=symbol,
            last_updated_utc=datetime.now(timezone.utc),
            data_quality="MISSING",
            data_gaps=["No data from providers"],
        )

    dq, gaps = _assess_data_quality(raw_data)
    opp_score = _compute_opportunity_score(raw_data)

    quote = UnifiedQuote(
        **raw_data,
        market_region=_infer_market_region(
            symbol, raw_data.get("exchange")
        ),
        data_quality=dq,
        data_gaps=gaps,
        opportunity_score=opp_score,
    )

    return quote


async def get_enriched_quotes(symbols: List[str]) -> List[UnifiedQuote]:
    """
    Fetch multiple quotes concurrently (module-level).
    """
    tasks = [get_enriched_quote(sym) for sym in symbols]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    quotes: List[UnifiedQuote] = []
    for sym, result in zip(symbols, results):
        if isinstance(result, UnifiedQuote):
            quotes.append(result)
        else:
            logger.error("[DataEngine v1] Error fetching %s: %s", sym, result)
            quotes.append(
                UnifiedQuote(
                    symbol=sym.strip().upper(),
                    data_quality="MISSING",
                    data_gaps=[f"Exception: {result}"],
                )
            )

    return quotes


# ----------------------------------------------------------------------
# OOP WRAPPER – DataEngine CLASS
# ----------------------------------------------------------------------


class DataEngine:
    """
    Thin OOP wrapper around the legacy module-level functions.

    This class is what `routes.enriched_quote` and
    `legacy_service` / `ai_analysis` expect when they do:

        from core.data_engine import DataEngine
        engine = DataEngine()
        quote = await engine.get_enriched_quote("AAPL")
    """

    def __init__(
        self,
        enabled_providers: Optional[List[str]] = None,
        primary_provider: Optional[str] = None,
    ) -> None:
        """
        Optional per-instance overrides for provider config.

        NOTE: For now, these are informational only – the actual
        provider list still comes from global ENV variables so that
        behaviour is consistent across the whole app.
        """
        self.enabled_providers = enabled_providers or ENABLED_PROVIDERS
        self.primary_provider = (primary_provider or PRIMARY_PROVIDER).lower()

    async def get_enriched_quote(self, symbol: str) -> UnifiedQuote:
        """
        Instance method: delegates to module-level function for now.
        """
        return await get_enriched_quote(symbol)

    async def get_enriched_quotes(self, symbols: List[str]) -> List[UnifiedQuote]:
        """
        Instance method: delegate to module-level batch function.
        """
        return await get_enriched_quotes(symbols)
