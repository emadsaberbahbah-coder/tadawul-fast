"""
core/data_engine_v2.py
===============================================
Core Data & Analysis Engine - v2.2

Author: Emad Bahbah (with GPT-5.1 Thinking)

Key features
------------
- Single unified EnrichedQuote model for ALL assets:
    • KSA (.SR), Global equities, Mutual Funds / ETFs, Commodities, FX.
- Uses yfinance as the primary provider for price, fundamentals, and
  basic technicals (RSI, MA20, MA50, 30-day volatility).
- KSA (.SR) symbols are KSA-safe:
    • Never calls EODHD for .SR (this engine currently doesn't use EODHD).
- Simple in-memory caching with TTL to reduce provider calls.
- AI-style scoring:
    • Value / Quality / Momentum / Opportunity + Recommendation
      via core.scoring_engine.
- Extremely defensive:
    • Never propagates provider exceptions.
    • On failure, returns EnrichedQuote with data_quality="MISSING"
      and error message instead of raising.

Typical usage
-------------
    from core.data_engine_v2 import DataEngineV2

    engine = DataEngineV2()

    quote = engine.get_enriched_quote("1120.SR", sheet_name="KSA_Tadawul")
    quotes = engine.get_enriched_quotes(["1120.SR", "COST"], sheet_name="Global_Markets")

    headers, rows = engine.get_sheet_rows(["1120.SR", "COST"], sheet_name="Global_Markets")
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import yfinance as yf

from .enriched_quote import EnrichedQuote
from .scoring_engine import enrich_with_scores
from .schemas import get_headers_for_sheet


# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

DEFAULT_CACHE_TTL_SECONDS = 60 * 5  # 5 minutes – adjust if needed


@dataclass
class CacheEntry:
    quote: EnrichedQuote
    expires_at: float


def _now_ts() -> float:
    return time.time()


# ---------------------------------------------------------------------------
# MARKET & ASSET TYPE DETECTION
# ---------------------------------------------------------------------------

def is_ksa_symbol(symbol: str) -> bool:
    return symbol.endswith(".SR")


def is_fx_symbol(symbol: str) -> bool:
    # Typical pattern: EURUSD=X, USDJPY=X, etc.
    return symbol.endswith("=X")


def is_commodity_symbol(symbol: str) -> bool:
    # Futures often look like GC=F, CL=F, BZ=F, etc.
    return symbol.endswith("=F") and not is_fx_symbol(symbol)


def detect_market_type(symbol: str, sheet_name: Optional[str]) -> str:
    """
    Return a simple market type string:
    - "KSA"
    - "GLOBAL"
    - "FUND"
    - "COMMODITY"
    - "FX"
    """
    s = symbol.upper()
    sn = (sheet_name or "").strip()

    if sn == "Mutual_Funds":
        return "FUND"

    if sn == "Commodities_FX" or is_fx_symbol(s) or is_commodity_symbol(s):
        if is_fx_symbol(s):
            return "FX"
        if is_commodity_symbol(s):
            return "COMMODITY"
        # Fallback inside this sheet
        return "COMMODITY"

    if is_ksa_symbol(s) or sn == "KSA_Tadawul":
        return "KSA"

    # Default
    return "GLOBAL"


# ---------------------------------------------------------------------------
# CORE ENGINE
# ---------------------------------------------------------------------------

class DataEngineV2:
    """
    YFinance-based Data & Analysis Engine.

    This class is designed to be used as a long-lived singleton in your app
    (e.g. created once in main.py and injected into routes).
    """

    def __init__(self, cache_ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS) -> None:
        self._cache_ttl = cache_ttl_seconds
        # Cache key: (symbol_upper, market_type)
        self._cache: Dict[Tuple[str, str], CacheEntry] = {}

    # ----------------------------
    # PUBLIC API
    # ----------------------------

    def get_enriched_quote(
        self,
        symbol: str,
        sheet_name: Optional[str] = None,
    ) -> EnrichedQuote:
        """
        Fetch a single EnrichedQuote, with caching and scoring.
        Never raises on normal usage; returns MISSING quote on errors.
        """
        symbol = symbol.strip()
        if not symbol:
            return self._missing_quote("UNKNOWN", sheet_name, error="Empty symbol")

        market_type = detect_market_type(symbol, sheet_name)
        cache_key = (symbol.upper(), market_type)

        # 1) Try cache
        now = _now_ts()
        entry = self._cache.get(cache_key)
        if entry and entry.expires_at > now:
            return entry.quote

        # 2) Fetch from provider(s)
        try:
            quote = self._fetch_from_yfinance(symbol, market_type, sheet_name)
            # 3) Enrich with AI-style scores
            quote = enrich_with_scores(quote)
            # 4) Cache
            self._cache[cache_key] = CacheEntry(
                quote=quote,
                expires_at=now + self._cache_ttl,
            )
            return quote
        except Exception as exc:
            # Extremely defensive: never propagate provider errors
            return self._missing_quote(
                symbol,
                sheet_name,
                error=f"Provider error: {exc!r}",
            )

    def get_enriched_quotes(
        self,
        symbols: Sequence[str],
        sheet_name: Optional[str] = None,
    ) -> List[EnrichedQuote]:
        """
        Fetch multiple EnrichedQuote objects.

        This is currently synchronous (sequential); if you want, we
        can later add simple concurrency with asyncio or threading.
        """
        cleaned = [s.strip() for s in symbols if s and s.strip()]
        result: List[EnrichedQuote] = []
        for sym in cleaned:
            result.append(self.get_enriched_quote(sym, sheet_name=sheet_name))
        return result

    def get_sheet_rows(
        self,
        symbols: Sequence[str],
        sheet_name: Optional[str] = None,
    ) -> Tuple[List[str], List[List[object]]]:
        """
        Helper used by /v1/enriched/sheet-rows:

        Returns:
            headers: list of header strings
            rows: list of rows (list of values), one per symbol
        """
        headers = get_headers_for_sheet(sheet_name)
        quotes = self.get_enriched_quotes(symbols, sheet_name=sheet_name)
        rows = [q.to_row(headers) for q in quotes]
        return headers, rows

    # ----------------------------
    # INTERNAL HELPERS
    # ----------------------------

    def _missing_quote(
        self,
        symbol: str,
        sheet_name: Optional[str],
        error: str,
    ) -> EnrichedQuote:
        """
        Build a safe "missing" quote record that still maps to the
        universal header template, with an error message.
        """
        now_utc = datetime.now(timezone.utc)
        return EnrichedQuote(
            symbol=symbol,
            name=None,
            sector=None,
            sub_sector=None,
            market=detect_market_type(symbol, sheet_name),
            currency=None,
            listing_date=None,
            last_price=None,
            previous_close=None,
            open_price=None,
            high_price=None,
            low_price=None,
            change=None,
            change_percent=None,
            high_52w=None,
            low_52w=None,
            position_52w_percent=None,
            volume=None,
            avg_volume_30d=None,
            value_traded=None,
            turnover_rate=None,
            bid=None,
            ask=None,
            bid_size=None,
            ask_size=None,
            spread_percent=None,
            liquidity_score=None,
            eps_ttm=None,
            pe_ratio=None,
            pb_ratio=None,
            dividend_yield=None,
            dividend_payout=None,
            roe=None,
            roa=None,
            debt_to_equity=None,
            current_ratio=None,
            quick_ratio=None,
            market_cap=None,
            revenue_growth=None,
            net_income_growth=None,
            ebitda_margin=None,
            operating_margin=None,
            net_margin=None,
            ev_to_ebitda=None,
            price_to_sales=None,
            price_to_cash_flow=None,
            peg_ratio=None,
            beta=None,
            volatility_30d=None,
            fair_value=None,
            upside_percent=None,
            valuation_label=None,
            value_score=None,
            quality_score=None,
            momentum_score=None,
            opportunity_score=None,
            recommendation=None,
            rsi_14=None,
            macd=None,
            ma_20d=None,
            ma_50d=None,
            provider="yfinance",
            data_quality="MISSING",
            last_updated_utc=now_utc,
            last_updated_local=now_utc,
            timezone="UTC",
            error=error,
        )

    def _fetch_from_yfinance(
        self,
        symbol: str,
        market_type: str,
        sheet_name: Optional[str],
    ) -> EnrichedQuote:
        """
        Single-provider implementation based on yfinance.

        - For KSA & GLOBAL equities, Mutual Funds, Commodities, FX:
          uses the same mapping from yfinance Ticker.
        - Many advanced fields may be None if provider doesn't supply them.
        """

        t = yf.Ticker(symbol)

        # Fast price snapshot
        try:
            fast_info = t.fast_info
        except Exception:
            fast_info = {}

        # Rich fundamental info
        try:
            info = t.info or {}
        except Exception:
            info = {}

        # 1-month history for basic technicals
        try:
            hist = t.history(period="1mo")
        except Exception:
            hist = None

        now_utc = datetime.now(timezone.utc)

        # ----------------------------
        # Market snapshot values
        # ----------------------------
        last_price = _safe_float(
            fast_info.get("last_price")
            or fast_info.get("lastClose")
            or info.get("regularMarketPrice")
        )

        previous_close = _safe_float(
            fast_info.get("previous_close")
            or fast_info.get("previousClose")
            or info.get("regularMarketPreviousClose")
        )

        open_price = _safe_float(
            fast_info.get("open")
            or info.get("regularMarketOpen")
        )

        high_price = _safe_float(
            fast_info.get("dayHigh")
            or info.get("dayHigh")
        )

        low_price = _safe_float(
            fast_info.get("dayLow")
            or info.get("dayLow")
        )

        change = None
        change_percent = None
        if last_price is not None and previous_close is not None and previous_close != 0:
            change = last_price - previous_close
            change_percent = change / previous_close  # decimal

        high_52w = _safe_float(
            fast_info.get("year_high")
            or info.get("fiftyTwoWeekHigh")
        )
        low_52w = _safe_float(
            fast_info.get("year_low")
            or info.get("fiftyTwoWeekLow")
        )

        position_52w_percent = None
        if last_price is not None and high_52w and low_52w and high_52w != low_52w:
            position_52w_percent = (last_price - low_52w) / (high_52w - low_52w)

        volume = _safe_float(
            fast_info.get("last_volume")
            or info.get("volume")
        )
        avg_volume_30d = _safe_float(
            fast_info.get("tenDayAverageVolume")
            or info.get("averageDailyVolume10Day")
            or info.get("averageVolume")
        )

        value_traded = None
        if last_price is not None and volume is not None:
            value_traded = last_price * volume

        bid = _safe_float(fast_info.get("bid") or info.get("bid"))
        ask = _safe_float(fast_info.get("ask") or info.get("ask"))
        bid_size = _safe_float(info.get("bidSize"))
        ask_size = _safe_float(info.get("askSize"))

        spread_percent = None
        if last_price and bid and ask and last_price != 0:
            spread_percent = (ask - bid) / last_price

        # ----------------------------
        # Fundamentals & ratios
        # ----------------------------
        eps_ttm = _safe_float(info.get("trailingEps"))
        pe_ratio = _safe_float(info.get("trailingPE"))
        pb_ratio = _safe_float(info.get("priceToBook"))

        dividend_yield = _safe_float(info.get("dividendYield"))  # decimal
        dividend_payout = _safe_float(info.get("payoutRatio"))   # decimal

        roe = _safe_float(info.get("returnOnEquity"))   # decimal
        roa = _safe_float(info.get("returnOnAssets"))   # decimal

        debt_to_equity = _safe_float(info.get("debtToEquity"))
        current_ratio = _safe_float(info.get("currentRatio"))
        quick_ratio = _safe_float(info.get("quickRatio"))
        market_cap = _safe_float(info.get("marketCap"))

        revenue_growth = _safe_float(info.get("revenueGrowth"))           # decimal
        net_income_growth = _safe_float(info.get("earningsGrowth"))       # decimal
        ebitda_margin = _safe_float(info.get("ebitdaMargins"))            # decimal
        operating_margin = _safe_float(info.get("operatingMargins"))      # decimal
        net_margin = _safe_float(info.get("profitMargins"))               # decimal

        ev_to_ebitda = _safe_float(info.get("enterpriseToEbitda"))
        price_to_sales = _safe_float(info.get("priceToSalesTrailing12Months"))
        # price_to_cash_flow is not always provided; leave as None for now.
        price_to_cash_flow = None
        peg_ratio = _safe_float(info.get("pegRatio"))
        beta = _safe_float(info.get("beta"))

        # ----------------------------
        # Technicals: volatility, MA, RSI, MACD
        # ----------------------------
        volatility_30d = None
        rsi_14 = None
        ma_20d = None
        ma_50d = None
        macd = None

        if hist is not None and not hist.empty:
            closes = hist["Close"].dropna()
            if not closes.empty:
                # Rolling averages
                if len(closes) >= 20:
                    ma_20d = float(closes.rolling(window=20).mean().iloc[-1])
                if len(closes) >= 50:
                    ma_50d = float(closes.rolling(window=50).mean().iloc[-1])

                # Daily returns for volatility
                returns = closes.pct_change().dropna()
                if len(returns) >= 10:
                    # 30-day volatility approx: daily std * sqrt(30)
                    vol = float(returns.std() * math.sqrt(30))
                    volatility_30d = vol

                # RSI(14)
                if len(closes) >= 15:
                    rsi_14 = _compute_rsi(closes, period=14)

                # MACD (12,26,9)
                if len(closes) >= 26:
                    macd = _compute_macd(closes)

        # ----------------------------
        # Fair value & upside (simple)
        # ----------------------------
        fair_value = _safe_float(
            info.get("targetMeanPrice")
            or info.get("targetMedianPrice")
        )

        upside_percent = None
        valuation_label = None
        if fair_value is not None and last_price:
            upside_percent = (fair_value - last_price) / last_price
            upside_pct_100 = upside_percent * 100.0
            if upside_pct_100 > 20:
                valuation_label = "CHEAP"
            elif upside_pct_100 > 0:
                valuation_label = "FAIR_VALUE"
            else:
                valuation_label = "EXPENSIVE"

        # ----------------------------
        # Data quality
        # ----------------------------
        if last_price is None:
            data_quality = "MISSING"
        elif eps_ttm is None and market_cap is None:
            data_quality = "PARTIAL"
        else:
            data_quality = "FULL"

        # ----------------------------
        # Build EnrichedQuote
        # ----------------------------
        listing_date = None
        first_trade_ts = info.get("firstTradeDateEpochUtc") or info.get("firstTradeDateTimestamp")
        if first_trade_ts:
            try:
                listing_date = datetime.fromtimestamp(first_trade_ts, tz=timezone.utc).date()
            except Exception:
                listing_date = None

        eq = EnrichedQuote(
            symbol=symbol,
            name=info.get("shortName") or info.get("longName") or info.get("name"),
            sector=info.get("sector"),
            sub_sector=info.get("industry"),
            market=market_type,
            currency=info.get("currency"),
            listing_date=listing_date,
            last_price=last_price,
            previous_close=previous_close,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            change=change,
            change_percent=change_percent,
            high_52w=high_52w,
            low_52w=low_52w,
            position_52w_percent=position_52w_percent,
            volume=volume,
            avg_volume_30d=avg_volume_30d,
            value_traded=value_traded,
            turnover_rate=None,  # yfinance does not directly provide; can be computed later
            bid=bid,
            ask=ask,
            bid_size=bid_size,
            ask_size=ask_size,
            spread_percent=spread_percent,
            liquidity_score=None,  # can be derived later (volume, spread, etc.)
            eps_ttm=eps_ttm,
            pe_ratio=pe_ratio,
            pb_ratio=pb_ratio,
            dividend_yield=dividend_yield,
            dividend_payout=dividend_payout,
            roe=roe,
            roa=roa,
            debt_to_equity=debt_to_equity,
            current_ratio=current_ratio,
            quick_ratio=quick_ratio,
            market_cap=market_cap,
            revenue_growth=revenue_growth,
            net_income_growth=net_income_growth,
            ebitda_margin=ebitda_margin,
            operating_margin=operating_margin,
            net_margin=net_margin,
            ev_to_ebitda=ev_to_ebitda,
            price_to_sales=price_to_sales,
            price_to_cash_flow=price_to_cash_flow,
            peg_ratio=peg_ratio,
            beta=beta,
            volatility_30d=volatility_30d,
            fair_value=fair_value,
            upside_percent=upside_percent,
            valuation_label=valuation_label,
            # AI scores filled later via enrich_with_scores()
            value_score=None,
            quality_score=None,
            momentum_score=None,
            opportunity_score=None,
            recommendation=None,
            rsi_14=rsi_14,
            macd=macd,
            ma_20d=ma_20d,
            ma_50d=ma_50d,
            provider="yfinance",
            data_quality=data_quality,
            last_updated_utc=now_utc,
            last_updated_local=now_utc,
            timezone=info.get("exchangeTimezoneName") or "UTC",
            error=None,
        )

        return eq


# ---------------------------------------------------------------------------
# Utility functions for yfinance mapping
# ---------------------------------------------------------------------------

def _safe_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _ema(series, span: int) -> float | None:
    if series is None or len(series) == 0:
        return None
    try:
        return float(series.ewm(span=span, adjust=False).mean().iloc[-1])
    except Exception:
        return None


def _compute_rsi(closes, period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    delta = closes.diff().dropna()
    gain = (delta.where(delta > 0, 0.0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, float("inf"))
    rsi = 100 - (100 / (1 + rs))
    try:
        return float(rsi.iloc[-1])
    except Exception:
        return None


def _compute_macd(closes) -> Optional[float]:
    """
    Basic MACD: 12-day EMA - 26-day EMA (we return just the MACD line).
    """
    if len(closes) < 26:
        return None
    ema12 = _ema(closes, span=12)
    ema26 = _ema(closes, span=26)
    if ema12 is None or ema26 is None:
        return None
    return float(ema12 - ema26)
