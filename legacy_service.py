"""
legacy_service.py
------------------------------------------------------------
LEGACY SERVICE BRIDGE – v3.3 (Aligned with App v4.4.0 / Engine v2.5)

GOAL
- Provide a stable "legacy" layer on top of the unified data engine,
  so old clients (PowerShell, early Google Sheets scripts, test tabs)
  can still receive the classic /v1/quote-style payload.

KEY PRINCIPLES
- NO direct EODHD calls here.
- All data comes via the unified data engine (v2 preferred, v1 module fallback).
- KSA (.SR) tickers are handled by Tadawul/Argaam providers inside the engine.
- Legacy quote format:
      {
          "quotes": [ { ... legacy fields ... } ],
          "meta":   { ... context ... }
      }
- Google Sheets integration helpers:
      • build_legacy_sheet_payload(tickers)
        -> { "headers": [...], "rows": [[...], ...], "meta": {...} }

ALIGNMENT
- Engine usage & fallbacks aligned with:
    • core.data_engine_v2.DataEngine (preferred, class-based)
    • core.data_engine (module-level async functions, fallback)
    • routes/enriched_quote.py
    • routes/ai_analysis.py
    • routes/advanced_analysis.py
    • routes_argaam.py (KSA / Tadawul-Argaam gateway)
- Symbol normalization aligned with KSA / Tadawul logic used elsewhere:
    • 1120                -> 1120.SR
    • TADAWUL:1120        -> 1120.SR
    • 1120.TADAWUL        -> 1120.SR
    • 1120.SR             -> 1120.SR
    • AAPL                -> AAPL

USAGE
- main.py:
      from legacy_service import get_legacy_quotes, build_legacy_sheet_payload
      # /v1/quote, /v1/legacy/sheet-rows use these functions.

- Preferred for new dashboards:
      /v1/enriched/sheet-rows
      /v1/analysis/sheet-rows
      /v1/advanced/sheet-rows
  This legacy layer is mainly for backward compatibility and quick tests.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("legacy_service")
if not logger.handlers:
    logger.setLevel(logging.INFO)

# ----------------------------------------------------------------------
# ENV / SETTINGS – env.settings preferred, env vars fallback
# ----------------------------------------------------------------------

try:  # pragma: no cover - env.py optional
    import env as _env_mod  # type: ignore
except Exception:  # pragma: no cover
    _env_mod = None  # type: ignore
    logger.warning(
        "[LegacyService] env.py not available. "
        "Using OS environment variables for basic config."
    )


@dataclass
class _SettingsFallback:
    app_env: str = os.getenv("APP_ENV", "production")
    app_name: str = os.getenv("APP_NAME", "Tadawul Fast Bridge")
    app_version: str = os.getenv("APP_VERSION", "4.4.0")
    backend_base_url: str = os.getenv("BACKEND_BASE_URL", "").strip()


def _get_env_attr(name: str, default: str = "") -> str:
    """
    Helper to read config from env.py if available, otherwise from OS env vars.
    Returns a string representation.
    """
    if _env_mod is not None and hasattr(_env_mod, name):
        value = getattr(_env_mod, name)
        if isinstance(value, str):
            return value
        try:
            return str(value)
        except Exception:
            return default
    return os.getenv(name, default)


settings = getattr(_env_mod, "settings", _SettingsFallback())

APP_NAME: str = getattr(
    settings, "app_name", _get_env_attr("APP_NAME", "tadawul-fast-bridge")
)
APP_VERSION: str = getattr(
    settings, "app_version", _get_env_attr("APP_VERSION", "4.4.0")
)
BACKEND_BASE_URL: str = getattr(
    settings, "backend_base_url", _get_env_attr("BACKEND_BASE_URL", "")
).strip()

# ----------------------------------------------------------------------
# DATA ENGINE IMPORT / FALLBACKS (v2 preferred, v1 module, stub)
# ----------------------------------------------------------------------

_ENGINE_MODE: str = "stub"  # "v2", "v1_module", "stub"
_ENGINE_IS_STUB: bool = True
_engine: Any = None
_data_engine_module: Any = None
_ENABLED_PROVIDERS: List[str] = []

try:  # Preferred: new class-based engine (core.data_engine_v2)
    from core.data_engine_v2 import DataEngine as _V2DataEngine  # type: ignore

    engine_kwargs: Dict[str, Any] = {}

    # Optional engine tuning via env.py
    if _env_mod is not None:
        cache_ttl = getattr(_env_mod, "ENGINE_CACHE_TTL_SECONDS", None)
        if cache_ttl is not None:
            engine_kwargs["cache_ttl"] = cache_ttl

        provider_timeout = getattr(
            _env_mod, "ENGINE_PROVIDER_TIMEOUT_SECONDS", None
        )
        if provider_timeout is not None:
            engine_kwargs["provider_timeout"] = provider_timeout

        enabled_providers = getattr(_env_mod, "ENABLED_PROVIDERS", None)
        if enabled_providers:
            engine_kwargs["enabled_providers"] = enabled_providers

        enable_adv = getattr(
            _env_mod, "ENGINE_ENABLE_ADVANCED_ANALYSIS", True
        )
        engine_kwargs["enable_advanced_analysis"] = enable_adv

    try:
        if engine_kwargs:
            _engine = _V2DataEngine(**engine_kwargs)
        else:
            _engine = _V2DataEngine()
    except TypeError:
        # Signature mismatch -> fall back to default constructor
        _engine = _V2DataEngine()

    _ENGINE_MODE = "v2"
    _ENGINE_IS_STUB = False
    _ENABLED_PROVIDERS = list(getattr(_engine, "enabled_providers", []))
    logger.info(
        "[LegacyService] Using DataEngine v2 from core.data_engine_v2 "
        "(kwargs=%s, enabled_providers=%s).",
        list(engine_kwargs.keys()),
        _ENABLED_PROVIDERS,
    )

except Exception as e_v2:  # pragma: no cover - defensive
    logger.exception(
        "[LegacyService] Failed to init core.data_engine_v2.DataEngine: %s",
        e_v2,
    )
    try:
        # Fallback: legacy module-level engine (core.data_engine.get_enriched_quotes)
        from core import data_engine as _data_engine_module  # type: ignore

        _ENGINE_MODE = "v1_module"
        _ENGINE_IS_STUB = False
        logger.warning(
            "[LegacyService] Falling back to core.data_engine module-level API."
        )
    except Exception as e_v1:  # pragma: no cover - defensive
        logger.exception(
            "[LegacyService] Failed to import core.data_engine as fallback: %s",
            e_v1,
        )

        class _StubEngine:
            """
            Stub engine used ONLY when no real engine is available.
            Returns placeholder MISSING data for all symbols.
            """

            async def get_enriched_quotes(
                self, symbols: List[str]
            ) -> List[Dict[str, Any]]:
                out: List[Dict[str, Any]] = []
                for s in symbols or []:
                    sym = (s or "").strip().upper()
                    if not sym:
                        continue
                    out.append(
                        {
                            "symbol": sym,
                            "data_quality": "MISSING",
                            "error": (
                                "Unified data engine (v2/v1) unavailable in "
                                "legacy_service."
                            ),
                        }
                    )
                return out

        _engine = _StubEngine()
        _ENGINE_MODE = "stub"
        _ENGINE_IS_STUB = True
        logger.error(
            "[LegacyService] Using stub DataEngine – legacy responses will "
            "contain MISSING placeholder data."
        )


async def _engine_get_enriched_quotes(symbols: List[str]) -> List[Any]:
    """
    Unified wrapper to call the underlying engine in any mode.

    Supports:
      - v2:  _engine.get_enriched_quotes(...)
      - v1:  core.data_engine.get_enriched_quotes(...)
      - stub: _engine.get_enriched_quotes(...) -> MISSING data
    """
    clean = [s.strip() for s in (symbols or []) if s and s.strip()]
    if not clean:
        return []

    if _ENGINE_MODE == "v2":
        return await _engine.get_enriched_quotes(clean)  # type: ignore[attr-defined]

    if _ENGINE_MODE == "v1_module" and _data_engine_module is not None:
        return await _data_engine_module.get_enriched_quotes(clean)  # type: ignore[attr-defined]

    # Stub mode
    return await _engine.get_enriched_quotes(clean)  # type: ignore[attr-defined]


# ----------------------------------------------------------------------
# CONSTANTS / TIMEZONES
# ----------------------------------------------------------------------

RIYADH_TZ = timezone(timedelta(hours=3))

# ----------------------------------------------------------------------
# SYMBOL HELPERS (aligned with KSA logic elsewhere)
# ----------------------------------------------------------------------


def _normalize_symbol(symbol: str) -> str:
    """
    Normalize symbols before sending to the engine:

    - TADAWUL:1120   -> 1120.SR
    - 1120.TADAWUL   -> 1120.SR
    - 1120           -> 1120.SR
    - 1120.SR        -> 1120.SR
    - AAPL           -> AAPL

    NOTE:
    - The engine itself may apply additional normalization.
    - This function ensures KSA numeric codes are pushed to .SR format.
    """
    s = (symbol or "").strip().upper()
    if not s:
        return ""

    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()

    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", ".SR")

    if s.isdigit():
        s = f"{s}.SR"

    return s


def _split_tickers_by_market(symbols: List[str]) -> Tuple[List[str], List[str]]:
    """
    Simple diagnostic split into KSA vs Global (for meta only).

    KSA:    endswith('.SR')
    Global: everything else
    """
    ksa: List[str] = []
    global_: List[str] = []
    for s in symbols or []:
        ss = (s or "").strip().upper()
        if not ss:
            continue
        if ss.endswith(".SR"):
            ksa.append(ss)
        else:
            global_.append(ss)
    return ksa, global_


# ----------------------------------------------------------------------
# DATA STRUCTURES
# ----------------------------------------------------------------------


@dataclass
class LegacyQuote:
    """
    Legacy quote representation, modeled on the original /v1/quote
    response used in PowerShell tests, but backed by UnifiedQuote-like data.

    NOTE:
    - Many fields may be None depending on provider data.
    """

    # Identity
    ticker: str
    symbol: str
    name: Optional[str] = None
    exchange: Optional[str] = None
    market: Optional[str] = None
    currency: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    sub_sector: Optional[str] = None
    listing_date: Optional[str] = None

    # Prices & change
    price: Optional[float] = None
    previous_close: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    change: Optional[float] = None
    change_percent: Optional[float] = None

    # Volumes & liquidity
    volume: Optional[float] = None
    avg_volume: Optional[float] = None
    market_cap: Optional[float] = None
    shares_outstanding: Optional[float] = None
    free_float: Optional[float] = None

    # 52-week data
    fifty_two_week_high: Optional[float] = None
    fifty_two_week_low: Optional[float] = None
    fifty_two_week_position_pct: Optional[float] = None  # 0–100

    # Fundamentals
    eps: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    profit_margin: Optional[float] = None
    debt_to_equity: Optional[float] = None
    revenue_growth: Optional[float] = None
    net_income_growth: Optional[float] = None
    ebitda_margin: Optional[float] = None
    operating_margin: Optional[float] = None

    # Risk / valuation extras
    beta: Optional[float] = None
    volatility_30d: Optional[float] = None
    volatility_30d_percent: Optional[float] = None
    ev_to_ebitda: Optional[float] = None
    price_to_sales: Optional[float] = None
    price_to_cash_flow: Optional[float] = None
    peg_ratio: Optional[float] = None

    # AI-style scoring & technicals
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    recommendation: Optional[str] = None
    valuation_label: Optional[str] = None
    rsi_14: Optional[float] = None
    macd: Optional[float] = None
    ma_20d: Optional[float] = None
    ma_50d: Optional[float] = None

    # Data quality & meta
    data_quality: str = "MISSING"
    data_source: Optional[str] = None
    last_updated_utc: Optional[datetime] = None
    last_updated_riyadh: Optional[datetime] = None
    error: Optional[str] = None


# ----------------------------------------------------------------------
# INTERNAL HELPERS – TYPES / MAPPING
# ----------------------------------------------------------------------


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _to_riyadh(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(RIYADH_TZ)


def _compute_52w_position_pct(
    price: Optional[float],
    low_52w: Optional[float],
    high_52w: Optional[float],
) -> Optional[float]:
    if price is None or low_52w is None or high_52w is None:
        return None
    span = high_52w - low_52w
    if span <= 0:
        return None
    try:
        pct = (price - low_52w) / span * 100.0
        return max(0.0, min(100.0, pct))
    except Exception:
        return None


def _quote_to_dict(q: Any) -> Dict[str, Any]:
    """
    Convert a UnifiedQuote-like object into a plain dict:

    - Supports pydantic models (.model_dump())
    - Plain dicts
    - Fallback to __dict__
    """
    if q is None:
        return {}
    if hasattr(q, "model_dump"):
        try:
            return q.model_dump()  # type: ignore[call-arg]
        except Exception:
            pass
    if isinstance(q, dict):
        return q
    return getattr(q, "__dict__", {}) or {}


def _g(data: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """
    Safe getter for multiple candidate keys.
    """
    for k in keys:
        if k in data and data[k] is not None:
            return data[k]
    return default


def _unified_to_legacy(quote: Any) -> LegacyQuote:
    """
    Map UnifiedQuote-like object/dict -> LegacyQuote.
    This is the central mapping used for all legacy responses.
    """
    data = _quote_to_dict(quote)

    if not data:
        # Fully missing quote from engine
        return LegacyQuote(
            ticker="",
            symbol="",
            data_quality="MISSING",
            error="No quote data returned from engine",
        )

    symbol = str(_g(data, "symbol", "ticker", default="").upper())

    name = _g(data, "name", "company_name", "longName", "shortName")
    exchange = _g(data, "exchange", "market", "primary_exchange")
    market_region = _g(data, "market_region", "region", "country", "market")
    currency = _g(data, "currency")
    sector = _g(data, "sector")
    industry = _g(data, "industry", "industryGroup")
    sub_sector = _g(data, "sub_sector", "industry_group")
    listing_date = _g(data, "listing_date")

    price = _safe_float(
        _g(data, "price", "last_price", "close", "regularMarketPrice")
    )
    prev_close = _safe_float(
        _g(data, "previous_close", "prev_close", "previousClose")
    )
    open_price = _safe_float(_g(data, "open", "regularMarketOpen"))
    high_price = _safe_float(_g(data, "high", "dayHigh", "regularMarketDayHigh"))
    low_price = _safe_float(_g(data, "low", "dayLow", "regularMarketDayLow"))
    change = _safe_float(_g(data, "change"))
    change_pct = _safe_float(
        _g(data, "change_pct", "change_percent", "changePercent")
    )

    volume = _safe_float(_g(data, "volume", "regularMarketVolume"))
    avg_volume = _safe_float(
        _g(data, "avg_volume", "avg_volume_30d", "average_volume_30d")
    )
    market_cap = _safe_float(_g(data, "market_cap", "marketCap"))
    shares_outstanding = _safe_float(_g(data, "shares_outstanding"))
    free_float = _safe_float(_g(data, "free_float"))

    high_52w = _safe_float(
        _g(data, "fifty_two_week_high", "high_52w", "yearHigh")
    )
    low_52w = _safe_float(
        _g(data, "fifty_two_week_low", "low_52w", "yearLow")
    )

    # Prefer engine-computed 52W position if present, else compute
    pos_52w = _safe_float(
        _g(data, "position_52w_percent", "fifty_two_week_position")
    )
    if pos_52w is None:
        pos_52w = _compute_52w_position_pct(price, low_52w, high_52w)

    eps = _safe_float(_g(data, "eps_ttm", "eps"))
    pe_ratio = _safe_float(_g(data, "pe_ttm", "pe_ratio", "pe"))
    pb_ratio = _safe_float(_g(data, "pb", "pb_ratio", "priceToBook"))
    dividend_yield = _safe_float(
        _g(data, "dividend_yield", "dividend_yield_percent")
    )
    roe = _safe_float(_g(data, "roe", "roe_percent", "returnOnEquity"))
    roa = _safe_float(_g(data, "roa", "roa_percent", "returnOnAssets"))
    profit_margin = _safe_float(
        _g(data, "profit_margin", "net_margin_percent", "profitMargins")
    )
    debt_to_equity = _safe_float(
        _g(data, "debt_to_equity", "debtToEquity")
    )

    revenue_growth = _safe_float(
        _g(data, "revenue_growth_percent", "revenueGrowth")
    )
    net_income_growth = _safe_float(
        _g(data, "net_income_growth_percent", "netIncomeGrowth")
    )
    ebitda_margin = _safe_float(
        _g(data, "ebitda_margin_percent", "ebitdaMargin")
    )
    operating_margin = _safe_float(
        _g(data, "operating_margin_percent", "operatingMargin")
    )

    beta = _safe_float(_g(data, "beta"))
    volatility_30d = _safe_float(
        _g(data, "volatility_30d", "volatility_30d_percent")
    )
    volatility_30d_percent = _safe_float(
        _g(data, "volatility_30d_percent", "volatility_30d")
    )
    ev_to_ebitda = _safe_float(_g(data, "ev_to_ebitda", "evToEbitda"))
    price_to_sales = _safe_float(
        _g(data, "price_to_sales", "priceToSales")
    )
    price_to_cash_flow = _safe_float(
        _g(data, "price_to_cash_flow", "priceToCashFlow")
    )
    peg_ratio = _safe_float(_g(data, "peg_ratio", "pegRatio"))

    value_score = _safe_float(_g(data, "value_score"))
    quality_score = _safe_float(_g(data, "quality_score"))
    momentum_score = _safe_float(_g(data, "momentum_score"))
    opportunity_score = _safe_float(_g(data, "opportunity_score"))
    recommendation = _g(data, "recommendation")
    valuation_label = _g(data, "valuation_label")

    rsi_14 = _safe_float(_g(data, "rsi_14"))
    macd = _safe_float(_g(data, "macd"))
    ma_20d = _safe_float(_g(data, "ma_20d"))
    ma_50d = _safe_float(_g(data, "ma_50d"))

    data_quality = str(
        _g(data, "data_quality", "data_quality_level", default="MISSING")
    )

    # Last updated
    last_updated_raw = _g(data, "last_updated_utc", "as_of_utc")
    last_utc: Optional[datetime] = None
    if isinstance(last_updated_raw, datetime):
        last_utc = last_updated_raw
    elif isinstance(last_updated_raw, str):
        try:
            last_utc = datetime.fromisoformat(last_updated_raw)
        except Exception:
            last_utc = None

    last_riyadh = _to_riyadh(last_utc) if last_utc else None

    # Sources -> data_source string
    raw_sources = _g(data, "sources", default=[]) or []
    sources: List[str] = []
    try:
        for s in raw_sources:
            if hasattr(s, "provider"):
                sources.append(str(getattr(s, "provider")))
            elif isinstance(s, dict) and "provider" in s:
                sources.append(str(s["provider"]))
            else:
                sources.append(str(s))
    except Exception:
        pass
    data_source = ", ".join([s for s in sources if s]) or None

    error = _g(data, "error", default=None)

    return LegacyQuote(
        ticker=symbol,
        symbol=symbol,
        name=name,
        exchange=exchange,
        market=market_region,
        currency=currency,
        sector=sector,
        industry=industry,
        sub_sector=sub_sector,
        listing_date=listing_date,
        price=price,
        previous_close=prev_close,
        open=open_price,
        high=high_price,
        low=low_price,
        change=change,
        change_percent=change_pct,
        volume=volume,
        avg_volume=avg_volume,
        market_cap=market_cap,
        shares_outstanding=shares_outstanding,
        free_float=free_float,
        fifty_two_week_high=high_52w,
        fifty_two_week_low=low_52w,
        fifty_two_week_position_pct=pos_52w,
        eps=eps,
        pe_ratio=pe_ratio,
        pb_ratio=pb_ratio,
        dividend_yield=dividend_yield,
        roe=roe,
        roa=roa,
        profit_margin=profit_margin,
        debt_to_equity=debt_to_equity,
        revenue_growth=revenue_growth,
        net_income_growth=net_income_growth,
        ebitda_margin=ebitda_margin,
        operating_margin=operating_margin,
        beta=beta,
        volatility_30d=volatility_30d,
        volatility_30d_percent=volatility_30d_percent,
        ev_to_ebitda=ev_to_ebitda,
        price_to_sales=price_to_sales,
        price_to_cash_flow=price_to_cash_flow,
        peg_ratio=peg_ratio,
        value_score=value_score,
        quality_score=quality_score,
        momentum_score=momentum_score,
        opportunity_score=opportunity_score,
        recommendation=recommendation,
        valuation_label=valuation_label,
        rsi_14=rsi_14,
        macd=macd,
        ma_20d=ma_20d,
        ma_50d=ma_50d,
        data_quality=data_quality,
        data_source=data_source,
        last_updated_utc=last_utc,
        last_updated_riyadh=last_riyadh,
        error=error,
    )


def _legacy_to_dict(lq: LegacyQuote) -> Dict[str, Any]:
    """
    Convert LegacyQuote dataclass to plain dict, ready for JSON response.
    """
    return {
        "ticker": lq.ticker,
        "symbol": lq.symbol,
        "name": lq.name,
        "exchange": lq.exchange,
        "market": lq.market,
        "currency": lq.currency,
        "sector": lq.sector,
        "industry": lq.industry,
        "sub_sector": lq.sub_sector,
        "listing_date": lq.listing_date,
        "price": lq.price,
        "previous_close": lq.previous_close,
        "open": lq.open,
        "high": lq.high,
        "low": lq.low,
        "change": lq.change,
        "change_percent": lq.change_percent,
        "volume": lq.volume,
        "avg_volume": lq.avg_volume,
        "market_cap": lq.market_cap,
        "shares_outstanding": lq.shares_outstanding,
        "free_float": lq.free_float,
        "fifty_two_week_high": lq.fifty_two_week_high,
        "fifty_two_week_low": lq.fifty_two_week_low,
        "fifty_two_week_position_pct": lq.fifty_two_week_position_pct,
        "eps": lq.eps,
        "pe_ratio": lq.pe_ratio,
        "pb_ratio": lq.pb_ratio,
        "dividend_yield": lq.dividend_yield,
        "roe": lq.roe,
        "roa": lq.roa,
        "profit_margin": lq.profit_margin,
        "debt_to_equity": lq.debt_to_equity,
        "revenue_growth": lq.revenue_growth,
        "net_income_growth": lq.net_income_growth,
        "ebitda_margin": lq.ebitda_margin,
        "operating_margin": lq.operating_margin,
        "beta": lq.beta,
        "volatility_30d": lq.volatility_30d,
        "volatility_30d_percent": lq.volatility_30d_percent,
        "ev_to_ebitda": lq.ev_to_ebitda,
        "price_to_sales": lq.price_to_sales,
        "price_to_cash_flow": lq.price_to_cash_flow,
        "peg_ratio": lq.peg_ratio,
        "value_score": lq.value_score,
        "quality_score": lq.quality_score,
        "momentum_score": lq.momentum_score,
        "opportunity_score": lq.opportunity_score,
        "recommendation": lq.recommendation,
        "valuation_label": lq.valuation_label,
        "rsi_14": lq.rsi_14,
        "macd": lq.macd,
        "ma_20d": lq.ma_20d,
        "ma_50d": lq.ma_50d,
        "data_quality": lq.data_quality,
        "data_source": lq.data_source,
        "last_updated_utc": (
            lq.last_updated_utc.isoformat() if lq.last_updated_utc else None
        ),
        "last_updated_riyadh": (
            lq.last_updated_riyadh.isoformat() if lq.last_updated_riyadh else None
        ),
        "error": lq.error,
    }


def _build_sheet_headers() -> List[str]:
    """
    A compact legacy header set for Google Sheets.

    SAFE for:
      - Legacy Sheets tabs
      - Ad-hoc test tabs

    The 9-page main dashboard should use the enriched/advanced sheet-rows
    endpoints instead; this is only for legacy-style support.
    """
    return [
        "Symbol",
        "Name",
        "Exchange/Market",
        "Currency",
        "Sector",
        "Industry",
        "Price",
        "Previous Close",
        "Change",
        "Change %",
        "Volume",
        "Market Cap",
        "P/E",
        "P/B",
        "Dividend Yield",
        "ROE",
        "Profit Margin",
        "52W High",
        "52W Low",
        "52W Position %",
        "Data Quality",
        "Data Source",
        "Last Updated (UTC)",
        "Last Updated (Riyadh)",
        "Error",
    ]


def _legacy_to_sheet_row(lq: LegacyQuote) -> List[Any]:
    """
    Flatten LegacyQuote into a single row for setValues().
    """
    return [
        lq.symbol,
        lq.name or "",
        lq.exchange or lq.market or "",
        lq.currency or "",
        lq.sector or "",
        lq.industry or "",
        lq.price,
        lq.previous_close,
        lq.change,
        lq.change_percent,
        lq.volume,
        lq.market_cap,
        lq.pe_ratio,
        lq.pb_ratio,
        lq.dividend_yield,
        lq.roe,
        lq.profit_margin,
        lq.fifty_two_week_high,
        lq.fifty_two_week_low,
        lq.fifty_two_week_position_pct,
        lq.data_quality,
        lq.data_source or "",
        lq.last_updated_utc.isoformat() if lq.last_updated_utc else None,
        lq.last_updated_riyadh.isoformat() if lq.last_updated_riyadh else None,
        lq.error or "",
    ]


# ----------------------------------------------------------------------
# PUBLIC API – LEGACY QUOTES
# ----------------------------------------------------------------------


async def get_legacy_quotes(tickers: List[str]) -> Dict[str, Any]:
    """
    Core function: return legacy-style quotes payload.

    Parameters
    ----------
    tickers : list[str]
        List of symbols, e.g. ["1120.SR", "1180.SR", "AAPL"].

    RETURNS
    -------
    dict:
        {
            "quotes": [ { ... legacy fields ... } ],
            "meta":   {
                "requested": [...],
                "normalized": [...],
                "count": int,
                "markets": { "ksa": int, "global": int },
                "app": ...,
                "version": ...,
                "backend_url": ...,
                "engine_mode": ...,
                "engine_is_stub": bool,
                "enabled_providers": [...],
                "timestamp_utc": ...,
                "note": ...
            }
        }
    """
    raw_symbols = [t.strip() for t in (tickers or []) if t and t.strip()]
    normalized = [_normalize_symbol(t) for t in raw_symbols]
    normalized = [s for s in normalized if s]

    ksa_syms, global_syms = _split_tickers_by_market(normalized)

    if not normalized:
        return {
            "quotes": [],
            "meta": {
                "requested": raw_symbols,
                "normalized": [],
                "count": 0,
                "markets": {"ksa": 0, "global": 0},
                "app": APP_NAME,
                "version": APP_VERSION,
                "backend_url": BACKEND_BASE_URL,
                "engine_mode": _ENGINE_MODE,
                "engine_is_stub": _ENGINE_IS_STUB,
                "enabled_providers": _ENABLED_PROVIDERS,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "note": "No symbols provided.",
            },
        }

    # Call the unified engine (handles KSA vs Global internally)
    try:
        unified_quotes = await _engine_get_enriched_quotes(normalized)
    except Exception as exc:
        logger.exception(
            "[LegacyService] Data engine error for symbols=%s", normalized
        )
        legacy_quotes: List[LegacyQuote] = [
            LegacyQuote(
                ticker=sym,
                symbol=sym,
                data_quality="MISSING",
                error=f"Data engine error: {exc}",
            )
            for sym in normalized
        ]
        return {
            "quotes": [_legacy_to_dict(lq) for lq in legacy_quotes],
            "meta": {
                "requested": raw_symbols,
                "normalized": normalized,
                "count": len(legacy_quotes),
                "markets": {
                    "ksa": len(ksa_syms),
                    "global": len(global_syms),
                },
                "app": APP_NAME,
                "version": APP_VERSION,
                "backend_url": BACKEND_BASE_URL,
                "engine_mode": _ENGINE_MODE,
                "engine_is_stub": _ENGINE_IS_STUB,
                "enabled_providers": _ENABLED_PROVIDERS,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "note": "Failed to fetch data from unified data engine.",
            },
        }

    legacy_quotes: List[LegacyQuote] = []
    for idx, sym in enumerate(normalized):
        uq = unified_quotes[idx] if idx < len(unified_quotes) else None
        try:
            if uq is None:
                raise ValueError("No quote returned from engine")
            lq = _unified_to_legacy(uq)
            if not lq.symbol:
                lq.symbol = sym
                lq.ticker = sym
        except Exception as exc:
            logger.exception(
                "[LegacyService] Legacy mapping error for %s", sym, exc_info=exc
            )
            lq = LegacyQuote(
                ticker=sym,
                symbol=sym,
                data_quality="MISSING",
                error=f"Legacy mapping error: {exc}",
            )
        legacy_quotes.append(lq)

    payload = {
        "quotes": [_legacy_to_dict(lq) for lq in legacy_quotes],
        "meta": {
            "requested": raw_symbols,
            "normalized": normalized,
            "count": len(legacy_quotes),
            "markets": {
                "ksa": len(ksa_syms),
                "global": len(global_syms),
            },
            "app": APP_NAME,
            "version": APP_VERSION,
            "backend_url": BACKEND_BASE_URL,
            "engine_mode": _ENGINE_MODE,
            "engine_is_stub": _ENGINE_IS_STUB,
            "enabled_providers": _ENABLED_PROVIDERS,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "note": (
                "Legacy format built on UnifiedQuote-style engine "
                f"(mode={_ENGINE_MODE}, stub={_ENGINE_IS_STUB}; "
                "KSA via Tadawul/Argaam providers; "
                "no direct EODHD calls inside legacy_service)."
            ),
        },
    }
    return payload


# ----------------------------------------------------------------------
# PUBLIC API – GOOGLE SHEETS HELPERS
# ----------------------------------------------------------------------


async def build_legacy_sheet_payload(tickers: List[str]) -> Dict[str, Any]:
    """
    Build a Google Sheets–friendly payload using the legacy quote format.

    RETURNS
    -------
    dict:
        {
            "headers": [...],
            "rows": [[...], ...],
            "meta": { ... same as get_legacy_quotes.meta ... }
        }

    This can be used by:
        - google_sheets_service (write_range)
        - Google Apps Script bridge
        - any script that needs simple headers/rows.

    NOTE
    - The 9-page Ultimate Dashboard should prefer enriched / analysis
      sheet-rows endpoints; this is mainly for legacy/test tabs and
      PowerShell experiments.
    """
    legacy_payload = await get_legacy_quotes(tickers)
    quotes = legacy_payload.get("quotes", [])
    meta = legacy_payload.get("meta", {})

    headers = _build_sheet_headers()
    rows: List[List[Any]] = []

    for q in quotes:
        # q is already a dict in legacy shape; reconstruct LegacyQuote for row:
        lq = LegacyQuote(
            ticker=q.get("ticker", ""),
            symbol=q.get("symbol", ""),
            name=q.get("name"),
            exchange=q.get("exchange"),
            market=q.get("market"),
            currency=q.get("currency"),
            sector=q.get("sector"),
            industry=q.get("industry"),
            sub_sector=q.get("sub_sector"),
            listing_date=q.get("listing_date"),
            price=q.get("price"),
            previous_close=q.get("previous_close"),
            open=q.get("open"),
            high=q.get("high"),
            low=q.get("low"),
            change=q.get("change"),
            change_percent=q.get("change_percent"),
            volume=q.get("volume"),
            avg_volume=q.get("avg_volume"),
            market_cap=q.get("market_cap"),
            shares_outstanding=q.get("shares_outstanding"),
            free_float=q.get("free_float"),
            fifty_two_week_high=q.get("fifty_two_week_high"),
            fifty_two_week_low=q.get("fifty_two_week_low"),
            fifty_two_week_position_pct=q.get("fifty_two_week_position_pct"),
            eps=q.get("eps"),
            pe_ratio=q.get("pe_ratio"),
            pb_ratio=q.get("pb_ratio"),
            dividend_yield=q.get("dividend_yield"),
            roe=q.get("roe"),
            roa=q.get("roa"),
            profit_margin=q.get("profit_margin"),
            debt_to_equity=q.get("debt_to_equity"),
            revenue_growth=q.get("revenue_growth"),
            net_income_growth=q.get("net_income_growth"),
            ebitda_margin=q.get("ebitda_margin"),
            operating_margin=q.get("operating_margin"),
            beta=q.get("beta"),
            volatility_30d=q.get("volatility_30d"),
            volatility_30d_percent=q.get("volatility_30d_percent"),
            ev_to_ebitda=q.get("ev_to_ebitda"),
            price_to_sales=q.get("price_to_sales"),
            price_to_cash_flow=q.get("price_to_cash_flow"),
            peg_ratio=q.get("peg_ratio"),
            value_score=q.get("value_score"),
            quality_score=q.get("quality_score"),
            momentum_score=q.get("momentum_score"),
            opportunity_score=q.get("opportunity_score"),
            recommendation=q.get("recommendation"),
            valuation_label=q.get("valuation_label"),
            rsi_14=q.get("rsi_14"),
            macd=q.get("macd"),
            ma_20d=q.get("ma_20d"),
            ma_50d=q.get("ma_50d"),
            data_quality=q.get("data_quality", "MISSING"),
            data_source=q.get("data_source"),
            last_updated_utc=None,
            last_updated_riyadh=None,
            error=q.get("error"),
        )

        # Optionally parse ISO timestamps back (defensive; safe to ignore failures)
        lu_utc = q.get("last_updated_utc")
        lu_riyadh = q.get("last_updated_riyadh")
        try:
            if lu_utc:
                lq.last_updated_utc = datetime.fromisoformat(lu_utc)
        except Exception:
            pass
        try:
            if lu_riyadh:
                lq.last_updated_riyadh = datetime.fromisoformat(lu_riyadh)
        except Exception:
            pass

        rows.append(_legacy_to_sheet_row(lq))

    return {
        "headers": headers,
        "rows": rows,
        "meta": meta,
    }


__all__ = [
    "LegacyQuote",
    "get_legacy_quotes",
    "build_legacy_sheet_payload",
]
