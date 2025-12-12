```python
"""
legacy_service.py
------------------------------------------------------------
LEGACY SERVICE BRIDGE – v3.6 (Aligned with App v4.5+ / Engine v2.x)

GOAL
- Provide a stable "legacy" layer on top of the unified data engine,
  so old clients (PowerShell, early Google Sheets scripts, test tabs)
  can still receive the classic /v1/quote-style payload.

KEY PRINCIPLES
- NO direct EODHD / Yahoo / providers calls here.
- All data comes via the unified data engine (v2 preferred, v1 module fallback).
- KSA (.SR) tickers are handled by Tadawul/Argaam providers inside the engine.
- Strongly defensive: normalization, mapping, and failures never crash the app.

PUBLIC API
- get_legacy_quotes(tickers: List[str]) -> Dict[str, Any]
- build_legacy_sheet_payload(tickers: List[str]) -> Dict[str, Any]
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

try:  # pragma: no cover
    import env as _env_mod  # type: ignore

    _SETTINGS = getattr(_env_mod, "settings", None)
except Exception:  # pragma: no cover
    _env_mod = None  # type: ignore
    _SETTINGS = None
    logger.warning(
        "[LegacyService] env.py not available. Using OS environment variables only."
    )


@dataclass
class _SettingsFallback:
    app_env: str = os.getenv("APP_ENV", "production")
    app_name: str = os.getenv("APP_NAME", "Tadawul Fast Bridge")
    app_version: str = os.getenv("APP_VERSION", "4.5.0")
    backend_base_url: str = (os.getenv("BACKEND_BASE_URL", "") or "").strip()


def _env_get_str(name: str, default: str = "") -> str:
    # Prefer env.settings.<snake_case> if present
    if _SETTINGS is not None:
        mapping = {
            "APP_ENV": "app_env",
            "APP_NAME": "app_name",
            "APP_VERSION": "app_version",
            "BACKEND_BASE_URL": "backend_base_url",
        }
        attr = mapping.get(name)
        if attr and hasattr(_SETTINGS, attr):
            try:
                v = getattr(_SETTINGS, attr)
                if v is None:
                    return default
                return str(v).strip()
            except Exception:
                pass

    # Prefer env.<NAME> constants (back-compat)
    if _env_mod is not None and hasattr(_env_mod, name):
        try:
            v = getattr(_env_mod, name)
            if v is None:
                return default
            return str(v).strip()
        except Exception:
            return default

    return (os.getenv(name, default) or default).strip()


settings = getattr(_env_mod, "settings", _SettingsFallback())

APP_NAME: str = getattr(settings, "app_name", _env_get_str("APP_NAME", "Tadawul Fast Bridge"))
APP_VERSION: str = getattr(settings, "app_version", _env_get_str("APP_VERSION", "4.5.0"))
BACKEND_BASE_URL: str = getattr(settings, "backend_base_url", _env_get_str("BACKEND_BASE_URL", "")).strip()

# ----------------------------------------------------------------------
# DATA ENGINE IMPORT / FALLBACKS (v2 preferred, v1 module, stub)
# ----------------------------------------------------------------------

_ENGINE_MODE: str = "stub"  # "v2", "v1_module", "stub"
_ENGINE_IS_STUB: bool = True
_engine: Any = None
_data_engine_module: Any = None
_ENABLED_PROVIDERS: List[str] = []

try:  # Preferred: class-based engine (core.data_engine_v2)
    from core.data_engine_v2 import DataEngine as _V2DataEngine  # type: ignore

    engine_kwargs: Dict[str, Any] = {}

    # Optional tuning via env.py constants or settings (defensive)
    def _maybe(name: str) -> Any:
        if _env_mod is not None and hasattr(_env_mod, name):
            return getattr(_env_mod, name)
        return None

    cache_ttl = _maybe("ENGINE_CACHE_TTL_SECONDS")
    if cache_ttl is not None:
        engine_kwargs["cache_ttl"] = cache_ttl

    provider_timeout = _maybe("ENGINE_PROVIDER_TIMEOUT_SECONDS")
    if provider_timeout is not None:
        engine_kwargs["provider_timeout"] = provider_timeout

    enabled_providers = _maybe("ENABLED_PROVIDERS")
    if enabled_providers:
        engine_kwargs["enabled_providers"] = enabled_providers

    enable_adv = _maybe("ENGINE_ENABLE_ADVANCED_ANALYSIS")
    if enable_adv is not None:
        engine_kwargs["enable_advanced_analysis"] = enable_adv

    try:
        _engine = _V2DataEngine(**engine_kwargs) if engine_kwargs else _V2DataEngine()
    except TypeError:
        _engine = _V2DataEngine()

    _ENGINE_MODE = "v2"
    _ENGINE_IS_STUB = False
    _ENABLED_PROVIDERS = list(getattr(_engine, "enabled_providers", []) or [])
    logger.info(
        "[LegacyService] Using DataEngine v2 (kwargs=%s, enabled_providers=%s).",
        list(engine_kwargs.keys()),
        _ENABLED_PROVIDERS,
    )

except Exception as e_v2:  # pragma: no cover
    logger.exception("[LegacyService] Failed to init core.data_engine_v2.DataEngine: %s", e_v2)
    try:
        # Fallback: module-level engine (core.data_engine.get_enriched_quotes)
        from core import data_engine as _data_engine_module  # type: ignore

        _ENGINE_MODE = "v1_module"
        _ENGINE_IS_STUB = False
        logger.warning("[LegacyService] Falling back to core.data_engine module-level API.")
    except Exception as e_v1:  # pragma: no cover
        logger.exception("[LegacyService] Failed to import core.data_engine fallback: %s", e_v1)

        class _StubEngine:
            async def get_enriched_quotes(self, symbols: List[str]) -> List[Dict[str, Any]]:
                out: List[Dict[str, Any]] = []
                for s in symbols or []:
                    sym = (s or "").strip().upper()
                    if not sym:
                        continue
                    out.append(
                        {
                            "symbol": sym,
                            "data_quality": "MISSING",
                            "error": "Unified data engine (v2/v1) unavailable in legacy_service.",
                        }
                    )
                return out

        _engine = _StubEngine()
        _ENGINE_MODE = "stub"
        _ENGINE_IS_STUB = True
        logger.error("[LegacyService] Using stub engine – legacy responses will be MISSING placeholders.")


async def _engine_get_enriched_quotes(symbols: List[str]) -> List[Any]:
    clean = [s.strip() for s in (symbols or []) if s and s.strip()]
    if not clean:
        return []

    if _ENGINE_MODE == "v2":
        return await _engine.get_enriched_quotes(clean)  # type: ignore[attr-defined]

    if _ENGINE_MODE == "v1_module" and _data_engine_module is not None:
        return await _data_engine_module.get_enriched_quotes(clean)  # type: ignore[attr-defined]

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
    - aapl           -> AAPL
    """
    s = (symbol or "").strip().upper()
    if not s:
        return ""

    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()

    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", ".SR")

    # Normalize bare numeric KSA codes to .SR
    if s.isdigit():
        s = f"{s}.SR"

    return s


def _split_tickers_by_market(symbols: List[str]) -> Tuple[List[str], List[str]]:
    ksa: List[str] = []
    global_: List[str] = []
    for s in symbols or []:
        ss = (s or "").strip().upper()
        if not ss:
            continue
        (ksa if ss.endswith(".SR") else global_).append(ss)
    return ksa, global_


def _dedupe_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for it in items or []:
        if it and it not in seen:
            seen.add(it)
            out.append(it)
    return out


# ----------------------------------------------------------------------
# DATA STRUCTURES
# ----------------------------------------------------------------------

@dataclass
class LegacyQuote:
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
# INTERNAL HELPERS – mapping and typing
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
    for k in keys:
        if k in data and data[k] is not None:
            return data[k]
    return default


def _parse_iso_dt(x: Any) -> Optional[datetime]:
    if isinstance(x, datetime):
        return x
    if isinstance(x, str) and x.strip():
        s = x.strip()
        try:
            dt = datetime.fromisoformat(s)
            return dt
        except Exception:
            return None
    return None


def _unified_to_legacy(quote: Any, fallback_symbol: str = "") -> LegacyQuote:
    data = _quote_to_dict(quote)
    symbol = str(_g(data, "symbol", "ticker", default=fallback_symbol) or "").upper()

    if not symbol:
        symbol = (fallback_symbol or "").upper()

    name = _g(data, "name", "company_name", "longName", "shortName")
    exchange = _g(data, "exchange", "primary_exchange", "exchangeName")
    market_region = _g(data, "market", "market_region", "region", "country")
    currency = _g(data, "currency")
    sector = _g(data, "sector")
    industry = _g(data, "industry", "industryGroup")
    sub_sector = _g(data, "sub_sector", "industry_group")
    listing_date = _g(data, "listing_date")

    price = _safe_float(_g(data, "price", "last_price", "close", "regularMarketPrice"))
    prev_close = _safe_float(_g(data, "previous_close", "prev_close", "previousClose"))
    open_price = _safe_float(_g(data, "open", "regularMarketOpen"))
    high_price = _safe_float(_g(data, "high", "dayHigh", "regularMarketDayHigh"))
    low_price = _safe_float(_g(data, "low", "dayLow", "regularMarketDayLow"))

    change = _safe_float(_g(data, "change"))
    change_pct = _safe_float(_g(data, "change_pct", "change_percent", "changePercent"))

    volume = _safe_float(_g(data, "volume", "regularMarketVolume"))
    avg_volume = _safe_float(_g(data, "avg_volume", "avg_volume_30d", "average_volume_30d"))
    market_cap = _safe_float(_g(data, "market_cap", "marketCap"))
    shares_outstanding = _safe_float(_g(data, "shares_outstanding"))
    free_float = _safe_float(_g(data, "free_float"))

    high_52w = _safe_float(_g(data, "high_52w", "fifty_two_week_high", "yearHigh"))
    low_52w = _safe_float(_g(data, "low_52w", "fifty_two_week_low", "yearLow"))

    pos_52w = _safe_float(_g(data, "position_52w_percent", "fifty_two_week_position"))
    if pos_52w is None:
        pos_52w = _compute_52w_position_pct(price, low_52w, high_52w)

    eps = _safe_float(_g(data, "eps_ttm", "eps"))
    pe_ratio = _safe_float(_g(data, "pe_ttm", "pe_ratio", "pe"))
    pb_ratio = _safe_float(_g(data, "pb", "pb_ratio", "priceToBook"))
    dividend_yield = _safe_float(_g(data, "dividend_yield", "dividend_yield_percent"))
    roe = _safe_float(_g(data, "roe", "roe_percent", "returnOnEquity"))
    roa = _safe_float(_g(data, "roa", "roa_percent", "returnOnAssets"))
    profit_margin = _safe_float(_g(data, "profit_margin", "net_margin_percent", "profitMargins"))
    debt_to_equity = _safe_float(_g(data, "debt_to_equity", "debtToEquity"))

    revenue_growth = _safe_float(_g(data, "revenue_growth_percent", "revenueGrowth"))
    net_income_growth = _safe_float(_g(data, "net_income_growth_percent", "netIncomeGrowth"))
    ebitda_margin = _safe_float(_g(data, "ebitda_margin_percent", "ebitdaMargin"))
    operating_margin = _safe_float(_g(data, "operating_margin_percent", "operatingMargin"))

    beta = _safe_float(_g(data, "beta"))
    volatility_30d = _safe_float(_g(data, "volatility_30d"))
    volatility_30d_percent = _safe_float(_g(data, "volatility_30d_percent"))
    ev_to_ebitda = _safe_float(_g(data, "ev_to_ebitda", "evToEbitda"))
    price_to_sales = _safe_float(_g(data, "price_to_sales", "priceToSales"))
    price_to_cash_flow = _safe_float(_g(data, "price_to_cash_flow", "priceToCashFlow"))
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

    data_quality = str(_g(data, "data_quality", "data_quality_level", default="MISSING"))
    error = _g(data, "error", default=None)

    # last updated
    last_utc = _parse_iso_dt(_g(data, "last_updated_utc", "as_of_utc"))
    last_riyadh = _to_riyadh(last_utc) if last_utc else None

    # sources list -> "a,b,c"
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
        "last_updated_utc": lq.last_updated_utc.isoformat() if lq.last_updated_utc else None,
        "last_updated_riyadh": lq.last_updated_riyadh.isoformat() if lq.last_updated_riyadh else None,
        "error": lq.error,
    }


# ----------------------------------------------------------------------
# SHEETS helpers (legacy, compact)
# ----------------------------------------------------------------------

def _build_sheet_headers() -> List[str]:
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
# PUBLIC API
# ----------------------------------------------------------------------

async def get_legacy_quotes(tickers: List[str]) -> Dict[str, Any]:
    raw = [t.strip() for t in (tickers or []) if t and t.strip()]
    normalized = [_normalize_symbol(t) for t in raw]
    normalized = [s for s in normalized if s]
    normalized = _dedupe_preserve_order(normalized)

    ksa_syms, global_syms = _split_tickers_by_market(normalized)

    now_utc = datetime.now(timezone.utc).isoformat()

    if not normalized:
        return {
            "quotes": [],
            "meta": {
                "requested": raw,
                "normalized": [],
                "count": 0,
                "markets": {"ksa": 0, "global": 0},
                "app": APP_NAME,
                "version": APP_VERSION,
                "backend_url": BACKEND_BASE_URL,
                "engine_mode": _ENGINE_MODE,
                "engine_is_stub": _ENGINE_IS_STUB,
                "enabled_providers": _ENABLED_PROVIDERS,
                "timestamp_utc": now_utc,
                "note": "No symbols provided.",
            },
        }

    try:
        unified_quotes = await _engine_get_enriched_quotes(normalized)
    except Exception as exc:
        logger.exception("[LegacyService] Engine error for symbols=%s", normalized)
        quotes = [
            _legacy_to_dict(
                LegacyQuote(
                    ticker=s,
                    symbol=s,
                    data_quality="MISSING",
                    error=f"Data engine error: {exc}",
                )
            )
            for s in normalized
        ]
        return {
            "quotes": quotes,
            "meta": {
                "requested": raw,
                "normalized": normalized,
                "count": len(quotes),
                "markets": {"ksa": len(ksa_syms), "global": len(global_syms)},
                "app": APP_NAME,
                "version": APP_VERSION,
                "backend_url": BACKEND_BASE_URL,
                "engine_mode": _ENGINE_MODE,
                "engine_is_stub": _ENGINE_IS_STUB,
                "enabled_providers": _ENABLED_PROVIDERS,
                "timestamp_utc": now_utc,
                "note": "Failed to fetch data from unified engine.",
            },
        }

    legacy_list: List[LegacyQuote] = []
    for i, sym in enumerate(normalized):
        uq = unified_quotes[i] if i < len(unified_quotes) else None
        try:
            if uq is None:
                raise ValueError("No quote returned from engine")
            legacy_list.append(_unified_to_legacy(uq, fallback_symbol=sym))
        except Exception as exc:
            logger.exception("[LegacyService] Mapping error for %s", sym)
            legacy_list.append(
                LegacyQuote(
                    ticker=sym,
                    symbol=sym,
                    data_quality="MISSING",
                    error=f"Legacy mapping error: {exc}",
                )
            )

    return {
        "quotes": [_legacy_to_dict(lq) for lq in legacy_list],
        "meta": {
            "requested": raw,
            "normalized": normalized,
            "count": len(legacy_list),
            "markets": {"ksa": len(ksa_syms), "global": len(global_syms)},
            "app": APP_NAME,
            "version": APP_VERSION,
            "backend_url": BACKEND_BASE_URL,
            "engine_mode": _ENGINE_MODE,
            "engine_is_stub": _ENGINE_IS_STUB,
            "enabled_providers": _ENABLED_PROVIDERS,
            "timestamp_utc": now_utc,
            "note": (
                "Legacy format built on unified engine (no direct provider calls here). "
                "KSA numeric symbols normalized to .SR."
            ),
        },
    }


async def build_legacy_sheet_payload(tickers: List[str]) -> Dict[str, Any]:
    payload = await get_legacy_quotes(tickers)
    quotes = payload.get("quotes", []) or []
    meta = payload.get("meta", {}) or {}

    headers = _build_sheet_headers()
    rows: List[List[Any]] = []

    for q in quotes:
        # q is dict (legacy). Convert quickly without strict parsing.
        lq = LegacyQuote(
            ticker=q.get("ticker", "") or "",
            symbol=q.get("symbol", "") or "",
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
            data_quality=q.get("data_quality", "MISSING") or "MISSING",
            data_source=q.get("data_source"),
            last_updated_utc=_parse_iso_dt(q.get("last_updated_utc")),
            last_updated_riyadh=_parse_iso_dt(q.get("last_updated_riyadh")),
            error=q.get("error"),
        )
        rows.append(_legacy_to_sheet_row(lq))

    return {"headers": headers, "rows": rows, "meta": meta}


__all__ = [
    "LegacyQuote",
    "get_legacy_quotes",
    "build_legacy_sheet_payload",
]
```
