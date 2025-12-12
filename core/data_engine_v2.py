"""
core/data_engine_v2.py
===============================================
Core Data & Analysis Engine - v2.8
(Global fundamentals-enriched, Sheets-aligned, hardened + derived-fields)

Author: Emad Bahbah (with GPT-5.2 Thinking)

Key features
------------
- Async, multi-provider quote engine:
    • FMP (price + basic fundamentals)
    • EODHD (real-time + optional deep fundamentals)  [GLOBAL ONLY]
    • Finnhub (real-time price)
- KSA (.SR) tickers are KSA-safe:
    • Never call EODHD for .SR symbols.
    • If legacy core.data_engine exists, it is used as the primary KSA delegate.
- In-memory cache with TTL.
- UnifiedQuote aligned with:
    • routes/enriched_quote
    • routes/ai_analysis
    • legacy_service
    • 9-page Google Sheets philosophy
- Deterministic scoring (Value / Quality / Momentum / Opportunity) + recommendation.
- Derived fields (safe):
    • change, change %, 52W position, value_traded, turnover_rate, spread_percent,
      liquidity_score (heuristic).
- Extremely defensive:
    • Never raises on normal usage.
    • Hardened env parsing.
    • Provider failures never crash the engine.

Configuration (env vars)
------------------------
ENGINE_CACHE_TTL_SECONDS         # optional; seconds, overrides DATAENGINE_CACHE_TTL
DATAENGINE_CACHE_TTL             # optional; seconds, default 120
ENGINE_PROVIDER_TIMEOUT_SECONDS  # optional; overrides DATAENGINE_TIMEOUT
DATAENGINE_TIMEOUT               # optional; per-provider timeout in seconds, default 10
ENGINE_MAX_CONCURRENCY           # optional; global semaphore for provider calls, default 25
ENABLED_PROVIDERS                # e.g. "fmp,eodhd,finnhub" default "fmp"
PRIMARY_PROVIDER                 # optional; if set and in enabled list, used first
LOCAL_TIMEZONE                   # optional; default "Asia/Riyadh"
ENABLE_ADVANCED_ANALYSIS         # optional; "1/true/on" enable, "0/false/off" disable
FMP_DEEP_ENRICH                  # optional; "1" enables extra FMP endpoints (can be heavy)

Provider API keys
-----------------
FMP_API_KEY
EODHD_BASE_URL                   # optional; default "https://eodhd.com/api"
EODHD_API_KEY                    # optional
FINNHUB_API_KEY                  # optional
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import httpx
from pydantic import BaseModel, Field

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Optional import of v1 engine for KSA delegation / global fallback
# ---------------------------------------------------------------------------

try:  # pragma: no cover
    from core import data_engine as _v1_engine  # type: ignore
    _HAS_V1_ENGINE = True
except Exception:  # pragma: no cover
    _v1_engine = None  # type: ignore
    _HAS_V1_ENGINE = False


# =======================================================================
# Helper utilities (safe configuration parsing)
# =======================================================================

def _safe_int(value: Any, default: int, label: str) -> int:
    if value is None:
        return default
    try:
        if isinstance(value, (int, float)):
            return int(value)
        return int(str(value).strip())
    except Exception:
        logger.warning("DataEngine config: invalid integer for %s=%r; using %s", label, value, default)
        return default


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _safe_bool(value: Any, default: bool, label: str) -> bool:
    if value is None:
        return default
    try:
        if isinstance(value, bool):
            return value
        s = str(value).strip().lower()
        if s in {"1", "true", "yes", "on", "y"}:
            return True
        if s in {"0", "false", "no", "off", "n"}:
            return False
    except Exception:
        pass
    logger.warning("DataEngine config: invalid boolean for %s=%r; using %s", label, value, default)
    return default


def _uniq_list_lower(items: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for x in items:
        s = (x or "").strip().lower()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


# =======================================================================
# ProviderSource & UnifiedQuote models
# =======================================================================

class ProviderSource(BaseModel):
    provider: str
    weight: Optional[float] = None
    quality: Optional[float] = None
    note: Optional[str] = None


class UnifiedQuote(BaseModel):
    # Identity
    symbol: str = Field(..., description="Canonical symbol, e.g. 1120.SR, AAPL")
    name: Optional[str] = None
    company_name: Optional[str] = None
    sector: Optional[str] = None
    sub_sector: Optional[str] = None
    industry: Optional[str] = None
    market: Optional[str] = None
    market_region: Optional[str] = None
    exchange: Optional[str] = None
    currency: Optional[str] = None
    listing_date: Optional[str] = None

    # Capital structure / ownership / short interest
    shares_outstanding: Optional[float] = None
    free_float: Optional[float] = None
    insider_ownership_percent: Optional[float] = None
    institutional_ownership_percent: Optional[float] = None
    short_ratio: Optional[float] = None
    short_percent_float: Optional[float] = None

    # Price / liquidity
    last_price: Optional[float] = None
    price: Optional[float] = None
    previous_close: Optional[float] = None
    prev_close: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    change: Optional[float] = None
    change_percent: Optional[float] = None
    change_pct: Optional[float] = None

    high_52w: Optional[float] = None
    low_52w: Optional[float] = None
    fifty_two_week_high: Optional[float] = None
    fifty_two_week_low: Optional[float] = None
    position_52w_percent: Optional[float] = None
    fifty_two_week_position: Optional[float] = None

    volume: Optional[float] = None
    avg_volume_30d: Optional[float] = None
    average_volume_30d: Optional[float] = None
    avg_volume: Optional[float] = None
    value_traded: Optional[float] = None
    turnover_rate: Optional[float] = None
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None
    bid_size: Optional[float] = None
    ask_size: Optional[float] = None
    spread_percent: Optional[float] = None
    liquidity_score: Optional[float] = None

    # Fundamentals
    eps_ttm: Optional[float] = None
    eps: Optional[float] = None
    pe_ratio: Optional[float] = None
    pe: Optional[float] = None
    pe_ttm: Optional[float] = None
    pb_ratio: Optional[float] = None
    pb: Optional[float] = None
    dividend_yield_percent: Optional[float] = None
    dividend_yield: Optional[float] = None
    dividend_payout_ratio: Optional[float] = None
    dividend_rate: Optional[float] = None
    ex_dividend_date: Optional[str] = None
    roe_percent: Optional[float] = None
    roe: Optional[float] = None
    roa_percent: Optional[float] = None
    roa: Optional[float] = None
    debt_to_equity: Optional[float] = None
    current_ratio: Optional[float] = None
    quick_ratio: Optional[float] = None
    market_cap: Optional[float] = None

    # Growth / profitability
    revenue_growth_percent: Optional[float] = None
    net_income_growth_percent: Optional[float] = None
    ebitda_margin_percent: Optional[float] = None
    operating_margin_percent: Optional[float] = None
    net_margin_percent: Optional[float] = None
    profit_margin: Optional[float] = None

    # Valuation / risk
    ev_to_ebitda: Optional[float] = None
    price_to_sales: Optional[float] = None
    price_to_cash_flow: Optional[float] = None
    peg_ratio: Optional[float] = None
    beta: Optional[float] = None
    volatility_30d_percent: Optional[float] = None
    volatility_30d: Optional[float] = None
    target_price: Optional[float] = None

    # AI valuation & scores
    fair_value: Optional[float] = None
    upside_percent: Optional[float] = None
    valuation_label: Optional[str] = None
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    overall_score: Optional[float] = None
    recommendation: Optional[str] = None
    risk_label: Optional[str] = None
    risk_bucket: Optional[str] = None
    exp_roi_3m: Optional[float] = None
    exp_roi_12m: Optional[float] = None

    # Technicals
    rsi_14: Optional[float] = None
    macd: Optional[float] = None
    ma_20d: Optional[float] = None
    ma_50d: Optional[float] = None

    # Meta & providers
    data_quality: str = Field("UNKNOWN", description="FULL / PARTIAL / MISSING / STALE / UNKNOWN")
    primary_provider: Optional[str] = None
    provider: Optional[str] = None
    data_source: Optional[str] = None
    sources: Optional[List[ProviderSource]] = None

    # Timestamps
    last_updated_utc: Optional[datetime] = None
    last_updated_riyadh: Optional[datetime] = None
    as_of_utc: Optional[datetime] = None
    as_of_local: Optional[datetime] = None
    timezone: Optional[str] = None

    # Raw / errors
    error: Optional[str] = None
    raw: Optional[Dict[str, Any]] = None


@dataclass
class _CacheEntry:
    expires_at: float
    quote: UnifiedQuote


# =======================================================================
# DataEngine
# =======================================================================

class DataEngine:
    def __init__(
        self,
        cache_ttl: Optional[int] = None,
        provider_timeout: Optional[int] = None,
        enabled_providers: Optional[List[str] | str] = None,
        enable_advanced_analysis: Optional[bool] = None,
    ) -> None:
        # Cache TTL
        if cache_ttl is not None:
            self.cache_ttl = _safe_int(cache_ttl, 120, "cache_ttl")
        else:
            ttl_env = os.getenv("ENGINE_CACHE_TTL_SECONDS") or os.getenv("DATAENGINE_CACHE_TTL")
            self.cache_ttl = _safe_int(ttl_env, 120, "ENGINE_CACHE_TTL_SECONDS/DATAENGINE_CACHE_TTL")

        # Provider timeout
        if provider_timeout is not None:
            self.provider_timeout = _safe_int(provider_timeout, 10, "provider_timeout")
        else:
            timeout_env = os.getenv("ENGINE_PROVIDER_TIMEOUT_SECONDS") or os.getenv("DATAENGINE_TIMEOUT")
            self.provider_timeout = _safe_int(timeout_env, 10, "ENGINE_PROVIDER_TIMEOUT_SECONDS/DATAENGINE_TIMEOUT")

        # Concurrency limiter (global)
        conc_env = os.getenv("ENGINE_MAX_CONCURRENCY")
        self.max_concurrency = _safe_int(conc_env, 25, "ENGINE_MAX_CONCURRENCY")
        self._sem = asyncio.Semaphore(max(1, self.max_concurrency))

        # Advanced analysis toggle
        if enable_advanced_analysis is not None:
            self.enable_advanced_analysis = _safe_bool(enable_advanced_analysis, True, "enable_advanced_analysis")
        else:
            flag = os.getenv("ENABLE_ADVANCED_ANALYSIS")
            self.enable_advanced_analysis = _safe_bool(flag, True, "ENABLE_ADVANCED_ANALYSIS")

        # FMP deep enrich toggle (can be heavy)
        self.fmp_deep_enrich = _safe_bool(os.getenv("FMP_DEEP_ENRICH"), False, "FMP_DEEP_ENRICH")

        # Local timezone
        self.local_tz_name: str = os.getenv("LOCAL_TIMEZONE", "Asia/Riyadh")

        # Enabled providers
        if enabled_providers is None:
            providers_raw = os.getenv("ENABLED_PROVIDERS", "fmp")
        elif isinstance(enabled_providers, str):
            providers_raw = enabled_providers
        else:
            providers_raw = ",".join(str(p) for p in enabled_providers)

        providers = _uniq_list_lower([p for p in (providers_raw or "").split(",")])
        if not providers:
            providers = ["fmp"]

        # Primary provider ordering
        primary_env = (os.getenv("PRIMARY_PROVIDER", "") or "").strip().lower() or None
        self.primary_provider_env = primary_env

        self.enabled_providers = providers[:]
        if self.primary_provider_env and self.primary_provider_env in self.enabled_providers:
            self.enabled_providers.sort(key=lambda p: 0 if p == self.primary_provider_env else 1)

        self._cache: Dict[str, _CacheEntry] = {}

        logger.info(
            "DataEngine v2.8 initialized "
            "(providers=%s, primary=%s, cache_ttl=%ss, timeout=%ss, max_concurrency=%s, "
            "v1_delegate=%s, advanced_analysis=%s, fmp_deep_enrich=%s)",
            self.enabled_providers,
            self.primary_provider_env or "auto",
            self.cache_ttl,
            self.provider_timeout,
            self.max_concurrency,
            _HAS_V1_ENGINE,
            self.enable_advanced_analysis,
            self.fmp_deep_enrich,
        )

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    async def get_enriched_quote(self, symbol: str) -> UnifiedQuote:
        symbol_norm = self._normalize_symbol(symbol)
        if not symbol_norm:
            return UnifiedQuote(symbol="", data_quality="MISSING", error="Empty or invalid symbol")

        now = time.time()
        cached = self._cache.get(symbol_norm)
        if cached and cached.expires_at > now:
            return cached.quote

        try:
            quote = await self._get_enriched_quote_uncached(symbol_norm)
        except Exception as exc:
            logger.exception("DataEngine.get_enriched_quote exception for %s", symbol_norm)
            quote = UnifiedQuote(
                symbol=symbol_norm,
                data_quality="MISSING",
                error=f"Exception in DataEngine.get_enriched_quote: {exc}",
            )

        # Final normalization + derived fields
        self._normalize_data_quality(quote)
        self._finalize_derived_fields(quote)

        self._cache[symbol_norm] = _CacheEntry(expires_at=now + self.cache_ttl, quote=quote)
        return quote

    async def get_enriched_quotes(self, symbols: List[str]) -> List[UnifiedQuote]:
        tasks = [self.get_enriched_quote(s) for s in (symbols or [])]
        return await asyncio.gather(*tasks)

    # ------------------------------------------------------------------ #
    # Symbol utils
    # ------------------------------------------------------------------ #

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
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

    @staticmethod
    def _is_ksa_symbol(symbol: str) -> bool:
        s = (symbol or "").upper()
        return s.endswith(".SR") or s.endswith(".TADAWUL")

    def _now_dt_pair(self) -> Tuple[datetime, Optional[datetime], Optional[str]]:
        now_utc = datetime.now(timezone.utc)
        as_of_local: Optional[datetime] = None
        tz_name: Optional[str] = "UTC"
        if ZoneInfo is not None:
            try:
                tz = ZoneInfo(self.local_tz_name)
                as_of_local = now_utc.astimezone(tz)
                tz_name = self.local_tz_name
            except Exception:
                as_of_local = None
        return now_utc, as_of_local, tz_name

    # ------------------------------------------------------------------ #
    # Core orchestration
    # ------------------------------------------------------------------ #

    async def _get_enriched_quote_uncached(self, symbol: str) -> UnifiedQuote:
        is_ksa = self._is_ksa_symbol(symbol)

        # 1) KSA delegate to v1 engine if available
        if is_ksa and _HAS_V1_ENGINE:
            try:
                v1_quotes = await _v1_engine.get_enriched_quotes([symbol])  # type: ignore[attr-defined]
                if v1_quotes:
                    q = self._from_v1_quote(v1_quotes[0])
                    self._normalize_data_quality(q)
                    self._finalize_derived_fields(q)
                    return q
            except Exception as exc:  # pragma: no cover
                logger.exception("V1 KSA delegate failed for %s: %s", symbol, exc)

        # 2) Multi-provider orchestration
        providers = list(self.enabled_providers)

        # KSA-safe: never call EODHD for .SR / .TADAWUL
        if is_ksa and "eodhd" in providers:
            providers = [p for p in providers if p != "eodhd"]

        tasks: List[asyncio.Task[UnifiedQuote]] = []

        for provider in providers:
            p = provider.lower()
            if p == "fmp":
                tasks.append(asyncio.create_task(self._safe_call_provider("fmp", self._fetch_from_fmp, symbol)))
            elif p == "eodhd":
                tasks.append(asyncio.create_task(self._safe_call_provider("eodhd", self._fetch_from_eodhd, symbol)))
            elif p == "finnhub":
                tasks.append(asyncio.create_task(self._safe_call_provider("finnhub", self._fetch_from_finnhub, symbol)))
            else:
                logger.warning("Unknown provider '%s' configured; ignoring", provider)

        if not tasks:
            if _HAS_V1_ENGINE and not is_ksa:
                try:
                    v1_quotes = await _v1_engine.get_enriched_quotes([symbol])  # type: ignore[attr-defined]
                    if v1_quotes:
                        q = self._from_v1_quote(v1_quotes[0])
                        self._normalize_data_quality(q)
                        self._finalize_derived_fields(q)
                        return q
                except Exception as exc:  # pragma: no cover
                    logger.exception("V1 global fallback failed for %s: %s", symbol, exc)

            return UnifiedQuote(symbol=symbol, data_quality="MISSING", error="No valid providers configured")

        results = await asyncio.gather(*tasks)
        merged = self._merge_provider_results(symbol, results)

        # 3) Global fallback via v1 if merged is missing
        if (not is_ksa) and _HAS_V1_ENGINE and merged.data_quality in ("MISSING", "UNKNOWN"):
            try:
                v1_quotes = await _v1_engine.get_enriched_quotes([symbol])  # type: ignore[attr-defined]
                if v1_quotes:
                    q = self._from_v1_quote(v1_quotes[0])
                    self._normalize_data_quality(q)
                    self._finalize_derived_fields(q)
                    return q
            except Exception as exc:  # pragma: no cover
                logger.exception("V1 global fallback failed for %s: %s", symbol, exc)

        self._normalize_data_quality(merged)
        self._finalize_derived_fields(merged)
        return merged

    async def _safe_call_provider(
        self,
        provider_name: str,
        func: Callable[[str], Any],
        symbol: str,
    ) -> UnifiedQuote:
        try:
            async with self._sem:
                q = await func(symbol)

            if q is None:
                return UnifiedQuote(
                    symbol=symbol,
                    data_quality="MISSING",
                    primary_provider=provider_name,
                    provider=provider_name,
                    data_source=provider_name,
                    error=f"{provider_name} returned no data",
                    sources=[ProviderSource(provider=provider_name, note="no data")],
                )

            q.primary_provider = q.primary_provider or provider_name
            q.provider = q.provider or provider_name
            q.data_source = q.data_source or provider_name
            if not q.sources:
                q.sources = [ProviderSource(provider=provider_name, note="primary")]

            self._normalize_data_quality(q)
            self._finalize_derived_fields(q)
            return q
        except Exception as exc:
            logger.exception("Provider '%s' exception for %s: %s", provider_name, symbol, exc)
            return UnifiedQuote(
                symbol=symbol,
                data_quality="MISSING",
                primary_provider=provider_name,
                provider=provider_name,
                data_source=provider_name,
                error=f"Provider {provider_name} exception: {exc}",
                sources=[ProviderSource(provider=provider_name, note="exception")],
            )

    def _merge_provider_results(self, symbol: str, results: List[UnifiedQuote]) -> UnifiedQuote:
        if not results:
            return UnifiedQuote(symbol=symbol, data_quality="MISSING", error="No provider results")

        for q in results:
            self._normalize_data_quality(q)

        usable = [q for q in results if q.data_quality not in ("MISSING", "UNKNOWN")]
        if not usable:
            combined_error = "; ".join([str(q.error) for q in results if q.error])
            return UnifiedQuote(symbol=symbol, data_quality="MISSING", error=combined_error or "All providers failed")

        priority_order = ["FULL", "EXCELLENT", "GOOD", "OK", "FAIR", "PARTIAL", "POOR"]

        def rank(q: UnifiedQuote) -> int:
            dq = (q.data_quality or "UNKNOWN").upper()
            try:
                return priority_order.index(dq)
            except ValueError:
                return len(priority_order)

        base = sorted(usable, key=rank)[0]

        field_map = getattr(UnifiedQuote, "model_fields", None) or getattr(UnifiedQuote, "__fields__", {})
        field_names = list(field_map.keys())

        merged_data = base.model_dump() if hasattr(base, "model_dump") else base.dict()  # type: ignore[attr-defined]

        for q in usable:
            if q is base:
                continue
            data = q.model_dump() if hasattr(q, "model_dump") else q.dict()  # type: ignore[attr-defined]
            for k in field_names:
                if k == "symbol":
                    continue
                if merged_data.get(k) is None and data.get(k) is not None:
                    merged_data[k] = data.get(k)

        merged_data["symbol"] = symbol.upper()
        merged_data["data_quality"] = base.data_quality or "UNKNOWN"

        combined_sources: List[ProviderSource] = []
        seen: set[str] = set()
        for q in usable:
            if q.sources:
                for s in q.sources:
                    prov = (s.provider or "").lower()
                    if prov and prov not in seen:
                        seen.add(prov)
                        combined_sources.append(s)
            else:
                prov = (q.primary_provider or q.provider or q.data_source or "").lower()
                if prov and prov not in seen:
                    seen.add(prov)
                    combined_sources.append(ProviderSource(provider=prov, note="merged"))

        if combined_sources:
            merged_data["sources"] = combined_sources
            merged_data["primary_provider"] = merged_data.get("primary_provider") or combined_sources[0].provider
            merged_data["provider"] = merged_data.get("provider") or combined_sources[0].provider
            merged_data["data_source"] = merged_data.get("data_source") or combined_sources[0].provider

        combined_error = "; ".join(sorted({str(q.error) for q in results if q.error}))
        if combined_error:
            merged_data["error"] = combined_error

        try:
            merged_quote = UnifiedQuote(**merged_data)
        except Exception as exc:
            logger.exception("Failed to merge provider results for %s: %s", symbol, exc)
            return base

        self._normalize_data_quality(merged_quote)
        self._finalize_derived_fields(merged_quote)
        return merged_quote

    # ------------------------------------------------------------------ #
    # Normalization + derived fields
    # ------------------------------------------------------------------ #

    def _normalize_alias_fields(self, q: UnifiedQuote) -> None:
        # Price aliases
        if q.last_price is None and q.price is not None:
            q.last_price = q.price
        if q.price is None and q.last_price is not None:
            q.price = q.last_price

        if q.previous_close is None and q.prev_close is not None:
            q.previous_close = q.prev_close
        if q.prev_close is None and q.previous_close is not None:
            q.prev_close = q.previous_close

        # 52W aliases
        if q.high_52w is None and q.fifty_two_week_high is not None:
            q.high_52w = q.fifty_two_week_high
        if q.fifty_two_week_high is None and q.high_52w is not None:
            q.fifty_two_week_high = q.high_52w

        if q.low_52w is None and q.fifty_two_week_low is not None:
            q.low_52w = q.fifty_two_week_low
        if q.fifty_two_week_low is None and q.low_52w is not None:
            q.fifty_two_week_low = q.low_52w

        if q.position_52w_percent is None and q.fifty_two_week_position is not None:
            q.position_52w_percent = q.fifty_two_week_position
        if q.fifty_two_week_position is None and q.position_52w_percent is not None:
            q.fifty_two_week_position = q.position_52w_percent

        # Yield aliases
        if q.dividend_yield_percent is None and q.dividend_yield is not None:
            q.dividend_yield_percent = q.dividend_yield
        if q.dividend_yield is None and q.dividend_yield_percent is not None:
            q.dividend_yield = q.dividend_yield_percent

        # ROE/ROA aliases
        if q.roe_percent is None and q.roe is not None:
            q.roe_percent = q.roe
        if q.roe is None and q.roe_percent is not None:
            q.roe = q.roe_percent

        if q.roa_percent is None and q.roa is not None:
            q.roa_percent = q.roa
        if q.roa is None and q.roa_percent is not None:
            q.roa = q.roa_percent

        # Net margin alias
        if q.net_margin_percent is None and q.profit_margin is not None:
            q.net_margin_percent = q.profit_margin
        if q.profit_margin is None and q.net_margin_percent is not None:
            q.profit_margin = q.net_margin_percent

        # Avg volume aliases
        if q.avg_volume_30d is None:
            if q.average_volume_30d is not None:
                q.avg_volume_30d = q.average_volume_30d
            elif q.avg_volume is not None:
                q.avg_volume_30d = q.avg_volume

        if q.average_volume_30d is None and q.avg_volume_30d is not None:
            q.average_volume_30d = q.avg_volume_30d
        if q.avg_volume is None and q.avg_volume_30d is not None:
            q.avg_volume = q.avg_volume_30d

        # Volatility aliases
        if q.volatility_30d_percent is None and q.volatility_30d is not None:
            q.volatility_30d_percent = q.volatility_30d
        if q.volatility_30d is None and q.volatility_30d_percent is not None:
            q.volatility_30d = q.volatility_30d_percent

        # Market / region
        if q.market_region is None:
            if self._is_ksa_symbol(q.symbol):
                q.market_region = "KSA"
                q.market = q.market or "KSA"
            elif q.market is not None:
                q.market_region = "GLOBAL"

        # Timestamps
        if q.as_of_utc is None and q.last_updated_utc is not None:
            q.as_of_utc = q.last_updated_utc
        if q.as_of_local is None and q.last_updated_riyadh is not None:
            q.as_of_local = q.last_updated_riyadh

        if q.as_of_utc is None:
            now_utc, as_local, tz_name = self._now_dt_pair()
            q.as_of_utc = now_utc
            q.as_of_local = q.as_of_local or as_local
            q.timezone = q.timezone or tz_name

        if q.timezone is None:
            q.timezone = "Asia/Riyadh" if q.market_region == "KSA" else (self.local_tz_name or "UTC")

    def _normalize_data_quality(self, q: UnifiedQuote) -> None:
        self._normalize_alias_fields(q)

        dq_raw = (q.data_quality or "UNKNOWN").upper().strip()

        if dq_raw in {"OK", "GOOD", "EXCELLENT"}:
            dq = "FULL"
        elif dq_raw in {"FAIR", "POOR"}:
            dq = "PARTIAL"
        elif dq_raw == "MISSING":
            dq = "MISSING"
        elif dq_raw == "STALE":
            dq = "STALE"
        elif dq_raw in {"UNKNOWN", "", "NONE"}:
            dq = "UNKNOWN"
        else:
            dq = dq_raw

        has_price = q.last_price is not None or q.price is not None
        has_basic = any(
            x is not None for x in [q.volume, q.market_cap, q.eps_ttm, q.pe_ttm, q.dividend_yield_percent]
        )
        has_deep = any(
            x is not None for x in [q.roe_percent, q.roa_percent, q.revenue_growth_percent, q.net_margin_percent]
        )

        if dq == "UNKNOWN":
            if not has_price and not has_basic and not has_deep:
                dq = "MISSING"
            elif has_price and (has_basic or has_deep):
                dq = "FULL"
            elif has_price:
                dq = "PARTIAL"
            else:
                dq = "PARTIAL" if (has_basic or has_deep) else "MISSING"
        else:
            if dq == "FULL" and not has_price:
                dq = "PARTIAL"
            if dq == "PARTIAL" and not has_price and not has_basic and not has_deep:
                dq = "MISSING"

        q.data_quality = dq

    def _finalize_derived_fields(self, q: UnifiedQuote) -> None:
        # change & change%
        if q.change is None and q.last_price is not None and q.previous_close is not None:
            try:
                q.change = float(q.last_price) - float(q.previous_close)
            except Exception:
                pass

        if q.change_percent is None and q.change is not None and q.previous_close:
            try:
                q.change_percent = float(q.change) / float(q.previous_close) * 100.0
            except Exception:
                pass

        if q.change_pct is None and q.change_percent is not None:
            q.change_pct = q.change_percent

        # 52W position
        if q.position_52w_percent is None and q.last_price is not None and q.low_52w is not None and q.high_52w is not None:
            try:
                span = float(q.high_52w) - float(q.low_52w)
                if span > 0:
                    q.position_52w_percent = max(0.0, min(100.0, (float(q.last_price) - float(q.low_52w)) / span * 100.0))
                    q.fifty_two_week_position = q.position_52w_percent
            except Exception:
                pass

        # value traded
        if q.value_traded is None and q.volume is not None and q.last_price is not None:
            try:
                q.value_traded = float(q.volume) * float(q.last_price)
            except Exception:
                pass

        # turnover_rate (volume / shares_outstanding)
        if q.turnover_rate is None and q.volume is not None and q.shares_outstanding not in (None, 0):
            try:
                q.turnover_rate = float(q.volume) / float(q.shares_outstanding) * 100.0
            except Exception:
                pass

        # spread_percent
        if q.spread_percent is None and q.bid_price is not None and q.ask_price is not None:
            try:
                mid = (float(q.bid_price) + float(q.ask_price)) / 2.0
                if mid > 0:
                    q.spread_percent = (float(q.ask_price) - float(q.bid_price)) / mid * 100.0
            except Exception:
                pass

        # liquidity_score
        if q.liquidity_score is None:
            liq = self._compute_liquidity_score(q.volume, q.avg_volume_30d, q.market_cap)
            if liq is not None:
                q.liquidity_score = liq

        # scoring
        if self.enable_advanced_analysis:
            if q.opportunity_score is None or q.value_score is None or q.quality_score is None or q.momentum_score is None:
                self._apply_basic_scoring(q, source="finalize")

    def _compute_liquidity_score(self, volume: Optional[float], avg_volume: Optional[float], market_cap: Optional[float]) -> Optional[float]:
        vol = _safe_float(volume)
        avg = _safe_float(avg_volume)
        mc = _safe_float(market_cap)

        if vol is None or vol <= 0:
            return None

        score = 50.0

        if avg is not None and avg > 0:
            ratio = vol / avg
            if ratio >= 3.0:
                score += 20.0
            elif ratio >= 1.0:
                score += 10.0
            elif ratio < 0.5:
                score -= 10.0

        if mc is not None:
            if mc >= 1e11:
                score += 10.0
            elif mc >= 1e10:
                score += 5.0
            elif mc <= 1e9:
                score -= 10.0

        score = max(0.0, min(100.0, score))
        return round(score, 2)

    # ------------------------------------------------------------------ #
    # Providers
    # ------------------------------------------------------------------ #

    async def _fetch_from_fmp(self, symbol: str) -> Optional[UnifiedQuote]:
        api_key = os.getenv("FMP_API_KEY")
        if not api_key:
            return None

        timeout = httpx.Timeout(self.provider_timeout, connect=self.provider_timeout)
        async with httpx.AsyncClient(timeout=timeout) as client:
            quote_url = f"https://financialmodelingprep.com/api/v3/quote/{symbol}"
            resp = await client.get(quote_url, params={"apikey": api_key})
            if resp.status_code != 200:
                return None
            data = resp.json()
            if not isinstance(data, list) or not data:
                return None
            raw_quote = data[0]

            profile = None
            metrics_ttm = None
            ratios_ttm = None

            # Deep enrich is optional (can be heavy)
            if self.fmp_deep_enrich:
                try:
                    prof_url = f"https://financialmodelingprep.com/api/v3/profile/{symbol}"
                    prof = await client.get(prof_url, params={"apikey": api_key})
                    if prof.status_code == 200:
                        pj = prof.json()
                        if isinstance(pj, list) and pj:
                            profile = pj[0]
                except Exception:
                    profile = None

                # These endpoints exist for many symbols, but are treated as best-effort
                try:
                    km_url = f"https://financialmodelingprep.com/api/v3/key-metrics-ttm/{symbol}"
                    km = await client.get(km_url, params={"apikey": api_key})
                    if km.status_code == 200:
                        kj = km.json()
                        if isinstance(kj, list) and kj:
                            metrics_ttm = kj[0]
                except Exception:
                    metrics_ttm = None

                try:
                    rt_url = f"https://financialmodelingprep.com/api/v3/ratios-ttm/{symbol}"
                    rr = await client.get(rt_url, params={"apikey": api_key})
                    if rr.status_code == 200:
                        rj = rr.json()
                        if isinstance(rj, list) and rj:
                            ratios_ttm = rj[0]
                except Exception:
                    ratios_ttm = None

        q = self._map_fmp_to_unified(symbol, raw_quote, profile=profile, metrics_ttm=metrics_ttm, ratios_ttm=ratios_ttm)
        q.primary_provider = "fmp"
        q.provider = "fmp"
        q.data_source = "fmp"
        q.sources = q.sources or [ProviderSource(provider="fmp", note="FMP quote")]
        self._normalize_data_quality(q)
        self._finalize_derived_fields(q)
        return q

    def _map_fmp_to_unified(
        self,
        symbol: str,
        d: Dict[str, Any],
        profile: Optional[Dict[str, Any]] = None,
        metrics_ttm: Optional[Dict[str, Any]] = None,
        ratios_ttm: Optional[Dict[str, Any]] = None,
    ) -> UnifiedQuote:
        def gv(obj: Optional[Dict[str, Any]], *keys: str, default=None):
            if not isinstance(obj, dict):
                return default
            for k in keys:
                if k in obj and obj[k] is not None:
                    return obj[k]
            return default

        last_price = gv(d, "price")
        previous_close = gv(d, "previousClose")
        change = gv(d, "change")
        change_pct = gv(d, "changesPercentage")

        high_52w = gv(d, "yearHigh")
        low_52w = gv(d, "yearLow")

        as_of_utc, as_of_local, tz_name = self._now_dt_pair()

        eps_val = gv(d, "eps", "epsTTM")
        pe_val = gv(d, "pe", "peTTM")
        pb_val = gv(d, "priceToBook", "pb", "pbRatio")
        beta_val = gv(d, "beta")

        # Profile enrich
        name = gv(d, "name") or gv(profile, "companyName", "company_name", "name")
        sector = gv(d, "sector") or gv(profile, "sector")
        industry = gv(d, "industry") or gv(profile, "industry")
        exchange = gv(d, "exchangeShortName") or gv(profile, "exchangeShortName")
        currency = gv(d, "currency") or gv(profile, "currency")

        # Market region heuristic
        market = "GLOBAL"
        market_region = "GLOBAL"
        if self._is_ksa_symbol(symbol) or (exchange and str(exchange).upper() in {"TADAWUL", "NOMU", "KSA"}):
            market = "KSA"
            market_region = "KSA"

        volume_val = gv(d, "volume")
        avg_vol = gv(d, "avgVolume")
        market_cap = gv(d, "marketCap")

        # Dividend yield
        dividend_yield_raw = gv(d, "dividendYield") or gv(profile, "lastDiv")
        dividend_yield_percent = None
        if dividend_yield_raw is not None:
            try:
                dy = float(dividend_yield_raw)
                dividend_yield_percent = dy * 100.0 if dy < 1.0 else dy
            except Exception:
                dividend_yield_percent = None

        # Metrics / ratios enrich (best effort)
        roe = _safe_float(gv(metrics_ttm, "roeTTM", "returnOnEquityTTM"))
        roa = _safe_float(gv(metrics_ttm, "roaTTM", "returnOnAssetsTTM"))
        net_margin = _safe_float(gv(metrics_ttm, "netProfitMarginTTM"))
        operating_margin = _safe_float(gv(metrics_ttm, "operatingProfitMarginTTM"))
        debt_to_equity = _safe_float(gv(metrics_ttm, "debtToEquityTTM"))
        current_ratio = _safe_float(gv(metrics_ttm, "currentRatioTTM"))
        quick_ratio = _safe_float(gv(metrics_ttm, "quickRatioTTM"))

        price_to_sales = _safe_float(gv(ratios_ttm, "priceToSalesRatioTTM"))
        peg_ratio = _safe_float(gv(ratios_ttm, "pegRatioTTM"))

        q = UnifiedQuote(
            symbol=str(gv(d, "symbol", default=symbol)).upper(),
            name=name,
            company_name=name,
            sector=sector,
            industry=industry,
            market=market,
            market_region=market_region,
            exchange=exchange,
            currency=currency,
            last_price=_safe_float(last_price),
            price=_safe_float(last_price),
            previous_close=_safe_float(previous_close),
            prev_close=_safe_float(previous_close),
            open=_safe_float(gv(d, "open")),
            high=_safe_float(gv(d, "dayHigh")),
            low=_safe_float(gv(d, "dayLow")),
            change=_safe_float(change),
            change_percent=_safe_float(change_pct),
            change_pct=_safe_float(change_pct),
            high_52w=_safe_float(high_52w),
            low_52w=_safe_float(low_52w),
            fifty_two_week_high=_safe_float(high_52w),
            fifty_two_week_low=_safe_float(low_52w),
            volume=_safe_float(volume_val),
            avg_volume_30d=_safe_float(avg_vol),
            average_volume_30d=_safe_float(avg_vol),
            avg_volume=_safe_float(avg_vol),
            market_cap=_safe_float(market_cap),
            eps_ttm=_safe_float(eps_val),
            eps=_safe_float(eps_val),
            pe_ratio=_safe_float(pe_val),
            pe=_safe_float(pe_val),
            pe_ttm=_safe_float(pe_val),
            pb_ratio=_safe_float(pb_val),
            pb=_safe_float(pb_val),
            dividend_yield_percent=_safe_float(dividend_yield_percent),
            dividend_yield=_safe_float(dividend_yield_percent),
            beta=_safe_float(beta_val),
            roe=_safe_float(roe),
            roe_percent=_safe_float(roe),
            roa=_safe_float(roa),
            roa_percent=_safe_float(roa),
            net_margin_percent=_safe_float(net_margin),
            profit_margin=_safe_float(net_margin),
            operating_margin_percent=_safe_float(operating_margin),
            debt_to_equity=_safe_float(debt_to_equity),
            current_ratio=_safe_float(current_ratio),
            quick_ratio=_safe_float(quick_ratio),
            price_to_sales=_safe_float(price_to_sales),
            peg_ratio=_safe_float(peg_ratio),
            last_updated_utc=as_of_utc,
            last_updated_riyadh=as_of_local,
            as_of_utc=as_of_utc,
            as_of_local=as_of_local,
            timezone=tz_name,
            raw={"quote": d, "profile": profile, "metrics_ttm": metrics_ttm, "ratios_ttm": ratios_ttm},
        )

        self._normalize_data_quality(q)
        if self.enable_advanced_analysis:
            self._apply_basic_scoring(q, source="fmp")
        return q

    async def _fetch_from_eodhd(self, symbol: str) -> Optional[UnifiedQuote]:
        # GLOBAL ONLY (never for KSA)
        if self._is_ksa_symbol(symbol):
            return None

        api_key = os.getenv("EODHD_API_KEY")
        base_url = os.getenv("EODHD_BASE_URL", "https://eodhd.com/api")
        if not api_key:
            return None

        fund_symbol = symbol if "." in symbol else f"{symbol}.US"
        rt_url = f"{base_url.rstrip('/')}/real-time/{symbol}"
        fund_url = f"{base_url.rstrip('/')}/fundamentals/{fund_symbol}"

        timeout = httpx.Timeout(self.provider_timeout, connect=self.provider_timeout)
        async with httpx.AsyncClient(timeout=timeout) as client:
            rt = await client.get(rt_url, params={"api_token": api_key, "fmt": "json"})
            if rt.status_code != 200:
                return None
            rt_data = rt.json()
            if not isinstance(rt_data, dict) or not rt_data:
                return None

            fundamentals_data = None
            try:
                fd = await client.get(fund_url, params={"api_token": api_key, "fmt": "json"})
                if fd.status_code == 200:
                    tmp = fd.json()
                    if isinstance(tmp, dict) and tmp:
                        fundamentals_data = tmp
            except Exception:
                fundamentals_data = None

        q = self._map_eodhd_to_unified(symbol, rt_data, fundamentals=fundamentals_data)
        q.primary_provider = "eodhd"
        q.provider = "eodhd"
        q.data_source = "eodhd"
        q.sources = q.sources or [ProviderSource(provider="eodhd", note="EODHD realtime+fundamentals")]
        self._normalize_data_quality(q)
        if self.enable_advanced_analysis:
            self._apply_basic_scoring(q, source="eodhd")
        self._finalize_derived_fields(q)
        return q

    def _map_eodhd_to_unified(self, symbol: str, d: Dict[str, Any], fundamentals: Optional[Dict[str, Any]] = None) -> UnifiedQuote:
        def gv(obj: Dict[str, Any], *keys: str, default=None):
            for k in keys:
                if k in obj and obj[k] is not None:
                    return obj[k]
            return default

        general = (fundamentals or {}).get("General") or {}
        highlights = (fundamentals or {}).get("Highlights") or {}
        valuation = (fundamentals or {}).get("Valuation") or {}
        shares = (fundamentals or {}).get("SharesStats") or {}
        technicals = (fundamentals or {}).get("Technicals") or {}
        splits = (fundamentals or {}).get("SplitsDividends") or {}

        def gf(section: Dict[str, Any], *keys: str, default=None):
            for k in keys:
                if k in section and section[k] is not None:
                    return section[k]
            return default

        last_price = gv(d, "close", "price", "last")
        previous_close = gv(d, "previousClose", "previous_close")
        change = gv(d, "change")
        change_pct = gv(d, "change_p", "change_percent")

        high_52w = gf(technicals, "52WeekHigh") or gv(d, "year_high", "high_52w", "fifty_two_week_high")
        low_52w = gf(technicals, "52WeekLow") or gv(d, "year_low", "low_52w", "fifty_two_week_low")

        as_of_utc, as_of_local, tz_name = self._now_dt_pair()

        eps_val = gf(highlights, "EarningsShare", "DilutedEpsTTM") or gv(d, "eps")
        pe_val = gf(highlights, "PERatio") or gf(valuation, "TrailingPE") or gv(d, "pe")
        pb_val = gf(valuation, "PriceBookMRQ") or gv(d, "pb", "priceToBook")

        dividend_yield_raw = gf(highlights, "DividendYield") or gf(splits, "ForwardAnnualDividendYield") or gv(d, "dividend_yield")
        dividend_yield_percent = None
        if dividend_yield_raw is not None:
            try:
                dy = float(dividend_yield_raw)
                dividend_yield_percent = dy * 100.0 if dy < 1.0 else dy
            except Exception:
                dividend_yield_percent = None

        market_cap = gf(highlights, "MarketCapitalization") or gv(d, "market_cap")
        volume_val = gv(d, "volume")
        avg_vol = gf(technicals, "20DayAverageVolume") or gv(d, "avgVolume", "average_volume")

        # Identity
        name = gf(general, "Name")
        sector = gf(general, "Sector")
        industry = gf(general, "Industry", "GicIndustry", "GicSubIndustry")
        listing_date = gf(general, "IPODate")
        exchange = gf(general, "Exchange") or gv(d, "exchange")
        currency = gf(general, "CurrencyCode") or gv(d, "currency")

        # Ownership & short
        insider_own = gf(shares, "PercentInsiders", "PercentHeldByInsiders")
        inst_own = gf(shares, "PercentInstitutions", "PercentHeldByInstitutions")
        short_ratio = gf(shares, "ShortRatio")
        short_pct_float = gf(shares, "ShortPercentFloat", "ShortPercent")

        # Margins & returns
        profit_margin = gf(highlights, "ProfitMargin")
        operating_margin = gf(highlights, "OperatingMarginTTM")
        roa = gf(highlights, "ReturnOnAssetsTTM")
        roe = gf(highlights, "ReturnOnEquityTTM")
        rev_growth = gf(highlights, "QuarterlyRevenueGrowthYOY")
        earn_growth = gf(highlights, "QuarterlyEarningsGrowthYOY")

        price_sales = gf(valuation, "PriceSalesTTM")
        ev_ebitda = gf(valuation, "EnterpriseValueEbitda")
        peg = gf(highlights, "PEGRatio")
        beta = gf(technicals, "Beta") or gv(d, "beta")
        target_price = gf(highlights, "WallStreetTargetPrice")

        dividend_rate = gf(splits, "ForwardAnnualDividendRate") or gf(highlights, "DividendShare")
        payout_ratio = gf(splits, "PayoutRatio")
        ex_div = gf(splits, "ExDividendDate")

        shares_out = gf(shares, "SharesOutstanding") or gv(d, "shares_outstanding")
        free_float = gf(shares, "SharesFloat")

        q = UnifiedQuote(
            symbol=str(gv(d, "code", "symbol", default=symbol)).upper(),
            name=name,
            company_name=name,
            sector=sector,
            industry=industry,
            market="GLOBAL",
            market_region="GLOBAL",
            exchange=exchange,
            currency=currency,
            listing_date=listing_date,
            shares_outstanding=_safe_float(shares_out),
            free_float=_safe_float(free_float),
            insider_ownership_percent=_safe_float(insider_own),
            institutional_ownership_percent=_safe_float(inst_own),
            short_ratio=_safe_float(short_ratio),
            short_percent_float=_safe_float(short_pct_float),
            last_price=_safe_float(last_price),
            price=_safe_float(last_price),
            previous_close=_safe_float(previous_close),
            prev_close=_safe_float(previous_close),
            open=_safe_float(gv(d, "open")),
            high=_safe_float(gv(d, "high")),
            low=_safe_float(gv(d, "low")),
            change=_safe_float(change),
            change_percent=_safe_float(change_pct),
            change_pct=_safe_float(change_pct),
            high_52w=_safe_float(high_52w),
            low_52w=_safe_float(low_52w),
            fifty_two_week_high=_safe_float(high_52w),
            fifty_two_week_low=_safe_float(low_52w),
            volume=_safe_float(volume_val),
            avg_volume_30d=_safe_float(avg_vol),
            average_volume_30d=_safe_float(avg_vol),
            avg_volume=_safe_float(avg_vol),
            market_cap=_safe_float(market_cap),
            eps_ttm=_safe_float(eps_val),
            eps=_safe_float(eps_val),
            pe_ratio=_safe_float(pe_val),
            pe=_safe_float(pe_val),
            pe_ttm=_safe_float(pe_val),
            pb_ratio=_safe_float(pb_val),
            pb=_safe_float(pb_val),
            dividend_yield=_safe_float(dividend_yield_percent),
            dividend_yield_percent=_safe_float(dividend_yield_percent),
            dividend_rate=_safe_float(dividend_rate),
            dividend_payout_ratio=_safe_float(payout_ratio),
            ex_dividend_date=ex_div,
            profit_margin=_safe_float(profit_margin),
            net_margin_percent=_safe_float(profit_margin),
            operating_margin_percent=_safe_float(operating_margin),
            roe=_safe_float(roe),
            roe_percent=_safe_float(roe),
            roa=_safe_float(roa),
            roa_percent=_safe_float(roa),
            revenue_growth_percent=_safe_float(rev_growth),
            net_income_growth_percent=_safe_float(earn_growth),
            ev_to_ebitda=_safe_float(ev_ebitda),
            price_to_sales=_safe_float(price_sales),
            peg_ratio=_safe_float(peg),
            beta=_safe_float(beta),
            target_price=_safe_float(target_price),
            last_updated_utc=as_of_utc,
            last_updated_riyadh=as_of_local,
            as_of_utc=as_of_utc,
            as_of_local=as_of_local,
            timezone=tz_name,
            raw={"realtime": d, "fundamentals": fundamentals} if fundamentals else {"realtime": d},
        )

        self._normalize_data_quality(q)
        return q

    async def _fetch_from_finnhub(self, symbol: str) -> Optional[UnifiedQuote]:
        api_key = os.getenv("FINNHUB_API_KEY")
        if not api_key:
            return None

        url = "https://finnhub.io/api/v1/quote"
        params = {"symbol": symbol, "token": api_key}

        timeout = httpx.Timeout(self.provider_timeout, connect=self.provider_timeout)
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url, params=params)
            if resp.status_code != 200:
                return None
            d = resp.json()

        if not isinstance(d, dict) or not d:
            return None

        last_price = d.get("c")
        prev_close = d.get("pc")

        change = None
        change_pct = None
        if last_price is not None and prev_close:
            try:
                change = float(last_price) - float(prev_close)
                change_pct = float(change) / float(prev_close) * 100.0
            except Exception:
                pass

        as_of_utc, as_of_local, tz_name = self._now_dt_pair()

        q = UnifiedQuote(
            symbol=symbol.upper(),
            last_price=_safe_float(last_price),
            price=_safe_float(last_price),
            previous_close=_safe_float(prev_close),
            prev_close=_safe_float(prev_close),
            open=_safe_float(d.get("o")),
            high=_safe_float(d.get("h")),
            low=_safe_float(d.get("l")),
            change=_safe_float(change),
            change_percent=_safe_float(change_pct),
            change_pct=_safe_float(change_pct),
            last_updated_utc=as_of_utc,
            last_updated_riyadh=as_of_local,
            as_of_utc=as_of_utc,
            as_of_local=as_of_local,
            timezone=tz_name,
            raw=d,
        )

        self._normalize_data_quality(q)
        return q

    # ------------------------------------------------------------------ #
    # v1 delegate mapping
    # ------------------------------------------------------------------ #

    def _from_v1_quote(self, v1_q: Any) -> UnifiedQuote:
        def gv(*names: str, default=None):
            for n in names:
                if hasattr(v1_q, n):
                    val = getattr(v1_q, n)
                    if val is not None:
                        return val
            return default

        symbol = (gv("symbol", "ticker", default="") or "").upper()
        name = gv("company_name", "name")
        sector = gv("sector")
        sub_sector = gv("sub_sector", "industry_group")
        industry = gv("industry")
        market = gv("market", "market_region", "exchange")
        market_region = gv("market_region", "market")
        exchange = gv("exchange")
        currency = gv("currency")
        listing_date = gv("listing_date", "ipo_date", "list_date")

        last_price = gv("price", "last_price")
        previous_close = gv("prev_close", "previous_close")

        q = UnifiedQuote(
            symbol=symbol,
            name=name,
            company_name=name,
            sector=sector,
            sub_sector=sub_sector,
            industry=industry,
            market=market,
            market_region=market_region,
            exchange=exchange,
            currency=currency,
            listing_date=listing_date,
            shares_outstanding=_safe_float(gv("shares_outstanding")),
            free_float=_safe_float(gv("free_float")),
            last_price=_safe_float(last_price),
            price=_safe_float(last_price),
            previous_close=_safe_float(previous_close),
            prev_close=_safe_float(previous_close),
            open=_safe_float(gv("open")),
            high=_safe_float(gv("high")),
            low=_safe_float(gv("low")),
            change=_safe_float(gv("change")),
            change_percent=_safe_float(gv("change_pct", "change_percent")),
            change_pct=_safe_float(gv("change_pct", "change_percent")),
            high_52w=_safe_float(gv("fifty_two_week_high", "high_52w")),
            low_52w=_safe_float(gv("fifty_two_week_low", "low_52w")),
            fifty_two_week_high=_safe_float(gv("fifty_two_week_high", "high_52w")),
            fifty_two_week_low=_safe_float(gv("fifty_two_week_low", "low_52w")),
            position_52w_percent=_safe_float(gv("fifty_two_week_position", "position_52w_percent")),
            fifty_two_week_position=_safe_float(gv("fifty_two_week_position", "position_52w_percent")),
            volume=_safe_float(gv("volume")),
            avg_volume_30d=_safe_float(gv("avg_volume", "avg_volume_30d")),
            average_volume_30d=_safe_float(gv("avg_volume", "avg_volume_30d")),
            avg_volume=_safe_float(gv("avg_volume", "avg_volume_30d")),
            market_cap=_safe_float(gv("market_cap")),
            eps_ttm=_safe_float(gv("eps_ttm", "eps")),
            eps=_safe_float(gv("eps_ttm", "eps")),
            pe_ttm=_safe_float(gv("pe_ttm", "pe", "pe_ratio")),
            pe=_safe_float(gv("pe_ttm", "pe", "pe_ratio")),
            pe_ratio=_safe_float(gv("pe_ttm", "pe", "pe_ratio")),
            pb=_safe_float(gv("pb", "pb_ratio")),
            pb_ratio=_safe_float(gv("pb", "pb_ratio")),
            dividend_yield=_safe_float(gv("dividend_yield", "dividend_yield_percent")),
            dividend_yield_percent=_safe_float(gv("dividend_yield", "dividend_yield_percent")),
            dividend_payout_ratio=_safe_float(gv("dividend_payout_ratio")),
            roe=_safe_float(gv("roe", "roe_percent")),
            roe_percent=_safe_float(gv("roe", "roe_percent")),
            roa=_safe_float(gv("roa", "roa_percent")),
            roa_percent=_safe_float(gv("roa", "roa_percent")),
            profit_margin=_safe_float(gv("profit_margin", "net_margin_percent")),
            net_margin_percent=_safe_float(gv("net_margin_percent", "profit_margin")),
            debt_to_equity=_safe_float(gv("debt_to_equity")),
            current_ratio=_safe_float(gv("current_ratio")),
            quick_ratio=_safe_float(gv("quick_ratio")),
            revenue_growth_percent=_safe_float(gv("revenue_growth_percent", "revenue_growth")),
            net_income_growth_percent=_safe_float(gv("net_income_growth_percent", "net_income_growth")),
            ebitda_margin_percent=_safe_float(gv("ebitda_margin_percent", "ebitda_margin")),
            operating_margin_percent=_safe_float(gv("operating_margin_percent", "operating_margin")),
            ev_to_ebitda=_safe_float(gv("ev_to_ebitda", "ev_ebitda")),
            price_to_sales=_safe_float(gv("price_to_sales", "ps_ratio")),
            price_to_cash_flow=_safe_float(gv("price_to_cash_flow", "pcf_ratio")),
            peg_ratio=_safe_float(gv("peg_ratio", "peg")),
            beta=_safe_float(gv("beta")),
            volatility_30d_percent=_safe_float(gv("volatility_30d_percent", "volatility_30d")),
            volatility_30d=_safe_float(gv("volatility_30d_percent", "volatility_30d")),
            fair_value=_safe_float(gv("fair_value")),
            upside_percent=_safe_float(gv("upside_percent")),
            valuation_label=gv("valuation_label"),
            value_score=_safe_float(gv("value_score")),
            quality_score=_safe_float(gv("quality_score")),
            momentum_score=_safe_float(gv("momentum_score")),
            opportunity_score=_safe_float(gv("opportunity_score")),
            overall_score=_safe_float(gv("overall_score")),
            recommendat
