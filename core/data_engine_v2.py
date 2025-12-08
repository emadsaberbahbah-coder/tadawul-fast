"""
core/data_engine_v2.py
===============================================
Core Data & Analysis Engine - v2.3 (Sheets-aligned, v1-backed)

Author: Emad Bahbah (with GPT-5.1 Thinking)

Key features
------------
- Uses the improved legacy engine `core.data_engine` (v1) as the
  PRIMARY source for **all** markets (GLOBAL + KSA).
    • v1 already handles multi-provider routing:
        - GLOBAL: EODHD + FMP + Yahoo (where available)
        - KSA: FMP + Yahoo + Tadawul/Argaam (via its own logic)
    • This avoids duplicated logic and ensures all routes use the same
      canonical data pipeline.

- This v2 engine adds:
    • Simple in-memory caching with TTL to reduce API calls.
    • A sheet-aligned `UnifiedQuote` model that maps v1 fields into:
        - Identity (Symbol, Sector, Market, etc.)
        - Price & Liquidity (Last Price, 52W position, Volume, etc.)
        - Fundamentals (PE, PB, EPS, Dividend Yield, ROE, ROA, etc.)
        - Valuation / Risk / Technicals
    • Basic AI-style scoring:
        - Value / Quality / Momentum / Opportunity + Recommendation

- Extremely defensive:
    • Never raises on normal usage (returns MISSING with error instead).

Configuration (env vars)
------------------------
ENGINE_CACHE_TTL_SECONDS         # optional; seconds, overrides DATAENGINE_CACHE_TTL
DATAENGINE_CACHE_TTL             # optional; seconds, default 120 if both missing
ENGINE_PROVIDER_TIMEOUT_SECONDS  # optional; overrides DATAENGINE_TIMEOUT
DATAENGINE_TIMEOUT               # optional; per-provider timeout in seconds, default 10
ENABLED_PROVIDERS                # e.g. "fmp,eodhd,finnhub" (fallback list). Default: "fmp"
PRIMARY_PROVIDER                 # optional; if set and in enabled list, used first (fallback only)
LOCAL_TIMEZONE                   # optional; default "Asia/Riyadh"
ENABLE_ADVANCED_ANALYSIS         # optional; "1/true/on" to enable, "0/false/off" to disable

Provider API keys (fallback path)
---------------------------------
FMP_API_KEY                      # used only if v1 delegate fails
EODHD_BASE_URL                   # optional; default "https://eodhd.com/api"
EODHD_API_KEY                    # used only if v1 delegate fails
FINNHUB_API_KEY                  # used only if v1 delegate fails
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
    # Python 3.11+
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional import of v1 engine (PRIMARY data source)
# ---------------------------------------------------------------------------

try:  # pragma: no cover - best-effort
    from core import data_engine as _v1_engine  # type: ignore

    _HAS_V1_ENGINE = True
except Exception:  # pragma: no cover - if core.data_engine is missing
    _v1_engine = None  # type: ignore
    _HAS_V1_ENGINE = False


# ===========================================================================
# ProviderSource & UnifiedQuote models
# ===========================================================================


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
    market: Optional[str] = None          # e.g. "US", "KSA"
    market_region: Optional[str] = None   # alias for legacy engine / sheets
    exchange: Optional[str] = None
    currency: Optional[str] = None
    listing_date: Optional[str] = None  # YYYY-MM-DD if available

    # Capital structure
    shares_outstanding: Optional[float] = None
    free_float: Optional[float] = None

    # Price / liquidity
    last_price: Optional[float] = None
    price: Optional[float] = None
    previous_close: Optional[float] = None
    prev_close: Optional[float] = None  # alias for legacy_service
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
    profit_margin: Optional[float] = None  # alias used in legacy_service

    # Valuation / risk
    ev_to_ebitda: Optional[float] = None
    price_to_sales: Optional[float] = None
    price_to_cash_flow: Optional[float] = None
    peg_ratio: Optional[float] = None
    beta: Optional[float] = None
    volatility_30d_percent: Optional[float] = None
    volatility_30d: Optional[float] = None

    # AI valuation & scores
    fair_value: Optional[float] = None
    upside_percent: Optional[float] = None
    valuation_label: Optional[str] = None
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    recommendation: Optional[str] = None

    # Technicals
    rsi_14: Optional[float] = None
    macd: Optional[float] = None
    ma_20d: Optional[float] = None
    ma_50d: Optional[float] = None

    # Meta & providers
    data_quality: str = Field(
        "UNKNOWN", description="OK / PARTIAL / MISSING / STALE / UNKNOWN"
    )
    primary_provider: Optional[str] = None
    provider: Optional[str] = None
    data_source: Optional[str] = None  # alias for "Data Source" column on Sheets
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


# ===========================================================================
# DataEngine
# ===========================================================================


class DataEngine:
    """
    Core async engine used by routes.enriched_quote and AI/analysis routes.

    PRIMARY behavior:
        - Delegates to `core.data_engine` (v1) for ALL symbols.
        - v1 already handles:
            • GLOBAL: EODHD + FMP + Yahoo merging (+ fundamentals)
            • KSA: KSA-safe routing via Tadawul / Argaam logic

    Added by v2:
        - Caching
        - Sheet-aligned UnifiedQuote mapping
        - Basic AI scoring (Value / Quality / Momentum / Opportunity)
    """

    def __init__(
        self,
        cache_ttl: Optional[int] = None,
        provider_timeout: Optional[int] = None,
        enabled_providers: Optional[List[str]] = None,
        enable_advanced_analysis: Optional[bool] = None,
    ) -> None:
        # Cache TTL (seconds)
        if cache_ttl is not None:
            self.cache_ttl = int(cache_ttl)
        else:
            ttl_env = (
                os.getenv("ENGINE_CACHE_TTL_SECONDS")
                or os.getenv("DATAENGINE_CACHE_TTL")
                or "120"
            )
            self.cache_ttl = int(ttl_env)

        # Per-provider timeout (seconds) – used only in fallback path
        if provider_timeout is not None:
            self.provider_timeout = int(provider_timeout)
        else:
            timeout_env = (
                os.getenv("ENGINE_PROVIDER_TIMEOUT_SECONDS")
                or os.getenv("DATAENGINE_TIMEOUT")
                or "10"
            )
            self.provider_timeout = int(timeout_env)

        # Advanced analysis toggle (env + explicit override)
        if enable_advanced_analysis is not None:
            self.enable_advanced_analysis = bool(enable_advanced_analysis)
        else:
            flag = os.getenv("ENABLE_ADVANCED_ANALYSIS")
            if flag is None:
                self.enable_advanced_analysis = True
            else:
                self.enable_advanced_analysis = flag.strip().lower() in {
                    "1",
                    "true",
                    "yes",
                    "on",
                    "y",
                }

        self.local_tz_name: str = os.getenv("LOCAL_TIMEZONE", "Asia/Riyadh")

        # Enabled providers – FMP/EODHD/Finnhub are used ONLY as fallback
        if enabled_providers is not None:
            if isinstance(enabled_providers, str):
                raw = enabled_providers
            else:
                raw = ",".join(str(p) for p in enabled_providers)
        else:
            raw = os.getenv("ENABLED_PROVIDERS", "fmp")

        providers = [p.strip().lower() for p in raw.split(",") if p.strip()]

        # Primary provider hint (for ordering, fallback only)
        self.primary_provider_env: Optional[str] = (
            os.getenv("PRIMARY_PROVIDER", "") or ""
        ).strip().lower() or None

        self.enabled_providers: List[str] = []
        for p in providers:
            if p and p not in self.enabled_providers:
                self.enabled_providers.append(p)

        if not self.enabled_providers:
            # Always have at least FMP in the list (fallback)
            self.enabled_providers = ["fmp"]

        # If PRIMARY_PROVIDER is set and present, move it to the front
        if (
            self.primary_provider_env
            and self.primary_provider_env in self.enabled_providers
        ):
            self.enabled_providers.sort(
                key=lambda p: 0 if p == self.primary_provider_env else 1
            )

        self._cache: Dict[str, _CacheEntry] = {}

        logger.info(
            "DataEngine v2.3 initialized "
            "(providers=%s, primary=%s, cache_ttl=%ss, timeout=%ss, "
            "v1_delegate=%s, advanced_analysis=%s)",
            self.enabled_providers,
            self.primary_provider_env or "auto",
            self.cache_ttl,
            self.provider_timeout,
            _HAS_V1_ENGINE,
            self.enable_advanced_analysis,
        )

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    async def get_enriched_quote(self, symbol: str) -> UnifiedQuote:
        """
        Main entry point for a single symbol.
        - Uses in-memory cache.
        - Never raises (returns UnifiedQuote with data_quality='MISSING' on errors).
        """
        symbol_norm = self._normalize_symbol(symbol)
        if not symbol_norm:
            return UnifiedQuote(
                symbol="",
                data_quality="MISSING",
                error="Empty or invalid symbol",
            )

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

        # Cache even MISSING responses (short TTL) to avoid hammering providers
        self._cache[symbol_norm] = _CacheEntry(
            expires_at=now + self.cache_ttl,
            quote=quote,
        )
        return quote

    async def get_enriched_quotes(self, symbols: List[str]) -> List[UnifiedQuote]:
        """
        Batch version – concurrently fetches multiple symbols.
        """
        tasks = [self.get_enriched_quote(s) for s in symbols]
        return await asyncio.gather(*tasks)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        s = (symbol or "").strip().upper()
        if not s:
            return ""

        # Normalize some TADAWUL formats to 1120.SR style
        if s.startswith("TADAWUL:"):
            s = s.split(":", 1)[1].strip()
        if s.endswith(".TADAWUL"):
            s = s.replace(".TADAWUL", ".SR")

        # Ensure .SR suffix for pure numeric KSA tickers
        if s.isdigit():
            s = f"{s}.SR"

        return s

    @staticmethod
    def _is_ksa_symbol(symbol: str) -> bool:
        s = (symbol or "").upper()
        return s.endswith(".SR") or s.endswith(".TADAWUL")

    def _now_dt_pair(self) -> Tuple[datetime, Optional[datetime], Optional[str]]:
        """
        Returns (as_of_utc_dt, as_of_local_dt, timezone_name)

        - as_of_utc is always UTC "now".
        - as_of_local is converted using LOCAL_TIMEZONE if possible.
        - timezone_name is LOCAL_TIMEZONE on success, else "UTC".
        """
        now_utc = datetime.now(timezone.utc)
        as_of_local: Optional[datetime] = None
        tz_name: Optional[str] = "UTC"

        if ZoneInfo is not None:
            try:
                tz = ZoneInfo(self.local_tz_name)
                as_of_local = now_utc.astimezone(tz)
                tz_name = self.local_tz_name
            except Exception:
                # Fallback: keep tz_name="UTC" and no local datetime
                as_of_local = None

        return now_utc, as_of_local, tz_name

    # ------------------------------------------------------------------ #
    # Core fetch + provider orchestration
    # ------------------------------------------------------------------ #

    async def _get_enriched_quote_uncached(self, symbol: str) -> UnifiedQuote:
        """
        PRIMARY path:
            - Delegate to v1 engine (core.data_engine) for ALL markets.

        FALLBACK path (only if v1 fails/unavailable):
            - Use direct providers (FMP / EODHD / Finnhub) and merge.
        """
        is_ksa = self._is_ksa_symbol(symbol)

        # 1) Primary: v1 delegate for ALL markets
        if _HAS_V1_ENGINE:
            try:
                # v1 engine exposes an async get_enriched_quotes(List[str]) API
                v1_quotes = await _v1_engine.get_enriched_quotes([symbol])  # type: ignore[attr-defined]
                if v1_quotes:
                    logger.debug("Using v1 delegate engine for %s", symbol)
                    return self._from_v1_quote(v1_quotes[0])
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("V1 delegate engine failed for %s: %s", symbol, exc)

        # 2) Fallback: Multi-provider orchestration (rare in practice)
        providers = list(self.enabled_providers)

        # KSA-safe: never call EODHD for .SR / .TADAWUL symbols
        if is_ksa and "eodhd" in providers:
            providers = [p for p in providers if p != "eodhd"]
            logger.debug(
                "KSA-safe mode (fallback): removed EODHD from providers for %s -> %s",
                symbol,
                providers,
            )

        # Map provider name -> coroutine
        tasks: List[asyncio.Task[UnifiedQuote]] = []

        for provider in providers:
            p = provider.lower()
            if p == "fmp":
                tasks.append(
                    asyncio.create_task(
                        self._safe_call_provider("fmp", self._fetch_from_fmp, symbol)
                    )
                )
            elif p == "eodhd":
                tasks.append(
                    asyncio.create_task(
                        self._safe_call_provider("eodhd", self._fetch_from_eodhd, symbol)
                    )
                )
            elif p == "finnhub":
                tasks.append(
                    asyncio.create_task(
                        self._safe_call_provider("finnhub", self._fetch_from_finnhub, symbol)
                    )
                )
            else:
                logger.warning("Unknown provider '%s' configured; ignoring", p)

        if not tasks:
            return UnifiedQuote(
                symbol=symbol,
                data_quality="MISSING",
                error="No valid providers configured (ENABLED_PROVIDERS) and v1 delegate unavailable",
            )

        results = await asyncio.gather(*tasks)
        return self._merge_provider_results(symbol, results)

    async def _safe_call_provider(
        self,
        provider_name: str,
        func: Callable[[str], Any],
        symbol: str,
    ) -> UnifiedQuote:
        """
        Wrap a provider call so it can never break the engine.
        """
        try:
            q = await func(symbol)
            if q is None:
                return UnifiedQuote(
                    symbol=symbol,
                    data_quality="MISSING",
                    primary_provider=provider_name,
                    provider=provider_name,
                    data_source=provider_name,
                    error=f"{provider_name} returned no data",
                )
            # Ensure provider fields are populated
            q.primary_provider = q.primary_provider or provider_name
            q.provider = q.provider or provider_name
            q.data_source = q.data_source or provider_name
            if not q.sources:
                q.sources = [ProviderSource(provider=provider_name, note="primary")]
            return q
        except Exception as exc:
            logger.exception(
                "Provider '%s' exception for %s: %s", provider_name, symbol, exc
            )
            return UnifiedQuote(
                symbol=symbol,
                data_quality="MISSING",
                primary_provider=provider_name,
                provider=provider_name,
                data_source=provider_name,
                error=f"Provider {provider_name} exception: {exc}",
            )

    def _merge_provider_results(
        self, symbol: str, results: List[UnifiedQuote]
    ) -> UnifiedQuote:
        """
        Merge multiple provider UnifiedQuote results into a single UnifiedQuote.
        - Prefer first OK result, then PARTIAL.
        - Backfill missing fields from other OK/PARTIAL results.
        - Combine sources and error messages.

        This is designed to align cleanly with the "unified sheet" template
        used by the 9-page Google Sheets dashboard.
        """
        if not results:
            return UnifiedQuote(
                symbol=symbol,
                data_quality="MISSING",
                error="No provider results",
            )

        # Separate by quality
        ok_or_partial: List[UnifiedQuote] = [
            q for q in results if q.data_quality in ("OK", "PARTIAL")
        ]

        # All providers failed
        if not ok_or_partial:
            combined_error = "; ".join([q.error for q in results if q.error])  # type: ignore
            return UnifiedQuote(
                symbol=symbol,
                data_quality="MISSING",
                error=combined_error or "All providers failed or returned no data",
            )

        # Choose base quote: first OK, else first PARTIAL
        base: UnifiedQuote = next(
            (q for q in ok_or_partial if q.data_quality == "OK"), ok_or_partial[0]
        )

        # Get field names for UnifiedQuote for safe merging
        field_map = getattr(UnifiedQuote, "model_fields", None)
        if field_map is None:
            field_map = getattr(UnifiedQuote, "__fields__", {})
        field_names = list(field_map.keys())

        # Use dict representation of base as starting point
        if hasattr(base, "model_dump"):
            merged_data: Dict[str, Any] = base.model_dump()
        else:  # pydantic v1 fallback
            merged_data = base.dict()  # type: ignore[attr-defined]

        # Merge from other OK/PARTIAL quotes (only fill Nones)
        for q in ok_or_partial:
            if q is base:
                continue
            if hasattr(q, "model_dump"):
                data = q.model_dump()
            else:
                data = q.dict()  # type: ignore[attr-defined]

            for k in field_names:
                if k == "symbol":
                    continue
                if merged_data.get(k) is None:
                    val = data.get(k)
                    if val is not None:
                        merged_data[k] = val

        # Symbol must be normalized symbol argument
        merged_data["symbol"] = symbol.upper()

        # Combine data_quality: OK if any OK, else PARTIAL
        merged_quality = (
            "OK"
            if any(q.data_quality == "OK" for q in ok_or_partial)
            else "PARTIAL"
        )
        merged_data["data_quality"] = merged_quality

        # Combine sources
        combined_sources: List[ProviderSource] = []
        seen_providers: set[str] = set()
        for q in ok_or_partial:
            # Use q.sources if present, otherwise synthesize from provider fields
            if q.sources:
                for s in q.sources:
                    prov = (s.provider or "").lower()
                    if prov and prov not in seen_providers:
                        seen_providers.add(prov)
                        combined_sources.append(s)
            else:
                prov = (q.primary_provider or q.provider or q.data_source or "").lower()
                if prov and prov not in seen_providers:
                    seen_providers.add(prov)
                    combined_sources.append(
                        ProviderSource(provider=prov, note="merged")
                    )

        if combined_sources:
            merged_data["sources"] = combined_sources
            merged_data["primary_provider"] = (
                merged_data.get("primary_provider")
                or combined_sources[0].provider
            )
            merged_data["provider"] = (
                merged_data.get("provider") or combined_sources[0].provider
            )
            merged_data["data_source"] = (
                merged_data.get("data_source") or combined_sources[0].provider
            )

        # Merge error messages for debugging if everything is PARTIAL
        combined_error = "; ".join(
            sorted({q.error for q in results if q.error})  # type: ignore
        )
        if combined_error:
            merged_data["error"] = combined_error

        try:
            merged_quote = UnifiedQuote(**merged_data)
        except Exception as exc:
            # Extremely defensive: if merge fails, fall back to base
            logger.exception("Failed to merge provider results for %s: %s", symbol, exc)
            return base

        return merged_quote

    # ------------------------------------------------------------------ #
    # Provider: Financial Modeling Prep (FMP) – Fallback only
    # ------------------------------------------------------------------ #

    async def _fetch_from_fmp(self, symbol: str) -> Optional[UnifiedQuote]:
        api_key = os.getenv("FMP_API_KEY")
        if not api_key:
            logger.warning("FMP_API_KEY not set; skipping FMP for %s", symbol)
            return None

        url = f"https://financialmodelingprep.com/api/v3/quote/{symbol}"

        timeout = httpx.Timeout(self.provider_timeout, connect=self.provider_timeout)
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url, params={"apikey": api_key})
            resp.raise_for_status()
            data = resp.json()

        if not isinstance(data, list) or not data:
            return None

        raw = data[0]
        quote = self._map_fmp_to_unified(symbol, raw)
        quote.primary_provider = "fmp"
        quote.provider = "fmp"
        quote.data_source = "fmp"
        if not quote.sources:
            quote.sources = [ProviderSource(provider="fmp", note="FMP quote")]
        return quote

    def _map_fmp_to_unified(self, symbol: str, d: Dict[str, Any]) -> UnifiedQuote:
        """
        Map FMP /quote response into UnifiedQuote.
        """

        def gv(*keys: str, default=None):
            for k in keys:
                if k in d and d[k] is not None:
                    return d[k]
            return default

        last_price = gv("price")
        previous_close = gv("previousClose")
        change = gv("change")
        change_pct = gv("changesPercentage")

        if change is None and last_price is not None and previous_close:
            try:
                change = float(last_price) - float(previous_close)
            except Exception:
                change = None

        if change_pct is None and change is not None and previous_close:
            try:
                change_pct = (float(change) / float(previous_close)) * 100.0
            except Exception:
                change_pct = None

        high_52w = gv("yearHigh")
        low_52w = gv("yearLow")
        position_52w = None
        if (
            high_52w is not None
            and low_52w is not None
            and last_price is not None
            and high_52w != low_52w
        ):
            try:
                position_52w = (float(last_price) - float(low_52w)) / (
                    float(high_52w) - float(low_52w)
                )
                position_52w *= 100.0
            except Exception:
                position_52w = None

        as_of_utc, as_of_local, tz_name = self._now_dt_pair()

        eps_val = gv("eps", "epsTTM")
        pe_val = gv("pe", "peTTM")
        pb_val = gv("priceToBook", "pb", "pbRatio")

        dividend_yield = gv("dividendYield", "lastDiv", "yield")
        if dividend_yield is not None:
            try:
                # FMP often returns dividendYield as %, sometimes as fraction.
                # We standardize to % where possible.
                if dividend_yield < 1.0:
                    dividend_yield_percent = float(dividend_yield) * 100.0
                else:
                    dividend_yield_percent = float(dividend_yield)
            except Exception:
                dividend_yield_percent = None
        else:
            dividend_yield_percent = None

        shares_outstanding = gv("sharesOutstanding", "shares_outstanding")

        q = UnifiedQuote(
            symbol=str(gv("symbol", default=symbol)).upper(),
            name=gv("name"),
            company_name=gv("name"),
            sector=gv("sector"),
            industry=gv("industry"),
            market=gv("exchangeShortName", "exchange"),
            market_region=gv("exchangeShortName", "exchange"),
            exchange=gv("exchangeShortName", "exchange"),
            currency=gv("currency"),
            shares_outstanding=shares_outstanding,
            last_price=last_price,
            price=last_price,
            previous_close=previous_close,
            prev_close=previous_close,
            open=gv("open"),
            high=gv("dayHigh"),
            low=gv("dayLow"),
            change=change,
            change_percent=change_pct,
            change_pct=change_pct,
            high_52w=high_52w,
            low_52w=low_52w,
            fifty_two_week_high=high_52w,
            fifty_two_week_low=low_52w,
            position_52w_percent=position_52w,
            fifty_two_week_position=position_52w,
            volume=gv("volume"),
            avg_volume_30d=gv("avgVolume"),
            average_volume_30d=gv("avgVolume"),
            avg_volume=gv("avgVolume"),
            market_cap=gv("marketCap"),
            eps_ttm=eps_val,
            eps=eps_val,
            pe_ratio=pe_val,
            pe=pe_val,
            pe_ttm=pe_val,
            pb_ratio=pb_val,
            pb=pb_val,
            dividend_yield_percent=dividend_yield_percent,
            dividend_yield=dividend_yield_percent,
            beta=gv("beta"),
            ma_50d=gv("priceAvg50"),
            data_quality="OK" if last_price is not None else "PARTIAL",
            last_updated_utc=as_of_utc,
            last_updated_riyadh=as_of_local,
            as_of_utc=as_of_utc,
            as_of_local=as_of_local,
            timezone=tz_name,
            raw=d,
        )

        if self.enable_advanced_analysis:
            self._apply_basic_scoring(q, source="fmp")

        return q

    # ------------------------------------------------------------------ #
    # Provider: EODHD (fallback – mainly for global, not KSA .SR)
    # ------------------------------------------------------------------ #

    async def _fetch_from_eodhd(self, symbol: str) -> Optional[UnifiedQuote]:
        api_key = os.getenv("EODHD_API_KEY")
        base_url = os.getenv("EODHD_BASE_URL", "https://eodhd.com/api")

        if not api_key:
            logger.warning("EODHD_API_KEY not set; skipping EODHD for %s", symbol)
            return None

        # Map bare symbols like AAPL -> AAPL.US for EODHD
        request_code = symbol
        if "." not in request_code:
            request_code = f"{request_code}.US"

        url = f"{base_url.rstrip('/')}/real-time/{request_code}"
        params = {"api_token": api_key, "fmt": "json"}

        timeout = httpx.Timeout(self.provider_timeout, connect=self.provider_timeout)
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            d = resp.json()

        if not isinstance(d, dict) or not d:
            return None

        q = self._map_eodhd_to_unified(symbol, d)
        q.primary_provider = "eodhd"
        q.provider = "eodhd"
        q.data_source = "eodhd"
        if not q.sources:
            q.sources = [ProviderSource(provider="eodhd", note="EODHD quote")]
        return q

    def _map_eodhd_to_unified(self, symbol: str, d: Dict[str, Any]) -> UnifiedQuote:
        """
        Map EODHD real-time response into UnifiedQuote.
        """

        def gv(*keys: str, default=None):
            for k in keys:
                if k in d and d[k] is not None:
                    return d[k]
            return default

        last_price = gv("close", "price", "last")
        previous_close = gv("previousClose", "previous_close")
        change = gv("change")
        change_pct = gv("change_p", "change_percent")

        if change is None and last_price is not None and previous_close:
            try:
                change = float(last_price) - float(previous_close)
            except Exception:
                change = None

        if change_pct is None and change is not None and previous_close:
            try:
                change_pct = (float(change) / float(previous_close)) * 100.0
            except Exception:
                change_pct = None

        high_52w = gv("fifty_two_week_high", "high_52w", "year_high")
        low_52w = gv("fifty_two_week_low", "low_52w", "year_low")
        position_52w = gv("fifty_two_week_position", "position_52w_percent")

        if (
            position_52w is None
            and high_52w is not None
            and low_52w is not None
            and last_price is not None
            and high_52w != low_52w
        ):
            try:
                position_52w = (float(last_price) - float(low_52w)) / (
                    float(high_52w) - float(low_52w)
                )
                position_52w *= 100.0
            except Exception:
                position_52w = None

        as_of_utc, as_of_local, tz_name = self._now_dt_pair()

        eps_val = gv("eps", "EPS")
        pe_val = gv("pe", "PE", "pe_ratio")
        pb_val = gv("pb", "P_B", "price_to_book", "priceToBook")
        dividend_yield = gv("dividend_yield", "DividendYield")

        market_cap = gv("market_cap", "marketCapitalization")
        volume_val = gv("volume")
        avg_vol = gv("avgVolume", "average_volume")

        q = UnifiedQuote(
            symbol=str(gv("code", "symbol", default=symbol)).upper(),
            name=gv("name"),
            company_name=gv("name"),
            market=gv("exchange_short_name", "exchange"),
            market_region=gv("exchange_short_name", "exchange"),
            exchange=gv("exchange_short_name", "exchange"),
            currency=gv("currency", "currency_code"),
            last_price=last_price,
            price=last_price,
            previous_close=previous_close,
            prev_close=previous_close,
            open=gv("open"),
            high=gv("high"),
            low=gv("low"),
            change=change,
            change_percent=change_pct,
            change_pct=change_pct,
            high_52w=high_52w,
            low_52w=low_52w,
            fifty_two_week_high=high_52w,
            fifty_two_week_low=low_52w,
            position_52w_percent=position_52w,
            fifty_two_week_position=position_52w,
            volume=volume_val,
            avg_volume_30d=avg_vol,
            average_volume_30d=avg_vol,
            avg_volume=avg_vol,
            market_cap=market_cap,
            eps_ttm=eps_val,
            eps=eps_val,
            pe_ratio=pe_val,
            pe=pe_val,
            pe_ttm=pe_val,
            pb_ratio=pb_val,
            pb=pb_val,
            dividend_yield=dividend_yield,
            dividend_yield_percent=dividend_yield,
            data_quality="OK" if last_price is not None else "PARTIAL",
            last_updated_utc=as_of_utc,
            last_updated_riyadh=as_of_local,
            as_of_utc=as_of_utc,
            as_of_local=as_of_local,
            timezone=tz_name,
            raw=d,
        )

        if self.enable_advanced_analysis:
            self._apply_basic_scoring(q, source="eodhd")

        return q

    # ------------------------------------------------------------------ #
    # Provider: Finnhub (fallback – real-time price only)
    # ------------------------------------------------------------------ #

    async def _fetch_from_finnhub(self, symbol: str) -> Optional[UnifiedQuote]:
        api_key = os.getenv("FINNHUB_API_KEY")
        if not api_key:
            logger.warning("FINNHUB_API_KEY not set; skipping Finnhub for %s", symbol)
            return None

        url = "https://finnhub.io/api/v1/quote"
        params = {"symbol": symbol, "token": api_key}

        timeout = httpx.Timeout(self.provider_timeout, connect=self.provider_timeout)
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            d = resp.json()

        if not isinstance(d, dict) or not d:
            return None

        # Finnhub uses c/h/l/o/pc/t format
        last_price = d.get("c")
        previous_close = d.get("pc")
        open_price = d.get("o")
        high = d.get("h")
        low = d.get("l")

        change = None
        change_pct = None
        if last_price is not None and previous_close:
            try:
                change = float(last_price) - float(previous_close)
                change_pct = (float(change) / float(previous_close)) * 100.0
            except Exception:
                change = None
                change_pct = None

        as_of_utc, as_of_local, tz_name = self._now_dt_pair()

        q = UnifiedQuote(
            symbol=symbol.upper(),
            last_price=last_price,
            price=last_price,
            previous_close=previous_close,
            prev_close=previous_close,
            open=open_price,
            high=high,
            low=low,
            change=change,
            change_percent=change_pct,
            change_pct=change_pct,
            volume=None,  # Finnhub quote doesn't always include volume here
            data_quality="PARTIAL" if last_price is not None else "MISSING",
            last_updated_utc=as_of_utc,
            last_updated_riyadh=as_of_local,
            as_of_utc=as_of_utc,
            as_of_local=as_of_local,
            timezone=tz_name,
            raw=d,
        )

        if self.enable_advanced_analysis:
            self._apply_basic_scoring(q, source="finnhub")

        return q

    # ------------------------------------------------------------------ #
    # KSA/global v1 delegate mapping (from v1 UnifiedQuote to v2)
    # ------------------------------------------------------------------ #

    def _from_v1_quote(self, v1_q: Any) -> UnifiedQuote:
        """
        Map a core.data_engine.UnifiedQuote (v1) instance to this v2 UnifiedQuote.
        This keeps all legacy fields while aligning with the 9-page template.
        """

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
        open_price = gv("open")
        high = gv("high")
        low = gv("low")
        change = gv("change")
        change_pct = gv("change_pct", "change_percent")

        high_52w = gv("fifty_two_week_high", "high_52w")
        low_52w = gv("fifty_two_week_low", "low_52w")
        pos_52w = gv("fifty_two_week_position", "position_52w_percent")

        volume = gv("volume")
        avg_volume = gv("avg_volume", "avg_volume_30d")
        market_cap = gv("market_cap")
        shares_outstanding = gv("shares_outstanding")
        free_float = gv("free_float")

        eps_ttm = gv("eps_ttm", "eps")
        pe_ttm = gv("pe_ttm", "pe", "pe_ratio")
        pb = gv("pb", "pb_ratio")
        dividend_yield = gv("dividend_yield", "dividend_yield_percent")
        dividend_payout_ratio = gv("dividend_payout_ratio")
        roe = gv("roe", "roe_percent")
        roa = gv("roa", "roa_percent")
        profit_margin = gv("profit_margin", "net_margin_percent")
        debt_to_equity = gv("debt_to_equity")
        current_ratio = gv("current_ratio")
        quick_ratio = gv("quick_ratio")

        revenue_growth = gv("revenue_growth_percent", "revenue_growth", "revenue_growth_yoy")
        net_income_growth = gv(
            "net_income_growth_percent", "net_income_growth", "net_income_growth_yoy"
        )
        ebitda_margin = gv("ebitda_margin_percent", "ebitda_margin")
        operating_margin = gv("operating_margin_percent", "operating_margin")
        net_margin = gv("net_margin_percent", "net_margin")

        ev_to_ebitda = gv("ev_to_ebitda", "ev_ebitda")
        price_to_sales = gv("price_to_sales", "ps_ratio")
        price_to_cash_flow = gv("price_to_cash_flow", "pcf_ratio")
        peg_ratio = gv("peg_ratio", "peg")
        beta = gv("beta")
        volatility_30d = gv("volatility_30d_percent", "volatility_30d")

        value_score = gv("value_score")
        quality_score = gv("quality_score")
        momentum_score = gv("momentum_score")
        opportunity_score = gv("opportunity_score")
        recommendation = gv("recommendation")
        valuation_label = gv("valuation_label")
        fair_value = gv("fair_value")
        upside_percent = gv("upside_percent")

        rsi_14 = gv("rsi_14")
        macd = gv("macd")
        ma_20d = gv("ma_20d", "moving_average_20d")
        ma_50d = gv("ma_50d", "moving_average_50d")

        data_quality = gv("data_quality", default="UNKNOWN")
        sources = gv("sources")
        last_updated_utc = gv("last_updated_utc")
        last_updated_riyadh = gv("last_updated_riyadh", "last_updated_local")

        as_of_utc = last_updated_utc
        as_of_local = last_updated_riyadh
        tz_name = gv("timezone")

        if last_updated_utc is None:
            # Fall back to "now" if v1 doesn't provide timestamps
            as_of_utc, as_of_local, tz_name = self._now_dt_pair()
            last_updated_utc = as_of_utc
            last_updated_riyadh = as_of_local

        provider = None
        primary_provider = None
        provider_sources: Optional[List[ProviderSource]] = None

        if sources:
            try:
                if isinstance(sources, list) and sources:
                    tmp_sources: List[ProviderSource] = []
                    for s in sources:
                        if isinstance(s, dict):
                            prov = str(
                                s.get("provider")
                                or s.get("name")
                                or s.get("source")
                                or ""
                            )
                            tmp_sources.append(
                                ProviderSource(
                                    provider=prov,
                                    weight=s.get("weight"),
                                    quality=s.get("quality"),
                                )
                            )
                        else:
                            prov = str(
                                getattr(s, "provider", None)
                                or getattr(s, "name", None)
                                or getattr(s, "source", None)
                                or ""
                            )
                            tmp_sources.append(
                                ProviderSource(
                                    provider=prov,
                                    weight=getattr(s, "weight", None),
                                    quality=getattr(s, "quality", None),
                                )
                            )
                    provider_sources = tmp_sources
                    if tmp_sources:
                        primary_provider = tmp_sources[0].provider
                        provider = primary_provider
            except Exception:  # pragma: no cover - extremely defensive
                provider_sources = None
                provider = None
                primary_provider = None

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
            shares_outstanding=shares_outstanding,
            free_float=free_float,
            last_price=last_price,
            price=last_price,
            previous_close=previous_close,
            prev_close=previous_close,
            open=open_price,
            high=high,
            low=low,
            change=change,
            change_percent=change_pct,
            change_pct=change_pct,
            high_52w=high_52w,
            low_52w=low_52w,
            fifty_two_week_high=high_52w,
            fifty_two_week_low=low_52w,
            position_52w_percent=pos_52w,
            fifty_two_week_position=pos_52w,
            volume=volume,
            avg_volume_30d=avg_volume,
            average_volume_30d=avg_volume,
            avg_volume=avg_volume,
            market_cap=market_cap,
            eps_ttm=eps_ttm,
            eps=eps_ttm,
            pe_ratio=pe_ttm,
            pe=pe_ttm,
            pe_ttm=pe_ttm,
            pb_ratio=pb,
            pb=pb,
            dividend_yield=dividend_yield,
            dividend_yield_percent=dividend_yield,
            dividend_payout_ratio=dividend_payout_ratio,
            roe=roe,
            roe_percent=roe,
            roa=roa,
            roa_percent=roa,
            profit_margin=profit_margin,
            net_margin_percent=net_margin or profit_margin,
            debt_to_equity=debt_to_equity,
            current_ratio=current_ratio,
            quick_ratio=quick_ratio,
            revenue_growth_percent=revenue_growth,
            net_income_growth_percent=net_income_growth,
            ebitda_margin_percent=ebitda_margin,
            operating_margin_percent=operating_margin,
            net_margin_percent=net_margin or profit_margin,
            ev_to_ebitda=ev_to_ebitda,
            price_to_sales=price_to_sales,
            price_to_cash_flow=price_to_cash_flow,
            peg_ratio=peg_ratio,
            beta=beta,
            volatility_30d=volatility_30d,
            volatility_30d_percent=volatility_30d,
            fair_value=fair_value,
            upside_percent=upside_percent,
            valuation_label=valuation_label,
            value_score=value_score,
            quality_score=quality_score,
            momentum_score=momentum_score,
            opportunity_score=opportunity_score,
            recommendation=recommendation,
            rsi_14=rsi_14,
            macd=macd,
            ma_20d=ma_20d,
            ma_50d=ma_50d,
            data_quality=data_quality,
            primary_provider=primary_provider,
            provider=provider,
            data_source=provider,
            sources=provider_sources,
            last_updated_utc=last_updated_utc,
            last_updated_riyadh=last_updated_riyadh,
            as_of_utc=as_of_utc,
            as_of_local=as_of_local,
            timezone=tz_name,
            raw=None,
        )

        if self.enable_advanced_analysis:
            # v1 might already have scores; only top-up if missing
            if (
                q.value_score is None
                or q.quality_score is None
                or q.momentum_score is None
            ):
                self._apply_basic_scoring(q, source="v1_delegate")

        return q

    # ------------------------------------------------------------------ #
    # Basic AI-style scoring (Value / Quality / Momentum / Opportunity)
    # ------------------------------------------------------------------ #

    def _apply_basic_scoring(self, q: UnifiedQuote, source: str) -> None:
        """
        Lightweight scoring engine:
        - Value score: based mainly on PE and dividend yield
        - Quality score: EPS sign + PE range
        - Momentum score: % change + 52W position + basic volatility penalty
        - Opportunity score: weighted combination

        This is intentionally simple and deterministic so that
        Google Sheets & Apps Script can rely on it as a stable layer.
        """
        # -------------------------
        # Quality score (EPS + PE)
        # -------------------------
        quality = 50.0
        pe = q.pe_ratio or q.pe or q.pe_ttm
        eps = q.eps_ttm or q.eps

        if eps is not None and eps > 0 and pe is not None and pe > 0:
            if pe < 10:
                quality = 80.0
            elif pe < 20:
                quality = 70.0
            elif pe < 30:
                quality = 60.0
            else:
                quality = 50.0
        elif eps is not None and eps <= 0:
            quality = 30.0

        # -------------------------
        # Value score (PE + Yield)
        # -------------------------
        value = 50.0
        if pe is not None and pe > 0:
            try:
                # 10 PE -> ~67, 20 PE -> ~50, 5 PE -> ~80, very rough
                value = max(10.0, min(90.0, 100.0 / (1.0 + pe / 10.0)))
            except Exception:
                value = 50.0

        dy = q.dividend_yield_percent or q.dividend_yield
        if dy is not None and dy > 0:
            try:
                value = min(90.0, value + min(20.0, float(dy)))
            except Exception:
                pass

        # -------------------------
        # Momentum score
        # -------------------------
        momentum = 50.0
        if q.change_percent is not None or q.change_pct is not None:
            try:
                cp = float(
                    q.change_percent
                    if q.change_percent is not None
                    else q.change_pct  # type: ignore[arg-type]
                )
                momentum += max(-20.0, min(20.0, cp))
            except Exception:
                pass

        if q.position_52w_percent is not None:
            try:
                p = float(q.position_52w_percent)
                if p > 80:
                    momentum += 5.0
                elif p < 20:
                    momentum -= 5.0
            except Exception:
                pass

        # Volatility penalty (if 30D vol is high, reduce momentum slightly)
        vol = q.volatility_30d_percent or q.volatility_30d
        if vol is not None:
            try:
                v = float(vol)
                # Above ~25% 30D vol, treat as higher risk and subtract up to 15 pts
                if v > 25.0:
                    penalty = min(15.0, (v - 25.0) / 2.0)
                    momentum -= max(0.0, penalty)
            except Exception:
                pass

        # Clamp scores
        value = max(0.0, min(100.0, value))
        quality = max(0.0, min(100.0, quality))
        momentum = max(0.0, min(100.0, momentum))

        # -------------------------
        # Opportunity score (weighted)
        # -------------------------
        opportunity = value * 0.4 + quality * 0.3 + momentum * 0.3

        q.value_score = round(value, 2)
        q.quality_score = round(quality, 2)
        q.momentum_score = round(momentum, 2)
        q.opportunity_score = round(opportunity, 2)

        # Rough recommendation buckets
        if opportunity >= 80:
            q.recommendation = "STRONG_BUY"
        elif opportunity >= 65:
            q.recommendation = "BUY"
        elif opportunity >= 50:
            q.recommendation = "HOLD"
        elif opportunity >= 35:
            q.recommendation = "REDUCE"
        else:
            q.recommendation = "SELL"

        # Valuation label based on value score
        if q.value_score is not None:
            if q.value_score >= 80:
                q.valuation_label = "DEEP_VALUE"
            elif q.value_score >= 65:
                q.valuation_label = "UNDERVALUED"
            elif q.value_score >= 45:
                q.valuation_label = "FAIR_VALUE"
            else:
                q.valuation_label = "EXPENSIVE"


__all__ = ["UnifiedQuote", "ProviderSource", "DataEngine"]
