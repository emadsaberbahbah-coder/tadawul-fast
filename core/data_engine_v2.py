# core/data_engine_v2.py
from __future__ import annotations

import asyncio
import logging
import math
import os
import random
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import httpx
from cachetools import TTLCache
from pydantic import BaseModel, ConfigDict, Field

from core.config import get_settings

# Optional deps (graceful fallback)
try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:  # pragma: no cover
    BeautifulSoup = None

try:
    import yfinance as yf  # type: ignore
except Exception:  # pragma: no cover
    yf = None


ENGINE_VERSION = "2.4.1"

# =============================================================================
# Settings + Logger
# =============================================================================
settings = get_settings()
logger = logging.getLogger("core.data_engine_v2")

# =============================================================================
# Provider URLs
# =============================================================================
EODHD_RT_URL = "https://eodhd.com/api/real-time/{symbol}"
EODHD_FUND_URL = "https://eodhd.com/api/fundamentals/{symbol}"
FMP_QUOTE_URL = "https://financialmodelingprep.com/api/v3/quote/{symbol}"

FINNHUB_QUOTE_URL = "https://finnhub.io/api/v1/quote"
FINNHUB_PROFILE_URL = "https://finnhub.io/api/v1/stock/profile2"
FINNHUB_METRIC_URL = "https://finnhub.io/api/v1/stock/metric"

# Argaam (best-effort; HTML can change)
ARGAAM_PRICES_URLS = [
    "https://www.argaam.com/ar/company/companies-prices/3",
    "https://www.argaam.com/en/company/companies-prices/3",
]
ARGAAM_VOLUME_URLS = [
    "https://www.argaam.com/ar/company/companies-volume/14",
    "https://www.argaam.com/en/company/companies-volume/14",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Riyadh is UTC+3 (no DST)
RIYADH_TZ = timezone(timedelta(hours=3))

# =============================================================================
# Helpers (env/settings adapters)
# =============================================================================
_TRUTHY = {"1", "true", "yes", "on", "y", "t"}
_FALSY = {"0", "false", "no", "off", "n", "f"}

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_riyadh_iso() -> str:
    return datetime.now(RIYADH_TZ).isoformat()


def _get_attr_or_env(names: Sequence[str], default: Any = None) -> Any:
    """
    Tries: settings.<name> then env <name> (case-insensitive).
    Returns first non-None, else default.
    """
    for n in names:
        if hasattr(settings, n):
            v = getattr(settings, n)
            if v is not None:
                return v
    for n in names:
        v = os.getenv(n) or os.getenv(n.upper()) or os.getenv(n.lower())
        if v is not None:
            return v
    return default


def _get_bool(names: Sequence[str], default: bool) -> bool:
    raw = _get_attr_or_env(names, None)
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


def _get_int(names: Sequence[str], default: int) -> int:
    raw = _get_attr_or_env(names, None)
    if raw is None:
        return default
    try:
        v = int(str(raw).strip())
        return v if v > 0 else default
    except Exception:
        return default


def _get_float(names: Sequence[str], default: float) -> float:
    raw = _get_attr_or_env(names, None)
    if raw is None:
        return default
    try:
        v = float(str(raw).strip())
        return v if v > 0 else default
    except Exception:
        return default


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def _safe_float(val: Any) -> Optional[float]:
    """
    Robust numeric parser:
    - supports Arabic digits
    - strips %, +, currency tokens, commas
    - supports suffix K/M/B (e.g. 1.2B)
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
        s = s.replace("SAR", "").replace("USD", "").replace("ريال", "").strip()
        s = s.replace("+", "").strip()

        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        s = s.replace("%", "").strip()
        s = s.replace(",", "")

        mult = 1.0
        m = re.match(r"^(-?\d+(\.\d+)?)([KMB])$", s, re.IGNORECASE)
        if m:
            num = m.group(1)
            suf = m.group(3).upper()
            if suf == "K":
                mult = 1_000.0
            elif suf == "M":
                mult = 1_000_000.0
            elif suf == "B":
                mult = 1_000_000_000.0
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


def normalize_symbol(raw: str) -> str:
    """
    - KSA numeric => 1120.SR
    - already has suffix => keep (upper)
    - Yahoo-style (^GSPC, GC=F, EURUSD=X) => keep
    - otherwise => .US (AAPL => AAPL.US)
    """
    s = (raw or "").strip().upper()
    if not s:
        return ""

    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "")

    if any(ch in s for ch in ("=", "^")):
        return s

    if s.isdigit():
        return f"{s}.SR"

    if "." in s:
        return s

    return f"{s}.US"


def is_ksa_symbol(symbol: str) -> bool:
    s = (symbol or "").upper().strip()
    return s.endswith(".SR") or s.isdigit()


def _fmp_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    return s[:-3] if s.endswith(".US") else s


def _finnhub_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return s
    if any(ch in s for ch in ("=", "^")):
        return s
    if s.endswith(".US"):
        return s[:-3]
    return s


def _yahoo_symbol(symbol: str) -> str:
    """
    yfinance is generally happier with plain tickers (AAPL not AAPL.US).
    Keep special symbols unchanged.
    """
    s = (symbol or "").strip().upper()
    if not s:
        return s
    if any(ch in s for ch in ("=", "^")):
        return s
    if s.endswith(".US"):
        return s[:-3]
    return s


def _get_enabled_providers() -> List[str]:
    """
    Reads provider order from (first match wins):
      - settings.providers_list (list)
      - settings.providers (csv)
      - env ENABLED_PROVIDERS / PROVIDERS (csv)
    Defaults to: finnhub, fmp, eodhd (and yfinance as fallback if enabled).
    """
    pl = getattr(settings, "providers_list", None)
    if isinstance(pl, list) and pl:
        return [str(x).strip().lower() for x in pl if str(x).strip()]

    p_csv = getattr(settings, "providers", None)
    if isinstance(p_csv, str) and p_csv.strip():
        return [p.strip().lower() for p in p_csv.split(",") if p.strip()]

    env_csv = os.getenv("ENABLED_PROVIDERS") or os.getenv("PROVIDERS") or ""
    if env_csv.strip():
        return [p.strip().lower() for p in env_csv.split(",") if p.strip()]

    return ["finnhub", "fmp", "eodhd"]


def _get_key(*names: str) -> Optional[str]:
    for n in names:
        v = getattr(settings, n, None)
        if isinstance(v, str) and v.strip():
            return v.strip()
    for n in names:
        v = os.getenv(n.upper()) or os.getenv(n)
        if v and v.strip():
            return v.strip()
    return None


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


# =============================================================================
# Data model (Pydantic v2) — aligned with 59-column schema
# =============================================================================
class UnifiedQuote(BaseModel):
    model_config = ConfigDict(populate_by_name=True, from_attributes=True, validate_assignment=True)

    # Identity
    symbol: str
    name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    sub_sector: Optional[str] = None
    market: str = "UNKNOWN"  # KSA / GLOBAL / UNKNOWN
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
    data_quality: str = "MISSING"  # FULL / PARTIAL / MISSING
    last_updated_utc: str = Field(default_factory=_now_utc_iso)
    last_updated_riyadh: str = Field(default_factory=_now_riyadh_iso)
    error: Optional[str] = None

    def finalize(self) -> "UnifiedQuote":
        if not self.last_updated_utc:
            self.last_updated_utc = _now_utc_iso()
        if not self.last_updated_riyadh:
            self.last_updated_riyadh = _now_riyadh_iso()

        if not self.market or self.market == "UNKNOWN":
            self.market = "KSA" if is_ksa_symbol(self.symbol) else "GLOBAL"
        if not self.currency:
            self.currency = "SAR" if self.market == "KSA" else "USD"

        if self.current_price is not None and self.previous_close is not None:
            if self.price_change is None:
                self.price_change = _safe_float(self.current_price - self.previous_close)
            if self.percent_change is None and self.previous_close not in (0, None):
                self.percent_change = _safe_float(
                    (self.current_price - self.previous_close) / self.previous_close * 100.0
                )

        if (
            self.position_52w_percent is None
            and self.current_price is not None
            and self.high_52w is not None
            and self.low_52w is not None
        ):
            rng = self.high_52w - self.low_52w
            if rng and rng > 0:
                self.position_52w_percent = _safe_float((self.current_price - self.low_52w) / rng * 100.0)

        if self.value_traded is None and self.current_price is not None and self.volume is not None:
            self.value_traded = _safe_float(self.current_price * self.volume)

        if self.turnover_percent is None and self.volume is not None and self.shares_outstanding not in (None, 0):
            self.turnover_percent = _safe_float((self.volume / self.shares_outstanding) * 100.0)

        # Liquidity score (0..100) based on value_traded log scale
        if self.liquidity_score is None and self.value_traded is not None and self.value_traded >= 0:
            # 1e4 -> 0, 1e8 -> ~100
            ls = (math.log10(self.value_traded + 1.0) - 4.0) * 25.0
            self.liquidity_score = _safe_float(_clamp(ls, 0.0, 100.0))

        if self.current_price is None:
            self.data_quality = "MISSING"
        else:
            have_core = all(x is not None for x in (self.current_price, self.previous_close, self.day_high, self.day_low))
            self.data_quality = "FULL" if have_core else "PARTIAL"

        if self.overall_score is None:
            self._calculate_simple_scores()

        return self

    def _calculate_simple_scores(self) -> None:
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


# =============================================================================
# Engine
# =============================================================================
class DataEngine:
    """
    Async multi-provider engine with KSA-safe routing.

    KSA (.SR):
      1) Legacy KSA delegate if available (core.data_engine)
      2) Argaam snapshot (HTML best-effort)
      3) Optional yfinance last resort (often blocked/401)

    GLOBAL:
      provider order from settings/providers:
        - eodhd (quote + optional fundamentals)
        - finnhub (quote + profile/metrics)
        - fmp (quote)
        - yfinance (fallback)
    """

    def __init__(self) -> None:
        self.enabled_providers: List[str] = _get_enabled_providers()

        self._timeout_sec: float = _get_float(["HTTP_TIMEOUT_SEC", "http_timeout_sec", "HTTP_TIMEOUT"], 25.0)
        max_conn = _get_int(["HTTP_MAX_CONNECTIONS", "MAX_CONNECTIONS"], 40)
        max_keepalive = _get_int(["HTTP_MAX_KEEPALIVE", "MAX_KEEPALIVE_CONNECTIONS"], 20)

        # Fine-grained timeout (connect/read/write/pool)
        timeout = httpx.Timeout(self._timeout_sec, connect=min(10.0, self._timeout_sec))

        self.client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.8,ar;q=0.6",
            },
            limits=httpx.Limits(max_keepalive_connections=max_keepalive, max_connections=max_conn),
        )

        self.enable_yfinance = _get_bool(["ENABLE_YFINANCE", "enable_yfinance"], True)

        quote_ttl = _get_float(["QUOTE_TTL_SEC", "quote_ttl_sec"], 30.0)
        cache_ttl = _get_float(["CACHE_TTL_SEC", "cache_ttl_sec"], 20.0)
        fund_ttl = _get_float(["FUNDAMENTALS_TTL_SEC", "fundamentals_ttl_sec"], 21600.0)
        snap_ttl = _get_float(["ARGAAM_SNAPSHOT_TTL_SEC", "argaam_snapshot_ttl_sec"], 30.0)

        self.quote_cache: TTLCache = TTLCache(maxsize=3000, ttl=max(5.0, quote_ttl))
        self.stale_cache: TTLCache = TTLCache(maxsize=5000, ttl=max(120.0, cache_ttl * 10.0))
        self.fund_cache: TTLCache = TTLCache(maxsize=2500, ttl=max(300.0, fund_ttl))
        self.snapshot_cache: TTLCache = TTLCache(maxsize=50, ttl=max(10.0, snap_ttl))

        self._sem = asyncio.Semaphore(_get_int(["ENGINE_CONCURRENCY", "ENRICHED_BATCH_CONCURRENCY"], 10))

        logger.info(
            "DataEngine v%s init | providers=%s | yfinance=%s | timeout=%.1fs",
            ENGINE_VERSION,
            ",".join(self.enabled_providers) if self.enabled_providers else "(none)",
            self.enable_yfinance,
            self._timeout_sec,
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------
    async def get_quote(self, symbol: str) -> UnifiedQuote:
        s = normalize_symbol(symbol)
        if not s:
            return UnifiedQuote(symbol=str(symbol or ""), data_quality="MISSING", error="Empty symbol").finalize()

        hit = self.quote_cache.get(s)
        if hit is not None:
            return hit

        stale = self.stale_cache.get(s)

        try:
            async with self._sem:
                if is_ksa_symbol(s):
                    q = await self._fetch_ksa(s)
                else:
                    q = await self._fetch_global(s)

            q = q.finalize()

            # Optional scoring (no hard dependency)
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
        items = [s for s in (symbols or []) if s and str(s).strip()]
        if not items:
            return []

        results = await asyncio.gather(*(self.get_quote(s) for s in items), return_exceptions=True)

        out: List[UnifiedQuote] = []
        for raw_sym, res in zip(items, results):
            if isinstance(res, Exception):
                sym = normalize_symbol(raw_sym) or str(raw_sym)
                out.append(UnifiedQuote(symbol=sym, data_quality="MISSING", error=str(res)).finalize())
            else:
                out.append(res)
        return out

    async def get_enriched_quote(self, symbol: str) -> UnifiedQuote:
        return await self.get_quote(symbol)

    async def get_enriched_quotes(self, symbols: Sequence[str]) -> List[UnifiedQuote]:
        return await self.get_quotes(symbols)

    # -------------------------------------------------------------------------
    # KSA
    # -------------------------------------------------------------------------
    async def _fetch_ksa(self, symbol: str) -> UnifiedQuote:
        # 1) Prefer legacy KSA engine
        try:
            from core import data_engine as legacy  # type: ignore
            if hasattr(legacy, "get_enriched_quote"):
                raw = await legacy.get_enriched_quote(symbol)  # type: ignore
                q = self._coerce_to_unified_quote(symbol, raw, market="KSA", currency="SAR", source="legacy")
                if q.current_price is not None:
                    q.data_source = q.data_source or "legacy"
                    q.data_quality = "PARTIAL"
                    return q
        except Exception:
            pass

        # 2) Argaam snapshot
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

        # 3) yfinance last resort
        if self.enable_yfinance and (("yfinance" in self.enabled_providers) or ("yahoo" in self.enabled_providers)) and yf:
            q = await self._fetch_yfinance(symbol, market="KSA")
            if q.current_price is not None:
                q.data_source = q.data_source or "yfinance"
                q.data_quality = "PARTIAL"
                return q

        return UnifiedQuote(
            symbol=symbol,
            market="KSA",
            currency="SAR",
            data_quality="MISSING",
            error="No KSA provider data (legacy+argaam+yfinance failed)",
        ).finalize()

    def _coerce_to_unified_quote(
        self,
        symbol: str,
        raw: Any,
        market: str,
        currency: str,
        source: str,
    ) -> UnifiedQuote:
        data: Dict[str, Any] = {}

        if raw is None:
            data = {}
        elif isinstance(raw, UnifiedQuote):
            return raw
        elif isinstance(raw, dict):
            data = dict(raw)
        else:
            if hasattr(raw, "model_dump"):
                try:
                    data = raw.model_dump()  # type: ignore
                except Exception:
                    data = {}
            elif hasattr(raw, "dict"):
                try:
                    data = raw.dict()  # type: ignore
                except Exception:
                    data = {}
            else:
                data = {}

        mapped: Dict[str, Any] = {
            "symbol": symbol,
            "market": data.get("market") or market,
            "currency": data.get("currency") or currency,
            "name": data.get("name") or data.get("company_name"),
        }

        mapped["current_price"] = data.get("current_price") or data.get("last_price") or data.get("lastPrice") or data.get("price")
        mapped["previous_close"] = data.get("previous_close") or data.get("previousClose")
        mapped["open"] = data.get("open")
        mapped["day_high"] = data.get("day_high") or data.get("high")
        mapped["day_low"] = data.get("day_low") or data.get("low")
        mapped["volume"] = data.get("volume")

        mapped["price_change"] = data.get("price_change") or data.get("change")
        mapped["percent_change"] = data.get("percent_change") or data.get("change_percent") or data.get("changePercent")

        for k in (
            "market_cap",
            "shares_outstanding",
            "eps_ttm",
            "pe_ttm",
            "pb",
            "dividend_yield",
            "beta",
            "roe",
            "roa",
            "net_margin",
            "high_52w",
            "low_52w",
            "ma20",
            "ma50",
        ):
            if k in data and data.get(k) is not None:
                mapped[k] = data.get(k)

        q = UnifiedQuote(**{k: v for k, v in mapped.items() if v is not None})
        q.data_source = data.get("data_source") or source
        q.error = data.get("error")
        return q

    async def _argaam_snapshot(self) -> Dict[str, Dict[str, Any]]:
        key = "argaam_snapshot_v2"
        cached = self.snapshot_cache.get(key)
        if isinstance(cached, dict):
            return cached

        merged: Dict[str, Dict[str, Any]] = {}

        prices_html = await self._http_get_first_text(ARGAAM_PRICES_URLS)
        if prices_html:
            part = self._parse_argaam_table(prices_html)
            for k, v in part.items():
                merged.setdefault(k, {}).update(v)

        vol_html = await self._http_get_first_text(ARGAAM_VOLUME_URLS)
        if vol_html:
            part = self._parse_argaam_table(vol_html)
            for k, v in part.items():
                merged.setdefault(k, {}).update(v)

        self.snapshot_cache[key] = merged
        return merged

    async def _http_get_first_text(self, urls: Sequence[str]) -> Optional[str]:
        tasks = [self._http_get_text(u) for u in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        def _looks_ok(txt: str) -> bool:
            t = txt.lower()
            if len(txt) < 500:
                return False
            if "access denied" in t or "cloudflare" in t:
                return False
            return True

        for r in results:
            if isinstance(r, str) and _looks_ok(r):
                return r
        for r in results:
            if isinstance(r, str) and r.strip():
                return r
        return None

    def _parse_argaam_table(self, html: str) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        if not BeautifulSoup or not html:
            return out

        # lxml may not exist; fallback safely
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:
            soup = BeautifulSoup(html, "html.parser")

        for row in soup.find_all("tr"):
            cols = row.find_all("td")
            if len(cols) < 4:
                continue

            txt = [c.get_text(" ", strip=True) for c in cols]

            code = None
            code_idx = None
            for i in range(min(3, len(txt))):
                cand = (txt[i] or "").strip().translate(_ARABIC_DIGITS)
                if re.match(r"^\d{3,6}$", cand):
                    code = cand
                    code_idx = i
                    break
            if not code or code_idx is None:
                continue

            name = _safe_str(txt[code_idx + 1]) if (code_idx + 1) < len(txt) else None

            nums: List[Optional[float]] = []
            for j in range(code_idx + 2, len(txt)):
                nums.append(_safe_float(txt[j]))

            price = nums[0] if len(nums) >= 1 else None
            change = nums[1] if len(nums) >= 2 else None
            chg_p = nums[2] if len(nums) >= 3 else None

            volume = None
            value_traded = None
            if len(txt) >= 8:
                tail = [txt[-k] for k in range(1, min(6, len(txt)))]
                tail_nums = [_safe_float(x) for x in tail]
                for v in tail_nums:
                    if v is not None and v > 0:
                        volume = volume or v

                if price and volume:
                    vt_guess = price * volume
                    for v in tail_nums:
                        if v is not None and vt_guess > 0 and (0.2 * vt_guess) <= v <= (5.0 * vt_guess):
                            value_traded = v
                            break

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
    # GLOBAL: provider loop
    # -------------------------------------------------------------------------
    async def _fetch_global(self, symbol: str) -> UnifiedQuote:
        if any(ch in symbol for ch in ("=", "^")):
            if self.enable_yfinance and (("yfinance" in self.enabled_providers) or ("yahoo" in self.enabled_providers)) and yf:
                q = await self._fetch_yfinance(symbol, market="GLOBAL")
                if q.current_price is not None:
                    q.data_source = "yfinance"
                    q.data_quality = "PARTIAL"
                    return q.finalize()
            return UnifiedQuote(
                symbol=symbol,
                market="GLOBAL",
                currency="USD",
                data_quality="MISSING",
                error="Special symbol requires yfinance (disabled/unavailable)",
            ).finalize()

        errors: List[str] = []

        for prov in self.enabled_providers:
            p = (prov or "").strip().lower()
            if not p:
                continue

            if p == "eodhd":
                api_key = _get_key("eodhd_api_key", "EODHD_API_KEY")
                if not api_key:
                    errors.append("eodhd:no_api_key")
                    continue
                q = await self._fetch_eodhd_realtime(symbol, api_key)
                if q.current_price is not None:
                    fetch_fund = _get_bool(["EODHD_FETCH_FUNDAMENTALS"], False)
                    if fetch_fund:
                        fund = await self._fetch_eodhd_fundamentals(symbol, api_key)
                        if fund:
                            q = q.model_copy(update={k: v for k, v in fund.items() if v is not None})
                    q.data_source = "eodhd"
                    q.market = q.market or "GLOBAL"
                    q.currency = q.currency or "USD"
                    q.data_quality = "FULL" if q.previous_close is not None else "PARTIAL"
                    return q.finalize()
                errors.append(f"eodhd:{q.error or 'no_price'}")

            elif p == "finnhub":
                api_key = _get_key("finnhub_api_key", "FINNHUB_API_KEY")
                if not api_key:
                    errors.append("finnhub:no_api_key")
                    continue
                q = await self._fetch_finnhub_quote(symbol, api_key)
                if q.current_price is not None:
                    try:
                        prof, met = await self._fetch_finnhub_profile_and_metrics(symbol, api_key)
                        upd: Dict[str, Any] = {}
                        upd.update({k: v for k, v in prof.items() if v is not None})
                        upd.update({k: v for k, v in met.items() if v is not None})
                        q = q.model_copy(update=upd)
                    except Exception:
                        pass
                    q.data_source = "finnhub"
                    q.market = "GLOBAL"
                    q.currency = q.currency or "USD"
                    q.data_quality = "FULL" if q.previous_close is not None else "PARTIAL"
                    return q.finalize()
                errors.append(f"finnhub:{q.error or 'no_price'}")

            elif p == "fmp":
                api_key = _get_key("fmp_api_key", "FMP_API_KEY")
                if not api_key:
                    errors.append("fmp:no_api_key")
                    continue
                q = await self._fetch_fmp_quote(symbol, api_key)
                if q.current_price is not None:
                    q.data_source = "fmp"
                    q.market = "GLOBAL"
                    q.currency = q.currency or "USD"
                    q.data_quality = "FULL" if q.previous_close is not None else "PARTIAL"
                    return q.finalize()
                errors.append(f"fmp:{q.error or 'no_price'}")

            elif p in {"yfinance", "yahoo"}:
                if self.enable_yfinance and yf:
                    q = await self._fetch_yfinance(symbol, market="GLOBAL")
                    if q.current_price is not None:
                        q.data_source = "yfinance"
                        q.data_quality = "PARTIAL"
                        return q.finalize()
                    errors.append(f"yfinance:{q.error or 'no_price'}")
                else:
                    errors.append("yfinance:disabled_or_missing")

        if self.enable_yfinance and yf:
            q = await self._fetch_yfinance(symbol, market="GLOBAL")
            if q.current_price is not None:
                q.data_source = "yfinance"
                q.data_quality = "PARTIAL"
                return q.finalize()
            errors.append(f"yfinance:{q.error or 'no_price'}")

        return UnifiedQuote(
            symbol=symbol,
            market="GLOBAL",
            currency="USD",
            data_quality="MISSING",
            error="No provider data. " + (" | ".join(errors) if errors else "no_providers_enabled"),
        ).finalize()

    # -------------------------------------------------------------------------
    # HTTP helpers (with light retry)
    # -------------------------------------------------------------------------
    async def _http_get_text(self, url: str, params: Optional[Dict[str, Any]] = None) -> Optional[str]:
        for attempt in range(3):
            try:
                r = await self.client.get(url, params=params)
                if r.status_code in (429,) or 500 <= r.status_code < 600:
                    if attempt < 2:
                        await asyncio.sleep(0.25 + random.random() * 0.35)
                        continue
                    return None
                if r.status_code >= 400:
                    return None
                return r.text
            except Exception:
                if attempt < 2:
                    await asyncio.sleep(0.25 + random.random() * 0.35)
                    continue
                return None
        return None

    async def _http_get_json(
        self, url: str, params: Optional[Dict[str, Any]] = None
    ) -> Optional[Union[Dict[str, Any], List[Any]]]:
        for attempt in range(3):
            try:
                r = await self.client.get(url, params=params)
                if r.status_code in (429,) or 500 <= r.status_code < 600:
                    if attempt < 2:
                        await asyncio.sleep(0.25 + random.random() * 0.35)
                        continue
                    return None
                if r.status_code >= 400:
                    return None
                try:
                    return r.json()
                except Exception:
                    return None
            except Exception:
                if attempt < 2:
                    await asyncio.sleep(0.25 + random.random() * 0.35)
                    continue
                return None
        return None

    # -------------------------------------------------------------------------
    # Provider: EODHD
    # -------------------------------------------------------------------------
    async def _fetch_eodhd_realtime(self, symbol: str, api_key: str) -> UnifiedQuote:
        url = EODHD_RT_URL.format(symbol=symbol)
        params = {"api_token": api_key, "fmt": "json"}
        t0 = time.perf_counter()
        try:
            data = await self._http_get_json(url, params=params)
            if not data or not isinstance(data, dict):
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

    async def _fetch_eodhd_fundamentals(self, symbol: str, api_key: str) -> Dict[str, Any]:
        cache_key = f"eodhd_fund::{symbol}"
        hit = self.fund_cache.get(cache_key)
        if isinstance(hit, dict):
            return hit

        url = EODHD_FUND_URL.format(symbol=symbol)
        params = {"api_token": api_key, "fmt": "json"}
        data = await self._http_get_json(url, params=params)
        if not data or not isinstance(data, dict):
            self.fund_cache[cache_key] = {}
            return {}

        general = data.get("General") or {}
        highlights = data.get("Highlights") or {}
        valuation = data.get("Valuation") or {}
        technicals = data.get("Technicals") or {}
        shares = data.get("SharesStats") or {}

        out = {
            "name": _safe_str(general.get("Name")),
            "sector": _safe_str(general.get("Sector")),
            "industry": _safe_str(general.get("Industry")),
            "listing_date": _safe_str(general.get("IPODate") or general.get("ListingDate")),
            "market_cap": _safe_float(highlights.get("MarketCapitalization") or general.get("MarketCapitalization")),
            "shares_outstanding": _safe_float(shares.get("SharesOutstanding") or general.get("SharesOutstanding")),
            "eps_ttm": _safe_float(highlights.get("EarningsShare")),
            "pe_ttm": _safe_float(highlights.get("PERatio")),
            "pb": _safe_float(valuation.get("PriceBookMRQ") or highlights.get("PriceToBookMRQ")),
            "ps": _safe_float(valuation.get("PriceSalesTTM") or highlights.get("PriceToSalesTTM")),
            "dividend_yield": _pct_if_fraction(highlights.get("DividendYield")),
            "beta": _safe_float(highlights.get("Beta")),
            "net_margin": _pct_if_fraction(highlights.get("ProfitMargin")),
            "roe": _pct_if_fraction(highlights.get("ReturnOnEquityTTM")),
            "roa": _pct_if_fraction(highlights.get("ReturnOnAssetsTTM")),
            "high_52w": _safe_float(technicals.get("52WeekHigh")),
            "low_52w": _safe_float(technicals.get("52WeekLow")),
            "ma50": _safe_float(technicals.get("50DayMA")),
            "ma20": _safe_float(technicals.get("20DayMA")) or _safe_float(technicals.get("200DayMA")),
        }

        self.fund_cache[cache_key] = out
        return out

    # -------------------------------------------------------------------------
    # Provider: Finnhub
    # -------------------------------------------------------------------------
    async def _fetch_finnhub_quote(self, symbol: str, api_key: str) -> UnifiedQuote:
        sym = _finnhub_symbol(symbol)
        params = {"symbol": sym, "token": api_key}
        try:
            data = await self._http_get_json(FINNHUB_QUOTE_URL, params=params)
            if not data or not isinstance(data, dict):
                return UnifiedQuote(symbol=symbol, market="GLOBAL", currency="USD", error="Finnhub empty response")

            cp = _safe_float(data.get("c"))
            pc = _safe_float(data.get("pc"))

            return UnifiedQuote(
                symbol=symbol,
                market="GLOBAL",
                currency="USD",
                current_price=cp,
                previous_close=pc,
                open=_safe_float(data.get("o")),
                day_high=_safe_float(data.get("h")),
                day_low=_safe_float(data.get("l")),
                price_change=_safe_float(data.get("d")),
                percent_change=_safe_float(data.get("dp")),
                data_source="finnhub",
                data_quality="PARTIAL" if cp is not None else "MISSING",
            )
        except Exception as exc:
            return UnifiedQuote(symbol=symbol, market="GLOBAL", currency="USD", error=f"Finnhub error: {exc}")

    async def _fetch_finnhub_profile_and_metrics(self, symbol: str, api_key: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        sym = _finnhub_symbol(symbol)
        cache_key = f"finnhub_profile_metrics::{sym}"
        hit = self.fund_cache.get(cache_key)
        if isinstance(hit, dict) and hit.get("_combined") is True:
            return hit.get("profile", {}) or {}, hit.get("metrics", {}) or {}

        prof_params = {"symbol": sym, "token": api_key}
        met_params = {"symbol": sym, "metric": "all", "token": api_key}

        prof_data, met_data = await asyncio.gather(
            self._http_get_json(FINNHUB_PROFILE_URL, params=prof_params),
            self._http_get_json(FINNHUB_METRIC_URL, params=met_params),
            return_exceptions=True,
        )

        profile: Dict[str, Any] = {}
        metrics: Dict[str, Any] = {}

        if isinstance(prof_data, dict) and prof_data:
            profile = {
                "name": _safe_str(prof_data.get("name")),
                "currency": _safe_str(prof_data.get("currency")),
                "industry": _safe_str(prof_data.get("finnhubIndustry")),
                "listing_date": _safe_str(prof_data.get("ipo")),
                "market_cap": _safe_float(prof_data.get("marketCapitalization")),
                "shares_outstanding": _safe_float(prof_data.get("shareOutstanding")),
            }

        if isinstance(met_data, dict) and met_data:
            m = met_data.get("metric") or {}
            if isinstance(m, dict):
                metrics = {
                    "high_52w": _safe_float(m.get("52WeekHigh")),
                    "low_52w": _safe_float(m.get("52WeekLow")),
                    "pe_ttm": _safe_float(m.get("peTTM") or m.get("peAnnual")),
                    "pb": _safe_float(m.get("pbAnnual") or m.get("pbQuarterly")),
                    "ps": _safe_float(m.get("psTTM") or m.get("psAnnual")),
                    "eps_ttm": _safe_float(m.get("epsTTM")),
                    "beta": _safe_float(m.get("beta")),
                    "dividend_yield": _pct_if_fraction(
                        m.get("dividendYieldIndicatedAnnual") or m.get("dividendYieldAnnual")
                    ),
                    "roe": _pct_if_fraction(m.get("roeTTM") or m.get("roeAnnual")),
                    "roa": _pct_if_fraction(m.get("roaTTM") or m.get("roaAnnual")),
                    "net_margin": _pct_if_fraction(m.get("netMarginTTM") or m.get("netMarginAnnual")),
                    "debt_to_equity": _safe_float(
                        m.get("totalDebt/totalEquityAnnual") or m.get("totalDebt/totalEquityQuarterly")
                    ),
                    "current_ratio": _safe_float(m.get("currentRatioAnnual") or m.get("currentRatioQuarterly")),
                    "quick_ratio": _safe_float(m.get("quickRatioAnnual") or m.get("quickRatioQuarterly")),
                }

        self.fund_cache[cache_key] = {"_combined": True, "profile": profile, "metrics": metrics}
        return profile, metrics

    # -------------------------------------------------------------------------
    # Provider: FMP
    # -------------------------------------------------------------------------
    async def _fetch_fmp_quote(self, symbol: str, api_key: str) -> UnifiedQuote:
        sym = _fmp_symbol(symbol)
        url = FMP_QUOTE_URL.format(symbol=sym)
        params = {"apikey": api_key}
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
            return UnifiedQuote(symbol=symbol, market=market, error="yfinance not installed", data_quality="MISSING")

        ysym = _yahoo_symbol(symbol)

        def _sync() -> Dict[str, Any]:
            t = yf.Ticker(ysym)
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
                if hist is None or getattr(hist, "empty", True):
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
                return UnifiedQuote(symbol=symbol, market=market, error="yfinance empty response", data_quality="MISSING")

            err_txt = str(info)[:600].lower()
            if "unauthorized" in err_txt or "unable to access this feature" in err_txt or "401" in err_txt:
                return UnifiedQuote(
                    symbol=symbol,
                    market=market,
                    error="yfinance blocked/Unauthorized (Yahoo 401). Disable ENABLE_YFINANCE or use other providers.",
                    data_quality="MISSING",
                )

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
            msg = str(exc)
            if "unauthorized" in msg.lower() or "401" in msg:
                msg = "yfinance blocked/Unauthorized (Yahoo 401)."
            return UnifiedQuote(symbol=symbol, market=market, error=f"yfinance error: {msg}", data_quality="MISSING")


__all__ = ["UnifiedQuote", "DataEngine", "normalize_symbol", "is_ksa_symbol", "ENGINE_VERSION"]
