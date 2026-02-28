#!/usr/bin/env python3
# core/schemas.py
"""
================================================================================
Core Schemas + Sheet Headers — v5.2.0 (STABLE / GLOBAL-FIRST / BEST-PRACTICE)
================================================================================

This revision focuses on correctness + maintainability + runtime safety.

Key improvements vs your pasted draft
- ✅ FIX: Recommendation enum now includes STRONG_BUY / STRONG_SELL (your validators referenced them).
- ✅ FIX: Symbol validation no longer strips suffixes like .SR (KSA must remain intact).
- ✅ FIX: Header mapping logic corrected:
     - Previously normalize_header() was used, but HEADER_TO_FIELD keys were NOT normalized → misses.
     - Now we build normalized maps at import time (fast O(1) lookups).
- ✅ FIX: DEFAULT_HEADERS_59 naming was incorrect (it contained more than 59 columns).
     - Now we provide clear, explicit canonical header sets:
       * ENRICHED_HEADERS_61  (matches your router output)
       * VNEXT_SCHEMAS        (vNext group-based schemas)
- ✅ Added: resolve_sheet_key(), validate_sheet_data() compatibility helpers.
- ✅ Added: defensive, lightweight coercion for percent strings and numeric strings (Pydantic v2/v1 safe).
- ✅ Reduced duplication and enforced deterministic schema registry behavior.
- ✅ All helpers are pure and safe (no network, no heavy imports).

Design Principles
- Keep schemas *strict enough* to prevent crashes, but *flexible enough* to carry extra fields.
- Use `extra="allow"` so we don’t silently drop advanced provider fields.
- Keep all schema/headers logic centralized here.

================================================================================
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
from datetime import date, datetime
from enum import Enum
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

# ---------------------------------------------------------------------------
# Fast JSON (optional orjson)
# ---------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_dumps(v: Any, *, default: Any = str) -> str:
        return orjson.dumps(v, default=default).decode("utf-8")

    def json_loads(v: Union[str, bytes]) -> Any:
        if isinstance(v, str):
            v = v.encode("utf-8")
        return orjson.loads(v)

    _HAS_ORJSON = True
except Exception:
    def json_dumps(v: Any, *, default: Any = str) -> str:
        return json.dumps(v, default=default, ensure_ascii=False)

    def json_loads(v: Union[str, bytes]) -> Any:
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", errors="replace")
        return json.loads(v)

    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Pydantic (v2 preferred, v1 compatible)
# ---------------------------------------------------------------------------
try:
    from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator  # type: ignore
    from pydantic import ValidationError  # type: ignore

    _PYDANTIC_V2 = True
except Exception:
    from pydantic import BaseModel, Field, ValidationError  # type: ignore
    from pydantic import validator, root_validator  # type: ignore

    ConfigDict = None  # type: ignore
    field_validator = None  # type: ignore
    model_validator = None  # type: ignore
    _PYDANTIC_V2 = False


SCHEMAS_VERSION = "5.2.0"

_LOCK = threading.RLock()

# =============================================================================
# Enums
# =============================================================================

class MarketType(str, Enum):
    KSA = "KSA"
    UAE = "UAE"
    QATAR = "QATAR"
    KUWAIT = "KUWAIT"
    US = "US"
    GLOBAL = "GLOBAL"
    COMMODITY = "COMMODITY"
    FOREX = "FOREX"
    CRYPTO = "CRYPTO"
    FUND = "FUND"


class AssetClass(str, Enum):
    EQUITY = "EQUITY"
    ETF = "ETF"
    MUTUAL_FUND = "MUTUAL_FUND"
    COMMODITY = "COMMODITY"
    CURRENCY = "CURRENCY"
    CRYPTOCURRENCY = "CRYPTOCURRENCY"
    BOND = "BOND"
    REAL_ESTATE = "REAL_ESTATE"
    DERIVATIVE = "DERIVATIVE"
    UNKNOWN = "UNKNOWN"


class Recommendation(str, Enum):
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    SELL = "SELL"
    STRONG_SELL = "STRONG_SELL"

    @classmethod
    def from_score(cls, score: float, scale: str = "0-100") -> "Recommendation":
        """
        Default scale: 0-100 (higher is better).
        """
        try:
            s = float(score)
        except Exception:
            return cls.HOLD

        if scale == "0-100":
            if s >= 85:
                return cls.STRONG_BUY
            if s >= 70:
                return cls.BUY
            if s >= 45:
                return cls.HOLD
            if s >= 30:
                return cls.REDUCE
            if s >= 15:
                return cls.SELL
            return cls.STRONG_SELL

        # backward compatibility
        if scale == "1-5":
            # 1 best, 5 worst
            if s <= 1.5:
                return cls.STRONG_BUY
            if s <= 2.5:
                return cls.BUY
            if s <= 3.5:
                return cls.HOLD
            if s <= 4.5:
                return cls.SELL
            return cls.STRONG_SELL

        return cls.HOLD


class DataQuality(str, Enum):
    EXCELLENT = "EXCELLENT"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    POOR = "POOR"
    STALE = "STALE"
    MISSING = "MISSING"
    ERROR = "ERROR"


class BadgeLevel(str, Enum):
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    NEUTRAL = "NEUTRAL"
    CAUTION = "CAUTION"
    DANGER = "DANGER"
    NONE = "NONE"


# =============================================================================
# Safe parsing helpers
# =============================================================================

_NUM_RE = re.compile(r"^[\s\-\+]*[\d\.,]+(?:[eE][\+\-]?\d+)?\s*%?\s*$")


def safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    try:
        return str(value).strip()
    except Exception:
        return default


def safe_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = safe_str(value).upper()
    if s in {"TRUE", "YES", "Y", "1", "ON", "ENABLED"}:
        return True
    if s in {"FALSE", "NO", "N", "0", "OFF", "DISABLED"}:
        return False
    return default


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return default

    s = safe_str(value)
    if not s:
        return default

    # allow trailing %
    pct = s.endswith("%")
    if pct:
        s = s[:-1].strip()

    # remove non-numeric currency separators safely (keep digits, dot, comma, sign, e/E)
    if not _NUM_RE.match(s):
        s = re.sub(r"[^\d\.,\-\+eE]", "", s)

    if not s:
        return default

    # locale handling: if both ',' and '.', choose last as decimal separator
    if "," in s and "." in s:
        if s.rindex(",") > s.rindex("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")

    try:
        f = float(s)
    except Exception:
        return default

    # if input had %, we do NOT auto-scale here (caller decides)
    return f


def safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    f = safe_float(value, None)
    if f is None:
        return default
    try:
        return int(round(f))
    except Exception:
        return default


def safe_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    s = safe_str(value)
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None


def safe_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            return datetime.fromtimestamp(float(value))
        except Exception:
            return None
    s = safe_str(value)
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


def bound_value(value: Optional[float], min_val: float, max_val: float, default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    return max(min_val, min(max_val, value))


def decimal_to_percent(value: Optional[float]) -> Optional[float]:
    """
    If value looks like decimal (<= 1), convert to percent.
    If already a percent (e.g. 12.3), return as-is.
    """
    if value is None:
        return None
    return value * 100.0 if -1.0 <= value <= 1.0 else value


def percent_to_decimal(value: Optional[float]) -> Optional[float]:
    """
    If value looks like percent (> 1), convert to decimal.
    If already decimal (<= 1), return as-is.
    """
    if value is None:
        return None
    return value / 100.0 if abs(value) > 1.0 else value


# =============================================================================
# UnifiedQuote (practical + extensible)
# =============================================================================

class UnifiedQuote(BaseModel):
    """
    UnifiedQuote is intentionally:
    - Flexible: extra fields allowed (do not drop provider/enrichment fields)
    - Safe: validators prevent common crashes
    - Practical: contains required fields used by engine/router
    """

    # -----------------------------------------------------------------
    # Identity / meta
    # -----------------------------------------------------------------
    symbol: str = Field(..., description="Trading symbol (as requested or canonical output)")
    symbol_normalized: Optional[str] = Field(None, description="Normalized symbol (canonical output)")
    requested_symbol: Optional[str] = Field(None, description="Original user request symbol")

    origin: Optional[str] = Field(None, description="Origin label (KSA_TADAWUL / GLOBAL_MARKETS / etc)")
    name: Optional[str] = Field(None, description="Company/asset name")
    exchange: Optional[str] = Field(None, description="Primary exchange")
    market: Optional[MarketType] = Field(None, description="Market type")
    asset_class: Optional[AssetClass] = Field(None, description="Asset class")
    currency: Optional[str] = Field(None, description="Currency code", max_length=3)
    sector: Optional[str] = Field(None, description="Sector")
    sub_sector: Optional[str] = Field(None, description="Sub sector / industry")
    industry: Optional[str] = Field(None, description="Industry")
    listing_date: Optional[date] = Field(None, description="Listing date / IPO date")

    # -----------------------------------------------------------------
    # Prices
    # -----------------------------------------------------------------
    price: Optional[float] = Field(None, description="Price (alias of current_price)")
    current_price: Optional[float] = Field(None, description="Current price")
    previous_close: Optional[float] = Field(None, description="Previous close")
    change: Optional[float] = Field(None, description="Price change (preferred field name for sheets)")
    change_pct: Optional[float] = Field(None, description="Change percent (preferred field name for sheets)")

    day_open: Optional[float] = Field(None, description="Open")
    day_high: Optional[float] = Field(None, description="High")
    day_low: Optional[float] = Field(None, description="Low")

    week_52_high: Optional[float] = Field(None, description="52W high")
    week_52_low: Optional[float] = Field(None, description="52W low")
    week_52_position_pct: Optional[float] = Field(None, description="52W position percent (0-100)")

    # -----------------------------------------------------------------
    # Volume / liquidity
    # -----------------------------------------------------------------
    volume: Optional[float] = Field(None, description="Volume")
    avg_vol_30d: Optional[float] = Field(None, description="Average volume 30D")
    value_traded: Optional[float] = Field(None, description="Value traded")
    turnover_pct: Optional[float] = Field(None, description="Turnover percent")
    shares_outstanding: Optional[float] = Field(None, description="Shares outstanding")
    free_float: Optional[float] = Field(None, description="Free float percent (0-100 or 0-1 depending source)")
    market_cap: Optional[float] = Field(None, description="Market cap")
    free_float_market_cap: Optional[float] = Field(None, description="Free float market cap")
    liquidity_score: Optional[float] = Field(None, description="Liquidity score 0-100")

    # -----------------------------------------------------------------
    # Fundamentals
    # -----------------------------------------------------------------
    eps_ttm: Optional[float] = Field(None, description="EPS TTM")
    forward_eps: Optional[float] = Field(None, description="Forward EPS")
    pe_ttm: Optional[float] = Field(None, description="P/E TTM")
    forward_pe: Optional[float] = Field(None, description="Forward P/E")
    pb: Optional[float] = Field(None, description="P/B")
    ps: Optional[float] = Field(None, description="P/S")
    ev_ebitda: Optional[float] = Field(None, description="EV/EBITDA")

    dividend_yield: Optional[float] = Field(None, description="Dividend yield (percent)")
    dividend_rate: Optional[float] = Field(None, description="Dividend rate")
    payout_ratio: Optional[float] = Field(None, description="Payout ratio (percent)")
    beta: Optional[float] = Field(None, description="Beta")

    roe: Optional[float] = Field(None, description="ROE (percent)")
    roa: Optional[float] = Field(None, description="ROA (percent)")
    net_margin: Optional[float] = Field(None, description="Net margin (percent)")
    ebitda_margin: Optional[float] = Field(None, description="EBITDA margin (percent)")
    revenue_growth: Optional[float] = Field(None, description="Revenue growth (percent)")
    net_income_growth: Optional[float] = Field(None, description="Net income growth (percent)")

    # -----------------------------------------------------------------
    # Technicals / forecast
    # -----------------------------------------------------------------
    volatility_30d: Optional[float] = Field(None, description="Volatility 30D (percent)")
    rsi_14: Optional[float] = Field(None, description="RSI 14")

    fair_value: Optional[float] = Field(None, description="Fair value / target")
    upside_pct: Optional[float] = Field(None, description="Upside percent")
    valuation_label: Optional[str] = Field(None, description="Valuation label")

    value_score: Optional[float] = Field(None, description="Value score")
    quality_score: Optional[float] = Field(None, description="Quality score")
    momentum_score: Optional[float] = Field(None, description="Momentum score")
    opportunity_score: Optional[float] = Field(None, description="Opportunity score")
    risk_score: Optional[float] = Field(None, description="Risk score")
    overall_score: Optional[float] = Field(None, description="Overall score")

    recommendation: Optional[Recommendation] = Field(None, description="Recommendation enum")
    recommendation_raw: Optional[str] = Field(None, description="Raw recommendation text")

    # -----------------------------------------------------------------
    # Quality / debugging
    # -----------------------------------------------------------------
    data_source: Optional[str] = Field(None, description="Single best data source label")
    data_sources: Optional[List[str]] = Field(None, description="Sources list")
    provider_latency: Optional[Dict[str, float]] = Field(None, description="Provider latency map")
    data_quality: Optional[DataQuality] = Field(None, description="Data quality enum")
    error: Optional[str] = Field(None, description="Error")
    warning: Optional[str] = Field(None, description="Warning")
    info: Optional[Any] = Field(None, description="Info payload (string/dict)")
    latency_ms: Optional[float] = Field(None, description="Router/engine latency")
    last_updated_utc: Optional[datetime] = Field(None, description="UTC timestamp")
    last_updated_riyadh: Optional[datetime] = Field(None, description="Riyadh timestamp")

    # -----------------------------------------------------------------
    # Pydantic config
    # -----------------------------------------------------------------
    if _PYDANTIC_V2:
        model_config = ConfigDict(
            extra="allow",              # keep unknown fields
            validate_assignment=True,
            arbitrary_types_allowed=True,
        )

        @model_validator(mode="before")
        @classmethod
        def _pre_coerce(cls, data: Any) -> Any:
            # Coerce percent strings safely: "12.3%" => 12.3
            if isinstance(data, dict):
                for k, v in list(data.items()):
                    if isinstance(v, str) and v.strip().endswith("%"):
                        data[k] = safe_float(v)
            return data

        @field_validator("symbol", mode="before")
        @classmethod
        def _validate_symbol(cls, v: Any) -> str:
            s = safe_str(v)
            if not s:
                raise ValueError("symbol is required")
            # DO NOT strip .SR or other suffixes (router/normalizer handles that)
            return s.upper()

        @field_validator("currency", mode="before")
        @classmethod
        def _validate_currency(cls, v: Any) -> Optional[str]:
            if v is None:
                return None
            s = safe_str(v).upper()
            if len(s) == 3:
                return s
            # best-effort mapping
            mapping = {
                "US DOLLAR": "USD",
                "DOLLAR": "USD",
                "EURO": "EUR",
                "POUND": "GBP",
                "SAUDI RIYAL": "SAR",
                "RIYAL": "SAR",
                "DIRHAM": "AED",
                "QATAR RIYAL": "QAR",
                "KUWAITI DINAR": "KWD",
            }
            return mapping.get(s, None)

        @field_validator(
            "week_52_position_pct",
            "free_float",
            "turnover_pct",
            "dividend_yield",
            "payout_ratio",
            "roe",
            "roa",
            "net_margin",
            "ebitda_margin",
            "revenue_growth",
            "net_income_growth",
            "volatility_30d",
            "change_pct",
            "upside_pct",
            mode="before",
            check_fields=False,
        )
        @classmethod
        def _percent_fields_smart(cls, v: Any) -> Any:
            f = safe_float(v, None)
            # Keep as-is (router converts to sheet percent). We only bound obvious 0-100 values.
            if f is None:
                return None
            # if it looks like decimal, convert to percent for canonical storage
            return decimal_to_percent(f)

        @field_validator("listing_date", mode="before", check_fields=False)
        @classmethod
        def _coerce_date(cls, v: Any) -> Any:
            return safe_date(v)

        @field_validator("last_updated_utc", "last_updated_riyadh", mode="before", check_fields=False)
        @classmethod
        def _coerce_dt(cls, v: Any) -> Any:
            return safe_datetime(v)

        @model_validator(mode="after")
        def _post_fixups(self) -> "UnifiedQuote":
            # unify price/current_price
            if self.price is not None and self.current_price is None:
                self.current_price = self.price
            if self.current_price is not None and self.price is None:
                self.price = self.current_price

            # derive change/change_pct from price and prev close if missing
            if self.change is None and self.current_price is not None and self.previous_close is not None:
                try:
                    self.change = float(self.current_price) - float(self.previous_close)
                except Exception:
                    pass
            if self.change_pct is None and self.current_price is not None and self.previous_close not in (None, 0):
                try:
                    self.change_pct = (float(self.current_price) / float(self.previous_close) - 1.0) * 100.0
                except Exception:
                    pass

            # normalize recommendation
            if self.recommendation is None and self.recommendation_raw:
                self.recommendation = normalize_recommendation(self.recommendation_raw)

            # normalize symbol_normalized
            if self.symbol and not self.symbol_normalized:
                self.symbol_normalized = self.symbol.upper()

            return self

    else:
        class Config:
            extra = "allow"
            validate_assignment = True

        @validator("symbol", pre=True, always=True)
        def _v1_symbol(cls, v: Any) -> str:
            s = safe_str(v)
            if not s:
                raise ValueError("symbol is required")
            return s.upper()

        @root_validator(pre=False)
        def _v1_post(cls, values: Dict[str, Any]) -> Dict[str, Any]:
            price = values.get("price")
            current_price = values.get("current_price")
            if price is not None and current_price is None:
                values["current_price"] = price
            if current_price is not None and price is None:
                values["price"] = current_price
            if values.get("symbol") and not values.get("symbol_normalized"):
                values["symbol_normalized"] = str(values["symbol"]).upper()
            return values

    # -----------------------------------------------------------------
    # Utility Methods
    # -----------------------------------------------------------------
    def to_dict(self, exclude_none: bool = True) -> Dict[str, Any]:
        if _PYDANTIC_V2:
            return self.model_dump(exclude_none=exclude_none)
        return self.dict(exclude_none=exclude_none)

    def to_json(self) -> str:
        if _HAS_ORJSON:
            return json_dumps(self.to_dict(exclude_none=True), default=str)
        if _PYDANTIC_V2:
            return self.model_dump_json(exclude_none=True)
        return self.json(exclude_none=True)

    @classmethod
    def from_dict(cls, data: Dict[str, Any], strict: bool = False) -> Optional["UnifiedQuote"]:
        try:
            return cls(**(data or {}))
        except ValidationError:
            if strict:
                raise
            return None
        except Exception:
            return None

    def compute_hash(self) -> str:
        payload = self.to_dict(exclude_none=True)
        s = json_dumps(payload, default=str) if _HAS_ORJSON else json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


# =============================================================================
# Recommendation normalization helper
# =============================================================================

_RECOMMENDATION_ALIASES = {
    "STRONG BUY": Recommendation.STRONG_BUY,
    "STRONG_BUY": Recommendation.STRONG_BUY,
    "BUY": Recommendation.BUY,
    "ACCUMULATE": Recommendation.BUY,
    "ADD": Recommendation.BUY,
    "OUTPERFORM": Recommendation.BUY,
    "OVERWEIGHT": Recommendation.BUY,
    "HOLD": Recommendation.HOLD,
    "NEUTRAL": Recommendation.HOLD,
    "MAINTAIN": Recommendation.HOLD,
    "MARKET PERFORM": Recommendation.HOLD,
    "EQUAL WEIGHT": Recommendation.HOLD,
    "REDUCE": Recommendation.REDUCE,
    "TRIM": Recommendation.REDUCE,
    "UNDERWEIGHT": Recommendation.REDUCE,
    "SELL": Recommendation.SELL,
    "EXIT": Recommendation.SELL,
    "AVOID": Recommendation.SELL,
    "STRONG SELL": Recommendation.STRONG_SELL,
    "STRONG_SELL": Recommendation.STRONG_SELL,
}


def normalize_recommendation(v: Any) -> Optional[Recommendation]:
    if v is None:
        return None
    if isinstance(v, Recommendation):
        return v
    s = safe_str(v).upper()
    if not s:
        return None
    s = re.sub(r"[\s\-_]+", " ", s).strip()
    return _RECOMMENDATION_ALIASES.get(s, Recommendation.HOLD)


# =============================================================================
# Canonical header sets (ALIGNED to router output)
# =============================================================================

# This matches core/enriched_quote.py output (your router now returns 61 columns)
ENRICHED_HEADERS_61: List[str] = [
    "Rank", "Symbol", "Origin", "Name", "Sector", "Sub Sector", "Market",
    "Currency", "Listing Date", "Price", "Prev Close", "Change", "Change %", "Day High", "Day Low",
    "52W High", "52W Low", "52W Position %",
    "Volume", "Avg Vol 30D", "Value Traded", "Turnover %", "Shares Outstanding", "Free Float %", "Market Cap", "Free Float Mkt Cap",
    "Liquidity Score", "EPS (TTM)", "Forward EPS", "P/E (TTM)", "Forward P/E", "P/B", "P/S", "EV/EBITDA",
    "Dividend Yield", "Dividend Rate", "Payout Ratio", "Beta",
    "ROE", "ROA", "Net Margin", "EBITDA Margin", "Revenue Growth", "Net Income Growth", "Volatility 30D", "RSI 14",
    "Fair Value", "Upside %", "Valuation Label", "Value Score", "Quality Score", "Momentum Score", "Opportunity Score", "Risk Score",
    "Overall Score", "Error", "Recommendation", "Data Source", "Data Quality",
    "Last Updated (UTC)", "Last Updated (Riyadh)",
]

# Backward alias name (some modules still refer to 59)
DEFAULT_HEADERS_59 = ENRICHED_HEADERS_61
DEFAULT_HEADERS_ANALYSIS = ENRICHED_HEADERS_61  # keep stable: analysis extras should be in vNext schemas


# =============================================================================
# vNext schema groups (kept, but cleaned + deduped)
# =============================================================================

def _dedupe(headers: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for h in headers:
        k = normalize_header(h)
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(h)
    return out


VN_IDENTITY: List[str] = _dedupe([
    "Rank", "Symbol", "Origin", "Name", "Sector", "Sub Sector", "Market",
    "Currency", "Listing Date", "Asset Class", "Exchange", "Country",
])

VN_PRICE: List[str] = _dedupe([
    "Price", "Prev Close", "Change", "Change %", "Day Open", "Day High", "Day Low", "Day VWAP",
    "52W High", "52W Low", "52W Position %",
])

VN_VOLUME: List[str] = _dedupe([
    "Volume", "Avg Vol 10D", "Avg Vol 30D", "Avg Vol 90D", "Value Traded",
    "Turnover %", "Rel Volume", "Bid", "Ask", "Spread", "Spread %",
])

VN_CAP: List[str] = _dedupe([
    "Shares Outstanding", "Free Float %", "Free Float Shares", "Market Cap",
    "Free Float Mkt Cap", "Enterprise Value", "Liquidity Score",
])

VN_FUNDAMENTALS: List[str] = _dedupe([
    "EPS (TTM)", "Forward EPS", "P/E (TTM)", "Forward P/E", "P/B", "P/S", "EV/EBITDA",
    "Dividend Yield", "Dividend Rate", "Payout Ratio", "Beta",
    "ROE", "ROA", "Net Margin", "EBITDA Margin", "Revenue Growth", "Net Income Growth",
])

VN_TECHNICALS: List[str] = _dedupe([
    "Volatility 30D", "RSI 14", "MA20", "MA50", "MA200", "VWAP", "SuperTrend",
])

VN_FORECAST: List[str] = _dedupe([
    "Fair Value", "Upside %", "Valuation Label",
    "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M",
    "Expected Return 1M %", "Expected Return 3M %", "Expected Return 12M %",
    "Forecast Confidence", "Forecast Method", "Forecast Source",
])

VN_SCORES: List[str] = _dedupe([
    "Value Score", "Quality Score", "Momentum Score", "Opportunity Score", "Risk Score", "Overall Score",
    "Recommendation",
])

VN_META: List[str] = _dedupe([
    "Error", "Warning", "Info", "Data Source", "Data Quality",
    "Last Updated (UTC)", "Last Updated (Riyadh)", "Latency ms", "Request ID",
])

# vNext sheet schemas (global-first)
VN_HEADERS_GLOBAL: List[str] = _dedupe(VN_IDENTITY + VN_PRICE + VN_VOLUME + VN_CAP + VN_FUNDAMENTALS + VN_TECHNICALS + VN_FORECAST + VN_SCORES + VN_META)
VN_HEADERS_KSA_TADAWUL: List[str] = _dedupe(VN_IDENTITY + VN_PRICE + VN_VOLUME[:6] + VN_CAP + VN_FUNDAMENTALS + VN_FORECAST[:6] + VN_SCORES + VN_META)
VN_HEADERS_INSIGHTS: List[str] = VN_HEADERS_GLOBAL

VNEXT_SCHEMAS: Dict[str, Tuple[str, ...]] = {
    "KSA_TADAWUL": tuple(VN_HEADERS_KSA_TADAWUL),
    "GLOBAL_MARKETS": tuple(VN_HEADERS_GLOBAL),
    "INSIGHTS_ANALYSIS": tuple(VN_HEADERS_INSIGHTS),
    # others can be added later:
    "MY_PORTFOLIO": tuple(VN_HEADERS_GLOBAL),
    "MUTUAL_FUNDS": tuple(VN_HEADERS_GLOBAL),
    "COMMODITIES_FX": tuple(VN_HEADERS_GLOBAL),
}

LEGACY_SCHEMAS: Dict[str, Tuple[str, ...]] = {
    "KSA_TADAWUL": tuple(ENRICHED_HEADERS_61),
    "GLOBAL_MARKETS": tuple(ENRICHED_HEADERS_61),
    "INSIGHTS_ANALYSIS": tuple(ENRICHED_HEADERS_61),
    "MY_PORTFOLIO": tuple(ENRICHED_HEADERS_61),
    "MUTUAL_FUNDS": tuple(ENRICHED_HEADERS_61),
    "COMMODITIES_FX": tuple(ENRICHED_HEADERS_61),
}

# =============================================================================
# Header normalization and mapping
# =============================================================================

@lru_cache(maxsize=4096)
def normalize_header(header: str) -> str:
    """
    Normalize header label for matching.
    - Lowercase
    - Remove punctuation
    - Convert % to 'percent'
    - Collapse whitespace to underscore
    """
    s = safe_str(header).lower()
    if not s:
        return ""
    s = re.sub(r"[^\w\s%]", " ", s)
    s = s.replace("%", " percent ")
    s = re.sub(r"\s+", " ", s).strip()

    # common abbreviations (minimal)
    replacements = {
        "ttm": "ttm",
        "avg": "average",
        "vol": "volume",
        "mkt": "market",
        "div": "dividend",
        "eps": "eps",
        "pe": "pe",
        "pb": "pb",
        "ps": "ps",
        "roe": "roe",
        "roa": "roa",
        "ev": "ev",
        "ebitda": "ebitda",
        "vwap": "vwap",
        "rsi": "rsi",
    }
    parts = []
    for p in s.split(" "):
        parts.append(replacements.get(p, p))
    return "_".join(parts)


# Canonical header->field map (RAW labels)
HEADER_TO_FIELD_RAW: Dict[str, str] = {
    # identity
    "Rank": "rank",
    "Symbol": "symbol",
    "Origin": "origin",
    "Name": "name",
    "Sector": "sector",
    "Sub Sector": "sub_sector",
    "Market": "market",
    "Currency": "currency",
    "Listing Date": "listing_date",
    # prices
    "Price": "current_price",
    "Prev Close": "previous_close",
    "Change": "change",
    "Change %": "change_pct",
    "Day Open": "day_open",
    "Day High": "day_high",
    "Day Low": "day_low",
    "52W High": "week_52_high",
    "52W Low": "week_52_low",
    "52W Position %": "week_52_position_pct",
    # volume/cap
    "Volume": "volume",
    "Avg Vol 30D": "avg_vol_30d",
    "Value Traded": "value_traded",
    "Turnover %": "turnover_pct",
    "Shares Outstanding": "shares_outstanding",
    "Free Float %": "free_float",
    "Market Cap": "market_cap",
    "Free Float Mkt Cap": "free_float_market_cap",
    "Liquidity Score": "liquidity_score",
    # fundamentals
    "EPS (TTM)": "eps_ttm",
    "Forward EPS": "forward_eps",
    "P/E (TTM)": "pe_ttm",
    "Forward P/E": "forward_pe",
    "P/B": "pb",
    "P/S": "ps",
    "EV/EBITDA": "ev_ebitda",
    "Dividend Yield": "dividend_yield",
    "Dividend Rate": "dividend_rate",
    "Payout Ratio": "payout_ratio",
    "Beta": "beta",
    "ROE": "roe",
    "ROA": "roa",
    "Net Margin": "net_margin",
    "EBITDA Margin": "ebitda_margin",
    "Revenue Growth": "revenue_growth",
    "Net Income Growth": "net_income_growth",
    # technicals/valuation
    "Volatility 30D": "volatility_30d",
    "RSI 14": "rsi_14",
    "Fair Value": "fair_value",
    "Upside %": "upside_pct",
    "Valuation Label": "valuation_label",
    # scores/meta
    "Value Score": "value_score",
    "Quality Score": "quality_score",
    "Momentum Score": "momentum_score",
    "Opportunity Score": "opportunity_score",
    "Risk Score": "risk_score",
    "Overall Score": "overall_score",
    "Error": "error",
    "Recommendation": "recommendation",
    "Data Source": "data_source",
    "Data Quality": "data_quality",
    "Last Updated (UTC)": "last_updated_utc",
    "Last Updated (Riyadh)": "last_updated_riyadh",
}

# Build normalized map once (fast lookups)
HEADER_TO_FIELD_NORM: Dict[str, str] = {normalize_header(k): v for k, v in HEADER_TO_FIELD_RAW.items()}
FIELD_TO_HEADER: Dict[str, str] = {}
for h, f in HEADER_TO_FIELD_RAW.items():
    # keep first “best” header
    FIELD_TO_HEADER.setdefault(f, h)


FIELD_ALIASES: Dict[str, Tuple[str, ...]] = {
    "symbol": ("ticker", "code", "symbol_normalized"),
    "name": ("company_name", "long_name", "title"),
    "sub_sector": ("subsector", "industry"),
    "current_price": ("price", "last_price"),
    "previous_close": ("prev_close", "prior_close"),
    "change": ("price_change",),
    "change_pct": ("percent_change", "change_percent"),
    "week_52_high": ("high_52w", "fifty_two_week_high"),
    "week_52_low": ("low_52w", "fifty_two_week_low"),
    "week_52_position_pct": ("position_52w_pct", "week_52_position"),
    "avg_vol_30d": ("avg_volume_30d", "average_volume"),
    "turnover_pct": ("turnover_percent",),
    "free_float_market_cap": ("free_float_mkt_cap",),
    "dividend_yield": ("div_yield",),
    "payout_ratio": ("payout",),
    "volatility_30d": ("vol_30d", "vol_30d_ann"),
    "upside_pct": ("upside_percent", "upside"),
}

ALIAS_TO_CANONICAL: Dict[str, str] = {}
for canon, aliases in FIELD_ALIASES.items():
    for a in aliases:
        ALIAS_TO_CANONICAL[a] = canon


@lru_cache(maxsize=2048)
def canonical_field(field: str) -> str:
    f = safe_str(field)
    if not f:
        return ""
    return ALIAS_TO_CANONICAL.get(f, f)


@lru_cache(maxsize=2048)
def header_to_field(header: str) -> str:
    """
    Converts an arbitrary header label to canonical field name.
    """
    if not header:
        return ""
    # 1) direct raw
    if header in HEADER_TO_FIELD_RAW:
        return HEADER_TO_FIELD_RAW[header]
    # 2) normalized
    h = normalize_header(header)
    if h in HEADER_TO_FIELD_NORM:
        return HEADER_TO_FIELD_NORM[h]
    # 3) last resort: if header itself looks like a field
    return canonical_field(h)


def field_to_header(field: str) -> str:
    f = canonical_field(field)
    return FIELD_TO_HEADER.get(f, f)


# =============================================================================
# Sheet name normalization + schema registry
# =============================================================================

@lru_cache(maxsize=256)
def normalize_sheet_name(name: Optional[str]) -> str:
    s = safe_str(name).lower()
    if not s:
        return ""
    s = re.sub(r"^(?:sheet_|tab_)", "", s)
    s = re.sub(r"(?:_sheet|_tab)$", "", s)
    s = re.sub(r"[\s\-_]+", "_", s).strip("_")

    replacements = {
        "ksa": "ksa_tadawul",
        "tadawul": "ksa_tadawul",
        "saudi": "ksa_tadawul",
        "global": "global_markets",
        "world": "global_markets",
        "portfolio": "my_portfolio",
        "myportfolio": "my_portfolio",
        "funds": "mutual_funds",
        "mutualfunds": "mutual_funds",
        "commodities": "commodities_fx",
        "fx": "commodities_fx",
        "forex": "commodities_fx",
        "insights": "insights_analysis",
        "analysis": "insights_analysis",
    }
    return replacements.get(s, s)


def resolve_sheet_key(sheet_name: Optional[str]) -> str:
    """
    Compatibility helper (some modules call resolve_sheet_key()).
    """
    return normalize_sheet_name(sheet_name) or "global_markets"


_SCHEMA_REGISTRY: Dict[str, Tuple[str, ...]] = {}


def register_schema(name: str, headers: Sequence[str], version: str = "vNext") -> None:
    key = f"{version}:{normalize_sheet_name(name)}"
    _SCHEMA_REGISTRY[key] = tuple(_dedupe(list(headers)))


# Register schemas at import time (deterministic)
for n, h in VNEXT_SCHEMAS.items():
    register_schema(n, h, "vNext")
for n, h in LEGACY_SCHEMAS.items():
    register_schema(n, h, "legacy")


@lru_cache(maxsize=256)
def get_headers_for_sheet(sheet_name: Optional[str] = None, version: str = "vNext") -> List[str]:
    """
    Returns headers for a given sheet (version can be: vNext | legacy).
    """
    norm = normalize_sheet_name(sheet_name)
    if not norm:
        # default
        return list(_SCHEMA_REGISTRY.get(f"{version}:global_markets", ENRICHED_HEADERS_61))

    key = f"{version}:{norm}"
    if key in _SCHEMA_REGISTRY:
        return list(_SCHEMA_REGISTRY[key])

    # fallback: try any matching suffix
    for k, headers in _SCHEMA_REGISTRY.items():
        if k.startswith(f"{version}:") and k.endswith(f":{norm}"):
            return list(headers)

    # fallback default
    return list(_SCHEMA_REGISTRY.get(f"{version}:global_markets", ENRICHED_HEADERS_61))


def get_supported_sheets(version: str = "vNext") -> List[str]:
    return sorted({k.split(":", 1)[1] for k in _SCHEMA_REGISTRY.keys() if k.startswith(f"{version}:")})


# =============================================================================
# Request models
# =============================================================================

class BatchProcessRequest(BaseModel):
    operation: str = Field(default="refresh")
    sheet_name: Optional[str] = None
    symbols: List[str] = Field(default_factory=list)
    tickers: List[str] = Field(default_factory=list)
    force_refresh: bool = False
    include_forecast: bool = True
    include_technical: bool = True
    priority: int = Field(default=0, ge=0, le=10)
    timeout_seconds: Optional[int] = Field(None, ge=1, le=300)
    webhook_url: Optional[str] = None

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")

        @field_validator("symbols", "tickers", mode="before")
        @classmethod
        def _validate_symbol_list(cls, v: Any) -> List[str]:
            if v is None:
                return []
            if isinstance(v, str):
                v = re.split(r"[,\s\n]+", v)
            if isinstance(v, (list, tuple)):
                out = []
                for x in v:
                    s = safe_str(x)
                    if s:
                        out.append(s.upper())
                return out
            return []

        @model_validator(mode="after")
        def _combine(self) -> "BatchProcessRequest":
            self.symbols = sorted(set(self.symbols + self.tickers))
            self.tickers = []
            return self

    else:
        class Config:
            extra = "ignore"

        @validator("symbols", "tickers", pre=True)
        def _validate_symbol_list_v1(cls, v: Any) -> List[str]:
            if v is None:
                return []
            if isinstance(v, str):
                v = re.split(r"[,\s\n]+", v)
            if isinstance(v, (list, tuple)):
                out = []
                for x in v:
                    s = safe_str(x)
                    if s:
                        out.append(s.upper())
                return out
            return []

        @root_validator
        def _combine_v1(cls, values: Dict[str, Any]) -> Dict[str, Any]:
            values["symbols"] = sorted(set((values.get("symbols") or []) + (values.get("tickers") or [])))
            values["tickers"] = []
            return values

    def all_symbols(self) -> List[str]:
        return self.symbols


class BatchProcessResponse(BaseModel):
    request_id: str
    operation: str
    sheet_name: Optional[str] = None
    symbols_processed: int = 0
    symbols_failed: int = 0
    symbols_total: int = 0
    processing_time_ms: float = 0.0
    status: str = "completed"
    error: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")


# =============================================================================
# Validation utilities
# =============================================================================

def validate_headers(headers: Sequence[str], expected_len: Optional[int] = None) -> Tuple[bool, List[str]]:
    if not headers:
        return False, ["Headers are empty"]

    errors: List[str] = []
    if expected_len is not None and len(headers) != expected_len:
        errors.append(f"Expected {expected_len} headers, got {len(headers)}")

    seen = set()
    dup = []
    for h in headers:
        k = normalize_header(h)
        if k in seen:
            dup.append(h)
        else:
            seen.add(k)
    if dup:
        errors.append(f"Duplicate headers: {dup}")

    return len(errors) == 0, errors


def validate_sheet_data(sheet_name: str, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Lightweight validator:
    - ensures headers exist for that sheet (schema registered)
    - ensures at least Symbol + Price exist (if present in schema)
    """
    errors: List[str] = []
    sheet_key = resolve_sheet_key(sheet_name)
    headers = get_headers_for_sheet(sheet_key, version="vNext")
    if not headers:
        errors.append("No schema headers found")

    # required checks (only if headers request them)
    required_headers = ["Symbol", "Price"]
    for rh in required_headers:
        if rh in headers:
            f = header_to_field(rh)
            if data.get(f) in (None, "", []):
                errors.append(f"Missing required field: {f} (from header {rh})")

    return len(errors) == 0, errors


def get_field_groups() -> Dict[str, List[str]]:
    """
    For UI/group rendering
    """
    return {
        "Identity": VN_IDENTITY,
        "Price": VN_PRICE,
        "Volume": VN_VOLUME,
        "Capitalization": VN_CAP,
        "Fundamentals": VN_FUNDAMENTALS,
        "Technicals": VN_TECHNICALS,
        "Forecast": VN_FORECAST,
        "Scores": VN_SCORES,
        "Meta": VN_META,
    }


# =============================================================================
# Exports
# =============================================================================
__all__ = [
    # version/enums
    "SCHEMAS_VERSION",
    "MarketType",
    "AssetClass",
    "Recommendation",
    "DataQuality",
    "BadgeLevel",
    # models
    "UnifiedQuote",
    "BatchProcessRequest",
    "BatchProcessResponse",
    # headers/schemas
    "ENRICHED_HEADERS_61",
    "DEFAULT_HEADERS_59",
    "DEFAULT_HEADERS_ANALYSIS",
    "VNEXT_SCHEMAS",
    "LEGACY_SCHEMAS",
    "VN_IDENTITY",
    "VN_PRICE",
    "VN_VOLUME",
    "VN_CAP",
    "VN_FUNDAMENTALS",
    "VN_TECHNICALS",
    "VN_FORECAST",
    "VN_SCORES",
    "VN_META",
    "VN_HEADERS_KSA_TADAWUL",
    "VN_HEADERS_GLOBAL",
    "VN_HEADERS_INSIGHTS",
    # mapping helpers
    "normalize_header",
    "canonical_field",
    "header_to_field",
    "field_to_header",
    "HEADER_TO_FIELD_RAW",
    "FIELD_TO_HEADER",
    "FIELD_ALIASES",
    "ALIAS_TO_CANONICAL",
    # sheet helpers
    "normalize_sheet_name",
    "resolve_sheet_key",
    "register_schema",
    "get_headers_for_sheet",
    "get_supported_sheets",
    # validation utilities
    "validate_headers",
    "validate_sheet_data",
    "get_field_groups",
    # parsing helpers
    "safe_float",
    "safe_int",
    "safe_str",
    "safe_bool",
    "safe_date",
    "safe_datetime",
    "bound_value",
    "percent_to_decimal",
    "decimal_to_percent",
    # misc
    "normalize_recommendation",
]
