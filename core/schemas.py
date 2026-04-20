#!/usr/bin/env python3
# core/schemas.py
"""
================================================================================
Core Schemas + Sheet Headers — v7.1.0 (REGISTRY-ALIGNED / VALIDATION-CORRECT)
================================================================================

Purpose
-------
This module provides:
- runtime-safe schema/header helpers
- lightweight shared models and validators
- backward-compatible exports used by routes/builders

Key design rule
---------------
The canonical source of truth is `core.sheets.schema_registry` whenever it is
available. This file mirrors that registry and only falls back to deterministic,
fixed-width contracts when the registry cannot be imported.

v7.1.0 changes (what moved from v7.0.0)
---------------------------------------
- CRITICAL FIX: `ValidationError` no longer shadows pydantic's ValidationError.
  v7.0.0 imported `from pydantic import ValidationError` then redefined
  `class ValidationError(SchemaError): pass` a few lines later, which
  overwrote the name. `UnifiedQuote.from_dict`'s `except ValidationError:
  if strict: raise` then caught a class pydantic never raises, so the
  strict-re-raise path was dead code and real pydantic errors fell into
  the bare `except Exception: return None` and were silently swallowed.
  v7.1.0 renames the custom exception to `SchemaValidationError`, exports
  `ValidationError` as an alias (same class) for backward compatibility,
  and binds pydantic's class as `PydanticValidationError` so `from_dict`
  can catch the correct exception.

- CRITICAL FIX: `validate_sheet_data` now derives expected column count
  from the actual sheet contract instead of a hardcoded map. v7.0.0 had:
      Market_Leaders:     99   (registry says 80)
      Global_Markets:     112  (registry says 80)
      Commodities_FX:     86   (registry says 80)
      Mutual_Funds:       94   (registry says 80)
      My_Portfolio:       110  (registry says 80)
      Top_10_Investments: 106  (registry says 83)
      Insights_Analysis:  9    (registry says 7)
      Data_Dictionary:    9    (registry says 9 -- the only correct one)
  With the v7.0.0 hardcoded numbers, validate_sheet_data reported
  "Expected 99 headers, got 80" for every standard sheet when the
  registry was active.

- FIX: `validate_sheet_data` Top_10 required-fields list trimmed to the
  canonical extras that actually exist in the registry
  (top10_rank, selection_reason, criteria_snapshot). v7.0.0 also
  demanded entry_price, stop_loss_suggested, take_profit_suggested,
  risk_reward_ratio — none of which are in the registry's 83-col
  contract, so every Top_10 payload failed validation.

- FIX: VN_* field-group lists now derive from canonical KEYS (stable
  across registry and fallback) and translate back to headers via
  `field_to_header`. v7.0.0 hardcoded header strings like "Percent
  Change" and "Avg Volume 10D" that don't exist in the fallback
  contract ("Change %" and "Avg Vol 10D"), so `_filter_present`
  silently dropped those entries.

- FIX: `UnifiedQuote._post_fixups` now wraps the inline
  `from core.reco_normalize import normalize_recommendation` in a
  try/except. v7.0.0 would raise ImportError out of pydantic validation
  if reco_normalize was missing, turning a cosmetic fallback into a
  fatal construction error.

- FIX: schema_registry import is now resilient to partial exports.
  v7.0.0 imported 4 names in a single `from ... import` — if ANY one
  was missing, the whole block fell into the ImportError branch and
  disabled the registry entirely. v7.1.0 imports per-name.

- DOC/LINT: the fallback standard contract remains at 97 columns for
  back-compat with any caller that relies on the v7.0.0 fallback shape,
  but is explicitly documented as "fallback only; used when registry
  unavailable". In production the registry provides 80 columns.

Public API preserved. All v7.0.0 exports remain available with the same
semantics on the happy path.
================================================================================
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from functools import lru_cache
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# ---------------------------------------------------------------------------
# Fast JSON Support (orjson optional)
# ---------------------------------------------------------------------------

try:
    import orjson  # type: ignore

    def _json_dumps(value: Any, default: Any = str) -> str:
        return orjson.dumps(value, default=default).decode("utf-8")

    def _json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

    _HAS_ORJSON = True
except ImportError:
    def _json_dumps(value: Any, default: Any = str) -> str:
        return json.dumps(value, default=default, ensure_ascii=False)

    def _json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Pydantic Compatibility (v2 Preferred, v1 Fallback)
# ---------------------------------------------------------------------------
# v7.1.0: pydantic's own ValidationError is kept under a distinct name
# (`PydanticValidationError`) so our custom SchemaValidationError no
# longer shadows it.

try:
    from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator
    from pydantic import ValidationError as PydanticValidationError
    _PYDANTIC_V2 = True
except ImportError:
    from pydantic import BaseModel, Field  # type: ignore
    from pydantic import ValidationError as PydanticValidationError  # type: ignore
    try:
        from pydantic import validator, root_validator  # type: ignore
    except ImportError:
        validator = None  # type: ignore
        root_validator = None  # type: ignore
    ConfigDict = None  # type: ignore
    field_validator = None  # type: ignore
    model_validator = None  # type: ignore
    _PYDANTIC_V2 = False

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

SCHEMAS_VERSION = "7.1.0"

# ---------------------------------------------------------------------------
# Custom Exceptions
# ---------------------------------------------------------------------------

class SchemaError(Exception):
    """Base exception for schema operations."""
    pass


class SchemaValidationError(SchemaError):
    """Raised when schema validation fails.

    v7.1.0: renamed from `ValidationError` to avoid shadowing pydantic's
    `ValidationError`, which is the actual class pydantic raises on model
    construction failure.
    """
    pass


# Back-compat alias. `ValidationError` used to refer to the custom
# exception above in v7.0.0 (after the shadow). Keep the alias pointing
# at the same class so any external `except schemas.ValidationError`
# continues to work.
ValidationError = SchemaValidationError


class SheetNotFoundError(SchemaError):
    """Raised when sheet is not found."""
    pass


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class MarketType(str, Enum):
    """Market type classification."""
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
    """Asset class classification."""
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
    """Recommendation values.

    Note: this enum has 6 levels (STRONG_BUY/BUY/HOLD/REDUCE/SELL/STRONG_SELL),
    while `core.reco_normalize.Recommendation` has 5 (no STRONG_SELL). Values
    returned by reco_normalize are a strict subset of this enum; the reverse
    is not true — STRONG_SELL rounds to SELL when passed through reco_normalize.
    """
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    SELL = "SELL"
    STRONG_SELL = "STRONG_SELL"

    @classmethod
    def from_score(cls, score: float, scale: str = "0-100") -> "Recommendation":
        """Convert score to recommendation."""
        try:
            s = float(score)
            if s != s:  # NaN
                return cls.HOLD
        except (TypeError, ValueError):
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

        if scale == "1-5":
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

    def to_score(self) -> int:
        """Convert to numeric score."""
        scores = {
            Recommendation.STRONG_BUY: 0,
            Recommendation.BUY: 1,
            Recommendation.HOLD: 2,
            Recommendation.REDUCE: 3,
            Recommendation.SELL: 4,
            Recommendation.STRONG_SELL: 5,
        }
        return scores.get(self, 2)


class DataQuality(str, Enum):
    """Data quality levels."""
    EXCELLENT = "EXCELLENT"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    POOR = "POOR"
    STALE = "STALE"
    MISSING = "MISSING"
    ERROR = "ERROR"


class BadgeLevel(str, Enum):
    """Badge level classification."""
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    NEUTRAL = "NEUTRAL"
    CAUTION = "CAUTION"
    DANGER = "DANGER"
    NONE = "NONE"


# ---------------------------------------------------------------------------
# Pure Utility Functions
# ---------------------------------------------------------------------------

_NUM_RE = re.compile(r"^[\s\-\+]*[\d\.,]+(?:[eE][\+\-]?\d+)?\s*%?\s*$")


def safe_str(value: Any, default: str = "") -> str:
    """Safely convert to string."""
    if value is None:
        return default
    try:
        return str(value).strip()
    except Exception:
        return default


def safe_bool(value: Any, default: bool = False) -> bool:
    """Safely convert to boolean."""
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
    """Safely convert to float."""
    if value is None or isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return default

    s = safe_str(value)
    if not s:
        return default

    if s.endswith("%"):
        s = s[:-1].strip()

    if not _NUM_RE.match(s):
        s = re.sub(r"[^\d\.,\-\+eE]", "", s)
    if not s:
        return default

    # Handle international number formats
    if "," in s and "." in s:
        if s.rindex(",") > s.rindex("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")

    try:
        return float(s)
    except Exception:
        return default


def safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    """Safely convert to integer."""
    f = safe_float(value, None)
    if f is None:
        return default
    try:
        return int(round(f))
    except Exception:
        return default


def safe_date(value: Any) -> Optional[date]:
    """Safely convert to date."""
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
    """Safely convert to datetime."""
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


def bound_value(
    value: Optional[float],
    min_val: float,
    max_val: float,
    default: Optional[float] = None,
) -> Optional[float]:
    """Clamp value between min and max."""
    if value is None:
        return default
    return max(min_val, min(max_val, value))


def decimal_to_percent(value: Optional[float]) -> Optional[float]:
    """Convert decimal to percent."""
    if value is None:
        return None
    return value * 100.0 if -1.0 <= value <= 1.0 else value


def percent_to_decimal(value: Optional[float]) -> Optional[float]:
    """Convert percent to decimal."""
    if value is None:
        return None
    return value / 100.0 if abs(value) > 1.0 else value


# ---------------------------------------------------------------------------
# UnifiedQuote Model
# ---------------------------------------------------------------------------

class UnifiedQuote(BaseModel):
    """Unified quote model with all fields."""
    symbol: str = Field(..., description="Trading symbol")
    symbol_normalized: Optional[str] = Field(None, description="Normalized symbol")
    requested_symbol: Optional[str] = Field(None, description="Original requested symbol")

    # Identity
    origin: Optional[str] = None
    name: Optional[str] = None
    exchange: Optional[str] = None
    market: Optional[MarketType] = None
    asset_class: Optional[AssetClass] = None
    currency: Optional[str] = Field(None, max_length=3)
    country: Optional[str] = None
    sector: Optional[str] = None
    sub_sector: Optional[str] = None
    industry: Optional[str] = None
    listing_date: Optional[date] = None

    # Price
    price: Optional[float] = None
    current_price: Optional[float] = None
    previous_close: Optional[float] = None
    open_price: Optional[float] = None
    day_open: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    week_52_high: Optional[float] = None
    week_52_low: Optional[float] = None
    week_52_position_pct: Optional[float] = None
    change: Optional[float] = None
    change_pct: Optional[float] = None

    # Volume
    volume: Optional[float] = None
    avg_volume_10d: Optional[float] = None
    avg_vol_30d: Optional[float] = None
    market_cap: Optional[float] = None
    float_shares: Optional[float] = None
    beta_5y: Optional[float] = None
    turnover_pct: Optional[float] = None
    value_traded: Optional[float] = None
    liquidity_score: Optional[float] = None

    # Fundamentals
    pe_ttm: Optional[float] = None
    forward_pe: Optional[float] = None
    eps_ttm: Optional[float] = None
    dividend_yield: Optional[float] = None
    payout_ratio: Optional[float] = None
    revenue_ttm: Optional[float] = None
    revenue_yoy_growth: Optional[float] = None
    gross_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    profit_margin: Optional[float] = None
    debt_to_equity: Optional[float] = None
    free_cash_flow_ttm: Optional[float] = None

    # Technicals
    rsi_14: Optional[float] = None
    volatility_30d: Optional[float] = None
    volatility_90d: Optional[float] = None
    max_drawdown_1y: Optional[float] = None
    var_95_1d: Optional[float] = None
    sharpe_1y: Optional[float] = None
    risk_score: Optional[float] = None
    risk_bucket: Optional[str] = None

    # Valuation
    pb_ratio: Optional[float] = None
    ps_ratio: Optional[float] = None
    ev_ebitda: Optional[float] = None
    peg_ratio: Optional[float] = None
    intrinsic_value: Optional[float] = None
    valuation_score: Optional[float] = None

    # Forecast
    forecast_price_1m: Optional[float] = None
    forecast_price_3m: Optional[float] = None
    forecast_price_12m: Optional[float] = None
    expected_roi_1m_pct: Optional[float] = None
    expected_roi_3m_pct: Optional[float] = None
    expected_roi_12m_pct: Optional[float] = None
    forecast_confidence: Optional[float] = None
    forecast_method: Optional[str] = None

    # Scores
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    growth_score: Optional[float] = None
    overall_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    rank_overall: Optional[float] = None
    confidence_bucket: Optional[str] = None

    # Recommendation
    recommendation: Optional[Recommendation] = None
    recommendation_reason: Optional[str] = None

    # Metadata
    data_source: Optional[str] = None
    data_quality: Optional[DataQuality] = None
    last_updated_utc: Optional[datetime] = None
    last_updated_riyadh: Optional[datetime] = None
    rank: Optional[float] = None
    error: Optional[str] = None
    warning: Optional[str] = None
    info: Optional[Any] = None

    # Top10
    top10_rank: Optional[float] = None
    selection_reason: Optional[str] = None
    criteria_snapshot: Optional[str] = None

    if _PYDANTIC_V2:
        model_config = ConfigDict(
            extra="allow",
            validate_assignment=True,
            arbitrary_types_allowed=True,
        )

        @model_validator(mode="before")
        @classmethod
        def _pre_coerce(cls, data: Any) -> Any:
            """Pre-coerce percent strings."""
            if isinstance(data, dict):
                for k, v in list(data.items()):
                    if isinstance(v, str) and v.strip().endswith("%"):
                        data[k] = safe_float(v)
            return data

        @field_validator("symbol", mode="before")
        @classmethod
        def _validate_symbol(cls, v: Any) -> str:
            """Validate symbol field."""
            s = safe_str(v)
            if not s:
                raise ValueError("symbol is required")
            return s.upper()

        @field_validator("currency", mode="before")
        @classmethod
        def _validate_currency(cls, v: Any) -> Optional[str]:
            """Validate currency field."""
            if v is None:
                return None
            s = safe_str(v).upper()
            return s[:3] if s else None

        @field_validator(
            "week_52_position_pct",
            "turnover_pct",
            "dividend_yield",
            "payout_ratio",
            "revenue_yoy_growth",
            "gross_margin",
            "operating_margin",
            "profit_margin",
            "volatility_30d",
            "volatility_90d",
            "change_pct",
            "expected_roi_1m_pct",
            "expected_roi_3m_pct",
            "expected_roi_12m_pct",
            mode="before",
        )
        @classmethod
        def _percent_fields_smart(cls, v: Any) -> Any:
            """Handle percent fields."""
            f = safe_float(v, None)
            return decimal_to_percent(f) if f is not None else None

        @field_validator("listing_date", mode="before")
        @classmethod
        def _coerce_date(cls, v: Any) -> Any:
            """Coerce to date."""
            return safe_date(v)

        @field_validator("last_updated_utc", "last_updated_riyadh", mode="before")
        @classmethod
        def _coerce_datetime(cls, v: Any) -> Any:
            """Coerce to datetime."""
            return safe_datetime(v)

        @model_validator(mode="after")
        def _post_fixups(self) -> "UnifiedQuote":
            """Post-validation fixups."""
            # Price aliases
            if self.price is not None and self.current_price is None:
                self.current_price = self.price
            if self.current_price is not None and self.price is None:
                self.price = self.current_price

            # Open price aliases
            if self.day_open is not None and self.open_price is None:
                self.open_price = self.day_open
            if self.open_price is not None and self.day_open is None:
                self.day_open = self.open_price

            # Calculate change
            if self.change is None and self.current_price is not None and self.previous_close is not None:
                try:
                    self.change = self.current_price - self.previous_close
                except Exception:
                    pass

            # Calculate change percent
            if self.change_pct is None and self.current_price is not None and self.previous_close not in (None, 0):
                try:
                    self.change_pct = ((self.current_price / self.previous_close) - 1.0) * 100.0
                except Exception:
                    pass

            # Normalize symbol
            if self.symbol and not self.symbol_normalized:
                self.symbol_normalized = self.symbol.upper()

            # Normalize recommendation from info string.
            # v7.1.0: wrapped in try/except so a missing `core.reco_normalize`
            # doesn't propagate ImportError out of pydantic construction.
            if self.recommendation is None and self.info and isinstance(self.info, str):
                try:
                    from core.reco_normalize import normalize_recommendation as _norm_reco
                    normalized = _norm_reco(self.info)
                    if normalized in Recommendation._value2member_map_:
                        self.recommendation = Recommendation(normalized)
                except Exception as e:
                    logger.debug("recommendation normalization skipped: %s", e)

            return self
    else:
        class Config:
            extra = "allow"
            validate_assignment = True

        if validator is not None:
            @validator("symbol", pre=True, always=True)
            def _v1_symbol(cls, v: Any) -> str:  # type: ignore[misc]
                s = safe_str(v)
                if not s:
                    raise ValueError("symbol is required")
                return s.upper()

        if root_validator is not None:
            @root_validator(pre=False)
            def _v1_post(cls, values: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore[misc]
                """V1 post-validation fixups."""
                price = values.get("price")
                current_price = values.get("current_price")
                if price is not None and current_price is None:
                    values["current_price"] = price
                if current_price is not None and price is None:
                    values["price"] = current_price

                if values.get("symbol") and not values.get("symbol_normalized"):
                    values["symbol_normalized"] = str(values["symbol"]).upper()

                return values

    def to_dict(self, exclude_none: bool = True) -> Dict[str, Any]:
        """Convert to dictionary."""
        if _PYDANTIC_V2:
            return self.model_dump(exclude_none=exclude_none)
        return self.dict(exclude_none=exclude_none)

    def to_json(self) -> str:
        """Convert to JSON string."""
        if _HAS_ORJSON:
            return _json_dumps(self.to_dict(exclude_none=True), default=str)
        if _PYDANTIC_V2:
            return self.model_dump_json(exclude_none=True)
        return self.json(exclude_none=True)

    @classmethod
    def from_dict(cls, data: Dict[str, Any], strict: bool = False) -> Optional["UnifiedQuote"]:
        """Create from dictionary.

        v7.1.0: catches `PydanticValidationError` explicitly. v7.0.0's
        `except ValidationError:` caught the custom SchemaValidationError
        (due to name shadowing), which pydantic never raises, so the
        strict-re-raise path was dead code and real validation errors
        were swallowed by the bare `except Exception`.
        """
        try:
            return cls(**(data or {}))
        except PydanticValidationError:
            if strict:
                raise
            return None
        except Exception:
            if strict:
                raise
            return None

    def compute_hash(self) -> str:
        """Compute hash for caching."""
        payload = self.to_dict(exclude_none=True)
        raw = _json_dumps(payload, default=str) if _HAS_ORJSON else json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Recommendation Normalization
# ---------------------------------------------------------------------------

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


def normalize_recommendation(value: Any) -> Optional[Recommendation]:
    """Normalize recommendation to enum."""
    if value is None:
        return None
    if isinstance(value, Recommendation):
        return value

    # Try the authoritative reco_normalize module first
    try:
        from core.reco_normalize import normalize_recommendation as _norm_reco
        normalized = _norm_reco(value)
        if normalized in Recommendation._value2member_map_:
            return Recommendation(normalized)
    except Exception:
        pass

    # Local fallback
    s = safe_str(value).upper()
    if not s:
        return None

    s = re.sub(r"[\s\-_]+", " ", s).strip()
    return _RECOMMENDATION_ALIASES.get(s, Recommendation.HOLD)


# ---------------------------------------------------------------------------
# Sheet Helpers
# ---------------------------------------------------------------------------

CANONICAL_SHEETS: Tuple[str, ...] = (
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
    "Insights_Analysis",
    "Top_10_Investments",
    "Data_Dictionary",
)

_CANONICAL_SHEET_ALIASES: Dict[str, str] = {
    "market_leaders": "Market_Leaders",
    "marketleaders": "Market_Leaders",
    "ksa": "Market_Leaders",
    "ksa_tadawul": "Market_Leaders",
    "tadawul": "Market_Leaders",
    "saudi": "Market_Leaders",
    "global_markets": "Global_Markets",
    "global": "Global_Markets",
    "world": "Global_Markets",
    "commodities_fx": "Commodities_FX",
    "commodities": "Commodities_FX",
    "fx": "Commodities_FX",
    "forex": "Commodities_FX",
    "mutual_funds": "Mutual_Funds",
    "funds": "Mutual_Funds",
    "etfs": "Mutual_Funds",
    "my_portfolio": "My_Portfolio",
    "portfolio": "My_Portfolio",
    "my_investments": "My_Portfolio",
    "top_10_investments": "Top_10_Investments",
    "top10": "Top_10_Investments",
    "insights_analysis": "Insights_Analysis",
    "insights": "Insights_Analysis",
    "analysis": "Insights_Analysis",
    "data_dictionary": "Data_Dictionary",
    "dictionary": "Data_Dictionary",
}


@lru_cache(maxsize=256)
def normalize_sheet_name(name: Optional[str]) -> str:
    """Normalize sheet name."""
    s = safe_str(name).lower()
    if not s:
        return ""

    s = re.sub(r"^(?:sheet_|tab_)", "", s)
    s = re.sub(r"(?:_sheet|_tab)$", "", s)
    s = re.sub(r"[\s\-_/]+", "_", s).strip("_")
    return s


@lru_cache(maxsize=256)
def resolve_sheet_key(sheet_name: Optional[str]) -> str:
    """Resolve sheet key to canonical name."""
    norm = normalize_sheet_name(sheet_name)
    if not norm:
        return "Global_Markets"

    if norm in _CANONICAL_SHEET_ALIASES:
        return _CANONICAL_SHEET_ALIASES[norm]

    for sheet in CANONICAL_SHEETS:
        if normalize_sheet_name(sheet) == norm:
            return sheet

    return sheet_name if safe_str(sheet_name) in CANONICAL_SHEETS else "Global_Markets"


# ---------------------------------------------------------------------------
# Fallback Contracts (Used Only If Schema Registry Unavailable)
# ---------------------------------------------------------------------------
# NOTE: the fallback standard contract is 97 columns, while the authoritative
# registry contract is 80 columns. The fallback is deliberately wider than
# the registry to preserve back-compat with callers that depend on the
# legacy fallback shape (v6.x behavior). In production, the registry is
# always importable and the registry shape takes precedence.

_FALLBACK_STANDARD_HEADERS: List[str] = [
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
    "Current Price", "Previous Close", "Open", "Day High", "Day Low",
    "52W High", "52W Low", "Price Change", "Change %", "52W Position %",
    "5D Change %",
    "Volume", "Avg Vol 10D", "Avg Vol 30D", "Market Cap", "Float Shares", "Volume Ratio",
    "Beta (5Y)", "P/E (TTM)", "P/E (Fwd)", "EPS (TTM)", "Div Yield %", "Payout Ratio %",
    "Revenue TTM", "Rev Growth YoY %", "Gross Margin %", "Op Margin %",
    "Net Margin %", "D/E Ratio", "FCF (TTM)",
    "ROE %", "ROA %",
    "RSI (14)", "Volatility 30D %", "Volatility 90D %", "Max DD 1Y %",
    "VaR 95% (1D)", "Sharpe (1Y)", "Risk Score", "Risk Bucket",
    "RSI Signal", "Tech Score", "Day Range Pos %", "ATR 14",
    "P/B", "P/S", "EV/EBITDA", "PEG Ratio", "Intrinsic Value", "Valuation Score", "Upside %",
    "Price Tgt 1M", "Price Tgt 3M", "Price Tgt 12M",
    "ROI 1M %", "ROI 3M %", "ROI 12M %",
    "AI Confidence", "Confidence Score", "Confidence",
    "Value Score", "Quality Score", "Momentum Score", "Growth Score", "Overall Score", "Opportunity Score",
    "Analyst Rating", "Target Price", "Upside/Downside %",
    "Recommendation", "Signal", "Trend 1M", "Trend 3M", "Trend 12M",
    "ST Signal", "Reason", "Horizon", "Horizon Days",
    "Rank (Overall)",
    "Qty", "Avg Cost", "Position Cost", "Position Value", "Unrealized P/L", "Unrealized P/L %",
    "Data Provider", "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",
]

_FALLBACK_STANDARD_KEYS: List[str] = [
    "symbol", "name", "asset_class", "exchange", "currency", "country", "sector", "industry",
    "current_price", "previous_close", "open_price", "day_high", "day_low",
    "week_52_high", "week_52_low", "price_change", "percent_change", "week_52_position_pct",
    "price_change_5d",
    "volume", "avg_volume_10d", "avg_volume_30d", "market_cap", "float_shares", "volume_ratio",
    "beta_5y", "pe_ttm", "pe_forward", "eps_ttm", "dividend_yield", "payout_ratio",
    "revenue_ttm", "revenue_growth_yoy", "gross_margin", "operating_margin",
    "profit_margin", "debt_to_equity", "free_cash_flow_ttm",
    "roe", "roa",
    "rsi_14", "volatility_30d", "volatility_90d", "max_drawdown_1y",
    "var_95_1d", "sharpe_1y", "risk_score", "risk_bucket",
    "rsi_signal", "technical_score", "day_range_position", "atr_14",
    "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio", "intrinsic_value", "valuation_score", "upside_pct",
    "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
    "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "forecast_confidence", "confidence_score", "confidence_bucket",
    "value_score", "quality_score", "momentum_score", "growth_score", "overall_score", "opportunity_score",
    "analyst_rating", "target_price", "upside_downside_pct",
    "recommendation", "signal", "trend_1m", "trend_3m", "trend_12m",
    "short_term_signal", "recommendation_reason", "invest_period_label", "horizon_days",
    "rank_overall",
    "position_qty", "avg_cost", "position_cost", "position_value", "unrealized_pl", "unrealized_pl_pct",
    "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",
]

# Sanity guard — if these drift apart, the contract pairing is broken.
assert len(_FALLBACK_STANDARD_HEADERS) == len(_FALLBACK_STANDARD_KEYS), (
    f"FALLBACK standard contract lengths differ: "
    f"{len(_FALLBACK_STANDARD_HEADERS)} headers vs {len(_FALLBACK_STANDARD_KEYS)} keys"
)

_FALLBACK_TOP10_HEADERS: List[str] = list(_FALLBACK_STANDARD_HEADERS) + [
    "Top 10 Rank", "Selection Reason", "Criteria Snapshot",
    "Entry Price", "Stop Loss (AI)", "Take Profit (AI)", "Risk/Reward",
]

_FALLBACK_TOP10_KEYS: List[str] = list(_FALLBACK_STANDARD_KEYS) + [
    "top10_rank", "selection_reason", "criteria_snapshot",
    "entry_price", "stop_loss_suggested", "take_profit_suggested", "risk_reward_ratio",
]

_FALLBACK_INSIGHTS_HEADERS: List[str] = [
    "Section", "Item", "Symbol", "Metric", "Value",
    "Signal", "Priority", "Notes", "Last Updated (Riyadh)",
]

_FALLBACK_INSIGHTS_KEYS: List[str] = [
    "section", "item", "symbol", "metric", "value",
    "signal", "priority", "notes", "as_of_riyadh",
]

_FALLBACK_DICTIONARY_HEADERS: List[str] = [
    "Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes",
]

_FALLBACK_DICTIONARY_KEYS: List[str] = [
    "sheet", "group", "header", "key", "dtype", "fmt",
    "required", "source", "notes",
]

_FALLBACK_CONTRACTS: Dict[str, Tuple[List[str], List[str]]] = {
    "Market_Leaders": (list(_FALLBACK_STANDARD_HEADERS), list(_FALLBACK_STANDARD_KEYS)),
    "Global_Markets": (list(_FALLBACK_STANDARD_HEADERS), list(_FALLBACK_STANDARD_KEYS)),
    "Commodities_FX": (list(_FALLBACK_STANDARD_HEADERS), list(_FALLBACK_STANDARD_KEYS)),
    "Mutual_Funds": (list(_FALLBACK_STANDARD_HEADERS), list(_FALLBACK_STANDARD_KEYS)),
    "My_Portfolio": (list(_FALLBACK_STANDARD_HEADERS), list(_FALLBACK_STANDARD_KEYS)),
    "Top_10_Investments": (list(_FALLBACK_TOP10_HEADERS), list(_FALLBACK_TOP10_KEYS)),
    "Insights_Analysis": (list(_FALLBACK_INSIGHTS_HEADERS), list(_FALLBACK_INSIGHTS_KEYS)),
    "Data_Dictionary": (list(_FALLBACK_DICTIONARY_HEADERS), list(_FALLBACK_DICTIONARY_KEYS)),
}


# ---------------------------------------------------------------------------
# Schema Registry Bridge
# ---------------------------------------------------------------------------
# v7.1.0: resilient to partial registry exports. v7.0.0 imported 4 names
# with a single `from ... import`; if ANY one was missing, the whole block
# fell into the ImportError branch and disabled the registry entirely.

_registry_get_sheet_headers = None
_registry_get_sheet_keys = None
_registry_get_sheet_len = None
_registry_list_sheets = None
_HAS_SCHEMA_REGISTRY = False

try:
    import core.sheets.schema_registry as _sr_mod  # noqa: F401
except ImportError:
    _sr_mod = None
except Exception as _e:
    logger.debug("schema_registry import skipped: %s", _e)
    _sr_mod = None

if _sr_mod is not None:
    _registry_get_sheet_headers = getattr(_sr_mod, "get_sheet_headers", None)
    _registry_get_sheet_keys = getattr(_sr_mod, "get_sheet_keys", None)
    _registry_get_sheet_len = getattr(_sr_mod, "get_sheet_len", None)
    _registry_list_sheets = getattr(_sr_mod, "list_sheets", None)
    # The registry is "usable" if we at least have headers and keys lookups.
    _HAS_SCHEMA_REGISTRY = callable(_registry_get_sheet_headers) and callable(_registry_get_sheet_keys)


@lru_cache(maxsize=128)
def _registry_headers(sheet: str) -> List[str]:
    """Get headers from registry."""
    if not _HAS_SCHEMA_REGISTRY or not callable(_registry_get_sheet_headers):
        return []
    try:
        return list(_registry_get_sheet_headers(sheet))
    except Exception:
        return []


@lru_cache(maxsize=128)
def _registry_keys(sheet: str) -> List[str]:
    """Get keys from registry."""
    if not _HAS_SCHEMA_REGISTRY or not callable(_registry_get_sheet_keys):
        return []
    try:
        return list(_registry_get_sheet_keys(sheet))
    except Exception:
        return []


@lru_cache(maxsize=128)
def _contract_for_sheet(sheet: str) -> Tuple[List[str], List[str]]:
    """Get contract for sheet (headers, keys)."""
    canonical = resolve_sheet_key(sheet)
    headers = _registry_headers(canonical)
    keys = _registry_keys(canonical)

    if headers and keys and len(headers) == len(keys):
        return list(headers), list(keys)

    return _FALLBACK_CONTRACTS.get(canonical, _FALLBACK_CONTRACTS["Global_Markets"])


# ---------------------------------------------------------------------------
# Canonical Header Sets
# ---------------------------------------------------------------------------

CANONICAL_STANDARD_HEADERS, CANONICAL_STANDARD_KEYS = _contract_for_sheet("Global_Markets")
CANONICAL_TOP10_HEADERS, CANONICAL_TOP10_KEYS = _contract_for_sheet("Top_10_Investments")
CANONICAL_INSIGHTS_HEADERS, CANONICAL_INSIGHTS_KEYS = _contract_for_sheet("Insights_Analysis")
CANONICAL_DICTIONARY_HEADERS, CANONICAL_DICTIONARY_KEYS = _contract_for_sheet("Data_Dictionary")

# Backward-compatible names (content is canonical now)
ENRICHED_HEADERS_61: List[str] = list(CANONICAL_STANDARD_HEADERS)
DEFAULT_HEADERS_59: List[str] = list(CANONICAL_STANDARD_HEADERS)
DEFAULT_HEADERS_ANALYSIS: List[str] = list(CANONICAL_STANDARD_HEADERS)


# ---------------------------------------------------------------------------
# Field Group Headers
# ---------------------------------------------------------------------------
# v7.1.0: defined by KEY (stable across registry and fallback) then
# translated back to headers via the live CANONICAL_STANDARD_KEYS list.
# v7.0.0 hardcoded header strings that didn't match the fallback contract
# (e.g. "Percent Change" vs actual "Change %"), so `_filter_present`
# silently dropped them.


def _keys_to_headers(keys: Sequence[str], contract_keys: Sequence[str], contract_headers: Sequence[str]) -> List[str]:
    """Translate a list of canonical keys to the matching headers present in the contract."""
    key_to_header = dict(zip(contract_keys, contract_headers))
    return [key_to_header[k] for k in keys if k in key_to_header]


_VN_IDENTITY_KEYS: Sequence[str] = (
    "symbol", "name", "asset_class", "exchange", "currency", "country", "sector", "industry",
    "rank", "origin", "requested_symbol", "symbol_normalized",
)
_VN_PRICE_KEYS: Sequence[str] = (
    "current_price", "previous_close", "open_price", "day_high", "day_low",
    "week_52_high", "week_52_low", "price_change", "percent_change", "week_52_position_pct",
)
_VN_VOLUME_KEYS: Sequence[str] = (
    "volume", "avg_volume_10d", "avg_volume_30d", "liquidity_score", "turnover_pct", "value_traded",
)
_VN_CAP_KEYS: Sequence[str] = (
    "market_cap", "float_shares", "beta_5y",
)
_VN_FUNDAMENTALS_KEYS: Sequence[str] = (
    "pe_ttm", "pe_forward", "eps_ttm", "dividend_yield", "payout_ratio", "revenue_ttm",
    "revenue_growth_yoy", "gross_margin", "operating_margin", "profit_margin", "debt_to_equity",
    "free_cash_flow_ttm", "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio", "intrinsic_value",
    "valuation_score",
)
_VN_TECHNICALS_KEYS: Sequence[str] = (
    "rsi_14", "volatility_30d", "volatility_90d", "max_drawdown_1y", "var_95_1d", "sharpe_1y",
)
_VN_FORECAST_KEYS: Sequence[str] = (
    "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
    "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "forecast_confidence", "forecast_method",
)
_VN_SCORES_KEYS: Sequence[str] = (
    "value_score", "quality_score", "momentum_score", "growth_score", "overall_score",
    "opportunity_score", "rank_overall", "confidence_bucket", "recommendation",
    "recommendation_reason", "risk_score", "risk_bucket",
)
_VN_META_KEYS: Sequence[str] = (
    "data_provider", "data_quality", "last_updated_utc", "last_updated_riyadh", "warnings",
)

VN_IDENTITY: List[str] = _keys_to_headers(_VN_IDENTITY_KEYS, CANONICAL_STANDARD_KEYS, CANONICAL_STANDARD_HEADERS)
VN_PRICE: List[str] = _keys_to_headers(_VN_PRICE_KEYS, CANONICAL_STANDARD_KEYS, CANONICAL_STANDARD_HEADERS)
VN_VOLUME: List[str] = _keys_to_headers(_VN_VOLUME_KEYS, CANONICAL_STANDARD_KEYS, CANONICAL_STANDARD_HEADERS)
VN_CAP: List[str] = _keys_to_headers(_VN_CAP_KEYS, CANONICAL_STANDARD_KEYS, CANONICAL_STANDARD_HEADERS)
VN_FUNDAMENTALS: List[str] = _keys_to_headers(_VN_FUNDAMENTALS_KEYS, CANONICAL_STANDARD_KEYS, CANONICAL_STANDARD_HEADERS)
VN_TECHNICALS: List[str] = _keys_to_headers(_VN_TECHNICALS_KEYS, CANONICAL_STANDARD_KEYS, CANONICAL_STANDARD_HEADERS)
VN_FORECAST: List[str] = _keys_to_headers(_VN_FORECAST_KEYS, CANONICAL_STANDARD_KEYS, CANONICAL_STANDARD_HEADERS)
VN_SCORES: List[str] = _keys_to_headers(_VN_SCORES_KEYS, CANONICAL_STANDARD_KEYS, CANONICAL_STANDARD_HEADERS)
VN_META: List[str] = _keys_to_headers(_VN_META_KEYS, CANONICAL_STANDARD_KEYS, CANONICAL_STANDARD_HEADERS)

VN_HEADERS_GLOBAL: List[str] = list(CANONICAL_STANDARD_HEADERS)
VN_HEADERS_KSA_TADAWUL: List[str] = list(CANONICAL_STANDARD_HEADERS)
VN_HEADERS_INSIGHTS: List[str] = list(CANONICAL_INSIGHTS_HEADERS)

VNEXT_SCHEMAS: Dict[str, Tuple[str, ...]] = {
    "Market_Leaders": tuple(CANONICAL_STANDARD_HEADERS),
    "Global_Markets": tuple(CANONICAL_STANDARD_HEADERS),
    "Commodities_FX": tuple(CANONICAL_STANDARD_HEADERS),
    "Mutual_Funds": tuple(CANONICAL_STANDARD_HEADERS),
    "My_Portfolio": tuple(CANONICAL_STANDARD_HEADERS),
    "Top_10_Investments": tuple(CANONICAL_TOP10_HEADERS),
    "Insights_Analysis": tuple(CANONICAL_INSIGHTS_HEADERS),
    "Data_Dictionary": tuple(CANONICAL_DICTIONARY_HEADERS),
}

LEGACY_SCHEMAS: Dict[str, Tuple[str, ...]] = dict(VNEXT_SCHEMAS)


# ---------------------------------------------------------------------------
# Header to Field Mapping
# ---------------------------------------------------------------------------

@lru_cache(maxsize=4096)
def normalize_header(header: str) -> str:
    """Normalize header for matching."""
    s = safe_str(header).lower()
    if not s:
        return ""

    s = re.sub(r"[^\w\s%]", " ", s)
    s = s.replace("%", " percent ")
    s = re.sub(r"\s+", " ", s).strip()

    replacements = {
        "avg": "average",
        "vol": "volume",
        "mkt": "market",
        "div": "dividend",
        "ttm": "ttm",
        "pe": "pe",
        "pb": "pb",
        "ps": "ps",
        "roe": "roe",
        "roa": "roa",
        "ev": "ev",
        "ebitda": "ebitda",
        "rsi": "rsi",
        "vwap": "vwap",
    }

    parts = [replacements.get(p, p) for p in s.split(" ")]
    return "_".join(parts)


HEADER_TO_FIELD_RAW: Dict[str, str] = {}
for _sheet in CANONICAL_SHEETS:
    _hdrs, _keys = _contract_for_sheet(_sheet)
    for _h, _k in zip(_hdrs, _keys):
        if _h and _k:
            HEADER_TO_FIELD_RAW.setdefault(_h, _k)

HEADER_TO_FIELD_RAW.update({
    "Price": HEADER_TO_FIELD_RAW.get("Current Price", "current_price"),
    "Prev Close": HEADER_TO_FIELD_RAW.get("Previous Close", "previous_close"),
    "Change": HEADER_TO_FIELD_RAW.get("Price Change", "price_change"),
    "Change %": HEADER_TO_FIELD_RAW.get("Percent Change", "percent_change"),
    "P/B": HEADER_TO_FIELD_RAW.get("P/B", "pb_ratio"),
    "P/S": HEADER_TO_FIELD_RAW.get("P/S", "ps_ratio"),
    "Top10 Rank": "top10_rank",
    "Selection Reason": "selection_reason",
    "Criteria Snapshot": "criteria_snapshot",
})

HEADER_TO_FIELD_NORM: Dict[str, str] = {normalize_header(k): v for k, v in HEADER_TO_FIELD_RAW.items()}
FIELD_TO_HEADER: Dict[str, str] = {}
for h, f in HEADER_TO_FIELD_RAW.items():
    FIELD_TO_HEADER.setdefault(f, h)

FIELD_ALIASES: Dict[str, Tuple[str, ...]] = {
    "symbol": ("ticker", "code", "symbol_normalized"),
    "name": ("company_name", "long_name", "title"),
    "country": ("domicile_country",),
    "industry": ("sub_sector", "subsector"),
    "current_price": ("price", "last_price"),
    "previous_close": ("prev_close", "prior_close"),
    "price_change": ("change", "delta_price"),
    "percent_change": ("change_pct", "percent_change_pct", "price_change_pct"),
    "week_52_high": ("high_52w", "fifty_two_week_high"),
    "week_52_low": ("low_52w", "fifty_two_week_low"),
    "week_52_position_pct": ("position_52w_pct",),
    "avg_volume_10d": ("avg_vol_10d",),
    "avg_volume_30d": ("avg_vol_30d", "average_volume"),
    "float_shares": ("free_float_shares",),
    "beta_5y": ("beta",),
    "dividend_yield": ("div_yield",),
    "revenue_yoy_growth": ("revenue_growth",),
    "profit_margin": ("net_margin",),
    "debt_to_equity": ("de_ratio", "debt_equity"),
    "pb_ratio": ("pb",),
    "ps_ratio": ("ps",),
    "peg_ratio": ("peg",),
    "intrinsic_value": ("fair_value",),
    "top10_rank": ("rank_top10",),
}

ALIAS_TO_CANONICAL: Dict[str, str] = {}
for canon, aliases in FIELD_ALIASES.items():
    for alias in aliases:
        ALIAS_TO_CANONICAL[alias] = canon


@lru_cache(maxsize=2048)
def canonical_field(field: str) -> str:
    """Get canonical field name."""
    f = safe_str(field)
    if not f:
        return ""
    return ALIAS_TO_CANONICAL.get(f, f)


@lru_cache(maxsize=2048)
def header_to_field(header: str) -> str:
    """Convert header to field name."""
    if not header:
        return ""

    if header in HEADER_TO_FIELD_RAW:
        return HEADER_TO_FIELD_RAW[header]

    h = normalize_header(header)
    if h in HEADER_TO_FIELD_NORM:
        return HEADER_TO_FIELD_NORM[h]

    return canonical_field(h)


def field_to_header(field: str) -> str:
    """Convert field name to header."""
    f = canonical_field(field)
    return FIELD_TO_HEADER.get(f, f.replace("_", " ").title())


# ---------------------------------------------------------------------------
# Schema Registry Helpers
# ---------------------------------------------------------------------------

_SCHEMA_REGISTRY: Dict[str, Tuple[str, ...]] = {}
_KEY_REGISTRY: Dict[str, Tuple[str, ...]] = {}


def register_schema(
    name: str,
    headers: Sequence[str],
    version: str = "vNext",
    keys: Optional[Sequence[str]] = None,
) -> None:
    """Register a schema."""
    canonical = resolve_sheet_key(name)
    key = f"{version}:{normalize_sheet_name(canonical)}"
    hdrs = tuple([safe_str(h) for h in headers if safe_str(h)])

    if keys is None:
        ks = tuple(header_to_field(h) or normalize_header(h) for h in hdrs)
    else:
        ks = tuple([safe_str(k) for k in keys if safe_str(k)])

    _SCHEMA_REGISTRY[key] = hdrs
    _KEY_REGISTRY[key] = ks


# Register built-in schemas
for _name, _hdrs in VNEXT_SCHEMAS.items():
    register_schema(_name, _hdrs, "vNext", keys=_contract_for_sheet(_name)[1])

for _name, _hdrs in LEGACY_SCHEMAS.items():
    register_schema(_name, _hdrs, "legacy", keys=_contract_for_sheet(_name)[1])


@lru_cache(maxsize=256)
def get_headers_for_sheet(sheet_name: Optional[str] = None, version: str = "vNext") -> List[str]:
    """Get headers for a sheet."""
    canonical = resolve_sheet_key(sheet_name)

    if version in {"vNext", "legacy"}:
        headers, _ = _contract_for_sheet(canonical)
        return list(headers)

    key = f"{version}:{normalize_sheet_name(canonical)}"
    if key in _SCHEMA_REGISTRY:
        return list(_SCHEMA_REGISTRY[key])

    return list(_contract_for_sheet(canonical)[0])


@lru_cache(maxsize=256)
def get_keys_for_sheet(sheet_name: Optional[str] = None, version: str = "vNext") -> List[str]:
    """Get keys for a sheet."""
    canonical = resolve_sheet_key(sheet_name)

    if version in {"vNext", "legacy"}:
        _, keys = _contract_for_sheet(canonical)
        return list(keys)

    key = f"{version}:{normalize_sheet_name(canonical)}"
    if key in _KEY_REGISTRY:
        return list(_KEY_REGISTRY[key])

    return list(_contract_for_sheet(canonical)[1])


def get_supported_sheets(version: str = "vNext") -> List[str]:
    """Get supported sheets."""
    if version in {"vNext", "legacy"}:
        return list(CANONICAL_SHEETS)

    prefix = f"{version}:"
    return sorted({k.split(":", 1)[1] for k in _SCHEMA_REGISTRY if k.startswith(prefix)})


def get_sheet_len(sheet_name: Optional[str] = None, version: str = "vNext") -> int:
    """Get sheet column count."""
    canonical = resolve_sheet_key(sheet_name)

    if callable(_registry_get_sheet_len):
        try:
            return int(_registry_get_sheet_len(canonical))
        except Exception:
            pass

    return len(get_headers_for_sheet(canonical, version=version))


def get_sheet_contract(
    sheet_name: Optional[str] = None,
    version: str = "vNext",
) -> Tuple[List[str], List[str]]:
    """Get sheet contract (headers, keys)."""
    canonical = resolve_sheet_key(sheet_name)
    return get_headers_for_sheet(canonical, version=version), get_keys_for_sheet(canonical, version=version)


def get_field_groups() -> Dict[str, List[str]]:
    """Get field groups."""
    return {
        "Identity": list(VN_IDENTITY),
        "Price": list(VN_PRICE),
        "Volume": list(VN_VOLUME),
        "Capitalization": list(VN_CAP),
        "Fundamentals": list(VN_FUNDAMENTALS),
        "Technicals": list(VN_TECHNICALS),
        "Forecast": list(VN_FORECAST),
        "Scores": list(VN_SCORES),
        "Meta": list(VN_META),
    }


# ---------------------------------------------------------------------------
# Validation Utilities
# ---------------------------------------------------------------------------

def validate_headers(headers: Sequence[str], expected_len: Optional[int] = None) -> Tuple[bool, List[str]]:
    """Validate headers."""
    if not headers:
        return False, ["Headers are empty"]

    errors: List[str] = []

    if expected_len is not None and len(headers) != expected_len:
        errors.append(f"Expected {expected_len} headers, got {len(headers)}")

    seen = set()
    duplicates = []
    for h in headers:
        k = normalize_header(h)
        if k in seen:
            duplicates.append(h)
        else:
            seen.add(k)

    if duplicates:
        errors.append(f"Duplicate headers: {duplicates}")

    return len(errors) == 0, errors


def validate_sheet_data(sheet_name: str, data: Mapping[str, Any]) -> Tuple[bool, List[str]]:
    """Validate sheet data.

    v7.1.0: expected column count is derived from the actual sheet
    contract rather than a hardcoded map that disagreed with the
    registry. v7.0.0 hardcoded 99/112/86/94/110/106/9/9 which caused
    every standard sheet to fail validation when the registry's
    80/80/80/80/80/83/7/9 was active.
    """
    errors: List[str] = []
    canonical = resolve_sheet_key(sheet_name)
    headers, keys = get_sheet_contract(canonical)
    expected = get_sheet_len(canonical)

    ok_headers, header_errors = validate_headers(headers, expected_len=expected)
    if not ok_headers:
        errors.extend(header_errors)

    if canonical not in {"Insights_Analysis", "Data_Dictionary"}:
        required_fields = [keys[0] if keys else "symbol"]

        # v7.1.0: only require the extras actually defined in the registry.
        # v7.0.0 also demanded entry_price, stop_loss_suggested,
        # take_profit_suggested, risk_reward_ratio — none of which are in
        # the 83-col Top_10 contract, so every Top_10 payload failed.
        if canonical == "Top_10_Investments":
            required_fields += ["top10_rank", "selection_reason", "criteria_snapshot"]

        for field_name in required_fields:
            if data.get(field_name) in (None, "", []):
                errors.append(f"Missing required field: {field_name}")

    return len(errors) == 0, errors


# ---------------------------------------------------------------------------
# Request/Response Models
# ---------------------------------------------------------------------------

class BatchProcessRequest(BaseModel):
    """Batch process request model."""
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
            """Validate symbol list."""
            if v is None:
                return []
            if isinstance(v, str):
                v = re.split(r"[,\s\n]+", v)
            if isinstance(v, (list, tuple)):
                return [safe_str(x).upper() for x in v if safe_str(x)]
            return []

        @model_validator(mode="after")
        def _combine(self) -> "BatchProcessRequest":
            """Combine symbols and tickers."""
            self.symbols = sorted(set(self.symbols + self.tickers))
            self.tickers = []
            return self
    else:
        class Config:
            extra = "ignore"

        if validator is not None:
            @validator("symbols", "tickers", pre=True)
            def _validate_symbol_list_v1(cls, v: Any) -> List[str]:  # type: ignore[misc]
                if v is None:
                    return []
                if isinstance(v, str):
                    v = re.split(r"[,\s\n]+", v)
                if isinstance(v, (list, tuple)):
                    return [safe_str(x).upper() for x in v if safe_str(x)]
                return []

        if root_validator is not None:
            @root_validator
            def _combine_v1(cls, values: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore[misc]
                values["symbols"] = sorted(set((values.get("symbols") or []) + (values.get("tickers") or [])))
                values["tickers"] = []
                return values

    def all_symbols(self) -> List[str]:
        """Get all symbols."""
        return list(self.symbols)


class BatchProcessResponse(BaseModel):
    """Batch process response model."""
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


# ---------------------------------------------------------------------------
# Module Exports
# ---------------------------------------------------------------------------

__all__ = [
    "SCHEMAS_VERSION",
    # Enums
    "MarketType",
    "AssetClass",
    "Recommendation",
    "DataQuality",
    "BadgeLevel",
    # Models
    "UnifiedQuote",
    "BatchProcessRequest",
    "BatchProcessResponse",
    # Exceptions
    "SchemaError",
    "SchemaValidationError",
    "ValidationError",          # back-compat alias for SchemaValidationError
    "PydanticValidationError",  # v7.1.0 new: pydantic's ValidationError, exported for callers
    "SheetNotFoundError",
    # Sheets
    "CANONICAL_SHEETS",
    "CANONICAL_STANDARD_HEADERS",
    "CANONICAL_STANDARD_KEYS",
    "CANONICAL_TOP10_HEADERS",
    "CANONICAL_TOP10_KEYS",
    "CANONICAL_INSIGHTS_HEADERS",
    "CANONICAL_INSIGHTS_KEYS",
    "CANONICAL_DICTIONARY_HEADERS",
    "CANONICAL_DICTIONARY_KEYS",
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
    # Header/field mapping
    "normalize_header",
    "canonical_field",
    "header_to_field",
    "field_to_header",
    "HEADER_TO_FIELD_RAW",
    "FIELD_TO_HEADER",
    "FIELD_ALIASES",
    "ALIAS_TO_CANONICAL",
    # Sheet helpers
    "normalize_sheet_name",
    "resolve_sheet_key",
    "register_schema",
    "get_headers_for_sheet",
    "get_keys_for_sheet",
    "get_sheet_len",
    "get_sheet_contract",
    "get_supported_sheets",
    # Validation
    "validate_headers",
    "validate_sheet_data",
    "get_field_groups",
    # Utilities
    "safe_float",
    "safe_int",
    "safe_str",
    "safe_bool",
    "safe_date",
    "safe_datetime",
    "bound_value",
    "percent_to_decimal",
    "decimal_to_percent",
    "normalize_recommendation",
]
