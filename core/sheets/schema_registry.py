#!/usr/bin/env python3
# core/sheets/schema_registry.py
"""
================================================================================
Schema Registry -- v5.0.0  (PER-PAGE SCHEMAS / CONTRACT-SAFE BACK-COMPAT)
================================================================================

Purpose
-------
Central, schema-first registry for all supported Google Sheets / API pages.
Provides canonical page names, field definitions, display headers, required
keys, row projection, validation, and data-dictionary generation.
Designed to stay lightweight and import-safe for Render / FastAPI startup.
No network I/O, no file I/O, no heavy runtime initialization.

v5.0.0 Changes (from v4.0.0)
----------------------------
**Critical contract fixes**: v4.0.0 was a structural refactor that broke
the external contract used by `core/sheets/data_dictionary.py` and the
`test_schema_alignment.py` regression suite. v5.0.0 preserves v4's
internal architecture (FieldSpec / SheetSchema / per-page granularity)
while exposing the back-compat surface downstream code relies on.

Contract fixes:
  - `SheetSchema.columns` property added (alias for `.fields`). Both
    `data_dictionary.py:_extract_spec_columns` and the regression suite
    read `spec.columns`; v4's `spec.fields` would have silently returned
    an empty column list through the None-fallback path.
  - `FieldSpec` now exposes `dtype`, `fmt`, `source`, `notes` properties
    alongside the v4 `data_type` / `format_hint` names. `data_dictionary`
    and tests read `dtype`/`fmt`/`source`/`notes`; v4 would have emitted
    empty cells for DType, Format, Source, Notes in every DD row.
  - Built-in `source` field added to FieldSpec. v4 had no source
    attribute; the DD contract requires a Source column.
  - `notes` is a proper FieldSpec attribute (mapped from `description`
    for display), matching the DD contract.
  - Data_Dictionary sheet redefined with the canonical 9-column contract
    enforced by the test suite:
        Headers: [Sheet, Group, Header, Key, DType, Format, Required, Source, Notes]
        Keys:    [sheet, group, header, key, dtype, fmt, required, source, notes]
    v4 had: [Page, Field Key, Display Header, Data Type, ...] which would
    fail `test_data_dictionary_matches_schema_registry` immediately.
  - `build_data_dictionary_rows()` emits rows keyed by `sheet`/`key`
    (matching the DD contract). v4 emitted `page`/`field_key` — invisible
    to the test suite's fallback chain which only checks `key/field/
    column_key`.

Bug fixes:
  - `field` shadowing: v4 wrote `for field in schema.fields:` in three
    methods. Since v4 imports AND uses `dataclasses.field(default_factory=
    tuple)`, shadowing was an actual landmine -- a future edit adding
    any `field(...)` call mid-method would NameError. Renamed to `spec`.
  - Column counts in schema descriptions are now auto-computed from the
    assembled field tuples rather than hardcoded in docstrings. v4's
    advertised counts were all off by 1-2 (e.g. "Market_Leaders (99 cols)"
    was actually 100).

Cleanup:
  - Removed `_EXTRA_ALIASES` duplicates already covered by
    `all_page_aliases`.
  - Removed unused imports: `cast`, `Union`.
  - Removed `dataclasses.field` from import (not used externally now
    that alias tuples are plain `tuple()` defaults via _field helper).

Preserved for backward compatibility:
  - All names in __all__.
  - All public function signatures and behaviors.
  - PAGE_ORDER / CANONICAL_SHEETS / SUPPORTED_PAGES / PAGE_ALIASES /
    SCHEMA_REGISTRY surfaces.
  - Module-level field-group constants (_IDENTITY, _PRICE_BASE, etc.).
  - Exported per-page field tuples (MARKET_LEADERS_FIELDS, etc.).
  - Back-compat aliases INSTRUMENT_FIELDS, INSIGHTS_FIELDS, TOP10_EXTRA_FIELDS.
  - Error types PageNotFoundError, InvalidRowError, SchemaRegistryError.
================================================================================
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

# =============================================================================
# Version + Canonical Page Order
# =============================================================================

SCHEMA_VERSION = "5.0.0"
_SCHEMA_VERSION = SCHEMA_VERSION  # v4 compatibility alias

PAGE_ORDER: List[str] = [
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
    "Insights_Analysis",
    "Top_10_Investments",
    "Data_Dictionary",
]

CANONICAL_SHEETS: Tuple[str, ...] = tuple(PAGE_ORDER)


# =============================================================================
# Enums
# =============================================================================

class DataType(str, Enum):
    """Data types for field specifications."""
    STRING = "string"
    NUMBER = "number"
    PERCENT = "percent"
    DATETIME = "datetime"
    BOOLEAN = "boolean"


class PageKind(str, Enum):
    """Page kind classifications."""
    INSTRUMENT_TABLE = "instrument_table"
    GLOBAL_MARKETS_TABLE = "global_markets_table"
    COMMODITIES_TABLE = "commodities_table"
    MUTUAL_FUNDS_TABLE = "mutual_funds_table"
    PORTFOLIO_TABLE = "portfolio_table"
    INSIGHTS_ANALYSIS = "insights_analysis"
    TOP10_OUTPUT = "top10_output"
    DATA_DICTIONARY = "data_dictionary"


# =============================================================================
# Exceptions
# =============================================================================

class SchemaRegistryError(Exception):
    """Base exception for schema registry errors."""


class PageNotFoundError(SchemaRegistryError):
    """Raised when a page is not found in the registry."""


class InvalidRowError(SchemaRegistryError):
    """Raised when a row cannot be projected to schema."""


# =============================================================================
# FieldSpec
# =============================================================================

@dataclass(frozen=True)
class FieldSpec:
    """
    Specification for a single field/column.

    Attributes:
        key: Canonical field key used in code and API.
        header: User-facing column header shown in Google Sheets.
        data_type: Expected data type (string, number, percent, datetime, boolean).
        required: Whether the field is required for the page to function.
        description: Field description and derivation notes.
        group: Logical column grouping.
        format_hint: Suggested Google Sheets format.
        source: Data source / provider label (shown in Data_Dictionary).
        aliases: Alternative accepted input key names.

    Back-compat properties (v5.0.0):
        dtype  -> alias of data_type     (DD generator + test suite read this)
        fmt    -> alias of format_hint   (DD generator + test suite read this)
        notes  -> alias of description   (DD generator displays this as "Notes")
    """
    key: str
    header: str
    data_type: str = DataType.STRING.value
    required: bool = False
    description: str = ""
    group: str = ""
    format_hint: str = ""
    source: str = ""
    aliases: Tuple[str, ...] = ()

    # ----- v5.0.0 back-compat attribute aliases -----
    # Downstream (data_dictionary.py, test_schema_alignment.py) reads these
    # names. Kept as @property so the dataclass stays frozen with a single
    # source of truth for each value.

    @property
    def dtype(self) -> str:
        """Alias of data_type (back-compat with v2/v3 ColumnSpec shape)."""
        return self.data_type

    @property
    def fmt(self) -> str:
        """Alias of format_hint (back-compat with v2/v3 ColumnSpec shape)."""
        return self.format_hint

    @property
    def notes(self) -> str:
        """Alias of description (surfaced as the 'Notes' column in Data_Dictionary)."""
        return self.description

    def as_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses (v4-compatible shape)."""
        return {
            "key": self.key,
            "header": self.header,
            "data_type": self.data_type,
            "dtype": self.data_type,           # back-compat
            "required": self.required,
            "description": self.description,
            "notes": self.description,         # back-compat
            "group": self.group,
            "format_hint": self.format_hint,
            "fmt": self.format_hint,           # back-compat
            "source": self.source,
            "aliases": list(self.aliases),
        }

    def normalized_names(self) -> List[str]:
        """Get normalized names for matching."""
        names = [self.key, self.header, *self.aliases]
        return _deduplicate_keep_order(_normalize(x) for x in names if str(x or "").strip())


# =============================================================================
# SheetSchema
# =============================================================================

@dataclass(frozen=True)
class SheetSchema:
    """
    Schema for a complete sheet/page.

    v5.0.0: exposes `columns` as an alias for `fields` so that downstream
    code (`data_dictionary._extract_spec_columns`, test suite) reading
    `spec.columns` keeps working.
    """
    page: str
    fields: Tuple[FieldSpec, ...]
    description: str = ""
    kind: str = PageKind.INSTRUMENT_TABLE.value
    aliases: Tuple[str, ...] = ()

    # ----- v5.0.0 back-compat property -----
    @property
    def columns(self) -> Tuple[FieldSpec, ...]:
        """Alias of `fields` (back-compat with v2/v3 SheetSpec.columns)."""
        return self.fields

    @property
    def sheet(self) -> str:
        """Alias of `page` (back-compat with v2/v3 SheetSpec.sheet)."""
        return self.page

    @property
    def keys(self) -> List[str]:
        """All field keys in declared order."""
        return [f.key for f in self.fields]

    @property
    def display_headers(self) -> List[str]:
        """All display headers in declared order."""
        return [f.header for f in self.fields]

    @property
    def required_keys(self) -> List[str]:
        """Field keys flagged as required."""
        return [f.key for f in self.fields if f.required]

    @property
    def field_map(self) -> Dict[str, FieldSpec]:
        """Mapping from key to field spec."""
        return {f.key: f for f in self.fields}

    @property
    def column_count(self) -> int:
        """Number of columns in the sheet."""
        return len(self.fields)

    @property
    def all_page_aliases(self) -> List[str]:
        """All page aliases for this sheet (including canonical name)."""
        return _deduplicate_keep_order([
            self.page,
            _normalize(self.page),
            self.page.replace("_", " "),
            *self.aliases,
        ])

    def alias_lookup(self) -> Dict[str, str]:
        """Map of normalized names to field keys."""
        # v5.0.0 fix: loop variable renamed from `field` to `spec` --
        # v4 shadowed the `dataclasses.field` import which is actively
        # used at class-definition time (FieldSpec / SheetSchema).
        mapping: Dict[str, str] = {}
        for spec in self.fields:
            for candidate in spec.normalized_names():
                mapping[candidate] = spec.key
        return mapping

    def as_sheet_spec(self, include_fields: bool = True) -> Dict[str, Any]:
        """Convert to sheet-spec dictionary for API responses."""
        spec: Dict[str, Any] = {
            "page": self.page,
            "sheet": self.page,
            "sheet_name": self.page,
            "description": self.description,
            "kind": self.kind,
            "column_count": self.column_count,
            "keys": self.keys,
            "headers": self.display_headers,
            "display_headers": self.display_headers,
            "required_keys": self.required_keys,
            "aliases": list(self.aliases),
        }
        if include_fields:
            spec["fields"] = [f.as_dict() for f in self.fields]
            spec["columns"] = spec["fields"]  # back-compat
        return spec


# =============================================================================
# Pure Utility Functions
# =============================================================================

def _normalize(value: Any) -> str:
    """
    Normalize page names / keys / headers into a comparison-safe token.

    Examples:
        "Market Leaders"     -> "market_leaders"
        "Top_10_Investments" -> "top_10_investments"
    """
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def _deduplicate_keep_order(values: Iterable[str]) -> List[str]:
    """Deduplicate items while preserving first-seen order."""
    seen: set = set()
    result: List[str] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            result.append(v)
    return result


def _assemble(*groups: Tuple[FieldSpec, ...]) -> Tuple[FieldSpec, ...]:
    """
    Concatenate field tuples, deduplicating by key (first occurrence wins).

    Field groups compose without duplicate keys.
    """
    seen: set = set()
    result: List[FieldSpec] = []
    for group in groups:
        for spec in group:
            if spec.key not in seen:
                seen.add(spec.key)
                result.append(spec)
    return tuple(result)


# =============================================================================
# Field Factory
# =============================================================================

def _field(
    key: str,
    header: str,
    data_type: str = DataType.STRING.value,
    *,
    required: bool = False,
    description: str = "",
    group: str = "",
    format_hint: str = "",
    source: str = "",
    aliases: Sequence[str] = (),
) -> FieldSpec:
    """Create a FieldSpec with sensible defaults."""
    return FieldSpec(
        key=key,
        header=header,
        data_type=data_type,
        required=required,
        description=description,
        group=group,
        format_hint=format_hint,
        source=source,
        aliases=tuple(aliases),
    )


def _schema(
    page: str,
    fields: Sequence[FieldSpec],
    *,
    aliases: Sequence[str] = (),
    description: str = "",
    kind: str = PageKind.INSTRUMENT_TABLE.value,
) -> SheetSchema:
    """Create a SheetSchema."""
    return SheetSchema(
        page=page,
        fields=tuple(fields),
        aliases=tuple(aliases),
        description=description,
        kind=kind,
    )


# =============================================================================
# Shared Field Building Blocks
# =============================================================================

# Identity fields
_IDENTITY: Tuple[FieldSpec, ...] = (
    _field("symbol", "Symbol", required=True, group="Identity",
           description="Ticker / trading symbol.", aliases=("ticker",)),
    _field("name", "Name", required=True, group="Identity",
           description="Instrument or fund name."),
    _field("asset_class", "Asset Class", group="Identity",
           description="Equity, ETF, commodity, FX, fund, etc."),
    _field("exchange", "Exchange", group="Identity",
           description="Primary exchange or market."),
    _field("currency", "Currency", group="Identity",
           description="Trading currency."),
    _field("country", "Country", group="Identity",
           description="Country / market classification."),
    _field("sector", "Sector", group="Identity",
           description="GICS sector classification."),
    _field("industry", "Industry", group="Identity",
           description="GICS industry classification."),
)

# Price base fields
_PRICE_BASE: Tuple[FieldSpec, ...] = (
    _field("current_price", "Current Price", DataType.NUMBER.value, required=True,
           group="Price", format_hint="0.00", aliases=("price", "last_price"),
           description="Latest traded / computed price."),
    _field("previous_close", "Previous Close", DataType.NUMBER.value,
           group="Price", format_hint="0.00",
           description="Prior session close."),
    _field("open", "Open", DataType.NUMBER.value,
           group="Price", format_hint="0.00",
           description="Current session open.", aliases=("open_price",)),
    _field("day_high", "Day High", DataType.NUMBER.value,
           group="Price", format_hint="0.00"),
    _field("day_low", "Day Low", DataType.NUMBER.value,
           group="Price", format_hint="0.00"),
    _field("week_52_high", "52W High", DataType.NUMBER.value,
           group="Price", format_hint="0.00", aliases=("52w_high", "year_high")),
    _field("week_52_low", "52W Low", DataType.NUMBER.value,
           group="Price", format_hint="0.00", aliases=("52w_low", "year_low")),
    _field("price_change", "Price Change", DataType.NUMBER.value,
           group="Price", format_hint="0.00"),
    _field("percent_change", "Change %", DataType.PERCENT.value,
           group="Price", format_hint="0.00%",
           description="Price movement percentage."),
    _field("position_52w_pct", "52W Position %", DataType.PERCENT.value,
           group="Price", format_hint="0.00%",
           description="Position between 52W high and low.",
           aliases=("52w_position_pct", "week_52_position_pct")),
)

# Short-term momentum
_PRICE_NEW: Tuple[FieldSpec, ...] = (
    _field("price_change_5d", "5D Change %", DataType.PERCENT.value,
           group="Price", format_hint="0.00%",
           description="5-day price return — short-term momentum signal."),
)

# Volume / liquidity fields
_VOLUME: Tuple[FieldSpec, ...] = (
    _field("volume", "Volume", DataType.NUMBER.value,
           group="Liquidity", format_hint="#,##0",
           description="Current or latest volume."),
    _field("avg_volume_10d", "Avg Vol 10D", DataType.NUMBER.value,
           group="Liquidity", format_hint="#,##0"),
    _field("avg_volume_30d", "Avg Vol 30D", DataType.NUMBER.value,
           group="Liquidity", format_hint="#,##0"),
    _field("market_cap", "Market Cap", DataType.NUMBER.value,
           group="Liquidity", format_hint="#,##0"),
    _field("float_shares", "Float Shares", DataType.NUMBER.value,
           group="Liquidity", format_hint="#,##0"),
    _field("volume_ratio", "Volume Ratio", DataType.NUMBER.value,
           group="Liquidity", format_hint="0.00",
           description="volume / avg_volume_10d. >1.5 = unusual activity surge."),
)

# Equity fundamentals
_FUNDAMENTALS_EQUITY: Tuple[FieldSpec, ...] = (
    _field("beta_5y", "Beta (5Y)", DataType.NUMBER.value,
           group="Fundamentals", format_hint="0.00"),
    _field("pe_ttm", "P/E (TTM)", DataType.NUMBER.value,
           group="Fundamentals", format_hint="0.00",
           description="Trailing price-to-earnings."),
    _field("pe_forward", "P/E (Fwd)", DataType.NUMBER.value,
           group="Fundamentals", format_hint="0.00",
           description="Forward price-to-earnings."),
    _field("eps_ttm", "EPS (TTM)", DataType.NUMBER.value,
           group="Fundamentals", format_hint="0.00"),
    _field("dividend_yield", "Div Yield %", DataType.PERCENT.value,
           group="Fundamentals", format_hint="0.00%"),
    _field("payout_ratio", "Payout Ratio %", DataType.PERCENT.value,
           group="Fundamentals", format_hint="0.00%"),
    _field("revenue_ttm", "Revenue TTM", DataType.NUMBER.value,
           group="Fundamentals", format_hint="#,##0"),
    _field("revenue_growth_yoy", "Rev Growth YoY %", DataType.PERCENT.value,
           group="Fundamentals", format_hint="0.00%"),
    _field("gross_margin", "Gross Margin %", DataType.PERCENT.value,
           group="Fundamentals", format_hint="0.00%"),
    _field("operating_margin", "Op Margin %", DataType.PERCENT.value,
           group="Fundamentals", format_hint="0.00%"),
    _field("profit_margin", "Net Margin %", DataType.PERCENT.value,
           group="Fundamentals", format_hint="0.00%"),
    _field("debt_to_equity", "D/E Ratio", DataType.NUMBER.value,
           group="Fundamentals", format_hint="0.00"),
    _field("free_cash_flow_ttm", "FCF (TTM)", DataType.NUMBER.value,
           group="Fundamentals", format_hint="#,##0"),
)

# Quality fundamentals
_FUNDAMENTALS_QUALITY: Tuple[FieldSpec, ...] = (
    _field("roe", "ROE %", DataType.PERCENT.value,
           group="Fundamentals", format_hint="0.00%",
           description="Return on equity = net_income / shareholders_equity."),
    _field("roa", "ROA %", DataType.PERCENT.value,
           group="Fundamentals", format_hint="0.00%",
           description="Return on assets = net_income / total_assets."),
)

# Minimal fundamentals (non-equity pages)
_FUNDAMENTALS_MINIMAL: Tuple[FieldSpec, ...] = (
    _field("beta_5y", "Beta (5Y)", DataType.NUMBER.value,
           group="Fundamentals", format_hint="0.00"),
    _field("dividend_yield", "Div Yield %", DataType.PERCENT.value,
           group="Fundamentals", format_hint="0.00%",
           description="Distribution / dividend yield where applicable."),
)

# Risk fields
_RISK: Tuple[FieldSpec, ...] = (
    _field("rsi_14", "RSI (14)", DataType.NUMBER.value,
           group="Risk", format_hint="0.00",
           description="14-period RSI."),
    _field("volatility_30d", "Volatility 30D %", DataType.PERCENT.value,
           group="Risk", format_hint="0.00%"),
    _field("volatility_90d", "Volatility 90D %", DataType.PERCENT.value,
           group="Risk", format_hint="0.00%"),
    _field("max_drawdown_1y", "Max DD 1Y %", DataType.PERCENT.value,
           group="Risk", format_hint="0.00%"),
    _field("var_95_1d", "VaR 95% (1D)", DataType.PERCENT.value,
           group="Risk", format_hint="0.00%"),
    _field("sharpe_1y", "Sharpe (1Y)", DataType.NUMBER.value,
           group="Risk", format_hint="0.00"),
    _field("risk_score", "Risk Score", DataType.NUMBER.value,
           group="Risk", format_hint="0.00"),
    _field("risk_bucket", "Risk Bucket",
           group="Risk",
           description="Low / Moderate / High."),
)

# New technical signals
_TECHNICALS_NEW: Tuple[FieldSpec, ...] = (
    _field("rsi_signal", "RSI Signal", group="Technicals",
           description="Oversold / Neutral / Overbought derived from rsi_14."),
    _field("technical_score", "Tech Score", DataType.NUMBER.value,
           group="Technicals", format_hint="0.00",
           description="Composite short-term technical signal 0-100."),
    _field("day_range_position", "Day Range Pos %", DataType.PERCENT.value,
           group="Technicals", format_hint="0.00%",
           description="(current_price − day_low) / (day_high − day_low)."),
    _field("atr_14", "ATR 14", DataType.NUMBER.value,
           group="Technicals", format_hint="0.00",
           description="14-period Average True Range."),
)

# EODHD technicals (Global_Markets only)
_EODHD_TECHNICALS: Tuple[FieldSpec, ...] = (
    _field("ma_50d", "50D MA", DataType.NUMBER.value,
           group="EODHD Technicals", format_hint="0.00",
           description="50-day simple moving average from EODHD."),
    _field("ma_200d", "200D MA", DataType.NUMBER.value,
           group="EODHD Technicals", format_hint="0.00",
           description="200-day simple moving average from EODHD."),
    _field("ema_signal", "EMA Signal",
           group="EODHD Technicals",
           description="Above / Below — price position relative to 50D EMA."),
    _field("macd_signal", "MACD Signal",
           group="EODHD Technicals",
           description="Bullish / Neutral / Bearish from MACD(12,26,9) crossover."),
)

# Valuation
_VALUATION: Tuple[FieldSpec, ...] = (
    _field("pb", "P/B", DataType.NUMBER.value,
           group="Valuation", format_hint="0.00",
           aliases=("pb_ratio",)),
    _field("ps", "P/S", DataType.NUMBER.value,
           group="Valuation", format_hint="0.00",
           aliases=("ps_ratio",)),
    _field("ev_to_ebitda", "EV/EBITDA", DataType.NUMBER.value,
           group="Valuation", format_hint="0.00",
           aliases=("ev_ebitda",)),
    _field("peg", "PEG Ratio", DataType.NUMBER.value,
           group="Valuation", format_hint="0.00",
           aliases=("peg_ratio",)),
    _field("intrinsic_value", "Intrinsic Value", DataType.NUMBER.value,
           group="Valuation", format_hint="0.00",
           description="Model-derived intrinsic / fair value estimate."),
    _field("valuation_score", "Valuation Score", DataType.NUMBER.value,
           group="Valuation", format_hint="0.00"),
    _field("upside_pct", "Upside %", DataType.PERCENT.value,
           group="Valuation", format_hint="0.00%",
           description="(intrinsic_value − current_price) / current_price."),
)

# Forecast
_FORECAST: Tuple[FieldSpec, ...] = (
    _field("forecast_price_1m", "Price Tgt 1M", DataType.NUMBER.value,
           group="Forecast", format_hint="0.00"),
    _field("forecast_price_3m", "Price Tgt 3M", DataType.NUMBER.value,
           group="Forecast", format_hint="0.00"),
    _field("forecast_price_12m", "Price Tgt 12M", DataType.NUMBER.value,
           group="Forecast", format_hint="0.00"),
    _field("expected_roi_1m", "ROI 1M %", DataType.PERCENT.value,
           group="Forecast", format_hint="0.00%"),
    _field("expected_roi_3m", "ROI 3M %", DataType.PERCENT.value,
           group="Forecast", format_hint="0.00%"),
    _field("expected_roi_12m", "ROI 12M %", DataType.PERCENT.value,
           group="Forecast", format_hint="0.00%"),
    _field("forecast_confidence", "AI Confidence", DataType.NUMBER.value,
           group="Forecast", format_hint="0.00"),
    _field("confidence", "Confidence Score", DataType.NUMBER.value,
           group="Forecast", format_hint="0.00"),
)

# Scoring
_SCORING: Tuple[FieldSpec, ...] = (
    _field("opportunity_score", "Opportunity Score", DataType.NUMBER.value,
           group="Scoring", format_hint="0.00"),
    _field("overall_score", "Overall Score", DataType.NUMBER.value,
           group="Scoring", format_hint="0.00"),
    _field("quality_score", "Quality Score", DataType.NUMBER.value,
           group="Scoring", format_hint="0.00"),
    _field("growth_score", "Growth Score", DataType.NUMBER.value,
           group="Scoring", format_hint="0.00"),
    _field("value_score", "Value Score", DataType.NUMBER.value,
           group="Scoring", format_hint="0.00"),
    _field("momentum_score", "Momentum Score", DataType.NUMBER.value,
           group="Scoring", format_hint="0.00"),
)

# Decision / recommendation
_DECISION: Tuple[FieldSpec, ...] = (
    _field("analyst_rating", "Analyst Rating", group="Decision",
           description="Consensus analyst rating."),
    _field("target_price", "Target Price", DataType.NUMBER.value,
           group="Decision", format_hint="0.00"),
    _field("upside_downside_pct", "Upside/Downside %", DataType.PERCENT.value,
           group="Decision", format_hint="0.00%"),
    _field("recommendation", "Recommendation", group="Decision",
           description="STRONG_BUY / BUY / HOLD / REDUCE / SELL.",
           aliases=("recommend",)),
    _field("signal", "Signal", group="Decision"),
    _field("trend_1m", "Trend 1M", group="Decision"),
    _field("trend_3m", "Trend 3M", group="Decision"),
    _field("trend_12m", "Trend 12M", group="Decision"),
)

# New recommendation fields
_RECOMMENDATION_NEW: Tuple[FieldSpec, ...] = (
    _field("short_term_signal", "ST Signal", group="Decision",
           description="Day / week direction: BUY / HOLD / SELL."),
    _field("recommendation_reason", "Reason", group="Decision",
           description="One-line explanation for the recommendation."),
    _field("confidence_bucket", "Confidence", group="Decision",
           description="High / Medium / Low confidence bucket."),
    _field("invest_period_label", "Horizon", group="Decision",
           description="1D / 1W / 1M / 3M / 12M investment horizon label."),
)

# Provenance
_PROVENANCE: Tuple[FieldSpec, ...] = (
    _field("provider_primary", "Provider", group="Provenance",
           aliases=("provider", "data_provider"),
           description="Primary data provider for this row."),
    _field("provider_secondary", "Provider Secondary", group="Provenance"),
    _field("data_quality_score", "Data Quality Score", DataType.NUMBER.value,
           group="Provenance", format_hint="0.00",
           description="Row-level data completeness / quality score."),
    _field("as_of_utc", "As Of (UTC)", DataType.DATETIME.value, group="Provenance"),
    _field("as_of_riyadh", "As Of (Riyadh)", DataType.DATETIME.value, group="Provenance",
           aliases=("updated_at_riyadh",)),
    _field("updated_at_utc", "Updated At (UTC)", DataType.DATETIME.value, group="Provenance"),
    _field("updated_at_riyadh", "Updated At (Riyadh)", DataType.DATETIME.value, group="Provenance"),
    _field("row_source", "Row Source", group="Provenance",
           description="Source pipeline / builder used for the row."),
)

# Portfolio basic P&L
_PORTFOLIO_BASIC: Tuple[FieldSpec, ...] = (
    _field("position_qty", "Qty", DataType.NUMBER.value,
           group="Portfolio", format_hint="0.00",
           description="Position quantity."),
    _field("avg_cost", "Avg Cost", DataType.NUMBER.value,
           group="Portfolio", format_hint="0.00",
           description="Average cost per unit."),
    _field("position_cost", "Position Cost", DataType.NUMBER.value,
           group="Portfolio", format_hint="0.00",
           description="position_qty × avg_cost."),
    _field("position_value", "Position Value", DataType.NUMBER.value,
           group="Portfolio", format_hint="0.00",
           description="position_qty × current_price."),
    _field("unrealized_pl", "Unrealized P/L", DataType.NUMBER.value,
           group="Portfolio", format_hint="0.00",
           description="position_value − position_cost."),
    _field("unrealized_pl_pct", "Unrealized P/L %", DataType.PERCENT.value,
           group="Portfolio", format_hint="0.00%"),
)

# Portfolio advanced management (My_Portfolio only)
_PORTFOLIO_ADVANCED: Tuple[FieldSpec, ...] = (
    _field("portfolio_weight_pct", "Weight %", DataType.PERCENT.value,
           group="Portfolio Management", format_hint="0.00%",
           description="position_value / total_portfolio_value."),
    _field("target_weight_pct", "Target Weight %", DataType.PERCENT.value,
           group="Portfolio Management", format_hint="0.00%",
           description="User-defined target allocation percentage."),
    _field("weight_deviation", "Weight Deviation %", DataType.PERCENT.value,
           group="Portfolio Management", format_hint="0.00%",
           description="actual_weight − target_weight."),
    _field("rebalance_signal", "Rebal Signal", group="Portfolio Management",
           description="Add / On-Target / Trim based on weight deviation."),
    _field("stop_loss", "Stop Loss", DataType.NUMBER.value,
           group="Risk Management", format_hint="0.00",
           description="User-defined stop loss price."),
    _field("take_profit", "Take Profit", DataType.NUMBER.value,
           group="Risk Management", format_hint="0.00",
           description="User-defined take profit price."),
    _field("distance_to_sl_pct", "Dist to SL %", DataType.PERCENT.value,
           group="Risk Management", format_hint="0.00%",
           description="(current_price − stop_loss) / current_price."),
    _field("distance_to_tp_pct", "Dist to TP %", DataType.PERCENT.value,
           group="Risk Management", format_hint="0.00%",
           description="(take_profit − current_price) / current_price."),
    _field("days_held", "Days Held", DataType.NUMBER.value,
           group="Portfolio Management", format_hint="0",
           description="Days since first purchase."),
    _field("annual_dividend_income", "Ann Div Income", DataType.NUMBER.value,
           group="Portfolio Management", format_hint="0.00",
           description="position_qty × current_price × dividend_yield."),
    _field("beta_contribution", "Beta Contribution", DataType.NUMBER.value,
           group="Risk Management", format_hint="0.00",
           description="portfolio_weight_pct × beta_5y."),
)

# Global context (Global_Markets only)
_GLOBAL_CONTEXT: Tuple[FieldSpec, ...] = (
    _field("region", "Region", group="Global Context",
           description="Americas / Europe / Asia-Pacific / MENA / Global EM."),
    _field("market_status", "Market Status", group="Global Context",
           description="Open / Closed / Pre-Market based on exchange timezone."),
    _field("price_usd", "Price (USD)", DataType.NUMBER.value,
           group="Global Context", format_hint="0.00",
           description="Price converted to USD for cross-border comparison."),
)

# Sector analysis (Global_Markets only)
_SECTOR_ANALYSIS: Tuple[FieldSpec, ...] = (
    _field("sector_pe_avg", "Sector P/E Avg", DataType.NUMBER.value,
           group="Sector Analysis", format_hint="0.00",
           description="Average P/E of the stock's own GICS sector."),
    _field("vs_sector_pe_pct", "vs Sector P/E %", DataType.PERCENT.value,
           group="Sector Analysis", format_hint="0.00%",
           description="(stock_PE − sector_PE) / sector_PE × 100."),
    _field("sector_ytd_pct", "Sector YTD %", DataType.PERCENT.value,
           group="Sector Analysis", format_hint="0.00%",
           description="Sector year-to-date return."),
    _field("sector_signal", "Sector Signal", group="Sector Analysis",
           description="Bullish / Neutral / Bearish based on sector momentum."),
    _field("sector_rank", "Sector Rank", DataType.NUMBER.value,
           group="Sector Analysis", format_hint="0",
           description="Stock rank within its sector universe."),
    _field("sector_vs_msci_pct", "Sector vs MSCI %", DataType.PERCENT.value,
           group="Sector Analysis", format_hint="0.00%",
           description="Sector YTD vs MSCI World."),
)

# Comparative performance (Global_Markets only)
_COMPARATIVE: Tuple[FieldSpec, ...] = (
    _field("vs_sp500_ytd", "vs S&P 500 YTD %", DataType.PERCENT.value,
           group="Comparative", format_hint="0.00%",
           description="YTD return relative to S&P 500."),
    _field("vs_msci_world_ytd", "vs MSCI World %", DataType.PERCENT.value,
           group="Comparative", format_hint="0.00%",
           description="YTD return relative to MSCI World index."),
    _field("wall_st_target", "Wall St Target", DataType.NUMBER.value,
           group="Comparative", format_hint="0.00",
           description="Analyst consensus 12-month price target."),
    _field("upside_to_target_pct", "Upside to Tgt %", DataType.PERCENT.value,
           group="Comparative", format_hint="0.00%",
           description="(wall_st_target − current_price) / current_price."),
    _field("analyst_consensus", "Analyst Consensus", group="Comparative",
           description="Buy / Neutral / Sell consensus from analyst community."),
    _field("country_risk", "Country Risk", group="Comparative",
           description="Investment Grade / Speculative Grade / Mixed."),
)

# Commodity-specific (Commodities_FX only)
_COMMODITY_SPECIFIC: Tuple[FieldSpec, ...] = (
    _field("commodity_type", "Asset Type", group="Asset Info",
           description="Metal / Energy / FX Pair / Agriculture / Crypto."),
    _field("contract_expiry", "Contract Expiry", group="Asset Info",
           description="Futures contract expiry (e.g. Jun-2026)."),
    _field("spot_price", "Spot Price", DataType.NUMBER.value,
           group="Asset Info", format_hint="0.00",
           description="Underlying spot price where applicable."),
    _field("usd_correlation", "USD Correlation", DataType.NUMBER.value,
           group="Commodity Signals", format_hint="0.00",
           description="90-day correlation with DXY (USD index)."),
    _field("seasonal_signal", "Seasonal Signal", group="Commodity Signals",
           description="Historical seasonal pattern for current month."),
    _field("carry_rate", "Carry Rate %", DataType.PERCENT.value,
           group="Commodity Signals", format_hint="0.00%",
           description="Interest rate differential for FX pairs."),
)

# Fund info (Mutual_Funds only)
_FUND_INFO: Tuple[FieldSpec, ...] = (
    _field("fund_type", "Fund Type", group="Fund Info",
           description="ETF / Open-End / Closed-End / REIT."),
    _field("benchmark_name", "Benchmark", group="Fund Info",
           description="Index the fund tracks."),
    _field("holdings_count", "Holdings Count", DataType.NUMBER.value,
           group="Fund Info", format_hint="0",
           description="Number of holdings in the fund."),
)

# Fund pricing (Mutual_Funds only)
_FUND_PRICING: Tuple[FieldSpec, ...] = (
    _field("aum", "AUM (B USD)", DataType.NUMBER.value,
           group="Fund Pricing", format_hint="0.00",
           description="Assets under management in USD billions."),
    _field("expense_ratio", "Expense Ratio %", DataType.PERCENT.value,
           group="Fund Pricing", format_hint="0.000%",
           description="Annual expense ratio."),
    _field("nav", "NAV", DataType.NUMBER.value,
           group="Fund Pricing", format_hint="0.00",
           description="Net asset value per share."),
    _field("nav_premium_pct", "NAV Prem/Disc %", DataType.PERCENT.value,
           group="Fund Pricing", format_hint="0.00%",
           description="(price − nav) / nav."),
    _field("distribution_yield", "Distribution Yield %", DataType.PERCENT.value,
           group="Fund Pricing", format_hint="0.00%"),
)

# Fund performance (Mutual_Funds only)
_FUND_PERFORMANCE: Tuple[FieldSpec, ...] = (
    _field("ytd_return", "YTD Return %", DataType.PERCENT.value,
           group="Fund Performance", format_hint="0.00%"),
    _field("return_1y", "1Y Return %", DataType.PERCENT.value,
           group="Fund Performance", format_hint="0.00%"),
    _field("return_3y_ann", "3Y Return (Ann) %", DataType.PERCENT.value,
           group="Fund Performance", format_hint="0.00%",
           description="3-year annualized total return."),
    _field("return_5y_ann", "5Y Return (Ann) %", DataType.PERCENT.value,
           group="Fund Performance", format_hint="0.00%",
           description="5-year annualized total return."),
    _field("tracking_error", "Tracking Error %", DataType.PERCENT.value,
           group="Fund Performance", format_hint="0.00%",
           description="How closely the ETF tracks its benchmark index."),
    _field("alpha_1y", "Alpha (1Y)", DataType.NUMBER.value,
           group="Fund Performance", format_hint="0.00",
           description="Excess return vs benchmark over 1 year."),
)

# Top10 extras
_TOP10_EXTRAS: Tuple[FieldSpec, ...] = (
    _field("top10_rank", "Top 10 Rank", DataType.NUMBER.value, required=True,
           group="Top10", format_hint="0",
           description="Rank within the Top 10 selection (1 = best)."),
    _field("selection_reason", "Selection Reason", required=True,
           group="Top10",
           description="Why this instrument entered the Top 10."),
    _field("criteria_snapshot", "Criteria Snapshot", group="Top10",
           description="Compact criteria / score snapshot at selection time."),
)

# Trade setup (Top_10_Investments only)
_TRADE_SETUP: Tuple[FieldSpec, ...] = (
    _field("entry_price", "Entry Price", DataType.NUMBER.value,
           group="Trade Setup", format_hint="0.00",
           description="AI-suggested entry price based on technical support zone."),
    _field("stop_loss_suggested", "Stop Loss (AI)", DataType.NUMBER.value,
           group="Trade Setup", format_hint="0.00",
           description="AI stop loss = entry − (atr_14 × 1.5)."),
    _field("take_profit_suggested", "Take Profit (AI)", DataType.NUMBER.value,
           group="Trade Setup", format_hint="0.00",
           description="AI take profit = entry + (upside_pct × entry)."),
    _field("risk_reward_ratio", "Risk/Reward", DataType.NUMBER.value,
           group="Trade Setup", format_hint="0.00",
           description="(take_profit − entry) / (entry − stop_loss)."),
)


# =============================================================================
# Per-Page Field Sets
# =============================================================================

MARKET_LEADERS_FIELDS: Tuple[FieldSpec, ...] = _assemble(
    _IDENTITY,
    _PRICE_BASE, _PRICE_NEW,
    _VOLUME,
    _FUNDAMENTALS_EQUITY, _FUNDAMENTALS_QUALITY,
    _RISK, _TECHNICALS_NEW,
    _VALUATION,
    _FORECAST, _SCORING,
    _DECISION, _RECOMMENDATION_NEW,
    _PORTFOLIO_BASIC,
    _PROVENANCE,
)

GLOBAL_MARKETS_FIELDS: Tuple[FieldSpec, ...] = _assemble(
    _IDENTITY,
    _GLOBAL_CONTEXT,
    _PRICE_BASE, _PRICE_NEW,
    _VOLUME,
    _FUNDAMENTALS_EQUITY, _FUNDAMENTALS_QUALITY,
    _RISK, _TECHNICALS_NEW, _EODHD_TECHNICALS,
    _VALUATION,
    _SECTOR_ANALYSIS,
    _COMPARATIVE,
    _FORECAST, _SCORING,
    _DECISION, _RECOMMENDATION_NEW,
    _PROVENANCE,
)

COMMODITIES_FX_FIELDS: Tuple[FieldSpec, ...] = _assemble(
    _IDENTITY,
    _COMMODITY_SPECIFIC,
    _PRICE_BASE, _PRICE_NEW,
    _VOLUME,
    _FUNDAMENTALS_MINIMAL,
    _RISK, _TECHNICALS_NEW,
    _VALUATION,
    _FORECAST, _SCORING,
    _DECISION, _RECOMMENDATION_NEW,
    _PROVENANCE,
)

MUTUAL_FUNDS_FIELDS: Tuple[FieldSpec, ...] = _assemble(
    _IDENTITY,
    _FUND_INFO, _FUND_PRICING, _FUND_PERFORMANCE,
    _PRICE_BASE, _PRICE_NEW,
    _VOLUME,
    _FUNDAMENTALS_MINIMAL,
    _RISK, _TECHNICALS_NEW,
    _VALUATION,
    _FORECAST, _SCORING,
    _DECISION, _RECOMMENDATION_NEW,
    _PROVENANCE,
)

MY_PORTFOLIO_FIELDS: Tuple[FieldSpec, ...] = _assemble(
    _IDENTITY,
    _PRICE_BASE, _PRICE_NEW,
    _VOLUME,
    _FUNDAMENTALS_EQUITY, _FUNDAMENTALS_QUALITY,
    _RISK, _TECHNICALS_NEW,
    _VALUATION,
    _FORECAST, _SCORING,
    _DECISION, _RECOMMENDATION_NEW,
    _PORTFOLIO_BASIC, _PORTFOLIO_ADVANCED,
    _PROVENANCE,
)

# Insights_Analysis — 9 cols
INSIGHTS_ANALYSIS_FIELDS: Tuple[FieldSpec, ...] = (
    _field("section", "Section", required=True, group="Insight",
           description="Market Summary / Top Picks / Risk Alerts / "
                       "Short-Term Opportunities / Portfolio KPIs / Macro Signals."),
    _field("metric", "Metric", required=True, group="Insight",
           description="Metric or KPI being reported."),
    _field("value", "Value", group="Insight",
           description="Metric value (Apps Script friendly string)."),
    _field("unit", "Unit", group="Insight"),
    _field("direction", "Direction", group="Insight",
           description="Positive / Negative / Neutral."),
    _field("signal", "Signal", group="Insight",
           description="BUY / SELL / HOLD / ALERT / INFO — at-a-glance action."),
    _field("priority", "Priority", group="Insight",
           description="High / Medium / Low — what to act on first."),
    _field("commentary", "Commentary", group="Insight",
           description="Narrative explanation / supporting data."),
    _field("updated_at", "Updated At", DataType.DATETIME.value, group="Provenance"),
)

# Top_10_Investments — Market_Leaders + top10 extras + trade setup
TOP10_INVESTMENTS_FIELDS: Tuple[FieldSpec, ...] = _assemble(
    MARKET_LEADERS_FIELDS,
    _TOP10_EXTRAS,
    _TRADE_SETUP,
)

# Data_Dictionary — v5.0.0: canonical 9-column contract enforced by
# `test_data_dictionary_matches_schema_registry` and relied on by
# `core/sheets/data_dictionary.py:data_dictionary_headers/keys`.
# Contract:
#   Headers: [Sheet, Group, Header, Key, DType, Format, Required, Source, Notes]
#   Keys:    [sheet, group, header, key, dtype, fmt, required, source, notes]
DATA_DICTIONARY_FIELDS: Tuple[FieldSpec, ...] = (
    _field("sheet", "Sheet", required=True, group="Dictionary",
           description="Page / sheet name."),
    _field("group", "Group", group="Dictionary",
           description="Logical column grouping."),
    _field("header", "Header", required=True, group="Dictionary",
           description="User-facing column header shown in Google Sheets."),
    _field("key", "Key", required=True, group="Dictionary",
           description="Canonical field key used in code and API."),
    _field("dtype", "DType", required=True, group="Dictionary",
           description="Expected data type: string / number / percent / datetime / boolean."),
    _field("fmt", "Format", group="Dictionary",
           description="Suggested Google Sheets format (e.g. 0.00%, #,##0)."),
    _field("required", "Required", DataType.BOOLEAN.value, group="Dictionary",
           description="Whether the field is required for the page to function."),
    _field("source", "Source", group="Dictionary",
           description="Primary data source / provider for this field."),
    _field("notes", "Notes", group="Dictionary",
           description="Field description, business meaning, and derivation notes."),
)


# =============================================================================
# Back-Compat Aliases for v3.x callers
# =============================================================================

INSTRUMENT_FIELDS: Tuple[FieldSpec, ...] = MARKET_LEADERS_FIELDS
INSIGHTS_FIELDS: Tuple[FieldSpec, ...] = INSIGHTS_ANALYSIS_FIELDS
TOP10_EXTRA_FIELDS: Tuple[FieldSpec, ...] = _TOP10_EXTRAS


# =============================================================================
# Schema Registry
# =============================================================================

def _count_desc(base: str, fields: Tuple[FieldSpec, ...]) -> str:
    """v5.0.0: auto-compute accurate column counts for schema descriptions."""
    return f"{base} ({len(fields)} cols)"


SCHEMA_REGISTRY: Dict[str, SheetSchema] = {
    "Market_Leaders": _schema(
        "Market_Leaders",
        MARKET_LEADERS_FIELDS,
        kind=PageKind.INSTRUMENT_TABLE.value,
        aliases=(
            "market leaders", "marketleaders", "market_leaders",
            "ksa", "ksa_shares", "local", "local_shares",
            "saudi_shares", "tadawul", "saudi leaders", "tasi leaders",
        ),
        description=_count_desc(
            "Primary Saudi & global equity leaders — "
            "full AI scoring + short-term signals.",
            MARKET_LEADERS_FIELDS,
        ),
    ),
    "Global_Markets": _schema(
        "Global_Markets",
        GLOBAL_MARKETS_FIELDS,
        kind=PageKind.GLOBAL_MARKETS_TABLE.value,
        aliases=(
            "global markets", "global_markets", "global",
            "international_markets", "world_markets", "international",
        ),
        description=_count_desc(
            "International equity universe — EODHD-enhanced + "
            "sector analysis + comparative performance.",
            GLOBAL_MARKETS_FIELDS,
        ),
    ),
    "Commodities_FX": _schema(
        "Commodities_FX",
        COMMODITIES_FX_FIELDS,
        kind=PageKind.COMMODITIES_TABLE.value,
        aliases=(
            "commodities fx", "commodities_fx", "commodities",
            "fx", "commodities_and_fx", "commodities & fx",
            "metals", "gold", "oil",
        ),
        description=_count_desc(
            "Commodities & FX pairs — commodity signals, "
            "carry rate, USD correlation.",
            COMMODITIES_FX_FIELDS,
        ),
    ),
    "Mutual_Funds": _schema(
        "Mutual_Funds",
        MUTUAL_FUNDS_FIELDS,
        kind=PageKind.MUTUAL_FUNDS_TABLE.value,
        aliases=(
            "mutual funds", "mutual_funds", "funds",
            "mutualfunds", "etfs", "etf",
            "index funds", "investment funds",
        ),
        description=_count_desc(
            "ETFs & mutual funds — AUM, expense ratio, "
            "NAV premium, multi-period returns.",
            MUTUAL_FUNDS_FIELDS,
        ),
    ),
    "My_Portfolio": _schema(
        "My_Portfolio",
        MY_PORTFOLIO_FIELDS,
        kind=PageKind.PORTFOLIO_TABLE.value,
        aliases=(
            "my portfolio", "my_portfolio", "portfolio",
            "my investments", "my_investments", "investments",
            "holdings", "positions",
        ),
        description=_count_desc(
            "User portfolio — P&L, stop/target, "
            "weight allocation, rebalancing signals.",
            MY_PORTFOLIO_FIELDS,
        ),
    ),
    "Insights_Analysis": _schema(
        "Insights_Analysis",
        INSIGHTS_ANALYSIS_FIELDS,
        kind=PageKind.INSIGHTS_ANALYSIS.value,
        aliases=(
            "insights analysis", "insights_analysis", "insights",
            "analysis", "analysis_insights",
            "ai insights", "market insights",
        ),
        description=_count_desc(
            "AI market intelligence — top picks, risk alerts, "
            "portfolio KPIs, macro signals.",
            INSIGHTS_ANALYSIS_FIELDS,
        ),
    ),
    "Top_10_Investments": _schema(
        "Top_10_Investments",
        TOP10_INVESTMENTS_FIELDS,
        kind=PageKind.TOP10_OUTPUT.value,
        aliases=(
            "top 10 investments", "top_10_investments",
            "top10", "top_10", "top10_investments",
            "top ten investments", "best picks", "top picks",
        ),
        description=_count_desc(
            "AI-selected top 10 — ranked with entry price, "
            "stop loss, take profit, R/R ratio.",
            TOP10_INVESTMENTS_FIELDS,
        ),
    ),
    "Data_Dictionary": _schema(
        "Data_Dictionary",
        DATA_DICTIONARY_FIELDS,
        kind=PageKind.DATA_DICTIONARY.value,
        aliases=(
            "data dictionary", "data_dictionary", "dictionary",
            "schema_dictionary", "column reference", "schema reference",
        ),
        description=_count_desc(
            "Schema reference — all column definitions, sources and formats.",
            DATA_DICTIONARY_FIELDS,
        ),
    ),
}


# =============================================================================
# Page Aliases Index
# =============================================================================

def _build_page_aliases() -> Dict[str, str]:
    """Build the PAGE_ALIASES index from the registry."""
    aliases: Dict[str, str] = {}
    for canonical, schema in SCHEMA_REGISTRY.items():
        for alias in schema.all_page_aliases:
            aliases[_normalize(alias)] = canonical
    # Extra aliases for common variations not already covered by the schema's
    # own alias list.
    extras = [
        ("My_Investments", "My_Portfolio"),
        ("Top 10", "Top_10_Investments"),
    ]
    for alias, target in extras:
        aliases[_normalize(alias)] = target
    return aliases


PAGE_ALIASES: Dict[str, str] = _build_page_aliases()

SUPPORTED_PAGES: List[str] = [p for p in PAGE_ORDER if p in SCHEMA_REGISTRY]


# =============================================================================
# Public API
# =============================================================================

def get_version() -> str:
    """Get schema registry version."""
    return SCHEMA_VERSION


def get_supported_pages() -> List[str]:
    """Get list of supported page names in canonical order."""
    return list(SUPPORTED_PAGES)


def list_pages() -> List[str]:
    """Alias for get_supported_pages()."""
    return get_supported_pages()


def list_sheets() -> List[str]:
    """Alias for get_supported_pages()."""
    return get_supported_pages()


def has_page(page: Optional[str]) -> bool:
    """Check if page exists in registry."""
    return normalize_page_name(page) in SCHEMA_REGISTRY


def page_exists(page: Optional[str]) -> bool:
    """Alias for has_page()."""
    return has_page(page)


def normalize_page_name(page: Optional[str]) -> str:
    """Normalize page name to canonical form; returns '' for unknown pages."""
    if page is None:
        return ""
    canonical = PAGE_ALIASES.get(_normalize(page))
    if canonical:
        return canonical
    raw = str(page).strip()
    return raw if raw in SCHEMA_REGISTRY else ""


def resolve_page_name(page: Optional[str]) -> str:
    """Alias for normalize_page_name()."""
    return normalize_page_name(page)


def get_schema(page: Optional[str]) -> SheetSchema:
    """Get sheet schema for a page; raises PageNotFoundError if unknown."""
    canonical = normalize_page_name(page)
    if not canonical or canonical not in SCHEMA_REGISTRY:
        supported = ", ".join(get_supported_pages())
        raise PageNotFoundError(f"Unknown page: {page!r}. Supported pages: {supported}")
    return SCHEMA_REGISTRY[canonical]


def get_page_schema(page: Optional[str]) -> SheetSchema:
    """Alias for get_schema()."""
    return get_schema(page)


def get_sheet_schema(page: Optional[str]) -> SheetSchema:
    """Alias for get_schema()."""
    return get_schema(page)


def get_keys(page: Optional[str]) -> List[str]:
    """Get field keys for a page."""
    return get_schema(page).keys


def get_sheet_keys(page: Optional[str]) -> List[str]:
    """Alias for get_keys() — back-compat with regression test's lookup."""
    return get_keys(page)


def get_headers(page: Optional[str]) -> List[str]:
    """Get display headers for a page."""
    return get_schema(page).display_headers


def get_sheet_headers(page: Optional[str]) -> List[str]:
    """Alias for get_headers() — back-compat with regression test's lookup."""
    return get_headers(page)


def get_display_headers(page: Optional[str]) -> List[str]:
    """Alias for get_headers()."""
    return get_headers(page)


def get_required_keys(page: Optional[str]) -> List[str]:
    """Get required field keys for a page."""
    return get_schema(page).required_keys


def get_field_specs(page: Optional[str]) -> List[Dict[str, Any]]:
    """Get field specifications as a list of dicts."""
    return [f.as_dict() for f in get_schema(page).fields]


def get_column_count(page: Optional[str]) -> int:
    """Get column count for a page."""
    return get_schema(page).column_count


def get_sheet_spec(page: Optional[str], include_fields: bool = True) -> Dict[str, Any]:
    """Get sheet specification as a dict payload."""
    return get_schema(page).as_sheet_spec(include_fields=include_fields)


def get_sheet_specs(include_fields: bool = True) -> Dict[str, Dict[str, Any]]:
    """Get all sheet specifications keyed by page name."""
    return {p: get_sheet_spec(p, include_fields=include_fields) for p in get_supported_pages()}


def get_pages_payload(include_fields: bool = False) -> Dict[str, Any]:
    """Get payload for /v1/schema/pages."""
    return {
        "ok": True,
        "version": get_version(),
        "count": len(get_supported_pages()),
        "pages": [
            get_sheet_spec(p, include_fields=include_fields)
            for p in get_supported_pages()
        ],
    }


def get_page_kind(page: Optional[str]) -> str:
    """Get kind string for a page (empty string if page unknown)."""
    canonical = normalize_page_name(page)
    if canonical and canonical in SCHEMA_REGISTRY:
        return SCHEMA_REGISTRY[canonical].kind
    return ""


def is_instrument_page(page: Optional[str]) -> bool:
    """Check if page is an instrument page."""
    canonical = normalize_page_name(page)
    return canonical in {
        "Market_Leaders", "Global_Markets", "Commodities_FX",
        "Mutual_Funds", "My_Portfolio", "Top_10_Investments",
    }


def is_special_page(page: Optional[str]) -> bool:
    """Check if page is a special (non-instrument) page."""
    return not is_instrument_page(page)


# =============================================================================
# Row Projection
# =============================================================================

def _row_to_dict_by_position(schema: SheetSchema, row: Sequence[Any]) -> Dict[str, Any]:
    """Convert positional row to dict keyed by schema keys."""
    result: Dict[str, Any] = {}
    row_list = list(row)
    for idx, key in enumerate(schema.keys):
        result[key] = row_list[idx] if idx < len(row_list) else None
    return result


def _row_to_dict_by_name(schema: SheetSchema, row: Mapping[str, Any]) -> Dict[str, Any]:
    """Convert name-keyed row to dict aligned to schema."""
    normalized_incoming: Dict[str, Any] = {}
    for incoming_key, value in row.items():
        normalized_incoming[_normalize(incoming_key)] = value

    # v5.0.0 fix: loop variable renamed from `field` to `spec` to avoid
    # shadowing the `dataclasses.field` import used at class-definition time.
    result: Dict[str, Any] = {}
    for spec in schema.fields:
        matched: Any = None
        for candidate in spec.normalized_names():
            if candidate in normalized_incoming:
                matched = normalized_incoming[candidate]
                break
        result[spec.key] = matched
    return result


def project_row_to_schema(
    page: Optional[str],
    row: Any,
    include_unknown: bool = False,
    preserve_input_on_unknown: bool = False,
) -> Dict[str, Any]:
    """
    Project a row (dict or list) to schema format.

    Args:
        page: Page name.
        row: Input row (dict or list).
        include_unknown: Include unknown fields in output (under key `_unknown`).
        preserve_input_on_unknown: Preserve original unknown field values.

    Returns:
        Projected row dictionary keyed by canonical schema keys.
    """
    schema = get_schema(page)

    if isinstance(row, Mapping):
        projected = _row_to_dict_by_name(schema, row)
        if include_unknown:
            alias_map = schema.alias_lookup()
            unknown = {str(k): v for k, v in row.items() if _normalize(k) not in alias_map}
            if unknown:
                projected["_unknown"] = dict(unknown) if preserve_input_on_unknown else unknown
        return projected

    if isinstance(row, (list, tuple)):
        return _row_to_dict_by_position(schema, row)

    return {key: None for key in schema.keys}


def project_rows_to_schema(
    page: Optional[str],
    rows: Iterable[Any],
    include_unknown: bool = False,
) -> List[Dict[str, Any]]:
    """Project multiple rows to schema format."""
    return [
        project_row_to_schema(page, row, include_unknown=include_unknown)
        for row in (rows or [])
    ]


def project_row(page: Optional[str], row: Any, **kwargs: Any) -> Dict[str, Any]:
    """Alias for project_row_to_schema()."""
    return project_row_to_schema(page, row, **kwargs)


def project_rows(page: Optional[str], rows: Iterable[Any], **kwargs: Any) -> List[Dict[str, Any]]:
    """Alias for project_rows_to_schema()."""
    return project_rows_to_schema(page, rows, **kwargs)


def rows_to_matrix(page: Optional[str], rows: Iterable[Any]) -> List[List[Any]]:
    """Convert rows to 2D matrix aligned to schema key order."""
    schema = get_schema(page)
    projected = project_rows_to_schema(page, rows)
    return [[row.get(key) for key in schema.keys] for row in projected]


def validate_row_against_schema(
    page: Optional[str],
    row: Any,
    strict: bool = False,
) -> Dict[str, Any]:
    """
    Validate a row against the page schema.

    Args:
        page: Page name.
        row: Input row.
        strict: If True, reject unknown fields.

    Returns:
        Validation result dict.
    """
    schema = get_schema(page)
    projected = project_row_to_schema(page, row, include_unknown=True)

    missing = [
        k for k in schema.required_keys
        if projected.get(k) in (None, "", [], {}, ())
    ]

    unexpected: List[str] = []
    if strict and isinstance(row, Mapping):
        alias_map = schema.alias_lookup()
        unexpected = [str(k) for k in row.keys() if _normalize(k) not in alias_map]

    filled = sum(1 for k in schema.keys if projected.get(k) not in (None, "", [], {}, ()))

    return {
        "ok": not missing and not unexpected,
        "page": schema.page,
        "expected_columns": schema.column_count,
        "filled_columns": filled,
        "completeness_ratio_pct": round((filled / max(1, schema.column_count)) * 100, 2),
        "missing_required_keys": missing,
        "unexpected_keys": unexpected,
        "projected_row": {k: projected.get(k) for k in schema.keys},
    }


def validate_row(page: Optional[str], row: Any, strict: bool = False) -> Dict[str, Any]:
    """Alias for validate_row_against_schema()."""
    return validate_row_against_schema(page, row, strict=strict)


# =============================================================================
# Data Dictionary Builders
# =============================================================================

def build_data_dictionary_rows(page: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Build Data_Dictionary rows.

    v5.0.0: emits rows keyed by the canonical Data_Dictionary contract
    (sheet, group, header, key, dtype, fmt, required, source, notes).
    v4.0.0 emitted (page, field_key, display_header, data_type, ...) which
    would fail `test_data_dictionary_matches_schema_registry`.

    Args:
        page: Specific page to document (None = all supported pages).

    Returns:
        List of row dicts conforming to the Data_Dictionary sheet schema.
    """
    pages = [normalize_page_name(page)] if page else get_supported_pages()
    pages = [p for p in pages if p]

    rows: List[Dict[str, Any]] = []
    for page_name in pages:
        schema = get_schema(page_name)
        # v5.0.0 fix: loop variable `spec` (was `field` in v4 -- shadowing bug).
        for spec in schema.fields:
            rows.append({
                "sheet": schema.page,
                "group": spec.group,
                "header": spec.header,
                "key": spec.key,
                "dtype": spec.data_type,
                "fmt": spec.format_hint,
                "required": spec.required,
                "source": spec.source,
                "notes": spec.description,
            })
    return rows


def get_data_dictionary_rows(page: Optional[str] = None) -> List[Dict[str, Any]]:
    """Alias for build_data_dictionary_rows()."""
    return build_data_dictionary_rows(page=page)


def build_data_dictionary_values(
    page: Optional[str] = None,
    include_header_row: bool = True,
) -> List[List[Any]]:
    """
    Build Data_Dictionary as a 2D values array.

    v5.0.0: output order strictly follows the Data_Dictionary sheet's own
    key order from the registry.
    """
    dd_schema = get_schema("Data_Dictionary")
    rows = build_data_dictionary_rows(page=page)
    values: List[List[Any]] = []
    if include_header_row:
        values.append(list(dd_schema.display_headers))
    for row in rows:
        values.append([row.get(k) for k in dd_schema.keys])
    return values


def get_data_dictionary_matrix(page: Optional[str] = None) -> List[List[Any]]:
    """Alias for build_data_dictionary_values() without header row."""
    return build_data_dictionary_values(page=page, include_header_row=False)


# =============================================================================
# Inference + Summary
# =============================================================================

def infer_page_from_headers(headers: Sequence[str]) -> str:
    """
    Infer page name from a sequence of headers.

    Returns the page whose keys+headers have the greatest overlap with the
    normalized input headers; '' if no pages are registered.
    """
    normalized_headers = {_normalize(h) for h in (headers or []) if str(h or "").strip()}
    best_page = ""
    best_score = -1

    for page_name in get_supported_pages():
        schema = get_schema(page_name)
        key_pool = {_normalize(x) for x in schema.keys + schema.display_headers}
        score = len(normalized_headers & key_pool)
        if score > best_score:
            best_page = page_name
            best_score = score

    return best_page


def infer_page_from_rows(rows: Sequence[Any]) -> str:
    """Infer page name from the first row's keys."""
    if not rows:
        return ""
    first_row = rows[0]
    if isinstance(first_row, Mapping):
        return infer_page_from_headers(list(first_row.keys()))
    return ""


def registry_summary() -> Dict[str, Any]:
    """Get a summary of the registry (version + per-page counts/required keys)."""
    return {
        "version": get_version(),
        "page_count": len(get_supported_pages()),
        "pages": {
            page: {
                "column_count": get_column_count(page),
                "required_keys": get_required_keys(page),
                "kind": get_schema(page).kind,
            }
            for page in get_supported_pages()
        },
    }


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    # Version
    "SCHEMA_VERSION",
    "get_version",
    # Page metadata
    "CANONICAL_SHEETS",
    "PAGE_ORDER",
    "PAGE_ALIASES",
    "SUPPORTED_PAGES",
    # Data models
    "FieldSpec",
    "SheetSchema",
    # Enums
    "DataType",
    "PageKind",
    # Exceptions
    "SchemaRegistryError",
    "PageNotFoundError",
    "InvalidRowError",
    # Per-page field tuples
    "MARKET_LEADERS_FIELDS",
    "GLOBAL_MARKETS_FIELDS",
    "COMMODITIES_FX_FIELDS",
    "MUTUAL_FUNDS_FIELDS",
    "MY_PORTFOLIO_FIELDS",
    "INSIGHTS_ANALYSIS_FIELDS",
    "TOP10_INVESTMENTS_FIELDS",
    "DATA_DICTIONARY_FIELDS",
    # Back-compat aliases
    "INSTRUMENT_FIELDS",
    "INSIGHTS_FIELDS",
    "TOP10_EXTRA_FIELDS",
    # Registry
    "SCHEMA_REGISTRY",
    # Public API
    "get_supported_pages",
    "list_pages",
    "list_sheets",
    "has_page",
    "page_exists",
    "normalize_page_name",
    "resolve_page_name",
    "get_schema",
    "get_page_schema",
    "get_sheet_schema",
    "get_keys",
    "get_sheet_keys",
    "get_headers",
    "get_sheet_headers",
    "get_display_headers",
    "get_required_keys",
    "get_field_specs",
    "get_column_count",
    "get_sheet_spec",
    "get_sheet_specs",
    "get_pages_payload",
    "get_page_kind",
    "is_instrument_page",
    "is_special_page",
    # Row projection / validation
    "project_row_to_schema",
    "project_rows_to_schema",
    "project_row",
    "project_rows",
    "rows_to_matrix",
    "validate_row_against_schema",
    "validate_row",
    # DD builders
    "build_data_dictionary_rows",
    "get_data_dictionary_rows",
    "build_data_dictionary_values",
    "get_data_dictionary_matrix",
    # Inference
    "infer_page_from_headers",
    "infer_page_from_rows",
    "registry_summary",
]
