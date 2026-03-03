#!/usr/bin/env python3
# core/sheets/schema_registry.py
"""
================================================================================
Schema Registry — v2.0.0 (CANONICAL / SHEET-FIRST / 60+ COLUMNS GUARANTEED)
================================================================================
Tadawul Fast Bridge (TFB)

This module is the SINGLE source of truth for every sheet’s column schema:
- Exact header order
- Column metadata (group, header, key, dtype, fmt, required, source, notes)
- Sheet registry (Market_Leaders, Global_Markets, Commodities_FX, Mutual_Funds,
  My_Portfolio, Insights_Analysis, Top_10_Investments, Data_Dictionary)

Hard rules:
- No KSA_Tadawul (removed completely)
- "Sheet-rows" endpoints MUST return full schema length using these keys
- Missing values are allowed (null/empty), but columns MUST exist

No network calls. Import-safe.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

__all__ = [
    "SCHEMA_VERSION",
    "ColumnSpec",
    "CriteriaField",
    "SheetSpec",
    "SCHEMA_REGISTRY",
    "list_sheets",
    "get_sheet_spec",
    "get_sheet_columns",
    "get_sheet_headers",
    "validate_schema_registry",
]

SCHEMA_VERSION = "2.0.0"

# -----------------------------
# Types / Models
# -----------------------------

_ALLOWED_DTYPES = {
    "str",        # text
    "float",      # numeric
    "int",        # integer
    "bool",       # boolean
    "pct",        # percent numeric
    "currency",   # currency numeric
    "date",       # date
    "datetime",   # timestamp
    "json",       # stringified json
}

_ALLOWED_KINDS = {
    "instrument_table",  # standard 60+ columns row-per-symbol
    "insights_analysis", # criteria block + insights table
    "data_dictionary",   # auto-generated from schema
}


@dataclass(frozen=True)
class ColumnSpec:
    group: str
    header: str
    key: str
    dtype: str = "str"
    fmt: str = "text"
    required: bool = False
    source: str = ""
    notes: str = ""


@dataclass(frozen=True)
class CriteriaField:
    key: str
    label: str
    dtype: str = "str"
    default: str = ""
    notes: str = ""


@dataclass(frozen=True)
class SheetSpec:
    sheet: str
    kind: str
    columns: Tuple[ColumnSpec, ...]
    criteria_fields: Tuple[CriteriaField, ...] = field(default_factory=tuple)
    notes: str = ""


# -----------------------------
# Canonical columns (60+)
# -----------------------------

def _canonical_instrument_columns() -> Tuple[ColumnSpec, ...]:
    """
    Canonical 60+ columns used by:
    Market_Leaders, Global_Markets, Commodities_FX, Mutual_Funds, My_Portfolio
    and extended for Top_10_Investments.

    Keep keys stable. Add only at the END to preserve compatibility.
    """
    cols: List[ColumnSpec] = []

    def add(
        group: str,
        header: str,
        key: str,
        dtype: str = "str",
        fmt: str = "text",
        required: bool = False,
        source: str = "",
        notes: str = "",
    ) -> None:
        cols.append(ColumnSpec(group, header, key, dtype, fmt, required, source, notes))

    # Identity
    add("Identity", "Symbol", "symbol", "str", "text", True, "input/provider", "Primary instrument identifier.")
    add("Identity", "Name", "name", "str", "text", False, "provider", "Instrument display name.")
    add("Identity", "Asset Class", "asset_class", "str", "text", False, "derived/provider", "Equity, ETF, Fund, FX, Commodity, Index, Crypto.")
    add("Identity", "Exchange", "exchange", "str", "text", False, "provider", "Exchange/mic where available.")
    add("Identity", "Currency", "currency", "str", "text", False, "provider", "Trading currency.")
    add("Identity", "Country", "country", "str", "text", False, "provider", "Issuer / listing country if known.")
    add("Identity", "Sector", "sector", "str", "text", False, "provider", "GICS/sector where available.")
    add("Identity", "Industry", "industry", "str", "text", False, "provider", "Industry where available.")

    # Price (daily)
    add("Price", "Current Price", "current_price", "float", "0.00", False, "provider", "Latest tradable/last price.")
    add("Price", "Previous Close", "previous_close", "float", "0.00", False, "provider", "Prior session close.")
    add("Price", "Open", "open_price", "float", "0.00", False, "provider", "Session open.")
    add("Price", "Day High", "day_high", "float", "0.00", False, "provider", "Session high.")
    add("Price", "Day Low", "day_low", "float", "0.00", False, "provider", "Session low.")
    add("Price", "52W High", "week_52_high", "float", "0.00", False, "provider", "52-week high.")
    add("Price", "52W Low", "week_52_low", "float", "0.00", False, "provider", "52-week low.")
    add("Price", "Price Change", "price_change", "float", "0.00", False, "derived", "current_price - previous_close.")
    add("Price", "Percent Change", "percent_change", "pct", "0.00%", False, "derived", "(current - prev_close)/prev_close.")
    add("Price", "52W Position %", "week_52_position_pct", "pct", "0.00%", False, "derived", "Position within 52-week range.")

    # Liquidity / size
    add("Liquidity", "Volume", "volume", "int", "0", False, "provider", "Latest trading volume.")
    add("Liquidity", "Avg Volume 10D", "avg_volume_10d", "int", "0", False, "provider/derived", "10-day average volume.")
    add("Liquidity", "Avg Volume 30D", "avg_volume_30d", "int", "0", False, "provider/derived", "30-day average volume.")
    add("Liquidity", "Market Cap", "market_cap", "float", "0", False, "provider", "Market capitalization (if applicable).")
    add("Liquidity", "Float Shares", "float_shares", "float", "0", False, "provider", "Free float shares (if applicable).")
    add("Liquidity", "Beta (5Y)", "beta_5y", "float", "0.00", False, "provider", "Beta vs benchmark (if available).")

    # Fundamentals
    add("Fundamentals", "P/E (TTM)", "pe_ttm", "float", "0.00", False, "provider", "Trailing P/E.")
    add("Fundamentals", "P/E (Forward)", "pe_forward", "float", "0.00", False, "provider", "Forward P/E.")
    add("Fundamentals", "EPS (TTM)", "eps_ttm", "float", "0.00", False, "provider", "Trailing EPS.")
    add("Fundamentals", "Dividend Yield", "dividend_yield", "pct", "0.00%", False, "provider", "Trailing dividend yield.")
    add("Fundamentals", "Payout Ratio", "payout_ratio", "pct", "0.00%", False, "provider", "Dividend payout ratio.")
    add("Fundamentals", "Revenue (TTM)", "revenue_ttm", "float", "0", False, "provider", "Trailing revenue.")
    add("Fundamentals", "Revenue Growth YoY", "revenue_growth_yoy", "pct", "0.00%", False, "provider/derived", "YoY revenue growth.")
    add("Fundamentals", "Gross Margin", "gross_margin", "pct", "0.00%", False, "provider", "Gross margin.")
    add("Fundamentals", "Operating Margin", "operating_margin", "pct", "0.00%", False, "provider", "Operating margin.")
    add("Fundamentals", "Profit Margin", "profit_margin", "pct", "0.00%", False, "provider", "Net profit margin.")
    add("Fundamentals", "Debt/Equity", "debt_to_equity", "float", "0.00", False, "provider", "Leverage ratio.")
    add("Fundamentals", "Free Cash Flow (TTM)", "free_cash_flow_ttm", "float", "0", False, "provider", "Trailing FCF.")

    # Technical / Risk
    add("Risk", "RSI (14)", "rsi_14", "float", "0.00", False, "provider/derived", "14-period RSI.")
    add("Risk", "Volatility 30D", "volatility_30d", "pct", "0.00%", False, "provider/derived", "30-day realized volatility.")
    add("Risk", "Volatility 90D", "volatility_90d", "pct", "0.00%", False, "provider/derived", "90-day realized volatility.")
    add("Risk", "Max Drawdown 1Y", "max_drawdown_1y", "pct", "0.00%", False, "derived", "1-year max drawdown.")
    add("Risk", "VaR 95% (1D)", "var_95_1d", "pct", "0.00%", False, "derived", "Historical/parametric VaR proxy.")
    add("Risk", "Sharpe (1Y)", "sharpe_1y", "float", "0.00", False, "derived", "Sharpe proxy (if computed).")
    add("Risk", "Risk Score", "risk_score", "float", "0.00", False, "model", "0-100 or normalized scoring.")
    add("Risk", "Risk Bucket", "risk_bucket", "str", "text", False, "derived", "Low / Moderate / High.")

    # Valuation
    add("Valuation", "P/B", "pb_ratio", "float", "0.00", False, "provider", "Price-to-book.")
    add("Valuation", "P/S", "ps_ratio", "float", "0.00", False, "provider", "Price-to-sales.")
    add("Valuation", "EV/EBITDA", "ev_ebitda", "float", "0.00", False, "provider", "Enterprise value multiple.")
    add("Valuation", "PEG", "peg_ratio", "float", "0.00", False, "provider", "PEG ratio.")
    add("Valuation", "Intrinsic Value", "intrinsic_value", "float", "0.00", False, "model", "Model intrinsic estimate.")
    add("Valuation", "Valuation Score", "valuation_score", "float", "0.00", False, "model", "0-100 or normalized scoring.")

    # Forecast / ROI
    add("Forecast", "Forecast Price 1M", "forecast_price_1m", "float", "0.00", False, "model", "Forecast horizon 1M.")
    add("Forecast", "Forecast Price 3M", "forecast_price_3m", "float", "0.00", False, "model", "Forecast horizon 3M.")
    add("Forecast", "Forecast Price 12M", "forecast_price_12m", "float", "0.00", False, "model", "Forecast horizon 12M.")
    add("Forecast", "Expected ROI 1M", "expected_roi_1m", "pct", "0.00%", False, "model", "(forecast_price_1m/current)-1.")
    add("Forecast", "Expected ROI 3M", "expected_roi_3m", "pct", "0.00%", False, "model", "(forecast_price_3m/current)-1.")
    add("Forecast", "Expected ROI 12M", "expected_roi_12m", "pct", "0.00%", False, "model", "(forecast_price_12m/current)-1.")
    add("Forecast", "Forecast Confidence", "forecast_confidence", "float", "0.00", False, "model", "Forecast confidence 0-1 or 0-100.")
    add("Forecast", "Confidence Score", "confidence_score", "float", "0.00", False, "model", "Scored confidence (normalized).")
    add("Forecast", "Confidence Bucket", "confidence_bucket", "str", "text", False, "derived", "High / Medium / Low.")

    # Scores / Ranking
    add("Scores", "Value Score", "value_score", "float", "0.00", False, "model", "0-100 or normalized.")
    add("Scores", "Quality Score", "quality_score", "float", "0.00", False, "model", "0-100 or normalized.")
    add("Scores", "Momentum Score", "momentum_score", "float", "0.00", False, "model", "0-100 or normalized.")
    add("Scores", "Growth Score", "growth_score", "float", "0.00", False, "model", "0-100 or normalized.")
    add("Scores", "Overall Score", "overall_score", "float", "0.00", False, "model", "Primary combined score.")
    add("Scores", "Opportunity Score", "opportunity_score", "float", "0.00", False, "model", "Composite opportunity score.")
    add("Scores", "Rank (Overall)", "rank_overall", "int", "0", False, "derived", "Rank within page/universe.")

    # Recommendation
    add("Recommendation", "Recommendation", "recommendation", "str", "text", False, "model/derived", "BUY / HOLD / AVOID etc.")
    add("Recommendation", "Recommendation Reason", "recommendation_reason", "str", "text", False, "model", "Short explanation for Apps Script/UI.")
    add("Recommendation", "Horizon Days", "horizon_days", "int", "0", False, "criteria/derived", "Internal horizon in days.")
    add("Recommendation", "Invest Period Label", "invest_period_label", "str", "text", False, "derived", "1M / 3M / 12M label.")

    # Portfolio (mostly for My_Portfolio; safe empty elsewhere)
    add("Portfolio", "Position Qty", "position_qty", "float", "0.00", False, "sheet/user", "Portfolio quantity.")
    add("Portfolio", "Avg Cost", "avg_cost", "float", "0.00", False, "sheet/user", "Average cost per unit.")
    add("Portfolio", "Position Cost", "position_cost", "float", "0.00", False, "derived", "position_qty * avg_cost.")
    add("Portfolio", "Position Value", "position_value", "float", "0.00", False, "derived", "position_qty * current_price.")
    add("Portfolio", "Unrealized P/L", "unrealized_pl", "float", "0.00", False, "derived", "position_value - position_cost.")
    add("Portfolio", "Unrealized P/L %", "unrealized_pl_pct", "pct", "0.00%", False, "derived", "unrealized_pl / position_cost.")

    # Provenance
    add("Provenance", "Data Provider", "data_provider", "str", "text", False, "system", "Primary provider used for the row.")
    add("Provenance", "Last Updated (UTC)", "last_updated_utc", "datetime", "yyyy-mm-dd hh:mm:ss", False, "system", "Last update UTC.")
    add("Provenance", "Last Updated (Riyadh)", "last_updated_riyadh", "datetime", "yyyy-mm-dd hh:mm:ss", False, "system", "Last update Asia/Riyadh.")
    add("Provenance", "Warnings", "warnings", "str", "text", False, "system", "Non-fatal warnings / missing fields summary.")

    return tuple(cols)


_CANONICAL_COLUMNS = _canonical_instrument_columns()


def _top10_extra_columns() -> Tuple[ColumnSpec, ...]:
    return (
        ColumnSpec("Top10", "Top10 Rank", "top10_rank", "int", "0", False, "derived", "Rank within Top 10 selection."),
        ColumnSpec("Top10", "Selection Reason", "selection_reason", "str", "text", False, "model", "Why this row was selected."),
        ColumnSpec("Top10", "Criteria Snapshot", "criteria_snapshot", "json", "text", False, "system", "JSON snapshot of criteria used."),
    )


def _insights_columns() -> Tuple[ColumnSpec, ...]:
    """
    Insights table schema (below criteria block) — keep lightweight and stable.
    You can expand later, but keep compatibility by appending at the end.
    """
    return (
        ColumnSpec("Insights", "Section", "section", "str", "text", True, "system", "e.g., Market Leaders Top 7, Portfolio Summary."),
        ColumnSpec("Insights", "Item", "item", "str", "text", True, "system", "Row label within section."),
        ColumnSpec("Insights", "Symbol", "symbol", "str", "text", False, "derived", "If insight is symbol-specific."),
        ColumnSpec("Insights", "Metric", "metric", "str", "text", False, "system", "Metric name."),
        ColumnSpec("Insights", "Value", "value", "str", "text", False, "system", "String-safe value (Apps Script friendly)."),
        ColumnSpec("Insights", "Notes", "notes", "str", "text", False, "system", "Extra notes / explanation."),
        ColumnSpec("Provenance", "Last Updated (Riyadh)", "last_updated_riyadh", "datetime", "yyyy-mm-dd hh:mm:ss", False, "system", "Timestamp."),
    )


def _insights_criteria_fields() -> Tuple[CriteriaField, ...]:
    """
    Criteria block lives at the TOP of Insights_Analysis (key/value pairs in sheet),
    but we define it here as first-class schema so the backend & Apps Script agree.
    """
    return (
        CriteriaField("risk_level", "Risk Level", "str", "Moderate", "Low / Moderate / High."),
        CriteriaField("confidence_level", "Confidence Level", "str", "High", "High / Medium / Low."),
        CriteriaField("invest_period_days", "Investment Period (Days)", "int", "90", "Always treated in DAYS internally."),
        CriteriaField("required_return_pct", "Required Return %", "pct", "0.10", "Minimum expected ROI threshold."),
        CriteriaField("amount", "Amount", "float", "0", "Investment amount (optional)."),
        CriteriaField("pages_selected", "Pages Selected", "str", "", "CSV of pages to include in Top 10."),
        CriteriaField("min_expected_roi_pct", "Min Expected ROI %", "pct", "0.00", "Filter floor for expected ROI."),
        CriteriaField("max_risk_score", "Max Risk Score", "float", "60", "Filter ceiling for risk score."),
        CriteriaField("min_ai_confidence", "Min AI Confidence", "float", "0.60", "Filter floor for forecast_confidence/confidence_score."),
    )


def _data_dictionary_columns() -> Tuple[ColumnSpec, ...]:
    """
    Data_Dictionary is auto-generated from SCHEMA_REGISTRY via core/sheets/data_dictionary.py
    """
    return (
        ColumnSpec("Dictionary", "Sheet", "sheet", "str", "text", True, "schema_registry", ""),
        ColumnSpec("Dictionary", "Group", "group", "str", "text", True, "schema_registry", ""),
        ColumnSpec("Dictionary", "Header", "header", "str", "text", True, "schema_registry", ""),
        ColumnSpec("Dictionary", "Key", "key", "str", "text", True, "schema_registry", ""),
        ColumnSpec("Dictionary", "DType", "dtype", "str", "text", True, "schema_registry", ""),
        ColumnSpec("Dictionary", "Format", "fmt", "str", "text", False, "schema_registry", ""),
        ColumnSpec("Dictionary", "Required", "required", "bool", "text", False, "schema_registry", ""),
        ColumnSpec("Dictionary", "Source", "source", "str", "text", False, "schema_registry", ""),
        ColumnSpec("Dictionary", "Notes", "notes", "str", "text", False, "schema_registry", ""),
    )


# -----------------------------
# Registry (NO KSA_Tadawul)
# -----------------------------

SCHEMA_REGISTRY: Dict[str, SheetSpec] = {
    "Market_Leaders": SheetSpec(
        sheet="Market_Leaders",
        kind="instrument_table",
        columns=_CANONICAL_COLUMNS,
        notes="Primary watchlist / leaders universe. Canonical schema enforced.",
    ),
    "Global_Markets": SheetSpec(
        sheet="Global_Markets",
        kind="instrument_table",
        columns=_CANONICAL_COLUMNS,
        notes="Global indices/shares. Canonical schema enforced.",
    ),
    "Commodities_FX": SheetSpec(
        sheet="Commodities_FX",
        kind="instrument_table",
        columns=_CANONICAL_COLUMNS,
        notes="Commodities & FX tickers. Canonical schema enforced.",
    ),
    "Mutual_Funds": SheetSpec(
        sheet="Mutual_Funds",
        kind="instrument_table",
        columns=_CANONICAL_COLUMNS,
        notes="Funds/ETFs. Canonical schema enforced.",
    ),
    "My_Portfolio": SheetSpec(
        sheet="My_Portfolio",
        kind="instrument_table",
        columns=_CANONICAL_COLUMNS,
        notes="User portfolio. Portfolio fields may be filled; others optional.",
    ),
    "Insights_Analysis": SheetSpec(
        sheet="Insights_Analysis",
        kind="insights_analysis",
        columns=_insights_columns(),
        criteria_fields=_insights_criteria_fields(),
        notes="Top block: criteria key/value. Below: insights table with stable columns.",
    ),
    "Top_10_Investments": SheetSpec(
        sheet="Top_10_Investments",
        kind="instrument_table",
        columns=_CANONICAL_COLUMNS + _top10_extra_columns(),
        notes="Criteria-driven selection. Canonical columns + Top10 extras.",
    ),
    "Data_Dictionary": SheetSpec(
        sheet="Data_Dictionary",
        kind="data_dictionary",
        columns=_data_dictionary_columns(),
        notes="Auto-built from schema_registry. Do not hand-edit.",
    ),
}


# -----------------------------
# Public helpers
# -----------------------------

def list_sheets() -> List[str]:
    return sorted(SCHEMA_REGISTRY.keys())


def get_sheet_spec(sheet: str) -> SheetSpec:
    try:
        return SCHEMA_REGISTRY[sheet]
    except KeyError as e:
        raise KeyError(f"Unknown sheet '{sheet}'. Known: {', '.join(list_sheets())}") from e


def get_sheet_columns(sheet: str) -> Tuple[ColumnSpec, ...]:
    return get_sheet_spec(sheet).columns


def get_sheet_headers(sheet: str) -> List[str]:
    return [c.header for c in get_sheet_columns(sheet)]


# -----------------------------
# Validation (safe at import-time)
# -----------------------------

def validate_schema_registry(registry: Optional[Dict[str, SheetSpec]] = None) -> None:
    reg = registry or SCHEMA_REGISTRY

    # No forbidden legacy tabs
    if "KSA_Tadawul" in reg:
        raise ValueError("Schema registry must NOT include 'KSA_Tadawul'.")

    # Validate each sheet
    for sheet_name, spec in reg.items():
        if spec.kind not in _ALLOWED_KINDS:
            raise ValueError(f"[{sheet_name}] Invalid kind='{spec.kind}'. Allowed: {sorted(_ALLOWED_KINDS)}")

        if not spec.columns:
            raise ValueError(f"[{sheet_name}] Must define at least 1 column.")

        # Validate dtype + uniqueness
        seen_keys = set()
        seen_headers = set()
        for col in spec.columns:
            if col.dtype not in _ALLOWED_DTYPES:
                raise ValueError(f"[{sheet_name}] Column '{col.key}' invalid dtype='{col.dtype}'. Allowed: {sorted(_ALLOWED_DTYPES)}")
            if col.key in seen_keys:
                raise ValueError(f"[{sheet_name}] Duplicate key: {col.key}")
            if col.header in seen_headers:
                raise ValueError(f"[{sheet_name}] Duplicate header: {col.header}")
            seen_keys.add(col.key)
            seen_headers.add(col.header)

        # Criteria field validation
        seen_ckeys = set()
        for cf in spec.criteria_fields:
            if cf.dtype not in _ALLOWED_DTYPES:
                raise ValueError(f"[{sheet_name}] Criteria '{cf.key}' invalid dtype='{cf.dtype}'. Allowed: {sorted(_ALLOWED_DTYPES)}")
            if cf.key in seen_ckeys:
                raise ValueError(f"[{sheet_name}] Duplicate criteria key: {cf.key}")
            seen_ckeys.add(cf.key)


# Validate immediately (fast, no I/O)
validate_schema_registry()
