#!/usr/bin/env python3
# core/sheets/schema_registry.py
"""
================================================================================
Schema Registry — v2.1.1 (CANONICAL / SHEET-FIRST / DRIFT-PROOF)
================================================================================
Tadawul Fast Bridge (TFB)

This module is the SINGLE source of truth for every sheet’s column schema:
- Exact header order (authoritative)
- Column metadata (group, header, key, dtype, fmt, required, source, notes)
- Sheet registry:
    Market_Leaders, Global_Markets, Commodities_FX, Mutual_Funds,
    My_Portfolio, Insights_Analysis, Top_10_Investments, Data_Dictionary

Hard rules:
- No KSA_Tadawul (removed completely)
- Sheet-rows endpoints MUST return full schema length using these keys
- Missing values are allowed (null/empty), but columns MUST exist
- No network calls. Import-safe.

Special sheets (must NOT fall back to the 80-col instrument schema):
- Insights_Analysis: 7 columns (plus criteria_fields metadata)
- Top_10_Investments: 83 columns (80 canonical + 3 Top10 extras)
- Data_Dictionary: 9 columns (generated from registry)

v2.1.1 FIX (Phase 1 — remove “false missing column”):
- Automatically removes any BLANK/EMPTY column specs (blank key/header) from ALL sheets.
- Enforces instrument_table first column must be Symbol (key="symbol") after sanitization.
This eliminates the “1 missing column” caused by a blank first column in Top_10_Investments spec.

================================================================================
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

__all__ = [
    "SCHEMA_VERSION",
    "ColumnSpec",
    "CriteriaField",
    "SheetSpec",
    "SCHEMA_REGISTRY",
    "CANONICAL_SHEETS",
    "list_sheets",
    "get_sheet_spec",
    "get_sheet_columns",
    "get_sheet_headers",
    "get_sheet_keys",
    "get_sheet_len",
    "get_sheet_key_index",
    "get_sheet_header_index",
    "schema_registry_snapshot",
    "schema_registry_digest",
    "validate_schema_registry",
]

SCHEMA_VERSION = "2.1.1"

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
    "instrument_table",  # standard 80 columns row-per-symbol
    "insights_analysis", # criteria block + insights table (7 cols)
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
# Sanitization (removes blank column specs)
# -----------------------------

def _sanitize_columns(sheet_name: str, kind: str, cols: Sequence[ColumnSpec]) -> Tuple[ColumnSpec, ...]:
    """
    Removes any column specs that have blank key/header (after strip).
    This is the Phase-1 guard to eliminate false “missing column” issues.
    """
    cleaned: List[ColumnSpec] = []
    for c in cols:
        k = (c.key or "").strip()
        h = (c.header or "").strip()
        g = (c.group or "").strip()
        if not g or not k or not h:
            # Drop blank placeholder columns entirely (recommended behavior).
            continue
        cleaned.append(c)

    if kind == "instrument_table":
        # After cleaning, the FIRST column must be symbol.
        if not cleaned:
            raise ValueError(f"[{sheet_name}] instrument_table resulted in 0 columns after sanitization.")
        if cleaned[0].key != "symbol":
            raise ValueError(
                f"[{sheet_name}] instrument_table first column must be key='symbol'. "
                f"Got '{cleaned[0].key}'. (A blank/placeholder column may have existed.)"
            )

    return tuple(cleaned)


def _sanitize_sheet(spec: SheetSpec) -> SheetSpec:
    return SheetSpec(
        sheet=spec.sheet,
        kind=spec.kind,
        columns=_sanitize_columns(spec.sheet, spec.kind, spec.columns),
        criteria_fields=spec.criteria_fields,
        notes=spec.notes,
    )


# -----------------------------
# Canonical columns (80)
# -----------------------------

def _canonical_instrument_columns() -> Tuple[ColumnSpec, ...]:
    """
    Canonical 80 columns used by:
      Market_Leaders, Global_Markets, Commodities_FX, Mutual_Funds, My_Portfolio

    IMPORTANT:
    - Keep keys stable.
    - Add only at the END to preserve compatibility.
    - Do NOT add alias columns (price/change/change_pct) here unless your Excel schema includes them.
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

    # Identity (8)
    add("Identity", "Symbol", "symbol", "str", "text", True, "input/provider", "Primary instrument identifier.")
    add("Identity", "Name", "name", "str", "text", False, "provider", "Instrument display name.")
    add("Identity", "Asset Class", "asset_class", "str", "text", False, "derived/provider", "Equity, ETF, Fund, FX, Commodity, Index, Crypto.")
    add("Identity", "Exchange", "exchange", "str", "text", False, "provider", "Exchange/MIC where available.")
    add("Identity", "Currency", "currency", "str", "text", False, "provider", "Trading currency.")
    add("Identity", "Country", "country", "str", "text", False, "provider", "Issuer/listing country if known.")
    add("Identity", "Sector", "sector", "str", "text", False, "provider", "GICS/sector where available.")
    add("Identity", "Industry", "industry", "str", "text", False, "provider", "Industry where available.")

    # Price (10) -> total 18
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

    # Liquidity / size (6) -> total 24
    add("Liquidity", "Volume", "volume", "int", "0", False, "provider", "Latest trading volume.")
    add("Liquidity", "Avg Volume 10D", "avg_volume_10d", "int", "0", False, "provider/derived", "10-day average volume.")
    add("Liquidity", "Avg Volume 30D", "avg_volume_30d", "int", "0", False, "provider/derived", "30-day average volume.")
    add("Liquidity", "Market Cap", "market_cap", "float", "0", False, "provider", "Market capitalization (if applicable).")
    add("Liquidity", "Float Shares", "float_shares", "float", "0", False, "provider", "Free float shares (if applicable).")
    add("Liquidity", "Beta (5Y)", "beta_5y", "float", "0.00", False, "provider", "Beta vs benchmark (if available).")

    # Fundamentals (12) -> total 36
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

    # Risk (8) -> total 44
    add("Risk", "RSI (14)", "rsi_14", "float", "0.00", False, "provider/derived", "14-period RSI.")
    add("Risk", "Volatility 30D", "volatility_30d", "pct", "0.00%", False, "provider/derived", "30-day realized volatility.")
    add("Risk", "Volatility 90D", "volatility_90d", "pct", "0.00%", False, "provider/derived", "90-day realized volatility.")
    add("Risk", "Max Drawdown 1Y", "max_drawdown_1y", "pct", "0.00%", False, "derived", "1-year max drawdown.")
    add("Risk", "VaR 95% (1D)", "var_95_1d", "pct", "0.00%", False, "derived", "Historical/parametric VaR proxy.")
    add("Risk", "Sharpe (1Y)", "sharpe_1y", "float", "0.00", False, "derived", "Sharpe proxy (if computed).")
    add("Risk", "Risk Score", "risk_score", "float", "0.00", False, "model", "0-100 or normalized scoring.")
    add("Risk", "Risk Bucket", "risk_bucket", "str", "text", False, "derived", "Low / Moderate / High.")

    # Valuation (6) -> total 50
    add("Valuation", "P/B", "pb_ratio", "float", "0.00", False, "provider", "Price-to-book.")
    add("Valuation", "P/S", "ps_ratio", "float", "0.00", False, "provider", "Price-to-sales.")
    add("Valuation", "EV/EBITDA", "ev_ebitda", "float", "0.00", False, "provider", "Enterprise value multiple.")
    add("Valuation", "PEG", "peg_ratio", "float", "0.00", False, "provider", "PEG ratio.")
    add("Valuation", "Intrinsic Value", "intrinsic_value", "float", "0.00", False, "model", "Model intrinsic estimate.")
    add("Valuation", "Valuation Score", "valuation_score", "float", "0.00", False, "model", "0-100 or normalized scoring.")

    # Forecast (9) -> total 59
    add("Forecast", "Forecast Price 1M", "forecast_price_1m", "float", "0.00", False, "model", "Forecast horizon 1M.")
    add("Forecast", "Forecast Price 3M", "forecast_price_3m", "float", "0.00", False, "model", "Forecast horizon 3M.")
    add("Forecast", "Forecast Price 12M", "forecast_price_12m", "float", "0.00", False, "model", "Forecast horizon 12M.")
    add("Forecast", "Expected ROI 1M", "expected_roi_1m", "pct", "0.00%", False, "model", "(forecast_price_1m/current)-1.")
    add("Forecast", "Expected ROI 3M", "expected_roi_3m", "pct", "0.00%", False, "model", "(forecast_price_3m/current)-1.")
    add("Forecast", "Expected ROI 12M", "expected_roi_12m", "pct", "0.00%", False, "model", "(forecast_price_12m/current)-1.")
    add("Forecast", "Forecast Confidence", "forecast_confidence", "float", "0.00", False, "model", "Forecast confidence 0-1 or 0-100.")
    add("Forecast", "Confidence Score", "confidence_score", "float", "0.00", False, "model", "Scored confidence (normalized).")
    add("Forecast", "Confidence Bucket", "confidence_bucket", "str", "text", False, "derived", "High / Medium / Low.")

    # Scores (7) -> total 66
    add("Scores", "Value Score", "value_score", "float", "0.00", False, "model", "0-100 or normalized.")
    add("Scores", "Quality Score", "quality_score", "float", "0.00", False, "model", "0-100 or normalized.")
    add("Scores", "Momentum Score", "momentum_score", "float", "0.00", False, "model", "0-100 or normalized.")
    add("Scores", "Growth Score", "growth_score", "float", "0.00", False, "model", "0-100 or normalized.")
    add("Scores", "Overall Score", "overall_score", "float", "0.00", False, "model", "Primary combined score.")
    add("Scores", "Opportunity Score", "opportunity_score", "float", "0.00", False, "model", "Composite opportunity score.")
    add("Scores", "Rank (Overall)", "rank_overall", "int", "0", False, "derived", "Rank within page/universe.")

    # Recommendation (4) -> total 70
    add("Recommendation", "Recommendation", "recommendation", "str", "text", False, "model/derived", "BUY / HOLD / AVOID etc.")
    add("Recommendation", "Recommendation Reason", "recommendation_reason", "str", "text", False, "model", "Short explanation for UI.")
    add("Recommendation", "Horizon Days", "horizon_days", "int", "0", False, "criteria/derived", "Internal horizon in days.")
    add("Recommendation", "Invest Period Label", "invest_period_label", "str", "text", False, "derived", "1M / 3M / 12M label.")

    # Portfolio (6) -> total 76
    add("Portfolio", "Position Qty", "position_qty", "float", "0.00", False, "sheet/user", "Portfolio quantity.")
    add("Portfolio", "Avg Cost", "avg_cost", "float", "0.00", False, "sheet/user", "Average cost per unit.")
    add("Portfolio", "Position Cost", "position_cost", "float", "0.00", False, "derived", "position_qty * avg_cost.")
    add("Portfolio", "Position Value", "position_value", "float", "0.00", False, "derived", "position_qty * current_price.")
    add("Portfolio", "Unrealized P/L", "unrealized_pl", "float", "0.00", False, "derived", "position_value - position_cost.")
    add("Portfolio", "Unrealized P/L %", "unrealized_pl_pct", "pct", "0.00%", False, "derived", "unrealized_pl / position_cost.")

    # Provenance (4) -> total 80
    add("Provenance", "Data Provider", "data_provider", "str", "text", False, "system", "Primary provider used for the row.")
    add("Provenance", "Last Updated (UTC)", "last_updated_utc", "datetime", "yyyy-mm-dd hh:mm:ss", False, "system", "Last update UTC.")
    add("Provenance", "Last Updated (Riyadh)", "last_updated_riyadh", "datetime", "yyyy-mm-dd hh:mm:ss", False, "system", "Last update Asia/Riyadh.")
    add("Provenance", "Warnings", "warnings", "str", "text", False, "system", "Non-fatal warnings / missing fields summary.")

    return tuple(cols)


_CANONICAL_COLUMNS = _sanitize_columns("CANONICAL", "instrument_table", _canonical_instrument_columns())


def _top10_extra_columns() -> Tuple[ColumnSpec, ...]:
    # 80 + 3 = 83 columns
    return _sanitize_columns(
        "Top_10_Investments(Top10Extras)",
        "instrument_table",
        (
            ColumnSpec("Top10", "Top10 Rank", "top10_rank", "int", "0", False, "derived", "Rank within Top 10 selection."),
            ColumnSpec("Top10", "Selection Reason", "selection_reason", "str", "text", False, "model", "Why this row was selected."),
            ColumnSpec("Top10", "Criteria Snapshot", "criteria_snapshot", "json", "text", False, "system", "JSON snapshot of criteria used."),
        ),
    )


def _insights_columns() -> Tuple[ColumnSpec, ...]:
    """
    Insights_Analysis table schema (7 columns).
    IMPORTANT: The criteria block is NOT returned as columns here; it is represented
    via criteria_fields metadata (key/value UI block in the sheet).
    """
    cols = (
        ColumnSpec("Insights", "Section", "section", "str", "text", True, "system", "e.g., Market Leaders Top 7, Portfolio Summary."),
        ColumnSpec("Insights", "Item", "item", "str", "text", True, "system", "Row label within section."),
        ColumnSpec("Insights", "Symbol", "symbol", "str", "text", False, "derived", "If insight is symbol-specific."),
        ColumnSpec("Insights", "Metric", "metric", "str", "text", False, "system", "Metric name."),
        ColumnSpec("Insights", "Value", "value", "str", "text", False, "system", "String-safe value (Apps Script friendly)."),
        ColumnSpec("Insights", "Notes", "notes", "str", "text", False, "system", "Extra notes / explanation."),
        ColumnSpec("Provenance", "Last Updated (Riyadh)", "last_updated_riyadh", "datetime", "yyyy-mm-dd hh:mm:ss", False, "system", "Timestamp."),
    )
    # For insights, allow symbol not first; do NOT enforce instrument-table rule.
    return _sanitize_columns("Insights_Analysis", "insights_analysis", cols)


def _insights_criteria_fields() -> Tuple[CriteriaField, ...]:
    """
    Criteria block lives at the TOP of Insights_Analysis (key/value pairs in sheet),
    defined here so backend + Apps Script agree.
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
    Expected length: 9 columns.
    """
    cols = (
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
    return _sanitize_columns("Data_Dictionary", "data_dictionary", cols)


# -----------------------------
# Registry (NO KSA_Tadawul)
# -----------------------------

_RAW_SCHEMA_REGISTRY: Dict[str, SheetSpec] = {
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
        columns=_insights_columns(),               # 7 columns
        criteria_fields=_insights_criteria_fields(),
        notes="Top block: criteria key/value. Below: insights table with stable columns.",
    ),
    "Top_10_Investments": SheetSpec(
        sheet="Top_10_Investments",
        kind="instrument_table",
        columns=_sanitize_columns(
            "Top_10_Investments",
            "instrument_table",
            _CANONICAL_COLUMNS + _top10_extra_columns(),  # 83 columns target
        ),
        notes="Criteria-driven selection. Canonical columns + Top10 extras.",
    ),
    "Data_Dictionary": SheetSpec(
        sheet="Data_Dictionary",
        kind="data_dictionary",
        columns=_data_dictionary_columns(),        # 9 columns
        notes="Auto-built from schema_registry. Do not hand-edit.",
    ),
}

# Final sanitized registry (guarantees no blank columns)
SCHEMA_REGISTRY: Dict[str, SheetSpec] = {k: _sanitize_sheet(v) for k, v in _RAW_SCHEMA_REGISTRY.items()}

# A stable “canonical list” for tests and UI ordering (single view)
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


# -----------------------------
# Public helpers
# -----------------------------

def list_sheets() -> List[str]:
    # keep stable ordering where possible
    if all(s in SCHEMA_REGISTRY for s in CANONICAL_SHEETS):
        return list(CANONICAL_SHEETS)
    return sorted(SCHEMA_REGISTRY.keys())


def get_sheet_spec(sheet: str) -> SheetSpec:
    s = (sheet or "").strip()
    try:
        return SCHEMA_REGISTRY[s]
    except KeyError as e:
        raise KeyError(f"Unknown sheet '{s}'. Known: {', '.join(list_sheets())}") from e


def get_sheet_columns(sheet: str) -> Tuple[ColumnSpec, ...]:
    return get_sheet_spec(sheet).columns


def get_sheet_headers(sheet: str) -> List[str]:
    return [c.header for c in get_sheet_columns(sheet)]


def get_sheet_keys(sheet: str) -> List[str]:
    return [c.key for c in get_sheet_columns(sheet)]


def get_sheet_len(sheet: str) -> int:
    return len(get_sheet_columns(sheet))


# -----------------------------
# Fast index maps (precomputed)
# -----------------------------

_KEY_INDEX: Dict[str, Dict[str, int]] = {}
_HEADER_INDEX: Dict[str, Dict[str, int]] = {}


def _build_indexes() -> None:
    _KEY_INDEX.clear()
    _HEADER_INDEX.clear()
    for s, spec in SCHEMA_REGISTRY.items():
        key_map: Dict[str, int] = {}
        header_map: Dict[str, int] = {}
        for i, col in enumerate(spec.columns):
            key_map[col.key] = i
            header_map[col.header] = i
        _KEY_INDEX[s] = key_map
        _HEADER_INDEX[s] = header_map


_build_indexes()


def get_sheet_key_index(sheet: str) -> Dict[str, int]:
    s = (sheet or "").strip()
    if s in _KEY_INDEX:
        return _KEY_INDEX[s]
    _ = get_sheet_spec(s)
    _build_indexes()
    return _KEY_INDEX.get(s, {})


def get_sheet_header_index(sheet: str) -> Dict[str, int]:
    s = (sheet or "").strip()
    if s in _HEADER_INDEX:
        return _HEADER_INDEX[s]
    _ = get_sheet_spec(s)
    _build_indexes()
    return _HEADER_INDEX.get(s, {})


# -----------------------------
# Snapshot + Digest (drift guard)
# -----------------------------

def schema_registry_snapshot() -> Dict[str, Dict[str, Any]]:
    """
    A compact, deterministic snapshot of the registry for tests/CI.
    """
    out: Dict[str, Dict[str, Any]] = {}
    for s in list_sheets():
        spec = SCHEMA_REGISTRY[s]
        out[s] = {
            "kind": spec.kind,
            "len": len(spec.columns),
            "headers": [c.header for c in spec.columns],
            "keys": [c.key for c in spec.columns],
            "dtypes": [c.dtype for c in spec.columns],
            "criteria_fields": [cf.key for cf in spec.criteria_fields],
            "notes": spec.notes,
        }
    return out


def schema_registry_digest() -> str:
    """
    Deterministic hash for drift detection.
    Use this in logs/health endpoints or tests if needed.
    """
    payload = {
        "schema_version": SCHEMA_VERSION,
        "sheets": schema_registry_snapshot(),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


# -----------------------------
# Validation (safe at import-time)
# -----------------------------

def validate_schema_registry(registry: Optional[Dict[str, SheetSpec]] = None) -> None:
    reg = registry or SCHEMA_REGISTRY

    # No forbidden legacy tabs
    if "KSA_Tadawul" in reg:
        raise ValueError("Schema registry must NOT include 'KSA_Tadawul'.")

    # Ensure all canonical sheets exist (if we declared them)
    for s in CANONICAL_SHEETS:
        if s not in reg:
            raise ValueError(f"Missing required sheet in registry: {s}")

    # Validate each sheet
    for sheet_name, spec in reg.items():
        sn = (sheet_name or "").strip()
        if not sn:
            raise ValueError("Schema registry contains an empty sheet key.")

        if spec.sheet != sheet_name:
            raise ValueError(f"[{sheet_name}] spec.sheet mismatch: '{spec.sheet}' != '{sheet_name}'")

        if spec.kind not in _ALLOWED_KINDS:
            raise ValueError(f"[{sheet_name}] Invalid kind='{spec.kind}'. Allowed: {sorted(_ALLOWED_KINDS)}")

        if not spec.columns:
            raise ValueError(f"[{sheet_name}] Must define at least 1 column.")

        # Validate dtype + uniqueness + non-empty fields
        seen_keys = set()
        seen_headers = set()
        for col in spec.columns:
            if not (col.group or "").strip():
                raise ValueError(f"[{sheet_name}] Column group is empty for key='{col.key}'")
            if not (col.header or "").strip():
                raise ValueError(f"[{sheet_name}] Column header is empty for key='{col.key}'")
            if not (col.key or "").strip():
                raise ValueError(f"[{sheet_name}] Column key is empty for header='{col.header}'")

            if col.dtype not in _ALLOWED_DTYPES:
                raise ValueError(f"[{sheet_name}] Column '{col.key}' invalid dtype='{col.dtype}'. Allowed: {sorted(_ALLOWED_DTYPES)}")

            if col.key in seen_keys:
                raise ValueError(f"[{sheet_name}] Duplicate key: {col.key}")
            if col.header in seen_headers:
                raise ValueError(f"[{sheet_name}] Duplicate header: {col.header}")

            seen_keys.add(col.key)
            seen_headers.add(col.header)

        # Special expected sizes (drift-proof)
        if sheet_name in {"Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds", "My_Portfolio"}:
            if len(spec.columns) != 80:
                raise ValueError(f"[{sheet_name}] instrument_table must be 80 columns. Got: {len(spec.columns)}")
            if spec.columns[0].key != "symbol":
                raise ValueError(f"[{sheet_name}] First column must be 'symbol'.")

        if sheet_name == "Top_10_Investments":
            if len(spec.columns) != 83:
                raise ValueError(f"[Top_10_Investments] must be 83 columns (80 + 3). Got: {len(spec.columns)}")
            if spec.columns[0].key != "symbol":
                raise ValueError("[Top_10_Investments] First column must be 'symbol' (no blank leading column).")

        if sheet_name == "Insights_Analysis":
            if len(spec.columns) != 7:
                raise ValueError(f"[Insights_Analysis] must be 7 columns. Got: {len(spec.columns)}")

        if sheet_name == "Data_Dictionary":
            if len(spec.columns) != 9:
                raise ValueError(f"[Data_Dictionary] must be 9 columns. Got: {len(spec.columns)}")

        # Criteria field validation
        seen_ckeys = set()
        for cf in spec.criteria_fields:
            if not (cf.key or "").strip():
                raise ValueError(f"[{sheet_name}] Criteria key is empty")
            if cf.dtype not in _ALLOWED_DTYPES:
                raise ValueError(f"[{sheet_name}] Criteria '{cf.key}' invalid dtype='{cf.dtype}'. Allowed: {sorted(_ALLOWED_DTYPES)}")
            if cf.key in seen_ckeys:
                raise ValueError(f"[{sheet_name}] Duplicate criteria key: {cf.key}")
            seen_ckeys.add(cf.key)


# Validate immediately (fast, no I/O)
validate_schema_registry()
