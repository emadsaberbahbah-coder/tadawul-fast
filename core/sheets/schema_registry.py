#!/usr/bin/env python3
# core/sheets/schema_registry.py
"""
================================================================================
Schema Registry — v2.8.0
(CANONICAL / SHEET-FIRST / STARTUP-SAFE / ALIAS-HARDENED / VIEW-AWARE FAMILY /
 INSIGHTS-EXTENDED / DECISION-MATRIX / CANDLESTICK-AWARE /
 CONVICTION-CRITERIA-EXTENDED)
================================================================================
Tadawul Fast Bridge (TFB)

This module is the SINGLE source of truth for every sheet's column schema:
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

Special sheets (must NOT fall back to the canonical instrument schema):
- Insights_Analysis: 7 columns (plus criteria_fields metadata)
- Top_10_Investments: canonical + 3 Top10 extras
- Data_Dictionary: 9 columns (generated from registry)

================================================================================
Cross-module integration (May 2026 cross-stack family)
================================================================================

The four "Views" columns (Fundamental / Technical / Risk / Value) are the
contract between the scoring engine and the recommendation engine:

  - core.scoring v5.2.5 derives them via derive_fundamental_view(),
    derive_technical_view(), derive_risk_view(), derive_value_view() and
    writes them to AssetScores. v5.2.5 also produces the four "Insights"
    enrichment row fields (conviction_score, top_factors, top_risks,
    position_size_hint), the canonical 5-tier recommendation, and the
    scoring_errors field (provider_error:* / engine_dropped_valuation).
  - core.reco_normalize v7.2.0 consumes them via Recommendation.from_views()
    / recommendation_from_views() / recommendation_from_views_with_rule_id()
    to apply the 5-tier priority cascade with EXPENSIVE/double-bearish
    vetoes AND the conviction-floor downgrade. Conviction floors are
    env-tunable via:
        RECO_STRONG_BUY_CONVICTION_FLOOR (default 60.0)
        RECO_BUY_CONVICTION_FLOOR        (default 45.0)
    Below these floors, STRONG_BUY downgrades to BUY and BUY to HOLD.
    v7.2.0 also fixed the AVOID-as-token mapping (now -> SELL, was
    REDUCE) and introduced 14 RULE_ID_* constants for downstream audit.
    Phase D added conviction-badge de-duplication so the conviction
    badge does not appear twice in the recommendation reason when
    upstream callers already include it.

The five "Insights" columns (sector_relative_score, conviction_score,
top_factors, top_risks, position_size_hint, added in v2.6.0) are produced
by core.scoring v5.2.5 as row fields on AssetScores (sector_relative_score
uses an optional sector cohort; the other four are derived from the same
fully-scored row that produces the recommendation). core.insights_builder
v7.0.0 consumes them for the Top Picks section AND surfaces a NEW Data
Quality Alerts section that aggregates the v5.60.0 engine warnings
(engine-dropped valuation tags + forecast_unavailable tags + provider
errors).

The two "Decision Matrix" columns (recommendation_detailed,
recommendation_priority, added in v2.7.0) are produced by
core.data_engine_v2 v5.60.0+ via the 8-tier classifier _classify_8tier().
The canonical 5-tier `recommendation` field is preserved unchanged; the
detailed/priority pair adds richer granularity (STRONG_SELL,
SPECULATIVE_BUY, ACCUMULATE) that collapses back to the canonical token
via _RECOMMENDATION_COLLAPSE_MAP. These two columns may be EMPTY when
`recommendation` is set upstream by core.scoring -> core.reco_normalize,
to avoid implying a detailed verdict that may disagree with the
canonical one.

The five "Candlestick" columns (candlestick_pattern, candlestick_signal,
candlestick_strength, candlestick_confidence, candlestick_patterns_recent,
added in v2.7.0) are produced by core.candlesticks v1.0.0 when invoked
from core.data_engine_v2 v5.60.0+ on OHLC history rows. Detection is
BEST-EFFORT and OPTIONAL (gated by the ENGINE_CANDLESTICKS_ENABLED env
flag); when the candlesticks module is unavailable or detection raises,
these five columns stay null.

The Insights_Analysis CRITERIA block (v2.8.0): the criteria_fields tuple
is the source of truth for the key/value rows rendered at the top of the
Insights_Analysis sheet. core.analysis.criteria_model v3.1.0 reads these
into the validated AdvisorCriteria pydantic model via from_kv_map() and
from_rows(). v2.8.0 adds four entries aligned with criteria_model v3.1.0:
    min_conviction_score             (was: min_conviction in v2.6.0/v2.7.0)
    exclude_engine_dropped_valuation (NEW)
    exclude_forecast_unavailable     (NEW)
    exclude_provider_errors          (NEW)

Upstream warning surface (v5.60.0 engine + v5.2.5 scoring):
  - engine v5.60.0 Phase H/I/P emits 5 engine-dropped-valuation tags
    into the canonicalized `warnings` string ('; '-joined):
      intrinsic_unit_mismatch_suspected,
      upside_synthesis_suspect,
      engine_52w_high_unit_mismatch_dropped,
      engine_52w_low_unit_mismatch_dropped,
      engine_52w_high_low_inverted
  - engine v5.60.0 Phase B emits 4 forecast-unavailable tags + bool
    flag: forecast_unavailable, forecast_unavailable_no_source,
    forecast_cleared_consistency_sweep, forecast_skipped_unavailable
  - engine v5.60.0 Phase Q preserves provider `last_error_class` from
    the eodhd/yahoo_fundamentals/yahoo_chart/argaam/finnhub/tadawul
    providers as an internal row field (NOT a schema column; surfaced
    via the warnings parser helpers in scoring v5.2.5 / insights_builder
    v7.0.0 / top10_selector v4.11.0).

Downstream consumers:
  - core.scoring_engine v3.4.2 (compatibility bridge) tracks
    _V525_ENRICHMENT_FIELDS, _RECO_RULE_ID_SYMBOLS, and exposes
    is_reco_rule_id_aware() for callers that need to detect the
    v7.2.0+ rule_id-aware classification surface.
  - core.analysis.top10_selector v4.11.0 applies soft data-quality
    penalties (env: TOP10_PENALTY_ENGINE_DROP=8.0,
    TOP10_PENALTY_FORECAST_UNAVAIL=10.0) when warnings indicate
    upstream data quality issues, and surfaces a data_quality_summary
    in route response meta.
  - core.analysis.insights_builder v7.0.0 renders the NEW Data Quality
    Alerts section that aggregates these signals across the candidate
    pool.

================================================================================
Changelog
================================================================================

v2.8.0 (current)  --  CONVICTION-CRITERIA-EXTENDED / CROSS-STACK SYNC
- RENAME: criteria_fields key 'min_conviction' -> 'min_conviction_score'
  for alignment with criteria_model v3.1.0's canonical field name.
  Backwards-compatible: criteria_model v3.1.0's KV alias map accepts
  both 'min_conviction' and 'min_conviction_score'.
- NEW criteria_fields entries (v3.1.0 data-quality exclusions):
    exclude_engine_dropped_valuation (bool, default 'false')
    exclude_forecast_unavailable     (bool, default 'false')
    exclude_provider_errors          (bool, default 'false')
- SYNC: docstring updated to reference May 2026 cross-stack family:
    core.scoring v5.1.0 -> v5.2.5
    core.reco_normalize v7.1.0 -> v7.2.0
    core.insights_builder v1.0.0 -> v7.0.0
    core.data_engine_v2 v5.50.0+ -> v5.60.0+
  Plus new references to:
    core.scoring_engine v3.4.2 (compatibility bridge)
    core.analysis.top10_selector v4.11.0 (consumer)
    core.analysis.criteria_model v3.1.0 (criteria data model)
- DOC: corrected attribution for the 5 Insights row fields -- they are
  produced by core.scoring v5.2.5 (not core.insights_builder v1.0.0 as
  the v2.7.0 docstring claimed).
- DOC: documented env-tunable conviction floors
  (RECO_STRONG_BUY_CONVICTION_FLOOR=60, RECO_BUY_CONVICTION_FLOOR=45)
  consumed by reco_normalize v7.2.0 and mirrored in criteria_model
  v3.1.0's get_strong_buy_conviction_floor() / get_buy_conviction_floor()
  helpers.
- DOC: documented the upstream warning surface from engine v5.60.0
  (5 engine-dropped tags, 4 forecast-unavailable tags + bool, plus
  preserved last_error_class as an internal row field).
- NEW EXPORT: __version__ alias = SCHEMA_VERSION (TFB module
  convention used by all v5/v7 family modules).
- COLUMN COUNTS UNCHANGED: 97 instrument, 100 Top10, 7 Insights,
  9 Data_Dictionary. No structural schema change.
- BUMP: SCHEMA_VERSION 2.7.0 -> 2.8.0.

v2.7.0
- NEW: "Decision Matrix" column group (2 columns, appended at END):
    1. recommendation_detailed (str, text, model)
    2. recommendation_priority (int, 0,    model)
  Aligns with core.data_engine_v2 v5.50.0+'s 8-tier Decision Matrix
  classifier. The canonical `recommendation` 5-tier field is unchanged.
- NEW: "Candlestick" column group (5 columns, appended at END):
    1. candlestick_pattern         (str,   text,  derived)
    2. candlestick_signal          (str,   text,  derived)
    3. candlestick_strength        (str,   text,  derived)
    4. candlestick_confidence      (float, 0.00,  derived)
    5. candlestick_patterns_recent (str,   text,  derived)
  Aligns with core.candlesticks v1.0.0 + core.data_engine_v2 v5.50.0+'s
  candlestick wiring. 10 patterns supported (Doji, Hammer, Inverted
  Hammer, Shooting Star, Hanging Man, Bullish/Bearish Marubozu,
  Bullish/Bearish Engulfing, Morning Star, Evening Star).
- BUMP: instrument_table column count 90 -> 97; Top10 93 -> 100.
- NEW EXPORTS: `DECISION_COLUMN_KEYS`, `CANDLESTICK_COLUMN_KEYS`.
- NEW VALIDATION: instrument_table now requires the seven new columns
  (2 Decision + 5 Candlestick) AND all prior required columns (Views,
  Insights). Same enforcement applied to Top_10_Investments.

v2.6.0
- NEW: "Insights" column group at the END of the canonical schema. Five
  columns added (canonical 85 -> 90; Top10 88 -> 93):
    1. sector_relative_score (float, 0-100, model)
    2. conviction_score      (float, 0-100, model)
    3. top_factors           (str,  text,    derived)
    4. top_risks             (str,  text,    derived)
    5. position_size_hint    (str,  text,    derived)
- NEW EXPORT: `INSIGHTS_COLUMN_KEYS`.
- NEW VALIDATION: instrument_table now requires the five Insights columns
  AND the four View columns. Both are enforced at validation time.
- DOC: Cross-module integration note expanded to cover insights_builder.

v2.5.0
- Version aligned with the view-aware recommendation family.
- NEW EXPORT: `VIEW_COLUMN_KEYS`.
- DOC sharpening on `value_view`, `risk_view`, `Upside %`.

v2.4.0
- Added `Upside %` (key: `upside_pct`) to the Valuation group.
- Bumped instrument_table 84 -> 85 and Top10 87 -> 88.

v2.3.0
- Added the four "Views" columns (Fundamental / Technical / Risk / Value).

v2.2.0
- Top10 extras sanitized as a fragment.
- Startup-safe validation (STRICT_SCHEMA_VALIDATION=1 to raise).
- Stable sheet-name aliases.
- Snapshot digest captures groups / formats / required flags.
================================================================================
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

__all__ = [
    "SCHEMA_VERSION",
    "__version__",  # v2.8.0 Phase C: TFB module-version convention alias
    "ColumnSpec",
    "CriteriaField",
    "SheetSpec",
    "SCHEMA_REGISTRY",
    "CANONICAL_SHEETS",
    "VIEW_COLUMN_KEYS",
    "INSIGHTS_COLUMN_KEYS",
    "DECISION_COLUMN_KEYS",
    "CANDLESTICK_COLUMN_KEYS",
    "SCHEMA_VALIDATED_OK",
    "SCHEMA_VALIDATION_ERRORS",
    "list_sheets",
    "get_sheet_spec",
    "has_sheet",
    "normalize_sheet_name",
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

SCHEMA_VERSION = "2.8.0"
# v2.8.0 Phase C: TFB module-version convention alias (mirrors scoring
# v5.2.5, reco_normalize v7.2.0, insights_builder v7.0.0, scoring_engine
# v3.4.2, top10_selector v4.11.0, criteria_model v3.1.0).
__version__ = SCHEMA_VERSION


# Canonical view column keys, exposed for downstream code that needs to
# project/select them out of rows without depending on a hardcoded list.
# The order here mirrors the column order in the canonical schema.
VIEW_COLUMN_KEYS: Tuple[str, ...] = (
    "fundamental_view",
    "technical_view",
    "risk_view",
    "value_view",
)


# v2.6.0: Insights column keys, mirror role of VIEW_COLUMN_KEYS for the
# new "Insights" column group. Order follows the column order in the
# canonical schema. Produced by core.scoring v5.2.5 as row fields on
# AssetScores (sector_relative_score additionally uses an optional
# sector cohort).
INSIGHTS_COLUMN_KEYS: Tuple[str, ...] = (
    "sector_relative_score",
    "conviction_score",
    "top_factors",
    "top_risks",
    "position_size_hint",
)


# v2.7.0: Decision Matrix column keys, produced by core.data_engine_v2
# v5.60.0+'s 8-tier classifier. The canonical 5-tier `recommendation`
# field is preserved unchanged; this pair adds richer detail and a
# numeric priority. May be EMPTY when `recommendation` is upstream-set.
DECISION_COLUMN_KEYS: Tuple[str, ...] = (
    "recommendation_detailed",
    "recommendation_priority",
)


# v2.7.0: Candlestick column keys, produced by core.candlesticks v1.0.0
# when invoked from core.data_engine_v2 v5.60.0+ on OHLC history rows.
# Detection is best-effort and may be disabled via the
# ENGINE_CANDLESTICKS_ENABLED env flag, in which case these stay null.
CANDLESTICK_COLUMN_KEYS: Tuple[str, ...] = (
    "candlestick_pattern",
    "candlestick_signal",
    "candlestick_strength",
    "candlestick_confidence",
    "candlestick_patterns_recent",
)


# -----------------------------
# Types / Models
# -----------------------------

_ALLOWED_DTYPES = {
    "str",        # text
    "float",      # numeric
    "int",        # integer
    "bool",       # boolean
    "pct",        # percent numeric (fraction is recommended: 0.25 == 25%)
    "currency",   # currency numeric
    "date",       # date
    "datetime",   # timestamp
    "json",       # stringified json
}

_ALLOWED_KINDS = {
    "instrument_table",   # standard canonical columns row-per-symbol
    "insights_analysis",  # criteria block + insights table (7 cols)
    "data_dictionary",    # auto-generated from schema
}

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled"}

# Canonical column counts (centralized so the docstring, validation code,
# and any downstream sanity-checkers agree on a single source of truth).
# v2.8.0: counts UNCHANGED from v2.7.0. Cross-module sync is metadata-only.
_CANONICAL_INSTRUMENT_COLS = 97  # 90 + 2 Decision + 5 Candlestick
_TOP10_TOTAL_COLS = 100          # 97 + 3 Top10 extras
_INSIGHTS_ANALYSIS_COLS = 7
_DATA_DICTIONARY_COLS = 9


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

def _sanitize_columns(
    sheet_name: str,
    kind: str,
    cols: Sequence[ColumnSpec],
    *,
    enforce_symbol_first: bool = True,
) -> Tuple[ColumnSpec, ...]:
    """
    Removes any column specs that have blank group/key/header (after strip).
    Optional: enforce first column key='symbol' (only for FULL instrument_table schemas).
    """
    cleaned: List[ColumnSpec] = []
    for c in cols:
        k = (c.key or "").strip()
        h = (c.header or "").strip()
        g = (c.group or "").strip()
        if not g or not k or not h:
            continue

        dt = (c.dtype or "str").strip().lower()
        if dt not in _ALLOWED_DTYPES:
            # keep startup-safe: coerce bad dtype to str (validation can still catch it in strict mode)
            dt = "str"
        cleaned.append(
            ColumnSpec(
                group=g,
                header=h,
                key=k,
                dtype=dt,
                fmt=(c.fmt or "text"),
                required=bool(c.required),
                source=(c.source or ""),
                notes=(c.notes or ""),
            )
        )

    if kind == "instrument_table" and enforce_symbol_first:
        if not cleaned:
            raise ValueError(f"[{sheet_name}] instrument_table resulted in 0 columns after sanitization.")
        if cleaned[0].key != "symbol":
            raise ValueError(
                f"[{sheet_name}] instrument_table first column must be key='symbol'. "
                f"Got '{cleaned[0].key}'. (A blank/placeholder column may have existed.)"
            )

    return tuple(cleaned)


def _sanitize_sheet(spec: SheetSpec) -> SheetSpec:
    enforce = True if spec.kind == "instrument_table" else False
    return SheetSpec(
        sheet=spec.sheet,
        kind=spec.kind,
        columns=_sanitize_columns(spec.sheet, spec.kind, spec.columns, enforce_symbol_first=enforce),
        criteria_fields=spec.criteria_fields,
        notes=spec.notes,
    )


# -----------------------------
# Canonical columns (97 in v2.7.0+; unchanged in v2.8.0)
# -----------------------------

def _canonical_instrument_columns() -> Tuple[ColumnSpec, ...]:
    """
    Canonical 97 columns used by:
      Market_Leaders, Global_Markets, Commodities_FX, Mutual_Funds, My_Portfolio

    Column groups and counts (running total):
      Identity (8)         ->  8
      Price (10)           -> 18
      Liquidity (6)        -> 24
      Fundamentals (12)    -> 36
      Risk (8)             -> 44
      Valuation (7)        -> 51   (Upside % since v2.4.0)
      Forecast (9)         -> 60
      Scores (7)           -> 67
      Views (4)            -> 71   (added v2.3.0)
      Recommendation (4)   -> 75
      Portfolio (6)        -> 81
      Provenance (4)       -> 85
      Insights (5)         -> 90   (added v2.6.0)
      Decision (2)         -> 92   (added v2.7.0)
      Candlestick (5)      -> 97   (added v2.7.0)

    IMPORTANT:
    - Keep keys stable.
    - Add only at the END to preserve compatibility, OR bump SCHEMA_VERSION
      AND coordinate with core.scoring / core.reco_normalize / both
      investment_advisor* modules and their fallback column lists.
    - v2.8.0: column set UNCHANGED from v2.7.0. The v2.8.0 cross-module
      sync is metadata-only (docstring updates + criteria_fields
      extension + __version__ alias).
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
    add("Price", "52W Position %", "week_52_position_pct", "pct", "0.00%", False, "derived", "Position within 52-week range. Auto-normalized to percent points by data_engine_v2 v5.60.0+ Phase O.")

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
    add("Risk", "Risk Score", "risk_score", "float", "0.00", False, "model", "0-100 numeric. Higher = riskier.")
    add("Risk", "Risk Bucket", "risk_bucket", "str", "text", False, "derived", "Mixed-case label: Low / Moderate / High. (See Risk View for the canonical uppercase token.)")

    # Valuation (7) -> total 51   (Upside % added in v2.4.0)
    add("Valuation", "P/B", "pb_ratio", "float", "0.00", False, "provider", "Price-to-book.")
    add("Valuation", "P/S", "ps_ratio", "float", "0.00", False, "provider", "Price-to-sales.")
    add("Valuation", "EV/EBITDA", "ev_ebitda", "float", "0.00", False, "provider", "Enterprise value multiple.")
    add("Valuation", "PEG", "peg_ratio", "float", "0.00", False, "provider", "PEG ratio.")
    add(
        "Valuation", "Intrinsic Value", "intrinsic_value", "float", "0.00", False, "model",
        "Model intrinsic estimate. May be cleared upstream by data_engine_v2 v5.60.0+ "
        "Phase H/I when 'intrinsic_unit_mismatch_suspected' or 'upside_synthesis_suspect' "
        "warning tags fire (engine-dropped valuation).",
    )
    add(
        "Valuation", "Upside %", "upside_pct", "pct", "0.00%", False, "model",
        "(intrinsic_value/current_price)-1. May be cleared alongside intrinsic_value when "
        "engine v5.60.0+ drops the valuation. Surfaced to top10_selector v4.11.0 "
        "as a soft data-quality penalty.",
    )
    add("Valuation", "Valuation Score", "valuation_score", "float", "0.00", False, "model", "0-100 or normalized scoring.")

    # Forecast (9) -> total 60
    add("Forecast", "Forecast Price 1M", "forecast_price_1m", "float", "0.00", False, "model", "Forecast horizon 1M. Cleared when engine v5.60.0+ Phase B emits forecast_unavailable* tags.")
    add("Forecast", "Forecast Price 3M", "forecast_price_3m", "float", "0.00", False, "model", "Forecast horizon 3M.")
    add("Forecast", "Forecast Price 12M", "forecast_price_12m", "float", "0.00", False, "model", "Forecast horizon 12M.")
    add("Forecast", "Expected ROI 1M", "expected_roi_1m", "pct", "0.00%", False, "model", "(forecast_price_1m/current)-1.")
    add("Forecast", "Expected ROI 3M", "expected_roi_3m", "pct", "0.00%", False, "model", "(forecast_price_3m/current)-1.")
    add("Forecast", "Expected ROI 12M", "expected_roi_12m", "pct", "0.00%", False, "model", "(forecast_price_12m/current)-1.")
    add("Forecast", "Forecast Confidence", "forecast_confidence", "float", "0.00", False, "model", "Forecast confidence 0-1 or 0-100. Scoring v5.2.5 'no-fabricated-confidence' guard prevents synthesizing this when source data is too sparse.")
    add("Forecast", "Confidence Score", "confidence_score", "float", "0.00", False, "model", "Scored confidence (normalized).")
    add("Forecast", "Confidence Bucket", "confidence_bucket", "str", "text", False, "derived", "High / Medium / Low.")

    # Scores (7) -> total 67
    add("Scores", "Value Score", "value_score", "float", "0.00", False, "model", "0-100 or normalized.")
    add("Scores", "Quality Score", "quality_score", "float", "0.00", False, "model", "0-100 or normalized.")
    add("Scores", "Momentum Score", "momentum_score", "float", "0.00", False, "model", "0-100 or normalized.")
    add("Scores", "Growth Score", "growth_score", "float", "0.00", False, "model", "0-100 or normalized.")
    add("Scores", "Overall Score", "overall_score", "float", "0.00", False, "model", "Primary combined score.")
    add("Scores", "Opportunity Score", "opportunity_score", "float", "0.00", False, "model", "Composite opportunity score.")
    add("Scores", "Rank (Overall)", "rank_overall", "int", "0", False, "derived", "Rank within page/universe.")

    # Views (4) -> total 71   (added v2.3.0)
    #
    # The four View columns are derived from the underlying scores by
    # core.scoring v5.2.5 (functions: derive_fundamental_view,
    # derive_technical_view, derive_risk_view, derive_value_view) and
    # then consumed by core.reco_normalize v7.2.0's view-aware 5-tier
    # rule cascade in Recommendation.from_views() /
    # recommendation_from_views() / recommendation_from_views_with_rule_id().
    # Moving these columns or renaming their keys requires coordinated
    # changes in those modules AND in both investment_advisor*.py
    # fallback alias maps.
    add(
        "Views", "Fundamental View", "fundamental_view",
        "str", "text", False, "derived",
        "BULLISH / NEUTRAL / BEARISH derived from quality_score + growth_score "
        "(core.scoring v5.2.5 derive_fundamental_view).",
    )
    add(
        "Views", "Technical View", "technical_view",
        "str", "text", False, "derived",
        "BULLISH / NEUTRAL / BEARISH derived from technical_score + momentum_score + RSI "
        "(core.scoring v5.2.5 derive_technical_view).",
    )
    add(
        "Views", "Risk View", "risk_view",
        "str", "text", False, "derived",
        "LOW / MODERATE / HIGH (uppercase canonical tokens) derived from risk_score "
        "(core.scoring v5.2.5 derive_risk_view). Distinct vocabulary from the existing "
        "`risk_bucket` column which uses mixed-case labels.",
    )
    add(
        "Views", "Value View", "value_view",
        "str", "text", False, "derived",
        "CHEAP / FAIR / EXPENSIVE derived from upside_pct (preferred when present) or "
        "valuation_score thresholds (core.scoring v5.2.5 derive_value_view).",
    )

    # Recommendation (4) -> total 75
    add(
        "Recommendation", "Recommendation", "recommendation",
        "str", "text", False, "model/derived",
        "5-tier canonical token: STRONG_BUY / BUY / HOLD / REDUCE / SELL. "
        "Set by core.reco_normalize v7.2.0 with conviction-floor downgrade "
        "applied (env: RECO_STRONG_BUY_CONVICTION_FLOOR=60, "
        "RECO_BUY_CONVICTION_FLOOR=45). AVOID input now maps to SELL (fixed "
        "in v7.2.0 Phase E; was REDUCE in v7.1.0).",
    )
    add(
        "Recommendation", "Recommendation Reason", "recommendation_reason",
        "str", "text", False, "model",
        "Structured explanation. v2.6.0+ format: 'Action: REC, Conv NN/100, "
        "Sector-Adj NN | Top factors: ... | Top risks: ... | <base reason>'. "
        "reco_normalize v7.2.0 Phase D de-duplicates the conviction badge "
        "when upstream callers already include it. May embed a RULE_ID_* "
        "marker when recommendation_from_views_with_rule_id() is used.",
    )
    add("Recommendation", "Horizon Days", "horizon_days", "int", "0", False, "criteria/derived", "Internal horizon in days.")
    add("Recommendation", "Invest Period Label", "invest_period_label", "str", "text", False, "derived", "1M / 3M / 12M label.")

    # Portfolio (6) -> total 81
    add("Portfolio", "Position Qty", "position_qty", "float", "0.00", False, "sheet/user", "Portfolio quantity.")
    add("Portfolio", "Avg Cost", "avg_cost", "float", "0.00", False, "sheet/user", "Average cost per unit.")
    add("Portfolio", "Position Cost", "position_cost", "float", "0.00", False, "derived", "position_qty * avg_cost.")
    add("Portfolio", "Position Value", "position_value", "float", "0.00", False, "derived", "position_qty * current_price.")
    add("Portfolio", "Unrealized P/L", "unrealized_pl", "float", "0.00", False, "derived", "position_value - position_cost.")
    add("Portfolio", "Unrealized P/L %", "unrealized_pl_pct", "pct", "0.00%", False, "derived", "unrealized_pl / position_cost.")

    # Provenance (4) -> total 85
    add("Provenance", "Data Provider", "data_provider", "str", "text", False, "system", "Primary provider used for the row.")
    add("Provenance", "Last Updated (UTC)", "last_updated_utc", "datetime", "yyyy-mm-dd hh:mm:ss", False, "system", "Last update UTC.")
    add("Provenance", "Last Updated (Riyadh)", "last_updated_riyadh", "datetime", "yyyy-mm-dd hh:mm:ss", False, "system", "Last update Asia/Riyadh.")
    add(
        "Provenance", "Warnings", "warnings", "str", "text", False, "system",
        "Non-fatal warnings / missing fields summary. Canonicalized as a "
        "'; '-joined string by data_engine_v2 v5.60.0+ Phase N. Carries "
        "engine-dropped valuation tags (5 from Phase H/I/P), "
        "forecast-unavailable tags (4 from Phase B), and any provider "
        "warning tags forwarded upstream.",
    )

    # Insights (5) -> total 90  (added v2.6.0)
    #
    # The five Insights columns are produced by core.scoring v5.2.5 as
    # row fields on AssetScores (sector_relative_score additionally uses
    # an optional sector cohort; conviction_score / top_factors /
    # top_risks / position_size_hint are derived from the same fully-
    # scored row that produces the recommendation). Conviction is fed
    # back into core.reco_normalize v7.2.0's Recommendation.from_views()
    # to enforce conviction-floor downgrades (env-tunable via
    # RECO_STRONG_BUY_CONVICTION_FLOOR / RECO_BUY_CONVICTION_FLOOR).
    # core.insights_builder v7.0.0 consumes these for the Top Picks
    # section.
    add(
        "Insights", "Sector-Adj Score", "sector_relative_score",
        "float", "0.00", False, "model",
        "Percentile rank (0-100) of overall_score within sector cohort. "
        "Returns null when sector cohort < 3 rows. "
        "(core.scoring v5.2.5)",
    )
    add(
        "Insights", "Conviction Score", "conviction_score",
        "float", "0.00", False, "model",
        "0-100 measure of recommendation strength. Composition: 40% view "
        "agreement, 30% score extremity, 20% forecast confidence, 10% data "
        "completeness. Distinct from forecast_confidence (data quality). "
        "Drives reco_normalize v7.2.0 conviction-floor downgrades. "
        "(core.scoring v5.2.5)",
    )
    add(
        "Insights", "Top Factors", "top_factors",
        "str", "text", False, "derived",
        "Top-3 component scores by contribution (score*weight). "
        "Pipe-separated, e.g. 'Quality 82 | Value 75 | Momentum 68'. "
        "(core.scoring v5.2.5)",
    )
    add(
        "Insights", "Top Risks", "top_risks",
        "str", "text", False, "derived",
        "Top-2 risk factors fired by rules (high vol, leverage, drawdown, "
        "RSI extremes, negative margins, elevated VaR, high overall risk). "
        "Pipe-separated. (core.scoring v5.2.5)",
    )
    add(
        "Insights", "Position Size Hint", "position_size_hint",
        "str", "text", False, "derived",
        "Heuristic position-size anchor based on (recommendation, conviction). "
        "NOT financial advice. e.g. '4-6% of portfolio' for high-conviction "
        "STRONG_BUY. (core.scoring v5.2.5)",
    )

    # Decision Matrix (2) -> total 92  (added v2.7.0)
    #
    # Produced by core.data_engine_v2 v5.60.0+'s 8-tier classifier
    # _classify_8tier(). The canonical 5-tier `recommendation` field
    # above is preserved unchanged; this pair adds richer detail and a
    # numeric priority. May be EMPTY when `recommendation` is set
    # upstream by core.scoring -> core.reco_normalize, to avoid
    # implying a detailed verdict that may disagree with the canonical.
    add(
        "Decision", "Recommendation Detail", "recommendation_detailed",
        "str", "text", False, "model",
        "8-tier Decision Matrix verdict: STRONG_BUY / BUY / SPECULATIVE_BUY / "
        "ACCUMULATE / HOLD / REDUCE / SELL / STRONG_SELL. Richer than the "
        "canonical 5-tier `recommendation` field which collapses these via "
        "core.data_engine_v2 v5.60.0+ _RECOMMENDATION_COLLAPSE_MAP. Empty when "
        "`recommendation` is set upstream by core.scoring -> core.reco_normalize.",
    )
    add(
        "Decision", "Reco Priority", "recommendation_priority",
        "int", "0", False, "model",
        "1-8 priority rule that fired in the Decision Matrix. "
        "1=Critical Risk (STRONG_SELL circuit breaker), "
        "2=Golden Setup (STRONG_BUY), "
        "3=High Beta/Growth (SPECULATIVE_BUY), "
        "4=Core Position (BUY), "
        "5=Value Play (ACCUMULATE), "
        "6=Fundamental Failure (SELL), "
        "7=Exit Strategy (REDUCE), "
        "8=Neutral (HOLD). "
        "Empty when `recommendation` is upstream-set.",
    )

    # Candlestick (5) -> total 97  (added v2.7.0)
    #
    # Produced by core.candlesticks v1.0.0 when invoked from
    # core.data_engine_v2 v5.60.0+ on OHLC history rows. Detection is
    # BEST-EFFORT and OPTIONAL: when the candlesticks module is
    # unavailable, when the env flag ENGINE_CANDLESTICKS_ENABLED is
    # disabled, or when detection raises, all five columns stay null.
    # 10 patterns supported: Doji, Hammer, Inverted Hammer,
    # Shooting Star, Hanging Man, Bullish/Bearish Marubozu,
    # Bullish/Bearish Engulfing, Morning Star, Evening Star.
    add(
        "Candlestick", "Candle Pattern", "candlestick_pattern",
        "str", "text", False, "derived",
        "Latest candlestick pattern detected (e.g. 'Bullish Engulfing', 'Hammer', "
        "'Doji'). Empty when no pattern fires or insufficient OHLC history. "
        "(core.candlesticks.detect_patterns)",
    )
    add(
        "Candlestick", "Candle Signal", "candlestick_signal",
        "str", "text", False, "derived",
        "Pattern signal: BULLISH / BEARISH / NEUTRAL / DOJI. Defaults to "
        "NEUTRAL when no pattern detected.",
    )
    add(
        "Candlestick", "Candle Strength", "candlestick_strength",
        "str", "text", False, "derived",
        "Pattern strength bucket: STRONG (confidence >= 75) / "
        "MODERATE (>= 55) / WEAK (< 55). Empty when no pattern detected.",
    )
    add(
        "Candlestick", "Candle Confidence", "candlestick_confidence",
        "float", "0.00", False, "derived",
        "Pattern confidence 0-100 derived from how comfortably the geometric "
        "criteria are met for the detected pattern. Conservative scoring.",
    )
    add(
        "Candlestick", "Recent Patterns (5D)", "candlestick_patterns_recent",
        "str", "text", False, "derived",
        "Pipe-separated list of pattern names detected across the last 5 "
        "trading bars, oldest -> newest. Helps spot pattern clusters / "
        "confirmations. Empty when no patterns in the window.",
    )

    return tuple(cols)


_CANONICAL_COLUMNS = _sanitize_columns(
    "CANONICAL",
    "instrument_table",
    _canonical_instrument_columns(),
    enforce_symbol_first=True,
)


def _top10_extra_columns() -> Tuple[ColumnSpec, ...]:
    """
    3 extra columns for Top_10_Investments.
    IMPORTANT: sanitize as a fragment (do NOT enforce symbol-first here).
    """
    cols = (
        ColumnSpec(
            "Top10", "Top10 Rank", "top10_rank", "int", "0", False, "derived",
            "Rank within Top 10 selection (1-based, set by top10_selector v4.11.0).",
        ),
        ColumnSpec(
            "Top10", "Selection Reason", "selection_reason", "str", "text", False, "model",
            "Why this row was selected. top10_selector v4.11.0 surfaces "
            "the leading top_factor / top_risk plus any active data-quality "
            "caveats (Provider Error: ..., Engine: ..., Forecast: unavailable).",
        ),
        ColumnSpec(
            "Top10", "Criteria Snapshot", "criteria_snapshot", "json", "text", False, "system",
            "JSON snapshot of criteria used. May include v3.1.0 fields "
            "(min_conviction_score, exclude_engine_dropped_valuation, "
            "exclude_forecast_unavailable, exclude_provider_errors).",
        ),
    )
    return _sanitize_columns(
        "Top_10_Investments(Top10Extras)",
        "top10_extras_fragment",
        cols,
        enforce_symbol_first=False,
    )


def _insights_columns() -> Tuple[ColumnSpec, ...]:
    cols = (
        ColumnSpec("Insights", "Section", "section", "str", "text", True, "system", "e.g., Market Leaders Top 7, Portfolio Summary, Data Quality Alerts (insights_builder v7.0.0)."),
        ColumnSpec("Insights", "Item", "item", "str", "text", True, "system", "Row label within section."),
        ColumnSpec("Insights", "Symbol", "symbol", "str", "text", False, "derived", "If insight is symbol-specific."),
        ColumnSpec("Insights", "Metric", "metric", "str", "text", False, "system", "Metric name."),
        ColumnSpec("Insights", "Value", "value", "str", "text", False, "system", "Apps Script friendly value."),
        ColumnSpec("Insights", "Notes", "notes", "str", "text", False, "system", "Extra notes / explanation."),
        ColumnSpec("Provenance", "Last Updated (Riyadh)", "last_updated_riyadh", "datetime", "yyyy-mm-dd hh:mm:ss", False, "system", "Timestamp."),
    )
    return _sanitize_columns("Insights_Analysis", "insights_analysis", cols, enforce_symbol_first=False)


def _insights_criteria_fields() -> Tuple[CriteriaField, ...]:
    """
    Criteria block rendered at the top of Insights_Analysis as key/value rows.

    v2.8.0 changes (aligned with core.analysis.criteria_model v3.1.0):
      - RENAME: 'min_conviction' -> 'min_conviction_score' (canonical
                field name in criteria_model v3.1.0). Backwards-compatible
                because criteria_model v3.1.0's KV alias map accepts both
                spellings.
      - NEW:    exclude_engine_dropped_valuation (bool, default 'false')
      - NEW:    exclude_forecast_unavailable     (bool, default 'false')
      - NEW:    exclude_provider_errors          (bool, default 'false')

    Field count: 9 (v2.7.0) -> 12 (v2.8.0). All three new fields default
    to 'false' so v2.7.0 sheets that lack them get the v3.0.0 semantics
    (no data-quality exclusions). Operators opt in by changing the value
    to a truthy token ('true', 'yes', '1', 'on').
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
        # v2.8.0: renamed from 'min_conviction' for criteria_model v3.1.0
        # canonical-name alignment. Backwards-compatible: criteria_model
        # v3.1.0's KV alias map accepts both 'min_conviction' and
        # 'min_conviction_score' as input keys.
        CriteriaField(
            "min_conviction_score", "Min Conviction Score", "float", "0",
            "Filter floor for conviction_score (0-100). 0 disables. "
            "Aligns with reco_normalize v7.2.0 floors: 60 mirrors "
            "STRONG_BUY downgrade floor, 45 mirrors BUY downgrade floor. "
            "(was 'min_conviction' in v2.6.0/v2.7.0)",
        ),
        # v2.8.0 NEW: data-quality exclusion flags from criteria_model v3.1.0
        CriteriaField(
            "exclude_engine_dropped_valuation", "Exclude Engine-Dropped Valuation",
            "bool", "false",
            "When true, drop rows where data_engine_v2 v5.60.0+ cleared "
            "intrinsic_value / upside_pct upstream (5 tags from Phase H/I/P).",
        ),
        CriteriaField(
            "exclude_forecast_unavailable", "Exclude Forecast Unavailable",
            "bool", "false",
            "When true, drop rows where forecast synthesis was skipped "
            "(4 tags from data_engine_v2 v5.60.0+ Phase B OR the "
            "forecast_unavailable bool flag).",
        ),
        CriteriaField(
            "exclude_provider_errors", "Exclude Provider Errors",
            "bool", "false",
            "When true, drop rows where the provider's last_error_class is "
            "non-empty (preserved by data_engine_v2 v5.60.0+ Phase Q from "
            "the eodhd/yahoo_*/argaam/finnhub/tadawul providers).",
        ),
    )


def _data_dictionary_columns() -> Tuple[ColumnSpec, ...]:
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
    return _sanitize_columns("Data_Dictionary", "data_dictionary", cols, enforce_symbol_first=False)


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
        columns=_insights_columns(),  # 7 columns
        criteria_fields=_insights_criteria_fields(),  # v2.8.0: 12 fields
        notes=(
            "Top block: criteria key/value (v2.8.0 has 12 fields incl. "
            "v3.1.0 conviction + data-quality exclusions). Below: insights "
            "table with stable columns + Data Quality Alerts section "
            "(insights_builder v7.0.0)."
        ),
    ),
    "Top_10_Investments": SheetSpec(
        sheet="Top_10_Investments",
        kind="instrument_table",
        columns=_sanitize_columns(
            "Top_10_Investments",
            "instrument_table",
            _CANONICAL_COLUMNS + _top10_extra_columns(),  # 100 columns in v2.7.0+
            enforce_symbol_first=True,
        ),
        notes="Criteria-driven selection. Canonical columns + Top10 extras.",
    ),
    "Data_Dictionary": SheetSpec(
        sheet="Data_Dictionary",
        kind="data_dictionary",
        columns=_data_dictionary_columns(),  # 9 columns
        notes="Auto-built from schema_registry. Do not hand-edit.",
    ),
}

# Final sanitized registry (guarantees no blank columns)
SCHEMA_REGISTRY: Dict[str, SheetSpec] = {k: _sanitize_sheet(v) for k, v in _RAW_SCHEMA_REGISTRY.items()}

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
# Name normalization / aliases
# -----------------------------

def _sheet_name_variants(value: Any) -> List[str]:
    raw = str(value or "").strip()
    if not raw:
        return []

    compact = raw.replace("-", "_").replace(" ", "_")
    lowered = compact.lower()
    collapsed = lowered.replace("__", "_")
    variants = [raw, compact, lowered, collapsed, lowered.replace("_", "")]
    if raw:
        variants.extend([raw.lower(), raw.upper(), raw.title()])

    seen = set()
    out: List[str] = []
    for item in variants:
        s = str(item or "").strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _build_sheet_alias_map() -> Dict[str, str]:
    alias_map: Dict[str, str] = {}

    explicit = {
        "marketleaders": "Market_Leaders",
        "market_leaders": "Market_Leaders",
        "market leaders": "Market_Leaders",
        "globalmarkets": "Global_Markets",
        "global_markets": "Global_Markets",
        "global markets": "Global_Markets",
        "commoditiesfx": "Commodities_FX",
        "commodities_fx": "Commodities_FX",
        "commodities fx": "Commodities_FX",
        "mutualfunds": "Mutual_Funds",
        "mutual_funds": "Mutual_Funds",
        "mutual funds": "Mutual_Funds",
        "myportfolio": "My_Portfolio",
        "my_portfolio": "My_Portfolio",
        "my portfolio": "My_Portfolio",
        "insights": "Insights_Analysis",
        "insightsanalysis": "Insights_Analysis",
        "insights_analysis": "Insights_Analysis",
        "insights analysis": "Insights_Analysis",
        "top10": "Top_10_Investments",
        "top_10": "Top_10_Investments",
        "top 10": "Top_10_Investments",
        "top10investments": "Top_10_Investments",
        "top_10_investments": "Top_10_Investments",
        "top 10 investments": "Top_10_Investments",
        "top10_investments": "Top_10_Investments",
        "datadictionary": "Data_Dictionary",
        "data_dictionary": "Data_Dictionary",
        "data dictionary": "Data_Dictionary",
        "dictionary": "Data_Dictionary",
    }
    alias_map.update(explicit)

    for canonical in CANONICAL_SHEETS:
        for variant in _sheet_name_variants(canonical):
            alias_map.setdefault(variant, canonical)
            alias_map.setdefault(variant.lower(), canonical)
            alias_map.setdefault(variant.replace("_", " ").lower(), canonical)
            alias_map.setdefault(variant.replace("_", "").lower(), canonical)

    return alias_map


_SHEET_ALIAS_MAP: Dict[str, str] = _build_sheet_alias_map()


def normalize_sheet_name(sheet: Any) -> str:
    raw = str(sheet or "").strip()
    if not raw:
        return ""

    # Explicitly keep removed/forbidden pages out of the registry contract.
    forbidden = raw.replace("-", "_").replace(" ", "_").strip().lower()
    if forbidden in {"ksa_tadawul", "ksa tadawul", "ksatadawul", "advisor_criteria", "advisor criteria", "advisorcriteria"}:
        return ""

    if raw in SCHEMA_REGISTRY:
        return raw

    for variant in _sheet_name_variants(raw):
        if variant in SCHEMA_REGISTRY:
            return variant
        mapped = _SHEET_ALIAS_MAP.get(variant) or _SHEET_ALIAS_MAP.get(variant.lower())
        if mapped:
            return mapped

    return raw


def has_sheet(sheet: Any) -> bool:
    canonical = normalize_sheet_name(sheet)
    return bool(canonical and canonical in SCHEMA_REGISTRY)

# -----------------------------
# Public helpers
# -----------------------------

def list_sheets() -> List[str]:
    if all(s in SCHEMA_REGISTRY for s in CANONICAL_SHEETS):
        return list(CANONICAL_SHEETS)
    return sorted(SCHEMA_REGISTRY.keys())


def get_sheet_spec(sheet: str) -> SheetSpec:
    requested = str(sheet or "").strip()
    canonical = normalize_sheet_name(requested)
    if canonical and canonical in SCHEMA_REGISTRY:
        return SCHEMA_REGISTRY[canonical]
    raise KeyError(f"Unknown sheet '{requested}'. Known: {', '.join(list_sheets())}")


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
    s = normalize_sheet_name(sheet)
    if s in _KEY_INDEX:
        return _KEY_INDEX[s]
    _ = get_sheet_spec(s)
    _build_indexes()
    return _KEY_INDEX.get(s, {})


def get_sheet_header_index(sheet: str) -> Dict[str, int]:
    s = normalize_sheet_name(sheet)
    if s in _HEADER_INDEX:
        return _HEADER_INDEX[s]
    _ = get_sheet_spec(s)
    _build_indexes()
    return _HEADER_INDEX.get(s, {})


# -----------------------------
# Snapshot + Digest (drift guard)
# -----------------------------

def schema_registry_snapshot() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for s in list_sheets():
        spec = SCHEMA_REGISTRY[s]
        out[s] = {
            "kind": spec.kind,
            "len": len(spec.columns),
            "headers": [c.header for c in spec.columns],
            "keys": [c.key for c in spec.columns],
            "groups": [c.group for c in spec.columns],
            "dtypes": [c.dtype for c in spec.columns],
            "formats": [c.fmt for c in spec.columns],
            "required": [bool(c.required) for c in spec.columns],
            "criteria_fields": [cf.key for cf in spec.criteria_fields],
            "criteria_fields_meta": [
                {
                    "key": cf.key,
                    "label": cf.label,
                    "dtype": cf.dtype,
                    "default": cf.default,
                }
                for cf in spec.criteria_fields
            ],
            "notes": spec.notes,
        }
    return out


def schema_registry_digest() -> str:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "sheets": schema_registry_snapshot(),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


# -----------------------------
# Validation (startup-safe)
# -----------------------------

def validate_schema_registry(registry: Optional[Dict[str, SheetSpec]] = None) -> None:
    reg = registry or SCHEMA_REGISTRY

    if "KSA_Tadawul" in reg:
        raise ValueError("Schema registry must NOT include 'KSA_Tadawul'.")

    for s in CANONICAL_SHEETS:
        if s not in reg:
            raise ValueError(f"Missing required sheet in registry: {s}")

    normalized_seen: Dict[str, str] = {}
    for sheet_name in reg.keys():
        norm = normalize_sheet_name(sheet_name) or str(sheet_name or "").strip()
        compact = norm.replace("-", "_").replace(" ", "_").lower()
        if compact in normalized_seen and normalized_seen[compact] != sheet_name:
            raise ValueError(
                f"Schema registry normalized-name collision: '{normalized_seen[compact]}' and '{sheet_name}' -> '{compact}'"
            )
        normalized_seen[compact] = sheet_name

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

        if sheet_name in {"Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds", "My_Portfolio"}:
            if len(spec.columns) != _CANONICAL_INSTRUMENT_COLS:
                raise ValueError(
                    f"[{sheet_name}] instrument_table must be {_CANONICAL_INSTRUMENT_COLS} columns "
                    f"in v{SCHEMA_VERSION}. Got: {len(spec.columns)}"
                )
            if spec.columns[0].key != "symbol":
                raise ValueError(f"[{sheet_name}] First column must be 'symbol'.")
            spec_keys = {c.key for c in spec.columns}
            missing_views = [k for k in VIEW_COLUMN_KEYS if k not in spec_keys]
            if missing_views:
                raise ValueError(
                    f"[{sheet_name}] Missing required view column(s): {missing_views}. "
                    f"The recommendation pipeline (core.reco_normalize v7.2.0 "
                    f"recommendation_from_views) depends on these."
                )
            # v2.6.0+: Insights columns required — produced by
            # core.scoring v5.2.5 and consumed by reco_normalize v7.2.0
            # conviction-floor downgrade + insights_builder v7.0.0.
            missing_insights = [k for k in INSIGHTS_COLUMN_KEYS if k not in spec_keys]
            if missing_insights:
                raise ValueError(
                    f"[{sheet_name}] Missing required insights column(s): {missing_insights}. "
                    f"core.scoring v5.2.5 writes these as row fields."
                )
            # v2.7.0+: Decision Matrix columns required for schema
            # alignment with core.data_engine_v2 v5.60.0+. Values may be
            # empty (when `recommendation` is upstream-set), but the
            # COLUMNS must exist so the engine can populate them.
            missing_decision = [k for k in DECISION_COLUMN_KEYS if k not in spec_keys]
            if missing_decision:
                raise ValueError(
                    f"[{sheet_name}] Missing required decision matrix column(s): {missing_decision}. "
                    f"core.data_engine_v2 v5.60.0+ writes these via _classify_8tier()."
                )
            # v2.7.0+: Candlestick columns required for schema alignment
            # with core.candlesticks v1.0.0. Values may be empty (when
            # detection is disabled / unavailable), but the COLUMNS must
            # exist.
            missing_candlestick = [k for k in CANDLESTICK_COLUMN_KEYS if k not in spec_keys]
            if missing_candlestick:
                raise ValueError(
                    f"[{sheet_name}] Missing required candlestick column(s): {missing_candlestick}. "
                    f"core.candlesticks.detect_patterns writes these via core.data_engine_v2 v5.60.0+."
                )

        if sheet_name == "Top_10_Investments":
            if len(spec.columns) != _TOP10_TOTAL_COLS:
                raise ValueError(
                    f"[Top_10_Investments] must be {_TOP10_TOTAL_COLS} columns "
                    f"({_CANONICAL_INSTRUMENT_COLS} canonical + 3 Top10) in v{SCHEMA_VERSION}. "
                    f"Got: {len(spec.columns)}"
                )
            if spec.columns[0].key != "symbol":
                raise ValueError("[Top_10_Investments] First column must be 'symbol' (no blank leading column).")
            spec_keys = {c.key for c in spec.columns}
            missing_views = [k for k in VIEW_COLUMN_KEYS if k not in spec_keys]
            if missing_views:
                raise ValueError(
                    f"[Top_10_Investments] Missing required view column(s): {missing_views}."
                )
            missing_insights = [k for k in INSIGHTS_COLUMN_KEYS if k not in spec_keys]
            if missing_insights:
                raise ValueError(
                    f"[Top_10_Investments] Missing required insights column(s): {missing_insights}."
                )
            missing_decision = [k for k in DECISION_COLUMN_KEYS if k not in spec_keys]
            if missing_decision:
                raise ValueError(
                    f"[Top_10_Investments] Missing required decision matrix column(s): {missing_decision}."
                )
            missing_candlestick = [k for k in CANDLESTICK_COLUMN_KEYS if k not in spec_keys]
            if missing_candlestick:
                raise ValueError(
                    f"[Top_10_Investments] Missing required candlestick column(s): {missing_candlestick}."
                )

        if sheet_name == "Insights_Analysis":
            if len(spec.columns) != _INSIGHTS_ANALYSIS_COLS:
                raise ValueError(f"[Insights_Analysis] must be {_INSIGHTS_ANALYSIS_COLS} columns. Got: {len(spec.columns)}")

        if sheet_name == "Data_Dictionary":
            if len(spec.columns) != _DATA_DICTIONARY_COLS:
                raise ValueError(f"[Data_Dictionary] must be {_DATA_DICTIONARY_COLS} columns. Got: {len(spec.columns)}")

        seen_ckeys = set()
        for cf in spec.criteria_fields:
            if not (cf.key or "").strip():
                raise ValueError(f"[{sheet_name}] Criteria key is empty")
            if cf.dtype not in _ALLOWED_DTYPES:
                raise ValueError(f"[{sheet_name}] Criteria '{cf.key}' invalid dtype='{cf.dtype}'. Allowed: {sorted(_ALLOWED_DTYPES)}")
            if cf.key in seen_ckeys:
                raise ValueError(f"[{sheet_name}] Duplicate criteria key: {cf.key}")
            seen_ckeys.add(cf.key)


# Validate immediately (fast, no I/O) — but do NOT crash unless strict mode
SCHEMA_VALIDATION_ERRORS: List[str] = []
SCHEMA_VALIDATED_OK: bool = True

try:
    validate_schema_registry()
except Exception as e:
    SCHEMA_VALIDATED_OK = False
    SCHEMA_VALIDATION_ERRORS = [repr(e)]
    strict = (os.getenv("STRICT_SCHEMA_VALIDATION", "") or "").strip().lower() in _TRUTHY
    if strict:
        raise
