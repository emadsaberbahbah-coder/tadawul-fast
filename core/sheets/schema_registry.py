#!/usr/bin/env python3
# core/sheets/schema_registry.py
"""
================================================================================
Schema Registry — v2.13.0
================================================================================
Tadawul Fast Bridge (TFB)

SINGLE source of truth for every sheet's column schema (exact header order,
column metadata, sheet registry). Sheets: Market_Leaders, Global_Markets,
Commodities_FX, Mutual_Funds, My_Portfolio, Insights_Analysis,
Top_10_Investments, Data_Dictionary.

Hard rules: no KSA_Tadawul; sheet-rows endpoints must return full schema
length using these keys; missing values allowed (null/empty) but columns MUST
exist; no network calls; import-safe.

(The verbose pre-v2.12.0 cross-module changelog is condensed in this working
copy; ALL code, validation logic, and per-column notes are preserved verbatim
except where the v2.13.0 / v2.12.0 changelogs below state otherwise.)

================================================================================
v2.13.0  --  ENGINE-V5.79.2-ALIGNMENT  (adds the 8-column Investability Gate so
            the registry mirrors the ACCEPTED data_engine_v2 v5.79.2 instrument
            contract: 115 canonical keys/headers, Top10 118)
================================================================================
The registry was aligned to engine v5.77.17 (107 cols, see v2.12.0 below) but
the ACCEPTED engine is v5.79.2, whose v5.78.0 Investability Gate appended 8
decision-readiness columns at positions 108-115. The engine emits all 8 on every
row, but because they were absent here they were projected OUT of every row and
never reached the sheet (the sheet stopped at "Overall Penalty Factor"). This
release adds them, order-exact and header-exact against data_engine_v2 v5.79.2
INSTRUMENT_CANONICAL_KEYS / INSTRUMENT_CANONICAL_HEADERS (verified on disk).

1. NEW COLUMNS (8), appended AFTER forecast_source as the FINAL canonical block
   (positions 108-115), group "Investability". Keys/headers/value-types:
     data_quality_score          "Data Quality Score"          float  0-100 POINTS
     forecast_reliability_score   "Forecast Reliability Score"  float  0-100 POINTS
     provider_engine_conflict     "Provider/Engine Conflict"    str    'TRUE'/'FALSE'
     conflict_type                "Conflict Type"               str
     final_decision_basis         "Final Decision Basis"        str
     investability_status         "Investability Status"        str    INVESTABLE /
                                                                 WATCHLIST / BLOCKED
     final_action                 "Final Action"                str    INVEST / WATCH /
                                                                 DO_NOT_INVEST
     block_reason                 "Block Reason"                str
   -> instrument 107 -> 115, Top10 110 -> 118.

   VALUE-TYPE NOTES (verified against the engine's gate logic):
     - data_quality_score / forecast_reliability_score are 0-100 POINTS, NOT
       fractions (like overall_score). They must NOT be percent-formatted; the
       engine excludes them from DECIMAL_FRACTION_FIELDS. Both are rounded to 1
       decimal by the engine.
     - provider_engine_conflict holds the TEXT 'TRUE' / 'FALSE' (the engine sets
       the literal strings), NOT a Python bool -> dtype "str".
     - A provider/engine conflict is FLAGGED only; it does NOT override the
       engine recommendation (v5.79.2). The engine recommendation always drives
       investability_status.

2. NEW EXPORT: INVESTABILITY_GATE_COLUMN_KEYS = the 8 keys above, plus a matching
   required-column validation check on instrument sheets AND Top_10_Investments
   (mirrors the existing VIEW / INSIGHTS / DECISION / CANDLESTICK / CANONICAL_RECO
   / SCORING_V574 / FORECAST_SOURCE enforcement). All prior required-column
   checks are retained unchanged.

3. BUMP: SCHEMA_VERSION 2.12.0 -> 2.13.0; _CANONICAL_INSTRUMENT_COLS 107 -> 115;
   _TOP10_TOTAL_COLS 110 -> 118.

Deploy AFTER (or together with) data_engine_v2 v5.79.2 (this registry now mirrors
that engine's contract). After deploy, widen the sheets to 115 by re-running
"Update Current Headers" (Apps Script updateHeadersOnly_ -> applyHeadersFromSchema_)
AND/OR the Python ensure_headers_and_formatting() path, then refresh so the 8 new
columns populate. The Apps Script 00_Config.gs HEADER_TO_KEY (Investability group,
v1.11.0) must also be live so the GAS writer recognises the 8 new headers.

================================================================================
v2.12.0  --  ENGINE-V5.77.17-ALIGNMENT  (the registry now MIRRORS the accepted
            data_engine_v2 v5.77.17 instrument contract, key-for-key and
            header-for-header, in EXACT order)
================================================================================
This release stops the registry from validating only against its own internal
constants and instead realigns it to the locked engine contract
(data_engine_v2 v5.77.17 INSTRUMENT_CANONICAL_KEYS / INSTRUMENT_CANONICAL_HEADERS
/ INSIGHTS_KEYS / INSIGHTS_HEADERS). Verified order-exact against that engine.

1. NEW COLUMN: forecast_source (group "Forecast", header "Forecast Source",
   key "forecast_source", str/text, source "model/system"). This is the
   engine's 107th canonical key (added in engine v5.77.1, elevated to a
   required canonical key in v5.77.3). It records forecast provenance:
   "provider_target" (engine honoured an upstream analyst target),
   "phase_ii_synthetic" (engine synthesized the forecast), or "fallback"
   (engine fallback path created it). Appended as the FINAL canonical column.
   -> instrument 106 -> 107, Top10 109 -> 110.

2. TAIL REORDER to match the engine's exact key order. The engine places the
   scoring/provenance block BEFORE the candlestick block and ends with
   forecast_source. Pre-v2.12.0 the registry emitted candlestick BEFORE the
   Canonical-Reco / Scoring-v5.74 fields. The emit order is now:
     recommendation_detailed, recommendation_priority,
     provider_rating, recommendation_source, recommendation_priority_band,
     scoring_recommendation_source, scoring_schema_version, scoring_errors,
     opportunity_source, overall_score_raw, overall_penalty_factor,
     candlestick_pattern, candlestick_signal, candlestick_strength,
     candlestick_confidence, candlestick_patterns_recent,
     forecast_source.
   The COLUMN ORDER (keys + headers) is the contract; the `group` labels
   ("Canonical Reco" / "Scoring v5.74" / "Candlestick" / "Decision") are
   retained as field metadata even though they are now physically interleaved
   (group is descriptive only and does not affect projection/order).

3. HEADER FIX: recommendation_source header "Reco Source" -> "Recommendation
   Source" to match the engine's INSTRUMENT_CANONICAL_HEADERS. (This was the
   ONLY body header that differed from the engine; all other 105 headers
   already matched.)

4. INSIGHTS_ANALYSIS RECONCILED TO THE ENGINE CONTRACT (behavioral change):
   was [Section, Item, Symbol, Metric, Value, Notes, Last Updated (Riyadh)]
   keyed (section, item, symbol, metric, value, notes, last_updated_riyadh);
   now [Section, Item, Metric, Value, Notes, Source, Sort Order] keyed
   (section, item, metric, value, notes, source, sort_order) to match the
   engine's INSIGHTS_KEYS / INSIGHTS_HEADERS. Column COUNT stays 7. Direction
   chosen per the audit: the engine is the locked contract, so the registry
   adopts it. NOTE: this drops `symbol` and `last_updated_riyadh` and adds
   `source` and `sort_order` — insights_builder, the Insights route, and any
   Apps Script reading the Insights page must use the engine keys.

5. DOC UPDATES for the accepted scoring.py v5.7.1 8-tier vocabulary
   (STRONG_BUY / BUY / ACCUMULATE / HOLD / REDUCE / SELL / STRONG_SELL /
   AVOID): the Recommendation note no longer says "5-tier", the
   Recommendation Detail note no longer says "6-tier mirror", and the stale
   AVOID-asymmetry / "scoring.py v5.6.0" references are updated to v5.7.1
   (AVOID is now its own canonical tier in scoring.py, not collapsed to
   STRONG_SELL).

6. NEW EXPORT: FORECAST_SOURCE_COLUMN_KEYS = ("forecast_source",), and a
   matching validation check that the column is present on instrument sheets
   and Top_10_Investments (mirrors the existing VIEW/INSIGHTS/DECISION/
   CANDLESTICK/CANONICAL_RECO/SCORING_V574 enforcement). All prior required-
   column checks are retained (their keys are unchanged, only reordered).

7. BUMP: SCHEMA_VERSION 2.11.0 -> 2.12.0; _CANONICAL_INSTRUMENT_COLS 106 ->
   107; _TOP10_TOTAL_COLS 109 -> 110.

Deploy AFTER data_engine_v2 v5.77.17 (this registry now mirrors that engine's
contract). Coordinate the Insights_Analysis key change (item 4) with
insights_builder / the Insights route / Apps Script before relying on it.
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
    # v2.9.0: canonical recommendation provenance pair
    "CANONICAL_RECO_COLUMN_KEYS",
    # v2.10.0: 7 new fields aligning registry with engine v5.74.0
    "SCORING_V574_COLUMN_KEYS",
    # v2.12.0: final canonical key (engine v5.77.17)
    "FORECAST_SOURCE_COLUMN_KEYS",
    # v2.13.0: 8 investability-gate keys (engine v5.79.2)
    "INVESTABILITY_GATE_COLUMN_KEYS",
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

SCHEMA_VERSION = "2.13.0"
# TFB module-version convention alias (mirrors scoring v5.6.0,
# data_engine_v2 v5.75.0, enriched_quote v4.7.0, reco_normalize v7.2.0,
# insights_builder v7.0.0, scoring_engine v3.4.4, top10_selector v4.11.0,
# criteria_model v3.1.0). Originally introduced in v2.8.0 Phase C.
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
# v5.74.0's 8-tier classifier. The canonical `recommendation` field
# (8-tier as of core.scoring v5.7.1) is preserved unchanged; this pair
# adds richer detail and a numeric priority. May be EMPTY when
# `recommendation` is upstream-set.
#
# IMPORTANT NAMING DISAMBIGUATION:
#   - DECISION_COLUMN_KEYS[1] == "recommendation_priority" (int 1..8)
#     The 8-tier classifier rule priority -- which rule fired.
#   - CANONICAL_RECO_COLUMN_KEYS[0] == "recommendation_priority_band" (str P1..P5)
#     The canonical priority band (P1..P5) from core.scoring v5.7.1.
# These two are DIFFERENT FIELDS with DIFFERENT SEMANTICS and coexist
# by design.
DECISION_COLUMN_KEYS: Tuple[str, ...] = (
    "recommendation_detailed",
    "recommendation_priority",
)


# v2.7.0: Candlestick column keys, produced by core.candlesticks v1.0.0
# when invoked from core.data_engine_v2 v5.74.0 on OHLC history rows.
# Detection is best-effort and may be disabled via the
# ENGINE_CANDLESTICKS_ENABLED env flag, in which case these stay null.
CANDLESTICK_COLUMN_KEYS: Tuple[str, ...] = (
    "candlestick_pattern",
    "candlestick_signal",
    "candlestick_strength",
    "candlestick_confidence",
    "candlestick_patterns_recent",
)


# v2.9.0: Canonical Reco column keys. Produced by core.scoring v5.2.8
# as row fields on AssetScores and emitted by compute_scores() plus the
# public helpers derive_canonical_recommendation() and
# apply_canonical_recommendation(). These two fields close the May 13
# 2026 cascade-discrepancy bug by providing a single canonical source
# of truth for the recommendation priority band (P1..P5) and a
# provenance tag that downstream engines (investment_advisor_engine
# v3.6.0+, data_engine_v2 v5.74.0) read to skip parallel
# recomputation.
#
# Distinct from DECISION_COLUMN_KEYS:
#   - recommendation_priority_band: str, "P1".."P5"  (THIS group)
#   - recommendation_priority:      int, 1..8        (Decision group)
# These coexist intentionally.
#
# NOTE: As of v2.10.0, `recommendation_source` in THIS group holds the
# engine v5.74.0 vocabulary (engine / provider_override / empty_row /
# scoring_unavailable). The scoring.py provenance tag is preserved
# SEPARATELY as `scoring_recommendation_source` in the new "Scoring
# v5.74" group to avoid collision between the engine and scoring layer
# vocabularies.
CANONICAL_RECO_COLUMN_KEYS: Tuple[str, ...] = (
    "recommendation_priority_band",
    "recommendation_source",
)


# v2.10.0: Scoring v5.74 column keys. Produced by core.scoring v5.4.2+
# as patch fields on the row dict emitted by compute_scores(), and
# preserved into the final row by core.data_engine_v2 v5.74.0 via
# _compute_scores_canonical_first() + _preserve_scoring_provenance() +
# _coerce_scoring_errors_for_sheet().
#
# These seven fields expand the instrument schema from 99 -> 106 columns
# to align with the v5.74.0 engine's INSTRUMENT_CANONICAL_KEYS.
SCORING_V574_COLUMN_KEYS: Tuple[str, ...] = (
    "provider_rating",
    "scoring_recommendation_source",
    "scoring_schema_version",
    "scoring_errors",
    "opportunity_source",
    "overall_score_raw",
    "overall_penalty_factor",
)


# v2.12.0: the engine's 107th / final pre-gate canonical key. Produced by
# core.data_engine_v2 v5.77.17 (added v5.77.1, elevated to a required
# canonical key in v5.77.3 _INSTRUMENT_CANONICAL_REQUIRED_KEYS). Records
# forecast provenance: "provider_target" (engine honoured an upstream
# analyst target), "phase_ii_synthetic" (engine synthesized the forecast),
# or "fallback" (engine fallback path created it). Empty/absent when no
# forecast was produced. Appended after the candlestick block so all prior
# positional indices are preserved.
FORECAST_SOURCE_COLUMN_KEYS: Tuple[str, ...] = (
    "forecast_source",
)


# v2.13.0: Investability Gate column keys (decision-readiness layer).
# Produced by core.data_engine_v2 v5.78.0+ (gate logic) and present on every
# row emitted by the accepted engine v5.79.2. Appended AFTER forecast_source
# as the FINAL canonical block (positions 108-115). Aligned with the engine's
# INSTRUMENT_CANONICAL_KEYS / INSTRUMENT_CANONICAL_HEADERS and with
# 00_Config.gs v1.11.0 HEADER_TO_KEY (Investability group) + AUDIT-16.
#
# VALUE-TYPE NOTES (verified against the engine gate logic):
#   - data_quality_score / forecast_reliability_score are 0-100 POINTS (NOT
#     fractions, like overall_score) -> NOT percent-formatted. Rounded to 1
#     decimal by the engine.
#   - provider_engine_conflict holds the TEXT 'TRUE' / 'FALSE' (the engine sets
#     the literal strings), NOT a Python bool -> dtype "str".
#   - A conflict is FLAGGED only; it does NOT override the engine (v5.79.2).
INVESTABILITY_GATE_COLUMN_KEYS: Tuple[str, ...] = (
    "data_quality_score",
    "forecast_reliability_score",
    "provider_engine_conflict",
    "conflict_type",
    "final_decision_basis",
    "investability_status",
    "final_action",
    "block_reason",
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
# v2.13.0: bumped 107 -> 115 instrument, 110 -> 118 Top10 for the 8 new
# investability-gate columns (engine v5.79.2), appended at end.
_CANONICAL_INSTRUMENT_COLS = 115  # 107 + 8 investability gate (v2.13.0)
_TOP10_TOTAL_COLS = 118           # 115 + 3 Top10 extras
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
# Canonical columns (115 in v2.13.0+; was 107 in v2.12.0)
# -----------------------------

def _canonical_instrument_columns() -> Tuple[ColumnSpec, ...]:
    """
    Canonical 115 columns used by:
      Market_Leaders, Global_Markets, Commodities_FX, Mutual_Funds, My_Portfolio

    v2.13.0: realigned to data_engine_v2 v5.79.2's INSTRUMENT_CANONICAL_KEYS
    (verified order-exact). v2.12.0 mirrored v5.77.17 (107 cols); v5.78.0
    appended the 8-column Investability Gate at positions 108-115. Emit order
    and running totals:
      Identity (8)         ->   8
      Price (10)           ->  18
      Liquidity (6)        ->  24
      Fundamentals (12)    ->  36
      Risk (8)             ->  44
      Valuation (7)        ->  51   (Upside % since v2.4.0)
      Forecast (9)         ->  60
      Scores (7)           ->  67
      Views (4)            ->  71   (added v2.3.0)
      Recommendation (4)   ->  75
      Portfolio (6)        ->  81
      Provenance (4)       ->  85
      Insights (5)         ->  90   (added v2.6.0)
      Decision (2)         ->  92   (added v2.7.0)
      Provider Rating (1)  ->  93   (Scoring v5.74 group; engine places it here)
      Canonical Reco (2)   ->  95   (recommendation_source, then priority_band)
      Scoring v5.74 (6)    -> 101   (remaining scoring/provenance fields)
      Candlestick (5)      -> 106   (engine places candlestick AFTER scoring)
      Forecast Source (1)  -> 107   (added v2.12.0; engine's pre-gate final key)
      Investability (8)    -> 115   (added v2.13.0; engine v5.79.2 decision-
                                     readiness gate, positions 108-115)

    IMPORTANT:
    - Keep keys stable. The COLUMN ORDER (keys + headers) is the contract and
      now mirrors data_engine_v2 v5.79.2 exactly; do not reorder without
      re-checking against that engine.
    - `group` labels are descriptive field metadata only. As of v2.12.0 the
      Canonical-Reco and Scoring-v5.74 groups are physically interleaved to
      match the engine's key order; this is intentional and does not affect
      projection or column order.
    - Add only at the END (after the Investability Gate block) to preserve
      compatibility, OR bump SCHEMA_VERSION AND coordinate with the engine +
      core.scoring + core.reco_normalize + Apps Script.
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
    add("Price", "52W Position %", "week_52_position_pct", "pct", "0.00%", False, "derived", "Position within 52-week range. Auto-normalized to percent points by data_engine_v2 v5.74.0 Phase O.")

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
        "Model intrinsic estimate. May be cleared upstream by data_engine_v2 v5.74.0 "
        "Phase H/I when 'intrinsic_unit_mismatch_suspected' or 'upside_synthesis_suspect' "
        "warning tags fire (engine-dropped valuation).",
    )
    add(
        "Valuation", "Upside %", "upside_pct", "pct", "0.00%", False, "model",
        "(intrinsic_value/current_price)-1. May be cleared alongside intrinsic_value when "
        "engine v5.74.0 drops the valuation. Surfaced to top10_selector v4.11.0 "
        "as a soft data-quality penalty.",
    )
    add("Valuation", "Valuation Score", "valuation_score", "float", "0.00", False, "model", "0-100 or normalized scoring.")

    # Forecast (9) -> total 60
    add("Forecast", "Forecast Price 1M", "forecast_price_1m", "float", "0.00", False, "model", "Forecast horizon 1M. Cleared when engine v5.74.0 Phase B emits forecast_unavailable* tags.")
    add("Forecast", "Forecast Price 3M", "forecast_price_3m", "float", "0.00", False, "model", "Forecast horizon 3M.")
    add("Forecast", "Forecast Price 12M", "forecast_price_12m", "float", "0.00", False, "model", "Forecast horizon 12M.")
    add("Forecast", "Expected ROI 1M", "expected_roi_1m", "pct", "0.00%", False, "model", "(forecast_price_1m/current)-1.")
    add("Forecast", "Expected ROI 3M", "expected_roi_3m", "pct", "0.00%", False, "model", "(forecast_price_3m/current)-1.")
    add("Forecast", "Expected ROI 12M", "expected_roi_12m", "pct", "0.00%", False, "model", "(forecast_price_12m/current)-1.")
    add("Forecast", "Forecast Confidence", "forecast_confidence", "float", "0.00", False, "model", "Forecast confidence 0-1 or 0-100. Scoring v5.4.2+ 'no-fabricated-confidence' guard prevents synthesizing this when source data is too sparse.")
    add("Forecast", "Confidence Score", "confidence_score", "float", "0.00", False, "model", "Scored confidence (normalized).")
    add("Forecast", "Confidence Bucket", "confidence_bucket", "str", "text", False, "derived", "High / Medium / Low.")

    # Scores (7) -> total 67
    add("Scores", "Value Score", "value_score", "float", "0.00", False, "model", "0-100 or normalized.")
    add("Scores", "Quality Score", "quality_score", "float", "0.00", False, "model", "0-100 or normalized.")
    add("Scores", "Momentum Score", "momentum_score", "float", "0.00", False, "model", "0-100 or normalized.")
    add("Scores", "Growth Score", "growth_score", "float", "0.00", False, "model", "0-100 or normalized.")
    add(
        "Scores", "Overall Score", "overall_score", "float", "0.00", False, "model",
        "Primary combined score (post-penalty). In v5.74.0 this is "
        "overall_score_raw * overall_penalty_factor (see Scoring v5.74 group).",
    )
    add("Scores", "Opportunity Score", "opportunity_score", "float", "0.00", False, "model", "Composite opportunity score.")
    add("Scores", "Rank (Overall)", "rank_overall", "int", "0", False, "derived", "Rank within page/universe.")

    # Views (4) -> total 71   (added v2.3.0)
    add(
        "Views", "Fundamental View", "fundamental_view",
        "str", "text", False, "derived",
        "BULLISH / NEUTRAL / BEARISH derived from quality_score + growth_score "
        "(core.scoring v5.2.8 derive_fundamental_view).",
    )
    add(
        "Views", "Technical View", "technical_view",
        "str", "text", False, "derived",
        "BULLISH / NEUTRAL / BEARISH derived from technical_score + momentum_score + RSI "
        "(core.scoring v5.2.8 derive_technical_view).",
    )
    add(
        "Views", "Risk View", "risk_view",
        "str", "text", False, "derived",
        "LOW / MODERATE / HIGH (uppercase canonical tokens) derived from risk_score "
        "(core.scoring v5.2.8 derive_risk_view). Distinct vocabulary from the existing "
        "`risk_bucket` column which uses mixed-case labels.",
    )
    add(
        "Views", "Value View", "value_view",
        "str", "text", False, "derived",
        "CHEAP / FAIR / EXPENSIVE derived from upside_pct (preferred when present) or "
        "valuation_score thresholds (core.scoring v5.2.8 derive_value_view).",
    )

    # Recommendation (4) -> total 75
    add(
        "Recommendation", "Recommendation", "recommendation",
        "str", "text", False, "model/derived",
        "8-tier canonical token (best -> worst): STRONG_BUY / BUY / "
        "ACCUMULATE / HOLD / REDUCE / SELL / STRONG_SELL / AVOID. Widened "
        "from 5 tiers by core.scoring v5.7.1 + core.reco_normalize v8.0.0 "
        "(ACCUMULATE added between BUY and HOLD; AVOID added as the worst-case "
        "'do not enter or hold' tier). Conviction-floor downgrade still applies "
        "(env: RECO_STRONG_BUY_CONVICTION_FLOOR=60, RECO_BUY_CONVICTION_FLOOR=45). "
        "Keyed to recommendation_source (see Canonical Reco group) so downstream "
        "engines can detect canonically-scored rows and skip parallel "
        "recomputation. v2.12.0 note on the legacy AVOID alias: as of "
        "core.scoring v5.7.1, AVOID is its OWN canonical tier and is NO LONGER "
        "collapsed to STRONG_SELL (the v5.6.0 AVOID->STRONG_SELL alias was "
        "removed). core.reco_normalize v8.0.0 likewise treats AVOID as a "
        "distinct tier. The earlier two-layer asymmetry (reco_normalize "
        "AVOID->SELL vs scoring AVOID->STRONG_SELL) is therefore resolved: "
        "both layers now route AVOID to AVOID.",
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
        "'; '-joined string by data_engine_v2 v5.74.0 Phase N. Carries "
        "engine-dropped valuation tags (5 from Phase H/I/P), "
        "forecast-unavailable tags (4 from Phase B), and any provider "
        "warning tags forwarded upstream.",
    )

    # Insights (5) -> total 90  (added v2.6.0)
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

    # ----- v2.12.0: tail emitted in EXACT data_engine_v2 key order -----
    # Engine order after Insights (pos 90): Decision pair, then the
    # scoring/provenance block (provider_rating, recommendation_source,
    # recommendation_priority_band, then the remaining Scoring-v5.74 fields),
    # then the candlestick block, then forecast_source, then (v2.13.0) the
    # Investability Gate block. The `group` labels ("Decision" / "Scoring v5.74"
    # / "Canonical Reco" / "Candlestick" / "Investability") are retained as
    # descriptive field metadata even though Canonical-Reco and Scoring-v5.74
    # are now physically interleaved — group is descriptive only and does not
    # affect column order or row projection.

    # Decision Matrix (2) -> total 92  (added v2.7.0)
    add(
        "Decision", "Recommendation Detail", "recommendation_detailed",
        "str", "text", False, "model",
        "Canonical 8-tier mirror of `recommendation`: STRONG_BUY / BUY / "
        "ACCUMULATE / HOLD / REDUCE / SELL / STRONG_SELL / AVOID. Written "
        "atomically next to `recommendation` by core.data_engine_v2 v5.77.17 "
        "_classify_recommendation_8tier() so the two fields cannot diverge. "
        "Also written by core.scoring v5.7.1 (AssetScores.recommendation_detailed). "
        "v5.7.1 widened the canonical vocabulary from 6 tiers to 8 (ACCUMULATE "
        "between BUY and HOLD; AVOID as the worst-case 'do not enter or hold' "
        "tier, no longer collapsed to STRONG_SELL). Empty when `recommendation` "
        "is set upstream by core.scoring -> core.reco_normalize.",
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
        "Empty when `recommendation` is upstream-set. "
        "DO NOT CONFUSE with recommendation_priority_band (Canonical Reco "
        "group, str P1..P5).",
    )

    # Provider Rating (Scoring v5.74) -> total 93  (engine places it here, before reco source)
    add(
        "Scoring v5.74", "Provider Rating", "provider_rating",
        "float", "0.00", False, "provider",
        "Analyst rating from provider feed (e.g. Yahoo/Finnhub buy/hold/sell "
        "rating mapped to numeric scale). Read by core.data_engine_v2 v5.74.0 "
        "_classify_recommendation_8tier() to drive the provider_override "
        "branch when the engine's own score disagrees with the provider "
        "analyst consensus by a configured margin.",
    )

    # Canonical Reco (2) -> total 95  (engine order: source THEN band)
    add(
        "Canonical Reco", "Recommendation Source", "recommendation_source",
        "str", "text", False, "system",
        "Engine recommendation provenance tag set by core.data_engine_v2 "
        "v5.74.0 _classify_recommendation_8tier(). Vocabulary: "
        "'engine' (engine-classified), 'provider_override' (provider "
        "rating took precedence), 'empty_row' (asset-aware empty guard "
        "fired), 'scoring_unavailable' (fallback path). DISTINCT from "
        "`scoring_recommendation_source` (Scoring v5.74 group), which "
        "holds the scoring.py provenance tag emitted by compute_scores(). "
        "Used by scoring_engine.apply_canonical_recommendation() as the "
        "idempotency key. v2.12.0: header aligned to the engine's "
        "INSTRUMENT_CANONICAL_HEADERS ('Recommendation Source', was "
        "'Reco Source').",
    )
    add(
        "Canonical Reco", "Priority Band", "recommendation_priority_band",
        "str", "text", False, "model",
        "Canonical priority band (P1 / P2 / P3 / P4 / P5) of the "
        "`recommendation` field. Computed by core.scoring v5.7.1 "
        "_compute_priority() centrally, so the top-line Recommendation "
        "column and any downstream P# [VERDICT] formatter (Apps Script "
        "04_Format.gs) cannot diverge by construction. DISTINCT from "
        "recommendation_priority (Decision group, int 1..8 for which "
        "8-tier rule fired). The two fields coexist by design. Under the "
        "v5.7.1 8-tier vocabulary, AVOID and STRONG_SELL both map to P1; "
        "ACCUMULATE shares the normal-BUY band (P3).",
    )

    # Scoring v5.74 (remaining 6) -> total 101
    add(
        "Scoring v5.74", "Scoring Reco Source", "scoring_recommendation_source",
        "str", "text", False, "system",
        "Provenance tag of the scoring.py recommendation, preserved "
        "SEPARATELY from the engine's `recommendation_source` field in "
        "the Canonical Reco group (which uses the vocabulary "
        "engine/provider_override/empty_row/scoring_unavailable). "
        "Captured by core.data_engine_v2 v5.75.0 _preserve_scoring_provenance() "
        "from the canonical compute_scores() patch. Typical value: "
        "'scoring.py v5.7.1' (the f-string is rendered from "
        "scoring.py's __version__, so it tracks scoring releases "
        "automatically). DISTINCT from `recommendation_source`.",
    )
    add(
        "Scoring v5.74", "Scoring Schema Version", "scoring_schema_version",
        "str", "text", False, "system",
        "Schema version tag from core.scoring at compute time (e.g. "
        "'scoring:5.4.2'). Lets downstream consumers detect rows scored "
        "under different scoring contracts and trigger backfill / "
        "recompute when the version changes.",
    )
    add(
        "Scoring v5.74", "Scoring Errors", "scoring_errors",
        "str", "text", False, "system",
        "'; '-joined error string from core.scoring (e.g. "
        "'canonical_scoring_failed:KeyError; missing_field:rsi_14'). "
        "Coerced from list/tuple/set form by core.data_engine_v2 v5.74.0 "
        "_coerce_scoring_errors_for_sheet() to guarantee sheet-safe "
        "output. Empty when scoring ran without errors.",
    )
    add(
        "Scoring v5.74", "Opportunity Source", "opportunity_source",
        "str", "text", False, "model",
        "Provenance for opportunity_score: which scoring branch produced "
        "the composite (canonical / local / fallback). Pass-through "
        "preserved by core.data_engine_v2 v5.74.0 "
        "_preserve_scoring_provenance(). Surfaces in audit logs when "
        "diagnosing why opportunity_score differs from overall_score.",
    )
    add(
        "Scoring v5.74", "Overall Score (Raw)", "overall_score_raw",
        "float", "0.00", False, "model",
        "Pre-penalty composite score from core.scoring v5.4.2+. The final "
        "`overall_score` is derived as overall_score_raw * "
        "overall_penalty_factor. Useful for debugging why a row scored "
        "lower than its component scores would suggest. When penalties "
        "are absent, overall_score_raw == overall_score.",
    )
    add(
        "Scoring v5.74", "Overall Penalty Factor", "overall_penalty_factor",
        "float", "0.0000", False, "model",
        "Penalty multiplier (0.0-1.0) applied to overall_score_raw to "
        "derive the final overall_score. Values < 1.0 indicate data-quality "
        "penalties (provider errors, engine-dropped valuations, forecast "
        "unavailable, sparse fundamentals) were applied. = 1.0 means no "
        "penalty. Surfaces alongside warnings to explain score divergence.",
    )

    # Candlestick (5) -> total 106  (engine places candlestick AFTER scoring/provenance)
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

    # Forecast Source (1) -> total 107  (v2.12.0; engine's pre-gate final key)
    add(
        "Forecast", "Forecast Source", "forecast_source",
        "str", "text", False, "model/system",
        "Forecast provenance set by core.data_engine_v2 v5.77.17: "
        "'provider_target' (engine honoured an upstream analyst target), "
        "'phase_ii_synthetic' (engine synthesized the forecast), or "
        "'fallback' (engine fallback path created it). Empty when no forecast "
        "was produced for the row. The engine's 107th / final pre-gate "
        "canonical key (added v5.77.1; elevated to a required canonical key "
        "in v5.77.3 _INSTRUMENT_CANONICAL_REQUIRED_KEYS).",
    )

    # ----- v2.13.0: Investability Gate (8) -> total 115 -----
    # Engine v5.79.2 (v5.78.0 gate) appends these AFTER forecast_source as the
    # FINAL canonical block (positions 108-115), group "Investability".
    # data_quality_score and forecast_reliability_score are 0-100 POINTS (NOT
    # fractions, like overall_score) -> do NOT percent-format; the engine rounds
    # them to 1 decimal. provider_engine_conflict holds the TEXT 'TRUE'/'FALSE'
    # (the engine sets the literal strings), NOT a bool -> dtype "str". A
    # provider/engine conflict is FLAGGED only; it does NOT override the engine
    # recommendation (v5.79.2).
    add(
        "Investability", "Data Quality Score", "data_quality_score",
        "float", "0.0", False, "model",
        "Weighted data-completeness score 0-100 (POINTS, not a fraction) across "
        "price, 52W range, volume, fundamentals, volatility, forecast, and "
        "overall_score buckets. D/E + FCF weights apply only to asset classes "
        "that support them (they drop out of both numerator and denominator "
        "otherwise). Set by core.data_engine_v2 v5.78.0+ investability gate; "
        "drives the BLOCKED hard-floor and WATCHLIST investable-minimum "
        "thresholds. Rounded to 1 decimal.",
    )
    add(
        "Investability", "Forecast Reliability Score", "forecast_reliability_score",
        "float", "0.0", False, "model",
        "Forecast confidence rebased to 0-100 (POINTS) then penalised for weak "
        "provenance: missing price (-60), missing forecast (-40), capped "
        "provider targets, dropped/rejected targets, and synthetic / fallback / "
        "momentum opportunity_source or forecast_source. Clamped to 0-100, "
        "rounded to 1 decimal. Set by core.data_engine_v2 v5.78.0+ "
        "investability gate.",
    )
    add(
        "Investability", "Provider/Engine Conflict", "provider_engine_conflict",
        "str", "text", False, "model",
        "TEXT flag 'TRUE' / 'FALSE' (NOT a bool). TRUE when the provider analyst "
        "direction and the engine recommendation direction disagree (provider "
        "bullish vs engine HOLD/TRIM, or provider cautious vs engine ADD). "
        "v5.79.1 compares canonical DIRECTION so a TEXT provider_rating "
        "(BUY / HOLD / SELL / ...) is handled (the prior numeric-only "
        "comparison left this FALSE for every row). A conflict is FLAGGED only "
        "and does NOT override the engine (v5.79.2).",
    )
    add(
        "Investability", "Conflict Type", "conflict_type",
        "str", "text", False, "model",
        "Human-readable conflict descriptor paired with provider_engine_conflict: "
        "'' (no comparison possible), 'Aligned', 'Provider bullish / engine "
        "cautious', or 'Provider cautious / engine constructive'.",
    )
    add(
        "Investability", "Final Decision Basis", "final_decision_basis",
        "str", "text", False, "model",
        "What drove the final action: 'Engine' normally, or 'Engine (provider "
        "conflict flagged)' when provider_engine_conflict is TRUE. The engine "
        "recommendation always drives investability_status; nothing overrides "
        "it (v5.79.2).",
    )
    add(
        "Investability", "Investability Status", "investability_status",
        "str", "text", False, "model",
        "Decision-readiness verdict: 'INVESTABLE', 'WATCHLIST', or 'BLOCKED'. "
        "BLOCKED on missing price / missing forecast / data quality below the "
        "hard floor. WATCHLIST on an excluded recommendation family, HOLD, "
        "moderate data quality, or incomplete fundamentals (D/E + FCF where the "
        "asset class requires them). INVESTABLE otherwise. Set by "
        "core.data_engine_v2 v5.78.0+ investability gate.",
    )
    add(
        "Investability", "Final Action", "final_action",
        "str", "text", False, "model",
        "Actionable verdict paired with investability_status: 'INVEST' "
        "(INVESTABLE), 'WATCH' (WATCHLIST), or 'DO_NOT_INVEST' (BLOCKED or "
        "excluded family). NOT financial advice.",
    )
    add(
        "Investability", "Block Reason", "block_reason",
        "str", "text", False, "model",
        "Short explanation when investability_status is not INVESTABLE, e.g. "
        "'Missing current price', 'No forecast available', 'Data quality below "
        "floor (NN)', 'Engine recommends reduce', 'Engine neutral (HOLD)', "
        "'Moderate data quality (NN)', 'Incomplete fundamentals (D/E, FCF)'. "
        "Empty when INVESTABLE.",
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
    # v2.12.0: reconciled to data_engine_v2 v5.77.17's INSIGHTS_KEYS /
    # INSIGHTS_HEADERS. Previously this was
    # [Section, Item, Symbol, Metric, Value, Notes, Last Updated (Riyadh)];
    # the engine's static contract is
    # [Section, Item, Metric, Value, Notes, Source, Sort Order]. Since the
    # engine is the locked contract, the registry adopts it. Column COUNT
    # stays 7. BEHAVIORAL CHANGE: drops `symbol` + `last_updated_riyadh`,
    # adds `source` + `sort_order` — insights_builder, the Insights route,
    # and any Apps Script reading the Insights page must use these keys.
    cols = (
        ColumnSpec("Insights", "Section", "section", "str", "text", True, "system", "e.g., Market Leaders Top 7, Portfolio Summary, Data Quality Alerts (insights_builder v7.0.0)."),
        ColumnSpec("Insights", "Item", "item", "str", "text", True, "system", "Row label within section."),
        ColumnSpec("Insights", "Metric", "metric", "str", "text", False, "system", "Metric name."),
        ColumnSpec("Insights", "Value", "value", "str", "text", False, "system", "Apps Script friendly value."),
        ColumnSpec("Insights", "Notes", "notes", "str", "text", False, "system", "Extra notes / explanation."),
        ColumnSpec("Insights", "Source", "source", "str", "text", False, "system", "Origin of the insight (module/provider/section that produced it)."),
        ColumnSpec("Insights", "Sort Order", "sort_order", "int", "0", False, "system", "Stable ordering index for rows within the sheet."),
    )
    return _sanitize_columns("Insights_Analysis", "insights_analysis", cols, enforce_symbol_first=False)


def _insights_criteria_fields() -> Tuple[CriteriaField, ...]:
    """
    Criteria block rendered at the top of Insights_Analysis as key/value rows.
    v2.8.0: 12 fields (aligned with core.analysis.criteria_model v3.1.0).
    v2.9.0/v2.10.0: no changes to criteria_fields. Field count remains 12.
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
        CriteriaField(
            "min_conviction_score", "Min Conviction Score", "float", "0",
            "Filter floor for conviction_score (0-100). 0 disables. "
            "Aligns with reco_normalize v7.2.0 floors: 60 mirrors "
            "STRONG_BUY downgrade floor, 45 mirrors BUY downgrade floor. "
            "(was 'min_conviction' in v2.6.0/v2.7.0)",
        ),
        CriteriaField(
            "exclude_engine_dropped_valuation", "Exclude Engine-Dropped Valuation",
            "bool", "false",
            "When true, drop rows where data_engine_v2 v5.74.0 cleared "
            "intrinsic_value / upside_pct upstream (5 tags from Phase H/I/P).",
        ),
        CriteriaField(
            "exclude_forecast_unavailable", "Exclude Forecast Unavailable",
            "bool", "false",
            "When true, drop rows where forecast synthesis was skipped "
            "(4 tags from data_engine_v2 v5.74.0 Phase B OR the "
            "forecast_unavailable bool flag).",
        ),
        CriteriaField(
            "exclude_provider_errors", "Exclude Provider Errors",
            "bool", "false",
            "When true, drop rows where the provider's last_error_class is "
            "non-empty (preserved by data_engine_v2 v5.74.0 Phase Q from "
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
            _CANONICAL_COLUMNS + _top10_extra_columns(),  # 118 columns in v2.13.0+
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
            missing_insights = [k for k in INSIGHTS_COLUMN_KEYS if k not in spec_keys]
            if missing_insights:
                raise ValueError(
                    f"[{sheet_name}] Missing required insights column(s): {missing_insights}. "
                    f"core.scoring v5.2.5 writes these as row fields."
                )
            missing_decision = [k for k in DECISION_COLUMN_KEYS if k not in spec_keys]
            if missing_decision:
                raise ValueError(
                    f"[{sheet_name}] Missing required decision matrix column(s): {missing_decision}. "
                    f"core.data_engine_v2 v5.74.0 writes these via _classify_recommendation_8tier()."
                )
            missing_candlestick = [k for k in CANDLESTICK_COLUMN_KEYS if k not in spec_keys]
            if missing_candlestick:
                raise ValueError(
                    f"[{sheet_name}] Missing required candlestick column(s): {missing_candlestick}. "
                    f"core.candlesticks.detect_patterns writes these via core.data_engine_v2 v5.74.0."
                )
            missing_canonical_reco = [k for k in CANONICAL_RECO_COLUMN_KEYS if k not in spec_keys]
            if missing_canonical_reco:
                raise ValueError(
                    f"[{sheet_name}] Missing required canonical reco column(s): "
                    f"{missing_canonical_reco}. core.scoring v5.2.8 writes these "
                    f"via compute_scores() and apply_canonical_recommendation()."
                )
            missing_scoring_v574 = [k for k in SCORING_V574_COLUMN_KEYS if k not in spec_keys]
            if missing_scoring_v574:
                raise ValueError(
                    f"[{sheet_name}] Missing required Scoring v5.74 column(s): "
                    f"{missing_scoring_v574}. core.data_engine_v2 v5.74.0 writes "
                    f"these via _compute_scores_canonical_first() and "
                    f"_preserve_scoring_provenance()."
                )
            # v2.12.0+: forecast_source required for schema alignment with
            # core.data_engine_v2 v5.77.17 (107th canonical key).
            missing_forecast_source = [k for k in FORECAST_SOURCE_COLUMN_KEYS if k not in spec_keys]
            if missing_forecast_source:
                raise ValueError(
                    f"[{sheet_name}] Missing required forecast source column(s): "
                    f"{missing_forecast_source}. core.data_engine_v2 v5.77.17 "
                    f"writes this as the final pre-gate canonical key."
                )
            # v2.13.0+: investability gate required for alignment with
            # core.data_engine_v2 v5.79.2 (8 columns, positions 108-115).
            missing_investability_gate = [k for k in INVESTABILITY_GATE_COLUMN_KEYS if k not in spec_keys]
            if missing_investability_gate:
                raise ValueError(
                    f"[{sheet_name}] Missing required investability gate column(s): "
                    f"{missing_investability_gate}. core.data_engine_v2 v5.78.0+ "
                    f"writes these as the decision-readiness layer (positions 108-115)."
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
            missing_canonical_reco = [k for k in CANONICAL_RECO_COLUMN_KEYS if k not in spec_keys]
            if missing_canonical_reco:
                raise ValueError(
                    f"[Top_10_Investments] Missing required canonical reco column(s): "
                    f"{missing_canonical_reco}."
                )
            missing_scoring_v574 = [k for k in SCORING_V574_COLUMN_KEYS if k not in spec_keys]
            if missing_scoring_v574:
                raise ValueError(
                    f"[Top_10_Investments] Missing required Scoring v5.74 column(s): "
                    f"{missing_scoring_v574}."
                )
            # v2.12.0+: forecast_source required for Top_10_Investments too.
            missing_forecast_source = [k for k in FORECAST_SOURCE_COLUMN_KEYS if k not in spec_keys]
            if missing_forecast_source:
                raise ValueError(
                    f"[Top_10_Investments] Missing required forecast source column(s): "
                    f"{missing_forecast_source}."
                )
            # v2.13.0+: investability gate required for Top_10_Investments too.
            missing_investability_gate = [k for k in INVESTABILITY_GATE_COLUMN_KEYS if k not in spec_keys]
            if missing_investability_gate:
                raise ValueError(
                    f"[Top_10_Investments] Missing required investability gate column(s): "
                    f"{missing_investability_gate}."
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
