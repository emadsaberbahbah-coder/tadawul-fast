#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/analysis/top10_selector.py
================================================================================
Top 10 Selector — v4.12.0
================================================================================
LIVE • SCHEMA-FIRST • ROUTE-COMPATIBLE • ENGINE-SELF-RESOLVING • JSON-SAFE
TOP10-METADATA GUARANTEED • SOURCE-PAGE SAFE • SNAPSHOT FALLBACK SAFE
SYNC+ASYNC CALLER TOLERANT • DISPLAY-HEADER TOLERANT • WRAPPER-PAYLOAD SAFE
PARTIAL-DEGRADATION SAFE • DIRECT-SYMBOLS SAFE • TIMEOUT-GUARDED
INSIGHTS-COLUMNS AWARE • DATA-QUALITY-AWARE RANKING
CRITERIA-v3.1.0 HARD-FILTER CONSUMER (v4.12.0)
DECISION-MATRIX AWARE • CANDLESTICK AWARE (v4.12.0)

================================================================================
Cross-stack family (May 2026)
================================================================================
This module sits at the tail of the May-2026 cross-stack family. The
contract surfaces it consumes from upstream are:

  - core.sheets.schema_registry v2.8.0
      * Canonical 100-column Top_10_Investments schema (97 instrument +
        3 Top10 extras). v2.7.0 added Decision Matrix (2) + Candlestick
        (5); v2.8.0 added the v3.1.0 criteria fields (rename
        min_conviction -> min_conviction_score; 3 new exclusion flags).
  - core.scoring v5.2.5
      * Produces the 5 Insights row fields (conviction_score,
        sector_relative_score, top_factors, top_risks,
        position_size_hint) and the canonical 5-tier recommendation.
  - core.reco_normalize v7.2.0
      * Applies the conviction-floor downgrade. Env-tunable via
        RECO_STRONG_BUY_CONVICTION_FLOOR=60 / RECO_BUY_CONVICTION_FLOOR=45.
        These floors mirror the suggested min_conviction_score values
        v4.12.0 honours below.
  - core.data_engine_v2 v5.60.0+
      * Phase B emits 4 forecast-unavailable tags + bool flag.
      * Phase H/I/P emits 5 engine-dropped-valuation tags.
      * Phase N canonicalizes `warnings` as a '; '-joined string.
      * Phase Q preserves provider `last_error_class` as a row field.
      * _classify_8tier() writes the Decision Matrix pair
        (recommendation_detailed, recommendation_priority).
  - core.candlesticks v1.0.0
      * Writes the 5 Candlestick columns via data_engine_v2 v5.60.0+
        (gated by ENGINE_CANDLESTICKS_ENABLED env flag).
  - core.scoring_engine v3.4.2 (compatibility bridge)
      * Tracks v5.2.5 enrichment fields and reco_normalize v7.2.0
        rule_id-aware classification surface.
  - core.analysis.criteria_model v3.1.0
      * Validated AdvisorCriteria pydantic model with the 4 new fields
        consumed in HARD filters here:
          min_conviction_score              (float, 0..100)
          exclude_engine_dropped_valuation  (bool, default false)
          exclude_forecast_unavailable      (bool, default false)
          exclude_provider_errors           (bool, default false)
  - core.analysis.investment_advisor_engine v4.4.0
      * Route-level dispatcher; calls build_top10_rows with criteria
        already normalized through criteria_model v3.1.0.
  - core.analysis.investment_advisor v5.3.0
      * Public-facing route wrapper.
  - core.analysis.insights_builder v7.0.0
      * Sibling consumer of the same data-quality signals; renders the
        Data Quality Alerts section in Insights_Analysis.

================================================================================
What v4.12.0 adds (over v4.11.0 / v4.10.0 / v4.9.0 / v4.8.0)
================================================================================
v4.11.0 added the upstream warning-tag parser helpers and applied SOFT
PENALTIES to `_selector_score` for engine-dropped valuation and
forecast-unavailable rows. The penalty foundation is preserved
verbatim. What v4.11.0 left on the table:

  - criteria_model v3.1.0 introduced four new criteria fields but
    top10_selector v4.11.0 did not consume them as HARD filters in
    `_passes_filters`. Operators could mark a row's valuation as
    upstream-dropped, but couldn't opt to drop those rows entirely.
  - schema_registry v2.7.0 / v2.8.0 added 7 new canonical columns
    (2 Decision Matrix + 5 Candlestick) but the local
    DEFAULT_FALLBACK_KEYS list was still at 93 cols, missing those 7.
  - DEFAULT_FALLBACK_KEYS placed opportunity_score / rank_overall as
    a "Composite (2)" fragment AFTER Views -- but the authoritative
    registry v2.8.0 has them INSIDE Scores(7) BEFORE Views.

v4.12.0 phase changes:

  A. Header docstring cross-stack sync. Adds references to the May
     2026 sibling modules above.

  B. Schema width 93 -> 100 cols. Adds Decision(2) + Candlestick(5)
     plus Upside % (which was already added in v4.9.0) and the 4
     Views columns and 5 Insights columns (added in v4.9.0/v4.10.0):
       - Valuation: upside_pct (between intrinsic_value and
         valuation_score) -- already in v4.11.0.
       - Views (4): fundamental_view, technical_view, risk_view,
         value_view (between Scores and Recommendation) -- v4.9.0.
       - Insights (5): sector_relative_score, conviction_score,
         top_factors, top_risks, position_size_hint (after Warnings,
         before Top10) -- v4.10.0.
       - Decision (2): recommendation_detailed, recommendation_priority
         (after Insights) -- NEW in v4.12.0 (data_engine_v2 v5.60.0+
         _classify_8tier output).
       - Candlestick (5): candlestick_pattern, candlestick_signal,
         candlestick_strength, candlestick_confidence,
         candlestick_patterns_recent (after Decision) -- NEW in
         v4.12.0 (core.candlesticks v1.0.0 via data_engine_v2).

  B1. Scores group ordering canonicalized: opportunity_score and
      rank_overall stay INSIDE Scores group (positions 65-66) BEFORE
      Views, matching schema_registry v2.8.0. Keyed-access callers
      are unaffected.

  C. ROW_KEY_ALIASES extended with camelCase + vendor mirrors for the
     seven new columns (recommendationDetailed, candlestickPattern,
     etc.) so legacy payloads normalize correctly.

  D. ** HEADLINE: HARD-FILTER CONSUMER for criteria_model v3.1.0. **
     `_passes_filters` now reads and honours four new criteria fields:

       min_conviction_score              (float, 0..100)
         When > 0, drops rows where conviction_score is missing OR
         below threshold. Aligns with reco_normalize v7.2.0 floors
         (RECO_STRONG_BUY_CONVICTION_FLOOR=60).

       exclude_engine_dropped_valuation  (bool, default false)
         When true, drops rows tagged with any of the 5 engine
         valuation-drop tags from data_engine_v2 v5.60.0+ Phase H/I/P:
           intrinsic_unit_mismatch_suspected
           upside_synthesis_suspect
           engine_52w_high_unit_mismatch_dropped
           engine_52w_low_unit_mismatch_dropped
           engine_52w_high_low_inverted

       exclude_forecast_unavailable      (bool, default false)
         When true, drops rows with any of the 4 forecast-unavailable
         tags from data_engine_v2 v5.60.0+ Phase B OR the bool flag:
           forecast_unavailable
           forecast_unavailable_no_source
           forecast_cleared_consistency_sweep
           forecast_skipped_unavailable

       exclude_provider_errors           (bool, default false)
         When true, drops rows where last_error_class is non-empty
         (preserved by data_engine_v2 v5.60.0+ Phase Q from the
         eodhd/yahoo_*/argaam/finnhub/tadawul providers).

     `_collect_criteria_from_inputs` is extended to pass these four
     fields through normalization so they survive the criteria
     plumbing intact.

  E. Meta surface: `applied_v310_filters` block in the response meta
     records which v3.1.0 filters fired and per-filter drop counts.
     Operators get an audit trail for opt-in hard exclusions.

  F. Version bump 4.11.0 -> 4.12.0.

================================================================================
Soft-penalty mechanics (preserved verbatim from v4.11.0)
================================================================================
`_selector_score` still applies soft penalties when data-quality
warning tags are present. These penalties REDUCE the composite score
without dropping the row, which is the v4.11.0 contract. v4.12.0 layers
the v3.1.0 hard filters ON TOP so operators can choose either or both:

  TOP10_PENALTY_ENGINE_DROP        env knob, default 8.0
  TOP10_PENALTY_FORECAST_UNAVAIL   env knob, default 10.0
  TOP10_PENALTY_PROVIDER_ERROR     env knob, default 6.0

A row tagged with both engine-drop AND forecast-unavail loses up to
~18 score points -- enough to push it out of the top 10 without the
operator having to opt in to hard exclusion. Rows with PROVIDER_ERROR
are penalized regardless of source page.

================================================================================
Why v4.8.0 (preserved)
================================================================================
- FIX: recognizes singular wrapper payloads like `quote`, `record`, and `item`
  in addition to plural envelopes, so valid single-row results are not dropped.
- FIX: lets sparse live page rows merge with snapshot rows instead of choosing
  one or the other, improving resilience when one source is only partially filled.
- FIX: allows Top10 output-page fallback to supplement sparse candidate pools,
  not only completely empty pools, reducing zero-row or under-filled results.
- FIX: preserves direct-symbol intent during final selection when the ranked
  result set is smaller than the requested limit.
- FIX: retains the earlier protections around signature-safe retries, wrapper
  payload safety, partial degradation, and emergency symbol fallback.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import logging
import math
import os
import re
import sys
import time
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger("core.analysis.top10_selector")
logger.addHandler(logging.NullHandler())

TOP10_SELECTOR_VERSION = "4.12.0"
# v4.12.0 Phase F: TFB module-version convention alias (mirrors
# schema_registry v2.8.0, scoring v5.2.5, reco_normalize v7.2.0,
# insights_builder v7.0.0, scoring_engine v3.4.2, criteria_model v3.1.0).
__version__ = TOP10_SELECTOR_VERSION
OUTPUT_PAGE = "Top_10_Investments"

DEFAULT_SOURCE_PAGES = [
    "Market_Leaders",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
    "My_Portfolio",
]

DERIVED_OR_NON_SOURCE_PAGES = {
    "KSA_TADAWUL",
    "Advisor_Criteria",
    "AI_Opportunity_Report",
    "Insights_Analysis",
    "Top_10_Investments",
    "Data_Dictionary",
}

TOP10_REQUIRED_FIELDS = (
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
)

TOP10_REQUIRED_HEADERS = {
    "top10_rank": "Top10 Rank",
    "selection_reason": "Selection Reason",
    "criteria_snapshot": "Criteria Snapshot",
}

DEFAULT_FALLBACK_KEYS = [
    # Identity (8)
    "symbol", "name", "asset_class", "exchange", "currency", "country", "sector", "industry",
    # Price (10)
    "current_price", "previous_close", "open_price", "day_high", "day_low",
    "week_52_high", "week_52_low", "price_change", "percent_change", "week_52_position_pct",
    # Liquidity (6)
    "volume", "avg_volume_10d", "avg_volume_30d", "market_cap", "float_shares", "beta_5y",
    # Fundamentals (12)
    "pe_ttm", "pe_forward", "eps_ttm", "dividend_yield", "payout_ratio", "revenue_ttm",
    "revenue_growth_yoy", "gross_margin", "operating_margin", "profit_margin", "debt_to_equity",
    "free_cash_flow_ttm",
    # Risk (8)
    "rsi_14", "volatility_30d", "volatility_90d", "max_drawdown_1y",
    "var_95_1d", "sharpe_1y", "risk_score", "risk_bucket",
    # Valuation (7)  -- v4.9.0 added upside_pct between intrinsic_value and valuation_score
    "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio",
    "intrinsic_value", "upside_pct", "valuation_score",
    # Forecast (9)
    "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
    "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "forecast_confidence", "confidence_score", "confidence_bucket",
    # Scores (7) -- opportunity_score & rank_overall stay INSIDE Scores (B1 alignment)
    "value_score", "quality_score", "momentum_score", "growth_score",
    "overall_score", "opportunity_score", "rank_overall",
    # Views (4) -- v4.9.0 (core.scoring v5.2.5)
    "fundamental_view", "technical_view", "risk_view", "value_view",
    # Recommendation (4)
    "recommendation", "recommendation_reason", "horizon_days", "invest_period_label",
    # Portfolio (6)
    "position_qty", "avg_cost", "position_cost", "position_value",
    "unrealized_pl", "unrealized_pl_pct",
    # Provenance (4)
    "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",
    # Insights (5) -- v4.10.0 (core.scoring v5.2.5 row fields)
    "sector_relative_score", "conviction_score", "top_factors", "top_risks", "position_size_hint",
    # Decision Matrix (2) -- v4.12.0 (core.data_engine_v2 v5.60.0+ _classify_8tier)
    "recommendation_detailed", "recommendation_priority",
    # Candlestick (5) -- v4.12.0 (core.candlesticks v1.0.0)
    "candlestick_pattern", "candlestick_signal", "candlestick_strength",
    "candlestick_confidence", "candlestick_patterns_recent",
    # Top10 extras (3)
    "top10_rank", "selection_reason", "criteria_snapshot",
]

DEFAULT_FALLBACK_HEADERS = [
    # Identity (8)
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
    # Price (10)
    "Current Price", "Previous Close", "Open", "Day High", "Day Low", "52W High", "52W Low",
    "Price Change", "Percent Change", "52W Position %",
    # Liquidity (6)
    "Volume", "Avg Volume 10D", "Avg Volume 30D", "Market Cap", "Float Shares", "Beta (5Y)",
    # Fundamentals (12)
    "P/E (TTM)", "P/E (Forward)", "EPS (TTM)", "Dividend Yield", "Payout Ratio", "Revenue (TTM)",
    "Revenue Growth YoY", "Gross Margin", "Operating Margin", "Profit Margin", "Debt/Equity",
    "Free Cash Flow (TTM)",
    # Risk (8)
    "RSI (14)", "Volatility 30D", "Volatility 90D", "Max Drawdown 1Y", "VaR 95% (1D)",
    "Sharpe (1Y)", "Risk Score", "Risk Bucket",
    # Valuation (7)
    "P/B", "P/S", "EV/EBITDA", "PEG", "Intrinsic Value", "Upside %", "Valuation Score",
    # Forecast (9)
    "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M",
    "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M",
    "Forecast Confidence", "Confidence Score", "Confidence Bucket",
    # Scores (7)
    "Value Score", "Quality Score", "Momentum Score", "Growth Score",
    "Overall Score", "Opportunity Score", "Rank (Overall)",
    # Views (4)
    "Fundamental View", "Technical View", "Risk View", "Value View",
    # Recommendation (4)
    "Recommendation", "Recommendation Reason", "Horizon Days", "Invest Period Label",
    # Portfolio (6)
    "Position Qty", "Avg Cost", "Position Cost", "Position Value", "Unrealized P/L",
    "Unrealized P/L %",
    # Provenance (4)
    "Data Provider", "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",
    # Insights (5)
    "Sector-Adj Score", "Conviction Score", "Top Factors", "Top Risks", "Position Size Hint",
    # Decision Matrix (2)
    "Recommendation Detail", "Reco Priority",
    # Candlestick (5)
    "Candle Pattern", "Candle Signal", "Candle Strength",
    "Candle Confidence", "Recent Patterns (5D)",
    # Top10 extras (3)
    "Top10 Rank", "Selection Reason", "Criteria Snapshot",
]

ROW_KEY_ALIASES: Dict[str, Tuple[str, ...]] = {
    "symbol": ("symbol", "ticker", "code", "instrument", "security", "symbol_normalized", "requested_symbol"),
    "name": ("name", "company_name", "long_name", "instrument_name", "security_name"),
    "asset_class": ("asset_class",),
    "exchange": ("exchange",),
    "currency": ("currency",),
    "country": ("country",),
    "sector": ("sector",),
    "industry": ("industry",),
    "current_price": ("current_price", "price", "last_price", "last", "close", "market_price", "nav"),
    "previous_close": ("previous_close",),
    "open_price": ("open_price", "open"),
    "day_high": ("day_high", "high"),
    "day_low": ("day_low", "low"),
    "week_52_high": ("week_52_high", "52w_high"),
    "week_52_low": ("week_52_low", "52w_low"),
    "price_change": ("price_change", "change"),
    "percent_change": ("percent_change", "change_pct", "pct_change"),
    "week_52_position_pct": ("week_52_position_pct",),
    "volume": ("volume", "avg_volume", "trading_volume"),
    "avg_volume_10d": ("avg_volume_10d",),
    "avg_volume_30d": ("avg_volume_30d",),
    "market_cap": ("market_cap",),
    "float_shares": ("float_shares",),
    "beta_5y": ("beta_5y",),
    "pe_ttm": ("pe_ttm",),
    "pe_forward": ("pe_forward",),
    "eps_ttm": ("eps_ttm",),
    "dividend_yield": ("dividend_yield",),
    "payout_ratio": ("payout_ratio",),
    "revenue_ttm": ("revenue_ttm",),
    "revenue_growth_yoy": ("revenue_growth_yoy",),
    "gross_margin": ("gross_margin",),
    "operating_margin": ("operating_margin",),
    "profit_margin": ("profit_margin",),
    "debt_to_equity": ("debt_to_equity",),
    "free_cash_flow_ttm": ("free_cash_flow_ttm",),
    "rsi_14": ("rsi_14",),
    "volatility_30d": ("volatility_30d",),
    "volatility_90d": ("volatility_90d",),
    "max_drawdown_1y": ("max_drawdown_1y",),
    "var_95_1d": ("var_95_1d",),
    "sharpe_1y": ("sharpe_1y",),
    "risk_score": ("risk_score", "risk"),
    "risk_bucket": ("risk_bucket", "risk_level"),
    "pb_ratio": ("pb_ratio",),
    "ps_ratio": ("ps_ratio",),
    "ev_ebitda": ("ev_ebitda",),
    "peg_ratio": ("peg_ratio",),
    "intrinsic_value": ("intrinsic_value", "intrinsicValue", "fair_value", "target_intrinsic"),
    "upside_pct": ("upside_pct", "upsidePct", "upside_percent", "upside", "intrinsic_upside"),
    "valuation_score": ("valuation_score", "valuationScore"),
    "forecast_price_1m": ("forecast_price_1m",),
    "forecast_price_3m": ("forecast_price_3m",),
    "forecast_price_12m": ("forecast_price_12m",),
    "expected_roi_1m": ("expected_roi_1m", "roi_1m", "expected_return_1m"),
    "expected_roi_3m": ("expected_roi_3m", "roi_3m", "expected_return_3m"),
    "expected_roi_12m": ("expected_roi_12m", "roi_12m", "expected_return_12m"),
    "forecast_confidence": ("forecast_confidence", "confidence_score", "ai_confidence"),
    "confidence_score": ("confidence_score", "forecast_confidence", "ai_confidence"),
    "confidence_bucket": ("confidence_bucket", "confidence_level"),
    "value_score": ("value_score",),
    "quality_score": ("quality_score",),
    "momentum_score": ("momentum_score",),
    "growth_score": ("growth_score",),
    "overall_score": ("overall_score", "advisor_score", "score"),
    "opportunity_score": ("opportunity_score",),
    "rank_overall": ("rank_overall",),
    "recommendation": ("recommendation", "reco", "signal"),
    "recommendation_reason": ("recommendation_reason",),
    "horizon_days": ("horizon_days", "invest_period_days", "investment_period_days"),
    "invest_period_label": ("invest_period_label", "horizon_label"),
    "position_qty": ("position_qty",),
    "avg_cost": ("avg_cost",),
    "position_cost": ("position_cost",),
    "position_value": ("position_value",),
    "unrealized_pl": ("unrealized_pl",),
    "unrealized_pl_pct": ("unrealized_pl_pct",),
    "data_provider": ("data_provider",),
    "last_updated_utc": ("last_updated_utc",),
    "last_updated_riyadh": ("last_updated_riyadh",),
    "warnings": ("warnings", "warning"),
    "liquidity_score": ("liquidity_score",),
    # Views (4) — v4.9.0; produced by core.scoring v5.2.5 derive_*_view()
    "fundamental_view": ("fundamental_view", "fundamentalView", "fundamentals_view"),
    "technical_view": ("technical_view", "technicalView", "technicals_view"),
    "risk_view": ("risk_view", "riskView"),
    "value_view": ("value_view", "valueView", "valuation_view"),
    # Insights (5) — v4.10.0; produced by core.scoring v5.2.5 row fields
    "sector_relative_score": ("sector_relative_score", "sectorRelativeScore", "sector_adj_score", "sectorAdjScore"),
    "conviction_score": ("conviction_score", "convictionScore", "conviction"),
    "top_factors": ("top_factors", "topFactors", "key_factors", "keyFactors"),
    "top_risks": ("top_risks", "topRisks", "key_risks", "keyRisks"),
    "position_size_hint": ("position_size_hint", "positionSizeHint", "position_size", "size_hint"),
    # Decision Matrix (2) — v4.12.0; produced by core.data_engine_v2 v5.60.0+ _classify_8tier
    "recommendation_detailed": (
        "recommendation_detailed", "recommendationDetailed", "recommendation_detail",
        "recommendationDetail", "decision_detailed", "decision_recommendation",
    ),
    "recommendation_priority": (
        "recommendation_priority", "recommendationPriority", "reco_priority",
        "recoPriority", "decision_priority",
    ),
    # Candlestick (5) — v4.12.0; produced by core.candlesticks v1.0.0
    "candlestick_pattern": ("candlestick_pattern", "candlestickPattern", "candle_pattern", "candlePattern"),
    "candlestick_signal": ("candlestick_signal", "candlestickSignal", "candle_signal", "candleSignal"),
    "candlestick_strength": ("candlestick_strength", "candlestickStrength", "candle_strength", "candleStrength"),
    "candlestick_confidence": ("candlestick_confidence", "candlestickConfidence", "candle_confidence", "candleConfidence"),
    "candlestick_patterns_recent": (
        "candlestick_patterns_recent", "candlestickPatternsRecent", "candle_patterns_recent",
        "recent_candle_patterns", "recent_patterns",
    ),
    "selection_reason": ("selection_reason", "selector_reason"),
    "top10_rank": ("top10_rank", "rank"),
    "criteria_snapshot": ("criteria_snapshot", "criteria_json"),
    "source_page": ("source_page", "page", "sheet", "sheet_name"),
}

CANONICAL_KEY_SET = set(DEFAULT_FALLBACK_KEYS)
WRAPPER_KEYS = {
    "status", "page", "sheet", "sheet_name", "route_family", "headers", "display_headers", "sheet_headers",
    "column_headers", "keys", "columns", "fields", "rows", "rows_matrix", "matrix", "row_objects", "records",
    "items", "item", "quotes", "quote", "data", "record", "result", "payload", "response", "output", "meta", "count", "version",
    "snapshot", "envelope", "content", "schema", "sheet_spec", "spec",
}

_ENGINE_CACHE: Optional[Any] = None
_ENGINE_CACHE_SOURCE: str = ""
_ENGINE_LOCK = asyncio.Lock()


# =============================================================================
# Runtime knobs
# =============================================================================
def _env_float(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, str(default)).strip())
        if math.isnan(value) or math.isinf(value):
            return default
        return max(0.1, value)
    except Exception:
        return default


def _env_int(name: str, default: int, minimum: int = 1, maximum: int = 100000) -> int:
    try:
        value = int(float(os.getenv(name, str(default)).strip()))
        return max(minimum, min(value, maximum))
    except Exception:
        return default


ENGINE_CALL_TIMEOUT_SEC = _env_float("TFB_TOP10_ENGINE_CALL_TIMEOUT_SEC", 8.0)
PAGE_TOTAL_TIMEOUT_SEC = _env_float("TFB_TOP10_PAGE_TIMEOUT_SEC", 12.0)
BUILDER_TOTAL_TIMEOUT_SEC = _env_float("TFB_TOP10_TOTAL_TIMEOUT_SEC", 32.0)
SOURCE_PAGE_LIMIT = _env_int("TOP10_SELECTOR_SOURCE_PAGE_LIMIT", 80, minimum=10, maximum=1000)
HYDRATION_SYMBOL_CAP = _env_int("TOP10_SELECTOR_HYDRATION_SYMBOL_CAP", 30, minimum=5, maximum=250)
MAX_SOURCE_PAGES = _env_int("TOP10_SELECTOR_MAX_SOURCE_PAGES", 5, minimum=1, maximum=20)
MAX_LIMIT = _env_int("TOP10_SELECTOR_MAX_LIMIT", 50, minimum=1, maximum=200)
EARLY_STOP_MULTIPLIER = _env_int("TOP10_SELECTOR_EARLY_STOP_MULTIPLIER", 6, minimum=2, maximum=20)
EMERGENCY_SYMBOLS = [
    s for s in [x.strip() for x in os.getenv("TOP10_SELECTOR_EMERGENCY_SYMBOLS", "").replace(";", ",").split(",")] if s
]

# -----------------------------------------------------------------------------
# v4.11.0 soft-penalty env knobs (preserved verbatim in v4.12.0)
# -----------------------------------------------------------------------------
# Applied in _selector_score when upstream warning tags / provider errors
# are present. These REDUCE the composite score without dropping the row.
# v4.12.0 layers the v3.1.0 HARD filters on top (see _passes_filters).
TOP10_PENALTY_ENGINE_DROP = _env_float("TOP10_PENALTY_ENGINE_DROP", 8.0)
TOP10_PENALTY_FORECAST_UNAVAIL = _env_float("TOP10_PENALTY_FORECAST_UNAVAIL", 10.0)
TOP10_PENALTY_PROVIDER_ERROR = _env_float("TOP10_PENALTY_PROVIDER_ERROR", 6.0)

# -----------------------------------------------------------------------------
# v4.11.0 warning-tag vocabularies (consumed by _passes_filters + _selector_score)
# -----------------------------------------------------------------------------
# 5 engine-dropped-valuation tags from data_engine_v2 v5.60.0+ Phase H/I/P:
_ENGINE_DROP_TAGS = (
    "intrinsic_unit_mismatch_suspected",
    "upside_synthesis_suspect",
    "engine_52w_high_unit_mismatch_dropped",
    "engine_52w_low_unit_mismatch_dropped",
    "engine_52w_high_low_inverted",
)

# 4 forecast-unavailable tags from data_engine_v2 v5.60.0+ Phase B:
_FORECAST_UNAVAIL_TAGS = (
    "forecast_unavailable",
    "forecast_unavailable_no_source",
    "forecast_cleared_consistency_sweep",
    "forecast_skipped_unavailable",
)


# =============================================================================
# Optional schema/page catalog
# =============================================================================
try:
    from core.sheets.schema_registry import get_sheet_spec as _get_sheet_spec  # type: ignore
except Exception:
    _get_sheet_spec = None  # type: ignore

try:
    from core.sheets.page_catalog import normalize_page_name as _normalize_page_name  # type: ignore
except Exception:
    _normalize_page_name = None  # type: ignore


# =============================================================================
# Basic helpers
# =============================================================================
def _s(v: Any) -> str:
    try:
        if v is None:
            return ""
        s = str(v).strip()
        return "" if s.lower() in {"none", "null", "nil"} else s
    except Exception:
        return ""


def _is_blank(v: Any) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())


def _safe_int(v: Any, default: int) -> int:
    try:
        if isinstance(v, bool):
            return default
        return int(float(v))
    except Exception:
        return default


def _safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None or isinstance(v, bool):
            return default
        if isinstance(v, (int, float)):
            f = float(v)
            if math.isnan(f) or math.isinf(f):
                return default
            return f
        s = _s(v).replace(",", "")
        if not s:
            return default
        if s.endswith("%"):
            f = float(s[:-1].strip()) / 100.0
        else:
            f = float(s)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except Exception:
        return default


def _safe_ratio(v: Any, default: Optional[float] = None) -> Optional[float]:
    f = _safe_float(v, default)
    if f is None:
        return default
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _coerce_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False
    if isinstance(v, (int, float)):
        try:
            return bool(int(v))
        except Exception:
            return default
    return default


# =============================================================================
# v4.11.0/v4.12.0 — Upstream warning-tag inspection helpers
# =============================================================================
# Used by:
#   _passes_filters    — v4.12.0 HARD-filter consumer for criteria_model
#                        v3.1.0 exclusion flags (exclude_engine_dropped_valuation
#                        / exclude_forecast_unavailable / exclude_provider_errors)
#   _selector_score    — v4.11.0 SOFT penalty foundation (preserved verbatim)
#
# Tag vocabularies come from data_engine_v2 v5.60.0+ Phase H/I/P (engine drop)
# and Phase B (forecast unavailable). The `warnings` field is canonicalized
# as a '; '-joined string by Phase N; we still accept list-shaped warnings
# for defensive parsing.

def _parse_warnings_tags(row: Mapping[str, Any]) -> List[str]:
    """Extract individual warning tags from a row's `warnings` field.

    Returns a lowercased, stripped list. Accepts:
      - '; '-joined string (canonical post-Phase-N form)
      - ',' or '|' joined string (legacy variants)
      - list[str] (defensive)
      - None / blank (returns [])
    """
    raw = row.get("warnings")
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        seq = [_s(x).lower() for x in raw]
        return [s for s in seq if s]
    s = _s(raw).lower()
    if not s:
        return []
    # Try the canonical separator first, then fall back to common alternatives.
    for sep in (";", "|", ","):
        if sep in s:
            return [t.strip() for t in s.split(sep) if t.strip()]
    return [s] if s else []


def _has_engine_drop_tag(tags: Sequence[str]) -> bool:
    """True if any of the 5 engine-dropped-valuation tags is present."""
    if not tags:
        return False
    tag_set = set(tags)
    for marker in _ENGINE_DROP_TAGS:
        if marker in tag_set:
            return True
    # Defensive substring match — tags occasionally arrive with prefixes/suffixes.
    joined = " ".join(tags)
    return any(marker in joined for marker in _ENGINE_DROP_TAGS)


def _has_forecast_unavail_tag(tags: Sequence[str], row: Mapping[str, Any]) -> bool:
    """True if any forecast-unavailable tag fires OR the bool flag is set."""
    # Phase B bool flag short-circuit
    if _coerce_bool(row.get("forecast_unavailable"), False):
        return True
    if not tags:
        return False
    tag_set = set(tags)
    for marker in _FORECAST_UNAVAIL_TAGS:
        if marker in tag_set:
            return True
    joined = " ".join(tags)
    return any(marker in joined for marker in _FORECAST_UNAVAIL_TAGS)


def _has_provider_error(row: Mapping[str, Any]) -> bool:
    """True if the row carries a provider error class from Phase Q."""
    for key in ("last_error_class", "provider_last_error_class", "provider_error", "error_class"):
        val = row.get(key)
        if val is None:
            continue
        s = _s(val).lower()
        if s and s not in {"none", "null", "ok", "success", "false", "0", "no"}:
            return True
    # Generic `error` field as a defensive last resort (some providers surface
    # this as a non-empty string when last_error_class is missing).
    err = _s(row.get("error"))
    return bool(err) and err.lower() not in {"none", "null", "false", "0"}


def _is_signature_mismatch_typeerror(exc: TypeError) -> bool:
    msg = _s(exc).lower()
    if not msg:
        return False
    signature_markers = (
        "unexpected keyword argument",
        "positional argument",
        "required positional argument",
        "takes ",
        "got an unexpected keyword",
        "multiple values for argument",
        "missing 1 required positional argument",
        "missing required positional argument",
        "too many positional arguments",
        "not enough positional arguments",
    )
    return any(marker in msg for marker in signature_markers)


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, Decimal):
        try:
            f = float(value)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        except Exception:
            return str(value)
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    try:
        if hasattr(value, "model_dump") and callable(getattr(value, "model_dump")):
            return _json_safe(value.model_dump(mode="python"))
    except Exception:
        pass
    try:
        if hasattr(value, "dict") and callable(getattr(value, "dict")):
            return _json_safe(value.dict())
    except Exception:
        pass
    try:
        if hasattr(value, "__dict__"):
            return _json_safe(dict(value.__dict__))
    except Exception:
        pass
    try:
        return str(value)
    except Exception:
        return None


def _json_compact(value: Any) -> str:
    try:
        return json.dumps(_json_safe(value), ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return str(value)


def _normalize_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        seq = [x.strip() for x in value.replace(";", ",").replace("\n", ",").split(",") if x.strip()]
    elif isinstance(value, (list, tuple, set)):
        seq = list(value)
    else:
        seq = [value]
    out: List[str] = []
    seen = set()
    for item in seq:
        s = _s(item)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _dedupe_keep_order(values: Iterable[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        s = _s(value)
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _normalize_symbol(sym: Any) -> str:
    s = _s(sym).upper().replace(" ", "")
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1]
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    if s.isdigit():
        return f"{s}.SR"
    return s


def _looks_like_symbol_token(x: Any) -> bool:
    s = _s(x).upper()
    if not s or " " in s or len(s) > 24:
        return False
    return bool(re.fullmatch(r"[A-Z0-9\.\=\-\^:_/]{1,24}", s))


def _safe_source_pages(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in values:
        s = _s(item)
        if not s or s in DERIVED_OR_NON_SOURCE_PAGES:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out[:MAX_SOURCE_PAGES]


def _normalize_page_name_safe(name: str) -> str:
    s = _s(name)
    if not s:
        return ""
    if callable(_normalize_page_name):
        try:
            return _normalize_page_name(s, allow_output_pages=False)
        except TypeError:
            try:
                return _normalize_page_name(s)
            except Exception:
                return s
        except Exception:
            return s
    return s


def _header_to_key(header: Any) -> str:
    s = _s(header)
    if not s:
        return ""
    out: List[str] = []
    prev_us = False
    for ch in s:
        if ch.isalnum():
            out.append(ch.lower())
            prev_us = False
        else:
            if not prev_us:
                out.append("_")
                prev_us = True
    key = "".join(out).strip("_")
    while "__" in key:
        key = key.replace("__", "_")
    key = key.replace("52w", "week_52")
    key = key.replace("week52", "week_52")
    key = key.replace("p_e_", "pe_")
    key = key.replace("p_b", "pb")
    key = key.replace("p_s", "ps")
    return key


def _compact_key(value: Any) -> str:
    s = _s(value).lower()
    if not s:
        return ""
    s = s.replace("52w", "week52")
    return re.sub(r"[^a-z0-9]+", "", s)


def _canonical_key_variants(name: str) -> List[str]:
    base = _s(name)
    variants = {base, base.lower(), _header_to_key(base), _compact_key(base)}
    if base.endswith("_pct"):
        variants.add(base[:-4] + "_percent")
        variants.add(_compact_key(base[:-4] + "percent"))
    if base.startswith("week_52"):
        variants.add(base.replace("week_52", "52w"))
        variants.add(_compact_key(base.replace("week_52", "52w")))
    if base == "open_price":
        variants.update({"open", "openprice"})
    return [v for v in variants if v]


def _row_lookup(row: Mapping[str, Any]) -> Dict[str, Any]:
    lookup: Dict[str, Any] = {}
    for k, v in row.items():
        raw = _s(k)
        if not raw:
            continue
        for token in {raw, raw.lower(), _header_to_key(raw), _compact_key(raw)}:
            if token and token not in lookup:
                lookup[token] = v
    return lookup


def _coerce_mapping(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)
    if isinstance(obj, Mapping):
        try:
            return dict(obj)
        except Exception:
            return {}
    try:
        if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
            d = obj.model_dump(mode="python")
            if isinstance(d, Mapping):
                return dict(d)
    except Exception:
        pass
    try:
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            d = obj.dict()
            if isinstance(d, Mapping):
                return dict(d)
    except Exception:
        pass
    try:
        d = getattr(obj, "__dict__", None)
        if isinstance(d, Mapping):
            return dict(d)
    except Exception:
        pass
    return {}


def _count_nonblank_fields(row: Mapping[str, Any]) -> int:
    count = 0
    for value in row.values():
        if isinstance(value, str):
            if value.strip():
                count += 1
        elif value not in (None, [], {}, ()):
            count += 1
    return count


def _rows_to_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    return [[_json_safe(row.get(k)) for k in keys] for row in rows]


def _looks_like_row_dict(d: Any) -> bool:
    if not isinstance(d, Mapping) or not d:
        return False
    row = _coerce_mapping(d)
    if not row:
        return False
    lookup = _row_lookup(row)
    if any(token in lookup for token in ("symbol", "ticker", "requested_symbol", "code", "instrument")):
        return True
    if "top10rank" in lookup or "selectionreason" in lookup:
        return True
    matched = 0
    for key in ("name", "current_price", "overall_score", "recommendation", "risk_score", "forecast_confidence"):
        for token in _canonical_key_variants(key):
            if token in lookup:
                matched += 1
                break
    if matched >= 2:
        return True
    non_meta = [k for k in row.keys() if _s(k) not in WRAPPER_KEYS]
    return len(non_meta) >= 4


# =============================================================================
# Schema helpers
# =============================================================================
def _complete_schema_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    raw_headers = list(headers or [])
    raw_keys = list(keys or [])
    max_len = max(len(raw_headers), len(raw_keys))
    out_headers: List[str] = []
    out_keys: List[str] = []
    for i in range(max_len):
        h = _s(raw_headers[i]) if i < len(raw_headers) else ""
        k = _s(raw_keys[i]) if i < len(raw_keys) else ""
        if h and not k:
            k = _header_to_key(h)
        elif k and not h:
            h = k.replace("_", " ").title()
        elif not h and not k:
            h = f"Column_{i+1}"
            k = f"key_{i+1}"
        out_headers.append(h)
        out_keys.append(k)
    return out_headers, out_keys


def _schema_columns_from_any(spec: Any) -> List[Any]:
    if spec is None:
        return []
    if isinstance(spec, dict) and len(spec) == 1 and "columns" not in spec and "fields" not in spec:
        first_val = list(spec.values())[0]
        if isinstance(first_val, dict) and ("columns" in first_val or "fields" in first_val):
            spec = first_val
    cols = getattr(spec, "columns", None)
    if isinstance(cols, list) and cols:
        return cols
    fields = getattr(spec, "fields", None)
    if isinstance(fields, list) and fields:
        return fields
    if isinstance(spec, Mapping):
        cols2 = spec.get("columns") or spec.get("fields")
        if isinstance(cols2, list) and cols2:
            return cols2
    try:
        d = getattr(spec, "__dict__", None)
        if isinstance(d, dict):
            cols3 = d.get("columns") or d.get("fields")
            if isinstance(cols3, list) and cols3:
                return cols3
    except Exception:
        pass
    return []


def _schema_keys_headers_from_spec(spec: Any) -> Tuple[List[str], List[str]]:
    if isinstance(spec, dict) and len(spec) == 1 and not any(
        k in spec for k in ("columns", "fields", "headers", "keys", "display_headers")
    ):
        first_val = list(spec.values())[0]
        if isinstance(first_val, dict):
            spec = first_val
    headers: List[str] = []
    keys: List[str] = []
    cols = _schema_columns_from_any(spec)
    for c in cols:
        if isinstance(c, Mapping):
            h = _s(c.get("header") or c.get("display_header") or c.get("displayHeader") or c.get("label") or c.get("title"))
            k = _s(c.get("key") or c.get("field") or c.get("name") or c.get("id"))
        else:
            h = _s(
                getattr(c, "header", getattr(c, "display_header", getattr(c, "displayHeader", getattr(c, "label", getattr(c, "title", None)))))
            )
            k = _s(getattr(c, "key", getattr(c, "field", getattr(c, "name", getattr(c, "id", None)))))
        if h or k:
            headers.append(h or k.replace("_", " ").title())
            keys.append(k or _header_to_key(h))
    if not headers and not keys and isinstance(spec, Mapping):
        headers2 = spec.get("headers") or spec.get("display_headers")
        keys2 = spec.get("keys") or spec.get("columns") or spec.get("fields")
        if isinstance(headers2, list):
            headers = [_s(x) for x in headers2 if _s(x)]
        if isinstance(keys2, list):
            keys = [_s(x) for x in keys2 if _s(x)]
    return _complete_schema_contract(headers, keys)


def _ensure_top10_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    out_headers = list(headers or [])
    out_keys = list(keys or [])
    for field in TOP10_REQUIRED_FIELDS:
        if field not in out_keys:
            out_keys.append(field)
            out_headers.append(TOP10_REQUIRED_HEADERS[field])
    return _complete_schema_contract(out_headers, out_keys)


def _load_schema_defaults() -> Tuple[List[str], List[str]]:
    if callable(_get_sheet_spec):
        try:
            spec = _get_sheet_spec(OUTPUT_PAGE)
            headers, keys = _schema_keys_headers_from_spec(spec)
            if keys:
                headers, keys = _ensure_top10_contract(headers, keys)
                return list(headers), list(keys)
        except Exception:
            pass
    headers, keys = _ensure_top10_contract(DEFAULT_FALLBACK_HEADERS, DEFAULT_FALLBACK_KEYS)
    return list(headers), list(keys)


# =============================================================================
# Engine detection / resolution
# =============================================================================
_ENGINE_METHOD_NAMES = (
    "get_sheet_rows", "get_page_rows", "sheet_rows", "build_sheet_rows", "execute_sheet_rows",
    "run_sheet_rows", "build_analysis_sheet_rows", "run_analysis_sheet_rows", "get_rows_for_sheet",
    "get_rows_for_page", "get_page_data", "get_sheet_data", "build_page_rows", "build_page_data",
    "get_cached_sheet_snapshot", "get_sheet_snapshot", "get_cached_sheet_rows", "get_page_snapshot",
    "get_enriched_quotes_batch", "get_analysis_quotes_batch", "get_quotes_batch", "quotes_batch",
    "get_quotes", "get_quote", "get_quote_dict", "get_enriched_quote",
)

_ENGINE_HOLDER_ATTRS = (
    "engine", "data_engine", "quote_engine", "cache_engine", "_engine", "_data_engine",
    "service", "runner", "advisor_engine",
)

_APP_ATTRS = ("app", "application", "fastapi_app", "api")
_STATE_ATTRS = ("state", "app_state")


def _safe_getattr(obj: Any, name: str, default: Any = None) -> Any:
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


def _looks_like_engine(obj: Any) -> bool:
    if obj is None:
        return False
    if isinstance(obj, (str, bytes, int, float, bool, list, tuple, set)):
        return False
    if isinstance(obj, Mapping):
        return False
    return any(callable(getattr(obj, m, None)) for m in _ENGINE_METHOD_NAMES)


def _iter_mapping_values(mapping: Mapping[str, Any]) -> Iterable[Tuple[str, Any]]:
    try:
        for k, v in mapping.items():
            yield _s(k), v
    except Exception:
        return


def _iter_object_values(obj: Any) -> Iterable[Tuple[str, Any]]:
    if obj is None:
        return
    if isinstance(obj, Mapping):
        yield from _iter_mapping_values(obj)
        return
    names: List[str] = []
    try:
        if hasattr(obj, "__dict__") and isinstance(getattr(obj, "__dict__", None), dict):
            names.extend([n for n in obj.__dict__.keys() if isinstance(n, str)])
    except Exception:
        pass
    preferred = list(_ENGINE_HOLDER_ATTRS) + list(_APP_ATTRS) + list(_STATE_ATTRS)
    names = preferred + [n for n in names if n not in preferred]
    seen = set()
    for name in names:
        if not name or name in seen:
            continue
        seen.add(name)
        try:
            yield name, getattr(obj, name)
        except Exception:
            continue


def _collect_engine_candidates_from_object(
    obj: Any,
    prefix: str = "",
    seen: Optional[set] = None,
    depth: int = 0,
) -> List[Tuple[Any, str]]:
    if seen is None:
        seen = set()
    out: List[Tuple[Any, str]] = []
    if obj is None or depth > 4:
        return out
    obj_id = id(obj)
    if obj_id in seen:
        return out
    seen.add(obj_id)
    if _looks_like_engine(obj):
        out.append((obj, prefix or type(obj).__name__))
    if isinstance(obj, Mapping):
        for name, value in _iter_mapping_values(obj):
            if name in set(_ENGINE_HOLDER_ATTRS + _APP_ATTRS + _STATE_ATTRS) or _looks_like_engine(value):
                out.extend(_collect_engine_candidates_from_object(value, f"{prefix}.{name}" if prefix else name, seen, depth + 1))
        return out
    for attr in _ENGINE_HOLDER_ATTRS + _APP_ATTRS + _STATE_ATTRS:
        val = _safe_getattr(obj, attr, None)
        if val is not None:
            out.extend(_collect_engine_candidates_from_object(val, f"{prefix}.{attr}" if prefix else attr, seen, depth + 1))
    for name, value in _iter_object_values(obj):
        if value is None:
            continue
        if name in set(_ENGINE_HOLDER_ATTRS + _APP_ATTRS + _STATE_ATTRS) or _looks_like_engine(value):
            out.extend(_collect_engine_candidates_from_object(value, f"{prefix}.{name}" if prefix else name, seen, depth + 1))
    return out


async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        result = fn(*args, **kwargs)
        return await result
    result = await asyncio.to_thread(fn, *args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


async def _call_with_timeout(fn: Callable[..., Any], *args: Any, timeout: float, **kwargs: Any) -> Any:
    return await asyncio.wait_for(_call_maybe_async(fn, *args, **kwargs), timeout=timeout)


async def _safe_call_zero_arg(fn: Callable[..., Any]) -> Any:
    try:
        return await _call_with_timeout(fn, timeout=min(ENGINE_CALL_TIMEOUT_SEC, 6.0))
    except TypeError:
        return None
    except Exception:
        return None


async def _scan_module_for_engine(module: Any, module_name: str) -> List[Tuple[Any, str]]:
    out: List[Tuple[Any, str]] = []
    if module is None:
        return out
    if _looks_like_engine(module):
        out.append((module, module_name))
    for fn_name in ("get_engine", "resolve_engine", "load_engine", "build_engine", "create_engine"):
        fn = _safe_getattr(module, fn_name, None)
        if callable(fn):
            result = await _safe_call_zero_arg(fn)
            if _looks_like_engine(result):
                out.append((result, f"{module_name}.{fn_name}"))
    for attr in _ENGINE_HOLDER_ATTRS + _APP_ATTRS + _STATE_ATTRS + ("ENGINE", "engine", "_ENGINE"):
        value = _safe_getattr(module, attr, None)
        if value is not None:
            out.extend(_collect_engine_candidates_from_object(value, f"{module_name}.{attr}"))
    return out


async def _resolve_engine_from_modules() -> Tuple[Optional[Any], str]:
    global _ENGINE_CACHE, _ENGINE_CACHE_SOURCE
    if _looks_like_engine(_ENGINE_CACHE):
        return _ENGINE_CACHE, _ENGINE_CACHE_SOURCE or "engine_cache"
    async with _ENGINE_LOCK:
        if _looks_like_engine(_ENGINE_CACHE):
            return _ENGINE_CACHE, _ENGINE_CACHE_SOURCE or "engine_cache"
        candidates: List[Tuple[Any, str]] = []
        module_candidates = (
            "main", "app", "core.data_engine_v2", "core.data_engine",
            "routes.analysis_sheet_rows", "routes.investment_advisor",
            "routes.advanced_analysis", "routes.enriched_quote", "routes.advisor",
        )
        for module_name in module_candidates:
            try:
                mod = importlib.import_module(module_name)
            except Exception:
                mod = None
            if mod is not None:
                candidates.extend(await _scan_module_for_engine(mod, module_name))
        loaded_names = sorted(
            name for name in sys.modules.keys()
            if isinstance(name, str) and (name == "main" or name == "app" or name.startswith("core.") or name.startswith("routes."))
        )
        seen_sources = {src for _, src in candidates}
        for module_name in loaded_names:
            mod = sys.modules.get(module_name)
            if mod is None:
                continue
            scanned = await _scan_module_for_engine(mod, module_name)
            for candidate, source in scanned:
                if source not in seen_sources:
                    seen_sources.add(source)
                    candidates.append((candidate, source))
        if candidates:
            _ENGINE_CACHE, _ENGINE_CACHE_SOURCE = candidates[0]
            return _ENGINE_CACHE, _ENGINE_CACHE_SOURCE
    return None, "engine_unavailable"


async def _resolve_engine(*args: Any, **kwargs: Any) -> Tuple[Optional[Any], str]:
    for key in ("engine", "data_engine", "quote_engine", "cache_engine", "service", "runner", "request", "req", "app", "context"):
        if kwargs.get(key) is not None:
            candidates = _collect_engine_candidates_from_object(kwargs.get(key), key)
            if candidates:
                return candidates[0]
    for i, arg in enumerate(args):
        candidates = _collect_engine_candidates_from_object(arg, f"arg{i}")
        if candidates:
            return candidates[0]
    for key in ("body", "payload", "request_data", "params", "criteria"):
        val = kwargs.get(key)
        if isinstance(val, Mapping):
            candidates = _collect_engine_candidates_from_object(val, key)
            if candidates:
                return candidates[0]
    for i, arg in enumerate(args):
        if isinstance(arg, Mapping):
            candidates = _collect_engine_candidates_from_object(arg, f"arg{i}")
            if candidates:
                return candidates[0]
    return await _resolve_engine_from_modules()


# =============================================================================
# Envelope / row extraction helpers
# =============================================================================
def _payload_keys_like(payload: Any, depth: int = 0) -> List[str]:
    if payload is None or depth > 6:
        return []
    mapping = _coerce_mapping(payload)
    if not mapping:
        return []
    for name in ("keys", "columns", "fields"):
        keys = mapping.get(name)
        if isinstance(keys, list):
            out = [_s(k) for k in keys if _s(k)]
            if out:
                return out
    for name in ("headers", "display_headers", "sheet_headers", "column_headers"):
        headers = mapping.get(name)
        if isinstance(headers, list):
            out = [_header_to_key(h) for h in headers if _header_to_key(h)]
            if out:
                return out
    for name in ("spec", "sheet_spec", "schema", "payload", "result", "response", "output", "data", "quote", "record", "item"):
        nested = mapping.get(name)
        if nested is not None and nested is not payload:
            out = _payload_keys_like(nested, depth + 1)
            if out:
                return out
    return []


def _rows_from_matrix(rows_matrix: Any, cols: Sequence[str]) -> List[Dict[str, Any]]:
    if not isinstance(rows_matrix, list) or not rows_matrix or not cols:
        return []
    keys = [_s(c) for c in cols if _s(c)]
    if not keys:
        return []
    out: List[Dict[str, Any]] = []
    for row in rows_matrix:
        if not isinstance(row, (list, tuple)):
            continue
        vals = list(row)
        out.append({keys[i]: (vals[i] if i < len(vals) else None) for i in range(len(keys))})
    return out


def _extract_rows_like(payload: Any, depth: int = 0) -> List[Dict[str, Any]]:
    if payload is None or depth > 8:
        return []

    if isinstance(payload, list):
        if not payload:
            return []
        if all(isinstance(x, Mapping) or _coerce_mapping(x) for x in payload):
            rows = [_coerce_mapping(x) for x in payload]
            rows = [r for r in rows if r]
            if rows:
                return rows
        if any(isinstance(x, (list, tuple)) for x in payload):
            return []
        return []

    mapping = _coerce_mapping(payload)
    if not mapping:
        return []

    if _looks_like_row_dict(mapping):
        return [mapping]

    # Symbol-keyed dict of row payloads. Only accept when values themselves look
    # like rows or can unwrap into a single row. Avoid treating diagnostics/meta
    # dicts as candidate rows.
    rows_from_symbol_map: List[Dict[str, Any]] = []
    maybe_symbol_map = True
    symbol_like_keys = 0
    for k, v in mapping.items():
        if not isinstance(v, Mapping):
            maybe_symbol_map = False
            break
        if not _looks_like_symbol_token(k):
            maybe_symbol_map = False
            break
        symbol_like_keys += 1
        nested_rows = _extract_rows_like(v, depth + 1)
        row = nested_rows[0] if len(nested_rows) == 1 else _coerce_mapping(v)
        if not row or not _looks_like_row_dict(row):
            maybe_symbol_map = False
            break
        if _is_blank(row.get("symbol")) and _is_blank(row.get("ticker")):
            row["symbol"] = _normalize_symbol(k)
            row["ticker"] = _normalize_symbol(k)
        rows_from_symbol_map.append(row)
    if maybe_symbol_map and symbol_like_keys > 0 and rows_from_symbol_map:
        return rows_from_symbol_map

    for key in ("row_objects", "rows", "records", "record", "items", "item", "results", "recommendations", "quotes", "quote", "data"):
        value = mapping.get(key)
        if isinstance(value, list):
            rows = _extract_rows_like(value, depth + 1)
            if rows:
                return rows
            if value and any(isinstance(x, (list, tuple)) for x in value):
                keys_like = _payload_keys_like(mapping)
                rows = _rows_from_matrix(value, keys_like)
                if rows:
                    return rows
        elif isinstance(value, Mapping):
            rows = _extract_rows_like(value, depth + 1)
            if rows:
                return rows

    for key in ("rows_matrix", "matrix"):
        value = mapping.get(key)
        if isinstance(value, list):
            keys_like = _payload_keys_like(mapping)
            rows = _rows_from_matrix(value, keys_like)
            if rows:
                return rows

    for key in ("payload", "result", "response", "output", "snapshot", "content", "envelope", "spec", "schema", "sheet_spec"):
        value = mapping.get(key)
        if value is not None and value is not payload:
            rows = _extract_rows_like(value, depth + 1)
            if rows:
                return rows

    return []


async def _call_engine_method(
    engine: Any,
    method_names: Sequence[str],
    attempts: Sequence[Tuple[Tuple[Any, ...], Dict[str, Any]]],
    *,
    timeout_seconds: float = ENGINE_CALL_TIMEOUT_SEC,
) -> Any:
    if engine is None:
        return None
    last_exc: Optional[Exception] = None
    for method_name in method_names:
        fn = getattr(engine, method_name, None)
        if not callable(fn):
            continue
        for args, kwargs in attempts:
            try:
                return await _call_with_timeout(fn, *args, timeout=timeout_seconds, **kwargs)
            except asyncio.TimeoutError as exc:
                last_exc = exc
                logger.debug("Engine method timed out: %s", method_name)
                continue
            except TypeError as exc:
                last_exc = exc
                if _is_signature_mismatch_typeerror(exc):
                    continue
                logger.debug("Engine method raised non-signature TypeError: %s", method_name, exc_info=True)
                raise
            except Exception as exc:
                last_exc = exc
                continue
    if last_exc is not None:
        logger.debug("Engine call attempts exhausted: %s", last_exc)
    return None


async def _fetch_page_rows(engine: Any, page: str, limit: int, mode: str) -> List[Dict[str, Any]]:
    body = {
        "page": page, "page_name": page, "sheet": page, "sheet_name": page,
        "tab": page, "worksheet": page, "name": page, "limit": limit, "top_n": limit,
        "mode": mode or "", "include_headers": True, "include_matrix": True,
        "schema_only": False, "headers_only": False,
    }
    attempts = [
        ((), {"page": page, "sheet": page, "sheet_name": page, "limit": limit, "mode": mode or "", "body": body}),
        ((), {"page_name": page, "sheet_name": page, "limit": limit, "mode": mode or "", "body": body}),
        ((), {"payload": body}),
        ((), {"body": body}),
        ((), {"page": page, "sheet": page, "sheet_name": page, "limit": limit, "mode": mode or ""}),
        ((), {"page_name": page, "sheet_name": page, "limit": limit, "mode": mode or ""}),
        ((), {"page": page, "sheet": page, "limit": limit, "mode": mode or ""}),
        ((), {"page": page, "limit": limit, "mode": mode or ""}),
        ((page,), {"limit": limit, "mode": mode or ""}),
        ((page,), {"limit": limit}),
        ((page,), {}),
    ]
    payload = await _call_engine_method(
        engine,
        (
            "get_sheet_rows", "get_page_rows", "sheet_rows", "build_sheet_rows",
            "execute_sheet_rows", "run_sheet_rows", "build_analysis_sheet_rows",
            "run_analysis_sheet_rows", "get_rows_for_sheet", "get_rows_for_page",
            "get_page_data", "get_sheet_data", "build_page_rows", "build_page_data",
        ),
        attempts,
    )
    return _extract_rows_like(payload)


async def _fetch_page_snapshot_rows(engine: Any, page: str) -> List[Dict[str, Any]]:
    attempts = [
        ((), {"sheet_name": page}),
        ((), {"sheet": page}),
        ((), {"page": page}),
        ((), {"page_name": page}),
        ((page,), {}),
    ]
    payload = await _call_engine_method(
        engine,
        ("get_cached_sheet_snapshot", "get_sheet_snapshot", "get_cached_sheet_rows", "get_page_snapshot", "get_sheet_cache"),
        attempts,
        timeout_seconds=min(ENGINE_CALL_TIMEOUT_SEC, 6.0),
    )
    return _extract_rows_like(payload)


async def _fetch_direct_symbol_rows(engine: Any, symbols: Sequence[str], mode: str) -> List[Dict[str, Any]]:
    syms = [_normalize_symbol(s) for s in symbols if _normalize_symbol(s)]
    if not syms:
        return []
    attempts = [
        ((), {"symbols": syms, "mode": mode or "", "schema": OUTPUT_PAGE}),
        ((), {"symbols": syms, "mode": mode or ""}),
        ((), {"symbols": syms}),
        ((syms,), {"mode": mode or "", "schema": OUTPUT_PAGE}),
        ((syms,), {"mode": mode or ""}),
        ((syms,), {}),
        ((), {"tickers": syms, "mode": mode or ""}),
        ((), {"tickers": syms}),
    ]
    payload = await _call_engine_method(
        engine,
        (
            "get_enriched_quotes_batch", "get_analysis_quotes_batch", "get_quotes_batch", "quotes_batch",
            "get_quotes", "get_enriched_quote_batch", "get_symbol_quotes", "get_live_quotes",
        ),
        attempts,
        timeout_seconds=min(ENGINE_CALL_TIMEOUT_SEC, 7.0),
    )
    rows = _extract_rows_like(payload)
    if rows:
        return rows

    if isinstance(payload, Mapping):
        direct_rows: List[Dict[str, Any]] = []
        for sym in syms:
            candidate = payload.get(sym) or payload.get(_normalize_symbol(sym))
            if isinstance(candidate, Mapping):
                direct_rows.append(_coerce_mapping(candidate))
        if direct_rows:
            return direct_rows

    out: List[Dict[str, Any]] = []
    for sym in syms[:HYDRATION_SYMBOL_CAP]:
        single_attempts = [
            ((sym,), {"mode": mode or "", "schema": OUTPUT_PAGE}),
            ((sym,), {"mode": mode or ""}),
            ((sym,), {}),
            ((), {"symbol": sym, "mode": mode or ""}),
            ((), {"symbol": sym}),
            ((), {"ticker": sym, "mode": mode or ""}),
        ]
        row_payload = await _call_engine_method(
            engine,
            ("get_enriched_quote", "get_quote", "get_quote_dict", "get_live_quote", "get_symbol_quote"),
            single_attempts,
            timeout_seconds=min(ENGINE_CALL_TIMEOUT_SEC, 5.0),
        )
        single_rows = _extract_rows_like(row_payload)
        if single_rows:
            out.extend(single_rows)
        elif isinstance(row_payload, Mapping):
            d = _coerce_mapping(row_payload)
            if d:
                out.append(d)
        else:
            d = _coerce_mapping(row_payload)
            if d:
                out.append(d)
    return out


# =============================================================================
# Criteria normalization
# =============================================================================
def _merge_mapping_like(*parts: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for part in parts:
        if isinstance(part, Mapping):
            out.update(dict(part))
    return out


def _collect_symbol_keys_from_mapping(mapping: Mapping[str, Any]) -> List[str]:
    out: List[str] = []
    for k, v in mapping.items():
        if isinstance(v, Mapping) and _looks_like_symbol_token(k):
            out.append(_normalize_symbol(k))
    return _dedupe_keep_order(out)


def _collect_criteria_from_inputs(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    positional_maps = [arg for arg in args if isinstance(arg, Mapping)]
    body = _merge_mapping_like(
        *positional_maps,
        kwargs.get("body"),
        kwargs.get("payload"),
        kwargs.get("request_data"),
        kwargs.get("params"),
    )

    criteria: Dict[str, Any] = {}
    if isinstance(kwargs.get("criteria"), Mapping):
        criteria.update(dict(kwargs["criteria"]))
    if isinstance(body.get("criteria"), Mapping):
        criteria.update(dict(body["criteria"]))
    if isinstance(body.get("filters"), Mapping):
        criteria.update(dict(body["filters"]))

    for k, v in body.items():
        if v is not None and k not in {"criteria", "filters"}:
            criteria.setdefault(k, v)

    for k in (
        "pages_selected", "pages", "selected_pages", "sources", "page", "page_name", "sheet", "sheet_name",
        "symbols", "tickers", "direct_symbols", "top_n", "limit", "risk_level", "risk_profile",
        "confidence_bucket", "confidence_level", "invest_period_days", "investment_period_days",
        "horizon_days", "invest_period_label", "min_expected_roi", "min_roi", "min_confidence",
        "min_ai_confidence", "max_risk_score", "min_volume", "enrich_final", "schema_only",
        "headers_only", "include_headers", "include_matrix", "mode", "emergency_symbols",
        # v4.12.0 Phase D: criteria_model v3.1.0 fields
        "min_conviction_score", "min_conviction",
        "exclude_engine_dropped_valuation",
        "exclude_forecast_unavailable",
        "exclude_provider_errors",
    ):
        if kwargs.get(k) is not None:
            criteria[k] = kwargs.get(k)

    pages = (
        _normalize_list(criteria.get("pages_selected"))
        or _normalize_list(criteria.get("pages"))
        or _normalize_list(criteria.get("selected_pages"))
        or _normalize_list(criteria.get("sources"))
        or _normalize_list(criteria.get("page"))
        or _normalize_list(criteria.get("page_name"))
        or _normalize_list(criteria.get("sheet"))
        or _normalize_list(criteria.get("sheet_name"))
    )
    pages = _safe_source_pages([_normalize_page_name_safe(p) for p in pages])
    if not pages:
        pages = _safe_source_pages(DEFAULT_SOURCE_PAGES)

    direct_symbols = _normalize_list(criteria.get("direct_symbols") or criteria.get("symbols") or criteria.get("tickers"))
    if not direct_symbols and isinstance(body, Mapping):
        direct_symbols = _collect_symbol_keys_from_mapping(body)
    direct_symbols = [_normalize_symbol(s) for s in direct_symbols if _normalize_symbol(s)]

    emergency_symbols = _normalize_list(criteria.get("emergency_symbols")) or list(EMERGENCY_SYMBOLS)
    emergency_symbols = [_normalize_symbol(s) for s in emergency_symbols if _normalize_symbol(s)]

    limit = _safe_int(criteria.get("limit") or criteria.get("top_n") or kwargs.get("limit"), 10)
    limit = max(1, min(limit, MAX_LIMIT))

    horizon_days = _safe_int(
        criteria.get("horizon_days") or criteria.get("invest_period_days") or criteria.get("investment_period_days"),
        90,
    )

    risk_level = _s(criteria.get("risk_level") or criteria.get("risk_profile") or "").lower()
    confidence_bucket = _s(criteria.get("confidence_bucket") or criteria.get("confidence_level") or "").lower()

    min_roi = criteria.get("min_expected_roi")
    if min_roi is None:
        min_roi = criteria.get("min_roi")
    min_roi_ratio = _safe_ratio(min_roi, None)

    normalized = dict(criteria)
    normalized["pages_selected"] = pages
    normalized["direct_symbols"] = direct_symbols
    normalized["direct_symbol_order"] = list(direct_symbols)
    normalized["emergency_symbols"] = emergency_symbols
    normalized["limit"] = limit
    normalized["top_n"] = limit
    normalized["risk_level"] = risk_level
    normalized["risk_profile"] = risk_level
    normalized["confidence_bucket"] = confidence_bucket
    normalized["confidence_level"] = confidence_bucket
    normalized["horizon_days"] = horizon_days
    normalized["invest_period_days"] = horizon_days
    normalized["min_expected_roi"] = min_roi_ratio
    normalized["min_roi"] = min_roi_ratio
    normalized["schema_only"] = _coerce_bool(normalized.get("schema_only"), False)
    normalized["headers_only"] = _coerce_bool(normalized.get("headers_only"), False)
    normalized["include_headers"] = _coerce_bool(normalized.get("include_headers", True), True)
    normalized["include_matrix"] = _coerce_bool(normalized.get("include_matrix", True), True)
    normalized.setdefault("enrich_final", True)

    # v4.12.0 Phase D: criteria_model v3.1.0 — normalize the 4 new fields.
    # `min_conviction` is the v2.7.0 spelling; v2.8.0 canonical is
    # `min_conviction_score`. We honour both and write the canonical
    # spelling forward (mirrors criteria_model v3.1.0's KV alias map).
    mcs_raw = normalized.get("min_conviction_score")
    if mcs_raw is None:
        mcs_raw = normalized.get("min_conviction")
    mcs_val = _safe_float(mcs_raw, None)
    if mcs_val is not None:
        # Accept 0..1 fractions ("0.60") as 0..100 by convention (60).
        if 0.0 < mcs_val <= 1.0:
            mcs_val = mcs_val * 100.0
        mcs_val = max(0.0, min(100.0, mcs_val))
    normalized["min_conviction_score"] = mcs_val if mcs_val is not None else 0.0
    # Preserve the legacy key for downstream callers that still read it.
    normalized["min_conviction"] = normalized["min_conviction_score"]

    normalized["exclude_engine_dropped_valuation"] = _coerce_bool(
        normalized.get("exclude_engine_dropped_valuation"), False
    )
    normalized["exclude_forecast_unavailable"] = _coerce_bool(
        normalized.get("exclude_forecast_unavailable"), False
    )
    normalized["exclude_provider_errors"] = _coerce_bool(
        normalized.get("exclude_provider_errors"), False
    )

    return normalized


# =============================================================================
# Row normalization / ranking
# =============================================================================
def _extract_value_by_aliases(row: Mapping[str, Any], key: str) -> Any:
    aliases = ROW_KEY_ALIASES.get(key, (key,))
    lookup = _row_lookup(row)
    for alias in aliases:
        for token in _canonical_key_variants(alias):
            if token in lookup:
                return lookup[token]
    return None


def _normalize_candidate_row(raw: Mapping[str, Any]) -> Dict[str, Any]:
    row: Dict[str, Any] = dict(raw)
    for key in ROW_KEY_ALIASES.keys():
        if key not in row or row.get(key) is None or row.get(key) == "":
            value = _extract_value_by_aliases(row, key)
            if value is not None:
                row[key] = value
    sym = _normalize_symbol(row.get("symbol") or row.get("ticker") or row.get("requested_symbol") or row.get("code"))
    if sym:
        row["symbol"] = sym
        row.setdefault("ticker", sym)
    return row


def _row_richness(row: Mapping[str, Any]) -> int:
    return _count_nonblank_fields(row)


def _choose_horizon_roi(row: Mapping[str, Any], horizon_days: int) -> Optional[float]:
    if horizon_days <= 31:
        return _safe_ratio(row.get("expected_roi_1m"), None) or _safe_ratio(row.get("expected_roi_3m"), None) or _safe_ratio(row.get("expected_roi_12m"), None)
    if horizon_days <= 92:
        return _safe_ratio(row.get("expected_roi_3m"), None) or _safe_ratio(row.get("expected_roi_12m"), None) or _safe_ratio(row.get("expected_roi_1m"), None)
    return _safe_ratio(row.get("expected_roi_12m"), None) or _safe_ratio(row.get("expected_roi_3m"), None) or _safe_ratio(row.get("expected_roi_1m"), None)


def _confidence_bucket_match(row: Mapping[str, Any], wanted: str) -> bool:
    if not wanted:
        return True
    row_bucket = _s(row.get("confidence_bucket") or row.get("confidence_level")).lower()
    if row_bucket:
        return wanted in row_bucket or row_bucket in wanted
    score = _safe_float(row.get("forecast_confidence") or row.get("confidence_score"), None)
    if score is None:
        return True
    if score <= 1.0:
        score *= 100.0
    if "high" in wanted:
        return score >= 70
    if "moderate" in wanted or "medium" in wanted:
        return 45 <= score < 70
    if "low" in wanted:
        return score < 45
    return True


def _risk_level_match(row: Mapping[str, Any], wanted: str) -> bool:
    if not wanted:
        return True
    row_bucket = _s(row.get("risk_bucket") or row.get("risk_level")).lower()
    if row_bucket:
        return wanted in row_bucket or row_bucket in wanted
    risk = _safe_float(row.get("risk_score"), None)
    if risk is None:
        return True
    if "low" in wanted or "conservative" in wanted:
        return risk <= 35
    if "moderate" in wanted or "medium" in wanted:
        return 20 <= risk <= 65
    if "high" in wanted or "aggressive" in wanted:
        return risk >= 45
    return True


def _passes_filters_with_reason(
    row: Mapping[str, Any],
    criteria: Mapping[str, Any],
) -> Tuple[bool, str]:
    """Filter gate with drop-reason instrumentation.

    Returns (passed, reason). reason is empty string when passed=True.
    Used by `_build_top10_rows_async` to populate the `applied_v310_filters`
    block in the response meta with per-filter drop counts.
    """
    wanted_conf = _s(criteria.get("confidence_bucket") or criteria.get("confidence_level")).lower()
    wanted_risk = _s(criteria.get("risk_level") or criteria.get("risk_profile")).lower()
    horizon_days = _safe_int(criteria.get("horizon_days") or criteria.get("invest_period_days"), 90)

    if not _confidence_bucket_match(row, wanted_conf):
        return False, "confidence_bucket"
    if not _risk_level_match(row, wanted_risk):
        return False, "risk_level"

    min_roi = _safe_ratio(criteria.get("min_expected_roi") or criteria.get("min_roi"), None)
    roi = _choose_horizon_roi(row, horizon_days)
    if min_roi is not None:
        if roi is None or roi < min_roi:
            return False, "min_expected_roi"

    min_conf = _safe_float(criteria.get("min_confidence") or criteria.get("min_ai_confidence"), None)
    row_conf = _safe_float(row.get("forecast_confidence") or row.get("confidence_score"), None)
    if row_conf is not None and row_conf <= 1.0:
        row_conf *= 100.0
    if min_conf is not None:
        if min_conf <= 1.0:
            min_conf *= 100.0
        if row_conf is None or row_conf < min_conf:
            return False, "min_ai_confidence"

    max_risk = _safe_float(criteria.get("max_risk_score"), None)
    risk = _safe_float(row.get("risk_score"), None)
    if max_risk is not None and risk is not None and risk > max_risk:
        return False, "max_risk_score"

    min_volume = _safe_float(criteria.get("min_volume"), None)
    volume = _safe_float(row.get("volume"), None)
    if min_volume is not None and volume is not None and volume < min_volume:
        return False, "min_volume"

    # -------------------------------------------------------------------------
    # v4.12.0 Phase D — criteria_model v3.1.0 HARD-FILTER CONSUMERS
    # -------------------------------------------------------------------------
    # Honoured only when explicitly set by the operator. The bool exclusions
    # default to false so v2.7.0-era criteria continue producing v4.11.0
    # behaviour (soft penalties only, no hard exclusion).

    # 1. min_conviction_score (float, 0..100) — drops rows below conviction
    #    floor. Aligns with reco_normalize v7.2.0 floors
    #    (RECO_STRONG_BUY_CONVICTION_FLOOR=60, RECO_BUY_CONVICTION_FLOOR=45).
    min_conv = _safe_float(
        criteria.get("min_conviction_score") or criteria.get("min_conviction"),
        None,
    )
    if min_conv is not None and min_conv > 0.0:
        # Accept fractions (0.6) as percentages (60).
        if 0.0 < min_conv <= 1.0:
            min_conv = min_conv * 100.0
        row_conv = _safe_float(row.get("conviction_score"), None)
        if row_conv is None or row_conv < min_conv:
            return False, "min_conviction_score"

    # 2. exclude_engine_dropped_valuation — drops rows tagged with any of
    #    the 5 engine-dropped-valuation tags from engine v5.60.0+ Phase H/I/P.
    if _coerce_bool(criteria.get("exclude_engine_dropped_valuation"), False):
        tags = _parse_warnings_tags(row)
        if _has_engine_drop_tag(tags):
            return False, "exclude_engine_dropped_valuation"

    # 3. exclude_forecast_unavailable — drops rows with any of the 4
    #    forecast-unavailable tags from engine v5.60.0+ Phase B OR the
    #    forecast_unavailable bool flag.
    if _coerce_bool(criteria.get("exclude_forecast_unavailable"), False):
        tags = _parse_warnings_tags(row)
        if _has_forecast_unavail_tag(tags, row):
            return False, "exclude_forecast_unavailable"

    # 4. exclude_provider_errors — drops rows where last_error_class is
    #    non-empty (preserved by engine v5.60.0+ Phase Q from the
    #    eodhd/yahoo_*/argaam/finnhub/tadawul providers).
    if _coerce_bool(criteria.get("exclude_provider_errors"), False):
        if _has_provider_error(row):
            return False, "exclude_provider_errors"

    return True, ""


def _passes_filters(row: Mapping[str, Any], criteria: Mapping[str, Any]) -> bool:
    """Thin wrapper preserving the v4.8.0 (passed: bool) signature.

    The reason-returning variant is used directly in
    `_build_top10_rows_async` for drop-count instrumentation.
    """
    passed, _reason = _passes_filters_with_reason(row, criteria)
    return passed


def _direct_symbol_order_index(row: Mapping[str, Any], criteria: Mapping[str, Any]) -> Optional[int]:
    direct_symbols = [_normalize_symbol(s) for s in _normalize_list(criteria.get("direct_symbol_order") or criteria.get("direct_symbols"))]
    if not direct_symbols:
        return None
    sym = _normalize_symbol(row.get("symbol") or row.get("ticker") or row.get("requested_symbol"))
    if not sym:
        return None
    try:
        return direct_symbols.index(sym)
    except ValueError:
        return None


def _selector_score(row: Mapping[str, Any], criteria: Mapping[str, Any]) -> float:
    overall = _safe_float(row.get("overall_score"), None)
    opportunity = _safe_float(row.get("opportunity_score"), None)
    value = _safe_float(row.get("value_score"), None)
    quality = _safe_float(row.get("quality_score"), None)
    momentum = _safe_float(row.get("momentum_score"), None)
    growth = _safe_float(row.get("growth_score"), None)
    risk = _safe_float(row.get("risk_score"), None)
    conf = _safe_float(row.get("forecast_confidence") or row.get("confidence_score"), None)
    liquidity = _safe_float(row.get("liquidity_score"), None)
    horizon_days = _safe_int(criteria.get("horizon_days") or criteria.get("invest_period_days"), 90)
    roi = _choose_horizon_roi(row, horizon_days)

    if conf is not None and conf <= 1.0:
        conf *= 100.0

    score = 0.0
    if overall is not None:
        score += overall * 0.35
    if opportunity is not None:
        score += opportunity * 0.20
    if value is not None:
        score += value * 0.08
    if quality is not None:
        score += quality * 0.08
    if momentum is not None:
        score += momentum * 0.08
    if growth is not None:
        score += growth * 0.08
    if conf is not None:
        score += conf * 0.08
    if liquidity is not None:
        score += liquidity * 0.05
    if risk is not None:
        score += (100.0 - risk) * 0.08
    if roi is not None:
        score += roi * 100.0 * 0.20

    # -------------------------------------------------------------------------
    # v4.10.0 — small conviction-score bump (Insights row field from
    # core.scoring v5.2.5). High-conviction rows get a modest edge.
    # -------------------------------------------------------------------------
    conviction = _safe_float(row.get("conviction_score"), None)
    if conviction is not None:
        score += conviction * 0.05

    # -------------------------------------------------------------------------
    # v4.12.0 — small candlestick-signal bump on BULLISH patterns. The
    # weight stays small because the engine's main scores already reflect
    # technicals; this is a tiebreaker, not a primary signal.
    # -------------------------------------------------------------------------
    candle_signal = _s(row.get("candlestick_signal")).upper()
    candle_conf = _safe_float(row.get("candlestick_confidence"), None)
    if candle_signal == "BULLISH" and candle_conf is not None:
        score += min(candle_conf, 100.0) * 0.02
    elif candle_signal == "BEARISH" and candle_conf is not None:
        score -= min(candle_conf, 100.0) * 0.02

    # -------------------------------------------------------------------------
    # v4.11.0 — Data-quality soft penalties (preserved verbatim in v4.12.0).
    # These REDUCE the composite score without dropping the row. v4.12.0
    # layers the v3.1.0 HARD filters on top via _passes_filters.
    # -------------------------------------------------------------------------
    tags = _parse_warnings_tags(row)
    if _has_engine_drop_tag(tags):
        score -= TOP10_PENALTY_ENGINE_DROP
    if _has_forecast_unavail_tag(tags, row):
        score -= TOP10_PENALTY_FORECAST_UNAVAIL
    if _has_provider_error(row):
        score -= TOP10_PENALTY_PROVIDER_ERROR

    direct_order_index = _direct_symbol_order_index(row, criteria)
    if direct_order_index is not None:
        score += max(0.0, 140.0 - float(direct_order_index * 2))

    score += min(_row_richness(row), 120) * 0.03
    return float(score)


def _canonical_selection_reason(row: Dict[str, Any], criteria: Mapping[str, Any]) -> str:
    recommendation = _s(row.get("recommendation"))
    confidence_bucket = _s(row.get("confidence_bucket"))
    risk_bucket = _s(row.get("risk_bucket"))
    source_page = _s(row.get("source_page"))
    horizon_days = _safe_int(criteria.get("horizon_days") or criteria.get("invest_period_days"), 90)
    horizon_roi = _choose_horizon_roi(row, horizon_days)

    # v4.10.0 — surface leading top_factor / top_risk from Insights row fields.
    top_factors = _s(row.get("top_factors"))
    top_risks = _s(row.get("top_risks"))
    conviction = _safe_float(row.get("conviction_score"), None)

    score_parts: List[str] = []
    for label, key in (
        ("overall", "overall_score"),
        ("opportunity", "opportunity_score"),
        ("value", "value_score"),
        ("quality", "quality_score"),
        ("momentum", "momentum_score"),
        ("growth", "growth_score"),
    ):
        val = row.get(key)
        if isinstance(val, (int, float)):
            score_parts.append(f"{label}={round(float(val), 2)}")

    parts: List[str] = []
    if recommendation:
        parts.append(f"Recommendation={recommendation}")
    if conviction is not None:
        parts.append(f"Conv={round(conviction, 0):.0f}/100")
    if confidence_bucket:
        parts.append(f"Confidence={confidence_bucket}")
    if risk_bucket:
        parts.append(f"Risk={risk_bucket}")
    if horizon_roi is not None:
        parts.append(f"Horizon ROI={round(horizon_roi * 100.0, 2)}%")
    if source_page:
        parts.append(f"Source={source_page}")
    if top_factors:
        # Show only the leading factor to keep reason readable.
        first_factor = top_factors.split("|")[0].strip()
        if first_factor:
            parts.append(f"Top factor: {first_factor}")
    if top_risks:
        first_risk = top_risks.split("|")[0].strip()
        if first_risk:
            parts.append(f"Top risk: {first_risk}")
    if score_parts:
        parts.append(", ".join(score_parts[:3]))

    # v4.11.0/v4.12.0 — append data-quality caveats so the operator can see
    # at a glance which rows the engine flagged. Soft-penalty rows kept
    # in the selection still surface their warnings here.
    tags = _parse_warnings_tags(row)
    caveats: List[str] = []
    if _has_provider_error(row):
        err = _s(row.get("last_error_class") or row.get("provider_last_error_class") or row.get("error"))
        caveats.append(f"Provider Error: {err}" if err else "Provider Error")
    if _has_engine_drop_tag(tags):
        # Pick the first matching tag for human readability.
        first_tag = next((t for t in tags if any(m in t for m in _ENGINE_DROP_TAGS)), "")
        caveats.append(f"Engine: {first_tag}" if first_tag else "Engine valuation dropped")
    if _has_forecast_unavail_tag(tags, row):
        caveats.append("Forecast: unavailable")
    if caveats:
        parts.append("[" + "; ".join(caveats) + "]")

    return " | ".join(parts) if parts else "Selected by Top10 composite scoring."


def _rank_and_project_rows(rows: Sequence[Mapping[str, Any]], keys: Sequence[str], criteria: Mapping[str, Any]) -> List[Dict[str, Any]]:
    criteria_snapshot = _json_compact(criteria)
    out: List[Dict[str, Any]] = []
    for idx, raw in enumerate(rows, start=1):
        row = dict(raw)
        if _is_blank(row.get("top10_rank")):
            row["top10_rank"] = idx
        if _is_blank(row.get("rank_overall")):
            row["rank_overall"] = idx
        if _is_blank(row.get("selection_reason")):
            row["selection_reason"] = _canonical_selection_reason(row, criteria)
        if _is_blank(row.get("criteria_snapshot")):
            row["criteria_snapshot"] = criteria_snapshot
        projected = {k: _json_safe(row.get(k)) for k in keys}
        out.append(projected)
    return out


# =============================================================================
# Candidate collection
# =============================================================================
def _page_priority_symbol_limit(criteria: Mapping[str, Any]) -> int:
    limit = _safe_int(criteria.get("limit"), 10)
    return max(min(limit * 3, HYDRATION_SYMBOL_CAP), min(HYDRATION_SYMBOL_CAP, 12))


def _merge_symbol_row_lists(primary: Sequence[Mapping[str, Any]], secondary: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}

    def _put(raw: Mapping[str, Any]) -> None:
        row = _normalize_candidate_row(raw)
        sym = _normalize_symbol(row.get("symbol") or row.get("ticker"))
        if not sym:
            return
        existing = merged.get(sym)
        if existing is None:
            merged[sym] = dict(row)
            return
        richer, poorer = (row, existing) if _row_richness(row) >= _row_richness(existing) else (existing, row)
        merged[sym] = _merge_row_prefer_richer(poorer, richer)

    for row in primary:
        _put(row)
    for row in secondary:
        _put(row)
    return list(merged.values())


def _merge_row_prefer_richer(base: Mapping[str, Any], update: Mapping[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in dict(update).items():
        if value not in (None, "", [], {}, ()):
            merged[key] = value
    return merged


async def _collect_page_rows_with_fallback(engine: Any, page: str, mode: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    meta: Dict[str, Any] = {
        "page": page,
        "rows": 0,
        "snapshot_rows": 0,
        "used_snapshot": False,
        "merged_snapshot": False,
        "timed_out": False,
        "error": "",
    }
    try:
        rows = await asyncio.wait_for(_fetch_page_rows(engine, page, SOURCE_PAGE_LIMIT, mode), timeout=PAGE_TOTAL_TIMEOUT_SEC)
        meta["rows"] = len(rows)
    except asyncio.TimeoutError:
        rows = []
        meta["timed_out"] = True
        meta["error"] = "page_rows_timeout"
    except Exception as exc:
        rows = []
        meta["error"] = f"page_rows_failed:{type(exc).__name__}"

    should_try_snapshot_merge = bool(rows) and (len(rows) < 5 or max((_row_richness(r) for r in rows), default=0) < 10)
    if rows and not should_try_snapshot_merge:
        return rows, meta

    try:
        snap_rows = await asyncio.wait_for(_fetch_page_snapshot_rows(engine, page), timeout=min(PAGE_TOTAL_TIMEOUT_SEC, 8.0))
        meta["snapshot_rows"] = len(snap_rows)
        meta["used_snapshot"] = bool(snap_rows)
        if rows and snap_rows:
            merged_rows = _merge_symbol_row_lists(rows, snap_rows)
            meta["merged_snapshot"] = True
            return merged_rows, meta
        if snap_rows:
            return snap_rows, meta
        return rows, meta
    except asyncio.TimeoutError:
        if not meta["error"]:
            meta["error"] = "snapshot_timeout"
        return rows if rows else [], meta
    except Exception as exc:
        if not meta["error"]:
            meta["error"] = f"snapshot_failed:{type(exc).__name__}"
        return rows if rows else [], meta


async def _hydrate_page_rows(engine: Any, rows: List[Dict[str, Any]], mode: str, criteria: Mapping[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    meta: Dict[str, Any] = {
        "requested_symbols": 0,
        "enriched_rows": 0,
        "hydration_used": False,
        "hydration_error": "",
    }
    if not rows:
        return rows, meta

    ranked_candidates: List[Tuple[float, Dict[str, Any]]] = []
    for row in rows:
        normalized = _normalize_candidate_row(row)
        sym = _normalize_symbol(normalized.get("symbol") or normalized.get("ticker"))
        if not sym:
            continue
        ranked_candidates.append((_selector_score(normalized, criteria), normalized))

    ranked_candidates.sort(key=lambda x: (x[0], _row_richness(x[1])), reverse=True)
    symbols = _dedupe_keep_order(
        _normalize_symbol(item[1].get("symbol") or item[1].get("ticker")) for item in ranked_candidates
    )[: _page_priority_symbol_limit(criteria)]

    meta["requested_symbols"] = len(symbols)
    if not symbols:
        return rows, meta

    try:
        enriched_rows = await asyncio.wait_for(_fetch_direct_symbol_rows(engine, symbols, mode), timeout=min(PAGE_TOTAL_TIMEOUT_SEC, 10.0))
        meta["enriched_rows"] = len(enriched_rows)
        meta["hydration_used"] = bool(enriched_rows)
    except asyncio.TimeoutError:
        meta["hydration_error"] = "hydration_timeout"
        return rows, meta
    except Exception as exc:
        meta["hydration_error"] = f"hydration_failed:{type(exc).__name__}"
        return rows, meta

    if not enriched_rows:
        return rows, meta

    row_map: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        normalized = _normalize_candidate_row(row)
        sym = _normalize_symbol(normalized.get("symbol") or normalized.get("ticker"))
        if sym:
            row_map[sym] = normalized

    for er in enriched_rows:
        normalized = _normalize_candidate_row(er)
        sym = _normalize_symbol(normalized.get("symbol") or normalized.get("ticker"))
        if not sym:
            continue
        if sym in row_map:
            row_map[sym] = _merge_row_prefer_richer(row_map[sym], normalized)
        else:
            row_map[sym] = normalized

    return list(row_map.values()), meta


async def _collect_candidate_rows(engine: Any, criteria: Mapping[str, Any], mode: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    direct_symbols = [_normalize_symbol(s) for s in _normalize_list(criteria.get("direct_symbols")) if _normalize_symbol(s)]
    emergency_symbols = _normalize_list(criteria.get("emergency_symbols"))
    pages = _safe_source_pages(_normalize_list(criteria.get("pages_selected")))
    direct_symbol_index_map = {sym: idx for idx, sym in enumerate(direct_symbols)}

    meta: Dict[str, Any] = {
        "engine_source": _ENGINE_CACHE_SOURCE or "",
        "source_pages": pages,
        "direct_symbols_count": len(direct_symbols),
        "direct_symbols_requested": list(direct_symbols),
        "source_page_rows": {},
        "snapshot_rows": {},
        "direct_symbol_rows": 0,
        "page_diagnostics": [],
        "hydration_diagnostics": [],
        "partial_success": False,
        "used_emergency_symbols": False,
    }

    candidates: Dict[str, Dict[str, Any]] = {}

    def _put_row(raw: Mapping[str, Any], source_page: str = "") -> None:
        row = _normalize_candidate_row(raw)
        sym = _normalize_symbol(row.get("symbol") or row.get("ticker"))
        if not sym:
            return
        if source_page and _is_blank(row.get("source_page")):
            row["source_page"] = source_page
        if sym in direct_symbol_index_map:
            row["_direct_symbol_priority"] = True
            row["_direct_symbol_index"] = direct_symbol_index_map[sym]
            if _is_blank(row.get("source_page")):
                row["source_page"] = "Direct"
        existing = candidates.get(sym)
        if existing is None:
            candidates[sym] = row
            return
        richer, poorer = (row, existing) if _row_richness(row) >= _row_richness(existing) else (existing, row)
        candidates[sym] = _merge_row_prefer_richer(poorer, richer)

    if direct_symbols:
        try:
            direct_rows = await asyncio.wait_for(_fetch_direct_symbol_rows(engine, direct_symbols[:HYDRATION_SYMBOL_CAP], mode), timeout=min(BUILDER_TOTAL_TIMEOUT_SEC / 2.0, 12.0))
            meta["direct_symbol_rows"] = len(direct_rows)
            meta["partial_success"] = meta["partial_success"] or bool(direct_rows)
            for row in direct_rows:
                _put_row(row, "")
        except asyncio.TimeoutError:
            meta["direct_symbol_error"] = "direct_symbol_timeout"
        except Exception as exc:
            meta["direct_symbol_error"] = f"direct_symbol_failed:{type(exc).__name__}"

    early_stop_target = max(10, _safe_int(criteria.get("limit"), 10) * EARLY_STOP_MULTIPLIER)

    for page in pages:
        page_rows, page_meta = await _collect_page_rows_with_fallback(engine, page, mode)
        meta["source_page_rows"][page] = page_meta.get("rows", 0)
        meta["snapshot_rows"][page] = page_meta.get("snapshot_rows", 0)

        hydrated_rows = page_rows
        hydration_meta: Dict[str, Any] = {"page": page}
        if page_rows:
            hydrated_rows, hydration_meta = await _hydrate_page_rows(engine, page_rows, mode, criteria)
            hydration_meta["page"] = page

        meta["page_diagnostics"].append(_json_safe(page_meta))
        meta["hydration_diagnostics"].append(_json_safe(hydration_meta))
        if hydrated_rows:
            meta["partial_success"] = True

        for row in hydrated_rows:
            _put_row(row, page)

        if len(candidates) >= early_stop_target and page != "My_Portfolio":
            meta["early_stop"] = True
            meta["early_stop_after_page"] = page
            break

    if not candidates and emergency_symbols:
        try:
            emergency_rows = await asyncio.wait_for(
                _fetch_direct_symbol_rows(engine, emergency_symbols[:HYDRATION_SYMBOL_CAP], mode),
                timeout=min(BUILDER_TOTAL_TIMEOUT_SEC / 2.0, 12.0),
            )
            meta["used_emergency_symbols"] = bool(emergency_rows)
            meta["emergency_symbol_rows"] = len(emergency_rows)
            for row in emergency_rows:
                _put_row(row, "Emergency")
        except Exception as exc:
            meta["emergency_symbol_error"] = f"emergency_symbol_failed:{type(exc).__name__}"

    fallback_target = max(1, _safe_int(criteria.get("limit"), 10))
    if len(candidates) < fallback_target:
        try:
            fallback_rows = await asyncio.wait_for(
                _fetch_page_rows(engine, OUTPUT_PAGE, max(30, fallback_target * 2), mode),
                timeout=min(PAGE_TOTAL_TIMEOUT_SEC, 10.0),
            )
            meta["top10_output_fallback_rows"] = len(fallback_rows)
        except asyncio.TimeoutError:
            fallback_rows = []
            meta["top10_output_fallback_error"] = "top10_output_timeout"
        except Exception as exc:
            fallback_rows = []
            meta["top10_output_fallback_error"] = f"top10_output_failed:{type(exc).__name__}"

        if not fallback_rows:
            try:
                fallback_rows = await asyncio.wait_for(_fetch_page_snapshot_rows(engine, OUTPUT_PAGE), timeout=min(PAGE_TOTAL_TIMEOUT_SEC, 8.0))
                meta["top10_output_snapshot_rows"] = len(fallback_rows)
            except asyncio.TimeoutError:
                fallback_rows = []
                meta["top10_output_snapshot_error"] = "top10_output_snapshot_timeout"
            except Exception as exc:
                fallback_rows = []
                meta["top10_output_snapshot_error"] = f"top10_output_snapshot_failed:{type(exc).__name__}"

        if fallback_rows:
            meta["top10_output_fallback_used"] = True
        for row in fallback_rows:
            _put_row(row, OUTPUT_PAGE)

    meta["deduped_candidate_count"] = len(candidates)
    return list(candidates.values()), meta


# =============================================================================
# Payload builder
# =============================================================================
def _build_payload(*, status: str, headers: List[str], keys: List[str], rows: List[Dict[str, Any]], meta: Dict[str, Any]) -> Dict[str, Any]:
    include_headers = _coerce_bool(meta.get("include_headers", True), True)
    include_matrix = _coerce_bool(meta.get("include_matrix", True), True)
    payload = {
        "status": status,
        "page": OUTPUT_PAGE,
        "sheet": OUTPUT_PAGE,
        "sheet_name": OUTPUT_PAGE,
        "route_family": "top10",
        "headers": headers if include_headers else [],
        "display_headers": headers if include_headers else [],
        "sheet_headers": headers if include_headers else [],
        "column_headers": headers if include_headers else [],
        "keys": keys,
        "columns": keys,
        "fields": keys,
        "rows": rows,
        "data": rows,
        "items": rows,
        "quotes": rows,
        "records": rows,
        "row_objects": rows,
        "rows_matrix": _rows_to_matrix(rows, keys) if include_matrix else [],
        "count": len(rows),
        "version": TOP10_SELECTOR_VERSION,
        "meta": meta,
    }
    return _json_safe(payload)


# =============================================================================
# Core async implementation
# =============================================================================
async def _build_top10_rows_async(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    async def _inner() -> Dict[str, Any]:
        started = time.perf_counter()
        headers, keys = _load_schema_defaults()
        criteria = _collect_criteria_from_inputs(*args, **kwargs)
        mode = _s(kwargs.get("mode") or criteria.get("mode") or "")
        limit = max(1, _safe_int(criteria.get("limit") or kwargs.get("limit"), 10))

        if criteria.get("schema_only") or criteria.get("headers_only"):
            return _build_payload(
                status="success",
                headers=headers,
                keys=keys,
                rows=[],
                meta={
                    "build_status": "OK",
                    "dispatch": "top10_selector",
                    "selector_version": TOP10_SELECTOR_VERSION,
                    "schema_only": bool(criteria.get("schema_only")),
                    "headers_only": bool(criteria.get("headers_only")),
                    "criteria_used": _json_safe(criteria),
                    "include_headers": criteria.get("include_headers", True),
                    "include_matrix": criteria.get("include_matrix", True),
                    "duration_ms": round((time.perf_counter() - started) * 1000.0, 3),
                },
            )

        engine, engine_source = await _resolve_engine(*args, **kwargs)
        if engine is None:
            return _build_payload(
                status="warn",
                headers=headers,
                keys=keys,
                rows=[],
                meta={
                    "build_status": "DEGRADED",
                    "dispatch": "top10_selector",
                    "selector_version": TOP10_SELECTOR_VERSION,
                    "warning": "engine_unavailable",
                    "criteria_used": _json_safe(criteria),
                    "include_headers": criteria.get("include_headers", True),
                    "include_matrix": criteria.get("include_matrix", True),
                    "engine_source": engine_source,
                    "duration_ms": round((time.perf_counter() - started) * 1000.0, 3),
                },
            )

        try:
            candidates, collect_meta = await _collect_candidate_rows(engine, criteria, mode)
        except Exception as exc:
            logger.warning("Top10 candidate collection failed: %s", exc, exc_info=True)
            return _build_payload(
                status="warn",
                headers=headers,
                keys=keys,
                rows=[],
                meta={
                    "build_status": "DEGRADED",
                    "dispatch": "top10_selector",
                    "selector_version": TOP10_SELECTOR_VERSION,
                    "warning": f"candidate_collection_failed:{type(exc).__name__}",
                    "criteria_used": _json_safe(criteria),
                    "include_headers": criteria.get("include_headers", True),
                    "include_matrix": criteria.get("include_matrix", True),
                    "engine_source": engine_source,
                    "duration_ms": round((time.perf_counter() - started) * 1000.0, 3),
                },
            )

        normalized_candidates = [_normalize_candidate_row(r) for r in candidates]

        # ---------------------------------------------------------------------
        # v4.12.0 Phase E — filter application with per-filter drop tracking
        # ---------------------------------------------------------------------
        filtered: List[Dict[str, Any]] = []
        drop_counts: Dict[str, int] = {}
        for r in normalized_candidates:
            passed, reason = _passes_filters_with_reason(r, criteria)
            if passed:
                filtered.append(r)
            elif reason:
                drop_counts[reason] = drop_counts.get(reason, 0) + 1

        filter_relaxed = False
        selected_pool = filtered
        if not selected_pool and normalized_candidates:
            selected_pool = list(normalized_candidates)
            filter_relaxed = True

        # Which v3.1.0 fields were actually set (used by Phase E meta block)
        _v310_active: Dict[str, Any] = {}
        mcs = _safe_float(
            criteria.get("min_conviction_score") or criteria.get("min_conviction"), None
        )
        if mcs is not None and mcs > 0.0:
            _v310_active["min_conviction_score"] = mcs
        if _coerce_bool(criteria.get("exclude_engine_dropped_valuation"), False):
            _v310_active["exclude_engine_dropped_valuation"] = True
        if _coerce_bool(criteria.get("exclude_forecast_unavailable"), False):
            _v310_active["exclude_forecast_unavailable"] = True
        if _coerce_bool(criteria.get("exclude_provider_errors"), False):
            _v310_active["exclude_provider_errors"] = True

        horizon_days = _safe_int(criteria.get("horizon_days") or criteria.get("invest_period_days"), 90)
        scored: List[Tuple[float, Dict[str, Any]]] = []
        for row in selected_pool:
            scored.append((_selector_score(row, criteria), dict(row)))

        scored.sort(
            key=lambda x: (
                x[0],
                _choose_horizon_roi(x[1], horizon_days) or 0.0,
                _safe_float(x[1].get("opportunity_score"), 0.0) or 0.0,
                _safe_float(x[1].get("overall_score"), 0.0) or 0.0,
                _safe_float(x[1].get("forecast_confidence"), 0.0) or 0.0,
                -(_safe_float(x[1].get("risk_score"), 999.0) or 999.0),
                _safe_float(x[1].get("liquidity_score"), 0.0) or 0.0,
                _row_richness(x[1]),
            ),
            reverse=True,
        )

        top_rows = [row for _, row in scored[:limit]]

        if len(top_rows) < limit:
            seen_symbols = {_normalize_symbol(r.get("symbol") or r.get("ticker")) for r in top_rows if _normalize_symbol(r.get("symbol") or r.get("ticker"))}
            direct_rows = [row for _, row in scored if _direct_symbol_order_index(row, criteria) is not None]
            direct_rows.sort(key=lambda row: _direct_symbol_order_index(row, criteria) if _direct_symbol_order_index(row, criteria) is not None else 999999)
            for row in direct_rows:
                sym = _normalize_symbol(row.get("symbol") or row.get("ticker"))
                if not sym or sym in seen_symbols:
                    continue
                top_rows.append(row)
                seen_symbols.add(sym)
                if len(top_rows) >= limit:
                    break

        projected_rows = _rank_and_project_rows(top_rows[:limit], keys, criteria)

        # ---------------------------------------------------------------------
        # v4.12.0 Phase E — data-quality summary aggregated from projected rows
        # (parallels insights_builder v7.0.0 Data Quality Alerts section).
        # ---------------------------------------------------------------------
        dq_engine_drop = 0
        dq_forecast_unavail = 0
        dq_provider_err = 0
        for _proj_row in projected_rows:
            _tags = _parse_warnings_tags(_proj_row)
            if _has_engine_drop_tag(_tags):
                dq_engine_drop += 1
            if _has_forecast_unavail_tag(_tags, _proj_row):
                dq_forecast_unavail += 1
            if _has_provider_error(_proj_row):
                dq_provider_err += 1

        status = "success" if projected_rows else "warn"
        meta = {
            "build_status": "OK" if projected_rows else "WARN",
            "dispatch": "top10_selector",
            "selector_version": TOP10_SELECTOR_VERSION,
            "criteria_used": _json_safe(criteria),
            "candidate_count": len(normalized_candidates),
            "filtered_count": len(filtered),
            "selected_count": len(projected_rows),
            "filter_relaxed": filter_relaxed,
            "selected_symbols": [_s(r.get("symbol")) for r in projected_rows if _s(r.get("symbol"))],
            "selected_direct_symbols": [
                _s(r.get("symbol"))
                for r in projected_rows
                if _direct_symbol_order_index(r, criteria) is not None and _s(r.get("symbol"))
            ],
            "include_headers": criteria.get("include_headers", True),
            "include_matrix": criteria.get("include_matrix", True),
            "engine_source": engine_source,
            "duration_ms": round((time.perf_counter() - started) * 1000.0, 3),
            # v4.12.0 Phase E: hard-filter audit surface
            "filter_drop_counts": dict(drop_counts),
            "applied_v310_filters": dict(_v310_active),
            # v4.12.0 Phase E: data-quality summary of the FINAL projected set
            "data_quality_summary": {
                "engine_dropped_valuation": dq_engine_drop,
                "forecast_unavailable": dq_forecast_unavail,
                "provider_errors": dq_provider_err,
                "total_projected": len(projected_rows),
            },
            **collect_meta,
        }
        if not projected_rows:
            meta["warning"] = "no_top10_rows_after_filtering"

        return _build_payload(status=status, headers=headers, keys=keys, rows=projected_rows, meta=meta)

    try:
        return await asyncio.wait_for(_inner(), timeout=BUILDER_TOTAL_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        headers, keys = _load_schema_defaults()
        criteria = _collect_criteria_from_inputs(*args, **kwargs)
        return _build_payload(
            status="warn",
            headers=headers,
            keys=keys,
            rows=[],
            meta={
                "build_status": "DEGRADED",
                "dispatch": "top10_selector",
                "selector_version": TOP10_SELECTOR_VERSION,
                "warning": "builder_total_timeout",
                "criteria_used": _json_safe(criteria),
                "include_headers": criteria.get("include_headers", True),
                "include_matrix": criteria.get("include_matrix", True),
            },
        )


# =============================================================================
# Public API (sync+async tolerant)
# =============================================================================
def build_top10_rows(*args: Any, **kwargs: Any) -> Any:
    coro = _build_top10_rows_async(*args, **kwargs)
    try:
        asyncio.get_running_loop()
        return coro
    except RuntimeError:
        return asyncio.run(coro)


def build_top10_output_rows(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def build_top10_investments_rows(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def build_top_10_investments_rows(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def build_top10(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def build_top10_investments(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def build_top10_output(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def build_top10_payload(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def get_top10_rows(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def select_top10(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def select_top10_symbols(*args: Any, **kwargs: Any) -> Any:
    async def _inner() -> List[str]:
        payload = await _build_top10_rows_async(*args, **kwargs)
        rows = payload.get("rows") if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            return []
        out: List[str] = []
        for row in rows:
            if isinstance(row, dict):
                sym = _normalize_symbol(row.get("symbol") or row.get("ticker"))
                if sym:
                    out.append(sym)
        return _dedupe_keep_order(out)

    try:
        asyncio.get_running_loop()
        return _inner()
    except RuntimeError:
        return asyncio.run(_inner())


__all__ = [
    "TOP10_SELECTOR_VERSION",
    "__version__",  # v4.12.0 Phase F: TFB module-version convention alias
    "DEFAULT_FALLBACK_KEYS",
    "DEFAULT_FALLBACK_HEADERS",
    "ROW_KEY_ALIASES",
    "TOP10_REQUIRED_FIELDS",
    "TOP10_REQUIRED_HEADERS",
    # v4.11.0 soft-penalty knobs (env-tunable)
    "TOP10_PENALTY_ENGINE_DROP",
    "TOP10_PENALTY_FORECAST_UNAVAIL",
    "TOP10_PENALTY_PROVIDER_ERROR",
    "build_top10_rows",
    "build_top10_output_rows",
    "build_top10_investments_rows",
    "build_top_10_investments_rows",
    "build_top10",
    "build_top10_investments",
    "build_top10_output",
    "build_top10_payload",
    "get_top10_rows",
    "select_top10",
    "select_top10_symbols",
]
