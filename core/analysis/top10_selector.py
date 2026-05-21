#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/analysis/top10_selector.py
================================================================================
Top 10 Selector — v4.14.0
================================================================================
LIVE • SCHEMA-FIRST • ROUTE-COMPATIBLE • ENGINE-SELF-RESOLVING • JSON-SAFE
TOP10-METADATA GUARANTEED • SOURCE-PAGE SAFE • SNAPSHOT FALLBACK SAFE
SYNC+ASYNC CALLER TOLERANT • DISPLAY-HEADER TOLERANT • WRAPPER-PAYLOAD SAFE
PARTIAL-DEGRADATION SAFE • DIRECT-SYMBOLS SAFE • TIMEOUT-GUARDED
INSIGHTS-COLUMNS AWARE • DATA-QUALITY-AWARE RANKING
CRITERIA-v3.1.0 HARD-FILTER CONSUMER (v4.12.0)
DECISION-MATRIX AWARE • CANDLESTICK AWARE (v4.12.0)
CANONICAL-BUCKET ROUTED • REAL OVERALL-RANK FALLBACK (v4.13.0)
8-TIER VOCABULARY AWARE • PRIORITY-BAND ROUTED • CASCADE-BRIDGE READY (v4.14.0)

================================================================================
Cross-stack family (May 2026 v8.0.0 family floor)
================================================================================
This module sits at the tail of the May-2026 cross-stack family. The
contract surfaces it consumes from upstream are:

  - core.sheets.schema_registry        v2.11.0
  - core.scoring                       v5.7.0
      * Produces the 5 Insights row fields AND the canonical 8-tier
        recommendation vocabulary (STRONG_BUY / BUY / ACCUMULATE / HOLD /
        REDUCE / SELL / STRONG_SELL / AVOID).
  - core.scoring_engine (bridge)       v3.6.0
  - core.reco_normalize                v8.0.0
      * 8-tier canonical normalization. ACCUMULATE (moderate-bullish /
        scale-in), STRONG_SELL (re-canonical), and AVOID (uninvestable)
        are first-class tokens — not collapsed to BUY/SELL.
      * Conviction-floor downgrade env knobs:
        RECO_STRONG_BUY_CONVICTION_FLOOR=60,
        RECO_BUY_CONVICTION_FLOOR=45.
  - core.schemas                       v7.0.0
      * Recommendation enum is 8-tier; UnifiedQuote carries the 9
        cascade-bridge / scoring-provenance fields (provider_rating,
        recommendation_priority_band P1..P5, scoring_recommendation_source,
        scoring_schema_version, scoring_errors, opportunity_source,
        overall_score_raw, overall_penalty_factor,
        recommendation_source).
  - core.data_engine_v2                v5.76.0
      * Phase B forecast-unavailable tags + bool flag.
      * Phase H/I/P engine-dropped-valuation tags.
      * Phase N canonicalizes warnings as '; '-joined.
      * Phase Q preserves provider last_error_class.
      * 8-tier passthrough preserved end-to-end.
      * 9 cascade-bridge / scoring-provenance fields emitted per row.
  - core.candlesticks                  v1.0.0
  - core.buckets                       v1.0.0
  - core.analysis.criteria_model       v3.1.1
      * v3.1.0 fields preserved (min_conviction_score,
        exclude_engine_dropped_valuation, exclude_forecast_unavailable,
        exclude_provider_errors).
      * v3.2.0 fields (forward-compat in this module): NEW
        `exclude_avoid_recommendations` (bool) and `min_priority_band`
        (str, "P1".."P5"). Honoured as HARD filters here even before
        criteria_model lands v3.2.0; off by default so existing
        v3.1.0 callers see zero behaviour change.
  - core.analysis.investment_advisor_engine v4.5.0
      * 8-tier routed; 106-col fallback. Emits the same 9 cascade-bridge
        fields v4.14.0 reads.
  - core.analysis.insights_builder     v8.0.0
      * Sibling consumer; renders Top Picks Context + Risk Alerts rows
        for the same 8-tier vocabulary v4.14.0 routes.

================================================================================
What v4.14.0 adds (over v4.13.0)
================================================================================
v4.13.0 closed the canonical-bucket-routing gap and gave us a real
overall-score rank fallback. Two backlog items it left in place:

  - top10_selector was 5-tier-aware only. ACCUMULATE flowed through
    as a string on the `recommendation` field but the selector did not
    recognise it as a Top10-eligible bullish tier. AVOID flowed
    through too — the genuinely "uninvestable" tier from
    reco_normalize v8.0.0 — and the selector did NOT auto-exclude it,
    so a row with recommendation=AVOID could in principle reach the
    Top10 if its other scores were high enough. STRONG_SELL was
    silently treated as just SELL with no separate handling.
  - `recommendation_priority_band` (P1..P5) is emitted by
    data_engine_v2 v5.74.0+ on every row and by advisor_engine v4.5.0
    as a first-class urgency band. v4.13.0 ignored it entirely, so
    two rows with near-identical composite scores were tie-broken by
    horizon ROI / opportunity_score / overall_score / risk — never by
    the urgency that scoring.py / reco_normalize.py had assigned.

v4.14.0 phase changes:

  A. CROSS-STACK HEADER SYNC. All upstream version refs updated to the
     v8.0.0 family floor (see "Cross-stack family" block above). Banner
     gains the v4.14.0 marker.

  B. VERSION BUMP 4.13.0 -> 4.14.0. `__version__` alias updated;
     `__all__` unchanged except for two new capability constants
     (`RECO_8TIER_AWARE_VERSION` + `_RECO_TIEBREAK_BUMPS`) — surface
     additions only.

  C. 8-TIER VOCABULARY CONSTANTS. New module-level frozensets:
       _RECO_8TIER_CANONICAL   — all 8 canonical tokens
       _BULLISH_RECOS          — {STRONG_BUY, BUY, ACCUMULATE}
       _BEARISH_RECOS          — {SELL, REDUCE, STRONG_SELL, AVOID}
       _RECO_8TIER_NEW_TOKENS  — {ACCUMULATE, STRONG_SELL, AVOID}
                                 (the three tokens that did NOT exist
                                  before reco_normalize v8.0.0)
     Plus `_RECO_TIEBREAK_BUMPS` — a small ±2.5 range of selector_score
     bumps per recommendation tier (AVOID = -8.0 to keep AVOID rows
     out of the Top10 unless directly requested via direct_symbols).
     These are tiebreakers, NOT primary scores — the engine's
     `overall_score` already incorporates the recommendation. The
     bumps mostly matter when two rows have near-identical scores.

  D. ROW_KEY_ALIASES EXTENDED with three new fields:
       recommendation_priority_band  (P1..P5 urgency band from
                                       data_engine_v2 v5.74.0+)
       provider_rating               (raw provider rating, separate
                                       from engine's recommendation)
       scoring_recommendation_source (which scoring path emitted the
                                       recommendation)
     Each carries the camelCase + snake_case variants advisor_engine
     v4.5.0 and insights_builder v8.0.0 already use, so payloads from
     anywhere in the stack normalize cleanly.

  E. PRIORITY-BAND HELPERS. New private helpers
     `_normalize_priority_band(val)` -> "P1".."P5" or "" and
     `_priority_band_rank(band)` -> int rank (P1=1 best, P5=5 worst,
     ""=99 = unranked). Used by the sort key tiebreaker and by
     `_passes_filters_with_reason` for the min_priority_band filter.

  F. _collect_criteria_from_inputs EXTENDED with three v4.14.0 fields
     (forward-compat for criteria_model v3.2.0):
       exclude_avoid_recommendations  (bool, default false)
       min_priority_band              (str, "P1".."P5"; default "")
       reco_8tier_strict              (bool, default false; when true
                                        rows with recommendation not in
                                        the canonical 8 tiers are dropped
                                        rather than treated as HOLD)
     All three default to off so v3.1.0 callers see zero behaviour
     change.

  G. _passes_filters_with_reason NEW HARD FILTERS:
       exclude_avoid_recommendations  -> drops rows where the row's
         recommendation normalizes to AVOID. Also drops rows where the
         recommendation field is non-empty and the case-folded value
         matches AVOID even without normalization.
       min_priority_band              -> drops rows whose priority band
         is worse than the floor (e.g., min="P3" drops P4 / P5 / blank
         when reco_8tier_strict=True; blank rows are kept when strict
         is off). Lower number = better.
       reco_8tier_strict              -> drops rows where the
         recommendation is non-empty AND does not normalize to one of
         the canonical 8 tiers (catches legacy/typo'd values).

  H. _selector_score TIEBREAK BUMPS. Adds `_reco_tiebreak_bump()` to
     the composite. AVOID gets -8.0 (effectively pushes AVOID rows out
     of any normal Top10), STRONG_SELL -2.5, SELL -1.5, REDUCE -1.0,
     HOLD 0.0, ACCUMULATE +1.0, BUY +1.5, STRONG_BUY +2.0. Bump
     magnitudes are small relative to the existing overall_score
     weight (0.35) so they only matter when scores are close.

  I. SORT-KEY PRIORITY BAND TIEBREAKER. The composite sort key now
     interleaves a `-_priority_band_rank()` term as the SECOND
     tiebreaker (right after selector_score). With reverse=True this
     means P1 > P2 > P3 > P4 > P5 > unranked, breaking selector-score
     ties by urgency rather than by horizon ROI.

  J. _canonical_selection_reason DISPLAY EXTENSION. When present, the
     reason now shows:
       - Priority Band (e.g., "Priority=P2")
       - Provider Rating divergence (e.g., "Provider=BUY,
         Engine=ACCUMULATE" — only when the two diverge)
       - 8-tier new-token markers (e.g., a "[NEW v8: ACCUMULATE]"
         badge on the moderate-bullish tier so operators can see when
         a row's recommendation is one of the three tokens that did
         NOT exist before reco_normalize v8.0.0).

  K. RESULT META v8-TIER SURFACE. The `applied_v310_filters` block is
     preserved verbatim. NEW parallel `applied_v8tier_filters` block
     records which v4.14.0 filters fired and per-filter drop counts.
     `data_quality_summary` extended with reco counts (avoid_count,
     accumulate_count, strong_sell_count, reco_8tier_seen,
     priority_band_seen). A new `reco_8tier_aware: true` capability
     flag tells downstream consumers (insights_builder v8.0.0, the
     advisor route layer) that this selector aligns with the v8.0.0
     vocabulary.

================================================================================
Defaults & back-compat
================================================================================
v4.14.0 is strictly additive at the runtime contract level:
  - All new criteria fields default to OFF; existing v3.1.0 callers
    see zero behaviour change.
  - The reco tiebreak bumps DO take effect by default, but they are
    small (±2.5 normal range, -8.0 for AVOID). The AVOID penalty is
    deliberately large so AVOID rows naturally drop out of the Top10
    without operators having to opt in to `exclude_avoid_recommendations`.
  - ACCUMULATE is recognised as a Top10-eligible bullish tier
    alongside BUY / STRONG_BUY.
  - All v4.13.0 public APIs preserved verbatim.

================================================================================
History (preserved)
================================================================================
[v4.13.0 / v4.12.0 / v4.11.0 / v4.10.0 / v4.9.0 / v4.8.0 changelog
history preserved verbatim in user source; trimmed in this on-disk
baseline for editing efficiency.]
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

TOP10_SELECTOR_VERSION = "4.14.0"
# v4.12.0 Phase F: TFB module-version convention alias (mirrors
# schema_registry v2.11.0, scoring v5.7.0, reco_normalize v8.0.0,
# insights_builder v8.0.0, scoring_engine v3.6.0, criteria_model v3.1.1,
# advisor_engine v4.5.0, data_engine_v2 v5.76.0, schemas v7.0.0).
__version__ = TOP10_SELECTOR_VERSION

# v4.14.0 Phase B/K: capability marker for downstream consumers. Surfaced
# in result meta as `reco_8tier_aware: true` and exported via __all__ so
# callers can content-check the selector's 8-tier alignment state.
RECO_8TIER_AWARE_VERSION = "v4.14.0"
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
    "symbol", "name", "asset_class", "exchange", "currency", "country", "sector", "industry",
    "current_price", "previous_close", "open_price", "day_high", "day_low",
    "week_52_high", "week_52_low", "price_change", "percent_change", "week_52_position_pct",
    "volume", "avg_volume_10d", "avg_volume_30d", "market_cap", "float_shares", "beta_5y",
    "pe_ttm", "pe_forward", "eps_ttm", "dividend_yield", "payout_ratio", "revenue_ttm",
    "revenue_growth_yoy", "gross_margin", "operating_margin", "profit_margin", "debt_to_equity",
    "free_cash_flow_ttm",
    "rsi_14", "volatility_30d", "volatility_90d", "max_drawdown_1y",
    "var_95_1d", "sharpe_1y", "risk_score", "risk_bucket",
    "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio",
    "intrinsic_value", "upside_pct", "valuation_score",
    "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
    "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "forecast_confidence", "confidence_score", "confidence_bucket",
    "value_score", "quality_score", "momentum_score", "growth_score",
    "overall_score", "opportunity_score", "rank_overall",
    "fundamental_view", "technical_view", "risk_view", "value_view",
    "recommendation", "recommendation_reason", "horizon_days", "invest_period_label",
    "position_qty", "avg_cost", "position_cost", "position_value",
    "unrealized_pl", "unrealized_pl_pct",
    "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",
    "sector_relative_score", "conviction_score", "top_factors", "top_risks", "position_size_hint",
    "recommendation_detailed", "recommendation_priority",
    "candlestick_pattern", "candlestick_signal", "candlestick_strength",
    "candlestick_confidence", "candlestick_patterns_recent",
    "top10_rank", "selection_reason", "criteria_snapshot",
]

DEFAULT_FALLBACK_HEADERS = [
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
    "Current Price", "Previous Close", "Open", "Day High", "Day Low", "52W High", "52W Low",
    "Price Change", "Percent Change", "52W Position %",
    "Volume", "Avg Volume 10D", "Avg Volume 30D", "Market Cap", "Float Shares", "Beta (5Y)",
    "P/E (TTM)", "P/E (Forward)", "EPS (TTM)", "Dividend Yield", "Payout Ratio", "Revenue (TTM)",
    "Revenue Growth YoY", "Gross Margin", "Operating Margin", "Profit Margin", "Debt/Equity",
    "Free Cash Flow (TTM)",
    "RSI (14)", "Volatility 30D", "Volatility 90D", "Max Drawdown 1Y", "VaR 95% (1D)",
    "Sharpe (1Y)", "Risk Score", "Risk Bucket",
    "P/B", "P/S", "EV/EBITDA", "PEG", "Intrinsic Value", "Upside %", "Valuation Score",
    "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M",
    "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M",
    "Forecast Confidence", "Confidence Score", "Confidence Bucket",
    "Value Score", "Quality Score", "Momentum Score", "Growth Score",
    "Overall Score", "Opportunity Score", "Rank (Overall)",
    "Fundamental View", "Technical View", "Risk View", "Value View",
    "Recommendation", "Recommendation Reason", "Horizon Days", "Invest Period Label",
    "Position Qty", "Avg Cost", "Position Cost", "Position Value", "Unrealized P/L",
    "Unrealized P/L %",
    "Data Provider", "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",
    "Sector-Adj Score", "Conviction Score", "Top Factors", "Top Risks", "Position Size Hint",
    "Recommendation Detail", "Reco Priority",
    "Candle Pattern", "Candle Signal", "Candle Strength",
    "Candle Confidence", "Recent Patterns (5D)",
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
    "fundamental_view": ("fundamental_view", "fundamentalView", "fundamentals_view"),
    "technical_view": ("technical_view", "technicalView", "technicals_view"),
    "risk_view": ("risk_view", "riskView"),
    "value_view": ("value_view", "valueView", "valuation_view"),
    "sector_relative_score": ("sector_relative_score", "sectorRelativeScore", "sector_adj_score", "sectorAdjScore"),
    "conviction_score": ("conviction_score", "convictionScore", "conviction"),
    "top_factors": ("top_factors", "topFactors", "key_factors", "keyFactors"),
    "top_risks": ("top_risks", "topRisks", "key_risks", "keyRisks"),
    "position_size_hint": ("position_size_hint", "positionSizeHint", "position_size", "size_hint"),
    "recommendation_detailed": (
        "recommendation_detailed", "recommendationDetailed", "recommendation_detail",
        "recommendationDetail", "decision_detailed", "decision_recommendation",
    ),
    "recommendation_priority": (
        "recommendation_priority", "recommendationPriority", "reco_priority",
        "recoPriority", "decision_priority",
    ),
    "candlestick_pattern": ("candlestick_pattern", "candlestickPattern", "candle_pattern", "candlePattern"),
    "candlestick_signal": ("candlestick_signal", "candlestickSignal", "candle_signal", "candleSignal"),
    "candlestick_strength": ("candlestick_strength", "candlestickStrength", "candle_strength", "candleStrength"),
    "candlestick_confidence": ("candlestick_confidence", "candlestickConfidence", "candle_confidence", "candleConfidence"),
    "candlestick_patterns_recent": (
        "candlestick_patterns_recent", "candlestickPatternsRecent", "candle_patterns_recent",
        "recent_candle_patterns", "recent_patterns",
    ),
    # Cascade-bridge / scoring-provenance (3) — v4.14.0; produced by
    # data_engine_v2 v5.74.0+ / advisor_engine v4.5.0 / insights_builder
    # v8.0.0 / schemas v7.0.0 UnifiedQuote.
    "recommendation_priority_band": (
        "recommendation_priority_band", "recommendationPriorityBand",
        "reco_priority_band", "recoPriorityBand", "priority_band", "priorityBand",
    ),
    "provider_rating": (
        "provider_rating", "providerRating", "vendor_rating", "vendorRating",
        "raw_provider_rating", "rawProviderRating",
    ),
    "scoring_recommendation_source": (
        "scoring_recommendation_source", "scoringRecommendationSource",
        "scoring_source", "scoringSource", "recommendation_source", "recommendationSource",
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

TOP10_PENALTY_ENGINE_DROP = _env_float("TOP10_PENALTY_ENGINE_DROP", 8.0)
TOP10_PENALTY_FORECAST_UNAVAIL = _env_float("TOP10_PENALTY_FORECAST_UNAVAIL", 10.0)
TOP10_PENALTY_PROVIDER_ERROR = _env_float("TOP10_PENALTY_PROVIDER_ERROR", 6.0)

_ENGINE_DROP_TAGS = (
    "intrinsic_unit_mismatch_suspected",
    "upside_synthesis_suspect",
    "engine_52w_high_unit_mismatch_dropped",
    "engine_52w_low_unit_mismatch_dropped",
    "engine_52w_high_low_inverted",
)

_FORECAST_UNAVAIL_TAGS = (
    "forecast_unavailable",
    "forecast_unavailable_no_source",
    "forecast_cleared_consistency_sweep",
    "forecast_skipped_unavailable",
)


# =============================================================================
# v4.14.0 — 8-tier recommendation vocabulary (Phase C)
# =============================================================================
# Canonical 8-tier vocabulary from reco_normalize v8.0.0 / scoring v5.7.0 /
# schemas v7.0.0. ACCUMULATE (moderate-bullish / scale-in), STRONG_SELL
# (re-canonical), and AVOID (genuinely uninvestable) are first-class tokens
# — NOT collapsed to BUY / SELL. This module routes them as such.
#
# The frozensets are used by:
#   - _selector_score        — tiebreak bump per tier (Phase H)
#   - _passes_filters_*      — exclude_avoid_recommendations / reco_8tier_strict
#                              (Phase G)
#   - _canonical_selection_reason — NEW-token markers + provider divergence
#                                    display (Phase J)
#   - _build_top10_rows_async — data_quality_summary reco counts (Phase K)

_RECO_8TIER_CANONICAL = frozenset({
    "STRONG_BUY", "BUY", "ACCUMULATE", "HOLD",
    "REDUCE", "SELL", "STRONG_SELL", "AVOID",
})

_BULLISH_RECOS = frozenset({"STRONG_BUY", "BUY", "ACCUMULATE"})
_BEARISH_RECOS = frozenset({"REDUCE", "SELL", "STRONG_SELL", "AVOID"})

# Three tokens that did NOT exist before reco_normalize v8.0.0. Used by
# `_canonical_selection_reason` to badge them for operators (e.g.
# "[NEW v8: ACCUMULATE]") and by the data-quality summary to count
# 8-tier-specific token occurrences in the projected output.
_RECO_8TIER_NEW_TOKENS = frozenset({"ACCUMULATE", "STRONG_SELL", "AVOID"})

# v4.14.0 selector-score tiebreak bumps per recommendation tier. These are
# TIEBREAKERS, not primary scores — the engine's `overall_score` (weighted
# at 0.35 in `_selector_score`) already incorporates the recommendation.
# A 10-point overall_score difference is 3.5 selector points; the bumps
# here mostly matter when two rows have near-identical composite scores.
#
# AVOID gets -8.0 (much larger than the other bumps) so AVOID rows
# naturally drop out of the Top10 without operators having to opt in to
# `exclude_avoid_recommendations`. STRONG_BUY / STRONG_SELL get the
# largest non-AVOID magnitudes so conviction-tier signals surface
# correctly under tied composites.
_RECO_TIEBREAK_BUMPS: Dict[str, float] = {
    "STRONG_BUY":   2.0,
    "BUY":          1.5,
    "ACCUMULATE":   1.0,
    "HOLD":         0.0,
    "REDUCE":      -1.0,
    "SELL":        -1.5,
    "STRONG_SELL": -2.5,
    "AVOID":       -8.0,
}

# Lightweight recommendation alias map used by `_normalize_reco_token`.
# Covers the most common upstream variants Claude sees on raw rows; the
# heavy lifting still lives in reco_normalize v8.0.0 — this is a defensive
# bridge for rows that have NOT been routed through it (e.g. legacy
# snapshot cache, Top_10_Investments fallback, third-party hydration).
_RECO_TOKEN_ALIASES: Dict[str, str] = {
    "STRONGBUY": "STRONG_BUY",
    "STRONG-BUY": "STRONG_BUY",
    "STRONG BUY": "STRONG_BUY",
    "CONVICTION BUY": "STRONG_BUY",
    "TOP PICK": "STRONG_BUY",
    "OVERWEIGHT": "BUY",
    "OUTPERFORM": "BUY",
    "POSITIVE": "BUY",
    "SCALE IN": "ACCUMULATE",
    "SCALE_IN": "ACCUMULATE",
    "SCALEIN": "ACCUMULATE",
    "ACC": "ACCUMULATE",
    "ADD": "ACCUMULATE",
    "MOD BUY": "ACCUMULATE",
    "MODERATE BUY": "ACCUMULATE",
    "NEUTRAL": "HOLD",
    "MAINTAIN": "HOLD",
    "MARKET PERFORM": "HOLD",
    "EQUAL WEIGHT": "HOLD",
    "PERFORM": "HOLD",
    "UNDERWEIGHT": "REDUCE",
    "UNDERPERFORM": "REDUCE",
    "TRIM": "REDUCE",
    "EXIT": "SELL",
    "NEGATIVE": "SELL",
    "STRONGSELL": "STRONG_SELL",
    "STRONG-SELL": "STRONG_SELL",
    "STRONG SELL": "STRONG_SELL",
    "STRONG REDUCE": "STRONG_SELL",
    "DEEP SELL": "STRONG_SELL",
    "CONVICTION SELL": "STRONG_SELL",
    "HARD PASS": "AVOID",
    "DO NOT BUY": "AVOID",
    "DNB": "AVOID",
    "UNINVESTABLE": "AVOID",
    "UNTRADEABLE": "AVOID",
}


try:
    from core.sheets.schema_registry import get_sheet_spec as _get_sheet_spec  # type: ignore
except Exception:
    _get_sheet_spec = None  # type: ignore

try:
    from core.sheets.page_catalog import normalize_page_name as _normalize_page_name  # type: ignore
except Exception:
    _normalize_page_name = None  # type: ignore


try:
    from core.buckets import (  # type: ignore
        risk_bucket_from_score as _bk_risk_bucket_from_score,
        confidence_bucket_from_score as _bk_confidence_bucket_from_score,
        normalize_risk_bucket as _bk_normalize_risk_bucket,
        normalize_confidence_bucket as _bk_normalize_confidence_bucket,
    )
    _BUCKETS_AVAILABLE = True
except Exception:  # pragma: no cover
    _bk_risk_bucket_from_score = None  # type: ignore
    _bk_confidence_bucket_from_score = None  # type: ignore
    _bk_normalize_risk_bucket = None  # type: ignore
    _bk_normalize_confidence_bucket = None  # type: ignore
    _BUCKETS_AVAILABLE = False


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


def _parse_warnings_tags(row: Mapping[str, Any]) -> List[str]:
    raw = row.get("warnings")
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        seq = [_s(x).lower() for x in raw]
        return [s for s in seq if s]
    s = _s(raw).lower()
    if not s:
        return []
    for sep in (";", "|", ","):
        if sep in s:
            return [t.strip() for t in s.split(sep) if t.strip()]
    return [s] if s else []


def _has_engine_drop_tag(tags: Sequence[str]) -> bool:
    if not tags:
        return False
    tag_set = set(tags)
    for marker in _ENGINE_DROP_TAGS:
        if marker in tag_set:
            return True
    joined = " ".join(tags)
    return any(marker in joined for marker in _ENGINE_DROP_TAGS)


def _has_forecast_unavail_tag(tags: Sequence[str], row: Mapping[str, Any]) -> bool:
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
    for key in ("last_error_class", "provider_last_error_class", "provider_error", "error_class"):
        val = row.get(key)
        if val is None:
            continue
        s = _s(val).lower()
        if s and s not in {"none", "null", "ok", "success", "false", "0", "no"}:
            return True
    err = _s(row.get("error"))
    return bool(err) and err.lower() not in {"none", "null", "false", "0"}


# =============================================================================
# v4.14.0 — 8-tier vocabulary helpers (Phase E)
# =============================================================================
# All five helpers are private (`_`-prefixed) and defensive: they tolerate
# blank input, return safe defaults, and never raise on unexpected types.
# They are used by:
#   - _selector_score             — `_reco_tiebreak_bump`
#   - _passes_filters_with_reason — `_normalize_reco_token`,
#                                    `_normalize_priority_band`,
#                                    `_priority_band_rank`,
#                                    `_is_avoid_recommendation`
#   - sort key in _build_top10_rows_async — `_normalize_priority_band`,
#                                            `_priority_band_rank`
#   - _canonical_selection_reason — `_normalize_reco_token`,
#                                    `_normalize_priority_band`

def _normalize_reco_token(val: Any) -> str:
    """Resolve a raw recommendation value to a canonical 8-tier token, or
    "" if the value cannot be matched. Defensive bridge for rows that have
    NOT been routed through reco_normalize v8.0.0 — checks the canonical
    set first, then a small alias map covering the most common upstream
    variants. Returns UPPERCASE on success."""
    s = _s(val).upper()
    if not s:
        return ""
    if s in _RECO_8TIER_CANONICAL:
        return s
    aliased = _RECO_TOKEN_ALIASES.get(s)
    if aliased and aliased in _RECO_8TIER_CANONICAL:
        return aliased
    # Defensive: strip non-alnum and retry against canonical (e.g.
    # "STRONG-BUY" -> "STRONGBUY" via the alias map).
    compact = re.sub(r"[^A-Z]+", "", s)
    if compact:
        aliased = _RECO_TOKEN_ALIASES.get(compact)
        if aliased and aliased in _RECO_8TIER_CANONICAL:
            return aliased
        if compact in _RECO_8TIER_CANONICAL:
            return compact
    return ""


def _normalize_priority_band(val: Any) -> str:
    """Resolve a raw priority_band value to canonical "P1".."P5", or "" if
    the value cannot be parsed. Accepts:
      - canonical strings "P1".."P5" (any casing, leading/trailing space)
      - bare integers / int-strings 1..5 (P1..P5 respectively)
      - "BAND_1".."BAND_5" or "BAND 1".."BAND 5" defensive variants
    Returns UPPERCASE on success."""
    s = _s(val).upper().replace(" ", "").replace("-", "").replace("_", "")
    if not s:
        return ""
    # Bare digits 1..5
    if s.isdigit():
        try:
            n = int(s)
        except Exception:
            return ""
        return f"P{n}" if 1 <= n <= 5 else ""
    # "P1".."P5"
    if len(s) == 2 and s[0] == "P" and s[1].isdigit():
        try:
            n = int(s[1])
        except Exception:
            return ""
        return f"P{n}" if 1 <= n <= 5 else ""
    # "BAND1".."BAND5"
    if s.startswith("BAND") and len(s) == 5 and s[4].isdigit():
        try:
            n = int(s[4])
        except Exception:
            return ""
        return f"P{n}" if 1 <= n <= 5 else ""
    return ""


def _priority_band_rank(band: Any) -> int:
    """Numeric rank for a priority band. P1=1 (best / most urgent), P5=5
    (worst), blank or unparseable = 99 (sorts after all real bands).
    Used by the sort key (negated, so P1 sorts first under reverse=True)
    and by the min_priority_band hard filter."""
    canon = _normalize_priority_band(band)
    if not canon or len(canon) != 2 or canon[0] != "P":
        return 99
    try:
        return int(canon[1])
    except Exception:
        return 99


def _reco_tiebreak_bump(row: Mapping[str, Any]) -> float:
    """Selector-score tiebreak bump per 8-tier recommendation tier. See
    `_RECO_TIEBREAK_BUMPS` for the canonical magnitudes. Returns 0.0 when
    the row's recommendation cannot be normalized to a canonical tier
    (treated as HOLD)."""
    token = _normalize_reco_token(row.get("recommendation"))
    if not token:
        return 0.0
    return _RECO_TIEBREAK_BUMPS.get(token, 0.0)


def _is_avoid_recommendation(row: Mapping[str, Any]) -> bool:
    """True iff the row's recommendation normalizes to the canonical
    AVOID token. Used by the `exclude_avoid_recommendations` hard filter."""
    return _normalize_reco_token(row.get("recommendation")) == "AVOID"


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
        "min_conviction_score", "min_conviction",
        "exclude_engine_dropped_valuation",
        "exclude_forecast_unavailable",
        "exclude_provider_errors",
        # v4.14.0 Phase F: criteria_model v3.2.0 forward-compat fields.
        # All three default to off so v3.1.0 callers see zero change.
        "exclude_avoid_recommendations",
        "min_priority_band",
        "reco_8tier_strict",
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

    mcs_raw = normalized.get("min_conviction_score")
    if mcs_raw is None:
        mcs_raw = normalized.get("min_conviction")
    mcs_val = _safe_float(mcs_raw, None)
    if mcs_val is not None:
        if 0.0 < mcs_val <= 1.0:
            mcs_val = mcs_val * 100.0
        mcs_val = max(0.0, min(100.0, mcs_val))
    normalized["min_conviction_score"] = mcs_val if mcs_val is not None else 0.0
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

    # v4.14.0 Phase F: criteria_model v3.2.0 forward-compat fields.
    # Normalized here so downstream filter / sort code can rely on
    # consistent shapes regardless of how the caller spelled them.
    normalized["exclude_avoid_recommendations"] = _coerce_bool(
        normalized.get("exclude_avoid_recommendations"), False
    )
    # min_priority_band -> canonical "P1".."P5" or "" (the floor; rows
    # whose band ranks WORSE than this are dropped — see Phase G).
    mpb_canon = _normalize_priority_band(normalized.get("min_priority_band"))
    normalized["min_priority_band"] = mpb_canon
    normalized["reco_8tier_strict"] = _coerce_bool(
        normalized.get("reco_8tier_strict"), False
    )

    return normalized


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


def _resolve_risk_bucket_canon(row: Mapping[str, Any]) -> str:
    if not (
        _BUCKETS_AVAILABLE
        and _bk_normalize_risk_bucket is not None
        and _bk_risk_bucket_from_score is not None
    ):
        return ""
    label = _s(row.get("risk_bucket") or row.get("risk_level"))
    if label:
        try:
            canon = _bk_normalize_risk_bucket(label)
        except Exception:
            canon = ""
        if canon:
            return canon
    risk = _safe_float(row.get("risk_score"), None)
    if risk is not None:
        try:
            return _bk_risk_bucket_from_score(risk) or ""
        except Exception:
            return ""
    return ""


def _resolve_confidence_bucket_canon(row: Mapping[str, Any]) -> str:
    if not (
        _BUCKETS_AVAILABLE
        and _bk_normalize_confidence_bucket is not None
        and _bk_confidence_bucket_from_score is not None
    ):
        return ""
    label = _s(row.get("confidence_bucket") or row.get("confidence_level"))
    if label:
        try:
            canon = _bk_normalize_confidence_bucket(label)
        except Exception:
            canon = ""
        if canon:
            return canon
    score = _safe_float(row.get("forecast_confidence") or row.get("confidence_score"), None)
    if score is not None:
        try:
            return _bk_confidence_bucket_from_score(score) or ""
        except Exception:
            return ""
    return ""


def _confidence_bucket_match(row: Mapping[str, Any], wanted: str) -> bool:
    if not wanted:
        return True

    wanted_canon = ""
    if _BUCKETS_AVAILABLE and _bk_normalize_confidence_bucket is not None:
        try:
            wanted_canon = _bk_normalize_confidence_bucket(wanted)
        except Exception:
            wanted_canon = ""
    if wanted_canon:
        row_canon = _resolve_confidence_bucket_canon(row)
        if row_canon:
            return row_canon == wanted_canon
        if (
            _safe_float(row.get("forecast_confidence") or row.get("confidence_score"), None) is None
            and not _s(row.get("confidence_bucket") or row.get("confidence_level"))
        ):
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

    wanted_canon = ""
    if _BUCKETS_AVAILABLE and _bk_normalize_risk_bucket is not None:
        try:
            wanted_canon = _bk_normalize_risk_bucket(wanted)
        except Exception:
            wanted_canon = ""
    if wanted_canon:
        row_canon = _resolve_risk_bucket_canon(row)
        if row_canon:
            return row_canon == wanted_canon
        if (
            _safe_float(row.get("risk_score"), None) is None
            and not _s(row.get("risk_bucket") or row.get("risk_level"))
        ):
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

    min_conv = _safe_float(
        criteria.get("min_conviction_score") or criteria.get("min_conviction"),
        None,
    )
    if min_conv is not None and min_conv > 0.0:
        if 0.0 < min_conv <= 1.0:
            min_conv = min_conv * 100.0
        row_conv = _safe_float(row.get("conviction_score"), None)
        if row_conv is None or row_conv < min_conv:
            return False, "min_conviction_score"

    if _coerce_bool(criteria.get("exclude_engine_dropped_valuation"), False):
        tags = _parse_warnings_tags(row)
        if _has_engine_drop_tag(tags):
            return False, "exclude_engine_dropped_valuation"

    if _coerce_bool(criteria.get("exclude_forecast_unavailable"), False):
        tags = _parse_warnings_tags(row)
        if _has_forecast_unavail_tag(tags, row):
            return False, "exclude_forecast_unavailable"

    if _coerce_bool(criteria.get("exclude_provider_errors"), False):
        if _has_provider_error(row):
            return False, "exclude_provider_errors"

    # -------------------------------------------------------------------------
    # v4.14.0 Phase G — criteria_model v3.2.0 forward-compat HARD FILTERS
    # -------------------------------------------------------------------------
    # All three filters default to off; honoured only when explicitly set
    # by the operator. They are evaluated AFTER the v3.1.0 filters so the
    # drop_counts meta block reflects the natural order of constraints.

    # 1. exclude_avoid_recommendations (bool) — drops rows where the row's
    #    recommendation normalizes to AVOID. Note that AVOID rows also
    #    receive a -8.0 selector_score bump (see Phase H in _selector_score),
    #    so they typically drop out of Top10 even without this flag. This
    #    filter is the HARD-exclusion path for operators who want a
    #    cleaner audit trail.
    if _coerce_bool(criteria.get("exclude_avoid_recommendations"), False):
        if _is_avoid_recommendation(row):
            return False, "exclude_avoid_recommendations"

    # 2. min_priority_band (str, "P1".."P5") — drops rows whose
    #    priority_band ranks WORSE than the floor. P1 is best (most
    #    urgent); P5 is worst. Blank / unparseable bands rank as 99 so
    #    they are dropped under any non-empty floor when reco_8tier_strict
    #    is on, and kept when strict is off (the legacy "can't determine
    #    -> keep" contract).
    mpb_floor_canon = _normalize_priority_band(criteria.get("min_priority_band"))
    if mpb_floor_canon:
        floor_rank = _priority_band_rank(mpb_floor_canon)
        row_band_canon = _normalize_priority_band(row.get("recommendation_priority_band"))
        if row_band_canon:
            if _priority_band_rank(row_band_canon) > floor_rank:
                return False, "min_priority_band"
        elif _coerce_bool(criteria.get("reco_8tier_strict"), False):
            # Strict mode: a row with no priority band fails the floor.
            return False, "min_priority_band"
        # Otherwise: blank band kept (matches legacy contract).

    # 3. reco_8tier_strict (bool) — drops rows where the recommendation
    #    is non-empty AND does not normalize to one of the canonical 8
    #    tiers. Catches legacy / typo'd / vendor-specific tokens that
    #    have not been routed through reco_normalize v8.0.0. Blank
    #    recommendations are NOT dropped under this flag — they only
    #    become a problem when paired with min_priority_band above.
    if _coerce_bool(criteria.get("reco_8tier_strict"), False):
        raw_reco = _s(row.get("recommendation"))
        if raw_reco and not _normalize_reco_token(raw_reco):
            return False, "reco_8tier_strict"

    return True, ""


def _passes_filters(row: Mapping[str, Any], criteria: Mapping[str, Any]) -> bool:
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

    conviction = _safe_float(row.get("conviction_score"), None)
    if conviction is not None:
        score += conviction * 0.05

    candle_signal = _s(row.get("candlestick_signal")).upper()
    candle_conf = _safe_float(row.get("candlestick_confidence"), None)
    if candle_signal == "BULLISH" and candle_conf is not None:
        score += min(candle_conf, 100.0) * 0.02
    elif candle_signal == "BEARISH" and candle_conf is not None:
        score -= min(candle_conf, 100.0) * 0.02

    # -------------------------------------------------------------------------
    # v4.14.0 Phase H — 8-tier recommendation tiebreak bump.
    # -------------------------------------------------------------------------
    # Small ±2.5-range bump per recommendation tier (AVOID is the
    # exception at -8.0). See `_RECO_TIEBREAK_BUMPS` for the canonical
    # magnitudes. These are TIEBREAKERS — the engine's `overall_score`
    # (weighted at 0.35 above) already incorporates the recommendation.
    # The bumps mostly matter when two rows have near-identical scores.
    #
    # AVOID's -8.0 magnitude is deliberately large so AVOID rows naturally
    # drop out of the Top10 without operators having to opt in to
    # `exclude_avoid_recommendations`. ACCUMULATE / STRONG_SELL get
    # symmetric ±1.0 / ±2.5 bumps so the v8.0.0 new-token signal lands
    # correctly under tied composites.
    score += _reco_tiebreak_bump(row)

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

    if _BUCKETS_AVAILABLE:
        if risk_bucket and _bk_normalize_risk_bucket is not None:
            try:
                risk_bucket = _bk_normalize_risk_bucket(risk_bucket) or risk_bucket
            except Exception:
                pass
        if confidence_bucket and _bk_normalize_confidence_bucket is not None:
            try:
                confidence_bucket = _bk_normalize_confidence_bucket(confidence_bucket) or confidence_bucket
            except Exception:
                pass

    horizon_days = _safe_int(criteria.get("horizon_days") or criteria.get("invest_period_days"), 90)
    horizon_roi = _choose_horizon_roi(row, horizon_days)

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
        # v4.14.0 Phase J — badge the three reco_normalize v8.0.0 NEW
        # tokens (ACCUMULATE / STRONG_SELL / AVOID) so operators can see
        # at a glance when a row's recommendation is one of the tokens
        # that did NOT exist before the v8.0.0 vocabulary expansion.
        reco_canon = _normalize_reco_token(recommendation)
        if reco_canon and reco_canon in _RECO_8TIER_NEW_TOKENS:
            parts.append(f"Recommendation={recommendation} [NEW v8: {reco_canon}]")
        else:
            parts.append(f"Recommendation={recommendation}")
    # v4.14.0 Phase J — priority band display (shows the P1..P5 urgency
    # band emitted by data_engine_v2 v5.74.0+ / advisor_engine v4.5.0
    # whenever it is present on the row).
    priority_band_canon = _normalize_priority_band(row.get("recommendation_priority_band"))
    if priority_band_canon:
        parts.append(f"Priority={priority_band_canon}")
    # v4.14.0 Phase J — provider-rating divergence. Surfaces only when
    # the raw provider rating disagrees with the engine's recommendation;
    # this helps operators see when the engine's 8-tier judgement
    # differs from what the upstream provider said.
    provider_rating = _s(row.get("provider_rating"))
    if provider_rating and recommendation:
        prov_canon = _normalize_reco_token(provider_rating)
        engine_canon = _normalize_reco_token(recommendation)
        if prov_canon and engine_canon and prov_canon != engine_canon:
            parts.append(f"Provider={provider_rating}|Engine={recommendation}")
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
        first_factor = top_factors.split("|")[0].strip()
        if first_factor:
            parts.append(f"Top factor: {first_factor}")
    if top_risks:
        first_risk = top_risks.split("|")[0].strip()
        if first_risk:
            parts.append(f"Top risk: {first_risk}")
    if score_parts:
        parts.append(", ".join(score_parts[:3]))

    tags = _parse_warnings_tags(row)
    caveats: List[str] = []
    if _has_provider_error(row):
        err = _s(row.get("last_error_class") or row.get("provider_last_error_class") or row.get("error"))
        caveats.append(f"Provider Error: {err}" if err else "Provider Error")
    if _has_engine_drop_tag(tags):
        first_tag = next((t for t in tags if any(m in t for m in _ENGINE_DROP_TAGS)), "")
        caveats.append(f"Engine: {first_tag}" if first_tag else "Engine valuation dropped")
    if _has_forecast_unavail_tag(tags, row):
        caveats.append("Forecast: unavailable")
    if caveats:
        parts.append("[" + "; ".join(caveats) + "]")

    return " | ".join(parts) if parts else "Selected by Top10 composite scoring."


def _compute_overall_rank_map(rows: Sequence[Mapping[str, Any]]) -> Dict[int, int]:
    ranked_positions = sorted(
        range(len(rows)),
        key=lambda i: (
            0 if _safe_float(rows[i].get("overall_score"), None) is not None else 1,
            -(_safe_float(rows[i].get("overall_score"), 0.0) or 0.0),
            i,
        ),
    )
    return {pos: rank for rank, pos in enumerate(ranked_positions, start=1)}


def _rank_and_project_rows(rows: Sequence[Mapping[str, Any]], keys: Sequence[str], criteria: Mapping[str, Any]) -> List[Dict[str, Any]]:
    criteria_snapshot = _json_compact(criteria)
    overall_rank_map = _compute_overall_rank_map(rows)
    out: List[Dict[str, Any]] = []
    for idx, raw in enumerate(rows, start=1):
        row = dict(raw)
        if _is_blank(row.get("top10_rank")):
            row["top10_rank"] = idx
        if _is_blank(row.get("rank_overall")):
            row["rank_overall"] = overall_rank_map.get(idx - 1, idx)
        if _is_blank(row.get("selection_reason")):
            row["selection_reason"] = _canonical_selection_reason(row, criteria)
        if _is_blank(row.get("criteria_snapshot")):
            row["criteria_snapshot"] = criteria_snapshot
        projected = {k: _json_safe(row.get(k)) for k in keys}
        out.append(projected)
    return out


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

        # v4.14.0 Phase K — parallel v8tier filter audit block. Records
        # which v4.14.0 filters fired so operators get a clean audit trail
        # for the 8-tier opt-in exclusions (matching the v3.1.0 pattern).
        _v8tier_active: Dict[str, Any] = {}
        if _coerce_bool(criteria.get("exclude_avoid_recommendations"), False):
            _v8tier_active["exclude_avoid_recommendations"] = True
        mpb_canon_active = _normalize_priority_band(criteria.get("min_priority_band"))
        if mpb_canon_active:
            _v8tier_active["min_priority_band"] = mpb_canon_active
        if _coerce_bool(criteria.get("reco_8tier_strict"), False):
            _v8tier_active["reco_8tier_strict"] = True

        horizon_days = _safe_int(criteria.get("horizon_days") or criteria.get("invest_period_days"), 90)
        scored: List[Tuple[float, Dict[str, Any]]] = []
        for row in selected_pool:
            scored.append((_selector_score(row, criteria), dict(row)))

        scored.sort(
            key=lambda x: (
                x[0],
                # v4.14.0 Phase I — priority band tiebreaker. NEGATED so that
                # under reverse=True the smaller rank (P1 = best / most urgent)
                # sorts first. Blank / unparseable bands rank as 99 and so
                # sort AFTER all real bands when scores are tied.
                -_priority_band_rank(x[1].get("recommendation_priority_band")),
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

        dq_engine_drop = 0
        dq_forecast_unavail = 0
        dq_provider_err = 0
        # v4.14.0 Phase K — 8-tier vocabulary counters for the
        # data_quality_summary block. Aggregated over the FINAL projected
        # rows so operators see what the Top10 surface actually contains.
        dq_avoid_count = 0
        dq_accumulate_count = 0
        dq_strong_sell_count = 0
        dq_reco_8tier_seen = 0
        dq_priority_band_seen = 0
        for _proj_row in projected_rows:
            _tags = _parse_warnings_tags(_proj_row)
            if _has_engine_drop_tag(_tags):
                dq_engine_drop += 1
            if _has_forecast_unavail_tag(_tags, _proj_row):
                dq_forecast_unavail += 1
            if _has_provider_error(_proj_row):
                dq_provider_err += 1
            _reco_canon = _normalize_reco_token(_proj_row.get("recommendation"))
            if _reco_canon:
                dq_reco_8tier_seen += 1
                if _reco_canon == "AVOID":
                    dq_avoid_count += 1
                elif _reco_canon == "ACCUMULATE":
                    dq_accumulate_count += 1
                elif _reco_canon == "STRONG_SELL":
                    dq_strong_sell_count += 1
            if _normalize_priority_band(_proj_row.get("recommendation_priority_band")):
                dq_priority_band_seen += 1

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
            "filter_drop_counts": dict(drop_counts),
            "applied_v310_filters": dict(_v310_active),
            # v4.14.0 Phase K — parallel 8-tier filter audit block.
            "applied_v8tier_filters": dict(_v8tier_active),
            # v4.14.0 Phase K — capability flag. Downstream consumers
            # (insights_builder v8.0.0, advisor route layer) can
            # content-check this to confirm the selector is aligned
            # with the v8.0.0 vocabulary.
            "reco_8tier_aware": True,
            "reco_8tier_aware_version": RECO_8TIER_AWARE_VERSION,
            "data_quality_summary": {
                "engine_dropped_valuation": dq_engine_drop,
                "forecast_unavailable": dq_forecast_unavail,
                "provider_errors": dq_provider_err,
                # v4.14.0 Phase K — 8-tier vocabulary counters.
                "avoid_count": dq_avoid_count,
                "accumulate_count": dq_accumulate_count,
                "strong_sell_count": dq_strong_sell_count,
                "reco_8tier_seen": dq_reco_8tier_seen,
                "priority_band_seen": dq_priority_band_seen,
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
    "__version__",
    # v4.14.0 Phase B/K: 8-tier capability marker.
    "RECO_8TIER_AWARE_VERSION",
    "DEFAULT_FALLBACK_KEYS",
    "DEFAULT_FALLBACK_HEADERS",
    "ROW_KEY_ALIASES",
    "TOP10_REQUIRED_FIELDS",
    "TOP10_REQUIRED_HEADERS",
    "TOP10_PENALTY_ENGINE_DROP",
    "TOP10_PENALTY_FORECAST_UNAVAIL",
    "TOP10_PENALTY_PROVIDER_ERROR",
    # v4.14.0 Phase C: 8-tier vocabulary surface (content-checkable
    # constants for ops + downstream tooling).
    "_RECO_8TIER_CANONICAL",
    "_BULLISH_RECOS",
    "_BEARISH_RECOS",
    "_RECO_8TIER_NEW_TOKENS",
    "_RECO_TIEBREAK_BUMPS",
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
