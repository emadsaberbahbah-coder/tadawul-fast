#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/investment_advisor.py
================================================================================
INVESTMENT ADVISOR ORCHESTRATOR -- v5.3.1
(SCHEMA v2.8.0 / SCORING v5.2.5 / RECO v7.2.0 / ENGINE v4.4.1 / TOP10 v4.12.0)
================================================================================
LIVE-BY-DEFAULT * ENGINE-FIRST * SNAPSHOT-TOLERANT * ROUTE-COMPATIBLE
MODE-AWARE * SCHEMA-SAFE * JSON-SAFE * IMPORT-SAFE * WORKER-THREAD SAFE
SPECIAL-PAGE SAFE * CONTRACT-PRESERVING * DICTIONARY-HARDENED
VIEW-AWARE * CONVICTION-AWARE * RULE-ID-AWARE * DECISION-MATRIX AWARE
CANDLESTICK AWARE

================================================================================
v5.3.1 changes (TOP10 v4.12.0 + ENGINE v4.4.1 ALIGNMENT)
================================================================================

METADATA-ONLY PATCH on top of v5.3.0. No positional indices shift. No
caller-visible behaviour changes. No column-count edits. No signature
edits. No new runtime code paths. The headline updates are:

  - core.analysis.top10_selector  advanced v4.11.0 -> v4.12.0
    (v4.12.0 activates 4 criteria_model v3.1.0 hard-filter fields:
     min_conviction_score, exclude_engine_dropped_valuation,
     exclude_forecast_unavailable, exclude_provider_errors)

  - core.investment_advisor_engine advanced v4.4.0 -> v4.4.1
    (v4.4.1 is itself a metadata-only sync that documents the top10
     hard-filter delegation; engine internals unchanged)

This module (the advisor ORCHESTRATOR) is upstream of both:
  advisor.run_investment_advisor()
      -> investment_advisor_engine v4.4.1 (resolution)
         -> data_engine_v2 v5.60.0 (row enrichment)
            -> scoring v5.2.5 + reco_normalize v7.2.0 (recommendations)
      -> top10_selector v4.12.0 (Top10 page only; hard-filter + ranking)

DIVISION OF LABOR (v5.3.1, unchanged from v5.3.0):
  - Orchestrator (this file): payload normalization, engine dispatch,
    result shaping, snapshot caching, schema-aligned fallbacks. Does NOT
    apply criteria_model v3.1.0 hard filters; that is top10_selector
    v4.12.0's job. Does NOT score rows; that is the engine + scoring
    pipeline's job.
  - Engine: row resolution from data sources; 97-col enrichment.
  - top10_selector v4.12.0: Top10 page criteria hard-filtering + soft
    penalties + final ranking.
  - reco_normalize v7.2.0: single source of truth for the 5-tier
    recommendation labels.

Phase-by-phase summary:
-----------------------

A. NARRATIVE SYNC. Header title line and the cross-stack module roster
   updated to current sibling versions.

B. VERSION BUMP 5.3.0 -> 5.3.1. `__version__` alias auto-tracks.

PRESERVED VERBATIM from v5.3.0:
- 97-col fallback schema (Decision Matrix + Candlestick groups intact)
- 100-col Top10 layout (97 + 3 extras)
- 4-tier view-aware reco delegation cascade
- _is_view_aware_available / _is_view_aware_rule_id_available helpers
- meta.view_aware + meta.view_aware_rule_id observability flags
- All 86 _FIELD_ALIAS_HINTS entries
- Internal field stripping (v5.1.1 + v5.2.0 + v5.3.0)
- ROI-safe scoring, sentinel handling, bucket fabrication safety
- All public API (InvestmentAdvisor class, singletons, factories,
  sync wrappers, async _impl hooks)

================================================================================
v5.3.0 changes (May 2026 cross-stack family)
================================================================================

Brings the advisor orchestrator into alignment with the May 2026
cross-stack family that landed across the rest of the Python backend:

    core/sheets/schema_registry      v2.8.0   (97-col canonical / 100-col Top10)
    core/data_engine_v2              v5.60.0  (warning canonicalization,
                                                5 engine-drop tags,
                                                4 forecast-unavail tags,
                                                last_error_class preservation)
    core/reco_normalize              v7.2.0   (rule-id-aware delegation,
                                                14 RULE_ID_* constants,
                                                env-tunable conviction floors,
                                                AVOID -> SELL fix,
                                                conviction-badge de-dup)
    core/scoring                     v5.2.5   (5 Insights row fields +
                                                scoring_errors)
    core/insights_builder            v7.0.0   (env-tunable conviction-floor
                                                introspection helpers)
    core/scoring_engine              v3.4.2   (compatibility bridge,
                                                is_reco_rule_id_aware helper)
    core/analysis/top10_selector     v4.12.0  (data-quality penalties +
                                                criteria_model v3.1.0
                                                hard-filter delegation)
    core/analysis/criteria_model     v3.1.0   (conviction + exclusion model)
    core/investment_advisor_engine   v4.4.1   (97-col fallback, rule-id +
                                                conviction-aware delegation,
                                                top10_selector v4.12.0 alignment)

Phases:

  A. Header docstring cross-stack version sync. Updates references to
     8 sibling modules across the May 2026 family. Drops the stale
     "run_dashboard_sync v6.6.0" reference since that module's version
     pin is out of scope for the orchestrator.

  B. Schema width 90 -> 97 cols. Adds two NEW column groups that
     landed in schema_registry v2.7.0:

       - Decision (2):
           recommendation_detailed   (8-tier verdict)
           recommendation_priority   (1-8 priority rule that fired)
         Produced by core.data_engine_v2 v5.60.0+'s 8-tier classifier
         _classify_8tier(). Empty when core.scoring -> core.reco_normalize
         sets the canonical 5-tier `recommendation` upstream.

       - Candlestick (5):
           candlestick_pattern, candlestick_signal,
           candlestick_strength, candlestick_confidence,
           candlestick_patterns_recent
         Produced by core.candlesticks v1.0.0 when invoked from
         core.data_engine_v2 v5.60.0+ on OHLC history rows. Best-effort
         and OPTIONAL, gated by ENGINE_CANDLESTICKS_ENABLED env flag;
         stay null when the candlesticks module is unavailable.

     Total column counts:
       Generic fallback:  90 (v5.2.0) -> 97 (v5.3.0)
       Top10:             93 (v5.2.0) -> 100 (v5.3.0)
       Insights:           7 unchanged
       Dictionary:         9 unchanged

  C. Rule-id-aware reco delegation (additive observability).
     reco_normalize v7.2.0 introduced
     `recommendation_from_views_with_rule_id(...)` which returns
     `(reco, reason, rule_id)` where rule_id identifies which of 14
     cascade rules fired (RULE_ID_STRONG_BUY_BULLISH_CHEAP,
     RULE_ID_AVOID_DOUBLE_BEARISH, RULE_ID_HOLD_INSUFFICIENT_VIEWS, etc.).
     v5.3.0 adds a NEW preferred tier at the top of the
     `_try_view_aware_recommendation` cascade:

       Tier 1 (PREFERRED): recommendation_from_views_with_rule_id  (v7.2.0+)
       Tier 2 (FALLBACK):  recommendation_from_views w/ conviction (v7.1.0+)
       Tier 3 (FALLBACK):  recommendation_from_views w/o conviction (v7.0.0)
       Tier 4 (FALLBACK):  legacy composite-threshold scoring

     reco_normalize v7.2.0 Phase D already handles conviction-badge
     de-dup and embeds the rule_id in the reason where appropriate,
     so v5.3.0 uses the reason as-is. The `(canonical_code, reason,
     overall_score)` return shape of `_try_view_aware_recommendation`
     is preserved verbatim -- rule_id is captured for meta
     observability via `_RECO_RULE_ID_AVAILABLE` only.
     Backwards-compatible: pre-v7.2.0 reco_normalize installations
     fall through to Tier 2 unchanged.

  D. `_FIELD_ALIAS_HINTS` extended with vendor / camelCase mirrors for
     the seven new columns:
       recommendation_detailed:     recommendationDetailed, recoDetailed,
                                    detailed_recommendation,
                                    decision_detailed
       recommendation_priority:     recommendationPriority, recoPriority,
                                    priority, decisionPriority,
                                    reco_priority
       candlestick_pattern:         candlestickPattern, candlePattern,
                                    pattern
       candlestick_signal:          candlestickSignal, candleSignal,
                                    pattern_signal
       candlestick_strength:        candlestickStrength, candleStrength,
                                    pattern_strength
       candlestick_confidence:      candlestickConfidence,
                                    candleConfidence,
                                    pattern_confidence
       candlestick_patterns_recent: candlestickPatternsRecent,
                                    patterns_recent, recent_patterns

  E. NEW `__version__ = INVESTMENT_ADVISOR_VERSION` alias (TFB module
     convention used by scoring v5.2.5, reco_normalize v7.2.0,
     insights_builder v7.0.0, scoring_engine v3.4.2, top10_selector
     v4.12.0, criteria_model v3.1.0, schema_registry v2.8.0,
     investment_advisor_engine v4.4.1).

  F. `__all__` augmented with `__version__`.

  G. Version bump 5.2.0 -> 5.3.0.

  H. Meta enrichment: `meta.view_aware` and `meta.view_aware_rule_id`
     now reflect whether the reco_normalize v7.0.0+ / v7.2.0+ paths
     are wired into THIS process. Mirrors the same flags emitted by
     investment_advisor_engine v4.4.1 so an ops dashboard can detect
     a partial v7.x deployment (e.g. one process pinned to v7.1.0
     while another runs v7.2.0).

The v5.1.1 internal-field stripping, ROI-safe scoring,
recommendation-synthesis sentinel handling, and bucket fabrication
fixes remain in place verbatim. The v5.2.0 view-aware delegation,
`conviction_score` as a first-class scoring signal, and removal of
the legacy `convictionScore` alias from `opportunity_score` are all
preserved.

================================================================================
v5.2.0 changes (preserved)
================================================================================
- ALIGN: _GENERIC_FALLBACK_KEYS rebuilt from 80 -> 90 cols (incl. Views
  group + Insights group + Upside %).
- ALIGN: _FIELD_ALIAS_HINTS removed misleading convictionScore alias
  from opportunity_score; added aliases for all 10 new keys.
- ENHANCE: _score_recommendation tries view-aware path first via
  core.reco_normalize.recommendation_from_views().
- ALIGN: _SCORING_SIGNAL_FIELDS includes conviction_score.

================================================================================
v5.1.1 changes (preserved)
================================================================================
- FIX: _resolve_roi_for_scoring replaces the unsafe
  "abs(roi) <= 1.5 -> roi *= 100" heuristic.
- FIX: _backfill_rows respects _skip_recommendation_synthesis sentinel.
- FIX: empty-data rows kept as None recommendation (not RECO_HOLD/SELL).
- FIX: defensive internal-field stripping throughout.
- FIX: status="warn" from engine treated as success-with-caveat.

================================================================================
v5.1.0 changes (preserved)
================================================================================
- Insights / Data_Dictionary / instrument fallbacks aligned to canonical
  schema columns and registry order.
- Forbidden pages removed from ALL_KNOWN_PAGES.
- _load_headers_for_page made sync.

Public API preserved: `InvestmentAdvisor`, the `advisor` /
`investment_advisor` singletons, factory functions, sync wrappers, and
`_run_investment_advisor_impl` / `_run_advisor_impl` async hooks all
resolve under the same names.
================================================================================
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import inspect
import json
import logging
import math
import os
import re
import threading
import time
from copy import deepcopy
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

try:
    from zoneinfo import ZoneInfo
    _HAS_ZONEINFO = True
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore
    _HAS_ZONEINFO = False


# =============================================================================
# Logging Setup
# =============================================================================

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


# =============================================================================
# Version
# =============================================================================

INVESTMENT_ADVISOR_VERSION = "5.3.1"
# v5.3.0 Phase E: TFB module-version convention alias (mirrors scoring
# v5.2.5, reco_normalize v7.2.0, insights_builder v7.0.0, scoring_engine
# v3.4.2, top10_selector v4.12.0, criteria_model v3.1.0, schema_registry
# v2.8.0, investment_advisor_engine v4.4.1).
# v5.3.1: top10_selector advanced 4.11.0 -> 4.12.0; engine advanced
# 4.4.0 -> 4.4.1; sibling-version references updated here.
__version__ = INVESTMENT_ADVISOR_VERSION


# =============================================================================
# Recommendation Constants
# =============================================================================

try:
    from core.reco_normalize import (  # type: ignore
        RECO_STRONG_BUY,
        RECO_BUY,
        RECO_HOLD,
        RECO_REDUCE,
        RECO_SELL,
    )
except ImportError:
    RECO_STRONG_BUY = "STRONG_BUY"
    RECO_BUY = "BUY"
    RECO_HOLD = "HOLD"
    RECO_REDUCE = "REDUCE"
    RECO_SELL = "SELL"


# =============================================================================
# Constants
# =============================================================================

DEFAULT_PAGE = "Top_10_Investments"
DEFAULT_LIMIT = 10
DEFAULT_OFFSET = 0
DEFAULT_SNAPSHOT_TTL_SEC = 900

BASE_SOURCE_PAGES = {
    "Market_Leaders",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
    "My_Portfolio",
}

SPECIAL_PAGES = {
    "Top_10_Investments",
    "Insights_Analysis",
    "Data_Dictionary",
}

# Registry-canonical known pages. Forbidden pages (KSA_Tadawul,
# Advisor_Criteria) are intentionally excluded -- page_catalog rejects them
# and keeping them here silently re-admitted them.
ALL_KNOWN_PAGES = BASE_SOURCE_PAGES | SPECIAL_PAGES | {"AI_Opportunity_Report"}

PAGE_ALIASES = {
    "top10": "Top_10_Investments",
    "top_10": "Top_10_Investments",
    "top_10_investments": "Top_10_Investments",
    "top-10-investments": "Top_10_Investments",
    "top10investments": "Top_10_Investments",
    "investment_advisor": "Top_10_Investments",
    "investment-advisor": "Top_10_Investments",
    "advisor": "Top_10_Investments",
    "insights": "Insights_Analysis",
    "insights_analysis": "Insights_Analysis",
    "insights-analysis": "Insights_Analysis",
    "data_dictionary": "Data_Dictionary",
    "data-dictionary": "Data_Dictionary",
    "dictionary": "Data_Dictionary",
    "market_leaders": "Market_Leaders",
    "market-leaders": "Market_Leaders",
    "global_markets": "Global_Markets",
    "global-markets": "Global_Markets",
    "mutual_funds": "Mutual_Funds",
    "mutual-funds": "Mutual_Funds",
    "commodities_fx": "Commodities_FX",
    "commodities-fx": "Commodities_FX",
    "my_portfolio": "My_Portfolio",
    "my-portfolio": "My_Portfolio",
    # Backward-compat alias (registry does not carry My_Investments, but some
    # callers still send it).
    "my_investments": "My_Portfolio",
    "my-investments": "My_Portfolio",
}

ENGINE_MODULE_CANDIDATES = (
    "core.investment_advisor_engine",
    "core.investment_advisor",
)

ENGINE_FUNCTION_CANDIDATES = (
    "run_investment_advisor_engine",
    "run_investment_advisor",
    "run_advisor",
    "execute_investment_advisor",
    "execute_advisor",
    "recommend",
    "recommend_investments",
    "get_recommendations",
    "build_recommendations",
)

ENGINE_OBJECT_CANDIDATES = (
    "investment_advisor_engine",
    "advisor_engine",
    "investment_advisor_service",
    "advisor_service",
    "investment_advisor_runner",
    "advisor_runner",
    "investment_advisor",
    "advisor",
)

ENGINE_OBJECT_METHOD_CANDIDATES = ENGINE_FUNCTION_CANDIDATES

SCHEMA_MODULE_CANDIDATES = (
    "core.sheets.schema_registry",
    "core.sheets.page_catalog",
    "core.schema_registry",
    "core.schemas",
    "schema_registry",
    "page_catalog",
)

SCHEMA_FUNCTION_CANDIDATES = (
    "get_sheet_spec",
    "get_page_spec",
    "get_schema_for_page",
    "sheet_spec",
    "build_sheet_spec",
)


# =============================================================================
# Canonical fallback schemas -- aligned with core.sheets.schema_registry v2.8.0
# =============================================================================
#
# Do NOT add non-canonical fields. The registry is the single source of
# truth; these lists exist only to keep the advisor functional when the
# registry can't be imported.
#
# v5.3.0 Phase B: column count 90 -> 97. Adds Decision Matrix (2) and
# Candlestick (5) groups at the END, matching schema_registry v2.7.0+
# additions. The Scores group ordering (positions 60-66) already matched
# the registry in v5.2.0 -- opportunity_score and rank_overall live
# INSIDE Scores, BEFORE the Views group.
#
# Totals:
#     Canonical instrument        97 cols  (v5.2.0: 90, v5.3.0: +7)
#     Top_10_Investments          100 cols (97 + 3 Top10 extras)
#     Insights_Analysis            7 cols  (unchanged)
#     Data_Dictionary              9 cols  (unchanged)
#
# Column order matches schema_registry.SCHEMA_REGISTRY["Market_Leaders"].

# Canonical 97 instrument columns:
#   Identity 8 / Price 10 / Liquidity 6 / Fundamentals 12 / Risk 8 /
#   Valuation 7 / Forecast 9 / Scores 7 / Views 4 / Recommendation 4 /
#   Portfolio 6 / Provenance 4 / Insights 5 / Decision 2 / Candlestick 5
#   = 97
_GENERIC_FALLBACK_KEYS: List[str] = [
    # Identity (8)
    "symbol", "name", "asset_class", "exchange", "currency", "country", "sector", "industry",
    # Price (10)
    "current_price", "previous_close", "open_price", "day_high", "day_low",
    "week_52_high", "week_52_low", "price_change", "percent_change", "week_52_position_pct",
    # Liquidity (6)
    "volume", "avg_volume_10d", "avg_volume_30d", "market_cap", "float_shares", "beta_5y",
    # Fundamentals (12)
    "pe_ttm", "pe_forward", "eps_ttm", "dividend_yield", "payout_ratio", "revenue_ttm",
    "revenue_growth_yoy", "gross_margin", "operating_margin", "profit_margin",
    "debt_to_equity", "free_cash_flow_ttm",
    # Risk (8)
    "rsi_14", "volatility_30d", "volatility_90d", "max_drawdown_1y",
    "var_95_1d", "sharpe_1y", "risk_score", "risk_bucket",
    # Valuation (7)  [v2.4.0: +upside_pct]
    "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio", "intrinsic_value",
    "upside_pct", "valuation_score",
    # Forecast (9)
    "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
    "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "forecast_confidence", "confidence_score", "confidence_bucket",
    # Scores (7) -- registry-canonical ordering: opportunity + rank INSIDE
    "value_score", "quality_score", "momentum_score", "growth_score",
    "overall_score", "opportunity_score", "rank_overall",
    # Views (4)  [v2.3.0]
    "fundamental_view", "technical_view", "risk_view", "value_view",
    # Recommendation (4)
    "recommendation", "recommendation_reason", "horizon_days", "invest_period_label",
    # Portfolio (6)
    "position_qty", "avg_cost", "position_cost", "position_value",
    "unrealized_pl", "unrealized_pl_pct",
    # Provenance (4)
    "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",
    # Insights (5)  [v2.6.0]
    "sector_relative_score", "conviction_score", "top_factors", "top_risks", "position_size_hint",
    # Decision (2) -- NEW v5.3.0 / aligned with schema_registry v2.7.0+
    # Produced by data_engine_v2 v5.60.0+'s 8-tier classifier _classify_8tier().
    "recommendation_detailed", "recommendation_priority",
    # Candlestick (5) -- NEW v5.3.0 / aligned with schema_registry v2.7.0+
    # Produced by core.candlesticks v1.0.0; gated by ENGINE_CANDLESTICKS_ENABLED.
    "candlestick_pattern", "candlestick_signal", "candlestick_strength",
    "candlestick_confidence", "candlestick_patterns_recent",
]

_GENERIC_FALLBACK_HEADERS: List[str] = [
    # Identity (8)
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
    # Price (10)
    "Current Price", "Previous Close", "Open", "Day High", "Day Low",
    "52W High", "52W Low", "Price Change", "Percent Change", "52W Position %",
    # Liquidity (6)
    "Volume", "Avg Volume 10D", "Avg Volume 30D", "Market Cap", "Float Shares", "Beta (5Y)",
    # Fundamentals (12)
    "P/E (TTM)", "P/E (Forward)", "EPS (TTM)", "Dividend Yield", "Payout Ratio",
    "Revenue (TTM)", "Revenue Growth YoY", "Gross Margin", "Operating Margin",
    "Profit Margin", "Debt/Equity", "Free Cash Flow (TTM)",
    # Risk (8)
    "RSI (14)", "Volatility 30D", "Volatility 90D", "Max Drawdown 1Y",
    "VaR 95% (1D)", "Sharpe (1Y)", "Risk Score", "Risk Bucket",
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
    "Position Qty", "Avg Cost", "Position Cost", "Position Value",
    "Unrealized P/L", "Unrealized P/L %",
    # Provenance (4)
    "Data Provider", "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",
    # Insights (5) -- note registry uses "Sector-Adj Score" not "Sector Relative Score"
    "Sector-Adj Score", "Conviction Score", "Top Factors", "Top Risks", "Position Size Hint",
    # Decision (2) -- NEW v5.3.0
    "Recommendation Detail", "Reco Priority",
    # Candlestick (5) -- NEW v5.3.0
    "Candle Pattern", "Candle Signal", "Candle Strength",
    "Candle Confidence", "Recent Patterns (5D)",
]

# Insights_Analysis: exactly 7 cols matching the registry verbatim
_INSIGHTS_KEYS: List[str] = [
    "section", "item", "symbol", "metric", "value", "notes", "last_updated_riyadh",
]
_INSIGHTS_HEADERS: List[str] = [
    "Section", "Item", "Symbol", "Metric", "Value", "Notes", "Last Updated (Riyadh)",
]

# Data_Dictionary: exactly 9 cols matching the registry verbatim
_DICTIONARY_KEYS: List[str] = [
    "sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes",
]
_DICTIONARY_HEADERS: List[str] = [
    "Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes",
]

# Top_10_Investments extras (3) -- registry appends these to the canonical 97
# (v5.3.0: was 90 in v5.2.0; the 3 Top10 extras count is unchanged)
_TOP10_EXTRA_KEYS: List[str] = ["top10_rank", "selection_reason", "criteria_snapshot"]
_TOP10_EXTRA_HEADERS: List[str] = ["Top10 Rank", "Selection Reason", "Criteria Snapshot"]

# v5.3.0 alias map. Changes vs v5.2.0:
#   - PRESERVED: all v5.2.0 entries (incl. the convictionScore-removed
#     from opportunity_score, and the 10 Wave-3 keys).
#   - NEW (Phase D): entries for the 7 new schema columns:
#     recommendation_detailed, recommendation_priority,
#     candlestick_pattern, candlestick_signal, candlestick_strength,
#     candlestick_confidence, candlestick_patterns_recent.
_FIELD_ALIAS_HINTS: Dict[str, List[str]] = {
    "symbol": ["ticker", "code", "requested_symbol", "symbol_normalized"],
    "ticker": ["symbol", "code", "requested_symbol"],
    "name": ["company_name", "long_name", "instrument_name", "security_name", "title", "shortName", "longName"],
    "current_price": ["price", "last_price", "last", "close", "market_price", "nav", "regularMarketPrice", "lastPrice"],
    "previous_close": ["prev_close", "prior_close", "regularMarketPreviousClose"],
    "open_price": ["open", "regularMarketOpen"],
    "day_high": ["high", "regularMarketDayHigh"],
    "day_low": ["low", "regularMarketDayLow"],
    "price_change": ["change", "net_change", "regularMarketChange"],
    "percent_change": ["change_pct", "change_percent", "pct_change", "regularMarketChangePercent"],
    # engine v5.47.4 mirrors
    "week_52_position_pct": ["position_52w_pct", "fifty_two_week_position_pct", "52w_position_pct"],
    "week_52_high": ["fiftyTwoWeekHigh", "fifty_two_week_high", "year_high", "52w_high"],
    "week_52_low": ["fiftyTwoWeekLow", "fifty_two_week_low", "year_low", "52w_low"],
    "market_cap": ["marketCap", "market_capitalization"],
    "float_shares": ["floatShares", "sharesFloat"],
    "beta_5y": ["beta"],
    "pe_ttm": ["trailingPE", "peRatio", "pe"],
    "pe_forward": ["forwardPE", "forward_pe"],
    "eps_ttm": ["trailingEps", "eps"],
    "dividend_yield": ["dividendYield", "trailingAnnualDividendYield"],
    "payout_ratio": ["payoutRatio"],
    "revenue_ttm": ["totalRevenue"],
    "revenue_growth_yoy": ["revenueGrowth"],
    "gross_margin": ["grossMargins"],
    "operating_margin": ["operatingMargins"],
    "profit_margin": ["profitMargins", "netMargin"],
    "debt_to_equity": ["debtToEquity"],
    "free_cash_flow_ttm": ["freeCashflow", "fcf_ttm"],
    "rsi_14": ["rsi", "rsi14"],
    "volatility_30d": ["vol30d", "volatility30d"],
    "volatility_90d": ["vol90d", "volatility90d"],
    "max_drawdown_1y": ["maxDrawdown1y", "drawdown1y"],
    # engine v5.47.4 valuation mirrors
    "pb_ratio": ["pb", "priceToBook"],
    "ps_ratio": ["ps", "priceToSalesTrailing12Months"],
    "ev_ebitda": ["ev_to_ebitda", "evToEbitda", "enterpriseToEbitda"],
    "peg_ratio": ["peg", "pegRatio"],
    "intrinsic_value": ["fairValue", "fair_value", "dcf"],
    # v5.2.0: upside_pct first-class home
    "upside_pct": ["upsidePct", "upside", "upside_percent", "upside_to_intrinsic"],
    "risk_score": ["risk", "riskscore"],
    "valuation_score": ["valuation", "valuationscore"],
    "overall_score": ["score", "overall", "totalscore", "compositeScore"],
    # v5.2.0: removed `convictionScore` from opportunity_score aliases --
    # it now belongs to the canonical conviction_score key below.
    "opportunity_score": ["opportunity", "opportunityscore"],
    "value_score": ["valueScore"],
    "quality_score": ["qualityScore"],
    "momentum_score": ["momentumScore"],
    "growth_score": ["growthScore"],
    # engine v5.47.4 confidence mirrors
    "confidence_score": ["confidence", "confidence_pct", "ai_confidence", "modelConfidenceScore"],
    "forecast_confidence": ["confidence", "ai_confidence", "modelConfidence"],
    "expected_roi_1m": ["roi_1m", "expected_return_1m", "expected_roi_1m_pct"],
    "expected_roi_3m": ["roi_3m", "expected_return_3m", "expected_roi_3m_pct"],
    "expected_roi_12m": ["roi_12m", "expected_return_12m", "expected_roi_12m_pct"],
    "forecast_price_1m": ["target_price_1m", "projected_price_1m"],
    "forecast_price_3m": ["target_price_3m", "projected_price_3m", "targetMeanPrice"],
    "forecast_price_12m": ["target_price_12m", "projected_price_12m", "targetMedianPrice"],
    # v5.2.0: Views group (schema v2.3.0)
    "fundamental_view": ["fundamentalView", "fund_view", "fundamentals_view"],
    "technical_view": ["technicalView", "tech_view"],
    "risk_view": ["riskView"],
    "value_view": ["valueView", "valuation_view"],
    "recommendation": ["signal", "rating", "action", "reco"],
    "recommendation_reason": ["rationale", "reasoning", "signal_reason", "reason", "thesis"],
    "selection_reason": ["reason", "recommendation_reason", "reco_reason"],
    "criteria_snapshot": ["criteria", "criteria_json", "snapshot"],
    # engine v5.47.4 provenance mirrors
    "data_provider": ["provider", "source_provider", "primary_provider", "provider_primary"],
    "last_updated_utc": ["updated_at_utc", "last_updated", "timestamp_utc", "as_of_utc", "asOf"],
    "last_updated_riyadh": ["updated_at_riyadh", "as_of_riyadh", "timestamp_riyadh"],
    "warnings": ["warning", "messages", "errors", "issues"],
    # engine v5.47.4 final-stage backfill mirrors
    "volume_ratio": ["volumeRatio"],
    "day_range_position": ["dayRangePosition"],
    # v5.2.0: Insights group (schema v2.6.0)
    "sector_relative_score": [
        "sectorRelativeScore", "sector_rel", "sector_rel_score",
        "sector_adj_score", "sectorAdjScore", "sector_relative",
    ],
    "conviction_score": ["conviction", "convictionScore", "conviction_pct"],
    "top_factors": ["topFactors", "factors", "top_factor_list"],
    "top_risks": ["topRisks", "risks", "top_risk_list"],
    "position_size_hint": [
        "positionSizeHint", "position_hint", "sizeHint",
        "size_hint", "position_size",
    ],
    # v5.3.0 Phase D: Decision Matrix columns (data_engine_v2 v5.60.0+
    # 8-tier classifier _classify_8tier output).
    "recommendation_detailed": [
        "recommendationDetailed", "recoDetailed",
        "detailed_recommendation", "detailedRecommendation",
        "decision_detailed",
    ],
    "recommendation_priority": [
        "recommendationPriority", "recoPriority",
        "priority", "decisionPriority", "reco_priority",
    ],
    # v5.3.0 Phase D: Candlestick columns (core.candlesticks v1.0.0
    # 10-pattern detector output).
    "candlestick_pattern": [
        "candlestickPattern", "candlePattern", "pattern",
    ],
    "candlestick_signal": [
        "candlestickSignal", "candleSignal", "pattern_signal",
    ],
    "candlestick_strength": [
        "candlestickStrength", "candleStrength", "pattern_strength",
    ],
    "candlestick_confidence": [
        "candlestickConfidence", "candleConfidence",
        "pattern_confidence", "candle_confidence",
    ],
    "candlestick_patterns_recent": [
        "candlestickPatternsRecent", "candlePatternsRecent",
        "patterns_recent", "recent_patterns", "recentPatterns",
    ],
}


# =============================================================================
# Internal field stripping (v5.1.1)
# =============================================================================

_INTERNAL_FIELD_PREFIXES: Tuple[str, ...] = (
    "_skip_", "_internal_", "_meta_", "_debug_", "_trace_",
)
_INTERNAL_FIELDS_PRESERVED_TEMPORARILY: set = {
    "_skip_recommendation_synthesis",
}
_INTERNAL_FIELDS_TO_STRIP_HARD: set = {
    "_placeholder",
    "unit_normalization_warnings",
    "intrinsic_value_source",
}


def _strip_internal_fields(row: Any, *, hard: bool = False) -> Any:
    """Remove engine internal coordination flags from a row dict.

    Args:
        row: The row dict to mutate
        hard: If True, also strip flags in `_INTERNAL_FIELDS_PRESERVED_TEMPORARILY`
              (use after they've been consumed downstream).
    """
    if not isinstance(row, dict):
        return row
    keys_to_remove: List[str] = []
    for k in list(row.keys()):
        ks = str(k)
        if ks in _INTERNAL_FIELDS_TO_STRIP_HARD:
            keys_to_remove.append(k)
            continue
        if hard and ks in _INTERNAL_FIELDS_PRESERVED_TEMPORARILY:
            keys_to_remove.append(k)
            continue
        if ks in _INTERNAL_FIELDS_PRESERVED_TEMPORARILY:
            continue  # Keep for now; consumed by _backfill_rows
        if any(ks.startswith(prefix) for prefix in _INTERNAL_FIELD_PREFIXES):
            keys_to_remove.append(k)
    for k in keys_to_remove:
        try:
            del row[k]
        except Exception:
            pass
    return row


# =============================================================================
# Enums
# =============================================================================

class AdvisorMode(str, Enum):
    """Advisor execution modes."""
    LIVE_QUOTES = "live_quotes"
    LIVE_SHEET = "live_sheet"
    SNAPSHOT = "snapshot"
    AUTO = "auto"


class AdvisorStatus(str, Enum):
    """Advisor response status."""
    SUCCESS = "success"
    WARN = "warn"
    ERROR = "error"


# =============================================================================
# Custom Exceptions
# =============================================================================

class InvestmentAdvisorError(Exception):
    """Base exception for investment advisor."""
    pass


class EngineResolutionError(InvestmentAdvisorError):
    """Raised when engine cannot be resolved."""
    pass


class InvalidPageError(InvestmentAdvisorError):
    """Raised when page is invalid."""
    pass


# =============================================================================
# Configuration
# =============================================================================

@dataclass(frozen=True)
class InvestmentAdvisorConfig:
    """Configuration for investment advisor."""
    default_page: str = DEFAULT_PAGE
    default_limit: int = DEFAULT_LIMIT
    default_offset: int = DEFAULT_OFFSET
    snapshot_ttl_sec: int = DEFAULT_SNAPSHOT_TTL_SEC
    max_limit: int = 200
    min_limit: int = 1
    enable_snapshot_cache: bool = True

    @classmethod
    def from_env(cls) -> "InvestmentAdvisorConfig":
        """Load configuration from environment."""
        def _env_int(name: str, default: int) -> int:
            try:
                return int(os.getenv(name, str(default)))
            except Exception:
                return default

        return cls(
            default_limit=_env_int("ADVISOR_DEFAULT_LIMIT", DEFAULT_LIMIT),
            snapshot_ttl_sec=_env_int("ADVISOR_SNAPSHOT_TTL_SEC", DEFAULT_SNAPSHOT_TTL_SEC),
            max_limit=_env_int("ADVISOR_MAX_LIMIT", 200),
            min_limit=_env_int("ADVISOR_MIN_LIMIT", 1),
            enable_snapshot_cache=(
                os.getenv("ADVISOR_ENABLE_SNAPSHOT_CACHE", "true").lower()
                in {"1", "true", "yes", "y", "on"}
            ),
        )


_CONFIG = InvestmentAdvisorConfig.from_env()


# =============================================================================
# Pure Utility Functions
# =============================================================================

def _to_string(value: Any) -> str:
    """Safely convert to string."""
    if value is None:
        return ""
    try:
        s = str(value).strip()
        return "" if s.lower() in {"none", "null", "nil"} else s
    except Exception:
        return ""


def _to_int(value: Any, default: int = 0) -> int:
    """Safely convert to integer."""
    try:
        if isinstance(value, bool):
            return default
        return int(float(value))
    except Exception:
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
    """Safely convert to float."""
    try:
        if isinstance(value, bool):
            return default
        if value is None:
            return default
        f = float(value)
        return default if math.isnan(f) or math.isinf(f) else f
    except Exception:
        return default


def _to_float_optional(value: Any) -> Optional[float]:
    """v5.1.1: Convert to float OR return None if not convertible."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _to_bool(value: Any, default: bool = False) -> bool:
    """Safely convert to boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        s = value.strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False
    if isinstance(value, (int, float)):
        try:
            return bool(int(value))
        except Exception:
            return default
    return default


def _is_blank(value: Any) -> bool:
    """Check if value is blank."""
    return value is None or (isinstance(value, str) and not value.strip())


def _deduplicate_keep_order(values: Iterable[Any]) -> List[str]:
    """Deduplicate items while preserving order."""
    result: List[str] = []
    seen: set = set()
    for v in values:
        s = _to_string(v)
        if not s or s in seen:
            continue
        seen.add(s)
        result.append(s)
    return result


def _normalize_list(value: Any) -> List[str]:
    """Normalize value to list of strings."""
    if value is None:
        return []
    if isinstance(value, str):
        raw = value.replace(";", ",").replace("\n", ",")
        parts = [p.strip() for p in raw.split(",")]
        return _deduplicate_keep_order([p for p in parts if p])
    if isinstance(value, (list, tuple, set)):
        return _deduplicate_keep_order(list(value))
    return _deduplicate_keep_order([value])


def _json_safe(value: Any) -> Any:
    """Convert value to JSON-safe format."""
    try:
        if is_dataclass(value):
            value = asdict(value)
        elif hasattr(value, "model_dump") and callable(value.model_dump):
            value = value.model_dump(mode="python")
        elif hasattr(value, "dict") and callable(value.dict):
            value = value.dict()
    except Exception:
        pass

    try:
        return json.loads(json.dumps(value, ensure_ascii=False, default=str))
    except Exception:
        try:
            return str(value)
        except Exception:
            return None


def _json_compact(value: Any) -> str:
    """Convert value to compact JSON string."""
    try:
        return json.dumps(_json_safe(value), ensure_ascii=False, separators=(",", ":"))
    except Exception:
        try:
            return str(value)
        except Exception:
            return ""


def _now_utc_iso() -> str:
    """Current UTC time in ISO format."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _now_riyadh_iso() -> str:
    """Current Asia/Riyadh time in ISO format (falls back to UTC+3 if zoneinfo is missing)."""
    try:
        if _HAS_ZONEINFO and ZoneInfo is not None:
            return datetime.now(ZoneInfo("Asia/Riyadh")).isoformat(timespec="seconds")
    except Exception:
        pass
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat(timespec="seconds")


def _deepcopy_json_safe(value: Any) -> Any:
    """Deep copy JSON-safe value."""
    try:
        return deepcopy(value)
    except Exception:
        return _json_safe(value)


def _rows_to_matrix(rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Any]]:
    """Convert rows to matrix."""
    return [[row.get(key) for key in keys] for row in rows]


def _title_case_header(key: str) -> str:
    """Convert key to title case header (used only when a registry header is unavailable)."""
    text = _to_string(key).replace("_", " ").replace("-", " ").strip()
    if not text:
        return ""
    return " ".join(
        part.capitalize() if part.upper() != part else part
        for part in text.split()
    )


def _normalize_key(key: str) -> str:
    """Normalize key for matching."""
    return _to_string(key).strip().lower().replace(" ", "_").replace("-", "_")


def _normalize_key_loose(key: str) -> str:
    """Loose normalization (remove non-alphanumeric)."""
    return re.sub(r"[^a-z0-9]+", "", _to_string(key).lower())


def _slice_rows(rows: List[Dict[str, Any]], offset: int, limit: int) -> List[Dict[str, Any]]:
    """Slice rows by offset and limit."""
    start = max(0, _to_int(offset, DEFAULT_OFFSET))
    size = max(1, _to_int(limit, DEFAULT_LIMIT))
    return rows[start:start + size]


def _row_value_for_aliases(row: Mapping[str, Any], aliases: Sequence[str]) -> Any:
    """Get value from row using aliases."""
    if not isinstance(row, Mapping):
        return None

    exact = {str(k): v for k, v in row.items()}
    lower = {str(k).lower(): v for k, v in row.items()}
    canonical = {_normalize_key(str(k)): v for k, v in row.items()}
    loose = {_normalize_key_loose(str(k)): v for k, v in row.items()}

    expanded: List[str] = []
    seen: set = set()

    for alias in aliases:
        a = _to_string(alias)
        if not a:
            continue
        candidates = [a, a.lower(), _normalize_key(a), _normalize_key_loose(a)]
        candidates.extend(_FIELD_ALIAS_HINTS.get(_normalize_key(a), []))
        for candidate in candidates:
            c = _to_string(candidate)
            if c and c not in seen:
                seen.add(c)
                expanded.append(c)

    for alias in expanded:
        if alias in exact and exact[alias] is not None:
            return exact[alias]
        if alias.lower() in lower and lower[alias.lower()] is not None:
            return lower[alias.lower()]
        nk = _normalize_key(alias)
        if nk in canonical and canonical[nk] is not None:
            return canonical[nk]
        nl = _normalize_key_loose(alias)
        if nl in loose and loose[nl] is not None:
            return loose[nl]

    return None


# =============================================================================
# Page and Mode Normalization
# =============================================================================

def _normalize_page_name(page: Any) -> str:
    """Normalize page name using page catalog, with safe fallbacks."""
    raw = _to_string(page)
    if not raw:
        return ""

    # Try page catalog modules
    for module_path in ("core.sheets.page_catalog", "core.page_catalog", "page_catalog"):
        try:
            mod = importlib.import_module(module_path)
        except ImportError:
            continue

        for fn_name in ("normalize_page_name", "normalize_page", "resolve_page_name"):
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                try:
                    result = fn(raw)
                    result_str = _to_string(result)
                    if result_str:
                        return result_str
                except Exception:
                    # page_catalog raises ValueError for forbidden pages;
                    # we don't want to silently re-admit them.
                    continue

    # Fallback to aliases
    direct = PAGE_ALIASES.get(raw.lower())
    if direct:
        return direct

    if raw in ALL_KNOWN_PAGES:
        return raw

    compact = raw.replace(" ", "_")
    compact = PAGE_ALIASES.get(compact.lower(), compact)
    return compact or raw


def _normalize_mode(value: Any) -> str:
    """Normalize advisor mode."""
    mode = _to_string(value).lower()
    if not mode or mode == "auto":
        return AdvisorMode.LIVE_QUOTES.value
    if mode in {"live", "quotes", "live_quotes", "live-quotes"}:
        return AdvisorMode.LIVE_QUOTES.value
    if mode in {"sheet", "rows", "live_sheet", "live-sheet", "sheet_rows", "sheet-rows"}:
        return AdvisorMode.LIVE_SHEET.value
    if mode in {"snapshot", "snapshots"}:
        return AdvisorMode.SNAPSHOT.value
    return mode


def _default_mode_from_env() -> str:
    """Get default mode from environment."""
    for env_name in (
        "ADVISOR_DATA_MODE",
        "AdvisorDataMode",
        "INVESTMENT_ADVISOR_MODE",
        "TFB_ADVISOR_MODE",
    ):
        value = os.getenv(env_name)
        if _to_string(value):
            return _normalize_mode(value)
    return AdvisorMode.LIVE_QUOTES.value


# =============================================================================
# Criteria Fingerprint
# =============================================================================

def _criteria_fingerprint(criteria: Mapping[str, Any]) -> str:
    """Generate fingerprint for criteria."""
    payload = {
        "page": _normalize_page_name(criteria.get("page")) or DEFAULT_PAGE,
        "mode": _normalize_mode(criteria.get("advisor_data_mode") or criteria.get("mode")),
        "symbols": sorted(_normalize_list(criteria.get("symbols") or criteria.get("tickers"))),
        "limit": max(1, _to_int(criteria.get("limit") or criteria.get("top_n"), DEFAULT_LIMIT)),
        "offset": max(0, _to_int(criteria.get("offset"), DEFAULT_OFFSET)),
    }
    encoded = _json_compact(payload)
    return hashlib.sha1(encoded.encode("utf-8")).hexdigest()


# =============================================================================
# Snapshot Cache
# =============================================================================

_SNAPSHOT_LOCK = threading.RLock()
_SNAPSHOT_STORE: Dict[str, Dict[str, Any]] = {}


def _snapshot_key(
    page: str,
    mode: str,
    criteria: Optional[Mapping[str, Any]] = None,
) -> str:
    """Generate snapshot cache key."""
    normalized_page = _normalize_page_name(page) or DEFAULT_PAGE
    normalized_mode = _normalize_mode(mode)
    if not isinstance(criteria, Mapping):
        return f"{normalized_page}::{normalized_mode}"
    return f"{normalized_page}::{normalized_mode}::{_criteria_fingerprint(criteria)}"


def _snapshot_get(
    page: str,
    mode: str,
    criteria: Optional[Mapping[str, Any]] = None,
    ttl_sec: int = DEFAULT_SNAPSHOT_TTL_SEC,
) -> Optional[Dict[str, Any]]:
    """Get snapshot from cache."""
    key = _snapshot_key(page, mode, criteria=criteria)
    now = time.time()

    with _SNAPSHOT_LOCK:
        entry = _SNAPSHOT_STORE.get(key)
        if not entry:
            return None

        created = _to_float(entry.get("ts"), 0.0)
        if ttl_sec > 0 and created > 0 and (now - created) > ttl_sec:
            _SNAPSHOT_STORE.pop(key, None)
            return None

        payload = entry.get("payload")
        if isinstance(payload, Mapping):
            return _deepcopy_json_safe(dict(payload))
        return None


def _snapshot_put(
    page: str,
    mode: str,
    criteria: Optional[Mapping[str, Any]],
    payload: Dict[str, Any],
) -> None:
    """Put snapshot in cache."""
    if not isinstance(payload, Mapping):
        return

    key = _snapshot_key(page, mode, criteria=criteria)
    with _SNAPSHOT_LOCK:
        _SNAPSHOT_STORE[key] = {
            "ts": time.time(),
            "payload": _deepcopy_json_safe(dict(payload)),
        }


def _snapshot_summary() -> Dict[str, Any]:
    """Get snapshot cache summary."""
    with _SNAPSHOT_LOCK:
        return {
            "entries": len(_SNAPSHOT_STORE),
            "keys": sorted(_SNAPSHOT_STORE.keys()),
        }


# =============================================================================
# Schema Helpers
# =============================================================================

def _extract_headers_keys_from_spec(spec: Any) -> Tuple[List[str], List[str]]:
    """Extract (headers, keys) from a SheetSpec or dict-like spec."""
    headers: List[str] = []
    keys: List[str] = []

    if spec is None:
        return headers, keys

    # SheetSpec / dataclass style: spec.columns is Iterable[ColumnSpec]
    cols = getattr(spec, "columns", None)
    if cols is None and isinstance(spec, Mapping):
        cols = spec.get("columns") or spec.get("fields")

    if isinstance(cols, (list, tuple)) and cols:
        for col in cols:
            if isinstance(col, Mapping):
                header = _to_string(
                    col.get("display_header") or col.get("header")
                    or col.get("label") or col.get("name") or col.get("key")
                )
                key = _to_string(
                    col.get("key") or col.get("field")
                    or col.get("name") or col.get("id")
                )
            else:
                header = _to_string(
                    getattr(col, "display_header",
                            getattr(col, "header",
                                    getattr(col, "label",
                                            getattr(col, "name",
                                                    getattr(col, "key", None)))))
                )
                key = _to_string(
                    getattr(col, "key",
                            getattr(col, "field",
                                    getattr(col, "name",
                                            getattr(col, "id", None))))
                )
            if not header and not key:
                continue
            if header and not key:
                key = _normalize_key(header)
            elif key and not header:
                header = _title_case_header(key)
            headers.append(header)
            keys.append(key)

        if headers and keys and len(headers) == len(keys):
            return headers, keys

    # Alternate shape: spec has explicit "headers" and "keys" lists
    if isinstance(spec, Mapping):
        raw_headers = spec.get("headers") or spec.get("display_headers")
        raw_keys = spec.get("keys") or spec.get("fields")
        if isinstance(raw_headers, list):
            headers = [_to_string(v) for v in raw_headers if _to_string(v)]
        if isinstance(raw_keys, list):
            keys = [_to_string(v) for v in raw_keys if _to_string(v)]
        if headers and not keys:
            keys = [_normalize_key(h) for h in headers]
        if keys and not headers:
            headers = [_title_case_header(k) for k in keys]

    return headers, keys


def _load_headers_keys_for_page(page: str) -> Tuple[List[str], List[str]]:
    """Load (headers, keys) for a page from the schema registry. Sync."""
    normalized = _normalize_page_name(page) or DEFAULT_PAGE

    for module_name in SCHEMA_MODULE_CANDIDATES:
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        except Exception:  # pragma: no cover
            continue

        for fn_name in SCHEMA_FUNCTION_CANDIDATES:
            fn = getattr(module, fn_name, None)
            if not callable(fn):
                continue
            for call in (
                lambda: fn(normalized),
                lambda: fn(page=normalized),
                lambda: fn(sheet=normalized),
            ):
                try:
                    result = call()
                except TypeError:
                    continue
                except Exception:
                    continue
                if inspect.isawaitable(result):
                    continue
                headers, keys = _extract_headers_keys_from_spec(result)
                if headers and keys and len(headers) == len(keys):
                    return list(headers), list(keys)

    # Canonical fallbacks (registry-aligned)
    if normalized == "Insights_Analysis":
        return list(_INSIGHTS_HEADERS), list(_INSIGHTS_KEYS)
    if normalized == "Data_Dictionary":
        return list(_DICTIONARY_HEADERS), list(_DICTIONARY_KEYS)
    if normalized == "Top_10_Investments":
        return (
            list(_GENERIC_FALLBACK_HEADERS) + list(_TOP10_EXTRA_HEADERS),
            list(_GENERIC_FALLBACK_KEYS) + list(_TOP10_EXTRA_KEYS),
        )
    return list(_GENERIC_FALLBACK_HEADERS), list(_GENERIC_FALLBACK_KEYS)


def _load_headers_for_page(page: str) -> List[str]:
    """Back-compat wrapper returning only headers."""
    headers, _ = _load_headers_keys_for_page(page)
    return headers


def _headers_to_keys(headers: List[str]) -> List[str]:
    """Derive keys from headers."""
    keys: List[str] = []
    seen: set = set()
    for idx, header in enumerate(headers):
        k = _normalize_key(header) or f"col_{idx + 1}"
        base = k
        n = 2
        while k in seen:
            k = f"{base}_{n}"
            n += 1
        seen.add(k)
        keys.append(k)
    return keys


# =============================================================================
# Payload Normalization
# =============================================================================

def _merge_payloads(*candidates: Any) -> Dict[str, Any]:
    """Merge multiple payload candidates."""
    merged: Dict[str, Any] = {}
    for candidate in candidates:
        if candidate is None:
            continue
        if isinstance(candidate, Mapping):
            merged.update(dict(candidate))
            continue
        if isinstance(candidate, str):
            txt = candidate.strip()
            if not txt:
                continue
            try:
                parsed = json.loads(txt)
                if isinstance(parsed, Mapping):
                    merged.update(dict(parsed))
                    continue
            except Exception:
                pass
        if hasattr(candidate, "model_dump") and callable(candidate.model_dump):
            try:
                dumped = candidate.model_dump(mode="python")
                if isinstance(dumped, Mapping):
                    merged.update(dict(dumped))
                    continue
            except Exception:
                pass
        if is_dataclass(candidate):
            try:
                dumped = asdict(candidate)
                if isinstance(dumped, Mapping):
                    merged.update(dict(dumped))
                    continue
            except Exception:
                pass
    return merged


def _normalize_payload(
    request: Any = None,
    body: Any = None,
    payload: Any = None,
    mode: Any = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Normalize request payload."""
    result = _merge_payloads(payload, body, kwargs)

    # Extract request ID
    try:
        request_state = getattr(request, "state", None)
        request_id = _to_string(getattr(request_state, "request_id", ""))
        if request_id and not result.get("request_id"):
            result["request_id"] = request_id
    except Exception:
        pass

    # Normalize page
    page = (
        result.get("page")
        or result.get("sheet")
        or result.get("sheet_name")
        or result.get("name")
        or result.get("tab")
        or DEFAULT_PAGE
    )
    normalized_page = _normalize_page_name(page) or DEFAULT_PAGE
    result["page"] = normalized_page
    result["sheet"] = normalized_page
    result["sheet_name"] = normalized_page

    # Normalize symbols
    symbol_values = (
        result.get("symbols")
        or result.get("tickers")
        or result.get("symbol")
        or result.get("ticker")
        or result.get("direct_symbols")
    )
    symbols = _normalize_list(symbol_values)
    if symbols:
        result["symbols"] = symbols
        result["tickers"] = list(symbols)

    # Normalize source pages
    selected_pages = _normalize_list(
        result.get("source_pages")
        or result.get("pages_selected")
        or result.get("pages")
        or result.get("sources")
    )
    if selected_pages:
        result["pages_selected"] = [_normalize_page_name(p) for p in selected_pages]
        result["source_pages"] = [
            p for p in result["pages_selected"] if p in BASE_SOURCE_PAGES
        ]

    # Normalize mode
    effective_mode = _normalize_mode(
        mode
        or result.get("advisor_data_mode")
        or result.get("data_mode")
        or result.get("advisor_mode")
        or result.get("mode")
        or _default_mode_from_env()
    )
    result["mode"] = effective_mode
    result["advisor_mode"] = effective_mode
    result["data_mode"] = effective_mode
    result["advisor_data_mode"] = effective_mode

    # Normalize limit and offset
    limit = _to_int(result.get("limit") or result.get("top_n") or DEFAULT_LIMIT, DEFAULT_LIMIT)
    limit = max(_CONFIG.min_limit, min(limit, _CONFIG.max_limit))
    offset = _to_int(result.get("offset") or DEFAULT_OFFSET, DEFAULT_OFFSET)
    offset = max(0, offset)

    result["limit"] = limit
    result["top_n"] = limit
    result["offset"] = offset

    # Flags
    result["include_matrix"] = _to_bool(result.get("include_matrix"), True)
    result["include_headers"] = _to_bool(result.get("include_headers"), True)
    result.setdefault("format", "rows")

    return result


# =============================================================================
# Tolerant Callable Execution
# =============================================================================

async def _maybe_await(value: Any) -> Any:
    """Await if value is awaitable."""
    if inspect.isawaitable(value):
        return await value
    return value


def _signature_typeerror_is_retryable(exc: TypeError) -> bool:
    """Check if TypeError is due to signature mismatch."""
    msg = _to_string(exc).lower()
    retry_markers = (
        "unexpected keyword argument",
        "got an unexpected keyword argument",
        "takes ",
        "positional argument",
        "required positional argument",
        "missing 1 required positional argument",
        "too many positional arguments",
    )
    return any(marker in msg for marker in retry_markers)


async def _call_tolerant(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Call function with multiple signature attempts."""
    attempts = [
        (args, kwargs),
        ((), kwargs),
        ((), {
            "request": kwargs.get("request"),
            "payload": kwargs.get("payload"),
            "body": kwargs.get("body"),
            "mode": kwargs.get("mode"),
        }),
        ((), {"payload": kwargs.get("payload"), "mode": kwargs.get("mode")}),
        ((), {"body": kwargs.get("body"), "mode": kwargs.get("mode")}),
        ((), {"payload": kwargs.get("payload")}),
        ((), {"body": kwargs.get("body")}),
        ((), {}),
    ]

    last_error: Optional[Exception] = None
    for call_args, call_kwargs in attempts:
        call_kwargs = {k: v for k, v in call_kwargs.items() if v is not None}
        try:
            result = fn(*call_args, **call_kwargs)
            return await _maybe_await(result)
        except TypeError as exc:
            last_error = exc
            if _signature_typeerror_is_retryable(exc):
                continue
            raise
        except Exception:
            raise

    if last_error is not None:
        raise last_error
    return None


def _run_sync(awaitable: Awaitable[Any]) -> Any:
    """Run awaitable synchronously, even from inside a running loop."""
    if not inspect.isawaitable(awaitable):
        return awaitable

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(awaitable)

    holder: Dict[str, Any] = {}

    def _runner() -> None:
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            holder["result"] = loop.run_until_complete(awaitable)
        except Exception as exc:
            holder["error"] = exc
        finally:
            try:
                loop.close()
            except Exception:
                pass
            asyncio.set_event_loop(None)

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()

    if "error" in holder:
        raise holder["error"]
    return holder.get("result")


# =============================================================================
# Engine Resolution
# =============================================================================

def _resolve_callable_from_object(obj: Any) -> Optional[Callable[..., Any]]:
    """Resolve callable from object."""
    if obj is None:
        return None
    if callable(obj):
        return obj
    for name in ENGINE_OBJECT_METHOD_CANDIDATES:
        fn = getattr(obj, name, None)
        if callable(fn):
            return fn
    return None


async def _resolve_engine_callable(
    request: Any = None,
) -> Tuple[Optional[Callable[..., Any]], Dict[str, Any]]:
    """Resolve engine callable from request or modules."""
    try:
        state = getattr(getattr(request, "app", None), "state", None)
    except Exception:
        state = None

    # Check app state first
    if state is not None:
        for name in ENGINE_FUNCTION_CANDIDATES:
            fn = getattr(state, name, None)
            if callable(fn):
                return fn, {"source": "app.state", "callable": name, "kind": "function"}

        for name in ENGINE_OBJECT_CANDIDATES:
            obj = getattr(state, name, None)
            fn = _resolve_callable_from_object(obj)
            if callable(fn):
                return fn, {
                    "source": "app.state",
                    "object": name,
                    "callable": getattr(fn, "__name__", "callable"),
                    "kind": "object_method",
                }

    # Check modules. Skip our own module to avoid recursion.
    self_names = {__name__, __name__.split(".")[-1]}
    for module_name in ENGINE_MODULE_CANDIDATES:
        if module_name in self_names:
            continue
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        except Exception:  # pragma: no cover
            continue

        for name in ENGINE_FUNCTION_CANDIDATES:
            fn = getattr(module, name, None)
            if callable(fn):
                return fn, {"source": module_name, "callable": name, "kind": "function"}

        for name in ENGINE_OBJECT_CANDIDATES:
            obj = getattr(module, name, None)
            fn = _resolve_callable_from_object(obj)
            if callable(fn):
                return fn, {
                    "source": module_name,
                    "object": name,
                    "callable": getattr(fn, "__name__", "callable"),
                    "kind": "object_method",
                }

    return None, {}


async def _execute_engine(
    request: Any,
    criteria: Dict[str, Any],
) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """Execute engine with criteria."""
    fn, meta = await _resolve_engine_callable(request)
    if not callable(fn):
        return None, meta

    try:
        result = await _call_tolerant(
            fn,
            request=request,
            payload=criteria,
            body=criteria,
            mode=criteria.get("advisor_data_mode") or criteria.get("mode"),
        )
        if isinstance(result, Mapping):
            return dict(result), meta
        if result is None:
            return None, meta
        return {"status": "success", "data": _json_safe(result)}, meta
    except Exception as exc:
        logger.warning("Investment advisor engine call failed: %s", exc, exc_info=True)
        return {
            "status": "error",
            "error": str(exc),
            "message": "Investment advisor engine execution failed",
        }, meta


# =============================================================================
# Result Normalization
# =============================================================================

def _pick_first_mapping(result: Mapping[str, Any], *keys: str) -> Optional[Mapping[str, Any]]:
    """Pick first mapping from result by keys."""
    for key in keys:
        value = result.get(key)
        if isinstance(value, Mapping):
            return value
    return None


def _extract_payload_contract(result: Mapping[str, Any]) -> Tuple[List[str], List[str]]:
    """Extract headers and keys from payload."""
    headers = [
        _to_string(v) for v in (
            result.get("display_headers")
            or result.get("headers")
            or result.get("sheet_headers")
            or []
        )
        if _to_string(v)
    ]
    keys = [
        _normalize_key(v) for v in (
            result.get("keys")
            or result.get("fields")
            or []
        )
        if _to_string(v)
    ]

    if not headers and isinstance(result.get("columns"), list):
        for idx, col in enumerate(result.get("columns") or []):
            if isinstance(col, Mapping):
                key = _normalize_key(col.get("key") or col.get("field") or col.get("name"))
                header = _to_string(
                    col.get("display_header")
                    or col.get("header")
                    or col.get("label")
                    or col.get("title")
                )
            else:
                key = ""
                header = _to_string(col)
            if not key and header:
                key = f"column_{idx + 1}"
            if not header and key:
                header = _title_case_header(key)
            if header:
                headers.append(header)
            if key:
                keys.append(key)

    if headers and not keys:
        keys = _headers_to_keys(headers)
    if keys and not headers:
        headers = [_title_case_header(k) for k in keys]

    return headers, keys


def _matrix_rows_to_dicts(matrix: Any, keys: Sequence[str]) -> List[Dict[str, Any]]:
    """Convert matrix to list of dicts."""
    result: List[Dict[str, Any]] = []
    if not isinstance(matrix, list):
        return result

    usable_keys = [str(k) for k in keys if _to_string(k)]
    if not usable_keys:
        return result

    for row in matrix:
        if isinstance(row, (list, tuple)):
            item = {
                usable_keys[idx]: row[idx] if idx < len(row) else None
                for idx in range(len(usable_keys))
            }
            result.append(item)

    return result


def _normalize_rows(result: Mapping[str, Any], keys: List[str]) -> List[Dict[str, Any]]:
    """Normalize rows from result."""
    rows: List[Dict[str, Any]] = []

    # Check common container keys
    for key in (
        "row_objects", "rowObjects", "rows", "items",
        "records", "data", "quotes", "recommendations",
    ):
        value = result.get(key)
        if isinstance(value, list) and value:
            if isinstance(value[0], Mapping):
                rows = [dict(v) for v in value if isinstance(v, Mapping)]
                break
            if isinstance(value[0], (list, tuple)):
                rows = _matrix_rows_to_dicts(value, keys)
                break

    if not rows:
        for key in ("rows_matrix", "matrix"):
            value = result.get(key)
            if isinstance(value, list) and value and isinstance(value[0], (list, tuple)):
                rows = _matrix_rows_to_dicts(value, keys)
                break

    if not rows:
        nested = _pick_first_mapping(result, "payload", "result", "response")
        if nested:
            nested_headers, nested_keys = _extract_payload_contract(dict(nested))
            rows = _normalize_rows(dict(nested), nested_keys or nested_headers or keys)

    # Strip hard-internals from each row before downstream helpers see them
    for row in rows:
        _strip_internal_fields(row, hard=False)

    return rows


def _normalize_headers_keys(
    result: Mapping[str, Any],
    page: str,
) -> Tuple[List[str], List[str], List[str]]:
    """Normalize headers and keys."""
    page = _normalize_page_name(page) or DEFAULT_PAGE

    headers = [
        _to_string(v) for v in (result.get("display_headers") or result.get("headers") or [])
        if _to_string(v)
    ]
    keys = [
        _normalize_key(v) for v in (result.get("keys") or [])
        if _to_string(v)
    ]

    if not headers or not keys or len(headers) != len(keys):
        payload_headers, payload_keys = _extract_payload_contract(result)
        headers = headers or payload_headers
        keys = keys or payload_keys

    if not headers and keys:
        headers = [_title_case_header(k) for k in keys]
    if headers and not keys:
        keys = _headers_to_keys(headers)

    # Pull the canonical (headers, keys) pair from schema_registry (sync).
    schema_headers, schema_keys = _load_headers_keys_for_page(page)

    # Special pages ALWAYS use the registry-authoritative contract
    if page in SPECIAL_PAGES:
        headers = list(schema_headers)
        keys = list(schema_keys)
    elif not headers or not keys or len(headers) != len(keys):
        headers = list(schema_headers)
        keys = list(schema_keys)

    # Safety net: if somehow we ended up on Top_10 without the 3 extras, add them.
    if page == "Top_10_Investments":
        for extra_h, extra_k in zip(_TOP10_EXTRA_HEADERS, _TOP10_EXTRA_KEYS):
            if extra_k not in keys:
                keys.append(extra_k)
                headers.append(extra_h)

    display_headers = list(headers)
    return headers, display_headers, keys


# =============================================================================
# v5.3.0 Phase C: View-aware recommendation delegation (rule-id-aware)
# =============================================================================
#
# The advisor's `_score_recommendation` historically used a local composite
# to label rows. With scoring v5.2.5 + reco_normalize v7.2.0 in place
# upstream, every real row already carries the four View tokens and the
# recommendation engine is the single source of truth.
#
# v5.3.0 cascade (preferred first):
#
#   Tier 1: recommendation_from_views_with_rule_id  (v7.2.0+)
#           Returns (reco, reason, rule_id) where rule_id identifies which
#           of 14 cascade rules fired. The rule_id is already embedded in
#           the reason where appropriate (reco_normalize v7.2.0 Phase D
#           handles conviction-badge de-dup). We use the reason as-is.
#
#   Tier 2: recommendation_from_views  (v7.1.0)
#           Called with conviction + sector_relative kwargs. v7.1.0
#           introduced conviction-floor cascading.
#
#   Tier 3: recommendation_from_views  (v7.0.0)
#           Called without conviction kwargs (TypeError fallback).
#
#   Tier 4: legacy composite-threshold scoring  (no reco_normalize)
#           Same thresholds as v5.1.0; no view veto, but ROI handling is safe.

_SCORING_SIGNAL_FIELDS: Tuple[str, ...] = (
    "overall_score", "opportunity_score", "value_score", "quality_score",
    "momentum_score", "growth_score", "valuation_score",
    "expected_roi_3m", "expected_roi_1m", "expected_roi_12m",
    "forecast_confidence", "confidence_score",
    "conviction_score",  # v5.2.0
)


def _row_has_scoring_signal(row: Mapping[str, Any]) -> bool:
    """v5.1.1: True if at least one scoring field has a real (non-None) value."""
    for f in _SCORING_SIGNAL_FIELDS:
        if _to_float_optional(row.get(f)) is not None:
            return True
    for f in _SCORING_SIGNAL_FIELDS:
        v = _row_value_for_aliases(row, _FIELD_ALIAS_HINTS.get(f, []))
        if _to_float_optional(v) is not None:
            return True
    return False


def _resolve_roi_for_scoring(row: Mapping[str, Any], horizon: str = "3m") -> Optional[float]:
    """v5.1.1: Resolve expected ROI in PERCENT for composite score."""
    pct_keys = (f"expected_roi_{horizon}_pct", f"roi_{horizon}_pct")
    for k in pct_keys:
        v = _to_float_optional(row.get(k))
        if v is not None:
            return v

    frac_keys = (f"expected_roi_{horizon}", f"roi_{horizon}", f"expected_return_{horizon}")
    for k in frac_keys:
        v = _to_float_optional(row.get(k))
        if v is not None:
            return v if abs(v) > 5.0 else (v * 100.0)
    return None


# Lazy import handles for reco_normalize. Resolved once per process so we
# don't repeatedly pay importlib overhead on every row in batch responses.
_RECO_FROM_VIEWS_FN: Any = None
_RECO_FROM_VIEWS_RESOLVED = False
# v5.3.0 Phase C: NEW rule-id-aware handle
_RECO_FROM_VIEWS_RID_FN: Any = None
_RECO_FROM_VIEWS_RID_RESOLVED = False


def _get_recommendation_from_views_fn() -> Optional[Callable[..., Tuple[str, str]]]:
    """Lazy resolve `core.reco_normalize.recommendation_from_views`.

    Returns None if the module isn't importable. Result is cached.
    Available since reco_normalize v7.0.0.
    """
    global _RECO_FROM_VIEWS_FN, _RECO_FROM_VIEWS_RESOLVED
    if _RECO_FROM_VIEWS_RESOLVED:
        return _RECO_FROM_VIEWS_FN
    _RECO_FROM_VIEWS_RESOLVED = True
    try:
        mod = importlib.import_module("core.reco_normalize")
    except ImportError:
        return None
    except Exception:  # pragma: no cover
        return None
    fn = getattr(mod, "recommendation_from_views", None)
    if callable(fn):
        _RECO_FROM_VIEWS_FN = fn
    return _RECO_FROM_VIEWS_FN


def _get_recommendation_from_views_with_rule_id_fn() -> Optional[Callable[..., Tuple[str, str, str]]]:
    """v5.3.0 Phase C: Lazy resolve the rule-id-aware function.

    Available since reco_normalize v7.2.0. Returns None on older
    installations or when reco_normalize isn't importable.
    Result is cached.
    """
    global _RECO_FROM_VIEWS_RID_FN, _RECO_FROM_VIEWS_RID_RESOLVED
    if _RECO_FROM_VIEWS_RID_RESOLVED:
        return _RECO_FROM_VIEWS_RID_FN
    _RECO_FROM_VIEWS_RID_RESOLVED = True
    try:
        mod = importlib.import_module("core.reco_normalize")
    except ImportError:
        return None
    except Exception:  # pragma: no cover
        return None
    fn = getattr(mod, "recommendation_from_views_with_rule_id", None)
    if callable(fn):
        _RECO_FROM_VIEWS_RID_FN = fn
    return _RECO_FROM_VIEWS_RID_FN


def _is_view_aware_available() -> bool:
    """Probe for the v7.0.0+ view-aware delegate. Resolves & caches if needed."""
    return _get_recommendation_from_views_fn() is not None


def _is_view_aware_rule_id_available() -> bool:
    """Probe for the v7.2.0+ rule-id-aware delegate. Resolves & caches if needed."""
    return _get_recommendation_from_views_with_rule_id_fn() is not None


def _try_view_aware_recommendation(
    row: Mapping[str, Any],
) -> Optional[Tuple[str, str, float]]:
    """v5.3.0: Try to compute a recommendation using the view-aware path.

    Returns (canonical_code, reason, overall_score) on success, or None
    when the view path doesn't apply (any of the four views missing, or
    no reco_normalize delegate available).

    Cascade (preferred first):
      Tier 1: recommendation_from_views_with_rule_id  (v7.2.0+)
      Tier 2: recommendation_from_views w/ conviction (v7.1.0+)
      Tier 3: recommendation_from_views w/o conviction (v7.0.0)
    """
    fund_view = _to_string(row.get("fundamental_view"))
    tech_view = _to_string(row.get("technical_view"))
    risk_view = _to_string(row.get("risk_view"))
    value_view = _to_string(row.get("value_view"))

    # All four views are required for the view-aware path to fire.
    if not (fund_view and tech_view and risk_view and value_view):
        return None

    overall = _to_float_optional(row.get("overall_score"))
    conviction = _to_float_optional(row.get("conviction_score"))
    sector_rel = _to_float_optional(row.get("sector_relative_score"))
    overall_for_return = overall if overall is not None else 0.0

    base_kwargs = {
        "fundamental": fund_view,
        "technical": tech_view,
        "risk": risk_view,
        "value": value_view,
        "score": overall,
    }

    # ---- Tier 1: rule-id-aware (reco_normalize v7.2.0+) ----
    fn_rid = _get_recommendation_from_views_with_rule_id_fn()
    if fn_rid is not None:
        # Try with conviction_score kwarg first (v7.2.0's signature)
        try:
            result = fn_rid(conviction_score=conviction, **base_kwargs)
        except TypeError:
            # Some installations may have the rule-id function but without
            # conviction_score in its signature; fall back to bare call.
            try:
                result = fn_rid(**base_kwargs)
            except Exception as exc:
                logger.debug(
                    "rule-id-aware reco call failed bare (%s); "
                    "falling through to legacy view-aware.", exc,
                )
                result = None
        except Exception as exc:
            logger.debug(
                "rule-id-aware reco call failed (%s); "
                "falling through to legacy view-aware.", exc,
            )
            result = None

        if isinstance(result, tuple) and len(result) >= 2:
            code = _to_string(result[0])
            reason = _to_string(result[1])
            # rule_id (result[2] if present) is already embedded in the
            # reason by reco_normalize v7.2.0 Phase D where appropriate;
            # we capture it implicitly via the reason string.
            if code:
                return code, reason, overall_for_return

    # ---- Tier 2/3: legacy recommendation_from_views (v7.0.0/v7.1.0) ----
    fn_legacy = _get_recommendation_from_views_fn()
    if fn_legacy is None:
        return None

    # Tier 2: v7.1.0 path with conviction + sector_relative
    try:
        result = fn_legacy(
            conviction=conviction,
            sector_relative=sector_rel,
            **base_kwargs,
        )
    except TypeError:
        # Tier 3: v7.0.0 path (no conviction kwargs)
        try:
            result = fn_legacy(**base_kwargs)
        except Exception:
            return None
    except Exception:
        return None

    if not isinstance(result, tuple) or len(result) < 2:
        return None
    code = _to_string(result[0])
    reason = _to_string(result[1])
    if not code:
        return None
    return code, reason, overall_for_return


def _score_recommendation(row: Mapping[str, Any]) -> Tuple[str, str, float]:
    """Score and generate a fallback recommendation.

    v5.3.0 cascade:
      1. View-aware via reco_normalize (rule-id-aware -> v7.1 -> v7.0)
      2. Legacy composite-threshold fallback (v5.1.1 ROI-safe)
    """
    # Tier 1-3: view-aware delegation
    view_aware = _try_view_aware_recommendation(row)
    if view_aware is not None:
        return view_aware

    # Tier 4: legacy composite fallback (preserved verbatim from v5.1.1)
    opportunity = _to_float(
        row.get("opportunity_score") or row.get("overall_score") or row.get("score"),
        0.0,
    )
    overall = _to_float(row.get("overall_score"), opportunity)
    risk = _to_float(row.get("risk_score"), 50.0)

    expected = _resolve_roi_for_scoring(row, horizon="3m")
    if expected is None:
        expected = _resolve_roi_for_scoring(row, horizon="1m")
    if expected is None:
        legacy = _to_float_optional(row.get("expected_roi") or row.get("forecast_return_pct"))
        expected = legacy if (legacy is None or abs(legacy) > 5.0) else (legacy * 100.0)
        if expected is None:
            expected = 0.0

    composite = overall + (0.35 * opportunity) + (0.20 * expected) - (0.25 * risk)

    if composite >= 70:
        return RECO_STRONG_BUY, "High score / attractive upside", composite
    if composite >= 55:
        return RECO_BUY, "Favorable score / acceptable risk", composite
    if composite >= 45:
        return RECO_HOLD, "Balanced score / wait for confirmation", composite
    if composite >= 30:
        return RECO_REDUCE, "Weak score / elevated risk", composite
    return RECO_SELL, "Low score / unfavorable risk-reward", composite


def _risk_bucket_from_row(row: MutableMapping[str, Any]) -> str:
    """Get risk bucket from row."""
    existing = _to_string(row.get("risk_bucket"))
    if existing:
        return existing

    risk = _to_float_optional(row.get("risk_score"))
    if risk is None:
        return ""  # v5.1.1: don't fabricate "Moderate" for missing data
    if risk < 35:
        return "Low"
    if risk < 65:
        return "Moderate"
    return "High"


def _confidence_bucket_from_row(row: MutableMapping[str, Any]) -> str:
    """Get confidence bucket from row."""
    existing = _to_string(row.get("confidence_bucket"))
    if existing:
        return existing

    score = _to_float_optional(
        row.get("confidence_score") or row.get("forecast_confidence")
        or row.get("overall_score") or row.get("opportunity_score")
    )
    if score is None:
        return ""  # v5.1.1: don't fabricate "Medium" for missing data
    if score >= 75:
        return "High"
    if score >= 50:
        return "Medium"
    return "Low"


def _ensure_top10_fields(rows: List[Dict[str, Any]], criteria: Dict[str, Any]) -> None:
    """Ensure Top10 required fields are present."""
    criteria_text = _json_compact(criteria)
    for idx, row in enumerate(rows, start=1):
        if _is_blank(row.get("top10_rank")):
            row["top10_rank"] = idx
        if _is_blank(row.get("selection_reason")):
            reco = _to_string(row.get("recommendation")) or "Candidate"
            row["selection_reason"] = f"{reco} based on live advisor scoring"
        if _is_blank(row.get("criteria_snapshot")):
            row["criteria_snapshot"] = criteria_text


def _backfill_rows(
    rows: List[Dict[str, Any]],
    page: str,
    criteria: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Backfill missing fields in rows."""
    page = _normalize_page_name(page) or DEFAULT_PAGE

    if page == "Insights_Analysis":
        return rows
    if page == "Data_Dictionary":
        return rows

    for row in rows:
        skip_sentinel = _to_bool(row.get("_skip_recommendation_synthesis"), False)

        reco = _to_string(row.get("recommendation"))
        if not reco and not skip_sentinel and _row_has_scoring_signal(row):
            reco_val, reason, composite = _score_recommendation(row)
            row["recommendation"] = reco_val
            if not _to_string(row.get("selection_reason")):
                row["selection_reason"] = reason
            if _is_blank(row.get("recommendation_reason")):
                row["recommendation_reason"] = reason
            if _is_blank(row.get("overall_score")) and not _is_blank(composite):
                row["overall_score"] = round(composite, 2)

        risk_bucket = _risk_bucket_from_row(row)
        if risk_bucket and _is_blank(row.get("risk_bucket")):
            row["risk_bucket"] = risk_bucket
        conf_bucket = _confidence_bucket_from_row(row)
        if conf_bucket and _is_blank(row.get("confidence_bucket")):
            row["confidence_bucket"] = conf_bucket

        _strip_internal_fields(row, hard=True)

    if page == "Top_10_Investments":
        _ensure_top10_fields(rows, criteria)

    return rows


def _ensure_rows_cover_keys(
    rows: List[Dict[str, Any]],
    keys: List[str],
    headers: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Ensure rows have all keys."""
    result: List[Dict[str, Any]] = []
    headers = headers or []

    for row in rows:
        normalized: Dict[str, Any] = {k: None for k in keys}
        for idx, key in enumerate(keys):
            aliases = [key, key.replace("_", " "), key.replace("_", "-")]
            if idx < len(headers):
                aliases.extend([
                    headers[idx],
                    headers[idx].replace(" ", "_"),
                    headers[idx].replace(" ", "-"),
                ])
            value = _row_value_for_aliases(row, aliases)
            normalized[key] = _json_safe(value)
        result.append(normalized)

    return result


def _build_data_dictionary_rows(criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build data dictionary rows."""
    # 1) Project-specific builder
    try:
        mod = importlib.import_module("core.sheets.data_dictionary")
        build_fn = getattr(mod, "build_data_dictionary_rows", None)
        if callable(build_fn):
            try:
                raw_rows = build_fn(include_meta_sheet=True)
            except TypeError:
                raw_rows = build_fn()
            result: List[Dict[str, Any]] = []
            for row in raw_rows if isinstance(raw_rows, list) else []:
                if isinstance(row, Mapping):
                    result.append(dict(row))
                else:
                    d = _json_safe(row)
                    if isinstance(d, Mapping):
                        result.append(dict(d))
            if result:
                return result
    except ImportError:
        pass
    except Exception:  # pragma: no cover
        pass

    # 2) Derive directly from schema_registry (authoritative)
    try:
        mod = importlib.import_module("core.sheets.schema_registry")
        registry = getattr(mod, "SCHEMA_REGISTRY", None)
        if isinstance(registry, Mapping) and registry:
            rows: List[Dict[str, Any]] = []
            for sheet_name, spec in registry.items():
                cols = getattr(spec, "columns", None)
                if cols is None and isinstance(spec, Mapping):
                    cols = spec.get("columns") or []
                if not isinstance(cols, (list, tuple)):
                    continue
                for col in cols:
                    if isinstance(col, Mapping):
                        entry = {
                            "sheet": _to_string(sheet_name),
                            "group": _to_string(col.get("group")),
                            "header": _to_string(col.get("header")),
                            "key": _to_string(col.get("key")),
                            "dtype": _to_string(col.get("dtype") or "str"),
                            "fmt": _to_string(col.get("fmt") or "text"),
                            "required": bool(col.get("required", False)),
                            "source": _to_string(col.get("source")),
                            "notes": _to_string(col.get("notes")),
                        }
                    else:
                        entry = {
                            "sheet": _to_string(sheet_name),
                            "group": _to_string(getattr(col, "group", "")),
                            "header": _to_string(getattr(col, "header", "")),
                            "key": _to_string(getattr(col, "key", "")),
                            "dtype": _to_string(getattr(col, "dtype", "str") or "str"),
                            "fmt": _to_string(getattr(col, "fmt", "text") or "text"),
                            "required": bool(getattr(col, "required", False)),
                            "source": _to_string(getattr(col, "source", "")),
                            "notes": _to_string(getattr(col, "notes", "")),
                        }
                    if entry["header"] and entry["key"]:
                        rows.append(entry)
            if rows:
                return rows
    except ImportError:
        pass
    except Exception:  # pragma: no cover
        pass

    # 3) Last-resort stub row
    return [
        {
            "sheet": criteria.get("page") or DEFAULT_PAGE,
            "group": "Advisor",
            "header": "Recommendation",
            "key": "recommendation",
            "dtype": "string",
            "fmt": None,
            "required": False,
            "source": "core.investment_advisor",
            "notes": "Fallback dictionary row generated by advisor wrapper",
        }
    ]


def _build_special_fallback(page: str, criteria: Dict[str, Any]) -> Dict[str, Any]:
    """Build fallback response for special pages (registry-aligned)."""
    page = _normalize_page_name(page) or DEFAULT_PAGE

    if page == "Insights_Analysis":
        symbols = criteria.get("symbols") or criteria.get("tickers") or []
        now_riyadh = _now_riyadh_iso()
        rows = [
            {
                "section": "Summary",
                "item": "Mode",
                "symbol": None,
                "metric": "advisor_data_mode",
                "value": criteria.get("advisor_data_mode") or criteria.get("mode") or "live_quotes",
                "notes": "Fallback insight generated by advisor wrapper",
                "last_updated_riyadh": now_riyadh,
            },
            {
                "section": "Summary",
                "item": "Universe",
                "symbol": None,
                "metric": "symbols_count",
                "value": len(symbols) if isinstance(symbols, list) else 0,
                "notes": "Symbol count from request payload",
                "last_updated_riyadh": now_riyadh,
            },
            {
                "section": "Status",
                "item": "Engine availability",
                "symbol": None,
                "metric": "warning",
                "value": "no_live_data",
                "notes": "Engine returned no usable rows; this is a fallback summary",
                "last_updated_riyadh": now_riyadh,
            },
        ]
        headers = list(_INSIGHTS_HEADERS)
        keys = list(_INSIGHTS_KEYS)
        rows = _ensure_rows_cover_keys(rows, keys, headers)
        return {
            "status": AdvisorStatus.WARN.value,
            "page": page,
            "sheet": page,
            "sheet_name": page,
            "headers": headers,
            "display_headers": headers,
            "keys": keys,
            "rows": rows,
            "row_objects": rows,
            "rows_matrix": _rows_to_matrix(rows, keys),
            "meta": {
                "source": "core.investment_advisor",
                "fallback": True,
                "reason": "engine_unavailable_or_empty",
                "advisor_data_mode_effective": criteria.get("advisor_data_mode"),
                "advisor_version": INVESTMENT_ADVISOR_VERSION,
            },
        }

    if page == "Data_Dictionary":
        headers = list(_DICTIONARY_HEADERS)
        keys = list(_DICTIONARY_KEYS)
        rows = _ensure_rows_cover_keys(_build_data_dictionary_rows(criteria), keys, headers)
        return {
            "status": AdvisorStatus.WARN.value if rows else AdvisorStatus.ERROR.value,
            "page": page,
            "sheet": page,
            "sheet_name": page,
            "headers": headers,
            "display_headers": headers,
            "keys": keys,
            "rows": rows,
            "row_objects": rows,
            "rows_matrix": _rows_to_matrix(rows, keys),
            "meta": {
                "source": "core.investment_advisor",
                "fallback": True,
                "reason": "engine_unavailable_or_empty",
                "advisor_version": INVESTMENT_ADVISOR_VERSION,
            },
        }

    # Instrument + Top_10_Investments fallback
    headers, keys = _load_headers_keys_for_page(page)
    rows: List[Dict[str, Any]] = []
    symbols = _normalize_list(criteria.get("symbols") or criteria.get("tickers"))

    fallback_warning = "Fallback row -- engine returned no live data for this symbol"

    for idx, symbol in enumerate(symbols[: criteria.get("top_n", DEFAULT_LIMIT)], start=1):
        row = {k: None for k in keys}
        if "symbol" in row:
            row["symbol"] = symbol
        if "name" in row:
            row["name"] = symbol
        # v5.1.1: do NOT default recommendation to RECO_HOLD. Leave None.
        if "data_provider" in row:
            row["data_provider"] = "advisor_fallback_no_live_data"
        if "warnings" in row:
            row["warnings"] = fallback_warning
        if "top10_rank" in row:
            row["top10_rank"] = idx
        if "selection_reason" in row:
            row["selection_reason"] = "Fallback candidate from supplied symbols (no live advisor data)"
        if "criteria_snapshot" in row:
            row["criteria_snapshot"] = _json_compact(criteria)
        rows.append(row)

    rows = _backfill_rows(rows, page, criteria)
    rows = _ensure_rows_cover_keys(rows, keys, headers)
    rows = _slice_rows(rows, criteria.get("offset", DEFAULT_OFFSET), criteria.get("limit", DEFAULT_LIMIT))

    return {
        "status": AdvisorStatus.WARN.value,
        "page": page,
        "sheet": page,
        "sheet_name": page,
        "headers": headers,
        "display_headers": headers,
        "keys": keys,
        "rows": rows,
        "row_objects": rows,
        "rows_matrix": _rows_to_matrix(rows, keys),
        "meta": {
            "source": "core.investment_advisor",
            "fallback": True,
            "reason": "engine_unavailable_or_empty",
            "advisor_data_mode_effective": criteria.get("advisor_data_mode"),
            "advisor_version": INVESTMENT_ADVISOR_VERSION,
        },
    }


def _normalize_engine_result(
    result: Optional[Mapping[str, Any]],
    criteria: Dict[str, Any],
    resolver_meta: Dict[str, Any],
) -> Dict[str, Any]:
    """Normalize engine result.

    v5.3.0 Phase H: meta exposes both `view_aware` (v7.0.0+ delegate
    availability) and `view_aware_rule_id` (v7.2.0+ rule-id-aware
    delegate availability). Lets ops dashboards detect partial v7.x
    deployments.
    """
    page = _normalize_page_name(criteria.get("page")) or DEFAULT_PAGE

    if not isinstance(result, Mapping) or not result:
        return _build_special_fallback(page, criteria)

    headers, display_headers, keys = _normalize_headers_keys(result, page)
    rows = _normalize_rows(result, keys)

    nested_meta: Dict[str, Any] = {}
    raw_meta = result.get("meta")
    if isinstance(raw_meta, Mapping):
        nested_meta = dict(raw_meta)

    if not rows and page in SPECIAL_PAGES:
        fallback = _build_special_fallback(page, criteria)
        fallback_meta = dict(fallback.get("meta") or {})
        fallback_meta.update({"engine_resolver": resolver_meta or None})
        fallback["meta"] = fallback_meta
        return fallback

    rows = _backfill_rows(rows, page, criteria)
    rows = _ensure_rows_cover_keys(rows, keys, headers)

    # Hard-strip internals one more time after backfill (defence in depth)
    for row in rows:
        _strip_internal_fields(row, hard=True)

    total_rows_before_slice = len(rows)
    offset = criteria.get("offset", DEFAULT_OFFSET)
    limit = criteria.get("limit", DEFAULT_LIMIT)
    rows = _slice_rows(rows, offset, limit)

    # Handle engine status carefully -- engine v5.60.0 emits "warn" for
    # partial-success and rows are still usable.
    raw_status = _to_string(result.get("status")).lower()
    if raw_status == "warn":
        out_status = AdvisorStatus.WARN.value if rows else AdvisorStatus.ERROR.value
    elif raw_status in {"error", "failed", "fail"} and not rows:
        out_status = AdvisorStatus.ERROR.value
    elif raw_status:
        out_status = raw_status
    else:
        out_status = AdvisorStatus.SUCCESS.value if rows else AdvisorStatus.WARN.value

    out: Dict[str, Any] = {
        "status": out_status,
        "page": page,
        "sheet": page,
        "sheet_name": page,
        "route_family": _to_string(result.get("route_family")) or "advisor",
        "headers": headers,
        "display_headers": display_headers,
        "keys": keys,
        "rows": rows,
        "row_objects": rows,
        "rows_matrix": _rows_to_matrix(rows, keys),
        "data": rows,
        "items": rows,
        "records": rows,
        "quotes": rows if page in BASE_SOURCE_PAGES else [],
        "recommendations": rows if page == "Top_10_Investments" else [],
        "meta": {
            **nested_meta,
            "source": nested_meta.get("source") or "core.investment_advisor",
            "resolver": resolver_meta or None,
            "page": page,
            "advisor_data_mode_effective": criteria.get("advisor_data_mode"),
            "normalized_by": "core.investment_advisor",
            "advisor_version": INVESTMENT_ADVISOR_VERSION,
            # v5.3.0 Phase H: reco_normalize delegate availability flags
            "view_aware": _is_view_aware_available(),
            "view_aware_rule_id": _is_view_aware_rule_id_available(),
            "timestamp_utc": _now_utc_iso(),
            "offset": max(0, _to_int(offset, DEFAULT_OFFSET)),
            "limit": max(1, _to_int(limit, DEFAULT_LIMIT)),
            "rows_before_local_slice": total_rows_before_slice,
            "rows_after_local_slice": len(rows),
            "engine_status": raw_status or None,
        },
    }

    for passthrough_key in (
        "message", "error", "warnings", "criteria",
        "criteria_snapshot", "summary", "stats",
    ):
        if passthrough_key in result and passthrough_key not in out:
            out[passthrough_key] = _json_safe(result.get(passthrough_key))

    return _json_safe(out)


# =============================================================================
# Async Implementation
# =============================================================================

async def _run_investment_advisor_async(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Async implementation of investment advisor."""
    request = kwargs.get("request")
    body = kwargs.get("body")
    payload = kwargs.get("payload")
    mode = kwargs.get("mode")

    passthrough_kwargs = dict(kwargs)
    for reserved in ("request", "body", "payload", "mode"):
        passthrough_kwargs.pop(reserved, None)

    criteria = _normalize_payload(
        request=request, body=body, payload=payload, mode=mode, **passthrough_kwargs
    )
    page = criteria["page"]
    effective_mode = criteria["advisor_data_mode"]
    ttl_sec = max(
        0,
        _to_int(
            criteria.get("snapshot_ttl")
            or os.getenv("ADVISOR_SNAPSHOT_TTL_SEC")
            or DEFAULT_SNAPSHOT_TTL_SEC,
            DEFAULT_SNAPSHOT_TTL_SEC,
        ),
    )

    # Check snapshot cache
    if effective_mode == AdvisorMode.SNAPSHOT.value and _CONFIG.enable_snapshot_cache:
        cached = _snapshot_get(page, effective_mode, criteria=criteria, ttl_sec=ttl_sec)
        if cached:
            meta = dict(cached.get("meta") or {})
            meta.update({
                "snapshot_hit": True,
                "advisor_data_mode_effective": effective_mode,
                "source": meta.get("source") or "core.investment_advisor.snapshot",
                "timestamp_utc": _now_utc_iso(),
                "snapshot_key": _snapshot_key(page, effective_mode, criteria=criteria),
            })
            cached["meta"] = meta
            cached["status"] = _to_string(cached.get("status")) or AdvisorStatus.SUCCESS.value
            return _json_safe(cached)

    # Execute engine
    engine_result, resolver_meta = await _execute_engine(request, criteria)
    normalized = _normalize_engine_result(engine_result, criteria, resolver_meta)

    # Update snapshot cache
    if _CONFIG.enable_snapshot_cache and normalized.get("rows"):
        _snapshot_put(page, effective_mode, criteria, normalized)
        if effective_mode != AdvisorMode.SNAPSHOT.value:
            _snapshot_put(page, AdvisorMode.SNAPSHOT.value, criteria, normalized)

    return normalized


# =============================================================================
# Public Service Class
# =============================================================================

class InvestmentAdvisor:
    """Investment advisor service class."""

    version = INVESTMENT_ADVISOR_VERSION

    def __call__(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.run_investment_advisor(*args, **kwargs)

    def run_investment_advisor(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return _run_sync(_run_investment_advisor_async(*args, **kwargs))

    def run_advisor(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.run_investment_advisor(*args, **kwargs)

    def execute_investment_advisor(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.run_investment_advisor(*args, **kwargs)

    def execute_advisor(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.run_investment_advisor(*args, **kwargs)

    def recommend(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.run_investment_advisor(*args, **kwargs)

    def recommend_investments(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.run_investment_advisor(*args, **kwargs)

    def get_recommendations(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.run_investment_advisor(*args, **kwargs)

    def build_recommendations(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.run_investment_advisor(*args, **kwargs)

    def warm_cache(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """Warm the cache."""
        payload = _normalize_payload(
            payload=kwargs.get("payload") or kwargs.get("body") or kwargs,
            mode=kwargs.get("mode"),
        )
        page = payload.get("page") or DEFAULT_PAGE
        result = self.run_investment_advisor(
            payload=payload,
            mode=payload.get("advisor_data_mode") or AdvisorMode.LIVE_QUOTES.value,
        )
        warmed = bool(result.get("rows"))
        return {
            "status": "ok" if warmed else "warn",
            "page": page,
            "warmed": warmed,
            "snapshot_summary": _snapshot_summary(),
            "advisor_data_mode_effective": payload.get("advisor_data_mode"),
        }

    def warm_snapshots(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """Warm snapshots for pages."""
        payload = _normalize_payload(
            payload=kwargs.get("payload") or kwargs.get("body") or kwargs,
            mode=AdvisorMode.SNAPSHOT.value,
        )
        pages = _normalize_list(
            kwargs.get("pages") or payload.get("pages")
        ) or [payload.get("page") or DEFAULT_PAGE]

        results = []
        for page in pages:
            local_payload = dict(payload)
            normalized_page = _normalize_page_name(page) or DEFAULT_PAGE
            local_payload.update({
                "page": normalized_page,
                "sheet": normalized_page,
                "sheet_name": normalized_page,
            })
            result = self.run_investment_advisor(
                payload=local_payload, mode=AdvisorMode.LIVE_QUOTES.value
            )
            if result.get("rows"):
                _snapshot_put(normalized_page, AdvisorMode.SNAPSHOT.value, local_payload, result)
            results.append({
                "page": normalized_page,
                "rows": len(result.get("rows") or []),
                "status": result.get("status") or AdvisorStatus.SUCCESS.value,
            })

        return {
            "status": "ok",
            "warmed": any(r.get("rows", 0) > 0 for r in results),
            "results": results,
            "snapshot_summary": _snapshot_summary(),
        }

    def preload_snapshots(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.warm_snapshots(*args, **kwargs)

    def build_snapshot_cache(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.warm_snapshots(*args, **kwargs)


# =============================================================================
# Singleton Exports
# =============================================================================

_SINGLETON = InvestmentAdvisor()

advisor = _SINGLETON
investment_advisor = _SINGLETON
advisor_service = _SINGLETON
investment_advisor_service = _SINGLETON
advisor_runner = _SINGLETON
investment_advisor_runner = _SINGLETON


def create_investment_advisor(*args: Any, **kwargs: Any) -> InvestmentAdvisor:
    return _SINGLETON


def get_investment_advisor(*args: Any, **kwargs: Any) -> InvestmentAdvisor:
    return _SINGLETON


def build_investment_advisor(*args: Any, **kwargs: Any) -> InvestmentAdvisor:
    return _SINGLETON


def create_advisor(*args: Any, **kwargs: Any) -> InvestmentAdvisor:
    return _SINGLETON


def get_advisor(*args: Any, **kwargs: Any) -> InvestmentAdvisor:
    return _SINGLETON


def build_advisor(*args: Any, **kwargs: Any) -> InvestmentAdvisor:
    return _SINGLETON


# =============================================================================
# Direct Function Exports
# =============================================================================

async def _run_investment_advisor_impl(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _run_investment_advisor_async(*args, **kwargs)


async def _run_advisor_impl(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await _run_investment_advisor_async(*args, **kwargs)


def run_investment_advisor(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _SINGLETON.run_investment_advisor(*args, **kwargs)


def run_advisor(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _SINGLETON.run_advisor(*args, **kwargs)


def execute_investment_advisor(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _SINGLETON.execute_investment_advisor(*args, **kwargs)


def execute_advisor(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _SINGLETON.execute_advisor(*args, **kwargs)


def recommend(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _SINGLETON.recommend(*args, **kwargs)


def recommend_investments(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _SINGLETON.recommend_investments(*args, **kwargs)


def get_recommendations(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _SINGLETON.get_recommendations(*args, **kwargs)


def build_recommendations(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return _SINGLETON.build_recommendations(*args, **kwargs)


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    "INVESTMENT_ADVISOR_VERSION",
    # v5.3.0 Phase F: __version__ alias (TFB module convention)
    "__version__",
    "InvestmentAdvisor",
    "advisor",
    "investment_advisor",
    "advisor_service",
    "investment_advisor_service",
    "advisor_runner",
    "investment_advisor_runner",
    "create_investment_advisor",
    "get_investment_advisor",
    "build_investment_advisor",
    "create_advisor",
    "get_advisor",
    "build_advisor",
    "_run_investment_advisor_impl",
    "_run_advisor_impl",
    "run_investment_advisor",
    "run_advisor",
    "execute_investment_advisor",
    "execute_advisor",
    "recommend",
    "recommend_investments",
    "get_recommendations",
    "build_recommendations",
]
