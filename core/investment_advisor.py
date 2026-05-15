#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/investment_advisor.py
================================================================================
INVESTMENT ADVISOR ORCHESTRATOR -- v5.4.0
(SCHEMA v2.8.0 / SCORING v5.3.0 / RECO v7.2.0 / ENGINE v4.4.2 / TOP10 v4.13.0
 / BUCKETS v1.0.0)
================================================================================
LIVE-BY-DEFAULT * ENGINE-FIRST * SNAPSHOT-TOLERANT * ROUTE-COMPATIBLE
MODE-AWARE * SCHEMA-SAFE * JSON-SAFE * IMPORT-SAFE * WORKER-THREAD SAFE
SPECIAL-PAGE SAFE * CONTRACT-PRESERVING * DICTIONARY-HARDENED
CORE-SCORING-DELEGATED * NO-PARALLEL-RECO-LADDER * CANONICAL-BUCKET ROUTED

================================================================================
WHY v5.4.0 -- CORE.SCORING DELEGATION / LOCAL LADDER REMOVAL
================================================================================

This release closes the THIRD parallel recommendation decision path in the TFB
stack. After data_engine_v2 v5.71.0 and core/scoring.py v5.3.0 established
`core.scoring` as the single source of truth for recommendation, risk bucket,
and confidence bucket, this file now follows that contract.

BUGS / FIXES ADDRESSED (A-I):

  A. PARALLEL RECOMMENDATION LADDER REMOVED.
     `_score_recommendation` previously cascaded through four tiers:
       Tier 1: reco_normalize rule-id-aware path (v7.2.0+)
       Tier 2: reco_normalize with conviction (v7.1.0+)
       Tier 3: reco_normalize without conviction (v7.0.0)
       Tier 4: local composite-threshold fallback
     v5.4.0 normal execution now delegates directly to
     `core.scoring._recommendation(overall, risk, confidence100, roi3)`.
     The previous 4-tier cascade is retained ONLY as emergency rollback
     behind `TFB_ADVISOR_TRUST_LOCAL_LADDER=true`. Default is false.

  B. COMPOSITE WEIGHTS AS DECLARATIVE CONSTANTS.
     The inline weights (0.35 * opportunity + 0.20 * expected - 0.25 * risk)
     are now module-level constants COMPOSITE_WEIGHT_OPPORTUNITY,
     COMPOSITE_WEIGHT_EXPECTED_RETURN, COMPOSITE_WEIGHT_RISK. Composite is
     kept as a numeric diagnostic / sorting signal -- it is NOT the
     recommendation authority.

  C. CANONICAL ENUM CLOSURE.
     All recommendation output is restricted to the 6-value
     core.scoring.RECOMMENDATION_ENUM: STRONG_BUY / BUY / HOLD / REDUCE /
     SELL / STRONG_SELL. No title-case labels. No "Avoid".

  D. BUCKET HELPERS DELEGATE TO CORE.SCORING.
     `_risk_bucket_from_row` and `_confidence_bucket_from_row` prefer
     `core.scoring._risk_bucket` and `core.scoring._confidence_bucket`.
     The v5.3.2 `core.buckets` routing remains as secondary fallback, and
     the legacy inline thresholds remain as a final fallback only when
     both canonical sources are unavailable.

  E. STRUCTURED REASON FORMAT.
     The recommendation reason text now comes from core.scoring in the
     canonical {REC}: {prose} | overall=X risk=Y conf=Z roi3m=W% format,
     so the advisor's recommendation_reason matches what data_engine_v2
     v5.71.0 emits.

  F. POSITION SIZE HINTS CENTRALIZED.
     A POSITION_SIZE_HINTS dict maps canonical enum to position hints
     in one place, replacing scattered if/elif logic.

  G. ADVISOR SCHEMA VERSION EMITTED.
     `advisor_schema_version` is emitted in both row-level and meta-level
     output so downstream cache invalidation can detect this file.

  H. DIAGNOSTIC LOGS SURFACE FAILURE MODES.
     Three new log lines make silent fallbacks visible:
       - [v5.4.0 ADVISOR] symbol=X composite=Y rec=Z source=engine|legacy|fallback
       - [v5.4.0 INSUFFICIENT] symbol=X missing=field_list
       - [v5.4.0 FALLBACK] symbol=X reason=core_scoring_unavailable|core_scoring_failed

  I. NO SILENT 50.0 FABRICATION.
     When inputs are insufficient under default TFB_ADVISOR_NULLABLE_COMPOSITE=true,
     composite returns the hotfix-safe sentinel 0.0 (not None, to preserve the
     Tuple[str, str, float] return type) and `advisor_scoring_source` is set to
     "advisor_insufficient_data" so downstream consumers see the failure mode
     explicitly. Emergency rollback (env=false) restores the legacy 50.0 path.

NEW / HONORED ENV VARS:
  - TFB_ADVISOR_TRUST_LOCAL_LADDER=false  (default: no legacy ladder)
  - TFB_ADVISOR_NULLABLE_COMPOSITE=true   (default: composite=0.0 when insufficient)

NEW MODULE CONSTANTS:
  - RECOMMENDATION_ENUM  (mirrors core.scoring on import success)
  - COMPOSITE_WEIGHT_OPPORTUNITY, COMPOSITE_WEIGHT_EXPECTED_RETURN, COMPOSITE_WEIGHT_RISK
  - POSITION_SIZE_HINTS
  - ADVISOR_SOURCE_CORE_SCORING / ADVISOR_SOURCE_INSUFFICIENT /
    ADVISOR_SOURCE_FALLBACK / ADVISOR_SOURCE_LEGACY

NEW ROW / META FIELDS (additive, safe defaults):
  - advisor_schema_version
  - advisor_scoring_source
  - advisor_scoring_warning (only when fallback fires)

NO ROW-LEVEL rule_id FIELD ADDED.
  v5.3.x already kept rule_id internal (reason-embedded only) and surfaced
  capability via meta.view_aware_rule_id. v5.4.0 avoids schema drift and
  surfaces source via advisor_scoring_source instead.

PRESERVED FROM v5.3.2 / v5.3.1 / v5.3.0:
  - All public function and class names; tuple return shape of
    _score_recommendation is (str, str, float).
  - 97-col fallback schema (Decision Matrix + Candlestick groups).
  - 100-col Top10 layout (97 + 3 extras).
  - All field aliases, special-page handlers, snapshot cache.
  - core.buckets v1.0.0 routing as secondary bucket source.
  - meta.view_aware / meta.view_aware_rule_id / meta.buckets_canonical
    capability flags.

================================================================================
v5.3.2 changes (CANONICAL-BUCKET ROUTING via core.buckets v1.0.0) [PRESERVED]
================================================================================
Routed bucket-backfill helpers through core.buckets v1.0.0 -- single
authoritative bucket helper shared with data_engine_v2 v5.69.0+ and
top10_selector v4.13.0+. Returned canonical UPPERCASE "LOW"/"MODERATE"/"HIGH"
labels with auto-scale 0-1 vs 0-100 detection. v5.4.0 keeps this routing as
secondary fallback after core.scoring.

================================================================================
v5.3.1 changes (TOP10 v4.12.0 + ENGINE v4.4.1 ALIGNMENT) [PRESERVED]
================================================================================
Metadata-only patch; sibling-version reference updates. v5.4.0 sibling
references updated to current versions.

================================================================================
v5.3.0 changes (May 2026 cross-stack family) [PRESERVED]
================================================================================
- 97-col instrument schema (Decision Matrix + Candlestick groups).
- 100-col Top10 layout.
- 4-tier view-aware reco delegation cascade (now legacy-only in v5.4.0).
- meta.view_aware / meta.view_aware_rule_id observability flags.
- _FIELD_ALIAS_HINTS extended for the 7 new schema columns.
- __version__ alias (TFB module convention).

================================================================================
v5.2.0 / v5.1.1 / v5.1.0 changes [PRESERVED VERBATIM]
================================================================================
- v5.2.0: _GENERIC_FALLBACK_KEYS rebuilt 80 -> 90; conviction_score
  first-class scoring signal; view-aware delegation introduced.
- v5.1.1: ROI-safe scoring; _skip_recommendation_synthesis sentinel;
  defensive internal-field stripping; "warn" treated as success-with-caveat.
- v5.1.0: Insights / Data_Dictionary / instrument fallbacks aligned to
  canonical schema; forbidden pages removed from ALL_KNOWN_PAGES.

Public API preserved: `InvestmentAdvisor`, `advisor` / `investment_advisor`
singletons, factory functions, sync wrappers, and
`_run_investment_advisor_impl` / `_run_advisor_impl` async hooks all resolve
under the same names.
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

INVESTMENT_ADVISOR_VERSION = "5.4.0"
# v5.4.0: core.scoring is the canonical recommendation source. Local reco
# cascade demoted to emergency rollback. __version__ alias auto-tracks.
__version__ = INVESTMENT_ADVISOR_VERSION


# =============================================================================
# v5.4.0 -- Canonical recommendation enum + composite constants
# =============================================================================

# v5.4.0 FIX: Canonical recommendation enum. After import below, this is
# overridden by core.scoring.RECOMMENDATION_ENUM so all advisor output uses
# the same enum the rest of the stack uses.
RECO_STRONG_BUY = "STRONG_BUY"
RECO_BUY = "BUY"
RECO_HOLD = "HOLD"
RECO_REDUCE = "REDUCE"
RECO_SELL = "SELL"
RECO_STRONG_SELL = "STRONG_SELL"

RECOMMENDATION_ENUM: Tuple[str, ...] = (
    RECO_STRONG_BUY,
    RECO_BUY,
    RECO_HOLD,
    RECO_REDUCE,
    RECO_SELL,
    RECO_STRONG_SELL,
)
_RECOMMENDATION_ENUM_SET = set(RECOMMENDATION_ENUM)

# v5.4.0 FIX: Composite formula weights as module-level declarative constants.
# Composite remains a diagnostic / sorting metric only; it is NOT the
# recommendation authority. Economic intent:
#   - opportunity boosts composite (favorable expected returns).
#   - expected_return (3m or 1m PCT) provides forward-looking lift.
#   - risk subtracts; high-risk rows lose composite even if score is high.
COMPOSITE_WEIGHT_OPPORTUNITY = 0.35
COMPOSITE_WEIGHT_EXPECTED_RETURN = 0.20
COMPOSITE_WEIGHT_RISK = -0.25

# v5.4.0 FIX: Position-size hint logic centralized in one dict instead of
# scattered if/elif ladders. Keys match the canonical RECOMMENDATION_ENUM.
POSITION_SIZE_HINTS: Dict[str, str] = {
    RECO_STRONG_BUY: "Core position",
    RECO_BUY: "Standard position",
    RECO_HOLD: "Maintain or trim",
    RECO_REDUCE: "Avoid / reduce",
    RECO_SELL: "Avoid / reduce",
    RECO_STRONG_SELL: "Avoid / reduce",
}

# v5.4.0 FIX: Source-attribution sentinels exposed on rows / meta so
# downstream consumers can detect WHICH path produced a recommendation
# without requiring a new rule_id schema column.
ADVISOR_SOURCE_CORE_SCORING = "core_scoring"
ADVISOR_SOURCE_INSUFFICIENT = "advisor_insufficient_data"
ADVISOR_SOURCE_FALLBACK = "advisor_fallback"
ADVISOR_SOURCE_LEGACY = "legacy_reco_normalize"


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


# v5.4.0 FIX: Emergency rollback flags. Defaults are safe: no legacy ladder,
# nullable composite returns the hotfix-safe 0.0 sentinel (not None) to
# preserve the Tuple[str, str, float] return type for existing call sites.
def _trust_local_ladder() -> bool:
    return _env_bool("TFB_ADVISOR_TRUST_LOCAL_LADDER", False)


def _nullable_composite() -> bool:
    return _env_bool("TFB_ADVISOR_NULLABLE_COMPOSITE", True)


# =============================================================================
# v5.4.0 -- Guarded core.scoring import (single source of truth)
# =============================================================================
# v5.4.0 FIX: Normal execution path delegates recommendation to
# core.scoring._recommendation. core.scoring v5.3.0 exports BOTH public
# (compute_recommendation, risk_bucket, confidence_bucket) and private
# (_recommendation, _risk_bucket, _confidence_bucket) names. We import the
# private names because they are the documented contract for data_engine_v2
# v5.71.0 -- using them keeps the advisor and the engine on the same path.
#
# Import is except-guarded so a partial deployment without core.scoring
# does not crash the advisor route. Failure mode falls back to a
# conservative HOLD with [v5.4.0 FALLBACK] log -- never fabricates a
# decision.

try:  # pragma: no cover - import availability depends on deployment package
    from core.scoring import (  # type: ignore
        _recommendation as _scoring_recommendation,
        _risk_bucket as _scoring_risk_bucket,
        _confidence_bucket as _scoring_confidence_bucket,
        RECOMMENDATION_ENUM as _SCORING_RECOMMENDATION_ENUM,
    )
    _SCORING_AVAILABLE = True
    try:
        # Mirror core.scoring's canonical enum so any local check uses the
        # same set the rest of the stack uses.
        RECOMMENDATION_ENUM = tuple(_SCORING_RECOMMENDATION_ENUM)  # type: ignore[assignment]
        _RECOMMENDATION_ENUM_SET = set(RECOMMENDATION_ENUM)
    except Exception:
        pass
except Exception:  # pragma: no cover
    _scoring_recommendation = None  # type: ignore[assignment]
    _scoring_risk_bucket = None  # type: ignore[assignment]
    _scoring_confidence_bucket = None  # type: ignore[assignment]
    _SCORING_AVAILABLE = False


# =============================================================================
# v5.3.2 -- core.buckets integration (PRESERVED as secondary bucket source)
# =============================================================================
# v5.4.0 NOTE: core.buckets remains as secondary bucket source after
# core.scoring. The except-guarded import pattern from v5.3.2 is unchanged.
# When BOTH core.scoring and core.buckets are unavailable, the legacy
# inline thresholds run as final fallback.
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
# (Preserved verbatim from v5.3.2.)
# =============================================================================

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
    # Valuation (7)
    "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio", "intrinsic_value",
    "upside_pct", "valuation_score",
    # Forecast (9)
    "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
    "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "forecast_confidence", "confidence_score", "confidence_bucket",
    # Scores (7)
    "value_score", "quality_score", "momentum_score", "growth_score",
    "overall_score", "opportunity_score", "rank_overall",
    # Views (4)
    "fundamental_view", "technical_view", "risk_view", "value_view",
    # Recommendation (4)
    "recommendation", "recommendation_reason", "horizon_days", "invest_period_label",
    # Portfolio (6)
    "position_qty", "avg_cost", "position_cost", "position_value",
    "unrealized_pl", "unrealized_pl_pct",
    # Provenance (4)
    "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",
    # Insights (5)
    "sector_relative_score", "conviction_score", "top_factors", "top_risks", "position_size_hint",
    # Decision (2) -- v5.3.0
    "recommendation_detailed", "recommendation_priority",
    # Candlestick (5) -- v5.3.0
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
    # Insights (5)
    "Sector-Adj Score", "Conviction Score", "Top Factors", "Top Risks", "Position Size Hint",
    # Decision (2)
    "Recommendation Detail", "Reco Priority",
    # Candlestick (5)
    "Candle Pattern", "Candle Signal", "Candle Strength",
    "Candle Confidence", "Recent Patterns (5D)",
]

_INSIGHTS_KEYS: List[str] = [
    "section", "item", "symbol", "metric", "value", "notes", "last_updated_riyadh",
]
_INSIGHTS_HEADERS: List[str] = [
    "Section", "Item", "Symbol", "Metric", "Value", "Notes", "Last Updated (Riyadh)",
]

_DICTIONARY_KEYS: List[str] = [
    "sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes",
]
_DICTIONARY_HEADERS: List[str] = [
    "Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes",
]

_TOP10_EXTRA_KEYS: List[str] = ["top10_rank", "selection_reason", "criteria_snapshot"]
_TOP10_EXTRA_HEADERS: List[str] = ["Top10 Rank", "Selection Reason", "Criteria Snapshot"]

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
    "pb_ratio": ["pb", "priceToBook"],
    "ps_ratio": ["ps", "priceToSalesTrailing12Months"],
    "ev_ebitda": ["ev_to_ebitda", "evToEbitda", "enterpriseToEbitda"],
    "peg_ratio": ["peg", "pegRatio"],
    "intrinsic_value": ["fairValue", "fair_value", "dcf"],
    "upside_pct": ["upsidePct", "upside", "upside_percent", "upside_to_intrinsic"],
    "risk_score": ["risk", "riskscore"],
    "valuation_score": ["valuation", "valuationscore"],
    "overall_score": ["score", "overall", "totalscore", "compositeScore"],
    "opportunity_score": ["opportunity", "opportunityscore"],
    "value_score": ["valueScore"],
    "quality_score": ["qualityScore"],
    "momentum_score": ["momentumScore"],
    "growth_score": ["growthScore"],
    "confidence_score": ["confidence", "confidence_pct", "ai_confidence", "modelConfidenceScore"],
    "forecast_confidence": ["confidence", "ai_confidence", "modelConfidence"],
    "expected_roi_1m": ["roi_1m", "expected_return_1m", "expected_roi_1m_pct"],
    "expected_roi_3m": ["roi_3m", "expected_return_3m", "expected_roi_3m_pct"],
    "expected_roi_12m": ["roi_12m", "expected_return_12m", "expected_roi_12m_pct"],
    "forecast_price_1m": ["target_price_1m", "projected_price_1m"],
    "forecast_price_3m": ["target_price_3m", "projected_price_3m", "targetMeanPrice"],
    "forecast_price_12m": ["target_price_12m", "projected_price_12m", "targetMedianPrice"],
    "fundamental_view": ["fundamentalView", "fund_view", "fundamentals_view"],
    "technical_view": ["technicalView", "tech_view"],
    "risk_view": ["riskView"],
    "value_view": ["valueView", "valuation_view"],
    "recommendation": ["signal", "rating", "action", "reco"],
    "recommendation_reason": ["rationale", "reasoning", "signal_reason", "reason", "thesis"],
    "selection_reason": ["reason", "recommendation_reason", "reco_reason"],
    "criteria_snapshot": ["criteria", "criteria_json", "snapshot"],
    "data_provider": ["provider", "source_provider", "primary_provider", "provider_primary"],
    "last_updated_utc": ["updated_at_utc", "last_updated", "timestamp_utc", "as_of_utc", "asOf"],
    "last_updated_riyadh": ["updated_at_riyadh", "as_of_riyadh", "timestamp_riyadh"],
    "warnings": ["warning", "messages", "errors", "issues"],
    "volume_ratio": ["volumeRatio"],
    "day_range_position": ["dayRangePosition"],
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
    "recommendation_detailed": [
        "recommendationDetailed", "recoDetailed",
        "detailed_recommendation", "detailedRecommendation",
        "decision_detailed",
    ],
    "recommendation_priority": [
        "recommendationPriority", "recoPriority",
        "priority", "decisionPriority", "reco_priority",
    ],
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
            continue
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
    LIVE_QUOTES = "live_quotes"
    LIVE_SHEET = "live_sheet"
    SNAPSHOT = "snapshot"
    AUTO = "auto"


class AdvisorStatus(str, Enum):
    SUCCESS = "success"
    WARN = "warn"
    ERROR = "error"


# =============================================================================
# Custom Exceptions
# =============================================================================

class InvestmentAdvisorError(Exception):
    pass


class EngineResolutionError(InvestmentAdvisorError):
    pass


class InvalidPageError(InvestmentAdvisorError):
    pass


# =============================================================================
# Configuration
# =============================================================================

@dataclass(frozen=True)
class InvestmentAdvisorConfig:
    default_page: str = DEFAULT_PAGE
    default_limit: int = DEFAULT_LIMIT
    default_offset: int = DEFAULT_OFFSET
    snapshot_ttl_sec: int = DEFAULT_SNAPSHOT_TTL_SEC
    max_limit: int = 200
    min_limit: int = 1
    enable_snapshot_cache: bool = True

    @classmethod
    def from_env(cls) -> "InvestmentAdvisorConfig":
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
# Pure Utility Functions (preserved from v5.3.2)
# =============================================================================

def _to_string(value: Any) -> str:
    if value is None:
        return ""
    try:
        s = str(value).strip()
        return "" if s.lower() in {"none", "null", "nil"} else s
    except Exception:
        return ""


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if isinstance(value, bool):
            return default
        return int(float(value))
    except Exception:
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
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
    return value is None or (isinstance(value, str) and not value.strip())


def _deduplicate_keep_order(values: Iterable[Any]) -> List[str]:
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
    try:
        return json.dumps(_json_safe(value), ensure_ascii=False, separators=(",", ":"))
    except Exception:
        try:
            return str(value)
        except Exception:
            return ""


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _now_riyadh_iso() -> str:
    try:
        if _HAS_ZONEINFO and ZoneInfo is not None:
            return datetime.now(ZoneInfo("Asia/Riyadh")).isoformat(timespec="seconds")
    except Exception:
        pass
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat(timespec="seconds")


def _deepcopy_json_safe(value: Any) -> Any:
    try:
        return deepcopy(value)
    except Exception:
        return _json_safe(value)


def _rows_to_matrix(rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Any]]:
    return [[row.get(key) for key in keys] for row in rows]


def _title_case_header(key: str) -> str:
    text = _to_string(key).replace("_", " ").replace("-", " ").strip()
    if not text:
        return ""
    return " ".join(
        part.capitalize() if part.upper() != part else part
        for part in text.split()
    )


def _normalize_key(key: str) -> str:
    return _to_string(key).strip().lower().replace(" ", "_").replace("-", "_")


def _normalize_key_loose(key: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", _to_string(key).lower())


def _slice_rows(rows: List[Dict[str, Any]], offset: int, limit: int) -> List[Dict[str, Any]]:
    start = max(0, _to_int(offset, DEFAULT_OFFSET))
    size = max(1, _to_int(limit, DEFAULT_LIMIT))
    return rows[start:start + size]


def _row_value_for_aliases(row: Mapping[str, Any], aliases: Sequence[str]) -> Any:
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
# Page and Mode Normalization (preserved from v5.3.2)
# =============================================================================

def _normalize_page_name(page: Any) -> str:
    raw = _to_string(page)
    if not raw:
        return ""

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
                    continue

    direct = PAGE_ALIASES.get(raw.lower())
    if direct:
        return direct

    if raw in ALL_KNOWN_PAGES:
        return raw

    compact = raw.replace(" ", "_")
    compact = PAGE_ALIASES.get(compact.lower(), compact)
    return compact or raw


def _normalize_mode(value: Any) -> str:
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
    if not isinstance(payload, Mapping):
        return

    key = _snapshot_key(page, mode, criteria=criteria)
    with _SNAPSHOT_LOCK:
        _SNAPSHOT_STORE[key] = {
            "ts": time.time(),
            "payload": _deepcopy_json_safe(dict(payload)),
        }


def _snapshot_summary() -> Dict[str, Any]:
    with _SNAPSHOT_LOCK:
        return {
            "entries": len(_SNAPSHOT_STORE),
            "keys": sorted(_SNAPSHOT_STORE.keys()),
        }


# =============================================================================
# Schema Helpers (preserved from v5.3.2)
# =============================================================================

def _extract_headers_keys_from_spec(spec: Any) -> Tuple[List[str], List[str]]:
    headers: List[str] = []
    keys: List[str] = []

    if spec is None:
        return headers, keys

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
    headers, _ = _load_headers_keys_for_page(page)
    return headers


def _headers_to_keys(headers: List[str]) -> List[str]:
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
# Payload Normalization (preserved from v5.3.2)
# =============================================================================

def _merge_payloads(*candidates: Any) -> Dict[str, Any]:
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
    result = _merge_payloads(payload, body, kwargs)

    try:
        request_state = getattr(request, "state", None)
        request_id = _to_string(getattr(request_state, "request_id", ""))
        if request_id and not result.get("request_id"):
            result["request_id"] = request_id
    except Exception:
        pass

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

    limit = _to_int(result.get("limit") or result.get("top_n") or DEFAULT_LIMIT, DEFAULT_LIMIT)
    limit = max(_CONFIG.min_limit, min(limit, _CONFIG.max_limit))
    offset = _to_int(result.get("offset") or DEFAULT_OFFSET, DEFAULT_OFFSET)
    offset = max(0, offset)

    result["limit"] = limit
    result["top_n"] = limit
    result["offset"] = offset

    result["include_matrix"] = _to_bool(result.get("include_matrix"), True)
    result["include_headers"] = _to_bool(result.get("include_headers"), True)
    result.setdefault("format", "rows")

    return result


# =============================================================================
# Tolerant Callable Execution (preserved from v5.3.2)
# =============================================================================

async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _signature_typeerror_is_retryable(exc: TypeError) -> bool:
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
# Engine Resolution (preserved from v5.3.2)
# =============================================================================

def _resolve_callable_from_object(obj: Any) -> Optional[Callable[..., Any]]:
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
    try:
        state = getattr(getattr(request, "app", None), "state", None)
    except Exception:
        state = None

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
# Result Normalization (preserved from v5.3.2)
# =============================================================================

def _pick_first_mapping(result: Mapping[str, Any], *keys: str) -> Optional[Mapping[str, Any]]:
    for key in keys:
        value = result.get(key)
        if isinstance(value, Mapping):
            return value
    return None


def _extract_payload_contract(result: Mapping[str, Any]) -> Tuple[List[str], List[str]]:
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
    rows: List[Dict[str, Any]] = []

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

    for row in rows:
        _strip_internal_fields(row, hard=False)

    return rows


def _normalize_headers_keys(
    result: Mapping[str, Any],
    page: str,
) -> Tuple[List[str], List[str], List[str]]:
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

    schema_headers, schema_keys = _load_headers_keys_for_page(page)

    if page in SPECIAL_PAGES:
        headers = list(schema_headers)
        keys = list(schema_keys)
    elif not headers or not keys or len(headers) != len(keys):
        headers = list(schema_headers)
        keys = list(schema_keys)

    if page == "Top_10_Investments":
        for extra_h, extra_k in zip(_TOP10_EXTRA_HEADERS, _TOP10_EXTRA_KEYS):
            if extra_k not in keys:
                keys.append(extra_k)
                headers.append(extra_h)

    display_headers = list(headers)
    return headers, display_headers, keys


# =============================================================================
# Scoring signal probes (preserved from v5.3.2)
# =============================================================================

_SCORING_SIGNAL_FIELDS: Tuple[str, ...] = (
    "overall_score", "opportunity_score", "value_score", "quality_score",
    "momentum_score", "growth_score", "valuation_score",
    "expected_roi_3m", "expected_roi_1m", "expected_roi_12m",
    "forecast_confidence", "confidence_score",
    "conviction_score",
)


def _row_has_scoring_signal(row: Mapping[str, Any]) -> bool:
    for f in _SCORING_SIGNAL_FIELDS:
        if _to_float_optional(row.get(f)) is not None:
            return True
    for f in _SCORING_SIGNAL_FIELDS:
        v = _row_value_for_aliases(row, _FIELD_ALIAS_HINTS.get(f, []))
        if _to_float_optional(v) is not None:
            return True
    return False


def _resolve_roi_for_scoring(row: Mapping[str, Any], horizon: str = "3m") -> Optional[float]:
    """Resolve expected ROI in PERCENT (preserved from v5.3.2)."""
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


def _as_roi_fraction(value: Any) -> Optional[float]:
    """v5.4.0 FIX: Convert ROI to fraction for core.scoring._recommendation.

    core.scoring._recommendation expects roi3 as a 0-1 fraction (e.g. 0.30
    for 30%). Engine rows often carry percentages (e.g. 30.0). This helper
    detects scale via the |v|>1.5 heuristic.
    """
    v = _to_float_optional(value)
    if v is None:
        return None
    return v / 100.0 if abs(v) > 1.5 else v


def _get_score(row: Mapping[str, Any], *aliases: str) -> Optional[float]:
    """Extract a numeric score from row using direct keys then alias hints."""
    for key in aliases:
        v = _to_float_optional(row.get(key))
        if v is not None:
            return v
    for key in aliases:
        hint_list = _FIELD_ALIAS_HINTS.get(_normalize_key(key))
        if not hint_list:
            continue
        v = _to_float_optional(_row_value_for_aliases(row, hint_list))
        if v is not None:
            return v
    return None


def _missing_scoring_fields(row: Mapping[str, Any]) -> str:
    """v5.4.0 FIX: List the scoring fields that are missing from a row."""
    missing: List[str] = []
    for name, key_chain in (
        ("overall_score", ("overall_score", "score", "overall", "totalscore", "compositeScore")),
        ("risk_score", ("risk_score", "risk", "riskscore")),
        ("confidence_score", ("confidence_score", "confidence", "confidence_pct", "ai_confidence", "modelConfidenceScore")),
        ("expected_roi_3m", ("expected_roi_3m", "expected_return_3m", "roi_3m")),
    ):
        if _get_score(row, *key_chain) is None:
            missing.append(name)
    return ",".join(missing) if missing else "none"


def _append_warning(row: MutableMapping[str, Any], tag: str) -> None:
    """v5.4.0 FIX: Idempotent warning appender matching the engine pattern."""
    if not isinstance(row, MutableMapping) or not tag:
        return
    existing = row.get("warnings")
    if isinstance(existing, list):
        if tag not in existing:
            existing.append(tag)
        return
    parts = [p.strip() for p in re.split(r"[;|]", _to_string(existing)) if p.strip()]
    if tag not in parts:
        parts.append(tag)
    row["warnings"] = "; ".join(parts)


# =============================================================================
# Legacy reco_normalize cascade (preserved as EMERGENCY ROLLBACK only)
# =============================================================================
#
# v5.4.0 NOTE: The reco_normalize cascade is NOT used in normal execution.
# It runs only when TFB_ADVISOR_TRUST_LOCAL_LADDER=true is explicitly set.
# Default execution delegates to core.scoring._recommendation. The lazy
# import handles and capability probes are preserved verbatim so the
# emergency rollback path still works if needed.

_RECO_FROM_VIEWS_FN: Any = None
_RECO_FROM_VIEWS_RESOLVED = False
_RECO_FROM_VIEWS_RID_FN: Any = None
_RECO_FROM_VIEWS_RID_RESOLVED = False


def _get_recommendation_from_views_fn() -> Optional[Callable[..., Tuple[str, str]]]:
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
    return _get_recommendation_from_views_fn() is not None


def _is_view_aware_rule_id_available() -> bool:
    return _get_recommendation_from_views_with_rule_id_fn() is not None


def _try_view_aware_recommendation_legacy(
    row: Mapping[str, Any],
) -> Optional[Tuple[str, str, float]]:
    """LEGACY v5.3.x reco_normalize cascade. Only called via emergency rollback."""
    fund_view = _to_string(row.get("fundamental_view"))
    tech_view = _to_string(row.get("technical_view"))
    risk_view = _to_string(row.get("risk_view"))
    value_view = _to_string(row.get("value_view"))

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

    fn_rid = _get_recommendation_from_views_with_rule_id_fn()
    if fn_rid is not None:
        try:
            result = fn_rid(conviction_score=conviction, **base_kwargs)
        except TypeError:
            try:
                result = fn_rid(**base_kwargs)
            except Exception:
                result = None
        except Exception:
            result = None

        if isinstance(result, tuple) and len(result) >= 2:
            code = _to_string(result[0])
            reason = _to_string(result[1])
            if code:
                return code, reason, overall_for_return

    fn_legacy = _get_recommendation_from_views_fn()
    if fn_legacy is None:
        return None

    try:
        result = fn_legacy(
            conviction=conviction,
            sector_relative=sector_rel,
            **base_kwargs,
        )
    except TypeError:
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


# =============================================================================
# v5.4.0 -- Composite helper, legacy ladder, row scoring metadata
# =============================================================================

def _compute_composite_score(
    overall: Optional[float],
    opportunity: Optional[float],
    risk: Optional[float],
    expected_return_pct: Optional[float],
) -> float:
    """v5.4.0 FIX: Composite is diagnostic/sorting only -- NOT recommendation authority.

    Returns the hotfix-safe sentinel 0.0 when all inputs are missing (under
    nullable=true) so the Tuple[str, str, float] return signature for
    `_score_recommendation` stays valid. Under nullable=false (emergency
    rollback), legacy 50.0 fabrication is restored.
    """
    if overall is None and opportunity is None and risk is None and expected_return_pct is None:
        return 0.0 if _nullable_composite() else 50.0
    o = overall if overall is not None else 0.0
    opp = opportunity if opportunity is not None else 0.0
    r = risk if risk is not None else 0.0
    exp = expected_return_pct if expected_return_pct is not None else 0.0
    return round(
        o
        + (COMPOSITE_WEIGHT_OPPORTUNITY * opp)
        + (COMPOSITE_WEIGHT_EXPECTED_RETURN * exp)
        + (COMPOSITE_WEIGHT_RISK * r),
        2,
    )


def _legacy_local_ladder(composite: float) -> Tuple[str, str]:
    """v5.4.0 FIX: Emergency-only v5.3.x local threshold ladder.

    This threshold ladder fires only when TFB_ADVISOR_TRUST_LOCAL_LADDER=true.
    Normal execution never reaches it -- recommendations come from
    core.scoring._recommendation.
    """
    if composite >= 70:
        return RECO_STRONG_BUY, "Legacy advisor ladder: high composite score"
    if composite >= 55:
        return RECO_BUY, "Legacy advisor ladder: favorable composite score"
    if composite >= 45:
        return RECO_HOLD, "Legacy advisor ladder: balanced composite score"
    if composite >= 30:
        return RECO_REDUCE, "Legacy advisor ladder: weak composite score"
    return RECO_SELL, "Legacy advisor ladder: unfavorable composite score"


def _set_advisor_scoring_fields(
    row: Mapping[str, Any],
    source: str,
    warning: str = "",
) -> None:
    """v5.4.0 FIX: Row-level observability without adding a new rule_id schema column.

    Surfaces:
      - advisor_schema_version: module __version__ for cache invalidation.
      - advisor_scoring_source: one of core_scoring | advisor_insufficient_data |
        advisor_fallback | legacy_reco_normalize.
      - advisor_scoring_warning: present only when a fallback fires.
    """
    if not isinstance(row, MutableMapping):
        return
    row["advisor_schema_version"] = __version__
    row["advisor_scoring_source"] = source
    if warning:
        row["advisor_scoring_warning"] = warning
        _append_warning(row, warning)


def _normalize_recommendation_code(code: Any) -> str:
    """Canonicalize a recommendation label to the closed enum."""
    raw = _to_string(code).upper().replace(" ", "_").replace("-", "_")
    if raw in _RECOMMENDATION_ENUM_SET:
        return raw
    # Common legacy / provider aliases
    alias_map = {
        "STRONG BUY": RECO_STRONG_BUY,
        "STRONGBUY": RECO_STRONG_BUY,
        "OUTPERFORM": RECO_BUY,
        "OVERWEIGHT": RECO_BUY,
        "ACCUMULATE": RECO_BUY,
        "ADD": RECO_BUY,
        "NEUTRAL": RECO_HOLD,
        "MARKET_PERFORM": RECO_HOLD,
        "MAINTAIN": RECO_HOLD,
        "WATCH": RECO_HOLD,
        "TRIM": RECO_REDUCE,
        "UNDERWEIGHT": RECO_REDUCE,
        "UNDERPERFORM": RECO_SELL,
        "AVOID": RECO_SELL,
        "EXIT": RECO_SELL,
        "STRONG SELL": RECO_STRONG_SELL,
        "STRONGSELL": RECO_STRONG_SELL,
    }
    return alias_map.get(raw, RECO_HOLD if raw == "" else raw if raw in _RECOMMENDATION_ENUM_SET else RECO_HOLD)


# =============================================================================
# v5.4.0 -- Authoritative recommendation function
# =============================================================================

def _score_recommendation(row: Mapping[str, Any]) -> Tuple[str, str, float]:
    """Return (recommendation, reason, composite) for a row.

    v5.4.0: Normal execution delegates to core.scoring._recommendation.
    The previous 4-tier reco_normalize cascade and the local composite
    threshold ladder are retained as EMERGENCY ROLLBACK only, gated by
    TFB_ADVISOR_TRUST_LOCAL_LADDER=true (default false).

    Return shape preserved as (str, str, float) for compatibility with
    every existing call site, including data_engine_v2 fallback consumers.
    """
    symbol = _to_string(row.get("symbol") or row.get("ticker") or "") or "UNKNOWN"

    # v5.4.0 FIX: Extract canonical scoring inputs.
    overall = _get_score(row, "overall_score", "score", "overall", "totalscore", "compositeScore")
    risk = _get_score(row, "risk_score", "risk", "riskscore")
    confidence100 = _get_score(
        row, "confidence_score", "confidence", "confidence_pct",
        "ai_confidence", "modelConfidenceScore",
    )
    if confidence100 is None:
        fc = _get_score(row, "forecast_confidence", "modelConfidence", "ai_confidence")
        if fc is not None:
            # forecast_confidence may be 0-1 (engine fraction) or 0-100 (legacy %)
            confidence100 = fc * 100.0 if abs(fc) <= 1.5 else fc

    roi3_fraction = _as_roi_fraction(
        row.get("expected_roi_3m")
        if row.get("expected_roi_3m") is not None
        else row.get("expected_return_3m")
    )

    # Composite uses PERCENT-scaled expected return to match legacy formula.
    expected_pct = _resolve_roi_for_scoring(row, horizon="3m")
    if expected_pct is None:
        expected_pct = _resolve_roi_for_scoring(row, horizon="1m")

    opportunity = _get_score(row, "opportunity_score", "opportunity", "opportunityscore")
    composite = _compute_composite_score(overall, opportunity, risk, expected_pct)

    # v5.4.0 FIX: Insufficient-data path produces HOLD, never fabricates a decision.
    missing = _missing_scoring_fields(row)
    insufficient = (
        overall is None and risk is None
        and confidence100 is None and roi3_fraction is None
    )
    if insufficient:
        reason = (
            "HOLD: Insufficient data to advise reliably. "
            "| overall=NA risk=NA conf=NA roi3m=NA%"
        )
        _set_advisor_scoring_fields(
            row, ADVISOR_SOURCE_INSUFFICIENT, "advisor_insufficient_data"
        )
        logger.warning(
            "[v5.4.0 INSUFFICIENT] symbol=%s missing=%s", symbol, missing
        )
        return RECO_HOLD, reason, composite

    # v5.4.0 FIX: Canonical normal path -- delegate to core.scoring.
    # No reco_normalize cascade. No local composite threshold ladder.
    if _SCORING_AVAILABLE and callable(_scoring_recommendation):
        try:
            result = _scoring_recommendation(  # type: ignore[misc]
                overall, risk, confidence100, roi3_fraction
            )
            if isinstance(result, tuple) and len(result) >= 2:
                rec = _normalize_recommendation_code(result[0])
                reason = _to_string(result[1]) or f"{rec}: core.scoring recommendation"
            else:
                rec = _normalize_recommendation_code(result)
                reason = f"{rec}: core.scoring recommendation"
            _set_advisor_scoring_fields(row, ADVISOR_SOURCE_CORE_SCORING)
            if isinstance(row, MutableMapping) and _is_blank(row.get("position_size_hint")):
                row["position_size_hint"] = POSITION_SIZE_HINTS.get(rec, "Maintain or trim")
            logger.info(
                "[v5.4.0 ADVISOR] symbol=%s composite=%s rec=%s source=engine",
                symbol, composite, rec,
            )
            return rec, reason, composite
        except Exception as exc:
            logger.warning(
                "[v5.4.0 FALLBACK] symbol=%s reason=core_scoring_failed error=%s",
                symbol, type(exc).__name__,
            )
            _set_advisor_scoring_fields(
                row, ADVISOR_SOURCE_FALLBACK, "core_scoring_failed"
            )
            if _trust_local_ladder():
                # Emergency rollback: try legacy reco_normalize cascade first, then ladder.
                legacy_view = _try_view_aware_recommendation_legacy(row)
                if legacy_view is not None:
                    rec, legacy_reason, _ = legacy_view
                    rec = _normalize_recommendation_code(rec)
                    _set_advisor_scoring_fields(
                        row, ADVISOR_SOURCE_LEGACY, "legacy_local_ladder_enabled"
                    )
                    logger.info(
                        "[v5.4.0 ADVISOR] symbol=%s composite=%s rec=%s source=legacy",
                        symbol, composite, rec,
                    )
                    return rec, legacy_reason, composite
                rec, legacy_reason = _legacy_local_ladder(composite)
                _set_advisor_scoring_fields(
                    row, ADVISOR_SOURCE_LEGACY, "legacy_local_ladder_enabled"
                )
                logger.info(
                    "[v5.4.0 ADVISOR] symbol=%s composite=%s rec=%s source=legacy",
                    symbol, composite, rec,
                )
                return rec, legacy_reason, composite
            return RECO_HOLD, "core.scoring failed; conservative HOLD applied", composite

    # v5.4.0 FIX: core.scoring import failed at module load.
    logger.warning(
        "[v5.4.0 FALLBACK] symbol=%s reason=core_scoring_unavailable", symbol
    )
    _set_advisor_scoring_fields(
        row, ADVISOR_SOURCE_FALLBACK, "core_scoring_unavailable"
    )
    if _trust_local_ladder():
        legacy_view = _try_view_aware_recommendation_legacy(row)
        if legacy_view is not None:
            rec, legacy_reason, _ = legacy_view
            rec = _normalize_recommendation_code(rec)
            _set_advisor_scoring_fields(
                row, ADVISOR_SOURCE_LEGACY, "legacy_local_ladder_enabled"
            )
            logger.info(
                "[v5.4.0 ADVISOR] symbol=%s composite=%s rec=%s source=legacy",
                symbol, composite, rec,
            )
            return rec, legacy_reason, composite
        rec, legacy_reason = _legacy_local_ladder(composite)
        _set_advisor_scoring_fields(
            row, ADVISOR_SOURCE_LEGACY, "legacy_local_ladder_enabled"
        )
        logger.info(
            "[v5.4.0 ADVISOR] symbol=%s composite=%s rec=%s source=legacy",
            symbol, composite, rec,
        )
        return rec, legacy_reason, composite

    logger.info(
        "[v5.4.0 ADVISOR] symbol=%s composite=%s rec=%s source=fallback",
        symbol, composite, RECO_HOLD,
    )
    return RECO_HOLD, "core.scoring unavailable; conservative HOLD applied", composite


# =============================================================================
# v5.4.0 -- Bucket backfill (delegates to core.scoring, fall back to core.buckets)
# =============================================================================

def _risk_bucket_from_row(row: MutableMapping[str, Any]) -> str:
    """v5.4.0 FIX: Risk bucket backfill prefers core.scoring._risk_bucket.

    Cascade:
      1. Existing row["risk_bucket"] (preferred, normalized).
      2. core.scoring._risk_bucket (v5.4.0 single source of truth).
      3. core.buckets v1.0.0 (v5.3.2 secondary fallback).
      4. Legacy inline thresholds (v5.1.1 final fallback).

    v5.1.1 contract preserved: returns "" when risk_score is missing
    -- no fabricated bucket for missing data.
    """
    existing = _to_string(row.get("risk_bucket"))
    if existing:
        # Prefer existing label but normalize via core.buckets if available.
        if _BUCKETS_AVAILABLE and _bk_normalize_risk_bucket is not None:
            try:
                return _bk_normalize_risk_bucket(existing) or existing
            except Exception:
                return existing
        return existing

    risk = _to_float_optional(row.get("risk_score"))
    if risk is None:
        return ""

    # v5.4.0 FIX: Primary source is core.scoring.
    if _SCORING_AVAILABLE and callable(_scoring_risk_bucket):
        try:
            result = _scoring_risk_bucket(risk)  # type: ignore[misc]
            label = _to_string(result).upper()
            if label:
                return label
        except Exception:
            _append_warning(row, "scoring_risk_bucket_failed")

    # Secondary: core.buckets v1.0.0.
    if _BUCKETS_AVAILABLE and _bk_risk_bucket_from_score is not None:
        try:
            canon = _bk_risk_bucket_from_score(risk)
            if canon:
                return canon
        except Exception:
            pass

    # Final: legacy v5.1.1 inline thresholds.
    if risk < 35:
        return "Low"
    if risk < 65:
        return "Moderate"
    return "High"


def _confidence_bucket_from_row(row: MutableMapping[str, Any]) -> str:
    """v5.4.0 FIX: Confidence bucket backfill prefers core.scoring._confidence_bucket.

    Cascade matches _risk_bucket_from_row. core.scoring is scale-aware
    (accepts both 0-1 and 0-100), so a forecast_confidence of 0.8 buckets
    correctly as HIGH.
    """
    existing = _to_string(row.get("confidence_bucket"))
    if existing:
        if _BUCKETS_AVAILABLE and _bk_normalize_confidence_bucket is not None:
            try:
                return _bk_normalize_confidence_bucket(existing) or existing
            except Exception:
                return existing
        return existing

    score = _to_float_optional(
        row.get("confidence_score") or row.get("forecast_confidence")
        or row.get("overall_score") or row.get("opportunity_score")
    )
    if score is None:
        return ""

    # v5.4.0 FIX: Primary source is core.scoring (scale-aware).
    if _SCORING_AVAILABLE and callable(_scoring_confidence_bucket):
        try:
            result = _scoring_confidence_bucket(score)  # type: ignore[misc]
            label = _to_string(result).upper()
            if label:
                return label
        except Exception:
            _append_warning(row, "scoring_confidence_bucket_failed")

    # Secondary: core.buckets v1.0.0.
    if _BUCKETS_AVAILABLE and _bk_confidence_bucket_from_score is not None:
        try:
            canon = _bk_confidence_bucket_from_score(score)
            if canon:
                return canon
        except Exception:
            pass

    # Final: legacy v5.1.1 inline thresholds.
    if score >= 75:
        return "High"
    if score >= 50:
        return "Medium"
    return "Low"


# =============================================================================
# Top10 + backfill (preserved from v5.3.2)
# =============================================================================

def _ensure_top10_fields(rows: List[Dict[str, Any]], criteria: Dict[str, Any]) -> None:
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
    page = _normalize_page_name(page) or DEFAULT_PAGE

    if page == "Insights_Analysis":
        return rows
    if page == "Data_Dictionary":
        return rows

    for row in rows:
        skip_sentinel = _to_bool(row.get("_skip_recommendation_synthesis"), False)

        # v5.4.0 FIX: Only synthesize a recommendation when the engine left
        # the row blank AND scoring signal is present. The synthesis path
        # itself now delegates to core.scoring through _score_recommendation.
        reco = _to_string(row.get("recommendation"))
        if not reco and not skip_sentinel and _row_has_scoring_signal(row):
            reco_val, reason, composite = _score_recommendation(row)
            row["recommendation"] = reco_val
            if not _to_string(row.get("selection_reason")):
                row["selection_reason"] = reason
            if _is_blank(row.get("recommendation_reason")):
                row["recommendation_reason"] = reason
            if _is_blank(row.get("overall_score")) and composite:
                row["overall_score"] = round(composite, 2)

        risk_bucket = _risk_bucket_from_row(row)
        if risk_bucket and _is_blank(row.get("risk_bucket")):
            row["risk_bucket"] = risk_bucket
        conf_bucket = _confidence_bucket_from_row(row)
        if conf_bucket and _is_blank(row.get("confidence_bucket")):
            row["confidence_bucket"] = conf_bucket

        # v5.4.0 FIX: Stamp schema_version on every row so downstream consumers
        # can detect this advisor version even on rows the engine populated.
        if isinstance(row, MutableMapping) and _is_blank(row.get("advisor_schema_version")):
            row["advisor_schema_version"] = __version__

        _strip_internal_fields(row, hard=True)

    if page == "Top_10_Investments":
        _ensure_top10_fields(rows, criteria)

    return rows


def _ensure_rows_cover_keys(
    rows: List[Dict[str, Any]],
    keys: List[str],
    headers: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
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
        # v5.4.0 FIX: Surface advisor source on fallback meta as well.
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
                "advisor_schema_version": __version__,
                "advisor_scoring_source": ADVISOR_SOURCE_FALLBACK,
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
                "advisor_schema_version": __version__,
            },
        }

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
        # v5.4.0 FIX: Stamp schema_version + source on fallback rows.
        row["advisor_schema_version"] = __version__
        row["advisor_scoring_source"] = ADVISOR_SOURCE_FALLBACK
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
            "advisor_schema_version": __version__,
            "advisor_scoring_source": ADVISOR_SOURCE_FALLBACK,
        },
    }


def _normalize_engine_result(
    result: Optional[Mapping[str, Any]],
    criteria: Dict[str, Any],
    resolver_meta: Dict[str, Any],
) -> Dict[str, Any]:
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

    for row in rows:
        _strip_internal_fields(row, hard=True)

    total_rows_before_slice = len(rows)
    offset = criteria.get("offset", DEFAULT_OFFSET)
    limit = criteria.get("limit", DEFAULT_LIMIT)
    rows = _slice_rows(rows, offset, limit)

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
            # v5.4.0 FIX: surface schema_version and source for cache invalidation.
            "advisor_schema_version": __version__,
            "advisor_scoring_source": (
                ADVISOR_SOURCE_CORE_SCORING if _SCORING_AVAILABLE
                else ADVISOR_SOURCE_FALLBACK
            ),
            # v5.3.0 capability flags preserved (legacy paths still probeable).
            "view_aware": _is_view_aware_available(),
            "view_aware_rule_id": _is_view_aware_rule_id_available(),
            # v5.3.2 canonical bucket availability flag preserved.
            "buckets_canonical": _BUCKETS_AVAILABLE,
            # v5.4.0 capability flag for core.scoring availability.
            "scoring_canonical": _SCORING_AVAILABLE,
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
# Async Implementation (preserved from v5.3.2)
# =============================================================================

async def _run_investment_advisor_async(*args: Any, **kwargs: Any) -> Dict[str, Any]:
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
                "advisor_schema_version": __version__,
            })
            cached["meta"] = meta
            cached["status"] = _to_string(cached.get("status")) or AdvisorStatus.SUCCESS.value
            return _json_safe(cached)

    engine_result, resolver_meta = await _execute_engine(request, criteria)
    normalized = _normalize_engine_result(engine_result, criteria, resolver_meta)

    if _CONFIG.enable_snapshot_cache and normalized.get("rows"):
        _snapshot_put(page, effective_mode, criteria, normalized)
        if effective_mode != AdvisorMode.SNAPSHOT.value:
            _snapshot_put(page, AdvisorMode.SNAPSHOT.value, criteria, normalized)

    return normalized


# =============================================================================
# Public Service Class (preserved from v5.3.2)
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
            "advisor_schema_version": __version__,
        }

    def warm_snapshots(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
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
            "advisor_schema_version": __version__,
        }

    def preload_snapshots(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.warm_snapshots(*args, **kwargs)

    def build_snapshot_cache(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.warm_snapshots(*args, **kwargs)


# =============================================================================
# Singleton Exports (preserved from v5.3.2)
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
# Direct Function Exports (preserved from v5.3.2)
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
    # Versions
    "INVESTMENT_ADVISOR_VERSION",
    "__version__",
    # v5.4.0 NEW: canonical constants surfaced for downstream introspection
    "RECOMMENDATION_ENUM",
    "RECO_STRONG_BUY",
    "RECO_BUY",
    "RECO_HOLD",
    "RECO_REDUCE",
    "RECO_SELL",
    "RECO_STRONG_SELL",
    "COMPOSITE_WEIGHT_OPPORTUNITY",
    "COMPOSITE_WEIGHT_EXPECTED_RETURN",
    "COMPOSITE_WEIGHT_RISK",
    "POSITION_SIZE_HINTS",
    "ADVISOR_SOURCE_CORE_SCORING",
    "ADVISOR_SOURCE_INSUFFICIENT",
    "ADVISOR_SOURCE_FALLBACK",
    "ADVISOR_SOURCE_LEGACY",
    # Service class + singletons (preserved)
    "InvestmentAdvisor",
    "advisor",
    "investment_advisor",
    "advisor_service",
    "investment_advisor_service",
    "advisor_runner",
    "investment_advisor_runner",
    # Factory functions (preserved)
    "create_investment_advisor",
    "get_investment_advisor",
    "build_investment_advisor",
    "create_advisor",
    "get_advisor",
    "build_advisor",
    # Async hooks (preserved)
    "_run_investment_advisor_impl",
    "_run_advisor_impl",
    # Sync wrappers (preserved)
    "run_investment_advisor",
    "run_advisor",
    "execute_investment_advisor",
    "execute_advisor",
    "recommend",
    "recommend_investments",
    "get_recommendations",
    "build_recommendations",
]
