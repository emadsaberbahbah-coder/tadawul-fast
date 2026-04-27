#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/analysis/top10_selector.py
================================================================================
Top 10 Selector -- v6.1.0
================================================================================
LIVE | SCHEMA-FIRST | ROUTE-COMPATIBLE | ENGINE-SELF-RESOLVING | JSON-SAFE
TOP10-METADATA GUARANTEED | SOURCE-PAGE SAFE | SNAPSHOT FALLBACK SAFE
SYNC+ASYNC CALLER TOLERANT | DISPLAY-HEADER TOLERANT | WRAPPER-PAYLOAD SAFE
PARTIAL-DEGRADATION SAFE | DIRECT-SYMBOLS SAFE | TIMEOUT-GUARDED
DATA-QUALITY TRANSPARENT (v6.1.0)

v6.1.0 Changes (from v6.0.0)
----------------------------
Tadawul Tracker user reported `Top_10_Investments` sheet rendering as
empty after every refresh. Root cause analysis:

  1. yfinance fails intermittently for ~30% of symbols on Render's
     egress IP (rate-limit / soft-block). The data engine emits rows
     with default scores (overall=50, confidence=55) and ROIs of 8%
     baseline -- legitimate "no signal" placeholders.

  2. v6.0.0's `passes_filters()` rejected those low-quality rows
     correctly. But `build_top10_rows_async` only relaxed filters
     when the strict pool was COMPLETELY empty (`if not selected_pool`).
     Yahoo failures were producing a handful of HIGH-quality rows but
     dozens of MODERATE/LOW rows -- the strict pool was small but
     non-empty, so the relaxation never kicked in, and the final
     `top_rows[:ctx.limit]` was 1-2 picks instead of 10. Apps Script
     interpreted the short list as "empty sheet."

  3. Even when relaxation did kick in, callers had no way to see WHICH
     picks were based on real data versus engine defaults. Users could
     accidentally trust a Top_10 row whose scores were all 50/55
     placeholders.

Fixes:
  - Filter relaxation now kicks in when `len(filtered) < ctx.limit`
    (previously: only when filtered was empty). The strict pool is
    preferred but supplemented from the relaxed pool, so HIGH picks
    still appear at the top while the sheet fills out to `limit`.

  - New `compute_data_quality_tier(row)` classifies each row as
    `HIGH` / `MODERATE` / `LOW` based on:
       * forecast confidence above the 8% baseline
       * count of real (non-None) fundamental fields
       * whether overall_score is a default (50.0 / 55.0) or real
    The tier is written into a new `data_quality_tier` column so users
    can see at a glance which picks are trustworthy.

  - `data_quality_tier` added to `TOP10_REQUIRED_FIELDS` /
    `TOP10_REQUIRED_HEADERS` so it's appended automatically by
    `_ensure_top10_contract`, regardless of whether the schema is
    loaded from `schema_registry` or from the hardcoded fallback.

  - `compute_data_quality_tier` exported in `__all__` for tests.

NOT changed (deliberate scope limit):
  - `compute_selector_score` weights and horizon buckets are unchanged.
  - Sort order is unchanged: tier is REPORTED, not used as a ranking
    primary. A LOW-tier row with a high selector score still ranks
    above a HIGH-tier row with a low score. Tier is the user's
    safeguard, not a hidden filter.
  - Engine resolution, retry helpers, schema loading are unchanged.

v6.0.0 Changes (carried over)
-----------------------------
Bug fixes:
  - min_confidence filter was broken: comparing a ratio (0.70) to a
    percent-scaled row value (70). Both sides now normalize to a ratio
    before comparison.
  - Scoring horizon buckets (<=14d / else) now match choose_horizon_roi
    (1M / 3M / 12M via criteria_model) so a 60-day horizon scores with
    the right weights for its ROI field.
  - _ENGINE_LOCK is lazy-initialized inside the resolver instead of
    being an asyncio.Lock() constructed at module import.

Alignment with criteria_model.py v3.0.0:
  - choose_horizon_roi delegates to map_days_to_horizon /
    horizon_to_expected_roi_key so the three modules in core.analysis
    agree on horizon thresholds (45 / 120 days).

Consolidation:
  - Three near-identical retry loops (~20 attempts each) in
    fetch_page_rows, fetch_page_snapshot, fetch_direct_symbol_rows
    collapsed into one helper `_try_engine_calls(engine, calls,
    timeout_sec)`. Net ~80 lines removed.
  - Magic numbers in compute_trade_setup promoted to named constants
    (_STOP_LOSS_ATR_MULT, _SQRT_TRADING_DAYS, etc.)

Dead code removed:
  - DataEngineProtocol, CANONICAL_KEY_SET, Top10SelectorError /
    EngineResolutionError / CandidateCollectionError, unused imports.

Added:
  - `build_top10_rows_async` is publicly exported for callers that want
    an async-only contract (no polymorphic return).
================================================================================
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
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# ---------------------------------------------------------------------------
# Version and Constants
# ---------------------------------------------------------------------------

TOP10_SELECTOR_VERSION = "6.1.0"
OUTPUT_PAGE = "Top_10_Investments"

DEFAULT_SOURCE_PAGES: List[str] = [
    "Market_Leaders",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
    "My_Portfolio",
]

DERIVED_OR_NON_SOURCE_PAGES: set = {
    "KSA_TADAWUL",
    "Advisor_Criteria",
    "AI_Opportunity_Report",
    "Insights_Analysis",
    "Top_10_Investments",
    "Data_Dictionary",
}

# v6.1.0: data_quality_tier added so users can see which picks are
# based on real data vs engine defaults.
TOP10_REQUIRED_FIELDS: Tuple[str, ...] = (
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
    "entry_price",
    "stop_loss_suggested",
    "take_profit_suggested",
    "risk_reward_ratio",
    "data_quality_tier",
)

TOP10_REQUIRED_HEADERS: Dict[str, str] = {
    "top10_rank": "Top 10 Rank",
    "selection_reason": "Selection Reason",
    "criteria_snapshot": "Criteria Snapshot",
    "entry_price": "Entry Price",
    "stop_loss_suggested": "Stop Loss (AI)",
    "take_profit_suggested": "Take Profit (AI)",
    "risk_reward_ratio": "Risk/Reward",
    "data_quality_tier": "Data Quality",
}

# ---------------------------------------------------------------------------
# Trade-setup constants (previously magic numbers in v5.0.0)
# ---------------------------------------------------------------------------

_SQRT_TRADING_DAYS = 15.874507866387544            # sqrt(252); annualized-vol -> daily
_STOP_LOSS_ATR_MULT = 1.5                          # stop = entry - ATR * mult
_STOP_LOSS_VOL_DAILY_MULT = 3.0                    # stop = entry - (daily_move * mult)
_STOP_LOSS_MIN_PCT = 0.025                         # floor when no ATR / vol
_STOP_LOSS_MAX_CLOSENESS = 0.98                    # never closer than 2% below entry
_TAKE_PROFIT_MIN_CLOSENESS = 1.04                  # never closer than 4% above entry
_TAKE_PROFIT_DEFAULT = 1.08                        # default 8% when no data
_TAKE_PROFIT_UPSIDE_CAP = 0.80                     # cap upside-derived TP at 80%
_DAY_RANGE_ENTRY_THRESHOLD = 0.40                  # below this, use current price as entry
_DAY_RANGE_ENTRY_DISCOUNT = 0.008                  # discount factor for higher positions

# ---------------------------------------------------------------------------
# Data-quality tier constants (v6.1.0)
# ---------------------------------------------------------------------------

# Set of values commonly used by the data engine when fundamentals are
# missing -- so the row "looks scored" but isn't actually based on
# meaningful signal. compute_data_quality_tier flags rows whose
# overall_score equals one of these as engine defaults.
_DEFAULT_OVERALL_SCORES: frozenset = frozenset({50.0, 55.0})

# Confidence thresholds (ratio scale, 0.0-1.0).
_CONFIDENCE_HIGH_TIER = 0.30          # >= 30% confidence -> HIGH eligible
_CONFIDENCE_MODERATE_TIER = 0.15      # >= 15% confidence -> MODERATE eligible

# Fundamentals checked for HIGH/MODERATE classification.
_FUNDAMENTAL_KEYS_FOR_TIER: Tuple[str, ...] = (
    "pe_ttm", "eps_ttm", "revenue_ttm", "revenue_growth_yoy",
    "gross_margin", "operating_margin", "profit_margin",
    "debt_to_equity", "free_cash_flow_ttm", "roe", "market_cap",
)

# Field counts required at each tier.
_TIER_HIGH_MIN_FUNDAMENTALS = 5
_TIER_MODERATE_MIN_FUNDAMENTALS = 3

# ---------------------------------------------------------------------------
# Schema Fallbacks (107 columns; +1 vs v6.0.0 for data_quality_tier)
# ---------------------------------------------------------------------------

DEFAULT_FALLBACK_KEYS: List[str] = [
    # Identity (8)
    "symbol", "name", "asset_class", "exchange", "currency", "country", "sector", "industry",
    # Price (11)
    "current_price", "previous_close", "open_price", "day_high", "day_low",
    "week_52_high", "week_52_low", "price_change", "percent_change",
    "week_52_position_pct", "price_change_5d",
    # Volume (6)
    "volume", "avg_volume_10d", "avg_volume_30d", "market_cap", "float_shares", "volume_ratio",
    # Fundamentals equity (13)
    "beta_5y", "pe_ttm", "pe_forward", "eps_ttm", "dividend_yield", "payout_ratio",
    "revenue_ttm", "revenue_growth_yoy", "gross_margin", "operating_margin",
    "profit_margin", "debt_to_equity", "free_cash_flow_ttm",
    # Fundamentals quality (2)
    "roe", "roa",
    # Risk (8)
    "rsi_14", "volatility_30d", "volatility_90d", "max_drawdown_1y",
    "var_95_1d", "sharpe_1y", "risk_score", "risk_bucket",
    # Technicals (4)
    "rsi_signal", "technical_score", "day_range_position", "atr_14",
    # Valuation (7)
    "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio",
    "intrinsic_value", "valuation_score", "upside_pct",
    # Forecast (9)
    "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
    "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "forecast_confidence", "confidence_score", "confidence_bucket",
    # Scores (6)
    "value_score", "quality_score", "momentum_score", "growth_score",
    "overall_score", "opportunity_score",
    # Decision (8)
    "analyst_rating", "target_price", "upside_downside_pct",
    "recommendation", "signal", "trend_1m", "trend_3m", "trend_12m",
    # Recommendation new (4)
    "short_term_signal", "recommendation_reason", "invest_period_label", "horizon_days",
    # Rank
    "rank_overall",
    # Portfolio light (6)
    "position_qty", "avg_cost", "position_cost", "position_value",
    "unrealized_pl", "unrealized_pl_pct",
    # Provenance (4)
    "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",
    # Top10 extras (3) + trade setup (4) + data quality (1, v6.1.0)
    "top10_rank", "selection_reason", "criteria_snapshot",
    "entry_price", "stop_loss_suggested", "take_profit_suggested", "risk_reward_ratio",
    "data_quality_tier",
]

DEFAULT_FALLBACK_HEADERS: List[str] = [
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
    "Current Price", "Previous Close", "Open", "Day High", "Day Low", "52W High", "52W Low",
    "Price Change", "Change %", "52W Position %", "5D Change %",
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
    "Value Score", "Quality Score", "Momentum Score", "Growth Score",
    "Overall Score", "Opportunity Score",
    "Analyst Rating", "Target Price", "Upside/Downside %",
    "Recommendation", "Signal", "Trend 1M", "Trend 3M", "Trend 12M",
    "ST Signal", "Reason", "Horizon", "Horizon Days",
    "Rank (Overall)",
    "Qty", "Avg Cost", "Position Cost", "Position Value", "Unrealized P/L", "Unrealized P/L %",
    "Data Provider", "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",
    "Top 10 Rank", "Selection Reason", "Criteria Snapshot",
    "Entry Price", "Stop Loss (AI)", "Take Profit (AI)", "Risk/Reward",
    "Data Quality",
]

# ---------------------------------------------------------------------------
# Field Aliases for Row Normalization
# ---------------------------------------------------------------------------

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
    "intrinsic_value": ("intrinsic_value",),
    "valuation_score": ("valuation_score",),
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
    "selection_reason": ("selection_reason", "selector_reason"),
    "top10_rank": ("top10_rank", "rank"),
    "criteria_snapshot": ("criteria_snapshot", "criteria_json"),
    "source_page": ("source_page", "page", "sheet", "sheet_name"),
    "price_change_5d": ("price_change_5d", "change5d", "five_day_change"),
    "volume_ratio": ("volume_ratio", "vol_ratio"),
    "roe": ("roe", "return_on_equity", "returnOnEquity"),
    "roa": ("roa", "return_on_assets", "returnOnAssets"),
    "rsi_signal": ("rsi_signal", "rsiSignal"),
    "technical_score": ("technical_score", "tech_score", "technicalScore"),
    "day_range_position": ("day_range_position", "dayRangePosition"),
    "atr_14": ("atr_14", "atr"),
    "upside_pct": ("upside_pct", "upside_percent"),
    "short_term_signal": ("short_term_signal", "st_signal", "shortTermSignal"),
    "analyst_rating": ("analyst_rating", "analyst_consensus"),
    "target_price": ("target_price", "wall_st_target"),
    "upside_downside_pct": ("upside_downside_pct", "upside_to_target_pct"),
    "signal": ("signal", "trade_signal"),
    "trend_1m": ("trend_1m",),
    "trend_3m": ("trend_3m",),
    "trend_12m": ("trend_12m",),
    "entry_price": ("entry_price", "entryPrice", "suggested_entry"),
    "stop_loss_suggested": ("stop_loss_suggested", "stopLossSuggested", "ai_stop_loss"),
    "take_profit_suggested": ("take_profit_suggested", "takeProfitSuggested", "ai_take_profit"),
    "risk_reward_ratio": ("risk_reward_ratio", "riskRewardRatio", "rr_ratio"),
    # v6.1.0
    "data_quality_tier": ("data_quality_tier", "dataQualityTier", "quality_tier"),
}

# Keys that indicate wrapper/metadata rather than data rows
WRAPPER_KEYS: set = {
    "status", "page", "sheet", "sheet_name", "route_family", "headers", "display_headers",
    "sheet_headers", "column_headers", "keys", "columns", "fields", "rows", "rows_matrix",
    "matrix", "row_objects", "records", "items", "item", "quotes", "quote", "data", "record",
    "result", "payload", "response", "output", "meta", "count", "version", "snapshot",
    "envelope", "content", "schema", "sheet_spec", "spec",
}

# ---------------------------------------------------------------------------
# Configuration (Environment Variables)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Top10Config:
    """Immutable configuration for Top10 selector (loaded once at import)."""
    engine_call_timeout_sec: float = 8.0
    page_total_timeout_sec: float = 12.0
    builder_total_timeout_sec: float = 32.0
    source_page_limit: int = 80
    hydration_symbol_cap: int = 30
    max_source_pages: int = 5
    max_limit: int = 50
    early_stop_multiplier: int = 6
    emergency_symbols: List[str] = field(default_factory=list)

    @classmethod
    def from_env(cls) -> "Top10Config":
        """Load configuration from environment variables."""
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

        emergency = [
            s for s in (x.strip() for x in os.getenv("TOP10_SELECTOR_EMERGENCY_SYMBOLS", "").replace(";", ",").split(","))
            if s
        ]

        return cls(
            engine_call_timeout_sec=_env_float("TFB_TOP10_ENGINE_CALL_TIMEOUT_SEC", 8.0),
            page_total_timeout_sec=_env_float("TFB_TOP10_PAGE_TIMEOUT_SEC", 12.0),
            builder_total_timeout_sec=_env_float("TFB_TOP10_TOTAL_TIMEOUT_SEC", 32.0),
            source_page_limit=_env_int("TOP10_SELECTOR_SOURCE_PAGE_LIMIT", 80, 10, 1000),
            hydration_symbol_cap=_env_int("TOP10_SELECTOR_HYDRATION_SYMBOL_CAP", 30, 5, 250),
            max_source_pages=_env_int("TOP10_SELECTOR_MAX_SOURCE_PAGES", 5, 1, 20),
            max_limit=_env_int("TOP10_SELECTOR_MAX_LIMIT", 50, 1, 200),
            early_stop_multiplier=_env_int("TOP10_SELECTOR_EARLY_STOP_MULTIPLIER", 6, 2, 20),
            emergency_symbols=emergency,
        )


_CONFIG = Top10Config.from_env()


# ---------------------------------------------------------------------------
# Pure Utility Functions
# ---------------------------------------------------------------------------

def _to_string(value: Any) -> str:
    """Safely convert any value to string; null-like sentinels become ''."""
    if value is None:
        return ""
    try:
        s = str(value).strip()
        return "" if s.lower() in {"none", "null", "nil"} else s
    except Exception:
        return ""


def _is_blank(value: Any) -> bool:
    """Check if value is None, empty, or whitespace-only string."""
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False


def _to_int(value: Any, default: int = 0) -> int:
    """Safely convert to int."""
    try:
        if isinstance(value, bool):
            return default
        return int(float(value))
    except (ValueError, TypeError):
        return default


def _to_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    """Safely convert to float; strips commas; handles '%'. Returns default for NaN/inf."""
    try:
        if value is None or isinstance(value, bool):
            return default
        if isinstance(value, (int, float)):
            f = float(value)
            return default if (math.isnan(f) or math.isinf(f)) else f
        s = _to_string(value).replace(",", "")
        if not s:
            return default
        if s.endswith("%"):
            f = float(s[:-1].strip()) / 100.0
        else:
            f = float(s)
        return default if (math.isnan(f) or math.isinf(f)) else f
    except (ValueError, TypeError):
        return default


def _to_ratio(value: Any, default: Optional[float] = None) -> Optional[float]:
    """Convert percent-like value to ratio (0.12 = 12%). abs > 1.5 treated as percent."""
    f = _to_float(value, default)
    if f is None:
        return default
    return f / 100.0 if abs(f) > 1.5 else f


def _to_bool(value: Any, default: bool = False) -> bool:
    """Convert various representations to boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        s = value.strip().lower()
        if s in {"1", "true", "yes", "y", "on", "enabled", "enable"}:
            return True
        if s in {"0", "false", "no", "n", "off", "disabled", "disable"}:
            return False
    if isinstance(value, (int, float)):
        return bool(int(value))
    return default


def _clamp(value: float, min_val: float, max_val: float) -> float:
    """Clamp value between min and max."""
    return max(min_val, min(value, max_val))


def _dedupe_keep_order(items: Iterable[Any]) -> List[str]:
    """Deduplicate items while preserving order; stringifies."""
    seen: set = set()
    result: List[str] = []
    for item in items:
        s = _to_string(item)
        if s and s not in seen:
            seen.add(s)
            result.append(s)
    return result


def _normalize_symbol(symbol: Any) -> str:
    """Normalize symbol to canonical format (e.g., 2222 -> 2222.SR, .SA -> .SR)."""
    s = _to_string(symbol).upper().replace(" ", "")
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1]
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    if s.isdigit():
        return f"{s}.SR"
    return s


def _looks_like_symbol(value: Any) -> bool:
    """Check if value looks like a stock symbol."""
    s = _to_string(value).upper()
    if not s or " " in s or len(s) > 24:
        return False
    return bool(re.fullmatch(r"[A-Z0-9\.\=\-\^:_/]{1,24}", s))


def _is_signature_mismatch(error: TypeError) -> bool:
    """Check if TypeError is due to function-signature mismatch (vs real bug)."""
    msg = _to_string(error).lower()
    markers = (
        "unexpected keyword argument", "positional argument", "required positional",
        "takes ", "got an unexpected keyword", "multiple values for argument",
        "missing 1 required positional", "missing required positional",
        "too many positional arguments", "not enough positional arguments",
        "keyword-only argument",
    )
    return any(marker in msg for marker in markers)


def _json_safe(value: Any) -> Any:
    """Convert value to JSON-safe format."""
    if value is None:
        return None
    if isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return None if (math.isnan(value) or math.isinf(value)) else value
    if isinstance(value, Decimal):
        try:
            f = float(value)
            return None if (math.isnan(f) or math.isinf(f)) else f
        except Exception:
            return str(value)
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    # Pydantic v2
    try:
        if hasattr(value, "model_dump") and callable(value.model_dump):
            return _json_safe(value.model_dump(mode="python"))
    except Exception:
        pass
    # Pydantic v1 (or any .dict())
    try:
        if hasattr(value, "dict") and callable(value.dict):
            return _json_safe(value.dict())
    except Exception:
        pass
    try:
        if hasattr(value, "__dict__"):
            return _json_safe(dict(value.__dict__))
    except Exception:
        pass
    return str(value)


def _compact_json(value: Any) -> str:
    """Convert value to compact JSON string."""
    try:
        return json.dumps(_json_safe(value), ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return str(value)


def _normalize_list(value: Any) -> List[str]:
    """Convert various inputs to normalized list of strings."""
    if value is None:
        return []
    if isinstance(value, str):
        items = [x.strip() for x in value.replace(";", ",").replace("\n", ",").split(",") if x.strip()]
    elif isinstance(value, (list, tuple, set)):
        items = list(value)
    else:
        items = [value]
    return _dedupe_keep_order(items)


def _safe_source_pages(pages: Sequence[str]) -> List[str]:
    """Filter and deduplicate source pages; caps at config.max_source_pages."""
    result: List[str] = []
    seen: set = set()
    for page in pages:
        s = _to_string(page)
        if not s or s in DERIVED_OR_NON_SOURCE_PAGES:
            continue
        if s not in seen:
            seen.add(s)
            result.append(s)
    return result[:_CONFIG.max_source_pages]


def _header_to_key(header: str) -> str:
    """Convert display header to internal key."""
    s = _to_string(header)
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
    # Special replacements
    key = key.replace("52w", "week_52")
    key = key.replace("week52", "week_52")
    key = key.replace("p_e_", "pe_")
    key = key.replace("p_b", "pb")
    key = key.replace("p_s", "ps")
    return key


def _compact_key(value: str) -> str:
    """Remove non-alphanumeric characters for compact fuzzy matching."""
    s = _to_string(value).lower()
    if not s:
        return ""
    s = s.replace("52w", "week52")
    return re.sub(r"[^a-z0-9]+", "", s)


def _canonical_key_variants(name: str) -> List[str]:
    """Generate variants of a key for fuzzy matching."""
    base = _to_string(name)
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
    """Build a lookup dict from a row keyed by every variant of its original keys."""
    lookup: Dict[str, Any] = {}
    for k, v in row.items():
        raw = _to_string(k)
        if not raw:
            continue
        for token in {raw, raw.lower(), _header_to_key(raw), _compact_key(raw)}:
            if token and token not in lookup:
                lookup[token] = v
    return lookup


def _extract_value_by_aliases(row: Mapping[str, Any], key: str) -> Any:
    """Extract value from row using known aliases for `key`."""
    aliases = ROW_KEY_ALIASES.get(key, (key,))
    lookup = _row_lookup(row)
    for alias in aliases:
        for token in _canonical_key_variants(alias):
            if token in lookup:
                return lookup[token]
    return None


def _count_nonblank_fields(row: Mapping[str, Any]) -> int:
    """Count non-blank fields in a row."""
    count = 0
    for value in row.values():
        if isinstance(value, str):
            if value.strip():
                count += 1
        elif value not in (None, [], {}, ()):
            count += 1
    return count


def _rows_to_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    """Convert list of row dicts to a matrix (list of lists) in `keys` order."""
    return [[_json_safe(row.get(k)) for k in keys] for row in rows]


def _coerce_mapping(obj: Any) -> Dict[str, Any]:
    """Coerce object to dictionary (pydantic v1/v2, dataclass, __dict__)."""
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
        if hasattr(obj, "model_dump") and callable(obj.model_dump):
            d = obj.model_dump(mode="python")
            if isinstance(d, Mapping):
                return dict(d)
    except Exception:
        pass
    try:
        if hasattr(obj, "dict") and callable(obj.dict):
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


def _looks_like_row(obj: Any) -> bool:
    """Check if object looks like a data row (vs. wrapper/metadata)."""
    if not isinstance(obj, Mapping) or not obj:
        return False
    row = _coerce_mapping(obj)
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
    non_meta = [k for k in row.keys() if _to_string(k) not in WRAPPER_KEYS]
    return len(non_meta) >= 4


# ---------------------------------------------------------------------------
# Schema Helpers
# ---------------------------------------------------------------------------

def _complete_schema_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    """Ensure headers and keys have the same length; fill gaps by inferring from each other."""
    raw_headers = list(headers or [])
    raw_keys = list(keys or [])
    max_len = max(len(raw_headers), len(raw_keys))
    out_headers: List[str] = []
    out_keys: List[str] = []
    for i in range(max_len):
        h = _to_string(raw_headers[i]) if i < len(raw_headers) else ""
        k = _to_string(raw_keys[i]) if i < len(raw_keys) else ""
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


def _ensure_top10_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    """Ensure Top10-required fields are appended if missing."""
    out_headers = list(headers or [])
    out_keys = list(keys or [])
    for required in TOP10_REQUIRED_FIELDS:
        if required not in out_keys:
            out_keys.append(required)
            out_headers.append(TOP10_REQUIRED_HEADERS[required])
    return _complete_schema_contract(out_headers, out_keys)


def _schema_columns_from_spec(spec: Any) -> List[Any]:
    """Extract a columns list from a schema spec (dict or object), including nested specs."""
    if spec is None:
        return []
    if isinstance(spec, dict) and len(spec) == 1 and "columns" not in spec and "fields" not in spec:
        first_val = next(iter(spec.values()))
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
    return []


def _schema_keys_headers_from_spec(spec: Any) -> Tuple[List[str], List[str]]:
    """Extract (headers, keys) from a schema spec."""
    if isinstance(spec, dict) and len(spec) == 1 and not any(k in spec for k in ("columns", "fields", "headers", "keys")):
        first_val = next(iter(spec.values()))
        if isinstance(first_val, dict):
            spec = first_val

    headers: List[str] = []
    keys: List[str] = []
    cols = _schema_columns_from_spec(spec)

    for col in cols:
        if isinstance(col, Mapping):
            h = _to_string(col.get("header") or col.get("display_header") or col.get("label") or col.get("title"))
            k = _to_string(col.get("key") or col.get("field") or col.get("name") or col.get("id"))
        else:
            h = _to_string(getattr(col, "header", getattr(col, "display_header", getattr(col, "label", getattr(col, "title", None)))))
            k = _to_string(getattr(col, "key", getattr(col, "field", getattr(col, "name", getattr(col, "id", None)))))
        if h or k:
            headers.append(h or k.replace("_", " ").title())
            keys.append(k or _header_to_key(h))

    # Fallback to direct headers/keys fields on the spec
    if not headers and not keys and isinstance(spec, Mapping):
        headers2 = spec.get("headers") or spec.get("display_headers")
        keys2 = spec.get("keys") or spec.get("columns") or spec.get("fields")
        if isinstance(headers2, list):
            headers = [_to_string(x) for x in headers2 if _to_string(x)]
        if isinstance(keys2, list):
            keys = [_to_string(x) for x in keys2 if _to_string(x)]

    return _complete_schema_contract(headers, keys)


def _load_schema_defaults() -> Tuple[List[str], List[str]]:
    """Load schema from registry with Top10 contract, or fall back to hardcoded defaults."""
    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore
        spec = get_sheet_spec(OUTPUT_PAGE)
        headers, keys = _schema_keys_headers_from_spec(spec)
        if keys:
            headers, keys = _ensure_top10_contract(headers, keys)
            return list(headers), list(keys)
    except Exception as exc:
        logger.debug("Failed to load schema from registry: %s", exc)

    headers, keys = _ensure_top10_contract(DEFAULT_FALLBACK_HEADERS, DEFAULT_FALLBACK_KEYS)
    return list(headers), list(keys)


# ---------------------------------------------------------------------------
# Criteria Context
# ---------------------------------------------------------------------------

@dataclass
class CriteriaContext:
    """Normalized criteria context for ranking."""
    pages_selected: List[str] = field(default_factory=list)
    direct_symbols: List[str] = field(default_factory=list)
    direct_symbol_order: List[str] = field(default_factory=list)
    emergency_symbols: List[str] = field(default_factory=list)
    limit: int = 10
    horizon_days: int = 90
    risk_level: str = ""
    confidence_bucket: str = ""
    min_expected_roi: Optional[float] = None        # stored as ratio
    min_confidence: Optional[float] = None          # stored as ratio (fixed in v6.0.0)
    max_risk_score: Optional[float] = None          # 0-100
    min_volume: Optional[float] = None
    min_technical_score: Optional[float] = None
    short_term_signal_required: str = ""
    min_upside_pct: Optional[float] = None          # ratio
    enrich_final: bool = True
    schema_only: bool = False
    headers_only: bool = False
    include_headers: bool = True
    include_matrix: bool = True
    mode: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_inputs(cls, *args: Any, **kwargs: Any) -> "CriteriaContext":
        """Build CriteriaContext from arbitrary call-site arguments."""
        mappings: List[Dict[str, Any]] = [dict(a) for a in args if isinstance(a, Mapping)]
        body: Dict[str, Any] = {}
        for mapping in mappings:
            body.update(mapping)
        for key in ("body", "payload", "request_data", "params"):
            if isinstance(kwargs.get(key), Mapping):
                body.update(dict(kwargs[key]))

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

        # Direct kwargs override
        for k in ("pages_selected", "pages", "selected_pages", "sources", "page", "page_name", "sheet", "sheet_name"):
            if kwargs.get(k) is not None:
                criteria[k] = kwargs[k]
        for k in ("symbols", "tickers", "direct_symbols"):
            if kwargs.get(k) is not None:
                criteria[k] = kwargs[k]
        for k in ("limit", "top_n", "risk_level", "confidence_bucket", "horizon_days", "invest_period_days", "mode"):
            if kwargs.get(k) is not None:
                criteria[k] = kwargs[k]

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
        pages = _safe_source_pages(pages) or _safe_source_pages(DEFAULT_SOURCE_PAGES)

        direct_symbols = _normalize_list(
            criteria.get("direct_symbols") or criteria.get("symbols") or criteria.get("tickers")
        )
        direct_symbols = [_normalize_symbol(s) for s in direct_symbols if _normalize_symbol(s)]

        emergency_symbols = _normalize_list(criteria.get("emergency_symbols")) or list(_CONFIG.emergency_symbols)
        emergency_symbols = [_normalize_symbol(s) for s in emergency_symbols if _normalize_symbol(s)]

        limit_int = int(_clamp(
            float(_to_int(criteria.get("limit") or criteria.get("top_n") or kwargs.get("limit"), 10)),
            1.0, float(_CONFIG.max_limit),
        ))

        horizon_days = _to_int(
            criteria.get("horizon_days")
            or criteria.get("invest_period_days")
            or criteria.get("investment_period_days"),
            90,
        )

        return cls(
            pages_selected=pages,
            direct_symbols=direct_symbols,
            direct_symbol_order=list(direct_symbols),
            emergency_symbols=emergency_symbols,
            limit=limit_int,
            horizon_days=horizon_days,
            risk_level=_to_string(criteria.get("risk_level") or criteria.get("risk_profile")),
            confidence_bucket=_to_string(criteria.get("confidence_bucket") or criteria.get("confidence_level")),
            min_expected_roi=_to_ratio(criteria.get("min_expected_roi") or criteria.get("min_roi")),
            # v6.0.0 bug fix: normalize to RATIO to match row-side normalization
            min_confidence=_to_ratio(criteria.get("min_confidence") or criteria.get("min_ai_confidence")),
            max_risk_score=_to_float(criteria.get("max_risk_score")),
            min_volume=_to_float(criteria.get("min_volume")),
            min_technical_score=_to_float(criteria.get("min_technical_score")),
            short_term_signal_required=_to_string(
                criteria.get("short_term_signal_required") or criteria.get("min_short_term_signal")
            ).upper(),
            min_upside_pct=_to_ratio(criteria.get("min_upside_pct")),
            enrich_final=_to_bool(criteria.get("enrich_final", True), True),
            schema_only=_to_bool(criteria.get("schema_only"), False),
            headers_only=_to_bool(criteria.get("headers_only"), False),
            include_headers=_to_bool(criteria.get("include_headers", True), True),
            include_matrix=_to_bool(criteria.get("include_matrix", True), True),
            mode=_to_string(kwargs.get("mode") or criteria.get("mode") or ""),
            raw=criteria,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization into `criteria_snapshot`."""
        return {
            "pages_selected": self.pages_selected,
            "direct_symbols": self.direct_symbols,
            "limit": self.limit,
            "horizon_days": self.horizon_days,
            "risk_level": self.risk_level,
            "confidence_bucket": self.confidence_bucket,
            "min_expected_roi": self.min_expected_roi,
            "min_confidence": self.min_confidence,
            "max_risk_score": self.max_risk_score,
            "min_volume": self.min_volume,
            "min_technical_score": self.min_technical_score,
            "short_term_signal_required": self.short_term_signal_required,
            "min_upside_pct": self.min_upside_pct,
            "enrich_final": self.enrich_final,
            "mode": self.mode,
        }


# ---------------------------------------------------------------------------
# Row Normalization
# ---------------------------------------------------------------------------

def normalize_candidate_row(raw: Mapping[str, Any]) -> Dict[str, Any]:
    """Normalize a candidate row by resolving aliases and canonicalizing the symbol."""
    row = dict(raw)
    for key in ROW_KEY_ALIASES.keys():
        if key not in row or row.get(key) is None or row.get(key) == "":
            value = _extract_value_by_aliases(row, key)
            if value is not None:
                row[key] = value
    sym = _normalize_symbol(
        row.get("symbol") or row.get("ticker") or row.get("requested_symbol") or row.get("code")
    )
    if sym:
        row["symbol"] = sym
        row.setdefault("ticker", sym)
    return row


def merge_rows(base: Mapping[str, Any], update: Mapping[str, Any]) -> Dict[str, Any]:
    """Merge two rows, preferring non-blank values from update."""
    merged = dict(base)
    for key, value in dict(update).items():
        if value not in (None, "", [], {}, ()):
            merged[key] = value
    return merged


def merge_symbol_rows(
    primary: Sequence[Mapping[str, Any]],
    secondary: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Merge two lists of rows by symbol, keeping richer rows."""
    merged: Dict[str, Dict[str, Any]] = {}

    def _add(row: Mapping[str, Any]) -> None:
        normalized = normalize_candidate_row(row)
        sym = _normalize_symbol(normalized.get("symbol") or normalized.get("ticker"))
        if not sym:
            return
        existing = merged.get(sym)
        if existing is None:
            merged[sym] = dict(normalized)
            return
        if _count_nonblank_fields(normalized) >= _count_nonblank_fields(existing):
            merged[sym] = merge_rows(existing, normalized)
        else:
            merged[sym] = merge_rows(normalized, existing)

    for row in primary:
        _add(row)
    for row in secondary:
        _add(row)
    return list(merged.values())


# ---------------------------------------------------------------------------
# Horizon Helpers (aligned with criteria_model.py)
# ---------------------------------------------------------------------------

def _horizon_label_for_days(days: int) -> str:
    """Return 1M / 3M / 12M using criteria_model thresholds (45 / 120)."""
    try:
        from core.analysis.criteria_model import map_days_to_horizon  # type: ignore
        return map_days_to_horizon(days)
    except Exception:
        d = max(1, int(days) if days else 1)
        if d <= 45:
            return "1M"
        if d <= 120:
            return "3M"
        return "12M"


def _primary_roi_key_for_horizon(horizon: str) -> str:
    """Return the expected_roi_* field for a horizon label."""
    try:
        from core.analysis.criteria_model import horizon_to_expected_roi_key  # type: ignore
        return horizon_to_expected_roi_key(horizon)
    except Exception:
        return {"1M": "expected_roi_1m", "3M": "expected_roi_3m"}.get(horizon, "expected_roi_12m")


def choose_horizon_roi(row: Mapping[str, Any], horizon_days: int) -> Optional[float]:
    """
    Choose the appropriate ROI field based on the horizon, with fallbacks.

    Uses `criteria_model.map_days_to_horizon` / `horizon_to_expected_roi_key`
    so horizons line up across the core.analysis package.
    """
    label = _to_string(row.get("horizon_label") or row.get("invest_period_label")).upper()
    if label in ("1M", "3M", "12M"):
        primary_horizon = label
    else:
        primary_horizon = _horizon_label_for_days(horizon_days)

    primary_key = _primary_roi_key_for_horizon(primary_horizon)
    primary = _to_ratio(row.get(primary_key))
    if primary is not None:
        return primary

    # Fall through the other horizons (3M -> 1M -> 12M preference)
    for fallback_key in ("expected_roi_3m", "expected_roi_1m", "expected_roi_12m"):
        if fallback_key == primary_key:
            continue
        val = _to_ratio(row.get(fallback_key))
        if val is not None:
            return val
    return None


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

def confidence_bucket_match(row: Mapping[str, Any], wanted: str) -> bool:
    """Check if row's confidence bucket matches the wanted label."""
    if not wanted:
        return True
    row_bucket = _to_string(row.get("confidence_bucket") or row.get("confidence_level")).lower()
    if row_bucket:
        return wanted in row_bucket or row_bucket in wanted
    score = _to_float(row.get("forecast_confidence") or row.get("confidence_score"))
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


def risk_level_match(row: Mapping[str, Any], wanted: str) -> bool:
    """Check if row's risk level matches the wanted label."""
    if not wanted:
        return True
    row_bucket = _to_string(row.get("risk_bucket") or row.get("risk_level")).lower()
    if row_bucket:
        return wanted in row_bucket or row_bucket in wanted
    risk = _to_float(row.get("risk_score"))
    if risk is None:
        return True
    if "low" in wanted or "conservative" in wanted:
        return risk <= 35
    if "moderate" in wanted or "medium" in wanted:
        return 20 <= risk <= 65
    if "high" in wanted or "aggressive" in wanted:
        return risk >= 45
    return True


def passes_filters(row: Mapping[str, Any], ctx: CriteriaContext) -> bool:
    """Check if row passes all criteria filters."""
    if not confidence_bucket_match(row, ctx.confidence_bucket):
        return False
    if not risk_level_match(row, ctx.risk_level):
        return False

    if ctx.min_expected_roi is not None:
        roi = choose_horizon_roi(row, ctx.horizon_days)
        if roi is None or roi < ctx.min_expected_roi:
            return False

    # v6.0.0 bug fix: both sides now on the ratio scale.
    if ctx.min_confidence is not None:
        conf = _to_float(row.get("forecast_confidence") or row.get("confidence_score"))
        if conf is None:
            return False
        if abs(conf) > 1.5:
            conf /= 100.0
        if conf < ctx.min_confidence:
            return False

    if ctx.max_risk_score is not None:
        risk = _to_float(row.get("risk_score"))
        if risk is not None and risk > ctx.max_risk_score:
            return False

    if ctx.min_volume is not None:
        volume = _to_float(row.get("volume"))
        if volume is not None and volume < ctx.min_volume:
            return False

    if ctx.min_technical_score is not None:
        tech = _to_float(row.get("technical_score"))
        if tech is None or tech < ctx.min_technical_score:
            return False

    if ctx.short_term_signal_required in ("BUY", "STRONG_BUY"):
        st_signal = _to_string(row.get("short_term_signal")).upper()
        if st_signal not in ("BUY", "STRONG_BUY"):
            return False

    if ctx.min_upside_pct is not None:
        upside = _to_ratio(row.get("upside_pct"))
        if upside is not None and upside < ctx.min_upside_pct:
            return False

    return True


def direct_symbol_order_index(row: Mapping[str, Any], ctx: CriteriaContext) -> Optional[int]:
    """Return the index of this row's symbol in the user-supplied direct-symbol order."""
    if not ctx.direct_symbol_order:
        return None
    sym = _normalize_symbol(row.get("symbol") or row.get("ticker") or row.get("requested_symbol"))
    if not sym:
        return None
    try:
        return ctx.direct_symbol_order.index(sym)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Data Quality Tier (v6.1.0)
# ---------------------------------------------------------------------------

def compute_data_quality_tier(row: Mapping[str, Any]) -> str:
    """
    Classify a candidate row's data quality.

    Returns one of:
      - "HIGH":     forecast confidence >= 30%, fundamentals >= 5 fields,
                    overall_score is not an engine default (50.0/55.0).
                    These picks are based on real data and are safe to
                    treat as actionable Top 10 candidates.
      - "MODERATE": confidence >= 15% OR fundamentals >= 3 with non-default
                    overall. Worth researching, but verify before acting.
      - "LOW":      mostly engine defaults. Treat as a "research candidate"
                    only -- the underlying scores are not strong enough to
                    drive a buy decision.

    The tier is a transparency safeguard: it does NOT change ranking. A
    LOW-tier row with a high selector score still ranks above a HIGH-tier
    row with a low selector score. The tier just lets the user see, at a
    glance, which Top 10 picks have the strongest evidence behind them.
    """
    # Confidence (normalize percent -> ratio if needed)
    confidence = _to_float(row.get("forecast_confidence") or row.get("confidence_score"))
    if confidence is not None and abs(confidence) > 1.5:
        confidence /= 100.0

    # Count fundamentals with real (non-None, non-zero) values
    fundamentals_present = 0
    for key in _FUNDAMENTAL_KEYS_FOR_TIER:
        val = _to_float(row.get(key))
        if val is not None and val != 0:
            fundamentals_present += 1

    # Is overall_score a real signal or an engine placeholder?
    overall = _to_float(row.get("overall_score"))
    has_real_overall = (
        overall is not None and overall not in _DEFAULT_OVERALL_SCORES
    )

    # HIGH tier
    if (confidence is not None and confidence >= _CONFIDENCE_HIGH_TIER
            and fundamentals_present >= _TIER_HIGH_MIN_FUNDAMENTALS
            and has_real_overall):
        return "HIGH"

    # MODERATE tier (either confidence OR fundamentals path)
    if confidence is not None and confidence >= _CONFIDENCE_MODERATE_TIER:
        return "MODERATE"
    if fundamentals_present >= _TIER_MODERATE_MIN_FUNDAMENTALS and has_real_overall:
        return "MODERATE"

    return "LOW"


# ---------------------------------------------------------------------------
# Scoring (horizon buckets aligned with criteria_model: 1M / 3M / 12M)
# ---------------------------------------------------------------------------

def compute_selector_score(row: Mapping[str, Any], ctx: CriteriaContext) -> float:
    """
    Compute composite selection score.

    v6.0.0 fix: horizon buckets now match `choose_horizon_roi` (1M / 3M / 12M
    via criteria_model thresholds 45 / 120), so the weights align with the
    ROI field being scored.
    """
    overall = _to_float(row.get("overall_score"))
    opportunity = _to_float(row.get("opportunity_score"))
    value_score = _to_float(row.get("value_score"))
    quality_score = _to_float(row.get("quality_score"))
    momentum_score = _to_float(row.get("momentum_score"))
    growth_score = _to_float(row.get("growth_score"))
    risk_score = _to_float(row.get("risk_score"))
    confidence = _to_float(row.get("forecast_confidence") or row.get("confidence_score"))
    liquidity = _to_float(row.get("liquidity_score"))
    technical = _to_float(row.get("technical_score"))
    upside = _to_ratio(row.get("upside_pct"))
    st_signal = _to_string(row.get("short_term_signal")).upper()

    if confidence is not None and confidence <= 1.0:
        confidence *= 100.0

    horizon = _horizon_label_for_days(ctx.horizon_days)
    roi = choose_horizon_roi(row, ctx.horizon_days)
    score = 0.0

    if horizon == "1M":
        # Short horizon: technical + momentum + ST signal dominate
        if technical is not None:
            score += technical * 0.30
        if momentum_score is not None:
            score += momentum_score * 0.25
        if overall is not None:
            score += overall * 0.20
        if opportunity is not None:
            score += opportunity * 0.10
        if roi is not None:
            score += roi * 100.0 * 0.15
        if confidence is not None:
            score += confidence * 0.05
        if risk_score is not None:
            score += (100.0 - risk_score) * 0.08
        if st_signal == "STRONG_BUY":
            score += 15.0
        elif st_signal == "BUY":
            score += 8.0
    elif horizon == "3M":
        # Medium horizon: balanced weights -- overall + roi + fundamentals
        if overall is not None:
            score += overall * 0.30
        if opportunity is not None:
            score += opportunity * 0.15
        if momentum_score is not None:
            score += momentum_score * 0.12
        if technical is not None:
            score += technical * 0.10
        if value_score is not None:
            score += value_score * 0.08
        if quality_score is not None:
            score += quality_score * 0.08
        if growth_score is not None:
            score += growth_score * 0.08
        if confidence is not None:
            score += confidence * 0.08
        if liquidity is not None:
            score += liquidity * 0.05
        if risk_score is not None:
            score += (100.0 - risk_score) * 0.08
        if roi is not None:
            score += roi * 100.0 * 0.20
    else:
        # Long horizon (12M): fundamentals dominate
        if overall is not None:
            score += overall * 0.35
        if opportunity is not None:
            score += opportunity * 0.20
        if value_score is not None:
            score += value_score * 0.10
        if quality_score is not None:
            score += quality_score * 0.10
        if growth_score is not None:
            score += growth_score * 0.10
        if momentum_score is not None:
            score += momentum_score * 0.06
        if confidence is not None:
            score += confidence * 0.08
        if liquidity is not None:
            score += liquidity * 0.05
        if risk_score is not None:
            score += (100.0 - risk_score) * 0.08
        if roi is not None:
            score += roi * 100.0 * 0.20
        if technical is not None:
            score += technical * 0.05

    # Upside bonus (ratio scale)
    if upside is not None and upside > 0:
        score += min(upside * 100.0, 20.0) * 0.15

    # Direct symbol priority -- user-supplied order comes first
    order_idx = direct_symbol_order_index(row, ctx)
    if order_idx is not None:
        score += max(0.0, 140.0 - float(order_idx * 2))

    # Richness micro-boost (break ties toward rows with more data)
    score += min(_count_nonblank_fields(row), 120) * 0.03

    return float(score)


def generate_selection_reason(row: Dict[str, Any], ctx: CriteriaContext) -> str:
    """Generate a one-line, human-readable selection reason."""
    recommendation = _to_string(row.get("recommendation"))
    confidence_bucket = _to_string(row.get("confidence_bucket"))
    risk_bucket = _to_string(row.get("risk_bucket"))
    source_page = _to_string(row.get("source_page"))
    horizon_roi = choose_horizon_roi(row, ctx.horizon_days)

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
    if confidence_bucket:
        parts.append(f"Confidence={confidence_bucket}")
    if risk_bucket:
        parts.append(f"Risk={risk_bucket}")
    if horizon_roi is not None:
        parts.append(f"Horizon ROI={round(horizon_roi * 100.0, 2)}%")
    if source_page:
        parts.append(f"Source={source_page}")
    if score_parts:
        parts.append(", ".join(score_parts[:3]))

    tech = row.get("technical_score")
    if isinstance(tech, (int, float)):
        parts.append(f"Tech={round(float(tech), 1)}")
    st_signal = _to_string(row.get("short_term_signal"))
    if st_signal and st_signal.upper() not in ("HOLD", "N/A", ""):
        parts.append(f"ST={st_signal}")
    upside = _to_ratio(row.get("upside_pct"))
    if upside is not None:
        parts.append(f"Upside={round(upside * 100.0, 1)}%")

    return " | ".join(parts) if parts else "Selected by Top10 composite scoring."


# ---------------------------------------------------------------------------
# Trade Setup (entry / stop / take-profit / R:R)
# ---------------------------------------------------------------------------

def compute_trade_setup(row: Dict[str, Any], ctx: CriteriaContext) -> None:
    """
    Compute entry_price, stop_loss_suggested, take_profit_suggested, risk_reward_ratio
    in place on `row`.

    Heuristics:
      Entry: if day_range_position < _DAY_RANGE_ENTRY_THRESHOLD, use current price.
             Otherwise slight discount toward range bottom.
      Stop:  entry - ATR * _STOP_LOSS_ATR_MULT, or vol-based, or _STOP_LOSS_MIN_PCT.
             Never closer than (1 - _STOP_LOSS_MAX_CLOSENESS) below entry.
      TP:    entry * (1 + upside), capped at _TAKE_PROFIT_UPSIDE_CAP; or forecast_price_3m;
             or _TAKE_PROFIT_DEFAULT. Never closer than _TAKE_PROFIT_MIN_CLOSENESS above entry.
    """
    if not _is_blank(row.get("entry_price")) and not _is_blank(row.get("risk_reward_ratio")):
        return

    price = _to_float(row.get("current_price"))
    if price is None or price <= 0:
        return

    # Entry price
    day_range_pos = _to_float(row.get("day_range_position"))
    if day_range_pos is not None and day_range_pos < _DAY_RANGE_ENTRY_THRESHOLD:
        entry = price
    elif day_range_pos is not None:
        entry = round(price * (1.0 - day_range_pos * _DAY_RANGE_ENTRY_DISCOUNT), 4)
    else:
        entry = price
    row["entry_price"] = entry

    # Stop loss
    if _is_blank(row.get("stop_loss_suggested")):
        atr = _to_float(row.get("atr_14"))
        if atr is not None and atr > 0:
            sl = round(entry - atr * _STOP_LOSS_ATR_MULT, 4)
        else:
            vol30 = _to_float(row.get("volatility_30d"))
            if vol30 is not None:
                # Convert annualized vol to daily move, handling both ratio and percent input
                vol_ratio = vol30 if abs(vol30) <= 1.5 else vol30 / 100.0
                daily_move = (vol_ratio / _SQRT_TRADING_DAYS) * entry
                sl = round(
                    entry - max(daily_move * _STOP_LOSS_VOL_DAILY_MULT, entry * _STOP_LOSS_MIN_PCT),
                    4,
                )
            else:
                sl = round(entry * (1.0 - _STOP_LOSS_MIN_PCT), 4)
        sl = min(sl, entry * _STOP_LOSS_MAX_CLOSENESS)
        row["stop_loss_suggested"] = sl

    # Take profit
    if _is_blank(row.get("take_profit_suggested")):
        upside = _to_ratio(row.get("upside_pct"))
        if upside is not None and upside > 0.01:
            tp = round(entry * (1.0 + min(upside, _TAKE_PROFIT_UPSIDE_CAP)), 4)
        else:
            fp3 = _to_float(row.get("forecast_price_3m"))
            if fp3 is not None and fp3 > entry * 1.03:
                tp = round(fp3, 4)
            else:
                tp = round(entry * _TAKE_PROFIT_DEFAULT, 4)
        tp = max(tp, entry * _TAKE_PROFIT_MIN_CLOSENESS)
        row["take_profit_suggested"] = tp

    # Risk/Reward ratio
    if _is_blank(row.get("risk_reward_ratio")):
        entry_v = _to_float(row.get("entry_price"))
        sl_v = _to_float(row.get("stop_loss_suggested"))
        tp_v = _to_float(row.get("take_profit_suggested"))
        if entry_v and sl_v and tp_v and entry_v > sl_v:
            row["risk_reward_ratio"] = round((tp_v - entry_v) / (entry_v - sl_v), 2)


def rank_and_project_rows(
    rows: Sequence[Mapping[str, Any]],
    keys: Sequence[str],
    ctx: CriteriaContext,
) -> List[Dict[str, Any]]:
    """Rank rows, compute trade setups, tag data quality, and project to schema keys."""
    criteria_snapshot = _compact_json(ctx.to_dict())
    result: List[Dict[str, Any]] = []

    for idx, raw in enumerate(rows, start=1):
        row = dict(raw)
        if _is_blank(row.get("top10_rank")):
            row["top10_rank"] = idx
        if _is_blank(row.get("rank_overall")):
            row["rank_overall"] = idx

        compute_trade_setup(row, ctx)

        if _is_blank(row.get("selection_reason")):
            row["selection_reason"] = generate_selection_reason(row, ctx)
        if _is_blank(row.get("criteria_snapshot")):
            row["criteria_snapshot"] = criteria_snapshot

        # v6.1.0: tag every row with its data-quality tier so users can
        # see which Top 10 picks have real signal vs engine defaults.
        if _is_blank(row.get("data_quality_tier")):
            row["data_quality_tier"] = compute_data_quality_tier(row)

        projected = {k: _json_safe(row.get(k)) for k in keys}
        result.append(projected)

    return result


# ---------------------------------------------------------------------------
# Engine Resolution (lazy lock, consolidated retry helper)
# ---------------------------------------------------------------------------

_ENGINE_CACHE: Optional[Any] = None
_ENGINE_CACHE_SOURCE: str = ""
_ENGINE_LOCK: Optional[asyncio.Lock] = None  # v6.0.0: lazy init

_ENGINE_METHOD_NAMES: Tuple[str, ...] = (
    "get_sheet_rows", "get_page_rows", "sheet_rows", "build_sheet_rows", "execute_sheet_rows",
    "run_sheet_rows", "build_analysis_sheet_rows", "run_analysis_sheet_rows", "get_rows_for_sheet",
    "get_rows_for_page", "get_page_data", "get_sheet_data", "build_page_rows", "build_page_data",
    "get_cached_sheet_snapshot", "get_sheet_snapshot", "get_cached_sheet_rows", "get_page_snapshot",
    "get_enriched_quotes_batch", "get_analysis_quotes_batch", "get_quotes_batch", "quotes_batch",
    "get_quotes", "get_quote", "get_quote_dict", "get_enriched_quote",
)

_ENGINE_HOLDER_ATTRS: Tuple[str, ...] = (
    "engine", "data_engine", "quote_engine", "cache_engine", "_engine", "_data_engine",
    "service", "runner", "advisor_engine",
)


def _get_engine_lock() -> asyncio.Lock:
    """Lazy-init the engine resolution lock so it binds to the live event loop."""
    global _ENGINE_LOCK
    if _ENGINE_LOCK is None:
        _ENGINE_LOCK = asyncio.Lock()
    return _ENGINE_LOCK


def _looks_like_engine(obj: Any) -> bool:
    """Duck-type check: does this object have any of the known engine methods?"""
    if obj is None:
        return False
    if isinstance(obj, (str, bytes, int, float, bool, list, tuple, set)):
        return False
    if isinstance(obj, Mapping):
        return False
    return any(callable(getattr(obj, m, None)) for m in _ENGINE_METHOD_NAMES)


async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Call fn, awaiting if coroutine; offload sync calls to a thread."""
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    result = await asyncio.to_thread(fn, *args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


async def _call_with_timeout(
    fn: Callable[..., Any],
    *args: Any,
    timeout: float,
    **kwargs: Any,
) -> Any:
    """Call fn with a timeout; raises asyncio.TimeoutError on expiry."""
    return await asyncio.wait_for(_call_maybe_async(fn, *args, **kwargs), timeout=timeout)


async def _try_engine_calls(
    engine: Any,
    calls: Sequence[Tuple[str, Tuple[Any, ...], Dict[str, Any]]],
    timeout_sec: float,
) -> List[Dict[str, Any]]:
    """
    Try a sequence of (method_name, args, kwargs) calls against `engine`.

    Returns the first non-empty row list produced by `_extract_rows_from_payload`,
    or [] if every call failed, timed out, or returned no rows.

    v6.0.0: Replaces three near-identical retry loops in v5.0.0.
    """
    for method_name, args, kwargs in calls:
        fn = getattr(engine, method_name, None)
        if not callable(fn):
            continue
        try:
            result = await _call_with_timeout(fn, *args, timeout=timeout_sec, **kwargs)
        except TypeError as exc:
            if not _is_signature_mismatch(exc):
                logger.debug("engine.%s raised non-signature TypeError: %s", method_name, exc)
            continue
        except asyncio.TimeoutError:
            logger.debug("engine.%s timed out after %.1fs", method_name, timeout_sec)
            continue
        except Exception as exc:
            logger.debug("engine.%s failed: %s", method_name, exc)
            continue

        rows = _extract_rows_from_payload(result)
        if rows:
            return rows
    return []


async def _resolve_engine_from_modules() -> Tuple[Optional[Any], str]:
    """Scan known modules looking for an engine instance or factory."""
    global _ENGINE_CACHE, _ENGINE_CACHE_SOURCE

    if _looks_like_engine(_ENGINE_CACHE):
        return _ENGINE_CACHE, _ENGINE_CACHE_SOURCE or "engine_cache"

    async with _get_engine_lock():
        if _looks_like_engine(_ENGINE_CACHE):
            return _ENGINE_CACHE, _ENGINE_CACHE_SOURCE or "engine_cache"

        module_names = (
            "main", "app", "core.data_engine_v2", "core.data_engine",
            "routes.analysis_sheet_rows", "routes.investment_advisor",
            "routes.advanced_analysis", "routes.enriched_quote", "routes.advisor",
        )

        candidates: List[Tuple[Any, str]] = []
        for module_name in module_names:
            try:
                mod = importlib.import_module(module_name)
            except Exception:
                continue
            if mod is None:
                continue
            if _looks_like_engine(mod):
                candidates.append((mod, module_name))
            for fn_name in ("get_engine", "resolve_engine", "load_engine", "build_engine", "create_engine"):
                fn = getattr(mod, fn_name, None)
                if callable(fn):
                    try:
                        result = await _call_with_timeout(
                            fn, timeout=min(_CONFIG.engine_call_timeout_sec, 6.0),
                        )
                        if _looks_like_engine(result):
                            candidates.append((result, f"{module_name}.{fn_name}"))
                    except Exception:
                        continue
            for attr in _ENGINE_HOLDER_ATTRS:
                val = getattr(mod, attr, None)
                if val is not None and _looks_like_engine(val):
                    candidates.append((val, f"{module_name}.{attr}"))

        if candidates:
            _ENGINE_CACHE, _ENGINE_CACHE_SOURCE = candidates[0]
            return _ENGINE_CACHE, _ENGINE_CACHE_SOURCE

    return None, "engine_unavailable"


async def resolve_engine(*args: Any, **kwargs: Any) -> Tuple[Optional[Any], str]:
    """Resolve a data engine from call args, kwargs, nested bodies, or module scan."""
    for key in ("engine", "data_engine", "quote_engine", "cache_engine", "service", "runner", "request", "app", "context"):
        val = kwargs.get(key)
        if val and _looks_like_engine(val):
            return val, f"kwarg:{key}"

    for i, arg in enumerate(args):
        if arg and _looks_like_engine(arg):
            return arg, f"arg:{i}"

    for key in ("body", "payload", "request_data", "params", "criteria"):
        val = kwargs.get(key)
        if isinstance(val, Mapping):
            for subkey in _ENGINE_HOLDER_ATTRS:
                subval = val.get(subkey)
                if subval and _looks_like_engine(subval):
                    return subval, f"{key}.{subkey}"

    return await _resolve_engine_from_modules()


# ---------------------------------------------------------------------------
# Data Fetching (via consolidated _try_engine_calls)
# ---------------------------------------------------------------------------

def _extract_rows_from_payload(payload: Any) -> List[Dict[str, Any]]:
    """Extract a list of row dicts from a variety of payload shapes."""
    if payload is None:
        return []

    if isinstance(payload, list):
        if not payload:
            return []
        if all(isinstance(x, dict) for x in payload):
            return [dict(x) for x in payload]
        return []

    if isinstance(payload, dict):
        # Symbol map form: {"AAPL": {...}, "MSFT": {...}}
        if payload:
            is_symbol_map = True
            symbol_rows: List[Dict[str, Any]] = []
            for key, value in payload.items():
                if not isinstance(value, dict) or not _looks_like_symbol(key):
                    is_symbol_map = False
                    break
                row = dict(value)
                if not row.get("symbol"):
                    row["symbol"] = _normalize_symbol(key)
                symbol_rows.append(row)
            if is_symbol_map and symbol_rows:
                return symbol_rows

        # Common container keys
        for key in ("row_objects", "rows", "records", "items", "data", "quotes", "results"):
            value = payload.get(key)
            if not isinstance(value, list) or not value:
                continue
            if isinstance(value[0], dict):
                return [dict(row) for row in value if isinstance(row, dict)]
            if isinstance(value[0], (list, tuple)):
                columns = payload.get("keys") or payload.get("headers") or payload.get("columns") or []
                if isinstance(columns, list) and columns:
                    matrix_rows = _rows_from_matrix(value, columns)
                    if matrix_rows:
                        return matrix_rows

        if _looks_like_row(payload):
            return [dict(payload)]

        for key in ("result", "payload", "response", "output", "snapshot", "content", "envelope"):
            nested = payload.get(key)
            nested_rows = _extract_rows_from_payload(nested)
            if nested_rows:
                return nested_rows

    return []


def _rows_from_matrix(matrix: Any, columns: Sequence[str]) -> List[Dict[str, Any]]:
    """Convert a matrix (list of lists) to list of dicts using the given column order."""
    if not isinstance(matrix, list) or not matrix or not columns:
        return []
    keys = [_to_string(c) for c in columns if _to_string(c)]
    if not keys:
        return []
    result: List[Dict[str, Any]] = []
    for row in matrix:
        if not isinstance(row, (list, tuple)):
            continue
        vals = list(row)
        result.append({keys[i]: (vals[i] if i < len(vals) else None) for i in range(len(keys))})
    return result


def _page_fetch_calls(page: str, limit: int, mode: str) -> List[Tuple[str, Tuple[Any, ...], Dict[str, Any]]]:
    """Build the call sequence for fetching rows from a source page."""
    body = {
        "page": page, "page_name": page, "sheet": page, "sheet_name": page,
        "tab": page, "worksheet": page, "name": page,
        "limit": limit, "top_n": limit, "mode": mode or "",
        "include_headers": True, "include_matrix": True,
        "schema_only": False, "headers_only": False,
    }
    kwargs_variants: List[Dict[str, Any]] = [
        {"page": page, "sheet": page, "sheet_name": page, "limit": limit, "mode": mode, "body": body},
        {"page_name": page, "sheet_name": page, "limit": limit, "mode": mode, "body": body},
        {"payload": body},
        {"body": body},
        {"page": page, "sheet": page, "sheet_name": page, "limit": limit, "mode": mode},
        {"page_name": page, "sheet_name": page, "limit": limit, "mode": mode},
        {"page": page, "sheet": page, "limit": limit, "mode": mode},
        {"page": page, "limit": limit, "mode": mode},
    ]
    args_variants: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = [
        ((page,), {"limit": limit, "mode": mode}),
        ((page,), {"limit": limit}),
        ((page,), {}),
    ]
    method_names = (
        "get_sheet_rows", "get_page_rows", "sheet_rows", "build_sheet_rows",
        "execute_sheet_rows", "run_sheet_rows", "build_analysis_sheet_rows",
        "run_analysis_sheet_rows", "get_rows_for_sheet", "get_rows_for_page",
        "get_page_data", "get_sheet_data", "build_page_rows", "build_page_data",
    )
    calls: List[Tuple[str, Tuple[Any, ...], Dict[str, Any]]] = []
    for method in method_names:
        for kw in kwargs_variants:
            calls.append((method, (), kw))
        for args, kw in args_variants:
            calls.append((method, args, kw))
    return calls


def _snapshot_fetch_calls(page: str) -> List[Tuple[str, Tuple[Any, ...], Dict[str, Any]]]:
    """Build the call sequence for fetching a cached page snapshot."""
    method_names = (
        "get_cached_sheet_snapshot", "get_sheet_snapshot", "get_cached_sheet_rows",
        "get_page_snapshot", "get_sheet_cache",
    )
    variants = [
        ((), {"sheet_name": page}),
        ((), {"sheet": page}),
        ((), {"page": page}),
        ((), {"page_name": page}),
        ((page,), {}),
    ]
    return [(m, a, k) for m in method_names for a, k in variants]


def _direct_symbol_batch_calls(
    syms: List[str], mode: str,
) -> List[Tuple[str, Tuple[Any, ...], Dict[str, Any]]]:
    """Build the call sequence for batch-fetching direct symbol rows."""
    method_names = (
        "get_enriched_quotes_batch", "get_analysis_quotes_batch", "get_quotes_batch", "quotes_batch",
        "get_quotes", "get_enriched_quote_batch", "get_symbol_quotes", "get_live_quotes",
    )
    variants = [
        ((), {"symbols": syms, "mode": mode, "schema": OUTPUT_PAGE}),
        ((), {"symbols": syms, "mode": mode}),
        ((), {"symbols": syms}),
        ((syms,), {"mode": mode, "schema": OUTPUT_PAGE}),
        ((syms,), {"mode": mode}),
        ((syms,), {}),
        ((), {"tickers": syms, "mode": mode}),
        ((), {"tickers": syms}),
    ]
    return [(m, a, k) for m in method_names for a, k in variants]


def _direct_symbol_single_calls(
    sym: str, mode: str,
) -> List[Tuple[str, Tuple[Any, ...], Dict[str, Any]]]:
    """Build the call sequence for per-symbol fallback fetches."""
    method_names = (
        "get_enriched_quote", "get_quote", "get_quote_dict", "get_live_quote", "get_symbol_quote",
    )
    variants = [
        ((sym,), {"mode": mode, "schema": OUTPUT_PAGE}),
        ((sym,), {"mode": mode}),
        ((sym,), {}),
        ((), {"symbol": sym, "mode": mode}),
        ((), {"symbol": sym}),
        ((), {"ticker": sym, "mode": mode}),
    ]
    return [(m, a, k) for m in method_names for a, k in variants]


async def fetch_page_rows(engine: Any, page: str, limit: int, mode: str) -> List[Dict[str, Any]]:
    """Fetch rows from a source page, trying many method/kwarg combinations."""
    if engine is None:
        return []
    return await _try_engine_calls(
        engine,
        _page_fetch_calls(page, limit, mode),
        timeout_sec=min(_CONFIG.engine_call_timeout_sec, 8.0),
    )


async def fetch_page_snapshot(engine: Any, page: str) -> List[Dict[str, Any]]:
    """Fetch cached snapshot of a page."""
    if engine is None:
        return []
    return await _try_engine_calls(
        engine,
        _snapshot_fetch_calls(page),
        timeout_sec=min(_CONFIG.engine_call_timeout_sec, 6.0),
    )


async def fetch_direct_symbol_rows(engine: Any, symbols: List[str], mode: str) -> List[Dict[str, Any]]:
    """Fetch rows directly by symbols (batch, then per-symbol fallback)."""
    if engine is None or not symbols:
        return []
    syms = [_normalize_symbol(s) for s in symbols if _normalize_symbol(s)]
    if not syms:
        return []

    rows = await _try_engine_calls(
        engine,
        _direct_symbol_batch_calls(syms, mode),
        timeout_sec=min(_CONFIG.engine_call_timeout_sec, 7.0),
    )
    if rows:
        return rows

    # Per-symbol fallback
    result_rows: List[Dict[str, Any]] = []
    for sym in syms[:_CONFIG.hydration_symbol_cap]:
        single_rows = await _try_engine_calls(
            engine,
            _direct_symbol_single_calls(sym, mode),
            timeout_sec=min(_CONFIG.engine_call_timeout_sec, 5.0),
        )
        if single_rows:
            result_rows.extend(single_rows)
    return result_rows


# ---------------------------------------------------------------------------
# Collection Pipeline
# ---------------------------------------------------------------------------

async def collect_page_rows_with_fallback(
    engine: Any,
    page: str,
    mode: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Collect rows from a page, falling back to the cached snapshot if needed."""
    meta: Dict[str, Any] = {
        "page": page, "rows": 0, "snapshot_rows": 0,
        "used_snapshot": False, "merged_snapshot": False,
        "timed_out": False, "error": "",
    }

    try:
        rows = await asyncio.wait_for(
            fetch_page_rows(engine, page, _CONFIG.source_page_limit, mode),
            timeout=_CONFIG.page_total_timeout_sec,
        )
        meta["rows"] = len(rows)
    except asyncio.TimeoutError:
        rows = []
        meta["timed_out"] = True
        meta["error"] = "page_rows_timeout"
    except Exception as exc:
        rows = []
        meta["error"] = f"page_rows_failed:{type(exc).__name__}"

    should_try_snapshot = bool(rows) and (
        len(rows) < 5
        or max((_count_nonblank_fields(r) for r in rows), default=0) < 10
    )
    if rows and not should_try_snapshot:
        return rows, meta

    try:
        snapshot = await asyncio.wait_for(
            fetch_page_snapshot(engine, page),
            timeout=min(_CONFIG.page_total_timeout_sec, 8.0),
        )
        meta["snapshot_rows"] = len(snapshot)
        meta["used_snapshot"] = bool(snapshot)
        if rows and snapshot:
            merged = merge_symbol_rows(rows, snapshot)
            meta["merged_snapshot"] = True
            return merged, meta
        if snapshot:
            return snapshot, meta
        return rows, meta
    except asyncio.TimeoutError:
        if not meta["error"]:
            meta["error"] = "snapshot_timeout"
        return rows if rows else [], meta
    except Exception as exc:
        if not meta["error"]:
            meta["error"] = f"snapshot_failed:{type(exc).__name__}"
        return rows if rows else [], meta


async def hydrate_page_rows(
    engine: Any,
    rows: List[Dict[str, Any]],
    mode: str,
    ctx: CriteriaContext,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Hydrate the top-scoring rows with enriched data via direct-symbol fetch."""
    meta: Dict[str, Any] = {
        "requested_symbols": 0, "enriched_rows": 0,
        "hydration_used": False, "hydration_error": "",
    }
    if not rows:
        return rows, meta

    # Score rows to prioritize which to hydrate first
    scored: List[Tuple[float, Dict[str, Any]]] = []
    for row in rows:
        normalized = normalize_candidate_row(row)
        sym = _normalize_symbol(normalized.get("symbol") or normalized.get("ticker"))
        if not sym:
            continue
        scored.append((compute_selector_score(normalized, ctx), normalized))
    scored.sort(key=lambda x: (x[0], _count_nonblank_fields(x[1])), reverse=True)

    symbols = _dedupe_keep_order(
        _normalize_symbol(item[1].get("symbol") or item[1].get("ticker"))
        for item in scored
    )[:_CONFIG.hydration_symbol_cap]

    meta["requested_symbols"] = len(symbols)
    if not symbols:
        return rows, meta

    try:
        enriched = await asyncio.wait_for(
            fetch_direct_symbol_rows(engine, symbols, mode),
            timeout=min(_CONFIG.page_total_timeout_sec, 10.0),
        )
        meta["enriched_rows"] = len(enriched)
        meta["hydration_used"] = bool(enriched)
    except asyncio.TimeoutError:
        meta["hydration_error"] = "hydration_timeout"
        return rows, meta
    except Exception as exc:
        meta["hydration_error"] = f"hydration_failed:{type(exc).__name__}"
        return rows, meta

    if not enriched:
        return rows, meta

    row_map: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        normalized = normalize_candidate_row(row)
        sym = _normalize_symbol(normalized.get("symbol") or normalized.get("ticker"))
        if sym:
            row_map[sym] = normalized

    for er in enriched:
        normalized = normalize_candidate_row(er)
        sym = _normalize_symbol(normalized.get("symbol") or normalized.get("ticker"))
        if not sym:
            continue
        row_map[sym] = merge_rows(row_map[sym], normalized) if sym in row_map else normalized

    return list(row_map.values()), meta


async def collect_candidate_rows(
    engine: Any,
    ctx: CriteriaContext,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Collect candidate rows from direct symbols, source pages, and fallbacks."""
    direct_symbols = ctx.direct_symbols
    emergency_symbols = ctx.emergency_symbols
    pages = ctx.pages_selected

    direct_symbol_index_map = {sym: idx for idx, sym in enumerate(direct_symbols)}

    meta: Dict[str, Any] = {
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

    def add_candidate(row: Mapping[str, Any], source_page: str = "") -> None:
        normalized = normalize_candidate_row(row)
        sym = _normalize_symbol(normalized.get("symbol") or normalized.get("ticker"))
        if not sym:
            return
        if source_page and _is_blank(normalized.get("source_page")):
            normalized["source_page"] = source_page
        if sym in direct_symbol_index_map:
            normalized["_direct_symbol_priority"] = True
            normalized["_direct_symbol_index"] = direct_symbol_index_map[sym]
            if _is_blank(normalized.get("source_page")):
                normalized["source_page"] = "Direct"

        existing = candidates.get(sym)
        if existing is None:
            candidates[sym] = normalized
            return
        if _count_nonblank_fields(normalized) >= _count_nonblank_fields(existing):
            candidates[sym] = merge_rows(existing, normalized)
        else:
            candidates[sym] = merge_rows(normalized, existing)

    # Direct symbols first
    if direct_symbols:
        try:
            direct_rows = await asyncio.wait_for(
                fetch_direct_symbol_rows(
                    engine, direct_symbols[:_CONFIG.hydration_symbol_cap], ctx.mode,
                ),
                timeout=min(_CONFIG.builder_total_timeout_sec / 2.0, 12.0),
            )
            meta["direct_symbol_rows"] = len(direct_rows)
            meta["partial_success"] = meta["partial_success"] or bool(direct_rows)
            for row in direct_rows:
                add_candidate(row, "")
        except asyncio.TimeoutError:
            meta["direct_symbol_error"] = "direct_symbol_timeout"
        except Exception as exc:
            meta["direct_symbol_error"] = f"direct_symbol_failed:{type(exc).__name__}"

    early_stop_target = max(10, ctx.limit * _CONFIG.early_stop_multiplier)

    for page in pages:
        page_rows, page_meta = await collect_page_rows_with_fallback(engine, page, ctx.mode)
        meta["source_page_rows"][page] = page_meta.get("rows", 0)
        meta["snapshot_rows"][page] = page_meta.get("snapshot_rows", 0)

        hydrated_rows = page_rows
        hydration_meta: Dict[str, Any] = {"page": page}
        if page_rows:
            hydrated_rows, hydration_meta = await hydrate_page_rows(engine, page_rows, ctx.mode, ctx)
            hydration_meta["page"] = page

        meta["page_diagnostics"].append(_json_safe(page_meta))
        meta["hydration_diagnostics"].append(_json_safe(hydration_meta))
        if hydrated_rows:
            meta["partial_success"] = True
        for row in hydrated_rows:
            add_candidate(row, page)

        # Early stop, but never skip My_Portfolio (portfolio positions always included)
        if len(candidates) >= early_stop_target and page != "My_Portfolio":
            meta["early_stop"] = True
            meta["early_stop_after_page"] = page
            break

    # Emergency fallback
    if not candidates and emergency_symbols:
        try:
            emergency_rows = await asyncio.wait_for(
                fetch_direct_symbol_rows(
                    engine, emergency_symbols[:_CONFIG.hydration_symbol_cap], ctx.mode,
                ),
                timeout=min(_CONFIG.builder_total_timeout_sec / 2.0, 12.0),
            )
            meta["used_emergency_symbols"] = bool(emergency_rows)
            meta["emergency_symbol_rows"] = len(emergency_rows)
            for row in emergency_rows:
                add_candidate(row, "Emergency")
        except Exception as exc:
            meta["emergency_symbol_error"] = f"emergency_symbol_failed:{type(exc).__name__}"

    # Top10 output-page fallback (read yesterday's Top10 if we still have nothing)
    if len(candidates) < max(1, ctx.limit):
        fallback_rows: List[Dict[str, Any]] = []
        try:
            fallback_rows = await asyncio.wait_for(
                fetch_page_rows(engine, OUTPUT_PAGE, max(30, ctx.limit * 2), ctx.mode),
                timeout=min(_CONFIG.page_total_timeout_sec, 10.0),
            )
            meta["top10_output_fallback_rows"] = len(fallback_rows)
        except asyncio.TimeoutError:
            meta["top10_output_fallback_error"] = "top10_output_timeout"
        except Exception as exc:
            meta["top10_output_fallback_error"] = f"top10_output_failed:{type(exc).__name__}"

        if not fallback_rows:
            try:
                fallback_rows = await asyncio.wait_for(
                    fetch_page_snapshot(engine, OUTPUT_PAGE),
                    timeout=min(_CONFIG.page_total_timeout_sec, 8.0),
                )
                meta["top10_output_snapshot_rows"] = len(fallback_rows)
            except asyncio.TimeoutError:
                meta["top10_output_snapshot_error"] = "top10_output_snapshot_timeout"
            except Exception as exc:
                meta["top10_output_snapshot_error"] = f"top10_output_snapshot_failed:{type(exc).__name__}"

        if fallback_rows:
            meta["top10_output_fallback_used"] = True
            for row in fallback_rows:
                add_candidate(row, OUTPUT_PAGE)

    meta["deduped_candidate_count"] = len(candidates)
    return list(candidates.values()), meta


# ---------------------------------------------------------------------------
# Payload Builder
# ---------------------------------------------------------------------------

def build_payload(
    status: str,
    headers: List[str],
    keys: List[str],
    rows: List[Dict[str, Any]],
    meta: Dict[str, Any],
) -> Dict[str, Any]:
    """Build the final payload for the Top10 output."""
    include_headers = _to_bool(meta.get("include_headers", True), True)
    include_matrix = _to_bool(meta.get("include_matrix", True), True)

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


# ---------------------------------------------------------------------------
# Core Async Implementation
# ---------------------------------------------------------------------------

async def build_top10_rows_async(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    """
    Core async implementation of Top10 selector.

    Call directly from async code when you want a clean `Awaitable[dict]`
    contract (no polymorphic return). The sync-tolerant `build_top10_rows`
    wraps this and handles event-loop detection.
    """
    start_time = time.perf_counter()
    headers, keys = _load_schema_defaults()
    ctx = CriteriaContext.from_inputs(*args, **kwargs)

    # Schema-only short-circuit
    if ctx.schema_only or ctx.headers_only:
        return build_payload(
            status="success", headers=headers, keys=keys, rows=[],
            meta={
                "build_status": "OK",
                "dispatch": "top10_selector",
                "selector_version": TOP10_SELECTOR_VERSION,
                "schema_only": ctx.schema_only,
                "headers_only": ctx.headers_only,
                "criteria_used": _json_safe(ctx.to_dict()),
                "include_headers": ctx.include_headers,
                "include_matrix": ctx.include_matrix,
                "duration_ms": round((time.perf_counter() - start_time) * 1000.0, 3),
            },
        )

    # Engine resolution
    engine, engine_source = await resolve_engine(*args, **kwargs)
    if engine is None:
        return build_payload(
            status="warn", headers=headers, keys=keys, rows=[],
            meta={
                "build_status": "DEGRADED",
                "dispatch": "top10_selector",
                "selector_version": TOP10_SELECTOR_VERSION,
                "warning": "engine_unavailable",
                "criteria_used": _json_safe(ctx.to_dict()),
                "include_headers": ctx.include_headers,
                "include_matrix": ctx.include_matrix,
                "engine_source": engine_source,
                "duration_ms": round((time.perf_counter() - start_time) * 1000.0, 3),
            },
        )

    # Candidate collection
    try:
        candidates, collect_meta = await collect_candidate_rows(engine, ctx)
    except Exception as exc:
        logger.warning("Top10 candidate collection failed: %s", exc, exc_info=True)
        return build_payload(
            status="warn", headers=headers, keys=keys, rows=[],
            meta={
                "build_status": "DEGRADED",
                "dispatch": "top10_selector",
                "selector_version": TOP10_SELECTOR_VERSION,
                "warning": f"candidate_collection_failed:{type(exc).__name__}",
                "criteria_used": _json_safe(ctx.to_dict()),
                "include_headers": ctx.include_headers,
                "include_matrix": ctx.include_matrix,
                "engine_source": engine_source,
                "duration_ms": round((time.perf_counter() - start_time) * 1000.0, 3),
            },
        )

    # Normalize, filter, score
    normalized = [normalize_candidate_row(r) for r in candidates]
    filtered = [r for r in normalized if passes_filters(r, ctx)]

    # v6.1.0: relax filters when the strict pool can't fill `limit` rows.
    # Previously: relaxation only kicked in when filtered was completely empty,
    # so a small (1-2 row) strict pool produced a near-empty Top_10. Now the
    # selector falls through to the full normalized pool whenever the strict
    # pool is short -- HIGH-quality picks still come first via scoring +
    # data_quality_tier ordering, and the sheet fills out with MODERATE/LOW
    # candidates that are clearly labeled.
    filter_relaxed = False
    selected_pool = filtered
    if len(selected_pool) < ctx.limit and normalized:
        selected_pool = list(normalized)
        filter_relaxed = True

    scored: List[Tuple[float, Dict[str, Any]]] = [
        (compute_selector_score(row, ctx), dict(row)) for row in selected_pool
    ]
    scored.sort(
        key=lambda x: (
            x[0],
            choose_horizon_roi(x[1], ctx.horizon_days) or 0.0,
            _to_float(x[1].get("opportunity_score"), 0.0) or 0.0,
            _to_float(x[1].get("overall_score"), 0.0) or 0.0,
            _to_float(x[1].get("forecast_confidence"), 0.0) or 0.0,
            -(_to_float(x[1].get("risk_score"), 999.0) or 999.0),
            _to_float(x[1].get("liquidity_score"), 0.0) or 0.0,
            _count_nonblank_fields(x[1]),
        ),
        reverse=True,
    )

    top_rows = [row for _, row in scored[:ctx.limit]]

    # Top up with direct symbols if still under limit
    if len(top_rows) < ctx.limit:
        seen_symbols = {
            _normalize_symbol(r.get("symbol") or r.get("ticker"))
            for r in top_rows
            if _normalize_symbol(r.get("symbol") or r.get("ticker"))
        }
        direct_rows = [row for _, row in scored if direct_symbol_order_index(row, ctx) is not None]
        direct_rows.sort(key=lambda row: direct_symbol_order_index(row, ctx) or 999999)
        for row in direct_rows:
            sym = _normalize_symbol(row.get("symbol") or row.get("ticker"))
            if not sym or sym in seen_symbols:
                continue
            top_rows.append(row)
            seen_symbols.add(sym)
            if len(top_rows) >= ctx.limit:
                break

    projected_rows = rank_and_project_rows(top_rows[:ctx.limit], keys, ctx)

    # v6.1.0: tally tiers for transparency in meta
    tier_counts: Dict[str, int] = {"HIGH": 0, "MODERATE": 0, "LOW": 0}
    for r in projected_rows:
        tier = _to_string(r.get("data_quality_tier")) or "LOW"
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

    status = "success" if projected_rows else "warn"
    meta = {
        "build_status": "OK" if projected_rows else "WARN",
        "dispatch": "top10_selector",
        "selector_version": TOP10_SELECTOR_VERSION,
        "criteria_used": _json_safe(ctx.to_dict()),
        "candidate_count": len(normalized),
        "filtered_count": len(filtered),
        "selected_count": len(projected_rows),
        "filter_relaxed": filter_relaxed,
        "data_quality_tier_counts": tier_counts,
        "selected_symbols": [
            _to_string(r.get("symbol")) for r in projected_rows if _to_string(r.get("symbol"))
        ],
        "selected_direct_symbols": [
            _to_string(r.get("symbol"))
            for r in projected_rows
            if direct_symbol_order_index(r, ctx) is not None and _to_string(r.get("symbol"))
        ],
        "include_headers": ctx.include_headers,
        "include_matrix": ctx.include_matrix,
        "engine_source": engine_source,
        "duration_ms": round((time.perf_counter() - start_time) * 1000.0, 3),
        **collect_meta,
    }
    if not projected_rows:
        meta["warning"] = "no_top10_rows_after_filtering"

    return build_payload(status=status, headers=headers, keys=keys, rows=projected_rows, meta=meta)


# ---------------------------------------------------------------------------
# Public API (sync-tolerant wrapper; polymorphic return for back-compat)
# ---------------------------------------------------------------------------

def build_top10_rows(*args: Any, **kwargs: Any) -> Any:
    """
    Build Top 10 Investments rows.

    Sync/async tolerant by design:
      - Called from sync code -> runs `asyncio.run(...)` and returns a dict.
      - Called from async code -> returns an awaitable (coroutine) that must
        be awaited.

    WARNING: the return type varies with caller context. When calling from
    async code, use `await build_top10_rows(...)`. If you want a clean
    always-awaitable contract, call `build_top10_rows_async(...)` directly.

    Examples:
        # Sync
        result = build_top10_rows(engine=my_engine, limit=10)

        # Async -- both work
        result = await build_top10_rows(engine=my_engine, limit=10)
        result = await build_top10_rows_async(engine=my_engine, limit=10)
    """
    coro = build_top10_rows_async(*args, **kwargs)
    try:
        asyncio.get_running_loop()
        return coro
    except RuntimeError:
        return asyncio.run(coro)


def select_top10_symbols(*args: Any, **kwargs: Any) -> Any:
    """
    Extract just the selected symbols from a Top10 build.

    Sync/async tolerant (same contract as build_top10_rows).
    """
    async def _inner() -> List[str]:
        payload = await build_top10_rows_async(*args, **kwargs)
        rows = payload.get("rows") if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            return []
        result: List[str] = []
        for row in rows:
            if isinstance(row, dict):
                sym = _normalize_symbol(row.get("symbol") or row.get("ticker"))
                if sym:
                    result.append(sym)
        return _dedupe_keep_order(result)

    try:
        asyncio.get_running_loop()
        return _inner()
    except RuntimeError:
        return asyncio.run(_inner())


# ---------------------------------------------------------------------------
# Convenience Aliases (preserved from v5.0.0 for backward compatibility)
# ---------------------------------------------------------------------------

build_top10_output_rows = build_top10_rows
build_top10_investments_rows = build_top10_rows
build_top_10_investments_rows = build_top10_rows
build_top10 = build_top10_rows
build_top10_investments = build_top10_rows
build_top10_output = build_top10_rows
build_top10_payload = build_top10_rows
get_top10_rows = build_top10_rows
select_top10 = build_top10_rows


# ---------------------------------------------------------------------------
# Module Exports
# ---------------------------------------------------------------------------

__all__ = [
    "TOP10_SELECTOR_VERSION",
    "build_top10_rows",
    "build_top10_rows_async",              # added in v6.0.0: always-async contract
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
    # Public helpers used by insights_builder and tests
    "CriteriaContext",
    "Top10Config",
    "normalize_candidate_row",
    "merge_rows",
    "merge_symbol_rows",
    "choose_horizon_roi",
    "compute_selector_score",
    "compute_data_quality_tier",            # v6.1.0
    "compute_trade_setup",
    "passes_filters",
    "resolve_engine",
]
