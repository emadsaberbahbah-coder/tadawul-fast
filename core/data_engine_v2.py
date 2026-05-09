#!/usr/bin/env python3
# core/data_engine_v2.py
"""
================================================================================
Data Engine V2 — GLOBAL-FIRST ORCHESTRATOR — v5.53.0
================================================================================

WHY v5.53.0
-----------
[BUG FIXES] Four production bugs identified from the May 8, 2026
spreadsheet audit, all confined to this file:

  [BUG-1] Expected ROI 1M / 3M / 12M was producing identical
      0.33% / 3% / 8% on every ticker.  Root cause in
      _compute_scores_fallback: when seed_best_roi was falsy (the
      common case for rows with no upstream forecast), drift fell
      back to flat (1.0, 3.0, 8.0) which divided by (300, 100, 100)
      produced exactly those values.  Fix: replace the flat defaults
      with a CAPM + momentum tilt + sqrt-time + 30D-vol-dampener
      forecast.  Tickers with different beta, momentum, or
      volatility now produce different ROIs as they should.

  [BUG-2] Upside % was clamped at exactly -70% / +200% on dozens
      of rows (most JSE / HKD / INR tickers).  Root cause in
      _compute_intrinsic_and_upside: a hard symmetric clamp on
      intrinsic_value at (price * 0.3, price * 3.0) was masking
      DCF / currency-unit bugs (e.g. JSE quotes are in ZAC but the
      DCF was producing ZAR, yielding upside that hit the -70%
      floor every time).  Fix: removed the symmetric clamp; only
      floor at zero so we don't store negatives.  Extreme values
      now surface as real signal so the underlying bug is
      diagnosable instead of hidden.

  [BUG-3] 52W Position % column rendered blank in the sheet on
      every row.  Root cause: stored as a fraction (0.0 - 1.0) in
      both _canonicalize_provider_row and _compute_history_patch_from_rows,
      but the column header reads "52W Position %" and the display
      layer expects percent points (0 - 100).  Fix: store as
      percent points in both code paths.

  [BUG-4] Unrealized P/L % rendered as 4,614,849.94% on EXE.US.
      Same fraction-vs-percent-points mismatch as BUG-3, this time
      in the position-P/L block of _canonicalize_provider_row.
      Fix: store as percent points so the value reads as a plain
      percent number.

All four fixes are surgical replacements of the offending
arithmetic inside two functions (_compute_scores_fallback and
_canonicalize_provider_row) plus the removal of one clamp in
_compute_intrinsic_and_upside.  No alias maps changed.  No schema
fields added or removed.  No public API affected.  v5.52.0 callers
continue to work unchanged; the only observable difference is
that the four columns above now carry meaningful values instead
of fakes / blanks / millions-percent.

WHY v5.52.0
-----------
[BUG FIXES] Six fixes that close a single-pattern bug surfaced by the AAPL/
Global_Markets production verification of v5.51.0. Real EODHD enrichment was
flowing (price, market cap, fundamentals, scores all populated) but seven
schema fields were systematically null across all symbols:
    fundamental_view, technical_view, risk_view, value_view,
    forecast_confidence, confidence_score, recommendation_reason

Root cause: `_normalize_to_schema_keys` populates the row with EVERY canonical
schema key, setting absent values to None. By the time `_compute_scores_fallback`
and `_compute_recommendation` run, the row dict ALREADY contains
`{"fundamental_view": None, ...}`. The helpers used `row.setdefault(key, value)`
which is a no-op when the key is present (even when its value is None). So the
computed values for views, forecast_confidence, confidence_score, and
recommendation_reason were silently discarded. The fields that did populate
(recommendation, recommendation_detailed, recommendation_priority,
confidence_bucket) used direct `row[k] = v` assignment or a falsy check, which
works against None-valued keys.

v5.52.0 fixes (all defensive, no business logic changes):

  [FIX-7]  _compute_recommendation: 4 view tokens (fundamental_view,
      technical_view, risk_view, value_view) and recommendation_reason now
      use `if row.get(k) is None: row[k] = ...` instead of setdefault. The
      computed values reach the wire.

  [FIX-8]  _compute_scores_fallback: forecast_confidence and confidence_score
      same fix. Confidence cascade now populates end-to-end.

  [FIX-9]  _apply_page_row_backfill: remaining setdefault calls swapped to
      the conditional-assignment pattern for idempotency regardless of
      whether backfill runs before or after schema normalization.

  [FIX-10] _compute_scores_fallback: NEW peg_ratio computation. EODHD
      returns pe_ttm and revenue_growth_yoy directly; v5.51.0 never computed
      the ratio. PEG = pe_ttm / (revenue_growth_yoy_pct) when both available
      and growth positive. Sanity-clamped to [0, 10].

  [FIX-11] _compute_recommendation: recommendation_reason now enriched with
      view-summary detail (`Fund {VIEW} | Tech {VIEW} | ...`) — was always
      computed in v5.51.0 but the setdefault prevented it from reaching the
      row.

  [FIX-12] _build_top10_rows_fallback: per-page get_page_rows call wrapped
      in try/except so one bad source page can't kill the whole Top10
      build. Defensive parity with v5.51.0 FIX-1.

All fixes are surgical setdefault-to-conditional swaps plus the additive
peg_ratio block. No public API changes. No schema changes. No alias map
changes. v5.51.0 callers continue to work unchanged. Field names preserved.

================================================================================

WHY v5.51.0
-----------
[HARDENING] Six defensive-programming fixes that close silent-failure paths
identified during the v4.3.4 advanced_analysis route diagnostic. See
preserved comments in code for the full per-fix rationale.

WHY v5.50.0
-----------
[DECISION MATRIX] 8-tier `recommendation_detailed` framework alongside the
canonical 5-tier `recommendation` field.  Adds optional candlestick
pattern detection.

WHY v5.49.x
-----------
[Wave 3 Insights group] sector_relative_score, conviction_score,
top_factors, top_risks, position_size_hint added to the canonical
schema.

WHY v5.47.x
-----------
[v5.47.4] country/currency resolvers delegate to core.symbols.normalize.
[v5.47.3] percent fields stored as fractions.  NOTE (v5.53.0): two of those
          fields (week_52_position_pct, unrealized_pl_pct) are now stored
          as percent points to match column-header conventions.
[v5.47.2] page-aware provider priority, provider-profile-aware caches.

Design goals
------------
- Never fail import because an optional module is missing.
- Never return an empty schema for a known page.
- Prefer live or external rows when available.
- Preserve schema-first contracts for route stability.
- Keep payloads JSON-safe and route-tolerant.
================================================================================
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import os
import re
import sys
import time
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from enum import Enum
from importlib import import_module
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

try:
    from pydantic import BaseModel, ConfigDict
except Exception:  # pragma: no cover
    class BaseModel:  # type: ignore
        def __init__(self, **data: Any) -> None:
            self.__dict__.update(data)

        def model_dump(self, mode: str = "python") -> Dict[str, Any]:
            return dict(self.__dict__)

    def ConfigDict(**kwargs: Any) -> Dict[str, Any]:  # type: ignore
        return dict(kwargs)

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

__version__ = "5.53.0"

logger = logging.getLogger("core.data_engine_v2")
logger.addHandler(logging.NullHandler())


# =============================================================================
# v5.47.4: optional symbol metadata helpers from core.symbols.normalize v7.1.0
# =============================================================================
try:
    from core.symbols.normalize import (  # type: ignore
        get_country_from_symbol as _ext_get_country_from_symbol,
        get_currency_from_symbol as _ext_get_currency_from_symbol,
    )
    _HAS_NORMALIZE_HELPERS = True
except Exception:  # pragma: no cover
    _ext_get_country_from_symbol = None  # type: ignore
    _ext_get_currency_from_symbol = None  # type: ignore
    _HAS_NORMALIZE_HELPERS = False


# =============================================================================
# v5.50.0: optional candlestick pattern detection from core.candlesticks v1.0.0
# =============================================================================
try:
    from core.candlesticks import detect_patterns as _detect_candle_patterns  # type: ignore
    _HAS_CANDLESTICKS = True
except Exception:  # pragma: no cover
    _detect_candle_patterns = None  # type: ignore
    _HAS_CANDLESTICKS = False

_CANDLESTICK_FIELD_KEYS: Tuple[str, ...] = (
    "candlestick_pattern",
    "candlestick_signal",
    "candlestick_strength",
    "candlestick_confidence",
    "candlestick_patterns_recent",
)


_FALLBACK_COUNTRY_BY_SUFFIX: Dict[str, str] = {
    ".SR": "Saudi Arabia", ".SAU": "Saudi Arabia", ".TADAWUL": "Saudi Arabia",
    ".US": "United States", ".N": "United States", ".NASDAQ": "United States",
    ".NYSE": "United States", ".OQ": "United States",
    ".L": "United Kingdom", ".LSE": "United Kingdom", ".LN": "United Kingdom",
    ".DE": "Germany", ".F": "Germany", ".BE": "Germany",
    ".PA": "France", ".FP": "France",
    ".SW": "Switzerland", ".VX": "Switzerland",
    ".AS": "Netherlands", ".BR": "Belgium", ".MC": "Spain",
    ".MI": "Italy", ".IM": "Italy",
    ".CO": "Denmark", ".ST": "Sweden", ".OL": "Norway", ".HE": "Finland",
    ".AT": "Austria", ".VI": "Austria", ".IR": "Ireland",
    ".T": "Japan", ".TYO": "Japan",
    ".HK": "Hong Kong", ".HKG": "Hong Kong",
    ".SS": "China", ".SZ": "China",
    ".NS": "India", ".BO": "India", ".NSE": "India", ".BSE": "India",
    ".KS": "South Korea", ".KQ": "South Korea",
    ".TW": "Taiwan",
    ".SI": "Singapore", ".SGX": "Singapore",
    ".KL": "Malaysia", ".JK": "Indonesia", ".BK": "Thailand",
    ".AX": "Australia", ".ASX": "Australia", ".NZ": "New Zealand",
    ".TO": "Canada", ".V": "Canada",
    ".SA": "Brazil", ".BA": "Argentina", ".MX": "Mexico",
    ".AE": "United Arab Emirates", ".DFM": "United Arab Emirates", ".ADX": "United Arab Emirates",
    ".QA": "Qatar", ".KW": "Kuwait", ".EG": "Egypt",
    ".JSE": "South Africa", ".ZA": "South Africa",
    ".TA": "Israel", ".TASE": "Israel",
}

_FALLBACK_CURRENCY_BY_SUFFIX: Dict[str, str] = {
    ".SR": "SAR", ".SAU": "SAR", ".TADAWUL": "SAR",
    ".US": "USD", ".N": "USD", ".NASDAQ": "USD", ".NYSE": "USD", ".OQ": "USD",
    ".L": "GBP", ".LSE": "GBP", ".LN": "GBP",
    ".DE": "EUR", ".F": "EUR", ".BE": "EUR",
    ".PA": "EUR", ".FP": "EUR",
    ".AS": "EUR", ".BR": "EUR", ".MC": "EUR",
    ".MI": "EUR", ".IM": "EUR", ".AT": "EUR", ".VI": "EUR", ".IR": "EUR",
    ".HE": "EUR",
    ".SW": "CHF", ".VX": "CHF",
    ".CO": "DKK", ".ST": "SEK", ".OL": "NOK",
    ".T": "JPY", ".TYO": "JPY",
    ".HK": "HKD", ".HKG": "HKD",
    ".SS": "CNY", ".SZ": "CNY",
    ".NS": "INR", ".BO": "INR", ".NSE": "INR", ".BSE": "INR",
    ".KS": "KRW", ".KQ": "KRW",
    ".TW": "TWD",
    ".SI": "SGD", ".SGX": "SGD",
    ".KL": "MYR", ".JK": "IDR", ".BK": "THB",
    ".AX": "AUD", ".ASX": "AUD", ".NZ": "NZD",
    ".TO": "CAD", ".V": "CAD",
    ".SA": "BRL", ".BA": "ARS", ".MX": "MXN",
    ".AE": "AED", ".DFM": "AED", ".ADX": "AED",
    ".QA": "QAR", ".KW": "KWD", ".EG": "EGP",
    ".JSE": "ZAR", ".ZA": "ZAR",
    ".TA": "ILS", ".TASE": "ILS",
}


# =============================================================================
# Minimal domain models
# =============================================================================
class QuoteQuality(str, Enum):
    GOOD = "good"
    FAIR = "fair"
    MISSING = "missing"


class DataSource(str, Enum):
    ENGINE_V2 = "engine_v2"
    EXTERNAL_ROWS = "external_rows"
    SNAPSHOT = "snapshot"
    FALLBACK = "fallback"


class UnifiedQuote(BaseModel):
    model_config = ConfigDict(extra="allow")


# =============================================================================
# v5.50.0: Decision Matrix 8-tier framework
# =============================================================================
DETAILED_TOKENS: Tuple[str, ...] = (
    "STRONG_SELL", "STRONG_BUY", "SPECULATIVE_BUY", "BUY",
    "ACCUMULATE", "SELL", "REDUCE", "HOLD",
)

CANONICAL_TOKENS: Tuple[str, ...] = (
    "STRONG_BUY", "BUY", "HOLD", "REDUCE", "SELL",
)

_RECOMMENDATION_COLLAPSE_MAP: Dict[str, str] = {
    "STRONG_SELL": "SELL",
    "STRONG_BUY": "STRONG_BUY",
    "SPECULATIVE_BUY": "BUY",
    "BUY": "BUY",
    "ACCUMULATE": "BUY",
    "SELL": "SELL",
    "REDUCE": "REDUCE",
    "HOLD": "HOLD",
}

_DETAILED_RULE_LABELS: Dict[str, str] = {
    "STRONG_SELL": "Critical Risk",
    "STRONG_BUY": "Golden Setup",
    "SPECULATIVE_BUY": "High Beta/Growth",
    "BUY": "Core Position",
    "ACCUMULATE": "Value Play",
    "SELL": "Fundamental Failure",
    "REDUCE": "Exit Strategy",
    "HOLD": "Neutral",
}


def _classify_8tier(overall: float, conf: float, risk: float) -> Tuple[str, int, str]:
    """v5.50.0 Decision Matrix classifier. Returns (detailed_token, priority, rule_label)."""
    if risk >= 90:
        return "STRONG_SELL", 1, _DETAILED_RULE_LABELS["STRONG_SELL"]
    if overall >= 80 and conf >= 75 and risk <= 50:
        return "STRONG_BUY", 2, _DETAILED_RULE_LABELS["STRONG_BUY"]
    if overall >= 75 and conf >= 50 and 51 <= risk <= 84:
        return "SPECULATIVE_BUY", 3, _DETAILED_RULE_LABELS["SPECULATIVE_BUY"]
    if overall >= 70 and conf >= 60 and risk <= 65:
        return "BUY", 4, _DETAILED_RULE_LABELS["BUY"]
    if 55 <= overall <= 69 and conf >= 65 and risk <= 55:
        return "ACCUMULATE", 5, _DETAILED_RULE_LABELS["ACCUMULATE"]
    if overall <= 20 or (overall <= 35 and risk >= 80):
        return "SELL", 6, _DETAILED_RULE_LABELS["SELL"]
    if overall <= 35 or risk >= 70:
        return "REDUCE", 7, _DETAILED_RULE_LABELS["REDUCE"]
    return "HOLD", 8, _DETAILED_RULE_LABELS["HOLD"]


def collapse_to_canonical(detailed_token: str) -> str:
    """Public helper: collapse 8-tier detailed token to canonical 5-tier."""
    return _RECOMMENDATION_COLLAPSE_MAP.get(_safe_str(detailed_token), "HOLD")


# =============================================================================
# Canonical page contracts
# =============================================================================
INSTRUMENT_CANONICAL_KEYS: List[str] = [
    "symbol", "name", "asset_class", "exchange", "currency", "country",
    "sector", "industry", "current_price", "previous_close", "open_price",
    "day_high", "day_low", "week_52_high", "week_52_low", "price_change",
    "percent_change", "week_52_position_pct", "volume", "avg_volume_10d",
    "avg_volume_30d", "market_cap", "float_shares", "beta_5y", "pe_ttm",
    "pe_forward", "eps_ttm", "dividend_yield", "payout_ratio", "revenue_ttm",
    "revenue_growth_yoy", "gross_margin", "operating_margin", "profit_margin",
    "debt_to_equity", "free_cash_flow_ttm", "rsi_14", "volatility_30d",
    "volatility_90d", "max_drawdown_1y", "var_95_1d", "sharpe_1y", "risk_score",
    "risk_bucket", "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio",
    "intrinsic_value", "upside_pct", "valuation_score", "forecast_price_1m",
    "forecast_price_3m", "forecast_price_12m", "expected_roi_1m",
    "expected_roi_3m", "expected_roi_12m", "forecast_confidence",
    "confidence_score", "confidence_bucket", "value_score", "quality_score",
    "momentum_score", "growth_score", "overall_score", "fundamental_view",
    "technical_view", "risk_view", "value_view", "opportunity_score",
    "rank_overall", "recommendation", "recommendation_reason", "horizon_days",
    "invest_period_label", "position_qty", "avg_cost", "position_cost",
    "position_value", "unrealized_pl", "unrealized_pl_pct", "data_provider",
    "last_updated_utc", "last_updated_riyadh", "warnings",
    "sector_relative_score", "conviction_score", "top_factors", "top_risks",
    "position_size_hint",
    "recommendation_detailed", "recommendation_priority",
    "candlestick_pattern", "candlestick_signal", "candlestick_strength",
    "candlestick_confidence", "candlestick_patterns_recent",
]

INSTRUMENT_CANONICAL_HEADERS: List[str] = [
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country",
    "Sector", "Industry", "Current Price", "Previous Close", "Open",
    "Day High", "Day Low", "52W High", "52W Low", "Price Change",
    "Percent Change", "52W Position %", "Volume", "Avg Volume 10D",
    "Avg Volume 30D", "Market Cap", "Float Shares", "Beta (5Y)", "P/E (TTM)",
    "P/E (Forward)", "EPS (TTM)", "Dividend Yield", "Payout Ratio",
    "Revenue (TTM)", "Revenue Growth YoY", "Gross Margin", "Operating Margin",
    "Profit Margin", "Debt/Equity", "Free Cash Flow (TTM)", "RSI (14)",
    "Volatility 30D", "Volatility 90D", "Max Drawdown 1Y", "VaR 95% (1D)",
    "Sharpe (1Y)", "Risk Score", "Risk Bucket", "P/B", "P/S", "EV/EBITDA",
    "PEG", "Intrinsic Value", "Upside %", "Valuation Score",
    "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M",
    "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M",
    "Forecast Confidence", "Confidence Score", "Confidence Bucket",
    "Value Score", "Quality Score", "Momentum Score", "Growth Score",
    "Overall Score", "Fundamental View", "Technical View", "Risk View",
    "Value View", "Opportunity Score", "Rank (Overall)", "Recommendation",
    "Recommendation Reason", "Horizon Days", "Invest Period Label",
    "Position Qty", "Avg Cost", "Position Cost", "Position Value",
    "Unrealized P/L", "Unrealized P/L %", "Data Provider",
    "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",
    "Sector-Adj Score", "Conviction Score", "Top Factors", "Top Risks",
    "Position Size Hint",
    "Recommendation Detail", "Reco Priority",
    "Candle Pattern", "Candle Signal", "Candle Strength",
    "Candle Confidence", "Recent Patterns (5D)",
]

TOP10_REQUIRED_FIELDS: Tuple[str, ...] = (
    "top10_rank", "selection_reason", "criteria_snapshot",
)
TOP10_REQUIRED_HEADERS: Dict[str, str] = {
    "top10_rank": "Top10 Rank",
    "selection_reason": "Selection Reason",
    "criteria_snapshot": "Criteria Snapshot",
}

INSIGHTS_HEADERS: List[str] = ["Section", "Item", "Metric", "Value", "Notes", "Source", "Sort Order"]
INSIGHTS_KEYS: List[str] = ["section", "item", "metric", "value", "notes", "source", "sort_order"]

DATA_DICTIONARY_HEADERS: List[str] = ["Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"]
DATA_DICTIONARY_KEYS: List[str] = ["sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes"]

STATIC_CANONICAL_SHEET_CONTRACTS: Dict[str, Dict[str, List[str]]] = {
    "Market_Leaders": {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "Global_Markets": {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "Commodities_FX": {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "Mutual_Funds": {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "My_Portfolio": {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "My_Investments": {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "Top_10_Investments": {
        "headers": list(INSTRUMENT_CANONICAL_HEADERS) + [TOP10_REQUIRED_HEADERS[k] for k in TOP10_REQUIRED_FIELDS],
        "keys": list(INSTRUMENT_CANONICAL_KEYS) + list(TOP10_REQUIRED_FIELDS),
    },
    "Insights_Analysis": {"headers": list(INSIGHTS_HEADERS), "keys": list(INSIGHTS_KEYS)},
    "Data_Dictionary": {"headers": list(DATA_DICTIONARY_HEADERS), "keys": list(DATA_DICTIONARY_KEYS)},
}

INSTRUMENT_SHEETS: Set[str] = {
    "Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds",
    "My_Portfolio", "My_Investments", "Top_10_Investments",
}
SPECIAL_SHEETS: Set[str] = {"Insights_Analysis", "Data_Dictionary"}

TOP10_ENGINE_DEFAULT_PAGES: List[str] = [
    "Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds",
    "My_Portfolio", "My_Investments",
]

EMERGENCY_PAGE_SYMBOLS: Dict[str, List[str]] = {
    "Market_Leaders": ["2222.SR", "1120.SR", "2010.SR", "7010.SR", "AAPL", "MSFT", "NVDA", "GOOGL"],
    "Global_Markets": ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AVGO"],
    "Commodities_FX": ["GC=F", "BZ=F", "SI=F", "EURUSD=X", "GBPUSD=X", "JPY=X", "SAR=X", "CL=F"],
    "Mutual_Funds": ["SPY", "QQQ", "VTI", "VOO", "IWM"],
    "My_Portfolio": ["2222.SR", "AAPL", "MSFT"],
    "My_Investments": ["2222.SR", "AAPL", "MSFT"],
    "Top_10_Investments": ["2222.SR", "1120.SR", "AAPL", "MSFT", "NVDA"],
}

PAGE_SYMBOL_ENV_KEYS: Dict[str, str] = {
    "Market_Leaders": "MARKET_LEADERS_SYMBOLS",
    "Global_Markets": "GLOBAL_MARKETS_SYMBOLS",
    "Commodities_FX": "COMMODITIES_FX_SYMBOLS",
    "Mutual_Funds": "MUTUAL_FUNDS_SYMBOLS",
    "My_Portfolio": "MY_PORTFOLIO_SYMBOLS",
    "My_Investments": "MY_INVESTMENTS_SYMBOLS",
    "Top_10_Investments": "TOP10_FALLBACK_SYMBOLS",
}

DEFAULT_PROVIDERS = ["eodhd", "yahoo", "finnhub"]
DEFAULT_KSA_PROVIDERS = ["tadawul", "argaam", "yahoo"]
DEFAULT_GLOBAL_PROVIDERS = ["eodhd", "yahoo", "finnhub"]
NON_KSA_EODHD_PRIMARY_PAGES = {"Global_Markets", "Commodities_FX", "Mutual_Funds"}
PAGE_PRIMARY_PROVIDER_DEFAULTS = {page: "eodhd" for page in NON_KSA_EODHD_PRIMARY_PAGES}
PROVIDER_PRIORITIES = {
    "tadawul": 10, "argaam": 20, "eodhd": 30,
    "yahoo": 40, "finnhub": 50, "yahoo_chart": 60,
}


# =============================================================================
# Small helpers
# =============================================================================
def _safe_str(x: Any, default: str = "") -> str:
    if x is None:
        return default
    try:
        s = str(x).strip()
        return s if s else default
    except Exception:
        return default


def _norm_key(x: Any) -> str:
    s = _safe_str(x).lower()
    if not s:
        return ""
    s = s.replace("-", "_").replace("/", "_").replace("&", "_")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"__+", "_", s).strip("_")
    return s


def _norm_key_loose(x: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", _safe_str(x).lower())


def _safe_bool(x: Any, default: bool = False) -> bool:
    if isinstance(x, bool):
        return x
    s = _safe_str(x).lower()
    if s in {"1", "true", "yes", "y", "on", "t"}:
        return True
    if s in {"0", "false", "no", "n", "off", "f"}:
        return False
    return default


def _safe_int(x: Any, default: int = 0, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        v = int(float(x))
    except Exception:
        v = int(default)
    if lo is not None:
        v = max(lo, v)
    if hi is not None:
        v = min(hi, v)
    return v


def _as_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _as_pct_fraction(x: Any) -> Optional[float]:
    """Coerce a percent-like value into a FRACTION (0.05 = 5%)."""
    v = _as_float(x)
    if v is None:
        return None
    if abs(v) > 1.5:
        return v / 100.0
    return v


def _as_pct_points(x: Any) -> Optional[float]:
    """Coerce a percent-like value into PERCENT POINTS (5.0 = 5%)."""
    v = _as_float(x)
    if v is None:
        return None
    return v * 100.0 if abs(v) <= 1.5 else v


def _dedupe_keep_order(items: Sequence[Any]) -> List[Any]:
    out: List[Any] = []
    seen: Set[Any] = set()
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _page_catalog_candidates() -> List[Any]:
    modules: List[Any] = []
    for mod_path in ("core.sheets.page_catalog", "sheets.page_catalog"):
        try:
            modules.append(import_module(mod_path))
        except Exception:
            continue
    return modules


def _page_catalog_canonical_name(name: str) -> str:
    raw = _safe_str(name)
    if not raw:
        return ""

    for mod in _page_catalog_candidates():
        for fn_name in ("canonicalize_page_name", "normalize_page_name", "get_canonical_page_name", "canonical_page_name"):
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                for args, kwargs in (((raw,), {}), ((), {"page": raw}), ((), {"name": raw}), ((), {"sheet": raw})):
                    try:
                        val = fn(*args, **kwargs)
                    except TypeError:
                        continue
                    except Exception:
                        continue
                    text = _safe_str(val)
                    if text:
                        return text

        for attr_name in ("PAGE_ALIASES", "SHEET_ALIASES", "ALIASES", "PAGE_NAME_ALIASES"):
            mapping = getattr(mod, attr_name, None)
            if isinstance(mapping, dict):
                for cand in (raw, raw.replace(" ", "_"), raw.replace("-", "_"), _norm_key(raw), _norm_key_loose(raw)):
                    for key, val in mapping.items():
                        if cand in {_safe_str(key), _norm_key(_safe_str(key)), _norm_key_loose(_safe_str(key))}:
                            text = _safe_str(val)
                            if text:
                                return text
    return ""


def _canonicalize_sheet_name(name: str) -> str:
    raw = _safe_str(name)
    if not raw:
        return ""

    candidates = [raw, raw.replace(" ", "_"), raw.replace("-", "_"), _norm_key(raw)]
    known = {k: k for k in STATIC_CANONICAL_SHEET_CONTRACTS.keys()}
    by_norm = {_norm_key(k): k for k in STATIC_CANONICAL_SHEET_CONTRACTS.keys()}
    by_loose = {_norm_key_loose(k): k for k in STATIC_CANONICAL_SHEET_CONTRACTS.keys()}

    for cand in candidates:
        if cand in known:
            return known[cand]
        nk = _norm_key(cand)
        if nk in by_norm:
            return by_norm[nk]
        nkl = _norm_key_loose(cand)
        if nkl in by_loose:
            return by_loose[nkl]

    page_catalog_name = _page_catalog_canonical_name(raw)
    if page_catalog_name:
        pc_candidates = [page_catalog_name, page_catalog_name.replace(" ", "_"), _norm_key(page_catalog_name), _norm_key_loose(page_catalog_name)]
        for cand in pc_candidates:
            if cand in known:
                return known[cand]
            if _norm_key(cand) in by_norm:
                return by_norm[_norm_key(cand)]
            if _norm_key_loose(cand) in by_loose:
                return by_loose[_norm_key_loose(cand)]
        return page_catalog_name.replace(" ", "_")

    return raw.replace(" ", "_")


def _sheet_lookup_candidates(sheet: str) -> List[str]:
    s = _canonicalize_sheet_name(sheet)
    vals = [s, s.replace("_", " "), s.lower(), _norm_key(s), _norm_key_loose(s)]
    return [v for v in _dedupe_keep_order(vals) if _safe_str(v)]


def _looks_like_symbol_token(x: Any) -> bool:
    s = _safe_str(x)
    if not s:
        return False
    if len(s) > 24:
        return False
    if re.match(r"^[A-Z0-9.=\-:^/]{1,24}$", s):
        return True
    if re.match(r"^[0-9]{4}(\.SR)?$", s):
        return True
    return False


def normalize_symbol(symbol: str) -> str:
    return _safe_str(symbol).upper()


def get_symbol_info(symbol: str) -> Dict[str, Any]:
    s = normalize_symbol(symbol)
    return {
        "requested": _safe_str(symbol),
        "normalized": s,
        "is_ksa": s.endswith(".SR") or re.match(r"^[0-9]{4}$", s) is not None,
    }


def _split_symbols(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        out: List[str] = []
        for v in value:
            out.extend(_split_symbols(v))
        return out
    s = _safe_str(value)
    if not s:
        return []
    parts = re.split(r"[,;|\s]+", s)
    return [p.strip() for p in parts if p.strip()]


def _normalize_symbol_list(symbols: Iterable[Any], limit: int = 5000) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for item in symbols:
        s = normalize_symbol(_safe_str(item))
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
        if len(out) >= limit:
            break
    return out


def _extract_nested_dict(payload: Dict[str, Any], key: str) -> Dict[str, Any]:
    val = payload.get(key)
    return dict(val) if isinstance(val, dict) else {}


def _extract_requested_symbols_from_body(body: Optional[Dict[str, Any]], limit: int = 5000) -> List[str]:
    if not isinstance(body, dict):
        return []
    raw: List[str] = []
    for key in (
        "symbols", "tickers", "selected_symbols", "direct_symbols", "codes",
        "watchlist", "portfolio_symbols", "symbol", "ticker", "code", "requested_symbol",
    ):
        raw.extend(_split_symbols(body.get(key)))
    criteria = body.get("criteria")
    if isinstance(criteria, dict):
        for key in ("symbols", "tickers", "selected_symbols", "direct_symbols", "codes", "symbol", "ticker", "code"):
            raw.extend(_split_symbols(criteria.get(key)))
    return _normalize_symbol_list(raw, limit=limit)


def _merge_route_body_dicts(*parts: Any) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    for part in parts:
        if part is None:
            continue
        if isinstance(part, Mapping):
            for k, v in part.items():
                key = _safe_str(k)
                if not key:
                    continue
                merged[key] = v
            continue
        try:
            if hasattr(part, "multi_items") and callable(getattr(part, "multi_items")):
                for k, v in part.multi_items():
                    key = _safe_str(k)
                    if key:
                        merged[key] = v
                continue
        except Exception:
            pass
        try:
            if hasattr(part, "items") and callable(getattr(part, "items")):
                for k, v in part.items():
                    key = _safe_str(k)
                    if key:
                        merged[key] = v
                continue
        except Exception:
            pass
        try:
            d = _model_to_dict(part)
            if isinstance(d, dict) and d:
                for k, v in d.items():
                    key = _safe_str(k)
                    if key:
                        merged[key] = v
        except Exception:
            continue
    return merged


def _extract_request_route_parts(request: Any) -> Dict[str, Any]:
    if request is None:
        return {}
    out: Dict[str, Any] = {}
    for attr in ("query_params", "path_params"):
        try:
            part = getattr(request, attr, None)
        except Exception:
            part = None
        if part is not None:
            out.update(_merge_route_body_dicts(part))
    try:
        state = getattr(request, "state", None)
        if state is not None:
            for attr in ("payload", "body", "json", "data", "params"):
                val = getattr(state, attr, None)
                if isinstance(val, Mapping):
                    out.update(_merge_route_body_dicts(val))
    except Exception:
        pass
    return out


def _normalize_route_call_inputs(
    *,
    page: Optional[str] = None,
    sheet: Optional[str] = None,
    sheet_name: Optional[str] = None,
    limit: int = 2000,
    offset: int = 0,
    mode: str = "",
    body: Optional[Dict[str, Any]] = None,
    extras: Optional[Dict[str, Any]] = None,
) -> Tuple[str, int, int, str, Dict[str, Any], Dict[str, Any]]:
    extras = dict(extras or {})
    request_parts = _extract_request_route_parts(extras.get("request"))

    merged_body = _merge_route_body_dicts(
        request_parts,
        extras.get("params"),
        extras.get("query"),
        extras.get("query_params"),
        extras.get("payload"),
        extras.get("data"),
        extras.get("json"),
        extras.get("body"),
        body,
        extras,
    )

    target_raw = (
        page
        or sheet
        or sheet_name
        or _safe_str(merged_body.get("page"))
        or _safe_str(merged_body.get("sheet"))
        or _safe_str(merged_body.get("sheet_name"))
        or _safe_str(merged_body.get("page_name"))
        or _safe_str(merged_body.get("name"))
        or _safe_str(merged_body.get("tab"))
        or _safe_str(merged_body.get("worksheet"))
        or _safe_str(merged_body.get("sheetName"))
        or _safe_str(merged_body.get("pageName"))
        or _safe_str(merged_body.get("worksheet_name"))
        or "Market_Leaders"
    )

    effective_limit = _safe_int(
        merged_body.get("limit", limit), default=limit, lo=1, hi=5000,
    )
    if effective_limit <= 0:
        effective_limit = max(1, min(5000, int(limit or 2000)))

    effective_offset = _safe_int(merged_body.get("offset", offset), default=offset, lo=0)
    effective_mode = _safe_str(merged_body.get("mode") or mode)

    passthrough = {
        k: v for k, v in merged_body.items()
        if k not in {"request", "params", "query", "query_params", "payload", "data", "json", "body"}
    }
    return _canonicalize_sheet_name(target_raw) or "Market_Leaders", effective_limit, effective_offset, effective_mode, passthrough, request_parts


def _extract_top10_pages_from_body(body: Optional[Dict[str, Any]]) -> List[str]:
    if not isinstance(body, dict):
        return []
    raw: List[str] = []
    for key in ("pages_selected", "pages", "source_pages"):
        val = body.get(key)
        if isinstance(val, (list, tuple, set)):
            raw.extend([_canonicalize_sheet_name(_safe_str(v)) for v in val if _safe_str(v)])
    criteria = body.get("criteria")
    if isinstance(criteria, dict):
        val = criteria.get("pages_selected") or criteria.get("pages")
        if isinstance(val, (list, tuple, set)):
            raw.extend([_canonicalize_sheet_name(_safe_str(v)) for v in val if _safe_str(v)])
    return [p for p in _dedupe_keep_order(raw) if p]


def _normalize_top10_body_for_engine(body: Optional[Dict[str, Any]], limit: int) -> Tuple[Dict[str, Any], List[str]]:
    out = dict(body or {})
    warnings: List[str] = []
    criteria = dict(out.get("criteria") or {}) if isinstance(out.get("criteria"), dict) else {}
    if not criteria:
        criteria = {}
    if not criteria.get("top_n"):
        criteria["top_n"] = max(1, min(limit, 50))
    out["criteria"] = criteria
    out.setdefault("top_n", criteria.get("top_n"))
    return out, warnings


def _is_schema_only_body(body: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(body, dict):
        return False
    return _safe_bool(body.get("schema_only"), False) or _safe_bool(body.get("headers_only"), False)


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_riyadh_iso() -> str:
    try:
        if ZoneInfo is not None:
            return datetime.now(ZoneInfo("Asia/Riyadh")).isoformat()
    except Exception:
        pass
    return datetime.now(timezone.utc).isoformat()


def _safe_env(name: str, default: str = "") -> str:
    return _safe_str(os.getenv(name), default)


def _get_env_bool(name: str, default: bool = False) -> bool:
    return _safe_bool(os.getenv(name), default)


def _get_env_int(name: str, default: int) -> int:
    return _safe_int(os.getenv(name), default)


def _get_env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except Exception:
        return float(default)


def _get_env_list(name: str, default: Sequence[str]) -> List[str]:
    raw = _safe_env(name, "")
    if not raw:
        return [str(x).lower() for x in default]
    return [p.strip().lower() for p in re.split(r"[,;|\s]+", raw) if p.strip()]


def _complete_schema_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    raw_headers = list(headers or [])
    raw_keys = list(keys or [])
    max_len = max(len(raw_headers), len(raw_keys))
    hdrs: List[str] = []
    ks: List[str] = []

    for i in range(max_len):
        h = _safe_str(raw_headers[i]) if i < len(raw_headers) else ""
        k = _safe_str(raw_keys[i]) if i < len(raw_keys) else ""

        if not h and not k:
            continue
        if h and not k:
            k = _norm_key(h)
        elif k and not h:
            h = k.replace("_", " ").title()

        if not h and k:
            h = k.replace("_", " ").title()
        if h and not k:
            k = _norm_key(h)

        if h and k:
            hdrs.append(h)
            ks.append(k)

    return hdrs, ks


def _usable_contract(headers: Sequence[str], keys: Sequence[str], sheet_name: str = "") -> bool:
    if not headers or not keys:
        return False
    if len(headers) != len(keys) or len(headers) == 0:
        return False
    canon = _canonicalize_sheet_name(sheet_name)
    keyset = set(keys)
    if canon in INSTRUMENT_SHEETS - {"Top_10_Investments"}:
        if not ({"symbol", "ticker", "requested_symbol"} & keyset):
            return False
        if not ({"current_price", "price", "name"} & keyset):
            return False
    if canon == "Top_10_Investments":
        if not ({"symbol", "ticker", "requested_symbol"} & keyset):
            return False
        if not set(TOP10_REQUIRED_FIELDS).issubset(keyset):
            return False
    if canon == "Insights_Analysis":
        if not ({"section", "item", "metric", "value"} <= keyset):
            return False
    if canon == "Data_Dictionary":
        if not {"sheet", "header", "key"}.issubset(keyset):
            return False
    return True


def _ensure_top10_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    hdrs = list(headers or [])
    ks = list(keys or [])
    for field in TOP10_REQUIRED_FIELDS:
        if field not in ks:
            ks.append(field)
            hdrs.append(TOP10_REQUIRED_HEADERS[field])
    return _complete_schema_contract(hdrs, ks)


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
        return _as_float(value)
    if isinstance(value, (datetime, date, dt_time)):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:
            return str(value)
    if is_dataclass(value):
        try:
            return {str(k): _json_safe(v) for k, v in asdict(value).items()}
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
        return str(value)
    except Exception:
        return None


def _model_to_dict(obj: Any) -> Dict[str, Any]:
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
            if isinstance(d, dict):
                return d
    except Exception:
        pass
    try:
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            d = obj.dict()
            if isinstance(d, dict):
                return d
    except Exception:
        pass
    try:
        if hasattr(obj, "__dict__"):
            d = getattr(obj, "__dict__", None)
            if isinstance(d, dict):
                return dict(d)
    except Exception:
        pass
    return {"result": obj}


def _looks_like_explicit_row_dict(d: Any) -> bool:
    if not isinstance(d, dict) or not d:
        return False
    keyset = {str(k) for k in d.keys()}
    if keyset & {"symbol", "ticker", "code", "requested_symbol"}:
        return True
    if {"sheet", "header", "key"}.issubset(keyset):
        return True
    if {"top10_rank", "selection_reason"}.issubset(keyset):
        return True
    if keyset & {"section", "item", "recommendation", "overall_score"}:
        return True
    return False


def _rows_from_matrix_payload(matrix: Any, cols: Sequence[Any]) -> List[Dict[str, Any]]:
    keys = [_safe_str(c) for c in cols if _safe_str(c)]
    out: List[Dict[str, Any]] = []
    for row in matrix or []:
        if not isinstance(row, (list, tuple)):
            continue
        d: Dict[str, Any] = {}
        for i, k in enumerate(keys):
            d[k] = row[i] if i < len(row) else None
        out.append(d)
    return out


def _coerce_rows_list(out: Any) -> List[Dict[str, Any]]:
    if out is None:
        return []

    if isinstance(out, list):
        if not out:
            return []
        if isinstance(out[0], dict):
            return [dict(r) for r in out if isinstance(r, dict)]
        if isinstance(out[0], (list, tuple)):
            return []
        return [_model_to_dict(r) for r in out if _model_to_dict(r)]

    if isinstance(out, dict):
        maybe_symbol_map = True
        rows_from_map: List[Dict[str, Any]] = []
        symbol_like_keys = 0
        if out:
            for k, v in out.items():
                if not isinstance(v, dict):
                    maybe_symbol_map = False
                    break
                if not _looks_like_symbol_token(k):
                    maybe_symbol_map = False
                    break
                symbol_like_keys += 1
                row = dict(v)
                if not row.get("symbol"):
                    row["symbol"] = _safe_str(k)
                rows_from_map.append(row)
        if maybe_symbol_map and symbol_like_keys > 0 and rows_from_map:
            return rows_from_map

        for key in ("row_objects", "records", "items", "data", "quotes", "rows"):
            val = out.get(key)
            if isinstance(val, list):
                if val and isinstance(val[0], dict):
                    return [dict(r) for r in val if isinstance(r, dict)]
                if val and isinstance(val[0], (list, tuple)):
                    cols = out.get("keys") or out.get("headers") or out.get("columns") or []
                    if isinstance(cols, list) and cols:
                        return _rows_from_matrix_payload(val, cols)
            if isinstance(val, dict):
                nested_rows = _coerce_rows_list(val)
                if nested_rows:
                    return nested_rows

        rows_matrix = out.get("rows_matrix") or out.get("matrix")
        if isinstance(rows_matrix, list):
            cols = out.get("keys") or out.get("headers") or out.get("columns") or []
            if isinstance(cols, list) and cols:
                return _rows_from_matrix_payload(rows_matrix, cols)

        if _looks_like_explicit_row_dict(out):
            return [dict(out)]

        for key in ("payload", "result", "response", "output"):
            nested = out.get(key)
            nested_rows = _coerce_rows_list(nested)
            if nested_rows:
                return nested_rows

        return []

    d = _model_to_dict(out)
    return [d] if _looks_like_explicit_row_dict(d) else []


def _extract_symbols_from_rows(rows: Sequence[Dict[str, Any]], limit: int = 5000) -> List[str]:
    raw: List[str] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        for key in ("symbol", "ticker", "code", "requested_symbol", "Symbol", "Ticker", "Code"):
            v = row.get(key)
            if v:
                raw.append(str(v).strip())
                break
    return _normalize_symbol_list(raw, limit=limit)


_NULL_STRINGS: Set[str] = {"", "null", "none", "n/a", "na", "nan", "-", "--"}

_CANONICAL_FIELD_ALIASES: Dict[str, Tuple[str, ...]] = {
    "symbol": ("symbol", "ticker", "code", "requested_symbol", "regularMarketSymbol"),
    "name": ("name", "shortName", "longName", "displayName", "companyName", "fundName", "description"),
    "asset_class": ("asset_class", "assetClass", "quoteType", "assetType", "instrumentType", "securityType", "type"),
    "exchange": ("exchange", "exchangeName", "fullExchangeName", "market", "marketName", "mic", "exchangeCode"),
    "currency": ("currency", "financialCurrency", "reportingCurrency", "quoteCurrency", "baseCurrency"),
    "country": ("country", "countryName", "country_code", "countryCode", "localeCountry"),
    "sector": ("sector", "sectorDisp", "gicsSector", "industryGroup", "sectorName", "gics_sector", "Sector", "General.Sector"),
    "industry": ("industry", "industryDisp", "gicsIndustry", "category", "industryName", "Industry", "General.Industry", "industry_group"),
    "current_price": ("current_price", "currentPrice", "price", "last", "lastPrice", "latestPrice", "regularMarketPrice", "nav", "close", "adjusted_close", "adjclose", "closePrice", "last_trade_price", "regular_market_price", "price_close"),
    "previous_close": ("previous_close", "previousClose", "regularMarketPreviousClose", "prevClose", "priorClose", "close_yesterday", "previous_close_price"),
    "open_price": ("open_price", "day_open", "dayOpen", "open", "openPrice", "regularMarketOpen", "open_price_day", "dailyOpen", "sessionOpen"),
    "day_high": ("day_high", "high", "dayHigh", "regularMarketDayHigh", "sessionHigh", "highPrice", "intradayHigh", "dailyHigh"),
    "day_low": ("day_low", "low", "dayLow", "regularMarketDayLow", "sessionLow", "lowPrice", "intradayLow", "dailyLow"),
    "week_52_high": ("week_52_high", "52WeekHigh", "fiftyTwoWeekHigh", "yearHigh", "week52High"),
    "week_52_low": ("week_52_low", "52WeekLow", "fiftyTwoWeekLow", "yearLow", "week52Low"),
    "price_change": ("price_change", "change", "priceChange", "regularMarketChange", "netChange"),
    "percent_change": ("percent_change", "changePercent", "percentChange", "regularMarketChangePercent", "pctChange", "change_pct"),
    "volume": ("volume", "regularMarketVolume", "sharesTraded", "tradeVolume", "Volume", "vol", "trade_count_volume"),
    "avg_volume_10d": ("avg_volume_10d", "avg_vol_10d", "averageVolume10days", "avgVolume10Day", "avgVol10d", "averageVolume10Day", "avg_volume_10_day"),
    "avg_volume_30d": ("avg_volume_30d", "avg_vol_30d", "averageVolume", "averageDailyVolume3Month", "avgVolume3Month", "avgVol30d", "averageVolume30Day", "avg_volume_30_day"),
    "market_cap": ("market_cap", "marketCap", "marketCapitalization", "MarketCapitalization", "capitalization", "Capitalization", "market_capitalization"),
    "float_shares": ("float_shares", "floatShares", "sharesFloat", "FloatShares", "SharesFloat", "sharesOutstanding", "SharesOutstanding"),
    "beta_5y": ("beta_5y", "beta", "beta5Y", "Beta", "beta5Year"),
    "pe_ttm": ("pe_ttm", "trailingPE", "peRatio", "priceEarningsTTM", "pe", "PERatio", "PriceEarningsTTM", "peTTM"),
    "pe_forward": ("pe_forward", "forward_pe", "forwardPE", "forwardPe", "ForwardPE", "ForwardPERatio", "forwardPERatio"),
    "eps_ttm": ("eps_ttm", "trailingEps", "eps", "earningsPerShare", "epsTTM", "EarningsShare", "epsTtm", "DilutedEPSTTM"),
    "dividend_yield": ("dividend_yield", "dividendYield", "trailingAnnualDividendYield", "distributionYield", "DividendYield", "forwardAnnualDividendYield", "Yield"),
    "payout_ratio": ("payout_ratio", "payoutRatio", "PayoutRatio", "payout", "PayoutRatioTTM"),
    "revenue_ttm": ("revenue_ttm", "totalRevenue", "revenueTTM", "revenue", "RevenueTTM", "TotalRevenueTTM", "Revenue", "SalesTTM"),
    "revenue_growth_yoy": ("revenue_growth_yoy", "revenueGrowth", "revenueGrowthYoY", "revenue_yoy_growth", "RevenueGrowthYOY", "QuarterlyRevenueGrowthYOY", "revenueGrowthYoy"),
    "gross_margin": ("gross_margin", "grossMargins", "grossMargin", "GrossMargin", "GrossProfitMargin", "grossMarginTTM"),
    "operating_margin": ("operating_margin", "operatingMargins", "operatingMargin", "OperatingMargin", "OperatingMarginTTM", "operatingMarginTTM"),
    "profit_margin": ("profit_margin", "profitMargins", "profitMargin", "netMargin", "ProfitMargin", "NetProfitMargin", "profitMarginTTM"),
    "debt_to_equity": ("debt_to_equity", "d_e_ratio", "debtToEquity", "deRatio", "DebtToEquity", "TotalDebtEquity"),
    "free_cash_flow_ttm": ("free_cash_flow_ttm", "fcf_ttm", "freeCashflow", "freeCashFlow", "fcf", "FreeCashFlow", "FreeCashFlowTTM"),
    "rsi_14": ("rsi_14", "rsi", "rsi14"),
    "volatility_30d": ("volatility_30d", "volatility30d", "vol30d"),
    "volatility_90d": ("volatility_90d", "volatility90d", "vol90d"),
    "max_drawdown_1y": ("max_drawdown_1y", "maxDrawdown1y", "drawdown1y"),
    "var_95_1d": ("var_95_1d", "var95_1d", "valueAtRisk95_1d"),
    "sharpe_1y": ("sharpe_1y", "sharpe1y", "sharpeRatio"),
    "risk_score": ("risk_score",),
    "risk_bucket": ("risk_bucket",),
    "pb_ratio": ("pb_ratio", "priceToBook", "pb"),
    "ps_ratio": ("ps_ratio", "priceToSalesTrailing12Months", "ps"),
    "ev_ebitda": ("ev_ebitda", "enterpriseToEbitda", "evToEbitda"),
    "peg_ratio": ("peg_ratio", "peg", "pegRatio"),
    "intrinsic_value": ("intrinsic_value", "fairValue", "dcf", "dcfValue", "intrinsicValue"),
    "upside_pct": ("upside_pct", "upsidePct", "upside_percent", "upsidePercent", "upside", "fairValueUpside", "intrinsicUpside"),
    "valuation_score": ("valuation_score",),
    "forecast_price_1m": ("forecast_price_1m", "targetPrice1m", "priceTarget1m"),
    "forecast_price_3m": ("forecast_price_3m", "targetPrice3m", "priceTarget3m", "targetPrice", "targetMeanPrice"),
    "forecast_price_12m": ("forecast_price_12m", "targetPrice12m", "priceTarget12m", "targetMedianPrice", "targetHighPrice"),
    "expected_roi_1m": ("expected_roi_1m", "expectedReturn1m", "roi1m"),
    "expected_roi_3m": ("expected_roi_3m", "expectedReturn3m", "roi3m"),
    "expected_roi_12m": ("expected_roi_12m", "expectedReturn12m", "roi12m"),
    "forecast_confidence": ("forecast_confidence", "confidence", "confidencePct", "modelConfidence"),
    "confidence_score": ("confidence_score", "modelConfidenceScore"),
    "confidence_bucket": ("confidence_bucket",),
    "value_score": ("value_score",),
    "quality_score": ("quality_score",),
    "momentum_score": ("momentum_score",),
    "growth_score": ("growth_score",),
    "overall_score": ("overall_score", "score", "compositeScore"),
    "opportunity_score": ("opportunity_score",),
    "rank_overall": ("rank_overall", "rank", "overallRank"),
    "fundamental_view": ("fundamental_view", "fundamentalView", "fund_view", "fundView"),
    "technical_view": ("technical_view", "technicalView", "tech_view", "techView"),
    "risk_view": ("risk_view", "riskView"),
    "value_view": ("value_view", "valueView"),
    "recommendation": ("recommendation", "rating", "action", "reco", "consensus"),
    "recommendation_reason": ("recommendation_reason", "reason", "summary", "thesis", "analysis"),
    "recommendation_detailed": ("recommendation_detailed", "reco_detailed", "recommendation_full", "recommendation_8tier", "detailed_recommendation"),
    "recommendation_priority": ("recommendation_priority", "reco_priority", "priority", "decision_priority"),
    "candlestick_pattern": ("candlestick_pattern", "candle_pattern", "pattern", "candlestickPattern"),
    "candlestick_signal": ("candlestick_signal", "candle_signal", "candlestickSignal"),
    "candlestick_strength": ("candlestick_strength", "candle_strength", "candlestickStrength"),
    "candlestick_confidence": ("candlestick_confidence", "candle_confidence", "candlestickConfidence"),
    "candlestick_patterns_recent": ("candlestick_patterns_recent", "candle_patterns_recent", "recent_patterns", "candlestickPatternsRecent"),
    "horizon_days": ("horizon_days", "horizon", "days"),
    "invest_period_label": ("invest_period_label", "periodLabel", "horizonLabel"),
    "position_qty": ("position_qty", "positionQty", "qty", "quantity", "shares", "holdingQty"),
    "avg_cost": ("avg_cost", "avgCost", "averageCost", "costBasisPerShare"),
    "position_cost": ("position_cost", "positionCost", "costBasis", "totalCost"),
    "position_value": ("position_value", "marketValue", "positionValue", "holdingValue"),
    "unrealized_pl": ("unrealized_pl", "unrealizedPnL", "unrealizedPL", "profitLoss"),
    "unrealized_pl_pct": ("unrealized_pl_pct", "unrealizedPnLPct", "unrealizedPLPct"),
    "data_provider": ("data_provider", "provider", "source", "dataProvider"),
    "last_updated_utc": ("last_updated_utc", "lastUpdated", "updatedAt", "timestamp", "asOf"),
    "last_updated_riyadh": ("last_updated_riyadh",),
    "warnings": ("warnings", "warning", "messages", "errors"),
}

_COMMODITY_SYMBOL_HINTS: Tuple[str, ...] = ("GC=F", "SI=F", "BZ=F", "CL=F", "NG=F", "HG=F")
_ETF_SYMBOL_HINTS: Tuple[str, ...] = ("SPY", "QQQ", "VTI", "VOO", "IWM", "DIA", "IVV", "EFA", "EEM", "ARKK")
_ETF_DISPLAY_NAMES: Dict[str, str] = {
    "SPY": "SPDR S&P 500 ETF",
    "QQQ": "Invesco QQQ Trust",
    "VTI": "Vanguard Total Stock Market ETF",
    "VOO": "Vanguard S&P 500 ETF",
    "IWM": "iShares Russell 2000 ETF",
    "DIA": "SPDR Dow Jones Industrial Average ETF",
    "IVV": "iShares Core S&P 500 ETF",
    "EFA": "iShares MSCI EAFE ETF",
    "EEM": "iShares MSCI Emerging Markets ETF",
    "ARKK": "ARK Innovation ETF",
}
_COMMODITY_DISPLAY_NAMES: Dict[str, str] = {
    "GC=F": "Gold Futures",
    "SI=F": "Silver Futures",
    "BZ=F": "Brent Crude Futures",
    "CL=F": "WTI Crude Futures",
    "NG=F": "Natural Gas Futures",
    "HG=F": "Copper Futures",
}
_COMMODITY_INDUSTRY_HINTS: Dict[str, str] = {
    "GC=F": "Precious Metals",
    "SI=F": "Precious Metals",
    "HG=F": "Industrial Metals",
    "BZ=F": "Energy",
    "CL=F": "Energy",
    "NG=F": "Energy",
}


def _is_blank_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in _NULL_STRINGS
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def _to_scalar(value: Any) -> Any:
    if isinstance(value, (list, tuple, set)):
        seq = [v for v in value if not _is_blank_value(v)]
        if not seq:
            return None
        if all(not isinstance(v, (dict, list, tuple, set)) for v in seq):
            if len(seq) == 1:
                return seq[0]
            return "; ".join(_safe_str(v) for v in seq if _safe_str(v))
        return None
    return value


def _flatten_scalar_fields(obj: Any, out: Optional[Dict[str, Any]] = None, prefix: str = "", depth: int = 0, max_depth: int = 4) -> Dict[str, Any]:
    if out is None:
        out = {}
    if depth > max_depth or obj is None:
        return out
    if isinstance(obj, Mapping):
        for k, v in obj.items():
            key = _safe_str(k)
            if not key:
                continue
            full = f"{prefix}.{key}" if prefix else key
            if isinstance(v, Mapping):
                _flatten_scalar_fields(v, out=out, prefix=full, depth=depth + 1, max_depth=max_depth)
                continue
            if isinstance(v, (list, tuple, set)) and v and isinstance(next(iter(v)), Mapping):
                continue
            scalar = _to_scalar(v)
            if scalar is None:
                continue
            out.setdefault(key, scalar)
            out.setdefault(full, scalar)
    return out


def _lookup_alias_value(src: Mapping[str, Any], flat: Mapping[str, Any], alias: str) -> Any:
    if not alias:
        return None
    candidates = [
        alias, alias.lower(), _norm_key(alias), _norm_key_loose(alias),
        alias.replace("_", " "), alias.replace("_", "-"),
    ]
    src_ci = {str(k).strip().lower(): v for k, v in src.items()}
    src_loose = {_norm_key_loose(k): v for k, v in src.items()}
    flat_ci = {str(k).strip().lower(): v for k, v in flat.items()}
    flat_loose = {_norm_key_loose(k): v for k, v in flat.items()}
    for cand in candidates:
        if cand in src and not _is_blank_value(src.get(cand)):
            return src.get(cand)
        if cand in flat and not _is_blank_value(flat.get(cand)):
            return flat.get(cand)
        lower = cand.lower()
        if lower in src_ci and not _is_blank_value(src_ci.get(lower)):
            return src_ci.get(lower)
        if lower in flat_ci and not _is_blank_value(flat_ci.get(lower)):
            return flat_ci.get(lower)
        loose = _norm_key_loose(cand)
        if loose in src_loose and not _is_blank_value(src_loose.get(loose)):
            return src_loose.get(loose)
        if loose in flat_loose and not _is_blank_value(flat_loose.get(loose)):
            return flat_loose.get(loose)
    return None


def _infer_asset_class_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if not s:
        return ""
    if s.endswith(".SR") or re.match(r"^[0-9]{4}$", s):
        return "Equity"
    if s.endswith("=X"):
        return "FX"
    if s.endswith("=F") or s in _COMMODITY_SYMBOL_HINTS:
        return "Commodity"
    if s in {"SPY", "QQQ", "VTI", "VOO", "IWM", "DIA"}:
        return "ETF"
    return "Equity"


def _infer_exchange_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if s.endswith(".SR") or re.match(r"^[0-9]{4}$", s):
        return "Tadawul"
    if s.endswith("=X"):
        return "FX"
    if s.endswith("=F"):
        return "Futures"
    return "NASDAQ/NYSE"


def _infer_currency_from_symbol(symbol: str) -> str:
    """v5.47.4: delegates to core.symbols.normalize.get_currency_from_symbol when available."""
    s = normalize_symbol(symbol)
    if not s:
        return "USD"

    if _HAS_NORMALIZE_HELPERS and _ext_get_currency_from_symbol is not None:
        try:
            ccy = _ext_get_currency_from_symbol(s)
            if ccy:
                return ccy
        except Exception:
            pass

    if s.endswith(".SR") or re.match(r"^[0-9]{4}$", s):
        return "SAR"
    if s.endswith("=X"):
        pair = s[:-2]
        if len(pair) >= 6:
            return pair[-3:]
        if pair:
            return pair
        return "FX"
    if s.endswith("=F"):
        return "USD"
    if "." in s:
        suffix = "." + s.rsplit(".", 1)[1].upper()
        ccy = _FALLBACK_CURRENCY_BY_SUFFIX.get(suffix)
        if ccy:
            return ccy
    return "USD"


def _infer_country_from_symbol(symbol: str) -> str:
    """v5.47.4 (AUDIT FIX): delegates to core.symbols.normalize.get_country_from_symbol when available."""
    s = normalize_symbol(symbol)
    if not s:
        return ""

    if _HAS_NORMALIZE_HELPERS and _ext_get_country_from_symbol is not None:
        try:
            country = _ext_get_country_from_symbol(s)
            if country:
                return country
        except Exception:
            pass

    if s.endswith(".SR") or re.match(r"^[0-9]{4}$", s):
        return "Saudi Arabia"
    if s.endswith("=X") or s.endswith("=F"):
        return "Global"
    if "." in s:
        suffix = "." + s.rsplit(".", 1)[1].upper()
        country = _FALLBACK_COUNTRY_BY_SUFFIX.get(suffix)
        if country:
            return country
    if s.isalpha() and 1 <= len(s) <= 5:
        return "United States"
    return ""


def _infer_sector_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if s.endswith("=X"):
        return "Currencies"
    if s.endswith("=F") or s in _COMMODITY_SYMBOL_HINTS:
        return "Commodities"
    if s in _ETF_SYMBOL_HINTS:
        return "Broad Market"
    if s.endswith(".SR") or re.match(r"^[0-9]{4}$", s):
        return "Saudi Market"
    return ""


def _infer_industry_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if s in _COMMODITY_INDUSTRY_HINTS:
        return _COMMODITY_INDUSTRY_HINTS[s]
    if s.endswith("=X"):
        return "Foreign Exchange"
    if s.endswith("=F"):
        return "Commodity Futures"
    if s in _ETF_SYMBOL_HINTS:
        return "ETF"
    if s.endswith(".SR") or re.match(r"^[0-9]{4}$", s):
        return "Listed Equities"
    return ""


def _infer_display_name_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if not s:
        return ""
    if s in _COMMODITY_DISPLAY_NAMES:
        return _COMMODITY_DISPLAY_NAMES[s]
    if s in _ETF_DISPLAY_NAMES:
        return _ETF_DISPLAY_NAMES[s]
    if s.endswith("=X"):
        pair = s[:-2]
        if len(pair) >= 6:
            return f"{pair[:3]}/{pair[3:6]}"
        return f"{pair} FX" if pair else s
    if s.endswith("=F"):
        return _safe_str(s.replace("=F", "")).strip() or s
    return s


def _apply_symbol_context_defaults(row: Dict[str, Any], symbol: str = "", page: str = "") -> Dict[str, Any]:
    out = dict(row or {})
    sym = normalize_symbol(symbol or _safe_str(out.get("symbol") or out.get("ticker") or out.get("requested_symbol")))
    if not sym:
        return out

    page = _canonicalize_sheet_name(page) if page else ""

    if not out.get("symbol"):
        out["symbol"] = sym
    if not out.get("requested_symbol"):
        out["requested_symbol"] = sym
    if not out.get("symbol_normalized"):
        out["symbol_normalized"] = sym

    if page == "Commodities_FX" or sym.endswith("=F") or sym.endswith("=X"):
        out.setdefault("asset_class", _infer_asset_class_from_symbol(sym))
        out.setdefault("exchange", _infer_exchange_from_symbol(sym))
        out.setdefault("currency", _infer_currency_from_symbol(sym))
        out.setdefault("country", _infer_country_from_symbol(sym))
        out.setdefault("sector", _infer_sector_from_symbol(sym))
        out.setdefault("industry", _infer_industry_from_symbol(sym))

        current_name = _safe_str(out.get("name"))
        inferred_name = _infer_display_name_from_symbol(sym)
        if not current_name or current_name == sym:
            out["name"] = inferred_name or sym

        if sym.endswith("=X"):
            out.setdefault("market_cap", None)
            out.setdefault("float_shares", None)
            out.setdefault("beta_5y", None)
        if sym.endswith("=F"):
            out.setdefault("market_cap", None)
            out.setdefault("float_shares", None)

        if out.get("invest_period_label") in (None, ""):
            out["invest_period_label"] = "1Y"
        if out.get("horizon_days") in (None, ""):
            out["horizon_days"] = 365

    return out


def _coerce_datetime_like(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        try:
            return value.isoformat()
        except Exception:
            return _safe_str(value)
    return _safe_str(value) or None


def _canonicalize_provider_row(row: Dict[str, Any], requested_symbol: str = "", normalized_symbol: str = "", provider: str = "") -> Dict[str, Any]:
    """
    v5.53.0 [BUG-3 / BUG-4 FIX]: week_52_position_pct and unrealized_pl_pct
    are now stored as PERCENT POINTS (0-100), not fractions (0.0-1.0).  The
    column headers in INSTRUMENT_CANONICAL_HEADERS read "52W Position %" and
    "Unrealized P/L %" and the display layer expected percent points; storing
    as fractions caused 52W Position % to render blank and Unrealized P/L %
    to render as e.g. 4,614,849.94% (the symptom of being multiplied by 100
    a second time downstream).  percent_change, max_drawdown_1y, var_95_1d
    remain as fractions per v5.47.3.
    """
    src = dict(row or {})
    flat = _flatten_scalar_fields(src)
    symbol = normalized_symbol or normalize_symbol(_safe_str(_lookup_alias_value(src, flat, "symbol") or requested_symbol))
    out: Dict[str, Any] = {
        "symbol": symbol or requested_symbol,
        "symbol_normalized": symbol or requested_symbol,
        "requested_symbol": requested_symbol or symbol,
    }
    for field, aliases in _CANONICAL_FIELD_ALIASES.items():
        for alias in (field,) + tuple(aliases):
            val = _lookup_alias_value(src, flat, alias)
            if not _is_blank_value(val):
                out[field] = _json_safe(_to_scalar(val))
                break

    inferred_symbol = out.get("symbol") or normalized_symbol or requested_symbol
    inferred_name = _infer_display_name_from_symbol(inferred_symbol)
    if not out.get("name") or _safe_str(out.get("name")) == _safe_str(inferred_symbol):
        out["name"] = inferred_name or out.get("symbol") or normalized_symbol or requested_symbol
    if not out.get("asset_class"):
        out["asset_class"] = _infer_asset_class_from_symbol(inferred_symbol)
    if not out.get("exchange"):
        out["exchange"] = _infer_exchange_from_symbol(inferred_symbol)
    if not out.get("currency"):
        out["currency"] = _infer_currency_from_symbol(inferred_symbol)
    if not out.get("country"):
        out["country"] = _infer_country_from_symbol(inferred_symbol)
    if not out.get("sector"):
        out["sector"] = _infer_sector_from_symbol(inferred_symbol)
    if not out.get("industry"):
        out["industry"] = _infer_industry_from_symbol(inferred_symbol)

    if provider and not out.get("data_provider"):
        out["data_provider"] = provider

    if not out.get("last_updated_utc"):
        out["last_updated_utc"] = _coerce_datetime_like(_lookup_alias_value(src, flat, "last_updated_utc")) or _now_utc_iso()
    if not out.get("last_updated_riyadh"):
        out["last_updated_riyadh"] = _now_riyadh_iso()

    warnings = out.get("warnings")
    if isinstance(warnings, (list, tuple, set)):
        out["warnings"] = "; ".join(_safe_str(v) for v in warnings if _safe_str(v))

    price = _as_float(out.get("current_price")) or _as_float(out.get("price"))
    prev = _as_float(out.get("previous_close"))
    change = _as_float(out.get("price_change"))
    pct = _as_float(out.get("percent_change"))
    if price is None:
        price = _as_float(out.get("close"))
        if price is not None:
            out["current_price"] = price
    if prev is None and price is not None and change is not None:
        prev = price - change
        out["previous_close"] = prev
    if change is None and price is not None and prev is not None:
        change = price - prev
        out["price_change"] = round(change, 6)
    # v5.47.3: percent_change stays as a FRACTION (display layer does *100).
    if pct is None and price is not None and prev not in (None, 0):
        pct = (price - prev) / prev
        out["percent_change"] = round(pct, 8)
    elif pct is not None and abs(pct) > 1.5:
        out["percent_change"] = round(pct / 100.0, 8)

    high52 = _as_float(out.get("week_52_high"))
    low52 = _as_float(out.get("week_52_low"))
    # v5.53.0 [BUG-3a FIX]: store week_52_position_pct as PERCENT POINTS
    # (0-100), not a fraction.  Column header reads "52W Position %" and
    # the display layer expects percent points; storing as a fraction
    # caused the column to render blank in the May 8 audit.
    if price is not None and high52 is not None and low52 is not None and high52 > low52 and out.get("week_52_position_pct") is None:
        out["week_52_position_pct"] = round((price - low52) / (high52 - low52) * 100.0, 4)

    qty = _as_float(out.get("position_qty"))
    avg_cost = _as_float(out.get("avg_cost"))
    if qty is not None and price is not None and out.get("position_value") is None:
        out["position_value"] = round(qty * price, 6)
    if qty is not None and avg_cost is not None and out.get("position_cost") is None:
        out["position_cost"] = round(qty * avg_cost, 6)
    pos_val = _as_float(out.get("position_value"))
    pos_cost = _as_float(out.get("position_cost"))
    if pos_val is not None and pos_cost is not None and out.get("unrealized_pl") is None:
        out["unrealized_pl"] = round(pos_val - pos_cost, 6)
    upl = _as_float(out.get("unrealized_pl"))
    # v5.53.0 [BUG-4 FIX]: percent points, not fraction.  EXE.US in the
    # May 8 audit displayed 4,614,849.94% — the symptom of this value
    # being multiplied by 100 twice (once here when stored as fraction,
    # then again by the display layer interpreting "%" suffix).
    if upl is not None and pos_cost not in (None, 0) and out.get("unrealized_pl_pct") is None:
        out["unrealized_pl_pct"] = round(upl / pos_cost * 100.0, 4)

    out = _apply_symbol_context_defaults(out, symbol=inferred_symbol)
    if _as_float(out.get("current_price")) is not None and _safe_str(out.get("warnings")).lower() == "no live provider data available":
        out["warnings"] = "Recovered from history/chart fallback"

    return out


def _normalize_to_schema_keys(keys: Sequence[str], headers: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    src = _canonicalize_provider_row(
        dict(row or {}),
        requested_symbol=_safe_str((row or {}).get("requested_symbol")),
        normalized_symbol=normalize_symbol(_safe_str((row or {}).get("symbol") or (row or {}).get("ticker"))),
        provider=_safe_str((row or {}).get("data_provider") or (row or {}).get("provider")),
    )
    flat = _flatten_scalar_fields(src)

    out: Dict[str, Any] = {}
    for idx, key in enumerate(keys or []):
        header = headers[idx] if idx < len(headers) else key
        aliases = [key, header, _norm_key(key), _norm_key(header), key.lower(), header.lower(), key.replace("_", " ")]
        aliases.extend(_CANONICAL_FIELD_ALIASES.get(key, ()))
        val = None
        found = False
        for alias in aliases:
            val = _lookup_alias_value(src, flat, alias)
            if not _is_blank_value(val):
                found = True
                break
        out[key] = _json_safe(_to_scalar(val)) if found else None
    return out


def _apply_page_row_backfill(sheet: str, row: Dict[str, Any]) -> Dict[str, Any]:
    target = _canonicalize_sheet_name(sheet)
    out = _apply_symbol_context_defaults(dict(row or {}), page=target)
    sym = normalize_symbol(_safe_str(out.get("symbol") or out.get("requested_symbol")))

    if out.get("invest_period_label") in (None, ""):
        out["invest_period_label"] = "1Y"
    if out.get("horizon_days") in (None, ""):
        out["horizon_days"] = 365

    if out.get("data_provider") in (None, ""):
        sources = out.get("data_sources")
        if isinstance(sources, list) and sources:
            out["data_provider"] = _safe_str(sources[0])

    conf = _as_float(out.get("confidence_score"))
    if conf is None:
        conf_fraction = _as_float(out.get("forecast_confidence"))
        if conf_fraction is not None:
            conf = conf_fraction * 100.0 if conf_fraction <= 1.5 else conf_fraction
            # v5.52.0 [FIX-9]: was setdefault, no-op against None.
            if out.get("confidence_score") is None:
                out["confidence_score"] = round(_clamp(conf, 0.0, 100.0), 2)
    if conf is not None and out.get("confidence_bucket") in (None, ""):
        out["confidence_bucket"] = "HIGH" if conf >= 75 else "MODERATE" if conf >= 55 else "LOW"

    if target == "Commodities_FX" or sym.endswith("=F") or sym.endswith("=X"):
        if out.get("data_provider") is None or out.get("data_provider") == "":
            out["data_provider"] = _safe_str(out.get("data_provider"), "history_or_fallback")
        if out.get("forecast_confidence") in (None, ""):
            out["forecast_confidence"] = 0.55
        if out.get("confidence_score") in (None, ""):
            out["confidence_score"] = 55.0
        if out.get("forecast_confidence") not in (None, "") and out.get("confidence_bucket") in (None, ""):
            conf = _as_float(out.get("confidence_score")) or ((_as_float(out.get("forecast_confidence")) or 0.55) * 100.0)
            out["confidence_bucket"] = "HIGH" if conf >= 75 else "MODERATE" if conf >= 55 else "LOW"
        if out.get("warnings") in (None, "") and _as_float(out.get("current_price")) is None:
            out["warnings"] = "Live quote sparse; chart/history fallback unavailable"

    if target == "Mutual_Funds":
        if out.get("asset_class") in (None, ""):
            out["asset_class"] = "Fund"
        if out.get("sector") in (None, ""):
            out["sector"] = "Diversified"
        if out.get("industry") in (None, ""):
            out["industry"] = "Mutual Funds"
        if out.get("country") in (None, ""):
            out["country"] = _infer_country_from_symbol(sym)
        if out.get("exchange") in (None, ""):
            out["exchange"] = _infer_exchange_from_symbol(sym)
        if out.get("currency") in (None, ""):
            out["currency"] = _infer_currency_from_symbol(sym)
        if out.get("invest_period_label") in (None, ""):
            out["invest_period_label"] = "1Y"
        if out.get("horizon_days") in (None, ""):
            out["horizon_days"] = 365

    if target in {"Global_Markets", "Market_Leaders", "My_Portfolio", "Top_10_Investments"}:
        asset_class = _safe_str(out.get("asset_class"))
        if sym in _ETF_SYMBOL_HINTS or asset_class.upper() == "ETF":
            out.setdefault("asset_class", "ETF")
            out.setdefault("sector", "Broad Market")
            out.setdefault("industry", "ETF")
            inferred_name = _infer_display_name_from_symbol(sym)
            if inferred_name and (_safe_str(out.get("name")) in ("", sym)):
                out["name"] = inferred_name

    return out


def _strict_project_row(keys: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _json_safe(row.get(k)) for k in keys}


def _strict_project_row_display(headers: Sequence[str], keys: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for idx, key in enumerate(keys or []):
        header = headers[idx] if idx < len(headers or []) else key
        out[header] = _json_safe(row.get(key))
    return out


def _rows_display_objects_from_rows(rows: List[Dict[str, Any]], headers: List[str], keys: List[str]) -> List[Dict[str, Any]]:
    return [_strict_project_row_display(headers, keys, row) for row in (rows or [])]


def _merge_missing_fields(base_row: Dict[str, Any], template_row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(base_row or {})
    if not isinstance(template_row, dict):
        return out
    for k, v in template_row.items():
        if out.get(k) in (None, "", [], {}) and v not in (None, "", [], {}):
            out[k] = _json_safe(v)
    return out


def _rows_matrix_from_rows(rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Any]]:
    return [[_json_safe(row.get(k)) for k in keys] for row in rows or []]


def _compute_scores_fallback(row: Dict[str, Any]) -> None:
    """
    v5.53.0 [BUG-1 FIX]: Forecast block at the end of this function used to
    fall back to flat (drift = 1.0 / 3.0 / 8.0) defaults whenever
    seed_best_roi was falsy.  Divided by (300, 100, 100) those produced
    exactly 0.33% / 3% / 8% on every ticker — the symptom seen across
    every row in the May 8 audit.  Replaced with a CAPM + momentum tilt
    + sqrt-time + 30D-vol-dampener model that differentiates per ticker.
    """
    price = _as_float(row.get("current_price")) or _as_float(row.get("price"))
    pe = _as_float(row.get("pe_ttm"))
    pb = _as_float(row.get("pb_ratio"))
    ps = _as_float(row.get("ps_ratio"))
    ev_ebitda = _as_float(row.get("ev_ebitda"))
    intrinsic = _as_float(row.get("intrinsic_value"))
    beta = _as_float(row.get("beta_5y"))
    debt_to_equity = _as_float(row.get("debt_to_equity"))

    div_yield_pct = _as_pct_points(row.get("dividend_yield")) or 0.0
    gross_margin_pct = _as_pct_points(row.get("gross_margin")) or 0.0
    operating_margin_pct = _as_pct_points(row.get("operating_margin")) or 0.0
    profit_margin_pct = _as_pct_points(row.get("profit_margin")) or 0.0
    revenue_growth_pct = _as_pct_points(row.get("revenue_growth_yoy")) or 0.0

    seed_roi_1m = _as_pct_points(row.get("expected_roi_1m"))
    seed_roi_3m = _as_pct_points(row.get("expected_roi_3m"))
    seed_roi_12m = _as_pct_points(row.get("expected_roi_12m"))
    seed_best_roi = next((v for v in (seed_roi_3m, seed_roi_12m, seed_roi_1m) if v is not None), 0.0)

    if row.get("value_score") is None:
        value_score = 55.0
        if pe is not None and pe > 0:
            value_score += max(0.0, 22.0 - min(pe, 22.0))
        if pb is not None and pb > 0:
            value_score += max(0.0, 12.0 - min(pb * 3.0, 12.0))
        if ps is not None and ps > 0:
            value_score += max(0.0, 10.0 - min(ps * 2.0, 10.0))
        value_score += min(max(div_yield_pct, 0.0), 12.0)
        row["value_score"] = round(_clamp(float(value_score), 0.0, 100.0), 2)

    # v5.52.0 [FIX-10]: PEG_ratio computation when not supplied by provider.
    if row.get("peg_ratio") is None:
        peg_pe = _as_float(row.get("pe_ttm")) or _as_float(row.get("pe_forward"))
        peg_growth_pct = _as_pct_points(row.get("revenue_growth_yoy"))
        if (peg_pe is not None and peg_pe > 0
                and peg_growth_pct is not None and peg_growth_pct > 1.0):
            peg = peg_pe / peg_growth_pct
            if 0.0 <= peg <= 10.0:
                row["peg_ratio"] = round(peg, 4)

    if row.get("valuation_score") is None:
        valuation_score = 50.0
        if intrinsic is not None and price not in (None, 0):
            upside_pct = ((intrinsic - price) / price) * 100.0
            valuation_score += _clamp(upside_pct, -20.0, 25.0)
        if ev_ebitda is not None and ev_ebitda > 0:
            valuation_score += max(0.0, 12.0 - min(ev_ebitda, 12.0))
        if pe is not None and pe > 0:
            valuation_score += max(0.0, 15.0 - min(pe, 15.0))
        row["valuation_score"] = round(_clamp(float(valuation_score), 0.0, 100.0), 2)

    if row.get("quality_score") is None:
        quality_score = 45.0
        quality_score += min(max(gross_margin_pct, 0.0), 20.0) * 0.6
        quality_score += min(max(operating_margin_pct, 0.0), 18.0) * 0.7
        quality_score += min(max(profit_margin_pct, 0.0), 15.0) * 0.7
        if debt_to_equity is not None:
            quality_score += max(0.0, 15.0 - min(max(debt_to_equity, 0.0), 15.0))
        row["quality_score"] = round(_clamp(float(quality_score), 0.0, 100.0), 2)

    if row.get("momentum_score") is None:
        pct = _as_pct_points(row.get("percent_change"))
        if pct is None:
            pct = _as_pct_points(row.get("change_pct"))
        if pct is None:
            pct = 0.0
        row["momentum_score"] = round(_clamp(50.0 + pct, 0.0, 100.0), 2)

    if row.get("growth_score") is None:
        growth_score = 50.0 + _clamp(revenue_growth_pct, -25.0, 35.0)
        eps = _as_float(row.get("eps_ttm"))
        if eps is not None and eps > 0:
            growth_score += 3.0
        row["growth_score"] = round(_clamp(float(growth_score), 0.0, 100.0), 2)

    conf = _as_float(row.get("forecast_confidence"))
    if conf is None:
        conf = _as_float(row.get("confidence_score"))
    if conf is None:
        conf = 0.55
    if conf > 1.5:
        conf = conf / 100.0
    # v5.52.0 [FIX-8]: explicit None-check (setdefault was a no-op against None).
    if row.get("forecast_confidence") is None:
        row["forecast_confidence"] = round(_clamp(conf, 0.0, 1.0), 4)
    if row.get("confidence_score") is None:
        row["confidence_score"] = round(_clamp(conf * 100.0, 0.0, 100.0), 2)

    if row.get("risk_score") is None:
        vol = _as_pct_points(row.get("volatility_90d"))
        drawdown = _as_pct_points(row.get("max_drawdown_1y"))
        var95 = _as_pct_points(row.get("var_95_1d"))
        risk_score = 30.0
        if vol is not None:
            risk_score += min(max(vol, 0.0), 35.0)
        if drawdown is not None:
            risk_score += min(max(drawdown, 0.0), 20.0) * 0.6
        if var95 is not None:
            risk_score += min(max(var95, 0.0), 12.0)
        if beta is not None:
            risk_score += min(max(beta * 8.0, 0.0), 15.0)
        row["risk_score"] = round(_clamp(float(risk_score), 0.0, 100.0), 2)

    if row.get("overall_score") is None:
        vals = [
            _as_float(row.get("value_score")),
            _as_float(row.get("valuation_score")),
            _as_float(row.get("quality_score")),
            _as_float(row.get("momentum_score")),
            _as_float(row.get("growth_score")),
        ]
        vals2 = [v for v in vals if v is not None]
        overall = sum(vals2) / len(vals2) if vals2 else 50.0
        row["overall_score"] = round(_clamp(float(overall), 0.0, 100.0), 2)

    # v5.53.0 [BUG-1 FIX]: differentiated forecast.  When no seed ROI is
    # available we no longer fall back to flat (1.0, 3.0, 8.0) drifts which
    # produced identical 0.33% / 3% / 8% on every ticker in the May 8 audit.
    # Instead, build a 12M anchor from CAPM + momentum tilt, then derive 3M
    # and 1M via sqrt-time scaling dampened by realized 30D vol.
    beta_for_forecast = _as_float(row.get("beta_5y")) or 1.0
    momentum_score_pct = _as_float(row.get("momentum_score")) or 50.0
    vol_30d_pct = _as_pct_points(row.get("volatility_30d")) or 25.0

    if seed_roi_12m is not None:
        roi_12m_pct = max(-60.0, min(150.0, seed_roi_12m))
    else:
        # CAPM with rf=4.5%, market_premium=3.5%; momentum tilt +/- 5%.
        capm_pct = 4.5 + beta_for_forecast * 3.5
        momentum_tilt_pct = (momentum_score_pct - 50.0) / 50.0 * 5.0
        roi_12m_pct = max(-60.0, min(150.0, capm_pct + momentum_tilt_pct))

    # Vol dampener: high-vol names carry less of the trend forward.
    vol_dampener = max(0.4, min(1.2, 25.0 / max(vol_30d_pct, 5.0)))

    if price is not None and row.get("forecast_price_12m") is None:
        row["forecast_price_12m"] = round(price * (1.0 + roi_12m_pct / 100.0), 4)
    if price is not None and row.get("forecast_price_3m") is None:
        roi_3m_pct = roi_12m_pct * (3.0 / 12.0) ** 0.5 * vol_dampener
        row["forecast_price_3m"] = round(price * (1.0 + roi_3m_pct / 100.0), 4)
    if price is not None and row.get("forecast_price_1m") is None:
        roi_1m_pct = roi_12m_pct * (1.0 / 12.0) ** 0.5 * vol_dampener
        row["forecast_price_1m"] = round(price * (1.0 + roi_1m_pct / 100.0), 4)

    if price is not None and row.get("expected_roi_1m") is None:
        fp1 = _as_float(row.get("forecast_price_1m"))
        if fp1 is not None and price:
            row["expected_roi_1m"] = round((fp1 - price) / price, 6)
    if price is not None and row.get("expected_roi_3m") is None:
        fp3 = _as_float(row.get("forecast_price_3m"))
        if fp3 is not None and price:
            row["expected_roi_3m"] = round((fp3 - price) / price, 6)
    if price is not None and row.get("expected_roi_12m") is None:
        fp12 = _as_float(row.get("forecast_price_12m"))
        if fp12 is not None and price:
            row["expected_roi_12m"] = round((fp12 - price) / price, 6)

    final_roi_1m = _as_pct_points(row.get("expected_roi_1m"))
    final_roi_3m = _as_pct_points(row.get("expected_roi_3m"))
    final_roi_12m = _as_pct_points(row.get("expected_roi_12m"))
    final_best_roi = next((v for v in (final_roi_3m, final_roi_12m, final_roi_1m) if v is not None), 0.0)

    if row.get("opportunity_score") is None:
        base = _as_float(row.get("overall_score")) or 50.0
        confidence_boost = ((_as_float(row.get("confidence_score")) or 50.0) - 50.0) * 0.20
        risk_penalty = ((_as_float(row.get("risk_score")) or 50.0) - 50.0) * 0.25
        roi_boost = _clamp(final_best_roi, -25.0, 35.0) * 0.35
        row["opportunity_score"] = round(_clamp(base + confidence_boost + roi_boost - risk_penalty, 0.0, 100.0), 2)

    if not row.get("risk_bucket"):
        rs = _as_float(row.get("risk_score")) or 50.0
        row["risk_bucket"] = "LOW" if rs < 40 else "MODERATE" if rs < 70 else "HIGH"

    if not row.get("confidence_bucket"):
        cs = _as_float(row.get("confidence_score")) or 55.0
        row["confidence_bucket"] = "HIGH" if cs >= 75 else "MODERATE" if cs >= 55 else "LOW"


def _derive_views(row: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """Derive the four view-column verdicts from existing per-row scores."""
    quality = _as_float(row.get("quality_score"))
    growth = _as_float(row.get("growth_score"))
    momentum = _as_float(row.get("momentum_score"))
    rsi = _as_float(row.get("rsi_14"))
    risk = _as_float(row.get("risk_score"))
    valuation = _as_float(row.get("valuation_score"))
    intrinsic = _as_float(row.get("intrinsic_value"))
    current = _as_float(row.get("current_price"))

    if quality is None and growth is None:
        fund_view = "N/A"
    else:
        fund_avg = sum(x for x in (quality, growth) if x is not None) / max(
            1, sum(1 for x in (quality, growth) if x is not None)
        )
        if fund_avg >= 70:
            fund_view = "BULLISH"
        elif fund_avg <= 40:
            fund_view = "BEARISH"
        else:
            fund_view = "NEUTRAL"

    if momentum is None and rsi is None:
        tech_view = "N/A"
    else:
        bullish_signals = 0
        bearish_signals = 0
        if momentum is not None:
            if momentum >= 70:
                bullish_signals += 1
            elif momentum <= 40:
                bearish_signals += 1
        if rsi is not None:
            if rsi >= 70:
                bearish_signals += 1
            elif rsi <= 30:
                bullish_signals += 1
            elif 45 <= rsi <= 60:
                bullish_signals += 1
        if bullish_signals > bearish_signals:
            tech_view = "BULLISH"
        elif bearish_signals > bullish_signals:
            tech_view = "BEARISH"
        else:
            tech_view = "NEUTRAL"

    if risk is None:
        risk_view = "N/A"
    elif risk <= 35:
        risk_view = "LOW"
    elif risk >= 65:
        risk_view = "HIGH"
    else:
        risk_view = "MODERATE"

    upside_pct: Optional[float] = None
    if intrinsic is not None and current is not None and current > 0:
        upside_pct = (intrinsic - current) / current
    if valuation is None and upside_pct is None:
        value_view = "N/A"
    else:
        if upside_pct is not None:
            if upside_pct >= 0.15:
                value_view = "CHEAP"
            elif upside_pct <= -0.10:
                value_view = "EXPENSIVE"
            else:
                value_view = "FAIR"
        else:
            if valuation is not None and valuation >= 70:
                value_view = "CHEAP"
            elif valuation is not None and valuation <= 35:
                value_view = "EXPENSIVE"
            else:
                value_view = "FAIR"

    return fund_view, tech_view, risk_view, value_view


def _compute_intrinsic_and_upside(row: Dict[str, Any]) -> None:
    """
    Backfill intrinsic_value and upside_pct when upstream provider did not
    supply them.

    v5.53.0 [BUG-2 FIX]: removed the symmetric (price*0.3, price*3.0)
    clamp at the end of this function.  It was hiding real DCF /
    currency-unit bugs (especially JSE quotes in ZAC vs intrinsic in
    ZAR), forcing upside_pct to land on exactly -70% or +200%.  Now we
    only floor at zero so we don't store a negative intrinsic value;
    the asymmetric ceiling is gone.  Extreme upside values now surface
    as real signal so the underlying issue is diagnosable instead of
    masked.
    """
    price = _as_float(row.get("current_price"))
    if price is None or price <= 0:
        return

    intrinsic = _as_float(row.get("intrinsic_value"))

    if intrinsic is None:
        candidates: List[float] = []

        eps = _as_float(row.get("eps_ttm"))
        pe_fwd = _as_float(row.get("pe_forward"))
        pe_ttm = _as_float(row.get("pe_ttm"))
        if eps is not None and eps > 0 and pe_fwd is not None and pe_fwd > 0:
            anchor_pe = max(pe_fwd, min(pe_ttm or pe_fwd, 25.0))
            candidates.append(eps * anchor_pe)
        elif eps is not None and eps > 0 and pe_ttm is not None and 0 < pe_ttm < 50:
            candidates.append(eps * 18.0)

        forecasts: List[float] = []
        for key in ("forecast_price_3m", "forecast_price_12m", "forecast_price_1m"):
            fp = _as_float(row.get(key))
            if fp is not None and fp > 0:
                forecasts.append(fp)
        if forecasts:
            candidates.append(sum(forecasts) / len(forecasts))

        rev_growth = _as_pct_points(row.get("revenue_growth_yoy"))
        if eps is not None and eps > 0 and rev_growth is not None:
            growth_capped = max(0.0, min(rev_growth, 25.0))
            anchor = eps * (8.5 + 2.0 * growth_capped)
            if anchor > 0:
                candidates.append(anchor)

        if candidates:
            candidates.sort()
            mid = len(candidates) // 2
            if len(candidates) % 2 == 1:
                intrinsic = candidates[mid]
            else:
                intrinsic = (candidates[mid - 1] + candidates[mid]) / 2.0
            # v5.53.0 [BUG-2 FIX]: floor at zero only; no symmetric clamp.
            intrinsic = max(0.0, intrinsic)
            row["intrinsic_value"] = round(intrinsic, 4)

    intrinsic = _as_float(row.get("intrinsic_value"))
    if intrinsic is not None and intrinsic > 0 and row.get("upside_pct") is None:
        upside = (intrinsic - price) / price
        row["upside_pct"] = round(upside, 4)


def _compute_recommendation(row: Dict[str, Any]) -> None:
    """
    Populate recommendation, recommendation_detailed, recommendation_priority,
    recommendation_reason, AND the four view columns.

    v5.50.0 (Decision Matrix): three recommendation fields produced
    (canonical 5-tier `recommendation`, plus 8-tier `recommendation_detailed`
    and `recommendation_priority`).  See header docstring for full priority
    rules.

    v5.52.0 [FIX-7 / FIX-11]: switched setdefault to explicit None-check
    so computed view tokens and reason text reach the wire.
    """
    _compute_intrinsic_and_upside(row)

    fund_view, tech_view, risk_view, value_view = _derive_views(row)
    if row.get("fundamental_view") is None:
        row["fundamental_view"] = fund_view
    if row.get("technical_view") is None:
        row["technical_view"] = tech_view
    if row.get("risk_view") is None:
        row["risk_view"] = risk_view
    if row.get("value_view") is None:
        row["value_view"] = value_view

    # Respect upstream recommendation if already set.
    if row.get("recommendation"):
        return

    overall = _as_float(row.get("overall_score")) or 50.0
    conf = _as_float(row.get("confidence_score")) or 55.0
    risk = _as_float(row.get("risk_score")) or 50.0

    detailed, priority, rule_label = _classify_8tier(overall, conf, risk)
    canonical = _RECOMMENDATION_COLLAPSE_MAP.get(detailed, "HOLD")

    row["recommendation_detailed"] = detailed
    row["recommendation_priority"] = priority
    row["recommendation"] = canonical
    if row.get("recommendation_reason") is None:
        row["recommendation_reason"] = (
            "P{0} {1} [{2}]: Fund {3} | Tech {4} | Risk {5} | Val {6} \u2192 {7}".format(
                priority, rule_label, detailed,
                fund_view, tech_view, risk_view, value_view, canonical,
            )
        )


def _apply_rank_overall(rows: List[Dict[str, Any]]) -> None:
    scored: List[Tuple[int, float]] = []
    for i, row in enumerate(rows):
        score = _as_float(row.get("overall_score"))
        if score is None:
            score = _as_float(row.get("opportunity_score"))
        if score is None:
            continue
        scored.append((i, score))
    scored.sort(key=lambda t: t[1], reverse=True)
    for rank, (idx, _) in enumerate(scored, start=1):
        rows[idx]["rank_overall"] = rank


def _top10_selection_reason(row: Dict[str, Any]) -> str:
    parts: List[str] = []
    for key, label in (
        ("overall_score", "overall"),
        ("opportunity_score", "opportunity"),
        ("confidence_score", "confidence"),
        ("risk_score", "risk"),
    ):
        val = _as_float(row.get(key))
        if val is None:
            continue
        suffix = "%" if key == "confidence_score" else ""
        parts.append(f"{label}={round(val, 1)}{suffix}")
    return "Selected by fallback ranking" if not parts else ("Selected by fallback ranking: " + ", ".join(parts))


def _top10_criteria_snapshot(criteria: Dict[str, Any]) -> str:
    keep = {
        "top_n": criteria.get("top_n"),
        "pages_selected": criteria.get("pages_selected"),
        "horizon_days": criteria.get("horizon_days") or criteria.get("invest_period_days"),
        "risk_level": criteria.get("risk_level"),
        "min_expected_roi": criteria.get("min_expected_roi"),
        "confidence_level": criteria.get("confidence_level"),
        "direct_symbols": criteria.get("direct_symbols") or criteria.get("symbols"),
    }
    keep = {k: v for k, v in keep.items() if v not in (None, "", [], {})}
    try:
        import json
        return json.dumps(_json_safe(keep), ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return "{}"


def _feature_flags(settings: Any) -> Dict[str, bool]:
    return {
        "computations_enabled": _safe_bool(getattr(settings, "computations_enabled", True), True),
        "forecasting_enabled": _safe_bool(getattr(settings, "forecasting_enabled", True), True),
        "scoring_enabled": _safe_bool(getattr(settings, "scoring_enabled", True), True),
    }


def _try_get_settings() -> Any:
    for mod_path in ("config", "core.config", "env"):
        try:
            mod = import_module(mod_path)
        except Exception:
            continue
        for fn_name in ("get_settings_cached", "get_settings"):
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                try:
                    return fn()
                except Exception:
                    continue
    return None


async def _call_maybe_async(fn: Any, *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)

    result = await asyncio.to_thread(fn, *args, **kwargs)
    return await result if inspect.isawaitable(result) else result


# =============================================================================
# Schema registry helpers
# =============================================================================
try:
    from core.sheets.schema_registry import SCHEMA_REGISTRY as _RAW_SCHEMA_REGISTRY  # type: ignore
    from core.sheets.schema_registry import get_sheet_spec as _RAW_GET_SHEET_SPEC  # type: ignore
    _SCHEMA_AVAILABLE = True
except Exception:
    _RAW_SCHEMA_REGISTRY = {}
    _RAW_GET_SHEET_SPEC = None
    _SCHEMA_AVAILABLE = False

SCHEMA_REGISTRY = _RAW_SCHEMA_REGISTRY if isinstance(_RAW_SCHEMA_REGISTRY, dict) else {}


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
    return []


def _schema_keys_headers_from_spec(spec: Any) -> Tuple[List[str], List[str]]:
    if isinstance(spec, dict) and len(spec) == 1 and not any(k in spec for k in ("columns", "fields", "headers", "keys", "display_headers")):
        first_val = list(spec.values())[0]
        if isinstance(first_val, dict):
            spec = first_val
    cols = _schema_columns_from_any(spec)
    headers: List[str] = []
    keys: List[str] = []
    for c in cols:
        if isinstance(c, Mapping):
            h = _safe_str(c.get("header") or c.get("display_header") or c.get("label") or c.get("title"))
            k = _safe_str(c.get("key") or c.get("field") or c.get("name") or c.get("id"))
        else:
            h = _safe_str(getattr(c, "header", getattr(c, "display_header", getattr(c, "label", getattr(c, "title", None)))))
            k = _safe_str(getattr(c, "key", getattr(c, "field", getattr(c, "name", getattr(c, "id", None)))))
        if h or k:
            headers.append(h or k.replace("_", " ").title())
            keys.append(k or _norm_key(h))
    if not headers and not keys and isinstance(spec, Mapping):
        h2 = spec.get("headers") or spec.get("display_headers")
        k2 = spec.get("keys") or spec.get("fields")
        if isinstance(h2, list):
            headers = [_safe_str(x) for x in h2 if _safe_str(x)]
        if isinstance(k2, list):
            keys = [_safe_str(x) for x in k2 if _safe_str(x)]
    return _complete_schema_contract(headers, keys)


def _registry_sheet_lookup(sheet: str) -> Any:
    if not SCHEMA_REGISTRY:
        return None
    candidates = [sheet, sheet.replace(" ", "_"), sheet.replace("_", " "), _norm_key(sheet), _norm_key_loose(sheet)]
    by_norm = {_norm_key(k): v for k, v in SCHEMA_REGISTRY.items()}
    by_loose = {_norm_key_loose(k): v for k, v in SCHEMA_REGISTRY.items()}
    for cand in candidates:
        if cand in SCHEMA_REGISTRY:
            return SCHEMA_REGISTRY.get(cand)
        nk = _norm_key(cand)
        if nk in by_norm:
            return by_norm[nk]
        nkl = _norm_key_loose(cand)
        if nkl in by_loose:
            return by_loose[nkl]
    return None


def get_sheet_spec(sheet: str) -> Any:
    canon = _canonicalize_sheet_name(sheet)
    if callable(_RAW_GET_SHEET_SPEC):
        for cand in _dedupe_keep_order([canon, canon.replace("_", " "), _norm_key(canon), sheet]):
            try:
                spec = _RAW_GET_SHEET_SPEC(cand)  # type: ignore[misc]
                if spec is not None:
                    return spec
            except Exception:
                continue
    spec = _registry_sheet_lookup(canon)
    if spec is not None:
        return spec
    static_contract = STATIC_CANONICAL_SHEET_CONTRACTS.get(canon)
    if static_contract:
        return dict(static_contract)
    raise KeyError(f"Unknown sheet spec: {sheet}")


def _schema_for_sheet(sheet: str) -> Tuple[Any, List[str], List[str], str]:
    canon = _canonicalize_sheet_name(sheet)

    try:
        spec = get_sheet_spec(canon)
        h, k = _schema_keys_headers_from_spec(spec)
        if canon == "Top_10_Investments":
            h, k = _ensure_top10_contract(h, k)
        if _usable_contract(h, k, canon):
            return spec, h, k, "schema_registry"
    except Exception:
        pass

    if canon in STATIC_CANONICAL_SHEET_CONTRACTS:
        c = STATIC_CANONICAL_SHEET_CONTRACTS[canon]
        h, k = _complete_schema_contract(c["headers"], c["keys"])
        if canon == "Top_10_Investments":
            h, k = _ensure_top10_contract(h, k)
        return dict(c), h, k, "static_canonical_contract_fallback"

    return None, [], [], "missing"


def _list_sheet_names_best_effort() -> List[str]:
    names = list(STATIC_CANONICAL_SHEET_CONTRACTS.keys())
    try:
        from core.sheets.page_catalog import CANONICAL_PAGES  # type: ignore
        for item in list(CANONICAL_PAGES or []):
            s = _canonicalize_sheet_name(_safe_str(item))
            if s and s not in names:
                names.append(s)
    except Exception:
        pass
    if isinstance(SCHEMA_REGISTRY, dict):
        for k in SCHEMA_REGISTRY.keys():
            s = _canonicalize_sheet_name(_safe_str(k))
            if s and s not in names:
                names.append(s)
    return names


def _build_union_schema_keys() -> List[str]:
    keys: List[str] = []
    seen: Set[str] = set()
    for contract in STATIC_CANONICAL_SHEET_CONTRACTS.values():
        for key in contract.get("keys", []):
            k = _safe_str(key)
            if k and k not in seen:
                seen.add(k)
                keys.append(k)
    if isinstance(SCHEMA_REGISTRY, dict):
        for _, spec in SCHEMA_REGISTRY.items():
            try:
                _, spec_keys = _schema_keys_headers_from_spec(spec)
                for key in spec_keys:
                    k = _safe_str(key)
                    if k and k not in seen:
                        seen.add(k)
                        keys.append(k)
            except Exception:
                continue
    for field in TOP10_REQUIRED_FIELDS:
        if field not in seen:
            seen.add(field)
            keys.append(field)
    return keys


_SCHEMA_UNION_KEYS: List[str] = _build_union_schema_keys()


# =============================================================================
# Async utilities
# =============================================================================
class SingleFlight:
    def __init__(self) -> None:
        self._inflight: Dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()

    async def do(self, key: str, fn: Any) -> Any:
        async with self._lock:
            fut = self._inflight.get(key)
            if fut is not None and not fut.done():
                return await fut
            # v5.51.0 [FIX-4]: get_running_loop() replaces deprecated
            # get_event_loop() (3.12+ deprecation warning).
            fut = asyncio.get_running_loop().create_future()
            self._inflight[key] = fut

        try:
            res = await fn()
            if not fut.done():
                fut.set_result(res)
            return res
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise
        finally:
            async with self._lock:
                if self._inflight.get(key) is fut:
                    self._inflight.pop(key, None)


@dataclass
class _MultiLevelEntry:
    value: Any
    expires_at: float


class MultiLevelCache:
    def __init__(self, *, default_ttl: float = 60.0, max_entries: int = 1024) -> None:
        self.default_ttl = float(default_ttl)
        self.max_entries = int(max_entries)
        self._store: Dict[str, _MultiLevelEntry] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any:
        async with self._lock:
            entry = self._store.get(key)
            if not entry:
                return None
            if entry.expires_at and entry.expires_at < time.time():
                self._store.pop(key, None)
                return None
            return entry.value

    async def set(self, key: str, value: Any, *, ttl: Optional[float] = None) -> None:
        async with self._lock:
            if len(self._store) >= self.max_entries:
                self._store.pop(next(iter(self._store)), None)
            ttl_value = self.default_ttl if ttl is None else float(ttl)
            self._store[key] = _MultiLevelEntry(value=value, expires_at=time.time() + ttl_value)

    async def clear(self) -> None:
        async with self._lock:
            self._store.clear()

    async def keys(self) -> List[str]:
        async with self._lock:
            return list(self._store.keys())


# =============================================================================
# Provider registry
# =============================================================================
@dataclass
class ProviderState:
    name: str
    healthy: bool = True
    failures: int = 0
    last_error: str = ""
    cooldown_until: float = 0.0


class ProviderRegistry:
    def __init__(self) -> None:
        self._states: Dict[str, ProviderState] = {}
        self._lock = asyncio.Lock()
        self._modules_cache: Dict[str, Any] = {}
        self._cooldown_seconds: float = float(_get_env_float("ENGINE_PROVIDER_COOLDOWN_SECONDS", 60.0))
        self._failure_threshold: int = int(_get_env_int("ENGINE_PROVIDER_FAILURE_THRESHOLD", 3))

    async def get_provider(self, name: str) -> Any:
        async with self._lock:
            cached = self._modules_cache.get(name)
            if cached is not None:
                return cached
            mod = None
            for path in (
                f"providers.{name}_provider",
                f"core.providers.{name}_provider",
                f"providers.{name}",
                f"core.providers.{name}",
            ):
                try:
                    mod = import_module(path)
                    break
                except Exception:
                    continue
            if mod is not None:
                self._modules_cache[name] = mod
            return mod

    async def record_success(self, name: str) -> None:
        async with self._lock:
            state = self._states.get(name) or ProviderState(name=name)
            state.healthy = True
            state.failures = 0
            state.last_error = ""
            state.cooldown_until = 0.0
            self._states[name] = state

    async def record_failure(self, name: str, error: str) -> None:
        async with self._lock:
            state = self._states.get(name) or ProviderState(name=name)
            state.failures += 1
            state.last_error = _safe_str(error)
            if state.failures >= self._failure_threshold:
                state.healthy = False
                state.cooldown_until = time.time() + self._cooldown_seconds
            self._states[name] = state

    async def get_stats(self) -> Dict[str, Any]:
        async with self._lock:
            return {name: asdict(state) for name, state in self._states.items()}


def _pick_provider_callable(mod: Any, *names: str) -> Optional[Any]:
    if mod is None:
        return None
    for n in names:
        fn = getattr(mod, n, None)
        if callable(fn):
            return fn
    return None


# =============================================================================
# Symbols reader proxy used by external selectors / advisors
# =============================================================================
class _EngineSymbolsReaderProxy:
    def __init__(self, engine: "DataEngineV5") -> None:
        self._engine = engine

    async def __call__(self, *args: Any, **kwargs: Any) -> List[str]:
        page = ""
        if args:
            page = _safe_str(args[0])
        page = page or _safe_str(kwargs.get("page") or kwargs.get("sheet") or kwargs.get("name"))
        return await self._engine.get_sheet_symbols(page or "Market_Leaders")

    async def get_sheet_symbols(self, page: str) -> List[str]:
        return await self._engine.get_sheet_symbols(page)

    async def get_page_symbols(self, page: str) -> List[str]:
        return await self._engine.get_sheet_symbols(page)

    async def list_symbols(self, page: str) -> List[str]:
        return await self._engine.get_sheet_symbols(page)


# =============================================================================
# Main engine
# =============================================================================
class DataEngineV5:
    def __init__(self) -> None:
        self.cache = MultiLevelCache(default_ttl=float(_get_env_float("ENGINE_CACHE_TTL_SECONDS", 60.0)), max_entries=int(_get_env_int("ENGINE_CACHE_MAX_ENTRIES", 1024)))
        self.singleflight = SingleFlight()
        self.providers = ProviderRegistry()
        self.settings = _try_get_settings()
        self.feature_flags = _feature_flags(self.settings)
        self._symbol_snapshot: Dict[str, Dict[str, Any]] = {}
        self._symbol_snapshot_lock = asyncio.Lock()
        self._snapshot_capacity: int = int(_get_env_int("ENGINE_SNAPSHOT_MAX_SYMBOLS", 4096))
        self._symbols_reader: Optional[Any] = None
        self._symbols_reader_inited: bool = False
        self._rows_reader: Optional[Any] = None
        self._rows_reader_inited: bool = False
        self._provider_priorities: Dict[str, int] = dict(PROVIDER_PRIORITIES)
        self._page_primary_provider: Dict[str, str] = dict(PAGE_PRIMARY_PROVIDER_DEFAULTS)
        self._page_provider_overrides: Dict[str, List[str]] = {}
        self._init_provider_overrides_from_env()
        try:
            from core.global_provider_overrides import GLOBAL_PROVIDER_OVERRIDES  # type: ignore
            for page, providers in (GLOBAL_PROVIDER_OVERRIDES or {}).items():
                cp = _canonicalize_sheet_name(_safe_str(page))
                if not cp:
                    continue
                if isinstance(providers, (list, tuple)):
                    self._page_provider_overrides[cp] = [_safe_str(p).lower() for p in providers if _safe_str(p)]
                elif isinstance(providers, dict):
                    primary = _safe_str(providers.get("primary"))
                    if primary:
                        self._page_primary_provider[cp] = primary.lower()
        except Exception:
            pass

        try:
            from core.global_provider_overrides import KSA_DISALLOW_EODHD  # type: ignore
            self._ksa_disallow_eodhd: bool = _safe_bool(KSA_DISALLOW_EODHD, True)
        except Exception:
            self._ksa_disallow_eodhd = _safe_bool(_safe_env("KSA_DISALLOW_EODHD"), True)

        self._max_concurrent_quotes: int = int(_get_env_int("ENGINE_MAX_CONCURRENT_QUOTES", 16))

    def _init_provider_overrides_from_env(self) -> None:
        for page, env_var in (
            ("Global_Markets", "GLOBAL_MARKETS_PROVIDERS"),
            ("Commodities_FX", "COMMODITIES_FX_PROVIDERS"),
            ("Mutual_Funds", "MUTUAL_FUNDS_PROVIDERS"),
            ("Market_Leaders", "MARKET_LEADERS_PROVIDERS"),
            ("My_Portfolio", "MY_PORTFOLIO_PROVIDERS"),
            ("My_Investments", "MY_INVESTMENTS_PROVIDERS"),
            ("Top_10_Investments", "TOP10_PROVIDERS"),
        ):
            raw = _safe_env(env_var)
            if not raw:
                continue
            providers = [p.strip().lower() for p in re.split(r"[,;|\s]+", raw) if p.strip()]
            if providers:
                self._page_provider_overrides[page] = providers

    async def aclose(self) -> None:
        try:
            await self.cache.clear()
        except Exception:
            pass

    # --- snapshot helpers --------------------------------------------------
    async def _record_symbol_snapshot(self, symbol: str, row: Dict[str, Any]) -> None:
        sym = normalize_symbol(symbol)
        if not sym:
            return
        if not isinstance(row, dict):
            return
        if _is_blank_value(row.get("current_price")) and not row.get("data_provider"):
            return
        async with self._symbol_snapshot_lock:
            if len(self._symbol_snapshot) >= self._snapshot_capacity:
                self._symbol_snapshot.pop(next(iter(self._symbol_snapshot)), None)
            self._symbol_snapshot[sym] = dict(row)

    async def _get_symbol_snapshot(self, symbol: str) -> Optional[Dict[str, Any]]:
        sym = normalize_symbol(symbol)
        if not sym:
            return None
        async with self._symbol_snapshot_lock:
            cached = self._symbol_snapshot.get(sym)
            return dict(cached) if cached else None

    def _resolve_symbol_meta(self, key: str, val: Any, requested: List[str], normalized: List[str]) -> Optional[Tuple[str, str]]:
        if isinstance(val, dict):
            requested_field = _safe_str(val.get("requested") or val.get("requested_symbol") or val.get("ticker") or val.get("code"))
            normalized_field = _safe_str(val.get("normalized") or val.get("symbol") or val.get("symbol_normalized"))
        else:
            requested_field = ""
            normalized_field = _safe_str(val)
        normalized_field = normalize_symbol(normalized_field) if normalized_field else ""

        if not requested_field:
            normalized_lookup = normalize_symbol(_safe_str(key))
            if normalized_lookup:
                idx = -1
                if normalized_field:
                    try:
                        idx = normalized.index(normalized_field)
                    except ValueError:
                        idx = -1
                if idx == -1:
                    try:
                        idx = normalized.index(normalized_lookup)
                    except ValueError:
                        idx = -1
                if 0 <= idx < len(requested):
                    requested_field = requested[idx]
        if not requested_field:
            requested_field = _safe_str(key)
        if not normalized_field:
            normalized_field = normalize_symbol(_safe_str(key))
        if not normalized_field:
            return None
        return requested_field, normalized_field

    def _finalize_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(payload or {})
        keys = list(payload.get("keys") or [])
        headers = list(payload.get("headers") or [])
        rows_raw = payload.get("rows") or []
        rows_dicts: List[Dict[str, Any]] = []
        if isinstance(rows_raw, list):
            for r in rows_raw:
                if isinstance(r, dict):
                    rows_dicts.append(r)
                elif isinstance(r, list):
                    if not keys:
                        continue
                    d = {}
                    for i, k in enumerate(keys):
                        d[k] = r[i] if i < len(r) else None
                    rows_dicts.append(d)
        rows_matrix = _rows_matrix_from_rows(rows_dicts, keys)
        rows_objects_keyed = [_strict_project_row(keys, r) for r in rows_dicts]
        rows_objects_display = _rows_display_objects_from_rows(rows_dicts, headers, keys)
        payload["rows"] = rows_matrix
        payload["rows_matrix"] = rows_matrix
        payload["row_objects"] = rows_objects_keyed
        payload["records"] = rows_objects_keyed
        payload["data"] = rows_objects_keyed
        payload["items"] = rows_objects_keyed
        payload["quotes"] = rows_objects_keyed
        payload["row_objects_display"] = rows_objects_display
        payload["row_count"] = len(rows_objects_keyed)
        payload.setdefault("count", len(rows_objects_keyed))
        payload.setdefault("symbols_returned", payload.get("symbols_returned") or _extract_symbols_from_rows(rows_objects_keyed))
        return payload

    # --- rows reader discovery (external orchestrators) --------------------
    def _init_rows_reader(self) -> None:
        if self._rows_reader_inited:
            return
        self._rows_reader_inited = True
        candidates = [
            ("core.sheets.rows_reader", ("get_page_rows", "get_sheet_rows", "get_rows", "get_pages_rows", "fetch_page_rows")),
            ("sheets.rows_reader", ("get_page_rows", "get_sheet_rows", "get_rows")),
            ("core.dashboards.aggregator", ("get_page_rows", "get_sheet_rows")),
            ("core.run_dashboard_sync", ("get_page_rows", "get_sheet_rows")),
        ]
        for mod_path, fn_names in candidates:
            try:
                mod = import_module(mod_path)
            except Exception:
                continue
            for fn_name in fn_names:
                fn = getattr(mod, fn_name, None)
                if callable(fn):
                    self._rows_reader = fn
                    return

    async def _call_rows_reader(self, sheet: str, criteria: Optional[Dict[str, Any]] = None) -> Any:
        self._init_rows_reader()
        if self._rows_reader is None:
            return None
        candidates = [sheet, sheet.replace("_", " "), _norm_key(sheet)]
        for cand in _dedupe_keep_order(candidates):
            for kwargs in (
                {"page": cand, "criteria": criteria or {}},
                {"sheet": cand, "criteria": criteria or {}},
                {"page_name": cand, "criteria": criteria or {}},
                {"page": cand},
                {"sheet": cand},
                {},
            ):
                try:
                    return await _call_maybe_async(self._rows_reader, **kwargs)
                except TypeError:
                    continue
                except Exception:
                    return None
            try:
                return await _call_maybe_async(self._rows_reader, cand)
            except Exception:
                continue
        return None

    async def _get_rows_from_external_reader(self, sheet: str, criteria: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        try:
            res = await self._call_rows_reader(sheet, criteria=criteria)
        except Exception:
            return []
        return _coerce_rows_list(res)

    # --- symbols reader discovery -----------------------------------------
    def _init_symbols_reader(self) -> None:
        if self._symbols_reader_inited:
            return
        self._symbols_reader_inited = True
        # v5.51.0 [FIX-5]: probe both singular and plural module names.
        candidates = [
            ("core.sheets.symbols_reader", ("get_symbols_for_sheet", "get_sheet_symbols", "get_symbols", "list_symbols")),
            ("sheets.symbols_reader", ("get_symbols_for_sheet", "get_sheet_symbols", "get_symbols", "list_symbols")),
            ("core.sheets.symbol_reader", ("get_symbols_for_sheet", "get_sheet_symbols", "get_symbols", "list_symbols")),
            ("sheets.symbol_reader", ("get_symbols_for_sheet", "get_sheet_symbols", "get_symbols", "list_symbols")),
            ("core.sheets.symbols_loader", ("get_symbols", "list_symbols")),
            ("sheets.symbols_loader", ("get_symbols", "list_symbols")),
        ]
        for mod_path, fn_names in candidates:
            try:
                mod = import_module(mod_path)
            except Exception:
                continue
            for fn_name in fn_names:
                fn = getattr(mod, fn_name, None)
                if callable(fn):
                    self._symbols_reader = fn
                    logger.debug(
                        "[engine_v2 v%s] symbols_reader bound: %s.%s",
                        __version__, mod_path, fn_name,
                    )
                    return

    async def _call_symbols_reader(self, sheet: str) -> Any:
        self._init_symbols_reader()
        if self._symbols_reader is None:
            return None
        candidates = [sheet, sheet.replace("_", " "), _norm_key(sheet)]
        for cand in _dedupe_keep_order(candidates):
            for kwargs in (
                {"page": cand},
                {"sheet": cand},
                {"page_name": cand},
                {"name": cand},
                {},
            ):
                try:
                    return await _call_maybe_async(self._symbols_reader, **kwargs)
                except TypeError:
                    continue
                except Exception:
                    return None
            try:
                return await _call_maybe_async(self._symbols_reader, cand)
            except Exception:
                continue
        return None

    async def _get_symbols_from_env(self, sheet: str) -> List[str]:
        env_var = PAGE_SYMBOL_ENV_KEYS.get(sheet)
        if not env_var:
            return []
        raw = _safe_env(env_var)
        if not raw:
            return []
        return _normalize_symbol_list(_split_symbols(raw))

    async def _get_symbols_from_settings(self, sheet: str) -> List[str]:
        if self.settings is None:
            return []
        attr_candidates = [
            f"{sheet.lower()}_symbols",
            f"{_norm_key(sheet)}_symbols",
            f"symbols_{sheet.lower()}",
            f"symbols_{_norm_key(sheet)}",
        ]
        for attr in attr_candidates:
            val = getattr(self.settings, attr, None)
            if val:
                return _normalize_symbol_list(_split_symbols(val))
        return []

    async def _get_symbols_from_page_catalog(self, sheet: str) -> List[str]:
        for mod_path in ("core.sheets.page_catalog", "sheets.page_catalog"):
            try:
                mod = import_module(mod_path)
            except Exception:
                continue
            for fn_name in (
                "get_symbols_for_page",
                "get_page_symbols",
                "get_symbols",
                "list_symbols_for_page",
            ):
                fn = getattr(mod, fn_name, None)
                if callable(fn):
                    for kwargs in (
                        {"page": sheet},
                        {"sheet": sheet},
                        {"page_name": sheet},
                        {"name": sheet},
                        {},
                    ):
                        try:
                            res = await _call_maybe_async(fn, **kwargs)
                        except TypeError:
                            continue
                        except Exception:
                            continue
                        if isinstance(res, (list, tuple, set)):
                            syms = _normalize_symbol_list(_split_symbols(res))
                            if syms:
                                return syms
                    try:
                        res = await _call_maybe_async(fn, sheet)
                    except Exception:
                        continue
                    if isinstance(res, (list, tuple, set)):
                        syms = _normalize_symbol_list(_split_symbols(res))
                        if syms:
                            return syms
        return []

    async def _get_symbols_for_sheet_impl(self, sheet: str) -> List[str]:
        canon = _canonicalize_sheet_name(sheet)
        for source in (self._call_symbols_reader, self._get_symbols_from_env, self._get_symbols_from_settings, self._get_symbols_from_page_catalog):
            try:
                if source is self._call_symbols_reader:
                    res = await self._call_symbols_reader(canon)
                    raw = _split_symbols(res) if res is not None else []
                    syms = _normalize_symbol_list(raw)
                else:
                    syms = await source(canon)
                if syms:
                    return syms
            except Exception:
                continue
        return list(EMERGENCY_PAGE_SYMBOLS.get(canon, []))

    async def get_sheet_symbols(self, sheet: str) -> List[str]:
        canon = _canonicalize_sheet_name(sheet)
        cache_key = f"symbols::{canon}"
        cached = await self.cache.get(cache_key)
        if isinstance(cached, list) and cached:
            return list(cached)
        symbols = await self._get_symbols_for_sheet_impl(canon)
        if symbols:
            await self.cache.set(cache_key, list(symbols), ttl=300.0)
        return symbols

    async def get_page_symbols(self, page: str) -> List[str]:
        return await self.get_sheet_symbols(page)

    async def list_symbols_for_page(self, page: str) -> List[str]:
        return await self.get_sheet_symbols(page)

    async def list_symbols(self, page: str = "Market_Leaders") -> List[str]:
        return await self.get_sheet_symbols(page)

    async def get_symbols(self, *args: Any, **kwargs: Any) -> List[str]:
        page = ""
        if args:
            page = _safe_str(args[0])
        if not page:
            page = _safe_str(kwargs.get("page") or kwargs.get("sheet") or kwargs.get("name") or "Market_Leaders")
        return await self.get_sheet_symbols(page or "Market_Leaders")

    # --- quote context / provider preference -------------------------------
    def _resolve_quote_page_context(self, symbol: str, page: Optional[str]) -> str:
        canon = _canonicalize_sheet_name(_safe_str(page or ""))
        if canon in NON_KSA_EODHD_PRIMARY_PAGES:
            return canon
        if canon in INSTRUMENT_SHEETS:
            return canon
        sym = normalize_symbol(symbol)
        if not sym:
            return canon or "Market_Leaders"
        if sym.endswith("=F") or sym.endswith("=X") or sym in _COMMODITY_SYMBOL_HINTS:
            return "Commodities_FX"
        if sym in _ETF_SYMBOL_HINTS:
            return "Mutual_Funds"
        if sym.endswith(".SR") or re.match(r"^[0-9]{4}$", sym):
            return canon or "Market_Leaders"
        if sym.isalpha() and 1 <= len(sym) <= 5:
            return "Global_Markets"
        return canon or "Market_Leaders"

    def _page_primary_provider_for(self, page: str) -> str:
        canon = _canonicalize_sheet_name(_safe_str(page))
        return self._page_primary_provider.get(canon, "")

    def _provider_profile_key(self, symbol: str, page: Optional[str]) -> str:
        ctx = self._resolve_quote_page_context(symbol, page)
        primary = self._page_primary_provider_for(ctx) or "default"
        sym = normalize_symbol(symbol)
        is_ksa = sym.endswith(".SR") or bool(re.match(r"^[0-9]{4}$", sym))
        ksa_flag = "ksa" if is_ksa else "global"
        return f"{ctx}|{primary}|{ksa_flag}"

    # --- single quote pipeline --------------------------------------------
    async def _fetch_patch(self, provider_name: str, symbol: str) -> Optional[Dict[str, Any]]:
        provider_mod = await self.providers.get_provider(provider_name)
        if provider_mod is None:
            return None
        fn = _pick_provider_callable(provider_mod, "get_quote_patch", "fetch_quote", "get_quote", "fetch_patch")
        if fn is None:
            return None
        try:
            patch = await _call_maybe_async(fn, symbol)
        except Exception as exc:
            await self.providers.record_failure(provider_name, str(exc))
            return None
        if patch is None:
            await self.providers.record_failure(provider_name, "empty patch")
            return None
        await self.providers.record_success(provider_name)
        if isinstance(patch, dict):
            return dict(patch)
        return _model_to_dict(patch)

    def _providers_for(self, symbol: str, page: Optional[str] = None) -> List[str]:
        sym = normalize_symbol(symbol)
        is_ksa = sym.endswith(".SR") or bool(re.match(r"^[0-9]{4}$", sym))

        canon_page = _canonicalize_sheet_name(_safe_str(page or ""))
        override = list(self._page_provider_overrides.get(canon_page, []))
        prefer_provider = self._page_primary_provider_for(canon_page) if canon_page else ""

        if is_ksa:
            base = list(self.settings and getattr(self.settings, "ksa_provider_chain", None) or DEFAULT_KSA_PROVIDERS)
            if not base:
                base = list(DEFAULT_KSA_PROVIDERS)
            if self._ksa_disallow_eodhd:
                base = [p for p in base if p.lower() != "eodhd"]
            if override:
                override = [p for p in override if (not self._ksa_disallow_eodhd) or p.lower() != "eodhd"]
                merged = list(_dedupe_keep_order(override + [p for p in base if p not in override]))
            else:
                merged = list(_dedupe_keep_order(base))
            if prefer_provider and (not self._ksa_disallow_eodhd or prefer_provider.lower() != "eodhd"):
                merged = [prefer_provider] + [p for p in merged if p != prefer_provider]
            return merged

        base = list(self.settings and getattr(self.settings, "provider_chain", None) or DEFAULT_GLOBAL_PROVIDERS)
        if not base:
            base = list(DEFAULT_GLOBAL_PROVIDERS)
        if override:
            merged = list(_dedupe_keep_order(override + [p for p in base if p not in override]))
        else:
            merged = list(_dedupe_keep_order(base))
        if prefer_provider:
            merged = [prefer_provider] + [p for p in merged if p != prefer_provider]
        else:
            if canon_page in NON_KSA_EODHD_PRIMARY_PAGES and "eodhd" in merged:
                merged = ["eodhd"] + [p for p in merged if p != "eodhd"]
        return merged

    async def _rows_from_parallel_series(self, symbol: str) -> List[Dict[str, Any]]:
        for mod_path, fn_names in (
            ("core.providers.parallel_series", ("get_history_rows", "fetch_history_rows", "get_rows", "fetch_chart")),
            ("providers.parallel_series", ("get_history_rows", "fetch_history_rows", "get_rows", "fetch_chart")),
            ("core.parallel_series", ("get_history_rows", "fetch_history_rows", "get_rows", "fetch_chart")),
        ):
            try:
                mod = import_module(mod_path)
            except Exception:
                continue
            fn = _pick_provider_callable(mod, *fn_names)
            if fn is None:
                continue
            for kwargs in (
                {"symbol": symbol, "interval": "1d", "range": "1y"},
                {"symbol": symbol, "period": "1y"},
                {"symbol": symbol},
            ):
                try:
                    res = await _call_maybe_async(fn, **kwargs)
                except TypeError:
                    continue
                except Exception:
                    return []
                rows = _coerce_rows_list(res)
                if rows:
                    return rows
        return []

    def _coerce_history_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for r in rows or []:
            if not isinstance(r, dict):
                continue
            close = _as_float(r.get("close") or r.get("adjusted_close") or r.get("adjclose") or r.get("c"))
            if close is None:
                continue
            high = _as_float(r.get("high") or r.get("h")) or close
            low = _as_float(r.get("low") or r.get("l")) or close
            open_ = _as_float(r.get("open") or r.get("o")) or close
            vol = _as_float(r.get("volume") or r.get("v")) or 0.0
            ts = r.get("date") or r.get("timestamp") or r.get("t")
            out.append(dict(open=open_, high=high, low=low, close=close, volume=vol, date=ts))
        return out

    def _safe_mean(self, values: Sequence[float]) -> Optional[float]:
        seq = [v for v in values if v is not None]
        if not seq:
            return None
        return float(sum(seq) / len(seq))

    def _safe_std(self, values: Sequence[float]) -> Optional[float]:
        seq = [v for v in values if v is not None]
        if len(seq) < 2:
            return None
        m = self._safe_mean(seq) or 0.0
        var = sum((v - m) ** 2 for v in seq) / (len(seq) - 1)
        return math.sqrt(var) if var >= 0 else None

    def _quantile(self, values: Sequence[float], q: float) -> Optional[float]:
        seq = sorted(v for v in values if v is not None)
        if not seq:
            return None
        idx = q * (len(seq) - 1)
        lo = int(math.floor(idx))
        hi = int(math.ceil(idx))
        if lo == hi:
            return seq[lo]
        frac = idx - lo
        return seq[lo] * (1.0 - frac) + seq[hi] * frac

    def _compute_history_patch_from_rows(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        v5.53.0 [BUG-3b FIX]: week_52_position_pct now stored as PERCENT
        POINTS (0-100), not a fraction.  Same fix as BUG-3a but in the
        history-patch code path (used as a fallback when provider quote
        is sparse).  Both code paths must agree on units.
        """
        rows = self._coerce_history_rows(rows)
        if not rows:
            return {}
        closes = [r["close"] for r in rows if r.get("close") is not None]
        if not closes:
            return {}
        last_close = closes[-1]
        first_close = closes[0]
        out: Dict[str, Any] = {
            "current_price": last_close,
            "previous_close": closes[-2] if len(closes) > 1 else last_close,
            "open_price": rows[-1].get("open") or last_close,
            "day_high": rows[-1].get("high") or last_close,
            "day_low": rows[-1].get("low") or last_close,
            "volume": rows[-1].get("volume") or 0.0,
            "data_provider": "history_or_chart",
        }
        if len(closes) >= 252:
            out["week_52_high"] = max(closes[-252:])
            out["week_52_low"] = min(closes[-252:])
        else:
            out["week_52_high"] = max(closes)
            out["week_52_low"] = min(closes)

        # v5.47.3: percent_change stays as a FRACTION.
        if first_close not in (None, 0):
            out["percent_change"] = (last_close - first_close) / first_close
        # v5.53.0 [BUG-3b FIX]: percent points (0-100), not fraction.
        if out["week_52_high"] not in (None, 0) and out["week_52_high"] > out["week_52_low"]:
            out["week_52_position_pct"] = (last_close - out["week_52_low"]) / (out["week_52_high"] - out["week_52_low"]) * 100.0

        if len(closes) >= 22:
            returns_30 = [
                (closes[i] - closes[i - 1]) / closes[i - 1] for i in range(-21, 0) if closes[i - 1] not in (None, 0)
            ]
            std_30 = self._safe_std(returns_30)
            if std_30 is not None:
                out["volatility_30d"] = round(std_30 * math.sqrt(252) * 100.0, 4)
        if len(closes) >= 60:
            returns_60 = [
                (closes[i] - closes[i - 1]) / closes[i - 1] for i in range(-59, 0) if closes[i - 1] not in (None, 0)
            ]
            std_60 = self._safe_std(returns_60)
            if std_60 is not None:
                out["volatility_90d"] = round(std_60 * math.sqrt(252) * 100.0, 4)

        if len(closes) >= 14:
            window = closes[-14:]
            running_max = window[0]
            max_dd = 0.0
            for c in window:
                if c > running_max:
                    running_max = c
                if running_max not in (None, 0):
                    dd = (c - running_max) / running_max
                    if dd < max_dd:
                        max_dd = dd
            # v5.47.3: max_drawdown_1y stays as fraction.
            out["max_drawdown_1y"] = max_dd
        if len(closes) >= 60:
            returns_60 = [
                (closes[i] - closes[i - 1]) / closes[i - 1] for i in range(-59, 0) if closes[i - 1] not in (None, 0)
            ]
            q5 = self._quantile(returns_60, 0.05)
            if q5 is not None:
                # v5.47.3: var_95_1d stays as fraction.
                out["var_95_1d"] = abs(q5)

        # v5.50.0: candlestick pattern detection on the same OHLC rows.
        if _HAS_CANDLESTICKS and _detect_candle_patterns is not None:
            try:
                candle = _detect_candle_patterns(rows)
                if isinstance(candle, dict):
                    for k in _CANDLESTICK_FIELD_KEYS:
                        if k in candle:
                            out[k] = candle[k]
            except Exception:
                pass

        return out

    async def _fetch_history_patch(self, symbol: str) -> Optional[Dict[str, Any]]:
        rows = await self._rows_from_parallel_series(symbol)
        if not rows:
            return None
        patch = self._compute_history_patch_from_rows(rows)
        if not patch:
            return None
        return patch

    async def _get_history_patch_best_effort(self, symbol: str) -> Optional[Dict[str, Any]]:
        try:
            return await self._fetch_history_patch(symbol)
        except Exception:
            return None

    @staticmethod
    def _merge(base: Dict[str, Any], patch: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not isinstance(patch, dict):
            return base
        out = dict(base)
        for k, v in patch.items():
            if v is None or v == "":
                continue
            if out.get(k) in (None, "", [], {}):
                out[k] = v
        return out

    @staticmethod
    def _data_quality(row: Dict[str, Any]) -> str:
        critical = ["current_price", "name", "sector"]
        missing = sum(1 for k in critical if row.get(k) in (None, "", []))
        if missing == 0:
            return QuoteQuality.GOOD.value
        if missing < len(critical):
            return QuoteQuality.FAIR.value
        return QuoteQuality.MISSING.value

    async def _get_enriched_quote_impl(self, symbol: str, page: Optional[str] = None) -> Dict[str, Any]:
        normalized = normalize_symbol(symbol)
        cache_key = f"quote::{normalized}::{self._provider_profile_key(normalized, page)}"

        async def _build() -> Dict[str, Any]:
            row: Dict[str, Any] = {
                "symbol": normalized,
                "symbol_normalized": normalized,
                "requested_symbol": symbol,
            }
            warnings: List[str] = []
            sources: List[str] = []
            sem = asyncio.Semaphore(self._max_concurrent_quotes)
            async with sem:
                providers = self._providers_for(normalized, page=page)
                for provider in providers:
                    patch = await self._fetch_patch(provider, normalized)
                    if patch:
                        # v5.51.0 [FIX-3]: defensive try/except around canonicalize.
                        try:
                            canon_patch = _canonicalize_provider_row(patch, requested_symbol=symbol, normalized_symbol=normalized, provider=provider)
                        except Exception as canon_err:
                            logger.warning(
                                "[engine_v2 v%s] canonicalize failed for symbol=%r provider=%r: %s: %s — falling back to raw patch",
                                __version__, normalized, provider,
                                canon_err.__class__.__name__, canon_err,
                            )
                            canon_patch = dict(patch) if isinstance(patch, dict) else {}
                            canon_patch.setdefault("symbol", normalized)
                            canon_patch.setdefault("requested_symbol", symbol)
                            canon_patch.setdefault("data_provider", provider)
                            warnings.append(
                                "Provider {} canonicalize error ({}); raw patch used".format(
                                    provider, canon_err.__class__.__name__,
                                )
                            )
                        row = self._merge(row, canon_patch)
                        sources.append(provider)
                        if row.get("current_price") not in (None, "") and row.get("name") not in (None, ""):
                            break
            if row.get("current_price") in (None, ""):
                hp = await self._get_history_patch_best_effort(normalized)
                if hp:
                    try:
                        canon_patch = _canonicalize_provider_row(hp, requested_symbol=symbol, normalized_symbol=normalized, provider="history_or_chart")
                    except Exception as canon_err:
                        logger.warning(
                            "[engine_v2 v%s] canonicalize failed for symbol=%r provider=history_or_chart: %s: %s",
                            __version__, normalized,
                            canon_err.__class__.__name__, canon_err,
                        )
                        canon_patch = dict(hp) if isinstance(hp, dict) else {}
                        canon_patch.setdefault("symbol", normalized)
                        canon_patch.setdefault("requested_symbol", symbol)
                        canon_patch.setdefault("data_provider", "history_or_chart")
                    row = self._merge(row, canon_patch)
                    sources.append("history_or_chart")
                else:
                    warnings.append("No live provider data available")

            # v5.50.0: best-effort candlestick pattern detection on quote-only path.
            if (
                _HAS_CANDLESTICKS
                and _detect_candle_patterns is not None
                and _get_env_bool("ENGINE_CANDLESTICKS_ENABLED", True)
                and "candlestick_pattern" not in row
            ):
                try:
                    candle_history = await self._rows_from_parallel_series(normalized)
                    if candle_history:
                        coerced = self._coerce_history_rows(candle_history)
                        if coerced:
                            candle = _detect_candle_patterns(coerced)
                            if isinstance(candle, dict):
                                for k in _CANDLESTICK_FIELD_KEYS:
                                    if k in candle:
                                        row[k] = candle[k]
                except Exception:
                    pass

            row["data_sources"] = sources
            if not row.get("data_provider") and sources:
                row["data_provider"] = sources[0]
            row["quote_quality"] = self._data_quality(row)
            row["last_updated_utc"] = _now_utc_iso()
            row["last_updated_riyadh"] = _now_riyadh_iso()
            if warnings:
                row["warnings"] = "; ".join(warnings)
            row.setdefault("symbol", normalized)
            row.setdefault("requested_symbol", symbol)
            row.setdefault("symbol_normalized", normalized)
            row = _apply_symbol_context_defaults(row, symbol=normalized, page=page or "")
            return row

        async def _build_and_cache() -> Dict[str, Any]:
            built = await self.singleflight.do(cache_key, _build)
            quality = _safe_str(built.get("quote_quality") or self._data_quality(built))
            if quality != QuoteQuality.MISSING.value:
                ttl = float(_get_env_float("ENGINE_QUOTE_TTL_SECONDS", 60.0))
                await self.cache.set(cache_key, built, ttl=ttl)
            await self._record_symbol_snapshot(normalized, built)
            return built

        cached = await self.cache.get(cache_key)
        if isinstance(cached, dict) and cached:
            await self._record_symbol_snapshot(normalized, cached)
            return cached
        return await _build_and_cache()

    async def get_enriched_quote(self, symbol: str, *, page: Optional[str] = None, **_: Any) -> UnifiedQuote:
        row = await self._get_enriched_quote_impl(symbol, page=page)
        return UnifiedQuote(**row)

    async def get_enriched_quote_dict(self, symbol: str, *, page: Optional[str] = None, **_: Any) -> Dict[str, Any]:
        return await self._get_enriched_quote_impl(symbol, page=page)

    async def get_enriched_quotes(self, symbols: Sequence[str], *, page: Optional[str] = None, **_: Any) -> List[UnifiedQuote]:
        unique = _normalize_symbol_list(symbols)
        sem = asyncio.Semaphore(self._max_concurrent_quotes)

        async def _one(s: str) -> UnifiedQuote:
            async with sem:
                row = await self._get_enriched_quote_impl(s, page=page)
                return UnifiedQuote(**row)

        if not unique:
            return []
        # v5.51.0 [FIX-2]: return_exceptions=True so one bad symbol doesn't kill the whole gather.
        results = await asyncio.gather(
            *[_one(s) for s in unique], return_exceptions=True
        )
        out: List[UnifiedQuote] = []
        for sym, res in zip(unique, results):
            if isinstance(res, BaseException):
                logger.warning(
                    "[engine_v2 v%s] enrich failed for symbol=%r: %s: %s — emitting placeholder",
                    __version__, sym, res.__class__.__name__, res,
                )
                placeholder = {
                    "symbol": sym,
                    "symbol_normalized": sym,
                    "requested_symbol": sym,
                    "data_provider": "engine_v2_error",
                    "warnings": "Enrichment failed: {}: {}".format(
                        res.__class__.__name__, str(res)[:200]
                    ),
                    "last_updated_utc": _now_utc_iso(),
                    "last_updated_riyadh": _now_riyadh_iso(),
                }
                out.append(UnifiedQuote(**placeholder))
            else:
                out.append(res)
        return out

    async def get_enriched_quotes_batch(self, symbols: Sequence[str], *, page: Optional[str] = None, **_: Any) -> Dict[str, Dict[str, Any]]:
        unique = _normalize_symbol_list(symbols)
        sem = asyncio.Semaphore(self._max_concurrent_quotes)

        async def _one(s: str) -> Tuple[str, Dict[str, Any]]:
            async with sem:
                return s, await self._get_enriched_quote_impl(s, page=page)

        if not unique:
            return {}
        # v5.51.0 [FIX-2]: return_exceptions=True for batch resilience.
        results = await asyncio.gather(
            *[_one(s) for s in unique], return_exceptions=True
        )
        out: Dict[str, Dict[str, Any]] = {}
        failed_symbols: List[str] = []
        for sym, res in zip(unique, results):
            if isinstance(res, BaseException):
                failed_symbols.append(sym)
                logger.warning(
                    "[engine_v2 v%s] batch enrich failed for symbol=%r: %s: %s — emitting placeholder",
                    __version__, sym, res.__class__.__name__, res,
                )
                out[sym] = {
                    "symbol": sym,
                    "symbol_normalized": sym,
                    "requested_symbol": sym,
                    "data_provider": "engine_v2_error",
                    "warnings": "Enrichment failed: {}: {}".format(
                        res.__class__.__name__, str(res)[:200]
                    ),
                    "last_updated_utc": _now_utc_iso(),
                    "last_updated_riyadh": _now_riyadh_iso(),
                }
            else:
                key, row = res
                out[key] = row
        if failed_symbols:
            logger.warning(
                "[engine_v2 v%s] batch enrich: %d/%d symbols failed: %s",
                __version__, len(failed_symbols), len(unique),
                ",".join(failed_symbols[:20]),
            )
        return out

    async def get_quote(self, symbol: str, *, page: Optional[str] = None, **kwargs: Any) -> UnifiedQuote:
        return await self.get_enriched_quote(symbol, page=page, **kwargs)

    async def get_quote_dict(self, symbol: str, *, page: Optional[str] = None, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_enriched_quote_dict(symbol, page=page, **kwargs)

    async def get_quotes(self, symbols: Sequence[str], *, page: Optional[str] = None, **kwargs: Any) -> List[UnifiedQuote]:
        return await self.get_enriched_quotes(symbols, page=page, **kwargs)

    async def get_quotes_batch(self, symbols: Sequence[str], *, page: Optional[str] = None, **kwargs: Any) -> Dict[str, Dict[str, Any]]:
        return await self.get_enriched_quotes_batch(symbols, page=page, **kwargs)

    async def fetch_quote(self, symbol: str, *, page: Optional[str] = None, **kwargs: Any) -> UnifiedQuote:
        return await self.get_enriched_quote(symbol, page=page, **kwargs)

    async def fetch_quotes(self, symbols: Sequence[str], *, page: Optional[str] = None, **kwargs: Any) -> List[UnifiedQuote]:
        return await self.get_enriched_quotes(symbols, page=page, **kwargs)

    async def get_analysis_quotes_batch(self, symbols: Sequence[str], *, page: Optional[str] = None, **kwargs: Any) -> Dict[str, Dict[str, Any]]:
        return await self.get_enriched_quotes_batch(symbols, page=page, **kwargs)

    async def quotes_batch(self, symbols: Sequence[str], *, page: Optional[str] = None, **kwargs: Any) -> Dict[str, Dict[str, Any]]:
        return await self.get_enriched_quotes_batch(symbols, page=page, **kwargs)


    # --- special page builders --------------------------------------------
    def _build_data_dictionary_rows(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        sort_order = 0
        for sheet in _list_sheet_names_best_effort():
            try:
                _, headers, keys, _ = _schema_for_sheet(sheet)
            except Exception:
                continue
            for header, key in zip(headers, keys):
                sort_order += 1
                rows.append({
                    "sheet": sheet,
                    "group": "",
                    "header": header,
                    "key": key,
                    "dtype": "any",
                    "fmt": "",
                    "required": "no",
                    "source": "schema_registry_or_static",
                    "notes": "",
                    "sort_order": sort_order,
                })
        return rows

    def _build_insights_rows_fallback(self, top_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        sort_order = 0
        for source_row in top_rows[:25]:
            sym = _safe_str(source_row.get("symbol") or source_row.get("requested_symbol"))
            if not sym:
                continue
            for metric_key, metric_label in (
                ("overall_score", "Overall Score"),
                ("opportunity_score", "Opportunity Score"),
                ("recommendation", "Recommendation"),
                ("risk_bucket", "Risk Bucket"),
                ("confidence_bucket", "Confidence Bucket"),
            ):
                val = source_row.get(metric_key)
                if val in (None, ""):
                    continue
                sort_order += 1
                rows.append({
                    "section": "Top Picks",
                    "item": sym,
                    "metric": metric_label,
                    "value": _json_safe(val),
                    "notes": "",
                    "source": "fallback_engine_v2",
                    "sort_order": sort_order,
                })
        return rows

    @staticmethod
    def _top10_sort_key(row: Dict[str, Any]) -> Tuple[float, float, float, float]:
        opportunity = _as_float(row.get("opportunity_score")) or 0.0
        overall = _as_float(row.get("overall_score")) or 0.0
        confidence = _as_float(row.get("confidence_score")) or 0.0
        risk = _as_float(row.get("risk_score")) or 100.0
        return (-opportunity, -overall, -confidence, risk)

    async def _build_top10_rows_fallback(self, criteria: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[str], int, str]:
        """v5.52.0 [FIX-12]: per-page get_page_rows wrapped in try/except for resilience."""
        pages = list(criteria.get("pages_selected") or criteria.get("pages") or TOP10_ENGINE_DEFAULT_PAGES)
        pages = [_canonicalize_sheet_name(p) for p in pages if _safe_str(p)]
        pages = list(_dedupe_keep_order([p for p in pages if p in INSTRUMENT_SHEETS - {"Top_10_Investments"}]))
        if not pages:
            pages = list(TOP10_ENGINE_DEFAULT_PAGES)

        seen: Set[str] = set()
        candidate_rows: List[Dict[str, Any]] = []
        warnings: List[str] = []
        per_page_loaded: Dict[str, int] = {}

        for page in pages:
            try:
                page_payload = await self.get_page_rows(page=page, limit=int(criteria.get("per_page_limit", 200)))
            except Exception as exc:
                warnings.append(
                    "Top10 source page {} failed: {}: {}".format(
                        page, exc.__class__.__name__, str(exc)[:140],
                    )
                )
                logger.warning(
                    "[engine_v2 v%s] _build_top10_rows_fallback: source page %r failed: %s: %s",
                    __version__, page, exc.__class__.__name__, exc,
                )
                continue

            page_rows = page_payload.get("row_objects") or []
            if isinstance(page_rows, list):
                per_page_loaded[page] = len(page_rows)
                for row in page_rows:
                    if not isinstance(row, dict):
                        continue
                    sym = normalize_symbol(_safe_str(row.get("symbol") or row.get("requested_symbol")))
                    if not sym or sym in seen:
                        continue
                    seen.add(sym)
                    candidate_rows.append(dict(row))

        if not candidate_rows:
            warnings.append("No candidate rows found across selected pages")
            return [], warnings, 0, "engine_top10_fallback_empty"

        for row in candidate_rows:
            _compute_scores_fallback(row)
            _compute_recommendation(row)
        candidate_rows.sort(key=self._top10_sort_key)

        top_n = max(1, min(int(criteria.get("top_n", 10)), 50))
        selected = candidate_rows[:top_n]

        criteria_snapshot = _top10_criteria_snapshot(criteria)
        for rank, row in enumerate(selected, start=1):
            row["top10_rank"] = rank
            row["selection_reason"] = _top10_selection_reason(row)
            row["criteria_snapshot"] = criteria_snapshot

        return selected, warnings, len(candidate_rows), "engine_top10_fallback"

    # --- main page-rows entry point ---------------------------------------
    async def get_page_rows(
        self,
        page: Optional[str] = None,
        sheet: Optional[str] = None,
        sheet_name: Optional[str] = None,
        limit: int = 2000,
        offset: int = 0,
        mode: str = "",
        body: Optional[Dict[str, Any]] = None,
        **extras: Any,
    ) -> Dict[str, Any]:
        """
        v5.51.0 [FIX-1]: entire body wrapped in try/except so any exception
        becomes a structured error envelope instead of an unframed propagation
        that confuses the route layer.
        """
        debug = _get_env_bool("ENGINE_DEBUG", False)
        try:
            target_sheet, eff_limit, eff_offset, eff_mode, passthrough_body, _request_parts = _normalize_route_call_inputs(
                page=page, sheet=sheet, sheet_name=sheet_name, limit=limit, offset=offset,
                mode=mode, body=body, extras=extras,
            )
            if debug:
                logger.warning(
                    "[engine_v2 v%s] get_page_rows ENTRY page=%r limit=%d offset=%d mode=%r body_keys=%s",
                    __version__, target_sheet, eff_limit, eff_offset, eff_mode,
                    sorted(list((passthrough_body or {}).keys()))[:20],
                )

            try:
                spec, headers, keys, contract_source = _schema_for_sheet(target_sheet)
            except Exception as schema_exc:
                logger.warning(
                    "[engine_v2 v%s] schema lookup failed for %r: %s",
                    __version__, target_sheet, schema_exc,
                )
                spec, headers, keys, contract_source = None, [], [], "missing"

            if not headers or not keys:
                payload = self._finalize_payload({
                    "ok": False,
                    "engine_version": __version__,
                    "sheet": target_sheet, "page": target_sheet,
                    "headers": [], "keys": [],
                    "rows": [], "row_objects": [],
                    "warnings": [f"No usable schema contract for sheet {target_sheet!r}"],
                    "contract_source": contract_source,
                })
                return payload

            schema_only = _is_schema_only_body(passthrough_body)

            warnings: List[str] = []
            row_objects: List[Dict[str, Any]] = []

            if schema_only:
                payload = self._finalize_payload({
                    "ok": True,
                    "engine_version": __version__,
                    "sheet": target_sheet, "page": target_sheet,
                    "headers": headers, "keys": keys,
                    "rows": [], "row_objects": [],
                    "warnings": warnings,
                    "contract_source": contract_source,
                    "schema_only": True,
                })
                return payload

            # External-rows path: prefer prebuilt rows from rows_reader if available.
            external_rows = await self._get_rows_from_external_reader(target_sheet, criteria=passthrough_body)
            if external_rows:
                if debug:
                    logger.warning(
                        "[engine_v2 v%s] external rows reader returned %d rows for %r",
                        __version__, len(external_rows), target_sheet,
                    )
                for row in external_rows[: eff_offset + eff_limit]:
                    proj = _normalize_to_schema_keys(keys, headers, row)
                    proj = _apply_page_row_backfill(target_sheet, proj)
                    if target_sheet in INSTRUMENT_SHEETS:
                        _compute_scores_fallback(proj)
                        _compute_recommendation(proj)
                    row_objects.append(proj)

            # Special pages
            if target_sheet == "Data_Dictionary":
                dict_rows = self._build_data_dictionary_rows()
                row_objects = [_normalize_to_schema_keys(keys, headers, r) for r in dict_rows]
                if eff_offset:
                    row_objects = row_objects[eff_offset:]
                if eff_limit:
                    row_objects = row_objects[:eff_limit]
                payload = self._finalize_payload({
                    "ok": True,
                    "engine_version": __version__,
                    "sheet": target_sheet, "page": target_sheet,
                    "headers": headers, "keys": keys,
                    "rows": row_objects, "row_objects": row_objects,
                    "warnings": warnings,
                    "contract_source": contract_source,
                    "build_source": "engine_data_dictionary",
                })
                return payload

            if target_sheet == "Top_10_Investments":
                top_body, top_warnings = _normalize_top10_body_for_engine(passthrough_body, eff_limit)
                criteria = dict(top_body.get("criteria") or {})
                pages_from_body = _extract_top10_pages_from_body(top_body)
                if pages_from_body:
                    criteria["pages_selected"] = pages_from_body
                top_rows, top10_warnings, candidate_count, build_source = await self._build_top10_rows_fallback(criteria)
                warnings.extend(top_warnings)
                warnings.extend(top10_warnings)
                row_objects = []
                for row in top_rows:
                    proj = _normalize_to_schema_keys(keys, headers, row)
                    proj = _apply_page_row_backfill(target_sheet, proj)
                    for required in TOP10_REQUIRED_FIELDS:
                        if proj.get(required) in (None, ""):
                            proj[required] = row.get(required)
                    row_objects.append(proj)
                if eff_offset:
                    row_objects = row_objects[eff_offset:]
                if eff_limit:
                    row_objects = row_objects[:eff_limit]
                payload = self._finalize_payload({
                    "ok": True,
                    "engine_version": __version__,
                    "sheet": target_sheet, "page": target_sheet,
                    "headers": headers, "keys": keys,
                    "rows": row_objects, "row_objects": row_objects,
                    "warnings": warnings,
                    "contract_source": contract_source,
                    "build_source": build_source,
                    "candidate_count": candidate_count,
                })
                return payload

            if target_sheet == "Insights_Analysis":
                base_rows = row_objects or []
                if not base_rows:
                    try:
                        ml_payload = await self.get_page_rows(page="Market_Leaders", limit=20)
                        base_rows = list(ml_payload.get("row_objects") or [])
                    except Exception as ml_exc:
                        warnings.append("Insights base load failed: {}".format(ml_exc.__class__.__name__))
                        base_rows = []
                insights_rows = self._build_insights_rows_fallback(base_rows)
                row_objects = [_normalize_to_schema_keys(keys, headers, r) for r in insights_rows]
                if eff_offset:
                    row_objects = row_objects[eff_offset:]
                if eff_limit:
                    row_objects = row_objects[:eff_limit]
                payload = self._finalize_payload({
                    "ok": True,
                    "engine_version": __version__,
                    "sheet": target_sheet, "page": target_sheet,
                    "headers": headers, "keys": keys,
                    "rows": row_objects, "row_objects": row_objects,
                    "warnings": warnings,
                    "contract_source": contract_source,
                    "build_source": "engine_insights_fallback",
                })
                return payload

            # Instrument-style sheet path
            if target_sheet in INSTRUMENT_SHEETS:
                if not row_objects:
                    body_symbols = _extract_requested_symbols_from_body(passthrough_body, limit=eff_limit) if passthrough_body else []
                    if body_symbols:
                        symbols = body_symbols
                        if debug:
                            logger.warning(
                                "[engine_v2 v%s] %s: %d body-supplied symbols",
                                __version__, target_sheet, len(symbols),
                            )
                    else:
                        symbols = await self.get_sheet_symbols(target_sheet)
                        if debug:
                            logger.warning(
                                "[engine_v2 v%s] %s: %d symbols from sheet config",
                                __version__, target_sheet, len(symbols),
                            )

                    if eff_limit:
                        symbols = symbols[: eff_offset + eff_limit]
                    quote_map = await self.get_enriched_quotes_batch(symbols, page=target_sheet)
                    requested_list = list(symbols)
                    normalized_list = [normalize_symbol(s) for s in requested_list]
                    for key_name, val in quote_map.items():
                        meta = self._resolve_symbol_meta(key_name, val, requested_list, normalized_list)
                        if meta is None:
                            continue
                        requested_field, normalized_field = meta
                        row = dict(val) if isinstance(val, dict) else _model_to_dict(val)
                        row.setdefault("symbol", normalized_field)
                        row.setdefault("requested_symbol", requested_field)
                        row.setdefault("symbol_normalized", normalized_field)
                        proj = _normalize_to_schema_keys(keys, headers, row)
                        proj = _apply_page_row_backfill(target_sheet, proj)
                        _compute_scores_fallback(proj)
                        _compute_recommendation(proj)
                        snapshot = await self._get_symbol_snapshot(normalized_field)
                        if snapshot:
                            template = _normalize_to_schema_keys(keys, headers, snapshot)
                            proj = _merge_missing_fields(proj, template)
                        row_objects.append(proj)

                _apply_rank_overall(row_objects)
                if eff_offset:
                    row_objects = row_objects[eff_offset:]
                if eff_limit:
                    row_objects = row_objects[:eff_limit]

                payload = self._finalize_payload({
                    "ok": True,
                    "engine_version": __version__,
                    "sheet": target_sheet, "page": target_sheet,
                    "headers": headers, "keys": keys,
                    "rows": row_objects, "row_objects": row_objects,
                    "warnings": warnings,
                    "contract_source": contract_source,
                    "build_source": "engine_instrument_pipeline",
                })
                return payload

            # Unknown sheet: schema-only payload.
            payload = self._finalize_payload({
                "ok": True,
                "engine_version": __version__,
                "sheet": target_sheet, "page": target_sheet,
                "headers": headers, "keys": keys,
                "rows": [], "row_objects": [],
                "warnings": warnings + [f"No build pipeline for sheet {target_sheet!r}; returning schema-only payload"],
                "contract_source": contract_source,
                "build_source": "engine_schema_only_unknown_sheet",
            })
            return payload

        except Exception as exc:
            # v5.51.0 [FIX-1]: structured error envelope.
            err_class = exc.__class__.__name__
            err_text = str(exc)[:500]
            logger.warning(
                "[engine_v2 v%s] get_page_rows EXCEPTION page=%r sheet=%r sheet_name=%r: %s: %s",
                __version__, page, sheet, sheet_name, err_class, err_text,
            )
            try:
                target_canon = _canonicalize_sheet_name(_safe_str(page or sheet or sheet_name) or "Market_Leaders")
                _, hdrs_fb, ks_fb, _ = _schema_for_sheet(target_canon)
            except Exception:
                target_canon = _canonicalize_sheet_name(_safe_str(page or sheet or sheet_name) or "Market_Leaders")
                hdrs_fb, ks_fb = list(INSTRUMENT_CANONICAL_HEADERS), list(INSTRUMENT_CANONICAL_KEYS)
            payload = self._finalize_payload({
                "ok": False,
                "engine_version": __version__,
                "sheet": target_canon, "page": target_canon,
                "headers": hdrs_fb, "keys": ks_fb,
                "rows": [], "row_objects": [],
                "warnings": ["Engine exception: {}: {}".format(err_class, err_text)],
                "_engine_error": err_text,
                "_engine_error_class": err_class,
                "contract_source": "exception_envelope",
                "build_source": "engine_exception_envelope_v5_51",
            })
            return payload

    async def get_sheet(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_page_rows(*args, **kwargs)

    async def get_sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_page_rows(*args, **kwargs)

    async def sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_page_rows(*args, **kwargs)

    async def build_sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_page_rows(*args, **kwargs)

    # --- contract aliases --------------------------------------------------
    def get_sheet_contract(self, sheet: str) -> Dict[str, Any]:
        canon = _canonicalize_sheet_name(sheet)
        try:
            spec, headers, keys, src = _schema_for_sheet(canon)
        except Exception:
            headers, keys, src = list(INSTRUMENT_CANONICAL_HEADERS), list(INSTRUMENT_CANONICAL_KEYS), "static_canonical_contract_fallback"
        return {"sheet": canon, "page": canon, "headers": headers, "keys": keys, "contract_source": src}

    def get_page_contract(self, page: str) -> Dict[str, Any]:
        return self.get_sheet_contract(page)

    def get_sheet_schema(self, sheet: str) -> Dict[str, Any]:
        return self.get_sheet_contract(sheet)

    def get_page_schema(self, page: str) -> Dict[str, Any]:
        return self.get_sheet_contract(page)

    def get_headers_for_sheet(self, sheet: str) -> List[str]:
        return list(self.get_sheet_contract(sheet).get("headers") or [])

    def get_keys_for_sheet(self, sheet: str) -> List[str]:
        return list(self.get_sheet_contract(sheet).get("keys") or [])

    # --- health ------------------------------------------------------------
    async def health(self) -> Dict[str, Any]:
        provider_stats = await self.providers.get_stats()
        return {
            "ok": True,
            "engine_version": __version__,
            "providers": provider_stats,
            "feature_flags": self.feature_flags,
            "cache_size": len(await self.cache.keys()),
            "snapshot_size": len(self._symbol_snapshot),
            "candlesticks_available": _HAS_CANDLESTICKS,
            "schema_registry_available": _SCHEMA_AVAILABLE,
            "normalize_helpers_available": _HAS_NORMALIZE_HELPERS,
        }

    async def get_health(self) -> Dict[str, Any]:
        return await self.health()

    async def health_check(self) -> Dict[str, Any]:
        return await self.health()

    async def get_stats(self) -> Dict[str, Any]:
        return await self.health()


# =============================================================================
# Top-level helpers exposed for external callers
# =============================================================================
def normalize_row_to_schema(sheet: str, row: Dict[str, Any]) -> Dict[str, Any]:
    canon = _canonicalize_sheet_name(sheet)
    try:
        _, headers, keys, _ = _schema_for_sheet(canon)
    except Exception:
        headers, keys = list(INSTRUMENT_CANONICAL_HEADERS), list(INSTRUMENT_CANONICAL_KEYS)
    proj = _normalize_to_schema_keys(keys, headers, dict(row or {}))
    return _apply_page_row_backfill(canon, proj)


# =============================================================================
# Module singleton
# =============================================================================
_ENGINE_INSTANCE: Optional[DataEngineV5] = None
_ENGINE_LOCK = asyncio.Lock()


async def get_engine() -> DataEngineV5:
    global _ENGINE_INSTANCE
    if _ENGINE_INSTANCE is not None:
        return _ENGINE_INSTANCE
    async with _ENGINE_LOCK:
        if _ENGINE_INSTANCE is None:
            _ENGINE_INSTANCE = DataEngineV5()
        return _ENGINE_INSTANCE


async def close_engine() -> None:
    global _ENGINE_INSTANCE
    async with _ENGINE_LOCK:
        if _ENGINE_INSTANCE is not None:
            try:
                await _ENGINE_INSTANCE.aclose()
            except Exception:
                pass
            _ENGINE_INSTANCE = None


def get_engine_if_ready() -> Optional[DataEngineV5]:
    return _ENGINE_INSTANCE


def peek_engine() -> Optional[DataEngineV5]:
    return _ENGINE_INSTANCE


def get_cache() -> Optional[MultiLevelCache]:
    return _ENGINE_INSTANCE.cache if _ENGINE_INSTANCE is not None else None


# =============================================================================
# Legacy aliases
# =============================================================================
ENGINE = None       # populated lazily on first get_engine() call
engine = None
_ENGINE = None
DataEngine = DataEngineV5
DataEngineV4 = DataEngineV5
DataEngineV3 = DataEngineV5
DataEngineV2 = DataEngineV5


__all__ = [
    "__version__",
    "DataEngineV5",
    "DataEngine",
    "DataEngineV4",
    "DataEngineV3",
    "DataEngineV2",
    "UnifiedQuote",
    "QuoteQuality",
    "DataSource",
    "MultiLevelCache",
    "SingleFlight",
    "ProviderRegistry",
    "ProviderState",
    "get_engine",
    "close_engine",
    "get_engine_if_ready",
    "peek_engine",
    "get_cache",
    "get_sheet_spec",
    "normalize_row_to_schema",
    "normalize_symbol",
    "get_symbol_info",
    "collapse_to_canonical",
    "DETAILED_TOKENS",
    "CANONICAL_TOKENS",
    "SCHEMA_REGISTRY",
    "INSTRUMENT_CANONICAL_KEYS",
    "INSTRUMENT_CANONICAL_HEADERS",
    "INSTRUMENT_SHEETS",
    "SPECIAL_SHEETS",
    "TOP10_REQUIRED_FIELDS",
]
