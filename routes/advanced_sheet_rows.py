#!/usr/bin/env python3
# routes/advanced_sheet_rows.py
"""
================================================================================
Advanced Sheet-Rows Router — v4.0.0  (V1 SIBLING / V2.5.0-ALIGNED / FAIL-SOFT)
================================================================================
VERSIONED PATHS • SCHEMA-FIRST • INDEPENDENT DATA PATH • STABLE ENVELOPE
JSON-SAFE • GET+POST MERGED • HEADERS-ONLY / SCHEMA-ONLY • CANONICAL WIDTHS
INTERNAL-FIELD HARDENED • CONSERVATIVE PLACEHOLDERS

Purpose
-------
This module is the v1-prefixed sibling of `routes.advanced_analysis`. It owns
the versioned sheet-rows paths under `/v1/advanced/...`:
- /v1/advanced/sheet-rows
- /v1/advanced/health
- /v1/advanced/schema/sheet-spec  (alias for advanced_analysis equivalent)

`routes.analysis_sheet_rows` v4.1.2 proxies its requests to BOTH
`routes.advanced_analysis._run_advanced_sheet_rows_impl` (root paths) AND
`routes.advanced_sheet_rows._run_advanced_sheet_rows_impl` (this module),
then picks the better-quality payload via `_pick_best_payload`. For that
comparison to be meaningful, this module must be an INDEPENDENT data path,
not a thin delegate to `advanced_analysis`. The two siblings can diverge
in two specific dimensions:

  1. Static fallback width. `advanced_analysis` v4.0.0 has an 80-column
     fallback that pads to 84 with placeholder columns. This module
     ships an explicit 85-column fallback that matches
     `core.sheets.schema_registry` v2.5.0 (and `core.enriched_quote`
     v4.2.0): adds `upside_pct` after `intrinsic_value` and 4 view
     columns (`fundamental_view`, `technical_view`, `risk_view`,
     `value_view`) after the score block. Top_10_Investments is 88
     (= 85 + 3 extras).

  2. Placeholder behavior. `advanced_analysis` v4.0.0 fabricates values
     for missing fields (`recommendation="Accumulate"`, `overall_score=99`,
     etc.). For a financial product these synthetic numbers can be acted
     upon by the user. This module follows `analysis_sheet_rows` v4.1.2:
     identity columns get filled, everything numeric returns None, and
     a `warnings` field is set explicitly to "Placeholder fallback —
     no live data".

When `core.sheets.schema_registry` is reachable (the normal production path)
both modules pick up whatever the registry says — including 90/93 from a
v2.6.0 registry. The static-fallback divergence above only matters when the
registry import has failed.

What this revision changed (vs. the prior broken file)
------------------------------------------------------
- REBUILD: prior file shipped a 76-column fallback that pre-dated
    schema_registry v2.3.0. That meant any production request landing on
    the v1/advanced path during a registry import failure emitted rows
    that were 9-14 columns short of every other consumer in the dispatch
    chain — every Apps Script poll that hit those rows wrote partial
    columns into the sheet and silently corrupted the daily sync.
- ADD: `upside_pct` and 4 view tokens are now in the static fallback.
- ADD: conservative placeholder behavior — never fabricate numeric values.
- ADD: internal-field stripping (`_skip_*`, `_internal_*`, `_meta_*`,
    `_debug_*`, `_trace_*`, plus the explicit hard-strip set used by
    `core.enriched_quote` v4.2.0).
- ADD: registry-first contract resolution; static fallback only fires when
    the registry import has failed.
- ADD: `status: "warn"` from engine v5.47.4 is treated as success-with-caveat.
- KEEP: `_run_advanced_sheet_rows_impl(request, body, mode, include_matrix_q,
    token, x_app_token, x_api_key, authorization, x_request_id) -> Dict[str, Any]`
    signature — `analysis_sheet_rows` v4.1.2 proxies to this name and
    breaks if the kwargs change.

Notes on schema drift in the 4 sibling files
--------------------------------------------
The 4 files attached alongside this rebuild span a range of schema
versions:
  - core.enriched_quote v4.2.0:   v2.5.0 alignment (85/88, has upside_pct, has views)
  - core.sheets.page_catalog 3.2.0: schema-agnostic (registry-first)
  - routes.advanced_analysis 4.0.0: pre-v2.5.0 (80-key list pads to 84,
                                    NO upside_pct, has views)
  - routes.analysis_sheet_rows 4.1.2: pre-v2.5.0 (80-key list, expects 80,
                                    NO upside_pct, has views)

This file (advanced_sheet_rows v4.0.0) is at v2.5.0 alignment. The other
3 files would benefit from the same alignment but they're not in scope
for this revision — flagged separately in the delivery note.
================================================================================
"""

from __future__ import annotations

import inspect
import json
import logging
import math
import os
import re
import time
import uuid
from dataclasses import is_dataclass
from datetime import date, datetime, time as dt_time
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

logger = logging.getLogger("routes.advanced_sheet_rows")
logger.addHandler(logging.NullHandler())

ADVANCED_SHEET_ROWS_VERSION = "4.0.0"
router = APIRouter(prefix="/v1/advanced", tags=["Advanced Sheet Rows"])

# ---------------------------------------------------------------------------
# Page constants
# ---------------------------------------------------------------------------
_TOP10_PAGE = "Top_10_Investments"
_INSIGHTS_PAGE = "Insights_Analysis"
_DICTIONARY_PAGE = "Data_Dictionary"
_SPECIAL_PAGES = {_TOP10_PAGE, _INSIGHTS_PAGE, _DICTIONARY_PAGE}

# v2.5.0 widths (85 / 88 / 7 / 9). Production registry-first path picks up
# whatever the deployed registry has (e.g. 90/93 under v2.6.0).
_EXPECTED_SHEET_LENGTHS: Dict[str, int] = {
    "Market_Leaders": 85,
    "Global_Markets": 85,
    "Commodities_FX": 85,
    "Mutual_Funds": 85,
    "My_Portfolio": 85,
    "My_Investments": 85,
    _TOP10_PAGE: 88,
    _INSIGHTS_PAGE: 7,
    _DICTIONARY_PAGE: 9,
}

_TOP10_REQUIRED_FIELDS: Tuple[str, ...] = (
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
)

_TOP10_REQUIRED_HEADERS: Dict[str, str] = {
    "top10_rank": "Top10 Rank",
    "selection_reason": "Selection Reason",
    "criteria_snapshot": "Criteria Snapshot",
}

# ---------------------------------------------------------------------------
# Optional imports — same as advanced_analysis / analysis_sheet_rows so the
# three modules behave consistently when these are available
# ---------------------------------------------------------------------------
try:
    from core.sheets.schema_registry import (  # type: ignore
        get_sheet_headers,
        get_sheet_keys,
        get_sheet_len,
        get_sheet_spec,
    )
except Exception:
    get_sheet_headers = None  # type: ignore
    get_sheet_keys = None  # type: ignore
    get_sheet_len = None  # type: ignore
    get_sheet_spec = None  # type: ignore

try:
    from core.sheets.page_catalog import (  # type: ignore
        CANONICAL_PAGES,
        FORBIDDEN_PAGES,
        allowed_pages,
        get_route_family,
        normalize_page_name,
    )
except Exception:
    CANONICAL_PAGES = []  # type: ignore
    FORBIDDEN_PAGES = {"KSA_Tadawul", "Advisor_Criteria"}  # type: ignore

    def allowed_pages() -> List[str]:  # type: ignore
        return list(CANONICAL_PAGES) if CANONICAL_PAGES else []

    def normalize_page_name(name: str, allow_output_pages: bool = True) -> str:  # type: ignore
        return (name or "").strip().replace(" ", "_")

    def get_route_family(name: str) -> str:  # type: ignore
        if name == _TOP10_PAGE:
            return "top10"
        if name == _INSIGHTS_PAGE:
            return "insights"
        if name == _DICTIONARY_PAGE:
            return "dictionary"
        return "instrument"

try:
    from core.config import auth_ok, get_settings_cached, is_open_mode, mask_settings  # type: ignore
except Exception:
    auth_ok = None  # type: ignore
    is_open_mode = None  # type: ignore
    mask_settings = None  # type: ignore

    def get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None

# Adapter import — prefer data_engine_v2 (the active engine) over the legacy
# data_engine. Same precedence analysis_sheet_rows v4.1.2 settled on.
CORE_GET_SHEET_ROWS_SOURCE = "unavailable"
try:
    from core.data_engine_v2 import get_sheet_rows as core_get_sheet_rows  # type: ignore
    CORE_GET_SHEET_ROWS_SOURCE = "core.data_engine_v2.get_sheet_rows"
except Exception:
    try:
        from core.data_engine import get_sheet_rows as core_get_sheet_rows  # type: ignore
        CORE_GET_SHEET_ROWS_SOURCE = "core.data_engine.get_sheet_rows"
    except Exception:
        core_get_sheet_rows = None  # type: ignore


# ---------------------------------------------------------------------------
# v2.5.0 canonical fallback (85 cols + 3 Top10 extras = 88).
#
# Only used when the schema registry import has failed. With the registry
# healthy this list is never read. It mirrors:
#   core.enriched_quote v4.2.0._FALLBACK_INSTRUMENT_KEYS
#   core.sheets.schema_registry v2.5.0._canonical_instrument_columns()
#
# Differences vs. routes.advanced_analysis v4.0.0:
#   - 5 more entries (85 vs 80)
#   - includes upside_pct  (Valuation group, position 49)
#   - includes 4 view tokens already (advanced_analysis has them too,
#     just at different positions because of the 80-key layout)
# ---------------------------------------------------------------------------
_CANONICAL_85_KEYS: List[str] = [
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
    # Valuation (7)  — includes upside_pct (added in registry v2.4.0)
    "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio", "intrinsic_value",
    "upside_pct", "valuation_score",
    # Forecast (9)
    "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
    "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "forecast_confidence", "confidence_score", "confidence_bucket",
    # Scores (7)
    "value_score", "quality_score", "momentum_score", "growth_score",
    "overall_score", "opportunity_score", "rank_overall",
    # Views (4) — added in registry v2.3.0
    "fundamental_view", "technical_view", "risk_view", "value_view",
    # Recommendation (4)
    "recommendation", "recommendation_reason", "horizon_days", "invest_period_label",
    # Portfolio (6)
    "position_qty", "avg_cost", "position_cost", "position_value",
    "unrealized_pl", "unrealized_pl_pct",
    # Provenance (4)
    "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",
]

_CANONICAL_85_HEADERS: List[str] = [
    # Identity
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
    # Price
    "Current Price", "Previous Close", "Open", "Day High", "Day Low",
    "52W High", "52W Low", "Price Change", "Percent Change", "52W Position %",
    # Liquidity
    "Volume", "Avg Volume 10D", "Avg Volume 30D", "Market Cap", "Float Shares", "Beta (5Y)",
    # Fundamentals
    "P/E (TTM)", "P/E (Forward)", "EPS (TTM)", "Dividend Yield", "Payout Ratio",
    "Revenue (TTM)", "Revenue Growth YoY", "Gross Margin", "Operating Margin",
    "Profit Margin", "Debt/Equity", "Free Cash Flow (TTM)",
    # Risk
    "RSI (14)", "Volatility 30D", "Volatility 90D", "Max Drawdown 1Y",
    "VaR 95% (1D)", "Sharpe (1Y)", "Risk Score", "Risk Bucket",
    # Valuation
    "P/B", "P/S", "EV/EBITDA", "PEG", "Intrinsic Value", "Upside %", "Valuation Score",
    # Forecast
    "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M",
    "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M",
    "Forecast Confidence", "Confidence Score", "Confidence Bucket",
    # Scores
    "Value Score", "Quality Score", "Momentum Score", "Growth Score",
    "Overall Score", "Opportunity Score", "Rank (Overall)",
    # Views
    "Fundamental View", "Technical View", "Risk View", "Value View",
    # Recommendation
    "Recommendation", "Recommendation Reason", "Horizon Days", "Invest Period Label",
    # Portfolio
    "Position Qty", "Avg Cost", "Position Cost", "Position Value",
    "Unrealized P/L", "Unrealized P/L %",
    # Provenance
    "Data Provider", "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",
]

assert len(_CANONICAL_85_KEYS) == 85, f"v2.5.0 instrument keys must be 85, got {len(_CANONICAL_85_KEYS)}"
assert len(_CANONICAL_85_HEADERS) == 85, f"v2.5.0 instrument headers must be 85, got {len(_CANONICAL_85_HEADERS)}"

_INSIGHTS_HEADERS = ["Section", "Item", "Symbol", "Metric", "Value", "Notes", "Last Updated (Riyadh)"]
_INSIGHTS_KEYS = ["section", "item", "symbol", "metric", "value", "notes", "last_updated_riyadh"]

_DICTIONARY_HEADERS = ["Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"]
_DICTIONARY_KEYS = ["sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes"]

EMERGENCY_PAGE_SYMBOLS: Dict[str, List[str]] = {
    "Market_Leaders": ["2222.SR", "1120.SR", "2010.SR", "7010.SR", "AAPL", "MSFT", "NVDA", "GOOGL"],
    "Global_Markets": ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AVGO"],
    "Commodities_FX": ["GC=F", "BZ=F", "SI=F", "EURUSD=X", "GBPUSD=X", "JPY=X", "SAR=X", "CL=F"],
    "Mutual_Funds": ["SPY", "QQQ", "VTI", "VOO", "IWM"],
    "My_Portfolio": ["2222.SR", "AAPL", "MSFT", "QQQ", "GC=F"],
    "My_Investments": ["2222.SR", "AAPL", "MSFT"],
    _INSIGHTS_PAGE: ["2222.SR", "AAPL", "GC=F"],
    _TOP10_PAGE: ["2222.SR", "1120.SR", "AAPL", "MSFT", "NVDA"],
}

# Field-alias hints — superset of the engine v5.47.4 mappings used by
# analysis_sheet_rows v4.1.2. Recognising both the canonical key and the
# engine-emitted mirror name lets _normalize_to_schema_keys find values
# regardless of which spelling the engine wrote.
_FIELD_ALIAS_HINTS: Dict[str, List[str]] = {
    "symbol": ["ticker", "code", "instrument", "security", "requested_symbol", "symbol_normalized"],
    "ticker": ["symbol", "code", "instrument", "security", "requested_symbol"],
    "name": ["short_name", "long_name", "display_name", "instrument_name", "shortName", "longName"],
    "asset_class": ["asset_type", "quote_type", "instrument_type", "type", "quoteType"],
    "exchange": ["exchange_name", "full_exchange_name", "market", "mic", "exchangeName"],
    "currency": ["currency_code", "ccy", "fx_currency", "financialCurrency"],
    "country": ["country_name", "domicile_country", "countryName"],
    "sector": ["sector_name", "gics_sector", "sectorDisp"],
    "industry": ["industry_name", "gics_industry", "industryDisp"],
    "current_price": ["price", "last_price", "last", "close", "market_price", "nav", "regularMarketPrice", "lastPrice"],
    "previous_close": ["prev_close", "prior_close", "regularMarketPreviousClose", "previousClose"],
    "open_price": ["open", "day_open", "regularMarketOpen"],
    "day_high": ["high", "session_high", "regularMarketDayHigh"],
    "day_low": ["low", "session_low", "regularMarketDayLow"],
    "week_52_high": ["fiftyTwoWeekHigh", "fifty_two_week_high", "year_high", "52_week_high"],
    "week_52_low": ["fiftyTwoWeekLow", "fifty_two_week_low", "year_low", "52_week_low"],
    "week_52_position_pct": ["position_52w_pct", "fifty_two_week_position_pct"],
    "price_change": ["change", "net_change", "regularMarketChange"],
    "percent_change": ["pct_change", "change_pct", "changePercent", "regularMarketChangePercent"],
    "volume": ["trade_volume", "regularMarketVolume"],
    "avg_volume_10d": ["averageDailyVolume10Day", "avg_vol_10d", "average_volume_10d"],
    "avg_volume_30d": ["averageDailyVolume3Month", "averageVolume", "avg_vol_30d"],
    "market_cap": ["marketCap", "market_capitalization"],
    "float_shares": ["floatShares", "free_float_shares"],
    "beta_5y": ["beta"],
    "pe_ttm": ["trailingPE", "peRatio", "pe"],
    "pe_forward": ["forwardPE", "forward_pe"],
    "eps_ttm": ["trailingEps", "eps"],
    "dividend_yield": ["dividendYield", "trailingAnnualDividendYield"],
    "payout_ratio": ["payoutRatio"],
    "revenue_ttm": ["totalRevenue", "Revenue"],
    "revenue_growth_yoy": ["revenueGrowth"],
    "gross_margin": ["grossMargins"],
    "operating_margin": ["operatingMargins"],
    "profit_margin": ["profitMargins", "netMargin"],
    "debt_to_equity": ["debtToEquity", "d_e_ratio"],
    "free_cash_flow_ttm": ["freeCashflow", "fcf_ttm"],
    "rsi_14": ["rsi", "rsi14"],
    "volatility_30d": ["vol30d"],
    "volatility_90d": ["vol90d"],
    "max_drawdown_1y": ["maxDrawdown1y"],
    "var_95_1d": ["var95_1d"],
    "sharpe_1y": ["sharpe1y", "sharpeRatio"],
    "pb_ratio": ["pb", "priceToBook"],
    "ps_ratio": ["ps", "priceToSalesTrailing12Months"],
    "ev_ebitda": ["ev_to_ebitda", "evToEbitda", "enterpriseToEbitda"],
    "peg_ratio": ["peg", "pegRatio"],
    "intrinsic_value": ["fairValue", "fair_value", "dcf"],
    "upside_pct": ["upsidePct", "upside_percent", "intrinsic_upside"],
    "valuation_score": ["valuationScore"],
    "overall_score": ["score", "composite_score", "compositeScore"],
    "opportunity_score": ["opportunity"],
    "forecast_confidence": ["confidence", "confidence_pct", "ai_confidence"],
    "confidence_score": ["confidence", "confidence_pct", "modelConfidenceScore"],
    "expected_roi_1m": ["roi_1m", "expected_roi_1m_pct"],
    "expected_roi_3m": ["roi_3m", "expected_roi_3m_pct"],
    "expected_roi_12m": ["roi_12m", "expected_roi_12m_pct"],
    "forecast_price_1m": ["target_price_1m"],
    "forecast_price_3m": ["target_price_3m", "targetMeanPrice"],
    "forecast_price_12m": ["target_price_12m", "targetMedianPrice"],
    "fundamental_view": ["fundamentalView", "fund_view", "fundamentals_view"],
    "technical_view": ["technicalView", "tech_view"],
    "risk_view": ["riskView"],
    "value_view": ["valueView", "valuation_view"],
    "recommendation": ["signal", "rating", "action", "reco"],
    "recommendation_reason": ["rationale", "reasoning", "signal_reason", "reason", "thesis"],
    "data_provider": ["provider", "source_provider", "primary_provider", "provider_primary"],
    "last_updated_utc": ["updated_at", "as_of_utc", "last_updated", "updated_at_utc", "asOf"],
    "last_updated_riyadh": ["timestamp_riyadh", "as_of_riyadh", "updated_at_riyadh"],
    "warnings": ["warning", "messages", "errors", "issues"],
    "top10_rank": ["rank", "top_rank", "position_rank"],
    "selection_reason": ["reason", "selection_notes", "selector_reason"],
    "criteria_snapshot": ["criteria", "criteria_json", "snapshot"],
    "group": ["section"],
    "fmt": ["format"],
    "dtype": ["type", "data_type"],
    "notes": ["description", "commentary"],
    "horizon_days": ["horizon", "days_horizon"],
    "invest_period_label": ["periodLabel", "horizonLabel"],
}

# Internal-field stripping — same set as enriched_quote v4.2.0 / analysis_sheet_rows v4.1.2.
_INTERNAL_FIELD_PREFIXES: Tuple[str, ...] = ("_skip_", "_internal_", "_meta_", "_debug_", "_trace_")
_INTERNAL_FIELDS_TO_STRIP_HARD: frozenset = frozenset({
    "_placeholder",
    "_skip_recommendation_synthesis",
    "unit_normalization_warnings",
    "intrinsic_value_source",
})


def _strip_internal_fields(row: Any) -> Any:
    """Remove engine internal coordination flags from a row dict.

    Defence-in-depth: engine v5.47.4 strips these at source, but rows from
    the legacy engine, proxies, or cached snapshots may still carry them.
    """
    if not isinstance(row, dict):
        return row
    keys_to_remove: List[str] = []
    for k in list(row.keys()):
        ks = str(k)
        if ks in _INTERNAL_FIELDS_TO_STRIP_HARD:
            keys_to_remove.append(k)
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
# Generic helpers
# =============================================================================
def _strip(v: Any) -> str:
    try:
        s = str(v).strip()
        return "" if s.lower() in {"none", "null"} else s
    except Exception:
        return ""


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return None if (math.isnan(value) or math.isinf(value)) else value
    if isinstance(value, Decimal):
        try:
            f = float(value)
            return None if (math.isnan(f) or math.isinf(f)) else f
        except Exception:
            return str(value)
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
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    try:
        if hasattr(value, "model_dump") and callable(getattr(value, "model_dump")):
            return _json_safe(value.model_dump(mode="python"))  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        if hasattr(value, "dict") and callable(getattr(value, "dict")):
            return _json_safe(value.dict())  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        if is_dataclass(value):
            return _json_safe(getattr(value, "__dict__", {}))
    except Exception:
        pass
    try:
        return _json_safe(vars(value))
    except Exception:
        return str(value)


async def _maybe_await(x: Any) -> Any:
    try:
        if inspect.isawaitable(x):
            return await x
    except Exception:
        pass
    return x


def _as_list(v: Any) -> List[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, tuple):
        return list(v)
    if isinstance(v, set):
        return list(v)
    if isinstance(v, str):
        return [v]
    if isinstance(v, Iterable) and not isinstance(v, Mapping):
        try:
            return list(v)
        except Exception:
            return [v]
    return [v]


def _to_plain_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)
    try:
        if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
            d = obj.model_dump(mode="python")  # type: ignore[attr-defined]
            return d if isinstance(d, dict) else {}
    except Exception:
        pass
    try:
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            d = obj.dict()  # type: ignore[attr-defined]
            return d if isinstance(d, dict) else {}
    except Exception:
        pass
    try:
        dd = getattr(obj, "__dict__", None)
        if isinstance(dd, dict):
            return dict(dd)
    except Exception:
        pass
    return {}


def _maybe_int(v: Any, default: int) -> int:
    try:
        if v is None or isinstance(v, bool):
            return default
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            return int(v)
        s = _strip(v)
        return default if not s else int(float(s))
    except Exception:
        return default


def _maybe_bool(v: Any, default: bool) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        try:
            return bool(int(v))
        except Exception:
            return default
    s = _strip(v).lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _split_symbols_string(v: str) -> List[str]:
    raw = (v or "").replace(";", ",").replace("\n", ",").replace("\t", ",").replace(" ", ",")
    out: List[str] = []
    seen = set()
    for p in [x.strip() for x in raw.split(",") if x.strip()]:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def _normalize_symbol_token(sym: Any) -> str:
    s = _strip(sym).upper().replace(" ", "")
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    if s.isdigit() and 3 <= len(s) <= 6:
        return f"{s}.SR"
    return s


def _get_list(body: Mapping[str, Any], *keys: str) -> List[str]:
    for k in keys:
        v = body.get(k)
        is_symbolic = "symbol" in k or "ticker" in k or k in {"code", "requested_symbol"}
        if isinstance(v, list):
            out: List[str] = []
            seen = set()
            for item in v:
                s = _normalize_symbol_token(item) if is_symbolic else _strip(item)
                if s and s not in seen:
                    seen.add(s)
                    out.append(s)
            if out:
                return out
        if isinstance(v, str) and v.strip():
            parts = _split_symbols_string(v)
            if is_symbolic:
                parts = [_normalize_symbol_token(x) for x in parts if _normalize_symbol_token(x)]
            if parts:
                return parts
    return []


def _extract_requested_symbols(body: Mapping[str, Any], limit: int) -> List[str]:
    symbols: List[str] = []
    for key in (
        "symbols", "tickers", "tickers_list", "selected_symbols", "selected_tickers", "direct_symbols",
        "symbol", "ticker", "code", "requested_symbol",
    ):
        symbols.extend(_get_list(body, key))
    out: List[str] = []
    seen = set()
    for sym in symbols:
        s = _normalize_symbol_token(sym)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
        if len(out) >= limit:
            break
    return out


def _pick_page_from_body(body: Mapping[str, Any]) -> str:
    for k in ("sheet", "page", "sheet_name", "sheetName", "page_name", "pageName", "worksheet", "name", "tab"):
        s = _strip(body.get(k))
        if s:
            return s
    return ""


def _collect_get_body(request: Request) -> Dict[str, Any]:
    qp = request.query_params
    body: Dict[str, Any] = {}
    for key in ("sheet", "page", "sheet_name", "sheetName", "page_name", "pageName", "worksheet", "name", "tab"):
        v = _strip(qp.get(key))
        if v:
            body[key] = v
    for key in ("symbols", "tickers", "tickers_list", "selected_symbols", "selected_tickers", "direct_symbols", "symbol", "ticker", "code", "requested_symbol"):
        vals = qp.getlist(key)
        if vals:
            body[key] = _split_symbols_string(vals[0]) if len(vals) == 1 else [s.strip() for s in vals if _strip(s)]
    for key in ("limit", "offset", "top_n", "include_matrix", "schema_only", "headers_only"):
        v = qp.get(key)
        if v is not None:
            body[key] = v
    return body


def _merge_body_with_query(body: Optional[Dict[str, Any]], request: Request) -> Dict[str, Any]:
    out = dict(body or {})
    for k, v in _collect_get_body(request).items():
        if k not in out or out.get(k) in (None, "", []):
            out[k] = v
    return out


# =============================================================================
# Auth — same flow as advanced_analysis / analysis_sheet_rows
# =============================================================================
def _allow_query_token(settings: Any, request: Request) -> bool:
    try:
        if settings is not None:
            return bool(getattr(settings, "ALLOW_QUERY_TOKEN", False) or getattr(settings, "allow_query_token", False))
    except Exception:
        pass
    if (os.getenv("ALLOW_QUERY_TOKEN", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}:
        return True
    try:
        if _strip(request.headers.get("X-Allow-Query-Token")).lower() in {"1", "true", "yes"}:
            return True
    except Exception:
        pass
    return False


def _extract_auth_token(*, token_query: Optional[str], x_app_token: Optional[str], x_api_key: Optional[str], authorization: Optional[str], settings: Any, request: Request) -> str:
    auth_token = _strip(x_app_token) or _strip(x_api_key)
    if authorization and authorization.strip().lower().startswith("bearer "):
        auth_token = authorization.strip().split(" ", 1)[1].strip()
    if token_query and not auth_token and _allow_query_token(settings, request):
        auth_token = _strip(token_query)
    return auth_token


def _auth_passed(*, request: Request, settings: Any, auth_token: str, authorization: Optional[str]) -> bool:
    if auth_ok is None:
        return True
    try:
        if callable(is_open_mode) and bool(is_open_mode()):
            return True
    except Exception:
        pass
    headers_dict = dict(request.headers)
    path = str(getattr(getattr(request, "url", None), "path", "") or "")
    attempts = [
        {"token": auth_token, "authorization": authorization, "headers": headers_dict, "path": path, "request": request, "settings": settings},
        {"token": auth_token, "authorization": authorization, "headers": headers_dict, "path": path, "request": request},
        {"token": auth_token, "authorization": authorization, "headers": headers_dict, "path": path},
        {"token": auth_token, "authorization": authorization, "headers": headers_dict},
        {"token": auth_token, "authorization": authorization},
        {"token": auth_token},
    ]
    for kwargs in attempts:
        try:
            return bool(auth_ok(**kwargs))
        except TypeError:
            continue
        except Exception:
            return False
    return False


# =============================================================================
# Schema / contract resolution
# =============================================================================
def _normalize_page_flexible(page_raw: str) -> str:
    raw = _strip(page_raw)
    if not raw:
        return "Market_Leaders"
    for kwargs in ({"allow_output_pages": True}, {}):
        try:
            value = normalize_page_name(raw, **kwargs)  # type: ignore[misc]
            normalized = _strip(value)
            if normalized:
                return normalized
        except TypeError:
            continue
        except Exception:
            break
    return raw.replace(" ", "_")


def _route_family_flexible(page: str) -> str:
    try:
        family = _strip(get_route_family(page))
        if family:
            return family
    except Exception:
        pass
    if page == _TOP10_PAGE:
        return "top10"
    if page == _INSIGHTS_PAGE:
        return "insights"
    if page == _DICTIONARY_PAGE:
        return "dictionary"
    return "instrument"


def _safe_allowed_pages() -> List[str]:
    try:
        pages = allowed_pages()
        if isinstance(pages, list):
            return pages
        if isinstance(pages, tuple):
            return list(pages)
    except Exception:
        pass
    return list(CANONICAL_PAGES or [])


def _ensure_page_allowed(page: str) -> None:
    forbidden = set(FORBIDDEN_PAGES or set())
    if page in forbidden:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"error": f"Forbidden/removed page: {page}"})
    ap = _safe_allowed_pages()
    if ap and page not in set(ap):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"error": f"Unknown page: {page}", "allowed_pages": ap})


def _normalize_key_name(header: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", _strip(header).lower()).strip("_")


def _complete_schema_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    raw_headers = list(headers or [])
    raw_keys = list(keys or [])
    max_len = max(len(raw_headers), len(raw_keys))
    hdrs: List[str] = []
    ks: List[str] = []
    for i in range(max_len):
        h = _strip(raw_headers[i]) if i < len(raw_headers) else ""
        k = _strip(raw_keys[i]) if i < len(raw_keys) else ""
        if h and not k:
            k = _normalize_key_name(h)
        elif k and not h:
            h = k.replace("_", " ").title()
        elif not h and not k:
            h = f"Column {i + 1}"
            k = f"column_{i + 1}"
        hdrs.append(h)
        ks.append(k)
    return hdrs, ks


def _pad_contract(headers: Sequence[str], keys: Sequence[str], expected_len: int) -> Tuple[List[str], List[str]]:
    hdrs, ks = _complete_schema_contract(headers, keys)
    while len(hdrs) < expected_len:
        i = len(hdrs) + 1
        hdrs.append(f"Column {i}")
        ks.append(f"column_{i}")
    return hdrs[:expected_len], ks[:expected_len]


def _ensure_top10_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    """Top10 = instrument width + 3 extras. With 85-col instrument fallback, total = 88."""
    hdrs, ks = _complete_schema_contract(headers, keys)
    for field in _TOP10_REQUIRED_FIELDS:
        if field not in ks:
            ks.append(field)
            hdrs.append(_TOP10_REQUIRED_HEADERS[field])
    return _pad_contract(hdrs, ks, _EXPECTED_SHEET_LENGTHS[_TOP10_PAGE])


def _static_contract(page: str) -> Tuple[List[str], List[str], str]:
    """Hardcoded fallback. Only used when schema_registry import has failed."""
    if page == _TOP10_PAGE:
        h, k = _ensure_top10_contract(_CANONICAL_85_HEADERS, _CANONICAL_85_KEYS)
        return h, k, "static_canonical_top10_v2.5.0"
    if page == _INSIGHTS_PAGE:
        h, k = _pad_contract(_INSIGHTS_HEADERS, _INSIGHTS_KEYS, 7)
        return h, k, "static_canonical_insights"
    if page == _DICTIONARY_PAGE:
        h, k = _pad_contract(_DICTIONARY_HEADERS, _DICTIONARY_KEYS, 9)
        return h, k, "static_canonical_dictionary"
    h, k = _pad_contract(_CANONICAL_85_HEADERS, _CANONICAL_85_KEYS, _EXPECTED_SHEET_LENGTHS.get(page, 85))
    return h, k, "static_canonical_instrument_v2.5.0"


def _expected_len(page: str) -> int:
    if callable(get_sheet_len):
        try:
            n = int(get_sheet_len(page))  # type: ignore[misc]
            if n > 0:
                return n
        except Exception:
            pass
    return _EXPECTED_SHEET_LENGTHS.get(page, 85)


def _extract_headers_keys_from_spec(spec: Any) -> Tuple[List[str], List[str]]:
    headers: List[str] = []
    keys: List[str] = []
    if isinstance(spec, Mapping):
        headers2 = spec.get("headers") or spec.get("display_headers") or spec.get("sheet_headers")
        keys2 = spec.get("keys") or spec.get("fields") or spec.get("columns")
        if isinstance(headers2, list):
            headers = [_strip(x) for x in headers2 if _strip(x)]
        if isinstance(keys2, list):
            keys = [_strip(x) for x in keys2 if _strip(x)]
        if headers or keys:
            return _complete_schema_contract(headers, keys)
        cols = spec.get("columns") or spec.get("fields")
        if isinstance(cols, list):
            for c in cols:
                if isinstance(c, Mapping):
                    h = _strip(c.get("header") or c.get("display_header") or c.get("label") or c.get("title"))
                    k = _strip(c.get("key") or c.get("field") or c.get("name") or c.get("id"))
                    if h or k:
                        headers.append(h or k.replace("_", " ").title())
                        keys.append(k or _normalize_key_name(h))
    return _complete_schema_contract(headers, keys)


def _schema_from_registry(page: str) -> Tuple[List[str], List[str], Any, str]:
    spec = None
    if callable(get_sheet_headers) and callable(get_sheet_keys):
        try:
            headers = [_strip(x) for x in get_sheet_headers(page) if _strip(x)]  # type: ignore[misc]
            keys = [_strip(x) for x in get_sheet_keys(page) if _strip(x)]  # type: ignore[misc]
            if headers and keys:
                if callable(get_sheet_spec):
                    try:
                        spec = get_sheet_spec(page)  # type: ignore[misc]
                    except Exception:
                        spec = None
                hdrs_done, keys_done = _complete_schema_contract(headers, keys)
                return hdrs_done, keys_done, spec, "schema_registry.helpers"
        except Exception:
            pass
    if get_sheet_spec is None:
        return [], [], None, "registry_unavailable"
    try:
        spec = get_sheet_spec(page)  # type: ignore[misc]
    except Exception as e:
        return [], [], None, f"registry_error:{e}"
    headers, keys = _extract_headers_keys_from_spec(spec)
    return headers, keys, spec, "schema_registry.spec"


def _resolve_contract(page: str) -> Tuple[List[str], List[str], Any, str]:
    expected_len = _expected_len(page)
    headers, keys, spec, source = _schema_from_registry(page)
    if headers and keys:
        headers, keys = _complete_schema_contract(headers, keys)
        if page == _TOP10_PAGE:
            headers, keys = _ensure_top10_contract(headers, keys)
        else:
            headers, keys = _pad_contract(headers, keys, expected_len)
        return headers, keys, spec, source
    sh, sk, ssrc = _static_contract(page)
    return sh, sk, {"source": ssrc, "page": page}, ssrc


# =============================================================================
# Payload extraction / row normalization
# =============================================================================
def _rows_to_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    return [[_json_safe(r.get(k)) for k in keys] for r in rows]


def _slice(rows: List[Dict[str, Any]], *, limit: int, offset: int) -> List[Dict[str, Any]]:
    start = max(0, int(offset))
    if limit <= 0:
        return rows[start:]
    return rows[start:start + max(0, int(limit))]


def _key_variants(key: str) -> List[str]:
    k = _strip(key)
    if not k:
        return []
    variants = [k, k.lower(), k.upper(), k.replace("_", " "), k.replace("_", "").lower()]
    for alias in _FIELD_ALIAS_HINTS.get(k, []):
        variants.extend([alias, alias.lower(), alias.upper(), alias.replace("_", " "), alias.replace("_", "").lower()])
    seen = set()
    out: List[str] = []
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _extract_from_raw(raw: Dict[str, Any], candidates: Sequence[str]) -> Any:
    raw_ci = {str(k).strip().lower(): v for k, v in raw.items()}
    raw_comp = {re.sub(r"[^a-z0-9]+", "", str(k).lower()): v for k, v in raw.items()}
    for candidate in candidates:
        if candidate in raw:
            return raw.get(candidate)
        lc = candidate.lower()
        if lc in raw_ci:
            return raw_ci.get(lc)
        cc = re.sub(r"[^a-z0-9]+", "", candidate.lower())
        if cc in raw_comp:
            return raw_comp.get(cc)
    return None


def _extract_from_nested_raw(raw: Any, candidates: Sequence[str], depth: int = 0) -> Any:
    if raw is None or depth > 3:
        return None
    if isinstance(raw, Mapping):
        direct = _extract_from_raw(dict(raw), candidates)
        if direct is not None:
            return direct
        preferred = (
            "quote", "analysis", "fundamentals", "forecast", "scores", "metrics",
            "summary", "snapshot", "payload", "data", "item", "record", "row",
            "stats", "price", "market_data",
        )
        for nk in preferred:
            nv = raw.get(nk)
            if isinstance(nv, Mapping):
                found = _extract_from_nested_raw(nv, candidates, depth + 1)
                if found is not None:
                    return found
        for nv in raw.values():
            if isinstance(nv, Mapping):
                found = _extract_from_nested_raw(nv, candidates, depth + 1)
                if found is not None:
                    return found
    return None


def _normalize_to_schema_keys(*, schema_keys: Sequence[str], schema_headers: Sequence[str], raw: Mapping[str, Any]) -> Dict[str, Any]:
    raw_dict = dict(raw or {})
    _strip_internal_fields(raw_dict)
    header_by_key = {str(k): str(h) for k, h in zip(schema_keys, schema_headers)}
    out: Dict[str, Any] = {}
    for k in schema_keys:
        ks = str(k)
        v = _extract_from_nested_raw(raw_dict, _key_variants(ks))
        if v is None:
            h = header_by_key.get(ks, "")
            if h:
                v = _extract_from_nested_raw(raw_dict, [h, h.lower(), h.upper()])
        if ks in {"warnings", "recommendation_reason", "selection_reason"} and isinstance(v, (list, tuple, set)):
            v = "; ".join([_strip(x) for x in v if _strip(x)])
        out[ks] = _json_safe(v)
    if "symbol" in out and not out.get("symbol"):
        out["symbol"] = _normalize_symbol_token(_extract_from_nested_raw(raw_dict, _key_variants("symbol")))
    return out


def _to_number(value: Any) -> float:
    if value is None:
        return float("-inf")
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        try:
            f = float(value)
            return f if math.isfinite(f) else float("-inf")
        except Exception:
            return float("-inf")
    s = _strip(value)
    if not s:
        return float("-inf")
    s = s.replace("%", "").replace(",", "")
    try:
        f = float(s)
        return f if math.isfinite(f) else float("-inf")
    except Exception:
        return float("-inf")


def _top10_sort_key(row: Mapping[str, Any]) -> Tuple[float, ...]:
    return (
        _to_number(row.get("overall_score")),
        _to_number(row.get("opportunity_score")),
        _to_number(row.get("expected_roi_3m")),
        _to_number(row.get("expected_roi_1m")),
        _to_number(row.get("forecast_confidence")),
        _to_number(row.get("confidence_score")),
    )


def _top10_selection_reason(row: Mapping[str, Any]) -> str:
    parts: List[str] = []
    labels = (
        ("overall_score", "Overall"),
        ("opportunity_score", "Opportunity"),
        ("expected_roi_3m", "Exp ROI 3M"),
        ("forecast_confidence", "Forecast Conf"),
    )
    for key, label in labels:
        value = row.get(key)
        if value in (None, "", [], {}, ()):
            continue
        parts.append(f"{label} {round(value, 2) if isinstance(value, float) else value}")
        if len(parts) >= 3:
            break
    return " | ".join(parts) if parts else "Top10 selection based on strongest available composite signals."


def _top10_criteria_snapshot(row: Mapping[str, Any]) -> str:
    snapshot: Dict[str, Any] = {}
    for key in (
        "overall_score", "opportunity_score", "expected_roi_1m", "expected_roi_3m",
        "forecast_confidence", "confidence_score", "risk_bucket", "recommendation", "symbol",
    ):
        value = row.get(key)
        if value not in (None, "", [], {}, ()):
            snapshot[key] = _json_safe(value)
    try:
        return json.dumps(snapshot, ensure_ascii=False, sort_keys=True)
    except Exception:
        return str(snapshot)


def _ensure_top10_rows(rows: Sequence[Mapping[str, Any]], *, requested_symbols: Sequence[str], top_n: int, schema_keys: Sequence[str], schema_headers: Sequence[str]) -> List[Dict[str, Any]]:
    normalized = [_normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=(r or {})) for r in rows or []]
    deduped: List[Dict[str, Any]] = []
    seen: set = set()
    for row in sorted(normalized, key=_top10_sort_key, reverse=True):
        sym = _strip(row.get("symbol"))
        name = _strip(row.get("name"))
        key = sym or name or f"row_{len(deduped)+1}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    final = deduped[: max(1, int(top_n))]
    for idx, row in enumerate(final, start=1):
        row["top10_rank"] = idx
        if not _strip(row.get("selection_reason")):
            row["selection_reason"] = _top10_selection_reason(row)
        if not _strip(row.get("criteria_snapshot")):
            row["criteria_snapshot"] = _top10_criteria_snapshot(row)
    return final


def _extract_rows_like(payload: Any, depth: int = 0) -> List[Dict[str, Any]]:
    if payload is None or depth > 6:
        return []
    if isinstance(payload, list):
        if payload and isinstance(payload[0], Mapping):
            return [dict(x) for x in payload]
        return []
    if not isinstance(payload, Mapping):
        return []
    for name in ("row_objects", "records", "items", "data", "quotes", "results"):
        value = payload.get(name)
        if isinstance(value, list) and value and isinstance(value[0], Mapping):
            return [dict(x) for x in value]
    rows_value = payload.get("rows")
    if isinstance(rows_value, list) and rows_value and isinstance(rows_value[0], Mapping):
        return [dict(x) for x in rows_value]
    for name in ("payload", "result", "response", "output", "data"):
        nested = payload.get(name)
        if isinstance(nested, Mapping):
            found = _extract_rows_like(nested, depth + 1)
            if found:
                return found
    return []


def _extract_status_error(payload: Any) -> Tuple[str, Optional[str], Dict[str, Any]]:
    if not isinstance(payload, Mapping):
        return "success", None, {}
    status_out = _strip(payload.get("status")) or "success"
    error_out = payload.get("error") or payload.get("detail") or payload.get("message")
    meta_out = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    return status_out, (str(error_out) if error_out is not None else None), meta_out


# =============================================================================
# Conservative placeholder rows (no fabricated numeric values)
# =============================================================================
def _placeholder_value_for_key(page: str, key: str, symbol: str, row_index: int) -> Any:
    """Conservative placeholder: identity columns get filled, everything
    numeric returns None. The row is clearly marked as a placeholder via the
    `warnings` field. This is the same philosophy as
    `routes.analysis_sheet_rows` v4.1.2.
    """
    kk = _normalize_key_name(key)

    # Identity columns — safe to populate
    if kk in {"symbol", "ticker"}:
        return symbol
    if kk == "name":
        return symbol  # don't fabricate a "Page Symbol" composite name
    if kk == "asset_class":
        if symbol.endswith("=F"):
            return "Commodity"
        if symbol.endswith("=X"):
            return "FX"
        if page == "Mutual_Funds":
            return "Fund"
        return "Equity"
    if kk == "exchange":
        if symbol.endswith(".SR"):
            return "Tadawul"
        if symbol.endswith("=F"):
            return "Futures"
        if symbol.endswith("=X"):
            return "FX"
        return "NASDAQ/NYSE"
    if kk == "currency":
        if symbol.endswith(".SR"):
            return "SAR"
        if symbol.endswith("=X") and len(symbol) >= 8:
            pair = symbol.rstrip("=X")
            if len(pair) >= 6:
                return pair[3:6]
        return "USD"
    if kk == "country":
        if symbol.endswith(".SR"):
            return "Saudi Arabia"
        if symbol.endswith("=F") or symbol.endswith("=X"):
            return "Global"
        return "USA"

    # Provenance
    if kk == "data_provider":
        return "placeholder_no_live_data"
    if kk in {"last_updated_utc", "last_updated_riyadh"}:
        return datetime.utcnow().isoformat()
    if kk == "warnings":
        return "Placeholder fallback — no live data available for this symbol"

    # Top10 metadata (schema requires non-empty)
    if kk == "top10_rank":
        return row_index
    if kk == "selection_reason":
        return "Placeholder — upstream returned no usable rows; no real ranking applied"
    if kk == "criteria_snapshot":
        return json.dumps(
            {"symbol": symbol, "row_index": row_index, "source": "placeholder_no_live_data"},
            ensure_ascii=False,
        )

    # Notes (Data_Dictionary)
    if kk == "notes":
        return "Placeholder fallback row"

    # Everything else (prices, scores, ROIs, fundamentals, risk metrics,
    # valuation ratios, forecasts, position data, view tokens) → None.
    return None


def _build_placeholder_rows(*, page: str, keys: Sequence[str], requested_symbols: Sequence[str], limit: int, offset: int) -> List[Dict[str, Any]]:
    symbols = [_normalize_symbol_token(x) for x in requested_symbols if _normalize_symbol_token(x)]
    if not symbols:
        symbols = [_normalize_symbol_token(x) for x in EMERGENCY_PAGE_SYMBOLS.get(page, []) if _normalize_symbol_token(x)]
    symbols = symbols[offset : offset + limit] if (offset or len(symbols) > limit) else symbols[:limit]
    rows: List[Dict[str, Any]] = []
    for idx, sym in enumerate(symbols, start=offset + 1):
        row = {str(k): _placeholder_value_for_key(page, str(k), sym, idx) for k in keys}
        if "warnings" in row and not row.get("warnings"):
            row["warnings"] = "Placeholder fallback — no live data available for this symbol"
        rows.append(row)
    if page == _TOP10_PAGE:
        for idx, row in enumerate(rows, start=offset + 1):
            row["top10_rank"] = idx
            row.setdefault("selection_reason", "Placeholder — upstream returned no usable rows; no real ranking applied")
            row.setdefault("criteria_snapshot", "{}")
    return rows


def _build_dictionary_fallback_rows(*, page: str, headers: Sequence[str], keys: Sequence[str], limit: int, offset: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx, (header, key) in enumerate(zip(headers, keys), start=1):
        rows.append({
            "sheet": page,
            "group": "Core Contract",
            "header": header,
            "key": key,
            "dtype": "number" if any(token in key for token in ("price", "score", "roi", "qty", "value", "cap", "volume", "margin")) else "text",
            "fmt": "" if "score" not in key and "roi" not in key else "0.00",
            "required": key in {"sheet", "header", "key", "symbol", "name", "current_price"},
            "source": "advanced_sheet_rows.local_dictionary_fallback",
            "notes": f"Auto-generated fallback row {idx} from schema contract",
        })
    return _slice(rows, limit=limit, offset=offset)


def _build_insights_fallback_rows(*, requested_symbols: Sequence[str], limit: int, offset: int) -> List[Dict[str, Any]]:
    symbols = [_normalize_symbol_token(x) for x in requested_symbols if _normalize_symbol_token(x)]
    if not symbols:
        symbols = [_normalize_symbol_token(x) for x in EMERGENCY_PAGE_SYMBOLS.get(_INSIGHTS_PAGE, []) if _normalize_symbol_token(x)]
    stamp = datetime.utcnow().isoformat()
    rows: List[Dict[str, Any]] = [
        {
            "section": "Coverage",
            "item": "Requested symbols",
            "symbol": "",
            "metric": "count",
            "value": len(symbols),
            "notes": "Local insights fallback summary — no live engine data",
            "last_updated_riyadh": stamp,
        },
        {
            "section": "Coverage",
            "item": "Universe sample",
            "symbol": "",
            "metric": "symbols",
            "value": ", ".join(symbols[:5]),
            "notes": "Sample of the symbols used by fallback mode",
            "last_updated_riyadh": stamp,
        },
        {
            "section": "Status",
            "item": "Engine availability",
            "symbol": "",
            "metric": "warning",
            "value": "Engine returned no usable rows",
            "notes": "Live engine and upstream proxies all returned empty/error payloads",
            "last_updated_riyadh": stamp,
        },
    ]
    for idx, sym in enumerate(symbols[: max(1, limit + offset)], start=1):
        rows.append({
            "section": "Pending Analysis",
            "item": f"Symbol {idx}",
            "symbol": sym,
            "metric": "status",
            "value": "no_live_data",
            "notes": "Symbol is in the requested universe but the engine returned no live row for it",
            "last_updated_riyadh": stamp,
        })
    return _slice(rows, limit=limit, offset=offset)


def _build_nonempty_failsoft_rows(*, page: str, headers: Sequence[str], keys: Sequence[str], requested_symbols: Sequence[str], limit: int, offset: int, top_n: int) -> List[Dict[str, Any]]:
    if page == _DICTIONARY_PAGE:
        return _build_dictionary_fallback_rows(page=page, headers=headers, keys=keys, limit=limit, offset=offset)
    if page == _INSIGHTS_PAGE:
        return _build_insights_fallback_rows(requested_symbols=requested_symbols, limit=limit, offset=offset)
    if page == _TOP10_PAGE:
        rows = _build_placeholder_rows(
            page=page,
            keys=keys,
            requested_symbols=requested_symbols or EMERGENCY_PAGE_SYMBOLS.get(page, []),
            limit=max(limit, top_n),
            offset=0,
        )
        rows = _ensure_top10_rows(rows, requested_symbols=requested_symbols, top_n=top_n, schema_keys=keys, schema_headers=headers)
        return _slice(rows, limit=limit, offset=offset)
    return _build_placeholder_rows(
        page=page,
        keys=keys,
        requested_symbols=requested_symbols or EMERGENCY_PAGE_SYMBOLS.get(page, []),
        limit=limit,
        offset=offset,
    )


# =============================================================================
# Envelope
# =============================================================================
def _payload_envelope(*, page: str, headers: Sequence[str], keys: Sequence[str], row_objects: Sequence[Mapping[str, Any]], include_matrix: bool, request_id: str, started_at: float, mode: str, status_out: str, error_out: Optional[str], meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    hdrs = list(headers or [])
    ks = list(keys or [])
    rows_dict = [_strip_internal_fields({str(k): _json_safe(dict(r).get(k)) for k in ks}) for r in (row_objects or [])]
    if page == _TOP10_PAGE:
        for idx, row in enumerate(rows_dict, start=1):
            row.setdefault("top10_rank", idx)
            row.setdefault("selection_reason", "Selected by advanced_sheet_rows.")
            row.setdefault("criteria_snapshot", "{}")
    matrix = _rows_to_matrix(rows_dict, ks) if include_matrix else []
    return _json_safe({
        "status": status_out,
        "page": page,
        "sheet": page,
        "sheet_name": page,
        "route_family": "v1_advanced",
        "headers": hdrs,
        "display_headers": hdrs,
        "sheet_headers": hdrs,
        "column_headers": hdrs,
        "keys": ks,
        "columns": ks,
        "fields": ks,
        "rows": matrix,
        "rows_matrix": matrix,
        "matrix": matrix,
        "row_objects": rows_dict,
        "items": rows_dict,
        "records": rows_dict,
        "data": rows_dict,
        "quotes": rows_dict,
        "count": len(rows_dict),
        "detail": error_out or "",
        "error": error_out,
        "version": ADVANCED_SHEET_ROWS_VERSION,
        "request_id": request_id,
        "meta": {
            "duration_ms": round((time.time() - started_at) * 1000.0, 3),
            "mode": mode,
            "count": len(rows_dict),
            "dispatch": "advanced_sheet_rows_v1",
            "source": CORE_GET_SHEET_ROWS_SOURCE,
            **(meta or {}),
        },
    })


async def _call_core_sheet_rows_best_effort(*, page: str, limit: int, offset: int, mode: str, body: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if core_get_sheet_rows is None:
        return None, None
    candidates = [
        ((), {"sheet": page, "limit": limit, "offset": offset, "mode": mode, "body": body}),
        ((), {"sheet": page, "limit": limit, "offset": offset, "mode": mode}),
        ((), {"sheet": page, "limit": limit, "offset": offset}),
        ((page,), {"limit": limit, "offset": offset, "mode": mode, "body": body}),
        ((page,), {"limit": limit, "offset": offset, "mode": mode}),
        ((page,), {"limit": limit, "offset": offset}),
        ((page,), {}),
    ]
    last_err: Optional[Exception] = None
    for args, kwargs in candidates:
        try:
            res = core_get_sheet_rows(*args, **kwargs)
            res = await _maybe_await(res)
            if isinstance(res, dict):
                return res, CORE_GET_SHEET_ROWS_SOURCE
            if isinstance(res, list):
                return {"row_objects": res}, CORE_GET_SHEET_ROWS_SOURCE
        except TypeError as e:
            last_err = e
            continue
        except Exception as e:
            last_err = e
            break
    if last_err is not None:
        return {"status": "error", "error": str(last_err), "row_objects": []}, CORE_GET_SHEET_ROWS_SOURCE
    return None, None


def _normalize_external_payload(*, external_payload: Mapping[str, Any], page: str, headers: Sequence[str], keys: Sequence[str], include_matrix: bool, request_id: str, started_at: float, mode: str, limit: int = 2000, offset: int = 0, top_n: int = 2000, requested_symbols: Optional[Sequence[str]] = None, meta_extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    ext = dict(external_payload or {})
    hdrs = list(headers or [])
    ks = list(keys or [])
    rows = _extract_rows_like(ext)
    normalized_rows = [
        _normalize_to_schema_keys(schema_keys=ks, schema_headers=hdrs, raw=(r or {}))
        for r in rows
    ]
    if page == _TOP10_PAGE:
        normalized_rows = _ensure_top10_rows(
            normalized_rows,
            requested_symbols=requested_symbols or [],
            top_n=top_n,
            schema_keys=ks,
            schema_headers=hdrs,
        )
    normalized_rows = _slice(normalized_rows, limit=limit, offset=offset)
    status_out, error_out, ext_meta = _extract_status_error(ext)
    # Treat engine v5.47.4 "warn" status as success when rows are present
    status_lc = (status_out or "").lower()
    if status_lc == "warn":
        effective = "success" if normalized_rows else "warn"
    elif not status_lc:
        effective = "success" if normalized_rows else "partial"
    else:
        effective = status_out
    if not normalized_rows and effective == "success":
        effective = "partial"
        error_out = error_out or "No usable rows returned"
    final_meta = dict(ext_meta or {})
    if meta_extra:
        final_meta.update(meta_extra)
    return _payload_envelope(
        page=page,
        headers=hdrs,
        keys=ks,
        row_objects=normalized_rows,
        include_matrix=include_matrix,
        request_id=request_id,
        started_at=started_at,
        mode=mode,
        status_out=effective,
        error_out=error_out,
        meta=final_meta,
    )


# =============================================================================
# Internal implementation
# =============================================================================
async def _run_advanced_sheet_rows_impl(
    request: Request,
    body: Dict[str, Any],
    mode: str = "",
    include_matrix_q: Optional[bool] = None,
    token: Optional[str] = None,
    x_app_token: Optional[str] = None,
    x_api_key: Optional[str] = None,
    authorization: Optional[str] = None,
    x_request_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Main impl, exposed by name so `routes.analysis_sheet_rows` can proxy
    to it. The signature must match `routes.advanced_analysis
    ._run_advanced_sheet_rows_impl` exactly — analysis_sheet_rows v4.1.2
    calls both with identical kwargs and breaks if either signature drifts.
    """
    start = time.time()
    request_id = _strip(x_request_id) or str(uuid.uuid4())[:12]
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    auth_token = _extract_auth_token(
        token_query=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        settings=settings,
        request=request,
    )
    if not _auth_passed(request=request, settings=settings, auth_token=auth_token, authorization=authorization):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    merged_body = _merge_body_with_query(body, request)
    page = _normalize_page_flexible(_pick_page_from_body(merged_body) or "Market_Leaders")
    _ensure_page_allowed(page)

    include_matrix = _maybe_bool(merged_body.get("include_matrix"), include_matrix_q if include_matrix_q is not None else True)
    limit = max(1, min(5000, _maybe_int(merged_body.get("limit"), 2000)))
    offset = max(0, _maybe_int(merged_body.get("offset"), 0))
    top_n = max(1, min(5000, _maybe_int(merged_body.get("top_n"), limit)))
    schema_only = _maybe_bool(merged_body.get("schema_only"), False)
    headers_only = _maybe_bool(merged_body.get("headers_only"), False)
    requested_symbols = _extract_requested_symbols(merged_body, max(top_n, limit + offset, 50))

    headers, keys, spec, schema_source = _resolve_contract(page)

    if schema_only or headers_only:
        return _payload_envelope(
            page=page,
            headers=headers,
            keys=keys,
            row_objects=[],
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=start,
            mode=mode,
            status_out="success",
            error_out=None,
            meta={
                "dispatch": "schema_only",
                "schema_source": schema_source,
                "headers_only": headers_only,
                "schema_only": schema_only,
            },
        )

    payload, source = await _call_core_sheet_rows_best_effort(
        page=page,
        limit=max(limit + offset, top_n),
        offset=0,
        mode=mode or "",
        body=merged_body,
    )
    if isinstance(payload, dict):
        normalized = _normalize_external_payload(
            external_payload=payload,
            page=page,
            headers=headers,
            keys=keys,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=start,
            mode=mode,
            limit=limit,
            offset=offset,
            top_n=top_n,
            requested_symbols=requested_symbols,
            meta_extra={
                "schema_source": schema_source,
                "source": source or CORE_GET_SHEET_ROWS_SOURCE,
            },
        )
        if _extract_rows_like(normalized):
            return normalized

    fallback_rows = _build_nonempty_failsoft_rows(
        page=page,
        headers=headers,
        keys=keys,
        requested_symbols=requested_symbols,
        limit=limit,
        offset=offset,
        top_n=top_n,
    )
    fallback_status = "partial" if fallback_rows else "error"
    fallback_error = (
        "Local non-empty fallback emitted after upstream degradation"
        if fallback_rows
        else "No usable rows returned; schema-shaped fallback emitted"
    )
    return _payload_envelope(
        page=page,
        headers=headers,
        keys=keys,
        row_objects=fallback_rows,
        include_matrix=include_matrix,
        request_id=request_id,
        started_at=start,
        mode=mode,
        status_out=fallback_status,
        error_out=fallback_error,
        meta={
            "dispatch": "advanced_sheet_rows_fail_soft_nonempty" if fallback_rows else "advanced_sheet_rows_fail_soft",
            "schema_source": schema_source,
            "source": source or CORE_GET_SHEET_ROWS_SOURCE,
        },
    )


# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
async def advanced_sheet_rows_health(request: Request) -> Dict[str, Any]:
    return _json_safe({
        "status": "ok",
        "service": "advanced_sheet_rows",
        "version": ADVANCED_SHEET_ROWS_VERSION,
        "schema_registry_available": bool(get_sheet_spec is not None),
        "adapter_available": bool(core_get_sheet_rows is not None),
        "adapter_source": CORE_GET_SHEET_ROWS_SOURCE,
        "allowed_pages_count": len(_safe_allowed_pages()),
        "schema_alignment": "v2.5.0 (85/88) — registry-first",
        "path": str(getattr(getattr(request, "url", None), "path", "")),
    })


@router.get("/schema/sheet-spec")
async def advanced_schema_sheet_spec_get(
    request: Request,
    page: str = Query(default=""),
    sheet: str = Query(default=""),
    sheet_name: str = Query(default=""),
    name: str = Query(default=""),
    tab: str = Query(default=""),
) -> Dict[str, Any]:
    page_name = _normalize_page_flexible(page or sheet or sheet_name or name or tab or "Market_Leaders")
    _ensure_page_allowed(page_name)
    headers, keys, spec, schema_source = _resolve_contract(page_name)
    columns = [{"header": h, "key": k} for h, k in zip(headers, keys)]
    return _json_safe({
        "status": "success",
        "page": page_name,
        "sheet": page_name,
        "sheet_name": page_name,
        "headers": headers,
        "display_headers": headers,
        "sheet_headers": headers,
        "column_headers": headers,
        "keys": keys,
        "fields": keys,
        "columns": columns,
        "meta": {
            "schema_source": schema_source,
            "version": ADVANCED_SHEET_ROWS_VERSION,
        },
    })


@router.post("/schema/sheet-spec")
async def advanced_schema_sheet_spec_post(body: Dict[str, Any] = Body(default_factory=dict)) -> Dict[str, Any]:
    page_name = _normalize_page_flexible(_pick_page_from_body(body) or "Market_Leaders")
    _ensure_page_allowed(page_name)
    headers, keys, spec, schema_source = _resolve_contract(page_name)
    columns = [{"header": h, "key": k} for h, k in zip(headers, keys)]
    return _json_safe({
        "status": "success",
        "page": page_name,
        "sheet": page_name,
        "sheet_name": page_name,
        "headers": headers,
        "display_headers": headers,
        "sheet_headers": headers,
        "column_headers": headers,
        "keys": keys,
        "fields": keys,
        "columns": columns,
        "meta": {
            "schema_source": schema_source,
            "version": ADVANCED_SHEET_ROWS_VERSION,
        },
    })


@router.get("/sheet-rows")
async def advanced_sheet_rows_get(
    request: Request,
    page: str = Query(default=""),
    sheet: str = Query(default=""),
    sheet_name: str = Query(default=""),
    name: str = Query(default=""),
    tab: str = Query(default=""),
    symbols: str = Query(default=""),
    tickers: str = Query(default=""),
    direct_symbols: str = Query(default=""),
    symbol: str = Query(default=""),
    ticker: str = Query(default=""),
    code: str = Query(default=""),
    requested_symbol: str = Query(default=""),
    limit: Optional[int] = Query(default=None),
    offset: Optional[int] = Query(default=None),
    top_n: Optional[int] = Query(default=None),
    mode: str = Query(default=""),
    include_matrix_q: Optional[bool] = Query(default=None, alias="include_matrix"),
    schema_only: Optional[bool] = Query(default=None),
    headers_only: Optional[bool] = Query(default=None),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    body: Dict[str, Any] = {}
    for k, v in {
        "page": page,
        "sheet": sheet,
        "sheet_name": sheet_name,
        "name": name,
        "tab": tab,
        "symbols": symbols,
        "tickers": tickers,
        "direct_symbols": direct_symbols,
        "symbol": symbol,
        "ticker": ticker,
        "code": code,
        "requested_symbol": requested_symbol,
        "limit": limit,
        "offset": offset,
        "top_n": top_n,
        "schema_only": schema_only,
        "headers_only": headers_only,
    }.items():
        if v not in (None, ""):
            body[k] = v
    return await _run_advanced_sheet_rows_impl(
        request=request,
        body=body,
        mode=mode,
        include_matrix_q=include_matrix_q,
        token=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        x_request_id=x_request_id,
    )


@router.post("/sheet-rows")
async def advanced_sheet_rows_post(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: str = Query(default=""),
    include_matrix_q: Optional[bool] = Query(default=None, alias="include_matrix"),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    return await _run_advanced_sheet_rows_impl(
        request=request,
        body=body,
        mode=mode,
        include_matrix_q=include_matrix_q,
        token=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        x_request_id=x_request_id,
    )


__all__ = [
    "router",
    "ADVANCED_SHEET_ROWS_VERSION",
    "_run_advanced_sheet_rows_impl",
]
