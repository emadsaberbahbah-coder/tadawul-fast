#!/usr/bin/env python3
# routes/advanced_sheet_rows.py
"""
================================================================================
ADVANCED Sheet-Rows Router — RETIRED ORPHAN (NOT MOUNTED) — v4.4.0
================================================================================
THIS FILE IS NOT THE LIVE ANALYSIS ROUTER. DO NOT CONFUSE THE TWO.

  • This module  = routes/advanced_sheet_rows.py   (RETIRED / orphan).
  • The LIVE sheet-rows routers are:
        routes/analysis_sheet_rows.py    (owns /v1/analysis/sheet-rows; v4.5.1+)
        routes/advanced_analysis.py      (owns /sheet-rows, /v1/schema/...)

main.py v8.11.2 REMOVED `routes.advanced_sheet_rows` from the controlled route
plan, so NOTHING in this file is mounted at runtime. It is retained ONLY so that
legacy string-based resolver fallbacks referencing
"routes.advanced_sheet_rows._run_advanced_sheet_rows_impl" still import cleanly;
those fallbacks sit AFTER `routes.advanced_analysis` in every resolver chain, so
this code is effectively never reached. The body below is kept BYTE-FOR-BYTE at
v4.4.0 — only the header identity (filename comment, this banner, and the version
constant name) was corrected — so there is zero behavioural drift.

>>> IF YOU ARE EDITING SHEET-ROWS CONTRACT LOGIC, you want
>>> routes/analysis_sheet_rows.py (currently v4.5.1), NOT this file. <<<

(Historical note: this module began life as a copy of the analysis router, which
is why its internal log lines and the legacy header still read "Analysis". That
copy lineage is exactly what caused a file-naming mix-up; this identity block
exists to make sure it never happens again.)
================================================================================
ENGINE-FIRST • ADAPTER-SECOND • ROOT-PROXY COMPAT • PLACEHOLDER FILTER
SCHEMA-FIRST • STABLE ENVELOPE • GET+POST MERGED • FAIL-SOFT • JSON-SAFE
DIAGNOSTIC-VISIBLE • ENGINE-V2-PREFERRED • PROXY-TIMEOUT-SAFE • NO-HARDCODED-CLAMP

WHY v4.4.0 — registry-derived contract widths (stop truncating Top_10)
----------------------------------------------------------------------

Root problem this release fixes: the router carried HARD-CODED column
widths (instrument 90, Top_10 93). The live engine grew to v5.78.0 = 115
canonical keys (Fix K added `forecast_source` + the 8 investability-gate
columns: data_quality_score, forecast_reliability_score,
provider_engine_conflict, conflict_type, final_decision_basis,
investability_status, final_action, block_reason). With the width frozen at
90/93, columns 91-115 — the ENTIRE investability gate — were silently
sliced off before reaching the sheet, even when the registry served them.

Top_10 was the worst offender: it had THREE independent truncation points,
all clamping to the literal `93`:
  (1) `_ensure_top10_contract` ended with `_pad_contract(hdrs, ks, 93)`.
  (2) `_resolve_contract` then required `len == expected_len`; a 118-col
      registry result clamped to 93 failed that check and fell THROUGH to
      the static 93-col fallback — discarding the registry entirely.
  (3) `_normalize_external_payload` RE-clamped already-correct headers back
      to 93 via `_ensure_top10_contract(hdrs, ks)` with no width.

v4.4.0 removes every hard-coded width literal from the contract path and
derives widths in two layers:
  - RUNTIME (authoritative): `_expected_len(page)` prefers the live
    registry's `get_sheet_len(page)`. The router now faithfully passes
    through whatever the deployed registry serves — 90 today, 115/118 once
    the schema_registry pass (cascade #3) lands — with ZERO further edits.
  - STATIC FALLBACK (offline only): every width derives from
    `len(_CANONICAL_80_KEYS)`; Top_10 derives as `instrument_len +
    _TOP10_EXTRA_COUNT`. The static snapshot is used ONLY when the registry
    import fails entirely. When that snapshot is later synced to the live
    115-key list (during cascade #3), all derived widths auto-update.

NOTE ON VISIBILITY: this router can only surface columns the registry (or
the static fallback) actually NAMES. It does not — and must not — fabricate
the 25 gate-column headers/keys. So the gate columns become visible on the
sheet once `core.sheets.schema_registry` serves them (cascade #3). Until
then this router faithfully tracks the registry's current width with no
truncation and no fabricated placeholders. `_ensure_top10_contract` now also
guarantees it will NEVER truncate below the canonical+extras it already
holds, regardless of the target width passed in.

v4.4.0 changes (from v4.3.1)
----------------------------
[FIX-A] `_EXPECTED_SHEET_LENGTHS` moved below the canonical lists and
    derived from `len(_CANONICAL_80_KEYS)` / `len(_INSIGHTS_KEYS)` /
    `len(_DICTIONARY_KEYS)`; Top_10 = `_CANONICAL_INSTRUMENT_LEN +
    _TOP10_EXTRA_COUNT`. Static-fallback-only; runtime prefers the registry.
[FIX-B] `_ensure_top10_contract` takes optional `target_len`; the literal
    `93` is gone. Hard invariant: never truncates below canonical+extras.
[FIX-C] `_resolve_contract` passes the registry-derived `expected_len` into
    `_ensure_top10_contract`, so a 118-col registry result is preserved
    rather than clamped-then-discarded.
[FIX-D] `_normalize_external_payload` passes `_expected_len(page)` into
    `_ensure_top10_contract` (was an unwidth'd re-clamp to 93).
[FIX-E] `_static_contract` and `_expected_len` use the derived widths
    instead of the literals 90 / 93 / 7 / 9.

NO BEHAVIOR CHANGE on the instrument pages when the registry width is
unchanged: `_pad_contract` already returned exactly `expected_len`, so those
paths are byte-equivalent at 90. The change is purely "track the registry /
stop clamping Top_10." All v4.3.1 diagnostic visibility is preserved verbatim.

v4.3.1 changes (preserved verbatim)
-----------------------------------
[FIX-1] Module-level CORE_ENGINE_SOURCE + explicit `_get_engine` cascade.
[FIX-2] `_call_core_sheet_rows_best_effort` returns
    `(payload, source, call_summary, outcome_label)`.
[FIX-3] `_proxy_callable` adds asyncio.wait_for timeout + full error capture
    (TFB_ANALYSIS_PROXY_TIMEOUT_SEC, default 25s).
[FIX-4] `_fetch_analysis_rows` returns `(results, diagnostic)` with
    `engine_method_used` / `engine_method_summary`.
[FIX-5] Upstream diagnostic meta (`upstream_call_*`, `bridge_call_*`)
    preserved end-to-end.
[FIX-6] `[analysis_sheet_rows v4.x]` diagnostic logging; ANALYSIS_SHEET_ROWS_DEBUG=1.

WHY v4.3.0 (preserved verbatim)
-------------------------------
- BUMP: static fallback contract widened 85 -> 90 (schema_registry v2.6.0, Wave 1).
- BUMP: `_EXPECTED_SHEET_LENGTHS` instrument 85 -> 90, Top10 88 -> 93.
- BUMP: `_static_contract` instrument 85 -> 90; `_ensure_top10_contract` 88 -> 93.
- KEEP: every v4.2.0 fix preserved.

WHY v4.2.0 (preserved verbatim)
-------------------------------
- FIX [HIGH]: static fallback contract widened 80 -> 85 (schema_registry v2.5.0+).

WHY v4.1.2 (preserved verbatim)
-------------------------------
- FIX [HIGH]: v5.47.4 engine alias mappings; conservative placeholder
    values (no fake numerics); internal-field stripping; "warn" status
    handling; page+body context to engine; density-aware quality scoring.

Co-deployment matrix (Wave 2A + Wave 3 + Fix-K gate)
----------------------------------------------------
  Module                                Version    Notes
  -------                               -------    -----
  core/data_engine_v2.py                5.78.0     115 canonical keys (Fix K gate)
  core/sheets/schema_registry.py        2.6.0*     *90/93 live; pending bump to 115/118 (cascade #3)
  core/scoring.py                       5.1.0      View + Insights producer
  core/reco_normalize.py                7.1.0      conviction-floor gating
  core/insights_builder.py              1.0.0      pure-function module
  core/investment_advisor.py            5.2.0      v2.6.0 fallback schemas
  routes/advanced_analysis.py           4.x*       *pending registry-derived widths (cascade #2)
  routes/enriched_quote.py              8.4.0      v2-binding cascade
  routes/investment_advisor.py          2.15.0     bridge-error capture
  routes/analysis_sheet_rows.py         4.4.0      this file — registry-derived, tracks canonical
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
import uuid
from dataclasses import is_dataclass
from datetime import date, datetime, time as dt_time
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

logger = logging.getLogger("routes.analysis_sheet_rows")
logger.addHandler(logging.NullHandler())

# -----------------------------------------------------------------------------
# Optional imports
# -----------------------------------------------------------------------------
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
        if name == "Top_10_Investments":
            return "top10"
        if name == "Insights_Analysis":
            return "insights"
        if name == "Data_Dictionary":
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

CORE_GET_SHEET_ROWS_SOURCE = "unavailable"
# v4.1.1: prefer core.data_engine_v2 (the primary engine) over the legacy
# core.data_engine. Previously this tried data_engine first, which always
# succeeded because the legacy module still exists in the repo.
try:
    from core.data_engine_v2 import get_sheet_rows as core_get_sheet_rows  # type: ignore
    CORE_GET_SHEET_ROWS_SOURCE = "core.data_engine_v2.get_sheet_rows"
except Exception:
    try:
        from core.data_engine import get_sheet_rows as core_get_sheet_rows  # type: ignore
        CORE_GET_SHEET_ROWS_SOURCE = "core.data_engine.get_sheet_rows"
    except Exception:
        core_get_sheet_rows = None  # type: ignore


# Canonical, clearly-named identity for THIS module (routes/advanced_sheet_rows).
ADVANCED_SHEET_ROWS_VERSION = "4.4.0"
# Backward-compat alias: this file began as a copy of the analysis router, so its
# internal log lines and the response 'version' field reference
# ANALYSIS_SHEET_ROWS_VERSION (14 references below). Keep it as an alias so none
# of them break; the module's TRUE identity is ADVANCED_SHEET_ROWS_VERSION above.
ANALYSIS_SHEET_ROWS_VERSION = ADVANCED_SHEET_ROWS_VERSION

# v4.3.1 [FIX-1]: tracks which engine binding actually loaded. Updated at
# request time inside _get_engine(). Surfaced in meta.engine_source on
# every response and in /health. Mirrors v4.3.1's CORE_GET_SHEET_ROWS_SOURCE
# pattern but for the engine factory rather than the get_sheet_rows adapter.
CORE_ENGINE_SOURCE: str = "unresolved"


def _analysis_sheet_rows_debug_enabled() -> bool:
    """v4.3.1 [FIX-6]: cheap env-flag check for diagnostic logging."""
    raw = (os.getenv("ANALYSIS_SHEET_ROWS_DEBUG", "") or "").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _analysis_proxy_timeout_sec() -> float:
    """v4.3.1 [FIX-3]: configurable proxy timeout (default 25s)."""
    try:
        raw = (os.getenv("TFB_ANALYSIS_PROXY_TIMEOUT_SEC", "") or "").strip()
        if raw:
            v = float(raw)
            return max(1.0, min(120.0, v))
    except Exception:
        pass
    return 25.0


router = APIRouter(prefix="/v1/analysis", tags=["Analysis Sheet Rows"])

_TOP10_PAGE = "Top_10_Investments"
_INSIGHTS_PAGE = "Insights_Analysis"
_DICTIONARY_PAGE = "Data_Dictionary"
_SPECIAL_PAGES = {_TOP10_PAGE, _INSIGHTS_PAGE, _DICTIONARY_PAGE}

# v4.4.0 [FIX-A]: _EXPECTED_SHEET_LENGTHS is now DERIVED from the canonical
# list lengths and defined AFTER the canonical lists below (see the
# "v4.4.0 derived contract widths" block). It is a STATIC-FALLBACK ONLY map:
# _expected_len() always prefers the live registry's get_sheet_len() first,
# so these values are used only when the registry is unavailable. Removing
# the early hard-coded {90, 93, 7, 9} dict is what lets Top_10 track the
# canonical instead of clamping to a frozen 93.

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

# v4.1.2: extended with v5.47.4 engine output mirrored aliases.
_FIELD_ALIAS_HINTS: Dict[str, List[str]] = {
    "symbol": ["ticker", "code", "instrument", "security", "requested_symbol", "symbol_code", "symbol_normalized"],
    "ticker": ["symbol", "code", "instrument", "security", "requested_symbol", "symbol_code"],
    "name": ["short_name", "long_name", "display_name", "instrument_name", "security_name", "shortName", "longName"],
    "asset_class": ["asset_type", "quote_type", "instrument_type", "security_type", "type", "quoteType"],
    "exchange": ["exchange_name", "full_exchange_name", "market", "market_name", "mic", "exchangeName"],
    "currency": ["currency_code", "ccy", "fx_currency", "financialCurrency", "quoteCurrency"],
    "country": ["country_name", "region_country", "domicile_country", "countryName"],
    "sector": ["sector_name", "gics_sector", "sectorDisp"],
    "industry": ["industry_name", "gics_industry", "industryDisp"],
    "current_price": ["price", "last_price", "last", "close", "market_price", "nav", "spot", "regularMarketPrice", "lastPrice", "latestPrice"],
    "previous_close": ["prev_close", "prior_close", "regularMarketPreviousClose", "previousClose"],
    "open_price": ["open", "day_open", "regularMarketOpen", "dayOpen"],
    "day_high": ["high", "session_high", "regularMarketDayHigh", "dayHigh"],
    "day_low": ["low", "session_low", "regularMarketDayLow", "dayLow"],
    "week_52_high": ["fiftyTwoWeekHigh", "fifty_two_week_high", "year_high", "52_week_high", "52WeekHigh"],
    "week_52_low": ["fiftyTwoWeekLow", "fifty_two_week_low", "year_low", "52_week_low", "52WeekLow"],
    "week_52_position_pct": ["position_52w_pct", "fifty_two_week_position_pct", "52w_position_pct"],
    "price_change": ["change", "net_change", "regularMarketChange"],
    "percent_change": ["pct_change", "change_pct", "changePercent", "percentChange", "regularMarketChangePercent"],
    "volume": ["trade_volume", "traded_volume", "volume_traded", "regularMarketVolume"],
    "avg_volume_10d": ["averageDailyVolume10Day", "avg10_volume", "ten_day_avg_volume", "average_volume_10d", "avg_vol_10d"],
    "avg_volume_30d": ["averageDailyVolume3Month", "averageVolume3Month", "avg30_volume", "thirty_day_avg_volume", "averageVolume", "avg_vol_30d"],
    "market_cap": ["marketCap", "market_capitalization", "MarketCapitalization"],
    "float_shares": ["floatShares", "sharesFloat", "free_float_shares", "FloatShares"],
    "beta_5y": ["beta", "Beta"],
    "pe_ttm": ["trailingPE", "peRatio", "PERatio", "pe"],
    "pe_forward": ["forwardPE", "forward_pe", "ForwardPE", "ForwardPERatio"],
    "eps_ttm": ["trailingEps", "eps", "EarningsShare"],
    "dividend_yield": ["dividendYield", "trailingAnnualDividendYield", "DividendYield"],
    "payout_ratio": ["payoutRatio", "PayoutRatio"],
    "revenue_ttm": ["totalRevenue", "RevenueTTM", "Revenue"],
    "revenue_growth_yoy": ["revenueGrowth", "RevenueGrowthYOY", "QuarterlyRevenueGrowthYOY"],
    "gross_margin": ["grossMargins", "GrossMargin"],
    "operating_margin": ["operatingMargins", "OperatingMargin"],
    "profit_margin": ["profitMargins", "netMargin", "ProfitMargin"],
    "debt_to_equity": ["debtToEquity", "DebtToEquity", "d_e_ratio"],
    "free_cash_flow_ttm": ["freeCashflow", "fcf_ttm", "FreeCashFlow"],
    "rsi_14": ["rsi", "rsi14"],
    "volatility_30d": ["vol30d", "volatility30d"],
    "volatility_90d": ["vol90d", "volatility90d"],
    "max_drawdown_1y": ["maxDrawdown1y", "drawdown1y"],
    "var_95_1d": ["var95_1d", "valueAtRisk95_1d"],
    "sharpe_1y": ["sharpe1y", "sharpeRatio"],
    "pb_ratio": ["pb", "priceToBook"],
    "ps_ratio": ["ps", "priceToSalesTrailing12Months"],
    "ev_ebitda": ["ev_to_ebitda", "evToEbitda", "enterpriseToEbitda"],
    "peg_ratio": ["peg", "pegRatio"],
    "intrinsic_value": ["fairValue", "fair_value", "dcf", "intrinsicValue"],
    "overall_score": ["score", "composite_score", "total_score", "compositeScore"],
    "opportunity_score": ["opportunity", "opportunity_rank_score", "conviction_score"],
    "forecast_confidence": ["confidence", "confidence_pct", "ai_confidence", "modelConfidence"],
    "confidence_score": ["confidence", "confidence_pct", "modelConfidenceScore"],
    "expected_roi_1m": ["roi_1m", "expected_return_1m", "target_return_1m", "expected_roi_1m_pct"],
    "expected_roi_3m": ["roi_3m", "expected_return_3m", "target_return_3m", "expected_roi_3m_pct"],
    "expected_roi_12m": ["roi_12m", "expected_return_12m", "target_return_12m", "expected_roi_12m_pct"],
    "forecast_price_1m": ["target_price_1m", "projected_price_1m"],
    "forecast_price_3m": ["target_price_3m", "projected_price_3m", "targetMeanPrice"],
    "forecast_price_12m": ["target_price_12m", "projected_price_12m", "targetMedianPrice"],
    "recommendation": ["signal", "rating", "action", "reco"],
    "recommendation_reason": ["rationale", "reasoning", "signal_reason", "reason", "thesis"],
    "data_provider": ["provider", "source_provider", "primary_provider", "provider_primary", "dataProvider"],
    "last_updated_utc": ["updated_at", "timestamp_utc", "as_of_utc", "last_updated", "updated_at_utc", "asOf"],
    "last_updated_riyadh": ["timestamp_riyadh", "as_of_riyadh", "last_update_riyadh", "updated_at_riyadh"],
    "warnings": ["warning", "messages", "errors", "issues"],
    "top10_rank": ["rank", "top_rank", "position_rank"],
    "selection_reason": ["reason", "selection_notes", "selector_reason"],
    "criteria_snapshot": ["criteria", "criteria_json", "snapshot"],
    "group": ["section"],
    "fmt": ["format"],
    "dtype": ["type", "data_type"],
    "notes": ["description", "commentary"],
    "volume_ratio": ["volumeRatio"],
    "day_range_position": ["dayRangePosition", "intraday_position"],
    "upside_pct": ["upsidePct", "upside_percent"],
    "data_quality_score": ["dataQualityScore", "quality_score_numeric"],
    "horizon_days": ["horizon", "days_horizon"],
    "invest_period_label": ["periodLabel", "horizonLabel"],
}

_CANONICAL_80_HEADERS: List[str] = [
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
    "Current Price", "Previous Close", "Open", "Day High", "Day Low", "52W High", "52W Low",
    "Price Change", "Percent Change", "52W Position %", "Volume", "Avg Volume 10D", "Avg Volume 30D",
    "Market Cap", "Float Shares", "Beta (5Y)", "P/E (TTM)", "P/E (Forward)", "EPS (TTM)",
    "Dividend Yield", "Payout Ratio", "Revenue (TTM)", "Revenue Growth YoY", "Gross Margin",
    "Operating Margin", "Profit Margin", "Debt/Equity", "Free Cash Flow (TTM)", "RSI (14)",
    "Volatility 30D", "Volatility 90D", "Max Drawdown 1Y", "VaR 95% (1D)", "Sharpe (1Y)",
    "Risk Score", "Risk Bucket", "P/B", "P/S", "EV/EBITDA", "PEG", "Intrinsic Value",
    "Upside %",
    "Valuation Score", "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M",
    "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M", "Forecast Confidence",
    "Confidence Score", "Confidence Bucket", "Value Score", "Quality Score", "Momentum Score",
    "Growth Score", "Overall Score",
    "Fundamental View", "Technical View", "Risk View", "Value View",
    "Opportunity Score", "Rank (Overall)", "Recommendation",
    "Recommendation Reason", "Horizon Days", "Invest Period Label", "Position Qty", "Avg Cost",
    "Position Cost", "Position Value", "Unrealized P/L", "Unrealized P/L %", "Data Provider",
    "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",
    # v2.6.0 Insights group (Wave 3) — produced by core.insights_builder v1.0.0
    "Sector-Adj Score", "Conviction Score", "Top Factors", "Top Risks", "Position Size Hint",
]

_CANONICAL_80_KEYS: List[str] = [
    "symbol", "name", "asset_class", "exchange", "currency", "country", "sector", "industry",
    "current_price", "previous_close", "open_price", "day_high", "day_low", "week_52_high",
    "week_52_low", "price_change", "percent_change", "week_52_position_pct", "volume",
    "avg_volume_10d", "avg_volume_30d", "market_cap", "float_shares", "beta_5y", "pe_ttm",
    "pe_forward", "eps_ttm", "dividend_yield", "payout_ratio", "revenue_ttm", "revenue_growth_yoy",
    "gross_margin", "operating_margin", "profit_margin", "debt_to_equity", "free_cash_flow_ttm",
    "rsi_14", "volatility_30d", "volatility_90d", "max_drawdown_1y", "var_95_1d", "sharpe_1y",
    "risk_score", "risk_bucket", "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio",
    "intrinsic_value", "upside_pct", "valuation_score", "forecast_price_1m", "forecast_price_3m",
    "forecast_price_12m", "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "forecast_confidence", "confidence_score", "confidence_bucket", "value_score", "quality_score",
    "momentum_score", "growth_score", "overall_score",
    "fundamental_view", "technical_view", "risk_view", "value_view",
    "opportunity_score", "rank_overall",
    "recommendation", "recommendation_reason", "horizon_days", "invest_period_label", "position_qty",
    "avg_cost", "position_cost", "position_value", "unrealized_pl", "unrealized_pl_pct",
    "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",
    # v2.6.0 Insights group (Wave 3) — produced by core.insights_builder v1.0.0
    "sector_relative_score", "conviction_score", "top_factors", "top_risks", "position_size_hint",
]

_INSIGHTS_HEADERS = ["Section", "Item", "Symbol", "Metric", "Value", "Notes", "Last Updated (Riyadh)"]
_INSIGHTS_KEYS = ["section", "item", "symbol", "metric", "value", "notes", "last_updated_riyadh"]

_DICTIONARY_HEADERS = ["Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"]
_DICTIONARY_KEYS = ["sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes"]

# =============================================================================
# v4.4.0 derived contract widths (TRACKS CANONICAL — no hardcoded 90/93)
# =============================================================================
# Every instrument-page width derives from len(_CANONICAL_80_KEYS); Top_10
# derives as instrument_len + the Top_10 extras count. When the static
# canonical snapshot grows (e.g. synced to the live 115-key registry during
# the schema_registry pass / cascade #3), ALL of these auto-update with zero
# further edits to this router. At runtime _expected_len() still prefers the
# live registry's get_sheet_len() over this static-fallback map, so the
# router faithfully tracks whatever the deployed registry serves (90 today,
# 115/118 once the registry pass lands).
_CANONICAL_INSTRUMENT_LEN: int = len(_CANONICAL_80_KEYS)
_TOP10_EXTRA_COUNT: int = len(_TOP10_REQUIRED_FIELDS)
_TOP10_STATIC_LEN: int = _CANONICAL_INSTRUMENT_LEN + _TOP10_EXTRA_COUNT
_INSIGHTS_STATIC_LEN: int = len(_INSIGHTS_KEYS)
_DICTIONARY_STATIC_LEN: int = len(_DICTIONARY_KEYS)

_EXPECTED_SHEET_LENGTHS: Dict[str, int] = {
    "Market_Leaders": _CANONICAL_INSTRUMENT_LEN,
    "Global_Markets": _CANONICAL_INSTRUMENT_LEN,
    "Commodities_FX": _CANONICAL_INSTRUMENT_LEN,
    "Mutual_Funds": _CANONICAL_INSTRUMENT_LEN,
    "My_Portfolio": _CANONICAL_INSTRUMENT_LEN,
    _TOP10_PAGE: _TOP10_STATIC_LEN,
    _INSIGHTS_PAGE: _INSIGHTS_STATIC_LEN,
    _DICTIONARY_PAGE: _DICTIONARY_STATIC_LEN,
}

EMERGENCY_PAGE_SYMBOLS: Dict[str, List[str]] = {
    "Market_Leaders": ["2222.SR", "1120.SR", "2010.SR", "7010.SR", "AAPL", "MSFT", "NVDA", "GOOGL"],
    "Global_Markets": ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AVGO"],
    "Commodities_FX": ["GC=F", "BZ=F", "SI=F", "EURUSD=X", "GBPUSD=X", "JPY=X", "SAR=X", "CL=F"],
    "Mutual_Funds": ["SPY", "QQQ", "VTI", "VOO", "IWM"],
    "My_Portfolio": ["2222.SR", "AAPL", "MSFT", "QQQ", "GC=F"],
    "Insights_Analysis": ["2222.SR", "AAPL", "GC=F"],
    "Top_10_Investments": ["2222.SR", "1120.SR", "AAPL", "MSFT", "NVDA"],
}

_INTERNAL_FIELD_PREFIXES: Tuple[str, ...] = ("_skip_", "_internal_", "_meta_", "_debug_", "_trace_")
_INTERNAL_FIELDS_TO_STRIP: set = {
    "_skip_recommendation_synthesis",
    "_placeholder",
    "unit_normalization_warnings",
    "intrinsic_value_source",
}


def _strip_internal_fields(row: Any) -> Any:
    """v4.1.2: Remove engine internal coordination flags from output rows."""
    if not isinstance(row, dict):
        return row
    keys_to_remove: List[str] = []
    for k in list(row.keys()):
        ks = str(k)
        if ks in _INTERNAL_FIELDS_TO_STRIP:
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
        return _json_safe(vars(value))
    except Exception:
        return str(value)


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
        if is_dataclass(obj):
            dd = getattr(obj, "__dict__", None)
            if isinstance(dd, dict):
                return {k: v for k, v in dd.items() if not str(k).startswith("_")}
    except Exception:
        pass
    try:
        dd = getattr(obj, "__dict__", None)
        if isinstance(dd, dict):
            return dict(dd)
    except Exception:
        pass
    return {}


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


def _request_id(request: Request, x_request_id: Optional[str]) -> str:
    if x_request_id and _strip(x_request_id):
        return _strip(x_request_id)
    try:
        rid = _strip(getattr(request.state, "request_id", ""))
        if rid:
            return rid
    except Exception:
        pass
    return str(uuid.uuid4())[:12]


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
        if isinstance(v, list):
            out: List[str] = []
            seen = set()
            for item in v:
                s = _normalize_symbol_token(item) if "symbol" in k or "ticker" in k or k in {"code", "requested_symbol"} else _strip(item)
                if s and s not in seen:
                    seen.add(s)
                    out.append(s)
            if out:
                return out
        if isinstance(v, str) and v.strip():
            parts = _split_symbols_string(v)
            if "symbol" in k or "ticker" in k or k in {"code", "requested_symbol"}:
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
    criteria = body.get("criteria") if isinstance(body.get("criteria"), Mapping) else None
    if criteria:
        for key in (
            "symbols", "tickers", "tickers_list", "selected_symbols", "selected_tickers", "direct_symbols",
            "symbol", "ticker", "code", "requested_symbol",
        ):
            symbols.extend(_get_list(criteria, key))
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
    for key in (
        "limit", "offset", "top_n", "include_matrix", "risk_level", "risk_profile", "confidence_level",
        "investment_period_days", "horizon_days", "min_expected_roi", "min_roi", "min_confidence",
        "schema_only", "headers_only",
    ):
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
# Auth
# =============================================================================
def _is_public_path(settings: Any, path: str) -> bool:
    p = _strip(path)
    if p in {"/", "/health", "/readyz", "/livez", "/meta"} or p.endswith("/health"):
        return True
    public_paths_env = os.getenv("PUBLIC_PATHS", "") or os.getenv("AUTH_PUBLIC_PATHS", "")
    for candidate in [x.strip() for x in public_paths_env.split(",") if x.strip()]:
        if candidate.endswith("*") and p.startswith(candidate[:-1]):
            return True
        if p == candidate:
            return True
    if settings is None:
        return False
    for attr in ("public_paths", "PUBLIC_PATHS", "auth_public_paths", "AUTH_PUBLIC_PATHS"):
        try:
            value = getattr(settings, attr, None)
            for candidate in _as_list(value):
                c = _strip(candidate)
                if not c:
                    continue
                if c.endswith("*") and p.startswith(c[:-1]):
                    return True
                if p == c:
                    return True
        except Exception:
            continue
    return False


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
    path = str(getattr(getattr(request, "url", None), "path", "") or "")
    if _is_public_path(settings, path):
        return True
    headers_dict = dict(request.headers)
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
# Schema / contract helpers
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": f"Forbidden/removed page: {page}", "forbidden_pages": sorted(list(forbidden))},
        )
    ap = _safe_allowed_pages()
    if ap and page not in set(ap):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": f"Unknown page: {page}", "allowed_pages": ap},
        )


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


def _pad_contract(headers: Sequence[str], keys: Sequence[str], expected_len: int, *, header_prefix: str = "Column", key_prefix: str = "column") -> Tuple[List[str], List[str]]:
    hdrs, ks = _complete_schema_contract(headers, keys)
    while len(hdrs) < expected_len:
        i = len(hdrs) + 1
        hdrs.append(f"{header_prefix} {i}")
        ks.append(f"{key_prefix}_{i}")
    return hdrs[:expected_len], ks[:expected_len]


def _ensure_top10_contract(headers: Sequence[str], keys: Sequence[str], target_len: Optional[int] = None) -> Tuple[List[str], List[str]]:
    """v4.4.0 [FIX-B]: append the 3 Top_10 extras, then pad to `target_len`.

    The literal `93` clamp this function used to end with was the PRIMARY
    Top_10 truncation point: it silently dropped every column past index 93
    — including the entire v5.78.0 investability gate (forecast_source + 8
    gate columns) once the registry/canonical grew to 115. v4.4.0 removes
    the literal.

    Width semantics:
      - target_len is None  -> pad to the natural width (canonical + extras),
        i.e. no truncation, no extra padding.
      - target_len provided -> pad UP to it, but NEVER truncate below the
        canonical+extras we already hold. This is the hard anti-truncation
        invariant: even if a caller passes a stale/too-small width (e.g. 93
        against a 115-col canonical), real columns are preserved.

    Callers that need a specific width (registry-derived in _resolve_contract
    / _normalize_external_payload, or static-fallback-derived in
    _static_contract) pass it explicitly so the contract tracks the live
    schema instead of a frozen constant.
    """
    hdrs, ks = _complete_schema_contract(headers, keys)
    for field in _TOP10_REQUIRED_FIELDS:
        if field not in ks:
            ks.append(field)
            hdrs.append(_TOP10_REQUIRED_HEADERS[field])
    natural_len = len(ks)
    if target_len is None:
        effective_len = natural_len
    else:
        try:
            effective_len = max(int(target_len), natural_len)
        except Exception:
            effective_len = natural_len
    return _pad_contract(hdrs, ks, effective_len)


def _static_contract(page: str) -> Tuple[List[str], List[str], str]:
    # v4.4.0 [FIX-E]: widths derived from list lengths (was literal 90/93/7/9).
    if page == _TOP10_PAGE:
        h, k = _ensure_top10_contract(_CANONICAL_80_HEADERS, _CANONICAL_80_KEYS, target_len=_TOP10_STATIC_LEN)
        return h, k, "static_canonical_top10"
    if page == _INSIGHTS_PAGE:
        h, k = _pad_contract(_INSIGHTS_HEADERS, _INSIGHTS_KEYS, _INSIGHTS_STATIC_LEN)
        return h, k, "static_canonical_insights"
    if page == _DICTIONARY_PAGE:
        h, k = _pad_contract(_DICTIONARY_HEADERS, _DICTIONARY_KEYS, _DICTIONARY_STATIC_LEN)
        return h, k, "static_canonical_dictionary"
    h, k = _pad_contract(_CANONICAL_80_HEADERS, _CANONICAL_80_KEYS, _CANONICAL_INSTRUMENT_LEN)
    return h, k, "static_canonical_instrument"


def _expected_len(page: str) -> int:
    # v4.4.0: runtime-authoritative — prefer the live registry's width so the
    # router tracks whatever the deployed schema_registry serves. Falls back
    # to the derived static map only when the registry is unavailable.
    if callable(get_sheet_len):
        try:
            n = int(get_sheet_len(page))  # type: ignore[misc]
            if n > 0:
                return n
        except Exception:
            pass
    return _EXPECTED_SHEET_LENGTHS.get(page, _CANONICAL_INSTRUMENT_LEN)


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
    if isinstance(cols, tuple) and cols:
        return list(cols)
    fields = getattr(spec, "fields", None)
    if isinstance(fields, list) and fields:
        return fields
    if isinstance(fields, tuple) and fields:
        return list(fields)
    if isinstance(spec, Mapping):
        cols2 = spec.get("columns") or spec.get("fields")
        if isinstance(cols2, list) and cols2:
            return cols2
        if isinstance(cols2, tuple) and cols2:
            return list(cols2)
    return []


def _extract_headers_keys_from_spec(spec: Any) -> Tuple[List[str], List[str]]:
    headers: List[str] = []
    keys: List[str] = []
    for c in _schema_columns_from_any(spec):
        if isinstance(c, Mapping):
            h = _strip(c.get("header") or c.get("display_header") or c.get("displayHeader") or c.get("label") or c.get("title"))
            k = _strip(c.get("key") or c.get("field") or c.get("name") or c.get("id"))
        else:
            h = _strip(getattr(c, "header", getattr(c, "display_header", getattr(c, "displayHeader", getattr(c, "label", getattr(c, "title", None))))))
            k = _strip(getattr(c, "key", getattr(c, "field", getattr(c, "name", getattr(c, "id", None)))))
        if h or k:
            headers.append(h or k.replace("_", " ").title())
            keys.append(k or _normalize_key_name(h))
    if not headers and not keys and isinstance(spec, Mapping):
        headers2 = spec.get("headers") or spec.get("display_headers") or spec.get("sheet_headers")
        keys2 = spec.get("keys") or spec.get("fields") or spec.get("columns")
        if isinstance(headers2, list):
            headers = [_strip(x) for x in headers2 if _strip(x)]
        if isinstance(keys2, list):
            keys = [_strip(x) for x in keys2 if _strip(x)]
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
                h, k = _complete_schema_contract(headers, keys)
                return h, k, spec, "schema_registry.helpers"
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
            # v4.4.0 [FIX-C]: pass the registry-derived width so a 118-col
            # registry result is preserved (was clamped to 93, then the
            # len-check failed and discarded the registry entirely).
            headers, keys = _ensure_top10_contract(headers, keys, target_len=expected_len)
        else:
            headers, keys = _pad_contract(headers, keys, expected_len)
        if len(headers) == expected_len and len(keys) == expected_len:
            return headers, keys, spec, source
    sh, sk, ssrc = _static_contract(page)
    return sh, sk, {"source": ssrc, "page": page}, ssrc


# =============================================================================
# Payload extraction / normalization helpers
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
        preferred_nested_keys = (
            "quote", "analysis", "fundamentals", "forecast", "scores", "metrics", "summary", "snapshot",
            "payload", "data", "item", "record", "row", "meta", "stats", "price", "market_data",
        )
        for nk in preferred_nested_keys:
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
    raw = dict(raw or {})
    raw = _strip_internal_fields(raw)
    header_by_key = {str(k): str(h) for k, h in zip(schema_keys, schema_headers)}
    out: Dict[str, Any] = {}
    for k in schema_keys:
        ks = str(k)
        v = _extract_from_nested_raw(raw, _key_variants(ks))
        if v is None:
            h = header_by_key.get(ks, "")
            if h:
                v = _extract_from_nested_raw(raw, [h, h.lower(), h.upper()])
        if ks in {"warnings", "recommendation_reason", "selection_reason"} and isinstance(v, (list, tuple, set)):
            v = "; ".join([_strip(x) for x in v if _strip(x)])
        out[ks] = _json_safe(v)
    if "symbol" in out and not out.get("symbol"):
        out["symbol"] = _normalize_symbol_token(_extract_from_nested_raw(raw, _key_variants("symbol")))
    if "current_price" in out and out.get("current_price") is None:
        out["current_price"] = _extract_from_nested_raw(raw, _key_variants("current_price"))
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
        _to_number(row.get("value_score")),
        _to_number(row.get("quality_score")),
        _to_number(row.get("momentum_score")),
        _to_number(row.get("growth_score")),
        _to_number(row.get("current_price")),
    )


def _top10_selection_reason(row: Mapping[str, Any]) -> str:
    parts: List[str] = []
    labels = (
        ("overall_score", "Overall"),
        ("opportunity_score", "Opportunity"),
        ("expected_roi_3m", "Exp ROI 3M"),
        ("forecast_confidence", "Forecast Conf"),
        ("confidence_score", "Confidence"),
        ("recommendation", "Reco"),
    )
    for key, label in labels:
        value = row.get(key)
        if value in (None, "", [], {}, ()):
            continue
        parts.append(f"{label} {round(value, 2) if isinstance(value, float) else value}")
        if len(parts) >= 3:
            break
    return " | ".join(parts) if parts else "Top10 fallback selection based on strongest available composite signals."


def _top10_criteria_snapshot(row: Mapping[str, Any]) -> str:
    snapshot = {}
    for key in (
        "overall_score", "opportunity_score", "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
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
    normalized_rows: List[Dict[str, Any]] = []
    for raw_row in rows or []:
        normalized = _normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=(raw_row or {}))
        normalized_rows.append(normalized)

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for row in sorted(normalized_rows, key=_top10_sort_key, reverse=True):
        sym = _strip(row.get("symbol"))
        name = _strip(row.get("name"))
        key = sym or name or f"row_{len(deduped)+1}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    if requested_symbols:
        requested = {_strip(s): idx for idx, s in enumerate(requested_symbols) if _strip(s)}
        deduped.sort(
            key=lambda r: (
                0 if _strip(r.get("symbol")) in requested else 1,
                requested.get(_strip(r.get("symbol")), 10**6),
                -_top10_sort_key(r)[0], -_top10_sort_key(r)[1], -_top10_sort_key(r)[2],
                -_top10_sort_key(r)[3], -_top10_sort_key(r)[4], -_top10_sort_key(r)[5],
                -_top10_sort_key(r)[6], -_top10_sort_key(r)[7], -_top10_sort_key(r)[8],
                -_top10_sort_key(r)[9], -_top10_sort_key(r)[10],
            )
        )

    final_rows = deduped[: max(1, int(top_n))]
    for idx, row in enumerate(final_rows, start=1):
        row["top10_rank"] = idx
        if not _strip(row.get("selection_reason")):
            row["selection_reason"] = _top10_selection_reason(row)
        if not _strip(row.get("criteria_snapshot")):
            row["criteria_snapshot"] = _top10_criteria_snapshot(row)
    return final_rows


def _looks_like_symbol_token(x: Any) -> bool:
    s = _strip(x).upper()
    if not s or " " in s or len(s) > 24:
        return False
    return bool(re.fullmatch(r"[A-Z0-9\.\=\-\^:_/]{1,24}", s))


def _looks_like_explicit_row_dict(d: Any) -> bool:
    if not isinstance(d, Mapping) or not d:
        return False
    keyset = {str(k) for k in d.keys()}
    if keyset & {"symbol", "ticker", "code", "requested_symbol"}:
        return True
    if {"sheet", "header", "key"}.issubset(keyset):
        return True
    if {"section", "item"}.issubset(keyset):
        return True
    if {"top10_rank", "selection_reason"}.issubset(keyset):
        return True
    return False


def _is_meta_wrapper_key(key: Any) -> bool:
    s = _strip(key).lower()
    return s in {
        "status", "ok", "success", "error", "detail", "message", "meta", "request_id", "requestid",
        "version", "count", "total", "returned", "page", "sheet", "sheet_name", "sheetname",
        "page_name", "pagename", "route_family", "schema_source", "engine_source", "duration_ms",
        "duration", "elapsed_ms", "elapsed",
    }


def _page_aliases(page: str) -> List[str]:
    raw = _strip(page)
    if not raw:
        return []
    variants = {
        raw,
        raw.lower(),
        raw.upper(),
        raw.replace(" ", "_"),
        raw.replace("_", " "),
        raw.replace("-", "_"),
        raw.replace("_", "-"),
        raw.replace(" ", ""),
        raw.replace("_", "").lower(),
        raw.replace("-", "").lower(),
    }
    return [v for v in variants if v]


def _extract_contract_from_payload(payload: Any, depth: int = 0) -> Tuple[List[str], List[str]]:
    if payload is None or depth > 8:
        return [], []
    if isinstance(payload, Mapping):
        headers: List[str] = []
        keys: List[str] = []
        for h_name in ("headers", "display_headers", "sheet_headers", "column_headers"):
            hv = payload.get(h_name)
            if isinstance(hv, list):
                headers = [_strip(x) for x in hv if _strip(x)]
                if headers:
                    break
        for k_name in ("keys", "columns", "fields"):
            kv = payload.get(k_name)
            if isinstance(kv, list):
                keys = [_strip(x) for x in kv if _strip(x)]
                if keys:
                    break
        if headers or keys:
            return _complete_schema_contract(headers, keys)
        for nested_name in ("schema", "spec", "payload", "result", "response", "output", "data"):
            nested = payload.get(nested_name)
            if isinstance(nested, Mapping):
                hh, kk = _extract_contract_from_payload(nested, depth + 1)
                if hh or kk:
                    return hh, kk
        for v in payload.values():
            if isinstance(v, Mapping):
                hh, kk = _extract_contract_from_payload(v, depth + 1)
                if hh or kk:
                    return hh, kk
    return [], []


def _rows_from_index_map(payload: Mapping[str, Any]) -> List[Dict[str, Any]]:
    if not payload or not all(str(k).isdigit() for k in payload.keys()):
        return []
    out: List[Dict[str, Any]] = []
    for k in sorted(payload.keys(), key=lambda x: int(str(x))):
        v = payload[k]
        if isinstance(v, Mapping):
            out.append(dict(v))
        else:
            s = _strip(v)
            if s:
                out.append({"symbol": s})
    return out


def _rows_from_matrix(rows_matrix: Any, cols: Sequence[str]) -> List[Dict[str, Any]]:
    if not isinstance(rows_matrix, list) or not cols:
        return []
    keys = [_strip(c) for c in cols if _strip(c)]
    if not keys:
        return []
    out: List[Dict[str, Any]] = []
    for row in rows_matrix:
        if not isinstance(row, (list, tuple)):
            continue
        vals = list(row)
        out.append({keys[i]: (vals[i] if i < len(vals) else None) for i in range(len(keys))})
    return out


def _extract_rows_like(payload: Any, depth: int = 0, page: str = "") -> List[Dict[str, Any]]:
    if payload is None or depth > 8:
        return []
    if isinstance(payload, list):
        if not payload:
            return []
        if all(isinstance(x, dict) for x in payload):
            return [_to_plain_dict(r) for r in payload]
        if payload and isinstance(payload[0], (list, tuple)):
            return []
        out: List[Dict[str, Any]] = []
        for item in payload:
            s = _strip(item)
            if s:
                out.append({"symbol": s})
        return out
    if not isinstance(payload, Mapping):
        d = _to_plain_dict(payload)
        return [d] if _looks_like_explicit_row_dict(d) else []
    if _looks_like_explicit_row_dict(payload):
        return [dict(payload)]

    index_rows = _rows_from_index_map(payload)
    if index_rows:
        return index_rows

    maybe_symbol_map = True
    rows_from_symbol_map: List[Dict[str, Any]] = []
    for k, v in payload.items():
        if _is_meta_wrapper_key(k):
            continue
        if not isinstance(v, Mapping) or not _looks_like_symbol_token(k):
            maybe_symbol_map = False
            break
        row = dict(v)
        if not row.get("symbol"):
            row["symbol"] = _strip(k)
        rows_from_symbol_map.append(row)
    if maybe_symbol_map and rows_from_symbol_map:
        return rows_from_symbol_map

    if page:
        for alias in _page_aliases(page):
            nested = payload.get(alias)
            if isinstance(nested, (Mapping, list)):
                rows = _extract_rows_like(nested, depth + 1, page=page)
                if rows:
                    return rows

    for name in (
        "row_objects", "rowObjects", "rows", "data", "items", "records", "quotes", "recommendations",
        "results", "sheet_rows", "sheetRows", "page_rows", "pageRows", "sheet_payload", "sheetPayload",
        "sheet_result", "sheetResult", "dataset", "value", "content", "body", "payload", "result",
        "response", "output",
    ):
        value = payload.get(name)
        if isinstance(value, (list, Mapping)):
            rows = _extract_rows_like(value, depth + 1, page=page)
            if rows:
                return rows

    non_meta_children = [(k, v) for k, v in payload.items() if not _is_meta_wrapper_key(k) and isinstance(v, (Mapping, list))]
    if len(non_meta_children) == 1:
        rows = _extract_rows_like(non_meta_children[0][1], depth + 1, page=page)
        if rows:
            return rows
    for _, child in non_meta_children:
        rows = _extract_rows_like(child, depth + 1, page=page)
        if rows:
            return rows
    return []


def _extract_matrix_like(payload: Any, depth: int = 0, page: str = "") -> Optional[List[List[Any]]]:
    if payload is None or depth > 8:
        return None
    if isinstance(payload, dict):
        for name in ("rows_matrix", "matrix", "values"):
            value = payload.get(name)
            if isinstance(value, list):
                return [list(r) if isinstance(r, (list, tuple)) else [r] for r in value]
        rows_value = payload.get("rows")
        if isinstance(rows_value, list) and rows_value and isinstance(rows_value[0], (list, tuple)):
            return [list(r) if isinstance(r, (list, tuple)) else [r] for r in rows_value]
        if page:
            for alias in _page_aliases(page):
                nested = payload.get(alias)
                if isinstance(nested, Mapping):
                    mx = _extract_matrix_like(nested, depth + 1, page=page)
                    if mx is not None:
                        return mx
        for name in ("data", "payload", "result", "response", "output", "content", "body", "value", "dataset", "sheet_rows", "page_rows"):
            nested = payload.get(name)
            if isinstance(nested, dict):
                mx = _extract_matrix_like(nested, depth + 1, page=page)
                if mx is not None:
                    return mx
            if isinstance(nested, list) and nested and isinstance(nested[0], (list, tuple)):
                return [list(r) if isinstance(r, (list, tuple)) else [r] for r in nested]
        non_meta_children = [v for k, v in payload.items() if not _is_meta_wrapper_key(k) and isinstance(v, Mapping)]
        if len(non_meta_children) == 1:
            mx = _extract_matrix_like(non_meta_children[0], depth + 1, page=page)
            if mx is not None:
                return mx
    return None


def _extract_status_error(payload: Any) -> Tuple[str, Optional[str], Dict[str, Any]]:
    if not isinstance(payload, dict):
        return "success", None, {}
    status_out = _strip(payload.get("status")) or "success"
    error_out = payload.get("error") or payload.get("detail") or payload.get("message")
    meta_out = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    return status_out, (str(error_out) if error_out is not None else None), meta_out


def _payload_has_real_rows(payload: Any, page: str = "") -> bool:
    return bool(_extract_rows_like(payload, page=page) or _extract_matrix_like(payload, page=page))


def _row_data_density(row: Mapping[str, Any]) -> int:
    """v4.1.2: Count meaningful (non-empty, non-zero, non-placeholder) fields."""
    if not isinstance(row, Mapping):
        return 0
    count = 0
    for k, v in row.items():
        ks = str(k).lower()
        if ks.startswith("_") or ks in {"warnings", "data_provider", "last_updated_utc", "last_updated_riyadh"}:
            continue
        if v in (None, "", [], {}, ()):
            continue
        if isinstance(v, str) and v.strip().lower() in {"placeholder", "n/a", "na", "-", "--", "none", "null"}:
            continue
        count += 1
    return count


def _payload_quality_score(payload: Any, page: str = "") -> int:
    if payload is None:
        return -100
    if isinstance(payload, list):
        return 100 + min(25, len(payload))
    if not isinstance(payload, Mapping):
        return 0
    score = 0
    rows_like = _extract_rows_like(payload, page=page)
    matrix_like = _extract_matrix_like(payload, page=page)
    headers_like, keys_like = _extract_contract_from_payload(payload)
    if rows_like:
        score += 100 + min(25, len(rows_like))
        sampled = rows_like[: min(8, len(rows_like))]
        avg_density = sum(_row_data_density(r) for r in sampled) / max(1, len(sampled))
        if avg_density < 5:
            score -= 60
        elif avg_density < 12:
            score -= 25
        elif avg_density >= 25:
            score += 15
    if matrix_like:
        score += 85 + min(15, len(matrix_like))
    if headers_like:
        score += 8
    if keys_like:
        score += 8
    if page == _TOP10_PAGE and rows_like:
        for field in _TOP10_REQUIRED_FIELDS:
            if any(isinstance(r, Mapping) and r.get(field) not in (None, "", [], {}) for r in rows_like):
                score += 10
    status_out, error_out, _ = _extract_status_error(payload)
    status_lc = _strip(status_out).lower()
    if status_lc == "success":
        score += 4
    elif status_lc in {"warn", "partial"}:
        score += 2
    elif status_lc in {"error", "failed", "fail"}:
        score -= 4
    if _strip(error_out):
        score -= 6
    return score


def _project_row(keys: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    return {str(k): _json_safe(row.get(k)) for k in keys}


def _payload_envelope(*, page: str, route_family: str, headers: Sequence[str], keys: Sequence[str], row_objects: Sequence[Mapping[str, Any]], include_matrix: bool, request_id: str, started_at: float, mode: str, status_out: str, error_out: Optional[str], meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    hdrs = list(headers or [])
    ks = list(keys or [])
    rows_dict = [_strip_internal_fields(_project_row(ks, dict(r))) for r in (row_objects or [])]
    if page == _TOP10_PAGE:
        for idx, row in enumerate(rows_dict, start=1):
            row.setdefault("top10_rank", idx)
            row.setdefault("selection_reason", "Selected by analysis_sheet_rows.")
            row.setdefault("criteria_snapshot", "{}")
    matrix = _rows_to_matrix(rows_dict, ks) if include_matrix else []
    display_rows = [{hdrs[i]: _json_safe(row.get(ks[i])) for i in range(len(ks))} for row in rows_dict]
    return _json_safe({
        "status": status_out,
        "page": page,
        "sheet": page,
        "sheet_name": page,
        "route_family": route_family,
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
        "display_row_objects": display_rows,
        "display_items": display_rows,
        "display_records": display_rows,
        "rows_dict_display": display_rows,
        "count": len(rows_dict),
        "detail": error_out or "",
        "error": error_out,
        "version": ANALYSIS_SHEET_ROWS_VERSION,
        "request_id": request_id,
        "meta": {
            "duration_ms": round((time.time() - started_at) * 1000.0, 3),
            "mode": mode,
            "count": len(rows_dict),
            "row_object_count": len(rows_dict),
            "matrix_row_count": len(matrix),
            **(meta or {}),
        },
    })


# v4.3.1 [FIX-5]: explicit list of upstream meta keys to preserve from
# advanced_analysis v4.3.4 and investment_advisor v2.15.0 responses. We
# preserve them in _normalize_external_payload so the diagnostic chain
# stays visible end-to-end.
_UPSTREAM_DIAGNOSTIC_META_KEYS: Tuple[str, ...] = (
    "upstream_call_outcome",
    "upstream_call_summary",
    "upstream_call_status",
    "upstream_status",
    "upstream_error",
    "upstream_error_class",
    "engine_payload_diagnostic",
    "bridge_call_outcome",
    "bridge_call_summary",
    "bridge_error",
    "bridge_error_class",
    "bridge_source_module",
    "bridge_callable",
    "engine_method_used",
    "engine_method_summary",
    "engine_source",
)


def _normalize_external_payload(*, external_payload: Mapping[str, Any], page: str, headers: Sequence[str], keys: Sequence[str], include_matrix: bool, request_id: str, started_at: float, mode: str, route_family: str, meta_extra: Optional[Dict[str, Any]] = None, limit: int = 2000, offset: int = 0, top_n: int = 2000, requested_symbols: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    ext = dict(external_payload or {})
    payload_headers, payload_keys = _extract_contract_from_payload(ext)
    hdrs = list(headers or payload_headers or [])
    ks = list(keys or payload_keys or [])

    if page == _TOP10_PAGE:
        # v4.4.0 [FIX-D]: pass the registry-derived width (was an unwidth'd
        # re-clamp to the literal 93, the third Top_10 truncation point).
        hdrs, ks = _ensure_top10_contract(hdrs, ks, target_len=_expected_len(page))
    else:
        hdrs, ks = _pad_contract(hdrs, ks, _expected_len(page))

    rows = _extract_rows_like(ext, page=page)
    matrix = _extract_matrix_like(ext, page=page)
    if not rows and matrix:
        rows = _rows_from_matrix(matrix, ks)

    normalized_rows = [_normalize_to_schema_keys(schema_keys=ks, schema_headers=hdrs, raw=(r or {})) for r in rows]
    if page == _TOP10_PAGE:
        normalized_rows = _ensure_top10_rows(normalized_rows, requested_symbols=requested_symbols or [], top_n=top_n, schema_keys=ks, schema_headers=hdrs)

    normalized_rows = _slice(normalized_rows, limit=limit, offset=offset)
    status_out, error_out, ext_meta = _extract_status_error(ext)

    # v4.3.1 [FIX-5]: explicitly preserve upstream diagnostic keys.
    # `dict(ext_meta or {})` already copied them, but being explicit makes
    # the contract visible and survives any future refactor of meta merging.
    final_meta = dict(ext_meta or {})
    if isinstance(meta_extra, Mapping):
        final_meta.update(meta_extra)
    final_meta["source_route_family"] = _strip(ext.get("route_family")) or None
    final_meta.setdefault("dispatch", "external_proxy")

    effective_status = (status_out or "").lower()
    if effective_status == "warn":
        effective_status = "success" if normalized_rows else "warn"
    elif not effective_status:
        effective_status = "success" if normalized_rows else "partial"
    return _payload_envelope(
        page=page,
        route_family=route_family,
        headers=hdrs,
        keys=ks,
        row_objects=normalized_rows,
        include_matrix=include_matrix,
        request_id=request_id,
        started_at=started_at,
        mode=mode,
        status_out=effective_status,
        error_out=error_out,
        meta=final_meta,
    )


def _placeholder_value_for_key(page: str, key: str, symbol: str, row_index: int) -> Any:
    """v4.1.2: Conservative placeholder — no fake numerics."""
    kk = _normalize_key_name(key)

    # IDENTITY columns — safe to populate
    if kk in {"symbol", "ticker"}:
        return symbol
    if kk == "name":
        return symbol
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

    # PROVENANCE
    if kk == "data_provider":
        return "placeholder_no_live_data"
    if kk in {"last_updated_utc", "last_updated_riyadh"}:
        return datetime.utcnow().isoformat()
    if kk == "warnings":
        return "Placeholder fallback — no live data available for this symbol"

    # TOP10 metadata
    if kk == "top10_rank":
        return row_index
    if kk == "selection_reason":
        return "Placeholder — upstream returned no usable rows; no real ranking applied"
    if kk == "criteria_snapshot":
        return json.dumps({"symbol": symbol, "row_index": row_index, "source": "placeholder_no_live_data"}, ensure_ascii=False)

    # CATEGORICAL — leave empty rather than fake
    if kk in {"sector", "industry"}:
        return None
    if kk in {"recommendation", "recommendation_reason"}:
        return None
    if kk in {"risk_bucket", "confidence_bucket"}:
        return None
    if kk in {"invest_period_label"}:
        return None

    # NOTES (Data_Dictionary)
    if kk == "notes":
        return "Placeholder fallback row"

    # ALL OTHER FIELDS — return None instead of fabricating values.
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
            "source": "analysis_sheet_rows.local_dictionary_fallback",
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
        rows = _build_placeholder_rows(page=page, keys=keys, requested_symbols=requested_symbols or EMERGENCY_PAGE_SYMBOLS.get(page, []), limit=max(limit, top_n), offset=0)
        rows = _ensure_top10_rows(rows, requested_symbols=requested_symbols, top_n=top_n, schema_keys=keys, schema_headers=headers)
        return _slice(rows, limit=limit, offset=offset)
    return _build_placeholder_rows(page=page, keys=keys, requested_symbols=requested_symbols or EMERGENCY_PAGE_SYMBOLS.get(page, []), limit=limit, offset=offset)


def _ordered_payload_candidates(page: str, root_payload: Optional[Dict[str, Any]], root_meta: Dict[str, Any], adv_payload: Optional[Dict[str, Any]], adv_meta: Dict[str, Any], adapter_payload: Optional[Dict[str, Any]], adapter_meta: Dict[str, Any]) -> List[Tuple[Optional[Dict[str, Any]], Dict[str, Any], str]]:
    mapping = {
        "root_proxy": (root_payload, root_meta, "root_proxy"),
        "advanced_proxy": (adv_payload, adv_meta, "advanced_proxy"),
        "adapter": (adapter_payload, adapter_meta, "adapter"),
    }
    if page == _TOP10_PAGE:
        order = ("root_proxy", "advanced_proxy", "adapter")
    elif page in {_INSIGHTS_PAGE, _DICTIONARY_PAGE}:
        order = ("root_proxy", "advanced_proxy", "adapter")
    else:
        order = ("root_proxy", "adapter", "advanced_proxy")
    return [mapping[name] for name in order]


# =============================================================================
# v4.3.1 [FIX-1] Engine binding cascade with source tracking
# =============================================================================
async def _get_engine(request: Request) -> Tuple[Optional[Any], str]:
    """v4.3.1 [FIX-1]: explicit engine binding with source tracking.

    Probes core.data_engine_v2 FIRST (the v5.51.0+ engine with hardening),
    tracks the live binding in module-level CORE_ENGINE_SOURCE, and returns
    it via tuple. Surfaced via meta.engine_source on every response.

    Cascade order (most-preferred first):
      1. request.app.state.engine|data_engine|quote_engine|cache_engine
      2. core.data_engine_v2.get_engine() — async factory
      3. core.data_engine_v2.get_engine_if_ready() — sync ready-check
      4. core.data_engine.get_engine() — legacy fallback (BUG INDICATOR)
    """
    global CORE_ENGINE_SOURCE

    # Path 1: app.state — instance shared across requests
    try:
        st = getattr(request.app, "state", None)
        if st is not None:
            for attr in ("engine", "data_engine", "quote_engine", "cache_engine"):
                value = getattr(st, attr, None)
                if value is not None:
                    src = "app.state." + attr
                    CORE_ENGINE_SOURCE = src
                    return value, src
    except Exception:
        pass

    # Path 2: core.data_engine_v2.get_engine() — preferred async factory
    try:
        mod = importlib.import_module("core.data_engine_v2")
        get_engine = getattr(mod, "get_engine", None)
        if callable(get_engine):
            eng = get_engine()
            eng = await _maybe_await(eng)
            if eng is not None:
                src = "core.data_engine_v2.get_engine().result"
                CORE_ENGINE_SOURCE = src
                return eng, src
    except Exception as v2_err:
        try:
            logger.info(
                "[analysis_sheet_rows v%s] core.data_engine_v2.get_engine() unavailable: %s: %s",
                ANALYSIS_SHEET_ROWS_VERSION, v2_err.__class__.__name__, v2_err,
            )
        except Exception:
            pass

    # Path 3: core.data_engine_v2.get_engine_if_ready() — sync ready-check
    try:
        mod = importlib.import_module("core.data_engine_v2")
        get_ready = getattr(mod, "get_engine_if_ready", None)
        if callable(get_ready):
            eng = get_ready()
            if eng is not None:
                src = "core.data_engine_v2.get_engine_if_ready().result"
                CORE_ENGINE_SOURCE = src
                return eng, src
    except Exception as ready_err:
        try:
            logger.info(
                "[analysis_sheet_rows v%s] core.data_engine_v2.get_engine_if_ready() unavailable: %s: %s",
                ANALYSIS_SHEET_ROWS_VERSION, ready_err.__class__.__name__, ready_err,
            )
        except Exception:
            pass

    # Path 4: legacy fallback — bug indicator if reached
    try:
        mod = importlib.import_module("core.data_engine")
        get_engine = getattr(mod, "get_engine", None)
        if callable(get_engine):
            eng = get_engine()
            eng = await _maybe_await(eng)
            if eng is not None:
                src = "core.data_engine.get_engine"
                CORE_ENGINE_SOURCE = src
                try:
                    logger.warning(
                        "[analysis_sheet_rows v%s] all v2 binding patterns failed; using legacy "
                        "core.data_engine (this loses v5.51.0+ enrichment — investigate v2 exports)",
                        ANALYSIS_SHEET_ROWS_VERSION,
                    )
                except Exception:
                    pass
                return eng, src
    except Exception:
        pass

    CORE_ENGINE_SOURCE = "unavailable"
    return None, "unavailable"


async def _call_engine(fn: Any, *args: Any, **kwargs: Any) -> Any:
    return await _maybe_await(fn(*args, **kwargs))


def _dict_is_symbol_map(d: Dict[str, Any], symbols: Sequence[str]) -> bool:
    if not isinstance(d, dict) or not symbols:
        return False
    symset = set(symbols)
    keys = [k for k in d.keys() if isinstance(k, str)]
    if not keys:
        return False
    hit = sum(1 for k in keys if k in symset)
    return hit == len(symset) if symset else False


# =============================================================================
# v4.3.1 [FIX-4] _fetch_analysis_rows returns (results, diagnostic) tuple
# =============================================================================
async def _fetch_analysis_rows(engine: Any, symbols: List[str], *, mode: str, settings: Any, schema: Any, page: str = "", body: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """v4.3.1 [FIX-4]: returns (results_map, diagnostic) tuple.

    diagnostic: engine_method_used / engine_method_summary / engine_source.
    Same preferred-method order and per-symbol fallback logic as v4.3.0,
    with full per-attempt diagnostic capture.
    """
    diagnostic: Dict[str, Any] = {
        "engine_method_used": None,
        "engine_method_summary": [],
        "engine_source": CORE_ENGINE_SOURCE,
    }
    if not symbols or engine is None:
        diagnostic["engine_method_summary"].append({
            "method": "(none)",
            "outcome": "engine_unavailable" if engine is None else "no_symbols",
        })
        return {}, diagnostic

    preferred = [
        "get_analysis_rows_batch", "get_analysis_quotes_batch", "get_enriched_quotes_batch",
        "get_quotes_batch", "quotes_batch", "get_enriched_quotes", "get_quotes",
    ]
    kwarg_candidates = (
        {"mode": mode, "schema": schema, "page": page, "body": body},
        {"mode": mode, "schema": schema, "page": page},
        {"mode": mode, "schema": schema},
        {"schema": schema, "page": page},
        {"schema": schema},
        {"page": page, "mode": mode},
        {"mode": mode},
        {},
    )
    for method in preferred:
        fn = getattr(engine, method, None)
        if not callable(fn):
            diagnostic["engine_method_summary"].append({"method": method, "outcome": "missing"})
            continue
        method_outcome = "untried"
        method_error_class: Optional[str] = None
        method_error_msg: Optional[str] = None
        try:
            res = None
            matched_signature = False
            for kwargs in kwarg_candidates:
                kw = {k: v for k, v in kwargs.items() if v not in (None, "")}
                try:
                    res = await _call_engine(fn, symbols, **kw)
                    matched_signature = True
                    break
                except TypeError:
                    res = None
                    continue

            if not matched_signature:
                method_outcome = "all_signatures_typed_mismatch"
                diagnostic["engine_method_summary"].append({"method": method, "outcome": method_outcome})
                continue

            if isinstance(res, dict):
                if _dict_is_symbol_map(res, symbols):
                    diagnostic["engine_method_used"] = method
                    diagnostic["engine_method_summary"].append({"method": method, "outcome": "success_symbol_map"})
                    return res, diagnostic
                data = res.get("data") or res.get("rows") or res.get("items") or res.get("quotes")
                if isinstance(data, dict) and _dict_is_symbol_map(data, symbols):
                    diagnostic["engine_method_used"] = method
                    diagnostic["engine_method_summary"].append({"method": method, "outcome": "success_nested_symbol_map"})
                    return data, diagnostic
                if isinstance(data, list):
                    out = {s: r for s, r in zip(symbols, data)}
                    if out:
                        diagnostic["engine_method_used"] = method
                        diagnostic["engine_method_summary"].append({"method": method, "outcome": "success_nested_list"})
                        return out, diagnostic
                method_outcome = "empty_or_unrecognized_dict"
            elif isinstance(res, list):
                out = {s: r for s, r in zip(symbols, res)}
                if out:
                    diagnostic["engine_method_used"] = method
                    diagnostic["engine_method_summary"].append({"method": method, "outcome": "success_list"})
                    return out, diagnostic
                method_outcome = "empty_list"
            elif res is None:
                method_outcome = "returned_none"
            else:
                method_outcome = f"non_mapping_non_list_{type(res).__name__}"
        except Exception as exc:
            method_outcome = "raised"
            method_error_class = exc.__class__.__name__
            method_error_msg = str(exc)[:200]
            try:
                logger.warning(
                    "[analysis_sheet_rows v%s] engine.%s raised: %s: %s",
                    ANALYSIS_SHEET_ROWS_VERSION, method, method_error_class, method_error_msg,
                )
            except Exception:
                pass

        entry = {"method": method, "outcome": method_outcome}
        if method_error_class:
            entry["error_class"] = method_error_class
        if method_error_msg:
            entry["error_message"] = method_error_msg
        diagnostic["engine_method_summary"].append(entry)

    # Per-symbol fallback path
    out: Dict[str, Any] = {}
    per_dict_fn = getattr(engine, "get_enriched_quote_dict", None) or getattr(engine, "get_analysis_row_dict", None) or getattr(engine, "get_quote_dict", None)
    per_fn = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_analysis_row", None) or getattr(engine, "get_quote", None)
    per_method_used: Optional[str] = None
    per_failures = 0
    for s in symbols:
        try:
            if callable(per_dict_fn):
                per_method_used = per_method_used or "get_enriched_quote_dict|get_quote_dict"
                row_set = False
                for kwargs in kwarg_candidates:
                    kw = {k: v for k, v in kwargs.items() if v not in (None, "")}
                    try:
                        out[s] = await _call_engine(per_dict_fn, s, **kw)
                        row_set = True
                        break
                    except TypeError:
                        continue
                if not row_set:
                    out[s] = {"symbol": s, "error": "per_symbol_dict_call_failed"}
                    per_failures += 1
            elif callable(per_fn):
                per_method_used = per_method_used or "get_enriched_quote|get_quote"
                row_set = False
                for kwargs in kwarg_candidates:
                    kw = {k: v for k, v in kwargs.items() if v not in (None, "")}
                    try:
                        out[s] = await _call_engine(per_fn, s, **kw)
                        row_set = True
                        break
                    except TypeError:
                        continue
                if not row_set:
                    out[s] = {"symbol": s, "error": "per_symbol_call_failed"}
                    per_failures += 1
            else:
                out[s] = {"symbol": s, "error": "engine_missing_quote_method"}
                per_failures += 1
        except Exception as e:
            out[s] = {"symbol": s, "error": "{}: {}".format(e.__class__.__name__, str(e)[:200])}
            per_failures += 1

    if per_method_used and out:
        diagnostic["engine_method_used"] = per_method_used + " (per-symbol)"
        diagnostic["engine_method_summary"].append({
            "method": per_method_used + " (per-symbol)",
            "outcome": "partial" if per_failures else "success_per_symbol",
            "failures": per_failures,
            "total": len(symbols),
        })
    elif not out:
        try:
            logger.warning(
                "[analysis_sheet_rows v%s] no engine method produced rows for symbols=%r summary=%r",
                ANALYSIS_SHEET_ROWS_VERSION, symbols[:10], diagnostic["engine_method_summary"],
            )
        except Exception:
            pass
    return out, diagnostic


# =============================================================================
# v4.3.1 [FIX-2] Adapter call with full per-attempt summary
# =============================================================================
async def _call_core_sheet_rows_best_effort(*, page: str, limit: int, offset: int, mode: str, body: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str], List[Dict[str, Any]], str]:
    """v4.3.1 [FIX-2]: returns (payload, source, call_summary, outcome).

    outcome: success | all_signatures_typed_mismatch | raised |
    adapter_unavailable | no_attempts_executed.
    """
    if core_get_sheet_rows is None:
        return None, None, [], "adapter_unavailable"

    candidates = [
        ((), {"sheet": page, "limit": limit, "offset": offset, "mode": mode, "body": body}),
        ((), {"sheet": page, "limit": limit, "offset": offset, "mode": mode}),
        ((), {"sheet": page, "limit": limit, "offset": offset}),
        ((page,), {"limit": limit, "offset": offset, "mode": mode, "body": body}),
        ((page,), {"limit": limit, "offset": offset, "mode": mode}),
        ((page,), {"limit": limit, "offset": offset}),
        ((page,), {}),
    ]
    call_summary: List[Dict[str, Any]] = []
    last_err: Optional[Exception] = None

    for attempt_idx, (args, kwargs) in enumerate(candidates):
        kwargs_keys = sorted(list(kwargs.keys()))[:15]
        try:
            res = core_get_sheet_rows(*args, **kwargs)
            res = await _maybe_await(res)
            call_summary.append({
                "attempt_idx": attempt_idx,
                "kwargs_keys": kwargs_keys,
                "args_count": len(args),
                "outcome": "success",
            })
            if isinstance(res, dict):
                return res, "core:get_sheet_rows", call_summary, "success"
            if isinstance(res, list):
                return {"row_objects": res}, "core:get_sheet_rows", call_summary, "success"
            return None, "core:get_sheet_rows", call_summary, "success"
        except TypeError as e:
            call_summary.append({
                "attempt_idx": attempt_idx,
                "kwargs_keys": kwargs_keys,
                "args_count": len(args),
                "outcome": "typeerror",
                "error_class": "TypeError",
                "error_message": str(e)[:200],
            })
            last_err = e
            continue
        except Exception as e:
            call_summary.append({
                "attempt_idx": attempt_idx,
                "kwargs_keys": kwargs_keys,
                "args_count": len(args),
                "outcome": "error",
                "error_class": e.__class__.__name__,
                "error_message": str(e)[:200],
            })
            last_err = e
            try:
                logger.warning(
                    "[analysis_sheet_rows v%s] adapter raised on attempt %d: %s: %s",
                    ANALYSIS_SHEET_ROWS_VERSION, attempt_idx, e.__class__.__name__, str(e)[:200],
                )
            except Exception:
                pass
            return (
                {"status": "error", "error": "{}: {}".format(e.__class__.__name__, str(e)[:500]), "row_objects": []},
                "core:get_sheet_rows",
                call_summary,
                "raised",
            )

    if last_err is not None and isinstance(last_err, TypeError):
        return None, "core:get_sheet_rows", call_summary, "all_signatures_typed_mismatch"
    return None, None, call_summary, "no_attempts_executed"


# =============================================================================
# v4.3.1 [FIX-3] Proxy call with timeout + better error capture
# =============================================================================
async def _proxy_callable(*, module_names: Sequence[str], function_name: str, request: Request, body: Dict[str, Any], mode: str, include_matrix_q: Optional[bool], token: Optional[str], x_app_token: Optional[str], x_api_key: Optional[str], authorization: Optional[str], x_request_id: Optional[str], page: str) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """v4.3.1 [FIX-3]: asyncio.wait_for timeout + full error capture.

    meta keys: proxy_call_outcome (success | timeout | raised |
    missing_callable | non_dict_result | import_failed), proxy_error,
    proxy_error_class, proxy_timeout_sec.
    """
    timeout_sec = _analysis_proxy_timeout_sec()
    meta: Dict[str, Any] = {
        "proxy_attempted": True,
        "proxy_target": function_name,
        "proxy_page": page,
        "proxy_timeout_sec": timeout_sec,
    }
    imported = None
    imported_name = ""
    last_import_error: Optional[Exception] = None
    for module_name in module_names:
        try:
            imported = importlib.import_module(module_name)
            imported_name = module_name
            break
        except Exception as e:
            last_import_error = e
            continue
    if imported is None:
        meta["proxy_import_error"] = "{}: {}".format(
            (last_import_error.__class__.__name__ if last_import_error else "ImportError"),
            (str(last_import_error)[:300] if last_import_error else "import failed"),
        )
        meta["proxy_call_outcome"] = "import_failed"
        return None, meta
    fn = getattr(imported, function_name, None)
    if not callable(fn):
        meta["proxy_missing_callable"] = True
        meta["proxy_module"] = imported_name
        meta["proxy_call_outcome"] = "missing_callable"
        return None, meta

    proxy_body = dict(body or {})
    for k in ("page", "sheet", "sheet_name", "page_name", "worksheet", "name", "tab"):
        proxy_body.setdefault(k, page)

    try:
        # v4.3.1 [FIX-3]: timeout-bounded call. We bypass _maybe_await here
        # because it has a try/except: pass that silently swallows exceptions
        # raised inside awaited coroutines — turning a "raised" outcome into
        # "non_dict_result" and losing the actual error message + class. Doing
        # the awaitable check inline lets the exception propagate to our
        # except Exception handler below where we capture full diagnostic.
        _raw_call_result = fn(
            request=request,
            body=proxy_body,
            mode=mode or "",
            include_matrix_q=include_matrix_q,
            token=token,
            x_app_token=x_app_token,
            x_api_key=x_api_key,
            authorization=authorization,
            x_request_id=x_request_id,
        )
        if inspect.isawaitable(_raw_call_result):
            result = await asyncio.wait_for(_raw_call_result, timeout=timeout_sec)
        else:
            result = _raw_call_result
        if isinstance(result, dict):
            meta["proxy_ok"] = True
            meta["proxy_module"] = imported_name
            meta["proxy_status"] = _strip(result.get("status")) or "success"
            meta["proxy_has_rows"] = _payload_has_real_rows(result, page=page)
            meta["proxy_call_outcome"] = "success"
            # v4.3.1 [FIX-5]: copy upstream diagnostic from proxy result.meta
            # so it propagates even when this proxy is the "best" payload.
            try:
                rmeta = result.get("meta") if isinstance(result.get("meta"), Mapping) else {}
                for diag_key in _UPSTREAM_DIAGNOSTIC_META_KEYS:
                    if diag_key in rmeta:
                        meta["proxy_" + diag_key] = rmeta[diag_key]
            except Exception:
                pass
            return result, meta
        meta["proxy_non_dict"] = True
        meta["proxy_module"] = imported_name
        meta["proxy_call_outcome"] = "non_dict_result"
        return None, meta
    except asyncio.TimeoutError:
        meta["proxy_module"] = imported_name
        meta["proxy_call_outcome"] = "timeout"
        meta["proxy_error"] = "TimeoutError: proxy exceeded {:.1f}s".format(timeout_sec)
        meta["proxy_error_class"] = "TimeoutError"
        try:
            logger.warning(
                "[analysis_sheet_rows v%s] proxy timeout: %s.%s exceeded %.1fs (page=%s)",
                ANALYSIS_SHEET_ROWS_VERSION, imported_name, function_name, timeout_sec, page,
            )
        except Exception:
            pass
        return None, meta
    except Exception as e:
        # v4.3.1 [FIX-3]: capture FULL error message + class, not just str(e)
        meta["proxy_call_error"] = str(e)[:500]  # v4.3.0 backwards-compatible field
        meta["proxy_error"] = "{}: {}".format(e.__class__.__name__, str(e)[:500])
        meta["proxy_error_class"] = e.__class__.__name__
        meta["proxy_module"] = imported_name
        meta["proxy_call_outcome"] = "raised"
        try:
            logger.warning(
                "[analysis_sheet_rows v%s] proxy raised: %s.%s — %s: %s (page=%s)",
                ANALYSIS_SHEET_ROWS_VERSION, imported_name, function_name,
                e.__class__.__name__, str(e)[:200], page,
            )
        except Exception:
            pass
        return None, meta


def _pick_best_payload(candidates: Sequence[Tuple[Optional[Dict[str, Any]], Dict[str, Any], str]], page: str) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any], str]:
    best_payload: Optional[Dict[str, Any]] = None
    best_meta: Dict[str, Any] = {}
    best_name = ""
    best_score = -10**9
    for payload, meta, name in candidates:
        score = _payload_quality_score(payload, page=page) if isinstance(payload, dict) else -10**9
        if score > best_score:
            best_payload = payload
            best_meta = dict(meta or {})
            best_name = name
            best_score = score
    return best_payload, best_meta, best_name


# =============================================================================
# Health
# =============================================================================
@router.get("/health")
async def analysis_sheet_rows_health(request: Request) -> Dict[str, Any]:
    settings = None
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None
    auth_summary = None
    try:
        if callable(mask_settings) and settings is not None:
            masked = mask_settings(settings)
            auth_summary = {"open_mode_effective": masked.get("open_mode_effective"), "token_count": masked.get("token_count")}
    except Exception:
        auth_summary = None
    # v4.3.1 [FIX-1]: surface engine_source so operators see at-a-glance which
    # engine binding the router is using right now.
    try:
        _eng, eng_src = await _get_engine(request)
    except Exception:
        eng_src = CORE_ENGINE_SOURCE
    return _json_safe({
        "status": "ok",
        "service": "analysis_sheet_rows",
        "version": ANALYSIS_SHEET_ROWS_VERSION,
        "schema_registry_available": bool(get_sheet_spec is not None),
        "adapter_available": bool(core_get_sheet_rows is not None),
        "adapter_source": CORE_GET_SHEET_ROWS_SOURCE,
        "engine_source": eng_src,
        "allowed_pages_count": len(_safe_allowed_pages()),
        "auth": auth_summary,
        "path": str(getattr(getattr(request, "url", None), "path", "")),
        # v4.4.0: expose the registry-derived contract widths the router is
        # serving so operators can confirm at-a-glance whether Top_10 is at
        # the full canonical width or still tracking a stale 90/93 registry.
        "contract_widths": {
            "instrument_static_fallback": _CANONICAL_INSTRUMENT_LEN,
            "top10_static_fallback": _TOP10_STATIC_LEN,
            "market_leaders_effective": _expected_len("Market_Leaders"),
            "top10_effective": _expected_len(_TOP10_PAGE),
            "insights_effective": _expected_len(_INSIGHTS_PAGE),
            "dictionary_effective": _expected_len(_DICTIONARY_PAGE),
        },
        "proxy_targets": [
            "routes.advanced_analysis._run_advanced_sheet_rows_impl",
            "routes.advanced_sheet_rows._run_advanced_sheet_rows_impl",
        ],
        "proxy_timeout_sec": _analysis_proxy_timeout_sec(),
    })


# =============================================================================
# Internal implementation
# =============================================================================
async def _analysis_sheet_rows_impl_core(request: Request, body: Dict[str, Any], mode: str, include_matrix_q: Optional[bool], token: Optional[str], x_app_token: Optional[str], x_api_key: Optional[str], authorization: Optional[str], x_request_id: Optional[str]) -> Dict[str, Any]:
    start = time.time()
    request_id = _request_id(request, x_request_id)
    try:
        settings = get_settings_cached()
    except Exception:
        settings = None

    auth_token = _extract_auth_token(token_query=token, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization, settings=settings, request=request)
    if not _auth_passed(request=request, settings=settings, auth_token=auth_token, authorization=authorization):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    merged_body = _merge_body_with_query(body, request)
    page = _normalize_page_flexible(_pick_page_from_body(merged_body) or "Market_Leaders")
    _ensure_page_allowed(page)
    route_family = _route_family_flexible(page)
    include_matrix = _maybe_bool(merged_body.get("include_matrix"), include_matrix_q if include_matrix_q is not None else True)
    limit = max(1, min(5000, _maybe_int(merged_body.get("limit"), 2000)))
    offset = max(0, _maybe_int(merged_body.get("offset"), 0))
    top_n = max(1, min(5000, _maybe_int(merged_body.get("top_n"), limit)))
    schema_only = _maybe_bool(merged_body.get("schema_only"), False)
    headers_only = _maybe_bool(merged_body.get("headers_only"), False)
    requested_symbols = _extract_requested_symbols(merged_body, max(top_n, limit + offset, 50))

    debug_enabled = _analysis_sheet_rows_debug_enabled()
    if debug_enabled:
        try:
            logger.debug(
                "[analysis_sheet_rows v%s] page=%r route_family=%r limit=%d offset=%d top_n=%d symbols=%d",
                ANALYSIS_SHEET_ROWS_VERSION, page, route_family, limit, offset, top_n, len(requested_symbols),
            )
        except Exception:
            pass

    headers, keys, spec, schema_source = _resolve_contract(page)
    engine, engine_source = await _get_engine(request)

    if schema_only or headers_only:
        return _payload_envelope(
            page=page,
            route_family=route_family,
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
                "engine_source": engine_source,
                "adapter_source": CORE_GET_SHEET_ROWS_SOURCE,
                # v4.4.0: surface the resolved width so schema-only callers
                # (e.g. setup_sheet_headers) can confirm they're getting the
                # full canonical, not a clamped contract.
                "contract_len": len(keys),
            },
        )

    root_payload, root_meta = await _proxy_callable(
        module_names=("routes.advanced_analysis",),
        function_name="_run_advanced_sheet_rows_impl",
        request=request,
        body=merged_body,
        mode=mode,
        include_matrix_q=include_matrix_q,
        token=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        x_request_id=x_request_id,
        page=page,
    )

    adv_payload, adv_meta = await _proxy_callable(
        module_names=("routes.advanced_sheet_rows",),
        function_name="_run_advanced_sheet_rows_impl",
        request=request,
        body=merged_body,
        mode=mode,
        include_matrix_q=include_matrix_q,
        token=token,
        x_app_token=x_app_token,
        x_api_key=x_api_key,
        authorization=authorization,
        x_request_id=x_request_id,
        page=page,
    )

    fetch_limit = min(5000, max(limit + offset, top_n, 1))
    # v4.3.1 [FIX-2]: adapter call now returns 4-tuple
    adapter_payload, adapter_source, adapter_call_summary, adapter_call_outcome = await _call_core_sheet_rows_best_effort(
        page=page,
        limit=fetch_limit,
        offset=0,
        mode=mode or "",
        body=merged_body,
    )
    adapter_meta = {
        "adapter_attempted": True,
        "adapter_source": adapter_source or CORE_GET_SHEET_ROWS_SOURCE,
        "adapter_has_rows": _payload_has_real_rows(adapter_payload, page=page) if isinstance(adapter_payload, dict) else False,
        "adapter_call_outcome": adapter_call_outcome,
        "adapter_call_summary": adapter_call_summary[:5],
    }

    best_payload, best_meta, best_name = _pick_best_payload(
        _ordered_payload_candidates(page, root_payload, root_meta, adv_payload, adv_meta, adapter_payload, adapter_meta),
        page=page,
    )

    if isinstance(best_payload, dict):
        best_has_rows = _payload_has_real_rows(best_payload, page=page)
        best_status, best_error, _ = _extract_status_error(best_payload)
        best_status_lc = _strip(best_status).lower()
        best_is_usable = best_has_rows or (best_status_lc in {"success", "warn"} and page not in _SPECIAL_PAGES)
        if best_is_usable:
            meta_extra = {
                "schema_source": schema_source,
                "engine_source": engine_source,
                "best_source": best_name,
                "requested_symbols_count": len(requested_symbols),
                "preferred_order": [name for _payload, _meta, name in _ordered_payload_candidates(page, root_payload, root_meta, adv_payload, adv_meta, adapter_payload, adapter_meta)],
                **(best_meta or {}),
            }
            meta_extra["best_status"] = best_status_lc or "unknown"
            meta_extra["best_has_rows"] = best_has_rows
            if best_error:
                meta_extra.setdefault("upstream_error", best_error)
            return _normalize_external_payload(
                external_payload=best_payload,
                page=page,
                headers=headers,
                keys=keys,
                include_matrix=include_matrix,
                request_id=request_id,
                started_at=start,
                mode=mode,
                route_family=route_family,
                meta_extra=meta_extra,
                limit=limit,
                offset=offset,
                top_n=top_n,
                requested_symbols=requested_symbols,
            )

    # Symbol-mode instrument fallback
    if requested_symbols and page not in {_INSIGHTS_PAGE, _DICTIONARY_PAGE}:
        if engine is not None:
            # v4.3.1 [FIX-4]: now returns (results, diagnostic) tuple
            data_map, engine_diag = await _fetch_analysis_rows(
                engine, requested_symbols,
                mode=(mode or ""), settings=settings, schema=spec,
                page=page, body=merged_body,
            )
            normalized_rows: List[Dict[str, Any]] = []
            errors = 0
            for sym in requested_symbols:
                raw = _to_plain_dict(data_map.get(sym))
                if not raw:
                    raw = {"symbol": sym, "error": "missing_row"}
                    errors += 1
                elif isinstance(raw, dict) and raw.get("error"):
                    errors += 1
                normalized = _normalize_to_schema_keys(schema_keys=keys, schema_headers=headers, raw=raw)
                if "symbol" in keys and not normalized.get("symbol"):
                    normalized["symbol"] = sym
                normalized_rows.append(normalized)

            if page == _TOP10_PAGE:
                normalized_rows = _ensure_top10_rows(
                    normalized_rows,
                    requested_symbols=requested_symbols,
                    top_n=top_n,
                    schema_keys=keys,
                    schema_headers=headers,
                )

            normalized_rows = _slice(normalized_rows, limit=limit, offset=offset)
            status_out = "success" if errors == 0 else ("partial" if errors < len(requested_symbols) else "error")
            return _payload_envelope(
                page=page,
                route_family=route_family,
                headers=headers,
                keys=keys,
                row_objects=normalized_rows,
                include_matrix=include_matrix,
                request_id=request_id,
                started_at=start,
                mode=mode,
                status_out=status_out,
                error_out=f"{errors} errors" if errors else None,
                meta={
                    "requested_total": len(requested_symbols),
                    "returned": len(normalized_rows),
                    "errors": errors,
                    "dispatch": "instrument_mode",
                    "schema_source": schema_source,
                    "engine_source": engine_source,
                    "best_source": best_name or "engine",
                    "root_meta": root_meta,
                    "advanced_meta": adv_meta,
                    "adapter_meta": adapter_meta,
                    # v4.3.1 [FIX-4]: surface engine method diagnostics
                    "engine_method_used": engine_diag.get("engine_method_used"),
                    "engine_method_summary": engine_diag.get("engine_method_summary", [])[:8],
                },
            )

    # Non-empty local fail-soft fallback
    degraded_upstream_meta = {
        "schema_source": schema_source,
        "engine_source": engine_source,
        "best_source": best_name or "none",
        "root_meta": root_meta,
        "advanced_meta": adv_meta,
        "adapter_meta": adapter_meta,
    }
    if isinstance(best_payload, dict):
        degraded_upstream_meta["best_payload_status"] = _strip(best_payload.get("status"))
        degraded_upstream_meta["best_payload_had_rows"] = _payload_has_real_rows(best_payload, page=page)
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
    fallback_error = "Local non-empty fallback emitted after upstream degradation" if fallback_rows else "No usable rows returned; schema-shaped fallback emitted"
    return _payload_envelope(
        page=page,
        route_family=route_family,
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
            "dispatch": "analysis_wrapper_fail_soft_nonempty" if fallback_rows else "analysis_wrapper_fail_soft",
            "adapter_source": adapter_source or CORE_GET_SHEET_ROWS_SOURCE,
            **degraded_upstream_meta,
        },
    )


async def _analysis_sheet_rows_impl(request: Request, body: Dict[str, Any], mode: str, include_matrix_q: Optional[bool], token: Optional[str], x_app_token: Optional[str], x_api_key: Optional[str], authorization: Optional[str], x_request_id: Optional[str]) -> Dict[str, Any]:
    start = time.time()
    request_id = _request_id(request, x_request_id)
    merged_body = _merge_body_with_query(body, request)
    fallback_page = _normalize_page_flexible(_pick_page_from_body(merged_body) or "Market_Leaders")
    try:
        return await _analysis_sheet_rows_impl_core(
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
    except HTTPException:
        raise
    except Exception as e:
        try:
            logger.exception(
                "[analysis_sheet_rows v%s] _analysis_sheet_rows_impl_unhandled (page=%s)",
                ANALYSIS_SHEET_ROWS_VERSION, fallback_page,
            )
        except Exception:
            pass
        headers, keys, _spec, schema_source = _resolve_contract(fallback_page)
        return _payload_envelope(
            page=fallback_page,
            route_family=_route_family_flexible(fallback_page),
            headers=headers,
            keys=keys,
            row_objects=[],
            include_matrix=_maybe_bool(merged_body.get("include_matrix"), True),
            request_id=request_id,
            started_at=start,
            mode=mode,
            status_out="partial",
            error_out="analysis_sheet_rows runtime fallback: {}: {}".format(e.__class__.__name__, str(e)[:300]),
            meta={
                "dispatch": "analysis_sheet_rows_emergency_fallback",
                "schema_source": schema_source,
                "exception_type": type(e).__name__,
                "engine_source": CORE_ENGINE_SOURCE,
                "_engine_error": "{}: {}".format(e.__class__.__name__, str(e)[:500]),
                "_engine_error_class": e.__class__.__name__,
            },
        )


# =============================================================================
# Routes
# =============================================================================
@router.get("/sheet-rows")
async def analysis_sheet_rows_get(
    request: Request,
    page: str = Query(default="", description="sheet/page name"),
    sheet: str = Query(default="", description="sheet/page name"),
    sheet_name: str = Query(default="", description="sheet/page name"),
    page_name: str = Query(default="", description="sheet/page name"),
    worksheet: str = Query(default="", description="sheet/page name"),
    name: str = Query(default="", description="sheet/page name"),
    tab: str = Query(default="", description="sheet/page name"),
    symbols: str = Query(default="", description="comma-separated symbols"),
    tickers: str = Query(default="", description="comma-separated tickers"),
    direct_symbols: str = Query(default="", description="comma-separated direct symbols"),
    symbol: str = Query(default="", description="single symbol"),
    ticker: str = Query(default="", description="single ticker"),
    code: str = Query(default="", description="single code"),
    requested_symbol: str = Query(default="", description="single requested symbol"),
    limit: Optional[int] = Query(default=None),
    offset: Optional[int] = Query(default=None),
    top_n: Optional[int] = Query(default=None),
    mode: str = Query(default="", description="Optional mode hint for engine/provider"),
    include_matrix_q: Optional[bool] = Query(default=None, alias="include_matrix"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
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
        "page_name": page_name,
        "worksheet": worksheet,
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
    }.items():
        if v not in (None, ""):
            body[k] = v
    return await _analysis_sheet_rows_impl(
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
async def analysis_sheet_rows(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: str = Query(default="", description="Optional mode hint for engine/provider"),
    include_matrix_q: Optional[bool] = Query(default=None, alias="include_matrix"),
    token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    return await _analysis_sheet_rows_impl(
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
    "ANALYSIS_SHEET_ROWS_VERSION",
    "CORE_ENGINE_SOURCE",
    "CORE_GET_SHEET_ROWS_SOURCE",
]
