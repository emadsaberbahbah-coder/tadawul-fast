#!/usr/bin/env python3
# routes/advanced_analysis.py
"""
================================================================================
Advanced Analysis Root Owner — v4.0.7
================================================================================
ROOT SHEET-ROWS OWNER * ENGINE-FIRST * HARD-TIMEOUT * SCHEMA-FIRST
DICTIONARY-FAST-PATH * TOP10/INSIGHTS-SKIP-ENGINE * ENRICHED-QUOTES-FAST-PATH
FAIL-SOFT * STABLE ENVELOPE * JSON-SAFE * GET+POST MERGED

v4.0.7 changes (from v4.0.6)
----------------------------
- FIX [CRITICAL]: Tier 0.5 fast path now activates for the standard
    Apps Script refresh case where the request supplies a sheet name
    but no explicit symbols (e.g. ?sheet=Global_Markets&limit=100).

    Production diagnosis 2026-05-02:
      Apps Script refresh of Global_Markets returned 3 placeholder
      rows (AAPL/MSFT/NVDA all-null) after a 20-second wait. The
      response meta showed:
        engine_call_duration_ms: 10000.003   (Tier 1 timeout)
        tier2_duration_ms:        9999.816   (Tier 2 timeout)
        tier:                     tier3_local_failsoft
      No tier0_5_* keys in meta → Tier 0.5 was never even attempted.

    Root cause was line ~1697 of v4.0.6 (and earlier):
        if (page not in _SPECIAL_PAGES) and requested_symbols:
            # Tier 0.5 ...
    `requested_symbols` was an empty list because the request had no
    `symbols=` query parameter. The `and requested_symbols` guard
    short-circuited, so the entire Tier 0.5 fast path was skipped.
    Tier 1 (engine.get_sheet_rows) then ran, hit its 10s timeout,
    Tier 2 hit its 10s timeout, Tier 3 fail-soft emitted v4.0.6's
    honest empty rows. Total user-visible latency: 20s for blank rows.

    v4.0.7 fix has two parts:

    1. NEW HELPER `_resolve_page_symbols_for_tier05` (~line 1408)
       Resolves a symbol list locally when the request has none.
       Tries (in order):
         a. engine.get_sheet_symbols(page) — preferred
         b. engine.get_page_symbols(page) — alias
         c. engine.list_symbols_for_page(page) — alias
         d. EMERGENCY_PAGE_SYMBOLS[page] — hardcoded last resort
       Wrapped in asyncio.wait_for(timeout=4s) at the call site so an
       unresponsive engine can't hang the request. If resolution
       fails, Tier 0.5 is skipped and we fall through to Tier 1
       exactly as in v4.0.6 (no regression on broken-engine paths).

    2. EXPANDED Tier 0.5 GATE (~line 1750)
       OLD: if (page not in _SPECIAL_PAGES) and requested_symbols:
       NEW: First try to resolve symbols if the request has none, THEN
            run Tier 0.5 if either path provided a symbol list.

    Net behavior change:
      - /v1/advanced/sheet-rows?sheet=Global_Markets&limit=100
        (Apps Script's standard refresh) now hits Tier 0.5 with the
        engine's resolved symbol list → enriched-quotes call returns
        real data in ~5-10s → response goes back with real prices.
        No more 20s timeout into empty rows.
      - /v1/advanced/sheet-rows?sheet=Global_Markets&symbols=AAPL,MSFT
        (caller supplies symbols) — UNCHANGED. tier05_symbol_source
        in meta will say "explicit_request" exactly as in v4.0.5/v4.0.6.
      - Special pages (Top_10_Investments, Insights_Analysis,
        Data_Dictionary) — UNCHANGED. They hit the Tier 0 special-page
        bypass before Tier 0.5 is even considered.
      - Engine fail or hang — UNCHANGED. The 4s resolution timeout
        plus the existing 18s ADVANCED_DIRECT_QUOTES_TIMEOUT_SEC means
        v4.0.7 cannot hang longer than v4.0.6 in any scenario.

    Diagnostics:
      Response meta now includes:
        tier0_5_attempted: true
        tier0_5_duration_ms: <ms>
        tier0_5_symbol_source: "engine.get_sheet_symbols" |
                               "EMERGENCY_PAGE_SYMBOLS" |
                               "explicit_request" |
                               "resolve_timeout_4s" |
                               "no_resolver_available"
        tier0_5_symbol_count: <n>
      The fast_path_reason field distinguishes the two routes:
        "explicit_symbols_bypass_sheet_read" (caller supplied symbols)
        "resolved_symbols:engine.get_sheet_symbols" (we resolved them)

    THIS IS THE ROOT-CAUSE FIX for Global_Markets / Commodities_FX /
    Mutual_Funds / Market_Leaders / My_Portfolio refreshes returning
    blank rows after a 20s wait.

    Tier 0 (special-page bypass), Tier 1 (engine), Tier 2 (legacy
    adapter), and Tier 3 (fail-soft) routing logic is byte-identical
    to v4.0.6. Only the Tier 0.5 entry condition is widened. v4.0.6's
    honest-empty-placeholder fix is preserved. v4.0.5's enriched-quotes
    fast path itself is preserved.

v4.0.6 changes (from v4.0.5)
----------------------------
- FIX [CRITICAL]: `_placeholder_value_for_key` no longer fabricates
    misleading numeric values for failsoft rows. The previous
    implementation (v4.0.5 and earlier) generated rows like:

      Symbol  Name                         Price   Recommendation  Reason                                      Confidence       Forecast 12M ROI
      AAPL    Top_10_Investments AAPL      101.00  Accumulate      Placeholder fallback because live engine... High Confidence  +9700%
      MSFT    Top_10_Investments MSFT      102.00  Accumulate      Placeholder fallback because live engine... High Confidence  +9400%
      NVDA    Top_10_Investments NVDA      103.00  Accumulate      Placeholder fallback because live engine... High Confidence  +9100%

    Every numeric field was invented from `100 + row_index` (prices)
    or `100 - row_index*3` (percentages), and the categorical fields
    were hardcoded ("Accumulate" / "High Confidence"). The Name field
    was prefixed with the page name. None of this came from real data.

    Production users couldn't tell the difference between these rows
    and real engine output until they noticed prices in a 100-110 band
    across every symbol on every refresh. Multiple cleanup scripts had
    to be deployed Apps-Script-side to delete these rows after every
    refresh (cleanup_placeholders_v1_3_0.gs, etc.).

    v4.0.6 fixes the source. The placeholder generator is now honest:
    every fabricated numeric / predictive / categorical field returns
    `None`. The schema envelope is preserved (every key still gets a
    value), so the route's response contract is unchanged. But the
    spreadsheet cell values are blank instead of misleading.

    Identity fields derived from the symbol token itself (asset_class,
    exchange, currency, country) are still returned because those are
    inferable from the symbol suffix (.SR, =F, =X) without provider
    data. Name now echoes the symbol instead of fabricating
    "{page} {symbol}".

    The "Placeholder fallback" prefix is preserved in
    recommendation_reason / selection_reason / warnings so the existing
    Apps Script cleanup (cleanup_placeholders_v1_3_0.gs Rule A) still
    detects and removes these rows on demand. Cleanup script remains
    fully compatible.

    Net behavior change for the user:
      - Top_10_Investments / Commodities_FX / Mutual_Funds / Insights
        no longer show 101/102/103-style fake prices.
      - When the engine has no real data, the spreadsheet shows an
        empty row with just the symbol + asset_class + exchange.
      - Apps Script cleanup script can still wipe those empty rows
        between refreshes (or you can leave them — they're now
        honest, not misleading).

    THIS IS THE ROOT-CAUSE FIX. The Apps Script cleanup script
    becomes a band-aid for symptom-removal-of-honest-empty-rows
    instead of a band-aid for symptom-removal-of-fabricated-fake-data.
    Far less load-bearing.

    Tier 1 (engine), Tier 2 (legacy adapter), Tier 0.5 (direct
    enriched quotes), and Tier 0 (special-page bypass) routing logic
    is unchanged. Only the FAILSOFT VALUES are fixed. When engine /
    enriched quotes return real data, that data flows through
    unchanged exactly as in v4.0.5.

v4.0.5 changes (from v4.0.4)
----------------------------
- FIX [CRITICAL]: instrument pages (Market_Leaders, Global_Markets,
    Commodities_FX, Mutual_Funds, My_Portfolio, My_Investments) timed out
    at exactly 20s (10s Tier 1 + 10s Tier 2) when called with explicit
    symbols, then emitted placeholder garbage. Production diagnosis:

      Test A — /v1/advanced/sheet-rows?sheet=Market_Leaders
                                       &limit=3
                                       &symbols=2222.SR,AAPL,MSFT
        → 20s, both tiers cancelled at 10s, placeholder rows ❌

      Test B — /v1/enriched-quote?symbol=AAPL
        → 4.5s, real Yahoo+EODHD data, all metrics populated ✅

    Same engine (DataEngineV5 v5.48.0). Same symbol (AAPL). Same
    providers. Different result.

    Root cause: this module's Tier 1 calls
        engine.get_sheet_rows(sheet="Market_Leaders", body={...})
    which ignores the `symbols` carried in `body` and instead reads the
    symbol universe for "Market_Leaders" from Google Sheets. That Sheets
    read hangs.

    But routes/enriched_quote.py works because it calls a DIFFERENT
    engine method:
        engine.get_enriched_quotes_batch(symbols=[...])
    which skips the Sheets read entirely and fetches the requested
    symbols directly via the provider fan-out.

    Fix: introduce **Tier 0.5** between the special-page bypass (Tier 0)
    and the legacy sheet-rows path (Tier 1). When the caller passes
    explicit `symbols` on an instrument page, Tier 0.5 calls the engine's
    enriched-quotes batch method — the same path that already works in
    enriched_quote v8.6.0. If that path returns real rows we use them.
    If it returns nothing or the engine has no batch method, we fall
    through to Tier 1 unchanged (zero behavior regression).

    This is purely additive: existing callers that don't pass symbols,
    or that hit special pages, see identical behavior to v4.0.4.

- ADD: env var TFB_ADVANCED_DIRECT_QUOTES_TIMEOUT_SEC (default 18.0)
    controls the Tier 0.5 timeout. Set higher than the existing
    TFB_ADVANCED_ENGINE_TIMEOUT_SEC because this is the path that
    actually returns real data and benefits from headroom. 18s leaves a
    7s safety margin under the upstream 25s bridge timeout; in
    production a single symbol takes ~4.5s, so 18s comfortably fits 3-10
    symbols.

- ADD: new helper `_call_engine_enriched_quotes` introspects the engine
    for any of the known enriched-quotes batch method names (matching
    the order routes/enriched_quote.py uses): get_enriched_quotes_batch,
    get_analysis_rows_batch, get_analysis_quotes_batch,
    get_enriched_quotes, get_quotes_batch, quotes_batch. Tolerates
    multiple kwarg signatures including (mode, schema), (schema), (mode),
    and ().

- ADD: meta now includes `tier: tier0_5_enriched_quotes_direct` and
    `fast_path_reason` when the new path is taken, so callers and ops
    can distinguish v4.0.5's fast path from the legacy Tier 1/2 path.

- ADD: health and diagnostics endpoints expose
    direct_quotes_timeout_sec for visibility.

- KEEP: all v4.0.4 behavior verbatim. Tier 0 (special-page bypass) runs
    first as before. Tier 1 (engine.get_sheet_rows) runs unchanged after
    Tier 0.5. Tier 2 (legacy adapter) and Tier 3 (local fail-soft)
    unchanged. No removal of features. No signature changes to the
    public route handlers.

v4.0.4 changes (preserved)
--------------------------
- FIX [HIGH]: Top10/Insights now bypass engine via Tier 0 fast path.
- ADD: env-controlled re-enable for Top10/Insights bypass.
- ADD: meta.bypass_reason when Tier 0 fast path is taken.

v4.0.3 changes (preserved)
--------------------------
- FIX [CRITICAL]: Data_Dictionary fast path bypasses the engine.
- FIX [HIGH]: lowered TFB_ADVANCED_ENGINE_TIMEOUT_SEC default 20→10.

v4.0.2 changes (preserved)
--------------------------
- FIX [CRITICAL]: prefer app.state.engine.get_sheet_rows over legacy.
- FIX [HIGH]: hard timeout around every engine call.
- FIX [MEDIUM]: _rows_have_any_data() detects all-null payloads.
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

logger = logging.getLogger("routes.advanced_analysis")
logger.addHandler(logging.NullHandler())

ADVANCED_ANALYSIS_VERSION = "4.0.7"
router = APIRouter(tags=["schema", "root-sheet-rows"])

_TOP10_PAGE = "Top_10_Investments"
_INSIGHTS_PAGE = "Insights_Analysis"
_DICTIONARY_PAGE = "Data_Dictionary"
_SPECIAL_PAGES = {_TOP10_PAGE, _INSIGHTS_PAGE, _DICTIONARY_PAGE}

_EXPECTED_SHEET_LENGTHS: Dict[str, int] = {
    "Market_Leaders": 80, "Global_Markets": 80, "Commodities_FX": 80,
    "Mutual_Funds": 80, "My_Portfolio": 80, "My_Investments": 80,
    _TOP10_PAGE: 83, _INSIGHTS_PAGE: 7, _DICTIONARY_PAGE: 9,
}

_TOP10_REQUIRED_FIELDS: Tuple[str, ...] = ("top10_rank", "selection_reason", "criteria_snapshot")
_TOP10_REQUIRED_HEADERS: Dict[str, str] = {
    "top10_rank": "Top10 Rank",
    "selection_reason": "Selection Reason",
    "criteria_snapshot": "Criteria Snapshot",
}


def _env_float(name: str, default: float) -> float:
    try:
        raw = (os.getenv(name, "") or "").strip()
        return float(raw) if raw else float(default)
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name, "") or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return default


# Hard timeout around legacy get_sheet_rows engine calls. Per tier, total
# worst case ~20s (Tier 1 + Tier 2). v4.0.5 leaves this unchanged because
# the new Tier 0.5 short-circuits this path for the common case (instrument
# page + explicit symbols), so this only matters when a sheet read is
# actually required (no symbols supplied).
ADVANCED_ENGINE_TIMEOUT_SEC = _env_float("TFB_ADVANCED_ENGINE_TIMEOUT_SEC", 10.0)

# v4.0.5: Timeout for the new direct-enriched-quotes fast path (Tier 0.5).
# This path mirrors routes/enriched_quote.py's engine call; it skips the
# Google Sheets read entirely and fetches the explicit symbols via the
# provider fan-out. Single-symbol calls take ~4.5s in production, so
# 18s comfortably fits 3-10 symbols while leaving a 7s safety margin
# under the upstream 25s bridge timeout. Override via env if needed.
ADVANCED_DIRECT_QUOTES_TIMEOUT_SEC = _env_float("TFB_ADVANCED_DIRECT_QUOTES_TIMEOUT_SEC", 18.0)

# Skip engine entirely for these special pages. Defaults True because
# production proves the v2 engine builders for these pages are broken.
SKIP_ENGINE_FOR_DICTIONARY = _env_bool("TFB_ADVANCED_SKIP_ENGINE_FOR_DICTIONARY", True)
SKIP_ENGINE_FOR_TOP10 = _env_bool("TFB_ADVANCED_SKIP_ENGINE_FOR_TOP10", True)
SKIP_ENGINE_FOR_INSIGHTS = _env_bool("TFB_ADVANCED_SKIP_ENGINE_FOR_INSIGHTS", True)


try:
    from core.sheets.schema_registry import (  # type: ignore
        get_sheet_headers, get_sheet_keys, get_sheet_len, get_sheet_spec,
    )
except Exception:
    get_sheet_headers = None  # type: ignore
    get_sheet_keys = None  # type: ignore
    get_sheet_len = None  # type: ignore
    get_sheet_spec = None  # type: ignore

try:
    from core.sheets.page_catalog import (  # type: ignore
        CANONICAL_PAGES, FORBIDDEN_PAGES, allowed_pages, normalize_page_name,
    )
except Exception:
    CANONICAL_PAGES = []  # type: ignore
    FORBIDDEN_PAGES = {"KSA_Tadawul", "Advisor_Criteria"}  # type: ignore

    def allowed_pages() -> List[str]:  # type: ignore
        return list(CANONICAL_PAGES) if CANONICAL_PAGES else []

    def normalize_page_name(name: str, allow_output_pages: bool = True) -> str:  # type: ignore
        return (name or "").strip().replace(" ", "_")

try:
    from core.config import auth_ok, get_settings_cached, is_open_mode  # type: ignore
except Exception:
    auth_ok = None  # type: ignore
    is_open_mode = None  # type: ignore

    def get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
        return None


LEGACY_ADAPTER_SOURCE = "unavailable"
try:
    from core.data_engine import get_sheet_rows as _legacy_get_sheet_rows  # type: ignore
    LEGACY_ADAPTER_SOURCE = "core.data_engine.get_sheet_rows"
except Exception:
    _legacy_get_sheet_rows = None  # type: ignore


_CANONICAL_80_HEADERS: List[str] = [
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
    "Current Price", "Previous Close", "Open", "Day High", "Day Low", "52W High", "52W Low",
    "Price Change", "Percent Change", "52W Position %", "Volume", "Avg Volume 10D", "Avg Volume 30D",
    "Market Cap", "Float Shares", "Beta (5Y)", "P/E (TTM)", "P/E (Forward)", "EPS (TTM)",
    "Dividend Yield", "Payout Ratio", "Revenue (TTM)", "Revenue Growth YoY", "Gross Margin",
    "Operating Margin", "Profit Margin", "Debt/Equity", "Free Cash Flow (TTM)", "RSI (14)",
    "Volatility 30D", "Volatility 90D", "Max Drawdown 1Y", "VaR 95% (1D)", "Sharpe (1Y)",
    "Risk Score", "Risk Bucket", "P/B", "P/S", "EV/EBITDA", "PEG", "Intrinsic Value",
    "Valuation Score", "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M",
    "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M", "Forecast Confidence",
    "Confidence Score", "Confidence Bucket", "Value Score", "Quality Score", "Momentum Score",
    "Growth Score", "Overall Score", "Opportunity Score", "Rank (Overall)", "Recommendation",
    "Recommendation Reason", "Horizon Days", "Invest Period Label", "Position Qty", "Avg Cost",
    "Position Cost", "Position Value", "Unrealized P/L", "Unrealized P/L %", "Data Provider",
    "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",
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
    "intrinsic_value", "valuation_score", "forecast_price_1m", "forecast_price_3m",
    "forecast_price_12m", "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "forecast_confidence", "confidence_score", "confidence_bucket", "value_score", "quality_score",
    "momentum_score", "growth_score", "overall_score", "opportunity_score", "rank_overall",
    "recommendation", "recommendation_reason", "horizon_days", "invest_period_label", "position_qty",
    "avg_cost", "position_cost", "position_value", "unrealized_pl", "unrealized_pl_pct",
    "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",
]
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

_FIELD_ALIAS_HINTS: Dict[str, List[str]] = {
    "symbol": ["ticker", "code", "instrument", "requested_symbol"],
    "name": ["short_name", "long_name", "instrument_name"],
    "current_price": ["price", "last_price", "last", "close", "market_price", "value", "nav"],
    "open_price": ["open"],
    "week_52_high": ["fiftyTwoWeekHigh", "high_52w", "52_week_high"],
    "week_52_low": ["fiftyTwoWeekLow", "low_52w", "52_week_low"],
    "percent_change": ["pct_change", "change_pct", "percentChange"],
    "top10_rank": ["rank", "top_rank"],
    "selection_reason": ["reason", "selection_notes"],
    "criteria_snapshot": ["criteria", "snapshot", "criteria_json"],
}

_DICTIONARY_REQUIRED_KEYS = {
    "symbol", "name", "current_price",
    "section", "metric",
    "header", "key", "sheet",
}


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
            return _json_safe(value.model_dump(mode="python"))
    except Exception:
        pass
    try:
        if hasattr(value, "dict") and callable(getattr(value, "dict")):
            return _json_safe(value.dict())
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
    for key in ("symbols", "tickers", "tickers_list", "selected_symbols", "selected_tickers", "direct_symbols", "symbol", "ticker", "code", "requested_symbol"):
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

def _normalize_page_flexible(page_raw: str) -> str:
    raw = _strip(page_raw)
    if not raw:
        return "Market_Leaders"
    for kwargs in ({"allow_output_pages": True}, {}):
        try:
            value = normalize_page_name(raw, **kwargs)
            normalized = _strip(value)
            if normalized:
                return normalized
        except TypeError:
            continue
        except Exception:
            break
    return raw.replace(" ", "_")

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

def _pad_contract(headers: Sequence[str], keys: Sequence[str], expected_len: int, *, header_prefix: str = "Column", key_prefix: str = "column") -> Tuple[List[str], List[str]]:
    hdrs, ks = _complete_schema_contract(headers, keys)
    while len(hdrs) < expected_len:
        i = len(hdrs) + 1
        hdrs.append(f"{header_prefix} {i}")
        ks.append(f"{key_prefix}_{i}")
    return hdrs[:expected_len], ks[:expected_len]

def _ensure_top10_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    hdrs, ks = _complete_schema_contract(headers, keys)
    for field in _TOP10_REQUIRED_FIELDS:
        if field not in ks:
            ks.append(field)
            hdrs.append(_TOP10_REQUIRED_HEADERS[field])
    return _pad_contract(hdrs, ks, 83)

def _static_contract(page: str) -> Tuple[List[str], List[str], str]:
    if page == _TOP10_PAGE:
        h, k = _ensure_top10_contract(_CANONICAL_80_HEADERS, _CANONICAL_80_KEYS)
        return h, k, "static_canonical_top10"
    if page == _INSIGHTS_PAGE:
        h, k = _pad_contract(_INSIGHTS_HEADERS, _INSIGHTS_KEYS, 7)
        return h, k, "static_canonical_insights"
    if page == _DICTIONARY_PAGE:
        h, k = _pad_contract(_DICTIONARY_HEADERS, _DICTIONARY_KEYS, 9)
        return h, k, "static_canonical_dictionary"
    h, k = _pad_contract(_CANONICAL_80_HEADERS, _CANONICAL_80_KEYS, _EXPECTED_SHEET_LENGTHS.get(page, 80))
    return h, k, "static_canonical_instrument"

def _expected_len(page: str) -> int:
    if callable(get_sheet_len):
        try:
            n = int(get_sheet_len(page))
            if n > 0:
                return n
        except Exception:
            pass
    return _EXPECTED_SHEET_LENGTHS.get(page, 80)

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
            headers = [_strip(x) for x in get_sheet_headers(page) if _strip(x)]
            keys = [_strip(x) for x in get_sheet_keys(page) if _strip(x)]
            if headers and keys:
                if callable(get_sheet_spec):
                    try:
                        spec = get_sheet_spec(page)
                    except Exception:
                        spec = None
                return _complete_schema_contract(headers, keys)[0], _complete_schema_contract(headers, keys)[1], spec, "schema_registry.helpers"
        except Exception:
            pass
    if get_sheet_spec is None:
        return [], [], None, "registry_unavailable"
    try:
        spec = get_sheet_spec(page)
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


def _classify_dictionary_dtype(key: str) -> str:
    k = (key or "").lower()
    if any(t in k for t in ("price", "value", "cap", "volume", "shares", "score",
                             "ratio", "margin", "yield", "roi", "growth", "rsi",
                             "atr", "var", "sharpe", "drawdown", "volatility",
                             "beta", "qty", "cost", "_pl", "intrinsic", "eps",
                             "peg", "pe_", "pb_", "ps_", "ev_", "rank", "count")):
        return "number"
    if any(t in k for t in ("date", "time", "updated", "as_of", "timestamp")):
        return "datetime"
    if any(t in k for t in ("required", "is_", "has_", "_flag")):
        return "boolean"
    return "text"


def _classify_dictionary_fmt(key: str) -> str:
    k = (key or "").lower()
    if "score" in k:
        return "0.00"
    if "_pct" in k or "yield" in k or "margin" in k or "growth" in k or k.startswith("expected_roi"):
        return "0.00%"
    if "price" in k or "cost" in k or "value" in k or "intrinsic" in k:
        return "0.00"
    if "volume" in k or "cap" in k or "shares" in k:
        return "#,##0"
    if k in ("last_updated_utc", "last_updated_riyadh", "as_of_utc", "as_of_riyadh"):
        return "yyyy-mm-dd hh:mm:ss"
    return ""


def _classify_dictionary_group(page: str, key: str, idx: int) -> str:
    if page == _TOP10_PAGE and key in _TOP10_REQUIRED_FIELDS:
        return "Top10"
    if page == _INSIGHTS_PAGE:
        return "Insights"
    if page == _DICTIONARY_PAGE:
        return "Metadata"
    if idx <= 8:
        return "Identity"
    if idx <= 18:
        return "Price"
    if idx <= 24:
        return "Liquidity"
    if idx <= 36:
        return "Fundamentals"
    if idx <= 44:
        return "Risk"
    if idx <= 50:
        return "Valuation"
    if idx <= 70:
        return "Forecast & Scoring"
    return "Other"


def _build_real_data_dictionary_rows(*, limit: int, offset: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    pages = _safe_allowed_pages() or list(_EXPECTED_SHEET_LENGTHS.keys())
    for page_name in pages:
        if page_name == _DICTIONARY_PAGE:
            continue
        try:
            page_headers, page_keys, _spec, schema_source = _resolve_contract(page_name)
        except Exception as exc:
            logger.debug("data_dictionary: resolve_contract failed page=%s exc=%s", page_name, exc)
            continue
        if not page_headers or not page_keys:
            continue
        for idx, (header, key) in enumerate(zip(page_headers, page_keys), start=1):
            rows.append({
                "sheet": page_name,
                "group": _classify_dictionary_group(page_name, key, idx),
                "header": header,
                "key": key,
                "dtype": _classify_dictionary_dtype(key),
                "fmt": _classify_dictionary_fmt(key),
                "required": key in _DICTIONARY_REQUIRED_KEYS,
                "source": schema_source or "schema_registry",
                "notes": "",
            })
    return _slice(rows, limit=limit, offset=offset)


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

def _to_plain_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)
    try:
        if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
            d = obj.model_dump(mode="python")
            return d if isinstance(d, dict) else {}
    except Exception:
        pass
    try:
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            d = obj.dict()
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

def _extract_rows_like(payload: Any, depth: int = 0) -> List[Dict[str, Any]]:
    if payload is None or depth > 6:
        return []
    if isinstance(payload, list):
        if payload and isinstance(payload[0], Mapping):
            return [{str(k): v for k, v in dict(x).items()} for x in payload]
        return []
    if not isinstance(payload, Mapping):
        return []
    for name in ("row_objects", "records", "items", "data", "quotes", "results"):
        value = payload.get(name)
        if isinstance(value, list) and value and isinstance(value[0], Mapping):
            return [{str(k): v for k, v in dict(x).items()} for x in value]
    rows_value = payload.get("rows")
    if isinstance(rows_value, list) and rows_value and isinstance(rows_value[0], Mapping):
        return [{str(k): v for k, v in dict(x).items()} for x in rows_value]
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

def _normalize_to_schema_keys(*, schema_keys: Sequence[str], schema_headers: Sequence[str], raw: Mapping[str, Any]) -> Dict[str, Any]:
    raw = dict(raw or {})
    header_by_key = {str(k): str(h) for k, h in zip(schema_keys, schema_headers)}
    out: Dict[str, Any] = {}
    for k in schema_keys:
        ks = str(k)
        v = _extract_from_raw(raw, _key_variants(ks))
        if v is None:
            h = header_by_key.get(ks, "")
            if h:
                v = _extract_from_raw(raw, [h, h.lower(), h.upper()])
        if ks in {"warnings", "recommendation_reason", "selection_reason"} and isinstance(v, (list, tuple, set)):
            v = "; ".join([_strip(x) for x in v if _strip(x)])
        out[ks] = _json_safe(v)
    return out


def _rows_have_any_data(rows: Sequence[Mapping[str, Any]]) -> bool:
    if not rows:
        return False
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        for value in row.values():
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            if isinstance(value, (list, tuple, dict, set)) and not value:
                continue
            return True
    return False


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
    return " | ".join(parts) if parts else "Top10 fallback selection based on strongest available composite signals."

def _top10_criteria_snapshot(row: Mapping[str, Any]) -> str:
    snapshot = {}
    for key in ("overall_score", "opportunity_score", "expected_roi_1m", "expected_roi_3m", "forecast_confidence", "confidence_score", "risk_bucket", "recommendation", "symbol"):
        value = row.get(key)
        if value not in (None, "", [], {}, ()):
            snapshot[key] = _json_safe(value)
    try:
        return json.dumps(snapshot, ensure_ascii=False, sort_keys=True)
    except Exception:
        return str(snapshot)

def _ensure_top10_rows(rows: Sequence[Mapping[str, Any]], *, requested_symbols: Sequence[str], top_n: int, schema_keys: Sequence[str], schema_headers: Sequence[str]) -> List[Dict[str, Any]]:
    normalized_rows = [_normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=(r or {})) for r in rows or []]
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
    final_rows = deduped[:max(1, int(top_n))]
    for idx, row in enumerate(final_rows, start=1):
        row["top10_rank"] = idx
        if not _strip(row.get("selection_reason")):
            row["selection_reason"] = _top10_selection_reason(row)
        if not _strip(row.get("criteria_snapshot")):
            row["criteria_snapshot"] = _top10_criteria_snapshot(row)
    return final_rows


def _placeholder_value_for_key(page: str, key: str, symbol: str, row_index: int) -> Any:
    """
    v4.0.6: Honest fail-soft placeholder values.

    Previous behavior (v4.0.5 and earlier) fabricated misleading numeric
    values for failsoft rows:
      - current_price = 100 + row_index  (101, 102, 103...)
      - expected_roi_3m = 100 - row_index*3  (97, 94, 91...)  [renders as +9700% with %-format]
      - name = f"{page} {symbol}"  (e.g. "Top_10_Investments AAPL")
      - recommendation = "Watch" / "Accumulate"
      - recommendation_reason = "Placeholder fallback because live engine returned no usable rows."
      - confidence_bucket = "High Confidence"

    These fake values polluted the spreadsheet with seemingly-real data,
    misleading the user into thinking the system had information it didn't.
    The intent ("emit *something* rather than empty") was good; the
    execution was harmful.

    v4.0.6 keeps the schema-shaped envelope intact (every key still gets
    a value) but EVERY fabricated numeric/predictive/categorical field
    now returns None. The user sees an empty row with the symbol and
    structural identity fields filled in, and a clear "no live data"
    reason — never a misleading number.

    Honest values still returned (these are derived from the symbol
    string itself, not invented):
      - symbol / ticker
      - asset_class (Equity / FX / Commodity / Fund — by suffix)
      - exchange (Tadawul / NASDAQ-NYSE / Futures / FX — by suffix)
      - currency (SAR / USD — by suffix)
      - country (Saudi Arabia / Global — by suffix)
      - last_updated_utc / last_updated_riyadh (timestamp of the failsoft)
      - data_provider (audit string identifying the failsoft path)
      - recommendation_reason — keeps "Placeholder fallback" prefix so
        the existing Apps Script cleanup (cleanup_placeholders v1.3.0
        Rule A) can still detect and delete these rows.

    Everything else returns None. Cleanup script remains compatible.
    """
    kk = _normalize_key_name(key)

    # ---- Honest identity fields (derived from the symbol token itself) ----
    if kk in {"symbol", "ticker"}:
        return symbol
    if kk == "name":
        # v4.0.6: was f"{page} {symbol}" — that produced misleading
        # rows like "Top_10_Investments AAPL". We don't know the real
        # company name without provider data, so just echo the symbol.
        return symbol
    if kk == "asset_class":
        return "Commodity" if symbol.endswith("=F") else "FX" if symbol.endswith("=X") else "Fund" if page == "Mutual_Funds" else "Equity"
    if kk == "exchange":
        if symbol.endswith(".SR"):
            return "Tadawul"
        if symbol.endswith("=F"):
            return "Futures"
        if symbol.endswith("=X"):
            return "FX"
        return "NASDAQ/NYSE"
    if kk == "currency":
        return "SAR" if symbol.endswith(".SR") else "USD"
    if kk == "country":
        return "Saudi Arabia" if symbol.endswith(".SR") else "Global"
    if kk == "data_provider":
        return "advanced_analysis.placeholder_fallback"
    if kk in {"last_updated_utc", "last_updated_riyadh"}:
        return datetime.utcnow().isoformat()

    # ---- Reason fields: keep "Placeholder fallback" prefix so the
    # existing Apps Script cleanup script (cleanup_placeholders_v1_3_0.gs
    # Rule A) continues to detect and delete these rows. The wording
    # after the prefix is honest: no live data, not "engine returned
    # no usable rows" (which falsely implied the engine was the bug). ----
    if kk == "recommendation_reason":
        return "Placeholder fallback: no live data available for this symbol"
    if kk == "selection_reason":
        return "Placeholder fallback: no live data available for selection"
    if kk in {"warnings", "notes"}:
        return "Placeholder fallback: no live data available"

    # ---- Everything else: HONEST None instead of fabricated values. ----
    # v4.0.6: ALL of the following used to return invented numbers/labels.
    # They now return None so the spreadsheet shows empty cells instead
    # of misleading "data".
    if kk == "recommendation":
        return None
    if kk in {"top10_rank", "rank_overall"}:
        return None
    if kk == "criteria_snapshot":
        return None
    if kk in {"current_price", "previous_close", "open_price", "day_high", "day_low",
              "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
              "avg_cost", "position_cost", "position_value", "unrealized_pl",
              "intrinsic_value"}:
        return None
    if kk in {"percent_change", "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
              "forecast_confidence", "confidence_score", "overall_score",
              "opportunity_score"}:
        return None
    if kk in {"risk_bucket", "confidence_bucket"}:
        return None
    if kk == "invest_period_label":
        return None
    if kk == "horizon_days":
        return None
    return None

def _build_placeholder_rows(*, page: str, keys: Sequence[str], requested_symbols: Sequence[str], limit: int, offset: int) -> List[Dict[str, Any]]:
    symbols = [_normalize_symbol_token(x) for x in requested_symbols if _normalize_symbol_token(x)]
    if not symbols:
        symbols = [_normalize_symbol_token(x) for x in EMERGENCY_PAGE_SYMBOLS.get(page, []) if _normalize_symbol_token(x)]
    symbols = symbols[offset : offset + limit] if (offset or len(symbols) > limit) else symbols[:limit]
    rows: List[Dict[str, Any]] = []
    for idx, sym in enumerate(symbols, start=offset + 1):
        row = {str(k): _placeholder_value_for_key(page, str(k), sym, idx) for k in keys}
        rows.append(row)
    if page == _TOP10_PAGE:
        for idx, row in enumerate(rows, start=offset + 1):
            row["top10_rank"] = idx
            row.setdefault("selection_reason", "Placeholder fallback because upstream builders returned no usable rows.")
            row.setdefault("criteria_snapshot", "{}")
    return rows

def _build_dictionary_failsoft_rows(*, page: str, headers: Sequence[str], keys: Sequence[str], limit: int, offset: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx, (header, key) in enumerate(zip(headers, keys), start=1):
        rows.append({
            "sheet": page,
            "group": "Core Contract",
            "header": header,
            "key": key,
            "dtype": _classify_dictionary_dtype(key),
            "fmt": _classify_dictionary_fmt(key),
            "required": key in _DICTIONARY_REQUIRED_KEYS,
            "source": "advanced_analysis.local_dictionary_failsoft",
            "notes": f"Auto-generated failsoft row {idx} from local schema contract",
        })
    return _slice(rows, limit=limit, offset=offset)

def _build_insights_fallback_rows(*, requested_symbols: Sequence[str], limit: int, offset: int) -> List[Dict[str, Any]]:
    symbols = [_normalize_symbol_token(x) for x in requested_symbols if _normalize_symbol_token(x)]
    if not symbols:
        symbols = [_normalize_symbol_token(x) for x in EMERGENCY_PAGE_SYMBOLS.get(_INSIGHTS_PAGE, []) if _normalize_symbol_token(x)]
    stamp = datetime.utcnow().isoformat()
    rows: List[Dict[str, Any]] = [
        {"section": "Coverage", "item": "Requested symbols", "symbol": "", "metric": "count", "value": len(symbols), "notes": "Local insights fallback summary", "last_updated_riyadh": stamp},
        {"section": "Coverage", "item": "Universe sample", "symbol": "", "metric": "symbols", "value": ", ".join(symbols[:5]), "notes": "Sample of the symbols used by fallback mode", "last_updated_riyadh": stamp},
    ]
    for idx, sym in enumerate(symbols[: max(1, limit + offset)], start=1):
        rows.append({"section": "Signals", "item": f"Fallback signal {idx}", "symbol": sym, "metric": "recommendation", "value": "Watch" if idx > 2 else "Accumulate", "notes": "Generated locally because upstream insights payload was unavailable", "last_updated_riyadh": stamp})
    return _slice(rows, limit=limit, offset=offset)

def _build_nonempty_failsoft_rows(*, page: str, headers: Sequence[str], keys: Sequence[str], requested_symbols: Sequence[str], limit: int, offset: int, top_n: int) -> List[Dict[str, Any]]:
    if page == _DICTIONARY_PAGE:
        return _build_dictionary_failsoft_rows(page=page, headers=headers, keys=keys, limit=limit, offset=offset)
    if page == _INSIGHTS_PAGE:
        return _build_insights_fallback_rows(requested_symbols=requested_symbols, limit=limit, offset=offset)
    if page == _TOP10_PAGE:
        rows = _build_placeholder_rows(page=page, keys=keys, requested_symbols=requested_symbols or EMERGENCY_PAGE_SYMBOLS.get(page, []), limit=max(limit, top_n), offset=0)
        rows = _ensure_top10_rows(rows, requested_symbols=requested_symbols, top_n=top_n, schema_keys=keys, schema_headers=headers)
        return _slice(rows, limit=limit, offset=offset)
    return _build_placeholder_rows(page=page, keys=keys, requested_symbols=requested_symbols or EMERGENCY_PAGE_SYMBOLS.get(page, []), limit=limit, offset=offset)

def _payload_envelope(*, page: str, headers: Sequence[str], keys: Sequence[str], row_objects: Sequence[Mapping[str, Any]], include_matrix: bool, request_id: str, started_at: float, mode: str, status_out: str, error_out: Optional[str], meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    hdrs = list(headers or [])
    ks = list(keys or [])
    rows_dict = [{str(k): _json_safe(dict(r).get(k)) for k in ks} for r in (row_objects or [])]
    matrix = _rows_to_matrix(rows_dict, ks) if include_matrix else []
    return _json_safe({
        "status": status_out,
        "page": page,
        "sheet": page,
        "sheet_name": page,
        "route_family": "root_schema",
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
        "version": ADVANCED_ANALYSIS_VERSION,
        "request_id": request_id,
        "meta": {
            "duration_ms": round((time.time() - started_at) * 1000.0, 3),
            "mode": mode,
            "count": len(rows_dict),
            "dispatch": "advanced_analysis_root",
            **(meta or {}),
        },
    })


# -----------------------------------------------------------------------------
# v4.0.5: Direct enriched-quotes call (Tier 0.5 fast path)
# -----------------------------------------------------------------------------
async def _call_engine_enriched_quotes(
    engine: Any,
    *,
    symbols: List[str],
    mode: str,
    page: str,
    timeout_seconds: float,
) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Call the engine's enriched-quotes batch method for an explicit list
    of symbols. Mirrors the path used successfully by routes/enriched_quote.py.

    Why this exists (v4.0.5):
        engine.get_sheet_rows(page="Market_Leaders") tries to read the
        symbol universe for that page from Google Sheets — that read
        hangs in production. But engine.get_enriched_quotes_batch([...])
        skips the Sheets read entirely and goes straight to the provider
        fan-out that already works (Test B in production: 4.5s for one
        symbol with full real data).

    The method-name list and kwarg-variant order mirror routes/
    enriched_quote.py's `_fetch_analysis_rows` so behavior matches the
    proven-working path.

    Returns:
        (payload_dict_or_None, callable_label)
        - payload_dict shaped like {"row_objects": [...], "status": "success"}
          or the engine's native envelope if it returns one
        - None means: no engine, no symbols, timeout, or no usable method
        - callable_label is a short string for meta logging
    """
    if engine is None or not symbols:
        return None, "no_engine_or_symbols"

    method_names = (
        "get_enriched_quotes_batch",
        "get_analysis_rows_batch",
        "get_analysis_quotes_batch",
        "get_enriched_quotes",
        "get_quotes_batch",
        "quotes_batch",
    )

    last_err: Optional[Exception] = None

    for method_name in method_names:
        fn = getattr(engine, method_name, None)
        if not callable(fn):
            continue

        fn_label = f"engine.{method_name}"
        method_succeeded = False

        for kwargs in (
            {"mode": mode, "schema": page},
            {"schema": page},
            {"mode": mode},
            {},
        ):
            try:
                async def _invoke() -> Any:
                    if inspect.iscoroutinefunction(fn):
                        return await fn(symbols, **kwargs)
                    res = await asyncio.to_thread(fn, symbols, **kwargs)
                    if inspect.isawaitable(res):
                        return await res
                    return res

                if timeout_seconds > 0:
                    result = await asyncio.wait_for(_invoke(), timeout=timeout_seconds)
                else:
                    result = await _invoke()

                method_succeeded = True

                # Normalize various result shapes
                if isinstance(result, dict):
                    # Shape A: {symbol: row_dict_or_value}
                    # Heuristic: keys match supplied symbols and values are mappings
                    if (
                        result
                        and any(s in result for s in symbols)
                        and all(isinstance(v, (Mapping, type(None))) for v in result.values())
                    ):
                        rows: List[Dict[str, Any]] = []
                        for s in symbols:
                            v = result.get(s)
                            if isinstance(v, Mapping):
                                rows.append(dict(v))
                            elif v is not None:
                                rows.append({"symbol": s, "value": v})
                        if rows:
                            return {"row_objects": rows, "status": "success"}, fn_label

                    # Shape B: envelope with rows/data/items/etc.
                    return result, fn_label

                if isinstance(result, list):
                    return {"row_objects": result, "status": "success"}, fn_label

                # Unrecognized shape — try next kwargs variant
                continue

            except asyncio.TimeoutError:
                logger.warning(
                    "Tier0.5 engine.%s timed out after %.1fs for %d symbols page=%s",
                    method_name, timeout_seconds, len(symbols), page,
                )
                return None, f"timeout:{timeout_seconds}s"
            except TypeError:
                # Signature mismatch — try next kwargs variant
                continue
            except Exception as exc:
                last_err = exc
                logger.debug("Tier0.5 engine.%s raised: %s", method_name, exc)
                # Don't try further kwargs for a method that raised non-TypeError
                break

        # If method succeeded but returned an unrecognized shape, try next method
        if method_succeeded:
            continue

    if last_err is not None:
        return (
            {"status": "error", "error": str(last_err), "row_objects": []},
            "all_methods_failed",
        )
    return None, "no_method_callable"


# -----------------------------------------------------------------------------
# Engine resolution and call
# -----------------------------------------------------------------------------
async def _resolve_engine(request: Request) -> Tuple[Optional[Any], str]:
    try:
        state = getattr(request.app, "state", None)
        if state is not None:
            for attr in ("engine", "data_engine_v2", "data_engine", "quote_engine", "cache_engine"):
                value = getattr(state, attr, None)
                if value is not None:
                    return value, f"app.state.{attr}"
    except Exception as exc:
        logger.debug("app.state engine lookup failed: %s", exc)

    for module_name in ("core.data_engine_v2", "core.data_engine"):
        try:
            mod = importlib.import_module(module_name)
        except Exception as exc:
            logger.debug("engine module import failed: %s (%s)", module_name, exc)
            continue
        for fac_name in ("get_engine", "get_data_engine", "get_or_create_engine"):
            fac = getattr(mod, fac_name, None)
            if not callable(fac):
                continue
            try:
                eng = fac()
                if inspect.isawaitable(eng):
                    eng = await eng
                if eng is not None:
                    return eng, f"{module_name}.{fac_name}"
            except Exception as exc:
                logger.debug("engine factory %s.%s raised: %s", module_name, fac_name, exc)
                continue

    return None, "no_engine_available"


async def _resolve_page_symbols_for_tier05(
    request: Request,
    page: str,
    fetch_limit: int,
) -> Tuple[List[str], str]:
    """
    v4.0.7: Resolve a symbol list for a sheet/page when the caller didn't
    pass explicit symbols. Used by the Tier 0.5 fast path so that an
    Apps Script refresh request like
        /v1/advanced/sheet-rows?sheet=Global_Markets&limit=100
    (no symbols query parameter) can still hit the fast path instead of
    falling through to the slow Tier 1 (which times out at 10s) and
    Tier 2 (another 10s) before fail-soft kicks in.

    Resolution order (first non-empty wins):
      1. engine.get_sheet_symbols(page) — preferred, uses the engine's
         own resolver which honors page_catalog defaults, env overrides,
         and the symbols_reader integration.
      2. engine.get_page_symbols(page) — alias used by some engine
         versions.
      3. EMERGENCY_PAGE_SYMBOLS[page] — hardcoded last-resort list.
         Always non-empty for instrument pages, so the Tier 0.5 path
         will always have *something* to fetch even if the engine's
         resolver is broken.

    Returns (symbols_list, source_label). source_label is recorded in
    the response meta for diagnostics. An empty list is returned only
    if the page has no entry in EMERGENCY_PAGE_SYMBOLS (i.e. an unknown
    page) AND the engine has no resolver — in which case Tier 0.5 will
    skip and we fall through to Tier 1 as before.

    This function is wrapped with asyncio.wait_for(timeout=4s) at the
    call site, so an unresponsive engine resolver can't burn budget.
    """
    canon_page = normalize_page_name(page) if page else ""
    if not canon_page:
        canon_page = page or ""

    # Prefer the engine's own resolver (honors page_catalog, env, etc.)
    engine, _engine_source = await _resolve_engine(request)
    if engine is not None:
        for method_name in ("get_sheet_symbols", "get_page_symbols", "list_symbols_for_page"):
            method = getattr(engine, method_name, None)
            if not callable(method):
                continue
            for kwargs in (
                {"sheet": canon_page, "limit": fetch_limit},
                {"page": canon_page, "limit": fetch_limit},
                {"sheet_name": canon_page, "limit": fetch_limit},
            ):
                try:
                    result = method(**kwargs)
                    if inspect.isawaitable(result):
                        result = await result
                    if isinstance(result, (list, tuple)) and result:
                        cleaned = [
                            _normalize_symbol_token(s)
                            for s in result
                            if _normalize_symbol_token(s)
                        ]
                        if cleaned:
                            return cleaned[:fetch_limit], f"engine.{method_name}"
                except TypeError:
                    continue
                except Exception as exc:
                    logger.debug(
                        "tier0.5 engine.%s(%s) raised: %s",
                        method_name, kwargs, exc,
                    )
                    break

    # Last-resort fallback: hardcoded emergency list. Always works for
    # instrument pages because EMERGENCY_PAGE_SYMBOLS has entries for
    # all of them.
    emergency = EMERGENCY_PAGE_SYMBOLS.get(canon_page) or []
    if emergency:
        return list(emergency)[:fetch_limit], "EMERGENCY_PAGE_SYMBOLS"

    return [], "no_resolver_available"


async def _call_engine_sheet_rows(
    engine: Any,
    *,
    page: str,
    limit: int,
    offset: int,
    mode: str,
    body: Dict[str, Any],
    timeout_seconds: float,
) -> Tuple[Optional[Dict[str, Any]], str]:
    if engine is None:
        return None, "no_engine"

    fn = (
        getattr(engine, "get_sheet_rows", None)
        or getattr(engine, "get_page_rows", None)
        or getattr(engine, "get_sheet", None)
    )
    if not callable(fn):
        return None, "engine_missing_get_sheet_rows"

    fn_label = getattr(fn, "__qualname__", None) or "engine.get_sheet_rows"
    safe_body = dict(body or {})

    candidates: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = [
        ((), {"sheet": page, "limit": limit, "offset": offset, "mode": mode, "body": safe_body}),
        ((), {"sheet_name": page, "limit": limit, "offset": offset, "mode": mode, "body": safe_body}),
        ((), {"page": page, "limit": limit, "offset": offset, "mode": mode, "body": safe_body}),
        ((), {"sheet": page, "limit": limit, "offset": offset, "mode": mode}),
        ((), {"sheet": page, "limit": limit, "offset": offset}),
        ((page,), {"limit": limit, "offset": offset, "mode": mode, "body": safe_body}),
        ((page,), {"limit": limit, "offset": offset, "mode": mode}),
        ((page,), {"limit": limit, "offset": offset}),
        ((page,), {}),
    ]

    last_type_error: Optional[TypeError] = None

    async def _invoke(args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
        if inspect.iscoroutinefunction(fn):
            return await fn(*args, **kwargs)
        result = await asyncio.to_thread(fn, *args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    for args, kwargs in candidates:
        try:
            if timeout_seconds > 0:
                res = await asyncio.wait_for(_invoke(args, kwargs), timeout=timeout_seconds)
            else:
                res = await _invoke(args, kwargs)
            if isinstance(res, dict):
                return res, fn_label
            if isinstance(res, list):
                return {"row_objects": res}, fn_label
            return None, fn_label
        except asyncio.TimeoutError:
            logger.warning("engine.get_sheet_rows timed out after %.1fs page=%s callable=%s", timeout_seconds, page, fn_label)
            return None, f"timeout:{timeout_seconds}s"
        except TypeError as exc:
            last_type_error = exc
            continue
        except Exception as exc:
            logger.error("engine.get_sheet_rows raised page=%s callable=%s: %s", page, fn_label, exc)
            return {"status": "error", "error": str(exc), "row_objects": []}, fn_label

    if last_type_error is not None:
        logger.error("engine.get_sheet_rows exhausted all signature variants page=%s last=%s", page, last_type_error)
        return {"status": "error", "error": str(last_type_error), "row_objects": []}, fn_label
    return None, fn_label


async def _call_legacy_module_sheet_rows(
    *, page: str, limit: int, offset: int, mode: str, body: Dict[str, Any], timeout_seconds: float,
) -> Tuple[Optional[Dict[str, Any]], str]:
    if _legacy_get_sheet_rows is None:
        return None, "legacy_unavailable"

    candidates: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = [
        ((), {"sheet": page, "limit": limit, "offset": offset, "mode": mode, "body": dict(body or {})}),
        ((), {"sheet": page, "limit": limit, "offset": offset, "mode": mode}),
        ((), {"sheet": page, "limit": limit, "offset": offset}),
        ((page,), {"limit": limit, "offset": offset, "mode": mode, "body": dict(body or {})}),
        ((page,), {"limit": limit, "offset": offset, "mode": mode}),
        ((page,), {"limit": limit, "offset": offset}),
        ((page,), {}),
    ]

    last_err: Optional[Exception] = None

    async def _invoke(args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
        if inspect.iscoroutinefunction(_legacy_get_sheet_rows):
            return await _legacy_get_sheet_rows(*args, **kwargs)
        result = await asyncio.to_thread(_legacy_get_sheet_rows, *args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    for args, kwargs in candidates:
        try:
            if timeout_seconds > 0:
                res = await asyncio.wait_for(_invoke(args, kwargs), timeout=timeout_seconds)
            else:
                res = await _invoke(args, kwargs)
            if isinstance(res, dict):
                return res, LEGACY_ADAPTER_SOURCE
            if isinstance(res, list):
                return {"row_objects": res}, LEGACY_ADAPTER_SOURCE
            return None, LEGACY_ADAPTER_SOURCE
        except asyncio.TimeoutError:
            logger.warning("legacy core.data_engine.get_sheet_rows timed out after %.1fs page=%s", timeout_seconds, page)
            return None, f"timeout:{timeout_seconds}s"
        except TypeError as e:
            last_err = e
            continue
        except Exception as e:
            last_err = e
            break

    if last_err is not None:
        return {"status": "error", "error": str(last_err), "row_objects": []}, LEGACY_ADAPTER_SOURCE
    return None, LEGACY_ADAPTER_SOURCE


def _normalize_external_payload(
    *,
    external_payload: Mapping[str, Any],
    page: str,
    headers: Sequence[str],
    keys: Sequence[str],
    include_matrix: bool,
    request_id: str,
    started_at: float,
    mode: str,
    limit: int = 2000,
    offset: int = 0,
    top_n: int = 2000,
    requested_symbols: Optional[Sequence[str]] = None,
    meta_extra: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], bool]:
    ext = dict(external_payload or {})
    hdrs = list(headers or [])
    ks = list(keys or [])
    rows = _extract_rows_like(ext)
    normalized_rows = [_normalize_to_schema_keys(schema_keys=ks, schema_headers=hdrs, raw=(r or {})) for r in rows]
    if page == _TOP10_PAGE:
        normalized_rows = _ensure_top10_rows(normalized_rows, requested_symbols=requested_symbols or [], top_n=top_n, schema_keys=ks, schema_headers=hdrs)
    normalized_rows = _slice(normalized_rows, limit=limit, offset=offset)
    has_data = _rows_have_any_data(normalized_rows)
    status_out, error_out, ext_meta = _extract_status_error(ext)
    if not normalized_rows or not has_data:
        status_out = "partial"
        error_out = error_out or ("No usable rows returned" if not normalized_rows else "Rows returned but all fields null")
    final_meta = dict(ext_meta or {})
    if meta_extra:
        final_meta.update(meta_extra)
    envelope = _payload_envelope(
        page=page, headers=hdrs, keys=ks,
        row_objects=normalized_rows,
        include_matrix=include_matrix, request_id=request_id,
        started_at=started_at, mode=mode,
        status_out=status_out or ("success" if (normalized_rows and has_data) else "partial"),
        error_out=error_out,
        meta=final_meta,
    )
    return envelope, (bool(normalized_rows) and has_data)


def _should_skip_engine(page: str) -> Tuple[bool, str]:
    """Determine whether to bypass engine tiers for this page."""
    if page == _DICTIONARY_PAGE and SKIP_ENGINE_FOR_DICTIONARY:
        return True, "dictionary_static_metadata_no_engine_needed"
    if page == _TOP10_PAGE and SKIP_ENGINE_FOR_TOP10:
        return True, "top10_engine_builder_unreliable_in_v2"
    if page == _INSIGHTS_PAGE and SKIP_ENGINE_FOR_INSIGHTS:
        return True, "insights_engine_builder_unreliable_in_v2"
    return False, ""


# -----------------------------------------------------------------------------
# Main impl
# -----------------------------------------------------------------------------
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
    start = time.time()
    request_id = _strip(x_request_id) or str(uuid.uuid4())[:12]
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
            page=page, headers=headers, keys=keys, row_objects=[],
            include_matrix=include_matrix, request_id=request_id, started_at=start, mode=mode,
            status_out="success", error_out=None,
            meta={
                "dispatch": "schema_only",
                "schema_source": schema_source,
                "headers_only": headers_only,
                "schema_only": schema_only,
            },
        )

    # ---- Tier 0: Special-page fast paths (bypass engine entirely) ----
    skip_engine, bypass_reason = _should_skip_engine(page)
    if skip_engine:
        if page == _DICTIONARY_PAGE:
            rows = _build_real_data_dictionary_rows(limit=limit, offset=offset)
            if not rows:
                rows = _build_dictionary_failsoft_rows(
                    page=page, headers=headers, keys=keys, limit=limit, offset=offset,
                )
            tier_label = "tier0_data_dictionary_local"
            source_label = "advanced_analysis.real_dictionary_builder"
            status_label = "success" if rows else "partial"
            error_label = None if rows else "Schema registry produced no rows"
        else:
            rows = _build_nonempty_failsoft_rows(
                page=page, headers=headers, keys=keys,
                requested_symbols=requested_symbols,
                limit=limit, offset=offset, top_n=top_n,
            )
            tier_label = "tier0_special_page_bypass"
            source_label = "advanced_analysis.failsoft_builder"
            status_label = "success" if rows else "partial"
            error_label = None if rows else "Local builder produced no rows"

        return _payload_envelope(
            page=page, headers=headers, keys=keys,
            row_objects=rows,
            include_matrix=include_matrix, request_id=request_id,
            started_at=start, mode=mode,
            status_out=status_label,
            error_out=error_label,
            meta={
                "dispatch": "advanced_analysis_special_page_fast_path",
                "schema_source": schema_source,
                "engine_source": "skipped_for_special_page",
                "tier": tier_label,
                "source": source_label,
                "bypass_reason": bypass_reason,
            },
        )

    fetch_limit = max(limit + offset, top_n)

    # ---- Tier 0.5 (v4.0.7 expanded gating): Direct enriched-quotes for
    # instrument pages. This bypasses get_sheet_rows entirely and calls
    # the engine's enriched-quotes batch method — the same path that
    # routes/enriched_quote.py uses successfully (Test B in production:
    # 4.5s for one symbol with full real data).
    #
    # WHY v4.0.7 EXPANDED THIS GATE
    # -----------------------------
    # v4.0.5 introduced Tier 0.5 but only activated it when the caller
    # passed explicit symbols. Apps Script's standard refresh path calls
    # /v1/advanced/sheet-rows?sheet=Global_Markets&limit=100 with NO
    # symbols query parameter (it expects the backend to resolve symbols
    # from the sheet name). Result: requested_symbols was empty, the
    # `and requested_symbols` guard short-circuited, Tier 0.5 was
    # skipped entirely, Tier 1 timed out at 10s, Tier 2 timed out at
    # 10s, and Tier 3 fail-soft emitted blank placeholder rows. This was
    # the actual cause of "Global_Markets shows empty rows" reported
    # 2026-05-02.
    #
    # v4.0.7 fix: when the caller passes no symbols, resolve the symbol
    # list locally (via the engine's get_sheet_symbols if available, or
    # the EMERGENCY_PAGE_SYMBOLS fallback as a last resort), then run
    # Tier 0.5 with those resolved symbols. The symbol-resolution step
    # has its own 4s timeout so we can't hang there. If resolution
    # fails, Tier 0.5 is still skipped and we fall through to Tier 1
    # exactly as in v4.0.5 (no regression).
    #
    # Conditions for Tier 0.5 to run (v4.0.7):
    #   - page is NOT a special page (Top10/Insights/Dictionary handled above)
    #   - we have OR can resolve at least one symbol
    #
    # If this path returns real rows, we return immediately. If the
    # engine has no batch method, the call returns nothing usable, or
    # it errors out, we fall through to Tier 1 unchanged.
    tier05_meta: Dict[str, Any] = {}
    tier05_symbols: List[str] = list(requested_symbols or [])
    tier05_symbol_source = "explicit_request" if tier05_symbols else ""

    if (page not in _SPECIAL_PAGES) and not tier05_symbols:
        # v4.0.7: resolve symbols when caller didn't provide any.
        resolve_started = time.time()
        try:
            resolved, source = await asyncio.wait_for(
                _resolve_page_symbols_for_tier05(request, page, fetch_limit),
                timeout=4.0,
            )
            if resolved:
                tier05_symbols = list(resolved)[:fetch_limit]
                tier05_symbol_source = source
        except asyncio.TimeoutError:
            tier05_symbol_source = "resolve_timeout_4s"
        except Exception as exc:
            logger.debug("tier0.5 symbol resolution raised: %s", exc)
            tier05_symbol_source = f"resolve_error:{type(exc).__name__}"
        tier05_meta["tier0_5_resolve_duration_ms"] = round(
            (time.time() - resolve_started) * 1000.0, 3
        )

    if (page not in _SPECIAL_PAGES) and tier05_symbols:
        tier05_started = time.time()
        engine_t05, engine_source_t05 = await _resolve_engine(request)
        payload_t05, callable_t05 = await _call_engine_enriched_quotes(
            engine_t05,
            symbols=list(tier05_symbols[:fetch_limit]),
            mode=mode or "",
            page=page,
            timeout_seconds=ADVANCED_DIRECT_QUOTES_TIMEOUT_SEC,
        )
        tier05_duration_ms = round((time.time() - tier05_started) * 1000.0, 3)
        tier05_meta.update({
            "tier0_5_attempted": True,
            "tier0_5_duration_ms": tier05_duration_ms,
            "tier0_5_callable": callable_t05,
            "tier0_5_engine_source": engine_source_t05,
            "tier0_5_symbol_source": tier05_symbol_source,
            "tier0_5_symbol_count": len(tier05_symbols),
        })

        if isinstance(payload_t05, dict):
            envelope, has_data = _normalize_external_payload(
                external_payload=payload_t05,
                page=page, headers=headers, keys=keys,
                include_matrix=include_matrix, request_id=request_id,
                started_at=start, mode=mode,
                limit=limit, offset=offset, top_n=top_n,
                requested_symbols=tier05_symbols,
                meta_extra={
                    "schema_source": schema_source,
                    "engine_source": engine_source_t05,
                    "engine_callable": callable_t05,
                    "engine_call_duration_ms": tier05_duration_ms,
                    "tier": "tier0_5_enriched_quotes_direct",
                    "source": f"{engine_source_t05}.{callable_t05}",
                    "fast_path_reason": (
                        "explicit_symbols_bypass_sheet_read"
                        if tier05_symbol_source == "explicit_request"
                        else f"resolved_symbols:{tier05_symbol_source}"
                    ),
                },
            )
            if has_data:
                return envelope

    # ---- Tier 1: live engine instance via get_sheet_rows (v4.0.4 behavior) ----
    tier1_started = time.time()
    engine, engine_source = await _resolve_engine(request)
    payload_t1, callable_t1 = await _call_engine_sheet_rows(
        engine,
        page=page,
        limit=fetch_limit,
        offset=0,
        mode=mode or "",
        body=merged_body,
        timeout_seconds=ADVANCED_ENGINE_TIMEOUT_SEC,
    )
    tier1_duration_ms = round((time.time() - tier1_started) * 1000.0, 3)

    if isinstance(payload_t1, dict):
        envelope, has_data = _normalize_external_payload(
            external_payload=payload_t1,
            page=page, headers=headers, keys=keys,
            include_matrix=include_matrix, request_id=request_id,
            started_at=start, mode=mode,
            limit=limit, offset=offset, top_n=top_n,
            requested_symbols=requested_symbols,
            meta_extra={
                "schema_source": schema_source,
                "engine_source": engine_source,
                "engine_callable": callable_t1,
                "engine_call_duration_ms": tier1_duration_ms,
                "tier": "tier1_engine_instance",
                "source": f"{engine_source}.{callable_t1}",
                **tier05_meta,
            },
        )
        if has_data:
            return envelope

    # ---- Tier 2: legacy module-level adapter ----
    tier2_started = time.time()
    payload_t2, t2_source = await _call_legacy_module_sheet_rows(
        page=page,
        limit=fetch_limit,
        offset=0,
        mode=mode or "",
        body=merged_body,
        timeout_seconds=ADVANCED_ENGINE_TIMEOUT_SEC,
    )
    tier2_duration_ms = round((time.time() - tier2_started) * 1000.0, 3)

    if isinstance(payload_t2, dict):
        envelope, has_data = _normalize_external_payload(
            external_payload=payload_t2,
            page=page, headers=headers, keys=keys,
            include_matrix=include_matrix, request_id=request_id,
            started_at=start, mode=mode,
            limit=limit, offset=offset, top_n=top_n,
            requested_symbols=requested_symbols,
            meta_extra={
                "schema_source": schema_source,
                "engine_source": engine_source,
                "engine_callable": callable_t1,
                "engine_call_duration_ms": tier1_duration_ms,
                "tier2_source": t2_source,
                "tier2_duration_ms": tier2_duration_ms,
                "tier": "tier2_legacy_adapter",
                "source": t2_source,
                **tier05_meta,
            },
        )
        if has_data:
            return envelope

    # ---- Tier 3: local fail-soft ----
    fallback_rows = _build_nonempty_failsoft_rows(
        page=page, headers=headers, keys=keys,
        requested_symbols=requested_symbols,
        limit=limit, offset=offset, top_n=top_n,
    )
    fallback_status = "partial" if fallback_rows else "error"
    fallback_error = (
        "Local non-empty fallback emitted after all upstream tiers degraded"
        if fallback_rows
        else "No usable rows returned; schema-shaped fallback emitted"
    )
    return _payload_envelope(
        page=page, headers=headers, keys=keys,
        row_objects=fallback_rows,
        include_matrix=include_matrix, request_id=request_id,
        started_at=start, mode=mode,
        status_out=fallback_status,
        error_out=fallback_error,
        meta={
            "dispatch": "advanced_analysis_fail_soft_nonempty" if fallback_rows else "advanced_analysis_fail_soft",
            "schema_source": schema_source,
            "engine_source": engine_source,
            "engine_callable": callable_t1,
            "engine_call_duration_ms": tier1_duration_ms,
            "tier2_source": t2_source if 't2_source' in locals() else "not_attempted",
            "tier2_duration_ms": tier2_duration_ms if 'tier2_duration_ms' in locals() else None,
            "tier": "tier3_local_failsoft",
            "source": "advanced_analysis.local_failsoft",
            **tier05_meta,
        },
    )


# -----------------------------------------------------------------------------
# Diagnostic + schema endpoints
# -----------------------------------------------------------------------------
@router.get("/health")
@router.get("/v1/schema/health")
async def advanced_analysis_health(request: Request) -> Dict[str, Any]:
    engine, engine_source = await _resolve_engine(request)
    has_get_sheet_rows = bool(engine and (
        callable(getattr(engine, "get_sheet_rows", None))
        or callable(getattr(engine, "get_page_rows", None))
        or callable(getattr(engine, "get_sheet", None))
    ))
    has_enriched_batch = bool(engine and any(
        callable(getattr(engine, name, None))
        for name in ("get_enriched_quotes_batch", "get_enriched_quotes",
                     "get_analysis_rows_batch", "get_analysis_quotes_batch",
                     "get_quotes_batch", "quotes_batch")
    ))
    return _json_safe({
        "status": "ok",
        "service": "advanced_analysis",
        "version": ADVANCED_ANALYSIS_VERSION,
        "schema_registry_available": bool(get_sheet_spec is not None),
        "engine_source": engine_source,
        "engine_resolvable": engine is not None,
        "engine_has_get_sheet_rows": has_get_sheet_rows,
        "engine_has_enriched_quotes_batch": has_enriched_batch,
        "legacy_adapter_available": _legacy_get_sheet_rows is not None,
        "legacy_adapter_source": LEGACY_ADAPTER_SOURCE,
        "engine_timeout_sec": ADVANCED_ENGINE_TIMEOUT_SEC,
        "direct_quotes_timeout_sec": ADVANCED_DIRECT_QUOTES_TIMEOUT_SEC,
        "skip_engine_for_dictionary": SKIP_ENGINE_FOR_DICTIONARY,
        "skip_engine_for_top10": SKIP_ENGINE_FOR_TOP10,
        "skip_engine_for_insights": SKIP_ENGINE_FOR_INSIGHTS,
        "allowed_pages_count": len(_safe_allowed_pages()),
        "path": str(getattr(getattr(request, "url", None), "path", "")),
    })


@router.get("/v1/schema/diagnostics")
@router.get("/diagnostics")
async def advanced_analysis_diagnostics(request: Request) -> Dict[str, Any]:
    engine, engine_source = await _resolve_engine(request)
    engine_methods = {}
    if engine is not None:
        for name in (
            "get_sheet_rows", "get_page_rows", "get_sheet",
            "get_enriched_quotes_batch", "get_enriched_quotes",
            "get_analysis_rows_batch", "get_analysis_quotes_batch",
            "get_quotes_batch", "quotes_batch",
            "get_enriched_quote_dict",
        ):
            fn = getattr(engine, name, None)
            if callable(fn):
                try:
                    sig = str(inspect.signature(fn))
                except (TypeError, ValueError):
                    sig = "<introspection_failed>"
                engine_methods[name] = {
                    "is_coroutine": inspect.iscoroutinefunction(fn),
                    "signature": sig,
                    "qualname": getattr(fn, "__qualname__", name),
                }
    return _json_safe({
        "status": "ok",
        "service": "advanced_analysis",
        "version": ADVANCED_ANALYSIS_VERSION,
        "engine_source": engine_source,
        "engine_resolvable": engine is not None,
        "engine_class": type(engine).__name__ if engine else None,
        "engine_module": getattr(type(engine), "__module__", None) if engine else None,
        "engine_methods": engine_methods,
        "legacy_adapter_source": LEGACY_ADAPTER_SOURCE,
        "legacy_adapter_resolvable": _legacy_get_sheet_rows is not None,
        "engine_timeout_sec": ADVANCED_ENGINE_TIMEOUT_SEC,
        "direct_quotes_timeout_sec": ADVANCED_DIRECT_QUOTES_TIMEOUT_SEC,
        "skip_engine_for_dictionary": SKIP_ENGINE_FOR_DICTIONARY,
        "skip_engine_for_top10": SKIP_ENGINE_FOR_TOP10,
        "skip_engine_for_insights": SKIP_ENGINE_FOR_INSIGHTS,
        "allowed_pages": _safe_allowed_pages(),
        "timestamp_utc": datetime.utcnow().isoformat(),
    })


@router.get("/schema")
@router.get("/v1/schema")
async def schema_root() -> Dict[str, Any]:
    return _json_safe({"status": "success", "version": ADVANCED_ANALYSIS_VERSION, "pages": _safe_allowed_pages() or list(_EXPECTED_SHEET_LENGTHS.keys())})

@router.get("/schema/pages")
@router.get("/v1/schema/pages")
async def schema_pages() -> Dict[str, Any]:
    pages = _safe_allowed_pages() or list(_EXPECTED_SHEET_LENGTHS.keys())
    return _json_safe({"status": "success", "pages": pages, "count": len(pages), "version": ADVANCED_ANALYSIS_VERSION})

def _schema_spec_payload(page: str) -> Dict[str, Any]:
    headers, keys, spec, schema_source = _resolve_contract(page)
    columns = [{"header": h, "key": k} for h, k in zip(headers, keys)]
    return _json_safe({
        "status": "success",
        "page": page,
        "sheet": page,
        "sheet_name": page,
        "headers": headers,
        "display_headers": headers,
        "sheet_headers": headers,
        "column_headers": headers,
        "keys": keys,
        "fields": keys,
        "columns": columns,
        "meta": {"schema_source": schema_source, "version": ADVANCED_ANALYSIS_VERSION},
    })

@router.get("/schema/sheet-spec")
@router.get("/v1/schema/sheet-spec")
async def schema_sheet_spec_get(
    request: Request,
    page: str = Query(default=""),
    sheet: str = Query(default=""),
    sheet_name: str = Query(default=""),
    name: str = Query(default=""),
    tab: str = Query(default=""),
) -> Dict[str, Any]:
    page_name = _normalize_page_flexible(page or sheet or sheet_name or name or tab or "Market_Leaders")
    _ensure_page_allowed(page_name)
    return _schema_spec_payload(page_name)

@router.post("/schema/sheet-spec")
@router.post("/v1/schema/sheet-spec")
async def schema_sheet_spec_post(body: Dict[str, Any] = Body(default_factory=dict)) -> Dict[str, Any]:
    page_name = _normalize_page_flexible(_pick_page_from_body(body) or "Market_Leaders")
    _ensure_page_allowed(page_name)
    return _schema_spec_payload(page_name)

@router.get("/schema/data-dictionary")
@router.get("/v1/schema/data-dictionary")
async def schema_data_dictionary() -> Dict[str, Any]:
    payload = _schema_spec_payload(_DICTIONARY_PAGE)
    payload["page"] = _DICTIONARY_PAGE
    payload["sheet"] = _DICTIONARY_PAGE
    payload["sheet_name"] = _DICTIONARY_PAGE
    return payload

@router.get("/sheet-rows")
async def root_sheet_rows_get(
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
        "page": page, "sheet": sheet, "sheet_name": sheet_name, "name": name, "tab": tab,
        "symbols": symbols, "tickers": tickers, "direct_symbols": direct_symbols,
        "symbol": symbol, "ticker": ticker, "code": code, "requested_symbol": requested_symbol,
        "limit": limit, "offset": offset, "top_n": top_n,
        "schema_only": schema_only, "headers_only": headers_only,
    }.items():
        if v not in (None, ""):
            body[k] = v
    return await _run_advanced_sheet_rows_impl(request=request, body=body, mode=mode, include_matrix_q=include_matrix_q, token=token, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization, x_request_id=x_request_id)

@router.post("/sheet-rows")
async def root_sheet_rows_post(
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
    return await _run_advanced_sheet_rows_impl(request=request, body=body, mode=mode, include_matrix_q=include_matrix_q, token=token, x_app_token=x_app_token, x_api_key=x_api_key, authorization=authorization, x_request_id=x_request_id)

__all__ = ["router", "ADVANCED_ANALYSIS_VERSION", "_run_advanced_sheet_rows_impl"]
