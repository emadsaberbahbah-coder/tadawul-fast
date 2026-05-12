#!/usr/bin/env python3
# routes/advanced_analysis.py
"""
================================================================================
Advanced Analysis Root Owner — v4.1.0
================================================================================
ROOT SHEET-ROWS OWNER • SCHEMA-FIRST • FAIL-SOFT • STABLE ENVELOPE • JSON-SAFE
GET+POST MERGED • HEADERS-ONLY / SCHEMA-ONLY • CANONICAL WIDTHS • OWNER-ALIGNED
PROVIDER-HEALTH SURFACE • v5.61.0 / v4.8.0 FAMILY ALIGNMENT

WHY v4.1.0 — SURFACE THE ENGINE'S PROVIDER-UNHEALTHY REGISTRY (May 12, 2026)
---------------------------------------------------------------------------
v5.61.0 of `core.data_engine_v2` consumes the new `provider_unhealthy:eodhd`
warning marker from `eodhd_provider v4.8.0`'s circuit breaker and tracks
which providers are currently being demoted/skipped via a module-level
registry. The engine's `health()` now returns a `provider_unhealthy_markers`
snapshot showing which providers are unhealthy and their TTL remaining.

v4.0.0 of this route had no surface for that information — every
response on `/sheet-rows` carried per-row `warnings` strings but no
envelope-level signal that the engine had detected a systemic provider
outage and was routing around it. Operators monitoring `/v1/schema/health`
saw only `adapter_available: true` regardless of whether the upstream
data fetch was healthy or in fallback mode.

v4.0.0 also had a second issue blocking any v5.x engine alignment: the
engine binding cascade tried `core.data_engine` (legacy) BEFORE
`core.data_engine_v2`. Since the legacy module always imports successfully
on this deployment, v2 was never reached — meaning even with v5.61.0
deployed, the route's responses came from the legacy adapter's
unenriched pipeline. v4.1.0 inverts that preference (v2 first, legacy
fallback) and uses runtime-awaitable detection so the adapter works
whether `get_engine` is exported as a sync function (v5.50.0+) or as
an async factory (older/future builds).

v4.1.0 changes (from v4.0.0)
----------------------------
[FIX-A — CRITICAL] Engine binding inversion. v4.0.0 tried
    `from core.data_engine import get_sheet_rows` (legacy) first; the
    legacy module always imports, so the v2 fallback was never reached.
    v4.1.0 cascades through patterns in preferred order:
      1. `core.data_engine_v2.get_engine()` (runtime-await-aware adapter)
      2. `core.data_engine_v2.get_engine_if_ready()` with cold-start fallback
      3. `core.data_engine_v2.get_sheet_rows` (top-level fn — may not exist)
      4. `core.data_engine.get_sheet_rows` (legacy — bug indicator if reached)
    Each step logs explicitly. `CORE_GET_SHEET_ROWS_SOURCE` and the
    response's `meta.source` field reveal which binding won.

[ADD-B] NEW `_get_engine_for_health()` helper. Lazily resolves the
    v2 engine instance for the new `health()` surface. Cached at
    module level after first successful import. Returns None if v2
    is unavailable (graceful degradation — health-snapshot fields
    just omit from the response).

[ADD-C] NEW `_extract_provider_health_snapshot()` helper. Calls
    `engine.health()`, extracts `provider_unhealthy_markers` +
    `engine_version`, returns a JSON-safe dict. Catches all
    exceptions and returns `{"unavailable": True, "error_class": ...}`
    on failure. Safe to call from any request path — total budget
    is whatever `engine.health()` costs (single async call, no API).

[ADD-D] NEW envelope field: `meta.provider_health`. The
    `_payload_envelope` builder accepts an optional `provider_health`
    kwarg and surfaces it under `meta.provider_health`. Shape:
      {
        "unhealthy_markers": {
          "active": [{"provider": "eodhd", "ttl_remaining_sec": 247.3}],
          "active_count": 1,
          "demote_enabled": true,
          "skip_enabled": false,
          "default_ttl_sec": 300.0
        },
        "engine_version": "5.61.0"
      }

[ADD-E] `_run_advanced_sheet_rows_impl` fetches provider_health
    once per request via `_extract_provider_health_snapshot()` and
    passes it into both the success-path envelope (via `_normalize_
    external_payload` meta_extra) AND the fail-soft envelope. So
    every response — success, partial, or error — carries the
    engine's current provider-health view at the moment of the
    request.

[ADD-F] Envelope-level warning lift. When any row in the response
    carries `circuit_open` or `provider_unhealthy:<X>` in its
    `warnings` string, the systemic markers are LIFTED into the
    envelope's top-level `warnings` array (preserving per-row
    warnings intact for the Sheet's Warnings column). One entry
    per distinct marker:
      "EODHD provider circuit open (engine routing to fallback chain)"
    Makes the systemic signal visible at glance without parsing
    every row.

[ADD-G] NEW `/v1/schema/provider-health` endpoint. Dedicated
    read-only GET. Returns the registry snapshot WITHOUT triggering
    a sheet build. Cheap polling target for Apps Script
    `12_Diagnostics.gs runFullDiagnosticSweep_()` — one HTTP call
    instead of one-per-page.

[ADD-H] `/health` and `/v1/schema/health` endpoint enhancement.
    Added: `engine_version` (from v2 engine), `provider_health_
    endpoint_enabled: true`, `provider_unhealthy_count` (the
    engine's `active_count` — 0 healthy, N>0 when providers are
    being demoted/skipped). Operators get a one-call boolean
    indicator of upstream-provider health.

[KEEP] Every v4.0.0 capability preserved:
    - Root paths: /sheet-rows, /schema, /schema/sheet-spec,
      /schema/pages, /schema/data-dictionary + /v1/schema aliases
    - GET+POST merging with query/body param normalization
    - schema_only / headers_only handling in both flows
    - Canonical envelope shape:
      headers/display_headers/sheet_headers/column_headers/keys/
      rows/rows_matrix/row_objects/items/records/data/quotes
    - Schema-safe local fallbacks for Top_10_Investments,
      Insights_Analysis, Data_Dictionary, and instrument pages
    - 80-column canonical contract baseline (engine-side schema
      registry override remains authoritative when available)
    - Auth dispatch cascade preserved (6 attempt shapes)
    - JSON-safe coercion preserved
    - `_call_core_sheet_rows_best_effort` 7-candidate signature
      loop preserved

May 2026 / v2.8.0 family alignment
----------------------------------
This route aligns with:
  - core.data_engine_v2          v5.61.0  (provider-unhealthy registry,
                                            health() surface, PHASE-X)
  - core.providers.eodhd_provider v4.8.0   (circuit breaker, diagnose_health,
                                             get_provider_stats)
  - core.providers.yahoo_fundamentals_provider v6.1.0
  - core.providers.yahoo_chart_provider        v8.2.0
  - core.scoring                  v5.2.5
  - core.scoring_engine           v3.4.2
  - core.reco_normalize           v7.2.0
  - core.insights_builder         v7.0.0
  - core.candlesticks             v1.0.0

New env variables (v4.1.0)
--------------------------
None. v4.1.0's behavior is gated by which engine binding wins (v2 vs
legacy). If v2 is unavailable, the new provider_health fields simply
omit from responses (graceful degradation).

Purpose
-------
Owns the canonical root paths:
- /sheet-rows
- /schema
- /schema/sheet-spec
- /schema/pages
- /schema/data-dictionary
- /v1/schema/provider-health  [v4.1.0 NEW]
and their /v1/schema aliases.
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
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

logger = logging.getLogger("routes.advanced_analysis")
logger.addHandler(logging.NullHandler())

# =============================================================================
# v4.1.0 — Version constant.
# =============================================================================
ADVANCED_ANALYSIS_VERSION = "4.1.0"


# =============================================================================
# v4.1.0 [FIX-A — CRITICAL] Engine binding cascade.
#
# v4.0.0 tried `core.data_engine.get_sheet_rows` (legacy) FIRST and
# only fell back to `core.data_engine_v2` if the legacy import failed.
# Since legacy always imports successfully on this deployment, the v2
# path was never reached and v5.61.0's enrichment (Decision Matrix,
# Insights, Candlesticks, provider_unhealthy registry, ...) never
# made it to the wire.
#
# v4.1.0 inverts that preference. Cascading binding probe in order:
#
#   1. `core.data_engine_v2.get_engine()` — async or sync factory.
#      Runtime-awaitable detection via `inspect.isawaitable()` so the
#      adapter works whether `get_engine` returns the engine instance
#      directly (v5.50.0+ sync) or returns a coroutine (older / future
#      async builds). EXPECTED PRODUCTION PATH.
#
#   2. `core.data_engine_v2.get_engine_if_ready()` — sync ready-check
#      with async-factory cold-start fallback. Belt-and-suspenders
#      against the rare race where the v2 module is imported but the
#      engine instance hasn't warmed yet.
#
#   3. `core.data_engine_v2.get_sheet_rows` — top-level module-level
#      function. Doesn't exist in v5.61.0 (`get_sheet_rows` is a method
#      on DataEngineV5) but we still probe in case a future build
#      adds a convenience wrapper.
#
#   4. `core.data_engine.get_sheet_rows` — legacy adapter. Bug
#      indicator if reached: v2 module is unavailable or broken.
#      Logs WARNING — this branch loses v5.61.0 enrichment.
#
# Every step logs explicitly. The chosen binding is reflected in
# CORE_GET_SHEET_ROWS_SOURCE and surfaces in every response's
# meta.source field, making the live binding trivially observable
# from production.
# =============================================================================
CORE_GET_SHEET_ROWS_SOURCE = "unavailable"
core_get_sheet_rows = None  # type: ignore[assignment]

# Pattern 1: get_engine() factory (sync or async) — EXPECTED PATH.
try:
    from core.data_engine_v2 import get_engine as _v2_get_engine  # type: ignore

    async def _v2_engine_factory_adapter(*args: Any, **kwargs: Any) -> Any:
        """v4.1.0: runtime-await-aware adapter for v2 engine factory.

        `core.data_engine_v2.get_engine` may be sync (v5.50.0+ behavior,
        returns DataEngineV5 instance directly) or async (older or
        future builds, returns a coroutine that resolves to the
        instance). `inspect.isawaitable` distinguishes at runtime.
        """
        raw = _v2_get_engine()
        engine = await raw if inspect.isawaitable(raw) else raw
        return await engine.get_sheet_rows(*args, **kwargs)

    core_get_sheet_rows = _v2_engine_factory_adapter  # type: ignore[assignment]
    CORE_GET_SHEET_ROWS_SOURCE = "core.data_engine_v2.get_engine().get_sheet_rows"
    logger.info(
        "[advanced_analysis v%s] engine bound via core.data_engine_v2.get_engine() (factory pattern, runtime-await-aware)",
        ADVANCED_ANALYSIS_VERSION,
    )
except Exception as _v2_factory_err:
    logger.info(
        "[advanced_analysis v%s] core.data_engine_v2.get_engine() unavailable (%s); trying ready-engine factory",
        ADVANCED_ANALYSIS_VERSION, _v2_factory_err,
    )

# Pattern 2: get_engine_if_ready() sync with async-factory fallback.
if core_get_sheet_rows is None:
    try:
        from core.data_engine_v2 import get_engine_if_ready as _v2_get_engine_if_ready  # type: ignore

        async def _v2_engine_ready_adapter(*args: Any, **kwargs: Any) -> Any:
            """v4.1.0: sync ready-check with cold-start async-factory fallback."""
            engine = _v2_get_engine_if_ready()
            if engine is None:
                try:
                    from core.data_engine_v2 import get_engine as _v2_async_factory  # type: ignore
                    raw = _v2_async_factory()
                    engine = await raw if inspect.isawaitable(raw) else raw
                except Exception as inner_err:
                    raise RuntimeError(
                        "v2 engine not ready and async factory unavailable: " + repr(inner_err)
                    ) from inner_err
            return await engine.get_sheet_rows(*args, **kwargs)

        core_get_sheet_rows = _v2_engine_ready_adapter  # type: ignore[assignment]
        CORE_GET_SHEET_ROWS_SOURCE = "core.data_engine_v2.get_engine_if_ready().get_sheet_rows"
        logger.info(
            "[advanced_analysis v%s] engine bound via core.data_engine_v2.get_engine_if_ready() (sync factory)",
            ADVANCED_ANALYSIS_VERSION,
        )
    except Exception as _v2_ready_err:
        logger.info(
            "[advanced_analysis v%s] core.data_engine_v2.get_engine_if_ready() unavailable (%s); trying top-level fn",
            ADVANCED_ANALYSIS_VERSION, _v2_ready_err,
        )

# Pattern 3: top-level module-level function on v2 (unlikely; future-proofing).
if core_get_sheet_rows is None:
    try:
        from core.data_engine_v2 import get_sheet_rows as core_get_sheet_rows  # type: ignore
        CORE_GET_SHEET_ROWS_SOURCE = "core.data_engine_v2.get_sheet_rows"
        logger.info(
            "[advanced_analysis v%s] engine bound to core.data_engine_v2.get_sheet_rows (top-level fn)",
            ADVANCED_ANALYSIS_VERSION,
        )
    except Exception as _v2_topfn_err:
        logger.info(
            "[advanced_analysis v%s] core.data_engine_v2.get_sheet_rows top-level not present (%s); trying legacy fallback",
            ADVANCED_ANALYSIS_VERSION, _v2_topfn_err,
        )

# Pattern 4: legacy fallback — BUG INDICATOR if reached.
if core_get_sheet_rows is None:
    try:
        from core.data_engine import get_sheet_rows as core_get_sheet_rows  # type: ignore
        CORE_GET_SHEET_ROWS_SOURCE = "core.data_engine.get_sheet_rows"
        logger.warning(
            "[advanced_analysis v%s] all v2 binding patterns failed; falling back to legacy core.data_engine "
            "(this loses v5.61.0 enrichment including provider_unhealthy registry — investigate v2 exports)",
            ADVANCED_ANALYSIS_VERSION,
        )
    except Exception as _legacy_err:
        core_get_sheet_rows = None  # type: ignore
        CORE_GET_SHEET_ROWS_SOURCE = "unavailable"
        logger.error(
            "[advanced_analysis v%s] BOTH v2 and legacy unavailable: legacy_err=%r",
            ADVANCED_ANALYSIS_VERSION, _legacy_err,
        )


# =============================================================================
# v4.1.0 [ADD-B/C] Provider-health snapshot helpers.
#
# These resolve the v2 engine instance directly (bypassing the
# get_sheet_rows adapter) so we can call `engine.health()` and read
# the v5.61.0 `provider_unhealthy_markers` field. Cached at module
# level after first successful resolution.
# =============================================================================
_V2_ENGINE_HEALTH_FACTORY: Optional[Any] = None
_V2_ENGINE_HEALTH_FACTORY_INITED: bool = False


def _resolve_v2_engine_factory() -> Optional[Any]:
    """v4.1.0: One-time resolution of the v2 engine factory callable.

    Caches the result at module level. Returns None if v2 is
    unavailable (graceful degradation — provider_health fields
    simply omit from responses).
    """
    global _V2_ENGINE_HEALTH_FACTORY, _V2_ENGINE_HEALTH_FACTORY_INITED
    if _V2_ENGINE_HEALTH_FACTORY_INITED:
        return _V2_ENGINE_HEALTH_FACTORY
    _V2_ENGINE_HEALTH_FACTORY_INITED = True
    try:
        from core.data_engine_v2 import get_engine as _factory  # type: ignore
        _V2_ENGINE_HEALTH_FACTORY = _factory
        return _factory
    except Exception as e:
        logger.info(
            "[advanced_analysis v%s] v2 engine factory unavailable for health snapshot: %s",
            ADVANCED_ANALYSIS_VERSION, e,
        )
        _V2_ENGINE_HEALTH_FACTORY = None
        return None


async def _get_v2_engine_instance() -> Optional[Any]:
    """v4.1.0: Resolve the v2 engine instance (sync or async factory)."""
    factory = _resolve_v2_engine_factory()
    if factory is None:
        return None
    try:
        raw = factory()
        if inspect.isawaitable(raw):
            return await raw
        return raw
    except Exception as e:
        logger.warning(
            "[advanced_analysis v%s] v2 engine factory call failed: %s: %s",
            ADVANCED_ANALYSIS_VERSION, e.__class__.__name__, e,
        )
        return None


async def _extract_provider_health_snapshot() -> Dict[str, Any]:
    """v4.1.0 [ADD-C]: Fetch the engine's provider-health snapshot.

    Calls `engine.health()` and extracts the v5.61.0
    `provider_unhealthy_markers` field plus `engine_version`.
    Returns a JSON-safe dict suitable for embedding in
    `meta.provider_health` or serving from the dedicated endpoint.

    Failure shapes:
      - v2 unavailable: {"unavailable": True, "reason": "v2_engine_not_bound"}
      - health() raised: {"unavailable": True, "error_class": "...", "error_detail": "..."}
      - health() returned no markers field: {"unavailable": True, "reason": "no_registry_in_health"}

    Never raises. Safe to call from any request path.
    """
    engine = await _get_v2_engine_instance()
    if engine is None:
        return {"unavailable": True, "reason": "v2_engine_not_bound"}

    health_method = getattr(engine, "health", None)
    if not callable(health_method):
        return {"unavailable": True, "reason": "engine_has_no_health_method"}

    try:
        raw = health_method()
        health = await raw if inspect.isawaitable(raw) else raw
    except Exception as e:
        return {
            "unavailable": True,
            "error_class": e.__class__.__name__,
            "error_detail": str(e)[:200],
        }

    if not isinstance(health, dict):
        return {"unavailable": True, "reason": "health_returned_non_dict",
                "type": type(health).__name__}

    markers = health.get("provider_unhealthy_markers")
    if markers is None:
        # Older engine version (pre-v5.61.0) without the registry field.
        return {
            "unavailable": True,
            "reason": "no_registry_in_health",
            "engine_version": _strip(health.get("engine_version")) or None,
        }

    return _json_safe({
        "unhealthy_markers": markers,
        "engine_version": _strip(health.get("engine_version")) or None,
    })


def _lift_systemic_warning_markers(row_objects: Sequence[Mapping[str, Any]]) -> List[str]:
    """v4.1.0 [ADD-F]: Lift systemic provider markers from row warnings
    into envelope-level warnings.

    Scans each row's `warnings` field for the v5.61.0 markers:
      - "circuit_open" → "Provider circuit open (engine routing to fallback chain)"
      - "provider_unhealthy:<X>" → "Upstream provider unhealthy: <X> (engine routing around it)"
      - "HTTP 403" → "Provider access issue (HTTP 403)"

    Deduplicated. Per-row warnings are NOT modified — only lifted.
    Empty list if no markers found.
    """
    out: List[str] = []
    seen_circuits = False
    seen_unhealthy: Set[str] = set()
    seen_http403 = False

    for row in row_objects or []:
        if not isinstance(row, Mapping):
            continue
        raw = row.get("warnings")
        if raw is None:
            continue
        text = _strip(raw).lower() if not isinstance(raw, (list, tuple, set)) else "; ".join(
            _strip(w).lower() for w in raw if _strip(w)
        )
        if not text:
            continue
        if "circuit_open" in text and not seen_circuits:
            out.append("Provider circuit open (engine routing to fallback chain)")
            seen_circuits = True
        # Extract provider_unhealthy:<name> entries
        for match in re.finditer(r"provider_unhealthy:([a-zA-Z0-9_\-]+)", text):
            name = match.group(1)
            if name and name not in seen_unhealthy:
                seen_unhealthy.add(name)
                out.append("Upstream provider unhealthy: {} (engine routing around it)".format(name))
        if "http 403" in text and not seen_http403:
            out.append("Provider access issue (HTTP 403)")
            seen_http403 = True

    return out


router = APIRouter(tags=["schema", "root-sheet-rows"])

_TOP10_PAGE = "Top_10_Investments"
_INSIGHTS_PAGE = "Insights_Analysis"
_DICTIONARY_PAGE = "Data_Dictionary"
_SPECIAL_PAGES = {_TOP10_PAGE, _INSIGHTS_PAGE, _DICTIONARY_PAGE}

_EXPECTED_SHEET_LENGTHS: Dict[str, int] = {
    "Market_Leaders": 80,
    "Global_Markets": 80,
    "Commodities_FX": 80,
    "Mutual_Funds": 80,
    "My_Portfolio": 80,
    "My_Investments": 80,
    _TOP10_PAGE: 83,
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
        normalize_page_name,
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

# (v4.1.0: the original legacy-first binding block was REMOVED here;
# the v2-first cascade now lives at module top after the imports.)

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
            value = normalize_page_name(raw, **kwargs)  # type: ignore[misc]
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
            n = int(get_sheet_len(page))  # type: ignore[misc]
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
            headers = [_strip(x) for x in get_sheet_headers(page) if _strip(x)]  # type: ignore[misc]
            keys = [_strip(x) for x in get_sheet_keys(page) if _strip(x)]  # type: ignore[misc]
            if headers and keys:
                if callable(get_sheet_spec):
                    try:
                        spec = get_sheet_spec(page)  # type: ignore[misc]
                    except Exception:
                        spec = None
                return _complete_schema_contract(headers, keys)[0], _complete_schema_contract(headers, keys)[1], spec, "schema_registry.helpers"
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
    kk = _normalize_key_name(key)
    if kk in {"symbol", "ticker"}:
        return symbol
    if kk == "name":
        return f"{page} {symbol}"
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
    if kk == "recommendation":
        return "Watch" if row_index > 3 else "Accumulate"
    if kk == "recommendation_reason":
        return "Placeholder fallback because live engine returned no usable rows."
    if kk in {"top10_rank", "rank_overall"}:
        return row_index
    if kk == "selection_reason":
        return "Placeholder fallback because upstream builders returned no usable rows."
    if kk == "criteria_snapshot":
        return json.dumps({"symbol": symbol, "row_index": row_index, "source": "placeholder"}, ensure_ascii=False)
    if kk in {"warnings", "notes"}:
        return "placeholder"
    if kk in {"current_price", "previous_close", "open_price", "day_high", "day_low", "forecast_price_1m", "forecast_price_3m", "forecast_price_12m", "avg_cost", "position_cost", "position_value", "unrealized_pl", "intrinsic_value"}:
        base = 100.0 + float(row_index)
        return round(base, 2)
    if kk in {"percent_change", "expected_roi_1m", "expected_roi_3m", "expected_roi_12m", "forecast_confidence", "confidence_score", "overall_score", "opportunity_score"}:
        return round(max(1.0, 100.0 - float(row_index * 3)), 2)
    if kk in {"risk_bucket", "confidence_bucket"}:
        return "Moderate" if row_index > 3 else "High Confidence"
    if kk == "invest_period_label":
        return "3M"
    if kk == "horizon_days":
        return 90
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
            "source": "advanced_analysis.local_dictionary_fallback",
            "notes": f"Auto-generated fallback row {idx} from schema contract",
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
        return _build_dictionary_fallback_rows(page=page, headers=headers, keys=keys, limit=limit, offset=offset)
    if page == _INSIGHTS_PAGE:
        return _build_insights_fallback_rows(requested_symbols=requested_symbols, limit=limit, offset=offset)
    if page == _TOP10_PAGE:
        rows = _build_placeholder_rows(page=page, keys=keys, requested_symbols=requested_symbols or EMERGENCY_PAGE_SYMBOLS.get(page, []), limit=max(limit, top_n), offset=0)
        rows = _ensure_top10_rows(rows, requested_symbols=requested_symbols, top_n=top_n, schema_keys=keys, schema_headers=headers)
        return _slice(rows, limit=limit, offset=offset)
    return _build_placeholder_rows(page=page, keys=keys, requested_symbols=requested_symbols or EMERGENCY_PAGE_SYMBOLS.get(page, []), limit=limit, offset=offset)

def _payload_envelope(
    *,
    page: str,
    headers: Sequence[str],
    keys: Sequence[str],
    row_objects: Sequence[Mapping[str, Any]],
    include_matrix: bool,
    request_id: str,
    started_at: float,
    mode: str,
    status_out: str,
    error_out: Optional[str],
    meta: Optional[Dict[str, Any]] = None,
    provider_health: Optional[Mapping[str, Any]] = None,
    lifted_warnings: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Build the canonical sheet-rows envelope.

    v4.1.0 additions:
      - `provider_health`: optional engine `health()` snapshot. Surfaced
        under `meta.provider_health`. Pass the dict from
        `_extract_provider_health_snapshot()`.
      - `lifted_warnings`: optional list of envelope-level warnings
        lifted from row warnings via `_lift_systemic_warning_markers`.
        Surfaced under the top-level `warnings` key (Apps Script
        12_Diagnostics.gs can read this without scanning every row).
    """
    hdrs = list(headers or [])
    ks = list(keys or [])
    rows_dict = [{str(k): _json_safe(dict(r).get(k)) for k in ks} for r in (row_objects or [])]
    matrix = _rows_to_matrix(rows_dict, ks) if include_matrix else []

    # Compose final meta with provider_health attached.
    final_meta: Dict[str, Any] = {
        "duration_ms": round((time.time() - started_at) * 1000.0, 3),
        "mode": mode,
        "count": len(rows_dict),
        "dispatch": "advanced_analysis_root",
        "source": CORE_GET_SHEET_ROWS_SOURCE,
    }
    if meta:
        final_meta.update(meta)
    if provider_health is not None:
        final_meta["provider_health"] = _json_safe(provider_health)

    envelope: Dict[str, Any] = {
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
        "meta": final_meta,
    }
    if lifted_warnings:
        envelope["warnings"] = list(lifted_warnings)
    return _json_safe(envelope)

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
    provider_health: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Normalize an engine payload into the canonical envelope.

    v4.1.0: forwards `provider_health` snapshot into the envelope so the
    success path carries the same `meta.provider_health` field as the
    fail-soft path. Also lifts systemic markers from row warnings into
    envelope-level `warnings`.
    """
    ext = dict(external_payload or {})
    hdrs = list(headers or [])
    ks = list(keys or [])
    rows = _extract_rows_like(ext)
    normalized_rows = [_normalize_to_schema_keys(schema_keys=ks, schema_headers=hdrs, raw=(r or {})) for r in rows]
    if page == _TOP10_PAGE:
        normalized_rows = _ensure_top10_rows(normalized_rows, requested_symbols=requested_symbols or [], top_n=top_n, schema_keys=ks, schema_headers=hdrs)
    normalized_rows = _slice(normalized_rows, limit=limit, offset=offset)
    status_out, error_out, ext_meta = _extract_status_error(ext)
    if not normalized_rows:
        status_out = "partial"
        error_out = error_out or "No usable rows returned"
    final_meta = dict(ext_meta or {})
    if meta_extra:
        final_meta.update(meta_extra)
    # v4.1.0 [ADD-F]: lift systemic markers from row warnings to envelope level.
    lifted = _lift_systemic_warning_markers(normalized_rows)
    return _payload_envelope(
        page=page,
        headers=hdrs,
        keys=ks,
        row_objects=normalized_rows,
        include_matrix=include_matrix,
        request_id=request_id,
        started_at=started_at,
        mode=mode,
        status_out=status_out or ("success" if normalized_rows else "partial"),
        error_out=error_out,
        meta=final_meta,
        provider_health=provider_health,
        lifted_warnings=lifted or None,
    )

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
        # v4.1.0: schema-only path does NOT fetch provider_health
        # (no live data being served — keep it cheap).
        return _payload_envelope(
            page=page, headers=headers, keys=keys, row_objects=[],
            include_matrix=include_matrix, request_id=request_id, started_at=start,
            mode=mode, status_out="success", error_out=None,
            meta={"dispatch": "schema_only", "schema_source": schema_source,
                  "headers_only": headers_only, "schema_only": schema_only},
        )

    # v4.1.0 [ADD-E]: fetch provider_health snapshot ONCE per request.
    # Total budget = single async call to engine.health() (no upstream API).
    # Forwarded into both success and fail-soft envelopes so every response
    # carries the engine's current view of provider health at request time.
    provider_health = await _extract_provider_health_snapshot()

    payload, source = await _call_core_sheet_rows_best_effort(
        page=page, limit=max(limit + offset, top_n), offset=0,
        mode=mode or "", body=merged_body,
    )
    if isinstance(payload, dict):
        normalized = _normalize_external_payload(
            external_payload=payload, page=page, headers=headers, keys=keys,
            include_matrix=include_matrix, request_id=request_id, started_at=start,
            mode=mode, limit=limit, offset=offset, top_n=top_n,
            requested_symbols=requested_symbols,
            meta_extra={"schema_source": schema_source,
                        "source": source or CORE_GET_SHEET_ROWS_SOURCE},
            provider_health=provider_health,
        )
        if _extract_rows_like(normalized):
            return normalized

    # Fail-soft path: build placeholder rows, but still surface provider_health.
    fallback_rows = _build_nonempty_failsoft_rows(
        page=page, headers=headers, keys=keys,
        requested_symbols=requested_symbols, limit=limit, offset=offset, top_n=top_n,
    )
    fallback_status = "partial" if fallback_rows else "error"
    fallback_error = (
        "Local non-empty fallback emitted after upstream degradation"
        if fallback_rows else "No usable rows returned; schema-shaped fallback emitted"
    )
    # v4.1.0: even fail-soft rows may carry warnings worth lifting (e.g. the
    # placeholder builder marks rows with "Placeholder fallback").
    lifted = _lift_systemic_warning_markers(fallback_rows)
    return _payload_envelope(
        page=page, headers=headers, keys=keys, row_objects=fallback_rows,
        include_matrix=include_matrix, request_id=request_id, started_at=start,
        mode=mode, status_out=fallback_status, error_out=fallback_error,
        meta={
            "dispatch": "advanced_analysis_fail_soft_nonempty" if fallback_rows else "advanced_analysis_fail_soft",
            "schema_source": schema_source,
            "source": source or CORE_GET_SHEET_ROWS_SOURCE,
        },
        provider_health=provider_health,
        lifted_warnings=lifted or None,
    )

@router.get("/health")
@router.get("/v1/schema/health")
async def advanced_analysis_health(request: Request) -> Dict[str, Any]:
    """v4.1.0: enhanced with engine_version + provider_health summary.

    Operators monitoring this endpoint can now see at a glance whether
    the upstream-provider chain is healthy:
      - provider_unhealthy_count == 0 → all providers healthy
      - provider_unhealthy_count > 0  → engine is demoting/skipping
        at least one provider; check /v1/schema/provider-health for detail.

    Failures inside provider_health resolution are caught and surfaced as
    `provider_health_error`; the health endpoint itself never raises.
    """
    # v4.1.0: cheap one-shot snapshot. Bounded by engine.health() cost.
    provider_health_summary: Dict[str, Any] = {}
    engine_version: Optional[str] = None
    provider_unhealthy_count: Optional[int] = None
    try:
        snap = await _extract_provider_health_snapshot()
        if isinstance(snap, dict):
            engine_version = snap.get("engine_version")
            markers = snap.get("unhealthy_markers")
            if isinstance(markers, dict):
                ac = markers.get("active_count")
                if isinstance(ac, int):
                    provider_unhealthy_count = ac
            if snap.get("unavailable"):
                provider_health_summary = {
                    "available": False,
                    "reason": snap.get("reason") or snap.get("error_class") or "unknown",
                }
            else:
                provider_health_summary = {"available": True}
    except Exception as e:
        provider_health_summary = {
            "available": False,
            "reason": "{}:{}".format(e.__class__.__name__, str(e)[:120]),
        }

    return _json_safe({
        "status": "ok",
        "service": "advanced_analysis",
        "version": ADVANCED_ANALYSIS_VERSION,
        "schema_registry_available": bool(get_sheet_spec is not None),
        "adapter_available": bool(core_get_sheet_rows is not None),
        "engine_source": CORE_GET_SHEET_ROWS_SOURCE,
        "engine_version": engine_version,
        "provider_health_endpoint_enabled": True,
        "provider_health_summary": provider_health_summary,
        "provider_unhealthy_count": provider_unhealthy_count,
        "allowed_pages_count": len(_safe_allowed_pages()),
        "path": str(getattr(getattr(request, "url", None), "path", "")),
    })


@router.get("/schema/provider-health")
@router.get("/v1/schema/provider-health")
async def schema_provider_health(request: Request) -> Dict[str, Any]:
    """v4.1.0 [ADD-G]: Dedicated provider-health snapshot endpoint.

    Returns the engine's `provider_unhealthy_markers` registry without
    triggering a sheet build. Cheap polling target for Apps Script
    `12_Diagnostics.gs runFullDiagnosticSweep_()` — one HTTP call
    instead of one-per-page to discover systemic provider outages.

    Response shape:
      {
        "status": "success" | "unavailable",
        "version": "4.1.0",
        "engine_version": "5.61.0",
        "provider_health": {
          "unhealthy_markers": {
            "active": [{"provider": "eodhd", "ttl_remaining_sec": 247.3}],
            "active_count": 1,
            "demote_enabled": true,
            "skip_enabled": false,
            "default_ttl_sec": 300.0
          },
          "engine_version": "5.61.0"
        },
        "timestamp_utc": "2026-05-12T10:24:31.482Z",
        "request_id": "...",
        "meta": { "source": "core.data_engine_v2.get_engine().health" }
      }

    On v2-engine-unavailable / no-registry-in-health, returns
    `status: "unavailable"` with a `reason` field instead of raising.
    Always returns 200; consumers should inspect the `status` field
    rather than relying on HTTP codes.
    """
    request_id = str(uuid.uuid4())[:12]
    started = time.time()

    snap = await _extract_provider_health_snapshot()
    unavailable = bool(snap.get("unavailable")) if isinstance(snap, dict) else True

    return _json_safe({
        "status": "unavailable" if unavailable else "success",
        "version": ADVANCED_ANALYSIS_VERSION,
        "engine_version": snap.get("engine_version") if isinstance(snap, dict) else None,
        "engine_source": CORE_GET_SHEET_ROWS_SOURCE,
        "provider_health": snap,
        "timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "request_id": request_id,
        "meta": {
            "duration_ms": round((time.time() - started) * 1000.0, 3),
            "dispatch": "schema_provider_health",
            "source": "core.data_engine_v2.get_engine().health",
            "path": str(getattr(getattr(request, "url", None), "path", "")),
        },
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
