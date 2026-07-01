#!/usr/bin/env python3
# routes/advanced_analysis.py
"""
================================================================================
Advanced Analysis Root Owner — v4.11.0  (EDGE-TIMEOUT-GUARD)
================================================================================
v4.11.0 [EDGE-TIMEOUT-GUARD]: WHY -- diagnosed from the 2026-07-01 sync +
Render logs: Market_Leaders/Global_Markets/Commodities_FX/Mutual_Funds batches
were sometimes coming back as a raw connection failure (502) from the SYNC's
point of view, even though this route already has a complete fail-soft design
(_build_nonempty_failsoft_rows, existing) meant to ALWAYS return a non-empty,
schema-shaped "partial" envelope on any upstream failure. Root cause:
_call_core_sheet_rows_best_effort had NO internal time budget -- for a large
or Yahoo-throttled batch (429 retries add up across hundreds of symbols) the
engine call can run past Render's platform-level edge timeout (~100s). When
the PLATFORM kills the request at the edge, this handler's fail-soft code
never gets to execute -- the caller sees a bare connection failure, not the
graceful envelope this route was designed to always return. FIX: the engine
call is now wrapped in asyncio.wait_for with a budget safely under the edge
timeout; on TimeoutError it falls through to the SAME existing fail-soft path
used for every other failure mode (no new fail-soft logic -- the existing
logic just gets a chance to run before the platform can kill the request).
ENV-GATED, DEFAULT OFF (matches this file's convention): set
TFB_ADV_ENGINE_CALL_TIMEOUT=1 to enable; TFB_ADV_ENGINE_CALL_TIMEOUT_S (default
75.0) sets the budget in seconds. Unset -> byte-identical to v4.10.0.
COMPLEMENTARY to (not a substitute for) run_dashboard_sync.py v6.17.0's
TFB_SYNC_SYMBOL_BATCH_SIZE, which reduces the odds of hitting this ceiling at
all by shrinking each request's symbol count -- this guard is defense-in-depth
for whatever size request the caller actually sends. ALL v4.10.0 behavior is
otherwise preserved verbatim; no route, schema, or fail-soft-content change.

================================================================================
Advanced Analysis Root Owner — v4.10.0  (LIVE NEWS DISPLAY — NEWS-DISPLAY)
================================================================================
v4.10.0 [NEWS-DISPLAY]: the opportunity-candidates News column is filled with
LIVE per-symbol sentiment from core.news_intelligence for the SURFACED decision
symbols (selected + near_miss + the top candidates_rows, capped). DISPLAY-ONLY
BY CONSTRUCTION: the builder runs FIRST and its scoring / gating / selection are
finalized with news_trend='Unknown' EXACTLY as in v4.9.0 -> selection is
byte-identical to NEWS_DISPLAY off. AFTER the build, _attach_news_display()
overwrites only the displayed `news_trend` label (and adds news_sentiment /
news_confidence / news_articles) on the OUTPUT rows whose symbol was fetched.
No input row is mutated, so the News GATE (Negative -> fail) and the §4.3
news_trend score NEVER fire on live data -- that news->action coupling is a
registered hypothesis that must clear a backtest (the News_Archive collector)
before it may influence a recommendation; until then news is informational.
ENV-GATED, DEFAULT OFF: TFB_NEWS_DISPLAY=1 to enable (unset -> no fetch, no
mutation, byte-identical to v4.9.0). Bounded by TFB_NEWS_DISPLAY_MAX (default
60 symbols) and time-boxed by TFB_NEWS_DISPLAY_TIMEOUT_S (default 25.0s) to
protect the Render edge timeout; label thresholds TFB_NEWS_DISPLAY_POS (+0.15)
/ TFB_NEWS_DISPLAY_NEG (-0.15). Fail-soft: news_intelligence unbound, a fetch
error, or a timeout -> rows stay Unknown, reason noted in
meta.route.news_display. opportunity_builder is UNCHANGED (it already emits
news_trend). ALL v4.9.0 behavior is otherwise preserved verbatim.

v4.9.0 [TREND-ADD]: the opportunity-candidates pool is passed through
core.analysis.trend_signals v1.0.0 (Plan v5.0 P9) BEFORE the builder runs.
The module computes SECTOR TREND cross-sectionally from the pool itself
(median Expected ROI 12M + sell-recommendation breadth per sector cohort —
no external calls, no provider quota) and stamps 'Sector Trend' onto rows
that do not already carry one, so the §4.2 Allow Negative Sector gate and
the §4.3 sector_trend score component finally receive real signal instead
of a universal "Unknown" (40, always passes). News trend is NOT fabricated —
it stays Unknown until a news source lands; the enrichment meta reports
news coverage honestly. ENV-GATED, DEFAULT OFF: set TFB_TREND_SIGNALS=1 to
enable; when unset, rows pass through byte-identical to v4.8.0 and
meta.route.trends reports {"enabled": false}. Enrichment summary (stamped /
supplied / no_sector counts + per-sector classifications) is exposed at
meta.route.trends on the opportunity endpoint. Fail-soft: module unbound or
raising -> pool used unenriched, error noted in meta.route.trends. The
portfolio-actions path is untouched (portfolio_actions does not consume
trend fields). ALL v4.8.0 behavior is otherwise preserved verbatim.

v4.8.0 [PF-ADD]: NEW endpoint POST /portfolio-actions (Plan v5.0 P5b,
milestone M2) — bridges GAS-supplied My_Portfolio holdings rows into
core.analysis.portfolio_actions v1.0.0 (P5): L3 action vocabulary
(ADD/HOLD/TRIM/EXIT/BLOCK), v1.0.0-pinned action truth table, L7 funding
identity (mode-aware via PF: Rebalance Mode), L8 advisor sentences, sector
summary, and alerts. Registered on THREE paths, mirroring the v4.7.1
mount-fix: /sheet-rows/portfolio-actions is the EFFECTIVE live path (main.py
entry v8.11.2 _clone_filtered_router admits only the "/sheet-rows", "/schema",
"/v1/schema" prefixes for this router); bare /portfolio-actions and
/v1/portfolio-actions are registered for the future main allowlist. Body:
`rows` (holdings dicts with the Top_10 pool display headers PLUS Quantity and
Buy Price), `controls` (PF panel labels "PF: Cash Available SAR" etc. or
snake_case, mapped via _PF_CONTROLS_ALIASES and echoed in
meta.controls_snapshot), `fx_rates` (GAS-read TFB_FX_LOOKUP). There is NO
selector pool path — holdings come exclusively from the spreadsheet (the
backend never owns portfolio truth). Fail-soft: builder unbound -> status
"unavailable" skeleton with the §-zone keys (kpis/actions/sector_summary/
alerts); builder exceptions -> status "error"; the only raise on the new path
is auth 401. ALL v4.7.2 behavior — every endpoint, the dispatch chain,
envelopes, widths, the opportunity-candidates path — is preserved verbatim;
v4.8.0 is purely additive.

v4.7.2 [META-MAP]: upstream_meta mapping aligned to the LIVE top10_selector
v4.19.0 meta key names observed on 2026-06-12 acceptance: the selector emits
page_coverage (not coverage), a boolean budget_exhausted (not a budget dict),
and universe_starved. v4.7.1 looked only for coverage/budget/budget_meta, so
every forwarded field arrived as None and the opportunity builder's
budget_exhausted alert (Plan §4.2/§5) could never fire from the live
selector. Mapping is fallback-chained (new keys first, old names kept) and a
universe_starved flag is forwarded inside budget so the builder surfaces
partial coverage honestly. No other change from v4.7.1.

v4.7.1 [MOUNT-FIX]: registered POST /sheet-rows/opportunity-candidates as the
EFFECTIVE path. Root cause of the v4.7.0 live 404: main.py (entry v8.11.2)
mounts every router through a controlled clone-filter
(_clone_filtered_router / _allowed_prefixes_for_key) that admits only paths
under this module's allowlisted prefixes — ("/v1/schema", "/schema",
"/sheet-rows") — so the bare /opportunity-candidates and
/v1/opportunity-candidates registrations were silently filtered at mount
(module imported and bound fine; the app simply never exposed the paths).
The /sheet-rows-prefixed alias passes the existing filter with ZERO main.py
changes. The two canonical paths are KEPT on the endpoint so they go live
automatically if/when main's allowlist adds "/opportunity-candidates"; until
then GAS (16_Decision_Top10.gs) must call /sheet-rows/opportunity-candidates.
No other change from v4.7.0.

v4.7.0 [OPP-ADD]: NEW endpoint POST /opportunity-candidates (+ /v1 alias) —
Plan v5.0 Phase P3. Serves the FROZEN §5 zone payload for the rebuilt
Top_10_Investments decision page by bridging the selector candidate pool into
core.analysis.opportunity_builder v1.0.1 (hard gates §4.2, opportunity score
§4.3, wealth math §4.4, L7 funding identity). The request body carries the GAS
control-panel values (`criteria` — panel labels or snake_case, mapped via
_OPP_CRITERIA_ALIASES and echoed back in meta.criteria_snapshot), `fx_rates`
(read by GAS from _Lists_Config TFB_FX_LOOKUP — the backend never reads the
spreadsheet), `portfolio` context (cash / pending proceeds / holdings),
optional explicit `rows` (bypasses the selector — used by tests and replays),
and `pool_limit`. Candidate pool: _build_top10_rows(limit=pool_limit, mode=…)
via _maybe_await (same v4.5.0 binding + coroutine handling); selector meta
(coverage / budget / timeouts / freshness) and versions are forwarded as
upstream_meta so budget_exhausted surfaces as a Top_10 alert. Fail-soft
end-to-end: builder unbound -> status "unavailable"; selector empty/unbound
with no body rows -> the builder's honest "no_candidates" skeleton; nothing in
the new path raises except auth 401. ALL v4.6.0 behavior — every endpoint, the
dispatch chain, envelopes, widths — is preserved verbatim; v4.7.0 is purely
additive.

v4.6.0 [ORDER-FIX]: the selector_real Top_10 path no longer re-sorts the
selector's rows. _ensure_top10_rows gained preserve_order (default False);
_normalize_external_payload threads top10_preserve_order, set True ONLY on the
v4.5.0 special-page (selector_real) dispatch. Root cause of the live 2026-06-10
audit finding "GC=F (HOLD) ranked #2 above ACCUMULATE names": the route's
unconditional overall_score-first re-sort overwrote the selector's ranking
(selector score, priority band, horizon ROI, direct-symbol order, backfill-last
from top10_selector v4.15.0/v4.16.1) and re-stamped top10_rank. In preserve
mode the route now only dedupes in selector order, slices, FILLS a blank rank,
and fills blank selection_reason / criteria_snapshot. The engine and fail-soft
Top_10 paths (no selector ordering to preserve) are byte-identical to v4.5.0.

v4.5.0 [BRIDGE-FIX]: Top_10_Investments and Insights_Analysis now dispatch to
their REAL builders on the happy path — mirroring the v4.4.0 Data_Dictionary
pattern. Root cause of the "Top_10 shows the engine's 1 placeholder row /
Insights shows the local fallback" symptom: the live engine.get_sheet_rows
returns only a 1-row envelope for these DERIVED pages, which was non-empty and
therefore pre-empted the real builders — so core.analysis.top10_selector
(v4.15.0) and core.analysis.insights_builder (v8.2.0) were never invoked by the
live route. v4.5.0 binds both builders at import (fail-soft, exactly like
_build_dd_payload) and calls them BEFORE the generic engine path:
  - Top_10_Investments  -> build_top10_rows(...)            -> dispatch=top10_selector_real
  - Insights_Analysis   -> build_insights_analysis_rows(...) -> dispatch=insights_builder_real
Both are awaited via _maybe_await (build_top10_rows returns a coroutine inside
the running event loop — the exact bug that left it un-awaited), normalized
through _normalize_external_payload, and used only when they yield usable rows;
any unbound/empty/raising builder falls through to the engine path and then to
the existing non-empty fallback, so this can ONLY help, never regress. Data_
Dictionary is unchanged. Insights' happy path additionally requires
insights_builder v8.2.0 to be deployed; until then it falls through fail-soft.
================================================================================
Advanced Analysis Root Owner — v4.4.0  (DATA_DICTIONARY CONTENT ROWS — DD-FIX)
================================================================================
ROOT SHEET-ROWS OWNER • SCHEMA-FIRST • FAIL-SOFT • STABLE ENVELOPE • JSON-SAFE
GET+POST MERGED • HEADERS-ONLY / SCHEMA-ONLY • REGISTRY-DERIVED WIDTHS •
OWNER-ALIGNED • PROVIDER-HEALTH SURFACE • NO-HARDCODED-CLAMP • v5.79.2 GATE-AWARE

(The verbose pre-v4.3.0 cross-version changelog is condensed in this working
copy; ALL code, the engine-binding cascade, the provider-health machinery, and
every endpoint are preserved verbatim except where the v4.4.0 / v4.3.0 changelog
below states otherwise.)

WHY v4.4.0 — Data_Dictionary returned "200 success / 0 rows" (DD-FIX)
---------------------------------------------------------------------

Symptom: the Data_Dictionary page rendered as an empty sheet — the frontend
received HTTP 200 with status "success" but zero content rows. Root cause was
NOT data: it was routing. Two code paths feed Data_Dictionary, and the one the
frontend hit served schema only.

  - `/schema/data-dictionary` (endpoint `schema_data_dictionary`) called
    `_schema_spec_payload(_DICTIONARY_PAGE)`, which returns ONLY the 9-column
    schema (headers / keys / columns) and NEVER invokes any row builder. So it
    returned exactly 0 rows BY DESIGN — matching the "exactly 0" symptom (the
    `/sheet-rows` fail-soft path always emits >= 1 row, so exactly-0 pointed
    here).
  - The route's prior fail-soft `_build_dictionary_fallback_rows` produced a
    DEGENERATE 9-row self-description (one row per the dictionary's OWN columns),
    not the real per-column content across all sheets.

The REAL Data_Dictionary content builder is
`core.sheets.data_dictionary.build_data_dictionary_payload(format="rows")`
(v3.2.0+). It is pure / import-safe — registry-only, no engine, no I/O, no
network — and emits one row per schema column across every sheet, with keys
already equal to this route's _DICTIONARY_KEYS
(sheet, group, header, key, dtype, fmt, required, source, notes).

v4.4.0 routes BOTH Data_Dictionary paths to that real builder, additively and
fail-soft (works even when the engine has not reached the container, because
the builder is pure):

[DD-FIX (a)] Bind the real builder once at import:
    `from core.sheets.data_dictionary import build_data_dictionary_payload as
     _build_dd_payload` (fail-soft -> None on ImportError).
[DD-FIX (b)] New helper `_real_data_dictionary_rows()` -> list[dict] from
    `build_data_dictionary_payload(format="rows")` (reads row_objects, then
    rows; fail-soft -> []).
[DD-FIX (c)] `_build_dictionary_fallback_rows` now PREFERS the real rows
    (normalized to the schema keys + sliced); the prior degenerate
    self-description is kept ONLY as a last resort when the builder is
    unavailable. This fixes `/sheet-rows?page=Data_Dictionary` (non-schema-only).
[DD-FIX (d)] `schema_data_dictionary` now attaches the real content rows
    (row_objects/items/records/data/quotes + rows/rows_matrix/matrix + count)
    on top of the schema-spec payload, fail-soft to the prior schema-only
    payload. This fixes `/schema/data-dictionary` and its /v1 alias.

NO BEHAVIOR CHANGE elsewhere: every other endpoint, the engine-binding cascade,
the provider-health surface, and the 115-key contract path are preserved
verbatim from v4.3.0. The instrument/Top_10/Insights static fallbacks and all
widths are unchanged. v4.4.0 is purely additive on the Data_Dictionary paths.

NOTE ON THE ONE INFERENCE: the exact frontend caller for Data_Dictionary lives
in `11_SpecialPages.gs` (not on disk this session). v4.4.0 fixes BOTH plausible
callers — the schema-only endpoint AND the /sheet-rows page path — so the page
populates regardless of which one the frontend uses.

WHY v4.3.0 — bring the OFFLINE fallback up to the live 115-key gate contract
----------------------------------------------------------------------------

Cascade #3 (core.sheets.schema_registry -> v2.13.0) has LANDED: the registry
now names the full 115-key instrument canonical (Top_10 = 118), including the
v5.78.0 investability gate — forecast_source + 8 gate columns
(data_quality_score, forecast_reliability_score, provider_engine_conflict,
conflict_type, final_decision_basis, investability_status, final_action,
block_reason). On the LIVE path this route already serves all of them, because
v4.2.0 made every width registry-derived (`_expected_len` prefers
`get_sheet_len`, `_resolve_contract` projects onto the registry's keys with no
re-clamp). Nothing on the live path needed changing.

The remaining gap was the route's STATIC FALLBACK. v4.2.0's static
`_CANONICAL_80_*` was the 90-column baseline (it pre-dated forecast_source and
the gate). That list is consumed ONLY when the registry import fails entirely —
but on this deployment the registry-import-failure path is NOT hypothetical
(the recurring "code didn't reach the container" issue). In that degraded mode
v4.2.0 would silently emit a 90-column contract and drop the gate before the
sheet ever saw it. v4.3.0 closes that: the static fallback is now the SAME
115-key canonical the live registry serves, in EXACT engine/registry order, so
the offline contract no longer regresses below the gate.

v4.3.0 changes (from v4.2.0)
----------------------------
[FIX-A] Static instrument canonical widened 90 -> 115 and REORDERED to match
    data_engine_v2 v5.79.2 INSTRUMENT_CANONICAL_KEYS / schema_registry v2.13.0
    exactly. This adds, after the prior 90: the Decision pair, Provider Rating,
    the Canonical-Reco pair, the remaining Scoring-v5.74 fields, the Candlestick
    block, forecast_source, and the 8-column Investability Gate. It also FIXES a
    pre-existing fallback-only ordering slip in v4.2.0 (opportunity_score /
    rank_overall now sit BEFORE the Views block, matching the engine's Scores
    group). Verified: 115 keys, no dup keys/headers, gate at positions 108-115,
    forecast_source at 107; Top_10 derives to 118.
[FIX-B] The misleading `_CANONICAL_80_*` names are renamed to
    `_CANONICAL_INSTRUMENT_HEADERS` / `_CANONICAL_INSTRUMENT_KEYS` (the literal
    "80" was already doubly-stale at 90). All references updated. The derived
    width block (`_CANONICAL_INSTRUMENT_LEN = len(...)`, `_TOP10_STATIC_LEN`)
    now auto-resolves to 115 / 118 with no further edits.
[FIX-C] Insights static fallback reconciled to the engine/registry contract.
    schema_registry v2.12.0 reconciled Insights_Analysis from
    [Section, Item, Symbol, Metric, Value, Notes, Last Updated (Riyadh)] to
    [Section, Item, Metric, Value, Notes, Source, Sort Order]. v4.2.0's static
    `_INSIGHTS_*` still carried the OLD keys (symbol / last_updated_riyadh).
    v4.3.0 adopts the engine keys (section, item, metric, value, notes, source,
    sort_order; count stays 7) AND updates `_build_insights_fallback_rows` to
    emit them, so the offline Insights fallback aligns with the live contract.

NO BEHAVIOR CHANGE on the live path. Width on every page is still taken from
the registry first (`_expected_len` -> `get_sheet_len`); the rows are still
projected onto the registry's keys with no re-clamp. v4.3.0 only changes what
the route emits when the registry import is UNAVAILABLE — it now emits the full
115-key gate contract instead of a 90-key subset. The Data_Dictionary static
contract was already correct (9 cols) and is unchanged.

NOTE ON VISIBILITY: unchanged from v4.2.0 — this route surfaces whatever the
live registry names. With the registry now at v2.13.0 (115/118), the gate
columns flow end-to-end on the live path. The static fallback only matters when
the registry import fails; v4.3.0 makes that fallback match too.

May/Jun 2026 / family alignment
-------------------------------
This route aligns with:
  - core.data_engine_v2          v5.79.2  (115-key canonical, v5.78.0 gate;
                                            provider-unhealthy registry, health())
  - core.sheets.schema_registry  v2.13.0  (115/118 LIVE — gate columns named)
  - core.providers.eodhd_provider v4.8.0   (circuit breaker, diagnose_health)
  - core.scoring                  v5.7.1   (8-tier vocabulary incl. AVOID)
  - core.reco_normalize           v8.0.0
  - core.insights_builder         v7.0.0
  - core.candlesticks             v1.0.0
  - routes/analysis_sheet_rows    4.4.0    (downstream registry-derived router)

WHY v4.2.0 (condensed) — removed every hard-coded width literal from the
contract path so Top_10 stops clamping to 83; `_expected_len` prefers the live
registry's `get_sheet_len`; `_ensure_top10_contract` gained an optional
`target_len` with a hard anti-truncation invariant; `_resolve_contract` passes
the registry-derived width through. WHY v4.1.0 (condensed) — inverted the
engine binding cascade (v2 first, legacy fallback) so v5.x enrichment reaches
the wire, and added the provider-health surface (`meta.provider_health`,
`/v1/schema/provider-health`, envelope warning lift, health-endpoint counts).
All of that machinery is preserved verbatim below.

Purpose
-------
Owns the canonical root paths:
- /sheet-rows
- /schema
- /schema/sheet-spec
- /schema/pages
- /schema/data-dictionary
- /v1/schema/provider-health
- /opportunity-candidates  (effective live path: /sheet-rows/opportunity-candidates;
  see v4.7.1 [MOUNT-FIX] — main.py prefix filter)
- /portfolio-actions  (effective live path: /sheet-rows/portfolio-actions;
  same mount-filter rationale — v4.8.0 [PF-ADD])
  (opportunity pool is trend-enriched when TFB_TREND_SIGNALS=1 — v4.9.0)
and their /v1/schema aliases.
================================================================================
"""

from __future__ import annotations

import asyncio
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
# v4.11.0 — Version constant.
# =============================================================================
ADVANCED_ANALYSIS_VERSION = "4.11.0"


# =============================================================================
# v4.1.0 [FIX-A — CRITICAL] Engine binding cascade.  (preserved verbatim)
#
# v4.0.0 tried `core.data_engine.get_sheet_rows` (legacy) FIRST and only fell
# back to `core.data_engine_v2` if the legacy import failed. Since legacy always
# imports successfully on this deployment, the v2 path was never reached and the
# v5.x enrichment (Decision Matrix, Insights, Candlesticks, provider_unhealthy
# registry, gate, ...) never made it to the wire.
#
# v4.1.0 inverts that preference. Cascading binding probe in order:
#   1. `core.data_engine_v2.get_engine()` — async or sync factory.
#   2. `core.data_engine_v2.get_engine_if_ready()` — sync ready-check with
#      async-factory cold-start fallback.
#   3. `core.data_engine_v2.get_sheet_rows` — top-level module-level fn.
#   4. `core.data_engine.get_sheet_rows` — legacy adapter (bug indicator).
#
# Every step logs explicitly. The chosen binding is reflected in
# CORE_GET_SHEET_ROWS_SOURCE and surfaces in every response's meta.source field.
# =============================================================================
CORE_GET_SHEET_ROWS_SOURCE = "unavailable"
core_get_sheet_rows = None  # type: ignore[assignment]

# Pattern 1: get_engine() factory (sync or async) — EXPECTED PATH.
try:
    from core.data_engine_v2 import get_engine as _v2_get_engine  # type: ignore

    async def _v2_engine_factory_adapter(*args: Any, **kwargs: Any) -> Any:
        """v4.1.0: runtime-await-aware adapter for v2 engine factory.

        `core.data_engine_v2.get_engine` may be sync (v5.50.0+ behavior,
        returns DataEngineV5 instance directly) or async (older or future
        builds, returns a coroutine that resolves to the instance).
        `inspect.isawaitable` distinguishes at runtime.
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
            "(this loses v5.x enrichment including the investability gate — investigate v2 exports)",
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
# v4.4.0 [DD-FIX] Real Data_Dictionary builder binding.  (NEW in v4.4.0)
#
# The canonical Data_Dictionary CONTENT builder is core.sheets.data_dictionary
# (v3.2.0+, `build_data_dictionary_payload`). It is pure / import-safe (no I/O,
# no network, reads only the schema registry) and emits one row per schema
# column across all sheets. Its row keys are EXACTLY the route's
# _DICTIONARY_KEYS (sheet, group, header, key, dtype, fmt, required, source,
# notes), so rows project onto the route contract with no remapping.
#
# Bound here once at import. Fail-soft: if the import fails, `_build_dd_payload`
# stays None and the Data_Dictionary paths fall back to their prior behavior.
# =============================================================================
_build_dd_payload = None  # type: ignore[assignment]
try:
    from core.sheets.data_dictionary import build_data_dictionary_payload as _build_dd_payload  # type: ignore
    logger.info(
        "[advanced_analysis v%s] real Data_Dictionary builder bound (core.sheets.data_dictionary)",
        ADVANCED_ANALYSIS_VERSION,
    )
except Exception as _dd_err:
    _build_dd_payload = None  # type: ignore
    logger.info(
        "[advanced_analysis v%s] core.sheets.data_dictionary unavailable (%s); "
        "Data_Dictionary will use degraded self-description fallback",
        ADVANCED_ANALYSIS_VERSION, _dd_err,
    )


# =============================================================================
# v4.5.0 [BRIDGE-FIX] Real Top_10 / Insights builder bindings.  (NEW in v4.5.0)
#
# These two pages are DERIVED (not direct instrument scans): the live engine's
# get_sheet_rows returns only a 1-row envelope for them, which previously
# pre-empted the real builders on the happy path. Binding the real builders
# here (once, at import) lets _run_advanced_sheet_rows_impl dispatch to them
# BEFORE the generic engine call — exactly the precedence Data_Dictionary
# already enjoys via _build_dd_payload.
#
#   _build_top10_rows : core.analysis.top10_selector.build_top10_rows (v4.15.0)
#       Returns the canonical Top_10 envelope ({rows, row_objects, headers,
#       keys, meta, ...}). NOTE: inside a running event loop it returns a
#       COROUTINE (its sync/async shim), so callers MUST _maybe_await it — the
#       exact bug that left the live route serving the engine's 1 root row.
#   _build_insights_rows : core.analysis.insights_builder.build_insights_analysis_rows
#       (v8.2.0). Async; keyword-only. Returns the Insights_Analysis envelope.
#
# Fail-soft: if either import fails the binding stays None and that page falls
# through to the engine path and then the existing non-empty fallback (no
# regression vs v4.4.0).
# =============================================================================
_build_top10_rows = None  # type: ignore[assignment]
try:
    from core.analysis.top10_selector import build_top10_rows as _build_top10_rows  # type: ignore
    logger.info(
        "[advanced_analysis v%s] real Top_10 builder bound (core.analysis.top10_selector.build_top10_rows)",
        ADVANCED_ANALYSIS_VERSION,
    )
except Exception as _t10_err:
    _build_top10_rows = None  # type: ignore
    logger.info(
        "[advanced_analysis v%s] core.analysis.top10_selector unavailable (%s); "
        "Top_10_Investments will use engine/fallback path",
        ADVANCED_ANALYSIS_VERSION, _t10_err,
    )

_build_insights_rows = None  # type: ignore[assignment]
try:
    from core.analysis.insights_builder import build_insights_analysis_rows as _build_insights_rows  # type: ignore
    logger.info(
        "[advanced_analysis v%s] real Insights builder bound (core.analysis.insights_builder.build_insights_analysis_rows)",
        ADVANCED_ANALYSIS_VERSION,
    )
except Exception as _ins_err:
    _build_insights_rows = None  # type: ignore
    logger.info(
        "[advanced_analysis v%s] core.analysis.insights_builder unavailable (%s); "
        "Insights_Analysis will use engine/fallback path",
        ADVANCED_ANALYSIS_VERSION, _ins_err,
    )


# =============================================================================
# v4.7.0 [OPP-ADD] Opportunity builder binding.  (NEW in v4.7.0)
#
# core.analysis.opportunity_builder v1.0.1 (Plan v5.0 P2) is the pure-compute
# intelligence layer for the rebuilt Top_10_Investments decision page: §4.2
# hard gates -> verdicts, §4.3 opportunity score, §4.4 wealth math (SAR sizing
# against Deployable Capital, L7 funding identity), and the FROZEN §5 zone
# payload served by POST /opportunity-candidates below. Stdlib-only and
# import-safe. Fail-soft: unbound -> the endpoint answers status="unavailable"
# instead of raising; every other route is unaffected.
# =============================================================================
_build_opportunity_payload = None  # type: ignore[assignment]
_OPP_BUILDER_VERSION: Optional[str] = None
try:
    from core.analysis.opportunity_builder import (  # type: ignore
        OPPORTUNITY_BUILDER_VERSION as _OPP_BUILDER_VERSION,
        build_opportunity_payload as _build_opportunity_payload,
    )
    logger.info(
        "[advanced_analysis v%s] opportunity builder bound (core.analysis.opportunity_builder v%s)",
        ADVANCED_ANALYSIS_VERSION, _OPP_BUILDER_VERSION,
    )
except Exception as _opp_err:
    _build_opportunity_payload = None  # type: ignore
    _OPP_BUILDER_VERSION = None
    logger.info(
        "[advanced_analysis v%s] core.analysis.opportunity_builder unavailable (%s); "
        "/opportunity-candidates will answer status=unavailable",
        ADVANCED_ANALYSIS_VERSION, _opp_err,
    )


# =============================================================================
# v4.10.0 [NEWS-DISPLAY] Live news sentiment for the surfaced decision symbols.
#
# core.news_intelligence (v5.x, Google-News RSS, no provider key) is bound here
# and used DISPLAY-ONLY: it runs AFTER the opportunity builder and only fills
# the `news_trend` label on OUTPUT rows. It never mutates an input row, so the
# builder's News gate / news_trend score / selection are byte-identical to
# NEWS_DISPLAY off. Fail-soft: unbound or erroring -> rows stay Unknown.
# =============================================================================
_ni_batch_news = None  # type: ignore[assignment]
_NEWS_DISPLAY_AVAILABLE = False
try:
    from core.news_intelligence import batch_news_intelligence as _ni_batch_news  # type: ignore
    _NEWS_DISPLAY_AVAILABLE = True
    logger.info("[advanced_analysis v%s] news_intelligence bound (core.news_intelligence)",
                ADVANCED_ANALYSIS_VERSION)
except Exception:
    try:
        from news_intelligence import batch_news_intelligence as _ni_batch_news  # type: ignore
        _NEWS_DISPLAY_AVAILABLE = True
        logger.info("[advanced_analysis v%s] news_intelligence bound (news_intelligence)",
                    ADVANCED_ANALYSIS_VERSION)
    except Exception as _ni_err:
        _ni_batch_news = None  # type: ignore
        _NEWS_DISPLAY_AVAILABLE = False
        logger.info(
            "[advanced_analysis v%s] core.news_intelligence unavailable (%s); "
            "News column stays Unknown even when TFB_NEWS_DISPLAY=1",
            ADVANCED_ANALYSIS_VERSION, _ni_err,
        )


def _env_truthy(name: str, default: bool = False) -> bool:
    """1/true/yes/y/on -> True; empty -> default. Matches the route's existing
    inline env idiom (see ALLOW_QUERY_TOKEN / TFB_TOP10_FULL_UNIVERSE)."""
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return default
    return v in {"1", "true", "yes", "y", "on"}


def _env_num(name: str, default: float) -> float:
    try:
        raw = (os.getenv(name) or "").strip()
        return float(raw) if raw else float(default)
    except Exception:
        return float(default)


def _news_display_enabled() -> bool:
    return _env_truthy("TFB_NEWS_DISPLAY", False)


def _adv_engine_call_timeout_enabled() -> bool:
    """v4.11.0 edge-timeout guard master switch. See CHANGELOG. Default OFF ->
    byte-identical to v4.10.0 (unbounded engine call)."""
    return _env_truthy("TFB_ADV_ENGINE_CALL_TIMEOUT", False)


def _adv_engine_call_timeout_s() -> float:
    """Budget (seconds) for the engine call when the guard is enabled. Default
    75.0 -- comfortably under Render's ~100s edge timeout, leaving headroom
    for auth/schema resolution + envelope serialization on either side of the
    call. Clamped to a sane [5, 95] range so a bad env value can't disable the
    protection (too high) or make every request fail-soft (too low)."""
    v = _env_num("TFB_ADV_ENGINE_CALL_TIMEOUT_S", 75.0)
    return max(5.0, min(95.0, v))


def _news_display_max() -> int:
    n = int(_env_num("TFB_NEWS_DISPLAY_MAX", 60.0))
    return max(1, min(400, n))


def _news_label(sentiment: Optional[float]) -> str:
    """Map a [-1..1] sentiment to the builder's Positive/Neutral/Negative
    vocabulary; None/non-numeric -> Unknown. Thresholds are env-tunable."""
    if sentiment is None:
        return "Unknown"
    try:
        s = float(sentiment)
    except Exception:
        return "Unknown"
    if s >= _env_num("TFB_NEWS_DISPLAY_POS", 0.15):
        return "Positive"
    if s <= _env_num("TFB_NEWS_DISPLAY_NEG", -0.15):
        return "Negative"
    return "Neutral"


def _news_safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _news_safe_int(x: Any) -> int:
    try:
        return int(float(x))
    except Exception:
        return 0


async def _attach_news_display(payload: Dict[str, Any]) -> Dict[str, Any]:
    """v4.10.0 [NEWS-DISPLAY]: fill the News column for the SURFACED decision
    symbols (selected + near_miss + candidates_rows) with live sentiment.

    DISPLAY-ONLY: called AFTER the builder; mutates only the displayed
    `news_trend` label (plus news_sentiment / news_confidence / news_articles)
    on the OUTPUT row dicts. The builder's scoring / gating / selection already
    ran on the un-mutated input pool and are unchanged. Bounded to
    TFB_NEWS_DISPLAY_MAX symbols and time-boxed; any failure leaves rows
    Unknown. Returns a meta dict for meta.route.news_display.
    """
    cap = _news_display_max()
    meta: Dict[str, Any] = {"enabled": True, "bound": bool(_NEWS_DISPLAY_AVAILABLE),
                            "fetched": 0, "labeled": 0, "cap": cap}
    if not (_NEWS_DISPLAY_AVAILABLE and _ni_batch_news is not None):
        meta["bound"] = False
        return meta

    buckets: List[List[Any]] = []
    for key in ("selected", "near_miss", "candidates_rows"):
        v = payload.get(key)
        if isinstance(v, list) and v:
            buckets.append(v)
    if not buckets:
        return meta

    # Symbol fetch list in priority order (selected first, then near_miss, then
    # the candidates_rows tail), deduped and capped.
    name_by_sym: Dict[str, str] = {}
    ordered_syms: List[str] = []
    for bucket in buckets:
        for row in bucket:
            if not isinstance(row, dict):
                continue
            sym = _strip(row.get("symbol") or row.get("requested_symbol")).upper()
            if not sym or sym in name_by_sym:
                continue
            name_by_sym[sym] = _strip(row.get("name"))
            ordered_syms.append(sym)
    want = ordered_syms[:cap]
    if not want:
        return meta

    items = [{"symbol": s, "name": name_by_sym.get(s, "")} for s in want]
    try:
        batch = await asyncio.wait_for(
            _ni_batch_news(items, include_articles=False),
            timeout=_env_num("TFB_NEWS_DISPLAY_TIMEOUT_S", 25.0),
        )
    except Exception as exc:  # timeout or runtime/network guard
        meta["error"] = "{}: {}".format(exc.__class__.__name__, str(exc)[:160])
        return meta

    results = getattr(batch, "items", None) or []
    smap: Dict[str, Dict[str, Any]] = {}
    for it in results:
        if not isinstance(it, dict):
            continue
        sym = _strip(it.get("symbol")).upper()
        if not sym:
            continue
        sent = _news_safe_float(it.get("sentiment"))
        smap[sym] = {
            "label": _news_label(sent),
            "sentiment": sent,
            "confidence": _news_safe_float(it.get("confidence")),
            "articles": _news_safe_int(it.get("articles_analyzed")),
        }
    meta["fetched"] = len(smap)

    labeled = 0
    for bucket in buckets:
        for row in bucket:
            if not isinstance(row, dict):
                continue
            sym = _strip(row.get("symbol") or row.get("requested_symbol")).upper()
            ent = smap.get(sym)
            if not ent or ent["label"] == "Unknown":
                continue
            row["news_trend"] = ent["label"]
            row["news_sentiment"] = ent["sentiment"]
            row["news_confidence"] = ent["confidence"]
            row["news_articles"] = ent["articles"]
            labeled += 1
    meta["labeled"] = labeled
    return meta


# =============================================================================
# v4.8.0 [PF-ADD] Portfolio actions binding.  (NEW in v4.8.0)
#
# core.analysis.portfolio_actions v1.0.0 (Plan v5.0 P5) is the pure-compute
# action engine for the rebuilt My_Portfolio decision page: the v1.0.0-pinned
# action truth table (BLOCK -> low-confidence cap -> EXIT -> TRIM -> ADD ->
# HOLD), the L7 funding pass (Target-Cash floor, mode-aware proceeds,
# cash-first funding with funds_from labels), L8 advisor sentences, sector
# summary, and alerts. It DELEGATES all row normalization to
# opportunity_builder (one implementation, two consumers) and degrades to its
# own "unavailable" skeleton if that import fails — so this binding succeeding
# does not guarantee full function; meta.versions reports both. Fail-soft:
# unbound -> POST /portfolio-actions answers status="unavailable"; every other
# route is unaffected.
# =============================================================================
_build_portfolio_actions = None  # type: ignore[assignment]
_PF_ACTIONS_VERSION: Optional[str] = None
try:
    from core.analysis.portfolio_actions import (  # type: ignore
        PORTFOLIO_ACTIONS_VERSION as _PF_ACTIONS_VERSION,
        build_portfolio_actions as _build_portfolio_actions,
    )
    logger.info(
        "[advanced_analysis v%s] portfolio actions bound (core.analysis.portfolio_actions v%s)",
        ADVANCED_ANALYSIS_VERSION, _PF_ACTIONS_VERSION,
    )
except Exception as _pf_err:
    _build_portfolio_actions = None  # type: ignore
    _PF_ACTIONS_VERSION = None
    logger.info(
        "[advanced_analysis v%s] core.analysis.portfolio_actions unavailable (%s); "
        "/portfolio-actions will answer status=unavailable",
        ADVANCED_ANALYSIS_VERSION, _pf_err,
    )


# =============================================================================
# v4.9.0 [TREND-ADD] Trend signals binding.  (NEW in v4.9.0)
#
# core.analysis.trend_signals v1.0.0 (Plan v5.0 P9) computes sector trend
# cross-sectionally from the candidate pool (stdlib-only, deterministic,
# no provider calls) and stamps rows the builder then consumes via its own
# sector_trend aliases. Env-gated DEFAULT OFF (TFB_TREND_SIGNALS=1); the
# module itself returns rows untouched when disabled. Fail-soft: unbound ->
# the opportunity endpoint runs exactly as v4.8.0 did.
# =============================================================================
_enrich_rows_with_trends = None  # type: ignore[assignment]
_TREND_SIGNALS_VERSION: Optional[str] = None
try:
    from core.analysis.trend_signals import (  # type: ignore
        TREND_SIGNALS_VERSION as _TREND_SIGNALS_VERSION,
        enrich_rows_with_trends as _enrich_rows_with_trends,
    )
    logger.info(
        "[advanced_analysis v%s] trend signals bound (core.analysis.trend_signals v%s)",
        ADVANCED_ANALYSIS_VERSION, _TREND_SIGNALS_VERSION,
    )
except Exception as _trend_err:
    _enrich_rows_with_trends = None  # type: ignore
    _TREND_SIGNALS_VERSION = None
    logger.info(
        "[advanced_analysis v%s] core.analysis.trend_signals unavailable (%s); "
        "opportunity pool will not be trend-enriched",
        ADVANCED_ANALYSIS_VERSION, _trend_err,
    )


# =============================================================================
# v4.1.0 [ADD-B/C] Provider-health snapshot helpers.  (preserved verbatim)
#
# These resolve the v2 engine instance directly (bypassing the get_sheet_rows
# adapter) so we can call `engine.health()` and read the `provider_unhealthy_
# markers` field. Cached at module level after first successful resolution.
# =============================================================================
_V2_ENGINE_HEALTH_FACTORY: Optional[Any] = None
_V2_ENGINE_HEALTH_FACTORY_INITED: bool = False


def _resolve_v2_engine_factory() -> Optional[Any]:
    """v4.1.0: One-time resolution of the v2 engine factory callable.

    Caches the result at module level. Returns None if v2 is unavailable
    (graceful degradation — provider_health fields simply omit from responses).
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

    Calls `engine.health()` and extracts the `provider_unhealthy_markers` field
    plus `engine_version`. Returns a JSON-safe dict suitable for embedding in
    `meta.provider_health` or serving from the dedicated endpoint.

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
        # Older engine version without the registry field.
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

    Scans each row's `warnings` field for the markers:
      - "circuit_open" -> "Provider circuit open (engine routing to fallback chain)"
      - "provider_unhealthy:<X>" -> "Upstream provider unhealthy: <X> (engine routing around it)"
      - "HTTP 403" -> "Provider access issue (HTTP 403)"

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

# v4.2.0 [FIX-A]: _EXPECTED_SHEET_LENGTHS is DERIVED from the canonical list
# lengths and defined AFTER the canonical lists below (see the "v4.2.0 derived
# contract widths" block). Static-fallback-only: _expected_len() always prefers
# the live registry's get_sheet_len() first. v4.3.0 keeps this — the derived
# values now resolve to 115 / 118 because the canonical list grew to 115.

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

# (v4.1.0: the original legacy-first binding block was REMOVED here; the
# v2-first cascade now lives at module top after the imports.)

# =============================================================================
# v4.3.0 [FIX-A/B]: static instrument canonical = the LIVE 115-key contract.
#
# This list is the EXACT data_engine_v2 v5.79.2 INSTRUMENT_CANONICAL_* /
# schema_registry v2.13.0 column order: 90 prior columns + Decision pair +
# Provider Rating + Canonical-Reco pair + remaining Scoring-v5.74 + Candlestick
# block + forecast_source + the 8-column Investability Gate (positions 108-115).
# FALLBACK-ONLY — on the live path the registry contract is authoritative
# (_expected_len -> get_sheet_len, _resolve_contract projects onto registry
# keys). v4.3.0 widened/reordered it so the registry-DOWN path no longer
# regresses below the gate. (Renamed from the misleading `_CANONICAL_80_*`.)
# =============================================================================
_CANONICAL_INSTRUMENT_HEADERS: List[str] = [
    # Identity (8)
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
    # Price (10) -> 18
    "Current Price", "Previous Close", "Open", "Day High", "Day Low", "52W High", "52W Low",
    "Price Change", "Percent Change", "52W Position %",
    # Liquidity (6) -> 24
    "Volume", "Avg Volume 10D", "Avg Volume 30D", "Market Cap", "Float Shares", "Beta (5Y)",
    # Fundamentals (12) -> 36
    "P/E (TTM)", "P/E (Forward)", "EPS (TTM)", "Dividend Yield", "Payout Ratio", "Revenue (TTM)",
    "Revenue Growth YoY", "Gross Margin", "Operating Margin", "Profit Margin", "Debt/Equity",
    "Free Cash Flow (TTM)",
    # Risk (8) -> 44
    "RSI (14)", "Volatility 30D", "Volatility 90D", "Max Drawdown 1Y", "VaR 95% (1D)", "Sharpe (1Y)",
    "Risk Score", "Risk Bucket",
    # Valuation (7) -> 51
    "P/B", "P/S", "EV/EBITDA", "PEG", "Intrinsic Value", "Upside %", "Valuation Score",
    # Forecast (9) -> 60
    "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M", "Expected ROI 1M",
    "Expected ROI 3M", "Expected ROI 12M", "Forecast Confidence", "Confidence Score",
    "Confidence Bucket",
    # Scores (7) -> 67  (opportunity_score / rank_overall BEFORE views — engine order)
    "Value Score", "Quality Score", "Momentum Score", "Growth Score", "Overall Score",
    "Opportunity Score", "Rank (Overall)",
    # Views (4) -> 71
    "Fundamental View", "Technical View", "Risk View", "Value View",
    # Recommendation (4) -> 75
    "Recommendation", "Recommendation Reason", "Horizon Days", "Invest Period Label",
    # Portfolio (6) -> 81
    "Position Qty", "Avg Cost", "Position Cost", "Position Value", "Unrealized P/L",
    "Unrealized P/L %",
    # Provenance (4) -> 85
    "Data Provider", "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",
    # Insights (5) -> 90
    "Sector-Adj Score", "Conviction Score", "Top Factors", "Top Risks", "Position Size Hint",
    # Decision (2) -> 92
    "Recommendation Detail", "Reco Priority",
    # Provider Rating (1) -> 93
    "Provider Rating",
    # Canonical Reco (2) -> 95
    "Recommendation Source", "Priority Band",
    # Scoring v5.74 remaining (6) -> 101
    "Scoring Reco Source", "Scoring Schema Version", "Scoring Errors", "Opportunity Source",
    "Overall Score (Raw)", "Overall Penalty Factor",
    # Candlestick (5) -> 106
    "Candle Pattern", "Candle Signal", "Candle Strength", "Candle Confidence",
    "Recent Patterns (5D)",
    # Forecast Source (1) -> 107
    "Forecast Source",
    # Investability Gate (8) -> 115
    "Data Quality Score", "Forecast Reliability Score", "Provider/Engine Conflict",
    "Conflict Type", "Final Decision Basis", "Investability Status", "Final Action",
    "Block Reason",
]
_CANONICAL_INSTRUMENT_KEYS: List[str] = [
    # Identity (8)
    "symbol", "name", "asset_class", "exchange", "currency", "country", "sector", "industry",
    # Price (10) -> 18
    "current_price", "previous_close", "open_price", "day_high", "day_low", "week_52_high",
    "week_52_low", "price_change", "percent_change", "week_52_position_pct",
    # Liquidity (6) -> 24
    "volume", "avg_volume_10d", "avg_volume_30d", "market_cap", "float_shares", "beta_5y",
    # Fundamentals (12) -> 36
    "pe_ttm", "pe_forward", "eps_ttm", "dividend_yield", "payout_ratio", "revenue_ttm",
    "revenue_growth_yoy", "gross_margin", "operating_margin", "profit_margin", "debt_to_equity",
    "free_cash_flow_ttm",
    # Risk (8) -> 44
    "rsi_14", "volatility_30d", "volatility_90d", "max_drawdown_1y", "var_95_1d", "sharpe_1y",
    "risk_score", "risk_bucket",
    # Valuation (7) -> 51
    "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio", "intrinsic_value", "upside_pct",
    "valuation_score",
    # Forecast (9) -> 60
    "forecast_price_1m", "forecast_price_3m", "forecast_price_12m", "expected_roi_1m",
    "expected_roi_3m", "expected_roi_12m", "forecast_confidence", "confidence_score",
    "confidence_bucket",
    # Scores (7) -> 67
    "value_score", "quality_score", "momentum_score", "growth_score", "overall_score",
    "opportunity_score", "rank_overall",
    # Views (4) -> 71
    "fundamental_view", "technical_view", "risk_view", "value_view",
    # Recommendation (4) -> 75
    "recommendation", "recommendation_reason", "horizon_days", "invest_period_label",
    # Portfolio (6) -> 81
    "position_qty", "avg_cost", "position_cost", "position_value", "unrealized_pl",
    "unrealized_pl_pct",
    # Provenance (4) -> 85
    "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",
    # Insights (5) -> 90
    "sector_relative_score", "conviction_score", "top_factors", "top_risks", "position_size_hint",
    # Decision (2) -> 92
    "recommendation_detailed", "recommendation_priority",
    # Provider Rating (1) -> 93
    "provider_rating",
    # Canonical Reco (2) -> 95
    "recommendation_source", "recommendation_priority_band",
    # Scoring v5.74 remaining (6) -> 101
    "scoring_recommendation_source", "scoring_schema_version", "scoring_errors",
    "opportunity_source", "overall_score_raw", "overall_penalty_factor",
    # Candlestick (5) -> 106
    "candlestick_pattern", "candlestick_signal", "candlestick_strength", "candlestick_confidence",
    "candlestick_patterns_recent",
    # Forecast Source (1) -> 107
    "forecast_source",
    # Investability Gate (8) -> 115
    "data_quality_score", "forecast_reliability_score", "provider_engine_conflict",
    "conflict_type", "final_decision_basis", "investability_status", "final_action",
    "block_reason",
]

# v4.3.0 [FIX-C]: Insights static fallback reconciled to schema_registry v2.12.0+
# / data_engine_v2 INSIGHTS_KEYS / INSIGHTS_HEADERS. Was the pre-v2.12.0
# [Section, Item, Symbol, Metric, Value, Notes, Last Updated (Riyadh)]; now the
# engine contract [Section, Item, Metric, Value, Notes, Source, Sort Order].
# Count stays 7. Fallback-only — the live registry contract is authoritative.
_INSIGHTS_HEADERS = ["Section", "Item", "Metric", "Value", "Notes", "Source", "Sort Order"]
_INSIGHTS_KEYS = ["section", "item", "metric", "value", "notes", "source", "sort_order"]
_DICTIONARY_HEADERS = ["Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"]
_DICTIONARY_KEYS = ["sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes"]

# =============================================================================
# v4.2.0 derived contract widths (TRACKS CANONICAL — no hardcoded 80/83/90/115)
# =============================================================================
# Every instrument-page width (including My_Investments) derives from
# len(_CANONICAL_INSTRUMENT_KEYS); Top_10 derives as instrument_len + the Top_10
# extras count. v4.3.0 grew the canonical list to 115, so these auto-resolve to
# 115 / 118 with no further edits. At runtime _expected_len() still prefers the
# live registry's get_sheet_len() over this static-fallback map, so the route
# faithfully tracks whatever the deployed registry serves.
_CANONICAL_INSTRUMENT_LEN: int = len(_CANONICAL_INSTRUMENT_KEYS)
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
    "My_Investments": _CANONICAL_INSTRUMENT_LEN,
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

def _ensure_top10_contract(headers: Sequence[str], keys: Sequence[str], target_len: Optional[int] = None) -> Tuple[List[str], List[str]]:
    """v4.2.0 [FIX-B]: append the 3 Top_10 extras, then pad to `target_len`.

    The literal `83` clamp this function used to end with was the Top_10
    truncation point: it silently dropped every column past index 83 — including
    the v5.78.0 investability gate (forecast_source + 8 gate columns) once the
    registry/canonical grew to 115. v4.2.0 removed it; v4.3.0 keeps the same
    contract.

    Width semantics:
      - target_len is None  -> pad to the natural width (canonical + extras),
        i.e. no truncation, no extra padding.
      - target_len provided -> pad UP to it, but NEVER truncate below the
        canonical+extras we already hold (hard anti-truncation invariant).
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
    # v4.3.0: widths derived from the 115-key canonical list lengths.
    if page == _TOP10_PAGE:
        h, k = _ensure_top10_contract(_CANONICAL_INSTRUMENT_HEADERS, _CANONICAL_INSTRUMENT_KEYS, target_len=_TOP10_STATIC_LEN)
        return h, k, "static_canonical_top10"
    if page == _INSIGHTS_PAGE:
        h, k = _pad_contract(_INSIGHTS_HEADERS, _INSIGHTS_KEYS, _INSIGHTS_STATIC_LEN)
        return h, k, "static_canonical_insights"
    if page == _DICTIONARY_PAGE:
        h, k = _pad_contract(_DICTIONARY_HEADERS, _DICTIONARY_KEYS, _DICTIONARY_STATIC_LEN)
        return h, k, "static_canonical_dictionary"
    h, k = _pad_contract(_CANONICAL_INSTRUMENT_HEADERS, _CANONICAL_INSTRUMENT_KEYS, _EXPECTED_SHEET_LENGTHS.get(page, _CANONICAL_INSTRUMENT_LEN))
    return h, k, "static_canonical_instrument"

def _expected_len(page: str) -> int:
    # v4.2.0 [FIX-E]: runtime-authoritative — prefer the live registry's width
    # so the route tracks whatever the deployed schema_registry serves. Falls
    # back to the derived static map only when the registry is unavailable.
    if callable(get_sheet_len):
        try:
            n = int(get_sheet_len(page))  # type: ignore[misc]
            if n > 0:
                return n
        except Exception:
            pass
    return _EXPECTED_SHEET_LENGTHS.get(page, _CANONICAL_INSTRUMENT_LEN)

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
            # v4.2.0 [FIX-C]: pass the registry-derived width so a 118-col
            # registry result is preserved (was clamped to the literal 83).
            headers, keys = _ensure_top10_contract(headers, keys, target_len=expected_len)
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

def _ensure_top10_rows(rows: Sequence[Mapping[str, Any]], *, requested_symbols: Sequence[str], top_n: int, schema_keys: Sequence[str], schema_headers: Sequence[str], preserve_order: bool = False) -> List[Dict[str, Any]]:
    normalized_rows = [_normalize_to_schema_keys(schema_keys=schema_keys, schema_headers=schema_headers, raw=(r or {})) for r in rows or []]
    # v4.6.0 [ORDER-FIX]: when the rows came from the REAL selector
    # (core.analysis.top10_selector), its ranking is authoritative — selector
    # score, priority band, horizon ROI, direct-symbol ordering, and
    # backfill-last placement (v4.15.0/v4.16.1). The pre-v4.6.0 unconditional
    # re-sort by _top10_sort_key (overall_score-first) DESTROYED that order:
    # live audit 2026-06-10 showed GC=F (HOLD, overall 69) re-sorted to rank 2
    # above ACCUMULATE names, and a "[BACKFILL: below criteria]" row can jump
    # above passing rows. In preserve mode: dedupe in GIVEN order (first
    # occurrence wins), slice, and only FILL a blank top10_rank — never
    # overwrite the selector's. The engine / fail-soft paths (no selector
    # ordering to preserve) keep the exact prior sort behavior.
    iterable = normalized_rows if preserve_order else sorted(normalized_rows, key=_top10_sort_key, reverse=True)
    deduped: List[Dict[str, Any]] = []
    seen = set()
    for row in iterable:
        sym = _strip(row.get("symbol"))
        name = _strip(row.get("name"))
        key = sym or name or f"row_{len(deduped)+1}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    final_rows = deduped[:max(1, int(top_n))]
    for idx, row in enumerate(final_rows, start=1):
        if preserve_order:
            if not _strip(row.get("top10_rank")) and not isinstance(row.get("top10_rank"), (int, float)):
                row["top10_rank"] = idx
        else:
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

def _real_data_dictionary_rows() -> List[Dict[str, Any]]:
    """v4.4.0 [DD-FIX]: rows from the real core.sheets.data_dictionary builder.

    Returns the canonical Data_Dictionary content — one dict per schema column
    across every sheet — using `build_data_dictionary_payload(format="rows")`.
    The builder is pure / import-safe (registry-only; no engine, no I/O, no
    network), so this path produces correct Data_Dictionary rows even when the
    engine has not reached the container. Row keys already equal the route's
    _DICTIONARY_KEYS (sheet, group, header, key, dtype, fmt, required, source,
    notes), so no remapping is needed here.

    Fail-soft: returns [] if the builder is unbound or raises, letting callers
    fall back to their prior degraded behavior.
    """
    if not callable(_build_dd_payload):
        return []
    try:
        payload = _build_dd_payload(format="rows")  # type: ignore[misc]
    except Exception as e:
        logger.warning(
            "[advanced_analysis v%s] real Data_Dictionary builder raised: %s: %s",
            ADVANCED_ANALYSIS_VERSION, e.__class__.__name__, e,
        )
        return []
    if not isinstance(payload, Mapping):
        return []
    rows = payload.get("row_objects")
    if not (isinstance(rows, list) and rows):
        rows = payload.get("rows")
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        if isinstance(r, Mapping):
            out.append(dict(r))
    return out

def _build_dictionary_fallback_rows(*, page: str, headers: Sequence[str], keys: Sequence[str], limit: int, offset: int) -> List[Dict[str, Any]]:
    # v4.4.0 [DD-FIX]: prefer the real registry-derived Data_Dictionary content
    # (one row per schema column across all sheets) over the prior degraded
    # self-description (which only described the dictionary's OWN 9 columns).
    # The real builder is pure/import-safe, so this works even when the engine
    # has not reached the container.
    real_rows = _real_data_dictionary_rows()
    if real_rows:
        normalized = [
            _normalize_to_schema_keys(schema_keys=keys, schema_headers=headers, raw=r)
            for r in real_rows
        ]
        return _slice(normalized, limit=limit, offset=offset)
    # Last-resort degraded fallback (builder unavailable): self-description of
    # the Data_Dictionary contract itself. Preserved verbatim from v4.3.0.
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
    # v4.3.0 [FIX-C]: emit the engine/registry v2.12.0+ Insights keys
    # (section, item, metric, value, notes, source, sort_order). Dropped the
    # pre-v2.12.0 `symbol` / `last_updated_riyadh` fields; the requested symbol
    # is folded into the row `item` so the fallback summary stays informative.
    symbols = [_normalize_symbol_token(x) for x in requested_symbols if _normalize_symbol_token(x)]
    if not symbols:
        symbols = [_normalize_symbol_token(x) for x in EMERGENCY_PAGE_SYMBOLS.get(_INSIGHTS_PAGE, []) if _normalize_symbol_token(x)]
    src = "advanced_analysis.local_insights_fallback"
    rows: List[Dict[str, Any]] = [
        {"section": "Coverage", "item": "Requested symbols", "metric": "count", "value": len(symbols), "notes": "Local insights fallback summary", "source": src, "sort_order": 1},
        {"section": "Coverage", "item": "Universe sample", "metric": "symbols", "value": ", ".join(symbols[:5]), "notes": "Sample of the symbols used by fallback mode", "source": src, "sort_order": 2},
    ]
    next_order = 3
    for idx, sym in enumerate(symbols[: max(1, limit + offset)], start=1):
        rows.append({
            "section": "Signals",
            "item": f"{sym} fallback signal",
            "metric": "recommendation",
            "value": "Watch" if idx > 2 else "Accumulate",
            "notes": "Generated locally because upstream insights payload was unavailable",
            "source": src,
            "sort_order": next_order,
        })
        next_order += 1
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
      - `provider_health`: optional engine `health()` snapshot. Surfaced under
        `meta.provider_health`. Pass the dict from
        `_extract_provider_health_snapshot()`.
      - `lifted_warnings`: optional list of envelope-level warnings lifted from
        row warnings via `_lift_systemic_warning_markers`. Surfaced under the
        top-level `warnings` key (Apps Script 12_Diagnostics.gs can read this
        without scanning every row).
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
    top10_preserve_order: bool = False,
) -> Dict[str, Any]:
    """Normalize an engine payload into the canonical envelope.

    v4.1.0: forwards `provider_health` snapshot into the envelope so the success
    path carries the same `meta.provider_health` field as the fail-soft path.
    Also lifts systemic markers from row warnings into envelope-level
    `warnings`.

    v4.2.0/v4.3.0 note: `keys` here is the registry-derived contract from
    `_resolve_contract` (115/118 now that the registry is at v2.13.0). Rows are
    projected onto `keys` directly — no re-clamp — so the gate columns survive
    whenever the registry serves them. This is why there is no third truncation
    point in this route (unlike analysis_sheet_rows v4.4.0).
    """
    ext = dict(external_payload or {})
    hdrs = list(headers or [])
    ks = list(keys or [])
    rows = _extract_rows_like(ext)
    normalized_rows = [_normalize_to_schema_keys(schema_keys=ks, schema_headers=hdrs, raw=(r or {})) for r in rows]
    if page == _TOP10_PAGE:
        # v4.6.0 [ORDER-FIX]: preserve selector ordering on the selector_real
        # dispatch; engine/fallback rows keep the prior sort (see
        # _ensure_top10_rows).
        normalized_rows = _ensure_top10_rows(normalized_rows, requested_symbols=requested_symbols or [], top_n=top_n, schema_keys=ks, schema_headers=hdrs, preserve_order=top10_preserve_order)
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

async def _build_special_page_payload(
    *,
    page: str,
    merged_body: Mapping[str, Any],
    mode: str,
    limit: int,
    offset: int,
    top_n: int,
    requested_symbols: Sequence[str],
) -> Optional[Dict[str, Any]]:
    """v4.5.0 [BRIDGE-FIX]: invoke the REAL builder for a derived special page.

    Returns the builder's raw envelope dict (to be projected by
    `_normalize_external_payload`), or None when the page is not a
    builder-backed special page, the builder is unbound, it raises, or it
    yields nothing. NEVER raises — every failure degrades to None so the caller
    falls through to the engine path and then the existing fallback.

    Top_10: `build_top10_rows` returns a coroutine inside the running loop, so
    we `_maybe_await` it (the un-awaited-coroutine bug fixed here). We pass
    `symbols` only when the request explicitly supplied them, so a bare Top_10
    request performs the full-universe selection rather than being constrained.

    Insights: `build_insights_analysis_rows` is async + keyword-only; we hand it
    the resolved v2 engine when available (it is engine-optional and will
    self-resolve its universe otherwise).
    """
    try:
        if page == _TOP10_PAGE and _build_top10_rows is not None:
            kwargs: Dict[str, Any] = {"limit": max(top_n, limit), "mode": mode or ""}
            syms = [s for s in (requested_symbols or []) if s]
            if syms:
                kwargs["symbols"] = list(syms)
                kwargs["direct_symbols"] = list(syms)
            res = _build_top10_rows(**kwargs)
            res = await _maybe_await(res)
            if isinstance(res, dict) and _extract_rows_like(res):
                return res
            return None

        if page == _INSIGHTS_PAGE and _build_insights_rows is not None:
            engine = None
            try:
                engine = await _get_v2_engine_instance()
            except Exception:
                engine = None
            syms = [s for s in (requested_symbols or []) if s]
            res = await _build_insights_rows(
                engine=engine,
                symbols=(list(syms) or None),
                mode=mode or "",
            )
            if isinstance(res, dict) and _extract_rows_like(res):
                return res
            return None
    except Exception as e:
        logger.warning(
            "[advanced_analysis v%s] special-page builder for %s failed (%s: %s); "
            "falling through to engine/fallback path",
            ADVANCED_ANALYSIS_VERSION, page, e.__class__.__name__, e,
        )
        return None

    return None


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
                  "headers_only": headers_only, "schema_only": schema_only,
                  "contract_len": len(keys)},
        )

    # v4.1.0 [ADD-E]: fetch provider_health snapshot ONCE per request.
    # Total budget = single async call to engine.health() (no upstream API).
    # Forwarded into both success and fail-soft envelopes so every response
    # carries the engine's current view of provider health at request time.
    provider_health = await _extract_provider_health_snapshot()

    # v4.5.0 [BRIDGE-FIX]: dispatch derived special pages to their REAL builders
    # FIRST (mirrors the v4.4.0 Data_Dictionary precedence). The live engine
    # returns only a 1-row envelope for Top_10 / Insights, which is non-empty
    # and would otherwise pre-empt the real multi-row builder result. Fail-soft:
    # a None return (unbound / empty / raised) falls straight through to the
    # engine path below, then to the existing non-empty fallback.
    if page in (_TOP10_PAGE, _INSIGHTS_PAGE):
        special_payload = await _build_special_page_payload(
            page=page, merged_body=merged_body, mode=mode or "",
            limit=limit, offset=offset, top_n=top_n,
            requested_symbols=requested_symbols,
        )
        if isinstance(special_payload, dict):
            special_dispatch = "top10_selector_real" if page == _TOP10_PAGE else "insights_builder_real"
            special_source = (
                "core.analysis.top10_selector.build_top10_rows"
                if page == _TOP10_PAGE
                else "core.analysis.insights_builder.build_insights_analysis_rows"
            )
            normalized_special = _normalize_external_payload(
                external_payload=special_payload, page=page, headers=headers, keys=keys,
                include_matrix=include_matrix, request_id=request_id, started_at=start,
                mode=mode, limit=limit, offset=offset, top_n=top_n,
                requested_symbols=requested_symbols,
                meta_extra={"schema_source": schema_source,
                            "source": special_source,
                            "dispatch": special_dispatch},
                provider_health=provider_health,
                top10_preserve_order=(page == _TOP10_PAGE),
            )
            if _extract_rows_like(normalized_special):
                return normalized_special

    # v4.11.0 [EDGE-TIMEOUT-GUARD]: see CHANGELOG. Bound the engine call so a
    # slow/throttled batch degrades to the EXISTING fail-soft path below
    # instead of running until Render's platform-level edge timeout kills the
    # whole request (which never gives this handler's fail-soft code a chance
    # to run -- the caller just sees a raw connection failure). Default OFF ->
    # the else-branch below is byte-identical to v4.10.0.
    if _adv_engine_call_timeout_enabled():
        try:
            payload, source = await asyncio.wait_for(
                _call_core_sheet_rows_best_effort(
                    page=page, limit=max(limit + offset, top_n), offset=0,
                    mode=mode or "", body=merged_body,
                ),
                timeout=_adv_engine_call_timeout_s(),
            )
        except asyncio.TimeoutError:
            logger.warning(
                "[advanced_analysis v%s] engine call timed out after %.1fs for page=%s "
                "(edge-timeout guard); falling through to fail-soft",
                ADVANCED_ANALYSIS_VERSION, _adv_engine_call_timeout_s(), page,
            )
            payload, source = (
                {"status": "error", "error": "engine_call_timeout", "row_objects": []},
                CORE_GET_SHEET_ROWS_SOURCE,
            )
    else:
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

    Operators monitoring this endpoint can now see at a glance whether the
    upstream-provider chain is healthy:
      - provider_unhealthy_count == 0 -> all providers healthy
      - provider_unhealthy_count > 0  -> engine is demoting/skipping at least
        one provider; check /v1/schema/provider-health for detail.

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
        # v4.2.0/v4.3.0: expose the registry-derived contract widths this route
        # is serving so operators can confirm whether Top_10 is at the full
        # canonical width (118) or still tracking a stale registry. The static
        # fallback values are now 115 / 118 (v4.3.0).
        "contract_widths": {
            "instrument_static_fallback": _CANONICAL_INSTRUMENT_LEN,
            "top10_static_fallback": _TOP10_STATIC_LEN,
            "market_leaders_effective": _expected_len("Market_Leaders"),
            "top10_effective": _expected_len(_TOP10_PAGE),
            "insights_effective": _expected_len(_INSIGHTS_PAGE),
            "dictionary_effective": _expected_len(_DICTIONARY_PAGE),
        },
        "allowed_pages_count": len(_safe_allowed_pages()),
        "path": str(getattr(getattr(request, "url", None), "path", "")),
    })


@router.get("/schema/provider-health")
@router.get("/v1/schema/provider-health")
async def schema_provider_health(request: Request) -> Dict[str, Any]:
    """v4.1.0 [ADD-G]: Dedicated provider-health snapshot endpoint.

    Returns the engine's `provider_unhealthy_markers` registry without
    triggering a sheet build. Cheap polling target for Apps Script
    `12_Diagnostics.gs runFullDiagnosticSweep_()` — one HTTP call instead of
    one-per-page to discover systemic provider outages.

    On v2-engine-unavailable / no-registry-in-health, returns
    `status: "unavailable"` with a `reason` field instead of raising. Always
    returns 200; consumers should inspect the `status` field rather than
    relying on HTTP codes.
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
    # v4.4.0 [DD-FIX (d)]: this endpoint previously returned a schema-only
    # payload (headers + keys + columns, but ZERO content rows) — the
    # "200 success / 0 rows" symptom the frontend was hitting. The real
    # Data_Dictionary content (one row per schema column across every sheet) is
    # now attached from the pure / import-safe core.sheets.data_dictionary
    # builder via _real_data_dictionary_rows(). Row keys already equal
    # _DICTIONARY_KEYS, but they are still projected through
    # _normalize_to_schema_keys for header-fallback safety. Fail-soft: if the
    # builder is unavailable or raises, the response degrades to the prior
    # schema-only payload (never raises, never regresses below v4.3.0 behavior).
    payload = _schema_spec_payload(_DICTIONARY_PAGE)
    payload["page"] = _DICTIONARY_PAGE
    payload["sheet"] = _DICTIONARY_PAGE
    payload["sheet_name"] = _DICTIONARY_PAGE
    try:
        keys = payload.get("keys") or []
        hdrs = payload.get("headers") or []
        real_rows = _real_data_dictionary_rows()
        if real_rows and keys:
            normalized = [
                _normalize_to_schema_keys(schema_keys=keys, schema_headers=hdrs, raw=r)
                for r in real_rows
            ]
            matrix = _rows_to_matrix(normalized, keys)
            payload["row_objects"] = normalized
            payload["items"] = normalized
            payload["records"] = normalized
            payload["data"] = normalized
            payload["quotes"] = normalized
            payload["rows"] = matrix
            payload["rows_matrix"] = matrix
            payload["matrix"] = matrix
            payload["count"] = len(normalized)
            meta = payload.get("meta")
            if isinstance(meta, dict):
                meta["dispatch"] = "schema_data_dictionary_real_rows"
                meta["row_source"] = "core.sheets.data_dictionary.build_data_dictionary_payload"
                meta["count"] = len(normalized)
    except Exception as e:
        logger.warning(
            "[advanced_analysis v%s] schema_data_dictionary real-rows attach failed: %s: %s",
            ADVANCED_ANALYSIS_VERSION, e.__class__.__name__, e,
        )
    return _json_safe(payload)

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


# =============================================================================
# v4.7.0 [OPP-ADD] POST /opportunity-candidates — Plan v5.0 §5 zone payload.
#
# Bridges the v4.5.0 selector binding (candidate pool) into the bound
# opportunity builder (gates §4.2 / score §4.3 / wealth math §4.4 / L7
# funding). The §5 payload shape is FROZEN by the builder; this endpoint only
# adds meta.provider_health and meta.route (additive, inside meta). Fail-soft
# everywhere except auth (401).
# =============================================================================
_OPP_CRITERIA_ALIASES: Dict[str, str] = {
    "universescope": "universe_scope",
    "maxselected": "max_selected",
    "period": "period_months",
    "periodmonths": "period_months",
    "requiredroi": "required_roi_pct",
    "requiredroipct": "required_roi_pct",
    "requiredannualizedroi": "required_ann_roi_pct",
    "requiredannualizedroipct": "required_ann_roi_pct",
    "requiredannroipct": "required_ann_roi_pct",
    "riskprofile": "risk_profile",
    "minreliability": "min_reliability",
    "mindataquality": "min_dq",
    "mindq": "min_dq",
    "minriskreward": "min_rr",
    "minrr": "min_rr",
    "maxrisklevel": "max_risk_level",
    "allowconflict": "allow_conflict",
    "allownegativenews": "allow_negative_news",
    "allownegativesector": "allow_negative_sector",
    "maxpersector": "max_per_sector",
    "maxsector": "max_per_sector",
    "maxpermarket": "max_per_market",
    "maxmarket": "max_per_market",
    "includeportfolioholdings": "include_portfolio_holdings",
    "basecurrency": "base_currency",
    "maxweightpct": "max_weight_pct",
    "maxweight": "max_weight_pct",
    "lotsize": "lot_size",
    "nearmissn": "near_miss_n",
    "reviewdays": "review_days",
}


def _opp_criteria_from_body(raw: Any) -> Dict[str, Any]:
    """v4.7.0: map GAS control-panel labels ("Max Selected",
    "T10: Required ROI %") or snake_case keys onto opportunity_builder
    criteria keys. Unknown keys pass through snake_cased — the builder
    ignores what it doesn't recognize, so this can't reject a panel."""
    out: Dict[str, Any] = {}
    if not isinstance(raw, Mapping):
        return out
    for k, v in raw.items():
        token = _strip(k)
        if not token:
            continue
        if token.lower().startswith("t10:"):
            token = token[4:]
        compact = re.sub(r"[^a-z0-9]+", "", token.lower())
        key = _OPP_CRITERIA_ALIASES.get(compact) or _normalize_key_name(token)
        if key:
            out[key] = v
    return out


async def _opp_collect_pool(*, pool_limit: int, mode: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any], str]:
    """v4.7.0: candidate pool via the v4.5.0 selector binding.

    Returns (rows, selector_meta, pool_source). pool_source is one of
    selector / selector_empty / selector_error / selector_unbound. NEVER
    raises — every failure degrades to an empty pool so the builder can
    answer honestly ("no_candidates")."""
    if _build_top10_rows is None:
        return [], {}, "selector_unbound"
    try:
        res = _build_top10_rows(limit=pool_limit, mode=mode or "")
        res = await _maybe_await(res)
    except Exception as e:
        logger.warning(
            "[advanced_analysis v%s] opportunity pool: selector raised %s: %s",
            ADVANCED_ANALYSIS_VERSION, e.__class__.__name__, e,
        )
        return [], {"error": "{}: {}".format(e.__class__.__name__, e)}, "selector_error"
    if not isinstance(res, Mapping):
        return [], {}, "selector_empty"
    rows = _extract_rows_like(res)
    meta = res.get("meta") if isinstance(res.get("meta"), Mapping) else {}
    return rows, dict(meta or {}), ("selector" if rows else "selector_empty")


@router.post("/sheet-rows/opportunity-candidates")
@router.post("/opportunity-candidates")
@router.post("/v1/opportunity-candidates")
async def opportunity_candidates_post(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: str = Query(default=""),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    """v4.7.0 [OPP-ADD]: §5 zone payload for the Top_10 decision page.

    Body fields (all optional):
      criteria   : dict — GAS panel values; labels or snake_case (see
                   _opp_criteria_from_body). Echoed in meta.criteria_snapshot.
      fx_rates   : dict ccy -> SAR rate, read by GAS from _Lists_Config
                   TFB_FX_LOOKUP (the backend never reads the spreadsheet).
      portfolio  : dict — cash_available_sar, pending_proceeds_sar,
                   portfolio_value_sar, holdings[{symbol, sector, market,
                   value_sar}].
      rows       : explicit candidate rows (bypasses the selector pool —
                   used by tests and GAS replays).
      pool_limit : selector pool size (default 60, clamp 1..500).

    Fail-soft: builder unbound -> status "unavailable"; selector failures
    degrade to an empty pool; the only raise is auth (401).
    """
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
    criteria = _opp_criteria_from_body(merged_body.get("criteria"))
    fx_rates = merged_body.get("fx_rates") if isinstance(merged_body.get("fx_rates"), Mapping) else {}
    portfolio = merged_body.get("portfolio") if isinstance(merged_body.get("portfolio"), Mapping) else {}
    # v8.12.0 (Fix AE -- FULL-UNIVERSE SCAN): pool_limit was clamped to a hard
    # ceiling of 500, capping the candidate pool well below the ~2,189-row live
    # universe even when the operator raised the panel's "Pool Limit" cell. The
    # ceiling now follows TFB_TOP10_POOL_CEILING (explicit override) OR rises
    # automatically to 20000 when TFB_TOP10_FULL_UNIVERSE is on, so the selector
    # can ingest every page. Default (both unset) = 500, byte-identical clamp.
    # NOTE: the panel "Pool Limit" operator cell still drives the actual count --
    # set it to ~2200 (or blank to inherit the larger default) to scan all names.
    _full_universe = (os.getenv("TFB_TOP10_FULL_UNIVERSE") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
    _pool_ceiling = _maybe_int(os.getenv("TFB_TOP10_POOL_CEILING"), 20000 if _full_universe else 500)
    if _pool_ceiling < 1:
        _pool_ceiling = 20000 if _full_universe else 500
    _pool_default = 3000 if _full_universe else 60
    pool_limit = max(1, min(_pool_ceiling, _maybe_int(merged_body.get("pool_limit"), _pool_default)))

    if _build_opportunity_payload is None:
        return _json_safe({
            "version": None,
            "status": "unavailable",
            "message": "core.analysis.opportunity_builder not deployed",
            "kpis": {}, "selected": [], "near_miss": [], "alerts": [],
            "candidates_rows": [],
            "meta": {
                "criteria_snapshot": criteria,
                "route": {
                    "version": ADVANCED_ANALYSIS_VERSION,
                    "request_id": request_id,
                    "dispatch": "opportunity_candidates_unavailable",
                    "duration_ms": round((time.time() - start) * 1000.0, 3),
                },
            },
        })

    explicit_rows = merged_body.get("rows")
    if isinstance(explicit_rows, list) and explicit_rows and isinstance(explicit_rows[0], Mapping):
        pool_rows: List[Dict[str, Any]] = [dict(r) for r in explicit_rows]
        sel_meta: Dict[str, Any] = {}
        pool_source = "body_rows"
    else:
        pool_rows, sel_meta, pool_source = await _opp_collect_pool(
            pool_limit=pool_limit, mode=mode or _strip(merged_body.get("mode")),
        )

    # v4.9.0 [TREND-ADD]: cross-sectional sector-trend enrichment (env-gated
    # inside the module; default OFF -> rows pass through untouched).
    trends_meta: Dict[str, Any] = {"enabled": False, "bound": False}
    if _enrich_rows_with_trends is not None:
        try:
            pool_rows, trends_meta = _enrich_rows_with_trends(pool_rows)
            trends_meta = dict(trends_meta or {})
            trends_meta["bound"] = True
        except Exception as _te:
            logger.warning(
                "[advanced_analysis v%s] trend enrichment raised %s: %s — pool used unenriched",
                ADVANCED_ANALYSIS_VERSION, _te.__class__.__name__, _te,
            )
            trends_meta = {"enabled": False, "bound": True,
                           "error": "{}: {}".format(_te.__class__.__name__, str(_te)[:160])}

    provider_health = await _extract_provider_health_snapshot()
    engine_version = provider_health.get("engine_version") if isinstance(provider_health, Mapping) else None

    # v4.7.2 [META-MAP]: live selector v4.19.0 key names first, prior names as
    # fallbacks. budget is normalized to a dict with an "exhausted" flag so the
    # builder's budget alert logic fires from the live boolean.
    _sel_budget = sel_meta.get("budget") or sel_meta.get("budget_meta")
    if not isinstance(_sel_budget, Mapping):
        _sel_budget = {}
    _budget_norm: Dict[str, Any] = dict(_sel_budget)
    if "exhausted" not in _budget_norm and sel_meta.get("budget_exhausted") is not None:
        _budget_norm["exhausted"] = bool(sel_meta.get("budget_exhausted"))
    if sel_meta.get("universe_starved") is not None:
        _budget_norm.setdefault("universe_starved", bool(sel_meta.get("universe_starved")))
    upstream_meta: Dict[str, Any] = {
        "coverage": sel_meta.get("page_coverage") or sel_meta.get("coverage"),
        "budget": _budget_norm or None,
        "timeouts": sel_meta.get("timeouts"),
        "freshness": sel_meta.get("freshness") or sel_meta.get("data_as_of") or sel_meta.get("generated_at"),
        "versions": {
            "selector": sel_meta.get("selector_version") or sel_meta.get("version"),
            "engine": engine_version,
        },
    }

    try:
        payload = _build_opportunity_payload(
            pool_rows,
            criteria=criteria,
            portfolio=dict(portfolio or {}),
            fx_rates=dict(fx_rates or {}),
            upstream_meta=upstream_meta,
        )
    except Exception as e:
        logger.warning(
            "[advanced_analysis v%s] opportunity builder raised %s: %s",
            ADVANCED_ANALYSIS_VERSION, e.__class__.__name__, e,
        )
        payload = {
            "version": _OPP_BUILDER_VERSION,
            "status": "error",
            "message": "{}: {}".format(e.__class__.__name__, str(e)[:200]),
            "kpis": {}, "selected": [], "near_miss": [], "alerts": [],
            "candidates_rows": [],
            "meta": {"criteria_snapshot": criteria},
        }
    if not isinstance(payload, dict):
        payload = {"status": "error", "message": "builder returned non-dict",
                   "kpis": {}, "selected": [], "near_miss": [], "alerts": [],
                   "candidates_rows": [], "meta": {}}
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        meta = {}
        payload["meta"] = meta
    meta["provider_health"] = _json_safe(provider_health)
    # v4.10.0 [NEWS-DISPLAY]: fill the News column for the surfaced decision
    # symbols (display-only; the builder's selection above is already final).
    # Env-gated default OFF -> no fetch, no mutation, byte-identical to v4.9.0.
    news_display_meta: Dict[str, Any] = {"enabled": False}
    if _news_display_enabled():
        try:
            news_display_meta = await _attach_news_display(payload)
        except Exception as _nde:
            news_display_meta = {"enabled": True,
                                 "error": "{}: {}".format(_nde.__class__.__name__, str(_nde)[:160])}
    meta["route"] = {
        "version": ADVANCED_ANALYSIS_VERSION,
        "opportunity_builder_version": _OPP_BUILDER_VERSION,
        "request_id": request_id,
        "dispatch": "opportunity_candidates",
        "pool": {"source": pool_source, "count": len(pool_rows), "pool_limit": pool_limit},
        "trends": _json_safe(trends_meta),
        "news_display": _json_safe(news_display_meta),
        "duration_ms": round((time.time() - start) * 1000.0, 3),
    }
    return _json_safe(payload)


# =============================================================================
# v4.8.0 [PF-ADD] POST /portfolio-actions — Plan v5.0 My_Portfolio payload.
#
# Bridges GAS-supplied holdings rows into the bound portfolio_actions builder
# (action truth table / L7 funding / L8 advisor notes). The payload shape is
# owned by the builder; this endpoint only adds meta.provider_health and
# meta.route (additive, inside meta). No selector path — holdings come
# exclusively from the spreadsheet. Fail-soft everywhere except auth (401).
# =============================================================================
_PF_CONTROLS_ALIASES: Dict[str, str] = {
    "cashavailablesar": "cash_available_sar",
    "cashavailable": "cash_available_sar",
    "cashsar": "cash_available_sar",
    "targetcash": "target_cash_pct",
    "targetcashpct": "target_cash_pct",
    "maxposition": "max_position_pct",
    "maxpositionpct": "max_position_pct",
    "maxsector": "max_sector_pct",
    "maxsectorpct": "max_sector_pct",
    "minreliabilitytoadd": "min_reliability_add",
    "minreliability": "min_reliability_add",
    "mindqtoadd": "min_dq_add",
    "mindataqualitytoadd": "min_dq_add",
    "mindataquality": "min_dq_add",
    "mindq": "min_dq_add",
    "rebalancemode": "rebalance_mode",
    "addroipct": "add_roi_pct",
    "addroi": "add_roi_pct",
    "trimroipct": "trim_roi_pct",
    "exitroipct": "exit_roi_pct",
    "reviewdays": "review_days",
    "lotsize": "lot_size",
    "periodmonths": "period_months",
    "period": "period_months",
}


def _pf_controls_from_body(raw: Any) -> Dict[str, Any]:
    """v4.8.0: map GAS PF-panel labels ("PF: Cash Available SAR",
    "PF: Rebalance Mode") or snake_case keys onto portfolio_actions controls
    keys. Unknown keys pass through snake_cased — the builder ignores what it
    doesn't recognize, so this can't reject a panel. NOTE: "minreliability"
    intentionally maps differently here (min_reliability_add) than in
    _OPP_CRITERIA_ALIASES (min_reliability) — separate panels, separate maps."""
    out: Dict[str, Any] = {}
    if not isinstance(raw, Mapping):
        return out
    for k, v in raw.items():
        token = _strip(k)
        if not token:
            continue
        if token.lower().startswith("pf:"):
            token = token[3:]
        compact = re.sub(r"[^a-z0-9]+", "", token.lower())
        key = _PF_CONTROLS_ALIASES.get(compact) or _normalize_key_name(token)
        if key:
            out[key] = v
    return out


@router.post("/sheet-rows/portfolio-actions")
@router.post("/portfolio-actions")
@router.post("/v1/portfolio-actions")
async def portfolio_actions_post(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    """v4.8.0 [PF-ADD]: action payload for the My_Portfolio decision page.

    Body fields:
      rows     : holdings dicts from 10_My_Portfolio.gs — the Top_10 pool
                 display headers PLUS "Quantity" and "Buy Price" (average
                 cost, native ccy, from _Portfolio_CostBasis). Empty/absent
                 rows -> the builder answers status="empty" honestly (L13).
      controls : dict — PF panel values; labels or snake_case (see
                 _pf_controls_from_body). Echoed in meta.controls_snapshot.
      fx_rates : dict ccy -> SAR rate, read by GAS from _Lists_Config
                 TFB_FX_LOOKUP (the backend never reads the spreadsheet).

    Fail-soft: builder unbound -> status "unavailable"; builder exceptions ->
    status "error" (inside the builder); the only raise is auth (401).
    """
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
    controls = _pf_controls_from_body(merged_body.get("controls"))
    fx_rates = merged_body.get("fx_rates") if isinstance(merged_body.get("fx_rates"), Mapping) else {}
    raw_rows = merged_body.get("rows")
    holdings: List[Dict[str, Any]] = []
    if isinstance(raw_rows, list):
        holdings = [dict(r) for r in raw_rows if isinstance(r, Mapping)]

    if _build_portfolio_actions is None:
        return _json_safe({
            "version": None,
            "status": "unavailable",
            "message": "core.analysis.portfolio_actions not deployed",
            "kpis": {}, "actions": [], "sector_summary": [], "alerts": [],
            "meta": {
                "controls_snapshot": controls,
                "route": {
                    "version": ADVANCED_ANALYSIS_VERSION,
                    "request_id": request_id,
                    "dispatch": "portfolio_actions_unavailable",
                    "duration_ms": round((time.time() - start) * 1000.0, 3),
                },
            },
        })

    provider_health = await _extract_provider_health_snapshot()
    engine_version = provider_health.get("engine_version") if isinstance(provider_health, Mapping) else None
    upstream_meta: Dict[str, Any] = {
        "versions": {"engine": engine_version},
        "rows_supplied_by": "gas_sheets",
    }

    try:
        payload = _build_portfolio_actions(
            holdings,
            controls=controls,
            fx_rates=dict(fx_rates or {}),
            upstream_meta=upstream_meta,
        )
    except Exception as e:
        logger.warning(
            "[advanced_analysis v%s] portfolio actions builder raised %s: %s",
            ADVANCED_ANALYSIS_VERSION, e.__class__.__name__, e,
        )
        payload = {
            "version": _PF_ACTIONS_VERSION,
            "status": "error",
            "message": "{}: {}".format(e.__class__.__name__, str(e)[:200]),
            "kpis": {}, "actions": [], "sector_summary": [], "alerts": [],
            "meta": {"controls_snapshot": controls},
        }
    if not isinstance(payload, dict):
        payload = {"status": "error", "message": "builder returned non-dict",
                   "kpis": {}, "actions": [], "sector_summary": [],
                   "alerts": [], "meta": {}}
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        meta = {}
        payload["meta"] = meta
    meta["provider_health"] = _json_safe(provider_health)
    meta["route"] = {
        "version": ADVANCED_ANALYSIS_VERSION,
        "portfolio_actions_version": _PF_ACTIONS_VERSION,
        "request_id": request_id,
        "dispatch": "portfolio_actions",
        "holdings": {"count": len(holdings)},
        "duration_ms": round((time.time() - start) * 1000.0, 3),
    }
    return _json_safe(payload)

__all__ = ["router", "ADVANCED_ANALYSIS_VERSION", "_run_advanced_sheet_rows_impl"]
