#!/usr/bin/env python3
# routes/advanced_analysis.py
"""
================================================================================
Advanced Analysis Root Owner — v4.3.3  (V2.7.0-ALIGNED / 97-COL / v5.50.0)
================================================================================
ROOT SHEET-ROWS OWNER • SCHEMA-FIRST • FAIL-SOFT • STABLE ENVELOPE • JSON-SAFE
GET+POST MERGED • HEADERS-ONLY / SCHEMA-ONLY • CANONICAL WIDTHS • OWNER-ALIGNED

Purpose
-------
Owns the canonical root paths:
- /sheet-rows
- /schema
- /schema/sheet-spec
- /schema/pages
- /schema/data-dictionary
and their /v1/schema aliases.

Why v4.3.3 — diagnostic visibility through the fallback path
------------------------------------------------------------

v4.3.2 added matrix-aware row extraction AND an `engine_payload_diagnostic`
field intended to surface the raw engine payload shape when extraction
returned 0 rows. The deploy succeeded (binding logs as expected) but the
data response STILL showed:

  status: "partial", dispatch: "fail_soft_nonempty",
  data_provider: "placeholder_no_live_data",
  duration_ms: 1.487

And critically, the `engine_payload_diagnostic` field was MISSING from
`meta`. Re-reading the impl exposed the bug:

  payload, source = await _call_core_sheet_rows_best_effort(...)
  if isinstance(payload, dict):
      normalized = _normalize_external_payload(...)        ← diagnostic captured here
      if _extract_rows_like(normalized):
          return normalized                                 ← only returned on success
  fallback_rows = _build_nonempty_failsoft_rows(...)        ← discards `normalized`
  return _payload_envelope(..., meta={...})                 ← fresh meta, diagnostic lost

The diagnostic was attached to `normalized.meta`, but the impl only
returns `normalized` when `_extract_rows_like(normalized)` is truthy.
On the empty path, `normalized` is discarded and a fresh fallback
envelope is built — which is why the diagnostic never reached the wire.

v4.3.3 changes (from v4.3.2)
----------------------------
- FIX: `_run_advanced_sheet_rows_impl` now CAPTURES the upstream
    diagnostic before falling through to fallback rows, and merges it
    into the fallback meta. Exposed fields:
      * `upstream_payload_type`     — type(payload).__name__
      * `upstream_payload_top_keys` — first 25 top-level keys
      * `upstream_status`           — engine-reported status
      * `upstream_error`            — engine-reported error (if any)
      * `engine_payload_diagnostic` — bucket type/length summary from v4.3.2

- ADD: explicit WARNING-level log when `_call_core_sheet_rows_best_effort`
    returns a payload but the route can't extract rows from it. The log
    line includes the payload top keys and the bucket summary, so we get
    diagnostic evidence in the Render deploy logs without needing
    additional API calls.

- ADD: explicit WARNING-level log inside `_call_core_sheet_rows_best_effort`
    when the engine raises a non-TypeError exception. v4.3.1/4.3.2
    swallowed those exceptions silently into an `{"status": "error"}`
    dict and returned, but the swallowed exception text was visible only
    in the discarded normalized envelope.

- KEEP: every v4.3.2 / v4.3.1 / v4.3.0 fix preserved unchanged. Matrix
    extraction, v2 binding cascade, 97-col canonical — all intact.

This is purely an observability revision. If v4.3.3's response STILL
shows `status: partial` (likely), the new fields in `meta` will tell
us exactly what the engine returned, and the deploy logs will show
the engine call's exception (if any). With that data in hand we can
write v4.3.4 as a precise behavioral fix.

Why v4.3.2 — matrix row-extraction parity with legacy adapter
-------------------------------------------------------------

After v4.3.1 deployed successfully (engine bound via `get_engine()` factory,
visible in startup log and meta.source), the data verification revealed
that `/v1/advanced/sheet-rows` was returning placeholder rows with all
v5.50.0 fields null:

  meta.source: "core.data_engine_v2.get_engine().get_sheet_rows"  ✓
  meta.dispatch: "advanced_analysis_fail_soft_nonempty"           ✗
  data_provider: "placeholder_no_live_data"                       ✗
  duration_ms: 1.037                                              ⚡ suspicious

The 1ms response time and `fail_soft_nonempty` dispatch label proved the
engine was returning a payload but the route was failing to extract any
rows from it, then falling through to the local placeholder fallback.

Comparing the v4.2.0 path (which DID populate `recommendation_detailed`)
to the v4.3.1 path (which did NOT) revealed the root cause:

The v5.50.0 engine returns rows in MATRIX form — `payload["rows"]` is a
list of lists positionally keyed to `payload["keys"]`, not a list of dicts.
The legacy adapter `core.data_engine.get_sheet_rows` includes a helper
`_extract_payload_rows` with built-in matrix-to-dicts conversion using
`_rows_from_matrix(rr, keys_hint)`. v4.3.1 went direct to v2 and bypassed
that helper, then this route's local `_extract_rows_like` (which only
recognized list-of-dicts) returned `[]` and the fallback fired.

v4.3.2 changes (from v4.3.1)
----------------------------
- FIX [CRITICAL]: add `_rows_from_matrix()` helper symmetric with the
    legacy adapter's logic. Converts list-of-lists rows into dict rows
    using the provided keys, with safe handling for short/long rows.

- FIX [CRITICAL]: add `_extract_rows_with_matrix_fallback(payload, keys)`
    which first tries `_extract_rows_like` (preserves v4.3.1 behavior for
    payloads that were already list-of-dicts), then falls back to matrix
    extraction when the payload contains list-of-lists in any of the
    standard buckets ("rows", "rows_matrix", "matrix", "data", "items",
    "records", "quotes", "results").

- FIX [CRITICAL]: `_normalize_external_payload` now calls the matrix-
    aware extractor and passes the schema keys. This is the single
    behavioral change that unblocks v5.50.0 enrichment from reaching
    the wire.

- KEEP: existing `_extract_rows_like` is preserved unchanged. It is
    still used by `_run_advanced_sheet_rows_impl` for the post-normalize
    "did this produce rows" check (which reads `row_objects` in the
    normalized envelope — always list-of-dicts at that stage).

- KEEP: every v4.3.1 / v4.3.0 / v4.2.0 / v4.1.0 fix preserved unchanged.

Production verification after v4.3.2 deploy
-------------------------------------------
Hit `/v1/advanced/sheet-rows?sheet=Market_Leaders&limit=1` and the
response should now satisfy:

  1. meta.source     == "core.data_engine_v2.get_engine().get_sheet_rows"  (unchanged from 4.3.1)
  2. meta.dispatch   == "advanced_analysis_root"                          (was: fail_soft_nonempty)
  3. status          == "success"                                         (was: partial)
  4. data_provider   non-null and NOT "placeholder_no_live_data"
  5. recommendation_detailed / recommendation_priority — non-null
  6. fundamental_view / technical_view / risk_view / value_view — non-null
  7. recommendation_reason — non-null
  8. sector_relative_score / conviction_score — non-null
  9. candlestick_signal — non-null on most symbols (some short-history symbols may legitimately be null)

If conditions 1-3 pass but 4-9 still fail → engine bug.
If condition 1 fails → v4.3.2 not actually deployed.
If condition 2 still says fail_soft_nonempty → matrix extraction not catching this payload shape; need to inspect `meta.engine_payload_diagnostic` (added below for 4.3.2 visibility).

Why v4.3.1 — v5.50.0 binding pattern corrected
----------------------------------------------

v4.3.0 inverted the engine import preference (v2 first, legacy fallback) but
attempted the import as `from core.data_engine_v2 import get_sheet_rows`.
The deploy log immediately revealed the issue:

  WARNING | routes.advanced_analysis | [advanced_analysis v4.3.0]
  core.data_engine_v2 unavailable (cannot import name 'get_sheet_rows'
  from 'core.data_engine_v2'); falling back to legacy core.data_engine

Inspection of `core/data_engine_v2.py` confirmed: `get_sheet_rows` is an
ASYNC METHOD on the `DataEngineV5` class, not a module-level function.
The module's `__all__` exports `DataEngineV5`, `get_engine` (async factory),
`get_engine_if_ready` (sync factory), and supporting constants — but NOT
`get_sheet_rows` itself. The startup log corroborates: `engine_source:
core.data_engine_v2` and `engine_ready: true`, so the v2 engine IS deployed
and live; only the route's binding pattern was wrong.

v4.3.1 changes (from v4.3.0)
----------------------------
- FIX [CRITICAL]: engine binding now uses `get_engine()` async factory
    instead of trying to import a non-existent module-level `get_sheet_rows`.
    A small adapter coroutine retrieves the engine and calls its async
    `.get_sheet_rows()` method, keeping the existing
    `_call_core_sheet_rows_best_effort` candidate-signature loop unchanged.

- ADD: cascading binding probe with explicit logging at each step:
    1. Try `from core.data_engine_v2 import get_sheet_rows` (top-level fn,
       in case a future v2 build adds it). Logs INFO if bound.
    2. Try `from core.data_engine_v2 import get_engine` (async factory) and
       wrap. Logs INFO if bound — this is the expected production path.
    3. Try `from core.data_engine_v2 import get_engine_if_ready` (sync
       factory) with async-factory fallback for cold starts. Logs INFO if
       bound. Belt-and-suspenders against the rare race where v2 module
       imported but engine instance not yet warmed.
    4. Fall back to `from core.data_engine import get_sheet_rows` (legacy).
       Logs WARNING — this branch loses v5.50.0 enrichment.
    5. All four failed: log ERROR. `core_get_sheet_rows is None`.

- KEEP: `CORE_GET_SHEET_ROWS_SOURCE` reflects the actual binding chosen.
    Production responses' `meta.source` will now read one of:
      * `core.data_engine_v2.get_sheet_rows` (top-level — unlikely)
      * `core.data_engine_v2.get_engine().get_sheet_rows` (expected)
      * `core.data_engine_v2.get_engine_if_ready().get_sheet_rows`
      * `core.data_engine.get_sheet_rows` (legacy fallback — bug indicator)

- KEEP: every other v4.3.0 change preserved unchanged. 97-col canonical
    fallback, `_EXPECTED_SHEET_LENGTHS` 97/100, all v4.2.0/v4.1.0 fixes.

Production verification after v4.3.1 deploy
-------------------------------------------
After Render redeploy, the startup log should now show:

  INFO | routes.advanced_analysis | [advanced_analysis v4.3.1]
  engine bound via core.data_engine_v2.get_engine() (async factory)

And NOT show the v4.3.0 warning about `cannot import name 'get_sheet_rows'`.
Then hit `/v1/advanced/sheet-rows?sheet=Market_Leaders&limit=1` and verify
the same 7 conditions documented in the v4.3.0 narrative below.

Why v4.3.0 — v5.50.0 ROUTING FIX (engine bypass corrected)
----------------------------------------------------------

Critical observation that motivated this revision:

After deploying v5.50.0 (`core/data_engine_v2`) and registry v2.7.0 (97
canonical columns), production responses on /v1/advanced/sheet-rows showed:

  * Schema correct: 97 keys returned including all 7 new fields ✓
  * `recommendation` populated, `recommendation_detailed` populated,
    `recommendation_priority` populated ✓
  * `fundamental_view`, `technical_view`, `risk_view`, `value_view` ALL NULL ✗
  * `sector_relative_score`, `conviction_score`, `top_factors`, `top_risks`,
    `position_size_hint` ALL NULL ✗
  * `candlestick_pattern`, `candlestick_signal`, `candlestick_strength`,
    `candlestick_confidence`, `candlestick_patterns_recent` ALL NULL ✗
  * `recommendation_reason` NULL ✗
  * Response meta: `"source": "core.data_engine.get_sheet_rows"`

The meta key "source" pinpoints the bug: this route was importing the LEGACY
`core.data_engine` adapter ahead of `core.data_engine_v2`. The legacy module
in turn delegates to v2 internally, but its schema-projection layer
(`_normalize_sheet_payload` → `_project_row`) only forwards keys that the
LIVE schema_registry exposes, which works for the 97 keys themselves but
DOES NOT trigger the v2 engine's full enrichment pipeline (Insights builder,
Views computer, Candlestick detector, recommendation_reason synthesizer).

v4.3.0 changes (from v4.2.0)
----------------------------
- FIX [CRITICAL]: engine import preference INVERTED. v4.2.0 tried
    `core.data_engine` (legacy) FIRST and only fell back to
    `core.data_engine_v2` if legacy import failed. Since legacy always
    imports successfully, v2 was never reached. v4.3.0 tries
    `core.data_engine_v2` FIRST. If v2 is unavailable, falls back to legacy.
    This routes calls through the v5.50.0 enrichment pipeline so that
    Views, Insights, Candlesticks, and recommendation_reason populate.

    Logging on import is now explicit so /meta and startup logs make the
    binding choice obvious. CORE_GET_SHEET_ROWS_SOURCE will read
    "core.data_engine_v2.get_sheet_rows" in production after this deploy
    (verifiable in the response.meta.source field).

- BUMP [HIGH]: static fallback contract widened from 90 → 97 columns to
    align with `core.sheets.schema_registry` v2.7.0. Adds the 7 v5.50.0
    columns at canonical positions:
      * positions 91-92 (after position_size_hint):
          - `recommendation_detailed` ("Recommendation Detail")
          - `recommendation_priority` ("Reco Priority")
      * positions 93-97 (end of canonical):
          - `candlestick_pattern`         ("Candle Pattern")
          - `candlestick_signal`          ("Candle Signal")
          - `candlestick_strength`        ("Candle Strength")
          - `candlestick_confidence`      ("Candle Confidence")
          - `candlestick_patterns_recent` ("Recent Patterns (5D)")
    Adding them after position_size_hint preserves all existing positional
    indices below 91 — purely additive, no key reordering.

- BUMP: `_EXPECTED_SHEET_LENGTHS` instrument 90 → 97, Top10 93 → 100.
- BUMP: `_static_contract` instrument padding 90 → 97.
- BUMP: `_ensure_top10_contract` padding 93 → 100.
- BUMP: `_expected_len` default 90 → 97.

- KEEP: every v4.2.0 fix preserved unchanged. The 5 Insights group columns
    (sector_relative_score, conviction_score, top_factors, top_risks,
    position_size_hint) keep their canonical positions 86-90. The 4 view
    columns (fundamental_view, technical_view, risk_view, value_view)
    keep their canonical positions between rank_overall and recommendation.

- KEEP: every v4.1.0 fix preserved unchanged. Conservative placeholders,
    `_strip_internal_fields()`, "warn" status handling — all intact.

Production verification after deploy
------------------------------------
Hit `/v1/advanced/sheet-rows?sheet=Market_Leaders&limit=1` and inspect the
response. The following MUST hold for v4.3.0 to be considered successful:

  1. `meta.source == "core.data_engine_v2.get_sheet_rows"`
  2. `recommendation_detailed` non-null (e.g. "HOLD")
  3. `recommendation_priority` non-null (e.g. 8)
  4. `fundamental_view` / `technical_view` / `risk_view` / `value_view`
     all non-null on at least 80% of rows
  5. `recommendation_reason` non-null on rows that have a recommendation
  6. `sector_relative_score` / `conviction_score` non-null on enriched rows
  7. `candlestick_signal` non-null on at least 80% of rows
     (some symbols may have insufficient history → null is acceptable)

If condition 1 holds but 2-7 fail, the route fix is correct but the v5.50.0
engine is broken. If condition 1 fails, the v2 engine is not deployed.

v4.2.0 changes (from v4.1.0) — Wave 3 — preserved
-------------------------------------------------
- BUMP: static fallback contract widened from 85 → 90 columns to align with
    `core.sheets.schema_registry` v2.6.0 (Wave 1). Appends the 5 Insights
    group columns at the END of the canonical schema (positions 86-90):
      * `sector_relative_score` ("Sector-Adj Score")
      * `conviction_score`      ("Conviction Score")
      * `top_factors`           ("Top Factors")
      * `top_risks`             ("Top Risks")
      * `position_size_hint`    ("Position Size Hint")
    All 5 are produced by `core.insights_builder` v1.0.0. Adding them at
    the END preserves all existing positional indices — purely additive.
- BUMP: `_EXPECTED_SHEET_LENGTHS` instrument 85 → 90, Top10 88 → 93.
- BUMP: `_static_contract` instrument padding 85 → 90.
- BUMP: `_ensure_top10_contract` padding 88 → 93.
- BUMP: `_expected_len` default 85 → 90.
- KEEP: every v4.1.0 fix preserved unchanged. Conservative placeholders
    (no fake numerics — also returns None for the new Insights fields),
    internal-field stripping, "warn" status handling — all preserved.

v4.1.0 changes (from v4.0.0) — preserved
----------------------------------------
- FIX [HIGH]: static fallback was at 80 keys, padding to 84 with placeholder
    columns ("Column 81" → "column_81"...). v4.1.0 grew the static list to
    the canonical 85 entries by inserting `upside_pct` after `intrinsic_value`
    (registry v2.4.0 added this column; we were the last sibling not to ship
    it). _EXPECTED_SHEET_LENGTHS bumped to 85/88. Production registry-first
    path was unaffected — this only mattered when registry import failed.
    [v4.2.0 superseded the 85/88 numbers; v4.3.0 supersedes again to 97/100.]
- FIX [HIGH]: `_placeholder_value_for_key` no longer fabricates numeric
    values. Previously returned `recommendation="Accumulate"`,
    `expected_roi_3m=12.5%`, `forecast_confidence=99`, etc. for symbols
    where the engine returned no data. In a financial product, those
    synthetic numbers can be acted on by users. v4.1.0 returns `None` for
    every numeric/score/ROI/forecast field and only fills identity columns
    (symbol, name, asset_class, exchange, currency, country) plus a clear
    `warnings` field. Same philosophy as `routes.analysis_sheet_rows`
    v4.1.2 and `routes.advanced_sheet_rows` v4.0.0.
- ADD: `_strip_internal_fields()` defensive helper. Removes engine internal
    coordination flags (`_skip_recommendation_synthesis`, `_internal_*`,
    `_meta_*`, `_debug_*`, `_trace_*`, plus the explicit hard-strip set).
    Engine v5.47.4+ strips these at source; this is defence-in-depth for
    legacy / proxy / cached rows.
- ADD: `status: "warn"` from engine v5.47.4+ is treated as success-with-caveat
    when rows are present (matching the rest of the route family).

Why this revision (preserved from v4.0.0)
-----------------------------------------
- FIX: keeps root /sheet-rows authoritative and returns a stable envelope even
       when upstream builders degrade.
- FIX: never emits empty-success payloads for special pages.
- FIX: supports schema_only / headers_only in both GET and POST flows.
- FIX: normalizes external/core payloads into one contract:
       headers/display_headers/sheet_headers/column_headers/keys/rows/
       rows_matrix/row_objects/items/records/data/quotes.
- ENHANCE: provides schema-safe local fallbacks for Top_10_Investments,
           Insights_Analysis, Data_Dictionary, and instrument pages.
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

logger = logging.getLogger("routes.advanced_analysis")
logger.addHandler(logging.NullHandler())

ADVANCED_ANALYSIS_VERSION = "4.3.3"
router = APIRouter(tags=["schema", "root-sheet-rows"])

_TOP10_PAGE = "Top_10_Investments"
_INSIGHTS_PAGE = "Insights_Analysis"
_DICTIONARY_PAGE = "Data_Dictionary"
_SPECIAL_PAGES = {_TOP10_PAGE, _INSIGHTS_PAGE, _DICTIONARY_PAGE}

# v4.3.0: bumped from 90/93 to 97/100 to align with registry v2.7.0
# (97 cols = 90 base + 2 Decision Matrix + 5 Candlestick).
_EXPECTED_SHEET_LENGTHS: Dict[str, int] = {
    "Market_Leaders": 97,
    "Global_Markets": 97,
    "Commodities_FX": 97,
    "Mutual_Funds": 97,
    "My_Portfolio": 97,
    "My_Investments": 97,
    _TOP10_PAGE: 100,
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

# =============================================================================
# v4.3.1 CRITICAL FIX: engine binding now uses get_engine() async factory.
#
# v4.3.0 tried `from core.data_engine_v2 import get_sheet_rows` directly,
# but `get_sheet_rows` is an async METHOD on the DataEngineV5 class, not a
# module-level function. The v2 module's __all__ exports the engine class,
# `get_engine`, `get_engine_if_ready`, and supporting constants — but no
# top-level `get_sheet_rows`. v4.3.0's import therefore raised ImportError
# and the route fell through to the legacy adapter.
#
# v4.3.1 cascades through binding patterns in order of preference:
#   1. top-level `get_sheet_rows` from v2 (in case a future build adds it)
#   2. `get_engine()` async factory + adapter coroutine — EXPECTED PATH
#   3. `get_engine_if_ready()` sync factory + adapter coroutine
#   4. legacy `core.data_engine.get_sheet_rows` — bug indicator if reached
#
# Every step logs explicitly. The chosen binding is reflected in
# CORE_GET_SHEET_ROWS_SOURCE and surfaces in every response's meta.source
# field, making the live binding trivially observable from production.
# =============================================================================
CORE_GET_SHEET_ROWS_SOURCE = "unavailable"
core_get_sheet_rows = None  # type: ignore[assignment]

# Pattern 1: top-level function on v2 (may not exist; still try first)
try:
    from core.data_engine_v2 import get_sheet_rows as core_get_sheet_rows  # type: ignore
    CORE_GET_SHEET_ROWS_SOURCE = "core.data_engine_v2.get_sheet_rows"
    logger.info(
        "[advanced_analysis v%s] engine bound to core.data_engine_v2.get_sheet_rows (top-level fn)",
        ADVANCED_ANALYSIS_VERSION,
    )
except Exception as _v2_topfn_err:
    logger.info(
        "[advanced_analysis v%s] core.data_engine_v2.get_sheet_rows top-level not present (%s); trying engine factory",
        ADVANCED_ANALYSIS_VERSION, _v2_topfn_err,
    )

# Pattern 2: get_engine() async factory + .get_sheet_rows() method (expected path)
if core_get_sheet_rows is None:
    try:
        from core.data_engine_v2 import get_engine as _v2_get_engine  # type: ignore

        async def _v2_engine_factory_adapter(*args: Any, **kwargs: Any) -> Any:
            """v4.3.1: Adapter that calls the v5.50.0 engine via get_engine() factory.

            The factory is async; the engine method is async; we await both.
            Signature is **args, **kwargs so it remains compatible with the
            existing _call_core_sheet_rows_best_effort candidate-signature loop.
            """
            engine = await _v2_get_engine()
            return await engine.get_sheet_rows(*args, **kwargs)

        core_get_sheet_rows = _v2_engine_factory_adapter  # type: ignore[assignment]
        CORE_GET_SHEET_ROWS_SOURCE = "core.data_engine_v2.get_engine().get_sheet_rows"
        logger.info(
            "[advanced_analysis v%s] engine bound via core.data_engine_v2.get_engine() (async factory)",
            ADVANCED_ANALYSIS_VERSION,
        )
    except Exception as _v2_factory_err:
        logger.info(
            "[advanced_analysis v%s] core.data_engine_v2.get_engine() unavailable (%s); trying ready-engine factory",
            ADVANCED_ANALYSIS_VERSION, _v2_factory_err,
        )

# Pattern 3: get_engine_if_ready() sync factory with async-factory fallback
if core_get_sheet_rows is None:
    try:
        from core.data_engine_v2 import get_engine_if_ready as _v2_get_engine_if_ready  # type: ignore

        async def _v2_engine_ready_adapter(*args: Any, **kwargs: Any) -> Any:
            """v4.3.1: Adapter using sync get_engine_if_ready() with cold-start fallback."""
            engine = _v2_get_engine_if_ready()
            if engine is None:
                # Cold-start race: ready-check returned None, fall back to async factory
                try:
                    from core.data_engine_v2 import get_engine as _v2_async_factory  # type: ignore
                    engine = await _v2_async_factory()
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
            "[advanced_analysis v%s] core.data_engine_v2.get_engine_if_ready() unavailable (%s); trying legacy fallback",
            ADVANCED_ANALYSIS_VERSION, _v2_ready_err,
        )

# Pattern 4: legacy fallback (bug indicator if reached — loses v5.50.0 enrichment)
if core_get_sheet_rows is None:
    try:
        from core.data_engine import get_sheet_rows as core_get_sheet_rows  # type: ignore
        CORE_GET_SHEET_ROWS_SOURCE = "core.data_engine.get_sheet_rows"
        logger.warning(
            "[advanced_analysis v%s] all v2 binding patterns failed; falling back to legacy core.data_engine "
            "(this loses v5.50.0 enrichment — investigate v2 exports)",
            ADVANCED_ANALYSIS_VERSION,
        )
    except Exception as _legacy_err:
        core_get_sheet_rows = None  # type: ignore
        CORE_GET_SHEET_ROWS_SOURCE = "unavailable"
        logger.error(
            "[advanced_analysis v%s] BOTH v2 and legacy unavailable: legacy_err=%r",
            ADVANCED_ANALYSIS_VERSION, _legacy_err,
        )

# v4.3.0: 97-column canonical contract (was 90 in v4.2.0, 85 in v4.1.0,
# 80 in v4.0.0). Aligned with registry v2.7.0 / engine v5.50.0.
#
# Layout (1-indexed positions, all 97 columns):
#   1-50  identity, prices, volume, fundamentals, risk metrics, valuation
#   51-65 forecasts, ROIs, scores
#   66-72 view tokens (4) + recommendation block start
#   73-85 horizon, position, last-updated, warnings
#   86-90 Wave 3 Insights (sector_relative_score..position_size_hint)
#   91-92 v5.50.0 Decision Matrix (recommendation_detailed, recommendation_priority)
#   93-97 v5.50.0 Candlestick (pattern, signal, strength, confidence, patterns_recent)
#
# IMPORTANT: name `_CANONICAL_80_HEADERS` is historical (started at 80
# columns). Do not rename — kept for stable diff/grep-ability across
# v4.0.0 → v4.3.0 history.
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
    "Sector-Adj Score", "Conviction Score", "Top Factors", "Top Risks", "Position Size Hint",
    # v4.3.0: v5.50.0 Decision Matrix columns
    "Recommendation Detail", "Reco Priority",
    # v4.3.0: v5.50.0 Candlestick columns
    "Candle Pattern", "Candle Signal", "Candle Strength", "Candle Confidence",
    "Recent Patterns (5D)",
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
    "sector_relative_score", "conviction_score", "top_factors", "top_risks", "position_size_hint",
    # v4.3.0: v5.50.0 Decision Matrix keys
    "recommendation_detailed", "recommendation_priority",
    # v4.3.0: v5.50.0 Candlestick keys
    "candlestick_pattern", "candlestick_signal", "candlestick_strength", "candlestick_confidence",
    "candlestick_patterns_recent",
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
    # v4.1.0: engine v5.47.4 mirrors upside_pct
    "upside_pct": ["upsidePct", "upside_percent", "intrinsic_upside"],
    # v4.3.0: v5.50.0 Decision Matrix aliases (engine may emit either form)
    "recommendation_detailed": ["recommendationDetailed", "recommendation_detail",
                                "reco_detailed", "reco_detail"],
    "recommendation_priority": ["recommendationPriority", "reco_priority", "reco_pri"],
    # v4.3.0: v5.50.0 Candlestick aliases
    "candlestick_pattern": ["candle_pattern", "candlestickPattern"],
    "candlestick_signal": ["candle_signal", "candlestickSignal"],
    "candlestick_strength": ["candle_strength", "candlestickStrength"],
    "candlestick_confidence": ["candle_confidence", "candlestickConfidence"],
    "candlestick_patterns_recent": ["candle_patterns_recent", "candlestickPatternsRecent",
                                    "recent_patterns_5d"],
}

# v4.1.0: internal-field stripping — same set as enriched_quote v4.2.0 /
# advanced_sheet_rows v4.0.0 / analysis_sheet_rows v4.1.2.
_INTERNAL_FIELD_PREFIXES: Tuple[str, ...] = ("_skip_", "_internal_", "_meta_", "_debug_", "_trace_")
_INTERNAL_FIELDS_TO_STRIP_HARD: frozenset = frozenset({
    "_placeholder",
    "_skip_recommendation_synthesis",
    "unit_normalization_warnings",
    "intrinsic_value_source",
})


def _strip_internal_fields(row: Any) -> Any:
    """v4.1.0: Remove engine internal coordination flags from a row dict.

    Defence-in-depth: engine v5.47.4 strips these at source, but rows from
    legacy engine, proxies, or cached snapshots may still carry them.
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
    # v4.3.0: Top10 padding 93 → 100 (97 base + 3 Top10 metadata)
    hdrs, ks = _complete_schema_contract(headers, keys)
    for field in _TOP10_REQUIRED_FIELDS:
        if field not in ks:
            ks.append(field)
            hdrs.append(_TOP10_REQUIRED_HEADERS[field])
    return _pad_contract(hdrs, ks, 100)

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
    # v4.3.0: instrument fallback default 90 → 97
    h, k = _pad_contract(_CANONICAL_80_HEADERS, _CANONICAL_80_KEYS, _EXPECTED_SHEET_LENGTHS.get(page, 97))
    return h, k, "static_canonical_instrument"

def _expected_len(page: str) -> int:
    if callable(get_sheet_len):
        try:
            n = int(get_sheet_len(page))  # type: ignore[misc]
            if n > 0:
                return n
        except Exception:
            pass
    # v4.3.0: default 90 → 97
    return _EXPECTED_SHEET_LENGTHS.get(page, 97)

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

def _rows_from_matrix(matrix: Any, keys: Sequence[str]) -> List[Dict[str, Any]]:
    """v4.3.2: Convert a list-of-lists matrix into list-of-dicts using keys.

    Symmetric with `core.data_engine._rows_from_matrix`. Each inner row is
    zipped with the provided keys positionally; missing trailing values
    become None, extra trailing values are dropped.

    Inner items that are already dicts are passed through (so a "mixed"
    bucket with both dicts and lists is handled). Inner items that are
    neither dicts nor list/tuple are skipped.
    """
    if not isinstance(matrix, list) or not keys:
        return []
    ks = [str(k) for k in keys]
    out: List[Dict[str, Any]] = []
    for row in matrix:
        if isinstance(row, dict):
            out.append({k: row.get(k) for k in ks})
            continue
        if not isinstance(row, (list, tuple)):
            continue
        out.append({k: (row[i] if i < len(row) else None) for i, k in enumerate(ks)})
    return out

def _extract_rows_with_matrix_fallback(payload: Any, keys: Sequence[str]) -> List[Dict[str, Any]]:
    """v4.3.2: Row extractor that handles both list-of-dicts and matrix payloads.

    The v5.50.0 engine `get_sheet_rows()` returns rows in MATRIX form —
    `payload["rows"]` is a list of lists positionally keyed to
    `payload["keys"]`, not a list of dicts. The legacy adapter
    `core.data_engine.get_sheet_rows` did matrix-to-dict conversion
    internally before returning, which is why v4.2.0 (legacy) saw dicts
    here and v4.3.1 (direct v2) saw nothing.

    Strategy:
      1. First try `_extract_rows_like` (handles dicts in any standard
         bucket — preserves v4.3.1 behavior for dict-shaped payloads).
      2. If that returns empty, scan the same buckets for list-of-list
         data and convert via `_rows_from_matrix(rr, keys)`.
      3. If the payload also has a top-level "rows_matrix" or "matrix"
         key (some engines keep dict rows in "rows" and matrix in "matrix"
         simultaneously), try that as a last resort.

    Returns a list of dicts every time. Empty list on no match.
    """
    # Path 1: existing behavior — find dict rows anywhere
    dict_rows = _extract_rows_like(payload)
    if dict_rows:
        return dict_rows
    if not isinstance(payload, Mapping) or not keys:
        return []
    ks = list(keys)
    # Path 2: scan same buckets for matrix data
    for bucket in ("rows", "rows_matrix", "matrix", "data", "items", "records", "quotes", "results"):
        rr = payload.get(bucket)
        if not isinstance(rr, list) or not rr:
            continue
        # Skip buckets that are clearly not row data (already-handled dicts caught above;
        # only matrix or mixed left here). Accept if first non-empty element is list/tuple.
        first = rr[0]
        if isinstance(first, (list, tuple)):
            converted = _rows_from_matrix(rr, ks)
            if converted:
                return converted
        elif isinstance(first, dict):
            # Already-dicts case was handled by _extract_rows_like above; if we got here
            # _extract_rows_like rejected this bucket (e.g. bucket=="rows" but first dict
            # was empty). Try projecting anyway as defence-in-depth.
            converted = _rows_from_matrix(rr, ks)
            if converted:
                return converted
    # Path 3: nested payload envelopes (mirror _extract_rows_like's recursion)
    for name in ("payload", "result", "response", "output", "data"):
        nested = payload.get(name)
        if isinstance(nested, Mapping):
            inner = _extract_rows_with_matrix_fallback(nested, ks)
            if inner:
                return inner
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
    # v4.1.0: defensively strip internal coordination flags before extraction.
    _strip_internal_fields(raw)
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
    """Conservative placeholder (v4.1.0): identity columns get filled,
    everything numeric returns None. The row is clearly marked as a
    placeholder via the `warnings` field. Same philosophy as
    `routes.analysis_sheet_rows` v4.1.2 / `routes.advanced_sheet_rows` v4.0.0.

    v4.0.0 fabricated values like `recommendation="Accumulate"`,
    `expected_roi_3m=12.5%`, `forecast_confidence=99` for symbols where
    the engine returned no data. In a financial product, those synthetic
    numbers can be acted on by users — financial-safety risk.
    """
    kk = _normalize_key_name(key)

    # Identity columns — safe to populate
    if kk in {"symbol", "ticker"}:
        return symbol
    if kk == "name":
        return symbol  # don't fabricate composite names
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

    # Provenance — clearly mark as placeholder
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
    # valuation ratios, forecasts, position data, view tokens,
    # v5.50.0 Decision Matrix fields, v5.50.0 Candlestick fields) → None.
    return None

def _build_placeholder_rows(*, page: str, keys: Sequence[str], requested_symbols: Sequence[str], limit: int, offset: int) -> List[Dict[str, Any]]:
    symbols = [_normalize_symbol_token(x) for x in requested_symbols if _normalize_symbol_token(x)]
    if not symbols:
        symbols = [_normalize_symbol_token(x) for x in EMERGENCY_PAGE_SYMBOLS.get(page, []) if _normalize_symbol_token(x)]
    symbols = symbols[offset : offset + limit] if (offset or len(symbols) > limit) else symbols[:limit]
    rows: List[Dict[str, Any]] = []
    for idx, sym in enumerate(symbols, start=offset + 1):
        row = {str(k): _placeholder_value_for_key(page, str(k), sym, idx) for k in keys}
        # v4.1.0: ALWAYS guarantee a warnings field so the user knows
        # this row is a placeholder, regardless of which page schema is used.
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
        {"section": "Coverage", "item": "Requested symbols", "symbol": "", "metric": "count", "value": len(symbols), "notes": "Local insights fallback summary — no live engine data", "last_updated_riyadh": stamp},
        {"section": "Coverage", "item": "Universe sample", "symbol": "", "metric": "symbols", "value": ", ".join(symbols[:5]), "notes": "Sample of the symbols used by fallback mode", "last_updated_riyadh": stamp},
        {"section": "Status", "item": "Engine availability", "symbol": "", "metric": "warning", "value": "Engine returned no usable rows", "notes": "Live engine and upstream proxies all returned empty/error payloads", "last_updated_riyadh": stamp},
    ]
    # v4.1.0: don't fabricate per-symbol "Watch"/"Accumulate" signals.
    # Just list which symbols WOULD have been analyzed.
    for idx, sym in enumerate(symbols[: max(1, limit + offset)], start=1):
        rows.append({"section": "Pending Analysis", "item": f"Symbol {idx}", "symbol": sym, "metric": "status", "value": "no_live_data", "notes": "Symbol is in the requested universe but the engine returned no live row for it", "last_updated_riyadh": stamp})
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

def _payload_envelope(*, page: str, headers: Sequence[str], keys: Sequence[str], row_objects: Sequence[Mapping[str, Any]], include_matrix: bool, request_id: str, started_at: float, mode: str, status_out: str, error_out: Optional[str], meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    hdrs = list(headers or [])
    ks = list(keys or [])
    # v4.1.0: strip internal flags at final emission too (defence-in-depth).
    rows_dict = [_strip_internal_fields({str(k): _json_safe(dict(r).get(k)) for k in ks}) for r in (row_objects or [])]
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
            # v4.3.3: explicit WARNING log so non-TypeError engine failures
            # are visible in Render deploy logs (previously swallowed silently
            # into an error dict whose text only reached the discarded
            # normalized envelope, never the wire).
            try:
                logger.warning(
                    "[advanced_analysis v%s] engine call raised %s: %s "
                    "(page=%r limit=%r offset=%r body_keys=%r)",
                    ADVANCED_ANALYSIS_VERSION,
                    e.__class__.__name__, e,
                    page, limit, offset,
                    sorted(list((body or {}).keys()))[:15],
                )
            except Exception:
                pass
            break
    if last_err is not None:
        return {
            "status": "error",
            "error": "{}: {}".format(last_err.__class__.__name__, last_err),
            "error_class": last_err.__class__.__name__,
            "row_objects": [],
        }, CORE_GET_SHEET_ROWS_SOURCE
    return None, None

def _normalize_external_payload(*, external_payload: Mapping[str, Any], page: str, headers: Sequence[str], keys: Sequence[str], include_matrix: bool, request_id: str, started_at: float, mode: str, limit: int = 2000, offset: int = 0, top_n: int = 2000, requested_symbols: Optional[Sequence[str]] = None, meta_extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    ext = dict(external_payload or {})
    hdrs = list(headers or [])
    ks = list(keys or [])
    # v4.3.2: matrix-aware extraction. v5.50.0 engine returns rows in
    # list-of-lists form (positionally keyed to ks); legacy adapter did
    # this conversion internally. Calling the matrix-aware extractor here
    # restores parity. Falls back identically to v4.3.1 for dict payloads.
    rows = _extract_rows_with_matrix_fallback(ext, ks)
    # v4.3.2: capture extraction diagnostic for production observability.
    # If extraction returned 0 rows but the payload clearly has data, the
    # diagnostic surfaces in meta.engine_payload_diagnostic so the next
    # debugging step doesn't require code changes.
    engine_payload_diagnostic: Dict[str, Any] = {}
    if not rows and isinstance(ext, Mapping):
        bucket_summary: Dict[str, str] = {}
        for bucket in ("rows", "rows_matrix", "matrix", "row_objects", "records", "items", "data", "quotes", "results"):
            v = ext.get(bucket)
            if v is None:
                continue
            if isinstance(v, list):
                if not v:
                    bucket_summary[bucket] = "list[empty]"
                else:
                    first = v[0]
                    bucket_summary[bucket] = "list[{}]({} els)".format(type(first).__name__, len(v))
            else:
                bucket_summary[bucket] = type(v).__name__
        engine_payload_diagnostic = {
            "buckets": bucket_summary,
            "top_keys": sorted(list(ext.keys()))[:25],
            "had_status": _strip(ext.get("status")) or "(absent)",
            "had_error": _strip(ext.get("error")) or "(absent)",
            "schema_keys_count": len(ks),
        }
    normalized_rows = [_normalize_to_schema_keys(schema_keys=ks, schema_headers=hdrs, raw=(r or {})) for r in rows]
    if page == _TOP10_PAGE:
        normalized_rows = _ensure_top10_rows(normalized_rows, requested_symbols=requested_symbols or [], top_n=top_n, schema_keys=ks, schema_headers=hdrs)
    normalized_rows = _slice(normalized_rows, limit=limit, offset=offset)
    status_out, error_out, ext_meta = _extract_status_error(ext)
    # v4.1.0: treat engine v5.47.4 "warn" as success when rows are present
    status_lc = (status_out or "").lower()
    if status_lc == "warn":
        status_out = "success" if normalized_rows else "warn"
    if not normalized_rows:
        status_out = "partial"
        error_out = error_out or "No usable rows returned"
    final_meta = dict(ext_meta or {})
    if meta_extra:
        final_meta.update(meta_extra)
    if engine_payload_diagnostic:
        final_meta["engine_payload_diagnostic"] = engine_payload_diagnostic
    return _payload_envelope(page=page, headers=hdrs, keys=ks, row_objects=normalized_rows, include_matrix=include_matrix, request_id=request_id, started_at=started_at, mode=mode, status_out=status_out or ("success" if normalized_rows else "partial"), error_out=error_out, meta=final_meta)

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
        return _payload_envelope(page=page, headers=headers, keys=keys, row_objects=[], include_matrix=include_matrix, request_id=request_id, started_at=start, mode=mode, status_out="success", error_out=None, meta={"dispatch": "schema_only", "schema_source": schema_source, "headers_only": headers_only, "schema_only": schema_only})

    payload, source = await _call_core_sheet_rows_best_effort(page=page, limit=max(limit + offset, top_n), offset=0, mode=mode or "", body=merged_body)

    # v4.3.3: capture upstream diagnostic info BEFORE falling through to
    # fallback. v4.3.2 attached this to the normalized envelope but the
    # impl discarded that envelope when extraction failed, so the
    # diagnostic never reached the wire. Capturing here ensures it
    # reaches the fallback meta below.
    upstream_diagnostic_meta: Dict[str, Any] = {}
    if isinstance(payload, dict):
        try:
            upstream_diagnostic_meta["upstream_payload_type"] = type(payload).__name__
            upstream_diagnostic_meta["upstream_payload_top_keys"] = sorted(list(payload.keys()))[:25]
            ups_status = _strip(payload.get("status"))
            ups_error = _strip(payload.get("error")) or _strip(payload.get("detail")) or _strip(payload.get("message"))
            ups_error_class = _strip(payload.get("error_class"))
            if ups_status:
                upstream_diagnostic_meta["upstream_status"] = ups_status
            if ups_error:
                upstream_diagnostic_meta["upstream_error"] = ups_error[:500]
            if ups_error_class:
                upstream_diagnostic_meta["upstream_error_class"] = ups_error_class
            # Bucket-shape summary (mirrors the v4.3.2 diagnostic that was
            # being lost). Tells us whether the engine returned matrix,
            # dicts, empty lists, or nothing.
            bucket_summary: Dict[str, str] = {}
            for bucket in ("rows", "rows_matrix", "matrix", "row_objects", "records", "items", "data", "quotes", "results"):
                v = payload.get(bucket)
                if v is None:
                    continue
                if isinstance(v, list):
                    if not v:
                        bucket_summary[bucket] = "list[empty]"
                    else:
                        first = v[0]
                        bucket_summary[bucket] = "list[{}]({} els)".format(type(first).__name__, len(v))
                else:
                    bucket_summary[bucket] = type(v).__name__
            if bucket_summary:
                upstream_diagnostic_meta["engine_payload_diagnostic"] = bucket_summary
        except Exception:
            pass

        normalized = _normalize_external_payload(external_payload=payload, page=page, headers=headers, keys=keys, include_matrix=include_matrix, request_id=request_id, started_at=start, mode=mode, limit=limit, offset=offset, top_n=top_n, requested_symbols=requested_symbols, meta_extra={"schema_source": schema_source, "source": source or CORE_GET_SHEET_ROWS_SOURCE})
        if _extract_rows_like(normalized):
            return normalized

    # v4.3.3: log the failure mode at WARNING so it shows up in Render
    # deploy logs. The wire response also carries the same info in
    # meta.upstream_* below, but log-side visibility is helpful when the
    # caller is a script / scheduler that doesn't surface response meta.
    try:
        if upstream_diagnostic_meta:
            logger.warning(
                "[advanced_analysis v%s] engine call returned non-empty payload but route extracted 0 rows. "
                "page=%r upstream_status=%r upstream_error=%r buckets=%r top_keys=%r",
                ADVANCED_ANALYSIS_VERSION, page,
                upstream_diagnostic_meta.get("upstream_status"),
                upstream_diagnostic_meta.get("upstream_error"),
                upstream_diagnostic_meta.get("engine_payload_diagnostic"),
                upstream_diagnostic_meta.get("upstream_payload_top_keys"),
            )
        elif payload is None:
            logger.warning(
                "[advanced_analysis v%s] engine call returned None (payload is None). page=%r",
                ADVANCED_ANALYSIS_VERSION, page,
            )
    except Exception:
        pass

    fallback_rows = _build_nonempty_failsoft_rows(page=page, headers=headers, keys=keys, requested_symbols=requested_symbols, limit=limit, offset=offset, top_n=top_n)
    fallback_status = "partial" if fallback_rows else "error"
    fallback_error = "Local non-empty fallback emitted after upstream degradation" if fallback_rows else "No usable rows returned; schema-shaped fallback emitted"
    fallback_meta: Dict[str, Any] = {
        "dispatch": "advanced_analysis_fail_soft_nonempty" if fallback_rows else "advanced_analysis_fail_soft",
        "schema_source": schema_source,
        "source": source or CORE_GET_SHEET_ROWS_SOURCE,
    }
    # v4.3.3: merge upstream diagnostic so the wire response shows what
    # the engine actually returned (or didn't).
    if upstream_diagnostic_meta:
        fallback_meta.update(upstream_diagnostic_meta)
    return _payload_envelope(page=page, headers=headers, keys=keys, row_objects=fallback_rows, include_matrix=include_matrix, request_id=request_id, started_at=start, mode=mode, status_out=fallback_status, error_out=fallback_error, meta=fallback_meta)

@router.get("/health")
@router.get("/v1/schema/health")
async def advanced_analysis_health(request: Request) -> Dict[str, Any]:
    return _json_safe({
        "status": "ok",
        "service": "advanced_analysis",
        "version": ADVANCED_ANALYSIS_VERSION,
        "schema_registry_available": bool(get_sheet_spec is not None),
        "adapter_available": bool(core_get_sheet_rows is not None),
        "engine_source": CORE_GET_SHEET_ROWS_SOURCE,
        "allowed_pages_count": len(_safe_allowed_pages()),
        "path": str(getattr(getattr(request, "url", None), "path", "")),
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
