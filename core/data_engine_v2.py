#!/usr/bin/env python3
# core/data_engine_v2.py
"""
================================================================================
Data Engine V2 - GLOBAL-FIRST ORCHESTRATOR - v5.77.5
================================================================================

WHY v5.77.5 - DOCSTRING ACCURACY (NO CODE CHANGE)
-------------------------------------------------
v5.77.5 is a documentation-only fix. The runtime behavior is byte-for-byte
identical to v5.77.4. No redeploy is required if v5.77.4 is already live;
this can be picked up on the next deployment cycle.

  Fix - _apply_sheet_compat_collapse docstring no longer contradicts code.
    The function's docstring listed `recommendation_reason` under "Fields
    NOT modified" with a rationale about preserving the structured-reason
    audit trail. That was accurate as of v5.76.0 but stale after v5.77.0,
    which added the leading-prefix rewrite (so the displayed action stays
    consistent with the collapsed value — "ACCUMULATE: Scale in..." now
    becomes "BUY: Scale in..." when collapsed). The behavior is correct
    and intentional; only the documentation was out of date. ChatGPT
    v5.77.4 audit caught this. v5.77.5 moves `recommendation_reason` to
    the modified-fields list with a precise description of WHAT is
    changed (leading prefix only) and WHY (visible mismatch fix from
    v5.77.0), and the audit-trail preservation rationale stays attached
    to the remaining structured-reason text.

PRESERVED - strictly:
  Everything. No code paths changed. All v5.77.4 / v5.77.3 / v5.77.2 /
  v5.77.1 / v5.77.0 behavior, math, contracts, and API shapes preserved
  exactly. The only file delta is the docstring block inside one
  function definition.

================================================================================
WHY v5.77.4 - TELEMETRY ACTIVATION (TINY FIX)
---------------------------------------------
v5.77.4 is a one-line correctness fix on top of v5.77.3.

  Fix - Schema-drift telemetry now actually fires on real sheets.
    v5.77.3's _finalize_payload telemetry gated the WARNING on
    `sheet in ("instruments", "instrument", "stocks", "stock", "")`
    — but those are placeholder names that NEVER appear in the TFB
    deployment. The real sheets are Market_Leaders, Global_Markets,
    Commodities_FX, Mutual_Funds, My_Portfolio, My_Investments,
    Top_10_Investments. ChatGPT v5.77.3 audit caught this: the
    telemetry was effectively dead code in production.

    v5.77.4 routes the sheet name through the existing
    `_canonicalize_sheet_name()` helper and checks membership in
    the existing INSTRUMENT_SHEETS set — the same predicate already
    used elsewhere in this module for recovery and emergency-symbol
    paths. Now the WARNING fires correctly on instrument sheets
    while SPECIAL_SHEETS (Insights_Analysis, Data_Dictionary) which
    have their own schemas are correctly excluded.

PRESERVED - strictly:
  All v5.77.3 / v5.77.2 / v5.77.1 / v5.77.0 architectural behavior.
  No contract changes, no math changes, no API changes. This release
  exists solely so the telemetry added in v5.77.3 actually does
  something useful.

================================================================================
WHY v5.77.3 - POLISH OVER v5.77.2 (DEFENSIVE SCHEMA / TRANSPARENCY / TELEMETRY)
------------------------------------------------------------------------------
v5.77.3 is a small polish pass closing out the v5.77.2 audit. Three changes
plus one deployment note. No new features, no contract changes.

  Polish 1 - forecast_source elevated to required canonical key.
    ChatGPT v5.77.2 audit flagged: if an external schema_registry.py
    returns 106 columns (the pre-v5.77.1 layout), the
    _instrument_contract_is_canonical() check would still return True
    because forecast_source (the 107th field added in v5.77.1) was
    not in _INSTRUMENT_CANONICAL_REQUIRED_KEYS. This made the schema-
    drift catch silently permissive during partial deployments.
    v5.77.3 adds "forecast_source" to the required set, so a 106-
    column external schema now correctly fails the canonical check
    and _usable_contract() falls back to the built-in STATIC 107-
    column canonical. Pure defensive hardening.

  Polish 2 - Capped-provider-target warning tag.
    ChatGPT v5.77.2 audit suggested surfacing when the provider 12M
    target return exceeds the Phase-II abs cap (±30%). v5.77.2 capped
    silently before deriving 1M/3M, which left users wondering why a
    provider 12M target of +60% produced a 3M figure of only +10.5%
    (= +30% × 0.35) instead of +21% (= +60% × 0.35). v5.77.3 appends
    "provider_target_capped_for_short_horizon_derivation" to the
    warnings field whenever the cap actually fires. Tolerance of 1e-9
    avoids spurious tagging on floating-point noise near the boundary.

  Polish 3 - Schema-count telemetry in _finalize_payload.
    Gemini v5.77.2 audit suggested logging when the finalized payload
    drifts from the canonical 107-column shape. Catches schema-
    mismatch bugs at the engine boundary instead of after the
    dashboard renders broken cells. Logs WARNING (not ERROR) so the
    payload still flows downstream — this is observability, not a
    hard gate.

  Deployment note - Render TFB_PROVIDER_POOL_WORKERS.
    ChatGPT v5.77.2 audit recommended lowering the default provider
    pool size (200) on Render to 80-120 for memory safety. This is
    NOT a code change — the in-code default stays at 200 to avoid
    surprising operators running on larger hosts. On Render, set the
    env var: TFB_PROVIDER_POOL_WORKERS=80 (or up to 120). The
    threading.Lock + double-checked locking from v5.77.1 makes any
    value safe; the question is memory footprint per worker.

PRESERVED - strictly:
  All v5.77.2 / v5.77.1 / v5.77.0 / v5.76.0 / v5.75.0 architectural
  behavior. The 107-field canonical schema is unchanged. v5.77.0's
  intrinsic-value calibration, Phase-II forecast weights, LRU cache,
  sanitization bounds, dividend yield ceiling, RSI multiplicative
  dampening, double-checked locking on _PROVIDER_EXECUTOR,
  forecast_source provider_target detection with _norm_key_loose,
  fallback source tagging, 1M/3M derivation from provider 12M target,
  and reset_provider_executor() test helper all preserved.

================================================================================
WHY v5.77.2 - HOTFIX OVER v5.77.1 (snake_case ALIASES / YAHOO POOL / FALLBACK SOURCE / 1M-3M DERIVATION)
--------------------------------------------------------------------------------
v5.77.2 closes four remaining gaps in v5.77.1 surfaced by post-release audit:

  Hotfix 1 - snake_case provider-target aliases NOT detected.
    v5.77.1 detected provider analyst targets by comparing alias keys
    via lowercase string equality. "targetMeanPrice" became
    "targetmeanprice" and matched. But "target_mean_price" stayed
    "target_mean_price" and DID NOT match the lowercase camelCase
    keys. Some upstream providers (and some snake_case JSON consumers
    upstream of the engine) emit snake_case field names, which meant
    forecast_source stayed empty and Phase-II overwrote a legitimate
    provider target with its synthesis. v5.77.2 switches to a
    fully-normalized comparison (strip all non-alphanumeric chars,
    lowercase) so "targetMeanPrice", "target_mean_price",
    "TARGET-MEAN-PRICE", and "target.mean.price" all collapse to
    "targetmeanprice" and match.

  Hotfix 2 - Yahoo enrichment bypassed the dedicated provider pool.
    v5.77.1 added _call_maybe_async to route sync provider calls
    through the dedicated 200-worker pool, but THREE Yahoo enrichment
    call sites (yahoo fundamentals, yahoo chart quote, yahoo history)
    still called `await asyncio.to_thread(fn, ...)` directly. Under
    high concurrency these calls saturated asyncio's default executor
    (~36 threads), partially defeating the dedicated-pool fix.
    v5.77.2 routes all three through _call_maybe_async so the
    dedicated pool serves the entire provider call surface.

  Hotfix 3 - forecast_source = "fallback" was documented but never set.
    v5.77.1's docstring listed four forecast_source values:
      provider_target / phase_ii_synthetic / fallback / blank
    But no code path ever wrote "fallback". When core.scoring failed
    and _compute_scores_local_fallback synthesized forecast prices,
    forecast_source stayed empty — making the dashboard column
    indistinguishable from "no forecast available". v5.77.2 sets
    row["forecast_source"] = "fallback" inside the fallback function,
    but ONLY when it actually creates a forecast price AND no upstream
    source (provider_target / phase_ii_synthetic) is already set.

  Hotfix 4 - Provider-target branch left 1M and 3M columns blank.
    v5.77.1's Phase-II guard `if existing_source == "provider_target":
    return` correctly protected the 12M analyst target from being
    overwritten — but `return` also skipped the 1M/3M derivation that
    happens later in the function. For every stock with a provider
    target (the majority of the universe), the 1M and 3M forecast
    columns came back blank. v5.77.2 still treats the provider 12M
    target as authoritative (no overwrite of the 12M field) but
    derives 1M and 3M from it using the same _PHASE_II_RATIO_3M_OF_12M
    (0.35) and _PHASE_II_RATIO_1M_OF_12M (0.12) constants used for
    synthesized forecasts. expected_roi_* fields are similarly
    derived. The result: provider-target rows now have populated
    short-horizon forecasts AND the 12M anchor stays untouched.

  Bonus - reset_provider_executor() helper exposed for test suites.
    Gemini suggested this for test-environment ergonomics: tests that
    vary TFB_PROVIDER_POOL_WORKERS need a way to discard the cached
    executor and rebuild it. Wraps _shutdown_provider_executor() with
    a clearer "intended for tests, not runtime" docstring.

PRESERVED - strictly:
  All v5.77.1 / v5.77.0 / v5.76.0 / v5.75.0 architectural behavior.
  No API shape changes — the 107-field canonical contract stays.
  v5.77.0's intrinsic-value calibration (sector_pe_map, sector_pb_map),
  Phase-II forecast weights, LRU cache, sanitization bounds, dividend
  yield ceiling, recommendation_reason prefix rewrite on collapse,
  v5.77.1's double-checked locking on the provider pool, multiplicative
  RSI dampening, and provider-target schema field all preserved.

================================================================================
WHY v5.77.1 - HOTFIX OVER v5.77.0 (RACE / FORECAST_SOURCE / RSI / SCHEMA)
------------------------------------------------------------------------
v5.77.1 closes four gaps in v5.77.0 surfaced by post-release audit:

  Hotfix 1 - RACE CONDITION on dedicated provider pool initialization.
    v5.77.0 lazy-initialized _PROVIDER_EXECUTOR with no lock. Under
    concurrent first-touch (200 quotes starting before the executor
    is created), every coroutine saw `is None` and each tried to
    spawn its own ThreadPoolExecutor, producing orphaned pools and
    a memory spike. Fixed with standard double-checked locking using
    threading.Lock (the correct sync primitive — _get_provider_executor
    is a sync function called from inside async coroutines).
    Also added _shutdown_provider_executor() called from
    DataEngineV5.aclose() so the pool is released on teardown.

  Hotfix 2 - forecast_source = "provider_target" was never actually set.
    v5.77.0 added the protection check `if existing_source ==
    "provider_target": return` in _phase_ii_quality_forecast, but no
    upstream code ever wrote that tag — leaving the feature half-
    finished. v5.77.1 sets the tag inside _canonicalize_provider_row
    when any known provider-target alias (targetMeanPrice,
    targetMedianPrice, targetHighPrice, targetPrice12m,
    priceTarget12m, analystTargetPrice, consensusTarget, etc.) is
    detected in the raw source. Phase-II's idempotency check now
    actually fires when a provider supplied an analyst target.

  Hotfix 3 - targetMeanPrice was incorrectly mapped to 3M horizon.
    v5.77.0 (and earlier) listed "targetMeanPrice" in the
    forecast_price_3m alias list. Yahoo's targetMeanPrice is the 12M
    analyst consensus, not a quarterly target. Putting it on 3M caused
    the engine to treat a 12-month price target as a quarterly
    forecast, corrupting the 3M ROI math. v5.77.1 moves it to
    forecast_price_12m where it belongs, and adds analystTargetPrice
    and consensusTarget for cross-provider coverage.

  Hotfix 4 - RSI penalty magnitude on 1M/3M was too aggressive.
    v5.77.0 moved the RSI bump (-3% / +3% absolute) from the 12M
    forecast to 1M/3M derived returns. The horizon-shift was correct
    (RSI doesn't predict 12M returns) but the magnitude wasn't
    reconsidered. On a blue-chip with baseline 1M return of +1.5%,
    an absolute -3% for RSI>75 swung the expectation to -1.5%
    (implying ~36% annualized technical drag). v5.77.1 switches to
    multiplicative dampening for positive expectations (halving on
    extreme RSI, smaller damping on moderate RSI) and uses a smaller
    absolute add only when no baseline positive expectation exists.

  Hotfix 5 - forecast_source now surfaced to the canonical sheet.
    Added as the 107th canonical key (and "Forecast Source" header).
    The sheet schema is now 107 fields. Values:
      "provider_target"    - upstream analyst target
      "phase_ii_synthetic" - engine synthesized
      "fallback"           - local heuristic
      ""                   - no forecast available
    This is the first schema expansion since v5.74.0's 97 -> 106
    expansion. Schema consumers (Apps Script, sheet validators) MUST
    accept the new column.

PRESERVED - strictly:
  All v5.77.0 / v5.76.0 / v5.75.0 architectural behavior. No API
  shape changes beyond the additive 107th canonical field. v5.77.0's
  intrinsic-value calibration (sector_pe_map, sector_pb_map),
  Phase-II forecast weights, LRU cache, sanitization bounds,
  dividend yield ceiling, and recommendation_reason prefix rewrite
  on collapse all preserved.

================================================================================
WHY v5.77.0 - VALUATION-MODEL CALIBRATION + FORECAST GUARDRAILS + LRU CACHE
---------------------------------------------------------------------------
v5.77.0 is a focused correctness patch over v5.76.0. It addresses the
systematic bearish-bias on growth-sector equities identified in the
2026-05-21 dashboard audit by recalibrating the intrinsic-value model
and tightening forecast guardrails. No API shape changes; all v5.76.0
contract markers and atomic-write semantics preserved. The behavior
change is in the math, not the structure.

Bug 1 - sector_pe_map produced systematically-low intrinsics on growth
        names. Audit math against GOOGL (eps=$13.11, pb=9.84, comm-services
        sector): the v5.76.0 map's fair_pe=18.0 for "communication services"
        and the universal book_value * 1.5 anchor produced intrinsic=$177.08
        vs actual market price $388.91 (-54.5% upside). The model is a
        1990s-era value-investor heuristic that systematically penalizes
        any profitable company trading above its book * 1.5.

  Fix in two places:
    (a) sector_pe_map updated to post-2020 sector P/E medians sourced
        from S&P 500 GICS composites and Damodaran sector multiples:
          technology 25.0 -> 32.0
          communication services 18.0 -> 28.0
          consumer cyclical 18.0 -> 22.0
          consumer electronics 22.0 -> 25.0
          financial services 13.0 -> 14.0
          healthcare 22.0 -> 24.0
          industrials 19.0 -> 20.0
          energy 12.0 -> 14.0
          utilities 17.0 -> 18.0
          basic materials 15.0 -> 16.0
          (consumer defensive, real estate unchanged)
        Default for unknown sectors raised 18.0 -> 20.0.
    (b) sector_pb_map ADDED. The v5.76.0 hardcoded `pb_candidate =
        book_value * 1.5` is replaced with `book_value * sector_pb_map.get(
        sector, 2.0)`. Modern tech-driven companies trade at legitimate
        P/B of 5-10x for moats / scale-economics reasons; the v5.76.0
        1.5x universal anchor flagged them as 80%+ overvalued.
        After fix: GOOGL intrinsic = $310.59 (was $177.08), upside =
        -20.1% (was -54.5%). NVDA/META/MSFT see similar corrections.

Bug 2 - _phase_ii_quality_forecast unconditionally overwrote provider
        analyst targets. The function ran twice per quote (pre-scoring
        and post-scoring via _apply_phase_dd_enhancements), and BOTH
        runs blindly overwrote forecast_price_12m, even when the
        upstream provider had supplied a legitimate analyst target.

  Fix:
    (a) New field `forecast_source` tags the origin: provider_target /
        phase_ii_synthetic / fallback / unavailable. Phase-II skips the
        overwrite when forecast_source == "provider_target".
    (b) RSI bump (-3% / +3%) moved from 12M return to 1M/3M only.
        14-day RSI has no statistical predictive power on 12-month
        returns; applying it there corrupted the long-horizon anchor.
        Now applied only to the short-horizon derivatives.
    (c) Intrinsic-reversion component weight reduced from 0.40 to 0.25;
        momentum component raised from 0.30 to 0.35; fundamentals from
        0.20 to 0.25; baseline raised 0.10 to 0.15. Total still sums to
        1.0 but the intrinsic-reversion drag is meaningfully reduced.
    (d) `forecast_capped_at_ceiling` / `forecast_capped_at_floor`
        warnings fire when the 12M return hits the ±30% clamp so
        downstream consumers know the value is a guardrail, not a
        per-symbol prediction.

Bug 3 - MultiLevelCache used FIFO eviction instead of LRU. Hot keys
        (frequently-accessed blue chips: AAPL, MSFT, NVDA, 2222.SR)
        were evicted constantly because the eviction picked next(iter()) -
        the first INSERTED key, not the least RECENTLY USED key.

  Fix:
    (a) _data switched from Dict to OrderedDict.
    (b) get() calls move_to_end() on hit so accessed keys become MRU.
    (c) Eviction now picks popitem(last=False) - the OLDEST USED key.

Bug 4 - _V573_SANITIZATION_BOUNDS allowed nonsensical negative ratios.
        v5.76.0 bounds permitted pe_ttm in (-1000, 1000), pb_ratio in
        (-200, 200), ev_ebitda in (-500, 500), peg_ratio in (-100, 100).
        Negative P/E means negative earnings (valuation undefined),
        negative P/B means negative shareholders' equity (often
        distressed), negative EV/EBITDA means negative EBITDA. These
        should null out, not pass through as raw numbers.

  Fix:
    (a) pe_ttm / pe_forward: (-1000, 1000) -> (0.0, 500.0)
    (b) pb_ratio:           (-200, 200)    -> (0.0, 100.0)
    (c) ps_ratio:           (0, 200)       -> (0.0, 100.0)
    (d) ev_ebitda:          (-500, 500)    -> (0.0, 200.0)
    (e) peg_ratio:          (-100, 100)    -> (0.0, 20.0)
    (f) debt_to_equity:     (0, 1000)      -> (0.0, 500.0)

Bug 5 - No dividend_yield sanitizer. Dashboard showed yields like
        BBCA 145%, MDLZ 138%, RACE 124% - obvious provider scale-factor
        errors passed through unchecked. _build_top_factors_and_risks
        even flagged these as positive "Dividend income" factors.

  Fix:
    (a) New _sanitize_dividend_yield function nulls yields > 30% and
        emits a "dividend_yield_out_of_range" warning. The 30% ceiling
        is generous (only a few mortgage REITs and BDCs legitimately
        exceed 15%).

Bug 6 - _as_float silently misinterpreted booleans as 0.0 / 1.0. If a
        provider schema shift sent `{"dividend_yield": False}`, the
        engine treated it as 0.0. Defensive engineering gap.

  Fix:
    (a) Bool guard added at top of _as_float.

Bug 7 - _call_maybe_async used asyncio.to_thread's default executor
        (capped at min(32, cpu_count+4) ~36 threads). Batch processing
        200+ symbols can exhaust the default pool and stall the event
        loop. With 8 call sites all using the same default pool,
        contention under load is real.

  Fix:
    (a) Dedicated ThreadPoolExecutor `_PROVIDER_EXECUTOR` with 200
        workers (configurable via TFB_PROVIDER_POOL_WORKERS env).
    (b) _call_maybe_async routes sync calls through the dedicated
        executor instead of asyncio.to_thread.

Bug 8 - Sheet-compat collapse left recommendation_reason out of sync.
        When TFB_COLLAPSE_RECOMMENDATION_TO_6TIER=true is set,
        recommendation becomes "BUY" but recommendation_reason still
        starts with "ACCUMULATE: ...". Visible mismatch.

  Fix:
    (a) _apply_sheet_compat_collapse now rewrites the leading prefix
        of recommendation_reason ("ACCUMULATE: ..." -> "BUY: ...",
        "AVOID: ..." -> "STRONG_SELL: ...") to keep the displayed
        action consistent with the collapsed recommendation.

PRESERVED - strictly:
  All v5.76.0 / v5.75.0 / v5.74.0 / v5.73.x architectural behavior.
  No API shape changes. No `data_quality` semantics changes. All
  canonical contract shapes (106 fields) unchanged. Atomic-write
  contract in _classify_recommendation_8tier preserved verbatim.
  Cache namespace versioning unchanged. v5.75.0 once-only
  provider_rating capture preserved. v5.76.0 8-tier vocabulary
  passthrough preserved. The sheet-compat collapse opt-in via
  TFB_COLLAPSE_RECOMMENDATION_TO_6TIER preserved with default false.

DEFERRED - Phase 2 (separate release):
  - Apps Script 04_Format.gs v2.9.0 deployment verification
  - Phase-DD pre-score invocation elimination (currently the first
    Phase-DD pass runs intrinsic + Phase-II forecast even though
    scores aren't available yet - the classifier branch is skipped
    via has_scores guard, but the forecast still runs. Net effect
    is wasted CPU on the first pass, since the second pass overwrites
    with full-input forecast anyway. Cleanup deferred to v5.78.0.)
  - file split along seams named in the v5.74.0 WHY block
  - core.scoring threshold recalibration
  - yahoo_fundamentals_provider.py foreign-ticker gap

================================================================================
WHY v5.76.0 - 8-TIER VOCABULARY PASSTHROUGH + CROSS-STACK CONTRACT MARKERS
-------------------------------------------------------------------------
v5.76.0 closes the last remaining 6-tier choke point in the TFB stack.
Every other module in the family is already on the 8-tier canonical
vocabulary (ACCUMULATE / STRONG_SELL / AVOID added):
  - core.reco_normalize          v8.0.0
  - core.scoring                  v5.7.0
  - core.scoring_engine (bridge) v3.6.0
  - core.sheets.schema_registry  v2.11.0

v5.75.0 was the lone holdout. Its `_V573_RECOMMENDATION_ENUM` was the
6-tier set (STRONG_BUY / BUY / HOLD / REDUCE / SELL / STRONG_SELL), and
`_V573_LEGACY_COLLAPSE = {ACCUMULATE: BUY, AVOID: STRONG_SELL}` was
applied unconditionally by `_v573_collapse_to_canonical_enum` to every
recommendation flowing through the classifier. The result: when
scoring.py v5.7.0 correctly emitted ACCUMULATE for a moderate-bullish
scale-in signal, the engine silently rewrote it back to BUY before the
row was written to the sheet. The new ACCUMULATE / AVOID distinctions
that scoring v5.7.0 and reco_normalize v8.0.0 worked hard to produce
were destroyed at the engine boundary.

v5.76.0 routes the 8-tier vocabulary through end-to-end by default
and provides an explicit opt-in for callers that still need the 6-tier
projection (Apps Script renderers, legacy dashboards). No removals;
all v5.73.x / v5.74.0 / v5.75.0 symbols preserved with their names
intact. The behavior change is gated by env so a deployment without
the new env flag set behaves exactly as v5.75.0 did *except* that
ACCUMULATE/AVOID no longer collapse — which is the whole point.

What changes in v5.76.0
-----------------------

  Phase A — Header docstring sync. Floor moves to scoring.py v5.7.0+
            and reco_normalize v8.0.0+ as documented above.

  Phase B — `__version__` bumped 5.75.0 → 5.76.0. New module-level
            constants surface the cross-stack contract alignment:
              * _SCORING_CONTRACT_VERSION         = "5.7.0"
              * _RECO_NORMALIZE_CONTRACT_VERSION  = "8.0.0"
            Both are echoed in health() and get_stats() output so
            ops tooling can verify cross-stack lockstep without
            importing each module separately.

  Phase C — 8-tier vocabulary as the canonical interior representation.
              * `_V573_RECOMMENDATION_ENUM` expanded from 6 tiers to 8:
                STRONG_BUY / BUY / ACCUMULATE / HOLD / REDUCE / SELL /
                STRONG_SELL / AVOID.
              * `_V573_LEGACY_COLLAPSE` becomes the empty dict by
                default. The symbol is preserved (its name is referenced
                by some external integrations, so deleting it would
                break import compatibility), and its semantics are
                clearly documented as "no longer applied by default."
              * `_v573_collapse_to_canonical_enum` updated to
                canonicalize TO the 8-tier set rather than collapse
                DOWN to the 6-tier set. The function name is preserved
                for back-compat; its docstring documents the v5.76.0
                semantics change.
              * Fallback `_SCORING_RECOMMENDATION_ENUM` (used when
                core.scoring can't be imported) widened from 6 tiers
                to 8.

  Phase D — Sheet-compatibility opt-in via env flag:
              `TFB_COLLAPSE_RECOMMENDATION_TO_6TIER` (default: false).
              When set, `_apply_sheet_compat_collapse(row)` is called
              at the end of `_classify_recommendation_8tier` to map
              ACCUMULATE → BUY and AVOID → STRONG_SELL on the
              recommendation / recommendation_detailed /
              recommendation_priority / recommendation_priority_band
              fields. The provider_rating audit field is NOT collapsed
              (it carries the upstream value, not the engine's output).
              This lets callers that haven't yet migrated their
              renderer code (Apps Script 04_Format.gs at time of cut)
              continue to receive only the legacy 6 tokens until
              they're ready.

  Phase E — Provider override priority bands extended for 8 tiers.
              In `_classify_recommendation_8tier` Step 3a (the rare
              path where TFB_TRUST_PROVIDER_RECO=true accepts the
              provider's rating verbatim), the priority_band mapping
              gains:
                * ACCUMULATE → P3   (between BUY=P2 and HOLD=P4)
                * AVOID      → P1   (urgent exit, like STRONG_SELL)
              Pre-existing mappings (STRONG_BUY=P1, BUY=P2,
              STRONG_SELL=P1, SELL/REDUCE=P5, else P4) preserved
              verbatim.

  Phase F — `_RECO_8TIER_PRIORITY` integer-rank alignment.
              ACCUMULATE moves from rank 2 (legacy collapse equivalent
              of BUY) to rank 3 — sitting between BUY=2 and HOLD=4 in
              the 8-tier ordinal table. AVOID stays at rank 5 alongside
              STRONG_SELL/SELL/REDUCE. The two are still distinguished
              by priority_band (AVOID=P1, REDUCE=P5) — that's the
              urgency axis; the integer rank is the conviction axis.

  Phase G — `_recommendation_priority` debug log wording updated.
              Previously warned that ACCUMULATE/AVOID arriving in the
              priority map signaled an upstream canonicalization miss
              (because they were supposed to collapse upstream). In
              v5.76.0 those values are first-class and the warning is
              dropped. The defensive fallback `_RECO_8TIER_PRIORITY`
              entries for ACCUMULATE/AVOID stay but no longer
              indicate a bug.

  Phase H — `_compute_recommendation` simplified. The redundant
              `_clear_recommendation_output_fields(row)` call is
              removed (the v5.75.0 classifier self-clears at entry).
              The clearing helper itself is preserved as a public
              symbol for any external caller that still references it.

  Phase I — Health and stats endpoints surface the contract markers.
              `health()` adds an "8tier_alignment" block with
              scoring_contract_version, reco_normalize_contract_version,
              sheet_collapse_to_6tier_enabled, and a list of the 8
              canonical tiers. `get_stats()` mirrors the same block.

PRESERVED — strictly
  All v5.75.0 / v5.74.0 / v5.73.x architectural behavior. No API shape
  changes. No `data_quality` semantics changes. All canonical contract
  shapes (97 + 9 = 106 fields) unchanged. Cache namespace versioning
  unchanged. The v5.75.0 atomic-write contract in
  `_classify_recommendation_8tier` is preserved (Steps 2a/2b/3a/3b/3c/4
  in the same order; the only changes are at Step 3a's priority-band
  table and a new optional Step 4b for the sheet-compat collapse).
  v5.75.0's once-only provider_rating capture is preserved verbatim.

DEFERRED — Phase 2 (separate release)
  - Apps Script 04_Format.gs update to render ACCUMULATE/AVOID natively
    (color, icon, tooltip). Until then, operators wanting old-style
    sheets set TFB_COLLAPSE_RECOMMENDATION_TO_6TIER=true.
  - `data_quality` semantics rework (Phase 1.5).
  - File split along seams named in the v5.74.0 WHY block.
  - `yahoo_fundamentals_provider.py` foreign-ticker gap.
  - `core.scoring` threshold recalibration.

================================================================================
WHY v5.75.0 - NAME FALLBACK FIX + RECOMMENDATION GOVERNANCE TIGHTENING
----------------------------------------------------------------------
v5.75.0 is a focused quality patch over v5.74.0. It fixes two production
bugs surfaced by the May 20 dashboard refresh and tightens silent
fall-through paths flagged by audit. No API shape changes, no
`data_quality` semantics changes (deferred to Phase 1.5).

Bug 1 - Name fallback stamps the ticker symbol as the Name.
  The dashboard showed Name=DBA.US / SCHD.US / NDA-FI.HE / SOXX - the
  ticker symbol itself in the Name column. Root cause:
  `_infer_display_name_from_symbol` returned `s` (the symbol) as a final
  fallback for any equity not in the static `_COMMODITY_DISPLAY_NAMES`
  or `_ETF_DISPLAY_NAMES` dicts. Then `_canonicalize_provider_row` and
  `_apply_symbol_context_defaults` stamped this symbol-as-name into
  `row["name"]` BEFORE the Yahoo enrichment pass ran. Yahoo's
  `_filter_patch_to_missing_fields` then treated `name="DBA.US"` as
  populated (not blank, not in `_YAHOO_UNKNOWN_STRINGS`), so the real
  name from Yahoo fundamentals (`longName` / `shortName`) was never
  copied over. The symbol-as-name persisted through to the sheet.

  Fix in three coordinated places:
    - `_infer_display_name_from_symbol` returns "" for unknown equities
      instead of returning the symbol itself. Commodity/ETF/FX symbols
      with known display names are unchanged.
    - `_canonicalize_provider_row` only writes `out["name"]` when a
      meaningful inferred name exists. Otherwise leaves it blank so
      downstream enrichment can fill it.
    - `_apply_symbol_context_defaults` follows the same rule: never
      stamps `sym` as the name. An empty name is the honest signal
      that enrichment should chase it.
  After this fix the Yahoo enrichment pass detects missing name (empty
  string registers as missing via `_is_missing_or_unknown_field`) and
  populates it from the fundamentals provider's `longName` /
  `shortName` / `displayName` fields. If the provider also fails to
  return a name (e.g. for foreign tickers where
  `yahoo_fundamentals_provider` only returns the analyst
  recommendation), the Name column stays blank rather than misleadingly
  showing the ticker.

Bug 2 - Recommendation/Detail/Reason/Priority divergence on the sheet.
  The dashboard showed DBA.US with Recommendation=HOLD, Recommendation
  Detail=BUY, Recommendation Reason="BUY: ...", Reco Priority=2 (BUY's
  priority), and Provider Rating=REDUCE. The five fields are supposed
  to be derived atomically by `_classify_recommendation_8tier`. Three
  governance gaps allowed them to drift:
    (a) `provider_rating` was captured by reading `row["recommendation"]`
        at classifier-entry, but the classifier itself writes
        `row["recommendation"]` at the end. On the second and
        subsequent classifier passes (within
        `_apply_phase_dd_enhancements` and again at sheet-level),
        `row["recommendation"]` was the engine's previous output, not
        the provider's value. The classifier then overwrote
        `provider_rating` with the engine's own previous
        recommendation, destroying the audit trail.
    (b) `recommendation_priority_band` was written conditionally
        (`if priority_band: row[...] = priority_band`). On runs where
        scoring did not emit a priority_band, the field was NOT
        written and a stale value from a previous classification
        could survive, producing inconsistency with the freshly
        written recommendation.
    (c) The classifier ran 3x per quote (once via the explicit
        `_compute_recommendation` call, and twice more inside the
        post-scoring DD pass). Each pass had the chance to write
        inconsistent state if anything between writes mutated the
        row, and the cost was paying for three full delegations into
        `core.scoring`.

  Fix:
    - `provider_rating` is captured ONCE per row. The classifier
      checks `if not row.get("provider_rating")` before writing -
      once set, it's preserved as audit evidence even on later
      classifier passes.
    - `_classify_recommendation_8tier` always writes ALL six
      recommendation fields atomically. `recommendation_priority_band`
      is written unconditionally (empty string if not derived), so
      a stale value cannot survive across passes.
    - The classifier now self-clears its output fields at entry, so
      callers don't need a paired `_clear_recommendation_output_fields`
      call. This eliminates a window where a partial clear plus a
      skipped write could leave inconsistent state.
    - `_apply_phase_dd_enhancements` runs the classifier ONCE per
      invocation (down from twice). With the two DD invocations in
      `_get_enriched_quote_impl` (pre-scoring DD has no scores so
      classifier doesn't run; post-scoring DD has scores so it does),
      the engine settles on the final classification through a
      single authoritative pass.

P1 quality improvements:
  - Silent `try / except` blocks in `_fetch_patch`,
    `_call_rows_reader`, and `_call_symbols_reader` now
    `logger.debug()` the swallowed exception. Failures stay
    non-fatal but become observable.
  - `MultiLevelCache` gains a docstring clarifying it's currently
    L1 in-memory only (no L2 yet); name preserved for back-compat.
  - `_RECO_8TIER_PRIORITY` legacy `ACCUMULATE` / `AVOID` entries kept
    as defensive fallback, but a debug warning fires if a value
    reaches the map without first being canonicalized - signals an
    upstream bug.

PRESERVED - strictly:
  All v5.74.0 / v5.73.x architectural deltas. No API shape changes.
  No `data_quality` semantics changes (deferred to v5.75.1 pending
  Apps Script audit). All canonical contract shapes (97 + 9 = 106
  fields) unchanged. Cache namespace versioning unchanged. Yahoo
  enrichment field maps, intrinsic-value sanity bounds, Phase-II
  forecast formula, candlestick detection, and all v5.74.0 scoring
  orchestration helpers (`_compute_scores_canonical_first`,
  `_preserve_scoring_provenance`, `_empty_row_fundamentals_exempt`)
  unchanged.

DEFERRED - Phase 2 (separate release):
  - File split along seams named in the v5.74.0 WHY block.
  - `data_quality` semantics rework (Phase 1.5).
  - Apps Script writer fix for blank Confidence Score / Data Provider
    / Last Updated columns (engine emits correctly).
  - `yahoo_fundamentals_provider.py` foreign-ticker gap.
  - `core.scoring` threshold recalibration.

================================================================================
WHY v5.74.0 — SCORING SEQUENCE + ASSET-AWARE EMPTY GUARD + SCHEMA EXPANSION
----------------------------------------------------------------------------
v5.74.0 is a controlled production patch over v5.73.2. It preserves the
v5.73.2 persistent-cache invalidation fix and applies only the remaining
orchestration and projection fixes surfaced by the post-deploy audit.

v5.74.0 CHANGES vs v5.73.2
  Issue 1 — scoring sequence and stale recommendation outputs.
    The quote pipeline now prepares intrinsic/upside and Phase-II forecast
    inputs before the authoritative scoring pass, then clears stale
    recommendation detail/reason/priority/source fields immediately before each
    final recommendation rewrite. This removes the window where a downstream
    reader could see a recommendation tuple built on default confidence or
    pre-forecast ROI. The legacy local scoring entry point is retained only as a
    compatibility delegator; the new _compute_scores_canonical_first path tries
    core.scoring.compute_scores first and falls back to the local engine scorer
    only if the canonical scorer is unavailable or raises.

  Issue 2 — asset-aware empty-row guard.
    The fundamentals-empty guard remains active for equity rows, including
    foreign equities where provider fundamentals failed, but it no longer marks
    FX pairs, futures, commodities, ETFs, funds, mutual funds, indices, or the
    Commodities_FX / Mutual_Funds pages as empty merely because equity
    fundamentals such as EPS, P/E, revenue, or market cap are absent.

  Issue 3 — canonical schema expansion 97 -> 106.
    The instrument schema now projects the nine scoring/provenance fields used
    by the v5.74.0 engine and core.scoring v5.4.2 contract:
      provider_rating, recommendation_source, recommendation_priority_band,
      scoring_recommendation_source, scoring_schema_version, scoring_errors,
      opportunity_source, overall_score_raw, overall_penalty_factor.
    The fields are added consistently to INSTRUMENT_CANONICAL_KEYS,
    INSTRUMENT_CANONICAL_HEADERS, _INSTRUMENT_CANONICAL_REQUIRED_KEYS, and
    _CANONICAL_FIELD_ALIASES so projection, schema enforcement, and sheet output
    stay aligned.

  Issue 4 — scoring provenance preservation.
    The engine continues to use its own recommendation_source vocabulary
    (engine / provider_override / empty_row / scoring_unavailable), but the
    scoring.py provenance tag is preserved separately in
    scoring_recommendation_source. Additional scoring.py diagnostics are kept
    when present: scoring_errors, scoring_schema_version, opportunity_source,
    overall_score_raw, and overall_penalty_factor.

PRESERVED — strictly:
  The v5.73.2 cache namespace fix, _make_cache_key and versioned_provider_profile
  pairing, provider recommendation trust gate, canonical 6-tier vocabulary,
  v5.67.0 unit-contract behavior, v5.68.0 provider symbol normalization,
  v5.70.0 schema-alignment behavior, v5.73.0 sanitization bounds, and the
  legacy all-empty guard remain intact.

================================================================================
[Older WHY blocks (v5.73.2 through v5.47.2 baseline) are preserved in
CHANGELOG.md. All behavior described in those WHY blocks is intact in the
code below.]

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

__version__ = "5.77.5"

# v5.76.0 cross-stack contract version markers.
# These document which core.scoring / core.reco_normalize releases
# v5.76.0 was built to align with. They are READ symbolically by the
# health() and get_stats() endpoints so operators can verify that the
# running data_engine_v2 build is in lockstep with the rest of the
# stack, without separately importing each module. Mirror of the
# markers core.scoring v5.7.0 publishes as _ENGINE_CONTRACT_VERSION /
# _RECO_NORMALIZE_CONTRACT_VERSION.
_SCORING_CONTRACT_VERSION: str = "5.7.0"
_RECO_NORMALIZE_CONTRACT_VERSION: str = "8.0.0"

logger = logging.getLogger("core.data_engine_v2")
logger.addHandler(logging.NullHandler())


# =============================================================================
# v5.68.0 — core.symbols.normalize integration (provider symbol routing)
# =============================================================================
try:
    from core.symbols.normalize import (
        to_yahoo_symbol as _nz_to_yahoo_symbol,
        to_eodhd_symbol as _nz_to_eodhd_symbol,
        to_finnhub_symbol as _nz_to_finnhub_symbol,
        normalize_symbol_for_provider as _nz_normalize_symbol_for_provider,
        get_country_from_symbol as _nz_get_country_from_symbol,
        get_currency_from_symbol as _nz_get_currency_from_symbol,
        get_primary_exchange as _nz_get_primary_exchange,
    )
    _NORMALIZE_AVAILABLE = True
except Exception:  # pragma: no cover
    _nz_to_yahoo_symbol = None  # type: ignore
    _nz_to_eodhd_symbol = None  # type: ignore
    _nz_to_finnhub_symbol = None  # type: ignore
    _nz_normalize_symbol_for_provider = None  # type: ignore
    _nz_get_country_from_symbol = None  # type: ignore
    _nz_get_currency_from_symbol = None  # type: ignore
    _nz_get_primary_exchange = None  # type: ignore
    _NORMALIZE_AVAILABLE = False


# =============================================================================
# v5.69.0 — core.reco_normalize integration
# =============================================================================
try:
    from core.reco_normalize import (
        normalize_recommendation as _rn_normalize_recommendation,
        is_valid_recommendation as _rn_is_valid_recommendation,
        get_recommendation_score as _rn_get_recommendation_score,
    )
    _RECO_NORMALIZE_AVAILABLE = True
except Exception:  # pragma: no cover
    _rn_normalize_recommendation = None  # type: ignore
    _rn_is_valid_recommendation = None  # type: ignore
    _rn_get_recommendation_score = None  # type: ignore
    _RECO_NORMALIZE_AVAILABLE = False

# =============================================================================
# v5.73.0 — core.scoring integration
# =============================================================================
try:
    from core.scoring import (
        _recommendation as _scoring_recommendation,
        _risk_bucket as _scoring_risk_bucket,
        _confidence_bucket as _scoring_confidence_bucket,
    )
    try:
        from core.scoring import (
            apply_canonical_recommendation as _scoring_apply_canonical,
        )
        _SCORING_APPLY_CANONICAL_AVAILABLE = True
    except Exception:
        _scoring_apply_canonical = None  # type: ignore
        _SCORING_APPLY_CANONICAL_AVAILABLE = False
    try:
        from core.scoring import RECOMMENDATION_ENUM as _SCORING_RECOMMENDATION_ENUM
    except Exception:
        # v5.76.0: 8-tier fallback. ACCUMULATE / AVOID added.
        _SCORING_RECOMMENDATION_ENUM = (
            "STRONG_BUY", "BUY", "ACCUMULATE", "HOLD",
            "REDUCE", "SELL", "STRONG_SELL", "AVOID",
        )
    try:
        from core.scoring import compute_scores as _scoring_compute_scores
        _SCORING_COMPUTE_SCORES_AVAILABLE = True
    except Exception:
        _scoring_compute_scores = None  # type: ignore
        _SCORING_COMPUTE_SCORES_AVAILABLE = False
    _CORE_SCORING_AVAILABLE = True
except Exception:  # pragma: no cover
    _scoring_recommendation = None  # type: ignore
    _scoring_risk_bucket = None  # type: ignore
    _scoring_confidence_bucket = None  # type: ignore
    _scoring_compute_scores = None  # type: ignore
    _scoring_apply_canonical = None  # type: ignore
    _SCORING_APPLY_CANONICAL_AVAILABLE = False
    _SCORING_COMPUTE_SCORES_AVAILABLE = False
    # v5.76.0: 8-tier fallback when core.scoring is entirely unavailable.
    _SCORING_RECOMMENDATION_ENUM = (
        "STRONG_BUY", "BUY", "ACCUMULATE", "HOLD",
        "REDUCE", "SELL", "STRONG_SELL", "AVOID",
    )
    _CORE_SCORING_AVAILABLE = False

# v5.76.0: 8-tier canonical recommendation vocabulary. Mirror of the
# RECOMMENDATION_ENUM tuple exported by core.scoring v5.7.0+ and the
# Recommendation enum in core.reco_normalize v8.0.0+.
# ACCUMULATE sits between BUY and HOLD (a scale-in / moderate-bullish
# call); AVOID sits below STRONG_SELL (uninvestable / hard pass).
# Despite the historical _V573_* name, this set is the v5.76.0 contract.
# The name is preserved so external modules that imported the symbol
# don't break at module load.
_V573_RECOMMENDATION_ENUM = frozenset({
    "STRONG_BUY", "BUY", "ACCUMULATE", "HOLD",
    "REDUCE", "SELL", "STRONG_SELL", "AVOID",
})

# v5.76.0: legacy collapse map. v5.73.0 — v5.75.0 used this to fold
# ACCUMULATE → BUY and AVOID → STRONG_SELL at the engine boundary,
# back when downstream consumers (sheets, Apps Script renderers) were
# 6-tier-only. v5.76.0 makes those tiers first-class so the default
# collapse map is empty. The symbol is preserved (some external
# integrations referenced it by name) and the dict can be repopulated
# by a caller that explicitly wants the legacy behavior — though the
# recommended path is to set TFB_COLLAPSE_RECOMMENDATION_TO_6TIER=true
# and let `_apply_sheet_compat_collapse()` handle it cleanly.
_V573_LEGACY_COLLAPSE: Dict[str, str] = {}

# v5.76.0: opt-in sheet-compatibility 6-tier collapse. When enabled
# via env, applied AT THE END of _classify_recommendation_8tier so
# the row's recommendation / recommendation_detailed /
# recommendation_priority / recommendation_priority_band fields are
# coerced from the 8-tier canonical down to the legacy 6-tier set
# right before they leave the engine. Internal pipeline (view
# derivation, factor building, position size hint) operates on the
# 8-tier value first; only the *output-facing* fields are collapsed.
# provider_rating is NOT collapsed — it's audit evidence of what the
# upstream provider said.
_V576_SHEET_COMPAT_COLLAPSE: Dict[str, str] = {
    "ACCUMULATE": "BUY",
    "AVOID":      "STRONG_SELL",
}


def _sheet_collapse_to_6tier_enabled() -> bool:
    """v5.76.0: True when TFB_COLLAPSE_RECOMMENDATION_TO_6TIER is set.

    Controls whether `_apply_sheet_compat_collapse(row)` runs at the
    end of `_classify_recommendation_8tier`. Default False — the
    engine emits the full 8-tier vocabulary so it matches scoring.py
    v5.7.0+, reco_normalize v8.0.0+, schema_registry v2.11.0+, and
    scoring_engine v3.6.0+. Set True for callers whose downstream
    renderers (Apps Script 04_Format.gs at time of cut) haven't been
    updated to handle ACCUMULATE / AVOID natively.
    """
    raw = os.getenv("TFB_COLLAPSE_RECOMMENDATION_TO_6TIER", "")
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on", "enabled")


def _v573_trust_provider_reco() -> bool:
    raw = os.getenv("TFB_TRUST_PROVIDER_RECO", "")
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on", "enabled")


def _v573_collapse_to_canonical_enum(value: Any) -> str:
    """v5.76.0: Canonicalize to the 8-tier vocabulary.

    Despite the historical `_collapse_` name, this no longer collapses
    ACCUMULATE → BUY or AVOID → STRONG_SELL. It accepts any input
    string (case / whitespace / dash / underscore variations, plus
    reco_normalize-recognized aliases like SCALE_IN → ACCUMULATE,
    UNINVESTABLE → AVOID, etc.) and returns the canonical 8-tier
    code, or "" if the input is not recognizable.

    The function name is preserved for back-compat (some external
    modules reference it by name). The behavioral change is gated by
    the data shape: _V573_RECOMMENDATION_ENUM is now the 8-tier set
    and _V573_LEGACY_COLLAPSE is empty by default, so the
    canonicalization lattice routes everything to 8 tiers.

    Sheet-compatibility 6-tier collapse is applied separately by
    _apply_sheet_compat_collapse(row) when the
    TFB_COLLAPSE_RECOMMENDATION_TO_6TIER env flag is set — never
    inside this function.
    """
    if value is None:
        return ""
    s = _safe_str(value).strip().upper().replace(" ", "_").replace("-", "_")
    if not s:
        return ""
    # Direct match against the 8-tier canonical set. This is now the
    # primary path; the _V573_LEGACY_COLLAPSE lookup below is a no-op
    # when the dict is empty (its v5.76.0 default), but the code path
    # is preserved so an external caller can repopulate
    # _V573_LEGACY_COLLAPSE if they explicitly want legacy behavior.
    if s in _V573_RECOMMENDATION_ENUM:
        return s
    if s in _V573_LEGACY_COLLAPSE:
        return _V573_LEGACY_COLLAPSE[s]
    if _RECO_NORMALIZE_AVAILABLE and _rn_normalize_recommendation is not None:
        try:
            canon = _safe_str(_rn_normalize_recommendation(value)).upper()
            if canon in _V573_RECOMMENDATION_ENUM:
                return canon
            if canon in _V573_LEGACY_COLLAPSE:
                return _V573_LEGACY_COLLAPSE[canon]
        except Exception:
            pass
    return ""


def _apply_sheet_compat_collapse(row: Dict[str, Any]) -> None:
    """v5.76.0: Sheet-compatibility 6-tier output collapse.

    When TFB_COLLAPSE_RECOMMENDATION_TO_6TIER=true is set, the engine
    rewrites the *output-facing* recommendation fields from the 8-tier
    canonical to the legacy 6-tier set just before the row leaves the
    classifier. Specifically:
      * recommendation:                 ACCUMULATE → BUY, AVOID → STRONG_SELL
      * recommendation_detailed:        same as recommendation (mirror)
      * recommendation_priority:        re-derived from the collapsed value
      * recommendation_priority_band:   re-derived from the collapsed value
      * recommendation_reason:          leading prefix only (e.g.
                                        "ACCUMULATE: Scale in..." → "BUY:
                                        Scale in..."). The rest of the
                                        reason text is preserved so the
                                        structured-reason audit trail
                                        remains intact. Added in v5.77.0
                                        to fix the visible mismatch where
                                        the recommendation column showed
                                        "BUY" while the reason column
                                        still led with "ACCUMULATE:".

    Fields NOT modified:
      * provider_rating          (audit evidence; the upstream provider's value)
      * recommendation_source    (engine / provider_override / etc; unaffected)
      * scoring_recommendation_source (scoring.py provenance tag preserved)

    The 8-tier source value is still inferable via priority_band (P3
    suggested ACCUMULATE; P1 with non-STRONG_SELL reason suggested
    AVOID), but the renderer-facing column carries only the legacy 6.

    Intended for callers whose downstream sheet renderer (Apps Script
    04_Format.gs) hasn't been updated to handle ACCUMULATE / AVOID
    natively. Once 04_Format.gs gains 8-tier rendering, the env flag
    can be unset and ACCUMULATE / AVOID flow through to the sheet
    unmodified.
    """
    if not isinstance(row, dict):
        return
    rec = _safe_str(row.get("recommendation")).upper()
    if rec not in _V576_SHEET_COMPAT_COLLAPSE:
        return  # already 6-tier or unrecognized; no-op.

    collapsed = _V576_SHEET_COMPAT_COLLAPSE[rec]
    row["recommendation"] = collapsed
    row["recommendation_detailed"] = collapsed
    # Re-derive priority integer from the collapsed value so the int
    # rank matches what's now in the recommendation column.
    row["recommendation_priority"] = _recommendation_priority(collapsed)
    # Re-derive priority_band from the collapsed value. The 8-tier
    # band mappings: STRONG_BUY=P1, BUY=P2, ACCUMULATE=P3, HOLD=P4,
    # REDUCE=P5, SELL=P5, STRONG_SELL=P1, AVOID=P1. After collapse:
    #   ACCUMULATE→BUY     P3 → P2
    #   AVOID→STRONG_SELL  P1 → P1 (no change)
    current_band = _safe_str(row.get("recommendation_priority_band"))
    if rec == "ACCUMULATE" and current_band == "P3":
        row["recommendation_priority_band"] = "P2"
    # AVOID → STRONG_SELL: priority_band stays P1.
    # v5.77.0: rewrite the leading prefix of recommendation_reason so
    # the displayed action stays consistent with the collapsed value.
    # Previously the reason text kept its original 8-tier prefix
    # ("ACCUMULATE: Scale in...") while the recommendation column
    # showed "BUY", producing a visible mismatch in the dashboard.
    # Only the prefix is rewritten; the rest of the reason text is
    # preserved so the structured-reason audit trail remains intact.
    reason_text = _safe_str(row.get("recommendation_reason"))
    if reason_text:
        for orig_prefix, new_prefix in (
            (rec + ":", collapsed + ":"),
            (rec + " :", collapsed + " :"),
        ):
            if reason_text.startswith(orig_prefix):
                row["recommendation_reason"] = new_prefix + reason_text[len(orig_prefix):]
                break
    _v573_append_warning(row, "sheet_compat_6tier_collapse_applied")


# =============================================================================
# v5.69.0 — core.buckets integration
# =============================================================================
try:
    from core.buckets import (
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
# v5.62.0 PHASE-Z — Yahoo enrichment field maps and helpers
# =============================================================================

_YAHOO_FUNDAMENTAL_FIELDS: Tuple[str, ...] = (
    "industry", "sector", "currency", "country", "name",
    "market_cap", "float_shares", "shares_outstanding",
    "pe_ttm", "pe_forward", "eps_ttm", "eps_forward",
    "dividend_yield", "payout_ratio", "beta_5y",
    "gross_margin", "operating_margin", "profit_margin",
    "debt_to_equity", "revenue_ttm", "revenue_growth_yoy",
    "free_cash_flow_ttm", "roe", "roa", "earnings_growth_yoy",
    "pb_ratio", "ps_ratio", "peg_ratio", "ev_ebitda",
    "target_mean_price", "target_high_price", "target_low_price",
    "analyst_count", "recommendation",
)

_YAHOO_CHART_FIELDS: Tuple[str, ...] = (
    "rsi_14", "volatility_30d", "volatility_90d",
    "max_drawdown_1y", "var_95_1d", "sharpe_1y",
    "week_52_high", "week_52_low", "week_52_position_pct",
    "avg_volume_10d", "avg_volume_30d",
    "candlestick_pattern", "candlestick_signal",
    "candlestick_strength", "candlestick_confidence",
    "candlestick_patterns_recent",
    "current_price", "previous_close", "open_price",
    "day_high", "day_low", "volume",
)

_YAHOO_UNKNOWN_STRINGS: Set[str] = {
    "", "unknown", "unclassified", "n/a", "na", "none", "null",
    "nan", "-", "--", "not available",
}

_YAHOO_ENRICHMENT_LAST_PASS: Dict[str, Any] = {
    "ts": 0.0,
    "symbol": "",
    "fundamentals_called": False,
    "chart_called": False,
    "fundamentals_filled_fields": [],
    "chart_filled_fields": [],
}


def _yahoo_enrichment_enabled() -> bool:
    raw = (os.getenv("ENGINE_YAHOO_ENRICHMENT_ENABLED") or "").strip().lower()
    if raw in {"0", "false", "no", "n", "off", "f"}:
        return False
    return True


def _yahoo_enrich_on_missing_industry() -> bool:
    raw = (os.getenv("ENGINE_YAHOO_ENRICH_ON_MISSING_INDUSTRY") or "").strip().lower()
    if raw in {"0", "false", "no", "n", "off", "f"}:
        return False
    return True


def _yahoo_enrich_on_missing_risk_metrics() -> bool:
    raw = (os.getenv("ENGINE_YAHOO_ENRICH_ON_MISSING_RISK_METRICS") or "").strip().lower()
    if raw in {"0", "false", "no", "n", "off", "f"}:
        return False
    return True


def _is_missing_or_unknown_field(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        s = v.strip().lower()
        if s in _YAHOO_UNKNOWN_STRINGS:
            return True
        return False
    if isinstance(v, (list, tuple, set, dict)):
        return len(v) == 0
    return False


def _row_needs_yahoo_enrichment(row: Dict[str, Any]) -> Tuple[bool, bool]:
    if not isinstance(row, dict):
        return False, False

    needs_fund = False
    needs_chart = False

    if _yahoo_enrich_on_missing_industry():
        needs_fund = any(
            _is_missing_or_unknown_field(row.get(k))
            for k in _YAHOO_FUNDAMENTAL_FIELDS
        )

    if _yahoo_enrich_on_missing_risk_metrics():
        needs_chart = any(
            _is_missing_or_unknown_field(row.get(k))
            for k in _YAHOO_CHART_FIELDS
        )

    return needs_fund, needs_chart


def _filter_patch_to_missing_fields(
    row: Dict[str, Any],
    patch: Dict[str, Any],
    candidate_fields: Sequence[str],
) -> Tuple[Dict[str, Any], List[str]]:
    if not isinstance(patch, dict) or not patch:
        return {}, []

    filtered: Dict[str, Any] = {}
    filled: List[str] = []

    candidates: Set[str] = set(candidate_fields)
    for k, v in patch.items():
        if k not in candidates:
            continue
        if _is_missing_or_unknown_field(v):
            continue
        if not _is_missing_or_unknown_field(row.get(k)):
            continue
        filtered[k] = v
        filled.append(k)

    return filtered, filled


def _import_yahoo_provider_module(module_basename: str) -> Optional[Any]:
    candidates = (
        "core.providers." + module_basename,
        "providers." + module_basename,
    )
    for path in candidates:
        try:
            return import_module(path)
        except Exception:
            continue
    return None


_YAHOO_STRIP_SUFFIXES: Tuple[str, ...] = (
    ".US",
    ".us",
    ".USA",
    ".usa",
)

_YAHOO_SUFFIX_REMAP: Dict[str, str] = {
    ".XETRA": ".DE",
    ".XETR": ".DE",
    ".LSE": ".L",
    ".PAR": ".PA",
    ".AMS": ".AS",
    ".MIL": ".MI",
    ".MAD": ".MC",
    ".BRU": ".BR",
    ".STO": ".ST",
    ".HEL": ".HE",
    ".OSL": ".OL",
    ".CPH": ".CO",
    ".VIE": ".VI",
    ".WAR": ".WA",
    ".SWX": ".SW",
    ".SAU": ".SR",
    ".TADAWUL": ".SR",
    ".KSE": ".SR",
}


def _yahoo_symbol_for(symbol: str) -> str:
    if not isinstance(symbol, str):
        return ""
    s = symbol.strip()
    if not s:
        return ""

    if _NORMALIZE_AVAILABLE and _nz_to_yahoo_symbol is not None:
        try:
            nz = _nz_to_yahoo_symbol(s)
        except Exception:
            nz = ""
        if isinstance(nz, str) and nz.strip():
            return nz.strip()

    for suf in _YAHOO_STRIP_SUFFIXES:
        if s.endswith(suf):
            return s[: -len(suf)]

    last_dot = s.rfind(".")
    if last_dot > 0:
        head, tail = s[:last_dot], s[last_dot:]
        tail_upper = tail.upper()
        if tail_upper in _YAHOO_SUFFIX_REMAP:
            return head + _YAHOO_SUFFIX_REMAP[tail_upper]

    return s


def _provider_symbol_for(provider: str, symbol: str) -> str:
    if not isinstance(symbol, str):
        return ""
    raw = symbol.strip()
    if not raw:
        return ""
    prov = (provider or "").strip().lower()

    if prov in ("yahoo", "yfinance", "yahoo_chart"):
        try:
            out = _yahoo_symbol_for(raw)
            if isinstance(out, str) and out.strip():
                return out.strip()
        except Exception:
            pass
        return raw

    if not _NORMALIZE_AVAILABLE:
        return raw

    try:
        if prov == "eodhd" and _nz_to_eodhd_symbol is not None:
            out = _nz_to_eodhd_symbol(raw)
            if isinstance(out, str) and out.strip():
                return out.strip()
        elif prov == "finnhub" and _nz_to_finnhub_symbol is not None:
            out = _nz_to_finnhub_symbol(raw)
            if isinstance(out, str) and out.strip():
                return out.strip()
    except Exception:
        return raw

    return raw


_PERCENT_CHANGE_DAILY_MAX_ABS: float = 50.0
_WEEK_52_POSITION_MAX: float = 100.0


def _sanitize_percent_change(row: Dict[str, Any]) -> None:
    if not isinstance(row, dict):
        return

    try:
        raw = row.get("percent_change")
        raw_f = float(raw) if raw is not None and raw != "" else None
    except (TypeError, ValueError):
        raw_f = None

    try:
        cp = row.get("current_price")
        pc = row.get("previous_close")
        cp_f = float(cp) if cp is not None and cp != "" else None
        pc_f = float(pc) if pc is not None and pc != "" else None
    except (TypeError, ValueError):
        cp_f = pc_f = None

    recomputed: Optional[float] = None
    if cp_f is not None and pc_f is not None and pc_f != 0.0:
        recomputed = (cp_f - pc_f) / pc_f

    chosen = recomputed if recomputed is not None else raw_f
    if chosen is None:
        return

    if raw_f is not None and abs(raw_f) > 1.5 and recomputed is not None:
        if abs(recomputed) <= _PERCENT_CHANGE_DAILY_MAX_ABS_FRACTION:
            row["percent_change"] = round(recomputed, 8)
            _append_yahoo_warning_tag(row, "percent_change_recomputed")
            return

    if abs(chosen) > _PERCENT_CHANGE_DAILY_MAX_ABS_FRACTION:
        if recomputed is not None and abs(recomputed) <= _PERCENT_CHANGE_DAILY_MAX_ABS_FRACTION:
            row["percent_change"] = round(recomputed, 8)
            _append_yahoo_warning_tag(row, "percent_change_clamped_from_provider")
        else:
            row["percent_change"] = None
            _append_yahoo_warning_tag(row, "percent_change_suspect_dropped")
        return

    if recomputed is not None and raw_f is not None:
        if abs(recomputed - chosen) > 0.01:
            row["percent_change"] = round(recomputed, 8)
            _append_yahoo_warning_tag(row, "percent_change_recomputed")
            return

    row["percent_change"] = round(chosen, 8)


def _sanitize_week_52_position_pct(row: Dict[str, Any]) -> None:
    if not isinstance(row, dict):
        return

    try:
        cp = row.get("current_price")
        hi = row.get("week_52_high")
        lo = row.get("week_52_low")
        cp_f = float(cp) if cp is not None and cp != "" else None
        hi_f = float(hi) if hi is not None and hi != "" else None
        lo_f = float(lo) if lo is not None and lo != "" else None
    except (TypeError, ValueError):
        return

    if cp_f is None or hi_f is None or lo_f is None:
        return
    if hi_f <= lo_f:
        return
    pct = ((cp_f - lo_f) / (hi_f - lo_f)) * 100.0
    pct = max(0.0, min(_WEEK_52_POSITION_MAX, pct))
    row["week_52_position_pct"] = round(pct, 6)


def _sanitize_price_change(row: Dict[str, Any]) -> None:
    if not isinstance(row, dict):
        return
    try:
        cp = row.get("current_price")
        pc = row.get("previous_close")
        cp_f = float(cp) if cp is not None and cp != "" else None
        pc_f = float(pc) if pc is not None and pc != "" else None
    except (TypeError, ValueError):
        return
    if cp_f is None or pc_f is None:
        return
    row["price_change"] = round(cp_f - pc_f, 6)


def _apply_phase_bb_sanity(row: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(row, dict):
        return row
    _sanitize_price_change(row)
    _sanitize_percent_change(row)
    _sanitize_week_52_position_pct(row)
    return row


# =============================================================================
# v5.63.0 PHASE-DD — Restored derived/synthesized columns
# =============================================================================

_INTRINSIC_UPSIDE_MIN_PCT: float = -90.0
_INTRINSIC_UPSIDE_MAX_PCT: float = 200.0

_INTRINSIC_VALUE_MIN_MULT: float = 1.0 + (_INTRINSIC_UPSIDE_MIN_PCT / 100.0)
_INTRINSIC_VALUE_MAX_MULT: float = 1.0 + (_INTRINSIC_UPSIDE_MAX_PCT / 100.0)
_INTRINSIC_CANDIDATE_MIN_MULT: float = 0.05
_INTRINSIC_CANDIDATE_MAX_MULT: float = 5.0
_INTRINSIC_MIN_TRUSTED_PB: float = 0.10


def _intrinsic_candidate_ok(value: float, cp: float) -> bool:
    if value is None or cp is None or cp <= 0 or value <= 0:
        return False
    mult = value / cp
    return _INTRINSIC_CANDIDATE_MIN_MULT <= mult <= _INTRINSIC_CANDIDATE_MAX_MULT


def _compute_intrinsic_and_upside(row: Dict[str, Any]) -> None:
    if not isinstance(row, dict):
        return
    if row.get("intrinsic_value") is not None and row.get("upside_pct") is not None:
        return

    cp = _as_float(row.get("current_price"))
    if cp is None or cp <= 0:
        return

    eps = _as_float(row.get("eps_ttm"))
    forecast_12m = _as_float(row.get("forecast_price_12m"))
    pb = _as_float(row.get("pb_ratio"))
    pe_ttm = _as_float(row.get("pe_ttm"))
    sector = _safe_str(row.get("sector")).lower()

    # v5.77.0: sector_pe_map updated to post-2020 modern multiples.
    # Previous values (v5.76.0 and earlier) were calibrated against
    # 1990s-2010s value-investor norms and produced systematic
    # "overvalued" calls on profitable growth names with P/E > 20.
    # Sources: 10-year median sector P/Es from S&P 500 GICS sector
    # composites and Damodaran sector multiples (NYU Stern, 2025).
    sector_pe_map = {
        "technology":              32.0,   # was 25.0 - AI/cloud-era
        "consumer electronics":    25.0,   # was 22.0
        "communication services":  28.0,   # was 18.0 - modern tech-driven sector
        "financial services":      14.0,   # was 13.0
        "healthcare":              24.0,   # was 22.0
        "consumer defensive":      22.0,   # unchanged
        "consumer cyclical":       22.0,   # was 18.0
        "industrials":             20.0,   # was 19.0
        "energy":                  14.0,   # was 12.0
        "utilities":               18.0,   # was 17.0
        "real estate":             25.0,   # unchanged
        "basic materials":         16.0,   # was 15.0
    }
    fair_pe = sector_pe_map.get(sector, 20.0)  # default was 18.0

    # v5.77.0: sector_pb_map added. Previously the engine used a
    # hardcoded `book_value * 1.5` as the P/B-based intrinsic candidate
    # for every sector. That heuristic systematically penalized any
    # profitable company trading above 1.5x book - basically every
    # modern tech, healthcare, and consumer-discretionary name. Modern
    # companies with sustainable moats / network effects / asset-light
    # business models trade at legitimate P/B multiples of 4-10x.
    # Sources: 10-year median P/B by GICS sector (S&P 500), Damodaran.
    sector_pb_map = {
        "technology":              6.0,
        "consumer electronics":    5.0,
        "communication services":  5.0,
        "financial services":      1.2,
        "healthcare":              4.0,
        "consumer defensive":      3.5,
        "consumer cyclical":       4.0,
        "industrials":             2.5,
        "energy":                  1.8,
        "utilities":               1.5,
        "real estate":             2.0,
        "basic materials":         1.8,
    }
    fair_pb = sector_pb_map.get(sector, 2.0)

    candidates: List[float] = []
    weights: List[float] = []

    if eps is not None and eps > 0:
        pe_fair = eps * fair_pe
        if pe_fair > 0 and _intrinsic_candidate_ok(pe_fair, cp):
            candidates.append(pe_fair)
            weights.append(0.4)

    if forecast_12m is not None and forecast_12m > 0:
        if _intrinsic_candidate_ok(forecast_12m, cp):
            candidates.append(forecast_12m)
            weights.append(0.4)

    if pb is not None and _INTRINSIC_MIN_TRUSTED_PB <= pb < 20:
        book_value = cp / pb
        if book_value > 0:
            # v5.77.0: sector-aware P/B anchor (was: hardcoded 1.5x).
            pb_candidate = book_value * fair_pb
            if _intrinsic_candidate_ok(pb_candidate, cp):
                candidates.append(pb_candidate)
                weights.append(0.2)

    if not candidates:
        return

    total_w = sum(weights)
    if total_w <= 0:
        return
    intrinsic = sum(c * w for c, w in zip(candidates, weights)) / total_w

    intrinsic = max(cp * _INTRINSIC_VALUE_MIN_MULT,
                    min(cp * _INTRINSIC_VALUE_MAX_MULT, intrinsic))

    upside_fraction = (intrinsic - cp) / cp
    upside_fraction = max(_INTRINSIC_UPSIDE_MIN_PCT / 100.0,
                          min(_INTRINSIC_UPSIDE_MAX_PCT / 100.0, upside_fraction))

    row["intrinsic_value"] = round(intrinsic, 4)
    row["upside_pct"] = round(upside_fraction, 6)


def _synthesize_market_cap_if_zero(row: Dict[str, Any]) -> None:
    if not isinstance(row, dict):
        return
    mc = _as_float(row.get("market_cap"))
    if mc is not None and mc > 0:
        return

    cp = _as_float(row.get("current_price"))
    shares = _as_float(row.get("float_shares"))
    if shares is None:
        shares = _as_float(row.get("shares_outstanding"))

    if cp is not None and cp > 0 and shares is not None and shares > 0:
        row["market_cap"] = round(cp * shares, 2)


def _derive_views(row: Dict[str, Any]) -> None:
    if not isinstance(row, dict):
        return

    quality = _as_float(row.get("quality_score"))
    growth = _as_float(row.get("growth_score"))
    if quality is not None and growth is not None:
        combined = (quality + growth) / 2.0
        if combined >= 70:
            fv = "STRONG"
        elif combined >= 55:
            fv = "POSITIVE"
        elif combined >= 40:
            fv = "NEUTRAL"
        else:
            fv = "WEAK"
        row["fundamental_view"] = row.get("fundamental_view") or fv

    momentum = _as_float(row.get("momentum_score"))
    rsi = _as_float(row.get("rsi_14"))
    if momentum is not None:
        if rsi is not None and rsi > 70:
            tv = "OVERBOUGHT"
        elif rsi is not None and rsi < 30:
            tv = "OVERSOLD"
        elif momentum >= 70:
            tv = "BULLISH"
        elif momentum >= 50:
            tv = "POSITIVE"
        elif momentum >= 30:
            tv = "NEUTRAL"
        else:
            tv = "BEARISH"
        row["technical_view"] = row.get("technical_view") or tv

    risk_bucket = _safe_str(row.get("risk_bucket")).upper()
    if risk_bucket:
        rv_map = {"LOW": "LOW", "MODERATE": "MODERATE", "HIGH": "HIGH"}
        row["risk_view"] = row.get("risk_view") or rv_map.get(risk_bucket, risk_bucket)

    upside_points = _as_pct_points(row.get("upside_pct"))
    if upside_points is not None:
        if upside_points >= 30:
            vv = "CHEAP"
        elif upside_points >= 10:
            vv = "FAIR"
        elif upside_points >= -10:
            vv = "FULL"
        else:
            vv = "EXPENSIVE"
        row["value_view"] = row.get("value_view") or vv


_RECO_8TIER_PRIORITY: Dict[str, int] = {
    # v5.76.0: integer rank table aligned with the 8-tier ordinal contract
    # in core.reco_normalize v8.0.0. Rank 1 = highest conviction
    # (STRONG_BUY) ... 5 = highest urgency-to-exit (REDUCE / SELL /
    # STRONG_SELL / AVOID). The urgency axis (P1..P5 priority_band) is
    # tracked separately: STRONG_BUY=P1, BUY=P2, ACCUMULATE=P3, HOLD=P4,
    # REDUCE=P5, SELL=P5, STRONG_SELL=P1, AVOID=P1.
    "STRONG_BUY":  1,
    "BUY":         2,
    "ACCUMULATE":  3,   # v5.76.0: was 2; now sits between BUY=2 and HOLD=4
    "HOLD":        4,
    "REDUCE":      5,
    "SELL":        5,
    "STRONG_SELL": 5,
    "AVOID":       5,
}


def _canonical_recommendation(value: Any) -> str:
    if value in (None, ""):
        return ""
    if _RECO_NORMALIZE_AVAILABLE and _rn_normalize_recommendation is not None:
        try:
            canon = _safe_str(_rn_normalize_recommendation(value)).upper()
            if canon in _RECO_8TIER_PRIORITY:
                return canon
        except Exception:
            pass
    raw = _safe_str(value).upper().replace("-", "_").replace(" ", "_")
    return raw if raw in _RECO_8TIER_PRIORITY else ""


def _recommendation_priority(rec: str) -> int:
    """v5.69.0: integer rank 1 (best) .. 5 (worst) for a canonical
    recommendation. Unknown -> 4 (HOLD-equivalent neutral).

    v5.76.0: ACCUMULATE and AVOID are first-class entries in the
    8-tier table (rank 3 and rank 5 respectively). The v5.75.0 debug
    warning that fired when ACCUMULATE/AVOID reached this map has
    been dropped — those values are no longer "legacy un-collapsed
    values that signal an upstream bug"; they're expected outputs
    from scoring.py v5.7.0+ and reco_normalize v8.0.0+.

    Note: this is the int-rank axis (conviction). The string
    priority_band axis (urgency, P1..P5) is computed separately by
    core.scoring.apply_canonical_recommendation and stored in
    row["recommendation_priority_band"]. Both AVOID and STRONG_SELL
    have int rank 5 (worst conviction) but priority_band P1 (urgent
    exit) — the two axes encode different concerns.
    """
    key = _safe_str(rec).upper()
    return _RECO_8TIER_PRIORITY.get(key, 4)


def _classify_recommendation_8tier(row: Dict[str, Any]) -> None:
    """v5.75.0 — SINGLE AUTHORITATIVE recommendation writer (atomic, idempotent).

    v5.75.0 changes vs v5.73.1:
      - provider_rating is captured ONCE per row. Subsequent classifier
        passes (e.g. the post-Phase-II re-run, or sheet-level re-scoring)
        no longer overwrite it with the engine's own previous output,
        preserving the audit trail.
      - recommendation_priority_band is now written unconditionally
        (empty string if not derived), so a stale value from a previous
        classification cannot survive across passes.
      - The function self-clears its six output fields at entry, so
        callers don't need a paired _clear_recommendation_output_fields
        call. This eliminates a window where a partial clear plus a
        skipped write could leave inconsistent state.

    PRESERVED from v5.73.1:

    Architectural contract preserved from v5.73.0:
      1. Empty-row guard fires first.
      2. Provider rating capture into `provider_rating` for audit.
      3. Provider override allowed only when TFB_TRUST_PROVIDER_RECO=true.
      4. Conservative HOLD fallback when core.scoring missing.
      5. Final vocabulary: STRONG_BUY / BUY / HOLD / REDUCE / SELL / STRONG_SELL.
    """
    if not isinstance(row, dict):
        return

    # -- Step 1: empty-row guard ------------------------------------
    if _is_empty_data_row(row):
        _mark_row_as_empty(row)
        return

    # -- Step 2a: SELF-CLEAR (v5.75.0) ------------------------------
    # Drop stale recommendation output fields so a missed write below
    # cannot leave inconsistent state from a prior classification.
    # `recommendation` itself is NOT cleared because we need to read the
    # provider's value from it in Step 2b.
    for _stale_key in (
        "recommendation_detailed",
        "recommendation_detail",
        "recommendation_reason",
        "recommendation_priority",
        "recommendation_priority_band",
        "recommendation_source",
    ):
        row.pop(_stale_key, None)

    # -- Step 2b: provider rating capture (v5.75.0: ONCE-ONLY) ------
    # On the first classifier pass, row["recommendation"] is the
    # provider's value (REDUCE for DBA.US, etc.). On subsequent passes
    # it's the engine's own previous output. Capturing only when
    # provider_rating is empty preserves the audit trail.
    raw_upstream = row.get("recommendation")
    provider_canon = _v573_collapse_to_canonical_enum(raw_upstream)
    if not _safe_str(row.get("provider_rating")):
        if provider_canon:
            row["provider_rating"] = provider_canon
        elif raw_upstream not in (None, ""):
            row["provider_rating"] = _safe_str(raw_upstream)

    trust_provider = _v573_trust_provider_reco()
    provider_wins = bool(provider_canon) and trust_provider

    rec: str = ""
    reason: str = ""
    source: str = ""
    priority_band: str = ""

    # -- Step 3a: provider override path (rare) ---------------------
    if provider_wins:
        rec = provider_canon
        source = "provider_override"
        reason = (
            f"{rec}: Provider rating accepted via TFB_TRUST_PROVIDER_RECO override."
        )
        # v5.76.0: priority_band table extended for the 8-tier vocabulary.
        # STRONG_BUY=P1, BUY=P2, ACCUMULATE=P3 (new), HOLD=P4 (else),
        # REDUCE/SELL=P5, STRONG_SELL=P1, AVOID=P1 (new — urgent exit).
        if rec == "STRONG_BUY":
            priority_band = "P1"
        elif rec == "BUY":
            priority_band = "P2"
        elif rec == "ACCUMULATE":
            priority_band = "P3"
        elif rec == "STRONG_SELL":
            priority_band = "P1"
        elif rec == "AVOID":
            priority_band = "P1"
        elif rec in ("SELL", "REDUCE"):
            priority_band = "P5"
        else:
            priority_band = "P4"

    # -- Step 3b: engine path via scoring.apply_canonical_recommendation
    elif _CORE_SCORING_AVAILABLE and _SCORING_APPLY_CANONICAL_AVAILABLE \
            and _scoring_apply_canonical is not None:
        try:
            patch = _scoring_apply_canonical(row, overwrite=True)
        except Exception as exc:
            patch = None
            err = f"core.scoring.apply_canonical_recommendation: {type(exc).__name__}: {exc}"
            errs = row.get("scoring_errors")
            if isinstance(errs, list):
                errs.append(err)
            else:
                row["scoring_errors"] = [err]

        if patch and isinstance(patch, dict):
            _preserve_scoring_provenance(row, patch)
            rec_raw = patch.get("recommendation")
            rec_canon = _v573_collapse_to_canonical_enum(rec_raw)
            if rec_canon:
                rec = rec_canon
                source = "engine"
                reason = _safe_str(patch.get("recommendation_reason")) or \
                    f"{rec}: Engine classification via core.scoring."
                priority_band = _safe_str(patch.get("recommendation_priority_band"))

    # -- Step 3c: fallback HOLD when scoring is unavailable ---------
    if not rec:
        rec = "HOLD"
        source = "scoring_unavailable"
        reason = "HOLD: core.scoring unavailable; conservative fallback applied."
        priority_band = "P4"

    # -- Step 4: write the final row fields atomically --------------
    # `recommendation` and `recommendation_detailed` are written together so
    # they cannot diverge (Bug C). recommendation_source is forced to the
    # engine's source-vocabulary contract (engine / provider_override /
    # empty_row / scoring_unavailable), NOT scoring.py's
    # "scoring.py v5.3.0" tag — that tag is informative but not in our
    # vocabulary.
    # v5.75.0 ATOMIC WRITE: all six recommendation fields written
    # unconditionally. recommendation_priority_band is written even when
    # empty so a stale value from a prior classification cannot survive.
    row["recommendation"] = rec
    row["recommendation_detailed"] = rec
    row["recommendation_source"] = source
    row["recommendation_reason"] = reason
    row["recommendation_priority"] = _recommendation_priority(rec)
    row["recommendation_priority_band"] = priority_band  # may be ""; that is fine

    # -- Step 4b (v5.76.0): optional sheet-compatibility 6-tier collapse ----
    # Default: disabled. When TFB_COLLAPSE_RECOMMENDATION_TO_6TIER=true is
    # set, the recommendation / recommendation_detailed /
    # recommendation_priority / recommendation_priority_band fields are
    # rewritten from the 8-tier canonical down to the legacy 6-tier set
    # (ACCUMULATE → BUY, AVOID → STRONG_SELL) so callers whose
    # downstream renderer hasn't been updated to handle ACCUMULATE/AVOID
    # continue to receive only the legacy tokens. provider_rating and
    # the scoring.py provenance fields are NOT touched.
    if _sheet_collapse_to_6tier_enabled():
        _apply_sheet_compat_collapse(row)

    # Scoring schema version stamp (informational; not yet a required
    # canonical column).
    row.setdefault("scoring_schema_version", _SCHEMA_VERSION)


def _build_top_factors_and_risks(row: Dict[str, Any]) -> None:
    if not isinstance(row, dict):
        return

    factors: List[str] = []
    risks: List[str] = []

    upside_points = _as_pct_points(row.get("upside_pct"))
    if upside_points is not None and upside_points >= 15:
        factors.append("Attractive valuation")
    momentum = _as_float(row.get("momentum_score"))
    if momentum is not None and momentum >= 70:
        factors.append("Positive momentum")
    quality = _as_float(row.get("quality_score"))
    if quality is not None and quality >= 70:
        factors.append("Strong fundamentals")
    growth = _as_float(row.get("growth_score"))
    if growth is not None and growth >= 70:
        factors.append("Solid growth")
    div_yield = _as_float(row.get("dividend_yield"))
    if div_yield is not None and div_yield >= 0.03:
        factors.append("Dividend income")
    rgyoy = _as_float(row.get("revenue_growth_yoy"))
    if rgyoy is not None and rgyoy >= 0.10:
        factors.append("Revenue growth")

    if not factors:
        factors.append("Limited positive signals")

    risk_bucket = _safe_str(row.get("risk_bucket")).upper()
    if risk_bucket == "HIGH":
        risks.append("High volatility")
    vol = _as_float(row.get("volatility_30d"))
    if vol is not None and vol >= 0.40:
        risks.append("Elevated volatility")
    dd_points = _as_pct_points(row.get("max_drawdown_1y"))
    if dd_points is not None and abs(dd_points) >= 30:
        risks.append("Recent drawdown")
    rsi = _as_float(row.get("rsi_14"))
    if rsi is not None and rsi > 75:
        risks.append("Overbought (RSI)")
    elif rsi is not None and rsi < 25:
        risks.append("Oversold (RSI)")
    de = _as_float(row.get("debt_to_equity"))
    if de is not None and de >= 100:
        risks.append("High leverage")
    pe = _as_float(row.get("pe_ttm"))
    if pe is not None and pe >= 50:
        risks.append("Expensive valuation")

    if not risks:
        risks.append("Limited downside signals")

    conviction = 50.0 + (len(factors) - len(risks)) * 8.0
    conviction = max(0.0, min(100.0, conviction))

    overall = _as_float(row.get("overall_score"))
    if overall is not None:
        sector_adj = overall + (5.0 if "Strong fundamentals" in factors else 0.0) - (5.0 if risk_bucket == "HIGH" else 0.0)
        sector_adj = max(0.0, min(100.0, sector_adj))
        if not row.get("sector_relative_score"):
            row["sector_relative_score"] = round(sector_adj, 2)

    if not row.get("top_factors"):
        row["top_factors"] = "; ".join(factors[:3])
    if not row.get("top_risks"):
        row["top_risks"] = "; ".join(risks[:3])
    if not row.get("conviction_score"):
        row["conviction_score"] = round(conviction, 2)

    rec = _safe_str(row.get("recommendation")).upper()
    if rec in ("STRONG_BUY",):
        psh = "Core position"
    elif rec in ("BUY", "ACCUMULATE"):
        psh = "Standard position"
    elif rec == "HOLD":
        psh = "Maintain or trim"
    else:
        psh = "Avoid / reduce"
    if not row.get("position_size_hint"):
        row["position_size_hint"] = psh



# =============================================================================
# v5.66.0 PHASE-JJ — Candlestick pattern detection
# =============================================================================

_CS_SIGNAL_BULLISH = "BULLISH"
_CS_SIGNAL_BEARISH = "BEARISH"
_CS_SIGNAL_NEUTRAL = "NEUTRAL"
_CS_SIGNAL_DOJI = "DOJI"

_CS_STRENGTH_STRONG = "STRONG"
_CS_STRENGTH_MODERATE = "MODERATE"
_CS_STRENGTH_WEAK = "WEAK"

_CS_P_DOJI = "Doji"
_CS_P_HAMMER = "Hammer"
_CS_P_INVERTED_HAMMER = "Inverted Hammer"
_CS_P_SHOOTING_STAR = "Shooting Star"
_CS_P_HANGING_MAN = "Hanging Man"
_CS_P_MARUBOZU_BULL = "Bullish Marubozu"
_CS_P_MARUBOZU_BEAR = "Bearish Marubozu"
_CS_P_BULL_ENGULFING = "Bullish Engulfing"
_CS_P_BEAR_ENGULFING = "Bearish Engulfing"
_CS_P_MORNING_STAR = "Morning Star"
_CS_P_EVENING_STAR = "Evening Star"

_CS_PATTERN_SIGNAL: Dict[str, str] = {
    _CS_P_DOJI: _CS_SIGNAL_DOJI,
    _CS_P_HAMMER: _CS_SIGNAL_BULLISH,
    _CS_P_INVERTED_HAMMER: _CS_SIGNAL_BULLISH,
    _CS_P_SHOOTING_STAR: _CS_SIGNAL_BEARISH,
    _CS_P_HANGING_MAN: _CS_SIGNAL_BEARISH,
    _CS_P_MARUBOZU_BULL: _CS_SIGNAL_BULLISH,
    _CS_P_MARUBOZU_BEAR: _CS_SIGNAL_BEARISH,
    _CS_P_BULL_ENGULFING: _CS_SIGNAL_BULLISH,
    _CS_P_BEAR_ENGULFING: _CS_SIGNAL_BEARISH,
    _CS_P_MORNING_STAR: _CS_SIGNAL_BULLISH,
    _CS_P_EVENING_STAR: _CS_SIGNAL_BEARISH,
}

_CS_DOJI_BODY_RATIO_MAX = 0.10
_CS_MARUBOZU_BODY_RATIO_MIN = 0.95
_CS_HAMMER_SHADOW_MULTIPLIER = 2.0
_CS_HAMMER_OPP_SHADOW_RATIO_MAX = 0.30
_CS_SMALL_BODY_RATIO_MAX = 0.30
_CS_LONG_BODY_RATIO_MIN = 0.55
_CS_TREND_LOOKBACK = 10
_CS_TREND_THRESHOLD = 0.01
_CS_RECENT_LOOKBACK = 5


def _cs_coerce_bar(row: Any) -> Optional[Dict[str, float]]:
    if not isinstance(row, dict):
        return None
    o = _as_float(row.get("open") if row.get("open") is not None else row.get("o"))
    h = _as_float(row.get("high") if row.get("high") is not None else row.get("h"))
    low = _as_float(row.get("low") if row.get("low") is not None else row.get("l"))
    c = _as_float(
        row.get("close")
        if row.get("close") is not None
        else (
            row.get("adjusted_close")
            if row.get("adjusted_close") is not None
            else (row.get("adjclose") if row.get("adjclose") is not None else row.get("c"))
        )
    )
    if c is None:
        return None
    if o is None:
        o = c
    if h is None:
        h = max(o, c)
    if low is None:
        low = min(o, c)
    if h < low:
        h, low = low, h
    v = _as_float(row.get("volume") if row.get("volume") is not None else row.get("v")) or 0.0
    return {"open": o, "high": h, "low": low, "close": c, "volume": v}


def _cs_coerce_bars(rows: Any) -> List[Dict[str, float]]:
    out: List[Dict[str, float]] = []
    if not rows:
        return out
    try:
        iterable = list(rows)
    except Exception:
        return out
    for row in iterable:
        bar = _cs_coerce_bar(row)
        if bar is not None:
            out.append(bar)
    return out


def _cs_bar_geom(bar: Dict[str, float]) -> Dict[str, float]:
    o = bar["open"]; h = bar["high"]; low = bar["low"]; c = bar["close"]
    rng = max(h - low, 1e-12)
    body = abs(c - o)
    upper_shadow = max(h - max(o, c), 0.0)
    lower_shadow = max(min(o, c) - low, 0.0)
    return {
        "open": o, "high": h, "low": low, "close": c, "range": rng,
        "body": body, "body_ratio": body / rng,
        "upper_shadow": upper_shadow, "lower_shadow": lower_shadow,
        "upper_shadow_ratio": upper_shadow / rng,
        "lower_shadow_ratio": lower_shadow / rng,
        "is_bullish": c > o, "is_bearish": c < o,
        "midpoint": (o + c) / 2.0,
    }


def _cs_trend_at(bars: List[Dict[str, float]], idx: int) -> str:
    start = idx - _CS_TREND_LOOKBACK
    if start < 0:
        return "FLAT"
    window = bars[start:idx]
    if len(window) < 3:
        return "FLAT"
    first = window[0]["close"]
    last = window[-1]["close"]
    if first <= 0:
        return "FLAT"
    pct = (last - first) / first
    if pct >= _CS_TREND_THRESHOLD:
        return "UP"
    if pct <= -_CS_TREND_THRESHOLD:
        return "DOWN"
    return "FLAT"


def _cs_detect_marubozu(g: Dict[str, float]) -> Optional[Tuple[str, float]]:
    if g["body_ratio"] < _CS_MARUBOZU_BODY_RATIO_MIN:
        return None
    confidence = 50.0 + (g["body_ratio"] - _CS_MARUBOZU_BODY_RATIO_MIN) * 1000.0
    confidence = max(50.0, min(100.0, confidence))
    if g["is_bullish"]:
        return _CS_P_MARUBOZU_BULL, confidence
    if g["is_bearish"]:
        return _CS_P_MARUBOZU_BEAR, confidence
    return None


def _cs_detect_doji(g: Dict[str, float]) -> Optional[Tuple[str, float]]:
    if g["body_ratio"] >= _CS_DOJI_BODY_RATIO_MAX:
        return None
    raw = 100.0 * (1.0 - g["body_ratio"] / _CS_DOJI_BODY_RATIO_MAX)
    confidence = max(40.0, min(95.0, raw))
    return _CS_P_DOJI, confidence


def _cs_has_hammer_shape(g: Dict[str, float]) -> bool:
    if g["body"] <= 0:
        return False
    if g["lower_shadow"] < _CS_HAMMER_SHADOW_MULTIPLIER * g["body"]:
        return False
    if g["upper_shadow_ratio"] > _CS_HAMMER_OPP_SHADOW_RATIO_MAX:
        return False
    return True


def _cs_has_inverted_hammer_shape(g: Dict[str, float]) -> bool:
    if g["body"] <= 0:
        return False
    if g["upper_shadow"] < _CS_HAMMER_SHADOW_MULTIPLIER * g["body"]:
        return False
    if g["lower_shadow_ratio"] > _CS_HAMMER_OPP_SHADOW_RATIO_MAX:
        return False
    return True


def _cs_detect_hammer_family(g: Dict[str, float], trend: str) -> Optional[Tuple[str, float]]:
    if _cs_has_hammer_shape(g):
        ratio = g["lower_shadow"] / max(g["body"], 1e-9)
        base_conf = min(85.0, 40.0 + ratio * 8.0)
        if trend == "DOWN":
            return _CS_P_HAMMER, base_conf
        if trend == "UP":
            return _CS_P_HANGING_MAN, base_conf * 0.85
        return _CS_P_HAMMER, base_conf * 0.65
    if _cs_has_inverted_hammer_shape(g):
        ratio = g["upper_shadow"] / max(g["body"], 1e-9)
        base_conf = min(80.0, 35.0 + ratio * 8.0)
        if trend == "DOWN":
            return _CS_P_INVERTED_HAMMER, base_conf
        if trend == "UP":
            return _CS_P_SHOOTING_STAR, base_conf
        return _CS_P_INVERTED_HAMMER, base_conf * 0.65
    return None


def _cs_detect_engulfing(g_prev: Dict[str, float], g_curr: Dict[str, float]) -> Optional[Tuple[str, float]]:
    if g_prev["body_ratio"] < _CS_DOJI_BODY_RATIO_MAX:
        return None
    if g_curr["body_ratio"] < _CS_DOJI_BODY_RATIO_MAX:
        return None
    prev_open = g_prev["open"]; prev_close = g_prev["close"]
    curr_open = g_curr["open"]; curr_close = g_curr["close"]
    if g_prev["is_bearish"] and g_curr["is_bullish"]:
        if curr_open <= prev_close and curr_close >= prev_open:
            engulf_factor = g_curr["body"] / max(g_prev["body"], 1e-9)
            confidence = min(90.0, 50.0 + engulf_factor * 10.0)
            return _CS_P_BULL_ENGULFING, confidence
    if g_prev["is_bullish"] and g_curr["is_bearish"]:
        if curr_open >= prev_close and curr_close <= prev_open:
            engulf_factor = g_curr["body"] / max(g_prev["body"], 1e-9)
            confidence = min(90.0, 50.0 + engulf_factor * 10.0)
            return _CS_P_BEAR_ENGULFING, confidence
    return None


def _cs_detect_star(g_first: Dict[str, float], g_star: Dict[str, float], g_third: Dict[str, float]) -> Optional[Tuple[str, float]]:
    if g_star["body_ratio"] >= _CS_SMALL_BODY_RATIO_MAX:
        return None
    if g_first["body_ratio"] < _CS_LONG_BODY_RATIO_MIN:
        return None
    if g_third["body_ratio"] < _CS_LONG_BODY_RATIO_MIN:
        return None
    if g_first["is_bearish"] and g_third["is_bullish"]:
        if g_third["close"] > g_first["midpoint"]:
            penetration = (g_third["close"] - g_first["midpoint"]) / max(g_first["body"], 1e-9)
            confidence = min(95.0, 60.0 + penetration * 30.0)
            return _CS_P_MORNING_STAR, confidence
    if g_first["is_bullish"] and g_third["is_bearish"]:
        if g_third["close"] < g_first["midpoint"]:
            penetration = (g_first["midpoint"] - g_third["close"]) / max(g_first["body"], 1e-9)
            confidence = min(95.0, 60.0 + penetration * 30.0)
            return _CS_P_EVENING_STAR, confidence
    return None


def _cs_detect_at_index(bars: List[Dict[str, float]], idx: int) -> Optional[Tuple[str, float]]:
    if idx < 0 or idx >= len(bars):
        return None
    trend = _cs_trend_at(bars, idx)
    g_curr = _cs_bar_geom(bars[idx])
    if idx >= 2:
        g_first = _cs_bar_geom(bars[idx - 2])
        g_star = _cs_bar_geom(bars[idx - 1])
        star = _cs_detect_star(g_first, g_star, g_curr)
        if star is not None:
            return star
    if idx >= 1:
        g_prev = _cs_bar_geom(bars[idx - 1])
        eng = _cs_detect_engulfing(g_prev, g_curr)
        if eng is not None:
            return eng
    marubozu = _cs_detect_marubozu(g_curr)
    if marubozu is not None:
        return marubozu
    hammer = _cs_detect_hammer_family(g_curr, trend)
    if hammer is not None:
        return hammer
    doji = _cs_detect_doji(g_curr)
    if doji is not None:
        return doji
    return None


def _cs_confidence_to_strength(confidence: float) -> str:
    if confidence >= 75.0:
        return _CS_STRENGTH_STRONG
    if confidence >= 55.0:
        return _CS_STRENGTH_MODERATE
    return _CS_STRENGTH_WEAK


def detect_candlestick_patterns(rows: Any) -> Dict[str, Any]:
    empty = {
        "candlestick_pattern": "",
        "candlestick_signal": _CS_SIGNAL_NEUTRAL,
        "candlestick_strength": "",
        "candlestick_confidence": 0.0,
        "candlestick_patterns_recent": "",
    }
    bars = _cs_coerce_bars(rows)
    if not bars:
        return empty
    last_idx = len(bars) - 1
    latest = _cs_detect_at_index(bars, last_idx)
    if latest is None:
        out = dict(empty)
    else:
        pattern, confidence = latest
        out = {
            "candlestick_pattern": pattern,
            "candlestick_signal": _CS_PATTERN_SIGNAL.get(pattern, _CS_SIGNAL_NEUTRAL),
            "candlestick_strength": _cs_confidence_to_strength(confidence),
            "candlestick_confidence": round(float(confidence), 2),
            "candlestick_patterns_recent": "",
        }
    recent_patterns: List[str] = []
    start = max(0, last_idx - _CS_RECENT_LOOKBACK + 1)
    for i in range(start, last_idx + 1):
        det = _cs_detect_at_index(bars, i)
        if det is None:
            continue
        name, _ = det
        recent_patterns.append(name)
    out["candlestick_patterns_recent"] = " | ".join(recent_patterns)
    return out


def _apply_phase_dd_enhancements(row: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(row, dict):
        return row

    # v5.74.0 sequencing: data enrichment first, recommendation last.
    # This function may be called before scoring and again after scoring.
    # Before scoring it prepares intrinsic/upside and Phase-II forecast inputs.
    # After scoring it derives views, writes the final recommendation, and
    # rebuilds factors/risks from the final scored state.
    _synthesize_market_cap_if_zero(row)
    _compute_intrinsic_and_upside(row)
    _phase_ii_quality_forecast(row)

    has_scores = any(
        _as_float(row.get(k)) is not None
        for k in ("overall_score", "valuation_score", "quality_score", "momentum_score", "opportunity_score")
    )
    if has_scores:
        # v5.75.0: single classification pass. The classifier self-clears
        # at entry (v5.75.0 P5) so no separate _clear call is needed, and
        # writes all six recommendation fields atomically so a re-run
        # would not produce different output anyway. Cuts per-quote
        # delegation cost into core.scoring from 3 calls to 1.
        _derive_views(row)
        _classify_recommendation_8tier(row)
        _build_top_factors_and_risks(row)

    return row


# =============================================================================
# v5.65.0 PHASE-II — Quality forecast generator
# =============================================================================

_PHASE_II_MAX_12M_ABS_RETURN: float = 0.30
# v5.77.2: lift inlined ratio constants to module-level so the provider-
# target branch can reuse them consistently with the synthesis branch.
_PHASE_II_RATIO_3M_OF_12M: float = 0.35
_PHASE_II_RATIO_1M_OF_12M: float = 0.12
_PHASE_II_MIN_12M_ABS_RETURN: float = -0.30
_PHASE_II_VOL_BAND_FACTOR: float = 0.5
_PHASE_II_CONF_MIN: float = 0.30
_PHASE_II_CONF_MAX: float = 0.85


def _phase_ii_quality_forecast(row: Dict[str, Any]) -> None:
    if not isinstance(row, dict):
        return

    cp = _as_float(row.get("current_price"))
    if cp is None or cp <= 0:
        return

    # v5.77.0: Idempotency / provider-target preservation.
    # If a trusted upstream provider already supplied forecast_price_12m
    # (analyst target) AND we've tagged the source as "provider_target",
    # skip the synthesis pass. This prevents the engine from blindly
    # overwriting legitimate analyst consensus with its own value-
    # investor projection on every classifier re-run.
    #
    # v5.77.2: when provider_target is detected, do NOT return entirely —
    # that left 1M and 3M forecast columns blank for every stock with a
    # provider analyst target (which is the majority of the universe).
    # ChatGPT v5.77.1 audit caught this. Fix: preserve the provider 12M
    # target, but derive 1M and 3M from it using the same Phase-II
    # ratio constants we use for synthesized forecasts. This way the
    # short-horizon columns stay populated AND the analyst 12M target
    # is the authoritative anchor (not overwritten).
    existing_source = _safe_str(row.get("forecast_source")).lower()
    if existing_source == "provider_target" and row.get("forecast_price_12m") is not None:
        fp12 = _as_float(row.get("forecast_price_12m"))
        if fp12 is not None and fp12 > 0:
            # Implied 12M return from provider target.
            return_12m = (fp12 - cp) / cp
            # Cap to the Phase-II abs ceiling for consistency. Provider
            # analyst targets sometimes exceed our ceiling; in that case
            # the 12M field stays at the raw provider value, but the
            # derived 1M/3M use the capped figure (otherwise short-
            # horizon ROI would look unrealistic).
            capped_12m = max(min(return_12m, _PHASE_II_MAX_12M_ABS_RETURN), -_PHASE_II_MAX_12M_ABS_RETURN)
            # v5.77.3: surface a warning tag when the cap actually fires,
            # so the dashboard can explain to users why a provider target
            # implying +60% / -60% does not translate into proportional
            # 1M/3M figures. ChatGPT v5.77.2 audit suggested this for
            # transparency. Use a tolerance to avoid spurious tagging on
            # floating-point noise near the cap boundary.
            if abs(return_12m) - _PHASE_II_MAX_12M_ABS_RETURN > 1e-9:
                _v573_append_warning(row, "provider_target_capped_for_short_horizon_derivation")
            if row.get("forecast_price_3m") is None:
                derived_3m_return = capped_12m * _PHASE_II_RATIO_3M_OF_12M
                row["forecast_price_3m"] = round(cp * (1.0 + derived_3m_return), 4)
            if row.get("forecast_price_1m") is None:
                derived_1m_return = capped_12m * _PHASE_II_RATIO_1M_OF_12M
                row["forecast_price_1m"] = round(cp * (1.0 + derived_1m_return), 4)
            # Also derive expected_roi_* fields if they're missing.
            if row.get("expected_roi_12m") is None:
                row["expected_roi_12m"] = round(return_12m, 6)
            if row.get("expected_roi_3m") is None:
                fp3 = _as_float(row.get("forecast_price_3m"))
                if fp3 is not None:
                    row["expected_roi_3m"] = round((fp3 - cp) / cp, 6)
            if row.get("expected_roi_1m") is None:
                fp1 = _as_float(row.get("forecast_price_1m"))
                if fp1 is not None:
                    row["expected_roi_1m"] = round((fp1 - cp) / cp, 6)
        return  # 12M provider target is authoritative; do not synthesize over it.

    intrinsic = _as_float(row.get("intrinsic_value"))
    momentum = _as_float(row.get("momentum_score"))
    quality = _as_float(row.get("quality_score"))
    value = _as_float(row.get("value_score"))
    growth = _as_float(row.get("growth_score"))
    overall = _as_float(row.get("overall_score"))
    vol_30d = _as_float(row.get("volatility_30d"))
    vol_90d = _as_float(row.get("volatility_90d"))
    rsi = _as_float(row.get("rsi_14"))
    risk_bucket = _safe_str(row.get("risk_bucket")).upper()

    components: List[Tuple[float, float]] = []

    # v5.77.0: intrinsic-reversion weight reduced 0.40 -> 0.25.
    # The 0.40 weight in v5.76.0 amplified any residual bias in the
    # intrinsic-value calculation directly into the 12M forecast.
    # Combined with the sector_pe / sector_pb fixes in
    # _compute_intrinsic_and_upside, this reduces the engine's net
    # mean-reversion bias on growth stocks. Momentum and fundamentals
    # weights raised proportionally so the components still sum to 1.0.
    if intrinsic is not None and intrinsic > 0:
        reversion_return = (intrinsic - cp) / cp
        reversion_return *= 0.6
        components.append((reversion_return, 0.25))  # was 0.40

    if momentum is not None:
        trend_return = ((momentum - 50.0) / 50.0) * 0.15
        components.append((trend_return, 0.35))  # was 0.30

    fundamentals_signals: List[float] = []
    for sub in (quality, value, growth):
        if sub is not None:
            fundamentals_signals.append((sub - 50.0) / 50.0)
    if fundamentals_signals:
        avg_fund = sum(fundamentals_signals) / len(fundamentals_signals)
        fund_return = avg_fund * 0.10
        components.append((fund_return, 0.25))  # was 0.20

    if overall is not None:
        baseline_return = ((overall - 50.0) / 50.0) * 0.08
        components.append((baseline_return, 0.15))  # was 0.10

    if not components:
        return

    total_weight = sum(w for _, w in components)
    if total_weight <= 0:
        return
    expected_12m_return = sum(r * w for r, w in components) / total_weight

    # v5.77.0: RSI bumps REMOVED from 12M return.
    # 14-day RSI has no statistical predictive power on 12-month
    # returns; applying it to the long-horizon anchor and then deriving
    # 1M/3M as fractions of that anchor propagated the corruption to
    # all three horizons. The RSI adjustment is now applied ONLY to
    # the 1M and 3M derived returns below, where its short-horizon
    # mean-reversion signal is statistically defensible.

    expected_12m_return_uncapped = expected_12m_return
    expected_12m_return = max(_PHASE_II_MIN_12M_ABS_RETURN,
                              min(_PHASE_II_MAX_12M_ABS_RETURN, expected_12m_return))

    # v5.77.0: surface cap-firing as a warning so downstream consumers
    # know the value is a guardrail, not a per-symbol prediction.
    if expected_12m_return >= 0.95 * _PHASE_II_MAX_12M_ABS_RETURN:
        _v573_append_warning(row, "forecast_capped_at_ceiling")
    elif expected_12m_return <= 0.95 * _PHASE_II_MIN_12M_ABS_RETURN:
        _v573_append_warning(row, "forecast_capped_at_floor")

    expected_3m_return = expected_12m_return * _PHASE_II_RATIO_3M_OF_12M
    expected_1m_return = expected_12m_return * _PHASE_II_RATIO_1M_OF_12M

    # v5.77.0: RSI bump applied here, on the 1M / 3M short-horizon
    # returns where 14-day RSI is statistically relevant.
    # v5.77.1: magnitude REDUCED. The v5.77.0 absolute -3% / +3% bumps
    # were too aggressive at short horizons. On a blue-chip with a
    # baseline 1M return of +1.5%, an absolute -3% for RSI>75 swung
    # the expectation to -1.5% (implied ~36% annualized technical
    # drag). v5.77.1 uses multiplicative dampening of positive returns
    # (halving them) when RSI is overbought, and a smaller absolute
    # add (-0.015 / +0.015 at 1M, -0.008 / +0.008 at 3M) when no
    # baseline positive return exists to dampen. This preserves the
    # mean-reversion signal but doesn't violently swing healthy
    # momentum stocks into negative territory on short horizons.
    if rsi is not None:
        if rsi > 75:
            # Overbought: dampen positive 1M expectation by 50%; if
            # already negative or near-zero, subtract a small absolute.
            if expected_1m_return > 0:
                expected_1m_return *= 0.5
            else:
                expected_1m_return -= 0.015
            if expected_3m_return > 0:
                expected_3m_return *= 0.65   # less aggressive at 3M
            else:
                expected_3m_return -= 0.008
        elif rsi > 70:
            if expected_1m_return > 0:
                expected_1m_return *= 0.75
            else:
                expected_1m_return -= 0.008
            if expected_3m_return > 0:
                expected_3m_return *= 0.85
            else:
                expected_3m_return -= 0.004
        elif rsi < 25:
            # Oversold: boost expectation modestly.
            if expected_1m_return < 0:
                expected_1m_return *= 0.5   # dampen negative
            else:
                expected_1m_return += 0.015
            if expected_3m_return < 0:
                expected_3m_return *= 0.65
            else:
                expected_3m_return += 0.008
        elif rsi < 30:
            if expected_1m_return < 0:
                expected_1m_return *= 0.75
            else:
                expected_1m_return += 0.008
            if expected_3m_return < 0:
                expected_3m_return *= 0.85
            else:
                expected_3m_return += 0.004

    forecast_12m = cp * (1.0 + expected_12m_return)
    forecast_3m = cp * (1.0 + expected_3m_return)
    forecast_1m = cp * (1.0 + expected_1m_return)

    row["forecast_price_1m"] = round(forecast_1m, 4)
    row["forecast_price_3m"] = round(forecast_3m, 4)
    row["forecast_price_12m"] = round(forecast_12m, 4)
    row["expected_roi_1m"] = round(expected_1m_return, 6)
    row["expected_roi_3m"] = round(expected_3m_return, 6)
    row["expected_roi_12m"] = round(expected_12m_return, 6)
    # v5.77.0: tag synthesis origin so subsequent calls can preserve
    # provider targets if they're added back by an upstream patch.
    row["forecast_source"] = "phase_ii_synthetic"

    conf = 0.50

    completeness_fields = [
        "pe_ttm", "pb_ratio", "eps_ttm", "dividend_yield",
        "revenue_growth_yoy", "gross_margin", "operating_margin",
        "rsi_14", "volatility_30d", "max_drawdown_1y",
    ]
    present = sum(1 for f in completeness_fields if _as_float(row.get(f)) is not None)
    completeness_ratio = present / len(completeness_fields)
    conf += completeness_ratio * 0.20

    sub_scores = [s for s in (quality, value, growth, momentum) if s is not None]
    if len(sub_scores) >= 3:
        mean_s = sum(sub_scores) / len(sub_scores)
        variance = sum((s - mean_s) ** 2 for s in sub_scores) / len(sub_scores)
        agreement = max(0.0, 1.0 - (variance / 800.0))
        conf += agreement * 0.10

    if risk_bucket == "HIGH":
        conf -= 0.10
    if vol_90d is not None and vol_90d > 0.50:
        conf -= 0.05
    elif vol_30d is not None and vol_30d > 0.45:
        conf -= 0.03

    if abs(expected_12m_return) > 0.25:
        conf -= 0.05

    conf = max(_PHASE_II_CONF_MIN, min(_PHASE_II_CONF_MAX, conf))

    row["forecast_confidence"] = round(conf, 4)
    row["confidence_score"] = round(conf * 100.0, 2)
    if conf >= 0.70:
        row["confidence_bucket"] = "HIGH"
    elif conf >= 0.50:
        row["confidence_bucket"] = "MODERATE"
    else:
        row["confidence_bucket"] = "LOW"


def _pick_yahoo_callable(mod: Any, *names: str) -> Optional[Any]:
    if mod is None:
        return None
    for n in names:
        fn = getattr(mod, n, None)
        if callable(fn):
            return fn
    return None


def _append_yahoo_warning_tag(row: Dict[str, Any], tag: str) -> None:
    if not tag:
        return
    existing = row.get("warnings")
    if isinstance(existing, list):
        if tag not in existing:
            existing.append(tag)
        return
    s = str(existing or "")
    if tag in s:
        return
    row["warnings"] = (s + "; " + tag) if s else tag


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
# Canonical page contracts
# =============================================================================
INSTRUMENT_CANONICAL_KEYS: List[str] = [
    "symbol", "name", "asset_class", "exchange", "currency", "country", "sector", "industry",
    "current_price", "previous_close", "open_price", "day_high", "day_low",
    "week_52_high", "week_52_low", "price_change", "percent_change", "week_52_position_pct",
    "volume", "avg_volume_10d", "avg_volume_30d", "market_cap", "float_shares", "beta_5y",
    "pe_ttm", "pe_forward", "eps_ttm", "dividend_yield", "payout_ratio",
    "revenue_ttm", "revenue_growth_yoy", "gross_margin", "operating_margin", "profit_margin",
    "debt_to_equity", "free_cash_flow_ttm",
    "rsi_14", "volatility_30d", "volatility_90d", "max_drawdown_1y",
    "var_95_1d", "sharpe_1y", "risk_score", "risk_bucket",
    "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio", "intrinsic_value",
    "upside_pct", "valuation_score",
    "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
    "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "forecast_confidence", "confidence_score", "confidence_bucket",
    "value_score", "quality_score", "momentum_score", "growth_score",
    "overall_score", "opportunity_score", "rank_overall",
    "fundamental_view", "technical_view", "risk_view", "value_view",
    "recommendation", "recommendation_reason", "horizon_days", "invest_period_label",
    "position_qty", "avg_cost", "position_cost", "position_value",
    "unrealized_pl", "unrealized_pl_pct",
    "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",
    "sector_relative_score", "conviction_score", "top_factors", "top_risks",
    "position_size_hint", "recommendation_detailed", "recommendation_priority",

    "provider_rating", "recommendation_source",
    "recommendation_priority_band", "scoring_recommendation_source",
    "scoring_schema_version", "scoring_errors", "opportunity_source",
    "overall_score_raw", "overall_penalty_factor",
    "candlestick_pattern", "candlestick_signal", "candlestick_strength",
    "candlestick_confidence", "candlestick_patterns_recent",
    # v5.77.1: forecast_source surfaced to the sheet. Values:
    #   "provider_target"   - upstream provider supplied an analyst target
    #   "phase_ii_synthetic" - engine synthesized the forecast
    #   "fallback"          - local fallback heuristic produced the forecast
    #   "" / None           - no forecast available
    "forecast_source",
]
_SCHEMA_VERSION = f"instrument:{len(INSTRUMENT_CANONICAL_KEYS)}:{__version__}"


INSTRUMENT_CANONICAL_HEADERS: List[str] = [
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
    "Current Price", "Previous Close", "Open", "Day High", "Day Low",
    "52W High", "52W Low", "Price Change", "Percent Change", "52W Position %",
    "Volume", "Avg Volume 10D", "Avg Volume 30D", "Market Cap", "Float Shares", "Beta (5Y)",
    "P/E (TTM)", "P/E (Forward)", "EPS (TTM)", "Dividend Yield", "Payout Ratio",
    "Revenue (TTM)", "Revenue Growth YoY", "Gross Margin", "Operating Margin",
    "Profit Margin", "Debt/Equity", "Free Cash Flow (TTM)",
    "RSI (14)", "Volatility 30D", "Volatility 90D", "Max Drawdown 1Y",
    "VaR 95% (1D)", "Sharpe (1Y)", "Risk Score", "Risk Bucket",
    "P/B", "P/S", "EV/EBITDA", "PEG", "Intrinsic Value", "Upside %", "Valuation Score",
    "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M",
    "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M",
    "Forecast Confidence", "Confidence Score", "Confidence Bucket",
    "Value Score", "Quality Score", "Momentum Score", "Growth Score",
    "Overall Score", "Opportunity Score", "Rank (Overall)",
    "Fundamental View", "Technical View", "Risk View", "Value View",
    "Recommendation", "Recommendation Reason", "Horizon Days", "Invest Period Label",
    "Position Qty", "Avg Cost", "Position Cost", "Position Value",
    "Unrealized P/L", "Unrealized P/L %",
    "Data Provider", "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",
    "Sector-Adj Score", "Conviction Score", "Top Factors", "Top Risks",
    "Position Size Hint", "Recommendation Detail", "Reco Priority",

    "Provider Rating", "Recommendation Source",
    "Priority Band", "Scoring Reco Source",
    "Scoring Schema Version", "Scoring Errors", "Opportunity Source",
    "Overall Score (Raw)", "Overall Penalty Factor",
    "Candle Pattern", "Candle Signal", "Candle Strength",
    "Candle Confidence", "Recent Patterns (5D)",
    # v5.77.1: surfaces whether forecast_price_* came from a provider
    # analyst target, the Phase-II synthesis, or a fallback heuristic.
    "Forecast Source",
]

TOP10_REQUIRED_FIELDS: Tuple[str, ...] = (
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
)
TOP10_REQUIRED_HEADERS: Dict[str, str] = {
    "top10_rank": "Top10 Rank",
    "selection_reason": "Selection Reason",
    "criteria_snapshot": "Criteria Snapshot",
}

INSIGHTS_HEADERS: List[str] = [
    "Section", "Item", "Metric", "Value", "Notes", "Source", "Sort Order",
]
INSIGHTS_KEYS: List[str] = [
    "section", "item", "metric", "value", "notes", "source", "sort_order",
]

DATA_DICTIONARY_HEADERS: List[str] = [
    "Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes",
]
DATA_DICTIONARY_KEYS: List[str] = [
    "sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes",
]

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
    "Market_Leaders", "Global_Markets", "Commodities_FX",
    "Mutual_Funds", "My_Portfolio", "My_Investments", "Top_10_Investments",
}
SPECIAL_SHEETS: Set[str] = {"Insights_Analysis", "Data_Dictionary"}

TOP10_ENGINE_DEFAULT_PAGES: List[str] = [
    "Market_Leaders", "Global_Markets", "Commodities_FX",
    "Mutual_Funds", "My_Portfolio", "My_Investments",
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
    "tadawul": 10,
    "argaam": 20,
    "eodhd": 30,
    "yahoo": 40,
    "finnhub": 50,
    "yahoo_chart": 60,
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
    # v5.77.0: bool guard. Without this, `_as_float(True)` returns 1.0
    # and `_as_float(False)` returns 0.0 (because bool subclasses int
    # in Python). If a provider schema shift sent a boolean for a
    # numeric field, the engine would silently parse it as 0.0/1.0.
    if isinstance(x, bool):
        return None
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
    v = _as_float(x)
    if v is None:
        return None
    if abs(v) > 1.5:
        return v / 100.0
    return v


def _as_pct_points(x: Any) -> Optional[float]:
    v = _as_float(x)
    if v is None:
        return None
    return v * 100.0 if abs(v) <= 1.5 else v


def _fraction_to_points(value: Any) -> Optional[float]:
    v = _as_float(value)
    if v is None:
        return None
    return v * 100.0


def _make_cache_key(
    symbol: Any,
    page: Any = "",
    provider_profile: Any = "default",
    schema_version: Optional[str] = None,
    *,
    mode: str = "cache",
) -> str:
    sym_s = _safe_str(symbol).strip()
    page_s = _safe_str(page).strip().lower() or "_"
    prof_s = _safe_str(provider_profile).strip() or "default"
    sv_s = _safe_str(schema_version).strip() if schema_version else _SCHEMA_VERSION
    mode_s = "cache" if mode == "cache" else "live"
    return f"quote:{sym_s}:{page_s}:{prof_s}:{sv_s}:{mode_s}"


_EMPTY_ROW_PRICE_KEYS: Tuple[str, ...] = (
    "current_price", "price", "close", "previous_close", "open_price",
    "day_high", "day_low",
)
_EMPTY_ROW_FUNDAMENTAL_KEYS: Tuple[str, ...] = (
    "market_cap", "revenue_ttm", "eps_ttm", "pe_ttm",
)
_EMPTY_ROW_DERIVED_KEYS: Tuple[str, ...] = (
    "rsi_14", "volatility_30d", "max_drawdown_1y", "week_52_high",
    "week_52_low",
)




# =============================================================================
# v5.74.0 — canonical scoring orchestration helpers
# =============================================================================

def _clear_recommendation_output_fields(row: Dict[str, Any]) -> None:
    """Clear stale recommendation outputs before a fresh final rewrite.

    This deliberately does NOT clear row["recommendation"]. The classifier reads
    an upstream provider value from that field to populate provider_rating before
    overwriting the final engine recommendation. Clearing it here would erase
    provider-rating audit evidence.
    """
    if not isinstance(row, dict):
        return
    for key in (
        "recommendation_detailed",
        "recommendation_detail",
        "recommendation_reason",
        "recommendation_priority",
        "recommendation_priority_band",
        "recommendation_source",
    ):
        row.pop(key, None)


def _coerce_scoring_errors_for_sheet(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, (list, tuple, set)):
        parts = [_safe_str(v) for v in value if _safe_str(v)]
        return "; ".join(parts) if parts else None
    text = _safe_str(value)
    return text or None


def _preserve_scoring_provenance(row: Dict[str, Any], patch: Mapping[str, Any]) -> None:
    if not isinstance(row, dict) or not isinstance(patch, Mapping):
        return

    src = patch.get("recommendation_source")
    if src:
        row["scoring_recommendation_source"] = _safe_str(src)

    errors_text = _coerce_scoring_errors_for_sheet(patch.get("scoring_errors"))
    if errors_text:
        row["scoring_errors"] = errors_text

    for key in (
        "scoring_schema_version",
        "opportunity_source",
        "overall_score_raw",
        "overall_penalty_factor",
    ):
        if key in patch and patch.get(key) is not None:
            row[key] = _json_safe(patch.get(key))


def _compute_scores_canonical_first(row: Dict[str, Any]) -> None:
    if not isinstance(row, dict):
        return

    if _SCORING_COMPUTE_SCORES_AVAILABLE and _scoring_compute_scores is not None:
        try:
            patch = _scoring_compute_scores(row)
            if isinstance(patch, Mapping) and patch:
                recommendation_owned_keys = {
                    "recommendation",
                    "recommendation_detailed",
                    "recommendation_detail",
                    "recommendation_reason",
                    "recommendation_priority",
                    "recommendation_priority_band",
                    "recommendation_source",
                }
                safe_patch = {
                    str(k): _json_safe(v)
                    for k, v in patch.items()
                    if str(k) not in recommendation_owned_keys
                }
                row.update(safe_patch)
                _preserve_scoring_provenance(row, patch)
                return
        except Exception as exc:
            logger.debug(
                "[engine_v2 v%s] canonical scoring failed for %s: %s: %s",
                __version__,
                _safe_str(row.get("symbol") or row.get("requested_symbol") or row.get("ticker"), "UNKNOWN"),
                exc.__class__.__name__,
                exc,
            )
            existing = row.get("scoring_errors")
            msg = f"canonical_scoring_failed:{type(exc).__name__}"
            existing_text = _coerce_scoring_errors_for_sheet(existing)
            row["scoring_errors"] = (existing_text + "; " + msg) if existing_text else msg

    _compute_scores_local_fallback(row)


def _compute_scores_fallback(row: Dict[str, Any]) -> None:
    """Backward-compatible v5.74.0 delegator."""
    _compute_scores_canonical_first(row)


def _empty_row_fundamentals_exempt(row: Mapping[str, Any]) -> bool:
    if not isinstance(row, Mapping):
        return False

    symbol = _safe_str(
        row.get("symbol") or row.get("requested_symbol") or row.get("ticker")
    ).upper()
    asset_class = _safe_str(row.get("asset_class") or row.get("assetClass")).strip().lower()
    page = _canonicalize_sheet_name(
        _safe_str(row.get("_page_context") or row.get("page") or row.get("sheet") or row.get("sheet_name"))
    )

    if symbol.endswith("=X") or symbol.endswith("=F"):
        return True
    if asset_class in {
        "fx", "currency", "currencies", "commodity", "commodities",
        "future", "futures", "etf", "fund", "mutual fund", "mutual_fund",
        "index", "indices",
    }:
        return True
    if page in {"Commodities_FX", "Mutual_Funds"}:
        return True
    return False

def _is_empty_data_row(row: Mapping[str, Any]) -> bool:
    if not isinstance(row, Mapping):
        return False

    def _has_value(key: str) -> bool:
        v = row.get(key)
        if v is None:
            return False
        if isinstance(v, str) and not v.strip():
            return False
        fv = _as_float(v)
        if fv is None:
            return True
        return fv != 0.0

    price_pop = sum(1 for k in _EMPTY_ROW_PRICE_KEYS if _has_value(k))
    fund_pop = sum(1 for k in _EMPTY_ROW_FUNDAMENTAL_KEYS if _has_value(k))
    derived_pop = sum(1 for k in _EMPTY_ROW_DERIVED_KEYS if _has_value(k))

    if price_pop == 0 and fund_pop == 0 and derived_pop == 0:
        return True

    if _empty_row_fundamentals_exempt(row):
        return False

    if fund_pop == 0:
        return True

    return False


def _mark_row_as_empty(row: Dict[str, Any]) -> None:
    if not isinstance(row, dict):
        return
    row["recommendation"] = "HOLD"
    row["recommendation_detailed"] = "HOLD"
    row["recommendation_source"] = "empty_row"
    row["recommendation_reason"] = (
        "HOLD: Insufficient provider data; recommendation suppressed / not actionable."
    )
    row["recommendation_priority"] = 4
    row["overall_score"] = None
    row["risk_score"] = None
    row["confidence_score"] = None
    row["opportunity_score"] = None
    row["rank_overall"] = None
    _v573_append_warning(row, "empty_row_no_provider_data")


def _v573_append_warning(row: Dict[str, Any], tag: str) -> None:
    if not isinstance(row, dict) or not tag:
        return
    raw = row.get("warnings")
    parts: List[str] = []
    if isinstance(raw, str) and raw.strip():
        parts = [p.strip() for p in raw.split(";") if p.strip()]
    elif isinstance(raw, (list, tuple, set)):
        parts = [_safe_str(p).strip() for p in raw if _safe_str(p).strip()]
    if tag in parts:
        return
    parts.append(tag)
    row["warnings"] = "; ".join(parts)


_V573_SANITIZATION_BOUNDS: Dict[str, Tuple[float, float]] = {
    # v5.77.0: tightened bounds. v5.76.0 allowed negative PE/PB/EV-EBITDA/
    # PEG values to pass through, which downstream scoring would
    # misinterpret. Negative P/E means negative earnings (valuation
    # undefined); negative P/B means negative shareholders' equity
    # (distressed); negative EV/EBITDA means negative EBITDA. These
    # should null out, not propagate as raw values.
    "pe_ttm":         (0.0, 500.0),    # was (-1000.0, 1000.0)
    "pe_forward":     (0.0, 500.0),    # was (-1000.0, 1000.0)
    "pb_ratio":       (0.0, 100.0),    # was (-200.0, 200.0)
    "ps_ratio":       (0.0, 100.0),    # was (0.0, 200.0)
    "ev_ebitda":      (0.0, 200.0),    # was (-500.0, 500.0)
    "peg_ratio":      (0.0, 20.0),     # was (-100.0, 100.0)
    "debt_to_equity": (0.0, 500.0),    # was (0.0, 1000.0)
    # v5.77.0: dividend_yield bounds added. Anything > 30% is almost
    # certainly a provider scale-factor error (1.45 returned for 145%
    # instead of 0.0145 for 1.45%). 30% ceiling is generous; only a
    # few mortgage REITs and BDCs legitimately exceed 15%.
    "dividend_yield": (0.0, 0.30),
}


def _v573_sanitization_enabled() -> bool:
    legacy_disable = os.getenv("TFB_DISABLE_V572_SANITIZATION", "")
    if str(legacy_disable).strip().lower() in ("1", "true", "yes", "y", "on"):
        return False
    primary = os.getenv("TFB_SANITIZATION_ENABLED", "true")
    return str(primary).strip().lower() not in ("0", "false", "no", "n", "off", "disabled")


def _sanitize_extreme_outliers(row: Dict[str, Any]) -> int:
    if not isinstance(row, dict):
        return 0
    nulled = 0
    for field, (lo, hi) in _V573_SANITIZATION_BOUNDS.items():
        v = _as_float(row.get(field))
        if v is None:
            continue
        if v < lo or v > hi:
            row[field] = None
            _v573_append_warning(row, f"sanitized:{field}_out_of_range")
            nulled += 1
    return nulled


def _sanitize_corrupt_52w_bounds(row: Dict[str, Any]) -> int:
    if not isinstance(row, dict):
        return 0
    nulled = 0
    hi = _as_float(row.get("week_52_high"))
    lo = _as_float(row.get("week_52_low"))
    if hi is not None and hi <= 0:
        row["week_52_high"] = None
        _v573_append_warning(row, "sanitized:week_52_high_nonpositive")
        nulled += 1
        hi = None
    if lo is not None and lo < 0:
        row["week_52_low"] = None
        _v573_append_warning(row, "sanitized:week_52_low_negative")
        nulled += 1
        lo = None
    if hi is not None and lo is not None:
        if lo > hi:
            row["week_52_high"] = None
            row["week_52_low"] = None
            _v573_append_warning(row, "sanitized:week_52_bounds_inverted")
            nulled += 2
        elif lo > 0 and hi / lo >= 1000.0:
            row["week_52_high"] = None
            row["week_52_low"] = None
            _v573_append_warning(row, "sanitized:week_52_bounds_scale_mismatch")
            nulled += 2
    return nulled


def _sanitize_cross_currency_revenue(row: Dict[str, Any]) -> int:
    if not isinstance(row, dict):
        return 0
    flagged = 0
    mc = _as_float(row.get("market_cap"))
    rev = _as_float(row.get("revenue_ttm"))
    cur = _safe_str(row.get("currency")).strip().upper()
    if mc is not None and cur == "USD" and mc > 1.0e13:
        _v573_append_warning(row, "market_cap_currency_suspect")
        flagged += 1
    if rev is not None and cur == "USD" and rev > 1.0e13:
        _v573_append_warning(row, "revenue_currency_suspect")
        flagged += 1
    return flagged


def _apply_v572_sanitization(row: Dict[str, Any]) -> Dict[str, int]:
    if not isinstance(row, dict):
        return {}
    if not _v573_sanitization_enabled():
        return {}
    return {
        "outliers_nulled":      _sanitize_extreme_outliers(row),
        "week_52_nulled":       _sanitize_corrupt_52w_bounds(row),
        "currency_flagged":     _sanitize_cross_currency_revenue(row),
    }


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
        merged_body.get("limit", limit),
        default=limit,
        lo=1,
        hi=5000,
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


_INSTRUMENT_CANONICAL_REQUIRED_KEYS: frozenset = frozenset({
    "symbol", "name", "asset_class", "exchange", "currency", "country",
    "current_price", "previous_close", "percent_change",
    "risk_score", "risk_bucket",
    "forecast_confidence", "confidence_score", "confidence_bucket",
    "overall_score", "opportunity_score", "rank_overall",
    "recommendation", "recommendation_reason",
    "recommendation_detailed", "recommendation_priority",

    "provider_rating", "recommendation_source",
    "recommendation_priority_band", "scoring_recommendation_source",
    "scoring_schema_version", "scoring_errors", "opportunity_source",
    "overall_score_raw", "overall_penalty_factor",
    "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",

    # v5.77.3: forecast_source elevated to required. ChatGPT v5.77.2 audit
    # flagged: an older external schema returning 106 columns would still
    # pass _instrument_contract_is_canonical() because forecast_source
    # (the 107th field added in v5.77.1) was not in the required set.
    # Treating it as required forces _usable_contract() to fall back to
    # the built-in STATIC canonical (107-column) when an external schema
    # source hasn't been updated. This guards against the silent
    # 106-column drift case during a partial deployment.
    "forecast_source",
})


def _instrument_contract_is_canonical(keys: Sequence[str]) -> bool:
    return _INSTRUMENT_CANONICAL_REQUIRED_KEYS.issubset(
        {_safe_str(k) for k in (keys or [])}
    )


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
        if not _instrument_contract_is_canonical(keys):
            return False
    if canon == "Top_10_Investments":
        if not ({"symbol", "ticker", "requested_symbol"} & keyset):
            return False
        if not set(TOP10_REQUIRED_FIELDS).issubset(keyset):
            return False
        if not _instrument_contract_is_canonical(keys):
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
    "upside_pct": ("upside_pct", "upsidePct", "upside_percent", "upsidePercent", "upside", "potentialUpside"),
    "valuation_score": ("valuation_score",),
    "forecast_price_1m": ("forecast_price_1m", "targetPrice1m", "priceTarget1m"),
    # v5.77.1: targetMeanPrice REMOVED from 3m aliases — Yahoo's
    # targetMeanPrice is the 12M analyst consensus, not a 3M target.
    # Putting it in 3m caused the engine to treat a 12M price target as
    # a quarterly forecast, which corrupted the 3M ROI math.
    "forecast_price_3m": ("forecast_price_3m", "targetPrice3m", "priceTarget3m"),
    # v5.77.1: targetMeanPrice ADDED to 12m aliases (correct horizon
    # for Yahoo's analyst consensus target). Also added analystTargetPrice
    # and consensusTarget for cross-provider coverage.
    "forecast_price_12m": ("forecast_price_12m", "targetPrice12m", "priceTarget12m", "targetMedianPrice", "targetHighPrice", "targetMeanPrice", "targetPrice", "analystTargetPrice", "consensusTarget"),
    "expected_roi_1m": ("expected_roi_1m", "expectedReturn1m", "roi1m"),
    "expected_roi_3m": ("expected_roi_3m", "expectedReturn3m", "roi3m"),
    "expected_roi_12m": ("expected_roi_12m", "expectedReturn12m", "roi12m"),
    # v5.77.1: forecast_source aliases for round-trip preservation.
    "forecast_source": ("forecast_source", "forecastSource", "forecast_origin", "forecastOrigin"),
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
    "fundamental_view": ("fundamental_view", "fundamentalView", "fundamental_rating"),
    "technical_view": ("technical_view", "technicalView", "technical_rating"),
    "risk_view": ("risk_view", "riskView", "risk_rating", "risk_label"),
    "value_view": ("value_view", "valueView", "value_rating", "valuation_label"),
    "recommendation": ("recommendation", "rating", "action", "reco", "consensus"),
    "recommendation_reason": ("recommendation_reason", "reason", "summary", "thesis", "analysis"),
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
    "sector_relative_score": ("sector_relative_score", "sectorAdjustedScore", "sectorAdjScore", "sector_adj_score", "sectorRelativeScore"),
    "conviction_score": ("conviction_score", "convictionScore", "conviction"),
    "top_factors": ("top_factors", "topFactors", "positives", "factors"),
    "top_risks": ("top_risks", "topRisks", "negatives", "risks"),
    "position_size_hint": ("position_size_hint", "positionSizeHint", "sizingHint", "sizing"),
    "recommendation_detailed": ("recommendation_detailed", "recommendationDetailed", "recommendationDetail", "reco_detail", "detailed_recommendation"),
    "recommendation_priority": ("recommendation_priority", "recoPriority", "priority", "reco_priority"),

    "provider_rating": ("provider_rating", "providerRating", "provider_recommendation", "providerRecommendation"),
    "recommendation_source": ("recommendation_source", "recommendationSource", "reco_source", "source_recommendation"),
    "recommendation_priority_band": ("recommendation_priority_band", "priority_band", "recoPriorityBand", "recommendationPriorityBand"),
    "scoring_recommendation_source": ("scoring_recommendation_source", "scoringRecommendationSource", "scoring_source", "scoringSource"),
    "scoring_schema_version": ("scoring_schema_version", "scoringSchemaVersion", "scoreSchemaVersion"),
    "scoring_errors": ("scoring_errors", "scoringErrors", "score_errors", "scoreErrors"),
    "opportunity_source": ("opportunity_source", "opportunitySource"),
    "overall_score_raw": ("overall_score_raw", "overallScoreRaw"),
    "overall_penalty_factor": ("overall_penalty_factor", "overallPenaltyFactor"),
    "candlestick_pattern": ("candlestick_pattern", "candlePattern", "candlestickPattern", "pattern"),
    "candlestick_signal": ("candlestick_signal", "candleSignal", "candlestickSignal", "patternSignal"),
    "candlestick_strength": ("candlestick_strength", "candleStrength", "candlestickStrength", "patternStrength"),
    "candlestick_confidence": ("candlestick_confidence", "candleConfidence", "candlestickConfidence", "patternConfidence"),
    "candlestick_patterns_recent": ("candlestick_patterns_recent", "recentPatterns", "candlestickPatternsRecent", "patterns5d", "patterns_recent"),
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


# =============================================================================
# v5.67.0 / v5.68.0 / v5.73.0 — Suffix -> locale map
# =============================================================================
_SUFFIX_TO_LOCALE: Dict[str, Tuple[str, str, str]] = {
    ".HK":    ("HKEX", "HKD", "Hong Kong"),
    ".L":     ("LSE", "GBp", "United Kingdom"),
    ".LON":   ("LSE", "GBp", "United Kingdom"),
    ".CO":    ("Copenhagen", "DKK", "Denmark"),
    ".NS":    ("NSE", "INR", "India"),
    ".BO":    ("BSE", "INR", "India"),
    ".SA":    ("B3", "BRL", "Brazil"),
    ".SR":    ("Tadawul", "SAR", "Saudi Arabia"),
    ".SAU":   ("Tadawul", "SAR", "Saudi Arabia"),
    ".TADAWUL": ("Tadawul", "SAR", "Saudi Arabia"),
    ".KSE":   ("Tadawul", "SAR", "Saudi Arabia"),
    ".TO":    ("TSX", "CAD", "Canada"),
    ".V":     ("TSX Venture", "CAD", "Canada"),
    ".CN":    ("CSE", "CAD", "Canada"),
    ".NE":    ("NEO Exchange", "CAD", "Canada"),
    ".XETRA": ("XETRA", "EUR", "Germany"),
    ".XETR":  ("XETRA", "EUR", "Germany"),
    ".DE":    ("XETRA", "EUR", "Germany"),
    ".F":     ("Frankfurt", "EUR", "Germany"),
    ".HM":    ("Hamburg", "EUR", "Germany"),
    ".MU":    ("Munich", "EUR", "Germany"),
    ".PA":    ("Euronext Paris", "EUR", "France"),
    ".PAR":   ("Euronext Paris", "EUR", "France"),
    ".AS":    ("Euronext Amsterdam", "EUR", "Netherlands"),
    ".AMS":   ("Euronext Amsterdam", "EUR", "Netherlands"),
    ".MI":    ("Borsa Italiana", "EUR", "Italy"),
    ".MIL":   ("Borsa Italiana", "EUR", "Italy"),
    ".MC":    ("BME", "EUR", "Spain"),
    ".MAD":   ("BME", "EUR", "Spain"),
    ".BR":    ("Euronext Brussels", "EUR", "Belgium"),
    ".BRU":   ("Euronext Brussels", "EUR", "Belgium"),
    ".LS":    ("Euronext Lisbon", "EUR", "Portugal"),
    ".HE":    ("Helsinki", "EUR", "Finland"),
    ".HEL":   ("Helsinki", "EUR", "Finland"),
    ".IR":    ("Euronext Dublin", "EUR", "Ireland"),
    ".ST":    ("Stockholm", "SEK", "Sweden"),
    ".STO":   ("Stockholm", "SEK", "Sweden"),
    ".OL":    ("Oslo", "NOK", "Norway"),
    ".OSL":   ("Oslo", "NOK", "Norway"),
    ".SW":    ("SIX", "CHF", "Switzerland"),
    ".SWX":   ("SIX", "CHF", "Switzerland"),
    ".VI":    ("Vienna", "EUR", "Austria"),
    ".VIE":   ("Vienna", "EUR", "Austria"),
    ".WA":    ("Warsaw", "PLN", "Poland"),
    ".WAR":   ("Warsaw", "PLN", "Poland"),
    ".AX":    ("ASX", "AUD", "Australia"),
    ".NZ":    ("NZX", "NZD", "New Zealand"),
    ".T":     ("TSE", "JPY", "Japan"),
    ".TYO":   ("TSE", "JPY", "Japan"),
    ".KS":    ("KRX", "KRW", "South Korea"),
    ".KQ":    ("KOSDAQ", "KRW", "South Korea"),
    ".SI":    ("SGX", "SGD", "Singapore"),
    ".KL":    ("Bursa Malaysia", "MYR", "Malaysia"),
    ".BK":    ("SET", "THB", "Thailand"),
    ".JK":    ("IDX", "IDR", "Indonesia"),
    ".SS":    ("Shanghai", "CNY", "China"),
    ".SZ":    ("Shenzhen", "CNY", "China"),
    ".TW":    ("TWSE", "TWD", "Taiwan"),
    ".TWO":   ("TPEx", "TWD", "Taiwan"),
    ".MX":    ("BMV", "MXN", "Mexico"),
    ".BA":    ("BCBA", "ARS", "Argentina"),
    ".JO":    ("JSE", "ZAR", "South Africa"),
    ".US":    ("NASDAQ/NYSE", "USD", "USA"),
    ".LSE":   ("LSE", "GBp", "United Kingdom"),
    ".LN":    ("LSE", "GBp", "United Kingdom"),
    ".NSE":   ("NSE", "INR", "India"),
    ".BSE":   ("BSE", "INR", "India"),
    ".JSE":   ("JSE", "ZAR", "South Africa"),
    ".ZA":    ("JSE", "ZAR", "South Africa"),
    ".HKG":   ("HKEX", "HKD", "Hong Kong"),
    ".SHG":   ("Shanghai", "CNY", "China"),
    ".SHE":   ("Shenzhen", "CNY", "China"),
    ".KOSDAQ": ("KOSDAQ", "KRW", "South Korea"),
    ".SGX":   ("SGX", "SGD", "Singapore"),
    ".ASX":   ("ASX", "AUD", "Australia"),
    ".KLSE":  ("Bursa Malaysia", "MYR", "Malaysia"),
    ".IDX":   ("IDX", "IDR", "Indonesia"),
    ".NZSE":  ("NZX", "NZD", "New Zealand"),
    ".NYSE":  ("NYSE", "USD", "USA"),
    ".NASDAQ": ("NASDAQ", "USD", "USA"),
    ".OQ":    ("NASDAQ", "USD", "USA"),
    ".NM":    ("NASDAQ", "USD", "USA"),
    ".NG":    ("NASDAQ", "USD", "USA"),
    ".KW":    ("Boursa Kuwait", "KWD", "Kuwait"),
    ".QA":    ("Qatar Exchange", "QAR", "Qatar"),
    ".QE":    ("Qatar Exchange", "QAR", "Qatar"),
    ".AE":    ("ADX", "AED", "UAE"),
    ".DFM":   ("DFM", "AED", "UAE"),
    ".ADX":   ("ADX", "AED", "UAE"),
    ".EG":    ("EGX", "EGP", "Egypt"),
    ".EGX":   ("EGX", "EGP", "Egypt"),
    ".TA":    ("TASE", "ILS", "Israel"),
    ".TASE":  ("TASE", "ILS", "Israel"),
}

_US_COUNTRY_TOKENS: Set[str] = {
    "", "USA", "US", "U.S.", "U.S.A.", "UNITED STATES", "UNITED STATES OF AMERICA",
}

_PERCENT_CHANGE_DAILY_MAX_ABS_FRACTION: float = 0.50


def _suffix_locale_for(symbol: str) -> Optional[Tuple[str, str, str]]:
    s = normalize_symbol(symbol)
    if not s or "." not in s:
        return None
    best_suffix: Optional[str] = None
    s_upper = s.upper()
    for suffix in _SUFFIX_TO_LOCALE:
        if s_upper.endswith(suffix):
            if best_suffix is None or len(suffix) > len(best_suffix):
                best_suffix = suffix
    if best_suffix is None:
        return None
    return _SUFFIX_TO_LOCALE[best_suffix]


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
        alias,
        alias.lower(),
        _norm_key(alias),
        _norm_key_loose(alias),
        alias.replace("_", " "),
        alias.replace("_", "-"),
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
    if not s:
        return ""
    locale = _suffix_locale_for(s)
    if locale is not None:
        return locale[0]
    if re.match(r"^[0-9]{4}$", s):
        return "Tadawul"
    if s.endswith("=X"):
        return "FX"
    if s.endswith("=F"):
        return "Futures"
    return "NASDAQ/NYSE"


def _infer_currency_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if not s:
        return ""
    locale = _suffix_locale_for(s)
    if locale is not None:
        return locale[1]
    if re.match(r"^[0-9]{4}$", s):
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
    return "USD"


def _infer_country_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if not s:
        return ""
    locale = _suffix_locale_for(s)
    if locale is not None:
        return locale[2]
    if re.match(r"^[0-9]{4}$", s):
        return "Saudi Arabia"
    if s.endswith("=X") or s.endswith("=F"):
        return "Global"
    if _NORMALIZE_AVAILABLE and _nz_get_country_from_symbol is not None:
        try:
            nz_country = _nz_get_country_from_symbol(s)
        except Exception:
            nz_country = None
        nz_country = (nz_country or "").strip()
        if nz_country and nz_country.upper() not in {
            "USA", "US", "UNITED STATES", "GLOBAL", "",
        }:
            return nz_country
    return "USA"


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
    # v5.75.0 NAME FIX: return "" for unknown equities (was: returned the
    # symbol itself, which caused Name=DBA.US / SCHD.US / NDA-FI.HE / SOXX
    # on the dashboard). The Yahoo enrichment pass will chase a real name
    # from the fundamentals provider's longName / shortName / displayName
    # when name is blank; if that also fails the column stays blank,
    # which is the honest signal.
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
    return ""


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
        # v5.75.0 NAME FIX: do NOT fall back to `sym` if no real name is
        # inferred. For Commodities_FX with known display names this still
        # populates correctly via _COMMODITY_DISPLAY_NAMES / FX synthesis;
        # for everything else, blank is the honest signal.
        if inferred_name and (not current_name or current_name == sym):
            out["name"] = inferred_name

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

    # v5.77.1: tag forecast_source = "provider_target" when an upstream
    # provider supplied an analyst price target (any of the known
    # target_* aliases). v5.77.0 added the protection check in
    # _phase_ii_quality_forecast (skip overwrite if source ==
    # "provider_target") but never actually set the tag here — leaving
    # the feature half-finished. This block closes that gap by detecting
    # the presence of a provider-target alias in the raw source and
    # tagging accordingly. If no provider target alias was present,
    # forecast_source is NOT set here; _phase_ii_quality_forecast will
    # set it to "phase_ii_synthetic" when it synthesizes the forecast.
    #
    # v5.77.2: switch alias matching from naive `.lower()` to
    # `_norm_key_loose()` (strips ALL non-alphanumerics). Under v5.77.1
    # the matcher worked for camelCase ("targetMeanPrice") but silently
    # missed snake_case variants ("target_mean_price") because
    # "targetmeanprice".lower() != "target_mean_price". REST providers
    # serving Python clients commonly emit snake_case, so this gap
    # meant a meaningful slice of provider targets fell through to
    # Phase-II synthesis and got overwritten. ChatGPT v5.77.1 audit
    # caught this. _norm_key_loose collapses both forms to
    # "targetmeanprice", so a single canonical alias list now covers
    # camelCase, snake_case, kebab-case, dotted, and mixed variants.
    if out.get("forecast_price_12m") is not None and not _safe_str(out.get("forecast_source")):
        provider_target_aliases_12m = (
            "targetPrice12m", "priceTarget12m", "targetMedianPrice",
            "targetHighPrice", "targetMeanPrice", "targetPrice",
            "priceTarget", "analystTargetPrice", "consensusTarget",
        )
        provider_target_aliases_3m = (
            "targetPrice3m", "priceTarget3m",
        )
        # v5.77.2: build normalized lookups using _norm_key_loose for
        # whitespace/underscore/case tolerance.
        src_keys_loose = {_norm_key_loose(k): k for k in src.keys()}
        flat_keys_loose = {_norm_key_loose(k): k for k in flat.keys()}
        for alias in provider_target_aliases_12m + provider_target_aliases_3m:
            alias_loose = _norm_key_loose(alias)
            if not alias_loose:
                continue
            if alias_loose in src_keys_loose or alias_loose in flat_keys_loose:
                raw_v = (
                    src.get(src_keys_loose.get(alias_loose))
                    if alias_loose in src_keys_loose
                    else flat.get(flat_keys_loose.get(alias_loose))
                )
                if not _is_blank_value(raw_v):
                    out["forecast_source"] = "provider_target"
                    break

    inferred_symbol = out.get("symbol") or normalized_symbol or requested_symbol
    inferred_name = _infer_display_name_from_symbol(inferred_symbol)
    current_name = _safe_str(out.get("name"))
    # v5.75.0 NAME FIX: only stamp `inferred_name` if it is meaningful. Do
    # NOT fall back to symbol-as-name; leave `name` blank so the Yahoo
    # enrichment pass (which treats "" as missing) can fill it with the
    # provider's longName / shortName / displayName.
    if inferred_name and (not current_name or current_name == _safe_str(inferred_symbol)):
        out["name"] = inferred_name
    elif not current_name:
        out["name"] = ""
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

    locale = _suffix_locale_for(inferred_symbol)
    if locale is not None and locale[2] != "USA":
        derived_exch, derived_curr, derived_country = locale
        if _safe_str(out.get("country")).upper() in _US_COUNTRY_TOKENS:
            out["country"] = derived_country
        current_exch_upper = _safe_str(out.get("exchange")).upper()
        if (not current_exch_upper) or "NASDAQ" in current_exch_upper or "NYSE" in current_exch_upper:
            out["exchange"] = derived_exch
        current_curr_upper = _safe_str(out.get("currency")).upper()
        if (not current_curr_upper) or current_curr_upper == "USD":
            out["currency"] = derived_curr

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

    if pct is None and price is not None and prev not in (None, 0):
        pct = (price - prev) / prev
        out["percent_change"] = round(pct, 8)
    elif pct is not None:
        if price is not None and prev not in (None, 0):
            true_fraction = (price - prev) / prev
            err_as_fraction = abs(pct - true_fraction)
            err_as_points = abs(pct - true_fraction * 100.0)
            if err_as_points < err_as_fraction:
                out["percent_change"] = round(pct / 100.0, 8)
            else:
                out["percent_change"] = round(pct, 8)
        else:
            if abs(pct) > 1.5:
                out["percent_change"] = round(pct / 100.0, 8)
            else:
                out["percent_change"] = round(pct, 8)

    high52 = _as_float(out.get("week_52_high"))
    low52 = _as_float(out.get("week_52_low"))
    if price is not None and high52 is not None and low52 is not None and high52 > low52 and out.get("week_52_position_pct") is None:
        out["week_52_position_pct"] = round(((price - low52) / (high52 - low52)) * 100.0, 6)

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
    if upl is not None and pos_cost not in (None, 0) and out.get("unrealized_pl_pct") is None:
        out["unrealized_pl_pct"] = round((upl / pos_cost) * 100.0, 6)

    out = _apply_symbol_context_defaults(out, symbol=inferred_symbol)
    if _as_float(out.get("current_price")) is not None and _safe_str(out.get("warnings")).lower() == "no live provider data available":
        out["warnings"] = "Recovered from history/chart fallback"

    return out


def _normalize_to_schema_keys(keys: Sequence[str], headers: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    src = _canonicalize_provider_row(dict(row or {}), requested_symbol=_safe_str((row or {}).get("requested_symbol")), normalized_symbol=normalize_symbol(_safe_str((row or {}).get("symbol") or (row or {}).get("ticker"))), provider=_safe_str((row or {}).get("data_provider") or (row or {}).get("provider")))
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
            out.setdefault("confidence_score", round(_clamp(conf, 0.0, 100.0), 2))
    if conf is not None and out.get("confidence_bucket") in (None, ""):
        _cb = ""
        if _BUCKETS_AVAILABLE and _bk_confidence_bucket_from_score is not None:
            try:
                _cb = _bk_confidence_bucket_from_score(conf)
            except Exception:
                _cb = ""
        if not _cb:
            _cb = "HIGH" if conf >= 75 else "MODERATE" if conf >= 50 else "LOW"
        out["confidence_bucket"] = _cb

    if target == "Commodities_FX" or sym.endswith("=F") or sym.endswith("=X"):
        out.setdefault("data_provider", _safe_str(out.get("data_provider"), "history_or_fallback"))
        if out.get("forecast_confidence") in (None, ""):
            out["forecast_confidence"] = 0.55
        if out.get("confidence_score") in (None, ""):
            out["confidence_score"] = 55.0
        if out.get("forecast_confidence") not in (None, "") and out.get("confidence_bucket") in (None, ""):
            conf = _as_float(out.get("confidence_score")) or ((_as_float(out.get("forecast_confidence")) or 0.55) * 100.0)
            _cb = ""
            if _BUCKETS_AVAILABLE and _bk_confidence_bucket_from_score is not None:
                try:
                    _cb = _bk_confidence_bucket_from_score(conf)
                except Exception:
                    _cb = ""
            if not _cb:
                _cb = "HIGH" if conf >= 75 else "MODERATE" if conf >= 50 else "LOW"
            out["confidence_bucket"] = _cb
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


def _compute_scores_local_fallback(row: Dict[str, Any]) -> None:
    try:
        sanitized_counts = _apply_v572_sanitization(row)
    except Exception as exc:
        sanitized_counts = None
        err = f"_apply_v572_sanitization: {type(exc).__name__}: {exc}"
        errs = row.get("scoring_errors")
        if isinstance(errs, list):
            errs.append(err)
        else:
            row["scoring_errors"] = [err]
    if sanitized_counts and isinstance(sanitized_counts, dict):
        for ratio_name, count in sanitized_counts.items():
            if count and isinstance(count, (int, float)) and count > 0:
                _v573_append_warning(row, f"sanitized:{ratio_name}")

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
    row.setdefault("forecast_confidence", round(_clamp(conf, 0.0, 1.0), 4))
    row.setdefault("confidence_score", round(_clamp(conf * 100.0, 0.0, 100.0), 2))

    if row.get("risk_score") is None:
        vol = _as_pct_points(row.get("volatility_90d"))
        drawdown = _as_pct_points(row.get("max_drawdown_1y"))
        var95 = _as_pct_points(row.get("var_95_1d"))
        risk_score = 10.0
        if vol is not None:
            risk_score += min(max(vol, 0.0) * 0.57, 30.0)
        if drawdown is not None:
            risk_score += min(abs(drawdown) * 0.40, 24.0)
        if var95 is not None:
            risk_score += min(abs(var95) * 2.80, 15.0)
        if beta is not None:
            risk_score += min(max(beta, 0.0) * 5.00, 12.0)
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

    # v5.77.2: tag forecast_source = "fallback" when this fallback path
    # synthesizes forecast prices. ChatGPT v5.77.1 audit caught this:
    # the header documented "fallback" as one of the four forecast_source
    # values but no code path ever wrote it. Track whether this function
    # actually creates any forecast price (vs. inheriting one from
    # upstream) so we only stamp "fallback" when WE are the origin.
    _fallback_created_forecast = False

    if price is not None and row.get("forecast_price_1m") is None:
        drift = max(0.5, min(4.0, seed_best_roi if seed_best_roi else 1.0))
        row["forecast_price_1m"] = round(price * (1.0 + drift / 300.0), 4)
        _fallback_created_forecast = True
    if price is not None and row.get("forecast_price_3m") is None:
        drift = max(1.0, min(8.0, seed_best_roi if seed_best_roi else 3.0))
        row["forecast_price_3m"] = round(price * (1.0 + drift / 100.0), 4)
        _fallback_created_forecast = True
    if price is not None and row.get("forecast_price_12m") is None:
        drift = max(3.0, min(18.0, (seed_roi_12m if seed_roi_12m is not None else seed_best_roi) or 8.0))
        row["forecast_price_12m"] = round(price * (1.0 + drift / 100.0), 4)
        _fallback_created_forecast = True

    # Stamp forecast_source = "fallback" only if we generated at least
    # one forecast AND no upstream source is already set (provider_target
    # or phase_ii_synthetic both take precedence — we don't overwrite).
    if _fallback_created_forecast and not _safe_str(row.get("forecast_source")):
        row["forecast_source"] = "fallback"

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
        rs = _as_float(row.get("risk_score"))
        rb = ""
        if _BUCKETS_AVAILABLE and _bk_risk_bucket_from_score is not None:
            try:
                rb = _bk_risk_bucket_from_score(rs)
            except Exception:
                rb = ""
        if not rb:
            rs_f = rs if rs is not None else 50.0
            rb = "LOW" if rs_f < 35 else "MODERATE" if rs_f < 70 else "HIGH"
        row["risk_bucket"] = rb

    if not row.get("confidence_bucket"):
        cs = _as_float(row.get("confidence_score"))
        cb = ""
        if _BUCKETS_AVAILABLE and _bk_confidence_bucket_from_score is not None:
            try:
                cb = _bk_confidence_bucket_from_score(cs)
            except Exception:
                cb = ""
        if not cb:
            cs_f = cs if cs is not None else 55.0
            cb = "HIGH" if cs_f >= 75 else "MODERATE" if cs_f >= 50 else "LOW"
        row["confidence_bucket"] = cb


def _compute_recommendation(row: Dict[str, Any]) -> None:
    """v5.74.0 / v5.75.0 / v5.76.0 — single canonical recommendation
    delegator.

    v5.76.0: redundant `_clear_recommendation_output_fields(row)` call
    removed. The v5.75.0 `_classify_recommendation_8tier` self-clears
    its six recommendation output fields at entry (Step 2a), so the
    explicit clear in this delegator was a no-op that only added
    noise to traces. The `_clear_recommendation_output_fields` helper
    is still defined and exported for any external caller that still
    references it.
    """
    _classify_recommendation_8tier(row)


def _apply_rank_overall(rows: List[Dict[str, Any]]) -> None:
    scored: List[Tuple[int, float]] = []
    for i, row in enumerate(rows):
        score = _as_float(row.get("overall_score"))
        if score is None:
            if isinstance(row, dict):
                _v573_append_warning(row, "rank_skipped_no_overall_score")
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
    if not isinstance(criteria, dict):
        criteria = {}
    try:
        max_chars_raw = os.getenv("TFB_CRITERIA_SNAPSHOT_MAX_CHARS", "2000")
        max_chars = int(max_chars_raw) if str(max_chars_raw).strip() else 2000
    except Exception:
        max_chars = 2000
    if max_chars < 100:
        max_chars = 100
    try:
        import json
        safe = _json_safe(criteria) if criteria else {}
        s = json.dumps(
            safe,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            default=str,
        )
    except Exception:
        return "{}"
    if len(s) <= max_chars:
        return s
    marker_template = "... [truncated at {n} chars]"
    marker = marker_template.format(n=len(s))
    cut = max_chars - len(marker)
    if cut < 1:
        cut = 1
    return s[:cut] + marker


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

    # v5.77.0: route sync calls through the dedicated provider pool
    # (lazily initialized via _get_provider_executor) instead of
    # asyncio.to_thread's default executor (~36 threads). Batches of
    # 200+ symbols hitting the default pool can stall the event loop
    # under contention. The dedicated pool defaults to 200 workers and
    # is sized via TFB_PROVIDER_POOL_WORKERS.
    loop = asyncio.get_running_loop()
    executor = _get_provider_executor()
    if executor is None:
        # Fallback to asyncio.to_thread if executor creation failed.
        result = await asyncio.to_thread(fn, *args, **kwargs)
    else:
        result = await loop.run_in_executor(executor, lambda: fn(*args, **kwargs))
    return await result if inspect.isawaitable(result) else result


# v5.77.0: dedicated thread pool for synchronous provider calls.
# Lazily initialized on first use so import time isn't blocked.
# Default worker count = 200 (configurable via TFB_PROVIDER_POOL_WORKERS).
# Floor of 32 prevents accidental misconfiguration to a degenerate pool.
#
# v5.77.1: race-condition fix. The v5.77.0 lazy initializer used an
# `if _PROVIDER_EXECUTOR is not None` check with NO lock. Under
# concurrent first-touch (e.g. 200 quotes starting simultaneously
# before the executor is initialized), 200 coroutines would all see
# `_PROVIDER_EXECUTOR is None` and each create its own
# ThreadPoolExecutor, spawning thousands of orphan threads before
# the garbage collector caught up. The fix is a standard double-
# checked locking pattern using threading.Lock (not asyncio.Lock —
# this is a sync function called from inside async coroutines, and
# threading.Lock is the correct primitive for sync code).
import concurrent.futures as _concurrent_futures
import threading as _threading

_PROVIDER_EXECUTOR: Optional[Any] = None
_PROVIDER_EXECUTOR_LOCK: "_threading.Lock" = _threading.Lock()


def _get_provider_executor() -> Optional[Any]:
    """Return the shared provider thread pool, creating it on first call.

    v5.77.1: double-checked locking. The fast path (no lock acquisition)
    is the common case once the executor is initialized. The slow path
    (lock acquired) only runs during the first concurrent first-touch
    burst and re-checks the singleton inside the critical section so
    only one executor is ever created.
    """
    global _PROVIDER_EXECUTOR
    if _PROVIDER_EXECUTOR is not None:
        return _PROVIDER_EXECUTOR
    with _PROVIDER_EXECUTOR_LOCK:
        # Double-check inside the critical section.
        if _PROVIDER_EXECUTOR is not None:
            return _PROVIDER_EXECUTOR
        try:
            workers_raw = os.getenv("TFB_PROVIDER_POOL_WORKERS", "200")
            workers = int(workers_raw) if str(workers_raw).strip() else 200
        except Exception:
            workers = 200
        workers = max(32, min(1000, workers))
        try:
            _PROVIDER_EXECUTOR = _concurrent_futures.ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix="tfb-provider",
            )
        except Exception as exc:
            logger.debug(
                "[engine_v2 v%s] dedicated provider pool init failed: %s: %s",
                __version__, exc.__class__.__name__, exc,
            )
            _PROVIDER_EXECUTOR = None
        return _PROVIDER_EXECUTOR


def _shutdown_provider_executor() -> None:
    """v5.77.1: graceful shutdown of the dedicated provider pool.

    Called from DataEngineV5.aclose() so the thread pool is released
    on engine teardown. Without this, the worker threads would linger
    for the process lifetime even after the engine is closed.
    """
    global _PROVIDER_EXECUTOR
    with _PROVIDER_EXECUTOR_LOCK:
        if _PROVIDER_EXECUTOR is None:
            return
        try:
            _PROVIDER_EXECUTOR.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            # cancel_futures only exists on Python 3.9+.
            try:
                _PROVIDER_EXECUTOR.shutdown(wait=False)
            except Exception:
                pass
        except Exception:
            pass
        _PROVIDER_EXECUTOR = None


def reset_provider_executor() -> None:
    """v5.77.2: public test helper. Shuts down the current executor and
    clears the singleton so the next _get_provider_executor() call
    rebuilds it from scratch — picking up any environment changes
    (TFB_PROVIDER_POOL_WORKERS) made since the previous build.

    Intended use: test suites that need to verify pool sizing under
    different env-var values. NOT meant for production runtime use —
    calling this while requests are in flight will orphan their
    futures. Prefer _shutdown_provider_executor() in production
    teardown paths.
    """
    _shutdown_provider_executor()


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
        self._lock = asyncio.Lock()
        self._tasks: Dict[str, asyncio.Task[Any]] = {}

    async def execute(self, key: str, factory: Any) -> Any:
        async with self._lock:
            task = self._tasks.get(key)
            if task is None:
                task = asyncio.create_task(factory())
                self._tasks[key] = task
        try:
            return await task
        finally:
            async with self._lock:
                if self._tasks.get(key) is task:
                    self._tasks.pop(key, None)


class MultiLevelCache:
    """In-memory TTL cache.

    v5.75.0 docstring note: despite the name, this is currently L1
    (in-memory) only. There is no L2 (Redis / disk) backend wired in -
    the name is preserved for back-compat with existing callers and to
    leave room for an L2 addition without renaming. If a later release
    adds an L2 tier, this docstring should be updated.

    v5.77.0: eviction changed from FIFO to LRU. v5.76.0 evicted via
    `next(iter(self._data.keys()))` which returns the FIRST INSERTED
    key (FIFO), regardless of access pattern. Hot keys (AAPL, MSFT,
    NVDA, 2222.SR) inserted early were evicted constantly while
    obscure tickers in recent batches survived. Now backed by an
    OrderedDict with move_to_end() called on every successful get(),
    so eviction picks the LEAST RECENTLY USED key (popitem(last=False)).
    Hot keys stay hot.
    """

    def __init__(self, name: str, l1_ttl: int = 60, max_l1_size: int = 5000) -> None:
        self.name = name
        self.l1_ttl = max(1, int(l1_ttl))
        self.max_l1_size = max(1, int(max_l1_size))
        # v5.77.0: OrderedDict for LRU semantics (was: plain Dict).
        from collections import OrderedDict as _OrderedDict
        self._data: "_OrderedDict[str, Tuple[float, Any]]" = _OrderedDict()
        self._lock = asyncio.Lock()

    def _key(self, **kwargs: Any) -> str:
        items = sorted((str(k), _safe_str(v)) for k, v in kwargs.items())
        return "|".join([self.name] + [f"{k}={v}" for k, v in items])

    async def get(self, **kwargs: Any) -> Any:
        key = self._key(**kwargs)
        async with self._lock:
            item = self._data.get(key)
            if not item:
                return None
            expires_at, value = item
            if expires_at < time.time():
                self._data.pop(key, None)
                return None
            # v5.77.0: mark as recently used so LRU eviction skips it.
            self._data.move_to_end(key)
            return value

    async def set(self, value: Any, **kwargs: Any) -> None:
        key = self._key(**kwargs)
        async with self._lock:
            # v5.77.0: LRU eviction via popitem(last=False) - pops the
            # OLDEST USED key, not the first inserted. If the key is
            # already present we move it to the end so an overwrite
            # also refreshes recency.
            if key in self._data:
                self._data.move_to_end(key)
            elif len(self._data) >= self.max_l1_size:
                try:
                    self._data.popitem(last=False)
                except KeyError:
                    pass
            self._data[key] = (time.time() + self.l1_ttl, value)


class ProviderRegistry:
    def __init__(self) -> None:
        self._stats: Dict[str, Dict[str, Any]] = {}

    async def get_provider(self, provider: str) -> Tuple[Optional[Any], Any]:
        module = None
        candidates = [
            f"core.providers.{provider}",
            f"providers.{provider}",
            f"core.providers.{provider}_provider",
            f"providers.{provider}_provider",
        ]
        for mod_path in candidates:
            try:
                module = import_module(mod_path)
                break
            except Exception:
                continue
        stats = type(
            "ProviderStats",
            (),
            {"is_circuit_open": False, "last_import_error": "" if module is not None else "provider module missing"},
        )()
        return module, stats

    async def record_success(self, provider: str, latency_ms: float) -> None:
        stat = self._stats.setdefault(provider, {"success": 0, "failure": 0, "last_error": "", "latency_ms": 0.0})
        stat["success"] += 1
        stat["latency_ms"] = round(float(latency_ms or 0.0), 2)

    async def record_failure(self, provider: str, error: str) -> None:
        stat = self._stats.setdefault(provider, {"success": 0, "failure": 0, "last_error": "", "latency_ms": 0.0})
        stat["failure"] += 1
        stat["last_error"] = _safe_str(error)

    async def get_stats(self) -> Dict[str, Any]:
        return {k: dict(v) for k, v in self._stats.items()}


def _pick_provider_callable(module: Any, provider: str) -> Optional[Any]:
    for name in ("get_quote", "fetch_quote", "fetch_enriched_quote", "get_enriched_quote", "quote"):
        fn = getattr(module, name, None)
        if callable(fn):
            return fn
    for attr in (provider, f"{provider}_quote", "client", "service"):
        obj = getattr(module, attr, None)
        if obj is not None:
            for name in ("get_quote", "fetch_quote", "get_enriched_quote"):
                fn = getattr(obj, name, None)
                if callable(fn):
                    return fn
    return None


# =============================================================================
# Engine symbols proxy
# =============================================================================
class _EngineSymbolsReaderProxy:
    def __init__(self, engine: "DataEngineV5") -> None:
        self._engine = engine

    async def get_symbols_for_sheet(self, sheet: str, limit: int = 5000) -> List[str]:
        return await self._engine.get_sheet_symbols(sheet, limit=limit)

    async def get_symbols_for_page(self, page: str, limit: int = 5000) -> List[str]:
        return await self._engine.get_page_symbols(page, limit=limit)

    async def list_symbols_for_page(self, page: str, limit: int = 5000) -> List[str]:
        return await self._engine.list_symbols_for_page(page, limit=limit)



# =============================================================================
# DataEngineV5
# =============================================================================
class DataEngineV5:
    def __init__(self, settings: Any = None) -> None:
        self.settings = settings if settings is not None else _try_get_settings()
        self.flags = _feature_flags(self.settings)
        self.version = __version__

        self.primary_provider = (
            _safe_str(getattr(self.settings, "primary_provider", "eodhd") if self.settings is not None else _safe_env("PRIMARY_PROVIDER", "eodhd")).lower()
            or "eodhd"
        )
        self.enabled_providers = _get_env_list("ENABLED_PROVIDERS", DEFAULT_PROVIDERS)
        self.ksa_providers = _get_env_list("KSA_PROVIDERS", DEFAULT_KSA_PROVIDERS)
        self.global_providers = _get_env_list("GLOBAL_PROVIDERS", DEFAULT_GLOBAL_PROVIDERS)
        self.non_ksa_primary_provider = (
            _safe_str(
                getattr(self.settings, "non_ksa_primary_provider", None) if self.settings is not None else None,
                _safe_env("NON_KSA_PRIMARY_PROVIDER", "eodhd"),
            ).lower()
            or "eodhd"
        )
        configured_non_ksa_pages = [
            _canonicalize_sheet_name(p)
            for p in _get_env_list("NON_KSA_PRIMARY_PAGES", list(NON_KSA_EODHD_PRIMARY_PAGES))
            if _safe_str(p)
        ]
        self.page_primary_providers = {
            page: self.non_ksa_primary_provider
            for page in configured_non_ksa_pages
            if page
        }
        self.history_fallback_providers = _get_env_list(
            "HISTORY_FALLBACK_PROVIDERS",
            ["yahoo_chart", "yahoo", "eodhd", "finnhub", "tadawul", "argaam"],
        )
        self.max_concurrency = _get_env_int("DATA_ENGINE_MAX_CONCURRENCY", 25)
        self.request_timeout = _get_env_float("DATA_ENGINE_TIMEOUT_SECONDS", 20.0)
        self.ksa_disallow_eodhd = _get_env_bool("KSA_DISALLOW_EODHD", True)

        self.schema_strict_sheet_rows = _get_env_bool("SCHEMA_STRICT_SHEET_ROWS", True)
        self.top10_force_full_schema = _get_env_bool("TOP10_FORCE_FULL_SCHEMA", True)
        self.instrument_force_canonical_schema = _get_env_bool("INSTRUMENT_FORCE_CANONICAL_SCHEMA", True)
        self.rows_hydrate_external = _get_env_bool("ROWS_HYDRATE_EXTERNAL_READER", True)

        self._sem = asyncio.Semaphore(max(1, self.max_concurrency))
        self._singleflight = SingleFlight()
        self._registry = ProviderRegistry()
        self._cache = MultiLevelCache(
            name="data_engine",
            l1_ttl=_get_env_int("CACHE_L1_TTL", 60),
            max_l1_size=_get_env_int("CACHE_L1_MAX", 5000),
        )
        self._symbols_cache = MultiLevelCache(
            name="sheet_symbols",
            l1_ttl=_get_env_int("SHEET_SYMBOLS_L1_TTL", 300),
            max_l1_size=_get_env_int("SHEET_SYMBOLS_L1_MAX", 256),
        )

        self._symbols_reader_lock = asyncio.Lock()
        self._symbols_reader_ready = False
        self._symbols_reader_obj: Any = None
        self._symbols_reader_source = ""

        self._rows_reader_lock = asyncio.Lock()
        self._rows_reader_ready = False
        self._rows_reader_obj: Any = None
        self._rows_reader_source = ""

        self._sheet_snapshots: Dict[str, Dict[str, Any]] = {}
        self._sheet_symbol_resolution_meta: Dict[str, Dict[str, Any]] = {}

        self.symbols_reader = _EngineSymbolsReaderProxy(self)

    async def aclose(self) -> None:
        # v5.77.1: gracefully release the dedicated provider thread pool
        # on engine teardown so worker threads don't linger.
        try:
            _shutdown_provider_executor()
        except Exception:
            pass
        return

    async def execute_sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def run_sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def build_analysis_sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def run_analysis_sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def get_rows_for_sheet(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def get_rows_for_page(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_page_rows(*args, **kwargs)

    async def get_analysis_rows_batch(self, symbols: List[str], mode: str = "", *, schema: Any = None) -> Dict[str, Dict[str, Any]]:
        return await self.get_enriched_quotes_batch(symbols, mode=mode, schema=schema)

    async def get_analysis_row_dict(self, symbol: str, use_cache: bool = True, *, schema: Any = None) -> Dict[str, Any]:
        return await self.get_enriched_quote_dict(symbol, use_cache=use_cache, schema=schema)

    def get_page_snapshot(self, *args: Any, **kwargs: Any) -> Optional[Dict[str, Any]]:
        return self.get_cached_sheet_snapshot(*args, **kwargs)

    def _store_sheet_snapshot(self, sheet: str, payload: Dict[str, Any]) -> None:
        target = _canonicalize_sheet_name(sheet)
        if not target or not isinstance(payload, dict):
            return
        self._sheet_snapshots[target] = dict(payload)

    def get_cached_sheet_snapshot(
        self,
        sheet: Optional[str] = None,
        page: Optional[str] = None,
        sheet_name: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        target = _canonicalize_sheet_name(sheet or page or sheet_name or "")
        if not target:
            return None
        snap = self._sheet_snapshots.get(target)
        return dict(snap) if isinstance(snap, dict) else None

    def get_sheet_snapshot(
        self,
        page: Optional[str] = None,
        sheet: Optional[str] = None,
        sheet_name: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        return self.get_cached_sheet_snapshot(sheet=sheet, page=page, sheet_name=sheet_name)

    def get_cached_sheet_rows(
        self,
        sheet_name: Optional[str] = None,
        sheet: Optional[str] = None,
        page: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        return self.get_cached_sheet_snapshot(sheet=sheet, page=page, sheet_name=sheet_name)

    def _set_sheet_symbols_meta(self, sheet: str, source: str, count: int, note: Optional[str] = None) -> None:
        target = _canonicalize_sheet_name(sheet)
        if not target:
            return
        self._sheet_symbol_resolution_meta[target] = {
            "sheet": target,
            "source": source or "",
            "count": int(count or 0),
            "note": note or "",
            "timestamp_utc": _now_utc_iso(),
        }

    def _get_sheet_symbols_meta(self, sheet: str) -> Dict[str, Any]:
        target = _canonicalize_sheet_name(sheet)
        meta = self._sheet_symbol_resolution_meta.get(target)
        return dict(meta) if isinstance(meta, dict) else {}

    @staticmethod
    def _extract_row_symbol(row: Dict[str, Any]) -> str:
        if not isinstance(row, dict):
            return ""
        for k in ("symbol", "ticker", "code", "requested_symbol", "Symbol", "Ticker", "Code"):
            v = row.get(k)
            if v:
                return _safe_str(v)
        return ""

    def _get_cached_snapshot_symbol_map(self, sheet: str) -> Dict[str, Dict[str, Any]]:
        target = _canonicalize_sheet_name(sheet)
        snap = self.get_cached_sheet_snapshot(sheet=target)
        rows = _coerce_rows_list(snap)
        out: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            sym = normalize_symbol(self._extract_row_symbol(row))
            if sym and sym not in out:
                out[sym] = dict(row)
        return out

    @staticmethod
    def _non_empty_field_count(row: Optional[Dict[str, Any]]) -> int:
        if not isinstance(row, dict):
            return 0
        return sum(1 for v in row.values() if v not in (None, "", [], {}))

    def _get_best_cached_snapshot_row_for_symbol(self, symbol: str, prefer_sheet: str = "") -> Optional[Dict[str, Any]]:
        sym = normalize_symbol(symbol)
        if not sym:
            return None
        preferred = _canonicalize_sheet_name(prefer_sheet) if prefer_sheet else ""
        best_row: Optional[Dict[str, Any]] = None
        best_score: float = -1.0
        for sheet_name, snap in self._sheet_snapshots.items():
            rows = _coerce_rows_list(snap)
            if not rows:
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                row_sym = normalize_symbol(self._extract_row_symbol(row))
                if row_sym != sym:
                    continue
                score = float(self._non_empty_field_count(row))
                if preferred and _canonicalize_sheet_name(sheet_name) == preferred:
                    score += 1000.0
                if score > best_score:
                    best_score = score
                    best_row = dict(row)
        return best_row

    def _finalize_payload(
        self,
        *,
        sheet: str,
        headers: List[str],
        keys: List[str],
        row_objects: List[Dict[str, Any]],
        include_matrix: bool,
        status: str = "success",
        meta: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> Dict[str, Any]:
        headers, keys = _complete_schema_contract(headers, keys)
        # v5.77.3: schema-count telemetry. Gemini v5.77.2 audit
        # suggested logging when the finalized payload's header/key
        # count drifts from the canonical 107 — this helps catch
        # schema-mismatch bugs (e.g., external schema_registry returning
        # 106 columns while the engine expects 107) at the engine
        # boundary rather than after the dashboard renders a broken
        # row. Logs WARNING when counts drift on instrument sheets;
        # special sheets (Insights_Analysis, Data_Dictionary) have
        # their own schemas and are correctly excluded.
        #
        # v5.77.4: ChatGPT v5.77.3 audit caught that the v5.77.3 filter
        # used placeholder names ("instruments", "stocks", "") that
        # never match TFB's actual sheet names (Market_Leaders,
        # Global_Markets, etc.), so the telemetry never fired in
        # production. Fixed by routing through the existing
        # _canonicalize_sheet_name() helper and checking membership
        # in the existing INSTRUMENT_SHEETS set. This is the correct
        # predicate and was already used elsewhere in this module
        # (e.g., recovery and emergency-symbol paths).
        try:
            canonical_count = len(INSTRUMENT_CANONICAL_KEYS)
            actual_count = len(keys)
            if actual_count != canonical_count:
                canonical_sheet = _canonicalize_sheet_name(sheet)
                if canonical_sheet in INSTRUMENT_SHEETS:
                    logger.warning(
                        "[engine_v2 v%s] _finalize_payload schema drift: sheet=%s (canonical=%s) expected=%d got=%d",
                        __version__, sheet or "(unset)", canonical_sheet,
                        canonical_count, actual_count,
                    )
        except Exception:
            pass
        dict_rows = [_strict_project_row(keys, r) for r in (row_objects or [])]
        display_row_objects = _rows_display_objects_from_rows(dict_rows, headers, keys)
        matrix_rows = _rows_matrix_from_rows(dict_rows, keys) if include_matrix else []

        payload = {
            "status": status,
            "sheet": sheet,
            "page": sheet,
            "sheet_name": sheet,
            "headers": headers,
            "display_headers": headers,
            "sheet_headers": headers,
            "column_headers": headers,
            "keys": keys,
            "columns": keys,
            "fields": keys,
            "rows": matrix_rows,
            "rows_matrix": matrix_rows,
            "row_objects": dict_rows,
            "items": dict_rows,
            "records": dict_rows,
            "data": dict_rows,
            "quotes": dict_rows,
            "display_row_objects": display_row_objects,
            "display_items": display_row_objects,
            "display_records": display_row_objects,
            "rows_dict_display": display_row_objects,
            "count": len(dict_rows),
            "meta": dict(meta or {}),
            "version": self.version,
        }
        if error is not None:
            payload["error"] = error
        return _json_safe(payload)

    async def _init_rows_reader(self) -> Tuple[Any, str]:
        if self._rows_reader_ready:
            return self._rows_reader_obj, self._rows_reader_source

        async with self._rows_reader_lock:
            if self._rows_reader_ready:
                return self._rows_reader_obj, self._rows_reader_source

            obj: Any = None
            source = ""
            for mod_path in (
                "integrations.google_sheets_service",
                "core.integrations.google_sheets_service",
                "google_sheets_service",
                "core.google_sheets_service",
                "integrations.symbols_reader",
                "core.integrations.symbols_reader",
            ):
                try:
                    mod = import_module(mod_path)
                except Exception:
                    continue

                if any(
                    callable(getattr(mod, nm, None))
                    for nm in ("get_rows_for_sheet", "read_rows_for_sheet", "get_sheet_rows", "fetch_sheet_rows", "sheet_rows", "get_rows")
                ):
                    obj = mod
                    source = mod_path
                    break

                for attr_name in ("service", "reader", "rows_reader", "google_sheets_service"):
                    candidate = getattr(mod, attr_name, None)
                    if candidate is not None:
                        obj = candidate
                        source = f"{mod_path}.{attr_name}"
                        break

                if obj is not None:
                    break

            self._rows_reader_obj = obj
            self._rows_reader_source = source
            self._rows_reader_ready = True
            return obj, source

    async def _call_rows_reader(self, obj: Any, sheet: str, limit: int) -> List[Dict[str, Any]]:
        if obj is None:
            return []

        for name in ("get_rows_for_sheet", "read_rows_for_sheet", "get_sheet_rows", "fetch_sheet_rows", "sheet_rows", "get_rows"):
            fn = getattr(obj, name, None)
            if not callable(fn):
                continue

            variants = [
                ((), {"sheet": sheet, "limit": limit}),
                ((), {"sheet_name": sheet, "limit": limit}),
                ((), {"page": sheet, "limit": limit}),
                ((sheet,), {"limit": limit}),
                ((sheet,), {}),
            ]
            for args, kwargs in variants:
                try:
                    async with asyncio.timeout(_get_env_float("ROWS_READER_TIMEOUT_SECONDS", 20.0)):
                        result = await _call_maybe_async(fn, *args, **kwargs)
                    rows = _coerce_rows_list(result)
                    if rows:
                        return rows[:limit]
                except TypeError:
                    continue
                except Exception as exc:
                    # v5.75.0: observable swallow.
                    logger.debug(
                        "[engine_v2 v%s] rows reader %s.%s failed for sheet=%s: %s: %s",
                        __version__, type(obj).__name__, name, sheet,
                        type(exc).__name__, str(exc)[:200],
                    )
                    continue

        return []

    async def _get_rows_from_external_reader(self, sheet: str, limit: int) -> List[Dict[str, Any]]:
        obj, _src = await self._init_rows_reader()
        if obj is None:
            return []
        return await self._call_rows_reader(obj, sheet, limit)

    async def _init_symbols_reader(self) -> Tuple[Any, str]:
        if self._symbols_reader_ready:
            return self._symbols_reader_obj, self._symbols_reader_source

        async with self._symbols_reader_lock:
            if self._symbols_reader_ready:
                return self._symbols_reader_obj, self._symbols_reader_source

            obj: Any = None
            source = ""
            for mod_path in (
                "symbols_reader",
                "core.symbols_reader",
                "integrations.symbols_reader",
                "core.integrations.symbols_reader",
                "integrations.google_sheets_service",
                "core.integrations.google_sheets_service",
                "google_sheets_service",
                "core.google_sheets_service",
            ):
                try:
                    mod = import_module(mod_path)
                except Exception:
                    continue

                if any(
                    callable(getattr(mod, nm, None))
                    for nm in (
                        "get_symbols_for_sheet",
                        "read_symbols_for_sheet",
                        "get_sheet_symbols",
                        "get_symbols",
                        "list_symbols_for_page",
                        "get_symbols_for_page",
                        "read_symbols",
                        "load_symbols",
                        "read_sheet_symbols",
                    )
                ):
                    obj = mod
                    source = mod_path
                    break

                for attr_name in ("symbols_reader", "reader", "symbol_reader", "sheet_reader", "service"):
                    candidate = getattr(mod, attr_name, None)
                    if candidate is not None:
                        obj = candidate
                        source = f"{mod_path}.{attr_name}"
                        break

                if obj is not None:
                    break

            self._symbols_reader_obj = obj
            self._symbols_reader_source = source
            self._symbols_reader_ready = True
            return obj, source

    async def _call_symbols_reader(self, obj: Any, sheet: str, limit: int) -> List[str]:
        if obj is None:
            return []

        if isinstance(obj, dict):
            for key in _sheet_lookup_candidates(sheet):
                vals = obj.get(key)
                syms = _normalize_symbol_list(_split_symbols(vals), limit=limit)
                if syms:
                    return syms

        for name in (
            "get_symbols_for_sheet",
            "read_symbols_for_sheet",
            "get_sheet_symbols",
            "get_symbols_for_page",
            "list_symbols_for_page",
            "get_symbols",
            "list_symbols",
            "read_symbols",
            "load_symbols",
            "read_sheet_symbols",
        ):
            fn = getattr(obj, name, None)
            if not callable(fn):
                continue

            variants = [
                ((), {"sheet": sheet, "limit": limit}),
                ((), {"sheet_name": sheet, "limit": limit}),
                ((), {"page": sheet, "limit": limit}),
                ((sheet,), {"limit": limit}),
                ((sheet,), {}),
            ]
            for args, kwargs in variants:
                try:
                    async with asyncio.timeout(_get_env_float("SHEET_SYMBOLS_TIMEOUT_SECONDS", 15.0)):
                        result = await _call_maybe_async(fn, *args, **kwargs)
                    syms = _normalize_symbol_list(_split_symbols(result), limit=limit)
                    if not syms and isinstance(result, (dict, list)):
                        syms = _extract_symbols_from_rows(_coerce_rows_list(result), limit=limit)
                    if syms:
                        return syms
                except TypeError:
                    continue
                except Exception as exc:
                    # v5.75.0: observable swallow.
                    logger.debug(
                        "[engine_v2 v%s] symbols reader %s.%s failed for sheet=%s: %s: %s",
                        __version__, type(obj).__name__, name, sheet,
                        type(exc).__name__, str(exc)[:200],
                    )
                    continue

        return []

    async def _get_symbols_from_env(self, sheet: str, limit: int) -> List[str]:
        env_candidates: List[str] = []
        specific = PAGE_SYMBOL_ENV_KEYS.get(sheet)
        if specific:
            env_candidates.append(specific)

        for cand in _sheet_lookup_candidates(sheet):
            token = re.sub(r"[^A-Za-z0-9]+", "_", cand).strip("_").upper()
            if token:
                env_candidates.extend([f"{token}_SYMBOLS", f"{token}_TICKERS", f"{token}_CODES"])

        env_candidates.extend(["TOP10_FALLBACK_SYMBOLS", "DEFAULT_PAGE_SYMBOLS", "DEFAULT_SYMBOLS"])

        seen: Set[str] = set()
        for env_key in env_candidates:
            if not env_key or env_key in seen:
                continue
            seen.add(env_key)
            raw = os.getenv(env_key, "") or ""
            if raw.strip():
                syms = _normalize_symbol_list(_split_symbols(raw), limit=limit)
                if syms:
                    return syms
        return []

    async def _get_symbols_from_settings(self, sheet: str, limit: int) -> List[str]:
        if self.settings is None:
            return []
        candidates = _sheet_lookup_candidates(sheet)

        for attr_name in (
            f"{sheet.lower()}_symbols",
            f"{sheet.lower()}_tickers",
            f"{sheet.lower()}_codes",
            "default_symbols",
            "page_symbols",
            "sheet_symbols",
        ):
            try:
                raw = getattr(self.settings, attr_name, None)
            except Exception:
                raw = None

            if isinstance(raw, dict):
                for cand in candidates:
                    vals = raw.get(cand)
                    syms = _normalize_symbol_list(_split_symbols(vals), limit=limit)
                    if syms:
                        return syms
            elif raw:
                syms = _normalize_symbol_list(_split_symbols(raw), limit=limit)
                if syms:
                    return syms

        return []

    async def _get_symbols_from_page_catalog(self, sheet: str, limit: int) -> List[str]:
        candidates = _sheet_lookup_candidates(sheet)

        for mod_path in ("core.sheets.page_catalog", "sheets.page_catalog"):
            try:
                mod = import_module(mod_path)
            except Exception:
                continue

            for attr_name in ("PAGE_SYMBOLS", "SHEET_SYMBOLS", "DEFAULT_PAGE_SYMBOLS", "PAGE_DEFAULT_SYMBOLS"):
                mapping = getattr(mod, attr_name, None)
                if isinstance(mapping, dict):
                    for cand in candidates:
                        vals = mapping.get(cand)
                        syms = _normalize_symbol_list(_split_symbols(vals), limit=limit)
                        if syms:
                            return syms

            for fn_name in ("get_default_symbols", "get_page_symbols", "get_symbols_for_page"):
                fn = getattr(mod, fn_name, None)
                if not callable(fn):
                    continue
                for args, kwargs in [
                    ((sheet,), {"limit": limit}),
                    ((sheet,), {}),
                    ((), {"page": sheet, "limit": limit}),
                    ((), {"sheet": sheet, "limit": limit}),
                ]:
                    try:
                        result = await _call_maybe_async(fn, *args, **kwargs)
                        syms = _normalize_symbol_list(_split_symbols(result), limit=limit)
                        if syms:
                            return syms
                    except TypeError:
                        continue
                    except Exception:
                        continue

        return []

    async def _get_symbols_for_sheet_impl(self, sheet: str, limit: int = 5000, body: Optional[Dict[str, Any]] = None) -> List[str]:
        target = _canonicalize_sheet_name(sheet)
        if target in SPECIAL_SHEETS:
            self._set_sheet_symbols_meta(target, "special_sheet", 0)
            return []

        limit = max(1, min(5000, int(limit or 5000)))
        from_body = _extract_requested_symbols_from_body(body, limit=limit)
        if from_body:
            self._set_sheet_symbols_meta(target, "body_symbols", len(from_body))
            return from_body

        cached = await self._symbols_cache.get(sheet=target, limit=limit)
        if isinstance(cached, list) and cached:
            syms = _normalize_symbol_list(cached, limit=limit)
            self._set_sheet_symbols_meta(target, "symbols_cache", len(syms))
            return syms

        obj, src = await self._init_symbols_reader()
        if obj is not None:
            syms = await self._call_symbols_reader(obj, target, limit=limit)
            if syms:
                self._set_sheet_symbols_meta(target, f"symbols_reader:{src or 'unknown'}", len(syms))
                await self._symbols_cache.set(syms, sheet=target, limit=limit)
                return syms

        syms = await self._get_symbols_from_page_catalog(target, limit=limit)
        if syms:
            self._set_sheet_symbols_meta(target, "page_catalog", len(syms))
            await self._symbols_cache.set(syms, sheet=target, limit=limit)
            return syms

        syms = await self._get_symbols_from_env(target, limit=limit)
        if syms:
            self._set_sheet_symbols_meta(target, "env", len(syms))
            await self._symbols_cache.set(syms, sheet=target, limit=limit)
            return syms

        syms = await self._get_symbols_from_settings(target, limit=limit)
        if syms:
            self._set_sheet_symbols_meta(target, "settings", len(syms))
            await self._symbols_cache.set(syms, sheet=target, limit=limit)
            return syms

        snap = self.get_cached_sheet_snapshot(sheet=target)
        snap_rows = _coerce_rows_list(snap)
        if snap_rows:
            syms = _extract_symbols_from_rows(snap_rows, limit=limit)
            if syms:
                self._set_sheet_symbols_meta(target, "snapshot_rows", len(syms))
                await self._symbols_cache.set(syms, sheet=target, limit=limit)
                return syms

        emergency = EMERGENCY_PAGE_SYMBOLS.get(target) or []
        if emergency:
            syms = _normalize_symbol_list(emergency, limit=limit)
            self._set_sheet_symbols_meta(target, "emergency_page_symbols", len(syms), note="last_resort_fallback")
            await self._symbols_cache.set(syms, sheet=target, limit=limit)
            return syms

        self._set_sheet_symbols_meta(target, "none", 0, note=(src or "no_source"))
        return []

    async def get_sheet_symbols(
        self,
        sheet: Optional[str] = None,
        *,
        sheet_name: Optional[str] = None,
        page: Optional[str] = None,
        limit: int = 5000,
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[str]:
        target_sheet, effective_limit, _offset, _mode, normalized_body, _request_parts = _normalize_route_call_inputs(
            page=page,
            sheet=sheet,
            sheet_name=sheet_name,
            limit=limit,
            offset=0,
            mode="",
            body=body,
            extras=kwargs,
        )
        return await self._get_symbols_for_sheet_impl(target_sheet, limit=effective_limit, body=normalized_body)

    async def get_page_symbols(
        self,
        page: Optional[str] = None,
        *,
        sheet: Optional[str] = None,
        sheet_name: Optional[str] = None,
        limit: int = 5000,
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[str]:
        target_sheet, effective_limit, _offset, _mode, normalized_body, _request_parts = _normalize_route_call_inputs(
            page=page,
            sheet=sheet,
            sheet_name=sheet_name,
            limit=limit,
            offset=0,
            mode="",
            body=body,
            extras=kwargs,
        )
        return await self._get_symbols_for_sheet_impl(target_sheet, limit=effective_limit, body=normalized_body)

    async def list_symbols_for_page(self, page: str, *, limit: int = 5000, body: Optional[Dict[str, Any]] = None, **kwargs: Any) -> List[str]:
        target_sheet, effective_limit, _offset, _mode, normalized_body, _request_parts = _normalize_route_call_inputs(
            page=page,
            limit=limit,
            offset=0,
            mode="",
            body=body,
            extras=kwargs,
        )
        return await self._get_symbols_for_sheet_impl(target_sheet, limit=effective_limit, body=normalized_body)

    async def list_symbols(
        self,
        sheet: Optional[str] = None,
        *,
        page: Optional[str] = None,
        sheet_name: Optional[str] = None,
        limit: int = 5000,
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[str]:
        target_sheet, effective_limit, _offset, _mode, normalized_body, _request_parts = _normalize_route_call_inputs(
            page=page,
            sheet=sheet,
            sheet_name=sheet_name,
            limit=limit,
            offset=0,
            mode="",
            body=body,
            extras=kwargs,
        )
        return await self._get_symbols_for_sheet_impl(target_sheet, limit=effective_limit, body=normalized_body)

    async def get_symbols(
        self,
        sheet: Optional[str] = None,
        *,
        page: Optional[str] = None,
        sheet_name: Optional[str] = None,
        limit: int = 5000,
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[str]:
        target_sheet, effective_limit, _offset, _mode, normalized_body, _request_parts = _normalize_route_call_inputs(
            page=page,
            sheet=sheet,
            sheet_name=sheet_name,
            limit=limit,
            offset=0,
            mode="",
            body=body,
            extras=kwargs,
        )
        return await self._get_symbols_for_sheet_impl(target_sheet, limit=effective_limit, body=normalized_body)

    def _resolve_quote_page_context(
        self,
        *,
        page: Optional[str] = None,
        sheet: Optional[str] = None,
        body: Optional[Dict[str, Any]] = None,
        schema: Any = None,
        extras: Optional[Dict[str, Any]] = None,
    ) -> str:
        merged = _merge_route_body_dicts(body, extras or {})
        target_raw = (
            page
            or sheet
            or _safe_str(merged.get("page"))
            or _safe_str(merged.get("sheet"))
            or _safe_str(merged.get("sheet_name"))
            or _safe_str(merged.get("page_name"))
            or _safe_str(merged.get("name"))
            or _safe_str(merged.get("tab"))
            or _safe_str(merged.get("worksheet"))
        )
        if not target_raw and isinstance(schema, str):
            target_raw = schema
        return _canonicalize_sheet_name(target_raw) if target_raw else ""

    def _page_primary_provider_for(self, symbol: str, page: str = "") -> str:
        info = get_symbol_info(symbol)
        is_ksa_sym = bool(info.get("is_ksa"))
        page_ctx = _canonicalize_sheet_name(page) if page else ""
        if (
            not is_ksa_sym
            and page_ctx
            and page_ctx in self.page_primary_providers
        ):
            candidate = _safe_str(self.page_primary_providers.get(page_ctx), self.non_ksa_primary_provider).lower()
            if candidate:
                return candidate
        return self.primary_provider

    def _provider_profile_key(self, symbol: str, page: str = "") -> str:
        info = get_symbol_info(symbol)
        page_ctx = _canonicalize_sheet_name(page) if page else ""
        primary = self._page_primary_provider_for(symbol, page_ctx)
        market = "ksa" if bool(info.get("is_ksa")) else "global"
        return f"{market}|{page_ctx or 'default'}|{primary or 'none'}"

    async def _fetch_patch(self, provider: str, symbol: str) -> Tuple[str, Optional[Dict[str, Any]], float, Optional[str]]:
        start = time.time()
        async with self._sem:
            module, stats = await self._registry.get_provider(provider)
            if getattr(stats, "is_circuit_open", False):
                return provider, None, 0.0, "circuit_open"

            if module is None:
                err = getattr(stats, "last_import_error", "provider module missing")
                await self._registry.record_failure(provider, err)
                return provider, None, (time.time() - start) * 1000.0, err

            fn = _pick_provider_callable(module, provider)
            if fn is None:
                err = f"no callable fetch function for provider '{provider}'"
                await self._registry.record_failure(provider, err)
                return provider, None, (time.time() - start) * 1000.0, err

            provider_symbol = _provider_symbol_for(provider, symbol)
            symbols_to_try = [provider_symbol]
            if symbol and symbol not in symbols_to_try:
                symbols_to_try.append(symbol)

            call_variants = []
            for _sym in symbols_to_try:
                call_variants.extend([
                    ((_sym,), {}),
                    ((), {"symbol": _sym}),
                    ((), {"ticker": _sym}),
                    ((), {"requested_symbol": _sym}),
                    ((_sym,), {"settings": self.settings}),
                    ((), {"symbol": _sym, "settings": self.settings}),
                ])

            result = None
            collected_errs: List[str] = []
            for args, kwargs in call_variants:
                try:
                    async with asyncio.timeout(self.request_timeout):
                        result = await _call_maybe_async(fn, *args, **kwargs)
                    break
                except TypeError:
                    continue
                except Exception as exc:
                    # v5.75.0: surface swallowed provider exceptions at DEBUG
                    # so failures stay non-fatal but observable.
                    logger.debug(
                        "[engine_v2 v%s] provider %s call failed for %s "
                        "(args=%r kwargs=%r): %s: %s",
                        __version__, provider, symbol, args,
                        {k: v for k, v in kwargs.items() if k != "settings"},
                        type(exc).__name__, str(exc)[:200],
                    )
                    collected_errs.append(f"{type(exc).__name__}: {str(exc)[:120]}")
                    continue

            latency = (time.time() - start) * 1000.0
            patch = _model_to_dict(result)
            if patch and isinstance(patch, dict):
                await self._registry.record_success(provider, latency)
                return provider, patch, latency, None

            err = " | ".join(collected_errs) if collected_errs else "non_dict_or_empty"
            await self._registry.record_failure(provider, err)
            return provider, None, latency, err

    def _providers_for(self, symbol: str, page: str = "") -> List[str]:
        info = get_symbol_info(symbol)
        is_ksa_sym = bool(info.get("is_ksa"))
        page_ctx = _canonicalize_sheet_name(page) if page else ""

        def _provider_allowed(provider: str) -> bool:
            provider = _safe_str(provider).lower()
            if not provider or provider not in self.enabled_providers:
                return False
            if is_ksa_sym and self.ksa_disallow_eodhd and provider == "eodhd":
                return False
            return True

        providers = [p for p in (self.ksa_providers if is_ksa_sym else self.global_providers) if _provider_allowed(p)]

        primary_provider = self._page_primary_provider_for(symbol, page_ctx)
        if primary_provider and _provider_allowed(primary_provider):
            if primary_provider in providers:
                providers = [p for p in providers if p != primary_provider]
            providers.insert(0, primary_provider)

        if (not is_ksa_sym) and page_ctx and page_ctx in self.page_primary_providers and _provider_allowed("eodhd"):
            providers = [p for p in providers if p != "eodhd"]
            providers.insert(0, "eodhd")

        seen: Set[str] = set()
        out: List[str] = []
        for p in providers:
            p2 = _safe_str(p).lower()
            if not p2 or p2 in seen:
                continue
            seen.add(p2)
            out.append(p2)
        return out

    def _rows_from_parallel_series(
        self,
        timestamps: Sequence[Any],
        opens: Optional[Sequence[Any]] = None,
        highs: Optional[Sequence[Any]] = None,
        lows: Optional[Sequence[Any]] = None,
        closes: Optional[Sequence[Any]] = None,
        volumes: Optional[Sequence[Any]] = None,
        adjcloses: Optional[Sequence[Any]] = None,
    ) -> List[Dict[str, Any]]:
        ts_list = list(timestamps or [])
        if not ts_list:
            return []

        rows: List[Dict[str, Any]] = []
        for idx, ts in enumerate(ts_list):
            row = {
                "timestamp": ts,
                "open": opens[idx] if opens is not None and idx < len(opens) else None,
                "high": highs[idx] if highs is not None and idx < len(highs) else None,
                "low": lows[idx] if lows is not None and idx < len(lows) else None,
                "close": closes[idx] if closes is not None and idx < len(closes) else None,
                "volume": volumes[idx] if volumes is not None and idx < len(volumes) else None,
                "adjclose": adjcloses[idx] if adjcloses is not None and idx < len(adjcloses) else None,
            }
            if any(v is not None for k, v in row.items() if k != "timestamp"):
                rows.append(row)
        return rows

    def _coerce_history_rows(self, result: Any) -> List[Dict[str, Any]]:
        if result is None:
            return []
        if hasattr(result, "to_dict") and callable(getattr(result, "to_dict")):
            try:
                rows = result.to_dict("records")
                if isinstance(rows, list):
                    return [dict(r) for r in rows if isinstance(r, dict)]
            except Exception:
                pass
        if isinstance(result, list):
            out: List[Dict[str, Any]] = []
            for item in result:
                if isinstance(item, dict):
                    if {"open", "high", "low", "close"} & set(item.keys()) and not isinstance(item.get("open"), list):
                        out.append(dict(item))
                    else:
                        nested = self._coerce_history_rows(item)
                        if nested:
                            out.extend(nested)
                        else:
                            out.append(dict(item))
                elif isinstance(item, (list, tuple)) and len(item) >= 5:
                    out.append({
                        "timestamp": item[0],
                        "open": item[1],
                        "high": item[2],
                        "low": item[3],
                        "close": item[4],
                        "volume": item[5] if len(item) > 5 else None,
                    })
            return out
        if isinstance(result, dict):
            if isinstance(result.get("chart"), Mapping):
                chart = result.get("chart") or {}
                nested = self._coerce_history_rows(chart.get("result"))
                if nested:
                    return nested

            if isinstance(result.get("result"), list):
                for item in result.get("result") or []:
                    nested = self._coerce_history_rows(item)
                    if nested:
                        return nested

            timestamps = result.get("timestamp") or result.get("timestamps") or result.get("time")
            if isinstance(timestamps, list) and timestamps:
                indicators = result.get("indicators") if isinstance(result.get("indicators"), Mapping) else {}
                quote = None
                if isinstance(indicators.get("quote"), list) and indicators.get("quote"):
                    quote = indicators.get("quote")[0]
                adj = None
                if isinstance(indicators.get("adjclose"), list) and indicators.get("adjclose"):
                    adj = indicators.get("adjclose")[0]
                if isinstance(quote, Mapping):
                    rows = self._rows_from_parallel_series(
                        timestamps=timestamps,
                        opens=list(quote.get("open") or []),
                        highs=list(quote.get("high") or []),
                        lows=list(quote.get("low") or []),
                        closes=list(quote.get("close") or []),
                        volumes=list(quote.get("volume") or []),
                        adjcloses=list(adj.get("adjclose") or []) if isinstance(adj, Mapping) else None,
                    )
                    if rows:
                        return rows

            if any(isinstance(result.get(k), list) for k in ("close", "open", "high", "low", "volume")):
                ts = result.get("timestamp") or list(range(len(result.get("close") or result.get("price") or [])))
                rows = self._rows_from_parallel_series(
                    timestamps=list(ts or []),
                    opens=list(result.get("open") or []),
                    highs=list(result.get("high") or []),
                    lows=list(result.get("low") or []),
                    closes=list(result.get("close") or result.get("adjclose") or result.get("price") or result.get("value") or []),
                    volumes=list(result.get("volume") or []),
                    adjcloses=list(result.get("adjclose") or []),
                )
                if rows:
                    return rows

            for key in ("Time Series (Daily)", "time_series", "series"):
                series = result.get(key)
                if isinstance(series, Mapping):
                    rows: List[Dict[str, Any]] = []
                    for ts, entry in series.items():
                        if not isinstance(entry, Mapping):
                            continue
                        rows.append({
                            "timestamp": ts,
                            "open": entry.get("1. open") or entry.get("open"),
                            "high": entry.get("2. high") or entry.get("high"),
                            "low": entry.get("3. low") or entry.get("low"),
                            "close": entry.get("4. close") or entry.get("close"),
                            "volume": entry.get("5. volume") or entry.get("volume"),
                        })
                    if rows:
                        rows.sort(key=lambda r: _safe_str(r.get("timestamp")))
                        return rows

            for key in ("history", "bars", "candles", "prices", "data", "items", "rows", "chart"):
                if key in result:
                    nested = self._coerce_history_rows(result.get(key))
                    if nested:
                        return nested
            if {"open", "high", "low", "close"} & set(result.keys()):
                return [dict(result)]
        return []

    def _safe_mean(self, values: List[float]) -> float:
        return sum(values) / len(values) if values else 0.0

    def _safe_std(self, values: List[float]) -> float:
        if len(values) < 2:
            return 0.0
        mean = self._safe_mean(values)
        var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
        return math.sqrt(max(var, 0.0))

    def _quantile(self, values: List[float], q: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        pos = max(0.0, min(1.0, q)) * (len(ordered) - 1)
        lo = int(math.floor(pos))
        hi = int(math.ceil(pos))
        if lo == hi:
            return ordered[lo]
        frac = pos - lo
        return ordered[lo] + (ordered[hi] - ordered[lo]) * frac

    def _compute_history_patch_from_rows(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        closes: List[float] = []
        highs: List[float] = []
        lows: List[float] = []
        volumes: List[float] = []
        opens: List[float] = []
        for row in rows or []:
            close = _as_float(row.get("close") or row.get("adjclose") or row.get("price") or row.get("value"))
            high = _as_float(row.get("high") or row.get("day_high"))
            low = _as_float(row.get("low") or row.get("day_low"))
            vol = _as_float(row.get("volume"))
            opn = _as_float(row.get("open") or row.get("open_price"))
            if close is not None:
                closes.append(close)
            if high is not None:
                highs.append(high)
            if low is not None:
                lows.append(low)
            if vol is not None:
                volumes.append(vol)
            if opn is not None:
                opens.append(opn)
        candle_fields: Dict[str, Any] = {}
        try:
            candle_fields = detect_candlestick_patterns(rows or [])
        except Exception as cs_err:
            logger.debug(
                "[engine_v2 v%s] PHASE-JJ candlestick detection failed: %s: %s",
                __version__, cs_err.__class__.__name__, cs_err,
            )
        if len(closes) < 2:
            return candle_fields if candle_fields else {}
        returns = []
        for prev, cur in zip(closes[:-1], closes[1:]):
            if prev not in (None, 0):
                returns.append((cur / prev) - 1.0)
        if not returns:
            return {}
        recent14 = closes[-15:]
        gains: List[float] = []
        losses: List[float] = []
        for prev, cur in zip(recent14[:-1], recent14[1:]):
            delta = cur - prev
            if delta >= 0:
                gains.append(delta)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(abs(delta))
        avg_gain = self._safe_mean(gains)
        avg_loss = self._safe_mean(losses)
        rsi = None
        if gains and losses:
            if avg_loss == 0:
                rsi = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi = 100.0 - (100.0 / (1.0 + rs))
        last30 = returns[-30:] if len(returns) >= 30 else returns
        last90 = returns[-90:] if len(returns) >= 90 else returns
        vol30 = self._safe_std(last30) * math.sqrt(252.0) if last30 else None
        vol90 = self._safe_std(last90) * math.sqrt(252.0) if last90 else None
        mean_daily = self._safe_mean(last90)
        std_daily = self._safe_std(last90)
        sharpe = (mean_daily / std_daily) * math.sqrt(252.0) if std_daily not in (None, 0.0) else None
        var95 = abs(self._quantile(last90, 0.05)) if last90 else None
        peak = closes[0]
        max_dd = 0.0
        for price in closes:
            peak = max(peak, price)
            if peak > 0:
                dd = (price / peak) - 1.0
                max_dd = min(max_dd, dd)
        patch: Dict[str, Any] = {
            "current_price": closes[-1],
            "previous_close": closes[-2],
            "open_price": opens[-1] if opens else None,
            "day_high": highs[-1] if highs else None,
            "day_low": lows[-1] if lows else None,
            "week_52_high": max(highs) if highs else max(closes),
            "week_52_low": min(lows) if lows else min(closes),
            "avg_volume_10d": self._safe_mean(volumes[-10:]) if volumes else None,
            "avg_volume_30d": self._safe_mean(volumes[-30:]) if volumes else None,
            "volatility_30d": vol30,
            "volatility_90d": vol90,
            "max_drawdown_1y": max_dd,
            "var_95_1d": var95 if var95 is not None else None,
            "sharpe_1y": sharpe,
            "rsi_14": rsi,
            "price_change": closes[-1] - closes[-2],
            "percent_change": ((closes[-1] - closes[-2]) / closes[-2]) if closes[-2] not in (None, 0) else None,
            "volume": volumes[-1] if volumes else None,
        }
        if patch.get("current_price") is not None and patch.get("week_52_high") is not None and patch.get("week_52_low") is not None:
            hi = _as_float(patch.get("week_52_high"))
            lo = _as_float(patch.get("week_52_low"))
            cp = _as_float(patch.get("current_price"))
            if hi is not None and lo is not None and cp is not None and hi > lo:
                patch["week_52_position_pct"] = ((cp - lo) / (hi - lo)) * 100.0
        result_patch = {k: v for k, v in patch.items() if v is not None}
        if candle_fields:
            for k, v in candle_fields.items():
                if v not in (None, "", 0.0):
                    result_patch[k] = v
                elif k not in result_patch:
                    result_patch[k] = v
        return result_patch

    async def _fetch_history_patch(self, provider: str, symbol: str) -> Dict[str, Any]:
        module, _stats = await self._registry.get_provider(provider)
        if module is None:
            return {}
        callables = []
        for name in (
            "get_history", "fetch_history", "get_price_history", "fetch_price_history", "history",
            "get_chart", "fetch_chart", "get_chart_history", "fetch_chart_history",
            "get_historical_data", "fetch_historical_data", "get_history_rows", "fetch_history_rows",
            "get_timeseries", "fetch_timeseries", "get_series", "fetch_series", "get_ohlcv", "fetch_ohlcv",
        ):
            fn = getattr(module, name, None)
            if callable(fn):
                callables.append(fn)
        if not callables:
            return {}
        provider_symbol = _provider_symbol_for(provider, symbol)
        symbols_to_try = [provider_symbol]
        if symbol and symbol not in symbols_to_try:
            symbols_to_try.append(symbol)

        variants = []
        for _sym in symbols_to_try:
            variants.extend([
                ((_sym,), {"period": "1y", "interval": "1d"}),
                ((_sym,), {"range": "1y", "interval": "1d"}),
                ((_sym,), {"lookback": "1y", "interval": "1d"}),
                ((), {"symbol": _sym, "period": "1y", "interval": "1d"}),
                ((), {"ticker": _sym, "period": "1y", "interval": "1d"}),
                ((), {"code": _sym, "period": "1y", "interval": "1d"}),
                ((), {"symbol": _sym, "range": "1y", "interval": "1d"}),
                ((), {"ticker": _sym, "range": "1y", "interval": "1d"}),
                ((_sym,), {}),
            ])
        for fn in callables:
            for args, kwargs in variants:
                try:
                    async with asyncio.timeout(max(5.0, self.request_timeout)):
                        result = await _call_maybe_async(fn, *args, **kwargs)
                    rows = self._coerce_history_rows(result)
                    patch = self._compute_history_patch_from_rows(rows)
                    if patch:
                        patch["data_provider"] = provider
                        return patch
                except TypeError:
                    continue
                except Exception:
                    continue
        return {}

    async def _get_history_patch_best_effort(self, symbol: str, providers: Sequence[str], page: str = "") -> Dict[str, Any]:
        candidates: List[str] = []
        for provider in list(providers or []):
            if provider and provider not in candidates:
                candidates.append(provider)

        page_ctx = _canonicalize_sheet_name(page) if page else ""
        preferred_history = list(self.history_fallback_providers or [])
        primary_provider = self._page_primary_provider_for(symbol, page_ctx)

        if (not get_symbol_info(symbol).get("is_ksa")) and page_ctx and page_ctx in self.page_primary_providers:
            preferred_history = [primary_provider, "yahoo_chart", "yahoo", "eodhd", "finnhub"] + preferred_history
        elif symbol.endswith("=F") or symbol.endswith("=X"):
            preferred_history = ["yahoo_chart", "yahoo", "eodhd", "finnhub"] + preferred_history

        for provider in preferred_history:
            provider = _safe_str(provider).lower()
            if provider and provider not in candidates and (provider in self.enabled_providers or provider in preferred_history):
                candidates.append(provider)

        for provider in candidates:
            patch = await self._fetch_history_patch(provider, symbol)
            if patch:
                return patch
        return {}

    def _merge(self, requested_symbol: str, norm: str, patches: List[Tuple[str, Dict[str, Any], float]]) -> Dict[str, Any]:
        merged: Dict[str, Any] = {
            "symbol": norm,
            "symbol_normalized": norm,
            "requested_symbol": requested_symbol,
            "last_updated_utc": _now_utc_iso(),
            "last_updated_riyadh": _now_riyadh_iso(),
            "data_sources": [],
            "provider_latency": {},
        }

        protected = {"symbol", "symbol_normalized", "requested_symbol"}
        normalized_patches: List[Tuple[str, Dict[str, Any], float]] = []
        for prov, patch, latency in patches:
            canonical = _canonicalize_provider_row(patch, requested_symbol=requested_symbol, normalized_symbol=norm, provider=prov)
            normalized_patches.append((prov, canonical, latency))

        sorted_patches = sorted(
            normalized_patches,
            key=lambda item: (
                PROVIDER_PRIORITIES.get(item[0], 999),
                -sum(1 for v in item[1].values() if v not in (None, "", [], {})),
            ),
        )

        for prov, patch, latency in sorted_patches:
            merged["data_sources"].append(prov)
            merged["provider_latency"][prov] = round(float(latency or 0.0), 2)
            for k, v in patch.items():
                if k in protected or v is None:
                    continue
                if k not in merged or merged.get(k) in (None, "", [], {}):
                    merged[k] = v

        merged = _canonicalize_provider_row(merged, requested_symbol=requested_symbol, normalized_symbol=norm, provider=_safe_str((merged.get("data_sources") or [""])[0] if isinstance(merged.get("data_sources"), list) else ""))
        return merged

    def _data_quality(self, row: Dict[str, Any]) -> str:
        if _as_float(row.get("current_price")) is None:
            return QuoteQuality.MISSING.value
        return QuoteQuality.GOOD.value if any(row.get(k) is not None for k in ("overall_score", "forecast_price_3m", "pb_ratio")) else QuoteQuality.FAIR.value


    async def _fetch_yahoo_fundamentals_patch(self, symbol: str) -> Optional[Dict[str, Any]]:
        mod = _import_yahoo_provider_module("yahoo_fundamentals_provider")
        if mod is None:
            logger.debug("[engine_v2 v%s] PHASE-Z fundamentals: module not importable", __version__)
            return None

        yahoo_symbol = _yahoo_symbol_for(symbol)
        if not yahoo_symbol:
            return None

        fn = _pick_yahoo_callable(
            mod,
            "get_quote_patch", "fetch_fundamentals_patch", "fetch_enriched_quote_patch",
            "fetch_quote", "get_quote", "quote", "enriched_quote",
        )
        if fn is None:
            logger.debug("[engine_v2 v%s] PHASE-Z fundamentals: no compatible callable", __version__)
            return None

        try:
            if inspect.iscoroutinefunction(fn):
                patch = await fn(yahoo_symbol)
            else:
                # v5.77.2: route through dedicated provider pool via
                # _call_maybe_async instead of asyncio.to_thread's
                # default executor (~36 threads). v5.77.1 left this
                # Yahoo enrichment path bypassing the dedicated pool.
                patch = await _call_maybe_async(fn, yahoo_symbol)
                if inspect.isawaitable(patch):
                    patch = await patch
        except Exception as exc:
            logger.debug(
                "[engine_v2 v%s] PHASE-Z fundamentals call failed for %s (yahoo=%s): %s: %s",
                __version__, symbol, yahoo_symbol, exc.__class__.__name__, exc,
            )
            return None

        if patch is None:
            return None
        if isinstance(patch, dict):
            return dict(patch)
        try:
            if hasattr(patch, "model_dump"):
                d = patch.model_dump(mode="python")
                if isinstance(d, dict):
                    return d
        except Exception:
            pass
        return None

    async def _fetch_yahoo_chart_patch(self, symbol: str) -> Optional[Dict[str, Any]]:
        mod = _import_yahoo_provider_module("yahoo_chart_provider")
        if mod is None:
            logger.debug("[engine_v2 v%s] PHASE-Z chart: module not importable", __version__)
            return None

        yahoo_symbol = _yahoo_symbol_for(symbol)
        if not yahoo_symbol:
            return None

        fn = _pick_yahoo_callable(
            mod,
            "get_quote_patch", "fetch_chart_patch", "fetch_quote", "get_quote", "quote",
        )
        patch: Optional[Dict[str, Any]] = None
        if fn is not None:
            try:
                if inspect.iscoroutinefunction(fn):
                    res = await fn(yahoo_symbol)
                else:
                    # v5.77.2: route through dedicated provider pool.
                    res = await _call_maybe_async(fn, yahoo_symbol)
                    if inspect.isawaitable(res):
                        res = await res
                if res is not None:
                    if isinstance(res, dict):
                        patch = dict(res)
                    elif hasattr(res, "model_dump"):
                        try:
                            d = res.model_dump(mode="python")
                            if isinstance(d, dict):
                                patch = d
                        except Exception:
                            patch = None
            except Exception as exc:
                logger.debug(
                    "[engine_v2 v%s] PHASE-Z chart patch call failed for %s (yahoo=%s): %s: %s",
                    __version__, symbol, yahoo_symbol, exc.__class__.__name__, exc,
                )

        history_needs = (
            patch is None
            or not isinstance(patch, dict)
            or any(_is_missing_or_unknown_field(patch.get(k)) for k in
                   ("rsi_14", "volatility_30d", "volatility_90d",
                    "max_drawdown_1y", "var_95_1d", "sharpe_1y"))
        )
        if history_needs:
            hist_fn = _pick_yahoo_callable(
                mod,
                "get_history_rows", "fetch_history_rows", "get_rows", "get_chart_rows", "fetch_chart_rows",
            )
            rows: List[Dict[str, Any]] = []
            if hist_fn is not None:
                for kwargs in (
                    {"symbol": yahoo_symbol, "interval": "1d", "range": "1y"},
                    {"symbol": yahoo_symbol, "period": "1y"},
                    {"symbol": yahoo_symbol},
                ):
                    try:
                        if inspect.iscoroutinefunction(hist_fn):
                            raw = await hist_fn(**kwargs)
                        else:
                            # v5.77.2: route through dedicated provider pool.
                            raw = await _call_maybe_async(hist_fn, **kwargs)
                            if inspect.isawaitable(raw):
                                raw = await raw
                    except TypeError:
                        continue
                    except Exception as exc:
                        logger.debug(
                            "[engine_v2 v%s] PHASE-Z chart history call failed for %s (yahoo=%s): %s: %s",
                            __version__, symbol, yahoo_symbol, exc.__class__.__name__, exc,
                        )
                        break
                    if isinstance(raw, list) and raw:
                        rows = [r for r in raw if isinstance(r, dict)]
                        if rows:
                            break

            if rows:
                hist_patch = self._compute_history_patch_from_rows(rows)
                if hist_patch:
                    if patch is None:
                        patch = {}
                    for k, v in hist_patch.items():
                        if k not in patch or _is_missing_or_unknown_field(patch.get(k)):
                            patch[k] = v

        return patch if patch else None

    async def _apply_yahoo_enrichment_pass(
        self,
        row: Dict[str, Any],
        normalized: str,
        requested: str,
    ) -> Dict[str, Any]:
        if not _yahoo_enrichment_enabled():
            return row

        needs_fund, needs_chart = _row_needs_yahoo_enrichment(row)
        if not needs_fund and not needs_chart:
            return row

        diag: Dict[str, Any] = {
            "ts": time.time(),
            "symbol": normalized,
            "fundamentals_called": False,
            "chart_called": False,
            "fundamentals_filled_fields": [],
            "chart_filled_fields": [],
        }

        if needs_fund:
            diag["fundamentals_called"] = True
            fund_patch = await self._fetch_yahoo_fundamentals_patch(normalized)
            if fund_patch:
                filtered, filled = _filter_patch_to_missing_fields(
                    row, fund_patch, _YAHOO_FUNDAMENTAL_FIELDS,
                )
                if filled:
                    for k, v in filtered.items():
                        if _is_missing_or_unknown_field(row.get(k)):
                            row[k] = v
                    diag["fundamentals_filled_fields"] = filled
                    sources = row.get("data_sources")
                    if isinstance(sources, list):
                        if "yahoo_fundamentals" not in sources:
                            sources.append("yahoo_fundamentals")
                    else:
                        row["data_sources"] = ["yahoo_fundamentals"]
                    _append_yahoo_warning_tag(
                        row,
                        "yahoo_fundamentals_enrichment_applied:"
                        + ",".join(filled[:8])
                        + ("..." if len(filled) > 8 else ""),
                    )

        if needs_chart:
            diag["chart_called"] = True
            chart_patch = await self._fetch_yahoo_chart_patch(normalized)
            if chart_patch:
                filtered, filled = _filter_patch_to_missing_fields(
                    row, chart_patch, _YAHOO_CHART_FIELDS,
                )
                if filled:
                    for k, v in filtered.items():
                        if _is_missing_or_unknown_field(row.get(k)):
                            row[k] = v
                    diag["chart_filled_fields"] = filled
                    sources = row.get("data_sources")
                    if isinstance(sources, list):
                        if "yahoo_chart" not in sources:
                            sources.append("yahoo_chart")
                    else:
                        row["data_sources"] = ["yahoo_chart"]
                    _append_yahoo_warning_tag(
                        row,
                        "yahoo_chart_enrichment_applied:"
                        + ",".join(filled[:8])
                        + ("..." if len(filled) > 8 else ""),
                    )

        try:
            _YAHOO_ENRICHMENT_LAST_PASS.update(diag)
        except Exception:
            pass

        if diag["fundamentals_filled_fields"] or diag["chart_filled_fields"]:
            logger.info(
                "[engine_v2 v%s] PHASE-Z enrichment for %s: fund=%d chart=%d",
                __version__, normalized,
                len(diag["fundamentals_filled_fields"]),
                len(diag["chart_filled_fields"]),
            )

        return row

    async def _get_enriched_quote_impl(self, symbol: str, use_cache: bool = True, *, page: str = "", sheet: str = "", body: Optional[Dict[str, Any]] = None, **kwargs: Any) -> UnifiedQuote:
        info = get_symbol_info(symbol)
        norm = _safe_str(info.get("normalized"))

        if not norm:
            row = {
                "symbol": _safe_str(symbol),
                "symbol_normalized": None,
                "requested_symbol": _safe_str(symbol),
                "data_quality": QuoteQuality.MISSING.value,
                "error": "Invalid symbol",
                "last_updated_utc": _now_utc_iso(),
                "last_updated_riyadh": _now_riyadh_iso(),
            }
            row = _apply_symbol_context_defaults(row, symbol=_safe_str(symbol))
            _compute_scores_canonical_first(row)
            # v5.75.0: classifier self-clears at entry; redundant explicit
            # clear+compute removed. The classifier still runs on this path
            # via _compute_recommendation below.
            _compute_recommendation(row)
            return UnifiedQuote(**row)

        page_context = self._resolve_quote_page_context(page=page, sheet=sheet, body=body, extras=kwargs)
        provider_profile = self._provider_profile_key(norm, page_context)

        versioned_provider_profile = f"{provider_profile}|sv={_SCHEMA_VERSION}"

        if use_cache:
            cached = await self._cache.get(symbol=norm, provider_profile=versioned_provider_profile)
            if isinstance(cached, dict) and cached:
                return UnifiedQuote(**cached)

        providers = self._providers_for(norm, page=page_context)
        patches_ok: List[Tuple[str, Dict[str, Any], float]] = []
        if providers:
            gathered = await asyncio.gather(*[self._fetch_patch(p, norm) for p in providers[:4]], return_exceptions=True)
            for item in gathered:
                if isinstance(item, tuple) and len(item) == 4:
                    provider, patch, latency, _err = item
                    if patch:
                        patches_ok.append((provider, patch, latency))

        if patches_ok:
            row = self._merge(symbol, norm, patches_ok)
        else:
            row = {
                "symbol": norm,
                "symbol_normalized": norm,
                "requested_symbol": _safe_str(symbol),
                "name": _infer_display_name_from_symbol(norm) or norm,
                "current_price": None,
                "data_sources": [],
                "provider_latency": {},
                "warnings": "No live provider data available",
                "last_updated_utc": _now_utc_iso(),
                "last_updated_riyadh": _now_riyadh_iso(),
            }

        row = _apply_symbol_context_defaults(row, symbol=norm)

        cached_best_row = self._get_best_cached_snapshot_row_for_symbol(norm, prefer_sheet=page_context)
        if cached_best_row:
            row = _merge_missing_fields(row, cached_best_row)
            row = _apply_page_row_backfill(page_context or sheet or "", row)

        missing_history_fields = [
            "current_price", "previous_close", "day_high", "day_low",
            "week_52_high", "week_52_low", "avg_volume_10d", "avg_volume_30d",
            "volatility_30d", "volatility_90d", "max_drawdown_1y",
            "var_95_1d", "sharpe_1y", "rsi_14",
        ]
        if any(row.get(k) in (None, "", [], {}) for k in missing_history_fields):
            hist_patch = await self._get_history_patch_best_effort(norm, providers, page=page_context)
            if hist_patch:
                row = self._merge(symbol, norm, patches_ok + [(hist_patch.get("data_provider") or "history", hist_patch, 0.0)])
                row = _apply_symbol_context_defaults(row, symbol=norm)

        if cached_best_row:
            row = _merge_missing_fields(row, cached_best_row)
            row = _apply_page_row_backfill(page_context or sheet or "", row)

        try:
            row = await self._apply_yahoo_enrichment_pass(row, norm, _safe_str(symbol))
        except Exception as enrich_err:
            logger.debug(
                "[engine_v2 v%s] PHASE-Z enrichment pass failed for %s: %s: %s",
                __version__, norm, enrich_err.__class__.__name__, enrich_err,
            )

        try:
            row = _apply_phase_bb_sanity(row)
        except Exception as bb_err:
            logger.debug(
                "[engine_v2 v%s] PHASE-BB sanity guard failed for %s: %s: %s",
                __version__, norm, bb_err.__class__.__name__, bb_err,
            )

        if _as_float(row.get("current_price")) is not None and _safe_str(row.get("warnings")).lower() == "no live provider data available":
            row["warnings"] = "Recovered from history/chart fallback"
        elif _as_float(row.get("current_price")) is None and not row.get("warnings"):
            row["warnings"] = "No live quote payload and no usable history fallback"

        try:
            row = _apply_phase_dd_enhancements(row)
        except Exception as dd_prepare_err:
            logger.debug(
                "[engine_v2 v%s] PHASE-DD pre-score enhancement failed for %s: %s: %s",
                __version__, norm, dd_prepare_err.__class__.__name__, dd_prepare_err,
            )

        _compute_scores_canonical_first(row)
        # v5.75.0: classifier self-clears at entry; _compute_recommendation
        # is enough on its own. The post-DD invocation below performs the
        # final authoritative classification after Phase-II refresh.
        _compute_recommendation(row)

        try:
            row = _apply_phase_dd_enhancements(row)
        except Exception as dd_err:
            logger.debug(
                "[engine_v2 v%s] PHASE-DD enhancement failed for %s: %s: %s",
                __version__, norm, dd_err.__class__.__name__, dd_err,
            )

        row["data_quality"] = self._data_quality(row)
        row["data_provider"] = row.get("data_provider") or ((row.get("data_sources") or [""])[0] if isinstance(row.get("data_sources"), list) else "")
        q = UnifiedQuote(**row)

        if use_cache:
            await self._cache.set(_model_to_dict(q), symbol=norm, provider_profile=versioned_provider_profile)

        return q

    async def get_enriched_quote(
        self,
        symbol: str,
        use_cache: bool = True,
        *,
        schema: Any = None,
        page: str = "",
        sheet: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> UnifiedQuote:
        page_context = self._resolve_quote_page_context(page=page, sheet=sheet, body=body, schema=schema, extras=kwargs)
        provider_profile = self._provider_profile_key(normalize_symbol(symbol), page_context)
        key = _make_cache_key(
            normalize_symbol(symbol),
            page_context,
            provider_profile,
            _SCHEMA_VERSION,
            mode="cache" if use_cache else "live",
        )
        raw_q = await self._singleflight.execute(
            key,
            lambda: self._get_enriched_quote_impl(symbol, use_cache, page=page_context, body=body, schema=schema, **kwargs),
        )
        if schema is None:
            return raw_q
        row = _model_to_dict(raw_q)
        if isinstance(schema, str):
            _spec, hdrs, keys, _src = _schema_for_sheet(_safe_str(schema))
            projected = _normalize_to_schema_keys(keys, hdrs, row)
            return UnifiedQuote(**projected)
        return raw_q

    async def get_enriched_quote_dict(
        self,
        symbol: str,
        use_cache: bool = True,
        *,
        schema: Any = None,
        page: str = "",
        sheet: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        q = await self.get_enriched_quote(symbol, use_cache=use_cache, schema=schema, page=page, sheet=sheet, body=body, **kwargs)
        return _model_to_dict(q)

    async def get_enriched_quotes(
        self,
        symbols: List[str],
        *,
        schema: Any = None,
        page: str = "",
        sheet: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[UnifiedQuote]:
        if not symbols:
            return []
        batch = max(1, min(500, _get_env_int("QUOTE_BATCH_SIZE", 25)))
        out: List[UnifiedQuote] = []
        for i in range(0, len(symbols), batch):
            part = symbols[i:i + batch]
            out.extend(
                await asyncio.gather(*[
                    self.get_enriched_quote(s, schema=schema, page=page, sheet=sheet, body=body, **kwargs)
                    for s in part
                ])
            )
        return out

    async def get_enriched_quotes_batch(
        self,
        symbols: List[str],
        mode: str = "",
        *,
        schema: Any = None,
        page: str = "",
        sheet: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        norm_syms = _normalize_symbol_list(symbols, limit=len(symbols) + 10)
        quotes = await asyncio.gather(*[self.get_enriched_quote_dict(s, schema=schema, page=page, sheet=sheet, body=body, **kwargs) for s in norm_syms])
        for req_sym, qd in zip(norm_syms, quotes):
            out[req_sym] = qd
            norm = _safe_str(qd.get("symbol_normalized") or qd.get("symbol"))
            if norm:
                out[norm] = qd
        for req in symbols:
            req2 = _safe_str(req)
            if req2 and req2 not in out:
                norm = normalize_symbol(req2)
                if norm in out:
                    out[req2] = out[norm]
        return out

    get_quote = get_enriched_quote
    get_quotes = get_enriched_quotes
    fetch_quote = get_enriched_quote
    fetch_quotes = get_enriched_quotes
    get_quotes_batch = get_enriched_quotes_batch
    get_analysis_quotes_batch = get_enriched_quotes_batch
    quotes_batch = get_enriched_quotes_batch
    get_quote_dict = get_enriched_quote_dict

    async def _build_data_dictionary_rows(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for sheet_name in _list_sheet_names_best_effort():
            spec, headers, keys, source = _schema_for_sheet(sheet_name)
            columns = _schema_columns_from_any(spec)
            for idx, (header, key) in enumerate(zip(headers, keys), start=1):
                col_meta = columns[idx - 1] if idx - 1 < len(columns) else {}
                if not isinstance(col_meta, Mapping):
                    col_meta = _model_to_dict(col_meta)
                dtype = _safe_str(col_meta.get("dtype") or col_meta.get("type") or col_meta.get("data_type") or "string")
                fmt = _safe_str(col_meta.get("format") or col_meta.get("fmt") or col_meta.get("number_format") or "")
                required = bool(col_meta.get("required")) if "required" in col_meta else idx <= 3
                source_hint = _safe_str(col_meta.get("source") or source)
                notes = _safe_str(col_meta.get("notes") or col_meta.get("description") or "static/registry contract")

                group = "Canonical"
                if key in TOP10_REQUIRED_FIELDS:
                    group = "Top10"
                elif sheet_name == "Insights_Analysis":
                    group = "Insights"
                elif sheet_name == "Data_Dictionary":
                    group = "Metadata"
                elif idx <= 8:
                    group = "Identity"
                elif idx <= 18:
                    group = "Price"
                elif idx <= 24:
                    group = "Liquidity"
                elif idx <= 36:
                    group = "Fundamentals"
                elif idx <= 44:
                    group = "Risk"
                elif idx <= 50:
                    group = "Valuation"
                elif idx <= 69:
                    group = "Forecast & Scoring"
                else:
                    group = "Portfolio & Provenance"

                rows.append(
                    {
                        "sheet": sheet_name,
                        "group": group,
                        "header": header,
                        "key": key,
                        "dtype": dtype,
                        "fmt": fmt,
                        "required": required,
                        "source": source_hint,
                        "notes": notes,
                    }
                )
        return rows

    async def _build_insights_rows_fallback(self, body: Optional[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
        body = dict(body or {})
        symbols = _extract_requested_symbols_from_body(body, limit=max(limit * 2, 10))
        if not symbols:
            for page_name in TOP10_ENGINE_DEFAULT_PAGES:
                symbols.extend(await self.get_sheet_symbols(page_name, limit=max(limit * 2, 10), body=body))
        symbols = _normalize_symbol_list(symbols, limit=max(limit * 3, 30))
        if not symbols:
            symbols = list(EMERGENCY_PAGE_SYMBOLS.get("Market_Leaders", [])[: max(limit, 6)])

        quotes = await self.get_enriched_quotes(symbols, schema=None, page="Insights_Analysis", body=body)
        quote_rows = [_model_to_dict(q) for q in quotes]
        quote_rows = [r for r in quote_rows if isinstance(r, dict)]
        quote_rows.sort(
            key=lambda r: (
                _as_float(r.get("opportunity_score")) or _as_float(r.get("overall_score")) or 0.0,
                _as_float(r.get("confidence_score")) or 0.0,
            ),
            reverse=True,
        )

        def _avg(values: List[Optional[float]]) -> Optional[float]:
            nums = [v for v in values if v is not None]
            return round(sum(nums) / len(nums), 4) if nums else None

        total = len(quote_rows)
        avg_overall = _avg([_as_float(r.get("overall_score")) for r in quote_rows])
        avg_roi_3m = _avg([_as_float(r.get("expected_roi_3m")) for r in quote_rows])

        risk_counts = {"LOW": 0, "MODERATE": 0, "HIGH": 0}
        for row in quote_rows:
            bucket = _safe_str(row.get("risk_bucket")).upper()
            if bucket in risk_counts:
                risk_counts[bucket] += 1

        rows: List[Dict[str, Any]] = []
        rows.append({"section": "Market Summary", "item": "Universe", "metric": "Symbols Analyzed",
                     "value": total, "notes": f"fallback summary from {len(symbols)} requested symbols",
                     "source": "engine_fallback", "sort_order": 1})
        rows.append({"section": "Market Summary", "item": "Universe", "metric": "Average Overall Score",
                     "value": avg_overall, "notes": "mean overall score across analyzed instruments",
                     "source": "engine_fallback", "sort_order": 2})
        rows.append({"section": "Market Summary", "item": "Universe", "metric": "Average Expected ROI 3M",
                     "value": avg_roi_3m, "notes": "fractional ROI where available",
                     "source": "engine_fallback", "sort_order": 3})

        sort_order = 10
        for bucket in ("LOW", "MODERATE", "HIGH"):
            rows.append({"section": "Risk Distribution", "item": bucket, "metric": "Count",
                         "value": risk_counts[bucket], "notes": "fallback risk bucket summary",
                         "source": "engine_fallback", "sort_order": sort_order})
            sort_order += 1

        top_quotes = quote_rows[: max(3, min(7, limit))]
        for idx, d in enumerate(top_quotes, start=1):
            rows.append({"section": "Top Ideas", "item": d.get("symbol"), "metric": "Recommendation",
                         "value": d.get("recommendation"),
                         "notes": d.get("recommendation_reason") or f"overall={d.get('overall_score')} opportunity={d.get('opportunity_score')}",
                         "source": "engine_fallback", "sort_order": 100 + idx})
            rows.append({"section": "Top Ideas", "item": d.get("symbol"), "metric": "Expected ROI 3M",
                         "value": d.get("expected_roi_3m"),
                         "notes": f"confidence={d.get('confidence_score')} risk={d.get('risk_bucket')}",
                         "source": "engine_fallback", "sort_order": 120 + idx})
            if _as_float(d.get("position_value")) is not None or _as_float(d.get("unrealized_pl")) is not None:
                rows.append({"section": "Portfolio Signals", "item": d.get("symbol"),
                             "metric": "Unrealized P/L", "value": d.get("unrealized_pl"),
                             "notes": f"value={d.get('position_value')} cost={d.get('position_cost')}",
                             "source": "engine_fallback", "sort_order": 140 + idx})

        return rows[:limit]

    def _top10_sort_key(self, row: Dict[str, Any]) -> Tuple[float, ...]:
        return (
            _as_float(row.get("opportunity_score")) or float("-inf"),
            _as_float(row.get("overall_score")) or float("-inf"),
            _as_float(row.get("confidence_score")) or float("-inf"),
            _as_float(row.get("expected_roi_3m")) or float("-inf"),
            _as_float(row.get("expected_roi_12m")) or float("-inf"),
            _as_float(row.get("value_score")) or float("-inf"),
            _as_float(row.get("quality_score")) or float("-inf"),
            _as_float(row.get("momentum_score")) or float("-inf"),
            _as_float(row.get("growth_score")) or float("-inf"),
            _as_float(row.get("current_price")) or float("-inf"),
        )

    async def _build_top10_rows_fallback(
        self,
        headers: Sequence[str],
        keys: Sequence[str],
        body: Optional[Dict[str, Any]],
        limit: int,
        mode: str = "",
    ) -> Tuple[List[str], List[str], List[Dict[str, Any]]]:
        body = dict(body or {})
        criteria = dict(body.get("criteria") or {}) if isinstance(body.get("criteria"), dict) else {}
        out_headers, out_keys = _ensure_top10_contract(headers, keys)

        top_n = max(1, min(int(criteria.get("top_n") or body.get("top_n") or 10), max(1, limit)))
        requested_pages = _extract_top10_pages_from_body(body) or list(TOP10_ENGINE_DEFAULT_PAGES)
        requested_symbols = _extract_requested_symbols_from_body(body, limit=max(limit * 10, 200))

        for page_name in requested_pages:
            if len(requested_symbols) >= max(limit * 10, 200):
                break
            syms = await self.get_sheet_symbols(page_name, limit=max(limit * 2, 25), body=body)
            if syms:
                requested_symbols.extend(syms)

        if not requested_symbols:
            requested_symbols = list(EMERGENCY_PAGE_SYMBOLS.get("Top_10_Investments") or [])

        requested_symbols = _normalize_symbol_list(requested_symbols, limit=max(limit * 10, 200))
        if not requested_symbols:
            return out_headers, out_keys, []

        quotes = await self.get_enriched_quotes(requested_symbols, schema=None, page="Top_10_Investments", body=body)
        rows: List[Dict[str, Any]] = []
        for q in quotes:
            row = _model_to_dict(q)
            row = _apply_page_row_backfill("Top_10_Investments", row)
            _compute_scores_canonical_first(row)
            # v5.75.0: classifier self-clears at entry.
            _compute_recommendation(row)
            rows.append(row)

        if not rows:
            return out_headers, out_keys, []

        _apply_rank_overall(rows)
        rows.sort(key=self._top10_sort_key, reverse=True)

        criteria_snapshot = _top10_criteria_snapshot({
            **criteria,
            "pages_selected": requested_pages,
            "direct_symbols": requested_symbols,
            "top_n": top_n,
        })

        selected: List[Dict[str, Any]] = []
        seen: Set[str] = set()
        for row in rows:
            sym = normalize_symbol(_safe_str(row.get("symbol") or row.get("ticker") or row.get("requested_symbol")))
            dedupe_key = sym or _safe_str(row.get("name")) or f"row_{len(selected)+1}"
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            row["top10_rank"] = len(selected) + 1
            row["selection_reason"] = row.get("selection_reason") or _top10_selection_reason(row)
            row["criteria_snapshot"] = row.get("criteria_snapshot") or criteria_snapshot
            projected = _normalize_to_schema_keys(out_keys, out_headers, row)
            projected["top10_rank"] = row["top10_rank"]
            projected["selection_reason"] = row["selection_reason"]
            projected["criteria_snapshot"] = row["criteria_snapshot"]
            selected.append(_strict_project_row(out_keys, projected))
            if len(selected) >= top_n:
                break

        return out_headers, out_keys, selected


    async def get_page_rows(
        self,
        page: Optional[str] = None,
        *,
        sheet: Optional[str] = None,
        sheet_name: Optional[str] = None,
        limit: int = 2000,
        offset: int = 0,
        mode: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        return await self.get_sheet_rows(
            page or sheet or sheet_name,
            limit=limit, offset=offset, mode=mode, body=body,
            page=page, sheet=sheet, sheet_name=sheet_name, **kwargs,
        )

    async def get_sheet(
        self,
        sheet_name: Optional[str] = None,
        *,
        sheet: Optional[str] = None,
        page: Optional[str] = None,
        limit: int = 2000,
        offset: int = 0,
        mode: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        return await self.get_sheet_rows(
            sheet_name or sheet or page,
            limit=limit, offset=offset, mode=mode, body=body,
            page=page, sheet=sheet, sheet_name=sheet_name, **kwargs,
        )

    async def get_sheet_rows(
        self,
        sheet: Optional[str] = None,
        *,
        sheet_name: Optional[str] = None,
        page: Optional[str] = None,
        limit: int = 2000,
        offset: int = 0,
        mode: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        target_sheet, limit, offset, mode, body, request_parts = _normalize_route_call_inputs(
            page=page, sheet=sheet, sheet_name=sheet_name,
            limit=limit, offset=offset, mode=mode, body=body, extras=kwargs,
        )
        include_matrix = _safe_bool(body.get("include_matrix"), True)

        spec, headers, keys, schema_src = _schema_for_sheet(target_sheet)
        headers, keys = _complete_schema_contract(headers, keys)

        if target_sheet == "Top_10_Investments" and self.top10_force_full_schema:
            static_contract = STATIC_CANONICAL_SHEET_CONTRACTS.get("Top_10_Investments", {})
            static_headers, static_keys = _complete_schema_contract(static_contract.get("headers", []), static_contract.get("keys", []))
            if len(static_keys) >= len(keys):
                headers, keys = _ensure_top10_contract(static_headers, static_keys)
                schema_src = f"{schema_src}|top10_force_full_schema"

        if (
            target_sheet in INSTRUMENT_SHEETS
            and target_sheet != "Top_10_Investments"
            and self.instrument_force_canonical_schema
        ):
            if list(keys) != list(INSTRUMENT_CANONICAL_KEYS):
                headers, keys = list(INSTRUMENT_CANONICAL_HEADERS), list(INSTRUMENT_CANONICAL_KEYS)
                schema_src = f"{schema_src}|instrument_force_canonical_schema"

        target_sheet_known = target_sheet in INSTRUMENT_SHEETS or target_sheet in SPECIAL_SHEETS or bool(spec)
        strict_req = bool(self.schema_strict_sheet_rows)
        contract_level = "canonical" if _usable_contract(headers, keys, target_sheet) else "partial"
        recovered_from: Optional[str] = None

        if target_sheet == "Data_Dictionary":
            if _is_schema_only_body(body):
                return self._finalize_payload(
                    sheet=target_sheet, headers=headers, keys=keys, row_objects=[],
                    include_matrix=include_matrix, status="success",
                    meta={"schema_source": schema_src, "contract_level": contract_level,
                          "strict_requested": strict_req, "strict_enforced": False,
                          "target_sheet_known": True, "builder": "schema_only_fast_path",
                          "rows": 0, "limit": limit, "offset": offset, "mode": mode},
                )
            rows_all = await self._build_data_dictionary_rows()
            rows_proj = [_strict_project_row(keys, _normalize_to_schema_keys(keys, headers, r)) for r in rows_all]
            payload_full = self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys, row_objects=rows_proj,
                include_matrix=include_matrix, status="success",
                meta={"schema_source": schema_src, "contract_level": contract_level,
                      "strict_requested": strict_req, "strict_enforced": False,
                      "target_sheet_known": True, "builder": "engine.internal_data_dictionary",
                      "rows": len(rows_proj), "limit": limit, "offset": offset, "mode": mode},
            )
            self._store_sheet_snapshot(target_sheet, payload_full)
            rows_page = rows_proj[offset : offset + limit]
            return self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys, row_objects=rows_page,
                include_matrix=include_matrix, status="success",
                meta={**payload_full.get("meta", {}), "rows": len(rows_page)},
            )

        if target_sheet == "Insights_Analysis":
            if _is_schema_only_body(body):
                return self._finalize_payload(
                    sheet=target_sheet, headers=headers, keys=keys, row_objects=[],
                    include_matrix=include_matrix, status="success",
                    meta={"schema_source": schema_src, "contract_level": contract_level,
                          "strict_requested": strict_req, "strict_enforced": False,
                          "target_sheet_known": True, "builder": "schema_only_fast_path",
                          "rows": 0, "limit": limit, "offset": offset, "mode": mode},
                )

            rows0: List[Dict[str, Any]] = []
            builder_name = "core.analysis.insights_builder"
            try:
                from core.analysis.insights_builder import build_insights_analysis_rows  # type: ignore
                crit = body.get("criteria") if isinstance(body.get("criteria"), dict) else None
                universes = body.get("universes") if isinstance(body.get("universes"), dict) else None
                symbols = body.get("symbols") if isinstance(body.get("symbols"), list) else None
                payload = await build_insights_analysis_rows(
                    engine=self, criteria=crit, universes=universes,
                    symbols=symbols, mode=mode or "",
                )
                rows0 = _coerce_rows_list(payload)
            except Exception as exc:
                builder_name = f"fallback:insights_builder_failed:{type(exc).__name__}"
                rows0 = []

            if not rows0:
                rows0 = await self._build_insights_rows_fallback(body, limit=max(limit + offset, 10))
                builder_name = "fallback:engine_insights_rows"

            rows_proj = [_strict_project_row(keys, _normalize_to_schema_keys(keys, headers, r)) for r in rows0]
            payload_full = self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys, row_objects=rows_proj,
                include_matrix=include_matrix,
                status="success" if rows_proj else "warn",
                meta={"schema_source": schema_src, "contract_level": contract_level,
                      "strict_requested": strict_req, "strict_enforced": False,
                      "target_sheet_known": True, "builder": builder_name,
                      "rows": len(rows_proj), "limit": limit, "offset": offset, "mode": mode},
            )
            self._store_sheet_snapshot(target_sheet, payload_full)
            rows_page = rows_proj[offset : offset + limit]
            return self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys, row_objects=rows_page,
                include_matrix=include_matrix, status=payload_full.get("status", "success"),
                meta={**payload_full.get("meta", {}), "rows": len(rows_page)},
            )

        if target_sheet == "Top_10_Investments":
            top10_body, route_warnings = _normalize_top10_body_for_engine(body, limit=max(1, min(limit, 50)))
            if _is_schema_only_body(top10_body):
                headers, keys = _ensure_top10_contract(headers, keys)
                return self._finalize_payload(
                    sheet=target_sheet, headers=headers, keys=keys, row_objects=[],
                    include_matrix=include_matrix, status="success",
                    meta={"schema_source": schema_src, "contract_level": contract_level,
                          "strict_requested": strict_req, "strict_enforced": False,
                          "target_sheet_known": True, "builder": "schema_only_fast_path",
                          "rows": 0, "limit": limit, "offset": offset, "mode": mode,
                          "warnings": route_warnings},
                )

            rows_proj: List[Dict[str, Any]] = []
            builder_used = "core.analysis.top10_selector"
            status_out = "success"

            try:
                from core.analysis.top10_selector import build_top10_rows  # type: ignore
                criteria = top10_body.get("criteria") if isinstance(top10_body.get("criteria"), dict) else None
                payload = await build_top10_rows(
                    engine=self, settings=self.settings,
                    criteria=criteria, body=dict(top10_body or {}),
                    limit=int(top10_body.get("limit") or top10_body.get("top_n") or min(limit, 10) or 10),
                    mode=mode or "",
                )
                rows0 = _coerce_rows_list(payload)
                if rows0:
                    rows_proj = [_strict_project_row(keys, _normalize_to_schema_keys(keys, headers, r)) for r in rows0]
                status_out = _safe_str(payload.get("status"), "success") if isinstance(payload, dict) else "success"
            except Exception as exc:
                builder_used = f"fallback:top10_selector_failed:{type(exc).__name__}"
                status_out = "warn"

            if not rows_proj:
                headers, keys, rows_proj = await self._build_top10_rows_fallback(headers, keys, top10_body, limit=max(limit + offset, 10), mode=mode)
                builder_used = "fallback:live_ranker"
                if rows_proj:
                    status_out = "warn"

            payload_full = self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys, row_objects=rows_proj,
                include_matrix=include_matrix,
                status=status_out if rows_proj else "warn",
                meta={"schema_source": schema_src, "contract_level": contract_level,
                      "strict_requested": strict_req, "strict_enforced": False,
                      "target_sheet_known": True, "builder": builder_used,
                      "rows": len(rows_proj), "limit": limit, "offset": offset, "mode": mode,
                      "warnings": route_warnings},
            )
            if rows_proj:
                self._store_sheet_snapshot(target_sheet, payload_full)

            rows_page = rows_proj[offset : offset + limit]
            return self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys, row_objects=rows_page,
                include_matrix=include_matrix, status=payload_full.get("status", "warn"),
                meta={**payload_full.get("meta", {}), "rows": len(rows_page)},
            )

        if contract_level != "canonical":
            cached_snap = self.get_cached_sheet_snapshot(sheet=target_sheet)
            recovered = False
            if isinstance(cached_snap, dict):
                c_headers = cached_snap.get("headers") or cached_snap.get("display_headers")
                c_keys = cached_snap.get("keys") or cached_snap.get("fields")
                if c_headers or c_keys:
                    ch, ck = _complete_schema_contract(c_headers or [], c_keys or [])
                    if _usable_contract(ch, ck, target_sheet):
                        headers, keys = ch, ck
                        schema_src = "recovered_from_cache_contract"
                        contract_level = "recovered"
                        recovered_from = "cache_contract"
                        recovered = True

            if not recovered and target_sheet in STATIC_CANONICAL_SHEET_CONTRACTS:
                c = STATIC_CANONICAL_SHEET_CONTRACTS[target_sheet]
                headers, keys = _complete_schema_contract(c["headers"], c["keys"])
                schema_src = "static_canonical_contract_recovery"
                contract_level = "recovered"
                recovered_from = "static_contract"
                recovered = True

            if not recovered and target_sheet in INSTRUMENT_SHEETS:
                headers, keys = list(INSTRUMENT_CANONICAL_HEADERS), list(INSTRUMENT_CANONICAL_KEYS)
                schema_src = "fallback_union"
                contract_level = "union_fallback"
                recovered_from = "union_fallback"

        final_status = "success"
        schema_warning: Optional[str] = None
        if contract_level == "union_fallback" and target_sheet_known:
            final_status = "warn"
            schema_warning = "canonical_schema_unusable_used_union_schema"
        elif not target_sheet_known and not strict_req:
            final_status = "warn"
            schema_warning = "unknown_sheet_non_strict_mode"

        base_meta = {
            "schema_source": schema_src, "contract_level": contract_level,
            "strict_requested": strict_req, "strict_enforced": False,
            "target_sheet_known": target_sheet_known,
            "route_input_keys": sorted([str(k) for k in body.keys()]) if isinstance(body, dict) else [],
            "request_input_keys": sorted([str(k) for k in request_parts.keys()]) if isinstance(request_parts, dict) else [],
        }
        if recovered_from:
            base_meta["recovered_from"] = recovered_from
        if schema_warning:
            base_meta["schema_warning"] = schema_warning

        if _is_schema_only_body(body):
            return self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys, row_objects=[],
                include_matrix=include_matrix, status=final_status,
                meta={**base_meta, "rows": 0, "limit": limit, "offset": offset, "mode": mode,
                      "built_from": "schema_only_fast_path"},
            )

        requested_symbols = _extract_requested_symbols_from_body(body, limit=limit + offset)
        built_from = "body_symbols" if requested_symbols else "live_quotes"
        if requested_symbols:
            self._set_sheet_symbols_meta(target_sheet, "body_symbols", len(requested_symbols))
        if not requested_symbols and target_sheet in INSTRUMENT_SHEETS:
            requested_symbols = await self.get_sheet_symbols(target_sheet, limit=limit + offset, body=body)
            built_from = self._get_sheet_symbols_meta(target_sheet).get("source") or ("auto_sheet_symbols" if requested_symbols else "empty")

        out_headers = list(headers)
        out_keys = list(keys)

        if target_sheet in INSTRUMENT_SHEETS:
            ext_rows = await self._get_rows_from_external_reader(target_sheet, limit + offset)
            if ext_rows:
                enriched_rows: List[Dict[str, Any]] = []
                symbols = _extract_symbols_from_rows(ext_rows, limit=limit + offset)
                quote_map: Dict[str, Dict[str, Any]] = {}

                if self.rows_hydrate_external and symbols:
                    for q in await self.get_enriched_quotes(symbols, schema=None, page=target_sheet, body=body):
                        d = _model_to_dict(q)
                        sym = normalize_symbol(_safe_str(d.get("symbol")))
                        if sym:
                            quote_map[sym] = d

                snapshot_map = self._get_cached_snapshot_symbol_map(target_sheet)

                for row in ext_rows:
                    merged = dict(row)
                    sym = normalize_symbol(self._extract_row_symbol(row))
                    if sym and sym in snapshot_map:
                        merged = _merge_missing_fields(merged, snapshot_map[sym])
                    best_snapshot_row = self._get_best_cached_snapshot_row_for_symbol(sym, prefer_sheet=target_sheet) if sym else None
                    if best_snapshot_row:
                        merged = _merge_missing_fields(merged, best_snapshot_row)
                    if sym and sym in quote_map:
                        merged = _merge_missing_fields(merged, quote_map[sym])
                    merged = _apply_page_row_backfill(target_sheet, merged)
                    _compute_scores_canonical_first(merged)
                    # v5.75.0: classifier self-clears at entry.
                    _compute_recommendation(merged)
                    enriched_rows.append(_strict_project_row(out_keys, _normalize_to_schema_keys(out_keys, out_headers, merged)))

                _apply_rank_overall(enriched_rows)

                payload_full = self._finalize_payload(
                    sheet=target_sheet, headers=out_headers, keys=out_keys,
                    row_objects=enriched_rows, include_matrix=include_matrix, status=final_status,
                    meta={**base_meta, "rows": len(enriched_rows), "limit": limit, "offset": offset, "mode": mode,
                          "built_from": "external_rows_reader",
                          "rows_reader_source": self._rows_reader_source,
                          "symbols_reader_source": self._symbols_reader_source,
                          "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet)},
                )
                self._store_sheet_snapshot(target_sheet, payload_full)
                rows_page = enriched_rows[offset : offset + limit]
                return self._finalize_payload(
                    sheet=target_sheet, headers=out_headers, keys=out_keys, row_objects=rows_page,
                    include_matrix=include_matrix, status=final_status,
                    meta={**payload_full.get("meta", {}), "rows": len(rows_page)},
                )

        if not requested_symbols:
            cached_snap = self.get_cached_sheet_snapshot(sheet=target_sheet)
            cached_rows = _coerce_rows_list(cached_snap)
            if cached_rows:
                proj_rows = [_strict_project_row(out_keys, _normalize_to_schema_keys(out_keys, out_headers, row)) for row in cached_rows]
                rows_page = proj_rows[offset : offset + limit]
                return self._finalize_payload(
                    sheet=target_sheet, headers=out_headers, keys=out_keys, row_objects=rows_page,
                    include_matrix=include_matrix, status=final_status,
                    meta={**base_meta, "rows": len(rows_page), "limit": limit, "offset": offset, "mode": mode,
                          "built_from": "cached_snapshot",
                          "symbols_reader_source": self._symbols_reader_source,
                          "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet)},
                )

        rows_full: List[Dict[str, Any]] = []
        if requested_symbols:
            snapshot_map = self._get_cached_snapshot_symbol_map(target_sheet)
            quotes = await self.get_enriched_quotes(requested_symbols, schema=None, page=target_sheet, body=body)
            for q in quotes:
                row = _model_to_dict(q)
                sym = normalize_symbol(_safe_str(row.get("symbol") or row.get("requested_symbol")))
                if sym and sym in snapshot_map:
                    row = _merge_missing_fields(row, snapshot_map[sym])
                best_snapshot_row = self._get_best_cached_snapshot_row_for_symbol(sym, prefer_sheet=target_sheet) if sym else None
                if best_snapshot_row:
                    row = _merge_missing_fields(row, best_snapshot_row)
                row = _apply_page_row_backfill(target_sheet, row)
                _compute_scores_canonical_first(row)
                # v5.75.0: classifier self-clears at entry.
                _compute_recommendation(row)
                rows_full.append(_strict_project_row(out_keys, _normalize_to_schema_keys(out_keys, out_headers, row)))
            _apply_rank_overall(rows_full)

        if rows_full:
            payload_full = self._finalize_payload(
                sheet=target_sheet, headers=out_headers, keys=out_keys, row_objects=rows_full,
                include_matrix=include_matrix, status=final_status,
                meta={**base_meta, "rows": len(rows_full), "limit": limit, "offset": offset, "mode": mode,
                      "built_from": built_from,
                      "resolved_symbols_count": len(requested_symbols),
                      "symbols_reader_source": self._symbols_reader_source,
                      "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet)},
            )
            self._store_sheet_snapshot(target_sheet, payload_full)
            rows_page = rows_full[offset : offset + limit]
            return self._finalize_payload(
                sheet=target_sheet, headers=out_headers, keys=out_keys, row_objects=rows_page,
                include_matrix=include_matrix, status=final_status,
                meta={**payload_full.get("meta", {}), "rows": len(rows_page)},
            )

        return self._finalize_payload(
            sheet=target_sheet, headers=out_headers, keys=out_keys, row_objects=[],
            include_matrix=include_matrix, status=final_status,
            meta={**base_meta, "rows": 0, "limit": limit, "offset": offset, "mode": mode,
                  "built_from": built_from, "resolved_symbols_count": len(requested_symbols),
                  "symbols_reader_source": self._symbols_reader_source,
                  "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet)},
        )

    async def sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def build_sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    def get_sheet_contract(self, sheet: str) -> Dict[str, Any]:
        target = _canonicalize_sheet_name(sheet) or sheet or "Market_Leaders"
        _spec, headers, keys, source = _schema_for_sheet(target)
        if target == "Top_10_Investments":
            headers, keys = _ensure_top10_contract(headers, keys)
        return {
            "sheet": target, "page": target, "sheet_name": target,
            "headers": headers, "display_headers": headers,
            "keys": keys, "fields": keys, "source": source, "count": len(keys),
        }

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

    async def health(self) -> Dict[str, Any]:
        return {
            "status": "ok",
            "version": self.version,
            "schema_available": True,
            "instrument_force_canonical_schema": bool(self.instrument_force_canonical_schema),
            "static_contract_sheets": sorted(list(STATIC_CANONICAL_SHEET_CONTRACTS.keys())),
            "snapshot_sheets": len(self._sheet_snapshots),
            "rows_reader_source": self._rows_reader_source,
            "symbols_reader_source": self._symbols_reader_source,
            "yahoo_enrichment_pass": {
                "enabled": _yahoo_enrichment_enabled(),
                "enrich_on_missing_industry": _yahoo_enrich_on_missing_industry(),
                "enrich_on_missing_risk_metrics": _yahoo_enrich_on_missing_risk_metrics(),
                "fundamental_fields_chased": list(_YAHOO_FUNDAMENTAL_FIELDS),
                "chart_fields_chased": list(_YAHOO_CHART_FIELDS),
                "last_pass": dict(_YAHOO_ENRICHMENT_LAST_PASS),
            },
            # v5.76.0: cross-stack 8-tier alignment snapshot. Operators can
            # check this block to verify scoring.py / reco_normalize /
            # data_engine_v2 are all in lockstep on the 8-tier vocabulary
            # and to confirm whether the sheet-compat opt-in collapse is
            # currently active.
            "8tier_alignment": {
                "scoring_contract_version": _SCORING_CONTRACT_VERSION,
                "reco_normalize_contract_version": _RECO_NORMALIZE_CONTRACT_VERSION,
                "scoring_recommendation_enum": tuple(_SCORING_RECOMMENDATION_ENUM),
                "engine_recommendation_enum": tuple(sorted(_V573_RECOMMENDATION_ENUM)),
                "sheet_collapse_to_6tier_enabled": _sheet_collapse_to_6tier_enabled(),
                "sheet_compat_collapse_map": dict(_V576_SHEET_COMPAT_COLLAPSE),
                "core_scoring_available": bool(_CORE_SCORING_AVAILABLE),
                "reco_normalize_available": bool(_RECO_NORMALIZE_AVAILABLE),
            },
            # v5.77.0: valuation-model markers so operators can verify the
            # sector_pe / sector_pb calibration in effect, the Phase-II
            # forecast weights, and whether the dedicated provider thread
            # pool is active.
            "valuation_model": {
                "version": "v5.77.0",
                "intrinsic_pe_default": 20.0,
                "intrinsic_pb_default": 2.0,
                "intrinsic_pb_anchor_mode": "sector_aware",
                "phase_ii_weights": {
                    "intrinsic_reversion": 0.25,
                    "momentum": 0.35,
                    "fundamentals": 0.25,
                    "baseline": 0.15,
                },
                "phase_ii_rsi_horizon": "short_only_1m_3m",
                "phase_ii_12m_cap_abs": _PHASE_II_MAX_12M_ABS_RETURN,
                "dividend_yield_ceiling": 0.30,
                "provider_pool_workers_target": int(os.getenv("TFB_PROVIDER_POOL_WORKERS", "200") or 200),
                "provider_pool_active": _PROVIDER_EXECUTOR is not None,
                "cache_eviction_mode": "lru",
            },
        }

    async def get_health(self) -> Dict[str, Any]:
        return await self.health()

    async def health_check(self) -> Dict[str, Any]:
        return await self.health()

    async def get_stats(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "primary_provider": self.primary_provider,
            "enabled_providers": list(self.enabled_providers),
            "ksa_providers": list(self.ksa_providers),
            "global_providers": list(self.global_providers),
            "non_ksa_primary_provider": self.non_ksa_primary_provider,
            "page_primary_providers": dict(self.page_primary_providers),
            "history_fallback_providers": list(self.history_fallback_providers),
            "ksa_disallow_eodhd": bool(self.ksa_disallow_eodhd),
            "flags": dict(self.flags),
            "provider_stats": await self._registry.get_stats(),
            "schema_available": True,
            "schema_strict_sheet_rows": bool(self.schema_strict_sheet_rows),
            "top10_force_full_schema": bool(self.top10_force_full_schema),
            "instrument_force_canonical_schema": bool(self.instrument_force_canonical_schema),
            "rows_hydrate_external": bool(self.rows_hydrate_external),
            "symbols_reader_source": self._symbols_reader_source,
            "rows_reader_source": self._rows_reader_source,
            "snapshot_sheets": sorted(list(self._sheet_snapshots.keys())),
            "sheet_symbol_resolution_meta": dict(self._sheet_symbol_resolution_meta),
            # v5.76.0: mirror of health()'s 8tier_alignment block so
            # operators have a single endpoint that reports both stats
            # and contract alignment.
            "8tier_alignment": {
                "scoring_contract_version": _SCORING_CONTRACT_VERSION,
                "reco_normalize_contract_version": _RECO_NORMALIZE_CONTRACT_VERSION,
                "sheet_collapse_to_6tier_enabled": _sheet_collapse_to_6tier_enabled(),
                "core_scoring_available": bool(_CORE_SCORING_AVAILABLE),
                "reco_normalize_available": bool(_RECO_NORMALIZE_AVAILABLE),
            },
        }


def normalize_row_to_schema(sheet: str, row: Dict[str, Any], keep_extras: bool = False) -> Dict[str, Any]:
    target = _canonicalize_sheet_name(sheet) or sheet or "Market_Leaders"
    _spec, headers, keys, _src = _schema_for_sheet(target)
    if target == "Top_10_Investments":
        headers, keys = _ensure_top10_contract(headers, keys)
    normalized = _normalize_to_schema_keys(keys, headers, dict(row or {}))
    normalized = _apply_page_row_backfill(target, normalized)
    if keep_extras and isinstance(row, dict):
        for k, v in row.items():
            if k not in normalized:
                normalized[k] = _json_safe(v)
    return normalized


_ENGINE_INSTANCE: Optional[DataEngineV5] = None
ENGINE: Optional[DataEngineV5] = None
engine: Optional[DataEngineV5] = None
_ENGINE: Optional[DataEngineV5] = None
_ENGINE_LOCK = asyncio.Lock()


async def get_engine() -> DataEngineV5:
    global _ENGINE_INSTANCE, ENGINE, engine, _ENGINE
    if _ENGINE_INSTANCE is None:
        async with _ENGINE_LOCK:
            if _ENGINE_INSTANCE is None:
                _ENGINE_INSTANCE = DataEngineV5()
    ENGINE = _ENGINE_INSTANCE
    engine = _ENGINE_INSTANCE
    _ENGINE = _ENGINE_INSTANCE
    return _ENGINE_INSTANCE


async def close_engine() -> None:
    global _ENGINE_INSTANCE, ENGINE, engine, _ENGINE
    if _ENGINE_INSTANCE is not None:
        await _ENGINE_INSTANCE.aclose()
    _ENGINE_INSTANCE = None
    ENGINE = None
    engine = None
    _ENGINE = None


def get_engine_if_ready() -> Optional[DataEngineV5]:
    return _ENGINE_INSTANCE


def peek_engine() -> Optional[DataEngineV5]:
    return _ENGINE_INSTANCE


def get_cache() -> Any:
    return getattr(_ENGINE_INSTANCE, "_cache", None)


DataEngineV4 = DataEngineV5
DataEngineV3 = DataEngineV5
DataEngineV2 = DataEngineV5
DataEngine = DataEngineV5

__all__ = [
    "DataEngineV5", "DataEngineV4", "DataEngineV3", "DataEngineV2", "DataEngine",
    "ENGINE", "engine", "_ENGINE",
    "get_engine", "get_engine_if_ready", "peek_engine", "close_engine", "get_cache",
    "QuoteQuality", "DataSource", "UnifiedQuote",
    "__version__",
    "STATIC_CANONICAL_SHEET_CONTRACTS",
    "get_sheet_spec", "normalize_row_to_schema",
]
