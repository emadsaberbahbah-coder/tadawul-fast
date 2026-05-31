#!/usr/bin/env python3
# core/data_engine_v2.py
"""
================================================================================
Data Engine V2 - GLOBAL-FIRST ORCHESTRATOR - v5.77.19
================================================================================

WHY v5.77.19 - PROVIDER-TARGET GATE HARDENING (audit follow-up to v5.77.18)
---------------------------------------------------------------------------
The v5.77.18 ingestion gate was accepted as directionally correct (it stops an
out-of-band analyst target from being tagged forecast_source="provider_target"
and saturating the +65.0 / +27.3 / +11.7 ROI triplet). The post-v5.77.18 audit
found three hardening items; v5.77.19 applies all three. No schema/contract/math
changes outside the three gated paths below.

  Fix 1 - GATE FAIL-OPEN WHEN THE YAHOO PATCH LACKS A PRICE (highest priority)
    _provider_target_is_plausible() is fail-open: with no price it returns True
    so a sparse feed never blanks a genuine forecast. That is correct for the
    primary provider pass (the patch carries its own current_price). But the
    Yahoo enrichment pass canonicalizes the Yahoo *fundamentals* patch on its
    OWN, and that patch frequently has targetMeanPrice but NO currentPrice — so
    the gate saw price=None, failed open, and an implausible Yahoo target could
    still tag provider_target on the merged row. Fix: before canonicalizing a
    Yahoo patch, inject the already-known row price (current_price/price) into a
    copy of the patch when the patch itself has none, so the gate judges the
    Yahoo target against the real price. The injected price is never merged back
    (current_price is gated out of the fundamentals whitelist; for the chart
    whitelist the missing-field filter only fills a blank row price, so an
    existing price is untouched).

  Fix 2 - GATE DROP-WARNINGS LOST IN THE ENRICHMENT FILTER
    The gate records provider_target_implausible_dropped_12m / _3m in warnings,
    but warnings is not in _YAHOO_FUNDAMENTAL_FIELDS / _YAHOO_CHART_FIELDS, so on
    the Yahoo path _filter_patch_to_missing_fields() discarded the tag and the
    drop happened with no audit trail. Fix: after filtering, explicitly carry
    any provider_target_implausible_dropped_* tag from the canonicalized patch
    back onto the row via _merge_gate_drop_warnings().

  Fix 3 - 3M-ONLY PROVIDER TARGET FELL THROUGH PHASE-II
    _phase_ii_quality_forecast honored a provider_target only when
    forecast_price_12m was populated. With the v5.77.18 gate this case is now
    common: the 12M leg can be dropped as implausible while a plausible 3M leg
    survives and the row stays tagged provider_target. The 12M-only guard then
    fell through to full synthesis, OVERWRITING the surviving 3M provider target.
    Fix: a dedicated 3M-only branch preserves the provider 3M value, derives the
    1M leg from the capped 3M return, and synthesizes the missing 12M leg by
    scaling the capped 3M return up to the 12M horizon (re-capped at +/-30%) —
    instead of discarding the provider signal.

VALIDATION (post-deploy)
  - UHS-like (price 146, target 417): 12M dropped, no provider_target tag, no
    +65/+27.3/+11.7 saturation.
  - Normal (price 100, target 125): accepted, forecast_source=provider_target.
  - No price in provider patch but row has price: gate still judges via row price.
  - Dropped target: provider_target_implausible_dropped_* visible in warnings,
    INCLUDING on the Yahoo enrichment path.
  - 3M-only provider target: 3M preserved; 1M + 12M derived from it.
  - /health: version 5.77.19, valuation_model.version v5.77.19, canonical_schema 107.

WHY v5.77.18 - IMPLAUSIBLE PROVIDER-TARGET GATE (closes the saturated-upside bug)
---------------------------------------------------------------------------------
A deployed refresh showed the SAME upside triplet — Expected ROI 12M / 3M / 1M
landing on +65.0 / +27.3 / +11.7 — saturating row after row. Traced end to end,
the cause is an upstream analyst target that is wildly out of band versus price
flowing through the pipeline UNCHECKED:

    1. _canonicalize_provider_row maps a provider analyst target (any of
       targetMeanPrice / targetHighPrice / targetMedianPrice / targetPrice /
       analystTargetPrice / consensusTarget) into forecast_price_12m via
       _CANONICAL_FIELD_ALIASES, then tags forecast_source = "provider_target".
       Real case: UHS target 417 vs current price 146 = +185%.
    2. _phase_ii_quality_forecast honors the provider_target tag and writes the
       raw, UNCAPPED (fp12 - cp)/cp into expected_roi_12m (it only caps the
       DERIVED 3M / 1M short horizons, not the 12M it inherits from the target).
    3. core.scoring then clamps that +185% down to its own max_roi_12m (0.65)
       and sub-splits via its 0.42 / 0.18 horizon ratios into exactly the
       +65.0 / +27.3 / +11.7 triplet seen on the sheet.

The engine's own Phase-II +/-30% cap (_PHASE_II_MAX_12M_ABS_RETURN) never gets
to weigh in, because the provider_target branch short-circuits synthesis before
the cap is applied, and scoring runs on the inherited target.

THE FIX (single ingestion chokepoint)
-------------------------------------
Gate the target at INGESTION, inside _canonicalize_provider_row, BEFORE the
provider_target tag is written. If the mapped target is outside a sane multiple
of current price, DROP the implausible forecast_price_* value and DO NOT tag
provider_target. With no tag and no forecast price, _phase_ii_quality_forecast
falls through to full intrinsic + momentum + quality synthesis (capped at
+/-30%), and core.scoring then reads a sane forecast. Gating here is the one
place that keeps the bad value out of BOTH Phase-II and scoring; gating in
scoring alone would still leave the engine tagging the value as a provider
target and Phase-II inheriting it.

Tunable band (multiples of current price), read at call time so an env change
takes effect without re-import:
    TFB_PROVIDER_TARGET_MIN_MULT      default 0.40  -> reject target < 0.40 x price
    TFB_PROVIDER_TARGET_MAX_MULT      default 2.50  -> reject target > 2.50 x price
    TFB_PROVIDER_TARGET_GATE_ENABLED  default on    -> set 0/false/off to disable
                                                       (restores pre-v5.77.18 behavior)

Fail-open by design: when current price or the target is missing / non-positive
the gate cannot judge and treats the target as plausible, so it never fabricates
a rejection from absent data. No try/except swallows the gate decision — the
skip path is explicit, INFO-logged ("[v5.77.18 TARGET-GATE] dropped ..."), and
tagged in warnings (provider_target_implausible_dropped_12m / _3m).

No schema changes. No contract changes. No math changes outside the gated path.
All v5.77.6 through v5.77.17 behavior preserved; the only behavioral delta from
v5.77.17 is the ingestion gate above (plus this WHY block, the version bump, and
the health() valuation_model version string).

WHY v5.77.17 - PROVIDER-OVERRIDE IDEMPOTENCY (closes the v5.77.16 asymmetry)
----------------------------------------------------------------------------
The v5.77.16 audit accepted the duplicate-classification fix but caught one
remaining asymmetry. v5.77.16 stopped an engine-written recommendation from
being CAPTURED as provider_rating, but the `provider_wins` decision was still
computed from `provider_canon`, which was derived from row["recommendation"]
regardless of who wrote it:

    raw_upstream  = row.get("recommendation")          # could be engine-written
    provider_canon = _v573_collapse_to_canonical_enum(raw_upstream)
    provider_wins  = bool(provider_canon) and trust_provider

So if the classifier were invoked again on a row whose recommendation the
engine had already written (e.g. "BUY"), and TFB_TRUST_PROVIDER_RECO=1, then
provider_canon="BUY" and provider_wins=True — Step 3a would stamp
source="provider_override" and reason "Provider rating accepted via
TFB_TRUST_PROVIDER_RECO override." on what is actually an engine
recommendation. (In the v5.77.16 single-pass orchestration recommendation_source
is unset at classify time, so this does not fire in the normal flow — it is a
latent defect that surfaces only with the trust flag on AND a re-invocation,
which is exactly the class of bug the v5.77.16 idempotency guard set out to
eliminate.)

THE FIX
-------
When _reco_is_engine_written is True, derive provider_canon from the already
-captured provider_rating field instead of from row["recommendation"]:

    if _reco_is_engine_written:
        provider_canon = _v573_collapse_to_canonical_enum(row.get("provider_rating"))
    else:
        provider_canon = _v573_collapse_to_canonical_enum(raw_upstream)

Now provider_wins depends only on a REAL upstream provider rating:
  - engine-written reco, no real provider rating -> provider_canon="" ->
    provider_wins=False -> no false override (source stays engine/engine_local_score).
  - engine-written reco, real provider rating present (captured on an earlier
    pass) -> provider_canon=<real rating> -> with trust on, the override is
    legitimate and intended.
  - first pass (not engine-written) -> unchanged behavior.

Together with the v5.77.16 capture guard, the classifier is now fully
idempotent for BOTH provider_rating capture AND the provider-override
decision, with or without TFB_TRUST_PROVIDER_RECO enabled.

No schema changes. No contract changes. No math changes. All v5.77.14
(fundamentals picker), v5.77.15 (master switch + score fallback), and v5.77.16
(single classification + capture guard) behavior preserved. Provider symbol
routing still intentionally NOT added (providers self-normalize); the
.NS/.T/.KS/.DE/.AX EODHD suffix gap remains a core.symbols.normalize change
tracked separately.

WHY v5.77.16 - DUPLICATE CLASSIFICATION FIX (provider_rating corruption)
------------------------------------------------------------------------
The v5.77.15 audit found a real state bug. In _get_enriched_quote_impl the
final block ran:

    _compute_scores_canonical_first(merged)
    _apply_phase_dd_enhancements(merged)   # already classifies (has_scores)
    _compute_recommendation(merged)        # classifies AGAIN

_apply_phase_dd_enhancements() calls _classify_recommendation_8tier() when
scores are present, and _compute_recommendation() is just a thin wrapper
that calls the same classifier — so the classifier ran TWICE per row.

The corruption: _classify_recommendation_8tier Step 2b captures the upstream
provider rating from row["recommendation"] into provider_rating, guarded by
"only if provider_rating is still blank". That guard is enough when the
provider actually supplied a rating (captured on pass 1, skipped on pass 2).
But when the provider supplied NO rating:
  - Pass 1: provider_rating stays blank; Step 4 writes the ENGINE's
    recommendation into row["recommendation"] (e.g. "BUY").
  - Pass 2: Step 2b reads row["recommendation"] == "BUY" (the engine's own
    output), sees provider_rating still blank, and captures "BUY" as
    provider_rating.
Result: Provider Rating on the dashboard showed an engine-generated value
masquerading as a real provider signal. Reproduced before the fix:
    after _apply_phase_dd_enhancements : rec=BUY  provider_rating=(blank)
    after _compute_recommendation      : rec=BUY  provider_rating=BUY   <-- bug

THE FIX (three parts, defense-in-depth)
  1. Removed the redundant _compute_recommendation(merged) call in
     _get_enriched_quote_impl. Classification now happens exactly once.
  2. _apply_phase_dd_enhancements() now calls the classifier UNCONDITIONALLY
     (it was gated behind has_scores; the removed _compute_recommendation
     previously provided the always-run guarantee for the rare "data but no
     computed scores" row). _derive_views / _build_top_factors_and_risks
     stay gated on has_scores; the classifier internally handles the
     no-score case via its Step 3d HOLD fallback.
  3. Hardened _classify_recommendation_8tier so the provider_rating capture
     is idempotent: it now records the prior recommendation_source before the
     Step 2a self-clear and refuses to capture row["recommendation"] as
     provider_rating when that source is engine-written (see
     _ENGINE_WRITTEN_RECO_SOURCES). So even if the classifier is ever invoked
     twice again (from any call path), it will not mistake the engine's own
     recommendation for a provider rating.

After the fix, a row with no provider rating keeps provider_rating blank
through any number of classifier passes, and a row WITH a genuine provider
rating (e.g. STRONG_BUY) retains it while the engine recommendation is
computed independently.

No schema changes. No contract changes. No math changes. The v5.77.14
fundamentals picker fix and the v5.77.15 master-switch guard + score-based
local fallback are all preserved. Provider symbol routing remains
intentionally NOT added (providers self-normalize — see v5.77.15 block); the
.NS/.T/.KS/.DE/.AX EODHD suffix gap is a core.symbols.normalize change tracked
separately.

WHY v5.77.15 - AUDIT FOLLOW-UP: MASTER GUARD + LOCAL SCORE FALLBACK
------------------------------------------------------------------
v5.77.15 applies two of the three fixes from the v5.77.14 audit. The third
audit item (provider symbol routing) was investigated and deliberately NOT
applied — see the "REJECTED" note below for the evidence.

  Fix 1 - YAHOO MASTER-SWITCH GUARD (doc/code mismatch)
    The v5.77.14 docs (and the v5.77.14 operational note) said the Yahoo
    enrichment pass is gated by ENGINE_YAHOO_ENRICHMENT_ENABLED. It was not:
    `_yahoo_enrichment_enabled()` existed but was never called anywhere, so
    setting that env var to 0/false/off did NOT disable enrichment. (The
    sub-flags ENGINE_YAHOO_ENRICH_ON_MISSING_INDUSTRY /
    ENGINE_YAHOO_ENRICH_ON_MISSING_RISK_METRICS were honored inside
    _row_needs_yahoo_enrichment, but the master switch was dead code.)
    v5.77.15 adds `if not _yahoo_enrichment_enabled(): return row` as the
    first runtime statement of `_apply_yahoo_enrichment_pass`, so the
    documented master switch is now real and short-circuits the whole pass
    before any needs-check or provider round-trip. NOTE: this also corrects
    the v5.77.14 operational note — with the switch now wired, leaving it
    UNSET (or "1"/"true") keeps enrichment ON, which is what the v5.77.14
    fundamentals fix needs; only an explicit 0/false/off disables it.

  Fix 2 - SCORE-BASED LOCAL RECO FALLBACK (less conservative degraded mode)
    `_classify_recommendation_8tier` Step 3c previously stamped HOLD /
    source="scoring_unavailable" whenever the engine path (Step 3b via
    core.scoring.apply_canonical_recommendation) produced no recommendation.
    But by that point _compute_scores_canonical_first /
    _compute_scores_local_fallback have usually already produced a real
    overall_score. Flattening a genuinely strong or weak row to HOLD throws
    that signal away. v5.77.15 inserts a score-based map BEFORE the HOLD
    fallback (Step 3c), using the audit's thresholds:
        >=85 STRONG_BUY  >=70 BUY  >=60 ACCUMULATE  >=50 HOLD
        >=40 REDUCE      >=30 SELL  <30 STRONG_SELL
    tagged source="engine_local_score" (visible in the Reco Source column),
    with priority bands mirroring the provider-override philosophy
    (STRONG_SELL high-priority P1; SELL/REDUCE P5). The conservative HOLD
    (now Step 3d, source="scoring_unavailable") still fires only when there
    is genuinely no usable overall_score. This is a fallback-only change: in
    production the engine path (Step 3b) is available, so this path is not
    even reached — it only improves the degraded mode.

  REJECTED (with evidence) - Audit item: route _fetch_patch /
    _fetch_history_patch through `_provider_symbol_for()`.
    The audit observed that `_provider_symbol_for()` is defined but unused
    and that `_fetch_patch` calls providers with the raw symbol, and inferred
    a symbol-routing bug. Investigation of the provider modules shows the
    opposite: EVERY provider self-normalizes the raw symbol at its own entry
    point, each with provider-specific context the engine's generic helper
    does not carry —
        * eodhd_provider.fetch_quote -> normalize_eodhd_symbol(...) ->
          to_eodhd_symbol(sym, default_exchange=_default_exchange())
        * finnhub_provider.fetch_quote -> normalize_finnhub_symbol(...) ->
          to_finnhub_symbol(...)
        * tadawul_provider.fetch_quote_patch -> normalize_ksa_symbol(...)
        * argaam_provider.get_quote/fetch_quote -> normalize_ksa_symbol(...)
    Passing the RAW symbol is therefore the correct contract. Routing through
    the engine's `_provider_symbol_for` would DOUBLE-normalize: e.g. the
    engine would call to_eodhd_symbol(sym) WITHOUT default_exchange, then
    EODHD would call to_eodhd_symbol(...) WITH default_exchange on the result
    — divergent output and a real risk of breaking the international tickers
    that currently price correctly (.PA/.SW/.ST/.CO/.SA/.TW/.TO/.MX). The
    genuine international-symbol gap the audit is worried about (.NS/.T/.KS/
    .DE/.AX returning no EODHD price) is a `to_eodhd_symbol` suffix-mapping
    issue inside core.symbols.normalize, and the fix belongs THERE — it then
    flows through automatically because EODHD already calls to_eodhd_symbol
    internally. That normalize.py change is tracked as a separate delivery
    and is intentionally out of scope for this engine file. (Yahoo is the one
    provider the engine pre-converts, via `_yahoo_symbol_for` in the two
    dedicated Yahoo helpers; that is pre-existing, near-idempotent, and
    unchanged here.)

No schema changes. No contract changes. No math changes. The only behavioral
deltas from v5.77.14 are the two fixes above; the v5.77.14 fundamentals
picker fix and all v5.77.6-v5.77.13 behavior are preserved.

WHY v5.77.14 - FUNDAMENTALS ACQUISITION FIX (the universal-SELL root cause)
---------------------------------------------------------------------------
A deployed refresh produced a wall of REDUCE/SELL/AVOID across ~90 equity
rows: every overall_score 11-53, every row tagged
`opportunity_source = momentum_only_fallback`, every row carrying a
`scoring_errors` entry of insufficient_scoring_inputs /
forecast_skipped_unavailable, and an overall_penalty_factor of 0.70-0.83.
Fundamentals (market_cap, pe_ttm, eps_ttm, dividend_yield, revenue_ttm,
margins, pb_ratio, ps_ratio, ev_ebitda, peg_ratio, beta_5y, float_shares)
were blank on every row except three EODHD-shaped rows.

ROOT CAUSE
----------
This is NOT a scoring or classifier bug. The scores collapse because the
rows reach scoring.py with price-only data and no fundamentals -> the
completeness penalty fires and opportunity falls back to momentum-only ->
scores land in the REDUCE/SELL band. The recommendation logic is then
faithfully reporting a genuinely fundamentals-starved row.

Fundamentals never arrive because the engine's ONLY fundamentals source at
runtime is the Yahoo enrichment pass, and that pass could not find the
provider's entry point. core.providers.yahoo_fundamentals_provider exposes
its module-level callable as `fetch_fundamentals_patch`, but
`_fetch_yahoo_fundamentals_patch()` asked `_pick_yahoo_callable()` only for
get_fundamentals / fetch_fundamentals / fundamentals (+ *_async). None of
those names exist on the module, so the picker returned None, the pass was
a silent no-op, and fundamentals stayed blank on every equity row. (EODHD's
fetch_quote returns price-only; its fundamentals live in a separate
fetch_fundamentals method the engine's quote-only provider picker never
calls - so Yahoo is the sole fundamentals path today.)

THE FIX
-------
Add the provider's real entry point `fetch_fundamentals_patch` (and the
combined `fetch_enriched_quote_patch`) as the FIRST candidates in the
`_pick_yahoo_callable()` name list inside `_fetch_yahoo_fundamentals_patch`.
This is the one functional change in v5.77.14. With it, the Yahoo
fundamentals pass resolves, fundamentals populate, the completeness penalty
stops firing on data-available rows, and recommendations spread back across
the full vocabulary instead of collapsing to SELL.

OPERATIONAL NOTE
----------------
The pass is still gated by ENGINE_YAHOO_ENRICHMENT_ENABLED. If that env var
is set to 0/false/off, this fix does nothing because the entire enrichment
pass is skipped before the picker runs. Confirm it is enabled (or unset) in
the deploy environment.

No schema changes. No contract changes. No math changes. All v5.77.6
through v5.77.13 behavior preserved; the only delta from v5.77.12 is the
two added candidate names in the Yahoo fundamentals picker (plus this WHY
block, the version bump, and the health() valuation_model version string).

WHY v5.77.12 - RECOMMENDATION/DETAIL DIVERGENCE: THE REAL ROOT CAUSE
-------------------------------------------------------------------
After v5.77.11 deployed to Render, a refresh produced rows like:

  NWSA.US  Recommendation=REDUCE  Reason="HOLD: Insufficient provider data; ...
                                          recommendation suppressed / not actionable."
                                  Recommendation Detail=HOLD
                                  Reco Source=empty_row     Provider Rating=STRONG_BUY
                                  Overall Score=54.67       Rank (Overall)=281
  ITUB.US  Recommendation=SELL    Reason="HOLD: Insufficient ..."
                                  Recommendation Detail=HOLD
                                  Reco Source=empty_row     Provider Rating=HOLD
                                  Overall Score=49.10
  TELIA.ST Recommendation=SELL    Reason="HOLD: Insufficient ..."
                                  Reco Source=empty_row     Provider Rating=STRONG_BUY
                                  Overall Score=43.12

Same pattern on every row: a real `recommendation` value (REDUCE/SELL/HOLD)
mapped from `overall_score` (<50 -> SELL, 50-65 -> REDUCE, 66+ -> HOLD),
but `recommendation_detail`, `recommendation_reason`, and `recommendation_source`
all carried the empty-row stamp. The audit consensus has been pointing at
`analysis_sheet_rows.py` as the source — and that route DOES have its own
score->recommendation logic — but the engine itself was setting the stage
for the divergence.

ROOT CAUSE (engine side)
------------------------
`_is_empty_data_row()` returned True whenever `_EMPTY_ROW_FUNDAMENTAL_KEYS`
(market_cap, revenue_ttm, eps_ttm, pe_ttm) were ALL missing, regardless of
how much price + derived-technical data the row carried. For less-popular
tickers where EODHD ships price+OHLC+history but no fundamentals (NWSA,
IWR, FULT, EWG, CI, EIX, ET, 1398.HK, BARC.L, TELIA.ST, BNS.TO, ...) this
fired on every row. That branch routed through `_mark_row_as_empty()`,
which stamps recommendation_source="empty_row", recommendation="HOLD",
recommendation_detailed="HOLD", recommendation_reason="HOLD: Insufficient
provider data; recommendation suppressed / not actionable." — and skips
the full scoring pipeline.

But the canonical scoring path (scoring.py) still produced a real
overall_score from the price+RSI+volatility data we DO have. Downstream
post-processing then mapped that score back into a `recommendation`
field — but didn't touch the related detail/reason/source/priority
fields the engine had already stamped. The result was the visible
divergence.

THE FIX
-------
A row is empty only when it has NOTHING — no price, no fundamentals,
AND no derived technicals. The previous `if fund_pop == 0: return True`
short-circuit is removed. Rows with price + technicals now flow through
the full pipeline (_compute_scores_canonical_first ->
_apply_phase_dd_enhancements -> _compute_recommendation), and the v5.77.6
classifier's Step 2a clears any stale recommendation fields before
writing a consistent set on Step 4. The fundamentals-exempt branch
(FX / commodities / ETFs / funds) is preserved as a no-op for
documentation.

EXPECTED EFFECT AFTER DEPLOY
----------------------------
On the next refresh, equity rows with price+derived data but no
fundamentals should show:
  - Recommendation, Recommendation Detail, Reason: all consistent
    (e.g. all reflect REDUCE / SELL / HOLD as classified by overall_score)
  - Reco Source: a real source ("composite_canonical" / "rules" / etc.),
    NOT "empty_row"
  - Warnings: no "empty_row_no_provider_data" tag
  - "rank_skipped_no_overall_score" warning should disappear because
    the row will have a real overall_score before _apply_rank_overall runs

No schema changes. No contract changes. All v5.77.6 through v5.77.11
fixes preserved.

WHY v5.77.11 - DYNAMIC DIAGNOSTIC LABELS (COSMETIC, NO FUNCTIONAL CHANGE)
------------------------------------------------------------------------
v5.77.11 addresses the lone cosmetic note from the v5.77.10 audit: the
CLASSIFIER and RANK diagnostic log labels were hard-coded as
`[v5.77.6 CLASSIFIER]` and `[v5.77.6 RANK]` since their introduction.
Every version bump from v5.77.7 through v5.77.10 widened the gap between
the module-load banner version and the diagnostic label version, making
Render logs read confusingly like

    [engine_v2 v5.77.10] module loaded; canonical_schema=107
    [v5.77.6 RANK] total=140 scored=140 skipped_no_score=0

The labels are now interpolated from __version__, so they automatically
track the deployed engine version. After v5.77.11 deploys you'll see:

    [engine_v2 v5.77.11] module loaded; canonical_schema=107
    [v5.77.11 RANK] total=140 scored=140 skipped_no_score=0
    [v5.77.11 CLASSIFIER] sym=AAPL rec=BUY detail=BUY src=... band=P2 ...

Why dynamic instead of literal `v5.77.11`: future version bumps no longer
need to remember to update these strings. The historical WHY blocks below
still reference [v5.77.6 CLASSIFIER] / [v5.77.6 RANK] because those
identify WHEN the diagnostics were added — that's accurate v5.77.6
history, not a current-runtime label.

No functional change. All v5.77.6 through v5.77.10 behavior preserved.

WHY v5.77.10 - YAHOO NEEDS-CHECK FULLY NARROWED TO CANONICAL CORE FUNDAMENTALS
-----------------------------------------------------------------------------
v5.77.10 closes the perf gap the v5.77.9 audit caught. v5.77.9 narrowed the
Yahoo needs-check by removing engine-computed forecast fields, but the post-
v5.77.9 audit pointed out it still left in nine Yahoo-only fields that:
  - are NOT in the 107-field canonical schema, and
  - are NEVER populated by non-Yahoo providers (EODHD, Finnhub, Tadawul,
    Argaam, etc.) so they're persistently missing on every row.

Because _row_needs_yahoo_enrichment() uses any(), one persistently-missing
field is enough to flip `needs_fund` to True on every refresh. With those
nine fields included, EVERY row triggered a Yahoo fundamentals call — even
rows with fully-populated core fundamentals — defeating the v5.77.9 fix.

The nine offenders removed in v5.77.10:
  shares_outstanding, eps_forward, roe, roa, earnings_growth_yoy,
  target_mean_price, target_high_price, target_low_price, analyst_count

The v5.77.10 needs-check is now exactly 24 canonical core fundamentals that
any reasonable provider supplies (identity/classification, market structure
floats, P/E + EPS + dividend, margins, debt, revenue, cash flow, P/B + P/S +
PEG + EV/EBITDA). When the row has these populated, Yahoo isn't called.

The FILTER list (`_YAHOO_FUNDAMENTAL_FIELDS`, broad) is unchanged — Yahoo
analyst-target enrichment etc. still flows through when Yahoo IS called.

VERIFICATION
------------
A realistic EODHD-style row (full core fundamentals, no Yahoo-only extras)
now returns `needs_fund=False`. The v5.77.9 synthetic-overpopulated test
that "passed" was misleading; the v5.77.10 test uses a realistic shape.

DEPLOYMENT
----------
After deploy, the Render startup log should show:
  [engine_v2 v5.77.10] module loaded; canonical_schema=107

All v5.77.6 / v5.77.7 / v5.77.8 / v5.77.9 fixes are preserved.

WHY v5.77.9 - YAHOO NEEDS-CHECK NARROWED + ROUTE-DISCIPLINE DOCS
---------------------------------------------------------------
v5.77.9 closes the last performance footnote from the v5.77.8 audit and
hardens the engine's docs against a route-side trap that no engine-side
fix can eliminate.

  Fix 1 - YAHOO ENRICHMENT OVER-TRIGGER (perf regression introduced in v5.77.8)
    To make Yahoo enrichment actually fill data, v5.77.8 widened
    _YAHOO_FUNDAMENTAL_FIELDS to include canonical forecast names
    (forecast_price_1m / forecast_price_3m / forecast_price_12m, plus
    expected_roi_*m and forecast_source). That was correct for the
    post-canon FILTER pass — those names are how the Yahoo response lands
    after _canonicalize_provider_row() runs.
    BUT _row_needs_yahoo_enrichment() uses the same whitelist to decide
    whether to call Yahoo at all. Forecast fields are engine-computed
    LATER in the pipeline (by _phase_ii_quality_forecast inside
    _apply_phase_dd_enhancements), so they're guaranteed missing at
    enrichment time. The result: every row reports `needs_fund=True`
    regardless of whether actual fundamentals are missing, and Yahoo
    gets a round-trip per symbol on every refresh of a 140-row page.
    Fix: introduce _YAHOO_FUNDAMENTAL_NEEDS_CHECK_FIELDS — the same
    whitelist with forecast / ROI / forecast_source removed — and use it
    only in the needs-check. The full _YAHOO_FUNDAMENTAL_FIELDS set still
    gates the filter pass. Net effect: Yahoo is called only when actual
    provider-sourced fundamentals are missing.

  Doc 1 - ROUTE-DISCIPLINE WARNING ON ENGINE ALIASES
    The v5.77.8 audit's remaining caveat: even after `get_engine()` syncs
    `ENGINE` / `engine` / `_ENGINE`, a route doing
        from core.data_engine_v2 import ENGINE
    at module load captures `None` and never sees the update. This is
    Python import semantics, not an engine bug — no fix exists on this
    side. v5.77.9 adds an unmissable comment block on the alias
    declarations explaining the trap, showing the correct async + sync
    access patterns, and explicitly calling out the import form to AVOID.
    Anyone reading the engine to learn how to use it will see this
    warning at the same time they see the names.

DEPLOYMENT
----------
After deploy, the Render startup log should show:
  [engine_v2 v5.77.9] module loaded; canonical_schema=107

All v5.77.6 / v5.77.7 / v5.77.8 fixes are preserved. The CLASSIFIER + RANK
diagnostic logs still emit; per-symbol enrichment failures still warn-log
and produce degraded-but-projectable rows.

WHY v5.77.8 - YAHOO CANONICALIZATION + ENGINE ALIAS SYNC + DEGRADED-ROW DEFAULTS
-------------------------------------------------------------------------------
v5.77.8 addresses four polish items the post-v5.77.7 audit caught. None of them
were runtime blockers, but each one weakened the engine's effective behavior:

  Fix 1 - YAHOO ENRICHMENT EFFECTIVENESS (silent no-op for most fields)
    v5.77.7 fixed the Yahoo TypeError chain, but the enrichment pass still
    filtered the RAW Yahoo response (camelCase: `marketCap`, `trailingPE`,
    `targetMeanPrice`, `shortName`) against a whitelist using CANONICAL names
    (`market_cap`, `pe_ttm`, `target_mean_price`, `name`). Nearly every key
    Yahoo returned was discarded before reaching the row. Fix: run each
    patch through `_canonicalize_provider_row()` first — the same path
    every other provider uses — so the whitelist sees snake_case keys.
    Effect: Yahoo fundamentals + chart enrichment now actually fills the
    fields it's supposed to fill.

  Fix 2 - MODULE-LEVEL ENGINE ALIAS DRIFT (legacy imports got permanent None)
    `ENGINE = _ENGINE_INSTANCE` / `engine = _ENGINE_INSTANCE` / `_ENGINE = _ENGINE_INSTANCE`
    were bound at module load time, when `_ENGINE_INSTANCE` was still None.
    They never updated when `get_engine()` later instantiated the engine, so
    any route doing `from core.data_engine_v2 import ENGINE` got a permanent
    `None`. Fix: `get_engine()` now updates the aliases under the same lock
    that creates `_ENGINE_INSTANCE`; `close_engine()` clears them. The
    canonical accessor is still `await get_engine()` (or
    `get_engine_if_ready()` from sync code), but the alias footgun is gone.

  Fix 3 - DEGRADED-ROW SPARSENESS (per-symbol failures left columns blank)
    v5.77.7's degraded-row payload had only symbol / provider / warnings /
    timestamps. When projected through the 107-column schema, the
    Recommendation / Reco Source / Reco Reason / Priority / Band /
    Confidence Bucket columns came out empty. Fix: degraded rows now
    carry `recommendation = "HOLD"`, `recommendation_source = "enrichment_failed"`,
    `recommendation_reason = "HOLD: quote enrichment failed; not actionable."`,
    `recommendation_priority = 4`, `recommendation_priority_band = "P4"`,
    `confidence_bucket = "LOW"`. risk_bucket / risk_score stay None — we
    don't fabricate a risk classification from no data.

  Fix 4 - CLASS DOCSTRING CONSISTENCY (cosmetic)
    The DataEngineV5 class docstring still read "(v5.77.6)" after the
    v5.77.7 bump. Updated to "(v5.77.8)".

DEPLOYMENT
----------
After deploy, the Render startup log should show:
  [engine_v2 v5.77.8] module loaded; canonical_schema=107

The v5.77.6 diagnostic logs (CLASSIFIER, RANK) still emit as designed.
The v5.77.7 warning log on per-symbol enrichment failures still fires.

WHY v5.77.7 - YAHOO ENRICHMENT + SINGLEFLIGHT + BATCH FAILURE ISOLATION
----------------------------------------------------------------------
v5.77.7 fixes five runtime issues an external audit caught in v5.77.6.
All v5.77.6 patches (live-quote freshness, zero-forecast guard, 3M-only
provider target tagging, CLASSIFIER+RANK diagnostics, sector-map hoisting)
are preserved. The fixes:

  Fix 1 - YAHOO ENRICHMENT SIGNATURE MISMATCHES (TypeErrors on every call)
    v5.77.6 had four broken Yahoo call sites:
      * `_import_yahoo_provider_module(module_basename: str)` was called
        with no arg in both fetch helpers — TypeError.
      * `_yahoo_symbol_for(symbol)` was called as `_yahoo_symbol_for(symbol, page)`
        — TypeError.
      * `_filter_patch_to_missing_fields(row, patch, candidate_fields)` returns
        a (filtered, filled) tuple; v5.77.6 called it with 2 args and used the
        tuple as a dict in self._merge — TypeError.
      * `_row_needs_yahoo_enrichment(row)` returns Tuple[bool, bool]; v5.77.6
        used it as a bare boolean. `bool((False, False))` is True (non-empty
        tuple), so the early-return never fired and the chart pass always ran.

    Fix: pass module basenames ("yahoo_fundamentals_provider" /
    "yahoo_chart_provider"); drop the page arg from _yahoo_symbol_for; pass
    the appropriate _YAHOO_*_FIELDS whitelist; unpack the (filtered, filled)
    return; unpack (needs_fund, needs_chart) and re-evaluate needs_chart
    after the fundamentals pass.

  Fix 2 - SINGLEFLIGHT DEADLOCK on concurrent same-key calls
    SingleFlight.do() did `return await task` while holding self._lock.
    The task's `finally` block needs the same lock to pop _inflight; two
    concurrent requests for the same key hung forever. Fix: take the lock
    just long enough to find-or-create the task, release it, then await
    outside the lock. Same coalescing semantics, no deadlock.

  Fix 3 - BATCH QUOTE FAILURE ISOLATION
    get_enriched_quotes() used asyncio.gather(..., return_exceptions=False),
    so one bad symbol dropped the whole page refresh. Now uses
    return_exceptions=True, logs each failure, and emits a degraded row
    tagged warnings="enrichment_failed:<ExceptionClassName>" so the
    dashboard sees the full 140-symbol response.

DEPLOYMENT
----------
After deploy, the Render startup log should show:
  [engine_v2 v5.77.7] module loaded; canonical_schema=107

The two v5.77.6 diagnostic logs (CLASSIFIER, RANK) still emit as designed.

WHY v5.77.6 - LIVE-QUOTE FRESHNESS + FORECAST GUARDS + DIAGNOSTIC SURFACE
------------------------------------------------------------------------
v5.77.6 is a targeted correctness patch over v5.77.5. Four small,
high-signal fixes plus three diagnostic hooks. No contract changes, no
math changes outside the four named bugs, no schema changes.

  Fix 1 - STALE-MERGE BUG in get_sheet_rows external-rows path.
    The external-rows hydration loop merged a fresh live quote into
    the sheet row via `_merge_missing_fields(merged, quote_map[sym])`.
    That helper only overwrites fields where the destination is blank
    or None - but the destination row (read from the external sheet
    reader) already had yesterday's stale values for price, score,
    recommendation, forecast, and timestamp. `_merge_missing_fields`
    preserved those stale values and the dashboard never refreshed
    on subsequent passes. Fix is a new `_overwrite_live_fields`
    helper that performs a whitelist-based overwrite: any field in
    `_V577_LIVE_OVERWRITE_FIELDS` (engine-owned fields like price,
    volume, score, view, recommendation, forecast, timestamp, provider
    metadata) is overwritten unconditionally when the live row has a
    non-blank value. Manually-edited fields (position_qty, avg_cost,
    position_cost, position_value, unrealized_pl, unrealized_pl_pct)
    remain protected via the standard _merge_missing_fields semantics
    in earlier steps of the same loop.

  Fix 2 - ZERO/NEGATIVE forecast price passing the populated check.
    `_phase_ii_quality_forecast` guarded the provider-target branch
    with `row.get("forecast_price_12m") is not None`. If an upstream
    provider returned `forecast_price_12m: 0` (or a negative value
    from a malformed feed), the check passed and Phase-II derived
    short-horizon forecasts of 0 / negative - producing implied ROI
    of -100% which flowed through to recommendation and rank scoring.
    Fix is a new `_forecast_price_is_populated` helper that treats
    None, zero, and negative as "not populated" and is used at all
    three forecast-price assignment guards (12m provider branch,
    1m / 3m derivation, and the equivalent guards in
    `_compute_scores_local_fallback`).

  Fix 3 - PROVIDER-TARGET SOURCE TAG missed when only 3M target present.
    `_canonicalize_provider_row` only tagged forecast_source =
    "provider_target" when `out.get("forecast_price_12m") is not None`.
    Providers supplying only a 3M target (rarer but real for some
    sector specialists) had their data overwritten by Phase-II
    synthesis on the next pass because the tag was never set.
    Fix is widening the outer guard to fire when EITHER
    forecast_price_12m OR forecast_price_3m is populated. The alias
    detection loop already handles both 12M and 3M provider-target
    alias lists; only the entry guard was over-restrictive.

  Diag - THREE INFO-level log lines to make the production engine
    self-diagnose:
      * `[engine_v2 v5.77.6] module loaded; canonical_schema=107`
        emitted once at module load so operators can verify via
        Render logs whether v5.77.6 is actually live.
      * `[v5.77.6 CLASSIFIER]` - emitted once per row from
        `_classify_recommendation_8tier` after the Step 4 atomic
        write. Surfaces sym / rec / detail / source / band / prio.
      * `[v5.77.6 RANK]` - emitted once per call from
        `_apply_rank_overall` with total / scored / skipped counts.
    All three are gated behind logger.isEnabledFor(INFO) so they
    cost nothing when the log level is WARNING or higher.

PRESERVED - strictly:
  All v5.77.5 / v5.77.4 / v5.77.3 / v5.77.2 / v5.77.1 / v5.77.0 /
  v5.76.0 / v5.75.0 architectural behavior. No API shape changes. No
  data_quality semantics changes. 107-field canonical schema
  unchanged. v5.77.0 intrinsic-value calibration, Phase-II forecast
  weights, LRU cache, sanitization bounds, dividend yield ceiling,
  recommendation_reason prefix rewrite on collapse all preserved.
  v5.77.1 double-checked locking on _PROVIDER_EXECUTOR, multiplicative
  RSI dampening, forecast_source schema field preserved. v5.77.2
  _norm_key_loose alias matching, fallback source tagging, 1M/3M
  derivation from provider 12M target, Yahoo provider pool routing
  preserved. v5.77.3 forecast_source in _INSTRUMENT_CANONICAL_REQUIRED_KEYS,
  provider_target_capped warning tag, _finalize_payload schema-count
  telemetry preserved. v5.77.4 INSTRUMENT_SHEETS check in telemetry
  preserved. v5.77.5 docstring-only fix preserved. v5.75.0 name-fallback
  fix (returns "" not symbol for unknowns), once-only provider_rating
  capture, classifier self-clear at Step 2a all preserved. v5.76.0
  8-tier vocabulary, _V576_SHEET_COMPAT_COLLAPSE, opt-in
  TFB_COLLAPSE_RECOMMENDATION_TO_6TIER preserved.

================================================================================
[Older WHY blocks (v5.77.5 through v5.47.2 baseline) are preserved in
CHANGELOG.md. All behavior described in those WHY blocks is intact in
the code below.]

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
from collections import OrderedDict
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

__version__ = "5.77.19"

# v5.76.0 cross-stack contract version markers. Kept in lockstep with
# core.scoring v5.7.0 and core.reco_normalize v8.0.0.
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
_V573_RECOMMENDATION_ENUM = frozenset({
    "STRONG_BUY", "BUY", "ACCUMULATE", "HOLD",
    "REDUCE", "SELL", "STRONG_SELL", "AVOID",
})

# v5.76.0: legacy collapse map. Default empty (8-tier first-class).
_V573_LEGACY_COLLAPSE: Dict[str, str] = {}

# v5.76.0: opt-in sheet-compatibility 6-tier collapse.
_V576_SHEET_COMPAT_COLLAPSE: Dict[str, str] = {
    "ACCUMULATE": "BUY",
    "AVOID":      "STRONG_SELL",
}


def _sheet_collapse_to_6tier_enabled() -> bool:
    """v5.76.0: True when TFB_COLLAPSE_RECOMMENDATION_TO_6TIER is set."""
    raw = os.getenv("TFB_COLLAPSE_RECOMMENDATION_TO_6TIER", "")
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on", "enabled")


def _v573_trust_provider_reco() -> bool:
    raw = os.getenv("TFB_TRUST_PROVIDER_RECO", "")
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on", "enabled")


def _v573_collapse_to_canonical_enum(value: Any) -> str:
    """v5.76.0: Canonicalize to the 8-tier vocabulary."""
    if value is None:
        return ""
    s = _safe_str(value).strip().upper().replace(" ", "_").replace("-", "_")
    if not s:
        return ""
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
    """v5.76.0/v5.77.0: Sheet-compatibility 6-tier output collapse."""
    if not isinstance(row, dict):
        return
    rec = _safe_str(row.get("recommendation")).upper()
    if rec not in _V576_SHEET_COMPAT_COLLAPSE:
        return  # already 6-tier or unrecognized; no-op.

    collapsed = _V576_SHEET_COMPAT_COLLAPSE[rec]
    row["recommendation"] = collapsed
    row["recommendation_detailed"] = collapsed
    row["recommendation_priority"] = _recommendation_priority(collapsed)
    current_band = _safe_str(row.get("recommendation_priority_band"))
    if rec == "ACCUMULATE" and current_band == "P3":
        row["recommendation_priority_band"] = "P2"
    # v5.77.0: rewrite the leading prefix of recommendation_reason
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
    # v5.77.8: also accept the post-canonicalization forecast_price_*m names
    # alongside the legacy target_*_price names. The alias map collapses
    # targetMeanPrice / targetHighPrice / targetMedianPrice into
    # forecast_price_12m at canonicalization, so without these the filter
    # silently drops the analyst-target enrichment Yahoo just delivered.
    "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
    "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "forecast_source",
    "target_mean_price", "target_high_price", "target_low_price",
    "analyst_count", "recommendation",
)

# v5.77.9 + v5.77.10: NARROWER subset used by _row_needs_yahoo_enrichment() to
# decide whether to call Yahoo for fundamentals. The full _YAHOO_FUNDAMENTAL_FIELDS
# set (above) is correct for the post-canon FILTER pass — it lists every field
# the filter is willing to accept from a Yahoo response. But it's the WRONG set
# for the needs-check.
#
# v5.77.9 removed engine-computed forecast fields (forecast_price_*m,
# expected_roi_*m, forecast_source) — those are produced LATER in the pipeline
# by _phase_ii_quality_forecast and are intentionally absent at enrichment time.
#
# v5.77.10 goes further: also removes Yahoo-only fields that are NOT in the
# 107-field canonical schema and are NEVER populated by non-Yahoo providers
# like EODHD / Finnhub / Tadawul / Argaam. The post-v5.77.9 audit caught that
# the v5.77.9 list still included nine such fields:
#     shares_outstanding, eps_forward, roe, roa, earnings_growth_yoy,
#     target_mean_price, target_high_price, target_low_price, analyst_count
# Because _row_needs_yahoo_enrichment uses any(), a single persistently-missing
# field is enough to flip needs_fund to True on every refresh. With those nine
# included, EVERY row triggered a Yahoo call — even rows with fully-populated
# core fundamentals — defeating the entire v5.77.9 perf fix.
#
# The list below is the auditor's recommendation: 24 canonical core fundamentals
# that any reasonable provider supplies. If these are populated, the row has
# enough data; Yahoo isn't called. The FILTER list below still accepts all the
# Yahoo extras when Yahoo IS called, so legitimate analyst-target enrichment
# still flows through.
_YAHOO_FUNDAMENTAL_NEEDS_CHECK_FIELDS: Tuple[str, ...] = (
    "industry", "sector", "currency", "country", "name",
    "market_cap", "float_shares",
    "pe_ttm", "pe_forward", "eps_ttm",
    "dividend_yield", "payout_ratio", "beta_5y",
    "gross_margin", "operating_margin", "profit_margin",
    "debt_to_equity", "revenue_ttm", "revenue_growth_yoy",
    "free_cash_flow_ttm",
    "pb_ratio", "ps_ratio", "peg_ratio", "ev_ebitda",
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
            for k in _YAHOO_FUNDAMENTAL_NEEDS_CHECK_FIELDS
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


# v5.77.6: hoisted from local-to-function to module-level constants so health()
# can report their sizes. Values unchanged from v5.77.0 / v5.77.5; the only
# difference is scope — both maps were previously defined inside
# `_compute_intrinsic_and_upside`. The function below now references these
# names directly.
_SECTOR_PE_MAP: Dict[str, float] = {
    "technology":              32.0,
    "consumer electronics":    25.0,
    "communication services":  28.0,
    "financial services":      14.0,
    "healthcare":              24.0,
    "consumer defensive":      22.0,
    "consumer cyclical":       22.0,
    "industrials":             20.0,
    "energy":                  14.0,
    "utilities":               18.0,
    "real estate":             25.0,
    "basic materials":         16.0,
}
_SECTOR_PB_MAP: Dict[str, float] = {
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

    # v5.77.6: read from module-level _SECTOR_PE_MAP / _SECTOR_PB_MAP constants
    # (hoisted from local scope so health() can report their sizes).
    fair_pe = _SECTOR_PE_MAP.get(sector, 20.0)

    fair_pb = _SECTOR_PB_MAP.get(sector, 2.0)

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
    "STRONG_BUY":  1,
    "BUY":         2,
    "ACCUMULATE":  3,
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
    """v5.69.0/v5.76.0: integer rank 1 (best) .. 5 (worst)."""
    key = _safe_str(rec).upper()
    return _RECO_8TIER_PRIORITY.get(key, 4)


# v5.77.16: recommendation_source values the ENGINE itself writes. When a row
# carries one of these from a prior classifier pass, its `recommendation` field
# holds an engine-generated value, not an upstream provider rating — so the
# classifier must not re-capture it as provider_rating. ("provider_override" is
# included because in that path provider_rating is already captured before the
# override, so re-capture is both unnecessary and would be a no-op under the
# existing `if not provider_rating` guard.)
_ENGINE_WRITTEN_RECO_SOURCES: frozenset = frozenset({
    "engine",
    "engine_local_score",
    "scoring_unavailable",
    "provider_override",
    "enrichment_failed",
    "empty_row",
})


def _classify_recommendation_8tier(row: Dict[str, Any]) -> None:
    """v5.75.0 — SINGLE AUTHORITATIVE recommendation writer (atomic, idempotent).

    v5.77.6 PATCH 4: appends a gated INFO log line after the Step 4
    atomic write so operators can verify in production whether the
    classifier ran for a given row and what values it produced.
    """
    if not isinstance(row, dict):
        return

    # -- Step 1: empty-row guard ------------------------------------
    if _is_empty_data_row(row):
        _mark_row_as_empty(row)
        return

    # -- Step 2a: SELF-CLEAR (v5.75.0) ------------------------------
    # v5.77.16: capture the prior recommendation_source BEFORE clearing it.
    # If the row's current `recommendation` was written by the engine on an
    # earlier classifier pass (source in _ENGINE_WRITTEN_RECO_SOURCES), the
    # value sitting in row["recommendation"] is NOT an upstream provider
    # rating and must NOT be captured as provider_rating in Step 2b. Without
    # this, a second classifier pass over the same row (e.g. the v5.77.15
    # _apply_phase_dd_enhancements + _compute_recommendation double-call)
    # would mistake the engine's own recommendation for a provider signal and
    # stamp provider_rating = <engine recommendation>. The double-call itself
    # is removed in v5.77.16; this guard additionally makes the capture
    # idempotent so any future re-invocation stays safe.
    _prior_reco_source = _safe_str(row.get("recommendation_source"))
    _reco_is_engine_written = _prior_reco_source in _ENGINE_WRITTEN_RECO_SOURCES

    for _stale_key in (
        "recommendation_detailed",
        "recommendation_detail",
        "recommendation_reason",
        "recommendation_priority",
        "recommendation_priority_band",
        "recommendation_source",
    ):
        row.pop(_stale_key, None)

    # -- Step 2b: provider rating capture (v5.75.0: ONCE-ONLY; v5.77.16:
    #             never from an engine-written recommendation) ------------
    # v5.77.17: provider_canon must reflect a REAL upstream provider rating,
    # never an engine-written one. When the row's recommendation was written
    # by the engine on an earlier pass (_reco_is_engine_written), derive
    # provider_canon from the already-captured provider_rating field instead
    # of from row["recommendation"]. Otherwise an engine-written "BUY" would
    # still flow into provider_canon and — with TFB_TRUST_PROVIDER_RECO=1 —
    # make provider_wins True, letting Step 3a stamp source="provider_override"
    # on what is actually an engine recommendation. v5.77.16 stopped the engine
    # value from being CAPTURED as provider_rating; v5.77.17 also stops it from
    # driving the override decision, so the whole classifier is idempotent even
    # with the trust flag enabled.
    raw_upstream = row.get("recommendation")
    if _reco_is_engine_written:
        provider_canon = _v573_collapse_to_canonical_enum(row.get("provider_rating"))
    else:
        provider_canon = _v573_collapse_to_canonical_enum(raw_upstream)
    if not _safe_str(row.get("provider_rating")) and not _reco_is_engine_written:
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

    # -- Step 3c: score-based local fallback, then conservative HOLD ----
    # v5.77.15: when the engine path (Step 3b) produced no recommendation —
    # because core.scoring is unavailable or apply_canonical_recommendation
    # returned nothing — do NOT blindly stamp HOLD if the row already carries
    # a real overall_score from _compute_scores_canonical_first /
    # _compute_scores_local_fallback. Map that score to the 8-tier vocabulary
    # so a genuinely strong (or weak) row is not flattened to HOLD. The source
    # is tagged "engine_local_score" so this degraded-mode path stays visible
    # in the Reco Source column for diagnostics. Only when there is no usable
    # overall_score do we fall through to the conservative HOLD below.
    if not rec:
        _ov = _as_float(row.get("overall_score"))
        if _ov is not None:
            if _ov >= 85.0:
                rec = "STRONG_BUY"
            elif _ov >= 70.0:
                rec = "BUY"
            elif _ov >= 60.0:
                rec = "ACCUMULATE"
            elif _ov >= 50.0:
                rec = "HOLD"
            elif _ov >= 40.0:
                rec = "REDUCE"
            elif _ov >= 30.0:
                rec = "SELL"
            else:
                rec = "STRONG_SELL"
            source = "engine_local_score"
            reason = (
                f"{rec}: Local score-based classification (overall_score="
                f"{round(_ov, 2)}); core.scoring canonical path unavailable."
            )
            if rec == "STRONG_BUY":
                priority_band = "P1"
            elif rec == "BUY":
                priority_band = "P2"
            elif rec == "ACCUMULATE":
                priority_band = "P3"
            elif rec == "STRONG_SELL":
                priority_band = "P1"
            elif rec in ("SELL", "REDUCE"):
                priority_band = "P5"
            else:
                priority_band = "P4"

    # -- Step 3d: conservative HOLD when there is no usable score -------
    if not rec:
        rec = "HOLD"
        source = "scoring_unavailable"
        reason = "HOLD: core.scoring unavailable and no usable overall_score; conservative fallback applied."
        priority_band = "P4"

    # -- Step 4: write the final row fields atomically --------------
    row["recommendation"] = rec
    row["recommendation_detailed"] = rec
    row["recommendation_source"] = source
    row["recommendation_reason"] = reason
    row["recommendation_priority"] = _recommendation_priority(rec)
    row["recommendation_priority_band"] = priority_band  # may be ""; that is fine

    # -- v5.77.6 PATCH 4a (label made dynamic in v5.77.11): diagnostic log line
    # on Step 4 atomic write. Gated behind INFO level so cost is ~zero at
    # WARNING/ERROR. The label embeds __version__ so it always reflects the
    # deployed engine version (was hard-coded "v5.77.6" through v5.77.10 —
    # see audit note on stale label tags).
    if logger.isEnabledFor(logging.INFO):
        try:
            logger.info(
                "[v%s CLASSIFIER] sym=%s rec=%s detail=%s src=%s band=%s prio=%s prov_rating=%s",
                __version__,
                _safe_str(row.get("symbol") or row.get("requested_symbol"), "?"),
                rec, rec, source, priority_band,
                _recommendation_priority(rec),
                _safe_str(row.get("provider_rating"), ""),
            )
        except Exception:
            pass

    # -- Step 4b (v5.76.0): optional sheet-compatibility 6-tier collapse ----
    if _sheet_collapse_to_6tier_enabled():
        _apply_sheet_compat_collapse(row)

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

    _synthesize_market_cap_if_zero(row)
    _compute_intrinsic_and_upside(row)
    _phase_ii_quality_forecast(row)

    has_scores = any(
        _as_float(row.get(k)) is not None
        for k in ("overall_score", "valuation_score", "quality_score", "momentum_score", "opportunity_score")
    )
    # v5.77.16: classification now runs unconditionally (it internally handles
    # the no-score case via its Step 3d HOLD fallback). Previously it was gated
    # behind has_scores, and the orchestrator's separate _compute_recommendation
    # call supplied the always-run guarantee. With that redundant call removed
    # in v5.77.16, the classifier must run here for every non-empty row so a row
    # that has data but (rarely) no computed scores still gets a recommendation
    # instead of going out blank. _derive_views and _build_top_factors_and_risks
    # still require scores, and _build_top_factors_and_risks must run AFTER the
    # classifier because it reads row["recommendation"] for position_size_hint.
    if has_scores:
        _derive_views(row)
    _classify_recommendation_8tier(row)
    if has_scores:
        _build_top_factors_and_risks(row)

    return row


# =============================================================================
# v5.65.0 PHASE-II — Quality forecast generator
# =============================================================================

_PHASE_II_MAX_12M_ABS_RETURN: float = 0.30
_PHASE_II_RATIO_3M_OF_12M: float = 0.35
_PHASE_II_RATIO_1M_OF_12M: float = 0.12
_PHASE_II_MIN_12M_ABS_RETURN: float = -0.30
_PHASE_II_VOL_BAND_FACTOR: float = 0.5
_PHASE_II_CONF_MIN: float = 0.30
_PHASE_II_CONF_MAX: float = 0.85


def _forecast_price_is_populated(v: Any) -> bool:
    """v5.77.6: treat None / zero / negative as 'missing' for forecast prices.

    The v5.77.5 Phase-II provider-target branch used a bare
    `row.get("forecast_price_12m") is not None` check, which let
    forecast_price_12m == 0 (or a negative artifact) flow through and
    produce -100% derived ROI. This helper consolidates the check so
    every provider-target population gate uses the same definition.
    """
    f = _as_float(v)
    return f is not None and f > 0.0


def _phase_ii_quality_forecast(row: Dict[str, Any]) -> None:
    if not isinstance(row, dict):
        return

    cp = _as_float(row.get("current_price"))
    if cp is None or cp <= 0:
        return

    # v5.77.0/v5.77.2 provider-target preservation, hardened in v5.77.6.
    # If an upstream provider supplied a real analyst 12M target AND we've
    # tagged forecast_source = "provider_target", honor it: do not synthesize
    # over it, but DO derive 1M / 3M (and missing ROI fields) from the
    # capped 12M return so the short-horizon columns stay populated.
    # v5.77.6 fix: use _forecast_price_is_populated() to reject None / 0 /
    # negative values (a 0 forecast_price_12m previously passed the
    # `is not None` check and produced a derived -100% ROI).
    existing_source = _safe_str(row.get("forecast_source")).lower()
    if existing_source == "provider_target" and _forecast_price_is_populated(row.get("forecast_price_12m")):
        fp12 = _as_float(row.get("forecast_price_12m"))
        if fp12 is not None and fp12 > 0:
            return_12m = (fp12 - cp) / cp
            capped_12m = max(min(return_12m, _PHASE_II_MAX_12M_ABS_RETURN), -_PHASE_II_MAX_12M_ABS_RETURN)
            if abs(return_12m) - _PHASE_II_MAX_12M_ABS_RETURN > 1e-9:
                _v573_append_warning(row, "provider_target_capped_for_short_horizon_derivation")
            if not _forecast_price_is_populated(row.get("forecast_price_3m")):
                derived_3m_return = capped_12m * _PHASE_II_RATIO_3M_OF_12M
                row["forecast_price_3m"] = round(cp * (1.0 + derived_3m_return), 4)
            if not _forecast_price_is_populated(row.get("forecast_price_1m")):
                derived_1m_return = capped_12m * _PHASE_II_RATIO_1M_OF_12M
                row["forecast_price_1m"] = round(cp * (1.0 + derived_1m_return), 4)
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
        return

    # -------------------------------------------------------------------------
    # v5.77.19 (Fix 3): 3M-only provider target.
    # -------------------------------------------------------------------------
    # The v5.77.18 gate can drop an implausible 12M leg while a plausible 3M leg
    # survives, leaving the row tagged provider_target with ONLY
    # forecast_price_3m populated (this is also the path for a provider that
    # only ever supplied a 3M target). The 12M-only guard above would otherwise
    # fall through to full synthesis and OVERWRITE that surviving provider 3M
    # value. Honor it instead: preserve the provider 3M target, derive the 1M
    # leg from the capped 3M return, and synthesize the missing 12M leg by
    # scaling the capped 3M return up to the 12M horizon (re-capped at +/-30%
    # so a near-band 3M target can't reintroduce an out-of-band 12M forecast).
    if existing_source == "provider_target" and _forecast_price_is_populated(row.get("forecast_price_3m")):
        fp3 = _as_float(row.get("forecast_price_3m"))
        if fp3 is not None and fp3 > 0:
            return_3m = (fp3 - cp) / cp
            cap_3m = _PHASE_II_MAX_12M_ABS_RETURN * _PHASE_II_RATIO_3M_OF_12M
            capped_3m = max(min(return_3m, cap_3m), -cap_3m)
            if abs(return_3m) - cap_3m > 1e-9:
                _v573_append_warning(row, "provider_target_3m_capped_for_short_horizon_derivation")
            if not _forecast_price_is_populated(row.get("forecast_price_1m")):
                # 1M return ~ capped 3M return scaled by (1M:12M)/(3M:12M).
                if _PHASE_II_RATIO_3M_OF_12M > 0:
                    ratio_1m_of_3m = _PHASE_II_RATIO_1M_OF_12M / _PHASE_II_RATIO_3M_OF_12M
                else:
                    ratio_1m_of_3m = 0.0
                derived_1m_return = capped_3m * ratio_1m_of_3m
                row["forecast_price_1m"] = round(cp * (1.0 + derived_1m_return), 4)
            if not _forecast_price_is_populated(row.get("forecast_price_12m")):
                # 12M return ~ capped 3M return scaled UP to the 12M horizon.
                if _PHASE_II_RATIO_3M_OF_12M > 0:
                    derived_12m_return = capped_3m / _PHASE_II_RATIO_3M_OF_12M
                else:
                    derived_12m_return = capped_3m
                derived_12m_return = max(min(derived_12m_return, _PHASE_II_MAX_12M_ABS_RETURN), -_PHASE_II_MAX_12M_ABS_RETURN)
                row["forecast_price_12m"] = round(cp * (1.0 + derived_12m_return), 4)
            if row.get("expected_roi_3m") is None:
                row["expected_roi_3m"] = round(return_3m, 6)
            if row.get("expected_roi_1m") is None:
                fp1 = _as_float(row.get("forecast_price_1m"))
                if fp1 is not None:
                    row["expected_roi_1m"] = round((fp1 - cp) / cp, 6)
            if row.get("expected_roi_12m") is None:
                fp12 = _as_float(row.get("forecast_price_12m"))
                if fp12 is not None:
                    row["expected_roi_12m"] = round((fp12 - cp) / cp, 6)
        return

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

    if intrinsic is not None and intrinsic > 0:
        reversion_return = (intrinsic - cp) / cp
        reversion_return *= 0.6
        components.append((reversion_return, 0.25))

    if momentum is not None:
        trend_return = ((momentum - 50.0) / 50.0) * 0.15
        components.append((trend_return, 0.35))

    fundamentals_signals: List[float] = []
    for sub in (quality, value, growth):
        if sub is not None:
            fundamentals_signals.append((sub - 50.0) / 50.0)
    if fundamentals_signals:
        avg_fund = sum(fundamentals_signals) / len(fundamentals_signals)
        fund_return = avg_fund * 0.10
        components.append((fund_return, 0.25))

    if overall is not None:
        baseline_return = ((overall - 50.0) / 50.0) * 0.08
        components.append((baseline_return, 0.15))

    if not components:
        return

    total_weight = sum(w for _, w in components)
    if total_weight <= 0:
        return
    expected_12m_return = sum(r * w for r, w in components) / total_weight

    expected_12m_return = max(
        _PHASE_II_MIN_12M_ABS_RETURN,
        min(_PHASE_II_MAX_12M_ABS_RETURN, expected_12m_return),
    )

    if expected_12m_return >= 0.95 * _PHASE_II_MAX_12M_ABS_RETURN:
        _v573_append_warning(row, "forecast_capped_at_ceiling")
    elif expected_12m_return <= 0.95 * _PHASE_II_MIN_12M_ABS_RETURN:
        _v573_append_warning(row, "forecast_capped_at_floor")

    expected_3m_return = expected_12m_return * _PHASE_II_RATIO_3M_OF_12M
    expected_1m_return = expected_12m_return * _PHASE_II_RATIO_1M_OF_12M

    # v5.77.1 multiplicative RSI dampening on short-horizon returns only.
    if rsi is not None:
        if rsi > 75:
            if expected_1m_return > 0:
                expected_1m_return *= 0.5
            else:
                expected_1m_return -= 0.015
            if expected_3m_return > 0:
                expected_3m_return *= 0.65
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
            if expected_1m_return < 0:
                expected_1m_return *= 0.5
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
# Domain models
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
# Canonical page contracts — 107-column instrument schema (since v5.77.1)
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
    # v5.77.0 bool guard: prevent True/False from being parsed as 1.0/0.0.
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

    Deliberately does NOT clear row["recommendation"]; the classifier reads
    the upstream provider value from that field to populate provider_rating
    before overwriting with the final engine recommendation.
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
    """Decide whether a row has so little data that the engine should
    short-circuit into `_mark_row_as_empty()` instead of running the full
    scoring + classifier pipeline.

    v5.77.12 fix
    ------------
    Previous versions returned True whenever the row's fundamental keys
    (market_cap, revenue_ttm, eps_ttm, pe_ttm) were all missing — even
    when the row had a complete price + OHLC payload and a full set of
    derived technicals (RSI, volatility, drawdown, Sharpe, VaR). That
    triggered `_mark_row_as_empty()`, which stamped the row with
    `recommendation_source="empty_row"`, `recommendation="HOLD"`,
    `recommendation_detailed="HOLD"`, and
    `recommendation_reason="HOLD: Insufficient provider data; ..."`.

    Downstream, the canonical scoring path still produced a real
    `overall_score` from price+technicals (e.g. 54.67 for NWSA, 49.10
    for ITUB, 43.12 for TELIA), and the route handler / post-processing
    mapped that score to "REDUCE" (50-65) or "SELL" (<50) — writing
    only the `recommendation` field, not the related detail/reason/
    source/priority_band. Result: every row on the deployed dashboard
    had `recommendation=REDUCE` (or SELL) but `recommendation_detail=HOLD`
    and `reason="HOLD: Insufficient..."` — the recommendation/detail
    divergence the audit consensus has been pointing at.

    The fix: a row is empty only when it has NOTHING — no price, no
    fundamentals, and no derived technicals. A row with price+RSI+
    volatility (even if fundamentals are blank) gets the full pipeline,
    and the v5.77.6 classifier's Step 2a auto-clears any stale
    recommendation fields before re-populating them consistently. The
    fundamentals-exempt branch (FX / commodities / ETFs / funds) is
    preserved for documentation; the simpler primary rule already
    covers those cases.
    """
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

    # Truly empty: NO signal anywhere — no price, no fundamentals, no
    # technicals. This catches provider failures, delisted symbols, and
    # genuine garbage rows. Everything else gets the full pipeline.
    if price_pop == 0 and fund_pop == 0 and derived_pop == 0:
        return True

    # Preserved for documentation: FX / commodities / ETFs / funds
    # intentionally don't carry equity fundamentals. Under the v5.77.12
    # rule this branch is a no-op (the rule above already returns False
    # for any row with a populated price or derived metric), but keeping
    # it makes the asset-class intent explicit for future maintainers.
    if _empty_row_fundamentals_exempt(row):
        return False

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
    "pe_ttm":         (0.0, 500.0),
    "pe_forward":     (0.0, 500.0),
    "pb_ratio":       (0.0, 100.0),
    "ps_ratio":       (0.0, 100.0),
    "ev_ebitda":      (0.0, 200.0),
    "peg_ratio":      (0.0, 20.0),
    "debt_to_equity": (0.0, 500.0),
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


# v5.77.3: forecast_source elevated to required canonical key so any
# external schema returning only 106 columns fails the canonical check
# and falls back to the built-in 107-column STATIC contract.
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
    "eps_ttm": ("eps_ttm", "trailingEps", "eps", "earningsPerShare", "epsTTM", "EarningsShare", "epsTtm", "DilutedEPSTTM", "epsTrailingTwelveMonths", "epsCurrentYear"),
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
    "forecast_price_3m": ("forecast_price_3m", "targetPrice3m", "priceTarget3m"),
    "forecast_price_12m": ("forecast_price_12m", "targetPrice12m", "priceTarget12m", "targetMedianPrice", "targetHighPrice", "targetMeanPrice", "targetPrice", "analystTargetPrice", "consensusTarget"),
    "expected_roi_1m": ("expected_roi_1m", "expectedReturn1m", "roi1m"),
    "expected_roi_3m": ("expected_roi_3m", "expectedReturn3m", "roi3m"),
    "expected_roi_12m": ("expected_roi_12m", "expectedReturn12m", "roi12m"),
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
# Suffix -> locale map (v5.67.0 / v5.68.0 / v5.73.0 expansions)
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
    # v5.75.0: returns "" for unknown equities (was: symbol itself), so the
    # Yahoo enrichment pass can fill name from longName / shortName / displayName.
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


# =============================================================================
# v5.77.18 — IMPLAUSIBLE PROVIDER-TARGET GATE helpers
# -----------------------------------------------------------------------------
# An upstream analyst 12M/3M target that is wildly out of band versus the
# current price (real case: UHS target 417 vs price 146 = +185%) would
# otherwise be mapped into forecast_price_12m by _canonicalize_provider_row,
# tagged forecast_source="provider_target", and then honored UNCAPPED by
# _phase_ii_quality_forecast (which writes the raw (fp12-cp)/cp into
# expected_roi_12m and only caps the DERIVED 3M/1M legs). core.scoring then
# clamps that +185% to its max_roi_12m=0.65 ceiling and sub-splits it into the
# saturated +65.0 / +27.3 / +11.7 ROI triplet seen across the dashboard. The
# engine's own Phase-II +/-30% cap (_PHASE_II_MAX_12M_ABS_RETURN) never fires
# because the provider_target branch short-circuits synthesis entirely.
#
# These helpers let _canonicalize_provider_row validate a mapped target against
# the current price BEFORE tagging provider_target. The band is a sane multiple
# of the current price; targets outside it are dropped at ingestion so Phase-II
# synthesis (which IS capped) produces the forecast and scoring reads a sane
# value. This is the single ingestion chokepoint for the bug. Everything is
# env-tunable and read at call time:
#     TFB_PROVIDER_TARGET_MIN_MULT       (default 0.40)
#     TFB_PROVIDER_TARGET_MAX_MULT       (default 2.50)
#     TFB_PROVIDER_TARGET_GATE_ENABLED   (default on; 0/false/no/n/off/f turns
#                                         the gate off and restores the exact
#                                         pre-v5.77.18 tagging behavior)
# Fail-open: if price or target is missing/non-positive, or the configured band
# is degenerate (lo<=0, hi<=0, or lo>=hi), the target is treated as plausible
# (band falls back to the defaults) so a config typo can never blank forecasts.
# =============================================================================
_PROVIDER_TARGET_MIN_MULT_DEFAULT: float = 0.40
_PROVIDER_TARGET_MAX_MULT_DEFAULT: float = 2.50


def _provider_target_gate_enabled() -> bool:
    """v5.77.18: gate master switch. Off only for an explicit falsey value."""
    raw = (os.getenv("TFB_PROVIDER_TARGET_GATE_ENABLED") or "").strip().lower()
    if raw in {"0", "false", "no", "n", "off", "f"}:
        return False
    return True


def _provider_target_band() -> Tuple[float, float]:
    """v5.77.18: (min_mult, max_mult) read at call time; defaults on degenerate config."""
    lo = _get_env_float("TFB_PROVIDER_TARGET_MIN_MULT", _PROVIDER_TARGET_MIN_MULT_DEFAULT)
    hi = _get_env_float("TFB_PROVIDER_TARGET_MAX_MULT", _PROVIDER_TARGET_MAX_MULT_DEFAULT)
    if lo <= 0 or hi <= 0 or lo >= hi:
        return _PROVIDER_TARGET_MIN_MULT_DEFAULT, _PROVIDER_TARGET_MAX_MULT_DEFAULT
    return lo, hi


def _provider_target_is_plausible(current_price: Any, provider_target: Any) -> bool:
    """v5.77.18: True if provider_target lies within [price*min_mult, price*max_mult].

    Fail-open: returns True when price or target is missing or non-positive, so a
    sparse / garbage feed never causes a genuine forecast to be dropped.
    """
    p = _as_float(current_price)
    t = _as_float(provider_target)
    if p is None or t is None or p <= 0 or t <= 0:
        return True
    lo, hi = _provider_target_band()
    return (p * lo) <= t <= (p * hi)


_GATE_DROP_WARNING_TAGS: Tuple[str, ...] = (
    "provider_target_implausible_dropped_12m",
    "provider_target_implausible_dropped_3m",
)


def _merge_gate_drop_warnings(row: Dict[str, Any], patch: Dict[str, Any]) -> None:
    """v5.77.19 (Fix 2): carry provider-target-gate drop tags from a
    canonicalized patch back onto the row.

    The Yahoo enrichment path canonicalizes its patch (which runs the gate),
    then narrows it with _filter_patch_to_missing_fields() against a data-field
    whitelist that does NOT contain "warnings". Without this re-merge the gate
    would drop an implausible Yahoo target but lose the audit trail
    (provider_target_implausible_dropped_12m / _3m). Idempotent: relies on
    _v573_append_warning(), which de-dupes existing tags.
    """
    if not isinstance(row, dict) or not isinstance(patch, dict):
        return
    raw = patch.get("warnings")
    if isinstance(raw, (list, tuple, set)):
        text = "; ".join(_safe_str(x) for x in raw if _safe_str(x))
    else:
        text = _safe_str(raw)
    if not text:
        return
    for tag in _GATE_DROP_WARNING_TAGS:
        if tag in text:
            _v573_append_warning(row, tag)


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

    # v5.77.1: tag forecast_source = "provider_target" when upstream supplied an analyst target.
    # v5.77.2: switched alias matching from naive .lower() to _norm_key_loose() so snake_case
    #          variants ("target_mean_price") collapse to the same key as camelCase
    #          ("targetMeanPrice").
    # v5.77.6: widen outer guard so providers that ship ONLY a 3M target (no 12M anchor)
    #          also get tagged "provider_target" — previously they were silently
    #          overwritten by Phase-II synthesis because the guard required
    #          forecast_price_12m to be populated.
    # v5.77.18: IMPLAUSIBLE PROVIDER-TARGET GATE. Before tagging "provider_target",
    #           validate each mapped target leg against the current price. A target
    #           outside the sane band (see _provider_target_is_plausible) is DROPPED
    #           here at ingestion — its forecast_price_*m is set back to None and a
    #           warning tag recorded — and that leg does NOT contribute the
    #           provider_target tag. With both legs dropped (or absent), forecast_source
    #           stays unset, so _phase_ii_quality_forecast falls through to its capped
    #           +/-30% synthesis instead of honoring the raw out-of-band target. A native
    #           (non-analyst-alias) forecast_price_12m/3m never enters this block's drop
    #           path: has_*_target is computed ONLY from the analyst-target alias lists,
    #           so an engine/other forecast is left untouched and simply does not get the
    #           provider_target tag (unchanged pre-v5.77.18 behavior). With the gate
    #           disabled, both legs "survive" exactly as before and the tag is applied
    #           whenever any analyst-target alias is present — byte-for-byte the old
    #           behavior. UHS check: 417 > 146*2.5=365 -> dropped.
    if (out.get("forecast_price_12m") is not None or out.get("forecast_price_3m") is not None) \
            and not _safe_str(out.get("forecast_source")):
        provider_target_aliases_12m = (
            "targetPrice12m", "priceTarget12m", "targetMedianPrice",
            "targetHighPrice", "targetMeanPrice", "targetPrice",
            "priceTarget", "analystTargetPrice", "consensusTarget",
        )
        provider_target_aliases_3m = (
            "targetPrice3m", "priceTarget3m",
        )
        src_keys_loose = {_norm_key_loose(k): k for k in src.keys()}
        flat_keys_loose = {_norm_key_loose(k): k for k in flat.keys()}

        def _provider_target_alias_present(aliases: Tuple[str, ...]) -> bool:
            for alias in aliases:
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
                        return True
            return False

        has_12m_target = _provider_target_alias_present(provider_target_aliases_12m)
        has_3m_target = _provider_target_alias_present(provider_target_aliases_3m)

        gate_on = _provider_target_gate_enabled()
        gate_price = out.get("current_price")
        if _as_float(gate_price) is None:
            gate_price = out.get("price")

        plausible_target_survives = False

        # 12M leg
        if has_12m_target:
            if gate_on and not _provider_target_is_plausible(gate_price, out.get("forecast_price_12m")):
                if logger.isEnabledFor(logging.INFO):
                    try:
                        logger.info(
                            "[v%s TARGET-GATE] dropped implausible 12M provider target "
                            "sym=%s price=%s target=%s band=%s",
                            __version__,
                            _safe_str(out.get("symbol") or out.get("requested_symbol"), "?"),
                            _safe_str(gate_price, "?"),
                            _safe_str(out.get("forecast_price_12m"), "?"),
                            _safe_str(_provider_target_band(), "?"),
                        )
                    except Exception:
                        pass
                out["forecast_price_12m"] = None
                _v573_append_warning(out, "provider_target_implausible_dropped_12m")
            else:
                plausible_target_survives = True

        # 3M leg
        if has_3m_target:
            if gate_on and not _provider_target_is_plausible(gate_price, out.get("forecast_price_3m")):
                if logger.isEnabledFor(logging.INFO):
                    try:
                        logger.info(
                            "[v%s TARGET-GATE] dropped implausible 3M provider target "
                            "sym=%s price=%s target=%s band=%s",
                            __version__,
                            _safe_str(out.get("symbol") or out.get("requested_symbol"), "?"),
                            _safe_str(gate_price, "?"),
                            _safe_str(out.get("forecast_price_3m"), "?"),
                            _safe_str(_provider_target_band(), "?"),
                        )
                    except Exception:
                        pass
                out["forecast_price_3m"] = None
                _v573_append_warning(out, "provider_target_implausible_dropped_3m")
            else:
                plausible_target_survives = True

        if plausible_target_survives:
            out["forecast_source"] = "provider_target"

    inferred_symbol = out.get("symbol") or normalized_symbol or requested_symbol
    inferred_name = _infer_display_name_from_symbol(inferred_symbol)
    current_name = _safe_str(out.get("name"))
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
    """Fill blank fields in base_row from template_row WITHOUT overwriting populated values.

    Used for cache / snapshot back-fill where the base row is authoritative and the
    template is fallback data. v5.77.6: kept for snapshot/cache merges; the live-quote
    merge in get_sheet_rows() uses _overwrite_live_fields() instead so engine-owned
    fields are refreshed from the current pass.
    """
    out = dict(base_row or {})
    if not isinstance(template_row, dict):
        return out
    for k, v in template_row.items():
        if out.get(k) in (None, "", [], {}) and v not in (None, "", [], {}):
            out[k] = _json_safe(v)
    return out


# =============================================================================
# v5.77.6: live-quote overwrite whitelist + helper
# -----------------------------------------------------------------------------
# Why this exists:
#   The external-rows path in DataEngineV5.get_sheet_rows() previously called
#   _merge_missing_fields() to fold in fresh live-quote data. But that helper
#   only writes blank fields and preserves anything already present in the row
#   read from the Google Sheet. Result: yesterday's stale price, scores,
#   recommendation, forecast, and timestamps survived every refresh cycle,
#   because the sheet row was already "populated" (with stale data).
#
# What this changes:
#   For every field in _V577_LIVE_OVERWRITE_FIELDS, the live quote wins over
#   whatever was in the sheet, even when both are populated. Manual fields the
#   user types into the sheet (position_qty, avg_cost, position_cost,
#   position_value, unrealized_pl, unrealized_pl_pct) are deliberately
#   EXCLUDED — those come from the sheet, not the engine. The snapshot-map and
#   best-snapshot merges in get_sheet_rows() still use _merge_missing_fields()
#   because those are about filling cache gaps, not refreshing live data.
# =============================================================================
_V577_LIVE_OVERWRITE_FIELDS: frozenset = frozenset({
    # Identity / classification (refresh in case provider corrected them)
    "name", "asset_class", "exchange", "currency", "country", "sector", "industry",

    # All price + volume + 52-week fields
    "current_price", "previous_close", "open_price", "day_high", "day_low",
    "week_52_high", "week_52_low", "price_change", "percent_change",
    "week_52_position_pct",
    "volume", "avg_volume_10d", "avg_volume_30d",

    # Market structure (provider-sourced)
    "market_cap", "float_shares", "beta_5y",

    # Fundamentals — let the provider win
    "pe_ttm", "pe_forward", "eps_ttm", "dividend_yield", "payout_ratio",
    "revenue_ttm", "revenue_growth_yoy", "gross_margin", "operating_margin",
    "profit_margin", "debt_to_equity", "free_cash_flow_ttm",
    "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio",

    # Risk / technicals — engine + chart-derived
    "rsi_14", "volatility_30d", "volatility_90d", "max_drawdown_1y",
    "var_95_1d", "sharpe_1y", "risk_score", "risk_bucket",

    # Valuation + forecast (the whole point of the engine)
    "intrinsic_value", "upside_pct", "valuation_score",
    "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
    "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "forecast_confidence", "confidence_score", "confidence_bucket",
    "forecast_source",

    # All scores
    "value_score", "quality_score", "momentum_score", "growth_score",
    "overall_score", "opportunity_score", "rank_overall",
    "overall_score_raw", "overall_penalty_factor",
    "sector_relative_score", "conviction_score",

    # Views
    "fundamental_view", "technical_view", "risk_view", "value_view",

    # Recommendation atomic group — these MUST refresh together
    "recommendation", "recommendation_detailed", "recommendation_detail",
    "recommendation_reason", "recommendation_priority",
    "recommendation_priority_band", "recommendation_source",
    "provider_rating", "scoring_recommendation_source",
    "scoring_schema_version", "scoring_errors", "opportunity_source",

    # Factor/risk narrative + sizing
    "top_factors", "top_risks", "position_size_hint",

    # Candlestick / pattern signals
    "candlestick_pattern", "candlestick_signal", "candlestick_strength",
    "candlestick_confidence", "candlestick_patterns_recent",

    # Provenance + timing — always show the freshest pass
    "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",
    "horizon_days", "invest_period_label",
})

# Manual / user-managed fields the engine MUST NEVER overwrite from a live quote.
# These come from the Google Sheet and represent the user's intent.
_V577_MANUAL_FIELDS: frozenset = frozenset({
    "position_qty", "avg_cost", "position_cost", "position_value",
    "unrealized_pl", "unrealized_pl_pct",
})


def _overwrite_live_fields(base_row: Dict[str, Any], live_row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """v5.77.6: Whitelist-based live-quote overwrite for the external-rows path.

    For every key in _V577_LIVE_OVERWRITE_FIELDS, if `live_row` carries a
    non-blank value, that value REPLACES whatever was in `base_row` — even
    if the base already had something. Keys not in the whitelist are
    untouched, which is how manual/user-managed columns (position_qty,
    avg_cost, etc., listed in _V577_MANUAL_FIELDS) stay safe.

    This is the v5.77.6 fix for the stale-merge bug: callers in
    DataEngineV5.get_sheet_rows() invoke this for the live-quote merge,
    and keep using _merge_missing_fields() for snapshot / cache fallback
    merges (where the goal is to fill gaps, not to refresh data).
    """
    out = dict(base_row or {})
    if not isinstance(live_row, dict):
        return out
    for k, v in live_row.items():
        if k not in _V577_LIVE_OVERWRITE_FIELDS:
            continue
        if v in (None, "", [], {}):
            continue
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
    # actually synthesizes a forecast. We only stamp "fallback" if WE
    # create the price (vs. inheriting it) AND no upstream source is set.
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
    """v5.74.0+ — single canonical recommendation delegator.

    v5.76.0 removed the redundant `_clear_recommendation_output_fields(row)`
    pre-call; the v5.75.0 classifier self-clears its six output fields at
    entry (Step 2a), so calling _clear here was a no-op. The clearing
    helper remains as a public symbol for any external caller that
    references it.
    """
    _classify_recommendation_8tier(row)


def _apply_rank_overall(rows: List[Dict[str, Any]]) -> None:
    """Rank rows in-place by overall_score, descending (rank 1 = best).

    v5.77.6: emits a single INFO log line at the end of the call recording
    the ranked / skipped counts. The log is gated on logger.isEnabledFor(INFO)
    so the per-call cost in production (where the default level is WARNING)
    is zero. If the dashboard shows every row with rank_overall=1, the
    `[v5.77.6 RANK]` line will read `total=1` for each call — confirming
    the route handler is calling the engine in a per-symbol loop instead
    of a sheet batch (the v5.77.5 audit's primary remaining suspect).
    """
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

    # v5.77.6 RANK observability — emit once per call, gated on INFO.
    # v5.77.11: label is now dynamic via __version__ so it always reflects
    # the deployed engine version.
    if logger.isEnabledFor(logging.INFO):
        try:
            total = len(rows) if rows is not None else 0
            scored_count = len(scored)
            skipped_no_score = total - scored_count
            logger.info(
                "[v%s RANK] total=%d scored=%d skipped_no_score=%d",
                __version__, total, scored_count, skipped_no_score,
            )
        except Exception:
            pass


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

    # v5.77.0/v5.77.2: route sync provider calls through the dedicated
    # provider pool (200 workers by default) instead of asyncio.to_thread's
    # ~36-thread default executor — important under 200+ symbol batch loads.
    loop = asyncio.get_running_loop()
    executor = _get_provider_executor()
    if executor is None:
        result = await asyncio.to_thread(fn, *args, **kwargs)
    else:
        result = await loop.run_in_executor(executor, lambda: fn(*args, **kwargs))
    return await result if inspect.isawaitable(result) else result


# v5.77.0: dedicated thread pool for synchronous provider calls.
# v5.77.1: double-checked locking on initialization to prevent the
# concurrent-first-touch race that previously spawned thousands of
# orphan threads under 200-symbol startup bursts.
import concurrent.futures as _concurrent_futures
import threading as _threading

_PROVIDER_EXECUTOR: Optional[Any] = None
_PROVIDER_EXECUTOR_LOCK: "_threading.Lock" = _threading.Lock()


def _get_provider_executor() -> Optional[Any]:
    """Return the shared provider thread pool, creating it on first call.

    Fast-path (no lock) is the common case once the executor is up.
    Slow-path (lock acquired) only runs during the first concurrent
    first-touch burst and re-checks the singleton inside the critical
    section so only one executor is ever created.
    """
    global _PROVIDER_EXECUTOR
    if _PROVIDER_EXECUTOR is not None:
        return _PROVIDER_EXECUTOR
    with _PROVIDER_EXECUTOR_LOCK:
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
    """Gracefully release the dedicated provider pool on engine teardown."""
    global _PROVIDER_EXECUTOR
    with _PROVIDER_EXECUTOR_LOCK:
        if _PROVIDER_EXECUTOR is None:
            return
        try:
            _PROVIDER_EXECUTOR.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            try:
                _PROVIDER_EXECUTOR.shutdown(wait=False)
            except Exception:
                pass
        except Exception:
            pass
        _PROVIDER_EXECUTOR = None


def reset_provider_executor() -> None:
    """v5.77.2: test-only helper. Discards the current executor so the next
    _get_provider_executor() rebuild can pick up new TFB_PROVIDER_POOL_WORKERS.
    Not for runtime use — calling while requests are in flight orphans
    their futures.
    """
    _shutdown_provider_executor()


# =============================================================================
# Schema registry helpers
# =============================================================================
try:
    from core.sheets import schema_registry as _schema_registry
except Exception:
    _schema_registry = None  # type: ignore


def _schema_columns_from_any(spec: Any) -> Tuple[List[str], List[str]]:
    """Extract (keys, headers) from any registry spec shape we've ever shipped."""
    if spec is None:
        return [], []

    keys_raw: List[str] = []
    headers_raw: List[str] = []

    if isinstance(spec, dict):
        keys_raw = list(spec.get("keys") or [])
        headers_raw = list(spec.get("headers") or [])
        if not keys_raw and not headers_raw:
            cols = spec.get("columns") or spec.get("cols")
            if isinstance(cols, list) and cols:
                for col in cols:
                    if isinstance(col, dict):
                        k = _safe_str(col.get("key") or col.get("field"))
                        h = _safe_str(col.get("header") or col.get("title") or col.get("label"))
                    else:
                        k = h = _safe_str(col)
                    if k or h:
                        keys_raw.append(k)
                        headers_raw.append(h)
    else:
        for k_attr in ("keys", "columns", "cols", "fields"):
            v = getattr(spec, k_attr, None)
            if isinstance(v, list) and v:
                if isinstance(v[0], (str, type(None))):
                    keys_raw = [_safe_str(x) for x in v if x]
                    break
                if isinstance(v[0], dict):
                    for col in v:
                        k = _safe_str(col.get("key") or col.get("field"))
                        h = _safe_str(col.get("header") or col.get("title") or col.get("label"))
                        if k or h:
                            keys_raw.append(k)
                            headers_raw.append(h)
                    break
        for h_attr in ("headers", "labels", "titles"):
            v = getattr(spec, h_attr, None)
            if isinstance(v, list) and v:
                headers_raw = [_safe_str(x) for x in v if x]
                break

    keys = [k for k in keys_raw if _safe_str(k)]
    headers = [h for h in headers_raw if _safe_str(h)]
    return keys, headers


def _schema_keys_headers_from_spec(spec: Any) -> Tuple[List[str], List[str]]:
    keys, headers = _schema_columns_from_any(spec)
    return _complete_schema_contract(headers, keys)


def _registry_sheet_lookup(sheet: str) -> Any:
    if _schema_registry is None:
        return None
    for cand in _sheet_lookup_candidates(sheet):
        for fn_name in ("get_spec_for_sheet", "get_spec", "get_schema_for_sheet", "get_schema", "spec_for", "schema_for", "lookup", "for_sheet"):
            fn = getattr(_schema_registry, fn_name, None)
            if not callable(fn):
                continue
            for args, kwargs in (((cand,), {}), ((), {"sheet": cand}), ((), {"name": cand}), ((), {"page": cand}), ((), {"sheet_name": cand})):
                try:
                    spec = fn(*args, **kwargs)
                except TypeError:
                    continue
                except Exception:
                    continue
                if spec is not None:
                    return spec
    return None


def get_sheet_spec(sheet: str) -> Tuple[List[str], List[str]]:
    spec = _registry_sheet_lookup(sheet)
    if spec is not None:
        keys, headers = _schema_keys_headers_from_spec(spec)
        if keys and headers:
            target = _canonicalize_sheet_name(sheet)
            if _usable_contract(headers, keys, target):
                return headers, keys

    target = _canonicalize_sheet_name(sheet)
    if target in STATIC_CANONICAL_SHEET_CONTRACTS:
        contract = STATIC_CANONICAL_SHEET_CONTRACTS[target]
        return list(contract["headers"]), list(contract["keys"])
    return list(INSTRUMENT_CANONICAL_HEADERS), list(INSTRUMENT_CANONICAL_KEYS)


def _schema_for_sheet(sheet: str) -> Tuple[List[str], List[str]]:
    return get_sheet_spec(sheet)


def _list_sheet_names_best_effort() -> List[str]:
    names: List[str] = list(STATIC_CANONICAL_SHEET_CONTRACTS.keys())
    if _schema_registry is not None:
        for fn_name in ("list_sheets", "all_sheets", "sheet_names", "names"):
            fn = getattr(_schema_registry, fn_name, None)
            if not callable(fn):
                continue
            try:
                val = fn()
            except Exception:
                continue
            if isinstance(val, (list, tuple, set)):
                for n in val:
                    s = _safe_str(n)
                    if s and s not in names:
                        names.append(s)
                break
    return names


def _build_union_schema_keys() -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for name in _list_sheet_names_best_effort():
        try:
            _, keys = get_sheet_spec(name)
        except Exception:
            keys = []
        for key in keys:
            k = _safe_str(key)
            if k and k not in seen:
                seen.add(k)
                out.append(k)
    for k in INSTRUMENT_CANONICAL_KEYS:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


_SCHEMA_UNION_KEYS: List[str] = _build_union_schema_keys()


# =============================================================================
# Concurrency primitives
# =============================================================================
class SingleFlight:
    """Coalesce concurrent identical async tasks to a single invocation."""

    def __init__(self) -> None:
        self._inflight: Dict[str, asyncio.Task[Any]] = {}
        self._lock = asyncio.Lock()

    async def do(self, key: str, factory: Any) -> Any:
        # v5.77.7 deadlock fix: never `await task` while holding self._lock.
        # The previous shape did `return await task` inside `async with self._lock`,
        # so when caller A held the lock awaiting an in-flight task, the task's
        # `finally` block — which needs the same lock to pop the entry from
        # _inflight — could never acquire it. Two concurrent requests for the
        # same key would hang forever.
        #
        # New shape: take the lock just long enough to either find the existing
        # task or create a new one, then release the lock and await whichever
        # task we captured a reference to. Only one factory runs per key, and
        # the runner's `finally` block can always re-take the lock to clean up.
        async with self._lock:
            task = self._inflight.get(key)
            if task is None:
                async def runner() -> Any:
                    try:
                        return await factory()
                    finally:
                        async with self._lock:
                            self._inflight.pop(key, None)

                task = asyncio.create_task(runner())
                self._inflight[key] = task
        # Lock released. Await the task without blocking anyone else.
        return await task


class MultiLevelCache:
    """Two-tier (per-symbol + per-page) cache with TTL + per-entry stamping.

    v5.77.0: switched the per-symbol map from a plain dict to an
    `OrderedDict` so we can evict the oldest entries when the map exceeds
    `max_entries`. Previous versions silently grew without bound under
    sustained heavy load.
    """

    def __init__(self, ttl_seconds: int = 30, max_entries: int = 50_000) -> None:
        self._ttl = max(1, int(ttl_seconds))
        self._max = max(1000, int(max_entries))
        self._data: OrderedDict[str, Tuple[float, Any]] = OrderedDict()
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any:
        if not key:
            return None
        async with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            ts, value = entry
            now = time.time()
            if now - ts > self._ttl:
                self._data.pop(key, None)
                return None
            self._data.move_to_end(key)
            return value

    async def set(self, key: str, value: Any) -> None:
        if not key:
            return
        async with self._lock:
            self._data[key] = (time.time(), value)
            self._data.move_to_end(key)
            while len(self._data) > self._max:
                self._data.popitem(last=False)

    async def invalidate(self, key: str) -> None:
        if not key:
            return
        async with self._lock:
            self._data.pop(key, None)

    async def clear(self) -> None:
        async with self._lock:
            self._data.clear()

    def size(self) -> int:
        return len(self._data)


class ProviderRegistry:
    """Late-bound, optional provider module registry."""

    def __init__(self) -> None:
        self._modules: Dict[str, Any] = {}
        self._missing: Set[str] = set()

    def get(self, name: str) -> Optional[Any]:
        if not name:
            return None
        if name in self._modules:
            return self._modules[name]
        if name in self._missing:
            return None
        candidates: List[str] = []
        for prefix in ("core.providers.", "providers.", "core.data.providers.", "data.providers."):
            candidates.append(prefix + name)
            for suffix in ("_provider", "_client", "_quotes", "_data"):
                candidates.append(prefix + name + suffix)
        for path in candidates:
            try:
                mod = import_module(path)
                self._modules[name] = mod
                return mod
            except Exception:
                continue
        self._missing.add(name)
        return None


def _pick_provider_callable(mod: Any, *names: str) -> Optional[Any]:
    if mod is None:
        return None
    for n in names:
        fn = getattr(mod, n, None)
        if callable(fn):
            return fn
    return None


# =============================================================================
# Engine symbols-reader proxy
# =============================================================================
class _EngineSymbolsReaderProxy:
    """Proxies engine.get_symbols_for_sheet() calls through a stable contract."""

    def __init__(self, engine: "DataEngineV5") -> None:
        self._engine = engine

    async def list_symbols_for_page(self, page: str) -> List[str]:
        return await self._engine._get_symbols_for_sheet_impl(page)

    def list_symbols_for_page_sync(self, page: str) -> List[str]:
        try:
            return asyncio.get_event_loop().run_until_complete(self.list_symbols_for_page(page))
        except Exception:
            return list(EMERGENCY_PAGE_SYMBOLS.get(_canonicalize_sheet_name(page), []))

    def get_symbols(self, page: str) -> List[str]:
        return self.list_symbols_for_page_sync(page)

    list_symbols = get_symbols
    list_for_page = get_symbols
    fetch_symbols = get_symbols
    page_symbols = get_symbols


# =============================================================================
# DataEngineV5 — the main orchestrator
# =============================================================================
class DataEngineV5:
    """Global-first data orchestrator (v5.77.19)."""

    def __init__(
        self,
        settings: Any = None,
        symbols_reader: Any = None,
        rows_reader: Any = None,
        providers: Optional[Sequence[Any]] = None,
        cache_ttl_seconds: Optional[int] = None,
    ) -> None:
        self._settings = settings if settings is not None else _try_get_settings()
        self._features = _feature_flags(self._settings)
        self._symbols_reader_input = symbols_reader
        self._rows_reader_input = rows_reader
        self._provider_registry = ProviderRegistry()
        self._configured_providers = list(providers) if providers else []
        ttl = cache_ttl_seconds or _get_env_int("ENGINE_CACHE_TTL_SECONDS", 30)
        self._cache = MultiLevelCache(ttl_seconds=ttl)
        self._single_flight = SingleFlight()
        self._snapshot_lock = asyncio.Lock()
        self._page_snapshots: Dict[str, List[Dict[str, Any]]] = {}
        self._symbol_snapshots: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._closed = False

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self._cache.clear()
        except Exception:
            pass
        _shutdown_provider_executor()

    # ------- aliases -------
    async def close(self) -> None:
        await self.aclose()

    async def shutdown(self) -> None:
        await self.aclose()

    def _provider_profile_key(self) -> str:
        provs = []
        for p in self._configured_providers or DEFAULT_PROVIDERS:
            if isinstance(p, str):
                provs.append(p)
            else:
                provs.append(_safe_str(getattr(p, "name", "")) or p.__class__.__name__)
        return "_".join(_dedupe_keep_order([p.lower() for p in provs if p])) or "default"

    def _page_primary_provider_for(self, page: str) -> str:
        canon = _canonicalize_sheet_name(page)
        if canon in PAGE_PRIMARY_PROVIDER_DEFAULTS:
            return PAGE_PRIMARY_PROVIDER_DEFAULTS[canon]
        return (self._configured_providers or DEFAULT_PROVIDERS)[0] if (self._configured_providers or DEFAULT_PROVIDERS) else "eodhd"

    def _resolve_quote_page_context(self, symbol: str, page: str = "") -> Tuple[str, str]:
        p = _canonicalize_sheet_name(page)
        if not p:
            sym_u = normalize_symbol(symbol)
            if sym_u.endswith("=X") or sym_u.endswith("=F"):
                p = "Commodities_FX"
            elif sym_u in _ETF_SYMBOL_HINTS:
                p = "Mutual_Funds"
            elif sym_u.endswith(".SR") or re.match(r"^[0-9]{4}$", sym_u):
                p = "Market_Leaders"
            else:
                p = "Global_Markets"
        return p, self._page_primary_provider_for(p)

    def _providers_for(self, page: str) -> List[str]:
        p = _canonicalize_sheet_name(page)
        configured = [
            _safe_str(getattr(prov, "name", "")) or prov.__class__.__name__
            if not isinstance(prov, str)
            else prov
            for prov in (self._configured_providers or [])
        ]
        configured = [c.lower() for c in configured if c]
        if configured:
            return _dedupe_keep_order(configured)
        if p in NON_KSA_EODHD_PRIMARY_PAGES:
            return _dedupe_keep_order(DEFAULT_GLOBAL_PROVIDERS)
        if p in {"Market_Leaders", "My_Portfolio", "My_Investments", "Top_10_Investments"}:
            return _dedupe_keep_order(DEFAULT_PROVIDERS)
        return _dedupe_keep_order(DEFAULT_PROVIDERS)

    # =========================================================================
    # Snapshot management
    # =========================================================================
    async def _store_sheet_snapshot(self, sheet: str, rows: Sequence[Dict[str, Any]]) -> None:
        canon = _canonicalize_sheet_name(sheet)
        snapshot = [dict(r) for r in (rows or []) if isinstance(r, dict)]
        async with self._snapshot_lock:
            if snapshot:
                self._page_snapshots[canon] = snapshot
                for row in snapshot:
                    sym = normalize_symbol(_safe_str(row.get("symbol") or row.get("requested_symbol")))
                    if not sym:
                        continue
                    self._symbol_snapshots[(canon, sym)] = dict(row)

    async def get_cached_sheet_snapshot(self, sheet: str) -> List[Dict[str, Any]]:
        canon = _canonicalize_sheet_name(sheet)
        async with self._snapshot_lock:
            return [dict(r) for r in self._page_snapshots.get(canon, [])]

    async def _get_symbol_snapshot_row(self, sheet: str, symbol: str) -> Optional[Dict[str, Any]]:
        canon = _canonicalize_sheet_name(sheet)
        sym = normalize_symbol(symbol)
        async with self._snapshot_lock:
            return dict(self._symbol_snapshots.get((canon, sym), {})) or None

    async def _get_best_snapshot_row(self, symbol: str) -> Optional[Dict[str, Any]]:
        sym = normalize_symbol(symbol)
        if not sym:
            return None
        async with self._snapshot_lock:
            best: Optional[Dict[str, Any]] = None
            for (_canon, snap_sym), row in self._symbol_snapshots.items():
                if snap_sym == sym:
                    candidate = dict(row)
                    if best is None:
                        best = candidate
                        continue
                    cand_score = _as_float(candidate.get("overall_score")) or 0.0
                    best_score = _as_float(best.get("overall_score")) or 0.0
                    if cand_score > best_score:
                        best = candidate
            return best

    # =========================================================================
    # Symbol resolution
    # =========================================================================
    def _bind_symbols_reader(self) -> Any:
        if self._symbols_reader_input is not None:
            return self._symbols_reader_input
        for mod_path in ("core.sheets.symbols_reader", "sheets.symbols_reader", "core.symbols_reader"):
            try:
                mod = import_module(mod_path)
            except Exception:
                continue
            cls = getattr(mod, "SymbolsReader", None) or getattr(mod, "Reader", None)
            if cls is not None:
                try:
                    return cls()
                except Exception:
                    continue
        return None

    def _bind_rows_reader(self) -> Any:
        if self._rows_reader_input is not None:
            return self._rows_reader_input
        for mod_path in ("core.sheets.rows_reader", "sheets.rows_reader", "core.rows_reader"):
            try:
                mod = import_module(mod_path)
            except Exception:
                continue
            cls = getattr(mod, "RowsReader", None) or getattr(mod, "Reader", None)
            if cls is not None:
                try:
                    return cls()
                except Exception:
                    continue
        return None

    async def _get_symbols_from_reader(self, reader: Any, page: str) -> List[str]:
        if reader is None:
            return []
        canon = _canonicalize_sheet_name(page)
        for fn_name in ("list_symbols_for_page", "get_symbols_for_page", "get_symbols", "list_symbols", "symbols_for_page", "page_symbols"):
            fn = getattr(reader, fn_name, None)
            if not callable(fn):
                continue
            for args, kwargs in (((canon,), {}), ((), {"page": canon}), ((), {"sheet": canon}), ((), {"sheet_name": canon}), ((), {"name": canon})):
                try:
                    result = fn(*args, **kwargs)
                except TypeError:
                    continue
                except Exception as exc:
                    # v5.75.0: observable swallow — debug log instead of silent pass.
                    logger.debug(
                        "[engine_v2 v%s] symbols reader %s.%s raised %s: %s",
                        __version__,
                        reader.__class__.__name__ if hasattr(reader, "__class__") else "reader",
                        fn_name,
                        exc.__class__.__name__,
                        exc,
                    )
                    continue
                if inspect.isawaitable(result):
                    try:
                        result = await result
                    except Exception as exc:
                        logger.debug(
                            "[engine_v2 v%s] symbols reader %s.%s await raised %s: %s",
                            __version__,
                            reader.__class__.__name__ if hasattr(reader, "__class__") else "reader",
                            fn_name,
                            exc.__class__.__name__,
                            exc,
                        )
                        continue
                syms = _normalize_symbol_list(_split_symbols(result), limit=5000)
                if syms:
                    return syms
        return []

    async def _get_rows_from_external_reader(self, reader: Any, page: str, limit: int = 2000, offset: int = 0) -> List[Dict[str, Any]]:
        if reader is None:
            return []
        canon = _canonicalize_sheet_name(page)
        for fn_name in ("get_rows_for_page", "list_rows_for_page", "get_rows", "list_rows", "rows_for_page", "page_rows", "read_rows", "fetch_rows"):
            fn = getattr(reader, fn_name, None)
            if not callable(fn):
                continue
            for args, kwargs in (
                ((canon,), {}),
                ((canon, limit), {}),
                ((canon, limit, offset), {}),
                ((), {"page": canon}),
                ((), {"sheet": canon}),
                ((), {"sheet_name": canon}),
                ((), {"page": canon, "limit": limit}),
                ((), {"page": canon, "limit": limit, "offset": offset}),
            ):
                try:
                    result = fn(*args, **kwargs)
                except TypeError:
                    continue
                except Exception as exc:
                    logger.debug(
                        "[engine_v2 v%s] rows reader %s.%s raised %s: %s",
                        __version__,
                        reader.__class__.__name__ if hasattr(reader, "__class__") else "reader",
                        fn_name,
                        exc.__class__.__name__,
                        exc,
                    )
                    continue
                if inspect.isawaitable(result):
                    try:
                        result = await result
                    except Exception as exc:
                        logger.debug(
                            "[engine_v2 v%s] rows reader %s.%s await raised %s: %s",
                            __version__,
                            reader.__class__.__name__ if hasattr(reader, "__class__") else "reader",
                            fn_name,
                            exc.__class__.__name__,
                            exc,
                        )
                        continue
                rows = _coerce_rows_list(result)
                if rows:
                    return rows
        return []

    async def _get_symbols_for_sheet_impl(self, page: str) -> List[str]:
        canon = _canonicalize_sheet_name(page)

        reader = self._bind_symbols_reader()
        syms = await self._get_symbols_from_reader(reader, canon)
        if syms:
            return syms

        rows_reader = self._bind_rows_reader()
        rows = await self._get_rows_from_external_reader(rows_reader, canon)
        if rows:
            extracted = _extract_symbols_from_rows(rows)
            if extracted:
                return extracted

        env_key = PAGE_SYMBOL_ENV_KEYS.get(canon)
        if env_key:
            env_val = _safe_env(env_key, "")
            if env_val:
                env_syms = _normalize_symbol_list(_split_symbols(env_val), limit=5000)
                if env_syms:
                    return env_syms

        snapshot = await self.get_cached_sheet_snapshot(canon)
        if snapshot:
            extracted = _extract_symbols_from_rows(snapshot)
            if extracted:
                return extracted

        return list(EMERGENCY_PAGE_SYMBOLS.get(canon, []))

    async def get_sheet_symbols(self, sheet: str) -> List[str]:
        return await self._get_symbols_for_sheet_impl(sheet)

    async def get_page_symbols(self, page: str) -> List[str]:
        return await self._get_symbols_for_sheet_impl(page)

    async def list_symbols_for_page(self, page: str) -> List[str]:
        return await self._get_symbols_for_sheet_impl(page)

    async def list_symbols(self, page: str = "Market_Leaders") -> List[str]:
        return await self._get_symbols_for_sheet_impl(page)

    async def get_symbols(self, page: str = "Market_Leaders") -> List[str]:
        return await self._get_symbols_for_sheet_impl(page)

    @property
    def symbols(self) -> "_EngineSymbolsReaderProxy":
        return _EngineSymbolsReaderProxy(self)

    # =========================================================================
    # Provider fetch
    # =========================================================================
    async def _fetch_patch(self, provider_name: str, symbol: str, page: str = "") -> Dict[str, Any]:
        mod = self._provider_registry.get(provider_name)
        if mod is None:
            return {}
        fn = _pick_provider_callable(
            mod,
            "get_quote_async", "fetch_quote_async", "get_quote", "fetch_quote",
            "quote_async", "quote", "get_unified_quote", "fetch",
        )
        if fn is None:
            return {}
        try:
            result = await _call_maybe_async(fn, symbol)
        except Exception as exc:
            # v5.75.0: observable swallow.
            logger.debug(
                "[engine_v2 v%s] provider %s.%s raised on %s: %s: %s",
                __version__, provider_name, getattr(fn, "__name__", "?"), symbol,
                exc.__class__.__name__, exc,
            )
            return {}
        if isinstance(result, dict):
            return result
        return _model_to_dict(result)

    # =========================================================================
    # History helpers
    # =========================================================================
    def _rows_from_parallel_series(self, payload: Any) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []
        ts = payload.get("timestamp") or payload.get("dates") or payload.get("t")
        closes = payload.get("close") or payload.get("c") or payload.get("closes")
        if not isinstance(ts, list) or not isinstance(closes, list):
            return []
        opens = payload.get("open") or payload.get("o") or payload.get("opens") or [None] * len(ts)
        highs = payload.get("high") or payload.get("h") or payload.get("highs") or [None] * len(ts)
        lows = payload.get("low") or payload.get("l") or payload.get("lows") or [None] * len(ts)
        vols = payload.get("volume") or payload.get("v") or payload.get("volumes") or [None] * len(ts)
        out: List[Dict[str, Any]] = []
        n = min(len(ts), len(closes), len(opens), len(highs), len(lows), len(vols))
        for i in range(n):
            out.append({
                "timestamp": ts[i],
                "open": opens[i],
                "high": highs[i],
                "low": lows[i],
                "close": closes[i],
                "volume": vols[i],
            })
        return out

    def _coerce_history_rows(self, payload: Any) -> List[Dict[str, Any]]:
        if payload is None:
            return []
        if isinstance(payload, list):
            return [dict(r) for r in payload if isinstance(r, dict)]
        if isinstance(payload, dict):
            for key in ("rows", "data", "history", "series", "candles"):
                v = payload.get(key)
                if isinstance(v, list):
                    return [dict(r) for r in v if isinstance(r, dict)]
            rows = self._rows_from_parallel_series(payload)
            if rows:
                return rows
        return []

    def _safe_mean(self, xs: Sequence[float]) -> Optional[float]:
        xs2 = [x for x in xs if x is not None]
        return (sum(xs2) / len(xs2)) if xs2 else None

    def _safe_std(self, xs: Sequence[float]) -> Optional[float]:
        xs2 = [x for x in xs if x is not None]
        if len(xs2) < 2:
            return None
        mean = sum(xs2) / len(xs2)
        variance = sum((x - mean) ** 2 for x in xs2) / max(1, (len(xs2) - 1))
        return math.sqrt(variance)

    def _quantile(self, xs: Sequence[float], q: float) -> Optional[float]:
        xs2 = sorted(x for x in xs if x is not None)
        if not xs2:
            return None
        k = (len(xs2) - 1) * q
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return xs2[int(k)]
        return xs2[f] + (xs2[c] - xs2[f]) * (k - f)

    def _compute_history_patch_from_rows(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not rows:
            return {}
        closes = [_as_float(r.get("close") or r.get("c")) for r in rows]
        closes = [c for c in closes if c is not None]
        if len(closes) < 20:
            return {}
        returns = [(closes[i] - closes[i - 1]) / closes[i - 1] for i in range(1, len(closes)) if closes[i - 1]]
        if not returns:
            return {}
        last_close = closes[-1]
        patch: Dict[str, Any] = {}
        recent_30 = returns[-30:] if len(returns) >= 30 else returns
        recent_90 = returns[-90:] if len(returns) >= 90 else returns
        std30 = self._safe_std(recent_30)
        std90 = self._safe_std(recent_90)
        if std30 is not None:
            patch["volatility_30d"] = round(std30 * math.sqrt(252.0), 6)
        if std90 is not None:
            patch["volatility_90d"] = round(std90 * math.sqrt(252.0), 6)
        if last_close is not None and closes:
            running_peak = closes[0]
            max_dd = 0.0
            for c in closes:
                if c > running_peak:
                    running_peak = c
                dd = (c - running_peak) / running_peak if running_peak else 0.0
                if dd < max_dd:
                    max_dd = dd
            patch["max_drawdown_1y"] = round(max_dd, 6)
        var95 = self._quantile(returns, 0.05)
        if var95 is not None:
            patch["var_95_1d"] = round(var95, 6)
        mean_r = self._safe_mean(returns)
        std_r = self._safe_std(returns)
        if mean_r is not None and std_r is not None and std_r > 0:
            patch["sharpe_1y"] = round((mean_r * 252.0) / (std_r * math.sqrt(252.0)), 4)

        # RSI(14)
        gains: List[float] = []
        losses: List[float] = []
        for i in range(1, min(len(closes), 15)):
            chg = closes[-i] - closes[-i - 1]
            if chg >= 0:
                gains.append(chg)
            else:
                losses.append(abs(chg))
        avg_gain = sum(gains) / 14 if gains else 0.0
        avg_loss = sum(losses) / 14 if losses else 0.0
        if avg_loss == 0 and avg_gain == 0:
            patch["rsi_14"] = 50.0
        elif avg_loss == 0:
            patch["rsi_14"] = 100.0
        else:
            rs = avg_gain / avg_loss
            patch["rsi_14"] = round(100.0 - (100.0 / (1.0 + rs)), 2)

        if rows[-1].get("timestamp") and not patch.get("last_updated_utc"):
            patch["last_updated_utc"] = _coerce_datetime_like(rows[-1].get("timestamp"))

        patches = detect_candlestick_patterns(rows[-30:] if len(rows) >= 30 else rows)
        for k, v in patches.items():
            patch[k] = v
        return patch

    async def _fetch_history_patch(self, provider_name: str, symbol: str) -> Dict[str, Any]:
        mod = self._provider_registry.get(provider_name)
        if mod is None:
            return {}
        fn = _pick_provider_callable(
            mod,
            "get_history_async", "fetch_history_async", "get_history",
            "fetch_history", "history_async", "history", "get_candles",
        )
        if fn is None:
            return {}
        try:
            result = await _call_maybe_async(fn, symbol)
        except Exception as exc:
            logger.debug(
                "[engine_v2 v%s] history provider %s.%s raised on %s: %s: %s",
                __version__, provider_name, getattr(fn, "__name__", "?"), symbol,
                exc.__class__.__name__, exc,
            )
            return {}
        rows = self._coerce_history_rows(result)
        if not rows:
            return {}
        return self._compute_history_patch_from_rows(rows)

    async def _get_history_patch_best_effort(self, symbol: str, page: str = "") -> Dict[str, Any]:
        for provider_name in self._providers_for(page):
            patch = await self._fetch_history_patch(provider_name, symbol)
            if patch:
                return patch
        return {}

    def _merge(self, base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(base or {})
        for k, v in (patch or {}).items():
            if v is None or v == "":
                continue
            if out.get(k) in (None, "", [], {}):
                out[k] = v
        return out

    def _data_quality(self, row: Dict[str, Any]) -> QuoteQuality:
        if not isinstance(row, dict):
            return QuoteQuality.MISSING
        if _as_float(row.get("current_price")) is None:
            return QuoteQuality.MISSING
        critical_present = sum(1 for k in ("current_price", "name", "exchange", "currency") if _safe_str(row.get(k)))
        if critical_present >= 4:
            return QuoteQuality.GOOD
        if critical_present >= 2:
            return QuoteQuality.FAIR
        return QuoteQuality.MISSING

    # =========================================================================
    # Yahoo enrichment (v5.77.2: routed through dedicated pool)
    # =========================================================================
    async def _fetch_yahoo_fundamentals_patch(self, symbol: str, page: str = "") -> Dict[str, Any]:
        # v5.77.7: pass the module basename to the importer (was called with no
        # args — TypeError); drop the `page` arg from _yahoo_symbol_for (it only
        # accepts one positional). `page` is kept on this method's signature so
        # call sites that pass it still work; we just don't forward it.
        mod = _import_yahoo_provider_module("yahoo_fundamentals_provider")
        # v5.77.14 FIX (the universal-SELL root cause): core.providers.
        # yahoo_fundamentals_provider exposes its module-level entry point as
        # `fetch_fundamentals_patch` (and the combined `fetch_enriched_quote_patch`),
        # NOT get_fundamentals / fetch_fundamentals / fundamentals. Searching only
        # the legacy names returned None, so this entire pass was a silent no-op
        # and fundamentals never populated — every equity row then fell back to
        # momentum-only scoring and collapsed into the SELL/REDUCE/AVOID band.
        # List the provider's real names FIRST so the picker resolves; the legacy
        # names remain as fallbacks for any alternate provider build.
        fn = _pick_yahoo_callable(
            mod,
            "fetch_fundamentals_patch", "fetch_enriched_quote_patch",
            "get_fundamentals_async", "fetch_fundamentals_async",
            "get_fundamentals", "fetch_fundamentals",
            "fundamentals_async", "fundamentals",
        )
        if fn is None:
            return {}
        ysym = _yahoo_symbol_for(symbol)
        try:
            result = await _call_maybe_async(fn, ysym)
        except Exception as exc:
            logger.debug(
                "[engine_v2 v%s] yahoo fundamentals raised on %s (yahoo=%s): %s: %s",
                __version__, symbol, ysym, exc.__class__.__name__, exc,
            )
            return {}
        if isinstance(result, dict):
            return result
        return _model_to_dict(result)

    async def _fetch_yahoo_chart_patch(self, symbol: str, page: str = "") -> Dict[str, Any]:
        # v5.77.7: pass module basename + drop page arg (see fundamentals patch).
        mod = _import_yahoo_provider_module("yahoo_chart_provider")
        fn = _pick_yahoo_callable(
            mod,
            "get_chart_async", "fetch_chart_async",
            "get_chart", "fetch_chart",
            "chart_async", "chart",
            "get_history_async", "get_history",
        )
        if fn is None:
            return {}
        ysym = _yahoo_symbol_for(symbol)
        try:
            result = await _call_maybe_async(fn, ysym)
        except Exception as exc:
            logger.debug(
                "[engine_v2 v%s] yahoo chart raised on %s (yahoo=%s): %s: %s",
                __version__, symbol, ysym, exc.__class__.__name__, exc,
            )
            return {}
        rows = self._coerce_history_rows(result)
        if not rows:
            return {}
        return self._compute_history_patch_from_rows(rows)

    async def _apply_yahoo_enrichment_pass(self, row: Dict[str, Any], symbol: str, page: str = "") -> Dict[str, Any]:
        # v5.77.7: signature-mismatch fixes (see WHY v5.77.7 block at top of file).
        # v5.77.8: canonicalize each Yahoo patch BEFORE running it through the
        # missing-field filter. v5.77.7 still passed the raw Yahoo response
        # (camelCase keys: `marketCap`, `trailingPE`, `targetMeanPrice`,
        # `shortName`, ...) into `_filter_patch_to_missing_fields()`, which
        # only kept fields whose key matched the canonical whitelist
        # (`market_cap`, `pe_ttm`, `target_mean_price`, `name`, ...). Result:
        # the filter rejected nearly everything Yahoo returned and the
        # enrichment was effectively a no-op. Canonicalizing first runs
        # the raw keys through `_CANONICAL_FIELD_ALIASES` so they land on
        # the snake_case names the whitelist expects.
        # v5.77.15: honor the documented master switch. Before v5.77.15,
        # _yahoo_enrichment_enabled() was defined but never called, so
        # ENGINE_YAHOO_ENRICHMENT_ENABLED=0/false/off did NOT actually disable
        # the pass (the doc/code mismatch the audit caught). With this guard,
        # setting that env var off cleanly short-circuits the entire pass
        # before any needs-check or provider round-trip runs.
        if not _yahoo_enrichment_enabled():
            return row

        needs_fund, needs_chart = _row_needs_yahoo_enrichment(row)
        if not (needs_fund or needs_chart):
            return row

        sym_for_canon = normalize_symbol(symbol) or normalize_symbol(
            _safe_str(row.get("symbol") or row.get("requested_symbol"))
        )

        if needs_fund:
            patch = await self._fetch_yahoo_fundamentals_patch(symbol, page)
            if patch:
                # v5.77.8: canonicalize the raw provider response so the
                # missing-field filter can match its canonical-name whitelist.
                # v5.77.19 (Fix 1): the Yahoo fundamentals patch frequently
                # carries targetMeanPrice but NO currentPrice, which would make
                # the provider-target gate fail open. Inject the already-known
                # row price into a COPY of the patch so the gate judges the
                # Yahoo target against the real price. The injected price is not
                # merged back: current_price is gated out of
                # _YAHOO_FUNDAMENTAL_FIELDS, so the filter cannot carry it onto
                # the row.
                patch_for_canon = dict(patch)
                ref_price = _as_float(row.get("current_price"))
                if ref_price is None:
                    ref_price = _as_float(row.get("price"))
                if ref_price is not None \
                        and _as_float(patch_for_canon.get("current_price")) is None \
                        and _as_float(patch_for_canon.get("price")) is None:
                    patch_for_canon["current_price"] = ref_price
                canon_patch = _canonicalize_provider_row(
                    patch_for_canon,
                    requested_symbol=sym_for_canon,
                    normalized_symbol=sym_for_canon,
                    provider="yahoo_fundamentals",
                )
                filtered, filled = _filter_patch_to_missing_fields(
                    row, canon_patch, _YAHOO_FUNDAMENTAL_FIELDS,
                )
                # v5.77.19 (Fix 2): the missing-field filter drops everything
                # not in the data whitelist, including the gate's
                # provider_target_implausible_dropped_* tags. Carry those
                # drop-audit tags back so the trail survives the enrichment.
                _merge_gate_drop_warnings(row, canon_patch)
                if filtered:
                    row = self._merge(row, filtered)
                    _append_yahoo_warning_tag(row, "yahoo_enrichment_applied")

        # Re-check chart needs against the (possibly fundamentals-enriched) row.
        _, needs_chart = _row_needs_yahoo_enrichment(row)
        if needs_chart:
            chart_patch = await self._fetch_yahoo_chart_patch(symbol, page)
            if chart_patch:
                # v5.77.19 (Fix 1): same reference-price injection as the
                # fundamentals branch. The chart patch is technicals-only today
                # (no analyst target), so the gate is normally a no-op here, but
                # injecting the row price keeps the gate correct if a future
                # chart patch ever carries a target. For _YAHOO_CHART_FIELDS the
                # missing-field filter only fills a BLANK row price, so an
                # existing current_price is never overwritten by the injection.
                chart_for_canon = dict(chart_patch)
                ref_price = _as_float(row.get("current_price"))
                if ref_price is None:
                    ref_price = _as_float(row.get("price"))
                if ref_price is not None \
                        and _as_float(chart_for_canon.get("current_price")) is None \
                        and _as_float(chart_for_canon.get("price")) is None:
                    chart_for_canon["current_price"] = ref_price
                canon_chart = _canonicalize_provider_row(
                    chart_for_canon,
                    requested_symbol=sym_for_canon,
                    normalized_symbol=sym_for_canon,
                    provider="yahoo_chart",
                )
                filtered, filled = _filter_patch_to_missing_fields(
                    row, canon_chart, _YAHOO_CHART_FIELDS,
                )
                # v5.77.19 (Fix 2): preserve any gate drop tags through the filter.
                _merge_gate_drop_warnings(row, canon_chart)
                if filtered:
                    row = self._merge(row, filtered)
                    _append_yahoo_warning_tag(row, "yahoo_chart_enrichment_applied")
        return row

    # =========================================================================
    # Enriched quote orchestration
    # =========================================================================
    async def _get_enriched_quote_impl(self, symbol: str, page: str = "") -> Dict[str, Any]:
        sym = normalize_symbol(symbol)
        if not sym:
            return {}
        page_ctx, _primary = self._resolve_quote_page_context(sym, page)
        cache_key = _make_cache_key(sym, page_ctx, self._provider_profile_key())
        cached = await self._cache.get(cache_key)
        if isinstance(cached, dict) and cached:
            return cached

        async def factory() -> Dict[str, Any]:
            merged: Dict[str, Any] = {}
            for provider_name in self._providers_for(page_ctx):
                patch = await self._fetch_patch(provider_name, sym, page_ctx)
                if not patch:
                    continue
                canon_patch = _canonicalize_provider_row(
                    patch, requested_symbol=sym, normalized_symbol=sym, provider=provider_name,
                )
                merged = self._merge(merged, canon_patch)
                merged.setdefault("data_provider", provider_name)
                if _as_float(merged.get("current_price")) is not None:
                    break

            # History fallback when live failed.
            if _as_float(merged.get("current_price")) is None:
                hist_patch = await self._get_history_patch_best_effort(sym, page_ctx)
                if hist_patch:
                    canon_hist = _canonicalize_provider_row(
                        hist_patch, requested_symbol=sym, normalized_symbol=sym, provider="history",
                    )
                    merged = self._merge(merged, canon_hist)
                    merged.setdefault("data_provider", "history_or_fallback")

            # Snapshot fallback.
            if _as_float(merged.get("current_price")) is None:
                snap = await self._get_symbol_snapshot_row(page_ctx, sym) or await self._get_best_snapshot_row(sym)
                if snap:
                    merged = _merge_missing_fields(merged, snap)
                    merged.setdefault("data_provider", _safe_str(snap.get("data_provider"), "snapshot"))

            # History technicals (RSI / volatility / max drawdown / candlesticks).
            if not any(
                _as_float(merged.get(k)) is not None
                for k in ("rsi_14", "volatility_30d", "volatility_90d", "max_drawdown_1y")
            ):
                hist_patch = await self._get_history_patch_best_effort(sym, page_ctx)
                if hist_patch:
                    merged = self._merge(merged, hist_patch)

            merged = _apply_symbol_context_defaults(merged, symbol=sym, page=page_ctx)

            # Yahoo enrichment pass (filtered to truly-missing fields).
            merged = await self._apply_yahoo_enrichment_pass(merged, sym, page_ctx)

            # Phase BB sanity normalization.
            merged = _apply_phase_bb_sanity(merged)

            # Final scoring + recommendation.
            # v5.77.16: _apply_phase_dd_enhancements now performs the single
            # authoritative classification (it calls _classify_recommendation_8tier
            # unconditionally). The previous extra _compute_recommendation(merged)
            # call here ran the classifier a SECOND time, which — when the row had
            # no upstream provider rating — captured the engine's own recommendation
            # as provider_rating on the second pass. Removed; classification happens
            # exactly once now.
            if not _is_empty_data_row(merged):
                _compute_scores_canonical_first(merged)
                _apply_phase_dd_enhancements(merged)
            else:
                _mark_row_as_empty(merged)

            merged = _apply_page_row_backfill(page_ctx, merged)

            if not merged.get("last_updated_utc"):
                merged["last_updated_utc"] = _now_utc_iso()
            if not merged.get("last_updated_riyadh"):
                merged["last_updated_riyadh"] = _now_riyadh_iso()

            await self._cache.set(cache_key, merged)
            await self._store_sheet_snapshot(page_ctx, [merged])
            return merged

        return await self._single_flight.do(cache_key, factory)

    async def get_enriched_quote(self, symbol: str, page: str = "") -> UnifiedQuote:
        row = await self._get_enriched_quote_impl(symbol, page)
        return UnifiedQuote(**row) if row else UnifiedQuote()

    async def get_enriched_quote_dict(self, symbol: str, page: str = "") -> Dict[str, Any]:
        return await self._get_enriched_quote_impl(symbol, page)

    async def get_enriched_quotes(self, symbols: Sequence[str], page: str = "") -> List[Dict[str, Any]]:
        # v5.77.7: per-symbol failure isolation. v5.77.6 used
        # `return_exceptions=False`, so a single bad symbol raised out of
        # asyncio.gather and dropped the whole page refresh on the floor.
        # We now collect exceptions and emit a degraded row for each failure
        # so the dashboard still gets a 140-row response — with the offending
        # symbols tagged in their warnings field for downstream visibility.
        symbols = _normalize_symbol_list(symbols, limit=5000)
        if not symbols:
            return []
        tasks = [self._get_enriched_quote_impl(sym, page) for sym in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        out: List[Dict[str, Any]] = []
        for sym, r in zip(symbols, results):
            if isinstance(r, dict):
                out.append(r)
                continue
            if isinstance(r, BaseException):
                logger.warning(
                    "[engine_v2 v%s] enriched_quote failed for %s on page=%s: %s: %s",
                    __version__, sym, page or "?", r.__class__.__name__, r,
                )
                # v5.77.8: degraded rows now carry conservative recommendation
                # defaults so the sheet projects something actionable (HOLD with
                # an explicit "enrichment_failed" source) instead of leaving the
                # Recommendation / Reco Source / Reco Reason / Priority / Band /
                # Confidence Bucket columns blank. risk_bucket and risk_score
                # stay None because we have no actual data to assign them from.
                degraded = {
                    "symbol": sym,
                    "requested_symbol": sym,
                    "data_provider": "fallback_error",
                    "warnings": f"enrichment_failed:{r.__class__.__name__}",
                    "recommendation": "HOLD",
                    "recommendation_detailed": "HOLD",
                    "recommendation_source": "enrichment_failed",
                    "recommendation_reason": "HOLD: quote enrichment failed; not actionable.",
                    "recommendation_priority": 4,
                    "recommendation_priority_band": "P4",
                    "confidence_bucket": "LOW",
                    "last_updated_utc": _now_utc_iso(),
                    "last_updated_riyadh": _now_riyadh_iso(),
                }
                out.append(degraded)
        return out

    async def get_enriched_quotes_batch(self, symbols: Sequence[str], page: str = "") -> List[Dict[str, Any]]:
        return await self.get_enriched_quotes(symbols, page)

    # Aliases
    get_quote = get_enriched_quote_dict
    quote = get_enriched_quote_dict
    fetch_quote = get_enriched_quote_dict

    # =========================================================================
    # Special-page builders
    # =========================================================================
    def _build_data_dictionary_rows(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for sheet_name in _list_sheet_names_best_effort():
            try:
                headers, keys = get_sheet_spec(sheet_name)
            except Exception:
                continue
            for header, key in zip(headers, keys):
                out.append({
                    "sheet": sheet_name,
                    "group": "Engine",
                    "header": header,
                    "key": key,
                    "dtype": "auto",
                    "fmt": "",
                    "required": True,
                    "source": "engine_v2",
                    "notes": "",
                })
        return out

    def _build_insights_rows_fallback(self) -> List[Dict[str, Any]]:
        ts_utc = _now_utc_iso()
        return [
            {"section": "Coverage", "item": "Engine Version", "metric": "version", "value": __version__,
             "notes": "Live", "source": "engine_v2", "sort_order": 1},
            {"section": "Coverage", "item": "Last Updated (UTC)", "metric": "timestamp", "value": ts_utc,
             "notes": "", "source": "engine_v2", "sort_order": 2},
        ]

    def _top10_sort_key(self, row: Dict[str, Any]) -> Tuple[float, float, float]:
        return (
            -(_as_float(row.get("opportunity_score")) or 0.0),
            -(_as_float(row.get("overall_score")) or 0.0),
            -(_as_float(row.get("confidence_score")) or 0.0),
        )

    async def _build_top10_rows_fallback(self, criteria: Optional[Dict[str, Any]] = None, top_n: int = 10) -> List[Dict[str, Any]]:
        criteria = criteria or {}
        pages = criteria.get("pages_selected") or TOP10_ENGINE_DEFAULT_PAGES
        all_rows: List[Dict[str, Any]] = []
        seen: Set[str] = set()
        for page in pages:
            try:
                page_rows = await self.get_page_rows(page, limit=200)
            except Exception:
                continue
            for row in page_rows:
                sym = normalize_symbol(_safe_str(row.get("symbol") or row.get("requested_symbol")))
                if not sym or sym in seen:
                    continue
                seen.add(sym)
                all_rows.append(row)
        all_rows.sort(key=self._top10_sort_key)
        top = all_rows[:max(1, int(top_n))]
        criteria_snapshot = _top10_criteria_snapshot(criteria)
        for idx, row in enumerate(top, start=1):
            row["top10_rank"] = idx
            row["selection_reason"] = _top10_selection_reason(row)
            row["criteria_snapshot"] = criteria_snapshot
        return top

    # =========================================================================
    # Page rows orchestration
    # =========================================================================
    async def get_page_rows(self, page: str, limit: int = 2000, offset: int = 0, **_kwargs: Any) -> List[Dict[str, Any]]:
        canon = _canonicalize_sheet_name(page)
        symbols = await self.list_symbols_for_page(canon)
        if not symbols:
            return []
        symbols = symbols[max(0, int(offset)):max(0, int(offset)) + max(1, int(limit))]
        return await self.get_enriched_quotes(symbols, canon)

    async def get_sheet(self, sheet: str, *, limit: int = 2000, offset: int = 0, **kwargs: Any) -> Dict[str, Any]:
        canon = _canonicalize_sheet_name(sheet)
        headers, keys = get_sheet_spec(canon)
        body = kwargs.get("body") or {}
        if _is_schema_only_body(body):
            return {
                "sheet": canon,
                "headers": headers,
                "keys": keys,
                "rows": [],
                "rows_display": [],
                "rows_matrix": [],
                "schema_only": True,
            }
        rows_data = await self.get_sheet_rows(canon, limit=limit, offset=offset, body=body)
        return {
            "sheet": canon,
            "headers": headers,
            "keys": keys,
            **rows_data,
        }

    async def get_sheet_rows(
        self,
        sheet: str,
        *,
        limit: int = 2000,
        offset: int = 0,
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Build full rows payload for a sheet.

        v5.77.6 fix site: the external-rows path now uses
        `_overwrite_live_fields` instead of `_merge_missing_fields` when
        folding the freshly-fetched live quote into the sheet row. This
        ensures price / score / recommendation / forecast / timestamps
        refresh every cycle even when the sheet row was already populated
        (with yesterday's stale data). The snapshot-map and best-snapshot
        merges still use `_merge_missing_fields` because those are about
        filling cache gaps, not refreshing live data.
        """
        target_sheet = _canonicalize_sheet_name(sheet)
        headers, keys = get_sheet_spec(target_sheet)
        body = body or {}

        # Special pages
        if target_sheet == "Insights_Analysis":
            rows = self._build_insights_rows_fallback()
            return {
                "rows": rows,
                "rows_display": _rows_display_objects_from_rows(rows, headers, keys),
                "rows_matrix": _rows_matrix_from_rows(rows, keys),
                "limit": limit,
                "offset": offset,
                "total": len(rows),
            }
        if target_sheet == "Data_Dictionary":
            rows = self._build_data_dictionary_rows()
            return {
                "rows": rows,
                "rows_display": _rows_display_objects_from_rows(rows, headers, keys),
                "rows_matrix": _rows_matrix_from_rows(rows, keys),
                "limit": limit,
                "offset": offset,
                "total": len(rows),
            }

        # Top_10_Investments fallback builder.
        if target_sheet == "Top_10_Investments":
            normalized_body, _warnings = _normalize_top10_body_for_engine(body, limit)
            criteria = normalized_body.get("criteria") or {}
            requested_top_n = int(criteria.get("top_n") or 10)
            requested_symbols = _extract_requested_symbols_from_body(normalized_body, limit=requested_top_n)
            rows: List[Dict[str, Any]] = []
            if requested_symbols:
                rows = await self.get_enriched_quotes(requested_symbols)
                rows.sort(key=self._top10_sort_key)
                rows = rows[:requested_top_n]
                criteria_snapshot = _top10_criteria_snapshot(criteria)
                for idx, row in enumerate(rows, start=1):
                    row["top10_rank"] = idx
                    row["selection_reason"] = _top10_selection_reason(row)
                    row["criteria_snapshot"] = criteria_snapshot
            else:
                rows = await self._build_top10_rows_fallback(criteria=criteria, top_n=requested_top_n)
            rows = [_apply_page_row_backfill("Top_10_Investments", r) for r in rows]
            rows = [_strict_project_row(keys, r) for r in rows]
            _apply_rank_overall(rows)
            return {
                "rows": rows,
                "rows_display": _rows_display_objects_from_rows(rows, headers, keys),
                "rows_matrix": _rows_matrix_from_rows(rows, keys),
                "limit": limit,
                "offset": offset,
                "total": len(rows),
            }

        # Instrument sheets — try external-rows reader first, then engine fetch.
        rows: List[Dict[str, Any]] = []
        if target_sheet in INSTRUMENT_SHEETS:
            rows_reader = self._bind_rows_reader()
            ext_rows = await self._get_rows_from_external_reader(rows_reader, target_sheet, limit=limit, offset=offset)
            if ext_rows:
                # Build a quote-map from a single batched engine pass so the merge
                # below sees fresh price / score / recommendation / forecast data
                # for every symbol in the external rowset.
                symbols = _extract_symbols_from_rows(ext_rows, limit=limit)
                quote_rows: List[Dict[str, Any]] = []
                if symbols:
                    try:
                        quote_rows = await self.get_enriched_quotes(symbols, target_sheet)
                    except Exception as exc:
                        logger.debug(
                            "[engine_v2 v%s] external-rows quote merge failed for %s: %s: %s",
                            __version__, target_sheet, exc.__class__.__name__, exc,
                        )
                        quote_rows = []
                quote_map: Dict[str, Dict[str, Any]] = {}
                for q in quote_rows:
                    if not isinstance(q, dict):
                        continue
                    qsym = normalize_symbol(_safe_str(q.get("symbol") or q.get("requested_symbol")))
                    if qsym:
                        quote_map[qsym] = q

                # First merge layer: snapshot map (gap fill from snapshot cache).
                snapshot_map: Dict[str, Dict[str, Any]] = {}
                async with self._snapshot_lock:
                    for (canon_p, snap_sym), snap_row in self._symbol_snapshots.items():
                        if canon_p == target_sheet and snap_sym:
                            snapshot_map[snap_sym] = dict(snap_row)

                for ext in ext_rows:
                    if not isinstance(ext, dict):
                        continue
                    sym = normalize_symbol(_safe_str(ext.get("symbol") or ext.get("requested_symbol")))
                    merged = dict(ext)

                    # First fill: snapshot cache (fill-only — preserve sheet values).
                    if sym and sym in snapshot_map:
                        merged = _merge_missing_fields(merged, snapshot_map[sym])

                    # Second fill: best-effort cross-page snapshot row (fill-only).
                    if sym:
                        best_snapshot_row = await self._get_best_snapshot_row(sym)
                        if best_snapshot_row:
                            merged = _merge_missing_fields(merged, best_snapshot_row)

                    # ---------------------------------------------------------
                    # v5.77.6: live-quote OVERWRITE (not merge-missing-fields).
                    # ---------------------------------------------------------
                    # This is the actual fix site. For every engine-owned field
                    # in _V577_LIVE_OVERWRITE_FIELDS, the fresh quote replaces
                    # whatever was in the sheet row. Manual fields (position_qty,
                    # avg_cost, position_cost, position_value, unrealized_pl,
                    # unrealized_pl_pct — see _V577_MANUAL_FIELDS) are not in the
                    # whitelist, so they're preserved from the sheet.
                    if sym and sym in quote_map:
                        merged = _overwrite_live_fields(merged, quote_map[sym])

                    merged = _apply_page_row_backfill(target_sheet, merged)
                    rows.append(merged)

        # Engine-only path (no external rows or external returned nothing).
        if not rows:
            symbols = await self.list_symbols_for_page(target_sheet)
            requested = _extract_requested_symbols_from_body(body)
            if requested:
                symbols = requested
            symbols = symbols[max(0, int(offset)):max(0, int(offset)) + max(1, int(limit))]
            engine_rows = await self.get_enriched_quotes(symbols, target_sheet)
            rows = [_apply_page_row_backfill(target_sheet, r) for r in engine_rows]

        # Final projection + ranking.
        rows = [_strict_project_row(keys, r) for r in rows]
        _apply_rank_overall(rows)

        return {
            "rows": rows,
            "rows_display": _rows_display_objects_from_rows(rows, headers, keys),
            "rows_matrix": _rows_matrix_from_rows(rows, keys),
            "limit": limit,
            "offset": offset,
            "total": len(rows),
        }

    # ------- aliases for get_sheet_rows -------
    async def sheet_rows(self, sheet: str, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(sheet, **kwargs)

    async def build_sheet_rows(self, sheet: str, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(sheet, **kwargs)

    async def execute_sheet_rows(self, sheet: str, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(sheet, **kwargs)

    async def run_sheet_rows(self, sheet: str, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(sheet, **kwargs)

    async def build_analysis_sheet_rows(self, sheet: str, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(sheet, **kwargs)

    # =========================================================================
    # Schema accessors
    # =========================================================================
    def get_sheet_contract(self, sheet: str) -> Dict[str, Any]:
        headers, keys = get_sheet_spec(sheet)
        return {
            "sheet": _canonicalize_sheet_name(sheet),
            "headers": headers,
            "keys": keys,
            "schema_version": _SCHEMA_VERSION,
        }

    def get_page_contract(self, page: str) -> Dict[str, Any]:
        return self.get_sheet_contract(page)

    def get_page_schema(self, page: str) -> Dict[str, Any]:
        return self.get_sheet_contract(page)

    def get_headers_for_sheet(self, sheet: str) -> List[str]:
        headers, _keys = get_sheet_spec(sheet)
        return headers

    def get_keys_for_sheet(self, sheet: str) -> List[str]:
        _headers, keys = get_sheet_spec(sheet)
        return keys

    # =========================================================================
    # Health / stats
    # =========================================================================
    def health(self) -> Dict[str, Any]:
        return {
            "ok": True,
            "version": __version__,
            "schema_version": _SCHEMA_VERSION,
            "scoring_contract_version": _SCORING_CONTRACT_VERSION,
            "reco_normalize_contract_version": _RECO_NORMALIZE_CONTRACT_VERSION,
            "valuation_model": {
                "version": "v5.77.19",  # v5.77.19: provider-target gate hardening on the v5.77.18 ingestion gate -- (1) inject the row price as the gate reference on the Yahoo enrichment path so a price-less patch can't fail the gate open, (2) carry provider_target_implausible_dropped_* warnings through the enrichment filter, (3) honor a 3M-only provider target in Phase-II instead of overwriting it; gate still env-tunable via TFB_PROVIDER_TARGET_MIN_MULT / _MAX_MULT / _GATE_ENABLED
                "sectors_pe": len(_SECTOR_PE_MAP),
                "sectors_pb": len(_SECTOR_PB_MAP),
            },
            "providers_configured": [
                _safe_str(getattr(p, "name", "")) or p.__class__.__name__ if not isinstance(p, str) else p
                for p in (self._configured_providers or [])
            ],
            "cache_size": self._cache.size(),
            "features": self._features,
            "snapshot_pages": len(self._page_snapshots),
            "snapshot_symbols": len(self._symbol_snapshots),
        }

    def get_health(self) -> Dict[str, Any]:
        return self.health()

    def health_check(self) -> Dict[str, Any]:
        return self.health()

    def get_stats(self) -> Dict[str, Any]:
        return {
            "cache_size": self._cache.size(),
            "snapshot_pages": len(self._page_snapshots),
            "snapshot_symbols": len(self._symbol_snapshots),
            "version": __version__,
        }


# =============================================================================
# Module-level helpers
# =============================================================================
def normalize_row_to_schema(row: Dict[str, Any], sheet: str = "Market_Leaders") -> Dict[str, Any]:
    headers, keys = get_sheet_spec(sheet)
    return _normalize_to_schema_keys(keys, headers, row)


# =============================================================================
# Engine instance globals
# =============================================================================
_ENGINE_INSTANCE: Optional[DataEngineV5] = None
_ENGINE_LOCK = asyncio.Lock()


async def get_engine(
    settings: Any = None,
    symbols_reader: Any = None,
    rows_reader: Any = None,
    providers: Optional[Sequence[Any]] = None,
    cache_ttl_seconds: Optional[int] = None,
) -> DataEngineV5:
    # v5.77.8: keep module-level ENGINE/engine/_ENGINE aliases in sync. Earlier
    # versions bound those at module load time when _ENGINE_INSTANCE was still
    # None, and never updated them — so `from core.data_engine_v2 import ENGINE`
    # gave routes a permanent `None`. We still recommend `await get_engine()` or
    # `get_engine_if_ready()` as the canonical accessors, but updating the
    # aliases removes the silent-None footgun for legacy imports.
    global _ENGINE_INSTANCE, ENGINE, engine, _ENGINE
    if _ENGINE_INSTANCE is not None:
        return _ENGINE_INSTANCE
    async with _ENGINE_LOCK:
        if _ENGINE_INSTANCE is not None:
            return _ENGINE_INSTANCE
        _ENGINE_INSTANCE = DataEngineV5(
            settings=settings,
            symbols_reader=symbols_reader,
            rows_reader=rows_reader,
            providers=providers,
            cache_ttl_seconds=cache_ttl_seconds,
        )
        ENGINE = _ENGINE_INSTANCE
        engine = _ENGINE_INSTANCE
        _ENGINE = _ENGINE_INSTANCE
        return _ENGINE_INSTANCE


async def close_engine() -> None:
    # v5.77.8: clear the module-level aliases alongside _ENGINE_INSTANCE so
    # post-close imports correctly see None and re-initialize on next request.
    global _ENGINE_INSTANCE, ENGINE, engine, _ENGINE
    if _ENGINE_INSTANCE is None:
        return
    try:
        await _ENGINE_INSTANCE.aclose()
    finally:
        _ENGINE_INSTANCE = None
        ENGINE = None
        engine = None
        _ENGINE = None


def get_engine_if_ready() -> Optional[DataEngineV5]:
    return _ENGINE_INSTANCE


def peek_engine() -> Optional[DataEngineV5]:
    return _ENGINE_INSTANCE


def get_cache() -> Optional[MultiLevelCache]:
    if _ENGINE_INSTANCE is None:
        return None
    return _ENGINE_INSTANCE._cache


# =============================================================================
# Module-level engine handles — read this carefully if you write routes
# -----------------------------------------------------------------------------
# `ENGINE`, `engine`, and `_ENGINE` are convenience aliases for code that has
# a synchronous context and a strong guarantee the engine is already
# initialized (e.g. a request handler running after FastAPI startup). They are
# kept in sync by `get_engine()` / `close_engine()`.
#
# *** ROUTE DISCIPLINE — IMPORTANT ***
# Python's `from X import Y` captures the VALUE of `Y` at import time. If a
# route does
#
#     from core.data_engine_v2 import ENGINE
#
# at module load — before `get_engine()` has run — that route will hold a
# permanent reference to `None`, and no amount of reassignment inside this
# module can rebind the name in the route's namespace. This is a Python
# semantics fact, not an engine bug. The fix lives in the route, not here.
#
# Use ONE of these patterns in route code:
#
#     # ASYNC (preferred):
#     engine = await get_engine()
#     rows = await engine.get_sheet_rows("Global_Markets")
#
#     # SYNC fallback (only if you already know the engine is up):
#     from core import data_engine_v2 as _engine_module
#     engine = _engine_module.ENGINE          # fresh attribute lookup each call
#     if engine is None:
#         raise RuntimeError("Engine not initialized")
#
# Do NOT use:
#     from core.data_engine_v2 import ENGINE  # captures None at import time
#
# (The audit consensus has flagged this in prior reviews; this comment
#  exists so anyone touching the engine sees the warning at the same time
#  they see the names being defined.)
# =============================================================================
ENGINE: Optional[DataEngineV5] = _ENGINE_INSTANCE
engine: Optional[DataEngineV5] = _ENGINE_INSTANCE
_ENGINE: Optional[DataEngineV5] = _ENGINE_INSTANCE


# =============================================================================
# Backward-compat class aliases
# =============================================================================
DataEngineV4 = DataEngineV5
DataEngineV3 = DataEngineV5
DataEngineV2 = DataEngineV5
DataEngine = DataEngineV5


__all__ = [
    "__version__",
    "DataEngineV5", "DataEngineV4", "DataEngineV3", "DataEngineV2", "DataEngine",
    "get_engine", "close_engine", "get_engine_if_ready", "peek_engine", "get_cache",
    "UnifiedQuote", "QuoteQuality", "DataSource",
    "INSTRUMENT_CANONICAL_KEYS", "INSTRUMENT_CANONICAL_HEADERS",
    "INSTRUMENT_SHEETS", "SPECIAL_SHEETS", "STATIC_CANONICAL_SHEET_CONTRACTS",
    "TOP10_REQUIRED_FIELDS", "TOP10_REQUIRED_HEADERS",
    "INSIGHTS_HEADERS", "INSIGHTS_KEYS",
    "DATA_DICTIONARY_HEADERS", "DATA_DICTIONARY_KEYS",
    "get_sheet_spec", "normalize_row_to_schema", "normalize_symbol", "get_symbol_info",
    "ENGINE", "engine",
    "reset_provider_executor",
]


# =============================================================================
# v5.77.11 module-load INFO banner
# -----------------------------------------------------------------------------
# Emitted exactly once when this module is loaded. Confirms in the Render
# startup log that the engine is actually live. Uses __version__ so this label
# is also automatically in sync with the constant — same approach now used
# by the CLASSIFIER and RANK diagnostic logs (see v5.77.11 WHY block).
# =============================================================================
if logger.isEnabledFor(logging.INFO):
    try:
        logger.info(
            "[engine_v2 v%s] module loaded; canonical_schema=%d",
            __version__, len(INSTRUMENT_CANONICAL_KEYS),
        )
    except Exception:
        pass
