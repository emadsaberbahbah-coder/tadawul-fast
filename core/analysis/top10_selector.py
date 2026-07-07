#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/analysis/top10_selector.py
================================================================================
Top 10 Selector — v4.21.0
================================================================================
LIVE • SCHEMA-FIRST • ROUTE-COMPATIBLE • ENGINE-SELF-RESOLVING • JSON-SAFE
TOP10-METADATA GUARANTEED • SOURCE-PAGE SAFE • SNAPSHOT FALLBACK SAFE
SYNC+ASYNC CALLER TOLERANT • DISPLAY-HEADER TOLERANT • WRAPPER-PAYLOAD SAFE
PARTIAL-DEGRADATION SAFE • DIRECT-SYMBOLS SAFE • TIMEOUT-GUARDED
INSIGHTS-COLUMNS AWARE • DATA-QUALITY-AWARE RANKING
CRITERIA-v3.1.0 HARD-FILTER CONSUMER (v4.12.0)
DECISION-MATRIX AWARE • CANDLESTICK AWARE (v4.12.0)
CANONICAL-BUCKET ROUTED • REAL OVERALL-RANK FALLBACK (v4.13.0)
8-TIER VOCABULARY AWARE • PRIORITY-BAND ROUTED • CASCADE-BRIDGE READY (v4.14.0)
PAGINATION-ENVELOPE SAFE • FULL-UNIVERSE INGEST • LIMIT-FILL BACKFILL (v4.15.0)
VINTAGE-PROTECTED MERGE • GATE-FIELD QUARANTINE • ADMISSION-FILTERED (v4.16.0)
FINAL-ROW GATE • GATE-KEY PROJECTION GUARANTEE (v4.17.0)
GOLDEN-COMPOSITE RANKED • TIER-DISCIPLINED • SELL-CLASS SAFE
RANK-INTEGRITY GUARANTEED • SECTOR-DIVERSIFIED (v4.18.0)
BUDGET-GRACEFUL • DE-CLAMPED TIMEOUTS • COVERAGE-AUDITED
ENRICHMENT-PROJECTION GUARANTEED (v4.19.0)
SECTOR-TAXONOMY UNIFIED · YAHOO->GICS CAP CANON (v4.20.0)
SELECTION-STABILITY LAYER · MEMBERSHIP HYSTERESIS · DAY-KEYED (v4.21.0)

================================================================================
What v4.21.0 fixes/adds (over v4.20.0)  --  SELECTION-STABILITY LAYER
================================================================================
OPERATOR FINDING (Emad, 2026-07-07): the Top-10 reshuffles intraday and day to
day; a list that churns cannot be traded without constant sell/buy round-trips.
Root causes: (a) scores cluster near the cutoff, so tiny price moves flip ranks
8-14; (b) every build was memoryless -- the selector re-decided membership from
scratch each run.

FIX: an OPT-IN membership-hysteresis layer applied AFTER the four-pool fill and
BEFORE projection. Engaged ONLY when the caller passes `stability_state` (a
mapping or JSON string; `{}` bootstraps) and/or a `stability` config mapping in
the request body -- with neither present the build is BYTE-IDENTICAL to v4.20.0
(no new columns, no meta keys, no behavior change). No new ENV vars.

Rules (all knobs operator-tunable via `stability`: confirm_days=3, exit_days=3,
rank_buffer=15, smooth_days=5):
  - ENTRY CONFIRMATION: a challenger must appear in the RAW fill result on
    `confirm_days` consecutive days before it may take a seat; until then it is
    reported in meta.stability.audit.pending (for the NEAR-MISS zone).
  - EXIT GRACE: an incumbent leaves only after missing the raw result on
    `exit_days` consecutive days ("GRACE n/m missed" while holding).
  - RANK-JITTER IMMUNITY: a day an incumbent misses the raw top-N but still
    ranks <= rank_buffer across ALL pools does NOT count against it (the grace
    counter pauses; it does not reset).
  - DISPLACEMENT: with seats full, a CONFIRMED challenger replaces the weakest
    incumbent only when its SMOOTHED score is higher — persistent superiority
    displaces; noise never does. Fast-track fills never displace.
  - HARD EXIT: a symbol absent from ALL pools (gated / SELL-class / dropped
    upstream) exits IMMEDIATELY -- safety verdicts are never grace-held.
  - CAPACITY EXIT: if `limit` shrinks, the lowest-smoothed incumbents beyond
    the new seat count exit (audited as exited_capacity).
  - FAST-TRACK FILL: empty seats (bootstrap / mass exits) fill from today's
    raw order so the sheet never under-fills; flagged "FAST-TRACK".
  - SMOOTHING & ORDER: members are ordered by the mean of the last
    `smooth_days` daily composite points (score_smoothed); top10_rank therefore
    follows the SMOOTHED order, not the instantaneous one.
  - DAY-KEYED COUNTERS: all counters advance at most once per UTC date;
    same-day re-runs refresh the day's score point and re-evaluate seating but
    cannot advance confirmation/exit clocks -- intraday churn is structurally
    removed (fast-track seat-fills and hard exits remain live intraday, by
    design).
State is round-tripped: the updated blob is returned in
meta.stability.state; the caller (16_Decision_Top10.gs) persists it and posts
it back on the next refresh. State is self-pruning (non-members idle >14 days
drop; hard cap 400 symbols).
Output columns (appended to headers/keys ONLY when engaged, mirroring the
v4.17.0 gate-append pattern; `stability_keys_appended` echoed in meta):
  Stability Status | Days In List | Entry Date | Score Smoothed |
  Stability Trend.
HONEST FRAMING: this improves ACTIONABILITY (turnover, entry timing, whipsaw
resistance). It does NOT improve 12M forecast accuracy and claims nothing of
the sort -- forecast quality remains gated on Performance_Log cohort maturity.
Additive: five new pure functions + one constants block; no functions removed;
no signatures changed; all v4.20.0 WHY blocks carried verbatim.

================================================================================
What v4.20.0 fixes/adds (over v4.19.0)  --  A2: SECTOR-TAXONOMY UNIFICATION
================================================================================
TFB Track A / A2 (the audit's H2 "load-bearing" finding). The W-5 sector
diversification cap keyed on the RAW provider sector string (lower-cased only),
so the SAME economic sector under two provider vocabularies counted as two
buckets and the cap leaked: a Saudi bank (GICS "Financials", from
data_engine_v2._KSA_SYMBOL_SECTOR) and a US bank (Yahoo "Financial Services",
raw from the Yahoo fundamentals provider) each took their OWN cap slot. Only six
sector spellings differ between the two 11-sector taxonomies; every other name
already matches across both.

FIX (default OFF -> byte-identical v4.19.0): new gate TFB_TOP10_SECTOR_NORMALIZE.
When ON, _sector_key() first canonicalizes the sector via
core.sectors.normalize_sector (Yahoo->GICS, the SINGLE shared map -- the same
canonical map opportunity_builder's TFB_OPP_SECTOR_NORMALIZE consumes, so the two
tabs cannot drift) BEFORE lower-casing, so one economic sector occupies one
bucket. Every remap is counted in fill_meta["sector_normalize"] and surfaced in
result meta for audit. With the gate OFF the canon step returns the raw string
unchanged, so output is byte-identical to v4.19.0; no functions removed; one
constant + one import + one nested-helper line added.
================================================================================
What v4.19.0 fixes/adds (over v4.18.0)  --  FIX X: GRACEFUL TOTAL BUDGET +
DE-CLAMPED TIMEOUTS + COVERAGE AUDIT + ENRICHMENT PROJECTION
================================================================================
Live Render shell runs (2026-06-11, post universe fix, env TOTAL=70/PAGE=30/
ENGINE=20) exposed the v4.18.0 failure shape this release closes: a COLD
full-universe build hit the outer `asyncio.wait_for(_inner(), TOTAL)` wall at
exactly 70.0s and returned rows:0 with NULL meta -- the hard cancel destroys
every candidate already collected and every diagnostic. A selector whose
degraded mode is "throw everything away silently" violates this module's own
PARTIAL-DEGRADATION-SAFE charter. Separately, the live ML/Top_10 workbook
diff confirmed an 18-column enrichment gap on organic selector rows (e.g.
the selection_reason said Priority=P3 while the Priority Band COLUMN sat
blank): values present on the row never survived projection because the
exact-key copy has no alias fallback, and the fallback contract lacked the
analyst/trend/scoring-provenance block entirely.

  X-1 GRACEFUL TOTAL BUDGET (the rows:0 / null-meta fix).
      The builder now computes an INTERNAL soft deadline:
          deadline = start + (BUILDER_TOTAL_TIMEOUT_SEC
                              - TFB_TOP10_TOTAL_RESERVE_SEC [default 6s])
      The page loop checks remaining budget BEFORE each page, clamps every
      data-path engine-call timeout to the remaining budget, and on
      exhaustion BREAKS COLLECTION (meta: budget_exhausted=true,
      pages_skipped=[...]) instead of being cancelled -- gating, tiering,
      ranking, and projection then run on whatever was collected, so a
      budget-bound run returns a PARTIAL ranked payload with FULL meta.
      The outer wait_for survives only as a BACKSTOP at
      TOTAL + TFB_TOP10_OUTER_GRACE_SEC (default 8s); reaching it means the
      graceful path itself wedged, and its fallback payload now at least
      carries the timeout-configuration block for diagnosis.

  X-2 DE-CLAMPED HARDCODED TIMEOUT LITERALS. v4.14.0-era literal caps
      (min(ENGINE,6/7/5), min(PAGE,8/10), min(TOTAL/2,12)) silently
      overrode the operator's env configuration -- with ENGINE=20 live, a
      cold snapshot call was still cut at 6s. All DATA-PATH call sites now
      use the configured constant clamped only by the X-1 remaining budget
      (`_budget_timeout`). The ONLY surviving literal clamp is the
      engine-RESOLUTION probe `_safe_call_zero_arg` (min(ENGINE,6)), kept
      deliberately: module scanning must stay cheap regardless of how
      generous the data-path timeouts are. Defaults bump to the
      live-proven values: ENGINE 8->20, PAGE 12->30, TOTAL 32->70 (env
      still wins; a missing env no longer regresses to starvation values).

  X-3 COVERAGE / STARVATION AUDIT. Per scanned page the selector now
      records the page UNIVERSE size (engine get_page_symbols, fail-soft,
      tightly budget-clamped) beside the rows actually ingested, plus a
      computed page_coverage ratio and a global universe_starved flag (a
      page with a non-empty universe yielding ZERO ingested rows). A cold
      run that returns 3 rows is now visibly a STARVED run rather than
      silently mistakable for a 3-instrument universe, and GM's
      SOURCE_PAGE_LIMIT=80-of-1931 truncation is visible instead of
      implicit.

  X-4 18-COLUMN ENRICHMENT PROJECTION GUARANTEE.
      (a) `_rank_and_project_rows` falls back to alias-aware extraction
          (`_extract_value_by_aliases`) for any contract key whose
          exact-key read is blank, so a value present under ANY known
          spelling (snake / camelCase / display-header) survives
          projection; exact-key values present are untouched.
      (b) ROW_KEY_ALIASES gains the analyst/trend/scoring-provenance
          block: analyst_rating, target_price, upside_downside_pct,
          signal, trend_1m/3m/12m, st_signal, recommendation_source,
          scoring_schema_version, scoring_errors, opportunity_source,
          overall_score_raw, overall_penalty_factor, provider_secondary,
          row_source, forecast_source.
      (c) DEFAULT_FALLBACK_KEYS/HEADERS gain the same 18 columns,
          appended before the Top10 trio (order-safe: the registry-served
          118 governs live column order downstream, and the GAS writer
          maps by header name in degraded fallback mode).

  X-5 SOURCE ATTRIBUTION. New meta `selected_sources` maps each selected
      symbol to the source_page that actually donated it, making the
      Source= component of selection_reason independently auditable (the
      2026-06-11 audit caught a Source=My_Portfolio mislabel minted in the
      emergency-list era; with the universe fixed and the map exposed, any
      recurrence is one glance away).

  Operational note (deployment sequencing, unchanged by this fix): a COLD
  full-universe build costs ~4.6 min of engine scoring and CANNOT fit any
  budget inside Render's ~100s edge window. Production sequence remains:
  GAS Refresh Market_Leaders (warms the worker's engine cache) ->
  standalone Top_10 immediately, with NO redeploy in between (each deploy
  wipes the in-process cache). With 2 gunicorn workers a request can land
  on the cold sibling worker; X-1 turns that case from rows:0/null-meta
  into a partial, fully-diagnosed payload.

Knobs (env):
  TFB_TOP10_TOTAL_RESERVE_SEC=<float> -> X-1 reserve (default 6; the slice
                                         of TOTAL kept for gate/tier/rank/
                                         project after collection stops)
  TFB_TOP10_OUTER_GRACE_SEC=<float>   -> X-1 backstop grace (default 8)
  (X-2/X-3/X-4/X-5 carry no kill switch: they only restore operator-
  configured timeouts and widen honesty/coverage; there is no prior
  behavior worth restoring.)
New meta: budget_exhausted / pages_skipped / page_universe / page_coverage /
universe_starved / selected_sources / timeouts{engine_call_sec, page_sec,
total_sec, total_reserve_sec, outer_grace_sec}.

================================================================================
What v4.18.0 fixes/adds (over v4.17.0)  --  FIX W: GOLDEN COMPOSITE +
TIER DISCIPLINE + RANK INTEGRITY + SECTOR DIVERSIFICATION
================================================================================
Live workbook audits (2026-06-10 / 2026-06-11, full-workbook diff + golden
2,192-row offline re-rank) confirmed defects that survive v4.17.0, plus a
structural ranking gap this release closes. All changes are fail-soft and
individually kill-switchable.

  W-1 GOLDEN COMPOSITE — reliability finally ranks (TFB_TOP10_GOLDEN_COMPOSITE,
      default ON; =0 restores the v4.17.0 legacy score verbatim).
      The v4.17.0 `_selector_score` never consumed
      `forecast_reliability_score` — ironic, since Fix U/Fix V exist to make
      that field FRESH on every candidate. The recalibrated base score is the
      agreed golden composite over 0-100 components:
          0.40 * overall_score
        + 0.25 * forecast_reliability_score   (fallback: forecast_confidence)
        + 0.20 * conviction_score
        + 0.15 * horizon-ROI component        (0..100; ±35% ROI = full scale,
                                               0% ROI = 50)
        -  risk penalty: (risk_score - 50) * 0.30, capped at 15, only when
           risk_score > 50.
      Weights renormalize over the components actually PRESENT on a row, so a
      missing conviction does not structurally sink a row against its peers.
      The orthogonal nudges of the legacy score are retained on top with the
      same magnitudes (8-tier reco bump, candlestick nudge, warnings
      penalties, direct-symbol priority bump, richness bump) — EXCEPT the
      legacy +conviction*0.05 nudge, which is omitted in golden mode because
      conviction is now a first-class 20% component.

  W-2 TIER DISCIPLINE + SELL-CLASS EXCLUSION.
      (a) SELL-CLASS EXCLUSION (TFB_TOP10_EXCLUDE_SELL_CLASS, default ON):
          rows whose recommendation normalizes to REDUCE / SELL /
          STRONG_SELL / AVOID can NEVER appear on the Top 10 — not as picks
          and not as backfill. v4.17.0's limit-fill could backfill a SELL
          name into a "best investments" list whenever the passing pool ran
          short (a real path with only ~51 INVESTABLE rows live). Excluded
          rows are meta-audited (`sell_class_excluded*`). If exclusion
          empties the pool entirely (all-SELL universe), the payload
          honestly degrades toward zero rows rather than recommending SELLs.
      (b) TIER SPLIT: Tier-1 = investability_status == INVESTABLE AND
          reliability >= TFB_TOP10_TIER1_RELIABILITY_FLOOR (default 65).
          Everything else admissible (WATCHLIST, BLOCKED-but-admissible,
          low-reliability INVESTABLE, verdict-less) is Tier-2.
      (c) FILL ORDER (four pools, each sorted by composite + the v4.17.0
          tiebreak tuple):
              1. Tier-1 rows passing operator criteria      (label "T1")
              2. Tier-2 rows passing operator criteria      ("T2 BACKFILL[..]")
              3. Tier-1 rows failing operator criteria      ("T1 BACKFILL[..]")
              4. Tier-2 rows failing operator criteria      ("T2 BACKFILL[..]")
          A Tier-2 row can therefore never outrank any Tier-1 PICK. Pool 2
          ranks above pool 3 deliberately: a row the operator explicitly
          filtered out (criteria fail) is a last resort even when Tier-1 —
          operator intent outranks tier. Every non-pool-1 row carries an
          explicit bracketed label with the tier reason and/or the failing
          criterion, so the sheet is self-auditing.

  W-3 CANDIDATE-WIDE FRESH GATE (supersedes the v4.17.0 final-rows-only
      timing; same kill switch TFB_TOP10_FINAL_GATE).
      v4.17.0 ran `_apply_investability_gate` on the 10 FINAL rows only —
      AFTER selection — so tier/SELL decisions would have ranked on stale or
      absent verdicts. v4.18.0 runs the same engine gate on ALL admissible
      candidates BEFORE tiering and ranking. The gate is pure-CPU,
      idempotent, and AA-tag-aware (engine v5.84.0+), so universe-wide
      application is cheap and safe. `final_gate_*` meta keys are preserved;
      `final_gate_scope` = "admissible_candidates" documents the new timing.

  W-4 RANK INTEGRITY — the duplicate-rank corruption fix.
      Root cause visible in this file: `_rank_and_project_rows` assigned
      `top10_rank` only when BLANK, so rows ingested via the Top10
      OUTPUT-PAGE fallback donor carried their OLD ranks into the new
      build — two overlapping runs produced the observed 13-row sheet with
      colliding ranks. v4.18.0 FORCE-overwrites `top10_rank = idx` and
      recomputes `rank_overall` fresh on every build, unconditionally.
      Contiguous unique ranks 1..N are now a structural guarantee of the
      payload. (The GAS-side clear-before-write remains queued as
      defense-in-depth for stale rows BELOW the new write window.)

  W-5 SECTOR DIVERSIFICATION CAP (TFB_TOP10_SECTOR_CAP, default 4; 0
      disables). Investment-director behavior: a Top 10 should not be ten
      names from one sector. During fill, a row whose sector already holds
      `cap` selections is deferred; if the pools cannot fill `limit`
      otherwise, deferred rows re-enter in order (labeled
      "[SECTOR-CAP RELAXED]") so the guarantee of a full list survives.
      Audited via `sector_cap_deferred` / `sector_cap_relaxed` meta.

  W-6 SELECTION-REASON TRANSPARENCY. The canonical reason now also carries
      `Rel=NN/100` (the reliability that golden ranking actually used) and
      `Gate=<investability_status>`, and every projected row's reason is
      prefixed with its tier label from W-2c.

  W-7 OUTPUT-FALLBACK TOP10-METADATA QUARANTINE. The previous-output donor
      (provenance rank 0) now also has `top10_rank`, `selection_reason`, and
      `criteria_snapshot` STRIPPED at ingest (in addition to the Fix U gate
      quarantine): these are artifacts of a PREVIOUS selector run and are
      stale by definition on re-ingest. Without this, a stale
      selection_reason would survive the fill-if-blank path and be served
      under a fresh tier label. Audited via `top10_metadata_stripped_count`.

  REMOVED-AS-SUPERSEDED (behavior, not API): the v4.13.0 direct-symbol
  top-up block and the v4.15.0 limit-fill backfill block are subsumed by the
  four-pool tiered fill — pools 2-4 exhaust the full admissible candidate
  set, and direct-symbol rows still float to the top of their pools via the
  +140 priority bump. `backfilled_count` / `backfill_symbols` meta keys are
  preserved and now mean "criteria-failing rows admitted with labels".

Kill switches (all env, all independent):
  TFB_TOP10_GOLDEN_COMPOSITE=0    -> v4.17.0 legacy selector score, verbatim
  TFB_TOP10_EXCLUDE_SELL_CLASS=0  -> SELL-class rows may rank again
  TFB_TOP10_SECTOR_CAP=0          -> no sector cap
  TFB_TOP10_FINAL_GATE=0          -> no gate application (tiering then relies
                                     on verdicts already present on rows)
  TFB_TOP10_TIER1_RELIABILITY_FLOOR=<float> -> Tier-1 reliability bar (65)
New meta: tier1_candidates / tier2_candidates / tier1_selected /
tier2_selected / tier2_selected_symbols / sell_class_excluded(_count) /
exclude_sell_class_enabled / golden_composite_enabled / composite_weights /
tier1_reliability_floor / sector_cap / sector_cap_deferred /
sector_cap_relaxed / final_gate_scope / top10_metadata_stripped_count.

================================================================================
What v4.17.0 fixes (over v4.16.1)  --  FIX V: FINAL-ROW GATE + KEY GUARANTEE
=============================================================================
Live shell audit (2026-06-10, post-v4.16.1 deploy) showed a real selector
build returning rows with NO gate columns at all: hydrated enriched-quote
rows never pass through the engine's investability gate (it runs on the
sheet-row path), and the resolved projection contract lacked the gate keys.
Fix U correctly stripped STALE donor verdicts, so the sheet's gate columns
went from stale-wrong to blank. v4.17.0 completes the fix:
  V-1: _ensure_gate_output_keys appends any missing gate output keys (and
       forecast_source) with aligned canonical headers after schema
       resolution, so fresh gate values cannot be dropped by projection.
  V-2: _apply_final_gate runs core.data_engine_v2._apply_investability_gate
       on each FINAL selected row immediately before projection — fresh
       verdicts from the same hydrated data, restoring reliability parity
       with Global_Markets (NVDA/META 71.5, MSFT 70.4 class of values).
       [v4.18.0 W-3 widens the application scope to ALL admissible
       candidates BEFORE tiering; the function and kill switch are the same.]
Both are fail-soft; TFB_TOP10_FINAL_GATE=0 restores v4.16.1 exactly.
New meta: final_gate_enabled / final_gate_available / final_gate_applied /
final_gate_errors / gate_keys_appended / projection_keys_count.

What v4.16.1 fixes (over v4.16.0)  --  AUDIT FOLLOW-UP: FULL GATE WRITE-SET
================================================================================
Pre-deployment cross-file audit (2026-06-10, selector vs data_engine_v2
v5.84.0): the v4.16.0 GATE_CRITICAL_KEYS quarantine covered 5 of the 9
columns the engine's _apply_investability_gate actually writes. The four
companion verdict columns -- conflict_type, final_decision_basis,
final_action, block_reason (all present on the Top_10 118-column sheet) --
could therefore still arrive STALE from a snapshot / output-fallback donor
and sit beside a fresh investability_status: e.g. a stale
final_action="INVEST" next to a fresh WATCHLIST, or an obsolete
block_reason explaining a verdict that no longer exists. v4.16.1 extends
GATE_CRITICAL_KEYS to the full write-set so all nine verdict columns
travel as ONE vintage. No other change; v4.16.0 logic, tests, and public
API are otherwise verbatim (never deployed -- v4.16.0 was superseded
pre-deployment by this audit).

================================================================================
What v4.16.0 fixes (over v4.15.0)  --  THE "STALE 46.4 RELIABILITY" BUG (Fix U)
================================================================================
Symptom: the live Top_10_Investments sheet showed PRE-Fix-S Forecast
Reliability Scores (NVDA/META 46.4, MSFT 44.8) on rows whose Investability
Status said INVESTABLE, while the SAME symbols on the Global_Markets source
page (refreshed the same morning by data_engine_v2 v5.83.3) showed the
POST-Fix-S values (71.5 / 70.4).  Audit date 2026-06-10; verified against
the live workbook by diffing Top_10 vs Global_Markets per symbol.

Root cause (merge vintage, in-module): the candidate pipeline merges rows
from FIVE donors of very different freshness --

    live page rows        (engine get_sheet_rows .......... fresh)
    direct-symbol rows    (engine quote batch .............. fresh)
    hydration rows        (engine quote batch .............. fresh)
    page SNAPSHOT rows    (engine cached sheet snapshot .... possibly stale)
    Top10 OUTPUT-PAGE
      fallback rows       (the PREVIOUS Top_10 output ...... stale by design)

-- but `_merge_row_prefer_richer` resolved every field by RICHNESS alone
(non-blank field count).  A fully-populated stale snapshot / previous-output
row (100+ filled fields) is almost always "richer" than a freshly fetched
candidate row, so its old gate fields (forecast_reliability_score,
investability_status, data_quality_score, ...) silently won the merge and
were written back to the sheet.  Because the previous Top_10 output is
itself a fallback donor, stale values could RE-ENTER the pool on every
rebuild -- a self-referential staleness loop with no expiry.

v4.16.0 phase changes:

  A. ROW PROVENANCE TAGGING.  Every ingested row is tagged with an internal
     `_row_provenance` rank at its fetch site: live page = 3, hydration /
     direct-symbol = 3, emergency = 2, page snapshot = 1, Top10 output-page
     fallback = 0.  The tag is internal-only: final projection copies ONLY
     schema-contract keys, so it never reaches the sheet or the payload rows.
     Untagged rows default to rank 3 (live) for back-compatibility with any
     external caller that feeds rows directly.

  B. PROVENANCE-AWARE MERGE (`_merge_row_provenance`).  When two rows for the
     same symbol merge, the HIGHER-provenance row's non-blank values now win
     unconditionally; the lower-provenance row may only fill blanks.  When
     ranks are EQUAL the v4.15.0 prefer-richer behavior is preserved verbatim
     (richer row's non-blank values win).  Two merge sites route through the
     new merge (`_merge_symbol_row_lists`, `_collect_candidate_rows._put_row`);
     the HYDRATION overlay in `_hydrate_page_rows` keeps its exact v4.15.0
     semantics (enriched non-blank values always overwrite the base row) and
     simply carries the max provenance rank forward, so a base row that was a
     quarantined snapshot is upgraded to fresh once hydrated.

  C. GATE-FIELD QUARANTINE for stale donors.  Rows arriving from the page
     SNAPSHOT donor (rank 1) or the Top10 OUTPUT-PAGE fallback donor (rank 0)
     have their gate-critical fields STRIPPED before entering the pool:
     forecast_reliability_score, investability_status, data_quality_score,
     provider_engine_conflict, scoring_errors (matched against snake_case,
     camelCase, and display-header spellings via the canonical key matcher).
     Principle: BLANK-OVER-STALE -- a visibly missing gate value is honest;
     a stale one masquerades as measurement.  Stripped-field counts are
     reported in meta (`gate_fields_quarantined_count`) so degraded runs
     stay auditable.  Non-gate fields (price, scores, fundamentals) are NOT
     stripped: stale-but-labeled context is still useful for ranking when
     nothing fresher exists.

  D. TOP10 ADMISSION FILTER (env-gated, DEFAULT ON;
     `TFB_TOP10_ADMISSION_FILTER=0` disables).  A row cannot be SELECTED or
     BACKFILLED into the final Top10 when any of these hold:
       - current_price is blank or <= 0          (reason: missing_price)
       - confidence (forecast_confidence falling back to confidence_score)
         is blank or <= 0                        (reason: zero_confidence)
       - warnings carry the engine tag
         recommendation_forced_hold_missing_price (reason: forced_hold)
     Root case from the 2026-06-10 audit: CPI.JSE sat in the live Top 10
     with NO price, confidence 0.00, and a forced-HOLD tag.  Excluded rows
     and their reasons are reported in meta (`admission_excluded`,
     `admission_excluded_count`) and logged, so the filter is auditable and
     reversible.  When the filter would empty the pool entirely, the
     degraded-mode guarantee is preserved: the v4.15.0 relaxed-pool path
     then operates on the admissible pool, and only if THAT is empty does
     the payload degrade to zero rows exactly as v4.15.0 did.

  E. VERSION BUMP 4.15.0 -> 4.16.0.  Cross-stack engine floor updated to
     data_engine_v2 v5.83.3 (live, verified via Render shell import check).
     All v4.15.0 public APIs preserved verbatim; `__all__` gains
     GATE_CRITICAL_KEYS and TOP10_ADMISSION_FILTER_ENABLED only.

================================================================================
What v4.15.0 fixes (over v4.14.0)  --  THE "8/5 NOT 10" BUG
================================================================================
Symptom: the live Top_10_Investments sheet returned only 5 (or 8) rows even
though the engine scored a full universe per page. Root-caused on Render:
`source_page_rows` showed exactly ONE row ingested per source page, and a
direct `_fetch_page_rows` probe returned `1 rows  syms=[None]`.

Root cause (parser, in-module): `core.data_engine_v2.get_sheet_rows` returns a
PAGINATION ENVELOPE shaped `{rows, rows_display, rows_matrix, limit, offset,
total}`. `_looks_like_row_dict` misclassified that envelope as a SINGLE data
row: it has no symbol/ticker, no top10_rank, and zero score-field hits, so it
fell through to the `len(non_meta) >= 4` heuristic — and `WRAPPER_KEYS`
listed `rows`/`rows_matrix` but NOT `rows_display`/`limit`/`offset`/`total`,
leaving exactly 4 "non-meta" keys → True. `_extract_rows_like` then did
`return [mapping]`, handing back the envelope itself as one (symbol-less) row
instead of descending into `rows`. Five pages x 1 envelope-row = 5 candidates.

v4.15.0 phase changes:

  A. WRAPPER GUARD in `_looks_like_row_dict`. After the strong positive row
     signals (symbol / ticker / top10_rank / selection_reason), a NEW guard
     returns False when the mapping carries a row-container LIST under any of
     rows / row_objects / records / items / results / quotes / data /
     rows_matrix / matrix. Such a mapping is a wrapper/envelope, never a
     single row. Symbol-bearing real rows still return True FIRST, so no
     genuine row is ever reclassified.

  B. WRAPPER_KEYS EXTENDED with pagination/envelope keys (rows_display, limit,
     offset, total, page, page_size, page_count, per_page, next_offset,
     has_more, pagination, generated_at, generated_at_utc, request_id,
     duration_ms) as defense-in-depth so the field-count heuristic also can
     no longer misfire on an envelope.

  C. LIMIT-FILL BACKFILL in `_build_top10_rows_async`. Once the parser is
     fixed the candidate universe is large enough to fill the Top10 normally,
     but when operator criteria trim the passing pool below `limit`, the
     selector now tops up to `limit` from the best-scoring NON-passing
     candidates (investable rows always ranked first). Backfill rows are
     flagged in `selection_reason` ("[BACKFILL: below criteria ...]") and
     counted in meta as `backfilled_count` + `backfill_symbols`, so the gate
     intent stays visible and auditable. Direct-symbol top-up (v4.13.0)
     remains and runs first.
     [v4.18.0: both top-up blocks are superseded by the four-pool tiered
     fill; meta keys preserved.]

  D. VERSION BUMP 4.14.0 -> 4.15.0. Cross-stack refs updated to the current
     family floor. All v4.14.0 public APIs preserved verbatim; `__all__`
     unchanged.

================================================================================
Cross-stack family (Jun 2026 floor)
================================================================================
  - core.sheets.schema_registry        v2.15.0  (115-col market layout +
                                                  analyst/trend keys; Top_10
                                                  118; My_Portfolio 122)
  - core.scoring                       v5.7.4
  - core.reco_normalize                v8.0.0   (8-tier canonical)
  - core.schemas                       v7.0.0   (8-tier enum + 9 cascade-bridge
                                                  fields on UnifiedQuote)
  - core.data_engine_v2                v5.85.3  (115-col canonical; analyst/
                                                  trend block Fix AD; registry
                                                  projection seam Fix AE;
                                                  get_sheet_rows returns
                                                  {rows, rows_display,
                                                  rows_matrix, limit, offset,
                                                  total} envelope)
  - core.buckets                       v1.0.0
  - core.analysis.criteria_model       v3.1.1
  - core.analysis.investment_advisor_engine v4.5.0
  - core.analysis.insights_builder     v8.2.0   (sibling consumer of the same
                                                  8-tier vocabulary + Top Picks)

================================================================================
Defaults & back-compat
================================================================================
v4.18.0 changes RANKING POLICY by default (golden composite, tier order,
SELL-class exclusion, sector cap) — that is the point of the release — but
every policy change carries its own kill switch restoring the prior
behavior, and the runtime contract is unchanged:
  - Payload shape, route entry points, and all public function names are
    preserved verbatim.
  - `top10_rank` is now ALWAYS 1..N contiguous (W-4); any consumer that
    depended on stale preserved ranks was depending on the corruption bug.
  - All v4.17.0 meta keys are preserved; v4.18.0 only ADDS keys.

================================================================================
History (preserved)
================================================================================
[v4.14.0 8-tier vocabulary / priority-band routing / cascade-bridge, and
v4.13.0 / v4.12.0 / earlier changelog history preserved verbatim in prior
source; trimmed in this baseline for editing efficiency per this file's
established header convention.]
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import importlib
import inspect
import json
import logging
import math
import os
from collections import OrderedDict
import re
import sys
import time
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, MutableMapping

logger = logging.getLogger("core.analysis.top10_selector")
logger.addHandler(logging.NullHandler())

TOP10_SELECTOR_VERSION = "4.21.0"
# v4.12.0 Phase F: TFB module-version convention alias (mirrors
# schema_registry v2.15.0, scoring v5.7.4, reco_normalize v8.0.0,
# insights_builder v8.2.0, criteria_model v3.1.1, advisor_engine v4.5.0,
# data_engine_v2 v5.85.3, schemas v7.0.0).
__version__ = TOP10_SELECTOR_VERSION

# v4.14.0 Phase B/K: capability marker for downstream consumers. Surfaced
# in result meta as `reco_8tier_aware: true` and exported via __all__ so
# callers can content-check the selector's 8-tier alignment state.
RECO_8TIER_AWARE_VERSION = "v4.14.0"
OUTPUT_PAGE = "Top_10_Investments"

DEFAULT_SOURCE_PAGES = [
    "Market_Leaders",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
    "My_Portfolio",
]

DERIVED_OR_NON_SOURCE_PAGES = {
    "KSA_TADAWUL",
    "Advisor_Criteria",
    "AI_Opportunity_Report",
    "Insights_Analysis",
    "Top_10_Investments",
    "Data_Dictionary",
}

TOP10_REQUIRED_FIELDS = (
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
)

TOP10_REQUIRED_HEADERS = {
    "top10_rank": "Top10 Rank",
    "selection_reason": "Selection Reason",
    "criteria_snapshot": "Criteria Snapshot",
}

DEFAULT_FALLBACK_KEYS = [
    "symbol", "name", "asset_class", "exchange", "currency", "country", "sector", "industry",
    "current_price", "previous_close", "open_price", "day_high", "day_low",
    "week_52_high", "week_52_low", "price_change", "percent_change", "week_52_position_pct",
    "volume", "avg_volume_10d", "avg_volume_30d", "market_cap", "float_shares", "beta_5y",
    "pe_ttm", "pe_forward", "eps_ttm", "dividend_yield", "payout_ratio", "revenue_ttm",
    "revenue_growth_yoy", "gross_margin", "operating_margin", "profit_margin", "debt_to_equity",
    "free_cash_flow_ttm",
    "rsi_14", "volatility_30d", "volatility_90d", "max_drawdown_1y",
    "var_95_1d", "sharpe_1y", "risk_score", "risk_bucket",
    "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio",
    "intrinsic_value", "upside_pct", "valuation_score",
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
    "sector_relative_score", "conviction_score", "top_factors", "top_risks", "position_size_hint",
    "recommendation_detailed", "recommendation_priority",
    "candlestick_pattern", "candlestick_signal", "candlestick_strength",
    "candlestick_confidence", "candlestick_patterns_recent",
    # v4.19.0 [FIX X-4c] — analyst/trend/scoring-provenance block appended
    # (order-safe: the registry-served 118 governs live column order
    # downstream, and the GAS writer maps by header name in degraded
    # fallback mode).
    "analyst_rating", "target_price", "upside_downside_pct", "signal",
    "trend_1m", "trend_3m", "trend_12m", "st_signal",
    "recommendation_priority_band", "recommendation_source", "provider_rating",
    "scoring_recommendation_source", "scoring_schema_version",
    "opportunity_source", "overall_score_raw", "overall_penalty_factor",
    "provider_secondary", "row_source",
    "top10_rank", "selection_reason", "criteria_snapshot",
]

DEFAULT_FALLBACK_HEADERS = [
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
    "Current Price", "Previous Close", "Open", "Day High", "Day Low", "52W High", "52W Low",
    "Price Change", "Percent Change", "52W Position %",
    "Volume", "Avg Volume 10D", "Avg Volume 30D", "Market Cap", "Float Shares", "Beta (5Y)",
    "P/E (TTM)", "P/E (Forward)", "EPS (TTM)", "Dividend Yield", "Payout Ratio", "Revenue (TTM)",
    "Revenue Growth YoY", "Gross Margin", "Operating Margin", "Profit Margin", "Debt/Equity",
    "Free Cash Flow (TTM)",
    "RSI (14)", "Volatility 30D", "Volatility 90D", "Max Drawdown 1Y", "VaR 95% (1D)",
    "Sharpe (1Y)", "Risk Score", "Risk Bucket",
    "P/B", "P/S", "EV/EBITDA", "PEG", "Intrinsic Value", "Upside %", "Valuation Score",
    "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M",
    "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M",
    "Forecast Confidence", "Confidence Score", "Confidence Bucket",
    "Value Score", "Quality Score", "Momentum Score", "Growth Score",
    "Overall Score", "Opportunity Score", "Rank (Overall)",
    "Fundamental View", "Technical View", "Risk View", "Value View",
    "Recommendation", "Recommendation Reason", "Horizon Days", "Invest Period Label",
    "Position Qty", "Avg Cost", "Position Cost", "Position Value", "Unrealized P/L",
    "Unrealized P/L %",
    "Data Provider", "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",
    "Sector-Adj Score", "Conviction Score", "Top Factors", "Top Risks", "Position Size Hint",
    "Recommendation Detail", "Reco Priority",
    "Candle Pattern", "Candle Signal", "Candle Strength",
    "Candle Confidence", "Recent Patterns (5D)",
    # v4.19.0 [FIX X-4c] — matching headers for the 18 appended keys.
    "Analyst Rating", "Target Price", "Upside/Downside %", "Signal",
    "Trend 1M", "Trend 3M", "Trend 12M", "ST Signal",
    "Priority Band", "Recommendation Source", "Provider Rating",
    "Scoring Reco Source", "Scoring Schema Version",
    "Opportunity Source", "Overall Score (Raw)", "Overall Penalty Factor",
    "Provider Secondary", "Row Source",
    "Top10 Rank", "Selection Reason", "Criteria Snapshot",
]

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
    "intrinsic_value": ("intrinsic_value", "intrinsicValue", "fair_value", "target_intrinsic"),
    "upside_pct": ("upside_pct", "upsidePct", "upside_percent", "upside", "intrinsic_upside"),
    "valuation_score": ("valuation_score", "valuationScore"),
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
    "fundamental_view": ("fundamental_view", "fundamentalView", "fundamentals_view"),
    "technical_view": ("technical_view", "technicalView", "technicals_view"),
    "risk_view": ("risk_view", "riskView"),
    "value_view": ("value_view", "valueView", "valuation_view"),
    "sector_relative_score": ("sector_relative_score", "sectorRelativeScore", "sector_adj_score", "sectorAdjScore"),
    "conviction_score": ("conviction_score", "convictionScore", "conviction"),
    "top_factors": ("top_factors", "topFactors", "key_factors", "keyFactors"),
    "top_risks": ("top_risks", "topRisks", "key_risks", "keyRisks"),
    "position_size_hint": ("position_size_hint", "positionSizeHint", "position_size", "size_hint"),
    "recommendation_detailed": (
        "recommendation_detailed", "recommendationDetailed", "recommendation_detail",
        "recommendationDetail", "decision_detailed", "decision_recommendation",
    ),
    "recommendation_priority": (
        "recommendation_priority", "recommendationPriority", "reco_priority",
        "recoPriority", "decision_priority",
    ),
    "candlestick_pattern": ("candlestick_pattern", "candlestickPattern", "candle_pattern", "candlePattern"),
    "candlestick_signal": ("candlestick_signal", "candlestickSignal", "candle_signal", "candleSignal"),
    "candlestick_strength": ("candlestick_strength", "candlestickStrength", "candle_strength", "candleStrength"),
    "candlestick_confidence": ("candlestick_confidence", "candlestickConfidence", "candle_confidence", "candleConfidence"),
    "candlestick_patterns_recent": (
        "candlestick_patterns_recent", "candlestickPatternsRecent", "candle_patterns_recent",
        "recent_candle_patterns", "recent_patterns",
    ),
    # Cascade-bridge / scoring-provenance (3) — v4.14.0; produced by
    # data_engine_v2 v5.74.0+ / advisor_engine v4.5.0 / insights_builder
    # v8.0.0 / schemas v7.0.0 UnifiedQuote.
    "recommendation_priority_band": (
        "recommendation_priority_band", "recommendationPriorityBand",
        "reco_priority_band", "recoPriorityBand", "priority_band", "priorityBand",
    ),
    "provider_rating": (
        "provider_rating", "providerRating", "vendor_rating", "vendorRating",
        "raw_provider_rating", "rawProviderRating",
    ),
    "scoring_recommendation_source": (
        "scoring_recommendation_source", "scoringRecommendationSource",
        "scoring_source", "scoringSource", "recommendation_source", "recommendationSource",
    ),
    # v4.19.0 [FIX X-4b] — analyst / trend / scoring-provenance block
    # (engine v5.85.x Fix AD/AE columns; registry v2.15.0 115/118 layout).
    "analyst_rating": ("analyst_rating", "analystRating", "analyst_recommendation", "analystRecommendation"),
    "target_price": ("target_price", "targetPrice", "analyst_target_price", "provider_target_price", "price_target"),
    "upside_downside_pct": ("upside_downside_pct", "upsideDownsidePct", "upside_downside", "target_upside_pct", "targetUpsidePct"),
    "signal": ("signal", "engine_signal", "overall_signal"),
    "trend_1m": ("trend_1m", "trend1m"),
    "trend_3m": ("trend_3m", "trend3m"),
    "trend_12m": ("trend_12m", "trend12m"),
    "st_signal": ("st_signal", "stSignal", "short_term_signal", "shortTermSignal"),
    "recommendation_source": ("recommendation_source", "recommendationSource", "reco_source", "recoSource"),
    "scoring_schema_version": ("scoring_schema_version", "scoringSchemaVersion", "schema_version"),
    "scoring_errors": ("scoring_errors", "scoringErrors"),
    "opportunity_source": ("opportunity_source", "opportunitySource"),
    "overall_score_raw": ("overall_score_raw", "overallScoreRaw", "raw_overall_score"),
    "overall_penalty_factor": ("overall_penalty_factor", "overallPenaltyFactor", "penalty_factor"),
    "provider_secondary": ("provider_secondary", "providerSecondary", "secondary_provider", "data_provider_secondary"),
    "row_source": ("row_source", "rowSource"),
    "forecast_source": ("forecast_source", "forecastSource"),
    "selection_reason": ("selection_reason", "selector_reason"),
    "top10_rank": ("top10_rank", "rank"),
    "criteria_snapshot": ("criteria_snapshot", "criteria_json"),
    "source_page": ("source_page", "page", "sheet", "sheet_name"),
}

CANONICAL_KEY_SET = set(DEFAULT_FALLBACK_KEYS)
WRAPPER_KEYS = {
    "status", "page", "sheet", "sheet_name", "route_family", "headers", "display_headers", "sheet_headers",
    "column_headers", "keys", "columns", "fields", "rows", "rows_matrix", "matrix", "row_objects", "records",
    "items", "item", "quotes", "quote", "data", "record", "result", "payload", "response", "output", "meta", "count", "version",
    "snapshot", "envelope", "content", "schema", "sheet_spec", "spec",
    # v4.15.0 Phase B: pagination / envelope keys. data_engine_v2 v5.83.2's
    # get_sheet_rows returns {rows, rows_display, rows_matrix, limit, offset,
    # total}; without these in the denylist the `len(non_meta) >= 4`
    # heuristic in _looks_like_row_dict misread the whole envelope as a
    # single (symbol-less) row. Listed here as defense-in-depth alongside the
    # Phase A structural wrapper guard.
    "rows_display", "limit", "offset", "total", "page_size", "page_count",
    "per_page", "next_offset", "has_more", "pagination",
    "generated_at", "generated_at_utc", "generated_at_riyadh", "request_id",
    "duration_ms", "route_owner", "ok",
}

_ENGINE_CACHE: Optional[Any] = None
_ENGINE_CACHE_SOURCE: str = ""
_ENGINE_LOCK = asyncio.Lock()


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


def _env_bool(name: str, default: bool = False) -> bool:
    # v4.20.0 (Fix AE): tri-state env flag reader (absent -> default), matching
    # the data_engine_v2 convention so full-universe gating is consistent across
    # the stack.
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return default


# v4.19.0 [FIX X-2]: defaults bumped to the live-proven values (Render env
# 2026-06-11: ENGINE=20 / PAGE=30 / TOTAL=70). Env always wins; the bump
# only protects deployments where the env vars are absent.
ENGINE_CALL_TIMEOUT_SEC = _env_float("TFB_TOP10_ENGINE_CALL_TIMEOUT_SEC", 20.0)
PAGE_TOTAL_TIMEOUT_SEC = _env_float("TFB_TOP10_PAGE_TIMEOUT_SEC", 30.0)
BUILDER_TOTAL_TIMEOUT_SEC = _env_float("TFB_TOP10_TOTAL_TIMEOUT_SEC", 70.0)
# v4.19.0 [FIX X-1]: graceful-budget knobs. The reserve is the slice of the
# total budget kept back for gating / tiering / ranking / projection after
# collection stops; the outer grace pushes the hard wait_for backstop past
# the soft deadline so the graceful path always wins when it is functioning.
BUILDER_TOTAL_RESERVE_SEC = _env_float("TFB_TOP10_TOTAL_RESERVE_SEC", 6.0)
BUILDER_OUTER_GRACE_SEC = _env_float("TFB_TOP10_OUTER_GRACE_SEC", 8.0)
# Minimum remaining soft budget (sec) worth starting another source page for.
PAGE_LOOP_MIN_REMAINING_SEC = 3.0


def _remaining_budget(deadline: Optional[float]) -> Optional[float]:
    """v4.19.0 [FIX X-1]: seconds left until the soft deadline (monotonic
    clock); None when no deadline is active (direct library callers)."""
    if deadline is None:
        return None
    return max(0.0, deadline - time.monotonic())


def _budget_timeout(base: float, deadline: Optional[float], floor: float = 0.5) -> float:
    """v4.19.0 [FIX X-1/X-2]: clamp a configured timeout to the remaining
    soft budget. Never below `floor`, so a nearly-expired budget still lets
    an in-flight cache hit return instead of raising instantly."""
    rem = _remaining_budget(deadline)
    if rem is None:
        return base
    return max(floor, min(base, rem))
# v4.20.0 (Fix AE -- FULL-UNIVERSE SCAN): the selector's read/select ceilings
# (SOURCE_PAGE_LIMIT max 1000, MAX_LIMIT max 200) bounded the candidate pool far
# below the ~2,189-row live universe -- the root reason Top_10 "scanned 250"
# while four pages held thousands. TFB_TOP10_FULL_UNIVERSE (default OFF) lifts
# BOTH the ceilings AND the defaults for the READ path (SOURCE_PAGE_LIMIT) and
# the RANK/SELECT path (MAX_LIMIT) in one switch, so the full universe is read
# and ranked. When OFF every maximum/default below is identical to v4.19.0
# (byte-identical). Explicit env vars still win within the (now higher) bounds.
#
# HYDRATION_SYMBOL_CAP is DELIBERATELY NOT scaled by full-universe. It bounds a
# per-symbol fallback re-fetch loop (line ~2292: each iteration is a serial
# engine call up to ENGINE_CALL_TIMEOUT_SEC), so a large value would risk a long
# serial stall on the Render free tier whenever the batch quote path misses. The
# full universe is *read* via SOURCE_PAGE_LIMIT and *ranked* via MAX_LIMIT using
# the rows' existing scores; only the small finalist shortlist (~limit*3, see
# _page_priority_symbol_limit) is ever hydrated, so 30/250 stays correct and safe.
_FULL_UNIVERSE = _env_bool("TFB_TOP10_FULL_UNIVERSE", False)
# v4.21.0 (Fix AF -- RESILIENT FETCH): live Render diagnosis showed Top_10
# collapsing to 1 candidate while engine.get_sheet_rows(page, limit=20) returned
# 20 full rows in seconds. Root cause: the selector fetches SOURCE_PAGE_LIMIT (80)
# rows/page wrapped in a per-page wait_for; on a cold cache under Yahoo history
# throttling the 80-row fetch (with ~60 uncached .SR names, each a throttled
# retry) exceeds the page budget -> TimeoutError -> rows=[] -> the whole pool
# collapses. TFB_TOP10_RESILIENT_FETCH lowers the per-page slice to a small
# cache-friendly value (default 30, override TFB_TOP10_RESILIENT_PAGE_LIMIT) so
# each page completes fast AND budget remains to cover all source pages. Default
# OFF -> 80, byte-identical. Ignored under full-universe (operator owns that cost).
_RESILIENT_FETCH = _env_bool("TFB_TOP10_RESILIENT_FETCH", False)
_RESILIENT_PAGE_LIMIT = _env_int("TFB_TOP10_RESILIENT_PAGE_LIMIT", 30, minimum=5, maximum=200)
_SRC_PAGE_MAX = 20000 if _FULL_UNIVERSE else 1000
_SELECT_LIMIT_MAX = 20000 if _FULL_UNIVERSE else 200
if _FULL_UNIVERSE:
    _SRC_PAGE_DFLT = 5000
elif _RESILIENT_FETCH:
    _SRC_PAGE_DFLT = _RESILIENT_PAGE_LIMIT
else:
    _SRC_PAGE_DFLT = 80
_SELECT_LIMIT_DFLT = 3000 if _FULL_UNIVERSE else 50
SOURCE_PAGE_LIMIT = _env_int("TOP10_SELECTOR_SOURCE_PAGE_LIMIT", _SRC_PAGE_DFLT, minimum=10, maximum=_SRC_PAGE_MAX)
HYDRATION_SYMBOL_CAP = _env_int("TOP10_SELECTOR_HYDRATION_SYMBOL_CAP", 30, minimum=5, maximum=250)
MAX_SOURCE_PAGES = _env_int("TOP10_SELECTOR_MAX_SOURCE_PAGES", 5, minimum=1, maximum=20)
MAX_LIMIT = _env_int("TOP10_SELECTOR_MAX_LIMIT", _SELECT_LIMIT_DFLT, minimum=1, maximum=_SELECT_LIMIT_MAX)
EARLY_STOP_MULTIPLIER = _env_int("TOP10_SELECTOR_EARLY_STOP_MULTIPLIER", 6, minimum=2, maximum=20)
EMERGENCY_SYMBOLS = [
    s for s in [x.strip() for x in os.getenv("TOP10_SELECTOR_EMERGENCY_SYMBOLS", "").replace(";", ",").split(",")] if s
]

TOP10_PENALTY_ENGINE_DROP = _env_float("TOP10_PENALTY_ENGINE_DROP", 8.0)
TOP10_PENALTY_FORECAST_UNAVAIL = _env_float("TOP10_PENALTY_FORECAST_UNAVAIL", 10.0)
TOP10_PENALTY_PROVIDER_ERROR = _env_float("TOP10_PENALTY_PROVIDER_ERROR", 6.0)

# =============================================================================
# v4.16.0 Phase A/C/D — vintage protection + admission filter knobs (Fix U)
# =============================================================================
# Row provenance ranks. Higher rank = fresher source. Assigned at each fetch
# site; consumed by `_merge_row_provenance`. Internal-only (`_PROV_KEY` never
# survives final projection because projection copies schema keys only).
_PROV_LIVE = 3              # engine get_sheet_rows (live page fetch)
_PROV_HYDRATED = 3          # engine quote batch (direct-symbol / hydration)
_PROV_EMERGENCY = 2         # emergency-symbol fetch (fresh engine call)
_PROV_SNAPSHOT = 1          # engine cached sheet snapshot (possibly stale)
_PROV_OUTPUT_FALLBACK = 0   # previous Top_10 output page (stale by design)
_PROV_KEY = "_row_provenance"

# Gate-critical fields quarantined from stale donors (rank <= _PROV_SNAPSHOT).
# These are the investability-gate outputs of data_engine_v2
# `_apply_investability_gate`; a stale copy of any of them contradicts the
# freshly computed gate decision (the exact defect verified 2026-06-10:
# pre-Fix-S reliability 46.4/44.8 displayed beside post-Fix-S INVESTABLE).
# v4.16.1: extended to the FULL engine gate write-set. The cross-file audit
# found the v4.16.0 list covered 5 of the 9 columns the engine gate writes,
# leaving the companion verdict fields (conflict_type, final_decision_basis,
# final_action, block_reason) able to arrive stale next to a fresh
# investability_status -- e.g. a stale final_action="INVEST" or an obsolete
# block_reason beside a fresh WATCHLIST. All 9 verdict columns (plus
# scoring_errors) now travel as one vintage.
GATE_CRITICAL_KEYS: Tuple[str, ...] = (
    "forecast_reliability_score",
    "investability_status",
    "data_quality_score",
    "provider_engine_conflict",
    "conflict_type",
    "final_decision_basis",
    "final_action",
    "block_reason",
    "scoring_errors",
)

# Admission filter (Phase D). Default ON; set TFB_TOP10_ADMISSION_FILTER=0
# to restore pre-v4.16.0 behavior. Reasons surfaced in meta + logs.
# =============================================================================
# v4.17.0 [FIX V] — FINAL-ROW GATE APPLICATION + GATE-KEY PROJECTION GUARANTEE
# -----------------------------------------------------------------------------
# WHY: Fix U (v4.16.x) quarantines STALE gate verdicts arriving on snapshot /
# output-fallback donors — correct, but it exposed the other half of the
# original C1 defect: freshly hydrated rows come from the engine's
# enriched-quote path, which does NOT run the investability gate (the gate
# runs on the engine's sheet-row path). Live evidence 2026-06-10: a real
# selector build returned 10 rows with NO gate keys at all
# (forecast_reliability_score / investability_status / final_action absent),
# and the resolved projection contract itself lacked the gate columns. Net
# sheet effect: Top_10 gate columns blank instead of stale-wrong.
#
# FIX V therefore does two things, both fail-soft and env-gated:
#   V-1 `_ensure_gate_output_keys`: after schema resolution, append any
#       missing gate output keys (+ forecast_source) WITH matching canonical
#       headers, so projection cannot drop fresh gate values. Downstream
#       routes re-project onto the registry's canonical 118-column order, so
#       sheet column ORDER is governed there; the selector only guarantees
#       the VALUES survive under canonical key names.
#   V-2 `_apply_final_gate`: run the engine's own `_apply_investability_gate`
#       to recompute verdicts FRESH from the same hydrated data — the exact
#       reliability-parity guarantee Fix S/Fix U exist to provide (e.g. NVDA
#       71.5 on Top_10 == Global_Markets). The gate is idempotent and
#       AA-tag-aware (v5.84.0), so repeated application is safe.
#       [v4.18.0 FIX W-3: application scope widened from the FINAL selected
#       rows to ALL admissible candidates BEFORE tiering, so the tier split
#       and SELL-class exclusion rank on fresh verdicts. Same kill switch.]
# Kill switch: TFB_TOP10_FINAL_GATE=0 disables gate application entirely.
# =============================================================================
TOP10_FINAL_GATE_ENV = "TFB_TOP10_FINAL_GATE"
TOP10_FINAL_GATE_ENABLED = (
    os.getenv(TOP10_FINAL_GATE_ENV, "1").strip().lower() in ("1", "true", "yes", "on")
)
# Gate output columns the projection must carry (canonical key -> sheet header).
GATE_OUTPUT_KEY_HEADERS: "OrderedDict[str, str]" = OrderedDict(
    (
        ("forecast_source", "Forecast Source"),
        ("forecast_reliability_score", "Forecast Reliability Score"),
        ("investability_status", "Investability Status"),
        ("data_quality_score", "Data Quality Score"),
        ("provider_engine_conflict", "Provider Engine Conflict"),
        ("conflict_type", "Conflict Type"),
        ("final_decision_basis", "Final Decision Basis"),
        ("final_action", "Final Action"),
        ("block_reason", "Block Reason"),
        ("scoring_errors", "Scoring Errors"),
    )
)


def _ensure_gate_output_keys(headers: List[str], keys: List[str]) -> Tuple[List[str], List[str], int]:
    """v4.17.0 [FIX V-1]: append missing gate output keys with aligned headers.

    Returns (headers, keys, appended_count). Never raises; never reorders or
    removes existing entries, so v4.16.1 contracts that already include the
    gate columns (e.g. a registry-served 118) pass through unchanged.
    """
    hs, ks = list(headers or []), list(keys or [])
    have = {_s(k).strip().lower() for k in ks}
    appended = 0
    for k, h in GATE_OUTPUT_KEY_HEADERS.items():
        if k not in have:
            ks.append(k)
            hs.append(h)
            appended += 1
    return hs, ks, appended


# =============================================================================
# v4.21.0 — SELECTION STABILITY LAYER (membership hysteresis) — constants.
# OPT-IN: engaged only when the request body carries `stability_state` and/or
# a `stability` config mapping; with neither present, every code path below
# is unreachable and the build is byte-identical to v4.20.0. No ENV vars.
# =============================================================================
STABILITY_OUTPUT_KEY_HEADERS: "OrderedDict[str, str]" = OrderedDict(
    (
        ("stability_status", "Stability Status"),
        ("days_in_list", "Days In List"),
        ("entry_date", "Entry Date"),
        ("score_smoothed", "Score Smoothed"),
        ("stability_trend", "Stability Trend"),
    )
)
STABILITY_STATE_VERSION = 1
STABILITY_DEFAULT_CONFIRM_DAYS = 3   # consecutive qualifying days before ENTRY
STABILITY_DEFAULT_EXIT_DAYS = 3      # consecutive missed days before soft EXIT
STABILITY_DEFAULT_RANK_BUFFER = 15   # all-pool rank <= buffer pauses the exit clock
STABILITY_DEFAULT_SMOOTH_DAYS = 5    # score-history window for smoothing/order
STABILITY_PRUNE_AFTER_DAYS = 14      # drop non-member state entries unseen this long
STABILITY_MAX_STATE_SYMBOLS = 400    # hard state-size cap (members always kept)
STABILITY_TREND_EPS = 2.0            # composite points; |delta| below = "steady"


def _ensure_stability_output_keys(headers: List[str], keys: List[str]) -> Tuple[List[str], List[str], int]:
    """v4.21.0: append the stability output columns (engaged builds only).

    Mirrors _ensure_gate_output_keys byte-for-byte in behavior: additive,
    order-preserving, never raises, no-op for keys already present.
    """
    hs, ks = list(headers or []), list(keys or [])
    have = {_s(k).strip().lower() for k in ks}
    appended = 0
    for k, h in STABILITY_OUTPUT_KEY_HEADERS.items():
        if k not in have:
            ks.append(k)
            hs.append(h)
            appended += 1
    return hs, ks, appended


def _apply_final_gate(rows: Sequence[MutableMapping[str, Any]]) -> Tuple[int, int, bool]:
    """v4.17.0 [FIX V-2] / v4.18.0 [FIX W-3]: run the engine investability
    gate on candidate rows.

    v4.18.0 widened the call site from the FINAL selected rows to ALL
    admissible candidates (pre-tiering), so tier classification and the
    SELL-class exclusion operate on FRESH verdicts. The function itself is
    unchanged: lazy import, per-row try/except — any failure leaves that row
    exactly as it was. Returns (applied_count, error_count, gate_available).
    """
    if not TOP10_FINAL_GATE_ENABLED:
        return 0, 0, False
    gate_fn = None
    try:
        from core import data_engine_v2 as _e  # type: ignore
        gate_fn = getattr(_e, "_apply_investability_gate", None)
    except Exception:
        gate_fn = None
    if not callable(gate_fn):
        return 0, 0, False
    applied = 0
    errors = 0
    for row in rows:
        if not isinstance(row, MutableMapping):
            continue
        try:
            gate_fn(row)
            applied += 1
        except Exception:
            errors += 1
    return applied, errors, True


TOP10_ADMISSION_FILTER_ENV = "TFB_TOP10_ADMISSION_FILTER"
TOP10_ADMISSION_FILTER_ENABLED = (
    os.getenv(TOP10_ADMISSION_FILTER_ENV, "1").strip().lower() in ("1", "true", "yes", "on")
)
_ADMISSION_FORCED_HOLD_TAG = "recommendation_forced_hold_missing_price"

# =============================================================================
# v4.18.0 [FIX W] — GOLDEN COMPOSITE + TIER DISCIPLINE + SECTOR CAP KNOBS
# -----------------------------------------------------------------------------
# WHY each knob exists is documented in the header (Fix W). All default ON /
# active because they ARE the v4.18.0 ranking policy; each one restores the
# prior behavior independently when disabled.
# =============================================================================
# W-1: golden composite ranking. =0 restores the v4.17.0 legacy selector
# score VERBATIM (kept as `_selector_score_legacy`).
TOP10_GOLDEN_COMPOSITE_ENV = "TFB_TOP10_GOLDEN_COMPOSITE"
TOP10_GOLDEN_COMPOSITE_ENABLED = (
    os.getenv(TOP10_GOLDEN_COMPOSITE_ENV, "1").strip().lower() in ("1", "true", "yes", "on")
)
# Golden composite weights over 0-100 components (renormalized over the
# components actually present on a row). Echoed into meta for auditability.
GOLDEN_COMPOSITE_WEIGHTS: Dict[str, float] = {
    "overall_score": 0.40,
    "forecast_reliability_score": 0.25,
    "conviction_score": 0.20,
    "horizon_roi": 0.15,
}
# Horizon-ROI -> 0..100 component mapping: 0% ROI = 50; ±35% ROI = full scale.
GOLDEN_ROI_FULL_SCALE_PCT = 35.0
# Risk penalty: only above this risk_score, linear slope, hard cap.
GOLDEN_RISK_PENALTY_START = 50.0
GOLDEN_RISK_PENALTY_SLOPE = 0.30
GOLDEN_RISK_PENALTY_MAX = 15.0

# W-2b: Tier-1 bar = INVESTABLE + reliability >= floor. (Note: _env_float
# clamps to a 0.1 minimum, so the floor cannot be set to exactly 0; use a
# small value like 1 to make the reliability bar effectively inert.)
TOP10_TIER1_RELIABILITY_FLOOR = _env_float("TFB_TOP10_TIER1_RELIABILITY_FLOOR", 65.0)

# W-2a: SELL-class exclusion. =0 restores pre-v4.18.0 behavior (SELL-class
# rows may be selected / backfilled again).
TOP10_EXCLUDE_SELL_CLASS_ENV = "TFB_TOP10_EXCLUDE_SELL_CLASS"
TOP10_EXCLUDE_SELL_CLASS_ENABLED = (
    os.getenv(TOP10_EXCLUDE_SELL_CLASS_ENV, "1").strip().lower() in ("1", "true", "yes", "on")
)

# W-5: sector diversification cap on the FINAL selection. 0 disables.
TOP10_SECTOR_CAP = _env_int("TFB_TOP10_SECTOR_CAP", 4, minimum=0, maximum=10)

# v4.20.0 (A2): when ON, the W-5 sector cap keys on the CANONICAL (Yahoo->GICS)
# sector so one economic sector occupies one bucket regardless of provider
# vocabulary. DEFAULT OFF (deploy-dark) -> the cap keys on the raw sector exactly
# as in v4.19.0. Requires core.sectors (imported above); if that import failed
# the flag stays an inert no-op even when set. Recommended ON in production
# together with TFB_OPP_SECTOR_NORMALIZE so BOTH decision tabs canonicalize
# consistently.
TOP10_SECTOR_NORMALIZE = _env_bool("TFB_TOP10_SECTOR_NORMALIZE", False)

# W-7: Top10-run metadata stripped from the OUTPUT-PAGE fallback donor at
# ingest — these fields describe a PREVIOUS selector run and are stale by
# definition when re-ingested (stale ranks were the W-4 corruption vector;
# stale selection_reason/criteria_snapshot survive the fill-if-blank path).
TOP10_METADATA_STRIP_KEYS: Tuple[str, ...] = (
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
)

_ENGINE_DROP_TAGS = (
    "intrinsic_unit_mismatch_suspected",
    "upside_synthesis_suspect",
    "engine_52w_high_unit_mismatch_dropped",
    "engine_52w_low_unit_mismatch_dropped",
    "engine_52w_high_low_inverted",
)

_FORECAST_UNAVAIL_TAGS = (
    "forecast_unavailable",
    "forecast_unavailable_no_source",
    "forecast_cleared_consistency_sweep",
    "forecast_skipped_unavailable",
)


# =============================================================================
# v4.14.0 — 8-tier recommendation vocabulary (Phase C)
# =============================================================================
_RECO_8TIER_CANONICAL = frozenset({
    "STRONG_BUY", "BUY", "ACCUMULATE", "HOLD",
    "REDUCE", "SELL", "STRONG_SELL", "AVOID",
})

_BULLISH_RECOS = frozenset({"STRONG_BUY", "BUY", "ACCUMULATE"})
_BEARISH_RECOS = frozenset({"REDUCE", "SELL", "STRONG_SELL", "AVOID"})

# v4.18.0 [FIX W-2a]: the SELL class barred from the Top 10 (picks AND
# backfill) while TFB_TOP10_EXCLUDE_SELL_CLASS is on. Deliberately the exact
# bearish set: a "best investments to BUY now" surface must never carry a
# name the engine says to exit or avoid. HOLD remains backfill-eligible
# (labeled) — a HOLD can legitimately be the best available when the
# bullish pool runs short; a SELL cannot.
SELL_CLASS_RECOS = frozenset(_BEARISH_RECOS)

_RECO_8TIER_NEW_TOKENS = frozenset({"ACCUMULATE", "STRONG_SELL", "AVOID"})

_RECO_TIEBREAK_BUMPS: Dict[str, float] = {
    "STRONG_BUY":   2.0,
    "BUY":          1.5,
    "ACCUMULATE":   1.0,
    "HOLD":         0.0,
    "REDUCE":      -1.0,
    "SELL":        -1.5,
    "STRONG_SELL": -2.5,
    "AVOID":       -8.0,
}

_RECO_TOKEN_ALIASES: Dict[str, str] = {
    "STRONGBUY": "STRONG_BUY",
    "STRONG-BUY": "STRONG_BUY",
    "STRONG BUY": "STRONG_BUY",
    "CONVICTION BUY": "STRONG_BUY",
    "TOP PICK": "STRONG_BUY",
    "OVERWEIGHT": "BUY",
    "OUTPERFORM": "BUY",
    "POSITIVE": "BUY",
    "SCALE IN": "ACCUMULATE",
    "SCALE_IN": "ACCUMULATE",
    "SCALEIN": "ACCUMULATE",
    "ACC": "ACCUMULATE",
    "ADD": "ACCUMULATE",
    "MOD BUY": "ACCUMULATE",
    "MODERATE BUY": "ACCUMULATE",
    "NEUTRAL": "HOLD",
    "MAINTAIN": "HOLD",
    "MARKET PERFORM": "HOLD",
    "EQUAL WEIGHT": "HOLD",
    "PERFORM": "HOLD",
    "UNDERWEIGHT": "REDUCE",
    "UNDERPERFORM": "REDUCE",
    "TRIM": "REDUCE",
    "EXIT": "SELL",
    "NEGATIVE": "SELL",
    "STRONGSELL": "STRONG_SELL",
    "STRONG-SELL": "STRONG_SELL",
    "STRONG SELL": "STRONG_SELL",
    "STRONG REDUCE": "STRONG_SELL",
    "DEEP SELL": "STRONG_SELL",
    "CONVICTION SELL": "STRONG_SELL",
    "HARD PASS": "AVOID",
    "DO NOT BUY": "AVOID",
    "DNB": "AVOID",
    "UNINVESTABLE": "AVOID",
    "UNTRADEABLE": "AVOID",
}


try:
    from core.sheets.schema_registry import get_sheet_spec as _get_sheet_spec  # type: ignore
except Exception:
    _get_sheet_spec = None  # type: ignore

try:
    from core.sheets.page_catalog import normalize_page_name as _normalize_page_name  # type: ignore
except Exception:
    _normalize_page_name = None  # type: ignore


# v4.20.0 (A2): canonical Yahoo->GICS sector map -- the SINGLE source of truth,
# shared with opportunity_builder so the two decision tabs' sector caps cannot
# drift apart. Defensive import: if the module is somehow absent, normalization
# simply cannot engage (the gate below becomes an inert no-op) -- this file holds
# NO duplicate copy of the map.
try:
    from core.sectors import normalize_sector as _canon_sector  # type: ignore
except Exception:  # pragma: no cover
    _canon_sector = None  # type: ignore


try:
    from core.buckets import (  # type: ignore
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


def _s(v: Any) -> str:
    try:
        if v is None:
            return ""
        s = str(v).strip()
        return "" if s.lower() in {"none", "null", "nil"} else s
    except Exception:
        return ""


def _is_blank(v: Any) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())


def _safe_int(v: Any, default: int) -> int:
    try:
        if isinstance(v, bool):
            return default
        return int(float(v))
    except Exception:
        return default


def _safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None or isinstance(v, bool):
            return default
        if isinstance(v, (int, float)):
            f = float(v)
            if math.isnan(f) or math.isinf(f):
                return default
            return f
        s = _s(v).replace(",", "")
        if not s:
            return default
        if s.endswith("%"):
            f = float(s[:-1].strip()) / 100.0
        else:
            f = float(s)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except Exception:
        return default


def _safe_ratio(v: Any, default: Optional[float] = None) -> Optional[float]:
    f = _safe_float(v, default)
    if f is None:
        return default
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _coerce_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False
    if isinstance(v, (int, float)):
        try:
            return bool(int(v))
        except Exception:
            return default
    return default


def _parse_warnings_tags(row: Mapping[str, Any]) -> List[str]:
    raw = row.get("warnings")
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        seq = [_s(x).lower() for x in raw]
        return [s for s in seq if s]
    s = _s(raw).lower()
    if not s:
        return []
    for sep in (";", "|", ","):
        if sep in s:
            return [t.strip() for t in s.split(sep) if t.strip()]
    return [s] if s else []


def _has_engine_drop_tag(tags: Sequence[str]) -> bool:
    if not tags:
        return False
    tag_set = set(tags)
    for marker in _ENGINE_DROP_TAGS:
        if marker in tag_set:
            return True
    joined = " ".join(tags)
    return any(marker in joined for marker in _ENGINE_DROP_TAGS)


def _has_forecast_unavail_tag(tags: Sequence[str], row: Mapping[str, Any]) -> bool:
    if _coerce_bool(row.get("forecast_unavailable"), False):
        return True
    if not tags:
        return False
    tag_set = set(tags)
    for marker in _FORECAST_UNAVAIL_TAGS:
        if marker in tag_set:
            return True
    joined = " ".join(tags)
    return any(marker in joined for marker in _FORECAST_UNAVAIL_TAGS)


def _has_provider_error(row: Mapping[str, Any]) -> bool:
    for key in ("last_error_class", "provider_last_error_class", "provider_error", "error_class"):
        val = row.get(key)
        if val is None:
            continue
        s = _s(val).lower()
        if s and s not in {"none", "null", "ok", "success", "false", "0", "no"}:
            return True
    err = _s(row.get("error"))
    return bool(err) and err.lower() not in {"none", "null", "false", "0"}


# =============================================================================
# v4.14.0 — 8-tier vocabulary helpers (Phase E)
# =============================================================================

def _normalize_reco_token(val: Any) -> str:
    """Resolve a raw recommendation value to a canonical 8-tier token, or ""."""
    s = _s(val).upper()
    if not s:
        return ""
    if s in _RECO_8TIER_CANONICAL:
        return s
    aliased = _RECO_TOKEN_ALIASES.get(s)
    if aliased and aliased in _RECO_8TIER_CANONICAL:
        return aliased
    compact = re.sub(r"[^A-Z]+", "", s)
    if compact:
        aliased = _RECO_TOKEN_ALIASES.get(compact)
        if aliased and aliased in _RECO_8TIER_CANONICAL:
            return aliased
        if compact in _RECO_8TIER_CANONICAL:
            return compact
    return ""


def _normalize_priority_band(val: Any) -> str:
    """Resolve a raw priority_band value to canonical "P1".."P5", or ""."""
    s = _s(val).upper().replace(" ", "").replace("-", "").replace("_", "")
    if not s:
        return ""
    if s.isdigit():
        try:
            n = int(s)
        except Exception:
            return ""
        return f"P{n}" if 1 <= n <= 5 else ""
    if len(s) == 2 and s[0] == "P" and s[1].isdigit():
        try:
            n = int(s[1])
        except Exception:
            return ""
        return f"P{n}" if 1 <= n <= 5 else ""
    if s.startswith("BAND") and len(s) == 5 and s[4].isdigit():
        try:
            n = int(s[4])
        except Exception:
            return ""
        return f"P{n}" if 1 <= n <= 5 else ""
    return ""


def _priority_band_rank(band: Any) -> int:
    """Numeric rank for a priority band. P1=1 (best), P5=5, blank=99."""
    canon = _normalize_priority_band(band)
    if not canon or len(canon) != 2 or canon[0] != "P":
        return 99
    try:
        return int(canon[1])
    except Exception:
        return 99


def _reco_tiebreak_bump(row: Mapping[str, Any]) -> float:
    """Selector-score tiebreak bump per 8-tier recommendation tier."""
    token = _normalize_reco_token(row.get("recommendation"))
    if not token:
        return 0.0
    return _RECO_TIEBREAK_BUMPS.get(token, 0.0)


def _is_avoid_recommendation(row: Mapping[str, Any]) -> bool:
    """True iff the row's recommendation normalizes to the canonical AVOID."""
    return _normalize_reco_token(row.get("recommendation")) == "AVOID"


# =============================================================================
# v4.18.0 [FIX W] — tier-discipline helpers
# =============================================================================

def _row_reliability(row: Mapping[str, Any]) -> Optional[float]:
    """v4.18.0 [FIX W-1 / W-2b]: the reliability the golden composite and the
    Tier-1 bar actually use.

    Prefers the engine gate's `forecast_reliability_score` (fresh on every
    admissible candidate via W-3); falls back to forecast_confidence /
    confidence_score when the gate value is absent (gate disabled or
    degraded run), so ranking never silently zeroes a row for a missing
    gate column. Fraction-scaled (0..1 -> 0..100) and clamped to 0..100.
    """
    rel = _safe_float(row.get("forecast_reliability_score"), None)
    if rel is None:
        rel = _safe_float(row.get("forecast_confidence"), None)
        if rel is None:
            rel = _safe_float(row.get("confidence_score"), None)
    if rel is None:
        return None
    if 0.0 < rel <= 1.0:
        rel *= 100.0
    return max(0.0, min(100.0, rel))


def _is_sell_class(row: Mapping[str, Any]) -> bool:
    """v4.18.0 [FIX W-2a]: True iff the recommendation normalizes into the
    SELL class (REDUCE / SELL / STRONG_SELL / AVOID)."""
    return _normalize_reco_token(row.get("recommendation")) in SELL_CLASS_RECOS


def _row_investability_status(row: Mapping[str, Any]) -> str:
    """Uppercased investability_status, or "" when absent."""
    return _s(row.get("investability_status")).upper()


def _tier_of_row(row: Mapping[str, Any]) -> int:
    """v4.18.0 [FIX W-2b]: 1 = INVESTABLE with reliability >= floor;
    2 = everything else admissible (WATCHLIST / BLOCKED-but-admissible /
    low-reliability INVESTABLE / verdict-less)."""
    if _row_investability_status(row) == "INVESTABLE":
        rel = _row_reliability(row)
        if rel is not None and rel >= TOP10_TIER1_RELIABILITY_FLOOR:
            return 1
    return 2


def _tier2_reason(row: Mapping[str, Any]) -> str:
    """Short human label explaining why a selected row is Tier-2 (used in
    the W-6 backfill label, so the sheet is self-auditing)."""
    status = _row_investability_status(row)
    if status and status != "INVESTABLE":
        return status
    rel = _row_reliability(row)
    if status == "INVESTABLE" and rel is not None and rel < TOP10_TIER1_RELIABILITY_FLOOR:
        return f"reliability {rel:.0f} < floor {TOP10_TIER1_RELIABILITY_FLOOR:.0f}"
    if not status:
        return "no investability verdict"
    return "below Tier-1 bar"


def _is_signature_mismatch_typeerror(exc: TypeError) -> bool:
    msg = _s(exc).lower()
    if not msg:
        return False
    signature_markers = (
        "unexpected keyword argument",
        "positional argument",
        "required positional argument",
        "takes ",
        "got an unexpected keyword",
        "multiple values for argument",
        "missing 1 required positional argument",
        "missing required positional argument",
        "too many positional arguments",
        "not enough positional arguments",
    )
    return any(marker in msg for marker in signature_markers)


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
        try:
            f = float(value)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
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
        if hasattr(value, "__dict__"):
            return _json_safe(dict(value.__dict__))
    except Exception:
        pass
    try:
        return str(value)
    except Exception:
        return None


def _json_compact(value: Any) -> str:
    try:
        return json.dumps(_json_safe(value), ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return str(value)


def _normalize_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        seq = [x.strip() for x in value.replace(";", ",").replace("\n", ",").split(",") if x.strip()]
    elif isinstance(value, (list, tuple, set)):
        seq = list(value)
    else:
        seq = [value]
    out: List[str] = []
    seen = set()
    for item in seq:
        s = _s(item)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _dedupe_keep_order(values: Iterable[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        s = _s(value)
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _normalize_symbol(sym: Any) -> str:
    s = _s(sym).upper().replace(" ", "")
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1]
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    if s.isdigit():
        return f"{s}.SR"
    return s


def _looks_like_symbol_token(x: Any) -> bool:
    s = _s(x).upper()
    if not s or " " in s or len(s) > 24:
        return False
    return bool(re.fullmatch(r"[A-Z0-9\.\=\-\^:_/]{1,24}", s))


def _safe_source_pages(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in values:
        s = _s(item)
        if not s or s in DERIVED_OR_NON_SOURCE_PAGES:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out[:MAX_SOURCE_PAGES]


def _normalize_page_name_safe(name: str) -> str:
    s = _s(name)
    if not s:
        return ""
    if callable(_normalize_page_name):
        try:
            return _normalize_page_name(s, allow_output_pages=False)
        except TypeError:
            try:
                return _normalize_page_name(s)
            except Exception:
                return s
        except Exception:
            return s
    return s


def _header_to_key(header: Any) -> str:
    s = _s(header)
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
    key = key.replace("52w", "week_52")
    key = key.replace("week52", "week_52")
    key = key.replace("p_e_", "pe_")
    key = key.replace("p_b", "pb")
    key = key.replace("p_s", "ps")
    return key


def _compact_key(value: Any) -> str:
    s = _s(value).lower()
    if not s:
        return ""
    s = s.replace("52w", "week52")
    return re.sub(r"[^a-z0-9]+", "", s)


def _canonical_key_variants(name: str) -> List[str]:
    base = _s(name)
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
    lookup: Dict[str, Any] = {}
    for k, v in row.items():
        raw = _s(k)
        if not raw:
            continue
        for token in {raw, raw.lower(), _header_to_key(raw), _compact_key(raw)}:
            if token and token not in lookup:
                lookup[token] = v
    return lookup


def _coerce_mapping(obj: Any) -> Dict[str, Any]:
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
            if isinstance(d, Mapping):
                return dict(d)
    except Exception:
        pass
    try:
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
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


def _count_nonblank_fields(row: Mapping[str, Any]) -> int:
    count = 0
    for value in row.values():
        if isinstance(value, str):
            if value.strip():
                count += 1
        elif value not in (None, [], {}, ()):
            count += 1
    return count


def _rows_to_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    return [[_json_safe(row.get(k)) for k in keys] for row in rows]


def _looks_like_row_dict(d: Any) -> bool:
    if not isinstance(d, Mapping) or not d:
        return False
    row = _coerce_mapping(d)
    if not row:
        return False
    lookup = _row_lookup(row)
    # Strong positive row signals win FIRST — a mapping that names a symbol /
    # ticker / top10_rank / selection_reason is a data row even if it also
    # happens to carry a stray list field.
    if any(token in lookup for token in ("symbol", "ticker", "requested_symbol", "code", "instrument")):
        return True
    if "top10rank" in lookup or "selectionreason" in lookup:
        return True
    # v4.15.0 Phase A — WRAPPER GUARD. A mapping that carries a row-container
    # LIST (rows / row_objects / records / items / results / quotes / data /
    # rows_matrix / matrix / rows_display) is an envelope, NOT a single row.
    # This stops data_engine_v2 v5.83.2's
    # {rows, rows_display, rows_matrix, limit, offset, total} pagination
    # envelope from being misclassified as one symbol-less row — the root
    # cause of the v4.14.0 "5/8 not 10" bug, where `_extract_rows_like`
    # returned `[envelope]` instead of descending into `rows`. The strong
    # positive signals above run first, so a genuine symbol-bearing row is
    # never reclassified by this guard.
    for container in (
        "rows", "row_objects", "records", "items", "results",
        "quotes", "data", "rows_matrix", "matrix", "rows_display",
    ):
        if isinstance(row.get(container), list):
            return False
    matched = 0
    for key in ("name", "current_price", "overall_score", "recommendation", "risk_score", "forecast_confidence"):
        for token in _canonical_key_variants(key):
            if token in lookup:
                matched += 1
                break
    if matched >= 2:
        return True
    non_meta = [k for k in row.keys() if _s(k) not in WRAPPER_KEYS]
    return len(non_meta) >= 4


def _complete_schema_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    raw_headers = list(headers or [])
    raw_keys = list(keys or [])
    max_len = max(len(raw_headers), len(raw_keys))
    out_headers: List[str] = []
    out_keys: List[str] = []
    for i in range(max_len):
        h = _s(raw_headers[i]) if i < len(raw_headers) else ""
        k = _s(raw_keys[i]) if i < len(raw_keys) else ""
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
    try:
        d = getattr(spec, "__dict__", None)
        if isinstance(d, dict):
            cols3 = d.get("columns") or d.get("fields")
            if isinstance(cols3, list) and cols3:
                return cols3
    except Exception:
        pass
    return []


def _schema_keys_headers_from_spec(spec: Any) -> Tuple[List[str], List[str]]:
    if isinstance(spec, dict) and len(spec) == 1 and not any(
        k in spec for k in ("columns", "fields", "headers", "keys", "display_headers")
    ):
        first_val = list(spec.values())[0]
        if isinstance(first_val, dict):
            spec = first_val
    headers: List[str] = []
    keys: List[str] = []
    cols = _schema_columns_from_any(spec)
    for c in cols:
        if isinstance(c, Mapping):
            h = _s(c.get("header") or c.get("display_header") or c.get("displayHeader") or c.get("label") or c.get("title"))
            k = _s(c.get("key") or c.get("field") or c.get("name") or c.get("id"))
        else:
            h = _s(
                getattr(c, "header", getattr(c, "display_header", getattr(c, "displayHeader", getattr(c, "label", getattr(c, "title", None)))))
            )
            k = _s(getattr(c, "key", getattr(c, "field", getattr(c, "name", getattr(c, "id", None)))))
        if h or k:
            headers.append(h or k.replace("_", " ").title())
            keys.append(k or _header_to_key(h))
    if not headers and not keys and isinstance(spec, Mapping):
        headers2 = spec.get("headers") or spec.get("display_headers")
        keys2 = spec.get("keys") or spec.get("columns") or spec.get("fields")
        if isinstance(headers2, list):
            headers = [_s(x) for x in headers2 if _s(x)]
        if isinstance(keys2, list):
            keys = [_s(x) for x in keys2 if _s(x)]
    return _complete_schema_contract(headers, keys)


def _ensure_top10_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    out_headers = list(headers or [])
    out_keys = list(keys or [])
    for field in TOP10_REQUIRED_FIELDS:
        if field not in out_keys:
            out_keys.append(field)
            out_headers.append(TOP10_REQUIRED_HEADERS[field])
    return _complete_schema_contract(out_headers, out_keys)


def _load_schema_defaults() -> Tuple[List[str], List[str]]:
    if callable(_get_sheet_spec):
        try:
            spec = _get_sheet_spec(OUTPUT_PAGE)
            headers, keys = _schema_keys_headers_from_spec(spec)
            if keys:
                headers, keys = _ensure_top10_contract(headers, keys)
                return list(headers), list(keys)
        except Exception:
            pass
    headers, keys = _ensure_top10_contract(DEFAULT_FALLBACK_HEADERS, DEFAULT_FALLBACK_KEYS)
    return list(headers), list(keys)


_ENGINE_METHOD_NAMES = (
    "get_sheet_rows", "get_page_rows", "sheet_rows", "build_sheet_rows", "execute_sheet_rows",
    "run_sheet_rows", "build_analysis_sheet_rows", "run_analysis_sheet_rows", "get_rows_for_sheet",
    "get_rows_for_page", "get_page_data", "get_sheet_data", "build_page_rows", "build_page_data",
    "get_cached_sheet_snapshot", "get_sheet_snapshot", "get_cached_sheet_rows", "get_page_snapshot",
    "get_enriched_quotes_batch", "get_analysis_quotes_batch", "get_quotes_batch", "quotes_batch",
    "get_quotes", "get_quote", "get_quote_dict", "get_enriched_quote",
)

_ENGINE_HOLDER_ATTRS = (
    "engine", "data_engine", "quote_engine", "cache_engine", "_engine", "_data_engine",
    "service", "runner", "advisor_engine",
)

_APP_ATTRS = ("app", "application", "fastapi_app", "api")
_STATE_ATTRS = ("state", "app_state")


def _safe_getattr(obj: Any, name: str, default: Any = None) -> Any:
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


def _looks_like_engine(obj: Any) -> bool:
    if obj is None:
        return False
    if isinstance(obj, (str, bytes, int, float, bool, list, tuple, set)):
        return False
    if isinstance(obj, Mapping):
        return False
    return any(callable(getattr(obj, m, None)) for m in _ENGINE_METHOD_NAMES)


def _iter_mapping_values(mapping: Mapping[str, Any]) -> Iterable[Tuple[str, Any]]:
    try:
        for k, v in mapping.items():
            yield _s(k), v
    except Exception:
        return


def _iter_object_values(obj: Any) -> Iterable[Tuple[str, Any]]:
    if obj is None:
        return
    if isinstance(obj, Mapping):
        yield from _iter_mapping_values(obj)
        return
    names: List[str] = []
    try:
        if hasattr(obj, "__dict__") and isinstance(getattr(obj, "__dict__", None), dict):
            names.extend([n for n in obj.__dict__.keys() if isinstance(n, str)])
    except Exception:
        pass
    preferred = list(_ENGINE_HOLDER_ATTRS) + list(_APP_ATTRS) + list(_STATE_ATTRS)
    names = preferred + [n for n in names if n not in preferred]
    seen = set()
    for name in names:
        if not name or name in seen:
            continue
        seen.add(name)
        try:
            yield name, getattr(obj, name)
        except Exception:
            continue


def _collect_engine_candidates_from_object(
    obj: Any,
    prefix: str = "",
    seen: Optional[set] = None,
    depth: int = 0,
) -> List[Tuple[Any, str]]:
    if seen is None:
        seen = set()
    out: List[Tuple[Any, str]] = []
    if obj is None or depth > 4:
        return out
    obj_id = id(obj)
    if obj_id in seen:
        return out
    seen.add(obj_id)
    if _looks_like_engine(obj):
        out.append((obj, prefix or type(obj).__name__))
    if isinstance(obj, Mapping):
        for name, value in _iter_mapping_values(obj):
            if name in set(_ENGINE_HOLDER_ATTRS + _APP_ATTRS + _STATE_ATTRS) or _looks_like_engine(value):
                out.extend(_collect_engine_candidates_from_object(value, f"{prefix}.{name}" if prefix else name, seen, depth + 1))
        return out
    for attr in _ENGINE_HOLDER_ATTRS + _APP_ATTRS + _STATE_ATTRS:
        val = _safe_getattr(obj, attr, None)
        if val is not None:
            out.extend(_collect_engine_candidates_from_object(val, f"{prefix}.{attr}" if prefix else attr, seen, depth + 1))
    for name, value in _iter_object_values(obj):
        if value is None:
            continue
        if name in set(_ENGINE_HOLDER_ATTRS + _APP_ATTRS + _STATE_ATTRS) or _looks_like_engine(value):
            out.extend(_collect_engine_candidates_from_object(value, f"{prefix}.{name}" if prefix else name, seen, depth + 1))
    return out


async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        result = fn(*args, **kwargs)
        return await result
    result = await asyncio.to_thread(fn, *args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


async def _call_with_timeout(fn: Callable[..., Any], *args: Any, timeout: float, **kwargs: Any) -> Any:
    return await asyncio.wait_for(_call_maybe_async(fn, *args, **kwargs), timeout=timeout)


async def _safe_call_zero_arg(fn: Callable[..., Any]) -> Any:
    # v4.19.0 [FIX X-2]: the ONLY literal clamp deliberately KEPT — this is
    # the engine-RESOLUTION probe (module scanning), not a data fetch, and
    # must stay cheap no matter how generous the data-path timeouts are.
    try:
        return await _call_with_timeout(fn, timeout=min(ENGINE_CALL_TIMEOUT_SEC, 6.0))
    except TypeError:
        return None
    except Exception:
        return None


async def _scan_module_for_engine(module: Any, module_name: str) -> List[Tuple[Any, str]]:
    out: List[Tuple[Any, str]] = []
    if module is None:
        return out
    if _looks_like_engine(module):
        out.append((module, module_name))
    for fn_name in ("get_engine", "resolve_engine", "load_engine", "build_engine", "create_engine"):
        fn = _safe_getattr(module, fn_name, None)
        if callable(fn):
            result = await _safe_call_zero_arg(fn)
            if _looks_like_engine(result):
                out.append((result, f"{module_name}.{fn_name}"))
    for attr in _ENGINE_HOLDER_ATTRS + _APP_ATTRS + _STATE_ATTRS + ("ENGINE", "engine", "_ENGINE"):
        value = _safe_getattr(module, attr, None)
        if value is not None:
            out.extend(_collect_engine_candidates_from_object(value, f"{module_name}.{attr}"))
    return out


async def _resolve_engine_from_modules() -> Tuple[Optional[Any], str]:
    global _ENGINE_CACHE, _ENGINE_CACHE_SOURCE
    if _looks_like_engine(_ENGINE_CACHE):
        return _ENGINE_CACHE, _ENGINE_CACHE_SOURCE or "engine_cache"
    async with _ENGINE_LOCK:
        if _looks_like_engine(_ENGINE_CACHE):
            return _ENGINE_CACHE, _ENGINE_CACHE_SOURCE or "engine_cache"
        candidates: List[Tuple[Any, str]] = []
        module_candidates = (
            "main", "app", "core.data_engine_v2", "core.data_engine",
            "routes.analysis_sheet_rows", "routes.investment_advisor",
            "routes.advanced_analysis", "routes.enriched_quote", "routes.advisor",
        )
        for module_name in module_candidates:
            try:
                mod = importlib.import_module(module_name)
            except Exception:
                mod = None
            if mod is not None:
                candidates.extend(await _scan_module_for_engine(mod, module_name))
        loaded_names = sorted(
            name for name in sys.modules.keys()
            if isinstance(name, str) and (name == "main" or name == "app" or name.startswith("core.") or name.startswith("routes."))
        )
        seen_sources = {src for _, src in candidates}
        for module_name in loaded_names:
            mod = sys.modules.get(module_name)
            if mod is None:
                continue
            scanned = await _scan_module_for_engine(mod, module_name)
            for candidate, source in scanned:
                if source not in seen_sources:
                    seen_sources.add(source)
                    candidates.append((candidate, source))
        if candidates:
            _ENGINE_CACHE, _ENGINE_CACHE_SOURCE = candidates[0]
            return _ENGINE_CACHE, _ENGINE_CACHE_SOURCE
    return None, "engine_unavailable"


async def _resolve_engine(*args: Any, **kwargs: Any) -> Tuple[Optional[Any], str]:
    for key in ("engine", "data_engine", "quote_engine", "cache_engine", "service", "runner", "request", "req", "app", "context"):
        if kwargs.get(key) is not None:
            candidates = _collect_engine_candidates_from_object(kwargs.get(key), key)
            if candidates:
                return candidates[0]
    for i, arg in enumerate(args):
        candidates = _collect_engine_candidates_from_object(arg, f"arg{i}")
        if candidates:
            return candidates[0]
    for key in ("body", "payload", "request_data", "params", "criteria"):
        val = kwargs.get(key)
        if isinstance(val, Mapping):
            candidates = _collect_engine_candidates_from_object(val, key)
            if candidates:
                return candidates[0]
    for i, arg in enumerate(args):
        if isinstance(arg, Mapping):
            candidates = _collect_engine_candidates_from_object(arg, f"arg{i}")
            if candidates:
                return candidates[0]
    return await _resolve_engine_from_modules()


def _payload_keys_like(payload: Any, depth: int = 0) -> List[str]:
    if payload is None or depth > 6:
        return []
    mapping = _coerce_mapping(payload)
    if not mapping:
        return []
    for name in ("keys", "columns", "fields"):
        keys = mapping.get(name)
        if isinstance(keys, list):
            out = [_s(k) for k in keys if _s(k)]
            if out:
                return out
    for name in ("headers", "display_headers", "sheet_headers", "column_headers"):
        headers = mapping.get(name)
        if isinstance(headers, list):
            out = [_header_to_key(h) for h in headers if _header_to_key(h)]
            if out:
                return out
    for name in ("spec", "sheet_spec", "schema", "payload", "result", "response", "output", "data", "quote", "record", "item"):
        nested = mapping.get(name)
        if nested is not None and nested is not payload:
            out = _payload_keys_like(nested, depth + 1)
            if out:
                return out
    return []


def _rows_from_matrix(rows_matrix: Any, cols: Sequence[str]) -> List[Dict[str, Any]]:
    if not isinstance(rows_matrix, list) or not rows_matrix or not cols:
        return []
    keys = [_s(c) for c in cols if _s(c)]
    if not keys:
        return []
    out: List[Dict[str, Any]] = []
    for row in rows_matrix:
        if not isinstance(row, (list, tuple)):
            continue
        vals = list(row)
        out.append({keys[i]: (vals[i] if i < len(vals) else None) for i in range(len(keys))})
    return out


def _extract_rows_like(payload: Any, depth: int = 0) -> List[Dict[str, Any]]:
    if payload is None or depth > 8:
        return []

    if isinstance(payload, list):
        if not payload:
            return []
        if all(isinstance(x, Mapping) or _coerce_mapping(x) for x in payload):
            rows = [_coerce_mapping(x) for x in payload]
            rows = [r for r in rows if r]
            if rows:
                return rows
        if any(isinstance(x, (list, tuple)) for x in payload):
            return []
        return []

    mapping = _coerce_mapping(payload)
    if not mapping:
        return []

    if _looks_like_row_dict(mapping):
        return [mapping]

    rows_from_symbol_map: List[Dict[str, Any]] = []
    maybe_symbol_map = True
    symbol_like_keys = 0
    for k, v in mapping.items():
        if not isinstance(v, Mapping):
            maybe_symbol_map = False
            break
        if not _looks_like_symbol_token(k):
            maybe_symbol_map = False
            break
        symbol_like_keys += 1
        nested_rows = _extract_rows_like(v, depth + 1)
        row = nested_rows[0] if len(nested_rows) == 1 else _coerce_mapping(v)
        if not row or not _looks_like_row_dict(row):
            maybe_symbol_map = False
            break
        if _is_blank(row.get("symbol")) and _is_blank(row.get("ticker")):
            row["symbol"] = _normalize_symbol(k)
            row["ticker"] = _normalize_symbol(k)
        rows_from_symbol_map.append(row)
    if maybe_symbol_map and symbol_like_keys > 0 and rows_from_symbol_map:
        return rows_from_symbol_map

    for key in ("row_objects", "rows", "records", "record", "items", "item", "results", "recommendations", "quotes", "quote", "data"):
        value = mapping.get(key)
        if isinstance(value, list):
            rows = _extract_rows_like(value, depth + 1)
            if rows:
                return rows
            if value and any(isinstance(x, (list, tuple)) for x in value):
                keys_like = _payload_keys_like(mapping)
                rows = _rows_from_matrix(value, keys_like)
                if rows:
                    return rows
        elif isinstance(value, Mapping):
            rows = _extract_rows_like(value, depth + 1)
            if rows:
                return rows

    for key in ("rows_matrix", "matrix"):
        value = mapping.get(key)
        if isinstance(value, list):
            keys_like = _payload_keys_like(mapping)
            rows = _rows_from_matrix(value, keys_like)
            if rows:
                return rows

    for key in ("payload", "result", "response", "output", "snapshot", "content", "envelope", "spec", "schema", "sheet_spec"):
        value = mapping.get(key)
        if value is not None and value is not payload:
            rows = _extract_rows_like(value, depth + 1)
            if rows:
                return rows

    return []


async def _call_engine_method(
    engine: Any,
    method_names: Sequence[str],
    attempts: Sequence[Tuple[Tuple[Any, ...], Dict[str, Any]]],
    *,
    timeout_seconds: float = ENGINE_CALL_TIMEOUT_SEC,
) -> Any:
    if engine is None:
        return None
    last_exc: Optional[Exception] = None
    for method_name in method_names:
        fn = getattr(engine, method_name, None)
        if not callable(fn):
            continue
        for args, kwargs in attempts:
            try:
                return await _call_with_timeout(fn, *args, timeout=timeout_seconds, **kwargs)
            except asyncio.TimeoutError as exc:
                last_exc = exc
                logger.debug("Engine method timed out: %s", method_name)
                continue
            except TypeError as exc:
                last_exc = exc
                if _is_signature_mismatch_typeerror(exc):
                    continue
                logger.debug("Engine method raised non-signature TypeError: %s", method_name, exc_info=True)
                raise
            except Exception as exc:
                last_exc = exc
                continue
    if last_exc is not None:
        logger.debug("Engine call attempts exhausted: %s", last_exc)
    return None


async def _fetch_page_rows(engine: Any, page: str, limit: int, mode: str) -> List[Dict[str, Any]]:
    body = {
        "page": page, "page_name": page, "sheet": page, "sheet_name": page,
        "tab": page, "worksheet": page, "name": page, "limit": limit, "top_n": limit,
        "mode": mode or "", "include_headers": True, "include_matrix": True,
        "schema_only": False, "headers_only": False,
    }
    attempts = [
        ((), {"page": page, "sheet": page, "sheet_name": page, "limit": limit, "mode": mode or "", "body": body}),
        ((), {"page_name": page, "sheet_name": page, "limit": limit, "mode": mode or "", "body": body}),
        ((), {"payload": body}),
        ((), {"body": body}),
        ((), {"page": page, "sheet": page, "sheet_name": page, "limit": limit, "mode": mode or ""}),
        ((), {"page_name": page, "sheet_name": page, "limit": limit, "mode": mode or ""}),
        ((), {"page": page, "sheet": page, "limit": limit, "mode": mode or ""}),
        ((), {"page": page, "limit": limit, "mode": mode or ""}),
        ((page,), {"limit": limit, "mode": mode or ""}),
        ((page,), {"limit": limit}),
        ((page,), {}),
    ]
    payload = await _call_engine_method(
        engine,
        (
            "get_sheet_rows", "get_page_rows", "sheet_rows", "build_sheet_rows",
            "execute_sheet_rows", "run_sheet_rows", "build_analysis_sheet_rows",
            "run_analysis_sheet_rows", "get_rows_for_sheet", "get_rows_for_page",
            "get_page_data", "get_sheet_data", "build_page_rows", "build_page_data",
        ),
        attempts,
    )
    return _extract_rows_like(payload)


async def _fetch_page_snapshot_rows(engine: Any, page: str) -> List[Dict[str, Any]]:
    attempts = [
        ((), {"sheet_name": page}),
        ((), {"sheet": page}),
        ((), {"page": page}),
        ((), {"page_name": page}),
        ((page,), {}),
    ]
    payload = await _call_engine_method(
        engine,
        ("get_cached_sheet_snapshot", "get_sheet_snapshot", "get_cached_sheet_rows", "get_page_snapshot", "get_sheet_cache"),
        attempts,
        timeout_seconds=ENGINE_CALL_TIMEOUT_SEC,  # v4.19.0 [FIX X-2] de-clamped (was min(...,6))
    )
    return _extract_rows_like(payload)


async def _fetch_direct_symbol_rows(engine: Any, symbols: Sequence[str], mode: str) -> List[Dict[str, Any]]:
    syms = [_normalize_symbol(s) for s in symbols if _normalize_symbol(s)]
    if not syms:
        return []
    attempts = [
        ((), {"symbols": syms, "mode": mode or "", "schema": OUTPUT_PAGE}),
        ((), {"symbols": syms, "mode": mode or ""}),
        ((), {"symbols": syms}),
        ((syms,), {"mode": mode or "", "schema": OUTPUT_PAGE}),
        ((syms,), {"mode": mode or ""}),
        ((syms,), {}),
        ((), {"tickers": syms, "mode": mode or ""}),
        ((), {"tickers": syms}),
    ]
    payload = await _call_engine_method(
        engine,
        (
            "get_enriched_quotes_batch", "get_analysis_quotes_batch", "get_quotes_batch", "quotes_batch",
            "get_quotes", "get_enriched_quote_batch", "get_symbol_quotes", "get_live_quotes",
        ),
        attempts,
        timeout_seconds=ENGINE_CALL_TIMEOUT_SEC,  # v4.19.0 [FIX X-2] de-clamped (was min(...,7))
    )
    rows = _extract_rows_like(payload)
    if rows:
        return rows

    if isinstance(payload, Mapping):
        direct_rows: List[Dict[str, Any]] = []
        for sym in syms:
            candidate = payload.get(sym) or payload.get(_normalize_symbol(sym))
            if isinstance(candidate, Mapping):
                direct_rows.append(_coerce_mapping(candidate))
        if direct_rows:
            return direct_rows

    out: List[Dict[str, Any]] = []
    for sym in syms[:HYDRATION_SYMBOL_CAP]:
        single_attempts = [
            ((sym,), {"mode": mode or "", "schema": OUTPUT_PAGE}),
            ((sym,), {"mode": mode or ""}),
            ((sym,), {}),
            ((), {"symbol": sym, "mode": mode or ""}),
            ((), {"symbol": sym}),
            ((), {"ticker": sym, "mode": mode or ""}),
        ]
        row_payload = await _call_engine_method(
            engine,
            ("get_enriched_quote", "get_quote", "get_quote_dict", "get_live_quote", "get_symbol_quote"),
            single_attempts,
            timeout_seconds=ENGINE_CALL_TIMEOUT_SEC,  # v4.19.0 [FIX X-2] de-clamped (was min(...,5))
        )
        single_rows = _extract_rows_like(row_payload)
        if single_rows:
            out.extend(single_rows)
        elif isinstance(row_payload, Mapping):
            d = _coerce_mapping(row_payload)
            if d:
                out.append(d)
        else:
            d = _coerce_mapping(row_payload)
            if d:
                out.append(d)
    return out


def _merge_mapping_like(*parts: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for part in parts:
        if isinstance(part, Mapping):
            out.update(dict(part))
    return out


def _collect_symbol_keys_from_mapping(mapping: Mapping[str, Any]) -> List[str]:
    out: List[str] = []
    for k, v in mapping.items():
        if isinstance(v, Mapping) and _looks_like_symbol_token(k):
            out.append(_normalize_symbol(k))
    return _dedupe_keep_order(out)


def _collect_criteria_from_inputs(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    positional_maps = [arg for arg in args if isinstance(arg, Mapping)]
    body = _merge_mapping_like(
        *positional_maps,
        kwargs.get("body"),
        kwargs.get("payload"),
        kwargs.get("request_data"),
        kwargs.get("params"),
    )

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

    for k in (
        "pages_selected", "pages", "selected_pages", "sources", "page", "page_name", "sheet", "sheet_name",
        "symbols", "tickers", "direct_symbols", "top_n", "limit", "risk_level", "risk_profile",
        "confidence_bucket", "confidence_level", "invest_period_days", "investment_period_days",
        "horizon_days", "invest_period_label", "min_expected_roi", "min_roi", "min_confidence",
        "min_ai_confidence", "max_risk_score", "min_volume", "enrich_final", "schema_only",
        "headers_only", "include_headers", "include_matrix", "mode", "emergency_symbols",
        "min_conviction_score", "min_conviction",
        "exclude_engine_dropped_valuation",
        "exclude_forecast_unavailable",
        "exclude_provider_errors",
        "exclude_avoid_recommendations",
        "min_priority_band",
        "reco_8tier_strict",
    ):
        if kwargs.get(k) is not None:
            criteria[k] = kwargs.get(k)

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
    pages = _safe_source_pages([_normalize_page_name_safe(p) for p in pages])
    if not pages:
        pages = _safe_source_pages(DEFAULT_SOURCE_PAGES)

    direct_symbols = _normalize_list(criteria.get("direct_symbols") or criteria.get("symbols") or criteria.get("tickers"))
    if not direct_symbols and isinstance(body, Mapping):
        direct_symbols = _collect_symbol_keys_from_mapping(body)
    direct_symbols = [_normalize_symbol(s) for s in direct_symbols if _normalize_symbol(s)]

    emergency_symbols = _normalize_list(criteria.get("emergency_symbols")) or list(EMERGENCY_SYMBOLS)
    emergency_symbols = [_normalize_symbol(s) for s in emergency_symbols if _normalize_symbol(s)]

    limit = _safe_int(criteria.get("limit") or criteria.get("top_n") or kwargs.get("limit"), 10)
    limit = max(1, min(limit, MAX_LIMIT))

    horizon_days = _safe_int(
        criteria.get("horizon_days") or criteria.get("invest_period_days") or criteria.get("investment_period_days"),
        90,
    )

    risk_level = _s(criteria.get("risk_level") or criteria.get("risk_profile") or "").lower()
    confidence_bucket = _s(criteria.get("confidence_bucket") or criteria.get("confidence_level") or "").lower()

    min_roi = criteria.get("min_expected_roi")
    if min_roi is None:
        min_roi = criteria.get("min_roi")
    min_roi_ratio = _safe_ratio(min_roi, None)

    normalized = dict(criteria)
    normalized["pages_selected"] = pages
    normalized["direct_symbols"] = direct_symbols
    normalized["direct_symbol_order"] = list(direct_symbols)
    normalized["emergency_symbols"] = emergency_symbols
    normalized["limit"] = limit
    normalized["top_n"] = limit
    normalized["risk_level"] = risk_level
    normalized["risk_profile"] = risk_level
    normalized["confidence_bucket"] = confidence_bucket
    normalized["confidence_level"] = confidence_bucket
    normalized["horizon_days"] = horizon_days
    normalized["invest_period_days"] = horizon_days
    normalized["min_expected_roi"] = min_roi_ratio
    normalized["min_roi"] = min_roi_ratio
    normalized["schema_only"] = _coerce_bool(normalized.get("schema_only"), False)
    normalized["headers_only"] = _coerce_bool(normalized.get("headers_only"), False)
    normalized["include_headers"] = _coerce_bool(normalized.get("include_headers", True), True)
    normalized["include_matrix"] = _coerce_bool(normalized.get("include_matrix", True), True)
    normalized.setdefault("enrich_final", True)

    mcs_raw = normalized.get("min_conviction_score")
    if mcs_raw is None:
        mcs_raw = normalized.get("min_conviction")
    mcs_val = _safe_float(mcs_raw, None)
    if mcs_val is not None:
        if 0.0 < mcs_val <= 1.0:
            mcs_val = mcs_val * 100.0
        mcs_val = max(0.0, min(100.0, mcs_val))
    normalized["min_conviction_score"] = mcs_val if mcs_val is not None else 0.0
    normalized["min_conviction"] = normalized["min_conviction_score"]

    normalized["exclude_engine_dropped_valuation"] = _coerce_bool(
        normalized.get("exclude_engine_dropped_valuation"), False
    )
    normalized["exclude_forecast_unavailable"] = _coerce_bool(
        normalized.get("exclude_forecast_unavailable"), False
    )
    normalized["exclude_provider_errors"] = _coerce_bool(
        normalized.get("exclude_provider_errors"), False
    )

    normalized["exclude_avoid_recommendations"] = _coerce_bool(
        normalized.get("exclude_avoid_recommendations"), False
    )
    mpb_canon = _normalize_priority_band(normalized.get("min_priority_band"))
    normalized["min_priority_band"] = mpb_canon
    normalized["reco_8tier_strict"] = _coerce_bool(
        normalized.get("reco_8tier_strict"), False
    )

    return normalized


def _extract_value_by_aliases(row: Mapping[str, Any], key: str) -> Any:
    aliases = ROW_KEY_ALIASES.get(key, (key,))
    lookup = _row_lookup(row)
    for alias in aliases:
        for token in _canonical_key_variants(alias):
            if token in lookup:
                return lookup[token]
    return None


def _normalize_candidate_row(raw: Mapping[str, Any]) -> Dict[str, Any]:
    row: Dict[str, Any] = dict(raw)
    for key in ROW_KEY_ALIASES.keys():
        if key not in row or row.get(key) is None or row.get(key) == "":
            value = _extract_value_by_aliases(row, key)
            if value is not None:
                row[key] = value
    sym = _normalize_symbol(row.get("symbol") or row.get("ticker") or row.get("requested_symbol") or row.get("code"))
    if sym:
        row["symbol"] = sym
        row.setdefault("ticker", sym)
    return row


def _row_richness(row: Mapping[str, Any]) -> int:
    return _count_nonblank_fields(row)


def _choose_horizon_roi(row: Mapping[str, Any], horizon_days: int) -> Optional[float]:
    if horizon_days <= 31:
        return _safe_ratio(row.get("expected_roi_1m"), None) or _safe_ratio(row.get("expected_roi_3m"), None) or _safe_ratio(row.get("expected_roi_12m"), None)
    if horizon_days <= 92:
        return _safe_ratio(row.get("expected_roi_3m"), None) or _safe_ratio(row.get("expected_roi_12m"), None) or _safe_ratio(row.get("expected_roi_1m"), None)
    return _safe_ratio(row.get("expected_roi_12m"), None) or _safe_ratio(row.get("expected_roi_3m"), None) or _safe_ratio(row.get("expected_roi_1m"), None)


def _resolve_risk_bucket_canon(row: Mapping[str, Any]) -> str:
    if not (
        _BUCKETS_AVAILABLE
        and _bk_normalize_risk_bucket is not None
        and _bk_risk_bucket_from_score is not None
    ):
        return ""
    label = _s(row.get("risk_bucket") or row.get("risk_level"))
    if label:
        try:
            canon = _bk_normalize_risk_bucket(label)
        except Exception:
            canon = ""
        if canon:
            return canon
    risk = _safe_float(row.get("risk_score"), None)
    if risk is not None:
        try:
            return _bk_risk_bucket_from_score(risk) or ""
        except Exception:
            return ""
    return ""


def _resolve_confidence_bucket_canon(row: Mapping[str, Any]) -> str:
    if not (
        _BUCKETS_AVAILABLE
        and _bk_normalize_confidence_bucket is not None
        and _bk_confidence_bucket_from_score is not None
    ):
        return ""
    label = _s(row.get("confidence_bucket") or row.get("confidence_level"))
    if label:
        try:
            canon = _bk_normalize_confidence_bucket(label)
        except Exception:
            canon = ""
        if canon:
            return canon
    score = _safe_float(row.get("forecast_confidence") or row.get("confidence_score"), None)
    if score is not None:
        try:
            return _bk_confidence_bucket_from_score(score) or ""
        except Exception:
            return ""
    return ""


def _confidence_bucket_match(row: Mapping[str, Any], wanted: str) -> bool:
    if not wanted:
        return True

    wanted_canon = ""
    if _BUCKETS_AVAILABLE and _bk_normalize_confidence_bucket is not None:
        try:
            wanted_canon = _bk_normalize_confidence_bucket(wanted)
        except Exception:
            wanted_canon = ""
    if wanted_canon:
        row_canon = _resolve_confidence_bucket_canon(row)
        if row_canon:
            return row_canon == wanted_canon
        if (
            _safe_float(row.get("forecast_confidence") or row.get("confidence_score"), None) is None
            and not _s(row.get("confidence_bucket") or row.get("confidence_level"))
        ):
            return True

    row_bucket = _s(row.get("confidence_bucket") or row.get("confidence_level")).lower()
    if row_bucket:
        return wanted in row_bucket or row_bucket in wanted
    score = _safe_float(row.get("forecast_confidence") or row.get("confidence_score"), None)
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


def _risk_level_match(row: Mapping[str, Any], wanted: str) -> bool:
    if not wanted:
        return True

    wanted_canon = ""
    if _BUCKETS_AVAILABLE and _bk_normalize_risk_bucket is not None:
        try:
            wanted_canon = _bk_normalize_risk_bucket(wanted)
        except Exception:
            wanted_canon = ""
    if wanted_canon:
        row_canon = _resolve_risk_bucket_canon(row)
        if row_canon:
            return row_canon == wanted_canon
        if (
            _safe_float(row.get("risk_score"), None) is None
            and not _s(row.get("risk_bucket") or row.get("risk_level"))
        ):
            return True

    row_bucket = _s(row.get("risk_bucket") or row.get("risk_level")).lower()
    if row_bucket:
        return wanted in row_bucket or row_bucket in wanted
    risk = _safe_float(row.get("risk_score"), None)
    if risk is None:
        return True
    if "low" in wanted or "conservative" in wanted:
        return risk <= 35
    if "moderate" in wanted or "medium" in wanted:
        return 20 <= risk <= 65
    if "high" in wanted or "aggressive" in wanted:
        return risk >= 45
    return True


def _passes_filters_with_reason(
    row: Mapping[str, Any],
    criteria: Mapping[str, Any],
) -> Tuple[bool, str]:
    wanted_conf = _s(criteria.get("confidence_bucket") or criteria.get("confidence_level")).lower()
    wanted_risk = _s(criteria.get("risk_level") or criteria.get("risk_profile")).lower()
    horizon_days = _safe_int(criteria.get("horizon_days") or criteria.get("invest_period_days"), 90)

    if not _confidence_bucket_match(row, wanted_conf):
        return False, "confidence_bucket"
    if not _risk_level_match(row, wanted_risk):
        return False, "risk_level"

    min_roi = _safe_ratio(criteria.get("min_expected_roi") or criteria.get("min_roi"), None)
    roi = _choose_horizon_roi(row, horizon_days)
    if min_roi is not None:
        if roi is None or roi < min_roi:
            return False, "min_expected_roi"

    min_conf = _safe_float(criteria.get("min_confidence") or criteria.get("min_ai_confidence"), None)
    row_conf = _safe_float(row.get("forecast_confidence") or row.get("confidence_score"), None)
    if row_conf is not None and row_conf <= 1.0:
        row_conf *= 100.0
    if min_conf is not None:
        if min_conf <= 1.0:
            min_conf *= 100.0
        if row_conf is None or row_conf < min_conf:
            return False, "min_ai_confidence"

    max_risk = _safe_float(criteria.get("max_risk_score"), None)
    risk = _safe_float(row.get("risk_score"), None)
    if max_risk is not None and risk is not None and risk > max_risk:
        return False, "max_risk_score"

    min_volume = _safe_float(criteria.get("min_volume"), None)
    volume = _safe_float(row.get("volume"), None)
    if min_volume is not None and volume is not None and volume < min_volume:
        return False, "min_volume"

    min_conv = _safe_float(
        criteria.get("min_conviction_score") or criteria.get("min_conviction"),
        None,
    )
    if min_conv is not None and min_conv > 0.0:
        if 0.0 < min_conv <= 1.0:
            min_conv = min_conv * 100.0
        row_conv = _safe_float(row.get("conviction_score"), None)
        if row_conv is None or row_conv < min_conv:
            return False, "min_conviction_score"

    if _coerce_bool(criteria.get("exclude_engine_dropped_valuation"), False):
        tags = _parse_warnings_tags(row)
        if _has_engine_drop_tag(tags):
            return False, "exclude_engine_dropped_valuation"

    if _coerce_bool(criteria.get("exclude_forecast_unavailable"), False):
        tags = _parse_warnings_tags(row)
        if _has_forecast_unavail_tag(tags, row):
            return False, "exclude_forecast_unavailable"

    if _coerce_bool(criteria.get("exclude_provider_errors"), False):
        if _has_provider_error(row):
            return False, "exclude_provider_errors"

    # -------------------------------------------------------------------------
    # v4.14.0 Phase G — criteria_model v3.2.0 forward-compat HARD FILTERS
    # -------------------------------------------------------------------------
    if _coerce_bool(criteria.get("exclude_avoid_recommendations"), False):
        if _is_avoid_recommendation(row):
            return False, "exclude_avoid_recommendations"

    mpb_floor_canon = _normalize_priority_band(criteria.get("min_priority_band"))
    if mpb_floor_canon:
        floor_rank = _priority_band_rank(mpb_floor_canon)
        row_band_canon = _normalize_priority_band(row.get("recommendation_priority_band"))
        if row_band_canon:
            if _priority_band_rank(row_band_canon) > floor_rank:
                return False, "min_priority_band"
        elif _coerce_bool(criteria.get("reco_8tier_strict"), False):
            return False, "min_priority_band"

    if _coerce_bool(criteria.get("reco_8tier_strict"), False):
        raw_reco = _s(row.get("recommendation"))
        if raw_reco and not _normalize_reco_token(raw_reco):
            return False, "reco_8tier_strict"

    return True, ""


def _passes_filters(row: Mapping[str, Any], criteria: Mapping[str, Any]) -> bool:
    passed, _reason = _passes_filters_with_reason(row, criteria)
    return passed


def _direct_symbol_order_index(row: Mapping[str, Any], criteria: Mapping[str, Any]) -> Optional[int]:
    direct_symbols = [_normalize_symbol(s) for s in _normalize_list(criteria.get("direct_symbol_order") or criteria.get("direct_symbols"))]
    if not direct_symbols:
        return None
    sym = _normalize_symbol(row.get("symbol") or row.get("ticker") or row.get("requested_symbol"))
    if not sym:
        return None
    try:
        return direct_symbols.index(sym)
    except ValueError:
        return None


def _selector_score_legacy(row: Mapping[str, Any], criteria: Mapping[str, Any]) -> float:
    """The v4.17.0 selector score, preserved VERBATIM (only renamed).

    Active when TFB_TOP10_GOLDEN_COMPOSITE=0 — the W-1 kill switch restores
    this exact ranking arithmetic.
    """
    overall = _safe_float(row.get("overall_score"), None)
    opportunity = _safe_float(row.get("opportunity_score"), None)
    value = _safe_float(row.get("value_score"), None)
    quality = _safe_float(row.get("quality_score"), None)
    momentum = _safe_float(row.get("momentum_score"), None)
    growth = _safe_float(row.get("growth_score"), None)
    risk = _safe_float(row.get("risk_score"), None)
    conf = _safe_float(row.get("forecast_confidence") or row.get("confidence_score"), None)
    liquidity = _safe_float(row.get("liquidity_score"), None)
    horizon_days = _safe_int(criteria.get("horizon_days") or criteria.get("invest_period_days"), 90)
    roi = _choose_horizon_roi(row, horizon_days)

    if conf is not None and conf <= 1.0:
        conf *= 100.0

    score = 0.0
    if overall is not None:
        score += overall * 0.35
    if opportunity is not None:
        score += opportunity * 0.20
    if value is not None:
        score += value * 0.08
    if quality is not None:
        score += quality * 0.08
    if momentum is not None:
        score += momentum * 0.08
    if growth is not None:
        score += growth * 0.08
    if conf is not None:
        score += conf * 0.08
    if liquidity is not None:
        score += liquidity * 0.05
    if risk is not None:
        score += (100.0 - risk) * 0.08
    if roi is not None:
        score += roi * 100.0 * 0.20

    conviction = _safe_float(row.get("conviction_score"), None)
    if conviction is not None:
        score += conviction * 0.05

    candle_signal = _s(row.get("candlestick_signal")).upper()
    candle_conf = _safe_float(row.get("candlestick_confidence"), None)
    if candle_signal == "BULLISH" and candle_conf is not None:
        score += min(candle_conf, 100.0) * 0.02
    elif candle_signal == "BEARISH" and candle_conf is not None:
        score -= min(candle_conf, 100.0) * 0.02

    # v4.14.0 Phase H — 8-tier recommendation tiebreak bump.
    score += _reco_tiebreak_bump(row)

    tags = _parse_warnings_tags(row)
    if _has_engine_drop_tag(tags):
        score -= TOP10_PENALTY_ENGINE_DROP
    if _has_forecast_unavail_tag(tags, row):
        score -= TOP10_PENALTY_FORECAST_UNAVAIL
    if _has_provider_error(row):
        score -= TOP10_PENALTY_PROVIDER_ERROR

    direct_order_index = _direct_symbol_order_index(row, criteria)
    if direct_order_index is not None:
        score += max(0.0, 140.0 - float(direct_order_index * 2))

    score += min(_row_richness(row), 120) * 0.03
    return float(score)


def _selector_score_golden(row: Mapping[str, Any], criteria: Mapping[str, Any]) -> float:
    """v4.18.0 [FIX W-1] — the golden composite base score.

    0-100 weighted blend (weights renormalized over the components PRESENT
    on the row, so a missing conviction does not structurally sink a row
    against its peers):

        0.40 * overall_score
      + 0.25 * reliability   (_row_reliability: gate value, confidence
                              fallback)
      + 0.20 * conviction_score
      + 0.15 * horizon-ROI component (0..100; 0% ROI = 50,
                              ±GOLDEN_ROI_FULL_SCALE_PCT% = full scale)
      -  risk penalty: (risk_score - 50) * 0.30 capped at 15, only when
         risk_score > 50.

    The legacy score's orthogonal nudges are retained with identical
    magnitudes (candlestick, 8-tier reco bump, warnings penalties,
    direct-symbol priority, richness) — EXCEPT the legacy
    +conviction*0.05 nudge, omitted because conviction is now a
    first-class 20% component.
    """
    horizon_days = _safe_int(criteria.get("horizon_days") or criteria.get("invest_period_days"), 90)
    overall = _safe_float(row.get("overall_score"), None)
    reliability = _row_reliability(row)
    conviction = _safe_float(row.get("conviction_score"), None)
    if conviction is not None:
        if 0.0 < conviction <= 1.0:
            conviction *= 100.0
        conviction = max(0.0, min(100.0, conviction))
    roi = _choose_horizon_roi(row, horizon_days)
    roi_component: Optional[float] = None
    if roi is not None:
        roi_component = max(
            0.0,
            min(100.0, 50.0 + (roi * 100.0) * (50.0 / GOLDEN_ROI_FULL_SCALE_PCT)),
        )

    components = (
        (overall, GOLDEN_COMPOSITE_WEIGHTS["overall_score"]),
        (reliability, GOLDEN_COMPOSITE_WEIGHTS["forecast_reliability_score"]),
        (conviction, GOLDEN_COMPOSITE_WEIGHTS["conviction_score"]),
        (roi_component, GOLDEN_COMPOSITE_WEIGHTS["horizon_roi"]),
    )
    weight_sum = sum(w for v, w in components if v is not None)
    if weight_sum > 0.0:
        score = sum(v * w for v, w in components if v is not None) / weight_sum
    else:
        score = 0.0

    risk = _safe_float(row.get("risk_score"), None)
    if risk is not None and risk > GOLDEN_RISK_PENALTY_START:
        score -= min(
            GOLDEN_RISK_PENALTY_MAX,
            (risk - GOLDEN_RISK_PENALTY_START) * GOLDEN_RISK_PENALTY_SLOPE,
        )

    candle_signal = _s(row.get("candlestick_signal")).upper()
    candle_conf = _safe_float(row.get("candlestick_confidence"), None)
    if candle_signal == "BULLISH" and candle_conf is not None:
        score += min(candle_conf, 100.0) * 0.02
    elif candle_signal == "BEARISH" and candle_conf is not None:
        score -= min(candle_conf, 100.0) * 0.02

    score += _reco_tiebreak_bump(row)

    tags = _parse_warnings_tags(row)
    if _has_engine_drop_tag(tags):
        score -= TOP10_PENALTY_ENGINE_DROP
    if _has_forecast_unavail_tag(tags, row):
        score -= TOP10_PENALTY_FORECAST_UNAVAIL
    if _has_provider_error(row):
        score -= TOP10_PENALTY_PROVIDER_ERROR

    direct_order_index = _direct_symbol_order_index(row, criteria)
    if direct_order_index is not None:
        score += max(0.0, 140.0 - float(direct_order_index * 2))

    score += min(_row_richness(row), 120) * 0.03
    return float(score)


def _selector_score(row: Mapping[str, Any], criteria: Mapping[str, Any]) -> float:
    """Dispatcher (v4.18.0 W-1): golden composite by default; the verbatim
    v4.17.0 legacy score when TFB_TOP10_GOLDEN_COMPOSITE=0."""
    if TOP10_GOLDEN_COMPOSITE_ENABLED:
        return _selector_score_golden(row, criteria)
    return _selector_score_legacy(row, criteria)


def _canonical_selection_reason(row: Dict[str, Any], criteria: Mapping[str, Any]) -> str:
    recommendation = _s(row.get("recommendation"))
    confidence_bucket = _s(row.get("confidence_bucket"))
    risk_bucket = _s(row.get("risk_bucket"))
    source_page = _s(row.get("source_page"))

    if _BUCKETS_AVAILABLE:
        if risk_bucket and _bk_normalize_risk_bucket is not None:
            try:
                risk_bucket = _bk_normalize_risk_bucket(risk_bucket) or risk_bucket
            except Exception:
                pass
        if confidence_bucket and _bk_normalize_confidence_bucket is not None:
            try:
                confidence_bucket = _bk_normalize_confidence_bucket(confidence_bucket) or confidence_bucket
            except Exception:
                pass

    horizon_days = _safe_int(criteria.get("horizon_days") or criteria.get("invest_period_days"), 90)
    horizon_roi = _choose_horizon_roi(row, horizon_days)

    top_factors = _s(row.get("top_factors"))
    top_risks = _s(row.get("top_risks"))
    conviction = _safe_float(row.get("conviction_score"), None)

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
        # v4.14.0 Phase J — badge the three reco_normalize v8.0.0 NEW
        # tokens (ACCUMULATE / STRONG_SELL / AVOID) so operators can see
        # at a glance when a row's recommendation is one of the tokens
        # that did NOT exist before the v8.0.0 vocabulary expansion.
        reco_canon = _normalize_reco_token(recommendation)
        if reco_canon and reco_canon in _RECO_8TIER_NEW_TOKENS:
            parts.append(f"Recommendation={recommendation} [NEW v8: {reco_canon}]")
        else:
            parts.append(f"Recommendation={recommendation}")
    # v4.14.0 Phase J — priority band display (shows the P1..P5 urgency
    # band emitted by data_engine_v2 v5.74.0+ / advisor_engine v4.5.0
    # whenever it is present on the row).
    priority_band_canon = _normalize_priority_band(row.get("recommendation_priority_band"))
    if priority_band_canon:
        parts.append(f"Priority={priority_band_canon}")
    # v4.14.0 Phase J — provider-rating divergence. Surfaces only when
    # the raw provider rating disagrees with the engine's recommendation;
    # this helps operators see when the engine's 8-tier judgement
    # differs from what the upstream provider said.
    provider_rating = _s(row.get("provider_rating"))
    if provider_rating and recommendation:
        prov_canon = _normalize_reco_token(provider_rating)
        engine_canon = _normalize_reco_token(recommendation)
        if prov_canon and engine_canon and prov_canon != engine_canon:
            parts.append(f"Provider={provider_rating}|Engine={recommendation}")
    if conviction is not None:
        parts.append(f"Conv={round(conviction, 0):.0f}/100")
    # v4.18.0 [FIX W-6] — reliability + gate verdict transparency: the two
    # values the golden composite and the tier split actually ranked on.
    _rel_v = _row_reliability(row)
    if _rel_v is not None:
        parts.append(f"Rel={_rel_v:.0f}/100")
    _gate_v = _s(row.get("investability_status"))
    if _gate_v:
        parts.append(f"Gate={_gate_v}")
    if confidence_bucket:
        parts.append(f"Confidence={confidence_bucket}")
    if risk_bucket:
        parts.append(f"Risk={risk_bucket}")
    if horizon_roi is not None:
        parts.append(f"Horizon ROI={round(horizon_roi * 100.0, 2)}%")
    if source_page:
        parts.append(f"Source={source_page}")
    if top_factors:
        first_factor = top_factors.split("|")[0].strip()
        if first_factor:
            parts.append(f"Top factor: {first_factor}")
    if top_risks:
        first_risk = top_risks.split("|")[0].strip()
        if first_risk:
            parts.append(f"Top risk: {first_risk}")
    if score_parts:
        parts.append(", ".join(score_parts[:3]))

    tags = _parse_warnings_tags(row)
    caveats: List[str] = []
    if _has_provider_error(row):
        err = _s(row.get("last_error_class") or row.get("provider_last_error_class") or row.get("error"))
        caveats.append(f"Provider Error: {err}" if err else "Provider Error")
    if _has_engine_drop_tag(tags):
        first_tag = next((t for t in tags if any(m in t for m in _ENGINE_DROP_TAGS)), "")
        caveats.append(f"Engine: {first_tag}" if first_tag else "Engine valuation dropped")
    if _has_forecast_unavail_tag(tags, row):
        caveats.append("Forecast: unavailable")
    if caveats:
        parts.append("[" + "; ".join(caveats) + "]")

    return " | ".join(parts) if parts else "Selected by Top10 composite scoring."


def _compute_overall_rank_map(rows: Sequence[Mapping[str, Any]]) -> Dict[int, int]:
    ranked_positions = sorted(
        range(len(rows)),
        key=lambda i: (
            0 if _safe_float(rows[i].get("overall_score"), None) is not None else 1,
            -(_safe_float(rows[i].get("overall_score"), 0.0) or 0.0),
            i,
        ),
    )
    return {pos: rank for rank, pos in enumerate(ranked_positions, start=1)}


def _rank_and_project_rows(rows: Sequence[Mapping[str, Any]], keys: Sequence[str], criteria: Mapping[str, Any]) -> List[Dict[str, Any]]:
    criteria_snapshot = _json_compact(criteria)
    overall_rank_map = _compute_overall_rank_map(rows)
    out: List[Dict[str, Any]] = []
    for idx, raw in enumerate(rows, start=1):
        row = dict(raw)
        # v4.18.0 [FIX W-4] RANK INTEGRITY: FORCE-overwrite ranks on every
        # build (the v4.17.0 fill-if-blank let rows ingested via the Top10
        # OUTPUT-PAGE fallback donor keep their OLD top10_rank — the root
        # cause of the live 13-row duplicate-rank corruption, 2026-06-10).
        # Contiguous unique 1..N is now a structural guarantee of the
        # payload, and rank_overall is always THIS build's overall-score
        # ordering, never an inherited one.
        row["top10_rank"] = idx
        row["rank_overall"] = overall_rank_map.get(idx - 1, idx)
        if _is_blank(row.get("selection_reason")):
            row["selection_reason"] = _canonical_selection_reason(row, criteria)
        # v4.18.0 [FIX W-6]: prefix the tier / backfill label assembled by
        # the fill stage (internal `_tier_label`; stripped by projection).
        _label = _s(row.get("_tier_label"))
        if _label:
            _base_reason = _s(row.get("selection_reason"))
            if _base_reason and not _base_reason.startswith(_label):
                row["selection_reason"] = f"{_label} | {_base_reason}"
            elif not _base_reason:
                row["selection_reason"] = _label
        if _is_blank(row.get("criteria_snapshot")):
            row["criteria_snapshot"] = criteria_snapshot
        # v4.19.0 [FIX X-4a]: alias-aware projection fallback. The exact-key
        # copy dropped values living on the row under a variant spelling
        # (camelCase / display-header) — the live 18-column enrichment gap
        # (selection_reason said Priority=P3 while the Priority Band column
        # sat blank). A BLANK exact read now falls back to
        # _extract_value_by_aliases; present exact values are untouched.
        projected: Dict[str, Any] = {}
        for k in keys:
            v = row.get(k)
            if v is None or (isinstance(v, str) and not v.strip()):
                _alt = _extract_value_by_aliases(row, k)
                if _alt is not None:
                    v = _alt
            projected[k] = _json_safe(v)
        out.append(projected)
    return out


def _page_priority_symbol_limit(criteria: Mapping[str, Any]) -> int:
    limit = _safe_int(criteria.get("limit"), 10)
    return max(min(limit * 3, HYDRATION_SYMBOL_CAP), min(HYDRATION_SYMBOL_CAP, 12))


def _merge_symbol_row_lists(primary: Sequence[Mapping[str, Any]], secondary: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}

    def _put(raw: Mapping[str, Any]) -> None:
        row = _normalize_candidate_row(raw)
        sym = _normalize_symbol(row.get("symbol") or row.get("ticker"))
        if not sym:
            return
        existing = merged.get(sym)
        if existing is None:
            merged[sym] = dict(row)
            return
        # v4.16.0 Phase B: provenance-aware merge. Equal-rank rows preserve
        # the v4.15.0 prefer-richer behavior verbatim inside the helper.
        merged[sym] = _merge_row_provenance(existing, row)

    for row in primary:
        _put(row)
    for row in secondary:
        _put(row)
    return list(merged.values())


def _merge_row_prefer_richer(base: Mapping[str, Any], update: Mapping[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in dict(update).items():
        if value not in (None, "", [], {}, ()):
            merged[key] = value
    return merged


# =============================================================================
# v4.16.0 Phase A/B/C/D helpers — vintage protection + admission (Fix U)
# =============================================================================
_GATE_CRITICAL_MATCH_CACHE: Optional[frozenset] = None


def _gate_critical_match_set() -> frozenset:
    """Canonical spellings of the gate-critical keys (snake / camel / compact).

    Built once and cached. Uses the same `_header_to_key` / `_compact_key`
    normalizers as the row-lookup machinery so any spelling an engine payload
    or sheet header could produce is matched (e.g. forecast_reliability_score,
    forecastReliabilityScore, "Forecast Reliability Score").
    """
    global _GATE_CRITICAL_MATCH_CACHE
    if _GATE_CRITICAL_MATCH_CACHE is None:
        tokens: set = set()
        for key in GATE_CRITICAL_KEYS:
            for variant in _canonical_key_variants(key):
                tokens.add(variant)
                tokens.add(_compact_key(variant))
        _GATE_CRITICAL_MATCH_CACHE = frozenset(t for t in tokens if t)
    return _GATE_CRITICAL_MATCH_CACHE


def _strip_gate_critical_fields(row: Mapping[str, Any]) -> Tuple[Dict[str, Any], int]:
    """Return a copy of `row` with gate-critical fields removed + strip count.

    v4.16.0 Phase C. Applied to STALE donors only (page snapshot, Top10
    output-page fallback) before they enter the candidate pool, so a stale
    gate value can never masquerade as the engine's current gate decision.
    BLANK-OVER-STALE: a missing reliability is honest; a stale one misleads.
    """
    match = _gate_critical_match_set()
    cleaned: Dict[str, Any] = {}
    stripped = 0
    for key, value in dict(row).items():
        raw = _s(key)
        if raw and (
            raw in match
            or raw.lower() in match
            or _header_to_key(raw) in match
            or _compact_key(raw) in match
        ):
            stripped += 1
            continue
        cleaned[key] = value
    return cleaned, stripped


# =============================================================================
# v4.18.0 [FIX W-7] — Top10-run metadata quarantine (output-fallback donor)
# =============================================================================
_TOP10_METADATA_MATCH_CACHE: Optional[frozenset] = None


def _top10_metadata_match_set() -> frozenset:
    """Canonical spellings of the Top10-run metadata keys (W-7). Mirrors the
    gate-critical matcher so display-header spellings ("Top10 Rank",
    "Selection Reason", "Criteria Snapshot") are caught too."""
    global _TOP10_METADATA_MATCH_CACHE
    if _TOP10_METADATA_MATCH_CACHE is None:
        tokens: set = set()
        for key in TOP10_METADATA_STRIP_KEYS:
            for variant in _canonical_key_variants(key):
                tokens.add(variant)
                tokens.add(_compact_key(variant))
        _TOP10_METADATA_MATCH_CACHE = frozenset(t for t in tokens if t)
    return _TOP10_METADATA_MATCH_CACHE


def _strip_top10_metadata(row: Mapping[str, Any]) -> Tuple[Dict[str, Any], int]:
    """v4.18.0 [FIX W-7]: strip previous-run Top10 metadata (top10_rank /
    selection_reason / criteria_snapshot) from the OUTPUT-PAGE fallback
    donor at ingest. These fields describe a PREVIOUS selector run and are
    stale by definition on re-ingest: stale ranks were the W-4 corruption
    vector, and a stale selection_reason would otherwise survive the
    fill-if-blank path and be served under a fresh tier label."""
    match = _top10_metadata_match_set()
    cleaned: Dict[str, Any] = {}
    stripped = 0
    for key, value in dict(row).items():
        raw = _s(key)
        if raw and (
            raw in match
            or raw.lower() in match
            or _header_to_key(raw) in match
            or _compact_key(raw) in match
        ):
            stripped += 1
            continue
        cleaned[key] = value
    return cleaned, stripped


def _row_prov(row: Mapping[str, Any]) -> int:
    """Provenance rank of a row. Untagged rows default to live (rank 3)."""
    return _safe_int(row.get(_PROV_KEY), _PROV_LIVE)


def _tag_provenance(row: Dict[str, Any], rank: int) -> Dict[str, Any]:
    """Tag `row` in place with its provenance rank and return it."""
    row[_PROV_KEY] = rank
    return row


def _merge_row_provenance(a: Mapping[str, Any], b: Mapping[str, Any]) -> Dict[str, Any]:
    """Provenance-aware symbol merge (v4.16.0 Phase B).

    Higher-provenance row's non-blank values win unconditionally; the
    lower-provenance row may only fill blanks. On EQUAL ranks the v4.15.0
    prefer-richer behavior is preserved verbatim: the richer row's non-blank
    values overwrite the poorer row's. The merged row keeps the HIGHER rank,
    so a candidate upgraded by a fresh donor stays protected in later merges.
    """
    rank_a, rank_b = _row_prov(a), _row_prov(b)
    if rank_a == rank_b:
        richer, poorer = (a, b) if _row_richness(a) >= _row_richness(b) else (b, a)
        merged = _merge_row_prefer_richer(poorer, richer)
        merged[_PROV_KEY] = rank_a
        return merged
    fresh, stale = (a, b) if rank_a > rank_b else (b, a)
    merged = _merge_row_prefer_richer(stale, fresh)
    merged[_PROV_KEY] = max(rank_a, rank_b)
    return merged


def _admission_check(row: Mapping[str, Any]) -> Tuple[bool, str]:
    """Top10 admission gate (v4.16.0 Phase D). Returns (admissible, reason).

    A row is NOT admissible to selection or backfill when:
      - current_price is blank or <= 0                  -> "missing_price"
      - confidence (forecast_confidence falling back to
        confidence_score) is blank or <= 0              -> "zero_confidence"
      - warnings carry recommendation_forced_hold_missing_price
                                                        -> "forced_hold"
    Verified defect case (2026-06-10): CPI.JSE in the live Top 10 with no
    price, confidence 0.00, and the forced-HOLD tag.
    """
    price = _safe_float(row.get("current_price"), None)
    if price is None or price <= 0.0:
        return False, "missing_price"
    conf = _safe_float(row.get("forecast_confidence"), None)
    if conf is None:
        conf = _safe_float(row.get("confidence_score"), None)
    if conf is None or conf <= 0.0:
        return False, "zero_confidence"
    tags = _parse_warnings_tags(row)
    if any(_ADMISSION_FORCED_HOLD_TAG in _s(t) for t in tags):
        return False, "forced_hold"
    return True, ""


async def _fetch_page_universe_size(engine: Any, page: str, deadline: Optional[float] = None) -> Optional[int]:
    """v4.19.0 [FIX X-3]: page universe size for the coverage audit.

    Fail-soft and tightly budget-clamped: this is bookkeeping, never worth
    spending real budget on. Returns None when the engine has no symbol-list
    surface or the call fails — coverage meta simply omits that page.
    """
    try:
        payload = await _call_engine_method(
            engine,
            ("get_page_symbols", "get_symbols_for_page", "get_page_symbol_list", "list_page_symbols"),
            [((page,), {}), ((), {"page": page}), ((), {"page_name": page}), ((), {"sheet_name": page}), ((), {"sheet": page})],
            timeout_seconds=_budget_timeout(min(ENGINE_CALL_TIMEOUT_SEC, 5.0), deadline),
        )
    except Exception:
        return None
    if payload is None:
        return None
    if isinstance(payload, (list, tuple, set)):
        return len([s for s in payload if _s(s)])
    if isinstance(payload, Mapping):
        for k in ("symbols", "tickers", "items", "data"):
            v = payload.get(k)
            if isinstance(v, (list, tuple, set)):
                return len([s for s in v if _s(s)])
        c = _safe_int(payload.get("count") or payload.get("total"), -1)
        return c if c >= 0 else None
    return None


async def _collect_page_rows_with_fallback(engine: Any, page: str, mode: str, deadline: Optional[float] = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    meta: Dict[str, Any] = {
        "page": page,
        "rows": 0,
        "snapshot_rows": 0,
        "used_snapshot": False,
        "merged_snapshot": False,
        "timed_out": False,
        "error": "",
        # v4.16.0 Phase C audit counter.
        "gate_fields_quarantined": 0,
    }
    try:
        rows = await asyncio.wait_for(_fetch_page_rows(engine, page, SOURCE_PAGE_LIMIT, mode), timeout=_budget_timeout(PAGE_TOTAL_TIMEOUT_SEC, deadline))  # v4.19.0 [FIX X-1]
        meta["rows"] = len(rows)
        # v4.16.0 Phase A: live page rows carry the freshest gate decision.
        for _r in rows:
            _tag_provenance(_r, _PROV_LIVE)
    except asyncio.TimeoutError:
        rows = []
        meta["timed_out"] = True
        meta["error"] = "page_rows_timeout"
    except Exception as exc:
        rows = []
        meta["error"] = f"page_rows_failed:{type(exc).__name__}"

    should_try_snapshot_merge = bool(rows) and (len(rows) < 5 or max((_row_richness(r) for r in rows), default=0) < 10)
    if rows and not should_try_snapshot_merge:
        return rows, meta

    try:
        snap_rows = await asyncio.wait_for(_fetch_page_snapshot_rows(engine, page), timeout=_budget_timeout(PAGE_TOTAL_TIMEOUT_SEC, deadline))  # v4.19.0 [FIX X-1/X-2] de-clamped (was min(...,8))
        # v4.16.0 Phase A/C: snapshot rows are a possibly-stale donor. Strip
        # gate-critical fields (blank-over-stale) and tag rank 1 so they can
        # fill blanks but never overwrite a fresh row's values.
        if snap_rows:
            quarantined: List[Dict[str, Any]] = []
            stripped_total = 0
            for _sr in snap_rows:
                cleaned, stripped = _strip_gate_critical_fields(_sr)
                stripped_total += stripped
                quarantined.append(_tag_provenance(cleaned, _PROV_SNAPSHOT))
            snap_rows = quarantined
            meta["gate_fields_quarantined"] = stripped_total
        meta["snapshot_rows"] = len(snap_rows)
        meta["used_snapshot"] = bool(snap_rows)
        if rows and snap_rows:
            merged_rows = _merge_symbol_row_lists(rows, snap_rows)
            meta["merged_snapshot"] = True
            return merged_rows, meta
        if snap_rows:
            return snap_rows, meta
        return rows, meta
    except asyncio.TimeoutError:
        if not meta["error"]:
            meta["error"] = "snapshot_timeout"
        return rows if rows else [], meta
    except Exception as exc:
        if not meta["error"]:
            meta["error"] = f"snapshot_failed:{type(exc).__name__}"
        return rows if rows else [], meta


async def _hydrate_page_rows(engine: Any, rows: List[Dict[str, Any]], mode: str, criteria: Mapping[str, Any], deadline: Optional[float] = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    meta: Dict[str, Any] = {
        "requested_symbols": 0,
        "enriched_rows": 0,
        "hydration_used": False,
        "hydration_error": "",
    }
    if not rows:
        return rows, meta

    ranked_candidates: List[Tuple[float, Dict[str, Any]]] = []
    for row in rows:
        normalized = _normalize_candidate_row(row)
        sym = _normalize_symbol(normalized.get("symbol") or normalized.get("ticker"))
        if not sym:
            continue
        ranked_candidates.append((_selector_score(normalized, criteria), normalized))

    ranked_candidates.sort(key=lambda x: (x[0], _row_richness(x[1])), reverse=True)
    symbols = _dedupe_keep_order(
        _normalize_symbol(item[1].get("symbol") or item[1].get("ticker")) for item in ranked_candidates
    )[: _page_priority_symbol_limit(criteria)]

    meta["requested_symbols"] = len(symbols)
    if not symbols:
        return rows, meta

    try:
        enriched_rows = await asyncio.wait_for(_fetch_direct_symbol_rows(engine, symbols, mode), timeout=_budget_timeout(PAGE_TOTAL_TIMEOUT_SEC, deadline))  # v4.19.0 [FIX X-1/X-2] de-clamped (was min(...,10))
        meta["enriched_rows"] = len(enriched_rows)
        meta["hydration_used"] = bool(enriched_rows)
    except asyncio.TimeoutError:
        meta["hydration_error"] = "hydration_timeout"
        return rows, meta
    except Exception as exc:
        meta["hydration_error"] = f"hydration_failed:{type(exc).__name__}"
        return rows, meta

    if not enriched_rows:
        return rows, meta

    row_map: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        normalized = _normalize_candidate_row(row)
        sym = _normalize_symbol(normalized.get("symbol") or normalized.get("ticker"))
        if sym:
            row_map[sym] = normalized

    for er in enriched_rows:
        normalized = _normalize_candidate_row(er)
        sym = _normalize_symbol(normalized.get("symbol") or normalized.get("ticker"))
        if not sym:
            continue
        # v4.16.0 Phase A: hydration rows are fresh engine quotes (rank 3).
        # The v4.15.0 overlay semantics are preserved VERBATIM here: the
        # enriched row's non-blank values always overwrite the base row
        # (`_merge_row_prefer_richer(base, update)` with update = enriched).
        # The merged row carries the max provenance rank so a base that was
        # a quarantined snapshot (rank 1) is upgraded to fresh (rank 3).
        _tag_provenance(normalized, _PROV_HYDRATED)
        if sym in row_map:
            base = row_map[sym]
            merged = _merge_row_prefer_richer(base, normalized)
            merged[_PROV_KEY] = max(_row_prov(base), _PROV_HYDRATED)
            row_map[sym] = merged
        else:
            row_map[sym] = normalized

    return list(row_map.values()), meta


async def _collect_candidate_rows(engine: Any, criteria: Mapping[str, Any], mode: str, deadline: Optional[float] = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    direct_symbols = [_normalize_symbol(s) for s in _normalize_list(criteria.get("direct_symbols")) if _normalize_symbol(s)]
    emergency_symbols = _normalize_list(criteria.get("emergency_symbols"))
    pages = _safe_source_pages(_normalize_list(criteria.get("pages_selected")))
    direct_symbol_index_map = {sym: idx for idx, sym in enumerate(direct_symbols)}

    meta: Dict[str, Any] = {
        "engine_source": _ENGINE_CACHE_SOURCE or "",
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
        # v4.16.0 Phase C audit counters.
        "gate_fields_quarantined_count": 0,
        "quarantined_donor_sources": [],
        # v4.18.0 [FIX W-7] audit counter.
        "top10_metadata_stripped_count": 0,
        # v4.19.0 [FIX X-1/X-3] budget + coverage audit.
        "budget_exhausted": False,
        "pages_skipped": [],
        "page_universe": {},
        "page_coverage": {},
        "universe_starved": False,
    }

    candidates: Dict[str, Dict[str, Any]] = {}

    def _put_row(raw: Mapping[str, Any], source_page: str = "") -> None:
        row = _normalize_candidate_row(raw)
        sym = _normalize_symbol(row.get("symbol") or row.get("ticker"))
        if not sym:
            return
        if source_page and _is_blank(row.get("source_page")):
            row["source_page"] = source_page
        if sym in direct_symbol_index_map:
            row["_direct_symbol_priority"] = True
            row["_direct_symbol_index"] = direct_symbol_index_map[sym]
            if _is_blank(row.get("source_page")):
                row["source_page"] = "Direct"
        existing = candidates.get(sym)
        if existing is None:
            candidates[sym] = row
            return
        # v4.16.0 Phase B: provenance-aware merge. Higher-provenance values
        # win unconditionally; equal ranks preserve v4.15.0 prefer-richer.
        candidates[sym] = _merge_row_provenance(existing, row)

    if direct_symbols:
        try:
            direct_rows = await asyncio.wait_for(_fetch_direct_symbol_rows(engine, direct_symbols[:HYDRATION_SYMBOL_CAP], mode), timeout=_budget_timeout(BUILDER_TOTAL_TIMEOUT_SEC / 2.0, deadline))  # v4.19.0 [FIX X-1/X-2] de-clamped (was min(...,12))
            meta["direct_symbol_rows"] = len(direct_rows)
            meta["partial_success"] = meta["partial_success"] or bool(direct_rows)
            for row in direct_rows:
                # v4.16.0 Phase A: direct-symbol rows are fresh engine quotes.
                _tag_provenance(row, _PROV_HYDRATED)
                _put_row(row, "")
        except asyncio.TimeoutError:
            meta["direct_symbol_error"] = "direct_symbol_timeout"
        except Exception as exc:
            meta["direct_symbol_error"] = f"direct_symbol_failed:{type(exc).__name__}"

    early_stop_target = max(10, _safe_int(criteria.get("limit"), 10) * EARLY_STOP_MULTIPLIER)

    for _page_idx, page in enumerate(pages):
        # v4.19.0 [FIX X-1]: stop COLLECTING (not the build) when the soft
        # budget is nearly spent — gating / tiering / ranking / projection
        # then run on whatever has been collected, so the payload degrades
        # to a PARTIAL ranked set with full meta instead of rows:0.
        # The FIRST page is ALWAYS attempted regardless of remaining budget
        # (its fetch timeout is itself budget-clamped via _budget_timeout,
        # and the outer backstop still bounds the worst case): a guard that
        # can skip page one reintroduces the very zero-row degradation this
        # fix exists to eliminate whenever TOTAL - RESERVE <= the floor.
        _rem = _remaining_budget(deadline)
        if _page_idx > 0 and _rem is not None and _rem <= PAGE_LOOP_MIN_REMAINING_SEC:
            meta["budget_exhausted"] = True
            meta["pages_skipped"] = list(pages[_page_idx:])
            logger.warning(
                "[v4.19.0 BUDGET] soft budget exhausted before page=%s; skipping %s",
                page, meta["pages_skipped"],
            )
            break
        page_rows, page_meta = await _collect_page_rows_with_fallback(engine, page, mode, deadline)
        meta["source_page_rows"][page] = page_meta.get("rows", 0)
        meta["snapshot_rows"][page] = page_meta.get("snapshot_rows", 0)
        # v4.19.0 [FIX X-3]: record the page universe size beside ingested
        # rows so truncation / starvation is visible, never implicit.
        _universe_size = await _fetch_page_universe_size(engine, page, deadline)
        if _universe_size is not None:
            meta["page_universe"][page] = _universe_size
        # v4.16.0 Phase C: roll up snapshot-donor quarantine counts.
        _q = _safe_int(page_meta.get("gate_fields_quarantined"), 0)
        if _q > 0:
            meta["gate_fields_quarantined_count"] += _q
            meta["quarantined_donor_sources"].append(f"snapshot:{page}")

        hydrated_rows = page_rows
        hydration_meta: Dict[str, Any] = {"page": page}
        if page_rows:
            hydrated_rows, hydration_meta = await _hydrate_page_rows(engine, page_rows, mode, criteria, deadline)
            hydration_meta["page"] = page

        meta["page_diagnostics"].append(_json_safe(page_meta))
        meta["hydration_diagnostics"].append(_json_safe(hydration_meta))
        if hydrated_rows:
            meta["partial_success"] = True

        for row in hydrated_rows:
            _put_row(row, page)

        if len(candidates) >= early_stop_target and page != "My_Portfolio":
            meta["early_stop"] = True
            meta["early_stop_after_page"] = page
            break

    if not candidates and emergency_symbols:
        try:
            emergency_rows = await asyncio.wait_for(
                _fetch_direct_symbol_rows(engine, emergency_symbols[:HYDRATION_SYMBOL_CAP], mode),
                timeout=_budget_timeout(BUILDER_TOTAL_TIMEOUT_SEC / 2.0, deadline),  # v4.19.0 [FIX X-1/X-2] de-clamped (was min(...,12))
            )
            meta["used_emergency_symbols"] = bool(emergency_rows)
            meta["emergency_symbol_rows"] = len(emergency_rows)
            for row in emergency_rows:
                # v4.16.0 Phase A: emergency rows are fresh engine fetches
                # (rank 2) — no quarantine, but they yield to rank-3 donors.
                _tag_provenance(row, _PROV_EMERGENCY)
                _put_row(row, "Emergency")
        except Exception as exc:
            meta["emergency_symbol_error"] = f"emergency_symbol_failed:{type(exc).__name__}"

    fallback_target = max(1, _safe_int(criteria.get("limit"), 10))
    if len(candidates) < fallback_target:
        try:
            fallback_rows = await asyncio.wait_for(
                _fetch_page_rows(engine, OUTPUT_PAGE, max(30, fallback_target * 2), mode),
                timeout=_budget_timeout(PAGE_TOTAL_TIMEOUT_SEC, deadline),  # v4.19.0 [FIX X-1/X-2] de-clamped (was min(...,10))
            )
            meta["top10_output_fallback_rows"] = len(fallback_rows)
        except asyncio.TimeoutError:
            fallback_rows = []
            meta["top10_output_fallback_error"] = "top10_output_timeout"
        except Exception as exc:
            fallback_rows = []
            meta["top10_output_fallback_error"] = f"top10_output_failed:{type(exc).__name__}"

        if not fallback_rows:
            try:
                fallback_rows = await asyncio.wait_for(_fetch_page_snapshot_rows(engine, OUTPUT_PAGE), timeout=_budget_timeout(PAGE_TOTAL_TIMEOUT_SEC, deadline))  # v4.19.0 [FIX X-1/X-2] de-clamped (was min(...,8))
                meta["top10_output_snapshot_rows"] = len(fallback_rows)
            except asyncio.TimeoutError:
                fallback_rows = []
                meta["top10_output_snapshot_error"] = "top10_output_snapshot_timeout"
            except Exception as exc:
                fallback_rows = []
                meta["top10_output_snapshot_error"] = f"top10_output_snapshot_failed:{type(exc).__name__}"

        if fallback_rows:
            meta["top10_output_fallback_used"] = True
            # v4.16.0 Phase A/C: the previous Top_10 output is STALE BY
            # DESIGN (rank 0) — the exact donor behind the self-referential
            # staleness loop. Strip gate-critical fields (blank-over-stale)
            # so old gate values can never re-enter the pool.
            # v4.18.0 [FIX W-7]: ALSO strip the previous run's Top10
            # metadata (top10_rank / selection_reason / criteria_snapshot) —
            # stale ranks were the W-4 corruption vector, and a stale
            # selection_reason would survive the fill-if-blank path.
            quarantined_fallback: List[Dict[str, Any]] = []
            _fb_stripped = 0
            _fb_meta_stripped = 0
            for _fr in fallback_rows:
                cleaned, stripped = _strip_gate_critical_fields(_fr)
                cleaned, meta_stripped = _strip_top10_metadata(cleaned)
                _fb_stripped += stripped
                _fb_meta_stripped += meta_stripped
                quarantined_fallback.append(_tag_provenance(cleaned, _PROV_OUTPUT_FALLBACK))
            fallback_rows = quarantined_fallback
            if _fb_stripped > 0:
                meta["gate_fields_quarantined_count"] += _fb_stripped
                meta["quarantined_donor_sources"].append(f"output_fallback:{OUTPUT_PAGE}")
            if _fb_meta_stripped > 0:
                meta["top10_metadata_stripped_count"] += _fb_meta_stripped
        for row in fallback_rows:
            _put_row(row, OUTPUT_PAGE)

    # v4.19.0 [FIX X-3]: coverage rollup — ingested vs universe per page,
    # plus the global starvation flag (non-empty universe, zero ingested).
    for _pg, _uni in meta["page_universe"].items():
        _fetched = _safe_int(meta["source_page_rows"].get(_pg), 0)
        if _uni and _uni > 0:
            meta["page_coverage"][_pg] = round(min(1.0, _fetched / float(_uni)), 4)
            if _fetched == 0:
                meta["universe_starved"] = True

    meta["deduped_candidate_count"] = len(candidates)
    return list(candidates.values()), meta


def _fill_top10_selection(
    pools: Sequence[Sequence[Tuple[float, Dict[str, Any]]]],
    limit: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """v4.18.0 [FIX W-2c / W-5] — tiered four-pool fill with sector cap.

    `pools` arrive PRE-SORTED (composite + tiebreak tuple, best first) in
    strict fill order: T1-pass, T2-pass, T1-fail, T2-fail. Rows are admitted
    in that order, deduped by symbol, subject to the sector-diversification
    cap (TOP10_SECTOR_CAP per lowercased sector; blank sector buckets as
    "unknown"; 0 disables). Cap-deferred rows re-enter IN ORIGINAL ORDER on
    a relaxation pass only when the pools cannot otherwise fill `limit`, so
    a full list is still guaranteed; relaxed rows are flagged via the
    internal `_sector_cap_relaxed` key for the W-6 label.
    """
    cap = TOP10_SECTOR_CAP
    selected: List[Dict[str, Any]] = []
    seen: set = set()
    sector_counts: Dict[str, int] = {}
    deferred: List[Dict[str, Any]] = []
    fill_meta: Dict[str, Any] = {
        "sector_cap": cap,
        "sector_cap_deferred": 0,
        "sector_cap_relaxed": 0,
    }

    # v4.20.0 (A2): canonicalize the sector BEFORE keying the cap, so one economic
    # sector occupies one bucket across provider vocabularies (Yahoo "Financial
    # Services" and GICS "Financials" collapse to one). Gate OFF (default) -- or
    # core.sectors unavailable -- leaves _norm_on False, and _sector_key then
    # produces a byte-identical key to v4.19.0. Each remap is counted for audit.
    _norm_on = bool(TOP10_SECTOR_NORMALIZE) and _canon_sector is not None
    fill_meta["sector_normalize"] = {"enabled": _norm_on, "remaps": {}}

    def _sector_key(row: Mapping[str, Any]) -> str:
        raw = _s(row.get("sector"))
        if _norm_on:
            canon = _canon_sector(raw)
            if raw and canon != raw:
                _rm = fill_meta["sector_normalize"]["remaps"]
                _k = raw + " -> " + canon
                _rm[_k] = _rm.get(_k, 0) + 1
            raw = canon
        return raw.strip().lower() or "unknown"

    def _admit(row: Dict[str, Any], relaxing: bool) -> bool:
        sym = _normalize_symbol(row.get("symbol") or row.get("ticker"))
        if not sym or sym in seen:
            return False
        sk = _sector_key(row)
        if cap > 0 and sector_counts.get(sk, 0) >= cap:
            if not relaxing:
                deferred.append(row)
                fill_meta["sector_cap_deferred"] += 1
                return False
            row["_sector_cap_relaxed"] = True
            fill_meta["sector_cap_relaxed"] += 1
        selected.append(row)
        seen.add(sym)
        sector_counts[sk] = sector_counts.get(sk, 0) + 1
        return True

    for pool in pools:
        if len(selected) >= limit:
            break
        for _score, row in pool:
            if len(selected) >= limit:
                break
            _admit(row, relaxing=False)
    if len(selected) < limit and deferred:
        for row in deferred:
            if len(selected) >= limit:
                break
            _admit(row, relaxing=True)
    return selected, fill_meta


# =============================================================================
# v4.21.0 — SELECTION STABILITY LAYER (membership hysteresis) — engine.
# Pure functions: no I/O, no ENV, no engine access. See the banner WHY block
# for the full rule set. All are unreachable unless the caller opted in.
# =============================================================================
def _stability_today_key() -> str:
    """UTC calendar date key — counters advance at most once per key."""
    return time.strftime("%Y-%m-%d", time.gmtime())


def _stability_days_between(a: str, b: str) -> int:
    """Whole days from ISO date `a` to ISO date `b` (floor 0; never raises)."""
    try:
        da = _dt.date.fromisoformat(_s(a).strip())
        db = _dt.date.fromisoformat(_s(b).strip())
        return max(0, (db - da).days)
    except Exception:
        return 0


def _stability_knobs(criteria: Mapping[str, Any]) -> Optional[Dict[str, int]]:
    """Return the clamped knob set when the caller opted in; else None.

    Engagement = `stability_state` present (any value; ''/{} bootstraps) OR a
    non-empty `stability` mapping. An explicit stability.enabled=false always
    disengages (kill switch without removing the state blob).
    """
    raw_state = criteria.get("stability_state")
    cfg_raw = criteria.get("stability")
    cfg: Dict[str, Any] = dict(cfg_raw) if isinstance(cfg_raw, Mapping) else {}
    if raw_state is None and not cfg:
        return None
    if "enabled" in cfg and not _coerce_bool(cfg.get("enabled"), True):
        return None

    def _k(name: str, default: int, lo: int, hi: int) -> int:
        return max(lo, min(hi, _safe_int(cfg.get(name), default)))

    return {
        "confirm_days": _k("confirm_days", STABILITY_DEFAULT_CONFIRM_DAYS, 1, 30),
        "exit_days": _k("exit_days", STABILITY_DEFAULT_EXIT_DAYS, 1, 30),
        "rank_buffer": _k("rank_buffer", STABILITY_DEFAULT_RANK_BUFFER, 0, 500),
        "smooth_days": _k("smooth_days", STABILITY_DEFAULT_SMOOTH_DAYS, 1, 30),
    }


def _stability_parse_state(raw: Any) -> Dict[str, Any]:
    """Sanitize a caller-supplied state blob (mapping or JSON string).

    Unknown shapes degrade to an empty state; never raises. Per-symbol fields:
    ci (consecutive qualifying days), co (consecutive missed days, members),
    member, since (entry ISO date), ls (last seen ISO date), hist (score pts).
    """
    obj: Any = raw
    if isinstance(raw, str):
        s = raw.strip()
        if s:
            try:
                obj = json.loads(s)
            except Exception:
                obj = {}
        else:
            obj = {}
    if not isinstance(obj, Mapping):
        obj = {}
    out: Dict[str, Any] = {"v": STABILITY_STATE_VERSION, "date": _s(obj.get("date")).strip(), "symbols": {}}
    syms = obj.get("symbols")
    if isinstance(syms, Mapping):
        for k, v in syms.items():
            sym = _normalize_symbol(k)
            if not sym or not isinstance(v, Mapping):
                continue
            hist: List[float] = []
            hist_raw = v.get("hist")
            if isinstance(hist_raw, (list, tuple)):
                for h in hist_raw:
                    f = _safe_float(h, None)
                    if f is not None:
                        hist.append(round(f, 4))
            out["symbols"][sym] = {
                "ci": max(0, _safe_int(v.get("ci"), 0)),
                "co": max(0, _safe_int(v.get("co"), 0)),
                "member": _coerce_bool(v.get("member"), False),
                "since": _s(v.get("since")).strip(),
                "ls": _s(v.get("ls")).strip(),
                "hist": hist[-30:],
            }
    return out


def _apply_selection_stability(
    *,
    raw_selected: Sequence[Dict[str, Any]],
    pools: Sequence[Sequence[Tuple[float, Dict[str, Any]]]],
    criteria: Mapping[str, Any],
    knobs: Mapping[str, int],
    limit: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Apply membership hysteresis to today's raw fill result.

    `raw_selected` = _fill_top10_selection output (today's memoryless answer);
    `pools` = the four pre-sorted pools (defines the all-pool global rank used
    by the rank buffer, and supplies row objects for grace-held incumbents).
    Returns (stable_rows, stability_meta); stability_meta["state"] is the
    updated blob the caller must persist and post back next run.
    """
    today = _stability_today_key()
    state = _stability_parse_state(criteria.get("stability_state"))
    prev_date = state.get("date") or ""
    day_advance = prev_date != today  # first-ever run (blank prev) also advances
    symbols: Dict[str, Dict[str, Any]] = state["symbols"]
    limit = max(1, int(limit))

    # ---- all-pool row map + global rank (fill order = quality order) --------
    sym2row: Dict[str, Dict[str, Any]] = {}
    global_rank: Dict[str, int] = {}
    rank = 0
    for pool in pools:
        for _score, row in pool:
            sym = _normalize_symbol(row.get("symbol") or row.get("ticker"))
            if not sym or sym in sym2row:
                continue
            rank += 1
            sym2row[sym] = row
            global_rank[sym] = rank

    raw_syms: List[str] = []
    raw_by_sym: Dict[str, Dict[str, Any]] = {}
    for row in raw_selected:
        sym = _normalize_symbol(row.get("symbol") or row.get("ticker"))
        if sym and sym not in raw_by_sym:
            raw_syms.append(sym)
            raw_by_sym[sym] = row
    raw_set = frozenset(raw_syms)

    def _score_of(sym: str) -> Optional[float]:
        row = raw_by_sym.get(sym) or sym2row.get(sym)
        if row is None:
            return None
        v = _safe_float(row.get("overall_score"), None)
        if v is None:
            v = _safe_float(row.get("opportunity_score"), None)
        return v

    # ---- counter pass (day-keyed) -------------------------------------------
    smooth_days = max(1, _safe_int(knobs.get("smooth_days"), STABILITY_DEFAULT_SMOOTH_DAYS))
    rank_buffer = max(0, _safe_int(knobs.get("rank_buffer"), STABILITY_DEFAULT_RANK_BUFFER))
    for sym in set(symbols) | raw_set:
        st = symbols.get(sym)
        if st is None:
            st = {"ci": 0, "co": 0, "member": False, "since": "", "ls": "", "hist": []}
            symbols[sym] = st
        sc = _score_of(sym)
        if day_advance:
            if sym in raw_set:
                st["ci"] = _safe_int(st.get("ci"), 0) + 1
                st["co"] = 0
            else:
                st["ci"] = 0
                if st.get("member"):
                    r = global_rank.get(sym)
                    within_buffer = rank_buffer > 0 and r is not None and r <= rank_buffer
                    if not within_buffer:
                        st["co"] = _safe_int(st.get("co"), 0) + 1
                    # within buffer: rank-jitter immunity — the exit clock
                    # PAUSES for the day (it does not reset).
            if sc is not None:
                hist = st.setdefault("hist", [])
                hist.append(round(sc, 4))
        else:
            # Same-day re-run: clocks frozen; refresh the day's score point.
            if sc is not None:
                hist = st.setdefault("hist", [])
                if hist:
                    hist[-1] = round(sc, 4)
                else:
                    hist.append(round(sc, 4))
        st["hist"] = (st.get("hist") or [])[-smooth_days:]
        if sym in sym2row:
            st["ls"] = today

    # ---- membership pass ------------------------------------------------------
    exited_hard: List[str] = []
    exited_soft: List[str] = []
    exited_capacity: List[str] = []
    survivors: List[str] = []
    for sym in [s for s, st in symbols.items() if st.get("member")]:
        st = symbols[sym]
        if sym not in sym2row:
            # HARD EXIT: absent from every pool (gated / SELL-class / dropped
            # upstream). Safety verdicts are never grace-held.
            st["member"] = False
            st["ci"] = 0
            st["co"] = 0
            exited_hard.append(sym)
            continue
        if _safe_int(st.get("co"), 0) >= _safe_int(knobs.get("exit_days"), STABILITY_DEFAULT_EXIT_DAYS):
            st["member"] = False
            st["ci"] = 0
            st["co"] = 0
            exited_soft.append(sym)
            continue
        survivors.append(sym)

    def _smoothed(sym: str) -> float:
        hist = symbols.get(sym, {}).get("hist") or []
        if hist:
            return sum(hist) / float(len(hist))
        sc = _score_of(sym)
        return sc if sc is not None else 0.0

    raw_index = {s: i for i, s in enumerate(raw_syms)}
    survivors.sort(key=lambda s: (-_smoothed(s), raw_index.get(s, 10 ** 6), global_rank.get(s, 10 ** 6)))
    if len(survivors) > limit:
        for sym in survivors[limit:]:
            st = symbols[sym]
            st["member"] = False
            st["ci"] = 0
            st["co"] = 0
            exited_capacity.append(sym)
        survivors = survivors[:limit]

    final: List[str] = list(survivors)
    entered: List[str] = []
    exited_displaced: List[str] = []
    fast_tracked: List[str] = []
    confirm_days = _safe_int(knobs.get("confirm_days"), STABILITY_DEFAULT_CONFIRM_DAYS)
    for sym in raw_syms:  # confirmed challengers, today's quality order
        if sym in final:
            continue
        if _safe_int(symbols[sym].get("ci"), 0) < confirm_days:
            continue
        if len(final) < limit:
            entered.append(sym)
            final.append(sym)
            continue
        # DISPLACEMENT: seats full — a CONFIRMED challenger (persistent, not
        # jitter) may replace the weakest incumbent it beats on the SMOOTHED
        # score. Grace/buffer protect against noise, not against a
        # demonstrably better replacement. Fast-track never displaces.
        weakest = final[-1] if final else None
        if weakest is not None and _smoothed(sym) > _smoothed(weakest):
            final.pop()
            st_w = symbols[weakest]
            st_w["member"] = False
            st_w["ci"] = 0
            st_w["co"] = 0
            exited_displaced.append(weakest)
            entered.append(sym)
            # keep `final` ordered: insert by smoothed among current members
            final.append(sym)
            final.sort(key=lambda s: (-_smoothed(s), raw_index.get(s, 10 ** 6), global_rank.get(s, 10 ** 6)))
    for sym in raw_syms:  # fast-track fill: the sheet never under-fills
        if len(final) >= limit:
            break
        if sym in final:
            continue
        fast_tracked.append(sym)
        final.append(sym)

    pending: List[Dict[str, Any]] = []
    for sym in raw_syms:
        st = symbols[sym]
        if sym not in final and not st.get("member"):
            pending.append({
                "symbol": sym,
                "confirmed_days": _safe_int(st.get("ci"), 0),
                "required_days": confirm_days,
            })

    held_by_grace: List[Dict[str, Any]] = []
    for sym in final:
        st = symbols[sym]
        if not st.get("member"):
            st["member"] = True
            st["since"] = today
            st["co"] = 0
        elif not st.get("since"):
            st["since"] = today
        if sym not in raw_set or _safe_int(st.get("co"), 0) > 0:
            held_by_grace.append({
                "symbol": sym,
                "missed_days": _safe_int(st.get("co"), 0),
                "exit_days": _safe_int(knobs.get("exit_days"), STABILITY_DEFAULT_EXIT_DAYS),
                "all_pool_rank": global_rank.get(sym),
            })

    # ---- output rows (stability columns stamped; projection carries them) ---
    exit_days = _safe_int(knobs.get("exit_days"), STABILITY_DEFAULT_EXIT_DAYS)
    out_rows: List[Dict[str, Any]] = []
    for sym in final:
        src = raw_by_sym.get(sym) or sym2row.get(sym)
        if src is None:  # structurally unreachable (final ⊆ sym2row); belt+braces
            continue
        row = dict(src)
        st = symbols[sym]
        hist = st.get("hist") or []
        smoothed = round(sum(hist) / float(len(hist)), 2) if hist else None
        if len(hist) >= 2:
            delta = hist[-1] - hist[0]
            if delta >= STABILITY_TREND_EPS:
                trend = "improving"
            elif delta <= -STABILITY_TREND_EPS:
                trend = "declining"
            else:
                trend = "steady"
        else:
            trend = "n/a"
        since = _s(st.get("since")) or today
        days_in = _stability_days_between(since, today) + 1
        if sym in fast_tracked:
            status = "FAST-TRACK (day 1)"
        elif sym in entered:
            status = "NEW (confirmed %d/%d)" % (confirm_days, confirm_days)
        elif sym not in raw_set or _safe_int(st.get("co"), 0) > 0:
            status = "GRACE (%d/%d missed)" % (_safe_int(st.get("co"), 0), exit_days)
        else:
            status = "ACTIVE (day %d)" % days_in
        row["stability_status"] = status
        row["days_in_list"] = days_in
        row["entry_date"] = since
        row["score_smoothed"] = smoothed
        row["stability_trend"] = trend
        out_rows.append(row)

    # ---- state hygiene: prune stale non-members, cap total size --------------
    pruned = 0
    for sym in list(symbols):
        st = symbols[sym]
        if st.get("member"):
            continue
        ls = _s(st.get("ls"))
        if (not ls) or _stability_days_between(ls, today) > STABILITY_PRUNE_AFTER_DAYS:
            symbols.pop(sym, None)
            pruned += 1
    if len(symbols) > STABILITY_MAX_STATE_SYMBOLS:
        keep_order = sorted(
            symbols.items(),
            key=lambda kv: (not kv[1].get("member"), kv[1].get("ls") or "", kv[0]),
        )
        # members first, then most-recently-seen; drop the tail beyond the cap
        keep_order = keep_order[: len(symbols)]
        overflow = [k for k, _v in sorted(
            ((k, v) for k, v in symbols.items() if not v.get("member")),
            key=lambda kv: (kv[1].get("ls") or "", kv[0]),
        )]
        while len(symbols) > STABILITY_MAX_STATE_SYMBOLS and overflow:
            victim = overflow.pop(0)
            symbols.pop(victim, None)
            pruned += 1

    state["date"] = today
    state["v"] = STABILITY_STATE_VERSION

    stability_meta: Dict[str, Any] = {
        "enabled": True,
        "engine_version": TOP10_SELECTOR_VERSION,
        "date": today,
        "day_advanced": day_advance,
        "knobs": dict(knobs),
        "state": _json_safe(state),
        "audit": {
            "raw_top": list(raw_syms),
            "final_order": list(final),
            "entered": entered,
            "fast_tracked": fast_tracked,
            "exited_hard": exited_hard,
            "exited_soft": exited_soft,
            "exited_capacity": exited_capacity,
            "exited_displaced": exited_displaced,
            "held_by_grace": held_by_grace,
            "pending": pending,
            "state_pruned": pruned,
        },
    }
    return out_rows, stability_meta


def _build_payload(*, status: str, headers: List[str], keys: List[str], rows: List[Dict[str, Any]], meta: Dict[str, Any]) -> Dict[str, Any]:
    include_headers = _coerce_bool(meta.get("include_headers", True), True)
    include_matrix = _coerce_bool(meta.get("include_matrix", True), True)
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


async def _build_top10_rows_async(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    async def _inner() -> Dict[str, Any]:
        started = time.perf_counter()
        headers, keys = _load_schema_defaults()
        # v4.17.0 [FIX V-1]: guarantee gate output columns survive projection.
        headers, keys, gate_keys_appended = _ensure_gate_output_keys(headers, keys)
        criteria = _collect_criteria_from_inputs(*args, **kwargs)
        # v4.21.0: SELECTION STABILITY LAYER — opt-in via `stability_state` /
        # `stability` in the request body. Disengaged (None) => every stability
        # path below is skipped and the build is byte-identical to v4.20.0.
        stability_knobs = _stability_knobs(criteria)
        stability_keys_appended = 0
        if stability_knobs is not None:
            headers, keys, stability_keys_appended = _ensure_stability_output_keys(headers, keys)
        mode = _s(kwargs.get("mode") or criteria.get("mode") or "")
        limit = max(1, _safe_int(criteria.get("limit") or kwargs.get("limit"), 10))
        # v4.19.0 [FIX X-1]: internal soft deadline — collection self-budgets
        # so the outer backstop never has to destroy a viable partial build.
        deadline = time.monotonic() + max(1.0, BUILDER_TOTAL_TIMEOUT_SEC - BUILDER_TOTAL_RESERVE_SEC)

        if criteria.get("schema_only") or criteria.get("headers_only"):
            return _build_payload(
                status="success",
                headers=headers,
                keys=keys,
                rows=[],
                meta={
                    "build_status": "OK",
                    "dispatch": "top10_selector",
                    "selector_version": TOP10_SELECTOR_VERSION,
                    "schema_only": bool(criteria.get("schema_only")),
                    "headers_only": bool(criteria.get("headers_only")),
                    "criteria_used": _json_safe(criteria),
                    "include_headers": criteria.get("include_headers", True),
                    "include_matrix": criteria.get("include_matrix", True),
                    "duration_ms": round((time.perf_counter() - started) * 1000.0, 3),
                },
            )

        engine, engine_source = await _resolve_engine(*args, **kwargs)
        if engine is None:
            return _build_payload(
                status="warn",
                headers=headers,
                keys=keys,
                rows=[],
                meta={
                    "build_status": "DEGRADED",
                    "dispatch": "top10_selector",
                    "selector_version": TOP10_SELECTOR_VERSION,
                    "warning": "engine_unavailable",
                    "criteria_used": _json_safe(criteria),
                    "include_headers": criteria.get("include_headers", True),
                    "include_matrix": criteria.get("include_matrix", True),
                    "engine_source": engine_source,
                    "duration_ms": round((time.perf_counter() - started) * 1000.0, 3),
                },
            )

        try:
            candidates, collect_meta = await _collect_candidate_rows(engine, criteria, mode, deadline)
        except Exception as exc:
            logger.warning("Top10 candidate collection failed: %s", exc, exc_info=True)
            return _build_payload(
                status="warn",
                headers=headers,
                keys=keys,
                rows=[],
                meta={
                    "build_status": "DEGRADED",
                    "dispatch": "top10_selector",
                    "selector_version": TOP10_SELECTOR_VERSION,
                    "warning": f"candidate_collection_failed:{type(exc).__name__}",
                    "criteria_used": _json_safe(criteria),
                    "include_headers": criteria.get("include_headers", True),
                    "include_matrix": criteria.get("include_matrix", True),
                    "engine_source": engine_source,
                    "duration_ms": round((time.perf_counter() - started) * 1000.0, 3),
                },
            )

        normalized_candidates = [_normalize_candidate_row(r) for r in candidates]

        # ---------------------------------------------------------------------
        # v4.16.0 Phase D — TOP10 ADMISSION FILTER (env-gated, default ON).
        # ---------------------------------------------------------------------
        # Rows failing the admission check can neither be SELECTED nor
        # BACKFILLED. The four-pool fill below operates on the admissible
        # pool only, so a price-less / zero-confidence / forced-HOLD row can
        # never reach the sheet while the filter is on. Set
        # TFB_TOP10_ADMISSION_FILTER=0 to restore pre-v4.16.0 behavior.
        admission_excluded: List[Dict[str, str]] = []
        if TOP10_ADMISSION_FILTER_ENABLED:
            admissible_candidates: List[Dict[str, Any]] = []
            for r in normalized_candidates:
                admissible, why = _admission_check(r)
                if admissible:
                    admissible_candidates.append(r)
                else:
                    admission_excluded.append({
                        "symbol": _s(r.get("symbol") or r.get("ticker")),
                        "reason": why,
                    })
            if admission_excluded:
                logger.info(
                    "[v4.16.0 ADMIT] excluded=%d %s",
                    len(admission_excluded),
                    _json_compact(admission_excluded[:10]),
                )
        else:
            admissible_candidates = list(normalized_candidates)

        # ---------------------------------------------------------------------
        # v4.18.0 [FIX W-3] — CANDIDATE-WIDE FRESH GATE (pre-tiering).
        # ---------------------------------------------------------------------
        # The engine investability gate runs on ALL admissible candidates
        # BEFORE the tier split and SELL-class exclusion, so both decisions
        # rank on fresh verdicts (v4.17.0 gated only the 10 FINAL rows —
        # after selection). Pure-CPU, idempotent, AA-tag-aware (v5.84.0+).
        final_gate_applied, final_gate_errors, final_gate_available = _apply_final_gate(admissible_candidates)

        # ---------------------------------------------------------------------
        # v4.18.0 [FIX W-2a] — SELL-CLASS EXCLUSION (env-gated, default ON).
        # ---------------------------------------------------------------------
        # A "best investments" surface must never carry a name the engine
        # says to exit or avoid — not as a pick, not as backfill. If this
        # empties the pool (all-SELL universe), the payload honestly
        # degrades toward zero rows; see `all_candidates_sell_class`.
        sell_class_excluded: List[Dict[str, str]] = []
        if TOP10_EXCLUDE_SELL_CLASS_ENABLED:
            selectable: List[Dict[str, Any]] = []
            for r in admissible_candidates:
                if _is_sell_class(r):
                    sell_class_excluded.append({
                        "symbol": _s(r.get("symbol") or r.get("ticker")),
                        "recommendation": _s(r.get("recommendation")),
                    })
                else:
                    selectable.append(r)
            if sell_class_excluded:
                logger.info(
                    "[v4.18.0 SELLX] excluded=%d %s",
                    len(sell_class_excluded),
                    _json_compact(sell_class_excluded[:10]),
                )
        else:
            selectable = list(admissible_candidates)

        _v310_active: Dict[str, Any] = {}
        mcs = _safe_float(
            criteria.get("min_conviction_score") or criteria.get("min_conviction"), None
        )
        if mcs is not None and mcs > 0.0:
            _v310_active["min_conviction_score"] = mcs
        if _coerce_bool(criteria.get("exclude_engine_dropped_valuation"), False):
            _v310_active["exclude_engine_dropped_valuation"] = True
        if _coerce_bool(criteria.get("exclude_forecast_unavailable"), False):
            _v310_active["exclude_forecast_unavailable"] = True
        if _coerce_bool(criteria.get("exclude_provider_errors"), False):
            _v310_active["exclude_provider_errors"] = True

        # v4.14.0 Phase K — parallel v8tier filter audit block. Records
        # which v4.14.0 filters fired so operators get a clean audit trail
        # for the 8-tier opt-in exclusions (matching the v3.1.0 pattern).
        _v8tier_active: Dict[str, Any] = {}
        if _coerce_bool(criteria.get("exclude_avoid_recommendations"), False):
            _v8tier_active["exclude_avoid_recommendations"] = True
        mpb_canon_active = _normalize_priority_band(criteria.get("min_priority_band"))
        if mpb_canon_active:
            _v8tier_active["min_priority_band"] = mpb_canon_active
        if _coerce_bool(criteria.get("reco_8tier_strict"), False):
            _v8tier_active["reco_8tier_strict"] = True

        # ---------------------------------------------------------------------
        # v4.18.0 [FIX W-2b / W-2c] — TIER SPLIT + FOUR-POOL FILL ORDER.
        # ---------------------------------------------------------------------
        # Pools, each sorted best-first by composite + the v4.17.0 tiebreak
        # tuple, filled strictly in this order:
        #   1. Tier-1 + criteria pass   ("T1" — the real picks)
        #   2. Tier-2 + criteria pass   (labeled backfill)
        #   3. Tier-1 + criteria fail   (labeled backfill; operator intent
        #                                outranks tier, hence below pool 2)
        #   4. Tier-2 + criteria fail   (labeled backfill, last resort)
        # This subsumes the v4.13.0 direct-symbol top-up and the v4.15.0
        # limit-fill backfill: pools 2-4 exhaust the entire admissible set,
        # and direct-symbol rows still float to the top of their pools via
        # the +140 priority bump in the score.
        horizon_days = _safe_int(criteria.get("horizon_days") or criteria.get("invest_period_days"), 90)

        def _pool_sort_key(item: Tuple[float, Dict[str, Any]]):
            score, row = item
            return (
                score,
                # v4.14.0 Phase I — priority band tiebreaker. NEGATED so that
                # under reverse=True the smaller rank (P1 = best / most
                # urgent) sorts first; blank bands rank 99 and sort last.
                -_priority_band_rank(row.get("recommendation_priority_band")),
                _choose_horizon_roi(row, horizon_days) or 0.0,
                _safe_float(row.get("opportunity_score"), 0.0) or 0.0,
                _safe_float(row.get("overall_score"), 0.0) or 0.0,
                _safe_float(row.get("forecast_confidence"), 0.0) or 0.0,
                -(_safe_float(row.get("risk_score"), 999.0) or 999.0),
                _safe_float(row.get("liquidity_score"), 0.0) or 0.0,
                _row_richness(row),
            )

        t1_pass: List[Tuple[float, Dict[str, Any]]] = []
        t2_pass: List[Tuple[float, Dict[str, Any]]] = []
        t1_fail: List[Tuple[float, Dict[str, Any]]] = []
        t2_fail: List[Tuple[float, Dict[str, Any]]] = []
        drop_counts: Dict[str, int] = {}
        tier1_candidates = 0
        tier2_candidates = 0
        for r in selectable:
            passed, reason = _passes_filters_with_reason(r, criteria)
            tier = _tier_of_row(r)
            if tier == 1:
                tier1_candidates += 1
            else:
                tier2_candidates += 1
            rr = dict(r)
            rr["_tier"] = tier
            rr["_criteria_fail"] = "" if passed else (reason or "criteria")
            entry = (_selector_score(rr, criteria), rr)
            if passed:
                (t1_pass if tier == 1 else t2_pass).append(entry)
            else:
                drop_key = reason or "criteria"
                drop_counts[drop_key] = drop_counts.get(drop_key, 0) + 1
                (t1_fail if tier == 1 else t2_fail).append(entry)

        for _pool in (t1_pass, t2_pass, t1_fail, t2_fail):
            _pool.sort(key=_pool_sort_key, reverse=True)

        filtered_count = len(t1_pass) + len(t2_pass)
        filter_relaxed = bool(selectable) and filtered_count == 0

        top_rows, fill_meta = _fill_top10_selection((t1_pass, t2_pass, t1_fail, t2_fail), limit)

        # v4.21.0: membership hysteresis over today's memoryless fill result.
        # Grace-held incumbents re-enter from the pool row map (they still
        # carry _tier/_criteria_fail, so the W-6 label pass below is intact);
        # the returned order (smoothed, incumbency-first) becomes top10_rank.
        stability_meta: Optional[Dict[str, Any]] = None
        if stability_knobs is not None:
            top_rows, stability_meta = _apply_selection_stability(
                raw_selected=top_rows,
                pools=(t1_pass, t2_pass, t1_fail, t2_fail),
                criteria=criteria,
                knobs=stability_knobs,
                limit=limit,
            )

        # v4.18.0 [FIX W-6] — assemble tier / backfill labels. Internal keys
        # (_tier / _criteria_fail / _tier_label) never reach the sheet:
        # projection copies schema-contract keys only.
        backfill_symbols: List[str] = []
        tier2_symbols: List[str] = []
        for row in top_rows:
            tier = _safe_int(row.get("_tier"), 2)
            fail = _s(row.get("_criteria_fail"))
            sym = _normalize_symbol(row.get("symbol") or row.get("ticker"))
            if tier == 2 and sym:
                tier2_symbols.append(sym)
            if fail and sym:
                backfill_symbols.append(sym)
            if tier == 1 and not fail:
                label = "T1"
            else:
                bits: List[str] = []
                if tier == 2:
                    bits.append(_tier2_reason(row))
                if fail:
                    bits.append(f"criteria: {fail}")
                prefix = "T1 BACKFILL[" if tier == 1 else "T2 BACKFILL["
                label = prefix + "; ".join(bits) + "]"
            if row.pop("_sector_cap_relaxed", False):
                label += " [SECTOR-CAP RELAXED]"
            row["_tier_label"] = label

        tier1_selected = sum(1 for r in top_rows if _safe_int(r.get("_tier"), 2) == 1)
        tier2_selected = len(top_rows) - tier1_selected

        projected_rows = _rank_and_project_rows(top_rows[:limit], keys, criteria)

        dq_engine_drop = 0
        dq_forecast_unavail = 0
        dq_provider_err = 0
        # v4.14.0 Phase K — 8-tier vocabulary counters for the
        # data_quality_summary block. Aggregated over the FINAL projected
        # rows so operators see what the Top10 surface actually contains.
        dq_avoid_count = 0
        dq_accumulate_count = 0
        dq_strong_sell_count = 0
        dq_reco_8tier_seen = 0
        dq_priority_band_seen = 0
        for _proj_row in projected_rows:
            _tags = _parse_warnings_tags(_proj_row)
            if _has_engine_drop_tag(_tags):
                dq_engine_drop += 1
            if _has_forecast_unavail_tag(_tags, _proj_row):
                dq_forecast_unavail += 1
            if _has_provider_error(_proj_row):
                dq_provider_err += 1
            _reco_canon = _normalize_reco_token(_proj_row.get("recommendation"))
            if _reco_canon:
                dq_reco_8tier_seen += 1
                if _reco_canon == "AVOID":
                    dq_avoid_count += 1
                elif _reco_canon == "ACCUMULATE":
                    dq_accumulate_count += 1
                elif _reco_canon == "STRONG_SELL":
                    dq_strong_sell_count += 1
            if _normalize_priority_band(_proj_row.get("recommendation_priority_band")):
                dq_priority_band_seen += 1

        status = "success" if projected_rows else "warn"
        meta = {
            "build_status": "OK" if projected_rows else "WARN",
            "dispatch": "top10_selector",
            "selector_version": TOP10_SELECTOR_VERSION,
            "criteria_used": _json_safe(criteria),
            "candidate_count": len(normalized_candidates),
            "filtered_count": filtered_count,
            "selected_count": len(projected_rows),
            "filter_relaxed": filter_relaxed,
            # v4.15.0 Phase C meta keys preserved; v4.18.0 meaning:
            # criteria-failing rows admitted with explicit labels (pools 3/4).
            "backfilled_count": len(backfill_symbols),
            "backfill_symbols": list(backfill_symbols),
            # v4.16.0 Phase D — admission filter audit (Fix U).
            "admission_filter_enabled": TOP10_ADMISSION_FILTER_ENABLED,
            "final_gate_enabled": TOP10_FINAL_GATE_ENABLED,
            "final_gate_available": final_gate_available,
            "final_gate_applied": final_gate_applied,
            "final_gate_errors": final_gate_errors,
            # v4.18.0 [FIX W-3]: gate now runs on ALL admissible candidates
            # BEFORE tiering (was: final selected rows only).
            "final_gate_scope": "admissible_candidates",
            "gate_keys_appended": gate_keys_appended,
            "projection_keys_count": len(keys),
            "admission_excluded_count": len(admission_excluded),
            "admission_excluded": [dict(x) for x in admission_excluded[:25]],
            # v4.18.0 [FIX W] audit surface.
            "golden_composite_enabled": TOP10_GOLDEN_COMPOSITE_ENABLED,
            "composite_weights": dict(GOLDEN_COMPOSITE_WEIGHTS) if TOP10_GOLDEN_COMPOSITE_ENABLED else "legacy",
            "tier1_reliability_floor": TOP10_TIER1_RELIABILITY_FLOOR,
            "tier1_candidates": tier1_candidates,
            "tier2_candidates": tier2_candidates,
            "tier1_selected": tier1_selected,
            "tier2_selected": tier2_selected,
            "tier2_selected_symbols": list(tier2_symbols),
            "exclude_sell_class_enabled": TOP10_EXCLUDE_SELL_CLASS_ENABLED,
            "sell_class_excluded_count": len(sell_class_excluded),
            "sell_class_excluded": [dict(x) for x in sell_class_excluded[:25]],
            "sector_cap": fill_meta.get("sector_cap"),
            "sector_cap_deferred": fill_meta.get("sector_cap_deferred"),
            "sector_cap_relaxed": fill_meta.get("sector_cap_relaxed"),
            "sector_normalize": fill_meta.get("sector_normalize"),
            # v4.16.0 Phase A — provenance of the FINAL projected rows
            # (3=live/hydrated, 2=emergency, 1=snapshot, 0=output-fallback).
            "selected_provenance_counts": {
                str(rank): sum(1 for r in top_rows[:limit] if _row_prov(r) == rank)
                for rank in (_PROV_LIVE, _PROV_EMERGENCY, _PROV_SNAPSHOT, _PROV_OUTPUT_FALLBACK)
            },
            "selected_symbols": [_s(r.get("symbol")) for r in projected_rows if _s(r.get("symbol"))],
            # v4.19.0 [FIX X-5]: symbol -> donating source_page, auditable.
            "selected_sources": {
                _s(r.get("symbol")): _s(r.get("source_page"))
                for r in top_rows[:limit]
                if _s(r.get("symbol"))
            },
            "selected_direct_symbols": [
                _s(r.get("symbol"))
                for r in projected_rows
                if _direct_symbol_order_index(r, criteria) is not None and _s(r.get("symbol"))
            ],
            "include_headers": criteria.get("include_headers", True),
            "include_matrix": criteria.get("include_matrix", True),
            "engine_source": engine_source,
            # v4.19.0 [FIX X-1/X-2]: effective timeout-configuration echo.
            "timeouts": {
                "engine_call_sec": ENGINE_CALL_TIMEOUT_SEC,
                "page_sec": PAGE_TOTAL_TIMEOUT_SEC,
                "total_sec": BUILDER_TOTAL_TIMEOUT_SEC,
                "total_reserve_sec": BUILDER_TOTAL_RESERVE_SEC,
                "outer_grace_sec": BUILDER_OUTER_GRACE_SEC,
            },
            "duration_ms": round((time.perf_counter() - started) * 1000.0, 3),
            "filter_drop_counts": dict(drop_counts),
            "applied_v310_filters": dict(_v310_active),
            # v4.14.0 Phase K — parallel 8-tier filter audit block.
            "applied_v8tier_filters": dict(_v8tier_active),
            # v4.14.0 Phase K — capability flag. Downstream consumers
            # (insights_builder v8.0.0, advisor route layer) can
            # content-check this to confirm the selector is aligned
            # with the v8.0.0 vocabulary.
            "reco_8tier_aware": True,
            "reco_8tier_aware_version": RECO_8TIER_AWARE_VERSION,
            "data_quality_summary": {
                "engine_dropped_valuation": dq_engine_drop,
                "forecast_unavailable": dq_forecast_unavail,
                "provider_errors": dq_provider_err,
                # v4.14.0 Phase K — 8-tier vocabulary counters.
                "avoid_count": dq_avoid_count,
                "accumulate_count": dq_accumulate_count,
                "strong_sell_count": dq_strong_sell_count,
                "reco_8tier_seen": dq_reco_8tier_seen,
                "priority_band_seen": dq_priority_band_seen,
                "total_projected": len(projected_rows),
            },
            **collect_meta,
        }
        if not projected_rows:
            meta["warning"] = "no_top10_rows_after_filtering"
            if TOP10_EXCLUDE_SELL_CLASS_ENABLED and sell_class_excluded and not selectable:
                # All admissible candidates were SELL-class: degrading to
                # zero rows is deliberate (W-2a) — honest over harmful.
                meta["warning"] = "all_candidates_sell_class"

        # v4.21.0: stability audit surface — engaged builds only, so the
        # disengaged meta dict stays byte-identical to v4.20.0.
        if stability_meta is not None:
            meta["stability"] = stability_meta
            meta["stability_keys_appended"] = stability_keys_appended

        return _build_payload(status=status, headers=headers, keys=keys, rows=projected_rows, meta=meta)

    try:
        # v4.19.0 [FIX X-1]: the hard wall is now a BACKSTOP behind the
        # graceful internal deadline (TOTAL - reserve). Reaching it means
        # the graceful path itself wedged (e.g. a non-cancellable engine
        # call); the fallback below is the last resort, not the design.
        return await asyncio.wait_for(
            _inner(), timeout=BUILDER_TOTAL_TIMEOUT_SEC + BUILDER_OUTER_GRACE_SEC
        )
    except asyncio.TimeoutError:
        headers, keys = _load_schema_defaults()
        criteria = _collect_criteria_from_inputs(*args, **kwargs)
        return _build_payload(
            status="warn",
            headers=headers,
            keys=keys,
            rows=[],
            meta={
                "build_status": "DEGRADED",
                "dispatch": "top10_selector",
                "selector_version": TOP10_SELECTOR_VERSION,
                "warning": "builder_total_timeout",
                # v4.19.0 [FIX X-1]: even the last-resort payload carries the
                # timeout configuration for diagnosis.
                "timeouts": {
                    "engine_call_sec": ENGINE_CALL_TIMEOUT_SEC,
                    "page_sec": PAGE_TOTAL_TIMEOUT_SEC,
                    "total_sec": BUILDER_TOTAL_TIMEOUT_SEC,
                    "total_reserve_sec": BUILDER_TOTAL_RESERVE_SEC,
                    "outer_grace_sec": BUILDER_OUTER_GRACE_SEC,
                },
                "criteria_used": _json_safe(criteria),
                "include_headers": criteria.get("include_headers", True),
                "include_matrix": criteria.get("include_matrix", True),
            },
        )


def build_top10_rows(*args: Any, **kwargs: Any) -> Any:
    coro = _build_top10_rows_async(*args, **kwargs)
    try:
        asyncio.get_running_loop()
        return coro
    except RuntimeError:
        return asyncio.run(coro)


def build_top10_output_rows(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def build_top10_investments_rows(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def build_top_10_investments_rows(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def build_top10(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def build_top10_investments(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def build_top10_output(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def build_top10_payload(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def get_top10_rows(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def select_top10(*args: Any, **kwargs: Any) -> Any:
    return build_top10_rows(*args, **kwargs)


def select_top10_symbols(*args: Any, **kwargs: Any) -> Any:
    async def _inner() -> List[str]:
        payload = await _build_top10_rows_async(*args, **kwargs)
        rows = payload.get("rows") if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            return []
        out: List[str] = []
        for row in rows:
            if isinstance(row, dict):
                sym = _normalize_symbol(row.get("symbol") or row.get("ticker"))
                if sym:
                    out.append(sym)
        return _dedupe_keep_order(out)

    try:
        asyncio.get_running_loop()
        return _inner()
    except RuntimeError:
        return asyncio.run(_inner())


__all__ = [
    "TOP10_SELECTOR_VERSION",
    "__version__",
    # v4.14.0 Phase B/K: 8-tier capability marker.
    "RECO_8TIER_AWARE_VERSION",
    "DEFAULT_FALLBACK_KEYS",
    "DEFAULT_FALLBACK_HEADERS",
    "ROW_KEY_ALIASES",
    "TOP10_REQUIRED_FIELDS",
    "TOP10_REQUIRED_HEADERS",
    "TOP10_PENALTY_ENGINE_DROP",
    "TOP10_PENALTY_FORECAST_UNAVAIL",
    "TOP10_PENALTY_PROVIDER_ERROR",
    # v4.16.0 Fix U: vintage protection + admission filter surface.
    "GATE_CRITICAL_KEYS",
    "TOP10_ADMISSION_FILTER_ENABLED",
    "TOP10_ADMISSION_FILTER_ENV",
    "TOP10_FINAL_GATE_ENV",
    "TOP10_FINAL_GATE_ENABLED",
    "GATE_OUTPUT_KEY_HEADERS",
    # v4.21.0: selection-stability layer surface (opt-in hysteresis).
    "STABILITY_OUTPUT_KEY_HEADERS",
    "STABILITY_STATE_VERSION",
    "STABILITY_DEFAULT_CONFIRM_DAYS",
    "STABILITY_DEFAULT_EXIT_DAYS",
    "STABILITY_DEFAULT_RANK_BUFFER",
    "STABILITY_DEFAULT_SMOOTH_DAYS",
    # v4.18.0 Fix W: golden composite + tier discipline surface.
    "SELL_CLASS_RECOS",
    "TOP10_EXCLUDE_SELL_CLASS_ENABLED",
    "TOP10_EXCLUDE_SELL_CLASS_ENV",
    "TOP10_TIER1_RELIABILITY_FLOOR",
    "TOP10_SECTOR_CAP",
    "TOP10_SECTOR_NORMALIZE",
    "TOP10_GOLDEN_COMPOSITE_ENABLED",
    "TOP10_GOLDEN_COMPOSITE_ENV",
    "GOLDEN_COMPOSITE_WEIGHTS",
    "TOP10_METADATA_STRIP_KEYS",
    # v4.19.0 Fix X: budget / timeout surface.
    "ENGINE_CALL_TIMEOUT_SEC",
    "PAGE_TOTAL_TIMEOUT_SEC",
    "BUILDER_TOTAL_TIMEOUT_SEC",
    "BUILDER_TOTAL_RESERVE_SEC",
    "BUILDER_OUTER_GRACE_SEC",
    # v4.14.0 Phase C: 8-tier vocabulary surface (content-checkable
    # constants for ops + downstream tooling).
    "_RECO_8TIER_CANONICAL",
    "_BULLISH_RECOS",
    "_BEARISH_RECOS",
    "_RECO_8TIER_NEW_TOKENS",
    "_RECO_TIEBREAK_BUMPS",
    "build_top10_rows",
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
]
