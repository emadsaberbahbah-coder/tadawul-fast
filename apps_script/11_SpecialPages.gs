/**
 * 11_SpecialPages.gs
 * Tadawul Tracker — Special Pages Module
 * Version: 1.8.6
 *
 * v1.8.6 (2026-08-11) — COCKPIT GUARD FAILS CLOSED (05_Refresh v1.18.1 parity)
 * --------------------------------------------------------------------------
 * Same hardening as 05_Refresh v1.18.1, same day, same doctrine: the v1.8.4
 * guard _specialIsDecisionOwnedPage_ delegated to 00_Config v1.12.6 and
 * returned FALSE when that authority was absent or threw — a mandatory
 * cockpit interlock failing OPEN. The fallback now protects
 * 'Top_10_Investments' by local case-insensitive constant. Zero behavior
 * change while 00_Config is present (the live state). Version constant and
 * its self-check literal bumped in lockstep. ES5. ZERO functions removed.
 * --------------------------------------------------------------------------
 * Aligned with:
 * - 00_Config.gs v1.12.6  (REQUIRED for isDecisionOwnedPage_ — the v1.8.4 guards)
 * - 01_Menu.gs   v2.10.0
 * - 02_Core.gs   v2.8.0
 * - 03_Schema.gs v2.7.0
 * - 04_Format.gs v2.14.0
 * - 05_Refresh.gs v1.12.0  (REQUIRED for the Top_10 empty-symbol default)
 * - 07_Global_Markets.gs v1.7.1
 * - 12_Diagnostics.gs v1.7.0
 * - 99_Diag.gs v2.4.0
 * - 16_Decision_Top10.gs v1.0.0  (renders the Top_10_Investments cockpit)
 * - Backend: routes/advanced_analysis v4.6.0, core/analysis/top10_selector
 *   v4.18.0 (Fix W: golden composite, tier discipline, SELL-class exclusion,
 *   rank force-overwrite, sector cap), insights_builder v8.2.0,
 *   data_engine_v2 v5.85.3
 * - Main investment pages: 115 columns
 * - Top_10_Investments: 118 columns
 *
 * v1.8.5 DEAD-CODE REMOVAL (closes the v1.8.2 item #4 grep gate)
 * ---------------------------------------------------------------------------
 * The v1.8.2 header block (item #4) RETAINED four recommendation-derivation
 * helpers as dead-code-within-this-file, gating their removal on a
 * project-wide grep because 99_Diag.gs / 12_Diagnostics.gs had not yet been
 * audited and might reference them. That grep is now complete:
 *   - 12_Diagnostics.gs v1.7.0 — its recommendation/accuracy path delegates to
 *     diagnoseRecommendationAccuracy_ (this file), which uses ONLY the three
 *     KEEPERS below; it never calls the four removed helpers.
 *   - 99_Diag.gs v2.4.0 — reads recommendations off the sheet/response and
 *     counts field coverage; it derives nothing and references none of the four.
 *   - Every other recommendation/accuracy diagnostic in the project routes
 *     through this file's keepers.
 * With no in-file and no external callers, the four helpers are removed:
 *     _deriveCanonicalRecommendation_
 *     _normalizeConfidenceToPercent_
 *     _normalizeRoiToFraction_
 *     _coerceNumericScore_
 * RETAINED (live callers in applyViewTokensAndRecommendationOverride_ and
 * diagnoseRecommendationAccuracy_):
 *     _normalizeRecommendationForCompare_
 *     _canonicalizeRecommendationLabel_
 *     _extractReasonPrefixRecommendation_
 * Only SPECIAL_PAGES_VERSION_, the documented [v1.8.5 SPECIAL-PAGES] verify
 * tag, and the recommendation-mirror section comment change. Internal logger
 * tags ([v1.4.0 RECO-GATE], [v1.4.0 RECO-ACCURACY]) stay stable per the Tag
 * Stability convention. Every other byte is carried forward verbatim from
 * v1.8.4.
 *
 * v1.8.4 TOP10 DECISION-WIRE (lockstep with 16_Decision_Top10.gs v1.0.0)
 * ---------------------------------------------------------------------------
 * CONTEXT Top_10_Investments is now a DECISION-OWNED cockpit page: row 1 is a
 * merged title banner and rows 2+ hold the control panel, KPI strip, and
 * decision sections, all rendered by 16_Decision_Top10.gs from the backend
 * opportunity endpoint. 00_Config.gs v1.12.6 is the ownership authority
 * (isDecisionOwnedPage_), and 05_Refresh.gs v1.12.0 / 03_Schema.gs v2.7.0
 * already guard the shared engine cores (refreshPageCore_/buildPage_/
 * validatePage_/applyHeadersFromSchema_/merge). This file, however, still had
 * DIRECT local write paths into the page that bypass those cores:
 *   - sortTop10Investments_           sorts rows 2..end -> scrambles the
 *                                     panel/KPI/section layout
 *   - enforceTop10StrictRows_         clearContent + rewrite of all data rows
 *                                     (manual/legacy tool, directly callable)
 *   - applyViewTokensAndRecommendationOverride_(Top_10)
 *                                     writes view/recommendation columns when
 *                                     matching headers are found
 *   - _trimTop10ResidualRows_         clearContent below the write window
 * and noisy read paths (validateTop10Investments_ / diagnoseTop10Investments_)
 * that judge the cockpit's title row against the 118-column contract and spam
 * false WARN statuses.
 *
 * CHANGES (guards only — every guarded function's body is carried VERBATIM):
 *   1. Two helpers: _specialIsDecisionOwnedPage_(pageName) (typeof-guarded on
 *      00_Config v1.12.6 isDecisionOwnedPage_; returns false when Config is
 *      older, so legacy behavior is fully preserved) and
 *      _specialSkipDecisionOwned_(actionName, pageName, level) returning
 *      { ok:true, skipped:true, action:'skipped_decision_owned', message }.
 *   2. Skip-not-throw guards at the top of: buildTop10Investments_,
 *      refreshTop10Investments_, sortTop10Investments_,
 *      enforceTop10StrictRows_, _trimTop10ResidualRows_ (all WARN-logged
 *      write paths, guarded BEFORE any lock/sheet access), and inside
 *      applyViewTokensAndRecommendationOverride_ for any decision-owned
 *      pageName (covers applyViewTokensTop10_ / applyViewTokensForCurrentPage_
 *      and 07_Global_Markets' generic caller — its
 *      `viewResult && !viewResult.skipped` check is shape-compatible).
 *   3. validateTop10Investments_ / diagnoseTop10Investments_ skip with an
 *      INFO log and NO writePageStatus_ call — no false WARN status spam
 *      against the cockpit. Insights_Analysis and Data_Dictionary remain
 *      engine-owned: their functions are untouched.
 *   4. verifySpecialPagesVersion reports top10DecisionWired and
 *      decisionGuardAvailable. Internal logger tags ([v1.4.0 RECO-GATE],
 *      [v1.4.0 RECO-ACCURACY]) stay stable per the Tag Stability convention.
 *
 * Everything else is carried forward verbatim from v1.8.3.
 *
 * v1.8.3 TOP10 RESIDUAL-ROW PURGE (lockstep with selector v4.18.0 Fix W)
 * ---------------------------------------------------------------------------
 * ROOT CAUSE the live sheet (2026-06-11) showed 17 Top_10 rows: the fresh
 * 10-row v4.18.0 payload interleaved with ghost rows from June 9-10 runs
 * (old-format Selection Reasons, stale timestamps, colliding ranks). The
 * backend selector guarantees contiguous ranks 1..N WITHIN a payload (W-4),
 * but the sheet writer updates the write window only — rows BELOW the new
 * window survive every refresh, so the sheet accumulates a UNION of runs.
 *   FIX: _trimTop10ResidualRows_ runs after every Top_10 refresh (before the
 *   view-token override and validation, so both see the final state). It
 *   clears all data rows below the freshly written payload — rows
 *   (rowCount + 2) .. lastRow across the schema width. The backend payload
 *   is authoritative for row COUNT as well as membership, order, and
 *   Selection Reason. The trim is skipped (with a refresh warning) when the
 *   refresh result does not report a usable numeric rowCount, so it can
 *   never guess.
 *
 * DEPLOYMENT NOTE (unchanged v1.8.2 dependency, now with live evidence): the
 * 2026-06-11 08:10 UTC run STILL carried direct_symbols
 * ["GOOGL","1120.SR","VOO"] in its Criteria Snapshot. The v1.8.2 symbol
 * OMISSION below only takes effect when 05_Refresh.gs v1.11.4
 * resolveRefreshSymbols_ sends EMPTY symbol lists for Top_10; older 05
 * versions re-inject default/working symbols DOWNSTREAM of this file, and
 * the selector's +140 direct-symbol priority bump (correct for explicit
 * user requests) then pins those names to the top of the page. Verify the
 * deployed 05_Refresh.gs version before attributing ordering issues to the
 * backend.
 *
 * Everything else is carried forward verbatim from v1.8.2.
 *
 * v1.8.2 TOP10 BACKEND-AUTHORITY REALIGNMENT (lockstep with selector v4.17.0)
 * ---------------------------------------------------------------------------
 * ROOT CAUSE two v1.8.x behaviors predate the live backend selector and now
 * actively conflict with it. The deployed backend (selector v4.17.0 Fix U/V,
 * verified on the wire 2026-06-10) returns the authoritative Top_10: ranks
 * 1..10 in selector composite-score order, fresh gate verdicts on every row,
 * and a real Selection Reason per row. The GAS layer must not second-guess it.
 *
 *   1. refreshTop10Investments_ injected getDefaultSymbols(Top_10) — Config's
 *      hardcoded default symbol list — as EXPLICIT payload overrides
 *      (symbols/tickers/direct_symbols). Explicit overrides win through every
 *      layer (00_Config buildBasePayload, 05_Refresh resolveRefreshSymbols_),
 *      so the primary menu refresh path PINNED the selector's candidate pool
 *      to that fixed list, while the verified-correct caller sends EMPTY
 *      symbol lists (live criteria snapshots show direct_symbols: []).
 *      FIX: symbol keys are now omitted from the payload unless the caller
 *      explicitly passes options.symbols. With 05_Refresh.gs v1.11.4,
 *      resolveRefreshSymbols_ then sends empty lists for Top_10 — matching
 *      the verified caller. Deploy 05 v1.11.4 together with this file.
 *
 *   2. The v1.8.0 rank-and-backfill pass (enforceTop10StrictRows_) ran by
 *      default after every Top_10 refresh. It was built when the backend
 *      selector was a placeholder. Against the live selector it:
 *        - re-sorts all rows by Opportunity Score, SCRAMBLING the selector's
 *          authoritative composite-score order;
 *        - reassigns Top 10 Rank 1..N in that wrong order;
 *        - stamps every below-strict-bar row (under the live conservative
 *          gate: ALL of them) with a "Backfill (below strict bar: ...)"
 *          marker prepended to the selector's real Selection Reason.
 *      FIX: enforcement is now OPT-IN. refreshTop10Investments_ runs it only
 *      when options.enforceStrictTop10 === true (default was !== false).
 *      enforceTop10StrictRows_ itself is retained VERBATIM as a manual /
 *      legacy tool (direct call or explicit option), including its script
 *      properties (TFB_TOP10_TARGET_COUNT / TFB_TOP10_BACKFILL).
 *
 *   3. validateTop10StrictRows_ all-backfill warning reworded: with the
 *      enforcement pass off, sheet rows are the selector's honest picks, not
 *      "backfill". The warning now states the rows are below the strict bar,
 *      which is EXPECTED under the engine's conservative gate (Fix T), and
 *      points the operator at Block Reason. Result-shape fields
 *      (backfillRows etc.) are unchanged for caller compatibility.
 *
 *   4. INTENTIONALLY RETAINED: _deriveCanonicalRecommendation_ and its three
 *      numeric normalizers are dead code WITHIN this file but removal stays
 *      gated on the project-wide grep (99_Diag / 12_Diagnostics not yet
 *      audited). Do not remove until that grep confirms no external callers.
 *      (v1.8.5: grep complete — 12_Diagnostics v1.7.0 / 99_Diag v2.4.0 audited,
 *      no external callers found — the four helpers have now been REMOVED.)
 *
 * Everything else is carried forward verbatim from v1.8.1.
 *
 * v1.8.1 INSIGHTS CONTRACT ALIGNMENT (lockstep with insights_builder v8.2.0)
 * --------------------------------------------------------------------------
 * ROOT CAUSE the Insights_Analysis sheet kept showing the 2026-04-26 local
 * fallback even though the backend route returns real rows
 * (dispatch=insights_builder_real). The live backend (routes.advanced_analysis
 * v4.5.0 dispatching to core.analysis.insights_builder) emits the registry
 * v2.14.0 contract:
 *     Section | Item | Metric | Value | Notes | Source | Sort Order
 *     keys:   section, item, metric, value, notes, source, sort_order
 * But this module's INSIGHTS_ANALYSIS_REQUIRED_HEADERS_ still demanded the
 * PRE-v2.12 shape (Section, Item, Symbol, Metric, Value, Signal,
 * Last Updated (Riyadh)). _ensureInsightsAnalysisHeadersReady_ therefore
 * "repaired" the sheet BACK to the old 7 names on every refresh, and because
 * the new payload keys (notes/source/sort_order) had no home in the old
 * header set, the writer could not place the real rows — so the stale fallback
 * content survived. This release moves the GAS contract to the live backend
 * contract.
 *
 * CHANGES (Insights_Analysis ONLY — every other function carried forward verbatim):
 *   1. INSIGHTS_ANALYSIS_REQUIRED_HEADERS_ ->
 *        ['Section','Item','Metric','Value','Notes','Source','Sort Order']
 *      (was ['Section','Item','Symbol','Metric','Value','Signal','Last Updated (Riyadh)']).
 *   2. sortInsightsAnalysis_ now sorts by 'Sort Order' (numeric, ascending)
 *      with 'Section' then 'Item' as tiebreakers; it no longer references the
 *      removed 'Symbol' column. Falls back to Section/Item ordering when
 *      'Sort Order' is absent.
 *   3. Freshness/staleness: the new contract has no 'Last Updated (Riyadh)'
 *      column, so _assessInsightsFreshness_ self-disables the AGE/STALE branch
 *      (no timestamp source) and relies on FALLBACK-marker text detection,
 *      which remains the authoritative degradation signal. The genuine
 *      Executive Summary rows (Overview/Market Tone/Top Picks/Risk Alerts) do
 *      not contain any INSIGHTS_FALLBACK_MARKERS_ string, so a healthy payload
 *      is correctly reported fresh. No code change was needed in the assessor
 *      beyond tolerating an absent Last Updated column (already guarded).
 *   4. verifySpecialPagesVersion bumped; insightsSymbolHeaderRequired
 *      is now false (Symbol is no longer part of the Insights contract).
 *
 * v1.8.0 TOP10 BACKFILL + INSIGHTS FRESHNESS
 * ------------------------------------------
 * ROOT-CAUSE FIX for the two persistent symptoms:
 *   (A) Top_10_Investments collapsing to a single row.
 *   (B) Insights_Analysis silently serving stale local-fallback content.
 *
 * 1. Top_10_Investments selection changed from HARD-DROP to RANK-AND-BACKFILL.
 *    Previously enforceTop10StrictRows_ kept ONLY rows clearing the full strict
 *    gate (Final Action=INVEST, Investability=INVESTABLE, ROI3M>0, ROI12M>0,
 *    DQ>=min, reliability>=min, no provider/engine conflict, no block reason,
 *    risk<>HIGH). When only one candidate cleared every gate, the page was gutted
 *    to 1 row. It now ranks all non-empty candidates (Opportunity, then Overall,
 *    then Forecast Reliability, then Expected ROI 12M), places strict-qualified
 *    rows first, backfills remaining slots with best near-miss candidates up to a
 *    target count (default 10), leaves each backfilled row's REAL Investability
 *    Status / Final Action untouched, stamps Selection Reason with a
 *    "Backfill (below strict bar)" marker, and assigns Top 10 Rank = 1..N.
 *    Backfill never invents symbols — it retains more of the existing fetched pool
 *    instead of discarding it. Reaching a full 10 still needs the backend selector
 *    to return >=10 candidates.
 *    (v1.8.2: this pass is now OPT-IN — the live backend selector is authoritative.)
 *    Script properties (optional):
 *      TFB_TOP10_TARGET_COUNT  (default 10)
 *      TFB_TOP10_BACKFILL      (default true; set 'false' to restore strict-only)
 *
 * 2. Top_10 validation/diagnostics are now backfill-aware. Below-strict-bar rows
 *    are expected and no longer force a WARN. validateTop10 reports
 *    strictApprovedRows / backfillRows, checks Top 10 Rank contiguity, and only
 *    fails on non-strict rows when backfill is explicitly disabled.
 *
 * 3. Insights_Analysis freshness/fallback detection added. The upstream
 *    /v1/advanced insights payload can degrade to a locally generated fallback
 *    ("Generated locally because upstream insights payload was unavailable").
 *    That arrives as CONTENT, not as a fallback endpoint, so the previous refresh
 *    reported SUCCESS while serving stale rows. refreshInsightsAnalysis_ now scans
 *    written content for fallback markers and checks the freshest
 *    Last Updated (Riyadh); if fallback content is present or the data is older
 *    than the staleness window it writes a FALLBACK / STALE status to _Status and
 *    surfaces it in refresh warnings. diagnoseInsightsAnalysis_ includes the same
 *    assessment. Restoring the payload itself is a backend concern
 *    (insights_builder) — this change makes the degradation VISIBLE.
 *    New script property (optional):
 *      TFB_INSIGHTS_STALE_HOURS (default 36)
 *
 * v1.7.1 INSIGHTS HEADER VALIDATION FIX
 * --------------------------------------
 * 1. validateInsightsAnalysis_ requires the approved Insights_Analysis header
 *    contract (now the v2.14.0 contract per v1.8.1).
 * 2. refreshInsightsAnalysis_ performs a pre-refresh header readiness check and repairs
 *    headers from schema/fallback if the width is right but names are wrong.
 * 3. diagnoseInsightsAnalysis_ flags missing required headers, not only wrong width.
 *
 * v1.7.0 ALIGNMENT UPDATE
 * -----------------------
 * 1. Updates dependency notes to the current 115 / 118 production stack.
 * 2. Keeps the strict Top_10_Investments gate from v1.6.0 (now used to CLASSIFY
 *    strict vs backfill — it no longer hard-drops).
 * 3. Removes direct visible status-panel writes from special-page fallback helpers.
 * 4. Restricts sort / enforcement data ranges to the expected schema width.
 * 5. Adds verifySpecialPagesVersion() public runner for Apps Script dropdown use.
 *
 * Existing entry points are preserved for Menu compatibility.
 */

'use strict';

var SPECIAL_PAGES_VERSION_ = '1.8.6';

/* =========================================================
 * Top-level identifiers
 * ========================================================= */

var INSIGHTS_ANALYSIS_PAGE_ = SHEET_NAMES.INSIGHTS_ANALYSIS;
var TOP_10_INVESTMENTS_PAGE_ = SHEET_NAMES.TOP_10_INVESTMENTS;
var DATA_DICTIONARY_PAGE_ = SHEET_NAMES.DATA_DICTIONARY;

var TOP10_STRICT_DQ_MIN_PROPERTY_ = 'TFB_STRICT_INVEST_DQ_MIN';
var TOP10_STRICT_RELIABILITY_MIN_PROPERTY_ = 'TFB_STRICT_INVEST_RELIABILITY_MIN';
var TOP10_DEFAULT_DQ_MIN_ = 80;
var TOP10_DEFAULT_RELIABILITY_MIN_ = 70;

// v1.8.0 — Top10 selection (rank-and-backfill) settings.
var TOP10_TARGET_COUNT_PROPERTY_ = 'TFB_TOP10_TARGET_COUNT';
var TOP10_BACKFILL_PROPERTY_ = 'TFB_TOP10_BACKFILL';
var TOP10_DEFAULT_TARGET_COUNT_ = 10;

// v1.8.0 — Insights freshness settings.
var INSIGHTS_STALE_HOURS_PROPERTY_ = 'TFB_INSIGHTS_STALE_HOURS';
var INSIGHTS_STALE_MAX_AGE_HOURS_ = 36;
var INSIGHTS_FALLBACK_MARKERS_ = Object.freeze([
  'generated locally',
  'local insights fallback',
  'upstream insights payload',
  'payload was unavailable',
  'fallback mode',
  'fallback signal'
]);

var TOP_10_INVESTMENTS_REQUIRED_BUSINESS_HEADERS_ = Object.freeze([
  'Top 10 Rank',
  'Symbol',
  'Name',
  'Current Price',
  'Recommendation',
  'Recommendation Detail',
  'Recommendation Reason',
  'Data Quality Score',
  'Forecast Reliability Score',
  'Provider/Engine Conflict',
  'Investability Status',
  'Final Action',
  'Block Reason',
  'Selection Reason',
  'Criteria Snapshot',
  'Last Updated (Riyadh)'
]);

var TOP10_STRICT_REQUIRED_HEADERS_ = Object.freeze([
  'Top 10 Rank',
  'Symbol',
  'Current Price',
  'Risk Bucket',
  'Expected ROI 3M',
  'Expected ROI 12M',
  'Data Quality Score',
  'Forecast Reliability Score',
  'Provider/Engine Conflict',
  'Investability Status',
  'Final Action',
  'Block Reason'
]);

var SPECIAL_VALID_RECOMMENDATIONS_ = Object.freeze({
  'BUY': true,
  'STRONG BUY': true,
  'STRONG_BUY': true,
  'ACCUMULATE': true,
  'HOLD': true,
  'NEUTRAL': true,
  'REDUCE': true,
  'AVOID': true,
  'SELL': true,
  'STRONG SELL': true,
  'STRONG_SELL': true
});

var V310_CRITERIA_FIELDS_ = Object.freeze([
  'min_conviction_score',
  'exclude_engine_dropped_valuation',
  'exclude_forecast_unavailable',
  'exclude_provider_errors'
]);

var DATA_QUALITY_ALERT_SECTION_ = 'Data Quality Alerts';

/* v1.8.1: Insights_Analysis contract now matches the live backend
 * (insights_builder v8.2.0 / registry v2.14.0):
 *   Section | Item | Metric | Value | Notes | Source | Sort Order
 * The pre-v2.12 columns (Symbol, Signal, Last Updated (Riyadh)) are removed. */
var INSIGHTS_ANALYSIS_REQUIRED_HEADERS_ = Object.freeze([
  'Section',
  'Item',
  'Metric',
  'Value',
  'Notes',
  'Source',
  'Sort Order'
]);

/* =========================================================
 * v1.8.4 — decision-ownership guard helpers
 * Top_10_Investments is a decision-owned cockpit (16_Decision_Top10.gs);
 * 00_Config.gs v1.12.6 isDecisionOwnedPage_ is the single ownership
 * authority. With an older Config the guards degrade to false and every
 * function below behaves exactly as in v1.8.3 (legacy path preserved).
 * ========================================================= */

function _specialIsDecisionOwnedPage_(pageName) {
  try {
    if (typeof isDecisionOwnedPage_ === 'function') {
      return isDecisionOwnedPage_(pageName) === true;
    }
  } catch (e) {}
  /* v1.8.6: FAIL-CLOSED fallback — if the 00_Config authority is absent or
   * throws, the cockpit page stays protected by a local constant. A
   * mandatory guard never fails open (05_Refresh v1.18.1 parity). */
  return String(pageName || '').toLowerCase() === 'top_10_investments';
}

/*
 * Skip-not-throw result for decision-owned pages. level 'INFO' is used by the
 * read-only validate/diagnose paths (no false WARN spam, and deliberately NO
 * writePageStatus_ call); write paths log WARN. The result shape
 * ({ ok:true, skipped:true, action:'skipped_decision_owned' }) matches the
 * 05_Refresh v1.12.0 / 03_Schema v2.7.0 guard convention, and callers that
 * test `result && !result.skipped` (e.g. 07_Global_Markets' view-token
 * consumer) handle it transparently.
 */
function _specialSkipDecisionOwned_(actionName, pageName, level) {
  var msg = pageName + ' is decision-owned (rendered by 16_Decision_Top10.gs); ' +
    actionName + ' skipped to protect the cockpit layout. Use the TFB Decision menu to refresh it.';
  try {
    logRun_(level === 'INFO' ? 'INFO' : 'WARN', actionName, pageName, msg, {
      status: 'SKIPPED_DECISION_OWNED',
      details: { pageName: pageName, action: 'skipped_decision_owned' }
    });
  } catch (e) {}
  return {
    ok: true,
    skipped: true,
    pageName: pageName,
    action: 'skipped_decision_owned',
    message: msg
  };
}

function verifySpecialPagesVersion() {
  var selectionSettings = _getTop10SelectionSettings_();
  var result = {
    ok: SPECIAL_PAGES_VERSION_ === '1.8.6',
    version: SPECIAL_PAGES_VERSION_,
    expectedVersion: '1.8.5',
    top10ExpectedColumns: 118,
    insightsExpectedColumns: 7,
    dataDictionaryExpectedColumns: 9,
    statusPanelWritesDisabled: true,
    top10BackendAuthoritative: true,
    top10EnforcementOptIn: true,
    top10ResidualTrim: true,
    top10DecisionWired: true,
    decisionGuardAvailable: typeof isDecisionOwnedPage_ === 'function',
    top10Mode: selectionSettings.backfillEnabled ? 'rank-and-backfill' : 'strict-only',
    top10TargetCount: selectionSettings.targetCount,
    top10BackfillEnabled: selectionSettings.backfillEnabled,
    insightsStaleHours: _getInsightsStaleHours_(),
    insightsRequiredHeaders: INSIGHTS_ANALYSIS_REQUIRED_HEADERS_.slice(),
    insightsSymbolHeaderRequired: INSIGHTS_ANALYSIS_REQUIRED_HEADERS_.indexOf('Symbol') >= 0
  };

  try {
    Logger.log('[v1.8.5 SPECIAL-PAGES] verify ok=' + result.ok +
      ' version=' + result.version +
      ' top10BackendAuthoritative=true' +
      ' top10EnforcementOptIn=true' +
      ' top10ResidualTrim=true' +
      ' top10DecisionWired=true' +
      ' decisionGuardAvailable=' + result.decisionGuardAvailable +
      ' top10Mode=' + result.top10Mode +
      ' top10TargetCount=' + result.top10TargetCount +
      ' insightsStaleHours=' + result.insightsStaleHours);
  } catch (e) {}

  return result;
}

function _ensureInsightsAnalysisHeadersReady_() {
  var page = getPageConfig(INSIGHTS_ANALYSIS_PAGE_);
  var sheet = ensureSheetExists_(INSIGHTS_ANALYSIS_PAGE_);
  var expected = Number(page.expectedColumnCount || 0);
  var beforeHeaders = readCurrentSheetHeaders_(sheet);
  var beforeMissing = _specialFindMissingHeaders_(beforeHeaders, INSIGHTS_ANALYSIS_REQUIRED_HEADERS_);
  var beforeWidthOk = expected > 0 ? beforeHeaders.length === expected : true;

  if (beforeWidthOk && beforeMissing.length === 0) {
    return {
      ok: true,
      pageName: INSIGHTS_ANALYSIS_PAGE_,
      action: 'no_repair_needed',
      beforeHeaders: beforeHeaders,
      afterHeaders: beforeHeaders.slice(),
      missingRequiredHeaders: []
    };
  }

  var repairAction = 'none';
  var repairError = '';

  try {
    if (typeof repairInsightsAnalysisHeaders === 'function') {
      repairAction = 'repairInsightsAnalysisHeaders';
      repairInsightsAnalysisHeaders();
    } else if (typeof getFallbackHeaders === 'function' && typeof writeHeaderRowExact_ === 'function') {
      var fallbackHeaders = getFallbackHeaders(INSIGHTS_ANALYSIS_PAGE_);
      if (fallbackHeaders && fallbackHeaders.length === expected) {
        repairAction = 'write_config_fallback_headers';
        writeHeaderRowExact_(sheet, fallbackHeaders);
        if (typeof applyHeaderSheetSetup_ === 'function') {
          try { applyHeaderSheetSetup_(sheet, page); } catch (setupErr) {}
        }
      } else if (typeof applyHeadersFromSchema_ === 'function') {
        repairAction = 'applyHeadersFromSchema_force';
        applyHeadersFromSchema_(INSIGHTS_ANALYSIS_PAGE_, { force: true });
      }
    } else if (typeof applyHeadersFromSchema_ === 'function') {
      repairAction = 'applyHeadersFromSchema_force';
      applyHeadersFromSchema_(INSIGHTS_ANALYSIS_PAGE_, { force: true });
    }
  } catch (e) {
    repairError = e && e.message ? e.message : String(e || '');
  }

  var afterHeaders = readCurrentSheetHeaders_(sheet);
  var afterMissing = _specialFindMissingHeaders_(afterHeaders, INSIGHTS_ANALYSIS_REQUIRED_HEADERS_);
  var afterWidthOk = expected > 0 ? afterHeaders.length === expected : true;
  var ok = afterWidthOk && afterMissing.length === 0;

  var report = {
    ok: ok,
    pageName: INSIGHTS_ANALYSIS_PAGE_,
    action: repairAction,
    expectedColumnCount: expected,
    beforeHeaderCount: beforeHeaders.length,
    afterHeaderCount: afterHeaders.length,
    beforeMissingRequiredHeaders: beforeMissing,
    missingRequiredHeaders: afterMissing,
    beforeHeaders: beforeHeaders,
    afterHeaders: afterHeaders,
    error: repairError,
    message: ok
      ? 'Insights_Analysis headers are refresh-ready.'
      : 'Insights_Analysis headers are not refresh-ready. Missing: ' + afterMissing.join(', ') + (repairError ? ' | error=' + repairError : '')
  };

  logRun_(ok ? 'INFO' : 'WARN', 'ensureInsightsAnalysisHeadersReady', INSIGHTS_ANALYSIS_PAGE_, report.message, {
    status: ok ? 'OK' : 'WARN',
    details: report
  });

  if (typeof writePageStatus_ === 'function') {
    try {
      writePageStatus_(INSIGHTS_ANALYSIS_PAGE_, {
        status: ok ? 'HEADERS_OK' : 'HEADERS_WARN',
        message: report.message,
        endpoint: '',
        httpCode: '',
        rows: Math.max(0, sheet.getLastRow() - 1),
        columns: afterHeaders.length,
        durationMs: '',
        warnings: ok ? [] : afterMissing
      });
    } catch (statusErr) {}
  }

  return report;
}

function repairInsightsAnalysisHeadersFromSpecialPages() {
  return _ensureInsightsAnalysisHeadersReady_();
}

function _specialFindMissingHeaders_(headers, requiredHeaders) {
  var safeHeaders = Array.isArray(headers) ? headers : [];
  var required = Array.isArray(requiredHeaders) ? requiredHeaders : [];
  var headerMap = getHeaderMapSpecial_(safeHeaders);

  return required.filter(function (h) {
    return !specialResolveHeaderColumn_(headerMap, h);
  });
}


/* =========================================================
 * Insights_Analysis
 * ========================================================= */

function buildInsightsAnalysis_() {
  return buildPage_(INSIGHTS_ANALYSIS_PAGE_);
}

function refreshInsightsAnalysis_(options) {
  options = options || {};

  var symbols = normalizeSpecialSymbols_(
    options.symbols && options.symbols.length
      ? options.symbols
      : getDefaultSymbols(INSIGHTS_ANALYSIS_PAGE_)
  );

  var payloadOverrides = { symbols: symbols };

  if (options.limit) payloadOverrides.limit = options.limit;
  if (options.mode) payloadOverrides.mode = options.mode;

  var refreshOpts = {
    payloadOverrides: payloadOverrides,
    applyFormat: options.applyFormat !== false,
    applySort: false
  };

  if (options.forceHeaders === true) refreshOpts.forceHeaders = true;
  if (options.forceHeaders === false) refreshOpts.forceHeaders = false;
  if (options.legacyPath === true) refreshOpts.legacyPath = true;

  var headerReady = _ensureInsightsAnalysisHeadersReady_();
  if (!headerReady.ok) {
    throw new Error('Insights_Analysis header readiness failed: ' + headerReady.message);
  }

  var result = refreshPage_(INSIGHTS_ANALYSIS_PAGE_, refreshOpts);
  result.insightsHeaderReadiness = headerReady;
  appendInsightsFallbackWarning_(result);
  _applyInsightsFreshnessStatus_(result);
  return result;
}

function validateInsightsAnalysis_() {
  var page = getPageConfig(INSIGHTS_ANALYSIS_PAGE_);
  var sheet = ensureSheetExists_(INSIGHTS_ANALYSIS_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var rowCount = Math.max(0, sheet.getLastRow() - 1);
  var statusPanelWarnings = readStatusPanelWarnings_(sheet, headers.length, INSIGHTS_ANALYSIS_PAGE_);
  var timeoutHint = String(statusPanelWarnings || '').toLowerCase().indexOf('timeout') > -1;

  var strictResult = validatePage_(INSIGHTS_ANALYSIS_PAGE_);
  var missingRequiredHeaders = _specialFindMissingHeaders_(headers, INSIGHTS_ANALYSIS_REQUIRED_HEADERS_);
  var widthOk = headers.length === page.expectedColumnCount;
  var requiredHeadersOk = missingRequiredHeaders.length === 0;
  var ok = (!!strictResult.ok && widthOk && requiredHeadersOk) || (
    widthOk &&
    requiredHeadersOk &&
    rowCount === 0 &&
    timeoutHint
  );

  var result = {
    ok: ok,
    pageName: INSIGHTS_ANALYSIS_PAGE_,
    expectedColumnCount: page.expectedColumnCount,
    actualHeadersCount: headers.length,
    widthOk: widthOk,
    requiredHeaders: INSIGHTS_ANALYSIS_REQUIRED_HEADERS_.slice(),
    requiredHeadersOk: requiredHeadersOk,
    missingRequiredHeaders: missingRequiredHeaders,
    rowCount: rowCount,
    timeoutHint: timeoutHint,
    strictResult: strictResult,
    message: ok ? 'Insights_Analysis validation passed.' : 'Insights_Analysis validation found issues.'
  };

  logRun_(ok ? 'INFO' : 'WARN', 'validateInsightsAnalysis', INSIGHTS_ANALYSIS_PAGE_, result.message, {
    status: ok ? 'PASS' : 'WARN',
    details: result
  });

  writePageStatus_(INSIGHTS_ANALYSIS_PAGE_, {
    status: ok ? 'VALID' : 'WARN',
    message: result.message,
    endpoint: '',
    httpCode: '',
    rows: rowCount,
    columns: headers.length,
    durationMs: '',
    warnings: uniqueStrings_(_specialCollectValidationWarnings_(strictResult).concat(missingRequiredHeaders || []))
  });

  return result;
}

function sortInsightsAnalysis_() {
  return withScriptLock_(function () {
    var page = getPageConfig(INSIGHTS_ANALYSIS_PAGE_);
    var sheet = ensureSheetExists_(INSIGHTS_ANALYSIS_PAGE_);
    var headers = readCurrentSheetHeaders_(sheet);
    var lastRow = sheet.getLastRow();
    var lastCol = Math.min(sheet.getLastColumn(), page.expectedColumnCount || headers.length || sheet.getLastColumn());

    if (lastRow <= 2 || !headers.length) {
      return { ok: true, pageName: INSIGHTS_ANALYSIS_PAGE_, skipped: true, message: 'No data rows to sort.' };
    }

    var headerMap = getHeaderMapSpecial_(headers);
    var sortSpec = [];
    // v1.8.1: new contract sorts by 'Sort Order' (numeric, ascending), then
    // 'Section' then 'Item'. The removed 'Symbol' column is no longer used.
    var sortOrderCol = specialResolveHeaderColumn_(headerMap, 'Sort Order');
    var sectionCol = specialResolveHeaderColumn_(headerMap, 'Section');
    var itemCol = specialResolveHeaderColumn_(headerMap, 'Item');

    if (sortOrderCol) sortSpec.push({ column: sortOrderCol, ascending: true });
    if (sectionCol) sortSpec.push({ column: sectionCol, ascending: true });
    if (itemCol) sortSpec.push({ column: itemCol, ascending: true });

    if (!sortSpec.length) {
      return { ok: true, pageName: INSIGHTS_ANALYSIS_PAGE_, skipped: true, message: 'No usable sort fields found.' };
    }

    sheet.getRange(2, 1, lastRow - 1, lastCol).sort(sortSpec);

    logRun_('INFO', 'sortInsightsAnalysis', INSIGHTS_ANALYSIS_PAGE_, 'Insights_Analysis sorted successfully.', {
      status: 'OK',
      sortSpec: sortSpec
    });

    return { ok: true, pageName: INSIGHTS_ANALYSIS_PAGE_, sortSpec: sortSpec };
  });
}

function mergeInsightsHeaders_(options) {
  if (typeof mergeBackendHeadersIntoSheet_ !== 'function') {
    throw new Error('mergeInsightsHeaders_ requires 03_Schema.gs v2.6.1 or later.');
  }
  return mergeBackendHeadersIntoSheet_(INSIGHTS_ANALYSIS_PAGE_, options);
}

function proposeInsightsHeaderUpgrades_() {
  if (typeof proposeHeaderUpgrades_ !== 'function') {
    throw new Error('proposeInsightsHeaderUpgrades_ requires 03_Schema.gs v2.6.1 or later.');
  }
  return proposeHeaderUpgrades_(INSIGHTS_ANALYSIS_PAGE_);
}

function diagnoseInsightsAnalysis_() {
  var page = getPageConfig(INSIGHTS_ANALYSIS_PAGE_);
  var sheet = ensureSheetExists_(INSIGHTS_ANALYSIS_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var headerMap = getHeaderMapSpecial_(headers);
  var rowCount = Math.max(0, sheet.getLastRow() - 1);
  var coverage = null;

  if (typeof getMappedHeaderCoverage_ === 'function') {
    try { coverage = getMappedHeaderCoverage_(INSIGHTS_ANALYSIS_PAGE_); } catch (e) { coverage = null; }
  }

  var missingRequiredHeaders = _specialFindMissingHeaders_(headers, INSIGHTS_ANALYSIS_REQUIRED_HEADERS_);
  var widthOk = headers.length === page.expectedColumnCount;
  var requiredHeadersOk = missingRequiredHeaders.length === 0;
  var freshness = _assessInsightsFreshness_(sheet, headers, headerMap);
  var structuralOk = widthOk && requiredHeadersOk && (!coverage || coverage.unmapped === 0);
  var ok = structuralOk && !freshness.fallbackDetected && !freshness.stale;
  var report = {
    ok: ok,
    structuralOk: structuralOk,
    pageName: INSIGHTS_ANALYSIS_PAGE_,
    headerCount: headers.length,
    expectedColumnCount: page.expectedColumnCount,
    widthOk: widthOk,
    requiredHeaders: INSIGHTS_ANALYSIS_REQUIRED_HEADERS_.slice(),
    requiredHeadersOk: requiredHeadersOk,
    missingRequiredHeaders: missingRequiredHeaders,
    rowCount: rowCount,
    coverage: coverage,
    freshness: freshness
  };

  logRun_(ok ? 'INFO' : 'WARN', 'diagnoseInsightsAnalysis', INSIGHTS_ANALYSIS_PAGE_, ok ? 'Insights diagnostic passed.' : 'Insights diagnostic flagged issues.', {
    status: ok ? 'OK' : 'WARN',
    details: report
  });

  return report;
}


/* =========================================================
 * Insights freshness / fallback detection (v1.8.0; v1.8.1 contract-tolerant)
 * ========================================================= */

function _getInsightsStaleHours_() {
  var props = PropertiesService.getScriptProperties();
  var v = _specialToNumber_(props.getProperty(INSIGHTS_STALE_HOURS_PROPERTY_), INSIGHTS_STALE_MAX_AGE_HOURS_);
  return (v && v > 0) ? v : INSIGHTS_STALE_MAX_AGE_HOURS_;
}

function _assessInsightsFreshness_(sheet, headers, headerMap) {
  var report = {
    fallbackDetected: false,
    fallbackMarkers: [],
    stale: false,
    ageHours: null,
    lastUpdated: null,
    rowCount: 0,
    maxAgeHours: _getInsightsStaleHours_()
  };

  if (!sheet) return report;
  var lastRow = sheet.getLastRow();
  report.rowCount = Math.max(0, lastRow - 1);
  if (lastRow <= 1 || !headers || !headers.length) return report;

  var width = Math.min(sheet.getLastColumn(), headers.length || sheet.getLastColumn());
  var values = sheet.getRange(2, 1, lastRow - 1, width).getValues();
  // v1.8.1: the v2.14.0 contract has no 'Last Updated (Riyadh)' column. When
  // absent, luCol is null and the AGE/STALE branch self-disables (no timestamp
  // source); FALLBACK-marker text detection remains the degradation signal.
  var luCol = specialResolveHeaderColumn_(headerMap, 'Last Updated (Riyadh)');
  var foundMarkers = {};
  var maxTs = null;

  for (var r = 0; r < values.length; r++) {
    var rowText = '';
    for (var c = 0; c < values[r].length; c++) {
      var cell = values[r][c];
      if (cell === null || cell === undefined) continue;
      rowText += ' ' + String(cell).toLowerCase();
    }
    for (var m = 0; m < INSIGHTS_FALLBACK_MARKERS_.length; m++) {
      if (rowText.indexOf(INSIGHTS_FALLBACK_MARKERS_[m]) !== -1) {
        foundMarkers[INSIGHTS_FALLBACK_MARKERS_[m]] = true;
      }
    }
    if (luCol) {
      var d = _specialParseDate_(values[r][luCol - 1]);
      if (d && (!maxTs || d.getTime() > maxTs.getTime())) maxTs = d;
    }
  }

  report.fallbackMarkers = Object.keys(foundMarkers);
  report.fallbackDetected = report.fallbackMarkers.length > 0;

  if (maxTs) {
    report.lastUpdated = maxTs.toISOString();
    report.ageHours = Math.round(((Date.now() - maxTs.getTime()) / 3600000) * 10) / 10;
    report.stale = report.ageHours > report.maxAgeHours;
  }

  return report;
}

function _applyInsightsFreshnessStatus_(result) {
  var sheet = ensureSheetExists_(INSIGHTS_ANALYSIS_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var headerMap = getHeaderMapSpecial_(headers);
  var assessment = _assessInsightsFreshness_(sheet, headers, headerMap);

  if (result) result.insightsFreshness = assessment;

  if (!assessment.fallbackDetected && !assessment.stale) {
    return assessment;
  }

  var bits = [];
  if (assessment.fallbackDetected) {
    bits.push('upstream insights payload unavailable — local fallback content detected (markers: ' + assessment.fallbackMarkers.join(', ') + ')');
  }
  if (assessment.stale) {
    bits.push('data is stale (' + assessment.ageHours + 'h old, max ' + assessment.maxAgeHours + 'h; last updated ' + (assessment.lastUpdated || 'unknown') + ')');
  }

  var msg = 'Insights_Analysis degraded: ' + bits.join('; ') + '. Restore the /v1/advanced insights payload (insights_builder) to clear this.';
  var statusCode = assessment.fallbackDetected ? 'FALLBACK' : 'STALE';

  if (result) {
    result.warnings = uniqueStrings_((result.warnings || []).concat([msg]));
    result.insightsDegraded = true;
  }

  logRun_('WARN', 'insightsFreshness', INSIGHTS_ANALYSIS_PAGE_, msg, {
    status: statusCode,
    details: assessment
  });

  try {
    writePageStatus_(INSIGHTS_ANALYSIS_PAGE_, {
      status: statusCode,
      message: msg,
      endpoint: '',
      httpCode: '',
      rows: assessment.rowCount,
      columns: headers.length,
      durationMs: '',
      warnings: [msg]
    });
  } catch (statusErr) {}

  return assessment;
}

/* =========================================================
 * Insights diagnostics
 * ========================================================= */

function diagnoseV310CriteriaVisibility_(pageName) {
  pageName = pageName || INSIGHTS_ANALYSIS_PAGE_;
  var sheet;

  try { sheet = ensureSheetExists_(pageName); } catch (e) {
    return { ok: false, pageName: pageName, skipped: true, message: 'Sheet not found: ' + (e && e.message ? e.message : 'unknown error') };
  }

  var headers = readCurrentSheetHeaders_(sheet);
  if (!headers || !headers.length) return { ok: false, pageName: pageName, skipped: true, message: 'No headers on sheet.' };

  var headerMap = getHeaderMapSpecial_(headers);
  var sectionCol = specialResolveHeaderColumn_(headerMap, 'Section');
  var itemCol = specialResolveHeaderColumn_(headerMap, 'Item');
  var valueCol = specialResolveHeaderColumn_(headerMap, 'Value');

  if (!sectionCol || !itemCol) {
    return { ok: false, pageName: pageName, skipped: true, message: 'Page does not have Section/Item layout.' };
  }

  var lastRow = sheet.getLastRow();
  if (lastRow <= 1) return { ok: false, pageName: pageName, skipped: true, message: 'No data rows.' };

  var values = sheet.getRange(2, 1, lastRow - 1, headers.length).getValues();
  var found = {};
  var present = [];
  var missing = [];

  for (var i = 0; i < V310_CRITERIA_FIELDS_.length; i++) found[V310_CRITERIA_FIELDS_[i]] = null;

  for (var r = 0; r < values.length; r++) {
    var item = String(values[r][itemCol - 1] == null ? '' : values[r][itemCol - 1]).trim().toLowerCase();
    if (!item) continue;

    for (var k in found) {
      if (!found.hasOwnProperty(k) || found[k] !== null) continue;
      var withSpaces = k.replace(/_/g, ' ');
      var bare = k.replace(/^exclude_/, '');
      var bareWithSpaces = bare.replace(/_/g, ' ');
      if (item === k || item === withSpaces || item === bare || item === bareWithSpaces || item.indexOf(k) !== -1 || item.indexOf(withSpaces) !== -1 || item.indexOf(bareWithSpaces) !== -1) {
        found[k] = { row: r + 2, section: String(values[r][sectionCol - 1] || ''), value: valueCol ? values[r][valueCol - 1] : '' };
      }
    }
  }

  for (var f in found) {
    if (!found.hasOwnProperty(f)) continue;
    if (found[f] === null) missing.push(f);
    else present.push({ field: f, row: found[f].row, section: found[f].section, value: found[f].value });
  }

  var report = {
    ok: missing.length === 0,
    pageName: pageName,
    skipped: false,
    fieldsExpected: V310_CRITERIA_FIELDS_.length,
    fieldsPresent: present.length,
    fieldsMissing: missing.length,
    present: present,
    missing: missing,
    backendModule: 'criteria_model v3.1.0'
  };

  logRun_(report.ok ? 'INFO' : 'WARN', 'diagnoseV310CriteriaVisibility', pageName,
    report.ok ? 'All v3.1.0 criteria fields present.' : 'v3.1.0 criteria fields missing: ' + missing.join(', '),
    { status: report.ok ? 'OK' : 'WARN', details: report });

  return report;
}

function diagnoseDataQualityAlerts_(pageName) {
  pageName = pageName || INSIGHTS_ANALYSIS_PAGE_;
  var sheet;

  try { sheet = ensureSheetExists_(pageName); } catch (e) {
    return { ok: false, pageName: pageName, skipped: true, message: 'Sheet not found: ' + (e && e.message ? e.message : 'unknown error') };
  }

  var headers = readCurrentSheetHeaders_(sheet);
  if (!headers || !headers.length) return { ok: false, pageName: pageName, skipped: true, message: 'No headers on sheet.' };

  var headerMap = getHeaderMapSpecial_(headers);
  var sectionCol = specialResolveHeaderColumn_(headerMap, 'Section');
  var itemCol = specialResolveHeaderColumn_(headerMap, 'Item');
  var metricCol = specialResolveHeaderColumn_(headerMap, 'Metric');
  var valueCol = specialResolveHeaderColumn_(headerMap, 'Value');
  var signalCol = specialResolveHeaderColumn_(headerMap, 'Signal');

  if (!sectionCol) return { ok: false, pageName: pageName, skipped: true, message: 'Page does not have a Section column.' };

  var lastRow = sheet.getLastRow();
  if (lastRow <= 1) {
    return { ok: true, pageName: pageName, skipped: false, alertsFound: 0, alerts: [], sectionPresent: false, classifications: { provider_error: 0, engine_dropped_valuation: 0, forecast_unavailable: 0, other: 0 }, backendModule: 'insights_builder v8.2.0' };
  }

  var values = sheet.getRange(2, 1, lastRow - 1, headers.length).getValues();
  var target = DATA_QUALITY_ALERT_SECTION_.toLowerCase();
  var alerts = [];

  for (var r = 0; r < values.length; r++) {
    var section = String(values[r][sectionCol - 1] || '').trim();
    var lower = section.toLowerCase();
    if (lower === target || lower.indexOf('data quality') === 0) {
      alerts.push({
        row: r + 2,
        section: section,
        item: itemCol ? String(values[r][itemCol - 1] || '') : '',
        metric: metricCol ? values[r][metricCol - 1] : '',
        value: valueCol ? values[r][valueCol - 1] : '',
        signal: signalCol ? String(values[r][signalCol - 1] || '') : ''
      });
    }
  }

  var classifications = { provider_error: 0, engine_dropped_valuation: 0, forecast_unavailable: 0, other: 0 };
  for (var i = 0; i < alerts.length; i++) {
    var itemLower = String(alerts[i].item).toLowerCase();
    if (itemLower.indexOf('provider') !== -1 || itemLower.indexOf('error_class') !== -1) classifications.provider_error++;
    else if (itemLower.indexOf('engine') !== -1 || itemLower.indexOf('valuation') !== -1 || itemLower.indexOf('engine_drop') !== -1) classifications.engine_dropped_valuation++;
    else if (itemLower.indexOf('forecast') !== -1) classifications.forecast_unavailable++;
    else classifications.other++;
  }

  var report = { ok: true, pageName: pageName, skipped: false, alertsFound: alerts.length, alerts: alerts, sectionPresent: alerts.length > 0, classifications: classifications, backendModule: 'insights_builder v8.2.0' };

  logRun_('INFO', 'diagnoseDataQualityAlerts', pageName,
    'Data Quality Alerts: ' + alerts.length + ' rows found.',
    { status: 'OK', details: report });

  return report;
}


/* =========================================================
 * Top_10_Investments — backend-authoritative shortlist
 * ========================================================= */

function buildTop10Investments_() {
  // v1.8.4: decision-owned cockpit — never (re)build the legacy 118-col page
  // over it. (05_Refresh v1.12.0 also guards buildPage_; this is local
  // defense-in-depth so the skip is reported by THIS entry point.)
  if (_specialIsDecisionOwnedPage_(TOP_10_INVESTMENTS_PAGE_)) {
    return _specialSkipDecisionOwned_('buildTop10Investments', TOP_10_INVESTMENTS_PAGE_);
  }
  return buildPage_(TOP_10_INVESTMENTS_PAGE_);
}

function refreshTop10Investments_(options) {
  options = options || {};

  // v1.8.4: decision-owned cockpit — skip the whole legacy pipeline (engine
  // refresh + residual trim + view-token pass + validation) in one place so
  // no trim warnings or false validation WARNs are emitted. 05_Refresh
  // v1.12.0 would stop refreshPage_ anyway; this guard stops the noise too.
  if (_specialIsDecisionOwnedPage_(TOP_10_INVESTMENTS_PAGE_)) {
    return _specialSkipDecisionOwned_('refreshTop10Investments', TOP_10_INVESTMENTS_PAGE_);
  }

  // v1.8.2: Top_10 is a BACKEND-COMPUTED selection page. The selector
  // (top10_selector v4.17.0+) scans its own candidate universe; the verified-
  // correct caller sends EMPTY symbol lists (live criteria snapshots show
  // direct_symbols: []). The previous code injected getDefaultSymbols(Top_10)
  // — Config's hardcoded default list — as EXPLICIT overrides, which pinned
  // the selector pool to that fixed list on every menu refresh. Symbol keys
  // are now omitted entirely unless the caller explicitly provides
  // options.symbols; 05_Refresh.gs v1.11.4 resolveRefreshSymbols_ then sends
  // empty lists for this page. (Deploy 05 v1.11.4 with this file.)
  var payloadOverrides = {};

  if (options.symbols && options.symbols.length) {
    var symbols = normalizeSpecialSymbols_(options.symbols);
    payloadOverrides.symbols = symbols;
    payloadOverrides.tickers = symbols.slice();
    payloadOverrides.direct_symbols = symbols.slice();
  }

  if (options.limit) payloadOverrides.limit = options.limit;
  if (options.mode) payloadOverrides.mode = options.mode;

  var refreshOpts = {
    payloadOverrides: payloadOverrides,
    applyFormat: options.applyFormat !== false,
    applySort: options.applySort !== false
  };

  if (options.forceHeaders === true) refreshOpts.forceHeaders = true;
  if (options.forceHeaders === false) refreshOpts.forceHeaders = false;
  if (options.legacyPath === true) refreshOpts.legacyPath = true;

  var result = refreshPage_(TOP_10_INVESTMENTS_PAGE_, refreshOpts);
  appendTop10FallbackWarning_(result);

  // v1.8.3: purge residual rows below the fresh backend payload. The sheet
  // writer updates the write window only, so rows from PREVIOUS (larger or
  // simply different) runs survive below it and the sheet accumulates a
  // union of runs — observed live 2026-06-11 as 17 rows (fresh 10 + June
  // 9-10 ghosts with colliding ranks). The backend payload is authoritative
  // for row COUNT as well as membership and order. Runs before the
  // view-token override and validation so both see the final state.
  try {
    result.top10ResidualTrim = _trimTop10ResidualRows_(result);
    if (result.top10ResidualTrim && result.top10ResidualTrim.skipped &&
        result.top10ResidualTrim.reason &&
        String(result.top10ResidualTrim.reason).indexOf('rowCount unavailable') === 0) {
      result.warnings = (result.warnings || []).concat(['Top10 residual-row trim skipped: ' + result.top10ResidualTrim.reason]);
    }
  } catch (eTrim) {
    result.warnings = (result.warnings || []).concat(['Top10 residual-row trim failed: ' + (eTrim && eTrim.message ? eTrim.message : String(eTrim))]);
  }

  if (options.skipViewTokenOverride !== true) {
    try {
      var viewResult = applyViewTokensAndRecommendationOverride_(TOP_10_INVESTMENTS_PAGE_);
      if (viewResult && !viewResult.skipped) {
        result.viewTokenOverride = viewResult;
      }
    } catch (e) {
      result.warnings = (result.warnings || []).concat(['View-token override failed: ' + (e && e.message ? e.message : 'unknown error')]);
      logRun_('WARN', 'applyViewTokensAndRecommendationOverride', TOP_10_INVESTMENTS_PAGE_, 'Helper threw — refresh result preserved without view override.', {
        status: 'WARN',
        details: { error: e && e.message ? e.message : String(e || '') }
      });
    }
  }

  // v1.8.2: the GAS rank-and-backfill pass is now OPT-IN (was default-on via
  // !== false). The backend selector is authoritative for membership, rank
  // order, and Selection Reason; re-ranking here would scramble the selector's
  // composite-score order and stamp every below-strict-bar row (under the live
  // conservative gate: all of them) with a Backfill marker. Pass
  // options.enforceStrictTop10 === true, or call enforceTop10StrictRows_()
  // directly, to run the legacy pass deliberately.
  if (options.enforceStrictTop10 === true) {
    try {
      result.strictTop10Enforcement = enforceTop10StrictRows_({
        preserveAtLeastOneRow: options.preserveAtLeastOneRow === true,
        backfill: options.top10Backfill,
        targetCount: options.top10TargetCount
      });
    } catch (e2) {
      result.warnings = (result.warnings || []).concat(['Top10 selection failed: ' + (e2 && e2.message ? e2.message : String(e2))]);
    }
  }

  try {
    result.top10Validation = validateTop10Investments_();
  } catch (e3) {
    result.warnings = (result.warnings || []).concat(['Top10 validation failed: ' + (e3 && e3.message ? e3.message : String(e3))]);
  }

  return result;
}

/*
 * v1.8.3 — Top_10 residual-row purge.
 *
 * The refresh writer places the backend payload into rows 2..(rowCount+1)
 * but never touches rows below that window, so ghosts from earlier runs
 * persist and the sheet becomes a UNION of runs (duplicate ranks, stale
 * Selection Reasons). This helper clears every data row below the freshly
 * written payload across the schema width, immediately after each refresh.
 *
 * Deliberately conservative: when the refresh result does not carry a usable
 * positive numeric rowCount, the trim is SKIPPED (reason reported and
 * surfaced as a refresh warning) rather than guessing the boundary. It never
 * touches rows 2..(rowCount+1) and never deletes rows — clearContent only —
 * so formatting and the header are untouched.
 */
function _trimTop10ResidualRows_(result) {
  // v1.8.4: decision-owned cockpit — clearContent below the "write window"
  // would wipe cockpit sections. Guarded before the lock is even taken.
  if (_specialIsDecisionOwnedPage_(TOP_10_INVESTMENTS_PAGE_)) {
    return _specialSkipDecisionOwned_('trimTop10ResidualRows', TOP_10_INVESTMENTS_PAGE_);
  }
  return withScriptLock_(function () {
    try {
      var written = _specialToNumber_(result && result.rowCount, null);
      if (written === null || written < 1) {
        return {
          ok: false,
          skipped: true,
          pageName: TOP_10_INVESTMENTS_PAGE_,
          reason: 'rowCount unavailable (' + String(result && result.rowCount) + ')'
        };
      }
      written = Math.floor(written);

      var page = getPageConfig(TOP_10_INVESTMENTS_PAGE_);
      var sheet = ensureSheetExists_(TOP_10_INVESTMENTS_PAGE_);
      var lastRow = sheet.getLastRow();
      var firstResidualRow = written + 2; // row 1 = header; rows 2..written+1 = fresh payload

      if (lastRow < firstResidualRow) {
        return {
          ok: true,
          skipped: true,
          pageName: TOP_10_INVESTMENTS_PAGE_,
          reason: 'no residual rows',
          writtenRows: written,
          lastRow: lastRow
        };
      }

      var width = Math.min(sheet.getLastColumn(), page.expectedColumnCount || sheet.getLastColumn());
      var residualCount = lastRow - firstResidualRow + 1;
      sheet.getRange(firstResidualRow, 1, residualCount, width).clearContent();

      logRun_('INFO', 'trimTop10ResidualRows', TOP_10_INVESTMENTS_PAGE_,
        'Cleared ' + residualCount + ' residual row(s) below the fresh ' + written + '-row backend payload.', {
          status: 'OK',
          writtenRows: written,
          residualRowsCleared: residualCount,
          firstResidualRow: firstResidualRow,
          width: width
        });

      return {
        ok: true,
        skipped: false,
        pageName: TOP_10_INVESTMENTS_PAGE_,
        writtenRows: written,
        residualRowsCleared: residualCount
      };
    } catch (e) {
      return {
        ok: false,
        skipped: true,
        pageName: TOP_10_INVESTMENTS_PAGE_,
        error: e && e.message ? e.message : String(e || '')
      };
    }
  });
}

function validateTop10Investments_() {
  // v1.8.4: decision-owned cockpit — the 118-column contract no longer
  // applies to this sheet, so validating it only produces false WARNs.
  // Skip with an INFO log and NO writePageStatus_ call.
  if (_specialIsDecisionOwnedPage_(TOP_10_INVESTMENTS_PAGE_)) {
    return _specialSkipDecisionOwned_('validateTop10Investments', TOP_10_INVESTMENTS_PAGE_, 'INFO');
  }
  var page = getPageConfig(TOP_10_INVESTMENTS_PAGE_);
  var sheet = ensureSheetExists_(TOP_10_INVESTMENTS_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var rowCount = Math.max(0, sheet.getLastRow() - 1);
  var headerMap = getHeaderMapSpecial_(headers);
  var requiredKeys = getRequiredKeys(TOP_10_INVESTMENTS_PAGE_) || [];

  var missingRequiredKeys = requiredKeys.filter(function (key) {
    return !_top10ResolveRequiredKey_(headerMap, headers, key);
  });

  var missingBusinessHeaders = TOP_10_INVESTMENTS_REQUIRED_BUSINESS_HEADERS_.filter(function (h) {
    return !specialResolveHeaderColumn_(headerMap, h);
  });

  var missingStrictHeaders = TOP10_STRICT_REQUIRED_HEADERS_.filter(function (h) {
    return !specialResolveHeaderColumn_(headerMap, h);
  });

  var strictResult = validatePage_(TOP_10_INVESTMENTS_PAGE_);
  var widthOk = page.expectedColumnCount === 118 && headers.length === 118;
  var strictRows = validateTop10StrictRows_();

  var ok = !!strictResult.ok &&
    widthOk &&
    missingRequiredKeys.length === 0 &&
    missingBusinessHeaders.length === 0 &&
    missingStrictHeaders.length === 0 &&
    strictRows.ok;

  var result = {
    ok: ok,
    pageName: TOP_10_INVESTMENTS_PAGE_,
    expectedColumnCount: page.expectedColumnCount,
    actualHeadersCount: headers.length,
    widthOk: widthOk,
    rowCount: rowCount,
    strictApprovedRows: strictRows.strictApprovedRows,
    backfillRows: strictRows.backfillRows,
    backfillEnabled: strictRows.backfillEnabled,
    targetCount: strictRows.targetCount,
    missingRequiredKeys: missingRequiredKeys,
    missingBusinessHeaders: missingBusinessHeaders,
    missingStrictHeaders: missingStrictHeaders,
    strictRows: strictRows,
    strictResult: strictResult,
    validationMode: strictResult.mappingModeUsed ? 'header-map' : 'strict-positional'
  };

  var warningsList = []
    .concat(_specialCollectValidationWarnings_(strictResult))
    .concat(missingRequiredKeys || [])
    .concat(missingBusinessHeaders || [])
    .concat(missingStrictHeaders || [])
    .concat(strictRows.warnings || []);

  if (!widthOk) warningsList.push('Top_10_Investments width mismatch: expected 118, actual ' + headers.length + '.');

  var statusMsg = ok
    ? ('Top_10_Investments validation passed (' + strictRows.strictApprovedRows + ' strict-qualified, ' + strictRows.backfillRows + ' below strict bar)')
    : 'Top_10_Investments validation issues found';

  logRun_(ok ? 'INFO' : 'WARN', 'validateTop10Investments', TOP_10_INVESTMENTS_PAGE_,
    ok ? statusMsg : 'Top_10_Investments validation found issues.',
    { status: ok ? 'PASS' : 'WARN', details: result });

  writePageStatus_(TOP_10_INVESTMENTS_PAGE_, {
    status: ok ? 'VALID' : 'WARN',
    message: statusMsg,
    endpoint: '',
    httpCode: '',
    rows: rowCount,
    columns: headers.length,
    durationMs: '',
    warnings: uniqueStrings_(warningsList)
  });

  return result;
}


function sortTop10Investments_() {
  // v1.8.4: decision-owned cockpit — sorting rows 2..end would scramble the
  // control panel / KPI / section layout. Guarded before the lock.
  if (_specialIsDecisionOwnedPage_(TOP_10_INVESTMENTS_PAGE_)) {
    return _specialSkipDecisionOwned_('sortTop10Investments', TOP_10_INVESTMENTS_PAGE_);
  }
  return withScriptLock_(function () {
    var page = getPageConfig(TOP_10_INVESTMENTS_PAGE_);
    var sheet = ensureSheetExists_(TOP_10_INVESTMENTS_PAGE_);
    var headers = readCurrentSheetHeaders_(sheet);
    var lastRow = sheet.getLastRow();
    var lastCol = Math.min(sheet.getLastColumn(), page.expectedColumnCount || headers.length || sheet.getLastColumn());

    if (lastRow <= 2 || !headers.length) {
      return { ok: true, pageName: TOP_10_INVESTMENTS_PAGE_, skipped: true, message: 'No data rows to sort.' };
    }

    var headerMap = getHeaderMapSpecial_(headers);
    var top10RankCol = specialResolveHeaderColumn_(headerMap, 'Top 10 Rank') ||
      specialResolveHeaderColumn_(headerMap, 'Top10 Rank') ||
      specialResolveHeaderColumn_(headerMap, 'top10_rank');
    var finalActionCol = specialResolveHeaderColumn_(headerMap, 'Final Action');
    var reliabilityCol = specialResolveHeaderColumn_(headerMap, 'Forecast Reliability Score');
    var roi12mCol = specialResolveHeaderColumn_(headerMap, 'Expected ROI 12M');
    var oppScoreCol = specialResolveHeaderColumn_(headerMap, 'Opportunity Score');

    var sortSpec = [];
    if (top10RankCol) sortSpec.push({ column: top10RankCol, ascending: true });
    else {
      if (finalActionCol) sortSpec.push({ column: finalActionCol, ascending: true });
      if (reliabilityCol) sortSpec.push({ column: reliabilityCol, ascending: false });
      if (roi12mCol) sortSpec.push({ column: roi12mCol, ascending: false });
      else if (oppScoreCol) sortSpec.push({ column: oppScoreCol, ascending: false });
    }

    if (!sortSpec.length) {
      return { ok: true, pageName: TOP_10_INVESTMENTS_PAGE_, skipped: true, message: 'No usable sort field found.' };
    }

    sheet.getRange(2, 1, lastRow - 1, lastCol).sort(sortSpec);

    logRun_('INFO', 'sortTop10Investments', TOP_10_INVESTMENTS_PAGE_, 'Top_10_Investments sorted successfully.', {
      status: 'OK',
      sortSpec: sortSpec
    });

    return { ok: true, pageName: TOP_10_INVESTMENTS_PAGE_, sortSpec: sortSpec };
  });
}

function mergeTop10Headers_(options) {
  if (typeof mergeBackendHeadersIntoSheet_ !== 'function') {
    throw new Error('mergeTop10Headers_ requires 03_Schema.gs v2.6.1 or later.');
  }
  return mergeBackendHeadersIntoSheet_(TOP_10_INVESTMENTS_PAGE_, options);
}

function proposeTop10HeaderUpgrades_() {
  if (typeof proposeHeaderUpgrades_ !== 'function') {
    throw new Error('proposeTop10HeaderUpgrades_ requires 03_Schema.gs v2.6.1 or later.');
  }
  return proposeHeaderUpgrades_(TOP_10_INVESTMENTS_PAGE_);
}

function diagnoseTop10Investments_() {
  // v1.8.4: decision-owned cockpit — read-only, but judging the cockpit's
  // title row against the 118-column contract is pure noise. INFO skip.
  if (_specialIsDecisionOwnedPage_(TOP_10_INVESTMENTS_PAGE_)) {
    return _specialSkipDecisionOwned_('diagnoseTop10Investments', TOP_10_INVESTMENTS_PAGE_, 'INFO');
  }
  var page = getPageConfig(TOP_10_INVESTMENTS_PAGE_);
  var sheet = ensureSheetExists_(TOP_10_INVESTMENTS_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var headerMap = getHeaderMapSpecial_(headers);
  var requiredKeys = getRequiredKeys(TOP_10_INVESTMENTS_PAGE_) || [];

  var missingRequiredKeys = requiredKeys.filter(function (key) {
    return !_top10ResolveRequiredKey_(headerMap, headers, key);
  });

  var missingBusinessHeaders = TOP_10_INVESTMENTS_REQUIRED_BUSINESS_HEADERS_.filter(function (h) {
    return !specialResolveHeaderColumn_(headerMap, h);
  });

  var missingStrictHeaders = TOP10_STRICT_REQUIRED_HEADERS_.filter(function (h) {
    return !specialResolveHeaderColumn_(headerMap, h);
  });

  var coverage = null;
  if (typeof getMappedHeaderCoverage_ === 'function') {
    try { coverage = getMappedHeaderCoverage_(TOP_10_INVESTMENTS_PAGE_); } catch (e) { coverage = null; }
  }

  var strictRows = validateTop10StrictRows_();
  var ok = headers.length === page.expectedColumnCount &&
    page.expectedColumnCount === 118 &&
    missingBusinessHeaders.length === 0 &&
    missingRequiredKeys.length === 0 &&
    missingStrictHeaders.length === 0 &&
    (!coverage || coverage.unmapped === 0) &&
    strictRows.ok;

  var report = {
    ok: ok,
    pageName: TOP_10_INVESTMENTS_PAGE_,
    expectedColumnCount: page.expectedColumnCount,
    headerCount: headers.length,
    rowCount: Math.max(0, sheet.getLastRow() - 1),
    strictApprovedRows: strictRows.strictApprovedRows,
    backfillRows: strictRows.backfillRows,
    backfillEnabled: strictRows.backfillEnabled,
    targetCount: strictRows.targetCount,
    missingBusinessHeaders: missingBusinessHeaders,
    missingRequiredKeys: missingRequiredKeys,
    missingStrictHeaders: missingStrictHeaders,
    coverage: coverage,
    strictRows: strictRows
  };

  logRun_(ok ? 'INFO' : 'WARN', 'diagnoseTop10Investments', TOP_10_INVESTMENTS_PAGE_,
    ok ? 'Top_10_Investments diagnostic passed.' : 'Top_10_Investments diagnostic flagged issues.',
    { status: ok ? 'OK' : 'WARN', details: report });

  return report;
}

/*
 * v1.8.2: RETAINED VERBATIM as a manual / legacy tool. This pass is no longer
 * invoked by refreshTop10Investments_ by default — the backend selector
 * (top10_selector v4.17.0+) is authoritative for Top_10 membership, rank order,
 * and Selection Reason. Run this deliberately (direct call, or
 * options.enforceStrictTop10 === true on the refresh) only when you want the
 * GAS-side rank-and-backfill reshaping of whatever rows are on the sheet.
 */
function enforceTop10StrictRows_(options) {
  options = options || {};

  // v1.8.4: decision-owned cockpit — this legacy tool clearContents and
  // rewrites every data row, which would destroy the cockpit. Guarded before
  // the lock; it remains fully usable should ownership ever revert.
  if (_specialIsDecisionOwnedPage_(TOP_10_INVESTMENTS_PAGE_)) {
    return _specialSkipDecisionOwned_('enforceTop10StrictRows', TOP_10_INVESTMENTS_PAGE_);
  }

  return withScriptLock_(function () {
    var page = getPageConfig(TOP_10_INVESTMENTS_PAGE_);
    var sheet = ensureSheetExists_(TOP_10_INVESTMENTS_PAGE_);
    var headers = readCurrentSheetHeaders_(sheet);
    var lastRow = sheet.getLastRow();

    if (!headers.length || lastRow <= 1) {
      return { ok: true, pageName: TOP_10_INVESTMENTS_PAGE_, skipped: true, keptRows: 0, strictRows: 0, backfillRows: 0, removedRows: 0, message: 'No rows to enforce.' };
    }

    var headerMap = getHeaderMapSpecial_(headers);
    var missing = TOP10_STRICT_REQUIRED_HEADERS_.filter(function (h) { return !specialResolveHeaderColumn_(headerMap, h); });
    if (missing.length) {
      return { ok: false, pageName: TOP_10_INVESTMENTS_PAGE_, skipped: true, keptRows: 0, strictRows: 0, backfillRows: 0, removedRows: 0, missingStrictHeaders: missing, message: 'Missing strict headers: ' + missing.join(', ') };
    }

    var settings = _getTop10SelectionSettings_(options);
    var cols = _getTop10StrictCols_(headerMap);
    var rankCol = cols.rank;
    var selectionReasonCol = specialResolveHeaderColumn_(headerMap, 'Selection Reason');

    var effectiveWidth = Math.min(sheet.getLastColumn(), page.expectedColumnCount || headers.length || sheet.getLastColumn());
    var values = sheet.getRange(2, 1, lastRow - 1, effectiveWidth).getValues();

    var strictCandidates = [];
    var backfillCandidates = [];
    var nonEmptyCount = 0;

    for (var i = 0; i < values.length; i++) {
      var row = values[i].slice(0, effectiveWidth);
      var evaluation = _evaluateTop10StrictRow_(row, headerMap, i + 2);
      if (evaluation.empty) continue;
      nonEmptyCount++;
      var entry = { row: row, score: _top10RankingScore_(row, cols), evaluation: evaluation };
      if (evaluation.ok) strictCandidates.push(entry);
      else backfillCandidates.push(entry);
    }

    strictCandidates.sort(_compareTop10Score_);
    backfillCandidates.sort(_compareTop10Score_);

    var allRanked = settings.backfillEnabled ? strictCandidates.concat(backfillCandidates) : strictCandidates.slice();
    var selected = allRanked.slice(0, settings.targetCount);

    if (selected.length === 0 && options.preserveAtLeastOneRow === true) {
      return {
        ok: false,
        pageName: TOP_10_INVESTMENTS_PAGE_,
        skipped: true,
        keptRows: 0,
        strictRows: 0,
        backfillRows: 0,
        removedRows: nonEmptyCount,
        targetCount: settings.targetCount,
        backfillEnabled: settings.backfillEnabled,
        message: 'No selectable rows. Existing rows preserved (preserveAtLeastOneRow=true).'
      };
    }

    var strictSelected = 0;
    var backfillSelected = 0;
    var outRows = [];

    for (var k = 0; k < selected.length; k++) {
      var r = selected[k].row.slice(0, effectiveWidth);
      var isStrict = !!selected[k].evaluation.ok;
      if (isStrict) strictSelected++; else backfillSelected++;

      if (rankCol) r[rankCol - 1] = k + 1;

      if (selectionReasonCol) {
        var existing = _specialCellString_(r, selectionReasonCol);
        if (isStrict) {
          if (!existing) r[selectionReasonCol - 1] = 'Strict-qualified Top 10';
        } else {
          var reasonsArr = selected[k].evaluation.reasons || [];
          var why = reasonsArr.length ? reasonsArr.join('; ') : 'below strict bar';
          var marker = 'Backfill (below strict bar: ' + why + ')';
          r[selectionReasonCol - 1] = existing ? (marker + ' | ' + existing) : marker;
        }
      }

      outRows.push(r);
    }

    var droppedPreview = [];
    for (var d = selected.length; d < allRanked.length && droppedPreview.length < 20; d++) {
      droppedPreview.push({ symbol: allRanked[d].evaluation.symbol, strict: !!allRanked[d].evaluation.ok });
    }

    var dataRows = Math.max(0, lastRow - 1);
    if (dataRows > 0) sheet.getRange(2, 1, dataRows, effectiveWidth).clearContent();
    if (outRows.length > 0) sheet.getRange(2, 1, outRows.length, effectiveWidth).setValues(outRows);

    if (typeof applySheetFormatting_ === 'function') {
      try { applySheetFormatting_(TOP_10_INVESTMENTS_PAGE_); } catch (fmtErr) {}
    }

    var removedRows = Math.max(0, nonEmptyCount - outRows.length);
    var modeLabel = settings.backfillEnabled ? 'backfill' : 'strict-only';
    var msg = 'Top10 selection (' + modeLabel + '): kept ' + outRows.length +
      ' (' + strictSelected + ' strict-qualified, ' + backfillSelected + ' backfill), removed ' + removedRows +
      ' of ' + nonEmptyCount + ' candidate(s). Target=' + settings.targetCount + '.';

    var statusWarnings = [];
    if (strictSelected === 0) statusWarnings.push('No strict-qualified investments; Top 10 is entirely backfill candidates.');
    if (outRows.length < settings.targetCount) statusWarnings.push('Only ' + outRows.length + ' candidate(s) available; fewer than target ' + settings.targetCount + '. Widen the backend selector pool (TOP10_SELECTOR_SOURCE_PAGE_LIMIT).');
    var degraded = statusWarnings.length > 0;

    logRun_(degraded ? 'WARN' : 'INFO', 'enforceTop10StrictRows', TOP_10_INVESTMENTS_PAGE_, msg, {
      status: degraded ? 'WARN' : 'OK',
      keptRows: outRows.length,
      strictRows: strictSelected,
      backfillRows: backfillSelected,
      removedRows: removedRows,
      targetCount: settings.targetCount,
      backfillEnabled: settings.backfillEnabled,
      droppedPreview: droppedPreview
    });

    writePageStatus_(TOP_10_INVESTMENTS_PAGE_, {
      status: degraded ? 'WARN' : (backfillSelected > 0 ? 'BACKFILL_OK' : 'STRICT_OK'),
      message: msg,
      endpoint: '',
      httpCode: '',
      rows: outRows.length,
      columns: headers.length,
      durationMs: '',
      warnings: statusWarnings
    });

    return {
      ok: true,
      pageName: TOP_10_INVESTMENTS_PAGE_,
      skipped: false,
      keptRows: outRows.length,
      strictRows: strictSelected,
      backfillRows: backfillSelected,
      removedRows: removedRows,
      targetCount: settings.targetCount,
      backfillEnabled: settings.backfillEnabled,
      droppedPreview: droppedPreview,
      message: msg
    };
  });
}


function validateTop10StrictRows_() {
  var sheet = ensureSheetExists_(TOP_10_INVESTMENTS_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var settings = _getTop10SelectionSettings_();
  var result = {
    ok: true,
    pageName: TOP_10_INVESTMENTS_PAGE_,
    checkedRows: 0,
    strictApprovedRows: 0,
    backfillRows: 0,
    invalidRows: [],
    missingStrictHeaders: [],
    backfillEnabled: settings.backfillEnabled,
    targetCount: settings.targetCount,
    warnings: []
  };

  if (!headers.length || sheet.getLastRow() <= 1) return result;

  var headerMap = getHeaderMapSpecial_(headers);
  var missing = TOP10_STRICT_REQUIRED_HEADERS_.filter(function (h) { return !specialResolveHeaderColumn_(headerMap, h); });
  if (missing.length) {
    result.ok = false;
    result.missingStrictHeaders = missing;
    result.warnings.push('Top10 missing strict headers: ' + missing.join(', '));
    return result;
  }

  var page = getPageConfig(TOP_10_INVESTMENTS_PAGE_);
  var effectiveWidth = Math.min(sheet.getLastColumn(), page.expectedColumnCount || headers.length || sheet.getLastColumn());
  var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, effectiveWidth).getValues();
  var rankCol = specialResolveHeaderColumn_(headerMap, 'Top 10 Rank');
  var ranks = [];
  var nonStrict = [];

  for (var i = 0; i < values.length; i++) {
    var evaluation = _evaluateTop10StrictRow_(values[i], headerMap, i + 2);
    if (evaluation.empty) continue;
    result.checkedRows++;
    if (evaluation.ok) result.strictApprovedRows++;
    else { result.backfillRows++; nonStrict.push(evaluation); }
    if (rankCol) {
      var rk = _specialToNumber_(values[i][rankCol - 1], null);
      if (rk !== null) ranks.push(rk);
    }
  }

  if (!settings.backfillEnabled && result.backfillRows > 0) {
    result.ok = false;
    result.invalidRows = nonStrict;
    result.warnings.push('Top10 (strict-only mode) contains ' + result.backfillRows + ' non-strict row(s): ' +
      nonStrict.slice(0, 15).map(function (x) { return x.symbol + '@R' + x.row + ' [' + x.reasons.join('; ') + ']'; }).join(' | ') +
      (nonStrict.length > 15 ? ' ...(+' + (nonStrict.length - 15) + ')' : ''));
  }

  // v1.8.2: neutral wording. With the GAS enforcement pass off by default,
  // below-strict-bar rows are the backend selector's honest picks under the
  // engine's conservative gate (Fix T) — not GAS-generated "backfill".
  if (settings.backfillEnabled && result.checkedRows > 0 && result.strictApprovedRows === 0) {
    result.warnings.push('Top10 contains no strict-qualified rows; all ' + result.backfillRows + ' row(s) are below the strict bar (expected under the engine conservative gate; see Block Reason).');
  }

  if (rankCol && ranks.length) {
    ranks.sort(function (a, b) { return a - b; });
    var contiguous = true;
    for (var z = 0; z < ranks.length; z++) {
      if (ranks[z] !== z + 1) { contiguous = false; break; }
    }
    if (!contiguous) result.warnings.push('Top10 Rank values are not a contiguous 1..N sequence.');
  }

  return result;
}

function _evaluateTop10StrictRow_(row, headerMap, rowNumber) {
  var settings = _getTop10StrictSettings_();
  var cols = _getTop10StrictCols_(headerMap);
  var symbol = _specialCellString_(row, cols.symbol);

  if (!symbol && !_specialCellString_(row, cols.rank)) {
    return { empty: true, ok: true, row: rowNumber, symbol: '' };
  }

  var reasons = [];
  var finalAction = _specialCellString_(row, cols.finalAction).toUpperCase();
  var investability = _specialCellString_(row, cols.investability).toUpperCase();
  var price = cols.price ? row[cols.price - 1] : '';
  var risk = _specialCellString_(row, cols.risk).toUpperCase();
  var roi3m = _specialToNumber_(cols.roi3m ? row[cols.roi3m - 1] : '', null);
  var roi12m = _specialToNumber_(cols.roi12m ? row[cols.roi12m - 1] : '', null);
  var dq = _specialToNumber_(cols.dq ? row[cols.dq - 1] : '', null);
  var rel = _specialToNumber_(cols.rel ? row[cols.rel - 1] : '', null);
  var conflict = _specialToBoolean_(cols.conflict ? row[cols.conflict - 1] : '', false);
  var blockReason = _specialCellString_(row, cols.blockReason);

  if (finalAction !== 'INVEST') reasons.push('Final Action <> INVEST');
  if (investability !== 'INVESTABLE') reasons.push('Investability Status <> INVESTABLE');
  if (_specialIsBlank_(price)) reasons.push('blank Current Price');
  if (risk === 'HIGH') reasons.push('HIGH Risk Bucket');
  if (roi3m === null || roi3m <= 0) reasons.push('Expected ROI 3M <= 0 or missing');
  if (roi12m === null || roi12m <= 0) reasons.push('Expected ROI 12M <= 0 or missing');
  if (dq === null || dq < settings.dqMin) reasons.push('Data Quality Score < ' + settings.dqMin + ' or missing');
  if (rel === null || rel < settings.reliabilityMin) reasons.push('Forecast Reliability Score < ' + settings.reliabilityMin + ' or missing');
  if (conflict) reasons.push('Provider/Engine Conflict TRUE');
  if (blockReason) reasons.push('Block Reason not blank');

  return {
    empty: false,
    ok: reasons.length === 0,
    row: rowNumber,
    symbol: symbol || '(blank-symbol)',
    reasons: reasons,
    metrics: {
      finalAction: finalAction,
      investability: investability,
      risk: risk,
      expectedRoi3m: roi3m,
      expectedRoi12m: roi12m,
      dataQualityScore: dq,
      forecastReliabilityScore: rel,
      providerConflict: conflict,
      blockReason: blockReason
    }
  };
}

function _getTop10StrictCols_(headerMap) {
  return {
    rank: specialResolveHeaderColumn_(headerMap, 'Top 10 Rank'),
    symbol: specialResolveHeaderColumn_(headerMap, 'Symbol'),
    price: specialResolveHeaderColumn_(headerMap, 'Current Price'),
    risk: specialResolveHeaderColumn_(headerMap, 'Risk Bucket'),
    roi3m: specialResolveHeaderColumn_(headerMap, 'Expected ROI 3M'),
    roi12m: specialResolveHeaderColumn_(headerMap, 'Expected ROI 12M'),
    dq: specialResolveHeaderColumn_(headerMap, 'Data Quality Score'),
    rel: specialResolveHeaderColumn_(headerMap, 'Forecast Reliability Score'),
    conflict: specialResolveHeaderColumn_(headerMap, 'Provider/Engine Conflict'),
    investability: specialResolveHeaderColumn_(headerMap, 'Investability Status'),
    finalAction: specialResolveHeaderColumn_(headerMap, 'Final Action'),
    blockReason: specialResolveHeaderColumn_(headerMap, 'Block Reason'),
    opp: specialResolveHeaderColumn_(headerMap, 'Opportunity Score'),
    overall: specialResolveHeaderColumn_(headerMap, 'Overall Score')
  };
}

function _getTop10StrictSettings_() {
  var props = PropertiesService.getScriptProperties();
  return {
    dqMin: _specialToNumber_(props.getProperty(TOP10_STRICT_DQ_MIN_PROPERTY_), TOP10_DEFAULT_DQ_MIN_),
    reliabilityMin: _specialToNumber_(props.getProperty(TOP10_STRICT_RELIABILITY_MIN_PROPERTY_), TOP10_DEFAULT_RELIABILITY_MIN_)
  };
}

function _getTop10SelectionSettings_(options) {
  options = options || {};
  var props = PropertiesService.getScriptProperties();

  var target = _specialToNumber_(options.targetCount, null);
  if (target === null) target = _specialToNumber_(props.getProperty(TOP10_TARGET_COUNT_PROPERTY_), TOP10_DEFAULT_TARGET_COUNT_);
  if (!(target >= 1)) target = TOP10_DEFAULT_TARGET_COUNT_;
  target = Math.floor(target);

  var backfillEnabled;
  if (options.backfill === true || options.backfill === false) {
    backfillEnabled = options.backfill;
  } else {
    backfillEnabled = _specialToBoolean_(props.getProperty(TOP10_BACKFILL_PROPERTY_), true);
  }

  return { targetCount: target, backfillEnabled: backfillEnabled };
}

function _top10RankingScore_(row, cols) {
  var opp = _specialToNumber_(cols.opp ? row[cols.opp - 1] : '', null);
  var overall = _specialToNumber_(cols.overall ? row[cols.overall - 1] : '', null);
  var rel = _specialToNumber_(cols.rel ? row[cols.rel - 1] : '', null);
  var roi12 = _specialToNumber_(cols.roi12m ? row[cols.roi12m - 1] : '', null);

  var primary = (opp !== null) ? opp : (overall !== null ? overall : -1);
  var secondary = (rel !== null) ? rel : -1;
  var tertiary = (roi12 !== null) ? roi12 : -999;

  return { primary: primary, secondary: secondary, tertiary: tertiary };
}

function _compareTop10Score_(a, b) {
  if (b.score.primary !== a.score.primary) return b.score.primary - a.score.primary;
  if (b.score.secondary !== a.score.secondary) return b.score.secondary - a.score.secondary;
  return b.score.tertiary - a.score.tertiary;
}


/* =========================================================
 * Data_Dictionary
 * ========================================================= */

function buildDataDictionary_() {
  return buildPage_(DATA_DICTIONARY_PAGE_);
}

function refreshDataDictionary_(options) {
  options = options || {};
  var refreshOpts = { applyFormat: options.applyFormat !== false, applySort: false };
  if (options.forceHeaders === true) refreshOpts.forceHeaders = true;
  if (options.forceHeaders === false) refreshOpts.forceHeaders = false;
  if (options.legacyPath === true) refreshOpts.legacyPath = true;
  return refreshPage_(DATA_DICTIONARY_PAGE_, refreshOpts);
}

function validateDataDictionary_() {
  var page = getPageConfig(DATA_DICTIONARY_PAGE_);
  var sheet = ensureSheetExists_(DATA_DICTIONARY_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var rowCount = Math.max(0, sheet.getLastRow() - 1);
  var strictResult = validatePage_(DATA_DICTIONARY_PAGE_);

  var result = {
    ok: !!strictResult.ok,
    pageName: DATA_DICTIONARY_PAGE_,
    expectedColumnCount: page.expectedColumnCount,
    actualHeadersCount: headers.length,
    rowCount: rowCount,
    strictResult: strictResult,
    emptyRowsAllowed: true
  };

  logRun_(result.ok ? 'INFO' : 'WARN', 'validateDataDictionary', DATA_DICTIONARY_PAGE_,
    result.ok ? 'Data_Dictionary validation passed.' : 'Data_Dictionary validation found issues.',
    { status: result.ok ? 'PASS' : 'WARN', details: result });

  writePageStatus_(DATA_DICTIONARY_PAGE_, {
    status: result.ok ? 'VALID' : 'WARN',
    message: result.ok ? 'Data_Dictionary validation passed' : 'Data_Dictionary validation issues found',
    endpoint: '',
    httpCode: '',
    rows: rowCount,
    columns: headers.length,
    durationMs: '',
    warnings: _specialCollectValidationWarnings_(strictResult)
  });

  return result;
}

function sortDataDictionary_() {
  return withScriptLock_(function () {
    var page = getPageConfig(DATA_DICTIONARY_PAGE_);
    var sheet = ensureSheetExists_(DATA_DICTIONARY_PAGE_);
    var headers = readCurrentSheetHeaders_(sheet);
    var lastRow = sheet.getLastRow();
    var lastCol = Math.min(sheet.getLastColumn(), page.expectedColumnCount || headers.length || sheet.getLastColumn());

    if (lastRow <= 2 || !headers.length) {
      return { ok: true, pageName: DATA_DICTIONARY_PAGE_, skipped: true, message: 'No data rows to sort.' };
    }

    var headerMap = getHeaderMapSpecial_(headers);
    var sortCol = specialResolveHeaderColumn_(headerMap, 'Field') ||
      specialResolveHeaderColumn_(headerMap, 'Item') ||
      specialResolveHeaderColumn_(headerMap, 'Metric') ||
      specialResolveHeaderColumn_(headerMap, 'Header');

    if (!sortCol) return { ok: true, pageName: DATA_DICTIONARY_PAGE_, skipped: true, message: 'No usable sort fields found.' };

    sheet.getRange(2, 1, lastRow - 1, lastCol).sort([{ column: sortCol, ascending: true }]);

    logRun_('INFO', 'sortDataDictionary', DATA_DICTIONARY_PAGE_, 'Data_Dictionary sorted successfully.', { status: 'OK', sortColumn: sortCol });
    return { ok: true, pageName: DATA_DICTIONARY_PAGE_, sortedBy: headers[sortCol - 1] };
  });
}

function mergeDataDictionaryHeaders_(options) {
  if (typeof mergeBackendHeadersIntoSheet_ !== 'function') {
    throw new Error('mergeDataDictionaryHeaders_ requires 03_Schema.gs v2.6.1 or later.');
  }
  return mergeBackendHeadersIntoSheet_(DATA_DICTIONARY_PAGE_, options);
}

function proposeDataDictionaryHeaderUpgrades_() {
  if (typeof proposeHeaderUpgrades_ !== 'function') {
    throw new Error('proposeDataDictionaryHeaderUpgrades_ requires 03_Schema.gs v2.6.1 or later.');
  }
  return proposeHeaderUpgrades_(DATA_DICTIONARY_PAGE_);
}

function diagnoseDataDictionary_() {
  var page = getPageConfig(DATA_DICTIONARY_PAGE_);
  var sheet = ensureSheetExists_(DATA_DICTIONARY_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var coverage = null;

  if (typeof getMappedHeaderCoverage_ === 'function') {
    try { coverage = getMappedHeaderCoverage_(DATA_DICTIONARY_PAGE_); } catch (e) { coverage = null; }
  }

  var ok = headers.length === page.expectedColumnCount && (!coverage || coverage.unmapped === 0);
  var report = { ok: ok, pageName: DATA_DICTIONARY_PAGE_, headerCount: headers.length, expectedColumnCount: page.expectedColumnCount, rowCount: Math.max(0, sheet.getLastRow() - 1), coverage: coverage, emptyRowsAllowed: true };

  logRun_(ok ? 'INFO' : 'WARN', 'diagnoseDataDictionary', DATA_DICTIONARY_PAGE_, ok ? 'Data_Dictionary diagnostic passed.' : 'Data_Dictionary diagnostic flagged issues.', { status: ok ? 'OK' : 'WARN', details: report });
  return report;
}


/* =========================================================
 * Recommendation mirror helpers
 * ========================================================= */

/*
 * v1.8.5: the four recommendation-derivation helpers that lived here
 * (_deriveCanonicalRecommendation_, _normalizeConfidenceToPercent_,
 * _normalizeRoiToFraction_, _coerceNumericScore_) were dead code within this
 * file since the v1.8.x Detail-authoritative mirror. Their removal was gated
 * on a project-wide grep (per the v1.8.2 header NOTE); that grep is now
 * complete — 12_Diagnostics.gs v1.7.0 and 99_Diag.gs v2.4.0 are audited and
 * reference none of them, and every recommendation/accuracy diagnostic in the
 * project routes through the three KEEPERS below. With no in-file and no
 * external callers, the four helpers have been removed.
 *
 * RETAINED (live callers in applyViewTokensAndRecommendationOverride_ and
 * diagnoseRecommendationAccuracy_):
 *   _normalizeRecommendationForCompare_
 *   _canonicalizeRecommendationLabel_
 *   _extractReasonPrefixRecommendation_
 */
function _normalizeRecommendationForCompare_(v) {
  if (v === null || v === undefined) return '';
  return String(v).trim().toUpperCase().replace(/\s+/g, '_');
}

function _canonicalizeRecommendationLabel_(v) {
  if (v === null || v === undefined) return null;
  var trimmed = String(v).trim();
  if (!trimmed) return null;
  var upper = trimmed.toUpperCase();
  if (SPECIAL_VALID_RECOMMENDATIONS_[upper]) return trimmed;
  return null;
}

function _extractReasonPrefixRecommendation_(reason) {
  if (reason === null || reason === undefined) return null;
  var s = String(reason).trim();
  if (!s) return null;
  var colonIdx = s.indexOf(':');
  if (colonIdx > 0) {
    var prefixLabel = _canonicalizeRecommendationLabel_(s.substring(0, colonIdx));
    if (prefixLabel) return prefixLabel;
  }
  var parsed = _parseRecommendationReason_(s);
  if (parsed && parsed.recommendation) return _canonicalizeRecommendationLabel_(parsed.recommendation);
  return null;
}

function applyViewTokensAndRecommendationOverride_(pageName) {
  if (!pageName) return { ok: false, pageName: '', skipped: true, message: 'No pageName provided.' };

  // v1.8.4: decision-owned pages get no view-token/recommendation writes —
  // covers applyViewTokensTop10_, applyViewTokensForCurrentPage_ (cockpit
  // active), and any generic caller. Skip shape is compatible with callers
  // that test `viewResult && !viewResult.skipped` (07_Global_Markets).
  if (_specialIsDecisionOwnedPage_(pageName)) {
    return _specialSkipDecisionOwned_('applyViewTokensAndRecommendationOverride', pageName);
  }

  return withScriptLock_(function () {
    var sheet;
    try { sheet = ensureSheetExists_(pageName); } catch (e) {
      return { ok: false, pageName: pageName, skipped: true, message: 'Sheet not found: ' + (e && e.message ? e.message : 'unknown error') };
    }

    var headers = readCurrentSheetHeaders_(sheet);
    if (!headers || !headers.length) return { ok: true, pageName: pageName, skipped: true, message: 'No headers on sheet.' };

    var headerMap = getHeaderMapSpecial_(headers);
    var reasonCol = specialResolveHeaderColumn_(headerMap, 'Recommendation Reason');
    if (!reasonCol) return { ok: true, pageName: pageName, skipped: true, message: 'No Recommendation Reason column found.' };

    var fundCol = specialResolveHeaderColumn_(headerMap, 'Fundamental View');
    var techCol = specialResolveHeaderColumn_(headerMap, 'Technical View');
    var riskCol = specialResolveHeaderColumn_(headerMap, 'Risk View');
    var valCol = specialResolveHeaderColumn_(headerMap, 'Value View');
    var recCol = specialResolveHeaderColumn_(headerMap, 'Recommendation');
    var recDetailCol = specialResolveHeaderColumn_(headerMap, 'Recommendation Detail');

    if (!(fundCol || techCol || riskCol || valCol || recCol)) {
      return { ok: true, pageName: pageName, skipped: true, message: 'No View columns or Recommendation column to update.' };
    }

    var lastRow = sheet.getLastRow();
    if (lastRow <= 1) return { ok: true, pageName: pageName, skipped: true, message: 'No data rows.' };

    var rowCount = lastRow - 1;
    var reasonValues = sheet.getRange(2, reasonCol, rowCount, 1).getValues();
    var fundValues = fundCol ? sheet.getRange(2, fundCol, rowCount, 1).getValues() : null;
    var techValues = techCol ? sheet.getRange(2, techCol, rowCount, 1).getValues() : null;
    var riskValues = riskCol ? sheet.getRange(2, riskCol, rowCount, 1).getValues() : null;
    var valValues = valCol ? sheet.getRange(2, valCol, rowCount, 1).getValues() : null;
    var recValues = recCol ? sheet.getRange(2, recCol, rowCount, 1).getValues() : null;
    var recDetailValues = recDetailCol ? sheet.getRange(2, recDetailCol, rowCount, 1).getValues() : null;

    var rowsProcessed = 0;
    var rowsParsed = 0;
    var fundWritten = 0;
    var techWritten = 0;
    var riskWritten = 0;
    var valWritten = 0;
    var recOverridden = 0;
    var alignedFromDetailCount = 0;
    var alignedFromReasonCount = 0;
    var sourceCounts = {};

    for (var i = 0; i < rowCount; i++) {
      var reason = String(reasonValues[i][0] == null ? '' : reasonValues[i][0]).trim();
      if (!reason) continue;
      rowsProcessed++;

      var parsed = _parseRecommendationReason_(reason);
      if (!parsed) continue;
      rowsParsed++;

      if (fundCol && parsed.fund && String(fundValues[i][0] == null ? '' : fundValues[i][0]).trim().toUpperCase() !== parsed.fund) { fundValues[i][0] = parsed.fund; fundWritten++; }
      if (techCol && parsed.tech && String(techValues[i][0] == null ? '' : techValues[i][0]).trim().toUpperCase() !== parsed.tech) { techValues[i][0] = parsed.tech; techWritten++; }
      if (riskCol && parsed.risk && String(riskValues[i][0] == null ? '' : riskValues[i][0]).trim().toUpperCase() !== parsed.risk) { riskValues[i][0] = parsed.risk; riskWritten++; }
      if (valCol && parsed.val && String(valValues[i][0] == null ? '' : valValues[i][0]).trim().toUpperCase() !== parsed.val) { valValues[i][0] = parsed.val; valWritten++; }

      if (recCol) {
        var mirrorRec = null;
        var mirrorSource = null;
        if (recDetailValues) {
          mirrorRec = _canonicalizeRecommendationLabel_(recDetailValues[i][0]);
          if (mirrorRec) { mirrorSource = 'detail'; alignedFromDetailCount++; }
        }
        if (!mirrorRec) {
          mirrorRec = _extractReasonPrefixRecommendation_(reason);
          if (mirrorRec) { mirrorSource = 'reason'; alignedFromReasonCount++; }
        }
        if (mirrorRec && mirrorSource) {
          sourceCounts[mirrorSource] = (sourceCounts[mirrorSource] || 0) + 1;
          if (_normalizeRecommendationForCompare_(recValues[i][0]) !== _normalizeRecommendationForCompare_(mirrorRec)) {
            recValues[i][0] = mirrorRec;
            recOverridden++;
          }
        }
      }
    }

    if (fundCol && fundValues && fundWritten > 0) sheet.getRange(2, fundCol, rowCount, 1).setValues(fundValues);
    if (techCol && techValues && techWritten > 0) sheet.getRange(2, techCol, rowCount, 1).setValues(techValues);
    if (riskCol && riskValues && riskWritten > 0) sheet.getRange(2, riskCol, rowCount, 1).setValues(riskValues);
    if (valCol && valValues && valWritten > 0) sheet.getRange(2, valCol, rowCount, 1).setValues(valValues);
    if (recCol && recValues && recOverridden > 0) sheet.getRange(2, recCol, rowCount, 1).setValues(recValues);

    var totalViewsWritten = fundWritten + techWritten + riskWritten + valWritten;
    var result = {
      ok: true,
      pageName: pageName,
      skipped: false,
      rowsProcessed: rowsProcessed,
      rowsParsed: rowsParsed,
      viewsWritten: totalViewsWritten,
      recommendationsOverridden: recOverridden,
      breakdown: { fundamentalView: fundWritten, technicalView: techWritten, riskView: riskWritten, valueView: valWritten, recommendation: recOverridden },
      columnsTouched: { fundamentalView: !!fundCol, technicalView: !!techCol, riskView: !!riskCol, valueView: !!valCol, recommendation: !!recCol, recommendationDetail: !!recDetailCol },
      alignedFromDetailCount: alignedFromDetailCount,
      alignedFromReasonCount: alignedFromReasonCount,
      recDetailColumnPresent: !!recDetailCol,
      sourceCounts: sourceCounts,
      canonicalDerivedCount: alignedFromDetailCount,
      viewDerivedFallbackCount: alignedFromReasonCount,
      suppressedByRiskGate: 0,
      suppressedByConfidenceGate: 0,
      ruleIdCounts: sourceCounts
    };

    var sourceSummary = [];
    for (var k in sourceCounts) if (sourceCounts.hasOwnProperty(k)) sourceSummary.push(k + '=' + sourceCounts[k]);
    Logger.log('[v1.4.0 RECO-GATE] page=' + pageName +
      ' rows-processed=' + rowsProcessed +
      ' canonical-derived=' + alignedFromDetailCount +
      ' view-fallback=' + alignedFromReasonCount +
      ' recommendations-overridden=' + recOverridden +
      ' suppressed-by-risk-gate=0' +
      ' suppressed-by-confidence-gate=0' +
      ' rule-id-counts={' + sourceSummary.join(' ') + '}');

    logRun_('INFO', 'applyViewTokensAndRecommendationOverride', pageName,
      'View tokens / recommendation mirror applied. parsed=' + rowsParsed + '/' + rowsProcessed + ' views=' + totalViewsWritten + ' overridden=' + recOverridden,
      { status: 'OK', details: result });

    return result;
  });
}


function _parseRecommendationReason_(reason) {
  if (reason === null || reason === undefined) return null;
  var s = String(reason);
  if (!s) return null;

  s = s.replace(/\u2192/g, '|||');
  s = s.replace(/-{1,2}>/g, '|||');
  s = s.replace(/=>/g, '|||');
  s = s.replace(/[\s]\bTHEN\b[\s]/gi, '|||');

  var sections = s.split('|||');
  var viewSection = sections[0] ? String(sections[0]) : '';
  var recSection = sections.length > 1 ? String(sections[sections.length - 1]) : '';

  var recommendation = recSection.replace(/^[\s\.,;:]+/, '').replace(/[\s\.,;:]+$/, '').replace(/[\s\u00A0]+/g, ' ').toUpperCase();
  if (recommendation && !SPECIAL_VALID_RECOMMENDATIONS_[recommendation]) {
    var firstToken = recommendation.split(/\s+/)[0];
    recommendation = firstToken && SPECIAL_VALID_RECOMMENDATIONS_[firstToken] ? firstToken : null;
  }

  var result = { fund: null, tech: null, risk: null, val: null, recommendation: recommendation || null };
  var pairs = viewSection.split('|');

  for (var i = 0; i < pairs.length; i++) {
    var pair = String(pairs[i] || '').trim();
    if (!pair) continue;
    var colonIdx = pair.indexOf(':');
    if (colonIdx < 1) continue;
    var label = pair.substring(0, colonIdx).trim().toUpperCase();
    var value = pair.substring(colonIdx + 1).trim().toUpperCase().replace(/[\s\.,;:]+$/, '');
    if (!label || !value) continue;

    if (label === 'FUND' || label === 'FUNDAMENTAL' || label === 'FUNDAMENTALS') result.fund = value;
    else if (label === 'TECH' || label === 'TECHNICAL' || label === 'TECHNICALS') result.tech = value;
    else if (label === 'RISK') result.risk = value;
    else if (label === 'VAL' || label === 'VALUE' || label === 'VALUATION') result.val = value;
  }

  return result;
}

function diagnoseRecommendationAccuracy_(pageName) {
  if (!pageName) return { ok: false, pageName: '', skipped: true, message: 'No pageName provided.' };

  var sheet;
  try { sheet = ensureSheetExists_(pageName); } catch (e) {
    return { ok: false, pageName: pageName, skipped: true, message: 'Sheet not found: ' + (e && e.message ? e.message : 'unknown error') };
  }

  var headers = readCurrentSheetHeaders_(sheet);
  if (!headers || !headers.length) return { ok: false, pageName: pageName, skipped: true, message: 'No headers on sheet.' };

  var headerMap = getHeaderMapSpecial_(headers);
  var recCol = specialResolveHeaderColumn_(headerMap, 'Recommendation');
  var recDetailCol = specialResolveHeaderColumn_(headerMap, 'Recommendation Detail');
  var reasonCol = specialResolveHeaderColumn_(headerMap, 'Recommendation Reason');
  var symbolCol = specialResolveHeaderColumn_(headerMap, 'Symbol');

  if (!recCol || (!recDetailCol && !reasonCol)) {
    return { ok: false, pageName: pageName, skipped: true, message: 'Page lacks Recommendation, or both Recommendation Detail and Recommendation Reason columns.' };
  }

  var lastRow = sheet.getLastRow();
  if (lastRow <= 1) return { ok: true, pageName: pageName, skipped: false, rowsScanned: 0, rowsMatching: 0, rowsDiffering: 0, rowsUnscored: 0, accuracyPct: 100.0, differences: [], topDiffPatterns: [] };

  var rowCount = lastRow - 1;
  var recValues = sheet.getRange(2, recCol, rowCount, 1).getValues();
  var recDetailValues = recDetailCol ? sheet.getRange(2, recDetailCol, rowCount, 1).getValues() : null;
  var reasonValues = reasonCol ? sheet.getRange(2, reasonCol, rowCount, 1).getValues() : null;
  var symbolValues = symbolCol ? sheet.getRange(2, symbolCol, rowCount, 1).getValues() : null;

  var rowsScanned = 0;
  var rowsMatching = 0;
  var rowsDiffering = 0;
  var rowsUnscored = 0;
  var differences = [];
  var patternCounts = {};
  var cap = 200;

  for (var i = 0; i < rowCount; i++) {
    var reference = null;
    var referenceSource = null;
    if (recDetailValues) {
      reference = _canonicalizeRecommendationLabel_(recDetailValues[i][0]);
      if (reference) referenceSource = 'detail';
    }
    if (!reference && reasonValues) {
      reference = _extractReasonPrefixRecommendation_(reasonValues[i][0]);
      if (reference) referenceSource = 'reason';
    }
    if (!reference) { rowsUnscored++; continue; }

    rowsScanned++;
    var referenceNorm = _normalizeRecommendationForCompare_(reference);
    var currentNorm = _normalizeRecommendationForCompare_(recValues[i][0]);
    if (currentNorm === referenceNorm) rowsMatching++;
    else {
      rowsDiffering++;
      var patternKey = (currentNorm || 'BLANK') + '->' + referenceNorm;
      patternCounts[patternKey] = (patternCounts[patternKey] || 0) + 1;
      if (differences.length < cap) differences.push({ row: i + 2, symbol: symbolValues ? String(symbolValues[i][0] || '') : '', current: currentNorm || '(blank)', canonical: referenceNorm, ruleId: referenceSource });
    }
  }

  var patternList = [];
  for (var p in patternCounts) if (patternCounts.hasOwnProperty(p)) patternList.push({ currentToCanonical: p, count: patternCounts[p] });
  patternList.sort(function (a, b) { return b.count - a.count; });

  var accuracyPct = rowsScanned > 0 ? Math.round((rowsMatching / rowsScanned) * 10000) / 100 : 100.0;
  var report = { ok: true, pageName: pageName, skipped: false, rowsScanned: rowsScanned, rowsMatching: rowsMatching, rowsDiffering: rowsDiffering, rowsUnscored: rowsUnscored, accuracyPct: accuracyPct, differences: differences, differencesCapped: rowsDiffering > cap, topDiffPatterns: patternList.slice(0, 10), backendModule: 'Recommendation == Recommendation Detail invariant' };

  Logger.log('[v1.4.0 RECO-ACCURACY] page=' + pageName + ' scanned=' + rowsScanned + ' matching=' + rowsMatching + ' differing=' + rowsDiffering + ' unscored=' + rowsUnscored + ' accuracy=' + accuracyPct + '%' + ' top-pattern=' + (patternList.length ? patternList[0].currentToCanonical + ':' + patternList[0].count : 'none'));

  logRun_(rowsDiffering === 0 ? 'INFO' : 'WARN', 'diagnoseRecommendationAccuracy', pageName,
    'Recommendation vs Detail agreement: ' + accuracyPct + '% (' + rowsMatching + '/' + rowsScanned + ' matching, ' + rowsDiffering + ' differing)',
    { status: rowsDiffering === 0 ? 'OK' : 'WARN', details: report });

  return report;
}

function applyViewTokensTop10_() { return applyViewTokensAndRecommendationOverride_(TOP_10_INVESTMENTS_PAGE_); }
function applyViewTokensInsights_() { return applyViewTokensAndRecommendationOverride_(INSIGHTS_ANALYSIS_PAGE_); }
function applyViewTokensForCurrentPage_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  if (!ss) return { ok: false, skipped: true, message: 'No active spreadsheet.' };
  var sheet = ss.getActiveSheet();
  if (!sheet) return { ok: false, skipped: true, message: 'No active sheet.' };
  return applyViewTokensAndRecommendationOverride_(sheet.getName());
}
function diagnoseV310Criteria_() { return diagnoseV310CriteriaVisibility_(INSIGHTS_ANALYSIS_PAGE_); }
function diagnoseInsightsDataQuality_() { return diagnoseDataQualityAlerts_(INSIGHTS_ANALYSIS_PAGE_); }
function diagnoseRecommendationAccuracyGlobalMarkets_() { return diagnoseRecommendationAccuracy_(SHEET_NAMES.GLOBAL_MARKETS); }
function diagnoseRecommendationAccuracyTop10_() { return diagnoseRecommendationAccuracy_(TOP_10_INVESTMENTS_PAGE_); }
function diagnoseRecommendationAccuracyForCurrentPage_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  if (!ss) return { ok: false, skipped: true, message: 'No active spreadsheet.' };
  var sheet = ss.getActiveSheet();
  if (!sheet) return { ok: false, skipped: true, message: 'No active sheet.' };
  return diagnoseRecommendationAccuracy_(sheet.getName());
}

/* =========================================================
 * Runtime-active symbol manager dispatcher
 * ========================================================= */

function manageSymbolsForPage_(pageName) {
  var normalized = typeof normalizePageName_ === 'function' ? normalizePageName_(pageName) : String(pageName || '').trim();

  if (normalized === SHEET_NAMES.MARKET_LEADERS && typeof manageMarketLeadersSymbols_ === 'function') return manageMarketLeadersSymbols_();
  if (normalized === SHEET_NAMES.GLOBAL_MARKETS && typeof manageGlobalMarketsSymbols_ === 'function') return manageGlobalMarketsSymbols_();
  if (normalized === SHEET_NAMES.COMMODITIES_FX && typeof manageCommoditiesFxSymbols_ === 'function') return manageCommoditiesFxSymbols_();
  if (normalized === SHEET_NAMES.MUTUAL_FUNDS && typeof manageMutualFundsSymbols_ === 'function') return manageMutualFundsSymbols_();
  if (normalized === SHEET_NAMES.MY_PORTFOLIO && typeof manageMyPortfolioSymbols_ === 'function') return manageMyPortfolioSymbols_();
  if (normalized === INSIGHTS_ANALYSIS_PAGE_) return manageInsightsSymbols_();
  if (normalized === TOP_10_INVESTMENTS_PAGE_) return manageTop10Symbols_();
  if (normalized === DATA_DICTIONARY_PAGE_) return manageDataDictionarySymbols_();

  var page = getPageConfig(normalized);
  var symbols = getWorkingSymbolsForPage_(page.pageName);
  var ui = SpreadsheetApp.getUi();
  ui.alert('Symbols - ' + page.pageName, symbols.length ? symbols.join(', ') : 'No symbols configured.', ui.ButtonSet.OK);
  return { ok: true, pageName: page.pageName, symbols: symbols };
}

function manageInsightsSymbols_() {
  var symbols = getDefaultSymbols(INSIGHTS_ANALYSIS_PAGE_);
  SpreadsheetApp.getUi().alert('Insights Analysis Symbols', symbols.length ? symbols.join(', ') : 'No symbols configured.', SpreadsheetApp.getUi().ButtonSet.OK);
  return { ok: true, pageName: INSIGHTS_ANALYSIS_PAGE_, symbols: symbols };
}

function manageTop10Symbols_() {
  var symbols = getDefaultSymbols(TOP_10_INVESTMENTS_PAGE_);
  SpreadsheetApp.getUi().alert('Top 10 Investments Symbols', symbols.length ? symbols.join(', ') : 'No symbols configured.', SpreadsheetApp.getUi().ButtonSet.OK);
  return { ok: true, pageName: TOP_10_INVESTMENTS_PAGE_, symbols: symbols };
}

function manageDataDictionarySymbols_() {
  SpreadsheetApp.getUi().alert('Data Dictionary', 'This page is a schema/reference page and does not use market symbols.', SpreadsheetApp.getUi().ButtonSet.OK);
  return { ok: true, pageName: DATA_DICTIONARY_PAGE_, symbols: [] };
}

/* =========================================================
 * Shared helpers
 * ========================================================= */

function normalizeSpecialSymbols_(symbols) {
  var safeInput = Array.isArray(symbols) ? symbols : [];
  return uniqueStrings_(safeInput.map(function (x) { return String(x || '').trim().toUpperCase(); }).filter(function (x) { return !!x; }));
}

function appendTop10FallbackWarning_(result) {
  if (!result || !result.endpointUsed) return result;
  var page = getPageConfig(TOP_10_INVESTMENTS_PAGE_);
  var primaryEndpoint = page.primaryEndpoint;
  if (result.endpointUsed === primaryEndpoint) return result;

  var sheet = ensureSheetExists_(TOP_10_INVESTMENTS_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var fallbackWarnings = uniqueStrings_((result.warnings || []).concat(['Fallback endpoint used: ' + result.endpointUsed]));

  // v1.7.1: Do not write a visible status panel into data sheets; use _Status / _Run_Log only.
  writePageStatus_(TOP_10_INVESTMENTS_PAGE_, { status: result.partial ? 'PARTIAL' : 'SUCCESS', message: 'Fallback endpoint used for Top_10_Investments: ' + result.endpointUsed, endpoint: result.endpointUsed, httpCode: '', rows: result.rowCount || '', columns: headers.length || '', durationMs: '', warnings: fallbackWarnings });

  logRun_('WARN', 'refreshTop10Fallback', TOP_10_INVESTMENTS_PAGE_, 'Fallback endpoint was used for Top_10_Investments: ' + result.endpointUsed, { status: 'WARN', endpoint: result.endpointUsed, warnings: fallbackWarnings, primaryEndpoint: primaryEndpoint });

  result.warnings = fallbackWarnings;
  result.usedFallback = true;
  result.fallbackEndpoint = result.endpointUsed;
  result.primaryEndpoint = primaryEndpoint;
  return result;
}

function appendInsightsFallbackWarning_(result) {
  if (!result || !result.endpointUsed) return result;
  var page = getPageConfig(INSIGHTS_ANALYSIS_PAGE_);
  var primaryEndpoint = page.primaryEndpoint;
  if (!primaryEndpoint || result.endpointUsed === primaryEndpoint) return result;

  var sheet = ensureSheetExists_(INSIGHTS_ANALYSIS_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var fallbackWarnings = uniqueStrings_((result.warnings || []).concat(['Fallback endpoint used: ' + result.endpointUsed]));

  // v1.7.1: Do not write a visible status panel into data sheets; use _Status / _Run_Log only.
  writePageStatus_(INSIGHTS_ANALYSIS_PAGE_, { status: result.partial ? 'PARTIAL' : 'SUCCESS', message: 'Fallback endpoint used for Insights_Analysis: ' + result.endpointUsed, endpoint: result.endpointUsed, httpCode: '', rows: result.rowCount || '', columns: headers.length || '', durationMs: '', warnings: fallbackWarnings });

  logRun_('WARN', 'refreshInsightsFallback', INSIGHTS_ANALYSIS_PAGE_, 'Fallback endpoint was used for Insights_Analysis: ' + result.endpointUsed, { status: 'WARN', endpoint: result.endpointUsed, warnings: fallbackWarnings, primaryEndpoint: primaryEndpoint });

  result.warnings = fallbackWarnings;
  result.usedFallback = true;
  result.fallbackEndpoint = result.endpointUsed;
  result.primaryEndpoint = primaryEndpoint;
  return result;
}

function readStatusPanelWarnings_(sheet, headersLength, pageName) {
  var startCol;
  try {
    if (typeof _normalizeStatusPanelStartCol_ === 'function') startCol = _normalizeStatusPanelStartCol_(sheet, headersLength, pageName || '');
  } catch (e) { startCol = null; }
  if (!startCol) startCol = Math.max(1, headersLength || sheet.getLastColumn()) + 2;

  try {
    var values = sheet.getRange(1, startCol, 4, 2).getValues();
    for (var i = 0; i < values.length; i++) if (String(values[i][0] || '') === 'Warnings') return String(values[i][1] || '');
  } catch (err) {}
  return '';
}

function getHeaderMapSpecial_(headers) {
  var safeHeaders = Array.isArray(headers) ? headers : [];
  var map = {};

  for (var i = 0; i < safeHeaders.length; i++) {
    var raw = String(safeHeaders[i] == null ? '' : safeHeaders[i]);
    var trimmed = raw.trim();
    var normalized = specialNormalizeHeader_(raw);
    if (raw) map[raw] = i + 1;
    if (trimmed) map[trimmed] = i + 1;
    if (normalized) {
      map[normalized] = i + 1;
      map[normalized.toLowerCase()] = i + 1;
    }
  }

  return map;
}

function specialNormalizeHeader_(text) {
  if (text === null || text === undefined) return '';
  if (typeof _normalizeHeaderText_ === 'function') return _normalizeHeaderText_(text);
  var s = String(text);
  s = s.replace(/[\u200B\u200C\u200D\uFEFF]/g, '');
  s = s.replace(/[\s\u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000]+/g, ' ');
  return s.trim();
}

function specialResolveHeaderColumn_(headerMap, headerName) {
  if (!headerMap || !headerName) return null;
  var raw = String(headerName);
  var trimmed = raw.trim();
  var normalized = specialNormalizeHeader_(raw);
  return headerMap[raw] || headerMap[trimmed] || headerMap[normalized] || headerMap[String(normalized).toLowerCase()] || null;
}

function _top10ResolveRequiredKey_(headerMap, headers, key) {
  if (!key) return null;

  var direct = specialResolveHeaderColumn_(headerMap, key);
  if (direct) return direct;

  var headerForKey = null;
  if (typeof KEY_TO_HEADER !== 'undefined' && KEY_TO_HEADER && typeof KEY_TO_HEADER === 'object' && KEY_TO_HEADER[key]) headerForKey = KEY_TO_HEADER[key];
  else if (typeof _coreKeyToHeader_ === 'function') {
    try { headerForKey = _coreKeyToHeader_(key); } catch (e) { headerForKey = null; }
  } else if (typeof getDisplayHeaderForKey_ === 'function') {
    try { headerForKey = getDisplayHeaderForKey_(TOP_10_INVESTMENTS_PAGE_, key); } catch (err) { headerForKey = null; }
  }

  if (headerForKey) {
    var byMappedHeader = specialResolveHeaderColumn_(headerMap, headerForKey);
    if (byMappedHeader) return byMappedHeader;
  }

  var keyLower = String(key).toLowerCase();
  for (var i = 0; i < headers.length; i++) {
    var h = String(headers[i] == null ? '' : headers[i]);
    var hNorm = specialNormalizeHeader_(h).toLowerCase();
    if (hNorm === keyLower || hNorm.replace(/\s+/g, '_') === keyLower) return i + 1;
  }

  return null;
}

function _specialCollectValidationWarnings_(result) {
  result = result || {};
  return []
    .concat(result.missingHeaders || [])
    .concat(result.extraHeaders || [])
    .concat(result.missingRequiredKeys || [])
    .concat(result.unmappedHeaders || [])
    .concat(result.warnings || []);
}

function _specialCellString_(row, col) {
  if (!row || !col) return '';
  var v = row[col - 1];
  if (v === null || v === undefined) return '';
  return String(v).trim();
}

function _specialIsBlank_(v) {
  return v === null || v === undefined || String(v).trim() === '';
}

function _specialToNumber_(value, fallback) {
  if (value === null || value === undefined || value === '') return fallback;
  if (typeof value === 'number') return (isNaN(value) || !isFinite(value)) ? fallback : value;
  var s = String(value).trim();
  if (!s) return fallback;
  var isPercent = s.indexOf('%') >= 0;
  s = s.replace(/%/g, '').replace(/,/g, '');
  var n = parseFloat(s);
  if (isNaN(n) || !isFinite(n)) return fallback;
  if (isPercent) return n / 100;
  return n;
}

function _specialToBoolean_(value, fallback) {
  if (value === null || value === undefined || value === '') return !!fallback;
  if (typeof value === 'boolean') return value;
  var s = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'y', 'on', 't', 'enabled', 'enable'].indexOf(s) >= 0) return true;
  if (['0', 'false', 'no', 'n', 'off', 'f', 'disabled', 'disable'].indexOf(s) >= 0) return false;
  return !!fallback;
}

function _specialParseDate_(value) {
  if (value === null || value === undefined || value === '') return null;
  if (Object.prototype.toString.call(value) === '[object Date]') {
    return isNaN(value.getTime()) ? null : value;
  }
  var s = String(value).trim();
  if (!s) return null;
  s = s.replace(' ', 'T');
  s = s.replace(/(\.\d{3})\d+/, '$1');
  var t = Date.parse(s);
  if (isNaN(t)) return null;
  return new Date(t);
}
