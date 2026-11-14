/**
 * 00_Config.gs
 * Tadawul Tracker — Core Configuration Registry
 * Version: 1.12.6
 *
 * PURPOSE
 * -------
 * Master configuration file for Tadawul Tracker / Tadawul Fast Bridge.
 *
 * v1.12.6 DECISION-LAYER WIRING (lockstep with Plan v5.0 P1-P9 GAS suite)
 * ------------------------------------------------------------------------
 * The decision cockpit suite (15_Lists_Config / 16_Decision_Top10 v1.0.0 /
 * 10_My_Portfolio v1.5.0 / 14_Portfolio_PnL v2.1.0 / 17_Selection_Log /
 * 18_Decision_Diagnostics) is live against backend routes v4.9.0. This
 * release wires it into the config layer. THREE ADDITIVE CHANGES (every
 * other byte identical to v1.12.5):
 *
 * 1. BACKEND/TOKEN ADAPTERS getBackendUrl_() / getAppToken_().
 *    ROOT CAUSE of the _Decision_Diagnostics WARNs "Backend URL hardcoded
 *    default" + "App token NOT SET": the decision modules resolve their
 *    backend via `typeof getBackendUrl_` / `typeof getAppToken_` probes
 *    first, then the BACKEND_URL / APP_TOKEN script properties — but this
 *    project's canonical accessors are getBaseUrl()/getApiToken() reading
 *    TFB_BASE_URL / TFB_TOKEN. No file defined the probed names, so every
 *    decision POST fell back to the hardcoded URL with NO token. The two
 *    one-line adapters below make the probes succeed, so 16/10/18 now use
 *    the SAME configured base URL and token as the legacy pipeline.
 *
 * 2. DECISION_SHEET_NAMES registry + DECISION_OWNED_PAGES_ declaration +
 *    isDecisionOwnedPage_() helper. Top_10_Investments is now RENDERED BY
 *    16_Decision_Top10.gs (cockpit layout: panel rows 1-15, dynamic zones)
 *    and must NOT be written by the legacy 118-column engine refresh —
 *    one "Refresh All Pages" would overwrite the cockpit. This file only
 *    DECLARES ownership (config is the contract authority); enforcement
 *    lands in the menu/refresh layer (01_Menu / 05_Refresh / 11_SpecialPages
 *    revisions), which consult isDecisionOwnedPage_ via typeof guards so
 *    deployment order never breaks.
 *
 * 3. debugVerifyConfigVersion_ bumped to v1.12.6 with additive guards:
 *    (a) adapters exist and return the configured values,
 *    (b) Top_10_Investments is decision-owned, instrument pages are not,
 *    (c) DECISION_SHEET_NAMES carries all six decision-layer sheets.
 *
 * v1.12.5 ANALYST/TREND COLUMN CLOSURE (lockstep with engine v5.85.3 + registry v2.15.0)
 * --------------------------------------------------------------------------------------
 * The 2026-06-11 Market_Leaders closure audit (engine v5.85.3 live, all 8
 * analyst/trend fields confirmed serving on /v1/analysis/sheet-rows) found two
 * frontend-only gaps:
 *
 * 1. ST SIGNAL BLANK ON EVERY ROW.
 *    ROOT CAUSE: HEADER_TO_KEY mapped 'ST Signal' -> 'short_term_signal', but
 *    the canonical contract key (schema_registry v2.15.0, col 75) is
 *    'st_signal'. The engine emits 'short_term_signal' only as a pre-projection
 *    alias; strict projection keeps 'st_signal' alone, so the column could
 *    never receive data. Remapped 'ST Signal' -> 'st_signal', with
 *    'st_signal' and legacy 'short_term_signal' retained as header-name
 *    variants pointing at the same key.
 *
 * 2. VOLATILITY 30D / 90D DISPLAYED 100x TOO SMALL (0.09% instead of 9.26%).
 *    ROOT CAUSE: volatility_30d / volatility_90d are correctly declared in
 *    DECIMAL_FRACTION_FIELDS (the engine emits fractions, e.g. 0.092627), but
 *    'Volatility 30D' / 'Volatility 90D' were missing from every page's
 *    formatting.percentColumns — so those columns never received true percent
 *    number format (which scales x100 on display) the way Expected ROI /
 *    Max Drawdown / VaR columns do. The raw fraction rendered with a literal
 *    percent suffix: 0.092627 -> "0.09%". Added both headers to percentColumns
 *    on Market_Leaders, Global_Markets, Commodities_FX, Mutual_Funds,
 *    My_Portfolio, and Top_10_Investments. Max Drawdown 1Y and VaR 95% (1D)
 *    were also added where absent for the same reason (they currently display
 *    correctly only on pages whose formatter falls through to true percent).
 *
 * CHANGES (every other byte identical to v1.12.4):
 *   1. HEADER_TO_KEY: 'ST Signal' -> 'st_signal'; added 'st_signal' and
 *      'short_term_signal' variant entries -> 'st_signal'.
 *   2. percentColumns: + 'Volatility 30D', 'Volatility 90D',
 *      'Max Drawdown 1Y', 'VaR 95% (1D)' on the six instrument pages.
 *   3. debugVerifyConfigVersion_ bumped to v1.12.5 with two additive guards:
 *      (a) 'ST Signal' maps to 'st_signal',
 *      (b) Volatility 30D/90D present in percentColumns on every instrument page.
 *
 * v1.12.4 INSIGHTS CONTRACT ALIGNMENT (lockstep with insights_builder v8.2.0)
 * --------------------------------------------------------------------------
 * ROOT CAUSE the live Insights_Analysis sheet kept serving the 2026-04-26
 * local fallback ("Generated locally because upstream insights payload was
 * unavailable") even though the backend route now returns real rows
 * (dispatch=insights_builder_real). The /sheet-rows owner (routes.advanced_analysis
 * v4.5.0) dispatches Insights_Analysis to the real builder, which emits the
 * v2.14.0 registry contract:
 *     Section | Item | Metric | Value | Notes | Source | Sort Order
 *     keys:   section, item, metric, value, notes, source, sort_order
 * But this config still pinned the PRE-v2.12 7-column shape
 *     Section | Item | Symbol | Metric | Value | Signal | Last Updated (Riyadh)
 * Same width (7) but three columns differ (Notes/Source/Sort Order vs
 * Symbol/Signal/Last Updated (Riyadh)). On refresh, 11_SpecialPages.gs
 * _ensureInsightsAnalysisHeadersReady_ "repaired" the sheet BACK to the old
 * shape, and HEADER_TO_KEY had no mapping for Notes/Source/Sort Order, so the
 * new-key rows could not be written — leaving the stale fallback content in
 * place. This release moves the config to the live backend contract.
 *
 * CHANGES (Insights_Analysis ONLY — every other page byte-identical to v1.12.3):
 *   1. Insights_Analysis fallbackHeaders ->
 *        ['Section','Item','Metric','Value','Notes','Source','Sort Order']
 *      (was ['Section','Item','Symbol','Metric','Value','Signal','Last Updated (Riyadh)']).
 *   2. HEADER_TO_KEY: added 'Notes'->'notes', 'Source'->'source',
 *      'Sort Order'->'sort_order' (Section/Item/Metric/Value already mapped).
 *      The legacy 'Signal'->'signal' and 'Last Updated (Riyadh)'->'last_updated_riyadh'
 *      mappings are RETAINED (used by instrument pages), they are simply no
 *      longer part of the Insights contract.
 *   3. expectedColumnCount stays 7 (width unchanged); Insights dateColumns
 *      cleared (no timestamp column in the new contract).
 *   4. debugVerifyConfigVersion_ bumped to v1.12.4 with two additive guards:
 *      (a) Insights fallbackHeaders match the live backend contract,
 *      (b) every Insights fallback header has a HEADER_TO_KEY mapping.
 *
 * v1.12.2 CLEANUP / ALIGNMENT
 * -----------------------------
 * - Updates verification messages to match the approved 115-column contract:
 *   the final canonical main-page column is Warnings, while Block Reason is
 *   an investability-gate field before the provider / timestamp block.
 * - Adds explicit guards that the canonical instrument fallback ends with
 *   Warnings, Top_10_Investments ends with Criteria Snapshot, and Block Reason
 *   is not the final canonical column.
 * - Updates diagnostic labels to v1.12.2. No schema width or mapping changes.
 *
 * v1.12.1 FIX
 * -----------
 * - Preserve-on-empty header alignment. SCORING_COLUMNS_ carried the legacy label
 *   'Reco Source', but the canonical instrument column is 'Recommendation Source'.
 *   The mismatch parked a dead entry in the preserve list and left the real
 *   'Recommendation Source' column OUT of the preserve-on-empty set (unlike every
 *   other scoring column). Renamed the entry to 'Recommendation Source' so it
 *   matches the canonical header. Counts are unchanged
 *   (SCORING_COLUMNS_ = 53, preserve-on-empty = 51). HEADER_TO_KEY already maps
 *   both labels to 'recommendation_source', so no mapping change was required.
 * - Added three additive guards to debugVerifyConfigVersion_:
 *     (a) every preserve-on-empty column exists in the canonical header set,
 *     (b) every canonical header has a HEADER_TO_KEY mapping,
 *     (c) every Top 10 header has a HEADER_TO_KEY mapping.
 *   These encode the invariants that catch silent header/key divergence on edits.
 *
 * v1.12.0 MAIN FIX
 * ----------------
 * 1. Corrects SCRIPT_VERSION to 1.12.0.
 * 2. Aligns all main investment pages to the current 115-column instrument schema.
 * 3. Upgrades Global_Markets from old 46-column Layout A to full 115-column output.
 * 4. Keeps Top_10_Investments at 118 columns:
 *      Top 10 Rank + 115 canonical instrument columns + Selection Reason + Criteria Snapshot.
 * 5. Preserves the v1.11.1 fix:
 *      Block Reason and Conflict Type are excluded from preserve-on-empty because
 *      empty string is authoritative and must clear stale old values.
 *
 * v1.12.3 PORTFOLIO DECISION LAYER (lockstep with engine v5.80.0)
 * --------------------------------------------------------------
 * 1. My_Portfolio widened 115 -> 122: the engine's canonical 115 + a 7-field
 *      decision block (Buy Date, Target Weight %, Actual Weight %, Weight Gap,
 *      Rebalance Action, Investor Decision, User Notes).
 * 2. My_Portfolio gets its OWN fallbackHeaders (_MY_PORTFOLIO_FALLBACK_HEADERS_)
 *      built from the engine's exact 122-header order, so this page's fallback
 *      matches what the engine emits (the shared canonical fallback omits the
 *      position/view columns the engine carries).
 * 3. HEADER_TO_KEY: added the engine's v5.80.0 header-name variants (old names
 *      retained for backward compatibility).
 * 4. preserveManualColumns/editableColumns: manual set is Position Qty, Avg Cost,
 *      Buy Date, User Notes, Target Weight %. Rebalance Action and Investor
 *      Decision are now ENGINE-COMPUTED and must NOT be preserved (preserving
 *      them would overwrite the engine's verdict on every refresh).
 * 5. DECIMAL_FRACTION_FIELDS: removed 'unrealized_pl_pct' (engine emits it as
 *      percent points, not a fraction) -- fixes a latent x100 display bug.
 * 6. My_Portfolio fallback symbols set to the configured book (1120/4013/7020).
 *
 * CURRENT EXPECTED WIDTHS
 * -----------------------
 * Market_Leaders       115
 * Global_Markets       115
 * Commodities_FX       115
 * Mutual_Funds         115
 * My_Portfolio         122
 * Top_10_Investments   118
 * Insights_Analysis      7
 * Data_Dictionary        9
 */

'use strict';

/* =========================================================
 * App / Menu / Sheet names
 * ========================================================= */

var SCRIPT_VERSION = '1.12.6';

var APP_INFO = Object.freeze({
  NAME: 'Tadawul Tracker',
  VERSION: SCRIPT_VERSION,
  BACKEND_NAME: 'Tadawul Fast Bridge',
  TIMEZONE: 'Asia/Riyadh'
});

var MENU_CONFIG = Object.freeze({
  ROOT: 'Tadawul Tracker',
  BUILD_CURRENT_PAGE: 'Build Current Page',
  REFRESH_CURRENT_PAGE: 'Refresh Current Page',
  REFRESH_ALL_PAGES: 'Refresh All Pages',
  REFRESH_ANALYSIS_AND_INSIGHTS: 'Refresh Analysis & Insights',
  UPDATE_CURRENT_HEADERS: 'Update Current Headers',
  VALIDATE_CURRENT_PAGE: 'Validate Current Page',
  REFORMAT_CURRENT_PAGE: 'Reformat Current Page',
  SORT_AND_RANK_CURRENT_PAGE: 'Sort & Rank Current Page',
  EXPORT_CURRENT_PAGE_CSV: 'Export Current Page CSV',
  TEST_HEALTH: 'Test Health',
  TEST_API: 'Test API',
  VIEW_LAST_RUN_LOG: 'View Last Run Log',
  ABOUT: 'About'
});

var SHEET_NAMES = Object.freeze({
  MARKET_LEADERS: 'Market_Leaders',
  GLOBAL_MARKETS: 'Global_Markets',
  COMMODITIES_FX: 'Commodities_FX',
  MUTUAL_FUNDS: 'Mutual_Funds',
  MY_PORTFOLIO: 'My_Portfolio',
  INSIGHTS_ANALYSIS: 'Insights_Analysis',
  TOP_10_INVESTMENTS: 'Top_10_Investments',
  DATA_DICTIONARY: 'Data_Dictionary'
});

var PAGE_ORDER = Object.freeze([
  SHEET_NAMES.MARKET_LEADERS,
  SHEET_NAMES.GLOBAL_MARKETS,
  SHEET_NAMES.COMMODITIES_FX,
  SHEET_NAMES.MUTUAL_FUNDS,
  SHEET_NAMES.MY_PORTFOLIO,
  SHEET_NAMES.INSIGHTS_ANALYSIS,
  SHEET_NAMES.TOP_10_INVESTMENTS,
  SHEET_NAMES.DATA_DICTIONARY
]);

/* =========================================================
 * v1.12.6 — Decision-layer registry
 * ========================================================= */

/* Sheets owned by the Plan v5.0 decision suite. Central naming registry;
 * the decision modules keep their own local constants (self-containment),
 * these MUST stay in lockstep if a sheet is ever renamed. */
var DECISION_SHEET_NAMES = Object.freeze({
  PORTFOLIO_DECISION: 'Portfolio_Decision',
  LISTS_CONFIG: '_Lists_Config',
  SELECTION_LOG: '_Selection_Log',
  PORTFOLIO_ACTION_LOG: '_Portfolio_Action_Log',
  DECISION_DIAGNOSTICS: '_Decision_Diagnostics',
  PORTFOLIO_COSTBASIS: '_Portfolio_CostBasis'
});

/* Pages whose SHEET is rendered by a decision cockpit module. The legacy
 * engine refresh (118-column dump) must NOT write these; doing so destroys
 * the cockpit layout. Refresh-layer modules consult isDecisionOwnedPage_
 * (via typeof guard) before any page write. My_Portfolio is NOT here: its
 * 122-column engine page remains the holdings source; the cockpit renders
 * on the separate Portfolio_Decision sheet. */
var DECISION_OWNED_PAGES_ = Object.freeze([
  SHEET_NAMES.TOP_10_INVESTMENTS
]);

function isDecisionOwnedPage_(pageName) {
  var s = sanitizeString_(pageName);
  if (!s) return false;
  var lower = s.toLowerCase();
  for (var i = 0; i < DECISION_OWNED_PAGES_.length; i++) {
    if (DECISION_OWNED_PAGES_[i].toLowerCase() === lower) return true;
  }
  return false;
}

var ENDPOINTS = Object.freeze({
  HEALTH: '/health',
  META: '/meta',
  OPENAPI: '/openapi.json',
  ROOT_SHEET_ROWS: '/sheet-rows',
  ANALYSIS_SHEET_ROWS: '/v1/analysis/sheet-rows',
  ADVANCED_SHEET_ROWS: '/v1/advanced/sheet-rows',
  ADVISOR_SHEET_ROWS: '/v1/advisor/sheet-rows',
  ENRICHED_SHEET_ROWS: '/v1/enriched/sheet-rows',
  SCHEMA_PAGES: '/v1/schema/pages',
  SCHEMA_SHEET_SPEC: '/v1/schema/sheet-spec',
  SCHEMA_SHEET_SPEC_ROOT: '/schema/sheet-spec',
  SCHEMA_DATA_DICTIONARY: '/v1/schema/data-dictionary'
});

var DEFAULTS = Object.freeze({
  TIMEOUT_SEC: 120,
  LIMIT: 200,
  MODE: 'live',
  LOG_SHEET_NAME: '_Run_Log',
  STATUS_SHEET_NAME: '_Status',
  CACHE_TTL_SEC: 300,
  PRESERVE_HEADER_ROWS: 1,
  SCHEMA_CHECK_MODE: 'min'
});

/* =========================================================
 * Preserve-on-empty configuration
 * ========================================================= */

var SCORING_COLUMNS_ = Object.freeze([
  'Risk Score',
  'Risk Bucket',
  'Valuation Score',
  'Forecast Price 1M',
  'Forecast Price 3M',
  'Forecast Price 12M',
  'Expected ROI 1M',
  'Expected ROI 3M',
  'Expected ROI 12M',
  'Forecast Confidence',
  'Confidence Score',
  'Confidence Bucket',
  'Value Score',
  'Quality Score',
  'Momentum Score',
  'Growth Score',
  'Overall Score',
  'Opportunity Score',
  'Rank (Overall)',
  'Recommendation',
  'Recommendation Reason',
  'Horizon Days',
  'Invest Period Label',

  'Sector-Adj Score',
  'Conviction Score',
  'Top Factors',
  'Top Risks',
  'Position Size Hint',

  'Recommendation Detail',
  'Reco Priority',

  'Candle Pattern',
  'Candle Signal',
  'Candle Strength',
  'Candle Confidence',
  'Recent Patterns (5D)',

  'Priority Band',
  'Recommendation Source',

  'Provider Rating',
  'Scoring Reco Source',
  'Scoring Schema Version',
  'Scoring Errors',
  'Opportunity Source',
  'Overall Score (Raw)',
  'Overall Penalty Factor',

  'Forecast Source',
  'Data Quality Score',
  'Forecast Reliability Score',
  'Provider/Engine Conflict',
  'Conflict Type',
  'Final Decision Basis',
  'Investability Status',
  'Final Action',
  'Block Reason'
]);

var SCORING_PRESERVE_ON_EMPTY_EXCLUDE_ = Object.freeze([
  'Block Reason',
  'Conflict Type'
]);

var SCORING_PRESERVE_ON_EMPTY_ = Object.freeze(SCORING_COLUMNS_.filter(function (col) {
  return SCORING_PRESERVE_ON_EMPTY_EXCLUDE_.indexOf(col) === -1;
}));

/* =========================================================
 * Fallback header contracts
 * ========================================================= */

var _LAYOUT_A_FALLBACK_HEADERS_ = Object.freeze([
  'Symbol',
  'Name',
  'Sector',
  'Industry',
  'Country',
  'Currency',
  'Exchange',
  'Asset Class',
  'Current Price',
  'Previous Close',
  'Open',
  'Day High',
  'Day Low',
  '52W High',
  '52W Low',
  'Price Change',
  'Percent Change',
  '52W Position %',
  'Volume',
  'Avg Volume 30D',
  'Market Cap',
  'P/E (TTM)',
  'P/E (Forward)',
  'EPS (TTM)',
  'Dividend Yield',
  'P/B',
  'P/S',
  'EV/EBITDA',
  'PEG',
  'Beta (5Y)',
  'Profit Margin',
  'Revenue Growth YoY',
  'RSI (14)',
  'Volatility 30D',
  'ATR 14',
  'Sharpe (1Y)',
  'Max DD 1Y %',
  'Fundamental View',
  'Technical View',
  'Risk View',
  'Value View',
  'Recommendation',
  'Recommendation Reason',
  'Forecast Confidence',
  'Upside %',
  'Intrinsic Value'
]);

var _CANONICAL_INSTRUMENT_FALLBACK_HEADERS_ = Object.freeze([
  'Symbol',
  'Name',
  'Asset Class',
  'Exchange',
  'Currency',
  'Country',
  'Sector',
  'Industry',
  'Current Price',
  'Previous Close',
  'Open',
  'Day High',
  'Day Low',
  '52W High',
  '52W Low',
  'Price Change',
  'Percent Change',
  '52W Position %',
  'Volume',
  'Avg Volume 10D',
  'Avg Volume 30D',
  'Market Cap',
  'Float Shares',
  'Beta (5Y)',
  'P/E (TTM)',
  'P/E (Forward)',
  'EPS (TTM)',
  'Dividend Yield',
  'Payout Ratio',
  'Revenue (TTM)',
  'Revenue Growth YoY',
  'Gross Margin',
  'Operating Margin',
  'Profit Margin',
  'Debt/Equity',
  'Free Cash Flow (TTM)',
  'RSI (14)',
  'Volatility 30D',
  'Volatility 90D',
  'Max Drawdown 1Y',
  'VaR 95% (1D)',
  'Sharpe (1Y)',
  'Risk Score',
  'Risk Bucket',
  'P/B',
  'P/S',
  'EV/EBITDA',
  'PEG',
  'Intrinsic Value',
  'Upside %',
  'Valuation Score',
  'Forecast Price 1M',
  'Forecast Price 3M',
  'Forecast Price 12M',
  'Expected ROI 1M',
  'Expected ROI 3M',
  'Expected ROI 12M',
  'Forecast Confidence',
  'Confidence Score',
  'Confidence Bucket',
  'Value Score',
  'Quality Score',
  'Momentum Score',
  'Growth Score',
  'Overall Score',
  'Opportunity Score',
  'Rank (Overall)',
  'Analyst Rating',
  'Target Price',
  'Upside/Downside %',
  'Signal',
  'Trend 1M',
  'Trend 3M',
  'Trend 12M',
  'ST Signal',
  'Recommendation',
  'Recommendation Detail',
  'Recommendation Reason',
  'Reco Priority',
  'Priority Band',
  'Recommendation Source',
  'Horizon Days',
  'Invest Period Label',
  'Sector-Adj Score',
  'Conviction Score',
  'Top Factors',
  'Top Risks',
  'Position Size Hint',
  'Candle Pattern',
  'Candle Signal',
  'Candle Strength',
  'Candle Confidence',
  'Recent Patterns (5D)',
  'Provider Rating',
  'Scoring Reco Source',
  'Scoring Schema Version',
  'Scoring Errors',
  'Opportunity Source',
  'Overall Score (Raw)',
  'Overall Penalty Factor',
  'Forecast Source',
  'Data Quality Score',
  'Forecast Reliability Score',
  'Provider/Engine Conflict',
  'Conflict Type',
  'Final Decision Basis',
  'Investability Status',
  'Final Action',
  'Block Reason',
  'Data Provider',
  'Provider Secondary',
  'Last Updated (UTC)',
  'Last Updated (Riyadh)',
  'Row Source',
  'Warnings'
]);

var _TOP10_FALLBACK_HEADERS_ = Object.freeze([
  'Top 10 Rank'
].concat(_CANONICAL_INSTRUMENT_FALLBACK_HEADERS_).concat([
  'Selection Reason',
  'Criteria Snapshot'
]));

/* v1.12.4: Insights_Analysis fallback now matches the live backend
 * (insights_builder v8.2.0 / registry v2.14.0) contract. */
var _INSIGHTS_FALLBACK_HEADERS_ = Object.freeze([
  'Section',
  'Item',
  'Metric',
  'Value',
  'Notes',
  'Source',
  'Sort Order'
]);

/* v1.12.3: My_Portfolio is the engine's canonical 115 + a 7-field portfolio
 * decision block (122 total). It is declared from the ENGINE's exact header
 * order (which carries the 6 position columns + 4 view columns) rather than
 * the shared _CANONICAL_INSTRUMENT_FALLBACK_HEADERS_, so this page's fallback
 * matches what the engine actually emits. Live render still uses the schema
 * endpoint; this is the schema-fetch-failure safety net. */
var _MY_PORTFOLIO_FALLBACK_HEADERS_ = Object.freeze([
  'Symbol',
  'Name',
  'Asset Class',
  'Exchange',
  'Currency',
  'Country',
  'Sector',
  'Industry',
  'Current Price',
  'Previous Close',
  'Open',
  'Day High',
  'Day Low',
  '52W High',
  '52W Low',
  'Price Change',
  'Percent Change',
  '52W Position %',
  'Volume',
  'Avg Volume 10D',
  'Avg Volume 30D',
  'Market Cap',
  'Float Shares',
  'Beta (5Y)',
  'P/E (TTM)',
  'P/E (Forward)',
  'EPS (TTM)',
  'Dividend Yield',
  'Payout Ratio',
  'Revenue (TTM)',
  'Revenue Growth YoY',
  'Gross Margin',
  'Operating Margin',
  'Profit Margin',
  'Debt/Equity',
  'Free Cash Flow (TTM)',
  'RSI (14)',
  'Volatility 30D',
  'Volatility 90D',
  'Max Drawdown 1Y',
  'VaR 95% (1D)',
  'Sharpe (1Y)',
  'Risk Score',
  'Risk Bucket',
  'P/B',
  'P/S',
  'EV/EBITDA',
  'PEG',
  'Intrinsic Value',
  'Upside %',
  'Valuation Score',
  'Forecast Price 1M',
  'Forecast Price 3M',
  'Forecast Price 12M',
  'Expected ROI 1M',
  'Expected ROI 3M',
  'Expected ROI 12M',
  'Forecast Confidence',
  'Confidence Score',
  'Confidence Bucket',
  'Value Score',
  'Quality Score',
  'Momentum Score',
  'Growth Score',
  'Overall Score',
  'Opportunity Score',
  'Rank (Overall)',
  'Fundamental View',
  'Technical View',
  'Risk View',
  'Value View',
  'Recommendation',
  'Recommendation Reason',
  'Horizon Days',
  'Invest Period Label',
  'Position Qty',
  'Avg Cost',
  'Position Cost',
  'Position Value',
  'Unrealized P/L',
  'Unrealized P/L %',
  'Data Provider',
  'Last Updated (UTC)',
  'Last Updated (Riyadh)',
  'Warnings',
  'Sector-Adj Score',
  'Conviction Score',
  'Top Factors',
  'Top Risks',
  'Position Size Hint',
  'Recommendation Detail',
  'Reco Priority',
  'Provider Rating',
  'Recommendation Source',
  'Priority Band',
  'Scoring Reco Source',
  'Scoring Schema Version',
  'Scoring Errors',
  'Opportunity Source',
  'Overall Score (Raw)',
  'Overall Penalty Factor',
  'Candle Pattern',
  'Candle Signal',
  'Candle Strength',
  'Candle Confidence',
  'Recent Patterns (5D)',
  'Forecast Source',
  'Data Quality Score',
  'Forecast Reliability Score',
  'Provider/Engine Conflict',
  'Conflict Type',
  'Final Decision Basis',
  'Investability Status',
  'Final Action',
  'Block Reason',
  'Buy Date',
  'Target Weight %',
  'Actual Weight %',
  'Weight Gap',
  'Rebalance Action',
  'Investor Decision',
  'User Notes'
]);

/* =========================================================
 * Header-to-key map
 * ========================================================= */

var HEADER_TO_KEY = Object.freeze({
  'Symbol': 'symbol',
  'Name': 'name',
  'Asset Class': 'asset_class',
  'Exchange': 'exchange',
  'Currency': 'currency',
  'Country': 'country',
  'Sector': 'sector',
  'Industry': 'industry',

  'Current Price': 'current_price',
  'Previous Close': 'previous_close',
  'Open': 'open_price',
  'Open Price': 'open_price',
  'Day Open': 'open_price',
  'Day High': 'day_high',
  'Day Low': 'day_low',
  '52W High': 'week_52_high',
  '52W Low': 'week_52_low',
  'Price Change': 'price_change',
  'Percent Change': 'percent_change',
  'Change %': 'percent_change',
  '52W Position %': 'week_52_position_pct',
  'week_52_position_pct': 'week_52_position_pct',
  'position_52w_pct': 'week_52_position_pct',
  '5D Change %': 'price_change_5d',

  'Volume': 'volume',
  'Avg Volume 10D': 'avg_volume_10d',
  'Avg Vol 10D': 'avg_volume_10d',
  'Avg Volume 30D': 'avg_volume_30d',
  'Avg Vol 30D': 'avg_volume_30d',
  'Volume Ratio': 'volume_ratio',
  'Market Cap': 'market_cap',
  'Float Shares': 'float_shares',

  'Beta (5Y)': 'beta_5y',
  'P/E (TTM)': 'pe_ttm',
  'P/E (Forward)': 'pe_forward',
  'P/E (Fwd)': 'pe_forward',
  'EPS (TTM)': 'eps_ttm',
  'Dividend Yield': 'dividend_yield',
  'Div Yield %': 'dividend_yield',
  'Payout Ratio': 'payout_ratio',
  'Payout Ratio %': 'payout_ratio',
  'Revenue (TTM)': 'revenue_ttm',
  'Revenue TTM': 'revenue_ttm',
  'Revenue Growth YoY': 'revenue_growth_yoy',
  'Rev Growth YoY %': 'revenue_growth_yoy',
  'Gross Margin': 'gross_margin',
  'Gross Margin %': 'gross_margin',
  'Operating Margin': 'operating_margin',
  'Op Margin %': 'operating_margin',
  'Profit Margin': 'profit_margin',
  'Net Margin %': 'profit_margin',
  'Debt/Equity': 'debt_to_equity',
  'D/E Ratio': 'debt_to_equity',
  'Free Cash Flow (TTM)': 'free_cash_flow_ttm',
  'FCF (TTM)': 'free_cash_flow_ttm',
  'ROE %': 'roe',
  'ROA %': 'roa',

  'RSI (14)': 'rsi_14',
  'RSI Signal': 'rsi_signal',
  'Tech Score': 'technical_score',
  'Day Range Pos %': 'day_range_position',
  'ATR 14': 'atr_14',
  'Volatility 30D': 'volatility_30d',
  'Volatility 30D %': 'volatility_30d',
  'Volatility 90D': 'volatility_90d',
  'Volatility 90D %': 'volatility_90d',
  'Max Drawdown 1Y': 'max_drawdown_1y',
  'Max DD 1Y %': 'max_drawdown_1y',
  'VaR 95% (1D)': 'var_95_1d',
  'Sharpe (1Y)': 'sharpe_1y',
  'Risk Score': 'risk_score',
  'Risk Bucket': 'risk_bucket',

  'P/B': 'pb_ratio',
  'P/B Ratio': 'pb_ratio',
  'P/S': 'ps_ratio',
  'P/S Ratio': 'ps_ratio',
  'EV/EBITDA': 'ev_ebitda',
  'EV / EBITDA': 'ev_ebitda',
  'EV/EBITDA Ratio': 'ev_ebitda',
  'PEG': 'peg_ratio',
  'PEG Ratio': 'peg_ratio',
  'Intrinsic Value': 'intrinsic_value',
  'Valuation Score': 'valuation_score',
  'Upside %': 'upside_pct',

  'Fundamental View': 'fundamental_view',
  'Technical View': 'technical_view',
  'Risk View': 'risk_view',
  'Value View': 'value_view',

  'Forecast Price 1M': 'forecast_price_1m',
  'Price Tgt 1M': 'forecast_price_1m',
  'Forecast Price 3M': 'forecast_price_3m',
  'Price Tgt 3M': 'forecast_price_3m',
  'Forecast Price 12M': 'forecast_price_12m',
  'Price Tgt 12M': 'forecast_price_12m',
  'Expected ROI 1M': 'expected_roi_1m',
  'ROI 1M %': 'expected_roi_1m',
  'Expected ROI 3M': 'expected_roi_3m',
  'ROI 3M %': 'expected_roi_3m',
  'Expected ROI 12M': 'expected_roi_12m',
  'ROI 12M %': 'expected_roi_12m',
  'Forecast Confidence': 'forecast_confidence',
  'AI Confidence': 'forecast_confidence',
  'Forecast Source': 'forecast_source',
  'forecast_source': 'forecast_source',

  'Confidence Score': 'confidence_score',
  'AI Confidence Score': 'confidence_score',
  'confidence_score': 'confidence_score',
  'Confidence': 'confidence',
  'Confidence Bucket': 'confidence_bucket',
  'Opportunity Score': 'opportunity_score',
  'Overall Score': 'overall_score',
  'Quality Score': 'quality_score',
  'Growth Score': 'growth_score',
  'Value Score': 'value_score',
  'Momentum Score': 'momentum_score',
  'Rank (Overall)': 'rank_overall',

  'Sector-Adj Score': 'sector_relative_score',
  'sector_relative_score': 'sector_relative_score',
  'Conviction Score': 'conviction_score',
  'conviction_score': 'conviction_score',
  'Top Factors': 'top_factors',
  'top_factors': 'top_factors',
  'Top Risks': 'top_risks',
  'top_risks': 'top_risks',
  'Position Size Hint': 'position_size_hint',
  'position_size_hint': 'position_size_hint',

  'Recommendation Detail': 'recommendation_detailed',
  'recommendation_detailed': 'recommendation_detailed',
  'Reco Priority': 'recommendation_priority',
  'recommendation_priority': 'recommendation_priority',

  'Candle Pattern': 'candlestick_pattern',
  'candlestick_pattern': 'candlestick_pattern',
  'Candle Signal': 'candlestick_signal',
  'candlestick_signal': 'candlestick_signal',
  'Candle Strength': 'candlestick_strength',
  'candlestick_strength': 'candlestick_strength',
  'Candle Confidence': 'candlestick_confidence',
  'candlestick_confidence': 'candlestick_confidence',
  'Recent Patterns (5D)': 'candlestick_patterns_recent',
  'candlestick_patterns_recent': 'candlestick_patterns_recent',

  'Priority Band': 'recommendation_priority_band',
  'recommendation_priority_band': 'recommendation_priority_band',
  'Reco Source': 'recommendation_source',
  'Recommendation Source': 'recommendation_source',
  'recommendation_source': 'recommendation_source',

  'Provider Rating': 'provider_rating',
  'provider_rating': 'provider_rating',
  'Scoring Reco Source': 'scoring_recommendation_source',
  'scoring_recommendation_source': 'scoring_recommendation_source',
  'Scoring Schema Version': 'scoring_schema_version',
  'scoring_schema_version': 'scoring_schema_version',
  'Scoring Errors': 'scoring_errors',
  'scoring_errors': 'scoring_errors',
  'Opportunity Source': 'opportunity_source',
  'opportunity_source': 'opportunity_source',
  'Overall Score (Raw)': 'overall_score_raw',
  'overall_score_raw': 'overall_score_raw',
  'Overall Penalty Factor': 'overall_penalty_factor',
  'overall_penalty_factor': 'overall_penalty_factor',

  'Data Quality Score': 'data_quality_score',
  'data_quality_score': 'data_quality_score',
  'Forecast Reliability Score': 'forecast_reliability_score',
  'forecast_reliability_score': 'forecast_reliability_score',
  'Provider/Engine Conflict': 'provider_engine_conflict',
  'provider_engine_conflict': 'provider_engine_conflict',
  'Conflict Type': 'conflict_type',
  'conflict_type': 'conflict_type',
  'Final Decision Basis': 'final_decision_basis',
  'final_decision_basis': 'final_decision_basis',
  'Investability Status': 'investability_status',
  'investability_status': 'investability_status',
  'Final Action': 'final_action',
  'final_action': 'final_action',
  'Block Reason': 'block_reason',
  'block_reason': 'block_reason',

  'Analyst Rating': 'analyst_rating',
  'Target Price': 'target_price',
  'Upside/Downside %': 'upside_downside_pct',
  'Recommendation': 'recommendation',
  'Recommendation Reason': 'recommendation_reason',
  'Reason': 'recommendation_reason',
  'Signal': 'signal',
  'Trend 1M': 'trend_1m',
  'Trend 3M': 'trend_3m',
  'Trend 12M': 'trend_12m',
  /* v1.12.5: 'ST Signal' now maps to the canonical contract key 'st_signal'
   * (schema_registry v2.15.0). The previous 'short_term_signal' target is an
   * engine-internal alias that strict projection strips, so the column never
   * received data. Both lowercase variants retained as header-name aliases. */
  'ST Signal': 'st_signal',
  'st_signal': 'st_signal',
  'short_term_signal': 'st_signal',
  'Horizon Days': 'horizon_days',
  'Horizon': 'horizon_days',
  'Invest Period Label': 'invest_period_label',

  'Position Qty': 'position_qty',
  'Qty': 'position_qty',
  'Avg Cost': 'avg_cost',
  'Position Cost': 'position_cost',
  'Position Value': 'position_value',
  'Unrealized P/L': 'unrealized_pl',
  'Unrealized P/L %': 'unrealized_pl_pct',
  'User Notes': 'user_notes',
  'Target Weight': 'target_weight',
  'Decision': 'decision',
  'Action Flag': 'action_flag',
  /* v1.12.3: engine v5.80.0 My_Portfolio decision-block header names (the old
   * names above are kept for backward compatibility with prior sheet layouts). */
  'Buy Date': 'buy_date',
  'Target Weight %': 'target_weight',
  'Actual Weight %': 'actual_weight',
  'Weight Gap': 'weight_gap',
  'Rebalance Action': 'action_flag',
  'Investor Decision': 'decision',

  'Section': 'section',
  'Item': 'item',
  'Metric': 'metric',
  'Value': 'value',
  /* v1.12.4: Insights_Analysis v2.14.0 contract columns (insights_builder v8.2.0). */
  'Notes': 'notes',
  'Source': 'source',
  'Sort Order': 'sort_order',
  'sort_order': 'sort_order',

  'Top 10 Rank': 'top10_rank',
  'top10_rank': 'top10_rank',
  'Selection Reason': 'selection_reason',
  'selection_reason': 'selection_reason',
  'Criteria Snapshot': 'criteria_snapshot',
  'criteria_snapshot': 'criteria_snapshot',

  'Data Provider': 'data_provider',
  'data_provider': 'data_provider',
  'provider_primary': 'data_provider',
  'Provider': 'data_provider',
  'Provider Secondary': 'provider_secondary',

  'Last Updated (UTC)': 'last_updated_utc',
  'last_updated_utc': 'last_updated_utc',
  'as_of_utc': 'last_updated_utc',
  'As Of (UTC)': 'last_updated_utc',
  'Updated At (UTC)': 'updated_at_utc',

  'Last Updated (Riyadh)': 'last_updated_riyadh',
  'last_updated_riyadh': 'last_updated_riyadh',
  'as_of_riyadh': 'last_updated_riyadh',
  'As Of (Riyadh)': 'last_updated_riyadh',
  'Updated At (Riyadh)': 'updated_at_riyadh',

  'Row Source': 'row_source',
  'Warnings': 'warnings'
});

/* =========================================================
 * Decimal-fraction fields
 * ========================================================= */

var DECIMAL_FRACTION_FIELDS = Object.freeze([
  'dividend_yield',
  'payout_ratio',
  'revenue_growth_yoy',
  'gross_margin',
  'operating_margin',
  'profit_margin',
  'volatility_30d',
  'volatility_90d',
  'max_drawdown_1y',
  'var_95_1d',
  'expected_roi_1m',
  'expected_roi_3m',
  'expected_roi_12m',
  'forecast_confidence'
]);

/* =========================================================
 * Page registry
 * ========================================================= */

var PAGE_REGISTRY = Object.freeze((function buildPageRegistry_() {
  return {
    [SHEET_NAMES.MARKET_LEADERS]: {
      pageName: SHEET_NAMES.MARKET_LEADERS,
      sheetName: SHEET_NAMES.MARKET_LEADERS,
      family: 'analysis',
      expectedColumnCount: 115,
      schemaCheckMode: 'min',
      allowPartial: false,
      allowEmptyRows: false,
      primaryEndpoint: ENDPOINTS.ANALYSIS_SHEET_ROWS,
      fallbackEndpoints: [],
      defaultMethod: 'POST',
      schemaEndpoint: ENDPOINTS.SCHEMA_SHEET_SPEC,
      schemaFallbackEndpoints: [ENDPOINTS.SCHEMA_SHEET_SPEC_ROOT],
      sort: {
        enabled: true,
        fieldProperty: 'SORT_FIELD_MARKET_LEADERS',
        fallbackField: 'Rank (Overall)',
        ascending: true
      },
      symbols: ['2222.SR', '1120.SR', '2010.SR', '7010.SR'],
      preserveManualColumns: [],
      preserveOnEmpty: SCORING_PRESERVE_ON_EMPTY_.slice(),
      headerOverrides: {},
      requiredKeys: [],
      formatting: {
        freezeRows: 5,
        filter: true,
        tabColor: '#0b8043',
        positiveColumns: ['Expected ROI 1M', 'Expected ROI 3M', 'Expected ROI 12M', 'Unrealized P/L', 'Unrealized P/L %'],
        negativeColumns: ['Risk Score'],
        percentColumns: [
          'Percent Change',
          '52W Position %',
          'Dividend Yield',
          'Payout Ratio',
          'Revenue Growth YoY',
          'Gross Margin',
          'Operating Margin',
          'Profit Margin',
          'Volatility 30D',
          'Volatility 90D',
          'Max Drawdown 1Y',
          'VaR 95% (1D)',
          'Expected ROI 1M',
          'Expected ROI 3M',
          'Expected ROI 12M',
          'Unrealized P/L %',
          'Upside %'
        ],
        dateColumns: ['Last Updated (UTC)', 'Last Updated (Riyadh)']
      },
      fallbackHeaders: _CANONICAL_INSTRUMENT_FALLBACK_HEADERS_.slice()
    },

    [SHEET_NAMES.GLOBAL_MARKETS]: {
      pageName: SHEET_NAMES.GLOBAL_MARKETS,
      sheetName: SHEET_NAMES.GLOBAL_MARKETS,
      family: 'analysis',
      expectedColumnCount: 115,
      schemaCheckMode: 'min',
      allowPartial: false,
      allowEmptyRows: false,
      primaryEndpoint: ENDPOINTS.ANALYSIS_SHEET_ROWS,
      fallbackEndpoints: [ENDPOINTS.ENRICHED_SHEET_ROWS],
      defaultMethod: 'POST',
      schemaEndpoint: ENDPOINTS.SCHEMA_SHEET_SPEC,
      schemaFallbackEndpoints: [ENDPOINTS.SCHEMA_SHEET_SPEC_ROOT],
      sort: {
        enabled: true,
        fieldProperty: 'SORT_FIELD_GLOBAL',
        fallbackField: 'Rank (Overall)',
        ascending: true
      },
      symbols: ['AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOGL'],
      preserveManualColumns: [],
      preserveOnEmpty: SCORING_PRESERVE_ON_EMPTY_.slice(),
      headerOverrides: {},
      requiredKeys: [],
      formatting: {
        freezeRows: 5,
        filter: true,
        tabColor: '#1a73e8',
        positiveColumns: ['Expected ROI 1M', 'Expected ROI 3M', 'Expected ROI 12M', 'Unrealized P/L', 'Unrealized P/L %'],
        negativeColumns: ['Risk Score'],
        percentColumns: [
          'Percent Change',
          '52W Position %',
          'Dividend Yield',
          'Payout Ratio',
          'Revenue Growth YoY',
          'Gross Margin',
          'Operating Margin',
          'Profit Margin',
          'Volatility 30D',
          'Volatility 90D',
          'Max Drawdown 1Y',
          'VaR 95% (1D)',
          'Expected ROI 1M',
          'Expected ROI 3M',
          'Expected ROI 12M',
          'Unrealized P/L %',
          'Upside %'
        ],
        dateColumns: ['Last Updated (UTC)', 'Last Updated (Riyadh)']
      },
      fallbackHeaders: _CANONICAL_INSTRUMENT_FALLBACK_HEADERS_.slice()
    },

    [SHEET_NAMES.COMMODITIES_FX]: {
      pageName: SHEET_NAMES.COMMODITIES_FX,
      sheetName: SHEET_NAMES.COMMODITIES_FX,
      family: 'analysis',
      expectedColumnCount: 115,
      schemaCheckMode: 'min',
      allowPartial: false,
      allowEmptyRows: false,
      primaryEndpoint: ENDPOINTS.ANALYSIS_SHEET_ROWS,
      fallbackEndpoints: [ENDPOINTS.ENRICHED_SHEET_ROWS, ENDPOINTS.ADVANCED_SHEET_ROWS],
      defaultMethod: 'POST',
      schemaEndpoint: ENDPOINTS.SCHEMA_SHEET_SPEC,
      schemaFallbackEndpoints: [ENDPOINTS.SCHEMA_SHEET_SPEC_ROOT],
      sort: {
        enabled: true,
        fieldProperty: '',
        fallbackField: 'Rank (Overall)',
        ascending: true
      },
      symbols: ['GC=F', 'BZ=F', 'SI=F', 'CL=F', 'EURUSD=X', 'GBPUSD=X', 'JPY=X', 'SAR=X'],
      preserveManualColumns: [],
      preserveOnEmpty: SCORING_PRESERVE_ON_EMPTY_.slice(),
      headerOverrides: {},
      requiredKeys: [],
      formatting: {
        freezeRows: 5,
        filter: true,
        tabColor: '#e37400',
        positiveColumns: ['Expected ROI 1M', 'Expected ROI 3M', 'Expected ROI 12M'],
        negativeColumns: ['Risk Score'],
        percentColumns: [
          'Percent Change',
          '52W Position %',
          'Volatility 30D',
          'Volatility 90D',
          'Max Drawdown 1Y',
          'VaR 95% (1D)',
          'Expected ROI 1M',
          'Expected ROI 3M',
          'Expected ROI 12M',
          'Upside %'
        ],
        dateColumns: ['Last Updated (UTC)', 'Last Updated (Riyadh)']
      },
      fallbackHeaders: _CANONICAL_INSTRUMENT_FALLBACK_HEADERS_.slice()
    },

    [SHEET_NAMES.MUTUAL_FUNDS]: {
      pageName: SHEET_NAMES.MUTUAL_FUNDS,
      sheetName: SHEET_NAMES.MUTUAL_FUNDS,
      family: 'analysis',
      expectedColumnCount: 115,
      schemaCheckMode: 'min',
      allowPartial: false,
      allowEmptyRows: false,
      primaryEndpoint: ENDPOINTS.ANALYSIS_SHEET_ROWS,
      fallbackEndpoints: [ENDPOINTS.ENRICHED_SHEET_ROWS],
      defaultMethod: 'POST',
      schemaEndpoint: ENDPOINTS.SCHEMA_SHEET_SPEC,
      schemaFallbackEndpoints: [ENDPOINTS.SCHEMA_SHEET_SPEC_ROOT],
      sort: {
        enabled: true,
        fieldProperty: 'SORT_FIELD_FUNDS',
        fallbackField: 'Rank (Overall)',
        ascending: true
      },
      symbols: ['SPY', 'QQQ', 'VTI', 'VOO', 'IWM'],
      preserveManualColumns: [],
      preserveOnEmpty: SCORING_PRESERVE_ON_EMPTY_.slice(),
      headerOverrides: {},
      requiredKeys: [],
      formatting: {
        freezeRows: 5,
        filter: true,
        tabColor: '#8e24aa',
        positiveColumns: ['Expected ROI 1M', 'Expected ROI 3M', 'Expected ROI 12M', 'Unrealized P/L', 'Unrealized P/L %'],
        negativeColumns: ['Risk Score'],
        percentColumns: [
          'Percent Change',
          '52W Position %',
          'Dividend Yield',
          'Volatility 30D',
          'Volatility 90D',
          'Max Drawdown 1Y',
          'VaR 95% (1D)',
          'Expected ROI 1M',
          'Expected ROI 3M',
          'Expected ROI 12M',
          'Unrealized P/L %',
          'Upside %'
        ],
        dateColumns: ['Last Updated (UTC)', 'Last Updated (Riyadh)']
      },
      fallbackHeaders: _CANONICAL_INSTRUMENT_FALLBACK_HEADERS_.slice()
    },

    [SHEET_NAMES.MY_PORTFOLIO]: {
      pageName: SHEET_NAMES.MY_PORTFOLIO,
      sheetName: SHEET_NAMES.MY_PORTFOLIO,
      family: 'advisor',
      expectedColumnCount: 122,
      schemaCheckMode: 'min',
      allowPartial: false,
      allowEmptyRows: false,
      primaryEndpoint: ENDPOINTS.ADVISOR_SHEET_ROWS,
      fallbackEndpoints: [ENDPOINTS.ANALYSIS_SHEET_ROWS],
      defaultMethod: 'POST',
      schemaEndpoint: ENDPOINTS.SCHEMA_SHEET_SPEC,
      schemaFallbackEndpoints: [ENDPOINTS.SCHEMA_SHEET_SPEC_ROOT],
      sort: {
        enabled: true,
        fieldProperty: '',
        fallbackField: 'Rank (Overall)',
        ascending: true
      },
      symbols: ['1120.SR', '4013.SR', '7020.SR'],
      preserveManualColumns: [
        'Position Qty',
        'Avg Cost',
        'Buy Date',
        'User Notes',
        'Target Weight %'
      ],
      preserveOnEmpty: SCORING_PRESERVE_ON_EMPTY_.slice(),
      headerOverrides: {},
      requiredKeys: [],
      formatting: {
        freezeRows: 5,
        filter: true,
        tabColor: '#c2185b',
        positiveColumns: ['Expected ROI 1M', 'Expected ROI 3M', 'Expected ROI 12M', 'Unrealized P/L', 'Unrealized P/L %'],
        negativeColumns: ['Risk Score'],
        percentColumns: [
          'Percent Change',
          '52W Position %',
          'Volatility 30D',
          'Volatility 90D',
          'Max Drawdown 1Y',
          'VaR 95% (1D)',
          'Expected ROI 1M',
          'Expected ROI 3M',
          'Expected ROI 12M',
          'Unrealized P/L %',
          'Upside %'
        ],
        dateColumns: ['Last Updated (UTC)', 'Last Updated (Riyadh)'],
        editableColumns: [
          'Position Qty',
          'Avg Cost',
          'Buy Date',
          'User Notes',
          'Target Weight %'
        ]
      },
      fallbackHeaders: _MY_PORTFOLIO_FALLBACK_HEADERS_.slice()
    },

    [SHEET_NAMES.INSIGHTS_ANALYSIS]: {
      pageName: SHEET_NAMES.INSIGHTS_ANALYSIS,
      sheetName: SHEET_NAMES.INSIGHTS_ANALYSIS,
      family: 'advanced',
      expectedColumnCount: 7,
      schemaCheckMode: 'min',
      allowPartial: true,
      allowEmptyRows: true,
      primaryEndpoint: ENDPOINTS.ADVANCED_SHEET_ROWS,
      fallbackEndpoints: [],
      defaultMethod: 'POST',
      schemaEndpoint: ENDPOINTS.SCHEMA_SHEET_SPEC,
      schemaFallbackEndpoints: [ENDPOINTS.SCHEMA_SHEET_SPEC_ROOT],
      sort: {
        enabled: false,
        fieldProperty: '',
        fallbackField: '',
        ascending: true
      },
      symbols: ['2222.SR', 'AAPL', 'GC=F', 'NVDA', 'MSFT'],
      preserveManualColumns: [],
      preserveOnEmpty: [],
      headerOverrides: {},
      requiredKeys: [],
      formatting: {
        freezeRows: 5,
        filter: true,
        tabColor: '#f9ab00',
        positiveColumns: [],
        negativeColumns: [],
        percentColumns: [],
        dateColumns: []
      },
      fallbackHeaders: _INSIGHTS_FALLBACK_HEADERS_.slice()
    },

    [SHEET_NAMES.TOP_10_INVESTMENTS]: {
      pageName: SHEET_NAMES.TOP_10_INVESTMENTS,
      sheetName: SHEET_NAMES.TOP_10_INVESTMENTS,
      family: 'advanced',
      expectedColumnCount: 118,
      schemaCheckMode: 'min',
      allowPartial: false,
      allowEmptyRows: false,
      primaryEndpoint: ENDPOINTS.ADVANCED_SHEET_ROWS,
      fallbackEndpoints: [ENDPOINTS.ROOT_SHEET_ROWS],
      defaultMethod: 'POST',
      schemaEndpoint: ENDPOINTS.SCHEMA_SHEET_SPEC,
      schemaFallbackEndpoints: [ENDPOINTS.SCHEMA_SHEET_SPEC_ROOT],
      sort: {
        enabled: true,
        fieldProperty: '',
        fallbackField: 'Top 10 Rank',
        ascending: true
      },
      symbols: ['2222.SR', '1120.SR', 'AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META'],
      preserveManualColumns: [],
      preserveOnEmpty: SCORING_PRESERVE_ON_EMPTY_.concat([
        'Top 10 Rank',
        'Selection Reason',
        'Criteria Snapshot'
      ]),
      headerOverrides: {},
      requiredKeys: ['top10_rank', 'selection_reason', 'criteria_snapshot'],
      formatting: {
        freezeRows: 5,
        filter: true,
        tabColor: '#34a853',
        positiveColumns: ['Expected ROI 1M', 'Expected ROI 3M', 'Expected ROI 12M'],
        negativeColumns: ['Risk Score'],
        percentColumns: [
          'Percent Change',
          'Volatility 30D',
          'Volatility 90D',
          'Max Drawdown 1Y',
          'VaR 95% (1D)',
          'Expected ROI 1M',
          'Expected ROI 3M',
          'Expected ROI 12M',
          'Upside %'
        ],
        dateColumns: ['Last Updated (UTC)', 'Last Updated (Riyadh)']
      },
      fallbackHeaders: _TOP10_FALLBACK_HEADERS_.slice()
    },

    [SHEET_NAMES.DATA_DICTIONARY]: {
      pageName: SHEET_NAMES.DATA_DICTIONARY,
      sheetName: SHEET_NAMES.DATA_DICTIONARY,
      family: 'schema',
      expectedColumnCount: 9,
      schemaCheckMode: 'min',
      allowPartial: true,
      allowEmptyRows: true,
      primaryEndpoint: ENDPOINTS.SCHEMA_DATA_DICTIONARY,
      fallbackEndpoints: [ENDPOINTS.SCHEMA_SHEET_SPEC, ENDPOINTS.SCHEMA_SHEET_SPEC_ROOT],
      defaultMethod: 'GET',
      schemaEndpoint: ENDPOINTS.SCHEMA_SHEET_SPEC,
      schemaFallbackEndpoints: [ENDPOINTS.SCHEMA_SHEET_SPEC_ROOT],
      sort: {
        enabled: false,
        fieldProperty: '',
        fallbackField: '',
        ascending: true
      },
      symbols: [],
      preserveManualColumns: [],
      preserveOnEmpty: [],
      headerOverrides: {},
      requiredKeys: [],
      formatting: {
        freezeRows: 1,
        filter: true,
        tabColor: '#5f6368',
        positiveColumns: [],
        negativeColumns: [],
        percentColumns: [],
        dateColumns: []
      },
      fallbackHeaders: []
    }
  };
})());

/* =========================================================
 * Runtime config helpers
 * ========================================================= */

var _cachedConfig = null;

function getConfig(forceRefresh) {
  forceRefresh = forceRefresh === true;

  if (_cachedConfig && !forceRefresh) {
    return _cachedConfig;
  }

  var props = PropertiesService.getScriptProperties();

  var cfg = {
    app: APP_INFO,
    menu: MENU_CONFIG,
    sheets: SHEET_NAMES,
    pageOrder: PAGE_ORDER.slice(),
    endpoints: ENDPOINTS,
    defaults: DEFAULTS,
    properties: {
      baseUrl: sanitizeBaseUrl_(props.getProperty('TFB_BASE_URL')),
      token: sanitizeToken_(props.getProperty('TFB_TOKEN')),
      timeoutSec: toPositiveInt_(props.getProperty('DEFAULT_TIMEOUT_SEC'), DEFAULTS.TIMEOUT_SEC),
      limit: toPositiveInt_(props.getProperty('DEFAULT_LIMIT'), DEFAULTS.LIMIT),
      mode: sanitizeString_(props.getProperty('DEFAULT_MODE')) || DEFAULTS.MODE,
      logSheetName: sanitizeString_(props.getProperty('LOG_SHEET_NAME')) || DEFAULTS.LOG_SHEET_NAME,
      statusSheetName: sanitizeString_(props.getProperty('STATUS_SHEET_NAME')) || DEFAULTS.STATUS_SHEET_NAME,
      insightsKeepLastGood: toBoolean_(props.getProperty('INSIGHTS_KEEP_LAST_GOOD'), true),
      sortFieldMarketLeaders: sanitizeString_(props.getProperty('SORT_FIELD_MARKET_LEADERS')) || '',
      sortFieldGlobal: sanitizeString_(props.getProperty('SORT_FIELD_GLOBAL')) || '',
      sortFieldFunds: sanitizeString_(props.getProperty('SORT_FIELD_FUNDS')) || '',
      schemaCheckMode: sanitizeSchemaCheckMode_(props.getProperty('SCHEMA_CHECK_MODE'))
    }
  };

  _cachedConfig = cfg;
  return cfg;
}

function getPageConfig(pageName) {
  var key = normalizePageName_(pageName);

  if (!key || !PAGE_REGISTRY[key]) {
    if (pageName === undefined || pageName === null || pageName === '') {
      throw new Error(
        'getPageConfig called with no pageName argument. ' +
        'Use the Tadawul Tracker menu or call a page-specific function.'
      );
    }

    throw new Error(
      'Unknown page: "' + pageName + '". Supported pages: ' +
      Object.keys(PAGE_REGISTRY).join(', ')
    );
  }

  var cfg = getConfig();
  var page = cloneObject_(PAGE_REGISTRY[key]);

  page.runtime = {
    baseUrl: cfg.properties.baseUrl,
    tokenLoaded: !!cfg.properties.token,
    timeoutSec: cfg.properties.timeoutSec,
    limit: cfg.properties.limit,
    mode: cfg.properties.mode,
    insightsKeepLastGood: cfg.properties.insightsKeepLastGood,
    schemaCheckMode: cfg.properties.schemaCheckMode
      || page.schemaCheckMode
      || DEFAULTS.SCHEMA_CHECK_MODE
  };

  if (page.pageName === SHEET_NAMES.MARKET_LEADERS && cfg.properties.sortFieldMarketLeaders) {
    page.sort.activeField = cfg.properties.sortFieldMarketLeaders;
  } else if (page.pageName === SHEET_NAMES.GLOBAL_MARKETS && cfg.properties.sortFieldGlobal) {
    page.sort.activeField = cfg.properties.sortFieldGlobal;
  } else if (page.pageName === SHEET_NAMES.MUTUAL_FUNDS && cfg.properties.sortFieldFunds) {
    page.sort.activeField = cfg.properties.sortFieldFunds;
  } else {
    page.sort.activeField = page.sort.fallbackField || '';
  }

  return page;
}

function getBaseUrl() {
  return getConfig().properties.baseUrl;
}

function getApiToken() {
  return getConfig().properties.token;
}

/* v1.12.6: backend/token adapters for the decision suite (16_Decision_Top10,
 * 10_My_Portfolio v1.5.0 decision layer, 18_Decision_Diagnostics). Those
 * modules probe `typeof getBackendUrl_` / `typeof getAppToken_` FIRST and
 * only then fall back to BACKEND_URL / APP_TOKEN script properties (names
 * this project does not use). These adapters route the probes onto the
 * canonical TFB_BASE_URL / TFB_TOKEN configuration so every decision POST
 * carries the same base URL and X-APP-TOKEN as the legacy pipeline. */
function getBackendUrl_() {
  return getBaseUrl();
}

function getAppToken_() {
  return getApiToken();
}

function getTimeoutMs() {
  return getConfig().properties.timeoutSec * 1000;
}

function getDefaultLimit() {
  return getConfig().properties.limit;
}

function getDefaultMode() {
  return getConfig().properties.mode;
}

function getLogSheetName() {
  return getConfig().properties.logSheetName;
}

function getStatusSheetName() {
  return getConfig().properties.statusSheetName;
}

function shouldKeepLastGoodInsightsData() {
  return !!getConfig().properties.insightsKeepLastGood;
}

function getSupportedPages() {
  return PAGE_ORDER.slice();
}

function getPageRegistry() {
  var out = {};
  PAGE_ORDER.forEach(function (pageName) {
    out[pageName] = getPageConfig(pageName);
  });
  return out;
}

function getFallbackHeaders(pageName) {
  var page = getPageConfig(pageName);
  return (page.fallbackHeaders || []).slice();
}

function getPreserveManualColumns(pageName) {
  var page = getPageConfig(pageName);
  return (page.preserveManualColumns || []).slice();
}

function getPreserveOnEmptyColumns(pageName) {
  var page = getPageConfig(pageName);
  return (page.preserveOnEmpty || []).slice();
}

function getRequiredKeys(pageName) {
  var page = getPageConfig(pageName);
  return (page.requiredKeys || []).slice();
}

function getDefaultSymbols(pageName) {
  var page = getPageConfig(pageName);
  return (page.symbols || []).slice();
}

function getEndpointCandidates(pageName) {
  var page = getPageConfig(pageName);
  return uniqueStrings_([page.primaryEndpoint].concat(page.fallbackEndpoints || []));
}

function getSchemaEndpointCandidates(pageName) {
  var page = getPageConfig(pageName);
  return uniqueStrings_([page.schemaEndpoint].concat(page.schemaFallbackEndpoints || []));
}

function getHeaderToKey(pageName) {
  var merged = Object.assign({}, HEADER_TO_KEY);

  if (pageName) {
    var page = getPageConfig(pageName);
    if (page.headerOverrides && typeof page.headerOverrides === 'object') {
      Object.assign(merged, page.headerOverrides);
    }
  }

  return merged;
}

function getDecimalFractionFields() {
  return DECIMAL_FRACTION_FIELDS.slice();
}

function getSchemaCheckMode(pageName) {
  var page = getPageConfig(pageName);
  return page.runtime.schemaCheckMode;
}

function buildBasePayload(pageName, overrides) {
  overrides = overrides || {};

  var page = getPageConfig(pageName);
  var symbols = (page.symbols || []).slice();

  var payload = {
    page: page.pageName,
    limit: page.runtime.limit,
    mode: page.runtime.mode,
    symbols: symbols
  };

  if (symbols.length > 0) {
    payload.tickers = symbols.slice();
    payload.direct_symbols = symbols.slice();
  }

  Object.assign(payload, overrides);
  return payload;
}

function validateRequiredConfig_() {
  if (!getBaseUrl()) {
    throw new Error('Missing Script Property: TFB_BASE_URL');
  }

  if (!getApiToken()) {
    throw new Error('Missing Script Property: TFB_TOKEN');
  }
}

function getConfigSummary_() {
  var cfg = getConfig();

  return {
    appName: cfg.app.NAME,
    version: cfg.app.VERSION,
    backendName: cfg.app.BACKEND_NAME,
    baseUrl: cfg.properties.baseUrl,
    tokenLoaded: !!cfg.properties.token,
    timeoutSec: cfg.properties.timeoutSec,
    defaultLimit: cfg.properties.limit,
    defaultMode: cfg.properties.mode,
    supportedPages: cfg.pageOrder.slice(),
    logSheetName: cfg.properties.logSheetName,
    statusSheetName: cfg.properties.statusSheetName,
    insightsKeepLastGood: cfg.properties.insightsKeepLastGood,
    schemaCheckMode: cfg.properties.schemaCheckMode || DEFAULTS.SCHEMA_CHECK_MODE,
    headerMapEntries: Object.keys(HEADER_TO_KEY).length,
    decimalFractionFields: DECIMAL_FRACTION_FIELDS.length,
    scoringColumnsCount: SCORING_COLUMNS_.length,
    canonicalFallbackHeaders: _CANONICAL_INSTRUMENT_FALLBACK_HEADERS_.length,
    top10FallbackHeaders: _TOP10_FALLBACK_HEADERS_.length,
    insightsFallbackHeaders: _INSIGHTS_FALLBACK_HEADERS_.length
  };
}

/* =========================================================
 * Utility helpers
 * ========================================================= */

function normalizePageName_(value) {
  var s = sanitizeString_(value);
  if (!s) return '';

  for (var i = 0; i < PAGE_ORDER.length; i++) {
    if (PAGE_ORDER[i] === s) return PAGE_ORDER[i];
  }

  var lowerS = s.toLowerCase();
  for (var j = 0; j < PAGE_ORDER.length; j++) {
    if (PAGE_ORDER[j].toLowerCase() === lowerS) return PAGE_ORDER[j];
  }

  return s;
}

function sanitizeBaseUrl_(value) {
  var s = sanitizeString_(value);
  if (!s) return '';
  return s.replace(/\/+$/, '');
}

function sanitizeToken_(value) {
  return sanitizeString_(value);
}

function sanitizeString_(value) {
  if (value === null || value === undefined) return '';
  return String(value).trim();
}

function toPositiveInt_(value, fallback) {
  var n = parseInt(value, 10);
  if (!isFinite(n) || isNaN(n) || n <= 0) return fallback;
  return n;
}

function toBoolean_(value, fallback) {
  if (value === null || value === undefined || value === '') return !!fallback;

  var s = String(value).trim().toLowerCase();

  if (['1', 'true', 'yes', 'y', 'on', 't', 'enabled', 'enable'].indexOf(s) >= 0) return true;
  if (['0', 'false', 'no', 'n', 'off', 'f', 'disabled', 'disable'].indexOf(s) >= 0) return false;

  return !!fallback;
}

function sanitizeSchemaCheckMode_(value) {
  var s = sanitizeString_(value).toLowerCase();
  if (s === 'min' || s === 'exact') return s;
  return '';
}

function uniqueStrings_(arr) {
  if (!Array.isArray(arr)) return [];

  var out = [];
  var seen = {};

  arr.forEach(function (v) {
    var s = sanitizeString_(v);
    if (!s || seen[s]) return;
    seen[s] = true;
    out.push(s);
  });

  return out;
}

function cloneObject_(obj) {
  return JSON.parse(JSON.stringify(obj));
}

/* =========================================================
 * Config verification
 * ========================================================= */

function debugVerifyConfigVersion_() {
  var checks = [];
  var pass = 0;
  var fail = 0;

  function check(label, condition, detail) {
    var status = condition ? 'PASS' : 'FAIL';
    if (condition) {
      pass++;
    } else {
      fail++;
    }
    checks.push(status + ' - ' + label + (detail ? ' (' + detail + ')' : ''));
  }

  check('SCRIPT_VERSION is 1.12.6', SCRIPT_VERSION === '1.12.6', 'actual=' + SCRIPT_VERSION);

  var expectedCounts = {
    'Market_Leaders': 115,
    'Global_Markets': 115,
    'Commodities_FX': 115,
    'Mutual_Funds': 115,
    'My_Portfolio': 122,
    'Insights_Analysis': 7,
    'Top_10_Investments': 118,
    'Data_Dictionary': 9
  };

  Object.keys(expectedCounts).forEach(function (pageName) {
    var actual = PAGE_REGISTRY[pageName] && PAGE_REGISTRY[pageName].expectedColumnCount;
    check(
      pageName + ' expectedColumnCount = ' + expectedCounts[pageName],
      actual === expectedCounts[pageName],
      'actual=' + actual
    );
  });

  check(
    '_CANONICAL_INSTRUMENT_FALLBACK_HEADERS_ has 115 headers',
    _CANONICAL_INSTRUMENT_FALLBACK_HEADERS_.length === 115,
    'actual=' + _CANONICAL_INSTRUMENT_FALLBACK_HEADERS_.length
  );

  check(
    '_TOP10_FALLBACK_HEADERS_ has 118 headers',
    _TOP10_FALLBACK_HEADERS_.length === 118,
    'actual=' + _TOP10_FALLBACK_HEADERS_.length
  );

  check(
    '_CANONICAL_INSTRUMENT_FALLBACK_HEADERS_ final header is Warnings',
    _CANONICAL_INSTRUMENT_FALLBACK_HEADERS_[_CANONICAL_INSTRUMENT_FALLBACK_HEADERS_.length - 1] === 'Warnings',
    'actual=' + _CANONICAL_INSTRUMENT_FALLBACK_HEADERS_[_CANONICAL_INSTRUMENT_FALLBACK_HEADERS_.length - 1]
  );

  check(
    'Block Reason is not the final canonical header',
    _CANONICAL_INSTRUMENT_FALLBACK_HEADERS_[_CANONICAL_INSTRUMENT_FALLBACK_HEADERS_.length - 1] !== 'Block Reason' &&
      _CANONICAL_INSTRUMENT_FALLBACK_HEADERS_.indexOf('Block Reason') >= 0,
    'blockReasonIndex=' + _CANONICAL_INSTRUMENT_FALLBACK_HEADERS_.indexOf('Block Reason')
  );

  check(
    '_TOP10_FALLBACK_HEADERS_ final header is Criteria Snapshot',
    _TOP10_FALLBACK_HEADERS_[_TOP10_FALLBACK_HEADERS_.length - 1] === 'Criteria Snapshot',
    'actual=' + _TOP10_FALLBACK_HEADERS_[_TOP10_FALLBACK_HEADERS_.length - 1]
  );

  /* v1.12.4: Insights_Analysis now tracks the live backend contract. */
  var _expectedInsightsHeaders = ['Section', 'Item', 'Metric', 'Value', 'Notes', 'Source', 'Sort Order'];
  var _insightsFallback = (PAGE_REGISTRY['Insights_Analysis'] && PAGE_REGISTRY['Insights_Analysis'].fallbackHeaders) || [];
  var _insightsMatch = _insightsFallback.length === _expectedInsightsHeaders.length &&
    _expectedInsightsHeaders.every(function (h, i) { return _insightsFallback[i] === h; });
  check(
    'Insights_Analysis fallbackHeaders match the live backend contract',
    _insightsMatch,
    'actual=' + JSON.stringify(_insightsFallback)
  );

  var _unmappedInsights = _expectedInsightsHeaders.filter(function (h) {
    return !Object.prototype.hasOwnProperty.call(HEADER_TO_KEY, h);
  });
  check(
    'every Insights fallback header has a HEADER_TO_KEY mapping',
    _unmappedInsights.length === 0,
    'unmapped=' + JSON.stringify(_unmappedInsights)
  );

  check(
    'SCORING_COLUMNS_ has 53 entries',
    SCORING_COLUMNS_.length === 53,
    'actual=' + SCORING_COLUMNS_.length
  );

  check(
    'SCORING_PRESERVE_ON_EMPTY_ excludes Block Reason',
    SCORING_PRESERVE_ON_EMPTY_.indexOf('Block Reason') === -1
  );

  check(
    'SCORING_PRESERVE_ON_EMPTY_ excludes Conflict Type',
    SCORING_PRESERVE_ON_EMPTY_.indexOf('Conflict Type') === -1
  );

  check(
    'DECIMAL_FRACTION_FIELDS excludes overall_penalty_factor',
    DECIMAL_FRACTION_FIELDS.indexOf('overall_penalty_factor') === -1
  );

  check(
    'DECIMAL_FRACTION_FIELDS excludes data_quality_score',
    DECIMAL_FRACTION_FIELDS.indexOf('data_quality_score') === -1
  );

  check(
    'DECIMAL_FRACTION_FIELDS excludes forecast_reliability_score',
    DECIMAL_FRACTION_FIELDS.indexOf('forecast_reliability_score') === -1
  );

  check(
    'Provider/Engine Conflict maps to provider_engine_conflict',
    HEADER_TO_KEY['Provider/Engine Conflict'] === 'provider_engine_conflict',
    'actual=' + HEADER_TO_KEY['Provider/Engine Conflict']
  );

  check(
    'Investability Status maps to investability_status',
    HEADER_TO_KEY['Investability Status'] === 'investability_status',
    'actual=' + HEADER_TO_KEY['Investability Status']
  );

  check(
    'Final Action maps to final_action',
    HEADER_TO_KEY['Final Action'] === 'final_action',
    'actual=' + HEADER_TO_KEY['Final Action']
  );

  check(
    'Block Reason maps to block_reason',
    HEADER_TO_KEY['Block Reason'] === 'block_reason',
    'actual=' + HEADER_TO_KEY['Block Reason']
  );

  check(
    'Notes maps to notes',
    HEADER_TO_KEY['Notes'] === 'notes',
    'actual=' + HEADER_TO_KEY['Notes']
  );

  check(
    'Source maps to source',
    HEADER_TO_KEY['Source'] === 'source',
    'actual=' + HEADER_TO_KEY['Source']
  );

  check(
    'Sort Order maps to sort_order',
    HEADER_TO_KEY['Sort Order'] === 'sort_order',
    'actual=' + HEADER_TO_KEY['Sort Order']
  );

  /* v1.12.5: analyst/trend closure guards. */
  check(
    'ST Signal maps to st_signal',
    HEADER_TO_KEY['ST Signal'] === 'st_signal',
    'actual=' + HEADER_TO_KEY['ST Signal']
  );

  var _volPages = ['Market_Leaders', 'Global_Markets', 'Commodities_FX', 'Mutual_Funds', 'My_Portfolio', 'Top_10_Investments'];
  var _volMissing = [];
  _volPages.forEach(function (pageName) {
    var pc = (PAGE_REGISTRY[pageName] && PAGE_REGISTRY[pageName].formatting && PAGE_REGISTRY[pageName].formatting.percentColumns) || [];
    ['Volatility 30D', 'Volatility 90D'].forEach(function (h) {
      if (pc.indexOf(h) === -1) {
        _volMissing.push(pageName + ':' + h);
      }
    });
  });
  check(
    'Volatility 30D/90D in percentColumns on every instrument page',
    _volMissing.length === 0,
    'missing=' + JSON.stringify(_volMissing)
  );

  var _deadPreserve = SCORING_PRESERVE_ON_EMPTY_.filter(function (col) {
    return _CANONICAL_INSTRUMENT_FALLBACK_HEADERS_.indexOf(col) === -1;
  });
  check(
    'every preserve-on-empty column exists in canonical headers',
    _deadPreserve.length === 0,
    'orphans=' + JSON.stringify(_deadPreserve)
  );

  var _unmappedCanonical = _CANONICAL_INSTRUMENT_FALLBACK_HEADERS_.filter(function (h) {
    return !Object.prototype.hasOwnProperty.call(HEADER_TO_KEY, h);
  });
  check(
    'every canonical header has a HEADER_TO_KEY mapping',
    _unmappedCanonical.length === 0,
    'unmapped=' + JSON.stringify(_unmappedCanonical)
  );

  var _unmappedTop10 = _TOP10_FALLBACK_HEADERS_.filter(function (h) {
    return !Object.prototype.hasOwnProperty.call(HEADER_TO_KEY, h);
  });
  check(
    'every Top 10 header has a HEADER_TO_KEY mapping',
    _unmappedTop10.length === 0,
    'unmapped=' + JSON.stringify(_unmappedTop10)
  );

  /* v1.12.6: decision-layer wiring guards. */
  check(
    'getBackendUrl_ adapter returns the configured base URL',
    typeof getBackendUrl_ === 'function' && getBackendUrl_() === getBaseUrl(),
    'actual=' + (typeof getBackendUrl_ === 'function' ? getBackendUrl_() : 'MISSING')
  );

  check(
    'getAppToken_ adapter returns the configured token',
    typeof getAppToken_ === 'function' && getAppToken_() === getApiToken(),
    'tokenLoaded=' + (typeof getAppToken_ === 'function' ? !!getAppToken_() : 'MISSING')
  );

  check(
    'Top_10_Investments is decision-owned',
    isDecisionOwnedPage_(SHEET_NAMES.TOP_10_INVESTMENTS) === true
  );

  check(
    'Market_Leaders is NOT decision-owned',
    isDecisionOwnedPage_(SHEET_NAMES.MARKET_LEADERS) === false
  );

  check(
    'My_Portfolio is NOT decision-owned (122-col engine page stays)',
    isDecisionOwnedPage_(SHEET_NAMES.MY_PORTFOLIO) === false
  );

  var _decisionSheetKeys = ['PORTFOLIO_DECISION', 'LISTS_CONFIG', 'SELECTION_LOG',
    'PORTFOLIO_ACTION_LOG', 'DECISION_DIAGNOSTICS', 'PORTFOLIO_COSTBASIS'];
  var _missingDecisionSheets = _decisionSheetKeys.filter(function (k) {
    return !DECISION_SHEET_NAMES[k];
  });
  check(
    'DECISION_SHEET_NAMES carries all six decision-layer sheets',
    _missingDecisionSheets.length === 0,
    'missing=' + JSON.stringify(_missingDecisionSheets)
  );

  Logger.log('=== debugVerifyConfigVersion_ - v1.12.6 ===');
  Logger.log('Total checks: ' + checks.length + ' | PASS: ' + pass + ' | FAIL: ' + fail);
  Logger.log('---');

  checks.forEach(function (line) {
    Logger.log(line);
  });

  Logger.log('---');

  if (fail === 0) {
    Logger.log('OK - ALL CHECKS PASSED - 00_Config.gs v1.12.6 deployed correctly.');
    Logger.log('Next steps:');
    Logger.log('1. Run tfbDecisionDiagnostics: the Backend URL and App token CONFIG checks must now PASS (adapters wired).');
    Logger.log('2. Do NOT run the legacy refresh on Top_10_Investments; it is decision-owned (cockpit). Enforcement arrives with the 01_Menu/05_Refresh revisions.');
    Logger.log('3. Main pages still 115 columns; My_Portfolio 122; Insights 7; Data_Dictionary 9.');
  } else {
    Logger.log('FAIL - ' + fail + ' check(s) failed. Review the log above.');
  }

  return {
    ok: fail === 0,
    pass: pass,
    fail: fail,
    total: checks.length,
    checks: checks
  };
}
