/**
 * 04_Format.gs
 * Tadawul Tracker — Formatting, Number Formats, Conditional Formatting, Validation
 * Version: 2.18.0
 *
 * Why v2.18.0 (vs v2.17.0) — EVIDENCE-DERIVED RISK BANDS (the v2.17.0 FIX 1
 * promise, delivered)
 * ─────────────────────────────────────────────────────────────────────────
 *
 *   v2.17.0 removed the FALSE green from the four fraction-scale risk columns
 *   and deliberately left them UNCOLOURED, gated on "bands derived from the
 *   live cross-sectional distribution — not guessed". The 2026-07-12 evening
 *   export supplied that distribution (n=4,662 rows across Market_Leaders +
 *   Global_Markets, winsorised at |v|<2.0 to exclude the known unadjusted-
 *   split garbage):
 *
 *       column            p25     p50     p75     p85     p95   green<  red>
 *       Volatility 30D   0.245   0.343   0.511   0.636   0.883   0.34   0.64
 *       Volatility 90D   0.258   0.357   0.516   0.636   0.873   0.36   0.64
 *       VaR 95% (1D)     0.024   0.031   0.044   0.053   0.072   0.03   0.05
 *       Max Drawdown 1Y  0.194   0.288   0.423   0.511   0.689   0.29   0.51
 *
 *   Band rule: green below the cross-sectional MEDIAN, red above p85, amber
 *   between. Relative-to-universe by construction — "red" means "riskier than
 *   85% of the live book's universe", not an arbitrary absolute.
 *
 *   TWO FACTS THE DATA SETTLED (both were open questions in v2.17.0):
 *     1. Volatility is ANNUALISED (median 0.343 == 34.3%; 30-day realised
 *        would centre far lower). The bands above are annualised bands.
 *     2. VaR and Max Drawdown are stored NEGATIVE in 81% of rows (loss
 *        convention) and positive in the rest — the sign is a CONVENTION,
 *        not information. Every rule therefore evaluates ABS(value) via
 *        whenFormulaSatisfied; a plain whenNumberGreaterThan on a negative
 *        MDD would have inverted the ramp (-0.9997 "less than" anything).
 *
 *   MECHANISM. NEW _NEGATIVE_RISK_FRACTION_BANDS_ maps each of the seven
 *   fraction columns (incl. the '%' aliases, same fraction storage) to its
 *   [green_lt, red_gt] pair. applyConditionalFormatting_ gains one loop that
 *   emits three whenFormulaSatisfied rules per present column:
 *
 *       red    =AND(ISNUMBER($X2), ABS($X2) >  red_gt)
 *       amber  =AND(ISNUMBER($X2), ABS($X2) >  green_lt, ABS($X2) <= red_gt)
 *       green  =AND(ISNUMBER($X2), ABS($X2) <= green_lt)
 *
 *   Column-absolute/row-relative ($X2) so the formula walks the range rows;
 *   ISNUMBER() keeps blanks unpainted. NEW helper _cfColLetterA1_ converts a
 *   1-based column index to its A1 letters. The columns are ALREADY in
 *   _MANAGED_HEADERS_SET_ (kept there by v2.17.0 exactly for this), so the
 *   sweep/idempotency machinery applies to the new rules unchanged. Unadjusted-
 *   split garbage (Volatility 15,068 == 1.5M%) paints red until data_engine_v2
 *   v5.115.0 nulls it upstream — red is the honest colour for garbage.
 *
 *   Risk Score is untouched (its 0-100 70/40 ramp was always correct). The
 *   v2.17.0 self-verify FIX-1 labels that read "no colour" are relabelled
 *   "ABS-banded (v2.18.0)"; new checks pin every band pair, the partition,
 *   and (reflectively) that the generator uses whenFormulaSatisfied + ABS.
 *
 * Why v2.17.0 (vs v2.16.0) — FALSE-GREEN RISK PAINT REMOVED + ZEBRA WIRED
 * ─────────────────────────────────────────────────────────────────────────
 *
 *   v2.17.0 FIX 1 — the fraction-scale risk columns no longer paint a FALSE
 *     "low risk" green. The v2.15.0 build correctly re-scaled Volatility 30D /
 *     90D to the fraction number-format but left the CONDITIONAL-FORMAT
 *     thresholds (>70 red / 40-70 amber / <40 green) assuming a 0-100 scale.
 *     Four of the eight _NEGATIVE_RISK_COLUMNS_ entries are 0-1 fractions and
 *     can therefore NEVER clear 40 — every value falls into whenNumberLessThan(40)
 *     and paints POSITIVE_BG. Verified by evaluating the generated rules:
 *
 *       Volatility 30D = 0.95 (95% vol)      -> whenNumberLessThan(40) -> GREEN
 *       Volatility 30D = 0.08 (8% vol)       -> whenNumberLessThan(40) -> GREEN
 *       VaR 95% (1D)   = 0.12 (12% 1-day)    -> whenNumberLessThan(40) -> GREEN
 *       Max Drawdown 1Y= 0.62 (62% drawdown) -> whenNumberLessThan(40) -> GREEN
 *       Risk Score     = 85   (true 0-100)   -> whenNumberGreaterThan(70) -> RED (correct)
 *
 *     The v2.15.0 header called this "uninformative". It is worse than that: a
 *     green cell ASSERTS low risk, so a name that lost 62% in a year renders
 *     the same green as a defensive utility. On a decision-support surface a
 *     wrong signal is strictly worse than no signal, so the rules are REMOVED
 *     for those columns rather than left painting a lie.
 *
 *     MECHANISM. _NEGATIVE_RISK_COLUMNS_ is SPLIT by scale:
 *       _NEGATIVE_RISK_POINTS_COLUMNS_   = ['Risk Score']  -> keeps the 70/40
 *         bands, which are correct for a 0-100 column, and is the ONLY list the
 *         conditional-format generator now iterates.
 *       _NEGATIVE_RISK_FRACTION_COLUMNS_ = VaR 95% (1D), Volatility 30D/90D
 *         (+ '%' aliases), Max Drawdown 1Y (+ alias) -> NO conditional-format
 *         colour until evidence-derived bands land (see v2.18.0 below).
 *
 *     *** CRITICAL — WHY _NEGATIVE_RISK_COLUMNS_ IS KEPT AS THE UNION ***
 *     _filterPreservedRules_ preserves any pre-existing rule that does NOT touch
 *     a column in _MANAGED_HEADERS_SET_. The stale v2.16.0 green/amber/red rules
 *     already sit on the fraction columns in every live sheet. If those columns
 *     were dropped from the union (and therefore from _MANAGED_HEADERS_SET_),
 *     the old rules would be misread as USER rules and PRESERVED FOREVER — the
 *     fix would silently no-op and the false green would survive. The union is
 *     therefore retained verbatim and still feeds _MANAGED_HEADERS_SET_; only
 *     the GENERATION loop narrows to the points list. Re-running the format pass
 *     sweeps the stale rules and emits none in their place.
 *
 *   v2.17.0 FIX 2 — the v2.16.0 reskin did not actually reskin the zebra.
 *     FORMAT_THEME_.ALT_ROW_BG was changed #F8FBFF -> #F5F7FA and asserted PASS
 *     by verifyFormatVersion_, but applyAlternatingRows_ never read it: it called
 *     range.applyRowBanding(SpreadsheetApp.BandingTheme.LIGHT_GREY), Google's
 *     stock theme. A read-tracking pass over the whole pipeline showed THREE
 *     theme keys were never consumed by any code path: ALT_ROW_BG, NEUTRAL_BG,
 *     NEUTRAL_FONT — and two of them were asserted green by the self-verify. A
 *     check that cannot fail is worse than no check (same defect class as the
 *     02_Core v2.9.0 self-verify that asserted a stale constant).
 *     FIX: applyAlternatingRows_ now applies the banding with header/footer bands
 *     OFF and sets the pair explicitly — setFirstRowColor(BASE_ROW_BG #FFFFFF) +
 *     setSecondRowColor(ALT_ROW_BG #F5F7FA) — so the promised proposal zebra is
 *     what actually renders. NEW theme key BASE_ROW_BG (the light half of the
 *     pair; it is READ, so it adds no dead value). Both calls are defensively
 *     wrapped: if the 3-arg applyRowBanding overload or the Banding colour
 *     setters are unavailable, the pass degrades to the v2.16.0 stock theme
 *     rather than throwing mid-format.
 *     NEUTRAL_BG / NEUTRAL_FONT are RETAINED (they are the shared cockpit palette
 *     reference) but are now explicitly LABELLED reference-only in both the theme
 *     and the self-verify, so no future reader mistakes their green check for
 *     evidence that something on the sheet is painted neutral.
 *
 *   v2.17.0 STAMP — SCRIPT_FORMAT_VERSION '2.16.0' -> '2.17.0'; verifyFormatVersion_
 *     version-stamp assertion -> '2.17.0', gains a FIX 1 scale-split block and a
 *     FIX 2 zebra-wiring block, and its palette checks are relabelled honestly.
 *
 *   NEXT (v2.18.0, evidence-gated) — real bands for the fraction-scale risk
 *     columns, derived from the cross-sectional distribution of Volatility 30D/90D,
 *     VaR 95% (1D) and Max Drawdown 1Y across the live universe (not guessed), and
 *     expressed sign-agnostically via ABS() so Max Drawdown's sign convention
 *     cannot invert them. Until that lands these four columns are deliberately
 *     uncoloured — the number they DISPLAY is correct (v2.15.0); only the paint
 *     is withheld.
 *
 * Why v2.16.0 (vs v2.15.0) — PROPOSAL-PALETTE THEME HARMONIZATION
 * ─────────────────────────────────────────────────────────────────────────
 *
 *   v2.16.0 RESKIN — PRESENTATION ONLY. Harmonizes FORMAT_THEME_ to the
 *     approved TFB_Final_Proposal navy/gold cockpit palette so the ~18 main
 *     instrument pages (Market_Leaders, Global_Markets, Commodities_FX,
 *     Mutual_Funds, etc.) visually match the two decision cockpits
 *     (16_Decision_Top10.gs v1.1.0 and 10_My_Portfolio.gs Portfolio_Decision
 *     v1.5.8). NOTHING about behaviour changes: every number-format list,
 *     conditional-format threshold / vocabulary / EXACT-match rule, column
 *     width, freeze, validation and the v2.15.0 volatility / v2.13.0
 *     monetary+weight+decision fixes are carried BYTE-FOR-BYTE. Only the
 *     hex values inside FORMAT_THEME_ change — i.e. the SAME conditional
 *     rules now paint with the proposal's verdict greens/ambers/reds, so a
 *     'BUY' cell on Market_Leaders is the same green as an 'ADD' cell on the
 *     cockpit. §8 honored (no schema / payload / sizing / gate / sort /
 *     column-count / verdict-LOGIC change). All 26 functions are verbatim.
 *
 *     FORMAT_THEME_ old -> new (from TFB_Final_Proposal):
 *       HEADER_BG      #1F4E78 -> #1F3A5F   (brand navy)
 *       HEADER_FONT    #FFFFFF -> #FFFFFF   (unchanged — already on-palette)
 *       BODY_FONT      #202124 -> #1A2433   (proposal ink)
 *       ALT_ROW_BG     #F8FBFF -> #F5F7FA   (proposal zebra)
 *       POSITIVE_BG    #E6F4EA -> #E6F4EA   (unchanged — already on-palette)
 *       POSITIVE_FONT  #137333 -> #146C43   (proposal ADD/INVEST green)
 *       NEGATIVE_BG    #FCE8E6 -> #FBE4E2   (proposal EXIT/DO_NOT_INVEST pink)
 *       NEGATIVE_FONT  #C5221F -> #B3261E   (proposal EXIT/DO_NOT_INVEST red)
 *       WARNING_BG     #FEF7E0 -> #FFF3CD   (proposal WATCH/TRIM amber bg)
 *       WARNING_FONT   #B06000 -> #9A6700   (proposal WATCH/TRIM amber)
 *       NEUTRAL_BG     #F1F3F4 -> #ECEEF1   (proposal BLOCK/other bg)
 *       NEUTRAL_FONT   #5F6368 -> #5B6470   (proposal BLOCK/other muted)
 *       EDITABLE_BG    #FFF8E1 -> #E8F0FE   (proposal operator-input blue bg)
 *       EDITABLE_FONT  #7A4F00 -> #1A57C2   (proposal operator-input blue)
 *       BORDER         #DADCE0 -> #D6DEE8   (proposal border)
 *     FONT_FAMILY (Verdana), HEADER_SIZE, HEADER_WEIGHT, BODY_SIZE: unchanged.
 *     Gold (#C8A04B) is the cockpit title-rule accent only and maps to no data
 *     column; the main pages have no title banner, so no gold is applied here.
 *
 *   v2.16.0 STAMP — SCRIPT_FORMAT_VERSION '2.15.0' -> '2.16.0';
 *     verifyFormatVersion_ version-stamp assertion -> '2.16.0' and gains a
 *     FORMAT_THEME_ proposal-palette assertion block.
 *
 * Why v2.15.0 (vs v2.14.0) — VOLATILITY PERCENT-SCALE FIX
 * ─────────────────────────────────────────────────────────────────────────
 *
 *   v2.15.0 FIX — Volatility 30D / 90D rendered ~100x too small. The engine
 *     emits 'Volatility 30D' / 'Volatility 90D' as FRACTIONS (0.10433 ==
 *     10.43%), the SAME scale as 'Max Drawdown 1Y' and 'VaR 95% (1D)'. They
 *     were in _KNOWN_POINTS_PERCENT_COLUMNS_, which forces the points format
 *     ('#,##0.00"%"'), so a stored fraction 0.10433 rendered as "0.10%"
 *     instead of "10.43%" (verified live: Market_Leaders Volatility 30D =
 *     0.10433 displaying "0.10%"). Drawdown / VaR were already correct because
 *     they sit in _KNOWN_FRACTION_PERCENT_COLUMNS_ ('0.00%', which x100s).
 *     FIX: moved 'Volatility 30D' / 'Volatility 90D' (and their '%' aliases)
 *     OUT of _KNOWN_POINTS_PERCENT_COLUMNS_ and INTO
 *     _KNOWN_FRACTION_PERCENT_COLUMNS_, so the fraction branch now renders
 *     "10.43%". They REMAIN in _PERCENT_COLUMNS_ (still percent-formatted) and
 *     in _NEGATIVE_RISK_COLUMNS_ (unchanged). This is a display/number-format
 *     change only — no value is rewritten.
 *
 *     *** KNOWN LIMITATION — SUPERSEDED BY v2.17.0 FIX 1 (the false green is
 *     now removed; bands remain pending v2.18.0). Original v2.15.0 text:
 *     (deliberately NOT fixed here — needs a risk-banding
 *     decision): the _NEGATIVE_RISK_COLUMNS_ conditional-format thresholds
 *     (>70 red / 40-70 amber / <40 green) assume a 0-100 scale. The now-
 *     correct 0-1 fraction columns (Volatility 30D/90D, VaR 95% (1D), Max
 *     Drawdown 1Y) therefore always fall under 40 and always paint green, so
 *     their risk COLORING is inert. The DISPLAYED number is now correct; the
 *     coloring stays uninformative until per-column fraction thresholds are
 *     chosen (Risk Score, a true 0-100 column, is unaffected and still colors
 *     correctly). Tracked for a follow-up CF pass. ***
 *
 *   v2.15.0 STAMP — SCRIPT_FORMAT_VERSION '2.14.0' -> '2.15.0';
 *     verifyFormatVersion_ gains a volatility fraction-scale assertion block.
 *
 * Why v2.14.0 (vs v2.13.0) — DECISION-LAYER WIRING + FORMAT PERSISTENCE
 * ─────────────────────────────────────────────────────────────────────────
 *
 *   v2.14.0 FIX 1 — decision-owned page guard (lockstep with 00_Config
 *     v1.12.6 / 01_Menu v2.10.0 / 03_Schema v2.7.0). Top_10_Investments is
 *     rendered by 16_Decision_Top10.gs (cockpit layout). Running
 *     applySheetFormatting_ on it would paint the cockpit title row dark
 *     blue, force theme fonts/widths/banding over the layout, REPLACE the
 *     cockpit's own conditional-format rules, and add a sheet filter. The
 *     01_Menu 'format' page-action is already blocked, but direct calls
 *     (script editor, loops, post-refresh passes) hit this function
 *     directly. applySheetFormatting_ now consults isDecisionOwnedPage_
 *     (typeof-guarded; degrades to legacy behavior if 00_Config v1.12.6 is
 *     absent) and returns action='skipped_decision_owned' without touching
 *     the sheet. Skip-not-throw so any batch caller continues.
 *
 *   v2.14.0 ENH 2 — formats persist for future rows. Number formats and
 *     conditional-format rule ranges previously stopped at the CURRENT
 *     last data row, so rows appended later inherited whatever stale
 *     format the cells carried — the exact mechanism behind the v2.13.0
 *     FIX A "Market Cap shows %" bug, just one refresh away from
 *     recurring. Both passes now extend to sheet.getMaxRows() (same
 *     extent applyValidations_ already used), so appended rows arrive
 *     pre-formatted. Base font/banding stays at the data extent (cheap,
 *     and empty rows need no theme paint).
 *
 *   v2.14.0 ENH 3 — filter kept in sync. applyBaseSheetFormatting_ used
 *     a bare createFilter() in try/catch: if a filter already existed the
 *     call silently failed and the filter range never grew with the data.
 *     The existing filter is now removed and recreated over the full
 *     sheet height each format pass (mirrors 03_Schema
 *     applyHeaderSheetSetup_).
 *
 *   v2.14.0 ENH 4 — frozen identity columns on wide pages. On 115/118/122
 *     column pages, scrolling right loses the Symbol. Pages with >= 20
 *     headers now get the first 2 columns frozen (Symbol + Name in the
 *     canonical layout). Narrow pages (Insights_Analysis 7 cols,
 *     Data_Dictionary) are untouched.
 *
 *   v2.14.0 STAMP — SCRIPT_FORMAT_VERSION '2.13.0' -> '2.14.0';
 *     verifyFormatVersion_ gains FIX 1 assertions and reports
 *     decisionOwnedWired.
 *
 * CURRENT CONTRACT (June 2026)
 * ─────────────────────────────────────────────────────────────────────────
 *   Main instrument pages        115 columns
 *   Top_10_Investments           118 columns (115 + 3 Top10 keys)
 *   My_Portfolio                 122 columns (115 + 7 engine-native
 *                                decision columns at 116-122)
 *
 *   Frontend  — 00_Config.gs v1.12.3 (canonical schema authority),
 *               10_My_Portfolio.gs v1.4.0, 14_Portfolio_PnL.gs v2.0.0
 *   Backend   — core/data_engine_v2 v5.80.0 (115 canonical + My_Portfolio
 *               decision layer _compute_portfolio_fields), schema_registry
 *               v2.14.0 (My_Portfolio 122), scoring.py v5.7.4
 *
 *   This module only ever styles columns that ALREADY exist on a sheet, so
 *   the same code serves 115 / 118 / 122 pages: extra-width pages simply
 *   match more header names, and absent headers are skipped.
 *
 * Why v2.13.0 (vs v2.12.0) — MONETARY-COLUMN FIX + 122 MY_PORTFOLIO ALIGNMENT
 * ─────────────────────────────────────────────────────────────────────────
 *
 *   v2.13.0 FIX A — large-magnitude monetary columns (the reported bug).
 *     'Market Cap', 'Revenue (TTM)' and 'Free Cash Flow (TTM)' were in NO
 *     number-format list, so applyNumberFormats_ never called setNumberFormat
 *     on them. They retained whatever stale format the cells already carried
 *     — a percent format — which rendered e.g. Market Cap 763593161887.09 as
 *     "763593161887.09%". They are now in _INTEGER_COLUMNS_ (plain '#,##0',
 *     same treatment as 'Float Shares'), which forces an explicit format that
 *     OVERWRITES the stale percent. Header-variant aliases (Market
 *     Capitalization / Mkt Cap / Revenue / Revenue TTM / Free Cash Flow / FCF
 *     / FCF (TTM)) are included; non-existent headers are skipped.
 *     *** These are absolute monetary values, NOT percents — they are kept
 *     out of every percent list (anti-percent guard). ***
 *
 *   v2.13.0 FIX B — My_Portfolio weight columns (engine cols 117/118/119).
 *     'Target Weight %', 'Actual Weight %' and 'Weight Gap' are written by
 *     data_engine_v2 _compute_portfolio_fields as PERCENT POINTS (40.0 ==
 *     40%). Added to _PERCENT_COLUMNS_ AND to _KNOWN_POINTS_PERCENT_COLUMNS_
 *     so they take the points format ('#,##0.00"%"' -> "40.00%") and NEVER
 *     the fraction branch (0.00% would x100-corrupt 40 -> 4000%). 'Weight
 *     Gap' (target - actual, may be negative) is additionally in
 *     _SIGNED_PERCENT_COLUMNS_ for the up/down-triangle signed display.
 *
 *   v2.13.0 FIX C — My_Portfolio decision columns (engine cols 120/121).
 *     'Rebalance Action' (drift-only: ADD / REDUCE / HOLD) and 'Investor
 *     Decision' (blended verdict: ADD / REDUCE / HOLD / SELL) are colour-
 *     coded ADD->green, HOLD->amber, REDUCE/SELL->red via EXACT-match rules
 *     (whenTextEqualTo, so 'ADD' never substring-matches), placed in
 *     _MEDIUM_COLUMNS_ (140px text), added to _MANAGED_HEADERS_SET_ for
 *     idempotent user-rule preservation, and given dropdown vocabularies
 *     (REBALANCE_ACTION_VOCAB_, INVESTOR_DECISION_VOCAB_, allowInvalid:true).
 *
 *   v2.13.0 FIX D — 'Buy Date' (engine col 116, manual input).
 *     New _DATE_ONLY_COLUMNS_ list with a date-only format ('yyyy-mm-dd'),
 *     distinct from the timestamp format used for the engine's Last Updated
 *     columns. 'User Notes' (col 122) is already covered by _WIDE_COLUMNS_
 *     and _STATUS_COLUMNS_.
 *
 *   v2.13.0 STAMP — SCRIPT_FORMAT_VERSION '2.12.0' -> '2.13.0';
 *     verifyFormatVersion_ gains FIX A/B/C/D assertion blocks.
 *
 * [PRESERVED — strictly] Every prior mechanic is carried forward unchanged:
 *   • FIX (v2.12.0) Top_10_Investments 'Top 10 Rank' display header in
 *     _NARROW_COLUMNS_ + _INTEGER_COLUMNS_.
 *   • Investability-gate formatting (v2.11.0): Investability Status (green/
 *     amber/red), Final Action (EXACT-match green/amber/red), Provider/Engine
 *     Conflict (TRUE->amber), Block Reason (non-empty->amber), Conflict Type
 *     / Final Decision Basis widths; Data Quality Score + Forecast
 *     Reliability Score as 0-100 plain '#,##0.00' in _RATIO_COLUMNS_ and held
 *     out of EVERY percent list (anti-percent guard — these are POINT scores,
 *     NOT percents; x100 would corrupt 73.4 -> 7340%).
 *   • Provider Rating as TEXT in _MEDIUM_COLUMNS_, never numeric (v2.10.0
 *     FIX 1 — the engine writes canonical 8-tier TOKEN STRINGS).
 *   • Forecast Source managed + dropdown (v2.10.0 FIX 2).
 *   • Candle Signal DOJI (v2.10.0 FIX 3).
 *   • Value View dual vocabulary CHEAP/UNDERVALUED + EXPENSIVE/OVERVALUED
 *     (v2.10.0 FIX 4).
 *   • 8-tier Recommendation + Recommendation Detail CF with STRONG_SELL-
 *     before-SELL precedence (v2.9.0).
 *   • Overall Penalty Factor fractional-multiplier guard; Candle Confidence
 *     null-display override; Priority Band vocab; all base/header/width/
 *     protection machinery — byte-identical.
 */

'use strict';

var SCRIPT_FORMAT_VERSION = '2.18.0';

/* =========================================================
 * Canonical vocabularies
 * ========================================================= */

var RECOMMENDATION_VOCAB_ = Object.freeze([
  'STRONG_BUY',
  'BUY',
  'HOLD',
  'REDUCE',
  'SELL',
  'ACCUMULATE',
  'STRONG_SELL',
  'AVOID'
]);

var RISK_BUCKET_VOCAB_ = Object.freeze(['Low', 'Moderate', 'High']);

var CONFIDENCE_BUCKET_VOCAB_ = Object.freeze(['Low', 'Medium', 'High']);

var FUNDAMENTAL_VIEW_VOCAB_ = Object.freeze(['BULLISH', 'NEUTRAL', 'BEARISH', 'N/A']);
var TECHNICAL_VIEW_VOCAB_   = Object.freeze(['BULLISH', 'NEUTRAL', 'BEARISH', 'N/A']);
var RISK_VIEW_VOCAB_        = Object.freeze(['LOW', 'MODERATE', 'HIGH', 'N/A']);

var VALUE_VIEW_VOCAB_       = Object.freeze([
  'CHEAP',
  'UNDERVALUED',
  'FAIR',
  'EXPENSIVE',
  'OVERVALUED',
  'N/A'
]);

var CANDLE_SIGNAL_VOCAB_    = Object.freeze([
  'BULLISH',
  'NEUTRAL',
  'BEARISH',
  'DOJI',
  'N/A'
]);

var FORECAST_SOURCE_VOCAB_ = Object.freeze([
  'provider_target',
  'phase_ii_synthetic',
  'fallback'
]);

var PRIORITY_BAND_VOCAB_    = Object.freeze(['P1', 'P2', 'P3', 'P4', 'P5']);

var INVESTABILITY_STATUS_VOCAB_ = Object.freeze([
  'INVESTABLE',
  'WATCHLIST',
  'BLOCKED'
]);

var FINAL_ACTION_VOCAB_ = Object.freeze([
  'INVEST',
  'WATCH',
  'DO_NOT_INVEST'
]);

var PROVIDER_ENGINE_CONFLICT_VOCAB_ = Object.freeze([
  'TRUE',
  'FALSE'
]);

// v2.13.0: My_Portfolio engine-native portfolio-decision vocabularies
// (data_engine_v2 v5.80.0 _compute_portfolio_fields). 'Rebalance Action'
// is drift-only (ADD / REDUCE / HOLD); 'Investor Decision' is the blended
// verdict and can additionally be SELL. Both are colour-coded via
// EXACT-match rules and offered as dropdowns (allowInvalid:true) like the
// other engine-written columns.
var REBALANCE_ACTION_VOCAB_ = Object.freeze([
  'ADD',
  'REDUCE',
  'HOLD'
]);

var INVESTOR_DECISION_VOCAB_ = Object.freeze([
  'ADD',
  'REDUCE',
  'HOLD',
  'SELL'
]);

var DEBUG_FORMAT_LOG_ = false;

/* =========================================================
 * Formatting theme
 * ========================================================= */

/* v2.16.0 RESKIN: FORMAT_THEME_ harmonized to the TFB_Final_Proposal navy/gold
 * cockpit palette. Hex values only — the conditional-format LOGIC, widths,
 * formats and validations are all unchanged, so the same rules simply paint
 * with the proposal colours (matching 16_Decision_Top10.gs / Portfolio_Decision).
 * See the v2.16.0 header block for the full old -> new map. §8 honored. */
var FORMAT_THEME_ = Object.freeze({
  FONT_FAMILY: 'Verdana',
  HEADER_BG: '#1F3A5F',          // was #1F4E78 — proposal brand navy
  HEADER_FONT: '#FFFFFF',
  HEADER_SIZE: 10,
  HEADER_WEIGHT: 'bold',

  BODY_FONT: '#1A2433',          // was #202124 — proposal ink
  BODY_SIZE: 10,
  BASE_ROW_BG: '#FFFFFF',        // v2.17.0: light half of the zebra pair (READ by applyAlternatingRows_)
  ALT_ROW_BG: '#F5F7FA',         // proposal zebra — v2.17.0 WIRED (v2.16.0 set it but never read it)

  POSITIVE_BG: '#E6F4EA',
  POSITIVE_FONT: '#146C43',      // was #137333 — proposal ADD/INVEST green

  NEGATIVE_BG: '#FBE4E2',        // was #FCE8E6 — proposal EXIT/DO_NOT_INVEST pink
  NEGATIVE_FONT: '#B3261E',      // was #C5221F — proposal EXIT/DO_NOT_INVEST red

  WARNING_BG: '#FFF3CD',         // was #FEF7E0 — proposal WATCH/TRIM amber bg
  WARNING_FONT: '#9A6700',       // was #B06000 — proposal WATCH/TRIM amber

  /* v2.17.0: NEUTRAL_* are PALETTE-REFERENCE ONLY — no rule in this file paints
   * neutral, so these values reach no cell. They are kept so the shared cockpit
   * palette stays in one place, and they are LABELLED as such in the self-verify
   * so a green check is never mistaken for evidence of on-sheet effect. */
  NEUTRAL_BG: '#ECEEF1',         // reference-only — proposal BLOCK/other bg
  NEUTRAL_FONT: '#5B6470',       // reference-only — proposal BLOCK/other muted

  EDITABLE_BG: '#E8F0FE',        // was #FFF8E1 — proposal operator-input blue bg
  EDITABLE_FONT: '#1A57C2',      // was #7A4F00 — proposal operator-input blue

  BORDER: '#D6DEE8'              // was #DADCE0 — proposal border
});

/* =========================================================
 * Module-level column lists
 * ========================================================= */

var _WIDE_COLUMNS_ = Object.freeze([
  'Name', 'Sector', 'Industry', 'Recommendation Reason', 'Reason',
  'selection_reason', 'criteria_snapshot', 'Notes', 'User Notes',
  'Warnings', 'Data Provider', 'Provider', 'Provider Secondary',
  'Item', 'Metric', 'Section',
  'Top Factors', 'Top Risks', 'Position Size Hint',
  'Recommendation Detail',
  'Recent Patterns (5D)',
  'Scoring Errors',
  'Scoring Schema Version',
  'Conflict Type',
  'Block Reason'
]);

var _MEDIUM_COLUMNS_ = Object.freeze([
  'Symbol', 'Exchange', 'Country', 'Asset Class', 'Recommendation',
  'Risk Bucket', 'Confidence Bucket', 'Last Updated (UTC)',
  'Last Updated (Riyadh)', 'As Of (UTC)', 'As Of (Riyadh)',
  'Updated At (UTC)', 'Updated At (Riyadh)', 'Invest Period Label',
  'Analyst Rating', 'Signal', 'ST Signal', 'Fundamental View',
  'Technical View', 'Risk View', 'Value View',
  'Candle Pattern',
  'Candle Signal',
  'Candle Strength',
  'Reco Source',
  'Scoring Reco Source',
  'Opportunity Source',
  'Provider Rating',
  'Forecast Source',
  'Investability Status',
  'Final Action',
  'Provider/Engine Conflict',
  'Final Decision Basis',
  // v2.13.0 FIX C: My_Portfolio engine-native decision tokens (text width).
  'Rebalance Action',
  'Investor Decision'
]);

var _NARROW_COLUMNS_ = Object.freeze([
  'Rank (Overall)', 'Top 10 Rank', 'top10_rank', 'Position Qty', 'Qty',
  'Horizon Days', 'Horizon',
  'Reco Priority',
  'Priority Band',
  'Overall Score (Raw)',
  'Overall Penalty Factor'
]);

var _PRICE_COLUMNS_ = Object.freeze([
  'Current Price', 'Previous Close', 'Open', 'Day High', 'Day Low',
  '52W High', '52W Low', 'Forecast Price 1M', 'Price Tgt 1M',
  'Forecast Price 3M', 'Price Tgt 3M', 'Forecast Price 12M',
  'Price Tgt 12M', 'Intrinsic Value', 'Avg Cost', 'Position Cost',
  'Position Value', 'Unrealized P/L', 'Value', 'ATR 14', 'Target Price'
]);

var _INTEGER_COLUMNS_ = Object.freeze([
  'Volume', 'Avg Volume 10D', 'Avg Vol 10D', 'Avg Volume 30D',
  'Avg Vol 30D', 'Float Shares', 'Horizon Days', 'Horizon',
  'Position Qty', 'Qty', 'Rank (Overall)', 'Top 10 Rank', 'top10_rank',
  'Reco Priority',
  // v2.13.0 FIX A: large-magnitude monetary columns. These were in NO
  // number-format list, so applyNumberFormats_ never set a format on them
  // and they kept whatever stale format the cells carried — a percent
  // format, which rendered e.g. Market Cap 763593161887.09 as
  // "763593161887.09%". Placing them here forces an explicit '#,##0'
  // (matching 'Float Shares'), overwriting the stale percent. Aliases
  // cover header variants across pages; non-existent headers are skipped.
  'Market Cap', 'Market Capitalization', 'Mkt Cap',
  'Revenue (TTM)', 'Revenue', 'Revenue TTM',
  'Free Cash Flow (TTM)', 'Free Cash Flow', 'FCF', 'FCF (TTM)'
]);

var _RATIO_COLUMNS_ = Object.freeze([
  'P/E (TTM)', 'P/E (Forward)', 'P/E (Fwd)', 'EPS (TTM)', 'Beta (5Y)',
  'P/B', 'P/S', 'EV/EBITDA', 'PEG', 'PEG Ratio', 'Debt/Equity',
  'D/E Ratio', 'Sharpe (1Y)', 'Risk Score', 'Valuation Score',
  'Confidence Score', 'Value Score', 'Quality Score', 'Momentum Score',
  'Growth Score', 'Overall Score', 'Opportunity Score', 'Tech Score',
  'Data Quality Score', 'Forecast Reliability Score', 'Volume Ratio',
  'Conviction Score', 'Sector-Adj Score',
  'RSI (14)', 'RSI 14', 'RSI', 'RSI(14)',
  'Candle Confidence',
  'Overall Score (Raw)'
]);

var _PERCENT_COLUMNS_ = Object.freeze([
  'Percent Change', 'Change %', '52W Position %', '5D Change %',
  'Day Range Pos %', 'ROE %', 'ROA %', 'Upside %', 'Upside/Downside %',
  'Dividend Yield', 'Div Yield %', 'Payout Ratio', 'Payout Ratio %',
  'Revenue Growth YoY', 'Rev Growth YoY %', 'Gross Margin',
  'Gross Margin %', 'Operating Margin', 'Op Margin %', 'Profit Margin',
  'Net Margin', 'Net Margin %', 'Volatility 30D', 'Volatility 30D %',
  'Volatility 90D', 'Volatility 90D %', 'Max Drawdown 1Y',
  'Max DD 1Y %', 'VaR 95% (1D)', 'Forecast Confidence', 'AI Confidence',
  'Expected ROI 1M', 'ROI 1M %', 'Expected ROI 3M', 'ROI 3M %',
  'Expected ROI 12M', 'ROI 12M %', 'Unrealized P/L %',
  // v2.13.0 FIX B: My_Portfolio engine-native weight columns. Stored as
  // PERCENT POINTS (40.0 == 40%) by data_engine_v2 _compute_portfolio_fields,
  // so they take the points-percent format (#,##0.00"%") via the
  // _KNOWN_POINTS_PERCENT_COLUMNS_ override below — never the fraction
  // branch (0.00% would x100-corrupt 40 -> 4000%).
  'Target Weight %', 'Actual Weight %', 'Weight Gap'
]);

var _SIGNED_PERCENT_COLUMNS_ = Object.freeze([
  'Percent Change', 'Change %', '5D Change %', 'Expected ROI 1M',
  'ROI 1M %', 'Expected ROI 3M', 'ROI 3M %', 'Expected ROI 12M',
  'ROI 12M %', 'Unrealized P/L %', 'Upside %', 'Upside/Downside %',
  // v2.13.0 FIX B: Weight Gap (target - actual) is signed -> gets the
  // signed points format (the up/down triangles). Target/Actual are >= 0,
  // so they stay in the plain points format from _PERCENT_COLUMNS_.
  'Weight Gap'
]);

var _SIGNED_NUMBER_COLUMNS_ = Object.freeze([
  'Price Change', 'Unrealized P/L'
]);

var _DATE_COLUMNS_ = Object.freeze([
  'Last Updated (UTC)', 'Last Updated (Riyadh)', 'As Of (UTC)',
  'As Of (Riyadh)', 'Updated At (UTC)', 'Updated At (Riyadh)'
]);

// v2.13.0: date-only columns (no time component). 'Buy Date' is a manual
// My_Portfolio input; a date-only format keeps it clean versus the
// timestamp format used for the engine's Last Updated columns.
var _DATE_ONLY_COLUMNS_ = Object.freeze([
  'Buy Date'
]);

var _POSITIVE_NUMERIC_COLUMNS_ = Object.freeze([
  'Price Change', 'Percent Change', 'Change %', 'Expected ROI 1M',
  'ROI 1M %', 'Expected ROI 3M', 'ROI 3M %', 'Expected ROI 12M',
  'ROI 12M %', 'Unrealized P/L', 'Unrealized P/L %', 'Upside %',
  'Upside/Downside %', '5D Change %'
]);

/* UNION of every risk-flavoured column. v2.17.0: this list is NO LONGER the
 * conditional-format generator's input — it exists to feed _MANAGED_HEADERS_SET_
 * (see the CRITICAL note in the v2.17.0 header block). Dropping the fraction
 * columns from here would make _filterPreservedRules_ treat the stale v2.16.0
 * green rules as user rules and preserve them forever, silently defeating FIX 1.
 * Keep the union intact. */
var _NEGATIVE_RISK_COLUMNS_ = Object.freeze([
  'Risk Score', 'VaR 95% (1D)', 'Volatility 30D', 'Volatility 30D %',
  'Volatility 90D', 'Volatility 90D %', 'Max Drawdown 1Y', 'Max DD 1Y %'
]);

/* v2.17.0 FIX 1 — POINTS-SCALE risk columns (0-100). The 70/40 bands are correct
 * for these, so they are the ONLY columns applyConditionalFormatting_ now paints
 * with the risk ramp. 'Risk Score' is engine-emitted on a 0-100 scale. */
var _NEGATIVE_RISK_POINTS_COLUMNS_ = Object.freeze([
  'Risk Score'
]);

/* v2.17.0 FIX 1 — FRACTION-SCALE risk columns (0-1). The engine emits these as
 * fractions (0.10433 == 10.43%), so a 70/40 ramp can never fire anything but the
 * <40 green branch: a 95%-volatility name and an 8%-volatility name both painted
 * "low risk" green. Deliberately UNCOLOURED until v2.18.0 lands bands derived
 * from the live cross-sectional distribution (sign-agnostic via ABS, because Max
 * Drawdown's sign convention is not assumed). The DISPLAYED value stays correct —
 * v2.15.0 already routes these through _KNOWN_FRACTION_PERCENT_COLUMNS_. */
var _NEGATIVE_RISK_FRACTION_COLUMNS_ = Object.freeze([
  'VaR 95% (1D)',
  'Volatility 30D', 'Volatility 30D %',
  'Volatility 90D', 'Volatility 90D %',
  'Max Drawdown 1Y', 'Max DD 1Y %'
]);

/* v2.18.0 — evidence-derived bands for the fraction-scale risk columns.
 * [green_lt, red_gt] on ABS(value): green < median, red > p85 of the live
 * cross-sectional distribution (2026-07-12 export, n=4,662, winsorised <2.0).
 * ABS because VaR/MDD are stored negative in 81% of rows (loss convention) —
 * the sign is convention, not information. Keys MUST stay a subset of
 * _NEGATIVE_RISK_FRACTION_COLUMNS_ (self-verify enforces the partition). */
var _NEGATIVE_RISK_FRACTION_BANDS_ = Object.freeze({
  'Volatility 30D':   Object.freeze([0.34, 0.64]),
  'Volatility 30D %': Object.freeze([0.34, 0.64]),
  'Volatility 90D':   Object.freeze([0.36, 0.64]),
  'Volatility 90D %': Object.freeze([0.36, 0.64]),
  'VaR 95% (1D)':     Object.freeze([0.03, 0.05]),
  'Max Drawdown 1Y':  Object.freeze([0.29, 0.51]),
  'Max DD 1Y %':      Object.freeze([0.29, 0.51])
});

/* v2.18.0 — 1-based column index -> A1 letters (27 -> AA). Needed because the
 * ABS() bands ride whenFormulaSatisfied, which addresses cells in A1 notation. */
function _cfColLetterA1_(colIndex) {
  var n = Math.floor(colIndex);
  var s = '';
  while (n > 0) {
    var r = (n - 1) % 26;
    s = String.fromCharCode(65 + r) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

var _POSITIVE_SCORE_COLUMNS_ = Object.freeze([
  'Conviction Score', 'Sector-Adj Score'
]);

var _STATUS_COLUMNS_ = Object.freeze([
  'Warnings', 'Notes', 'User Notes', 'Recommendation Reason',
  'Reason', 'selection_reason',
  'Scoring Errors'
]);

var _KNOWN_FRACTION_PERCENT_COLUMNS_ = Object.freeze([
  'Percent Change', 'Change %',
  '5D Change %',
  'Expected ROI 1M', 'ROI 1M %',
  'Expected ROI 3M', 'ROI 3M %',
  'Expected ROI 12M', 'ROI 12M %',
  'Forecast Confidence', 'AI Confidence',
  'Upside %', 'Upside/Downside %',
  'Max Drawdown 1Y', 'Max DD 1Y %',
  'VaR 95% (1D)',
  // v2.15.0 FIX: the engine emits these as FRACTIONS (0.10433 == 10.43%),
  // the same scale as 'Max Drawdown 1Y' / 'VaR 95% (1D)' directly above.
  // Moved here from _KNOWN_POINTS_PERCENT_COLUMNS_ so the fraction format
  // ('0.00%', which x100s) renders "10.43%" instead of the points format's
  // "0.10%". They remain in _PERCENT_COLUMNS_ and _NEGATIVE_RISK_COLUMNS_.
  'Volatility 30D', 'Volatility 30D %',
  'Volatility 90D', 'Volatility 90D %'
]);

var _KNOWN_POINTS_PERCENT_COLUMNS_ = Object.freeze([
  '52W Position %',
  'Unrealized P/L %',
  // v2.15.0 FIX: 'Volatility 30D' / 'Volatility 90D' (and their '%' aliases)
  // moved OUT of this list to _KNOWN_FRACTION_PERCENT_COLUMNS_ — the engine
  // emits them as fractions, so the points format ('#,##0.00"%"') rendered
  // them ~100x too small ("0.10%" for a true 10.43%).
  // v2.13.0 FIX B: weight columns are percent POINTS -> force the points
  // branch so 40.0 renders "40.00%", not the fraction branch.
  'Target Weight %', 'Actual Weight %', 'Weight Gap'
]);

var _NULL_DISPLAY_OVERRIDES_ = Object.freeze({
  'Candle Confidence': '[=0]"\u2014";#,##0.00'
});

var _FRACTIONAL_MULTIPLIER_COLUMNS_ = Object.freeze([
  'Overall Penalty Factor'
]);

var _MANAGED_HEADERS_SET_ = (function () {
  var union = {};
  function add(arr) {
    for (var i = 0; i < arr.length; i++) {
      union[arr[i]] = true;
    }
  }
  add(_POSITIVE_NUMERIC_COLUMNS_);
  add(_NEGATIVE_RISK_COLUMNS_);
  add(_POSITIVE_SCORE_COLUMNS_);
  add(_STATUS_COLUMNS_);
  add(['Recommendation', 'Risk Bucket', 'Confidence Bucket',
       'Fundamental View', 'Technical View', 'Risk View', 'Value View']);
  add(['Recommendation Detail', 'Reco Priority',
       'Candle Pattern', 'Candle Signal', 'Candle Strength',
       'Candle Confidence', 'Recent Patterns (5D)']);
  add(['Priority Band', 'Reco Source']);
  add(['Provider Rating', 'Scoring Reco Source', 'Scoring Schema Version',
       'Scoring Errors', 'Opportunity Source',
       'Overall Score (Raw)', 'Overall Penalty Factor']);
  add(['Forecast Source']);
  add(['Forecast Reliability Score', 'Provider/Engine Conflict',
       'Conflict Type', 'Final Decision Basis', 'Investability Status',
       'Final Action', 'Block Reason']);
  // v2.13.0: My_Portfolio engine-native decision columns (idempotent re-runs).
  add(['Rebalance Action', 'Investor Decision']);
  return Object.freeze(union);
})();

/* =========================================================
 * v2.14.0 — decision-owned page guard
 * ========================================================= */

function _formatIsDecisionOwnedPage_(pageName) {
  try {
    if (typeof isDecisionOwnedPage_ === 'function') {
      return isDecisionOwnedPage_(pageName) === true;
    }
  } catch (ownErr) {}
  return false;
}

/* =========================================================
 * Main public entry point
 * ========================================================= */

function applySheetFormatting_(pageName) {
  return withScriptLock_(function () {
    var page = getPageConfig(pageName);

    /* v2.14.0 FIX 1: the cockpit owns its own layout, widths, and CF rules.
     * Engine formatting must never run on it. Skip (not throw) so batch
     * callers continue across the remaining pages. */
    if (_formatIsDecisionOwnedPage_(page.pageName)) {
      var ownedMsg = '"' + page.pageName + '" is rendered by the Decision cockpit ' +
        '(16_Decision_Top10.gs). Engine formatting skipped to protect the cockpit layout. ' +
        'Use the Decision menu (Build Top 10 Cockpit Layout) to rebuild it.';

      try {
        logRun_('WARN', 'applySheetFormatting', page.pageName, ownedMsg, {
          status: 'SKIPPED_DECISION_OWNED'
        });
      } catch (logErr) {}

      return {
        ok: true,
        pageName: page.pageName,
        action: 'skipped_decision_owned',
        message: ownedMsg
      };
    }

    var sheet = ensureSheetExists_(page.pageName);
    var headers = readCurrentSheetHeaders_(sheet);

    if (!headers.length) {
      headers = getExpectedHeaders_(page.pageName);
      if (headers.length) {
        writeHeaderRowExact_(sheet, headers);
      }
    }

    if (!headers.length) {
      throw new Error('Cannot format page without headers: ' + page.pageName);
    }

    applyBaseSheetFormatting_(sheet, page, headers);
    applyHeaderFormatting_(sheet, page, headers);
    applyColumnWidths_(sheet, page.pageName, headers);
    applyNumberFormats_(sheet, page.pageName, headers);
    applyConditionalFormatting_(sheet, page.pageName, headers);
    applyValidations_(sheet, page.pageName, headers);
    protectHeaderRows_(sheet);

    logRun_(
      'INFO',
      'applySheetFormatting',
      page.pageName,
      'Formatting applied successfully.',
      { status: 'OK', endpoint: '', httpCode: '', columns: headers.length }
    );

    writePageStatus_(page.pageName, {
      status: 'FORMAT_OK',
      message: 'Formatting applied',
      endpoint: '',
      httpCode: '',
      rows: Math.max(0, sheet.getLastRow() - 1),
      columns: headers.length,
      durationMs: '',
      warnings: []
    });

    return {
      ok: true,
      pageName: page.pageName,
      headers: headers,
      columnCount: headers.length
    };
  });
}

/* =========================================================
 * Base sheet formatting
 * ========================================================= */

function applyBaseSheetFormatting_(sheet, page, headers) {
  var lastRow = Math.max(sheet.getLastRow(), 2);
  var lastCol = Math.max(headers.length, 1);

  try { sheet.setFrozenRows(1); } catch (e) {}

  // v2.14.0 ENH 4: keep Symbol + Name visible while scrolling wide pages.
  // Only pages with >= 20 headers (the 115/118/122 family); narrow pages
  // such as Insights_Analysis (7 cols) keep their existing freeze state.
  if (headers.length >= 20) {
    try { sheet.setFrozenColumns(2); } catch (eFc) {}
  }

  if (page && page.formatting && page.formatting.tabColor) {
    try { sheet.setTabColor(page.formatting.tabColor); } catch (e2) {}
  }

  var wholeRange = sheet.getRange(1, 1, lastRow, lastCol);
  wholeRange
    .setFontFamily(FORMAT_THEME_.FONT_FAMILY)
    .setFontSize(FORMAT_THEME_.BODY_SIZE)
    .setFontColor(FORMAT_THEME_.BODY_FONT)
    .setVerticalAlignment('middle');

  if (lastRow > 1) {
    var dataRange = sheet.getRange(2, 1, lastRow - 1, lastCol);
    applyAlternatingRows_(dataRange);
  }

  // v2.14.0 ENH 3: remove-then-recreate so the filter range never goes
  // stale as data grows (a bare createFilter() silently fails when a
  // filter already exists). Created over the full sheet height so it
  // covers rows appended between format passes.
  try {
    var existingFilter = sheet.getFilter();
    if (existingFilter) existingFilter.remove();
    var filterRange = sheet.getRange(1, 1, Math.max(sheet.getMaxRows(), 2), lastCol);
    filterRange.createFilter();
  } catch (e3) {}

  try { sheet.setRowHeight(1, 30); } catch (e4) {}
}

function applyAlternatingRows_(range) {
  try {
    var bandings = range.getBandings();
    for (var i = 0; i < bandings.length; i++) {
      try { bandings[i].remove(); } catch (e) {}
    }
  } catch (e2) {}

  /* v2.17.0 FIX 2: paint the zebra with the PROPOSAL palette. v2.16.0 changed
   * FORMAT_THEME_.ALT_ROW_BG and asserted it green, but this function never read
   * it — it applied Google's stock LIGHT_GREY theme, so the reskin never reached
   * a cell. Apply with header/footer bands OFF (the range already starts at row 2;
   * a banded "header" would make the first data row look like a second header),
   * then set the pair explicitly. Every step is guarded: if the 3-arg overload or
   * the Banding colour setters are unavailable in this runtime, the pass degrades
   * to the v2.16.0 stock-theme behaviour instead of throwing mid-format. */
  var banding = null;
  try {
    banding = range.applyRowBanding(SpreadsheetApp.BandingTheme.LIGHT_GREY, false, false);
  } catch (e3) {
    try {
      banding = range.applyRowBanding(SpreadsheetApp.BandingTheme.LIGHT_GREY);
    } catch (e3b) {}
  }

  try {
    if (banding && typeof banding.setSecondRowColor === 'function') {
      banding
        .setFirstRowColor(FORMAT_THEME_.BASE_ROW_BG)
        .setSecondRowColor(FORMAT_THEME_.ALT_ROW_BG);
    }
  } catch (e4) {}
}

/* =========================================================
 * Header formatting
 * ========================================================= */

function applyHeaderFormatting_(sheet, page, headers) {
  var headerRange = sheet.getRange(1, 1, 1, headers.length);

  headerRange
    .setBackground(FORMAT_THEME_.HEADER_BG)
    .setFontColor(FORMAT_THEME_.HEADER_FONT)
    .setFontWeight(FORMAT_THEME_.HEADER_WEIGHT)
    .setFontFamily(FORMAT_THEME_.FONT_FAMILY)
    .setFontSize(FORMAT_THEME_.HEADER_SIZE)
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle')
    .setWrap(true);

  try {
    headerRange.setBorder(
      true, true, true, true, true, true,
      FORMAT_THEME_.BORDER,
      SpreadsheetApp.BorderStyle.SOLID_MEDIUM
    );
  } catch (e) {}
}

/* =========================================================
 * Column widths
 * ========================================================= */

function applyColumnWidths_(sheet, pageName, headers) {
  var page = getPageConfig(pageName);

  for (var i = 0; i < headers.length; i++) {
    var name = headers[i];
    var col = i + 1;

    if (formatListHasHeader_(_WIDE_COLUMNS_, name)) {
      sheet.setColumnWidth(col, 220);
    } else if (formatListHasHeader_(_MEDIUM_COLUMNS_, name)) {
      sheet.setColumnWidth(col, 140);
    } else if (formatListHasHeader_(_NARROW_COLUMNS_, name)) {
      sheet.setColumnWidth(col, 90);
    } else {
      sheet.setColumnWidth(col, 120);
    }
  }

  var manualColumns = page && page.formatting && page.formatting.editableColumns
    ? page.formatting.editableColumns
    : [];

  var headerMap = getHeaderMap_(headers);

  for (var m = 0; m < manualColumns.length; m++) {
    var manualCol = headerMap[manualColumns[m]];
    if (manualCol) {
      sheet.setColumnWidth(manualCol, 150);
    }
  }
}

/* =========================================================
 * Number formats
 * ========================================================= */

function applyNumberFormats_(sheet, pageName, headers) {
  var headerMap = getHeaderMap_(headers);
  // v2.14.0 ENH 2: format the full sheet height (same extent
  // applyValidations_ already uses) so rows appended later arrive
  // pre-formatted instead of inheriting stale cell formats — the
  // mechanism behind the v2.13.0 FIX A "Market Cap shows %" bug.
  var lastRow = Math.max(sheet.getMaxRows(), 2);

  if (lastRow < 2) return;

  var numeric2 = '#,##0.00';
  var numeric0 = '#,##0';
  var priceFmt = '#,##0.00';
  var dateFmt = 'yyyy-mm-dd hh:mm:ss';
  var dateOnlyFmt = 'yyyy-mm-dd';

  var pctFmtPoints = '#,##0.00"%"';
  var pctFmtFraction = '0.00%';

  var signedPctFmtPoints = '\u25B2 #,##0.00"%";\u25BC -#,##0.00"%";0.00"%"';
  var signedPctFmtFraction = '\u25B2 #,##0.00%;\u25BC -#,##0.00%;0.00%';

  var signedNumberFmt = '\u25B2 #,##0.00;\u25BC -#,##0.00;0.00';

  var fractionalMultiplierFmt = '0.0000';

  var decimalFieldSet = (typeof getDecimalFractionFields === 'function')
    ? new Set(getDecimalFractionFields())
    : new Set();

  var headerToKey = (typeof getHeaderToKey === 'function')
    ? getHeaderToKey(pageName)
    : {};

  var coverage = {
    matched: 0, missed: 0, fractionPct: 0, pointsPct: 0,
    fractionPctOverride: 0, pointsPctOverride: 0,
    fractionPctSchema: 0, pointsPctSchema: 0
  };

  var isFraction = function (headerName) {
    if (formatListHasHeader_(_KNOWN_FRACTION_PERCENT_COLUMNS_, headerName)) {
      coverage.fractionPctOverride++;
      return true;
    }
    if (formatListHasHeader_(_KNOWN_POINTS_PERCENT_COLUMNS_, headerName)) {
      coverage.pointsPctOverride++;
      return false;
    }

    if (decimalFieldSet.size === 0) {
      coverage.pointsPctSchema++;
      return false;
    }

    var apiKey = (typeof coreResolveHeaderKey_ === 'function')
      ? coreResolveHeaderKey_(headerName, headerToKey)
      : (headerToKey[String(headerName || '').trim()] || null);

    if (!!apiKey && decimalFieldSet.has(apiKey)) {
      coverage.fractionPctSchema++;
      return true;
    }

    coverage.pointsPctSchema++;
    return false;
  };

  applyFormatToNamedColumns_(sheet, headerMap, lastRow, _PRICE_COLUMNS_, priceFmt, coverage);
  applyFormatToNamedColumns_(sheet, headerMap, lastRow, _INTEGER_COLUMNS_, numeric0, coverage);
  applyFormatToNamedColumns_(sheet, headerMap, lastRow, _RATIO_COLUMNS_, numeric2, coverage);

  applyPercentFormatPerColumn_(
    sheet, headerMap, lastRow, _PERCENT_COLUMNS_,
    pctFmtFraction, pctFmtPoints, isFraction, coverage
  );

  applyPercentFormatPerColumn_(
    sheet, headerMap, lastRow, _SIGNED_PERCENT_COLUMNS_,
    signedPctFmtFraction, signedPctFmtPoints, isFraction, coverage
  );

  applyFormatToNamedColumns_(sheet, headerMap, lastRow, _SIGNED_NUMBER_COLUMNS_, signedNumberFmt, coverage);
  applyFormatToNamedColumns_(sheet, headerMap, lastRow, _DATE_COLUMNS_, dateFmt, coverage);
  applyFormatToNamedColumns_(sheet, headerMap, lastRow, _DATE_ONLY_COLUMNS_, dateOnlyFmt, coverage);

  applyFractionalMultiplierFormat_(sheet, headerMap, lastRow, fractionalMultiplierFmt, coverage);
  applyNullDisplayOverrides_(sheet, headerMap, lastRow, coverage);

  if (DEBUG_FORMAT_LOG_) {
    try {
      logRun_(
        'INFO', 'applyNumberFormats', pageName,
        'Format coverage: matched=' + coverage.matched +
          ', missed=' + coverage.missed +
          ', fraction-percent=' + coverage.fractionPct +
          ', points-percent=' + coverage.pointsPct + '.',
        { status: 'OK', totalHeaders: headers.length,
          matched: coverage.matched, missed: coverage.missed,
          fractionPercentColumns: coverage.fractionPct,
          pointsPercentColumns: coverage.pointsPct }
      );
    } catch (e) {}

    try {
      logRun_(
        'INFO', 'applyNumberFormats', pageName,
        '[v2.6.0 PCT-FORMAT DIAG] override-fraction=' + coverage.fractionPctOverride +
          ' override-points=' + coverage.pointsPctOverride +
          ' schema-fraction=' + coverage.fractionPctSchema +
          ' schema-points=' + coverage.pointsPctSchema +
          ' total-matched=' + coverage.matched +
          ' total-missed=' + coverage.missed,
        { status: 'OK', totalHeaders: headers.length,
          matched: coverage.matched, missed: coverage.missed,
          fractionOverride: coverage.fractionPctOverride,
          pointsOverride: coverage.pointsPctOverride,
          fractionSchema: coverage.fractionPctSchema,
          pointsSchema: coverage.pointsPctSchema }
      );
    } catch (e2) {}
  }
}

function applyFormatToNamedColumns_(sheet, headerMap, lastRow, names, formatPattern, coverage) {
  for (var i = 0; i < names.length; i++) {
    var name = names[i];
    var col = headerMap[name];
    if (!col) {
      if (coverage) coverage.missed++;
      continue;
    }
    var range = sheet.getRange(2, col, Math.max(lastRow - 1, 1), 1);
    range.setNumberFormat(formatPattern);
    if (coverage) coverage.matched++;
  }
}

function applyPercentFormatPerColumn_(sheet, headerMap, lastRow, names, fractionFmt, pointsFmt, isFraction, coverage) {
  for (var i = 0; i < names.length; i++) {
    var name = names[i];
    var col = headerMap[name];
    if (!col) {
      if (coverage) coverage.missed++;
      continue;
    }
    var range = sheet.getRange(2, col, Math.max(lastRow - 1, 1), 1);
    if (isFraction(name)) {
      range.setNumberFormat(fractionFmt);
      if (coverage) { coverage.matched++; coverage.fractionPct++; }
    } else {
      range.setNumberFormat(pointsFmt);
      if (coverage) { coverage.matched++; coverage.pointsPct++; }
    }
  }
}

function applyFractionalMultiplierFormat_(sheet, headerMap, lastRow, formatPattern, coverage) {
  for (var i = 0; i < _FRACTIONAL_MULTIPLIER_COLUMNS_.length; i++) {
    var name = _FRACTIONAL_MULTIPLIER_COLUMNS_[i];
    var col = headerMap[name];
    if (!col) continue;
    var range = sheet.getRange(2, col, Math.max(lastRow - 1, 1), 1);
    range.setNumberFormat(formatPattern);
    if (coverage) coverage.matched++;
  }
}

function applyNullDisplayOverrides_(sheet, headerMap, lastRow, coverage) {
  var keys = Object.keys(_NULL_DISPLAY_OVERRIDES_);
  for (var i = 0; i < keys.length; i++) {
    var headerName = keys[i];
    var fmtString = _NULL_DISPLAY_OVERRIDES_[headerName];
    var col = headerMap[headerName];
    if (!col) continue;
    var range = sheet.getRange(2, col, Math.max(lastRow - 1, 1), 1);
    range.setNumberFormat(fmtString);
    if (coverage) coverage.matched++;
  }
}

function isDecimalFractionByHeader_(headerName, pageName) {
  if (typeof getDecimalFractionFields !== 'function') return false;
  if (typeof getHeaderToKey !== 'function') return false;

  var decimalFields = new Set(getDecimalFractionFields());
  var headerToKey = getHeaderToKey(pageName);

  var apiKey = (typeof coreResolveHeaderKey_ === 'function')
    ? coreResolveHeaderKey_(headerName, headerToKey)
    : (headerToKey[String(headerName || '').trim()] || null);

  return !!apiKey && decimalFields.has(apiKey);
}

/* =========================================================
 * Conditional formatting
 * ========================================================= */

function applyConditionalFormatting_(sheet, pageName, headers) {
  // v2.14.0 ENH 2: CF rule ranges cover the full sheet height so rules
  // keep applying to rows appended between format passes.
  var lastRow = Math.max(sheet.getMaxRows(), 2);
  var headerMap = getHeaderMap_(headers);
  var newOwnedRules = [];

  for (var i = 0; i < _POSITIVE_NUMERIC_COLUMNS_.length; i++) {
    var pName = _POSITIVE_NUMERIC_COLUMNS_[i];
    var pCol = headerMap[pName];
    if (!pCol) continue;
    var pRange = sheet.getRange(2, pCol, Math.max(lastRow - 1, 1), 1);
    newOwnedRules.push(
      SpreadsheetApp.newConditionalFormatRule()
        .whenNumberGreaterThan(0)
        .setBackground(FORMAT_THEME_.POSITIVE_BG)
        .setFontColor(FORMAT_THEME_.POSITIVE_FONT)
        .setRanges([pRange]).build(),
      SpreadsheetApp.newConditionalFormatRule()
        .whenNumberLessThan(0)
        .setBackground(FORMAT_THEME_.NEGATIVE_BG)
        .setFontColor(FORMAT_THEME_.NEGATIVE_FONT)
        .setRanges([pRange]).build()
    );
  }

  /* v2.17.0 FIX 1: iterate the POINTS-scale list only. The fraction-scale columns
   * (_NEGATIVE_RISK_FRACTION_COLUMNS_) get NO rule — a 70/40 ramp on a 0-1 value
   * can only ever emit the <40 green branch, which asserts "low risk" on a 95%-
   * volatility name. They remain in _NEGATIVE_RISK_COLUMNS_ (and therefore in
   * _MANAGED_HEADERS_SET_) so this pass SWEEPS the stale v2.16.0 rules off them
   * instead of preserving them as user rules. */
  for (var j = 0; j < _NEGATIVE_RISK_POINTS_COLUMNS_.length; j++) {
    var rName = _NEGATIVE_RISK_POINTS_COLUMNS_[j];
    var rCol = headerMap[rName];
    if (!rCol) continue;
    var rRange = sheet.getRange(2, rCol, Math.max(lastRow - 1, 1), 1);
    newOwnedRules.push(
      SpreadsheetApp.newConditionalFormatRule().whenNumberGreaterThan(70)
        .setBackground(FORMAT_THEME_.NEGATIVE_BG).setFontColor(FORMAT_THEME_.NEGATIVE_FONT).setRanges([rRange]).build(),
      SpreadsheetApp.newConditionalFormatRule().whenNumberBetween(40, 70)
        .setBackground(FORMAT_THEME_.WARNING_BG).setFontColor(FORMAT_THEME_.WARNING_FONT).setRanges([rRange]).build(),
      SpreadsheetApp.newConditionalFormatRule().whenNumberLessThan(40)
        .setBackground(FORMAT_THEME_.POSITIVE_BG).setFontColor(FORMAT_THEME_.POSITIVE_FONT).setRanges([rRange]).build()
    );
  }

  /* v2.18.0: fraction-scale risk columns get their evidence-derived ABS bands.
   * whenFormulaSatisfied because VaR/MDD are stored negative (81% of rows) —
   * plain number conditions would invert the ramp on the loss convention.
   * $X2: column-absolute, row-relative, walking the range from its first data
   * row. ISNUMBER keeps blanks unpainted. These columns are in
   * _MANAGED_HEADERS_SET_, so sweep + idempotency apply unchanged. */
  for (var fb = 0; fb < _NEGATIVE_RISK_FRACTION_COLUMNS_.length; fb++) {
    var fbName = _NEGATIVE_RISK_FRACTION_COLUMNS_[fb];
    var fbBand = _NEGATIVE_RISK_FRACTION_BANDS_[fbName];
    if (!fbBand) continue;
    var fbCol = headerMap[fbName];
    if (!fbCol) continue;
    var fbRange = sheet.getRange(2, fbCol, Math.max(lastRow - 1, 1), 1);
    var fbCell = '$' + _cfColLetterA1_(fbCol) + '2';
    var fbAbs = 'ABS(' + fbCell + ')';
    var fbGuard = 'ISNUMBER(' + fbCell + ')';
    newOwnedRules.push(
      SpreadsheetApp.newConditionalFormatRule()
        .whenFormulaSatisfied('=AND(' + fbGuard + ',' + fbAbs + '>' + fbBand[1] + ')')
        .setBackground(FORMAT_THEME_.NEGATIVE_BG).setFontColor(FORMAT_THEME_.NEGATIVE_FONT).setRanges([fbRange]).build(),
      SpreadsheetApp.newConditionalFormatRule()
        .whenFormulaSatisfied('=AND(' + fbGuard + ',' + fbAbs + '>' + fbBand[0] + ',' + fbAbs + '<=' + fbBand[1] + ')')
        .setBackground(FORMAT_THEME_.WARNING_BG).setFontColor(FORMAT_THEME_.WARNING_FONT).setRanges([fbRange]).build(),
      SpreadsheetApp.newConditionalFormatRule()
        .whenFormulaSatisfied('=AND(' + fbGuard + ',' + fbAbs + '<=' + fbBand[0] + ')')
        .setBackground(FORMAT_THEME_.POSITIVE_BG).setFontColor(FORMAT_THEME_.POSITIVE_FONT).setRanges([fbRange]).build()
    );
  }

  for (var k = 0; k < _POSITIVE_SCORE_COLUMNS_.length; k++) {
    var sName = _POSITIVE_SCORE_COLUMNS_[k];
    var sCol = headerMap[sName];
    if (!sCol) continue;
    var sRange = sheet.getRange(2, sCol, Math.max(lastRow - 1, 1), 1);
    newOwnedRules.push(
      SpreadsheetApp.newConditionalFormatRule().whenNumberGreaterThan(70)
        .setBackground(FORMAT_THEME_.POSITIVE_BG).setFontColor(FORMAT_THEME_.POSITIVE_FONT).setRanges([sRange]).build(),
      SpreadsheetApp.newConditionalFormatRule().whenNumberBetween(40, 70)
        .setBackground(FORMAT_THEME_.WARNING_BG).setFontColor(FORMAT_THEME_.WARNING_FONT).setRanges([sRange]).build(),
      SpreadsheetApp.newConditionalFormatRule().whenNumberLessThan(40)
        .setBackground(FORMAT_THEME_.NEGATIVE_BG).setFontColor(FORMAT_THEME_.NEGATIVE_FONT).setRanges([sRange]).build()
    );
  }

  var recCol = headerMap['Recommendation'];
  if (recCol) {
    var recRange = sheet.getRange(2, recCol, Math.max(lastRow - 1, 1), 1);
    newOwnedRules.push(
      textEqualToRule_(recRange, 'STRONG_BUY', FORMAT_THEME_.POSITIVE_BG, FORMAT_THEME_.POSITIVE_FONT),
      textEqualToRule_(recRange, 'ACCUMULATE', FORMAT_THEME_.POSITIVE_BG, FORMAT_THEME_.POSITIVE_FONT),
      textEqualToRule_(recRange, 'BUY', FORMAT_THEME_.POSITIVE_BG, FORMAT_THEME_.POSITIVE_FONT),
      textEqualToRule_(recRange, 'HOLD', FORMAT_THEME_.WARNING_BG, FORMAT_THEME_.WARNING_FONT),
      textEqualToRule_(recRange, 'REDUCE', FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT),
      textEqualToRule_(recRange, 'SELL', FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT),
      textEqualToRule_(recRange, 'STRONG_SELL', FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT),
      textEqualToRule_(recRange, 'AVOID', FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT)
    );
  }

  var recDetailCol = headerMap['Recommendation Detail'];
  if (recDetailCol) {
    var recDetailRange = sheet.getRange(2, recDetailCol, Math.max(lastRow - 1, 1), 1);
    newOwnedRules.push(
      textContainsRule_(recDetailRange, 'STRONG_BUY', FORMAT_THEME_.POSITIVE_BG, FORMAT_THEME_.POSITIVE_FONT),
      textContainsRule_(recDetailRange, 'BUY', FORMAT_THEME_.POSITIVE_BG, FORMAT_THEME_.POSITIVE_FONT),
      textContainsRule_(recDetailRange, 'ACCUMULATE', FORMAT_THEME_.POSITIVE_BG, FORMAT_THEME_.POSITIVE_FONT),
      textContainsRule_(recDetailRange, 'HOLD', FORMAT_THEME_.WARNING_BG, FORMAT_THEME_.WARNING_FONT),
      textContainsRule_(recDetailRange, 'REDUCE', FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT),
      textContainsRule_(recDetailRange, 'STRONG_SELL', FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT),
      textContainsRule_(recDetailRange, 'SELL', FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT),
      textContainsRule_(recDetailRange, 'AVOID', FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT)
    );
  }

  var bucketColumns = ['Risk Bucket', 'Confidence Bucket'];
  for (var b = 0; b < bucketColumns.length; b++) {
    var bucketName = bucketColumns[b];
    var bucketCol = headerMap[bucketName];
    if (!bucketCol) continue;
    var bucketRange = sheet.getRange(2, bucketCol, Math.max(lastRow - 1, 1), 1);
    newOwnedRules.push(
      textContainsRule_(bucketRange, 'Low', FORMAT_THEME_.POSITIVE_BG, FORMAT_THEME_.POSITIVE_FONT),
      textContainsRule_(bucketRange, 'High', FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT),
      textContainsRule_(bucketRange, 'Moderate', FORMAT_THEME_.WARNING_BG, FORMAT_THEME_.WARNING_FONT),
      textContainsRule_(bucketRange, 'Medium', FORMAT_THEME_.WARNING_BG, FORMAT_THEME_.WARNING_FONT)
    );
  }

  applyViewRulesEqual_(sheet, headerMap, lastRow, ['Fundamental View'], [
    ['BULLISH', FORMAT_THEME_.POSITIVE_BG, FORMAT_THEME_.POSITIVE_FONT],
    ['BEARISH', FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT],
    ['NEUTRAL', FORMAT_THEME_.WARNING_BG, FORMAT_THEME_.WARNING_FONT]
  ], newOwnedRules);

  applyViewRulesEqual_(sheet, headerMap, lastRow, ['Technical View'], [
    ['BULLISH', FORMAT_THEME_.POSITIVE_BG, FORMAT_THEME_.POSITIVE_FONT],
    ['BEARISH', FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT],
    ['NEUTRAL', FORMAT_THEME_.WARNING_BG, FORMAT_THEME_.WARNING_FONT]
  ], newOwnedRules);

  applyViewRulesEqual_(sheet, headerMap, lastRow, ['Risk View'], [
    ['LOW', FORMAT_THEME_.POSITIVE_BG, FORMAT_THEME_.POSITIVE_FONT],
    ['HIGH', FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT],
    ['MODERATE', FORMAT_THEME_.WARNING_BG, FORMAT_THEME_.WARNING_FONT]
  ], newOwnedRules);

  applyViewRulesEqual_(sheet, headerMap, lastRow, ['Value View'], [
    ['CHEAP',       FORMAT_THEME_.POSITIVE_BG, FORMAT_THEME_.POSITIVE_FONT],
    ['UNDERVALUED', FORMAT_THEME_.POSITIVE_BG, FORMAT_THEME_.POSITIVE_FONT],
    ['EXPENSIVE',   FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT],
    ['OVERVALUED',  FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT],
    ['FAIR',        FORMAT_THEME_.WARNING_BG, FORMAT_THEME_.WARNING_FONT]
  ], newOwnedRules);

  applyViewRulesEqual_(sheet, headerMap, lastRow, ['Candle Signal'], [
    ['BULLISH', FORMAT_THEME_.POSITIVE_BG, FORMAT_THEME_.POSITIVE_FONT],
    ['BEARISH', FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT],
    ['NEUTRAL', FORMAT_THEME_.WARNING_BG, FORMAT_THEME_.WARNING_FONT],
    ['DOJI',    FORMAT_THEME_.WARNING_BG, FORMAT_THEME_.WARNING_FONT]
  ], newOwnedRules);

  applyViewRulesEqual_(sheet, headerMap, lastRow, ['Priority Band'], [
    ['P1', FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT],
    ['P2', FORMAT_THEME_.POSITIVE_BG, FORMAT_THEME_.POSITIVE_FONT],
    ['P3', FORMAT_THEME_.POSITIVE_BG, FORMAT_THEME_.POSITIVE_FONT],
    ['P4', FORMAT_THEME_.WARNING_BG, FORMAT_THEME_.WARNING_FONT],
    ['P5', FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT]
  ], newOwnedRules);

  applyViewRulesEqual_(sheet, headerMap, lastRow, ['Investability Status'], [
    ['INVESTABLE', FORMAT_THEME_.POSITIVE_BG, FORMAT_THEME_.POSITIVE_FONT],
    ['WATCHLIST',  FORMAT_THEME_.WARNING_BG,  FORMAT_THEME_.WARNING_FONT],
    ['BLOCKED',    FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT]
  ], newOwnedRules);

  applyViewRulesEqual_(sheet, headerMap, lastRow, ['Final Action'], [
    ['INVEST',        FORMAT_THEME_.POSITIVE_BG, FORMAT_THEME_.POSITIVE_FONT],
    ['WATCH',         FORMAT_THEME_.WARNING_BG,  FORMAT_THEME_.WARNING_FONT],
    ['DO_NOT_INVEST', FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT]
  ], newOwnedRules);

  applyViewRulesEqual_(sheet, headerMap, lastRow, ['Provider/Engine Conflict'], [
    ['TRUE', FORMAT_THEME_.WARNING_BG, FORMAT_THEME_.WARNING_FONT]
  ], newOwnedRules);

  // v2.13.0 FIX C: My_Portfolio engine-native decision columns. ADD -> green,
  // HOLD -> amber, REDUCE / SELL -> red. EXACT-match (whenTextEqualTo via
  // applyViewRulesEqual_) so 'ADD' never substring-matches another token.
  // Both columns share the mapping; 'Investor Decision' additionally carries
  // SELL (the blended verdict), 'Rebalance Action' is drift-only (ADD/REDUCE/
  // HOLD) and simply never emits SELL.
  applyViewRulesEqual_(sheet, headerMap, lastRow, ['Rebalance Action', 'Investor Decision'], [
    ['ADD',    FORMAT_THEME_.POSITIVE_BG, FORMAT_THEME_.POSITIVE_FONT],
    ['HOLD',   FORMAT_THEME_.WARNING_BG,  FORMAT_THEME_.WARNING_FONT],
    ['REDUCE', FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT],
    ['SELL',   FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT]
  ], newOwnedRules);

  var blockReasonCol = headerMap['Block Reason'];
  if (blockReasonCol) {
    var blockReasonRange = sheet.getRange(2, blockReasonCol, Math.max(lastRow - 1, 1), 1);
    newOwnedRules.push(
      SpreadsheetApp.newConditionalFormatRule()
        .whenCellNotEmpty()
        .setBackground(FORMAT_THEME_.WARNING_BG)
        .setFontColor(FORMAT_THEME_.WARNING_FONT)
        .setRanges([blockReasonRange]).build()
    );
  }

  for (var s = 0; s < _STATUS_COLUMNS_.length; s++) {
    var statusName = _STATUS_COLUMNS_[s];
    var statusCol = headerMap[statusName];
    if (!statusCol) continue;
    var statusRange = sheet.getRange(2, statusCol, Math.max(lastRow - 1, 1), 1);
    newOwnedRules.push(
      textContainsRule_(statusRange, 'warning', FORMAT_THEME_.WARNING_BG, FORMAT_THEME_.WARNING_FONT),
      textContainsRule_(statusRange, 'timeout', FORMAT_THEME_.WARNING_BG, FORMAT_THEME_.WARNING_FONT),
      textContainsRule_(statusRange, 'error', FORMAT_THEME_.NEGATIVE_BG, FORMAT_THEME_.NEGATIVE_FONT),
      textContainsRule_(statusRange, 'Engine:', FORMAT_THEME_.WARNING_BG, FORMAT_THEME_.WARNING_FONT),
      textContainsRule_(statusRange, 'unavailable', FORMAT_THEME_.WARNING_BG, FORMAT_THEME_.WARNING_FONT)
    );
  }

  var page = getPageConfig(pageName);
  var editableColumns = page && page.formatting && page.formatting.editableColumns
    ? page.formatting.editableColumns
    : [];

  for (var e = 0; e < editableColumns.length; e++) {
    var editableName = editableColumns[e];
    var editableCol = headerMap[editableName];
    if (!editableCol) continue;
    var editableRange = sheet.getRange(2, editableCol, Math.max(lastRow - 1, 1), 1);
    editableRange
      .setBackground(FORMAT_THEME_.EDITABLE_BG)
      .setFontColor(FORMAT_THEME_.EDITABLE_FONT);
  }

  var preserveUserRules = _shouldPreserveUserRules_();
  var finalRules;
  if (preserveUserRules) {
    var existingRules = sheet.getConditionalFormatRules();
    var preservedUserRules = _filterPreservedRules_(existingRules, headerMap, headers);
    finalRules = preservedUserRules.concat(newOwnedRules);
  } else {
    finalRules = newOwnedRules;
  }
  sheet.setConditionalFormatRules(finalRules);
}

function _shouldPreserveUserRules_() {
  try {
    var props = PropertiesService.getScriptProperties();
    var v = props.getProperty('TFB_FORMAT_PRESERVE_USER_RULES');
    if (v === null || v === undefined) return true;
    var s = String(v).trim().toLowerCase();
    return ['0', 'false', 'no', 'off', 'n'].indexOf(s) === -1;
  } catch (e) {
    return true;
  }
}

function _filterPreservedRules_(existingRules, headerMap, headers) {
  if (!Array.isArray(existingRules) || existingRules.length === 0) return [];
  var managedCols = {};
  for (var h = 0; h < headers.length; h++) {
    var headerName = headers[h];
    if (_MANAGED_HEADERS_SET_[headerName]) {
      managedCols[h + 1] = true;
    }
  }
  var preserved = [];
  for (var i = 0; i < existingRules.length; i++) {
    var rule = existingRules[i];
    var ranges;
    try { ranges = rule.getRanges() || []; } catch (e) { preserved.push(rule); continue; }
    var touchesManaged = false;
    for (var r = 0; r < ranges.length; r++) {
      var rng = ranges[r];
      var startCol = rng.getColumn();
      var endCol = startCol + rng.getNumColumns() - 1;
      for (var col = startCol; col <= endCol; col++) {
        if (managedCols[col]) { touchesManaged = true; break; }
      }
      if (touchesManaged) break;
    }
    if (!touchesManaged) preserved.push(rule);
  }
  return preserved;
}

function applyViewRulesEqual_(sheet, headerMap, lastRow, columnNames, rulesSpec, rulesOut) {
  for (var i = 0; i < columnNames.length; i++) {
    var name = columnNames[i];
    var col = headerMap[name];
    if (!col) continue;
    var range = sheet.getRange(2, col, Math.max(lastRow - 1, 1), 1);
    for (var r = 0; r < rulesSpec.length; r++) {
      var spec = rulesSpec[r];
      rulesOut.push(textEqualToRule_(range, spec[0], spec[1], spec[2]));
    }
  }
}

function applyViewRules_(sheet, headerMap, lastRow, columnNames, rulesSpec, rulesOut) {
  for (var i = 0; i < columnNames.length; i++) {
    var name = columnNames[i];
    var col = headerMap[name];
    if (!col) continue;
    var range = sheet.getRange(2, col, Math.max(lastRow - 1, 1), 1);
    for (var r = 0; r < rulesSpec.length; r++) {
      var spec = rulesSpec[r];
      rulesOut.push(textContainsRule_(range, spec[0], spec[1], spec[2]));
    }
  }
}

function textContainsRule_(range, text, bg, font) {
  return SpreadsheetApp.newConditionalFormatRule()
    .whenTextContains(text)
    .setBackground(bg).setFontColor(font)
    .setRanges([range]).build();
}

function textEqualToRule_(range, text, bg, font) {
  return SpreadsheetApp.newConditionalFormatRule()
    .whenTextEqualTo(text)
    .setBackground(bg).setFontColor(font)
    .setRanges([range]).build();
}

/* =========================================================
 * Protection
 * ========================================================= */

function protectHeaderRows_(sheet) {
  try {
    var lastCol = Math.max(sheet.getLastColumn(), 1);
    var targetA1 = sheet.getRange(1, 1, 1, lastCol).getA1Notation();
    var protections = sheet.getProtections(SpreadsheetApp.ProtectionType.RANGE);
    for (var i = 0; i < protections.length; i++) {
      try {
        if (protections[i].getDescription &&
            protections[i].getDescription() === 'Protected Header Row' &&
            protections[i].getRange &&
            protections[i].getRange().getA1Notation() === targetA1) {
          protections[i].setWarningOnly(true);
          return;
        }
      } catch (innerErr) {}
    }
    var protection = sheet.getRange(1, 1, 1, lastCol).protect();
    protection.setDescription('Protected Header Row');
    protection.setWarningOnly(true);
  } catch (e) {}
}

/* =========================================================
 * Validation
 * ========================================================= */

function applyValidations_(sheet, pageName, headers) {
  var headerMap = getHeaderMap_(headers);
  var lastRow = Math.max(sheet.getMaxRows(), 2);

  setDropdownValidationIfColumnExists_(sheet, headerMap, 'Recommendation',
    [].concat(RECOMMENDATION_VOCAB_), lastRow);

  setDropdownValidationIfColumnExists_(sheet, headerMap, 'Risk Bucket',
    [].concat(RISK_BUCKET_VOCAB_), lastRow);

  setDropdownValidationIfColumnExists_(sheet, headerMap, 'Confidence Bucket',
    [].concat(CONFIDENCE_BUCKET_VOCAB_), lastRow);

  setDropdownValidationIfColumnExists_(sheet, headerMap, 'Decision',
    ['Buy', 'Hold', 'Sell', 'Watch'], lastRow);

  setDropdownValidationIfColumnExists_(sheet, headerMap, 'Action Flag',
    ['Yes', 'No', 'Review'], lastRow);

  setDropdownValidationIfColumnExists_(sheet, headerMap, 'Candle Signal',
    [].concat(CANDLE_SIGNAL_VOCAB_), lastRow);

  setDropdownValidationIfColumnExists_(sheet, headerMap, 'Priority Band',
    [].concat(PRIORITY_BAND_VOCAB_), lastRow);

  setDropdownValidationIfColumnExists_(sheet, headerMap, 'Forecast Source',
    [].concat(FORECAST_SOURCE_VOCAB_), lastRow);

  setDropdownValidationIfColumnExists_(sheet, headerMap, 'Investability Status',
    [].concat(INVESTABILITY_STATUS_VOCAB_), lastRow);

  setDropdownValidationIfColumnExists_(sheet, headerMap, 'Final Action',
    [].concat(FINAL_ACTION_VOCAB_), lastRow);

  setDropdownValidationIfColumnExists_(sheet, headerMap, 'Provider/Engine Conflict',
    [].concat(PROVIDER_ENGINE_CONFLICT_VOCAB_), lastRow);

  // v2.13.0 FIX C: My_Portfolio engine-native decision dropdowns.
  setDropdownValidationIfColumnExists_(sheet, headerMap, 'Rebalance Action',
    [].concat(REBALANCE_ACTION_VOCAB_), lastRow);

  setDropdownValidationIfColumnExists_(sheet, headerMap, 'Investor Decision',
    [].concat(INVESTOR_DECISION_VOCAB_), lastRow);
}

function setDropdownValidationIfColumnExists_(sheet, headerMap, columnName, values, lastRow) {
  var col = headerMap[columnName];
  if (!col) return;
  var range = sheet.getRange(2, col, Math.max(lastRow - 1, 1), 1);
  var rule = SpreadsheetApp.newDataValidation()
    .requireValueInList(values, true)
    .setAllowInvalid(true)
    .build();
  range.setDataValidation(rule);
}

/* =========================================================
 * Helpers
 * ========================================================= */

function getHeaderMap_(headers) {
  headers = Array.isArray(headers) ? headers : [];
  var map = {};
  for (var i = 0; i < headers.length; i++) {
    var raw = String(headers[i] == null ? '' : headers[i]);
    var trimmed = raw.trim();
    var normalized = formatNormalizeHeaderText_(raw);
    var col = i + 1;
    if (raw && map[raw] === undefined) map[raw] = col;
    if (trimmed && map[trimmed] === undefined) map[trimmed] = col;
    if (normalized && map[normalized] === undefined) map[normalized] = col;
    if (raw) { var rl = raw.toLowerCase(); if (map[rl] === undefined) map[rl] = col; }
    if (trimmed) { var tl = trimmed.toLowerCase(); if (map[tl] === undefined) map[tl] = col; }
    if (normalized) { var nl = normalized.toLowerCase(); if (map[nl] === undefined) map[nl] = col; }
  }
  return map;
}

function formatNormalizeHeaderText_(text) {
  if (text === null || text === undefined) return '';
  if (typeof _normalizeHeaderText_ === 'function') return _normalizeHeaderText_(text);
  var s = String(text);
  s = s.replace(/[\u200B\u200C\u200D\uFEFF]/g, '');
  s = s.replace(/[\s\u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000]+/g, ' ');
  return s.trim();
}

function formatListHasHeader_(list, headerName) {
  var target = formatNormalizeHeaderText_(headerName).toLowerCase();
  for (var i = 0; i < list.length; i++) {
    if (formatNormalizeHeaderText_(list[i]).toLowerCase() === target) return true;
  }
  return false;
}

/* =========================================================
 * Diagnostic helper
 * ========================================================= */

function verifyFormatVersion_() {
  var checks = [];
  var pass = 0;
  var fail = 0;

  function check(label, condition, detail) {
    var status = condition ? 'PASS' : 'FAIL';
    if (condition) { pass++; } else { fail++; }
    checks.push(status + ' \u2014 ' + label + (detail ? ' (' + detail + ')' : ''));
  }

  // --- version stamp ---
  check(
    'SCRIPT_FORMAT_VERSION is "2.18.0"',
    SCRIPT_FORMAT_VERSION === '2.18.0',
    'actual=' + SCRIPT_FORMAT_VERSION
  );

  // =========================================================
  // v2.14.0 FIX 1 — decision-owned page guard
  // =========================================================
  check(
    '_formatIsDecisionOwnedPage_ helper exists (v2.14.0 FIX 1)',
    typeof _formatIsDecisionOwnedPage_ === 'function'
  );
  check(
    'Top_10_Investments is decision-owned when 00_Config v1.12.6 is deployed (v2.14.0 FIX 1)',
    typeof isDecisionOwnedPage_ !== 'function' ||
    _formatIsDecisionOwnedPage_('Top_10_Investments') === true,
    typeof isDecisionOwnedPage_ === 'function'
      ? 'wired=' + _formatIsDecisionOwnedPage_('Top_10_Investments')
      : 'isDecisionOwnedPage_ absent — legacy mode (deploy 00_Config v1.12.6)'
  );

  // --- PRESERVED: Provider Rating is TEXT, not numeric (v2.10.0 FIX 1) ---
  check(
    "'Provider Rating' is NOT in _RATIO_COLUMNS_ (PRESERVED v2.10.0)",
    _RATIO_COLUMNS_.indexOf('Provider Rating') < 0
  );
  check(
    "'Provider Rating' is NOT in _PERCENT_COLUMNS_ (PRESERVED v2.10.0)",
    _PERCENT_COLUMNS_.indexOf('Provider Rating') < 0
  );
  check(
    "'Provider Rating' IS in _MEDIUM_COLUMNS_ (PRESERVED v2.10.0)",
    _MEDIUM_COLUMNS_.indexOf('Provider Rating') >= 0
  );

  // --- PRESERVED: Forecast Source / DOJI / Value View (v2.10.0 FIX 2-4) ---
  check(
    "'Forecast Source' IS in _MEDIUM_COLUMNS_ (PRESERVED v2.10.0)",
    _MEDIUM_COLUMNS_.indexOf('Forecast Source') >= 0
  );
  check(
    "'Forecast Source' IS in _MANAGED_HEADERS_SET_ (PRESERVED v2.10.0)",
    _MANAGED_HEADERS_SET_['Forecast Source'] === true
  );
  check(
    "CANDLE_SIGNAL_VOCAB_ contains 'DOJI' (PRESERVED v2.10.0)",
    CANDLE_SIGNAL_VOCAB_.indexOf('DOJI') >= 0
  );
  check(
    "VALUE_VIEW_VOCAB_ has both CHEAP/EXPENSIVE and UNDERVALUED/OVERVALUED (PRESERVED v2.10.0)",
    VALUE_VIEW_VOCAB_.indexOf('CHEAP') >= 0 &&
    VALUE_VIEW_VOCAB_.indexOf('EXPENSIVE') >= 0 &&
    VALUE_VIEW_VOCAB_.indexOf('UNDERVALUED') >= 0 &&
    VALUE_VIEW_VOCAB_.indexOf('OVERVALUED') >= 0
  );

  // --- PRESERVED: 8-tier Recommendation + Priority Band (v2.9.0 / v2.7.1) ---
  check(
    'RECOMMENDATION_VOCAB_ has 8 entries (PRESERVED v2.9.0)',
    RECOMMENDATION_VOCAB_.length === 8,
    'actual length=' + RECOMMENDATION_VOCAB_.length
  );
  check(
    'PRIORITY_BAND_VOCAB_ has 5 entries (PRESERVED v2.7.1)',
    PRIORITY_BAND_VOCAB_.length === 5
  );

  // --- PRESERVED: Top_10_Investments display header (v2.12.0) ---
  check(
    "'Top 10 Rank' IS in _NARROW_COLUMNS_ (PRESERVED v2.12.0)",
    _NARROW_COLUMNS_.indexOf('Top 10 Rank') >= 0
  );
  check(
    "'Top 10 Rank' IS in _INTEGER_COLUMNS_ (PRESERVED v2.12.0)",
    _INTEGER_COLUMNS_.indexOf('Top 10 Rank') >= 0
  );

  // --- PRESERVED: investability-gate placements + anti-percent guards (v2.11.0) ---
  ['Investability Status', 'Final Action', 'Provider/Engine Conflict',
   'Final Decision Basis'].forEach(function (h) {
    check("'" + h + "' IS in _MEDIUM_COLUMNS_ (PRESERVED v2.11.0)",
      _MEDIUM_COLUMNS_.indexOf(h) >= 0);
  });
  ['Conflict Type', 'Block Reason'].forEach(function (h) {
    check("'" + h + "' IS in _WIDE_COLUMNS_ (PRESERVED v2.11.0)",
      _WIDE_COLUMNS_.indexOf(h) >= 0);
  });
  var pointScores_ = ['Data Quality Score', 'Forecast Reliability Score'];
  for (var ps = 0; ps < pointScores_.length; ps++) {
    var psName = pointScores_[ps];
    check("'" + psName + "' IS in _RATIO_COLUMNS_ (PRESERVED v2.11.0)",
      _RATIO_COLUMNS_.indexOf(psName) >= 0);
    check("'" + psName + "' is NOT in _PERCENT_COLUMNS_ (anti-percent guard)",
      _PERCENT_COLUMNS_.indexOf(psName) < 0);
    check("'" + psName + "' is NOT in _SIGNED_PERCENT_COLUMNS_ (anti-percent guard)",
      _SIGNED_PERCENT_COLUMNS_.indexOf(psName) < 0);
  }

  // =========================================================
  // v2.13.0 FIX A — large-magnitude monetary columns
  // =========================================================
  var monetary_ = ['Market Cap', 'Revenue (TTM)', 'Free Cash Flow (TTM)'];
  for (var mi = 0; mi < monetary_.length; mi++) {
    var mName = monetary_[mi];
    check("'" + mName + "' IS in _INTEGER_COLUMNS_ (v2.13.0 FIX A)",
      _INTEGER_COLUMNS_.indexOf(mName) >= 0);
    // anti-percent: a monetary value must never carry a percent format.
    check("'" + mName + "' is NOT in _PERCENT_COLUMNS_ (v2.13.0 FIX A anti-percent)",
      _PERCENT_COLUMNS_.indexOf(mName) < 0);
    check("'" + mName + "' is NOT in _SIGNED_PERCENT_COLUMNS_ (v2.13.0 FIX A anti-percent)",
      _SIGNED_PERCENT_COLUMNS_.indexOf(mName) < 0);
    check("'" + mName + "' is NOT in _KNOWN_FRACTION_PERCENT_COLUMNS_ (v2.13.0 FIX A anti-percent)",
      _KNOWN_FRACTION_PERCENT_COLUMNS_.indexOf(mName) < 0);
    check("'" + mName + "' is NOT in _KNOWN_POINTS_PERCENT_COLUMNS_ (v2.13.0 FIX A anti-percent)",
      _KNOWN_POINTS_PERCENT_COLUMNS_.indexOf(mName) < 0);
  }

  // =========================================================
  // v2.13.0 FIX B — My_Portfolio weight columns (percent POINTS)
  // =========================================================
  var weights_ = ['Target Weight %', 'Actual Weight %', 'Weight Gap'];
  for (var wi = 0; wi < weights_.length; wi++) {
    var wName = weights_[wi];
    check("'" + wName + "' IS in _PERCENT_COLUMNS_ (v2.13.0 FIX B)",
      _PERCENT_COLUMNS_.indexOf(wName) >= 0);
    check("'" + wName + "' IS in _KNOWN_POINTS_PERCENT_COLUMNS_ (v2.13.0 FIX B \u2014 points, not fraction)",
      _KNOWN_POINTS_PERCENT_COLUMNS_.indexOf(wName) >= 0);
    check("'" + wName + "' is NOT in _KNOWN_FRACTION_PERCENT_COLUMNS_ (v2.13.0 FIX B anti-fraction)",
      _KNOWN_FRACTION_PERCENT_COLUMNS_.indexOf(wName) < 0);
  }
  check(
    "'Weight Gap' IS in _SIGNED_PERCENT_COLUMNS_ (v2.13.0 FIX B \u2014 signed)",
    _SIGNED_PERCENT_COLUMNS_.indexOf('Weight Gap') >= 0
  );

  // =========================================================
  // v2.13.0 FIX C — My_Portfolio decision columns
  // =========================================================
  ['Rebalance Action', 'Investor Decision'].forEach(function (h) {
    check("'" + h + "' IS in _MEDIUM_COLUMNS_ (v2.13.0 FIX C)",
      _MEDIUM_COLUMNS_.indexOf(h) >= 0);
    check("_MANAGED_HEADERS_SET_['" + h + "'] === true (v2.13.0 FIX C)",
      _MANAGED_HEADERS_SET_[h] === true);
  });
  check(
    'REBALANCE_ACTION_VOCAB_ is [ADD, REDUCE, HOLD] (v2.13.0 FIX C)',
    REBALANCE_ACTION_VOCAB_.length === 3 &&
    REBALANCE_ACTION_VOCAB_.indexOf('ADD') >= 0 &&
    REBALANCE_ACTION_VOCAB_.indexOf('REDUCE') >= 0 &&
    REBALANCE_ACTION_VOCAB_.indexOf('HOLD') >= 0
  );
  check(
    'INVESTOR_DECISION_VOCAB_ is [ADD, REDUCE, HOLD, SELL] (v2.13.0 FIX C)',
    INVESTOR_DECISION_VOCAB_.length === 4 &&
    INVESTOR_DECISION_VOCAB_.indexOf('SELL') >= 0
  );

  // =========================================================
  // v2.13.0 FIX D — Buy Date (date-only)
  // =========================================================
  check(
    "'Buy Date' IS in _DATE_ONLY_COLUMNS_ (v2.13.0 FIX D)",
    _DATE_ONLY_COLUMNS_.indexOf('Buy Date') >= 0
  );
  check(
    "'Buy Date' is NOT in _DATE_COLUMNS_ (v2.13.0 FIX D \u2014 date-only, not timestamp)",
    _DATE_COLUMNS_.indexOf('Buy Date') < 0
  );

  // =========================================================
  // v2.15.0 FIX — Volatility 30D / 90D are percent FRACTIONS
  // The engine emits these as fractions (0.10433 == 10.43%), same scale as
  // Max Drawdown 1Y / VaR 95% (1D). They must take the FRACTION branch
  // ('0.00%'), not the POINTS branch ('#,##0.00"%"') which rendered them
  // ~100x too small. They remain percent-formatted (in _PERCENT_COLUMNS_).
  // =========================================================
  var volFrac_ = ['Volatility 30D', 'Volatility 30D %',
                  'Volatility 90D', 'Volatility 90D %'];
  for (var vf = 0; vf < volFrac_.length; vf++) {
    var vName = volFrac_[vf];
    check("'" + vName + "' IS in _KNOWN_FRACTION_PERCENT_COLUMNS_ (v2.15.0 FIX \u2014 fraction, not points)",
      _KNOWN_FRACTION_PERCENT_COLUMNS_.indexOf(vName) >= 0);
    check("'" + vName + "' is NOT in _KNOWN_POINTS_PERCENT_COLUMNS_ (v2.15.0 FIX anti-points)",
      _KNOWN_POINTS_PERCENT_COLUMNS_.indexOf(vName) < 0);
    check("'" + vName + "' IS still in _PERCENT_COLUMNS_ (v2.15.0 \u2014 still percent-formatted)",
      _PERCENT_COLUMNS_.indexOf(vName) >= 0);
  }

  // =========================================================
  // v2.16.0 RESKIN — FORMAT_THEME_ proposal-palette assertions
  // Presentation-only: confirms the navy/gold cockpit palette is in place so
  // the main instrument pages match 16_Decision_Top10.gs / Portfolio_Decision.
  // =========================================================
  check("FORMAT_THEME_.HEADER_BG is proposal navy #1F3A5F (v2.16.0)",
    FORMAT_THEME_.HEADER_BG === '#1F3A5F', 'actual=' + FORMAT_THEME_.HEADER_BG);
  check("FORMAT_THEME_.HEADER_FONT is white #FFFFFF (v2.16.0)",
    FORMAT_THEME_.HEADER_FONT === '#FFFFFF');
  check("FORMAT_THEME_.BODY_FONT is proposal ink #1A2433 (v2.16.0)",
    FORMAT_THEME_.BODY_FONT === '#1A2433');
  check("FORMAT_THEME_.ALT_ROW_BG is proposal zebra #F5F7FA (v2.16.0; WIRED in v2.17.0 FIX 2)",
    FORMAT_THEME_.ALT_ROW_BG === '#F5F7FA');
  check("FORMAT_THEME_.POSITIVE_FONT is proposal green #146C43 (v2.16.0)",
    FORMAT_THEME_.POSITIVE_FONT === '#146C43');
  check("FORMAT_THEME_.NEGATIVE_FONT is proposal red #B3261E (v2.16.0)",
    FORMAT_THEME_.NEGATIVE_FONT === '#B3261E');
  check("FORMAT_THEME_.WARNING_FONT is proposal amber #9A6700 (v2.16.0)",
    FORMAT_THEME_.WARNING_FONT === '#9A6700');
  check("FORMAT_THEME_.NEUTRAL_FONT is proposal muted #5B6470 (REFERENCE-ONLY \u2014 no rule in 04_Format paints neutral)",
    FORMAT_THEME_.NEUTRAL_FONT === '#5B6470');
  check("FORMAT_THEME_.NEUTRAL_BG is proposal muted bg #ECEEF1 (REFERENCE-ONLY \u2014 reaches no cell)",
    FORMAT_THEME_.NEUTRAL_BG === '#ECEEF1');
  check("FORMAT_THEME_.EDITABLE_BG is proposal operator-blue bg #E8F0FE (v2.16.0)",
    FORMAT_THEME_.EDITABLE_BG === '#E8F0FE');
  check("FORMAT_THEME_.EDITABLE_FONT is proposal operator-blue #1A57C2 (v2.16.0)",
    FORMAT_THEME_.EDITABLE_FONT === '#1A57C2');
  check("FORMAT_THEME_.BORDER is proposal border #D6DEE8 (v2.16.0)",
    FORMAT_THEME_.BORDER === '#D6DEE8');
  // anti-regression: the two already-on-palette values stayed put.
  check("FORMAT_THEME_.POSITIVE_BG remained #E6F4EA (v2.16.0)",
    FORMAT_THEME_.POSITIVE_BG === '#E6F4EA');

  // =========================================================
  // v2.17.0 FIX 1 — risk-column SCALE SPLIT (false-green removed)
  // The 70/40 ramp is valid ONLY on a 0-100 column. The fraction-scale columns
  // are deliberately uncoloured until v2.18.0 lands evidence-derived bands.
  // =========================================================
  check("_NEGATIVE_RISK_POINTS_COLUMNS_ is exactly ['Risk Score'] (v2.17.0 FIX 1)",
    _NEGATIVE_RISK_POINTS_COLUMNS_.length === 1 &&
    _NEGATIVE_RISK_POINTS_COLUMNS_[0] === 'Risk Score');

  var fracRisk_ = ['VaR 95% (1D)', 'Volatility 30D', 'Volatility 30D %',
                   'Volatility 90D', 'Volatility 90D %',
                   'Max Drawdown 1Y', 'Max DD 1Y %'];
  for (var fr = 0; fr < fracRisk_.length; fr++) {
    var frName = fracRisk_[fr];
    check("'" + frName + "' IS in _NEGATIVE_RISK_FRACTION_COLUMNS_ (fraction-scale \u2014 v2.18.0 ABS-banded)",
      _NEGATIVE_RISK_FRACTION_COLUMNS_.indexOf(frName) >= 0);
    check("'" + frName + "' is NOT in _NEGATIVE_RISK_POINTS_COLUMNS_ (v2.17.0 FIX 1 anti-false-green)",
      _NEGATIVE_RISK_POINTS_COLUMNS_.indexOf(frName) < 0);
    // THE TRAP: it must stay in the union, or _filterPreservedRules_ keeps the
    // stale v2.16.0 green rule forever and the fix silently no-ops.
    check("'" + frName + "' IS still in _NEGATIVE_RISK_COLUMNS_ union \u2014 stale-rule SWEEP (v2.17.0 FIX 1 CRITICAL)",
      _NEGATIVE_RISK_COLUMNS_.indexOf(frName) >= 0);
    check("_MANAGED_HEADERS_SET_['" + frName + "'] === true \u2014 stale-rule SWEEP (v2.17.0 FIX 1 CRITICAL)",
      _MANAGED_HEADERS_SET_[frName] === true);
  }
  check("points + fraction lists partition the union exactly (v2.17.0 FIX 1)",
    _NEGATIVE_RISK_POINTS_COLUMNS_.length + _NEGATIVE_RISK_FRACTION_COLUMNS_.length ===
    _NEGATIVE_RISK_COLUMNS_.length);
  // anti-regression: the fraction columns must KEEP their correct v2.15.0 display.
  check("fraction-risk columns still render as fractions (v2.15.0 preserved under v2.17.0)",
    _KNOWN_FRACTION_PERCENT_COLUMNS_.indexOf('Volatility 30D') >= 0 &&
    _KNOWN_FRACTION_PERCENT_COLUMNS_.indexOf('VaR 95% (1D)') >= 0 &&
    _KNOWN_FRACTION_PERCENT_COLUMNS_.indexOf('Max Drawdown 1Y') >= 0);

  // =========================================================
  // v2.17.0 FIX 2 — zebra actually wired to the proposal palette
  // =========================================================
  // =========================================================
  // v2.18.0 — evidence-derived ABS bands on the fraction risk columns
  // =========================================================
  var bandCols_ = Object.keys(_NEGATIVE_RISK_FRACTION_BANDS_);
  check("every fraction column has a band (7/7) (v2.18.0)",
    bandCols_.length === _NEGATIVE_RISK_FRACTION_COLUMNS_.length);
  for (var bc = 0; bc < bandCols_.length; bc++) {
    var bcName = bandCols_[bc];
    var bcBand = _NEGATIVE_RISK_FRACTION_BANDS_[bcName];
    check("band column '" + bcName + "' IS in _NEGATIVE_RISK_FRACTION_COLUMNS_ (v2.18.0 partition)",
      _NEGATIVE_RISK_FRACTION_COLUMNS_.indexOf(bcName) >= 0);
    check("band '" + bcName + "' is ordered 0 < green_lt < red_gt (v2.18.0)",
      bcBand && bcBand.length === 2 && bcBand[0] > 0 && bcBand[0] < bcBand[1]);
  }
  check("Volatility 30D band is [0.34, 0.64] \u2014 median/p85 of the 2026-07-12 universe",
    _NEGATIVE_RISK_FRACTION_BANDS_['Volatility 30D'][0] === 0.34 &&
    _NEGATIVE_RISK_FRACTION_BANDS_['Volatility 30D'][1] === 0.64);
  check("Max Drawdown 1Y band is [0.29, 0.51]",
    _NEGATIVE_RISK_FRACTION_BANDS_['Max Drawdown 1Y'][0] === 0.29 &&
    _NEGATIVE_RISK_FRACTION_BANDS_['Max Drawdown 1Y'][1] === 0.51);
  check("VaR 95% (1D) band is [0.03, 0.05]",
    _NEGATIVE_RISK_FRACTION_BANDS_['VaR 95% (1D)'][0] === 0.03 &&
    _NEGATIVE_RISK_FRACTION_BANDS_['VaR 95% (1D)'][1] === 0.05);
  check("generator evaluates ABS via whenFormulaSatisfied (v2.18.0 \u2014 sign is convention, not information)",
    String(applyConditionalFormatting_).indexOf('whenFormulaSatisfied') >= 0 &&
    String(applyConditionalFormatting_).indexOf('ABS(') >= 0);
  check("_cfColLetterA1_ converts 1->A, 26->Z, 27->AA, 40->AN (v2.18.0)",
    _cfColLetterA1_(1) === 'A' && _cfColLetterA1_(26) === 'Z' &&
    _cfColLetterA1_(27) === 'AA' && _cfColLetterA1_(40) === 'AN');

  check("FORMAT_THEME_.BASE_ROW_BG is #FFFFFF (v2.17.0 FIX 2 \u2014 light half of the zebra pair)",
    FORMAT_THEME_.BASE_ROW_BG === '#FFFFFF');
  check("applyAlternatingRows_ READS ALT_ROW_BG (v2.17.0 FIX 2 \u2014 v2.16.0 asserted it but never used it)",
    String(applyAlternatingRows_).indexOf('ALT_ROW_BG') >= 0);
  check("applyAlternatingRows_ READS BASE_ROW_BG (v2.17.0 FIX 2)",
    String(applyAlternatingRows_).indexOf('BASE_ROW_BG') >= 0);

  // --- output ---
  Logger.log('=== verifyFormatVersion_ \u2014 v2.18.0 ===');
  Logger.log('Total checks: ' + checks.length + '   PASS: ' + pass + '   FAIL: ' + fail);
  Logger.log('---');
  for (var i = 0; i < checks.length; i++) {
    Logger.log(checks[i]);
  }
  Logger.log('---');
  if (fail === 0) {
    Logger.log('\u2713 ALL CHECKS PASSED \u2014 v2.18.0 deployed correctly.');
    Logger.log('');
    Logger.log('Next: run "Apply Sheet Formatting" / "Reformat Current Page".');
    Logger.log('  A. Market Cap / Revenue (TTM) / Free Cash Flow (TTM) show as');
    Logger.log('     plain grouped numbers (#,##0), NOT "<number>%".');
    Logger.log('  B. (My_Portfolio) Target/Actual Weight % and Weight Gap show');
    Logger.log('     "40.00%" style; Weight Gap is signed with up/down arrows.');
    Logger.log('  C. (My_Portfolio) Rebalance Action / Investor Decision render');
    Logger.log('     ADD green, HOLD amber, REDUCE/SELL red; both have dropdowns.');
    Logger.log('  D. (My_Portfolio) Buy Date renders yyyy-mm-dd (no time).');
    Logger.log('  E. Volatility 30D / 90D show "10.43%" style (fraction x100),');
    Logger.log('     matching Max Drawdown / VaR \u2014 NOT "0.10%".');
    Logger.log('');
    Logger.log('  Note: 04_Format styles only columns that ALREADY exist on the');
    Logger.log('  sheet, and it does not clear a stale format until it runs. If a');
    Logger.log('  page still shows "%" on a monetary column, reformat that page \u2014');
    Logger.log('  the explicit #,##0 now wins. For missing 122 columns on');
    Logger.log('  My_Portfolio, run 00_Config.gs v1.12.3 "Update Current Headers".');
    Logger.log('');
    Logger.log('  F. Volatility 30D/90D, VaR 95% (1D) and Max Drawdown 1Y now carry');
    Logger.log('     EVIDENCE-DERIVED bands (v2.18.0): green below the universe median,');
    Logger.log('     red above p85, evaluated on ABS(value) so the negative loss');
    Logger.log('     convention on VaR/MDD cannot invert the ramp. Vol30 0.34/0.64,');
    Logger.log('     Vol90 0.36/0.64, VaR 0.03/0.05, MDD 0.29/0.51. "Red" means');
    Logger.log('     "riskier than 85% of the live universe". Unadjusted-split garbage');
    Logger.log('     (vol > 300%) paints red until data_engine_v2 v5.115.0 nulls it');
    Logger.log('     upstream \u2014 red is the honest colour for garbage.');
    Logger.log('  G. Zebra banding paints #FFFFFF / #F5F7FA (v2.17.0 FIX 2),');
    Logger.log('     not Google stock LIGHT_GREY. Visual check on any main page.');
  } else {
    Logger.log('\u2717 ' + fail + ' CHECK(S) FAILED \u2014 review the log above.');
  }

  return { ok: fail === 0, pass: pass, fail: fail, total: checks.length, checks: checks };
}
