/**
 * 10_My_Portfolio.gs
 * Tadawul Tracker — My Portfolio Page Module
 * Version: 1.5.11
 *
 * v1.5.11 MAIN CHANGES  (SOURCE RECEIPT + RECONCILIATION EVIDENCE TRANSPORT —
 *                        receipt default OFF, evidence inert until the owner
 *                        populates it; ALL v1.5.10 logic carried verbatim)
 * ---------------------------------------------------------------
 * WHY (2026-10-14 full project review; the repository's real
 * final-publication audit replayed offline on the 14 Oct export, sync run
 * 38486122061):
 * (A) Final publication fails PF_SOURCE_CONSUMPTION_UNPROVEN on every run.
 *     audit_decision_surface_freshness 1.3.x requires Portfolio_Decision B2
 *     to carry exactly one 'source readiness VERIFIED (run <id>)' receipt
 *     whose run equals the My_Portfolio _Status publication run. This
 *     producer never wrote one; the replay shows a correct receipt clears
 *     the finding.
 * (B) Every holding renders BLOCK 'RECONCILIATION_REQUIRED: missing_evidence'.
 *     portfolio_actions 1.15+ certifies holdings only from
 *     body.reconciliation_evidence, and this caller never sent it.
 *     16_Decision_Top10 already forwards the private document property
 *     TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1; this page did not.
 * WHAT:
 *  (A) Script Property PD_SOURCE_RECEIPT = off (default) | observe | enforce
 *      (any other value = off).
 *      off     -> no _Status read; the status line is v1.5.10 byte-identical.
 *      observe/enforce -> the _Status My_Portfolio row is captured BEFORE the
 *      holdings read and re-checked after the render (a one-page port of
 *      16_Decision_Top10 dt10SourceReadinessCore_): exactly one row; one
 *      positive run= token; status OK/SUCCESS/VALID/PASS/COMPLETE; timestamp
 *      parseable, not in the future, at most 8 h old (the final audit's
 *      My_Portfolio window); acquired N/N (100%); acquisition=COMPLETE;
 *      data=COMPLETE; fetchfail_effective off/observe/enforce; published
 *      rows == holdings consumed; the row unchanged and no newer than the
 *      capture; decision status ok.
 *      observe -> appends ' | [receipt-observe] would publish VERIFIED
 *                 (run N)' (or 'WITHHELD: why'), text the audit ignores.
 *      enforce -> appends ' | source readiness VERIFIED (run N)' or
 *                 ' | source readiness WITHHELD: why' (exactly one).
 *  (B) pdEvidencePacket_ reads that document property with Top_10's
 *      contract (string, <= 9,000 chars, JSON object, schema_version 1)
 *      and, when present, sends it as body.reconciliation_evidence.
 *      Nothing writes the property today, so the request is unchanged
 *      until an authorized capture populates it. Never logged or rendered.
 *  pdSelfTest also reports the receipt mode, the current My_Portfolio
 *  publication verdict and whether the evidence property is present
 *  (never its contents).
 * ES5. Functions added: 10 (pdReceiptMode_, pdEvidencePacket_,
 * pdSrcNormToken_, pdSrcStampMs_, pdSrcToken_, pdSrcCount_,
 * pdSrcReadinessCore_, pdSrcReadiness_, pdSrcCapture_, pdReceiptNote_).
 * Removed: 0. Rollback: unset PD_SOURCE_RECEIPT (receipt) and leave the
 * evidence property unset, or reinstall v1.5.10.
 *
 * v1.5.10 MAIN CHANGES  (DELTA SHARES RENDERS FOR TRIM/EXIT — one-expression
 *                        render fix; ALL v1.5.9 logic carried verbatim,
 *                        zero regressions, §8 honored)
 * ---------------------------------------------------------------
 * WHY (2026-09-07 six-gate audit + off-platform real-module harness):
 * portfolio_actions v1.9.0 (D1, 2026-08-26) share-resolves the SELL side —
 * every EXIT/TRIM row carries a SIGNED INTEGER suggested_delta_shares
 * (EXIT: -quantity; TRIM: the smallest whole-share count whose sale brings
 * post-trade weight to/below the position cap). The 2026-09-07 live board
 * proved the value is computed but never shown: HCI.US TRIM rendered
 * Delta SAR -1,746 with Delta Shares BLANK, while the harness (the REAL
 * build_portfolio_actions, the board's own seven holdings, x3 identical
 * runs) emitted suggested_delta_shares = -3 (post-trade weight 19.6% vs
 * the 20% cap). ROOT CAUSE is ONE expression in pdActionToRow_: the
 * Delta Shares cell was gated ADD-only
 * (a.action === 'ADD' ? pdCell_(a.suggested_delta_shares) : '') — written
 * for v1.5.0, when only the ADD funding pass populated the field, and
 * never revisited when D1 extended it to the sell side. The renderer
 * therefore DISCARDED the backend's integer. (The Delta SAR cell proved
 * the bespoke branch: for TRIM/EXIT it renders -proceeds_sar, which is
 * why -1,746 appeared while the payload's suggested_delta_sar is None on
 * TRIM rows.)
 * WHAT (one expression): the Delta Shares cell is now unconditionally
 * pdCell_(a.suggested_delta_shares).
 *   - ADD rows: byte-identical (same field, same pdCell_).
 *   - TRIM/EXIT: render the signed integer when the backend supplies it;
 *     pdCell_ maps null/undefined -> '' so a pre-1.9.0 backend renders
 *     exactly the previous blank — forward AND backward compatible.
 *   - Column 16 already carries PD_FMT_INT; no format-map change.
 *   - _Portfolio_Action_Log inherits automatically (pdActLogRowFromAction_
 *     reuses pdActionToRow_ verbatim); log width unchanged (32).
 * DELIBERATELY NOT CHANGED: Delta SAR for TRIM/EXIT stays -proceeds_sar
 * (the cap-exact SAR target); Delta Shares is the smallest whole-share
 * EXECUTION that clears the cap, so the two can differ by sub-share
 * rounding (HCI: -1,746 target vs -3 sh ~ -2,113 executable) — target and
 * execution shown side by side, per the audit rule 'no advisory SAR
 * amount without an executable quantity'. post_trade_weight_pct (also new
 * in D1) still has no column — Register item; schema change deliberately
 * deferred.
 * ES5. ZERO functions added or removed; ONE expression changed inside
 * pdActionToRow_; PD_VERSION and the header version bumped in lockstep.
 *
 * v1.5.9 MAIN CHANGES  (LEDGER-V2 AWARENESS + PORTFOLIO ACTION LOG PORT —
 *                       two coupled fixes shipped together by design; ALL
 *                       v1.5.8 logic carried verbatim, zero regressions)
 * ---------------------------------------------------------------
 * (A) LEDGER-V2 AWARENESS. 21_Portfolio_Ledger.gs v1.0.0 migrates
 * _Portfolio_CostBasis to the v2 lot ledger: header at ROW 4 (23 cols,
 * 'Symbol'..'Holding Days'), data rows 5+, Status dropdown (Active/Inactive),
 * qty under 'Shares' (not 'Quantity'), one row per LOT (a symbol can have
 * many). This file had THREE readers that all assumed the v1 layout (header
 * row 1, 'Quantity'):
 *   1. the seed/reseed hooks (via 14_Portfolio_PnL seedPortfolioInputsFrom-
 *      CostBasis_) — v1-layout reader; against a migrated sheet it silently
 *      seeds nothing;
 *   2. the symbol-universe union (_myPortfolioCostBasisSymbols_) — row-1
 *      'Symbol' scan finds nothing on v2 -> [] -> HIDE-UNHELD bypassed,
 *      target-only BLOCK rows reappear, and the cost-basis wipe-guard
 *      fallback goes blind;
 *   3. the decision-layer qty/cost override (pdCollectHoldings_ via
 *      readCostBasisMap_) — inherits whatever 14's reader vintage does
 *      against the v2 sheet: a row-1 reader returns {} (the exact v1.5.4
 *      BLOCK bug reborn); a scanning reader happens to cope. Either way the
 *      override is one 14 paste away from going blind — v1.5.9 removes the
 *      cross-file dependency for v2 books.
 * FIX (all three, one shared reader):
 *   - NEW _myPortfolioLedgerMap_() detects v2 by the row-4 fingerprint
 *     (A4 === 'Symbol', matching 18_Decision_Diagnostics v1.3.0), locates
 *     Symbol/Status/Buy Date/Buy Price/Shares/Buy Fees BY HEADER NAME on
 *     row 4, and aggregates ACTIVE lots per symbol: qty = sum of Shares;
 *     cost = shares-weighted average Buy Price INCLUDING Buy Fees,
 *     deliberately UNROUNDED — numerically identical to 21's
 *     syncLedgerToPortfolio and 14 v2.5.0's pfReadCostBasisV2_, so the
 *     feeders can never oscillate the Avg Cost cell between runs; Buy Date
 *     = earliest lot. Inactive lots are excluded per the FEEDER RULE
 *     (My_Portfolio reads ACTIVE lots only), so a fully-sold symbol drops
 *     off the universe on the next refresh — and the working-list
 *     resurrection of sold rows (21's prune deletes the row, the next page
 *     refresh re-adds it from the working list) ends here, because the
 *     held-filter now sees the ACTIVE set. Returns NULL when the sheet is
 *     absent / still v1 / unreadable, so every caller falls through to the
 *     legacy chain.
 *   - _myPortfolioCostBasisSymbols_ returns the v2 ACTIVE-symbol set when v2
 *     is detected; otherwise the legacy chain (readCostBasisMap_ -> row-1
 *     direct read) runs verbatim. Once v2 is detected the legacy chain is
 *     NEVER consulted (it would read the row-1 title banner as a header).
 *   - pdCollectHoldings_ prefers the v2 aggregate for the qty/cost override;
 *     legacy chain unchanged otherwise. Same {qty, buyPrice} map shape.
 *   - The seed/reseed hooks prefer 21's syncLedgerToPortfolio() (the ACTIVE-
 *     lot feeder that OWNS the input columns and prunes unheld rows) via the
 *     shared _myPortfolioLedgerSeed_ helper when 21 is installed; 14's
 *     seeder remains ONLY as the fallback for 14-only installs.
 *     This also removes the dual-writer from this file's side (14's own seed
 *     retirement is a separate 14_Portfolio_PnL revision). On feeder failure
 *     the hook degrades to a WARN — it never falls back to the v1 seeder
 *     against a migrated ledger.
 * WIPE-GUARDS preserved: v2-empty / v2-read-failure both yield an empty
 * cost-basis set -> the v1.5.6 held-filter is bypassed and the v1.5.1 union
 * survives; a transient ledger problem can never blank the page. Deploy
 * order safe both ways: without 21 pasted (and ledger un-migrated) every
 * path behaves exactly as v1.5.8.
 *
 * (B) PORTFOLIO ACTION LOG PORT (item 12 — closes the last capability gap
 * from retiring 17_Selection_Log.gs). The cockpit now owns its audit trail,
 * same pattern as 16_Decision_Top10 v1.3.1: after every successful render,
 * refreshPortfolioDecision appends the IN-MEMORY actions to
 * _Portfolio_Action_Log — no sheet-offset re-reads, so future layout changes
 * cannot break the trail (what broke 17 twice). Details:
 *   - SCHEMA: 17's documented row preserved — [Logged At | Run Info | Source
 *     Page] + the 26 ACTIONS grid columns VERBATIM (pdActionToRow_ is reused
 *     field-for-field) + [Panel Snapshot (JSON) | Outcome | Review Notes]
 *     (Outcome / Review Notes are blank operator columns, L14). 32 columns.
 *   - HEADER SELF-HEAL: fresh sheet -> full header. A legacy 17-era sheet
 *     (25-col grid, no 'Engine ROI %') gets ONE column INSERTED right after
 *     the 'ROI %' header column, so historical rows keep their alignment and
 *     their new cell is honestly blank. Blank header cells are stamped;
 *     non-blank cells are never overwritten.
 *   - CADENCE (ScriptProperty PD_ACTION_LOG): 'off' kill-switch | 'always'
 *     every run | unset DEFAULT = once per UTC day PLUS any run whose logged
 *     action set changed (signature = sorted Symbol|Action join, so an
 *     action FLIP re-logs same-day; repeated identical runs do not flood).
 *   - HOLD rows are skipped by default (17's default: ADD/TRIM/EXIT/BLOCK
 *     are the decisions, HOLD is the steady state) and the skip is DISCLOSED
 *     in the note — 'ActLog: +N (M HOLD skipped)'; ScriptProperty
 *     PD_ACTLOG_HOLDS='1' captures everything.
 *   - NEVER THROWS: any failure becomes an 'ActLog: ERROR ...' tail on the
 *     status line; the render is never touched. State (day + signature)
 *     lives in ScriptProperty PD_ACTLOG_STATE and is saved ONLY when rows
 *     are actually appended, so a skipped/empty run can never mask a later
 *     real change.
 * The _Portfolio_Action_Log WARN in 18_Decision_Diagnostics v1.3.0 clears on
 * the first refreshPortfolioDecision run after this paste.
 *
 * FUNCTIONS: all 69 v1.5.8 functions carried verbatim; TEN added
 * (_myPortfolioLedgerMap_, _myPortfolioLedgerSeed_, pdActLogMode_,
 * pdActLogToday_, pdActLogSignature_, pdActLogRowFromAction_,
 * pdActLogStateLoad_, pdActLogStateSave_, pdActLogSheet_,
 * pdAppendActionLog_) = 79. SIX bodies extended (_myPortfolioSeedManual-
 * Inputs_, _myPortfolioReseedInputsAfterRefresh_, _myPortfolioCostBasis-
 * Symbols_, pdCollectHoldings_, refreshPortfolioDecision, pdSelfTest).
 * FIVE constants added (PD_ACTLOG_*).
 * Aligned with: 16_Decision_Top10.gs v1.3.1, 21_Portfolio_Ledger.gs v1.0.0,
 * 18_Decision_Diagnostics.gs v1.3.0, 14_Portfolio_PnL.gs v2.4.0 (seed path
 * retirement pending there), portfolio_actions v1.0.5, route v4.11.0.
 *
 * OPERATIONAL: paste AFTER 21_Portfolio_Ledger.gs is live — this file
 * PREFERS its feeder (with only 14 present, behaviour is unchanged from
 * v1.5.8; with neither, seeding skips honestly). No build step needed —
 * the log sheet self-creates / self-heals on first use. Expected first
 * 'Refresh Portfolio Decision' against the current book (1 ADD + 1 TRIM +
 * 7 HOLD): status line ends '… | ActLog: +2 (7 HOLD skipped)', and
 * 18_Decision_Diagnostics' LOGS WARN for _Portfolio_Action_Log flips to
 * PASS on its next run. Then DELETE 17_Selection_Log.gs (its menu items,
 * triggers and file) — its manual-run wrapper writes this same sheet and
 * would double-log every manual refresh.
 *
 * v1.5.8 MAIN CHANGES  (PROPOSAL-PALETTE RESKIN of the Portfolio_Decision
 *                       cockpit — PRESENTATION ONLY; ALL v1.5.7 logic carried
 *                       verbatim, zero regressions, §8 honored)
 * ---------------------------------------------------------------
 * Restyles the 'Portfolio_Decision' sheet to the approved TFB_Final_Proposal
 * navy/gold cockpit palette so it matches 16_Decision_Top10.gs (Top 10
 * cockpit, delivered v1.1.0) and the rest of the reskin. This is the PD_*
 * section ONLY — the 122-column My_Portfolio engine page above is untouched.
 * NOTHING about behaviour changes: no schema, no payload field, no column
 * count (ACTIONS stay 26 / PD_LAST_COL 26), no sizing, no action/gate/sort
 * logic, no verdict mapping. Only fill/font colours, a gold title rule, the
 * version marker and this block change. All 69 functions are carried verbatim.
 * PALETTE (from TFB_Final_Proposal 'Portfolio Evaluation' sheet):
 *   - brand navy  #1F3A5F  -> title banner + CONTROL PANEL / KPIs / section
 *                             banners + every column-header row (white text)
 *   - light tint  #EEF2F7  -> KPI tiles + control-panel labels (navy text)
 *   - body zebra  #F5F7FA  (was #FAFAFA)
 *   - border      #D6DEE8  (was #E0E0E0)
 *   - ink         #1A2433  -> table body text
 *   - muted       #6B7280  -> status / footer / labels (was #5F6368)
 *   - gold        #C8A04B  -> title bottom-rule accent ONLY (not a data column)
 *   - verdicts    ADD green #146C43/#E6F4EA, TRIM amber #9A6700/#FFF3CD,
 *                 EXIT red #B3261E/#FBE4E2, BLOCK muted #5B6470/#ECEEF1
 *   - operator    blue #1A57C2 on #E8F0FE
 * pdColorActions_ maps ADD/TRIM/EXIT/BLOCK onto the VERDICT_* tokens, so it
 * auto-adopts the new verdict colours with no change to its own code. The
 * design-token named range (TFB_DESIGN_TOKENS) publishes COLOR_* keys, which
 * the renderers never read (they read HEADER / VERDICT_* / OPERATOR_INPUT), so
 * recolouring the PD_FALLBACK_TOKENS map reskins immediately on the next
 * Build + Refresh — the schema contract is never reopened.
 * OPERATIONAL: paste + Save, then run 'Build Portfolio Decision Layout' ONCE
 * (restyles the fixed title / control panel / KPI band), then 'Refresh
 * Portfolio Decision' (restyles the dynamic ACTIONS / SECTOR / ALERTS zones
 * every run). Order matters. Portfolio_Decision reads My_Portfolio, so refresh
 * My_Portfolio first.
 *
 * v1.5.7 MAIN CHANGES  (ENGINE ROI % COLUMN on Portfolio_Decision — single
 *                       additive column; ALL v1.5.6 logic carried verbatim,
 *                       zero regressions)
 * ---------------------------------------------------------------
 * The Portfolio_Decision ACTIONS grid gains one column, 'Engine ROI %', placed
 * immediately after the existing 'ROI %'. They are DIFFERENT measures, shown
 * side by side and never substituted:
 *   - 'ROI %' (roi_pct) is the holding's VALUATION upside the action engine
 *     already surfaces.
 *   - 'Engine ROI %' (engine_roi_pct) is the engine's honest 12-month forecast
 *     return, emitted per action row by portfolio_actions. It is already in
 *     percent units (same scale as ROI %), so it renders with PD_FMT_PCT.
 * SINGLE MASTER SWITCH: portfolio_actions only populates engine_roi_pct when
 * the backend env flag TFB_PF_ENGINE_ROI_DISPLAY=1 (portfolio_actions v1.0.1+).
 * With the flag OFF the field is absent and pdCell_ renders the cell blank — so
 * the column is inert (header shows, cells empty) until the flag is set on
 * Render. There is deliberately NO separate GAS flag.
 * CHANGES (render-only; §8 honored — no action/sizing logic added):
 *   1. PD_ACTION_HEADERS: 'Engine ROI %' inserted after 'ROI %' (25 -> 26).
 *   2. pdActionToRow_: pdCell_(a.engine_roi_pct) emitted after a.roi_pct.
 *   3. PD_LAST_COL 25 -> 26; the ACTIONS format map shifts the post-ROI%
 *      columns by one (col 22 Engine ROI % = PD_FMT_PCT). All PD_LAST_COL-
 *      driven merges, column widths, and the advisor-note wrap auto-follow.
 * The SECTOR SUMMARY and ALERTS tables are unchanged. No change to holdings
 * collection, the cost-basis override, KPIs, the control panel, or any
 * My_Portfolio 115/122 page logic. Every other byte carried verbatim from
 * v1.5.6.
 *
 * v1.5.6 MAIN CHANGES  (HIDE UNHELD/TARGET-ONLY SYMBOLS — single fix; ALL v1.5.5
 *                       logic carried verbatim, zero regressions)
 * ---------------------------------------------------------------
 * SYMPTOM: My_Portfolio and My_Portfolio_Decision show unrelated symbols (e.g.
 * the configured target-allocation tickers) as BLOCK / "missing quantity" even
 * though they are not held. ROOT CAUSE: the symbol universe is the UNION of the
 * page working list and the cost-basis symbols (v1.5.3); a ticker listed in the
 * working set but never bought has no cost-basis row -> no quantity -> BLOCK.
 * FIX: new module flag MY_PORTFOLIO_HIDE_UNHELD_ (default true) restricts the
 * universe to symbols that actually have a position in _Portfolio_CostBasis via
 * the new helper _filterToHeldSymbols_. The filter runs ONLY when cost-basis is
 * non-empty, so the v1.5.1 WIPE-GUARD still holds (a transient empty read can
 * never blank the page). Because both pages derive from the same written sheet,
 * this clears the stray BLOCK rows from BOTH. Set the flag to false to restore
 * the v1.5.3 union behavior. NOTE: this does not touch P&L — an incorrect P&L is
 * an input/data issue in _Portfolio_CostBasis (e.g. a wrong Buy Price), not a
 * symbol-universe issue.
 *
 * v1.5.5 MAIN CHANGES  (POST-REFRESH DISPLAY RE-SEED — single fix; ALL v1.5.4
 *                       logic carried verbatim, zero regressions)
 * ---------------------------------------------------------------
 * SYMPTOM: after a My_Portfolio refresh, the page's own Position Qty / Avg Cost
 * cells (canonical cols 75/76) are BLANK — so the per-row economics look empty
 * on My_Portfolio itself, even though the decision page is correct. ROOT CAUSE
 * (documented since v1.5.4): the cost-basis seed writes those manual inputs
 * BEFORE refresh, the engine reads them and computes the position math, but the
 * engine does NOT preserve Position Qty / Avg Cost on write-back, so the two
 * input cells are blanked by the time the refresh returns.
 *
 * FIX: a guarded POST-refresh re-seed. After refresh (and validation), this file
 * re-runs 14_Portfolio_PnL.gs's existing seedPortfolioInputsFromCostBasis_ to
 * restore Position Qty / Avg Cost / Buy Date / Target Weight % onto the page for
 * DISPLAY. This is SAFE: it writes ONLY the manual-input columns (exactly the
 * same cells the existing pre-refresh seed writes) — it never touches an
 * engine-output column, never changes width, is sort-safe (the seeder re-joins
 * on Symbol), and is idempotent. New entry point: _myPortfolioReseedInputsAfter-
 * Refresh_ (called from _myPortfolioRunOptionalPostProcessors_, BEFORE the
 * validation pass so the page is validated in its final displayed state).
 * Default-ON; disable per-call with options.reseedAfterRefresh === false. It is
 * a safe no-op when 14_Portfolio_PnL.gs is absent (typeof-guarded). No other
 * change from v1.5.4.
 *
 * BOUNDARY (read this): the re-seed restores the INPUT columns (Position Qty /
 * Avg Cost / Buy Date / Target Weight %). If Position Value / Unrealized P&L
 * (engine-COMPUTED columns) ALSO show blank after a refresh, that means the
 * engine is not persisting the computed position math — and the complete fix is
 * a small backend change (add Position Qty / Avg Cost to data_engine_v2's
 * manual-preserve whitelist). This file deliberately does NOT write any
 * engine-output column: that path was retired in 14_Portfolio_PnL.gs v2.0.0
 * because it corrupted the 122-column contract.
 *
 * v1.5.4 MAIN CHANGES  (DECISION READS COST BASIS DIRECTLY — single fix; ALL
 *                       v1.5.3 logic carried verbatim, zero regressions)
 * ---------------------------------------------------------------
 * SYMPTOM: every holding on Portfolio_Decision returned BLOCK with
 * "Data gate: missing quantity". The backend RECEIVED the holdings
 * (status holdings=13) and processed them — it was not a payload/key
 * rejection — but Quantity and Avg Cost arrived empty, because the manual
 * Position Qty / Avg Cost cells on My_Portfolio were blank at read time. The
 * cost-basis seed writes them pre-refresh, but the engine refresh does not
 * preserve those two canonical columns, so they are blank again by the time
 * the decision page reads My_Portfolio. (Adding snake_case key aliases does
 * nothing here: the value is absent, not mis-keyed.)
 *
 * FIX: pdCollectHoldings_ now reads Quantity + cost DIRECTLY from
 * _Portfolio_CostBasis (via readCostBasisMap_, typeof-guarded) and OVERRIDES
 * whatever My_Portfolio holds for those fields when the cost-basis row supplies
 * them. The decision POST therefore carries qty + cost from the durable source
 * regardless of the seed/preserve hop. Cost is sent under BOTH 'Buy Price' (the
 * proven backend key) and 'Avg Cost'. The status line reports how many rows got
 * qty/cost from cost basis. This fixes ONLY the decision layer; My_Portfolio's
 * own position-math columns stay blank until the engine preserves Position Qty /
 * Avg Cost on refresh (a separate, backend change). No other change from v1.5.3.
 *
 * Aligned with — current 122 investment reliability contract
 *   (My_Portfolio = canonical 115 + 7 engine-native decision columns;
 *    Top_10_Investments = decision cockpit, 16_Decision_Top10.gs)
 * - 00_Config.gs        v1.12.6
 * - 01_Menu.gs          v2.10.0
 * - 03_Schema.gs        v2.7.0
 * - 04_Format.gs        v2.14.0
 * - 05_Refresh.gs       v1.12.0
 * - 06_Market_Leaders.gs v1.6.0
 * - 07_Global_Markets.gs v1.7.1
 * - 08_Commodities_FX.gs v1.3.0
 * - 09_Mutual_Funds.gs  v1.4.0
 * - 11_SpecialPages.gs  v1.8.4
 * - 14_Portfolio_PnL.gs v2.1.1
 * - 99_Data_Audit.gs    v1.4.0
 * - 99_Diag.gs          v2.4.0
 *
 * v1.5.3 MAIN CHANGES  (COST-BASIS UNIVERSE UNION — single fix; ALL v1.5.2
 *                       logic carried verbatim, zero regressions)
 * ---------------------------------------------------------------
 * SYMPTOM: a holding ADDED to _Portfolio_CostBasis did not appear on
 * My_Portfolio (and therefore not on Portfolio_Decision). ROOT CAUSE: the
 * cost-basis seeder (14_Portfolio_PnL seedPortfolioInputsFromCostBasis_)
 * JOINS cost basis onto rows that ALREADY EXIST on My_Portfolio — it walks
 * the existing My_Portfolio symbols and fills each from cost basis. It never
 * CREATES a row. And getMyPortfolioSymbols_ only consulted _Portfolio_CostBasis
 * when the page working list was EMPTY (the v1.5.1 wipe-guard fallback). So a
 * brand-new cost-basis symbol that was not already in the working list got no
 * My_Portfolio row, nothing to seed, and never showed downstream.
 *
 * FIX: getMyPortfolioSymbols_ now returns the UNION of the page working list
 * and the _Portfolio_CostBasis symbols (de-duplicated, normalized). Adding a
 * holding to _Portfolio_CostBasis now gives it a My_Portfolio row on the next
 * refresh — which then seeds (Position Qty / Avg Cost / Buy Date / Target
 * Weight %), the engine computes position math + weights, and the decision
 * layer renders it. The v1.5.1 wipe-guard still holds: when BOTH the working
 * list and cost basis are empty, the refresh entry points abort rather than
 * fall back to default symbols. No other change from v1.5.2.
 *
 * NOTE (operational): the seed runs as a PRE-REFRESH step, so editing
 * _Portfolio_CostBasis only takes effect on the NEXT My_Portfolio refresh.
 * Edit cost basis, then refresh My_Portfolio, then refresh Portfolio_Decision.
 *
 * v1.5.2 MAIN CHANGES  (COST-KEY FIX — single fix; ALL v1.5.1 logic carried
 *                       verbatim, zero regressions)
 * ---------------------------------------------------------------
 * The decision-layer holdings POST sent the average-cost value under the
 * JSON key 'Avg Cost'. But the backend action engine
 * (core/analysis/portfolio_actions v1.0.0) is proven against the key
 * 'Buy Price' — that is the key its own acceptance test feeds and the key
 * routes/advanced_analysis.py v4.8.0 documents ("Quantity and Buy Price").
 * opportunity_builder (to which portfolio_actions delegates row
 * normalization) carries NO cost/quantity alias at all, so the cost parse is
 * portfolio_actions' own code and the only confirmed key there is 'Buy Price'.
 * If that parser does not also accept 'Avg Cost', every holding POSTed cost
 * basis under a key the engine ignores -> P&L returns None -> the exact bug #2
 * (cost basis never reaching the compute layer) reappears, this time as a
 * field-name mismatch.
 *
 * FIX: PD_HOLDING_FIELDS now emits the average-cost value under BOTH
 * 'Buy Price' (the proven/documented key) AND 'Avg Cost' (back-compat),
 * reading the identical My_Portfolio column for each. Both keys carry the
 * same value, so whichever alias the parser reads, cost basis arrives;
 * sending an extra recognized-or-ignored key is harmless to a tolerant
 * builder. No other change from v1.5.1.
 *
 * v1.5.1 MAIN CHANGES  (WIPE-GUARD — incident fix, 2026-06-12)
 * ---------------------------------------------------------------
 * INCIDENT: a My_Portfolio refresh ran while the page's working symbol
 * list was empty. getMyPortfolioSymbols_() fell back to
 * getDefaultSymbols('My_Portfolio') — the generic 3-symbol sample list —
 * and the symbol-keyed writer rewrote the page to that universe,
 * REPLACING all real holdings (inputs survived only because
 * _Portfolio_CostBasis is the durable home).
 *
 * FIX (defaults are NEVER a sane fallback for a portfolio page):
 * 1. getMyPortfolioSymbols_(): empty working list now falls back to the
 *    symbols in _Portfolio_CostBasis (via 14_Portfolio_PnL
 *    readCostBasisMap_ when present, else a local direct read). The
 *    getDefaultSymbols fallback is REMOVED for this page. Had this been
 *    in place, today's refresh would have self-healed from cost basis.
 * 2. refreshMyPortfolio_ / refreshMyPortfolioInBatches_: if the resolved
 *    symbol list is STILL empty (working list and cost basis both empty),
 *    the refresh ABORTS with { ok:false, aborted:true,
 *    reason:'no_symbols' } and a clear operator message — it never fires
 *    a request that would let any fallback universe replace holdings.
 * 3. Everything else carried verbatim from v1.5.0 (+2 helper functions:
 *    _myPortfolioCostBasisSymbols_, _myPortfolioAbortNoSymbols_).
 *
 * v1.5.0 MAIN CHANGES  (Plan v5.0 P6 / milestone M2 — Portfolio_Decision page)
 * --------------------------------------------------------------------------
 * 1. NEW DECISION LAYER (additive — ALL v1.4.0 logic carried verbatim, zero
 *    regressions). The 122-column My_Portfolio page is unchanged and remains
 *    the engine-enriched holdings source. v1.5.0 adds a separate
 *    'Portfolio_Decision' sheet rendered from the backend action engine:
 *      core/analysis/portfolio_actions.py v1.0.0 (P5) via
 *      POST /sheet-rows/portfolio-actions (routes/advanced_analysis.py
 *      v4.8.0 [PF-ADD], P5b).
 *    Per holding: one L3 action (ADD/HOLD/TRIM/EXIT/BLOCK), SAR sizing under
 *    the L7 funding identity (Target-Cash floor, mode-aware TRIM/EXIT
 *    proceeds, funds_from labels), L8 one-sentence advisor notes, sector
 *    summary, and alerts. This file ONLY collects inputs and renders the
 *    payload — no action/gate/sizing logic is re-implemented in GAS (§8).
 *
 * 2. HOLDINGS POOL (§5 GAS-pool pattern, same as 16_Decision_Top10.gs):
 *    refreshPortfolioDecision() reads the refreshed My_Portfolio rows through
 *    the existing v1.4.0 header machinery (getHeaderMapMyPortfolio_ /
 *    myPortfolioResolveHeaderColumn_) and POSTs ~20 fields per holding.
 *    'Position Qty' is sent as 'Quantity' and the average cost is sent as
 *    BOTH 'Buy Price' (proven key, v1.5.2) and 'Avg Cost' — the keys the
 *    action engine's cost-basis parser consumes — so the Option-A seeded
 *    inputs now reach the decision compute layer end-to-end.
 *    Analyst-target headers only are mapped to 'Target Price' (engine
 *    'Forecast Price 12M' is intentionally excluded — L5 double-count guard).
 *
 * 3. CONTROL PANEL: the 7 §3.2 'PF: *' labels verbatim (defaults re-seeded
 *    from _Lists_Config TFB_PANEL_DEFAULTS; operator-blue per L14), sent as
 *    criteria keys verbatim — the route's _PF_CONTROLS_ALIASES maps them.
 *    FX rates are read from TFB_FX_LOOKUP (backend never reads the sheet).
 *
 * 4. NEW ENTRY POINTS (wire into 01_Menu.gs; no onOpen here):
 *      buildPortfolioDecisionLayout()  one-time/repair: panel + formats
 *      refreshPortfolioDecision()      collect -> POST -> render
 *      pdSelfTest()                    config/named-range/mapping dry checks
 *    Honest empties per L13; §3.4 design tokens with hardcoded fallbacks;
 *    breakApart() guards before every merged-zone rewrite.
 *
 * v1.4.0 MAIN CHANGES  (engine-native portfolio decision layer — "Option A")
 * --------------------------------------------------------------------------
 * 1. EXPECTED WIDTH 115 -> 122. My_Portfolio now carries the 7 engine-native
 *    decision columns appended after Block Reason (cols 116-122), matching
 *    data_engine_v2 v5.80.0 + schema_registry v2.14.0 + 00_Config.gs v1.12.3:
 *      Buy Date | Target Weight % | Actual Weight % | Weight Gap |
 *      Rebalance Action | Investor Decision | User Notes
 *    Validation / diagnostics / sort / layout inference are all 122-aware,
 *    so a correctly-refreshed sheet no longer trips a width WARN.
 *
 * 2. OWNERSHIP: the ENGINE owns the decision math. Actual Weight %, Weight Gap,
 *    Rebalance Action and Investor Decision are computed cross-sectionally by
 *    data_engine_v2 _compute_portfolio_fields() on every refresh. Buy Date,
 *    Target Weight % and User Notes are MANUAL inputs: the engine reads them
 *    from the sheet (RowsReader external-rows path) and preserves them (they
 *    are NOT in the live-overwrite whitelist), so they survive every refresh.
 *
 * 3. INPUT FEED (Option A): the engine computes weights only when the manual
 *    inputs (Position Qty, Avg Cost, Buy Date, Target Weight %) are present on
 *    the sheet. refreshMyPortfolio_/refreshMyPortfolioInBatches_ now call a
 *    guarded pre-refresh seeder (_myPortfolioSeedManualInputs_) that, when
 *    14_Portfolio_PnL.gs v2.0.0+ is present, copies those inputs from the
 *    _Portfolio_CostBasis sheet onto My_Portfolio BEFORE the engine reads it.
 *    The hook is a no-op (safe) when the feed module is absent, and can be
 *    disabled per-call with options.seedInputs === false.
 *
 * 4. New awareness surface: MY_PORTFOLIO_PORTFOLIO_DECISION_HEADERS_ (the 7
 *    new headers), decision-coverage helpers, and a richer layout label
 *    ('full_122_investment_schema'). All v1.3.0 logic is carried verbatim.
 *
 * v1.3.0 MAIN CHANGES
 * -------------------
 * 1. Updates My_Portfolio from the old v1.2.2 dependency chain
 *    to the current 115-column main-page contract.
 *
 * 2. Adds explicit investment-gate header tracking:
 *      Forecast Source
 *      Data Quality Score
 *      Forecast Reliability Score
 *      Provider/Engine Conflict
 *      Conflict Type
 *      Final Decision Basis
 *      Investability Status
 *      Final Action
 *      Block Reason
 *
 * 3. Uses the current 05_Refresh validators when available:
 *      validateExpectedRefreshWidth_
 *      validatePostRefreshIntegrity_
 *    This keeps My_Portfolio aligned with the same validation layer
 *    used by Market_Leaders, Global_Markets, Commodities_FX, and
 *    Top_10_Investments.
 *
 * 4. Removes the duplicate shared dispatcher manageSymbolsForPage_().
 *    That dispatcher must live only in the central menu/special-pages
 *    layer to avoid Apps Script global-namespace overwrites. This file
 *    keeps only manageMyPortfolioSymbols_().
 *
 * 5. Keeps the useful existing logic:
 *      - build / refresh / batch refresh wrappers
 *      - selected-row refresh wrapper
 *      - portfolio-specific preserve and sort logic
 *      - symbol normalization
 *      - header diagnostics
 *
 * 6. Adds debugMyPortfolioInvestmentGate_(symbol) for a direct
 *    row-level check of the current investment-gate fields.
 */

'use strict';

/* =========================================================
 * Top-level identifiers
 * ========================================================= */

var MY_PORTFOLIO_PAGE_ = SHEET_NAMES.MY_PORTFOLIO;
var MY_PORTFOLIO_EXPECTED_COLUMNS_ = 122;

/* v1.5.6 HIDE-UNHELD: when true, the My_Portfolio symbol universe is restricted
 * to symbols that actually have a position in _Portfolio_CostBasis. This drops
 * "target-only" / watch symbols (present in the page working list but never
 * bought, hence no quantity) so they no longer appear as BLOCK ("missing
 * quantity") rows on My_Portfolio or My_Portfolio_Decision. The filter is
 * applied ONLY when cost-basis has at least one symbol, so a transient
 * cost-basis read failure can never wipe the page (the v1.5.1 WIPE-GUARD intent
 * is preserved). Set to false to restore the v1.5.3 working+cost-basis union. */
var MY_PORTFOLIO_HIDE_UNHELD_ = true;

var MY_PORTFOLIO_REQUIRED_BUSINESS_HEADERS_ = Object.freeze([
  'Symbol',
  'Name',
  'Asset Class',
  'Exchange',
  'Currency',
  'Country',
  'Current Price',
  'Position Qty',
  'Avg Cost',
  'Position Cost',
  'Position Value',
  'Unrealized P/L',
  'Unrealized P/L %',
  'Percent Change',
  'Opportunity Score',
  'Overall Score',
  'Rank (Overall)',
  'Recommendation',
  'Recommendation Detail',
  'Recommendation Reason',
  'Data Provider',
  'Last Updated (Riyadh)',
  'Warnings'
]);

var MY_PORTFOLIO_INVESTMENT_GATE_HEADERS_ = Object.freeze([
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

/*
 * v1.4.0: the 7 engine-native portfolio decision columns (cols 116-122),
 * appended after the canonical 115. Mirrors data_engine_v2 v5.80.0
 * MY_PORTFOLIO_EXTRA_HEADERS and 00_Config.gs v1.12.3. Header order is the
 * contract — do not reorder.
 */
var MY_PORTFOLIO_PORTFOLIO_DECISION_HEADERS_ = Object.freeze([
  'Buy Date',
  'Target Weight %',
  'Actual Weight %',
  'Weight Gap',
  'Rebalance Action',
  'Investor Decision',
  'User Notes'
]);

/*
 * v1.4.0: the manual-input columns the engine reads from the sheet and never
 * overwrites (preserved across refresh). These are what the Option-A cost-basis
 * feed seeds so the engine can compute the weight columns. Two of them live in
 * the canonical position block (Position Qty / Avg Cost, cols 75-76); the rest
 * are in the new decision block.
 */
var MY_PORTFOLIO_MANUAL_INPUT_HEADERS_ = Object.freeze([
  'Position Qty',
  'Avg Cost',
  'Buy Date',
  'Target Weight %',
  'User Notes'
]);

/* =========================================================
 * Public page module entry points
 * ========================================================= */

function buildMyPortfolio_() {
  return buildPage_(MY_PORTFOLIO_PAGE_);
}

function refreshMyPortfolio_(options) {
  options = options || {};

  _myPortfolioSeedManualInputs_(options);

  var symbols = normalizeMyPortfolioSymbols_(
    options.symbols && options.symbols.length
      ? options.symbols
      : getMyPortfolioSymbols_()
  );

  /* v1.5.1 WIPE-GUARD: never fire a refresh without holdings symbols. */
  if (!symbols.length) {
    return _myPortfolioAbortNoSymbols_();
  }

  var payloadOverrides = {
    symbols: symbols,
    tickers: symbols,
    direct_symbols: symbols
  };

  if (options.limit) {
    payloadOverrides.limit = options.limit;
  }

  if (options.mode) {
    payloadOverrides.mode = options.mode;
  }

  var refreshOpts = {
    payloadOverrides: payloadOverrides,
    applyFormat: options.applyFormat !== false,
    applySort: options.applySort !== false,
    allowAutoBatch: options.allowAutoBatch !== false
  };

  if (options.forceHeaders === true) {
    refreshOpts.forceHeaders = true;
  }

  if (options.forceHeaders === false) {
    refreshOpts.forceHeaders = false;
  }

  if (options.legacyPath === true) {
    refreshOpts.legacyPath = true;
  }

  var result = refreshPage_(MY_PORTFOLIO_PAGE_, refreshOpts);
  result = appendMyPortfolioFallbackWarning_(result);

  if (!result || result.paused || result.partial) {
    return result;
  }

  result = _myPortfolioRunOptionalPostProcessors_(result, options);
  return result;
}

function refreshMyPortfolioSelectedRow_(options) {
  options = options || {};
  var result = refreshSelectedRow_(MY_PORTFOLIO_PAGE_, options);
  result = _myPortfolioRunOptionalPostProcessors_(result, {
    skipValidation: false,
    reseedAfterRefresh: options.reseedAfterRefresh
  });
  return result;
}

function refreshMyPortfolioInBatches_(options) {
  options = options || {};

  _myPortfolioSeedManualInputs_(options);

  var symbols = normalizeMyPortfolioSymbols_(
    options.symbols && options.symbols.length
      ? options.symbols
      : getMyPortfolioSymbols_()
  );

  /* v1.5.1 WIPE-GUARD: never fire a batch refresh without holdings symbols. */
  if (!symbols.length) {
    return _myPortfolioAbortNoSymbols_();
  }

  var payloadOverrides = {
    symbols: symbols,
    tickers: symbols,
    direct_symbols: symbols,
    mode: options.mode || getDefaultMode()
  };

  if (options.limit) {
    payloadOverrides.limit = options.limit;
  }

  var batchOpts = {
    batchSize: options.batchSize || REFRESH_BATCH_SIZE_DEFAULT_,
    payloadOverrides: payloadOverrides,
    applyFormat: options.applyFormat !== false,
    applySort: options.applySort !== false
  };

  if (options.forceHeaders === true) {
    batchOpts.forceHeaders = true;
  }

  if (options.forceHeaders === false) {
    batchOpts.forceHeaders = false;
  }

  if (options.legacyPath === true) {
    batchOpts.legacyPath = true;
  }

  var result = refreshPageInBatches_(MY_PORTFOLIO_PAGE_, batchOpts);
  result = appendMyPortfolioFallbackWarning_(result);

  if (!result || result.paused || result.partial) {
    return result;
  }

  result = _myPortfolioRunOptionalPostProcessors_(result, options);
  return result;
}

function validateMyPortfolio_() {
  var page = getPageConfig(MY_PORTFOLIO_PAGE_);
  var sheet = ensureSheetExists_(MY_PORTFOLIO_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var rowCount = Math.max(0, sheet.getLastRow() - 1);
  var headerMap = getHeaderMapMyPortfolio_(headers);

  var expectedWidth = Number(page.expectedColumnCount || MY_PORTFOLIO_EXPECTED_COLUMNS_);
  var preserveColumns = getMyPortfolioPreserveColumns_() || [];
  var missingBusinessHeaders = getMissingMyPortfolioBusinessHeaders_(headerMap);
  var missingPreserveColumns = preserveColumns.filter(function (h) {
    return !myPortfolioResolveHeaderColumn_(headerMap, h);
  });
  var missingGateHeaders = getMissingMyPortfolioInvestmentGateHeaders_(headerMap);
  var missingDecisionHeaders = getMissingMyPortfolioDecisionHeaders_(headerMap);
  var inferredLayout = inferMyPortfolioLayout_(headerMap);

  var strictResult = null;
  var mappingResult = null;
  var widthValidation = null;
  var postRefreshValidation = null;

  try {
    strictResult = validatePage_(MY_PORTFOLIO_PAGE_);
  } catch (e) {
    strictResult = {
      ok: false,
      error: e && e.message ? e.message : String(e),
      missingHeaders: [],
      extraHeaders: [],
      missingRequiredKeys: [],
      unmappedHeaders: []
    };
  }

  if (strictResult && strictResult.mappingModeUsed) {
    mappingResult = {
      ok: (strictResult.unmappedHeaders || []).length === 0,
      pageName: strictResult.pageName || MY_PORTFOLIO_PAGE_,
      unmapped: (strictResult.unmappedHeaders || []).slice(),
      backendNew: [],
      counts: {
        total: strictResult.actualHeadersCount || headers.length,
        mapped: Math.max(0, (strictResult.actualHeadersCount || headers.length) -
          (strictResult.unmappedHeaders || []).length),
        unmapped: (strictResult.unmappedHeaders || []).length,
        userOwned: 0,
        backendNew: 0
      }
    };
  }

  if (typeof validateExpectedRefreshWidth_ === 'function') {
    try {
      widthValidation = validateExpectedRefreshWidth_(MY_PORTFOLIO_PAGE_, sheet);
    } catch (e1) {
      widthValidation = {
        ok: false,
        warnings: ['Width validation failed: ' + (e1 && e1.message ? e1.message : String(e1))]
      };
    }
  } else {
    widthValidation = {
      ok: expectedWidth <= 0 || headers.length === expectedWidth,
      expected: expectedWidth,
      actual: headers.length,
      warnings: headers.length === expectedWidth
        ? []
        : ['Width mismatch: expected ' + expectedWidth + ', actual ' + headers.length]
    };
  }

  if (typeof validatePostRefreshIntegrity_ === 'function') {
    try {
      postRefreshValidation = validatePostRefreshIntegrity_(MY_PORTFOLIO_PAGE_, sheet, {
        expectedSymbolCount: getMyPortfolioSymbols_().length
      });
    } catch (e2) {
      postRefreshValidation = {
        ok: false,
        warnings: ['Post-refresh validation failed: ' + (e2 && e2.message ? e2.message : String(e2))]
      };
    }
  } else {
    postRefreshValidation = validateMyPortfolioGateLocally_(sheet, headers);
  }

  var strictOk = !strictResult || strictResult.ok !== false;
  var mappingOk = !mappingResult || mappingResult.ok !== false;
  var widthOk = !widthValidation || widthValidation.ok !== false;
  var postOk = !postRefreshValidation || postRefreshValidation.ok !== false;

  var ok = strictOk && mappingOk && widthOk && postOk &&
    headers.length === expectedWidth &&
    missingBusinessHeaders.length === 0 &&
    missingPreserveColumns.length === 0 &&
    missingGateHeaders.length === 0 &&
    missingDecisionHeaders.length === 0;

  var warningsList = []
    .concat(missingBusinessHeaders.map(function (h) { return 'Missing business header: ' + h; }))
    .concat(missingPreserveColumns.map(function (h) { return 'Missing preserve header: ' + h; }))
    .concat(missingGateHeaders.map(function (h) { return 'Missing gate header: ' + h; }))
    .concat(missingDecisionHeaders.map(function (h) { return 'Missing decision header: ' + h; }))
    .concat(strictResult && strictResult.missingHeaders ? strictResult.missingHeaders : [])
    .concat(strictResult && strictResult.extraHeaders ? strictResult.extraHeaders : [])
    .concat(strictResult && strictResult.missingRequiredKeys ? strictResult.missingRequiredKeys : [])
    .concat(strictResult && strictResult.unmappedHeaders ? strictResult.unmappedHeaders : [])
    .concat(widthValidation && widthValidation.warnings ? widthValidation.warnings : [])
    .concat(postRefreshValidation && postRefreshValidation.warnings ? postRefreshValidation.warnings : []);

  warningsList = myPortfolioUniqueStrings_(warningsList);

  var enrichedResult = {
    ok: ok,
    pageName: MY_PORTFOLIO_PAGE_,
    expectedColumnCount: expectedWidth,
    actualHeadersCount: headers.length,
    rowCount: rowCount,
    inferredLayout: inferredLayout,
    missingBusinessHeaders: missingBusinessHeaders,
    missingPreserveColumns: missingPreserveColumns,
    preserveColumnCount: preserveColumns.length,
    missingGateHeaders: missingGateHeaders,
    missingDecisionHeaders: missingDecisionHeaders,
    missingHeaders: strictResult.missingHeaders || [],
    extraHeaders: strictResult.extraHeaders || [],
    orderMatches: !!strictResult.orderMatches,
    missingRequiredKeys: strictResult.missingRequiredKeys || [],
    strictResult: strictResult,
    mappingResult: mappingResult,
    widthValidation: widthValidation,
    postRefreshValidation: postRefreshValidation,
    gateCoverage: getMyPortfolioInvestmentGateCoverage_(headerMap),
    decisionCoverage: getMyPortfolioDecisionCoverage_(headerMap),
    strictApprovedRows: getMyPortfolioStrictApprovedRows_(),
    validationMode: mappingResult ? 'header-map' : 'strict-positional'
  };

  logRun_(
    enrichedResult.ok ? 'INFO' : 'WARN',
    'validateMyPortfolio',
    MY_PORTFOLIO_PAGE_,
    enrichedResult.ok
      ? 'My_Portfolio validation passed as full 122-column investment page.'
      : 'My_Portfolio validation found issues.',
    {
      status: enrichedResult.ok ? 'PASS' : 'WARN',
      details: {
        ok: enrichedResult.ok,
        validationMode: enrichedResult.validationMode,
        expectedColumnCount: expectedWidth,
        actualHeadersCount: enrichedResult.actualHeadersCount,
        rowCount: enrichedResult.rowCount,
        inferredLayout: inferredLayout,
        missingBusinessHeaders: missingBusinessHeaders,
        missingPreserveColumns: missingPreserveColumns,
        missingGateHeaders: missingGateHeaders,
        missingDecisionHeaders: missingDecisionHeaders,
        widthValidation: widthValidation,
        postRefreshValidation: postRefreshValidation
      }
    }
  );

  writePageStatus_(MY_PORTFOLIO_PAGE_, {
    status: enrichedResult.ok ? 'VALID' : 'WARN',
    message: enrichedResult.ok
      ? 'My_Portfolio validation passed - full 122-column investment page'
      : 'My_Portfolio validation issues found',
    endpoint: '',
    httpCode: '',
    rows: rowCount,
    columns: headers.length,
    durationMs: '',
    warnings: warningsList
  });

  return enrichedResult;
}

function sortMyPortfolio_() {
  return withScriptLock_(function () {
    var page = getPageConfig(MY_PORTFOLIO_PAGE_);
    var sheet = ensureSheetExists_(MY_PORTFOLIO_PAGE_);
    var headers = readCurrentSheetHeaders_(sheet);
    var lastRow = sheet.getLastRow();

    if (lastRow <= 2 || !headers.length) {
      return {
        ok: true,
        pageName: MY_PORTFOLIO_PAGE_,
        skipped: true,
        message: 'No data rows to sort.'
      };
    }

    var headerMap = getHeaderMapMyPortfolio_(headers);
    var sortPlan = resolveMyPortfolioSortPlan_(page, headerMap);

    if (!sortPlan || !sortPlan.column) {
      return {
        ok: true,
        pageName: MY_PORTFOLIO_PAGE_,
        skipped: true,
        message: 'No usable My_Portfolio sort field found.'
      };
    }

    var expectedWidth = Number(page.expectedColumnCount || MY_PORTFOLIO_EXPECTED_COLUMNS_);
    var sortRangeCols = Math.min(
      Math.max(1, headers.length),
      Math.max(1, sheet.getLastColumn())
    );

    if (expectedWidth > 0) {
      sortRangeCols = Math.min(sortRangeCols, expectedWidth);
    }

    if (sortPlan.column > sortRangeCols) {
      sortRangeCols = Math.min(sortPlan.column, sheet.getLastColumn());
    }

    sheet.getRange(2, 1, lastRow - 1, sortRangeCols).sort({
      column: sortPlan.column,
      ascending: sortPlan.ascending
    });

    logRun_(
      'INFO',
      'sortMyPortfolio',
      MY_PORTFOLIO_PAGE_,
      'My_Portfolio sorted successfully.',
      {
        status: 'OK',
        sortField: sortPlan.field,
        ascending: sortPlan.ascending,
        sortRangeCols: sortRangeCols
      }
    );

    writePageStatus_(MY_PORTFOLIO_PAGE_, {
      status: 'SORT_OK',
      message: 'Sorted by ' + sortPlan.field,
      endpoint: '',
      httpCode: '',
      rows: lastRow - 1,
      columns: headers.length,
      durationMs: '',
      warnings: []
    });

    return {
      ok: true,
      pageName: MY_PORTFOLIO_PAGE_,
      sortedBy: sortPlan.field,
      ascending: sortPlan.ascending,
      sortRangeCols: sortRangeCols
    };
  });
}

function manageMyPortfolioSymbols_() {
  var symbols = getMyPortfolioSymbols_();
  var ui = SpreadsheetApp.getUi();

  ui.alert(
    'My Portfolio Symbols',
    symbols.length ? symbols.join(', ') : 'No symbols configured.',
    ui.ButtonSet.OK
  );

  return {
    ok: true,
    pageName: MY_PORTFOLIO_PAGE_,
    symbols: symbols
  };
}

/* =========================================================
 * Optional post-processors
 * ========================================================= */

/*
 * v1.4.0: Option-A input feed hook. The engine computes the portfolio weight
 * columns (Actual Weight %, Weight Gap, Rebalance Action, Investor Decision)
 * only when the manual inputs (Position Qty, Avg Cost, Buy Date, Target
 * Weight %) are already present on the sheet when the engine reads it. This
 * guarded hook seeds those inputs from the _Portfolio_CostBasis sheet via
 * 14_Portfolio_PnL.gs v2.0.0+ (seedPortfolioInputsFromCostBasis_) BEFORE the
 * refresh request fires. It is a safe no-op when that module is absent, and
 * can be disabled per-call with options.seedInputs === false.
 */
function _myPortfolioSeedManualInputs_(options) {
  options = options || {};

  if (options.seedInputs === false) {
    return { ok: true, skipped: true, reason: 'seedInputs disabled by caller' };
  }

  /* v1.5.9 LEDGER-V2: when 21_Portfolio_Ledger.gs is installed, its ACTIVE-
   * lot feeder syncLedgerToPortfolio() OWNS the My_Portfolio input columns
   * (qty aggregated per symbol, weighted avg cost, unheld rows pruned) and is
   * v2-layout native. Prefer it. The 14_Portfolio_PnL seeder below reads the
   * v1 layout (header row 1 / 'Quantity') and silently seeds nothing against
   * a migrated ledger — it remains ONLY as the fallback for 14-only installs.
   * On feeder failure we WARN and stop: falling back to the v1 seeder here
   * would reintroduce the dual-writer this revision removes. */
  var ledgerSeed = _myPortfolioLedgerSeed_('seedMyPortfolioInputs',
      'Seeded My_Portfolio inputs via 21_Portfolio_Ledger syncLedgerToPortfolio (ACTIVE lots) before refresh.');
  if (ledgerSeed) { return ledgerSeed; }

  if (typeof seedPortfolioInputsFromCostBasis_ !== 'function') {
    return {
      ok: true,
      skipped: true,
      reason: 'no input feeder present (21_Portfolio_Ledger syncLedgerToPortfolio or 14_Portfolio_PnL seedPortfolioInputsFromCostBasis_)'
    };
  }

  try {
    var seedResult = seedPortfolioInputsFromCostBasis_() || { ok: true };

    logRun_(
      'INFO',
      'seedMyPortfolioInputs',
      MY_PORTFOLIO_PAGE_,
      'Seeded My_Portfolio manual inputs from _Portfolio_CostBasis before refresh.',
      {
        status: 'OK',
        details: seedResult
      }
    );

    return seedResult;
  } catch (e) {
    logRun_(
      'WARN',
      'seedMyPortfolioInputs',
      MY_PORTFOLIO_PAGE_,
      'Manual-input seeding failed (non-fatal): ' + (e && e.message ? e.message : String(e)),
      {
        status: 'WARN'
      }
    );

    return { ok: false, error: e && e.message ? e.message : String(e) };
  }
}

/*
 * v1.5.5: POST-refresh display re-seed. The engine reads the manual inputs
 * (Position Qty / Avg Cost / Buy Date / Target Weight %) seeded BEFORE refresh,
 * computes the position math, but does NOT preserve those input cells on
 * write-back — so they are blank on the page after the refresh returns. This
 * hook re-runs 14_Portfolio_PnL.gs's seedPortfolioInputsFromCostBasis_ AFTER
 * the refresh to restore them for DISPLAY.
 *
 * SAFE BY CONSTRUCTION: it calls the SAME seeder the pre-refresh hook uses, so
 * it writes ONLY the manual-input columns (sparse: only where _Portfolio_Cost-
 * Basis supplies a value), never an engine-output column, never changes width,
 * and re-joins on Symbol so it is sort-safe. Idempotent. Disable per-call with
 * options.reseedAfterRefresh === false. Safe no-op when 14_Portfolio_PnL.gs is
 * absent (typeof-guarded) or when the seeder throws (caught, logged WARN).
 *
 * NOTE: this restores the INPUT columns only. Engine-COMPUTED columns (Position
 * Value / Unrealized P&L) are owned by the engine; if those are also blank, the
 * engine is not persisting the computed position math and the complete fix is a
 * backend preserve-whitelist change (see the v1.5.5 header BOUNDARY note). This
 * file never writes engine-output columns (retired in 14_Portfolio_PnL v2.0.0).
 */
function _myPortfolioReseedInputsAfterRefresh_(options) {
  options = options || {};

  if (options.reseedAfterRefresh === false) {
    return { ok: true, skipped: true, reason: 'reseedAfterRefresh disabled by caller' };
  }

  /* v1.5.9 LEDGER-V2: same preference as the pre-refresh hook — 21's ACTIVE-
   * lot feeder restores Position Qty / Avg Cost for display (and prunes
   * unheld rows) with v2-native reads; 14's v1-layout seeder is fallback
   * only. On feeder failure: WARN, no v1 fallback (dual-writer guard). */
  var ledgerReseed = _myPortfolioLedgerSeed_('reseedMyPortfolioInputs',
      'Re-seeded My_Portfolio inputs via 21_Portfolio_Ledger syncLedgerToPortfolio after refresh (display restore).');
  if (ledgerReseed) { return ledgerReseed; }

  if (typeof seedPortfolioInputsFromCostBasis_ !== 'function') {
    return {
      ok: true,
      skipped: true,
      reason: 'no input feeder present (21_Portfolio_Ledger syncLedgerToPortfolio or 14_Portfolio_PnL seedPortfolioInputsFromCostBasis_)'
    };
  }

  try {
    var seedResult = seedPortfolioInputsFromCostBasis_() || { ok: true };

    logRun_(
      'INFO',
      'reseedMyPortfolioInputs',
      MY_PORTFOLIO_PAGE_,
      'Re-seeded My_Portfolio manual inputs from _Portfolio_CostBasis after refresh (display restore).',
      {
        status: 'OK',
        details: seedResult
      }
    );

    return seedResult;
  } catch (e) {
    logRun_(
      'WARN',
      'reseedMyPortfolioInputs',
      MY_PORTFOLIO_PAGE_,
      'Post-refresh re-seed failed (non-fatal): ' + (e && e.message ? e.message : String(e)),
      {
        status: 'WARN'
      }
    );

    return { ok: false, error: e && e.message ? e.message : String(e) };
  }
}

function _myPortfolioRunOptionalPostProcessors_(result, options) {
  options = options || {};
  result = result || {};

  /* v1.5.5: restore the manual-input columns (Position Qty / Avg Cost / Buy
   * Date / Target Weight %) that the engine blanks on write-back, so the
   * My_Portfolio page DISPLAYS them. Runs BEFORE validation so the page is
   * validated in its final displayed state. Writes only manual-input columns
   * (never engine-output columns); safe no-op when 14_Portfolio_PnL.gs absent. */
  result.myPortfolioReseed = _myPortfolioReseedInputsAfterRefresh_(options);

  if (options.skipValidation !== true) {
    try {
      result.myPortfolioValidation = validateMyPortfolio_();
    } catch (e) {
      result.warnings = (result.warnings || []).concat([
        'My_Portfolio validation failed: ' + (e && e.message ? e.message : String(e))
      ]);
    }
  }

  return result;
}

function appendMyPortfolioFallbackWarning_(result) {
  if (typeof appendGenericFallbackWarning_ === 'function' && result) {
    return appendGenericFallbackWarning_(MY_PORTFOLIO_PAGE_, result) || result;
  }

  if (!result || !result.endpointUsed) return result;

  var page = getPageConfig(MY_PORTFOLIO_PAGE_);
  var primaryEndpoint = page.primaryEndpoint;

  if (result.endpointUsed === primaryEndpoint) return result;

  var sheet = ensureSheetExists_(MY_PORTFOLIO_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var fallbackWarnings = myPortfolioUniqueStrings_(
    (result.warnings || []).concat([
      'Fallback endpoint used: ' + result.endpointUsed
    ])
  );

  writePageStatus_(MY_PORTFOLIO_PAGE_, {
    status: result.partial ? 'PARTIAL' : 'SUCCESS',
    message: 'Fallback endpoint used for My_Portfolio: ' + result.endpointUsed,
    endpoint: result.endpointUsed,
    httpCode: '',
    rows: result.rowCount || '',
    columns: headers.length || '',
    durationMs: '',
    warnings: fallbackWarnings
  });

  logRun_(
    'WARN',
    'refreshMyPortfolioFallback',
    MY_PORTFOLIO_PAGE_,
    'Fallback endpoint was used for My_Portfolio: ' + result.endpointUsed,
    {
      status: 'WARN',
      endpoint: result.endpointUsed,
      warnings: fallbackWarnings,
      primaryEndpoint: primaryEndpoint
    }
  );

  result.warnings = fallbackWarnings;
  result.usedFallback = true;
  result.fallbackEndpoint = result.endpointUsed;
  result.primaryEndpoint = primaryEndpoint;

  return result;
}

/* =========================================================
 * Header diagnostic / non-destructive merge entry points
 * ========================================================= */

function mergeMyPortfolioHeaders_(options) {
  if (typeof mergeBackendHeadersIntoSheet_ !== 'function') {
    throw new Error(
      'mergeMyPortfolioHeaders_ requires 03_Schema.gs v2.5.0 or later. '
      + 'Update 03_Schema.gs first, then re-run this command.'
    );
  }

  options = options || {};

  if (options.force !== false) {
    options.force = true;
  }

  return mergeBackendHeadersIntoSheet_(MY_PORTFOLIO_PAGE_, options);
}

function proposeMyPortfolioHeaderUpgrades_() {
  if (typeof proposeHeaderUpgrades_ !== 'function') {
    throw new Error(
      'proposeMyPortfolioHeaderUpgrades_ requires 03_Schema.gs v2.5.0 or later.'
    );
  }

  return proposeHeaderUpgrades_(MY_PORTFOLIO_PAGE_);
}

function diagnoseMyPortfolio_() {
  var page = getPageConfig(MY_PORTFOLIO_PAGE_);
  var sheet = ensureSheetExists_(MY_PORTFOLIO_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var headerMap = getHeaderMapMyPortfolio_(headers);

  var expectedWidth = Number(page.expectedColumnCount || MY_PORTFOLIO_EXPECTED_COLUMNS_);
  var preserveColumns = getMyPortfolioPreserveColumns_() || [];
  var missingBusinessHeaders = getMissingMyPortfolioBusinessHeaders_(headerMap);
  var missingPreserveColumns = preserveColumns.filter(function (h) {
    return !myPortfolioResolveHeaderColumn_(headerMap, h);
  });
  var missingGateHeaders = getMissingMyPortfolioInvestmentGateHeaders_(headerMap);
  var missingDecisionHeaders = getMissingMyPortfolioDecisionHeaders_(headerMap);
  var symbols = getMyPortfolioSymbols_();
  var sortPlan = resolveMyPortfolioSortPlan_(page, headerMap);

  var coverage = null;
  if (typeof getMappedHeaderCoverage_ === 'function') {
    try {
      coverage = getMappedHeaderCoverage_(MY_PORTFOLIO_PAGE_);
    } catch (e) {
      coverage = null;
    }
  }

  var ok = headers.length === expectedWidth &&
    missingBusinessHeaders.length === 0 &&
    missingGateHeaders.length === 0 &&
    missingDecisionHeaders.length === 0 &&
    (!coverage || coverage.unmapped === 0);

  var report = {
    ok: ok,
    pageName: MY_PORTFOLIO_PAGE_,
    expectedColumnCount: expectedWidth,
    headerCount: headers.length,
    rowCount: Math.max(0, sheet.getLastRow() - 1),
    inferredLayout: inferMyPortfolioLayout_(headerMap),
    missingBusinessHeaders: missingBusinessHeaders,
    missingPreserveColumns: missingPreserveColumns,
    preserveColumnCount: preserveColumns.length,
    missingGateHeaders: missingGateHeaders,
    missingDecisionHeaders: missingDecisionHeaders,
    gateCoverage: getMyPortfolioInvestmentGateCoverage_(headerMap),
    decisionCoverage: getMyPortfolioDecisionCoverage_(headerMap),
    strictApprovedRows: getMyPortfolioStrictApprovedRows_(),
    symbolsCount: symbols.length,
    sortField: sortPlan ? sortPlan.field : '',
    sortAscending: sortPlan ? sortPlan.ascending : null,
    coverage: coverage
  };

  logRun_(
    ok ? 'INFO' : 'WARN',
    'diagnoseMyPortfolio',
    MY_PORTFOLIO_PAGE_,
    ok ? 'My_Portfolio diagnostic passed.' : 'My_Portfolio diagnostic flagged issues.',
    {
      status: ok ? 'OK' : 'WARN',
      details: report
    }
  );

  return report;
}

/* =========================================================
 * Row-level diagnostics
 * ========================================================= */

function debugMyPortfolioInvestmentGate_(optSymbol) {
  var sheet = ensureSheetExists_(MY_PORTFOLIO_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var headerMap = getHeaderMapMyPortfolio_(headers);
  var lastRow = sheet.getLastRow();

  if (lastRow < 2 || headers.length === 0) {
    var emptyMsg = 'My_Portfolio has no data — run buildMyPortfolio_() first.';
    Logger.log('[v1.4.0 GATE DIAG] ' + emptyMsg);
    return { ok: false, message: emptyMsg };
  }

  var symbolCol = myPortfolioResolveHeaderColumn_(headerMap, 'Symbol');
  if (!symbolCol) {
    var noSymMsg = 'My_Portfolio has no Symbol column.';
    Logger.log('[v1.4.0 GATE DIAG] ' + noSymMsg);
    return { ok: false, message: noSymMsg };
  }

  var target = resolveMyPortfolioTargetRow_(sheet, symbolCol, optSymbol);
  if (!target.ok) {
    Logger.log('[v1.4.0 GATE DIAG] ' + target.message);
    return target;
  }

  var rowValues = sheet.getRange(target.row, 1, 1, Math.max(headers.length, sheet.getLastColumn())).getValues()[0];

  var fields = MY_PORTFOLIO_INVESTMENT_GATE_HEADERS_.concat([
    'Current Price',
    'Risk Bucket',
    'Expected ROI 3M',
    'Expected ROI 12M',
    'Recommendation',
    'Recommendation Detail',
    'Rebalance Action',
    'Investor Decision',
    'Warnings'
  ]);

  var fieldReport = {};
  var missingColumns = [];
  var blankFields = [];

  fields.forEach(function (field) {
    var col = myPortfolioResolveHeaderColumn_(headerMap, field);

    if (!col) {
      missingColumns.push(field);
      fieldReport[field] = {
        present: false,
        blank: true,
        value: null
      };
      return;
    }

    var value = rowValues[col - 1];
    var blank = myPortfolioIsBlank_(value);

    if (blank) {
      blankFields.push(field);
    }

    fieldReport[field] = {
      present: true,
      blank: blank,
      value: value
    };
  });

  var ok = missingColumns.length === 0 &&
    blankFields.indexOf('Final Action') === -1 &&
    blankFields.indexOf('Investability Status') === -1;

  Logger.log('[v1.4.0 GATE DIAG] ' +
    (ok ? 'OK' : 'WARN') +
    ' symbol=' + target.symbol +
    ' row=' + target.row +
    ' missingColumns=' + missingColumns.length +
    ' blankFields=' + blankFields.length);

  return {
    ok: ok,
    symbol: target.symbol,
    row: target.row,
    missingColumns: missingColumns,
    blankFields: blankFields,
    fields: fieldReport
  };
}


/* =========================================================
 * Portfolio manual-preserve diagnostics
 * ========================================================= */

function getMyPortfolioPreserveColumns_() {
  return getPreserveManualColumns(MY_PORTFOLIO_PAGE_);
}

function debugMyPortfolioPreserve_(optSymbol) {
  var sheet = ensureSheetExists_(MY_PORTFOLIO_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var headerMap = getHeaderMapMyPortfolio_(headers);
  var preserveColumns = getMyPortfolioPreserveColumns_() || [];
  var lastRow = sheet.getLastRow();

  if (lastRow < 2 || headers.length === 0) {
    var emptyMsg = 'My_Portfolio has no data — run buildMyPortfolio_() first.';
    Logger.log('[v1.4.0 PRESERVE DIAG] ' + emptyMsg);
    return { ok: false, message: emptyMsg };
  }

  var symbolColIdx = myPortfolioResolveHeaderColumn_(headerMap, 'Symbol');
  if (!symbolColIdx) {
    var noSymMsg = 'My_Portfolio has no Symbol column.';
    Logger.log('[v1.4.0 PRESERVE DIAG] ' + noSymMsg);
    return { ok: false, message: noSymMsg };
  }

  var target = resolveMyPortfolioTargetRow_(sheet, symbolColIdx, optSymbol);
  if (!target.ok) {
    Logger.log('[v1.4.0 PRESERVE DIAG] ' + target.message);
    return target;
  }

  var rowValues = sheet.getRange(target.row, 1, 1, Math.max(headers.length, sheet.getLastColumn())).getValues()[0];
  var preserveValues = [];

  Logger.log('[v1.4.0 PRESERVE DIAG] symbol=' + target.symbol + ' row=' + target.row);

  for (var p = 0; p < preserveColumns.length; p++) {
    var colName = preserveColumns[p];
    var col = myPortfolioResolveHeaderColumn_(headerMap, colName);
    var value = col ? rowValues[col - 1] : null;

    preserveValues.push({
      name: colName,
      present: !!col,
      value: value
    });

    Logger.log('[v1.4.0 PRESERVE DIAG]   ' + colName +
      ': present=' + !!col +
      ' value=' + JSON.stringify(value));
  }

  return {
    ok: true,
    symbol: target.symbol,
    row: target.row,
    preserveColumns: preserveValues,
    preserveColumnCount: preserveColumns.length
  };
}

function preservePortfolioInputs_(oldSheetData, newRows, headers) {
  var safeOldData = Array.isArray(oldSheetData) ? oldSheetData : [];
  var safeNewRows = Array.isArray(newRows) ? newRows : [];
  var safeHeaders = Array.isArray(headers) ? headers : [];
  var preserveColumns = getMyPortfolioPreserveColumns_() || [];
  var oldMap = _mpMapBySymbol_(safeOldData, safeHeaders);
  var symbolColumnName = _mpFindSymbolHeader_(safeHeaders);
  var symbolIndex = symbolColumnName ? safeHeaders.indexOf(symbolColumnName) : -1;

  var snapshot = {
    preserveColumns: preserveColumns.slice(),
    symbolsFound: Object.keys(oldMap),
    mergedRowsPreviewCount: 0
  };

  if (symbolIndex < 0 || !safeNewRows.length) {
    return snapshot;
  }

  var mergedPreview = 0;
  for (var i = 0; i < safeNewRows.length; i++) {
    var row = safeNewRows[i];
    if (!Array.isArray(row)) continue;
    var symbol = String(row[symbolIndex] == null ? '' : row[symbolIndex]).trim().toUpperCase();
    if (symbol && oldMap[symbol]) mergedPreview++;
  }

  snapshot.mergedRowsPreviewCount = mergedPreview;
  return snapshot;
}

function _mpMapBySymbol_(rows, headers) {
  var map = {};
  if (!Array.isArray(rows) || rows.length === 0) return map;

  var first = rows[0];
  if (first && !Array.isArray(first) && typeof first === 'object') {
    var symbolKey = _mpFindSymbolHeader_(Object.keys(first));
    if (!symbolKey) return map;

    for (var i = 0; i < rows.length; i++) {
      var rec = rows[i];
      if (!rec || typeof rec !== 'object') continue;
      var sym = String(rec[symbolKey] == null ? '' : rec[symbolKey]).trim().toUpperCase();
      if (sym) map[sym] = rec;
    }
    return map;
  }

  var headerSymbolName = _mpFindSymbolHeader_(headers);
  var symbolIndex = headerSymbolName && Array.isArray(headers) ? headers.indexOf(headerSymbolName) : -1;
  if (symbolIndex < 0) return map;

  for (var j = 0; j < rows.length; j++) {
    var row = rows[j];
    if (!Array.isArray(row)) continue;
    var sym2 = String(row[symbolIndex] == null ? '' : row[symbolIndex]).trim().toUpperCase();
    if (sym2) map[sym2] = row;
  }

  return map;
}

function _mpFindSymbolHeader_(headers) {
  if (!Array.isArray(headers)) return null;

  var candidates = ['Symbol', 'Ticker', 'symbol', 'ticker'];
  for (var i = 0; i < headers.length; i++) {
    var h = String(headers[i] == null ? '' : headers[i]).trim();
    if (!h) continue;
    for (var j = 0; j < candidates.length; j++) {
      if (h === candidates[j] || h.toLowerCase() === candidates[j].toLowerCase()) {
        return headers[i];
      }
    }
  }

  return null;
}

/* =========================================================
 * Helpers — local gate audit
 * ========================================================= */

function validateMyPortfolioGateLocally_(sheet, headers) {
  var result = {
    ok: true,
    pageName: MY_PORTFOLIO_PAGE_,
    checkedRows: 0,
    investRows: 0,
    warnings: [],
    investWithBlankPrice: [],
    investWithNegativeRoi3m: [],
    investWithNegativeRoi12m: [],
    investWithHighRisk: [],
    investWithConflict: [],
    investWithBlockReason: [],
    investWithLowDataQuality: [],
    investWithLowReliability: []
  };

  headers = headers || readCurrentSheetHeaders_(sheet);

  if (!headers.length || sheet.getLastRow() < 2) {
    result.ok = false;
    result.warnings.push('My_Portfolio has no data rows.');
    return result;
  }

  var headerMap = getHeaderMapMyPortfolio_(headers);

  var cols = {
    symbol: myPortfolioResolveHeaderColumn_(headerMap, 'Symbol'),
    price: myPortfolioResolveHeaderColumn_(headerMap, 'Current Price'),
    finalAction: myPortfolioResolveHeaderColumn_(headerMap, 'Final Action'),
    investability: myPortfolioResolveHeaderColumn_(headerMap, 'Investability Status'),
    risk: myPortfolioResolveHeaderColumn_(headerMap, 'Risk Bucket'),
    roi3m: myPortfolioResolveHeaderColumn_(headerMap, 'Expected ROI 3M'),
    roi12m: myPortfolioResolveHeaderColumn_(headerMap, 'Expected ROI 12M'),
    dq: myPortfolioResolveHeaderColumn_(headerMap, 'Data Quality Score'),
    rel: myPortfolioResolveHeaderColumn_(headerMap, 'Forecast Reliability Score'),
    conflict: myPortfolioResolveHeaderColumn_(headerMap, 'Provider/Engine Conflict'),
    blockReason: myPortfolioResolveHeaderColumn_(headerMap, 'Block Reason')
  };

  if (!cols.symbol || !cols.finalAction) {
    result.ok = false;
    result.warnings.push('Local gate validation cannot run because Symbol or Final Action column is missing.');
    return result;
  }

  var dqMin = getMyPortfolioStrictThreshold_('TFB_STRICT_INVEST_DQ_MIN', 80);
  var relMin = getMyPortfolioStrictThreshold_('TFB_STRICT_INVEST_RELIABILITY_MIN', 70);

  var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, Math.max(headers.length, sheet.getLastColumn())).getValues();

  for (var i = 0; i < values.length; i++) {
    var row = values[i];
    var rowNum = i + 2;
    var symbol = String(row[cols.symbol - 1] || '').trim();

    if (!symbol) continue;

    result.checkedRows++;

    var finalAction = String(row[cols.finalAction - 1] || '').trim().toUpperCase();
    if (finalAction !== 'INVEST') continue;

    result.investRows++;

    var tag = symbol + '@R' + rowNum;

    if (cols.price && myPortfolioIsBlank_(row[cols.price - 1])) {
      result.investWithBlankPrice.push(tag);
    }

    if (cols.roi3m) {
      var roi3m = myPortfolioNumber_(row[cols.roi3m - 1], null);
      if (roi3m !== null && roi3m <= 0) result.investWithNegativeRoi3m.push(tag + '=' + roi3m);
    }

    if (cols.roi12m) {
      var roi12m = myPortfolioNumber_(row[cols.roi12m - 1], null);
      if (roi12m !== null && roi12m <= 0) result.investWithNegativeRoi12m.push(tag + '=' + roi12m);
    }

    if (cols.risk) {
      var risk = String(row[cols.risk - 1] || '').trim().toUpperCase();
      if (risk === 'HIGH') result.investWithHighRisk.push(tag);
    }

    if (cols.conflict) {
      var conflict = myPortfolioBoolean_(row[cols.conflict - 1], false);
      if (conflict) result.investWithConflict.push(tag);
    }

    if (cols.blockReason) {
      var blockReason = String(row[cols.blockReason - 1] || '').trim();
      if (blockReason) result.investWithBlockReason.push(tag);
    }

    if (cols.dq) {
      var dq = myPortfolioNumber_(row[cols.dq - 1], null);
      if (dq !== null && dq < dqMin) result.investWithLowDataQuality.push(tag + '=' + dq);
    }

    if (cols.rel) {
      var rel = myPortfolioNumber_(row[cols.rel - 1], null);
      if (rel !== null && rel < relMin) result.investWithLowReliability.push(tag + '=' + rel);
    }
  }

  myPortfolioPushWarning_(result.warnings, 'INVEST with blank Current Price', result.investWithBlankPrice);
  myPortfolioPushWarning_(result.warnings, 'INVEST with Expected ROI 3M <= 0', result.investWithNegativeRoi3m);
  myPortfolioPushWarning_(result.warnings, 'INVEST with Expected ROI 12M <= 0', result.investWithNegativeRoi12m);
  myPortfolioPushWarning_(result.warnings, 'INVEST with HIGH risk', result.investWithHighRisk);
  myPortfolioPushWarning_(result.warnings, 'INVEST with Provider/Engine Conflict', result.investWithConflict);
  myPortfolioPushWarning_(result.warnings, 'INVEST with nonblank Block Reason', result.investWithBlockReason);
  myPortfolioPushWarning_(result.warnings, 'INVEST with Data Quality below ' + dqMin, result.investWithLowDataQuality);
  myPortfolioPushWarning_(result.warnings, 'INVEST with Forecast Reliability below ' + relMin, result.investWithLowReliability);

  result.ok = result.warnings.length === 0;
  return result;
}

function getMyPortfolioStrictApprovedRows_() {
  var sheet = ensureSheetExists_(MY_PORTFOLIO_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);

  if (!headers.length || sheet.getLastRow() < 2) {
    return [];
  }

  var headerMap = getHeaderMapMyPortfolio_(headers);

  var cols = {
    symbol: myPortfolioResolveHeaderColumn_(headerMap, 'Symbol'),
    recommendation: myPortfolioResolveHeaderColumn_(headerMap, 'Recommendation'),
    finalAction: myPortfolioResolveHeaderColumn_(headerMap, 'Final Action'),
    investability: myPortfolioResolveHeaderColumn_(headerMap, 'Investability Status'),
    price: myPortfolioResolveHeaderColumn_(headerMap, 'Current Price'),
    dq: myPortfolioResolveHeaderColumn_(headerMap, 'Data Quality Score'),
    rel: myPortfolioResolveHeaderColumn_(headerMap, 'Forecast Reliability Score'),
    conflict: myPortfolioResolveHeaderColumn_(headerMap, 'Provider/Engine Conflict'),
    risk: myPortfolioResolveHeaderColumn_(headerMap, 'Risk Bucket'),
    roi3m: myPortfolioResolveHeaderColumn_(headerMap, 'Expected ROI 3M'),
    roi12m: myPortfolioResolveHeaderColumn_(headerMap, 'Expected ROI 12M'),
    blockReason: myPortfolioResolveHeaderColumn_(headerMap, 'Block Reason')
  };

  if (!cols.symbol || !cols.finalAction || !cols.investability) {
    return [];
  }

  var dqMin = getMyPortfolioStrictThreshold_('TFB_STRICT_INVEST_DQ_MIN', 80);
  var relMin = getMyPortfolioStrictThreshold_('TFB_STRICT_INVEST_RELIABILITY_MIN', 70);

  var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, Math.max(headers.length, sheet.getLastColumn())).getValues();
  var out = [];

  values.forEach(function (row, i) {
    var symbol = String(row[cols.symbol - 1] || '').trim();
    if (!symbol) return;

    var finalAction = String(row[cols.finalAction - 1] || '').trim().toUpperCase();
    var investability = String(row[cols.investability - 1] || '').trim().toUpperCase();
    var price = cols.price ? row[cols.price - 1] : '';
    var dq = cols.dq ? myPortfolioNumber_(row[cols.dq - 1], null) : null;
    var rel = cols.rel ? myPortfolioNumber_(row[cols.rel - 1], null) : null;
    var conflict = cols.conflict ? myPortfolioBoolean_(row[cols.conflict - 1], false) : false;
    var risk = cols.risk ? String(row[cols.risk - 1] || '').trim().toUpperCase() : '';
    var roi3m = cols.roi3m ? myPortfolioNumber_(row[cols.roi3m - 1], null) : null;
    var roi12m = cols.roi12m ? myPortfolioNumber_(row[cols.roi12m - 1], null) : null;
    var blockReason = cols.blockReason ? String(row[cols.blockReason - 1] || '').trim() : '';

    if (
      finalAction === 'INVEST' &&
      investability === 'INVESTABLE' &&
      !myPortfolioIsBlank_(price) &&
      dq !== null && dq >= dqMin &&
      rel !== null && rel >= relMin &&
      conflict === false &&
      risk !== 'HIGH' &&
      (roi3m === null || roi3m > 0) &&
      (roi12m === null || roi12m > 0) &&
      !blockReason
    ) {
      out.push({
        row: i + 2,
        symbol: symbol,
        recommendation: cols.recommendation ? String(row[cols.recommendation - 1] || '').trim() : '',
        finalAction: finalAction,
        investabilityStatus: investability,
        dataQualityScore: dq,
        forecastReliabilityScore: rel,
        expectedRoi3m: roi3m,
        expectedRoi12m: roi12m,
        riskBucket: risk
      });
    }
  });

  return out;
}

/* =========================================================
 * Helpers — symbols
 * ========================================================= */

function getMyPortfolioSymbols_() {
  var page = getPageConfig(MY_PORTFOLIO_PAGE_);
  var working = getWorkingSymbolsForPage_(page.pageName) || [];

  /* v1.5.3 COST-BASIS UNIVERSE UNION: the My_Portfolio symbol universe is the
   * UNION of the page working list and the _Portfolio_CostBasis symbols, so a
   * holding added to _Portfolio_CostBasis automatically gets a My_Portfolio row
   * (then seeds + computes + renders). Previously cost-basis symbols were used
   * ONLY when the working list was empty (v1.5.1 wipe-guard fallback), so a new
   * cost-basis holding that was not already in the working list never appeared.
   * normalizeMyPortfolioSymbols_ de-duplicates, so overlap is harmless.
   *
   * v1.5.1 WIPE-GUARD still holds: when BOTH lists are empty the result is empty
   * and the refresh entry points abort rather than fall back to default symbols
   * (which would replace real holdings). */
  var costBasis = _myPortfolioCostBasisSymbols_() || [];

  var merged = [];
  var i;
  for (i = 0; i < working.length; i++) { merged.push(working[i]); }
  for (i = 0; i < costBasis.length; i++) { merged.push(costBasis[i]); }

  var normalized = normalizeMyPortfolioSymbols_(merged);

  /* v1.5.6 HIDE-UNHELD: restrict the universe to held (cost-basis) symbols so
   * target-only / watch symbols (no quantity) do not surface as BLOCK rows.
   * Applied ONLY when cost-basis is non-empty, preserving the v1.5.1 WIPE-GUARD:
   * a transient empty cost-basis read leaves the union untouched rather than
   * blanking the page. */
  if (MY_PORTFOLIO_HIDE_UNHELD_ && costBasis.length > 0) {
    normalized = _filterToHeldSymbols_(normalized, costBasis);
  }

  return normalized;
}

/**
 * v1.5.6 — keep only symbols that have a position of record in
 * _Portfolio_CostBasis. Membership is compared on the normalized (trimmed,
 * upper-cased) ticker so it matches normalizeMyPortfolioSymbols_ output.
 * Returns the held subset of `symbols`, preserving their order. Never throws.
 */
function _filterToHeldSymbols_(symbols, costBasisSymbols) {
  var held = {};
  var i;
  var key;
  var cb = Array.isArray(costBasisSymbols) ? costBasisSymbols : [];
  for (i = 0; i < cb.length; i++) {
    key = String(cb[i] || '').trim().toUpperCase();
    if (key) { held[key] = true; }
  }

  var src = Array.isArray(symbols) ? symbols : [];
  var out = [];
  for (i = 0; i < src.length; i++) {
    key = String(src[i] || '').trim().toUpperCase();
    if (key && held.hasOwnProperty(key)) { out.push(src[i]); }
  }
  return out;
}

/**
 * v1.5.1 — symbols of record from _Portfolio_CostBasis. Prefers
 * 14_Portfolio_PnL.gs readCostBasisMap_ (typeof-guarded, one-file-per-
 * delivery); falls back to a direct sheet read. Never throws; returns []
 * when the sheet is absent or empty.
 */
function _myPortfolioCostBasisSymbols_() {
  /* v1.5.9 LEDGER-V2: when _Portfolio_CostBasis carries the 21_Portfolio_
   * Ledger v2 layout (header at row 4), the position of record is the set of
   * ACTIVE-lot symbols — Inactive (sold/closed) lots are deliberately
   * excluded (FEEDER RULE: My_Portfolio reads ACTIVE lots only). The legacy
   * chain below reads header row 1 and would silently return [] against a
   * migrated sheet. Once v2 is detected the legacy chain is never consulted;
   * a v2 read failure returns isV2=false and falls through safely. */
  try {
    var lm0 = _myPortfolioLedgerMap_();
    if (lm0) {
      var out0 = [];
      for (var lk0 in lm0) {
        if (Object.prototype.hasOwnProperty.call(lm0, lk0)) { out0.push(lk0); }
      }
      return out0;
    }
  } catch (e0) { /* never let the v2 probe break the legacy chain */ }
  try {
    if (typeof readCostBasisMap_ === 'function') {
      var map = readCostBasisMap_() || {};
      var out = [];
      for (var k in map) {
        if (Object.prototype.hasOwnProperty.call(map, k)) { out.push(k); }
      }
      return out;
    }
  } catch (e1) {
    /* fall through to direct read */
  }
  try {
    var sh = SpreadsheetApp.getActive().getSheetByName('_Portfolio_CostBasis');
    if (!sh) { return []; }
    var lastRow = sh.getLastRow();
    var lastCol = sh.getLastColumn();
    if (lastRow < 2 || lastCol < 1) { return []; }
    var hdr = sh.getRange(1, 1, 1, lastCol).getValues()[0];
    var symCol = -1;
    for (var i = 0; i < hdr.length; i++) {
      if (String(hdr[i] || '').replace(/^\s+|\s+$/g, '') === 'Symbol') {
        symCol = i + 1;
        break;
      }
    }
    if (symCol < 1) { return []; }
    var vals = sh.getRange(2, symCol, lastRow - 1, 1).getValues();
    var syms = [];
    for (var r = 0; r < vals.length; r++) {
      var s = String(vals[r][0] || '').replace(/^\s+|\s+$/g, '');
      if (s) { syms.push(s); }
    }
    return syms;
  } catch (e2) {
    return [];
  }
}

/**
 * v1.5.9 — the 21_Portfolio_Ledger (v2) ACTIVE-lot cost-basis reader.
 * Detects the v2 layout by the row-4 fingerprint (A4 === 'Symbol' after
 * trim — the same fingerprint 18_Decision_Diagnostics v1.3.0 checks; a v1
 * sheet has data or a title banner there, never the literal 'Symbol'),
 * locates Symbol / Status / Buy Date / Buy Price / Shares / Buy Fees BY
 * HEADER NAME on row 4 ('Quantity' accepted as a defensive Shares alias),
 * and aggregates ACTIVE lots per symbol:
 *   qty      = sum of Shares across active lots;
 *   buyPrice = shares-weighted average cost INCLUDING Buy Fees, computed
 *              only over lots that carry BOTH shares and price (nothing is
 *              fabricated when either is missing), deliberately UNROUNDED
 *              — numerically identical to 21's syncLedgerToPortfolio and
 *              14 v2.5.0's pfReadCostBasisV2_, so the feeders can never
 *              oscillate the Avg Cost cell between runs;
 *   buyDate  = earliest lot date.
 * A lot is INACTIVE only when Status trims to 'inactive' (case-
 * insensitive); blank status counts as active, matching the 21 dropdown
 * default and the diagnostics lot counting. Returns the {SYM: {qty,
 * buyPrice, buyDate, lots}} map (possibly empty — v2 with zero active
 * lots), keyed by trimmed upper-cased symbol, the same {qty, buyPrice}
 * shape readCostBasisMap_ consumers already expect. Returns NULL when the
 * sheet is absent, still v1, or ANY read fails — so every caller falls
 * through to the legacy chain and a transient ledger problem can never
 * blank the page (v1.5.1 wipe-guard preserved). Never throws.
 */
function _myPortfolioLedgerMap_() {
  try {
    var sh = SpreadsheetApp.getActive().getSheetByName('_Portfolio_CostBasis');
    if (!sh) { return null; }
    var lastRow = sh.getLastRow();
    if (lastRow < 4 || sh.getLastColumn() < 1) { return null; }
    var a4 = String(sh.getRange(4, 1).getValue() == null ? ''
        : sh.getRange(4, 1).getValue()).replace(/^\s+|\s+$/g, '');
    if (a4 !== 'Symbol') { return null; }
    var lastCol = Math.max(1, sh.getLastColumn());
    var hdr = sh.getRange(4, 1, 1, lastCol).getValues()[0];
    var col = { symbol: 0, status: 0, buyDate: 0, buyPrice: 0, shares: 0,
                buyFees: 0 };
    for (var i = 0; i < hdr.length; i++) {
      var hName = String(hdr[i] == null ? '' : hdr[i])
          .replace(/^\s+|\s+$/g, '');
      if (!hName) { continue; }
      if (hName === 'Symbol' && !col.symbol) { col.symbol = i + 1; }
      else if (hName === 'Status' && !col.status) { col.status = i + 1; }
      else if (hName === 'Buy Date' && !col.buyDate) { col.buyDate = i + 1; }
      else if (hName === 'Buy Price' && !col.buyPrice) { col.buyPrice = i + 1; }
      else if ((hName === 'Shares' || hName === 'Quantity') && !col.shares) {
        col.shares = i + 1;
      }
      else if (hName === 'Buy Fees' && !col.buyFees) { col.buyFees = i + 1; }
    }
    if (!col.symbol) { return null; }
    var map = {};
    if (lastRow < 5) { return map; }
    var vals = sh.getRange(5, 1, lastRow - 4, lastCol).getValues();
    var wSum = {};
    var wQty = {};
    for (var r = 0; r < vals.length; r++) {
      var row = vals[r];
      var sym = String(row[col.symbol - 1] == null ? '' : row[col.symbol - 1])
          .replace(/^\s+|\s+$/g, '').toUpperCase();
      if (!sym) { continue; }
      var status = col.status
          ? String(row[col.status - 1] == null ? '' : row[col.status - 1])
              .replace(/^\s+|\s+$/g, '').toLowerCase()
          : '';
      if (status === 'inactive') { continue; }
      if (!Object.prototype.hasOwnProperty.call(map, sym)) {
        map[sym] = { qty: null, buyPrice: null, buyDate: null, lots: 0 };
        wSum[sym] = 0;
        wQty[sym] = 0;
      }
      var agg = map[sym];
      agg.lots = agg.lots + 1;
      var qty = col.shares ? myPortfolioNumber_(row[col.shares - 1], null) : null;
      var price = col.buyPrice
          ? myPortfolioNumber_(row[col.buyPrice - 1], null) : null;
      var fees = col.buyFees
          ? myPortfolioNumber_(row[col.buyFees - 1], null) : null;
      if (qty !== null && qty > 0) {
        agg.qty = (agg.qty === null ? 0 : agg.qty) + qty;
        if (price !== null && price > 0) {
          wSum[sym] = wSum[sym] + qty * price +
              (fees !== null && fees > 0 ? fees : 0);
          wQty[sym] = wQty[sym] + qty;
        }
      }
      var d = col.buyDate ? row[col.buyDate - 1] : null;
      if (d instanceof Date && !isNaN(d.getTime())) {
        if (!(agg.buyDate instanceof Date) ||
            d.getTime() < agg.buyDate.getTime()) {
          agg.buyDate = d;
        }
      }
    }
    for (var k in map) {
      if (Object.prototype.hasOwnProperty.call(map, k) && wQty[k] > 0) {
        map[k].buyPrice = wSum[k] / wQty[k];
      }
    }
    return map;
  } catch (e) {
    return null;
  }
}

/**
 * v1.5.9 — shared ledger-feeder preference for the seed/reseed hooks.
 * Returns NULL when 21_Portfolio_Ledger's syncLedgerToPortfolio is not
 * installed (caller falls through to the 14_Portfolio_PnL seeder branch,
 * behaviour unchanged from v1.5.8). When present it is CALLED and owns the
 * My_Portfolio input columns (ACTIVE-lot qty per symbol, fee-inclusive
 * weighted avg cost, unheld rows pruned). On feeder failure this WARNs and
 * returns {ok:false} — it deliberately does NOT fall back to the v1
 * seeder, which would reintroduce the dual-writer this revision removes.
 * Never throws.
 */
function _myPortfolioLedgerSeed_(actionName, okMessage) {
  if (typeof syncLedgerToPortfolio !== 'function') { return null; }
  try {
    var ledgerResult = syncLedgerToPortfolio() || { ok: true };
    logRun_('INFO', actionName, MY_PORTFOLIO_PAGE_, okMessage, {
      status: 'OK',
      details: ledgerResult
    });
    return ledgerResult;
  } catch (eLedger) {
    var msg = eLedger && eLedger.message ? eLedger.message : String(eLedger);
    logRun_('WARN', actionName, MY_PORTFOLIO_PAGE_,
        'Ledger feeder failed (non-fatal): ' + msg, { status: 'WARN' });
    return { ok: false, error: msg };
  }
}

/**
 * v1.5.1 — abort result for a symbol-less My_Portfolio refresh. Logged
 * and toasted; shaped so callers and menu wrappers surface it cleanly.
 */
function _myPortfolioAbortNoSymbols_() {
  var msg = 'My_Portfolio refresh ABORTED: no symbols available (page ' +
      'working list empty AND _Portfolio_CostBasis empty). Refusing to ' +
      'refresh with default symbols — that would replace your holdings. ' +
      'Fill _Portfolio_CostBasis (or the page symbol list) and retry.';
  Logger.log('[v1.5.1 WIPE-GUARD] ' + msg);
  try {
    SpreadsheetApp.getActive().toast(msg, 'TFB My_Portfolio', 8);
  } catch (e) { /* toast unavailable in some contexts */ }
  return { ok: false, aborted: true, reason: 'no_symbols', message: msg };
}

function normalizeMyPortfolioSymbols_(symbols) {
  var safeInput = Array.isArray(symbols) ? symbols : [];

  return myPortfolioUniqueStrings_(
    safeInput
      .map(function (x) {
        return String(x || '').trim().toUpperCase();
      })
      .filter(function (x) {
        return !!x;
      })
  );
}

/* =========================================================
 * Helpers — headers and layout
 * ========================================================= */

function getHeaderMapMyPortfolio_(headers) {
  var safeHeaders = Array.isArray(headers) ? headers : [];
  var map = {};

  for (var i = 0; i < safeHeaders.length; i++) {
    var raw = String(safeHeaders[i] == null ? '' : safeHeaders[i]);
    var trimmed = raw.trim();
    var normalized = myPortfolioNormalizeHeader_(raw);
    var col = i + 1;

    if (raw && map[raw] === undefined) {
      map[raw] = col;
    }

    if (trimmed && map[trimmed] === undefined) {
      map[trimmed] = col;
    }

    if (normalized && map[normalized] === undefined) {
      map[normalized] = col;
    }

    if (normalized && map[String(normalized).toLowerCase()] === undefined) {
      map[String(normalized).toLowerCase()] = col;
    }
  }

  return map;
}

function myPortfolioNormalizeHeader_(text) {
  if (text === null || text === undefined) return '';

  if (typeof _normalizeHeaderText_ === 'function') {
    return _normalizeHeaderText_(text);
  }

  var s = String(text);
  s = s.replace(/[\u200B\u200C\u200D\uFEFF]/g, '');
  s = s.replace(/[\s\u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000]+/g, ' ');
  return s.trim();
}

function myPortfolioResolveHeaderColumn_(headerMap, headerName) {
  if (!headerMap || !headerName) return null;

  var raw = String(headerName);
  var trimmed = raw.trim();
  var normalized = myPortfolioNormalizeHeader_(raw);

  return headerMap[raw] ||
         headerMap[trimmed] ||
         headerMap[normalized] ||
         headerMap[String(normalized).toLowerCase()] ||
         null;
}

function getMissingMyPortfolioBusinessHeaders_(headerMap) {
  var missing = [];

  MY_PORTFOLIO_REQUIRED_BUSINESS_HEADERS_.forEach(function (h) {
    if (!myPortfolioResolveHeaderColumn_(headerMap, h)) {
      missing.push(h);
    }
  });

  return missing;
}

function getMissingMyPortfolioInvestmentGateHeaders_(headerMap) {
  var missing = [];

  MY_PORTFOLIO_INVESTMENT_GATE_HEADERS_.forEach(function (h) {
    if (!myPortfolioResolveHeaderColumn_(headerMap, h)) {
      missing.push(h);
    }
  });

  return missing;
}

function getMyPortfolioInvestmentGateCoverage_(headerMap) {
  var present = [];
  var missing = [];

  MY_PORTFOLIO_INVESTMENT_GATE_HEADERS_.forEach(function (h) {
    if (myPortfolioResolveHeaderColumn_(headerMap, h)) {
      present.push(h);
    } else {
      missing.push(h);
    }
  });

  return {
    total: MY_PORTFOLIO_INVESTMENT_GATE_HEADERS_.length,
    presentCount: present.length,
    missingCount: missing.length,
    present: present,
    missing: missing,
    complete: missing.length === 0
  };
}

function getMissingMyPortfolioDecisionHeaders_(headerMap) {
  var missing = [];

  MY_PORTFOLIO_PORTFOLIO_DECISION_HEADERS_.forEach(function (h) {
    if (!myPortfolioResolveHeaderColumn_(headerMap, h)) {
      missing.push(h);
    }
  });

  return missing;
}

function getMyPortfolioDecisionCoverage_(headerMap) {
  var present = [];
  var missing = [];

  MY_PORTFOLIO_PORTFOLIO_DECISION_HEADERS_.forEach(function (h) {
    if (myPortfolioResolveHeaderColumn_(headerMap, h)) {
      present.push(h);
    } else {
      missing.push(h);
    }
  });

  return {
    total: MY_PORTFOLIO_PORTFOLIO_DECISION_HEADERS_.length,
    presentCount: present.length,
    missingCount: missing.length,
    present: present,
    missing: missing,
    complete: missing.length === 0
  };
}

function inferMyPortfolioLayout_(headerMap) {
  var hasFinalAction = !!myPortfolioResolveHeaderColumn_(headerMap, 'Final Action');
  var hasBlockReason = !!myPortfolioResolveHeaderColumn_(headerMap, 'Block Reason');
  var hasDataQuality = !!myPortfolioResolveHeaderColumn_(headerMap, 'Data Quality Score');
  var hasForecastReliability = !!myPortfolioResolveHeaderColumn_(headerMap, 'Forecast Reliability Score');
  var gateComplete = hasFinalAction && hasBlockReason && hasDataQuality && hasForecastReliability;
  var decisionComplete = getMissingMyPortfolioDecisionHeaders_(headerMap).length === 0;

  if (gateComplete && decisionComplete) {
    return 'full_122_investment_schema';
  }

  if (gateComplete) {
    return 'gate_complete_missing_decision_columns';
  }

  if (
    myPortfolioResolveHeaderColumn_(headerMap, 'Rank (Overall)') ||
    myPortfolioResolveHeaderColumn_(headerMap, 'Opportunity Score') ||
    myPortfolioResolveHeaderColumn_(headerMap, 'Percent Change')
  ) {
    return 'partial_canonical_schema_missing_gate';
  }

  return 'unknown_or_minimal_schema';
}

function resolveMyPortfolioSortPlan_(page, headerMap) {
  var configured = page && page.sort ? page.sort.activeField : '';
  var configuredCol = configured ? myPortfolioResolveHeaderColumn_(headerMap, configured) : null;

  if (configured && configuredCol) {
    return {
      field: configured,
      column: configuredCol,
      ascending: !!(page.sort && page.sort.ascending)
    };
  }

  var rankCol = myPortfolioResolveHeaderColumn_(headerMap, 'Rank (Overall)');
  if (rankCol) {
    return {
      field: 'Rank (Overall)',
      column: rankCol,
      ascending: true
    };
  }

  var roi12mCol = myPortfolioResolveHeaderColumn_(headerMap, 'Expected ROI 12M');
  if (roi12mCol) {
    return {
      field: 'Expected ROI 12M',
      column: roi12mCol,
      ascending: false
    };
  }

  var opportunityCol = myPortfolioResolveHeaderColumn_(headerMap, 'Opportunity Score');
  if (opportunityCol) {
    return {
      field: 'Opportunity Score',
      column: opportunityCol,
      ascending: false
    };
  }

  var pnlPctCol = myPortfolioResolveHeaderColumn_(headerMap, 'Unrealized P/L %');
  if (pnlPctCol) {
    return {
      field: 'Unrealized P/L %',
      column: pnlPctCol,
      ascending: false
    };
  }

  var overallCol = myPortfolioResolveHeaderColumn_(headerMap, 'Overall Score');
  if (overallCol) {
    return {
      field: 'Overall Score',
      column: overallCol,
      ascending: false
    };
  }

  return null;
}

function resolveMyPortfolioTargetRow_(sheet, symbolCol, optSymbol) {
  var lastRow = sheet.getLastRow();
  var targetSymbol = optSymbol ? String(optSymbol).trim().toUpperCase() : '';
  var targetRow = 0;
  var resolvedSymbol = '';

  if (targetSymbol) {
    var symbolValues = sheet.getRange(2, symbolCol, lastRow - 1, 1).getValues();

    for (var i = 0; i < symbolValues.length; i++) {
      if (String(symbolValues[i][0] || '').trim().toUpperCase() === targetSymbol) {
        targetRow = i + 2;
        resolvedSymbol = targetSymbol;
        break;
      }
    }

    if (!targetRow) {
      return {
        ok: false,
        message: 'Symbol "' + targetSymbol + '" not found in My_Portfolio.',
        requestedSymbol: targetSymbol
      };
    }
  } else {
    targetRow = 2;
    resolvedSymbol = String(sheet.getRange(2, symbolCol).getValue() || '').trim().toUpperCase();

    if (!resolvedSymbol) {
      return {
        ok: false,
        message: 'First data row has no Symbol value.'
      };
    }
  }

  return {
    ok: true,
    row: targetRow,
    symbol: resolvedSymbol
  };
}

/* =========================================================
 * Helpers — small utilities
 * ========================================================= */

function myPortfolioPushWarning_(warnings, label, items) {
  if (!items || !items.length) return;
  warnings.push(label + ': ' + items.slice(0, 15).join(', ') + (items.length > 15 ? ' ...(+' + (items.length - 15) + ')' : ''));
}

function myPortfolioIsBlank_(value) {
  return value === null || value === undefined || String(value).trim() === '';
}

function myPortfolioNumber_(value, fallback) {
  if (value === null || value === undefined || value === '') return fallback;

  if (typeof value === 'number') {
    if (!isFinite(value) || isNaN(value)) return fallback;
    return value;
  }

  var s = String(value).trim();
  if (!s) return fallback;

  var isPercent = s.indexOf('%') >= 0;
  s = s.replace(/,/g, '').replace(/%/g, '');

  var n = Number(s);
  if (!isFinite(n) || isNaN(n)) return fallback;

  if (isPercent) return n / 100;
  return n;
}

function myPortfolioBoolean_(value, fallback) {
  if (value === null || value === undefined || value === '') return !!fallback;
  if (typeof value === 'boolean') return value;

  var s = String(value).trim().toLowerCase();

  if (['1', 'true', 'yes', 'y', 'on', 't', 'enabled', 'enable'].indexOf(s) >= 0) return true;
  if (['0', 'false', 'no', 'n', 'off', 'f', 'disabled', 'disable'].indexOf(s) >= 0) return false;

  return !!fallback;
}

function myPortfolioUniqueStrings_(arr) {
  if (typeof uniqueStrings_ === 'function') {
    return uniqueStrings_(arr);
  }

  var seen = {};
  var out = [];
  var safe = Array.isArray(arr) ? arr : [];

  for (var i = 0; i < safe.length; i++) {
    var s = String(safe[i] || '').trim();
    if (!s) continue;
    if (seen[s]) continue;
    seen[s] = true;
    out.push(s);
  }

  return out;
}

function getMyPortfolioStrictThreshold_(propertyName, fallback) {
  try {
    var props = PropertiesService.getScriptProperties();
    var raw = props.getProperty(propertyName);
    if (raw === null || raw === undefined || raw === '') return fallback;
    var n = Number(raw);
    return isNaN(n) || !isFinite(n) ? fallback : n;
  } catch (e) {
    return fallback;
  }
}

/* =========================================================================
 * v1.5.0 — PORTFOLIO DECISION LAYER (Plan v5.0 P6 / M2)
 * Renders core/analysis/portfolio_actions v1.0.0 output on the
 * 'Portfolio_Decision' sheet. Render-only: no action logic in GAS (§8).
 * ========================================================================= */

var PD_VERSION = '1.5.11';
var PD_SHEET = 'Portfolio_Decision';
var PD_ENDPOINT = '/sheet-rows/portfolio-actions';
var PD_DEFAULT_BACKEND = 'https://tadawul-fast-bridge.onrender.com';
var PD_LAST_COL = 26;

var PD_ROW_TITLE = 1;
var PD_ROW_STATUS = 2;
var PD_ROW_PANEL_HEAD = 4;
var PD_ROW_PANEL_FIRST = 5;
var PD_PANEL_ROWS = 3;            // 7 inputs / 3 per row
var PD_ROW_KPI_HEAD = 9;
var PD_ROW_KPI_LABELS = 10;
var PD_ROW_KPI_VALUES = 11;
var PD_ZONES_START = 13;

/* §3.4 token fallbacks — RESKINNED v1.5.8 to the TFB_Final_Proposal navy/gold
 * cockpit palette (matches 16_Decision_Top10.gs DT10_FALLBACK_TOKENS). Renderers
 * read these HEADER / VERDICT_* / OPERATOR_INPUT keys directly; the design-token
 * named range only publishes COLOR_* keys, so editing this map reskins on the
 * next Build + Refresh without reopening the schema contract. */
var PD_FALLBACK_TOKENS = {
  VERDICT_POSITIVE: { fg: '#146C43', bg: '#E6F4EA' },   // ADD  (was #137333/#E6F4EA)
  VERDICT_WATCH:    { fg: '#9A6700', bg: '#FFF3CD' },   // TRIM (was #B45309/#FEF3C7)
  VERDICT_NEGATIVE: { fg: '#B3261E', bg: '#FBE4E2' },   // EXIT (was #B91C1C/#FDE8E8)
  VERDICT_BLOCK:    { fg: '#5B6470', bg: '#ECEEF1' },   // BLOCK(was #5F6368/#F1F3F4)
  HEADER:           { fg: '#FFFFFF', bg: '#1F3A5F' },   // navy (was #FFFFFF/#1F2937)
  OPERATOR_INPUT:   { fg: '#1A57C2', bg: '#E8F0FE' }    // op   (was #1A73E8/#E8F0FE)
};
/* Proposal cockpit constants (presentation only — no data column maps to gold). */
var PD_NAVY = '#1F3A5F';
var PD_NAVY_SUB = '#2C4A70';
var PD_TINT = '#EEF2F7';
var PD_ACCENT_GOLD = '#C8A04B';
var PD_GOLD_TEXT = '#4A3A12';
var PD_GOLD_FILL = '#F3F1EC';
var PD_INK = '#1A2433';
var PD_BORDER = '#D6DEE8';   // was #E0E0E0
var PD_ZEBRA = '#F5F7FA';    // was #FAFAFA
var PD_MUTED = '#6B7280';    // was #5F6368
var PD_FMT_SAR = '#,##0 "SAR"';
var PD_FMT_PRICE = '#,##0.00';
var PD_FMT_PCT = '0.0"%"';
var PD_FMT_INT = '#,##0';

/* §3.2 PF panel — labels verbatim (TFB_PANEL_DEFAULTS / route alias map). */
var PD_PANEL = [
  { label: 'PF: Cash Available SAR', def: 0, kind: 'sar' },
  { label: 'PF: Target Cash %', def: 10, kind: 'num' },
  { label: 'PF: Max Position %', def: 15, kind: 'num' },
  { label: 'PF: Max Sector %', def: 30, kind: 'num' },
  { label: 'PF: Min Reliability to Add', def: 70, kind: 'num' },
  { label: 'PF: Min DQ to Add', def: 80, kind: 'num' },
  { label: 'PF: Rebalance Mode', def: 'Advisory Only', kind: 'text' }
];

/*
 * Holdings field specs: `send` keys hit portfolio_actions/opportunity_builder
 * aliases after normalization ('Position Qty' -> 'Quantity' is the explicit
 * remap; the average cost is sent under BOTH 'Buy Price' (the proven/
 * documented cost-basis key, v1.5.2) AND 'Avg Cost' (back-compat), each
 * reading the same My_Portfolio column, so the cost-basis parser receives the
 * value regardless of which alias it reads). `match` lists My_Portfolio header
 * candidates (resolved via the v1.4.0 header machinery, so unicode/space
 * variants are handled).
 */
var PD_HOLDING_FIELDS = [
  { send: 'Symbol', match: ['Symbol'] },
  { send: 'Name', match: ['Name'] },
  { send: 'Sector', match: ['Sector'] },
  { send: 'Exchange', match: ['Exchange'] },
  { send: 'Currency', match: ['Currency'] },
  { send: 'Quantity', match: ['Position Qty', 'Quantity'] },
  { send: 'Buy Price', match: ['Avg Cost', 'Buy Price'] },
  { send: 'Avg Cost', match: ['Avg Cost', 'Buy Price'] },
  { send: 'Current Price', match: ['Current Price'] },
  { send: 'Target Price', match: ['Price Target', 'Target Price'] },
  { send: 'Intrinsic Value', match: ['Intrinsic Value', 'Fair Value'] },
  { send: 'Expected ROI 12M', match: ['Expected ROI 12M'] },
  { send: 'Forecast Reliability Score',
    match: ['Forecast Reliability Score'] },
  { send: 'Data Quality Score', match: ['Data Quality Score'] },
  { send: 'Risk Bucket', match: ['Risk Bucket', 'Risk Level'] },
  { send: 'Provider/Engine Conflict', match: ['Provider/Engine Conflict'] },
  { send: 'Volatility 30D', match: ['Volatility 30D'] },
  { send: 'Avg Volume 30D', match: ['Avg Volume 30D'] },
  { send: 'Recommendation Detail', match: ['Recommendation Detail'] },
  { send: 'Investability Status', match: ['Investability Status'] },
  { send: 'Block Reason', match: ['Block Reason'] }
];

/* ----- pure helpers (node-testable) -------------------------------------- */

function pdHasValue_(v) {
  if (v === null || v === undefined) return false;
  if (typeof v === 'number') return isFinite(v);
  if (typeof v === 'boolean') return true;
  var s = String(v).trim();
  if (!s) return false;
  var low = s.toLowerCase();
  return !(low === '-' || low === '\u2014' || low === 'n/a' || low === 'na' ||
           low === 'none' || low === 'null' || low === 'nan' ||
           low === '#n/a' || low === 'no data');
}

function pdCell_(v) {
  return (v === null || v === undefined) ? '' : v;
}

var PD_ACTION_HEADERS = ['Action', 'Symbol', 'Name', 'Sector', 'Ccy', 'Qty',
  'Avg Cost', 'Price', 'Price SAR', 'MV SAR', 'Cost SAR', 'P&L SAR', 'P&L %',
  'Weight %', '\u0394 SAR', '\u0394 Shares', 'Funds From', 'Stop SAR',
  'TP1 SAR', 'TP2 SAR', 'ROI %', 'Engine ROI %', 'Rel', 'DQ', 'Conf',
  'Advisor Note'];

function pdActionToRow_(a) {
  var delta = '';
  if (a.action === 'ADD') {
    delta = pdCell_(a.suggested_delta_sar);
  } else if (a.action === 'TRIM' || a.action === 'EXIT') {
    delta = (a.proceeds_sar !== null && a.proceeds_sar !== undefined)
        ? -a.proceeds_sar : '';
  }
  return [pdCell_(a.action), pdCell_(a.symbol), pdCell_(a.name),
          pdCell_(a.sector), pdCell_(a.currency), pdCell_(a.quantity),
          pdCell_(a.avg_cost), pdCell_(a.price), pdCell_(a.price_sar),
          pdCell_(a.market_value_sar), pdCell_(a.cost_sar),
          pdCell_(a.pnl_sar), pdCell_(a.pnl_pct), pdCell_(a.weight_pct),
          delta,
          pdCell_(a.suggested_delta_shares),
          pdCell_(a.funds_from), pdCell_(a.stop_sar), pdCell_(a.tp1_sar),
          pdCell_(a.tp2_sar), pdCell_(a.roi_pct), pdCell_(a.engine_roi_pct),
          pdCell_(a.reliability),
          pdCell_(a.dq), pdCell_(a.confidence_band),
          pdCell_(a.advisor_note)];
}

var PD_SECTOR_HEADERS = ['Sector', 'Value SAR', 'Weight %', 'Cap %',
  'Over Cap', 'Positions'];

function pdSectorToRow_(s) {
  return [pdCell_(s.sector), pdCell_(s.value_sar), pdCell_(s.weight_pct),
          pdCell_(s.cap_pct), s.over_cap === true ? 'Yes' : 'No',
          pdCell_(s.positions)];
}

var PD_ALERT_HEADERS = ['Type', 'Count', 'Required Action'];

function pdAlertToRow_(a) {
  return [pdCell_(a.type), pdCell_(a.count), pdCell_(a.required_action)];
}

var PD_KPI_LABELS = ['Portfolio (SAR)', 'Holdings (SAR)', 'Cash (SAR)',
  'Cash %', 'P&L (SAR)', 'P&L %', 'Deployable (SAR)', 'Adds Funded (SAR)',
  'Unallocated (SAR)', 'Actions A/H/T/E/B'];

function pdKpiValues_(k) {
  k = k || {};
  var c = k.action_counts || {};
  var actions = [pdCell_(c.ADD), pdCell_(c.HOLD), pdCell_(c.TRIM),
                 pdCell_(c.EXIT), pdCell_(c.BLOCK)].join(' / ');
  return [pdCell_(k.portfolio_value_sar), pdCell_(k.holdings_value_sar),
          pdCell_(k.cash_sar), pdCell_(k.cash_pct), pdCell_(k.pnl_sar),
          pdCell_(k.pnl_pct), pdCell_(k.deployable_sar),
          pdCell_(k.adds_funded_sar), pdCell_(k.capital_unallocated_sar),
          actions];
}

function pdMetaLine_(meta) {
  meta = meta || {};
  var route = meta.route || {};
  var versions = meta.versions || {};
  var parts = [];
  parts.push('route v' + pdCell_(route.version));
  parts.push('actions v' + pdCell_(versions.portfolio_actions ||
             route.portfolio_actions_version));
  parts.push('builder v' + pdCell_(versions.opportunity_builder));
  if (route.holdings && route.holdings.count !== undefined) {
    parts.push('holdings=' + route.holdings.count);
  }
  if (meta.cash_floor_sar !== undefined && meta.cash_floor_sar !== null) {
    parts.push('cash_floor=' + meta.cash_floor_sar);
  }
  if (route.duration_ms !== undefined && route.duration_ms !== null) {
    parts.push(Math.round(route.duration_ms) + 'ms');
  }
  if (route.request_id) parts.push('req ' + route.request_id);
  return parts.join(' | ');
}

function pdStatusLine_(status, extra) {
  var stamp = '';
  try {
    stamp = Utilities.formatDate(new Date(),
        Session.getScriptTimeZone() || 'Asia/Riyadh', 'yyyy-MM-dd HH:mm:ss');
  } catch (e) {
    stamp = new Date().toString();
  }
  return 'Last run ' + stamp + ' | status: ' + status +
         (extra ? ' | ' + extra : '');
}

/* ----- config / named-range readers --------------------------------------- */

function pdBackendUrl_() {
  try {
    if (typeof getBackendUrl_ === 'function') {
      var u = getBackendUrl_();
      if (u) return String(u).replace(/\/+$/, '');
    }
  } catch (e1) {}
  try {
    var props = PropertiesService.getScriptProperties();
    var keys = ['BACKEND_URL', 'TFB_BACKEND_URL', 'BACKEND_BASE_URL'];
    for (var i = 0; i < keys.length; i++) {
      var v = props.getProperty(keys[i]);
      if (v) return String(v).replace(/\/+$/, '');
    }
  } catch (e2) {}
  return PD_DEFAULT_BACKEND;
}

function pdAppToken_() {
  try {
    if (typeof getAppToken_ === 'function') {
      var t = getAppToken_();
      if (t) return String(t);
    }
  } catch (e1) {}
  try {
    var props = PropertiesService.getScriptProperties();
    var keys = ['APP_TOKEN', 'TFB_APP_TOKEN', 'X_APP_TOKEN'];
    for (var i = 0; i < keys.length; i++) {
      var v = props.getProperty(keys[i]);
      if (v) return String(v);
    }
  } catch (e2) {}
  return '';
}

/* ----- v1.5.11 source receipt + reconciliation evidence ------------------- */

var PD_RECEIPT_MODE_PROP = 'PD_SOURCE_RECEIPT';  /* off (default) | observe | enforce */
var PD_EVIDENCE_PROP = 'TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1';
var PD_EVIDENCE_MAX_CHARS = 9000;
var PD_SRC_PAGE = 'My_Portfolio';
var PD_SRC_MAX_AGE_MS = 8 * 3600000;    /* final audit: My_Portfolio <= 8 h */
var PD_SRC_STATUS_MAX_ROWS = 1000;

function pdReceiptMode_() {
  try {
    var raw = PropertiesService.getScriptProperties()
        .getProperty(PD_RECEIPT_MODE_PROP);
    var v = String(raw === null || raw === undefined ? '' : raw)
        .replace(/^\s+|\s+$/g, '').toLowerCase();
    return (v === 'observe' || v === 'enforce') ? v : 'off';
  } catch (e) {
    return 'off';
  }
}

/** Private declared evidence; same document property and limits as
 * 16_Decision_Top10 dt10ReconciliationEvidence_. Missing, unavailable,
 * malformed or oversized -> null. Never logged or rendered. */
function pdEvidencePacket_() {
  try {
    var props = PropertiesService.getDocumentProperties();
    if (!props) return null;
    var raw = props.getProperty(PD_EVIDENCE_PROP);
    if (typeof raw !== 'string' || !raw || raw.length > PD_EVIDENCE_MAX_CHARS) {
      return null;
    }
    var packet = JSON.parse(raw);
    if (!packet || Object.prototype.toString.call(packet) !== '[object Object]' ||
        packet.schema_version !== 1) return null;
    return packet;
  } catch (e) {
    return null;
  }
}

function pdSrcNormToken_(v) {
  return String(v === null || v === undefined ? '' : v).toLowerCase()
      .replace(/[^a-z0-9]/g, '');
}

/** _Status clock (port of dt10SourceStampMs_): a Date keeps its instant, a
 * naive string is Riyadh wall time, an explicit offset is honoured;
 * date-only, malformed or impossible values -> null. */
function pdSrcStampMs_(value) {
  if (Object.prototype.toString.call(value) === '[object Date]') {
    var dateMs = value.getTime();
    return isFinite(dateMs) ? dateMs : null;
  }
  var m = String(value === null || value === undefined ? '' : value)
      .replace(/^\s+|\s+$/g, '')
      .match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?(Z|[+-]\d{2}:?\d{2})?$/);
  if (!m) return null;
  var year = +m[1], month = +m[2], day = +m[3];
  var hour = +m[4], minute = +m[5], second = +m[6];
  if (year < 1900 || month < 1 || month > 12 || day < 1 || day > 31 ||
      hour > 23 || minute > 59 || second > 59) return null;
  var utc = Date.UTC(year, month - 1, day, hour, minute, second,
      m[7] ? Number((m[7] + '00').slice(0, 3)) : 0);
  var check = new Date(utc);
  if (check.getUTCFullYear() !== year || check.getUTCMonth() !== month - 1 ||
      check.getUTCDate() !== day) return null;
  var offset = 180;  /* _Status naive Last Updated is Riyadh, never UTC */
  if (m[8] === 'Z') {
    offset = 0;
  } else if (m[8]) {
    var parts = m[8].match(/^([+-])(\d{2}):?(\d{2})$/);
    if (+parts[2] > 23 || +parts[3] > 59) return null;
    offset = (+parts[2] * 60 + +parts[3]) * (parts[1] === '-' ? -1 : 1);
  }
  return utc - offset * 60000;
}

/** Exactly one name=value token, or '' (absent or repeated). */
function pdSrcToken_(message, name) {
  var text = String(message === null || message === undefined ? '' : message);
  var re = new RegExp('(?:^|[\\s|])' + name + '=([^\\s|]+)', 'gi');
  var first = re.exec(text);
  return first && !re.exec(text) ? first[1] : '';
}

function pdSrcCount_(value) {
  if (typeof value === 'boolean' || value === null || value === undefined ||
      String(value).replace(/^\s+|\s+$/g, '') === '') return null;
  var n = Number(String(value).replace(/,/g, ''));
  return isFinite(n) && n >= 0 && n <= 9007199254740991 && Math.floor(n) === n
      ? n : null;
}

/** PURE readiness of the My_Portfolio publication this decision consumed.
 * ctx = the pre-read capture (or null); consumedRows = holdings sent (or
 * null). Reasons never contain the receipt phrase itself. */
function pdSrcReadinessCore_(grid, nowMs, ctx, consumedRows) {
  var out = {ready: false, reason: '', issues: [], generation: '', cohort: ''};
  function issue(text) { out.issues.push(text); }
  var header = (grid && grid[0]) || [];
  var index = {};
  for (var h = 0; h < header.length; h++) {
    var tok = pdSrcNormToken_(header[h]);
    if (!tok) continue;
    if (index.hasOwnProperty(tok)) issue('duplicate _Status header ' + tok);
    else index[tok] = h;
  }
  var need = ['page', 'lastupdated', 'status', 'message', 'rows'];
  for (var k = 0; k < need.length; k++) {
    if (!index.hasOwnProperty(need[k])) issue('_Status missing ' + need[k]);
  }
  if (typeof nowMs !== 'number' || !isFinite(nowMs)) issue('decision clock invalid');
  if (out.issues.length) { out.reason = out.issues.join('; '); return out; }
  var row = null, seen = 0;
  for (var r = 1; r < grid.length; r++) {
    var g = grid[r] || [];
    var page = String(g[index.page] === null || g[index.page] === undefined
        ? '' : g[index.page]).replace(/^\s+|\s+$/g, '');
    if (page === PD_SRC_PAGE) { seen++; row = g; }
  }
  if (seen !== 1) {
    issue(PD_SRC_PAGE + (seen ? ' status row repeated' : ' status row missing'));
    out.reason = out.issues.join('; ');
    return out;
  }
  var state = String(row[index.status] === null || row[index.status] === undefined
      ? '' : row[index.status]).replace(/^\s+|\s+$/g, '').toUpperCase();
  var message = String(row[index.message] === null || row[index.message] === undefined
      ? '' : row[index.message]);
  var stamp = pdSrcStampMs_(row[index.lastupdated]);
  var count = pdSrcCount_(row[index.rows]);
  out.generation = JSON.stringify([String(row[index.lastupdated]), state, message, count]);
  var run = pdSrcToken_(message, 'run');
  if (!/^[1-9]\d*$/.test(run)) issue('publication run unverified');
  else out.cohort = run;
  if (!/^(OK|SUCCESS|VALID|PASS|COMPLETE)$/.test(state)) {
    issue('status=' + (state || 'unknown'));
  }
  if (stamp === null) issue('publication timestamp unverified');
  else if (stamp > nowMs) issue('publication is in the future');
  else if (nowMs - stamp > PD_SRC_MAX_AGE_MS) issue('publication stale (>8h)');
  if (count === null || count < 1) {
    issue('rows ' + (count === null ? 'unknown' : count) + ' below floor 1');
  }
  var acq = pdSrcToken_(message, 'acquired').match(/^(\d+)\/(\d+)$/);
  var fresh = acq ? Number(acq[1]) : null;
  var requested = acq ? Number(acq[2]) : null;
  if (fresh === null || requested === null || !isFinite(fresh) || !isFinite(requested) ||
      requested <= 0 || requested > 9007199254740991 || fresh > requested) {
    issue('acquisition receipt unknown/invalid');
  } else if (fresh !== requested) {
    issue('acquired ' + fresh + '/' + requested + ' below 100%');
  }
  if (pdSrcToken_(message, 'acquisition').toUpperCase() !== 'COMPLETE') {
    issue('acquisition not COMPLETE');
  }
  if (pdSrcToken_(message, 'data').toUpperCase() !== 'COMPLETE') issue('data not COMPLETE');
  var effective = pdSrcToken_(message, 'fetchfail_effective').toLowerCase();
  if (effective === 'error') issue('acquisition policy error');
  else if (!/^(off|observe|enforce)$/.test(effective)) {
    issue('acquisition policy unknown/ambiguous');
  }
  if (consumedRows !== null && consumedRows !== undefined && count !== null &&
      consumedRows !== count) {
    issue('consumed ' + consumedRows + ' holdings vs ' + count + ' published rows');
  }
  if (ctx) {
    if (ctx.initial_ready !== true) {
      issue('publication not ready before the holdings read (' +
            (ctx.reason || 'unknown') + ')');
    }
    if (ctx.generation !== out.generation) issue('publication changed during the decision');
    if (stamp !== null && typeof ctx.captured_at_ms === 'number' &&
        stamp > ctx.captured_at_ms) issue('publication newer than the holdings read');
  }
  out.ready = out.issues.length === 0;
  out.reason = out.issues.join('; ');
  return out;
}

/** Bounded _Status read (A1:J); never writes. Any failure -> not ready. */
function pdSrcReadiness_(ss, ctx, consumedRows) {
  try {
    var sheet = ss.getSheetByName('_Status');
    if (!sheet) throw new Error('_Status missing');
    var rows = sheet.getLastRow(), cols = sheet.getLastColumn();
    if (!(rows >= 2) || rows > PD_SRC_STATUS_MAX_ROWS || !(cols >= 7)) {
      throw new Error('_Status dimensions invalid');
    }
    var values = sheet.getRange(1, 1, rows, Math.min(cols, 10)).getValues();
    return pdSrcReadinessCore_(values, Date.now(), ctx, consumedRows);
  } catch (e) {
    return {ready: false, reason: 'publication status unavailable',
            issues: ['publication status unavailable'], generation: '', cohort: ''};
  }
}

/** Taken immediately BEFORE the holdings read. */
function pdSrcCapture_(ss) {
  var captured = Date.now();
  var source = pdSrcReadiness_(ss, null, null);
  return {captured_at_ms: captured, initial_ready: source.ready,
          generation: source.generation, cohort: source.cohort || '',
          reason: source.reason || ''};
}

/** Status-line fragment. off -> '' (v1.5.10 byte-identical); observe -> a
 * tag the final audit does not parse; enforce -> exactly one receipt. */
function pdReceiptNote_(mode, ss, ctx, consumedRows, payload) {
  if (mode !== 'observe' && mode !== 'enforce') return '';
  var src;
  try {
    src = pdSrcReadiness_(ss, ctx, consumedRows);
    var st = (payload && payload.status !== null && payload.status !== undefined)
        ? String(payload.status).toLowerCase() : '';
    if (src.ready && st !== 'ok') {
      src = {ready: false, reason: 'decision status ' + (st || 'unknown'), cohort: ''};
    }
  } catch (e) {
    src = {ready: false, reason: 'receipt check failed', cohort: ''};
  }
  var why = String(src.reason || 'unknown')
      .replace(/source\s+readiness/gi, 'source check').slice(0, 240);
  var verdict = src.ready ? 'VERIFIED (run ' + src.cohort + ')' : 'WITHHELD: ' + why;
  return mode === 'observe'
      ? ' | [receipt-observe] would publish ' + verdict
      : ' | source readiness ' + verdict;
}

function pdTokens_(ss) {
  var out = {};
  for (var k in PD_FALLBACK_TOKENS) {
    if (PD_FALLBACK_TOKENS.hasOwnProperty(k)) {
      out[k] = { fg: PD_FALLBACK_TOKENS[k].fg, bg: PD_FALLBACK_TOKENS[k].bg };
    }
  }
  try {
    var rng = ss.getRangeByName('TFB_DESIGN_TOKENS');
    if (rng) {
      var vals = rng.getValues();
      for (var i = 0; i < vals.length; i++) {
        var name = String(vals[i][0] || '').trim();
        if (!name) continue;
        out[name] = { fg: String(vals[i][1] || '') || (out[name] || {}).fg,
                      bg: String(vals[i][2] || '') || (out[name] || {}).bg };
      }
    }
  } catch (e) {}
  return out;
}

function pdFxRates_(ss) {
  var out = {};
  try {
    var rng = ss.getRangeByName('TFB_FX_LOOKUP');
    if (!rng) return out;
    var vals = rng.getValues();
    for (var i = 0; i < vals.length; i++) {
      var ccy = String(vals[i][0] || '').trim();
      var eff = vals[i][3];
      if (ccy && typeof eff === 'number' && isFinite(eff) && eff > 0) {
        out[ccy] = eff;
      }
    }
  } catch (e) {}
  return out;
}

function pdPanelDefaults_(ss) {
  var out = {};
  try {
    var rng = ss.getRangeByName('TFB_PANEL_DEFAULTS');
    if (!rng) return out;
    var vals = rng.getValues();
    for (var i = 0; i < vals.length; i++) {
      var label = String(vals[i][0] || '').trim();
      if (label) out[label] = vals[i][1];
    }
  } catch (e) {}
  return out;
}

function pdPanelPos_(i) {
  var row = PD_ROW_PANEL_FIRST + Math.floor(i / 3);
  var slot = i % 3;
  var labelCol = 1 + slot * 3;
  return { row: row, labelCol: labelCol, valueCol: labelCol + 1 };
}

function pdReadPanel_(sheet, ss) {
  var defaults = pdPanelDefaults_(ss);
  var out = {};
  for (var i = 0; i < PD_PANEL.length; i++) {
    var item = PD_PANEL[i];
    var pos = pdPanelPos_(i);
    var v = sheet.getRange(pos.row, pos.valueCol).getValue();
    if (!pdHasValue_(v)) {
      v = defaults.hasOwnProperty(item.label) &&
          pdHasValue_(defaults[item.label]) ? defaults[item.label] : item.def;
    }
    out[item.label] = v;
  }
  return out;
}

/* ----- holdings pool (reads the 122-col My_Portfolio page) ----------------- */

function pdCollectHoldings_() {
  var sheet = ensureSheetExists_(MY_PORTFOLIO_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  if (!headers.length || sheet.getLastRow() < 2) {
    return { rows: [], reason: 'My_Portfolio has no data rows' };
  }
  var headerMap = getHeaderMapMyPortfolio_(headers);
  var colByField = {};
  for (var f = 0; f < PD_HOLDING_FIELDS.length; f++) {
    var spec = PD_HOLDING_FIELDS[f];
    var col = null;
    for (var m = 0; m < spec.match.length; m++) {
      col = myPortfolioResolveHeaderColumn_(headerMap, spec.match[m]);
      if (col) break;
    }
    colByField[spec.send] = col;
  }
  if (!colByField['Symbol']) {
    return { rows: [], reason: 'My_Portfolio has no Symbol column' };
  }
  /* v1.5.4: pull Quantity + cost DIRECTLY from _Portfolio_CostBasis (the
   * durable source of truth) and let it OVERRIDE My_Portfolio for the decision
   * POST. The engine BLOCKs a holding when quantity is missing; the manual
   * Position Qty / Avg Cost cells on My_Portfolio go blank whenever the engine
   * refresh does not preserve them after the pre-refresh seed writes them, so
   * the decision page could not see the quantity (every row -> "missing
   * quantity"). Reading cost basis here makes the decision layer independent of
   * that fragile hop. Cost is sent under BOTH 'Buy Price' (the proven backend
   * key) and 'Avg Cost'. readCostBasisMap_ is typeof-guarded (14_Portfolio_PnL
   * provides it); absent it we use whatever My_Portfolio holds.
   *
   * v1.5.9 LEDGER-V2: the v2 ledger ACTIVE-lot aggregate (qty = sum of
   * Shares, cost = weighted avg Buy Price) is preferred when the migrated
   * layout is detected — readCostBasisMap_ reads header row 1 and returns an
   * empty map against a v2 sheet, which would send every holding back to
   * BLOCK "missing quantity". Same {qty, buyPrice} shape either way. */
  var cb = null;
  try { cb = _myPortfolioLedgerMap_(); } catch (eLV) { cb = null; }
  if (!cb) {
    cb = {};
    try {
      if (typeof readCostBasisMap_ === 'function') { cb = readCostBasisMap_() || {}; }
    } catch (eCB) { cb = {}; }
  }

  var values = sheet.getRange(2, 1, sheet.getLastRow() - 1,
      Math.max(headers.length, sheet.getLastColumn())).getValues();
  var rows = [];
  var cbApplied = 0;
  for (var r = 0; r < values.length; r++) {
    var sym = values[r][colByField['Symbol'] - 1];
    if (!pdHasValue_(sym)) continue;
    var obj = {};
    for (var i = 0; i < PD_HOLDING_FIELDS.length; i++) {
      var send = PD_HOLDING_FIELDS[i].send;
      var c = colByField[send];
      if (!c) continue;
      var v = values[r][c - 1];
      if (pdHasValue_(v)) obj[send] = v;
    }

    var cbInfo = cb[String(sym).replace(/^\s+|\s+$/g, '').toUpperCase()];
    if (cbInfo) {
      var usedCb = false;
      if (cbInfo.qty !== null && cbInfo.qty !== undefined) {
        obj['Quantity'] = cbInfo.qty;
        usedCb = true;
      }
      if (cbInfo.buyPrice !== null && cbInfo.buyPrice !== undefined) {
        obj['Buy Price'] = cbInfo.buyPrice;
        obj['Avg Cost'] = cbInfo.buyPrice;
        usedCb = true;
      }
      if (usedCb) { cbApplied = cbApplied + 1; }
    }

    rows.push(obj);
  }
  return { rows: rows, reason: '', costBasisApplied: cbApplied };
}

/* ----- HTTP ----------------------------------------------------------------- */

function pdPost_(body) {
  var url = pdBackendUrl_() + PD_ENDPOINT;
  var options = {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(body),
    muteHttpExceptions: true,
    followRedirects: true,
    headers: {}
  };
  var token = pdAppToken_();
  if (token) options.headers['X-APP-TOKEN'] = token;
  var resp = UrlFetchApp.fetch(url, options);
  var text = resp.getContentText() || '';
  var json = null;
  try {
    json = JSON.parse(text);
  } catch (e) {}
  return { code: resp.getResponseCode(), json: json, text: text, url: url };
}

/* ----- layout builder -------------------------------------------------------- */

function buildPortfolioDecisionLayout() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(PD_SHEET);
  if (!sheet) sheet = ss.insertSheet(PD_SHEET);
  var tokens = pdTokens_(ss);
  var header = tokens.HEADER || PD_FALLBACK_TOKENS.HEADER;
  var op = tokens.OPERATOR_INPUT || PD_FALLBACK_TOKENS.OPERATOR_INPUT;

  sheet.clear();
  sheet.getRange(1, 1, sheet.getMaxRows(), sheet.getMaxColumns()).breakApart();
  if (sheet.getMaxColumns() < PD_LAST_COL) {
    sheet.insertColumnsAfter(sheet.getMaxColumns(),
        PD_LAST_COL - sheet.getMaxColumns());
  }

  var pdTitle = sheet.getRange(PD_ROW_TITLE, 1, 1, PD_LAST_COL).merge();
  pdTitle.setValue('MY PORTFOLIO \u2014 DECISION')
      .setBackground(header.bg).setFontColor(header.fg)
      .setFontWeight('bold').setFontSize(13).setHorizontalAlignment('left');
  /* v1.5.8: gold thick bottom-rule accent under the title banner. */
  pdTitle.setBorder(null, null, true, null, null, null, PD_ACCENT_GOLD,
                    SpreadsheetApp.BorderStyle.SOLID_THICK);
  sheet.getRange(PD_ROW_STATUS, 1).setValue('Status:')
      .setFontWeight('bold').setFontColor(PD_MUTED);
  sheet.getRange(PD_ROW_STATUS, 2, 1, PD_LAST_COL - 1).merge()
      .setValue('Never run \u2014 refresh My_Portfolio first, then run ' +
                'Refresh Portfolio Decision')
      .setFontColor(PD_MUTED);

  sheet.getRange(PD_ROW_PANEL_HEAD, 1, 1, PD_LAST_COL).merge()
      .setValue('CONTROL PANEL \u2014 blue cells are operator inputs (L14). ' +
                'Blanks re-seed from _Lists_Config defaults on run.')
      .setBackground(header.bg).setFontColor(header.fg).setFontWeight('bold');
  var defaults = pdPanelDefaults_(ss);
  for (var i = 0; i < PD_PANEL.length; i++) {
    var item = PD_PANEL[i];
    var pos = pdPanelPos_(i);
    sheet.getRange(pos.row, pos.labelCol).setValue(item.label)
        .setFontWeight('bold').setFontSize(9).setWrap(true)
        .setBackground(PD_TINT).setFontColor(PD_NAVY);
    var seed = defaults.hasOwnProperty(item.label) &&
               pdHasValue_(defaults[item.label]) ? defaults[item.label]
                                                 : item.def;
    var cell = sheet.getRange(pos.row, pos.valueCol);
    cell.setValue(seed).setBackground(op.bg).setFontColor(op.fg)
        .setFontWeight('bold');
    if (item.kind === 'sar') cell.setNumberFormat(PD_FMT_SAR);
    else if (item.kind === 'num') cell.setNumberFormat('0.0#');
  }
  sheet.getRange(PD_ROW_PANEL_FIRST, 1, PD_PANEL_ROWS, 8)
      .setBorder(true, true, true, true, true, true, PD_BORDER,
                 SpreadsheetApp.BorderStyle.SOLID);

  sheet.getRange(PD_ROW_KPI_HEAD, 1, 1, PD_LAST_COL).merge()
      .setValue('KPIs').setBackground(header.bg).setFontColor(header.fg)
      .setFontWeight('bold');
  sheet.getRange(PD_ROW_KPI_LABELS, 1, 1, PD_KPI_LABELS.length)
      .setValues([PD_KPI_LABELS]).setFontWeight('bold').setFontSize(9)
      .setWrap(true).setBackground(PD_TINT).setFontColor(PD_MUTED);
  var kpiVals = sheet.getRange(PD_ROW_KPI_VALUES, 1, 1, PD_KPI_LABELS.length);
  kpiVals.setValue('');
  kpiVals.setFontWeight('bold').setFontSize(11)
      .setBackground(PD_TINT).setFontColor(PD_NAVY);
  sheet.getRange(PD_ROW_KPI_LABELS, 1, 2, PD_KPI_LABELS.length)
      .setBorder(true, true, true, true, true, true, PD_BORDER,
                 SpreadsheetApp.BorderStyle.SOLID);

  sheet.setFrozenRows(PD_ROW_KPI_VALUES);
  sheet.setColumnWidths(1, PD_LAST_COL, 90);
  sheet.setColumnWidth(2, 110);
  sheet.setColumnWidth(3, 160);
  sheet.setColumnWidth(PD_LAST_COL, 380);
  Logger.log('[MP-DEC v' + PD_VERSION + '] layout built');
  return sheet;
}

/* ----- zone renderers ---------------------------------------------------------- */

function pdWriteSection_(sheet, row, title, tokens) {
  /* v1.5.8: tint band with navy text (proposal cockpit). `tokens` retained for
   * call-signature compatibility; section colour is now palette-fixed. */
  sheet.getRange(row, 1, 1, PD_LAST_COL).merge().setValue(title)
      .setBackground(PD_TINT).setFontColor(PD_NAVY)
      .setFontWeight('bold').setFontSize(11);
  return row + 1;
}

function pdWriteTable_(sheet, row, headers, dataRows, emptyText) {
  /* v1.5.8: navy column-header band with white text (was #F1F3F4). */
  sheet.getRange(row, 1, 1, headers.length).setValues([headers])
      .setFontWeight('bold').setFontSize(9).setWrap(true)
      .setFontColor('#FFFFFF').setBackground(PD_NAVY);
  row++;
  if (!dataRows.length) {
    sheet.getRange(row, 1, 1, PD_LAST_COL).merge().setValue(emptyText)
        .setFontStyle('italic').setFontColor(PD_MUTED);
    return { next: row + 1, firstDataRow: -1, count: 0 };
  }
  sheet.getRange(row, 1, dataRows.length, headers.length).setValues(dataRows)
      .setFontColor(PD_INK);
  for (var i = 0; i < dataRows.length; i += 2) {
    sheet.getRange(row + i, 1, 1, headers.length).setBackground(PD_ZEBRA);
  }
  sheet.getRange(row - 1, 1, dataRows.length + 1, headers.length)
      .setBorder(true, true, true, true, true, true, PD_BORDER,
                 SpreadsheetApp.BorderStyle.SOLID);
  return { next: row + dataRows.length, firstDataRow: row,
           count: dataRows.length };
}

function pdColorActions_(sheet, firstDataRow, count, col, tokens) {
  if (firstDataRow < 0 || !count) return;
  var vals = sheet.getRange(firstDataRow, col, count, 1).getValues();
  for (var i = 0; i < count; i++) {
    var v = String(vals[i][0] || '');
    var tok = null;
    if (v === 'ADD') tok = tokens.VERDICT_POSITIVE;
    else if (v === 'TRIM') tok = tokens.VERDICT_WATCH;
    else if (v === 'EXIT') tok = tokens.VERDICT_NEGATIVE;
    else if (v === 'BLOCK') tok = tokens.VERDICT_BLOCK;
    if (tok) {
      sheet.getRange(firstDataRow + i, col).setBackground(tok.bg)
          .setFontColor(tok.fg).setFontWeight('bold');
    }
  }
}

function pdApplyColFormats_(sheet, firstDataRow, count, fmtByCol) {
  if (firstDataRow < 0 || !count) return;
  for (var col in fmtByCol) {
    if (fmtByCol.hasOwnProperty(col)) {
      sheet.getRange(firstDataRow, parseInt(col, 10), count, 1)
          .setNumberFormat(fmtByCol[col]);
    }
  }
}

/* ----- main refresh --------------------------------------------------------------- */

function refreshPortfolioDecision() {
  var t0 = new Date().getTime();
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(PD_SHEET);
  if (!sheet || sheet.getRange(PD_ROW_PANEL_HEAD, 1).getValue() === '') {
    sheet = buildPortfolioDecisionLayout();
  }
  var tokens = pdTokens_(ss);
  var statusCell = sheet.getRange(PD_ROW_STATUS, 2);
  statusCell.setValue(pdStatusLine_('running\u2026', ''));
  SpreadsheetApp.flush();

  var panel = pdReadPanel_(sheet, ss);
  /* v1.5.11 (A): capture the My_Portfolio publication BEFORE the holdings
   * read; off = no _Status read (the v1.5.10 path). */
  var receiptMode = pdReceiptMode_();
  var sourceCtx = receiptMode === 'off' ? null : pdSrcCapture_(ss);
  var pool = pdCollectHoldings_();
  var poolNote = pool.rows.length
      ? 'holdings ' + pool.rows.length + ' rows from ' + MY_PORTFOLIO_PAGE_ +
        ' (qty/cost from cost-basis: ' + (pool.costBasisApplied || 0) + ')'
      : 'NO holdings (' + (pool.reason || 'empty') + ')';
  Logger.log('[MP-DEC v' + PD_VERSION + '] POST ' + PD_ENDPOINT + ' | ' +
             poolNote);

  var body = {
    rows: pool.rows,
    controls: panel,             // PF: labels verbatim; route maps them
    fx_rates: pdFxRates_(ss)
  };
  /* v1.5.11 (B): forward the private declared evidence when present
   * (absent -> request unchanged). Never logged. */
  var evidence = pdEvidencePacket_();
  if (evidence) body.reconciliation_evidence = evidence;

  var resp;
  try {
    resp = pdPost_(body);
  } catch (eNet) {
    statusCell.setValue(pdStatusLine_('NETWORK ERROR', String(eNet)));
    return;
  }
  if (resp.code !== 200 || !resp.json) {
    statusCell.setValue(pdStatusLine_('HTTP ' + resp.code,
        (resp.text || '').slice(0, 180)));
    return;
  }
  var payload = resp.json;
  pdRenderPayload_(sheet, payload, tokens);

  /* v1.5.9: append the just-rendered board to _Portfolio_Action_Log from
   * memory (16 v1.3.1 pattern). Never throws — failures become the note. */
  var logNote = pdAppendActionLog_(ss, payload.actions || [],
      pdMetaLine_(payload.meta), panel);

  var secs = Math.round((new Date().getTime() - t0) / 100) / 10;
  /* v1.5.11 (A): '' when off, so the line stays v1.5.10 byte-identical. */
  var receiptNote = pdReceiptNote_(receiptMode, ss, sourceCtx,
      pool.rows.length, payload);
  var statusLine = poolNote + receiptNote + ' | ' + pdMetaLine_(payload.meta) +
      ' | ' + secs + 's';
  statusCell.setValue(pdStatusLine_(String(payload.status || '?'),
      logNote ? statusLine + ' | ' + logNote : statusLine));
  Logger.log('[MP-DEC v' + PD_VERSION + '] done status=' + payload.status +
             ' in ' + secs + 's' + (logNote ? ' | ' + logNote : ''));
}

function pdRenderPayload_(sheet, payload, tokens) {
  var lastRow = sheet.getMaxRows();
  if (lastRow >= PD_ZONES_START) {
    var zone = sheet.getRange(PD_ZONES_START, 1,
                              lastRow - PD_ZONES_START + 1,
                              sheet.getMaxColumns());
    zone.breakApart();
    zone.clear();
  }

  sheet.getRange(PD_ROW_KPI_VALUES, 1, 1, PD_KPI_LABELS.length)
      .setValues([pdKpiValues_(payload.kpis)]);
  var kpiFmts = [PD_FMT_SAR, PD_FMT_SAR, PD_FMT_SAR, PD_FMT_PCT, PD_FMT_SAR,
                 PD_FMT_PCT, PD_FMT_SAR, PD_FMT_SAR, PD_FMT_SAR, '@'];
  for (var kf = 0; kf < kpiFmts.length; kf++) {
    sheet.getRange(PD_ROW_KPI_VALUES, kf + 1).setNumberFormat(kpiFmts[kf]);
  }

  var row = PD_ZONES_START;
  var actions = payload.actions || [];
  var sectors = payload.sector_summary || [];
  var alerts = payload.alerts || [];

  row = pdWriteSection_(sheet, row,
      'ACTIONS \u2014 one per holding (' + actions.length + ')', tokens);
  var aRows = [];
  for (var a = 0; a < actions.length; a++) {
    aRows.push(pdActionToRow_(actions[a]));
  }
  var aT = pdWriteTable_(sheet, row, PD_ACTION_HEADERS, aRows,
      'No holdings \u2014 refresh My_Portfolio first, or the portfolio is ' +
      'empty (an honest result, L13).');
  pdApplyColFormats_(sheet, aT.firstDataRow, aT.count, {
    6: PD_FMT_INT, 7: PD_FMT_PRICE, 8: PD_FMT_PRICE, 9: PD_FMT_PRICE,
    10: PD_FMT_SAR, 11: PD_FMT_SAR, 12: PD_FMT_SAR, 13: PD_FMT_PCT,
    14: PD_FMT_PCT, 15: PD_FMT_SAR, 16: PD_FMT_INT, 18: PD_FMT_PRICE,
    19: PD_FMT_PRICE, 20: PD_FMT_PRICE, 21: PD_FMT_PCT, 22: PD_FMT_PCT,
    23: '0.0', 24: '0.0'
  });
  pdColorActions_(sheet, aT.firstDataRow, aT.count, 1, tokens);
  if (aT.firstDataRow > 0) {
    sheet.getRange(aT.firstDataRow, PD_LAST_COL, aT.count, 1)
        .setWrap(true).setFontSize(9);
  }
  row = aT.next + 1;

  row = pdWriteSection_(sheet, row,
      'SECTOR SUMMARY (' + sectors.length + ')', tokens);
  var sRows = [];
  for (var s = 0; s < sectors.length; s++) {
    sRows.push(pdSectorToRow_(sectors[s]));
  }
  var sT = pdWriteTable_(sheet, row, PD_SECTOR_HEADERS, sRows,
      'No valued sectors.');
  pdApplyColFormats_(sheet, sT.firstDataRow, sT.count, {
    2: PD_FMT_SAR, 3: PD_FMT_PCT, 4: PD_FMT_PCT, 6: PD_FMT_INT
  });
  row = sT.next + 1;

  row = pdWriteSection_(sheet, row, 'ALERTS (' + alerts.length + ')', tokens);
  var alRows = [];
  for (var l = 0; l < alerts.length; l++) {
    alRows.push(pdAlertToRow_(alerts[l]));
  }
  var alT = pdWriteTable_(sheet, row, PD_ALERT_HEADERS, alRows,
      'None \u2014 no data or capital alerts raised.');
  row = alT.next + 1;

  sheet.getRange(row, 1, 1, PD_LAST_COL).merge()
      .setValue(pdMetaLine_(payload.meta))
      .setFontColor(PD_MUTED).setFontSize(8).setFontStyle('italic');
}

/* =========================================================================
 * v1.5.9 — PORTFOLIO ACTION AUDIT TRAIL (17_Selection_Log port, item 12)
 * The cockpit owns its own log, same pattern as 16_Decision_Top10 v1.3.1:
 * after every successful render the IN-MEMORY actions are appended to
 * _Portfolio_Action_Log — no sheet-offset re-reads, so future layout
 * changes cannot break the trail (what broke 17 twice). Never throws: any
 * failure degrades to an 'ActLog: ERROR ...' tail on the status line.
 * ========================================================================= */

var PD_ACTLOG_SHEET = '_Portfolio_Action_Log';
var PD_ACTLOG_MODE_PROP = 'PD_ACTION_LOG';        /* 'off' | 'always' | unset */
var PD_ACTLOG_HOLDS_PROP = 'PD_ACTLOG_HOLDS';     /* '1' logs HOLD rows too   */
var PD_ACTLOG_STATE_PROP = 'PD_ACTLOG_STATE';     /* JSON {date, sig}         */

/* 17's documented schema preserved: [Logged At | Run Info | Source Page] +
 * the ACTIONS grid columns VERBATIM + [Panel Snapshot (JSON) | Outcome |
 * Review Notes]. Outcome / Review Notes are blank operator columns (L14)
 * for the tracking workflow. Width follows PD_ACTION_HEADERS (26) -> 32. */
var PD_ACTLOG_HEADERS = ['Logged At', 'Run Info', 'Source Page']
    .concat(PD_ACTION_HEADERS)
    .concat(['Panel Snapshot (JSON)', 'Outcome', 'Review Notes']);

/** Effective cadence mode from ScriptProperty PD_ACTION_LOG, trimmed and
 *  lower-cased: 'off' | 'always' | '' (default: daily + change). Never
 *  throws — a Properties failure reads as the default cadence. */
function pdActLogMode_() {
  try {
    var raw = PropertiesService.getScriptProperties()
        .getProperty(PD_ACTLOG_MODE_PROP);
    return String(raw == null ? '' : raw)
        .replace(/^\s+|\s+$/g, '').toLowerCase();
  } catch (e) {
    return '';
  }
}

/** Pure: today's UTC day key ('yyyy-mm-dd') — the once-per-day cadence
 *  anchor. UTC (not script TZ) so the key is stable across TZ edits. */
function pdActLogToday_() {
  var now = new Date();
  return now.getUTCFullYear() + '-' +
      ('0' + (now.getUTCMonth() + 1)).slice(-2) + '-' +
      ('0' + now.getUTCDate()).slice(-2);
}

/** Pure: order-independent identity of the LOGGED action set — sorted
 *  'Symbol|Action' join. An action flip (e.g. TRIM -> EXIT) changes the
 *  signature, so the default cadence re-logs it same-day. */
function pdActLogSignature_(actions) {
  var src = Array.isArray(actions) ? actions : [];
  var keys = [];
  for (var i = 0; i < src.length; i++) {
    var a = src[i] || {};
    var sym = a.symbol;
    if (sym === null || sym === undefined || String(sym) === '') { continue; }
    keys.push(String(sym) + '|' + String(a.action == null ? '' : a.action));
  }
  keys.sort();
  return keys.join(',');
}

/** Pure: one PD_ACTLOG_HEADERS-width row from an action. Reuses
 *  pdActionToRow_ VERBATIM so the log mirrors the rendered ACTIONS grid
 *  field-for-field. Outcome / Review Notes stay blank (operator columns). */
function pdActLogRowFromAction_(a, loggedAt, runInfo, panelJson) {
  return [loggedAt, runInfo, PD_SHEET]
      .concat(pdActionToRow_(a || {}))
      .concat([panelJson, '', '']);
}

function pdActLogStateLoad_() {
  var raw = '';
  try {
    raw = PropertiesService.getScriptProperties()
        .getProperty(PD_ACTLOG_STATE_PROP) || '';
  } catch (e) {}
  var st = { date: '', sig: '' };
  if (raw) {
    try {
      var parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object') {
        st.date = String(parsed.date || '');
        st.sig = String(parsed.sig || '');
      }
    } catch (e2) {}
  }
  return st;
}

function pdActLogStateSave_(dateKey, sig) {
  try {
    PropertiesService.getScriptProperties().setProperty(
        PD_ACTLOG_STATE_PROP, JSON.stringify({ date: dateKey, sig: sig }));
  } catch (e) {}
}

/** Resolve/create the log sheet; self-heal the header.
 *  Fresh sheet -> full current header. A legacy 17-era sheet (25-col grid,
 *  no 'Engine ROI %') gets ONE column INSERTED right after the 'ROI %'
 *  header column and titled — historical rows keep their alignment and
 *  their new cell is honestly blank (engine ROI was not logged then).
 *  Blank header cells are stamped; non-blank cells are never overwritten. */
function pdActLogSheet_(ss) {
  var want = PD_ACTLOG_HEADERS.length;
  var sh = ss.getSheetByName(PD_ACTLOG_SHEET);
  if (!sh) {
    sh = ss.insertSheet(PD_ACTLOG_SHEET);
    sh.getRange(1, 1, 1, want).setValues([PD_ACTLOG_HEADERS])
        .setFontWeight('bold');
    sh.setFrozenRows(1);
    return sh;
  }
  var lastCol = Math.max(1, sh.getLastColumn());
  var hdr = sh.getRange(1, 1, 1, lastCol).getValues()[0];
  var first = String(hdr[0] == null ? '' : hdr[0]).replace(/^\s+|\s+$/g, '');
  if (!first) {
    if (sh.getMaxColumns() < want) {
      sh.insertColumnsAfter(sh.getMaxColumns(), want - sh.getMaxColumns());
    }
    sh.getRange(1, 1, 1, want).setValues([PD_ACTLOG_HEADERS])
        .setFontWeight('bold');
    return sh;
  }
  var hasEngine = false;
  var roiCol = 0;
  for (var i = 0; i < hdr.length; i++) {
    var h = String(hdr[i] == null ? '' : hdr[i]).replace(/^\s+|\s+$/g, '');
    if (h === 'Engine ROI %') { hasEngine = true; }
    if (h === 'ROI %' && !roiCol) { roiCol = i + 1; }
  }
  if (!hasEngine && roiCol) {
    sh.insertColumnAfter(roiCol);
    sh.getRange(1, roiCol + 1).setValue('Engine ROI %').setFontWeight('bold');
  }
  if (sh.getMaxColumns() < want) {
    sh.insertColumnsAfter(sh.getMaxColumns(), want - sh.getMaxColumns());
  }
  var hdr2 = sh.getRange(1, 1, 1, want).getValues()[0];
  for (var j = 0; j < want; j++) {
    if (String(hdr2[j] == null ? '' : hdr2[j])
        .replace(/^\s+|\s+$/g, '') === '') {
      sh.getRange(1, j + 1).setValue(PD_ACTLOG_HEADERS[j])
          .setFontWeight('bold');
    }
  }
  return sh;
}

/**
 * Append the rendered ACTIONS to _Portfolio_Action_Log. Modes (ScriptProperty
 * PD_ACTION_LOG): 'off' kill-switch; 'always' every run; unset = DEFAULT:
 * once per UTC day PLUS any run whose logged action set changed (a new
 * decision or an action flip re-logs same-day; identical re-runs do not
 * flood the trail). HOLD rows are skipped by default; ScriptProperty
 * PD_ACTLOG_HOLDS='1' captures them too. State (day + signature) is saved
 * ONLY when rows are actually appended, so a skipped/empty run can never
 * mask a later real change. NEVER THROWS — every failure becomes the
 * returned status-line note.
 */
function pdAppendActionLog_(ss, actions, runInfo, panel) {
  try {
    var mode = pdActLogMode_();
    if (mode === 'off') { return 'ActLog: off'; }

    var includeHolds = false;
    try {
      includeHolds = myPortfolioBoolean_(
          String(PropertiesService.getScriptProperties()
              .getProperty(PD_ACTLOG_HOLDS_PROP) || ''), false);
    } catch (eP) { includeHolds = false; }

    var src = Array.isArray(actions) ? actions : [];
    var logged = [];
    var holdSkipped = 0;
    for (var fi = 0; fi < src.length; fi++) {
      var fa = src[fi];
      if (!fa) { continue; }
      var fSym = fa.symbol;
      if (fSym === null || fSym === undefined ||
          String(fSym).replace(/^\s+|\s+$/g, '') === '') { continue; }
      var fAct = String(fa.action == null ? '' : fa.action)
          .replace(/^\s+|\s+$/g, '').toUpperCase();
      if (!includeHolds && fAct === 'HOLD') {
        holdSkipped = holdSkipped + 1;
        continue;
      }
      logged.push(fa);
    }
    if (!logged.length) {
      return holdSkipped
          ? 'ActLog: 0 to log (' + holdSkipped + ' HOLD skipped)'
          : 'ActLog: nothing to log';
    }

    var sig = pdActLogSignature_(logged);
    var now = new Date();
    var dateKey = pdActLogToday_();
    if (mode !== 'always') {
      var st = pdActLogStateLoad_();
      if (st.date === dateKey && st.sig === sig) {
        return 'ActLog: unchanged today (skipped)';
      }
    }

    var loggedAt = '';
    try {
      loggedAt = Utilities.formatDate(now,
          Session.getScriptTimeZone() || 'Asia/Riyadh', 'yyyy-MM-dd HH:mm:ss');
    } catch (eT) {
      loggedAt = String(now);
    }
    var panelJson = '';
    try { panelJson = JSON.stringify(panel || {}); } catch (eJ) { panelJson = ''; }

    var rows = [];
    for (var i = 0; i < logged.length; i++) {
      rows.push(pdActLogRowFromAction_(logged[i], loggedAt,
          String(runInfo == null ? '' : runInfo), panelJson));
    }

    var sh = pdActLogSheet_(ss);
    sh.getRange(sh.getLastRow() + 1, 1, rows.length,
        PD_ACTLOG_HEADERS.length).setValues(rows);
    pdActLogStateSave_(dateKey, sig);
    return 'ActLog: +' + rows.length +
        (holdSkipped ? ' (' + holdSkipped + ' HOLD skipped)' : '');
  } catch (e) {
    return 'ActLog: ERROR ' + (e && e.message ? e.message : String(e));
  }
}

/* ----- self-test --------------------------------------------------------------------- */

function pdSelfTest() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var report = [];
  report.push('[MP-DEC v' + PD_VERSION + '] self-test');
  report.push('backend: ' + pdBackendUrl_());
  report.push('token set: ' + (pdAppToken_() ? 'yes' : 'NO'));
  var fx = pdFxRates_(ss);
  var fxCount = 0;
  for (var k in fx) {
    if (fx.hasOwnProperty(k)) fxCount++;
  }
  report.push('fx rates from TFB_FX_LOOKUP: ' + fxCount);
  var defs = pdPanelDefaults_(ss);
  var pfDefs = 0;
  for (var d in defs) {
    if (defs.hasOwnProperty(d) && d.indexOf('PF: ') === 0) pfDefs++;
  }
  report.push('PF panel defaults found: ' + pfDefs + ' (expect 7)');
  var pool = pdCollectHoldings_();
  report.push('holdings sample: ' + pool.rows.length + ' rows' +
              (pool.reason ? ' (' + pool.reason + ')' : ''));
  if (pool.rows.length) {
    var first = pool.rows[0];
    report.push('first row Quantity=' + first['Quantity'] +
                ' BuyPrice=' + first['Buy Price'] +
                ' AvgCost=' + first['Avg Cost'] +
                ' keys=' + JSON.stringify(first).slice(0, 260));
  }
  /* v1.5.9: ledger map + action log dry checks (read-only). */
  var lm = _myPortfolioLedgerMap_();
  var lmN = 0;
  for (var lk in lm) { if (lm.hasOwnProperty(lk)) lmN++; }
  report.push('ledger map (ACTIVE cost basis): ' + lmN + ' symbol(s)');
  var alRow = pdActLogRowFromAction_({ action: 'ADD', symbol: 'TEST' },
      'stamp', 'run info', '{}');
  report.push('action log: headers=' + PD_ACTLOG_HEADERS.length +
              ' rowCells=' + alRow.length + ' mode=' +
              (pdActLogMode_() || 'daily+change'));
  /* v1.5.11: receipt mode, current publication verdict, evidence presence
   * (read-only; the evidence packet itself is never printed). */
  report.push('source receipt mode (PD_SOURCE_RECEIPT): ' + pdReceiptMode_());
  var srcNow = pdSrcReadiness_(ss, null, null);
  report.push('My_Portfolio publication: ' + (srcNow.ready
      ? 'ready, run ' + srcNow.cohort : 'not ready: ' + srcNow.reason));
  report.push('reconciliation evidence property: ' +
      (pdEvidencePacket_() ? 'present (schema 1)' : 'absent'));
  var msg = report.join('\n');
  Logger.log(msg);
  try {
    SpreadsheetApp.getUi().alert(msg);
  } catch (e) {}
  return msg;
}