/**
 * ============================================================================
 * 21_Portfolio_Ledger.gs — _Portfolio_CostBasis INVESTMENT LEDGER (v2 page)
 * Version: 1.2.1   (TFB Final Execution Plan v5.0 — Phase P7, ledger rebuild)
 * Runtime: ES5 ONLY (V8 exceptions are 01_Menu.gs / 03_Schema.gs only).
 * ============================================================================
 *
 * v1.2.1 — FLAG HYGIENE + PERSISTENT NUMBER FORMATS (2026-08-22)
 * ------------------------------------------------------------------------------
 * WHY (2026-08-22 morning audit, ChatGPT cross-audit adjudicated):
 *   (1) T82U.SI finalized to Inactive but its Notes cell still read the
 *       v1.2.0 ⛔ CONTRADICTION flag. Root cause IN CODE: plClearNoteFlag_
 *       strips only \u26a0 segments — the \u26d4 flag introduced by v1.2.0
 *       is invisible to the very cleaner that should retire it, and the
 *       frozen branch never calls the cleaner at all. FIX: the cleaner now
 *       strips BOTH \u26a0 and \u26d4 segments (operator text preserved,
 *       unconditional by design so stale flags heal even with the guard
 *       kill-switched off), and the frozen branch calls it — the ONLY
 *       thing it may touch on a frozen row is a stale flag; computed
 *       cells remain untouched.
 *   (2) Closed rows displayed Return % as raw fractions (0.02257…) while
 *       active rows showed 0.0% format. plApplyFormats_ already defines
 *       RET='0.0%' but is only invoked by the one-time layout builder.
 *       FIX: refreshPortfolioLedger now re-applies plApplyFormats_ once
 *       per refresh (idempotent, reuses the existing map — no duplicate
 *       format logic).
 *   NOT CHANGED: contradiction predicate, feeder exclusions, kill-switch,
 *   computed-cell writes, ES5-only, never-throws.
 *
 * v1.2.0 — CONTRADICTION GUARD: Active-with-Sell-fields FAIL-CLOSED (2026-08-21)
 * ------------------------------------------------------------------------------
 * WHY (six-gate audit P0-1, 2026-08-21 16:30 TSVs, adjudicated):
 *   T82U.SI sat with Status=Active AND Sell Date=2026-08-21 AND Sell
 *   Price=1.54. plIsActive_ tests STATUS ALONE, so the lot (a) fed
 *   My_Portfolio as a normal 3,800-share holding via plAggregateActive_,
 *   (b) received live MV 17,431 / Unrealized +495 from plComputeActive_,
 *   and (c) tripped NO flag — the amber system covers the INVERSE state
 *   only (Inactive missing sell fields). Broker cash independently showed
 *   the sale proceeds (+4,590 USD vs 4,606.63 expected): the decision
 *   surfaces were double-counting a sold position as cash AND stock, and
 *   Portfolio_Decision emitted TRIM on it. FAIL-CLOSED FIX:
 *   - plContradiction_(lot): Active AND (Sell Date OR Sell Price>0).
 *   - Refresh loop: a contradictory row keeps factual Cost Basis, gets
 *     DASHES in the 9 live computed cells (no phantom MV/P&L), a precise
 *     ⛔ note flag, amber paint, and its own status-line counter.
 *   - BOTH feeders (plAggregateActive_ / plGroupActiveLots_) EXCLUDE it:
 *     per the layout contract My_Portfolio reads ACTIVE only, and a
 *     contradictory lot is not cleanly ACTIVE. Loud, never silent.
 *   - Diagnostics lot-count line gains the contradiction bucket.
 *   KILL-SWITCH: script property PL_CONTRA_GUARD='off' restores v1.1.0
 *   byte-path-identically (predicate never consulted). ES5 ONLY. NEVER-
 *   THROWS discipline unchanged.
 *
 * v1.1.0 — INPUT VALIDATION + DIVIDEND-FETCH DIAGNOSTICS (2026-07-12)
 * ------------------------------------------------------------------------------
 * WHY (audit evidence, 2026-07-12 morning six-gate audit, v38 export):
 *   1. FER.US Sell Price held the literal text '64ز73' — the Arabic
 *      keyboard maps the physical '.' key to ز, so '64.73' typed in
 *      Arabic layout lands as '64ز73'. plNum_ -> NaN -> null ->
 *      plPendingClose_ stayed TRUE forever: the sale could never finalize,
 *      the status line said '1 pending', and the generic 'sell fields
 *      required' flag pointed at a cell that LOOKED filled. Same class:
 *      Arabic-Indic digits (٠-٩ / ۰-۹), the Arabic decimal
 *      separator ٫ and thousands separator ٬.
 *   2. NMM.US Ccy was blank: the ACTIVE branch computed rate=null and
 *      wrote dashes into FX→SAR / Market Value / Unrealized — SILENTLY.
 *      The fxMissing flag only fired 'if (rate === null && ccy)', so a
 *      blank ccy produced no flag, no amber, no status mention.
 *   3. Dividend auto-fetch failed EVERY run for RCI.US / BBD.US / NMM.US
 *      with a bare 'fail RCI.US,BBD.US,NMM.US' — no HTTP code, no Yahoo
 *      error body, nothing to debug with. (Working hypothesis: Yahoo
 *      rate-limits Google-egress IPs — UrlFetchApp — with 429; the new
 *      diagnostics will confirm or refute it from the status line.)
 *
 * FIXES (a) — input validation & normalization:
 *   - plNormalizeNumericText_: transliterates Arabic-Indic and Extended
 *     (Persian) digits to ASCII, maps ٫ -> '.', drops ٬, converts the
 *     digit-ز-digit period-key typo to a decimal point ('64ز73' ->
 *     64.73), and strips STRICT ASCII thousands grouping
 *     (1,234,567.89). Anything still non-numeric stays null — nothing is
 *     guessed ('6ز4ز7' -> NaN -> flagged, never fabricated).
 *   - plReadLot_ parses the five operator NUMERIC INPUT cells (Buy Price,
 *     Shares, Buy Fees, Sell Price, Sell Fees) through the normalizer and
 *     reports fixes[] (auto-corrected) and bad[] (unrecoverable) per lot.
 *   - refreshPortfolioLedger WRITES each auto-corrected value back into
 *     its cell (visible, logged '[PL] INPUT_FIX row R col ...'), so the
 *     cell and the computation can never diverge; the FER.US sale
 *     finalizes on the very next refresh with zero manual edits.
 *     Unrecoverable text gets a PRECISE amber flag naming the column and
 *     the offending text ('⚠ Sell Price ("abc") not numeric — fix to
 *     finalize') instead of the generic 'sell fields required'.
 *   - CCY REQUIRED GATE: suffix inference widened via PL_CCY_BY_SUFFIX
 *     (SR→SAR, US→USD, TO→CAD, NS→INR, IS→TRY, MC→EUR — .L is
 *     DELIBERATELY excluded: LSE quotes in pence, a wrong inference is
 *     worse than a flag). NMM.US therefore self-heals to USD on the next
 *     refresh. An ACTIVE row whose ccy is still blank is now flagged
 *     '⚠ Ccy required' + amber + a 'ccy required:' status bit; a
 *     finalize with blank ccy flags '⚠ Ccy required — cannot finalize'
 *     (distinct from a genuine FX-rate gap for a known ccy).
 *   - plClearNoteFlag_ now strips ALL ⚠ segments (split on ' · '),
 *     so multi-flag rows clean up completely once healthy.
 *   KILL-SWITCH: PL_INPUT_NORMALIZE='off' restores exact v1.0.0 parsing
 *   (no auto-fix writes, no bad-cell flags; plNum_ semantics verbatim).
 *
 * FIXES (b) — dividend fetch:
 *   - Fetch is now grouped PER SYMBOL (one call per symbol from the
 *     earliest active buy date; events distributed per lot by ex-date),
 *     with a 400 ms courtesy sleep between symbols — multi-lot symbols
 *     no longer double-fetch and the burst pattern that invites 429s is
 *     gone.
 *   - interval=1d -> 1mo: dividend EVENTS are interval-independent; the
 *     daily-bar payload over multi-year lookbacks was the heaviest
 *     possible variant for zero benefit.
 *   - plFetchDividends_ now returns {ok, list, code, err}: HTTP code and
 *     Yahoo chart.error.description (plParseDividends_ surfaces it) are
 *     captured; host1 -> 350 ms -> host2 -> (on 429/5xx only) 1200 ms ->
 *     host1 retry. Failures land in the status line as 'SYM(429)' /
 *     'SYM(404:No data found...)' and in a '[PL] DIV_FETCH FAIL' log
 *     line — debuggable at a glance.
 *   - plDivDiag(): menu-callable read-only probe — fetches ONE symbol
 *     verbosely (per-host HTTP code + error + event count) to confirm
 *     the 429 hypothesis live. If 429 persists from Google egress IPs,
 *     the durable fix is routing dividends through the TFB backend
 *     (which already talks to Yahoo successfully from Render) — out of
 *     scope here, decision surfaced honestly.
 *   - Failure containment unchanged and verified: Dividends Recv is
 *     recomputed from the _Dividend_Log alone; a failed fetch only means
 *     no NEW rows — it can never zero or reduce an existing total.
 * Everything else byte-identical to v1.0.0: freeze mechanics, feeder,
 * layout, manual dividend, never-throws contract all carried verbatim.
 *
 * v1.0.0 — LEDGER AS SOURCE OF TRUTH (operator spec, 2026-07-10)
 * ------------------------------------------------------------------------------
 * OPERATOR SPEC (Emad): _Portfolio_CostBasis becomes the permanent log of ALL
 * investments — buys and sells, active and closed — so every symbol's gain or
 * loss history (dividends included) is reconstructable. Requirements locked:
 *   1. One row = one lot. Columns: purchase date/price/fees, sell
 *      date/price/fees, shares, accumulated dividends, realized gain,
 *      unrealized gain/loss, status.
 *   2. STATUS is a dropdown (Active / Inactive). Flipping to Inactive FREEZES
 *      the row: gain/loss calculation stops, dividends stop accumulating,
 *      realized G/L (incl. dividends) is computed ONCE and never touched.
 *   3. DIVIDENDS ARE AUTOMATIC: per active lot, dividend events are pulled
 *      from Yahoo Chart (events=div) since the lot's buy date, every event is
 *      appended to a _Dividend_Log audit sheet (dedup key symbol|buyDate|
 *      exDate — idempotent, never double-counts), and the ledger's
 *      "Dividends Recv" cell is recomputed as the SUM of that lot's log rows.
 *      Inactive rows are never re-synced. Coverage honesty: Yahoo's dividend
 *      feed can lag or miss (esp. some .SR names) — plAddManualDividend()
 *      appends a MANUAL log row that flows into the same total.
 *   4. FEEDER RULE: My_Portfolio (and therefore Portfolio_Decision and the
 *      daily brief) reads ACTIVE lots only. syncLedgerToPortfolio()
 *      aggregates active lots per symbol (total shares, weighted avg cost)
 *      and upserts My_Portfolio's input columns — located BY HEADER NAME
 *      ('Symbol', 'Position Qty', 'Avg Cost', 'Position Cost'), never by
 *      index — and DELETES rows whose symbol has no active lot left.
 *
 * FREEZE MECHANICS — VALUES, NOT FORMULAS (deliberate)
 * ------------------------------------------------------------------------------
 * All computed columns are SCRIPT-WRITTEN VALUES. refreshPortfolioLedger()
 * touches Active rows only; an Inactive row is finalized exactly once (FX
 * stamped at close, realized computed from stored inputs + frozen dividends)
 * and then skipped forever — detected by a numeric Realized cell. A formula
 * cannot un-freeze it and a page refresh cannot drift it. An Inactive row
 * missing Sell Date/Price is NOT finalized: it is painted amber and flagged
 * "sell fields required" until the operator completes it (L13 — nothing is
 * fabricated).
 *
 * SUPERSESSION
 * ------------------------------------------------------------------------------
 * This file supersedes the legacy syncPortfolioLedger flow. After this file
 * is live, DO NOT run the old syncPortfolioLedger — the ledger now owns the
 * My_Portfolio holdings block through syncLedgerToPortfolio() (also called
 * automatically at the start of every refreshPortfolioLedger run). Remove
 * the old menu item when 10_My_Portfolio.gs is next revised.
 *
 * SHEET CONTRACTS (verified against the 2026-07-09 production export)
 * ------------------------------------------------------------------------------
 *   My_Portfolio: header row 1; data rows 2+. Feeder columns located by
 *     name: 'Symbol' (1), 'Position Qty' (76), 'Avg Cost' (77),
 *     'Position Cost' (78) — indexes shown for reference only; lookup is by
 *     header text so schema inserts cannot break the sync. 'Name' /
 *     'Currency' / 'Current Price' are read for enrichment of ledger rows.
 *   _Lists_Config: FX via the TFB_FX_LOOKUP named range — col 1 CCY,
 *     col 4 'Effective → SAR' (fallback col 2 live, col 3 static).
 *   _Portfolio_CostBasis v1 seed (Symbol | Buy Date | Buy Price | Quantity)
 *     is auto-migrated into the v2 layout as Active lots on first build.
 *
 * PAGE LAYOUT (built/repaired by buildPortfolioLedgerLayout)
 * ------------------------------------------------------------------------------
 *   Row 1 title banner · Row 2 status line (B2) · Row 3 legend ·
 *   Row 4 header (23 cols) · Rows 5+ data.
 *   Columns: A Symbol | B Name | C Ccy | D Status(dropdown) | E Buy Date |
 *   F Buy Price | G Shares | H Buy Fees | I Sell Date | J Sell Price |
 *   K Sell Fees | L Dividends Recv (script) | M Notes | N Cost Basis |
 *   O Current Price | P FX→SAR | Q Market Value SAR | R Unrealized G/L SAR |
 *   S Sell Proceeds | T Realized G/L SAR | U Total Return SAR | V Return % |
 *   W Holding Days.
 *   BLUE = operator inputs (A–K, M) · GRAY = script-computed (L, N–W) ·
 *   AMBER = Inactive pending sell fields.
 *
 * ENTRY POINTS (wire into 01_Menu.gs; no onOpen here)
 *   buildPortfolioLedgerLayout() — one-time / repair; migrates the v1 seed
 *   refreshPortfolioLedger()     — main run: sync feeder → prices/FX →
 *                                  auto-dividends → compute → finalize
 *   syncLedgerToPortfolio()      — feeder only (Active lots → My_Portfolio)
 *   plAddManualDividend()        — prompt-driven MANUAL dividend log row
 *   plDivDiag()                  — verbose one-symbol dividend fetch probe
 *   plRefinalizeRow()            — v1.4.1: redo the freeze of ONE closed lot
 *   plSelfTest()                 — read-only config / contract checks
 *
 * KILL-SWITCHES (ScriptProperties)
 *   PL_FX_PEG_GUARD = 'off'   → v1.4.1 peg guard bypassed (v1.4.0 FX path)
 *   PL_LEDGER_DISABLED = '1'  → refresh & sync become no-ops (status notes it)
 *   PL_DIV_SYNC = 'off'       → auto-dividend fetch skipped; existing
 *                               Dividends Recv values are KEPT, never zeroed
 *   PL_INPUT_NORMALIZE = 'off' → v1.0.0 numeric parsing exactly (no
 *                               Arabic-input auto-fix, no bad-cell flags)
 *
 * LOGGER TAG: [PL v1.0.0]. NEVER-THROWS: refresh/sync/dividend paths degrade
 * to a status-line note + Logger line; per-symbol fetch failures are counted,
 * named, and skipped.
 * ============================================================================
 */

var PL_VERSION = '1.5.0';
/* v1.5.0 (2026-09-13) — SCRIPT-OWNED LEDGER TOTALS (P-135)
 * FORENSIC WHY (2026-09-13 audit): status-row cells T2/U2 read
 * 2,236 / 3,174 while the true column sums measure 1,075 / 2,012
 * (Δ≈+1,161 in both — the orphans embed a stale snapshot). Cross-checked
 * by three parsers on two same-day exports, per-cell identical; frozen
 * closed-row cells byte-stable across exports as designed. (An interim
 * figure of 1,157 in the morning audit was a measurement-script bug —
 * itself an argument for script-owned totals.) Root cause of the
 * orphans: NO code path in this file has ever
 * written those cells — no SUM, no setFormula; the layout writes only
 * Title/Status/Legend/Headers — so the numbers were orphaned MANUAL
 * entries that drifted as rows changed and nothing maintained them.
 * FIX: the script now OWNS the totals. After each refresh, one batched
 * grid read feeds plTotalsFromGrid_ (pure, node-tested): Active rows sum
 * Unrealized + Total Return; finalized Inactive rows sum the FROZEN
 * Realized cells (finalize-once truth is READ, never recomputed);
 * lifetime = closed realized + active total return. Written to S2
 * (label), T2 (realized closed), U2 (lifetime); the active total goes to
 * the status line. First run REPLACES the orphaned manual values by
 * design. Kill switch PL_TOTALS='off' restores the v1.4.1 byte-path (no
 * read, no writes, no status bit). Functions added: 2
 * (plTotalsFromGrid_, plTotalsOn_). Removed: 0. */
/* v1.4.1 (2026-09-06) — FX PEG GUARD AT FREEZE + SELL-FEE VISIBILITY +
 * RE-FINALIZE ENTRY POINT
 * FORENSIC WHY (2026-09-06 morning six-gate audit, _Portfolio_CostBasis
 * export + IBKR live): SHG.US closed 2026-09-03 at 84.34 froze with
 * FX→SAR = 3.8090 and Realized 2,650 SAR. The frozen rate came from the
 * row's own FX cell, written during the ACTIVE phase from the _Lists_Config
 * USD cell of that moment (3.8090, 1.5% off the SAMA band; corrected to
 * 3.7528 by Run 2 on 09-05). Second gap: Sell Fees was 0, so Sell Proceeds
 * carried the gross 5,903.80 while the broker realized +693.32 USD net of
 * a 2.48 commission. Broker-true realized ≈ 2,602 SAR; the ledger overstated
 * by ≈48 SAR (39 FX + 9 commission). The freeze design (compute once, never
 * touch) is correct and kept; what was missing is (a) a sanity guard on the
 * rate being frozen and (b) a sanctioned way to redo a freeze.
 * CHANGES (ES5, never-throws, kill-switchable):
 *   (1) plFxGuard_(ccy, rate, on) — mirrors opportunity_builder v1.19.3
 *       _fx_peg_guard: USD band [3.74, 3.77], static peg 3.75. Applied in
 *       the ACTIVE branch (table rate) and in the FINALIZE branch to BOTH the
 *       reused FX cell and the table rate. An out-of-band cell value is not
 *       reused (falls through to the guarded table rate). Replacements are
 *       counted per currency and named in the status line
 *       ('fx peg-guard: USD 3.809→3.7528').
 *   (2) Finalize with Sell Fees = 0 writes an INFORMATIONAL note segment
 *       (ⓘ prefix; the ⚠/⛔ cleaner ignores it; re-finalize with fees > 0
 *       removes it) and a status bit 'sell fees 0 at finalize: N'.
 *   (3) plRefinalizeRow() (menu-callable) — for ONE Inactive row: clears
 *       FX→SAR + Realized, then runs the refresh, which re-finalizes the
 *       row exactly once with the guarded table rate and the current Sell
 *       Fees / frozen dividends. Operator path for SHG.US: enter Sell Fees
 *       2.48, run plRefinalizeRow on its row → FX 3.7528, Realized ≈2,602.
 *   KILL-SWITCH: script property PL_FX_PEG_GUARD='off' restores v1.4.0
 *   byte-path-identically for (1); (2) and (3) are additive.
 *   NOT CHANGED: freeze-once semantics, feeder, dividends, contradiction
 *   guard, input normalizer, layout, formats. Functions added: 4
 *   (plFxPegOn_, plFxGuard_, plClearInfoFlag_, plRefinalizeCore_) + 1 entry
 *   point (plRefinalizeRow). Removed: 0. */
/* v1.4.0 (2026-09-01) — YAHOO SYMBOL MAPPING FOR DIVIDENDS
 * FORENSIC WHY (ledger status line 2026-08-31): "div: +0 event(s), fail
 * SHG.US(404:No data found, symbol may be delisted), EPRT.US(404:...)".
 * plFetchDividends_ placed the TFB symbol verbatim in Yahoo's chart path;
 * Yahoo addresses US equities by BARE ticker (SHG, EPRT), so every '.US'
 * lot failed with Yahoo's "may be delisted" 404 while unsuffixed OTIS/SBAC/
 * PFS auto-logged correctly (24.20 / 26.25 / 1.20). Same class as the
 * ARCO.US / UVV.US / PFLT.US failures of 2026-08-07. Native suffixes Yahoo
 * understands (.SR .SI .KL .T .HK .L .PA .DE ...) are passed through.
 * CHANGE: plYahooSymbol_(sym) strips a trailing '.US' (case-insensitive)
 * before BOTH Yahoo chart calls (auto-dividends and the menu probe);
 * nothing else moves. */
/* v1.3.0 (2026-08-28) - PRUNE CAP + BUY DATE/NAME SEEDING.
 * FORENSIC WHY: (1) the prune guard was only activeCount>0, so a PARTIAL
 * ledger read (parsed some rows, missed others) could silently delete live
 * My_Portfolio positions. New cap: at most TFB_PL_PRUNE_MAX rows (default
 * 2) pruned per run; larger prunes are SKIPPED LOUDLY as a partial-read
 * suspect. (2) Buy Date was never written to My_Portfolio (5 of 6 blank in
 * every export; 5023.SR stale at 2025-01-10) and Name was not seeded on
 * insert. Aggregate now carries the EARLIEST lot Buy Date per symbol;
 * inserts write Buy Date + Name; updates fill Buy Date only when the cell
 * is blank (the page refresher owns Name on existing rows - refresher-side
 * preservation of Buy Date is the durable half, queued separately).
 * Functions added: 0. Removed: 0. */
var PL_SHEET = '_Portfolio_CostBasis';
var PL_DIVLOG_SHEET = '_Dividend_Log';
var PL_MP_SHEET = 'My_Portfolio';
var PL_FX_RANGE = 'TFB_FX_LOOKUP';

var PL_ROW_TITLE = 1;
var PL_ROW_STATUS = 2;
var PL_ROW_LEGEND = 3;
var PL_ROW_HEADER = 4;
var PL_ROW_FIRST = 5;
var PL_MAX_LOTS = 500;

var PL_HEADERS = ['Symbol', 'Name', 'Ccy', 'Status', 'Buy Date', 'Buy Price',
  'Shares', 'Buy Fees', 'Sell Date', 'Sell Price', 'Sell Fees',
  'Dividends Recv', 'Notes', 'Cost Basis', 'Current Price', 'FX\u2192SAR',
  'Market Value SAR', 'Unrealized G/L SAR', 'Sell Proceeds',
  'Realized G/L SAR', 'Total Return SAR', 'Return %', 'Holding Days'];

/* 1-based column indexes (match PL_HEADERS order). */
var PLC = { SYM: 1, NAME: 2, CCY: 3, STATUS: 4, BDATE: 5, BPX: 6, SH: 7,
  BFEE: 8, SDATE: 9, SPX: 10, SFEE: 11, DIV: 12, NOTE: 13, CB: 14, CUR: 15,
  FX: 16, MV: 17, UNRL: 18, PROC: 19, REAL: 20, TOT: 21, RET: 22, DAYS: 23 };

var PL_DIVLOG_HEADERS = ['Logged At', 'Type', 'Symbol', 'Lot Buy Date',
  'Ex-Date', 'Amount/Share', 'Shares', 'Amount', 'Ccy', 'Note'];

var PL_STATUS_ACTIVE = 'Active';
var PL_STATUS_INACTIVE = 'Inactive';
var PL_DASH = '\u2014';
var PL_PENDING_FLAG = '\u26a0 sell fields required \u2014 not finalized';

var PL_PROP_DISABLED = 'PL_LEDGER_DISABLED';
var PL_PROP_DIV_MODE = 'PL_DIV_SYNC';
/* v1.1.0: 'off' restores exact v1.0.0 numeric parsing (no auto-fix). */
var PL_PROP_INPUT_NORM = 'PL_INPUT_NORMALIZE';
/* v1.4.1: FX peg guard — mirrors opportunity_builder v1.19.3 _fx_peg_guard.
 * 'off' restores v1.4.0 byte-path-identically. */
var PL_PROP_FX_PEG_GUARD = 'PL_FX_PEG_GUARD';
/* v1.5.0: 'off' restores v1.4.1 exactly (no totals read/write/bit). */
var PL_PROP_TOTALS = 'PL_TOTALS';
var PL_FX_PEG_BAND = { USD: [3.74, 3.77], SAR: [1, 1] };
var PL_FX_PEG_STATIC = { USD: 3.75, SAR: 1 };
var PL_INFO_FEES_FLAG = '\u24d8 Sell Fees 0 at finalize \u2014 broker sell ' +
    'commission not netted (enter Sell Fees, then plRefinalizeRow)';

/* v1.1.0: certain suffix->currency inferences ONLY. '.L' is deliberately
 * absent — LSE quotes in pence (GBp); a wrong inference silently corrupts
 * FX math, a flag does not. */
var PL_CCY_BY_SUFFIX = { SR: 'SAR', US: 'USD', TO: 'CAD', NS: 'INR',
  IS: 'TRY', MC: 'EUR' };

/* v1.1.0: the five operator NUMERIC INPUT columns (validated/normalized).
 * Script-written numerics (Div/FX/Real...) keep plain plNum_ parsing. */
var PL_NUM_INPUT_COLS = [
  { col: 6, name: 'Buy Price', side: 'buy' },
  { col: 7, name: 'Shares', side: 'buy' },
  { col: 8, name: 'Buy Fees', side: 'buy' },
  { col: 10, name: 'Sell Price', side: 'sell' },
  { col: 11, name: 'Sell Fees', side: 'sell' }
];

var PL_YH_HOSTS = ['https://query1.finance.yahoo.com',
                   'https://query2.finance.yahoo.com'];

var PL_NAVY = '#1F3A5F';
var PL_TINT = '#EEF2F7';
var PL_BLUE = '#E8F0FE';
var PL_GRAY = '#F1F3F6';
var PL_AMBER = '#FFF3CD';
var PL_MUTED = '#6B7280';
var PL_BORDER = '#D6DEE8';

var PL_MP_COL_SYMBOL = 'Symbol';
var PL_MP_COL_QTY = 'Position Qty';
var PL_MP_COL_AVG = 'Avg Cost';
var PL_MP_COL_COST = 'Position Cost';
var PL_MP_COL_NAME = 'Name';
var PL_MP_COL_CCY = 'Currency';
var PL_MP_COL_PRICE = 'Current Price';

// ---------------------------------------------------------------------------
// Pure helpers (node-testable: no Apps Script services touched)
// ---------------------------------------------------------------------------

function plTrim_(v) {
  return String(v === null || v === undefined ? '' : v)
      .replace(/^\s+|\s+$/g, '');
}

function plNum_(v) {
  if (v === null || v === undefined || v === '') { return null; }
  var n = Number(v);
  return isNaN(n) ? null : n;
}

function plIsDate_(v) {
  return Object.prototype.toString.call(v) === '[object Date]' &&
         !isNaN(v.getTime());
}

/** v1.5.0 (P-135): totals over the raw data grid (rows in PL column
 * order). Active rows sum UNRL + TOT; finalized Inactive rows sum the
 * frozen REAL cell. Rows with a blank Symbol are skipped; non-numeric
 * cells skip per-cell (plNum_ null-guard). Pure — node-testable. */
function plTotalsFromGrid_(vals) {
  var out = { realizedClosed: 0, activeUnrl: 0, activeTot: 0,
              lifetime: 0, nClosed: 0, nActive: 0 };
  for (var i = 0; i < vals.length; i++) {
    var row = vals[i];
    if (!row || !plTrim_(row[PLC.SYM - 1])) { continue; }
    var st = plTrim_(row[PLC.STATUS - 1]);
    if (st === PL_STATUS_ACTIVE) {
      var u = plNum_(row[PLC.UNRL - 1]);
      var t = plNum_(row[PLC.TOT - 1]);
      if (u !== null) { out.activeUnrl += u; }
      if (t !== null) { out.activeTot += t; }
      out.nActive++;
    } else if (st === PL_STATUS_INACTIVE) {
      var rl = plNum_(row[PLC.REAL - 1]);
      if (rl !== null) { out.realizedClosed += rl; out.nClosed++; }
    }
  }
  out.lifetime = out.realizedClosed + out.activeTot;
  return out;
}

/** v1.5.0: totals gate — default ON; PL_TOTALS='off' kills. */
function plTotalsOn_() {
  return plProp_(PL_PROP_TOTALS).toLowerCase() !== 'off';
}

function plPad2_(n) {
  return (n < 10 ? '0' : '') + n;
}

/** Local-calendar key for a Date (lot identity; stable per spreadsheet tz). */
function plDateKey_(d) {
  if (!plIsDate_(d)) { return ''; }
  return d.getFullYear() + '-' + plPad2_(d.getMonth() + 1) + '-' +
         plPad2_(d.getDate());
}

/** UTC-calendar key from a unix epoch (dividend ex-dates from Yahoo). */
function plDateKeyFromEpoch_(ts) {
  var d = new Date(ts * 1000);
  return d.getUTCFullYear() + '-' + plPad2_(d.getUTCMonth() + 1) + '-' +
         plPad2_(d.getUTCDate());
}

function plLotKey_(sym, buyKey) {
  return plTrim_(sym).toUpperCase() + '|' + plTrim_(buyKey);
}

function plDivKey_(sym, buyKey, exKey) {
  return plLotKey_(sym, buyKey) + '|' + plTrim_(exKey);
}

/** v1.1.0: certain currency from the symbol suffix, else ''. */
function plCcyFromSuffix_(sym) {
  var s = plTrim_(sym).toUpperCase();
  var dot = s.lastIndexOf('.');
  if (dot < 0 || dot === s.length - 1) { return ''; }
  var suf = s.slice(dot + 1);
  return PL_CCY_BY_SUFFIX.hasOwnProperty(suf) ? PL_CCY_BY_SUFFIX[suf] : '';
}

/**
 * v1.1.0: normalize Arabic-keyboard numeric input to a Number.
 *   - Arabic-Indic (\u0660-\u0669) and Extended/Persian (\u06F0-\u06F9)
 *     digits -> ASCII
 *   - \u066B (arabic decimal separator) -> '.'   \u066C (thousands) -> ''
 *   - digit-\u0632-digit -> digit.digit  (the Arabic keyboard maps the
 *     physical '.' key to \u0632: '64\u063273' is '64.73' typed in Arabic
 *     layout — the FER.US Sell Price defect, 2026-07-12 audit)
 *   - STRICT ASCII thousands grouping 1,234,567.89 -> commas stripped
 * Anything still non-numeric returns value:null — never guessed.
 * Returns { value: Number|null, changed: bool, text: normalizedString }.
 */
function plNormalizeNumericText_(v) {
  var raw = plTrim_(v);
  var out = { value: null, changed: false, text: raw };
  if (!raw) { return out; }
  var s = raw;
  s = s.replace(/[\u0660-\u0669]/g, function (ch) {
    return String.fromCharCode(ch.charCodeAt(0) - 0x0660 + 48);
  });
  s = s.replace(/[\u06F0-\u06F9]/g, function (ch) {
    return String.fromCharCode(ch.charCodeAt(0) - 0x06F0 + 48);
  });
  s = s.replace(/\u066B/g, '.');
  s = s.replace(/\u066C/g, '');
  s = s.replace(/(\d)\u0632(\d)/g, '$1.$2');
  if (/^\d{1,3}(,\d{3})+(\.\d+)?$/.test(s)) { s = s.replace(/,/g, ''); }
  var n = Number(s);
  if (isNaN(n)) { return out; }
  out.value = n;
  out.changed = s !== raw;
  out.text = s;
  return out;
}

/**
 * v1.1.0: parse one operator numeric-input cell.
 * Returns { value, changed, raw, bad }.
 *   changed=true  -> normalization recovered a number the cell should be
 *                    rewritten with (raw kept for the audit log line).
 *   bad=true      -> non-blank text that no safe rule can interpret.
 * normOn===false restores exact v1.0.0 plNum_ semantics (no fixes, and
 * bad stays false so no new flags fire).
 */
function plNumCell_(v, normOn) {
  if (typeof v === 'number') {
    return { value: isNaN(v) ? null : v, changed: false, raw: '', bad: false };
  }
  var raw = plTrim_(v);
  if (!raw) { return { value: null, changed: false, raw: '', bad: false }; }
  if (normOn === false) {
    return { value: plNum_(raw), changed: false, raw: raw, bad: false };
  }
  var norm = plNormalizeNumericText_(raw);
  return {
    value: norm.value,
    changed: norm.changed && norm.value !== null,
    raw: raw,
    bad: norm.value === null
  };
}

/** v1.1.0: '⚠ Sell Price ("64ز73"), Sell Fees ("x") not numeric'. */
function plBadCellsText_(badList) {
  var parts = [];
  for (var i = 0; i < badList.length; i++) {
    parts.push(badList[i].name + ' ("' + badList[i].raw + '")');
  }
  return parts.join(', ') + ' not numeric';
}

/**
 * v1.1.0: group complete ACTIVE lots per symbol for ONE fetch per symbol
 * (earliest buy date wins the lookback window). Pure; node-testable.
 */
function plGroupActiveLots_(lots) {
  var by = {};
  var order = [];
  for (var i = 0; i < lots.length; i++) {
    var lot = lots[i];
    if (!plLotComplete_(lot) || !plIsActive_(lot)) { continue; }
    if (plContraOn_() && plContradiction_(lot)) { continue; } /* v1.2.0 */
    if (!by[lot.sym]) {
      by[lot.sym] = { sym: lot.sym, fromDate: lot.bdate, lots: [] };
      order.push(lot.sym);
    }
    by[lot.sym].lots.push(lot);
    if (lot.bdate.getTime() < by[lot.sym].fromDate.getTime()) {
      by[lot.sym].fromDate = lot.bdate;
    }
  }
  var out = [];
  for (var o = 0; o < order.length; o++) { out.push(by[order[o]]); }
  return out;
}

/** Parse a Yahoo chart response (events=div) into sorted dividend events. */
function plParseDividends_(text) {
  var out = [];
  var j = null;
  try { j = JSON.parse(text); } catch (e) {
    return { ok: false, list: out, err: 'non-JSON body' };
  }
  var res = j && j.chart && j.chart.result && j.chart.result[0];
  if (!res) {
    var errDesc = '';
    try {
      errDesc = (j && j.chart && j.chart.error &&
          (j.chart.error.description || j.chart.error.code)) || '';
    } catch (eE) {}
    return { ok: false, list: out, err: errDesc || 'empty chart.result' };
  }
  var divs = res.events && res.events.dividends;
  if (!divs) { return { ok: true, list: out }; } /* valid, just no events */
  for (var k in divs) {
    if (!divs.hasOwnProperty(k)) { continue; }
    var d = divs[k];
    var ts = Number((d && d.date) || k);
    var amt = plNum_(d && d.amount);
    if (!(ts > 0) || amt === null || !(amt > 0)) { continue; }
    out.push({ ts: ts, exKey: plDateKeyFromEpoch_(ts), amount: amt });
  }
  out.sort(function (a, b) { return a.ts - b.ts; });
  return { ok: true, list: out };
}

/**
 * Raw sheet row (23 cells) -> lot object. rowIndex is the sheet row.
 * v1.1.0: the five operator numeric-input cells go through plNumCell_
 * (Arabic-input normalization). lot.fixes = auto-corrected cells to write
 * back; lot.bad = unrecoverable non-numeric cells to flag. normOn=false
 * (PL_INPUT_NORMALIZE='off') restores exact v1.0.0 plNum_ semantics.
 */
function plReadLot_(rowVals, rowIndex, normOn) {
  var fixes = [];
  var bad = [];
  var cells = {};
  for (var nI = 0; nI < PL_NUM_INPUT_COLS.length; nI++) {
    var def = PL_NUM_INPUT_COLS[nI];
    var pc = plNumCell_(rowVals[def.col - 1], normOn);
    cells[def.col] = pc.value;
    if (pc.changed) {
      fixes.push({ col: def.col, name: def.name, raw: pc.raw,
                   value: pc.value });
    }
    if (pc.bad) {
      bad.push({ col: def.col, name: def.name, raw: pc.raw,
                 side: def.side });
    }
  }
  return {
    row: rowIndex,
    sym: plTrim_(rowVals[PLC.SYM - 1]).toUpperCase(),
    name: plTrim_(rowVals[PLC.NAME - 1]),
    ccy: plTrim_(rowVals[PLC.CCY - 1]).toUpperCase(),
    status: plTrim_(rowVals[PLC.STATUS - 1]) || PL_STATUS_ACTIVE,
    bdate: plIsDate_(rowVals[PLC.BDATE - 1]) ? rowVals[PLC.BDATE - 1] : null,
    bpx: cells[PLC.BPX],
    sh: cells[PLC.SH],
    bfee: cells[PLC.BFEE] || 0,
    sdate: plIsDate_(rowVals[PLC.SDATE - 1]) ? rowVals[PLC.SDATE - 1] : null,
    spx: cells[PLC.SPX],
    sfee: cells[PLC.SFEE] || 0,
    div: plNum_(rowVals[PLC.DIV - 1]) || 0,
    note: plTrim_(rowVals[PLC.NOTE - 1]),
    realCell: plNum_(rowVals[PLC.REAL - 1]),
    fxCell: plNum_(rowVals[PLC.FX - 1]),
    fixes: fixes,
    bad: bad
  };
}

function plLotComplete_(lot) {
  return !!(lot.sym && lot.bdate && lot.bpx !== null && lot.bpx > 0 &&
            lot.sh !== null && lot.sh > 0);
}

function plIsActive_(lot) {
  return lot.status !== PL_STATUS_INACTIVE;
}

/** Inactive but missing the fields needed to finalize. */
function plPendingClose_(lot) {
  return !plIsActive_(lot) &&
         !(lot.sdate && lot.spx !== null && lot.spx > 0);
}

/* v1.2.0: kill-switch property + contradiction predicate + flag text. */
var PL_PROP_CONTRA_GUARD = 'PL_CONTRA_GUARD';
var PL_CONTRA_FLAG = '\u26d4 CONTRADICTION \u2014 Active with Sell ' +
    'Date/Price. Set Status=Inactive to finalize, or clear the sell ' +
    'cells. Excluded from My_Portfolio; computed cells withheld.';

function plContraOn_() {
  return plProp_(PL_PROP_CONTRA_GUARD).toLowerCase() !== 'off';
}

/** v1.2.0: Active but sell-side already populated — the T82U state. */
function plContradiction_(lot) {
  return plIsActive_(lot) &&
         !!(lot.sdate || (lot.spx !== null && lot.spx > 0));
}

/** Already finalized (frozen): Inactive with a numeric Realized cell. */
function plFinalized_(lot) {
  return !plIsActive_(lot) && lot.realCell !== null;
}

function plCostBasis_(lot) {
  return lot.sh * lot.bpx + lot.bfee;
}

/* v1.4.1 */
function plFxPegOn_() {
  return plProp_(PL_PROP_FX_PEG_GUARD).toLowerCase() !== 'off';
}

/**
 * v1.4.1 PURE: guard one rate for one currency.
 * Returns { rate, guarded, table }: rate is the input when the guard is
 * off, the ccy has no band, the input is null/NaN, or the input sits
 * inside its band; otherwise the static peg with guarded=true (table keeps
 * the rejected input for the status line). Never throws.
 */
function plFxGuard_(ccy, rate, on) {
  var out = { rate: rate, guarded: false, table: rate };
  try {
    if (on === false || rate === null || rate === undefined) { return out; }
    var c = plTrim_(ccy).toUpperCase();
    var band = PL_FX_PEG_BAND.hasOwnProperty(c) ? PL_FX_PEG_BAND[c] : null;
    if (!band) { return out; }
    var r = Number(rate);
    if (isNaN(r)) { return out; }
    if (r >= band[0] && r <= band[1]) { return out; }
    out.rate = PL_FX_PEG_STATIC[c];
    out.guarded = true;
  } catch (e) {}
  return out;
}

/** Computed cells for an ACTIVE lot. today: Date. Returns nulls if no px. */
function plComputeActive_(lot, price, fx, today) {
  var cb = plCostBasis_(lot);
  var out = { cb: cb, cur: price, fx: fx, mv: null, unrl: null, proc: '',
              real: '', tot: null, ret: null, days: null };
  if (plIsDate_(lot.bdate) && plIsDate_(today)) {
    out.days = Math.max(0, Math.floor(
        (today.getTime() - lot.bdate.getTime()) / 86400000));
  }
  if (price !== null && fx !== null && fx > 0) {
    out.mv = lot.sh * price * fx;
    out.unrl = (lot.sh * price - cb) * fx;
    out.tot = out.unrl + lot.div * fx;
    var cbSar = cb * fx;
    out.ret = cbSar > 0 ? out.tot / cbSar : null;
  }
  return out;
}

/** Computed cells for a CLOSED lot (finalize-once). fx: frozen rate. */
function plComputeClosed_(lot, fx) {
  var cb = plCostBasis_(lot);
  var proc = lot.sh * lot.spx - lot.sfee;
  var real = (proc - cb + lot.div) * fx;
  var cbSar = cb * fx;
  var days = (plIsDate_(lot.bdate) && plIsDate_(lot.sdate)) ?
      Math.max(0, Math.round(
          (lot.sdate.getTime() - lot.bdate.getTime()) / 86400000)) : null;
  return { cb: cb, proc: proc, real: real, tot: real,
           ret: cbSar > 0 ? real / cbSar : null, days: days, fx: fx };
}

/** Aggregate ACTIVE lots per symbol -> {SYM: {qty, avg, ccy, name}}. */
function plAggregateActive_(lots) {
  var agg = {};
  for (var i = 0; i < lots.length; i++) {
    var lot = lots[i];
    if (!plLotComplete_(lot) || !plIsActive_(lot)) { continue; }
    if (plContraOn_() && plContradiction_(lot)) { continue; } /* v1.2.0 */
    if (!agg[lot.sym]) {
      agg[lot.sym] = { qty: 0, cost: 0, ccy: lot.ccy, name: lot.name, bdate: '' };
    }
    agg[lot.sym].qty += lot.sh;
    try { /* v1.3.0: earliest Buy Date wins */
      var plBt = lot.bdate ? new Date(lot.bdate).getTime() : NaN;
      var plCt = agg[lot.sym].bdate ? new Date(agg[lot.sym].bdate).getTime() : NaN;
      if (!isNaN(plBt) && (isNaN(plCt) || plBt < plCt)) {
        agg[lot.sym].bdate = lot.bdate;
      }
    } catch (plBdErr) {}
    agg[lot.sym].cost += plCostBasis_(lot);
  }
  for (var s in agg) {
    if (agg.hasOwnProperty(s)) {
      agg[s].avg = agg[s].qty > 0 ? agg[s].cost / agg[s].qty : 0;
    }
  }
  return agg;
}

// ---------------------------------------------------------------------------
// Config / service readers
// ---------------------------------------------------------------------------

function plProp_(key) {
  try {
    return PropertiesService.getScriptProperties().getProperty(key) || '';
  } catch (e) { return ''; }
}

function plNow_() {
  try {
    return Utilities.formatDate(new Date(),
        Session.getScriptTimeZone() || 'Asia/Riyadh', 'yyyy-MM-dd HH:mm:ss');
  } catch (e) { return String(new Date()); }
}

function plStatus_(sheet, msg) {
  try {
    sheet.getRange(PL_ROW_STATUS, 2).setValue(
        'Last refresh ' + plNow_() + ' | ' + msg);
  } catch (e) {}
}

/** FX map CCY -> effective SAR rate, from the TFB_FX_LOOKUP named range. */
function plFxRates_(ss) {
  var map = { SAR: 1 };
  var vals = null;
  try {
    var rng = ss.getRangeByName(PL_FX_RANGE);
    if (rng) { vals = rng.getValues(); }
  } catch (e) {}
  if (!vals) {
    try { /* fallback: scan _Lists_Config for the CCY table header */
      var lc = ss.getSheetByName('_Lists_Config');
      if (lc) {
        var all = lc.getDataRange().getValues();
        for (var r = 0; r < all.length; r++) {
          if (plTrim_(all[r][0]) === 'CCY') {
            vals = all.slice(r + 1);
            break;
          }
        }
      }
    } catch (e2) {}
  }
  if (!vals) { return map; }
  for (var i = 0; i < vals.length; i++) {
    var ccy = plTrim_(vals[i][0]);
    if (!ccy || ccy === 'CCY') { continue; }
    var rate = plNum_(vals[i][3]);
    if (rate === null) { rate = plNum_(vals[i][1]); }
    if (rate === null) { rate = plNum_(vals[i][2]); }
    if (rate !== null && rate > 0) { map[ccy.toUpperCase()] = rate; }
  }
  map.SAR = 1;
  return map;
}

/** My_Portfolio column map by header NAME. Throws if feeder cols missing. */
function plMpCols_(mp) {
  var hdr = mp.getRange(1, 1, 1, mp.getLastColumn()).getValues()[0];
  var find = function (name) {
    for (var i = 0; i < hdr.length; i++) {
      if (plTrim_(hdr[i]) === name) { return i + 1; }
    }
    return 0;
  };
  var cols = {
    sym: find(PL_MP_COL_SYMBOL), qty: find(PL_MP_COL_QTY),
    avg: find(PL_MP_COL_AVG), cost: find(PL_MP_COL_COST),
    name: find(PL_MP_COL_NAME), ccy: find(PL_MP_COL_CCY),
    buyDate: find('Buy Date'),
    price: find(PL_MP_COL_PRICE)
  };
  if (!cols.sym || !cols.qty || !cols.avg) {
    throw new Error('[PL v' + PL_VERSION + '] My_Portfolio feeder columns ' +
        'not found by name (need Symbol / Position Qty / Avg Cost) \u2014 ' +
        'schema drift; sync aborted.');
  }
  return cols;
}

/** My_Portfolio enrichment map SYM -> {name, ccy, price, row}. */
function plMpMap_(mp, cols) {
  var out = {};
  var last = mp.getLastRow();
  if (last < 2) { return out; }
  var width = mp.getLastColumn();
  var vals = mp.getRange(2, 1, last - 1, width).getValues();
  for (var i = 0; i < vals.length; i++) {
    var sym = plTrim_(vals[i][cols.sym - 1]).toUpperCase();
    if (!sym) { continue; }
    out[sym] = {
      row: i + 2,
      name: cols.name ? plTrim_(vals[i][cols.name - 1]) : '',
      ccy: cols.ccy ? plTrim_(vals[i][cols.ccy - 1]).toUpperCase() : '',
      price: cols.price ? plNum_(vals[i][cols.price - 1]) : null
    };
  }
  return out;
}

// ---------------------------------------------------------------------------
// Layout builder + v1-seed migration
// ---------------------------------------------------------------------------

function plPaintDataRow_(sheet, r, amber) {
  var inputBg = amber ? PL_AMBER : PL_BLUE;
  var calcBg = amber ? PL_AMBER : PL_GRAY;
  sheet.getRange(r, 1, 1, 11).setBackground(inputBg);          /* A-K */
  sheet.getRange(r, PLC.DIV).setBackground(calcBg);            /* L    */
  sheet.getRange(r, PLC.NOTE).setBackground(inputBg);          /* M    */
  sheet.getRange(r, PLC.CB, 1, 10).setBackground(calcBg);      /* N-W  */
}

function plApplyFormats_(sheet, firstRow, numRows) {
  var f = {};
  f[PLC.BDATE] = 'yyyy-mm-dd'; f[PLC.SDATE] = 'yyyy-mm-dd';
  f[PLC.BPX] = '0.00'; f[PLC.SPX] = '0.00'; f[PLC.CUR] = '0.00';
  f[PLC.SH] = '0'; f[PLC.BFEE] = '0.00'; f[PLC.SFEE] = '0.00';
  f[PLC.DIV] = '#,##0.00'; f[PLC.CB] = '#,##0.00';
  f[PLC.FX] = '0.0000'; f[PLC.PROC] = '#,##0.00';
  f[PLC.MV] = '#,##0;[Red](#,##0)'; f[PLC.UNRL] = '#,##0;[Red](#,##0)';
  f[PLC.REAL] = '#,##0;[Red](#,##0)'; f[PLC.TOT] = '#,##0;[Red](#,##0)';
  f[PLC.RET] = '0.0%'; f[PLC.DAYS] = '0';
  for (var col in f) {
    if (f.hasOwnProperty(col)) {
      sheet.getRange(firstRow, Number(col), numRows, 1)
          .setNumberFormat(f[col]);
    }
  }
}

/**
 * One-time / repair. Detects and migrates the v1 4-column seed
 * (Symbol | Buy Date | Buy Price | Quantity) into v2 Active lots.
 * Existing v2 data rows are preserved untouched.
 */
function buildPortfolioLedgerLayout() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(PL_SHEET);
  var migrated = [];
  if (!sheet) {
    sheet = ss.insertSheet(PL_SHEET);
  } else {
    var r1 = sheet.getRange(1, 1, 1, 4).getValues()[0];
    var isV1 = plTrim_(r1[0]) === 'Symbol' && plTrim_(r1[1]) === 'Buy Date' &&
               plTrim_(r1[2]) === 'Buy Price' && plTrim_(r1[3]) === 'Quantity';
    if (isV1) {
      var old = sheet.getDataRange().getValues();
      for (var i = 1; i < old.length; i++) {
        var sym = plTrim_(old[i][0]);
        if (!sym) { continue; }
        migrated.push({ sym: sym.toUpperCase(),
                        bdate: plIsDate_(old[i][1]) ? old[i][1] : null,
                        bpx: plNum_(old[i][2]), sh: plNum_(old[i][3]) });
      }
      sheet.clear();
      try { sheet.getDataRange().clearDataValidations(); } catch (eV) {}
    }
  }
  var already = plTrim_(sheet.getRange(PL_ROW_HEADER, 1).getValue()) ===
                PL_HEADERS[0];
  if (sheet.getMaxColumns() < PL_HEADERS.length) {
    sheet.insertColumnsAfter(sheet.getMaxColumns(),
        PL_HEADERS.length - sheet.getMaxColumns());
  }
  sheet.getRange(PL_ROW_TITLE, 1, 1, PL_HEADERS.length).breakApart();
  sheet.getRange(PL_ROW_TITLE, 1, 1, PL_HEADERS.length).merge()
      .setValue(PL_SHEET + ' \u2014 INVESTMENT LEDGER  (source of truth: ' +
                'buys, sells, dividends \u2014 active & closed)')
      .setBackground(PL_NAVY).setFontColor('#FFFFFF').setFontWeight('bold')
      .setFontSize(12);
  sheet.getRange(PL_ROW_STATUS, 1).setValue('Status:')
      .setFontColor(PL_MUTED).setFontSize(9);
  sheet.getRange(PL_ROW_LEGEND, 1, 1, PL_HEADERS.length).breakApart();
  sheet.getRange(PL_ROW_LEGEND, 1, 1, PL_HEADERS.length).merge()
      .setValue('BLUE = operator input \u00b7 GRAY = script-computed ' +
                '(freezes when Inactive) \u00b7 AMBER = Inactive but sell ' +
                'fields missing \u2014 not finalized \u00b7 Dividends are ' +
                'AUTO from _Dividend_Log (plAddManualDividend for gaps) ' +
                '\u00b7 My_Portfolio & Portfolio_Decision read ACTIVE only')
      .setBackground(PL_TINT).setFontColor(PL_MUTED).setFontSize(9)
      .setWrap(true);
  sheet.getRange(PL_ROW_HEADER, 1, 1, PL_HEADERS.length)
      .setValues([PL_HEADERS]).setBackground(PL_NAVY)
      .setFontColor('#FFFFFF').setFontWeight('bold').setFontSize(9)
      .setWrap(true).setVerticalAlignment('middle');
  sheet.setRowHeight(PL_ROW_HEADER, 30);
  var dv = SpreadsheetApp.newDataValidation()
      .requireValueInList([PL_STATUS_ACTIVE, PL_STATUS_INACTIVE], true)
      .setAllowInvalid(false).build();
  sheet.getRange(PL_ROW_FIRST, PLC.STATUS, PL_MAX_LOTS, 1)
      .setDataValidation(dv);
  plApplyFormats_(sheet, PL_ROW_FIRST, PL_MAX_LOTS);
  var widths = [9, 26, 6, 10, 11, 9, 8, 8, 11, 9, 8, 12, 30, 11, 11, 8, 13,
                14, 11, 14, 13, 9, 8];
  for (var w = 0; w < widths.length; w++) {
    sheet.setColumnWidth(w + 1, widths[w] * 8);
  }
  sheet.setFrozenRows(PL_ROW_HEADER);
  sheet.setFrozenColumns(1);
  if (migrated.length) {
    var rows = [];
    for (var m = 0; m < migrated.length; m++) {
      var lot = migrated[m];
      var row = ['', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                 '', '', '', '', '', '', '', ''];
      row[PLC.SYM - 1] = lot.sym;
      row[PLC.CCY - 1] = /\.SR$/.test(lot.sym) ? 'SAR' : '';
      row[PLC.STATUS - 1] = PL_STATUS_ACTIVE;
      row[PLC.BDATE - 1] = lot.bdate;
      row[PLC.BPX - 1] = lot.bpx;
      row[PLC.SH - 1] = lot.sh;
      row[PLC.BFEE - 1] = 0;
      row[PLC.NOTE - 1] = 'migrated from v1 seed';
      rows.push(row);
    }
    sheet.getRange(PL_ROW_FIRST, 1, rows.length, PL_HEADERS.length)
        .setValues(rows);
    for (var p = 0; p < rows.length; p++) {
      plPaintDataRow_(sheet, PL_ROW_FIRST + p, false);
    }
  }
  var msg = '[PL v' + PL_VERSION + '] layout ' +
      (already ? 'repaired' : 'built') +
      (migrated.length ? ' \u2014 migrated ' + migrated.length +
       ' v1 seed lot(s) as Active' : '');
  plStatus_(sheet, msg + ' \u2014 run refreshPortfolioLedger next');
  Logger.log(msg);
  try { ss.toast(msg, 'TFB Ledger', 8); } catch (e) {}
  return sheet;
}

// ---------------------------------------------------------------------------
// Dividend log + auto-sync (yahoo chart events=div)
// ---------------------------------------------------------------------------

function plDivLogSheet_(ss) {
  var sh = ss.getSheetByName(PL_DIVLOG_SHEET);
  if (!sh) { sh = ss.insertSheet(PL_DIVLOG_SHEET); }
  /* Header self-heal: covers a fresh sheet AND a manually created empty
   * one (otherwise appends would land on row 1 with no header). */
  if (plTrim_(sh.getRange(1, 1).getValue()) !== PL_DIVLOG_HEADERS[0]) {
    if (sh.getMaxColumns() < PL_DIVLOG_HEADERS.length) {
      sh.insertColumnsAfter(sh.getMaxColumns(),
          PL_DIVLOG_HEADERS.length - sh.getMaxColumns());
    }
    sh.getRange(1, 1, 1, PL_DIVLOG_HEADERS.length)
        .setValues([PL_DIVLOG_HEADERS]).setFontWeight('bold')
        .setBackground(PL_NAVY).setFontColor('#FFFFFF').setFontSize(9);
    sh.setFrozenRows(1);
  }
  return sh;
}

/** Read the whole dividend log once -> { keys: {divKey:1}, totals: {lotKey:sum} }. */
function plDivLogIndex_(logSheet) {
  var keys = {}, totals = {};
  var last = logSheet.getLastRow();
  if (last < 2) { return { keys: keys, totals: totals }; }
  var vals = logSheet.getRange(2, 1, last - 1,
      PL_DIVLOG_HEADERS.length).getValues();
  for (var i = 0; i < vals.length; i++) {
    var sym = plTrim_(vals[i][2]).toUpperCase();
    var buyKey = plIsDate_(vals[i][3]) ? plDateKey_(vals[i][3]) :
                 plTrim_(vals[i][3]);
    var exKey = plIsDate_(vals[i][4]) ? plDateKey_(vals[i][4]) :
                plTrim_(vals[i][4]);
    var amt = plNum_(vals[i][7]);
    if (!sym || amt === null) { continue; }
    keys[plDivKey_(sym, buyKey, exKey)] = 1;
    var lk = plLotKey_(sym, buyKey);
    totals[lk] = (totals[lk] || 0) + amt;
  }
  return { keys: keys, totals: totals };
}

/** v1.4.0: the symbol Yahoo resolves — bare ticker for '.US', otherwise as-is. */
function plYahooSymbol_(sym) {
  var s = String(sym || '').trim();
  if (/\.US$/i.test(s)) { return s.slice(0, -3); }
  return s;
}

function plFetchDividends_(sym, fromDate) {
  var p1 = Math.max(0, Math.floor(fromDate.getTime() / 1000) - 3 * 86400);
  var p2 = Math.floor(new Date().getTime() / 1000) + 86400;
  /* v1.1.0: interval 1d -> 1mo. Dividend EVENTS are interval-independent;
   * the daily-bar payload over multi-year lookbacks was the heaviest
   * possible variant for zero benefit. */
  var path = '/v8/finance/chart/' + encodeURIComponent(plYahooSymbol_(sym)) +   // v1.4.0
      '?period1=' + p1 + '&period2=' + p2 +
      '&interval=1mo&events=div&includePrePost=false';
  /* v1.1.0: host1 -> host2 -> (429/5xx only, after 1200 ms) host1 again;
   * capture the last HTTP code and Yahoo error for the status line. */
  var attempts = [PL_YH_HOSTS[0], PL_YH_HOSTS[1], PL_YH_HOSTS[0]];
  var lastCode = 0;
  var lastErr = '';
  for (var h = 0; h < attempts.length; h++) {
    if (h === 1) {
      try { Utilities.sleep(350); } catch (eS1) {}
    } else if (h === 2) {
      if (!(lastCode === 429 || lastCode >= 500)) { break; }
      try { Utilities.sleep(1200); } catch (eS2) {}
    }
    try {
      var resp = UrlFetchApp.fetch(attempts[h] + path, {
        muteHttpExceptions: true, followRedirects: true,
        headers: { 'User-Agent': 'Mozilla/5.0 (TFB-Ledger)' }
      });
      lastCode = resp.getResponseCode();
      var parsed = plParseDividends_(resp.getContentText());
      if (lastCode === 200 && parsed.ok) {
        return { ok: true, list: parsed.list, code: 200, err: '' };
      }
      lastErr = parsed.err || '';
    } catch (e) {
      lastErr = String(e).slice(0, 120);
    }
  }
  return { ok: false, list: [], code: lastCode, err: lastErr };
}

/**
 * Auto-dividends for ACTIVE lots only. Appends missing AUTO rows to
 * _Dividend_Log (dedup key symbol|buyDate|exDate) and returns
 * { totals: {lotKey: nativeSum incl. MANUAL rows}, added, failed: [] }.
 */
function plSyncDividends_(ss, lots) {
  var logSheet = plDivLogSheet_(ss);
  var idx = plDivLogIndex_(logSheet);
  var mode = plProp_(PL_PROP_DIV_MODE).toLowerCase();
  var out = { totals: idx.totals, added: 0, failed: [], skipped: mode === 'off' };
  if (mode === 'off') { return out; }
  var now = plNow_();
  var toAppend = [];
  /* v1.1.0: ONE fetch per SYMBOL (earliest active buy date), 400 ms
   * courtesy sleep between symbols; failures carry the HTTP code and
   * Yahoo error so 'fail RCI.US' becomes 'fail RCI.US(429)'. */
  var groups = plGroupActiveLots_(lots);
  for (var g = 0; g < groups.length; g++) {
    var grp = groups[g];
    if (g > 0) {
      try { Utilities.sleep(400); } catch (eSl) {}
    }
    var fetched = plFetchDividends_(grp.sym, grp.fromDate);
    if (!fetched.ok) {
      out.failed.push(grp.sym + '(' + (fetched.code || 'ERR') +
          (fetched.err ? ':' + String(fetched.err).slice(0, 60) : '') + ')');
      try {
        Logger.log('[PL v' + PL_VERSION + '] DIV_FETCH FAIL ' + grp.sym +
            ' code=' + fetched.code + ' err=' + (fetched.err || ''));
      } catch (eLg) {}
      continue;
    }
    for (var li = 0; li < grp.lots.length; li++) {
      var lot = grp.lots[li];
      var buyKey = plDateKey_(lot.bdate);
      var lk = plLotKey_(lot.sym, buyKey);
      for (var e = 0; e < fetched.list.length; e++) {
        var ev = fetched.list[e];
        if (ev.ts * 1000 < lot.bdate.getTime()) { continue; }
        var dk = plDivKey_(lot.sym, buyKey, ev.exKey);
        if (idx.keys[dk]) { continue; }
        idx.keys[dk] = 1;
        var amount = ev.amount * lot.sh;
        out.totals[lk] = (out.totals[lk] || 0) + amount;
        toAppend.push([now, 'AUTO', lot.sym, buyKey, ev.exKey, ev.amount,
                       lot.sh, amount, lot.ccy || '',
                       'yahoo_chart events=div']);
        out.added++;
      }
    }
  }
  if (toAppend.length) {
    logSheet.getRange(logSheet.getLastRow() + 1, 1, toAppend.length,
        PL_DIVLOG_HEADERS.length).setValues(toAppend);
  }
  return out;
}

// ---------------------------------------------------------------------------
// Feeder: ACTIVE lots -> My_Portfolio (upsert + prune)
// ---------------------------------------------------------------------------

function plSyncCore_(ss, agg) {
  var mp = ss.getSheetByName(PL_MP_SHEET);
  if (!mp) {
    return { ok: false, note: 'My_Portfolio absent \u2014 sync skipped' };
  }
  var cols = plMpCols_(mp);
  var map = plMpMap_(mp, cols);
  var upserts = 0, added = 0;
  var activeCount = 0;
  for (var sym in agg) {
    if (!agg.hasOwnProperty(sym)) { continue; }
    activeCount++;
    var a = agg[sym];
    if (map[sym]) {
      var r = map[sym].row;
      mp.getRange(r, cols.qty).setValue(a.qty);
      mp.getRange(r, cols.avg).setValue(a.avg);
      if (cols.cost) { mp.getRange(r, cols.cost).setValue(a.qty * a.avg); }
      if (cols.buyDate && a.bdate) { /* v1.3.0: fill-if-blank only */
        var plBdCell = mp.getRange(r, cols.buyDate);
        if (!String(plBdCell.getValue() || '').trim()) {
          plBdCell.setValue(a.bdate);
        }
      }
      upserts++;
    } else {
      var newRow = mp.getLastRow() + 1;
      mp.getRange(newRow, cols.sym).setValue(sym);
      mp.getRange(newRow, cols.qty).setValue(a.qty);
      mp.getRange(newRow, cols.avg).setValue(a.avg);
      if (cols.cost) { mp.getRange(newRow, cols.cost).setValue(a.qty * a.avg); }
      if (cols.buyDate && a.bdate) { mp.getRange(newRow, cols.buyDate).setValue(a.bdate); }
      if (cols.name && a.name) { mp.getRange(newRow, cols.name).setValue(a.name); }
      added++;
    }
  }
  var removed = 0;
  var toDelete = [];
  for (var msym in map) {
    if (map.hasOwnProperty(msym) && !agg[msym]) {
      toDelete.push(map[msym].row);
    }
  }
  var pruneCap = parseInt(plProp_('TFB_PL_PRUNE_MAX') || '2', 10);
  if (isNaN(pruneCap) || pruneCap < 0) { pruneCap = 2; }
  var pruneSkipped = '';
  if (activeCount > 0 && toDelete.length > 0 && toDelete.length <= pruneCap) {
    toDelete.sort(function (a, b) { return b - a; });
    for (var d = 0; d < toDelete.length; d++) {
      mp.deleteRow(toDelete[d]);
      removed++;
    }
  } else if (toDelete.length > pruneCap) {
    pruneSkipped = ' (PRUNE SKIPPED: would remove ' + toDelete.length +
        ' rows > cap ' + pruneCap +
        ' - partial ledger read suspected; verify, then raise TFB_PL_PRUNE_MAX)';
    Logger.log('[PL v' + PL_VERSION + ']' + pruneSkipped);
  }
  return { ok: true, upserts: upserts, added: added, removed: removed,
           guard: activeCount === 0,
           note: 'MP upd ' + upserts + ' / add ' + added + ' / removed ' +
                 removed + pruneSkipped + (activeCount === 0 ?
                 ' (0 active lots \u2014 prune skipped)' : '') };
}

/** Public feeder entry: aggregates ACTIVE lots and writes My_Portfolio. */
function syncLedgerToPortfolio() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  if (plProp_(PL_PROP_DISABLED) === '1') {
    Logger.log('[PL v' + PL_VERSION + '] disabled by ' + PL_PROP_DISABLED);
    return { ok: false, note: 'disabled' };
  }
  var sheet = ss.getSheetByName(PL_SHEET);
  if (!sheet) {
    Logger.log('[PL v' + PL_VERSION + '] ' + PL_SHEET + ' absent \u2014 run ' +
               'buildPortfolioLedgerLayout first');
    return { ok: false, note: 'ledger sheet absent' };
  }
  var lots = plReadAllLots_(sheet);
  var res = plSyncCore_(ss, plAggregateActive_(lots));
  Logger.log('[PL v' + PL_VERSION + '] sync: ' + res.note);
  return res;
}

function plReadAllLots_(sheet) {
  var last = sheet.getLastRow();
  var lots = [];
  if (last < PL_ROW_FIRST) { return lots; }
  /* v1.1.0: one switch read per pass; 'off' = exact v1.0.0 parsing. */
  var normOn = plProp_(PL_PROP_INPUT_NORM).toLowerCase() !== 'off';
  var vals = sheet.getRange(PL_ROW_FIRST, 1, last - PL_ROW_FIRST + 1,
      PL_HEADERS.length).getValues();
  for (var i = 0; i < vals.length; i++) {
    var lot = plReadLot_(vals[i], PL_ROW_FIRST + i, normOn);
    if (lot.sym) { lots.push(lot); }
  }
  return lots;
}

// ---------------------------------------------------------------------------
// Main refresh: sync feeder -> prices/FX -> auto-dividends -> compute/freeze
// ---------------------------------------------------------------------------

function refreshPortfolioLedger() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(PL_SHEET);
  if (!sheet ||
      plTrim_(sheet.getRange(PL_ROW_HEADER, 1).getValue()) !==
      PL_HEADERS[0]) {
    sheet = buildPortfolioLedgerLayout();
  }
  if (plProp_(PL_PROP_DISABLED) === '1') {
    plStatus_(sheet, 'DISABLED (' + PL_PROP_DISABLED + '=1)');
    return;
  }
  try {
    var t0 = new Date().getTime();
    var lots = plReadAllLots_(sheet);
    var syncRes = plSyncCore_(ss, plAggregateActive_(lots));
    var mp = ss.getSheetByName(PL_MP_SHEET);
    var mpMap = {};
    if (mp) {
      try { mpMap = plMpMap_(mp, plMpCols_(mp)); } catch (eM) {
        Logger.log('[PL v' + PL_VERSION + '] MP map: ' + eM);
      }
    }
    var fx = plFxRates_(ss);
    var divRes = plSyncDividends_(ss, lots);
    var today = new Date();
    var nActive = 0, nClosed = 0, nPending = 0, nFinalized = 0;
    var nContra = 0;                                   /* v1.2.0 */
    var fxMissing = {}, pxMissing = {};
    /* v1.1.0 */
    var ccyMissing = {};
    var nFixed = 0;
    var normOn = plProp_(PL_PROP_INPUT_NORM).toLowerCase() !== 'off';
    var pegOn = plFxPegOn_();                          /* v1.4.1 */
    var fxGuarded = {};
    var nFeeZero = 0;
    for (var i = 0; i < lots.length; i++) {
      var lot = lots[i];
      var r = lot.row;

      /* v1.1.0 INPUT_FIX: write auto-normalized values back into their
       * cells so the cell and the computation can never diverge; every
       * fix is logged with the original text for the audit trail. */
      var badBuy = [];
      var badSell = [];
      if (lot.bad && lot.bad.length) {
        for (var bI = 0; bI < lot.bad.length; bI++) {
          if (lot.bad[bI].side === 'sell') { badSell.push(lot.bad[bI]); }
          else { badBuy.push(lot.bad[bI]); }
        }
      }
      if (normOn && lot.fixes && lot.fixes.length) {
        for (var fI = 0; fI < lot.fixes.length; fI++) {
          var fitem = lot.fixes[fI];
          sheet.getRange(r, fitem.col).setValue(fitem.value);
          nFixed++;
          Logger.log('[PL v' + PL_VERSION + '] INPUT_FIX row ' + r + ' ' +
              fitem.name + ' "' + fitem.raw + '" -> ' + fitem.value);
        }
      }

      if (!plLotComplete_(lot)) {
        plWriteNoteFlag_(sheet, lot, badBuy.length ?
            '\u26a0 ' + plBadCellsText_(badBuy) + ' \u2014 fix to compute' :
            '\u26a0 buy date/price/shares required');
        plPaintDataRow_(sheet, r, true);
        continue;
      }
      var buyKey = plDateKey_(lot.bdate);
      var lk = plLotKey_(lot.sym, buyKey);
      var mrow = mpMap[lot.sym];
      if (!lot.name && mrow && mrow.name) {
        sheet.getRange(r, PLC.NAME).setValue(mrow.name);
      }
      /* v1.1.0: suffix inference widened (SR/US/TO/NS/IS/MC — .L
       * excluded: pence). NMM.US self-heals to USD here. */
      var ccy = lot.ccy || (mrow && mrow.ccy) || plCcyFromSuffix_(lot.sym);
      if (ccy && !lot.ccy) { sheet.getRange(r, PLC.CCY).setValue(ccy); }
      if (plContraOn_() && plContradiction_(lot)) {       /* v1.2.0 */
        nContra++;
        /* Factual Cost Basis stays; the 9 LIVE cells are withheld so a
         * sold-or-ambiguous lot can never show phantom MV / P&L. */
        sheet.getRange(r, PLC.CB, 1, 10).setValues([[
            plCostBasis_(lot),
            PL_DASH, PL_DASH, PL_DASH, PL_DASH,
            PL_DASH, PL_DASH, PL_DASH, PL_DASH, PL_DASH]]);
        plClearNoteFlag_(sheet, lot);
        plWriteNoteFlag_(sheet, lot, PL_CONTRA_FLAG);
        plPaintDataRow_(sheet, r, true);
      } else if (plIsActive_(lot)) {
        nActive++;
        if (!divRes.skipped) {
          lot.div = divRes.totals[lk] || 0;
          sheet.getRange(r, PLC.DIV).setValue(lot.div);
        }
        var price = mrow ? mrow.price : null;
        var rate = ccy && fx[ccy] ? fx[ccy] : null;
        var pgA = plFxGuard_(ccy, rate, pegOn);            /* v1.4.1 */
        if (pgA.guarded) {
          fxGuarded[ccy] = pgA.table + '\u2192' + pgA.rate;
          rate = pgA.rate;
        }
        if (price === null) { pxMissing[lot.sym] = 1; }
        if (rate === null && ccy) { fxMissing[ccy] = 1; }
        if (!ccy) { ccyMissing[lot.sym] = 1; }   /* v1.1.0 */
        var c = plComputeActive_(lot, price, rate, today);
        sheet.getRange(r, PLC.CB, 1, 10).setValues([[
            c.cb,
            price === null ? PL_DASH : price,
            rate === null ? PL_DASH : rate,
            c.mv === null ? PL_DASH : c.mv,
            c.unrl === null ? PL_DASH : c.unrl,
            PL_DASH, PL_DASH,
            c.tot === null ? PL_DASH : c.tot,
            c.ret === null ? PL_DASH : c.ret,
            c.days === null ? PL_DASH : c.days]]);
        plClearNoteFlag_(sheet, lot);
        /* v1.1.0: an ACTIVE row with no currency or with unrecoverable
         * sell-side text is flagged PRECISELY and painted amber — the
         * v1.0.0 behavior was silent dashes with no flag at all. */
        var activeFlags = [];
        if (!ccy) {
          activeFlags.push('\u26a0 Ccy required \u2014 set currency ' +
              '(FX\u2192SAR unavailable)');
        }
        if (badSell.length) {
          activeFlags.push('\u26a0 ' + plBadCellsText_(badSell) +
              ' \u2014 fix before closing');
        }
        for (var afI = 0; afI < activeFlags.length; afI++) {
          plWriteNoteFlag_(sheet, lot, activeFlags[afI]);
        }
        plPaintDataRow_(sheet, r, activeFlags.length > 0);
      } else if (plFinalized_(lot)) {
        nFinalized++; nClosed++;
        /* v1.2.1: frozen computed cells stay untouched, but a stale
         * \u26d4/\u26a0 flag on a finalized row is misinformation —
         * the cleaner no-ops when the note carries no flags. */
        plClearNoteFlag_(sheet, lot);
      } else if (plPendingClose_(lot)) {
        nPending++;
        /* v1.1.0: when the pending state is CAUSED by non-numeric sell
         * cells, say so — the FER.US '64\u063273' row looked filled while
         * the generic flag claimed fields were missing. */
        plWriteNoteFlag_(sheet, lot, badSell.length ?
            '\u26a0 ' + plBadCellsText_(badSell) + ' \u2014 fix to finalize' :
            PL_PENDING_FLAG);
        plPaintDataRow_(sheet, r, true);
      } else {                                       /* finalize ONCE */
        nClosed++;
        var c2 = lot.ccy || ccy;
        var frozenFx = lot.fxCell;
        /* v1.4.1: an FX cell outside its peg band is NOT reused as the
         * frozen rate (the SHG.US 3.8090 case) — fall through to the
         * table rate, which is guarded too. */
        var pgC = plFxGuard_(c2, frozenFx, pegOn);
        if (pgC.guarded) { frozenFx = null; }
        if (frozenFx === null || !(frozenFx > 0)) {
          frozenFx = (c2 && fx[c2]) ? fx[c2] : (c2 === 'SAR' ? 1 : null);
          var pgT = plFxGuard_(c2, frozenFx, pegOn);
          if (pgT.guarded) { frozenFx = pgT.rate; }
          if (pgC.guarded || pgT.guarded) {
            fxGuarded[c2] = (pgC.guarded ? pgC.table : pgT.table) +
                '\u2192' + frozenFx;
          }
        }
        if (frozenFx === null) {
          /* v1.1.0: distinguish 'no currency set' from 'rate table gap'. */
          var c3 = lot.ccy || ccy;
          if (!c3) {
            ccyMissing[lot.sym] = 1;
            plWriteNoteFlag_(sheet, lot,
                '\u26a0 Ccy required \u2014 cannot finalize');
          } else {
            fxMissing[c3] = 1;
            plWriteNoteFlag_(sheet, lot,
                '\u26a0 FX missing for ' + c3 + ' \u2014 cannot finalize');
          }
          plPaintDataRow_(sheet, r, true);
          continue;
        }
        var cc = plComputeClosed_(lot, frozenFx);
        sheet.getRange(r, PLC.CB, 1, 10).setValues([[
            cc.cb, PL_DASH, cc.fx, PL_DASH, PL_DASH, cc.proc, cc.real,
            cc.tot, cc.ret === null ? PL_DASH : cc.ret,
            cc.days === null ? PL_DASH : cc.days]]);
        plClearNoteFlag_(sheet, lot);
        plClearInfoFlag_(sheet, lot);                      /* v1.4.1 */
        if (!(lot.sfee > 0)) {
          nFeeZero++;
          plWriteNoteFlag_(sheet, lot, PL_INFO_FEES_FLAG);
        }
        plPaintDataRow_(sheet, r, false);
      }
    }
    plApplyFormats_(sheet, PL_ROW_FIRST, PL_MAX_LOTS);   /* v1.2.1 */
    var plTt = null;                                      /* v1.5.0 */
    if (plTotalsOn_()) {
      var plTtGrid = sheet.getRange(PL_ROW_FIRST, 1, PL_MAX_LOTS,
          PL_HEADERS.length).getValues();
      plTt = plTotalsFromGrid_(plTtGrid);
      sheet.getRange(PL_ROW_STATUS, PLC.PROC).setValue('TOTALS \u2192')
          .setFontColor(PL_MUTED).setFontSize(9)
          .setHorizontalAlignment('right');
      sheet.getRange(PL_ROW_STATUS, PLC.REAL, 1, 2)
          .setValues([[Math.round(plTt.realizedClosed),
                       Math.round(plTt.lifetime)]])
          .setNumberFormat('#,##0').setFontWeight('bold')
          .setFontColor(PL_NAVY).setFontSize(9);
    }
    var secs = Math.round((new Date().getTime() - t0) / 100) / 10;
    var bits = [nActive + ' active \u00b7 ' + nClosed + ' closed \u00b7 ' +
                nPending + ' pending'];
    if (nContra > 0) {                                    /* v1.2.0 */
      bits.push('\u26d4 ' + nContra + ' CONTRADICTION' +
          (nContra > 1 ? 'S' : '') +
          ' (Active+Sell fields) \u2014 excluded from My_Portfolio');
      Logger.log('[PL v' + PL_VERSION + '] \u26d4 ' + nContra +
          ' contradictory lot(s) excluded from feed');
    }
    bits.push('div: ' + (divRes.skipped ? 'OFF' : '+' + divRes.added +
        ' event(s)' + (divRes.failed.length ? ', fail ' +
        divRes.failed.join(',') : '')));
    bits.push(syncRes.ok ? syncRes.note : 'sync: ' + syncRes.note);
    var pxList = [];
    for (var pk in pxMissing) { if (pxMissing.hasOwnProperty(pk)) {
      pxList.push(pk); } }
    if (pxList.length) {
      bits.push('price pending: ' + pxList.join(',') +
                ' (run My_Portfolio refresh)');
    }
    var fxList = [];
    for (var fk in fxMissing) { if (fxMissing.hasOwnProperty(fk)) {
      fxList.push(fk); } }
    if (fxList.length) { bits.push('fx missing: ' + fxList.join(',')); }
    /* v1.4.1 status bits */
    var fgList = [];
    for (var gk in fxGuarded) { if (fxGuarded.hasOwnProperty(gk)) {
      fgList.push(gk + ' ' + fxGuarded[gk]); } }
    if (fgList.length) { bits.push('fx peg-guard: ' + fgList.join(',')); }
    if (nFeeZero) { bits.push('sell fees 0 at finalize: ' + nFeeZero); }
    /* v1.1.0 status bits */
    if (nFixed) { bits.push('fixed ' + nFixed + ' input cell(s)'); }
    var ccyList = [];
    for (var ck in ccyMissing) { if (ccyMissing.hasOwnProperty(ck)) {
      ccyList.push(ck); } }
    if (ccyList.length) {
      bits.push('ccy required: ' + ccyList.join(','));
    }
    if (plTt) {                                           /* v1.5.0 */
      bits.push('totals: closed ' + Math.round(plTt.realizedClosed) +
          ' \u00b7 active ' + Math.round(plTt.activeTot) +
          ' \u00b7 lifetime ' + Math.round(plTt.lifetime) + ' SAR');
    }
    bits.push(secs + 's');
    plStatus_(sheet, bits.join(' | '));
    Logger.log('[PL v' + PL_VERSION + '] done: ' + bits.join(' | '));
  } catch (e) {
    plStatus_(sheet, 'ERROR: ' + String(e).slice(0, 160));
    Logger.log('[PL v' + PL_VERSION + '] ERROR: ' + e);
  }
}

function plWriteNoteFlag_(sheet, lot, flag) {
  if (lot.note.indexOf(flag) !== -1) { return; }
  var v = lot.note ? lot.note + ' \u00b7 ' + flag : flag;
  sheet.getRange(lot.row, PLC.NOTE).setValue(v);
  lot.note = v;
}

/** v1.4.1: strip \u24d8 (informational) segments only; operator text kept. */
function plClearInfoFlag_(sheet, lot) {
  if (lot.note.indexOf('\u24d8') === -1) { return; }
  var parts = lot.note.split(' \u00b7 ');
  var kept = [];
  for (var i = 0; i < parts.length; i++) {
    var seg = plTrim_(parts[i]);
    if (!seg || seg.charAt(0) === '\u24d8') { continue; }
    kept.push(seg);
  }
  var v = kept.join(' \u00b7 ');
  sheet.getRange(lot.row, PLC.NOTE).setValue(v);
  lot.note = v;
}

function plClearNoteFlag_(sheet, lot) {
  if (lot.note.indexOf('\u26a0') === -1 &&
      lot.note.indexOf('\u26d4') === -1) { return; }   /* v1.2.1 */
  /* v1.1.0: strip EVERY \u26a0 segment — rows can now carry more than one
   * flag (bad cell + ccy), and the v1.0.0 regex pair only removed a
   * leading and a trailing one. Operator note text is preserved. */
  var parts = lot.note.split(' \u00b7 ');
  var kept = [];
  for (var i = 0; i < parts.length; i++) {
    var seg = plTrim_(parts[i]);
    if (!seg) { continue; }
    if (seg.charAt(0) === '\u26a0' ||
        seg.charAt(0) === '\u26d4') { continue; }      /* v1.2.1 */
    kept.push(seg);
  }
  var v = kept.join(' \u00b7 ');
  sheet.getRange(lot.row, PLC.NOTE).setValue(v);
  lot.note = v;
}

// ---------------------------------------------------------------------------
// Manual dividend entry (gap-filler; flows into the same audited total)
// ---------------------------------------------------------------------------

function plAddManualDividend() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var ui;
  try { ui = SpreadsheetApp.getUi(); } catch (e) {
    Logger.log('[PL v' + PL_VERSION + '] UI unavailable'); return;
  }
  var sheet = ss.getSheetByName(PL_SHEET);
  if (!sheet) { ui.alert('Ledger sheet absent \u2014 build it first.'); return; }
  var a1 = ui.prompt('Manual dividend', 'Symbol (e.g. 1050.SR):',
      ui.ButtonSet.OK_CANCEL);
  if (a1.getSelectedButton() !== ui.Button.OK) { return; }
  var sym = plTrim_(a1.getResponseText()).toUpperCase();
  var lots = plReadAllLots_(sheet);
  var candidates = [];
  for (var i = 0; i < lots.length; i++) {
    if (lots[i].sym === sym && plIsActive_(lots[i]) &&
        plLotComplete_(lots[i])) { candidates.push(lots[i]); }
  }
  if (!candidates.length) {
    ui.alert('No ACTIVE lot found for ' + sym +
             ' (dividends freeze on Inactive).');
    return;
  }
  var lot = candidates[0];
  if (candidates.length > 1) {
    var listing = [];
    for (var c = 0; c < candidates.length; c++) {
      listing.push('row ' + candidates[c].row + ' (buy ' +
          plDateKey_(candidates[c].bdate) + ', ' + candidates[c].sh + ' sh)');
    }
    var a2 = ui.prompt('Manual dividend',
        'Multiple active lots:\n' + listing.join('\n') +
        '\nEnter the ROW number:', ui.ButtonSet.OK_CANCEL);
    if (a2.getSelectedButton() !== ui.Button.OK) { return; }
    var rn = plNum_(a2.getResponseText());
    lot = null;
    for (var c2 = 0; c2 < candidates.length; c2++) {
      if (candidates[c2].row === rn) { lot = candidates[c2]; }
    }
    if (!lot) { ui.alert('Row not in the list \u2014 aborted.'); return; }
  }
  var a3 = ui.prompt('Manual dividend',
      'TOTAL amount received for this lot, in ' + (lot.ccy || 'native ccy') +
      ':', ui.ButtonSet.OK_CANCEL);
  if (a3.getSelectedButton() !== ui.Button.OK) { return; }
  /* v1.1.0: the prompt accepts Arabic-keyboard numerals too. */
  var amt = plNormalizeNumericText_(a3.getResponseText()).value;
  if (amt === null || !(amt > 0)) { ui.alert('Invalid amount.'); return; }
  var a4 = ui.prompt('Manual dividend',
      'Ex/receipt date (yyyy-mm-dd, blank = today):', ui.ButtonSet.OK_CANCEL);
  if (a4.getSelectedButton() !== ui.Button.OK) { return; }
  var dtxt = plTrim_(a4.getResponseText());
  var exKey = dtxt || plDateKey_(new Date());
  var logSheet = plDivLogSheet_(ss);
  logSheet.getRange(logSheet.getLastRow() + 1, 1, 1,
      PL_DIVLOG_HEADERS.length).setValues([[plNow_(), 'MANUAL', lot.sym,
      plDateKey_(lot.bdate), exKey, lot.sh > 0 ? amt / lot.sh : '',
      lot.sh, amt, lot.ccy || '', 'operator entry']]);
  Logger.log('[PL v' + PL_VERSION + '] MANUAL dividend ' + sym + ' ' + amt);
  refreshPortfolioLedger();
}

// ---------------------------------------------------------------------------
// Re-finalize one closed lot (v1.4.1; menu-callable)
// ---------------------------------------------------------------------------

/**
 * v1.4.1: the sanctioned way to redo a freeze. For ONE Inactive lot with
 * sell fields present: clears FX\u2192SAR + Realized, then runs the refresh,
 * which re-finalizes the row exactly once with the guarded table rate and
 * the current Sell Fees / frozen dividends. Never throws.
 */
function plRefinalizeRow() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var ui = null;
  try { ui = SpreadsheetApp.getUi(); } catch (eU) {}
  var sheet = ss.getSheetByName(PL_SHEET);
  if (!sheet) {
    if (ui) { try { ui.alert('Ledger sheet absent.'); } catch (eA0) {} }
    return '';
  }
  var rowNum = 0;
  if (ui) {
    var a = ui.prompt('Re-finalize closed lot',
        'Sheet ROW number of the Inactive lot. FX\u2192SAR and Realized are ' +
        'cleared, then the refresh re-finalizes it once with the current ' +
        'guarded FX and Sell Fees:', ui.ButtonSet.OK_CANCEL);
    if (a.getSelectedButton() !== ui.Button.OK) { return ''; }
    rowNum = plNum_(plTrim_(a.getResponseText()));
  }
  var msg = plRefinalizeCore_(sheet, rowNum);
  Logger.log('[PL v' + PL_VERSION + '] refinalize: ' + msg);
  if (ui) { try { ui.alert(msg); } catch (eA) {} }
  if (msg.indexOf('cleared') === 0) { refreshPortfolioLedger(); }
  return msg;
}

/** v1.4.1: validate + clear FX/Realized on one closed row -> message. */
function plRefinalizeCore_(sheet, rowNum) {
  try {
    if (!(rowNum >= PL_ROW_FIRST)) { return 'invalid row ' + rowNum; }
    var vals = sheet.getRange(rowNum, 1, 1, PL_HEADERS.length).getValues()[0];
    var normOn = plProp_(PL_PROP_INPUT_NORM).toLowerCase() !== 'off';
    var lot = plReadLot_(vals, rowNum, normOn);
    if (!lot.sym) { return 'row ' + rowNum + ' is empty'; }
    if (plIsActive_(lot)) {
      return lot.sym + ' row ' + rowNum + ' is Active \u2014 only Inactive ' +
          'lots re-finalize';
    }
    if (plPendingClose_(lot)) {
      return lot.sym + ' row ' + rowNum + ' is missing Sell Date/Price';
    }
    sheet.getRange(rowNum, PLC.FX).setValue('');
    sheet.getRange(rowNum, PLC.REAL).setValue('');
    return 'cleared FX\u2192SAR + Realized on ' + lot.sym + ' row ' + rowNum +
        ' \u2014 refresh re-finalizes it';
  } catch (e) { return 'ERROR ' + String(e).slice(0, 120); }
}

// ---------------------------------------------------------------------------
// Dividend fetch diagnostic (read-only probe; v1.1.0)
// ---------------------------------------------------------------------------

/**
 * v1.1.0: verbose one-symbol dividend probe. Hits each Yahoo host once and
 * reports HTTP code + chart.error + event count — confirms or refutes the
 * '429 on Google egress IPs' hypothesis for the RCI/BBD/NMM failures live.
 * Read-only: nothing is written to any sheet or log.
 */
function plDivDiag() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var ui = null;
  try { ui = SpreadsheetApp.getUi(); } catch (eU) {}
  var sym = 'RCI.US';
  if (ui) {
    var a = ui.prompt('Dividend fetch diagnostic',
        'Symbol to probe (e.g. RCI.US):', ui.ButtonSet.OK_CANCEL);
    if (a.getSelectedButton() !== ui.Button.OK) { return; }
    var t = plTrim_(a.getResponseText()).toUpperCase();
    if (t) { sym = t; }
  }
  var from = new Date(new Date().getTime() - 400 * 86400000);
  var p1 = Math.floor(from.getTime() / 1000);
  var p2 = Math.floor(new Date().getTime() / 1000) + 86400;
  var path = '/v8/finance/chart/' + encodeURIComponent(plYahooSymbol_(sym)) +   // v1.4.0
      '?period1=' + p1 + '&period2=' + p2 +
      '&interval=1mo&events=div&includePrePost=false';
  var lines = ['[PL v' + PL_VERSION + '] dividend probe: ' + sym +
               ' (13m lookback)'];
  for (var h = 0; h < PL_YH_HOSTS.length; h++) {
    var line = PL_YH_HOSTS[h].replace('https://', '') + ': ';
    try {
      var resp = UrlFetchApp.fetch(PL_YH_HOSTS[h] + path, {
        muteHttpExceptions: true, followRedirects: true,
        headers: { 'User-Agent': 'Mozilla/5.0 (TFB-Ledger)' }
      });
      var code = resp.getResponseCode();
      var parsed = plParseDividends_(resp.getContentText());
      line += 'HTTP ' + code +
          (parsed.ok ? ' · ' + parsed.list.length + ' event(s)' :
           ' · err: ' + (parsed.err || 'unparseable'));
    } catch (e) {
      line += 'THREW ' + String(e).slice(0, 100);
    }
    lines.push(line);
    if (h === 0) {
      try { Utilities.sleep(350); } catch (eS) {}
    }
  }
  lines.push('If both hosts show 429: Yahoo is rate-limiting Google ' +
      'egress IPs — durable fix is routing dividends via the TFB ' +
      'backend; use plAddManualDividend meanwhile.');
  var msg = lines.join('\n');
  Logger.log(msg);
  if (ui) { try { ui.alert(msg); } catch (eA) {} }
  return msg;
}

// ---------------------------------------------------------------------------
// Self-test (read-only)
// ---------------------------------------------------------------------------

function plSelfTest() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var report = ['[PL v' + PL_VERSION + '] self-test (read-only)'];
  var sheet = ss.getSheetByName(PL_SHEET);
  report.push(PL_SHEET + ': ' + (sheet ?
      (plTrim_(sheet.getRange(PL_ROW_HEADER, 1).getValue()) ===
       PL_HEADERS[0] ? 'v2 layout OK' : 'PRESENT but layout not v2 \u2014 ' +
       'run buildPortfolioLedgerLayout') : 'ABSENT'));
  if (sheet) {
    var lots = plReadAllLots_(sheet);
    var a = 0, cl = 0, pe = 0, cx = 0;                    /* v1.2.0 */
    for (var i = 0; i < lots.length; i++) {
      if (plContraOn_() && plContradiction_(lots[i])) { cx++; }
      else if (plIsActive_(lots[i])) { a++; }
      else if (plPendingClose_(lots[i])) { pe++; }
      else { cl++; }
    }
    report.push('lots: ' + lots.length + ' (' + a + ' active, ' + cl +
        ' closed, ' + pe + ' pending' +
        (cx > 0 ? ', \u26d4 ' + cx + ' CONTRADICTION' : '') + ')');
  }
  try {
    var mp = ss.getSheetByName(PL_MP_SHEET);
    if (mp) {
      var cols = plMpCols_(mp);
      report.push('My_Portfolio feeder cols: Symbol@' + cols.sym +
          ' Qty@' + cols.qty + ' AvgCost@' + cols.avg + ' Cost@' + cols.cost);
    } else { report.push('My_Portfolio: ABSENT'); }
  } catch (eC) { report.push('My_Portfolio: ' + eC); }
  var fx = plFxRates_(ss);
  var n = 0;
  for (var k in fx) { if (fx.hasOwnProperty(k)) { n++; } }
  report.push('fx rates: ' + n + ' (USD=' + (fx.USD || 'MISSING') + ')');
  var lg = ss.getSheetByName(PL_DIVLOG_SHEET);
  report.push(PL_DIVLOG_SHEET + ': ' + (lg ? (lg.getLastRow() - 1) +
      ' row(s)' : 'will be created'));
  var fixture = '{"chart":{"result":[{"events":{"dividends":{' +
      '"1750000000":{"amount":0.5,"date":1750000000},' +
      '"1758000000":{"amount":0.55,"date":1758000000}}}}]}}';
  var parsed = plParseDividends_(fixture);
  report.push('dividend parser: ' + (parsed.ok && parsed.list.length === 2 ?
      'OK (2 events)' : 'FAILED'));
  /* v1.1.0 fixtures */
  var nfx1 = plNormalizeNumericText_('64\u063273');
  var nfx2 = plNormalizeNumericText_('\u0666\u0664\u066b\u0667\u0663');
  var nfx3 = plNormalizeNumericText_('1,234.56');
  var nfx4 = plNormalizeNumericText_('abc');
  report.push('input normalizer: ' +
      ((nfx1.value === 64.73 && nfx2.value === 64.73 &&
        nfx3.value === 1234.56 && nfx4.value === null) ?
       'OK (64\u063273\u219264.73, arabic digits, grouping, bad-guard)' :
       'FAILED'));
  report.push('ccy suffix map: ' +
      ((plCcyFromSuffix_('NMM.US') === 'USD' &&
        plCcyFromSuffix_('1050.SR') === 'SAR' &&
        plCcyFromSuffix_('HSBA.L') === '') ?
       'OK (US\u2192USD, SR\u2192SAR, .L excluded)' : 'FAILED'));
  /* v1.4.1 fixtures */
  var pg1 = plFxGuard_('USD', 3.8090, true);
  var pg2 = plFxGuard_('USD', 3.7528, true);
  var pg3 = plFxGuard_('SGD', 2.9597, true);
  var pg4 = plFxGuard_('USD', 3.8090, false);
  report.push('fx peg guard: ' +
      ((pg1.guarded && pg1.rate === 3.75 && !pg2.guarded &&
        pg2.rate === 3.7528 && !pg3.guarded && !pg4.guarded) ?
       'OK (USD 3.8090\u21923.75, 3.7528 kept, SGD untouched, off=pass)' :
       'FAILED'));
  report.push('props: ' + PL_PROP_DISABLED + '=' +
      (plProp_(PL_PROP_DISABLED) || '(unset)') + ', ' + PL_PROP_DIV_MODE +
      '=' + (plProp_(PL_PROP_DIV_MODE) || '(unset=auto)') + ', ' +
      PL_PROP_INPUT_NORM + '=' +
      (plProp_(PL_PROP_INPUT_NORM) || '(unset=on)') + ', ' +
      PL_PROP_FX_PEG_GUARD + '=' +
      (plProp_(PL_PROP_FX_PEG_GUARD) || '(unset=on)'));
  var msg = report.join('\n');
  Logger.log(msg);
  try { SpreadsheetApp.getUi().alert(msg); } catch (e) {}
  return msg;
}
