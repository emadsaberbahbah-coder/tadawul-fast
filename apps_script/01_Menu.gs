/**
 * 01_Menu.gs
 * Emad Bahbah Financial Advisor — Tadawul Tracker Menu Bar
 * Version: 2.20.3
 *
 * v2.20.3 DECISION PAGES v2 — MENU-BAR ENTRY (2026-10-20)
 * --------------------------------------------------------------------------
 * A third top-level menu, '📊 Decision v2', built by dp2_addTopMenu(ui) from
 * 27_Decision_Pages_v2.gs (v1.2.0+): refresh Portfolio_Decision_v2 /
 * Top_10_Investments_v2 / both, open the tabs, dynamic-exit controls, the
 * hourly trigger and the self-test. Guarded by typeof, so the menu bar is
 * unchanged until that file is deployed; the Today-submenu items from
 * v2.20.2 stay. Version check 2.20.3. Nothing else changed from v2.20.2.
 *
 * v2.20.2 DECISION PAGES v2 HOOK (2026-10-18)
 * --------------------------------------------------------------------------
 * Today submenu gains the Decision Pages v2 items (Build both / Build
 * Portfolio_Decision_v2 / Build Top_10_Investments_v2 / Seed IBKR tabs /
 * Self-Test) through dp2_addMenuItems(menu) from 27_Decision_Pages_v2.gs,
 * guarded by typeof so the menu is unchanged until that file is deployed.
 * Version check 2.20.2. Nothing else changed from v2.20.1.
 *
 * v2.20.1 ADDITIVE UPDATE — TODAY SUBMENU, PORTFOLIO_DECISION PAGE, MODULE WIRING (2026-10-17)
 * --------------------------------------------------------------------------
 * Operator instruction: keep the menu exactly as shared (root title with the
 * operator's name, every page submenu with its full refresh / design items)
 * and add only what the 2026-10-17 review found missing. v2.20.0 (a
 * restructured tree) was withdrawn before deployment; this build replaces it.
 *
 * ADDED (nothing removed, no label or handler of an existing item changed):
 *   A. '🧭 Today' submenu, FIRST in the root menu: Refresh Portfolio
 *      Decision, Refresh Decision Top 10, Morning Cockpit Refresh
 *      (tfbMorningCockpitRefresh), IBKR Evidence Status (PD_* properties +
 *      newest capture age vs the 14-minute limit), IBKR capture watch now,
 *      GitHub dispatch now / sync status / S-1 status, Trade Note log /
 *      score (25_Trade_Notes), Cash Ledger entries (22_Cash_Ledger via
 *      cashLedger_addMenuItems), Run Decision Diagnostics. Items that also
 *      exist on page submenus reuse the same handlers.
 *   B. '📄 Portfolio_Decision' submenu after My_Portfolio: Navigate, Refresh,
 *      Build Layout, Seed / Audit / Clean inputs, install / remove the IBKR
 *      capture watcher (pdInstallCaptureWatcher / pdRemoveCaptureWatcher),
 *      Self-Test: Portfolio Cockpit. The page had no submenu before.
 *   C. Configuration ▸ '📡 GitHub Dispatch' (install / remove sync and S-1
 *      dispatch triggers, probe) — 24_Sync_Dispatch had no UI.
 *   D. Tests: Self-Test Trade Notes, Sync Dispatch, Cash Ledger, and
 *      '🧪 Self-Check: Menu Wiring' (menuWiringSelfCheck_ — lists any
 *      referenced handler that does not resolve and the presence of the
 *      wired modules).
 *   E. Ownership: menuIsDecisionOwnedPage_ also treats Portfolio_Decision as
 *      decision-owned (MENU_DECISION_OWNED_EXTRA_) ahead of the 00_Config
 *      registry change; menuDecisionOwnedAction_ is page-aware (Portfolio →
 *      10_My_Portfolio targets, Top 10 → 16_Decision_Top10 targets).
 *   F. Verify Manual Priority Wiring: menu version check 2.20.1; the
 *      auto-refresh alignment check accepts 13_AutoRefresh 1.10.1 (live) as
 *      well as 1.10.0 — it was reporting FAIL on the live project.
 *   G. 24 new wrapper functions, all lazily resolved through
 *      menuCallFirstAvailable_ (a missing module shows a deploy hint, never a
 *      broken menu). Root submenus 15 → 17.
 *
 * VERSION NOTE: the previous header said 2.19.0 while SCRIPT_MENU_VERSION
 * stayed '2.18.0'; both now read 2.20.1.
 * DEPLOY: replace this file, save, reload the spreadsheet tab (or Utilities →
 * Reload Menu); run Tests → Self-Check: Menu Wiring once.
 * ROLLBACK: paste the retained v2.18.0 file back.
 *
 * v2.19.0 CONFIGURATION MENU REGROUPED — BOTTOM ITEMS WERE UNREACHABLE
 * --------------------------------------------------------------------------
 * DEFECT (live, 2026-08-05): the operator could not find
 * 'Build _Lists_Config Defaults'. It was present and correctly wired the
 * whole time — it was simply item #32 of 33 in a FLAT submenu. Google
 * Sheets renders a long custom menu inside a scroll region whose arrow is
 * easy to miss, so the last few entries are effectively invisible. The
 * consequence was real: _Lists_Config was never built, TFB_FX_LOOKUP stayed
 * empty, and 37 Top_10 candidates in 13 currencies sat blocked on the FX
 * gate (ARS CLP DKK IDR MXN MYR NOK NZD PLN SEK SGD THB TWD — exactly the
 * set absent from opportunity_builder's FX_STATIC_TO_SAR fallback).
 *
 * FIX: buildConfigurationMenu_ groups the SAME 33 items into six named
 * sub-menus along the separators the file already used, so nothing sits
 * deeper than two hovers and no list exceeds nine entries:
 *
 *   🧱 Decision Cockpit ....  2  (FIRST — this is the buried pair)
 *   🔐 Connection & Defaults  5
 *   ⚡ Auto-Refresh .......  7
 *   📅 Schedule & Audit ...  3
 *   🔬 Verify & Inspect ...  7
 *   🧪 Diagnostics & Audits  9
 *                          ---
 *                           33  (unchanged)
 *
 * ZERO REMOVALS. Every label is byte-identical to v2.18.0 and every item
 * keeps its exact handler name, so no wiring, no onOpen behaviour and no
 * other menu changes. Only the tree shape differs. This is presentation
 * only — nothing here touches data, sync, or the backend.
 *
 * DEPLOY: replace this file, save, then reload the spreadsheet tab (the
 * menu is built by onOpen; an open tab keeps the old tree until reloaded).
 *
 * v2.18.0 TFB SYNC HOLD — CROSS-LAYER MANUAL-HOLD SURFACE (2026-08-02)
 * --------------------------------------------------------------------------
 * Operator decision: single-file integration (17_Manual_Hold retired before
 * ever shipping) — one onOpen, zero collision risk. The hold is the bridge
 * between manual in-sheet work and the GitHub-Actions Python sync: this file
 * WRITES the signal cell; run_dashboard_sync.py v6.32.0 READS it (startup
 * defer + per-task skip; a page mid-write always completes; expiry clamped
 * to +12h server-side so a forgotten hold cannot deadlock automation).
 * Resume is OLDEST-FIRST on the Python side — after release, the next
 * automatic run continues from the least-recently-refreshed rows, never
 * from the beginning.
 *
 * A. NEW top-level menu '⏸️ TFB Sync Hold' (buildSyncHoldMenu_, called at
 *    the end of buildMenu_): Hold 2h / Hold 4h / Release / Show Status.
 *    A SECOND small top-level menu, deliberately NOT a submenu of the root
 *    — hold/release must be one click when a sync is about to collide.
 * B. CELL CONTRACT (must match the Python reader verbatim): tab
 *    `_Sync_Control`, key cell in column A reading 'Manual Hold Until'
 *    (searched in A1:A6), value in column B = ISO-8601 UTC expiry
 *    ('yyyy-MM-ddTHH:mm:ssZ'). tfbHoldCell_ creates + hides the tab on
 *    first use. Blank / past / unparsable / missing tab => NO hold on the
 *    Python side (fail-open).
 * C. HOUSE-STYLE WRAPPERS: menuTfbHold2h_/4h_/Release_/Status_ via
 *    invokeMenuAction_ (skipValidation:true — pure sheet-local writes, no
 *    backend call; showSuccess:false — the core functions toast their own
 *    result). Status reads the cell and reports raw value + active verdict
 *    without touching it.
 * D. WIRING CHECK UPDATED: menuVerifyManualPriorityWiring_'s menu-version
 *    assertion moves 2.17.0 -> 2.18.0 (it would otherwise report FAIL on
 *    a healthy deploy) + one new check that the hold surface is present.
 *
 * COUNTS: 193 of 193 v2.17.0 functions carried verbatim (0 removals);
 * 7 added (buildSyncHoldMenu_, 4 wrappers, tfbSetHold_, tfbReleaseHold_,
 * tfbHoldCell_ — Status logic lives in its wrapper) → 200. Root-level
 * menus 1 → 2. DEPLOY: replace this file, delete 17_Manual_Hold if
 * present, save, reload the spreadsheet tab; approve the authorization
 * prompt on first hold click. The Python reader ships in the pending
 * five-file main commit — until it lands, holds are written but not yet
 * honored by the Actions legs.
 *
 * v2.17.0 MANUAL REFRESH PRIORITY MENU WIRING (2026-07-30)
 * --------------------------------------------------------------------------
 * Every menu path that starts a data refresh now announces manual intent
 * through 02_Core.gs v2.11.1 BEFORE waiting for DocumentLock. Automatic
 * batches in 05_Refresh.gs v1.18.0 yield only after a completed write and
 * saved checkpoint; 13_AutoRefresh.gs v1.10.0 then resumes normally.
 *
 * COVERED: current-page build/full/row/batch refresh; every page submenu;
 * Refresh All Pages; Refresh Analysis & Insights; Top-10 cockpit build/refresh;
 * Portfolio Decision build/refresh; Portfolio Ledger build/refresh; and
 * Manual Refresh & Verify. Prompt-based dividend/reconcile/cleanup operations
 * are deliberately not wrapped, so no DocumentLock is held while waiting for
 * operator input. Two operator controls were added: priority status and an
 * emergency stale-state clear. No menu item was removed.
 *
 * Aligned with — July 2026 current dashboard contract
 * - 00_Config.gs          v1.12.6 (decision-layer wiring)
 * - 02_Core.gs            v2.11.1 (manual priority + re-entrant DocumentLock)
 * - 03_Schema.gs          v2.5.0
 * - 04_Format.gs          v2.16.0
 * - 05_Refresh.gs         v1.18.0 (safe cooperative manual-priority yield)
 * - 07_Global_Markets.gs  v1.7.1
 * - 10_My_Portfolio.gs    v1.5.9
 * - 11_SpecialPages.gs    v1.6.0
 * - 12_Diagnostics.gs     v1.7.0
 * - 13_AutoRefresh.gs     v1.10.0 (manual-priority preflight + cockpit lock)
 * - 14_Portfolio_PnL.gs   v2.5.0 (ledger-aware cost basis)
 * - 16_Decision_Top10.gs  v1.3.1
 * - 18_Decision_Diagnostics.gs v1.3.0
 * - 20_Portfolio_Reconcile.gs v2.0.0
 * - 21_Portfolio_Ledger.gs v1.1.0 (input normalize + Ccy + plDivDiag)
 * - 99_Data_Audit.gs      v1.3.0
 * - 99_Diag.gs            v2.3.0
 *
 * Current sheet-width contract:
 * - Main investment pages: 115 columns
 * - Top_10_Investments: 118 columns
 * - Insights_Analysis: 7 columns
 * - Data_Dictionary: 9 columns
 * - Final main-page column: Warnings
 * - Block Reason is an investability-gate field, not the final column.
 *
 * v2.16.0 DIAGNOSTIC SURFACES + SEPARATOR FIX (this build)
 * ----------------------------------------------------------------------------------
 * Two operator diagnostics shipped in the 2026-07-12 builds were reachable
 * only from the Apps Script editor; both are now menu items. One real
 * cosmetic defect fixed. Alignment header brought to the 2026-07-12 suite.
 * Zero removals; every v2.15.0 handler/wrapper is carried verbatim.
 *
 * A. DIVIDEND FETCH DIAGNOSTIC SURFACED. 21_Portfolio_Ledger.gs v1.1.0
 *    ships plDivDiag() — the read-only probe that fetches dividends for
 *    one symbol (default RCI.US) against both Yahoo chart hosts and
 *    reports per-host status codes, built to confirm or refute the
 *    Google-egress-IP 429 hypothesis behind the RCI/BBD/NMM auto-dividend
 *    failures. NEW wrapper menuLedgerDivDiag_ (lazy-resolve, hint names
 *    21 v1.1.0 — the minimum version that HAS the probe; skipValidation
 *    — provider fetch only, no backend page call; showSuccess:false —
 *    the probe reports through its own dialog/log). Surfaced on BOTH
 *    ledger surfaces per the v2.15.0 dual-surface pattern: the
 *    '📒 Portfolio Ledger' submenu and the My_Portfolio extras, directly
 *    after 'Add Manual Dividend…' on each.
 *
 * B. CORE SELF-VERIFY SURFACED. 02_Core.gs v2.9.1 repaired
 *    verifyCoreVersion_ (the constant + expected value had been frozen at
 *    2.8.0 through two releases, and the concurrency guards were never
 *    checked); the verify is now meaningful and belongs next to its
 *    siblings. NEW wrapper menuVerifyCoreVersion_ under Configuration,
 *    directly after 'Verify Format Version' — result dialog mirrors
 *    menuVerifyConfigVersion_ (ok/expected/actual + checks count + names
 *    of any failing checks + pointer to Executions). Hint names 02_Core
 *    v2.9.1 — earlier versions HAVE verifyCoreVersion but it lies, so the
 *    minimum-useful version is the honest one.
 *
 * C. DOUBLE SEPARATOR FIXED. buildQuickActionsMenu_ chained
 *    .addSeparator().addSeparator() between the global block and the
 *    current-page block — rendered as a doubled divider line. Collapsed
 *    to one. Menu structure otherwise unchanged.
 *
 * D. ALIGNMENT HEADER (doc only). 02_Core v2.9.0 → v2.9.1 (self-verify
 *    integrity), 05_Refresh v1.13.0 → v1.14.0 (keep-last-good row
 *    withhold), 13_AutoRefresh → v1.9.1, 14_Portfolio_PnL → v2.5.0 live
 *    (ledger-aware cost basis), 21_Portfolio_Ledger v1.0.0 → v1.1.0
 *    (input normalization + Ccy inference + dividend retry ladder +
 *    plDivDiag). Existing deploy-hint version strings inside wrappers
 *    are deliberately NOT bumped: each states the MINIMUM version that
 *    provides the target function, which remains true.
 *
 * COUNTS: 191 of 191 v2.15.0 functions carried verbatim (0 removals);
 * 2 added (items A + B) → 193. Ledger submenu items 6 → 7; My_Portfolio
 * extras 11 → 12; Configuration items +1. Root submenus unchanged at 15.
 * DEPLOY: paste after 21_Portfolio_Ledger.gs v1.1.0 and 02_Core.gs v2.9.1
 * are live (clicking the new items earlier just shows their deploy hints);
 * the menu rebuilds on the next spreadsheet open (or run onOpen once /
 * Utilities → Reload Menu).
 *
 * v2.15.0 LEDGER PAGE SURFACED + LAST 17 SURFACE RETIRED
 * ----------------------------------------------------------------------------------
 * Operator request: surface _Portfolio_CostBasis for refresh/update as a
 * first-class menu page. The ledger is NOT an engine page (no 115-column
 * contract; its layout is owned by 21_Portfolio_Ledger.gs), so it must NOT
 * join getSupportedPages / buildPageSubMenu_ — the generic engine actions
 * (Build / Header / Format / Refresh…) would overwrite the ledger cockpit,
 * the same protection rationale as the decision-owned pages. It gets its
 * OWN submenu of ledger-native actions only:
 *
 * A. NEW '📒 Portfolio Ledger' submenu (buildPortfolioLedgerMenu_) placed
 *    directly after the My_Portfolio page submenu: Navigate (NEW wrapper
 *    menuLedgerNavigate_ — plain sheet activation via the local
 *    navigateToPageByName_, so it works even before 21 is pasted) + the
 *    five existing ledger wrappers REUSED BY NAME (Build Layout / Refresh /
 *    Sync from Ledger / Add Manual Dividend / Self-Test). The same items
 *    REMAIN under the My_Portfolio extras — identical handlers, two
 *    surfaces, zero duplicated logic.
 *
 * B. LAST 17 SURFACE REMOVED. Tests → '🧪 Self-Test: Selection Log'
 *    targeted 17_Selection_Log's slSelfTest; with 17 deleted the click
 *    could only show a hint instructing the operator to re-deploy the
 *    retired file. Item + wrapper (menuDecisionSelfTestLog_) removed —
 *    the ONLY function removal in this build. Selection logging is owned
 *    and self-tested by 16_Decision_Top10 v1.3.1's cockpit self-test.
 *
 * C. HINT/HEADER ACCURACY (cosmetic). The eight auto-refresh deploy hints
 *    named '09_AutoRefresh.gs'; the module in this project is
 *    13_AutoRefresh.gs — hint strings corrected (candidate-name resolution
 *    unchanged). Alignment header brought up to the live July-2026 suite
 *    (02_Core v2.9.0, 05_Refresh v1.13.0, 10_My_Portfolio v1.5.9,
 *    16_Decision_Top10 v1.3.1, 18_Decision_Diagnostics v1.3.0;
 *    14_Portfolio_PnL v2.4.0 live with the v2.5.0 rebuild queued).
 *
 * COUNTS: 189 of 190 v2.14.0 functions carried verbatim (1 removal, item
 * B); 2 added (item A) → 191. Tests items 6 → 5; root submenus 14 → 15.
 * DEPLOY: paste any time after 21_Portfolio_Ledger.gs is live; the menu
 * rebuilds on the next spreadsheet open (or run onOpen once / Utilities →
 * Reload Menu). Then delete 17_Selection_Log.gs (triggers → items → file).
 *
 * v2.14.0 LEDGER SURFACE + 17 RETIREMENT (this build)
 * ----------------------------------------------------------------------------------
 * Four coordinated menu changes for the 21_Portfolio_Ledger.gs v1.0.0 rollout
 * and the retirement of 17_Selection_Log.gs. All other handlers/wrappers are
 * verbatim v2.13.0.
 *
 * A. 17_Selection_Log SURFACES REMOVED. Both audit trails are now written
 *    automatically by the cockpits that render the data: 16_Decision_Top10
 *    v1.3.1 appends _Selection_Log on every Refresh Decision Top 10, and
 *    10_My_Portfolio v1.5.9 appends _Portfolio_Action_Log on every Refresh
 *    Portfolio Decision. The manual loggers are therefore superseded — and
 *    with 17 deleted from the project, their menu items would throw an error
 *    dialog on click (17's strict header validation was already broken
 *    against the 33-column log). Removed: the root '🧾 Log All Decisions'
 *    item, the Top_10 extras 'Log Top 10 Selections' entry, the My_Portfolio
 *    extras 'Log Portfolio Actions' entry, and their three wrappers
 *    (menuDecisionLogAll_ / menuDecisionLogTop10_ / menuDecisionLogPortfolio_
 *    — the ONLY function removals in this build).
 *
 * B. LEDGER-SYNC PREFERENCE REWIRE. menuPortfolioSyncLedger_ now resolves
 *    syncLedgerToPortfolio() (21_Portfolio_Ledger: ACTIVE-lots-only upsert +
 *    prune + Position Cost) FIRST and falls back to 20_Portfolio_Reconcile's
 *    syncPortfolioLedger() only when 21 is absent — running 20's reconciler
 *    alongside a live 21 would put two owners on My_Portfolio membership.
 *    21's silent result note is toasted (guarded on its return signature) so
 *    the operator gets feedback without 20's double-dialog risk. Combined
 *    deploy hint when neither file is present.
 *
 * C. LEDGER ENTRY POINTS SURFACED. Four new wrappers + extras under the
 *    My_Portfolio submenu: '📒 Build Portfolio Ledger Layout'
 *    (menuLedgerBuild_), '♻️ Refresh Portfolio Ledger'
 *    (menuLedgerRefresh_ — success modal points at the ledger's own B2
 *    status line), '💰 Add Manual Dividend…' (menuLedgerAddDividend_),
 *    '🧪 Self-Test: Ledger' (menuLedgerSelfTest_). Same lazy-resolve
 *    pattern as every decision wrapper; build/dividend/self-test run their
 *    own dialogs (showSuccess:false); all skipValidation:true (sheet-local).
 *
 * D. SEED MESSAGE CORRECTED. The 'Seed Portfolio Inputs' success text stated
 *    the data direction backwards; it now reads _Portfolio_CostBasis →
 *    My_Portfolio and notes the ledger-first routing (14_Portfolio_PnL
 *    v2.5.0 delegates to 21 when installed).
 *
 * COUNTS: 186 of 189 v2.13.0 functions carried verbatim (the 3 removals in
 * item A), 4 added (item C) → 190. My_Portfolio extras 7 → 11; Top_10
 * extras 4 → 3. Alignment header: 14 → v2.5.0, + 21 v1.0.0.
 * DEPLOY: paste after 21/18/10/14 are live, then delete 17_Selection_Log.gs;
 * the menu rebuilds on the next spreadsheet open (or run onOpen once).
 *
 * v2.13.0 LEDGER SYNC EXPOSED (this build)
 * ----------------------------------------------------------------------------------
 * 20_Portfolio_Reconcile.gs v2.0.0 introduces syncPortfolioLedger() — the ONLY
 * operation that adds/removes My_Portfolio holdings from the ledger (refresh
 * re-prices EXISTING rows only; deletions always require an explicit YES in the
 * function's own confirmation dialog). Until now it was invocable only from the
 * Apps Script editor. This build surfaces it in the sheet UI:
 *   - NEW wrapper menuPortfolioSyncLedger_ — invokeMenuAction_ +
 *     menuCallFirstAvailable_, so a clear deploy hint appears if
 *     20_Portfolio_Reconcile.gs is not pasted. showSuccess:false because the
 *     function runs its OWN confirm dialog + result toast (no double dialogs);
 *     skipValidation:true because the reconcile is sheet-local (no backend call).
 *   - Surfaced under the My_Portfolio submenu extras, between "Refresh Portfolio
 *     Decision" and "Seed Portfolio Inputs".
 * Alignment header refreshed to the July 2026 suite (02_Core v2.8.1, 05_Refresh
 * v1.12.7, 14_Portfolio_PnL v2.4.0, 20_Portfolio_Reconcile v2.0.0). Every other
 * handler/wrapper in this file is verbatim v2.12.0.
 *
 * v2.12.0 DECISION-OWNED REDIRECT (this build) — fixes "Action failed:
 *   Refresh All Data Top_10_Investments"
 * ----------------------------------------------------------------------------------
 * Until now, menuPageAction_ THREW for a decision-owned page's engine action
 * (build / header / format / refresh / refreshRow / refreshBatches / sort). That
 * guard correctly protected the 16_Decision_Top10.gs cockpit layout, but a throw
 * is surfaced by invokeMenuAction_ as a red "Action failed" dialog and re-thrown,
 * so clicking 🔄 Refresh All Data on the Top_10_Investments submenu (and any other
 * blocked action) aborted instead of doing the right thing.
 *
 * FIX: menuPageAction_ now REDIRECTS the blocked actions to their cockpit
 * equivalents instead of throwing, via the new helper menuDecisionOwnedAction_:
 *   - build / header / format -> buildDecisionTop10Layout()  (rebuilds the cockpit
 *     structure + its own styling; the cockpit has no separate header/format pass)
 *   - refresh                 -> refreshDecisionTop10()       (re-pulls cockpit data)
 *   - refreshRow / refreshBatches / sort -> graceful skip (returns an ok result with
 *     status 'skipped_decision_owned'); the cockpit is a fixed, wholly-rebuilt block,
 *     so per-row / batch / sort have no meaning — skip, never abort.
 * navigate / validate / diagnose still fall through to the normal read-only handlers.
 * Only Top_10_Investments is decision-owned (isDecisionOwnedPage_ returns true for it
 * alone; Portfolio_Decision is a separate sheet, not in getSupportedPages, so
 * menuPageAction_ is never invoked for it), so routing the redirect to the Top 10
 * cockpit functions is exact. The redirect reuses menuCallFirstAvailable_, so a clear
 * deploy hint still appears if 16_Decision_Top10.gs is missing. DECISION_BLOCKED_ACTIONS_
 * and menuIsDecisionOwnedPage_ are unchanged; every other handler/wrapper is verbatim.
 *
 * v2.11.0 DECISION ACTIONS DISTRIBUTED PER PAGE (this build)
 * ----------------------------------------------------------------------------------
 * The combined "Decision" submenu is dissolved. Each PAGE now owns its own
 * decision actions, so the menu reads page-by-page with no combined bucket:
 *   - Top 10 decision actions    -> under the Top_10_Investments submenu
 *     (Build Cockpit, Refresh Decision, Log Selections, Self-Test).
 *   - Portfolio decision actions -> under the My_Portfolio submenu
 *     (Build / Refresh Portfolio Decision, Seed / Audit / Clean inputs,
 *      Log Portfolio Actions, Self-Test).
 *   - Genuinely cross-page items fold into the existing global menus:
 *       Quick Actions  -> Log All Decisions (Top 10 + Portfolio)
 *       Configuration  -> Build _Lists_Config Defaults, Run Decision Diagnostics
 *       Tests          -> Self-Test: Selection Log, Self-Test: Decision Diagnostics
 * No handler/wrapper logic changed - every menuDecision*_ wrapper is reused by
 * name; only WHERE each is surfaced moved. buildDecisionMenu_() is removed (it
 * was the only caller of the combined submenu). menuPageAction_'s decision-owned
 * block message now points to the page's own Decision items instead of the
 * removed submenu. The DECISION-OWNED PAGE ENFORCEMENT guard is unchanged.
 *
 * v2.10.0 DECISION-LAYER WIRING (lockstep with 00_Config v1.12.6 + Plan v5.0 suite)
 * ----------------------------------------------------------------------------------
 * The decision cockpit suite is live: 15_Lists_Config v1.0.0, 16_Decision_Top10
 * v1.0.0, 10_My_Portfolio v1.5.0 (Portfolio_Decision layer), 14_Portfolio_PnL
 * v2.1.0, 17_Selection_Log v1.0.0, 18_Decision_Diagnostics v1.0.0.
 *
 * DECISION-OWNED PAGE ENFORCEMENT. Top_10_Investments is RENDERED BY
 * 16_Decision_Top10.gs (cockpit layout); the legacy 118-column engine actions
 * (build / header / format / refresh / refreshRow / refreshBatches / sort) would
 * overwrite or scramble the cockpit. menuPageAction_ - the single dispatch choke
 * point for every page action in this file - consults isDecisionOwnedPage_
 * (00_Config v1.12.6, via typeof guard so deploy order cannot break the menu)
 * and blocks those actions with a redirect. navigate / validate / diagnose
 * remain allowed (read-only). NOTE: "Refresh All Pages" iterates inside
 * 05_Refresh.gs; its skip of decision-owned pages lands in the 05_Refresh
 * revision.
 *
 * v2.9.0 MAIN UPDATES
 * -------------------
 * 1. Replaces old v5.74 / 106 / 109 / Global_Markets 46-column menu
 *    assumptions with the current 115/118 schema contract.
 * 2. Adds direct menu exposure for trim, audit, format verification,
 *    full diagnostics, and data-audit functions.
 * 3. Uses menuGetActivePageName_() instead of getActivePageName_() to
 *    avoid a global-name collision with 05_Refresh.gs.
 * 4. Keeps all existing page actions and adds safe fallbacks when a
 *    page-specific function is not available.
 */
'use strict';
var SCRIPT_MENU_VERSION = '2.20.3';
var ROOT_MENU_TITLE_ = '👤 Emad Bahbah Financial Advisor';
/* =========================================================
 * Lifecycle
 * ========================================================= */
function onOpen() {
  buildMenu_();
  try {
    if (typeof tfbAutoResumeOnOpen_ === 'function') {
      tfbAutoResumeOnOpen_();
    }
  } catch (autoResumeErr) {}
}
function onInstall() {
  buildMenu_();
}
/* =========================================================
 * Menu builder
 * ========================================================= */
function buildMenu_() {
  var ui = SpreadsheetApp.getUi();
  var menu = ui.createMenu(ROOT_MENU_TITLE_);
  menu
    .addSubMenu(buildTodayMenu_())
    .addSeparator()
    .addSubMenu(buildQuickActionsMenu_())
    .addSeparator()
    .addSubMenu(buildActivePageMenu_())
    .addSeparator()
    .addSubMenu(buildPageSubMenu_(SHEET_NAMES.MARKET_LEADERS, getPageMenuHandlers_(SHEET_NAMES.MARKET_LEADERS)))
    .addSubMenu(buildPageSubMenu_(SHEET_NAMES.GLOBAL_MARKETS, getPageMenuHandlers_(SHEET_NAMES.GLOBAL_MARKETS), getGlobalMarketsExtras_()))
    .addSubMenu(buildPageSubMenu_(SHEET_NAMES.COMMODITIES_FX, getPageMenuHandlers_(SHEET_NAMES.COMMODITIES_FX)))
    .addSubMenu(buildPageSubMenu_(SHEET_NAMES.MUTUAL_FUNDS, getPageMenuHandlers_(SHEET_NAMES.MUTUAL_FUNDS)))
    .addSubMenu(buildPageSubMenu_(SHEET_NAMES.MY_PORTFOLIO, getPageMenuHandlers_(SHEET_NAMES.MY_PORTFOLIO), getMyPortfolioDecisionExtras_()))
    .addSubMenu(buildPortfolioDecisionMenu_())
    .addSubMenu(buildPortfolioLedgerMenu_())
    .addSubMenu(buildPageSubMenu_(SHEET_NAMES.INSIGHTS_ANALYSIS, getPageMenuHandlers_(SHEET_NAMES.INSIGHTS_ANALYSIS)))
    .addSubMenu(buildPageSubMenu_(SHEET_NAMES.TOP_10_INVESTMENTS, getPageMenuHandlers_(SHEET_NAMES.TOP_10_INVESTMENTS), getTop10DecisionExtras_()))
    .addSubMenu(buildPageSubMenu_(SHEET_NAMES.DATA_DICTIONARY, getPageMenuHandlers_(SHEET_NAMES.DATA_DICTIONARY)))
    .addSeparator()
    .addSubMenu(buildSymbolsMenu_())
    .addSubMenu(buildConfigurationMenu_())
    .addSubMenu(buildTestsMenu_())
    .addSubMenu(buildUtilitiesMenu_())
    .addToUi();
  /* v2.18.0: second top-level menu — hold/release one click away. */
  buildSyncHoldMenu_(ui);
  /* v2.20.3: third top-level menu — Decision Pages v2 (27_Decision_Pages_v2.gs v1.2.0+), only when deployed. */
  if (typeof dp2_addTopMenu === 'function') {
    try { dp2_addTopMenu(ui); } catch (dp2TopErr) {}
  }
}
/* v2.18.0: TFB Sync Hold — the operator side of the cross-layer manual-hold
 * bridge (cell contract in the header). Kept top-level, not under the root
 * menu, so it is reachable in one click when a collision is imminent. */
function buildSyncHoldMenu_(ui) {
  ui.createMenu('⏸️ TFB Sync Hold')
    .addItem('✋ Hold Auto-Sync 2 Hours', 'menuTfbHold2h_')
    .addItem('✋ Hold Auto-Sync 4 Hours', 'menuTfbHold4h_')
    .addItem('▶️ Release Hold Now', 'menuTfbReleaseHold_')
    .addItem('📟 Show Hold Status', 'menuTfbHoldStatus_')
    .addToUi();
}
function buildQuickActionsMenu_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('🚀 Quick Actions')
    .addItem('🔄 Refresh All Pages', 'menuRefreshAllPages_')
    .addItem('📊 Refresh Analysis & Insights', 'menuRefreshAnalysisAndInsights_')
    .addItem('🧱 Ensure All Sheets & Headers', 'menuEnsureAllSheetsAndHeaders_')
    .addItem('🧹 Trim All Sheets To Expected Columns', 'menuTrimAllSheetsToExpectedColumns_')
    .addItem('🎨 Reformat All Sheets', 'menuReformatAllSheets_')
    .addItem('🧾 Run Data Audit All Pages', 'menuAuditAllPages_')
    .addItem('🧪 Run Full Diagnostic Sweep', 'menuRunFullDiagnosticSweep_')
    .addSeparator()
    .addItem('🎨 Apply Format To Current Page', 'menuReformatCurrentPage_')
    .addItem('🧹 Trim Current Sheet To Expected Columns', 'menuTrimCurrentSheetToExpectedColumns_')
    .addItem('✅ Validate Current Page', 'menuValidateCurrentPage_')
    .addItem('🔬 Diagnose Headers (Current Page)', 'menuDiagnoseCurrentHeaders_');
}
function buildActivePageMenu_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('📍 Active Page')
    .addItem('🛠️ Build Current Page', 'menuBuildCurrentPage_')
    .addItem('📍 Navigate Current Page', 'menuNavigateCurrentPage_')
    .addItem('🧱 Build Current Header', 'menuUpdateCurrentHeaders_')
    .addItem('🧹 Trim Current Sheet To Expected Columns', 'menuTrimCurrentSheetToExpectedColumns_')
    .addItem('🎨 Apply Current Format', 'menuReformatCurrentPage_')
    .addItem('🔄 Refresh Current Full Data', 'menuRefreshCurrentPage_')
    .addItem('🎯 Refresh Selected Row', 'menuRefreshSelectedRowCurrent_')
    .addItem('🧩 Refresh Current Page In Batches', 'menuRefreshCurrentPageInBatches_')
    .addItem('🔁 Manual Refresh & Verify (Active)', 'menuManualRefreshAndVerifyCurrent_')
    .addItem('🔍 Audit Current Page Columns', 'menuAuditCurrentPageColumns_')
    .addItem('🔣 Show Current Page Symbols', 'menuShowCurrentPageSymbols_')
    .addItem('✅ Validate Current Page', 'menuValidateCurrentPage_')
    .addItem('📈 Sort & Rank Current Page', 'menuSortAndRankCurrentPage_')
    .addItem('🔬 Diagnose Headers', 'menuDiagnoseCurrentHeaders_');
}
function buildPageSubMenu_(pageName, handlers, extras) {
  var ui = SpreadsheetApp.getUi();
  var menu = ui.createMenu('📄 ' + pageName)
    .addItem('🛠️ Build Page', handlers.build)
    .addItem('📍 Navigate Page', handlers.navigate)
    .addItem('🧱 Build Header', handlers.header)
    .addItem('🎨 Apply Format', handlers.format)
    .addItem('🔄 Refresh All Data', handlers.refresh)
    .addItem('🎯 Refresh Selected Row', handlers.refreshRow)
    .addItem('🧩 Refresh in Batches', handlers.refreshBatches)
    .addItem('✅ Validate Page', handlers.validate)
    .addItem('📈 Sort & Rank Page', handlers.sort)
    .addItem('🔬 Diagnose Headers', handlers.diagnose);
  if (Array.isArray(extras) && extras.length) {
    menu.addSeparator();
    for (var i = 0; i < extras.length; i++) {
      if (extras[i] && extras[i].label && extras[i].handler) {
        menu.addItem(extras[i].label, extras[i].handler);
      }
    }
  }
  return menu;
}
function getGlobalMarketsExtras_() {
  return [
    { label: '🔧 Reapply Rank (Overall) Sheet Pass', handler: 'menuApplyRankOverallGlobalMarkets_' },
    { label: '🔧 Reapply View Tokens & Reco Override', handler: 'menuApplyViewTokensGlobalMarkets_' },
    { label: '🌍 Validate Global_Markets Gate', handler: 'menuValidate_Global_Markets_' },
    { label: '🌍 Audit Global_Markets', handler: 'menuAuditGlobalMarkets_' }
  ];
}
/* v2.15.0: the _Portfolio_CostBasis (Investment Ledger) page as a first-
 * class submenu. Deliberately NOT a buildPageSubMenu_ page: the ledger has
 * no 115-column engine contract and its layout is owned by
 * 21_Portfolio_Ledger.gs, so the generic Build/Header/Format/Refresh engine
 * actions would overwrite it (same protection rationale as the decision-
 * owned cockpit pages). Only ledger-native actions are surfaced; every
 * handler except the new Navigate wrapper is the existing v2.14.0 ledger
 * wrapper reused by name. */
function buildPortfolioLedgerMenu_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('📒 Portfolio Ledger')
    .addItem('📍 Navigate Ledger (_Portfolio_CostBasis)', 'menuLedgerNavigate_')
    .addItem('📒 Build Portfolio Ledger Layout', 'menuLedgerBuild_')
    .addItem('♻️ Refresh Portfolio Ledger', 'menuLedgerRefresh_')
    .addItem('🔗 Sync Portfolio from Ledger', 'menuPortfolioSyncLedger_')
    .addItem('💰 Add Manual Dividend…', 'menuLedgerAddDividend_')
    .addItem('🩺 Dividend Fetch Diagnostic', 'menuLedgerDivDiag_')
    .addItem('🧪 Self-Test: Ledger', 'menuLedgerSelfTest_');
}
function buildSymbolsMenu_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('🔣 Symbols')
    .addItem('🔍 Show Active Page Symbols', 'menuShowCurrentPageSymbols_')
    .addItem('⚙️ Manage Active Page Symbols', 'menuManageCurrentPageSymbols_')
    .addItem('🧪 Test Active Page API With Symbols', 'menuTestCurrentPageApi_');
}
function buildConfigurationMenu_() {
  /* v2.19.0 — same 33 items, same handlers, grouped so none is buried.
     Decision Cockpit is FIRST because it holds the pair that was unreachable
     at the bottom of the old flat list. */
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('⚙️ Configuration')
    .addSubMenu(buildConfigDecisionMenu_())
    .addSeparator()
    .addSubMenu(buildConfigConnectionMenu_())
    .addSubMenu(buildConfigAutoRefreshMenu_())
    .addSubMenu(buildConfigScheduleAuditMenu_())
    .addSubMenu(buildConfigDispatchMenu_())
    .addSeparator()
    .addSubMenu(buildConfigVerifyMenu_())
    .addSubMenu(buildConfigDiagnosticsMenu_());
}

/* v2.19.0 — Configuration sub-menus. Labels and handlers are byte-identical
   to the v2.18.0 flat list; only the grouping is new. */

function buildConfigDecisionMenu_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('🧱 Decision Cockpit')
    .addItem('🧱 Build _Lists_Config Defaults', 'menuDecisionBuildListsConfig_')
    .addItem('🩺 Run Decision Diagnostics', 'menuDecisionDiagnostics_');
}

function buildConfigConnectionMenu_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('🔐 Connection & Defaults')
    .addItem('🔐 Set / Update API Token', 'menuSetApiToken_')
    .addItem('🌐 Set / Update Backend URL', 'menuSetBaseUrl_')
    .addItem('⏱️ Set Default Timeout', 'menuSetDefaultTimeout_')
    .addItem('📦 Set Default Limit', 'menuSetDefaultLimit_')
    .addItem('🧭 Set Schema Check Mode', 'menuSetSchemaCheckMode_');
}

function buildConfigAutoRefreshMenu_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('⚡ Auto-Refresh')
    .addItem('⚡ Install Auto-Refresh Trigger', 'menuInstallAutoRefreshTrigger_')
    .addItem('🛑 Remove Auto-Refresh Trigger', 'menuRemoveAutoRefreshTrigger_')
    .addItem('▶️ Fire Auto-Refresh Now', 'menuFireAutoRefreshNow_')
    .addItem('📊 Show Auto-Refresh Status', 'menuShowAutoRefreshStatus_')
    .addItem('🖐️ Show Manual Refresh Priority', 'menuShowManualRefreshPriorityStatus_')
    .addItem('🧯 Clear Manual Refresh Priority (Emergency)', 'menuClearManualRefreshPriorityEmergency_')
    .addItem('🔁 Toggle Auto-Resume On Open', 'menuToggleAutoResumeOnOpen_');
}

function buildConfigScheduleAuditMenu_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('📅 Schedule & Audit')
    .addItem('📅 Configure Auto-Refresh Schedule', 'menuConfigureAutoStartSchedule_')
    .addItem('🛡️ Toggle Audit After Refresh', 'menuToggleAuditAfterRefresh_')
    .addItem('🔍 Run Completeness Audit Now', 'menuRunCompletenessAuditNow_');
}

function buildConfigVerifyMenu_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('🔬 Verify & Inspect')
    .addItem('📄 Show Configuration Summary', 'menuShowConfigSummary_')
    .addItem('✅ Validate Configuration', 'menuValidateConfiguration_')
    .addItem('🔬 Verify Config Version', 'menuVerifyConfigVersion_')
    .addItem('🎨 Verify Format Version', 'menuVerifyFormatVersion_')
    .addItem('🧬 Verify Core Version', 'menuVerifyCoreVersion_')
    .addItem('🩺 Run Phase 1 Verification', 'menuRunPhase1Verification_')
    .addItem('📋 Show Header Mapping', 'menuShowHeaderMapping_');
}

function buildConfigDiagnosticsMenu_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('🧪 Diagnostics & Audits')
    .addItem('🧹 Trim All Sheets To Expected Columns', 'menuTrimAllSheetsToExpectedColumns_')
    .addItem('🧪 Run Full Diagnostic Sweep', 'menuRunFullDiagnosticSweep_')
    .addItem('🔎 Verify 115/118 Schema Propagation', 'menuVerifyV28SchemaPropagation_')
    .addItem('🗺️ Diagnose Schema Contract Status', 'menuDiagnoseSchemaContractStatus_')
    .addItem('🧾 Run Data Audit All Pages', 'menuAuditAllPages_')
    .addItem('🌍 Audit Global_Markets', 'menuAuditGlobalMarkets_')
    .addItem('🏆 Audit Top_10_Investments', 'menuAuditTop10Investments_')
    .addItem('🎯 Diagnose Criteria Visibility', 'menuDiagnoseV310CriteriaVisibility_')
    .addItem('🚨 Diagnose Data Quality Alerts', 'menuDiagnoseDataQualityAlerts_');
}
function buildTestsMenu_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('🧪 Tests')
    .addItem('💓 Test Health', 'menuTestHealth_')
    .addItem('🛰️ Test Provider Health', 'menuTestProviderHealth_')
    .addItem('🔍 Test Current Page API', 'menuTestCurrentPageApi_')
    .addItem('📡 Test Meta / OpenAPI', 'menuTestMetaAndOpenApi_')
    .addItem('🖐️ Verify Manual Priority Wiring', 'menuVerifyManualPriorityWiring_')
    .addSeparator()
    .addItem('🧪 Self-Test: Decision Diagnostics', 'menuDecisionSelfTestDiagnostics_')
    .addItem('🧪 Self-Test: Trade Notes', 'menuTradeNotesSelfTest_')
    .addItem('🧪 Self-Test: Sync Dispatch', 'menuSyncDispatchSelfTest_')
    .addItem('🧪 Self-Test: Cash Ledger', 'menuCashLedgerSelfTest_')
    .addItem('🧪 Self-Check: Menu Wiring', 'menuWiringSelfCheck_');
}
function buildUtilitiesMenu_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('🧰 Utilities')
    .addItem('🧹 Trim Current Sheet To Expected Columns', 'menuTrimCurrentSheetToExpectedColumns_')
    .addItem('🧹 Trim All Sheets To Expected Columns', 'menuTrimAllSheetsToExpectedColumns_')
    .addItem('🎨 Verify Format Version', 'menuVerifyFormatVersion_')
    .addItem('🎨 Reformat All Sheets', 'menuReformatAllSheets_')
    .addSeparator()
    .addItem('📤 Export Current Page CSV', 'menuExportCurrentPageCsv_')
    .addItem('📝 View Last Run Log', 'menuViewLastRunLog_')
    .addItem('📋 Show Header Mapping', 'menuShowHeaderMapping_')
    .addItem('🔁 Reload Menu', 'menuReloadMenu_')
    .addItem('ℹ️ About', 'menuAbout_');
}
/* =========================================================
 * Decision suite (Plan v5.0) — wrappers + ownership guard
 * (v2.11.0: the combined submenu builder is removed; the page-extra providers
 *  below feed each page's own submenu, and the cross-page items are wired into
 *  Quick Actions / Configuration / Tests. All wrappers are unchanged.)
 * ========================================================= */
/* Legacy page actions that would overwrite or scramble a decision-owned
 * cockpit sheet. navigate / validate / diagnose stay allowed (read-only). */
var DECISION_BLOCKED_ACTIONS_ = ['build', 'header', 'format', 'refresh', 'refreshRow', 'refreshBatches', 'sort'];
/* v2.20.1: pages the menu treats as decision-owned even before 00_Config's
 * DECISION_OWNED_PAGES_ lists them (Part A2 adds Portfolio_Decision there). */
var MENU_DECISION_OWNED_EXTRA_ = ['Portfolio_Decision'];
function menuIsDecisionOwnedPage_(pageName) {
  var s = String(pageName || '').trim().toLowerCase();
  if (!s) return false;
  for (var i = 0; i < MENU_DECISION_OWNED_EXTRA_.length; i++) {
    if (MENU_DECISION_OWNED_EXTRA_[i].toLowerCase() === s) return true;
  }
  try {
    if (typeof isDecisionOwnedPage_ === 'function') {
      return isDecisionOwnedPage_(pageName) === true;
    }
  } catch (ownErr) {}
  return false;
}
/* Page-extra providers: each page's decision actions live under that page's
 * own submenu (wired in buildMenu_ via buildPageSubMenu_'s `extras` arg). */
function getTop10DecisionExtras_() {
  return [
    { label: '🧭 Build Decision Cockpit Layout', handler: 'menuDecisionBuildTop10_' },
    { label: '🚀 Refresh Decision Top 10', handler: 'menuDecisionRefreshTop10_' },
    { label: '🧪 Self-Test: Top 10 Cockpit', handler: 'menuDecisionSelfTestTop10_' }
  ];
}
function getMyPortfolioDecisionExtras_() {
  return [
    { label: '🧭 Build Portfolio Decision Layout', handler: 'menuDecisionBuildPortfolio_' },
    { label: '🚀 Refresh Portfolio Decision', handler: 'menuDecisionRefreshPortfolio_' },
    { label: '📒 Build Portfolio Ledger Layout', handler: 'menuLedgerBuild_' },
    { label: '♻️ Refresh Portfolio Ledger', handler: 'menuLedgerRefresh_' },
    { label: '🔗 Sync Portfolio from Ledger', handler: 'menuPortfolioSyncLedger_' },
    { label: '💰 Add Manual Dividend…', handler: 'menuLedgerAddDividend_' },
    { label: '🩺 Dividend Fetch Diagnostic', handler: 'menuLedgerDivDiag_' },
    { label: '🌱 Seed Portfolio Inputs', handler: 'menuDecisionSeedInputs_' },
    { label: '🔍 Audit Portfolio Inputs', handler: 'menuDecisionAuditInputs_' },
    { label: '🧹 Clean Corrupt Manual Inputs', handler: 'menuDecisionCleanInputs_' },
    { label: '🧪 Self-Test: Portfolio Cockpit', handler: 'menuDecisionSelfTestPortfolio_' },
    { label: '🧪 Self-Test: Ledger', handler: 'menuLedgerSelfTest_' }
  ];
}
/* --- Decision wrappers. Each resolves its target lazily so the menu still
 * builds and gives a clear deploy hint when a suite file is missing. --- */
function menuDecisionBuildTop10_() {
  return invokeMenuAction_('Build Top 10 Cockpit Layout', function () {
    return menuRunWithManualRefreshPriority_('Build Top 10 Cockpit Layout', function () {
      return menuCallFirstAvailable_(['buildDecisionTop10Layout'], [], 'buildDecisionTop10Layout() not found. Deploy 16_Decision_Top10.gs v1.0.0 or later.');
    });
  }, { successMessage: 'Top 10 decision cockpit layout built.' });
}
function menuDecisionRefreshTop10_() {
  return invokeMenuAction_('Refresh Decision Top 10', function () {
    return menuRunWithManualRefreshPriority_('Refresh Decision Top 10', function () {
      return menuCallFirstAvailable_(['refreshDecisionTop10'], [], 'refreshDecisionTop10() not found. Deploy 16_Decision_Top10.gs v1.0.0 or later.');
    });
  }, { successMessage: 'Decision Top 10 refreshed. Check the B2 status line on Top_10_Investments.' });
}
function menuDecisionBuildPortfolio_() {
  return invokeMenuAction_('Build Portfolio Decision Layout', function () {
    return menuRunWithManualRefreshPriority_('Build Portfolio Decision Layout', function () {
      return menuCallFirstAvailable_(['buildPortfolioDecisionLayout'], [], 'buildPortfolioDecisionLayout() not found. Deploy 10_My_Portfolio.gs v1.5.0 or later.');
    });
  }, { successMessage: 'Portfolio_Decision layout built.' });
}
function menuDecisionRefreshPortfolio_() {
  return invokeMenuAction_('Refresh Portfolio Decision', function () {
    return menuRunWithManualRefreshPriority_('Refresh Portfolio Decision', function () {
      return menuCallFirstAvailable_(['refreshPortfolioDecision'], [], 'refreshPortfolioDecision() not found. Deploy 10_My_Portfolio.gs v1.5.0 or later.');
    });
  }, { successMessage: 'Portfolio_Decision refreshed. Check the B2 status line on Portfolio_Decision.' });
}
/* v2.13.0: expose the ledger sync in the UI. v2.14.0 PREFERENCE REWIRE —
 * 21_Portfolio_Ledger.gs v1.0.0 supersedes the feeder role, so this wrapper
 * now resolves syncLedgerToPortfolio() (21: ACTIVE-lots-only upsert + prune +
 * Position Cost) FIRST and falls back to 20_Portfolio_Reconcile's
 * syncPortfolioLedger() only when 21 is absent. Running 20's reconciler
 * alongside a live 21 would put TWO owners on My_Portfolio membership; the
 * preference order prevents that from the menu path.
 * showSuccess:false — 20's path runs its own confirm dialog + toast; 21's
 * path returns silently, so its result note is toasted here (guarded on the
 * 21 return signature: upserts/added keys) without risking double dialogs.
 * skipValidation:true — the sync is sheet-local (no backend call). */
function menuPortfolioSyncLedger_() {
  return invokeMenuAction_('Sync Portfolio from Ledger', function () {
    var res = menuCallFirstAvailable_(['syncLedgerToPortfolio', 'syncPortfolioLedger'], [], 'Neither syncLedgerToPortfolio() (21_Portfolio_Ledger.gs v1.0.0, preferred) nor syncPortfolioLedger() (20_Portfolio_Reconcile.gs v2.0.0) found. Deploy 21_Portfolio_Ledger.gs.');
    if (res && res.note && (res.upserts !== undefined || res.added !== undefined)) {
      try { SpreadsheetApp.getActive().toast('Ledger sync: ' + res.note, 'TFB', 6); } catch (toastErr) {}
    }
    return res;
  }, { skipValidation: true, showSuccess: false });
}
/* v2.14.0: 21_Portfolio_Ledger.gs v1.0.0 entry points. Same lazy-resolve
 * pattern as every decision wrapper — the menu still builds before 21 is
 * pasted, and a click then shows the deploy hint instead of a hard error.
 * Flags: the build / dividend / self-test targets run their OWN dialogs or
 * toasts (showSuccess:false avoids doubles); the refresh target reports on
 * the sheet's own status line, so its wrapper gives a modal pointer to B2.
 * All skipValidation:true — ledger operations are sheet-local + provider
 * fetches, never a backend page call. */
function menuLedgerBuild_() {
  return invokeMenuAction_('Build Portfolio Ledger Layout', function () {
    return menuRunWithManualRefreshPriority_('Build Portfolio Ledger Layout', function () {
      return menuCallFirstAvailable_(['buildPortfolioLedgerLayout'], [], 'buildPortfolioLedgerLayout() not found. Deploy 21_Portfolio_Ledger.gs v1.0.0 or later.');
    });
  }, { skipValidation: true, showSuccess: false });
}
function menuLedgerRefresh_() {
  return invokeMenuAction_('Refresh Portfolio Ledger', function () {
    return menuRunWithManualRefreshPriority_('Refresh Portfolio Ledger', function () {
      return menuCallFirstAvailable_(['refreshPortfolioLedger'], [], 'refreshPortfolioLedger() not found. Deploy 21_Portfolio_Ledger.gs v1.0.0 or later.');
    });
  }, { skipValidation: true, successMessage: 'Ledger refreshed — run summary is on the _Portfolio_CostBasis status line (B2): active/closed/pending counts, dividend events synced, and the My_Portfolio feeder result.' });
}
function menuLedgerAddDividend_() {
  return invokeMenuAction_('Add Manual Dividend', function () {
    return menuCallFirstAvailable_(['plAddManualDividend'], [], 'plAddManualDividend() not found. Deploy 21_Portfolio_Ledger.gs v1.0.0 or later.');
  }, { skipValidation: true, showSuccess: false });
}
function menuLedgerSelfTest_() {
  return invokeMenuAction_('Self-Test: Ledger', function () {
    return menuCallFirstAvailable_(['plSelfTest'], [], 'plSelfTest() not found. Deploy 21_Portfolio_Ledger.gs v1.0.0 or later.');
  }, { skipValidation: true, showSuccess: false });
}
/* v2.16.0: read-only dividend-fetch probe from 21_Portfolio_Ledger v1.1.0.
 * Fetches dividend events for one symbol (default RCI.US, 13-month
 * lookback) against BOTH Yahoo chart hosts and reports per-host status —
 * built to confirm/refute the Google-egress-IP 429 hypothesis behind the
 * recurring RCI.US / BBD.US / NMM.US auto-dividend failures. If both
 * hosts return 429 here, the durable fix is routing dividend fetches via
 * the TFB backend instead of Apps Script egress. skipValidation — a
 * provider fetch only, never a backend page call; showSuccess:false —
 * the probe reports through its own dialog and the Executions log. */
function menuLedgerDivDiag_() {
  return invokeMenuAction_('Dividend Fetch Diagnostic', function () {
    return menuCallFirstAvailable_(['plDivDiag'], [], 'plDivDiag() not found. Deploy 21_Portfolio_Ledger.gs v1.1.0 or later.');
  }, { skipValidation: true, showSuccess: false });
}
/* v2.15.0: plain sheet activation for the ledger submenu — no 21 dependency
 * (navigateToPageByName_ is local to this file), so it works even before 21
 * is pasted; a missing sheet surfaces the standard 'Sheet not found' error
 * dialog with the sheet name. */
function menuLedgerNavigate_() {
  return invokeMenuAction_('Navigate Portfolio Ledger', function () {
    return navigateToPageByName_('_Portfolio_CostBasis');
  }, { skipValidation: true, successMessage: 'Portfolio Ledger (_Portfolio_CostBasis) opened.' });
}
function menuDecisionSeedInputs_() {
  return invokeMenuAction_('Seed Portfolio Inputs', function () {
    return menuCallFirstAvailable_(['tfbSeedPortfolioInputs'], [], 'tfbSeedPortfolioInputs() not found. Deploy 14_Portfolio_PnL.gs v2.0.1 or later.');
  }, { successMessage: 'Portfolio inputs seeded from _Portfolio_CostBasis into My_Portfolio (ledger-first: 21_Portfolio_Ledger when installed, else 14_Portfolio_PnL). Refresh My_Portfolio to compute weights.' });
}
function menuDecisionAuditInputs_() {
  return invokeMenuAction_('Audit Portfolio Inputs', function () {
    return menuCallFirstAvailable_(['tfbAuditPortfolioInputs'], [], 'tfbAuditPortfolioInputs() not found. Deploy 14_Portfolio_PnL.gs v2.1.0 or later.');
  }, { skipValidation: true, showSuccess: false });
}
function menuDecisionCleanInputs_() {
  return invokeMenuAction_('Clean Corrupt Manual Inputs', function () {
    return menuCallFirstAvailable_(['tfbCleanCorruptManualInputs'], [], 'tfbCleanCorruptManualInputs() not found. Deploy 14_Portfolio_PnL.gs v2.1.0 or later.');
  }, { skipValidation: true, showSuccess: false });
}
function menuDecisionBuildListsConfig_() {
  return invokeMenuAction_('Build _Lists_Config Defaults', function () {
    return menuCallFirstAvailable_(['buildListsConfig'], [], 'buildListsConfig() not found. Deploy 15_Lists_Config.gs v1.0.0 or later.');
  }, { successMessage: '_Lists_Config defaults ensured.' });
}
function menuDecisionDiagnostics_() {
  return invokeMenuAction_('Run Decision Diagnostics', function () {
    return menuCallFirstAvailable_(['tfbDecisionDiagnostics'], [], 'tfbDecisionDiagnostics() not found. Deploy 18_Decision_Diagnostics.gs v1.0.0 or later.');
  }, { skipValidation: true, successMessage: 'Decision diagnostics completed. See _Decision_Diagnostics for the report.' });
}
function menuDecisionSelfTestTop10_() {
  return invokeMenuAction_('Self-Test: Top 10 Cockpit', function () {
    return menuCallFirstAvailable_(['dt10SelfTest'], [], 'dt10SelfTest() not found. Deploy 16_Decision_Top10.gs v1.0.0 or later.');
  }, { skipValidation: true, showSuccess: false });
}
function menuDecisionSelfTestPortfolio_() {
  return invokeMenuAction_('Self-Test: Portfolio Cockpit', function () {
    return menuCallFirstAvailable_(['pdSelfTest'], [], 'pdSelfTest() not found. Deploy 10_My_Portfolio.gs v1.5.0 or later.');
  }, { skipValidation: true, showSuccess: false });
}
function menuDecisionSelfTestDiagnostics_() {
  return invokeMenuAction_('Self-Test: Diagnostics', function () {
    return menuCallFirstAvailable_(['ddSelfTest'], [], 'ddSelfTest() not found. Deploy 18_Decision_Diagnostics.gs v1.0.0 or later.');
  }, { skipValidation: true, showSuccess: false });
}
/* =========================================================
 * Core menu helpers
 * ========================================================= */
function invokeMenuAction_(actionName, fn, options) {
  options = options || {};
  var showSuccess = options.showSuccess !== false;
  var skipValidation = options.skipValidation === true;
  var successMessage = options.successMessage || 'Completed successfully.';
  var ui = SpreadsheetApp.getUi();
  try {
    if (!skipValidation && typeof validateRequiredConfig_ === 'function') {
      validateRequiredConfig_();
    }
    var startedAt = new Date();
    var result = fn();
    SpreadsheetApp.flush();
    if (showSuccess) {
      var endedAt = new Date();
      var seconds = ((endedAt.getTime() - startedAt.getTime()) / 1000).toFixed(2);
      ui.alert(ROOT_MENU_TITLE_, successMessage + '\n\nAction: ' + actionName + '\nDuration: ' + seconds + ' sec', ui.ButtonSet.OK);
    }
    return result;
  } catch (err) {
    var message = getMenuErrorMessage_(err);
    try { Logger.log('[Menu][' + actionName + '] ' + message); } catch (logErr) {}
    ui.alert(ROOT_MENU_TITLE_, 'Action failed:\n\n' + actionName + '\n\n' + message, ui.ButtonSet.OK);
    throw err;
  }
}
function getMenuErrorMessage_(err) {
  if (!err) return 'Unknown error.';
  if (err.message) return String(err.message);
  return String(err);
}
function confirmAction_(title, message) {
  var ui = SpreadsheetApp.getUi();
  var response = ui.alert(title, message, ui.ButtonSet.YES_NO);
  return response === ui.Button.YES;
}
function menuGetGlobalFunction_(name) {
  if (!name) return null;
  try {
    if (typeof globalThis !== 'undefined' && typeof globalThis[name] === 'function') {
      return globalThis[name];
    }
  } catch (e1) {}
  try {
    var fn = eval(name);
    if (typeof fn === 'function') return fn;
  } catch (e2) {}
  return null;
}
function menuCallFirstAvailable_(names, args, missingMessage) {
  args = args || [];
  for (var i = 0; i < names.length; i++) {
    var fn = menuGetGlobalFunction_(names[i]);
    if (fn) return fn.apply(null, args);
  }
  throw new Error(missingMessage || ('None of these functions is available: ' + names.join(', ')));
}
function menuNormalizePageName_(pageName) {
  if (typeof normalizePageName_ === 'function') {
    return normalizePageName_(pageName);
  }
  var raw = String(pageName || '').trim();
  var supported = (typeof getSupportedPages === 'function') ? getSupportedPages() : [];
  for (var i = 0; i < supported.length; i++) {
    if (supported[i] === raw || String(supported[i]).toLowerCase() === raw.toLowerCase()) {
      return supported[i];
    }
  }
  return raw;
}
function menuGetActivePageName_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getActiveSheet();
  var rawName = sheet ? sheet.getName() : '';
  var pageName = menuNormalizePageName_(rawName);
  var supported = (typeof getSupportedPages === 'function') ? getSupportedPages() : [];
  if (supported.indexOf(pageName) < 0) {
    throw new Error(
      'The active sheet "' + rawName + '" is not a supported dashboard page.\n\n' +
      'Supported pages:\n- ' + supported.join('\n- ') + '\n\n' +
      'Select a supported page before running this action.'
    );
  }
  return pageName;
}
function navigateToPageByName_(pageName) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(pageName);
  if (!sheet) throw new Error('Sheet not found: ' + pageName);
  ss.setActiveSheet(sheet);
  return true;
}
/* =========================================================
 * Page handlers
 * ========================================================= */
function getPageMenuHandlers_(pageName) {
  var map = {};
  map[SHEET_NAMES.MARKET_LEADERS] = {
    build: 'menuBuild_Market_Leaders_',
    navigate: 'menuNavigate_Market_Leaders_',
    header: 'menuHeader_Market_Leaders_',
    format: 'menuFormat_Market_Leaders_',
    refresh: 'menuRefresh_Market_Leaders_',
    refreshRow: 'menuRefreshRow_Market_Leaders_',
    refreshBatches: 'menuRefreshBatches_Market_Leaders_',
    validate: 'menuValidate_Market_Leaders_',
    sort: 'menuSort_Market_Leaders_',
    diagnose: 'menuDiagnose_Market_Leaders_'
  };
  map[SHEET_NAMES.GLOBAL_MARKETS] = {
    build: 'menuBuild_Global_Markets_',
    navigate: 'menuNavigate_Global_Markets_',
    header: 'menuHeader_Global_Markets_',
    format: 'menuFormat_Global_Markets_',
    refresh: 'menuRefresh_Global_Markets_',
    refreshRow: 'menuRefreshRow_Global_Markets_',
    refreshBatches: 'menuRefreshBatches_Global_Markets_',
    validate: 'menuValidate_Global_Markets_',
    sort: 'menuSort_Global_Markets_',
    diagnose: 'menuDiagnose_Global_Markets_'
  };
  map[SHEET_NAMES.COMMODITIES_FX] = {
    build: 'menuBuild_Commodities_FX_',
    navigate: 'menuNavigate_Commodities_FX_',
    header: 'menuHeader_Commodities_FX_',
    format: 'menuFormat_Commodities_FX_',
    refresh: 'menuRefresh_Commodities_FX_',
    refreshRow: 'menuRefreshRow_Commodities_FX_',
    refreshBatches: 'menuRefreshBatches_Commodities_FX_',
    validate: 'menuValidate_Commodities_FX_',
    sort: 'menuSort_Commodities_FX_',
    diagnose: 'menuDiagnose_Commodities_FX_'
  };
  map[SHEET_NAMES.MUTUAL_FUNDS] = {
    build: 'menuBuild_Mutual_Funds_',
    navigate: 'menuNavigate_Mutual_Funds_',
    header: 'menuHeader_Mutual_Funds_',
    format: 'menuFormat_Mutual_Funds_',
    refresh: 'menuRefresh_Mutual_Funds_',
    refreshRow: 'menuRefreshRow_Mutual_Funds_',
    refreshBatches: 'menuRefreshBatches_Mutual_Funds_',
    validate: 'menuValidate_Mutual_Funds_',
    sort: 'menuSort_Mutual_Funds_',
    diagnose: 'menuDiagnose_Mutual_Funds_'
  };
  map[SHEET_NAMES.MY_PORTFOLIO] = {
    build: 'menuBuild_My_Portfolio_',
    navigate: 'menuNavigate_My_Portfolio_',
    header: 'menuHeader_My_Portfolio_',
    format: 'menuFormat_My_Portfolio_',
    refresh: 'menuRefresh_My_Portfolio_',
    refreshRow: 'menuRefreshRow_My_Portfolio_',
    refreshBatches: 'menuRefreshBatches_My_Portfolio_',
    validate: 'menuValidate_My_Portfolio_',
    sort: 'menuSort_My_Portfolio_',
    diagnose: 'menuDiagnose_My_Portfolio_'
  };
  map[SHEET_NAMES.INSIGHTS_ANALYSIS] = {
    build: 'menuBuild_Insights_Analysis_',
    navigate: 'menuNavigate_Insights_Analysis_',
    header: 'menuHeader_Insights_Analysis_',
    format: 'menuFormat_Insights_Analysis_',
    refresh: 'menuRefresh_Insights_Analysis_',
    refreshRow: 'menuRefreshRow_Insights_Analysis_',
    refreshBatches: 'menuRefreshBatches_Insights_Analysis_',
    validate: 'menuValidate_Insights_Analysis_',
    sort: 'menuSort_Insights_Analysis_',
    diagnose: 'menuDiagnose_Insights_Analysis_'
  };
  map[SHEET_NAMES.TOP_10_INVESTMENTS] = {
    build: 'menuBuild_Top_10_Investments_',
    navigate: 'menuNavigate_Top_10_Investments_',
    header: 'menuHeader_Top_10_Investments_',
    format: 'menuFormat_Top_10_Investments_',
    refresh: 'menuRefresh_Top_10_Investments_',
    refreshRow: 'menuRefreshRow_Top_10_Investments_',
    refreshBatches: 'menuRefreshBatches_Top_10_Investments_',
    validate: 'menuValidate_Top_10_Investments_',
    sort: 'menuSort_Top_10_Investments_',
    diagnose: 'menuDiagnose_Top_10_Investments_'
  };
  map[SHEET_NAMES.DATA_DICTIONARY] = {
    build: 'menuBuild_Data_Dictionary_',
    navigate: 'menuNavigate_Data_Dictionary_',
    header: 'menuHeader_Data_Dictionary_',
    format: 'menuFormat_Data_Dictionary_',
    refresh: 'menuRefresh_Data_Dictionary_',
    refreshRow: 'menuRefreshRow_Data_Dictionary_',
    refreshBatches: 'menuRefreshBatches_Data_Dictionary_',
    validate: 'menuValidate_Data_Dictionary_',
    sort: 'menuSort_Data_Dictionary_',
    diagnose: 'menuDiagnose_Data_Dictionary_'
  };
  if (!map[pageName]) throw new Error('Unknown page for handlers: ' + pageName);
  return map[pageName];
}
function dispatchActivePageMenuAction_(actionKey) {
  return menuPageAction_(menuGetActivePageName_(), actionKey);
}
/* v2.17.0: single menu-side gateway for manual refresh priority. Fail-loud
 * when the aligned Core is absent; silently falling back would recreate the
 * original lock-timeout problem while telling the operator the fix is live. */
function menuRunWithManualRefreshPriority_(label, fn) {
  if (typeof fn !== 'function') {
    throw new Error('menuRunWithManualRefreshPriority_ requires a function.');
  }
  if (typeof withManualRefreshPriority_ !== 'function') {
    throw new Error('Manual refresh priority coordinator not found. Deploy 02_Core.gs v2.11.1 or later.');
  }
  return withManualRefreshPriority_(label || 'Manual Refresh', fn, {});
}
function menuPageActionNeedsManualPriority_(actionKey) {
  return ['build', 'refresh', 'refreshRow', 'refreshBatches'].indexOf(String(actionKey || '')) >= 0;
}
function menuPageAction_(pageName, actionKey) {
  var page = menuNormalizePageName_(pageName);
  function runPageAction_() {
    /* v2.12.0: decision-owned pages are rendered by a cockpit module
     * (16_Decision_Top10.gs). The legacy engine actions would overwrite
     * (build/header/refresh*) or scramble (format/sort) the cockpit. Instead of
     * THROWING, redirect destructive actions to the cockpit owner. */
    if (menuIsDecisionOwnedPage_(page) && DECISION_BLOCKED_ACTIONS_.indexOf(actionKey) >= 0) {
      return menuDecisionOwnedAction_(page, actionKey);
    }
    if (actionKey === 'build') return menuBuildPageByName_(page);
    if (actionKey === 'navigate') return navigateToPageByName_(page);
    if (actionKey === 'header') return updateHeadersOnly_(page);
    if (actionKey === 'format') return applySheetFormatting_(page);
    if (actionKey === 'refresh') return menuRefreshPageByName_(page);
    if (actionKey === 'refreshRow') return refreshSelectedRow_(page);
    if (actionKey === 'refreshBatches') return menuRefreshPageInBatchesByName_(page);
    if (actionKey === 'validate') return menuValidatePageByName_(page);
    if (actionKey === 'sort') return menuSortPageByName_(page);
    if (actionKey === 'diagnose') return diagnosePageHeaders_(page);
    throw new Error('Unsupported page action: ' + actionKey + ' for ' + page);
  }
  if (menuPageActionNeedsManualPriority_(actionKey)) {
    return menuRunWithManualRefreshPriority_(
      'Manual ' + actionKey + ' — ' + page,
      runPageAction_
    );
  }
  return runPageAction_();
}
/* v2.12.0: route a decision-owned page's blocked engine action to its cockpit
 * equivalent instead of throwing. build / header / format rebuild the cockpit
 * layout (which re-applies its own header banner and styling); refresh re-pulls
 * the cockpit data; refreshRow / refreshBatches / sort have no cockpit meaning
 * (the cockpit is a fixed, wholly-rebuilt block), so they return a friendly
 * skip result rather than aborting. v2.20.1: page-aware — Portfolio_Decision
 * routes to the 10_My_Portfolio cockpit, everything else to 16_Decision_Top10.
 * menuCallFirstAvailable_ still yields a clear deploy hint if the file is
 * missing. */
function menuDecisionOwnedAction_(page, actionKey) {
  var isPortfolio = String(page || '').toLowerCase() === 'portfolio_decision';
  var buildFn = isPortfolio ? 'buildPortfolioDecisionLayout' : 'buildDecisionTop10Layout';
  var refreshFn = isPortfolio ? 'refreshPortfolioDecision' : 'refreshDecisionTop10';
  var hint = isPortfolio ? 'Deploy 10_My_Portfolio.gs v1.5.0 or later.' : 'Deploy 16_Decision_Top10.gs v1.0.0 or later.';
  if (actionKey === 'build' || actionKey === 'header' || actionKey === 'format') {
    return menuCallFirstAvailable_([buildFn], [], buildFn + '() not found. ' + hint);
  }
  if (actionKey === 'refresh') {
    return menuCallFirstAvailable_([refreshFn], [], refreshFn + '() not found. ' + hint);
  }
  // refreshRow / refreshBatches / sort: no cockpit equivalent — skip, do not abort.
  return {
    ok: true,
    page: page,
    action: actionKey,
    status: 'skipped_decision_owned',
    message: '"' + page + '" is a Decision cockpit page; "' + actionKey + '" does not apply ' +
      '(the cockpit is rebuilt and refreshed as a whole). Use Build Decision Cockpit Layout / ' +
      'Refresh Decision Top 10 (or the Portfolio_Decision page items) instead.'
  };
}
function menuBuildPageByName_(pageName) {
  if (pageName === SHEET_NAMES.MARKET_LEADERS && typeof buildMarketLeaders_ === 'function') return buildMarketLeaders_();
  if (pageName === SHEET_NAMES.GLOBAL_MARKETS && typeof buildGlobalMarkets_ === 'function') return buildGlobalMarkets_();
  if (pageName === SHEET_NAMES.COMMODITIES_FX && typeof buildCommoditiesFx_ === 'function') return buildCommoditiesFx_();
  if (pageName === SHEET_NAMES.MUTUAL_FUNDS && typeof buildMutualFunds_ === 'function') return buildMutualFunds_();
  if (pageName === SHEET_NAMES.MY_PORTFOLIO && typeof buildMyPortfolio_ === 'function') return buildMyPortfolio_();
  if (pageName === SHEET_NAMES.INSIGHTS_ANALYSIS && typeof buildInsightsAnalysis_ === 'function') return buildInsightsAnalysis_();
  if (pageName === SHEET_NAMES.TOP_10_INVESTMENTS && typeof buildTop10Investments_ === 'function') return buildTop10Investments_();
  if (pageName === SHEET_NAMES.DATA_DICTIONARY && typeof buildDataDictionary_ === 'function') return buildDataDictionary_();
  return buildPage_(pageName);
}
function menuRefreshPageByName_(pageName) {
  if (pageName === SHEET_NAMES.MARKET_LEADERS && typeof refreshMarketLeaders_ === 'function') return refreshMarketLeaders_();
  if (pageName === SHEET_NAMES.GLOBAL_MARKETS && typeof refreshGlobalMarkets_ === 'function') return refreshGlobalMarkets_();
  if (pageName === SHEET_NAMES.COMMODITIES_FX && typeof refreshCommoditiesFx_ === 'function') return refreshCommoditiesFx_();
  if (pageName === SHEET_NAMES.MUTUAL_FUNDS && typeof refreshMutualFunds_ === 'function') return refreshMutualFunds_();
  if (pageName === SHEET_NAMES.MY_PORTFOLIO && typeof refreshMyPortfolio_ === 'function') return refreshMyPortfolio_();
  if (pageName === SHEET_NAMES.INSIGHTS_ANALYSIS && typeof refreshInsightsAnalysis_ === 'function') return refreshInsightsAnalysis_();
  if (pageName === SHEET_NAMES.TOP_10_INVESTMENTS && typeof refreshTop10Investments_ === 'function') return refreshTop10Investments_();
  if (pageName === SHEET_NAMES.DATA_DICTIONARY && typeof refreshDataDictionary_ === 'function') return refreshDataDictionary_();
  return refreshPage_(pageName);
}
function menuRefreshPageInBatchesByName_(pageName) {
  if (pageName === SHEET_NAMES.MARKET_LEADERS && typeof refreshMarketLeadersInBatches_ === 'function') return refreshMarketLeadersInBatches_();
  if (pageName === SHEET_NAMES.GLOBAL_MARKETS && typeof refreshGlobalMarketsInBatches_ === 'function') return refreshGlobalMarketsInBatches_();
  if (pageName === SHEET_NAMES.COMMODITIES_FX && typeof refreshCommoditiesFxInBatches_ === 'function') return refreshCommoditiesFxInBatches_();
  if (pageName === SHEET_NAMES.MUTUAL_FUNDS && typeof refreshMutualFundsInBatches_ === 'function') return refreshMutualFundsInBatches_();
  if (pageName === SHEET_NAMES.MY_PORTFOLIO && typeof refreshMyPortfolioInBatches_ === 'function') return refreshMyPortfolioInBatches_();
  return refreshPageInBatches_(pageName);
}
function menuValidatePageByName_(pageName) {
  if (pageName === SHEET_NAMES.GLOBAL_MARKETS && typeof validateGlobalMarkets_ === 'function') return validateGlobalMarkets_();
  if (pageName === SHEET_NAMES.TOP_10_INVESTMENTS && typeof validateTop10Investments_ === 'function') return validateTop10Investments_();
  if (pageName === SHEET_NAMES.MARKET_LEADERS && typeof validateMarketLeaders_ === 'function') return validateMarketLeaders_();
  if (pageName === SHEET_NAMES.COMMODITIES_FX && typeof validateCommoditiesFx_ === 'function') return validateCommoditiesFx_();
  if (pageName === SHEET_NAMES.MUTUAL_FUNDS && typeof validateMutualFunds_ === 'function') return validateMutualFunds_();
  if (pageName === SHEET_NAMES.MY_PORTFOLIO && typeof validateMyPortfolio_ === 'function') return validateMyPortfolio_();
  if (pageName === SHEET_NAMES.INSIGHTS_ANALYSIS && typeof validateInsightsAnalysis_ === 'function') return validateInsightsAnalysis_();
  if (pageName === SHEET_NAMES.DATA_DICTIONARY && typeof validateDataDictionary_ === 'function') return validateDataDictionary_();
  return validatePage_(pageName);
}
function menuSortPageByName_(pageName) {
  if (pageName === SHEET_NAMES.GLOBAL_MARKETS && typeof sortGlobalMarkets_ === 'function') return sortGlobalMarkets_();
  if (pageName === SHEET_NAMES.TOP_10_INVESTMENTS && typeof sortTop10Investments_ === 'function') return sortTop10Investments_();
  if (pageName === SHEET_NAMES.MARKET_LEADERS && typeof sortMarketLeaders_ === 'function') return sortMarketLeaders_();
  if (pageName === SHEET_NAMES.COMMODITIES_FX && typeof sortCommoditiesFx_ === 'function') return sortCommoditiesFx_();
  if (pageName === SHEET_NAMES.MUTUAL_FUNDS && typeof sortMutualFunds_ === 'function') return sortMutualFunds_();
  if (pageName === SHEET_NAMES.MY_PORTFOLIO && typeof sortMyPortfolio_ === 'function') return sortMyPortfolio_();
  if (pageName === SHEET_NAMES.INSIGHTS_ANALYSIS && typeof sortInsightsAnalysis_ === 'function') return sortInsightsAnalysis_();
  if (pageName === SHEET_NAMES.DATA_DICTIONARY && typeof sortDataDictionary_ === 'function') return sortDataDictionary_();
  return sortAndRankPage_(pageName);
}
/* =========================================================
 * Active page actions
 * ========================================================= */
function menuBuildCurrentPage_() { return invokeMenuAction_('Build Current Page', function () { return dispatchActivePageMenuAction_('build'); }, { successMessage: 'Current page build completed successfully.' }); }
function menuNavigateCurrentPage_() { return invokeMenuAction_('Navigate Current Page', function () { return dispatchActivePageMenuAction_('navigate'); }, { skipValidation: true, successMessage: 'Current page opened successfully.' }); }
function menuUpdateCurrentHeaders_() { return invokeMenuAction_('Build Current Header', function () { return dispatchActivePageMenuAction_('header'); }, { successMessage: 'Current page headers updated successfully.' }); }
function menuReformatCurrentPage_() { return invokeMenuAction_('Apply Current Format', function () { return dispatchActivePageMenuAction_('format'); }, { successMessage: 'Current page formatting applied successfully.' }); }
function menuRefreshCurrentPage_() { return invokeMenuAction_('Refresh Current Full Data', function () { return dispatchActivePageMenuAction_('refresh'); }, { successMessage: 'Current page data refreshed successfully.' }); }
function menuRefreshSelectedRowCurrent_() { return invokeMenuAction_('Refresh Selected Row', function () { return dispatchActivePageMenuAction_('refreshRow'); }, { successMessage: 'Selected row refresh completed successfully.' }); }
function menuRefreshCurrentPageInBatches_() { return invokeMenuAction_('Refresh Current Page In Batches', function () { return dispatchActivePageMenuAction_('refreshBatches'); }, { successMessage: 'Batch refresh completed successfully.' }); }
function menuValidateCurrentPage_() { return invokeMenuAction_('Validate Current Page', function () { return dispatchActivePageMenuAction_('validate'); }, { successMessage: 'Page validation completed successfully.' }); }
function menuSortAndRankCurrentPage_() { return invokeMenuAction_('Sort & Rank Current Page', function () { return dispatchActivePageMenuAction_('sort'); }, { successMessage: 'Page sort/rank completed successfully.' }); }
function menuShowCurrentPageSymbols_() { return invokeMenuAction_('Show Current Page Symbols', function () { return manageSymbolsForPage_(menuGetActivePageName_()); }, { showSuccess: false, skipValidation: true }); }
function menuManageCurrentPageSymbols_() { return invokeMenuAction_('Manage Current Page Symbols', function () { return manageSymbolsForPage_(menuGetActivePageName_()); }, { showSuccess: false, skipValidation: true }); }
function menuDiagnoseCurrentHeaders_() { return invokeMenuAction_('Diagnose Headers (Current Page)', function () { return diagnosePageHeaders_(menuGetActivePageName_()); }, { showSuccess: false, skipValidation: true }); }
/* =========================================================
 * Global actions
 * ========================================================= */
function menuRefreshAllPages_() {
  if (!confirmAction_('Refresh All Pages', 'This will refresh all supported pages.\n\nContinue?')) return;
  return invokeMenuAction_('Refresh All Pages', function () {
    return menuRunWithManualRefreshPriority_('Refresh All Pages', function () { return refreshAllPages_(); });
  }, { successMessage: 'All pages refreshed successfully.' });
}
function menuRefreshAnalysisAndInsights_() {
  return invokeMenuAction_('Refresh Analysis & Insights', function () {
    return menuRunWithManualRefreshPriority_('Refresh Analysis & Insights', function () { return refreshAnalysisAndInsights_(); });
  }, { successMessage: 'Analysis and insights refresh completed successfully.' });
}
function menuEnsureAllSheetsAndHeaders_() {
  return invokeMenuAction_('Ensure All Sheets & Headers', function () { return ensureAllSheetsAndHeaders_(); }, { successMessage: 'All supported sheets and headers were ensured successfully.' });
}
function menuTrimAllSheetsToExpectedColumns_() {
  if (!confirmAction_('Trim All Sheets', 'This will delete columns after the approved expected width for supported sheets only.\n\nContinue?')) return;
  return invokeMenuAction_('Trim All Sheets To Expected Columns', function () {
    return menuCallFirstAvailable_(['trimAllSheetsToExpectedColumns'], [], 'trimAllSheetsToExpectedColumns() not found. Deploy 05_Refresh.gs v1.11.2 or later.');
  }, { successMessage: 'All supported sheets were trimmed to expected columns.' });
}
function menuTrimCurrentSheetToExpectedColumns_() {
  return invokeMenuAction_('Trim Current Sheet To Expected Columns', function () {
    return menuCallFirstAvailable_(['trimCurrentSheetToExpectedColumns'], [], 'trimCurrentSheetToExpectedColumns() not found. Deploy 05_Refresh.gs v1.11.2 or later.');
  }, { successMessage: 'Current sheet was trimmed to expected columns.' });
}
function menuReformatAllSheets_() {
  return invokeMenuAction_('Reformat All Sheets', function () {
    return menuCallFirstAvailable_(['reformatAllSheets_', 'reformatAllSheets'], [], 'reformatAllSheets_() not found. Deploy 05_Refresh.gs v1.11.2 or later.');
  }, { successMessage: 'All supported sheets were reformatted.' });
}
function menuVerifyFormatVersion_() {
  return invokeMenuAction_('Verify Format Version', function () {
    return menuCallFirstAvailable_(['verifyFormatVersion_', 'verifyFormatVersion'], [], 'verifyFormatVersion_() not found. Deploy 04_Format.gs v2.12.0 or later.');
  }, { skipValidation: true, showSuccess: false });
}
function menuAuditAllPages_() {
  return invokeMenuAction_('Run Data Audit All Pages', function () {
    return menuCallFirstAvailable_(['auditAllPages'], [], 'auditAllPages() not found. Deploy 99_Data_Audit.gs v1.3.0 or later.');
  }, { successMessage: 'Data audit completed successfully.' });
}
function menuAuditGlobalMarkets_() {
  return invokeMenuAction_('Audit Global_Markets', function () {
    return menuCallFirstAvailable_(['auditGlobalMarkets'], [], 'auditGlobalMarkets() not found. Deploy 99_Data_Audit.gs v1.3.0 or later.');
  }, { successMessage: 'Global_Markets audit completed successfully.' });
}
function menuAuditTop10Investments_() {
  return invokeMenuAction_('Audit Top_10_Investments', function () {
    return menuCallFirstAvailable_(['auditTop10Investments'], [], 'auditTop10Investments() not found. Deploy 99_Data_Audit.gs v1.3.0 or later.');
  }, { successMessage: 'Top_10_Investments audit completed successfully.' });
}
/* =========================================================
 * Configuration wrappers
 * ========================================================= */
function menuSetApiToken_() {
  return invokeMenuAction_('Set / Update API Token', function () {
    var ui = SpreadsheetApp.getUi();
    var props = PropertiesService.getScriptProperties();
    var current = props.getProperty('TFB_TOKEN') || '';
    var prompt = ui.prompt('Set / Update API Token', 'Enter the backend token.\n\nCurrent token loaded: ' + (current ? 'Yes' : 'No'), ui.ButtonSet.OK_CANCEL);
    if (prompt.getSelectedButton() !== ui.Button.OK) return false;
    var value = String(prompt.getResponseText() || '').trim();
    if (!value) throw new Error('Token cannot be empty.');
    props.setProperty('TFB_TOKEN', value);
    return true;
  }, { skipValidation: true, successMessage: 'API token saved successfully.' });
}
function menuSetBaseUrl_() {
  return invokeMenuAction_('Set / Update Backend URL', function () {
    var ui = SpreadsheetApp.getUi();
    var props = PropertiesService.getScriptProperties();
    var current = props.getProperty('TFB_BASE_URL') || '';
    var prompt = ui.prompt('Set / Update Backend URL', 'Enter the backend base URL.\n\nExample:\nhttps://tadawul-fast-bridge.onrender.com\n\nCurrent:\n' + (current || '(not set)'), ui.ButtonSet.OK_CANCEL);
    if (prompt.getSelectedButton() !== ui.Button.OK) return false;
    var value = String(prompt.getResponseText() || '').trim().replace(/\/+$/, '');
    if (!value) throw new Error('Base URL cannot be empty.');
    props.setProperty('TFB_BASE_URL', value);
    return true;
  }, { skipValidation: true, successMessage: 'Backend URL saved successfully.' });
}
function menuSetDefaultTimeout_() {
  return invokeMenuAction_('Set Default Timeout', function () {
    var ui = SpreadsheetApp.getUi();
    var props = PropertiesService.getScriptProperties();
    var fallback = (typeof DEFAULTS !== 'undefined' && DEFAULTS.TIMEOUT_SEC) ? DEFAULTS.TIMEOUT_SEC : 120;
    var current = props.getProperty('DEFAULT_TIMEOUT_SEC') || String(fallback);
    var prompt = ui.prompt('Set Default Timeout', 'Enter timeout in seconds.\n\nCurrent: ' + current, ui.ButtonSet.OK_CANCEL);
    if (prompt.getSelectedButton() !== ui.Button.OK) return false;
    var value = parseInt(prompt.getResponseText(), 10);
    if (!value || value <= 0) throw new Error('Timeout must be a positive number.');
    props.setProperty('DEFAULT_TIMEOUT_SEC', String(value));
    return true;
  }, { skipValidation: true, successMessage: 'Default timeout updated successfully.' });
}
function menuSetDefaultLimit_() {
  return invokeMenuAction_('Set Default Limit', function () {
    var ui = SpreadsheetApp.getUi();
    var props = PropertiesService.getScriptProperties();
    var fallback = (typeof DEFAULTS !== 'undefined' && DEFAULTS.LIMIT) ? DEFAULTS.LIMIT : 200;
    var current = props.getProperty('DEFAULT_LIMIT') || String(fallback);
    var prompt = ui.prompt('Set Default Limit', 'Enter default limit.\n\nCurrent: ' + current, ui.ButtonSet.OK_CANCEL);
    if (prompt.getSelectedButton() !== ui.Button.OK) return false;
    var value = parseInt(prompt.getResponseText(), 10);
    if (!value || value <= 0) throw new Error('Default limit must be a positive number.');
    props.setProperty('DEFAULT_LIMIT', String(value));
    return true;
  }, { skipValidation: true, successMessage: 'Default limit updated successfully.' });
}
function menuSetSchemaCheckMode_() {
  return invokeMenuAction_('Set Schema Check Mode', function () {
    var ui = SpreadsheetApp.getUi();
    var props = PropertiesService.getScriptProperties();
    var current = props.getProperty('SCHEMA_CHECK_MODE') || '(unset → defaults to min)';
    var prompt = ui.prompt('Set Schema Check Mode', 'Enter mode:\n\nmin = lenient\nexact = strict\nblank = clear override\n\nCurrent: ' + current, ui.ButtonSet.OK_CANCEL);
    if (prompt.getSelectedButton() !== ui.Button.OK) return false;
    var value = String(prompt.getResponseText() || '').trim().toLowerCase();
    if (!value) {
      props.deleteProperty('SCHEMA_CHECK_MODE');
      if (typeof getConfig === 'function') getConfig(true);
      return true;
    }
    if (value !== 'min' && value !== 'exact') throw new Error('Schema check mode must be "min", "exact", or blank.');
    props.setProperty('SCHEMA_CHECK_MODE', value);
    if (typeof getConfig === 'function') getConfig(true);
    return true;
  }, { skipValidation: true, successMessage: 'Schema check mode updated successfully.' });
}
function menuShowConfigSummary_() {
  return invokeMenuAction_('Show Configuration Summary', function () {
    var summary = (typeof getConfigSummary_ === 'function') ? getConfigSummary_() : {};
    var message = [
      'Menu Version: ' + SCRIPT_MENU_VERSION,
      'App Name: ' + (summary.appName || 'Tadawul Tracker'),
      'Config Version: ' + (summary.version || (typeof SCRIPT_VERSION !== 'undefined' ? SCRIPT_VERSION : 'unknown')),
      'Base URL: ' + (summary.baseUrl || '(not set)'),
      'Token Loaded: ' + (summary.tokenLoaded ? 'Yes' : 'No'),
      'Default Limit: ' + (summary.defaultLimit || ''),
      'Default Mode: ' + (summary.defaultMode || ''),
      'Supported Pages: ' + ((summary.supportedPages || getSupportedPages()).length),
      '',
      'Contract: main pages 115 columns, Top_10_Investments 118 columns, final main-page column Warnings.'
    ].join('\n');
    SpreadsheetApp.getUi().alert('Configuration Summary', message, SpreadsheetApp.getUi().ButtonSet.OK);
    return summary;
  }, { skipValidation: true, showSuccess: false });
}
function menuValidateConfiguration_() {
  return invokeMenuAction_('Validate Configuration', function () {
    validateRequiredConfig_();
    return true;
  }, { skipValidation: true, successMessage: 'Configuration is valid.' });
}
function menuVerifyConfigVersion_() {
  return invokeMenuAction_('Verify Config Version', function () {
    var result = menuCallFirstAvailable_(['debugVerifyConfigVersion_', 'debugVerifyConfigVersion'], [], 'debugVerifyConfigVersion_() not found. Deploy 00_Config.gs v1.12.1 or later.');
    var ui = SpreadsheetApp.getUi();
    var lines = [];
    lines.push('Config verification ' + (result && result.ok ? 'PASSED' : 'completed with attention items'));
    if (result && typeof result.total === 'number') lines.push('Total checks: ' + result.total);
    if (result && typeof result.pass === 'number') lines.push('PASS: ' + result.pass);
    if (result && typeof result.fail === 'number') lines.push('FAIL: ' + result.fail);
    lines.push('');
    lines.push('Full output: Apps Script editor → Executions.');
    ui.alert('Config Version', lines.join('\n'), ui.ButtonSet.OK);
    return result;
  }, { skipValidation: true, showSuccess: false });
}
/* v2.16.0: surface 02_Core v2.9.1's repaired self-verify next to its Config
 * and Format siblings. Result dialog mirrors menuVerifyConfigVersion_:
 * ok/expected/actual version, checks count, names of any failing checks,
 * pointer to Executions for the [v2.9.1 CORE VERIFY] log line. Hint names
 * v2.9.1 because earlier Cores HAVE verifyCoreVersion but it asserted a
 * stale constant — the minimum USEFUL version is the honest one. */
function menuVerifyCoreVersion_() {
  return invokeMenuAction_('Verify Core Version', function () {
    var result = menuCallFirstAvailable_(['verifyCoreVersion', 'verifyCoreVersion_'], [], 'verifyCoreVersion() not found. Deploy 02_Core.gs v2.9.1 or later.');
    var ui = SpreadsheetApp.getUi();
    var lines = [];
    lines.push('Core verification ' + (result && result.ok ? 'PASSED' : 'FAILED'));
    if (result && result.expected) lines.push('Expected version: ' + result.expected);
    if (result && result.version) lines.push('Actual version: ' + result.version);
    if (result && result.checks && result.checks.length) {
      lines.push('Checks: ' + result.checks.length);
      var failing = [];
      for (var ci = 0; ci < result.checks.length; ci++) {
        if (!result.checks[ci].ok) failing.push(result.checks[ci].name);
      }
      if (failing.length) {
        lines.push('');
        lines.push('Failing checks:');
        for (var fi = 0; fi < failing.length; fi++) lines.push('  • ' + failing[fi]);
      }
    }
    lines.push('');
    lines.push('Full output: Apps Script editor → Executions ([v2.9.1 CORE VERIFY] line).');
    ui.alert('Core Version', lines.join('\n'), ui.ButtonSet.OK);
    return result;
  }, { skipValidation: true, showSuccess: false });
}
function menuRunPhase1Verification_() {
  return invokeMenuAction_('Run Phase 1 Verification', function () {
    var result = menuCallFirstAvailable_(['debugVerifyConfigVersion_', 'debugVerifyConfigVersion'], [], 'debugVerifyConfigVersion_() not found. Deploy 00_Config.gs v1.12.1 or later.');
    var ui = SpreadsheetApp.getUi();
    ui.alert('Phase 1 Verification',
      'Config audit executed.\n\n' +
      'PASS: ' + (result.pass || 0) + ' / ' + (result.total || 0) + '\n' +
      'FAIL: ' + (result.fail || 0) + '\n\n' +
      'Next: run Trim All Sheets, Diagnose Schema Contract Status, then Run Full Diagnostic Sweep.',
      ui.ButtonSet.OK);
    return result;
  }, { skipValidation: true, showSuccess: false });
}
/* =========================================================
 * Diagnostics wrappers
 * ========================================================= */
function menuRunFullDiagnosticSweep_() {
  return invokeMenuAction_('Run Full Diagnostic Sweep', function () {
    return menuCallFirstAvailable_(['runFullDiagnosticSweep', 'runFullDiagnosticSweep_'], [], 'runFullDiagnosticSweep() not found. Deploy 12_Diagnostics.gs v1.7.0 or later.');
  }, { skipValidation: true, showSuccess: false });
}
function menuVerifyV28SchemaPropagation_() {
  return invokeMenuAction_('Verify 115/118 Schema Propagation', function () {
    var pageName = SHEET_NAMES.MARKET_LEADERS;
    try { pageName = menuGetActivePageName_(); } catch (e) {}
    return menuCallFirstAvailable_(['debugV28SchemaPropagation_'], [pageName], 'debugV28SchemaPropagation_() not found. Deploy 03_Schema.gs v2.5.0 or later.');
  }, { skipValidation: true, showSuccess: false });
}
function menuDiagnoseSchemaContractStatus_() {
  return invokeMenuAction_('Diagnose Schema Contract Status', function () {
    return menuCallFirstAvailable_(['diagnoseSchemaContractStatus_', 'diagnoseSchemaContractStatus'], [], 'diagnoseSchemaContractStatus_() not found. Deploy 03_Schema.gs v2.5.0 or later.');
  }, { skipValidation: true, showSuccess: false });
}
function menuDiagnoseV310CriteriaVisibility_() {
  return invokeMenuAction_('Diagnose Criteria Visibility', function () {
    return menuCallFirstAvailable_(['diagnoseV310CriteriaVisibility_'], [], 'diagnoseV310CriteriaVisibility_() not found. Deploy 11_SpecialPages.gs v1.6.0 or later.');
  }, { skipValidation: true, showSuccess: false });
}
function menuDiagnoseDataQualityAlerts_() {
  return invokeMenuAction_('Diagnose Data Quality Alerts', function () {
    return menuCallFirstAvailable_(['diagnoseDataQualityAlerts_'], [], 'diagnoseDataQualityAlerts_() not found. Deploy 11_SpecialPages.gs v1.6.0 or later.');
  }, { skipValidation: true, showSuccess: false });
}
/* =========================================================
 * Global_Markets extras
 * ========================================================= */
function menuApplyRankOverallGlobalMarkets_() {
  return invokeMenuAction_('Reapply Rank (Overall) — Global_Markets', function () {
    return menuCallFirstAvailable_(['applyRankOverallGlobalMarkets_'], [], 'applyRankOverallGlobalMarkets_() not found. Deploy 07_Global_Markets.gs v1.7.1 or later.');
  }, { skipValidation: true, successMessage: 'Rank pass completed.' });
}
function menuApplyViewTokensGlobalMarkets_() {
  return invokeMenuAction_('Reapply View Tokens — Global_Markets', function () {
    return menuCallFirstAvailable_(['applyViewTokensGlobalMarkets_'], [], 'applyViewTokensGlobalMarkets_() not found. Deploy 07_Global_Markets.gs v1.7.1 or later.');
  }, { skipValidation: true, successMessage: 'View-token pass completed.' });
}
/* =========================================================
 * 99_Diag wrappers
 * ========================================================= */
function menuManualRefreshAndVerifyCurrent_() {
  if (typeof manualRefreshAndVerify !== 'function') {
    throw new Error('manualRefreshAndVerify() not found. Deploy 99_Diag.gs v2.3.0 or later.');
  }
  var pageName = menuGetActivePageName_();
  var ui = SpreadsheetApp.getUi();
  var prompt = ui.prompt('Manual Refresh & Verify — ' + pageName, 'Enter comma-separated symbols, or leave blank to use first symbol on this page.', ui.ButtonSet.OK_CANCEL);
  if (prompt.getSelectedButton() !== ui.Button.OK) return { ok: true, action: 'cancelled' };
  var text = String(prompt.getResponseText() || '').trim();
  var symbols = text ? text.split(',').map(function (sym) { return sym.trim().toUpperCase(); }).filter(String) : [menuGetFirstSymbolFromPage_(pageName)];
  return invokeMenuAction_('Manual Refresh & Verify (Active)', function () {
    return menuRunWithManualRefreshPriority_('Manual Refresh & Verify — ' + pageName, function () {
      return manualRefreshAndVerify(symbols, pageName);
    });
  }, { skipValidation: true, showSuccess: false });
}
function menuAuditCurrentPageColumns_() {
  return invokeMenuAction_('Audit Current Page Columns', function () {
    return menuCallFirstAvailable_(['verifyAllColumnsComplete'], [menuGetActivePageName_()], 'verifyAllColumnsComplete() not found. Deploy 99_Diag.gs v2.3.0 or later.');
  }, { skipValidation: true, showSuccess: false });
}
function menuGetFirstSymbolFromPage_(pageName) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(pageName);
  if (!sheet) throw new Error('Sheet not found: ' + pageName);
  var lastCol = sheet.getLastColumn();
  if (lastCol < 1) throw new Error('Sheet has no columns: ' + pageName);
  var headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0].map(function (h) { return String(h == null ? '' : h).trim(); });
  var symbolCol = headers.indexOf('Symbol');
  if (symbolCol < 0) symbolCol = headers.indexOf('Ticker');
  if (symbolCol < 0) throw new Error('No Symbol/Ticker column on ' + pageName);
  if (sheet.getLastRow() < 2) throw new Error('No data rows on ' + pageName);
  var symbol = String(sheet.getRange(2, symbolCol + 1).getValue() || '').trim().toUpperCase();
  if (!symbol) throw new Error('First data row has blank symbol on ' + pageName);
  return symbol;
}
/* =========================================================
 * Tests and utilities
 * ========================================================= */
function menuTestHealth_() { return invokeMenuAction_('Test Health', function () { return menuCallFirstAvailable_(['testHealth', 'testHealth_'], [], 'testHealth() not found. Deploy 12_Diagnostics.gs.'); }, { successMessage: 'Health test completed successfully.' }); }
function menuTestProviderHealth_() { return invokeMenuAction_('Test Provider Health', function () { return menuCallFirstAvailable_(['testProviderHealth', 'testProviderHealth_'], [], 'testProviderHealth() not found. Deploy 12_Diagnostics.gs v1.7.0 or later.'); }, { successMessage: 'Provider health test completed successfully.' }); }
function menuTestCurrentPageApi_() { return invokeMenuAction_('Test Current Page API', function () { return menuCallFirstAvailable_(['testCurrentPageApi', 'testCurrentPageApi_'], [menuGetActivePageName_()], 'testCurrentPageApi_() not found. Deploy 12_Diagnostics.gs.'); }, { successMessage: 'Current page API test completed successfully.' }); }
function menuTestMetaAndOpenApi_() { return invokeMenuAction_('Test Meta / OpenAPI', function () { return menuCallFirstAvailable_(['testMetaAndOpenApi', 'testMetaAndOpenApi_'], [], 'testMetaAndOpenApi() not found. Deploy 12_Diagnostics.gs.'); }, { successMessage: 'Meta / OpenAPI test completed successfully.' }); }
function menuExportCurrentPageCsv_() { return invokeMenuAction_('Export Current Page CSV', function () { return menuCallFirstAvailable_(['exportCurrentPageCsv', 'exportCurrentPageCsv_'], [menuGetActivePageName_()], 'exportCurrentPageCsv_() not found. Deploy 12_Diagnostics.gs.'); }, { successMessage: 'CSV export completed successfully.' }); }
function menuViewLastRunLog_() { return invokeMenuAction_('View Last Run Log', function () { return menuCallFirstAvailable_(['viewLastRunLog_', 'viewLastRunLog'], [], 'viewLastRunLog_() not found.'); }, { showSuccess: false, skipValidation: true }); }
function menuReloadMenu_() { return invokeMenuAction_('Reload Menu', function () { buildMenu_(); return true; }, { showSuccess: false, skipValidation: true }); }
function menuAbout_() { return invokeMenuAction_('About', function () { return menuCallFirstAvailable_(['aboutTadawulTracker_', 'aboutTadawulTracker'], [], 'aboutTadawulTracker_() not found.'); }, { showSuccess: false, skipValidation: true }); }
/* =========================================================
 * Header diagnostics
 * ========================================================= */
function lookupHeaderKey_(headerText, headerToKeyMap) {
  if (headerText === null || headerText === undefined) return null;
  var trimmed = String(headerText).trim();
  if (!trimmed) return null;
  if (headerToKeyMap[trimmed]) return headerToKeyMap[trimmed];
  var lower = trimmed.toLowerCase();
  var keys = Object.keys(headerToKeyMap || {});
  for (var i = 0; i < keys.length; i++) {
    if (keys[i].toLowerCase() === lower) return headerToKeyMap[keys[i]];
  }
  return null;
}
function diagnosePageHeaders_(pageName) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(pageName);
  if (!sheet) throw new Error('Sheet not found: ' + pageName);
  var lastCol = sheet.getLastColumn();
  if (lastCol < 1) throw new Error('Sheet has no columns: ' + pageName);
  var headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0]
    .map(function (h) { return String(h == null ? '' : h).trim(); })
    .filter(function (h) { return h.length > 0; });
  var headerToKey = (typeof getHeaderToKey === 'function') ? getHeaderToKey(pageName) : {};
  var mapped = [];
  var unmapped = [];
  headers.forEach(function (h) {
    var key = lookupHeaderKey_(h, headerToKey);
    if (key) mapped.push({ header: h, key: key }); else unmapped.push(h);
  });
  var lines = [];
  lines.push('Page: ' + pageName);
  lines.push('Headers: ' + headers.length);
  lines.push('Mapped: ' + mapped.length);
  lines.push('Unmapped: ' + unmapped.length);
  lines.push('');
  lines.push('Unmapped headers:');
  if (!unmapped.length) lines.push('  none');
  unmapped.slice(0, 25).forEach(function (h) { lines.push('  • ' + h); });
  if (unmapped.length > 25) lines.push('  ... and ' + (unmapped.length - 25) + ' more');
  SpreadsheetApp.getUi().alert('Header Diagnostic — ' + pageName, lines.join('\n'), SpreadsheetApp.getUi().ButtonSet.OK);
  return { page: pageName, headers: headers.length, mapped: mapped.length, unmapped: unmapped.length, unmappedHeaders: unmapped };
}
function buildHeaderMappingReport_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var supported = getSupportedPages();
  var allUnmapped = [];
  var sheetsScanned = [];
  for (var i = 0; i < supported.length; i++) {
    var pageName = supported[i];
    var sheet = ss.getSheetByName(pageName);
    if (!sheet) {
      sheetsScanned.push({ name: pageName, mapped: 0, unmapped: 0, missing: true });
      continue;
    }
    var headers = sheet.getLastColumn() ? sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0].map(function (h) { return String(h == null ? '' : h).trim(); }).filter(String) : [];
    var pageMap = (typeof getHeaderToKey === 'function') ? getHeaderToKey(pageName) : {};
    var mapped = 0;
    var unmapped = 0;
    headers.forEach(function (h) {
      if (lookupHeaderKey_(h, pageMap)) mapped++; else { unmapped++; allUnmapped.push({ sheet: pageName, header: h }); }
    });
    sheetsScanned.push({ name: pageName, mapped: mapped, unmapped: unmapped });
  }
  return { totalEntries: (typeof getHeaderToKey === 'function') ? Object.keys(getHeaderToKey()).length : 0, uniqueKeys: 0, sheetsScanned: sheetsScanned, allUnmapped: allUnmapped };
}
function menuShowHeaderMapping_() {
  return invokeMenuAction_('Show Header Mapping', function () {
    var report = buildHeaderMappingReport_();
    var lines = [];
    lines.push('Header → API key mapping');
    lines.push('Total HEADER_TO_KEY entries: ' + report.totalEntries);
    lines.push('');
    report.sheetsScanned.forEach(function (s) { lines.push('• ' + s.name + ': ' + s.mapped + ' mapped, ' + s.unmapped + ' unmapped'); });
    lines.push('');
    lines.push('Total unmapped headers: ' + report.allUnmapped.length);
    SpreadsheetApp.getUi().alert('Header Mapping', lines.join('\n'), SpreadsheetApp.getUi().ButtonSet.OK);
    return report;
  }, { skipValidation: true, showSuccess: false });
}
/* =========================================================
 * Auto-refresh wrappers — candidate-based for compatibility
 * ========================================================= */
function menuInstallAutoRefreshTrigger_() { return invokeMenuAction_('Install Auto-Refresh Trigger', function () { return menuCallFirstAvailable_(['installAutoRefreshTrigger_', 'tfbInstallAutoRefreshTrigger_', 'installAutoRefreshTrigger'], [], 'Auto-refresh install function not found. Deploy 13_AutoRefresh.gs.'); }, { successMessage: 'Auto-refresh trigger installed.' }); }
function menuRemoveAutoRefreshTrigger_() { return invokeMenuAction_('Remove Auto-Refresh Trigger', function () { return menuCallFirstAvailable_(['removeAutoRefreshTrigger_', 'tfbRemoveAutoRefreshTrigger_', 'removeAutoRefreshTrigger'], [], 'Auto-refresh remove function not found. Deploy 13_AutoRefresh.gs.'); }, { successMessage: 'Auto-refresh trigger removed.' }); }
function menuFireAutoRefreshNow_() { return invokeMenuAction_('Fire Auto-Refresh Now', function () { return menuCallFirstAvailable_(['fireAutoRefreshNow_', 'tfbFireAutoRefreshNow_', 'runAutoRefreshNow_', 'autoRefreshNow_'], [], 'Auto-refresh fire-now function not found. Deploy 13_AutoRefresh.gs.'); }, { successMessage: 'Auto-refresh executed.' }); }
function menuShowAutoRefreshStatus_() { return invokeMenuAction_('Show Auto-Refresh Status', function () { return menuCallFirstAvailable_(['showAutoRefreshStatus_', 'tfbShowAutoRefreshStatus_', 'getAutoRefreshStatus_'], [], 'Auto-refresh status function not found. Deploy 13_AutoRefresh.gs.'); }, { showSuccess: false, skipValidation: true }); }
function menuShowManualRefreshPriorityStatus_() {
  return invokeMenuAction_('Show Manual Refresh Priority', function () {
    var status = menuCallFirstAvailable_(
      ['getManualRefreshPriorityStatus', 'getManualRefreshPriorityStatus_'], [],
      'Manual refresh priority status function not found. Deploy 02_Core.gs v2.11.1 or later.'
    );
    var ui = SpreadsheetApp.getUi();
    ui.alert(ROOT_MENU_TITLE_,
      'Manual Refresh Priority\n\n' +
      'Enabled: ' + (status.enabled ? 'Yes' : 'No') + '\n' +
      'Pending: ' + (status.pending ? 'Yes' : 'No') + '\n' +
      'Phase: ' + String(status.phase || 'idle') + '\n' +
      'Action: ' + String(status.label || '-') + '\n' +
      'Requested: ' + String(status.requestedAt || '-') + '\n' +
      'Active: ' + String(status.activeAt || '-') + '\n' +
      'Expires: ' + String(status.expiresAt || '-'),
      ui.ButtonSet.OK);
    return status;
  }, { showSuccess: false, skipValidation: true });
}
function menuClearManualRefreshPriorityEmergency_() {
  if (!confirmAction_(
    'Emergency Clear Manual Refresh Priority',
    'Use this only when no manual refresh is actually running and the priority state is stale.\n\nContinue?'
  )) return { ok: true, action: 'cancelled' };
  return invokeMenuAction_('Emergency Clear Manual Refresh Priority', function () {
    return menuCallFirstAvailable_(
      ['clearManualRefreshPriorityEmergency'], [],
      'Emergency clear function not found. Deploy 02_Core.gs v2.11.1 or later.'
    );
  }, { skipValidation: true, successMessage: 'Manual refresh priority state cleared.' });
}
function menuVerifyManualPriorityWiring_() {
  return invokeMenuAction_('Verify Manual Priority Wiring', function () {
    var checks = [
      { name: 'Menu version 2.20.3', ok: SCRIPT_MENU_VERSION === '2.20.3' },
      { name: 'Sync Hold surface present', ok: typeof tfbSetHold_ === 'function' && typeof tfbHoldCell_ === 'function' },
      { name: 'Core manual wrapper available', ok: typeof withManualRefreshPriority_ === 'function' },
      { name: 'Core priority status available', ok: typeof getManualRefreshPriorityStatus_ === 'function' || typeof getManualRefreshPriorityStatus === 'function' },
      { name: 'Core version 2.11.1', ok: typeof CORE_SCRIPT_VERSION_ !== 'undefined' && CORE_SCRIPT_VERSION_ === '2.11.1' },
      { name: 'Core re-entrant lock depth available', ok: typeof CORE_DOCUMENT_LOCK_DEPTH_ === 'number' },
      { name: 'Refresh cooperative probe available', ok: typeof _refreshManualPriorityProbe_ === 'function' },
      { name: 'Auto trigger version aligned (1.10.0 / 1.10.1)', ok: typeof SCRIPT_AUTOREFRESH_VERSION !== 'undefined' && (SCRIPT_AUTOREFRESH_VERSION === '1.10.0' || SCRIPT_AUTOREFRESH_VERSION === '1.10.1') }
    ];
    var ok = checks.every(function (c) { return c.ok; });
    var lines = checks.map(function (c) { return (c.ok ? 'PASS' : 'FAIL') + ' — ' + c.name; });
    SpreadsheetApp.getUi().alert(
      ROOT_MENU_TITLE_,
      'Manual Priority Wiring: ' + (ok ? 'PASS' : 'FAIL') + '\n\n' + lines.join('\n'),
      SpreadsheetApp.getUi().ButtonSet.OK
    );
    return { ok: ok, version: SCRIPT_MENU_VERSION, checks: checks };
  }, { showSuccess: false, skipValidation: true });
}
function menuToggleAutoResumeOnOpen_() { return invokeMenuAction_('Toggle Auto-Resume On Open', function () { return menuCallFirstAvailable_(['toggleAutoResumeOnOpen_', 'tfbToggleAutoResumeOnOpen_'], [], 'Auto-resume toggle function not found. Deploy 13_AutoRefresh.gs.'); }, { successMessage: 'Auto-resume setting toggled.' }); }
function menuConfigureAutoStartSchedule_() { return invokeMenuAction_('Configure Auto-Refresh Schedule', function () { return menuCallFirstAvailable_(['configureAutoStartSchedule_', 'tfbConfigureAutoStartSchedule_', 'configureAutoRefreshSchedule_'], [], 'Auto-refresh schedule function not found. Deploy 13_AutoRefresh.gs.'); }, { showSuccess: false, skipValidation: true }); }
function menuToggleAuditAfterRefresh_() { return invokeMenuAction_('Toggle Audit After Refresh', function () { return menuCallFirstAvailable_(['toggleAuditAfterRefresh_', 'tfbToggleAuditAfterRefresh_'], [], 'Auto-refresh audit toggle function not found. Deploy 13_AutoRefresh.gs.'); }, { successMessage: 'Audit-after-refresh setting toggled.' }); }
function menuRunCompletenessAuditNow_() { return invokeMenuAction_('Run Completeness Audit Now', function () { return menuCallFirstAvailable_(['runCompletenessAuditNow_', 'tfbRunCompletenessAuditNow_', 'verifyAllColumnsComplete'], [], 'Completeness audit function not found. Deploy 13_AutoRefresh.gs or 99_Diag.gs.'); }, { successMessage: 'Completeness audit completed.' }); }
/* =========================================================
 * Page submenu wrappers
 * ========================================================= */
/* Market_Leaders */
function menuBuild_Market_Leaders_() { return invokeMenuAction_('Build Page Market_Leaders', function () { return menuPageAction_(SHEET_NAMES.MARKET_LEADERS, 'build'); }, { successMessage: 'Market_Leaders build completed.' }); }
function menuNavigate_Market_Leaders_() { return invokeMenuAction_('Navigate Page Market_Leaders', function () { return menuPageAction_(SHEET_NAMES.MARKET_LEADERS, 'navigate'); }, { successMessage: 'Market_Leaders opened.' }); }
function menuHeader_Market_Leaders_() { return invokeMenuAction_('Build Header Market_Leaders', function () { return menuPageAction_(SHEET_NAMES.MARKET_LEADERS, 'header'); }, { successMessage: 'Market_Leaders header built.' }); }
function menuFormat_Market_Leaders_() { return invokeMenuAction_('Apply Format Market_Leaders', function () { return menuPageAction_(SHEET_NAMES.MARKET_LEADERS, 'format'); }, { successMessage: 'Market_Leaders format applied.' }); }
function menuRefresh_Market_Leaders_() { return invokeMenuAction_('Refresh All Data Market_Leaders', function () { return menuPageAction_(SHEET_NAMES.MARKET_LEADERS, 'refresh'); }, { successMessage: 'Market_Leaders refreshed.' }); }
function menuRefreshRow_Market_Leaders_() { return invokeMenuAction_('Refresh Selected Row Market_Leaders', function () { return menuPageAction_(SHEET_NAMES.MARKET_LEADERS, 'refreshRow'); }, { successMessage: 'Selected row refreshed for Market_Leaders.' }); }
function menuRefreshBatches_Market_Leaders_() { return invokeMenuAction_('Refresh in Batches Market_Leaders', function () { return menuPageAction_(SHEET_NAMES.MARKET_LEADERS, 'refreshBatches'); }, { successMessage: 'Market_Leaders batch refresh completed.' }); }
function menuValidate_Market_Leaders_() { return invokeMenuAction_('Validate Page Market_Leaders', function () { return menuPageAction_(SHEET_NAMES.MARKET_LEADERS, 'validate'); }, { successMessage: 'Market_Leaders validated.' }); }
function menuSort_Market_Leaders_() { return invokeMenuAction_('Sort & Rank Page Market_Leaders', function () { return menuPageAction_(SHEET_NAMES.MARKET_LEADERS, 'sort'); }, { successMessage: 'Market_Leaders sorted.' }); }
function menuDiagnose_Market_Leaders_() { return invokeMenuAction_('Diagnose Headers Market_Leaders', function () { return menuPageAction_(SHEET_NAMES.MARKET_LEADERS, 'diagnose'); }, { skipValidation: true, showSuccess: false }); }
/* Global_Markets */
function menuBuild_Global_Markets_() { return invokeMenuAction_('Build Page Global_Markets', function () { return menuPageAction_(SHEET_NAMES.GLOBAL_MARKETS, 'build'); }, { successMessage: 'Global_Markets build completed.' }); }
function menuNavigate_Global_Markets_() { return invokeMenuAction_('Navigate Page Global_Markets', function () { return menuPageAction_(SHEET_NAMES.GLOBAL_MARKETS, 'navigate'); }, { successMessage: 'Global_Markets opened.' }); }
function menuHeader_Global_Markets_() { return invokeMenuAction_('Build Header Global_Markets', function () { return menuPageAction_(SHEET_NAMES.GLOBAL_MARKETS, 'header'); }, { successMessage: 'Global_Markets header built.' }); }
function menuFormat_Global_Markets_() { return invokeMenuAction_('Apply Format Global_Markets', function () { return menuPageAction_(SHEET_NAMES.GLOBAL_MARKETS, 'format'); }, { successMessage: 'Global_Markets format applied.' }); }
function menuRefresh_Global_Markets_() { return invokeMenuAction_('Refresh All Data Global_Markets', function () { return menuPageAction_(SHEET_NAMES.GLOBAL_MARKETS, 'refresh'); }, { successMessage: 'Global_Markets refreshed.' }); }
function menuRefreshRow_Global_Markets_() { return invokeMenuAction_('Refresh Selected Row Global_Markets', function () { return menuPageAction_(SHEET_NAMES.GLOBAL_MARKETS, 'refreshRow'); }, { successMessage: 'Selected row refreshed for Global_Markets.' }); }
function menuRefreshBatches_Global_Markets_() { return invokeMenuAction_('Refresh in Batches Global_Markets', function () { return menuPageAction_(SHEET_NAMES.GLOBAL_MARKETS, 'refreshBatches'); }, { successMessage: 'Global_Markets batch refresh completed.' }); }
function menuValidate_Global_Markets_() { return invokeMenuAction_('Validate Page Global_Markets', function () { return menuPageAction_(SHEET_NAMES.GLOBAL_MARKETS, 'validate'); }, { successMessage: 'Global_Markets validated.' }); }
function menuSort_Global_Markets_() { return invokeMenuAction_('Sort & Rank Page Global_Markets', function () { return menuPageAction_(SHEET_NAMES.GLOBAL_MARKETS, 'sort'); }, { successMessage: 'Global_Markets sorted.' }); }
function menuDiagnose_Global_Markets_() { return invokeMenuAction_('Diagnose Headers Global_Markets', function () { return menuPageAction_(SHEET_NAMES.GLOBAL_MARKETS, 'diagnose'); }, { skipValidation: true, showSuccess: false }); }
/* Commodities_FX */
function menuBuild_Commodities_FX_() { return invokeMenuAction_('Build Page Commodities_FX', function () { return menuPageAction_(SHEET_NAMES.COMMODITIES_FX, 'build'); }, { successMessage: 'Commodities_FX build completed.' }); }
function menuNavigate_Commodities_FX_() { return invokeMenuAction_('Navigate Page Commodities_FX', function () { return menuPageAction_(SHEET_NAMES.COMMODITIES_FX, 'navigate'); }, { successMessage: 'Commodities_FX opened.' }); }
function menuHeader_Commodities_FX_() { return invokeMenuAction_('Build Header Commodities_FX', function () { return menuPageAction_(SHEET_NAMES.COMMODITIES_FX, 'header'); }, { successMessage: 'Commodities_FX header built.' }); }
function menuFormat_Commodities_FX_() { return invokeMenuAction_('Apply Format Commodities_FX', function () { return menuPageAction_(SHEET_NAMES.COMMODITIES_FX, 'format'); }, { successMessage: 'Commodities_FX format applied.' }); }
function menuRefresh_Commodities_FX_() { return invokeMenuAction_('Refresh All Data Commodities_FX', function () { return menuPageAction_(SHEET_NAMES.COMMODITIES_FX, 'refresh'); }, { successMessage: 'Commodities_FX refreshed.' }); }
function menuRefreshRow_Commodities_FX_() { return invokeMenuAction_('Refresh Selected Row Commodities_FX', function () { return menuPageAction_(SHEET_NAMES.COMMODITIES_FX, 'refreshRow'); }, { successMessage: 'Selected row refreshed for Commodities_FX.' }); }
function menuRefreshBatches_Commodities_FX_() { return invokeMenuAction_('Refresh in Batches Commodities_FX', function () { return menuPageAction_(SHEET_NAMES.COMMODITIES_FX, 'refreshBatches'); }, { successMessage: 'Commodities_FX batch refresh completed.' }); }
function menuValidate_Commodities_FX_() { return invokeMenuAction_('Validate Page Commodities_FX', function () { return menuPageAction_(SHEET_NAMES.COMMODITIES_FX, 'validate'); }, { successMessage: 'Commodities_FX validated.' }); }
function menuSort_Commodities_FX_() { return invokeMenuAction_('Sort & Rank Page Commodities_FX', function () { return menuPageAction_(SHEET_NAMES.COMMODITIES_FX, 'sort'); }, { successMessage: 'Commodities_FX sorted.' }); }
function menuDiagnose_Commodities_FX_() { return invokeMenuAction_('Diagnose Headers Commodities_FX', function () { return menuPageAction_(SHEET_NAMES.COMMODITIES_FX, 'diagnose'); }, { skipValidation: true, showSuccess: false }); }
/* Mutual_Funds */
function menuBuild_Mutual_Funds_() { return invokeMenuAction_('Build Page Mutual_Funds', function () { return menuPageAction_(SHEET_NAMES.MUTUAL_FUNDS, 'build'); }, { successMessage: 'Mutual_Funds build completed.' }); }
function menuNavigate_Mutual_Funds_() { return invokeMenuAction_('Navigate Page Mutual_Funds', function () { return menuPageAction_(SHEET_NAMES.MUTUAL_FUNDS, 'navigate'); }, { successMessage: 'Mutual_Funds opened.' }); }
function menuHeader_Mutual_Funds_() { return invokeMenuAction_('Build Header Mutual_Funds', function () { return menuPageAction_(SHEET_NAMES.MUTUAL_FUNDS, 'header'); }, { successMessage: 'Mutual_Funds header built.' }); }
function menuFormat_Mutual_Funds_() { return invokeMenuAction_('Apply Format Mutual_Funds', function () { return menuPageAction_(SHEET_NAMES.MUTUAL_FUNDS, 'format'); }, { successMessage: 'Mutual_Funds format applied.' }); }
function menuRefresh_Mutual_Funds_() { return invokeMenuAction_('Refresh All Data Mutual_Funds', function () { return menuPageAction_(SHEET_NAMES.MUTUAL_FUNDS, 'refresh'); }, { successMessage: 'Mutual_Funds refreshed.' }); }
function menuRefreshRow_Mutual_Funds_() { return invokeMenuAction_('Refresh Selected Row Mutual_Funds', function () { return menuPageAction_(SHEET_NAMES.MUTUAL_FUNDS, 'refreshRow'); }, { successMessage: 'Selected row refreshed for Mutual_Funds.' }); }
function menuRefreshBatches_Mutual_Funds_() { return invokeMenuAction_('Refresh in Batches Mutual_Funds', function () { return menuPageAction_(SHEET_NAMES.MUTUAL_FUNDS, 'refreshBatches'); }, { successMessage: 'Mutual_Funds batch refresh completed.' }); }
function menuValidate_Mutual_Funds_() { return invokeMenuAction_('Validate Page Mutual_Funds', function () { return menuPageAction_(SHEET_NAMES.MUTUAL_FUNDS, 'validate'); }, { successMessage: 'Mutual_Funds validated.' }); }
function menuSort_Mutual_Funds_() { return invokeMenuAction_('Sort & Rank Page Mutual_Funds', function () { return menuPageAction_(SHEET_NAMES.MUTUAL_FUNDS, 'sort'); }, { successMessage: 'Mutual_Funds sorted.' }); }
function menuDiagnose_Mutual_Funds_() { return invokeMenuAction_('Diagnose Headers Mutual_Funds', function () { return menuPageAction_(SHEET_NAMES.MUTUAL_FUNDS, 'diagnose'); }, { skipValidation: true, showSuccess: false }); }
/* My_Portfolio */
function menuBuild_My_Portfolio_() { return invokeMenuAction_('Build Page My_Portfolio', function () { return menuPageAction_(SHEET_NAMES.MY_PORTFOLIO, 'build'); }, { successMessage: 'My_Portfolio build completed.' }); }
function menuNavigate_My_Portfolio_() { return invokeMenuAction_('Navigate Page My_Portfolio', function () { return menuPageAction_(SHEET_NAMES.MY_PORTFOLIO, 'navigate'); }, { successMessage: 'My_Portfolio opened.' }); }
function menuHeader_My_Portfolio_() { return invokeMenuAction_('Build Header My_Portfolio', function () { return menuPageAction_(SHEET_NAMES.MY_PORTFOLIO, 'header'); }, { successMessage: 'My_Portfolio header built.' }); }
function menuFormat_My_Portfolio_() { return invokeMenuAction_('Apply Format My_Portfolio', function () { return menuPageAction_(SHEET_NAMES.MY_PORTFOLIO, 'format'); }, { successMessage: 'My_Portfolio format applied.' }); }
function menuRefresh_My_Portfolio_() { return invokeMenuAction_('Refresh All Data My_Portfolio', function () { return menuPageAction_(SHEET_NAMES.MY_PORTFOLIO, 'refresh'); }, { successMessage: 'My_Portfolio refreshed.' }); }
function menuRefreshRow_My_Portfolio_() { return invokeMenuAction_('Refresh Selected Row My_Portfolio', function () { return menuPageAction_(SHEET_NAMES.MY_PORTFOLIO, 'refreshRow'); }, { successMessage: 'Selected row refreshed for My_Portfolio.' }); }
function menuRefreshBatches_My_Portfolio_() { return invokeMenuAction_('Refresh in Batches My_Portfolio', function () { return menuPageAction_(SHEET_NAMES.MY_PORTFOLIO, 'refreshBatches'); }, { successMessage: 'My_Portfolio batch refresh completed.' }); }
function menuValidate_My_Portfolio_() { return invokeMenuAction_('Validate Page My_Portfolio', function () { return menuPageAction_(SHEET_NAMES.MY_PORTFOLIO, 'validate'); }, { successMessage: 'My_Portfolio validated.' }); }
function menuSort_My_Portfolio_() { return invokeMenuAction_('Sort & Rank Page My_Portfolio', function () { return menuPageAction_(SHEET_NAMES.MY_PORTFOLIO, 'sort'); }, { successMessage: 'My_Portfolio sorted.' }); }
function menuDiagnose_My_Portfolio_() { return invokeMenuAction_('Diagnose Headers My_Portfolio', function () { return menuPageAction_(SHEET_NAMES.MY_PORTFOLIO, 'diagnose'); }, { skipValidation: true, showSuccess: false }); }
/* Insights_Analysis */
function menuBuild_Insights_Analysis_() { return invokeMenuAction_('Build Page Insights_Analysis', function () { return menuPageAction_(SHEET_NAMES.INSIGHTS_ANALYSIS, 'build'); }, { successMessage: 'Insights_Analysis build completed.' }); }
function menuNavigate_Insights_Analysis_() { return invokeMenuAction_('Navigate Page Insights_Analysis', function () { return menuPageAction_(SHEET_NAMES.INSIGHTS_ANALYSIS, 'navigate'); }, { successMessage: 'Insights_Analysis opened.' }); }
function menuHeader_Insights_Analysis_() { return invokeMenuAction_('Build Header Insights_Analysis', function () { return menuPageAction_(SHEET_NAMES.INSIGHTS_ANALYSIS, 'header'); }, { successMessage: 'Insights_Analysis header built.' }); }
function menuFormat_Insights_Analysis_() { return invokeMenuAction_('Apply Format Insights_Analysis', function () { return menuPageAction_(SHEET_NAMES.INSIGHTS_ANALYSIS, 'format'); }, { successMessage: 'Insights_Analysis format applied.' }); }
function menuRefresh_Insights_Analysis_() { return invokeMenuAction_('Refresh All Data Insights_Analysis', function () { return menuPageAction_(SHEET_NAMES.INSIGHTS_ANALYSIS, 'refresh'); }, { successMessage: 'Insights_Analysis refreshed.' }); }
function menuRefreshRow_Insights_Analysis_() { return invokeMenuAction_('Refresh Selected Row Insights_Analysis', function () { return menuPageAction_(SHEET_NAMES.INSIGHTS_ANALYSIS, 'refreshRow'); }, { successMessage: 'Selected row refreshed for Insights_Analysis.' }); }
function menuRefreshBatches_Insights_Analysis_() { return invokeMenuAction_('Refresh in Batches Insights_Analysis', function () { return menuPageAction_(SHEET_NAMES.INSIGHTS_ANALYSIS, 'refreshBatches'); }, { successMessage: 'Insights_Analysis batch refresh completed.' }); }
function menuValidate_Insights_Analysis_() { return invokeMenuAction_('Validate Page Insights_Analysis', function () { return menuPageAction_(SHEET_NAMES.INSIGHTS_ANALYSIS, 'validate'); }, { successMessage: 'Insights_Analysis validated.' }); }
function menuSort_Insights_Analysis_() { return invokeMenuAction_('Sort & Rank Page Insights_Analysis', function () { return menuPageAction_(SHEET_NAMES.INSIGHTS_ANALYSIS, 'sort'); }, { successMessage: 'Insights_Analysis sorted.' }); }
function menuDiagnose_Insights_Analysis_() { return invokeMenuAction_('Diagnose Headers Insights_Analysis', function () { return menuPageAction_(SHEET_NAMES.INSIGHTS_ANALYSIS, 'diagnose'); }, { skipValidation: true, showSuccess: false }); }
/* Top_10_Investments */
function menuBuild_Top_10_Investments_() { return invokeMenuAction_('Build Page Top_10_Investments', function () { return menuPageAction_(SHEET_NAMES.TOP_10_INVESTMENTS, 'build'); }, { successMessage: 'Top_10_Investments build completed.' }); }
function menuNavigate_Top_10_Investments_() { return invokeMenuAction_('Navigate Page Top_10_Investments', function () { return menuPageAction_(SHEET_NAMES.TOP_10_INVESTMENTS, 'navigate'); }, { successMessage: 'Top_10_Investments opened.' }); }
function menuHeader_Top_10_Investments_() { return invokeMenuAction_('Build Header Top_10_Investments', function () { return menuPageAction_(SHEET_NAMES.TOP_10_INVESTMENTS, 'header'); }, { successMessage: 'Top_10_Investments header built.' }); }
function menuFormat_Top_10_Investments_() { return invokeMenuAction_('Apply Format Top_10_Investments', function () { return menuPageAction_(SHEET_NAMES.TOP_10_INVESTMENTS, 'format'); }, { successMessage: 'Top_10_Investments format applied.' }); }
function menuRefresh_Top_10_Investments_() { return invokeMenuAction_('Refresh All Data Top_10_Investments', function () { return menuPageAction_(SHEET_NAMES.TOP_10_INVESTMENTS, 'refresh'); }, { successMessage: 'Top_10_Investments refreshed.' }); }
function menuRefreshRow_Top_10_Investments_() { return invokeMenuAction_('Refresh Selected Row Top_10_Investments', function () { return menuPageAction_(SHEET_NAMES.TOP_10_INVESTMENTS, 'refreshRow'); }, { successMessage: 'Selected row refreshed for Top_10_Investments.' }); }
function menuRefreshBatches_Top_10_Investments_() { return invokeMenuAction_('Refresh in Batches Top_10_Investments', function () { return menuPageAction_(SHEET_NAMES.TOP_10_INVESTMENTS, 'refreshBatches'); }, { successMessage: 'Top_10_Investments batch refresh completed.' }); }
function menuValidate_Top_10_Investments_() { return invokeMenuAction_('Validate Page Top_10_Investments', function () { return menuPageAction_(SHEET_NAMES.TOP_10_INVESTMENTS, 'validate'); }, { successMessage: 'Top_10_Investments validated.' }); }
function menuSort_Top_10_Investments_() { return invokeMenuAction_('Sort & Rank Page Top_10_Investments', function () { return menuPageAction_(SHEET_NAMES.TOP_10_INVESTMENTS, 'sort'); }, { successMessage: 'Top_10_Investments sorted.' }); }
function menuDiagnose_Top_10_Investments_() { return invokeMenuAction_('Diagnose Headers Top_10_Investments', function () { return menuPageAction_(SHEET_NAMES.TOP_10_INVESTMENTS, 'diagnose'); }, { skipValidation: true, showSuccess: false }); }
/* Data_Dictionary */
function menuBuild_Data_Dictionary_() { return invokeMenuAction_('Build Page Data_Dictionary', function () { return menuPageAction_(SHEET_NAMES.DATA_DICTIONARY, 'build'); }, { successMessage: 'Data_Dictionary build completed.' }); }
function menuNavigate_Data_Dictionary_() { return invokeMenuAction_('Navigate Page Data_Dictionary', function () { return menuPageAction_(SHEET_NAMES.DATA_DICTIONARY, 'navigate'); }, { successMessage: 'Data_Dictionary opened.' }); }
function menuHeader_Data_Dictionary_() { return invokeMenuAction_('Build Header Data_Dictionary', function () { return menuPageAction_(SHEET_NAMES.DATA_DICTIONARY, 'header'); }, { successMessage: 'Data_Dictionary header built.' }); }
function menuFormat_Data_Dictionary_() { return invokeMenuAction_('Apply Format Data_Dictionary', function () { return menuPageAction_(SHEET_NAMES.DATA_DICTIONARY, 'format'); }, { successMessage: 'Data_Dictionary format applied.' }); }
function menuRefresh_Data_Dictionary_() { return invokeMenuAction_('Refresh All Data Data_Dictionary', function () { return menuPageAction_(SHEET_NAMES.DATA_DICTIONARY, 'refresh'); }, { successMessage: 'Data_Dictionary refreshed.' }); }
function menuRefreshRow_Data_Dictionary_() { return invokeMenuAction_('Refresh Selected Row Data_Dictionary', function () { return menuPageAction_(SHEET_NAMES.DATA_DICTIONARY, 'refreshRow'); }, { successMessage: 'Selected row refreshed for Data_Dictionary.' }); }
function menuRefreshBatches_Data_Dictionary_() { return invokeMenuAction_('Refresh in Batches Data_Dictionary', function () { return menuPageAction_(SHEET_NAMES.DATA_DICTIONARY, 'refreshBatches'); }, { successMessage: 'Data_Dictionary batch refresh completed.' }); }
function menuValidate_Data_Dictionary_() { return invokeMenuAction_('Validate Page Data_Dictionary', function () { return menuPageAction_(SHEET_NAMES.DATA_DICTIONARY, 'validate'); }, { successMessage: 'Data_Dictionary validated.' }); }
function menuSort_Data_Dictionary_() { return invokeMenuAction_('Sort & Rank Page Data_Dictionary', function () { return menuPageAction_(SHEET_NAMES.DATA_DICTIONARY, 'sort'); }, { successMessage: 'Data_Dictionary sorted.' }); }
function menuDiagnose_Data_Dictionary_() { return invokeMenuAction_('Diagnose Headers Data_Dictionary', function () { return menuPageAction_(SHEET_NAMES.DATA_DICTIONARY, 'diagnose'); }, { skipValidation: true, showSuccess: false }); }
/* =========================================================
 * v2.18.0 TFB Sync Hold — cross-layer manual-hold bridge
 * (writer side; reader = run_dashboard_sync.py v6.32.0)
 * ========================================================= */
var TFB_MH_SHEET_ = '_Sync_Control';
var TFB_MH_KEY_ = 'Manual Hold Until';
function menuTfbHold2h_() {
  return invokeMenuAction_('Hold Auto-Sync 2 Hours', function () {
    return tfbSetHold_(2);
  }, { skipValidation: true, showSuccess: false });
}
function menuTfbHold4h_() {
  return invokeMenuAction_('Hold Auto-Sync 4 Hours', function () {
    return tfbSetHold_(4);
  }, { skipValidation: true, showSuccess: false });
}
function menuTfbReleaseHold_() {
  return invokeMenuAction_('Release Sync Hold', function () {
    return tfbReleaseHold_();
  }, { skipValidation: true, showSuccess: false });
}
function menuTfbHoldStatus_() {
  return invokeMenuAction_('Show Sync Hold Status', function () {
    var cell = tfbHoldCell_();
    var raw = String(cell.getValue() || '').trim();
    var active = false;
    var untilText = '(no hold set)';
    if (raw) {
      var parsed = new Date(raw);
      if (!isNaN(parsed.getTime())) {
        active = parsed.getTime() > Date.now();
        untilText = parsed.toUTCString();
      } else {
        untilText = raw + ' (unparsable — the Python reader treats this as NO hold)';
      }
    }
    SpreadsheetApp.getUi().alert(
      '⏸️ TFB Sync Hold',
      'Hold active: ' + (active ? 'YES' : 'No') + '\n' +
      'Hold until: ' + untilText + '\n' +
      'Cell: ' + TFB_MH_SHEET_ + ' → "' + TFB_MH_KEY_ + '" (raw: ' + (raw || 'blank') + ')\n\n' +
      'Reader: run_dashboard_sync.py v6.32.0 — automatic legs defer at ' +
      'startup and per task while active; expiry is clamped to +12h ' +
      'server-side; resume is OLDEST-FIRST (stalest rows first).',
      SpreadsheetApp.getUi().ButtonSet.OK);
    return { active: active, raw: raw };
  }, { skipValidation: true, showSuccess: false });
}
function tfbSetHold_(hours) {
  var cell = tfbHoldCell_();
  var until = new Date(Date.now() + hours * 3600 * 1000);
  cell.setValue(Utilities.formatDate(until, 'UTC', "yyyy-MM-dd'T'HH:mm:ss'Z'"));
  SpreadsheetApp.getActive().toast(
    'Auto-sync deferred until ' + until.toUTCString() +
    '. Run your manual refresh now; Release (or let it expire) when done — ' +
    'automation resumes from the stalest rows.',
    '⏸️ TFB Sync Hold', 8);
  return true;
}
function tfbReleaseHold_() {
  tfbHoldCell_().clearContent();
  SpreadsheetApp.getActive().toast(
    'Hold released — automatic sync resumes on its next cycle, continuing ' +
    'from the least-recently-refreshed rows (OLDEST-FIRST).',
    '⏸️ TFB Sync Hold', 8);
  return true;
}
function tfbHoldCell_() {
  var ss = SpreadsheetApp.getActive();
  var sh = ss.getSheetByName(TFB_MH_SHEET_);
  if (!sh) {
    sh = ss.insertSheet(TFB_MH_SHEET_);
    sh.getRange('A1:B1').setValues([['Key', 'Value']]).setFontWeight('bold');
    sh.getRange('A2').setValue(TFB_MH_KEY_);
    sh.hideSheet();
  }
  var finder = sh.getRange('A1:A6').createTextFinder(TFB_MH_KEY_).matchEntireCell(true);
  var hit = finder.findNext();
  if (!hit) { sh.getRange('A2').setValue(TFB_MH_KEY_); return sh.getRange('B2'); }
  return sh.getRange(hit.getRow(), 2);
}
/* =========================================================
 * v2.20.1 additions — Today submenu, Portfolio_Decision submenu, GitHub
 * Dispatch submenu, module wrappers and the wiring self-check. Every target is
 * resolved lazily (deploy hint if the module is missing). Nothing above this
 * banner changed except the additive lines noted in the header.
 * ========================================================= */
/* The operator's daily list. Items that also exist on page submenus reuse the
 * same handlers (a deliberate second surface). */
function buildTodayMenu_() {
  var ui = SpreadsheetApp.getUi();
  var m = ui.createMenu('🧭 Today')
    .addItem('🚀 Refresh Portfolio Decision', 'menuDecisionRefreshPortfolio_')
    .addItem('🚀 Refresh Decision Top 10', 'menuDecisionRefreshTop10_')
    .addItem('🌅 Morning Cockpit Refresh (Portfolio, then Top 10)', 'menuMorningCockpitRefresh_')
    .addSeparator()
    .addItem('🪪 IBKR Evidence: Status', 'menuIbkrEvidenceStatus_')
    .addItem('🪪 IBKR Evidence: Run Capture Watch Now', 'menuIbkrCaptureNow_')
    .addItem('📡 Dispatch Daily Sync Now (GitHub)', 'menuDispatchDailySyncNow_')
    .addItem('📡 Sync Dispatch Status', 'menuSyncDispatchStatus_')
    .addItem('📡 S-1 Dispatch Status', 'menuS1DispatchStatus_')
    .addSeparator()
    .addItem('📝 Trade Notes: Setup Tabs', 'menuTradeNotesSetup_')
    .addItem('📝 Log Trade Note (from _Trade_Notes_Input)', 'menuLogTradeNote_')
    .addItem('📝 Score Trade Note (from _Trade_Notes_Input)', 'menuScoreTradeNote_');
  if (typeof cashLedger_addMenuItems === 'function') {
    m.addSeparator();
    try { cashLedger_addMenuItems(m); } catch (cashErr) {}
  }
  /* v2.20.2: Decision Pages v2 (27_Decision_Pages_v2.gs) — present only when deployed. */
  if (typeof dp2_addMenuItems === 'function') {
    m.addSeparator();
    try { dp2_addMenuItems(m); } catch (dp2Err) {}
  }
  m.addSeparator().addItem('🩺 Run Decision Diagnostics', 'menuDecisionDiagnostics_');
  return m;
}
/* Portfolio_Decision is rendered by 10_My_Portfolio's cockpit and is not a
 * getSupportedPages page (no engine contract), so — like the ledger — it gets
 * its own submenu of cockpit-native actions. */
function buildPortfolioDecisionMenu_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('📄 Portfolio_Decision')
    .addItem('📍 Navigate Page', 'menuNavigatePortfolioDecision_')
    .addItem('🚀 Refresh Portfolio Decision', 'menuDecisionRefreshPortfolio_')
    .addItem('🧭 Build Portfolio Decision Layout', 'menuDecisionBuildPortfolio_')
    .addSeparator()
    .addItem('🌱 Seed Portfolio Inputs', 'menuDecisionSeedInputs_')
    .addItem('🔍 Audit Portfolio Inputs', 'menuDecisionAuditInputs_')
    .addItem('🧹 Clean Corrupt Manual Inputs', 'menuDecisionCleanInputs_')
    .addSeparator()
    .addItem('🪪 IBKR Evidence: Status', 'menuIbkrEvidenceStatus_')
    .addItem('⚡ Install IBKR Capture Watcher', 'menuInstallCaptureWatcher_')
    .addItem('🛑 Remove IBKR Capture Watcher', 'menuRemoveCaptureWatcher_')
    .addSeparator()
    .addItem('🧪 Self-Test: Portfolio Cockpit', 'menuDecisionSelfTestPortfolio_');
}
function buildConfigDispatchMenu_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('📡 GitHub Dispatch')
    .addItem('⚡ Install Sync Dispatch Triggers', 'menuInstallSyncDispatchTriggers_')
    .addItem('🛑 Remove Sync Dispatch Triggers', 'menuRemoveSyncDispatchTriggers_')
    .addItem('⚡ Install S-1 Dispatch Triggers', 'menuInstallS1DispatchTriggers_')
    .addItem('🛑 Remove S-1 Dispatch Triggers', 'menuRemoveS1DispatchTriggers_')
    .addItem('🔎 Dispatch Probe (no dispatch)', 'menuSyncDispatchProbe_');
}

/* ---------------------------------------------------------------- wrappers */
function menuNavigatePortfolioDecision_() {
  return invokeMenuAction_('Navigate Portfolio_Decision', function () {
    var name = (typeof DECISION_SHEET_NAMES !== 'undefined' && DECISION_SHEET_NAMES && DECISION_SHEET_NAMES.PORTFOLIO_DECISION) ? DECISION_SHEET_NAMES.PORTFOLIO_DECISION : 'Portfolio_Decision';
    return navigateToPageByName_(name);
  }, { showSuccess: false, skipValidation: true });
}
function menuMorningCockpitRefresh_() {
  return invokeMenuAction_('Morning Cockpit Refresh', function () {
    return menuRunWithManualRefreshPriority_('Morning Cockpit Refresh', function () {
      return menuCallFirstAvailable_(['tfbMorningCockpitRefresh'], [], 'tfbMorningCockpitRefresh() not found. Deploy 16_Decision_Top10.gs with the morning cockpit (v1.11+).');
    });
  }, { successMessage: 'Morning cockpit refresh finished (Portfolio_Decision, then Top_10_Investments). Check both B2 status lines.' });
}
/* IBKR evidence: PD_* script properties + the newest file in the capture folder;
 * reports its age against the 14-minute limit the page enforces. */
function menuIbkrEvidenceStatus_() {
  return invokeMenuAction_('IBKR Evidence Status', function () {
    var props = PropertiesService.getScriptProperties();
    var mode = props.getProperty('PD_IBKR_EVIDENCE') || '(unset)';
    var receipt = props.getProperty('PD_SOURCE_RECEIPT') || '(unset)';
    var folderId = props.getProperty('PD_IBKR_CAPTURE_FOLDER_ID') || '';
    var newest = null;
    var newestName = '';
    var count = 0;
    if (folderId) {
      var files = DriveApp.getFolderById(folderId).getFiles();
      while (files.hasNext()) {
        var f = files.next();
        count++;
        var t = f.getLastUpdated();
        if (!newest || t.getTime() > newest.getTime()) {
          newest = t;
          newestName = f.getName();
        }
      }
    }
    var ageMin = newest ? Math.round((Date.now() - newest.getTime()) / 60000) : null;
    var fresh = (ageMin !== null && ageMin <= 14);
    var text = [
      'Evidence mode: ' + mode + '    Source receipt: ' + receipt,
      'Capture folder: ' + (folderId ? folderId : '(unset)') + '    files: ' + count,
      'Newest capture: ' + (newest ? Utilities.formatDate(newest, 'Asia/Riyadh', 'yyyy-MM-dd HH:mm') + ' Riyadh  (' + newestName + ')' : '(none)'),
      'Age: ' + (ageMin === null ? 'n/a' : ageMin + ' min') + '    Limit: 14 min',
      fresh ? 'FRESH — Portfolio_Decision can certify on the next refresh.' : 'STALE — export a new Flex capture into the folder, then Refresh Portfolio Decision.'
    ].join('\n');
    SpreadsheetApp.getUi().alert('🪪 IBKR Evidence', text, SpreadsheetApp.getUi().ButtonSet.OK);
    return { ageMin: ageMin, fresh: fresh, files: count };
  }, { showSuccess: false, skipValidation: true });
}
function menuIbkrCaptureNow_() {
  return invokeMenuAction_('IBKR Capture Watch', function () {
    return menuCallFirstAvailable_(['pdIbkrCaptureWatch'], [], 'pdIbkrCaptureWatch() not found. Deploy 10_My_Portfolio.gs v1.5.0 or later.');
  }, { successMessage: 'Capture watch ran. Check Portfolio_Decision B2 for the evidence line.' });
}
function menuInstallCaptureWatcher_() {
  return invokeMenuAction_('Install IBKR Capture Watcher', function () {
    return menuCallFirstAvailable_(['pdInstallCaptureWatcher'], [], 'pdInstallCaptureWatcher() not found. Deploy 10_My_Portfolio.gs v1.5.0 or later.');
  }, { successMessage: 'IBKR capture watcher trigger installed.' });
}
function menuRemoveCaptureWatcher_() {
  return invokeMenuAction_('Remove IBKR Capture Watcher', function () {
    return menuCallFirstAvailable_(['pdRemoveCaptureWatcher'], [], 'pdRemoveCaptureWatcher() not found. Deploy 10_My_Portfolio.gs v1.5.0 or later.');
  }, { successMessage: 'IBKR capture watcher trigger removed.' });
}
/* 24_Sync_Dispatch */
function menuDispatchDailySyncNow_() {
  return invokeMenuAction_('Dispatch Daily Sync Now', function () {
    return menuCallFirstAvailable_(['tfbDispatchDailySyncNow'], [], 'tfbDispatchDailySyncNow() not found. Deploy 24_Sync_Dispatch.gs v1.1.0 or later.');
  }, { successMessage: 'Daily sync dispatch requested. Check TFB_SYNC_DISPATCH_LAST_EVENT / Sync Dispatch Status.', skipValidation: true });
}
function menuSyncDispatchStatus_() {
  return invokeMenuAction_('Sync Dispatch Status', function () {
    return menuCallFirstAvailable_(['tfbSyncDispatchStatus'], [], 'tfbSyncDispatchStatus() not found. Deploy 24_Sync_Dispatch.gs v1.1.0 or later.');
  }, { showSuccess: false, skipValidation: true });
}
function menuS1DispatchStatus_() {
  return invokeMenuAction_('S-1 Dispatch Status', function () {
    return menuCallFirstAvailable_(['tfbS1DispatchStatus'], [], 'tfbS1DispatchStatus() not found. Deploy 24_Sync_Dispatch.gs v1.1.0 or later.');
  }, { showSuccess: false, skipValidation: true });
}
function menuSyncDispatchProbe_() {
  return invokeMenuAction_('Dispatch Probe', function () {
    return menuCallFirstAvailable_(['tfbSyncDispatchProbe'], [], 'tfbSyncDispatchProbe() not found. Deploy 24_Sync_Dispatch.gs v1.1.0 or later.');
  }, { showSuccess: false, skipValidation: true });
}
function menuInstallSyncDispatchTriggers_() {
  return invokeMenuAction_('Install Sync Dispatch Triggers', function () {
    return menuCallFirstAvailable_(['tfbInstallSyncDispatchTriggers'], [], 'tfbInstallSyncDispatchTriggers() not found. Deploy 24_Sync_Dispatch.gs v1.1.0 or later.');
  }, { successMessage: 'Sync dispatch triggers installed.', skipValidation: true });
}
function menuRemoveSyncDispatchTriggers_() {
  return invokeMenuAction_('Remove Sync Dispatch Triggers', function () {
    return menuCallFirstAvailable_(['tfbRemoveSyncDispatchTriggers'], [], 'tfbRemoveSyncDispatchTriggers() not found. Deploy 24_Sync_Dispatch.gs v1.1.0 or later.');
  }, { successMessage: 'Sync dispatch triggers removed.', skipValidation: true });
}
function menuInstallS1DispatchTriggers_() {
  return invokeMenuAction_('Install S-1 Dispatch Triggers', function () {
    return menuCallFirstAvailable_(['tfbInstallS1DispatchTriggers'], [], 'tfbInstallS1DispatchTriggers() not found. Deploy 24_Sync_Dispatch.gs v1.1.0 or later.');
  }, { successMessage: 'S-1 dispatch triggers installed.', skipValidation: true });
}
function menuRemoveS1DispatchTriggers_() {
  return invokeMenuAction_('Remove S-1 Dispatch Triggers', function () {
    return menuCallFirstAvailable_(['tfbRemoveS1DispatchTriggers'], [], 'tfbRemoveS1DispatchTriggers() not found. Deploy 24_Sync_Dispatch.gs v1.1.0 or later.');
  }, { successMessage: 'S-1 dispatch triggers removed.', skipValidation: true });
}
function menuSyncDispatchSelfTest_() {
  return invokeMenuAction_('Self-Test: Sync Dispatch', function () {
    return menuCallFirstAvailable_(['tfbSyncDispatchSelfTest'], [], 'tfbSyncDispatchSelfTest() not found. Deploy 24_Sync_Dispatch.gs v1.1.0 or later.');
  }, { showSuccess: false, skipValidation: true });
}
/* 25_Trade_Notes */
function menuTradeNotesSetup_() {
  return invokeMenuAction_('Trade Notes: Setup Tabs', function () {
    return menuCallFirstAvailable_(['tbTradeNotesSetup'], [], 'tbTradeNotesSetup() not found. Deploy 25_Trade_Notes.gs v1.0.0 or later.');
  }, { successMessage: '_Trade_Notes and _Trade_Notes_Input are in place.', skipValidation: true });
}
function menuLogTradeNote_() {
  return invokeMenuAction_('Log Trade Note', function () {
    return menuCallFirstAvailable_(['tbLogTradeNoteFromInput'], [], 'tbLogTradeNoteFromInput() not found. Deploy 25_Trade_Notes.gs v1.0.0 or later.');
  }, { successMessage: 'Trade note logged from _Trade_Notes_Input.', skipValidation: true });
}
function menuScoreTradeNote_() {
  return invokeMenuAction_('Score Trade Note', function () {
    return menuCallFirstAvailable_(['tbScoreTradeNoteFromInput'], [], 'tbScoreTradeNoteFromInput() not found. Deploy 25_Trade_Notes.gs v1.0.0 or later.');
  }, { successMessage: 'Trade note scored from _Trade_Notes_Input.', skipValidation: true });
}
function menuTradeNotesSelfTest_() {
  return invokeMenuAction_('Self-Test: Trade Notes', function () {
    return menuCallFirstAvailable_(['tbTradeNotesSelfTest'], [], 'tbTradeNotesSelfTest() not found. Deploy 25_Trade_Notes.gs v1.0.0 or later.');
  }, { showSuccess: false, skipValidation: true });
}
/* 22_Cash_Ledger */
function menuCashLedgerSelfTest_() {
  return invokeMenuAction_('Self-Test: Cash Ledger', function () {
    return menuCallFirstAvailable_(['cashLedger_selftest'], [], 'cashLedger_selftest() not found. Deploy 22_Cash_Ledger.gs v1.1.0 or later.');
  }, { showSuccess: false, skipValidation: true });
}
/* Wiring self-check: every handler named by the v2.20.1 additions must resolve,
 * and the wired modules should be present. Expected: "Missing handlers: none". */
function menuWiringSelfCheck_() {
  var names = ['menuDecisionRefreshPortfolio_', 'menuDecisionRefreshTop10_', 'menuMorningCockpitRefresh_', 'menuIbkrEvidenceStatus_',
    'menuIbkrCaptureNow_', 'menuDispatchDailySyncNow_', 'menuSyncDispatchStatus_', 'menuS1DispatchStatus_', 'menuTradeNotesSetup_',
    'menuLogTradeNote_', 'menuScoreTradeNote_', 'menuDecisionDiagnostics_', 'menuNavigatePortfolioDecision_', 'menuDecisionBuildPortfolio_',
    'menuDecisionSeedInputs_', 'menuDecisionAuditInputs_', 'menuDecisionCleanInputs_', 'menuInstallCaptureWatcher_', 'menuRemoveCaptureWatcher_',
    'menuDecisionSelfTestPortfolio_', 'menuInstallSyncDispatchTriggers_', 'menuRemoveSyncDispatchTriggers_', 'menuInstallS1DispatchTriggers_',
    'menuRemoveS1DispatchTriggers_', 'menuSyncDispatchProbe_', 'menuTradeNotesSelfTest_', 'menuSyncDispatchSelfTest_', 'menuCashLedgerSelfTest_',
    'menuDecisionBuildTop10_', 'menuDecisionSelfTestTop10_', 'menuAuditTop10Investments_', 'menuTfbHold2h_', 'menuTfbHoldStatus_'];
  var missing = [];
  for (var i = 0; i < names.length; i++) {
    if (typeof menuGetGlobalFunction_(names[i]) !== 'function') missing.push(names[i]);
  }
  var modules = [
    ['22_Cash_Ledger (cashLedger_addMenuItems)', typeof cashLedger_addMenuItems === 'function'],
    ['24_Sync_Dispatch (tfbSyncDispatchStatus)', typeof tfbSyncDispatchStatus === 'function'],
    ['25_Trade_Notes (tbLogTradeNoteFromInput)', typeof tbLogTradeNoteFromInput === 'function'],
    ['10_My_Portfolio capture watcher (pdIbkrCaptureWatch)', typeof pdIbkrCaptureWatch === 'function'],
    ['16_Decision_Top10 morning cockpit (tfbMorningCockpitRefresh)', typeof tfbMorningCockpitRefresh === 'function'],
    ['00_Config ownership registry (isDecisionOwnedPage_)', typeof isDecisionOwnedPage_ === 'function'],
    ['27_Decision_Pages_v2 (dp2_addMenuItems)', typeof dp2_addMenuItems === 'function'],
    ['27_Decision_Pages_v2 menu bar (dp2_addTopMenu)', typeof dp2_addTopMenu === 'function']
  ];
  var modLines = [];
  for (var j = 0; j < modules.length; j++) modLines.push((modules[j][1] ? 'present  ' : 'MISSING  ') + modules[j][0]);
  var msg = 'Menu v' + SCRIPT_MENU_VERSION + ' wiring self-check\n\nHandlers checked: ' + names.length +
    '\nMissing handlers: ' + (missing.length ? missing.join(', ') : 'none') + '\n\nModules:\n' + modLines.join('\n');
  SpreadsheetApp.getUi().alert(ROOT_MENU_TITLE_, msg, SpreadsheetApp.getUi().ButtonSet.OK);
  return { missing: missing, modules: modules };
}
