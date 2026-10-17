/* ============================================================================
 * 01_Menu.gs — v2.19.0 APPEND BLOCK (decision-first menu)
 * ----------------------------------------------------------------------------
 * Paste this whole block at the END of 01_Menu.gs (after the last line of the
 * TFB Sync Hold section), then apply the four one-line anchored edits listed in
 * the commit sheet:
 *   L354  var SCRIPT_MENU_VERSION = '2.18.0';   ->  '2.19.0'
 *   L355  var ROOT_MENU_TITLE_ = '👤 Emad Bahbah Financial Advisor';  ->  '🧭 TFB Cockpit'
 *   L373  function buildMenu_() {               ->  function buildMenuLegacy_() {
 *   L616  function menuIsDecisionOwnedPage_(pageName) {  ->  function menuIsDecisionOwnedPageLegacy_(pageName) {
 *   L1049 function menuDecisionOwnedAction_(page, actionKey) {  ->  function menuDecisionOwnedActionLegacy_(page, actionKey) {
 * Nothing else in the file changes. onOpen()/onInstall() keep calling
 * buildMenu_(), which now resolves to the v2.19.0 builder below. Rollback =
 * undo the five renames (the legacy builders are still in the file).
 *
 * What changes for the operator (see TFB_Decision_Pages_Format_Menu_Review §2):
 *   - Root menu "🧭 TFB Cockpit" opens with "🧭 Today": the two daily refreshes,
 *     the morning cockpit run, IBKR evidence status/capture, GitHub dispatch,
 *     trade notes, cash ledger entries and decision diagnostics.
 *   - Decision-owned pages (Top_10_Investments, Portfolio_Decision) carry no
 *     generic engine items; only navigate / validate / diagnose plus their own
 *     cockpit actions.
 *   - My_Portfolio keeps refresh/validate/format; Build / Header / Sort sit
 *     behind a confirm dialog (membership is ledger-owned, 21_Portfolio_Ledger).
 *   - Every handler appears once (the two daily refreshes also sit in Today by
 *     design): ~25 duplicate entries and 7 dead Top 10 items removed; 11
 *     top-level submenus instead of 16. Destructive global actions (trim /
 *     reformat all) are confirm-guarded under Utilities.
 *   - Modules that had no UI are wired: 22_Cash_Ledger (cashLedger_addMenuItems),
 *     24_Sync_Dispatch, 25_Trade_Notes, 10_My_Portfolio capture watcher.
 *   - Sync Hold stays a second top-level menu, renamed "⏸️ TFB Hold".
 * Conventions: ES5 only; every target resolved lazily through
 * menuCallFirstAvailable_ so a missing module yields a deploy hint, never a
 * broken menu.
 * ========================================================================== */

/* Pages the menu treats as decision-owned even before 00_Config lists them. */
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

/* ---------------------------------------------------------------- builder */
function buildMenu_() {
  var ui = SpreadsheetApp.getUi();
  ui.createMenu(ROOT_MENU_TITLE_)
    .addSubMenu(buildTodayMenu_())
    .addSeparator()
    .addSubMenu(buildDecisionPageSubMenu_(SHEET_NAMES.TOP_10_INVESTMENTS, getPageMenuHandlers_(SHEET_NAMES.TOP_10_INVESTMENTS), getTop10DecisionExtrasV2_()))
    .addSubMenu(buildPortfolioDecisionMenu_())
    .addSubMenu(buildMyPortfolioMenu_())
    .addSubMenu(buildPortfolioLedgerMenuV2_())
    .addSeparator()
    .addSubMenu(buildActivePageMenuV2_())
    .addSubMenu(buildEnginePagesMenu_())
    .addSeparator()
    .addSubMenu(buildLayoutsMenu_())
    .addSubMenu(buildConfigurationMenuV2_())
    .addSubMenu(buildDiagnosticsMenu_())
    .addSubMenu(buildUtilitiesMenuV2_())
    .addToUi();
  buildSyncHoldMenuV2_(ui);
}

function buildSyncHoldMenuV2_(ui) {
  ui.createMenu('⏸️ TFB Hold')
    .addItem('✋ Hold Auto-Sync 2 Hours', 'menuTfbHold2h_')
    .addItem('✋ Hold Auto-Sync 4 Hours', 'menuTfbHold4h_')
    .addItem('▶️ Release Hold Now', 'menuTfbReleaseHold_')
    .addItem('📟 Show Hold Status', 'menuTfbHoldStatus_')
    .addToUi();
}

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
    .addItem('📝 Log Trade Note (from _Trade_Notes_Input)', 'menuLogTradeNote_')
    .addItem('📝 Score Trade Note (from _Trade_Notes_Input)', 'menuScoreTradeNote_');
  if (typeof cashLedger_addMenuItems === 'function') {
    m.addSeparator();
    try { cashLedger_addMenuItems(m); } catch (cashErr) {}
  }
  m.addSeparator().addItem('🩺 Run Decision Diagnostics', 'menuDecisionDiagnostics_');
  return m;
}

/* Decision-owned page: read-only generic items + the page's own cockpit actions. */
function buildDecisionPageSubMenu_(pageName, handlers, extras) {
  var ui = SpreadsheetApp.getUi();
  var menu = ui.createMenu('📄 ' + pageName)
    .addItem('📍 Navigate Page', handlers.navigate)
    .addItem('✅ Validate Page (read-only)', handlers.validate)
    .addItem('🔬 Diagnose Headers (read-only)', handlers.diagnose);
  if (extras && extras.length) {
    menu.addSeparator();
    for (var i = 0; i < extras.length; i++) {
      menu.addItem(extras[i].label, extras[i].handler);
    }
  }
  return menu;
}

function getTop10DecisionExtrasV2_() {
  return [
    { label: '🚀 Refresh Decision Top 10', handler: 'menuDecisionRefreshTop10_' },
    { label: '🧭 Build Decision Cockpit Layout', handler: 'menuDecisionBuildTop10_' },
    { label: '🏆 Audit Top_10_Investments', handler: 'menuAuditTop10Investments_' }
  ];
}

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
    .addItem('⚡ Install IBKR Capture Watcher', 'menuInstallCaptureWatcher_')
    .addItem('🛑 Remove IBKR Capture Watcher', 'menuRemoveCaptureWatcher_');
}

/* My_Portfolio: engine page whose membership is ledger-owned (21). Refresh and
 * read-only items stay direct; Build / Header / Sort require a confirm. */
function buildMyPortfolioMenu_() {
  var ui = SpreadsheetApp.getUi();
  var h = getPageMenuHandlers_(SHEET_NAMES.MY_PORTFOLIO);
  return ui.createMenu('📄 My_Portfolio')
    .addItem('📍 Navigate Page', h.navigate)
    .addItem('🔄 Refresh All Data', h.refresh)
    .addItem('🎯 Refresh Selected Row', h.refreshRow)
    .addItem('🧩 Refresh in Batches', h.refreshBatches)
    .addItem('🎨 Apply Format', h.format)
    .addItem('✅ Validate Page', h.validate)
    .addItem('🔬 Diagnose Headers', h.diagnose)
    .addSeparator()
    .addItem('🔗 Sync Portfolio from Ledger', 'menuPortfolioSyncLedger_')
    .addSeparator()
    .addItem('⚠️ Build Page (confirm — ledger-owned membership)', 'menuGuardedBuildMyPortfolio_')
    .addItem('⚠️ Build Header (confirm)', 'menuGuardedHeaderMyPortfolio_')
    .addItem('⚠️ Sort & Rank Page (confirm)', 'menuGuardedSortMyPortfolio_');
}

function buildPortfolioLedgerMenuV2_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('📒 Portfolio Ledger')
    .addItem('📍 Navigate Ledger (_Portfolio_CostBasis)', 'menuLedgerNavigate_')
    .addItem('♻️ Refresh Portfolio Ledger', 'menuLedgerRefresh_')
    .addItem('💰 Add Manual Dividend…', 'menuLedgerAddDividend_')
    .addItem('🩺 Dividend Fetch Diagnostic', 'menuLedgerDivDiag_');
}

function buildActivePageMenuV2_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('📍 Active Page')
    .addItem('📍 Navigate Current Page', 'menuNavigateCurrentPage_')
    .addItem('🔄 Refresh Current Full Data', 'menuRefreshCurrentPage_')
    .addItem('🎯 Refresh Selected Row', 'menuRefreshSelectedRowCurrent_')
    .addItem('🧩 Refresh Current Page In Batches', 'menuRefreshCurrentPageInBatches_')
    .addItem('🔁 Manual Refresh & Verify (Active)', 'menuManualRefreshAndVerifyCurrent_')
    .addSeparator()
    .addItem('🛠️ Build Current Page', 'menuBuildCurrentPage_')
    .addItem('🧱 Build Current Header', 'menuUpdateCurrentHeaders_')
    .addItem('🎨 Apply Current Format', 'menuReformatCurrentPage_')
    .addItem('📈 Sort & Rank Current Page', 'menuSortAndRankCurrentPage_')
    .addSeparator()
    .addItem('✅ Validate Current Page', 'menuValidateCurrentPage_')
    .addItem('🔬 Diagnose Headers', 'menuDiagnoseCurrentHeaders_')
    .addItem('🔍 Audit Current Page Columns', 'menuAuditCurrentPageColumns_')
    .addItem('🔣 Show Current Page Symbols', 'menuShowCurrentPageSymbols_')
    .addItem('⚙️ Manage Current Page Symbols', 'menuManageCurrentPageSymbols_');
}

function buildEnginePagesMenu_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('📄 Engine Pages')
    .addItem('🔄 Refresh All Pages', 'menuRefreshAllPages_')
    .addItem('📊 Refresh Analysis & Insights', 'menuRefreshAnalysisAndInsights_')
    .addSeparator()
    .addSubMenu(buildPageSubMenu_(SHEET_NAMES.MARKET_LEADERS, getPageMenuHandlers_(SHEET_NAMES.MARKET_LEADERS)))
    .addSubMenu(buildPageSubMenu_(SHEET_NAMES.GLOBAL_MARKETS, getPageMenuHandlers_(SHEET_NAMES.GLOBAL_MARKETS), getGlobalMarketsExtras_()))
    .addSubMenu(buildPageSubMenu_(SHEET_NAMES.COMMODITIES_FX, getPageMenuHandlers_(SHEET_NAMES.COMMODITIES_FX)))
    .addSubMenu(buildPageSubMenu_(SHEET_NAMES.MUTUAL_FUNDS, getPageMenuHandlers_(SHEET_NAMES.MUTUAL_FUNDS)))
    .addSubMenu(buildPageSubMenu_(SHEET_NAMES.INSIGHTS_ANALYSIS, getPageMenuHandlers_(SHEET_NAMES.INSIGHTS_ANALYSIS)))
    .addSubMenu(buildPageSubMenu_(SHEET_NAMES.DATA_DICTIONARY, getPageMenuHandlers_(SHEET_NAMES.DATA_DICTIONARY)));
}

function buildLayoutsMenu_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('🧱 Layouts')
    .addItem('📒 Build Portfolio Ledger Layout', 'menuLedgerBuild_')
    .addItem('🧱 Build _Lists_Config Defaults', 'menuDecisionBuildListsConfig_')
    .addItem('📝 Trade Notes: Setup Tabs', 'menuTradeNotesSetup_')
    .addSeparator()
    .addItem('🧱 Ensure All Sheets & Headers', 'menuEnsureAllSheetsAndHeaders_');
}

function buildConfigurationMenuV2_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('⚙️ Configuration')
    .addSubMenu(buildConfigConnectionMenu_())
    .addSubMenu(buildConfigAutoRefreshMenu_())
    .addSubMenu(buildConfigScheduleAuditMenu_())
    .addSubMenu(buildConfigDispatchMenu_())
    .addSeparator()
    .addSubMenu(buildConfigVerifyMenu_());
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

function buildDiagnosticsMenu_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('🧪 Diagnostics & Tests')
    .addItem('💓 Test Health', 'menuTestHealth_')
    .addItem('🛰️ Test Provider Health', 'menuTestProviderHealth_')
    .addItem('🔍 Test Current Page API', 'menuTestCurrentPageApi_')
    .addItem('📡 Test Meta / OpenAPI', 'menuTestMetaAndOpenApi_')
    .addItem('🖐️ Verify Manual Priority Wiring', 'menuVerifyManualPriorityWiring_')
    .addSeparator()
    .addItem('🧪 Self-Test: Top 10 Cockpit', 'menuDecisionSelfTestTop10_')
    .addItem('🧪 Self-Test: Portfolio Cockpit', 'menuDecisionSelfTestPortfolio_')
    .addItem('🧪 Self-Test: Ledger', 'menuLedgerSelfTest_')
    .addItem('🧪 Self-Test: Decision Diagnostics', 'menuDecisionSelfTestDiagnostics_')
    .addItem('🧪 Self-Test: Trade Notes', 'menuTradeNotesSelfTest_')
    .addItem('🧪 Self-Test: Sync Dispatch', 'menuSyncDispatchSelfTest_')
    .addItem('🧪 Self-Test: Cash Ledger', 'menuCashLedgerSelfTest_')
    .addSeparator()
    .addItem('🧪 Run Full Diagnostic Sweep', 'menuRunFullDiagnosticSweep_')
    .addItem('🧾 Run Data Audit All Pages', 'menuAuditAllPages_')
    .addItem('🌍 Audit Global_Markets', 'menuAuditGlobalMarkets_')
    .addItem('🔎 Verify 115/118 Schema Propagation', 'menuVerifyV28SchemaPropagation_')
    .addItem('🗺️ Diagnose Schema Contract Status', 'menuDiagnoseSchemaContractStatus_')
    .addItem('🎯 Diagnose Criteria Visibility', 'menuDiagnoseV310CriteriaVisibility_')
    .addItem('🚨 Diagnose Data Quality Alerts', 'menuDiagnoseDataQualityAlerts_');
}

function buildUtilitiesMenuV2_() {
  var ui = SpreadsheetApp.getUi();
  return ui.createMenu('🧰 Utilities')
    .addItem('⚠️ Trim Current Sheet To Expected Columns (confirm)', 'menuGuardedTrimCurrentSheet_')
    .addItem('⚠️ Trim All Sheets To Expected Columns (confirm)', 'menuGuardedTrimAllSheets_')
    .addItem('⚠️ Reformat All Sheets (confirm)', 'menuGuardedReformatAllSheets_')
    .addSeparator()
    .addItem('📤 Export Current Page CSV', 'menuExportCurrentPageCsv_')
    .addItem('📝 View Last Run Log', 'menuViewLastRunLog_')
    .addItem('🔁 Reload Menu', 'menuReloadMenu_')
    .addItem('ℹ️ About', 'menuAbout_');
}

/* ---------------------------------------------------------------- ownership */
/* Route a decision-owned page's blocked engine action to ITS cockpit (v2.19.0:
 * page-aware — Portfolio_Decision goes to 10_My_Portfolio, Top 10 to 16). */
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
  return {
    ok: true,
    page: page,
    action: actionKey,
    status: 'skipped_decision_owned',
    message: '"' + page + '" is a Decision cockpit page; "' + actionKey + '" does not apply ' +
      '(the cockpit is rebuilt and refreshed as a whole). Use the page\'s own Build / Refresh items instead.'
  };
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

/* IBKR evidence: reads the PD_* script properties and the newest file in the
 * capture folder; reports age against the 14-minute limit used by the page. */
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
    var lines = [
      'Evidence mode: ' + mode + '    Source receipt: ' + receipt,
      'Capture folder: ' + (folderId ? folderId : '(unset)') + '    files: ' + count,
      'Newest capture: ' + (newest ? Utilities.formatDate(newest, 'Asia/Riyadh', 'yyyy-MM-dd HH:mm') + ' Riyadh  (' + newestName + ')' : '(none)'),
      'Age: ' + (ageMin === null ? 'n/a' : ageMin + ' min') + '    Limit: 14 min',
      fresh ? 'FRESH — Portfolio_Decision can certify on the next refresh.' : 'STALE — export a new Flex capture into the folder, then Refresh Portfolio Decision.'
    ];
    SpreadsheetApp.getUi().alert('🪪 IBKR Evidence', lines.join('\n'), SpreadsheetApp.getUi().ButtonSet.OK);
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

/* 24_Sync_Dispatch — all targets resolved lazily. */
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

/* ---------------------------------------------------------------- guards */
function menuConfirmDestructive_(title, message) {
  var ui = SpreadsheetApp.getUi();
  var res = ui.alert(ROOT_MENU_TITLE_ + ' — ' + title, message + '\n\nThis rewrites sheet content. Continue?', ui.ButtonSet.OK_CANCEL);
  return res === ui.Button.OK;
}
function menuGuardedBuildMyPortfolio_() {
  if (!menuConfirmDestructive_('Build Page My_Portfolio', 'My_Portfolio membership is owned by the Portfolio Ledger (21_Portfolio_Ledger). Build re-lays the engine columns and headers; run "Sync Portfolio from Ledger" afterwards.')) return { ok: false, status: 'cancelled' };
  return menuBuild_My_Portfolio_();
}
function menuGuardedHeaderMyPortfolio_() {
  if (!menuConfirmDestructive_('Build Header My_Portfolio', 'Rewrites the My_Portfolio header row from the schema contract.')) return { ok: false, status: 'cancelled' };
  return menuHeader_My_Portfolio_();
}
function menuGuardedSortMyPortfolio_() {
  if (!menuConfirmDestructive_('Sort & Rank My_Portfolio', 'Re-orders the holdings rows; the ledger upsert keys on symbol, so membership is unaffected.')) return { ok: false, status: 'cancelled' };
  return menuSort_My_Portfolio_();
}
function menuGuardedTrimCurrentSheet_() {
  if (!menuConfirmDestructive_('Trim Current Sheet', 'Deletes columns beyond the expected schema width on the ACTIVE sheet.')) return { ok: false, status: 'cancelled' };
  return menuTrimCurrentSheetToExpectedColumns_();
}
function menuGuardedTrimAllSheets_() {
  if (!menuConfirmDestructive_('Trim All Sheets', 'Deletes columns beyond the expected schema width on EVERY engine sheet.')) return { ok: false, status: 'cancelled' };
  return menuTrimAllSheetsToExpectedColumns_();
}
function menuGuardedReformatAllSheets_() {
  if (!menuConfirmDestructive_('Reformat All Sheets', 'Re-applies formatting to EVERY engine sheet (decision cockpits are skipped by ownership).')) return { ok: false, status: 'cancelled' };
  return menuReformatAllSheets_();
}

/* ---------------------------------------------------------------- self-check */
/* Run once after pasting: lists any menu handler that does not resolve to a
 * global function. Expected result: an empty list. */
function menuV2SelfCheck_() {
  var names = ['menuDecisionRefreshPortfolio_', 'menuDecisionRefreshTop10_', 'menuMorningCockpitRefresh_', 'menuIbkrEvidenceStatus_', 'menuIbkrCaptureNow_',
    'menuDispatchDailySyncNow_', 'menuSyncDispatchStatus_', 'menuS1DispatchStatus_', 'menuLogTradeNote_', 'menuScoreTradeNote_', 'menuDecisionDiagnostics_',
    'menuNavigatePortfolioDecision_', 'menuDecisionBuildPortfolio_', 'menuDecisionSeedInputs_', 'menuDecisionAuditInputs_', 'menuDecisionCleanInputs_',
    'menuInstallCaptureWatcher_', 'menuRemoveCaptureWatcher_', 'menuPortfolioSyncLedger_', 'menuGuardedBuildMyPortfolio_', 'menuGuardedHeaderMyPortfolio_',
    'menuGuardedSortMyPortfolio_', 'menuLedgerNavigate_', 'menuLedgerRefresh_', 'menuLedgerAddDividend_', 'menuLedgerDivDiag_', 'menuNavigateCurrentPage_',
    'menuRefreshCurrentPage_', 'menuRefreshSelectedRowCurrent_', 'menuRefreshCurrentPageInBatches_', 'menuManualRefreshAndVerifyCurrent_', 'menuBuildCurrentPage_',
    'menuUpdateCurrentHeaders_', 'menuReformatCurrentPage_', 'menuSortAndRankCurrentPage_', 'menuValidateCurrentPage_', 'menuDiagnoseCurrentHeaders_',
    'menuAuditCurrentPageColumns_', 'menuShowCurrentPageSymbols_', 'menuManageCurrentPageSymbols_', 'menuRefreshAllPages_', 'menuRefreshAnalysisAndInsights_',
    'menuLedgerBuild_', 'menuDecisionBuildListsConfig_', 'menuTradeNotesSetup_', 'menuEnsureAllSheetsAndHeaders_', 'menuInstallSyncDispatchTriggers_',
    'menuRemoveSyncDispatchTriggers_', 'menuInstallS1DispatchTriggers_', 'menuRemoveS1DispatchTriggers_', 'menuSyncDispatchProbe_', 'menuTestHealth_',
    'menuTestProviderHealth_', 'menuTestCurrentPageApi_', 'menuTestMetaAndOpenApi_', 'menuVerifyManualPriorityWiring_', 'menuDecisionSelfTestTop10_',
    'menuDecisionSelfTestPortfolio_', 'menuLedgerSelfTest_', 'menuDecisionSelfTestDiagnostics_', 'menuTradeNotesSelfTest_', 'menuSyncDispatchSelfTest_',
    'menuCashLedgerSelfTest_', 'menuRunFullDiagnosticSweep_', 'menuAuditAllPages_', 'menuAuditGlobalMarkets_', 'menuAuditTop10Investments_',
    'menuVerifyV28SchemaPropagation_', 'menuDiagnoseSchemaContractStatus_', 'menuDiagnoseV310CriteriaVisibility_', 'menuDiagnoseDataQualityAlerts_',
    'menuGuardedTrimCurrentSheet_', 'menuGuardedTrimAllSheets_', 'menuGuardedReformatAllSheets_', 'menuExportCurrentPageCsv_', 'menuViewLastRunLog_',
    'menuReloadMenu_', 'menuAbout_', 'menuTfbHold2h_', 'menuTfbHold4h_', 'menuTfbReleaseHold_', 'menuTfbHoldStatus_', 'menuDecisionBuildTop10_'];
  var missing = [];
  for (var i = 0; i < names.length; i++) {
    var fn = menuGetGlobalFunction_(names[i]);
    if (typeof fn !== 'function') missing.push(names[i]);
  }
  var modules = {
    '22_Cash_Ledger': typeof cashLedger_addMenuItems === 'function',
    '24_Sync_Dispatch': typeof tfbSyncDispatchStatus === 'function',
    '25_Trade_Notes': typeof tbLogTradeNoteFromInput === 'function',
    '10_My_Portfolio capture watcher': typeof pdIbkrCaptureWatch === 'function',
    '16 morning cockpit': typeof tfbMorningCockpitRefresh === 'function'
  };
  var modLines = [];
  for (var k in modules) if (modules.hasOwnProperty(k)) modLines.push(k + ': ' + (modules[k] ? 'present' : 'MISSING'));
  var msg = 'Menu v' + SCRIPT_MENU_VERSION + ' self-check\n\nHandlers checked: ' + names.length + '\nMissing handlers: ' + (missing.length ? missing.join(', ') : 'none') + '\n\nModules:\n' + modLines.join('\n');
  SpreadsheetApp.getUi().alert(ROOT_MENU_TITLE_, msg, SpreadsheetApp.getUi().ButtonSet.OK);
  return { missing: missing, modules: modules };
}
