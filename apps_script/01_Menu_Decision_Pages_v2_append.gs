/* ============================================================================
 * 01_Menu.gs - DECISION PAGES v2 MENU BLOCK - v1.0.0 (2026-10-19)
 * ----------------------------------------------------------------------------
 * WHAT: a "🆕 Decision Pages v2" menu in the menu bar with on-demand refresh of
 * the two new tabs, Portfolio_Decision_v2 and Top_10_Investments_v2:
 *   🔄 Refresh Portfolio_Decision_v2
 *   🔄 Refresh Top_10_Investments_v2
 *   🔄 Refresh both (Portfolio, then Top 10)
 *   📍 Open Portfolio_Decision_v2 / 📍 Open Top_10_Investments_v2
 *   ⚙️ Set up refresh functions   🩺 Status
 *
 * INSTALL (owner, Apps Script editor, about 5 minutes):
 *   1. Paste this whole block at the END of 01_Menu.gs (after the last line).
 *      It may instead be added as its own file; never both. Ctrl+S.
 *   2. In onOpen() in 01_Menu.gs, directly after the line that builds the main
 *      menu (buildMenu_();), add this one line:
 *          tfbPagesV2AddMenu_();
 *      To show the items inside the main menu instead of the menu bar, add
 *      .addSubMenu(tfbPagesV2Menu_(ui)) to the main menu chain in buildMenu_()
 *      (before .addToUi()) and skip the onOpen line.
 *   3. Reload the spreadsheet: the menu bar shows "🆕 Decision Pages v2".
 *   4. Click "⚙️ Set up refresh functions" once (a Refresh item does the same
 *      on first use). It lists the project functions that look like each v2
 *      renderer and asks you to confirm one per page; the choice is saved as
 *      Script Property TFB_V2_PD_REFRESH_FN / TFB_V2_T10_REFRESH_FN.
 *
 * WHY a setup step: the code that renders the two _v2 tabs is not in the
 * repository (docs/apps-script-source-prerequisites.md, 19 Oct), so its
 * function names cannot be hard-coded without guessing. The block never runs a
 * function the owner has not confirmed; a configured name that stops resolving
 * asks again instead of falling back to a guess.
 *
 * SAFETY:
 *   - Manual only: no trigger, no onOpen of its own, no automatic writer.
 *   - Each page refresh runs through menuRunWithManualRefreshPriority_ when the
 *     live menu defines it (automatic refresh yields), otherwise directly.
 *   - A second click while a page refresh is running is refused (document
 *     cache flag, claimed under the document lock, cleared in finally,
 *     expires after 7 minutes if an execution dies).
 *   - Suggestions skip private helpers (trailing _), the v1 refreshers, and any
 *     name with test / install / remove / delete / clear / reset / purge /
 *     trigger / status / diag / debug / menu / setup / probe / audit / log /
 *     seed / layout in it. A typed name may be any function except this
 *     block's own.
 *   - When pasted after the v2.19.0 block, both _v2 tabs join
 *     MENU_DECISION_OWNED_EXTRA_, so "Active Page > Refresh" does not run the
 *     generic engine refresh on them. Without v2.19.0 this is a no-op.
 *
 * ROLLBACK: delete the one onOpen line (the menu disappears; the block is inert
 * without it), optionally delete the block and the two Script Properties.
 * Conventions: ES5 (var, function), no service call at load time.
 * ========================================================================== */

var TFB_PAGES_V2_MENU_VERSION = '1.0.0';
var TFB_PAGES_V2_MENU_TITLE_ = '🆕 Decision Pages v2';
var TFB_PAGES_V2_INFLIGHT_TTL_S_ = 420;
var TFB_PAGES_V2_ = {
  pd: {
    key: 'pd', sheet: 'Portfolio_Decision_v2', prop: 'TFB_V2_PD_REFRESH_FN',
    hints: ['portfoliodecision', 'portfolio', 'pdv2', 'pd2']
  },
  t10: {
    key: 't10', sheet: 'Top_10_Investments_v2', prop: 'TFB_V2_T10_REFRESH_FN',
    hints: ['top10', 't10', 'topten', 'investments']
  }
};
var TFB_PAGES_V2_EXCLUDE_ = ['test', 'selftest', 'install', 'uninstall', 'remove', 'delete', 'clear',
  'reset', 'purge', 'trigger', 'triggers', 'status', 'diag', 'diagnose', 'diagnostics', 'debug',
  'menu', 'onopen', 'onedit', 'oninstall', 'setup', 'probe', 'audit', 'log', 'seed', 'layout'];
var TFB_PAGES_V2_STRONG_VERBS_ = ['refresh', 'render', 'run', 'update', 'rebuild', 'generate', 'publish'];
var TFB_PAGES_V2_WEAK_VERBS_ = ['build', 'write', 'make', 'compute', 'create', 'draw'];

/* Both _v2 tabs are decision-owned for the v2.19.0 menu (no generic engine
 * refresh). Runs at load; effective only when MENU_DECISION_OWNED_EXTRA_ is
 * already defined, i.e. this block sits after the v2.19.0 block. */
(function tfbPagesV2MarkDecisionOwned_() {
  try {
    if (typeof MENU_DECISION_OWNED_EXTRA_ === 'undefined' || !MENU_DECISION_OWNED_EXTRA_ ||
        typeof MENU_DECISION_OWNED_EXTRA_.push !== 'function') return;
    var names = [TFB_PAGES_V2_.pd.sheet, TFB_PAGES_V2_.t10.sheet];
    for (var i = 0; i < names.length; i++) {
      if (MENU_DECISION_OWNED_EXTRA_.indexOf(names[i]) < 0) MENU_DECISION_OWNED_EXTRA_.push(names[i]);
    }
  } catch (ownErr) {}
})();

/* ---------------------------------------------------------------- menu */
function tfbPagesV2Menu_(ui) {
  return ui.createMenu(TFB_PAGES_V2_MENU_TITLE_)
    .addItem('🔄 Refresh Portfolio_Decision_v2', 'tfbPagesV2RefreshPortfolioDecision')
    .addItem('🔄 Refresh Top_10_Investments_v2', 'tfbPagesV2RefreshTop10')
    .addItem('🔄 Refresh both (Portfolio, then Top 10)', 'tfbPagesV2RefreshBoth')
    .addSeparator()
    .addItem('📍 Open Portfolio_Decision_v2', 'tfbPagesV2OpenPortfolioDecision')
    .addItem('📍 Open Top_10_Investments_v2', 'tfbPagesV2OpenTop10')
    .addSeparator()
    .addItem('⚙️ Set up refresh functions', 'tfbPagesV2Setup')
    .addItem('🩺 Status', 'tfbPagesV2Status');
}

/* Called from onOpen(); never throws, so the main menu is never affected. */
function tfbPagesV2AddMenu_() {
  try {
    tfbPagesV2Menu_(SpreadsheetApp.getUi()).addToUi();
    return true;
  } catch (menuErr) {
    try { Logger.log('[PAGES-V2 MENU] not added: ' + menuErr); } catch (logErr) {}
    return false;
  }
}

/* ---------------------------------------------------------------- public handlers */
function tfbPagesV2RefreshPortfolioDecision() { return tfbPagesV2RefreshAndReport_(['pd']); }
function tfbPagesV2RefreshTop10() { return tfbPagesV2RefreshAndReport_(['t10']); }
function tfbPagesV2RefreshBoth() { return tfbPagesV2RefreshAndReport_(['pd', 't10']); }
function tfbPagesV2OpenPortfolioDecision() { return tfbPagesV2Open_('pd'); }
function tfbPagesV2OpenTop10() { return tfbPagesV2Open_('t10'); }

function tfbPagesV2Setup() {
  var ui = tfbPagesV2Ui_();
  if (!ui) return null;
  var out = {};
  var keys = ['pd', 't10'];
  for (var i = 0; i < keys.length; i++) out[keys[i]] = tfbPagesV2SetupPage_(TFB_PAGES_V2_[keys[i]], ui, true);
  tfbPagesV2Toast_('Refresh functions: Portfolio_Decision_v2 = ' + (out.pd || '(not set)') +
    ' · Top_10_Investments_v2 = ' + (out.t10 || '(not set)'), 8);
  return out;
}

function tfbPagesV2Status() {
  var lines = ['Decision Pages v2 menu block v' + TFB_PAGES_V2_MENU_VERSION, ''];
  var keys = ['pd', 't10'];
  var report = {};
  for (var i = 0; i < keys.length; i++) {
    var page = TFB_PAGES_V2_[keys[i]];
    var name = tfbPagesV2Prop_(page.prop);
    var sh = tfbPagesV2Sheet_(page.sheet);
    var cands = tfbPagesV2Candidates_(page);
    var resolved = !!(name && tfbPagesV2Resolve_(name));
    report[page.key] = { configured: name, resolves: resolved, tab: !!sh, candidates: cands.slice(0, 5),
      running: tfbPagesV2InFlight_(page.key) };
    lines.push(page.sheet + ':');
    lines.push('  tab: ' + (sh ? 'present (' + sh.getLastRow() + ' rows)' : 'NOT FOUND'));
    lines.push('  refresh function: ' + (name ? name + (resolved ? ' (found)' : ' (NOT FOUND)') : '(not set)') +
      '  [Script Property ' + page.prop + ']');
    lines.push('  suggestions: ' + (cands.length ? cands.slice(0, 5).join(', ') : 'none found'));
    if (report[page.key].running) lines.push('  a refresh is running now');
    lines.push('');
  }
  lines.push('Manual-priority wrapper: ' +
    (typeof menuRunWithManualRefreshPriority_ === 'function' ? 'used' : 'not in this project (direct run)'));
  lines.push('Decision-owned in the menu: ' + (tfbPagesV2DecisionOwned_() ? 'yes' : 'no (needs the v2.19.0 block above this one)'));
  var ui = tfbPagesV2Ui_();
  if (ui) ui.alert(TFB_PAGES_V2_MENU_TITLE_, lines.join('\n'), ui.ButtonSet.OK);
  return report;
}

/* ---------------------------------------------------------------- refresh */
function tfbPagesV2RefreshAndReport_(keys) {
  var ui = tfbPagesV2Ui_();
  var targets = [];
  for (var i = 0; i < keys.length; i++) {
    var page = TFB_PAGES_V2_[keys[i]];
    var name = tfbPagesV2EnsureConfigured_(page, ui);
    if (!name) {
      tfbPagesV2Toast_('Nothing refreshed: no refresh function chosen for ' + page.sheet + '.', 6);
      return { status: 'not_configured', page: page.sheet };
    }
    targets.push({ page: page, name: name });
  }
  var results = [];
  var ran = {};
  for (var j = 0; j < targets.length; j++) {
    var t = targets[j];
    if (ran[t.name]) {
      results.push({ page: t.page.sheet, status: 'ok', fn: t.name, ms: 0, shared: ran[t.name] });
      continue;
    }
    var r = tfbPagesV2RunOne_(t.page, t.name);
    results.push(r);
    if (r.status === 'ok') ran[t.name] = t.page.sheet;
  }
  var failed = [];
  var parts = [];
  for (var k = 0; k < results.length; k++) {
    var x = results[k];
    if (x.status === 'ok') {
      parts.push(x.page + ': done' + (x.shared ? ' (same function as ' + x.shared + ')' : ' in ' + Math.round(x.ms / 1000) + ' s') +
        (x.summary ? ' - ' + x.summary : ''));
    } else if (x.status === 'busy') {
      parts.push(x.page + ': already refreshing, try again when it finishes');
      failed.push(x);
    } else if (x.status === 'deferred') {
      parts.push(x.page + ': not run now (the manual-refresh coordinator answered: ' + (x.summary || 'busy') + ')');
      failed.push(x);
    } else {
      parts.push(x.page + ': FAILED in ' + x.fn + '() - ' + x.error);
      failed.push(x);
    }
  }
  if (failed.length && ui) {
    ui.alert(TFB_PAGES_V2_MENU_TITLE_, parts.join('\n'), ui.ButtonSet.OK);
  } else {
    tfbPagesV2Toast_(parts.join(' · '), 8);
  }
  return { status: failed.length ? 'partial' : 'ok', results: results };
}

function tfbPagesV2EnsureConfigured_(page, ui) {
  var name = tfbPagesV2Prop_(page.prop);
  if (name && tfbPagesV2Resolve_(name)) return name;
  if (!ui) return null;
  if (name) {
    ui.alert(TFB_PAGES_V2_MENU_TITLE_, 'The refresh function saved for ' + page.sheet + ' (' + name +
      ') is not in the project any more. Choose it again.', ui.ButtonSet.OK);
  }
  return tfbPagesV2SetupPage_(page, ui, false);
}

function tfbPagesV2RunOne_(page, name) {
  var target = tfbPagesV2Resolve_(name);
  if (!target) return { page: page.sheet, status: 'error', fn: name, ms: 0, error: 'function not found' };
  if (!tfbPagesV2Claim_(page.key)) return { page: page.sheet, status: 'busy', fn: name, ms: 0 };
  var t0 = new Date().getTime();
  var called = false;
  try {
    /* Open the tab first: the user sees the page being refreshed, and a
     * renderer that writes to the active sheet writes to the right one. */
    var sh = tfbPagesV2Sheet_(page.sheet);
    if (sh) { try { SpreadsheetApp.getActiveSpreadsheet().setActiveSheet(sh); } catch (actErr) {} }
    tfbPagesV2Toast_('Refreshing ' + page.sheet + ' with ' + name + '() ...', 5);
    var call = function () { called = true; return target.fn.apply(target.self, []); };
    var result = (typeof menuRunWithManualRefreshPriority_ === 'function')
      ? menuRunWithManualRefreshPriority_('Refresh ' + page.sheet, call)
      : call();
    try { SpreadsheetApp.flush(); } catch (flushErr) {}
    return { page: page.sheet, status: called ? 'ok' : 'deferred', fn: name, ms: new Date().getTime() - t0,
      summary: tfbPagesV2Summary_(result) };
  } catch (err) {
    return { page: page.sheet, status: 'error', fn: name, ms: new Date().getTime() - t0,
      error: String((err && err.message) || err).slice(0, 300) };
  } finally {
    tfbPagesV2Release_(page.key);
  }
}

function tfbPagesV2Summary_(result) {
  if (result === undefined || result === null || result === true) return '';
  if (typeof result === 'string') return result.slice(0, 120);
  if (typeof result === 'object') {
    var s = result.status || result.message || result.state;
    if (s) return String(s).slice(0, 120);
  }
  return '';
}

/* ---------------------------------------------------------------- setup */
function tfbPagesV2SetupPage_(page, ui, fromSetupItem) {
  var current = tfbPagesV2Prop_(page.prop);
  var cands = tfbPagesV2Candidates_(page);
  var suggestion = (current && tfbPagesV2Resolve_(current)) ? current : (cands[0] || '');
  var listed = cands.slice(0, 8);
  var body = 'Which function refreshes the ' + page.sheet + ' tab?\n\n';
  if (listed.length) {
    body += 'Functions in this project that look like it:\n';
    for (var i = 0; i < listed.length; i++) body += '  ' + (i + 1) + '. ' + listed[i] + '\n';
  } else {
    body += 'No function name in this project looks like a ' + page.sheet + ' renderer.\n';
  }
  body += '\nType a function name or its number' +
    (suggestion ? ', or leave blank and press OK to use ' + suggestion : '') + '.';
  if (fromSetupItem && current) body += '\nCurrently saved: ' + current;
  var resp = ui.prompt(TFB_PAGES_V2_MENU_TITLE_ + ' - ' + page.sheet, body, ui.ButtonSet.OK_CANCEL);
  if (resp.getSelectedButton() !== ui.Button.OK) return null;
  var text = String(resp.getResponseText() || '').trim();
  if (/^\d+$/.test(text)) text = listed[parseInt(text, 10) - 1] || '';
  if (!text) text = suggestion;
  if (!text) {
    ui.alert(TFB_PAGES_V2_MENU_TITLE_, 'Nothing was chosen for ' + page.sheet + '. Open the tab\'s own script ' +
      'file, find the function that writes it, and type its name here.', ui.ButtonSet.OK);
    return null;
  }
  if (tfbPagesV2IsOwn_(text)) {
    ui.alert(TFB_PAGES_V2_MENU_TITLE_, text + ' is part of this menu block; choose the function that renders ' +
      page.sheet + '.', ui.ButtonSet.OK);
    return null;
  }
  if (!tfbPagesV2Resolve_(text)) {
    ui.alert(TFB_PAGES_V2_MENU_TITLE_, 'No function named "' + text + '" exists in this project. Nothing was saved.',
      ui.ButtonSet.OK);
    return null;
  }
  PropertiesService.getScriptProperties().setProperty(page.prop, text);
  return text;
}

/* ---------------------------------------------------------------- discovery */
function tfbPagesV2Root_() {
  if (typeof globalThis !== 'undefined') return globalThis;
  return (function () { return this; })();
}

function tfbPagesV2IsOwn_(name) {
  return String(name).replace(/^\s+/, '').toLowerCase().indexOf('tfbpagesv2') === 0;
}

/* "fn" or "Library.fn"; returns {fn, self} or null. */
function tfbPagesV2Resolve_(name) {
  var parts = String(name || '').trim().split('.');
  if (!parts[0]) return null;
  var self = null;
  var cur = tfbPagesV2Root_();
  try {
    for (var i = 0; i < parts.length; i++) {
      if (!/^[A-Za-z_$][\w$]*$/.test(parts[i])) return null;
      if (cur === null || cur === undefined) return null;
      self = cur;
      cur = cur[parts[i]];
    }
  } catch (resolveErr) {
    return null;
  }
  if (typeof cur !== 'function') return null;
  return { fn: cur, self: parts.length > 1 ? self : null };
}

function tfbPagesV2GlobalFunctionNames_() {
  var root = tfbPagesV2Root_();
  var seen = {};
  var out = [];
  var names = [];
  try { for (var k in root) names.push(k); } catch (enumErr) {}
  try { names = names.concat(Object.getOwnPropertyNames(root)); } catch (ownErr) {}
  for (var i = 0; i < names.length; i++) {
    var n = names[i];
    if (seen[n]) continue;
    seen[n] = true;
    try {
      if (typeof root[n] === 'function') out.push(n);
    } catch (readErr) {}
  }
  return out;
}

function tfbPagesV2Words_(name) {
  return String(name).replace(/([a-z0-9])([A-Z])/g, '$1 $2').replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
    .toLowerCase().split(/[^a-z0-9]+/).filter(function (w) { return !!w; });
}

function tfbPagesV2Score_(name, page) {
  if (/_$/.test(name) || tfbPagesV2IsOwn_(name)) return -1;
  var flat = String(name).toLowerCase().replace(/[^a-z0-9]/g, '');
  if (flat.indexOf('v2') < 0 && flat.indexOf('dp2') < 0) return -1;
  var words = tfbPagesV2Words_(name);
  for (var i = 0; i < words.length; i++) {
    if (TFB_PAGES_V2_EXCLUDE_.indexOf(words[i]) >= 0) return -1;
  }
  var other = page.key === 'pd' ? TFB_PAGES_V2_.t10 : TFB_PAGES_V2_.pd;
  var own = false;
  var foreign = false;
  for (var a = 0; a < page.hints.length; a++) if (flat.indexOf(page.hints[a]) >= 0) own = true;
  for (var b = 0; b < other.hints.length; b++) if (flat.indexOf(other.hints[b]) >= 0) foreign = true;
  var score = 1;
  if (own) score += 3;
  if (foreign && !own) score -= 3;
  for (var s = 0; s < words.length; s++) {
    if (TFB_PAGES_V2_STRONG_VERBS_.indexOf(words[s]) >= 0) { score += 2; break; }
    if (TFB_PAGES_V2_WEAK_VERBS_.indexOf(words[s]) >= 0) { score += 1; break; }
  }
  return score;
}

/* Ranked suggestions for one page (best first, name order on ties). */
function tfbPagesV2Candidates_(page) {
  var names = tfbPagesV2GlobalFunctionNames_();
  var scored = [];
  for (var i = 0; i < names.length; i++) {
    var sc = tfbPagesV2Score_(names[i], page);
    if (sc >= 2) scored.push({ name: names[i], score: sc });
  }
  scored.sort(function (x, y) { return (y.score - x.score) || (x.name < y.name ? -1 : x.name > y.name ? 1 : 0); });
  var out = [];
  for (var j = 0; j < scored.length; j++) out.push(scored[j].name);
  return out;
}

/* ---------------------------------------------------------------- in-flight guard */
function tfbPagesV2FlagKey_(key) { return 'TFB_PAGES_V2_INFLIGHT_' + key; }

function tfbPagesV2Cache_() {
  try { return CacheService.getDocumentCache() || CacheService.getScriptCache(); } catch (cacheErr) { return null; }
}

function tfbPagesV2InFlight_(key) {
  var cache = tfbPagesV2Cache_();
  try { return !!(cache && cache.get(tfbPagesV2FlagKey_(key))); } catch (getErr) { return false; }
}

function tfbPagesV2Claim_(key) {
  var cache = tfbPagesV2Cache_();
  if (!cache) return true;
  var lock = null;
  try {
    lock = LockService.getDocumentLock();
    /* A briefly held document lock (another short transaction) is not a
     * running refresh: claim without it rather than refuse the click. */
    if (lock && !lock.tryLock(3000)) lock = null;
  } catch (lockErr) {
    lock = null;
  }
  try {
    if (cache.get(tfbPagesV2FlagKey_(key))) return false;
    cache.put(tfbPagesV2FlagKey_(key), String(new Date().getTime()), TFB_PAGES_V2_INFLIGHT_TTL_S_);
    return true;
  } catch (claimErr) {
    return true;
  } finally {
    try { if (lock) lock.releaseLock(); } catch (relErr) {}
  }
}

function tfbPagesV2Release_(key) {
  var cache = tfbPagesV2Cache_();
  try { if (cache) cache.remove(tfbPagesV2FlagKey_(key)); } catch (remErr) {}
}

/* ---------------------------------------------------------------- small helpers */
function tfbPagesV2Ui_() {
  try { return SpreadsheetApp.getUi(); } catch (uiErr) { return null; }
}

function tfbPagesV2Prop_(prop) {
  try { return String(PropertiesService.getScriptProperties().getProperty(prop) || '').trim(); } catch (propErr) { return ''; }
}

function tfbPagesV2Sheet_(name) {
  try { return SpreadsheetApp.getActiveSpreadsheet().getSheetByName(name); } catch (sheetErr) { return null; }
}

function tfbPagesV2Toast_(msg, seconds) {
  try { SpreadsheetApp.getActiveSpreadsheet().toast(String(msg).slice(0, 400), TFB_PAGES_V2_MENU_TITLE_, seconds || 5); } catch (toastErr) {}
}

function tfbPagesV2DecisionOwned_() {
  try {
    if (typeof menuIsDecisionOwnedPage_ === 'function') {
      return menuIsDecisionOwnedPage_(TFB_PAGES_V2_.pd.sheet) === true &&
        menuIsDecisionOwnedPage_(TFB_PAGES_V2_.t10.sheet) === true;
    }
  } catch (ownErr) {}
  return false;
}

function tfbPagesV2Open_(key) {
  var page = TFB_PAGES_V2_[key];
  var sh = tfbPagesV2Sheet_(page.sheet);
  if (!sh) {
    var ui = tfbPagesV2Ui_();
    if (ui) ui.alert(TFB_PAGES_V2_MENU_TITLE_, 'The tab ' + page.sheet + ' does not exist in this spreadsheet.', ui.ButtonSet.OK);
    return false;
  }
  SpreadsheetApp.getActiveSpreadsheet().setActiveSheet(sh);
  return true;
}
