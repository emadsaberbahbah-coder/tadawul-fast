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
 *   - Manual only: no trigger, no onOpen of its own, no automatic writer, no
 *     service call and no change to any other global when the file loads.
 *   - Each refresh runs through menuRunWithManualRefreshPriority_ when the
 *     live menu defines it (automatic refresh yields), otherwise directly.
 *   - One run per renderer function at a time: a second click on a running
 *     function is refused (document-cache flag keyed by the function, claimed
 *     under the document lock when it is free within 3 s, cleared in finally,
 *     expires after 7 minutes if an execution dies).
 *   - Suggestions skip private helpers (trailing _), the v1 refreshers, and any
 *     name with test / install / remove / delete / clear / reset / purge /
 *     trigger / status / diag / debug / menu / setup / probe / audit / log /
 *     seed / layout in it. A typed name may be any function except this
 *     block's own (checked on the resolved function, so a dotted path to one
 *     of them is refused too).
 *   - "Refresh both" stops before Top 10 when Portfolio has used 4 of the
 *     6 execution minutes, and says so; refresh Top 10 on its own then.
 *   - The block does not change how the main menu's "Active Page" items treat
 *     the _v2 tabs; use this menu for them.
 *
 * ROLLBACK: delete the one onOpen line (the menu disappears; the block is inert
 * without it), optionally delete the block and the two Script Properties.
 * Conventions: ES5 (var, function).
 * ========================================================================== */

var TFB_PAGES_V2_MENU_VERSION = '1.0.0';
var TFB_PAGES_V2_MENU_TITLE_ = '🆕 Decision Pages v2';
var TFB_PAGES_V2_INFLIGHT_TTL_S_ = 420;
var TFB_PAGES_V2_BOTH_BUDGET_MS_ = 240000;
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
  for (var i = 0; i < keys.length; i++) {
    var page = TFB_PAGES_V2_[keys[i]];
    tfbPagesV2SetupPage_(page, ui, true);
    out[keys[i]] = tfbPagesV2Prop_(page.prop) || null;   // what is saved now, whatever was answered
  }
  tfbPagesV2Toast_('Saved refresh functions: Portfolio_Decision_v2 = ' + (out.pd || '(none)') +
    ' · Top_10_Investments_v2 = ' + (out.t10 || '(none)'), 8);
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
    var running = !!(name && tfbPagesV2InFlight_(name));
    report[page.key] = { configured: name, resolves: resolved, tab: !!sh, candidates: cands.slice(0, 5),
      running: running };
    lines.push(page.sheet + ':');
    lines.push('  tab: ' + (sh ? 'present (' + sh.getLastRow() + ' rows)' : 'NOT FOUND'));
    lines.push('  refresh function: ' + (name ? name + (resolved ? ' (found)' : ' (NOT FOUND)') : '(not set)') +
      '  [Script Property ' + page.prop + ']');
    lines.push('  suggestions: ' + (cands.length ? cands.slice(0, 5).join(', ') : 'none found'));
    if (running) lines.push('  its refresh function is running now');
    lines.push('');
  }
  lines.push('Manual-priority wrapper: ' +
    (typeof menuRunWithManualRefreshPriority_ === 'function' ? 'used' : 'not in this project (direct run)'));
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
  var t0 = new Date().getTime();
  var results = [];
  var attempted = {};
  for (var j = 0; j < targets.length; j++) {
    var t = targets[j];
    var prior = attempted[t.name];
    if (prior) {
      /* One function renders both tabs: never run it a second time in this click. */
      results.push({ page: t.page.sheet, status: prior.status, fn: t.name, ms: 0, shared: prior.page,
        error: prior.error, summary: prior.summary });
      continue;
    }
    if (j > 0 && new Date().getTime() - t0 > TFB_PAGES_V2_BOTH_BUDGET_MS_) {
      results.push({ page: t.page.sheet, status: 'skipped_time', fn: t.name, ms: 0 });
      continue;
    }
    var r = tfbPagesV2RunOne_(t.page, t.name);
    results.push(r);
    attempted[t.name] = r;
  }
  var failed = [];
  var parts = [];
  for (var k = 0; k < results.length; k++) {
    var x = results[k];
    var sharedNote = x.shared ? ' (same function as ' + x.shared + ', run once)' : '';
    if (x.status === 'ok') {
      parts.push(x.page + ': done' + (x.shared ? sharedNote : ' in ' + Math.round(x.ms / 1000) + ' s') +
        (x.summary ? ' - ' + x.summary : ''));
      continue;
    }
    failed.push(x);
    if (x.status === 'busy') {
      parts.push(x.page + ': ' + x.fn + '() is already running, try again when it finishes' + sharedNote);
    } else if (x.status === 'deferred') {
      parts.push(x.page + ': not run now - the manual-refresh coordinator did not run ' + x.fn + '()' +
        (x.summary ? ' (it answered: ' + x.summary + ')' : '') + sharedNote);
    } else if (x.status === 'skipped_time') {
      parts.push(x.page + ': not started, the first refresh used most of the 6-minute limit; refresh it on its own');
    } else {
      parts.push(x.page + ': FAILED - ' + x.error + sharedNote);
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
  if (!target) return { page: page.sheet, status: 'error', fn: name, ms: 0, error: name + '() not found' };
  if (!tfbPagesV2Claim_(name)) return { page: page.sheet, status: 'busy', fn: name, ms: 0 };
  var t0 = new Date().getTime();
  var called = false;
  var wrapped = typeof menuRunWithManualRefreshPriority_ === 'function';
  try {
    /* Open the tab first: the user sees the page being refreshed, and a
     * renderer that writes to the active sheet writes to the right one. */
    var sh = tfbPagesV2Sheet_(page.sheet);
    if (sh) { try { SpreadsheetApp.getActiveSpreadsheet().setActiveSheet(sh); } catch (actErr) {} }
    tfbPagesV2Toast_('Refreshing ' + page.sheet + ' with ' + name + '() ...', 5);
    var call = function () { called = true; return target.fn.apply(target.self, []); };
    var result = wrapped ? menuRunWithManualRefreshPriority_('Refresh ' + page.sheet, call) : call();
    try { SpreadsheetApp.flush(); } catch (flushErr) {}
    return { page: page.sheet, status: called ? 'ok' : 'deferred', fn: name, ms: new Date().getTime() - t0,
      summary: tfbPagesV2Summary_(result) };
  } catch (err) {
    var msg = String((err && err.message) || err).slice(0, 300);
    return { page: page.sheet, status: 'error', fn: name, ms: new Date().getTime() - t0,
      error: called ? name + '(): ' + msg
        : 'the manual-refresh coordinator failed before running ' + name + '(): ' + msg };
  } finally {
    tfbPagesV2Release_(name);
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
/* Returns the name now saved for the page, or null when nothing was saved by
 * this call. A blank answer takes the shown default; '-' removes the saved one. */
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
  if (fromSetupItem && current) body += '\nCurrently saved: ' + current + ' (type - to remove it).';
  var resp = ui.prompt(TFB_PAGES_V2_MENU_TITLE_ + ' - ' + page.sheet, body, ui.ButtonSet.OK_CANCEL);
  if (resp.getSelectedButton() !== ui.Button.OK) return null;
  var text = String(resp.getResponseText() || '').trim();
  if (text === '-') {
    PropertiesService.getScriptProperties().deleteProperty(page.prop);
    return null;
  }
  if (/^\d+$/.test(text)) {
    var n = parseInt(text, 10);
    if (n < 1 || n > listed.length) {
      ui.alert(TFB_PAGES_V2_MENU_TITLE_, 'There is no suggestion number ' + text + ' for ' + page.sheet +
        '. Nothing was saved.', ui.ButtonSet.OK);
      return null;
    }
    text = listed[n - 1];
  }
  if (!text) text = suggestion;
  if (!text) {
    ui.alert(TFB_PAGES_V2_MENU_TITLE_, 'Nothing was chosen for ' + page.sheet + '. Open the tab\'s own script ' +
      'file, find the function that writes it, and type its name here.', ui.ButtonSet.OK);
    return null;
  }
  var target = tfbPagesV2Resolve_(text);
  if (tfbPagesV2IsOwn_(text) || (target && tfbPagesV2IsOwnFn_(target.fn))) {
    ui.alert(TFB_PAGES_V2_MENU_TITLE_, text + ' is part of this menu block; choose the function that renders ' +
      page.sheet + '.', ui.ButtonSet.OK);
    return null;
  }
  if (!target) {
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

/* True for every function this block defines, however it was reached. */
function tfbPagesV2IsOwnFn_(fn) {
  try {
    var n = fn && fn.name;
    if (n && tfbPagesV2IsOwn_(n)) {
      var root = tfbPagesV2Root_();
      return root[n] === fn;
    }
  } catch (ownErr) {}
  return false;
}

/* A top-level const/let/class lives in the global lexical scope, not on the
 * global object; a Function body evaluates in global scope and can see it. */
function tfbPagesV2GlobalLexical_(ident) {
  try {
    return new Function('try { return typeof ' + ident + ' === "undefined" ? undefined : ' + ident +
      '; } catch (e) { return undefined; }')();
  } catch (lexErr) {
    return undefined;
  }
}

/* "fn" or "Library.fn"; returns {fn, self} or null. */
function tfbPagesV2Resolve_(name) {
  var parts = String(name || '').trim().split('.');
  for (var p = 0; p < parts.length; p++) {
    if (!/^[A-Za-z_$][\w$]*$/.test(parts[p])) return null;
  }
  var self = null;
  var cur;
  try {
    var root = tfbPagesV2Root_();
    cur = root[parts[0]];
    if (cur === undefined) cur = tfbPagesV2GlobalLexical_(parts[0]);
    for (var i = 1; i < parts.length; i++) {
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
function tfbPagesV2FlagKey_(fnName) { return 'TFB_PAGES_V2_INFLIGHT_' + String(fnName).trim(); }

function tfbPagesV2Cache_() {
  try { return CacheService.getDocumentCache() || CacheService.getScriptCache(); } catch (cacheErr) { return null; }
}

function tfbPagesV2InFlight_(fnName) {
  var cache = tfbPagesV2Cache_();
  try { return !!(cache && cache.get(tfbPagesV2FlagKey_(fnName))); } catch (getErr) { return false; }
}

function tfbPagesV2Claim_(fnName) {
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
    if (cache.get(tfbPagesV2FlagKey_(fnName))) return false;
    cache.put(tfbPagesV2FlagKey_(fnName), String(new Date().getTime()), TFB_PAGES_V2_INFLIGHT_TTL_S_);
    return true;
  } catch (claimErr) {
    return true;
  } finally {
    try { if (lock) lock.releaseLock(); } catch (relErr) {}
  }
}

function tfbPagesV2Release_(fnName) {
  var cache = tfbPagesV2Cache_();
  try { if (cache) cache.remove(tfbPagesV2FlagKey_(fnName)); } catch (remErr) {}
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
