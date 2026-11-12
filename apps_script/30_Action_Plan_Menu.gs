/**
 * 30_Action_Plan_Menu.gs
 * Emad Bahbah Financial Advisor - the Today Action Plan menu
 * Version: 1.0.0 (2026-11-12)
 *   1.0.0: A MENU FOR THE TODAY ACTION PLAN (owner, Thu 12 Nov 2026 08:59 Riyadh: the daily action plan should be in
 *          the menu bar, to build and refresh it). The page is built by the GitHub workflow today_action_plan.yml
 *          (scripts/today_action_plan.py), so a menu item starts that workflow with the token the sync start already
 *          uses (Script Property TFB_GH_DISPATCH_TOKEN; repository TFB_GH_DISPATCH_REPO, ref TFB_GH_DISPATCH_REF).
 *          ADDITIVE: this file adds its OWN menu next to the existing one and touches nothing in 01_Menu.gs. Run
 *          apmInstallMenu once from the editor: it installs one "on open" trigger (apmOnOpen) and shows the menu now.
 *          Items: Build / refresh now (the page only) | Refresh the plan pages first (Daily Plan and Exit Plan; the
 *          Today Action Plan follows them by itself) | Open the page | Status.
 *          The token never reaches a toast or a log line. Rollback: apmRemoveMenu.
 */
var APM_VERSION = '1.0.0';   // lockstep with the header

var APM_ = {
  PROP_TOKEN: 'TFB_GH_DISPATCH_TOKEN',
  PROP_REPO: 'TFB_GH_DISPATCH_REPO',
  PROP_REF: 'TFB_GH_DISPATCH_REF',
  DEFAULT_REPO: 'emadsaberbahbah-coder/tadawul-fast',
  DEFAULT_REF: 'main',
  HANDLER: 'apmOnOpen',
  PAGE: 'Today_Action_Plan',
  MENU: '\u2705 Today Action Plan',
  WF_PAGE: 'today_action_plan.yml',
  WF_DAILY: 'daily_plan.yml',
  WF_EXIT: 'exit_plan.yml',
  API_BASE: 'https://api.github.com',
  API_VERSION: '2022-11-28'
};

/* ------------------------------------------------------------ pure */
function apmTrim_(v) { return String(v === null || v === undefined ? '' : v).replace(/^\s+|\s+$/g, ''); }
function apmRedact_(s, token) {
  var t = String(s === null || s === undefined ? '' : s);
  return token ? t.split(token).join('[token]') : t;
}
/* PURE: the GitHub workflow_dispatch request. */
function apmRequestCore_(repo, workflow, ref, inputs, token) {
  return {
    url: APM_.API_BASE + '/repos/' + repo + '/actions/workflows/' + encodeURIComponent(workflow) + '/dispatches',
    options: {
      method: 'post',
      contentType: 'application/json',
      payload: JSON.stringify({ ref: ref, inputs: inputs }),
      muteHttpExceptions: true,
      headers: { Authorization: 'Bearer ' + token, Accept: 'application/vnd.github+json', 'X-GitHub-Api-Version': APM_.API_VERSION }
    }
  };
}

/* ------------------------------------------------------------ helpers */
function apmNotify_(msg) {
  try { Logger.log(msg); } catch (eL) {}
  try { SpreadsheetApp.getActive().toast(String(msg).slice(0, 250), 'Today Action Plan ' + APM_VERSION, 12); } catch (eT) {}
  return msg;
}
/* Starts one workflow. Returns '' when GitHub accepted it (204), else the reason (token taken out). */
function apmDispatch_(workflow, inputs) {
  var p = PropertiesService.getScriptProperties(), token = apmTrim_(p.getProperty(APM_.PROP_TOKEN));
  if (!token) return 'no GitHub token (Script Property ' + APM_.PROP_TOKEN + ')';
  var repo = apmTrim_(p.getProperty(APM_.PROP_REPO)) || APM_.DEFAULT_REPO, ref = apmTrim_(p.getProperty(APM_.PROP_REF)) || APM_.DEFAULT_REF;
  var rq = apmRequestCore_(repo, workflow, ref, inputs, token), code = 0, body = '';
  try { var resp = UrlFetchApp.fetch(rq.url, rq.options); code = resp.getResponseCode(); body = resp.getContentText(); }
  catch (eF) { body = String(eF && eF.message || eF); }
  return code === 204 ? '' : (workflow + ' was not accepted (' + (code || 'no answer') + ': ' + apmRedact_(body, token).slice(0, 120) + ')');
}

/* ------------------------------------------------------------ menu items */
function apmRefreshNow() {
  var why = apmDispatch_(APM_.WF_PAGE, { dry_run: 'false' });
  return apmNotify_(why ? 'Not started: ' + why : 'Today Action Plan: build started. The page updates in about one minute.');
}
function apmRefreshAll() {
  var a = apmDispatch_(APM_.WF_DAILY, { dry_run: 'false' });
  var b = apmDispatch_(APM_.WF_EXIT, { dry_run: 'false', send_mail: 'false' });
  if (a || b) return apmNotify_('Not all started: ' + [a, b].filter(function (x) { return x; }).join('; '));
  return apmNotify_('Daily Plan and Exit Plan started. The Today Action Plan rebuilds by itself when they finish (about 2 minutes).');
}
function apmOpenPage() {
  var ss = SpreadsheetApp.getActive(), sh = ss.getSheetByName(APM_.PAGE);
  if (!sh) return apmNotify_('The ' + APM_.PAGE + ' page does not exist yet: choose "Build / refresh now" first.');
  ss.setActiveSheet(sh);
  return APM_.PAGE;
}
function apmStatus() {
  var n = 0, token = '', built = 'the page does not exist yet';
  try { ScriptApp.getProjectTriggers().forEach(function (tr) { if (tr.getHandlerFunction() === APM_.HANDLER) n++; }); } catch (eT) {}
  try { token = apmTrim_(PropertiesService.getScriptProperties().getProperty(APM_.PROP_TOKEN)); } catch (eP) {}
  try {
    var sh = SpreadsheetApp.getActive().getSheetByName(APM_.PAGE);
    if (sh) built = apmTrim_(sh.getRange(2, 1).getDisplayValue()).slice(0, 60) || 'the page is empty';
  } catch (eS) {}
  return apmNotify_('Today Action Plan menu ' + APM_VERSION + ': ' + n + ' menu trigger(s); GitHub token ' + (token ? 'present' : 'MISSING') + '; page: ' + built);
}

/* ------------------------------------------------------------ the menu */
function apmOnOpen() {
  SpreadsheetApp.getUi().createMenu(APM_.MENU)
    .addItem('Build / refresh now', 'apmRefreshNow')
    .addItem('Refresh the plan pages first (Daily Plan + Exit Plan)', 'apmRefreshAll')
    .addSeparator()
    .addItem('Open the page', 'apmOpenPage')
    .addItem('Status', 'apmStatus')
    .addToUi();
}
function apmRemoveTrigger_() {
  var trs = ScriptApp.getProjectTriggers(), n = 0;
  for (var i = 0; i < trs.length; i++) if (trs[i].getHandlerFunction() === APM_.HANDLER) { ScriptApp.deleteTrigger(trs[i]); n++; }
  return n;
}
function apmInstallMenu() {
  var old = apmRemoveTrigger_();
  ScriptApp.newTrigger(APM_.HANDLER).forSpreadsheet(SpreadsheetApp.getActive()).onOpen().create();
  var shown = true;
  try { apmOnOpen(); } catch (eU) { shown = false; }
  var token = apmTrim_(PropertiesService.getScriptProperties().getProperty(APM_.PROP_TOKEN));
  return apmNotify_('Today Action Plan menu installed' + (old ? ' (replaced ' + old + ')' : '') + '. ' +
    (shown ? 'It is in the menu bar now' : 'It appears when the workbook is opened again') + '. GitHub token: ' +
    (token ? 'present' : 'MISSING - set ' + APM_.PROP_TOKEN) + '.');
}
function apmRemoveMenu() {
  var n = apmRemoveTrigger_();
  return apmNotify_(n ? 'Removed ' + n + ' menu trigger(s); the menu is gone the next time the workbook opens.' : 'No Today Action Plan menu trigger was installed.');
}
