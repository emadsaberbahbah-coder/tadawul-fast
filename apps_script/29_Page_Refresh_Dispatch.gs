/**
 * 29_Page_Refresh_Dispatch.gs
 * Emad Bahbah Financial Advisor - hourly start of the GitHub outcome pages
 * Version: 1.0.0 (2026-11-11)
 *   1.0.0: THE OUTCOME PAGES START FROM HERE EVERY HOUR (owner, Wed 11 Nov 2026 12:52 Riyadh: the outcome pages refresh
 *          at least every hour, automatically). GitHub's schedule did not start the Daily Plan, the Exit Plan or the
 *          Consistency Check once between 14:00 and 18:00 Riyadh on 11 Nov (0 of about 12 hourly runs); this
 *          repository's crons ran 3-7 hours late in September, which is why 24_Sync_Dispatch starts the daily sync.
 *          prdTick (ONE time trigger every 10 minutes, installed by prdInstallTrigger) starts each lane once per Riyadh
 *          hour, from its minute on:
 *            plan  daily_plan.yml         :20  07-23           inputs dry_run false
 *            mail  exit_plan.yml          :30  07              inputs dry_run false, send_mail true (the morning e-mail;
 *                                                               the only automatic sender: exit_plan.yml's schedule sends none)
 *            exit  exit_plan.yml          :35  08-23 and 00    inputs dry_run false, send_mail false
 *            cons  consistency_check.yml  :50  07-23 and 00    inputs dry_run false
 *          It uses the GitHub token of 24_Sync_Dispatch (Script Property TFB_GH_DISPATCH_TOKEN; repository
 *          TFB_GH_DISPATCH_REPO, ref TFB_GH_DISPATCH_REF). A lane GitHub does not accept (any answer but 204) is tried
 *          again at the next tick, at most 3 times in its hour. The token never reaches a log line.
 *          GATE: Script Property PRD_MODE = off (default: prdTick reads that one property and returns) | observe (logs
 *          the lanes it would start, starts nothing) | on. prdInstallTrigger sets PRD_MODE to on when it is not set.
 *          PRD_SKIP = lane ids to leave out (comma list). State: PRD_STATE (JSON: hour, done, tries). prdStatus
 *          reports; prdRemoveTrigger removes the trigger. Rollback: PRD_MODE = off, or prdRemoveTrigger.
 */
var PRD_VERSION = '1.0.0';   // lockstep with the header

var PRD_ = {
  PROP_MODE: 'PRD_MODE',
  PROP_SKIP: 'PRD_SKIP',
  PROP_STATE: 'PRD_STATE',
  PROP_TOKEN: 'TFB_GH_DISPATCH_TOKEN',
  PROP_REPO: 'TFB_GH_DISPATCH_REPO',
  PROP_REF: 'TFB_GH_DISPATCH_REF',
  DEFAULT_REPO: 'emadsaberbahbah-coder/tadawul-fast',
  DEFAULT_REF: 'main',
  HANDLER: 'prdTick',
  EVERY_MIN: 10,
  MAX_TRIES: 3,
  TZ: 'Asia/Riyadh',
  API_BASE: 'https://api.github.com',
  API_VERSION: '2022-11-28'
};
var PRD_DAY_ = [7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23];
var PRD_LANES_ = [
  { id: 'plan', workflow: 'daily_plan.yml', minute: 20, hours: PRD_DAY_, inputs: { dry_run: 'false' } },
  { id: 'mail', workflow: 'exit_plan.yml', minute: 30, hours: [7], inputs: { dry_run: 'false', send_mail: 'true' } },
  { id: 'exit', workflow: 'exit_plan.yml', minute: 35, hours: PRD_DAY_.slice(1).concat([0]), inputs: { dry_run: 'false', send_mail: 'false' } },
  { id: 'cons', workflow: 'consistency_check.yml', minute: 50, hours: PRD_DAY_.concat([0]), inputs: { dry_run: 'false' } }
];

/* ------------------------------------------------------------ pure */
function prdTrim_(v) { return String(v === null || v === undefined ? '' : v).replace(/^\s+|\s+$/g, ''); }
/* PURE: 'off' (default, anything unknown) | 'observe' | 'on'. */
function prdModeCore_(raw) {
  var v = prdTrim_(raw).toLowerCase();
  if (v === 'on' || v === '1' || v === 'true' || v === 'yes') return 'on';
  return v === 'observe' ? 'observe' : 'off';
}
/* PURE: the lane ids to leave out. */
function prdSkipCore_(raw) {
  var out = [], parts = prdTrim_(raw).toLowerCase().split(/[\s,;]+/);
  for (var i = 0; i < parts.length; i++) if (parts[i] && out.indexOf(parts[i]) < 0) out.push(parts[i]);
  return out;
}
/* PURE: the state of hour `key` ('yyyy-MM-dd HH', Riyadh); a state of another hour, or an unreadable one, starts empty. */
function prdStateCore_(raw, key) {
  var st = null;
  try { st = JSON.parse(String(raw || '')); } catch (e) { st = null; }
  if (!st || typeof st !== 'object' || st.hour !== key) st = { hour: key, done: {}, tries: {} };
  if (!st.done || typeof st.done !== 'object') st.done = {};
  if (!st.tries || typeof st.tries !== 'object') st.tries = {};
  return st;
}
/* PURE: the lanes to start now - in their hours, at or after their minute, not started this hour, tries left, not skipped. */
function prdDueCore_(lanes, hour, minute, st, skip) {
  var out = [];
  for (var i = 0; i < lanes.length; i++) {
    var l = lanes[i];
    if (l.hours.indexOf(hour) < 0 || minute < l.minute || st.done[l.id] || (+st.tries[l.id] || 0) >= PRD_.MAX_TRIES) continue;
    if (skip.indexOf(l.id) >= 0) continue;
    out.push(l);
  }
  return out;
}
/* PURE: the GitHub workflow_dispatch request. */
function prdRequestCore_(repo, workflow, ref, inputs, token) {
  return {
    url: PRD_.API_BASE + '/repos/' + repo + '/actions/workflows/' + encodeURIComponent(workflow) + '/dispatches',
    options: {
      method: 'post',
      contentType: 'application/json',
      payload: JSON.stringify({ ref: ref, inputs: inputs }),
      muteHttpExceptions: true,
      headers: { Authorization: 'Bearer ' + token, Accept: 'application/vnd.github+json', 'X-GitHub-Api-Version': PRD_.API_VERSION }
    }
  };
}
/* PURE: a text with the token taken out. */
function prdRedact_(s, token) {
  var t = String(s === null || s === undefined ? '' : s);
  return token ? t.split(token).join('[token]') : t;
}

/* ------------------------------------------------------------ the trigger */
function prdTick() {
  var p = null;
  try { p = PropertiesService.getScriptProperties(); } catch (eP) { return { mode: 'off', started: [], failed: [], would: [] }; }
  var mode = prdModeCore_(p.getProperty(PRD_.PROP_MODE));
  var out = { mode: mode, hour: '', started: [], failed: [], would: [] };
  if (mode === 'off') return out;
  var now = new Date(), key = Utilities.formatDate(now, PRD_.TZ, 'yyyy-MM-dd HH'), minute = +Utilities.formatDate(now, PRD_.TZ, 'mm');
  out.hour = key;
  var st = prdStateCore_(p.getProperty(PRD_.PROP_STATE), key);
  var due = prdDueCore_(PRD_LANES_, +key.slice(11, 13), minute, st, prdSkipCore_(p.getProperty(PRD_.PROP_SKIP)));
  if (!due.length) return out;
  var i;
  if (mode === 'observe') {
    for (i = 0; i < due.length; i++) out.would.push(due[i].id);
    try { Logger.log('page refresh ' + key + ': observe - would start ' + out.would.join(', ')); } catch (eL) {}
    return out;
  }
  var token = prdTrim_(p.getProperty(PRD_.PROP_TOKEN));
  if (!token) {
    out.error = 'no GitHub token (Script Property ' + PRD_.PROP_TOKEN + ')';
    try { Logger.log('page refresh ' + key + ': ' + out.error); } catch (eL2) {}
    return out;
  }
  var repo = prdTrim_(p.getProperty(PRD_.PROP_REPO)) || PRD_.DEFAULT_REPO, ref = prdTrim_(p.getProperty(PRD_.PROP_REF)) || PRD_.DEFAULT_REF;
  for (i = 0; i < due.length; i++) {
    var l = due[i], rq = prdRequestCore_(repo, l.workflow, ref, l.inputs, token), code = 0, body = '';
    st.tries[l.id] = (+st.tries[l.id] || 0) + 1;
    try { var resp = UrlFetchApp.fetch(rq.url, rq.options); code = resp.getResponseCode(); body = resp.getContentText(); }
    catch (eF) { body = String(eF && eF.message || eF); }
    if (code === 204) { st.done[l.id] = 1; out.started.push(l.id); }
    else out.failed.push(l.id + ' (' + (code || 'no answer') + ': ' + prdRedact_(body, token).slice(0, 120) + ')');
    try { p.setProperty(PRD_.PROP_STATE, JSON.stringify(st)); } catch (eS) {}
  }
  try { Logger.log('page refresh ' + key + ': started ' + (out.started.join(', ') || 'none') + (out.failed.length ? '; not accepted ' + out.failed.join('; ') : '')); } catch (eL3) {}
  return out;
}

/* ------------------------------------------------------------ owner functions (run from the editor) */
function prdRemoveTrigger_() {
  var trs = ScriptApp.getProjectTriggers(), n = 0;
  for (var i = 0; i < trs.length; i++) if (trs[i].getHandlerFunction() === PRD_.HANDLER) { ScriptApp.deleteTrigger(trs[i]); n++; }
  return n;
}
function prdNotify_(msg) {
  try { Logger.log(msg); } catch (eL) {}
  try { SpreadsheetApp.getActive().toast(String(msg).slice(0, 250), 'Page refresh ' + PRD_VERSION, 15); } catch (eT) {}
  return msg;
}
function prdInstallTrigger() {
  var old = prdRemoveTrigger_();
  ScriptApp.newTrigger(PRD_.HANDLER).timeBased().everyMinutes(PRD_.EVERY_MIN).create();
  var p = PropertiesService.getScriptProperties(), set = '';
  if (!prdTrim_(p.getProperty(PRD_.PROP_MODE))) { p.setProperty(PRD_.PROP_MODE, 'on'); set = ' (set to on)'; }
  var token = prdTrim_(p.getProperty(PRD_.PROP_TOKEN));
  return prdNotify_('Page refresh installed: ' + PRD_.HANDLER + ' every ' + PRD_.EVERY_MIN + ' min' + (old ? ' (replaced ' + old + ')' : '') +
    '. PRD_MODE = ' + prdModeCore_(p.getProperty(PRD_.PROP_MODE)) + set + '. GitHub token: ' + (token ? 'present' : 'MISSING - set ' + PRD_.PROP_TOKEN) +
    '. Lanes (Riyadh): Daily Plan :20 07-23; Exit Plan :30 at 07 with the e-mail, :35 08-00; Consistency Check :50 07-00.');
}
function prdRemoveTrigger() {
  var n = prdRemoveTrigger_();
  return prdNotify_(n ? 'Removed ' + n + ' page-refresh trigger(s); nothing starts the pages from here until it is installed again.' : 'No page-refresh trigger was installed.');
}
function prdStatus() {
  var props = {}, n = 0;
  try { props = PropertiesService.getScriptProperties().getProperties() || {}; } catch (eP) { props = {}; }
  try { ScriptApp.getProjectTriggers().forEach(function (tr) { if (tr.getHandlerFunction() === PRD_.HANDLER) n++; }); } catch (eT) {}
  var lanes = [];
  for (var i = 0; i < PRD_LANES_.length; i++) lanes.push(PRD_LANES_[i].id + ' ' + PRD_LANES_[i].workflow + ' :' + PRD_LANES_[i].minute);
  return prdNotify_('Page refresh ' + PRD_VERSION + ': ' + n + ' ' + PRD_.HANDLER + ' trigger(s); PRD_MODE = ' + prdModeCore_(props[PRD_.PROP_MODE]) +
    '; token ' + (prdTrim_(props[PRD_.PROP_TOKEN]) ? 'present' : 'MISSING') + '; skip [' + prdSkipCore_(props[PRD_.PROP_SKIP]).join(',') + ']; last hour ' +
    String(props[PRD_.PROP_STATE] || 'none') + '; lanes ' + lanes.join(' | '));
}
