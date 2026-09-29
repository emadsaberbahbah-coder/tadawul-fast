// TADAWUL FAST BRIDGE - DAILY SYNC DISPATCHER v1.0.0  [P-170 DISPATCH-FROM-GAS]
//
// Purpose
// -------
// Start the GitHub Actions workflow daily_sync.yml (the four-page dashboard
// sync) from a time-driven Apps Script trigger, so the morning sync starts ON
// THE MINUTE instead of whenever GitHub gets round to a schedule event.
//
// -----------------------------------------------------------------------------
// v1.0.0 (2026-09-29, One-Pass Script - P-170 SCHEDULE DRIFT)
// WHY (measured from _Run_Log / _Status / the GitHub jobs API):
//   * 2026-09-26: the 20Z schedule slot fired 22:59Z (3 h late); the 04Z slot
//     fired 09:00Z (5 h late). Moving the minute to :17 (P-170 first attempt,
//     cron "17 4,12,20") did not help:
//   * 2026-09-28: the three slots executed ~10:30Z, ~19:15Z and ~00:25Z (09-29)
//     - 6 h, 7 h and 3.7-4 h late (EODHD-QUOTA lines 14:04 / 22:46 / 03:27
//     Riyadh mark the leg ends).
//   * 2026-09-29: the 04:17Z slot had not started by 05:51Z; the morning
//     cockpit (08:10 Riyadh) ran on the 04:27 Riyadh epoch - "feed: EXECUTABLE
//     age 220m". Program v2's "cockpit only after the 07:17 sync" cannot be
//     met by a GitHub schedule trigger at all: GitHub documents that
//     schedule events are delayed under load and never guarantees a start.
//   A workflow_dispatch REST call starts a run within seconds, on a queue
//   GitHub does honour. Apps Script time-driven triggers fire inside a
//   15-minute window of the requested minute - deterministic enough to put
//   the sync ahead of the 08:10 cockpit every day.
// WHAT (new file, no existing function touched):
//   tfbDispatchDailySync()          trigger handler: guards -> POST dispatch
//   tfbDispatchDailySyncNow()       manual variant (bypasses gap/fresh guards)
//   tfbSyncDispatchProbe()          GET the workflow (proves token/permission,
//                                   dispatches NOTHING)
//   tfbInstallSyncDispatchTriggers() idempotent installer (deletes its own
//                                   triggers first, one per configured hour)
//   tfbRemoveSyncDispatchTriggers() remove them
//   tfbSyncDispatchStatus()         one-line status (never prints the token)
//   tfbSyncDispatchSelfTest()       pure-logic self-test, no network, no write
// GUARDS (all fail-open on sheet read errors - a missing read never blocks the
//   dispatch; a missing TOKEN always does):
//   1. kill switch  Script Property TFB_SYNC_DISPATCH_DISABLED = 1
//   2. no token     Script Property TFB_GH_DISPATCH_TOKEN absent -> FAILED, no call
//   3. in-flight    _Sync_Control "backend sync hold until" is in the future
//                   (a sync is writing right now) -> SKIPPED
//   4. min gap      last successful dispatch < TFB_SYNC_DISPATCH_MIN_GAP_MIN
//                   (default 120) minutes ago -> SKIPPED (double-trigger safety)
//   5. fresh page   _Status Global_Markets stamp younger than
//                   TFB_SYNC_DISPATCH_FRESH_SKIP_MIN (default 90) -> SKIPPED
//                   (a schedule run already delivered this epoch)
//   The manual "Now" variant bypasses 4 and 5 only.
// SECRETS: the token lives ONLY in Script Properties (TFB_GH_DISPATCH_TOKEN);
//   it is never written to _Run_Log, Logger, the status line or an error text
//   (tfbSdRedact_ scrubs every outgoing string). Recommended token: GitHub
//   fine-grained PAT, single repository, permissions Actions: Read and write +
//   Metadata: Read-only, 90-day expiry.
// EVIDENCE OF LIVE STATE: every call appends one _Run_Log row
//   (Action = the function name, Page = the workflow file) and stores
//   TFB_SYNC_DISPATCH_LAST_EVENT (JSON) for tfbSyncDispatchStatus().
// COMPANION (operator, GitHub lane): daily_sync.yml cron "17 4,12,20" ->
//   "17 12,20" so the late-firing 04:17Z slot does not double-run behind the
//   dispatched morning run (the production write lease queues, it does not
//   cancel). Under the 2-runs/day cost plan set TFB_SYNC_DISPATCH_HOURS=6,15
//   and drop the schedule block entirely.
// ES5 only (file is loaded by the classic runtime path); no let/const/arrow.
// -----------------------------------------------------------------------------
var TFB_SYNC_DISPATCH_VERSION = '1.0.0';

var TFB_SYNC_DISPATCH_ = Object.freeze({
  PROP_TOKEN: 'TFB_GH_DISPATCH_TOKEN',
  PROP_REPO: 'TFB_GH_DISPATCH_REPO',
  PROP_WORKFLOW: 'TFB_GH_DISPATCH_WORKFLOW',
  PROP_REF: 'TFB_GH_DISPATCH_REF',
  PROP_HOURS: 'TFB_SYNC_DISPATCH_HOURS',
  PROP_MINUTE: 'TFB_SYNC_DISPATCH_MINUTE',
  PROP_DISABLED: 'TFB_SYNC_DISPATCH_DISABLED',
  PROP_MIN_GAP_MIN: 'TFB_SYNC_DISPATCH_MIN_GAP_MIN',
  PROP_FRESH_SKIP_MIN: 'TFB_SYNC_DISPATCH_FRESH_SKIP_MIN',
  PROP_LAST_OK_MS: 'TFB_SYNC_DISPATCH_LAST_OK_MS',
  PROP_LAST_EVENT: 'TFB_SYNC_DISPATCH_LAST_EVENT',
  DEFAULT_REPO: 'emadsaberbahbah-coder/tadawul-fast',
  DEFAULT_WORKFLOW: 'daily_sync.yml',
  DEFAULT_REF: 'main',
  DEFAULT_HOURS: '6',
  DEFAULT_MINUTE: 15,
  DEFAULT_MIN_GAP_MIN: 120,
  DEFAULT_FRESH_SKIP_MIN: 90,
  HOLD_CEILING_MS: 16 * 60 * 1000,
  RUN_MODE: 'full_sync',
  HANDLER: 'tfbDispatchDailySync',
  TIMEZONE: 'Asia/Riyadh',
  API_BASE: 'https://api.github.com',
  API_VERSION: '2022-11-28',
  TAB_RUN_LOG: '_Run_Log',
  TAB_SYNC_CONTROL: '_Sync_Control',
  TAB_STATUS: '_Status',
  HOLD_KEY: 'backend sync hold until',
  STATUS_PAGE: 'Global_Markets',
  STATUS_FEED_KEY: 'TFB Feed Global_Markets',
  BODY_MAX: 200
});

// ---------------------------------------------------------------------------
// Pure helpers (self-tested; no GAS services touched)
// ---------------------------------------------------------------------------

function tfbSdNowMs_() {
  return Date.now();
}

function tfbSdTrim_(v) {
  return v === null || v === undefined ? '' : String(v).replace(/^\s+|\s+$/g, '');
}

// "6,15" -> [6, 15]; blanks/invalid entries dropped; empty -> [6].
function tfbSdParseHours_(raw) {
  var out = [];
  var seen = {};
  var parts = tfbSdTrim_(raw).split(/[,\s;]+/);
  for (var i = 0; i < parts.length; i++) {
    var p = tfbSdTrim_(parts[i]);
    if (p === '') { continue; }
    if (!/^\d{1,2}$/.test(p)) { continue; }
    var h = parseInt(p, 10);
    if (h < 0 || h > 23) { continue; }
    if (seen[h]) { continue; }
    seen[h] = true;
    out.push(h);
  }
  if (out.length === 0) { out.push(6); }
  out.sort(function (a, b) { return a - b; });
  return out;
}

function tfbSdParseIntProp_(raw, dflt, lo, hi) {
  var s = tfbSdTrim_(raw);
  if (!/^-?\d+$/.test(s)) { return dflt; }
  var n = parseInt(s, 10);
  if (n < lo || n > hi) { return dflt; }
  return n;
}

function tfbSdIsOn_(raw) {
  var s = tfbSdTrim_(raw).toLowerCase();
  return s === '1' || s === 'true' || s === 'yes' || s === 'on';
}

// ISO / sheet timestamp -> epoch ms, or 0 when unparseable.
// Accepts Date objects, "2026-09-29T01:28:32.942810+00:00" (6-digit fractions
// trimmed to 3), "2026-09-29 04:27:48+03:00" (space -> T), and the _Status
// feed-key form "OK | cov=100.0 | run=... | 2026-09-29 04:27:50+03:00".
function tfbSdParseIsoMs_(raw) {
  try {
    if (raw instanceof Date) {
      var t0 = raw.getTime();
      return isNaN(t0) ? 0 : t0;
    }
    var s = tfbSdTrim_(raw);
    if (s === '') { return 0; }
    var m = s.match(/(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(\.\d+)?(Z|[+-]\d{2}:?\d{2})?/);
    if (!m) { return 0; }
    var frac = m[3] ? m[3].slice(0, 4) : '';
    var zone = m[4] ? m[4] : '';
    if (/^[+-]\d{4}$/.test(zone)) { zone = zone.slice(0, 3) + ':' + zone.slice(3); }
    var iso = m[1] + 'T' + m[2] + frac + zone;
    var t = Date.parse(iso);
    return isNaN(t) ? 0 : t;
  } catch (err) {
    return 0;
  }
}

// Decision table. inp = {disabled, tokenPresent, force, nowMs, lastOkMs,
// minGapMs, holdUntilMs, holdCeilingMs, gmStampMs, freshSkipMs}
function tfbSdDecide_(inp) {
  var i = inp || {};
  if (i.disabled) { return { dispatch: false, reason: 'disabled' }; }
  if (!i.tokenPresent) { return { dispatch: false, reason: 'no_token' }; }
  var now = Number(i.nowMs) || 0;
  var hold = Number(i.holdUntilMs) || 0;
  var ceiling = Number(i.holdCeilingMs) || TFB_SYNC_DISPATCH_.HOLD_CEILING_MS;
  if (hold > now && hold - now <= ceiling) {
    return { dispatch: false, reason: 'sync_in_flight' };
  }
  if (!i.force) {
    var lastOk = Number(i.lastOkMs) || 0;
    var gap = Number(i.minGapMs) || 0;
    if (lastOk > 0 && gap > 0 && now - lastOk < gap) {
      return { dispatch: false, reason: 'min_gap' };
    }
    var stamp = Number(i.gmStampMs) || 0;
    var fresh = Number(i.freshSkipMs) || 0;
    if (stamp > 0 && fresh > 0 && now - stamp < fresh) {
      return { dispatch: false, reason: 'gm_fresh' };
    }
  }
  return { dispatch: true, reason: i.force ? 'forced' : 'ok' };
}

function tfbSdBuildDispatchRequest_(repo, workflow, ref, token) {
  var url = TFB_SYNC_DISPATCH_.API_BASE + '/repos/' + repo +
    '/actions/workflows/' + encodeURIComponent(workflow) + '/dispatches';
  var payload = { ref: ref, inputs: { run_mode: TFB_SYNC_DISPATCH_.RUN_MODE } };
  var options = {
    method: 'post',
    contentType: 'application/json',
    headers: {
      Authorization: 'Bearer ' + token,
      Accept: 'application/vnd.github+json',
      'X-GitHub-Api-Version': TFB_SYNC_DISPATCH_.API_VERSION
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };
  return { url: url, options: options };
}

function tfbSdBuildProbeRequest_(repo, workflow, token) {
  var url = TFB_SYNC_DISPATCH_.API_BASE + '/repos/' + repo +
    '/actions/workflows/' + encodeURIComponent(workflow);
  var options = {
    method: 'get',
    headers: {
      Authorization: 'Bearer ' + token,
      Accept: 'application/vnd.github+json',
      'X-GitHub-Api-Version': TFB_SYNC_DISPATCH_.API_VERSION
    },
    muteHttpExceptions: true
  };
  return { url: url, options: options };
}

// Scrub the token (and its bearer form) out of any outgoing string.
function tfbSdRedact_(s, token) {
  var out = s === null || s === undefined ? '' : String(s);
  var t = tfbSdTrim_(token);
  if (t.length >= 6) {
    while (out.indexOf(t) !== -1) { out = out.replace(t, '[redacted]'); }
  }
  return out;
}

function tfbSdClip_(s, n) {
  var out = s === null || s === undefined ? '' : String(s);
  var max = n || TFB_SYNC_DISPATCH_.BODY_MAX;
  return out.length > max ? out.slice(0, max) + '...' : out;
}

// ---------------------------------------------------------------------------
// GAS-facing helpers (each fails open / soft)
// ---------------------------------------------------------------------------

function tfbSdProps_() {
  return PropertiesService.getScriptProperties();
}

function tfbSdProp_(key, dflt) {
  try {
    var v = tfbSdProps_().getProperty(key);
    return v === null || v === undefined || tfbSdTrim_(v) === '' ? dflt : v;
  } catch (err) {
    return dflt;
  }
}

function tfbSdConfig_() {
  var C = TFB_SYNC_DISPATCH_;
  return {
    token: tfbSdTrim_(tfbSdProp_(C.PROP_TOKEN, '')),
    repo: tfbSdTrim_(tfbSdProp_(C.PROP_REPO, C.DEFAULT_REPO)),
    workflow: tfbSdTrim_(tfbSdProp_(C.PROP_WORKFLOW, C.DEFAULT_WORKFLOW)),
    ref: tfbSdTrim_(tfbSdProp_(C.PROP_REF, C.DEFAULT_REF)),
    hours: tfbSdParseHours_(tfbSdProp_(C.PROP_HOURS, C.DEFAULT_HOURS)),
    minute: tfbSdParseIntProp_(tfbSdProp_(C.PROP_MINUTE, String(C.DEFAULT_MINUTE)), C.DEFAULT_MINUTE, 0, 59),
    disabled: tfbSdIsOn_(tfbSdProp_(C.PROP_DISABLED, '')),
    minGapMs: tfbSdParseIntProp_(tfbSdProp_(C.PROP_MIN_GAP_MIN, String(C.DEFAULT_MIN_GAP_MIN)), C.DEFAULT_MIN_GAP_MIN, 0, 1440) * 60 * 1000,
    freshSkipMs: tfbSdParseIntProp_(tfbSdProp_(C.PROP_FRESH_SKIP_MIN, String(C.DEFAULT_FRESH_SKIP_MIN)), C.DEFAULT_FRESH_SKIP_MIN, 0, 1440) * 60 * 1000,
    lastOkMs: tfbSdParseIntProp_(tfbSdProp_(C.PROP_LAST_OK_MS, '0'), 0, 0, 9007199254740991)
  };
}

function tfbSdSheet_(name) {
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    return ss ? ss.getSheetByName(name) : null;
  } catch (err) {
    return null;
  }
}

// _Sync_Control: Key | Value  -> hold-until ms (0 when none / unreadable)
function tfbSdReadHoldUntilMs_() {
  try {
    var sh = tfbSdSheet_(TFB_SYNC_DISPATCH_.TAB_SYNC_CONTROL);
    if (!sh) { return 0; }
    var vals = sh.getDataRange().getValues();
    for (var r = 0; r < vals.length; r++) {
      if (tfbSdTrim_(vals[r][0]).toLowerCase() === TFB_SYNC_DISPATCH_.HOLD_KEY) {
        return tfbSdParseIsoMs_(vals[r][1]);
      }
    }
    return 0;
  } catch (err) {
    return 0;
  }
}

// _Status: Page row "Global_Markets" col B, and/or the Global Key
// "TFB Feed Global_Markets" value -> newest parsable stamp ms (0 when none)
function tfbSdReadGmStampMs_() {
  try {
    var sh = tfbSdSheet_(TFB_SYNC_DISPATCH_.TAB_STATUS);
    if (!sh) { return 0; }
    var vals = sh.getDataRange().getValues();
    var best = 0;
    for (var r = 0; r < vals.length; r++) {
      var row = vals[r];
      if (tfbSdTrim_(row[0]) === TFB_SYNC_DISPATCH_.STATUS_PAGE) {
        var a = tfbSdParseIsoMs_(row[1]);
        if (a > best) { best = a; }
      }
      for (var c = 0; c < row.length - 1; c++) {
        if (tfbSdTrim_(row[c]) === TFB_SYNC_DISPATCH_.STATUS_FEED_KEY) {
          var b = tfbSdParseIsoMs_(row[c + 1]);
          if (b > best) { best = b; }
        }
      }
    }
    return best;
  } catch (err) {
    return 0;
  }
}

// One _Run_Log row: Timestamp | Level | Action | Page | Status | Message |
// Endpoint | HTTP Code | Duration ms | Details JSON   (fail-open)
function tfbSdLog_(level, action, status, message, endpoint, httpCode, durationMs, details, token) {
  var C = TFB_SYNC_DISPATCH_;
  var msg = tfbSdRedact_(message, token);
  var det = details || {};
  det.version = TFB_SYNC_DISPATCH_VERSION;
  var detJson = tfbSdRedact_(JSON.stringify(det), token);
  try {
    var sh = tfbSdSheet_(C.TAB_RUN_LOG);
    if (sh) {
      sh.appendRow([new Date(), level, action, C.DEFAULT_WORKFLOW, status, msg,
        tfbSdRedact_(endpoint || '', token), httpCode === undefined || httpCode === null ? '' : httpCode,
        durationMs === undefined || durationMs === null ? '' : durationMs, detJson]);
    }
  } catch (err) {
    // fail-open: the dispatch outcome must not depend on the log write
  }
  try {
    Logger.log('[SYNC-DISPATCH v' + TFB_SYNC_DISPATCH_VERSION + '] ' + action + ' ' + status + ' | ' + msg);
  } catch (err2) { /* noop */ }
  try {
    tfbSdProps_().setProperty(C.PROP_LAST_EVENT, tfbSdRedact_(JSON.stringify({
      ts: new Date().toISOString(), action: action, status: status, code: httpCode === undefined ? '' : httpCode,
      message: tfbSdClip_(msg, 160)
    }), token));
  } catch (err3) { /* noop */ }
}

// ---------------------------------------------------------------------------
// Core: dispatch
// ---------------------------------------------------------------------------

function tfbSdDispatch_(force) {
  var C = TFB_SYNC_DISPATCH_;
  var t0 = tfbSdNowMs_();
  var cfg = tfbSdConfig_();
  var action = force ? 'tfbDispatchDailySyncNow' : C.HANDLER;
  var holdMs = tfbSdReadHoldUntilMs_();
  var gmMs = tfbSdReadGmStampMs_();
  var now = tfbSdNowMs_();
  var verdict = tfbSdDecide_({
    disabled: cfg.disabled, tokenPresent: cfg.token !== '', force: !!force, nowMs: now,
    lastOkMs: cfg.lastOkMs, minGapMs: cfg.minGapMs, holdUntilMs: holdMs,
    holdCeilingMs: C.HOLD_CEILING_MS, gmStampMs: gmMs, freshSkipMs: cfg.freshSkipMs
  });
  var ctx = {
    reason: verdict.reason, repo: cfg.repo, workflow: cfg.workflow, ref: cfg.ref,
    gm_stamp_age_min: gmMs > 0 ? Math.round((now - gmMs) / 60000) : null,
    hold_until_in_s: holdMs > 0 ? Math.round((holdMs - now) / 1000) : null,
    last_ok_age_min: cfg.lastOkMs > 0 ? Math.round((now - cfg.lastOkMs) / 60000) : null,
    force: !!force
  };
  if (!verdict.dispatch) {
    var status = verdict.reason === 'no_token' ? 'FAILED' : 'SKIPPED';
    var level = verdict.reason === 'no_token' ? 'ERROR' : 'INFO';
    tfbSdLog_(level, action, status, 'dispatch ' + status.toLowerCase() + ': ' + verdict.reason, '', '', tfbSdNowMs_() - t0, ctx, cfg.token);
    return status + ':' + verdict.reason;
  }
  var req = tfbSdBuildDispatchRequest_(cfg.repo, cfg.workflow, cfg.ref, cfg.token);
  var code = 0;
  var body = '';
  try {
    var resp = UrlFetchApp.fetch(req.url, req.options);
    code = resp.getResponseCode();
    body = tfbSdClip_(resp.getContentText() || '');
  } catch (err) {
    code = -1;
    body = tfbSdClip_(String(err && err.message ? err.message : err));
  }
  var dur = tfbSdNowMs_() - t0;
  if (code === 204) {
    try { tfbSdProps_().setProperty(C.PROP_LAST_OK_MS, String(tfbSdNowMs_())); } catch (e1) { /* noop */ }
    ctx.http = 204;
    tfbSdLog_('INFO', action, 'OK', 'workflow_dispatch accepted (HTTP 204) ref=' + cfg.ref + ' run_mode=' + C.RUN_MODE, req.url, 204, dur, ctx, cfg.token);
    return 'OK:204';
  }
  ctx.http = code;
  ctx.body = body;
  tfbSdLog_('ERROR', action, 'FAILED', 'workflow_dispatch rejected HTTP ' + code + ' ' + body, req.url, code, dur, ctx, cfg.token);
  return 'FAILED:' + code;
}

// Trigger handler (installed by tfbInstallSyncDispatchTriggers)
function tfbDispatchDailySync() {
  return tfbSdDispatch_(false);
}

// Manual: bypasses the min-gap and fresh-page guards (NOT the kill switch,
// the token check or a live write hold).
function tfbDispatchDailySyncNow() {
  return tfbSdDispatch_(true);
}

// Proves token + permission without dispatching: GET the workflow record.
function tfbSyncDispatchProbe() {
  var t0 = tfbSdNowMs_();
  var cfg = tfbSdConfig_();
  if (cfg.token === '') {
    tfbSdLog_('ERROR', 'tfbSyncDispatchProbe', 'FAILED', 'no token in Script Property ' + TFB_SYNC_DISPATCH_.PROP_TOKEN, '', '', 0, {}, '');
    return 'FAILED:no_token';
  }
  var req = tfbSdBuildProbeRequest_(cfg.repo, cfg.workflow, cfg.token);
  var code = 0;
  var body = '';
  try {
    var resp = UrlFetchApp.fetch(req.url, req.options);
    code = resp.getResponseCode();
    body = resp.getContentText() || '';
  } catch (err) {
    code = -1;
    body = String(err && err.message ? err.message : err);
  }
  var dur = tfbSdNowMs_() - t0;
  if (code === 200) {
    var state = '';
    var wfId = '';
    var path = '';
    try {
      var j = JSON.parse(body);
      state = j.state || '';
      wfId = j.id || '';
      path = j.path || '';
    } catch (e1) { /* noop */ }
    tfbSdLog_('INFO', 'tfbSyncDispatchProbe', 'OK', 'workflow reachable: id=' + wfId + ' state=' + state + ' path=' + path, req.url, 200, dur,
      { repo: cfg.repo, workflow: cfg.workflow, ref: cfg.ref, hours: cfg.hours, minute: cfg.minute, state: state }, cfg.token);
    return 'OK:200:' + state;
  }
  var hint = code === 401 ? 'bad/expired token' : code === 403 ? 'token lacks Actions permission' :
    code === 404 ? 'repo/workflow not visible to this token' : 'network or API error';
  tfbSdLog_('ERROR', 'tfbSyncDispatchProbe', 'FAILED', 'HTTP ' + code + ' (' + hint + ') ' + tfbSdClip_(body), req.url, code, dur,
    { repo: cfg.repo, workflow: cfg.workflow, hint: hint }, cfg.token);
  return 'FAILED:' + code + ':' + hint;
}

// ---------------------------------------------------------------------------
// Triggers
// ---------------------------------------------------------------------------

function tfbSdOwnTriggers_() {
  var out = [];
  try {
    var all = ScriptApp.getProjectTriggers();
    for (var i = 0; i < all.length; i++) {
      if (all[i].getHandlerFunction() === TFB_SYNC_DISPATCH_.HANDLER) { out.push(all[i]); }
    }
  } catch (err) { /* noop */ }
  return out;
}

function tfbRemoveSyncDispatchTriggers() {
  var own = tfbSdOwnTriggers_();
  var n = 0;
  for (var i = 0; i < own.length; i++) {
    try { ScriptApp.deleteTrigger(own[i]); n++; } catch (err) { /* noop */ }
  }
  tfbSdLog_('INFO', 'tfbRemoveSyncDispatchTriggers', 'OK', 'removed ' + n + ' trigger(s) for ' + TFB_SYNC_DISPATCH_.HANDLER, '', '', 0, { removed: n }, '');
  return 'OK:removed=' + n;
}

// Idempotent: removes this handler's triggers, then creates one daily
// trigger per configured hour (Asia/Riyadh) near the configured minute.
function tfbInstallSyncDispatchTriggers() {
  var C = TFB_SYNC_DISPATCH_;
  var cfg = tfbSdConfig_();
  var own = tfbSdOwnTriggers_();
  var removed = 0;
  for (var i = 0; i < own.length; i++) {
    try { ScriptApp.deleteTrigger(own[i]); removed++; } catch (err) { /* noop */ }
  }
  var created = [];
  for (var h = 0; h < cfg.hours.length; h++) {
    try {
      ScriptApp.newTrigger(C.HANDLER)
        .timeBased()
        .everyDays(1)
        .atHour(cfg.hours[h])
        .nearMinute(cfg.minute)
        .inTimezone(C.TIMEZONE)
        .create();
      created.push(cfg.hours[h]);
    } catch (err2) {
      tfbSdLog_('ERROR', 'tfbInstallSyncDispatchTriggers', 'FAILED', 'create failed for hour ' + cfg.hours[h] + ': ' + String(err2 && err2.message ? err2.message : err2), '', '', 0, { hour: cfg.hours[h] }, '');
    }
  }
  tfbSdLog_('INFO', 'tfbInstallSyncDispatchTriggers', created.length === cfg.hours.length ? 'OK' : 'PARTIAL',
    'installed ' + created.length + '/' + cfg.hours.length + ' daily trigger(s) at hours [' + created.join(',') + '] near minute ' + cfg.minute + ' ' + C.TIMEZONE + ' (removed ' + removed + ' old)',
    '', '', 0, { hours: created, minute: cfg.minute, removed: removed }, '');
  return 'OK:hours=' + created.join(',') + ':minute=' + cfg.minute + ':removed=' + removed;
}

// ---------------------------------------------------------------------------
// Status + self-test
// ---------------------------------------------------------------------------

function tfbSyncDispatchStatus() {
  var C = TFB_SYNC_DISPATCH_;
  var cfg = tfbSdConfig_();
  var own = tfbSdOwnTriggers_();
  var last = tfbSdProp_(C.PROP_LAST_EVENT, '');
  var s = 'SYNC-DISPATCH v' + TFB_SYNC_DISPATCH_VERSION +
    ' | token=' + (cfg.token === '' ? 'MISSING' : 'present(' + cfg.token.length + ' chars)') +
    ' | repo=' + cfg.repo + ' | workflow=' + cfg.workflow + ' | ref=' + cfg.ref +
    ' | hours=' + cfg.hours.join(',') + ' | minute=' + cfg.minute +
    ' | disabled=' + (cfg.disabled ? 'YES' : 'no') +
    ' | min_gap_min=' + Math.round(cfg.minGapMs / 60000) + ' | fresh_skip_min=' + Math.round(cfg.freshSkipMs / 60000) +
    ' | triggers=' + own.length +
    ' | last_ok=' + (cfg.lastOkMs > 0 ? new Date(cfg.lastOkMs).toISOString() : 'never') +
    ' | last_event=' + tfbSdRedact_(last, cfg.token);
  try { Logger.log(s); } catch (err) { /* noop */ }
  return s;
}

// Pure-logic self-test: no network, no sheet write, no trigger change.
function tfbSyncDispatchSelfTest() {
  var fails = [];
  function ok(cond, name) { if (!cond) { fails.push(name); } }
  // T1 hour parsing
  ok(tfbSdParseHours_('6,15').join(',') === '6,15', 'hours basic');
  ok(tfbSdParseHours_('').join(',') === '6', 'hours default');
  ok(tfbSdParseHours_('15, 6 ;x,25,6').join(',') === '6,15', 'hours dedup/sort/invalid');
  ok(tfbSdParseIntProp_('45', 15, 0, 59) === 45 && tfbSdParseIntProp_('61', 15, 0, 59) === 15 && tfbSdParseIntProp_('', 15, 0, 59) === 15, 'int prop bounds');
  // T2 ISO parsing
  ok(tfbSdParseIsoMs_('2026-09-29T01:28:32.942810+00:00') === Date.parse('2026-09-29T01:28:32.942+00:00'), 'iso microseconds');
  ok(tfbSdParseIsoMs_('2026-09-29 04:27:48+03:00') === Date.parse('2026-09-29T04:27:48+03:00'), 'iso space form');
  ok(tfbSdParseIsoMs_('OK | cov=100.0 | run=36502914213 | 2026-09-29 04:27:50+03:00') === Date.parse('2026-09-29T04:27:50+03:00'), 'iso feed key form');
  ok(tfbSdParseIsoMs_('') === 0 && tfbSdParseIsoMs_('garbage') === 0, 'iso empty/garbage');
  ok(tfbSdParseIsoMs_(new Date(1000)) === 1000, 'iso Date object');
  // T3 decision table
  var now = 1000000000000;
  var base = { disabled: false, tokenPresent: true, force: false, nowMs: now, lastOkMs: 0, minGapMs: 7200000, holdUntilMs: 0, holdCeilingMs: 960000, gmStampMs: 0, freshSkipMs: 5400000 };
  function d(over) { var x = {}; for (var k in base) { if (base.hasOwnProperty(k)) { x[k] = base[k]; } } for (var k2 in over) { if (over.hasOwnProperty(k2)) { x[k2] = over[k2]; } } return tfbSdDecide_(x); }
  ok(d({}).dispatch === true && d({}).reason === 'ok', 'decide default ok');
  ok(d({ disabled: true }).reason === 'disabled', 'decide disabled');
  ok(d({ tokenPresent: false }).reason === 'no_token', 'decide no token');
  ok(d({ holdUntilMs: now + 60000 }).reason === 'sync_in_flight', 'decide hold live');
  ok(d({ holdUntilMs: now - 1 }).dispatch === true, 'decide hold expired');
  ok(d({ holdUntilMs: now + 3600000 }).dispatch === true, 'decide hold beyond ceiling ignored');
  ok(d({ lastOkMs: now - 600000 }).reason === 'min_gap', 'decide min gap');
  ok(d({ lastOkMs: now - 7200001 }).dispatch === true, 'decide gap elapsed');
  ok(d({ gmStampMs: now - 1800000 }).reason === 'gm_fresh', 'decide gm fresh');
  ok(d({ gmStampMs: now - 5400001 }).dispatch === true, 'decide gm stale');
  ok(d({ force: true, lastOkMs: now - 600000, gmStampMs: now - 60000 }).reason === 'forced', 'decide force bypasses gap/fresh');
  ok(d({ force: true, holdUntilMs: now + 60000 }).reason === 'sync_in_flight', 'decide force keeps hold');
  ok(d({ force: true, disabled: true }).reason === 'disabled', 'decide force keeps kill switch');
  // T4 request shape
  var rq = tfbSdBuildDispatchRequest_('o/r', 'daily_sync.yml', 'main', 'tok_ABCDEFG');
  ok(rq.url === 'https://api.github.com/repos/o/r/actions/workflows/daily_sync.yml/dispatches', 'dispatch url');
  ok(rq.options.method === 'post' && rq.options.muteHttpExceptions === true, 'dispatch method');
  var pl = JSON.parse(rq.options.payload);
  ok(pl.ref === 'main' && pl.inputs && pl.inputs.run_mode === 'full_sync', 'dispatch payload');
  ok(rq.options.headers.Authorization === 'Bearer tok_ABCDEFG' && rq.options.headers['X-GitHub-Api-Version'] === '2022-11-28', 'dispatch headers');
  var pr = tfbSdBuildProbeRequest_('o/r', 'daily_sync.yml', 'tok_ABCDEFG');
  ok(pr.url === 'https://api.github.com/repos/o/r/actions/workflows/daily_sync.yml' && pr.options.method === 'get', 'probe request');
  // T5 redaction
  ok(tfbSdRedact_('Bearer tok_ABCDEFG failed tok_ABCDEFG', 'tok_ABCDEFG') === 'Bearer [redacted] failed [redacted]', 'redact');
  ok(tfbSdRedact_('x', '') === 'x' && tfbSdRedact_(null, 'tok_ABCDEFG') === '', 'redact edge');
  ok(tfbSdClip_('abcdefghij', 4) === 'abcd...', 'clip');
  var verdict = fails.length === 0 ? 'sync dispatch core: ok' : 'sync dispatch core: FAIL ' + fails.length + ' [' + fails.join('; ') + ']';
  try { Logger.log('[SYNC-DISPATCH v' + TFB_SYNC_DISPATCH_VERSION + '] selftest -> ' + verdict); } catch (err) { /* noop */ }
  return verdict;
}
