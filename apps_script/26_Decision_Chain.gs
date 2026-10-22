/**
 * 26_Decision_Chain.gs -- TFB Decision Chain v1.0.2 (2026-10-23)
 * ============================================================================
 * Refresh Portfolio_Decision and Top_10_Investments when the sync's data
 * LANDS, instead of on fixed clocks.
 *
 * v1.0.2 -- require native receipt readback before completing a cohort.
 * A successful HTTP response / nonthrowing refresh may render WITHHELD,
 * return early, or leave an older board in place. Only a changed status
 * line, stamped during this attempt and carrying exactly one VERIFIED
 * source receipt for the planned run, completes the chain step. A rendered
 * Top_10 may still be WITHHELD pending release; execution gates are separate.
 * Unproven renders retain the existing bounded retry path. Default off and
 * observe behavior are unchanged; no receipt or release verdict is written.
 *
 * v1.0.1 (2026-10-19) -- RE-RENDER TOP_10 AFTER A RELEASE. The P0-1 release
 * (release_decision_feed.yml) promotes 'TFB Decision Feed' to EXECUTABLE
 * only AFTER the cockpits consumed the cohort, and the board shows the
 * promotion only on a render AFTER it (P0-1 commit sheet, prerequisite 3).
 * Until now that second render waited for the 4-hourly wrapper, inside the
 * release's 480-minute clock. Now, when the feed reads EXECUTABLE with
 * run=N for the cohort this chain already refreshed (state.t10 == N) and an
 * issued= stamp the chain has not rendered yet (state.t10rel), one more
 * Top_10 refresh is planned (same skips: in flight, cockpit running, quiet,
 * manual priority; TFB_DC_MAX_ATTEMPTS per issued= stamp). A NOT_ACTIONABLE
 * feed, a feed for another run, or an issued= already rendered plans
 * nothing. When the normal post-audit refresh already happens under an
 * EXECUTABLE feed, that render counts (state.t10rel is set with it), so a
 * release never causes two renders. Functions added: 1 (tfbDcReleaseOf_,
 * PURE). Changed: tfbDcStateOf_ (t10rel), tfbDcPlanCore_ (release branch),
 * tfbDcReadInputs_ (release fields), tfbDecisionChainTick (t10rel state,
 * attempts key, prune). Removed: 0. Default (TFB_DECISION_CHAIN unset/off)
 * still returns before any read. Rollback: v1.0.0, or off.
 *
 * WHY (GitHub Actions + _Run_Log evidence, 14-16 Oct 2026): the sync
 * publishes My_Portfolio ~2 min into a run and the market pages by the end of
 * its recovery leg, but nothing in the project reacts to a publication:
 *   - Top_10 refreshes on the 08:07 morning trigger and a 4-hourly wrapper
 *     (13_AutoRefresh) -> on 14 Oct it rendered ~4 h 24 min after
 *     Global_Markets landed;
 *   - Portfolio_Decision has no scheduled producer outside the IBKR capture
 *     watcher (08:30-10:30) -> it trailed its My_Portfolio source by ~6 h
 *     (02:07 -> 08:45 Riyadh on 16 Oct; 02:46 -> 08:50 on 14 Oct);
 *   - so the final audit inside every sync run always meets decision pages
 *     older than the cohort it audits.
 *
 * WHAT: one 5-minute time trigger (installed only by tfbInstallDecisionChain),
 * Script Property TFB_DECISION_CHAIN = off (DEFAULT: the handler returns
 * immediately, nothing is read or written) | observe | on.
 * Each tick reads _Status once and plans at most ONE refresh (PURE
 * tfbDcPlanCore_):
 *   1. Portfolio_Decision when the My_Portfolio publication passes the
 *      existing 10_My_Portfolio readiness check (pdSrcReadiness_: SUCCESS,
 *      run=N, <= 8 h, acquired 100%, data COMPLETE ...) and run N has not
 *      been refreshed by the chain yet.
 *   2. Top_10_Investments when 'TFB Feed Global_Markets' carries run=N AND
 *      'TFB Decision Feed' carries run=N (the final audit job of that run has
 *      finished, so the pages are final) and N is new.
 *   Skips (no claim): sync in flight (16_Decision_Top10 dt10SyncInflightCore_),
 *   a cockpit run in progress (DT10_RUN_MARKER younger than 7 min), another
 *   chain claim younger than 7 min, the coordinator's manual-priority or
 *   backend-hold guard (11_Manual_Refresh_Coordinator
 *   tfbAutomaticRefreshAllowed_), and -- for Top_10 only -- the quiet windows
 *   around the S-1 board dispatch slots (TFB_DECISION_CHAIN_QUIET, default
 *   07:55-08:25,16:55-17:25 Riyadh) so a board never reads a half-rendered
 *   cockpit.
 *   observe: logs one _Run_Log row per (action, run) it WOULD perform.
 *   on: claims the cohort under the script lock, runs the existing refresh
 *   function OUTSIDE the lock (the cockpit takes the script lock itself), then
 *   records the outcome; a failed refresh is retried on later ticks, at most
 *   TFB_DC_MAX_ATTEMPTS times per (action, run).
 * UNTOUCHED: refreshPortfolioDecision, refreshDecisionTop10, every gate,
 * receipt, verdict and release path. The release (release_decision_feed.yml)
 * stays manual. No HTTP call is made by this file. ES5 only.
 * Kill: TFB_DECISION_CHAIN=off (or tfbRemoveDecisionChain()).
 * ============================================================================
 */

var TFB_DC_VERSION = '1.0.2';
var TFB_DC_MODE_PROP = 'TFB_DECISION_CHAIN';          // off (default) | observe | on
var TFB_DC_QUIET_PROP = 'TFB_DECISION_CHAIN_QUIET';   // Riyadh windows, 'none' = no quiet
var TFB_DC_STATE_PROP = 'TFB_DECISION_CHAIN_STATE';   // JSON, owned by this file
var TFB_DC_QUIET_DEFAULT = '07:55-08:25,16:55-17:25';
var TFB_DC_HANDLER = 'tfbDecisionChainTick';
var TFB_DC_EVERY_MIN = 5;
var TFB_DC_CLAIM_TTL_MIN = 7;                          // GAS 6-minute limit + skew
var TFB_DC_COCKPIT_MARKER_PROP = 'DT10_RUN_MARKER';   // 16_Decision_Top10 v1.11.12
var TFB_DC_MAX_ATTEMPTS = 3;
var TFB_DC_GM_FEED_KEY = 'TFB Feed Global_Markets';
var TFB_DC_ML_FEED_KEY = 'TFB Feed Market_Leaders';
var TFB_DC_DECISION_FEED_KEY = 'TFB Decision Feed';
var TFB_DC_INFLIGHT_MAX_MIN = 90;

/* ------------------------------------------------------------------ PURE --- */

/** Mode from a raw property value: 'observe' | 'on'; anything else 'off'. */
function tfbDcModeOf_(raw) {
  var v = String(raw === null || raw === undefined ? '' : raw)
      .replace(/^\s+|\s+$/g, '').toLowerCase();
  return (v === 'observe' || v === 'on') ? v : 'off';
}

/** 'HH:MM-HH:MM[,HH:MM-HH:MM...]' -> [{from, to}] minutes of the Riyadh day.
 * 'none' / '' -> []. Malformed parts are dropped. Never throws. */
function tfbDcQuietWindows_(raw) {
  var out = [];
  var s = String(raw === null || raw === undefined ? TFB_DC_QUIET_DEFAULT : raw)
      .replace(/\s+/g, '');
  if (!s || s.toLowerCase() === 'none') return out;
  var parts = s.split(',');
  for (var i = 0; i < parts.length; i++) {
    var m = parts[i].match(/^(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})$/);
    if (!m) continue;
    var a = (+m[1]) * 60 + (+m[2]), b = (+m[3]) * 60 + (+m[4]);
    if (+m[1] > 23 || +m[3] > 23 || +m[2] > 59 || +m[4] > 59) continue;
    out.push({from: a, to: b});
  }
  return out;
}

/** Is nowMs (epoch) inside any Riyadh (UTC+3) window? */
function tfbDcInQuiet_(windows, nowMs) {
  var t = Math.floor(((nowMs + 3 * 3600000) % 86400000) / 60000);
  for (var i = 0; i < (windows || []).length; i++) {
    var w = windows[i];
    if (w.from <= w.to ? (t >= w.from && t < w.to) : (t >= w.from || t < w.to)) {
      return true;
    }
  }
  return false;
}

/** The single 'run=<digits>' token of a feed / verdict value, or ''. */
function tfbDcRunOf_(raw) {
  var s = String(raw === null || raw === undefined ? '' : raw);
  var re = /(?:^|[\s|])run=(\d+)/g;
  var first = re.exec(s);
  return first && !re.exec(s) ? first[1] : '';
}

/** v1.0.1 PURE: an EXECUTABLE release value -> {run, issued}; anything else
 * -> {run: '', issued: ''}. The head token must be exactly EXECUTABLE (the
 * cockpit's own rule) and the value must carry exactly one run= and exactly
 * one issued= token (the release grammar). */
function tfbDcReleaseOf_(raw) {
  var s = String(raw === null || raw === undefined ? '' : raw);
  var out = {run: '', issued: ''};
  if (s.split('|')[0].replace(/^\s+|\s+$/g, '') !== 'EXECUTABLE') return out;
  var run = tfbDcRunOf_(s);
  var re = /(?:^|[\s|])issued=(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2})/g;
  var first = re.exec(s);
  if (!run || !first || re.exec(s)) return out;
  out.run = run;
  out.issued = first[1];
  return out;
}

/** Parse the chain's own state JSON; anything malformed -> empty state. */
function tfbDcStateOf_(raw) {
  var st = {pd: '', t10: '', t10rel: '', claim: null, attempts: {}, observed: {}};
  try {
    var o = JSON.parse(String(raw || ''));
    if (o && typeof o === 'object') {
      st.pd = String(o.pd || '');
      st.t10 = String(o.t10 || '');
      st.t10rel = String(o.t10rel || '');   /* v1.0.1: issued= last rendered */
      st.claim = (o.claim && typeof o.claim === 'object') ? o.claim : null;
      st.attempts = (o.attempts && typeof o.attempts === 'object') ? o.attempts : {};
      st.observed = (o.observed && typeof o.observed === 'object') ? o.observed : {};
    }
  } catch (e) {}
  return st;
}

/** PURE: completion means this attempt rendered and consumed its planned
 * source cohort, not merely that a refresh returned without throwing.
 * Last run is a second-precision Riyadh wall clock, as in both producers.
 * Execution eligibility (e.g. output: WITHHELD) is deliberately not tested:
 * the release audit needs these receipts before it can release the feed. */
function tfbDcReceiptCore_(before, after, run, startedMs, nowMs) {
  function fail(reason) { return {ok: false, reason: reason}; }
  if (typeof before !== 'string') {
    return fail('native status before the refresh was unavailable');
  }
  if (typeof after !== 'string' || !after || after === before) {
    return fail('native status was not updated');
  }
  if (!/^[1-9]\d*$/.test(String(run || ''))) return fail('planned run invalid');
  var occurrences = after.match(/source\s+readiness/gi) || [];
  var receipt = after.match(/(?:^|\|)\s*source readiness VERIFIED \(run ([1-9]\d*)\)(?=\s*(?:\||$))/);
  if (occurrences.length !== 1 || !receipt || receipt[1] !== String(run)) {
    return fail('native source receipt does not verify the planned run');
  }
  var status = after.match(/^Last run (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s*\|\s*status:\s*ok(?=\s*(?:\||$))/i);
  var stamp = status && typeof pdSrcStampMs_ === 'function'
      ? pdSrcStampMs_(status[1]) : null;
  if (typeof startedMs !== 'number' || !isFinite(startedMs) ||
      typeof nowMs !== 'number' || !isFinite(nowMs) || nowMs < startedMs ||
      stamp === null || stamp < Math.floor(startedMs / 1000) * 1000 || stamp > nowMs) {
    return fail('native render timestamp is outside this attempt');
  }
  return {ok: true, reason: ''};
}

/** PURE plan. s = {mode, nowMs, state, pd: {available, ready, cohort, reason},
 * t10: {available, gmRun, decisionRun}, inflight, cockpitRunning, quiet,
 * automationAllowed}. -> {action: 'none'|'pd'|'t10', run, reason}. */
function tfbDcPlanCore_(s) {
  function none(reason) { return {action: 'none', run: '', reason: reason}; }
  if (!s || s.mode === 'off') return none('off');
  var st = s.state || {pd: '', t10: '', claim: null, attempts: {}};
  var c = st.claim;
  if (c && typeof c.at_ms === 'number' &&
      s.nowMs - c.at_ms < TFB_DC_CLAIM_TTL_MIN * 60000 && s.mode === 'on') {
    return none('claimed ' + c.act + ' run ' + c.run);
  }
  if (s.automationAllowed === false) return none('automation paused (manual priority or backend hold)');
  if (s.inflight) return none('sync in flight');
  var att = st.attempts || {};
  var pd = s.pd || {};
  if (pd.available && pd.ready && /^[1-9]\d*$/.test(String(pd.cohort || '')) &&
      String(pd.cohort) !== st.pd &&
      (att['pd:' + pd.cohort] || 0) < TFB_DC_MAX_ATTEMPTS) {
    return {action: 'pd', run: String(pd.cohort),
            reason: 'My_Portfolio run ' + pd.cohort + ' published and ready'};
  }
  var t = s.t10 || {};
  var run = String(t.gmRun || '');
  if (!t.available) return none('nothing new (Top_10 refresh unavailable)');
  if (!/^[1-9]\d*$/.test(run)) return none('Global_Markets feed run unreadable');
  /* v1.0.1: the issued= stamp of an EXECUTABLE feed for THIS cohort, or '' */
  var issued = (String(t.releaseRun || '') === run) ? String(t.releaseIssued || '') : '';
  if (run === st.t10) {
    /* v1.0.1: the board shows a release only on a render AFTER it (P0-1
     * prerequisite 3): one more Top_10 refresh per issued= stamp. */
    if (!issued || issued === String(st.t10rel || '')) {
      return none('Top_10 already refreshed for run ' + run);
    }
    if ((att['t10rel:' + issued] || 0) >= TFB_DC_MAX_ATTEMPTS) {
      return none('Top_10 release re-render gave up after ' + TFB_DC_MAX_ATTEMPTS + ' attempts');
    }
    if (s.cockpitRunning) return none('cockpit refresh in progress');
    if (s.quiet) return none('quiet window (S-1 board slot)');
    return {action: 't10', run: run, release: issued,
            reason: 'release issued ' + issued + ' for run ' + run};
  }
  if (String(t.decisionRun || '') !== run) {
    return none('run ' + run + ' final audit not finished');
  }
  if ((att['t10:' + run] || 0) >= TFB_DC_MAX_ATTEMPTS) {
    return none('Top_10 run ' + run + ' gave up after ' + TFB_DC_MAX_ATTEMPTS + ' attempts');
  }
  if (s.cockpitRunning) return none('cockpit refresh in progress');
  if (s.quiet) return none('quiet window (S-1 board slot)');
  /* v1.0.1: a post-audit render under an already EXECUTABLE feed counts as
   * the release render too (release: issued), so a release never costs two. */
  return {action: 't10', run: run, release: issued,
          reason: 'run ' + run + ' final audit done'};
}

/* -------------------------------------------------------------------- I/O --- */

function tfbDcProps_() { return PropertiesService.getScriptProperties(); }

function tfbDcMode_() {
  try { return tfbDcModeOf_(tfbDcProps_().getProperty(TFB_DC_MODE_PROP)); }
  catch (e) { return 'off'; }
}

/** _Status L:M value by key (same reading as dt10UvReadKeyed_). */
function tfbDcStatusValue_(ss, key) {
  if (typeof dt10UvReadKeyed_ === 'function') return dt10UvReadKeyed_(ss, key);
  try {
    var sh = ss.getSheetByName('_Status');
    if (!sh) return '';
    var vals = sh.getRange('L1:M60').getValues();
    var want = String(key).toLowerCase();
    for (var i = 0; i < vals.length; i++) {
      if (String(vals[i][0] || '').replace(/^\s+|\s+$/g, '').toLowerCase() === want) {
        return String(vals[i][1] || '');
      }
    }
  } catch (e) {}
  return '';
}

/** The producer-owned B2 status line. Missing/unreadable is not evidence. */
function tfbDcReadReceipt_(ss, action) {
  try {
    var name = action === 'pd' ? 'Portfolio_Decision' : 'Top_10_Investments';
    var sh = ss.getSheetByName(name);
    if (!sh) return null;
    var value = sh.getRange(2, 2).getValue();
    return typeof value === 'string' ? value : null;
  } catch (e) { return null; }
}

/** Everything the plan needs, read once. Never throws. */
function tfbDcReadInputs_(ss, mode, state, nowMs) {
  var s = {mode: mode, nowMs: nowMs, state: state,
           pd: {available: typeof refreshPortfolioDecision === 'function' &&
                           typeof pdSrcReadiness_ === 'function',
                ready: false, cohort: '', reason: ''},
           t10: {available: typeof refreshDecisionTop10 === 'function',
                 gmRun: '', decisionRun: '', releaseRun: '', releaseIssued: ''},
           inflight: false, cockpitRunning: false, quiet: false,
           automationAllowed: true};
  try {
    if (s.pd.available) {
      var src = pdSrcReadiness_(ss, null, null);
      s.pd.ready = !!src.ready; s.pd.cohort = String(src.cohort || '');
      s.pd.reason = String(src.reason || '');
    }
  } catch (ePd) { s.pd.ready = false; s.pd.reason = 'readiness unavailable'; }
  try {
    var gm = tfbDcStatusValue_(ss, TFB_DC_GM_FEED_KEY);
    var ml = tfbDcStatusValue_(ss, TFB_DC_ML_FEED_KEY);
    s.t10.gmRun = tfbDcRunOf_(gm);
    var feedRaw = tfbDcStatusValue_(ss, TFB_DC_DECISION_FEED_KEY);
    s.t10.decisionRun = tfbDcRunOf_(feedRaw);
    var rel = tfbDcReleaseOf_(feedRaw);          /* v1.0.1 */
    s.t10.releaseRun = rel.run;
    s.t10.releaseIssued = rel.issued;
    if (typeof dt10SyncInflightCore_ === 'function') {
      s.inflight = !!dt10SyncInflightCore_(ml, gm, nowMs, TFB_DC_INFLIGHT_MAX_MIN).active;
    }
  } catch (eSt) {}
  try {
    var marker = tfbDcProps_().getProperty(TFB_DC_COCKPIT_MARKER_PROP) || '';
    if (marker && typeof dt10RunMarkerCore_ === 'function') {
      var j = dt10RunMarkerCore_(marker, nowMs, TFB_DC_CLAIM_TTL_MIN);
      s.cockpitRunning = j.ageMin !== null && !j.aborted;
    }
  } catch (eMk) {}
  try {
    s.quiet = tfbDcInQuiet_(tfbDcQuietWindows_(
        tfbDcProps_().getProperty(TFB_DC_QUIET_PROP)), nowMs);
  } catch (eQ) {}
  try {
    if (typeof tfbAutomaticRefreshAllowed_ === 'function') {
      s.automationAllowed = !!tfbAutomaticRefreshAllowed_(TFB_DC_HANDLER);
    }
  } catch (eA) {}
  return s;
}

/** One _Run_Log row, 10-column house format. Never throws. */
function tfbDcLog_(level, page, status, message, ms, details) {
  try {
    var sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('_Run_Log');
    if (!sh) return false;
    var dj = '{}';
    try { dj = JSON.stringify(details || {}); } catch (eJ) {}
    sh.appendRow([new Date(), String(level || 'INFO'), TFB_DC_HANDLER,
                  String(page || ''), String(status || ''),
                  String(message || '').slice(0, 400), '', '',
                  ms === null || ms === undefined ? '' : String(ms), dj]);
    return true;
  } catch (e) { return false; }
}

function tfbDcWithLock_(fn) {
  var lock = null;
  try {
    lock = LockService.getScriptLock();
    if (!lock.tryLock(5000)) return {busy: true};
  } catch (eL) { return {busy: true}; }
  try { return {busy: false, value: fn()}; }
  finally { try { lock.releaseLock(); } catch (eR) {} }
}

/** Time-driven (tfbInstallDecisionChain). Returns a short outcome string. */
function tfbDecisionChainTick() {
  var mode = tfbDcMode_();
  if (mode === 'off') return 'off';
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var planned = tfbDcWithLock_(function() {
    var props = tfbDcProps_();
    var state = tfbDcStateOf_(props.getProperty(TFB_DC_STATE_PROP));
    var nowMs = Date.now();
    /* observe walks both steps: what it already reported counts as done */
    var view = mode === 'observe'
        ? {pd: state.observed.pd || state.pd, t10: state.observed.t10 || state.t10,
           t10rel: state.observed.t10rel || state.t10rel,
           claim: null, attempts: {}, observed: state.observed}
        : state;
    var plan = tfbDcPlanCore_(tfbDcReadInputs_(ss, mode, view, nowMs));
    if (plan.action === 'none') return {plan: plan};
    /* v1.0.1: a release re-render is keyed by its issued= stamp */
    var rerender = plan.action === 't10' && plan.release && plan.run === String(state.t10 || '');
    var key = rerender ? 't10rel:' + plan.release : plan.action + ':' + plan.run;
    if (mode === 'observe') {
      var seenKey = rerender ? 't10rel' : plan.action;
      var seenVal = rerender ? plan.release : plan.run;
      if (state.observed[seenKey] !== seenVal) {   /* always true via view */
        state.observed[seenKey] = seenVal;
        if (plan.release && !rerender) state.observed.t10rel = plan.release;
        props.setProperty(TFB_DC_STATE_PROP, JSON.stringify(state));
        tfbDcLog_('INFO', plan.action === 'pd' ? 'Portfolio_Decision' : 'Top_10_Investments',
                  'OBSERVE', '[DECISION-CHAIN v' + TFB_DC_VERSION + '] would refresh ' +
                  plan.action + ' for run ' + plan.run + ' (' + plan.reason + ')', null,
                  {version: TFB_DC_VERSION, mode: mode, act: plan.action, run: plan.run});
      }
      return {plan: plan, observed: true};
    }
    state.claim = {act: plan.action, run: plan.run, at_ms: nowMs};
    state.attempts[key] = (state.attempts[key] || 0) + 1;
    props.setProperty(TFB_DC_STATE_PROP, JSON.stringify(state));
    return {plan: plan, key: key};
  });
  if (planned.busy) return 'busy';
  var p = planned.value;
  if (!p.key) return p.observed ? 'observed ' + p.plan.action + ' run ' + p.plan.run
                                : p.plan.reason;
  var t0 = Date.now(), ok = false, outcome = '';
  var previous = tfbDcReadReceipt_(ss, p.plan.action);
  try {
    if (p.plan.action === 'pd') {
      outcome = String(refreshPortfolioDecision() || '');
    } else {
      refreshDecisionTop10();
      outcome = 'rendered';
    }
    if (p.plan.action !== 'pd' || outcome.toLowerCase() === 'ok') {
      SpreadsheetApp.flush();
      var verified = tfbDcReceiptCore_(previous,
          tfbDcReadReceipt_(ss, p.plan.action), p.plan.run, t0, Date.now());
      ok = verified.ok;
      if (!ok) outcome += ' | ' + verified.reason;
    }
  } catch (eRun) {
    outcome = 'error: ' + String(eRun && eRun.message ? eRun.message : eRun).slice(0, 200);
  }
  var ms = Date.now() - t0;
  tfbDcWithLock_(function() {
    var props = tfbDcProps_();
    var state = tfbDcStateOf_(props.getProperty(TFB_DC_STATE_PROP));
    if (ok) {
      state[p.plan.action] = p.plan.run;
      if (p.plan.action === 't10') state.t10rel = String(p.plan.release || '');   /* v1.0.1 */
      delete state.attempts[p.key];
    }
    state.claim = null;
    var keep = {};   // prune attempts to the cohort (or release stamp) being worked on
    for (var k in state.attempts) {
      if (!state.attempts.hasOwnProperty(k)) continue;
      var tail = String(k).slice(String(k).indexOf(':') + 1);
      if (tail === p.plan.run || (p.plan.release && tail === p.plan.release)) keep[k] = state.attempts[k];
    }
    state.attempts = keep;
    props.setProperty(TFB_DC_STATE_PROP, JSON.stringify(state));
    return true;
  });
  tfbDcLog_(ok ? 'INFO' : 'ERROR',
            p.plan.action === 'pd' ? 'Portfolio_Decision' : 'Top_10_Investments',
            ok ? 'OK' : 'FAILED',
            '[DECISION-CHAIN v' + TFB_DC_VERSION + '] ' + p.plan.action + ' run ' +
            p.plan.run + ' -> ' + outcome + ' (' + p.plan.reason + ')', ms,
            {version: TFB_DC_VERSION, act: p.plan.action, run: p.plan.run, ok: ok});
  return (ok ? 'refreshed ' : 'failed ') + p.plan.action + ' run ' + p.plan.run;
}

/* -------------------------------------------------------------- OPERATOR --- */

function tfbDcRemove_() {
  var n = 0;
  var all = ScriptApp.getProjectTriggers();
  for (var i = 0; i < all.length; i++) {
    if (all[i].getHandlerFunction() === TFB_DC_HANDLER) {
      ScriptApp.deleteTrigger(all[i]);
      n++;
    }
  }
  return n;
}

/** Operator: install (or replace) the 5-minute chain trigger. The handler
 * still does nothing until TFB_DECISION_CHAIN is observe or on. */
function tfbInstallDecisionChain() {
  var replaced = tfbDcRemove_();
  ScriptApp.newTrigger(TFB_DC_HANDLER).timeBased().everyMinutes(TFB_DC_EVERY_MIN).create();
  var msg = '[DECISION-CHAIN v' + TFB_DC_VERSION + '] installed: every ' + TFB_DC_EVERY_MIN +
      ' min, mode ' + tfbDcMode_() + (replaced ? ' (replaced ' + replaced + ')' : '');
  Logger.log(msg);
  try { SpreadsheetApp.getUi().alert(msg); } catch (e) {}
  return msg;
}

/** Operator: remove the chain trigger(s). */
function tfbRemoveDecisionChain() {
  var msg = '[DECISION-CHAIN v' + TFB_DC_VERSION + '] removed: ' + tfbDcRemove_();
  Logger.log(msg);
  try { SpreadsheetApp.getUi().alert(msg); } catch (e) {}
  return msg;
}

/** Operator: read-only status (mode, state, what the next tick would plan). */
function tfbDecisionChainStatus() {
  var mode = tfbDcMode_();
  var state = tfbDcStateOf_(tfbDcProps_().getProperty(TFB_DC_STATE_PROP));
  var plan = tfbDcPlanCore_(tfbDcReadInputs_(SpreadsheetApp.getActiveSpreadsheet(),
      mode === 'off' ? 'observe' : mode, state, Date.now()));
  var msg = '[DECISION-CHAIN v' + TFB_DC_VERSION + '] mode=' + mode +
      ' last_pd=' + (state.pd || '-') + ' last_t10=' + (state.t10 || '-') +
      ' last_release_render=' + (state.t10rel || '-') +
      ' next=' + plan.action + (plan.run ? ' run ' + plan.run : '') + ' (' + plan.reason + ')';
  Logger.log(msg);
  return msg;
}
