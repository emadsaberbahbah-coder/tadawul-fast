/**
 * 26_Decision_Chain.gs -- TFB Decision Chain v1.0.3 (2026-10-24)
 * ============================================================================
 * Refresh Portfolio_Decision and Top_10_Investments when the sync's data
 * LANDS, instead of on fixed clocks.
 *
 * v1.0.3 WHY: stack #804 and newer main independently repaired this chain.
 * Preserve typed completion, source-publication identity, owned claims and HELD
 * deferrals, and also require main's changed B2 receipt readback stamped during
 * this attempt. EXECUTABLE release-issued renders remain tracked and bounded;
 * a completed render under a release consumes that release exactly once.
 * Legacy unknown publication identities still need reconciliation. No source,
 * financial, trigger, retry-budget, eligibility or release policy is changed.
 *
 * v1.0.2 (main, 2026-10-23): require changed native source receipt readback,
 * including script-timezone and DST-safe render timestamp verification.
 * v1.0.1 (main, 2026-10-19): re-render Top_10 after each EXECUTABLE issued=.
 *
 * v1.0.1 WHY: a normal return did not prove that a native decision refreshed
 * the planned source cohort. HELD, legacy strings and an advanced/unverified
 * source must never acknowledge completion. Only a proven no-work deferral
 * refunds its attempt; a rendered/unverifiable result retains the bounded budget.
 * Only the typed tfb_native_refresh_v1 receipt can complete an exact cohort.
 * Claims now carry a nonce; the finishing transaction may change state only
 * while that exact owner/action/run/start/publication still owns it. Completed
 * and observed runs retain publication identities, so a later same-run rerun
 * cannot inherit an old completion. Legacy unknown identities need reconciliation;
 * legacy attempt budgets remain conservative floors. Oversized state defers.
 * Unreadable or ambiguous
 * source feeds never imply "no sync in flight". No gates or release changed.
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
 * after reading only its mode property; no Sheet/HTTP/state/trigger work)
 * | observe | on.
 * Each tick reads bounded source status and plans at most ONE refresh (PURE
 * tfbDcPlanCore_):
 *   1. Portfolio_Decision when the My_Portfolio publication passes the
 *      existing 10_My_Portfolio readiness check (pdSrcReadiness_: SUCCESS,
 *      run=N, <= 8 h, acquired 100%, data COMPLETE ...) and run N has not
 *      been refreshed by the chain yet.
 *   2. Top_10_Investments when 'TFB Feed Global_Markets' carries run=N AND
 *      'TFB Decision Feed' carries run=N plus an exact finalizer/release
 *      witness no older than either market feed, and N is new. A runner
 *      composite alone does not prove the final audit has finished.
 *   Skips (no claim): sync in flight (16_Decision_Top10 dt10SyncInflightCore_),
 *   a cockpit run in progress (DT10_RUN_MARKER younger than 7 min), another
 *   chain claim younger than 7 min, the coordinator's manual-priority or
 *   backend-hold guard (11_Manual_Refresh_Coordinator
 *   tfbAutomaticRefreshAllowed_), and -- for Top_10 only -- the quiet windows
 *   around the S-1 board dispatch slots (TFB_DECISION_CHAIN_QUIET, default
 *   07:55-08:25,16:55-17:25 Riyadh) so a board never reads a half-rendered
 *   cockpit.
 *   observe: logs one _Run_Log row per (action, run) it WOULD perform.
 *   on: claims the cohort under a short ScriptLock transaction, runs the native
 *   function OUTSIDE that transaction, then owner-checks the outcome under a
 *   second short transaction. Native work uses coordinator11's existing
 *   tfbRunAutomaticRefresh_ writer lock, separately from these transactions.
 *   This is not a global lease against external sync writers. A native
 *   operational throw is retried on later ticks, at most
 *   TFB_DC_MAX_ATTEMPTS times per (action, run).
 *   A proven no-work deferral refunds its owned attempt; no immediate retry.
 *   Unverifiable completion keeps its attempt and requires reconciliation if
 *   the budget is exhausted; an unknown commit is never blindly re-executed.
 * UNTOUCHED: refreshPortfolioDecision, refreshDecisionTop10, every gate,
 * receipt, verdict and release path. The release (release_decision_feed.yml)
 * keeps its existing scheduled/manual workflow. No HTTP call is made by this file. ES5 only.
 * Kill: TFB_DECISION_CHAIN=off (or tfbRemoveDecisionChain()).
 * ============================================================================
 */

var TFB_DC_VERSION = '1.0.3';
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
var TFB_DC_STATE_MAX_BYTES = 8000;                    // below Script Properties 9KB limit
var TFB_DC_ATTEMPT_KEYS_MAX = 96;

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
  // Same widest token boundaries as dt10FeedCohortCheck_: a second RUN =
  // token must not be hidden by the native stamp parser's narrower spelling.
  var ws = '\\t\\n\\v\\f\\r\\x1c-\\x20\\x85\\xa0\\u1680\\u2000-\\u200a' +
      '\\u2028\\u2029\\u202f\\u205f\\u3000';
  var re = new RegExp('(^|[^a-z0-9_\\u0130\\u0131\\u017f\\u212a])run[' + ws +
      ']*=[' + ws + ']*([^' + ws + ';|,]*)', 'gi');
  var first = re.exec(s);
  return first && !re.exec(s) && /^[1-9]\d{0,31}$/.test(first[2]) ? first[2] : '';
}

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


/** Parse bounded persisted state. Nonempty invalid state is unknown, never reset. */
function tfbDcEpoch_(v) {
  return typeof v === 'number' && isFinite(v) && v > 0 &&
      Math.floor(v) === v && v <= 8640000000000000;
}
function tfbDcTextFits_(text) {
  try { return encodeURIComponent(text).replace(/%[0-9A-F]{2}/g, 'x').length <= TFB_DC_STATE_MAX_BYTES; }
  catch (e) { return false; }
}
function tfbDcStateJson_(state) {
  try {
    var text = JSON.stringify(state);
    return tfbDcTextFits_(text) && tfbDcStateOf_(text) ? text : null;
  } catch (e) { return null; }
}
function tfbDcStateOf_(raw) {
  var st = {pd: '', t10: '', t10rel: '', claim: null, attempts: {}, observed: {}};
  function object(v) { return !!v && Object.prototype.toString.call(v) === '[object Object]'; }
  function run(v) { return typeof v === 'string' && /^[1-9]\d{0,31}$/.test(v); }
  function has(o, k) { return Object.prototype.hasOwnProperty.call(o, k); }
  function issued(v) {
    return typeof v === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+03:00$/.test(v) &&
        tfbDcEpoch_(Date.parse(v)) && new Date(Date.parse(v) + 3 * 3600000).toISOString().slice(0, 19) === v.slice(0, 19);
  }
  if (raw === null || raw === undefined || raw === '') return st;
  if (!tfbDcTextFits_(String(raw))) return null;
  try {
    var o = JSON.parse(String(raw));
    if (!object(o)) return null;
    for (var field in o) if (has(o, field) &&
        !/^(?:pd|t10|t10rel|pd_ms|t10_ms|claim|attempts|observed)$/.test(field)) return null;
    if (has(o, 't10rel')) {
      if (o.t10rel !== '' && !issued(o.t10rel)) return null;
      st.t10rel = o.t10rel;
    }
    var names = ['pd', 't10'];
    for (var i = 0; i < names.length; i++) {
      var n = names[i];
      if (has(o, n) && o[n] !== '' && !run(o[n])) return null;
      if (has(o, n)) st[n] = o[n];
      if (has(o, n + '_ms')) {
        if (!st[n] || !tfbDcEpoch_(o[n + '_ms'])) return null;
        st[n + '_ms'] = o[n + '_ms'];
      }
    }
    if (has(o, 'attempts')) {
      if (!object(o.attempts)) return null;
      var attemptKeys = 0;
      for (var k in o.attempts) if (has(o.attempts, k)) {
        var count = o.attempts[k];
        var key = /^(?:pd|t10):[1-9]\d{0,31}(?::([1-9]\d{0,15}))?$/.exec(k);
        var legacyRelease = k.indexOf('t10rel:') === 0 && issued(k.slice(7));
        if (++attemptKeys > TFB_DC_ATTEMPT_KEYS_MAX || (!key && !legacyRelease) ||
            (key && key[1] && !tfbDcEpoch_(Number(key[1]))) || typeof count !== 'number' ||
            !isFinite(count) || count < 0 || Math.floor(count) !== count ||
            count > TFB_DC_MAX_ATTEMPTS) return null;
        st.attempts[k] = count;
      }
    }
    if (has(o, 'observed')) {
      if (!object(o.observed)) return null;
      for (var j in o.observed) if (has(o.observed, j)) {
        if (j === 't10rel') {
          if (o.observed[j] !== '' && !issued(o.observed[j])) return null;
        } else if (j === 'pd_ms' || j === 't10_ms') {
          if (!run(o.observed[j.slice(0, -3)]) || !tfbDcEpoch_(o.observed[j])) return null;
        } else if ((j !== 'pd' && j !== 't10') || (o.observed[j] !== '' && !run(o.observed[j]))) return null;
        st.observed[j] = o.observed[j];
      }
    }
    if (has(o, 'claim') && o.claim !== null) {
      var c = o.claim;
      if (!object(c) || (c.act !== 'pd' && c.act !== 't10') || !run(c.run) ||
          typeof c.at_ms !== 'number' || !isFinite(c.at_ms) || c.at_ms < 0 ||
          Math.floor(c.at_ms) !== c.at_ms || c.at_ms > 8640000000000000 ||
          (has(c, 'identity_ms') && !tfbDcEpoch_(c.identity_ms)) ||
          (has(c, 'owner') && (typeof c.owner !== 'string' || !c.owner || c.owner.length > 128))) return null;
      st.claim = c; // An expired unfinalized claim still requires reconciliation.
    }
  } catch (e) { return null; }
  return st;
}

/** PURE plan. s = {mode, nowMs, state, pd: {available, ready, cohort, reason},
 * t10: {available, gmRun, decisionRun}, inflight, cockpitRunning, quiet,
 * automationAllowed}. -> {action: 'none'|'pd'|'t10', run, reason}. */
function tfbDcPlanCore_(s) {
  function none(reason, deferred, unverified) {
    return {action: 'none', run: '', reason: reason, deferred: deferred === true,
            unverified: unverified === true};
  }
  if (!s || s.mode === 'off') return none('off');
  var st = s.state || {pd: '', t10: '', claim: null, attempts: {}};
  var c = st.claim;
  if (c && typeof c.at_ms === 'number' &&
      s.nowMs - c.at_ms < TFB_DC_CLAIM_TTL_MIN * 60000 && s.mode === 'on') {
    return none('claimed ' + c.act + ' run ' + c.run, true);
  }
  if (c && s.mode === 'on') {
    return none('unfinalized claim expired; manual reconciliation required', false, true);
  }
  if (s.inputsKnown !== true) return none(s.inputReason || 'source status unavailable', true);
  if (s.automationAllowed !== true) return none(s.automationAllowed === false ?
      'automation paused (manual priority or backend hold)' : 'automation guard not evaluated (diagnostic)', true);
  if (s.writerAvailable !== true) return none('automatic refresh coordinator unavailable', true);
  if (s.inflight !== false) return none(s.inflight ? 'sync in flight' : 'sync state unknown', true);
  var att = st.attempts || {};
  function budget(action, run, identity) {
    return (att[action + ':' + run] || 0) + (att[action + ':' + run + ':' + identity] || 0);
  }
  var pd = s.pd || {};
  if (pd.available && pd.ready && /^[1-9]\d*$/.test(String(pd.cohort || ''))) {
    if (!tfbDcEpoch_(pd.stamp_ms)) return none('portfolio publication identity unavailable', true);
    if (String(pd.cohort) === st.pd && !tfbDcEpoch_(st.pd_ms)) {
      return none('legacy portfolio completion identity unknown; manual reconciliation required', true);
    }
    if (String(pd.cohort) === st.pd && pd.stamp_ms < st.pd_ms) {
      return none('portfolio publication identity moved backward; manual reconciliation required', true);
    }
    if ((String(pd.cohort) !== st.pd || pd.stamp_ms !== st.pd_ms) &&
        budget('pd', pd.cohort, pd.stamp_ms) < TFB_DC_MAX_ATTEMPTS) {
      return {action: 'pd', run: String(pd.cohort), identity_ms: pd.stamp_ms,
            reason: 'My_Portfolio run ' + pd.cohort + ' published and ready'};
    }
  }
  var t = s.t10 || {};
  var run = String(t.gmRun || '');
  var issued = String(t.releaseRun || '') === run ? String(t.releaseIssued || '') : '';
  if (!t.available) return none('nothing new (Top_10 refresh unavailable)');
  if (!/^[1-9]\d*$/.test(run)) return none('Global_Markets feed run unreadable');
  if (String(t.decisionRun || '') !== run) {
    return none('run ' + run + ' final audit not finished');
  }
  if (typeof t.finalized_ms !== 'number' || !isFinite(t.finalized_ms)) {
    return none('final publication witness unavailable or older than source', true);
  }
  if (run === st.t10 && !tfbDcEpoch_(st.t10_ms)) {
    return none('legacy Top_10 completion identity unknown; manual reconciliation required', true);
  }
  if (run === st.t10 && t.finalized_ms < st.t10_ms) {
    return none('Top_10 publication identity moved backward; manual reconciliation required', true);
  }
  if (run === st.t10 && t.finalized_ms === st.t10_ms && (!issued || issued === st.t10rel)) {
    return none('Top_10 already refreshed for run ' + run);
  }
  if (budget('t10', run, t.finalized_ms) + (issued ? att['t10rel:' + issued] || 0 : 0) >= TFB_DC_MAX_ATTEMPTS) {
    return none('Top_10 run ' + run + ' gave up after ' + TFB_DC_MAX_ATTEMPTS + ' attempts');
  }
  if (s.cockpitRunning !== false) return none('cockpit refresh in progress or unknown', true);
  if (s.quiet) return none('quiet window (S-1 board slot)', true);
  return {action: 't10', run: run, finalized_ms: t.finalized_ms, identity_ms: t.finalized_ms,
          release: issued, reason: issued && run === st.t10 ?
              'release issued ' + issued + ' for run ' + run : 'run ' + run + ' final audit done'};
}

/** A typed completed result still needs independent B2 readback. Native empty
 * and disabled skeletons are valid rendered outcomes, never executable proof. */
function tfbDcReceiptCore_(before, after, run, startedMs, nowMs, parseStamp) {
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
  var status = after.match(/^Last run (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s*\|\s*status:\s*(?:ok|no_candidates|disabled)(?=\s*(?:\||$))/i);
  var stamp = status && typeof parseStamp === 'function'
      ? parseStamp(status[1], startedMs, nowMs) : null;
  if (typeof startedMs !== 'number' || !isFinite(startedMs) ||
      typeof nowMs !== 'number' || !isFinite(nowMs) || nowMs < startedMs ||
      typeof stamp !== 'number' || !isFinite(stamp) ||
      stamp < Math.floor(startedMs / 1000) * 1000 || stamp > nowMs) {
    return fail('native render timestamp is outside this attempt');
  }
  return {ok: true, reason: ''};
}


/* -------------------------------------------------------------------- I/O --- */

function tfbDcProps_() { return PropertiesService.getScriptProperties(); }

function tfbDcMode_() {
  try { return tfbDcModeOf_(tfbDcProps_().getProperty(TFB_DC_MODE_PROP)); }
  catch (e) { return 'off'; }
}

/** Compatibility reader retained for existing callers. Planning uses the
 * ambiguity-rejecting tfbDcStatusFeeds_ below. */
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


/** Exact existing producer grammar proves final work, never promotes a feed.
 * Healthy final audits currently emit no marker; that case needs an explicit
 * final witness before this automation can acknowledge Top10 for the cohort.
 * A release's issued clock is its operation time; its anchor is intentionally
 * the oldest receipt and is not evidence of when the final release happened. */
function tfbDcFinalWitness_(raw, run, mlMs, gmMs, nowMs) {
  function clock(s) {
    var iso = s.replace(' ', 'T'), ms = Date.parse(iso);
    // Date.parse normalizes impossible dates; no actual producer emits those.
    return isFinite(ms) && new Date(ms + 3 * 3600000).toISOString().slice(0, 19) ===
        iso.slice(0, 19) ? ms : NaN;
  }
  var text = String(raw || ''), stamp = '\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\+03:00';
  var final = new RegExp('^NOT_ACTIONABLE\\(final_publication\\) \\| run=' + run +
      ' \\| (' + stamp + ') \\| ([a-z_]+(?:,[a-z_]+)*)$').exec(text);
  var witnessMs = NaN;
  if (final) {
    if (!/^(?:(?:coverage_failed|coverage_unavailable)(?:,(?:decision_failed|decision_unavailable))?|decision_failed|decision_unavailable|approved_scope_unavailable,coverage_unavailable,decision_unavailable)$/.test(final[2])) return null;
    witnessMs = clock(final[1]);
  } else {
    var t = '\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\+03:00';
    var release = new RegExp('^EXECUTABLE \\| run=' + run + ' \\| ' + stamp +
        ' \\| issued=(' + t + ') \\| expires=' + t +
        ' \\| ttl_min=480 \\| release=[1-9]\\d* \\| attempt=[1-9]\\d*' +
        ' \\| receipts=T10:' + run + ',PF:' + run + ' \\| t10_at=' + t +
        ' \\| pf_at=' + t + ' \\| mp_at=' + t + ' \\| audits=0,0$').exec(text);
    if (!release) return null;
    var clocks = text.match(/\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}\+03:00/g) || [];
    for (var i = 0; i < clocks.length; i++) if (!isFinite(clock(clocks[i]))) return null;
    witnessMs = clock(release[1]);
  }
  return isFinite(witnessMs) && witnessMs >= mlMs && witnessMs >= gmMs &&
      witnessMs <= nowMs ? witnessMs : null;
}

/** Exactly one readable value per required feed. No first-match ambiguity. */
function tfbDcStatusFeeds_(ss) {
  try {
    var sh = ss.getSheetByName('_Status');
    if (!sh) throw new Error('missing status');
    var vals = sh.getRange('L1:M60').getValues();
    var keys = [TFB_DC_ML_FEED_KEY, TFB_DC_GM_FEED_KEY, TFB_DC_DECISION_FEED_KEY];
    var counts = {}, values = {}, parsedFeeds = {};
    if (!Array.isArray(vals)) throw new Error('status grid invalid');
    for (var i = 0; i < vals.length; i++) {
      var row = vals[i];
      if (!Array.isArray(row)) throw new Error('status row invalid');
      var name = String(row[0] || '').replace(/^\s+|\s+$/g, '').toLowerCase();
      for (var k = 0; k < keys.length; k++) {
        if (name !== keys[k].toLowerCase()) continue;
        counts[keys[k]] = (counts[keys[k]] || 0) + 1;
        if (typeof row[1] !== 'string' || !row[1].trim()) throw new Error('status value invalid');
        values[keys[k]] = row[1];
      }
    }
    for (var j = 0; j < keys.length; j++) {
      if (counts[keys[j]] !== 1 || !tfbDcRunOf_(values[keys[j]])) throw new Error('status cohort ambiguous');
      if (typeof dt10FeedStampParse_ !== 'function') throw new Error('status parser unavailable');
      var parsed = dt10FeedStampParse_(values[keys[j]]);
      if (!parsed || !parsed.state || typeof parsed.ms !== 'number' || !isFinite(parsed.ms) ||
          parsed.run !== tfbDcRunOf_(values[keys[j]])) throw new Error('status clock unavailable');
      var terminal = keys[j] === TFB_DC_DECISION_FEED_KEY ?
          /^(?:EXECUTABLE|NOT_ACTIONABLE(?:\(.*\))?)$/.test(parsed.state) :
          /^(?:OK|PARTIAL|PARTIAL_FRESH|STALE_COV|FAILED|SKIPPED|POLICY_ERROR)$/.test(parsed.state);
      if (!terminal) throw new Error('status not terminal');
      if (keys[j] !== TFB_DC_DECISION_FEED_KEY) {
        var parts = values[keys[j]].split('|'), clocks = [], clockTokens =
            values[keys[j]].match(/\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}/g) || [];
        for (var p = 0; p < parts.length; p++) {
          var part = parts[p].replace(/^\s+|\s+$/g, '');
          if (/^\d{4}-\d{2}-\d{2}/.test(part)) clocks.push(part);
        }
        if (clocks.length !== 1 || clockTokens.length !== 1 ||
            typeof dt10SourceStampMs_ !== 'function') throw new Error('source clock ambiguous');
        var strictMs = dt10SourceStampMs_(clocks[0]);
        if (typeof strictMs !== 'number' || !isFinite(strictMs) || strictMs !== parsed.ms) {
          throw new Error('source clock invalid');
        }
      }
      parsedFeeds[keys[j]] = parsed;
    }
    return {known: true, values: values, reason: '', finalized_ms: tfbDcFinalWitness_(
      values[TFB_DC_DECISION_FEED_KEY], tfbDcRunOf_(values[TFB_DC_DECISION_FEED_KEY]),
      parsedFeeds[TFB_DC_ML_FEED_KEY].ms, parsedFeeds[TFB_DC_GM_FEED_KEY].ms, Date.now())};
  } catch (e) {
    return {known: false, values: {}, reason: 'source status missing, unreadable or ambiguous'};
  }
}

function tfbDcReadReceipt_(ss, action) {
  try {
    var name = action === 'pd' ? 'Portfolio_Decision' : 'Top_10_Investments';
    var sh = ss.getSheetByName(name);
    if (!sh) return null;
    var value = sh.getRange(2, 2).getValue();
    return typeof value === 'string' ? value : null;
  } catch (e) { return null; }
}


function tfbDcScriptStampMs_(text, startedMs, nowMs) {
  if (typeof startedMs !== 'number' || !isFinite(startedMs) ||
      typeof nowMs !== 'number' || !isFinite(nowMs) || nowMs < startedMs) return null;
  try {
    var zone = Session.getScriptTimeZone();
    if (typeof zone !== 'string' || !zone || typeof pdSrcStampMs_ !== 'function') return null;
    var clocks = [startedMs, nowMs];
    for (var i = 0; i < clocks.length; i++) {
      var offset = Utilities.formatDate(new Date(clocks[i]), zone, 'Z');
      if (!/^[+-]\d{4}$/.test(offset)) continue;
      var stamp = pdSrcStampMs_(text + offset);
      if (stamp !== null && stamp >= Math.floor(startedMs / 1000) * 1000 && stamp <= nowMs &&
          Utilities.formatDate(new Date(stamp), zone, 'yyyy-MM-dd HH:mm:ss') === text) {
        return stamp;
      }
    }
  } catch (e) {}
  return null;
}


/** Everything the plan needs, read once. Never throws. */
function tfbDcReadInputs_(ss, mode, state, nowMs, readOnly) {
  var s = {mode: mode, nowMs: nowMs, state: state,
           pd: {available: typeof refreshPortfolioDecision === 'function' &&
                           typeof pdSrcReadiness_ === 'function',
                ready: false, cohort: '', reason: ''},
           t10: {available: typeof refreshDecisionTop10 === 'function',
                 gmRun: '', decisionRun: '', finalized_ms: null, releaseRun: '', releaseIssued: ''},
           inputsKnown: false, inputReason: 'source status unavailable',
           inflight: null, cockpitRunning: null, quiet: true,
           writerAvailable: typeof tfbRunAutomaticRefresh_ === 'function',
           automationAllowed: readOnly === true ? null : true};
  try {
    if (s.pd.available) {
      var src = pdSrcReadiness_(ss, null, null);
      s.pd.ready = !!src.ready; s.pd.cohort = String(src.cohort || '');
      s.pd.stamp_ms = src.stamp_ms;
      s.pd.reason = String(src.reason || '');
    }
  } catch (ePd) { s.pd.ready = false; s.pd.reason = 'readiness unavailable'; }
  try {
    var feeds = tfbDcStatusFeeds_(ss);
    if (!feeds.known) { s.inputReason = feeds.reason; return s; }
    var gm = feeds.values[TFB_DC_GM_FEED_KEY];
    var ml = feeds.values[TFB_DC_ML_FEED_KEY];
    s.t10.gmRun = tfbDcRunOf_(gm);
    s.t10.decisionRun = tfbDcRunOf_(feeds.values[TFB_DC_DECISION_FEED_KEY]);
    s.t10.finalized_ms = feeds.finalized_ms;
    var rel = tfbDcReleaseOf_(feeds.values[TFB_DC_DECISION_FEED_KEY]);
    s.t10.releaseRun = rel.run;
    s.t10.releaseIssued = rel.issued;
    if (typeof dt10SyncInflightCore_ === 'function') {
      var flight = dt10SyncInflightCore_(ml, gm, nowMs, TFB_DC_INFLIGHT_MAX_MIN);
      if (flight && typeof flight.active === 'boolean' &&
          flight.detail !== 'error' && flight.detail !== 'stamps unreadable') {
        s.inflight = flight.active;
        s.inputsKnown = true;
        s.inputReason = '';
      }
    }
  } catch (eSt) { s.inputReason = 'source state unavailable'; }
  try {
    var marker = tfbDcProps_().getProperty(TFB_DC_COCKPIT_MARKER_PROP) || '';
    if (!marker) s.cockpitRunning = false;
    else if (typeof dt10RunMarkerCore_ === 'function') {
      var j = dt10RunMarkerCore_(marker, nowMs, TFB_DC_CLAIM_TTL_MIN);
      if (j && typeof j.ageMin === 'number' && isFinite(j.ageMin)) s.cockpitRunning = !j.aborted;
    }
  } catch (eMk) {}
  try {
    s.quiet = tfbDcInQuiet_(tfbDcQuietWindows_(
        tfbDcProps_().getProperty(TFB_DC_QUIET_PROP)), nowMs);
  } catch (eQ) {}
  try {
    if (readOnly !== true && typeof tfbAutomaticRefreshAllowed_ === 'function') {
      s.automationAllowed = !!tfbAutomaticRefreshAllowed_(TFB_DC_HANDLER);
    }
  } catch (eA) { s.automationAllowed = false; }
  return s;
}

/** PURE native completion contract; financial output labels are not eligibility. */
function tfbDcCompletionOf_(result, plan) {
  function unverified(reason) { return {state: 'unverified', reason: reason}; }
  if (!result || Object.prototype.toString.call(result) !== '[object Object]' ||
      result.contract !== 'tfb_native_refresh_v1' ||
      result.page !== (plan.action === 'pd' ? 'Portfolio_Decision' : 'Top_10_Investments') ||
      (result.state !== 'completed' && result.state !== 'deferred')) {
    return unverified('completion receipt unsupported');
  }
  if (result.state === 'deferred') {
    if (plan.action === 't10' && result.source_run === '' && result.output_state === 'HELD' &&
        (result.reason === 'sync_inflight' || result.reason === 'sheets_pool_required')) {
      return {state: 'deferred', reason: 'native refresh deferred before publication'};
    }
    return unverified('native deferral not verified before publication');
  }
  if (typeof result.source_run !== 'string' || !/^[1-9]\d*$/.test(result.source_run)) {
    return unverified('consumed cohort unverified');
  }
  if (result.source_run !== plan.run) return unverified('consumed cohort changed');
  if (plan.action === 'pd' && (!tfbDcEpoch_(result.source_stamp_ms) ||
      result.source_stamp_ms !== plan.identity_ms)) {
    return unverified('consumed portfolio publication changed or unverified');
  }
  if (plan.action === 't10' && (!tfbDcEpoch_(result.source_latest_ms) ||
      typeof plan.finalized_ms !== 'number' || !isFinite(plan.finalized_ms) ||
      plan.finalized_ms < result.source_latest_ms)) {
    return unverified('final publication witness predates or cannot bind consumed source');
  }
  if (typeof result.output_state !== 'string' || !result.output_state.trim()) {
    return unverified('completion output unverified');
  }
  return {state: 'completed', reason: 'consumed cohort verified'};
}

/** PURE: a stale finisher never clears or updates a replacement owner's claim. */
function tfbDcOwnsClaim_(claim, expected) {
  return !!claim && !!expected && typeof expected.owner === 'string' && !!expected.owner &&
      claim.owner === expected.owner && claim.act === expected.act &&
      claim.run === expected.run && claim.at_ms === expected.at_ms &&
      claim.identity_ms === expected.identity_ms;
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

function tfbDcNotice_(status, action, run, reason, ms) {
  tfbDcLog_('WARN', action === 'pd' ? 'Portfolio_Decision' : 'Top_10_Investments',
      status, '[DECISION-CHAIN v' + TFB_DC_VERSION + '] ' +
      (action || 'plan') + (run ? ' run ' + run : '') + ' ' + status.toLowerCase() + ': ' + reason,
      ms === undefined ? null : ms,
      {version: TFB_DC_VERSION, act: action || '', run: run || '', reason: reason});
  return status.toLowerCase() + ' ' + (action || 'plan') + (run ? ' run ' + run : '') + ': ' + reason;
}
function tfbDcDeferred_(action, run, reason, ms) { return tfbDcNotice_('DEFERRED', action, run, reason, ms); }
function tfbDcUnverified_(action, run, reason, ms) { return tfbDcNotice_('UNVERIFIED', action, run, reason, ms); }

/** Time-driven (tfbInstallDecisionChain). Returns a short outcome string. */
function tfbDecisionChainTick() {
  var mode = tfbDcMode_();
  if (mode === 'off') return 'off';
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var planned;
  try { planned = tfbDcWithLock_(function() {
    var props = tfbDcProps_();
    var state = tfbDcStateOf_(props.getProperty(TFB_DC_STATE_PROP));
    if (!state) return {plan: {action: 'none', run: '', reason: 'persisted state invalid', deferred: true}};
    var nowMs = Date.now();
    /* observe walks both steps: what it already reported counts as done */
    var view = mode === 'observe'
        ? {pd: state.observed.pd || state.pd, t10: state.observed.t10 || state.t10,
           pd_ms: state.observed.pd ? state.observed.pd_ms : state.pd_ms,
           t10_ms: state.observed.t10 ? state.observed.t10_ms : state.t10_ms,
           t10rel: state.observed.t10 ? state.observed.t10rel || '' : state.t10rel,
           claim: null, attempts: {}, observed: state.observed}
        : state;
    var plan = tfbDcPlanCore_(tfbDcReadInputs_(ss, mode, view, nowMs));
    if (plan.action === 'none') return {plan: plan};
    var key = plan.action + ':' + plan.run + ':' + plan.identity_ms;
    if (mode === 'observe') {
      if (state.observed[plan.action] !== plan.run || state.observed[plan.action + '_ms'] !== plan.identity_ms ||
          (plan.action === 't10' && String(state.observed.t10rel || '') !== String(plan.release || ''))) {
        state.observed[plan.action] = plan.run;
        state.observed[plan.action + '_ms'] = plan.identity_ms;
        if (plan.action === 't10') state.observed.t10rel = String(plan.release || '');
        var observedJson = tfbDcStateJson_(state);
        if (!observedJson) return {plan: {action: 'none', run: '', reason: 'persisted state bound reached', deferred: true}};
        props.setProperty(TFB_DC_STATE_PROP, observedJson);
        tfbDcLog_('INFO', plan.action === 'pd' ? 'Portfolio_Decision' : 'Top_10_Investments',
                  'OBSERVE', '[DECISION-CHAIN v' + TFB_DC_VERSION + '] would refresh ' +
                  plan.action + ' for run ' + plan.run + ' (' + plan.reason + ')', null,
                  {version: TFB_DC_VERSION, mode: mode, act: plan.action, run: plan.run});
      }
      return {plan: plan, observed: true};
    }
    var owner;
    try { owner = Utilities.getUuid(); } catch (eId) { owner = ''; }
    if (typeof owner !== 'string' || !owner || owner.length > 128) {
      return {plan: {action: 'none', run: '', reason: 'claim owner unavailable', deferred: true}};
    }
    state.claim = {owner: owner, act: plan.action, run: plan.run, at_ms: nowMs, identity_ms: plan.identity_ms};
    state.attempts[key] = (state.attempts[key] || 0) + 1;
    var claimJson = tfbDcStateJson_(state);
    if (!claimJson) return {plan: {action: 'none', run: '', reason: 'persisted state bound reached', deferred: true}};
    props.setProperty(TFB_DC_STATE_PROP, claimJson);
    return {plan: plan, key: key, claim: state.claim};
  }); } catch (ePlan) {
    return tfbDcDeferred_('', '', 'planning state unavailable');
  }
  if (planned.busy) return tfbDcDeferred_('', '', 'planning lock busy');
  var p = planned.value;
  if (!p.key) {
    if (p.plan.unverified) return tfbDcUnverified_('', '', p.plan.reason);
    if (p.plan.deferred) return tfbDcDeferred_('', '', p.plan.reason);
    return p.observed ? 'observed ' + p.plan.action + ' run ' + p.plan.run : p.plan.reason;
  }
  var t0 = Date.now(), completion = {state: 'failed', reason: 'native refresh threw'};
  var previous = tfbDcReadReceipt_(ss, p.plan.action);
  try {
    if (typeof tfbRunAutomaticRefresh_ !== 'function') {
      completion = {state: 'deferred', reason: 'automatic refresh coordinator unavailable'};
    } else {
      var result = tfbRunAutomaticRefresh_(function() {
        return p.plan.action === 'pd' ? refreshPortfolioDecision({completionContract: true}) :
            refreshDecisionTop10({completionContract: true});
      }, TFB_DC_HANDLER);
      completion = result && (result.status === 'skipped_manual_priority' || result.status === 'skipped_lock_busy') ?
          {state: 'deferred', reason: 'automatic refresh coordinator deferred'} :
          tfbDcCompletionOf_(result, p.plan);
      if (completion.state === 'completed') {
        SpreadsheetApp.flush();
        var readback = tfbDcReceiptCore_(previous, tfbDcReadReceipt_(ss, p.plan.action),
            p.plan.run, t0, Date.now(), tfbDcScriptStampMs_);
        if (!readback.ok) completion = {state: 'unverified', reason: readback.reason};
      }
    }
  } catch (eRun) { /* Failed native work retains its owned attempt; never expose payload text. */ }
  var ms = Date.now() - t0;
  var finished;
  try { finished = tfbDcWithLock_(function() {
    var props = tfbDcProps_();
    var state = tfbDcStateOf_(props.getProperty(TFB_DC_STATE_PROP));
    if (!state) throw new Error('persisted state invalid');
    if (!tfbDcOwnsClaim_(state.claim, p.claim)) return false;
    if (completion.state === 'completed') {
      state[p.plan.action] = p.plan.run;
      state[p.plan.action + '_ms'] = p.plan.identity_ms;
      if (p.plan.action === 't10') {
        state.t10rel = String(p.plan.release || '');
        if (p.plan.release) delete state.attempts['t10rel:' + p.plan.release];
      }
      delete state.attempts[p.key];
    } else if (completion.state === 'deferred') {
      var attempts = Math.max(0, (state.attempts[p.key] || 1) - 1);
      if (attempts) state.attempts[p.key] = attempts;
      else delete state.attempts[p.key];
    }
    state.claim = null;
    var finalJson = tfbDcStateJson_(state);
    if (!finalJson) throw new Error('persisted state bound reached');
    props.setProperty(TFB_DC_STATE_PROP, finalJson);
    return true;
  }); } catch (eFinish) {
    return tfbDcUnverified_(p.plan.action, p.plan.run,
        'completion commit unverified; manual reconciliation required', ms);
  }
  if (finished.busy) return tfbDcUnverified_(p.plan.action, p.plan.run,
      'completion commit unverified (lock busy); manual reconciliation required', ms);
  if (!finished.value) return tfbDcUnverified_(p.plan.action, p.plan.run,
      'claim ownership changed; manual reconciliation required', ms);
  if (completion.state === 'deferred') {
    return tfbDcDeferred_(p.plan.action, p.plan.run, completion.reason, ms);
  }
  if (completion.state === 'unverified') {
    return tfbDcUnverified_(p.plan.action, p.plan.run, completion.reason, ms);
  }
  var ok = completion.state === 'completed';
  tfbDcLog_(ok ? 'INFO' : 'ERROR',
            p.plan.action === 'pd' ? 'Portfolio_Decision' : 'Top_10_Investments',
            ok ? 'OK' : 'FAILED',
            '[DECISION-CHAIN v' + TFB_DC_VERSION + '] ' + p.plan.action + ' run ' +
            p.plan.run + ' -> ' + completion.reason + ' (' + p.plan.reason + ')', ms,
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

/** Operator: read-only status/source preview. The effectful automation guard is
 * not evaluated, so an executable next-tick plan remains unverified here. */
function tfbDecisionChainStatus() {
  var mode = tfbDcMode_();
  var state = tfbDcStateOf_(tfbDcProps_().getProperty(TFB_DC_STATE_PROP));
  var plan = state ? tfbDcPlanCore_(tfbDcReadInputs_(SpreadsheetApp.getActiveSpreadsheet(),
      mode === 'off' ? 'observe' : mode, state, Date.now(), true)) :
      {action: 'none', run: '', reason: 'persisted state invalid'};
  state = state || {};
  var msg = '[DECISION-CHAIN v' + TFB_DC_VERSION + '] mode=' + mode +
      ' last_pd=' + (state.pd || '-') + ' last_t10=' + (state.t10 || '-') +
      ' last_release_render=' + (state.t10rel || '-') +
      ' next=' + plan.action + (plan.run ? ' run ' + plan.run : '') + ' (' + plan.reason + ')';
  Logger.log(msg);
  return msg;
}
