/* 26_Decision_Chain v1.0.0: refresh Portfolio_Decision and Top_10 when the
 * sync's data lands. Runs the REAL 10_My_Portfolio, 11_Manual_Refresh_
 * Coordinator, 16_Decision_Top10 and 26_Decision_Chain sources in one vm (the
 * shared Apps Script namespace) with service stubs; only the two refresh
 * entry points are replaced by spies. All runs, stamps and symbols are synthetic.
 *
 *   node tests/js/decision_chain.test.js
 */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const DIR = path.join(__dirname, '../../apps_script');
const FILES = ['10_My_Portfolio.gs', '11_Manual_Refresh_Coordinator.gs',
               '16_Decision_Top10.gs', '26_Decision_Chain.gs'];
const RUN = '7777777771';
const OLD = '7777777770';
const NEXT = '7777777772';
let passed = 0;
function check(label, fn) { fn(); passed++; console.log('PASS ' + label); }

const riyadh = (ms) => new Date(ms + 3 * 3600000).toISOString().slice(0, 19).replace('T', ' ') + '+03:00';
const STATUS_HEADER = ['Page', 'Last Updated', 'Status', 'Message', 'Endpoint',
  'HTTP Code', 'Rows', 'Columns', 'Duration ms', 'Warnings'];

function myPortfolioRow(stampMs, run, acquired) {
  const acq = acquired || '4/4';
  const msg = '[STATUS-STAMP v6.70.0] leg=success written=4 failed=0 requested=4 fresh=4 ' +
    'acquired=' + acq + ' acquisition=COMPLETE fetchfail_requested=observe fetchfail_effective=observe ' +
    'warnings=0 | data=COMPLETE policy_data=COMPLETE sha=deadbeef run=' + run;
  return ['My_Portfolio', riyadh(stampMs), 'SUCCESS', msg, 'backend:run_dashboard_sync', '', 4, 122, 1000, ''];
}
function feed(state, run, stampMs) { return state + ' | cov=97.0 | run=' + run + ' | ' + riyadh(stampMs); }

/** Build a world: now, _Status rows (A:J), L:M key/values, props. */
function world(o) {
  const now = o.now;
  const props = Object.assign({}, o.props || {});
  const runLog = [];
  const triggers = (o.triggers || []).slice();
  const calls = {pd: 0, t10: 0, sheetReads: 0};
  const grid = [STATUS_HEADER].concat(o.status || []);
  const kv = o.kv || {};
  const status = {
    getLastRow() { return grid.length; }, getLastColumn() { return 13; },
    getRange(a, b, c, d) {
      calls.sheetReads++;
      if (typeof a === 'string') {   // 'L1:M60'
        const rows = Object.keys(kv).map(k => [k, kv[k]]);
        while (rows.length < 60) rows.push(['', '']);
        return {getValues() { return rows; }};
      }
      return {getValues() { return grid.slice(a - 1, a - 1 + c).map(r => r.slice(b - 1, b - 1 + d)); }};
    }};
  const ss = {getSheetByName(name) {
    if (name === '_Status') return status;
    if (name === '_Run_Log') return {appendRow(r) { runLog.push(r); }};
    return null;
  }};
  class FixedDate extends Date {
    constructor(...args) { super(...(args.length ? args : [world.clock.now])); }
    static now() { return world.clock.now; }
  }
  world.clock = {now};
  const scriptProps = {getProperty(k) { return Object.prototype.hasOwnProperty.call(props, k) ? props[k] : null; },
    setProperty(k, v) { props[k] = String(v); }, deleteProperty(k) { delete props[k]; },
    getProperties() { return Object.assign({}, props); }};
  const ctx = {
    Date: FixedDate, Math, JSON, Logger: {log() {}},
    SHEET_NAMES: {MY_PORTFOLIO: 'My_Portfolio'},   // defined by an unmirrored file
    PropertiesService: {getScriptProperties() { return scriptProps; },
                        getDocumentProperties() { return {getProperty() { return null; }}; }},
    LockService: {getScriptLock() { return {tryLock() { return o.lockBusy ? false : true; }, releaseLock() {}}; }},
    SpreadsheetApp: {getActiveSpreadsheet() { return ss; }, getActive() { return ss; }, flush() {},
                     getUi() { throw new Error('no ui'); }},
    ScriptApp: {getProjectTriggers() { return triggers; },
                deleteTrigger(t) { triggers.splice(triggers.indexOf(t), 1); },
                newTrigger(h) { return {timeBased() { return {everyMinutes(n) { return {create() {
                  const t = {getHandlerFunction() { return h; }, every: n}; triggers.push(t); return t; }}; }}; }}; }},
    Utilities: {getUuid() { return 'uuid-synthetic'; }},
    Session: {getScriptTimeZone() { return 'Asia/Riyadh'; }},
  };
  vm.createContext(ctx);
  for (const f of FILES) vm.runInContext(fs.readFileSync(path.join(DIR, f), 'utf8'), ctx, {filename: f});
  ctx.refreshPortfolioDecision = function () { calls.pd++; return o.pdOutcome ? o.pdOutcome(calls.pd) : 'ok'; };
  ctx.refreshDecisionTop10 = function () { calls.t10++; if (o.t10Throws) throw new Error('synthetic cockpit failure'); };
  return {ctx, props, runLog, calls, triggers, setNow(ms) { world.clock.now = ms; }};
}

const T0 = Date.parse('2026-10-16T00:58:00Z');          // 03:58 Riyadh, after the 10-16 final job
const PUB = Date.parse('2026-10-15T23:08:00Z');         // My_Portfolio published 02:08 Riyadh
const GM_AT = Date.parse('2026-10-16T00:53:00Z');
const readyStatus = (run, acquired) => [myPortfolioRow(PUB, run, acquired)];
const readyKv = (gmRun, decisionRun, mlRun) => ({
  'TFB Feed Market_Leaders': feed('OK', mlRun || gmRun, Date.parse('2026-10-15T23:14:00Z')),
  'TFB Feed Global_Markets': feed('PARTIAL_FRESH', gmRun, GM_AT),
  'TFB Decision Feed': 'NOT_ACTIONABLE(final_publication) | run=' + decisionRun + ' | ' + riyadh(GM_AT + 60000) +
    ' | coverage_failed,decision_failed'});

check('C1 off (no property): nothing read, nothing refreshed, nothing logged', () => {
  const w = world({now: T0, status: readyStatus(RUN), kv: readyKv(RUN, RUN)});
  assert.equal(w.ctx.tfbDecisionChainTick(), 'off');
  assert.equal(w.calls.pd + w.calls.t10 + w.calls.sheetReads + w.runLog.length, 0);
  assert.equal(w.props.TFB_DECISION_CHAIN_STATE, undefined);
});

check('C2 observe: one OBSERVE row per step, walks pd then t10, never refreshes', () => {
  const w = world({now: T0, status: readyStatus(RUN), kv: readyKv(RUN, RUN), props: {TFB_DECISION_CHAIN: 'observe'}});
  assert.equal(w.ctx.tfbDecisionChainTick(), 'observed pd run ' + RUN);
  assert.equal(w.ctx.tfbDecisionChainTick(), 'observed t10 run ' + RUN);
  assert.equal(w.ctx.tfbDecisionChainTick(), 'Top_10 already refreshed for run ' + RUN);
  assert.equal(w.calls.pd + w.calls.t10, 0);
  assert.equal(w.runLog.length, 2);
  assert.equal(w.runLog[0][4], 'OBSERVE');
  assert.match(w.runLog[0][5], new RegExp('would refresh pd for run ' + RUN));
  assert.match(w.runLog[1][5], new RegExp('would refresh t10 for run ' + RUN));
  assert.equal(w.runLog[0].length, 10);
});

check('C3 on: Portfolio_Decision first, Top_10 next tick, then idle until a new run', () => {
  const w = world({now: T0, status: readyStatus(RUN), kv: readyKv(RUN, RUN), props: {TFB_DECISION_CHAIN: 'on'}});
  assert.equal(w.ctx.tfbDecisionChainTick(), 'refreshed pd run ' + RUN);
  assert.equal(w.ctx.tfbDecisionChainTick(), 'refreshed t10 run ' + RUN);
  assert.equal(w.ctx.tfbDecisionChainTick(), 'Top_10 already refreshed for run ' + RUN);
  assert.deepEqual([w.calls.pd, w.calls.t10], [1, 1]);
  const st = JSON.parse(w.props.TFB_DECISION_CHAIN_STATE);
  assert.deepEqual([st.pd, st.t10, st.claim], [RUN, RUN, null]);
  assert.deepEqual(w.runLog.map(r => [r[1], r[3], r[4]]),
    [['INFO', 'Portfolio_Decision', 'OK'], ['INFO', 'Top_10_Investments', 'OK']]);
});

check('C4 Top_10 waits for the final audit of the same run', () => {
  const w = world({now: T0, status: readyStatus(OLD), kv: readyKv(RUN, OLD),
                   props: {TFB_DECISION_CHAIN: 'on', TFB_DECISION_CHAIN_STATE: JSON.stringify({pd: OLD})}});
  assert.equal(w.ctx.tfbDecisionChainTick(), 'run ' + RUN + ' final audit not finished');
  assert.equal(w.calls.t10, 0);
});

check('C5 sync in flight (Market_Leaders newer run than Global_Markets): no refresh at all', () => {
  const kv = readyKv(RUN, RUN, NEXT);
  kv['TFB Feed Market_Leaders'] = feed('OK', NEXT, T0 - 2 * 60000);   // newer than GM: run NEXT started
  const w = world({now: T0, status: readyStatus(RUN), kv, props: {TFB_DECISION_CHAIN: 'on'}});
  assert.equal(w.ctx.tfbDecisionChainTick(), 'sync in flight');
  assert.equal(w.calls.pd + w.calls.t10, 0);
});

check('C6 quiet window defers Top_10 only (Portfolio_Decision still runs)', () => {
  const at0810 = Date.parse('2026-10-16T05:10:00Z');     // 08:10 Riyadh
  const w = world({now: at0810, status: [myPortfolioRow(at0810 - 3600000, RUN)], kv: readyKv(RUN, RUN),
                   props: {TFB_DECISION_CHAIN: 'on'}});
  assert.equal(w.ctx.tfbDecisionChainTick(), 'refreshed pd run ' + RUN);
  assert.equal(w.ctx.tfbDecisionChainTick(), 'quiet window (S-1 board slot)');
  w.props.TFB_DECISION_CHAIN_QUIET = 'none';
  assert.equal(w.ctx.tfbDecisionChainTick(), 'refreshed t10 run ' + RUN);
});

check('C7 a cockpit run in progress (DT10_RUN_MARKER 2 min old) defers Top_10', () => {
  const marker = JSON.stringify({started: new Date(T0 - 120000).toISOString(), v: '1.13.9', entry: 'x'});
  const w = world({now: T0, status: readyStatus(OLD), kv: readyKv(RUN, RUN),
                   props: {TFB_DECISION_CHAIN: 'on', DT10_RUN_MARKER: marker,
                           TFB_DECISION_CHAIN_STATE: JSON.stringify({pd: OLD})}});
  assert.equal(w.ctx.tfbDecisionChainTick(), 'cockpit refresh in progress');
  assert.equal(w.calls.t10, 0);
});

check('C8 failed Portfolio_Decision: ERROR row, retried after the claim expires, gives up after 3', () => {
  const w = world({now: T0, status: readyStatus(RUN), kv: readyKv(RUN, RUN), props: {TFB_DECISION_CHAIN: 'on'},
                   pdOutcome: () => 'HTTP 503'});
  assert.equal(w.ctx.tfbDecisionChainTick(), 'failed pd run ' + RUN);
  assert.equal(w.runLog[0][1], 'ERROR');
  assert.match(w.runLog[0][5], /HTTP 503/);
  w.setNow(T0 + 8 * 60000);
  assert.equal(w.ctx.tfbDecisionChainTick(), 'failed pd run ' + RUN);
  w.setNow(T0 + 16 * 60000);
  assert.equal(w.ctx.tfbDecisionChainTick(), 'failed pd run ' + RUN);
  w.setNow(T0 + 24 * 60000);
  assert.equal(w.ctx.tfbDecisionChainTick(), 'refreshed t10 run ' + RUN);   // pd gave up, t10 proceeds
  assert.equal(w.calls.pd, 3);
  assert.equal(JSON.parse(w.props.TFB_DECISION_CHAIN_STATE).pd, '');
});

check('C9 a fresh claim from a concurrent tick blocks; an expired one does not', () => {
  const claim = (ageMin) => JSON.stringify({claim: {act: 'pd', run: RUN, at_ms: T0 - ageMin * 60000}});
  const w = world({now: T0, status: readyStatus(RUN), kv: readyKv(RUN, RUN),
                   props: {TFB_DECISION_CHAIN: 'on', TFB_DECISION_CHAIN_STATE: claim(2)}});
  assert.equal(w.ctx.tfbDecisionChainTick(), 'claimed pd run ' + RUN);
  w.props.TFB_DECISION_CHAIN_STATE = claim(9);
  assert.equal(w.ctx.tfbDecisionChainTick(), 'refreshed pd run ' + RUN);
});

check('C10 manual-refresh priority (11_Manual_Refresh_Coordinator) pauses the chain', () => {
  const w = world({now: T0, status: readyStatus(RUN), kv: readyKv(RUN, RUN),
                   props: {TFB_DECISION_CHAIN: 'on', TFB_MANUAL_REFRESH_UNTIL_MS: String(T0 + 600000),
                           TFB_MANUAL_REFRESH_REQUEST_ID: 'req-1'}});
  assert.equal(w.ctx.tfbDecisionChainTick(), 'automation paused (manual priority or backend hold)');
  assert.equal(w.calls.pd + w.calls.t10, 0);
});

check('C11 an unready My_Portfolio publication (acquired 3/4) skips Portfolio_Decision, Top_10 proceeds', () => {
  const w = world({now: T0, status: readyStatus(RUN, '3/4'), kv: readyKv(RUN, RUN), props: {TFB_DECISION_CHAIN: 'on'}});
  assert.equal(w.ctx.tfbDecisionChainTick(), 'refreshed t10 run ' + RUN);
  assert.equal(w.calls.pd, 0);
});

check('C12 a cockpit exception is logged as FAILED and retried, never thrown', () => {
  const w = world({now: T0, status: readyStatus(OLD), kv: readyKv(RUN, RUN), t10Throws: true,
                   props: {TFB_DECISION_CHAIN: 'on', TFB_DECISION_CHAIN_STATE: JSON.stringify({pd: OLD})}});
  assert.equal(w.ctx.tfbDecisionChainTick(), 'failed t10 run ' + RUN);
  assert.match(w.runLog[0][5], /synthetic cockpit failure/);
});

check('C13 lock busy returns busy without reading', () => {
  const w = world({now: T0, status: readyStatus(RUN), kv: readyKv(RUN, RUN), lockBusy: true,
                   props: {TFB_DECISION_CHAIN: 'on'}});
  assert.equal(w.ctx.tfbDecisionChainTick(), 'busy');
  assert.equal(w.calls.pd + w.calls.t10, 0);
});

check('C14 pure helpers: mode, run token, quiet windows', () => {
  const w = world({now: T0});
  const c = w.ctx;
  assert.deepEqual(['', 'ON', 'observe', 'enforce', null].map(c.tfbDcModeOf_), ['off', 'on', 'observe', 'off', 'off']);
  assert.equal(c.tfbDcRunOf_('OK | run=12 | x'), '12');
  assert.equal(c.tfbDcRunOf_('OK | run=12 | run=13'), '');
  assert.equal(c.tfbDcRunOf_('OK | rerun=12'), '');
  assert.deepEqual(JSON.parse(JSON.stringify(c.tfbDcQuietWindows_(null))), [{from: 475, to: 505}, {from: 1015, to: 1045}]);
  assert.deepEqual(JSON.parse(JSON.stringify(c.tfbDcQuietWindows_('none'))), []);
  assert.deepEqual(JSON.parse(JSON.stringify(c.tfbDcQuietWindows_('23:50-00:10,bad,25:00-26:00'))), [{from: 1430, to: 10}]);
  assert.equal(c.tfbDcInQuiet_(c.tfbDcQuietWindows_('23:50-00:10'), Date.parse('2026-10-16T21:05:00Z')), true);
  assert.equal(c.tfbDcInQuiet_(c.tfbDcQuietWindows_('23:50-00:10'), Date.parse('2026-10-16T21:15:00Z')), false);
});

check('C15 installer replaces its own trigger only; remover leaves others', () => {
  const other = {getHandlerFunction() { return 'pdIbkrCaptureWatch'; }};
  const w = world({now: T0, triggers: [other]});
  w.ctx.tfbInstallDecisionChain();
  w.ctx.tfbInstallDecisionChain();
  assert.equal(w.triggers.filter(t => t.getHandlerFunction() === 'tfbDecisionChainTick').length, 1);
  assert.equal(w.triggers.find(t => t.getHandlerFunction() === 'tfbDecisionChainTick').every, 5);
  w.ctx.tfbRemoveDecisionChain();
  assert.deepEqual(w.triggers.map(t => t.getHandlerFunction()), ['pdIbkrCaptureWatch']);
});

console.log('RESULT ' + passed + '/15 PASS');
assert.equal(passed, 15);
