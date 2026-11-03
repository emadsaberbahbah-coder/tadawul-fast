#!/usr/bin/env node
/* tests/test_my_portfolio_v1518_cash_snapshot.js
 * Harness for apps_script/10_My_Portfolio.gs v1.5.18 [CASH EVIDENCE ROW /
 * LAST-KNOWN CASH / FOLLOW-UP RETRY] (workbook review of 2 Nov 2026): two
 * Script Properties, PD_CASH_SNAPSHOT_WRITE = off | observe | on and
 * PD_IBKR_FOLLOWUP_RETRY = off | on, both default OFF.
 *
 * Dual-tree node harness over the REAL file: the delivered .gs source and
 * the v1.5.17 BASE (argv[3], PD_BASE, or `git show <pinned commit>`; its
 * SHA-256 is pinned) run in separate vm contexts with Apps Script service
 * stubs (SpreadsheetApp with Portfolio_Decision / My_Portfolio / _Status /
 * _Cash_Snapshot / _Run_Log, PropertiesService, DriveApp, ScriptApp,
 * LockService, UrlFetchApp, Utilities, Session, Logger). Every holding,
 * amount, alias, file name and account here is synthetic.
 * Sections:
 *   T0 compile both, version lockstep, base oracle (SHA-256 pinned)
 *   T1 GOLDEN NEGATIVE / kill identity: the property absent / off / junk ->
 *      the REAL refreshPortfolioDecision (B2 line, POST body, every sheet
 *      write, Logger lines, _Run_Log rows, triggers) deep-equals the base;
 *      _Cash_Snapshot is never opened; exactly one property read is added
 *   T2 observe: nothing appended; B2 carries the would-append cells
 *   T3 on: exactly one appendRow with the certification's cells; a second
 *      fresh capture the same Riyadh day -> no second row (property, then
 *      the sheet scan, then an operator-typed row); the base never writes
 *   T4 stale / absent capture: no row; the last-known token; controls, the
 *      packet, the render and the KPI cells are the base's
 *   T5 missing tab, legacy / partial / empty header, cash withheld, two cash
 *      currencies -> nothing written, the reason named, evidence untouched
 *   T6 a Sheets failure inside the writer never breaks the evidence send
 *   T7 the retry gate: off = the base follow-up; on = ONE +3 min trigger
 *      after a cockpit collision, age-bounded, never re-armed by the retry
 *   T8 pdSelfTest: the three new lines; never amounts / alias / file name
 *   T9 ES5 / ASCII lint of the added lines, inventory (20 functions, 0
 *      removed), the WHY block
 *   T10 mutants: each must be killed by the battery
 * The whole battery runs three times and the three digests must match; the
 * last line is 'PASS k/k' (exit 1 on any failure).
 * Usage: node tests/test_my_portfolio_v1518_cash_snapshot.js [new.gs] [base.gs]
 */
'use strict';
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {execFileSync} = require('node:child_process');

const ROOT = path.join(__dirname, '..');
const NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '10_My_Portfolio.gs');
const BASE_SHA = 'd1927aeb25306ad5e8c34621800ee08f6492a8aef35754593793e043b0ea6a57';   // v1.5.17 (origin/main 953f84e2)
const BASE_COMMIT = '953f84e2';
const newSource = fs.readFileSync(NEW_PATH, 'utf8');
const sha256 = (s) => crypto.createHash('sha256').update(s).digest('hex');

function loadBase() {
  const candidates = [];
  if (process.argv[3]) candidates.push(() => fs.readFileSync(process.argv[3], 'utf8'));
  if (process.env.PD_BASE) candidates.push(() => fs.readFileSync(process.env.PD_BASE, 'utf8'));
  for (const ref of [BASE_COMMIT, 'origin/main', 'main']) {
    candidates.push(() => execFileSync('git', ['show', ref + ':apps_script/10_My_Portfolio.gs'],
                                       {cwd: ROOT, encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore']}));
  }
  for (const get of candidates) {
    try {
      const text = get();
      if (sha256(text) === BASE_SHA) return text;
    } catch (e) { /* next candidate */ }
  }
  return null;
}
const baseSource = loadBase();
let ACTIVE_SOURCE = newSource;   // the file under test for this battery (a mutant while T10 runs)

/* ------------------------------------------------------------ fixtures */
const FOLDER = 'synthFolder_0123456789';
const CAP_MS = Date.parse('2026-11-02T07:06:12.000Z');     // 10:06:12 Riyadh
const START = CAP_MS + 288000;                             // 10:11:00 Riyadh, age 4m48s -> '5m'
const CREATED_MS = CAP_MS + 20000;
const FILE_NAME = 'TFB_IBKR_Capture_20261102-070612.json';
const PANEL_CASH = 70000;                                  // the operator's typed figure (not the certified one)
const RUN = '7171717171';
const CASH = 'PF: Cash Available SAR';
const DAY_MS = 86400000, RIYADH = 3 * 3600000;
const riyadhIso = (ms) => new Date(ms + RIYADH).toISOString().slice(0, 23) + '873+03:00';
const iso = (ms) => new Date(ms).toISOString();
const plain = (v) => JSON.parse(JSON.stringify(v));
const riyadhMidnight = (dateText) => new Date(Date.parse(dateText + 'T00:00:00Z') - RIYADH);

const STATUS_HEADER = ['Page', 'Last Updated', 'Status', 'Message', 'Endpoint', 'HTTP Code', 'Rows',
  'Columns', 'Duration ms', 'Warnings'];
function statusGrid(stampMs) {
  const msg = '[STATUS-STAMP v6.70.0] leg=success written=2 failed=0 requested=2 fresh=2 ' +
    'acquired=2/2 acquisition=COMPLETE fetchfail_requested=observe fetchfail_effective=observe ' +
    'warnings=0 | data=COMPLETE policy_data=COMPLETE sha=deadbeef run=' + RUN;
  const stamp = new Date(stampMs + RIYADH).toISOString().slice(0, 19).replace('T', ' ') + '+03:00';
  return [STATUS_HEADER, ['My_Portfolio', stamp, 'SUCCESS', msg, 'backend:run_dashboard_sync', '', 2, 122, 1000, '']];
}
const HOLD_HEADERS = ['Symbol', 'Name', 'Sector', 'Exchange', 'Currency', 'Position Qty', 'Avg Cost',
  'Current Price', 'Price Target', 'Intrinsic Value', 'Expected ROI 12M', 'Forecast Reliability Score',
  'Data Quality Score', 'Risk Bucket', 'Volatility 30D', 'Avg Volume 30D', 'Recommendation Detail',
  'Investability Status', 'Data Provider', 'Last Updated (UTC)', 'Warnings'];
function holdRows(refMs) {
  const acq = iso(refMs - 120000), quote = iso(refMs - 150000);
  const warn = (p) => 'acquisition_status:success; acquisition_acquired_at:' + acq +
    '; acquisition_provider:' + p + '; acquisition_quote_asof:' + quote;
  return [
    ['SYNF.US', 'Synthetic First', 'Financials', 'NYSE/NASDAQ', 'USD', 174, 15.44, 14.73, 18, 17.5,
     23, 76.5, 100, 'Moderate', 25, 120000, 'HOLD', 'INVESTABLE', 'eodhd', acq, warn('eodhd')],
    ['9999.SR', 'Synthetic Sukuk', 'Fixed Income', 'SAU', 'SAR', 100, 98.5, 96.3, '', '', '',
     60, 80, 'Low', 2, 1000, 'HOLD', 'INVESTABLE', 'yahoo_chart', acq, warn('yahoo_chart')]
  ];
}
const RESPONSE = {status: 'ok', kpis: {}, sector_summary: [], alerts: [],
  actions: [{action: 'HOLD', symbol: 'SYNF.US', name: 'Synthetic First'}],
  meta: {route: {version: '4.19.0', request_id: 'req-synthetic'},
         versions: {portfolio_actions: '1.17.1', opportunity_builder: '1.26.1'}}};
const FX_LOOKUP = [['SAR', 'Saudi Riyal', '', 1], ['USD', 'US Dollar', '', 3.7526]];

function capture(o) {
  o = o || {};
  const c = {schema: 'tfb.ibkr-capture/v1', captured_at: riyadhIso(o.capMs || CAP_MS),
    account_alias: 'ibkr-primary', producer: 'synthetic',
    positions: {positions: [{contract_id: 2000001, contract_description: 'SYNF', position: 174,
                             market_price: 14.73, currency: 'USD', asset_class: 'STK'}]},
    balances: {balances: [
      {currency: 'BASE', cash_balance: 19399.2, settled_cash: 19399.2, net_liquidation_value: 21962.09},
      {currency: 'USD', cash_balance: 19399.2, settled_cash: 19399.2, net_liquidation_value: 21962.09}]},
    orders: {orders: [{order_id: 1, order_status: 'SUBMITTED', order_type: 'STOP', side: 'SELL',
                       stop_price: '14.56', remaining_shares_qty: '174'}]}};
  if (o.mutate) o.mutate(c);
  return c;
}
function driveFile(cap, o) {
  o = o || {};
  return {id: o.id || 'fileA', name: o.name || FILE_NAME,
          created: o.created === undefined ? CREATED_MS : o.created,
          text: typeof cap === 'string' ? cap : JSON.stringify(cap)};
}
const FULL_HEADER = ['Date', 'Time', 'Type', 'Amount SAR', 'Balance SAR', 'Delta vs Prev', 'Note',
  'Account', 'Currency', 'Balance Type', 'Balance', 'FX to SAR', 'FX As Of'];
const LEGACY_HEADER = FULL_HEADER.slice(0, 7);
/* the operator's rows: typed dates (a Sheets date cell is Riyadh midnight), no time, a formula balance */
const OPERATOR_ROWS = () => [
  [riyadhMidnight('2026-10-26'), '', 'SNAPSHOT', '', 82818.75, '', 'Derayah USD 9130@ 3.7528; Al Rajhi sukuk only', '', '', '', '', '', ''],
  [riyadhMidnight('2026-10-27'), '', 'SNAPSHOT', '', 72747, '', 'Derayah USD 9130@ 3.7528; Al Rajhi sukuk only', '', '', '', '', '', '']
];
const ENFORCE = {PD_IBKR_EVIDENCE: 'enforce', PD_IBKR_CAPTURE_FOLDER_ID: FOLDER, PD_EXECUTION_EXCLUDE: '9999.SR'};
const OBSERVE_IBKR = Object.assign({}, ENFORCE, {PD_IBKR_EVIDENCE: 'observe'});

function world(o) {
  o = o || {};
  return {props: Object.assign({PD_ACTION_LOG: 'off'}, o.props || {}), docProps: o.docProps || {},
          files: o.files === undefined ? [driveFile(capture())] : o.files, triggers: [], posts: [],
          drive: [], locks: 0, lockBusy: false, clock: o.clock || START, code: o.code || 200,
          statusMs: o.statusMs || START - 3 * 3600000, holdRef: o.holdRef || START,
          cash: o.cash === undefined ? {header: FULL_HEADER.slice(), rows: OPERATOR_ROWS()} : o.cash,
          cashOpens: 0, cashReads: [], appended: [], appendThrows: o.appendThrows || null,
          runLog: [], propReads: [], propSets: [], propDeletes: [], hasRunLog: o.hasRunLog !== false,
          chain: o.chain || null, top10: o.top10 || null};
}

function chain(rec, at, nr, nc) {
  const blank = () => Array.from({length: nr || 1}, () => Array.from({length: nc || 1}, () => ''));
  const proxy = new Proxy(function () {}, {get(_t, prop) {
    if (prop === 'getValue') return () => '';
    if (prop === 'getValues') return blank;
    if (prop === 'setValue') return (v) => { if (rec) rec.push({at: at, value: v}); return proxy; };
    if (prop === 'setValues') return (v) => { if (rec) rec.push({at: at, values: plain(v)}); return proxy; };
    return () => proxy;
  }});
  return proxy;
}

/** A _Cash_Snapshot stub: header + rows; bounded getRange reads; appendRow recorded (or throwing). */
function cashSheet(w) {
  const grid = () => [w.cash.header].concat(w.cash.rows);
  return {
    getLastRow() { return w.cash.header.length || w.cash.rows.length ? grid().length : 0; },
    getLastColumn() { return Math.max(w.cash.header.length, ...w.cash.rows.map((r) => r.length), 0); },
    getRange(r, c, nr, nc) {
      const g = grid();
      assert.ok(r >= 1 && c >= 1 && nr >= 1 && nc >= 1 && r + nr - 1 <= g.length, 'range inside the grid');
      w.cashReads.push([r, c, nr, nc]);
      const block = g.slice(r - 1, r - 1 + nr).map((row) => {
        const out = [];
        for (let j = 0; j < nc; j++) out.push(row[c - 1 + j] === undefined ? '' : row[c - 1 + j]);
        return out;
      });
      return {getValues() { return block; }, getValue() { return block[0][0]; }};
    },
    appendRow(row) {
      if (w.appendThrows) throw new Error(w.appendThrows);
      w.appended.push(plain(row));
      w.cash.rows.push(row.slice());
    }
  };
}

/** One call of a bound entry point against world w. */
function run(w, call, source) {
  call = call || 'refreshPortfolioDecision';
  class FixedDate extends Date {
    constructor(...a) { if (a.length) super(...a); else super(w.clock); }
    static now() { return w.clock; }
  }
  const logs = [], writes = [], statusSets = [];
  const rows = holdRows(w.holdRef);
  const pd = {
    getRange(r, c, nr, nc) {
      if (arguments.length === 2 && r === 4 && c === 1) return {getValue() { return 'CONTROL PANEL'; }};
      if (arguments.length === 2 && r === 5 && c === 2) return {getValue() { return PANEL_CASH; }};
      if (arguments.length === 2 && r === 2 && c === 2) return chain({push(x) { statusSets.push(x.value); }}, [r, c]);
      return chain(writes, [r, c, nr, nc], nr, nc);
    },
    getMaxRows() { return 80; }, getMaxColumns() { return 26; }
  };
  const mp = {
    __headers: HOLD_HEADERS,
    getLastRow() { return rows.length + 1; }, getLastColumn() { return HOLD_HEADERS.length; },
    getRange(r, c, nr, nc) { return {getValues() { return rows.slice(0, nr).map((x) => x.slice(0, nc)); }}; }
  };
  const grid = statusGrid(w.statusMs);
  const status = {
    getLastRow() { return grid.length; }, getLastColumn() { return 10; },
    getRange(r, c, nr, nc) { return {getValues() { return grid.slice(0, nr).map((x) => x.slice(0, nc)); }}; }
  };
  const runLogSheet = {appendRow(row) { w.runLog.push(plain(row)); }};
  const sheets = {Portfolio_Decision: pd, My_Portfolio: mp, _Status: status};
  const ss = {
    getSheetByName(n) {
      if (n === '_Cash_Snapshot') { w.cashOpens++; return w.cash ? cashSheet(w) : null; }
      if (n === '_Run_Log') return w.hasRunLog ? runLogSheet : null;
      return Object.prototype.hasOwnProperty.call(sheets, n) ? sheets[n] : null;
    },
    getRangeByName(n) { return n === 'TFB_FX_LOOKUP' ? {getValues() { return FX_LOOKUP; }} : null; },
    insertSheet() { return chain(); }
  };
  const props = (store, name) => ({
    getProperty(k) { if (name === 'script') w.propReads.push(k); return Object.prototype.hasOwnProperty.call(store, k) ? store[k] : null; },
    setProperty(k, v) { if (name === 'script') w.propSets.push([k, String(v)]); store[k] = String(v); },
    deleteProperty(k) { if (name === 'script') w.propDeletes.push(k); delete store[k]; }
  });
  const trigger = (handler, spec) => ({getHandlerFunction() { return handler; }, spec: spec});
  const ctx = {
    SHEET_NAMES: {MY_PORTFOLIO: 'My_Portfolio'},
    Date: FixedDate,
    Logger: {log(m) { logs.push(String(m)); }},
    Session: {getScriptTimeZone() { return 'Asia/Riyadh'; }},
    Utilities: {formatDate(d) { return new Date(d.getTime() + RIYADH).toISOString().slice(0, 19).replace('T', ' '); }},
    PropertiesService: {getScriptProperties() { return props(w.props, 'script'); },
                        getDocumentProperties() { return props(w.docProps, 'doc'); }},
    SpreadsheetApp: {getActiveSpreadsheet() { return ss; }, getActive() { return ss; }, flush() {},
      BorderStyle: {SOLID: 'SOLID', SOLID_THICK: 'SOLID_THICK'}, getUi() { return {alert() {}}; }},
    UrlFetchApp: {fetch(url, options) {
      w.posts.push(JSON.parse(options.payload));
      return {getContentText() { return JSON.stringify(RESPONSE); }, getResponseCode() { return w.code; }};
    }},
    DriveApp: {searchFiles(q) {
      w.drive.push(q);
      const m = q.match(/modifiedDate > '([^']+)'/);
      const since = m ? Date.parse(m[1] + 'Z') : -Infinity;
      const list = w.files.filter((f) => q.indexOf("'" + w.props.PD_IBKR_CAPTURE_FOLDER_ID + "' in parents") === 0 && f.created > since);
      let i = 0;
      return {hasNext() { return i < list.length; }, next() {
        const f = list[i++];
        return {getName() { return f.name; }, getId() { return f.id; }, getDateCreated() { return new Date(f.created); },
                getBlob() { return {getDataAsString() { return f.text; }}; }};
      }};
    }},
    LockService: {getScriptLock() {
      return {tryLock() { if (w.lockBusy) return false; w.locks++; return true; }, releaseLock() { w.locks--; }};
    }},
    ScriptApp: {
      getProjectTriggers() { return w.triggers.slice(); },
      deleteTrigger(t) { w.triggers = w.triggers.filter((x) => x !== t); },
      newTrigger(handler) {
        const b = {spec: {}, timeBased() { return b; }, everyMinutes(n) { b.spec.every = n; return b; },
                   after(ms) { b.spec.after = ms; return b; },
                   create() { const t = trigger(handler, b.spec); w.triggers.push(t); return t; }};
        return b;
      }
    },
    ensureSheetExists_(name) { return ss.getSheetByName(name); },
    readCurrentSheetHeaders_(sheet) { return sheet.__headers || []; }
  };
  if (w.chain) ctx.tfbDcReadInputs_ = w.chain;
  if (w.top10) ctx.refreshDecisionTop10 = w.top10;
  vm.createContext(ctx);
  vm.runInContext(source || ACTIVE_SOURCE, ctx, {filename: '10_My_Portfolio.gs'});
  const before = w.posts.length;
  const ret = call === 'none' ? undefined : ctx[call]();
  return {ret: ret, b2: statusSets[statusSets.length - 1], post: w.posts.length > before ? w.posts[w.posts.length - 1] : null,
          writes: writes, logs: logs, ctx: ctx, w: w};
}
const sha = (x) => sha256(JSON.stringify(x));
const snapshot = (r) => ({b2: r.b2, post: r.post, writes: sha(r.writes), logs: r.logs, runLog: r.w.runLog,
                          appended: r.w.appended, triggers: r.w.triggers.map((t) => [t.getHandlerFunction(), t.spec]),
                          sets: r.w.propSets, cashOpens: r.w.cashOpens});

/* ------------------------------------------------------------ battery */
function battery(source, opts) {
  opts = opts || {};
  ACTIVE_SOURCE = source;
  const results = [], dump = {};
  const base = baseSource;
  let passed = 0, total = 0;
  function section(label, fn) {
    total++;
    try { fn(); passed++; results.push('PASS ' + label); }
    catch (e) {
      results.push('FAIL ' + label + ' ... ' + String(e && e.message || e).split('\n').slice(0, 3).join(' '));
      if (opts.throwOnFail) throw new Error(label + ': ' + String(e && e.message || e));
    }
  }
  const SENT = ' | ibkr evidence SENT (capture 10:06 age 5m; linked 1/1; cash 72747.00 SAR; USD 3.75; excluded 9999.SR)';
  const POOL = 'holdings 2 rows from My_Portfolio (qty/cost from cost-basis: 0)';
  const EXPECTED_ROW = [
    '2026-11-01T21:00:00.000Z', '10:06:12', 'SNAPSHOT (IBKR capture)', 0, 72747, 0,
    'ibkr-capture:' + FILE_NAME + '; captured_at 2026-11-02T07:06:12.000Z; written 2026-11-02 10:11 Riyadh, age 5m; ' +
      'settled 19399.2 USD, reserved 0 USD; FX policy://sama-usd-peg-3.75 @ 3.75; writer 10_My_Portfolio v1.5.18',
    'IBKR (ibkr-primary)', 'USD', 'settled_cash', 19399.2, 3.75, '2026-11-02T10:06:12+03:00'];
  const OBSERVE_TOKEN = ' | [cash-snapshot-observe] would append (2026-11-02 10:06:12 Riyadh; IBKR (ibkr-primary); ' +
    'USD 19399.2 settled_cash; 72747.00 SAR; FX 3.75 policy://sama-usd-peg-3.75; delta 0; ibkr-capture:' + FILE_NAME + ')';
  const ON_TOKEN = ' | cash snapshot appended (2026-11-02 10:06 Riyadh; 72747.00 SAR; USD 19399.2 settled_cash; FX 3.75)';
  const SECRETS = ['19399', '72747', '82818', 'ibkr-primary', 'ibkr-capture:', FILE_NAME];

  section('T0 compile, version lockstep, base oracle', () => {
    new vm.Script(source, {filename: 'new.gs'});
    assert.equal((source.match(/^var PD_VERSION = '1\.5\.18';$/mg) || []).length, 1);
    assert.ok(/^ \* Version: 1\.5\.18$/m.test(source));
    assert.ok(source.indexOf(' * v1.5.18 (2026-11-02)  (CASH EVIDENCE ROW') > 0);
    assert.ok(source.indexOf(' * v1.5.17 (2026-10-28)  (D1b POST-EARNINGS INPUTS') > 0, 'prior WHY kept');
    if (base) {
      new vm.Script(base, {filename: 'base.gs'});
      assert.equal(sha256(base), BASE_SHA);
      assert.ok(/^var PD_VERSION = '1\.5\.17';$/m.test(base));
      for (const name of ['pdCashSnapApply_', 'pdCashSnapRow_', 'pdIbkrEvidenceFollowUpRetry', 'pdIbkrRetryCore_']) {
        assert.equal(base.indexOf('function ' + name + '('), -1, 'base lacks ' + name);
      }
      assert.equal(base.indexOf('PD_CASH_SNAPSHOT_WRITE'), -1);
      assert.equal(base.indexOf('PD_IBKR_FOLLOWUP_RETRY'), -1);
    }
  });

  section('T1 golden negative / kill identity: absent, off and junk never open _Cash_Snapshot; everything equals the base', () => {
    const scenarios = [
      ['enforce fresh', {props: ENFORCE}],
      ['observe fresh', {props: OBSERVE_IBKR}],
      ['enforce stale', {props: ENFORCE, clock: START + 20 * 60000}],
      ['enforce no capture', {props: ENFORCE, files: []}],
      ['ibkr off', {props: {}}]
    ];
    for (const raw of [undefined, 'off', '', 'enforce', 'yes', '1', 'true', 'observed', 'onn']) {
      for (const [label, o] of scenarios) {
        const props = Object.assign({}, o.props, raw === undefined ? {} : {PD_CASH_SNAPSHOT_WRITE: raw, PD_IBKR_FOLLOWUP_RETRY: raw});
        const r = run(world(Object.assign({}, o, {props: props})));
        assert.equal(r.w.cashOpens, 0, label + ' ' + raw + ': _Cash_Snapshot opened');
        assert.deepEqual(r.w.appended, [], label);
        assert.deepEqual(r.w.propSets, [], label);
        assert.deepEqual(r.w.runLog, [], label);
        assert.equal(r.ret, 'ok');
        if (label === 'enforce fresh') {
          assert.ok(r.b2.indexOf(POOL + SENT + ' | ') > 0, r.b2);
          assert.equal(r.post.controls[CASH], 72747);
        }
        if (base) {
          const bw = world(Object.assign({}, o, {props: props}));
          const b = run(bw, null, base);
          assert.equal(r.b2, b.b2, label + ' ' + raw);
          assert.deepEqual(r.post, b.post, label);
          assert.deepEqual(r.writes, b.writes, label);
          /* Logger lines carry the version token of the file under test */
          assert.deepEqual(r.logs, b.logs.map((l) => l.replace(/\[MP-DEC v1\.5\.17\]/g, '[MP-DEC v1.5.18]')), label);
          assert.deepEqual(r.w.runLog, bw.runLog, label);
          assert.deepEqual(r.w.triggers, bw.triggers, label);
          const extra = r.w.propReads.filter((k) => k === 'PD_CASH_SNAPSHOT_WRITE').length;
          const others = r.w.propReads.filter((k) => k !== 'PD_CASH_SNAPSHOT_WRITE');
          assert.deepEqual(others, bw.propReads, label + ': the other property reads are the base reads');
          assert.equal(extra, label === 'ibkr off' ? 0 : 1, label + ': exactly one PD_CASH_SNAPSHOT_WRITE read');
        }
      }
    }
    if (!base) results.push('SKIP T1 live base comparison (v1.5.17 base not reachable)');
  });

  section('T2 observe: nothing appended; B2 carries the would-append cells; request, render and rows unchanged', () => {
    const off = run(world({props: ENFORCE}));
    const w = world({props: Object.assign({PD_CASH_SNAPSHOT_WRITE: 'observe'}, ENFORCE)});
    const r = run(w);
    dump.observe = r.b2;
    assert.equal(r.b2, off.b2.replace(SENT, SENT + OBSERVE_TOKEN));
    assert.deepEqual(r.post, off.post);
    assert.equal(sha(r.writes), sha(off.writes));
    assert.deepEqual(w.appended, []);
    assert.deepEqual(w.propSets, []);
    assert.deepEqual(w.runLog, []);
    assert.ok(w.cashOpens >= 1 && w.cashReads.length === 2, 'header + tail read once: ' + JSON.stringify(w.cashReads));
    assert.deepEqual(w.cashReads, [[1, 1, 1, 13], [2, 1, 2, 13]]);
    const line = r.logs.find((l) => /cash snapshot \(observe\) would append a row for 2026-11-02/.test(l));
    assert.ok(line, r.logs.join('\n'));
    for (const l of r.logs) for (const s of SECRETS) assert.equal(l.indexOf(s), -1, 'Logger carries ' + s);
    const o2 = run(world({props: Object.assign({PD_CASH_SNAPSHOT_WRITE: ' Observe '}, OBSERVE_IBKR)}));
    assert.ok(o2.b2.indexOf('[ibkr-observe] would send') > 0 && o2.b2.indexOf(OBSERVE_TOKEN) > 0, o2.b2);
    assert.equal(o2.post.controls[CASH], PANEL_CASH, 'observe IBKR mode still sends the panel cash');
  });

  section('T3 on: exactly one appendRow with the certified cells; never a second row the same Riyadh day', () => {
    const off = run(world({props: ENFORCE}));
    const w = world({props: Object.assign({PD_CASH_SNAPSHOT_WRITE: 'on'}, ENFORCE)});
    const r = run(w);
    dump.on = r.b2;
    dump.row = w.appended[0];
    assert.equal(w.appended.length, 1);
    assert.deepEqual(w.appended[0], EXPECTED_ROW);
    assert.equal(w.appended[0].length, 13);
    assert.ok(Math.abs(w.appended[0][10] * w.appended[0][11] - w.appended[0][4]) <= 0.01, 'Balance x FX = Balance SAR');
    assert.equal(r.b2, off.b2.replace(SENT, SENT + ON_TOKEN));
    assert.deepEqual(r.post, off.post, 'the request is the base request');
    assert.equal(r.post.controls[CASH], 72747);
    assert.equal(sha(r.writes), sha(off.writes), 'the render is the base render');
    assert.deepEqual(w.propSets, [['PD_CASH_SNAPSHOT_LAST', '2026-11-02']]);
    assert.equal(w.runLog.length, 1);
    const row = w.runLog[0];
    assert.equal(row[1], 'INFO'); assert.equal(row[2], 'pdCashSnapApply_'); assert.equal(row[3], '_Cash_Snapshot');
    assert.equal(row[4], 'OK');
    assert.equal(row[5], '[MP-DEC v1.5.18] cash snapshot appended for 2026-11-02 (capture 10:06 Riyadh, age 5m)');
    assert.deepEqual(JSON.parse(row[9]), {version: '1.5.18', mode: 'on', date: '2026-11-02', age_ms: 288000});
    for (const l of r.logs.concat([row[5], row[9]])) for (const s of SECRETS) assert.equal(l.indexOf(s), -1, 'log carries ' + s);
    /* a second fresh capture the same day: the property short-circuits (no sheet read), nothing appended */
    const later = CAP_MS + 3600000;
    w.files = [driveFile(capture({capMs: later}), {id: 'fileB', name: 'TFB_IBKR_Capture_20261102-080612.json', created: later + 20000})];
    w.clock = later + 288000; w.holdRef = w.clock; w.statusMs = w.clock - 3 * 3600000;
    w.cashOpens = 0; w.cashReads = []; w.propSets = []; w.runLog = [];
    const r2 = run(w);
    assert.equal(w.appended.length, 1, 'no second row');
    assert.equal(w.cashOpens, 0, 'property short-circuit: no sheet access');
    assert.ok(r2.b2.indexOf('cash 72747.00 SAR; USD 3.75; excluded 9999.SR) | cash snapshot kept: row for 2026-11-02 exists | ') > 0, r2.b2);
    assert.deepEqual(w.runLog, [], 'the routine same-day skip is not a _Run_Log row');
    /* the property lost (another deployment, a manual delete): the sheet scan still finds the row */
    delete w.props.PD_CASH_SNAPSHOT_LAST;
    w.cashOpens = 0; w.cashReads = [];
    const r3 = run(w);
    assert.equal(w.appended.length, 1, 'no second row after the scan');
    assert.equal(w.cashOpens, 1);
    assert.ok(r3.b2.indexOf(' | cash snapshot kept: row for 2026-11-02 exists | ') > 0, r3.b2);
    assert.deepEqual(w.propSets, [], 'no claim without a write');
    /* an operator-typed row for today (a date cell, no time) also blocks */
    const w4 = world({props: Object.assign({PD_CASH_SNAPSHOT_WRITE: 'on'}, ENFORCE),
                      cash: {header: FULL_HEADER.slice(), rows: OPERATOR_ROWS().concat([[riyadhMidnight('2026-11-02'), '', 'SNAPSHOT', '', 72747, '', 'typed', '', '', '', '', '', '']])}});
    const r4 = run(w4);
    assert.deepEqual(w4.appended, []);
    assert.ok(r4.b2.indexOf(' | cash snapshot kept: row for 2026-11-02 exists | ') > 0, r4.b2);
    /* a text date for today blocks too; a row for another day does not */
    const w5 = world({props: Object.assign({PD_CASH_SNAPSHOT_WRITE: 'on'}, ENFORCE),
                      cash: {header: FULL_HEADER.slice(), rows: OPERATOR_ROWS().concat([['2026-11-02', '09:00:00', 'SNAPSHOT', '', 1, '', 'x', '', '', '', '', '', '']])}});
    run(w5);
    assert.deepEqual(w5.appended, []);
    const w6 = world({props: Object.assign({PD_CASH_SNAPSHOT_WRITE: 'on'}, ENFORCE),
                      cash: {header: FULL_HEADER.slice(), rows: OPERATOR_ROWS().concat([['2026-11-03', '09:00:00', 'SNAPSHOT', '', 1, '', 'x', '', '', '', '', '', '']])}});
    run(w6);
    assert.equal(w6.appended.length, 1, 'another day does not block');
    assert.equal(w6.appended[0][5], 72746, 'delta vs the newest numeric balance (1)');
    /* the header order is not assumed: the cells follow the names */
    const shuffled = ['Balance SAR', 'Account', 'Date', 'Time', 'Currency', 'Balance Type', 'Balance', 'FX to SAR', 'FX As Of', 'Note'];
    const w7 = world({props: Object.assign({PD_CASH_SNAPSHOT_WRITE: 'on'}, ENFORCE), cash: {header: shuffled, rows: []}});
    run(w7);
    assert.equal(w7.appended.length, 1);
    assert.deepEqual(w7.appended[0].slice(0, 9), [72747, 'IBKR (ibkr-primary)', '2026-11-01T21:00:00.000Z', '10:06:12', 'USD', 'settled_cash', 19399.2, 3.75, '2026-11-02T10:06:12+03:00']);
    /* a working BUY limit reserves: Balance is settled less reserved, Balance SAR stays its certified cents */
    const w8 = world({props: Object.assign({PD_CASH_SNAPSHOT_WRITE: 'on'}, ENFORCE), files: [driveFile(capture({mutate(c) {
      c.orders.orders.push({order_id: 2, order_status: 'NEW', order_type: 'LIMIT', side: 'BUY', limit_price: '10', remaining_shares_qty: '100'}); }}))]});
    const r8 = run(w8);
    assert.equal(w8.appended.length, 1);
    assert.equal(w8.appended[0][10], 18399.2); assert.equal(w8.appended[0][4], 68997);
    assert.equal(r8.post.controls[CASH], 68997);
    assert.ok(w8.appended[0][6].indexOf('settled 19399.2 USD, reserved 1000 USD') > 0, w8.appended[0][6]);
    /* the base never writes, whatever the property says */
    if (base) {
      const bw = world({props: Object.assign({PD_CASH_SNAPSHOT_WRITE: 'on'}, ENFORCE)});
      const b = run(bw, null, base);
      assert.deepEqual(bw.appended, [], 'GOLDEN NEGATIVE: the base ignores the property');
      assert.equal(bw.cashOpens, 0);
      assert.equal(b.b2, off.b2);
    }
  });

  section('T4 stale or absent capture: no row; the last-known token; controls, packet, render and KPI cells are the base', () => {
    const stale = {props: Object.assign({PD_CASH_SNAPSHOT_WRITE: 'on'}, ENFORCE), clock: START + 20 * 60000};
    const off = run(world(Object.assign({}, stale, {props: ENFORCE})));
    const w = world(stale);
    const r = run(w);
    dump.stale = r.b2;
    const age = r.ctx.pdIbkrAge_(w.clock - (Date.parse('2026-10-27T00:00:00Z') - RIYADH + DAY_MS));
    const KNOWN = ' | last known cash 72747.00 SAR (as of 2026-10-27 (no time), at least ' + age + ' old; not applied to funding)';
    assert.equal(r.b2, off.b2.replace('; USD 3.75; excluded 9999.SR | ', '; USD 3.75; excluded 9999.SR' + KNOWN + ' | '));
    assert.ok(r.b2.indexOf('NOT SENT: capture 10:06 is 25m old (limit 14m)') > 0, r.b2);
    assert.deepEqual(w.appended, []);
    assert.deepEqual(w.propSets, []);
    assert.deepEqual(r.post, off.post, 'the request is the base request');
    assert.equal(r.post.controls[CASH], PANEL_CASH, 'the panel cash, not the last-known cash');
    assert.equal('reconciliation_evidence' in r.post, false);
    assert.equal(sha(r.writes), sha(off.writes), 'the KPI cells and the render are the base render');
    assert.deepEqual(w.runLog, []);
    /* a row with a time (today's own appended row) reads as of that time */
    const w2 = world(Object.assign({}, stale, {cash: {header: FULL_HEADER.slice(), rows: OPERATOR_ROWS().concat([plain(EXPECTED_ROW).map((v, i) => (i === 0 ? new Date(v) : v))])}}));
    const r2 = run(w2);
    assert.ok(r2.b2.indexOf(' | last known cash 72747.00 SAR (as of 2026-11-02 10:06 Riyadh, 25m old; not applied to funding) | ') > 0, r2.b2);
    assert.ok(r2.b2.indexOf('cash snapshot') < 0, 'no writer token on a stale render');
    assert.equal(r2.post.controls[CASH], PANEL_CASH);
    /* an As Of column wins when present; a legacy header still serves the note */
    const w3 = world(Object.assign({}, stale, {cash: {header: LEGACY_HEADER.concat(['As Of']), rows: [[riyadhMidnight('2026-11-01'), '', 'SNAPSHOT', '', 1234.5, '', 'n', '2026-11-01T18:30:00+03:00']]}}));
    const r3 = run(w3);
    const age3 = r3.ctx.pdIbkrAge_(w3.clock - Date.parse('2026-11-01T15:30:00Z'));
    assert.ok(r3.b2.indexOf(' | last known cash 1234.50 SAR (as of 2026-11-01 18:30 Riyadh, ' + age3 + ' old; not applied to funding) | ') > 0, r3.b2);
    /* no capture at all, observe mode of the property: same note */
    const w4 = world({props: Object.assign({PD_CASH_SNAPSHOT_WRITE: 'observe'}, ENFORCE), files: []});
    const r4 = run(w4);
    assert.ok(r4.b2.indexOf('NOT SENT: no capture in the last 36h; USD 3.75; excluded 9999.SR | last known cash 72747.00 SAR (as of 2026-10-27 (no time), at least ') > 0, r4.b2);
    /* the tab missing / no balance: the note says so, nothing else moves */
    const w5 = world(Object.assign({}, stale, {cash: null}));
    assert.ok(run(w5).b2.indexOf(' | last known cash unavailable (_Cash_Snapshot missing) | ') > 0);
    const w6 = world(Object.assign({}, stale, {cash: {header: FULL_HEADER.slice(), rows: [[riyadhMidnight('2026-10-27'), '', 'SNAPSHOT', '', 'n/a', '', 'n']]}}));
    assert.ok(run(w6).b2.indexOf(' | last known cash unavailable (no Balance SAR row) | ') > 0);
    /* a thousands-separated text balance and a day-fraction time are read */
    const w7 = world(Object.assign({}, stale, {cash: {header: FULL_HEADER.slice(), rows: [['2026-11-02', 0.25, 'SNAPSHOT', '', '72,747.00', '', 'n']]}}));
    const r7 = run(w7);
    assert.ok(r7.b2.indexOf(' | last known cash 72747.00 SAR (as of 2026-11-02 06:00 Riyadh, 4h31m old; not applied to funding) | ') > 0, r7.b2);
    if (base) {
      const bw = world(stale);
      const b = run(bw, null, base);
      assert.equal(b.b2, off.b2, 'GOLDEN NEGATIVE: the base shows no last-known cash');
      assert.deepEqual(b.post, r.post, 'the request the base sends is the request v1.5.18 sends');
    }
  });

  section('T5 missing tab, legacy / partial / empty header, withheld cash, two currencies: nothing written, the reason named', () => {
    const on = Object.assign({PD_CASH_SNAPSHOT_WRITE: 'on'}, ENFORCE);
    const cases = [
      ['missing tab', {cash: null}, '_Cash_Snapshot missing'],
      ['legacy header', {cash: {header: LEGACY_HEADER.slice(), rows: [['2026-10-27', '', 'SNAPSHOT', '', 72747, '', 'n']]}}, 'header lacks Account, Currency, Balance Type'],
      ['no FX As Of', {cash: {header: FULL_HEADER.slice(0, 12), rows: []}}, 'header lacks FX As Of'],
      ['no Time', {cash: {header: FULL_HEADER.filter((h) => h !== 'Time'), rows: []}}, 'header lacks Time'],
      ['empty sheet', {cash: {header: [], rows: []}}, 'header empty'],
      ['blank header', {cash: {header: ['', '', ''], rows: []}}, 'header empty'],
      ['market BUY', {files: [driveFile(capture({mutate(c) { c.orders.orders.push({order_id: 3, order_status: 'NEW', order_type: 'MARKET', side: 'BUY', remaining_shares_qty: '5'}); }}))]},
       'cash withheld (reservations incomplete (BUY order without a limit price))'],
      ['two currencies', {files: [driveFile(capture({mutate(c) { c.balances.balances.push({currency: 'SAR', settled_cash: 100}); }}))]},
       'several cash currencies (USD,SAR); one-currency rows only']
    ];
    for (const [label, o, why] of cases) {
      const w = world(Object.assign({props: on}, o));
      const r = run(w);
      assert.deepEqual(w.appended, [], label);
      assert.deepEqual(w.propSets, [], label);
      assert.ok(r.b2.indexOf(' | cash snapshot NOT appended: ' + why + ' | ') > 0, label + ': ' + r.b2);
      assert.ok(r.post.reconciliation_evidence, label + ': evidence still sent');
      assert.equal(w.runLog.length, 1, label);
      assert.equal(w.runLog[0][4], 'SKIPPED', label);
      assert.equal(w.runLog[0][2], 'pdCashSnapApply_');
      assert.ok(w.runLog[0][5].indexOf('cash snapshot not appended: ' + why) > 0, label);
      for (const s of SECRETS) assert.equal(w.runLog[0][5].indexOf(s), -1, label + ' row carries ' + s);
      const ow = world(Object.assign({props: Object.assign({}, on, {PD_CASH_SNAPSHOT_WRITE: 'observe'})}, o));
      const orr = run(ow);
      assert.deepEqual(ow.appended, [], label);
      assert.ok(orr.b2.indexOf(' | [cash-snapshot-observe] would not append: ' + why + ' | ') > 0, label + ': ' + orr.b2);
      assert.deepEqual(ow.runLog, [], label + ': observe writes no _Run_Log row');
    }
    const twoCcy = run(world({props: on, files: cases[7][1].files}));
    assert.equal(twoCcy.post.controls[CASH], 72847, 'the certified multi-currency cash (USD 72747 + SAR 100) still funds the request');
  });

  section('T6 a Sheets failure inside the writer never breaks the evidence send', () => {
    const off = run(world({props: ENFORCE}));
    const w = world({props: Object.assign({PD_CASH_SNAPSHOT_WRITE: 'on'}, ENFORCE), appendThrows: 'Service error: Spreadsheets'});
    const r = run(w);
    assert.deepEqual(w.appended, []);
    assert.deepEqual(w.propSets, [['PD_CASH_SNAPSHOT_LAST', '2026-11-02']]);
    assert.deepEqual(w.propDeletes, ['PD_CASH_SNAPSHOT_LAST'], 'the claim is released');
    assert.equal(w.props.PD_CASH_SNAPSHOT_LAST, undefined);
    assert.equal(r.b2, off.b2.replace(SENT, SENT + ' | cash snapshot ERROR (Service error: Spreadsheets)'));
    assert.deepEqual(r.post, off.post);
    assert.equal(r.post.controls[CASH], 72747);
    assert.ok(r.post.reconciliation_evidence);
    assert.equal(r.ret, 'ok');
    assert.equal(w.runLog.length, 1); assert.equal(w.runLog[0][4], 'FAILED'); assert.equal(w.runLog[0][1], 'ERROR');
    /* a read failure (getLastRow throws) is also contained */
    const w2 = world({props: Object.assign({PD_CASH_SNAPSHOT_WRITE: 'on'}, ENFORCE)});
    w2.cash.header = null;
    const r2 = run(w2);
    assert.ok(r2.b2.indexOf(' | cash snapshot ERROR (') > 0, r2.b2);
    assert.ok(r2.post.reconciliation_evidence);
    /* a _Run_Log tab missing never matters */
    const w3 = world({props: Object.assign({PD_CASH_SNAPSHOT_WRITE: 'on'}, ENFORCE), hasRunLog: false});
    const r3 = run(w3);
    assert.equal(w3.appended.length, 1);
    assert.ok(r3.b2.indexOf(ON_TOKEN) > 0);
  });

  section('T7 retry gate: off = the base follow-up; on = ONE +3 min trigger after a cockpit collision, age-bounded, never re-armed', () => {
    const PACKET = {schema_version: 1, source_ref: 'ibkr-capture:' + FILE_NAME, captured_at: iso(CAP_MS),
      accounts: [{account_id: 'ibkr-primary', positions_asof: iso(CAP_MS), positions_complete: true, positions: [],
                  cash_asof: iso(CAP_MS), cash_complete: true,
                  cash: [{currency: 'USD', settled_cash: '19399.2', reserved_cash: '0', reservations_complete: true}]}],
      holding_links: [], funding_accounts: ['ibkr-primary'],
      fx_rates: [{currency: 'USD', rate_to_sar: '3.75', asof: iso(CAP_MS), source_ref: 'policy://sama-usd-peg-3.75'}]};
    const guards = (busy) => () => ({automationAllowed: true, inflight: false, cockpitRunning: busy, quiet: false});
    function fu(o, call, src) {
      let rendered = 0;
      const w = world(Object.assign({props: Object.assign({PD_IBKR_EVIDENCE_PUBLISH: 'on'}, o.props || {}),
                                     docProps: {TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1: JSON.stringify(PACKET)},
                                     clock: CAP_MS + (o.ageMs === undefined ? 8 * 60000 : o.ageMs),
                                     chain: guards(o.busy !== false), top10: () => { rendered++; }}, o.world || {}));
      w.triggers.push({getHandlerFunction() { return call || 'pdIbkrEvidenceFollowUp'; }, spec: {}});
      const r = run(w, call || 'pdIbkrEvidenceFollowUp', src);
      return {r: r, w: w, rendered: rendered, line: w.runLog[0] ? w.runLog[0][5] : '', row: w.runLog[0]};
    }
    const SKIP = '[MP-DEC v1.5.18] evidence follow-up -> skipped: cockpit refresh in progress (cockpit refresh in progress; trigger cleared)';
    /* off / absent / junk: no trigger, the base line, row and details */
    for (const raw of [undefined, 'off', 'yes', '1', 'true', 'enforce']) {
      const x = fu({props: raw === undefined ? {} : {PD_IBKR_FOLLOWUP_RETRY: raw}});
      assert.equal(x.r.ret, 'skipped: cockpit refresh in progress');
      assert.deepEqual(x.w.triggers, [], 'no trigger when ' + raw);
      assert.equal(x.line, SKIP);
      assert.equal(x.row[2], 'pdIbkrEvidenceFollowUp'); assert.equal(x.row[4], 'SKIPPED');
      assert.deepEqual(JSON.parse(x.row[9]), {version: '1.5.18', go: false});
      assert.equal(x.rendered, 0);
      if (base) {
        const b = fu({props: raw === undefined ? {} : {PD_IBKR_FOLLOWUP_RETRY: raw}}, null, base);
        assert.equal(b.line, SKIP.replace('v1.5.18', 'v1.5.17'));
        assert.deepEqual(b.w.triggers, [], 'GOLDEN NEGATIVE: the base never arms a retry');
        assert.deepEqual(JSON.parse(b.row[9]), {version: '1.5.17', go: false});
      }
    }
    /* on: one +3 min trigger, the line names it */
    const on = fu({props: {PD_IBKR_FOLLOWUP_RETRY: 'on'}});
    dump.retry = on.line;
    assert.equal(on.r.ret, 'skipped: cockpit refresh in progress');
    assert.deepEqual(on.w.triggers.map((t) => [t.getHandlerFunction(), t.spec]), [['pdIbkrEvidenceFollowUpRetry', {after: 180000}]]);
    assert.equal(on.line, SKIP.replace('trigger cleared)', 'trigger cleared; retry armed (+3m) (packet 11m old at the retry))'));
    assert.deepEqual(JSON.parse(on.row[9]), {version: '1.5.18', go: false, retry: 'retry armed (+3m) (packet 11m old at the retry)'});
    assert.ok(on.r.logs.some((l) => l === on.line));
    /* on, but the packet would be too old at the retry: not armed, said so */
    const late = fu({props: {PD_IBKR_FOLLOWUP_RETRY: 'on'}, ageMs: 12 * 60000});
    assert.deepEqual(late.w.triggers, []);
    assert.ok(late.line.indexOf('; retry not armed: packet would be 15m old at the retry (limit 14m))') > 0, late.line);
    const edge = fu({props: {PD_IBKR_FOLLOWUP_RETRY: 'on'}, ageMs: 11 * 60000});
    assert.equal(edge.w.triggers.length, 1, '11m + 3m = 14m is still inside');
    /* on, another skip reason: nothing */
    const quiet = fu({props: {PD_IBKR_FOLLOWUP_RETRY: 'on'}, world: {chain: () => ({automationAllowed: true, inflight: false, cockpitRunning: false, quiet: true})}});
    assert.deepEqual(quiet.w.triggers, []);
    assert.equal(quiet.line, '[MP-DEC v1.5.18] evidence follow-up -> skipped: quiet window (quiet window; trigger cleared)');
    /* on and the cockpit free: rendered, no trigger */
    const free = fu({props: {PD_IBKR_FOLLOWUP_RETRY: 'on'}, busy: false});
    assert.equal(free.r.ret, 'rendered'); assert.equal(free.rendered, 1); assert.deepEqual(free.w.triggers, []);
    assert.equal(free.line, '[MP-DEC v1.5.18] evidence follow-up -> rendered (packet age 8m; trigger cleared)');
    /* the retry run: clears its trigger, marks the line and the row, renders when free */
    const retryFree = fu({props: {PD_IBKR_FOLLOWUP_RETRY: 'on'}, busy: false, ageMs: 11 * 60000}, 'pdIbkrEvidenceFollowUpRetry');
    assert.equal(retryFree.r.ret, 'rendered'); assert.equal(retryFree.rendered, 1);
    assert.deepEqual(retryFree.w.triggers, []);
    assert.equal(retryFree.line, '[MP-DEC v1.5.18] evidence follow-up (retry) -> rendered (packet age 11m; trigger cleared)');
    assert.equal(retryFree.row[2], 'pdIbkrEvidenceFollowUpRetry'); assert.equal(retryFree.row[4], 'OK');
    assert.equal(retryFree.r.ctx.PD_IBKR_RETRY_RUN_, null, 'the marker is cleared');
    /* the retry run still colliding: skipped, NEVER re-armed */
    const retryBusy = fu({props: {PD_IBKR_FOLLOWUP_RETRY: 'on'}, ageMs: 11 * 60000}, 'pdIbkrEvidenceFollowUpRetry');
    assert.equal(retryBusy.r.ret, 'skipped: cockpit refresh in progress');
    assert.deepEqual(retryBusy.w.triggers, [], 'a retry never re-arms');
    assert.equal(retryBusy.line, '[MP-DEC v1.5.18] evidence follow-up (retry) -> skipped: cockpit refresh in progress (cockpit refresh in progress; trigger cleared; retry not armed: a retry never re-arms)');
    assert.equal(retryBusy.row[2], 'pdIbkrEvidenceFollowUpRetry'); assert.equal(retryBusy.row[4], 'SKIPPED');
    assert.equal(retryBusy.r.ctx.PD_IBKR_RETRY_RUN_, null);
    /* a retry with the gate meanwhile off: still marked, nothing armed */
    const retryOff = fu({props: {}, ageMs: 11 * 60000}, 'pdIbkrEvidenceFollowUpRetry');
    assert.deepEqual(retryOff.w.triggers, []);
    assert.equal(retryOff.line, '[MP-DEC v1.5.18] evidence follow-up (retry) -> skipped: cockpit refresh in progress (cockpit refresh in progress; trigger cleared)');
    /* arming replaces, never stacks; the state line counts */
    const ctx = run(world({props: ENFORCE}), 'none').ctx;
    assert.equal(ctx.pdIbkrArmRetry_(), 'retry armed (+3m)');
    assert.equal(ctx.pdIbkrArmRetry_(), 'retry armed (+3m)');
    assert.equal(ctx.pdIbkrRetryState_(), '1 armed');
    assert.equal(ctx.pdIbkrRemoveRetry_(), 1);
    assert.equal(ctx.pdIbkrRetryState_(), 'none armed');
    /* the pure core */
    const R = 'cockpit refresh in progress';
    assert.equal(ctx.pdIbkrRetryCore_('on', R, 300000, false).arm, true);
    assert.equal(ctx.pdIbkrRetryCore_('on', R, 660000, false).arm, true);
    assert.equal(ctx.pdIbkrRetryCore_('on', R, 660001, false).arm, false);
    assert.equal(ctx.pdIbkrRetryCore_('on', R, 300000, true).reason, 'a retry never re-arms');
    assert.equal(ctx.pdIbkrRetryCore_('on', 'quiet window', 300000, false).reason, 'not a cockpit collision');
    assert.equal(ctx.pdIbkrRetryCore_('off', R, 300000, false).reason, 'retry off');
    assert.equal(ctx.pdIbkrRetryCore_('on', R, null, false).reason, 'packet age unknown');
    assert.equal(ctx.pdIbkrRetryCore_('on', R, -1, false).reason, 'packet age unknown');
  });

  section('T8 pdSelfTest: the three new lines; never amounts, the alias or the file name', () => {
    const w = world({props: ENFORCE});
    const report = run(w, 'pdSelfTest').ret;
    dump.self_test = report;
    for (const line of ['cash snapshot writer (PD_CASH_SNAPSHOT_WRITE): off; last written here: none; _Cash_Snapshot: header ok (13 columns); rows 2; newest row 2026-10-27 (no time)',
                        'cash snapshot core: ok',
                        'follow-up retry (PD_IBKR_FOLLOWUP_RETRY): off; retry trigger: none armed; retry core: ok',
                        'IBKR capture: ' + FILE_NAME + ' -> valid; captured 10:06 Riyadh, age 5m; positions 1; linked 1/1; cash certifiable']) {
      assert.ok(report.indexOf(line) >= 0, line + '\n' + report);
    }
    for (const s of ['19399', '72747', '82818', 'ibkr-primary', 'ibkr-capture:']) assert.equal(report.indexOf(s), -1, s);
    assert.ok(!/FAIL/.test(report), report);
    const w2 = world({props: Object.assign({PD_CASH_SNAPSHOT_WRITE: 'on', PD_CASH_SNAPSHOT_LAST: '2026-11-01', PD_IBKR_FOLLOWUP_RETRY: 'on'}, ENFORCE), cash: null});
    w2.triggers.push({getHandlerFunction() { return 'pdIbkrEvidenceFollowUpRetry'; }, spec: {}});
    const r2 = run(w2, 'pdSelfTest').ret;
    assert.ok(r2.indexOf('cash snapshot writer (PD_CASH_SNAPSHOT_WRITE): on; last written here: 2026-11-01; _Cash_Snapshot: missing') >= 0, r2);
    assert.ok(r2.indexOf('follow-up retry (PD_IBKR_FOLLOWUP_RETRY): on; retry trigger: 1 armed; retry core: ok') >= 0, r2);
    const w3 = world({props: ENFORCE, cash: {header: LEGACY_HEADER.slice(), rows: [['2026-10-27', '', 'SNAPSHOT', '', 72747, '', 'n']]}});
    const r3 = run(w3, 'pdSelfTest').ret;
    assert.ok(r3.indexOf('_Cash_Snapshot: header lacks Account, Currency, Balance Type; rows 1; newest row 2026-10-27 (no time)') >= 0, r3);
    if (base) {
      const b = run(world({props: ENFORCE}), 'pdSelfTest', base).ret;
      assert.equal(b.indexOf('cash snapshot writer'), -1, 'GOLDEN NEGATIVE: the base lacks the line');
      assert.equal(b.indexOf('follow-up retry'), -1);
      const kept = report.split('\n').filter((l) => !/cash snapshot|follow-up retry|self-test$/.test(l));
      for (const l of kept) assert.ok(b.indexOf(l) >= 0, 'base line kept: ' + l);
    }
  });

  section('T9 ES5 / ASCII lint of the added lines, inventory (20 functions, 0 removed), the WHY block', () => {
    const names = (s) => (s.match(/^function\s+([A-Za-z_$][\w$]*)\s*\(/mg) || []).map((m) => m.replace(/^function\s+/, '').replace(/\s*\($/, ''));
    const ADDED = ['pdCashSnapMode_', 'pdCashSnapRiyadh_', 'pdCashSnapHeaderMap_', 'pdCashSnapCents_', 'pdCashSnapStampMs_',
      'pdCashSnapRead_', 'pdCashSnapTailScan_', 'pdCashSnapRow_', 'pdCashSnapToken_', 'pdCashSnapLastKnownNote_',
      'pdCashSnapApply_', 'pdCashSnapSheetState_', 'pdCashSnapCoreOk_', 'pdIbkrRetryMode_', 'pdIbkrRetryCore_',
      'pdIbkrRemoveRetry_', 'pdIbkrArmRetry_', 'pdIbkrRetryAfterSkip_', 'pdIbkrEvidenceFollowUpRetry', 'pdIbkrRetryState_'];
    const nn = names(source);
    for (const f of ADDED) assert.equal(nn.filter((n) => n === f).length, 1, 'inventory ' + f);
    const i0 = source.indexOf('/* ----- v1.5.18 cash snapshot writer');
    const i1 = source.indexOf('/** PURE: one OUTSIDE EXECUTION SCOPE display row.', i0);
    assert.ok(i0 > 0 && i1 > i0, 'block located');
    const block = source.slice(i0, i1);
    const ES6 = /(^|[^A-Za-z0-9_$.'"])(let|const|class|import|export|yield|async|await)\s|=>|`|\.\.\./;
    for (const l of block.split('\n')) {
      assert.ok(![...l].some((ch) => ch.charCodeAt(0) > 126), 'ASCII: ' + l);
      if (!/^\s*(\*|\/\*|\/\/)/.test(l)) assert.ok(!ES6.test(l), 'ES5: ' + l);
    }
    const h0 = source.indexOf(' * v1.5.18 (2026-11-02)'), h1 = source.indexOf(' * v1.5.17 (2026-10-28)');
    assert.ok(h0 > 0 && h1 > h0 && ![...source.slice(h0, h1)].some((ch) => ch.charCodeAt(0) > 126), 'WHY block ASCII, above v1.5.17');
    assert.equal((source.match(/\r/g) || []).length, 0);
    if (base) {
      const nb = names(base), set = new Set(nn);
      for (const n of nb) assert.ok(set.has(n), 'removed: ' + n);
      assert.deepEqual(nn.filter((n) => nb.indexOf(n) < 0).sort(), ADDED.slice().sort());
      assert.equal(nn.length, nb.length + 20);
      /* every line that is not in the base (a multiset difference) is ASCII and ES5 */
      const count = (s) => { const m = new Map(); for (const l of s.split('\n')) m.set(l, (m.get(l) || 0) + 1); return m; };
      const cb = count(base), added = [];
      for (const l of source.split('\n')) {
        const n = cb.get(l) || 0;
        if (n > 0) cb.set(l, n - 1); else added.push(l);
      }
      assert.ok(added.length >= 650 && added.length <= 800, 'added lines ' + added.length);
      for (const l of added) {
        assert.ok(![...l].some((ch) => ch.charCodeAt(0) > 126), 'ASCII: ' + l);
        if (!/^\s*(\*|\/\*|\/\/)/.test(l)) assert.ok(!ES6.test(l), 'ES5: ' + l);
      }
      const nonAscii = (s) => s.split('\n').filter((l) => /[^\x00-\x7f]/.test(l)).length;
      assert.equal(nonAscii(source), nonAscii(base), 'the non-ASCII line count is the base count');
    }
  });

  ACTIVE_SOURCE = newSource;
  return {passed: passed, total: total, results: results, dump: dump};
}

/* ------------------------------------------------------------ mutants */
function mutate(source, pairs) {
  let out = source;
  for (const [from, to] of pairs) {
    assert.equal(out.split(from).length, 2, 'mutant anchor must occur once: ' + from.slice(0, 60));
    out = out.replace(from, to);
  }
  return out;
}
const MUTANTS = [
  ['M1 a second row per day', [
    ["        if (scan.exists) {\n          out.reason = 'row for ' + day + ' exists';", "        if (false) {\n          out.reason = 'row for ' + day + ' exists';"],
    ["    if (pdIbkrProp_(PD_CASH_SNAP_LAST_PROP) === day) {", "    if (false) {"]]],
  ['M2 append when cashCents is null', [
    ["    else if (plan.cashCents === null || plan.cashCents === undefined) {\n      out.reason = 'cash withheld ('", "    else if (false) {\n      out.reason = 'cash withheld ('"],
    ["  if (plan.cashCents === null || plan.cashCents === undefined) {\n    out.reason = 'cash withheld (' + (plan.cashNote || 'not certifiable') + ')';\n    return out;\n  }\n  var acct", "  if (false) {\n    out.reason = 'cash withheld (' + (plan.cashNote || 'not certifiable') + ')';\n    return out;\n  }\n  var acct"]]],
  ['M3 the note changing the controls', [
    ["      res.cashSnapNote = pdCashSnapApply_(snapMode, plan, fresh, cap.name, nowMs).note;",
     "      res.cashSnapNote = pdCashSnapApply_(snapMode, plan, fresh, cap.name, nowMs).note;\n      if (!fresh && body.controls) { body.controls[PD_IBKR_CASH_LABEL] = 72747; }"]]],
  ['M4 the retry re-arming itself', [
    ["  if (isRetry) return {arm: false, reason: 'a retry never re-arms'};", "  if (false) return {arm: false, reason: 'a retry never re-arms'};"]]],
  ['M5 a stale capture written as a row', [
    ["    if (!fresh) {\n      out.note = pdCashSnapLastKnownNote_(nowMs);\n      return out;\n    }", "    if (false) {\n      out.note = pdCashSnapLastKnownNote_(nowMs);\n      return out;\n    }"]]]
];

/* ------------------------------------------------------------ main */
const runs = [];
for (let i = 0; i < 3; i++) {
  const out = battery(newSource);
  runs.push(out);
  console.log('run ' + (i + 1) + ': ' + out.passed + '/' + out.total);
}
for (const line of runs[0].results) console.log(line);
const digests = runs.map((r) => sha256(JSON.stringify(r.dump)).slice(0, 16));
let ok = runs.every((r) => r.passed === r.total) && new Set(digests).size === 1;
let killed = 0;
for (const [label, pairs] of MUTANTS) {
  let verdict;
  try {
    const m = battery(mutate(newSource, pairs), {throwOnFail: true});
    verdict = 'SURVIVED (' + m.passed + '/' + m.total + ')';
  } catch (e) {
    ACTIVE_SOURCE = newSource;
    verdict = 'killed by ' + String(e.message).split(':')[0].slice(0, 60);
    killed++;
  }
  console.log('MUTANT ' + label + ': ' + verdict);
}
if (killed !== MUTANTS.length) ok = false;
console.log('V1518 T0-T9 x3 ' + (new Set(digests).size === 1 ? 'identical' : 'DIVERGENT') + ' digest ' + digests[0] +
  ' | mutants killed ' + killed + '/' + MUTANTS.length + ' | new SHA256 ' + sha256(newSource) +
  ' | base ' + (baseSource ? 'SHA256 ' + BASE_SHA.slice(0, 16) : 'NOT REACHABLE (identity sections skipped)'));
console.log((ok ? 'PASS ' : 'FAIL ') + runs[0].passed + '/' + runs[0].total);
process.exit(ok ? 0 : 1);
