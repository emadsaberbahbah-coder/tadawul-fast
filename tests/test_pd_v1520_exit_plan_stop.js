#!/usr/bin/env node
/* tests/test_pd_v1520_exit_plan_stop.js
 * Harness for apps_script/10_My_Portfolio.gs v1.5.20 [EXIT-PLAN-STOP] (owner, 16 Nov 2026: one final decision shown
 * the same on every page; Consistency_Check C9 read MISMATCH every hour because the Stop SAR column of
 * Portfolio_Decision was the engine's stop, not the Exit Plan's). Script Property PD_EXIT_PLAN_STOP, default ON:
 * the rendered ACTIONS rows show the Exit Plan's stop (the _Exit_Plan ledger) and name the engine's in the note.
 *
 * Dual-tree node harness over the REAL file: the delivered .gs and the v1.5.19 BASE (argv[3], or rebuilt from the
 * delivered file by reversing exactly the enumerated v1.5.20 edits of tests/js/pd_edits_v1520.json; SHA-256 pinned)
 * run in separate vm contexts with the Apps Script stubs of tests/test_pd_v1519_plain_block.js (copied; the world
 * gains the _Exit_Plan ledger). Every holding, price, stop and date is synthetic.
 * Sections:
 *   X0  compile both, version lockstep, base oracle (SHA-256 pinned); +6 functions +3 vars, 0 removed
 *   X1  GOLDEN NEGATIVE: PD_EXIT_PLAN_STOP off / 0 / false / NO with a ledger present -> the REAL
 *       refreshPortfolioDecision (B2, the POST body, every sheet write, Logger lines, _Run_Log) equals the base; one
 *       extra property read; the ledger is never read
 *   X2  on (absent / on / junk): only the Stop SAR and Advisor Note cells of the holdings in the ledger change; B2 and
 *       the POST body are the base's; the payload the action log reads keeps the engine's stop
 *   X3  no ledger tab, a ledger fault, an old day, a future day, lots that disagree, a row without a rate -> the
 *       base's cells; a SAR row without prices uses rate 1; a Date cell is read as its Riyadh day
 *   X4  the readers: consistency_check C9's conversion gives back the Exit Plan's stop; the header row is where it was
 *   X5  pure: pdExitStopDay_, pdExitStopsFromValues_, pdExitStopsApply_, pdExitStopOn_
 *   X6  pdSelfTest: one line added, core ok; every base line kept
 *   X7  ES5 / ASCII lint of the diff; 5 hunks whose reversal is the v1.5.19 file; WHY block; every prior WHY kept
 *   X8  mutants
 * The battery runs twice and the two digests must match; the last line is 'PASS k/k' (exit 1 on any failure).
 * Usage: node tests/test_pd_v1520_exit_plan_stop.js [new.gs] [v1.5.19.gs]
 */
'use strict';
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const ROOT = path.join(__dirname, '..');
const NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '10_My_Portfolio.gs');
const newSource = fs.readFileSync(NEW_PATH, 'utf8');
const sha256 = (s) => crypto.createHash('sha256').update(s).digest('hex');
const EDITS = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'pd_edits_v1520.json'), 'utf8'));
const V1519_SHA = '4f246be194a7ce5ade7a53e56381c19b44c20055608e33979584e47aeccfe584';   // v1.5.19 (origin/main 2b66b51)
function reverseEdits(src) {
  let out = src;
  for (const [cur, prev] of EDITS) {
    assert.equal(out.split(cur).length, 2, 'exactly one approved v1.5.20 hunk: ' + cur.split('\n')[0].slice(0, 80));
    out = out.replace(cur, () => prev);
  }
  assert.equal(sha256(out), V1519_SHA, 'v1.5.20 changes only its enumerated hunks (their reversal = the v1.5.19 file)');
  return out;
}
let baseSource = null, baseErr = '';
try {
  baseSource = process.argv[3] ? fs.readFileSync(process.argv[3], 'utf8') : reverseEdits(newSource);
  assert.equal(sha256(baseSource), V1519_SHA, 'v1.5.19 SHA-256');
} catch (e) { baseErr = String(e && e.message || e).split('\n')[0]; baseSource = null; }
if (!baseSource) { console.log('FAIL P0 base oracle: ' + baseErr); console.log('FAIL 0/1'); process.exit(1); }
let ACTIVE_SOURCE = newSource;
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
          chain: o.chain || null, top10: o.top10 || null, rows: o.rows || null, response: o.response || null,
          exit: o.exit || null, exitReads: []};   // v1.5.20 harness: the _Exit_Plan ledger grid (null = no tab)
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
  const rows = w.rows ? w.rows(w.holdRef) : holdRows(w.holdRef);   // v1.5.19 harness: a board per scenario
  const pd = {
    getParent() { return ss; },   // v1.5.20 harness: a real Sheet has it
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
      if (n === '_Exit_Plan') {   // v1.5.20 harness
        if (!w.exit) return null;
        if (w.exit === 'throw') throw new Error('synthetic ledger outage');
        const g = w.exit;
        return {getLastRow() { return g.length; }, getLastColumn() { return g[0].length; },
                getRange(r, c, nr, nc) { w.exitReads.push([r, c, nr, nc]); assert.ok(r >= 1 && r + nr - 1 <= g.length, 'ledger range');
                  return {getValues() { return g.slice(r - 1, r - 1 + nr).map((x) => x.slice(c - 1, c - 1 + nc)); }}; }};
      }
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
      return {getContentText() { return JSON.stringify(w.response || RESPONSE); }, getResponseCode() { return w.code; }};
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

/* ------------------------------------------------------------ v1.5.20 battery (synthetic figures) */
const NV = (newSource.match(/^var PD_VERSION = '(\d+\.\d+\.\d+)';$/m) || [])[1] || '?';
const GATE = 'PD_EXIT_PLAN_STOP';
const TODAY = new Date(START + RIYADH).toISOString().slice(0, 10);          // the Riyadh day of the run
const dayBack = (n) => new Date(START + RIYADH - n * DAY_MS).toISOString().slice(0, 10);
const LHEAD = ['Date', 'Run At (Riyadh)', 'Symbol', 'Lot', 'Entry', 'Shares', 'Last', 'Highest Close', 'ATR', 'R', 'Stop', 'Target', 'TP1 Reached', 'Action', 'Tighten', 'Bars To', 'Basis'];
const lrow = (day, sym, stop, lot) => [day, day + ' 08:41', sym, lot || '', 10, 5, 10, 10, 0.2, 0.5, stop, 12, '', 'HOLD', '', day, 10];
const LEDGER = () => [LHEAD, lrow(dayBack(1), 'SYNF.US', 13.9), lrow(dayBack(1), '9999.SR', 90), lrow(TODAY, 'SYNF.US', 14.1), lrow(TODAY, '9999.SR', 91.5)];
const ACT = (o) => Object.assign({action: 'HOLD', symbol: 'SYNF.US', name: 'Synthetic First', currency: 'USD', quantity: 174, price: 14.73,
  price_sar: 55.24, stop_sar: 50.81, tp1_sar: 60, tp2_sar: 66, advisor_note: 'HOLD: within the bands'}, o || {});
const RESP2 = () => Object.assign(plain(RESPONSE), {actions: [ACT(), ACT({symbol: '9999.SR', name: 'Synthetic Sukuk', currency: 'SAR', price: 96.3, price_sar: 96.3, stop_sar: 88.6, advisor_note: ''})]});
const mk = (o, props) => world(Object.assign({response: RESP2(), exit: LEDGER()}, o || {}, {props: Object.assign({}, props || {})}));
const vnorm = (x) => JSON.parse(JSON.stringify(x).split('[MP-DEC v' + NV + ']').join('[MP-DEC v1.5.19]'));
function gridOf(writes) {
  const g = {};
  for (const x of writes) {
    const [r, c] = x.at; const vals = x.values || [[x.value]];
    for (let i = 0; i < vals.length; i++) for (let j = 0; j < vals[i].length; j++) { const row = (g[r + i] = g[r + i] || []); while (row.length < c - 1 + j) row.push(''); row[c - 1 + j] = vals[i][j]; }
  }
  return g;
}
const H = ['Action', 'Symbol', 'Name', 'Sector', 'Ccy', 'Qty', 'Avg Cost', 'Price', 'Price SAR', 'MV SAR', 'Cost SAR', 'P&L SAR', 'P&L %', 'Weight %', 'Δ SAR', 'Δ Shares', 'Funds From', 'Stop SAR', 'TP1 SAR', 'TP2 SAR', 'ROI %', 'Engine ROI %', 'Rel', 'DQ', 'Conf', 'Advisor Note'];
const I_STOP = H.indexOf('Stop SAR'), I_NOTE = H.indexOf('Advisor Note');
function tableOf(r) {   // {headerRow, rows: {SYM: cells}}
  const g = gridOf(r.writes), out = {headerRow: -1, rows: {}};
  for (const k of Object.keys(g).map(Number).sort((a, b) => a - b)) {
    if (out.headerRow < 0 && g[k][0] === 'Action' && g[k][1] === 'Symbol') { out.headerRow = k; continue; }
    if (out.headerRow > 0 && g[k][1] && /\.(US|SR)$/.test(String(g[k][1]))) out.rows[g[k][1]] = g[k];
  }
  return out;
}
/* the delivered writes with the overlay cells put back to the base's values must equal the base writes */
function sameButCells(n, b, cells, tag) {
  const tn = tableOf(n), tb = tableOf(b);
  assert.equal(tn.headerRow, tb.headerRow, 'the header row is where it was ' + tag);
  assert.deepEqual(Object.keys(tn.rows), Object.keys(tb.rows), 'the same rows ' + tag);
  for (const sym of Object.keys(tb.rows)) for (let c = 0; c < H.length; c++) {
    if (cells[sym] && (c === I_STOP || c === I_NOTE)) continue;
    assert.deepEqual(tn.rows[sym][c], tb.rows[sym][c], sym + ' cell ' + H[c] + ' ' + tag);
  }
  assert.equal(n.writes.length, b.writes.length, 'the same number of writes ' + tag);
  for (let i = 0; i < b.writes.length; i++) assert.deepEqual(n.writes[i].at, b.writes[i].at, 'write ' + i + ' at the same range ' + tag);
}
function checkGolden(source) {
  for (const v of ['off', '0', 'false', ' NO ', 'Off']) {
    const n = run(mk(null, {[GATE]: v}), null, source), b = run(mk(null, {[GATE]: v}), null, baseSource);
    assert.deepEqual(vnorm(snapshot(n)), snapshot(b), 'gate ' + v + ': the refresh equals the base');
    assert.deepEqual(n.w.exitReads, [], 'gate ' + v + ': the ledger is never read');
    assert.equal(n.w.propReads.filter((k) => k === GATE).length, 1, 'one gate read');
    assert.deepEqual(n.w.propReads.filter((k) => k !== GATE), b.w.propReads, 'every other property read is the base\'s');
  }
  const e = run(mk({code: 500}, {}), null, source), eb = run(mk({code: 500}, {}), null, baseSource);
  assert.deepEqual(vnorm(snapshot(e)), snapshot(eb), 'an HTTP error renders nothing: the base');
}
function checkOn(source) {
  const b = run(mk(), null, baseSource);
  let last = null;
  for (const props of [{}, {[GATE]: 'on'}, {[GATE]: 'yes'}, {[GATE]: ''}]) {
    const tag = JSON.stringify(props);
    const w = mk(null, props);
    let seen = null;
    const src = source + '\n;(function () { var real = pdRenderPayload_; pdRenderPayload_ = function (s, p) { __seen = p; return real.apply(this, arguments); }; })();';
    const n = run(w, 'none', src); n.ctx.__seen = null;
    n.ctx.refreshPortfolioDecision(); seen = n.ctx.__seen;
    const r = run(mk(null, props), null, source);
    const t = tableOf(r), tb = tableOf(b);
    assert.equal(t.rows['SYNF.US'][I_STOP], Math.round(14.1 * (55.24 / 14.73) * 100) / 100, 'USD row: stop x Price SAR / Price ' + tag);
    assert.equal(t.rows['SYNF.US'][I_NOTE], 'HOLD: within the bands | Stop SAR: Exit Plan 14.10 USD of ' + TODAY + '; engine stop 50.81 SAR', 'USD note ' + tag);
    assert.equal(t.rows['9999.SR'][I_STOP], 91.5, 'SAR row ' + tag);
    assert.equal(t.rows['9999.SR'][I_NOTE], 'Stop SAR: Exit Plan 91.50 SAR of ' + TODAY + '; engine stop 88.60 SAR', 'SAR note ' + tag);
    assert.equal(tb.rows['SYNF.US'][I_STOP], 50.81, 'GOLDEN NEGATIVE: the base shows the engine stop');
    sameButCells(r, b, {'SYNF.US': 1, '9999.SR': 1}, tag);
    assert.equal(r.b2, b.b2.split('[MP-DEC v1.5.19]').join('[MP-DEC v' + NV + ']'), 'B2 ' + tag);
    assert.deepEqual(r.post, b.post, 'the POST body ' + tag);
    assert.deepEqual(r.w.runLog, b.w.runLog, '_Run_Log ' + tag);
    assert.deepEqual(vnorm(r.logs.filter((l) => l.indexOf('exit plan stop on') < 0)), b.logs, 'every other Logger line ' + tag);
    assert.ok(r.logs.some((l) => l === '[MP-DEC v' + NV + '] exit plan stop on 2 of 2 row(s)'), 'the Logger line ' + tag);
    assert.ok(seen && seen.actions[0].stop_sar === 50.81 && seen.actions[0].advisor_note === 'HOLD: within the bands' && seen.actions[1].stop_sar === 88.6, 'the payload keeps the engine\'s stop and note (the action log reads it) ' + tag);
    assert.deepEqual(r.w.exitReads, [[1, 1, 1, 17], [2, 1, 4, 17]], 'the header and the newest rows are read once ' + tag);
    last = r;
  }
  return last;
}
function checkEdges(source) {
  const b = run(mk(), null, baseSource), same = (o, why) => {
    const r = run(mk(o, {}), null, source);
    assert.deepEqual(tableOf(r).rows, tableOf(b).rows, why + ': the base\'s cells');
    assert.deepEqual(r.writes, vnormWrites(b.writes), why + ': every write');
  };
  const vnormWrites = (x) => x;
  same({exit: null}, 'no ledger tab');
  same({exit: 'throw'}, 'a ledger fault');
  same({exit: [LHEAD]}, 'an empty ledger');
  same({exit: [LHEAD, lrow(dayBack(5), 'SYNF.US', 14.1), lrow(dayBack(5), '9999.SR', 91.5)]}, 'a ledger 5 days old');
  same({exit: [LHEAD, lrow(dayBack(-1), 'SYNF.US', 14.1), lrow(dayBack(-1), '9999.SR', 91.5)]}, 'a future day');
  same({exit: [LHEAD, lrow(TODAY, 'SYNF.US', 14.1, 'a'), lrow(TODAY, 'SYNF.US', 14.4, 'b'), lrow(TODAY, 'OTHER.US', 3)]}, 'lots that disagree');
  same({exit: [['x', 'y'], ['a', 'b']]}, 'no header');
  const old4 = tableOf(run(mk({exit: [LHEAD, lrow(dayBack(4), 'SYNF.US', 14.1)]}, {}), null, source));
  assert.equal(old4.rows['SYNF.US'][I_STOP], Math.round(14.1 * (55.24 / 14.73) * 100) / 100, 'four days old is still used (a weekend and a holiday)');
  assert.ok(String(old4.rows['SYNF.US'][I_NOTE]).indexOf(' of ' + dayBack(4) + ';') > 0, 'and its day is named');
  assert.equal(old4.rows['9999.SR'][I_STOP], 88.6, 'a holding without a ledger row keeps the engine stop');
  const norate = Object.assign(plain(RESPONSE), {actions: [ACT({price: null, price_sar: null}), ACT({symbol: '9999.SR', currency: 'SAR', price: null, price_sar: null, stop_sar: null, advisor_note: null})]});
  const nr = tableOf(run(mk({response: norate}, {}), null, source));
  assert.equal(nr.rows['SYNF.US'][I_STOP], 50.81, 'a USD row without prices is unchanged');
  assert.equal(nr.rows['9999.SR'][I_STOP], 91.5, 'a SAR row without prices uses rate 1');
  assert.equal(nr.rows['9999.SR'][I_NOTE], 'Stop SAR: Exit Plan 91.50 SAR of ' + TODAY, 'no engine stop to name');
  const dated = [LHEAD, [riyadhMidnight(TODAY), 'x', 'SYNF.US', '', 10, 5, 10, 10, 0.2, 0.5, 14.1, 12, '', 'HOLD', '', '', 10]];
  assert.equal(tableOf(run(mk({exit: dated}, {}), null, source)).rows['SYNF.US'][I_STOP], Math.round(14.1 * (55.24 / 14.73) * 100) / 100, 'a Date cell is its Riyadh day');
  const many = [LHEAD]; for (let i = 0; i < 500; i++) many.push(lrow(dayBack(9), 'F' + i + '.US', 1)); many.push(lrow(TODAY, 'SYNF.US', 14.1));
  const mr = run(mk({exit: many}, {}), null, source);
  assert.deepEqual(mr.w.exitReads, [[1, 1, 1, 17], [103, 1, 400, 17]], 'a long ledger: the header and the newest 400 rows');
  assert.equal(tableOf(mr).rows['SYNF.US'][I_STOP], Math.round(14.1 * (55.24 / 14.73) * 100) / 100);
}
function checkReaders(source) {
  const r = run(mk(), null, source), t = tableOf(r);
  const row = t.rows['SYNF.US'], ps = row[I_STOP], px = row[H.indexOf('Price')], pxs = row[H.indexOf('Price SAR')];
  const back = ps * px / pxs;                                    // scripts/consistency_check.py C9
  assert.ok(Math.abs(back - 14.1) / 14.1 < 0.001, 'C9 reads back the Exit Plan stop: ' + back);
  const g = gridOf(r.writes), head = g[t.headerRow];
  assert.deepEqual(head.slice(0, H.length), H, 'the 26 headers unchanged');
  assert.equal(String(row[I_NOTE]).indexOf('\n'), -1); assert.ok(/^[\x20-\x7e]*$/.test(String(row[I_NOTE])), 'the note is one ASCII line');
}
function ctxOf(source, props, throwIt) {
  const w = world({props: props || {}});
  const r = run(w, 'none', source);
  if (throwIt) r.ctx.PropertiesService = {getScriptProperties() { throw new Error('down'); }};
  return r.ctx;
}
function checkPure(source) {
  const c = ctxOf(source, {});
  assert.equal(c.pdExitStopCoreOk_(), true, 'the embedded battery');
  const f = c.pdExitStopsFromValues_;
  const v = [['Date', 'Run At (Riyadh)', 'Symbol', 'Lot', 'Stop'], ['2026-11-13', 'x', 'aaa.us ', '', 9.2], ['2026-11-16', 'y', 'AAA.US', '', '9.5'],
             ['2026-11-16', 'y', 'ZERO.US', '', 0], ['2026-11-16', 'y', 'TXT.US', '', 'n/a'], ['', 'y', 'NOD.US', '', 4], ['2026-11-16', 'y', '', '', 4]];
  assert.deepEqual(plain(f(v, '2026-11-16', 4)), {'AAA.US': {stop: 9.5, day: '2026-11-16'}});
  assert.deepEqual(plain(f(v, '2026-11-20', 4)), {'AAA.US': {stop: 9.5, day: '2026-11-16'}}, 'four days');
  assert.deepEqual(plain(f(v, '2026-11-21', 4)), {}, 'five days');
  assert.deepEqual(plain(f(v, '2026-11-15', 4)), {}, 'a ledger day after today is not used');
  assert.deepEqual(plain(f(v, 'junk', 4)), {}); assert.deepEqual(plain(f(null, '2026-11-16', 4)), {}); assert.deepEqual(plain(f([], '2026-11-16', 4)), {});
  assert.deepEqual(plain(f([['Date', 'Symbol', 'Stop'], ['2026-11-16', 'A.US', 5], ['2026-11-16', 'A.US', 5.00001]], '2026-11-16', 4)), {'A.US': {stop: 5, day: '2026-11-16'}}, 'equal lots are one stop');
  assert.equal(c.pdExitStopDay_('2026-11-16 08:41'), '2026-11-16'); assert.equal(c.pdExitStopDay_(null), ''); assert.equal(c.pdExitStopDay_(20261116), '');
  assert.equal(c.pdExitStopDay_(new Date(Date.UTC(2026, 10, 15, 21, 0))), '2026-11-16'); assert.equal(c.pdExitStopDay_(new Date(NaN)), '');
  const res = c.pdExitStopsApply_([], {}); assert.deepEqual(plain(res), {applied: 0, of: 0});
  assert.deepEqual(plain(c.pdExitStopsApply_(null, null)), {applied: 0, of: 0});
  for (const [val, want] of [[undefined, true], ['', true], ['on', true], ['junk', true], ['off', false], [' OFF ', false], ['0', false], ['false', false], ['No', false]]) {
    assert.equal(ctxOf(source, val === undefined ? {} : {[GATE]: val}).pdExitStopOn_(), want, 'gate ' + JSON.stringify(val));
  }
  assert.equal(ctxOf(source, {[GATE]: 'off'}, true).pdExitStopOn_(), true, 'a property fault reads on');
}
function checkSelfTest(source) {
  for (const [props, word] of [[{}, 'on'], [{[GATE]: 'off'}, 'off']]) {
    const lines = String(ctxOf(source, props).pdSelfTest()).split('\n'), base = String(ctxOf(baseSource, props).pdSelfTest()).split('\n');
    const line = 'exit plan stop (PD_EXIT_PLAN_STOP): ' + word + ' (the Stop SAR column shows the Exit Plan\'s stop; the engine\'s is in the Advisor Note) | core: ok';
    const i = lines.indexOf(line);
    assert.ok(i > 0, 'the line: ' + lines.filter((l) => /exit plan stop/.test(l)).join(' / '));
    assert.deepEqual(vnorm(lines.filter((l, k) => k !== i)), base, 'every other line is the base\'s (' + word + ')');
  }
}
function checkLint() {
  assert.equal(newSource.indexOf('\r'), -1, 'LF only');
  assert.equal(EDITS.length, 5, 'five hunks');
  const names = (s, re) => (s.match(re) || []).map((m) => m.replace(/^(function|var)\s+/, '').replace(/[\s(=].*$/, ''));
  const fb = names(baseSource, /^function\s+[A-Za-z_$][\w$]*\s*\(/mg), fn = names(newSource, /^function\s+[A-Za-z_$][\w$]*\s*\(/mg);
  for (const f of fb) assert.ok(fn.indexOf(f) >= 0, 'removed ' + f);
  assert.deepEqual(fn.filter((f) => fb.indexOf(f) < 0).sort(), ['pdExitStopCoreOk_', 'pdExitStopDay_', 'pdExitStopOn_', 'pdExitStopsApply_', 'pdExitStopsFromValues_', 'pdExitStopsRead_']);
  const vb = names(baseSource, /^var\s+[A-Za-z_$][\w$]*/mg), vn = names(newSource, /^var\s+[A-Za-z_$][\w$]*/mg);
  for (const v of vb) assert.ok(vn.indexOf(v) >= 0, 'var removed ' + v);
  assert.deepEqual(vn.filter((v) => vb.indexOf(v) < 0).sort(), ['PD_EXIT_STOP_MAX_DAYS', 'PD_EXIT_STOP_PROP', 'PD_EXIT_STOP_SHEET']);
  for (const [cur, prev] of EDITS) {
    const added = cur.split('\n').filter((l) => prev.split('\n').indexOf(l) < 0);
    for (const l of added) assert.ok(/^[\x00-\x7f]*$/.test(l), 'ASCII: ' + l.slice(0, 80));
    const code = added.filter((l) => !/^\s*(\*|\/\*|\/\/)/.test(l)).map((l) => l.replace(/\/\*.*?\*\//g, '').replace(/\/\/.*$/, '')).join('\n');
    assert.ok(!/=>|`|\blet\s|\bconst\s|\bclass\s/.test(code), 'ES5: ' + code.slice(0, 100));
  }
  const h0 = newSource.indexOf(' * v1.5.20 (2026-11-16)'), h1 = newSource.indexOf(' * v1.5.19 (2026-11-03)');
  assert.ok(h0 > 0 && h1 > h0, 'the WHY block is newest-first');
  const baseHead = baseSource.slice(0, baseSource.indexOf('*/'));
  for (const l of baseHead.split('\n')) if (!/^ \* Version: /.test(l)) assert.ok(newSource.indexOf(l) >= 0, 'prior WHY line kept: ' + l.slice(0, 80));
}
function battery() {
  let passed = 0, total = 0;
  const results = [], digest = [];
  function section(label, fn) {
    total++;
    try { fn(); passed++; results.push('PASS ' + label); }
    catch (e) { results.push('FAIL ' + label + ' ... ' + String(e && e.message || e).split('\n').slice(0, 4).join(' ')); }
  }
  section('X0 compile both, version lockstep 1.5.20, base oracle (SHA-256 pinned, history helper)', () => {
    new vm.Script(newSource, {filename: 'new.gs'}); new vm.Script(baseSource, {filename: 'base.gs'});
    assert.equal(sha256(baseSource), V1519_SHA);
    assert.equal(sha256(require(path.join(__dirname, 'js', 'pd_history.js')).v1519Source(newSource)), V1519_SHA, 'the history helper');
    assert.equal(NV, '1.5.20'); assert.ok(/^ \* Version: 1\.5\.20$/m.test(newSource), 'header lockstep');
    assert.ok(/^var PD_VERSION = '1\.5\.19';$/m.test(baseSource) && /^ \* Version: 1\.5\.19$/m.test(baseSource), 'base is 1.5.19');
    assert.equal(baseSource.indexOf('PD_EXIT_PLAN_STOP'), -1);
  });
  section('X1 GOLDEN NEGATIVE: gate off / 0 / false / NO / Off with a ledger present -> the REAL refresh equals the base; one extra property read; the ledger is never read; an HTTP error = the base', () => { checkGolden(ACTIVE_SOURCE); });
  section('X2 on (absent / on / junk / blank): only Stop SAR and Advisor Note of the holdings in the ledger change; B2, the POST body, _Run_Log and the payload are the base\'s', () => { const n = checkOn(ACTIVE_SOURCE); digest.push(sha256(JSON.stringify(n.writes))); });
  section('X3 no tab, a fault, an empty or headerless ledger, 5 days old, a future day, lots that disagree -> the base\'s cells; 4 days old is used; SAR rate 1; a Date cell; a long ledger reads its newest 400 rows', () => { checkEdges(ACTIVE_SOURCE); });
  section('X4 readers: consistency_check C9 reads back the Exit Plan stop; the 26 headers and the header row are unchanged; the note is one ASCII line', () => { checkReaders(ACTIVE_SOURCE); });
  section('X5 pure: the ledger map (newest day, age window, bad rows, equal lots), the day reader, the apply, the gate spellings, a property fault reads on', () => { checkPure(ACTIVE_SOURCE); });
  section('X6 pdSelfTest: one line (on / off), core ok; every other line is the base\'s', () => { checkSelfTest(ACTIVE_SOURCE); });
  section('X7 ES5 / ASCII lint of the v1.5.20 diff; +6 functions +3 vars, 0 removed; 5 hunks; WHY block newest-first; every prior WHY kept', () => { checkLint(); });
  return {passed: passed, total: total, results: results, digest: sha256(JSON.stringify(digest)).slice(0, 16)};
}
const MUTANTS = [
  ['off does not disarm', "    return !/^(0|off|false|no)$/i.test(String(PropertiesService.getScriptProperties()\n        .getProperty(PD_EXIT_STOP_PROP)", "    return !/^(never)$/i.test(String(PropertiesService.getScriptProperties()\n        .getProperty(PD_EXIT_STOP_PROP)", checkGolden],
  ['the payload mutated', "      var esRes = pdExitStopsApply_(aRows, pdExitStopsRead_(sheet.getParent()));", "      var esMap = pdExitStopsRead_(sheet.getParent()); for (var q = 0; q < actions.length; q++) if (esMap[actions[q].symbol]) actions[q].stop_sar = 1;\n      var esRes = pdExitStopsApply_(aRows, esMap);", checkOn],
  ['no rate applied', "    row[iStop] = Math.round(e.stop * rate * 100) / 100;", "    row[iStop] = Math.round(e.stop * 100) / 100;", checkOn],
  ['the engine stop not named', "'; engine stop ' + (Math.round(Number(was) * 100) / 100).toFixed(2) + ' SAR' : '');", "'' : '');", checkOn],
  ['an old ledger used', "    if (!(age >= 0 && age <= maxDays)) continue;", "    if (!(age >= 0)) continue;", checkEdges],
  ['a future day used', "    if (!(age >= 0 && age <= maxDays)) continue;", "    if (!(age <= maxDays)) continue;", checkEdges],
  ['lots that disagree used', "    if (same) out[k] = { stop: by[k].stops[0], day: by[k].day };", "    out[k] = { stop: by[k].stops[0], day: by[k].day };", checkEdges],
  ['an older day wins', "    if (!by[sym] || day > by[sym].day) by[sym] = { day: day, stops: [stop] };", "    if (!by[sym] || day < by[sym].day) by[sym] = { day: day, stops: [stop] };", checkOn],
  ['a USD row without a rate changed', "(ccy === 'SAR' ? 1 : null);", "(1);", checkEdges],
  ['a ledger fault thrown', "  } catch (eEr) {}\n  return {};", "  } catch (eEr) { throw eEr; }\n  return {};", (s) => { const src = s.replace("    } catch (eEs) {}\n  }\n  var aT", "    } catch (eEs) { throw eEs; }\n  }\n  var aT"); checkEdges(src); }],
  ['the whole ledger read', "    var n = Math.min(last - 1, 400);", "    var n = last - 1;", checkEdges]
];

/* ------------------------------------------------------------ main */
const runs = [];
for (let i = 0; i < 2; i++) { runs.push(battery()); console.log('run ' + (i + 1) + ': ' + runs[i].passed + '/' + runs[i].total + ' digest ' + runs[i].digest); }
for (const line of runs[0].results) console.log(line);
let ok = runs.every((r) => r.passed === r.total) && runs[0].digest === runs[1].digest;
let killed = 0, counted = 0;
for (const [label, from, to, check] of MUTANTS) {
  counted++;
  let verdict;
  try {
    assert.equal(newSource.split(from).length, 2, 'mutant anchor occurs once: ' + label);
    const m = newSource.replace(from, () => to);
    try { check(m); verdict = 'SURVIVED'; } catch (e) { verdict = 'killed'; killed++; }
  } catch (e) { verdict = 'ANCHOR ' + String(e.message).slice(0, 80); }
  console.log('MUTANT ' + label + ': ' + verdict);
}
if (killed !== counted) ok = false;
console.log('PD V1520 X0-X7 x2 ' + (runs[0].digest === runs[1].digest ? 'identical' : 'DIVERGENT') + ' digest ' + runs[0].digest + ' | mutants killed ' + killed + '/' + counted);
console.log((ok ? 'PASS ' : 'FAIL ') + runs[0].passed + '/' + runs[0].total);
process.exit(ok ? 0 : 1);
