#!/usr/bin/env node
/* tests/test_pd_v1519_plain_block.js
 * Harness for apps_script/10_My_Portfolio.gs v1.5.19 [PLAIN-BLOCK] (owner, Tue 3 Nov 2026 12:14 Riyadh: 'still my
 * portfolio showing Block'): Script Property PD_PLAIN_REASONS = on (default OFF) writes ONE plain line between the
 * ACTIONS section title and its header row when a holding is BLOCK -- why, and the one step that clears it, with a
 * ticker-change hint (a page row no IBKR position links to and an IBKR position no row represents, one to one by
 * an equal share count: the INDV.US -> SUPN shape of 3 Nov).
 *
 * Dual-tree node harness over the REAL file: the delivered .gs source and the v1.5.18 BASE (argv[3], or rebuilt
 * from the delivered file by reversing exactly the enumerated v1.5.19 edits of tests/js/pd_edits_v1519.json; its
 * SHA-256 is pinned) run in separate vm contexts with the Apps Script stubs of
 * tests/test_my_portfolio_v1518_cash_snapshot.js (copied verbatim; the board and the backend response are
 * parameters here). Every holding, amount, alias and file name is synthetic.
 * Sections:
 *   P0  compile both, version lockstep, base oracle (SHA-256 pinned); the base lacks the new names
 *   P1  GOLDEN NEGATIVE: the gate absent / off / junk, on six scenarios (enforce with the ticker-change shape, the
 *       stale-price shape, observe, IBKR off, a clean board, an HTTP error) -> the REAL refreshPortfolioDecision
 *       (B2, the POST body, every sheet write, Logger lines, _Run_Log rows, triggers) equals the base; the only
 *       extra read is PD_PLAIN_REASONS once per rendered refresh; the pdIbkrApply_ result keys are the base's
 *   P2  gate on, the 3 Nov ticker-change shape: the exact line at row 14; every other write is the base's, the
 *       rows from 14 on shifted down by one; B2 and the POST body unchanged
 *   P3  gate on, the stale-price shape (after the ledger rename): 'NEXT: wait for a fresh price ..'
 *   P4  gate on: a clean board writes no line; an HTTP error writes none; IBKR off -> no ticker hint
 *   P5  pdIbkrDrift_ / pdPlainCodes_ / pdPlainClause_ / pdPlainLine_ (pure): pairing rules, parsing, the NEXT
 *       priority, ASCII, and never the words the readers use to find the ACTIONS header
 *   P6  readers: run_daily_brief's _find_header rule (substring), tfb_acceptance / tfb_export_audit / the
 *       21_v_portfolio_decision.sql rule (cells) all still find the real header row with the line present
 *   P7  pdSelfTest: the v1.5.18 report while off; one 'core: ok' line while on
 *   P8  ES5 / ASCII lint of the v1.5.19 diff; inventory (7 functions + 2 vars added, 0 removed); the replaced base
 *       lines pinned; 8 hunks whose reversal is the v1.5.18 file; WHY block; every prior WHY kept
 *   P9  mutants: each mutation of the delivered file fails at least one section check
 * The battery runs twice and the two digests must match; the last line is 'PASS k/k' (exit 1 on any failure).
 * Usage: node tests/test_pd_v1519_plain_block.js [new.gs] [v1.5.18.gs]
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
const EDITS = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'pd_edits_v1519.json'), 'utf8'));
const V1518_SHA = '62b15eaa5eda541b867cd5fd667b71bbcd15c53732a730636ce0056d70f7f973';   // v1.5.18 (origin/main 8478029d)
function reverseEdits(src) {
  let out = src;
  for (const [cur, prev] of EDITS) {
    assert.equal(out.split(cur).length, 2, 'exactly one approved v1.5.19 hunk: ' + cur.split('\n')[0].slice(0, 80));
    out = out.replace(cur, () => prev);
  }
  assert.equal(sha256(out), V1518_SHA, 'v1.5.19 changes only its enumerated hunks (their reversal = the v1.5.18 file)');
  return out;
}
let baseSource = null, baseErr = '';
try {
  baseSource = process.argv[3] ? fs.readFileSync(process.argv[3], 'utf8') : reverseEdits(newSource);
  assert.equal(sha256(baseSource), V1518_SHA, 'v1.5.18 SHA-256');
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
          chain: o.chain || null, top10: o.top10 || null, rows: o.rows || null, response: o.response || null};
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

/* ------------------------------------------------------------ the boards of 3 Nov (synthetic amounts) */
const NV = (newSource.match(/^var PD_VERSION = '(\d+\.\d+\.\d+)';$/m) || [])[1] || '?';
const GATE = 'PD_PLAIN_REASONS';
function holdRow(sym, name, qty, ref) {
  const acq = iso(ref - 120000), quote = iso(ref - 150000);
  return [sym, name, 'Healthcare', 'NYSE/NASDAQ', 'USD', qty, 30.5, 28.15, 35, 34, 20, 72, 100, 'Moderate', 30, 90000, 'HOLD',
          'INVESTABLE', 'eodhd', acq, 'acquisition_status:success; acquisition_acquired_at:' + acq + '; acquisition_provider:eodhd; acquisition_quote_asof:' + quote];
}
const SUKUK = (ref) => holdRows(ref)[1];
const BOARD_DRIFT = (ref) => [holdRows(ref)[0], holdRow('INDV.US', 'Indivior', 95, ref), SUKUK(ref)];
const BOARD_RENAMED = (ref) => [holdRows(ref)[0], holdRow('SUPN.US', 'Supernus (ex-Indivior)', 95, ref), SUKUK(ref)];
const capWith = (extra) => capture({mutate: (c) => { c.positions.positions.push(extra); }});
const SUPN_POS = {contract_id: 849142054, contract_description: 'SUPN', position: 95, market_price: 28.15, currency: 'USD', asset_class: 'STK'};
const NOTE_CU = 'BLOCKED; RECONCILIATION_REQUIRED: custody_unknown, unrepresented_positions | [custody-unverified]';
const NOTE_C = 'BLOCKED; RECONCILIATION_REQUIRED: custody_unknown';
const NOTE_Q = 'BLOCKED; RECONCILIATION_REQUIRED: holding_quote_unverified';
const NOTE_M = 'BLOCKED; RECONCILIATION_REQUIRED: missing_evidence | [custody-unverified]';
const resp = (actions) => Object.assign(plain(RESPONSE), {actions: actions});
const RESP_DRIFT = resp([{action: 'BLOCK', symbol: 'SYNF.US', name: 'Synthetic First', advisor_note: NOTE_CU},
                         {action: 'BLOCK', symbol: 'INDV.US', name: 'Indivior', advisor_note: NOTE_C, detail: {reconciliation_status: 'custody_unknown'}}]);
const RESP_QUOTE = resp([{action: 'BLOCK', symbol: 'SYNF.US', name: 'Synthetic First', advisor_note: NOTE_Q},
                         {action: 'BLOCK', symbol: 'SUPN.US', name: 'Supernus (ex-Indivior)', advisor_note: NOTE_Q}]);
const RESP_MISSING = resp([{action: 'BLOCK', symbol: 'SYNF.US', name: 'Synthetic First', advisor_note: NOTE_M}]);
const LINE_DRIFT = 'WHY BLOCKED (2 of 2 holdings): a My_Portfolio row has no matching IBKR position (ticker or currency differ) (custody_unknown); ' +
  'IBKR holds a position that is not on My_Portfolio (unrepresented_positions). NEXT: IBKR lists SUPN where My_Portfolio has INDV.US (same 95 shares) ' +
  '-- likely a ticker change (merger or rename): put the new ticker on that row in _Portfolio_CostBasis, then refresh.';
const LINE_QUOTE = 'WHY BLOCKED (2 of 2 holdings): a holding has no current price (no fresh market quote) (holding_quote_unverified). NEXT: wait for a fresh ' +
  'price on every holding (a renamed or delisted listing keeps an old price until the feed switches); funding stays withheld until then.';
const LINE_MISSING = 'WHY BLOCKED (1 of 1 holdings): no fresh IBKR capture went with this request (captures are hourly; each is valid for 14 minutes) ' +
  '(missing_evidence). NEXT: refresh within 14 minutes of the next IBKR capture (about :05 past each hour).';
const SCEN = {
  drift: {props: ENFORCE, rows: BOARD_DRIFT, files: [driveFile(capWith(SUPN_POS))], response: RESP_DRIFT},
  quote: {props: ENFORCE, rows: BOARD_RENAMED, files: [driveFile(capWith(SUPN_POS))], response: RESP_QUOTE},
  observe: {props: OBSERVE_IBKR, rows: BOARD_DRIFT, files: [driveFile(capWith(SUPN_POS))], response: RESP_DRIFT},
  ibkrOff: {props: {}, rows: BOARD_DRIFT, files: [driveFile(capWith(SUPN_POS))], response: RESP_MISSING},
  clean: {props: ENFORCE},
  http500: {props: ENFORCE, rows: BOARD_DRIFT, files: [driveFile(capWith(SUPN_POS))], response: RESP_DRIFT, code: 500}
};
const scen = (k, gate) => world(Object.assign({}, SCEN[k], {props: Object.assign({}, SCEN[k].props, gate === undefined ? {} : {[GATE]: gate})}));
const vnorm = (lines) => lines.map((l) => String(l).split('[MP-DEC v1.5.18]').join('[MP-DEC v' + NV + ']'));
const ZONE = 13, PLAIN_ROW = 14;
/* the base writes as the delivered file must write them with the line present: rows >= 14 shifted down by one, the
 * line inserted right after the ACTIONS title */
function expectedWrites(baseWrites, line) {
  const out = [];
  for (const x of baseWrites) {
    const y = plain(x);
    if (y.at[0] >= PLAIN_ROW) y.at[0] += 1;
    out.push(y);
    if (x.at[0] === ZONE && x.at[1] === 1 && /^ACTIONS /.test(String(x.value))) out.push({at: [PLAIN_ROW, 1, 1, 26], value: line});
  }
  return out;
}
/* the written grid (setValue / setValues) -> {row: [cells]} */
function gridOf(writes) {
  const g = {};
  for (const x of writes) {
    const [r, c] = x.at;
    const vals = x.values || [[x.value]];
    for (let i = 0; i < vals.length; i++) for (let j = 0; j < vals[i].length; j++) {
      const row = (g[r + i] = g[r + i] || []);
      while (row.length < c - 1 + j) row.push('');
      row[c - 1 + j] = vals[i][j];
    }
  }
  return g;
}
/* the readers' header rules */
const briefHeader = (rows) => { for (let i = 0; i < Math.min(40, rows.length); i++) { const cells = rows[i].map((c) => String(c === null || c === undefined ? '' : c).toLowerCase());
  if (['action', 'symbol'].every((k) => cells.some((c) => c === k || c.indexOf(k) >= 0))) return i; } return -1; };   // run_daily_brief _find_header
const acceptanceHeader = (rows) => rows.findIndex((r) => r.indexOf('Action') >= 0 && r.indexOf('Symbol') >= 0);    // tfb_acceptance / tfb_export_audit
const sqlHeader = (rows) => rows.findIndex((r) => r.indexOf('Action') >= 0 && r.indexOf('Symbol') >= 0 && r.indexOf('Qty') >= 0);   // 21_v_portfolio_decision.sql
function rowsOf(writes) { const g = gridOf(writes), out = []; const max = Math.max(...Object.keys(g).map(Number)); for (let r = 1; r <= max; r++) out.push((g[r] || []).map((v) => (v === undefined ? '' : v))); return out; }

function checkGolden(src) {
  const JUNK = [undefined, 'off', '', 'yes', '1', 'true', 'onn', 'enforce', 'on x'];
  for (const key of Object.keys(SCEN)) {
    for (const g of JUNK) {
      const label = key + ' gate ' + JSON.stringify(g);
      const n = run(scen(key, g), null, src), bw = scen(key, g), b = run(bw, null, baseSource);
      assert.equal(n.ret, b.ret, label + ' ret');
      assert.equal(n.b2, b.b2, label + ' B2');
      assert.deepEqual(n.post, b.post, label + ' POST');
      assert.deepEqual(n.writes, b.writes, label + ' writes');
      assert.deepEqual(n.logs, vnorm(b.logs), label + ' Logger');
      assert.deepEqual(n.w.runLog, bw.runLog, label + ' _Run_Log');
      assert.deepEqual(n.w.triggers.map((t) => t.getHandlerFunction()), bw.triggers.map((t) => t.getHandlerFunction()), label + ' triggers');
      const reads = n.w.propReads.filter((k) => k === GATE).length;
      assert.equal(reads, key === 'http500' ? 0 : 1, label + ': one PD_PLAIN_REASONS read per rendered refresh');
      assert.deepEqual(n.w.propReads.filter((k) => k !== GATE), bw.propReads, label + ': every other read is the base read');
    }
  }
  // the apply result: the base keys; the drift hint is non-enumerable
  const body = () => ({rows: [{Symbol: 'SYNF.US', Currency: 'USD', Quantity: 174}, {Symbol: 'INDV.US', Currency: 'USD', Quantity: 95}], controls: {}, fx_rates: {USD: 3.75}});
  const nr = run(scen('drift'), 'none', src).ctx.pdIbkrApply_('enforce', body()), br = run(scen('drift'), 'none', baseSource).ctx.pdIbkrApply_('enforce', body());
  assert.deepEqual(Object.keys(nr).sort(), Object.keys(br).sort(), 'pdIbkrApply_ keys = the base keys');
  assert.equal(JSON.stringify(Object.assign({}, nr, {note: ''})), JSON.stringify(Object.assign({}, br, {note: ''})), 'the serialised result = the base');
  assert.equal(nr.note, br.note, 'the status-line fragment = the base');
}
function checkDrift(src) {
  const n = run(scen('drift', 'on'), null, src), b = run(scen('drift'), null, baseSource);
  const line = n.writes.find((x) => x.at[0] === PLAIN_ROW && x.at[1] === 1 && x.value !== undefined);
  assert.ok(line, 'a write at row 14');
  assert.equal(line.value, LINE_DRIFT, 'the line');
  assert.deepEqual(n.writes, expectedWrites(b.writes, LINE_DRIFT), 'every other write = the base, rows from 14 shifted down by one');
  assert.equal(n.b2, b.b2, 'B2 unchanged'); assert.deepEqual(n.post, b.post, 'the POST body unchanged');
  assert.deepEqual(n.logs, vnorm(b.logs), 'Logger unchanged');
  return n;
}
function checkQuote(src) {
  const n = run(scen('quote', ' ON '), null, src), b = run(scen('quote'), null, baseSource);
  assert.deepEqual(n.writes, expectedWrites(b.writes, LINE_QUOTE), 'the stale-price line, every other write shifted');
  assert.ok(b.b2.indexOf('linked 2/2') > 0 || b.b2.indexOf('linked') > 0, 'the renamed board links: ' + b.b2);
}
function checkQuiet(src) {
  for (const key of ['clean', 'http500']) {
    const n = run(scen(key, 'on'), null, src), b = run(scen(key), null, baseSource);
    assert.deepEqual(n.writes, b.writes, key + ': no line'); assert.equal(n.b2, b.b2, key + ' B2');
  }
  const off = run(scen('ibkrOff', 'on'), null, src), bOff = run(scen('ibkrOff'), null, baseSource);
  assert.deepEqual(off.writes, expectedWrites(bOff.writes, LINE_MISSING), 'IBKR off: the reason line without a ticker hint');
  const obs = run(scen('observe', 'on'), null, src), bObs = run(scen('observe'), null, baseSource);
  assert.deepEqual(obs.writes, expectedWrites(bObs.writes, LINE_DRIFT), 'observe IBKR mode: the hint comes from the same plan');
}
function checkPure(src) {
  const c = run(world({props: {}}), 'none', src).ctx;
  const plan = (unlinked, unrep, positions, links) => ({ok: true, unlinked, unrepresented: unrep,
    packet: {holding_links: (links || []).map((i) => ({row_index: i})), accounts: [{positions}]}});
  const P = (s, q) => ({symbol: s, quantity: String(q)}), Rw = (s, q) => ({Symbol: s, Quantity: q});
  const d = (pl, rows) => plain(c.pdIbkrDrift_(pl, rows));
  assert.deepEqual(d(plan(['INDV.US'], ['SUPN'], [P('SYNF', 174), P('SUPN', 95)], [0]), [Rw('SYNF.US', 174), Rw('INDV.US', 95)]), [{row: 'INDV.US', position: 'SUPN', quantity: '95'}], 'the 3 Nov pair');
  assert.deepEqual(d(plan(['A.US', 'B.US'], ['X', 'Y'], [P('X', 95), P('Y', 95)], []), [Rw('A.US', 95), Rw('B.US', 95)]), [], 'twins on both sides: no pair');
  assert.deepEqual(d(plan(['A.US'], ['X', 'Y'], [P('X', 95), P('Y', 95)], []), [Rw('A.US', 95)]), [], 'two candidate positions: no pair');
  assert.deepEqual(d(plan(['A.US', 'B.US'], ['X'], [P('X', 95)], []), [Rw('A.US', 95), Rw('B.US', 95)]), [], 'two candidate rows: no pair');
  assert.deepEqual(d(plan(['A.US'], ['X'], [P('X', 96)], []), [Rw('A.US', 95)]), [], 'different counts: no pair');
  assert.deepEqual(d(plan(['A.US', 'B.US'], ['X', 'Y'], [P('X', 10), P('Y', 20)], []), [Rw('A.US', 20), Rw('B.US', 10)]),
    [{row: 'A.US', position: 'Y', quantity: '20'}, {row: 'B.US', position: 'X', quantity: '10'}], 'two independent pairs');
  assert.deepEqual(d(plan(['A.US'], ['X'], [P('X', 0)], []), [Rw('A.US', 0)]), [], 'zero shares never pair');
  assert.deepEqual(d(plan(['A.US'], [], [P('X', 95)], []), [Rw('A.US', 95)]), [], 'nothing unrepresented');
  assert.deepEqual(d({ok: false, reason: 'x'}, [Rw('A.US', 95)]), [], 'an invalid plan');
  assert.deepEqual(d(null, null), [], 'no plan');
  assert.deepEqual(d(plan(['A.US'], ['X'], null, []), [Rw('A.US', 95)]), [], 'a malformed packet never throws');
  const codes = (acts) => plain(c.pdPlainCodes_(acts));
  assert.deepEqual(codes([{action: 'BLOCK', advisor_note: NOTE_CU}, {action: 'BLOCK', advisor_note: NOTE_C}, {action: 'HOLD', advisor_note: NOTE_Q}]),
    {codes: ['custody_unknown', 'unrepresented_positions'], blocked: 2, total: 3}, 'BLOCK rows only, sorted, unique');
  assert.deepEqual(codes([{action: 'block', advisor_note: 'x', detail: {reconciliation_status: 'quantity_mismatch'}}, {action: 'BLOCK', detail: {reconciliation_status: 'matched'}}]),
    {codes: ['quantity_mismatch'], blocked: 2, total: 2}, 'detail status; matched is not a reason');
  assert.deepEqual(codes([{action: 'BLOCK', advisor_note: 'BLOCKED; RECONCILIATION_REQUIRED: bad code!, stale_positions'}]), {codes: ['bad'], blocked: 1, total: 1}, 'the code list stops at the first non-code');
  assert.deepEqual(codes(null), {codes: [], blocked: 0, total: 0}, 'no actions');
  for (const [code, want] of [['custody_unknown', 'a My_Portfolio row has no matching IBKR position (ticker or currency differ)'], ['stale_positions', 'the IBKR capture is too old'],
    ['future_cash', 'the IBKR capture time is ahead of this clock'], ['cash_overreserved', 'cash could not be certified'], ['reservations_incomplete', 'cash could not be certified'],
    ['invalid_fx', 'cash could not be certified'], ['some_new_code', 'some new code']]) assert.equal(c.pdPlainClause_(code), want, 'clause ' + code);
  assert.equal(c.pdPlainLine_({codes: [], blocked: 0, total: 3}, []), '', 'no BLOCK: no line');
  assert.equal(c.pdPlainLine_(null, null), '', 'no info');
  const next = (codesList, drift) => c.pdPlainLine_({codes: codesList, blocked: 1, total: 1}, drift || []).split('NEXT: ')[1];
  assert.ok(/^IBKR lists SUPN where My_Portfolio has INDV\.US/.test(next(['holding_quote_unverified', 'quantity_mismatch'], [{row: 'INDV.US', position: 'SUPN', quantity: '95'}])), 'a pair wins');
  assert.ok(/^make the share counts/.test(next(['custody_unknown', 'quantity_mismatch', 'holding_quote_unverified'])), 'a share-count difference next');
  assert.ok(/^make every My_Portfolio row match one IBKR position/.test(next(['unrepresented_positions', 'holding_quote_unverified'])), 'an unmatched row or position next');
  assert.ok(/^wait for a fresh price/.test(next(['holding_quote_unverified', 'missing_evidence'])), 'a missing price next');
  assert.ok(/^refresh within 14 minutes/.test(next(['missing_evidence'])), 'else the next capture');
  assert.ok(/^refresh within 14 minutes/.test(next([])), 'no code');
  assert.equal(c.pdPlainLine_({codes: [], blocked: 1, total: 1}, []).indexOf('no reason code on the rows'), 'WHY BLOCKED (1 of 1 holdings): '.length, 'BLOCK without a code');
  // every clause and every NEXT, with and without a pair: ASCII and never the ACTIONS-header words
  const all = Object.keys(c.PD_PLAIN_TEXT).concat(['stale_positions', 'future_cash', 'cash_missing', 'some_new_code']);
  for (let i = 0; i < all.length; i++) {
    for (const drift of [[], [{row: 'INDV.US', position: 'SUPN', quantity: '95'}]]) {
      const l = c.pdPlainLine_({codes: [all[i]], blocked: 1, total: 2}, drift);
      assert.ok(/^[\x20-\x7e]+$/.test(l), 'ASCII: ' + l);
      assert.ok(!/action|symbol/i.test(l), 'never the ACTIONS-header words: ' + l);
    }
  }
  const whole = c.pdPlainLine_({codes: all, blocked: 9, total: 9}, [{row: 'INDV.US', position: 'SUPN', quantity: '95'}]);
  assert.ok(!/action|symbol/i.test(whole) && whole.length < 1500, 'the longest line');
  for (const v of ['on', 'ON', ' On ']) assert.equal(run(world({props: {[GATE]: v}}), 'none', src).ctx.pdPlainMode_(), 'on', 'gate ' + JSON.stringify(v));
  for (const v of ['', 'off', 'yes', '1', 'true', 'onn', 'on x']) assert.equal(run(world({props: {[GATE]: v}}), 'none', src).ctx.pdPlainMode_(), 'off', 'gate ' + JSON.stringify(v));
}
function checkReaders(src) {
  const n = run(scen('drift', 'on'), null, src), b = run(scen('drift'), null, baseSource);
  const nr = rowsOf(n.writes), br = rowsOf(b.writes);
  assert.equal(briefHeader(nr), briefHeader(br) + 1, 'run_daily_brief finds the header one row lower');
  assert.deepEqual(nr[briefHeader(nr)], br[briefHeader(br)], 'the same header row');
  assert.equal(acceptanceHeader(nr), acceptanceHeader(br) + 1, 'tfb_acceptance / tfb_export_audit');
  assert.equal(sqlHeader(nr), sqlHeader(br) + 1, '21_v_portfolio_decision.sql');
  assert.equal(nr[PLAIN_ROW - 1][0], LINE_DRIFT, 'the line sits between the title and the header');
  assert.ok(/^ACTIONS /.test(String(nr[ZONE - 1][0])) && nr[PLAIN_ROW][0] === 'Action', 'title, line, header');
}
function checkSelfTest(src) {
  const off = run(scen('drift'), 'pdSelfTest', src).ret, boff = run(scen('drift'), 'pdSelfTest', baseSource).ret;
  assert.equal(off, String(boff).split('[MP-DEC v1.5.18]').join('[MP-DEC v' + NV + ']'), 'off: the v1.5.18 report');
  const on = String(run(scen('drift', 'on'), 'pdSelfTest', src).ret).split('\n');
  const want = 'plain block reasons (PD_PLAIN_REASONS): on | core: ok';
  assert.equal(on.filter((l) => l === want).length, 1, 'the line: ' + on.filter((l) => /plain/.test(l)).join(' / '));
  assert.deepEqual(on.filter((l) => l !== want), String(off).split('\n'), 'every other line = the off report');
  assert.ok(/^cash snapshot core: /.test(on[on.indexOf(want) - 1]), 'right after the cash snapshot core line');
}

/* ------------------------------------------------------------ battery */
function battery() {
  let passed = 0, total = 0;
  const results = [], digest = [];
  function section(label, fn) {
    total++;
    try { fn(); passed++; results.push('PASS ' + label); }
    catch (e) { results.push('FAIL ' + label + ' ... ' + String(e && e.message || e).split('\n').slice(0, 4).join(' ')); }
  }
  section('P0 compile both, version lockstep 1.5.19, base oracle (SHA-256 pinned); the base lacks the new names', () => {
    new vm.Script(newSource, {filename: 'new.gs'}); new vm.Script(baseSource, {filename: 'base.gs'});
    assert.equal(sha256(baseSource), V1518_SHA);
    assert.equal(NV, '1.5.19'); assert.ok(/^ \* Version: 1\.5\.19$/m.test(newSource), 'header lockstep');
    assert.ok(/^var PD_VERSION = '1\.5\.18';$/m.test(baseSource) && /^ \* Version: 1\.5\.18$/m.test(baseSource), 'base is 1.5.18');
    for (const name of ['pdPlainMode_', 'pdIbkrDrift_', 'pdPlainCodes_', 'pdPlainClause_', 'pdPlainLine_', 'pdWritePlainLine_', 'pdPlainCoreOk_']) {
      assert.equal(baseSource.indexOf('function ' + name + '('), -1, 'the base lacks ' + name);
    }
    assert.equal(baseSource.indexOf('PD_PLAIN_REASONS'), -1);
  });
  section('P1 GOLDEN NEGATIVE: gate absent / off / junk on six scenarios -> the REAL refresh equals the base; one PD_PLAIN_REASONS read per rendered refresh; the apply keys are the base keys', () => {
    checkGolden(ACTIVE_SOURCE);
  });
  section('P2 gate on, the 3 Nov ticker-change shape: the exact line at row 14; every other write = the base, rows from 14 shifted by one; B2 and the POST unchanged', () => {
    const n = checkDrift(ACTIVE_SOURCE);
    digest.push(sha256(JSON.stringify(n.writes)));
  });
  section('P3 gate on, the stale-price shape after the ledger rename: NEXT wait for a fresh price', () => { checkQuote(ACTIVE_SOURCE); });
  section('P4 gate on: a clean board and an HTTP error write no line; IBKR off -> no ticker hint; observe IBKR mode keeps the hint', () => { checkQuiet(ACTIVE_SOURCE); });
  section('P5 the pure helpers: one-to-one pairing, code parsing, clauses, the NEXT priority, ASCII, never the ACTIONS-header words; the gate spellings', () => { checkPure(ACTIVE_SOURCE); });
  section('P6 readers still find the real ACTIONS header (run_daily_brief substring rule, tfb_acceptance, tfb_export_audit, 21_v_portfolio_decision.sql)', () => { checkReaders(ACTIVE_SOURCE); });
  section('P7 pdSelfTest: the v1.5.18 report while off; one core-ok line while on, after the cash snapshot core line', () => { checkSelfTest(ACTIVE_SOURCE); });
  section('P8 ES5 / ASCII lint of the v1.5.19 diff; inventory 7 functions + 2 vars, 0 removed; the four replaced base lines; 8 hunks whose reversal is the v1.5.18 file; WHY block', () => {
    const names = (s, re) => (s.match(re) || []).map((m) => m.replace(/^(function|var)\s+/, '').replace(/[\s(=].*$/, ''));
    const fb = names(baseSource, /^function\s+[A-Za-z_$][\w$]*\s*\(/mg), fn = names(newSource, /^function\s+[A-Za-z_$][\w$]*\s*\(/mg);
    for (const f of fb) assert.ok(fn.indexOf(f) >= 0, 'removed ' + f);
    assert.deepEqual(fn.filter((f) => fb.indexOf(f) < 0).sort(), ['pdIbkrDrift_', 'pdPlainClause_', 'pdPlainCodes_', 'pdPlainCoreOk_', 'pdPlainLine_', 'pdPlainMode_', 'pdWritePlainLine_']);
    const vb = names(baseSource, /^var\s+[A-Za-z_$][\w$]*/mg), vn = names(newSource, /^var\s+[A-Za-z_$][\w$]*/mg);
    for (const v of vb) assert.ok(vn.indexOf(v) >= 0, 'var removed ' + v);
    assert.deepEqual(vn.filter((v) => vb.indexOf(v) < 0).sort(), ['PD_PLAIN_PROP', 'PD_PLAIN_TEXT']);
    const nl = new Set(newSource.split('\n')), bl = new Set(baseSource.split('\n'));
    assert.deepEqual(baseSource.split('\n').filter((l) => !nl.has(l)).map((l) => l.trim()),
      ['* Version: 1.5.18', "var PD_VERSION = '1.5.18';", 'pdRenderPayload_(sheet, payload, tokens, ibkr ? ibkr.excluded : null);',
       'function pdRenderPayload_(sheet, payload, tokens, outOfScope) {'], 'only these base lines replaced');
    const added = newSource.split('\n').filter((l) => !bl.has(l));
    assert.ok(added.length >= 180 && added.length <= 300, 'added lines ' + added.length);
    const ES6 = /(^|[^A-Za-z0-9_$.'"])(let|const|class|import|export|yield|async|await)\s|=>|`|\.\.\./;
    for (const l of added) {
      assert.ok(/^[\x09\x20-\x7e]*$/.test(l), 'ASCII: ' + l);
      if (!/^\s*(\*|\/\*|\/\/)/.test(l)) assert.ok(!ES6.test(l), 'ES5: ' + l);
    }
    assert.ok(!added.some((l) => /^\s{4,}function\s+[A-Za-z_$]/.test(l) && !/^\s*(\*|\/\/)/.test(l)), 'no block-level function declaration added (body-level helpers sit at two spaces)');
    const h0 = newSource.indexOf(' * v1.5.19 (2026-11-03)'), h1 = newSource.indexOf(' * v1.5.18 (2026-11-02)');
    assert.ok(h0 > 0 && h1 > h0 && /^[\x09\x0a\x20-\x7e]*$/.test(newSource.slice(h0, h1)), 'WHY block ASCII, above v1.5.18');
    for (const w of [' * WHY (owner, 3 Nov 2026', ' * WHAT (Script Property PD_PLAIN_REASONS', ' * GATES: ', ' * ROLLBACK: ', ' * Harness: tests/test_pd_v1519_plain_block.js']) assert.ok(newSource.slice(h0, h1).indexOf(w) >= 0, 'WHY section ' + w);
    for (const l of baseSource.split('\n').filter((x) => /^ \* v1\.\d+\.\d+ \(/.test(x))) assert.ok(newSource.indexOf(l) > 0, 'prior WHY kept: ' + l);
    assert.equal(newSource.indexOf('\r'), -1, 'LF only');
    assert.equal(EDITS.length, 8, 'eight hunks'); assert.equal(sha256(reverseEdits(newSource)), V1518_SHA, 'reversal = the v1.5.18 file');
  });
  return {passed, total, results, digest: sha256(digest.join('|')).slice(0, 16)};
}

/* ------------------------------------------------------------ mutants */
const MUTANTS = [
  ['gate always on', "  return pdIbkrProp_(PD_PLAIN_PROP).toLowerCase() === 'on' ? 'on' : 'off';", "  return 'on';", checkGolden],
  ['pairs without the one-to-one rule', '      if (nHit === 1 && nTwin === 1) {', '      if (nHit >= 1) {', checkPure],
  ['the ticker hint dropped', '  if (pairs.length) {', '  if (false) {', checkDrift],
  ['a line without a BLOCK row', "  if (!info || !info.blocked) return '';", "  if (!info) return '';", checkQuiet],
  ['the drift key enumerable', "                                           enumerable: false});", "                                           enumerable: true});", checkGolden],
  ['the self-test line while off', "  if (pdPlainMode_() === 'on') {\n    report.push('plain block reasons", "  if (true) {\n    report.push('plain block reasons", checkSelfTest],
  ['matched read as a reason', "c !== 'matched' && ", '', checkPure],
  ['a header word in a clause', "holding_quote_unverified: 'a holding has no current price (no fresh market quote)'", "holding_quote_unverified: 'a holding has no current price (no symbol quote, no action)'", checkPure],
  ['the line below the header', '  if (plainLine) row = pdWritePlainLine_(sheet, row, plainLine);   /* v1.5.19 */\n  var aRows = [];', '  var aRows = [];', checkReaders],
  ['the price step before the unmatched rows', "  } else if (any(['custody_unknown', 'unrepresented_positions', 'position_identity_mismatch',", "  } else if (has.holding_quote_unverified) {\n    next = 'wait';\n  } else if (any(['custody_unknown', 'unrepresented_positions', 'position_identity_mismatch',", checkPure]
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
console.log('PD V1519 P0-P8 x2 ' + (runs[0].digest === runs[1].digest ? 'identical' : 'DIVERGENT') + ' digest ' + runs[0].digest + ' | mutants killed ' + killed + '/' + counted +
  ' | new SHA256 ' + sha256(newSource) + ' | base SHA256 ' + sha256(baseSource));
console.log((ok ? 'PASS ' : 'FAIL ') + runs[0].passed + '/' + runs[0].total);
process.exit(ok ? 0 : 1);
