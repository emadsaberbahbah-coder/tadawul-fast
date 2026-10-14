/* 10_My_Portfolio v1.5.12+: IBKR capture evidence, execution scope, USD peg,
 * quote provenance and the capture watcher. Runs the REAL bound source in a
 * vm with Apps Script service stubs. Every holding, contract id, amount and
 * account here is synthetic.
 *
 *   node tests/js/pf_ibkr_evidence.test.js
 *   PF_GAS=<file>        source under test (default apps_script/10_My_Portfolio.gs)
 *   PF_BASE_GAS=<file>   optional v1.5.11 copy, compared live in off mode
 *   PF_WRITE_GOLDEN=1    regenerate the off-mode golden (PF_GAS must be v1.5.11)
 *   PF_DUMP=<file>       write produced lines, request bodies and cash cases
 *   PF_E2E_NOW=<iso> PF_E2E_OUT=<file>
 *                        only build the enforce request around that clock
 *                        (tests/test_pf_ibkr_evidence.py feeds it to the
 *                        real portfolio_actions build)
 */
'use strict';
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const GAS = process.env.PF_GAS || path.join(__dirname, '../../apps_script/10_My_Portfolio.gs');
const GOLDEN = path.join(__dirname, '../fixtures/pf_ibkr_v1_5_11_golden.json');
const SOURCE = fs.readFileSync(GAS, 'utf8');
const RUN = '5151515151';
const FOLDER = 'synthFolder_0123456789';
const START = Date.parse('2026-10-15T05:45:00Z');          // 08:45:00 Riyadh
const CAP_MS = Date.parse('2026-10-15T05:41:07.522Z');     // 08:41:07 Riyadh
const CREATED_MS = Date.parse('2026-10-15T05:41:50Z');
const FILE_NAME = 'TFB_IBKR_Capture_20261015T054107Z.json';
const PANEL_CASH = 23000;
const RECEIPT_RE = /(?<![a-z0-9_])source readiness VERIFIED \(run ([1-9][0-9]*)\)/gi;
const DECLARATION_RE = /(?<![a-z0-9_])source\s+readiness\b/gi;

let passed = 0;
const TOTAL = 14;
const dump = {};
function check(label, fn) { fn(); passed++; console.log('PASS ' + label); }
function count(re, text) { return (String(text).match(re) || []).length; }
const plain = (v) => JSON.parse(JSON.stringify(v));
const iso = (ms) => new Date(ms).toISOString();
/** Like the claude.ai current_time tool: Riyadh wall time, microseconds, +03:00. */
const riyadhIso = (ms) => new Date(ms + 3 * 3600000).toISOString().slice(0, 23) + '873+03:00';

const STATUS_HEADER = ['Page', 'Last Updated', 'Status', 'Message', 'Endpoint',
  'HTTP Code', 'Rows', 'Columns', 'Duration ms', 'Warnings'];
function statusGrid(stampMs) {
  const msg = '[STATUS-STAMP v6.70.0] leg=success written=4 failed=0 requested=4 fresh=4 ' +
    'acquired=4/4 acquisition=COMPLETE fetchfail_requested=observe fetchfail_effective=observe ' +
    'warnings=0 | data=COMPLETE policy_data=COMPLETE sha=deadbeef run=' + RUN;
  const stamp = new Date(stampMs + 3 * 3600000).toISOString().slice(0, 19).replace('T', ' ') + '+03:00';
  return [STATUS_HEADER, ['My_Portfolio', stamp, 'SUCCESS', msg, 'backend:run_dashboard_sync', '',
                          4, 122, 1000, '']];
}

const HOLD_HEADERS = ['Symbol', 'Name', 'Sector', 'Exchange', 'Currency', 'Position Qty',
  'Avg Cost', 'Current Price', 'Price Target', 'Intrinsic Value', 'Expected ROI 12M',
  'Forecast Reliability Score', 'Data Quality Score', 'Risk Bucket', 'Volatility 30D',
  'Avg Volume 30D', 'Recommendation Detail', 'Investability Status', 'Data Provider',
  'Last Updated (UTC)', 'Warnings'];
function holdRows(refMs) {
  const acq = iso(refMs - 120000), quote = iso(refMs - 150000);
  const warn = (p) => 'acquisition_status:success; acquisition_acquired_at:' + acq +
    '; acquisition_provider:' + p + '; acquisition_quote_asof:' + quote;
  return [
    ['SYNA.US', 'Synthetic Alpha', 'Financials', 'NYSE/NASDAQ', 'USD', 40, 25.5, 26.1, 30, 29,
     12.5, 80, 90, 'Moderate', 22.5, 150000, 'HOLD: steady', 'INVESTABLE', 'eodhd', acq, warn('eodhd')],
    ['SYNB.US', 'Synthetic Beta', 'Energy', 'NYSE/NASDAQ', 'USD', 75, 12.4, 13.2, 15, 14.6,
     8, 78, 88, 'Moderate', 28, 300000, 'HOLD', 'INVESTABLE', 'eodhd', acq, warn('eodhd')],
    ['SYNC.US', 'Synthetic Gamma', 'Industrials', 'NYSE/NASDAQ', 'USD', 120, 14.9, 14.1, 16,
     15.2, 6, 75, 85, 'Moderate', 25, 90000, 'HOLD', 'INVESTABLE', 'eodhd', acq, warn('eodhd')],
    ['9999.SR', 'Synthetic Sukuk', 'Fixed Income', 'SAU', 'SAR', 10, 1000, 998.5, '', '', '',
     60, 80, 'Low', 2, 1000, 'HOLD', 'INVESTABLE', 'yahoo_chart', acq, warn('yahoo_chart')]
  ];
}
const RESPONSE = {status: 'ok', kpis: {}, sector_summary: [], alerts: [],
  actions: [{action: 'HOLD', symbol: 'SYNA.US', name: 'Synthetic Alpha'}],
  meta: {route: {version: '4.18.2', request_id: 'req-synthetic'},
         versions: {portfolio_actions: '1.16.0', opportunity_builder: '1.26.1'}}};
const FX_LOOKUP = [['SAR', 'Saudi Riyal', '', 1], ['USD', 'US Dollar', '', 3.75547],
                   ['EUR', 'Euro', '', 4.1]];

function capture(o) {
  o = o || {};
  const c = {schema: 'tfb.ibkr-capture/v1', captured_at: riyadhIso(o.capMs || CAP_MS),
    account_alias: 'ibkr-synthetic', producer: 'synthetic',
    positions: {positions: [
      {contract_id: 1000001, contract_description: 'SYNA', position: 40, market_price: 26.1,
       currency: 'USD', asset_class: 'STK'},
      {contract_id: 1000002, contract_description: 'SYNB', position: 75, currency: 'USD',
       asset_class: 'STK'},
      {contract_id: 1000003, contract_description: 'SYNC', position: 120, currency: 'USD',
       asset_class: 'STK'}]},
    balances: {balances: [
      {currency: 'BASE', cash_balance: 12345.67, settled_cash: 12345.67},
      {currency: 'USD', cash_balance: 12345.67, settled_cash: 12345.67}]},
    orders: {orders: [
      {order_id: 1, order_status: 'NEW', order_type: 'LIMIT', side: 'SELL', limit_price: '30',
       remaining_shares_qty: '40'},
      {order_id: 2, order_status: 'REPLACED', order_type: 'STOP', side: 'SELL',
       remaining_shares_qty: '75'}]}};
  if (o.mutate) o.mutate(c);
  return c;
}
function driveFile(cap, o) {
  o = o || {};
  return {id: o.id || 'fileA', name: o.name || FILE_NAME,
          created: o.created === undefined ? CREATED_MS : o.created,
          text: typeof cap === 'string' ? cap : JSON.stringify(cap)};
}
const ENFORCE = {PD_IBKR_EVIDENCE: 'enforce', PD_IBKR_CAPTURE_FOLDER_ID: FOLDER,
                 PD_EXECUTION_EXCLUDE: '9999.SR'};
const OBSERVE = Object.assign({}, ENFORCE, {PD_IBKR_EVIDENCE: 'observe'});

function world(o) {
  o = o || {};
  return {props: Object.assign({PD_ACTION_LOG: 'off'}, o.props || {}),
          docProps: o.docProps || {}, files: o.files || [], triggers: [], posts: [],
          drive: [], locks: 0, lockBusy: false, driveThrows: o.driveThrows || null,
          clock: o.clock || START, code: o.code || 200, statusMs: o.statusMs || START - 3 * 3600000,
          holdRef: o.holdRef || START};
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
      if (arguments.length === 2 && r === 2 && c === 2) {
        return chain({push(x) { statusSets.push(x.value); }}, [r, c]);
      }
      return chain(writes, [r, c, nr, nc], nr, nc);
    },
    getMaxRows() { return 80; }, getMaxColumns() { return 26; }
  };
  const mp = {
    __headers: HOLD_HEADERS,
    getLastRow() { return rows.length + 1; },
    getLastColumn() { return HOLD_HEADERS.length; },
    getRange(r, c, nr, nc) {
      return {getValues() { return rows.slice(0, nr).map((x) => x.slice(0, nc)); }};
    }
  };
  const grid = statusGrid(w.statusMs);
  const status = {
    getLastRow() { return grid.length; }, getLastColumn() { return 10; },
    getRange(r, c, nr, nc) { return {getValues() { return grid.slice(0, nr).map((x) => x.slice(0, nc)); }}; }
  };
  const sheets = {Portfolio_Decision: pd, My_Portfolio: mp, _Status: status};
  const ss = {
    getSheetByName(n) { return Object.prototype.hasOwnProperty.call(sheets, n) ? sheets[n] : null; },
    getRangeByName(n) { return n === 'TFB_FX_LOOKUP' ? {getValues() { return FX_LOOKUP; }} : null; },
    insertSheet() { return chain(); }
  };
  const props = (store) => ({
    getProperty(k) { return Object.prototype.hasOwnProperty.call(store, k) ? store[k] : null; },
    setProperty(k, v) { store[k] = String(v); }
  });
  const trigger = (handler, every) => ({getHandlerFunction() { return handler; }, every: every});
  const ctx = {
    SHEET_NAMES: {MY_PORTFOLIO: 'My_Portfolio'},
    Date: FixedDate,
    Logger: {log(m) { logs.push(String(m)); }},
    Session: {getScriptTimeZone() { return 'Asia/Riyadh'; }},
    Utilities: {formatDate(d) {
      return new Date(d.getTime() + 3 * 3600000).toISOString().slice(0, 19).replace('T', ' ');
    }},
    PropertiesService: {getScriptProperties() { return props(w.props); },
                        getDocumentProperties() { return props(w.docProps); }},
    SpreadsheetApp: {getActiveSpreadsheet() { return ss; }, getActive() { return ss; }, flush() {},
      BorderStyle: {SOLID: 'SOLID', SOLID_THICK: 'SOLID_THICK'}, getUi() { return {alert() {}}; }},
    UrlFetchApp: {fetch(url, options) {
      w.posts.push(JSON.parse(options.payload));
      return {getContentText() { return JSON.stringify(RESPONSE); },
              getResponseCode() { return w.code; }};
    }},
    DriveApp: {searchFiles(q) {
      w.drive.push(q);
      if (w.driveThrows) throw new Error(w.driveThrows);
      const m = q.match(/modifiedDate > '([^']+)'/);
      const since = m ? Date.parse(m[1] + 'Z') : -Infinity;
      const list = w.files.filter((f) => q.indexOf("'" + w.props.PD_IBKR_CAPTURE_FOLDER_ID + "' in parents") === 0 &&
                                          f.created > since);
      let i = 0;
      return {hasNext() { return i < list.length; }, next() {
        const f = list[i++];
        return {getName() { return f.name; }, getId() { return f.id; },
                getDateCreated() { return new Date(f.created); },
                getBlob() { return {getDataAsString() { return f.text; }}; }};
      }};
    }},
    LockService: {getScriptLock() {
      return {tryLock() { if (w.lockBusy) return false; w.locks++; return true; },
              releaseLock() { w.locks--; }};
    }},
    ScriptApp: {
      getProjectTriggers() { return w.triggers.slice(); },
      deleteTrigger(t) { w.triggers = w.triggers.filter((x) => x !== t); },
      newTrigger(handler) {
        const b = {every: null, timeBased() { return b; },
                   everyMinutes(n) { b.every = n; return b; },
                   create() { const t = trigger(handler, b.every); w.triggers.push(t); return t; }};
        return b;
      }
    },
    ensureSheetExists_(name) { return ss.getSheetByName(name); },
    readCurrentSheetHeaders_(sheet) { return sheet.__headers || []; }
  };
  vm.createContext(ctx);
  vm.runInContext(source || SOURCE, ctx, {filename: '10_My_Portfolio.gs'});
  const before = w.posts.length;
  const ret = call === 'none' ? undefined : ctx[call]();
  return {ret: ret, b2: statusSets[statusSets.length - 1], post: w.posts.length > before
          ? w.posts[w.posts.length - 1] : null, writes: writes, logs: logs, ctx: ctx};
}

/* ------------------------------------------------------------------ e2e mode */
if (process.env.PF_E2E_OUT) {
  const now = Date.parse(process.env.PF_E2E_NOW);
  assert.ok(isFinite(now), 'PF_E2E_NOW must be an ISO instant');
  const cap = capture({capMs: now - 60000});
  const w = world({props: ENFORCE, clock: now, holdRef: now, statusMs: now - 600000,
                   files: [driveFile(cap, {created: now - 30000})]});
  const r = run(w);
  fs.writeFileSync(process.env.PF_E2E_OUT, JSON.stringify({body: r.post, b2: r.b2}));
  console.log('E2E request written');
  process.exit(0);
}

/* ------------------------------------------------------------------ golden */
const off = run(world({files: [driveFile(capture())]}));
if (process.env.PF_WRITE_GOLDEN) {
  fs.writeFileSync(GOLDEN, JSON.stringify({
    note: 'refreshPortfolioDecision off-mode output of 10_My_Portfolio.gs v1.5.11 on the synthetic IBKR scenario',
    source_sha256: crypto.createHash('sha256').update(SOURCE).digest('hex'),
    b2: off.b2, post_body: off.post, writes_sha256: crypto.createHash('sha256')
      .update(JSON.stringify(off.writes)).digest('hex')}, null, 1) + '\n');
  console.log('golden written from ' + GAS);
  process.exit(0);
}
const golden = JSON.parse(fs.readFileSync(GOLDEN, 'utf8'));
const POOL = 'holdings 4 rows from My_Portfolio (qty/cost from cost-basis: 0)';
const withNote = (note) => golden.b2.replace(POOL, POOL + note);
dump.off = off.b2;

check('P01 off (default and any other value): request, render and line are v1.5.11 byte-identical; no Drive, lock or trigger use', () => {
  const sha = (x) => crypto.createHash('sha256').update(JSON.stringify(x)).digest('hex');
  for (const raw of [undefined, '', 'on', 'true', '1', 'ENFORCE-ish', 'yes']) {
    const props = raw === undefined ? {} : {PD_IBKR_EVIDENCE: raw, PD_IBKR_CAPTURE_FOLDER_ID: FOLDER,
                                           PD_EXECUTION_EXCLUDE: '9999.SR'};
    const w = world({props: props, files: [driveFile(capture())]});
    const r = run(w);
    assert.equal(r.b2, golden.b2, 'mode ' + raw);
    assert.deepEqual(r.post, golden.post_body, 'mode ' + raw);
    assert.equal(sha(r.writes), golden.writes_sha256, 'render ' + raw);
    assert.equal(w.drive.length, 0); assert.equal(w.locks, 0);
    assert.equal(r.ret, 'ok');
  }
  for (const row of golden.post_body.rows) {
    for (const k of ['Data Provider', 'Warnings', 'Last Updated (UTC)']) assert.equal(k in row, false);
  }
  if (process.env.PF_BASE_GAS) {
    const base = fs.readFileSync(process.env.PF_BASE_GAS, 'utf8');
    const live = run(world({files: [driveFile(capture())]}), null, base);
    assert.equal(off.b2, live.b2);
    assert.deepEqual(off.post, live.post);
    assert.deepEqual(off.writes, live.writes);
  }
});

const SENT = ' | ibkr evidence SENT (capture 08:41 age 4m; linked 3/3; cash 46296.26 SAR; USD 3.75; excluded 9999.SR)';
const EXPECTED_PACKET = {
  schema_version: 1, source_ref: 'ibkr-capture:' + FILE_NAME, captured_at: '2026-10-15T05:41:07.522Z',
  accounts: [{account_id: 'ibkr-synthetic', positions_asof: '2026-10-15T05:41:07.522Z',
    positions_complete: true,
    positions: [{instrument_id: '1000001', symbol: 'SYNA', currency: 'USD', quantity: '40'},
                {instrument_id: '1000002', symbol: 'SYNB', currency: 'USD', quantity: '75'},
                {instrument_id: '1000003', symbol: 'SYNC', currency: 'USD', quantity: '120'}],
    cash_asof: '2026-10-15T05:41:07.522Z', cash_complete: true,
    cash: [{currency: 'USD', settled_cash: '12345.67', reserved_cash: '0',
            reservations_complete: true}]}],
  holding_links: [
    {row_index: 0, account_id: 'ibkr-synthetic', instrument_id: '1000001', position_symbol: 'SYNA',
     symbol: 'SYNA.US', currency: 'USD'},
    {row_index: 1, account_id: 'ibkr-synthetic', instrument_id: '1000002', position_symbol: 'SYNB',
     symbol: 'SYNB.US', currency: 'USD'},
    {row_index: 2, account_id: 'ibkr-synthetic', instrument_id: '1000003', position_symbol: 'SYNC',
     symbol: 'SYNC.US', currency: 'USD'}],
  funding_accounts: ['ibkr-synthetic'],
  fx_rates: [{currency: 'USD', rate_to_sar: '3.75', asof: '2026-10-15T05:41:07.522Z',
              source_ref: 'policy://sama-usd-peg-3.75'}]};

check('P02 observe: one tag of what enforce would send; request and render unchanged', () => {
  const w = world({props: OBSERVE, files: [driveFile(capture())]});
  const r = run(w);
  dump.observe = r.b2;
  assert.equal(r.b2, withNote(' | [ibkr-observe] would send (capture 08:41 age 4m; linked 3/3; ' +
    'cash 46296.26 SAR; would exclude 9999.SR; USD 3.75)'));
  assert.deepEqual(r.post, golden.post_body);
  assert.equal(crypto.createHash('sha256').update(JSON.stringify(r.writes)).digest('hex'),
               golden.writes_sha256);
  assert.equal(w.drive.length, 1);
  assert.ok(w.drive[0].indexOf("'" + FOLDER + "' in parents and trashed = false and title contains " +
    "'TFB_IBKR_Capture_' and modifiedDate > '2026-10-13T17:45:00'") === 0, w.drive[0]);
  const stale = run(world({props: OBSERVE, clock: START + 20 * 60000, files: [driveFile(capture())]}));
  assert.ok(stale.b2.indexOf('Last run 2026-10-15 09:05:00 | status: ok | ' + POOL +
    ' | [ibkr-observe] would not send: capture 08:41 is 24m old (limit 14m); would exclude 9999.SR | ') === 0,
    stale.b2);
});

check('P03 enforce + fresh capture: scope, peg, provenance, exact packet, certified cash, out-of-scope render', () => {
  const w = world({props: ENFORCE, files: [driveFile(capture())]});
  const r = run(w);
  dump.enforce = r.b2;
  dump.enforce_body = r.post;
  assert.equal(r.b2, withNote(SENT));
  const body = r.post;
  assert.deepEqual(body.rows.map((x) => x.Symbol), ['SYNA.US', 'SYNB.US', 'SYNC.US']);
  for (let i = 0; i < 3; i++) {
    const want = Object.assign({}, golden.post_body.rows[i], {
      'Data Provider': 'eodhd', 'Last Updated (UTC)': holdRows(START)[i][19], 'Warnings': holdRows(START)[i][20]});
    assert.deepEqual(body.rows[i], want);
  }
  assert.deepEqual(body.fx_rates, {SAR: 1, USD: 3.75, EUR: 4.1});
  assert.deepEqual(body.controls, Object.assign({}, golden.post_body.controls,
    {'PF: Cash Available SAR': 46296.26}));
  assert.equal(golden.post_body.controls['PF: Cash Available SAR'], PANEL_CASH);
  assert.deepEqual(body.reconciliation_evidence, EXPECTED_PACKET);
  assert.deepEqual(Object.keys(body).sort(), ['controls', 'fx_rates', 'reconciliation_evidence', 'rows']);
  const titles = r.writes.filter((x) => typeof x.value === 'string').map((x) => x.value);
  assert.ok(titles.indexOf('OUTSIDE EXECUTION SCOPE (1) — held, not weighted, funded or actioned ' +
    '(PD_EXECUTION_EXCLUDE)') >= 0, titles.join(' || '));
  const table = r.writes.find((x) => x.values && x.values.length === 1 && x.values[0][0] === '9999.SR');
  assert.deepEqual(table.values, [['9999.SR', 'Synthetic Sukuk', 'SAR', 10, 1000, 998.5,
                                   'outside execution scope']]);
  for (const line of r.logs) {
    for (const secret of ['ibkr-capture:', '12345.67', '46296', 'ibkr-synthetic', '1000001']) {
      assert.equal(line.indexOf(secret), -1, 'logged ' + secret + ': ' + line);
    }
  }
  assert.equal(r.ret, 'ok');
});

check('P04 receipt still binds: excluded rows count as consumed (4 of 4); rows newer than the run withhold', () => {
  /* the publication was stamped 1 min before the decision, after the rows' 08:43 acquisition */
  const r = run(world({props: Object.assign({PD_SOURCE_RECEIPT: 'enforce'}, ENFORCE),
                       files: [driveFile(capture())], statusMs: START - 60000}));
  dump.receipt_and_ibkr = r.b2;
  assert.equal(r.b2, withNote(' | source readiness VERIFIED (run ' + RUN + ')' + SENT));
  assert.equal(count(DECLARATION_RE, r.b2), 1);
  const m = [...r.b2.matchAll(RECEIPT_RE)];
  assert.equal(m.length, 1); assert.equal(m[0][1], RUN);
  /* v1.5.13: the page was refreshed after a publication stamped at 05:45 */
  const late = run(world({props: Object.assign({PD_SOURCE_RECEIPT: 'enforce'}, ENFORCE),
                          files: [driveFile(capture())]}));
  assert.equal(late.b2, withNote(' | source readiness WITHHELD: holdings refreshed after the publication ' +
    '(08:43 > 05:45 Riyadh)' + SENT));
  assert.equal(count(RECEIPT_RE, late.b2), 0);
});

check('P05 enforce + stale capture: scope, peg and provenance only; no evidence, panel cash kept', () => {
  const packet = {schema_version: 1, source_ref: 'synthetic://declared'};
  const w = world({props: ENFORCE, clock: START + 20 * 60000, files: [driveFile(capture())],
                   docProps: {TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1: JSON.stringify(packet)}});
  const r = run(w);
  dump.stale = r.b2;
  assert.ok(r.b2.indexOf(POOL + ' | ibkr evidence NOT SENT: capture 08:41 is 24m old (limit 14m); ' +
    'USD 3.75; excluded 9999.SR | ') > 0, r.b2);
  assert.equal('reconciliation_evidence' in r.post, false);
  assert.equal(r.post.controls['PF: Cash Available SAR'], PANEL_CASH);
  assert.equal(r.post.fx_rates.USD, 3.75);
  assert.equal(r.post.rows.length, 3);
  assert.equal(r.post.rows[0]['Data Provider'], 'eodhd');
});

const REJECT = [
  ['schema', capture({mutate(c) { c.schema = 'tfb.ibkr-capture/v0'; }}), {}, 'capture schema unsupported'],
  ['naive time', capture({mutate(c) { c.captured_at = '2026-10-15T08:41:07'; }}), {},
   'captured_at needs a full timestamp with offset'],
  ['future', capture({capMs: START + 180000}), {created: START + 200000}, 'capture is in the future'],
  ['drive time', capture(), {created: CAP_MS + 12 * 60000}, 'captured_at disagrees with the Drive file time'],
  ['alias', capture({mutate(c) { c.account_alias = 'U 123'; }}), {}, 'account alias invalid'],
  ['positions', capture({mutate(c) { delete c.positions; }}), {}, 'positions read missing'],
  ['balances', capture({mutate(c) { c.balances = {rows: []}; }}), {}, 'balances read missing'],
  ['orders', capture({mutate(c) { c.orders = 'none'; }}), {}, 'orders read missing'],
  ['repeated', capture({mutate(c) { c.positions.positions[1].contract_id = 1000001; }}), {},
   'position repeated (SYNB)'],
  ['short', capture({mutate(c) { c.positions.positions[0].position = -5; }}), {},
   'position quantity invalid (SYNA)'],
  ['exponent qty', capture({mutate(c) { c.positions.positions[0].position = '4e1'; }}), {},
   'position quantity invalid (SYNA)'],
  ['eur cash', capture({mutate(c) { c.balances.balances.push({currency: 'EUR', settled_cash: 10}); }}),
   {}, 'cash in EUR has no FX proof'],
  ['negative cash', capture({mutate(c) { c.balances.balances[1].settled_cash = -1; }}), {},
   'settled cash negative (USD)'],
  ['base only', capture({mutate(c) { c.balances.balances = c.balances.balances.slice(0, 1); }}), {},
   'no cash balance in the capture'],
  ['not json', '{"schema":', {}, 'capture is not JSON'],
];
check('P06 enforce rejects ' + (REJECT.length + 4) + ' bad captures or reads with a reason and sends no evidence', () => {
  dump.rejected = {};
  const cases = REJECT.map(([label, cap, fo, why]) => [label, {files: [driveFile(cap, fo)]}, why]);
  cases.push(['folder unset', {files: [driveFile(capture())], props: {PD_IBKR_CAPTURE_FOLDER_ID: ''}},
              'capture folder not set (PD_IBKR_CAPTURE_FOLDER_ID)']);
  cases.push(['no capture 36h', {files: [driveFile(capture(), {created: START - 37 * 3600000})]},
              'no capture in the last 36h']);
  cases.push(['drive auth', {files: [driveFile(capture())], driveThrows: 'Exception: You do not have ' +
              'permission to call DriveApp.searchFiles'}, 'Drive not authorized (run pdSelfTest from the editor)']);
  cases.push(['drive other', {files: [driveFile(capture())], driveThrows: 'Service error: Drive'},
              'capture read failed']);
  for (const [label, o, why] of cases) {
    const w = world({props: Object.assign({}, ENFORCE, o.props || {}), files: o.files,
                     driveThrows: o.driveThrows});
    const r = run(w);
    dump.rejected[label] = r.b2;
    assert.ok(r.b2.indexOf(POOL + ' | ibkr evidence NOT SENT: ' + why + '; USD 3.75; excluded 9999.SR | ') > 0,
              label + ': ' + r.b2);
    assert.equal('reconciliation_evidence' in r.post, false, label);
    assert.equal(r.post.controls['PF: Cash Available SAR'], PANEL_CASH, label);
    assert.equal(r.post.rows.length, 3, label);
  }
  const zeroEur = run(world({props: ENFORCE, files: [driveFile(capture({mutate(c) {
    c.balances.balances.push({currency: 'EUR', settled_cash: 0}); }}))]}));
  assert.equal(zeroEur.b2, withNote(SENT), 'zero non-USD cash is ignored');
});

/** BigInt oracle: (settled - ceil-cents reservations) x 3.75, half-even cents. */
function oracleCents(settledText, orders) {
  const units = (t, s) => { const [w, f = ''] = String(t).split('.'); return BigInt(w + f.padEnd(s, '0')); };
  let reserved = 0n;
  for (const [qty, limit] of orders) {
    const prod = units(qty, 4) * units(limit, 4);           // 1e-8
    reserved += (prod + 999999n) / 1000000n;                // ceil cents
  }
  const sar = (units(settledText, 6) - reserved * 10000n) * 375n;   // 1e-8
  let q = sar / 1000000n; const r = sar % 1000000n;
  if (r * 2n > 1000000n || (r * 2n === 1000000n && q % 2n === 1n)) q += 1n;
  return {cents: Number(q), reserved: Number(reserved)};
}
const buy = (qty, limit, extra) => Object.assign({order_status: 'NEW', order_type: 'LIMIT', side: 'BUY',
  remaining_shares_qty: qty, limit_price: limit}, extra || {});

check('P07 working BUY orders reserve cash (ceil cents); unknown or market buys withhold the cash', () => {
  const two = [buy('10', '20.005'), buy('3.5', '10.333'), buy('7', '1', {order_status: 'FILLED'})];
  const r = run(world({props: ENFORCE, files: [driveFile(capture({mutate(c) {
    c.orders.orders = c.orders.orders.concat(two); }}))]}));
  const o = oracleCents('12345.67', [['10', '20.005'], ['3.5', '10.333']]);
  assert.equal(o.reserved, 20005 + 3617);
  assert.equal(r.post.reconciliation_evidence.accounts[0].cash[0].reserved_cash, '236.22');
  assert.equal(r.post.controls['PF: Cash Available SAR'], o.cents / 100);
  assert.equal(o.cents, 4541044);
  assert.ok(r.b2.indexOf('cash 45410.44 SAR') > 0, r.b2);
  const withheld = [
    ['market buy', [buy('5', undefined, {order_type: 'MARKET'})], 'reservations incomplete (BUY order without a limit price)'],
    ['odd side', [{order_status: 'NEW', side: 'SSHORT', limit_price: '1', remaining_shares_qty: '1'}],
     'reservations incomplete (order side unknown)'],
    ['over reserved', [buy('1000', '20')], 'cash over-reserved'],
  ];
  for (const [label, orders, why] of withheld) {
    const w = run(world({props: ENFORCE, files: [driveFile(capture({mutate(c) {
      c.orders.orders = c.orders.orders.concat(orders); }}))]}));
    assert.ok(w.b2.indexOf(POOL + ' | ibkr evidence SENT (capture 08:41 age 4m; linked 3/3; ' + why +
      '; USD 3.75; excluded 9999.SR) | ') > 0, label + ': ' + w.b2);
    assert.equal(w.post.controls['PF: Cash Available SAR'], PANEL_CASH, label);
    assert.ok(w.post.reconciliation_evidence, label);
  }
  const multi = run(world({props: ENFORCE, files: [driveFile(capture({mutate(c) {
    c.balances.balances.push({currency: 'SAR', settled_cash: 100});
    c.orders.orders.push(buy('1', '1')); }}))]}));
  assert.ok(multi.b2.indexOf('reservations incomplete (BUY order currency ambiguous)') > 0, multi.b2);
  assert.equal(multi.post.reconciliation_evidence.accounts[0].cash[1].reservations_complete, false);
});

check('P08 links: SYMBOL.US + USD stock only; unmatched rows and IBKR-only positions are reported', () => {
  const extra = run(world({props: ENFORCE, files: [driveFile(capture({mutate(c) {
    c.positions.positions.push({contract_id: 1000004, contract_description: 'SYND', position: 3,
                                currency: 'USD', asset_class: 'STK'});
    c.positions.positions.push({contract_id: 1000005, contract_description: 'SYNE', position: 0,
                                currency: 'USD', asset_class: 'STK'});
    c.positions.positions.push({contract_id: 1000006, contract_description: 'SYNF', position: 2,
                                currency: 'EUR', asset_class: 'STK'}); }}))]}));
  assert.ok(extra.b2.indexOf('linked 3/3; IBKR not on page: SYND,SYNF; cash 46296.26 SAR') > 0, extra.b2);
  assert.equal(extra.post.reconciliation_evidence.accounts[0].positions.length, 6);
  const missing = run(world({props: ENFORCE, files: [driveFile(capture({mutate(c) {
    c.positions.positions = c.positions.positions.slice(0, 2); }}))]}));
  assert.ok(missing.b2.indexOf('linked 2/3; cash 46296.26 SAR') > 0, missing.b2);
  assert.deepEqual(missing.post.reconciliation_evidence.holding_links.map((x) => x.row_index), [0, 1]);
  const {ctx} = run(world(), 'none');
  assert.equal(ctx.pdIbkrNative_('BRK B', 'USD', 'STK'), 'BRK-B.US');
  assert.equal(ctx.pdIbkrNative_('SYNA', 'USD', 'OPT'), '');
  assert.equal(ctx.pdIbkrNative_('SYNA', 'EUR', 'STK'), '');
  assert.deepEqual(plain(ctx.pdIbkrExcludeList_(' 9999.sr, 1111.SR;9999.SR  2222.SR ')),
                   ['9999.SR', '1111.SR', '2222.SR']);
});

const CASH_CASES = ['12345.67', '0.02', '0.06', '1.1', '1.3', '0.01', '0.03', '1234.567',
  '0.000001', '99999.99', '250000', '7'];
check('P09 certified cash = (settled - reserved) x 3.75 rounded half-even, matching a BigInt oracle', () => {
  const {ctx} = run(world(), 'none');
  dump.cash_cases = [];
  const cases = CASH_CASES.map((t) => [t, []]).concat([
    ['12345.67', [['10', '20.005']]], ['100', [['3', '0.0001']]], ['50.5', [['2', '12.345']]]]);
  for (const [settled, orders] of cases) {
    const cap = capture({mutate(c) {
      c.balances.balances[1].settled_cash = settled;
      c.orders.orders = orders.map(([q, l]) => buy(q, l)); }});
    const plan = plain(ctx.pdIbkrPlan_(JSON.parse(JSON.stringify(cap)), [], START, CREATED_MS, 'x'));
    assert.equal(plan.ok, true, settled);
    const o = oracleCents(settled, orders);
    assert.equal(plan.cashCents, o.cents, settled + ' ' + JSON.stringify(orders));
    dump.cash_cases.push({settled: settled, orders: orders, cents: plan.cashCents,
                          cash: plan.packet.accounts[0].cash[0], packet: plan.packet});
  }
  const ties = Object.fromEntries(dump.cash_cases.filter((c) => !c.orders.length).map((c) => [c.settled, c.cents]));
  assert.equal(ties['0.02'], 8); assert.equal(ties['0.06'], 22); assert.equal(ties['1.1'], 412);
  assert.equal(ties['1.3'], 488); assert.equal(ties['0.000001'], 0);
  /* float noise in a numeric balance is absorbed at 6 decimals */
  const noisy = capture({mutate(c) { c.balances.balances[1].settled_cash = 0.1 + 0.2; }});
  assert.equal(ctx.pdIbkrPlan_(noisy, [], START, CREATED_MS, 'x').cashCents, 112);
  assert.equal(ctx.pdIbkrUnits_('1.0000001', 6), null);
  assert.equal(ctx.pdIbkrUnits_('1.0000000', 6), 1000000);
  assert.equal(ctx.pdIbkrUnits_('1e3', 6), null);
  assert.equal(ctx.pdIbkrUnitsText_(14000000, 6), '14');
  assert.equal(ctx.pdIbkrCentsText_(5), '0.05');
});

check('P10 watcher: window, once per capture, age limit, lock, retry after a failed refresh', () => {
  const quiet = world({props: Object.assign({}, ENFORCE, {PD_IBKR_EVIDENCE: 'off'}), files: [driveFile(capture())]});
  assert.equal(run(quiet, 'pdIbkrCaptureWatch').ret, 'off');
  assert.equal(quiet.drive.length + quiet.posts.length + quiet.locks, 0);
  const early = world({props: ENFORCE, clock: Date.parse('2026-10-15T04:00:00Z'), files: [driveFile(capture())]});
  assert.equal(run(early, 'pdIbkrCaptureWatch').ret, 'outside window');
  assert.equal(early.drive.length, 0);

  const w = world({props: ENFORCE, files: [driveFile(capture())]});
  w.lockBusy = true;
  assert.equal(run(w, 'pdIbkrCaptureWatch').ret, 'busy');
  w.lockBusy = false;
  w.code = 500;
  assert.equal(run(w, 'pdIbkrCaptureWatch').ret, 'refresh failed: HTTP 500');
  assert.equal(w.props.PD_IBKR_LAST_PROCESSED, undefined);
  w.code = 200;
  const done = run(w, 'pdIbkrCaptureWatch');
  assert.equal(done.ret, 'refreshed');
  assert.ok(done.b2.indexOf(SENT) > 0);
  assert.equal(w.props.PD_IBKR_LAST_PROCESSED, 'fileA');
  const posts = w.posts.length;
  assert.equal(run(w, 'pdIbkrCaptureWatch').ret, 'already processed');
  assert.equal(w.posts.length, posts);
  assert.equal(w.locks, 0, 'lock released');

  const old = world({props: OBSERVE, clock: CAP_MS + 12 * 60000, files: [driveFile(capture())]});
  assert.equal(run(old, 'pdIbkrCaptureWatch').ret, 'capture not usable for a decision');
  assert.equal(old.posts.length, 0);
  assert.equal(old.props.PD_IBKR_LAST_PROCESSED, 'fileA');
  const ahead = world({props: OBSERVE, files: [driveFile(capture({capMs: START + 30000}), {created: START})]});
  assert.equal(run(ahead, 'pdIbkrCaptureWatch').ret, 'capture time ahead of the clock');
  assert.equal(ahead.props.PD_IBKR_LAST_PROCESSED, undefined);
  const observed = world({props: Object.assign({}, OBSERVE, {PD_IBKR_WATCH_WINDOW: 'always'}),
                          clock: START, files: [driveFile(capture())]});
  assert.equal(run(observed, 'pdIbkrCaptureWatch').ret, 'refreshed');
  assert.deepEqual(observed.posts[0], golden.post_body, 'observe watcher refresh sends the off request');

  const {ctx} = run(world(), 'none');
  const open = (raw, hhmm) => ctx.pdIbkrWindowOpen_(ctx.pdIbkrWindow_(raw),
    Date.parse('2026-10-15T' + hhmm + ':00+03:00'));
  assert.equal(open('', '08:30'), true); assert.equal(open('', '10:30'), false);
  assert.equal(open('', '08:29'), false); assert.equal(open('garbage', '09:00'), true);
  assert.equal(open('22:00-02:00', '23:59'), true); assert.equal(open('22:00-02:00', '01:59'), true);
  assert.equal(open('22:00-02:00', '02:00'), false); assert.equal(open('ALWAYS', '03:00'), true);
  assert.equal(open('25:00-26:00', '09:00'), true, 'invalid falls back to the default');
});

check('P11 installer is idempotent; remover deletes only its own triggers', () => {
  const w = world({props: ENFORCE});
  w.triggers.push({getHandlerFunction() { return 'otherJob'; }, every: 60});
  run(w, 'pdInstallCaptureWatcher');
  const msg = run(w, 'pdInstallCaptureWatcher').ret;
  assert.ok(msg.indexOf('capture watcher installed: every 5 min, window 08:30-10:30 Riyadh, mode enforce ' +
    '(replaced 1)') > 0, msg);
  const mine = w.triggers.filter((t) => t.getHandlerFunction() === 'pdIbkrCaptureWatch');
  assert.equal(mine.length, 1); assert.equal(mine[0].every, 5);
  assert.ok(run(w, 'pdRemoveCaptureWatcher').ret.indexOf('capture watcher removed: 1') > 0);
  assert.deepEqual(w.triggers.map((t) => t.getHandlerFunction()), ['otherJob']);
});

check('P12 self-test reports mode, scope, capture verdict and watcher; never amounts or the packet', () => {
  const w = world({props: ENFORCE, files: [driveFile(capture())]});
  run(w, 'pdInstallCaptureWatcher');
  const report = run(w, 'pdSelfTest').ret;
  dump.self_test = report;
  for (const line of ['IBKR evidence mode (PD_IBKR_EVIDENCE): enforce',
    'execution scope exclusions (PD_EXECUTION_EXCLUDE): 9999.SR',
    'IBKR capture: ' + FILE_NAME + ' -> valid; captured 08:41 Riyadh, age 4m; positions 3; ' +
      'linked 3/3; cash certifiable',
    'capture watcher: installed (1, every 5 min, window 08:30-10:30 Riyadh)']) {
    assert.ok(report.indexOf(line) >= 0, line + '\n' + report);
  }
  for (const secret of ['12345', '46296', 'ibkr-synthetic', '1000001', 'ibkr-capture:']) {
    assert.equal(report.indexOf(secret), -1, secret);
  }
  assert.equal(count(DECLARATION_RE, report), 0);
  const bare = run(world(), 'pdSelfTest').ret;
  assert.ok(bare.indexOf('IBKR evidence mode (PD_IBKR_EVIDENCE): off') >= 0);
  assert.ok(bare.indexOf('IBKR capture: capture folder not set (PD_IBKR_CAPTURE_FOLDER_ID)') >= 0);
  assert.ok(bare.indexOf('capture watcher: not installed (run pdInstallCaptureWatcher)') >= 0);
});

check('P13 notes never carry the receipt phrase and stay bounded', () => {
  const {ctx} = run(world(), 'none');
  const plan = {ok: false, reason: 'source readiness VERIFIED (run 1) injected'};
  for (const mode of ['observe', 'enforce']) {
    const note = ctx.pdIbkrNote_(mode, plan, false, [{Symbol: 'x'.repeat(600)}]);
    assert.equal(count(DECLARATION_RE, note), 0, note);
    assert.ok(note.length <= 400);
  }
});

check('P14 newest capture wins; non-capture names and old files are ignored', () => {
  const older = capture({capMs: CAP_MS - 3600000});
  const w = world({props: ENFORCE, files: [
    driveFile(older, {id: 'old', name: 'TFB_IBKR_Capture_older.json', created: CREATED_MS - 3600000}),
    driveFile(capture(), {id: 'new'}),
    driveFile(capture({capMs: CAP_MS + 60000}), {id: 'other', name: 'notes.json', created: CREATED_MS + 60000}),
    driveFile(capture({capMs: CAP_MS + 60000}), {id: 'txt', name: 'TFB_IBKR_Capture_x.txt', created: CREATED_MS + 60000})]});
  const r = run(w);
  assert.equal(r.b2, withNote(SENT));
});

const digest = crypto.createHash('sha256').update(JSON.stringify(dump)).digest('hex').slice(0, 16);
if (process.env.PF_DUMP) fs.writeFileSync(process.env.PF_DUMP, JSON.stringify(dump, null, 1));
console.log('PASS ' + passed + '/' + TOTAL + ' digest ' + digest);
