/* 10_My_Portfolio v1.5.15 [PD-ALLCASH]: a broker-proven all-cash book is a
 * decided state. Runs the REAL bound source in a vm with Apps Script service
 * stubs on the 24 Oct shape: one My_Portfolio row outside the execution
 * scope, an IBKR capture with 0 positions, the backend answering 'empty'.
 * Every holding, contract id, amount and account here is synthetic.
 *
 *   node tests/js/pf_allcash_ok.test.js
 *   PF_GAS=<file>        source under test (default apps_script/10_My_Portfolio.gs)
 *   PF_BASE_GAS=<file>   optional v1.5.14 copy, run live and compared to the golden
 *   PF_WRITE_GOLDEN=1    regenerate the golden (PF_GAS must be v1.5.14)
 *   PF_DUMP=<file>       write the produced lines and request bodies
 */
'use strict';
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const GAS = process.env.PF_GAS || path.join(__dirname, '../../apps_script/10_My_Portfolio.gs');
const GOLDEN = path.join(__dirname, '../fixtures/pf_allcash_v1_5_14_golden.json');
const SOURCE = fs.readFileSync(GAS, 'utf8');
const RUN = '6363636363';
const FOLDER = 'synthFolder_0123456789';
const START = Date.parse('2026-10-24T14:43:37Z');          // 17:43:37 Riyadh (the 24 Oct B2)
const CAP_MS = Date.parse('2026-10-24T14:39:20.117Z');     // 17:39 Riyadh, age 4m
const CREATED_MS = CAP_MS + 25000;
const FILE_NAME = 'TFB_IBKR_Capture_20261024T143920Z.json';
const PANEL_CASH = 500;
const TOKEN = 'book all-cash (broker 0 positions, decisions 0)';
const RECEIPT_RE = /(?<![a-z0-9_])source readiness VERIFIED \(run ([1-9][0-9]*)\)/gi;
const DECLARATION_RE = /(?<![a-z0-9_])source\s+readiness\b/gi;

let passed = 0;
const TOTAL = 7;
const dump = {};
function check(label, fn) { fn(); passed++; console.log('PASS ' + label); }
function count(re, text) { return (String(text).match(re) || []).length; }
const plain = (v) => JSON.parse(JSON.stringify(v));
const iso = (ms) => new Date(ms).toISOString();
const sha = (x) => crypto.createHash('sha256').update(typeof x === 'string' ? x : JSON.stringify(x)).digest('hex');
const riyadhIso = (ms) => new Date(ms + 3 * 3600000).toISOString().slice(0, 23) + '415+03:00';

const STATUS_HEADER = ['Page', 'Last Updated', 'Status', 'Message', 'Endpoint',
  'HTTP Code', 'Rows', 'Columns', 'Duration ms', 'Warnings'];
function statusGrid(stampMs, rows) {
  const msg = '[STATUS-STAMP v6.70.0] leg=success written=' + rows + ' failed=0 requested=' + rows +
    ' fresh=' + rows + ' acquired=' + rows + '/' + rows + ' acquisition=COMPLETE fetchfail_requested=observe ' +
    'fetchfail_effective=observe warnings=0 | data=COMPLETE policy_data=COMPLETE sha=deadbeef run=' + RUN;
  const stamp = new Date(stampMs + 3 * 3600000).toISOString().slice(0, 19).replace('T', ' ') + '+03:00';
  return [STATUS_HEADER, ['My_Portfolio', stamp, 'SUCCESS', msg, 'backend:run_dashboard_sync', '',
                          rows, 122, 1000, '']];
}

const HOLD_HEADERS = ['Symbol', 'Name', 'Sector', 'Exchange', 'Currency', 'Position Qty',
  'Avg Cost', 'Current Price', 'Price Target', 'Intrinsic Value', 'Expected ROI 12M',
  'Forecast Reliability Score', 'Data Quality Score', 'Risk Bucket', 'Volatility 30D',
  'Avg Volume 30D', 'Recommendation Detail', 'Investability Status', 'Data Provider',
  'Last Updated (UTC)', 'Warnings'];
/* The 24 Oct shape: the only row is a sukuk held outside the IBKR account. */
function holdRows(refMs) {
  const acq = iso(refMs - 120000), quote = iso(refMs - 150000);
  const warn = (p) => 'acquisition_status:success; acquisition_acquired_at:' + acq +
    '; acquisition_provider:' + p + '; acquisition_quote_asof:' + quote;
  return [
    ['9999.SR', 'Synthetic Sukuk', 'Fixed Income', 'SAU', 'SAR', 10, 1000, 998.5, '', '', '',
     60, 80, 'Low', 2, 1000, 'HOLD', 'INVESTABLE', 'yahoo_chart', acq, warn('yahoo_chart')]
  ];
}
/* What core/analysis/portfolio_actions._build_verified answers for 0 rows. */
const META = {route: {version: '4.18.2', request_id: 'req-synthetic', holdings: {received: 0, used: 0}},
              versions: {portfolio_actions: '1.16.0', opportunity_builder: '1.26.1'}};
const EMPTY = {status: 'empty', kpis: {portfolio_value_sar: 82819, holdings_value_sar: null,
  cash_sar: 82819, cash_pct: 100, positions: 0, action_counts: {ADD: 0, HOLD: 0, TRIM: 0, EXIT: 0, BLOCK: 0}},
  actions: [], sector_summary: [], alerts: [], meta: Object.assign({reason: 'no holdings rows supplied'}, META)};
const OK_EMPTY = Object.assign({}, EMPTY, {status: 'ok'});
const ERROR = Object.assign({}, EMPTY, {status: 'error', meta: Object.assign({reason: 'ValueError: synthetic'}, META)});
const FX_LOOKUP = [['SAR', 'Saudi Riyal', '', 1], ['USD', 'US Dollar', '', 3.75547]];

/* The all-cash capture: no positions, USD settled cash only, no orders. */
function capture(o) {
  o = o || {};
  const c = {schema: 'tfb.ibkr-capture/v1', captured_at: riyadhIso(o.capMs || CAP_MS),
    account_alias: 'ibkr-synthetic', producer: 'synthetic',
    positions: {positions: []},
    balances: {balances: [
      {currency: 'BASE', cash_balance: 22085.12, settled_cash: 22085.12},
      {currency: 'USD', cash_balance: 22085.12, settled_cash: 22085.12}]},
    orders: {orders: []}};
  if (o.mutate) o.mutate(c);
  return c;
}
const ONE_POSITION = capture({mutate(c) {
  c.positions.positions.push({contract_id: 1000001, contract_description: 'SYNA', position: 40,
                              currency: 'USD', asset_class: 'STK'});
}});
const CLOSED_LINE = capture({mutate(c) {
  c.positions.positions.push({contract_id: 1000009, contract_description: 'SYNZ', position: 0,
                              currency: 'USD', asset_class: 'STK'});
}});
function driveFile(cap, o) {
  o = o || {};
  return {id: o.id || 'fileA', name: o.name || FILE_NAME,
          created: o.created === undefined ? CREATED_MS : o.created,
          text: typeof cap === 'string' ? cap : JSON.stringify(cap)};
}
const ENFORCE = {PD_IBKR_EVIDENCE: 'enforce', PD_IBKR_CAPTURE_FOLDER_ID: FOLDER,
                 PD_EXECUTION_EXCLUDE: '9999.SR', PD_SOURCE_RECEIPT: 'enforce'};

function world(o) {
  o = o || {};
  return {props: Object.assign({PD_ACTION_LOG: 'off'}, ENFORCE, o.props || {}),
          docProps: {}, files: o.files === undefined ? [driveFile(capture())] : o.files,
          triggers: [], posts: [], drive: [], response: o.response || EMPTY,
          clock: o.clock || START, code: 200, statusMs: o.statusMs || START - 60000,
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
  const grid = statusGrid(w.statusMs, rows.length);
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
      return {getContentText() { return JSON.stringify(w.response); },
              getResponseCode() { return w.code; }};
    }},
    DriveApp: {searchFiles(q) {
      w.drive.push(q);
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
    LockService: {getScriptLock() { return {tryLock() { return true; }, releaseLock() {}}; }},
    ScriptApp: {getProjectTriggers() { return w.triggers.slice(); }},
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

/* Fail-closed scenarios: on v1.5.14 and on v1.5.15 these lines must agree. */
const CLOSED = [
  ['no capture', {files: []}],
  ['stale capture (24m)', {clock: START + 20 * 60000}],
  ['broker holds 1 position not on the page', {files: [driveFile(ONE_POSITION)]}],
  ['status ok with 0 rows', {response: OK_EMPTY}],
  ['status error', {response: ERROR}],
  ['empty but actions present', {response: Object.assign({}, EMPTY,
    {actions: [{action: 'BLOCK', symbol: 'SYNA.US', name: 'Synthetic Alpha'}]})}],
  ['ibkr observe', {props: {PD_IBKR_EVIDENCE: 'observe'}}],
  ['ibkr off', {props: {PD_IBKR_EVIDENCE: 'off'}}],
  ['capture folder unset', {props: {PD_IBKR_CAPTURE_FOLDER_ID: ''}}],
];
function scenarios(source) {
  const out = {};
  const main = run(world(), null, source);
  out.allcash = {b2: main.b2, post: main.post, writes_sha256: sha(main.writes), ret: main.ret,
                 logs: main.logs.filter((l) => l.indexOf('done status=') > 0)};
  for (const [label, o] of CLOSED) {
    const r = run(world(o), null, source);
    out[label] = {b2: r.b2, post: r.post, writes_sha256: sha(r.writes), ret: r.ret};
  }
  return out;
}
const functionNames = (text) => [...text.matchAll(/^function\s+([A-Za-z0-9_$]+)\s*\(/gm)].map((m) => m[1]).sort();

/* ------------------------------------------------------------------ golden */
if (process.env.PF_WRITE_GOLDEN) {
  const base = scenarios();
  fs.writeFileSync(GOLDEN, JSON.stringify({
    note: 'refreshPortfolioDecision output of 10_My_Portfolio.gs v1.5.14 on the synthetic 24 Oct all-cash scenario ' +
          'and its fail-closed variants; functions = every top-level function of v1.5.14',
    source_sha256: sha(SOURCE), source_lines: SOURCE.split('\n').length,
    non_ascii_lines: SOURCE.split('\n').filter((l) => /[^\x00-\x7f]/.test(l)).length,
    functions: functionNames(SOURCE), scenarios: base}, null, 1) + '\n');
  console.log('golden written from ' + GAS);
  process.exit(0);
}
const golden = JSON.parse(fs.readFileSync(GOLDEN, 'utf8'));
const G = golden.scenarios;
const POOL = 'holdings 1 rows from My_Portfolio (qty/cost from cost-basis: 0)';
const SENT = ' | ibkr evidence SENT (capture 17:39 age 4m; linked 0/0; cash 82819.20 SAR; USD 3.75; excluded 9999.SR)';
const TAIL = ' | route v4.18.2 | actions v1.16.0 | builder v1.26.1 | req req-synthetic | 0s | ActLog: off';

check('T1 v1.5.14 (golden negative): the all-cash book writes status empty and withholds the receipt', () => {
  assert.equal(G.allcash.b2, 'Last run 2026-10-24 17:43:37 | status: empty | ' + POOL +
    ' | source readiness WITHHELD: decision status empty' + SENT + TAIL);
  assert.equal(count(RECEIPT_RE, G.allcash.b2), 0);
  assert.equal(G.allcash.post.rows.length, 0);
  assert.equal(G.allcash.post.reconciliation_evidence.accounts[0].positions.length, 0);
  assert.equal(G.allcash.post.controls['PF: Cash Available SAR'], 82819.2);
  for (const [label] of CLOSED) assert.ok(G[label] && G[label].b2.indexOf('status: ') > 0, label);
  assert.equal(golden.functions.length >= 118, true);
  if (process.env.PF_BASE_GAS) {
    const base = fs.readFileSync(process.env.PF_BASE_GAS, 'utf8');
    assert.equal(sha(base), golden.source_sha256, 'PF_BASE_GAS is not the golden source');
    assert.equal(base.indexOf('pdAllCashProof_'), -1);
    assert.deepEqual(plain(scenarios(base)), G, 'the golden is the live v1.5.14 output');
  }
});

const NEW_LINE = 'Last run 2026-10-24 17:43:37 | status: ok | ' + TOKEN + ' | ' + POOL +
  ' | source readiness VERIFIED (run ' + RUN + ')' + SENT + TAIL;

check('T2 v1.5.15 enforce (default): status ok, the all-cash token, the receipt; request, render and log unchanged', () => {
  for (const raw of [undefined, 'enforce', ' ENFORCE ', '', 'on', 'true', 'observe', 'garbage']) {
    const w = world({props: raw === undefined ? {} : {PD_ALLCASH_OK: raw}});
    const r = run(w);
    if (raw === undefined) dump.allcash_enforce = r.b2;
    assert.equal(r.b2, NEW_LINE, 'PD_ALLCASH_OK=' + JSON.stringify(raw));
    assert.equal(count(DECLARATION_RE, r.b2), 1);
    const m = [...r.b2.matchAll(RECEIPT_RE)];
    assert.equal(m.length, 1); assert.equal(m[0][1], RUN);
    assert.deepEqual(r.post, G.allcash.post, 'the request is the v1.5.14 request');
    assert.equal(sha(r.writes), G.allcash.writes_sha256, 'the render is the v1.5.14 render');
    assert.equal(r.ret, 'empty', 'the return value is the backend status');
    assert.deepEqual(r.logs.filter((l) => l.indexOf('done status=') > 0),
                     G.allcash.logs.map((l) => l.replace('v1.5.14', 'v' + SOURCE.match(/var PD_VERSION = '([0-9.]+)';/)[1])), 'Logger line unchanged in form');   // merge note 2026-10-25 (#780): panel preserve is v1.5.16 over #834's v1.5.15
    assert.equal(w.drive.length, 1);
  }
  /* a zero-quantity closed line in the capture is not a position (pdIbkrPlan_ counts qty > 0) */
  const closed = run(world({files: [driveFile(CLOSED_LINE)]}));
  assert.equal(closed.b2, NEW_LINE);
  assert.equal(closed.post.reconciliation_evidence.accounts[0].positions.length, 1);
  /* the status token does not depend on the receipt mode */
  const noReceipt = run(world({props: {PD_SOURCE_RECEIPT: 'off'}}));
  dump.allcash_no_receipt = noReceipt.b2;
  assert.equal(noReceipt.b2, 'Last run 2026-10-24 17:43:37 | status: ok | ' + TOKEN + ' | ' + POOL + SENT + TAIL);
  assert.equal(count(DECLARATION_RE, noReceipt.b2), 0);
  const observe = run(world({props: {PD_SOURCE_RECEIPT: 'observe'}}));
  assert.equal(observe.b2, 'Last run 2026-10-24 17:43:37 | status: ok | ' + TOKEN + ' | ' + POOL +
    ' | [receipt-observe] would publish VERIFIED (run ' + RUN + ')' + SENT + TAIL);
  /* the backend's casing of the status does not matter */
  const upper = run(world({response: Object.assign({}, EMPTY, {status: 'EMPTY'})}));
  assert.equal(upper.b2, NEW_LINE);
});

check('T3 kill switch PD_ALLCASH_OK=off: every line, the request and the render are v1.5.14 byte-identical', () => {
  for (const raw of ['off', ' OFF ', 'Off']) {
    const w = world({props: {PD_ALLCASH_OK: raw}});
    const r = run(w);
    if (raw === 'off') dump.allcash_off = r.b2;
    assert.equal(r.b2, G.allcash.b2, raw);
    assert.deepEqual(r.post, G.allcash.post, raw);
    assert.equal(sha(r.writes), G.allcash.writes_sha256, raw);
    assert.equal(r.ret, G.allcash.ret, raw);
    for (const [label, o] of CLOSED) {
      const c = run(world(Object.assign({}, o, {props: Object.assign({PD_ALLCASH_OK: raw}, o.props || {})})));
      assert.equal(c.b2, G[label].b2, raw + ' / ' + label);
      assert.deepEqual(c.post, G[label].post, raw + ' / ' + label);
      assert.equal(sha(c.writes), G[label].writes_sha256, raw + ' / ' + label);
    }
  }
});

check('T4 the proof fails closed: ' + CLOSED.length + ' variants are v1.5.14 byte-identical under enforce', () => {
  dump.closed = {};
  for (const [label, o] of CLOSED) {
    const r = run(world(o));
    dump.closed[label] = r.b2;
    assert.equal(r.b2, G[label].b2, label);
    assert.deepEqual(r.post, G[label].post, label);
    assert.equal(sha(r.writes), G[label].writes_sha256, label);
    assert.equal(r.ret, G[label].ret, label);
    assert.equal(r.b2.indexOf(TOKEN), -1, label);
    assert.equal(count(RECEIPT_RE, r.b2), label === 'status ok with 0 rows' ? 1 : 0, label);
  }
  assert.ok(G['no capture'].b2.indexOf('status: empty | ' + POOL + ' | source readiness WITHHELD: decision status empty' +
    ' | ibkr evidence NOT SENT: no capture in the last 36h; USD 3.75; excluded 9999.SR | ') > 0);
  assert.ok(G['stale capture (24m)'].b2.indexOf('ibkr evidence NOT SENT: capture 17:39 is 24m old (limit 14m)') > 0);
  assert.ok(G['broker holds 1 position not on the page'].b2.indexOf(
    'ibkr evidence SENT (capture 17:39 age 4m; linked 0/0; IBKR not on page: SYNA; cash 82819.20 SAR') > 0);
  assert.ok(G['status ok with 0 rows'].b2.indexOf('status: ok | ' + POOL + ' | source readiness VERIFIED (run ' + RUN + ')') > 0);
  assert.ok(G['status error'].b2.indexOf('status: error | ' + POOL + ' | source readiness WITHHELD: decision status error') > 0);
});

check('T5 pdAllCashProof_ is pure and fail-closed on every input; pdAllCashMode_ is off only on off', () => {
  const {ctx} = run(world(), 'none');
  const proof = (p, i) => ctx.pdAllCashProof_(p, i);
  const ibkr = {note: ' | ibkr evidence SENT (capture 17:39 age 4m; linked 0/0; cash 82819.20 SAR; USD 3.75)',
                excluded: [], controls: {}, sent: true, positions: 0, linked: 0, ageMs: 257000};
  const payload = {status: 'empty', actions: []};
  assert.equal(proof(payload, ibkr), true);
  assert.equal(proof({status: 'EMPTY', actions: []}, ibkr), true);
  const bad = [
    ['no payload', null, ibkr], ['no ibkr', payload, null], ['ibkr off', payload, undefined],
    ['status ok', {status: 'ok', actions: []}, ibkr], ['status error', {status: 'error', actions: []}, ibkr],
    ['status missing', {actions: []}, ibkr], ['status null', {status: null, actions: []}, ibkr],
    ['actions missing', {status: 'empty'}, ibkr], ['actions not a list', {status: 'empty', actions: 0}, ibkr],
    ['actions present', {status: 'empty', actions: [{action: 'BLOCK'}]}, ibkr],
    ['not sent', payload, Object.assign({}, ibkr, {sent: false})],
    ['sent not boolean', payload, Object.assign({}, ibkr, {sent: 'true'})],
    ['note NOT SENT', payload, Object.assign({}, ibkr, {note: ' | ibkr evidence NOT SENT: no capture; USD 3.75'})],
    ['note observe', payload, Object.assign({}, ibkr, {note: ' | [ibkr-observe] would send (linked 0/0)'})],
    ['note missing', payload, Object.assign({}, ibkr, {note: undefined})],
    ['positions 1', payload, Object.assign({}, ibkr, {positions: 1})],
    ['positions null', payload, Object.assign({}, ibkr, {positions: null})],
    ['positions text', payload, Object.assign({}, ibkr, {positions: '0'})],
    ['positions missing', payload, (() => { const o = Object.assign({}, ibkr); delete o.positions; return o; })()],
    ['linked 1', payload, Object.assign({}, ibkr, {linked: 1})],
    ['linked null', payload, Object.assign({}, ibkr, {linked: null})],
    ['age null', payload, Object.assign({}, ibkr, {ageMs: null})],
    ['age negative', payload, Object.assign({}, ibkr, {ageMs: -1})],
    ['age over the send limit', payload, Object.assign({}, ibkr, {ageMs: 840001})],
    ['age NaN', payload, Object.assign({}, ibkr, {ageMs: NaN})],
    ['age text', payload, Object.assign({}, ibkr, {ageMs: '4m'})],
  ];
  for (const [label, p, i] of bad) assert.equal(proof(p, i), false, label);
  assert.equal(proof(payload, Object.assign({}, ibkr, {ageMs: 840000})), true, 'age at the send limit');
  assert.equal(proof(payload, Object.assign({}, ibkr, {ageMs: 0})), true, 'age 0');
  for (const [raw, want] of [[undefined, 'enforce'], ['', 'enforce'], ['enforce', 'enforce'], ['off', 'off'],
                             [' OFF ', 'off'], ['false', 'enforce'], ['0', 'enforce'], ['observe', 'enforce']]) {
    const w = world({props: raw === undefined ? {} : {PD_ALLCASH_OK: raw}});
    assert.equal(run(w, 'pdAllCashMode_').ret, want, JSON.stringify(raw));
  }
});

check('T6 pdIbkrApply_ exposes the plan counts read-only; the proof reads them (never the note alone)', () => {
  const {ctx} = run(world(), 'none');
  const body = () => ({rows: [], controls: {'PF: Cash Available SAR': PANEL_CASH}, fx_rates: {SAR: 1, USD: 3.75547}});
  const fresh = ctx.pdIbkrApply_('enforce', body());
  assert.equal(fresh.sent, true);
  assert.equal(fresh.positions, 0); assert.equal(fresh.linked, 0);
  assert.equal(fresh.ageMs, START - CAP_MS);
  assert.deepEqual(Object.keys(fresh).sort(), ['ageMs', 'controls', 'excluded', 'linked', 'note', 'positions', 'sent']);
  const none = run(world({files: []}), 'none').ctx.pdIbkrApply_('enforce', body());
  assert.equal(none.sent, false);
  assert.equal(none.positions, null); assert.equal(none.linked, null); assert.equal(none.ageMs, null);
  const one = run(world({files: [driveFile(ONE_POSITION)]}), 'none').ctx.pdIbkrApply_('enforce', body());
  assert.equal(one.sent, true); assert.equal(one.positions, 1); assert.equal(one.linked, 0);
  const observe = run(world(), 'none').ctx.pdIbkrApply_('observe', body());
  assert.equal(observe.sent, false); assert.equal(observe.positions, 0);
  assert.equal(ctx.pdAllCashProof_({status: 'empty', actions: []}, observe), false, 'observe never proves');
});

check('T7 self-test reports the gate; the receipt note keeps its v1.5.14 verdicts without the 7th argument', () => {
  const on = run(world(), 'pdSelfTest').ret;
  dump.self_test = on;
  assert.ok(on.indexOf('all-cash gate (PD_ALLCASH_OK): enforce') >= 0, on);
  assert.ok(on.indexOf('IBKR capture: ' + FILE_NAME + ' -> valid; captured 17:39 Riyadh, age 4m; positions 0; ' +
    'linked 0/0; cash certifiable') >= 0, on);
  assert.equal(count(DECLARATION_RE, on), 0);
  const off = run(world({props: {PD_ALLCASH_OK: 'off'}}), 'pdSelfTest').ret;
  assert.ok(off.indexOf('all-cash gate (PD_ALLCASH_OK): off') >= 0, off);
  for (const secret of ['22085', '82819', 'ibkr-synthetic', 'ibkr-capture:']) {
    assert.equal(on.indexOf(secret), -1, secret);
  }
  const {ctx} = run(world(), 'none');
  const ss = ctx.SpreadsheetApp.getActiveSpreadsheet();
  const sourceCtx = ctx.pdSrcCapture_(ss);
  const six = ctx.pdReceiptNote_('enforce', ss, sourceCtx, 1, EMPTY, null);
  assert.equal(six, ' | source readiness WITHHELD: decision status empty');
  assert.equal(ctx.pdReceiptNote_('enforce', ss, sourceCtx, 1, EMPTY, null, false), six);
  assert.equal(ctx.pdReceiptNote_('enforce', ss, sourceCtx, 1, EMPTY, null, 'true'), six, 'only boolean true');
  assert.equal(ctx.pdReceiptNote_('enforce', ss, sourceCtx, 1, EMPTY, null, true),
               ' | source readiness VERIFIED (run ' + RUN + ')');
  assert.equal(ctx.pdReceiptNote_('enforce', ss, sourceCtx, 1, ERROR, null, true),
               ' | source readiness WITHHELD: decision status error', 'all-cash never excuses another status');
  assert.equal(ctx.pdReceiptNote_('enforce', ss, sourceCtx, 2, EMPTY, null, true),
               ' | source readiness WITHHELD: consumed 2 holdings vs 1 published rows', 'the cohort check stays');
});

const digest = sha(dump).slice(0, 16);
if (process.env.PF_DUMP) fs.writeFileSync(process.env.PF_DUMP, JSON.stringify(dump, null, 1));
console.log('PASS ' + passed + '/' + TOTAL + ' digest ' + digest);
