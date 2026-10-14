/* 10_My_Portfolio v1.5.11+: Portfolio_Decision source receipt and private
 * reconciliation-evidence transport; v1.5.13 binds the receipt to the rows
 * actually read (each row's Last Updated (UTC) inside the run window and
 * unchanged after the decision). Runs the REAL bound source in a vm with
 * Apps Script service stubs; holdings, accounts and runs are synthetic.
 *
 *   node tests/js/pf_source_receipt.test.js
 *   PF_GAS=<file>       source under test (default apps_script/10_My_Portfolio.gs)
 *   PF_BASE_GAS=<file>  optional v1.5.10 copy, compared live in off mode
 *   PF_DUMP=<file>      write produced status lines and payloads as JSON
 */
'use strict';
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const GAS = process.env.PF_GAS || path.join(__dirname, '../../apps_script/10_My_Portfolio.gs');
const GOLDEN = path.join(__dirname, '../fixtures/pf_receipt_v1_5_10_golden.json');
const SOURCE = fs.readFileSync(GAS, 'utf8');
const RUN = '4242424242';
const START = Date.parse('2026-10-14T05:50:11Z');           // 08:50:11 Riyadh
const PACKET = {schema_version: 1, source_ref: 'synthetic://declared-capture-7f3a',
  captured_at: '2026-10-14T05:49:30Z', accounts: [{account_id: 'synthetic-account'}],
  holding_links: [], funding_accounts: ['synthetic-account']};
const RECEIPT_RE = /(?<![a-z0-9_])source readiness VERIFIED \(run ([1-9][0-9]*)\)/gi;
const DECLARATION_RE = /(?<![a-z0-9_])source\s+readiness\b/gi;

let passed = 0;
const dump = {};
function check(label, fn) { fn(); passed++; console.log('PASS ' + label); }
function count(re, text) { return (String(text).match(re) || []).length; }

const STATUS_HEADER = ['Page', 'Last Updated', 'Status', 'Message', 'Endpoint',
  'HTTP Code', 'Rows', 'Columns', 'Duration ms', 'Warnings'];
function statusMessage(t) {
  t = Object.assign({run: RUN, acquired: '2/2', acquisition: 'COMPLETE', data: 'COMPLETE',
    fetchfail_effective: 'observe', extra: ''}, t || {});
  return '[STATUS-STAMP v6.69.0] leg=success written=2 failed=0 requested=2 fresh=2 ' +
    'acquired=' + t.acquired + ' acquisition=' + t.acquisition +
    ' fetchfail_requested=observe fetchfail_effective=' + t.fetchfail_effective +
    ' warnings=0 | data=' + t.data + ' policy_data=COMPLETE sha=deadbeef' +
    (t.run === null ? '' : ' run=' + t.run) + t.extra;
}
function statusGrid(o) {
  o = o || {};
  const mp = ['My_Portfolio', o.stamp || '2026-10-14 02:46:45+03:00', o.state || 'SUCCESS',
    o.message || statusMessage(o.tokens), 'backend:run_dashboard_sync', '',
    o.rows === undefined ? 2 : o.rows, 122, 1000, ''];
  const grid = [STATUS_HEADER,
    ['Market_Leaders', '2026-10-14 02:50:38+03:00', 'SUCCESS', '[STATUS-STAMP] run=' + RUN,
     'backend:run_dashboard_sync', '', 255, 115, 900, '']];
  if (!o.drop) grid.push(mp);
  if (o.duplicate) grid.push(mp.slice());
  return grid;
}
const HOLD_HEADERS = ['Symbol', 'Name', 'Sector', 'Currency', 'Position Qty', 'Avg Cost', 'Current Price',
                      'Last Updated (UTC)'];
/* Last Updated (UTC) sits inside run 4242424242's window (published 23:46:45Z). */
const HOLD_ROWS = [['SYNTH1.US', 'Synthetic One', 'Energy', 'USD', 10, 20.5, 21.25,
                    '2026-10-13T23:45:10.123456+00:00'],
                   ['SYNTH2.SR', 'Synthetic Two', 'Financials', 'SAR', 5, 100, 99.5,
                    '2026-10-13 23:45:11']];
function rowsWithStamps(a, b) {
  return [HOLD_ROWS[0].slice(0, 7).concat([a]), HOLD_ROWS[1].slice(0, 7).concat([b])];
}
const RESPONSE = {status: 'ok', kpis: {}, sector_summary: [], alerts: [],
  actions: [{action: 'BLOCK', symbol: 'SYNTH1.US', name: 'Synthetic One'},
            {action: 'BLOCK', symbol: 'SYNTH2.SR', name: 'Synthetic Two'}],
  meta: {route: {version: '4.18.2', request_id: 'req-synthetic'},
         versions: {portfolio_actions: '1.16.0', opportunity_builder: '1.26.1'}}};

function chain(onSet, nr, nc) {
  const blank = () => Array.from({length: nr || 1}, () => Array.from({length: nc || 1}, () => ''));
  const proxy = new Proxy(function () {}, {get(_t, prop) {
    if (prop === 'getValue') return () => '';
    if (prop === 'getValues') return blank;
    if (prop === 'setValue') return (v) => { if (onSet) onSet(v); return proxy; };
    return () => proxy;
  }});
  return proxy;
}

/** One refreshPortfolioDecision run; returns the final B2 line and the POST. */
function run(opts) {
  opts = opts || {};
  const clock = {now: START};
  class FixedDate extends Date {
    constructor(...a) { if (a.length) super(...a); else super(clock.now); }
    static now() { return clock.now; }
  }
  const scriptProps = Object.assign({PD_ACTION_LOG: 'off'}, opts.scriptProps || {});
  const docProps = opts.docProps || {};
  const logs = [];
  const calls = {status_reads: 0, posts: []};
  let grid = opts.grid || statusGrid();
  let holdRows = opts.rows || HOLD_ROWS;
  const holdHeaders = opts.headers || HOLD_HEADERS;
  const statusSets = [];
  const pd = {
    getRange(r, c, nr, nc) {
      if (r === 4 && c === 1 && arguments.length === 2) {
        return {getValue() { return 'CONTROL PANEL'; }};
      }
      if (r === 2 && c === 2 && arguments.length === 2) return chain((v) => statusSets.push(v));
      return chain(null, nr, nc);
    },
    getMaxRows() { return 60; }, getMaxColumns() { return 26; }
  };
  const mp = {
    __headers: holdHeaders,
    getLastRow() { return holdRows.length + 1; },
    getLastColumn() { return holdHeaders.length; },
    getRange(r, c, nr, nc) {
      assert.equal(r, 2); assert.equal(c, 1);
      return {getValues() { return holdRows.slice(0, nr).map((x) => x.slice(0, nc)); }};
    }
  };
  const status = {
    getLastRow() { return grid.length; },
    getLastColumn() { return 13; },
    getRange(r, c, nr, nc) {
      calls.status_reads++;
      return {getValues() { return grid.slice(0, nr).map((x) => x.slice(0, nc)); }};
    }
  };
  const sheets = {Portfolio_Decision: pd, My_Portfolio: mp};
  if (!opts.noStatusSheet) sheets._Status = status;
  const ss = {
    getSheetByName(name) { return Object.prototype.hasOwnProperty.call(sheets, name) ? sheets[name] : null; },
    getRangeByName() { return null; },
    insertSheet() { return chain(); }
  };
  const ctx = {
    SHEET_NAMES: {MY_PORTFOLIO: 'My_Portfolio'},
    Date: FixedDate,
    Logger: {log(m) { logs.push(String(m)); }},
    Session: {getScriptTimeZone() { return 'Asia/Riyadh'; }},
    Utilities: {formatDate(d) {
      return new Date(d.getTime() + 3 * 3600000).toISOString().slice(0, 19).replace('T', ' ');
    }},
    PropertiesService: {
      getScriptProperties() { return {
        getProperty(k) { return Object.prototype.hasOwnProperty.call(scriptProps, k) ? scriptProps[k] : null; },
        setProperty(k, v) { scriptProps[k] = v; }}; },
      getDocumentProperties() { return {
        getProperty(k) { return Object.prototype.hasOwnProperty.call(docProps, k) ? docProps[k] : null; }}; }
    },
    SpreadsheetApp: {getActiveSpreadsheet() { return ss; }, getActive() { return ss; }, flush() {},
      BorderStyle: {SOLID: 'SOLID', SOLID_THICK: 'SOLID_THICK'}, getUi() { return {alert() {}}; }},
    UrlFetchApp: {fetch(url, options) {
      calls.posts.push({url: url, body: JSON.parse(options.payload), headers: options.headers});
      if (opts.onFetch) grid = opts.onFetch(grid, clock) || grid;
      if (opts.onFetchRows) holdRows = opts.onFetchRows(holdRows) || holdRows;
      const response = opts.response || RESPONSE;
      return {getContentText() { return JSON.stringify(response); },
              getResponseCode() { return opts.code || 200; }};
    }},
    ensureSheetExists_(name) { return ss.getSheetByName(name); },
    readCurrentSheetHeaders_(sheet) { return sheet.__headers || []; }
  };
  vm.createContext(ctx);
  vm.runInContext(SOURCE_FOR(opts), ctx, {filename: '10_My_Portfolio.gs'});
  if (opts.selfTest) {
    return {report: ctx.pdSelfTest(), logs: logs, status_reads: calls.status_reads};
  }
  ctx.refreshPortfolioDecision();
  return {b2: statusSets[statusSets.length - 1], post: calls.posts[0],
          status_reads: calls.status_reads, logs: logs, ctx: ctx};
}
function SOURCE_FOR(opts) { return opts.source || SOURCE; }

const base = run({});
if (process.env.PF_WRITE_GOLDEN) {           // regenerate from a v1.5.10 PF_GAS only
  fs.writeFileSync(GOLDEN, JSON.stringify({
    note: 'refreshPortfolioDecision off-mode output of 10_My_Portfolio.gs v1.5.10 on the synthetic scenario',
    source_sha256: crypto.createHash('sha256').update(SOURCE).digest('hex'),
    b2: base.b2, post_body: base.post.body}, null, 1) + '\n');
  console.log('golden written from ' + GAS);
  process.exit(0);
}
const golden = JSON.parse(fs.readFileSync(GOLDEN, 'utf8'));
dump.off = base.b2;

check('T01 off (default): status line and POST are v1.5.10 byte-identical; _Status never read', () => {
  assert.equal(base.b2, golden.b2);
  assert.deepEqual(base.post.body, golden.post_body);
  assert.equal(base.status_reads, 0);
  assert.equal(count(DECLARATION_RE, base.b2), 0);
  if (process.env.PF_BASE_GAS) {
    const live = run({source: fs.readFileSync(process.env.PF_BASE_GAS, 'utf8')});
    assert.equal(base.b2, live.b2);
    assert.deepEqual(base.post.body, live.post.body);
  }
  for (const raw of ['', 'on', 'true', '1', 'OBSERVE-ish', 'yes']) {
    const r = run({scriptProps: {PD_SOURCE_RECEIPT: raw}});
    assert.equal(r.b2, golden.b2, 'mode ' + JSON.stringify(raw) + ' must be off');
    assert.equal(r.status_reads, 0);
  }
});

const poolNote = 'holdings 2 rows from My_Portfolio (qty/cost from cost-basis: 0)';
function withFragment(fragment) { return golden.b2.replace(poolNote, poolNote + fragment); }

check('T02 observe: tag the audit does not parse; POST unchanged', () => {
  const r = run({scriptProps: {PD_SOURCE_RECEIPT: 'observe'}});
  dump.observe = r.b2;
  assert.equal(r.b2, withFragment(' | [receipt-observe] would publish VERIFIED (run ' + RUN + ')'));
  assert.equal(count(DECLARATION_RE, r.b2), 0);
  assert.deepEqual(r.post.body, golden.post_body);
  assert.ok(r.status_reads >= 2);
});

check('T03 enforce: exactly one receipt bound to the My_Portfolio run', () => {
  for (const raw of ['enforce', ' ENFORCE ']) {
    const r = run({scriptProps: {PD_SOURCE_RECEIPT: raw}});
    dump.enforce = r.b2;
    assert.equal(r.b2, withFragment(' | source readiness VERIFIED (run ' + RUN + ')'));
    assert.equal(count(DECLARATION_RE, r.b2), 1);
    const m = [...r.b2.matchAll(RECEIPT_RE)];
    assert.equal(m.length, 1); assert.equal(m[0][1], RUN);
    assert.deepEqual(r.post.body, golden.post_body);
  }
});

const WITHHELD = [
  ['stale publication', {grid: statusGrid({stamp: '2026-10-13 23:00:00+03:00'})}, 'publication stale (>8h)'],
  ['future publication', {grid: statusGrid({stamp: '2026-10-14 09:00:00+03:00'})}, 'publication is in the future'],
  ['date-only stamp', {grid: statusGrid({stamp: '2026-10-14'})}, 'publication timestamp unverified'],
  ['run missing', {grid: statusGrid({tokens: {run: null}})}, 'publication run unverified'],
  ['run repeated', {grid: statusGrid({tokens: {extra: ' run=' + RUN}})}, 'publication run unverified'],
  ['run not positive', {grid: statusGrid({tokens: {run: 'local'}})}, 'publication run unverified'],
  ['partial acquisition', {grid: statusGrid({tokens: {acquired: '1/2'}})}, 'acquired 1/2 below 100%'],
  ['acquisition receipt missing', {grid: statusGrid({tokens: {acquired: 'x'}})}, 'acquisition receipt unknown/invalid'],
  ['acquisition not complete', {grid: statusGrid({tokens: {acquisition: 'PARTIAL'}})}, 'acquisition not COMPLETE'],
  ['data not complete', {grid: statusGrid({tokens: {data: 'PARTIAL'}})}, 'data not COMPLETE'],
  ['fetchfail error', {grid: statusGrid({tokens: {fetchfail_effective: 'error'}})}, 'acquisition policy error'],
  ['fetchfail unknown', {grid: statusGrid({tokens: {fetchfail_effective: 'maybe'}})}, 'acquisition policy unknown/ambiguous'],
  ['status not ok', {grid: statusGrid({state: 'FAILED'})}, 'status=FAILED'],
  ['published rows differ', {grid: statusGrid({rows: 3})}, 'consumed 2 holdings vs 3 published rows'],
  ['row repeated', {grid: statusGrid({duplicate: true})}, 'My_Portfolio status row repeated'],
  ['row missing', {grid: statusGrid({drop: true})}, 'My_Portfolio status row missing'],
  ['_Status missing', {noStatusSheet: true}, 'publication status unavailable'],
  ['decision status not ok', {response: Object.assign({}, RESPONSE, {status: 'error'})}, 'decision status error'],
  ['publication changed during the decision',
   {onFetch(g) { const n = g.map((x) => x.slice()); n[2][3] = statusMessage({run: '4242424243'}); return n; }},
   'publication changed during the decision'],
  ['publication newer than the read',
   {onFetch(g, clock) { clock.now += 60000; const n = g.map((x) => x.slice());
     n[2][1] = '2026-10-14 08:50:41+03:00'; return n; }},
   'publication newer than the holdings read'],
  ['rows refreshed after the publication (GAS auto refresh, 14 Oct shape)',
   {rows: rowsWithStamps('2026-10-14T04:40:17.486516+00:00', '2026-10-14T04:40:16.867444+00:00')},
   'holdings refreshed after the publication (07:40 > 02:46 Riyadh)'],
  ['rows the run did not rewrite',
   {rows: rowsWithStamps('2026-10-13T19:00:00+00:00', '2026-10-13T23:45:11+00:00')},
   'holdings older than the publication'],
  ['rows rewritten during the decision',
   {onFetchRows() { return rowsWithStamps('2026-10-13T23:46:00+00:00', '2026-10-13 23:45:11'); }},
   'holdings changed during the decision'],
  ['row without a timestamp', {rows: rowsWithStamps('2026-10-13T23:45:10Z', '')},
   'holding timestamp unverified (SYNTH2.SR)'],
  ['no Last Updated (UTC) column', {headers: HOLD_HEADERS.slice(0, 7)},
   'holdings timestamps unavailable'],
  ['not ready before the read',
   {grid: statusGrid({tokens: {data: 'PARTIAL'}}),
    onFetch() { return statusGrid(); }},
   'publication not ready before the holdings read (data not COMPLETE)'],
];
check('T04 enforce withholds with one declaration and a reason, for ' + WITHHELD.length + ' cases', () => {
  dump.withheld = {};
  for (const [label, o, why] of WITHHELD) {
    const r = run(Object.assign({scriptProps: {PD_SOURCE_RECEIPT: 'enforce'}}, o));
    dump.withheld[label] = r.b2;
    assert.equal(count(DECLARATION_RE, r.b2), 1, label);
    assert.equal(count(RECEIPT_RE, r.b2), 0, label);
    assert.ok(r.b2.indexOf(' | source readiness WITHHELD: ') > 0, label + ': ' + r.b2);
    assert.ok(r.b2.indexOf(why) > 0, label + ' expected "' + why + '" in ' + r.b2);
  }
});

check('T05 evidence: a valid packet travels as body.reconciliation_evidence; anything else is absent', () => {
  const ok = run({docProps: {TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1: JSON.stringify(PACKET)}});
  const expected = Object.assign({}, golden.post_body, {reconciliation_evidence: PACKET});
  assert.deepEqual(ok.post.body, expected);
  assert.equal(ok.b2, golden.b2);
  for (const line of ok.logs) assert.equal(line.indexOf(PACKET.source_ref), -1, 'packet logged');
  const bad = ['{', JSON.stringify(Object.assign({}, PACKET, {schema_version: 2})),
    JSON.stringify(Object.assign({}, PACKET, {schema_version: '1'})), '"text"', '[1]', 'null',
    JSON.stringify(Object.assign({}, PACKET, {pad: 'x'.repeat(9000)}))];
  for (const raw of bad) {
    const r = run({docProps: {TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1: raw}});
    assert.deepEqual(r.post.body, golden.post_body, raw.slice(0, 40));
    assert.equal(Object.prototype.hasOwnProperty.call(r.post.body, 'reconciliation_evidence'), false);
  }
});

check('T06 receipt and evidence are independent', () => {
  const r = run({scriptProps: {PD_SOURCE_RECEIPT: 'enforce'},
                 docProps: {TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1: JSON.stringify(PACKET)}});
  assert.equal(r.b2, withFragment(' | source readiness VERIFIED (run ' + RUN + ')'));
  assert.deepEqual(r.post.body.reconciliation_evidence, PACKET);
});

check('T07 _Status clock parsing (port of dt10SourceStampMs_)', () => {
  const {ctx} = run({});
  const ms = (v) => ctx.pdSrcStampMs_(v);
  assert.equal(ms('2026-10-14 02:46:45+03:00'), Date.parse('2026-10-13T23:46:45Z'));
  assert.equal(ms('2026-10-14 02:46:45'), Date.parse('2026-10-13T23:46:45Z'));
  assert.equal(ms('2026-10-14T08:46:22.499000'), Date.parse('2026-10-14T05:46:22.499Z'));
  assert.equal(ms('2026-10-13T23:46:45Z'), Date.parse('2026-10-13T23:46:45Z'));
  const utc = (v) => ctx.pdSrcUtcMs_ ? ctx.pdSrcUtcMs_(v) : 'missing';
  if (ctx.pdSrcUtcMs_) {                     /* v1.5.13: the (UTC) column is UTC when naive */
    assert.equal(utc('2026-10-13 23:45:11'), Date.parse('2026-10-13T23:45:11Z'));
    assert.equal(utc('2026-10-14T04:40:17.486516+00:00'), Date.parse('2026-10-14T04:40:17.486Z'));
    assert.equal(utc(new Date(Date.parse('2026-10-13T23:45:11Z'))), Date.parse('2026-10-13T23:45:11Z'));
    for (const bad of ['', '2026-10-13', 'soon', null]) assert.equal(utc(bad), null, String(bad));
  }
  assert.equal(ms(new Date(Date.parse('2026-10-13T23:46:45Z'))), Date.parse('2026-10-13T23:46:45Z'));
  for (const bad of ['2026-10-14', '2026-02-30 10:00:00', '2026-10-14 24:00:00', '', null, 'soon']) {
    assert.equal(ms(bad), null, String(bad));
  }
});

check('T08 self-test reports mode, publication verdict and evidence presence, never the packet', () => {
  const quiet = run({selfTest: true});
  assert.ok(quiet.report.indexOf('source receipt mode (PD_SOURCE_RECEIPT): off') >= 0);
  assert.ok(quiet.report.indexOf('My_Portfolio publication: ready, run ' + RUN) >= 0);
  if (SOURCE.indexOf('pdSrcBindingCore_') >= 0) {
    assert.ok(quiet.report.indexOf('receipt binding (page rows vs run ' + RUN + '): inside the run window') >= 0,
              quiet.report);
  }
  assert.ok(quiet.report.indexOf('reconciliation evidence property: absent') >= 0);
  const loud = run({selfTest: true, scriptProps: {PD_SOURCE_RECEIPT: 'observe'},
    docProps: {TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1: JSON.stringify(PACKET)}});
  assert.ok(loud.report.indexOf('source receipt mode (PD_SOURCE_RECEIPT): observe') >= 0);
  assert.ok(loud.report.indexOf('reconciliation evidence property: present (schema 1)') >= 0);
  assert.equal(loud.report.indexOf(PACKET.source_ref), -1);
  assert.equal(count(DECLARATION_RE, loud.report), 0);
});

const digest = crypto.createHash('sha256').update(JSON.stringify(dump)).digest('hex').slice(0, 16);
if (process.env.PF_DUMP) fs.writeFileSync(process.env.PF_DUMP, JSON.stringify(dump, null, 1));
console.log('PASS ' + passed + '/8 digest ' + digest);
