/* tests/js/dt10_audit_keep.test.js -- full-source node vm harness for
 * 16_Decision_Top10.gs v1.17.2 HELD SEATS KEPT IN THE WRITTEN AUDIT.
 * VERSION 1.1.0 (2026-10-14): renumbered with the cockpit (v1.13.9 draft ->
 * v1.13.10 over #774's v1.13.9); base copies are now v1.13.9. No case changed.
 * 2026-10-21: renumbered v1.13.11 over #810's v1.13.10; base copies are now
 * main's v1.13.10 (git show 55b47f24:apps_script/16_Decision_Top10.gs).
 * 2026-10-27: renumbered v1.17.2 over #852's v1.17.1; base copies are now
 * main's v1.17.1 (git show e8337a16:apps_script/16_Decision_Top10.gs).
 * 2026-10-25: renumbered v1.15.1 over #838's v1.15.0; base copies were
 * main's v1.15.0 (git show 08a15bc0:apps_script/16_Decision_Top10.gs).
 * 2026-10-22: renumbered v1.13.12 over #815's v1.13.11; base copies were
 * main's v1.13.11 (git show df99bcf3:apps_script/16_Decision_Top10.gs).
 * VERSION 1.0.0 (2026-10-13).
 *
 * WHY: the 2026-10-13 20:37 Riyadh Top_10 export rendered six grace-held
 * seats with blank Rel / DQ / Conf / Engine ROI because the backend's capped
 * written audit never contained them. v1.13.10 sends the stability members as
 * body.criteria.audit_keep_symbols when Script Property DT10_AUDIT_KEEP is
 * exactly 'on'; opportunity_builder 1.27.0 honours them only under its own
 * TFB_OPP_AUDIT_KEEP=1.
 *
 * Executes the COMPLETE Apps Script source in a vm with the fixtures of
 * tests/js/dt10_feed_cohort_binding.test.js (copied verbatim: Sheets and
 * Properties stubs, backend POST, unrelated refresh orchestration);
 * refreshDecisionTop10, dt10StabLoad_ and the request assembly are
 * production code.
 *   K1 reader: exact 'on' arms; every other word, empty, absent or a
 *      property-service failure is off; one memoized read.
 *   K2 pure dt10AuditKeepSymbols_: members only (seated, grace, fast-track),
 *      sorted, capped at 60, [] on missing/malformed state.
 *   K3 REAL refresh, off: no audit_keep_symbols in any POST body.
 *   K4 REAL refresh, on: research and allocation bodies carry the sorted
 *      members; nothing else in the body changes versus off.
 *   K5 REAL refresh, on with no members / unparsable state: body as off.
 *   D1 (DT10_BASE set): off POST bodies equal the v1.13.9 base's.
 *   D2 (DT10_BASE set): removing the designed v1.13.10 regions from the
 *      delivered file gives the base byte for byte.
 *
 *   node tests/js/dt10_audit_keep.test.js
 *   DT10_BASE=<copy of v1.13.9 16_Decision_Top10.gs> enables D1-D2.
 */
'use strict';
const assert = require('node:assert/strict');
const childProcess = require('node:child_process');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const ROOT = path.join(__dirname, '../..');
const SOURCE_PATH = process.env.DT10_DELIVERED || path.join(ROOT, 'apps_script/16_Decision_Top10.gs');
const source = fs.readFileSync(SOURCE_PATH, 'utf8');
const BASE_PATH = process.env.DT10_BASE || '';
const baseSource = BASE_PATH ? fs.readFileSync(BASE_PATH, 'utf8') : null;
const plain = value => (value === undefined ? undefined : JSON.parse(JSON.stringify(value)));

const NOW = Date.parse('2026-10-12T09:30:00Z');
const STAMP = '2026-10-12 12:00:00+03:00';            // 30 minutes before NOW
const COHORT = '90001';
const FLOORS = {Market_Leaders: 255, Global_Markets: 6512, Commodities_FX: 453, Mutual_Funds: 2474};
const SCOPE_PAGES = ['Market_Leaders', 'Mutual_Funds'];
const FULL_PAGES = ['Market_Leaders', 'Global_Markets', 'Commodities_FX', 'Mutual_Funds'];
const SYMBOLS = Object.fromEntries(FULL_PAGES.map(page => [page,
  Array.from({length: FLOORS[page]}, (_, i) => 'SYNTH_' + page.toUpperCase() + '_' + String(i).padStart(4, '0'))]));
const symbolDigest = values => crypto.createHash('sha256')
  .update(values.map(x => x.trim().toUpperCase()).sort().join('\n') + '\n').digest('hex');
const HEADER = ['Page', 'Last Updated', 'Status', 'Message', 'Endpoint', 'HTTP Code', 'Rows', 'Columns', 'Duration ms', 'Warnings'];
const EM = '\u2014';

/* ------------------------------------------------------------ test runner */
let passed = 0, failed = 0, skipped = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n  ')); }
}
function skip(name, why) { skipped++; console.log('SKIP ' + name + ' (' + why + ')'); }

/* ------------------------------------------------------------ GAS stubs */
function goodGrid() {
  return [HEADER, ...Object.entries(FLOORS).map(([page, count]) => [page, STAMP, 'SUCCESS',
    `acquired=${count}/${count} acquisition=COMPLETE fetchfail_effective=enforce | data=COMPLETE run=${COHORT}`,
    'synthetic-source', 200, count, 115, 1, 0])];
}
function load(src, props, options) {
  props = props || {}; options = options || {};
  const propReads = {};
  class FixedDate extends Date {
    constructor(...args) { super(...(args.length ? args : [NOW])); }
    static now() { return NOW; }
  }
  const ctx = {Date: FixedDate, Logger: {log() {}},
    PropertiesService: {getScriptProperties() {
      if (options.propsThrow) throw new Error('synthetic property outage');
      return {getProperty(k) { propReads[k] = (propReads[k] || 0) + 1; return Object.prototype.hasOwnProperty.call(props, k) ? props[k] : null; },
        setProperty(k, v) { props[k] = String(v); }, deleteProperty(k) { delete props[k]; }};
    }},
    Session: {getScriptTimeZone() { return 'Asia/Riyadh'; }},
    Utilities: {DigestAlgorithm: {SHA_256: 'SHA_256'}, Charset: {UTF_8: 'UTF_8'},
      computeDigest(_algorithm, value) { return [...crypto.createHash('sha256').update(value).digest()].map(n => (n > 127 ? n - 256 : n)); },
      formatDate(date, _zone, pattern) {
        const local = new Date(date.getTime() + 3 * 3600000).toISOString();
        return pattern === 'yyyy-MM-dd' ? local.slice(0, 10) : local.replace('T', ' ').slice(0, 19);
      }}};
  vm.createContext(ctx); vm.runInContext(src, ctx, {filename: '16_Decision_Top10.gs'});
  for (const page of SCOPE_PAGES) ctx.DT10_SOURCE_APPROVED_SCOPE.pages[page].symbol_set_sha256 = symbolDigest(SYMBOLS[page]);
  ctx._propReads = propReads;
  return ctx;
}
function statusWorkbook(grid, options) {
  grid = grid || goodGrid(); options = options || {};
  const feed = Object.prototype.hasOwnProperty.call(options, 'verdict') ? options.verdict :
    'EXECUTABLE | run=synthetic | 2026-10-12 12:00:00+03:00 | ML:OK GM:OK CFX:OK MF:OK';
  const feedReads = [];
  const status = {getLastRow() { return grid.length; }, getLastColumn() { return 13; },
    getRange(...args) {
      return {getValues() {
        if (typeof args[0] === 'string') {
          feedReads.push(args[0]);
          if (options.feedFail) throw new Error('synthetic feed read failure');
          return [['TFB Decision Feed', feed]];
        }
        return grid;
      }};
    }};
  const sourceGrids = Object.fromEntries(FULL_PAGES.map(page => [page, [['Name', 'Symbol'], ...SYMBOLS[page].map(s => ['fixture', s])]]));
  return {feedReads, sourceGrids, getSheetByName(name) {
    if (name === '_Status') return status;
    const rows = sourceGrids[name]; if (!rows) return null;
    return {getLastRow() { return rows.length; }, getLastColumn() { return rows[0].length; },
      getDataRange() { return {getValues() { return plain(rows); }}; },
      getRange(r, c, n, m) { return {getValues() { return rows.slice(r - 1, r - 1 + n).map(row => row.slice(c - 1, c - 1 + m)); }}; }};
  }};
}
/* One production verdict for one feed value under one property set. */
function verdict(src, props, feed, opts) {
  opts = opts || {};
  const ctx = load(src, Object.assign({}, props || {}), {propsThrow: opts.propsThrow});
  const ss = statusWorkbook(opts.grid, {verdict: feed, feedFail: opts.feedFail});
  if (opts.patch) opts.patch(ctx);
  return {ctx, ss, v: plain(ctx.dt10BoardVerdict_(ss))};
}

function capture(ctx) {
  const grid = goodGrid(), ss = statusWorkbook(grid), source0 = ctx.dt10SourceReadiness_(ss);
  const before = {captured_at_ms: NOW, initial_ready: source0.ready, generation: plain(source0.generation), cohort: source0.cohort};
  const collected = ctx.dt10CollectPoolRows_(ss, 50000, before);
  Object.assign(before, {pool_counts: plain(collected.available), pool_scope_receipts: plain(collected.approved_scope),
    pool_included_counts: plain(collected.perPage), pool_sent_total: collected.rows.length, pool_reconciliation: plain(collected.pool_reconciliation)});
  return before;
}
function ticket() {
  return {symbol: 'SYNTH.US', rank: 1, price: 100, price_sar: 375, currency: 'USD', suggested_shares: 20, suggested_sar: 7500,
    exp_gain_12m_sar: 1500, entry_zone: '370-380', stop_sar: 350, tp1_sar: 420, tp2_sar: 450,
    advisor_note: 'Buy synthetic 20 shares', detail: {funds_from: 'Cash'}};
}
function payload(ctx) {
  return {version: 'synthetic-builder', status: 'ok', selected: [ticket()],
    candidates_rows: [{symbol: 'SYNTH.US', verdict: 'INVEST', selected: true}],
    kpis: {scanned: 1, passed: 1, deployable_sar: 10000, max_selected: 10}, alerts: [], near_miss: [],
    meta: {board_funding: {contract_version: 1, stage: 'allocate', finalized: true, allocation_version: 'synthetic-builder', eligible_symbols: ['SYNTH.US']}},
    _dt10_source_context: capture(ctx)};
}
function renderer(ctx, ss) {
  const titles = [], tables = [], writes = [];
  const range = new Proxy({}, {get(_obj, method) {
    if (method === 'setValues') return values => { writes.push(plain(values)); return range; };
    return () => range;
  }});
  const sheet = {getMaxRows() { return 100; }, getMaxColumns() { return 60; }, getRange() { return range; }, getParent() { return ss; }};
  ctx.dt10WriteSection_ = (_s, row, title) => { titles.push(title); return row + 1; };
  ctx.dt10WriteTable_ = (_s, row, headers, rows) => { tables.push(plain({headers, rows})); return {firstDataRow: 0, count: 0, next: row + 1}; };
  return {sheet, titles, tables, writes};
}
function render(src, props, feed) {
  const ctx = load(src, Object.assign({}, props || {})), p = payload(ctx);
  const r = renderer(ctx, statusWorkbook(goodGrid(), {verdict: feed}));
  ctx.dt10RenderPayload_(r.sheet, p, {});
  return {ctx, p, titles: r.titles, tables: r.tables, writes: r.writes, output: ctx.dt10OutputStatus_(p), uv: plain(p._dt10_uv)};
}
function refresh(src, props, feed) {
  const ctx = load(src, Object.assign({}, props || {})), grid = goodGrid(), ss = statusWorkbook(grid, {verdict: feed});
  const painting = renderer(ctx, ss), calls = [], pageStatuses = [], selectionLogs = [];
  let stabilityCalls = 0;
  const baseRange = painting.sheet.getRange;
  painting.sheet.getRange = (row, col, ...args) => {
    const range = baseRange(row, col, ...args);
    return new Proxy(range, {get(target, method) {
      if (method === 'getValue') return () => (row === 4 ? 'CONTROL PANEL' : row === 14 ? 'KPIs' : '');
      if (method === 'setValue') return value => { painting.writes.push({row, col, value}); return range; };
      return target[method];
    }});
  };
  const statusGetter = ss.getSheetByName.bind(ss);
  ss.getSheetByName = name => (name === 'Top_10_Investments' ? painting.sheet : statusGetter(name));
  ctx.SpreadsheetApp = {getActiveSpreadsheet() { return ss; }, flush() {}};
  ctx.dt10Tokens_ = () => ({}); ctx.dt10RunMarkerCheck_ = () => {}; ctx.dt10RunMarkerClear_ = () => {};
  ctx.dt10ReadPanel_ = () => ({'Pool Source': 'Sheets', 'Pool Limit': 0, 'T10: Max Selected': 10});
  ctx.dt10SyncInflight_ = () => ({active: false}); ctx.dt10FxRates_ = () => ({USD: 3.75, SAR: 1});
  ctx.dt10PortfolioInputs_ = () => ({cash_available_sar: 10000, holdings: [], holdings_input_incomplete: false});
  ctx.dt10ApplyStability_ = () => { stabilityCalls++; return {note: 'synthetic unchanged stability'}; };
  ctx.dt10WritePageStatus_ = (...args) => pageStatuses.push(plain(args));
  ctx.dt10AppendSelectionLog_ = (...args) => { selectionLogs.push(plain(args.slice(1, 5))); return ''; };
  ctx.dt10AppendExitLog_ = () => ''; ctx.dt10RememberSuccess_ = () => {};
  ctx.dt10Post_ = body => {
    calls.push(plain(body)); const result = payload(ctx); delete result._dt10_source_context;
    if (body.criteria.board_funding_stage === 'research') {
      result.meta.board_funding = {contract_version: 1, stage: 'research', snapshot_available: true, snapshot_id: 'synthetic-snapshot',
        snapshot: {contract_version: 1, builder_version: result.version, snapshot_id: 'synthetic-snapshot', rows: plain(body.rows)}};
      result.selected[0]._board_research = true;
    } else result.meta.board_funding.snapshot_id = 'synthetic-snapshot';
    return {code: 200, json: result};
  };
  ctx.refreshDecisionTop10();
  return {ctx, calls, pageStatuses, selectionLogs, stabilityCalls, titles: painting.titles, tables: painting.tables, writes: painting.writes, feedReads: ss.feedReads.length};
}

const count = (text, needle) => String(text).split(needle).length - 1;
const KEEP = 'DT10_AUDIT_KEEP';
const STAB = 'DT10_STAB_STATE_V1';
const KOFF = [undefined, '', 'off', 'OFF', '   ', 'true', '1', 'yes', 'enforce', 'onn', 'o n', 'on;', 'enabled'];
const KON = ['on', 'ON', 'On', ' on ', '\ton\n'];
function stabState(extra) {
  return JSON.stringify({v: 1, date: '2026-10-12', symbols: Object.assign({
    'MGY.US': {ci: 4, co: 1, member: true, since: '2026-10-10', ls: '', hist: [68.4]},
    'AED.BR': {ci: 2, co: 1, member: true, since: '2026-10-12', ls: '', hist: [81]},
    '0883.HK': {ci: 1, co: 0, member: true, since: '2026-10-13', ls: '', hist: [74.2], ft: true},
    'WATCH.US': {ci: 2, co: 0, member: false, since: '', ls: '', hist: [60]},
  }, extra || {})});
}
const keepOf = body => (body && body.criteria ? body.criteria.audit_keep_symbols : undefined);
const withoutKeep = body => { const b = plain(body); if (b && b.criteria) delete b.criteria.audit_keep_symbols; return b; };

/* ============================================================ K1 */
test('K1 reader: exact on (trimmed, any case) arms; every other word, empty or absent is off', () => {
  for (const value of KON) assert.equal(load(source, {[KEEP]: value}).dt10AuditKeepOn_(), true, JSON.stringify(value));
  for (const value of KOFF) {
    const props = value === undefined ? {} : {[KEEP]: value};
    assert.equal(load(source, props).dt10AuditKeepOn_(), false, JSON.stringify(value));
  }
  assert.equal(load(source, {}).DT10_AUDIT_KEEP_PROP, KEEP);
  assert.equal(load(source, {}).DT10_AUDIT_KEEP_MAX, 60);
});
test('K1 property failure reads as off (fail-open to the base body); one memoized read', () => {
  assert.equal(load(source, {}, {propsThrow: true}).dt10AuditKeepOn_(), false);
  const props = {[KEEP]: 'on'}, ctx = load(source, props);
  for (let i = 0; i < 3; i++) assert.equal(ctx.dt10AuditKeepOn_(), true);
  assert.equal(ctx._propReads[KEEP], 1);
});

/* ============================================================ K2 */
test('K2 pure: members only, sorted, capped at 60; [] on missing or malformed state', () => {
  const ctx = load(source, {});
  assert.deepEqual(plain(ctx.dt10AuditKeepSymbols_(ctx.dt10StabParseState_(stabState()))), ['0883.HK', 'AED.BR', 'MGY.US']);
  for (const bad of [null, undefined, {}, {symbols: null}, {symbols: 'x'}, 'not-state'])
    assert.deepEqual(plain(ctx.dt10AuditKeepSymbols_(bad)), []);
  assert.deepEqual(plain(ctx.dt10AuditKeepSymbols_({symbols: {A: {member: 1}, B: {member: 'true'}, C: null, D: {member: true}}})), ['D']);
  const many = {};
  for (let i = 0; i < 75; i++) many['S' + String(i).padStart(3, '0')] = {member: true};
  const out = plain(ctx.dt10AuditKeepSymbols_({symbols: many}));
  assert.equal(out.length, 60); assert.equal(out[0], 'S000'); assert.equal(out[59], 'S059');
});

/* ============================================================ K3-K5 */
test('K3 REAL refresh, off: no POST body carries audit_keep_symbols (state present)', () => {
  for (const value of KOFF) {
    const props = {[STAB]: stabState()};
    if (value !== undefined) props[KEEP] = value;
    const r = refresh(source, props);
    assert.ok(r.calls.length >= 1, 'refresh posted');
    for (const body of r.calls) assert.equal(keepOf(body), undefined, JSON.stringify(value));
  }
});
test('K4 REAL refresh, on: every POST body carries the sorted members; nothing else changes', () => {
  const off = refresh(source, {[STAB]: stabState()});
  const on = refresh(source, {[STAB]: stabState(), [KEEP]: 'on'});
  assert.equal(on.calls.length, off.calls.length);
  for (const body of on.calls) assert.deepEqual(keepOf(body), ['0883.HK', 'AED.BR', 'MGY.US']);
  assert.deepEqual(on.calls.map(withoutKeep), off.calls.map(withoutKeep));
  assert.deepEqual(on.tables, off.tables);
  assert.deepEqual(on.titles, off.titles);
});
test('K5 REAL refresh, on with no members or an unparsable state: body as off', () => {
  const off = refresh(source, {});
  for (const state of [undefined, '', '{broken', JSON.stringify({symbols: {X: {member: false}}})]) {
    const props = {[KEEP]: 'on'};
    if (state !== undefined) props[STAB] = state;
    const on = refresh(source, props);
    for (const body of on.calls) assert.equal(keepOf(body), undefined, String(state));
    assert.deepEqual(on.calls, off.calls, String(state));
  }
});

/* ============================================================ D1-D2 */
const {HEADER_REGION, FN_REGION, CALL_REGION, stripDesigned} = require('./dt10_audit_keep_regions.js');

if (baseSource) {
  test('D1 off: POST bodies equal the base copy (with and without a stability state)', () => {
    for (const props of [{}, {[STAB]: stabState()}, {[STAB]: stabState(), [KEEP]: 'off'}]) {
      const a = refresh(source, Object.assign({}, props)), b = refresh(baseSource, Object.assign({}, props));
      assert.deepEqual(a.calls, b.calls);
      assert.deepEqual(a.tables, b.tables);
    }
  });
  test('D2 every changed line lies inside a designed v1.17.2 region; regions are exact text', () => {
    assert.equal(stripDesigned(source), baseSource);
    assert.equal((FN_REGION.match(/^function /mg) || []).length, 2, 'two top-level functions');
    assert.ok(/^[\x00-\x7f]*$/.test(HEADER_REGION + FN_REGION + CALL_REGION), 'designed regions are ASCII');
    const mutant = source.replace('out.sort();\n  return out.slice(0, DT10_AUDIT_KEEP_MAX);',
      'out.sort();\n  return out.slice(0, DT10_AUDIT_KEEP_MAX);\n  // stray');
    assert.notEqual(mutant, source);
    let rejected = false;
    try { assert.equal(stripDesigned(mutant), baseSource); } catch (e) { rejected = true; }
    assert.ok(rejected, 'a stray line inside the designed region is still caught by the byte diff');
  });
} else {
  skip('D1 off bodies equal base', 'DT10_BASE unset');
  skip('D2 designed regions only', 'DT10_BASE unset');
}

console.log('PASS ' + passed + '/' + (passed + failed) + (skipped ? '  (skipped ' + skipped + ')' : ''));
process.exitCode = failed ? 1 : 0;
