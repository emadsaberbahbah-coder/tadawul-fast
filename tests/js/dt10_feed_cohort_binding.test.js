/* tests/js/dt10_feed_cohort_binding.test.js -- full-source node vm harness
 * for 16_Decision_Top10.gs v1.13.8 FEED/COHORT BINDING (P0-1 arming
 * prerequisite 2). VERSION 1.0.1 (2026-10-13).
 *
 * v1.0.1 (2026-10-13) WHY: review finding. D4 checked removed lines and the
 * added NAMES only, so an unrelated behavioural line added inside another
 * function (for example dt10SourceCount_) passed 25/25, and G4 of
 * tests/test_release_decision_feed.py relied on exactly this gate. New D5
 * removes the designed v1.13.8 regions from the delivered file (the header
 * WHY block, the guarded block in dt10BoardVerdict_, the doc comments, var
 * and two new functions, the two dt10SelfTest lines), undoes the designed
 * one-line edits (header Version, DT10_VERSION, 'var out', the feed read
 * line) and requires the result to equal the base copy byte for byte. Each
 * region is also checked for shape so code cannot hide in it outside its
 * designed place, and D5 proves it rejects four synthetic mutants. Nothing
 * else changed; C1-C11, D1-D4 and G1 are as in 1.0.0.
 *
 * WHY (2026-10-13): the audited release (scripts/release_decision_feed.py)
 * writes 'EXECUTABLE | run=<SOURCE> | <anchor> | ...' into the _Status
 * 'TFB Decision Feed' cell. Up to v1.13.7 the cockpit honoured an EXECUTABLE
 * feed on state and age only (dt10UvParse_) and never compared the feed's
 * run= with its own source cohort (the shared run= of the four market
 * _Status rows). v1.13.8 adds Script Property DT10_FEED_COHORT_BIND (default
 * absent = off = v1.13.7 behaviour). This harness proves the off identity,
 * the armed binding and its fail-closed faults on the REAL source.
 *
 * Executes the COMPLETE Apps Script source in a vm. Only the Sheets and
 * Properties services, the backend POST and unrelated refresh orchestration
 * are fixtures; dt10BoardVerdict_, dt10UvParse_, source readiness, the
 * renderer and refreshDecisionTop10 are production code.
 *
 *   C1  property reader: exact 'on' (trimmed, any case), anything else off,
 *       service error -> on (fail-closed), memoized, read only for an
 *       EXECUTABLE feed while the upstream verdict is on
 *   C2  OFF corpus: pinned verdicts (state, reason, age, shape) per feed value
 *   C3  ON corpus: same run passes; other run, missing, ambiguous or invalid
 *       run tokens are blocked with their reasons; other verdicts unchanged
 *   C4  a source-blocked verdict keeps its source reason (check not run)
 *   C5  DT10_UPSTREAM_VERDICT=off bypass unchanged (bypasses binding too)
 *   C6  fail-closed faults: property outage, helper throw, missing cohort,
 *       feed read fault
 *   C7  release grammar from scripts/release_decision_feed.py (composed by
 *       python when available, else the pinned fixture): accepted for its own
 *       cohort only; the supersede downgrade stays NOT_ACTIONABLE
 *   C8  run= token rule parity with release_decision_feed._single_run
 *   C9  REAL renderer: armed other-run withholds money, same-run executes
 *   C10 REAL refreshDecisionTop10: B2 keeps 'source readiness VERIFIED (run
 *       N)' and exactly one 'pool_reconciliation_v1=' token in every mode
 *   C11 pure helper edge cases, version lockstep, ES5/ASCII of the new code
 *   D1-D4 (DT10_BASE set): off verdicts, render, refresh and census against
 *       the base copy
 *   D5  (DT10_BASE set, v1.0.1): every changed line lies inside a designed
 *       v1.13.8 region (delivered minus those regions == base, byte for byte)
 *   G1  (DT10_BASE set): GOLDEN NEGATIVE -- every blocking armed case FAILS
 *       on the base copy (and the golden itself fails on a HEAD copy)
 *
 * Usage: node tests/js/dt10_feed_cohort_binding.test.js
 *   DT10_BASE=<copy of the dda1211 16_Decision_Top10.gs> enables D1-D5 and
 *   G1; they print SKIP when it is unset (CI). TFB_PYTHON (default python3)
 *   runs the real release grammar for C7/C8; SKIP only when no interpreter
 *   exists. DT10_DELIVERED=<file> runs every case against another copy
 *   (mutation checks). The last line is 'PASS k/k'; any failure exits 1. */
'use strict';
const assert = require('node:assert/strict');
const childProcess = require('node:child_process');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {feedHistorySource, HISTORY_SHA256} = require('./dt10_cash_utc_history');
// v1.13.10 (held seats kept in the written audit) is stripped byte-exact
// first, so the v1.13.9 history helper sees exactly the v1.13.9 text.
const stripAuditKeep = require('./dt10_audit_keep_regions.js').stripDesigned;

const ROOT = path.join(__dirname, '../..');
const SOURCE_PATH = process.env.DT10_DELIVERED || path.join(ROOT, 'apps_script/16_Decision_Top10.gs');
const source = fs.readFileSync(SOURCE_PATH, 'utf8');
const historySource = feedHistorySource(stripAuditKeep(source));
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
const BIND = 'DT10_FEED_COHORT_BIND';
const ON = {[BIND]: 'on'};

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

/* ------------------------------------------------------------ the corpus */
const F = STAMP;
const RELEASE_FIXTURE = {
  '90001': 'EXECUTABLE | run=90001 | 2026-10-12 11:30:00+03:00 | issued=2026-10-12T12:30:00+03:00 | expires=2026-10-12T19:30:00+03:00 | ttl_min=480 | release=777001 | attempt=1 | receipts=T10:90001,PF:90001 | t10_at=2026-10-12T11:30:00+03:00 | pf_at=2026-10-12T11:30:00+03:00 | mp_at=2026-10-12T11:30:00+03:00 | audits=0,0',
  '90000': 'EXECUTABLE | run=90000 | 2026-10-12 11:30:00+03:00 | issued=2026-10-12T12:30:00+03:00 | expires=2026-10-12T19:30:00+03:00 | ttl_min=480 | release=777001 | attempt=1 | receipts=T10:90000,PF:90000 | t10_at=2026-10-12T11:30:00+03:00 | pf_at=2026-10-12T11:30:00+03:00 | mp_at=2026-10-12T11:30:00+03:00 | audits=0,0'
};
const SUPERSEDE_FIXTURE = 'NOT_ACTIONABLE(final_publication) | run=90001 | 2026-10-12 12:30:00+03:00 | superseded_by_run=90002.1';
const X = age => ({state: 'EXECUTABLE', reason: '', ageMin: age});
const NA = (reason, age) => ({state: 'NOT_ACTIONABLE', reason, ageMin: age});
const MISSING = 'feed run token missing/ambiguous';
// [name, feed, off verdict (v1.13.7 semantics, pinned), on verdict]
const CORPUS = [
  ['EXECUTABLE same run', `EXECUTABLE | run=90001 | ${F} | ML:OK GM:OK CFX:OK MF:OK`, X(30), X(30)],
  ['EXECUTABLE other run', `EXECUTABLE | run=90000 | ${F} | ML:OK GM:OK CFX:OK MF:OK`, X(30), NA('feed run 90000 != cohort 90001', 30)],
  ['EXECUTABLE newer run', `EXECUTABLE | run=90002 | ${F}`, X(30), NA('feed run 90002 != cohort 90001', 30)],
  ['EXECUTABLE no run token', `EXECUTABLE | ${F} | ML:OK GM:OK CFX:OK MF:OK`, X(30), NA(MISSING, 30)],
  ['EXECUTABLE two equal run tokens', `EXECUTABLE | run=90001 | ${F} | run=90001`, X(30), NA(MISSING, 30)],
  ['EXECUTABLE two different run tokens', `EXECUTABLE | run=90001 | ${F} | RUN = 90000`, X(30), NA(MISSING, 30)],
  ['EXECUTABLE non-numeric run', `EXECUTABLE | run=synthetic | ${F} | ML:OK GM:OK CFX:OK MF:OK`, X(30), NA(MISSING, 30)],
  ['EXECUTABLE run=local', `EXECUTABLE | run=local | ${F}`, X(30), NA(MISSING, 30)],
  ['EXECUTABLE leading-zero run', `EXECUTABLE | run=090001 | ${F}`, X(30), NA(MISSING, 30)],
  ['EXECUTABLE empty run', `EXECUTABLE | run= | ${F}`, X(30), NA(MISSING, 30)],
  ['EXECUTABLE superseded_by_run only', `EXECUTABLE | superseded_by_run=90001.2 | ${F}`, X(30), NA(MISSING, 30)],
  ['release grammar, own cohort', RELEASE_FIXTURE['90001'], X(60), X(60)],
  ['release grammar, other cohort', RELEASE_FIXTURE['90000'], X(60), NA('feed run 90000 != cohort 90001', 60)],
  ['NOT_ACTIONABLE final_publication veto', `NOT_ACTIONABLE(final_publication) | run=90001 | ${F}`, NA('final_publication', 30), NA('final_publication', 30)],
  ['supersede downgrade', SUPERSEDE_FIXTURE, NA('final_publication', 0), NA('final_publication', 0)],
  ['stale EXECUTABLE same run', 'EXECUTABLE | run=90001 | 2026-10-12 04:00:00+03:00', NA('verdict stale (510m old)', 510), NA('verdict stale (510m old)', 510)],
  ['future EXECUTABLE same run', 'EXECUTABLE | run=90001 | 2026-10-12 13:00:00+03:00',
    NA('verdict timestamp 30m in the future ' + EM + ' clock mismatch; fail-closed', -30),
    NA('verdict timestamp 30m in the future ' + EM + ' clock mismatch; fail-closed', -30)],
  ['lower-case head', `executable | run=90001 | ${F}`, NA('executable', 30), NA('executable', 30)],
  ['empty feed', '', NA('no verdict published (arm TFB_SYNC_UPSTREAM_VERDICT)', null), NA('no verdict published (arm TFB_SYNC_UPSTREAM_VERDICT)', null)],
  ['no timestamp', 'EXECUTABLE | run=90001', NA('verdict timestamp unreadable', null), NA('verdict timestamp unreadable', null)]
];
const OFF_VALUES = [undefined, '', 'off', 'OFF', '   ', 'true', '1', 'yes', 'enforce', 'observe', 'onn', 'o n', 'on;', 'enabled'];
const ON_VALUES = ['on', 'ON', 'On', ' on ', '\ton\n'];
const pick = v => ({state: v.state, reason: v.reason, ageMin: v.ageMin});
const BASE_KEYS = ['ageMin', 'reason', 'source_readiness', 'state'];

/* ------------------------------------------------------------ python bridge */
const PY_CODE = [
  'import json, sys',
  'sys.path.insert(0, sys.argv[1])',
  'from datetime import datetime, timedelta, timezone',
  'from scripts import release_decision_feed as R',
  'anchor = datetime(2026, 10, 12, 8, 30, tzinfo=timezone.utc)',
  'out = {"release": {}, "single": {}, "supersede": R.supersede_value("90001", datetime(2026, 10, 12, 9, 30, tzinfo=timezone.utc), "90002", "1")}',
  'for run in ("90001", "90000"):',
  '    out["release"][run] = R.compose_release_value(run, anchor=anchor, issued=anchor + timedelta(minutes=60), release_run="777001", run_attempt=1, t10_at=anchor, pf_at=anchor, mp_at=anchor)',
  'for value in json.loads(sys.argv[2]):',
  '    out["single"][value] = R._single_run(value)',
  'print(json.dumps(out))'].join('\n');
const TOKEN_CORPUS = ['run=5', 'RUN=5', 'Run = 5', 'xrun=5', '_run=5', '9run=5', '-run=5', '|run=5|', 'run=5,run=6',
  'run=5;run=5', 'run=', 'run=05', 'superseded_by_run=5', 'myrun=5 run=6', 'run=5 superseded_by_run=6.1',
  'run=5\trun=6', 'release=5', 'run=5x', 'run=\uff15', 'a.run=5', 'run==5', 'prerun=5|run=7', 'RUN =  12 | x',
  // Python's case-insensitive [a-z] also matches these four letters; Python str \s differs from JS \s.
  '\u0130run=5', '\u0131run=5', '\u017frun=5', '\u212arun=5', 'run=5\x1crun=6', 'run=5\x1c', 'run\x1c=5',
  'run=5\x85', 'run\x85=5', 'run=5\ufeff', 'run\ufeff=5', 'run=5\ufeffrun=6', 'run=5\u3000run=6', 'run=5\u00a0'];
const PARITY_VALUES = TOKEN_CORPUS.concat(CORPUS.map(c => c[1]));
function python() {
  const exe = process.env.TFB_PYTHON || 'python3';
  const r = childProcess.spawnSync(exe, ['-I', '-c', PY_CODE, ROOT, JSON.stringify(PARITY_VALUES)],
    {encoding: 'utf8', timeout: 120000, cwd: ROOT});
  if (r.error && r.error.code === 'ENOENT') return {missing: exe};
  if (r.error || r.status !== 0) throw new Error('python release grammar failed: ' + (r.error || r.stderr));
  return JSON.parse(r.stdout);
}
let PY = null, PY_MISSING = '';
try { const got = python(); if (got.missing) PY_MISSING = got.missing; else PY = got; }
catch (e) { PY = {error: String(e)}; }

/* ------------------------------------------------------------ render/refresh */
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

/* ============================================================ C1 */
test('C1 property reader: exact on (trimmed, any case) arms; every other word, empty or absent is off', () => {
  for (const value of ON_VALUES) assert.equal(load(source, {[BIND]: value}).dt10FeedCohortBindOn_(), true, JSON.stringify(value));
  for (const value of OFF_VALUES) {
    const props = value === undefined ? {} : {[BIND]: value};
    assert.equal(load(source, props).dt10FeedCohortBindOn_(), false, JSON.stringify(value));
  }
  assert.equal(load(source, {}).DT10_FEED_COHORT_BIND_PROP, BIND);
});
test('C1 property service failure reads as on (fail-closed); one memoized read per execution; reset re-reads', () => {
  assert.equal(load(source, {}, {propsThrow: true}).dt10FeedCohortBindOn_(), true);
  const props = {[BIND]: 'on'}, ctx = load(source, props), ss = statusWorkbook(goodGrid(), {verdict: CORPUS[1][1]});
  for (let i = 0; i < 3; i++) assert.equal(ctx.dt10BoardVerdict_(ss).state, 'NOT_ACTIONABLE');
  assert.equal(ctx._propReads[BIND], 1, 'one read across three verdicts');
  props[BIND] = 'off';
  assert.equal(ctx.dt10BoardVerdict_(ss).state, 'NOT_ACTIONABLE', 'memo is stable within an execution');
  ctx.dt10PropMemoReset_();
  assert.equal(ctx.dt10BoardVerdict_(ss).state, 'EXECUTABLE');
  assert.equal(ctx._propReads[BIND], 2);
});
test('C1 the property is read only for an EXECUTABLE feed with the upstream verdict on', () => {
  for (const [name, feed, off] of CORPUS) {
    const {ctx} = verdict(source, {}, feed);
    assert.equal(ctx._propReads[BIND] || 0, off.state === 'EXECUTABLE' ? 1 : 0, name);
  }
  const {ctx} = verdict(source, {DT10_UPSTREAM_VERDICT: 'off'}, CORPUS[1][1]);
  assert.equal(ctx._propReads[BIND] || 0, 0, 'upstream verdict off: never read');
});

/* ============================================================ C2 */
test('C2 OFF corpus: every feed value keeps its v1.13.7 verdict (state, reason, age) for every off spelling', () => {
  for (const value of OFF_VALUES) {
    const props = value === undefined ? {} : {[BIND]: value};
    for (const [name, feed, off] of CORPUS) {
      const {v} = verdict(source, props, feed);
      assert.deepEqual(pick(v), off, name + ' / ' + JSON.stringify(value));
      assert.deepEqual(Object.keys(v).sort(), BASE_KEYS, 'verdict shape unchanged when off: ' + name);
      assert.equal(v.source_readiness.ready, true); assert.equal(v.source_readiness.cohort, COHORT);
    }
  }
});

/* ============================================================ C3 */
function onCase(src, name, feed, expected) {
  const {v} = verdict(src, ON, feed);
  assert.deepEqual(pick(v), expected, name);
  if (expected.state === 'NOT_ACTIONABLE' && /^feed run/.test(expected.reason)) {
    assert.equal(v.feed_cohort_blocked, true, name);
    assert.equal(v.feed_cohort.ok, false); assert.equal(v.feed_cohort.cohort, COHORT);
    assert.notEqual(v.source_blocked, true, 'not a source block');
  }
}
const BLOCKING = CORPUS.filter(([, , off, on]) => off.state === 'EXECUTABLE' && on.state === 'NOT_ACTIONABLE');
test('C3 ON corpus: same run passes; other/missing/ambiguous/invalid run tokens blocked with their reasons', () => {
  assert.equal(BLOCKING.length, 11, 'eleven blocking feed values');
  for (const value of ON_VALUES) {
    for (const [name, feed, , on] of CORPUS) {
      const {v} = verdict(source, {[BIND]: value}, feed);
      assert.deepEqual(pick(v), on, name + ' / ' + JSON.stringify(value));
    }
  }
  for (const [name, feed, , on] of CORPUS) onCase(source, name, feed, on);
  const same = verdict(source, ON, CORPUS[0][1]).v;
  assert.deepEqual(same.feed_cohort, {ok: true, reason: '', run: COHORT, cohort: COHORT});
  assert.equal(same.feed_cohort_blocked, undefined);
});
test('C3 non-EXECUTABLE verdicts are byte-identical on and off (the check never upgrades or rewrites them)', () => {
  for (const [name, feed, off] of CORPUS.filter(c => c[2].state !== 'EXECUTABLE')) {
    assert.deepEqual(verdict(source, ON, feed).v, verdict(source, {}, feed).v, name);
    assert.equal(off.state, 'NOT_ACTIONABLE');
  }
});

/* ============================================================ C4 */
test('C4 a source-blocked verdict keeps its source reason; the binding is not consulted', () => {
  const grid = goodGrid(); grid[1][6] = 254;
  for (const props of [{}, ON]) {
    const {ctx, v} = verdict(source, props, CORPUS[1][1], {grid});
    assert.equal(v.state, 'NOT_ACTIONABLE'); assert.equal(v.source_blocked, true);
    assert(v.reason.startsWith('source readiness: '), v.reason);
    assert.equal(v.feed_cohort, undefined); assert.equal(ctx._propReads[BIND] || 0, 0);
  }
  const mixed = goodGrid(); mixed[2][3] = mixed[2][3].replace('run=90001', 'run=90000');
  const {v} = verdict(source, ON, CORPUS[0][1], {grid: mixed});
  assert.equal(v.source_blocked, true); assert(v.reason.includes('differs from cohort'), v.reason);
});

/* ============================================================ C5 */
test('C5 DT10_UPSTREAM_VERDICT=off stays the cockpit bypass and bypasses the binding too', () => {
  for (const [name, feed] of CORPUS) {
    const armed = verdict(source, {DT10_UPSTREAM_VERDICT: 'off', [BIND]: 'on'}, feed);
    const plainOff = verdict(source, {DT10_UPSTREAM_VERDICT: 'off'}, feed);
    assert.deepEqual(armed.v, plainOff.v, name);
    assert.deepEqual(pick(armed.v), {state: 'EXECUTABLE', reason: '', ageMin: null}, name);
    assert.equal(armed.ss.feedReads.length, 0, 'feed not read under the bypass');
  }
});

/* ============================================================ C6 */
test('C6 fail-closed faults: property outage, helper throw, missing cohort, feed read fault', () => {
  const outage = verdict(source, {}, CORPUS[1][1], {propsThrow: true}).v;
  assert.deepEqual(pick(outage), NA('feed run 90000 != cohort 90001', 30), 'outage arms the check');
  assert.equal(pick(verdict(source, {}, CORPUS[0][1], {propsThrow: true}).v).state, 'EXECUTABLE', 'same run still passes');
  const thrown = verdict(source, ON, CORPUS[0][1], {patch: ctx => { ctx.dt10FeedCohortCheck_ = () => { throw new Error('synthetic'); }; }}).v;
  assert.deepEqual(pick(thrown), NA('feed cohort check failed', 30)); assert.equal(thrown.feed_cohort_blocked, true);
  const junk = verdict(source, ON, CORPUS[0][1], {patch: ctx => { ctx.dt10FeedCohortCheck_ = () => ({ok: 'yes'}); }}).v;
  assert.deepEqual(pick(junk), NA('feed cohort check failed', 30), 'a non-true ok withholds');
  const noCohort = verdict(source, ON, CORPUS[0][1], {patch: ctx => {
    const real = ctx.dt10SourceReadiness_;
    ctx.dt10SourceReadiness_ = (...a) => Object.assign(real(...a), {cohort: ''});
  }}).v;
  assert.deepEqual(pick(noCohort), NA('cockpit source cohort unverified', 30));
  for (const props of [{}, ON]) {
    const fault = verdict(source, props, CORPUS[0][1], {feedFail: true}).v;
    assert.deepEqual(pick(fault), NA('no verdict published (arm TFB_SYNC_UPSTREAM_VERDICT)', null));
  }
  const viaThrowingParse = verdict(source, ON, CORPUS[0][1], {patch: ctx => { ctx.dt10UvParse_ = () => { throw new Error('x'); }; }}).v;
  assert.deepEqual(pick(viaThrowingParse), NA('verdict read failed', null));
});

/* ============================================================ C7 */
if (PY_MISSING) skip('C7 release grammar composed by scripts/release_decision_feed.py', 'no python interpreter ' + PY_MISSING);
else {
  test('C7 the real compose_release_value and supersede_value equal the pinned fixtures', () => {
    assert(!PY.error, PY.error);
    assert.equal(PY.release['90001'], RELEASE_FIXTURE['90001']);
    assert.equal(PY.release['90000'], RELEASE_FIXTURE['90000']);
    assert.equal(PY.supersede, SUPERSEDE_FIXTURE);
  });
}
test('C7 a release-grammar value is accepted for its own cohort only; the supersede downgrade never executes', () => {
  const values = PY && PY.release ? PY.release : RELEASE_FIXTURE;
  assert.deepEqual(pick(verdict(source, ON, values['90001']).v), X(60));
  assert.deepEqual(pick(verdict(source, ON, values['90000']).v), NA('feed run 90000 != cohort 90001', 60));
  const other = load(source, ON);
  assert.deepEqual(plain(other.dt10FeedCohortCheck_(values['90000'], '90000')), {ok: true, reason: '', run: '90000', cohort: '90000'},
    'the other release would pass for its own cohort');
  const supersede = PY && PY.supersede ? PY.supersede : SUPERSEDE_FIXTURE;
  for (const props of [{}, ON]) assert.deepEqual(pick(verdict(source, props, supersede).v), NA('final_publication', 0));
});

/* ============================================================ C8 */
if (PY_MISSING) skip('C8 run= token parity with release_decision_feed._single_run', 'no python interpreter ' + PY_MISSING);
else {
  test('C8 the cockpit counts run= tokens exactly like release_decision_feed._single_run', () => {
    assert(!PY.error, PY.error);
    const ctx = load(source, {});
    for (const value of PARITY_VALUES) {
      const r = ctx.dt10FeedCohortCheck_(value, '1');
      const single = r.reason === MISSING ? null : r.run;
      assert.equal(single, PY.single[value], JSON.stringify(value));
    }
  });
}

/* ============================================================ C9 */
function renderOther(src) {
  const r = render(src, ON, CORPUS[1][1]);
  assert.equal(r.output, 'WITHHELD'); assert.equal(r.p.kpis.total_suggested_sar, 0); assert.equal(r.p.kpis.selected_count, 0);
  assert(r.titles.some(t => t.includes('FEED NOT ACTIONABLE ' + EM + ' feed run 90000 != cohort 90001')), r.titles.join(' / '));
  const row = r.tables[0].rows[0];
  for (const idx of r.ctx.DT10_UV_BOARD_WITHHOLD_IDX) assert.equal(row[idx], EM);
  assert(!row[29].includes('20 shares'));
}
test('C9 REAL renderer, armed: other-run feed withholds the order surface and money with the binding reason', () => renderOther(source));
test('C9 REAL renderer, armed: same-run (and release-grammar) feed keeps the funded executable board', () => {
  for (const feed of [CORPUS[0][1], (PY && PY.release ? PY.release : RELEASE_FIXTURE)['90001']]) {
    const r = render(source, ON, feed);
    assert.equal(r.output, 'EXECUTABLE'); assert.equal(r.p.kpis.total_suggested_sar, 7500);
    assert(r.titles.some(t => t.includes('FEED ACTIONABLE ' + EM + ' EXECUTABLE')), r.titles.join(' / '));
  }
});
test('C9 REAL renderer, off: an other-run feed still renders EXECUTABLE (v1.13.7 behaviour, the gap being closed)', () => {
  const r = render(source, {}, CORPUS[1][1]);
  assert.equal(r.output, 'EXECUTABLE'); assert.equal(r.p.kpis.total_suggested_sar, 7500);
  assert.equal(r.uv.feed_cohort, undefined);
});

/* ============================================================ C10 */
function refreshOther(src) {
  const r = refresh(src, ON, CORPUS[1][1]);
  assert.equal(r.calls.length, 1, 'no allocation replay under a withheld feed');
  const line = r.pageStatuses[0][1];
  assert(line.includes('output: WITHHELD'), line);
  assert(r.titles.some(t => t.includes('feed run 90000 != cohort 90001')), r.titles.join(' / '));
  for (const idx of r.ctx.DT10_UV_BOARD_WITHHOLD_IDX) assert.equal(r.tables[0].rows[0][idx], EM);
  return r;
}
test('C10 REAL refresh, armed: other-run feed is withheld while B2 keeps the receipt and one pool token', () => {
  const r = refreshOther(source), line = r.pageStatuses[0][1];
  assert(line.includes('source readiness VERIFIED (run 90001)'), 'receipt kept so a release can still bootstrap');
  assert.equal(count(line, 'pool_reconciliation_v1='), 1);
  assert.equal(r.pageStatuses[0][0], 'OK', 'a feed veto is not a source block (same as final_publication)');
});
test('C10 REAL refresh, armed: same-run release value renders EXECUTABLE with one pool token', () => {
  const r = refresh(source, ON, (PY && PY.release ? PY.release : RELEASE_FIXTURE)['90001']), line = r.pageStatuses[0][1];
  assert.equal(r.calls.length, 2); assert.equal(r.pageStatuses[0][0], 'OK');
  assert(line.includes('output: EXECUTABLE'), line); assert(line.includes('source readiness VERIFIED (run 90001)'));
  assert.equal(count(line, 'pool_reconciliation_v1='), 1);
  assert(r.titles.some(t => t.includes('FEED ACTIONABLE ' + EM + ' EXECUTABLE')));
});
test('C10 REAL refresh, off: status line and writes identical to the armed same-run refresh; other-run stays EXECUTABLE', () => {
  const off = refresh(source, {}, CORPUS[0][1]), armed = refresh(source, ON, CORPUS[0][1]);
  assert.deepEqual(off.pageStatuses, armed.pageStatuses); assert.deepEqual(off.tables, armed.tables);
  assert.deepEqual(off.writes, armed.writes); assert.deepEqual(off.calls, armed.calls);
  const other = refresh(source, {}, CORPUS[1][1]);
  assert(other.pageStatuses[0][1].includes('output: EXECUTABLE'));
  assert.equal(count(other.pageStatuses[0][1], 'pool_reconciliation_v1='), 1);
});

/* ============================================================ C11 */
test('C11 pure helper edge cases', () => {
  const ctx = load(source, {}), chk = (raw, cohort) => plain(ctx.dt10FeedCohortCheck_(raw, cohort));
  assert.deepEqual(chk('EXECUTABLE | run=90001 | x', 90001), {ok: true, reason: '', run: '90001', cohort: '90001'});
  for (const cohort of ['', null, undefined, '0', '090001', 'local', '-1', '1.5'])
    assert.equal(chk('EXECUTABLE | run=90001', cohort).reason, 'cockpit source cohort unverified', String(cohort));
  assert.equal(chk(null, '1').reason, MISSING); assert.equal(chk(undefined, '1').reason, MISSING);
  assert.equal(chk({toString() { throw new Error('boom'); }}, '1').reason, 'feed cohort check failed');
  assert.equal(chk('run=1\nrun=1', '1').reason, MISSING, 'exactly one token, even when equal');
});
test('C11 version lockstep, WHY block, census additions and ES5/ASCII of the new functions', () => {
  const ctx = load(source, {});
  assert.equal(ctx.DT10_VERSION, '1.13.10');
  assert.match(source.split('\n')[3], /^ \* Version: 1\.13\.10 /);
  assert(source.includes(' * v1.13.8 (2026-10-13) -- FEED RUN BOUND TO THE SOURCE COHORT (P0-1 PREREQ 2)'));
  assert(source.includes(' * v1.13.7 (2026-10-12) -- PROVED CROSS-PAGE POOL EXCLUSIONS'), 'prior WHY kept');
  for (const name of ['dt10FeedCohortBindOn_', 'dt10FeedCohortCheck_', 'dt10BoardVerdict_']) {
    const start = source.indexOf('function ' + name + '(');
    assert(start > 0, name);
    const body = source.slice(start, source.indexOf('\n}\n', start) + 2);
    const code = body.split('\n').filter(l => !/^\s*(\*|\/\*|\/\/)/.test(l)).map(l => l.replace(/\/\/.*$/, ''));
    for (const l of code) assert(!/\b(let|const)\b|=>|`|\bclass\b|\.\.\.|\.includes\(/.test(l), 'ES5 guard: ' + l);
    assert(/^[\x00-\x7f]*$/.test(body), 'ASCII: ' + name);
  }
  // The new block from the v1.13.8 comment in dt10BoardVerdict_ to the end of dt10FeedCohortCheck_
  // (both doc comments, the var and the two functions) is ASCII.
  const blockStart = source.indexOf('  // v1.13.8: DT10_FEED_COHORT_BIND=on binds');
  const checkStart = source.indexOf('function dt10FeedCohortCheck_(');
  assert(blockStart > 0 && checkStart > blockStart);
  assert(/^[\x00-\x7f]*$/.test(source.slice(blockStart, source.indexOf('\n}\n', checkStart))), 'new block is ASCII');
  const why = source.slice(source.indexOf(' * v1.13.8 (2026-10-13)'), source.indexOf(' * v1.13.7 (2026-10-12)'));
  assert(/^[\x00-\x7f]*$/.test(why), 'WHY block is ASCII');
});

/* ============================================================ D5 helpers (v1.0.1) */
/* revertDesigned(src) cuts the designed v1.13.8 regions out of a delivered
 * source and undoes its designed one-line edits; it throws an AssertionError
 * when a region is missing, duplicated or shaped wrongly. D5 then requires
 * the result to equal the base copy byte for byte, so any other change (an
 * added, removed or edited line anywhere else) fails. The shape checks are
 * line and token structural, not a JavaScript parser. */
function onlyIndex(lines, line, label) {
  const hits = [];
  lines.forEach((l, i) => { if (l === line) hits.push(i); });
  assert.equal(hits.length, 1, label + ': expected exactly one line ' + JSON.stringify(line));
  return hits[0];
}
const stripStrings = l => l.replace(/'(?:[^'\\]|\\.)*'/g, "''").replace(/"(?:[^"\\]|\\.)*"/g, '""');
/* Brace depth over the code lines (strings removed): it must stay open
 * (>= 1) from the end of the first line to the end of the line before the
 * last, and close to 0 on the last line, so no line closes the scope early. */
function staysInside(code, label) {
  let depth = 0;
  code.forEach((l, i) => {
    for (const ch of stripStrings(l)) {
      if (ch === '{') depth++;
      else if (ch === '}') depth--;
      assert(depth >= 0, label + ': brace closes below zero at ' + JSON.stringify(l));
    }
    if (i < code.length - 1) assert(depth >= 1, label + ': scope closed early at ' + JSON.stringify(l));
  });
  assert.equal(depth, 0, label + ': braces balance');
}
function revertDesigned(src) {
  let lines = src.split('\n');
  const cut = (from, to) => { const region = lines.slice(from, to); lines = lines.slice(0, from).concat(lines.slice(to)); return region; };
  // 1. The header WHY block: comment lines only, above the v1.13.7 entry.
  const why = onlyIndex(lines, ' * v1.13.8 (2026-10-13) -- FEED RUN BOUND TO THE SOURCE COHORT (P0-1 PREREQ 2)', 'WHY block');
  const prior = onlyIndex(lines, ' * v1.13.7 (2026-10-12) -- PROVED CROSS-PAGE POOL EXCLUSIONS', 'v1.13.7 WHY');
  assert(prior > why, 'WHY block sits above the v1.13.7 entry');
  for (const l of cut(why, prior)) assert(/^ \*( |$)/.test(l) && !l.includes('*/'), 'WHY region is comment only: ' + l);
  // 2. The header Version line and the version constant.
  assert(lines[3].startsWith(' * Version: 1.13.8 ') && count(lines[3], '1.13.8') === 1, 'header Version line: ' + lines[3]);
  lines[3] = lines[3].replace('1.13.8', '1.13.7');
  lines[onlyIndex(lines, "var DT10_VERSION = '1.13.8';", 'DT10_VERSION')] = "var DT10_VERSION = '1.13.7';";
  // 3. dt10BoardVerdict_: the two edited lines and the one guarded block.
  const fn = onlyIndex(lines, 'function dt10BoardVerdict_(ss, sourceContext) {', 'dt10BoardVerdict_');
  assert.equal(lines[fn + 1], '  var out, feedRaw = null;'); lines[fn + 1] = '  var out;';
  assert.equal(lines[fn + 4], '    try { feedRaw = dt10UvRead_(ss); out = dt10UvParse_(feedRaw, Date.now()); }');
  lines[fn + 4] = '    try { out = dt10UvParse_(dt10UvRead_(ss), Date.now()); }';
  const blockStart = onlyIndex(lines, '  // v1.13.8: DT10_FEED_COHORT_BIND=on binds an EXECUTABLE feed to this', 'binding block');
  assert(blockStart > fn, 'binding block inside dt10BoardVerdict_');
  const blockEnd = lines.indexOf('  return out;', blockStart);
  assert(blockEnd > blockStart, 'binding block ends before return out');
  const block = cut(blockStart, blockEnd);
  let k = 0;
  while (k < block.length && /^  \/\/ /.test(block[k])) k++;
  assert.equal(block[k], "  if (out.state === 'EXECUTABLE' && feedRaw !== null) {", 'binding block guard');
  assert.equal(block[block.length - 1], '  }', 'binding block guard close');
  for (const l of block.slice(k + 1, -1)) assert(/^ {4,}\S/.test(l), 'binding block body indented inside the guard: ' + JSON.stringify(l));
  staysInside(block.slice(k), 'binding block');
  // 4. Doc comments, the var and the two new top-level functions.
  const r2Start = onlyIndex(lines, "/** v1.13.8: Script Property DT10_FEED_COHORT_BIND. Exactly 'on' (trimmed,", 'new functions');
  assert.equal(lines[r2Start - 1], '}', 'new functions follow dt10BoardVerdict_');
  const checkFn = onlyIndex(lines, 'function dt10FeedCohortCheck_(raw, cohort) {', 'dt10FeedCohortCheck_');
  const r2End = lines.indexOf('}', checkFn) + 1;
  assert(r2End > checkFn && checkFn > r2Start, 'new functions region');
  const r2 = cut(r2Start, r2End), top = [];
  let inDoc = false, body = [];
  for (const l of r2) {
    if (inDoc) {
      assert(/^ \*/.test(l), 'doc comment line: ' + JSON.stringify(l));
      if (l.includes('*/')) { assert(/\*\/$/.test(l), 'nothing after a doc comment close: ' + l); inDoc = false; }
      continue;
    }
    if (/^\/\*\*/.test(l)) { assert(!body.length, 'no comment inside a new function'); inDoc = !/\*\/$/.test(l); continue; }
    if (/^\S/.test(l)) {
      top.push(l);
      if (/^function /.test(l)) body = [l];
      else if (l === '}') { body.push(l); staysInside(body, top[top.length - 2]); body = []; }
      continue;
    }
    assert(/^ +\S/.test(l) && body.length, 'indented code only inside a new function: ' + JSON.stringify(l));
    body.push(l);
  }
  assert.deepEqual(top, ["var DT10_FEED_COHORT_BIND_PROP = 'DT10_FEED_COHORT_BIND';", 'function dt10FeedCohortBindOn_() {', '}',
    'function dt10FeedCohortCheck_(raw, cohort) {', '}'], 'top-level code of the new region');
  const sandbox = {};
  vm.createContext(sandbox); vm.runInContext(r2.join('\n'), sandbox);
  assert.deepEqual(Object.keys(sandbox).sort(), ['DT10_FEED_COHORT_BIND_PROP', 'dt10FeedCohortBindOn_', 'dt10FeedCohortCheck_'],
    'the new region defines exactly the designed globals and nothing else');
  // 5. dt10SelfTest: one comment and exactly two report.push statements.
  const st = onlyIndex(lines, '  // v1.13.8: feed/cohort binding -- mode line + pure core (no feed read).', 'self-test lines');
  const msg = lines.indexOf("  var msg = report.join('\\n');", st);
  assert(msg > st, 'self-test lines end before report.join');
  const selfTest = cut(st, msg).slice(1);
  for (const l of selfTest) assert(/^  report\.push\(/.test(l) || /^ {6,}\S/.test(l), 'self-test region: ' + JSON.stringify(l));
  assert.equal(selfTest.filter(l => /^  \S/.test(l)).length, 2, 'two report.push statements');
  const pushes = stripStrings(selfTest.join('\n'));
  assert(/^(\s*report\.push\([^;{}]*\);)+$/.test(pushes), 'self-test region holds only report.push(...) statements');
  assert(!/[^=!<>]=[^=]|\bfunction\b|\+\+|--/.test(pushes.replace(/report\.push\(/g, '')), 'no assignment or function in the self-test lines');
  return lines.join('\n');
}
function firstDifference(a, b) {
  const x = a.split('\n'), y = b.split('\n');
  for (let i = 0; i < Math.max(x.length, y.length); i++) {
    if (x[i] !== y[i]) return 'line ' + (i + 1) + ': delivered-minus-regions ' + JSON.stringify(x[i]) + ' vs base ' + JSON.stringify(y[i]);
  }
  return '';
}

/* v1.13.9's approved extension is separate from the historical default-off
 * gate. A full-source digest prevents an unrelated change being hidden by
 * reversing this release's cosmetic/input-adapter edits. */
test('H1 immutable v1.13.8 fixture digest and current edit mutation guard', () => {
  assert.equal(crypto.createHash('sha256').update(historySource).digest('hex'), HISTORY_SHA256);
  assert.equal(count(source, 'function dt10SourceCount_(value) {\n'), 1);
  const unrelated = source.replace('function dt10SourceCount_(value) {\n',
    'function dt10SourceCount_(value) {\n  if (value === 424242) return 1;\n');
  assert.throws(() => feedHistorySource(stripAuditKeep(unrelated)), assert.AssertionError);
  const timestampMutation = source.replace("extra.updatedKey = t === 'lastupdatedutc' ? 'last_updated_utc' : 'last_updated';",
    "extra.updatedKey = 'last_updated';");
  assert.throws(() => feedHistorySource(stripAuditKeep(timestampMutation)), assert.AssertionError);
});

/* ============================================================ D1-D5, G1: immutable v1.13.8 */
if (baseSource) {
  test('D1 OFF identity vs base: full verdict objects deep-equal for every corpus value and off spelling', () => {
    for (const value of OFF_VALUES) {
      const props = value === undefined ? {} : {[BIND]: value};
      for (const [name, feed] of CORPUS) assert.deepEqual(verdict(historySource, props, feed).v, verdict(baseSource, props, feed).v, name + ' / ' + JSON.stringify(value));
    }
    for (const [name, feed] of CORPUS) {
      assert.deepEqual(verdict(historySource, {DT10_UPSTREAM_VERDICT: 'off', [BIND]: 'on'}, feed).v,
        verdict(baseSource, {DT10_UPSTREAM_VERDICT: 'off', [BIND]: 'on'}, feed).v, 'bypass ' + name);
    }
    const grid = goodGrid(); grid[1][6] = 254;
    assert.deepEqual(verdict(historySource, ON, CORPUS[1][1], {grid}).v, verdict(baseSource, ON, CORPUS[1][1], {grid}).v, 'source block');
  });
  test('D2 OFF identity vs base: REAL renderer titles, tables, writes and payload verdict', () => {
    for (const [name, feed] of CORPUS) {
      const a = render(historySource, {}, feed), b = render(baseSource, {}, feed);
      assert.deepEqual([a.titles, a.tables, a.writes, a.output, a.uv, plain(a.p.kpis)], [b.titles, b.tables, b.writes, b.output, b.uv, plain(b.p.kpis)], name);
    }
  });
  test('D3 OFF identity vs base: REAL refreshDecisionTop10 (B2 status, POSTs, writes, selection log args)', () => {
    for (const feed of [CORPUS[0][1], CORPUS[1][1], CORPUS[13][1], RELEASE_FIXTURE['90000']]) {
      const a = refresh(historySource, {}, feed), b = refresh(baseSource, {}, feed);
      for (const key of ['calls', 'pageStatuses', 'selectionLogs', 'stabilityCalls', 'titles', 'tables', 'writes', 'feedReads']) {
        assert.deepEqual(a[key], b[key], key + ' / ' + feed.slice(0, 40));
      }
    }
  });
  test('D4 census vs base: zero functions or vars removed; added exactly the designed names; added code ES5 and ASCII', () => {
    const census = (src, re) => { const out = new Set(); let m; while ((m = re.exec(src))) out.add(m[1]); return out; };
    const fnRe = () => /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg, varRe = () => /^var\s+([A-Za-z_$][\w$]*)/mg;
    const bf = census(baseSource, fnRe()), df = census(historySource, fnRe()), bv = census(baseSource, varRe()), dv = census(historySource, varRe());
    assert.deepEqual([...bf].filter(f => !df.has(f)), []); assert.deepEqual([...bv].filter(v => !dv.has(v)), []);
    assert.deepEqual([...df].filter(f => !bf.has(f)).sort(), ['dt10FeedCohortBindOn_', 'dt10FeedCohortCheck_']);
    assert.deepEqual([...dv].filter(v => !bv.has(v)), ['DT10_FEED_COHORT_BIND_PROP']);
    const baseLines = new Set(baseSource.split('\n'));
    const added = historySource.split('\n').filter(l => !baseLines.has(l));
    // The header Version line keeps its pre-existing em dash; only 1.13.7 -> 1.13.8 changed there.
    for (const l of added) assert(/^[\x00-\x7f]*$/.test(l) || baseLines.has(l.split('1.13.8').join('1.13.7')), 'ASCII: ' + l);
    for (const l of added.filter(x => !/^\s*(\*|\/\*|\/\/)/.test(x)).map(x => x.replace(/\/\/.*$/, ''))) {
      assert(!/\b(let|const)\b|=>|`|\bclass\b|\.\.\.|\.includes\(/.test(l), 'ES5 guard: ' + l);
    }
    const newLines = new Set(historySource.split('\n'));
    const removed = baseSource.split('\n').filter(l => !newLines.has(l)).map(l => l.trim()).filter(l => l && !/^\*/.test(l));
    assert.deepEqual(removed.sort(), ['try { out = dt10UvParse_(dt10UvRead_(ss), Date.now()); }',
      'var DT10_VERSION = \'1.13.7\';', 'var out;'].sort(), 'only the feed read line, its var line and the version line changed');
  });
  test('D5 designed regions only: delivered minus the v1.13.8 regions equals the base copy byte for byte', () => {
    const reverted = revertDesigned(historySource);
    assert(reverted === baseSource, 'a change outside the designed v1.13.8 regions: ' + firstDifference(reverted, baseSource));
    // Negative controls: D5 must reject a line added anywhere else, code hidden
    // after the guard, a top-level statement among the new functions and an
    // assignment in the self-test lines.
    const inject = (after, line) => {
      assert.equal(count(historySource, after), 1, 'mutation anchor ' + after);
      return historySource.replace(after, after + line);
    };
    const mutants = [
      ['unrelated line in dt10SourceCount_', inject('function dt10SourceCount_(value) {\n', "  if (String(value) === '424242') return 9007199254740991;\n")],
      ['statement after the guard', inject("      out.reason = 'feed cohort check failed';\n    }\n  }\n", '  out.ageMin = 0;\n')],
      ['top-level statement among the new functions', inject("var DT10_FEED_COHORT_BIND_PROP = 'DT10_FEED_COHORT_BIND';\n", 'dt10SourceCount_ = function (v) { return 1; };\n')],
      ['assignment in the self-test lines', inject("  report.push('feed cohort bind (DT10_FEED_COHORT_BIND): ' +\n", '      (DT10_VERSION = \'x\') +\n')]
    ];
    for (const [name, mutant] of mutants) {
      let accepted = false;
      try { accepted = revertDesigned(mutant) === baseSource; }
      catch (e) { if (!(e instanceof assert.AssertionError)) throw e; }
      assert.equal(accepted, false, 'D5 must reject the mutant: ' + name);
    }
  });
  test('G1 GOLDEN NEGATIVE: every blocking armed case FAILS on the base copy (DT10_BASE)', () => {
    const cases = BLOCKING.map(([name, feed, , on]) => ['verdict ' + name, () => onCase(baseSource, name, feed, on)])
      .concat([['renderer other run', () => renderOther(baseSource)], ['refresh other run', () => refreshOther(baseSource)]]);
    const survived = [];
    for (const [name, fn] of cases) {
      try { fn(); survived.push(name); }
      catch (e) { if (!(e instanceof assert.AssertionError)) throw new Error(name + ': expected an assertion failure, got ' + e); }
    }
    assert.deepEqual(survived, [], 'armed cases that unexpectedly pass on base (is DT10_BASE a HEAD copy?)');
    assert.equal(cases.length, 13);
  });
} else {
  for (const name of ['D1 OFF identity verdicts', 'D2 OFF identity renderer', 'D3 OFF identity refresh', 'D4 census vs base', 'D5 designed regions only', 'G1 GOLDEN NEGATIVE'])
    skip(name, 'DT10_BASE unset');
}

const total = passed + failed;
console.log('dt10 feed cohort binding: ' + passed + ' passed, ' + failed + ' failed, ' + skipped + ' skipped');
console.log('SOURCE_SHA256 ' + crypto.createHash('sha256').update(source).digest('hex'));
console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + total);
if (failed) process.exitCode = 1;
