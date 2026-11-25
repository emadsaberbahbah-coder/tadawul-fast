#!/usr/bin/env node
/* tests/test_refresh_v1200_failed_ledger.js
 * Harness for apps_script/05_Refresh.gs v1.20.0 [FAILED-LEDGER] (week plan, package 3): the symbols of a failed batch
 * are kept on a ledger saved with the checkpoint; with Script Property REFRESH_FAILED_RETRY=on each is retried once at
 * the tail of the run (at most REFRESH_FAILED_RETRY_MAX_ a run) and a run that ends with symbols still unwritten closes
 * PARTIAL, their previous rows kept; otherwise (observe) the run is the v1.19.0 one plus one warning line.
 *
 * Two trees over the REAL file, each in its own vm context: the delivered source (NEW) and the v1.19.0 file (OLD,
 * rebuilt from NEW by putting back the hunks of tests/js/refresh_edits_v1200.json, SHA-256 pinned). Every
 * dependency of refreshPageInBatchesCore_ is a stub that records what the run did (fetches, writes, statuses, logs).
 *   R1 version lockstep and inventory: +6 functions, 0 removed
 *   R2 GOLDEN NEGATIVE: observe, no failure = the v1.19.0 run (statuses, panel, writes, fetches, log, return)
 *   R3 observe with a failed batch = the v1.19.0 run plus one '[v1.20.0 FAILED-LEDGER observe]' line; no retry
 *   R4 on: a batch that fails once is retried and recovered; the run closes SUCCESS
 *   R5 on: a batch that always fails is retried once only and the run closes PARTIAL, naming the symbols; never written
 *   R6 on: a batch whose write throws is treated the same way
 *   R7 on: the retry is bounded (REFRESH_FAILED_RETRY_MAX_)
 *   R8 a pause keeps the ledger and the retry tail in the checkpoint; the next run resumes, not restarts
 *   R9 isCheckpointCompatible_: a checkpoint without baseCount is judged as in v1.19.0
 *   R10 pure helpers
 *   R11 a symbol comes off the ledger only when its row is written (Codex P2 on #1078)
 * The last line is 'PASS k/k' (exit 1 on any failure).
 */
'use strict';
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const ROOT = path.join(__dirname, '..');
const FILE = path.join(ROOT, 'apps_script', '05_Refresh.gs');
const NEW = fs.readFileSync(FILE, 'utf8');

const crypto = require('crypto');
const EDITS = JSON.parse(fs.readFileSync(path.join(__dirname, 'js', 'refresh_edits_v1200.json'), 'utf8'));
// The v1.19.0 file (origin/main cf79d93), rebuilt from the delivered one by putting each v1.20.0 hunk back, bottom-up,
// and pinned by SHA-256; CI checks out one commit, so git history is not read.
function oldSource() {
  let s = NEW;
  for (const e of EDITS.edits) {
    if (s.split(e.new).length !== 2) return '';
  }
  for (let i = EDITS.edits.length - 1; i >= 0; i--) s = s.replace(EDITS.edits[i].new, () => EDITS.edits[i].old);
  return crypto.createHash('sha256').update(s).digest('hex') === EDITS.base_sha256 ? s : '';
}
const OLD = oldSource();

let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); } catch (e) { failed++; console.log('FAIL ' + name + ': ' + (e && e.stack || e)); }
}
function eq(a, b, msg) {
  const x = JSON.stringify(a), y = JSON.stringify(b);
  if (x !== y) throw new Error((msg || 'not equal') + '\n  got:  ' + x + '\n  want: ' + y);
}
function ok(c, msg) { if (!c) throw new Error(msg || 'assertion failed'); }
const plain = (v) => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));

const HEADERS = ['Symbol', 'Name', 'Price'];
function load(src) {
  const ctx = vm.createContext({console: {log() {}, warn() {}, error() {}}, JSON, Math, Date, Object, Array, String, Number});
  vm.runInContext(src, ctx, {filename: '05_Refresh.gs'});
  return ctx;
}

/* One run of refreshPageInBatchesCore_ over stubs.
 *   plan(chunk, callNo) -> 'ok' | 'fail' | 'throw'   (throw = the write throws after a good fetch)
 *   pauseAfter: pause (runtime budget) at the loop top after this many fetches */
function run(src, o) {
  const ctx = load(src);
  const props = Object.assign({}, o.props || {});
  const rec = {fetches: [], writes: [], panel: [], status: [], logs: [], deferred: {}};
  let calls = 0;
  ctx.PropertiesService = {getScriptProperties: () => ({
    getProperty: (k) => (Object.prototype.hasOwnProperty.call(props, k) ? props[k] : null),
    setProperty: (k, v) => { props[k] = String(v); },
    deleteProperty: (k) => { delete props[k]; }
  })};
  const sheet = {getLastRow: () => 1 + rec.writes.length, getName: () => 'Market_Leaders'};
  const stub = {
    SHEET_NAMES: {DATA_DICTIONARY: 'Data_Dictionary', INSIGHTS_ANALYSIS: 'Insights_Analysis', TOP_10_INVESTMENTS: 'Top_10_Investments'},
    uniqueStrings_: (arr) => { const out = [], seen = {}; (arr || []).forEach((v) => { const x = String(v == null ? '' : v).trim(); if (!x || seen[x]) return; seen[x] = 1; out.push(x); }); return out; },
    arraysEqual_: (a, b) => Array.isArray(a) && Array.isArray(b) && a.length === b.length && a.every((v, i) => String(v) === String(b[i])),
    getPageConfig: (n) => ({pageName: n, preserveManualColumns: []}),
    _refreshManualPriorityProbe_: () => ({yield: false}),
    ensureSheetExists_: () => sheet,
    readCurrentSheetHeaders_: () => HEADERS.slice(),
    applyHeadersFromSchema_: () => {},
    _detectMissingCanonicalHeaders_: () => [],
    _logCanonicalHeaderDrift_: () => {},
    _clearStatusPanelZone_: () => {},
    getRequestedSymbolsFromOptions_: () => o.symbols.slice(),
    _refreshBatchSizeDefault_: () => o.batchSize,
    getEndpointCandidates: () => ['/v1/analysis/sheet-rows'],
    getPreserveOnEmptyColumns: () => [],
    _refreshCanUseHeaderMap_: () => true,
    _logBatchPathChoice_: () => {},
    findColumnIndex_: () => 1,
    _refreshTripwireEnabled_: () => false,
    _refreshKeepLastGoodEnabled_: () => false,
    _refreshBackendHoldSnapshot_: () => null,
    _refreshBackendHoldPreWriteCheck_: () => ({yield: false}),
    fetchPageData_: (page, opt) => {
      const chunk = opt.payloadOverrides.symbols.slice();
      calls++;
      rec.fetches.push(chunk);
      if (o.pauseAfter && calls >= o.pauseAfter) ctx.REFRESH_MAX_RUNTIME_MS_ = -1;
      const what = o.plan ? o.plan(chunk, calls) : 'ok';
      if (what === 'fail') return {ok: false};
      const served = what === 'empty' ? [] : (what === 'first' ? chunk.slice(0, 1) : chunk);
      return {ok: true, endpointUsed: '/v1/analysis/sheet-rows', warnings: [],
        normalized: {headers: HEADERS, keys: [], rows2d: [], rowObjects: served.map((s) => ({symbol: s, price: 1, _throw: what === 'throw'}))}};
    },
    dedupeRows2dBySymbol_: (r) => r,
    _injectProvenanceFields_: () => ({filledUtc: 0}),
    upsertRowsBySymbol_: (sh, h, r2, pc, pe, opt) => {
      if (opt.rowObjects.some((r) => r._throw)) throw new Error('write failed');
      const defer = (o.defer || []).filter((s) => opt.rowObjects.some((r) => r.symbol === s) && !rec.deferred[s]);
      defer.forEach((s) => { rec.deferred[s] = 1; });
      opt.rowObjects.forEach((r) => { if (defer.indexOf(r.symbol) < 0) rec.writes.push(r.symbol); });
      return {updated: opt.rowObjects.length - defer.length, appended: 0, skipped: 0, deferredSymbols: defer};
    },
    _verifyCriticalFieldsAfterWrite_: () => ({ok: true}),
    getRefreshErrorMessage_: (e) => 'Error: ' + e.message,
    _refreshDedupeInstrumentSheetBySymbol_: () => {},
    _findExpectedColumnCountForPage_: () => 0,
    applySheetFormatting_: () => {},
    sortAndRankPageCore_: () => {},
    validatePostRefreshIntegrity_: () => ({ok: true, warnings: []}),
    validateExpectedRefreshWidth_: () => ({warnings: []}),
    writePageStatusPanel_: (sh, obj) => rec.panel.push(plain(obj)),
    writePageStatus_: (n, obj) => { const c = plain(obj); delete c.durationMs; rec.status.push(c); },
    logRun_: (sev, fn, page, msg, det) => { const d = plain(det || {}); delete d.durationMs; rec.logs.push([sev, fn, page, msg, d]); }
  };
  Object.keys(stub).forEach((k) => { ctx[k] = stub[k]; });
  if (o.checkpoint) props['TFB_BATCH_CHECKPOINT::Market_Leaders'] = JSON.stringify(o.checkpoint);
  const out = plain(ctx.refreshPageInBatchesCore_('Market_Leaders', {applyFormat: false, applySort: false}));
  const saved = props['TFB_BATCH_CHECKPOINT::Market_Leaders'];
  return {out, rec, checkpoint: saved ? JSON.parse(saved) : null, ctx};
}
const SYMS = (n, p) => Array.from({length: n}, (_, i) => (p || 'S') + String(i + 1).padStart(3, '0'));
const NEW_KEYS_LOG = ['failedLedger', 'failedRetried', 'failedRecovered', 'failedRetryMode'];
const NEW_KEYS_OUT = ['partial', 'unwrittenSymbols', 'failedRetried', 'failedRecovered', 'failedRetryMode'];
const strip = (obj, keys) => { const c = plain(obj); keys.forEach((k) => { delete c[k]; }); return c; };
const lastLog = (r) => r.rec.logs[r.rec.logs.length - 1];

test('R1 version lockstep and inventory (+6 functions, 0 removed)', () => {
  ok(NEW.indexOf(' * Version: 1.20.0') >= 0 && NEW.indexOf("var REFRESH_VERSION = '1.20.0';") >= 0, 'version');
  ok(OLD, 'the v1.19.0 file could not be rebuilt (a hunk moved or the pin differs)');
  const fns = (s) => new Set((s.match(/^function ([A-Za-z0-9_$]+)\s*\(/gm) || []).map((x) => x.replace(/^function\s+|\s*\($/g, '')));
  const a = fns(OLD), b = fns(NEW);
  const added = [...b].filter((x) => !a.has(x)).sort(), removed = [...a].filter((x) => !b.has(x));
  eq(added, ['_refreshClearFailedChunk_', '_refreshFailedRetryMode_', '_refreshFailedSymbols_', '_refreshLedgerKey_',
    '_refreshNoteFailedChunk_', '_refreshWrittenKeys_']);
  eq(removed, []);
  ok(!/[^\x00-\x7F]/.test(NEW.split('v1.20.0 (2026-11-25)')[1].split(' * v1.18.1 ')[0]), 'the 1.20.0 header block is ASCII');
});

test('R2 golden negative: observe without a failure equals the v1.19.0 run', () => {
  const o = {symbols: SYMS(25), batchSize: 10};
  const a = run(OLD, o), b = run(NEW, o);
  eq(b.rec.fetches, a.rec.fetches, 'fetches');
  eq(b.rec.writes, a.rec.writes, 'writes');
  eq(b.rec.panel, a.rec.panel, 'panel');
  eq(b.rec.status, a.rec.status, 'status');
  eq(b.rec.logs.map((l) => [l[0], l[1], l[2], l[3], strip(l[4], NEW_KEYS_LOG)]), a.rec.logs, 'logs');
  eq(strip(b.out, NEW_KEYS_OUT), a.out, 'return');
  eq(b.out.failedRetryMode, 'observe');
  eq(b.checkpoint, null, 'checkpoint cleared');
});

test('R3 observe with a failed batch: the v1.19.0 run plus one ledger line, no retry', () => {
  const o = {symbols: SYMS(25), batchSize: 10, plan: (c) => (c[0] === 'S011' ? 'fail' : 'ok')};
  const a = run(OLD, o), b = run(NEW, o);
  eq(b.rec.fetches, a.rec.fetches, 'no extra fetch');
  eq(b.rec.writes, a.rec.writes, 'writes');
  eq(b.rec.status.map((s) => s.status), ['WARN']);
  eq(a.rec.status.map((s) => s.status), ['WARN']);
  const extra = b.out.warnings.filter((w) => a.out.warnings.indexOf(w) < 0);
  eq(extra.length, 1, 'one added line');
  ok(/^\[v1\.20\.0 FAILED-LEDGER observe\] 10 symbol\(s\) in failed batches kept their previous rows: S011,S012/.test(extra[0]), extra[0]);
  eq(b.out.unwrittenSymbols, SYMS(20).slice(10));
  eq(b.out.partial, false);
  eq(lastLog(b)[4].failedLedger, 10);
});

test('R4 on: a batch that fails once is retried and recovered; the run closes SUCCESS', () => {
  const r = run(NEW, {symbols: SYMS(25), batchSize: 10, props: {REFRESH_FAILED_RETRY: 'on'},
    plan: (c, n) => (c[0] === 'S011' && n === 2 ? 'fail' : 'ok')});
  eq(r.rec.fetches.length, 4, 'three chunks, then the retry of the failed ten in one chunk');
  eq(r.rec.fetches[3], SYMS(20).slice(10));
  eq(r.rec.writes.slice().sort(), SYMS(25));
  eq(r.rec.status.map((s) => s.status), ['SUCCESS']);
  ok(/the retry recovered 10 symbol\(s\)/.test(r.rec.status[0].message), r.rec.status[0].message);
  eq(r.out.partial, false);
  eq(r.out.unwrittenSymbols, []);
  eq([r.out.failedRetried, r.out.failedRecovered], [10, 10]);
});

test('R5 on: a batch that always fails is retried once and the run closes PARTIAL; never written', () => {
  const bad = new Set(['S011', 'S012', 'S013']);
  const r = run(NEW, {symbols: SYMS(25), batchSize: 10, props: {REFRESH_FAILED_RETRY: 'on'},
    plan: (c) => (c.some((s) => bad.has(s)) ? 'fail' : 'ok')});
  const tries = {};
  r.rec.fetches.forEach((c) => c.forEach((s) => { tries[s] = (tries[s] || 0) + 1; }));
  ['S011', 'S015', 'S020'].forEach((s) => eq(tries[s], 2, s + ' fetched twice'));
  eq(tries.S001, 1);
  SYMS(20).slice(10).forEach((s) => ok(r.rec.writes.indexOf(s) < 0, s + ' must keep its previous row'));
  eq(r.rec.status.map((s) => s.status), ['PARTIAL']);
  eq(r.rec.panel.map((p) => p.status), ['PARTIAL']);
  ok(/^Batch refresh partial: 10 symbol\(s\) stayed unwritten after one retry and keep their previous rows\.$/.test(r.rec.status[0].message), r.rec.status[0].message);
  eq(r.out.partial, true);
  eq(r.out.unwrittenSymbols, SYMS(20).slice(10));
  ok(r.out.warnings.some((w) => /^\[v1\.20\.0 FAILED-LEDGER\] retried=10 recovered=0 unwritten=10 \(previous rows kept\): S011/.test(w)));
  eq(lastLog(r)[4].status, 'PARTIAL');
  eq(lastLog(r)[0], 'WARN');
});

test('R6 on: a write that throws is noted, retried and recovered like a failed fetch', () => {
  const r = run(NEW, {symbols: SYMS(12), batchSize: 5, props: {REFRESH_FAILED_RETRY: 'on'},
    plan: (c, n) => (n === 1 ? 'throw' : 'ok')});
  eq(r.rec.fetches[r.rec.fetches.length - 1], SYMS(5));
  eq(r.rec.writes.slice().sort(), SYMS(12));
  eq(r.rec.status[0].status, 'SUCCESS');
});

test('R7 on: the retry is bounded by REFRESH_FAILED_RETRY_MAX_', () => {
  const r = run(NEW, {symbols: SYMS(80), batchSize: 10, props: {REFRESH_FAILED_RETRY: 'on'}, plan: () => 'fail'});
  eq(r.ctx.REFRESH_FAILED_RETRY_MAX_, 60);
  eq(r.out.failedRetried, 60);
  eq(r.rec.fetches.reduce((s, c) => s + c.length, 0), 140, '80 first tries + 60 retries');
  eq(r.out.unwrittenSymbols.length, 80);
  eq(r.rec.status[0].status, 'PARTIAL');
});

test('R8 a pause keeps the ledger and the retry tail; the next run resumes', () => {
  let firstFail = true;
  const a = run(NEW, {symbols: SYMS(30), batchSize: 10, props: {REFRESH_FAILED_RETRY: 'on'}, pauseAfter: 2,
    plan: (c) => { if (c[0] === 'S001' && firstFail) { firstFail = false; return 'fail'; } return 'ok'; }});
  eq(a.out.paused, true);
  ok(a.checkpoint, 'checkpoint kept');
  eq(a.checkpoint.nextIndex, 20);
  eq(a.checkpoint.totalSymbols, 40, 'the failed ten at the tail');
  eq(Object.keys(a.checkpoint.failed).sort(), SYMS(10));
  eq(a.checkpoint.baseCount, 30);
  const b = run(NEW, {symbols: SYMS(30), batchSize: 10, props: {REFRESH_FAILED_RETRY: 'on'}, checkpoint: a.checkpoint});
  eq(b.rec.fetches, [SYMS(30).slice(20), SYMS(10)], 'resumes at 20, then the retry tail');
  eq(b.rec.status[0].status, 'SUCCESS');
  eq(b.checkpoint, null);
});

test('R9 isCheckpointCompatible_: without baseCount, as in v1.19.0', () => {
  const n = load(NEW), o = load(OLD);
  n.arraysEqual_ = o.arraysEqual_ = (x, y) => JSON.stringify(x) === JSON.stringify(y);
  const s = SYMS(5);
  const cases = [
    {pageName: 'P', batchSize: 2, symbols: s.slice(), nextIndex: 0, totalSymbols: 5},
    {pageName: 'P', batchSize: 2, symbols: s.concat(['S001']), nextIndex: 0, totalSymbols: 6},
    {pageName: 'P', batchSize: 3, symbols: s.slice()},
    {pageName: 'Q', batchSize: 2, symbols: s.slice()},
    null
  ];
  cases.forEach((c, i) => eq(n.isCheckpointCompatible_(c, s, 2, 'P'), o.isCheckpointCompatible_(c, s, 2, 'P'), 'case ' + i));
  eq(n.isCheckpointCompatible_({pageName: 'P', batchSize: 2, symbols: s.concat(['S001']), baseCount: 5}, s, 2, 'P'), true);
  eq(n.isCheckpointCompatible_({pageName: 'P', batchSize: 2, symbols: ['X'].concat(s), baseCount: 5}, s, 2, 'P'), false);
  eq(n.isCheckpointCompatible_({pageName: 'P', batchSize: 2, symbols: s.slice(), baseCount: 4}, s, 2, 'P'), true, 'equal lists still match');
});

test('R10 pure helpers', () => {
  const c = load(NEW);
  const k = {symbols: ['a', 'b'], totalSymbols: 2};
  eq(c._refreshNoteFailedChunk_(k, ['a', 'b'], 'observe', 60), 0);
  eq(k.failed, {A: 1, B: 1});
  eq(k.symbols, ['a', 'b']);
  eq(c._refreshNoteFailedChunk_(k, ['a', 'b'], 'on', 1), 1, 'cap 1');
  eq(k.symbols, ['a', 'b', 'a']);
  eq(c._refreshNoteFailedChunk_(k, ['a', 'b'], 'on', 5), 1, 'a was retried already');
  eq(k.failed, {A: 3, B: 3});
  eq(c._refreshClearFailedChunk_(k, ['b', 'z'], {B: 1, Z: 1}), 1);
  eq(c._refreshFailedSymbols_(k), ['A']);
  eq(c._refreshFailedSymbols_(null), []);
  const props = {};
  c.PropertiesService = {getScriptProperties: () => ({getProperty: (x) => (x in props ? props[x] : null)})};
  eq(c._refreshFailedRetryMode_(), 'observe');
  props.REFRESH_FAILED_RETRY = ' ON ';
  eq(c._refreshFailedRetryMode_(), 'on');
  props.REFRESH_FAILED_RETRY = '1';
  eq(c._refreshFailedRetryMode_(), 'observe');
  c.PropertiesService = {getScriptProperties: () => { throw new Error('x'); }};
  eq(c._refreshFailedRetryMode_(), 'observe');
});

test('R11 a symbol comes off the ledger only when its row is written (Codex P2 on #1078)', () => {
  const on = {REFRESH_FAILED_RETRY: 'on'};
  // the retry answers ok with no rows: nothing written, nothing recovered, PARTIAL
  const a = run(NEW, {symbols: SYMS(2), batchSize: 2, props: on, plan: (c, n) => (n === 1 ? 'fail' : 'empty')});
  eq(a.rec.writes, []);
  eq([a.out.failedRecovered, a.out.unwrittenSymbols], [0, SYMS(2)]);
  eq(a.rec.status[0].status, 'PARTIAL');
  // the retry answers with the first row only: that one recovered, the other stays
  const b = run(NEW, {symbols: SYMS(3), batchSize: 3, props: on, plan: (c, n) => (n === 1 ? 'fail' : 'first')});
  eq(b.rec.writes, ['S001']);
  eq([b.out.failedRecovered, b.out.unwrittenSymbols], [1, ['S002', 'S003']]);
  eq(b.rec.status[0].status, 'PARTIAL');
  // the writer defers a retried row: it stays until the v1.13.0 requeue writes it
  const c = run(NEW, {symbols: SYMS(2), batchSize: 2, props: on, defer: ['S002'], plan: (ch, n) => (n === 1 ? 'fail' : 'ok')});
  eq(c.rec.writes.slice().sort(), SYMS(2));
  eq(c.out.unwrittenSymbols, []);
  eq(c.rec.status[0].status, 'SUCCESS');
  // observe: a successful fetch with a missing row still names the earlier failure
  const d = run(NEW, {symbols: SYMS(4), batchSize: 2, plan: (ch, n) => (n === 1 ? 'fail' : 'ok')});
  eq(d.out.unwrittenSymbols, ['S001', 'S002']);
  const k = load(NEW);
  eq(k._refreshLedgerKey_(' aapl.us '), 'AAPL');
  eq(k._refreshWrittenKeys_([{symbol: 'A.US'}, {Symbol: 'b'}, {}], [], [], true, ['b']), {A: 1});
  eq(k._refreshWrittenKeys_([], [['x', 'Y.US'], ['z', '']], ['Name', 'Symbol'], false, null), {Y: 1});
  const cp2 = {failed: {'A.US': 1, B: 2}};
  eq(k._refreshClearFailedChunk_(cp2, ['a.us', 'b'], {A: 1}), 1);
  eq(cp2.failed, {B: 2});
  eq(k._refreshClearFailedChunk_(cp2, ['b'], null), 0, 'no written keys, nothing cleared');
});

console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
