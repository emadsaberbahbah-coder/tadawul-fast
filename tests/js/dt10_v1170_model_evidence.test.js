/* 16_Decision_Top10.gs v1.17.0 (2026-10-17) -- L3: the MODEL EVIDENCE strip
 * from the _Forecast_Scorecard tab (track_performance v6.48.0). Pure cores in
 * node:vm; the reversal layer (dt10_v1170_history.js) takes the file back to
 * v1.16.0 for the hash chain. Run: node tests/js/dt10_v1170_model_evidence.test.js */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {revertV1170, addedLines1170, EDITS_1170} = require('./dt10_v1170_history');
const {revertV1160} = require('./dt10_v1160_history');
const {revertV1150} = require('./dt10_v1150_history');
const {revertV1140} = require('./dt10_v1140_history');
const {feedHistorySource} = require('./dt10_cash_utc_history');

const ROOT = path.join(__dirname, '../..');
// v1.18.0: this harness proves the v1.17.0 layer on the delivered file with the
// v1.18.0 edits reversed (proven by dt10_v1180_evidence_send.test.js).
const {revertV1180} = require('./dt10_v1180_history');
const source = revertV1180(fs.readFileSync(path.join(ROOT, 'apps_script/16_Decision_Top10.gs'), 'utf8'));
const plain = v => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));
let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + (e && e.stack || e)); }
}
function load(props, sheets) {
  const store = Object.assign({}, props || {});
  const propsService = {
    getProperty(k) { return Object.prototype.hasOwnProperty.call(store, k) ? store[k] : null; },
    setProperty(k, v) { store[k] = String(v); return propsService; },
    deleteProperty(k) { delete store[k]; return propsService; }
  };
  const ss = { getSheetByName(n) {
    if (!sheets || !Object.prototype.hasOwnProperty.call(sheets, n)) return null;
    if (sheets[n] === 'throw') throw new Error('boom');
    return { getDataRange() { return { getValues() { return sheets[n]; } }; } };
  } };
  const ctx = {
    console, Logger: { log() {} }, Date,
    Utilities: { formatDate() { return ''; }, sleep() {}, getUuid() { return 'u'; }, computeDigest() { return []; },
      base64Encode() { return ''; }, DigestAlgorithm: { MD5: 'MD5', SHA_256: 'SHA_256' }, Charset: { UTF_8: 'UTF_8' } },
    PropertiesService: { getScriptProperties() { return propsService; }, getDocumentProperties() { return null; }, getUserProperties() { return propsService; } },
    SpreadsheetApp: { getActiveSpreadsheet() { return ss; }, getActive() { return ss; }, flush() {}, BorderStyle: { SOLID: 'SOLID' } },
    UrlFetchApp: { fetch() { throw new Error('no network'); } },
    Session: { getScriptTimeZone() { return 'Asia/Riyadh'; } },
    CacheService: { getScriptCache() { return { get() { return null; }, put() {} }; } },
    LockService: { getScriptLock() { return { tryLock() { return true; }, releaseLock() {} }; } }
  };
  vm.createContext(ctx);
  vm.runInContext(source, ctx, { filename: '16_Decision_Top10.gs' });
  return { ctx, ss };
}
const HEAD = ['As Of (Riyadh)', 'Horizon', 'Dimension', 'Cohort', 'n', 'Wins', 'Losses', 'Hit Rate %', 'Hit Lo %', 'Hit Hi %', 'Mean ROI %', 'Median ROI %', 'Tracker Version'];
const GRID = [HEAD,
  ['2026-10-17 11:00:00', '1M', 'ALL', 'ALL', 11791, 5000, 5100, 49.5, 48.6, 50.4, 0.94, -2.5, '6.48.0'],
  ['2026-10-17 11:00:00', '3M', 'ALL', 'ALL', 9000, 4000, 4500, 47.1, 46.0, 48.1, -1.32, -2.5, '6.48.0'],
  ['2026-10-17 11:00:00', '1M', 'Investability', 'INVESTABLE', 963, 450, 505, 47.1, 44.0, 50.3, 1.1, -1.0, '6.48.0'],
  ['2026-10-17 11:00:00', '1M', 'Investability', 'WATCHLIST', 10100, 4700, 5300, 47.1, 46.1, 48.1, 0.9, -2.6, '6.48.0'],
  ['2026-10-17 11:00:00', '3M', 'Investability', 'BLOCKED', 500, 100, 395, 20.2, 16.9, 24.0, -3.0, -4.0, '6.48.0'],
  ['2026-10-17 11:00:00', '1M', 'Recommendation', 'HOLD', 2000, 1026, 974, 51.3, 49.1, 53.5, 0.5, -1.0, '6.48.0'],
  ['2026-10-17 11:00:00', '1M', 'Investability x Recommendation', 'INVESTABLE|BUY', 671, 420, 243, 63.3, 59.6, 66.9, 2.0, 1.0, '6.48.0'],
  ['2026-10-17 11:00:00', '1M', 'Origin Tab', 'Top_10_Investments', 3000, 1485, 1515, 49.5, 47.7, 51.3, 1.0, 0.0, '6.48.0'],
  ['2026-10-17 11:00:00', '1M', 'Origin Tab', 'Decision_Coverage', 8000, 2928, 5072, 36.6, 35.6, 37.7, 0.3, -3.0, '6.48.0'],
  ['', '', '', '', '', '', '', '', '', '', '', '', '']];

test('V1 version lockstep, header, syntax, census 217/106, functions and var exist', () => {
  const { ctx } = load();
  assert.equal(ctx.DT10_VERSION, '1.17.0');
  assert.match(source.split('\n')[3], /^ \* Version: 1\.17\.0 /);
  assert(source.includes(' * v1.17.0 (2026-10-17) -- MODEL EVIDENCE STRIP FROM THE FORECAST SCORECARD (L3)'));
  assert(source.includes(' * v1.16.0 (2026-10-17) -- RESEARCH LEVELS WHEN SIZING IS WITHHELD (T7)'), 'prior WHY kept');
  assert.doesNotThrow(() => new vm.Script(source, { filename: '16_Decision_Top10.gs' }));
  const fns = new Set(); let m; const re = /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg; while ((m = re.exec(source))) fns.add(m[1]);
  const vars = new Set(); const rv = /^var\s+([A-Za-z_$][\w$]*)/mg; while ((m = rv.exec(source))) vars.add(m[1]);
  assert.equal(fns.size, 217); assert.equal(vars.size, 106);
  for (const f of ['dt10ModelEvidenceLegacy_', 'dt10ReadScorecard_', 'dt10FmtN_', 'dt10ScorecardIndex_', 'dt10ModelEvidenceLines_']) {
    assert(fns.has(f), f); assert.equal(typeof ctx[f], 'function', f);
  }
  assert.equal(ctx.DT10_SCORECARD_SHEET, '_Forecast_Scorecard');
});
test('V2 reversal layer: six v1.17.0 edits occur once; the chain reaches the pinned v1.13.8 digest', () => {
  assert.equal(EDITS_1170.length, 6);
  const prior = revertV1170(source);
  assert(prior.includes("var DT10_VERSION = '1.16.0';") && !prior.includes('dt10ModelEvidenceLines_'));
  feedHistorySource(revertV1140(revertV1150(revertV1160(prior))));
  assert.throws(() => revertV1170(source + "\nvar DT10_VERSION = '1.17.0';"), /exactly one approved v1\.17\.0 edit/);
});
test('V3 every added line is ES5 and free of smart quotes / CR', () => {
  const bad = /\b(let|const)\b|=>|`|\bclass\b|\.\.\.|\.includes\(/;
  for (const line of addedLines1170()) { assert(!bad.test(line), 'ES5: ' + line); assert(!/[‘’“”\r]/.test(line), 'quotes/CR: ' + line); }
  assert.equal((source.match(/[‘’“”]/g) || []).length, 0);
});
test('L1 index: header-name driven, blank rows skipped, asOf from the first row, null on a foreign header or empty grid', () => {
  const { ctx } = load();
  const idx = ctx.dt10ScorecardIndex_(GRID);
  assert.equal(idx.rows, 9); assert.equal(idx.asOf, '2026-10-17 11:00:00');
  assert.deepEqual(plain(idx.map['Investability|1M|INVESTABLE']), { n: 963, wins: 450, losses: 505, hit: 47.1, lo: 44, hi: 50.3, mean: 1.1, median: -1 });
  assert.equal(ctx.dt10ScorecardIndex_([['x', 'y']]), null); assert.equal(ctx.dt10ScorecardIndex_([HEAD]), null);
  assert.equal(ctx.dt10ScorecardIndex_(null), null); assert.equal(ctx.dt10ScorecardIndex_([]), null);
  const shuffled = [['Cohort', 'Horizon', 'n', 'Hit Rate %', 'Dimension'], ['ALL', '1M', '7', '42.9', 'ALL']];
  const i2 = ctx.dt10ScorecardIndex_(shuffled);
  assert.deepEqual(plain(i2.map['ALL|1M|ALL']), { n: 7, wins: null, losses: null, hit: 42.9, lo: null, hi: null, mean: null, median: null });
  assert.equal(i2.asOf, '');
  assert.equal(ctx.dt10FmtN_(10100), '10,100'); assert.equal(ctx.dt10FmtN_('963'), '963'); assert.equal(ctx.dt10FmtN_(null), '0');
});
test('L2 lines: investability by horizon, each seat own cohort with fallback, origin + mean ROI, the "not a gate" closer', () => {
  const { ctx } = load();
  const idx = ctx.dt10ScorecardIndex_(GRID);
  const selected = [{ symbol: 'AX.US', detail: { engine_recommendation: 'BUY' } }, { symbol: 'VICI.US', detail: { engine_recommendation: 'Hold' } },
    { symbol: 'ZZ.US', detail: { engine_recommendation: 'STRONG SELL' } }, { symbol: '' }];
  const cands = [{ symbol: 'AX.US', engine_gate: { investability: 'INVESTABLE' } }, { symbol: 'VICI.US', engine_gate: { investability: 'WATCHLIST' } }];
  const lines = ctx.dt10ModelEvidenceLines_(idx, selected, cands);
  assert.equal(lines.length, 3);
  assert.equal(lines[0], 'MODEL EVIDENCE — matured Performance_Log outcomes (as of 2026-10-17 11:00:00) — hit-rate by Entry Investability: ' +
    '1M: INVESTABLE 47.1% (n=963, 44%–50.3%) · WATCHLIST 47.1% (n=10,100, 46.1%–48.1%) | 3M: BLOCKED 20.2% (n=500, 16.9%–24%)');
  assert.equal(lines[1], 'Seats: AX.US INVESTABLE/BUY 1M 63.3% (n=671, 59.6%–66.9%) · VICI.US HOLD (rec only) 1M 51.3% (n=2,000, 49.1%–53.5%) · ZZ.US UNSPECIFIED/STRONG_SELL 1M n/a');
  assert.equal(lines[2], 'Origin 1M: Top_10 49.5% (n=3,000, 47.7%–51.3%) vs Decision_Coverage 36.6% (n=8,000, 35.6%–37.7%) | mean realized ROI 1M +0.94% (med -2.5%) · 3M -1.32% (med -2.5%) — evidence, not a gate (L4 observe decides).');
  assert.deepEqual(plain(ctx.dt10ModelEvidenceLines_(null, selected, cands)), []);
  const sparse = ctx.dt10ModelEvidenceLines_(ctx.dt10ScorecardIndex_([HEAD, ['', '1M', 'Score Decile', 'D7', 5, 3, 2, 60, 23, 88, 1, 1, 'v']]), [], []);
  assert.equal(sparse.length, 3); assert.match(sparse[0], /no cohort published yet$/); assert.equal(sparse[1], 'Seats: none');
  assert.match(sparse[2], /^no origin \/ ROI cohorts published yet — evidence, not a gate/);
});
test('L3 reader: null for a missing tab, an empty tab, a throwing sheet or the kill switch; grid otherwise', () => {
  assert.equal(load({}, {}).ctx.dt10ReadScorecard_(load({}, {}).ss), null);
  const one = load({}, { _Forecast_Scorecard: [HEAD] }); assert.equal(one.ctx.dt10ReadScorecard_(one.ss), null, 'header only = empty');
  const thr = load({}, { _Forecast_Scorecard: 'throw' }); assert.equal(thr.ctx.dt10ReadScorecard_(thr.ss), null);
  const off = load({ DT10_MODEL_EVIDENCE_LEGACY: '1' }, { _Forecast_Scorecard: GRID }); assert.equal(off.ctx.dt10ReadScorecard_(off.ss), null);
  assert.equal(load({ DT10_MODEL_EVIDENCE_LEGACY: '1' }).ctx.dt10ModelEvidenceLegacy_(), true); assert.equal(load().ctx.dt10ModelEvidenceLegacy_(), false);
  const on = load({}, { _Forecast_Scorecard: GRID }); assert.equal(on.ctx.dt10ReadScorecard_(on.ss).length, GRID.length);
  assert.equal(on.ctx.dt10ReadScorecard_(null), null);
});
test('L4 hooks: the strip is drawn at the top of the dynamic zone before the research-levels block and the SELECTED title; selftest line present', () => {
  const zones = source.indexOf('  var row = DT10_ZONES_START;'), strip = source.indexOf('  var meLines = dt10ModelEvidenceLines_('),
    t7 = source.indexOf('  var rlCtx = dt10ResearchLevelsCtx_(payload, selected);'), title = source.indexOf('row = dt10WriteSection_(sheet, row, dt10SelTitle, tokens);');
  assert(zones > 0 && zones < strip && strip < t7 && t7 < title);
  assert(source.includes("    sheet.getRange(row, 1, 1, DT10_LAST_COL).merge().setValue(meLines[mi])"));
  assert(source.includes("  if (meLines.length) row++;"));
  assert(source.includes("report.push('model evidence core: ' +"));
});

console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
