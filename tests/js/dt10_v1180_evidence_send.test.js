/* 16_Decision_Top10.gs v1.18.0 (2026-10-17) -- L4: send the Forecast Scorecard's
 * Investability x Recommendation cohorts to the backend evidence gate
 * (Script Property DT10_EVIDENCE_SCORECARD_SEND = on; default off = request
 * byte-identical). Run: node tests/js/dt10_v1180_evidence_send.test.js */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {revertV1180, addedLines1180, EDITS_1180} = require('./dt10_v1180_history');
const {revertV1170} = require('./dt10_v1170_history');
const {revertV1160} = require('./dt10_v1160_history');
const {revertV1150} = require('./dt10_v1150_history');
const {revertV1140} = require('./dt10_v1140_history');
const {feedHistorySource} = require('./dt10_cash_utc_history');
const {revertV1190} = require('./dt10_v1190_history');

const ROOT = path.join(__dirname, '../..');
const source = revertV1190(fs.readFileSync(path.join(ROOT, 'apps_script/16_Decision_Top10.gs'), 'utf8'));
const plain = v => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));
let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + (e && e.stack || e)); }
}
function load(props) {
  const store = Object.assign({}, props || {});
  const propsService = { getProperty(k) { return Object.prototype.hasOwnProperty.call(store, k) ? store[k] : null; },
    setProperty(k, v) { store[k] = String(v); return propsService; }, deleteProperty(k) { delete store[k]; return propsService; } };
  const ctx = {
    console, Logger: { log() {} }, Date,
    Utilities: { formatDate() { return ''; }, sleep() {}, getUuid() { return 'u'; }, computeDigest() { return []; },
      base64Encode() { return ''; }, DigestAlgorithm: { MD5: 'MD5', SHA_256: 'SHA_256' }, Charset: { UTF_8: 'UTF_8' } },
    PropertiesService: { getScriptProperties() { return propsService; }, getDocumentProperties() { return null; }, getUserProperties() { return propsService; } },
    SpreadsheetApp: { getActiveSpreadsheet() { throw new Error('no sheet'); }, getActive() { throw new Error('no sheet'); }, flush() {}, BorderStyle: { SOLID: 'SOLID' } },
    UrlFetchApp: { fetch() { throw new Error('no network'); } }, Session: { getScriptTimeZone() { return 'Asia/Riyadh'; } },
    CacheService: { getScriptCache() { return { get() { return null; }, put() {} }; } },
    LockService: { getScriptLock() { return { tryLock() { return true; }, releaseLock() {} }; } }
  };
  vm.createContext(ctx);
  vm.runInContext(source, ctx, { filename: '16_Decision_Top10.gs' });
  return ctx;
}
test('V1 version lockstep, census 219/106, functions exist', () => {
  const ctx = load();
  assert.equal(ctx.DT10_VERSION, '1.18.0');
  assert.match(source.split('\n')[3], /^ \* Version: 1\.18\.0 /);
  assert(source.includes(' * v1.18.0 (2026-10-17) -- SEND THE SCORECARD COHORTS TO THE EVIDENCE GATE (L4)'));
  assert.doesNotThrow(() => new vm.Script(source, { filename: '16_Decision_Top10.gs' }));
  const fns = new Set(); let m; const re = /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg; while ((m = re.exec(source))) fns.add(m[1]);
  const vars = new Set(); const rv = /^var\s+([A-Za-z_$][\w$]*)/mg; while ((m = rv.exec(source))) vars.add(m[1]);
  assert.equal(fns.size, 219); assert.equal(vars.size, 106);
  for (const f of ['dt10EvidenceScorecardSendOn_', 'dt10EvidenceScorecardRows_']) assert.equal(typeof ctx[f], 'function', f);
});
test('V2 reversal layer: seven edits once; chain to the pinned v1.13.8 digest', () => {
  assert.equal(EDITS_1180.length, 7);
  const prior = revertV1180(source);
  assert(prior.includes("var DT10_VERSION = '1.17.0';") && !prior.includes('dt10EvidenceScorecardRows_'));
  feedHistorySource(revertV1140(revertV1150(revertV1160(revertV1170(prior)))));
  assert.throws(() => revertV1180(source + "\nvar DT10_VERSION = '1.18.0';"), /exactly one approved v1\.18\.0 edit/);
});
test('V3 every added line is ES5 and free of smart quotes / CR', () => {
  const bad = /\b(let|const)\b|=>|`|\bclass\b|\.\.\.|\.includes\(/;
  for (const line of addedLines1180()) { assert(!bad.test(line), 'ES5: ' + line); assert(!/[‘’“”\r]/.test(line), 'quotes/CR: ' + line); }
});
test('S1 gate reads on only; rows: the Investability x Recommendation dimension, largest n first, capped at 400, [] on nothing', () => {
  assert.equal(load().dt10EvidenceScorecardSendOn_(), false);
  assert.equal(load({ DT10_EVIDENCE_SCORECARD_SEND: 'on' }).dt10EvidenceScorecardSendOn_(), true);
  for (const v of ['off', '1', 'true', '']) assert.equal(load({ DT10_EVIDENCE_SCORECARD_SEND: v }).dt10EvidenceScorecardSendOn_(), false, v);
  const ctx = load();
  const idx = { asOf: 'x', rows: 4, map: {
    'Investability x Recommendation|1M|INVESTABLE|BUY': { n: 671, hit: 63.3, lo: 1, hi: 2, mean: 0, median: 0 },
    'Investability x Recommendation|3M|WATCHLIST|HOLD': { n: 5000, hit: 47, lo: null, hi: null, mean: null, median: null },
    'Investability x Recommendation|1M|BLOCKED|SELL': { n: 9, hit: null },
    'Investability|1M|INVESTABLE': { n: 963, hit: 47.1 }, 'ALL|1M|ALL': { n: 11791, hit: 49.5 }, 'Investability x Recommendation|junk': { n: 1, hit: 1 } } };
  assert.deepEqual(plain(ctx.dt10EvidenceScorecardRows_(idx)), [{ horizon: '3M', cohort: 'WATCHLIST|HOLD', n: 5000, hit: 47 }, { horizon: '1M', cohort: 'INVESTABLE|BUY', n: 671, hit: 63.3 }]);
  assert.deepEqual(plain(ctx.dt10EvidenceScorecardRows_(null)), []); assert.deepEqual(plain(ctx.dt10EvidenceScorecardRows_({})), []);
  const big = { map: {} }; for (let i = 0; i < 450; i++) big.map['Investability x Recommendation|1M|C' + i + '|BUY'] = { n: i, hit: 50 };
  const rows = plain(ctx.dt10EvidenceScorecardRows_(big)); assert.equal(rows.length, 400); assert.equal(rows[0].n, 449);
});
test('S2 hooks: body gains evidence_scorecard only inside the property branch; status token wired; selftest line', () => {
  const body = source.indexOf('    pool_limit: poolLimit\n  };\n'), hook = source.indexOf('  if (dt10EvidenceScorecardSendOn_()) {'), held = source.indexOf('  if (dt10SendHoldingsEnabled_()) {', body);
  assert(body > 0 && body < hook && hook < held, 'hook right after the body literal, before the holdings block');
  assert(source.includes('      body.evidence_scorecard = escRows;'));
  assert(source.includes("poolNote + heldNote + cashNote + scorecardNote + ' | ' + dt10MetaLine_(payload.meta) +"));
  assert(source.includes("report.push('scorecard rows core: ' +"));
});
console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
