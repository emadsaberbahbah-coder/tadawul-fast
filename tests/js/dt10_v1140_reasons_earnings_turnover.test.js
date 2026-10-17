/* 16_Decision_Top10.gs v1.14.0 (2026-10-17) -- T1 investability-reason table,
 * T5 earnings hold, T6 turnover token + SelLog column. Full Apps Script
 * source loaded into node:vm with minimal service stubs; pure functions
 * called directly. The reversal layer (dt10_v1140_history.js) must take the
 * delivered file back to the hash-proven v1.13.9 source, and every added
 * line must be ES5/ASCII-safe. Run: node tests/js/dt10_v1140_reasons_earnings_turnover.test.js */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {revertV1140, addedLines1140, EDITS_1140} = require('./dt10_v1140_history');
const {feedHistorySource} = require('./dt10_cash_utc_history');

const ROOT = path.join(__dirname, '../..');
// v1.15.0: this harness proves the v1.14.0 layer, so it runs on the delivered
// file with the v1.15.0 edits reversed (dt10_v1150_history.js is itself proven
// by tests/js/dt10_v1150_cluster_seat_cap.test.js).
const {revertV1150} = require('./dt10_v1150_history');
const {revertV1160} = require('./dt10_v1160_history');
const source = revertV1150(revertV1160(fs.readFileSync(path.join(ROOT, 'apps_script/16_Decision_Top10.gs'), 'utf8')));
const plain = v => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));
let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + (e && e.stack || e)); }
}
function load(props) {
  const store = Object.assign({}, props || {});
  const propsService = {
    getProperty(k) { return Object.prototype.hasOwnProperty.call(store, k) ? store[k] : null; },
    setProperty(k, v) { store[k] = String(v); return propsService; },
    deleteProperty(k) { delete store[k]; return propsService; },
    getProperties() { return store; }
  };
  const ctx = {
    console, Logger: { log() {} }, Date,
    Utilities: { formatDate() { return ''; }, sleep() {}, getUuid() { return 'u'; }, computeDigest() { return []; },
      base64Encode() { return ''; }, DigestAlgorithm: { MD5: 'MD5', SHA_256: 'SHA_256' }, Charset: { UTF_8: 'UTF_8' } },
    PropertiesService: { getScriptProperties() { return propsService; }, getDocumentProperties() { return null; }, getUserProperties() { return propsService; } },
    SpreadsheetApp: { getActiveSpreadsheet() { throw new Error('no sheet in this harness'); }, getActive() { throw new Error('no sheet'); }, flush() {}, BorderStyle: { SOLID: 'SOLID' } },
    UrlFetchApp: { fetch() { throw new Error('no network'); } },
    Session: { getScriptTimeZone() { return 'Asia/Riyadh'; } },
    CacheService: { getScriptCache() { return { get() { return null; }, put() {} }; } },
    LockService: { getScriptLock() { return { tryLock() { return true; }, releaseLock() {} }; } }
  };
  vm.createContext(ctx);
  vm.runInContext(source, ctx, { filename: '16_Decision_Top10.gs' });
  return ctx;
}

/* ---------------------------------------------------------------- V */
test('V1 version lockstep, header line 4, syntax, census 199/104, the eight functions and one var exist', () => {
  const ctx = load();
  assert.equal(ctx.DT10_VERSION, '1.14.0');
  assert.match(source.split('\n')[3], /^ \* Version: 1\.14\.0 /);
  assert(source.includes(" * v1.14.0 (2026-10-17) -- INVESTABILITY REASONS, EARNINGS HOLD, TURNOVER TOKEN (T1/T5/T6)"));
  assert(source.includes(' * v1.13.9 (2026-10-14) -- ENTERED CASH LABEL AND HOLDING UTC PROVENANCE'), 'prior WHY kept');
  assert.doesNotThrow(() => new vm.Script(source, { filename: '16_Decision_Top10.gs' }));
  const fns = new Set(); let m; const re = /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg; while ((m = re.exec(source))) fns.add(m[1]);
  const vars = new Set(); const rv = /^var\s+([A-Za-z_$][\w$]*)/mg; while ((m = rv.exec(source))) vars.add(m[1]);
  assert.equal(fns.size, 199); assert.equal(vars.size, 104);
  for (const f of ['dt10InvestReasonsLegacy_', 'dt10InvestReasonRows_', 'dt10EarnHoldLegacy_', 'dt10EarningsSuspend_',
    'dt10ApplyEarningsHold_', 'dt10TurnoverLegacy_', 'dt10TurnoverNote_', 'dt10SelLogNewToday_']) {
    assert(fns.has(f), f); assert.equal(typeof ctx[f], 'function', f);
  }
  assert(vars.has('DT10_INVREASON_HEADERS'));
  assert.deepEqual(plain(ctx.DT10_INVREASON_HEADERS), ['Engine Reason', 'Candidates', 'Share %', 'Example Symbols']);
});
test('V2 reversal layer: the eleven v1.14.0 edits occur exactly once and the chain reaches the pinned v1.13.8 digest', () => {
  assert.equal(EDITS_1140.length, 11);
  const prior = revertV1140(source);
  assert(prior.includes("var DT10_VERSION = '1.13.9';") && !prior.includes('dt10TurnoverNote_'));
  feedHistorySource(prior);   // throws on any digest mismatch
  assert.throws(() => revertV1140(source + "\nvar DT10_VERSION = '1.14.0';"), /exactly one approved v1\.14\.0 edit/);
});
test('V3 every added line is ES5 and free of smart quotes / CR', () => {
  const bad = /\b(let|const)\b|=>|`|\bclass\b|\.\.\.|\.includes\(/;
  for (const line of addedLines1140()) {
    assert(!bad.test(line), 'ES5: ' + line);
    assert(!/[‘’“”\r]/.test(line), 'quotes/CR: ' + line);
  }
  assert.equal((source.match(/[‘’“”]/g) || []).length, 0);
});

/* ---------------------------------------------------------------- T1 */
const META = { investability_reasons: { total: 466, other_reasons: 2, distinct_reasons: 8, rows: [
  { reason: 'Engine recommends SELL', count: 178, share_pct: 38.2, examples: ['AAA.US', 'BBB.US', 'CCC.US'] },
  { reason: 'Engine recommends REDUCE', count: 138, share_pct: 29.6, examples: ['DDD.US'] },
  { reason: 'Other (2 reasons)', count: 12, share_pct: 2.6, examples: [] }] } };
test('T1a rows come from meta.investability_reasons in table shape (reason, count, share, examples)', () => {
  const ctx = load();
  const rows = plain(ctx.dt10InvestReasonRows_(META));
  assert.deepEqual(rows, [['Engine recommends SELL', 178, 38.2, 'AAA.US, BBB.US, CCC.US'],
    ['Engine recommends REDUCE', 138, 29.6, 'DDD.US'], ['Other (2 reasons)', 12, 2.6, '']]);
  assert.equal(rows.reduce((a, r) => a + r[1], 0) <= META.investability_reasons.total, true);
});
test('T1b missing / odd shapes yield [] and never throw', () => {
  const ctx = load();
  for (const meta of [undefined, null, {}, { investability_reasons: null }, { investability_reasons: { rows: [] } },
    { investability_reasons: { rows: 'junk' } }, { investability_reasons: 7 }]) {
    assert.deepEqual(plain(ctx.dt10InvestReasonRows_(meta)), [], JSON.stringify(meta));
  }
  assert.deepEqual(plain(ctx.dt10InvestReasonRows_({ investability_reasons: { rows: [null, { reason: 'x' }] } })),
    [['', '', 0, ''], ['x', '', 0, '']]);
});
test('T1c kill switch reads DT10_INVEST_REASONS_LEGACY = "1" only', () => {
  assert.equal(load().dt10InvestReasonsLegacy_(), false);
  assert.equal(load({ DT10_INVEST_REASONS_LEGACY: '1' }).dt10InvestReasonsLegacy_(), true);
  assert.equal(load({ DT10_INVEST_REASONS_LEGACY: 'true' }).dt10InvestReasonsLegacy_(), false);
  assert(source.includes("'INVESTABILITY \\u2014 BY ENGINE REASON ('"), 'section drawn under DATA GAPS');
  const gaps = source.indexOf('  row = gapT.next + 1;'), tbl = source.indexOf('dt10InvestReasonRows_(payload.meta)'), cand = source.indexOf('// CANDIDATES audit grid');
  assert(gaps > 0 && gaps < tbl && tbl < cand, 'table sits between DATA GAPS and the CANDIDATES grid');
});

/* ---------------------------------------------------------------- T5 */
function ticket(sym, extra) {
  return Object.assign({ symbol: sym, entry_zone: '10.0–10.5', suggested_sar: 4650, suggested_shares: 120, stop_sar: 9.1,
    tp1_sar: 11.0, tp2_sar: 12.0, exp_gain_12m_sar: 900, advisor_note: 'Buy the dip.', detail: { funds_from: 'Cash 53,769 SAR', review_date: '2026-11-16' } }, extra || {});
}
test('T5a dt10EarningsSuspend_ applies the P-145 dash vocabulary, keeps the seat, blanks funds_from on a copy', () => {
  const ctx = load();
  const d0 = { funds_from: 'Cash 53,769 SAR', review_date: '2026-11-16' };
  const tk = ticket('EPRT.US', { sizing_hold: 'earnings', earnings_hold_days: 4, detail: d0 });
  const out = ctx.dt10EarningsSuspend_(tk);
  assert.equal(out, tk);
  assert.equal(tk._earn_hold, true);
  for (const k of ['entry_zone', 'suggested_sar', 'suggested_shares', 'stop_sar', 'tp1_sar', 'tp2_sar', 'exp_gain_12m_sar']) assert.equal(tk[k], '—', k);
  assert.equal(tk.detail.funds_from, '—'); assert.equal(d0.funds_from, 'Cash 53,769 SAR', 'raw detail untouched');
  assert.equal(tk.detail.review_date, '2026-11-16');
  assert.match(tk.advisor_note, /^EARNINGS HOLD \(4d to print\) — sizing suspended until the earnings window clears/);
  assert.equal(tk.symbol, 'EPRT.US', 'seat kept');
  assert.match(ctx.dt10EarningsSuspend_(ticket('X', {})).advisor_note, /^EARNINGS HOLD \(\?d to print\)/);
  assert.match(ctx.dt10EarningsSuspend_(ticket('Y', { earnings_hold_days: 0 })).advisor_note, /^EARNINGS HOLD \(0d to print\)/);
});
test('T5b dt10ApplyEarningsHold_ suspends only marked seats and returns earn-held n', () => {
  const ctx = load();
  const payload = { selected: [ticket('VICI.US', { sizing_hold: 'earnings', earnings_hold_days: 2 }), ticket('AX.US'),
    ticket('GLPI.US', { sizing_hold: 'earnings', earnings_hold_days: 5 }), ticket('WL.US', { sizing_hold: 'other' })] };
  assert.equal(ctx.dt10ApplyEarningsHold_(payload), 'earn-held 2');
  assert.equal(payload.selected.length, 4, 'no seat removed');
  assert.equal(payload.selected[0]._earn_hold, true); assert.equal(payload.selected[2]._earn_hold, true);
  assert.equal(payload.selected[1].suggested_sar, 4650); assert.equal(payload.selected[3].suggested_sar, 4650);
  assert.equal(ctx.dt10ApplyEarningsHold_({ selected: [ticket('A')] }), '');
  assert.equal(ctx.dt10ApplyEarningsHold_({}), ''); assert.equal(ctx.dt10ApplyEarningsHold_(null), '');
});
test('T5c kill switch DT10_EARN_HOLD_LEGACY = "1" leaves marked tickets untouched and prints nothing', () => {
  const ctx = load({ DT10_EARN_HOLD_LEGACY: '1' });
  const payload = { selected: [ticket('VICI.US', { sizing_hold: 'earnings', earnings_hold_days: 2 })] };
  assert.equal(ctx.dt10ApplyEarningsHold_(payload), '');
  assert.equal(payload.selected[0].suggested_sar, 4650); assert.equal(payload.selected[0]._earn_hold, undefined);
  assert(source.includes("var earnHoldNote = dt10ApplyEarningsHold_(payload);"), 'hook after finalize');
  assert(source.includes("(earnHoldNote ? ' | ' + earnHoldNote : '') +"), 'status token wired');
});

/* ---------------------------------------------------------------- T6 */
test('T6a turnover token counts seats whose clock started today; blank when unknown or empty', () => {
  const ctx = load();
  const sel = [{ _stab_days: 1 }, { _stab_days: 1 }, { _stab_days: 7 }, { _stab_days: 1 }, { _stab_days: 22 }];
  assert.equal(ctx.dt10TurnoverNote_({ selected: sel }), 'turnover=3/5 new');
  assert.equal(ctx.dt10TurnoverNote_({ selected: [{ _stab_days: 7 }] }), 'turnover=0/1 new');
  assert.equal(ctx.dt10TurnoverNote_({ selected: [{ symbol: 'A' }, { symbol: 'B' }] }), '', 'stability OFF/frozen: no fields');
  assert.equal(ctx.dt10TurnoverNote_({ selected: [] }), ''); assert.equal(ctx.dt10TurnoverNote_(null), '');
  assert.equal(ctx.dt10TurnoverNote_({ selected: [{ _stab_days: '1' }, { _stab_days: 2 }] }), 'turnover=1/2 new', 'string day');
});
test('T6b kill switch DT10_TURNOVER_TOKEN_LEGACY = "1" silences the token only', () => {
  const ctx = load({ DT10_TURNOVER_TOKEN_LEGACY: '1' });
  assert.equal(ctx.dt10TurnoverNote_({ selected: [{ _stab_days: 1 }] }), '');
  assert.equal(ctx.dt10SelLogNewToday_({ _stab_days: 1 }), 'YES', 'SelLog column unaffected by the token switch');
  assert(source.includes("(turnoverNote ? ' | ' + turnoverNote : '') +"), 'status token wired after stab note');
  const stabIdx = source.indexOf("(stab && stab.note ? ' | ' + stab.note : '') +"), toIdx = source.indexOf("(turnoverNote ? ' | ' + turnoverNote : '') +"), earnIdx = source.indexOf("(earn && earn.note ? ' | ' + earn.note : '') +");
  assert(stabIdx < toIdx && toIdx < earnIdx, 'order stab | turnover | earn');
});
test('T6c _Selection_Log gains column 34 New Today: headers, row builder, header heal, positional withhold indexes untouched', () => {
  const ctx = load();
  assert.equal(ctx.DT10_SELLOG_HEADERS.length, 34);
  assert.equal(ctx.DT10_SELLOG_HEADERS[31], 'Stability'); assert.equal(ctx.DT10_SELLOG_HEADERS[32], 'Days'); assert.equal(ctx.DT10_SELLOG_HEADERS[33], 'New Today');
  const row = ctx.dt10SelLogRowFromTicket_({ symbol: 'AX.US', _stab_status: 'FAST-TRACK (day 1)', _stab_days: 1 }, 'stamp', 'run', '{}');
  assert.equal(row.length, 34); assert.equal(row[33], 'YES'); assert.equal(row[32], 1); assert.equal(row[4], 'AX.US');
  assert.equal(ctx.dt10SelLogRowFromTicket_({ _stab_days: 9 }, 's', 'r', '{}')[33], 'NO');
  assert.equal(ctx.dt10SelLogRowFromTicket_({}, 's', 'r', '{}')[33], '');
  assert.equal(ctx.dt10SelLogNewToday_(null), '');
  assert.equal(ctx.dt10SelLogNewToday_({ _stab_days: 0 }), 'NO');
  assert.deepEqual(plain(ctx.DT10_UV_LOG_WITHHOLD_IDX), [9, 11, 12, 13, 14, 15, 16, 17, 18, 20, 21, 25]);
  assert.equal(ctx.DT10_UV_LOG_NOTE_IDX, 27);
  assert(source.includes("sh.getRange(1, 34).setValue('New Today').setFontWeight('bold');"), 'header heal for existing sheets');
});

console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
