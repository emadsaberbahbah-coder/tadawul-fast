#!/usr/bin/env node
/* tests/test_dp2_v1470_div_log.js
 * Harness for apps_script/27_Decision_Pages_v2.gs v1.4.7 [DIV-LOG] (full audit, Wed 25 Nov 2026: _DP2_Exit_State read
 * SUPN.US 'SELL NOW - stop 33.76 hit' on cached bars while every page of record read HOLD; a cached chart carries no
 * dividend events and Dividend_Events lists only coming ex-dates, so the 8.13 USD special of 3 Nov reached neither
 * source). The REAL Apps Script source runs in a vm. The v1.4.6 file (OLD, SHA-256 pinned 12fde222 = origin/main
 * cd69829) is rebuilt by reversing the enumerated v1.4.7 hunks (tests/js/dp2_edits_v1470.json). Every row, price and
 * date is synthetic (the SUPN shape uses round invented closes around the real entry).
 *   R0  oracle: reversal = v1.4.6; history chain; versions; both compile; +4 functions, 0 removed; ES5 / ASCII hunks
 *   R1  GOLDEN NEGATIVE: DP2_DIV_LOG off (five spellings) -> dp2DivLog_ is {} and dp2ExitDivsFor_ equals the v1.4.6
 *       function on every case; on, with no log row for the symbol -> equal too
 *   R2  the SUPN shape on cached bars: v1.4.6 applies no dividend and the trail reads a stop hit; v1.4.7 takes the
 *       ledger's 8.13 (net 5.691 after the 30% tax row), entry about 29.89, and the stop sits under the last price
 *   R3  the pure parser and the source rules (arrived, currency, no double count, a blank currency never guessed);
 *       the self-test line
 *   R4  mutants: each guard removed is caught
 * The battery runs three times and the digests must match; the last line is 'PASS k/k' (exit 1 on any failure).
 */
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const vm = require('vm');
const ROOT = path.join(__dirname, '..');
const NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '27_Decision_Pages_v2.gs');
const NEW = fs.readFileSync(NEW_PATH, 'utf8');
const EDITS = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'dp2_edits_v1470.json'), 'utf8'));
const HISTORY = require(path.join(ROOT, 'tests', 'js', 'dp2_history.js'));
const V1460_SHA256 = '12fde2222c46c7803d718b153d69f7f15a6468ff2df4bb045880c209d3b2fb63';   // v1.4.6 (origin/main cd69829)
const sha256 = s => crypto.createHash('sha256').update(s).digest('hex');

let passed = 0, failed = 0;
const digests = [];
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n  ')); }
}
function ok(c, m) { if (!c) throw new Error(m || 'assertion failed'); }
function eq(a, b, m) { if (a !== b) throw new Error((m || 'not equal') + ': ' + JSON.stringify(a) + ' !== ' + JSON.stringify(b)); }
function deq(a, b, m) { const ja = JSON.stringify(a), jb = JSON.stringify(b); if (ja !== jb) throw new Error((m || 'deep-equal') + ': ' + ja.slice(0, 300) + ' | ' + jb.slice(0, 300)); }
function near(a, b, tol, m) { if (!(Math.abs(a - b) <= tol)) throw new Error((m || 'not near') + ': ' + a + ' vs ' + b); }

function OLD_() {
  let s = NEW;
  for (const [cur, prev] of EDITS) { eq(s.split(cur).length - 1, 1, 'exactly one approved v1.4.7 hunk: ' + cur.split('\n')[0].slice(0, 80)); s = s.replace(cur, () => prev); }
  return s;
}
const OLD = OLD_();

/* A context with the Apps Script services the pure paths and the property reads touch. */
function load(src, props, sheets) {
  const ctx = {
    console, Math, Date, JSON, Object, Array, String, Number, RegExp, Error, isNaN, isFinite, parseFloat, parseInt,
    PropertiesService: { getScriptProperties: () => ({ getProperty: k => (props && Object.prototype.hasOwnProperty.call(props, k)) ? props[k] : null }) },
    SpreadsheetApp: (function () { const ss = { getSheetByName: n => (sheets && sheets[n]) || null, getSpreadsheetTimeZone: () => 'Asia/Riyadh' };
      return { getActive: () => ss, getActiveSpreadsheet: () => ss }; })(),
    Utilities: { formatDate: (d, tz, f) => new Date(d.getTime() + 3 * 3600000).toISOString().slice(0, 10) },
    Session: { getScriptTimeZone: () => 'Asia/Riyadh' }
  };
  vm.createContext(ctx);
  vm.runInContext(src, ctx, { filename: '27_Decision_Pages_v2.gs' });
  return ctx;
}
function sheetOf(vals) {
  return { getLastRow: () => vals.length, getLastColumn: () => Math.max.apply(null, vals.map(r => r.length)),
           getRange: (r, c, nr, nc) => ({ getValues: () => vals.slice(r - 1, r - 1 + nr).map(row => { const o = row.slice(c - 1, c - 1 + nc); while (o.length < nc) o.push(''); return o; }) }) };
}
const LOG_HDR = ['Logged At', 'Type', 'Symbol', 'Lot Buy Date', 'Ex-Date', 'Amount/Share', 'Shares', 'Amount', 'Ccy', 'Note'];
const LOG = [LOG_HDR,
  ['46330.32', 'AUTO', 'SUPN.US', '2026-11-02', '2026-11-03', 8.13, 95, 772.35, 'USD', 'yahoo_chart events=div'],
  ['46340.28', 'AUTO', 'STNG.US', '2026-11-12', '2026-11-13', 0.75, 25, 18.75, 'USD', 'yahoo_chart events=div'],
  ['46343.44', 'MANUAL', 'SUPN.US', '2026-11-02', '2026-11-03', -2.439, 95, -231.71, 'USD', 'US withholding tax 30%'],
  ['46273.04', 'MANUAL', '5023.SR', '2026-01-01', '2026-08-31', 6.375, 100, 637.5, 'SAR', 'operator entry'],
  ['46273.05', 'MANUAL', '5023.SR', '2025-11-23', '2026-08-31', 6.375, 100, 637.5, 'SAR', 'operator entry'],
  ['46213.10', 'AUTO', '7030.SR', '2026-01-01', '2026-07-23', 0.5, 178, 89, '', 'yahoo_chart events=div']];

/* SUPN-shaped cached history: no chart events (xb.divs absent). 40 bars of ~36 before the entry (2 Nov), the ex-day
 * open at about 28-30 (8.13 special), then a quiet drift to the last close 30.24. */
function supnBars() {
  const bars = []; const d0 = Date.UTC(2026, 8, 20);
  let day = 0;
  const push = (c, w) => { const d = new Date(d0 + day * 86400000).toISOString().slice(0, 10); day++; bars.push({ date: d, o: c, h: c + w, l: c - w, c: c }); };
  while (bars.length < 43) push(36 + 0.25 * Math.sin(bars.length), 0.6);   // to 2026-11-01
  push(36.28, 0.6);                                                          // 2026-11-02 the entry day (cum dividend)
  const after = [28.15, 28.6, 29.1, 29.4, 29.0, 29.6, 29.9, 30.2, 30.0, 30.4, 30.1, 29.8, 30.0, 30.3, 30.1, 30.5, 30.2, 30.1, 30.0, 30.24, 30.1, 29.9, 30.24];
  for (const c of after) push(c, 0.55);
  return bars;
}

function checkOracle() {
  eq(sha256(OLD), V1460_SHA256, 'v1.4.6 SHA-256');
  eq(HISTORY.V1460_SHA256, V1460_SHA256, 'history pin');
  eq(sha256(HISTORY.v1460Source(NEW)), V1460_SHA256, 'history chain');
  eq(sha256(HISTORY.v1450Source(NEW)), HISTORY.V1450_SHA256, 'the older chain still lands on v1.4.5');
  eq(HISTORY.v1460Source(OLD), OLD, 'a source that already is the v1.4.6 file is returned unchanged');
  eq(EDITS.length, 8, 'eight hunks');
  ok(/ \* Version: 1\.4\.7 \(2026-11-25\)/.test(NEW) && /var DP2_VERSION = '1\.4\.7';/.test(NEW), 'version lockstep');
  ok(NEW.indexOf(' *   1.4.7: [DIV-LOG]') > 0 && NEW.indexOf(' *   1.4.6: [RETURN-COL]') > NEW.indexOf(' *   1.4.7: [DIV-LOG]'), 'the new WHY block is first and the 1.4.6 one is kept');
  const fns = s => new Set((s.match(/^function ([A-Za-z0-9_]+)\(/mg) || []).map(x => x.slice(9, -1)));
  const a = fns(OLD), b = fns(NEW);
  deq([...b].filter(x => !a.has(x)).sort(), ['dp2DivLogFromValues_', 'dp2DivLogOn_', 'dp2DivLog_', 'dp2V147CoreOk_'], '+4 functions');
  deq([...a].filter(x => !b.has(x)), [], '0 removed');
  for (const [cur] of EDITS) {
    for (const line of cur.split('\n')) {
      ok(![...line].some(ch => ch.charCodeAt(0) > 127), 'ASCII only: ' + line.slice(0, 60));
      ok(!/\b(let|const)\s|=>|`/.test(line.replace(/'[^']*'/g, "''")), 'ES5 only: ' + line.slice(0, 80));
    }
  }
  load(NEW, {}, {}); load(OLD, {}, {});
}

function divsCases() {
  const xb = (divs, ccy) => ({ divs: divs, ccy: ccy });
  return [
    [xb(undefined, 'USD'), 'SUPN.US', {}, '2026-11-25'],
    [xb([{ date: '2026-11-03', amount: 8.13, source: 'chart' }], 'USD'), 'SUPN.US', {}, '2026-11-25'],
    [xb([], 'USD'), 'STNG.US', { 'STNG.US': { date: '2026-11-13', amount: 0.75, currency: 'USD' } }, '2026-11-25'],
    [xb([], 'SAR'), '5023.SR', {}, '2026-11-25'],
    [xb([], 'GBp'), 'X.L', {}, '2026-11-25'],
    [null, 'SUPN.US', null, ''],
    [xb([], 'USD'), 'NONE.US', {}, '2026-11-25']
  ];
}

function checkGoldenNegative() {
  const old = load(OLD, {}, { _Dividend_Log: sheetOf(LOG) });
  for (const off of ['0', 'off', 'OFF', 'false', ' no ']) {
    const c = load(NEW, { DP2_DIV_LOG: off }, { _Dividend_Log: sheetOf(LOG) });
    deq(JSON.parse(JSON.stringify(c.dp2DivLog_())), {}, 'gate off reads nothing: ' + off);
    for (const [x, sym, tab, today] of divsCases()) {
      deq(c.dp2ExitDivsFor_(x, sym, tab, today, c.dp2DivLog_()), old.dp2ExitDivsFor_(x, sym, tab, today), 'off = v1.4.6: ' + sym);
    }
  }
  const on = load(NEW, {}, { _Dividend_Log: sheetOf(LOG) });
  for (const [x, sym, tab, today] of divsCases()) {
    deq(on.dp2ExitDivsFor_(x, sym, tab, today), old.dp2ExitDivsFor_(x, sym, tab, today), 'no log argument = v1.4.6: ' + sym);
    deq(on.dp2ExitDivsFor_(x, sym, tab, today, {}), old.dp2ExitDivsFor_(x, sym, tab, today), 'an empty log = v1.4.6: ' + sym);
  }
  deq(on.dp2ExitDivsFor_({ divs: [], ccy: 'USD' }, 'NONE.US', {}, '2026-11-25', on.dp2DivLog_()), [], 'no row for the symbol');
  eq(Object.keys(on.dp2DivLog_()).length, 4, 'gate on: the log tab is read (four symbols)');
}

function checkSupn() {
  const bars = supnBars(), entry = 35.45, entryDate = '2026-11-02', today = '2026-11-25';
  const old = load(OLD, {}, { _Dividend_Log: sheetOf(LOG) });
  const nw = load(NEW, {}, { _Dividend_Log: sheetOf(LOG) });
  const xb = { bars: bars, ccy: 'USD' };                       // cached: no divs
  const run = (c, withLog) => {
    const tax = c.dp2DivTax_()['SUPN.US'];
    const divs = withLog ? c.dp2ExitDivsFor_(xb, 'SUPN.US', {}, today, c.dp2DivLog_()) : c.dp2ExitDivsFor_(xb, 'SUPN.US', {}, today);
    const adj = c.dp2ExitDivAdjust_(bars, entry, entryDate, c.dp2ExitDivsNet_(divs, tax, entryDate), today);
    const x = adj.applied.length ? c.dp2ExitCompute_(adj.bars, adj.entry, entryDate, null, '') : c.dp2ExitCompute_(bars, entry, entryDate, null, '');
    return { adj: adj, x: x, sig: c.dp2ExitSignal_(x, { tp: 45, stop: 26.40 }, null, '') };
  };
  const o = run(old, false), n = run(nw, true);
  eq(o.adj.applied.length, 0, 'v1.4.6 finds no dividend on cached bars');
  eq(o.sig.code, 'STOP_HIT', 'v1.4.6 reads the ex-dividend gap as a stop hit');
  eq(n.adj.applied.length, 1, 'v1.4.7 applies the ledger dividend');
  const NET = 8.13 * (772.35 - 231.71) / 772.35;   // the kept share of the ledger's own rows (1.4.5 rule)
  near(n.adj.applied[0].amount, NET, 1e-9, 'net of the 30% tax row (1.4.5 rule)');
  eq(n.adj.applied[0].source, 'dividend log', 'named source');
  near(n.adj.entry, 35.45 * (1 - NET / 36.28), 1e-9, 'entry in today\'s terms');
  near(n.adj.entry, 29.89, 0.01, 'the Exit Plan\'s own entry (29.87) within rounding');
  ok(n.x && n.x.stop < n.x.last, 'the stop sits under the last price');
  ok(n.sig.code !== 'STOP_HIT' && n.sig.code !== 'SELL_NOW', 'no SELL NOW: ' + n.sig.code);
  // the same engine on a chart that DOES carry the event is not changed by the log (no double count)
  const xc = { bars: bars, ccy: 'USD', divs: [{ date: '2026-11-03', amount: 8.13, source: 'chart' }] };
  deq(nw.dp2ExitDivsFor_(xc, 'SUPN.US', {}, today, nw.dp2DivLog_()), old.dp2ExitDivsFor_(xc, 'SUPN.US', {}, today), 'chart event wins');
}

function checkPure() {
  const c = load(NEW, {}, {});
  const log = JSON.parse(JSON.stringify(c.dp2DivLogFromValues_(LOG)));
  deq(Object.keys(log).sort(), ['5023.SR', '7030.SR', 'STNG.US', 'SUPN.US'], 'symbols with a positive row');
  deq(log['SUPN.US'], [{ date: '2026-11-03', amount: 8.13, currency: 'USD' }], 'the tax row is not a dividend');
  eq(log['5023.SR'].length, 1, 'one entry per symbol and ex-date (two lots)');
  eq(log['7030.SR'][0].currency, '', 'a blank currency stays blank');
  const taxFirst = JSON.parse(JSON.stringify(c.dp2DivLogFromValues_([LOG_HDR, ['', 'MANUAL', 'AAA.US', '', '2026-03-10', -0.6, 10, -6, 'USD'],
    ['', 'AUTO', 'AAA.US', '', '2026-03-10', 2, 10, 20, 'USD'], ['', 'MANUAL', 'BBB.US', '', '2026-03-10', -0.6, 10, -6, 'USD']])));
  deq(taxFirst, { 'AAA.US': [{ date: '2026-03-10', amount: 2, currency: 'USD' }] }, 'a tax row listed first is still not the dividend');
  deq(JSON.parse(JSON.stringify(c.dp2DivLogFromValues_([['a', 'b'], [1, 2]]))), {}, 'no header -> {}');
  deq(JSON.parse(JSON.stringify(c.dp2DivLogFromValues_(null))), {}, 'null -> {}');
  const L = c.dp2DivLogFromValues_(LOG);
  eq(c.dp2ExitDivsFor_({ divs: [], ccy: 'USD' }, 'SUPN.US', {}, '2026-11-02', L).length, 0, 'not arrived yet');
  eq(c.dp2ExitDivsFor_({ divs: [], ccy: 'USD' }, 'SUPN.US', {}, '2026-11-03', L).length, 1, 'arrived on the ex-date');
  eq(c.dp2ExitDivsFor_({ divs: [], ccy: 'SAR' }, 'SUPN.US', {}, '2026-11-25', L).length, 0, 'another currency');
  eq(c.dp2ExitDivsFor_({ divs: [], ccy: 'SAR' }, '7030.SR', {}, '2026-11-25', L).length, 0, 'a blank log currency is never guessed');
  eq(c.dp2ExitDivsFor_({ divs: [], ccy: 'SAR' }, '5023.SR', {}, '2026-11-25', L).length, 1, 'SAR on SAR');
  eq(c.dp2ExitDivsFor_({ divs: [], ccy: 'USD' }, 'supn.us', {}, '2026-11-25', L).length, 1, 'symbol case');
  const tabbed = c.dp2ExitDivsFor_({ divs: [], ccy: 'USD' }, 'STNG.US', { 'STNG.US': { date: '2026-11-14', amount: 0.75, currency: 'USD' } }, '2026-11-25', L);
  eq(tabbed.length, 1, 'a Dividend_Events date within 3 days is the same dividend'); eq(tabbed[0].source, 'Dividend_Events', 'the tab keeps its place');
  ok(c.dp2V147CoreOk_() === true, 'self-check core');
  const st = load(NEW, {}, {});
  const src = 'v1.4.7 core: the exit engine reads the dividend log (DP2_DIV_LOG: ';
  ok(NEW.indexOf("checks.push({ name: '" + src + "' + (dp2DivLogOn_() ? 'on' : 'off') + ')', ok: dp2V147CoreOk_() });") > 0, 'self-test line');
  eq(load(NEW, { DP2_DIV_LOG: 'off' }, {}).dp2DivLogOn_(), false, 'off'); eq(st.dp2DivLogOn_(), true, 'default on');
  eq(load(NEW, { DP2_DIV_LOG: 'yes' }, {}).dp2DivLogOn_(), true, 'any other word = on');
}

function checkMutants() {
  const muts = [
    ["e.date > today", "false"],
    ["e.currency !== ccy.toUpperCase()", "false"],
    ["if (!dup) out.push({ date: e.date, amount: e.amount, source: 'dividend log' });", "out.push({ date: e.date, amount: e.amount, source: 'dividend log' });"],
    ["if (!sym || !date || amt === null || !(amt > 0)) continue;", "if (!sym || !date || amt === null) continue;"],
    ["if (Object.prototype.hasOwnProperty.call(seen, k)) continue;", ""]
  ];
  for (const [from, to] of muts) {
    ok(NEW.split(from).length === 2, 'mutant site unique: ' + from);
    const M = NEW.replace(from, to);
    let caught = false;
    const save = NEW_SRC.v; NEW_SRC.v = M;
    try { checkPureOn(M); checkSupnOn(M); } catch (e) { caught = true; }
    NEW_SRC.v = save;
    ok(caught, 'mutant survived: ' + from);
  }
}
const NEW_SRC = { v: NEW };
function checkPureOn(src) { const keep = global.__src; global.__src = src; try { withSource(src, checkPure); } finally { global.__src = keep; } }
function checkSupnOn(src) { withSource(src, checkSupn); }
function withSource(src, fn) {
  const real = load;
  const saved = loadNewSrc.v; loadNewSrc.v = src;
  try { fn(); } finally { loadNewSrc.v = saved; }
}
const loadNewSrc = { v: null };
const _load = load;
// route every load(NEW, ...) through the active mutant when one is set
load = function (src, props, sheets) { return _load(src === NEW && loadNewSrc.v ? loadNewSrc.v : src, props, sheets); };   // eslint-disable-line no-func-assign

for (let round = 0; round < 3; round++) {
  passed = 0; failed = 0;
  const out = [];
  const log = console.log; console.log = m => out.push(m);
  test('R0 oracle: reversal = v1.4.6 (SHA-256 pinned, history chain); versions; both compile; +4 functions, 0 removed; ES5 / ASCII hunks', checkOracle);
  test('R1 GOLDEN NEGATIVE: DP2_DIV_LOG off (five spellings) and every no-log call equal the v1.4.6 dividend sources', checkGoldenNegative);
  test('R2 the SUPN shape on cached bars: v1.4.6 STOP_HIT; v1.4.7 entry 29.89 net of tax, stop under the last, no double count', checkSupn);
  test('R3 the pure parser, the source rules, the gate spellings and the self-test line', checkPure);
  test('R4 mutants: five guards removed, each caught', checkMutants);
  console.log = log;
  digests.push(sha256(out.join('\n')));
  if (round === 2) out.forEach(m => console.log(m));
}
if (new Set(digests).size !== 1) { console.log('FAIL the three rounds differ'); process.exit(1); }
console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
