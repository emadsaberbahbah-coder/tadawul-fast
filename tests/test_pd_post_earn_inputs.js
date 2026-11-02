#!/usr/bin/env node
/* tests/test_pd_post_earn_inputs.js
 * Harness for apps_script/10_My_Portfolio.gs v1.5.17 [D1b POST-EARNINGS INPUTS].
 * Extracts the REAL functions from the delivered file, and the REAL Top-10
 * post-earnings reader from apps_script/16_Decision_Top10.gs, and runs them in
 * node with PropertiesService / Spreadsheet stubs. All data is synthetic.
 *   E1 core: a Calendar_Events grid -> ok, held-only map, Top-10 sessions
 *   E2 unavailable: parser missing, no grid, no header, parse error
 *   E3 stale: newest update older than 7 days, or no readable update day
 *   E4 Date cells and text cells give the same object
 *   E5 pdPostEarnBody_: missing sheet, oversized sheet, read error, live read
 *   E6 mode: off by default, 'on' only (any case); other words are off
 *   E7 pdMetaLine_: no guard blocks -> the v1.5.16 line; blocks -> tokens
 *   E8 wiring: one gated request assignment; self-test line
 *   E9 ES5/ASCII lint + inventory + version lockstep
 *   E10 golden negative (PD_BASE=<v1.5.16 file>): the base has none of it
 * Run: node tests/test_pd_post_earn_inputs.js   (exit 0 = all PASS)
 */
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const SRC = process.argv[2] || path.join(__dirname, '..', 'apps_script', '10_My_Portfolio.gs');
const TOP10 = path.join(__dirname, '..', 'apps_script', '16_Decision_Top10.gs');
const src = fs.readFileSync(SRC, 'utf8');
const top10 = fs.readFileSync(TOP10, 'utf8');

function extract(source, name) {
  const i = source.indexOf('function ' + name + '(');
  if (i < 0) throw new Error('missing ' + name);
  let d = 0;
  const j = source.indexOf('{', i);
  for (let k = j; k < source.length; k++) {
    if (source[k] === '{') d++;
    else if (source[k] === '}') { d--; if (!d) return source.slice(i, k + 1); }
  }
  throw new Error('unbalanced ' + name);
}
function extractVar(source, name) {
  const m = source.match(new RegExp('^var ' + name + ' = [^;]+;', 'm'));
  if (!m) throw new Error('missing var ' + name);
  return m[0];
}

// ------------------------------------------------------------------ world
const W = { script: {} };
global.PropertiesService = {
  getScriptProperties: () => ({ getProperty: (k) => (Object.prototype.hasOwnProperty.call(W.script, k) ? W.script[k] : null) })
};
const geval = eval;
for (const v of ['DT10_POST_EARN_SESSIONS', 'DT10_POST_EARN_SESSIONS_NEG', 'DT10_POST_EARN_MAX_DAYS', 'DT10_CALENDAR_MAX_ROWS']) {
  geval(extractVar(top10, v));
}
for (const f of ['dt10NormToken_', 'dt10NormSym_', 'dt10CalendarDay_', 'dt10WeekdaysBetween_', 'dt10PostEarnMapFromValues_']) {
  geval(extract(top10, f));
}
const VARS = ['PD_POST_EARN_PROP', 'PD_POST_EARN_SCHEMA', 'PD_POST_EARN_STALE_DAYS'];
const FUNCS = ['pdPostEarnMode_', 'pdPostEarnRiyadhDay_', 'pdPostEarnCore_', 'pdPostEarnBody_'];
for (const v of VARS) geval(extractVar(src, v));
for (const f of FUNCS) geval(extract(src, f));
geval(extract(src, 'pdCell_'));
geval(extract(src, 'pdMetaLine_'));

let pass = 0;
function ok(cond, label) { if (!cond) { console.error('FAIL ' + label); process.exit(1); } pass++; }
function deep(a, b) { return JSON.stringify(a) === JSON.stringify(b); }

// 2026-10-28 08:46 Riyadh (05:46Z), a Wednesday.
const NOW = new Date('2026-10-28T05:46:00Z');
const HEADER = ['Symbol', 'Next Earnings Date', 'Days To Earnings', 'Next Ex-Div Date', 'Days To ExDiv',
  'Updated At (Riyadh)', 'Source', 'Last Earnings Date', 'Days Since Earnings', 'EPS Surprise %'];
function grid(updated) {
  const u = updated || '2026-10-27 18:28';
  return [HEADER,
    ['FRST.US', '2027-01-28', 93, '2026-11-06', 10, u, 'eodhd', '2026-10-22', 5, -22.2],
    ['WPC.US', '2026-10-27', 0, '', '', u, 'eodhd', '', '', ''],
    ['UMBF.US', '2026-10-27', 0, '', '', u, 'eodhd', '', '', ''],
    ['OLD.US', '2027-02-01', 96, '', '', u, 'eodhd', '2026-08-01', 60, 3.1],
    ['CRC.US', '2026-11-02', 6, '', '', updated ? u : '2026-10-27 13:31', 'eodhd', '', '', ''],
    ['2222.SR', '2026-11-04', 8, '', '', u, 'eodhd', '', '', '']];
}

// ---------------------------------------------------------------- E1
let r = pdPostEarnCore_(grid(), ['FRST.US', ' wpc.us ', '5023.SR', 'CRC.US'], NOW);
ok(r.schema === 'tfb.pd-post-earnings/v1' && r.status === 'ok' && r.asof === '2026-10-28', 'E1 ok, schema, Riyadh asof');
ok(r.updated === '2026-10-27' && r.reports === 3, 'E1 newest update day, recent reports (FRST, WPC, UMBF)');
ok(deep(Object.keys(r.map).sort(), ['FRST.US', 'WPC.US']), 'E1 held symbols only, normalized');
ok(deep(r.map['FRST.US'], { date: '2026-10-22', sessions: 4, surprise: -22.2 }), 'E1 FRST: 4 sessions since 22 Oct, EPS -22.2');
ok(deep(r.map['WPC.US'], { date: '2026-10-27', sessions: 1, surprise: null }), 'E1 a past Next Earnings Date is a report, surprise unknown');
const t10 = dt10PostEarnMapFromValues_(grid(), NOW);
ok(deep(r.map['FRST.US'], t10['FRST.US']) && deep(r.map['WPC.US'], t10['WPC.US']), 'E1 identical to the Top-10 reader');
ok(!('reason' in r), 'E1 no reason on ok');
console.log('E1 PASS core');

// ---------------------------------------------------------------- E1b report day (Codex P1, PR #869)
const sameDay = grid().concat([
  ['TODAY.US', '2027-01-20', 84, '', '', '2026-10-28 02:10', 'eodhd', '2026-10-28', 0, '-5.0%'],
  ['RELEASE.US', '2026-10-28', 0, '', '', '2026-10-28 02:10', 'eodhd', '', '', ''],
  ['TOMORROW.US', '2026-10-29', 1, '', '', '2026-10-28 02:10', 'eodhd', '', '', ''],
  ['NOTHELD.US', '2026-10-28', 0, '', '', '2026-10-28 02:10', 'eodhd', '', '', '']]);
r = pdPostEarnCore_(sameDay, ['FRST.US', 'TODAY.US', 'RELEASE.US', 'TOMORROW.US'], NOW);
ok(r.status === 'ok' && r.reports === 3, 'E1b ok; the Top-10 reader still counts past reports only');
ok(deep(r.map['TODAY.US'], { date: '2026-10-28', sessions: 0, surprise: -5 }), 'E1b reported today: 0 sessions, surprise parsed');
ok(deep(r.map['RELEASE.US'], { date: '2026-10-28', sessions: 0, surprise: null }), 'E1b reports today (Next Earnings Date): 0 sessions, surprise unknown');
ok(!('TOMORROW.US' in r.map) && !('NOTHELD.US' in r.map), 'E1b a later date and a name not held stay out');
ok(deep(r.map['FRST.US'], { date: '2026-10-22', sessions: 4, surprise: -22.2 }), 'E1b past reports unchanged');
ok(!('TODAY.US' in dt10PostEarnMapFromValues_(sameDay, NOW)), 'E1b the Top-10 reader itself is untouched (strictly past)');
const sameDayDates = sameDay.map((row, i) => {
  if (!i) return row;
  const out = row.slice();
  for (const c of [1, 7]) if (/^\d{4}-\d{2}-\d{2}$/.test(String(row[c]))) out[c] = new Date(Date.parse(row[c] + 'T00:00:00.000Z') - 3 * 3600 * 1000);
  return out;
});
ok(deep(pdPostEarnCore_(sameDayDates, ['TODAY.US', 'RELEASE.US'], NOW).map,
        pdPostEarnCore_(sameDay, ['TODAY.US', 'RELEASE.US'], NOW).map),
   'E1b Date cells give the same report-day entries');
const lateNow = new Date('2026-10-28T20:59:00Z');   // 23:59 Riyadh, still 28 Oct
ok(pdPostEarnCore_(sameDay, ['TODAY.US'], lateNow).map['TODAY.US'].sessions === 0, 'E1b 23:59 Riyadh is still the report day');
const nextNow = new Date('2026-10-28T21:00:00Z');   // 00:00 Riyadh, 29 Oct
ok(pdPostEarnCore_(sameDay, ['TODAY.US'], nextNow).map['TODAY.US'].sessions === 1, 'E1b the next Riyadh day counts one session (Top-10 reader)');
console.log('E1b PASS report day');

// ---------------------------------------------------------------- E2
const saved = global.dt10PostEarnMapFromValues_;
global.dt10PostEarnMapFromValues_ = undefined;
r = pdPostEarnCore_(grid(), ['FRST.US'], NOW);
ok(r.status === 'unavailable' && /parser missing/.test(r.reason) && deep(r.map, {}), 'E2 parser missing');
global.dt10PostEarnMapFromValues_ = saved;
r = pdPostEarnCore_(null, ['FRST.US'], NOW);
ok(r.status === 'unavailable' && /not readable/.test(r.reason), 'E2 no grid');
r = pdPostEarnCore_([['Ticker', 'When'], ['FRST.US', '2026-10-22']], ['FRST.US'], NOW);
ok(r.status === 'unavailable' && /header not found/.test(r.reason), 'E2 header missing');
global.dt10PostEarnMapFromValues_ = () => { throw new Error('boom'); };
r = pdPostEarnCore_(grid(), ['FRST.US'], NOW);
ok(r.status === 'unavailable' && /parse failed/.test(r.reason) && deep(r.map, {}), 'E2 parser throws');
global.dt10PostEarnMapFromValues_ = saved;
console.log('E2 PASS unavailable');

// ---------------------------------------------------------------- E3
r = pdPostEarnCore_(grid('2026-10-20 09:00'), ['FRST.US'], NOW);
ok(r.status === 'stale' && r.updated === '2026-10-20' && /newest calendar update 2026-10-20/.test(r.reason), 'E3 8 days old');
ok(r.map['FRST.US'] && r.map['FRST.US'].sessions === 4, 'E3 map still reported for diagnostics');
r = pdPostEarnCore_(grid('2026-10-21 23:59'), ['FRST.US'], NOW);
ok(r.status === 'ok', 'E3 7 days is still fresh');
const noDays = grid('n/a');
noDays[5][5] = 'n/a';
r = pdPostEarnCore_(noDays, ['FRST.US'], NOW);
ok(r.status === 'stale' && /no Updated At day/.test(r.reason), 'E3 no readable update day');
const noCol = grid().map((row) => row.filter((_, i) => i !== 5));
r = pdPostEarnCore_(noCol, ['FRST.US'], NOW);
ok(r.status === 'ok' && r.updated === '', 'E3 without the column the freshness check does not apply');
console.log('E3 PASS stale');

// ---------------------------------------------------------------- E4
const dated = grid().map((row, i) => {
  if (!i) return row;
  const out = row.slice();
  out[5] = new Date(Date.parse(String(row[5]).replace(' ', 'T') + ':00.000Z') - 3 * 3600 * 1000);
  for (const c of [1, 7]) if (/^\d{4}-\d{2}-\d{2}$/.test(String(row[c]))) out[c] = new Date(Date.parse(row[c] + 'T00:00:00.000Z') - 3 * 3600 * 1000);
  return out;
});
const a = pdPostEarnCore_(grid(), ['FRST.US', 'WPC.US'], NOW);
const b = pdPostEarnCore_(dated, ['FRST.US', 'WPC.US'], NOW);
ok(deep(a, b), 'E4 Date cells (Riyadh midnight / time) equal text cells');
ok(pdPostEarnRiyadhDay_(new Date('2026-10-27T21:00:00Z')) === pdPostEarnRiyadhDay_('2026-10-28'), 'E4 21:00Z is the next Riyadh day');
ok(pdPostEarnRiyadhDay_('2026-02-30') === null && pdPostEarnRiyadhDay_(7) === null && pdPostEarnRiyadhDay_(new Date('x')) === null, 'E4 invalid days');
console.log('E4 PASS cell types');

// ---------------------------------------------------------------- E5
function sheetSs(opts) {
  return {
    getSheetByName: (n) => {
      if (n !== 'Calendar_Events' || opts.missing) return null;
      return {
        getLastRow: () => opts.rows,
        getLastColumn: () => opts.cols,
        getRange: (r1, c1, nr, nc) => {
          if (opts.rangeThrows) throw new Error('quota');
          ok(r1 === 1 && c1 === 1 && nr === opts.rows && nc === Math.min(opts.cols, 16), 'E5 bounded read');
          return { getValues: () => opts.values };
        }
      };
    }
  };
}
const rows = [{ Symbol: 'FRST.US', Quantity: 174 }, { Symbol: '5023.SR', Quantity: 100 }];
r = pdPostEarnBody_(sheetSs({ missing: true }), rows);
ok(r.status === 'unavailable' && /not readable/.test(r.reason), 'E5 missing sheet');
r = pdPostEarnBody_(sheetSs({ rows: 5002, cols: 10, values: grid() }), rows);
ok(r.status === 'unavailable', 'E5 over the Top-10 row bound');
r = pdPostEarnBody_(sheetSs({ rows: 7, cols: 10, rangeThrows: true }), rows);
ok(r.status === 'unavailable', 'E5 read error');
r = pdPostEarnBody_({ getSheetByName: () => { throw new Error('auth'); } }, rows);
ok(r.status === 'unavailable', 'E5 getSheetByName throws');
const realDate = global.Date;
global.Date = class extends realDate { constructor(...args) { if (args.length) super(...args); else super(NOW.getTime()); } static now() { return NOW.getTime(); } };
r = pdPostEarnBody_(sheetSs({ rows: 7, cols: 10, values: grid() }), rows);
global.Date = realDate;
ok(r.status === 'ok' && deep(Object.keys(r.map), ['FRST.US']) && r.map['FRST.US'].sessions === 4, 'E5 live read, held symbols from the rows');
r = pdPostEarnBody_(sheetSs({ rows: 7, cols: 10, values: grid() }), null);
ok(r.status === 'ok' && deep(r.map, {}), 'E5 no rows -> empty map');
console.log('E5 PASS live read');

// ---------------------------------------------------------------- E6
W.script = {}; ok(pdPostEarnMode_() === 'off', 'E6 absent -> off');
W.script.PD_POST_EARN = ' On '; ok(pdPostEarnMode_() === 'on', 'E6 on');
for (const w of ['enforce', 'observe', '1', 'true', '']) { W.script.PD_POST_EARN = w; ok(pdPostEarnMode_() === 'off', 'E6 ' + w + ' -> off'); }
console.log('E6 PASS mode');

// ---------------------------------------------------------------- E7
const meta = { route: { version: '4.19.0', holdings: { count: 2 }, duration_ms: 812.4, request_id: 'abc' },
  versions: { portfolio_actions: '1.17.0', opportunity_builder: '1.28.0' }, cash_floor_sar: 8243 };
const plain = 'route v4.19.0 | actions v1.17.0 | builder v1.28.0 | holdings=2 | cash_floor=8243 | 812ms | req abc';
ok(pdMetaLine_(meta) === plain, 'E7 no guard blocks -> the v1.5.16 line');
ok(pdMetaLine_(null) === 'route v | actions v | builder v', 'E7 null meta unchanged');
const armed = Object.assign({}, meta, {
  post_earn_guard: { mode: 'enforce', input_status: 'ok', held: 1, would_hold: 0 },
  ticket_cap: { mode: 'observe', cap: 1, open_tickets: 1, held: 0, would_hold: 1 } });
ok(pdMetaLine_(armed) === plain + ' | post-earn enforce (ok; held 1) | tickets 1/1 observe (held 0; would 1)', 'E7 tokens appended');
ok(pdMetaLine_(Object.assign({}, meta, { post_earn_guard: { mode: 'observe', input_status: 'absent', held: 0, would_hold: 2 } })) ===
   plain + ' | post-earn observe (absent; held 0; would 2)', 'E7 observe token');
console.log('E7 PASS meta line');

// ---------------------------------------------------------------- E8
const refresh = extract(src, 'refreshPortfolioDecision');
const line = "  if (pdPostEarnMode_() === 'on') body.post_earnings = pdPostEarnBody_(ss, pool.rows);";
ok(refresh.split(line).length === 2, 'E8 one gated assignment');
ok(refresh.indexOf('if (evidence) body.reconciliation_evidence = evidence;') < refresh.indexOf(line) &&
   refresh.indexOf(line) < refresh.indexOf('pdIbkrApply_(ibkrMode, body)') &&
   refresh.indexOf(line) < refresh.indexOf('resp = pdPost_(body);'), 'E8 after the evidence line, before IBKR apply and the POST');
ok((src.match(/post_earnings/g) || []).length >= 1 && !/Logger\.log\([^)]*post_earnings/.test(src), 'E8 never logged');
const self = extract(src, 'pdSelfTest');
ok(self.indexOf("report.push('post-earnings inputs (PD_POST_EARN=' + pdPostEarnMode_() + '): ' + peNow.status +") > 0, 'E8 self-test line');
console.log('E8 PASS wiring');

// ---------------------------------------------------------------- E9
const i0 = src.indexOf('/* ----- v1.5.17 [D1b POST-EARN]');
const i1 = src.indexOf('function refreshPortfolioDecision() {', i0);
const block = src.slice(i0, i1);
ok(i0 > 0 && i1 > i0, 'E9 block located');
ok(!/\b(let|const)\s|=>|`|\.\.\.|class\s/.test(block), 'E9 ES5 only');
ok(![...block].some((ch) => ch.charCodeAt(0) > 126), 'E9 ASCII only');
for (const f of FUNCS) ok(block.indexOf('function ' + f + '(') >= 0, 'E9 inventory ' + f);
const h0 = src.indexOf(' * v1.5.17 (2026-10-28)');
const h1 = src.indexOf(' * v1.5.16 (2026-10-26)');
ok(h0 > 0 && h1 > h0 && ![...src.slice(h0, h1)].some((ch) => ch.charCodeAt(0) > 126), 'E9 WHY block ASCII, above v1.5.16');
// 2026-11-02: v1.5.18 (cash snapshot writer, last-known cash, follow-up retry) stacks on this change; the
// lockstep holds at the delivered file's own version, which must be 1.5.17 or later.
const pdv = src.match(/^var PD_VERSION = '(\d+)\.(\d+)\.(\d+)';$/m) || [];
ok(pdv.length === 4 && (src.match(/var PD_VERSION = '/g) || []).length === 1 &&
   new RegExp('^ \\* Version: ' + pdv[1] + '\\.' + pdv[2] + '\\.' + pdv[3] + '$', 'm').test(src) &&
   (+pdv[1]) * 1e6 + (+pdv[2]) * 1e3 + (+pdv[3]) >= 1005017, 'E9 version lockstep');
ok(src.indexOf(" * v1.5.16 (2026-10-26)  (EVIDENCE PUBLICATION") > 0 && src.indexOf(" * v1.5.15 (2026-10-24)  (PD-ALLCASH") > 0, 'E9 prior WHY blocks kept');
const metaFn = extract(src, 'pdMetaLine_');
ok(!/\b(let|const)\s|=>|`/.test(metaFn) && ![...metaFn].some((ch) => ch.charCodeAt(0) > 126), 'E9 pdMetaLine_ ES5/ASCII');
console.log('E9 PASS lint + inventory');

// ---------------------------------------------------------------- E10 golden negative
if (process.env.PD_BASE) {
  const base = fs.readFileSync(process.env.PD_BASE, 'utf8');
  const sha = crypto.createHash('sha256').update(base).digest('hex');
  let missing = 0;
  for (const f of FUNCS) { try { extract(base, f); } catch (e) { missing++; } }
  ok(missing === FUNCS.length && /var PD_VERSION = '1\.5\.16';/.test(base) && base.indexOf('post_earnings') < 0, 'E10 base lacks every added function and the field (sha256 ' + sha.slice(0, 12) + ')');
  geval(extract(base, 'pdMetaLine_'));
  ok(pdMetaLine_(meta) === plain && pdMetaLine_(armed) === plain, 'E10 the base line ignores the guard blocks (no read-back)');
  const names = (s) => (s.match(/^function\s+([A-Za-z_$][\w$]*)\s*\(/mg) || []).map((m) => m.replace(/^function\s+/, '').replace(/\s*\($/, ''));
  const nb = names(base), nn = new Set(names(src));
  ok(nb.every((n) => nn.has(n)) && nn.size === nb.length + FUNCS.length, 'E10 zero functions removed; exactly ' + FUNCS.length + ' added (' + nb.length + ' -> ' + nn.size + ')');
  console.log('E10 PASS golden negative on the v1.5.16 base ' + sha.slice(0, 12));
} else {
  console.log('E10 SKIP (set PD_BASE=<v1.5.16 file> to run the golden negative)');
}
console.log('ALL PASS (' + pass + ' checks) on ' + path.basename(SRC) + ' sha256 ' + crypto.createHash('sha256').update(src).digest('hex').slice(0, 12));
