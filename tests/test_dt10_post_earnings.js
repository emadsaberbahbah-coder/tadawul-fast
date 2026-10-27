#!/usr/bin/env node
/* tests/test_dt10_post_earnings.js
 * Harness for apps_script/16_Decision_Top10.gs v1.18.0 [POST-EARNINGS]
 * (program register A4 part 2; FRST post-mortem P1). Loads the REAL
 * delivered file -- and the v1.17.1 base rebuilt from it by reversing
 * tests/js/dt10_edits_v1180.json (SHA-256 pinned) -- into separate node vm
 * contexts with a cell-model spreadsheet (the dt10_bulk_format.test.js
 * model plus getLastRow / getLastColumn), a PropertiesService stub and a
 * clock frozen at Tue 27 Oct 2026 10:00 Riyadh. Sections:
 *   P1 kill identity: DT10_POST_EARN absent / off / blank / junk / enforce /
 *      service fault -> the REAL dt10RenderPayload_ paints the same sheet as
 *      the base (EXECUTABLE and NOT_ACTIONABLE feed), returns the same
 *      status token and makes no extra Calendar_Events read
 *   P2 armed (observe / on / ' On ' / 'OBSERVE'): exactly the Advisor Note
 *      cells of the seats inside their quarantine change -- the FRST case
 *      (3/10, EPS -22.2%), a blank surprise (5/5), a Riyadh-midnight Date
 *      cell (2/10), a seat carrying both tags (post first, then forward);
 *      the status token 'post-earn !4/9'; a withheld row keeps the prefixes
 *   P3 golden negative: the v1.17.1 base, armed, never tags (the FRST
 *      ticket of 26 Oct was bought two sessions after a 22% miss)
 *   P4 pure cores: weekday sessions, the calendar map (header detection,
 *      blanks, future / same-day reports, text and numeric surprises,
 *      duplicates), the tagger's 5 / 10 boundaries, idempotency, formatting
 *   P5 the token: no last-report rows / no tab / read fault -> 'post-earn
 *      n/a' and no tag; nothing in quarantine -> 'post-earn 0/9'
 *   P6 the embedded self-test line (off / observe / ON); the full
 *      dt10SelfTest is decision_input_truth.test.js T8
 *   P7 version lockstep, WHY block, ES5/ASCII lint of the diff, inventory,
 *      seven hunks whose reversal is the reviewed v1.17.1 file
 * Run: node tests/test_dt10_post_earnings.js   (exit 0 = all PASS)
 */
'use strict';
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const vm = require('vm');
const history = require('./js/dt10_cash_utc_history');

const SRC = process.argv[2] || path.join(__dirname, '..', 'apps_script', '16_Decision_Top10.gs');
const src = fs.readFileSync(SRC, 'utf8');
const base = history.v1171Source(src);
const NOW = Date.UTC(2026, 9, 27, 7, 0, 0);   // Tue 27 Oct 2026 10:00 Riyadh

let pass = 0;
function ok(cond, msg) {
  if (!cond) { console.log('FAIL ' + msg); process.exit(1); }
  pass++;
}
function deep(a, b) { return JSON.stringify(a) === JSON.stringify(b); }
const WARN = '\u26a0', DOT = '\u00b7', LE = '\u2264';

// ------------------------------------------------------------ cell model
// (the model of tests/js/dt10_bulk_format.test.js, plus getLastRow /
// getLastColumn for the Calendar_Events reads and a read counter per tab)
function makeBook(opts) {
  opts = opts || {};
  const counts = {};
  const sheets = {};
  const reads = {};
  const book = {
    counts, reads,
    bump(m) { counts[m] = (counts[m] || 0) + 1; },
    getSheetByName(n) {
      if (opts.throwOn === n) throw new Error('sheet service down');
      return sheets[n] || null;
    },
    addSheet(name, maxRows, maxCols) { sheets[name] = makeSheet(book, name, maxRows, maxCols); return sheets[name]; }
  };
  return book;
}
function makeSheet(book, name, maxRows, maxCols) {
  const cells = new Map();
  let merges = [];
  const borders = [];
  const key = (r, c) => r + ':' + c;
  const upd = (r, c, f) => { const k = key(r, c); const cur = Object.assign({}, cells.get(k) || {}); f(cur); cells.set(k, cur); };
  const each = (r, c, nr, nc, fn) => { for (let i = 0; i < nr; i++) for (let j = 0; j < nc; j++) fn(r + i, c + j, i, j); };
  const sheet = {
    _cells: cells, _borders: borders, get _merges() { return merges; },
    getName() { return name; }, getMaxRows() { return maxRows; }, getMaxColumns() { return maxCols; },
    getParent() { return book; },
    getLastRow() { let m = 0; for (const [k, x] of cells) if (x.v !== undefined && x.v !== '') m = Math.max(m, +k.split(':')[0]); return m; },
    getLastColumn() { let m = 0; for (const [k, x] of cells) if (x.v !== undefined && x.v !== '') m = Math.max(m, +k.split(':')[1]); return m; },
    insertColumnsAfter(_after, n) { book.bump('insertColumnsAfter'); maxCols += n; return sheet; },
    getRange(a, b, nr, nc) {
      const [r, c, h, w] = [a, b, nr || 1, nc || 1];
      assert(r >= 1 && c >= 1 && h >= 1 && w >= 1, 'range shape ' + [r, c, h, w]);
      return makeRange(r, c, h, w);
    }
  };
  function makeRange(r, c, nr, nc) {
    const self = {};
    const all = (field, m) => v => { book.bump(m); each(r, c, nr, nc, (R, C) => upd(R, C, x => { x[field] = v; })); return self; };
    const mat = (field, m) => grid => {
      book.bump(m);
      assert(Array.isArray(grid) && grid.length === nr && grid.every(row => Array.isArray(row) && row.length === nc),
        m + ' shape ' + nr + 'x' + nc);
      each(r, c, nr, nc, (R, C, i, j) => upd(R, C, x => { x[field] = grid[i][j]; }));
      return self;
    };
    const read = (field, dflt, m) => () => {
      book.bump(m);
      if (field === 'v') book.reads[name] = (book.reads[name] || []).concat([[r, c, nr, nc].join('x')]);
      return Array.from({length: nr}, (_, i) => Array.from({length: nc}, (_, j) => {
        const v = (cells.get(key(r + i, c + j)) || {})[field];
        return v === undefined || v === null ? dflt : v;
      }));
    };
    Object.assign(self, {
      setValue(v) { book.bump('setValue'); upd(r, c, x => { x.v = v; }); return self; },
      setValues: mat('v', 'setValues'), getValues: read('v', '', 'getValues'),
      setBackground: all('bg', 'setBackground'), setBackgrounds: mat('bg', 'setBackgrounds'),
      getBackgrounds: read('bg', '#ffffff', 'getBackgrounds'),
      setFontColor: all('fg', 'setFontColor'), setFontColors: mat('fg', 'setFontColors'),
      getFontColors: read('fg', '#000000', 'getFontColors'),
      setFontWeight: all('fw', 'setFontWeight'), setFontWeights: mat('fw', 'setFontWeights'),
      getFontWeights: read('fw', 'normal', 'getFontWeights'),
      setFontSize: all('fs', 'setFontSize'), setWrap: all('wrap', 'setWrap'),
      setFontStyle: all('fst', 'setFontStyle'), setNumberFormat: all('nf', 'setNumberFormat'),
      setBorder(...args) { book.bump('setBorder'); borders.push([r, c, nr, nc].concat(args.map(String)).join('|')); return self; },
      merge() { book.bump('merge'); merges.push([r, c, nr, nc].join('|')); return self; },
      breakApart() {
        book.bump('breakApart');
        merges = merges.filter(m => { const [mr, mc, mh, mw] = m.split('|').map(Number);
          return mr + mh - 1 < r || mr > r + nr - 1 || mc + mw - 1 < c || mc > c + nc - 1; });
        return self;
      },
      clear() { book.bump('clear'); each(r, c, nr, nc, (R, C) => cells.delete(key(R, C))); return self; }
    });
    return self;
  }
  return sheet;
}
function snapshot(sheet) {
  const out = {};
  for (const [k, x] of [...sheet._cells.entries()].sort()) out[k] = x;
  return JSON.parse(JSON.stringify({cells: out, merges: [...sheet._merges].sort(), borders: [...sheet._borders].sort()}));
}

// ------------------------------------------------------------ vm contexts
function fakeDate() {
  const D = Date;
  function FD(...args) {
    if (!(this instanceof FD)) return new D(NOW).toString();
    return args.length ? new D(...args) : new D(NOW);
  }
  FD.prototype = D.prototype;
  FD.now = () => NOW;
  FD.UTC = D.UTC;
  FD.parse = D.parse;
  return FD;
}
function load(source, props, opts) {
  opts = opts || {};
  const logs = [];
  const propReads = [];
  const ctx = {
    Date: fakeDate(),
    Logger: { log(m) { logs.push(String(m)); } },
    Session: { getScriptTimeZone: () => 'Asia/Riyadh' },
    Utilities: { formatDate(d) { return new Date(d).toISOString().replace('T', ' ').slice(0, 19); } },
    SpreadsheetApp: { BorderStyle: { SOLID: 'SOLID' }, flush() {}, getActiveSpreadsheet() { return null; },
                      getUi() { throw new Error('no ui'); } },
    PropertiesService: {
      getScriptProperties() {
        if (opts.propsThrow) throw new Error('property service down');
        return {
          getProperty(k) {
            propReads.push(k);
            return Object.prototype.hasOwnProperty.call(props || {}, k) ? props[k] : null;
          },
          setProperty() {}, deleteProperty() {}
        };
      }
    }
  };
  vm.createContext(ctx);
  vm.runInContext(source, ctx, { filename: '16_Decision_Top10.gs' });
  ctx.__logs = logs;
  ctx.__propReads = propReads;
  return ctx;
}

// ------------------------------------------------------------ fixtures
const CAL_HDR = ['Symbol', 'Next Earnings Date', 'Days To Earnings', 'Next Ex-Div Date', 'Days To ExDiv',
  'Updated At (Riyadh)', 'Source', 'Last Earnings Date', 'Days Since Earnings', 'EPS Surprise %'];
// A Sheets date cell for 23 Oct 2026 (midnight Riyadh = 22 Oct 21:00 UTC).
const RIYADH_23OCT = new Date(Date.UTC(2026, 9, 22, 21, 0, 0));
const CAL_ROWS = [
  ['SYM0.US', '2027-01-28', 93, '', '', '', 'eodhd', '2026-10-22', 5, -22.2],   // FRST: Thu after close, -22.2%
  ['SYM1.US', '', '', '', '', '', 'eodhd', '2026-10-13', 14, 3.1],              // 10 sessions, positive -> out
  ['SYM2.US', '', '', '', '', '', 'eodhd', '2026-10-20', 7, ''],                // 5 sessions, blank -> 5/5
  ['SYM3.US', '2026-11-01', 5, '', '', '', 'eodhd', '', '', ''],                // forward tag only
  ['SYM4.US', '2026-11-03', 7, '', '', '', 'eodhd', '2026-10-26', 1, 6.4],      // both tags
  ['SYM5.US', '', '', '', '', '', 'eodhd', '2026-10-27', 0, -3.0],              // today -> not a past report
  ['SYM6.US', '', '', '', '', '', 'eodhd', RIYADH_23OCT, 4, -1.9],             // a Sheets Date cell -> 2/10
  ['SYM8.US', '', '', '', '', '', 'eodhd', '2026-10-06', 21, -5],               // 15 sessions -> out
  ['C1.US', '', '', '', '', '', 'eodhd', '2026-10-26', 1, -9.9]                 // not on the board
];
function ticket(i) {
  const fast = i < 2;
  return {rank: i + 1, symbol: 'SYM' + i + '.US', name: 'Name ' + i, market: 'US', sector: 'Sector ' + (i % 3),
    currency: 'USD', fx_to_sar: 3.75, price: 10 + i, price_sar: (10 + i) * 3.75, entry_zone: '1-2',
    suggested_sar: fast ? '' : 0, suggested_shares: '', stop_sar: 30 + i, tp1_sar: 50 + i, tp2_sar: 60 + i,
    roi_pct: 0.1, engine_roi_pct: 0.2, ann_roi_pct: 0.3, exp_gain_12m_sar: 100, reliability: 80, dq: 95,
    confidence_band: 'HIGH', detail: {funds_from: 'Cash', review_date: '2026-10-29'},
    _stab_status: fast ? 'FAST-TRACK 1/3' : 'GRACE 1/2', _stab_days: 1, _stab_since: '2026-10-21',
    _stab_smoothed: 70, _stab_trend: 'flat', _grace_hold: !fast, advisor_note: 'note ' + i};
}
function cand(i) {
  const verdict = ['INVEST', 'WATCH', 'DO_NOT_INVEST', 'BLOCKED', ''][i % 5];
  return {symbol: 'C' + i + '.US', name: 'Cand ' + i, market: 'US', sector: 'S' + (i % 7), currency: 'USD',
    price: 1 + i, price_sar: (1 + i) * 3.75, roi_pct: 0.01 * (i % 30), engine_roi_pct: 0.02, ann_roi_pct: 0.03,
    rr: 1.5, reliability: 70, dq: 90, risk_level: 'Low', news_trend: '', sector_trend: '', conflict: i % 2 === 0,
    verdict, confidence_band: 'MID', opportunity_score: 1000 - i, first_fail: verdict === 'INVEST' ? null : {gate: 'Quote Freshness'},
    failure_reason: verdict === 'INVEST' ? '' : 'stale', structural_block: false, selected: i < 3, deferral: i === 5 ? 'sector cap' : ''};
}
function payload() {
  return {version: 'synthetic', status: 'ok', selected: Array.from({length: 9}, (_, i) => ticket(i)),
    candidates_rows: Array.from({length: 40}, (_, i) => cand(i)),
    near_miss: Array.from({length: 4}, (_, i) => ({symbol: 'N' + i, failed_gate: 'ROI', current: 1, required: 2,
      verdict: ['WATCH', 'DO_NOT_INVEST', 'INVEST', 'HOLD'][i], improve_note: 'n' + i})),
    alerts: [{type: 'T0', count: 0, required_action: 'act 0'}],
    kpis: {scanned: 9788, passed: 3, deployable_sar: 71374.54, max_selected: 10, selected_count: 9},
    meta: {route: 'v4.18.2', board_funding: {contract_version: 1, stage: 'research', finalized: true, eligible_symbols: []}}};
}
function render(source, props, state, opts) {
  opts = opts || {};
  const ctx = load(source, props, opts);
  const book = makeBook({ throwOn: opts.throwOn });
  const sheet = book.addSheet('Top_10_Investments', 400, 40);
  if (!opts.noCalendar) {
    const cal = book.addSheet('Calendar_Events', 100, 12);
    const rows = [CAL_HDR.slice(0, opts.narrowCalendar ? 7 : 10)].concat(
      (opts.calRows || CAL_ROWS).map(r => r.slice(0, opts.narrowCalendar ? 7 : 10)));
    cal.getRange(1, 1, rows.length, rows[0].length).setValues(rows);
    book.reads.Calendar_Events = [];
  }
  const uv = state === 'EXECUTABLE'
    ? {state: 'EXECUTABLE', reason: '', ageMin: 206, source_readiness: {ready: true, cohort: '39642923547'}}
    : {state: 'NOT_ACTIONABLE', reason: 'final_publication', ageMin: 30, source_readiness: {ready: true, cohort: '39642923547'}};
  ctx.dt10BoardVerdict_ = () => JSON.parse(JSON.stringify(uv));
  const p = payload();
  const earn = ctx.dt10RenderPayload_(sheet, p, ctx.DT10_FALLBACK_TOKENS);
  return { ctx, book, sheet, snap: snapshot(sheet), note: earn && earn.note, payload: p,
           calReads: (book.reads.Calendar_Events || []).slice() };
}
const NOTE_COL = 30;   // Advisor Note (board, 1-based) = DT10_UV_BOARD_NOTE_IDX + 1
function diffCells(a, b) {
  const keys = new Set(Object.keys(a.cells).concat(Object.keys(b.cells)));
  const out = [];
  for (const k of keys) if (!deep(a.cells[k], b.cells[k])) out.push(k);
  return out.sort();
}
function noteRowOf(snap, sym) {
  for (const k of Object.keys(snap.cells)) {
    const [r, c] = k.split(':').map(Number);
    if (c === 2 && snap.cells[k].v === sym) return r;
  }
  return -1;
}

// ------------------------------------------------------------ P1 kill identity
const OFF = [[{}, {}], [{ DT10_POST_EARN: 'off' }, {}], [{ DT10_POST_EARN: '' }, {}], [{ DT10_POST_EARN: 'yes' }, {}],
  [{ DT10_POST_EARN: '1' }, {}], [{ DT10_POST_EARN: 'onn' }, {}], [{ DT10_POST_EARN: 'enforce' }, {}],
  [{ DT10_POST_EARN: 'observe' }, { propsThrow: true }]];
for (const state of ['EXECUTABLE', 'NOT_ACTIONABLE']) {
  ok(/earn /.test(render(base, {}, state).note || ''), 'P1 base fixture carries the forward tag token');
  for (const [props, opts] of OFF) {
    const b = render(base, props, state, opts);   // the base under the same properties and faults
    const n = render(src, props, state, opts);
    const label = state + ' ' + JSON.stringify(props) + (opts.propsThrow ? ' (service fault)' : '');
    ok(deep(n.snap, b.snap), 'P1 identical sheet ' + label + ' (diff ' + diffCells(n.snap, b.snap).slice(0, 4).join(',') + ')');
    ok(n.note === b.note, 'P1 identical status token ' + label + ': ' + n.note);
    ok(deep(n.calReads, b.calReads), 'P1 no extra Calendar_Events read ' + label + ': ' + n.calReads.join(' '));
    ok(deep(n.payload.selected, b.payload.selected), 'P1 tickets untouched ' + label);
    ok(!n.ctx.__logs.some(l => /post-earnings/.test(l)), 'P1 no post-earnings log line ' + label);
  }
}
console.log('P1 PASS kill identity (absent / off / blank / junk / enforce / fault; both feed states)');

// ------------------------------------------------------------ P2 armed
const TAGS = {
  'SYM0.US': WARN + ' post-earnings 3/10 ' + DOT + ' EPS -22.2% ' + DOT + ' ',
  'SYM2.US': WARN + ' post-earnings 5/5 ' + DOT + ' EPS n/a ' + DOT + ' ',
  'SYM4.US': WARN + ' post-earnings 1/5 ' + DOT + ' EPS +6.4% ' + DOT + ' ',
  'SYM6.US': WARN + ' post-earnings 2/10 ' + DOT + ' EPS -1.9% ' + DOT + ' '
};
for (const mode of ['observe', 'on', ' On ', 'OBSERVE']) {
  for (const state of ['EXECUTABLE', 'NOT_ACTIONABLE']) {
    const b = render(base, { DT10_POST_EARN: mode }, state);
    const n = render(src, { DT10_POST_EARN: mode }, state);
    const label = state + ' ' + JSON.stringify(mode);
    const changed = diffCells(n.snap, b.snap);
    const want = Object.keys(TAGS).map(s => noteRowOf(b.snap, s) + ':' + NOTE_COL).sort();
    ok(want.every(k => !/^-1:/.test(k)) && deep(changed, want),
       'P2 exactly the four quarantined Advisor Note cells change ' + label + ': ' + changed.join(','));
    for (const sym of Object.keys(TAGS)) {
      const k = noteRowOf(b.snap, sym) + ':' + NOTE_COL;
      const was = String(b.snap.cells[k].v), now = String(n.snap.cells[k].v);
      if (state === 'EXECUTABLE') {
        ok(now === TAGS[sym] + was, 'P2 ' + sym + ' note = post-earnings prefix + the v1.17.1 note ' + label + ': ' + now);
      } else {
        ok(now === TAGS[sym] + was && /SIZING WITHHELD/.test(was),
           'P2 ' + sym + ' withheld row keeps the prefix(es) before the withhold text ' + label + ': ' + now);
      }
    }
    const k4 = noteRowOf(n.snap, 'SYM4.US') + ':' + NOTE_COL;
    ok(String(n.snap.cells[k4].v).indexOf(TAGS['SYM4.US'] + WARN + ' earnings ' + LE + '7d ' + DOT + ' ') === 0,
       'P2 both tags: post-earnings first, then the forward tag ' + label);
    ok(n.note === b.note + ' | post-earn ' + WARN + '4/9', 'P2 status token ' + label + ': ' + n.note);
    ok(n.calReads.length === b.calReads.length + 1 && n.calReads[n.calReads.length - 1] === '1x1x10x10',
       'P2 one extra Calendar_Events read (A1, all 10 columns) ' + label + ': ' + n.calReads.join(' '));
    ok(n.ctx.__logs.some(l => l === '[DT10 v1.18.0] post-earnings tags: 4/9 tickets (last reports 7; quarantine 5/10 sessions)'),
       'P2 Logger line ' + label);
  }
}
// a second annotation pass never doubles a tag
{
  const n = load(src, { DT10_POST_EARN: 'observe' });
  const book = makeBook();
  const cal = book.addSheet('Calendar_Events', 100, 12);
  const rows = [CAL_HDR].concat(CAL_ROWS);
  cal.getRange(1, 1, rows.length, 10).setValues(rows);
  const p = payload();
  ok(n.dt10PostEarnAnnotate_(p, book) === 'post-earn ' + WARN + '4/9', 'P2 orchestrator token');
  const once = JSON.stringify(p.selected);
  ok(n.dt10PostEarnAnnotate_(p, book) === 'post-earn 0/9' && JSON.stringify(p.selected) === once,
     'P2 idempotent: a second pass tags nothing and changes no note');
}
console.log('P2 PASS armed (observe / on / mixed case; both feed states; idempotent)');

// ------------------------------------------------------------ P3 golden negative
{
  const b = render(base, { DT10_POST_EARN: 'observe' }, 'EXECUTABLE');
  const k0 = noteRowOf(b.snap, 'SYM0.US') + ':' + NOTE_COL;
  ok(String(b.snap.cells[k0].v).indexOf('post-earnings') < 0 && !/post-earn/.test(b.note || ''),
     'P3 the v1.17.1 base, armed, shows nothing for the FRST case (bought blind on 26 Oct)');
  ok(typeof load(base, {}).dt10PostEarnAnnotate_ === 'undefined', 'P3 the base has no post-earnings path at all');
}
console.log('P3 PASS golden negative');

// ------------------------------------------------------------ P4 pure cores
const C = load(src, {});
const day = s => C.dt10CalendarDay_(s);
ok(C.dt10WeekdaysBetween_(day('2026-10-22'), day('2026-10-27')) === 3, 'P4 Thu -> Tue = 3 sessions');
ok(C.dt10WeekdaysBetween_(day('2026-10-23'), day('2026-10-26')) === 1, 'P4 Fri -> Mon = 1 session');
ok(C.dt10WeekdaysBetween_(day('2026-10-24'), day('2026-10-25')) === 0, 'P4 Sat -> Sun = 0 sessions');
ok(C.dt10WeekdaysBetween_(day('2026-10-27'), day('2026-10-27')) === 0, 'P4 same day = 0');
ok(C.dt10WeekdaysBetween_(day('2026-10-27'), day('2026-10-20')) === 0, 'P4 reversed range = 0');
ok(C.dt10WeekdaysBetween_(day('2026-09-29'), day('2026-10-27')) === 20, 'P4 four weeks = 20');
ok(C.dt10WeekdaysBetween_(-10, -3) === 5, 'P4 days before 1970 still count weekdays (1969-12-22..12-29)');
ok(C.dt10WeekdaysBetween_(null, 5) === 0 && C.dt10WeekdaysBetween_('1', 5) === 0, 'P4 non-numbers = 0');
{
  const vals = [['title row', 'x'], ['', ''], CAL_HDR.slice(0, 7).concat(['Last Earnings Date', 'EPS Surprise %']),
    [' frst.us ', '', '', '', '', '', '', '2026-10-22', '-22.2'],
    ['FRST.US', '', '', '', '', '', '', '2026-10-01', 9],          // duplicate -> first wins
    ['OPBK.US', '', '', '', '', '', '', '2026-10-13', 3.1],
    ['TXT.US', '', '', '', '', '', '', '2026-10-21', '\u221222.2'], // unicode minus text -> unknown
    ['NUM.US', '', '', '', '', '', '', '2026-10-21', ' +4.5 '],
    ['FUT.US', '', '', '', '', '', '', '2026-11-02', -1],
    ['TODAY.US', '', '', '', '', '', '', '2026-10-27', -1],
    ['BAD.US', '', '', '', '', '', '', '2026-02-30', -1],
    ['TS.US', '', '', '', '', '', '', '2026-10-21T10:00:00Z', -1],
    ['SER.US', '', '', '', '', '', '', 46300, -1],
    ['', '', '', '', '', '', '', '2026-10-21', -1],
    ['SYMBOL', '', '', '', '', '', '', '2026-10-21', -1]];
  const m = C.dt10PostEarnMapFromValues_(vals, '2026-10-27');
  ok(deep(Object.keys(m).sort(), ['FRST.US', 'NUM.US', 'OPBK.US', 'TXT.US']), 'P4 map keys: ' + Object.keys(m).sort().join(','));
  ok(deep(m['FRST.US'], { date: '2026-10-22', sessions: 3, surprise: -22.2 }), 'P4 FRST entry (numeric text parsed)');
  ok(m['OPBK.US'].sessions === 10 && m['OPBK.US'].surprise === 3.1, 'P4 OPBK entry');
  ok(m['TXT.US'].surprise === null && m['NUM.US'].surprise === 4.5, 'P4 text surprise unknown; signed numeric text parsed');
  ok(deep(C.dt10PostEarnMapFromValues_([CAL_HDR.slice(0, 7), ['FRST.US', '', '', '', '', '', '']], '2026-10-27'), {}),
     'P4 a 7-column calendar (lane off) -> empty map');
  ok(deep(C.dt10PostEarnMapFromValues_([['Symbol', 'Last Earnings Date'], ['A.US', '2026-10-26']], '2026-10-27'),
          { 'A.US': { date: '2026-10-26', sessions: 1, surprise: null } }), 'P4 no EPS column -> surprise null');
  ok(deep(C.dt10PostEarnMapFromValues_([], '2026-10-27'), {}) && deep(C.dt10PostEarnMapFromValues_(null), {}) &&
     deep(C.dt10PostEarnMapFromValues_(vals, 'not a date'), {}), 'P4 empty / null / bad as-of -> {}');
  ok(C.dt10PostEarnMapFromValues_([CAL_HDR, ['A.US', '', '', '', '', '', '', RIYADH_23OCT, '', '']], '2026-10-27')['A.US'].date ===
     '2026-10-23', 'P4 a Riyadh-midnight Date cell is its Riyadh calendar day');
}
{
  const map = {
    'P5.US': { date: 'd', sessions: 5, surprise: 0 }, 'P6.US': { date: 'd', sessions: 6, surprise: 0 },
    'N10.US': { date: 'd', sessions: 10, surprise: -0.04 }, 'N11.US': { date: 'd', sessions: 11, surprise: -50 },
    'U5.US': { date: 'd', sessions: 5, surprise: null }, 'U6.US': { date: 'd', sessions: 6, surprise: null },
    'Z0.US': { date: 'd', sessions: 0, surprise: 12.345 }, 'NEG.US': { date: 'd', sessions: -1, surprise: -1 }
  };
  const t = Object.keys(map).map(s => ({ symbol: s.toLowerCase(), advisor_note: 'x' })).concat([null, { symbol: '' }]);
  const n = C.dt10ApplyPostEarnTags_(t, map);
  const notes = {};
  t.forEach(x => { if (x && x.symbol) notes[x.symbol.toUpperCase()] = x.advisor_note; });
  ok(n === 4, 'P4 boundaries tag 4: ' + n);
  ok(notes['P5.US'] === WARN + ' post-earnings 5/5 ' + DOT + ' EPS 0.0% ' + DOT + ' x', 'P4 zero surprise = not negative -> 5, tagged at 5');
  ok(notes['P6.US'] === 'x' && notes['U6.US'] === 'x' && notes['N11.US'] === 'x' && notes['NEG.US'] === 'x',
     'P4 6/5, 6/5 (unknown), 11/10 and a negative count are not tagged');
  ok(notes['N10.US'] === WARN + ' post-earnings 10/10 ' + DOT + ' EPS -0.0% ' + DOT + ' x', 'P4 a tiny negative surprise -> 10, tagged at 10');
  ok(notes['U5.US'] === WARN + ' post-earnings 5/5 ' + DOT + ' EPS n/a ' + DOT + ' x', 'P4 unknown surprise -> 5');
  ok(notes['Z0.US'] === WARN + ' post-earnings 0/5 ' + DOT + ' EPS +12.3% ' + DOT + ' x', 'P4 a weekend read (0 sessions) is inside');
  ok(C.dt10ApplyPostEarnTags_(t, map) === 0, 'P4 idempotent');
  ok(C.dt10ApplyPostEarnTags_([], map) === 0 && C.dt10ApplyPostEarnTags_(null, map) === 0 &&
     C.dt10ApplyPostEarnTags_(t, null) === 0, 'P4 empty / null inputs -> 0');
  ok(C.dt10UvWithholdRow_(['x', 'plain note'], [], 1, 'r')[1] === C.dt10UvWithholdNote_('r') &&
     C.dt10UvWithholdRow_(['x', WARN + ' earnings ' + LE + '4d ' + DOT + ' n'], [], 1, 'r')[1] ===
       WARN + ' earnings ' + LE + '4d ' + DOT + ' ' + C.dt10UvWithholdNote_('r'),
     'P4 withheld rows without a post-earnings tag keep exactly the v1.17.1 prefix');
}
{
  const B = load(base, {});
  const corpus = ['', 'plain', WARN + ' earnings ' + LE + '3d ' + DOT + ' INVEST', WARN + ' earnings soon', WARN + ' post-earnings x',
    'note ' + WARN + ' earnings ' + LE + '3d ' + DOT + ' ', 'Research seat \u2014 no executable allocation today.', 12, null];
  for (const v of corpus) {
    ok(deep(C.dt10UvWithholdRow_(['a', v, 'b'], [0], 1, 'final_publication'), B.dt10UvWithholdRow_(['a', v, 'b'], [0], 1, 'final_publication')),
       'P4 withheld note identical to the base for ' + JSON.stringify(v));
  }
}
console.log('P4 PASS pure cores');

// ------------------------------------------------------------ P5 tokens
{
  const noRows = render(src, { DT10_POST_EARN: 'observe' }, 'EXECUTABLE', { calRows: CAL_ROWS.map(r => r.slice(0, 7).concat(['', '', ''])) });
  ok(/\| post-earn n\/a$/.test(noRows.note || '') && !/post-earnings/.test(JSON.stringify(noRows.snap)), 'P5 no last-report rows -> n/a, no tag: ' + noRows.note);
  const narrow = render(src, { DT10_POST_EARN: 'observe' }, 'EXECUTABLE', { narrowCalendar: true });
  ok(/\| post-earn n\/a$/.test(narrow.note || ''), 'P5 a 7-column calendar -> n/a: ' + narrow.note);
  const none = render(src, { DT10_POST_EARN: 'observe' }, 'EXECUTABLE', { noCalendar: true });
  ok(none.note === 'post-earn n/a', 'P5 no Calendar_Events tab -> n/a alone: ' + none.note);
  const fault = render(src, { DT10_POST_EARN: 'observe' }, 'EXECUTABLE', { throwOn: 'Calendar_Events' });
  ok(fault.note === 'post-earn n/a' && !/post-earnings/.test(JSON.stringify(fault.snap)), 'P5 read fault -> n/a, render completes: ' + fault.note);
  const quiet = render(src, { DT10_POST_EARN: 'observe' }, 'EXECUTABLE',
    { calRows: [['SYM1.US', '', '', '', '', '', 'eodhd', '2026-10-13', 14, 3.1]] });
  ok(quiet.note === 'post-earn 0/9', 'P5 nothing inside a quarantine -> 0/9: ' + quiet.note);
}
console.log('P5 PASS tokens');

// ------------------------------------------------------------ P6 self-test
const st0 = src.indexOf('  /* v1.18.0 [POST-EARNINGS]: pure core on the FRST case');
const st1 = src.indexOf('  /* v1.11.12 [SYNC-INFLIGHT]: pure core on the 2026-10-03 13:05 race', st0);
ok(st0 > 0 && st1 > st0, 'P6 self-test block located');
function runBlock(ctx, code) {
  const report = [];
  vm.runInContext('(function (report) {\n' + code + '\n})', ctx)(report);
  return report;
}
for (const [props, mode] of [[{}, 'off'], [{ DT10_POST_EARN: 'observe' }, 'observe'], [{ DT10_POST_EARN: 'ON' }, 'observe']]) {
  const rep = runBlock(load(src, props), src.slice(st0, st1));
  ok(rep.length === 1 && rep[0] === 'post-earnings core: ok (FRST 3/10 tagged; +EPS at 10 sessions untagged; blank EPS 5/5) | mode now: ' + mode,
     'P6 self-test line ' + JSON.stringify(props) + ': ' + rep[0]);
}
// The REAL dt10SelfTest (full workbook stub, zero FAIL, the new line present) is
// tests/js/decision_input_truth.test.js T8; this harness runs the block itself.
console.log('P6 PASS self-test');

// ------------------------------------------------------------ P7 static
ok((src.match(/^var DT10_VERSION = '1\.18\.0';$/m) || []).length === 1 && /^ \* Version: 1\.18\.0 \(see DT10_VERSION;/.test(src.split('\n')[3]),
   'P7 version lockstep 1.18.0');
const why = src.slice(src.indexOf(' * v1.18.0 (2026-10-27) [POST-EARNINGS]'), src.indexOf(' * v1.17.1 (2026-10-27) [SHADOW-MOMENTUM]'));
ok(why.length > 2000 && /^[\x09\x0a\x20-\x7e]*$/.test(why), 'P7 WHY block present (newest first) and ASCII');
ok(src.indexOf(base.slice(base.indexOf(' * v1.17.1 (2026-10-27) [SHADOW-MOMENTUM]'), base.indexOf(' * v1.17.0 (2026-10-27) [SHADOW-RANK-LOG]'))) > 0,
   'P7 the v1.17.1 WHY block verbatim');
const baseLines = new Set(base.split('\n'));
const added = src.split('\n').filter((l) => !baseLines.has(l));
ok(added.length >= 140 && added.length <= 240, 'P7 added lines ' + added.length);
for (const l of added) {
  if (/^ \* Version: 1\.18\.0 /.test(l)) continue;   // the version line keeps its pre-existing em dash
  ok(/^[\x09\x20-\x7e]*$/.test(l), 'P7 ASCII added line: ' + l);
  const code = l.replace(/\/\/.*$/, '');
  ok(!/\b(let|const|class)\s|=>|`|\.\.\./.test(code) || /^\s*(\*|\/\*)/.test(l), 'P7 ES5 added line: ' + l);
}
function census(s, re) { const out = new Set(); let m; while ((m = re.exec(s))) out.add(m[1]); return out; }
const fb = census(base, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), fn = census(src, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
const vb = census(base, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = census(src, /^var\s+([A-Za-z_$][\w$]*)/mg);
ok([...fb].every((f) => fn.has(f)) && [...vb].every((v) => vn.has(v)), 'P7 zero functions / vars removed');
ok(deep([...fn].filter((f) => !fb.has(f)).sort(), ['dt10ApplyPostEarnTags_', 'dt10PostEarnAnnotate_', 'dt10PostEarnMapFromValues_',
   'dt10PostEarnMap_', 'dt10PostEarnMode_', 'dt10WeekdaysBetween_']) &&
   deep([...vn].filter((v) => !vb.has(v)).sort(), ['DT10_POST_EARN_PROP', 'DT10_POST_EARN_SESSIONS', 'DT10_POST_EARN_SESSIONS_NEG']),
   'P7 exactly the designed names added');
ok(fb.size === 218 && fn.size === 224 && vb.size === 117 && vn.size === 120, 'P7 census 218 -> 224 functions, 117 -> 120 vars');
const edits = require('./js/dt10_edits_v1180.json');
ok(edits.length === 7, 'P7 seven hunks');
ok(crypto.createHash('sha256').update(base).digest('hex') === history.V1171_SHA256, 'P7 reversal = the reviewed v1.17.1 file (SHA-256 pinned)');
for (const name of ['dt10PostEarnMode_', 'dt10WeekdaysBetween_', 'dt10PostEarnMapFromValues_', 'dt10PostEarnMap_',
                    'dt10ApplyPostEarnTags_', 'dt10PostEarnAnnotate_']) {
  const start = src.indexOf('function ' + name + '(');
  const body = src.slice(start, src.indexOf('\n}\n', start) + 3);
  ok(start > 0 && /^[\x00-\x7f]*$/.test(body), 'P7 ASCII ' + name);
  new vm.Script('(function(){' + body + '})');
}
new vm.Script(src, { filename: '16_Decision_Top10.gs' });
console.log('P7 PASS lockstep, WHY, lint, inventory, hunks');

const digest = crypto.createHash('sha256').update(src).digest('hex').slice(0, 12);
console.log('ALL PASS (' + pass + ' checks) on ' + path.basename(SRC) + ' sha256 ' + digest);
