#!/usr/bin/env node
/* tests/test_dp2_v1301_slot_carry.js
 * Harness for apps_script/27_Decision_Pages_v2.gs v1.3.1 [SLOT-CARRY] (first slot, Wed 11 Nov 2026 17:00 Riyadh: the
 * main page did not render in the 17:00 hour, the slot waited for minute 40 and built Portfolio_Decision_v2 only). In an
 * hour that is not a slot hour, dp2SlotTick goes on with the slot of the hour before when that slot still has a page to
 * build with tries left; it waits for no main-page render; the busy, claim and tries rules stand.
 *
 * Two trees over the REAL file, each in its own vm context: the delivered source (NEW) and the v1.3.0 file (OLD,
 * SHA-256 pinned f670ad1c = origin/main 53f2abd), rebuilt from NEW by reversing the enumerated v1.3.1 hunks
 * (tests/js/dp2_edits_v1301.json). The GAS shim, the fixture, the boards and the slot worlds are those of
 * tests/test_dp2_v1300_refresh_slots.js, copied verbatim (norm folds 1.3.1).
 *   P0 oracle, version lockstep, inventory (+1 function, 0 line-start vars, 0 removed), 6 hunks reverse to the base
 *   P1 GOLDEN NEGATIVE: menu builds, the installers and every tick inside a slot hour = v1.3.0
 *   P2 pure: dp2SlotCarryCore_ and the core check
 *   P3 carry ticks: the second page in the next hour, an empty slot, tries used up, a decision render, hour 0 -> 1,
 *      no carry after a non-slot hour, nothing pending
 *   P4 dp2SelfTest = the v1.3.0 report
 *   P5 lint: ES5; no non-ASCII character in a hunk; LF; WHY newest-first; every prior WHY kept
 *   P6 mutants
 * The battery runs twice and the two digests must match; the last line is 'PASS k/k' (exit 1 on any failure).
 * Usage: node tests/test_dp2_v1301_slot_carry.js [new.gs] [v1.3.0.gs]
 */
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const vm = require('vm');
const ROOT = path.join(__dirname, '..');
const NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '27_Decision_Pages_v2.gs');
const OLD_PATH = process.argv[3] || '';
const NEW = fs.readFileSync(NEW_PATH, 'utf8');
const FIX = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'fixtures', 'dp2_board_2026-11-02.json'), 'utf8'));
const EDITS = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'dp2_edits_v1301.json'), 'utf8'));
const V1300_SHA256 = 'f670ad1cd2ccadfbd4a4b60028ceaeacf0939f51a6ea4407d0bbe4a03330c4c9';   // v1.3.0 (origin/main 53f2abd)
const NOW = Date.parse(FIX.frozen_now);   // 2026-11-02T06:24:00Z = 09:24 Riyadh
const DOT = '·', DASH = '—', ARROW = '→', NDASH = '–';
const sha256 = s => crypto.createHash('sha256').update(s).digest('hex');
const plain = v => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));
const clone = plain;

let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + String(e && e.stack || e).split('\n').slice(0, 7).join('\n  ')); }
}
function ok(c, m) { if (!c) throw new Error(m || 'assertion failed'); }
function eq(a, b, m) { if (a !== b) throw new Error((m || 'not equal') + ': ' + JSON.stringify(a) + ' !== ' + JSON.stringify(b)); }
function deq(a, b, m) {
  const ja = JSON.stringify(a), jb = JSON.stringify(b);
  if (ja !== jb) {
    let i = 0; while (i < ja.length && ja[i] === jb[i]) i++;
    throw new Error((m || 'deep-equal') + ' differs at ' + i + ': ...' + ja.slice(Math.max(0, i - 140), i + 160) + ' | ...' + jb.slice(Math.max(0, i - 140), i + 160));
  }
}
function near(a, b, tol, m) { if (!(Math.abs(a - b) <= tol)) throw new Error((m || 'not near') + ': ' + a + ' vs ' + b); }


/* ------------------------------------------------------------ P0 oracle */
function reverseEdits(src) {
  let s = src;
  for (const [cur, prev] of EDITS) {
    eq(s.split(cur).length - 1, 1, 'exactly one approved v1.3.1 hunk: ' + cur.split('\n')[0].slice(0, 90));
    s = s.replace(cur, () => prev);
  }
  eq(sha256(s), V1300_SHA256, 'v1.3.1 changes only its enumerated hunks (their reversal = the v1.3.0 file)');
  return s;
}
let OLD = null, oldErr = '';
try {
  OLD = OLD_PATH ? fs.readFileSync(OLD_PATH, 'utf8') : reverseEdits(NEW);
  eq(sha256(OLD), V1300_SHA256, 'v1.3.0 SHA-256');
} catch (e) { oldErr = String(e && e.message || e); OLD = null; }
if (!OLD) { console.log('FAIL P0 oracle: ' + oldErr); console.log('FAIL 0/1'); process.exit(1); }

function colLetter(n) { let s = ''; while (n > 0) { const m = (n - 1) % 26; s = String.fromCharCode(65 + m) + s; n = Math.floor((n - 1) / 26); } return s; }
function makeBook() {
  const sheets = new Map();
  const book = {
    calls: [], toasts: [],
    getSheetByName(n) { return sheets.get(n) || null; },
    insertSheet(n) { const s = makeSheet(book, n); sheets.set(n, s); book.calls.push('insertSheet|' + n); return s; },
    toast(msg, title, sec) { book.toasts.push([String(msg), String(title), sec]); book.calls.push('toast|' + msg + '|' + title + '|' + sec); },
    getSpreadsheetTimeZone() { return 'Asia/Riyadh'; },
    setActiveSheet(sh) { book.calls.push('setActiveSheet|' + sh.getName()); return sh; },
    getName() { return 'TFB'; }
  };
  book._sheets = sheets;
  book.load = (name, grid) => { const s = makeSheet(book, name); sheets.set(name, s); s._load(grid); return s; };
  return book;
}
function makeSheet(book, name) {
  const cells = new Map();
  let maxRows = 1000, maxCols = 26, frozen = 0, hidden = false, gridHidden = false;
  let merges = [], borders = [], charts = [], cf = [];
  const rowH = {}, colW = {};
  const key = (r, c) => r + ':' + c;
  const log = m => book.calls.push(name + '|' + m);
  const cell = (r, c) => { const k = key(r, c); let x = cells.get(k); if (!x) { x = {}; cells.set(k, x); } return x; };
  const has = x => x && x.v !== undefined && x.v !== '' && x.v !== null;
  const sheet = {
    _cells: cells,
    _state() {
      const cl = [];
      for (const [k, x] of cells) cl.push(k + '=' + JSON.stringify(x));
      cl.sort();
      return { cells: cl, merges: merges.slice().sort(), borders: borders.slice(), rowH: Object.assign({}, rowH), colW: Object.assign({}, colW),
               frozen, hidden, gridHidden, charts: charts.map(c => c.spec), cf: cf.length, maxRows, maxCols };
    },
    _load(grid) {
      for (let i = 0; i < grid.length; i++) for (let j = 0; j < grid[i].length; j++) { const v = grid[i][j]; if (v !== '' && v !== null && v !== undefined) cell(i + 1, j + 1).v = v; }
      maxRows = Math.max(maxRows, grid.length); maxCols = Math.max(maxCols, grid.reduce((m, r) => Math.max(m, r.length), 0));
    },
    getName: () => name, getParent: () => book,
    getLastRow() { let m = 0; for (const [k, x] of cells) if (has(x)) m = Math.max(m, +k.split(':')[0]); return m; },
    getLastColumn() { let m = 0; for (const [k, x] of cells) if (has(x)) m = Math.max(m, +k.split(':')[1]); return m; },
    getMaxRows: () => maxRows, getMaxColumns: () => maxCols,
    getRange(r, c, nr, nc) {
      if (typeof r !== 'number') throw new Error('A1 notation is not modelled');
      nr = nr === undefined ? 1 : nr; nc = nc === undefined ? 1 : nc;
      if (!(r >= 1 && c >= 1 && nr >= 1 && nc >= 1)) throw new Error('range shape ' + [r, c, nr, nc]);
      return makeRange(r, c, nr, nc);
    },
    getDataRange() { return makeRange(1, 1, Math.max(1, sheet.getLastRow()), Math.max(1, sheet.getLastColumn())); },
    insertColumnsAfter(a, n) { maxCols += n; log('insertColumnsAfter|' + a + '|' + n); },
    deleteColumns(s, n) { maxCols -= n; for (const k of Array.from(cells.keys())) { const c = +k.split(':')[1]; if (c >= s && c < s + n) cells.delete(k); } log('deleteColumns|' + s + '|' + n); },
    insertRowsAfter(a, n) { maxRows += n; log('insertRowsAfter|' + a + '|' + n); },
    setColumnWidth(c, w) { colW[c] = w; log('setColumnWidth|' + c + '|' + w); },
    setRowHeight(r, h) { rowH[r] = h; log('setRowHeight|' + r + '|' + h); },
    setRowHeightsForced(r, n, h) { for (let i = 0; i < n; i++) rowH[r + i] = h; log('setRowHeightsForced|' + r + '|' + n + '|' + h); },
    setHiddenGridlines(b) { gridHidden = b; log('setHiddenGridlines|' + b); },
    setFrozenRows(n) { frozen = n; log('setFrozenRows|' + n); },
    getFrozenRows: () => frozen,
    clear() { cells.clear(); merges = []; borders = []; log('clear'); },
    clearConditionalFormatRules() { cf = []; log('clearConditionalFormatRules'); },
    getConditionalFormatRules: () => cf.slice(),
    setConditionalFormatRules(rules) { cf = rules.slice(); log('setConditionalFormatRules|' + rules.length); },
    getCharts: () => charts.slice(),
    removeChart(ch) { charts = charts.filter(x => x !== ch); log('removeChart'); },
    newChart() { return makeChartBuilder(); },
    insertChart(ch) { charts.push(ch); log('insertChart|' + JSON.stringify(ch.spec)); },
    hideSheet() { hidden = true; log('hideSheet'); }
  };
  function makeRange(r, c, nr, nc) {
    const self = {};
    const tag = r + ',' + c + ',' + nr + ',' + nc;
    const each = fn => { for (let i = 0; i < nr; i++) for (let j = 0; j < nc; j++) fn(r + i, c + j, i, j); };
    const setAll = (field, m) => v => { each((R, C) => { cell(R, C)[field] = v; }); log(m + '|' + tag + '|' + JSON.stringify(v)); return self; };
    Object.assign(self, {
      setValue(v) { const x = cell(r, c); x.v = v; delete x.f; log('setValue|' + tag + '|' + JSON.stringify(v)); return self; },
      setValues(g) {
        if (!(Array.isArray(g) && g.length === nr && g.every(row => Array.isArray(row) && row.length === nc))) throw new Error('setValues shape ' + tag);
        each((R, C, i, j) => { cell(R, C).v = g[i][j]; }); log('setValues|' + tag + '|' + JSON.stringify(g)); return self;
      },
      setFormula(f) { const x = cell(r, c); x.f = f; x.v = ''; log('setFormula|' + tag + '|' + f); return self; },
      getValues() { return Array.from({ length: nr }, (_, i) => Array.from({ length: nc }, (_, j) => { const x = cells.get(key(r + i, c + j)); return x && x.v !== undefined && x.v !== null ? x.v : ''; })); },
      setBackground: setAll('bg', 'setBackground'), setFontColor: setAll('fg', 'setFontColor'), setFontWeight: setAll('fw', 'setFontWeight'),
      setFontSize: setAll('fs', 'setFontSize'), setFontFamily: setAll('ff', 'setFontFamily'), setFontStyle: setAll('fst', 'setFontStyle'),
      setNumberFormat: setAll('nf', 'setNumberFormat'), setWrap: setAll('wrap', 'setWrap'),
      setHorizontalAlignment: setAll('ha', 'setHorizontalAlignment'), setVerticalAlignment: setAll('va', 'setVerticalAlignment'),
      setBorder(...a) { borders.push(tag + '|' + a.map(String).join('|')); log('setBorder|' + tag + '|' + a.map(String).join('|')); return self; },
      merge() { merges.push(tag); log('merge|' + tag); return self; },
      breakApart() {
        merges = merges.filter(m => { const [mr, mc, mnr, mnc] = m.split(',').map(Number); return mr + mnr - 1 < r || mr > r + nr - 1 || mc + mnc - 1 < c || mc > c + nc - 1; });
        log('breakApart|' + tag); return self;
      },
      getA1Notation() { return name + '!' + colLetter(c) + r + ':' + colLetter(c + nc - 1) + (r + nr - 1); },
      getRow: () => r, getColumn: () => c, getNumRows: () => nr, getNumColumns: () => nc
    });
    return self;
  }
  return sheet;
}
function makeChartBuilder() {
  const spec = { ranges: [], options: {}, type: null, pos: null, headers: null };
  const b = {
    addRange(rg) { spec.ranges.push(rg.getA1Notation()); return b; },
    setNumHeaders(n) { spec.headers = n; return b; },
    setPosition(r, c, ox, oy) { spec.pos = [r, c, ox, oy]; return b; },
    setOption(k, v) { spec.options[k] = plain(v); return b; },
    setChartType(t) { spec.type = t; return b; },
    build() { return { spec: plain(spec) }; }
  };
  return b;
}
function fakeDate(nowMs) {
  const RealDate = Date;
  class FakeDate extends RealDate {
    constructor(...a) { if (a.length === 0) super(nowMs); else super(...a); }
    static now() { return nowMs; }
  }
  return FakeDate;
}
function formatDate(d, tz, pattern) {
  const ms = (d && typeof d.getTime === 'function') ? d.getTime() : Date.parse(String(d));
  const off = /Riyadh/.test(String(tz)) ? 3 : (/New_York/.test(String(tz)) ? -4 : 0);
  const t = new Date(ms + off * 3600000), p = n => (n < 10 ? '0' : '') + n;
  return String(pattern).replace('yyyy', t.getUTCFullYear()).replace('MM', p(t.getUTCMonth() + 1)).replace('dd', p(t.getUTCDate()))
    .replace('HH', p(t.getUTCHours())).replace('mm', p(t.getUTCMinutes())).replace('ss', p(t.getUTCSeconds()));
}
/* ------------------------------------------------------------ fixture -> workbook */
function padRow(row, n) { const r = row.slice(); while (r.length < n) r.push(''); return r; }
function top10Grid(t) {
  const W = 30, g = [];
  const put = (r, row) => { while (g.length < r) g.push(padRow([], W)); g[r - 1] = padRow(row, W); };
  put(1, [t.title]); put(2, ['Status', t.status]); put(4, ['CONTROL PANEL']);
  t.panel.forEach((row, i) => put(5 + i, row));
  put(14, ['KPIs']); put(15, t.kpi_labels); put(16, t.kpi_values);
  put(18, [t.banner]); put(19, t.seat_headers);
  t.seats.forEach((s, i) => put(20 + i, s));
  let r = 20 + t.seats.length + 1;
  put(r++, ['ALERTS']); put(r++, ['Type', 'Count', 'Required action']); t.alerts.forEach(a => put(r++, a)); r++;
  put(r++, ['ALL QUALIFIED ' + DASH + ' INVEST opportunity set (' + t.qualified.length + ')']); put(r++, t.qualified_headers); t.qualified.forEach(q => put(r++, q)); r++;
  put(r++, [t.gaps_title]); put(r++, t.gaps_headers); t.gaps.forEach(x => put(r++, x));
  return g;
}
const statusGrid = kv => [['Global Key', 'Value']].concat(Object.keys(kv).map(k => [k, kv[k]]));
const pdGrid = pd => [['PORTFOLIO DECISION'], ['Status', pd.status], ['']].concat(pd.panel);
const s1Grid = s1 => [s1.title, [''], ['#', 'Criterion', 'Status', 'Detail']].concat(s1.criteria);
const ledgerGrid = l => [['PORTFOLIO COST BASIS'], ['Status', l.status], [''], l.headers].concat(l.lots);
const mpGrid = m => [m.headers].concat(m.rows);
function gmGrid(counts) {
  const g = [['Symbol', 'Name', 'Investability Status']];
  let n = 0;
  for (const k of Object.keys(counts)) for (let i = 0; i < counts[k]; i++) { n++; g.push(['GM' + n + '.US', 'Global name ' + n, k]); }
  return g;
}
function perfGrid(rows) {
  const g = [['Record ID', 'Status', 'Outcome', 'Entry Investability', 'Entry Recommendation', 'Horizon']];
  rows.forEach((x, i) => g.push(['PL-' + (i + 1), 'matured', x[0], x[1], x[2], x[3]]));
  return g;
}
function barsGrid(spec) {
  const g = [['Symbol', 'Key', 'Date', 'Open', 'High', 'Low', 'Close', 'Source', 'Fetched (Riyadh)']];
  for (const sym of Object.keys(spec)) {
    const b = spec[sym];
    let d = Date.UTC(2026, 8, 21), n = 0;
    while (n < 30) {
      const wd = new Date(d).getUTCDay();
      if (wd !== 0 && wd !== 6) {
        const c = Math.round((b.base + b.step * ((n % 7) - 3)) * 100) / 100;
        g.push([sym, sym.replace(/\.US$/, ''), new Date(d).toISOString().slice(0, 10), Math.round((c - b.step * 0.3) * 100) / 100,
                Math.round((c + b.step * 1.2) * 100) / 100, Math.round((c - b.step * 1.1) * 100) / 100, c, 'yahoo', '2026-11-01 18:00']);
        n++;
      }
      d += 86400000;
    }
  }
  return g;
}
/* ------------------------------------------------------------ boards (today, and the variants the sections need) */
const SEAT = {}; FIX.top10.seat_headers.forEach((h, i) => { SEAT[h] = i; });
const RESEARCH_FF = 'Research ' + DASH + ' no allocation';
function board(name, mutTop, mutKv) {
  const top = clone(FIX.top10), kv = clone(FIX.status_kv);
  if (mutTop) mutTop(top);
  if (mutKv) mutKv(kv);
  return { name, top10: top, kv };
}
function setSeat(top, sym, field, v) { const s = top.seats.find(x => x[SEAT.Symbol] === sym); if (!s) throw new Error('no seat ' + sym); s[SEAT[field]] = v; }
function unfund(top, sym) {   // what the cockpit's dt10FinalizeBoard_ writes on a seat it did not fund
  const s = top.seats.find(x => x[SEAT.Symbol] === sym);
  ['Ticket SAR', 'Shares', 'Gain 12M SAR'].forEach(f => { s[SEAT[f]] = 0; });
  ['Entry Zone', 'Stop SAR', 'TP1 SAR', 'TP2 SAR'].forEach(f => { s[SEAT[f]] = DASH; });
  s[SEAT['Funds From']] = RESEARCH_FF;
  s[SEAT['Advisor Note']] = 'Research seat ' + DASH + ' no executable allocation today. ' + s[SEAT.Stability];
}
const ENGINE_SYMS = ['ITRN.US', 'DDI.US', 'CRC.US', 'GLPI.US', 'EFC.US', 'INDV.US'];
const ACTIVE_SYMS = ENGINE_SYMS.concat(['OLP.US']);
const SEAT_SYMS = FIX.top10.seats.map(s => s[SEAT.Symbol]);
function researchTop(top, strict, ftNote) {
  ENGINE_SYMS.forEach(s => unfund(top, s));
  setSeat(top, '2020.HK', 'Rank', 10); setSeat(top, '2020.HK', 'Stability', 'FAST-TRACK (day 1)');
  setSeat(top, '2020.HK', 'Advisor Note', ftNote || ('Research seat ' + DASH + ' no executable allocation today. FAST-TRACK (day 1)'));
  top.status = top.status.replace('output: EXECUTABLE', 'output: WITHHELD').replace('funding=final/6exec', 'funding=research/0exec')
    .replace('stab[strict]: 3 grace, confirm=qualified', (strict ? 'stab[strict]' : 'stab') + ': 2 grace, 1 fast-track, confirm=qualified');
  top.kpi_values[2] = 0; top.kpi_values[7] = 72747; top.kpi_values[8] = 0;
  top.banner = 'ℹ FEED NOT ACTIONABLE ' + DASH + ' coverage_failed ' + DASH + ' SELECTED ' + DASH + ' 0 EXECUTABLE / 8 QUALIFIED PLANS + 2 GRACE-HELD (NO PLAN TODAY) ' + DASH + ' SIZING WITHHELD';
}
const naFeed = kv => { kv['TFB Decision Feed'] = FIX.feed_not_actionable; };
const BOARDS = {
  today: board('today'),
  research: board('research strict', t => researchTop(t, true), naFeed),
  nonStrict: board('research not strict', t => researchTop(t, false), naFeed),
  noteSuspended: board('research, suspended by its note', t => researchTop(t, false, 'FAST-TRACK (day 1) ' + DASH + ' sizing suspended under strict until confirmed (seat filled to avoid under-fill; not an executable ticket today).'), naFeed),
  // v1.2.30 on the research board's seats labelled the old way: the seven ACTIVE seats as FAST-TRACK, the fast-track seat as GRACE
  researchAsFastTrack: board('research seats labelled FAST-TRACK', t => { researchTop(t, true); ACTIVE_SYMS.forEach((s, i) => setSeat(t, s, 'Stability', 'FAST-TRACK (day ' + (i + 1) + ')')); setSeat(t, '2020.HK', 'Stability', 'GRACE (0/3 missed)'); }, naFeed),
  engineCaps: board('engine tickets beyond the sector cap', t => { setSeat(t, 'CRC.US', 'Sector', 'Real Estate'); setSeat(t, 'EFC.US', 'Sector', 'Real Estate'); }),
  held: board('OLP held for earnings', t => { setSeat(t, 'OLP.US', 'Funds From', 'Held ' + DASH + ' earnings in 0d'); }),
  researchHeld: board('research strict, ITRN held for earnings', t => { researchTop(t, true); setSeat(t, 'ITRN.US', 'Funds From', 'Held ' + DASH + ' earnings in 0d'); }, naFeed),
  todayNA: board('today, feed NOT_ACTIONABLE', null, naFeed),
  engineActiveUnfunded: board('engine board, OLP without a Funds From', t => { setSeat(t, 'OLP.US', 'Funds From', ''); }),
  researchCaps: board('research strict, three ACTIVE seats in one sector', t => { researchTop(t, true); setSeat(t, 'CRC.US', 'Sector', 'Real Estate'); }, naFeed),
  todaySuper: board('today, feed superseded', null, kv => { kv['TFB Decision Feed'] = FIX.feed_superseded; }),
  researchExec: board('research strict, feed EXECUTABLE', t => researchTop(t, true))
};

/* ------------------------------------------------------------ environments and builds */
function makeEnv(source, opts) {
  opts = opts || {};
  const b = opts.board || BOARDS.today;
  const book = opts.book || makeBook();
  if (!opts.book) {
  book.load('Top_10_Investments', top10Grid(b.top10));
  book.load('_Status', statusGrid(b.kv));
  book.load('Portfolio_Decision', pdGrid(opts.pd || FIX.pd));
  book.load('S1_Gate', s1Grid(opts.s1 || FIX.s1));
  book.load('_Portfolio_CostBasis', ledgerGrid(opts.ledger || FIX.ledger));
  book.load('My_Portfolio', mpGrid(opts.mp || FIX.my_portfolio));
  book.load('Global_Markets', gmGrid(FIX.global_markets_status));
  book.load('Performance_Log', opts.perf || perfGrid(FIX.performance_log));   // v1.2.36 harness: a dated grid when given
  book.load('_DP2_Exit_Engine', barsGrid(FIX.bars));
  if (opts.divTab) book.load('Dividend_Events', opts.divTab);
  }
  const props = opts.propsRef || Object.assign({}, opts.props || {}), reads = {}, logs = [], mails = [];
  const capture = opts.capture === undefined ? FIX.ibkr_capture : opts.capture;
  const ctx = {
    Date: opts.DateCls || fakeDate(opts.now || NOW),
    SpreadsheetApp: { getActive: () => book, getActiveSpreadsheet: () => book, getUi() { throw new Error('no ui in the harness'); }, BorderStyle: { SOLID: 'SOLID' } },
    PropertiesService: { getScriptProperties() {
      if (opts.propsThrow) throw new Error('synthetic property outage');
      return { getProperty(k) { reads[k] = (reads[k] || 0) + 1; return Object.prototype.hasOwnProperty.call(props, k) ? props[k] : null; },
               setProperty(k, v) { props[k] = String(v); }, getProperties() { return Object.assign({}, props); } }; } },
    Utilities: { formatDate, sleep() {} },
    LockService: { getScriptLock() { return { tryLock() { return true; }, releaseLock() {} }; } },
    MailApp: { sendEmail(...a) { mails.push(plain(a)); }, getRemainingDailyQuota() { return 100; } },
    Session: { getEffectiveUser() { return { getEmail() { return 'owner@example.test'; } }; }, getActiveUser() { return { getEmail() { return 'owner@example.test'; } }; }, getScriptTimeZone() { return 'Asia/Riyadh'; } },
    UrlFetchApp: opts.fetch || { fetch() { throw new Error('no network in the harness'); } },
    Logger: { log(m) { logs.push(String(m)); } },
    Charts: { ChartType: { BAR: 'BAR', COLUMN: 'COLUMN', LINE: 'LINE', SCATTER: 'SCATTER', AREA: 'AREA', PIE: 'PIE' } },
    HtmlService: { createHtmlOutput() { throw new Error('no html in the harness'); } },
    ScriptApp: opts.scriptApp || { getProjectTriggers() { return []; } }
  };
  if (capture) ctx.pdIbkrLatestCapture_ = () => clone(capture);
  vm.createContext(ctx);
  vm.runInContext(source, ctx, { filename: '27_Decision_Pages_v2.gs' });
  return { ctx, book, logs, mails, reads, props };
}
/* which: 't10' | 'pd' | 'both' */
function build(source, which, opts) {
  const env = makeEnv(source, opts);
  const fn = which === 'pd' ? 'buildPortfolioDecisionV2' : (which === 't10' ? 'buildTop10V2' : 'buildDecisionPagesV2');
  const ret = env.ctx[fn]();
  const sheets = {};
  for (const [n, s] of env.book._sheets) sheets[n] = s._state();
  return { env, ret: plain(ret), sheets, calls: env.book.calls.slice(), toasts: env.book.toasts.slice(), logs: env.logs.slice(), mails: env.mails.slice() };
}
function norm(b) {   // the only bytes the pages may differ in while the gate is off: the version printed on both footers / the toast title
  const strip = o => { const x = Object.assign({}, o); delete x.env; return x; };
  let s = JSON.stringify(strip(b));
  for (const v of ['1.3.1', '1.3.0', '1.2.36', '1.2.35', '1.2.34', '1.2.33', '1.2.32', '1.2.31']) s = s.split('Decision Pages v2 ' + v).join('Decision Pages v2 1.2.30');
  return JSON.parse(s);
}
function sheet(b, name) { return b.env.book.getSheetByName(name); }
function val(sh, r, c) { const x = sh._cells.get(r + ':' + c); return x && x.v !== undefined && x.v !== null ? x.v : ''; }
function fmt(sh, r, c) { return sh._cells.get(r + ':' + c) || {}; }
function rowsWhere(sh, c, pred) { const out = [], last = sh.getLastRow(); for (let r = 1; r <= last; r++) if (pred(String(val(sh, r, c)), r)) out.push(r); return out; }

/* ------------------------------------------------------------ slot worlds: one workbook, one property store, ticks */
const T10 = 'Top_10_Investments_v2', PDV = 'Portfolio_Decision_v2';
const NO_NET = { fetch() { return { getResponseCode: () => 404, getContentText: () => '{}' }; } };
const at = (h, m) => Date.UTC(2026, 10, 2, h - 3, m, 0);   // Riyadh h:m on 2 Nov 2026
const HOURS = { DP2_REFRESH_HOURS: '8,9' };
const ELL = '…';
const isoAgo = (ms, min) => new Date(ms - min * 60000).toISOString();
function steppingDate(startMs, stepMs) {
  let t = startMs;
  const RealDate = Date;
  class SteppingDate extends RealDate {
    constructor(...a) { if (a.length === 0) { super(t); t += stepMs; } else super(...a); }
    static now() { const v = t; t += stepMs; return v; }
  }
  return SteppingDate;
}
function scriptAppShim(w) {
  return {
    getProjectTriggers() { return w.triggers.map(t => ({ getHandlerFunction: () => t.h, _t: t })); },
    deleteTrigger(tr) { w.triggers = w.triggers.filter(t => t !== tr._t); },
    newTrigger(h) {
      const t = { h, kind: '', every: 0 };
      const b = { timeBased() { return b; }, everyMinutes(n) { t.kind = 'minutes'; t.every = n; return b; },
                  everyHours(n) { t.kind = 'hours'; t.every = n; return b; }, create() { w.triggers.push(t); return t; } };
      return b;
    }
  };
}
function world(src, props, opts) {
  opts = opts || {};
  const w = { src, props: Object.assign({}, props || {}), triggers: (opts.triggers || []).map(t => Object.assign({}, t)), book: null };
  w.book = makeEnv(src, { board: opts.board || BOARDS.today, fetch: NO_NET, propsRef: w.props }).book;
  if (opts.status) w.book.getSheetByName('Top_10_Investments').getRange(2, 2).setValue(opts.status);
  w.book.calls.length = 0;
  return w;
}
function tick(w, ms, extra) {
  extra = extra || {};
  const env = makeEnv(w.src, { fetch: NO_NET, book: w.book, propsRef: w.props, now: ms, DateCls: extra.DateCls, scriptApp: scriptAppShim(w) });
  if (extra.before) extra.before(env);
  const from = w.book.calls.length;
  let ret = null, err = null;
  try { ret = plain(env.ctx.dp2SlotTick()); } catch (e) { err = String(e && e.message || e); }
  return { ret, err, calls: w.book.calls.slice(from), logs: env.logs.slice(), reads: Object.assign({}, env.reads) };
}
const state = w => (w.props.DP2_SLOT_DONE ? JSON.parse(w.props.DP2_SLOT_DONE) : null);
const built = w => (w.props.DP2_LAST_BUILT ? JSON.parse(w.props.DP2_LAST_BUILT) : null);
const has = (w, n) => !!w.book.getSheetByName(n);
const clears = (calls, n) => calls.filter(c => c === n + '|clear').length;


const S8 = o => JSON.stringify(Object.assign({ slot: '2026-11-02 08', pd: 0, t10: 0, tries: { pd: 0, t10: 0 } }, o));

/* ------------------------------------------------------------ scenarios (also the mutant signature) */
function scenarios(src) {
  const out = {};
  const e = makeEnv(src, { fetch: NO_NET }).ctx;
  const C = (hours, key, prev, st) => e.dp2SlotCarryCore_(hours, key, prev, st);
  out.pure = [
    C([8], '2026-11-02 09', '2026-11-02 08', S8({ pd: 1, tries: { pd: 1, t10: 0 } })), C([8], '2026-11-02 09', '2026-11-02 08', ''),
    C([8], '2026-11-02 09', '2026-11-02 08', S8({ pd: 1, t10: 1, tries: { pd: 1, t10: 1 } })),
    C([8], '2026-11-02 09', '2026-11-02 08', S8({ tries: { pd: 2, t10: 2 } })), C([8], '2026-11-02 09', '2026-11-02 08', S8({ pd: 1, tries: { pd: 1, t10: 2 } })),
    C([8], '2026-11-02 09', '2026-11-02 08', S8({ t10: 1, tries: { pd: 1, t10: 1 } })),
    C([8, 9], '2026-11-02 09', '2026-11-02 08', ''), C([7], '2026-11-02 09', '2026-11-02 08', ''),
    C([0], '2026-11-02 01', '2026-11-02 00', ''), C([8], '2026-11-02 09', '2026-11-02 08', JSON.stringify({ slot: '2026-11-02 07', pd: 1, t10: 1 })),
    e.dp2SlotCoreOk_()
  ];
  // P3 carry ticks
  const H8 = { DP2_REFRESH_HOURS: '8' };
  const w1 = world(src, Object.assign({ DP2_SLOT_DONE: S8({ pd: 1, tries: { pd: 1, t10: 0 } }), DP2_LAST_BUILT: JSON.stringify({ pd: 5, t10: 1 }) }, H8));
  const c1 = tick(w1, at(9, 5)), s1 = state(w1), b1 = built(w1), t1 = has(w1, T10), p1 = has(w1, PDV);
  const c2 = tick(w1, at(9, 15));
  const w2 = world(src, H8);                                             // an empty slot: no tick ran in it
  const e1 = tick(w2, at(9, 5)), e2 = tick(w2, at(9, 15)), e3 = tick(w2, at(9, 25));
  const w3 = world(src, Object.assign({ DP2_SLOT_DONE: S8({ tries: { pd: 2, t10: 2 } }) }, H8));
  const u1 = tick(w3, at(9, 5));
  const w4 = world(src, Object.assign({ DP2_SLOT_DONE: S8({ pd: 1, tries: { pd: 1, t10: 0 } }),
                                        DT10_RUN_MARKER: JSON.stringify({ started: new Date(at(9, 5) - 2 * 60000).toISOString() }) }, H8));
  const r1 = tick(w4, at(9, 5));
  const w5 = world(src, { DP2_REFRESH_HOURS: '0' });
  const z1 = tick(w5, Date.UTC(2026, 10, 1, 22, 5, 0));                 // 01:05 Riyadh on 2 Nov: the 00:00 slot goes on
  const w6 = world(src, Object.assign({ DP2_SLOT_DONE: S8({ pd: 1, tries: { pd: 1, t10: 0 } }) }, H8));
  const n1 = tick(w6, at(10, 5));                                        // 10:00: the hour before (9) is no slot hour
  out.carry = [c1.ret, s1, b1 && b1.t10, t1, p1, c1.logs.filter(l => /^v2 refresh slot/.test(l)), c2.ret,
               e1.ret, e2.ret, e3.ret, state(w2), u1.ret, Object.keys(u1.reads).sort(), r1.ret, z1.ret, state(w5), n1.ret, Object.keys(n1.reads).sort()];
  return out;
}

/* ------------------------------------------------------------ checks */
function checkOracle() {
  eq(sha256(OLD), V1300_SHA256, 'base SHA-256');
  ok(NEW.indexOf("var DP2_VERSION = '1.3.1';   // lockstep with the header") > 0 && /^ \* Version: 1\.3\.1 \(2026-11-11\)$/.test(NEW.split('\n')[3]), 'version lockstep');
  new vm.Script(NEW); new vm.Script(OLD);
  const names = (s, re) => { const o = {}; let m; while ((m = re.exec(s)) !== null) o[m[1]] = true; return o; };
  const fb = names(OLD, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), fn = names(NEW, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
  deq(Object.keys(fb).filter(k => !fn[k]), [], 'functions removed');
  deq(Object.keys(fn).filter(k => !fb[k]).sort(), ['dp2SlotCarryCore_'], 'functions added');
  const vb = names(OLD, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = names(NEW, /^var\s+([A-Za-z_$][\w$]*)/mg);
  deq(Object.keys(vb).filter(k => !vn[k]), [], 'vars removed');
  deq(Object.keys(vn).filter(k => !vb[k]), [], 'vars added');
  eq(EDITS.length, 6, 'hunk count');
  for (const [cur] of EDITS) eq(NEW.split(cur).length - 1, 1, 'each current is unique: ' + cur.split('\n')[0].slice(0, 80));
  eq(sha256(reverseEdits(NEW)), V1300_SHA256, 'reversal of the delivered file');
}
function checkGoldenNegative(parts) {
  for (const key of ['today', 'research']) for (const which of ['both', 't10', 'pd']) {
    const n = build(NEW, which, { board: BOARDS[key], fetch: NO_NET }), o = build(OLD, which, { board: BOARDS[key], fetch: NO_NET });
    deq(norm(n), norm(o), which + ' ' + key);
  }
  // every tick inside a slot hour is that of v1.3.0
  const run = s => {
    const w = world(s, Object.assign({ DP2_LAST_BUILT: JSON.stringify({ pd: 2, t10: 1 }) }, HOURS));
    const r = [tick(w, at(8, 55)).ret, tick(w, at(8, 58)).ret, tick(w, at(8, 59)).ret, tick(w, at(9, 5)).ret, tick(w, at(9, 41)).ret];
    const w2 = world(s, HOURS); w2.book.load(PDV, [['SENTINEL']]); w2.book.calls.length = 0;
    r.push(tick(w2, at(8, 55), { DateCls: steppingDate(at(8, 55), 20000) }).ret, tick(w2, at(8, 56)).ret, state(w2));
    const w3 = world(s, {}, { triggers: [{ h: 'buildDecisionPagesV2', kind: 'hours', every: 1 }] });
    const e3 = makeEnv(s, { fetch: NO_NET, book: w3.book, propsRef: w3.props, scriptApp: scriptAppShim(w3) });
    r.push(e3.ctx.dp2InstallSlotTrigger(), plain(w3.triggers), e3.ctx.dp2SlotStatus());
    return JSON.parse(JSON.stringify(r).split('1.3.1').join('v').split('1.3.0').join('v'));
  };
  const a = run(NEW), b = run(OLD);
  deq(a, b, 'slot-hour ticks, the late page and the installer = v1.3.0');
  parts.push(a);
}
function checkPure(sc, parts) {
  deq(sc.pure, ['2026-11-02 08', '2026-11-02 08', '', '', '', '2026-11-02 08', '', '', '2026-11-02 00', '2026-11-02 08', true], 'dp2SlotCarryCore_ and the core check');
  parts.push(sc.pure);
}
function checkCarry(sc, parts) {
  const [c1, s1, b1t10, t1, p1, log1, c2, e1, e2, e3, sw2, u1, ur, r1, z1, sw5, n1, nr] = sc.carry;
  deq([c1.act, c1.page, c1.slot, c1.why], ['built', 't10', '2026-11-02 08', 'carried over from the 08:00 slot'], 'the second page in the next hour');
  deq(s1, { slot: '2026-11-02 08', pd: 1, t10: 1, tries: { pd: 1, t10: 1 } }, 'the 08:00 slot is complete');
  eq(b1t10, at(9, 5), 'stamped'); ok(t1 && !p1, 'only Top_10_Investments_v2 drawn');
  deq(log1, ['v2 refresh slot 2026-11-02 08: ' + T10 + ' built - carried over from the 08:00 slot'], 'the log line');
  deq([c2.act, c2.why], ['idle', 'not a refresh hour'], 'nothing left: idle');
  deq([e1.act, e1.page, e2.act, e2.page, e3.act], ['built', 'pd', 'built', 't10', 'idle'], 'an empty slot: both pages, one per tick');
  deq(sw2, { slot: '2026-11-02 08', pd: 1, t10: 1, tries: { pd: 1, t10: 1 } }, 'empty slot completed');
  deq([u1.act, u1.why], ['idle', 'not a refresh hour'], 'tries used up');
  deq(ur, ['DP2_REFRESH_HOURS', 'DP2_SLOT_DONE'], 'with nothing to carry, the hours and the slot state are read, nothing else');
  deq([r1.act, r1.page, r1.why], ['wait', 't10', 'a Top_10 / Portfolio_Decision render is running'], 'a decision render still comes first');
  deq([z1.act, z1.page, z1.slot, z1.why], ['built', 'pd', '2026-11-02 00', 'carried over from the 00:00 slot'], 'hour 0 goes on at 01:00');
  eq(sw5.slot, '2026-11-02 00', 'the 00:00 slot state');
  deq([n1.act, n1.why], ['idle', 'not a refresh hour'], 'no carry two hours later');
  deq(nr, ['DP2_REFRESH_HOURS', 'DP2_SLOT_DONE'], 'reads');
  parts.push(sc.carry);
}
function checkSelfTest(parts) {
  const run = (s, triggers) => { const w = world(s, {}, { triggers }); const env = makeEnv(s, { fetch: NO_NET, book: w.book, propsRef: w.props, scriptApp: scriptAppShim(w) }); return plain(env.ctx.dp2SelfTest()); };
  const fold = x => x.name.split('1.3.1').join('v').split('1.3.0').join('v');
  for (const tr of [[], [{ h: 'dp2SlotTick', kind: 'minutes', every: 10 }]]) {
    const n = run(NEW, tr), o = run(OLD, tr);
    deq(n.checks.map(x => [fold(x), x.ok]), o.checks.map(x => [fold(x), x.ok]), 'self-test = v1.3.0 (' + tr.length + ' slot trigger)');
    parts.push(n.checks.length);
  }
}
function checkLint() {
  ok(NEW.indexOf('\r') < 0, 'LF only');
  for (const [cur, prev] of EDITS) {
    const added = cur.split('\n').filter(l => prev.split('\n').indexOf(l) < 0).join('\n');
    for (let i = 0; i < added.length; i++) ok(added.charCodeAt(i) < 128, 'ASCII only in the hunk: ' + JSON.stringify(added.slice(Math.max(0, i - 30), i + 10)));
    const code = added.split('\n').filter(l => !/^\s*(\*|\/\*)/.test(l)).join('\n');
    ok(!/=>|`|\blet\s|\bconst\s|\bclass\s|\.\.\.[A-Za-z_]/.test(code), 'ES5 in the hunk: ' + code.slice(0, 120));
  }
  const hdr = s => s.slice(0, s.indexOf(' */') + 3);
  const vs = hdr(NEW).split('\n').map(l => (/^ \*   (\d+\.\d+\.\d+)[ :(]/.exec(l) || [])[1]).filter(Boolean);
  eq(vs[0], '1.3.1', 'the newest WHY first'); eq(vs[1], '1.3.0', 'then 1.3.0');
  const oldLines = hdr(OLD).split('\n').filter(l => !/^ \* Version: /.test(l));
  for (const l of oldLines) ok(hdr(NEW).indexOf(l) >= 0, 'prior WHY kept: ' + l.slice(0, 90));
  eq(hdr(NEW).split('\n')[3], ' * Version: 1.3.1 (2026-11-11)', 'the version line');
}
const MUTANTS = [
  ['carry inside a slot hour', "if (hours.indexOf(+String(key).slice(11, 13)) >= 0 || hours.indexOf(+String(prevKey).slice(11, 13)) < 0) return '';", "if (hours.indexOf(+String(prevKey).slice(11, 13)) < 0) return '';"],
  ['carry after any hour', "if (hours.indexOf(+String(key).slice(11, 13)) >= 0 || hours.indexOf(+String(prevKey).slice(11, 13)) < 0) return '';", "if (hours.indexOf(+String(key).slice(11, 13)) >= 0) return '';"],
  ['tries ignored', "if (!st[pg] && st.tries[pg] < DP2_SLOT_MAX_TRIES_) return prevKey;", "if (!st[pg]) return prevKey;"],
  ['done ignored', "if (!st[pg] && st.tries[pg] < DP2_SLOT_MAX_TRIES_) return prevKey;", "if (st.tries[pg] < DP2_SLOT_MAX_TRIES_) return prevKey;"],
  ['only one page checked', "  for (var i = 0; i < 2; i++) {\n    var pg = i ? 't10' : 'pd';", "  for (var i = 0; i < 1; i++) {\n    var pg = i ? 't10' : 'pd';"],
  ['no carry', "    if (!carry) return { act: 'idle', page: '', why: 'not a refresh hour' };\n    key = carry; minute = 60;", "    return { act: 'idle', page: '', why: 'not a refresh hour' };"],
  ['carry waits for a render', "    key = carry; minute = 60;", "    key = carry;"],
  ['the wrong hour before', "Utilities.formatDate(new Date(nowMs - 3600000), DP2.TZ, 'yyyy-MM-dd HH')", "Utilities.formatDate(new Date(nowMs - 7200000), DP2.TZ, 'yyyy-MM-dd HH')"],
  ['no carry log', "  if (carry && plan.act === 'build') plan.why = 'carried over from the ' + carry.slice(11, 13) + ':00 slot';   // v1.3.1\n", '']
];

function battery() {
  const parts = [];
  let sc = null;
  test('P0 oracle: v1.3.0 rebuilt from the delivered file (SHA-256 pinned); version lockstep 1.3.1; both compile; inventory +1 function, 0 vars, 0 removed; 6 hunks reverse to the base', () => checkOracle());
  test('P1 GOLDEN NEGATIVE: menu builds, every tick inside a slot hour, the late page and the installer = v1.3.0', () => checkGoldenNegative(parts));
  test('P2 pure: dp2SlotCarryCore_ (pending, empty, done, tries used up, adjacent slots, hour 0, another slot) and dp2SlotCoreOk_', () => { sc = scenarios(NEW); checkPure(sc, parts); });
  test('P3 carry ticks: the second page at 09:05 for the 08:00 slot, an empty slot, tries used up, a decision render, 00:00 -> 01:05, none at 10:00', () => checkCarry(sc, parts));
  test('P4 dp2SelfTest = the v1.3.0 report, with and without the slot trigger', () => checkSelfTest(parts));
  test('P5 lint: LF; ASCII and ES5 in every hunk; the WHY newest-first; every prior WHY line kept', () => checkLint());
  test('P6 mutants: ' + MUTANTS.length + ' single-line mutants of the delivered file are each caught', () => {
    const want = JSON.stringify(sc);
    for (const [label, from, to] of MUTANTS) {
      eq(NEW.split(from).length - 1, 1, 'mutant anchor unique: ' + label);
      const M = NEW.replace(from, () => to);
      let caught = false;
      try { caught = JSON.stringify(scenarios(M)) !== want; } catch (eM) { caught = true; }
      ok(caught, 'mutant not caught: ' + label);
    }
    parts.push(want);
  });
  return sha256(JSON.stringify(parts));
}

const digests = [];
let total = 0;
for (let run = 1; run <= 2; run++) {
  passed = 0; failed = 0;
  const d = battery();
  digests.push(d);
  total = passed + failed;
  console.log('run ' + run + ': ' + passed + '/' + total + ' digest ' + d);
  if (failed) { console.log('FAIL ' + passed + '/' + total); process.exit(1); }
}
if (digests[0] !== digests[1]) { console.log('FAIL x2 digest mismatch ' + digests.join(' ')); process.exit(1); }
console.log('DP2 V1301 P0-P6 x2 identical digest ' + digests[0].slice(0, 16) + ' | new SHA256 ' + sha256(NEW).slice(0, 12) + ' | v1.3.0 SHA256 ' + sha256(OLD).slice(0, 12));
console.log('PASS ' + passed + '/' + total);
