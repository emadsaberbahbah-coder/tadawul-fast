#!/usr/bin/env node
/* tests/test_dp2_v1300_refresh_slots.js
 * Harness for apps_script/27_Decision_Pages_v2.gs v1.3.0 [REFRESH-SLOTS] (owner, Wed 11 Nov 2026 14:15 Riyadh: both v2
 * pages 10 times a day or more). One time trigger, dp2SlotTick every 10 minutes, installed by dp2InstallSlotTrigger: in
 * each slot hour (DP2_REFRESH_HOURS; default 7,9,11,13,15,17,19,21,23,0) each page is built once, one page per
 * execution, after the main page's render of that hour (or from minute 40), never beside a decision render or another
 * slot build, at most 2 tries; a build that reaches its drawing step late clears nothing.
 *
 * Two trees over the REAL file, each in its own vm context: the delivered source (NEW) and the v1.2.36 file (OLD,
 * SHA-256 pinned 487c0778 = origin/main 58910af), rebuilt from NEW by reversing the enumerated v1.3.0 hunks
 * (tests/js/dp2_edits_v1300.json). The GAS shim, the fixture and the boards are those of
 * tests/test_dp2_v1236_evidence_since.js, copied verbatim (makeEnv also takes a workbook, a property store, a clock and
 * a trigger store, so several ticks share one workbook; norm folds 1.3.0).
 *   P0 oracle, version lockstep, inventory (+12 functions, +11 line-start vars, 0 removed), 8 hunks reverse to the base
 *   P1 GOLDEN NEGATIVE: without the slot trigger every menu build (both, each page, hourly installer) = the v1.2.36 one
 *   P2 pure: hours, slot state, the main-page key, the drawing limit, the plan matrix, the core check
 *   P3 ticks: one page per execution, the older page first, the same page as the menu build, then idle in the slot
 *   P4 waits: outside the hours, a decision render, a fresh claim, no render this hour before minute 40, 'off'
 *   P5 a late drawing step keeps the previous page; the next tick builds it; two tries at most
 *   P6 a failing build: the try is counted, the claim cleared, the error rethrown
 *   P7 the installer, the hourly installer, the remover and the status
 *   P8 dp2SelfTest: the v1.2.36 report without the slot trigger; one 'refresh slots' line with it
 *   P9 lint: ES5; no non-ASCII character in a hunk; LF; WHY newest-first; every prior WHY kept
 *   P10 mutants
 * The battery runs twice and the two digests must match; the last line is 'PASS k/k' (exit 1 on any failure).
 * Usage: node tests/test_dp2_v1300_refresh_slots.js [new.gs] [v1.2.36.gs]
 */
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const vm = require('vm');
const ROOT = path.join(__dirname, '..');
const NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '27_Decision_Pages_v2.gs');
const OLD_PATH = process.argv[3] || '';
// 2026-11-11: v1.3.1 is delivered; this harness proves the v1.3.0 diff, so NEW is the v1.3.0 file rebuilt from the
// delivered one (tests/js/dp2_history.js: the v1.3.1 hunks reversed, SHA-256 pinned).
const NEW = require(path.join(__dirname, 'js', 'dp2_history.js')).v1300Source(fs.readFileSync(NEW_PATH, 'utf8'));
const FIX = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'fixtures', 'dp2_board_2026-11-02.json'), 'utf8'));
const EDITS = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'dp2_edits_v1300.json'), 'utf8'));
const V1236_SHA256 = '487c0778e1f509eb9ee60e91d4603ed884367719af1b065e6231b723bb1f10f3';   // v1.2.36 (origin/main 58910af)
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
    eq(s.split(cur).length - 1, 1, 'exactly one approved v1.3.0 hunk: ' + cur.split('\n')[0].slice(0, 90));
    s = s.replace(cur, () => prev);
  }
  eq(sha256(s), V1236_SHA256, 'v1.3.0 changes only its enumerated hunks (their reversal = the v1.2.36 file)');
  return s;
}
let OLD = null, oldErr = '';
try {
  OLD = OLD_PATH ? fs.readFileSync(OLD_PATH, 'utf8') : reverseEdits(NEW);
  eq(sha256(OLD), V1236_SHA256, 'v1.2.36 SHA-256');
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
  for (const v of ['1.3.0', '1.2.36', '1.2.35', '1.2.34', '1.2.33', '1.2.32', '1.2.31']) s = s.split('Decision Pages v2 ' + v).join('Decision Pages v2 1.2.30');
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

/* ------------------------------------------------------------ scenarios (also the mutant signature) */
function scenarios(src) {
  const out = {};
  const e = makeEnv(src, { fetch: NO_NET }).ctx;
  const K = '2026-11-02 08';
  out.pure = [
    e.dp2SlotHoursCore_('').join(','), e.dp2SlotHoursCore_(null).join(','), e.dp2SlotHoursCore_(' OFF ').length, e.dp2SlotHoursCore_('none').length,
    e.dp2SlotHoursCore_(' 23, 7;7 x 25 -1 ').join(','), e.dp2SlotHoursCore_('x y').join(','), e.dp2SlotHoursCore_('0 12').join(','),
    plain(e.dp2SlotStateCore_('', K)), plain(e.dp2SlotStateCore_('{"slot":"2026-11-02 07","pd":1}', K)), plain(e.dp2SlotStateCore_('junk', K)),
    plain(e.dp2SlotStateCore_('{"slot":"2026-11-02 08","pd":true,"t10":0,"tries":{"pd":"1"}}', K)),
    e.dp2SlotMainKeyCore_([['TOP'], ['Status', 'Last run 2026-11-02 08:49:07 | status: ok | output: EXECUTABLE']]),
    e.dp2SlotMainKeyCore_([['Status', 'Last run 2026-11-02 08:49:07 | status: OK']]),
    e.dp2SlotMainKeyCore_([['Status', 'Last run 2026-11-02 08:49:07 | status: running' + ELL]]),
    e.dp2SlotMainKeyCore_([['Status', 'Last run 2026-11-02 08:49:07 | status: failed']]),
    e.dp2SlotMainKeyCore_([[''], [''], [''], [''], [''], [''], ['Last run 2026-11-02 08:49:07 | status: ok']]),
    e.dp2SlotMainKeyCore_([['Status', 'Last run 2026-11-02 | status: ok']]), e.dp2SlotMainKeyCore_([]),
    e.dp2SlotDrawMaxMsCore_(''), e.dp2SlotDrawMaxMsCore_(null), e.dp2SlotDrawMaxMsCore_(' 90 '), e.dp2SlotDrawMaxMsCore_('30'), e.dp2SlotDrawMaxMsCore_('330'),
    e.dp2SlotDrawMaxMsCore_('29'), e.dp2SlotDrawMaxMsCore_('331'), e.dp2SlotDrawMaxMsCore_('abc'),
    e.dp2SlotCoreOk_()
  ];
  const P = (min, st, mk, busy, claim, builtRaw, hours) => plain(e.dp2SlotPlanCore_(hours || [8, 9], K, min, st, mk, busy, claim, builtRaw || ''));
  const S = o => JSON.stringify(Object.assign({ slot: K, pd: 0, t10: 0, tries: { pd: 0, t10: 0 } }, o));
  out.plan = [
    P(10, '', K, false, null), P(10, '', K, false, null, JSON.stringify({ pd: 5, t10: 3 })), P(10, S({ pd: 1 }), K, false, null),
    P(10, S({ pd: 1, t10: 1 }), K, false, null), P(10, S({ tries: { pd: 2, t10: 2 } }), K, false, null), P(10, S({ tries: { pd: 2, t10: 1 } }), K, false, null),
    P(10, S({ tries: { pd: 1, t10: 0 } }), K, false, null), P(10, '', K, true, null), P(10, '', K, false, 6.9), P(10, '', K, false, 7), P(10, '', K, false, -1),
    P(39, '', '2026-11-02 07', false, null), P(40, '', '2026-11-02 07', false, null), P(39, '', '', false, null),
    P(10, '', K, false, null, '', [9]), P(10, '', K, false, null, '', []), P(10, S({ slot: '2026-11-02 07', pd: 1, t10: 1 }), K, false, null)
  ];
  // P3 ticks: the older page first, one page per execution, then idle in the slot
  const w = world(src, Object.assign({ DP2_LAST_BUILT: JSON.stringify({ pd: 2, t10: 1 }) }, HOURS));
  const a1 = tick(w, at(8, 55));
  const s1 = { ret: a1.ret, st: state(w), t10: has(w, T10), pd: has(w, PDV), claim: w.props.DP2_SLOT_CLAIM, b: built(w), log: a1.logs.filter(l => /^v2 refresh slot/.test(l)) };
  const a2 = tick(w, at(8, 58));
  const s2 = { ret: a2.ret, st: state(w), pd: has(w, PDV), b: built(w), t10clear: clears(a2.calls, T10) };
  const a3 = tick(w, at(8, 59));
  const a4 = tick(w, at(9, 5));
  out.ticks = [s1, s2, { ret: a3.ret, calls: a3.calls.length }, { ret: a4.ret, calls: a4.calls.length }];
  // P4 waits
  const w0 = world(src, HOURS);
  const o1 = tick(w0, at(10, 55));
  out.idle = { ret: o1.ret, reads: o1.reads, calls: o1.calls.length };
  const wb = world(src, Object.assign({ DT10_RUN_MARKER: JSON.stringify({ started: isoAgo(at(8, 55), 2) }) }, HOURS));
  const ob = tick(wb, at(8, 55));
  const wc = world(src, Object.assign({ DP2_SLOT_CLAIM: String(at(8, 52)) }, HOURS));
  const oc = tick(wc, at(8, 55));
  const wc2 = world(src, Object.assign({ DP2_SLOT_CLAIM: String(at(8, 47)) }, HOURS));
  const oc2 = tick(wc2, at(8, 55));
  const wr = world(src, HOURS, { status: 'Last run 2026-11-02 09:05:00 | status: running' + ELL });
  const or1 = tick(wr, at(9, 24)), or2 = tick(wr, at(9, 41));
  const wo = world(src, { DP2_REFRESH_HOURS: 'off' });
  const oo = tick(wo, at(9, 55));
  const wd = world(src, {});
  const od = tick(wd, at(9, 24));
  out.waits = [ob.ret, has(wb, PDV) || has(wb, T10), state(wb), oc.ret, state(wc), oc2.ret.act, oc2.ret.page, or1.ret, or2.ret.act, or2.ret.why, state(wr), oo.ret, od.ret];
  // P5 a late drawing step keeps the previous page; the next tick builds it; two tries at most
  const wl = world(src, HOURS);
  wl.book.load(PDV, [['SENTINEL page of the previous build']]);
  wl.book.calls.length = 0;
  const l1 = tick(wl, at(8, 55), { DateCls: steppingDate(at(8, 55), 20000) });
  const kept = { ret: l1.ret, sentinel: String(wl.book.getSheetByName(PDV)._cells.get('1:1').v), clears: clears(l1.calls, PDV), st: state(wl), claim: wl.props.DP2_SLOT_CLAIM, b: built(wl) };
  const l2 = tick(wl, at(8, 56));
  const other = { act: l2.ret.act, page: l2.ret.page, sentinel: String(wl.book.getSheetByName(PDV)._cells.get('1:1').v), st: state(wl) };
  const l3 = tick(wl, at(8, 57));
  const second = { act: l3.ret.act, page: l3.ret.page, gone: !wl.book.getSheetByName(PDV)._cells.get('1:1') || String(wl.book.getSheetByName(PDV)._cells.get('1:1').v) !== kept.sentinel, st: state(wl), other };
  const wt = world(src, Object.assign({ DP2_SLOT_DONE: JSON.stringify({ slot: '2026-11-02 08', pd: 0, t10: 0, tries: { pd: 1, t10: 2 } }) }, HOURS));
  wt.book.load(PDV, [['SENTINEL']]);
  const t1 = tick(wt, at(8, 55), { DateCls: steppingDate(at(8, 55), 20000) });
  const t2 = tick(wt, at(8, 57));
  const wlim = world(src, Object.assign({ DP2_SLOT_DRAW_START_MAX_S: '330' }, HOURS));
  const lim = tick(wlim, at(8, 55), { DateCls: steppingDate(at(8, 55), 1000) });
  out.late = [kept, second, t1.ret.act, t2.ret, state(wt), lim.ret.act];
  // P6 a failing build
  const wf = world(src, Object.assign({ DP2_LAST_BUILT: JSON.stringify({ pd: 2, t10: 1 }) }, HOURS));
  const f1 = tick(wf, at(8, 55), { before: env => { env.ctx.buildTop10V2 = () => { throw new Error('synthetic build failure'); }; } });
  out.fail = [f1.err, f1.ret, state(wf), wf.props.DP2_SLOT_CLAIM, built(wf), f1.logs.filter(l => /^v2 refresh slot/.test(l))];
  // P7 the installers, the remover, the status
  const wi = world(src, {}, { triggers: [{ h: 'buildDecisionPagesV2', kind: 'hours', every: 1 }, { h: 'dp2SlotTick', kind: 'minutes', every: 5 }, { h: 'tfbDecisionChainTick', kind: 'minutes', every: 5 }] });
  const ie = makeEnv(src, { fetch: NO_NET, book: wi.book, propsRef: wi.props, now: at(9, 0), scriptApp: scriptAppShim(wi) });
  const msg = ie.ctx.dp2InstallSlotTrigger();
  const afterInstall = plain(wi.triggers);
  const st0 = ie.ctx.dp2SlotStatus();
  ie.ctx.dp2InstallTrigger();
  const afterHourly = plain(wi.triggers);
  ie.ctx.dp2InstallSlotTrigger();
  const rm = ie.ctx.dp2RemoveSlotTrigger(), rm2 = ie.ctx.dp2RemoveSlotTrigger();
  wi.props.DP2_REFRESH_HOURS = '8 9';
  wi.props.DP2_LAST_BUILT = JSON.stringify({ pd: at(8, 55) });
  const st1 = ie.ctx.dp2SlotStatus();
  out.install = [msg, afterInstall, st0, afterHourly, rm, rm2, plain(wi.triggers), st1, wi.book.toasts.map(t => t[1])];
  // a menu build on a moving clock is never 'late' (the guard is for slot builds only)
  out.manual = [build(src, 't10', { board: BOARDS.today, fetch: NO_NET, DateCls: steppingDate(at(9, 0), 5000) }).ret.rows > 0,
                build(src, 'pd', { board: BOARDS.today, fetch: NO_NET, DateCls: steppingDate(at(9, 0), 5000) }).ret.rows > 0];
  return out;
}

/* ------------------------------------------------------------ checks */
function checkOracle() {
  eq(sha256(OLD), V1236_SHA256, 'base SHA-256');
  ok(NEW.indexOf("var DP2_VERSION = '1.3.0';   // lockstep with the header") > 0 && /^ \* Version: 1\.3\.0 \(2026-11-11\)$/.test(NEW.split('\n')[3]), 'version lockstep');
  new vm.Script(NEW); new vm.Script(OLD);
  const names = (s, re) => { const o = {}; let m; while ((m = re.exec(s)) !== null) o[m[1]] = true; return o; };
  const fb = names(OLD, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), fn = names(NEW, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
  deq(Object.keys(fb).filter(k => !fn[k]), [], 'functions removed');
  deq(Object.keys(fn).filter(k => !fb[k]).sort(), ['dp2InstallSlotTrigger', 'dp2RemoveSlotTrigger', 'dp2RemoveSlotTrigger_', 'dp2SlotCoreOk_', 'dp2SlotDrawMaxMsCore_',
    'dp2SlotHoursCore_', 'dp2SlotMainKeyCore_', 'dp2SlotPlanCore_', 'dp2SlotStateCore_', 'dp2SlotStatus', 'dp2SlotTick', 'dp2SlotTooLate_'], 'functions added');
  const vb = names(OLD, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = names(NEW, /^var\s+([A-Za-z_$][\w$]*)/mg);
  deq(Object.keys(vb).filter(k => !vn[k]), [], 'vars removed');
  deq(Object.keys(vn).filter(k => !vb[k]).sort(), ['DP2_SLOT_CLAIM_PROP_', 'DP2_SLOT_DEFAULT_', 'DP2_SLOT_DONE_PROP_', 'DP2_SLOT_DRAW_DEFAULT_S_', 'DP2_SLOT_DRAW_PROP_',
    'DP2_SLOT_EVERY_MIN_', 'DP2_SLOT_FALLBACK_MIN_', 'DP2_SLOT_HANDLER_', 'DP2_SLOT_HOURS_PROP_', 'DP2_SLOT_MAX_TRIES_', 'DP2_SLOT_RUN_'], 'vars added (line starts)');
  ok(NEW.indexOf('var DP2_SLOT_RUN_ = false, DP2_SLOT_DRAW_MS_ = 0;') > 0, 'DP2_SLOT_DRAW_MS_ shares a line');
  eq(EDITS.length, 8, 'hunk count');
  for (const [cur] of EDITS) eq(NEW.split(cur).length - 1, 1, 'each current is unique: ' + cur.split('\n')[0].slice(0, 80));
  eq(sha256(reverseEdits(NEW)), V1236_SHA256, 'reversal of the delivered file');
}
function checkGoldenNegative(parts) {
  const PROPS = [{}, Object.assign({ DP2_SLOT_DRAW_START_MAX_S: '30', DP2_SLOT_DONE: '{"slot":"2026-11-02 09","pd":0,"t10":0,"tries":{"pd":2,"t10":2}}' }, HOURS)];
  for (const props of PROPS) {
    for (const key of ['today', 'research']) {
      for (const which of ['both', 't10', 'pd']) {
        const n = build(NEW, which, { board: BOARDS[key], props, fetch: NO_NET }), o = build(OLD, which, { board: BOARDS[key], props, fetch: NO_NET });
        deq(norm(n), norm(o), which + ' ' + key + ' ' + JSON.stringify(props).slice(0, 60));
        if (key === 'today' && which === 'both' && !Object.keys(props).length) parts.push(norm(n).ret);
      }
    }
  }
  // the hourly installer, with no slot trigger installed: the same triggers and notice as v1.2.36
  const run = s => { const w = world(s, {}, { triggers: [{ h: 'buildDecisionPagesV2', kind: 'hours', every: 1 }] }); const env = makeEnv(s, { fetch: NO_NET, book: w.book, propsRef: w.props, scriptApp: scriptAppShim(w) }); const r = env.ctx.dp2InstallTrigger(); return [r, plain(w.triggers), w.book.toasts.map(t => t[0])]; };
  const fold = x => JSON.parse(JSON.stringify(x).split('1.3.0').join('v').split('1.2.36').join('v'));
  deq(fold(run(NEW)), fold(run(OLD)), 'dp2InstallTrigger without a slot trigger');
}
function checkPure(sc, parts) {
  const p = sc.pure, D = '0,7,9,11,13,15,17,19,21,23';
  deq(p.slice(0, 7), [D, D, 0, 0, '7,23', D, '0,12'], 'hours');
  deq(p[7], { slot: '2026-11-02 08', pd: 0, t10: 0, tries: { pd: 0, t10: 0 } }, 'empty state');
  deq(p[8], p[7], 'another slot starts empty'); deq(p[9], p[7], 'junk starts empty');
  deq(p[10], { slot: '2026-11-02 08', pd: 1, t10: 0, tries: { pd: 1, t10: 0 } }, 'a state is normalised');
  deq(p.slice(11, 18), ['2026-11-02 08', '2026-11-02 08', '', '', '', '', ''], 'the main-page key');
  deq(p.slice(18, 26), [150000, 150000, 90000, 30000, 330000, 150000, 150000, 150000], 'the drawing limit');
  eq(p[26], true, 'dp2SlotCoreOk_');
  const acts = sc.plan.map(x => x.act + ':' + x.page);
  deq(acts, ['build:pd', 'build:t10', 'build:t10', 'idle:', 'idle:', 'build:t10', 'build:t10', 'wait:pd', 'wait:pd', 'build:pd', 'build:pd', 'wait:pd', 'build:pd', 'wait:pd', 'idle:', 'idle:', 'build:pd'], 'the plan matrix');
  eq(sc.plan[3].why, 'both pages built in this slot'); eq(sc.plan[4].why, 'the tries of this slot are used up');
  eq(sc.plan[7].why, 'a Top_10 / Portfolio_Decision render is running'); eq(sc.plan[8].why, 'a v2 build started 6 min ago');
  eq(sc.plan[11].why, 'waiting for the main-page render of this hour (until minute 40)');
  eq(sc.plan[12].why, 'no finished main-page render this hour; built from minute 40'); eq(sc.plan[0].why, 'after the main-page render of this hour');
  eq(sc.plan[14].why, 'not a refresh hour (9)'); eq(sc.plan[15].why, 'not a refresh hour (off)');
  parts.push(p, acts);
}
function checkTicks(sc, parts) {
  const [s1, s2, s3, s4] = sc.ticks;
  deq([s1.ret.act, s1.ret.page, s1.ret.slot, s1.ret.why], ['built', 't10', '2026-11-02 08', 'after the main-page render of this hour'], 'tick 1 builds the older page');
  ok(s1.ret.rows > 0, 'tick 1 rows');
  ok(s1.t10 && !s1.pd, 'one page per execution');
  deq(s1.st, { slot: '2026-11-02 08', pd: 0, t10: 1, tries: { pd: 0, t10: 1 } }, 'tick 1 state');
  eq(s1.claim, '', 'the claim is cleared'); eq(s1.b.t10, at(8, 55), 'stamped'); eq(s1.b.pd, 2, 'the other stamp kept');
  deq(s1.log, ['v2 refresh slot 2026-11-02 08: ' + T10 + ' built - after the main-page render of this hour'], 'the log line');
  deq([s2.ret.act, s2.ret.page], ['built', 'pd'], 'tick 2 builds the other page');
  ok(s2.pd && s2.t10clear === 0, 'tick 2 leaves Top_10_Investments_v2 alone');
  deq(s2.st, { slot: '2026-11-02 08', pd: 1, t10: 1, tries: { pd: 1, t10: 1 } }, 'tick 2 state');
  eq(s2.b.pd, at(8, 58), 'pd stamped');
  deq([s3.ret.act, s3.ret.why, s3.calls], ['idle', 'both pages built in this slot', 0], 'tick 3 idle');
  deq([s4.ret.act, s4.ret.page, s4.ret.slot], ['wait', 't10', '2026-11-02 09'], 'the next slot waits for its render');
  // the page a tick builds is the page the menu builds at that moment
  const w = world(NEW, Object.assign({ DP2_LAST_BUILT: JSON.stringify({ pd: 2, t10: 1 }) }, HOURS));
  tick(w, at(8, 55));
  const menu = build(NEW, 't10', { board: BOARDS.today, fetch: NO_NET, now: at(8, 55), props: Object.assign({ DP2_LAST_BUILT: JSON.stringify({ pd: 2, t10: 1 }) }, HOURS) });
  deq(w.book.getSheetByName(T10)._state(), menu.sheets[T10], 'the slot-built Top_10_Investments_v2 = the menu-built one');
  parts.push(sc.ticks);
}
function checkWaits(sc, parts) {
  deq([sc.idle.ret.act, sc.idle.ret.why, sc.idle.calls], ['idle', 'not a refresh hour', 0], 'outside the hours');
  deq(Object.keys(sc.idle.reads), ['DP2_REFRESH_HOURS'], 'outside the hours only the hours are read');
  const [busy, anyPage, busySt, claim, claimSt, staleAct, stalePage, run24, run41act, run41why, runSt, off, dflt] = sc.waits;
  deq([busy.act, busy.why], ['wait', 'a Top_10 / Portfolio_Decision render is running'], 'a decision render');
  eq(anyPage, false, 'no page while waiting'); eq(busySt, null, 'no try while waiting');
  deq([claim.act, claim.why], ['wait', 'a v2 build started 3 min ago'], 'a fresh claim'); eq(claimSt, null, 'no try');
  deq([staleAct, stalePage], ['built', 'pd'], 'a claim of 8 minutes is a stopped build');
  deq([run24.act, run24.why], ['wait', 'waiting for the main-page render of this hour (until minute 40)'], 'a running render, minute 24');
  deq([run41act, run41why], ['built', 'no finished main-page render this hour; built from minute 40'], 'minute 41 builds');
  deq(runSt, { slot: '2026-11-02 09', pd: 1, t10: 0, tries: { pd: 1, t10: 0 } }, 'one page');
  deq([off.act, off.why], ['idle', 'not a refresh hour'], 'off');
  deq([dflt.act, dflt.page, dflt.slot], ['wait', 'pd', '2026-11-02 09'], 'the default hours include 9; the fixture render is of 08');
  parts.push(sc.waits);
}
function checkLate(sc, parts) {
  const [kept, second, t1, t2, tst, lim] = sc.late;
  eq(kept.ret.act, 'kept', 'a late drawing step');
  ok(/^its drawing step would have started at \d+ s \(limit 150 s\); the previous page stays and the next tick tries again$/.test(kept.ret.why), 'the why: ' + kept.ret.why);
  eq(kept.sentinel, 'SENTINEL page of the previous build', 'the previous page stays'); eq(kept.clears, 0, 'nothing cleared');
  deq(kept.st, { slot: '2026-11-02 08', pd: 0, t10: 0, tries: { pd: 1, t10: 0 } }, 'the try is counted, the page not done');
  eq(kept.claim, '', 'the claim is cleared'); eq(kept.b, null, 'no stamp');
  deq([second.other.act, second.other.page, second.other.sentinel], ['built', 't10', 'SENTINEL page of the previous build'], 'the page not yet tried goes before the retry (Codex P2 on #961)');
  deq(second.other.st, { slot: '2026-11-02 08', pd: 0, t10: 1, tries: { pd: 1, t10: 1 } }, 'the other page built');
  deq([second.act, second.page, second.gone], ['built', 'pd', true], 'then the retry builds it');
  deq(second.st, { slot: '2026-11-02 08', pd: 1, t10: 1, tries: { pd: 2, t10: 1 } }, 'second try');
  eq(t1, 'kept', 'the last try kept the page');
  deq([t2.act, t2.why], ['idle', 'the tries of this slot are used up'], 'two tries at most');
  deq(tst, { slot: '2026-11-02 08', pd: 0, t10: 0, tries: { pd: 2, t10: 2 } }, 'tries used up');
  eq(lim, 'built', 'a 330 s limit with a slow clock still draws');
  parts.push(sc.late);
}
function checkFail(sc, parts) {
  const [err, ret, st, claim, b, log] = sc.fail;
  eq(err, 'synthetic build failure', 'the error is rethrown'); eq(ret, null, 'no return');
  deq(st, { slot: '2026-11-02 08', pd: 0, t10: 0, tries: { pd: 0, t10: 1 } }, 'the try is counted');
  eq(claim, '', 'the claim is cleared'); deq(b, { pd: 2, t10: 1 }, 'no stamp');
  deq(log, ['v2 refresh slot 2026-11-02 08: ' + T10 + ' failed - synthetic build failure'], 'logged');
  parts.push(sc.fail);
}
function checkManual(sc, parts) {
  deq(sc.manual, [true, true], 'a menu build on a moving clock draws its page');
  parts.push(sc.manual);
}
function checkInstall(sc, parts) {
  const [msg, afterInstall, st0, afterHourly, rm, rm2, left, st1, titles] = sc.install;
  eq(msg, 'v2 refresh slots installed: dp2SlotTick checks every 10 min and builds each v2 page once in each slot hour, one page per execution, after the main-page render of that hour.\n' +
    'Slot hours (Riyadh, Script Property DP2_REFRESH_HOURS): 0, 7, 9, 11, 13, 15, 17, 19, 21, 23 = 10 a day per page.\nRemoved the hourly buildDecisionPagesV2 trigger (1).\nReplaced 1 earlier slot trigger(s).', 'the notice');
  deq(afterInstall, [{ h: 'tfbDecisionChainTick', kind: 'minutes', every: 5 }, { h: 'dp2SlotTick', kind: 'minutes', every: 10 }], 'one slot trigger, every 10 min; the hourly one removed; others kept');
  eq(st0, 'ON: 1 dp2SlotTick trigger(s), every 10 min\nSlot hours (Riyadh): 0, 7, 9, 11, 13, 15, 17, 19, 21, 23 = 10 a day per page (default)\nLast slot: none\nLast builds: ' + PDV + ' never ' + DOT + ' ' + T10 + ' never\nDrawing-step limit: 150 s', 'status');
  deq(afterHourly, [{ h: 'tfbDecisionChainTick', kind: 'minutes', every: 5 }, { h: 'buildDecisionPagesV2', kind: 'hours', every: 1 }], 'the hourly installer removes the slot trigger');
  deq([rm, rm2], [1, 0], 'the remover');
  deq(left, [{ h: 'tfbDecisionChainTick', kind: 'minutes', every: 5 }], 'only the other trigger is left');
  eq(st1, 'OFF: 0 dp2SlotTick trigger(s), every 10 min\nSlot hours (Riyadh): 8, 9 = 2 a day per page\nLast slot: none\nLast builds: ' + PDV + ' 2026-11-02 08:55 ' + DOT + ' ' + T10 + ' never\nDrawing-step limit: 150 s', 'status after');
  ok(titles.indexOf('Decision Pages v2 1.3.0') >= 0 && titles.indexOf('Decision Pages v2 1.3.0 - refresh slots') >= 0, 'notice titles');
  parts.push(sc.install);
}
function checkSelfTest(parts) {
  const run = (s, triggers) => { const w = world(s, {}, { triggers }); const env = makeEnv(s, { fetch: NO_NET, book: w.book, propsRef: w.props, scriptApp: scriptAppShim(w) }); return plain(env.ctx.dp2SelfTest()); };
  const fold = x => x.name.split('1.3.0').join('v').split('1.2.36').join('v');
  const n0 = run(NEW, []), o0 = run(OLD, []);
  deq(n0.checks.map(x => [fold(x), x.ok]), o0.checks.map(x => [fold(x), x.ok]), 'without the slot trigger: the v1.2.36 report');
  const n1 = run(NEW, [{ h: 'dp2SlotTick', kind: 'minutes', every: 10 }]);
  eq(n1.checks.length, o0.checks.length + 1, 'one more line');
  const i = n1.checks.findIndex(x => /^refresh slots/.test(x.name));
  ok(i > 0 && /^price history fetch/.test(n1.checks[i + 1].name), 'before the price-history line');
  eq(n1.checks[i].name, 'refresh slots (dp2SlotTick every 10 min): 0,7,9,11,13,15,17,19,21,23 ' + DOT + ' core ok (one page per execution, after the main-page render of the hour)', 'the line');
  eq(n1.checks[i].ok, true, 'ok');
  deq(n1.checks.filter((x, j) => j !== i).map(x => [fold(x), x.ok]), o0.checks.map(x => [fold(x), x.ok]), 'every other line = v1.2.36');
  parts.push(n1.checks[i]);
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
  eq(vs[0], '1.3.0', 'the newest WHY first');
  eq(vs[1], '1.2.36', 'then 1.2.36');
  const oldLines = hdr(OLD).split('\n').filter(l => !/^ \* Version: /.test(l));
  for (const l of oldLines) ok(hdr(NEW).indexOf(l) >= 0, 'prior WHY kept: ' + l.slice(0, 90));
  eq(hdr(NEW).split('\n')[3], ' * Version: 1.3.0 (2026-11-11)', 'the version line');
}
const MUTANTS = [
  ['the fallback minute', 'if (mainKey !== key && minute < DP2_SLOT_FALLBACK_MIN_)', 'if (mainKey !== key && minute < 0)'],
  ['the main render ignored', 'if (mainKey !== key && minute < DP2_SLOT_FALLBACK_MIN_)', 'if (minute < DP2_SLOT_FALLBACK_MIN_)'],
  ['one more try', 'if (!st[pg] && st.tries[pg] < DP2_SLOT_MAX_TRIES_)', 'if (!st[pg] && st.tries[pg] <= DP2_SLOT_MAX_TRIES_)'],
  ['the claim window', 'claimAgeMin >= 0 && claimAgeMin < DP2_RP_BUSY_MIN_', 'claimAgeMin >= 0 && claimAgeMin < 1'],
  ['a decision render ignored', "  if (busy) return { act: 'wait'", "  if (false) return { act: 'wait'"],
  ['never late', 'function dp2SlotTooLate_() { return DP2_SLOT_RUN_ && dp2RunElapsed_() > DP2_SLOT_DRAW_MS_; }', 'function dp2SlotTooLate_() { return false; }'],
  ['always late outside a slot', 'function dp2SlotTooLate_() { return DP2_SLOT_RUN_ && dp2RunElapsed_() > DP2_SLOT_DRAW_MS_; }', 'function dp2SlotTooLate_() { return dp2RunElapsed_() > DP2_SLOT_DRAW_MS_; }'],
  ['off ignored', "if (s === 'off' || s === 'none') return [];", "if (s === 'none') return [];"],
  ['hour 24', 'if (h >= 0 && h <= 23 && out.indexOf(h) < 0) out.push(h);', 'if (h >= 0 && h <= 25 && out.indexOf(h) < 0) out.push(h);'],
  ['the try after the build', "  st.tries[plan.page]++;\n  p.setProperty(DP2_SLOT_DONE_PROP_, JSON.stringify(st));\n", "  st.tries[plan.page]++;\n"],
  ['the claim kept', "  try { p.setProperty(DP2_SLOT_CLAIM_PROP_, ''); } catch (eC) {}\n", ''],
  ['no stamp', '    dp2RpStamp_(plan.page, new Date().getTime());\n', ''],
  ['every 15 min', 'ScriptApp.newTrigger(DP2_SLOT_HANDLER_).timeBased().everyMinutes(DP2_SLOT_EVERY_MIN_).create();', 'ScriptApp.newTrigger(DP2_SLOT_HANDLER_).timeBased().everyMinutes(15).create();'],
  ['the hourly trigger kept', 'var hourly = dp2RemoveTrigger(), old = dp2RemoveSlotTrigger_();', 'var hourly = 0, old = dp2RemoveSlotTrigger_();'],
  ['two schedules', '  dp2RemoveSlotTrigger_();   // v1.3.0: one schedule at a time\n', ''],
  ['any status finished', "return (m && st && st[1].toLowerCase() === 'ok') ? m[1] + ' ' + m[2] : '';", "return (m && st) ? m[1] + ' ' + m[2] : '';"],
  ['no guard on Portfolio_Decision_v2', "  if (dp2SlotTooLate_()) return { rows: 0, late: true, ms: dp2RunElapsed_() };   // v1.3.0 [REFRESH-SLOTS]\n  var sh = dp2Sheet_(DP2.SHEET_PD, true);", '  var sh = dp2Sheet_(DP2.SHEET_PD, true);'],
  ['the error swallowed', '  if (err) throw err;\n  return out;', '  return out;'],
  ['a retry before an untried page', '  var due = fresh.concat(retry);', '  var due = retry.concat(fresh);'],
  ['a wider drawing limit', 'if (!isFinite(n) || n < 30 || n > 330) n = DP2_SLOT_DRAW_DEFAULT_S_;', 'if (!isFinite(n) || n < 30 || n > 600) n = DP2_SLOT_DRAW_DEFAULT_S_;']
];

function battery() {
  const parts = [];
  let sc = null;
  test('P0 oracle: v1.2.36 rebuilt from the delivered file (SHA-256 pinned); version lockstep 1.3.0; both compile; inventory +12 functions +11 line-start vars, 0 removed; 8 hunks reverse to the base', () => checkOracle());
  test('P1 GOLDEN NEGATIVE: without the slot trigger every menu build (both, each page; two boards; with and without the slot properties) and the hourly installer = v1.2.36', () => checkGoldenNegative(parts));
  test('P2 pure: hours, state, the main-page key, the drawing limit, the plan matrix, dp2SlotCoreOk_', () => { sc = scenarios(NEW); checkPure(sc, parts); });
  test('P3 ticks: the older page first, one page per execution, stamped, claim cleared, the same page as the menu build, then idle; the next slot waits for its render', () => checkTicks(sc, parts));
  test('P4 waits: outside the hours (only the hours read), a decision render, a fresh claim (a stale one builds), a running render until minute 40, off, the default hours', () => checkWaits(sc, parts));
  test('P5 a late drawing step keeps the previous page (nothing cleared, try counted); the untried page goes next, then the retry builds it; two tries at most', () => checkLate(sc, parts));
  test('P6 a failing build: the try counted, the claim cleared, no stamp, the error logged and rethrown', () => checkFail(sc, parts));
  test('P7 the slot installer (removes the hourly trigger), the hourly installer (removes the slot trigger), the remover, the status', () => checkInstall(sc, parts));
  test('P7b a menu build on a moving clock is never late (the guard is for slot builds only)', () => checkManual(sc, parts));
  test('P8 dp2SelfTest: the v1.2.36 report without the slot trigger; one refresh-slots line with it', () => checkSelfTest(parts));
  test('P9 lint: LF; ASCII and ES5 in every hunk; the WHY newest-first; every prior WHY line kept', () => checkLint());
  test('P10 mutants: ' + MUTANTS.length + ' single-line mutants of the delivered file are each caught', () => {
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
console.log('DP2 V1300 P0-P10 x2 identical digest ' + digests[0].slice(0, 16) + ' | new SHA256 ' + sha256(NEW).slice(0, 12) + ' | v1.2.36 SHA256 ' + sha256(OLD).slice(0, 12));
console.log('PASS ' + passed + '/' + total);
