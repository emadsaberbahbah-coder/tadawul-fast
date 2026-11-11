#!/usr/bin/env node
/* tests/test_dp2_v1236_evidence_since.js
 * Harness for apps_script/27_Decision_Pages_v2.gs v1.2.36 [EVIDENCE-SINCE] (owner, Mon 9 Nov 2026 11:44 Riyadh: the Model
 * evidence tile read 48.70% and the engine rule that answers it - data_engine_v2 5.160.0 - cannot move an all-time figure
 * for months). Script Property DP2_EVIDENCE_SINCE = 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM' (Riyadh; default OFF = the v1.2.35
 * pages byte for byte): the tile adds ' . since <when>: <rate> (n <records>)' for the matured INVESTABLE records made at or
 * after that time (Performance_Log 'Date Recorded (Riyadh)'), and the MODEL EVIDENCE chart adds a 'since' bar once one has
 * matured.
 *
 * Two trees over the REAL file, each in its own vm context: the delivered source (NEW) and the v1.2.35 file (OLD,
 * SHA-256 pinned dc88cfab = origin/main 772a497), by default rebuilt from NEW by reversing the enumerated v1.2.36 hunks
 * (tests/js/dp2_edits_v1236.json). The GAS shim, the fixture and the boards are those of
 * tests/test_dp2_v1235_stop_rule.js, copied verbatim (makeEnv also takes a Performance_Log grid; norm folds 1.2.36).
 *   P0 oracle, version lockstep, inventory (+5 functions, +2 line-start vars, 0 removed), 9 hunks reverse to the base
 *   P1 GOLDEN NEGATIVE / KILL: absent, blank, on, yes, a bad date, junk, a fault -> both pages = the v1.2.35 build
 *   P2 armed without a Date Recorded column: only the tile text differs ('n/a (no Date Recorded column)')
 *   P3 armed over a dated Performance_Log: the tile, the chart bar, the boundary, a date-typed cell, the header fallback,
 *      'none matured yet'; the all-time figure is the v1.2.35 one
 *   P4 pure: the parser, the record key, the tile text, the core check
 *   P5 the gate: one property read per execution; Portfolio_Decision_v2 alone never reads it; a fault reads off
 *   P6 dp2SelfTest: the v1.2.35 report while off; one 'evidence since' line while on
 *   P7 lint: ES5; no non-ASCII character in a hunk; LF; WHY newest-first; every prior WHY kept
 *   P8 mutants
 * The battery runs twice and the two digests must match; the last line is 'PASS k/k' (exit 1 on any failure).
 * Usage: node tests/test_dp2_v1236_evidence_since.js [new.gs] [v1.2.35.gs]
 */
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const vm = require('vm');
const ROOT = path.join(__dirname, '..');
const NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '27_Decision_Pages_v2.gs');
const OLD_PATH = process.argv[3] || '';
// 2026-11-11: v1.3.1 is delivered on top of v1.3.0; v1236Source chains through the v1.3.0 file (dp2_history).
// 2026-11-11: v1.3.0 is delivered; this harness proves the v1.2.36 diff, so NEW is the v1.2.36 file rebuilt from the
// delivered one (tests/js/dp2_history.js: the v1.3.0 hunks reversed, SHA-256 pinned).
const NEW = require(path.join(__dirname, 'js', 'dp2_history.js')).v1236Source(fs.readFileSync(NEW_PATH, 'utf8'));
const FIX = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'fixtures', 'dp2_board_2026-11-02.json'), 'utf8'));
const EDITS = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'dp2_edits_v1236.json'), 'utf8'));
const V1235_SHA256 = 'dc88cfab7cc706b391f6288dcc44d8e9e0cf6479015b22a725f134c6fd694abf';   // v1.2.35 (origin/main 772a497)
const NOW = Date.parse(FIX.frozen_now);   // 2026-11-02T06:24:00Z = 09:24 Riyadh
const GATE = 'DP2_EVIDENCE_SINCE';
const DOT = '·', DASH = '—', ARROW = '→', NDASH = '–';
const sha256 = s => crypto.createHash('sha256').update(s).digest('hex');
const plain = v => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));
const clone = plain;

/* ------------------------------------------------------------ runner */
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
    eq(s.split(cur).length - 1, 1, 'exactly one approved v1.2.36 hunk: ' + cur.split('\n')[0].slice(0, 90));
    s = s.replace(cur, () => prev);
  }
  eq(sha256(s), V1235_SHA256, 'v1.2.36 changes only its enumerated hunks (their reversal = the v1.2.35 file)');
  return s;
}
let OLD = null, oldErr = '';
try {
  OLD = OLD_PATH ? fs.readFileSync(OLD_PATH, 'utf8') : reverseEdits(NEW);
  eq(sha256(OLD), V1235_SHA256, 'v1.2.35 SHA-256');
} catch (e) { oldErr = String(e && e.message || e); OLD = null; }
if (!OLD) { console.log('FAIL P0 oracle: ' + oldErr); console.log('FAIL 0/1'); process.exit(1); }

/* ------------------------------------------------------------ the recording spreadsheet */
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
  const book = makeBook();
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
  const props = Object.assign({}, opts.props || {}), reads = {}, logs = [], mails = [];
  const capture = opts.capture === undefined ? FIX.ibkr_capture : opts.capture;
  const ctx = {
    Date: fakeDate(opts.now || NOW),
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
    ScriptApp: { getProjectTriggers() { return []; } }
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
  for (const v of ['1.2.36', '1.2.35', '1.2.34', '1.2.33', '1.2.32', '1.2.31']) s = s.split('Decision Pages v2 ' + v).join('Decision Pages v2 1.2.30');
  return JSON.parse(s);
}
function sheet(b, name) { return b.env.book.getSheetByName(name); }
function val(sh, r, c) { const x = sh._cells.get(r + ':' + c); return x && x.v !== undefined && x.v !== null ? x.v : ''; }
function fmt(sh, r, c) { return sh._cells.get(r + ':' + c) || {}; }
function rowsWhere(sh, c, pred) { const out = [], last = sh.getLastRow(); for (let r = 1; r <= last; r++) if (pred(String(val(sh, r, c)), r)) out.push(r); return out; }
function firstRow(sh, c, pred) { const r = rowsWhere(sh, c, pred); return r.length ? r[0] : -1; }
/* Top_10_Investments_v2 read-back */
function t10View(b) {
  const sh = sheet(b, 'Top_10_Investments_v2');
  const hdr = firstRow(sh, 1, (v, r) => v === '#' && val(sh, r, 2) === 'Symbol ' + DOT + ' name');
  ok(hdr > 0, 'seats table found');
  const seats = [];
  for (let r = hdr + 1; r <= hdr + 10; r++) {
    seats.push({ sym: String(val(sh, r, 2)).split(' ' + DOT + ' ')[0], status: val(sh, r, 5), statusColor: fmt(sh, r, 5).fg, alloc: val(sh, r, 7), qty: val(sh, r, 8),
                 allocColor: fmt(sh, r, 7).fg, risk: val(sh, r, 10), gain: val(sh, r, 11), flags: String(val(sh, r, 15)) });
  }
  const title = String(val(sh, firstRow(sh, 1, v => v.indexOf('SEATS ') === 0), 1));
  const titleRow = firstRow(sh, 1, v => v.indexOf('SEATS ') === 0);
  const sub = String(val(sh, titleRow + 1, 1));
  const tileRow = firstRow(sh, 1, v => v === 'SCANNED');   // the tiles row (labels upper-cased by dp2Tiles_)
  const tile = label => { for (let c = 1; c <= 16; c++) if (val(sh, tileRow, c) === label) return { row: tileRow, col: c, value: val(sh, tileRow + 1, c), sub: val(sh, tileRow + 2, c), color: fmt(sh, tileRow + 1, c).fg }; return null; };
  const kv = label => { const r = firstRow(sh, 1, v => v.indexOf(label) === 0); return r > 0 ? { row: r, label: String(val(sh, r, 1)), value: String(val(sh, r, 6)) } : null; };
  const allocTitleRow = firstRow(sh, 1, v => v.indexOf('ALLOCATION ') === 0);
  const aTop = allocTitleRow + 2;
  const charts = sheet(b, '_DP2_Charts_T10');
  const fr = firstRow(charts, 1, v => v === '## Funnel'), funnel = [];
  for (let r = fr + 2; String(val(charts, r, 1)) !== ''; r++) funnel.push([val(charts, r, 1), val(charts, r, 2)]);
  const funnelChart = b.sheets['Top_10_Investments_v2'].charts.find(c => /Decision funnel/.test(String(c.options.title || '')));
  return { sh, hdr, seats, title, titleRow, sub, tile, kv, allocTitle: String(val(sh, allocTitleRow, 1)), allocTitleRow, aTop, funding: String(val(sh, aTop, 9)), funnel,
           funnelSub: funnelChart ? String(funnelChart.options.subtitle) : '' };
}
/* Portfolio_Decision_v2 read-back */
function pdView(b) {
  const sh = sheet(b, 'Portfolio_Decision_v2');
  const actions = rowsWhere(sh, 1, v => /^\d+\. /.test(v)).map(r => String(val(sh, r, 1)).replace(/^\d+\. /, ''));
  const holdHdr = firstRow(sh, 1, (v, r) => v === 'Symbol' && val(sh, r, 15) === 'Order status (IBKR)');
  const frst = firstRow(sh, 1, v => v === 'FRST.US');
  const limit = label => { for (const [k, x] of sh._cells) if (x.v === label) { const [r, c] = k.split(':').map(Number); return String(val(sh, r, c + 4)); } return null; };
  return { sh, actions, chip: String(val(sh, 1, 11)), right: String(val(sh, 1, 13)), reason: String(val(sh, 2, 1)), holdHdr, frstStatus: frst > 0 ? String(val(sh, frst, 15)) : null, limit };
}




/* ============================================================ the 5 Nov scenarios */
const T10 = 'Top_10_Investments_v2', PDV = 'Portfolio_Decision_v2';
const ON = { [GATE]: 'on' };
const ELL = '…';
const isoAgo = min => new Date(NOW - min * 60000).toISOString();
const MARKER = min => JSON.stringify({ started: isoAgo(min), v: '1.20.0' });
const CLAIM = min => JSON.stringify({ claim: { act: 't10', run: '41901407829', at_ms: NOW - min * 60000 } });
const RUN_STATUS = hm => 'Last run 2026-11-02 ' + hm + ' | status: running' + ELL;
BOARDS.runningNow = board('cockpit render in progress (started 09:20)', t => { t.status = RUN_STATUS('09:20:00'); });
BOARDS.runningStopped = board('cockpit render stopped (started 09:05)', t => { t.status = RUN_STATUS('09:05:00'); });
const PREV = 'read the seats below as the previous render' + "'" + 's';
const NOTE_NOW = 'a cockpit render started 09:20 is in progress; until it finishes, ' + PREV;
const NOTE_STOP = 'the cockpit render started 09:05 did not finish (Apps Script stops an execution at 6 minutes; it wrote no receipt); ' + PREV + ' until the cockpit shows a finished run';
const NO_NET = { fetch() { return { getResponseCode: () => 404, getContentText: () => '{}' }; } };
function v2Writes(calls) {   // the v2 pages in the order they were first created
  const out = [];
  for (const c of calls) { const m = /^insertSheet\|(Top_10_Investments_v2|Portfolio_Decision_v2)$/.exec(c); if (m && out.indexOf(m[1]) < 0) out.push(m[1]); }
  return out;
}
function statusLine(b) {
  const sh = sheet(b, T10);
  const r = firstRow(sh, 1, v => v.indexOf('Status: ') === 0);
  ok(r > 0, 'the Status line');
  return { row: r, text: String(val(sh, r, 1)), bg: fmt(sh, r, 1).bg, h: b.sheets[T10].rowH[r] };
}
const headRight = b => String(val(sheet(b, T10), 1, 13));
const stamps = b => { const raw = b.env.props.DP2_LAST_BUILT; return raw ? JSON.parse(raw) : null; };
const LIVE = { DT10_RUN_MARKER: MARKER(2), TFB_DECISION_CHAIN_STATE: CLAIM(1), DP2_LAST_BUILT: JSON.stringify({ pd: NOW, t10: NOW - 3 * 3600000 }) };

/* ============================================================ v1.2.36 [EVIDENCE-SINCE] */
const SINCE = '2026-10-20 13:05', SINCE_KEY = '2026-10-20 13:05:00';
const BASE_TXT = 'INVESTABLE hit rate, matured records';
const TILE = 'MODEL EVIDENCE';
const ARMED = { [GATE]: SINCE };
/* a Performance_Log with its 'Date Recorded (Riyadh)' column: the fixture's 30 matured records (before SINCE) + 11 more */
function perfDated(hdrName) {
  const g = [['Record ID', 'Status', 'Outcome', 'Entry Investability', 'Entry Recommendation', 'Horizon', hdrName || 'Date Recorded (Riyadh)']];
  FIX.performance_log.forEach((x, i) => g.push(['PL-' + (i + 1), 'matured', x[0], x[1], x[2], x[3], '2026-09-15 03:00:00']));
  const add = (st, o, inv, d) => g.push(['PL-' + g.length, st, o, inv, 'BUY', '1W', d]);
  add('matured', 'WIN', 'INVESTABLE', '2026-10-20 13:05:00');                 // a: on the boundary -> counts
  add('matured', 'LOSS', 'INVESTABLE', '2026-10-20 13:04:59');                // b: one second before -> not counted
  add('matured', 'WIN', 'INVESTABLE', '2026-10-21 03:52:16');                 // c
  add('matured', 'LOSS', 'INVESTABLE', '2026-10-21 03:52:16');                // d
  add('matured', 'WIN', 'INVESTABLE', new Date(Date.UTC(2026, 9, 20, 10, 6, 0)));   // e: a date-typed cell, 13:06 Riyadh (10:06 UTC) -> counts
  add('matured', 'LOSS', 'INVESTABLE', '2026-10-23');                         // f: a date only -> 00:00:00 -> counts
  add('active', '', 'INVESTABLE', '2026-10-24 03:00:00');                     // g: not matured
  add('matured', 'WIN', 'WATCHLIST', '2026-10-24 03:00:00');                  // h: not INVESTABLE
  add('matured', 'FLAT', 'INVESTABLE', '2026-10-24 03:00:00');                // i: not WIN / LOSS
  add('matured', 'WIN', 'INVESTABLE', 'n/a');                                 // j: no date -> not counted (all-time only)
  add('matured', 'WIN', 'INVESTABLE', '2026-10-25T10:00');                    // k: the T separator -> counts
  return g;
}
const PERF = perfDated();
// since SINCE: a W, c W, d L, e W, f L, k W -> 4 of 6; all time INVESTABLE: 11 W / 8 L + a..f, j, k (5 W, 3 L) -> 16 of 27
const SINCE_TXT = BASE_TXT + ' ' + DOT + ' since ' + SINCE + ': 66.7% (n 6)';
function hitBlock(b) {
  const ch = sheet(b, '_DP2_Charts_T10');
  const r0 = firstRow(ch, 1, v => v === '## HitRate');
  ok(r0 > 0, 'the HitRate chart data');
  const out = [];
  for (let r = r0 + 1; String(val(ch, r, 1)) !== ''; r++) out.push([val(ch, r, 1), val(ch, r, 2), val(ch, r, 3)]);
  return out;
}
function tileOf(b) { const t = t10View(b).tile(TILE); ok(t, 'the Model evidence tile'); return t; }
/* the two builds differ only in the tile text: replace it, then they must be equal */
function onlyTileDiffers(n, o, newTxt, label) {
  eq(tileOf(n).sub, newTxt, 'tile text (' + label + ')');
  eq(tileOf(o).sub, BASE_TXT, 'v1.2.35 tile text (' + label + ')');
  const ns = JSON.stringify(norm(n)).split(newTxt).join(BASE_TXT), os = JSON.stringify(norm(o));
  deq(JSON.parse(ns), JSON.parse(os), 'everything but the tile text = v1.2.35 (' + label + ')');
}
const OFF_RAW = [undefined, '', 'off', 'on', '1', 'yes', 'true', '2026-13-01', '2026-02-30', '2026-11-31', '2026-11-09 24:00', '2026-11-09 12:60',
                 '2026-11-09 13:05:00', '9 Nov 2026', '2026-11-9', '2026/11/09', 'since 2026-11-09', '20261109'];

function checkGoldenNegative(src, parts) {
  for (const raw of OFF_RAW) {
    const props = raw === undefined ? {} : { [GATE]: raw };
    for (const key of ['today', 'research']) {
      const n = build(src, 'both', { board: BOARDS[key], props, fetch: NO_NET }), o = build(OLD, 'both', { board: BOARDS[key], props, fetch: NO_NET });
      deq(norm(n), norm(o), 'gate ' + JSON.stringify(raw) + ', board ' + key);
      if (key === 'today' && raw === undefined) parts.push(norm(n).ret);
    }
    const nd = build(src, 't10', { perf: PERF, props, fetch: NO_NET }), od = build(OLD, 't10', { perf: PERF, props, fetch: NO_NET });
    deq(norm(nd), norm(od), 'gate ' + JSON.stringify(raw) + ', the dated Performance_Log');
  }
  for (const which of ['t10', 'pd']) deq(norm(build(src, which, { board: BOARDS.todayNA, fetch: NO_NET })), norm(build(OLD, which, { board: BOARDS.todayNA, fetch: NO_NET })), 'todayNA, ' + which);
  for (const props of [ARMED, {}]) {
    const f1 = build(src, 'both', { board: BOARDS.today, props, propsThrow: true, fetch: NO_NET }), f0 = build(OLD, 'both', { board: BOARDS.today, props, propsThrow: true, fetch: NO_NET });
    deq(norm(f1), norm(f0), 'a property fault reads off (' + JSON.stringify(props) + ')');
  }
  // armed, Portfolio_Decision_v2 alone: unchanged
  deq(norm(build(src, 'pd', { perf: PERF, props: ARMED, fetch: NO_NET })), norm(build(OLD, 'pd', { perf: PERF, props: ARMED, fetch: NO_NET })), 'armed, Portfolio_Decision_v2 = v1.2.35');
}
function checkNoColumn(src, parts) {
  for (const key of ['today', 'research']) {
    const n = build(src, 'both', { board: BOARDS[key], props: ARMED, fetch: NO_NET }), o = build(OLD, 'both', { board: BOARDS[key], props: ARMED, fetch: NO_NET });
    onlyTileDiffers(n, o, BASE_TXT + ' ' + DOT + ' since ' + SINCE + ': n/a (no Date Recorded column)', 'no column, ' + key);
    if (key === 'today') parts.push(tileOf(n));
  }
}
function checkDated(src, parts) {
  const n = build(src, 't10', { perf: PERF, props: ARMED, fetch: NO_NET }), o = build(OLD, 't10', { perf: PERF, props: ARMED, fetch: NO_NET });
  const tn = tileOf(n), to = tileOf(o);
  eq(tn.sub, SINCE_TXT, 'the tile adds the since figure');
  eq(tn.value, '59.3%', 'the all-time INVESTABLE figure: 16 of 27');
  eq(tn.value, to.value, 'the all-time figure is the v1.2.35 one');
  eq(tn.color, to.color, 'tile colour');
  const hn = hitBlock(n), ho = hitBlock(o);
  eq(hn.length, ho.length + 1, 'one more chart row');
  deq(hn[0], ho[0], 'the chart header');
  eq(String(ho[1][0]), 'INVESTABLE (n 27)', 'the INVESTABLE bar');
  deq(hn[1], ho[1], 'the INVESTABLE bar unchanged');
  eq(hn[2][0], 'INVESTABLE since ' + SINCE + ' (n 6)', 'the since bar sits under the INVESTABLE bar');
  near(hn[2][1], 4 / 6, 1e-12, 'the since rate');
  eq(hn[2][2], 0.5, 'the coin flip');
  deq(hn.slice(3), ho.slice(2), 'every other bar unchanged');
  deq(n.ret, o.ret, 'the return value');
  eq(n.sheets[T10].charts.length, o.sheets[T10].charts.length, 'the same charts');
  // the header fallback 'Date Recorded'
  const nf = build(src, 't10', { perf: perfDated('Date Recorded'), props: ARMED, fetch: NO_NET });
  eq(tileOf(nf).sub, SINCE_TXT, 'the Date Recorded header is read too');
  // nothing matured since: only the tile text differs, no bar
  const late = { [GATE]: '2026-12-01' };
  const nl = build(src, 't10', { perf: PERF, props: late, fetch: NO_NET }), ol = build(OLD, 't10', { perf: PERF, props: late, fetch: NO_NET });
  onlyTileDiffers(nl, ol, BASE_TXT + ' ' + DOT + ' since 2026-12-01: none matured yet', 'none matured');
  // a date-only property counts the whole day; the T separator in the property
  eq(tileOf(build(src, 't10', { perf: PERF, props: { [GATE]: '2026-10-21' }, fetch: NO_NET })).sub, BASE_TXT + ' ' + DOT + ' since 2026-10-21: 50.0% (n 4)', 'since 2026-10-21 counts the whole day: c W, d L, f L, k W (a and e are 20 Oct)');
  eq(tileOf(build(src, 't10', { perf: PERF, props: { [GATE]: ' 2026-10-20T13:05 ' }, fetch: NO_NET })).sub, SINCE_TXT, 'the T separator and spaces');
  // the both-pages build: Portfolio_Decision_v2 = v1.2.35, Top_10_Investments_v2 carries the figure
  const nb = build(src, 'both', { perf: PERF, props: ARMED, fetch: NO_NET }), ob = build(OLD, 'both', { perf: PERF, props: ARMED, fetch: NO_NET });
  eq(tileOf(nb).sub, SINCE_TXT, 'both pages: the tile');
  deq(norm(nb).sheets[PDV], norm(ob).sheets[PDV], 'both pages: Portfolio_Decision_v2 = v1.2.35');
  parts.push([tn.sub, hn]);
}
function checkPure(src, parts) {
  const e = makeEnv(src, {}).ctx;
  const P = s => plain(e.dp2EvidenceSinceParse_(s));
  deq(P('2026-11-09'), { label: '2026-11-09', key: '2026-11-09 00:00:00' }, 'a date');
  deq(P('2026-11-09 13:05'), { label: '2026-11-09 13:05', key: '2026-11-09 13:05:00' }, 'a date and time');
  deq(P('2026-11-09T13:05'), { label: '2026-11-09 13:05', key: '2026-11-09 13:05:00' }, 'the T separator');
  deq(P('  2026-11-09 00:00\n'), { label: '2026-11-09 00:00', key: '2026-11-09 00:00:00' }, 'trimmed');
  deq(P('2028-02-29'), { label: '2028-02-29', key: '2028-02-29 00:00:00' }, 'a leap day');
  for (const bad of OFF_RAW.concat([null, '2026-02-29', '2026-11-09 1:05', '2026-11-09 13:5', 'x2026-11-09', '2026-11-09 13:05 extra'])) eq(P(bad), null, 'rejected: ' + JSON.stringify(bad));
  const K = v => e.dp2RecordedKey_(v);
  eq(K('2026-11-09 03:52:16'), '2026-11-09 03:52:16', 'text as written');
  eq(K('2026-11-09'), '2026-11-09 00:00:00', 'a date only');
  eq(K('2026-11-09T03:52'), '2026-11-09 03:52:00', 'the T separator, no seconds');
  eq(K(' 2026-11-09 03:52:16 '), '2026-11-09 03:52:16', 'trimmed');
  eq(K(new Date(Date.UTC(2026, 10, 9, 0, 52, 16))), '2026-11-09 03:52:16', 'a date-typed cell in Riyadh time');
  eq(K(new Date(NaN)), '', 'an invalid date');
  for (const x of ['', 'n/a', null, undefined, 45000, '09/11/2026', true]) eq(K(x), '', 'no date: ' + JSON.stringify(x));
  const T = s => e.dp2EvidenceSinceTxt_(s);
  eq(T(null), '', 'off'); eq(T(undefined), '', 'off (undefined)');
  eq(T({ label: '2026-11-10', col: false, wins: 0, n: 0 }), ' ' + DOT + ' since 2026-11-10: n/a (no Date Recorded column)', 'no column');
  eq(T({ label: '2026-11-10', col: true, wins: 0, n: 0 }), ' ' + DOT + ' since 2026-11-10: none matured yet', 'none');
  eq(T({ label: '2026-11-10', col: true, wins: 2, n: 3 }), ' ' + DOT + ' since 2026-11-10: 66.7% (n 3)', 'a rate');
  eq(T({ label: '2026-11-10', col: true, wins: 0, n: 1234 }), ' ' + DOT + ' since 2026-11-10: 0.0% (n 1,234)', 'thousands');
  eq(e.dp2EvidenceSinceCoreOk_(), true, 'the self-test core');
  parts.push(P('2026-11-09 13:05'));
}
function checkGate(src) {
  for (const props of [ARMED, {}, { [GATE]: 'junk' }]) {
    const b = build(src, 'both', { perf: PERF, props, fetch: NO_NET });
    eq(b.env.reads[GATE], 1, 'both pages: one read (' + JSON.stringify(props) + ')');
    const p = build(src, 'pd', { perf: PERF, props, fetch: NO_NET });
    eq(p.env.reads[GATE], undefined, 'Portfolio_Decision_v2 alone never reads it (' + JSON.stringify(props) + ')');
  }
  const e = makeEnv(src, { props: ARMED }); e.ctx.dp2EvidenceSince_(); e.ctx.dp2EvidenceSince_(); e.ctx.dp2ReadHitRates_();
  eq(e.reads[GATE], 1, 'memoised: one read per execution');
  eq(makeEnv(src, { props: ARMED, propsThrow: true }).ctx.dp2EvidenceSince_(), null, 'a fault reads off');
  eq(OLD.indexOf(GATE), -1, 'v1.2.35 never names the property');
}
function checkSelfTest(src, parts) {
  const run = (s, opts) => { const env = makeEnv(s, Object.assign({ fetch: NO_NET }, opts)); return plain(env.ctx.dp2SelfTest()); };
  const fold = x => x.name.split('1.2.36').join('v').split('1.2.35').join('v');
  const n0 = run(src, {}), o0 = run(OLD, {});
  deq(n0.checks.map(x => [fold(x), x.ok]), o0.checks.map(x => [fold(x), x.ok]), 'off: the v1.2.35 report');
  const n1 = run(src, { props: ARMED, perf: PERF }), o1 = run(OLD, { props: ARMED, perf: PERF });
  eq(n1.checks.length, o1.checks.length + 1, 'on: one more line');
  const at = n1.checks.findIndex(x => /^evidence since/.test(x.name));
  ok(at > 0, 'the evidence since line');
  eq(n1.checks[at].name, 'evidence since (DP2_EVIDENCE_SINCE): ' + SINCE + ' ' + DOT + ' 6 matured INVESTABLE records since, hit rate 66.7% ' + DOT + ' core ok', 'the line');
  eq(n1.checks[at].ok, true, 'ok with the column');
  eq(n1.checks[at - 1].name, 'symbol keys (GOOGLEFINANCE / chart API)', 'after the symbol-key line (the stop rule and exit sanity are off)');
  deq(n1.checks.filter((x, i) => i !== at).map(x => [fold(x), x.ok]), o1.checks.map(x => [fold(x), x.ok]), 'every other line = v1.2.35');
  const n2 = run(src, { props: ARMED });
  const l2 = n2.checks.filter(x => /^evidence since/.test(x.name));
  eq(l2.length, 1, 'one line without the column');
  eq(l2[0].name, 'evidence since (DP2_EVIDENCE_SINCE): ' + SINCE + ' ' + DOT + ' no Date Recorded column in Performance_Log ' + DOT + ' core ok', 'the line without the column');
  eq(l2[0].ok, false, 'FAIL without the column');
  const n3 = run(src, { props: { [GATE]: '2026-12-01' }, perf: PERF });
  eq(n3.checks.filter(x => /^evidence since/.test(x.name))[0].name, 'evidence since (DP2_EVIDENCE_SINCE): 2026-12-01 ' + DOT + ' 0 matured INVESTABLE records since ' + DOT + ' core ok', 'none yet');
  parts.push(n1.checks[at]);
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
  eq(vs[0], '1.2.36', 'the newest WHY first');
  eq(vs[1], '1.2.35', 'then 1.2.35');
  const oldLines = hdr(OLD).split('\n').filter(l => !/^ \* Version: /.test(l));
  for (const l of oldLines) ok(hdr(NEW).indexOf(l) >= 0, 'prior WHY kept: ' + l.slice(0, 90));
  eq(hdr(NEW).split('\n')[3], ' * Version: 1.2.36 (2026-11-09)', 'the version line');
}
/* one scenario signature for the mutants */
function signature(src) {
  const b = build(src, 't10', { perf: PERF, props: ARMED, fetch: NO_NET });
  const e = makeEnv(src, { props: ARMED }).ctx;
  return JSON.stringify([tileOf(b).sub, hitBlock(b), plain(e.dp2EvidenceSinceParse_('2026-02-30')), plain(e.dp2EvidenceSinceParse_('2026-11-09 13:05')),
                         e.dp2EvidenceSinceCoreOk_(), e.dp2EvidenceSinceTxt_({ label: 'x', col: true, wins: 0, n: 0 }), e.dp2RecordedKey_(new Date(Date.UTC(2026, 9, 20, 10, 6))),
                         e.dp2RecordedKey_('2026-10-23')]);
}
const MUTANTS = [
  ['the boundary', "dp2RecordedKey_(vals[r][cRec]) >= es.key", "dp2RecordedKey_(vals[r][cRec]) > es.key"],
  ['any label', "if (es && inv === 'INVESTABLE' && cRec", "if (es && cRec"],
  ['a date-typed cell in UTC', "isNaN(v.getTime()) ? '' : Utilities.formatDate(v, DP2.TZ, 'yyyy-MM-dd HH:mm:ss')", "isNaN(v.getTime()) ? '' : Utilities.formatDate(v, 'UTC', 'yyyy-MM-dd HH:mm:ss')"],
  ['no calendar check', "if (t.getUTCFullYear() !== y || t.getUTCMonth() !== mo - 1 || t.getUTCDate() !== d) return null;", ""],
  ['the bar above INVESTABLE', "esAt = ej + 1; hRows.splice", "esAt = ej; hRows.splice"],
  ['wins counted as records', "{ esW += win; esN++; }", "{ esW++; esN++; }"],
  ['the none text', "head + 'none matured yet'", "head + 'none yet'"],
  ['the date-only key', "m[1] + ' ' + (m[2] === undefined ? '00:00:00'", "m[1] + ' ' + (m[2] === undefined ? '23:59:59'"],
  ['the old header only', "h['Date Recorded (Riyadh)'] !== undefined ? h['Date Recorded (Riyadh)'] : h['Date Recorded']", "h['Date Recorded']"]
];

function battery() {
  const parts = [];
  test('P0 oracle: v1.2.35 rebuilt from the delivered file (SHA-256 pinned); version lockstep 1.2.36; both compile; inventory +5 functions +2 line-start vars, 0 removed; 9 hunks reverse to the base', () => {
    eq(sha256(OLD), V1235_SHA256, 'base SHA-256');
    ok(NEW.indexOf("var DP2_VERSION = '1.2.36';   // lockstep with the header") > 0 && /^ \* Version: 1\.2\.36 \(2026-11-09\)$/.test(NEW.split('\n')[3]), 'version lockstep');
    new vm.Script(NEW); new vm.Script(OLD);
    const names = (s, re) => { const o = {}; let m; while ((m = re.exec(s)) !== null) o[m[1]] = true; return o; };
    const fb = names(OLD, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), fn = names(NEW, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    deq(Object.keys(fb).filter(k => !fn[k]), [], 'functions removed');
    deq(Object.keys(fn).filter(k => !fb[k]).sort(), ['dp2EvidenceSinceCoreOk_', 'dp2EvidenceSinceParse_', 'dp2EvidenceSinceTxt_', 'dp2EvidenceSince_', 'dp2RecordedKey_'], 'functions added');
    const vb = names(OLD, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = names(NEW, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq(Object.keys(vb).filter(k => !vn[k]), [], 'vars removed');
    deq(Object.keys(vn).filter(k => !vb[k]).sort(), ['DP2_ES_READ_', 'DP2_EVIDENCE_SINCE_PROP'], 'vars added (line starts)');
    ok(NEW.indexOf('var DP2_ES_READ_ = false, DP2_ES_ = null;') > 0 && OLD.indexOf('DP2_ES_') < 0, 'DP2_ES_ shares a line');
    eq(EDITS.length, 9, 'hunk count');
    for (const [cur] of EDITS) eq(NEW.split(cur).length - 1, 1, 'each current is unique: ' + cur.split('\n')[0].slice(0, 80));
    eq(sha256(reverseEdits(NEW)), V1235_SHA256, 'reversal of the delivered file');
  });
  test('P1 GOLDEN NEGATIVE / KILL: the gate absent, blank, on, yes, a bad date, junk or a property fault -> both pages equal the v1.2.35 build (cells, formats, charts, calls, logs, mails, returns); armed, Portfolio_Decision_v2 alone is unchanged', () => checkGoldenNegative(NEW, parts));
  test('P2 armed without a Date Recorded column: only the Model evidence tile text differs (n/a (no Date Recorded column))', () => checkNoColumn(NEW, parts));
  test('P3 armed over a dated Performance_Log: tile 66.7% (n 6) beside the unchanged all-time 59.3%; the since bar under INVESTABLE; boundary, date-typed cell, header fallback, date-only property, none matured yet', () => checkDated(NEW, parts));
  test('P4 pure: parser (dates, times, T, leap days, rejects), record key (text, date-typed cells in Riyadh time), tile text, core check', () => checkPure(NEW, parts));
  test('P5 the gate: one property read per execution; Portfolio_Decision_v2 alone never reads it; memoised; a fault reads off', () => checkGate(NEW));
  test('P6 dp2SelfTest: off = the v1.2.35 report; on = one evidence since line (ok with the column, FAIL without it)', () => checkSelfTest(NEW, parts));
  test('P7 lint: LF; ASCII and ES5 in every hunk; the WHY newest-first; every prior WHY line kept', () => checkLint());
  test('P8 mutants: ' + MUTANTS.length + ' single-line mutants of the delivered file are each caught', () => {
    const want = signature(NEW);
    for (const [label, from, to] of MUTANTS) {
      eq(NEW.split(from).length - 1, 1, 'mutant anchor unique: ' + label);
      const M = NEW.replace(from, () => to);
      let caught = false;
      try { caught = signature(M) !== want; } catch (eM) { caught = true; }
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
console.log('DP2 V1236 P0-P8 x2 identical digest ' + digests[0].slice(0, 16) + ' | new SHA256 ' + sha256(NEW).slice(0, 12) + ' | v1.2.35 SHA256 ' + sha256(OLD).slice(0, 12));
console.log('PASS ' + passed + '/' + total);
