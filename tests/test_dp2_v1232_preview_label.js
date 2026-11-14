#!/usr/bin/env node
/* tests/test_dp2_v1232_preview_label.js
 * Harness for apps_script/27_Decision_Pages_v2.gs v1.2.32 [PREVIEW-LABEL] (owner, Tue 3 Nov 2026 12:14 Riyadh: 'my top
 * 10 investment page 2 showing that there is cash allocated to symbols but the page at top showing red no execution').
 * Script Property DP2_PREVIEW_LABEL = 1 / on / true / yes (default OFF): on a Top-10 page that cannot be acted on (no
 * engine ticket, or the release not live) the seat sizing reads as a preview.
 *
 * Two trees over the REAL file, each in its own vm context: the delivered source (NEW) and the v1.2.31 file (OLD,
 * SHA-256 pinned d789cf50 = origin/main 8478029d), by default rebuilt from NEW by reversing the enumerated v1.2.32
 * hunks (tests/js/dp2_edits_v1232.json); a second argument names the v1.2.31 file instead. The GAS shim, the fixture
 * (tests/fixtures/dp2_board_2026-11-02.json) and the board variants are those of tests/test_dp2_v1231_engine_tickets.js,
 * copied verbatim (the recording spreadsheet, the frozen clock 2026-11-02 09:24 Riyadh).
 * Sections:
 *   P0  oracle (SHA-256), version lockstep, both files compile; the base lacks the new names
 *   P1  GOLDEN NEGATIVE: the gate absent / blank / junk / a property fault, or DP2_COHORT_LEGACY on -> both pages
 *       (every cell, format, merge, chart, the call log, the return values) equal the v1.2.31 build on seven boards
 *   P2  the 3 Nov shape (research board, WITHHELD, feed NOT ACTIONABLE), gate on: 'Preview SAR' / 'Preview qty', the
 *       SEATS line prefix, every sized seat grey italic with 'preview . not a ticket' first in its Flags, the
 *       ALLOCATION title 'SIZING PREVIEW (not tickets) -- ..'; every number identical; only the documented cells differ
 *   P3  a live EXECUTABLE engine board, gate on: the v1.2.31 page exactly (actionable)
 *   P4  an engine board whose release is not live (NOT ACTIONABLE, SYNC RUNNING, EXPIRED), gate on: 'ENGINE TICKETS
 *       NOT LIVE (feed ..)' and the preview labels; numbers identical
 *   P5  a research board on a live EXECUTABLE feed (QUALIFIED, nothing funded): why = 'nothing executable today'
 *   P6  dp2PreviewWhy_ (pure) and the gate spellings; one property read per Top-10 build
 *   P7  Portfolio_Decision_v2 is untouched by the gate
 *   P8  ES5 / ASCII lint of the v1.2.32 diff; inventory (2 functions + 1 var added, 0 removed); the replaced base
 *       lines pinned; 11 hunks whose reversal is the v1.2.31 file; WHY block; every prior WHY kept
 *   P9  mutants: each mutation of the delivered file fails at least one section check
 * The battery runs twice and the two digests must match; the last line is 'PASS k/k' (exit 1 on any failure).
 * Usage: node tests/test_dp2_v1232_preview_label.js [new.gs] [v1.2.31.gs]
 */
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const vm = require('vm');
const ROOT = path.join(__dirname, '..');
const NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '27_Decision_Pages_v2.gs');
const OLD_PATH = process.argv[3] || '';
// 2026-11-08: v1.2.35 [STOP-RULE] is delivered; this harness proves an older diff, so it reads the v1.2.34 file
// rebuilt from the delivered one (tests/js/dp2_history.js: the v1.2.35 hunks reversed, SHA-256 pinned).
const NEW = require(path.join(__dirname, 'js', 'dp2_history.js')).v1234Source(fs.readFileSync(NEW_PATH, 'utf8'));
const FIX = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'fixtures', 'dp2_board_2026-11-02.json'), 'utf8'));
const EDITS = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'dp2_edits_v1232.json'), 'utf8'));
const V1231_SHA256 = 'd789cf502c08ed00278196707b4f6d3057c5c1bbd00b86ad47254ed77e3cac20';   // v1.2.31 (origin/main 8478029d)
const NOW = Date.parse(FIX.frozen_now);   // 2026-11-02T06:24:00Z = 09:24 Riyadh
const GATE = 'DP2_PREVIEW_LABEL', LEGACY = 'DP2_COHORT_LEGACY';
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

/* ------------------------------------------------------------ P0 oracle */
function reverseEdits(src) {
  let s = src;
  for (const [cur, prev] of EDITS) {
    eq(s.split(cur).length - 1, 1, 'exactly one approved v1.2.32 hunk: ' + cur.split('\n')[0].slice(0, 90));
    s = s.replace(cur, () => prev);
  }
  eq(sha256(s), V1231_SHA256, 'v1.2.32 changes only its enumerated hunks (their reversal = the v1.2.31 file)');
  return s;
}
// 2026-11-03: v1.2.33 (exit sanity, DP2_EXIT_SANITY default off) stacked on this change; pins moved. Behaviour runs on
// the delivered file; P0 / P8 (lockstep, lint, inventory, replaced lines, hunks, WHY) read the v1.2.32 file rebuilt
// from it (the v1.2.33 hunks of tests/js/dp2_edits_v1233.json reversed, SHA-256 pinned).
const EDITS1233 = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'dp2_edits_v1233.json'), 'utf8'));
const V1232_SHA256 = '5ac92c367d86a344ed7beefff33d490d292b82fb68b4ede7fd0c0a9afb18a723';   // v1.2.32 (origin/main 3e326e26)
// 2026-11-05: v1.2.34 (render priority, TFB_RENDER_PRIORITY default off) stacked on v1.2.33; the chain first reverses
// the v1.2.34 hunks of tests/js/dp2_edits_v1234.json (the v1.2.33 file, SHA-256 pinned).
const EDITS1234 = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'dp2_edits_v1234.json'), 'utf8'));
const V1233_SHA256 = 'dd5ee2041b065b5e12aed7c41ea505fd3b75b3219e29807d060ff4d95a394756';   // v1.2.33 (origin/main c54eba40)
function v1233Of(src) {
  if (src.indexOf("var DP2_VERSION = '1.2.34';") < 0) return src;
  let s = src;
  for (const [cur, prev] of EDITS1234) {
    if (s.split(cur).length - 1 !== 1) throw new Error('exactly one approved v1.2.34 hunk: ' + cur.split('\n')[0].slice(0, 90));
    s = s.replace(cur, () => prev);
  }
  if (crypto.createHash('sha256').update(s).digest('hex') !== V1233_SHA256) throw new Error('v1.2.34 changes only its enumerated hunks (their reversal = the v1.2.33 file)');
  return s;
}
function v1232Of(src) {
  src = v1233Of(src);
  if (src.indexOf("var DP2_VERSION = '1.2.33';") < 0) return src;
  let s = src;
  for (const [cur, prev] of EDITS1233) {
    eq(s.split(cur).length - 1, 1, 'exactly one approved v1.2.33 hunk: ' + cur.split('\n')[0].slice(0, 90));
    s = s.replace(cur, () => prev);
  }
  eq(sha256(s), V1232_SHA256, 'v1.2.33 changes only its enumerated hunks (their reversal = the v1.2.32 file)');
  return s;
}
let OLD = null, oldErr = '', S1232 = null;
try {
  S1232 = v1232Of(NEW);
  OLD = OLD_PATH ? fs.readFileSync(OLD_PATH, 'utf8') : reverseEdits(S1232);
  eq(sha256(OLD), V1231_SHA256, 'v1.2.31 SHA-256');
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
  book.load('_Portfolio_CostBasis', ledgerGrid(FIX.ledger));
  book.load('My_Portfolio', mpGrid(FIX.my_portfolio));
  book.load('Global_Markets', gmGrid(FIX.global_markets_status));
  book.load('Performance_Log', perfGrid(FIX.performance_log));
  book.load('_DP2_Exit_Engine', barsGrid(FIX.bars));
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
    UrlFetchApp: { fetch() { throw new Error('no network in the harness'); } },
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
function norm(b) {   // the only bytes v1.2.31 under its kill switch may differ in: the version printed on both footers / the toast title
  const strip = o => { const x = Object.assign({}, o); delete x.env; return x; };
  return JSON.parse(JSON.stringify(strip(b)).split('Decision Pages v2 1.2.34').join('Decision Pages v2 1.2.30').split('Decision Pages v2 1.2.33').join('Decision Pages v2 1.2.30').split('Decision Pages v2 1.2.32').join('Decision Pages v2 1.2.30').split('Decision Pages v2 1.2.31').join('Decision Pages v2 1.2.30'));
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


/* ============================================================ the battery */
const T10 = 'Top_10_Investments_v2', PDV = 'Portfolio_Decision_v2';
const GREY = '#898781', GREEN = '#1a7f37';
const ON = { [GATE]: 'on' };
const cellMap = (b, name) => new Map(norm(b).sheets[name].cells.map(x => [x.split('=')[0], x]));
function diffKeys(a, b, name) {
  const ca = cellMap(a, name), cb = cellMap(b, name), out = [];
  for (const k of new Set(Array.from(ca.keys()).concat(Array.from(cb.keys())))) if (ca.get(k) !== cb.get(k)) out.push(k);
  return out.sort((x, y) => { const [r1, c1] = x.split(':').map(Number), [r2, c2] = y.split(':').map(Number); return r1 - r2 || c1 - c2; });
}
/* the preview read-back (robust to the relabelled ALLOCATION title) */
function pv(b) {
  const sh = sheet(b, T10);
  const hdr = firstRow(sh, 1, (v, r) => v === '#' && val(sh, r, 2) === 'Symbol ' + DOT + ' name');
  ok(hdr > 0, 'seats table found');
  const seats = [];
  for (let r = hdr + 1; r <= hdr + 10; r++) {
    const f7 = fmt(sh, r, 7), f8 = fmt(sh, r, 8);
    seats.push({ row: r, sym: String(val(sh, r, 2)).split(' ' + DOT + ' ')[0], alloc: val(sh, r, 7), qty: val(sh, r, 8), risk: val(sh, r, 10), gain: val(sh, r, 11),
                 flags: String(val(sh, r, 15)), fg: f7.fg, fw: f7.fw, fst: f7.fst, fg8: f8.fg, fw8: f8.fw, fst8: f8.fst });
  }
  const titleRow = firstRow(sh, 1, v => v.indexOf('SEATS ') === 0);
  const allocTitleRow = firstRow(sh, 1, v => /^(ALLOCATION|SIZING PREVIEW|ENGINE TICKETS NOT LIVE) /.test(v));
  return { sh, hdr, seats, h7: val(sh, hdr, 7), h8: val(sh, hdr, 8), titleRow, title: String(val(sh, titleRow, 1)), sub: String(val(sh, titleRow + 1, 1)),
           allocTitleRow, allocTitle: String(val(sh, allocTitleRow, 1)) };
}
const sized = v => v.seats.filter(s => s.alloc !== '' && s.alloc !== DASH && s.alloc !== null && Number(String(s.alloc).replace(/,/g, '')) > 0);
const PREFIX = why => 'PREVIEW ONLY ' + DASH + ' not tickets (' + why + '). ';
const NA_WHY = 'nothing executable today ' + DOT + ' feed NOT ACTIONABLE';

/* P2: the 3 Nov shape (research board, feed NOT ACTIONABLE) with the gate on (throws otherwise) */
function checkPreview(src, boardKey, why, allocTitle) {
  const n = build(src, 't10', { board: BOARDS[boardKey], props: ON }), o = build(OLD, 't10', { board: BOARDS[boardKey] });
  const v = pv(n), w = pv(o);
  eq(v.h7, 'Preview SAR', 'column 7 label'); eq(v.h8, 'Preview qty', 'column 8 label');
  eq(w.h7, 'Alloc SAR', 'v1.2.31 column 7 label');
  eq(v.title, w.title, 'SEATS title unchanged');
  eq(v.sub, PREFIX(why) + w.sub, 'SEATS line = the prefix + the v1.2.31 line');
  eq(v.allocTitle, allocTitle, 'ALLOCATION title');
  const sz = sized(v);
  ok(sz.length >= 1, 'at least one sized seat');
  deq(sz.map(s => s.sym), sized(w).map(s => s.sym), 'the same seats are sized');
  for (let i = 0; i < v.seats.length; i++) {
    const s = v.seats[i], t = w.seats[i];
    deq([s.sym, s.alloc, s.qty, s.risk, s.gain], [t.sym, t.alloc, t.qty, t.risk, t.gain], 'numbers unchanged ' + s.sym);
    if (sz.indexOf(s) >= 0) {
      eq(s.flags, 'preview ' + DOT + ' not a ticket' + (t.flags ? ' ' + DOT + ' ' + t.flags : ''), s.sym + ' flags: ' + s.flags);
      deq([s.fg, s.fw, s.fst, s.fg8, s.fw8, s.fst8], [GREY, 'normal', 'italic', GREY, 'normal', 'italic'], s.sym + ' preview format');
      deq([t.fg, t.fw], [GREEN, 'bold'], s.sym + ' v1.2.31 format (bold green)');
    } else {
      eq(s.flags, t.flags, s.sym + ' unsized: flags unchanged');
      deq([s.fg, s.fw, s.fst], [t.fg, t.fw, t.fst], s.sym + ' unsized: format unchanged');
    }
  }
  // scope: only the documented cells move
  const allowed = new Set([v.hdr + ':7', v.hdr + ':8', (v.titleRow + 1) + ':1', v.allocTitleRow + ':1']);
  for (const s of sz) { allowed.add(s.row + ':7'); allowed.add(s.row + ':8'); allowed.add(s.row + ':15'); }
  const d = diffKeys(n, o, T10);
  deq(d.filter(k => !allowed.has(k)), [], 'only the documented cells differ');
  ok(d.length >= 4, 'cells differ: ' + d.length);
  for (const name of Object.keys(n.sheets)) if (name !== T10) deq(norm(n).sheets[name], norm(o).sheets[name], 'sheet ' + name + ' unchanged');
  deq(norm(n).sheets[T10].charts, norm(o).sheets[T10].charts, 'charts unchanged');
  deq(norm(n).sheets[T10].merges, norm(o).sheets[T10].merges, 'merges unchanged');
  deq(norm(n).ret, norm(o).ret, 'return value unchanged');
  return { n, o, v, w };
}
function checkLive(src) {   // P3: an actionable page is the v1.2.31 page exactly
  for (const key of ['today', 'engineCaps', 'held', 'engineActiveUnfunded']) {
    const n = build(src, 't10', { board: BOARDS[key], props: ON }), o = build(OLD, 't10', { board: BOARDS[key] });
    deq(norm(n), norm(o), 'live EXECUTABLE engine board = v1.2.31 (' + key + ')');
  }
}
function checkGoldenNegative(src) {   // P1
  const boards = ['today', 'todayNA', 'todaySuper', 'research', 'nonStrict', 'researchExec', 'researchHeld'];
  for (const key of boards) {
    const o = norm(build(OLD, 't10', { board: BOARDS[key] }));
    for (const props of [{}, { [GATE]: '' }, { [GATE]: 'off' }, { [GATE]: '0' }, { [GATE]: 'no' }, { [GATE]: 'onn' }, { [GATE]: 'preview' }]) {
      deq(norm(build(src, 't10', { board: BOARDS[key], props })), o, 'gate ' + JSON.stringify(props) + ' = v1.2.31 (' + key + ')');
    }
    deq(norm(build(src, 't10', { board: BOARDS[key], props: ON, propsThrow: true })), norm(build(OLD, 't10', { board: BOARDS[key], propsThrow: true })), 'a property fault = v1.2.31 (' + key + ')');
    deq(norm(build(src, 't10', { board: BOARDS[key], props: { [GATE]: 'on', [LEGACY]: 'on' } })), norm(build(OLD, 't10', { board: BOARDS[key], props: { [LEGACY]: 'on' } })), 'DP2_COHORT_LEGACY wins over the gate (' + key + ')');
  }
  for (const key of ['today', 'research']) {
    deq(norm(build(src, 'both', { board: BOARDS[key] })), norm(build(OLD, 'both', { board: BOARDS[key] })), "'Refresh both' = v1.2.31 (" + key + ')');
  }
}
function checkNotLive(src) {   // P4
  checkPreview(src, 'todayNA', 'feed NOT ACTIONABLE', 'ENGINE TICKETS NOT LIVE (feed NOT ACTIONABLE) ' + DASH + ' the engine\'s tickets, mirrored');
  checkPreview(src, 'todaySuper', 'feed SYNC RUNNING', 'ENGINE TICKETS NOT LIVE (feed SYNC RUNNING) ' + DASH + ' the engine\'s tickets, mirrored');
  // EXPIRED: the same live board built after its 10:42 expiry
  const late = Date.parse('2026-11-02T08:00:00Z');   // 11:00 Riyadh
  const n = build(src, 't10', { board: BOARDS.today, props: ON, now: late }), o = build(OLD, 't10', { board: BOARDS.today, now: late });
  const v = pv(n), w = pv(o);
  eq(v.allocTitle, 'ENGINE TICKETS NOT LIVE (feed EXPIRED) ' + DASH + ' the engine\'s tickets, mirrored', 'EXPIRED title');
  eq(v.sub, PREFIX('feed EXPIRED') + w.sub, 'EXPIRED SEATS line');
  deq(v.seats.map(s => [s.alloc, s.qty]), w.seats.map(s => [s.alloc, s.qty]), 'EXPIRED: the engine tickets unchanged');
}

function battery() {
  const digest = [];
  test('P0 oracle: v1.2.31 rebuilt from the delivered file (SHA-256 pinned); version lockstep 1.2.32; both files compile; the base lacks the new names', () => {
    eq(sha256(OLD), V1231_SHA256, 'v1.2.31');
    ok(/^ \* Version: 1\.2\.3[23] \(2026-11-03\)$/m.test(NEW) || /^ \* Version: 1\.2\.34 \(2026-11-05\)$/m.test(NEW), 'header Version line 1.2.32 or later');
    ok(/^ \* Version: 1\.2\.32 \(2026-11-03\)$/m.test(S1232), 'header Version line 1.2.32 (rebuilt)');
    ok(S1232.indexOf("var DP2_VERSION = '1.2.32';   // lockstep with the header") > 0, 'DP2_VERSION 1.2.32 (rebuilt)');
    ok(/^ \* Version: 1\.2\.31 \(2026-11-02\)$/m.test(OLD) && OLD.indexOf("var DP2_VERSION = '1.2.31';") > 0, 'base is 1.2.31');
    for (const s of [NEW, OLD]) new vm.Script(s, { filename: '27_Decision_Pages_v2.gs' });
    const e = makeEnv(OLD, {});
    for (const k of ['dp2PreviewWhy_', 'dp2PreviewLabelOn_', 'DP2_PREVIEW_LABEL_PROP']) eq(typeof e.ctx[k], 'undefined', 'the base lacks ' + k);
    eq(makeEnv(NEW, {}).ctx.DP2_PREVIEW_LABEL_PROP, GATE, 'the gate name');
  });

  test('P1 GOLDEN NEGATIVE: the gate absent / blank / junk / a property fault, or DP2_COHORT_LEGACY on -> both pages = the v1.2.31 build (every cell, format, merge, chart, call, return value) on seven boards', () => {
    checkGoldenNegative(NEW);
    digest.push(sha256(JSON.stringify(norm(build(NEW, 't10', { board: BOARDS.research })))));
  });

  test('P2 the 3 Nov shape (research board, WITHHELD, feed NOT ACTIONABLE), gate on: Preview SAR / Preview qty; the SEATS prefix; sized seats grey italic with "preview . not a ticket"; SIZING PREVIEW title; numbers unchanged; only the documented cells differ', () => {
    const r = checkPreview(NEW, 'research', NA_WHY, 'SIZING PREVIEW (not tickets) ' + DASH + ' research mirror of the engine\'s conviction sizing');
    eq(sized(r.v).length, 7, 'the seven active seats are sized (the water-fill)');
    checkPreview(NEW, 'nonStrict', NA_WHY, 'SIZING PREVIEW (not tickets) ' + DASH + ' research mirror of the engine\'s conviction sizing');
    checkPreview(NEW, 'researchHeld', NA_WHY, 'SIZING PREVIEW (not tickets) ' + DASH + ' research mirror of the engine\'s conviction sizing');
    ok(r.v.sub.indexOf('PREVIEW ONLY ' + DASH + ' not tickets (nothing executable today ' + DOT + ' feed NOT ACTIONABLE). Alloc / Buy qty = research allocation') === 0, 'the line reads: ' + r.v.sub.slice(0, 140));
    digest.push(sha256(JSON.stringify(norm(r.n).sheets[T10].cells)));
  });

  test('P3 a live EXECUTABLE engine board (today 09:24), gate on: the v1.2.31 page exactly (both pages, every byte)', () => {
    checkLive(NEW);
    deq(norm(build(NEW, 'both', { board: BOARDS.today, props: ON })), norm(build(OLD, 'both', { board: BOARDS.today })), "'Refresh both' on the live board = v1.2.31");
  });

  test('P4 an engine board whose release is not live (NOT ACTIONABLE, SYNC RUNNING, EXPIRED), gate on: ENGINE TICKETS NOT LIVE (feed ..) and the preview labels; the engine tickets unchanged', () => {
    checkNotLive(NEW);
  });

  test('P5 a research board on a live EXECUTABLE feed (nothing funded), gate on: why = nothing executable today (no feed suffix)', () => {
    checkPreview(NEW, 'researchExec', 'nothing executable today', 'SIZING PREVIEW (not tickets) ' + DASH + ' research mirror of the engine\'s conviction sizing');
  });

  test('P6 dp2PreviewWhy_ (pure) and the gate spellings; one property read per Top-10 build', () => {
    const c = makeEnv(NEW, {}).ctx;
    const live = { live: true, label: 'EXECUTABLE' };
    eq(c.dp2PreviewWhy_(6, live), '', 'engine tickets on a live release: actionable');
    eq(c.dp2PreviewWhy_(1, live), '', 'one ticket: actionable');
    eq(c.dp2PreviewWhy_(0, live), 'nothing executable today', 'no ticket, live feed');
    eq(c.dp2PreviewWhy_(6, { live: false, label: 'EXPIRED' }), 'feed EXPIRED', 'tickets, expired feed');
    eq(c.dp2PreviewWhy_(6, { live: false, label: 'NOT ACTIONABLE' }), 'feed NOT ACTIONABLE', 'tickets, withdrawn feed');
    eq(c.dp2PreviewWhy_(0, { live: false, label: 'SYNC RUNNING' }), 'nothing executable today ' + DOT + ' feed SYNC RUNNING', 'nothing, sync running');
    eq(c.dp2PreviewWhy_(6, null), 'feed n/a', 'no feed view');
    eq(c.dp2PreviewWhy_(0, { live: 'yes', label: 'EXECUTABLE' }), 'nothing executable today ' + DOT + ' feed EXECUTABLE', 'live must be exactly true');
    for (const [v, want] of [['1', true], ['on', true], ['ON', true], [' On ', true], ['true', true], ['yes', true], ['Yes', true], ['1 ', true],
      ['', false], ['0', false], ['off', false], ['no', false], ['onn', false], ['enable', false], ['preview', false]]) {
      eq(makeEnv(NEW, { props: { [GATE]: v } }).ctx.dp2PreviewLabelOn_(), want, 'gate ' + JSON.stringify(v));
    }
    eq(makeEnv(NEW, { props: ON, propsThrow: true }).ctx.dp2PreviewLabelOn_(), false, 'a property fault reads off');
    const n = build(NEW, 't10', { board: BOARDS.research, props: ON }), o = build(OLD, 't10', { board: BOARDS.research, props: ON });
    eq(n.env.reads[GATE], 1, 'the gate is read once per Top-10 build');
    eq(o.env.reads[GATE] || 0, 0, 'the base never reads it');
    const lg = build(NEW, 't10', { board: BOARDS.research, props: { [GATE]: 'on', [LEGACY]: 'on' } });
    eq(lg.env.reads[GATE] || 0, 0, 'under DP2_COHORT_LEGACY the gate is not read');
  });

  test('P7 Portfolio_Decision_v2 is untouched by the gate (every board, gate on)', () => {
    for (const key of ['today', 'todayNA', 'research', 'researchExec']) {
      deq(norm(build(NEW, 'pd', { board: BOARDS[key], props: ON })), norm(build(OLD, 'pd', { board: BOARDS[key] })), 'Portfolio_Decision_v2 = v1.2.31 (' + key + ')');
    }
  });

  test('P8 ES5 / ASCII lint of the v1.2.32 diff (on the rebuilt v1.2.32 file); inventory (2 functions + 1 var added, 0 removed); the replaced base lines pinned; 11 hunks whose reversal is the v1.2.31 file; WHY block', () => {
    const bl = OLD.split('\n'), nl = S1232.split('\n');
    const baseSet = new Set(bl), newSet = new Set(nl);
    const added = nl.filter(l => !baseSet.has(l));
    ok(added.length >= 25 && added.length <= 60, 'added lines ' + added.length);
    for (const l of added) {
      const code = l.replace(/\/\/.*$/, '');
      ok(!/\b(let|const|class)\s|=>|`|\.\.\.|\.includes\(|\.startsWith\(|\.endsWith\(|\.padStart\(|\.find\(/.test(code) || /^\s*(\*|\/\*)/.test(l), 'ES6 token: ' + l.slice(0, 120));
      ok(/^[\x09\x20-\x7e]*$/.test(l), 'non-ASCII added line: ' + l.slice(0, 120));
    }
    ok(S1232.indexOf('\r') < 0, 'LF only');
    for (const name of ['dp2PreviewLabelOn_', 'dp2PreviewWhy_']) {
      const start = S1232.indexOf('function ' + name + '(');
      ok(start > 0, 'defined ' + name);
      const body = S1232.slice(start, S1232.indexOf('\n}\n', start) + 3);
      ok(/^[\x00-\x7f]*$/.test(body), 'ASCII ' + name);
      new vm.Script('(function(){' + body + '})');
    }
    const names = (src, re) => { const out = {}; let m; while ((m = re.exec(src))) out[m[1]] = true; return out; };
    const fb = names(OLD, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), fn = names(S1232, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    deq(Object.keys(fb).filter(k => !fn[k]), [], 'top-level functions removed');
    deq(Object.keys(fn).filter(k => !fb[k]).sort(), ['dp2PreviewLabelOn_', 'dp2PreviewWhy_'], 'top-level functions added');
    const vb = names(OLD, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = names(S1232, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq(Object.keys(vb).filter(k => !vn[k]), [], 'vars removed');
    deq(Object.keys(vn).filter(k => !vb[k]), ['DP2_PREVIEW_LABEL_PROP'], 'vars added');
    const REMOVED = ['* Version: 1.2.31 (2026-11-02)', "var DP2_VERSION = '1.2.31';", 'if (!cohort.legacy) row = dp2Section_(sh, row, 1, 16, dp2SeatsTitle_(seats.length, cohort, coN), dp2SeatsSub_(',
      "if (sState[i] === 'FUNDED') ac.setFontColor(DP2C.green); else if", "if (cohort.basis === 'engine') row = dp2Section_(sh, row, 1, 8, 'ALLOCATION \\u2014 the engine",
      "row = dp2Section_(sh, row, 1, 8, 'ALLOCATION " + DASH + " research mirror"];
    const removed = bl.filter(l => !newSet.has(l)).map(l => l.trim());
    eq(removed.length, REMOVED.length, 'base lines replaced: ' + JSON.stringify(removed.map(x => x.slice(0, 60))));
    for (let i = 0; i < removed.length; i++) ok(removed[i].indexOf(REMOVED[i]) === 0, 'replaced base line ' + i + ': ' + removed[i].slice(0, 120));
    let pos = 0; for (const l of bl) { if (!newSet.has(l)) continue; const at = nl.indexOf(l, pos); ok(at >= pos, 'base line out of order: ' + l.slice(0, 80)); pos = at + 1; }
    const why = S1232.slice(S1232.indexOf(' *   1.2.32: '), S1232.indexOf(' *   1.2.31: '));
    ok(why.length > 1200 && /^[\x09\x0a\x20-\x7e]*$/.test(why), 'WHY block (1.2.32) present, newest first, ASCII');
    for (const w of ['DP2_PREVIEW_LABEL', 'default OFF', 'Rollback: delete DP2_PREVIEW_LABEL']) ok(why.indexOf(w) >= 0, 'WHY names ' + w);
    for (const l of bl.filter(x => /^ \*   1\.\d+\.\d+[: ]/.test(x))) ok(S1232.indexOf(l) > 0, 'prior WHY entry kept: ' + l.slice(0, 60));
    eq(reverseEdits(S1232), OLD, 'hunk reversal = the v1.2.31 file');
    eq(EDITS.length, 11, 'eleven hunks');
    digest.push(String(EDITS.length));
  });

  test('P9 mutants: each mutation of the delivered file fails at least one section check', () => {
    const M = [
      ['gate always on', 'function dp2PreviewLabelOn_() { return /^(1|on|true|yes)$/i.test(', 'function dp2PreviewLabelOn_() { return true || /^(1|on|true|yes)$/i.test(', [checkGoldenNegative]],
      ['gate never on', 'function dp2PreviewLabelOn_() { return /^(1|on|true|yes)$/i.test(', 'function dp2PreviewLabelOn_() { return false && /^(1|on|true|yes)$/i.test(', [s => checkPreview(s, 'research', NA_WHY, 'SIZING PREVIEW (not tickets) ' + DASH + ' research mirror of the engine\'s conviction sizing')]],
      ['live page labelled', '  if (executable > 0 && live) return \'\';\n', '', [checkLive]],
      ['legacy not respected', "var pvWhy = (!cohort.legacy && dp2PreviewLabelOn_())", 'var pvWhy = (dp2PreviewLabelOn_()', [checkGoldenNegative]],
      ['cells stay green', "if (sState[i] === 'FUNDED' && pvWhy) ac.setFontColor(DP2C.grey)", "if (sState[i] === 'FUNDED' && pvWhy) ac.setFontColor(DP2C.green)", [s => checkPreview(s, 'research', NA_WHY, 'SIZING PREVIEW (not tickets) ' + DASH + ' research mirror of the engine\'s conviction sizing')]],
      ['flag on unsized seats', 'if (pvWhy && shares !== null) flags.unshift(', 'if (pvWhy) flags.unshift(', [s => checkPreview(s, 'research', NA_WHY, 'SIZING PREVIEW (not tickets) ' + DASH + ' research mirror of the engine\'s conviction sizing')]],
      ['column labels kept', "if (pvWhy) { sHdr[5] = 'Preview SAR'; sHdr[6] = 'Preview qty'; }", 'if (false) {}', [s => checkPreview(s, 'research', NA_WHY, 'SIZING PREVIEW (not tickets) ' + DASH + ' research mirror of the engine\'s conviction sizing')]],
      ['engine title not relabelled', "(pvWhy ? 'ENGINE TICKETS NOT LIVE (' + pvWhy + ')' : 'ALLOCATION')", "'ALLOCATION'", [checkNotLive]],
      ['the feed suffix dropped', "return 'nothing executable today' + (live ? '' : ' \\u00b7 feed ' + lbl);", "return 'nothing executable today';", [s => checkPreview(s, 'research', NA_WHY, 'SIZING PREVIEW (not tickets) ' + DASH + ' research mirror of the engine\'s conviction sizing')]]
    ];
    let killed = 0;
    for (const [name, find, repl, checks] of M) {
      eq(NEW.split(find).length - 1, 1, 'mutant anchor occurs once: ' + name);
      const mutant = NEW.replace(find, () => repl);
      let k = false;
      for (const ch of checks) { try { ch(mutant); } catch (e) { k = true; break; } }
      ok(k, 'mutant survived: ' + name);
      killed++;
    }
    eq(killed, M.length, 'every mutant killed');
    digest.push(String(killed));
  });
  return sha256(digest.join('|')).slice(0, 16);
}

const t0 = Date.now();
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
console.log('DP2 V1232 P0-P9 x2 identical digest ' + digests[0] + ' | new SHA256 ' + sha256(NEW) + ' | v1.2.31 SHA256 ' + sha256(OLD) + ' | ' + Math.round((Date.now() - t0) / 1000) + 's');
console.log('PASS ' + passed + '/' + total);
