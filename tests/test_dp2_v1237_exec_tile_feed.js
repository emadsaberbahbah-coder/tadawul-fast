#!/usr/bin/env node
/* tests/test_dp2_v1237_exec_tile_feed.js
 * Harness for apps_script/27_Decision_Pages_v2.gs v1.2.37 [EXEC-TILE-FEED] (the v1.2.31 follow-up: Top_10_Investments_v2's
 * Executable tile counted the seats with a Ticket SAR and was green whatever the decision feed said; Mon 2 Nov the
 * 10:42 release expired and every hourly build until the 17:32 render showed 'Executable 6' green under an EXPIRED /
 * SYNC RUNNING chip). Script Property DP2_EXEC_TILE_FEED = 1 / on / true / yes (default OFF = the v1.2.36 pages byte for
 * byte): when the board carries an engine ticket and the feed has a state that is not live, the tile is grey and its sub
 * reads 'feed <state> - not executable now - of N . <output>'; the label and the count stay.
 *
 * Two trees over the REAL file, each in its own vm context: the delivered source (NEW) and the v1.2.36 file (OLD,
 * SHA-256 pinned 487c0778 = origin/main 86edd72), by default rebuilt from NEW by reversing the enumerated v1.2.37 hunks
 * (tests/js/dp2_edits_v1237.json). The GAS shim, the fixture and the boards are those of
 * tests/test_dp2_v1236_evidence_since.js, copied verbatim (norm folds 1.2.37; build() takes an optional dp2Tiles_ hook,
 * used to build the EXPECTED armed page from the v1.2.36 source with only the Executable tile changed by the harness).
 *   P0 oracle, version lockstep, inventory (+3 functions, +2 line-start vars, 0 removed), 5 hunks reverse to the base
 *   P1 GOLDEN NEGATIVE / KILL: absent, blank, off words, near misses, a fault -> the v1.2.36 build on every board
 *   P2 armed, nothing to correct (live, no ticket, research under a live feed, no feed key) -> the v1.2.36 build
 *   P3 armed, release not live (EXPIRED, SYNC RUNNING, NOT ACTIONABLE; DP2_COHORT_LEGACY) -> only the tile changes
 *   P4 the export audit's view of the tile (label cell, numeric value) is unchanged
 *   P5 pure: the tile rule and the core check
 *   P6 the gate: one property read per Top-10 build; Portfolio_Decision_v2 alone never reads it; a fault reads off
 *   P7 dp2SelfTest: the v1.2.36 report while off; one line while on
 *   P8 lint: ES5; no non-ASCII character in a hunk; LF; WHY newest-first; every prior WHY kept
 *   P9 mutants
 * The battery runs twice and the two digests must match; the last line is 'PASS k/k' (exit 1 on any failure).
 * Usage: node tests/test_dp2_v1237_exec_tile_feed.js [new.gs] [v1.2.36.gs]
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
const EDITS = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'dp2_edits_v1237.json'), 'utf8'));
const V1236_SHA256 = '487c0778e1f509eb9ee60e91d4603ed884367719af1b065e6231b723bb1f10f3';   // v1.2.36 (origin/main 86edd72)
const NOW = Date.parse(FIX.frozen_now);   // 2026-11-02T06:24:00Z = 09:24 Riyadh (the release is live until 10:42)
const GATE = 'DP2_EXEC_TILE_FEED';
const DOT = '\u00b7', DASH = '\u2014', ARROW = '\u2192', NDASH = '\u2013';
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
    eq(s.split(cur).length - 1, 1, 'exactly one approved v1.2.37 hunk: ' + cur.split('\n')[0].slice(0, 90));
    s = s.replace(cur, () => prev);
  }
  eq(sha256(s), V1236_SHA256, 'v1.2.37 changes only its enumerated hunks (their reversal = the v1.2.36 file)');
  return s;
}
let OLD = null, oldErr = '';
try {
  OLD = OLD_PATH ? fs.readFileSync(OLD_PATH, 'utf8') : reverseEdits(NEW);
  eq(sha256(OLD), V1236_SHA256, 'v1.2.36 SHA-256');
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
  top.banner = '\u2139 FEED NOT ACTIONABLE ' + DASH + ' coverage_failed ' + DASH + ' SELECTED ' + DASH + ' 0 EXECUTABLE / 8 QUALIFIED PLANS + 2 GRACE-HELD (NO PLAN TODAY) ' + DASH + ' SIZING WITHHELD';
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
  if (opts && opts.tileHook) { const orig = env.ctx.dp2Tiles_, hook = opts.tileHook; env.ctx.dp2Tiles_ = function (sh, row, tiles) { return orig(sh, row, hook(sh, tiles)); }; }
  const fn = which === 'pd' ? 'buildPortfolioDecisionV2' : (which === 't10' ? 'buildTop10V2' : 'buildDecisionPagesV2');
  const ret = env.ctx[fn]();
  const sheets = {};
  for (const [n, s] of env.book._sheets) sheets[n] = s._state();
  return { env, ret: plain(ret), sheets, calls: env.book.calls.slice(), toasts: env.book.toasts.slice(), logs: env.logs.slice(), mails: env.mails.slice() };
}
function norm(b) {   // the only bytes the pages may differ in while the gate is off: the version printed on both footers / the toast title
  const strip = o => { const x = Object.assign({}, o); delete x.env; return x; };
  let s = JSON.stringify(strip(b));
  for (const v of ['1.2.37', '1.2.36', '1.2.35', '1.2.34', '1.2.33', '1.2.32', '1.2.31']) s = s.split('Decision Pages v2 ' + v).join('Decision Pages v2 1.2.30');
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





/* ============================================================ v1.2.37 [EXEC-TILE-FEED] */
const T10 = 'Top_10_Investments_v2', PDV = 'Portfolio_Decision_v2';
const NO_NET = { fetch() { return { getResponseCode: () => 404, getContentText: () => '{}' }; } };
const AT_1100 = Date.parse('2026-11-02T08:00:00Z');   // 11:00 Riyadh: the 10:42 release has expired, no re-render since
const GREY = makeEnv(NEW, {}).ctx.DP2C.grey, GREEN = makeEnv(NEW, {}).ctx.DP2C.green, RED = makeEnv(NEW, {}).ctx.DP2C.red;
const ON = { [GATE]: 'on' };
const LEGACY = { DP2_COHORT_LEGACY: 'on' };
BOARDS.noFeed = board('today, no TFB Decision Feed key', null, kv => { delete kv['TFB Decision Feed']; });
/* the scenarios: [key, board, now, the feed label the armed tile must name or '' = unchanged] */
const SCEN = {
  live: ['live', BOARDS.today, NOW, ''],
  expired: ['expired', BOARDS.today, AT_1100, 'EXPIRED'],
  syncRunning: ['syncRunning', BOARDS.todaySuper, NOW, 'SYNC RUNNING'],
  notActionable: ['notActionable', BOARDS.todayNA, NOW, 'NOT ACTIONABLE'],
  research: ['research', BOARDS.research, NOW, ''],
  researchExec: ['researchExec', BOARDS.researchExec, NOW, ''],
  noFeed: ['noFeed', BOARDS.noFeed, AT_1100, '']
};
const SUB_PREFIX = lbl => 'feed ' + lbl + ' - not executable now - ';
/* the expected page: the v1.2.36 build with ONLY the Executable tile changed, computed by the harness (not by the code under test) */
const expectHook = lbl => (sh, tiles) => tiles.map(t => (t.label === 'Executable' ? { label: t.label, value: t.value, sub: SUB_PREFIX(lbl) + t.sub, color: GREY } : t));
function tileOf(b, label) { const t = t10View(b).tile(label); ok(t, 'the ' + label + ' tile'); return t; }
const OFF_RAW = [undefined, '', 'off', '0', 'false', 'no', 'enable', 'enabled', 'onn', 'y', '2', 'on;', 'yes please'];
const ON_RAW = ['1', 'on', 'ON', ' on ', 'true', 'True', 'yes', 'YES'];

function checkGoldenNegative(src, parts) {
  for (const raw of OFF_RAW) {
    const props = raw === undefined ? {} : { [GATE]: raw };
    for (const k of ['live', 'expired', 'syncRunning', 'notActionable']) {
      const [, bd, now] = SCEN[k];
      const n = build(src, 't10', { board: bd, now, props, fetch: NO_NET }), o = build(OLD, 't10', { board: bd, now, props, fetch: NO_NET });
      deq(norm(n), norm(o), 'gate ' + JSON.stringify(raw) + ', ' + k);
    }
  }
  for (const k of ['expired', 'research', 'noFeed']) {
    const [, bd, now] = SCEN[k];
    deq(norm(build(src, 'both', { board: bd, now, fetch: NO_NET })), norm(build(OLD, 'both', { board: bd, now, fetch: NO_NET })), 'gate absent, both pages, ' + k);
    deq(norm(build(src, 'both', { board: bd, now, props: Object.assign({}, LEGACY), fetch: NO_NET })), norm(build(OLD, 'both', { board: bd, now, props: Object.assign({}, LEGACY), fetch: NO_NET })), 'gate absent, DP2_COHORT_LEGACY, ' + k);
  }
  for (const props of [ON, {}]) {
    const f1 = build(src, 'both', { board: BOARDS.today, now: AT_1100, props, propsThrow: true, fetch: NO_NET }), f0 = build(OLD, 'both', { board: BOARDS.today, now: AT_1100, props, propsThrow: true, fetch: NO_NET });
    deq(norm(f1), norm(f0), 'a property fault reads off (' + JSON.stringify(props) + ')');
  }
  for (const k of ['expired', 'syncRunning']) {   // armed, Portfolio_Decision_v2 alone is untouched
    const [, bd, now] = SCEN[k];
    deq(norm(build(src, 'pd', { board: bd, now, props: ON, fetch: NO_NET })), norm(build(OLD, 'pd', { board: bd, now, props: ON, fetch: NO_NET })), 'armed, Portfolio_Decision_v2 alone, ' + k);
  }
  parts.push(norm(build(src, 't10', { board: BOARDS.today, now: AT_1100, fetch: NO_NET })).ret);
}
function checkArmedUnchanged(src, parts) {
  for (const k of ['live', 'research', 'researchExec', 'noFeed']) {
    for (const extra of [{}, LEGACY]) {
      const props = Object.assign({}, ON, extra);
      const [, bd, now] = SCEN[k];
      const n = build(src, 'both', { board: bd, now, props, fetch: NO_NET }), o = build(OLD, 'both', { board: bd, now, props, fetch: NO_NET });
      deq(norm(n), norm(o), 'armed ' + JSON.stringify(extra) + ', ' + k + ' = v1.2.36');
    }
  }
  const lv = tileOf(build(src, 't10', { board: BOARDS.today, props: ON, fetch: NO_NET }), 'EXECUTABLE');
  eq(lv.color, GREEN, 'a live release keeps the green tile'); eq(lv.value, '6', 'six tickets'); eq(lv.sub, 'of 10 ' + DOT + ' EXECUTABLE', 'the live sub');
  const rs = tileOf(build(src, 't10', { board: BOARDS.research, props: ON, fetch: NO_NET }), 'EXECUTABLE');
  eq(rs.color, RED, 'nothing executable keeps the red tile'); eq(rs.value, '0', 'no ticket');
  parts.push([lv, rs]);
}
function checkArmedGrey(src, parts) {
  for (const k of ['expired', 'syncRunning', 'notActionable']) {
    for (const extra of [{}, LEGACY]) {
      for (const raw of ON_RAW) {
        if (raw !== 'on' && Object.keys(extra).length) continue;
        const props = Object.assign({ [GATE]: raw }, extra);
        const [, bd, now, lbl] = SCEN[k];
        const n = build(src, 'both', { board: bd, now, props, fetch: NO_NET });
        const e = build(OLD, 'both', { board: bd, now, props, fetch: NO_NET, tileHook: expectHook(lbl) });
        deq(norm(n), norm(e), 'armed ' + JSON.stringify(raw) + ' ' + JSON.stringify(extra) + ', ' + k + ': only the Executable tile changes');
        const t = tileOf(n, 'EXECUTABLE'), o = tileOf(build(OLD, 'both', { board: bd, now, props, fetch: NO_NET }), 'EXECUTABLE');
        eq(t.value, '6', 'the count stays'); eq(t.value, o.value, 'the count = v1.2.36');
        eq(t.color, GREY, 'grey'); eq(o.color, GREEN, 'v1.2.36 painted it green');
        eq(t.sub, SUB_PREFIX(lbl) + 'of 10 ' + DOT + ' EXECUTABLE', 'the sub names the feed');
        eq(t.row, o.row, 'same row'); eq(t.col, o.col, 'same column');
      }
    }
  }
  // the header chip already says the state; the tile now agrees with it
  const x = build(src, 't10', { board: BOARDS.today, now: AT_1100, props: ON, fetch: NO_NET });
  eq(String(val(sheet(x, T10), 1, 11)), 'EXPIRED', 'the header chip');
  parts.push(tileOf(x, 'EXECUTABLE'));
}
function checkExportAudit(src, parts) {
  // scripts/tfb_export_audit.py _view_kpis finds the label row by the upper-cased 'EXECUTABLE' and reads a number below it
  for (const k of ['live', 'expired', 'syncRunning', 'notActionable', 'research']) {
    const [, bd, now] = SCEN[k];
    const n = build(src, 't10', { board: bd, now, props: ON, fetch: NO_NET }), o = build(OLD, 't10', { board: bd, now, props: ON, fetch: NO_NET });
    const tn = tileOf(n, 'EXECUTABLE'), to = tileOf(o, 'EXECUTABLE');
    eq(val(sheet(n, T10), tn.row, tn.col), 'EXECUTABLE', 'the label cell (' + k + ')');
    ok(/^\d[\d,]*$/.test(String(tn.value)), 'a numeric value (' + k + '): ' + tn.value);
    eq(tn.value, to.value, 'the value = v1.2.36 (' + k + ')');
  }
  parts.push('audit ok');
}
function checkPure(src, parts) {
  const e = makeEnv(src, {}).ctx;
  const T = { label: 'Executable', value: '6', sub: 'of 10 ' + DOT + ' EXECUTABLE', color: GREEN };
  const f = (x, st, fv) => plain(e.dp2ExecTileFeedTile_(T, x, st, fv));
  deq(f(6, 'EXECUTABLE', { label: 'EXPIRED', live: false }), { label: 'Executable', value: '6', sub: 'feed EXPIRED - not executable now - of 10 ' + DOT + ' EXECUTABLE', color: GREY }, 'expired');
  deq(f(6, 'NOT_ACTIONABLE', { label: 'SYNC RUNNING', live: false }).sub, SUB_PREFIX('SYNC RUNNING') + T.sub, 'sync running');
  deq(f(6, 'NOT_ACTIONABLE', { live: false }).sub, SUB_PREFIX('n/a') + T.sub, 'no label');
  ok(e.dp2ExecTileFeedTile_(T, 6, 'EXECUTABLE', { label: 'EXECUTABLE', live: true }) === T, 'live: the same object');
  ok(e.dp2ExecTileFeedTile_(T, 6, 'ACTIONABLE', { label: 'ACTIONABLE', live: true }) === T, 'legacy ACTIONABLE live: the same object');
  for (const x of [0, -1, null, undefined, NaN]) ok(e.dp2ExecTileFeedTile_(T, x, 'EXECUTABLE', { label: 'EXPIRED', live: false }) === T, 'executable ' + x + ': the same object');
  for (const st of ['', null, undefined]) ok(e.dp2ExecTileFeedTile_(T, 6, st, { label: 'n/a', live: false }) === T, 'feed state ' + JSON.stringify(st) + ': the same object');
  ok(e.dp2ExecTileFeedTile_(T, 6, 'EXECUTABLE', null) === T, 'no feed view: the same object');
  eq(T.color, GREEN, 'the input tile is not mutated'); eq(T.sub, 'of 10 ' + DOT + ' EXECUTABLE', 'the input sub is not mutated');
  eq(e.dp2ExecTileFeedCoreOk_(), true, 'the self-test core');
  parts.push(f(6, 'EXECUTABLE', { label: 'EXPIRED', live: false }));
}
function checkGate(src) {
  for (const props of [ON, {}, { [GATE]: 'junk' }]) {
    for (const which of ['both', 't10']) {
      const b = build(src, which, { board: BOARDS.today, now: AT_1100, props, fetch: NO_NET });
      eq(b.env.reads[GATE], 1, which + ': one read (' + JSON.stringify(props) + ')');
    }
    const p = build(src, 'pd', { board: BOARDS.today, now: AT_1100, props, fetch: NO_NET });
    eq(p.env.reads[GATE], undefined, 'Portfolio_Decision_v2 alone never reads it (' + JSON.stringify(props) + ')');
    const lv = build(src, 't10', { board: BOARDS.today, props, fetch: NO_NET });
    eq(lv.env.reads[GATE], 1, 'read on a live board too (' + JSON.stringify(props) + ')');
  }
  const e = makeEnv(src, { props: ON }); e.ctx.dp2ExecTileFeedOn_(); e.ctx.dp2ExecTileFeedOn_();
  eq(e.reads[GATE], 1, 'memoised: one read per execution');
  eq(makeEnv(src, { props: ON, propsThrow: true }).ctx.dp2ExecTileFeedOn_(), false, 'a fault reads off');
  for (const raw of OFF_RAW) eq(makeEnv(src, { props: raw === undefined ? {} : { [GATE]: raw } }).ctx.dp2ExecTileFeedOn_(), false, 'off word ' + JSON.stringify(raw));
  for (const raw of ON_RAW) eq(makeEnv(src, { props: { [GATE]: raw } }).ctx.dp2ExecTileFeedOn_(), true, 'on word ' + JSON.stringify(raw));
  eq(OLD.indexOf(GATE), -1, 'v1.2.36 never names the property');
}
function checkSelfTest(src, parts) {
  const run = (s, opts) => { const env = makeEnv(s, Object.assign({ fetch: NO_NET }, opts)); return plain(env.ctx.dp2SelfTest()); };
  const fold = x => x.name.split('1.2.37').join('v').split('1.2.36').join('v');
  const n0 = run(src, {}), o0 = run(OLD, {});
  deq(n0.checks.map(x => [fold(x), x.ok]), o0.checks.map(x => [fold(x), x.ok]), 'off: the v1.2.36 report');
  const n1 = run(src, { props: ON }), o1 = run(OLD, { props: ON });
  eq(n1.checks.length, o1.checks.length + 1, 'on: one more line');
  const at = n1.checks.findIndex(x => /^executable tile follows the feed/.test(x.name));
  ok(at > 0, 'the line');
  eq(n1.checks[at].name, 'executable tile follows the feed (DP2_EXEC_TILE_FEED): on ' + DOT + ' core ok (grey and "feed <state> - not executable now" once the release is not live)', 'the text');
  eq(n1.checks[at].ok, true, 'ok');
  eq(n1.checks[at - 1].name, 'symbol keys (GOOGLEFINANCE / chart API)', 'after the symbol-key line (the other optional lines are off)');
  deq(n1.checks.filter((x, i) => i !== at).map(x => [fold(x), x.ok]), o1.checks.map(x => [fold(x), x.ok]), 'every other line = v1.2.36');
  const n2 = run(src, { props: Object.assign({ DP2_EVIDENCE_SINCE: '2026-10-20' }, ON) });
  const i2 = n2.checks.findIndex(x => /^executable tile follows the feed/.test(x.name));
  ok(/^evidence since/.test(n2.checks[i2 - 1].name), 'after the v1.2.36 evidence since line when that is on');
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
  eq(vs[0], '1.2.37', 'the newest WHY first');
  eq(vs[1], '1.2.36', 'then 1.2.36');
  const oldLines = hdr(OLD).split('\n').filter(l => !/^ \* Version: /.test(l));
  for (const l of oldLines) ok(hdr(NEW).indexOf(l) >= 0, 'prior WHY kept: ' + l.slice(0, 90));
  eq(hdr(NEW).split('\n')[3], ' * Version: 1.2.37 (2026-11-09)', 'the version line');
}
/* one scenario signature for the mutants */
function signature(src) {
  const out = [];
  for (const k of ['live', 'expired', 'syncRunning', 'research', 'noFeed']) {
    const [, bd, now] = SCEN[k];
    const b = build(src, 't10', { board: bd, now, props: ON, fetch: NO_NET });
    out.push(tileOf(b, 'EXECUTABLE'), b.env.reads[GATE]);
  }
  const p = build(src, 'pd', { board: BOARDS.today, now: AT_1100, props: ON, fetch: NO_NET });
  out.push(p.env.reads[GATE] === undefined);
  const off = build(src, 't10', { board: BOARDS.today, now: AT_1100, props: {}, fetch: NO_NET });
  out.push(tileOf(off, 'EXECUTABLE'));
  const e = makeEnv(src, { props: ON }).ctx;
  out.push(e.dp2ExecTileFeedCoreOk_(), plain(makeEnv(src, { props: { [GATE]: 'enable' } }).ctx.dp2ExecTileFeedOn_()));
  const m = makeEnv(src, { props: ON }); m.ctx.dp2ExecTileFeedOn_(); m.ctx.dp2ExecTileFeedOn_(); out.push(m.reads[GATE]);   // memoised
  return JSON.stringify(out);
}
const MUTANTS = [
  ['live ignored', "|| fv.live === true) return tile;", ") return tile;"],
  ['no ticket ignored', "if (!(executable > 0) || !feedState", "if (!feedState"],
  ['no feed state ignored', "|| !feedState || !fv ||", "|| !fv ||"],
  ['stays green', "' - not executable now - ' + tile.sub, color: DP2C.grey };", "' - not executable now - ' + tile.sub, color: tile.color };"],
  ['sub unchanged', "sub: 'feed ' + (fv.label || 'n/a') + ' - not executable now - ' + tile.sub,", "sub: tile.sub,"],
  ['label renamed', "return { label: tile.label, value: tile.value,", "return { label: 'Executable (feed ' + fv.label + ')', value: tile.value,"],
  ['default on', "DP2_ETF_ = /^(1|on|true|yes)$/i.test(", "DP2_ETF_ = !/^(0|off|false|no)$/i.test("],
  ['read only when not live', "  if (dp2ExecTileFeedOn_()) { for (var tx = 0;", "  if (!fv.live && dp2ExecTileFeedOn_()) { for (var tx = 0;"],
  ['not memoised', "DP2_ETF_READ_ = true; }", "DP2_ETF_READ_ = false; }"],
  ['wrong tile', "if (tiles[tx].label === 'Executable') tiles[tx] =", "if (tiles[tx].label === 'Held seats') tiles[tx] ="]
];

function battery() {
  const parts = [];
  test('P0 oracle: v1.2.36 rebuilt from the delivered file (SHA-256 pinned 487c0778 = origin/main 86edd72); version lockstep 1.2.37; both compile; inventory +3 functions +2 line-start vars, 0 removed; 5 hunks reverse to the base', () => {
    eq(sha256(OLD), V1236_SHA256, 'base SHA-256');
    ok(NEW.indexOf("var DP2_VERSION = '1.2.37';   // lockstep with the header") > 0 && /^ \* Version: 1\.2\.37 \(2026-11-09\)$/.test(NEW.split('\n')[3]), 'version lockstep');
    new vm.Script(NEW); new vm.Script(OLD);
    const names = (s, re) => { const o = {}; let m; while ((m = re.exec(s)) !== null) o[m[1]] = true; return o; };
    const fb = names(OLD, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), fn = names(NEW, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    deq(Object.keys(fb).filter(k => !fn[k]), [], 'functions removed');
    deq(Object.keys(fn).filter(k => !fb[k]).sort(), ['dp2ExecTileFeedCoreOk_', 'dp2ExecTileFeedOn_', 'dp2ExecTileFeedTile_'], 'functions added');
    const vb = names(OLD, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = names(NEW, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq(Object.keys(vb).filter(k => !vn[k]), [], 'vars removed');
    deq(Object.keys(vn).filter(k => !vb[k]).sort(), ['DP2_ETF_READ_', 'DP2_EXEC_TILE_FEED_PROP'], 'vars added (line starts)');
    ok(NEW.indexOf('var DP2_ETF_READ_ = false, DP2_ETF_ = false;') > 0 && OLD.indexOf('DP2_ETF_') < 0, 'DP2_ETF_ shares a line');
    eq(EDITS.length, 5, 'hunk count');
    for (const [cur] of EDITS) eq(NEW.split(cur).length - 1, 1, 'each current is unique: ' + cur.split('\n')[0].slice(0, 80));
    eq(sha256(reverseEdits(NEW)), V1236_SHA256, 'reversal of the delivered file');
  });
  test('P1 GOLDEN NEGATIVE / KILL: the gate absent, blank, off words, near misses or a property fault -> the v1.2.36 build on a live, expired, sync-running and not-actionable board (cells, formats, charts, calls, logs, mails, returns); DP2_COHORT_LEGACY too; armed, Portfolio_Decision_v2 alone is unchanged', () => checkGoldenNegative(NEW, parts));
  test('P2 armed, nothing to correct: a live release, a board with no ticket, a live release over a research board and a feed with no state build the v1.2.36 pages (with and without DP2_COHORT_LEGACY)', () => checkArmedUnchanged(NEW, parts));
  test('P3 armed, release not live (EXPIRED at 11:00, SYNC RUNNING, NOT ACTIONABLE; every on-spelling; DP2_COHORT_LEGACY): only the Executable tile changes - grey, "feed <state> - not executable now - of 10 . EXECUTABLE", the count 6 stays - against an expected page the harness builds from v1.2.36', () => checkArmedGrey(NEW, parts));
  test('P4 export audit: the EXECUTABLE label cell and a numeric value below it on every board, equal to v1.2.36 (scripts/tfb_export_audit.py check_top10_v2_presentation)', () => checkExportAudit(NEW, parts));
  test('P5 pure: the tile rule (expired, sync running, no label, live, legacy ACTIONABLE, no ticket, no feed state, no view, no mutation) and the core check', () => checkPure(NEW, parts));
  test('P6 the gate: one property read per Top-10 build (live or not); Portfolio_Decision_v2 alone never reads it; memoised; a fault reads off; off and on spellings', () => checkGate(NEW));
  test('P7 dp2SelfTest: off = the v1.2.36 report; on = one line after the optional lines before it', () => checkSelfTest(NEW, parts));
  test('P8 lint: LF; ASCII and ES5 in every hunk; the WHY newest-first; every prior WHY line kept', () => checkLint());
  test('P9 mutants: ' + MUTANTS.length + ' single-line mutants of the delivered file are each caught', () => {
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
console.log('DP2 V1237 P0-P9 x2 identical digest ' + digests[0].slice(0, 16) + ' | new SHA256 ' + sha256(NEW).slice(0, 12) + ' | v1.2.36 SHA256 ' + sha256(OLD).slice(0, 12));
console.log('PASS ' + passed + '/' + total);
