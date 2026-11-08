#!/usr/bin/env node
/* tests/test_dp2_v1235_stop_rule.js
 * Harness for apps_script/27_Decision_Pages_v2.gs v1.2.35 [STOP-RULE] (owner decision B6, Sun 8 Nov 2026: one stop rule
 * for every new ticket, -8% hard + a 45-session time stop). Script Property DP2_STOP_RULE = 'fixed' or
 * 'fixed:YYYY-MM-DD' (default start 2026-11-08; default OFF = the v1.2.34 trailing engine byte for byte): lots entered
 * on/after the start date replay a fixed stop 8% under the entry plus a time stop 45 sessions after the entry bar (an exit
 * episode that latches like a stop touch); older lots keep the trailing rule; Top_10_Investments_v2 seats show the
 * initial stop 8% under the price.
 *
 * Two trees over the REAL file, each in its own vm context: the delivered source (NEW) and the v1.2.34 file (OLD,
 * SHA-256 pinned 992fb7f7 = origin/main bdf2da2), by default rebuilt from NEW by reversing the enumerated v1.2.35 hunks
 * (tests/js/dp2_edits_v1235.json). The GAS shim, the fixture and the boards are those of
 * tests/test_dp2_v1234_render_priority.js, copied verbatim (its version normaliser also folds 1.2.35).
 *   P0 oracle, version lockstep, inventory (+6 functions, +4 vars, 0 removed), 20 hunks reverse to the base
 *   P1 GOLDEN NEGATIVE / KILL: absent, blank, off, atr, a bad date, junk, a fault -> both pages = the v1.2.34 build
 *   P2 armed (default start): the 26 Oct FRST lot keeps the trailing engine; funded Top_10_v2 seats risk 8% of allocation
 *   P3 armed from 2026-10-01: the FRST lot replays the fixed rule; the v1.2.34 page trails 2xATR
 *   P4 pure: parser, dp2StopRuleFor_, the fixed replay, the time-stop latch, the acknowledgement, the e-mail texts
 *   P5 the gate: one property read per execution; a fault reads off
 *   P6 dp2SelfTest: the v1.2.34 report while off; one 'stop rule .. core ok' line while on
 *   P7 lint: ES5; no new non-ASCII character per hunk; LF; WHY newest-first; every prior WHY kept
 *   P8 mutants
 * The battery runs twice and the two digests must match; the last line is 'PASS k/k' (exit 1 on any failure).
 * Usage: node tests/test_dp2_v1235_stop_rule.js [new.gs] [v1.2.34.gs]
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
const NEW = fs.readFileSync(NEW_PATH, 'utf8');
const FIX = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'fixtures', 'dp2_board_2026-11-02.json'), 'utf8'));
const EDITS = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'dp2_edits_v1235.json'), 'utf8'));
const V1234_SHA256 = '992fb7f77e0d4c4c59f1eb6fb42b7b45be41e1ffaa47957797222662246811c0';   // v1.2.34 (origin/main bdf2da2)
const NOW = Date.parse(FIX.frozen_now);   // 2026-11-02T06:24:00Z = 09:24 Riyadh
const GATE = 'DP2_STOP_RULE';
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
    eq(s.split(cur).length - 1, 1, 'exactly one approved v1.2.35 hunk: ' + cur.split('\n')[0].slice(0, 90));
    s = s.replace(cur, () => prev);
  }
  eq(sha256(s), V1234_SHA256, 'v1.2.35 changes only its enumerated hunks (their reversal = the v1.2.34 file)');
  return s;
}
let OLD = null, oldErr = '';
try {
  OLD = OLD_PATH ? fs.readFileSync(OLD_PATH, 'utf8') : reverseEdits(NEW);
  eq(sha256(OLD), V1234_SHA256, 'v1.2.34 SHA-256');
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
  book.load('Performance_Log', perfGrid(FIX.performance_log));
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
function norm(b) {   // the only bytes v1.2.31 under its kill switch may differ in: the version printed on both footers / the toast title
  const strip = o => { const x = Object.assign({}, o); delete x.env; return x; };
  return JSON.parse(JSON.stringify(strip(b)).split('Decision Pages v2 1.2.35').join('Decision Pages v2 1.2.30').split('Decision Pages v2 1.2.34').join('Decision Pages v2 1.2.30').split('Decision Pages v2 1.2.33').join('Decision Pages v2 1.2.30').split('Decision Pages v2 1.2.33').join('Decision Pages v2 1.2.30').split('Decision Pages v2 1.2.32').join('Decision Pages v2 1.2.30').split('Decision Pages v2 1.2.31').join('Decision Pages v2 1.2.30'));
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
const OFFS = [{}, { [GATE]: '' }, { [GATE]: 'off' }, { [GATE]: '0' }, { [GATE]: 'enable' }, { [GATE]: 'no' }];

function checkGoldenNegative(src) {
  for (const off of OFFS) {
    for (const key of ['today', 'runningStopped']) {
      const props = Object.assign({}, LIVE, off);
      const n = build(src, 'both', { board: BOARDS[key], props }), o = build(OLD, 'both', { board: BOARDS[key], props });
      deq(norm(n), norm(o), 'gate ' + JSON.stringify(off[GATE]) + ': both pages = v1.2.33 (' + key + ', a live marker and claim)');
      eq(n.env.props.DP2_LAST_BUILT, LIVE.DP2_LAST_BUILT, 'no stamp while off (' + key + ')');
    }
  }
  for (const key of ['today', 'research', 'todayNA', 'runningNow', 'runningStopped']) {
    for (const which of ['both', 't10', 'pd']) {
      deq(norm(build(src, which, { board: BOARDS[key] })), norm(build(OLD, which, { board: BOARDS[key] })), 'board ' + key + ', ' + which + ': = v1.2.33');
    }
  }
  const f1 = build(src, 'both', { board: BOARDS.runningStopped, propsThrow: true }), f0 = build(OLD, 'both', { board: BOARDS.runningStopped, propsThrow: true });
  deq(norm(f1), norm(f0), 'a property fault reads off');
}
function checkYield(src) {
  for (const [label, extra] of [['marker 2 min', { DT10_RUN_MARKER: MARKER(2) }], ['bare-timestamp marker 6 min', { DT10_RUN_MARKER: isoAgo(6) }],
                                ['claim 1 min', { TFB_DECISION_CHAIN_STATE: CLAIM(1) }], ['claim 6.5 min', { TFB_DECISION_CHAIN_STATE: CLAIM(6.5) }]]) {
    const b = build(src, 'both', { props: Object.assign({}, ON, extra) });
    eq(b.ret.ok, true, label + ': ok');
    eq(b.ret.yielded, true, label + ': yielded');
    eq(b.ret.pd, undefined, label + ': no page result');
    deq(v2Writes(b.calls), [], label + ': no v2 page written');
    ok(!b.calls.some(c => /^(Top_10_Investments_v2|Portfolio_Decision_v2|_DP2_)/.test(c)), label + ': no v2 call');
    eq(b.toasts.length, 1, label + ': one toast');
    eq(b.toasts[0][0], 'Decision Pages v2 yielded: a Top_10 / Portfolio_Decision render is running (TFB_RENDER_PRIORITY); both pages are rebuilt at the next hourly run', label + ': the toast');
    eq(b.logs.filter(l => /yielded/.test(l)).length, 1, label + ': one log line');
    eq(b.env.props.DP2_LAST_BUILT, undefined, label + ': no stamp');
  }
  for (const [label, extra] of [['marker 8 min', { DT10_RUN_MARKER: MARKER(8) }], ['marker 7 min', { DT10_RUN_MARKER: MARKER(7) }], ['future marker', { DT10_RUN_MARKER: MARKER(-3) }],
                                ['junk marker', { DT10_RUN_MARKER: 'running' }], ['claim 8 min', { TFB_DECISION_CHAIN_STATE: CLAIM(8) }],
                                ['no claim', { TFB_DECISION_CHAIN_STATE: JSON.stringify({ t10: '41901407829' }) }], ['junk state', { TFB_DECISION_CHAIN_STATE: '{' }]]) {
    const b = build(src, 'both', { props: Object.assign({}, ON, extra) });
    ok(!b.ret.yielded, label + ': not yielded');
    deq(v2Writes(b.calls), [PDV, T10], label + ': both pages built');
  }
}
function checkOrder(src) {
  const b0 = build(src, 'both', { props: ON });
  deq(v2Writes(b0.calls), [PDV, T10], 'no stamps: Portfolio_Decision_v2 first');
  eq(b0.ret.order, 'pd,t10', 'order pd,t10');
  deq(stamps(b0), { pd: NOW, t10: NOW }, 'both pages stamped');
  const at = (pd, t10) => { const o = {}; if (pd !== null) o.pd = pd; if (t10 !== null) o.t10 = t10; return Object.assign({}, ON, { DP2_LAST_BUILT: JSON.stringify(o) }); };
  const b1 = build(src, 'both', { props: at(NOW - 3600000, NOW - 3 * 3600000) });
  deq(v2Writes(b1.calls), [T10, PDV], 'the older page (Top_10_Investments_v2) first');
  eq(b1.ret.order, 't10,pd', 'order t10,pd');
  deq(v2Writes(build(src, 'both', { props: at(NOW - 3600000, null) }).calls), [T10, PDV], 'an unstamped page first');
  deq(v2Writes(build(src, 'both', { props: at(NOW - 3 * 3600000, NOW - 3600000) }).calls), [PDV, T10], 'the older page (Portfolio_Decision_v2) first');
  // the soft limit: the second page is skipped and keeps its stamp
  const env = makeEnv(src, { props: at(NOW - 3600000, NOW - 3 * 3600000) });
  env.ctx.dp2RunElapsed_ = () => 300000;
  const r = plain(env.ctx.buildDecisionPagesV2());
  deq(v2Writes(env.book.calls), [T10], 'only the first page past the soft limit');
  eq(r.pd.skipped, true, 'the second page skipped');
  ok(r.t10.rows > 0, 'the first page built');
  deq(JSON.parse(env.props.DP2_LAST_BUILT), { pd: NOW - 3600000, t10: NOW }, 'only the built page stamped');
  ok(env.book.toasts[0][0].indexOf(PDV + ' NOT rebuilt (the first page used 300 s of the 6-minute limit; it goes first at the next hourly run)') > 0, 'the toast names the skipped page: ' + env.book.toasts[0][0]);
  eq(env.book.toasts[0][2], 20, 'a long toast');
  // a failing page: stamped, the other still built, the failure named, the error rethrown
  const e2 = makeEnv(src, { props: at(NOW - 3600000, NOW - 3 * 3600000) });
  e2.ctx.buildTop10V2 = () => { throw new Error('synthetic T10 fault'); };
  let thrown = '';
  try { e2.ctx.buildDecisionPagesV2(); } catch (e) { thrown = String(e && e.message); }
  eq(thrown, 'synthetic T10 fault', 'the error rethrown');
  deq(v2Writes(e2.book.calls), [PDV], 'the other page still built');
  deq(JSON.parse(e2.props.DP2_LAST_BUILT), { pd: NOW, t10: NOW }, 'the failing page stamped too');
  ok(e2.book.toasts[0][0].indexOf(T10 + ' FAILED (synthetic T10 fault)') > 0, 'the toast names the failure: ' + e2.book.toasts[0][0]);
  return [b0.ret.order, b1.ret.order].join('/');
}
function checkPage(src) {
  const on = build(src, 'both', { props: ON }), off = build(src, 'both', {});
  ok(/^run .* cockpit /.test(headRight(off)), 'the base header text: ' + headRight(off));
  eq(headRight(on), headRight(off) + ' ' + DOT + ' page built 2026-11-02 09:24', 'the header names the build time');
  const sOn = statusLine(on), sOff = statusLine(off);
  eq(sOn.text, sOff.text, 'a finished status is unchanged');
  eq(sOn.bg, sOff.bg, 'the same background');
  const cellsOf = (b, page) => norm(b).sheets[page].cells;
  deq(cellsOf(on, PDV), cellsOf(off, PDV), 'Portfolio_Decision_v2 unchanged by the gate');
  deq(cellsOf(on, T10).filter(c => c.indexOf('1:13=') !== 0), cellsOf(off, T10).filter(c => c.indexOf('1:13=') !== 0), 'Top_10_Investments_v2: only the header cell differs');
  for (const [key, note] of [['runningNow', NOTE_NOW], ['runningStopped', NOTE_STOP]]) {
    const bOn = build(src, 't10', { board: BOARDS[key], props: ON }), bOff = build(src, 't10', { board: BOARDS[key] });
    const a = statusLine(bOn), z = statusLine(bOff);
    eq(z.text, 'Status: ' + BOARDS[key].top10.status, key + ': the base Status line');
    eq(a.text, z.text + ' ' + DASH + ' ' + note, key + ': the note');
    eq(a.bg, '#fff4e5', key + ': amber');
    eq(a.h, 30, key + ': a taller line');
  }
  return statusLine(build(src, 't10', { board: BOARDS.runningStopped, props: ON })).text;
}
function checkPure(src) {
  const c = makeEnv(src, {}).ctx;
  for (const v of ['1', 'on', 'ON', 'true', 'True', 'yes', ' Yes ', 'on\n']) eq(c.dp2RpOnCore_(v), true, 'on ' + JSON.stringify(v));
  for (const v of ['', '0', 'off', 'false', 'no', 'enable', 'onn', 'y', null, undefined]) eq(c.dp2RpOnCore_(v), false, 'off ' + JSON.stringify(v));
  const B = (m, s) => c.dp2RpBusyCore_(m, s, NOW);
  eq(B(MARKER(0), ''), true, 'a marker just set'); eq(B(MARKER(6.9), ''), true, 'a marker 6.9 min'); eq(B(MARKER(7), ''), false, 'a marker 7 min');
  eq(B(isoAgo(3), ''), true, 'a bare timestamp'); eq(B(MARKER(-1), ''), false, 'a marker in the future'); eq(B('{"v":"1"}', ''), false, 'a marker without started');
  eq(B('', CLAIM(0)), true, 'a claim just taken'); eq(B('', CLAIM(7)), false, 'a claim 7 min'); eq(B('', '{"claim":{"at_ms":"x"}}'), false, 'a claim without a number');
  eq(B(null, null), false, 'nothing'); eq(B('junk', 'junk'), false, 'junk'); eq(B(MARKER(9), CLAIM(2)), true, 'an old marker and a live claim');
  const O = raw => c.dp2RpOrderCore_(raw).join(',');
  eq(O(''), 'pd,t10', 'no stamps'); eq(O(null), 'pd,t10', 'null'); eq(O('junk'), 'pd,t10', 'junk'); eq(O('[1,2]'), 'pd,t10', 'an array');
  eq(O('{"pd":5,"t10":3}'), 't10,pd', 'the older t10'); eq(O('{"pd":3,"t10":5}'), 'pd,t10', 'the older pd'); eq(O('{"pd":5,"t10":5}'), 'pd,t10', 'a tie');
  eq(O('{"pd":5}'), 't10,pd', 'an unstamped t10'); eq(O('{"t10":5}'), 'pd,t10', 'an unstamped pd'); eq(O('{"pd":"5","t10":3}'), 'pd,t10', 'a text stamp counts as never stamped');
  const N = (st, lr) => c.dp2RpRunningNoteCore_(st, lr, NOW);
  eq(N('Last run 2026-11-02 09:20:00 | status: OK | output: EXECUTABLE', '2026-11-02 09:20:00'), '', 'a finished status');
  eq(N('', ''), '', 'no status'); eq(N('running', ''), '', 'no status field');
  eq(N(RUN_STATUS('09:20:00'), '2026-11-02 09:20:00'), NOTE_NOW, 'in progress (4 min)');
  eq(N(RUN_STATUS('09:05:00'), '2026-11-02 09:05:00'), NOTE_STOP, 'did not finish (19 min)');
  ok(/in progress/.test(N(RUN_STATUS('09:17:01'), '2026-11-02 09:17:01')), '6.98 min: in progress');
  ok(/did not finish/.test(N(RUN_STATUS('09:17:00'), '2026-11-02 09:17:00')), '7 min: did not finish');
  ok(/in progress/.test(N(RUN_STATUS('09:30:00'), '2026-11-02 09:30:00')), 'a start in the future (clock skew): in progress');
  eq(N('Last run ? | status: running', ''), 'the cockpit render did not finish (Apps Script stops an execution at 6 minutes; it wrote no receipt); ' + PREV + ' until the cockpit shows a finished run', 'no start time');
  eq(N('x | status: RUNNING', '2026-11-02 09:20'), NOTE_NOW, 'no seconds, any case');
  eq(c.dp2RpCoreOk_(), true, 'the self-test core');
}
function checkGate(src) {
  for (const sp of ['1', 'on', 'ON', 'true', 'True', 'yes', ' Yes ', 'on\n']) eq(makeEnv(src, { props: { [GATE]: sp } }).ctx.dp2RpOn_(), true, 'on: ' + JSON.stringify(sp));
  for (const sp of ['', '0', 'off', 'false', 'no', 'enable', 'onn', 'y']) eq(makeEnv(src, { props: { [GATE]: sp } }).ctx.dp2RpOn_(), false, 'off: ' + JSON.stringify(sp));
  eq(makeEnv(src, { propsThrow: true }).ctx.dp2RpOn_(), false, 'a fault reads off');
  const e = makeEnv(src, { props: ON }); e.ctx.dp2RpOn_(); e.ctx.dp2RpOn_();
  eq(e.reads[GATE], 1, 'memoised: one read per execution');
  for (const props of [ON, {}]) { const b = build(src, 'both', { props }); eq(b.env.reads[GATE], 1, 'both pages: one read (' + JSON.stringify(props) + ')'); }
}
function checkSelfTest(src) {
  const run = (s, props) => { const env = makeEnv(s, { props, fetch: NO_NET }); return plain(env.ctx.dp2SelfTest()); };
  const n0 = run(src, {}), o0 = run(OLD, {});
  deq(n0.checks.map(x => [x.name.split('1.2.34').join('v'), x.ok]), o0.checks.map(x => [x.name.split('1.2.33').join('v'), x.ok]), 'off: the v1.2.33 report');
  const n1 = run(src, ON);
  eq(n1.checks.length, o0.checks.length + 1, 'on: one more line');
  const line = n1.checks.filter(x => /^render priority/.test(x.name));
  eq(line.length, 1, 'one render priority line');
  eq(line[0].name, 'render priority (TFB_RENDER_PRIORITY): on ' + DOT + ' core ok (yields to a running decision render; the page built longest ago goes first; a stopped cockpit render is named)', 'the line');
  eq(line[0].ok, true, 'core ok');
}

/* the bars cache (Codex P2 on #920): Top_10 first fetches a held seat with the six-month range */
function wkBars(from, n, f) {
  const out = []; let d = Date.parse(from + 'T00:00:00Z'), i = 0;
  while (out.length < n) {
    const wd = new Date(d).getUTCDay();
    if (wd !== 0 && wd !== 6) { const c = Math.round(f(i) * 100) / 100; out.push({ date: new Date(d).toISOString().slice(0, 10), o: c, h: Math.round((c + 0.5) * 100) / 100, l: Math.round((c - 0.5) * 100) / 100, c }); i++; }
    d += 86400000;
  }
  return out;
}
const LONG = wkBars('2025-09-01', 300, i => 20 + 0.01 * i + 0.4 * Math.sin(i / 4));   // the holding's persisted history
const SHORT = LONG.slice(-130).map(b => Object.assign({}, b, { c: b.c + 0.05, h: b.h + 0.05, l: b.l + 0.05 }));   // a six-month fetch
const cacheGrid = series => [['Symbol', 'Key', 'Date', 'Open', 'High', 'Low', 'Close', 'Source', 'Fetched (Riyadh)']].concat(...Object.keys(series).map(sym => series[sym].map(b => [sym, sym.replace(/\.US$/, ''), b.date, b.o, b.h, b.l, b.c, 'yahoo', '2026-11-01 18:00'])));
function chartOf(seriesByKey) {
  return { fetch(url) {
    const m = /\/v8\/finance\/chart\/([^?]+)\?/.exec(String(url)); const s = m ? seriesByKey[decodeURIComponent(m[1])] : null;
    if (!s) return { getResponseCode: () => 404, getContentText: () => '{}' };
    const res = { meta: { regularMarketPrice: s[s.length - 1].c, currency: 'USD', exchangeTimezoneName: 'America/New_York', marketState: 'REGULAR' },
                  timestamp: s.map(b => Date.parse(b.date + 'T13:30:00Z') / 1000),
                  indicators: { quote: [{ open: s.map(b => b.o), high: s.map(b => b.h), low: s.map(b => b.l), close: s.map(b => b.c) }] } };
    return { getResponseCode: () => 200, getContentText: () => JSON.stringify({ chart: { result: [res], error: null } }) };
  } };
}
function checkCache(src) {
  const SYM = 'HLD.US';
  const run = props => {
    const env = makeEnv(src, { props, fetch: chartOf({ HLD: SHORT }) });
    env.book.load('_DP2_Exit_Engine', cacheGrid({ [SYM]: LONG, 'OTH.US': LONG }));
    const seat = plain(env.ctx.dp2LoadBars_([SYM]));                        // Top_10_Investments_v2 first: the default range
    const read = plain(env.ctx.dp2BarsCacheRead_()), after = read[SYM].bars, other = read['OTH.US'].bars;
    env.ctx.dp2RunElapsed_ = () => 10 * 60000;                              // the download budget is spent
    const hold = plain(env.ctx.dp2LoadBars_([SYM], LONG[5].date));          // the holding's replay asks for its entry
    return { seat: seat[SYM].bars.length, after, other, hold: hold[SYM], x: env.ctx.dp2ExitCompute_(hold[SYM].bars, LONG[5].c, LONG[5].date, null) };
  };
  const on = run(ON), off = run({});
  eq(on.seat, 130, 'the seat gets its six-month bars'); eq(off.seat, 130, 'base: the same');
  eq(off.after.length, 130, 'base: the six-month fetch truncates the cached history');
  eq(off.x, null, 'base: the holding replay has no history at its entry');
  eq(off.other.length, 130, 'base: the write cuts another holding to KEEP_BARS');
  eq(on.other.length, 300, 'gate on: another holding keeps the bars it holds');
  eq(on.after.length, 300, 'gate on: the older cached bars are kept');
  eq(on.after[0].date, LONG[0].date, 'the oldest cached bar kept');
  deq(on.after.slice(-130), SHORT, 'the fresh bars win from their first date on');
  ok(on.hold.stale === true && on.hold.bars.length === 300, 'the holding falls back to the kept history');
  ok(on.x !== null, 'gate on: the holding replay reaches its entry');
  const c = makeEnv(src, {}).ctx, K = (a, b) => plain(c.dp2RpKeepOlder_(a, b));
  const SHORT2 = LONG.slice(200).concat(wkBars('2026-10-27', 30, i => 23 + 0.01 * i)).slice(-130);   // 100 overlapping + 30 newer bars
  const k2 = K({ bars: LONG }, { bars: SHORT2, key: 'HLD', source: 'yahoo' });
  eq(k2.bars.length, 300, 'capped at the longer series');
  deq(k2.bars.slice(-130), SHORT2, 'the fresh bars kept whole');
  eq(k2.bars[0].date, LONG[30].date, 'the oldest bars beyond the cap dropped');
  eq(k2.key + '/' + k2.source, 'HLD/yahoo', 'the fresh fields kept');
  const fr = { bars: SHORT };
  ok(c.dp2RpKeepOlder_(null, fr) === fr && c.dp2RpKeepOlder_({ bars: [] }, fr) === fr && c.dp2RpKeepOlder_({ bars: SHORT }, fr) === fr, 'nothing older: the fresh series itself');
  eq(c.dp2RpKeepOlder_({ bars: LONG }, { bars: [] }).bars.length, 0, 'an empty fresh series stays empty');
  return on.after.length + '/' + off.after.length;
}

/* ------------------------------------------------------------ v1.2.35 [STOP-RULE] sections */
const FIXED = { [GATE]: 'fixed' };
const FROM_OCT = { [GATE]: 'fixed:2026-10-01' };
const FRST_BARS = wkBars('2026-06-01', 110, i => 15.2 + 0.25 * Math.sin(i / 3) + (i > 104 ? 0.4 : 0)).filter(b => b.date <= '2026-11-02');
const FRST_FETCH = chartOf({ FRST: FRST_BARS });
function allText(b, name) { const out = []; const sh = sheet(b, name); for (const [, x] of sh._cells) if (x && typeof x.v === 'string') out.push(x.v); return out.join('\n'); }
function num(s) { const m = String(s).replace(/,/g, '').match(/-?\d+(\.\d+)?/); return m ? Number(m[0]) : null; }
function syntheticBars(n, f) { const out = [], t0 = Date.UTC(2026, 0, 1); for (let q = 0; q < n; q++) { const c = f(q); out.push({ date: new Date(t0 + q * 86400000).toISOString().slice(0, 10), o: c, h: c + 1, l: c - 1, c: c }); } return out; }
const BR = stop => ({ tp: null, stop: stop, known: true, stopQty: null, shares: 10, stops: null, lotOfMany: false });

function battery() {
  const parts = [];
  test('P0 oracle: v1.2.34 rebuilt from the delivered file (SHA-256 pinned); version lockstep 1.2.35; both compile; inventory +6 functions +4 vars, 0 removed; 27 hunks reverse to the base', () => {
    eq(sha256(OLD), V1234_SHA256, 'base SHA-256');
    ok(NEW.indexOf("var DP2_VERSION = '1.2.35';   // lockstep with the header") > 0 && /^ \* Version: 1\.2\.35 \(2026-11-08\)$/.test(NEW.split('\n')[3]), 'version lockstep');
    new vm.Script(NEW); new vm.Script(OLD);
    const names = (s, re) => { const o = {}; let m; while ((m = re.exec(s)) !== null) o[m[1]] = true; return o; };
    const fb = names(OLD, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), fn = names(NEW, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    deq(Object.keys(fb).filter(k => !fn[k]), [], 'functions removed');
    deq(Object.keys(fn).filter(k => !fb[k]).sort(), ['dp2ExitFixed_', 'dp2StopRuleCoreOk_', 'dp2StopRuleFor_', 'dp2StopRuleParse_', 'dp2StopRule_', 'dp2StopTimeTxt_'], 'functions added');
    const vb = names(OLD, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = names(NEW, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq(Object.keys(vb).filter(k => !vn[k]), [], 'vars removed');
    deq(Object.keys(vn).filter(k => !vb[k]).sort(), ['DP2X_SR_FROM_', 'DP2X_SR_READ_', 'DP2_STOP_RULE_PROP'], 'vars added (line starts)');
    ok(NEW.indexOf('var DP2X_SR_READ_ = false, DP2X_SR_ = null;') > 0 && OLD.indexOf('DP2X_SR_') < 0, 'the fourth var DP2X_SR_ shares a line');
    eq(EDITS.length, 27, 'hunk count');
    for (const [cur] of EDITS) eq(NEW.split(cur).length - 1, 1, 'each current is unique: ' + cur.split('\n')[0].slice(0, 80));
    eq(sha256(reverseEdits(NEW)), V1234_SHA256, 'reversal of the delivered file');
  });

  test('P1 GOLDEN NEGATIVE / KILL: the gate absent, blank, off, atr, a bad date, junk or a property fault -> both pages equal the v1.2.34 build (cells, formats, charts, calls, logs, mails, returns)', () => {
    for (const raw of [undefined, '', 'off', 'atr', 'fixed:2026-1-1', 'trailing', 'fixedx', 'fixed 2026-13']) {
      const props = raw === undefined ? {} : { [GATE]: raw };
      for (const key of ['today', 'research']) {
        const n = build(NEW, 'both', { board: BOARDS[key], props, fetch: NO_NET }), o = build(OLD, 'both', { board: BOARDS[key], props, fetch: NO_NET });
        deq(norm(n), norm(o), 'gate ' + JSON.stringify(raw) + ', board ' + key);
        if (key === 'today' && raw === undefined) parts.push(norm(n).ret);
      }
    }
    const nf = build(NEW, 'both', { propsThrow: true, fetch: NO_NET }), of = build(OLD, 'both', { propsThrow: true, fetch: NO_NET });
    deq(norm(nf), norm(of), 'a property fault reads off');
  });

  test('P2 armed with the default start (2026-11-08): the FRST lot of 26 Oct keeps the trailing engine (Portfolio_Decision_v2 = v1.2.34); every funded Top_10_v2 seat risks 8% of its allocation', () => {
    const n = build(NEW, 'pd', { props: FIXED, fetch: FRST_FETCH }), o = build(OLD, 'pd', { props: FIXED, fetch: FRST_FETCH });
    const NOTE = ' DP2_STOP_RULE: lots entered from 2026-11-08 use a fixed stop 8% under the entry and a time stop after 45 sessions.';
    const FOOT = 'Lots entered from 2026-11-08: a fixed stop 8% under the entry and a time stop after 45 sessions. Older lots: ';
    const nj = JSON.stringify(norm(n));
    ok(nj.indexOf(NOTE) > 0, 'the holdings note names the rule while armed');
    const DSTOP = ' Lots entered from 2026-11-08 follow DP2_STOP_RULE instead: a fixed stop 8% under the entry and a time stop after 45 sessions; keep their stop fixed (no trailing order).';
    ok(nj.indexOf(DSTOP) > 0, 'the D-stop note names the rule while armed');
    deq(JSON.parse(nj.split(NOTE).join('').split(FOOT).join('').split(DSTOP).join('')), norm(o), 'Portfolio_Decision_v2 otherwise unchanged for a lot before the start date');
    const tn = t10View(build(NEW, 't10', { props: FIXED, fetch: NO_NET })), to = t10View(build(OLD, 't10', { props: {}, fetch: NO_NET }));
    let funded = 0, moved = 0;
    tn.seats.forEach((s, i) => {
      const alloc = num(s.alloc), risk = num(s.risk);
      if (alloc && alloc > 0 && risk !== null) { funded++; near(risk / alloc, 0.08, 0.002, s.sym + ' risk/alloc'); if (String(s.risk) !== String(to.seats[i].risk)) moved++; }
    });
    ok(funded >= 2, 'funded seats ' + funded); ok(moved >= 1, 'risk cells moved from the 2xATR stop: ' + moved);
    parts.push(tn.seats.map(s => [s.sym, s.risk]));
  });

  test('P3 armed from 2026-10-01: the FRST lot replays the fixed rule (stop 8% under 15.20 = 13.98, no trailing, time stop in N sessions); the v1.2.34 page trails 2xATR', () => {
    const n = build(NEW, 'pd', { props: FROM_OCT, fetch: FRST_FETCH }), o = build(OLD, 'pd', { props: FROM_OCT, fetch: FRST_FETCH });
    const tn = allText(n, PDV), to = allText(o, PDV);
    ok(tn.indexOf('fixed stop -8%, time stop in ') >= 0, 'engine text names the fixed rule');
    ok(tn.indexOf('13.98') >= 0, 'the fixed stop 13.98');
    ok(to.indexOf('fixed stop -8%') < 0 && to.indexOf('trail 2×ATR') >= 0, 'GOLDEN NEGATIVE: v1.2.34 trails 2xATR');
    ok(tn.indexOf('trail 2×ATR') < 0, 'no trailing text for the fixed lot');
    ok(tn.indexOf('null × ATR') < 0 && tn.indexOf('null') < 0, 'no null in any page text (review fix: the page action never offers a trailing order)');
    const frstActs = pdView(n).actions.filter(a => a.indexOf('FRST.US') === 0);
    ok(frstActs.every(a => a.indexOf('trailing') < 0 && a.indexOf('null') < 0), 'no trailing advice in the FRST actions: ' + JSON.stringify(frstActs));
    ok(tn.indexOf('let the trail') < 0 && tn.indexOf('HOLD · trailing') < 0, 'no trailing hold or TP advice for the fixed lot');
    ok(tn.indexOf('Lots entered from 2026-10-01 follow DP2_STOP_RULE instead') >= 0, 'the D-stop note names the rule');
    ok(tn.indexOf('DP2_STOP_RULE: lots entered from 2026-10-01 use a fixed stop 8% under the entry') >= 0, 'the holdings note names the rule');
    eq(n.env.reads[GATE], 1, 'one property read per execution');
    parts.push(tn.length);
  });

  test('P4 pure: parser spellings; dp2StopRuleFor_; the fixed replay (stop, touch, time stop, sessions left); the signal latches the time stop; the e-mail texts name the fixed rule', () => {
    const e = makeEnv(NEW, { props: FIXED, fetch: NO_NET }).ctx;
    const P = e.dp2StopRuleParse_;
    eq(P('fixed').from, '2026-11-08'); eq(P(' FIXED ').from, '2026-11-08'); eq(P('fixed:2026-12-01').from, '2026-12-01');
    eq(P('fixed=2026-12-01').from, '2026-12-01'); eq(P('Fixed 2026-12-01').from, '2026-12-01');
    for (const bad of ['', 'atr', 'on', '1', 'fixed:2026-1-1', 'fixed:', 'fixed2026-12-01x', null, undefined]) eq(P(bad), null, 'off: ' + JSON.stringify(bad));
    eq(e.dp2StopRuleFor_('2026-11-08'), 'fixed'); eq(e.dp2StopRuleFor_('2026-11-07'), undefined); eq(e.dp2StopRuleFor_(''), undefined);
    const bars = syntheticBars(70, q => 100 + (q % 5));
    const x = e.dp2ExitCompute_(bars, 100, bars[20].date, null, 'fixed');
    near(x.stop, 92, 1e-9, 'stop'); eq(x.rule, 'fixed'); eq(x.trailMult, null); eq(x.sma, null); eq(x.beLocked, false); eq(x.momentum, false);
    eq(x.times.length, 1); eq(x.times[0].date, bars[65].date); eq(x.sessionsHeld, 49); eq(x.sessionsLeft, 0);
    const s = e.dp2ExitSignal_(x, BR(92), null, '');
    eq(s.code, 'EXIT_LATCHED'); eq(s.latched.kind, 'time'); ok(s.text.indexOf('time stop, 45 sessions held, close 100.00') > 0, s.text);
    const ack = e.dp2ExitSignal_(x, BR(92), null, bars[65].date);
    ok(ack.latchedAck === true && ack.code !== 'EXIT_LATCHED', 'an acknowledged time stop holds by decision');
    const y = e.dp2ExitCompute_(bars, 100, bars[60].date, null, 'fixed');
    const hy = e.dp2ExitSignal_(y, BR(92), null, '');
    eq(hy.code, 'HOLD'); eq(hy.text, 'HOLD - fixed stop 92.00, time stop in 36 sessions');
    eq(e.dp2ExitSignal_(y, BR(85), null, '').code, 'RAISE_STOP', 'a lower broker stop is raised to the rule');
    eq(e.dp2ExitSignal_(y, BR(95), null, '').code, 'HOLD', 'a tighter broker stop is left alone');
    const crash = bars.slice(0, 69).concat([{ date: bars[69].date, o: 90, h: 91, l: 89, c: 90 }]);
    const z = e.dp2ExitCompute_(crash, 100, bars[60].date, null, 'fixed');
    eq(z.hits.length, 1); eq(e.dp2ExitSignal_(z, BR(92), null, '').code, 'STOP_HIT');
    const line = e.dp2ExitLine_({ x: z, sig: e.dp2ExitSignal_(z, BR(92), null, ''), qty: 10, entry: 100, fx: 3.75, brokerStop: 92, symbol: 'T.US', name: 'T' }, 3.75);
    const lineTxt = JSON.stringify(line);
    ok(lineTxt.indexOf('is at or below the fixed stop 92.00 (8% under the entry 100.00)') >= 0, 'STOP_HIT why: ' + lineTxt.slice(0, 300));
    const ly = JSON.stringify(e.dp2ExitLine_({ x: y, sig: e.dp2ExitSignal_(y, BR(85), null, ''), qty: 10, entry: 100, fx: 3.75, brokerStop: 85, symbol: 'T.US', name: 'T' }, 3.75));
    ok(ly.indexOf('the fixed 8% rule puts the stop at 92.00') >= 0 && ly.indexOf('(a fixed stop: no trailing order).') >= 0 && ly.indexOf('trailing stop order') < 0, 'RAISE_STOP texts: ' + ly.slice(0, 300));
    const a = e.dp2ExitCompute_(bars, 100, bars[20].date, null);
    eq(a.rule, undefined); eq(a.trailMult, 2);
    const ob = makeEnv(OLD, { props: FIXED, fetch: NO_NET }).ctx;
    deq(plain(ob.dp2ExitCompute_(bars, 100, bars[20].date, null)), plain(a), 'the trailing replay is the v1.2.34 replay');
    parts.push(plain(x), s.text, hy.text);
  });

  test('P5 the gate: one property read per execution on both pages; a fault reads off', () => {
    for (const props of [FIXED, {}]) { const b = build(NEW, 'both', { props, fetch: NO_NET }); eq(b.env.reads[GATE], 1, 'one read (' + JSON.stringify(props) + ')'); }
    eq(makeEnv(NEW, { propsThrow: true }).ctx.dp2StopRule_(), null, 'a fault reads off');
  });

  test('P6 dp2SelfTest: the v1.2.34 report while off; one stop rule line (core ok) while on', () => {
    const run = (s, props) => { const env = makeEnv(s, { props, fetch: NO_NET }); return plain(env.ctx.dp2SelfTest()); };
    const n0 = run(NEW, {}), o0 = run(OLD, {});
    deq(n0.checks.map(x => [x.name.split('1.2.35').join('v'), x.ok]), o0.checks.map(x => [x.name.split('1.2.34').join('v'), x.ok]), 'off: the v1.2.34 report');
    const n1 = run(NEW, FIXED);
    eq(n1.checks.length, o0.checks.length + 1, 'on: one more line');
    const line = n1.checks.find(c => c.name.indexOf('stop rule (DP2_STOP_RULE)') === 0);
    ok(line && line.ok === true && line.name === 'stop rule (DP2_STOP_RULE): fixed from 2026-11-08 ' + DOT + ' core ok (8% under the entry, time stop after 45 sessions)', 'line: ' + (line && line.name));
  });

  test('P7 lint: ES5 added lines; no new non-ASCII character in any hunk; LF only; WHY paragraph newest-first; every prior WHY kept', () => {
    const bl = new Set(OLD.split('\n'));
    const added = NEW.split('\n').filter(l => !bl.has(l));
    ok(added.length >= 80 && added.length <= 140, 'added lines ' + added.length);
    const code = l => l.replace(/'(?:[^'\\]|\\.)*'/g, "''").replace(/"(?:[^"\\]|\\.)*"/g, '""');   // string literals out
    for (const l of added) ok(!/\b(let|const|class)\b|=>|`/.test(code(l)), 'ES5 violation: ' + l);
    const nonAscii = s => (s.match(/[^\x00-\x7f]/g) || []).sort().join('');
    for (const [cur, prev] of EDITS) {
      const extra = nonAscii(cur).split(''), have = nonAscii(prev).split('');
      for (const ch of have) { const i = extra.indexOf(ch); if (i >= 0) extra.splice(i, 1); }
      eq(extra.join(''), '', 'new non-ASCII in hunk: ' + cur.split('\n')[0].slice(0, 80));
    }
    ok(NEW.indexOf('\r') < 0, 'LF only');
    const w35 = NEW.indexOf(' *   1.2.35: ONE STOP RULE FOR EVERY NEW TICKET'), w34 = NEW.indexOf(' *   1.2.34: THE SECOND PAGE IS NEVER STARVED');
    ok(w35 > 0 && w34 > w35, 'WHY newest-first');
    const whyOld = OLD.split('\n').filter(l => /^ \*   1\.\d+\.\d+:/.test(l));
    for (const l of whyOld) ok(NEW.indexOf(l) >= 0, 'prior WHY kept: ' + l.slice(0, 60));
  });

  test('P8 mutants: four one-line mutations are each caught', () => {
    const mutants = [
      ['trailing kept under the rule', "  if (rule === 'fixed') return dp2ExitFixed_(bars, entry, e, a0, atr, atrApprox, lastOverride);   // v1.2.35 [STOP-RULE]\n", ''],
      ['no time stop', "    if (i - e === DP2X.TIME_STOP_SESSIONS) times.push({ date: bars[i].date, price: bars[i].c });\n", ''],
      ['older lots switched too', "  return (r && d && d >= r.from) ? 'fixed' : undefined;", "  return r ? 'fixed' : undefined;"],
      ['time episodes not latched', "  for (ei = 0; ei < timesArr.length; ei++) episodes.push({ date: timesArr[ei].date, kind: 'time', price: timesArr[ei].price });\n", '']
    ];
    for (const [label, from, to] of mutants) {
      eq(NEW.split(from).length - 1, 1, 'mutant anchor unique: ' + label);
      const M = NEW.replace(from, to);
      let caught = false;
      try {
        const e = makeEnv(M, { props: FIXED, fetch: NO_NET }).ctx;
        const bars = syntheticBars(70, q => 100 + (q % 5));
        const x = e.dp2ExitCompute_(bars, 100, bars[20].date, null, 'fixed');
        const ok1 = x.rule === 'fixed' && x.times.length === 1;
        const ok2 = e.dp2StopRuleFor_('2026-11-07') === undefined;
        const ok3 = e.dp2ExitSignal_(x, BR(92), null, '').code === 'EXIT_LATCHED';
        caught = !(ok1 && ok2 && ok3 && e.dp2StopRuleCoreOk_());
      } catch (eM) { caught = true; }
      ok(caught, 'mutant not caught: ' + label);
    }
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
console.log('DP2 V1235 P0-P8 x2 identical digest ' + digests[0].slice(0, 16) + ' | new SHA256 ' + sha256(NEW).slice(0, 12) + ' | v1.2.34 SHA256 ' + sha256(OLD).slice(0, 12));
console.log('PASS ' + passed + '/' + total);
