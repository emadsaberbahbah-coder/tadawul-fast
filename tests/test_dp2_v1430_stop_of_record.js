#!/usr/bin/env node
/* tests/test_dp2_v1430_stop_of_record.js
 * Harness for apps_script/27_Decision_Pages_v2.gs v1.4.3 [STOP-OF-RECORD] (owner, Mon 16 Nov 2026 14:19 Riyadh: check
 * that the pages give one consistent recommendation). On Portfolio_Decision_v2 the Exit signal cell read FINAL: HOLD
 * while the 'Dyn stop' cell beside it showed this page's own stop with a 'raise' arrow. With a stop on the symbol's
 * Final_Decisions row the cell now shows the stop of record first, the broker stop against it, and the page's own stop
 * as analysis. Shims copied from tests/test_dp2_v1420_status_line.js. The v1.4.2 file (OLD, SHA-256 pinned fc90004f =
 * origin/main 2b66b51) is rebuilt by reversing the enumerated v1.4.3 hunks (tests/js/dp2_edits_v1430.json).
 * Every Final_Decisions row is synthetic.
 *   R0  oracle: reversal = v1.4.2; versions; both compile; +2 functions, 0 removed; ES5 / ASCII of the new lines
 *   R1  GOLDEN NEGATIVE: DP2_FINAL off, or final rows without a stop -> both pages equal the v1.4.2 build
 *   R2  a final row with a stop: only the holding's 'Dyn stop' cell changes; it starts with the stop of record and
 *       keeps the page's own stop as 'v2 rule ... (analysis)'; no raise arrow unless the broker stop is below it
 *   R3  ACTIONS TODAY: a final HOLD ending 'nothing to change.' is not listed; a real raise is
 *   R4  the pure helpers (a stop on part of the position is never 'in place'; a verify-only row never instructs)
 *   R5  mutants
 * The battery runs three times and the digests must match; the last line is 'PASS k/k' (exit 1 on any failure).
 */
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const vm = require('vm');
const ROOT = path.join(__dirname, '..');
const NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '27_Decision_Pages_v2.gs');
// 2026-11-17: v1.4.4 delivered -- this harness runs on the v1.4.3 file rebuilt from it (v1430Source).
const NEW = require(path.join(ROOT, 'tests', 'js', 'dp2_history.js')).v1430Source(fs.readFileSync(NEW_PATH, 'utf8'));
const FIX = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'fixtures', 'dp2_board_2026-11-02.json'), 'utf8'));
const EDITS = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'dp2_edits_v1430.json'), 'utf8'));
const HISTORY = require(path.join(ROOT, 'tests', 'js', 'dp2_history.js'));
const V1420_SHA256 = 'fc90004f4febcbe77b414e53f49e08cf09099ac77104015db57ab97e5ad67df1';   // v1.4.2 (origin/main 2b66b51)
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
      if (c + nc - 1 > maxCols || r + nr - 1 > maxRows) throw new Error('beyond the sheet: ' + [r, c, nr, nc] + ' > ' + [maxRows, maxCols]);   // Codex P1, #1002: Sheets enforces the bounds
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
      setNumberFormat: setAll('nf', 'setNumberFormat'), setWrap: setAll('wrap', 'setWrap'), setNote: setAll('note', 'setNote'),   // 1.4.2 harness
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
  for (const v of ['1.4.3', '1.4.2', '1.4.1', '1.4.0', '1.3.1', '1.3.0', '1.2.36', '1.2.35', '1.2.34', '1.2.33', '1.2.32', '1.2.31']) s = s.split('Decision Pages v2 ' + v).join('Decision Pages v2 1.2.30');
  return JSON.parse(s);
}
function sheet(b, name) { return b.env.book.getSheetByName(name); }
function val(sh, r, c) { const x = sh._cells.get(r + ':' + c); return x && x.v !== undefined && x.v !== null ? x.v : ''; }
function fmt(sh, r, c) { return sh._cells.get(r + ':' + c) || {}; }
function rowsWhere(sh, c, pred) { const out = [], last = sh.getLastRow(); for (let r = 1; r <= last; r++) if (pred(String(val(sh, r, c)), r)) out.push(r); return out; }

/* ------------------------------------------------------------ v1.4.0 */
function reverseEdits(src) {
  let s = src;
  for (const [cur, prev] of EDITS) {
    eq(s.split(cur).length - 1, 1, 'exactly one approved v1.4.3 hunk: ' + cur.split('\n')[0].slice(0, 90));
    s = s.replace(cur, () => prev);
  }
  return s;
}
let OLD = null;
try { OLD = reverseEdits(NEW); } catch (e) { OLD = null; }
const T10 = 'Top_10_Investments_v2', PDV = 'Portfolio_Decision_v2';
const NO_NET = { fetch() { return { getResponseCode: () => 404, getContentText: () => '{}' }; } };
const FHEAD = ['Symbol', 'Decision', 'Do today', 'Qty', 'Limit', 'Stop', 'Cost SAR', 'Why', 'Rule of record', "Other pages' own rules",
               'Analysis differs on', 'Decided At (Riyadh)', 'Version'];
function finalGrid(rows, at) {
  return [FHEAD].concat(rows.map(r => [r[0], r[1], r[2], r[3] === undefined ? '' : r[3], r[4] || '', r[5] === undefined ? '' : r[5],
    r[6] === undefined ? '' : r[6], 'synthetic', r[1] === 'BUY' || r[1] === 'WATCH' ? 'Daily Plan' : 'Exit Plan', '', 'none', at || '2026-11-02 09:00', '1.6.0']));
}
const FINAL_ROWS = [
  ['FRST.US', 'HOLD', 'Hold; stop 14.00.', 100, '', 14],
  ['5023.SR', 'HOLD', 'Hold; the Exit Plan has no rule for this holding.'],
  ['ITRN.US', 'BUY', 'Buy 7 shares with a limit between 10.00 and 10.50; place the stop at 9.50.', 7, '10.00 - 10.50', 9.5, 276],
  ['DDI.US', 'WATCH', 'Do not buy from the Top 10 pages: it is not a pick of today\'s plan.'],
  ['CRC.US', 'DO NOT INVEST', 'Do not buy: the plan invalidated it today (earnings in 2 day(s)).']
];
function withFinal(rows, at) { const b = makeBook(); return b; }
function frstYahoo() {   // a live FRST.US history: about 15.20 until the entry day (26 Oct), then 13.00 - a fresh stop hit
  const ts = [], o = [], h = [], l = [], c = [];
  for (let d = Date.UTC(2026, 8, 1, 14, 30); d <= Date.UTC(2026, 9, 30, 14, 30); d += 86400000) {
    const wd = new Date(d).getUTCDay(); if (wd === 0 || wd === 6) continue;
    const after = d > Date.UTC(2026, 9, 26, 23, 0), base = after ? 13.0 : 15.2 + ((ts.length % 3) - 1) * 0.05;
    ts.push(d / 1000); o.push(base); h.push(base + 0.15); l.push(base - 0.15); c.push(base);
  }
  const body = JSON.stringify({ chart: { result: [{ meta: { regularMarketPrice: 13.0, currency: 'USD', exchangeTimezoneName: 'America/New_York' }, timestamp: ts, indicators: { quote: [{ open: o, high: h, low: l, close: c }] } }] } });
  return { fetch(url) { return /chart\/FRST\b/.test(url) ? { getResponseCode: () => 200, getContentText: () => body } : { getResponseCode: () => 404, getContentText: () => '{}' }; } };
}
function buildWith(source, which, opts) {
  opts = Object.assign({ fetch: NO_NET }, opts || {});
  const env = makeEnv(source, opts);
  if (opts.final) env.book.load('Final_Decisions', opts.final);
  if (opts.bars) env.book.load('_DP2_Exit_Engine', barsGrid(Object.assign({}, FIX.bars, opts.bars)));
  const fn = which === 'pd' ? 'buildPortfolioDecisionV2' : (which === 't10' ? 'buildTop10V2' : 'buildDecisionPagesV2');
  const ret = env.ctx[fn]();
  const sheets = {};
  for (const [n, s] of env.book._sheets) sheets[n] = s._state();
  return { env, ret: plain(ret), sheets, calls: env.book.calls.slice(), toasts: env.book.toasts.slice(), logs: env.logs.slice(), mails: env.mails.slice() };
}
function cellsWith(sh, re) { const out = []; for (const [k, x] of sh._cells) if (x && typeof x.v === 'string' && re.test(x.v)) out.push([k, x.v]); return out; }
function actionLines(sh) { return cellsWith(sh, /^\d\. /).map(x => x[1]); }


function stopped(top) {   // what a render stopped after clearing the board leaves: title, status, panel, KPIs
  const g = top10Grid(top).slice(0, 16);
  g[1] = ['Status', 'Last run 2026-11-02 09:20:11 | status: running\u2026'];
  return g;
}
const seatRows = sh => cellsWith(sh, /^(ITRN|DDI|CRC|GLPI|EFC|INDV|OLP)\.US \u00b7 /).length;
const cellAt = (sh, r, c) => val(sh, r, c);
function chain(first, second) {   // two builds over one workbook
  const e1 = makeEnv(NEW, Object.assign({ fetch: NO_NET }, first || {}));
  if (first && first.final) e1.book.load('Final_Decisions', first.final);
  e1.ctx.buildTop10V2();
  if (second && second.top) e1.book.load('Top_10_Investments', second.top);
  const e2 = makeEnv(NEW, Object.assign({ fetch: NO_NET, book: e1.book }, second || {}));
  const ret = e2.ctx.buildTop10V2();
  return { e1, e2, ret, sh: e2.book.getSheetByName(T10) };
}

/* ============================================================ v1.4.3 battery */
const CELL = '28:12';   // the FRST.US 'Dyn stop' cell of the fixture's holdings table
const cellOf = b => { const x = sheet(b, PDV)._cells.get(CELL); return x ? String(x.v) : ''; };
const stripCell = st => { const x = clone(st); x.cells = x.cells.filter(c => c.split('=')[0] !== CELL); return x; };
const rowsWith = (todo, stop) => FINAL_ROWS.map(r => r[0] === 'FRST.US' ? ['FRST.US', 'HOLD', todo, 100, '', stop] : r);
function checkOracle() {
  ok(OLD !== null, 'the hunks reverse'); eq(sha256(OLD), V1420_SHA256, 'v1.4.2 SHA-256');
  eq(HISTORY.V1420_SHA256, V1420_SHA256, 'history pin'); eq(sha256(HISTORY.v1420Source(NEW)), V1420_SHA256, 'history chain');
  eq(EDITS.length, 5, 'five hunks');
  ok(/ \* Version: 1\.4\.3 \(2026-11-16\)/.test(NEW) && /var DP2_VERSION = '1\.4\.3';/.test(NEW), 'version lockstep');
  new vm.Script(NEW, { filename: '27_Decision_Pages_v2.gs' }); new vm.Script(OLD, { filename: 'old.gs' });
  const fns = s => new Set((s.match(/^function\s+([A-Za-z_$][\w$]*)/mg) || []).map(x => x.split(/\s+/)[1]));
  const fo = fns(OLD), fn = fns(NEW);
  deq([...fo].filter(k => !fn.has(k)), [], 'functions removed'); deq([...fn].filter(k => !fo.has(k)).sort(), ['dp2FinalVerifyOnly_', 'dp2StopOfRecordText_'], 'functions added');
  for (const [cur, prev] of EDITS) {
    const na = s => s.split('').filter(ch => ch.charCodeAt(0) > 127).sort().join('');
    eq(na(cur), na(prev), 'no new non-ASCII character: ' + cur.split('\n')[0].slice(0, 70));
    const added = cur.split('\n').filter(l => prev.split('\n').indexOf(l) < 0);
    const code = added.filter(l => !/^\s*(\*|\/\*|\/\/)/.test(l)).map(l => l.replace(/\/\/.*$/, '')).join('\n');
    ok(!/=>|`|\blet\s|\bconst\s|\bclass\s/.test(code), 'ES5: ' + code.slice(0, 90));
  }
}
function checkGolden() {
  for (const which of ['pd', 't10']) {
    for (const bname of ['today', 'research']) {
      const fin = finalGrid(FINAL_ROWS);
      deq(norm(buildWith(NEW, which, { board: BOARDS[bname], props: { DP2_FINAL: 'off' }, final: fin })),
          norm(buildWith(OLD, which, { board: BOARDS[bname], props: { DP2_FINAL: 'off' }, final: fin })), 'DP2_FINAL off ' + which + ' ' + bname);
      const noStop = finalGrid(rowsWith('Hold.', ''));
      deq(norm(buildWith(NEW, which, { board: BOARDS[bname], final: noStop })), norm(buildWith(OLD, which, { board: BOARDS[bname], final: noStop })), 'no stop on the final row ' + which + ' ' + bname);
      deq(norm(buildWith(NEW, which, { board: BOARDS[bname] })), norm(buildWith(OLD, which, { board: BOARDS[bname] })), 'no Final_Decisions tab ' + which + ' ' + bname);
    }
  }
  const t = finalGrid(FINAL_ROWS);
  deq(norm(buildWith(NEW, 't10', { final: t })), norm(buildWith(OLD, 't10', { final: t })), 'the Top 10 v2 page is the 1.4.2 page');
}
function checkCell(out) {
  for (const [opts, tag] of [[{}, 'no engine'], [{ fetch: frstYahoo() }, 'engine live']]) {
    const fin = finalGrid(FINAL_ROWS);
    const n = buildWith(NEW, 'pd', Object.assign({ final: fin }, opts)), o = buildWith(OLD, 'pd', Object.assign({ final: fin }, opts));
    const cn = cellOf(n), co = cellOf(o);
    ok(/^14\.00 \([^)]*\) stop of record\nbroker 14\.56 — in place/.test(cn), tag + ': ' + JSON.stringify(cn));
    ok(cn.indexOf('↑') < 0, tag + ': no raise arrow when the broker stop is at or above the stop of record');
    const own = /^(\d+\.\d\d) \(/.exec(co);
    if (own) ok(cn.endsWith('\nv2 rule ' + own[1] + ' (analysis)'), tag + ': the page\'s own stop kept as analysis: ' + JSON.stringify(cn) + ' / ' + JSON.stringify(co));
    else ok(cn.indexOf('v2 rule') < 0, tag + ': no own stop, no v2 line');
    deq(norm({ x: stripCell(n.sheets[PDV]) }), norm({ x: stripCell(o.sheets[PDV]) }), tag + ': every other cell of the page');
    for (const name of Object.keys(o.sheets)) if (name !== PDV) deq(norm({ x: n.sheets[name] }), norm({ x: o.sheets[name] }), tag + ': ' + name);
    deq(n.mails, o.mails, tag + ': mails');
    if (out) out.push(cn);
  }
  const hi = buildWith(NEW, 'pd', { final: finalGrid(rowsWith('Hold; stop 14.90.', 14.9)) });
  ok(/^14\.90 \([^)]*\) stop of record\nbroker 14\.56 ↑ raise to 14\.90/.test(cellOf(hi)), 'broker below the stop of record: ' + JSON.stringify(cellOf(hi)));
  const nc = buildWith(NEW, 'pd', { final: finalGrid(FINAL_ROWS), capture: null });
  ok(/stop of record\n/.test(cellOf(nc)), 'without a capture the stop of record still leads: ' + JSON.stringify(cellOf(nc)));
  if (out) out.push(cellOf(hi), cellOf(nc));
  // Codex P2 x2 (#1007): a stop on part of the position is never 'in place'; a verify-only row gets no raise / place
  const part = clone(FIX.ibkr_capture);
  for (const o of part.json.orders.orders) if (/STP/.test(o.order_type)) { o.total_shares_qty = 40; o.remaining_shares_qty = 40; o.primary_description = 'SELL 40 FRST STP 14.56'; }
  const pc = cellOf(buildWith(NEW, 'pd', { final: finalGrid(FINAL_ROWS), capture: part }));
  ok(/\nbroker 14\.56 on 40 of 174 sh only/.test(pc) && pc.indexOf('in place') < 0, 'partial cover: ' + JSON.stringify(pc));
  const vr = cellOf(buildWith(NEW, 'pd', { final: finalGrid(rowsWith('Hold. Verify at IBKR before any change: the quantity differs.', 14.9)) }));
  ok(/\nbroker 14\.56 below the stop of record \u2014 verify first/.test(vr) && vr.indexOf('raise') < 0 && vr.indexOf('place') < 0, 'verify-only: ' + JSON.stringify(vr));
  const vs = cellOf(buildWith(NEW, 'pd', { final: finalGrid(rowsWith('Verify FRST.US at IBKR before acting: the quantity differs.', 14.9)) }));
  eq(vs, vr, 'the step form of a verification is verify-only too');
  if (out) out.push(pc, vr);
}
function checkActions() {
  const quiet = "Hold; the stop working at IBKR (14.56) already covers the plan's 14.00 on all 100 shares - nothing to change.";
  const a = buildWith(NEW, 'pd', { final: finalGrid(rowsWith(quiet, 14)) }), b = buildWith(NEW, 'pd', { final: finalGrid(rowsWith('Hold; raise the stop to 14.90 for all 100 shares.', 14.9)) });
  eq(actionLines(sheet(a, PDV)).filter(l => /FRST\.US \(Today Action Plan\)/.test(l)).length, 0, 'nothing to change: not an action');
  eq(actionLines(sheet(b, PDV)).filter(l => /HOLD FRST\.US \(Today Action Plan\): Hold; raise the stop to 14\.90/.test(l)).length, 1, 'a real raise is an action');
  const o = buildWith(OLD, 'pd', { final: finalGrid(rowsWith(quiet, 14)) });
  eq(actionLines(sheet(o, PDV)).filter(l => /FRST\.US \(Today Action Plan\)/.test(l)).length, 1, 'GOLDEN NEGATIVE: 1.4.2 listed it');
}
function checkPure() {   // invented figures
  const f = makeEnv(NEW, { fetch: NO_NET }).ctx.dp2StopOfRecordText_;
  eq(f(9.5, 10, 9.5, true, null, 9.8), '9.50 (\u22125.0%) stop of record\nbroker 9.50 \u2014 in place\nv2 rule 9.80 (analysis)');
  eq(f(9.5, 10, 9.6, true, null, 9.8), '9.50 (\u22125.0%) stop of record\nbroker 9.60 \u2014 in place\nv2 rule 9.80 (analysis)');
  eq(f(9.5, 10, 9.2, true, null, null), '9.50 (\u22125.0%) stop of record\nbroker 9.20 \u2191 raise to 9.50');
  eq(f(9.5, 10, 9.496, true, null, null).split('\n')[1], 'broker 9.50 \u2014 in place', 'half a cent is in place');
  eq(f(9.5, 10, null, true, null, null), '9.50 (\u22125.0%) stop of record\nno broker stop \u2014 place 9.50');
  eq(f(9.5, 10, null, false, 9.0, null), '9.50 (\u22125.0%) stop of record\nbroker orders not read');
  eq(f(9.5, 10, null, true, 9.0, null), '9.50 (\u22125.0%) stop of record\nbroker 9.00 not working \u2014 place 9.50');
  eq(f(9.5, null, 9.6, true, null, null), '9.50 stop of record\nbroker 9.60 \u2014 in place');
  const L2 = (...a) => f(...a).split('\n')[1];
  eq(L2(9.5, 10, 9.6, true, null, null, { covered: 40, shares: 100 }), 'broker 9.60 on 40 of 100 sh only');
  eq(L2(9.5, 10, 9.2, true, null, null, { covered: 40, shares: 100 }), 'broker 9.20 on 40 of 100 sh only, below the stop of record');
  eq(L2(9.5, 10, 9.2, true, null, null, { covered: 40, shares: 100, verifyOnly: true }), 'broker 9.20 on 40 of 100 sh only, below the stop of record \u2014 verify first');
  eq(L2(9.5, 10, 9.6, true, null, null, { covered: 100, shares: 100 }), 'broker 9.60 \u2014 in place');
  eq(L2(9.5, 10, 9.6, true, null, null, { covered: null, shares: 100 }), 'broker 9.60 \u2014 in place', 'coverage unknown: the price decides');
  eq(L2(9.5, 10, 9.2, true, null, null, { covered: 100, shares: 100, verifyOnly: true }), 'broker 9.20 below the stop of record \u2014 verify first');
  eq(L2(9.5, 10, 9.6, true, null, null, { covered: 100, shares: 100, verifyOnly: true }), 'broker 9.60 \u2014 in place');
  eq(L2(9.5, 10, null, true, null, null, { verifyOnly: true }), 'no broker stop \u2014 verify first');
  eq(L2(9.5, 10, null, true, 9.0, null, { verifyOnly: true }), 'broker 9.00 not working \u2014 verify first');
  const v = makeEnv(NEW, { fetch: NO_NET }).ctx.dp2FinalVerifyOnly_;
  ok(v({ todo: 'Hold. Verify at IBKR before any change: quantity differs.' }) && v({ todo: 'Verify A.US at IBKR before acting: x.' }) &&
     v({ todo: 'Verify A.US before any new buy or stop change: x.' }), 'the three verify forms');
  ok(!v({ todo: 'Hold; stop 9.50.' }) && !v({ todo: 'Hold; raise the stop to 9.50 for all 100 shares.' }) && !v(null) && !v({}), 'ordinary rows');
}
const MUTANTS = [
  ['the cell not replaced', "      if (fc.row && fc.row.stop) dynTxt = dp2StopOfRecordText_(", "      if (false) dynTxt = dp2StopOfRecordText_("],
  ['a low broker stop reads in place', "    var low = brokerStop < recStop - 0.005;", "    var low = brokerStop < recStop - 1;"],
  ['the v2 rule dropped', "(v2Stop ? '\\nv2 rule ' + dp2Fmt_(v2Stop, 2) + ' (analysis)' : '')", "''"],
  ['nothing-to-change listed', "nothing to change\\.$/.test(dp2Str_(r.todo).trim())) return null;", "nothing to change\\.$/.test(dp2Str_(r.todo).trim())) r = r;"],
  ['a partial stop reads in place', "  var partial = typeof cov.covered === 'number' && typeof cov.shares === 'number' && cov.covered > 0 && cov.covered < cov.shares - 1e-9;", "  var partial = false;"],
  ['verify-only still instructs', "  var verify = cov.verifyOnly === true,", "  var verify = false,"],
  ['unknown orders read as none', "  else if (!known) line2 = 'broker orders not read';", "  else if (known) line2 = 'broker orders not read';"]
];
function signature(src) {
  const quiet = "Hold; the stop working at IBKR (14.56) already covers the plan's 14.00 on all 100 shares - nothing to change.";
  const a = buildWith(src, 'pd', { final: finalGrid(FINAL_ROWS), fetch: frstYahoo() }), b = buildWith(src, 'pd', { final: finalGrid(rowsWith('Hold; stop 14.90.', 14.9)) });
  const c = buildWith(src, 'pd', { final: finalGrid(rowsWith(quiet, 14)) }), d = buildWith(src, 'pd', { props: { DP2_FINAL: 'off' }, final: finalGrid(FINAL_ROWS) });
  const e = buildWith(src, 'pd', { final: finalGrid(FINAL_ROWS), capture: null });
  const part = clone(FIX.ibkr_capture);
  for (const o of part.json.orders.orders) if (/STP/.test(o.order_type)) { o.total_shares_qty = 40; o.remaining_shares_qty = 40; o.primary_description = 'SELL 40 FRST STP 14.56'; }
  const g = buildWith(src, 'pd', { final: finalGrid(FINAL_ROWS), capture: part });
  const h = buildWith(src, 'pd', { final: finalGrid(rowsWith('Hold. Verify at IBKR before any change: the quantity differs.', 14.9)) });
  return sha256(JSON.stringify([cellOf(a), cellOf(b), actionLines(sheet(c, PDV)), cellOf(d), cellOf(e), cellOf(g), cellOf(h)]));
}
function battery() {
  passed = 0; failed = 0; const out = [];
  test('R0 oracle: the reversal of the v1.4.3 hunks is the v1.4.2 file (SHA-256 pinned, history chain); versions; both compile; +2 functions; ES5 / ASCII', () => checkOracle());
  test('R1 GOLDEN NEGATIVE: DP2_FINAL off, no stop on the final row, or no Final_Decisions tab -> the v1.4.2 pages; Top 10 v2 unchanged', () => checkGolden());
  test('R2 a final row with a stop: only the Dyn stop cell changes - stop of record first, broker against it, own stop as analysis; arrow only when the broker is below', () => checkCell(out));
  test('R3 ACTIONS TODAY: "nothing to change." is not listed (1.4.2 listed it); a real raise is', () => checkActions());
  test('R4 the pure helpers: coverage, verify-only', () => checkPure());
  test('R5 mutants: ' + MUTANTS.length + ' one-line mutations are each caught', () => {
    const want = signature(NEW);
    for (const [label, from, to] of MUTANTS) {
      eq(NEW.split(from).length - 1, 1, 'mutant anchor unique: ' + label);
      let caught = false; try { caught = signature(NEW.replace(from, () => to)) !== want; } catch (e) { caught = true; }
      ok(caught, 'mutant not caught: ' + label);
    }
    out.push(want);
  });
  return sha256(JSON.stringify(out));
}
const digests = [];
for (let run = 0; run < 3; run++) { digests.push(battery()); if (failed) { console.log('FAIL ' + passed + '/' + (passed + failed)); process.exit(1); } }
if (!(digests[0] === digests[1] && digests[1] === digests[2])) { console.log('FAIL digest mismatch'); process.exit(1); }
console.log('digest ' + digests[0].slice(0, 16) + ' x3 identical');
console.log('PASS ' + passed + '/' + (passed + failed));
