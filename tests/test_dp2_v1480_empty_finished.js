#!/usr/bin/env node
/* tests/test_dp2_v1480_empty_finished.js
 * Harness for apps_script/27_Decision_Pages_v2.gs v1.4.8 [EMPTY-FINISHED] (owner, Wed 25 Nov 2026 15:59 Riyadh:
 * Top_10_Investments_v2 built 15:42 still showed the 02:17:42 run while Top_10_Investments held the 15:22:11 run of the
 * released sync - 'status: ok | output: EMPTY', 9,727 scanned, 0 passed, 0 seats). The 1.4.1 [LAST-GOOD] rule read any
 * board with no seat as a render Google stopped and printed the kept 02:17:42 board.
 * The REAL file runs in a vm with the GAS shim, fixture and board helpers of tests/test_dp2_v1410_top10_v2_review.js
 * (copied). The v1.4.7 file (OLD, SHA-256 pinned 8d7e7daf = origin/main a739438) is rebuilt by reversing the enumerated
 * v1.4.8 hunks (tests/js/dp2_edits_v1480.json). Every board below is the 2 Nov fixture or a synthetic variant of it.
 *   E0  oracle: reversal = v1.4.7; history chain; version lockstep; both compile; +3 functions, 0 removed; ES5 / ASCII
 *   E1  GOLDEN NEGATIVE: DP2_T10_EMPTY_LIVE off (five spellings) -> the v2 page equals the v1.4.7 build on the finished
 *       empty board; on or off, a stopped render and a seated board equal v1.4.7
 *   E2  the 25 Nov case: a finished empty render is read live (no amber line, no kept seats, the live run's time);
 *       v1.4.7 printed the kept board
 *   E3  a stopped render ('running') still reads the kept copy
 *   E4  the pure rule, the gate spellings, the self-test line
 *   E5  mutants: each guard removed is caught
 * The last line is 'PASS k/k' (exit 1 on any failure).
 */
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const vm = require('vm');
const ROOT = path.join(__dirname, '..');
const NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '27_Decision_Pages_v2.gs');
const NEW = fs.readFileSync(NEW_PATH, 'utf8');
const HISTORY = require(path.join(ROOT, 'tests', 'js', 'dp2_history.js'));
const FIX = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'fixtures', 'dp2_board_2026-11-02.json'), 'utf8'));
const EDITS = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'dp2_edits_v1480.json'), 'utf8'));
const V1470_SHA256 = '8d7e7daf0c0dfbb656e5386081e7d759eff76523fb21003e34b7c5200e6fefe1';   // v1.4.7 (origin/main a739438)
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
  for (const v of ['1.4.1', '1.4.0', '1.3.1', '1.3.0', '1.2.36', '1.2.35', '1.2.34', '1.2.33', '1.2.32', '1.2.31']) s = s.split('Decision Pages v2 ' + v).join('Decision Pages v2 1.2.30');
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
    eq(s.split(cur).length - 1, 1, 'exactly one approved v1.4.8 hunk: ' + cur.split('\n')[0].slice(0, 90));
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


const EMPTY_STATUS = 'Last run 2026-11-02 09:20:11 | status: ok | output: EMPTY | sheets pool 9727 rows [Market_Leaders 255/255, Global_Markets 6602/6602]';
function emptyFinished(top) {   // a render that ran to its end and seated nobody: title, status, panel, KPIs
  const g = top10Grid(top).slice(0, 16);
  g[1] = ['Status', EMPTY_STATUS];
  return g;
}
function chainWith(source, first, second) {   // two builds over one workbook, on a given source
  const e1 = makeEnv(source, Object.assign({ fetch: NO_NET }, first || {}));
  e1.ctx.buildTop10V2();
  if (second && second.top) e1.book.load('Top_10_Investments', second.top);
  const e2 = makeEnv(source, Object.assign({ fetch: NO_NET, book: e1.book }, second || {}));
  const ret = e2.ctx.buildTop10V2();
  return { e1, e2, ret: plain(ret), sh: e2.book.getSheetByName(T10) };
}
function pageState(r) {   // the v2 page as cells, the version string made neutral
  let s = JSON.stringify(r.sh._state());
  for (const v of ['1.4.8', '1.4.7']) s = s.split('Decision Pages v2 ' + v).join('Decision Pages v2 x');
  return JSON.parse(s);
}
const SRC = { v: NEW };   // the source E2-E4 run on (E5 swaps in a mutant)

test('E0 oracle: reversal = v1.4.7 (SHA-256 pinned, history chain); versions; both compile; +3 functions, 0 removed; ES5 / ASCII hunks', () => {
  ok(OLD !== null, 'the hunks reverse');
  eq(sha256(OLD), V1470_SHA256, 'v1.4.7 SHA-256');
  eq(HISTORY.V1470_SHA256, V1470_SHA256, 'history pin');
  eq(sha256(HISTORY.v1470Source(NEW)), V1470_SHA256, 'history chain');
  eq(sha256(HISTORY.v1460Source(NEW)), HISTORY.V1460_SHA256, 'the older chain still lands on v1.4.6');
  eq(HISTORY.v1470Source(OLD), OLD, 'a source that already is the v1.4.7 file is returned unchanged');
  eq(EDITS.length, 6, 'six hunks');
  ok(/ \* Version: 1\.4\.8 \(2026-11-25\)/.test(NEW) && /var DP2_VERSION = '1\.4\.8';/.test(NEW), 'version lockstep');
  ok(NEW.indexOf(' *   1.4.8: [EMPTY-FINISHED]') > 0 && NEW.indexOf(' *   1.4.7: [DIV-LOG]') > NEW.indexOf(' *   1.4.8: [EMPTY-FINISHED]'), 'the new WHY block is first, the 1.4.7 one kept');
  new vm.Script(NEW, { filename: '27_Decision_Pages_v2.gs' }); new vm.Script(OLD, { filename: 'old.gs' });
  const fns = s => new Set((s.match(/^function ([A-Za-z0-9_]+)\(/mg) || []).map(x => x.slice(9, -1)));
  const a = fns(OLD), b = fns(NEW);
  deq([...b].filter(x => !a.has(x)).sort(), ['dp2T10EmptyFinished_', 'dp2T10EmptyLiveOn_', 'dp2V148CoreOk_'], '+3 functions');
  deq([...a].filter(x => !b.has(x)), [], '0 removed');
  for (const [cur] of EDITS) for (const line of cur.split('\n')) {
    ok(![...line].some(ch => ch.charCodeAt(0) > 127), 'ASCII only: ' + line.slice(0, 60));
    ok(!/\b(let|const)\s|=>|`/.test(line.replace(/'[^']*'/g, "''")), 'ES5 only: ' + line.slice(0, 80));
  }
});

test('E1 GOLDEN NEGATIVE: DP2_T10_EMPTY_LIVE off (five spellings) = v1.4.7 on the finished empty board; a stopped and a seated board = v1.4.7 either way', () => {
  const old = pageState(chainWith(OLD, {}, { top: emptyFinished(FIX.top10) }));
  for (const off of ['0', 'off', 'OFF', 'false', ' no ']) {
    deq(pageState(chainWith(NEW, { props: { DP2_T10_EMPTY_LIVE: off } }, { top: emptyFinished(FIX.top10), props: { DP2_T10_EMPTY_LIVE: off } })), old, 'off = v1.4.7: ' + JSON.stringify(off));
  }
  for (const props of [{}, { DP2_T10_EMPTY_LIVE: 'off' }]) {
    deq(pageState(chainWith(NEW, { props }, { top: stopped(FIX.top10), props })), pageState(chainWith(OLD, {}, { top: stopped(FIX.top10) })), 'a stopped render: ' + JSON.stringify(props));
    deq(pageState(chainWith(NEW, { props }, { props })), pageState(chainWith(OLD, {}, {})), 'a seated board: ' + JSON.stringify(props));
  }
});

function checkE2(src) {
  const r = chainWith(src, {}, { top: emptyFinished(FIX.top10) });
  eq(seatRows(r.sh), 0, 'no kept seat on the page');
  eq(cellsWith(r.sh, /^SEATS FROM THE LAST FINISHED RENDER/).length, 0, 'no amber did-not-finish line');
  ok(cellsWith(r.sh, /2026-11-02 09:20:11/).length > 0, 'the live run is the one shown');
  ok(r.e2.book.getSheetByName('_DP2_T10_LastGood').getLastRow() > 20, 'the kept copy is left as it was');
  const e3 = makeEnv(src, { fetch: NO_NET, book: r.e2.book });
  const t = plain(e3.ctx.dp2ReadTop10_());
  eq(t.seats.length, 0); eq(t.lastRun, '2026-11-02 09:20:11'); ok(!t.fromLastGood, 'read live');
  return r;
}
test('E2 the 25 Nov case: a finished empty render is read live; v1.4.7 printed the kept board under a did-not-finish line', () => {
  checkE2(SRC.v);
  const o = chainWith(OLD, {}, { top: emptyFinished(FIX.top10) });
  ok(seatRows(o.sh) >= 6, 'v1.4.7 showed the kept seats');
  const amber = cellsWith(o.sh, /^SEATS FROM THE LAST FINISHED RENDER/);
  eq(amber.length, 1, 'v1.4.7 amber line');
  ok(/its render of 2026-11-02 09:20:11 did not finish \(status: ok \| output: EMPTY/.test(amber[0][1]), amber[0][1]);
});

function checkE3(src) {
  const r = chainWith(src, {}, { top: stopped(FIX.top10) });
  ok(seatRows(r.sh) >= 6, 'every kept seat');
  eq(cellsWith(r.sh, /^SEATS FROM THE LAST FINISHED RENDER/).length, 1, 'the amber line');
}
test('E3 a stopped render (status: running) still reads the kept copy', () => { checkE3(SRC.v); });

function checkE4(src) {
  const c = makeEnv(src, { fetch: NO_NET }).ctx;
  const done = 'Last run 2026-11-25 15:22:11 | status: ok | output: EMPTY | sheets pool 9727 rows';
  eq(c.dp2T10EmptyFinished_(done), true, 'the 25 Nov line');
  eq(c.dp2T10EmptyFinished_('Last run 2026-11-25 08:44:11 | status: running…'), false, 'running');
  eq(c.dp2T10EmptyFinished_('Last run 2026-11-25 02:17:42 | status: ok | output: HELD'), false, 'a seated output');
  eq(c.dp2T10EmptyFinished_('Last run 2026-11-25 02:17:42 | status: error | output: EMPTY'), false, 'an error');
  eq(c.dp2T10EmptyFinished_('Last run x | status: ok | output: EMPTY | running'), false, 'a line still running');
  eq(c.dp2T10EmptyFinished_(''), false); eq(c.dp2T10EmptyFinished_(null), false);
  eq(c.dp2T10LastGoodAction_({ ok: true, seats: [], status: done }, true), 'live');
  eq(c.dp2T10LastGoodAction_({ ok: true, seats: [], status: done }, false), 'use', 'gate off = 1.4.7');
  eq(c.dp2T10LastGoodAction_({ ok: true, seats: [], status: done }), 'use', 'one argument = 1.4.7');
  eq(c.dp2T10LastGoodAction_({ ok: true, seats: [], status: 'Last run x | status: running…' }, true), 'use');
  eq(c.dp2T10LastGoodAction_({ ok: true, seats: [1], status: 'Last run x | status: ok | output: HELD' }, true), 'keep');
  eq(c.dp2T10LastGoodAction_({ ok: false, seats: [], status: '' }, true), 'live');
  eq(c.dp2V148CoreOk_(), true, 'self-check core');
  ok(src.indexOf("checks.push({ name: 'v1.4.8 core: a finished empty Top 10 render is the live board (DP2_T10_EMPTY_LIVE: ' + (dp2T10EmptyLiveOn_() ? 'on' : 'off') + ')', ok: dp2V148CoreOk_() });") > 0, 'self-test line');
  eq(makeEnv(src, { fetch: NO_NET, props: { DP2_T10_EMPTY_LIVE: 'off' } }).ctx.dp2T10EmptyLiveOn_(), false, 'off');
  eq(c.dp2T10EmptyLiveOn_(), true, 'default on');
  eq(makeEnv(src, { fetch: NO_NET, props: { DP2_T10_EMPTY_LIVE: 'yes' } }).ctx.dp2T10EmptyLiveOn_(), true, 'any other word = on');
}
test('E4 the pure rule, the gate spellings and the self-test line', () => { checkE4(SRC.v); });

test('E5 mutants: four guards removed, each caught', () => {
  const muts = [
    ["&& !/running/i.test(s);", ";"],
    ["/\\|\\s*output:\\s*EMPTY\\b/i.test(s) && ", ""],
    ["return emptyLive && dp2T10EmptyFinished_(parsed.status) ? 'live' : 'use';", "return dp2T10EmptyFinished_(parsed.status) ? 'live' : 'use';"],
    ["dp2T10LastGoodAction_(live, dp2T10EmptyLiveOn_())", "dp2T10LastGoodAction_(live)"]
  ];
  for (const [from, to] of muts) {
    eq(NEW.split(from).length, 2, 'mutant site unique: ' + from);
    const M = NEW.replace(from, to);
    let caught = false;
    const save = SRC.v; SRC.v = M;
    try {
      checkE2(M); checkE3(M); checkE4(M);
      // the golden negative on the mutant
      const old = pageState(chainWith(OLD, {}, { top: emptyFinished(FIX.top10) }));
      deq(pageState(chainWith(M, { props: { DP2_T10_EMPTY_LIVE: 'off' } }, { top: emptyFinished(FIX.top10), props: { DP2_T10_EMPTY_LIVE: 'off' } })), old, 'off = v1.4.7');
    } catch (e) { caught = true; }
    SRC.v = save;
    ok(caught, 'mutant survived: ' + from);
  }
});

console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
