#!/usr/bin/env node
/* tests/test_dp2_v1400_one_decision.js
 * Harness for apps_script/27_Decision_Pages_v2.gs v1.4.0 [ONE-DECISION] (owner, Mon 16 Nov 2026 01:01 Riyadh: "the final
 * decision should be one decision buy hold sale or watchlist or don't invest"). Both pages read the Final_Decisions tab
 * that today_action_plan 1.6.0 writes (one decision per stock) and show it as theirs; their own rules stay as analysis.
 * The 1.2.33 history checks (DP2_EXIT_SANITY) are on by default.
 * The REAL file runs in a vm with the GAS shim, fixture (tests/fixtures/dp2_board_2026-11-02.json) and board helpers of
 * tests/test_dp2_v1301_slot_carry.js, copied verbatim. The v1.3.1 file (OLD, SHA-256 pinned adffdfed = origin/main
 * c8c0d51) is rebuilt by reversing the enumerated v1.4.0 hunks (tests/js/dp2_edits_v1400.json). Every Final_Decisions
 * row below is synthetic.
 * Sections:
 *   P0  oracle: the reversal is the v1.3.1 file; version lockstep; both compile
 *   P1  GOLDEN NEGATIVE: DP2_FINAL = off (and the history checks at the old default) -> both pages, every cell, format,
 *       chart, the call log, the e-mails and the return values equal the v1.3.1 build, on three boards
 *   P2  Portfolio_Decision_v2 with the final decisions: the Exit signal cell, ACTIONS TODAY and the e-mail follow them
 *   P3  Top_10_Investments_v2: the seat status starts with the final decision; Alloc / Buy qty only on a BUY
 *   P4  an old, missing or unreadable Final_Decisions table: 'FINAL: n/a (why)', and no page gives an order
 *   P5  the pure helpers and the self-test core; the history checks default on
 * The last line is 'PASS k/k' (exit 1 on any failure).
 * Usage: node tests/test_dp2_v1400_one_decision.js [new.gs]
 */
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const vm = require('vm');
const ROOT = path.join(__dirname, '..');
const NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '27_Decision_Pages_v2.gs');
const NEW = fs.readFileSync(NEW_PATH, 'utf8');
const FIX = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'fixtures', 'dp2_board_2026-11-02.json'), 'utf8'));
const EDITS = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'dp2_edits_v1400.json'), 'utf8'));
const V1301_SHA256 = 'adffdfeddf59ecdfafcb8ad8e13a72454ba42bc99afd7381b8e89df9efd041e6';   // v1.3.1 (origin/main c8c0d51)
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
  for (const v of ['1.4.0', '1.3.1', '1.3.0', '1.2.36', '1.2.35', '1.2.34', '1.2.33', '1.2.32', '1.2.31']) s = s.split('Decision Pages v2 ' + v).join('Decision Pages v2 1.2.30');
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
    eq(s.split(cur).length - 1, 1, 'exactly one approved v1.4.0 hunk: ' + cur.split('\n')[0].slice(0, 90));
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

test('P0 oracle: the reversal of the v1.4.0 hunks is the v1.3.1 file; versions; both compile', () => {
  ok(OLD !== null, 'the hunks reverse');
  eq(sha256(OLD), V1301_SHA256, 'v1.3.1 SHA-256');
  eq(EDITS.length, 16, 'sixteen hunks');
  ok(/ \* Version: 1\.4\.0 \(2026-11-16\)/.test(NEW) && /var DP2_VERSION = '1\.4\.0';/.test(NEW), 'version lockstep');
  new vm.Script(NEW, { filename: '27_Decision_Pages_v2.gs' }); new vm.Script(OLD, { filename: 'old.gs' });
  for (const name of ['dp2FinalFromValues_', 'dp2FinalAgeH_', 'dp2FinalState_', 'dp2ReadFinal_', 'dp2FinalCell_', 'dp2FinalAction_', 'dp2FinalCoreOk_', 'dp2FinalOn_'])
    ok(NEW.includes('function ' + name + '(') && !OLD.includes('function ' + name + '('), name);
  ok(!/[^\x00-\x7F]/.test(EDITS.map(e => e[0]).join('').replace(/[·—→]/g, '')), 'the new lines are ASCII');
  ok(!/\b(let|const)\s|=>|`/.test(EDITS.map(e => e[0]).join('\n').replace(/don't/g, '')), 'ES5 only');
});

test('P1 golden negative: DP2_FINAL = off equals the v1.3.1 build on three boards (history checks at the old default)', () => {
  for (const bname of ['today', 'research', 'held']) {
    for (const which of ['pd', 't10']) {
      const fin = finalGrid(FINAL_ROWS);
      const a = buildWith(NEW, which, { board: BOARDS[bname], props: { DP2_FINAL: 'off', DP2_EXIT_SANITY: 'off' }, final: fin });
      const b = buildWith(OLD, which, { board: BOARDS[bname], props: { DP2_FINAL: 'off', DP2_EXIT_SANITY: 'off' }, final: fin });
      deq(norm(a), norm(b), bname + ' ' + which);
    }
  }
  // with the history checks on in both, the off switch still gives the 1.3.1 page
  const a = buildWith(NEW, 'pd', { props: { DP2_FINAL: 'no' } }), b = buildWith(OLD, 'pd', { props: { DP2_EXIT_SANITY: 'on' } });
  deq(norm(a), norm(b), 'DP2_FINAL = no, sanity on');
});

test('P2 Portfolio_Decision_v2 shows the final decision; its own rule is analysis; ACTIONS TODAY and e-mail follow', () => {
  // FRST.US (entry 15.20 on 26 Oct) given a history far under its entry: the 1.3.1 page acts on its own rule
  const FALL = { 'FRST.US': { base: 11.0, step: 0.05 } };
  const base = buildWith(OLD, 'pd', { bars: FALL });
  const baseActs = actionLines(sheet(base, PDV));
  ok(baseActs.some(t => /FRST\.US/.test(t)), 'the 1.3.1 page gives its own FRST.US line: ' + JSON.stringify(baseActs));
  ok(baseActs.some(t => /^\d\. Board: 6 engine tickets/.test(t)), 'and the board tickets line');
  const r = buildWith(NEW, 'pd', { final: finalGrid(FINAL_ROWS), bars: FALL });
  const sh = sheet(r, PDV);
  const sig = cellsWith(sh, /^FINAL: /).map(x => x[1]);
  ok(sig.some(t => /^FINAL: HOLD \(Today Action Plan\) · v2 rule: /.test(t)), 'FRST.US reads the final HOLD: ' + JSON.stringify(sig));
  ok(cellsWith(sh, /^ONE DECISION PER STOCK: the Exit signal column/).length === 1, 'the page says where its decisions come from');
  ok(cellsWith(sh, /Final_Decisions of 2026-11-02 09:00 Riyadh/).length === 1, 'and as of when');
  const acts = actionLines(sh);
  ok(!acts.some(t => /FRST\.US/.test(t)), 'no own FRST.US line, and a plain hold at its stop asks nothing: ' + JSON.stringify(acts));
  ok(!acts.some(t => /SELL NOW|SWITCH|Switch advisory|engine tickets/.test(t)), 'no own order of any kind: ' + JSON.stringify(acts));
  eq(r.mails.filter(m => /SELL/.test(JSON.stringify(m))).length, 0, 'no SELL e-mail on a final HOLD');
  // the same holding with a final SELL: the action leads
  const sell = FINAL_ROWS.map(x => x[0] === 'FRST.US' ? ['FRST.US', 'SELL', 'Sell all 100 shares.', 100, '', 14] : x);
  const r2 = buildWith(NEW, 'pd', { final: finalGrid(sell), bars: FALL });
  const acts2 = actionLines(sheet(r2, PDV));
  eq(acts2[0], '1. SELL FRST.US (Today Action Plan): Sell all 100 shares.', 'the final SELL is action 1');
  ok(cellsWith(sheet(r2, PDV), /^FINAL: SELL \(Today Action Plan\)/).length >= 1, 'the cell reads FINAL: SELL');
  // the e-mail of a final SELL carries the plan's instruction, never the v2 rule's own liquidation (Codex P1, #1000)
  const LIVE = frstYahoo();
  const baseMail = JSON.stringify(buildWith(OLD, 'pd', { fetch: LIVE }).mails);
  ok(/FRST\.US/.test(baseMail) && /Sell now \(market/.test(baseMail), 'the 1.3.1 page e-mails its own sale: ' + baseMail.slice(0, 300));
  const half = FINAL_ROWS.map(x => x[0] === 'FRST.US' ? ['FRST.US', 'SELL', 'Sell half (50 shares); keep a stop at 14.00 on the rest.', 100, '', 14] : x);
  const r3 = buildWith(NEW, 'pd', { final: finalGrid(half), fetch: LIVE });
  const mail = JSON.stringify(r3.mails);
  ok(/FINAL DECISION SELL \(Today Action Plan\): Sell half \(50 shares\); keep a stop at 14\.00 on the rest\./.test(mail), 'the half sale is mailed: ' + mail.slice(0, 400));
  ok(!/Sell now \(market/.test(mail) && /v2 rule \(analysis\): /.test(mail), 'no own liquidation instruction');
  eq(buildWith(NEW, 'pd', { final: finalGrid(FINAL_ROWS), fetch: LIVE }).mails.length, 0, 'a final HOLD sends no sale e-mail on the same live stop hit');
  // a hold that asks for something (a raised stop) is an action
  const raise = FINAL_ROWS.map(x => x[0] === 'FRST.US' ? ['FRST.US', 'HOLD', 'Hold; raise the stop to 14.50 for all 100 shares.', 100, '', 14.5] : x);
  ok(actionLines(sheet(buildWith(NEW, 'pd', { final: finalGrid(raise), bars: FALL }), PDV)).some(t => /HOLD FRST\.US \(Today Action Plan\): Hold; raise the stop to 14\.50/.test(t)), 'raise');
});

test('P3 Top_10_Investments_v2: the seat status starts with the final decision; sizing only on a BUY', () => {
  const r = buildWith(NEW, 't10', { final: finalGrid(FINAL_ROWS) });
  const sh = sheet(r, T10);
  ok(cellsWith(sh, /^ONE DECISION PER STOCK: each seat's status/).length === 1, 'the note');
  const status = c => { const hit = cellsWith(sh, new RegExp('^' + c.replace('.', '\\.') + ' \u00b7 ')).map(h => +h[0].split(':')[0]).sort((a, b) => a - b); ok(hit.length >= 1, c); return hit[0]; };   // the seats table is the first
  const hdr = cellsWith(sh, /^Seat status$/); ok(hdr.length === 1, 'seats header'); const hr = +hdr[0][0].split(':')[0];
  const col = n => { for (let c = 1; c <= 20; c++) if (val(sh, hr, c) === n) return c; throw new Error('no column ' + n); };
  const cS = col('Seat status'), cA = col('Alloc SAR'), cQ = col('Buy qty');
  const iRow = status('ITRN.US'), dRow = status('DDI.US'), cRow = status('CRC.US'), gRow = status('GLPI.US');
  ok(/^FINAL: BUY \(Today Action Plan\) · seat: /.test(val(sh, iRow, cS)), 'ITRN BUY: ' + val(sh, iRow, cS));
  eq(val(sh, iRow, cQ), 7, 'the plan qty'); eq(val(sh, iRow, cA), 276, 'the plan cost');
  ok(/^FINAL: WATCH \(Today Action Plan\) · seat: /.test(val(sh, dRow, cS)), 'DDI WATCH');
  ok(/^FINAL: DO NOT INVEST \(Today Action Plan\)/.test(val(sh, cRow, cS)), 'CRC DO NOT INVEST');
  ok(/^FINAL: n\/a \(not on Final_Decisions\) · seat: /.test(val(sh, gRow, cS)), 'GLPI not listed');
  for (const rr of [dRow, cRow, gRow]) { ok(typeof val(sh, rr, cQ) !== 'number', 'no qty on a non-BUY seat'); ok(typeof val(sh, rr, cA) !== 'number', 'no alloc'); }
  ok(cellsWith(sh, /engine sizing \d/).length >= 1, 'the engine sizing is kept as analysis in Flags');
  // the Today Action Plan reads this page: only the BUY is a ticket there
  const old = buildWith(OLD, 't10', {});
  ok(cellsWith(sheet(old, T10), /^FINAL/).length === 0, 'the 1.3.1 page has no final cells');
});

test('P4 an old, missing or unreadable table reads FINAL: n/a and no page gives an order', () => {
  const stale = buildWith(NEW, 'pd', { final: finalGrid(FINAL_ROWS, '2026-11-01 20:00') });
  const sh = sheet(stale, PDV);
  ok(cellsWith(sh, /^FINAL: n\/a \(Final_Decisions is 13\.4 h old \(limit 6 h\)\) · v2 rule: /).length >= 1, 'stale cell');
  eq(actionLines(sh)[0], '1. Final decisions not available (Final_Decisions is 13.4 h old (limit 6 h)): take orders from the Today_Action_Plan page only; nothing on this page is an order.', 'first action');
  ok(!actionLines(sh).some(t => /SELL NOW|SWITCH/.test(t)), 'no own order');
  const longer = buildWith(NEW, 'pd', { final: finalGrid(FINAL_ROWS, '2026-11-01 20:00'), props: { DP2_FINAL_MAX_H: '24' } });
  ok(cellsWith(sheet(longer, PDV), /^FINAL: HOLD \(Today Action Plan\)/).length >= 1, 'DP2_FINAL_MAX_H widens the limit');
  const none = buildWith(NEW, 'pd', {});
  ok(cellsWith(sheet(none, PDV), /^FINAL: n\/a \(Final_Decisions tab missing or empty\)/).length >= 1, 'missing tab');
  const junk = buildWith(NEW, 't10', { final: [['x', 'y'], ['1', '2']] });
  ok(cellsWith(sheet(junk, T10), /^FINAL: n\/a \(Final_Decisions has no decisions table\) · seat: /).length >= 1, 'no table');
  ok(cellsWith(sheet(junk, T10), /engine sizing \d/).length >= 1, 'engine tickets are analysis even then');
});

test('P5 pure helpers, the self-test core and the history checks on by default', () => {
  const env = makeEnv(NEW, { fetch: NO_NET });
  const c = env.ctx;
  ok(c.dp2FinalCoreOk_() === true, 'core');
  const p = c.dp2FinalFromValues_(finalGrid(FINAL_ROWS));
  ok(p.ok && Object.keys(p.rows).length === 5 && p.at === '2026-11-02 09:00', 'parsed');
  eq(p.rows['ITRN.US'].qty, 7); eq(p.rows['ITRN.US'].cost, 276); eq(p.rows['CRC.US'].decision, 'DO NOT INVEST');
  eq(c.dp2FinalAgeH_('2026-11-02 09:00', NOW), 0.4); eq(c.dp2FinalAgeH_('junk', NOW), null);
  eq(c.dp2FinalAction_('A', { decision: 'HOLD', todo: 'Hold; stop 1.00.' }), null);
  eq(c.dp2FinalAction_('A', { decision: 'HOLD', todo: 'Hold.' }), null);
  eq(c.dp2FinalAction_('A', { decision: 'WATCH', todo: 'x' }).p, 2);
  eq(c.dp2FinalAction_('A', { decision: 'SELL', todo: 'x' }).p, 0);
  for (const [v, on] of [[null, true], ['', true], ['on', true], ['junk', true], ['off', false], ['0', false], ['NO', false], ['false', false]]) {
    const e = makeEnv(NEW, { fetch: NO_NET, props: v === null ? {} : { DP2_EXIT_SANITY: v, DP2_FINAL: v } });
    eq(e.ctx.dp2ExitSanityOn_(), on, 'sanity ' + v); eq(e.ctx.dp2FinalOn_(), on, 'final ' + v);
  }
  const eo = makeEnv(OLD, { fetch: NO_NET });
  eq(eo.ctx.dp2ExitSanityOn_(), false, 'the 1.3.1 default was off');
});

console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
