#!/usr/bin/env node
/* tests/test_dp2_v1450_div_net.js
 * Harness for apps_script/27_Decision_Pages_v2.gs v1.4.5 [DIV-NET] (owner, Tue 17 Nov 2026 10:33 Riyadh, asked whether the
 * entry should count a dividend net of the tax withheld: "go with the best option"). The exit engine cell lowered the
 * entry by the whole dividend; a dividend whose tax is recorded in _Dividend_Log now counts net of that tax, the rule of
 * exit_plan 1.4.0. Shims copied from tests/test_dp2_v1440_three_cells.js; the chart stub and the bar maker from
 * tests/test_dp2_v1233_exit_sanity.js. The v1.4.4 file (OLD, SHA-256 pinned 988c1f53 = origin/main 626f676) is rebuilt by
 * reversing the enumerated v1.4.5 hunks (tests/js/dp2_edits_v1450.json). Every symbol, row, price and date is synthetic.
 *   S0  oracle: reversal = v1.4.4; history chain; versions; both compile; +6 functions, 0 removed; ES5 / ASCII of the new lines
 *   S1  GOLDEN NEGATIVE: DP2_DIV_NET off (five spellings) with a tax row in the log -> both pages equal the v1.4.4 build;
 *       by default with no log, a log with no tax row, or a tax row of a symbol that is not held -> equal too
 *   S2  net: a held lot paid a dividend whose tax is in the log -> the engine cell names it, the dynamic stop is the pure
 *       replay with the net dividend, and no cell outside that holding's row changes
 *   S3  scope: bought on the ex-date, an implausible share, a log ex-date a week away -> the 1.4.4 cells; an ex-date one
 *       day off and a dividend that only Dividend_Events lists -> net
 *   S4  the pure helpers, the gate spellings, the self-test line
 *   S5  mutants (the page cells of seven shapes and eight pure calls are the oracle)
 * The battery runs three times and the digests must match; the last line is 'PASS k/k' (exit 1 on any failure).
 */
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const vm = require('vm');
const ROOT = path.join(__dirname, '..');
const NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '27_Decision_Pages_v2.gs');
// 2026-11-19: v1.4.6 [RETURN-COL] is delivered; this harness proves the v1.4.5 diff, so it reads the v1.4.5 file rebuilt
// from the delivered one (tests/js/dp2_history.js: the v1.4.6 hunks reversed, SHA-256 pinned).
const NEW = require(path.join(__dirname, 'js', 'dp2_history.js')).v1450Source(fs.readFileSync(NEW_PATH, 'utf8'));
const FIX = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'fixtures', 'dp2_board_2026-11-02.json'), 'utf8'));
const EDITS = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'dp2_edits_v1450.json'), 'utf8'));
const HISTORY = require(path.join(ROOT, 'tests', 'js', 'dp2_history.js'));
const V1440_SHA256 = '988c1f5367e9acbcce0203d2255ac53cea2bf417faa78dc931b1f50af6fd3575';   // v1.4.4 (origin/main 626f676)
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
  if (opts.divLog) book.load('_Dividend_Log', opts.divLog);
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
  for (const v of ['1.4.5', '1.4.4', '1.4.3', '1.4.2', '1.4.1', '1.4.0', '1.3.1', '1.3.0', '1.2.36', '1.2.35', '1.2.34', '1.2.33', '1.2.32', '1.2.31']) s = s.split('Decision Pages v2 ' + v).join('Decision Pages v2 1.2.30');
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
    eq(s.split(cur).length - 1, 1, 'exactly one approved v1.4.5 hunk: ' + cur.split('\n')[0].slice(0, 90));
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

/* ============================================================ v1.4.4 battery */
const ON = {};                                    // DP2_EXIT_SANITY and DP2_DIV_NET are on by default
const NET_OFF = { DP2_DIV_NET: 'off' };
const SYM = 'TAXD.US', BUY_DAY = '2026-10-12', EX_DAY = '2026-10-13';
const DIV_NOW = Date.parse('2026-10-30T15:00:00Z');   // 18:00 Riyadh, a Friday after the ex-date
/* weekday bars, close f(i) rounded to cents; o / h / l around it */
function wbars(from, to, f) {
  const out = [], end = Date.parse(to + 'T00:00:00Z');
  for (let d = Date.parse(from + 'T00:00:00Z'), i = 0; d <= end; d += 86400000) {
    const wd = new Date(d).getUTCDay(); if (wd === 0 || wd === 6) continue;
    const c = Math.round(f(i) * 100) / 100, r2 = x => Math.round(x * 100) / 100;
    out.push({ date: new Date(d).toISOString().slice(0, 10), o: r2(c - 0.1), h: r2(c + 0.45), l: r2(c - 0.45), c }); i++;
  }
  return out;
}
const FRST_BARS = wbars('2026-08-03', '2026-10-30', i => 15.3 + 0.25 * Math.sin(i / 3));
const PRE = (() => { const b = wbars('2026-08-03', BUY_DAY, i => 50.2 + 0.3 * Math.sin(i / 2)); b[b.length - 1] = { date: BUY_DAY, o: 49.9, h: 50.6, l: 49.7, c: 50.0 }; return b; })();
const POST = wbars(EX_DAY, '2026-10-29', i => 44.0 + 0.3 * Math.sin(i / 2));
const DIV = { date: EX_DAY, amount: 10 };
/* the ledger and My_Portfolio of the fixture plus the invented lot (40 sh bought 12 Oct at 50.00) */
const lotRow = (buy, px) => [SYM, 'Tax Demo Corp', 'USD', 'Active', buy || BUY_DAY, px || 50, 40, (px || 50) * 40, 44.2, 3.75, 6630, -870, 280, 180, 0.024, 14];
const LEDGER = (buy, px) => { const l = clone(FIX.ledger); l.lots.splice(1, 0, lotRow(buy, px)); return l; };
const MP = px => { const m = clone(FIX.my_portfolio); const r = m.rows[0].slice(); r[0] = SYM; r[5] = 44.2; r[6] = 55.0; r[7] = 38.0; r[17] = 40; r[18] = px || 50; m.rows.splice(1, 0, r); return m; };
const LOG_HDR = ['Logged At', 'Type', 'Symbol', 'Lot Buy Date', 'Ex-Date', 'Amount/Share', 'Shares', 'Amount', 'Ccy', 'Note'];
const logRow = (sym, ex, amt, type) => ['2026-10-14 07:00:00', type || (amt < 0 ? 'MANUAL' : 'AUTO'), sym, BUY_DAY, ex, amt / 40, 40, amt, 'USD', amt < 0 ? 'tax' : 'chart'];
const logGrid = rows => [['DIVIDEND LOG'], LOG_HDR].concat(rows);
const LOG_TAX = logGrid([logRow('OTHER.US', '2026-09-01', 5), logRow(SYM, EX_DAY, 400), logRow(SYM, EX_DAY, -120)]);
const LOG_GROSS = logGrid([logRow(SYM, EX_DAY, 400)]);
const DIV_HDR = ['Symbol', 'Ex-Div Date', 'Days To ExDiv', 'Amount', 'Currency', 'Pay Date', 'Record Date', 'Period', 'Updated At (Riyadh)', 'Source'];
const divGrid = rows => [DIV_HDR].concat(rows.map(r => [r[0], r[1], 0, r[2], r[3], '2026-10-16', '2026-10-13', 'Other', '2026-10-13 16:45', 'synthetic']));
/* the chart API: { KEY: { bars, last, divs } }; events are returned only when the request asks (events=div) */
function chartStub(series) {
  return { fetch(url) {
    const m = /\/v8\/finance\/chart\/([^?]+)\?/.exec(String(url));
    const s = m ? series[decodeURIComponent(m[1])] : null;
    if (!s) return { getResponseCode: () => 404, getContentText: () => '{}' };
    const ts = s.bars.map(b => Date.parse(b.date + 'T13:30:00Z') / 1000);
    const res = { meta: { regularMarketPrice: s.last, currency: 'USD', exchangeTimezoneName: 'America/New_York', marketState: 'CLOSED' }, timestamp: ts,
                  indicators: { quote: [{ open: s.bars.map(b => b.o), high: s.bars.map(b => b.h), low: s.bars.map(b => b.l), close: s.bars.map(b => b.c) }] } };
    if (/[?&]events=div/.test(String(url)) && s.divs && s.divs.length) {
      res.events = { dividends: {} };
      for (const d of s.divs) { const t = Date.parse(d.date + 'T13:30:00Z') / 1000; res.events.dividends[String(t)] = { amount: d.amount, date: t }; }
    }
    return { getResponseCode: () => 200, getContentText: () => JSON.stringify({ chart: { result: [res], error: null } }) };
  } };
}
const SERIES = divs => ({ FRST: { bars: FRST_BARS, last: 15.0 }, TAXD: { bars: PRE.concat(POST), last: 44.2, divs: divs === undefined ? [DIV] : divs } });
function shape(source, which, extra) {
  extra = extra || {};
  return build(source, which || 'pd', { board: BOARDS[extra.board || 'today'], ledger: LEDGER(extra.buy, extra.px), mp: MP(extra.px), fetch: chartStub(SERIES(extra.divs)), now: DIV_NOW,
                                        divLog: extra.log, divTab: extra.divTab, props: extra.props || ON, propsThrow: extra.propsThrow });
}
const firstRow = (sh, c, pred) => { const r = rowsWhere(sh, c, pred); return r.length ? r[0] : -1; };
function holding(b, sym) {
  const sh = sheet(b, PDV), r = firstRow(sh, 1, v => v === (sym || SYM));
  ok(r > 0, 'holding row ' + (sym || SYM));
  return { row: r, dyn: String(val(sh, r, 12)), engine: String(val(sh, r, 13)), signal: String(val(sh, r, 16)) };
}
const pdCells = b => norm(b).sheets[PDV].cells;
const GROSS_TXT = ' ' + DOT + ' dividend-adjusted (10.00 ex ' + EX_DAY + ')';
const NET_TXT = ' ' + DOT + ' dividend-adjusted (10.00 ex ' + EX_DAY + ', net of 30% tax (dividend log))';

function checkOracle() {
  ok(OLD !== null, 'the hunks reverse'); eq(sha256(OLD), V1440_SHA256, 'v1.4.4 SHA-256');
  eq(HISTORY.V1440_SHA256, V1440_SHA256, 'history pin'); eq(sha256(HISTORY.v1440Source(NEW)), V1440_SHA256, 'history chain');
  eq(sha256(HISTORY.v1430Source(NEW)), HISTORY.V1430_SHA256, 'the older chain still lands on v1.4.3');
  eq(HISTORY.v1440Source(OLD), OLD, 'a source that already is the v1.4.4 file is returned unchanged');
  eq(EDITS.length, 8, 'eight hunks');
  ok(/ \* Version: 1\.4\.5 \(2026-11-17\)/.test(NEW) && /var DP2_VERSION = '1\.4\.5';/.test(NEW), 'version lockstep');
  ok(NEW.indexOf(' *   1.4.4: [PLAN-ACCEPT') > NEW.indexOf(' *   1.4.5: [DIV-NET]') && NEW.indexOf(' *   1.4.5: [DIV-NET]') > 0, 'the new WHY block is first and the 1.4.4 one is kept');
  new vm.Script(NEW, { filename: '27_Decision_Pages_v2.gs' }); new vm.Script(OLD, { filename: 'old.gs' });
  const fns = s => new Set((s.match(/^function\s+([A-Za-z_$][\w$]*)/mg) || []).map(x => x.split(/\s+/)[1]));
  const fo = fns(OLD), fn = fns(NEW);
  deq([...fo].filter(k => !fn.has(k)), [], 'functions removed');
  deq([...fn].filter(k => !fo.has(k)).sort(), ['dp2DivNetOn_', 'dp2DivTaxFromValues_', 'dp2DivTax_', 'dp2ExitDivTxt_', 'dp2ExitDivsNet_', 'dp2V145CoreOk_'], 'functions added');
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
    for (const off of ['off', 'OFF', '0', 'false', ' No ']) {
      deq(norm(shape(NEW, which, { log: LOG_TAX, props: { DP2_DIV_NET: off } })), norm(shape(OLD, which, { log: LOG_TAX, props: { DP2_DIV_NET: off } })), which + ' with DP2_DIV_NET=' + off);
    }
    for (const board of ['today', 'research']) {
      deq(norm(shape(NEW, which, { board })), norm(shape(OLD, which, { board })), which + ' ' + board + ': no dividend log');
      deq(norm(shape(NEW, which, { board, log: LOG_GROSS })), norm(shape(OLD, which, { board, log: LOG_GROSS })), which + ' ' + board + ': a log with no tax row');
    }
    const other = logGrid([logRow('OTHER.US', EX_DAY, 400), logRow('OTHER.US', EX_DAY, -120)]);
    deq(norm(shape(NEW, which, { log: other })), norm(shape(OLD, which, { log: other })), which + ': the tax of a symbol that is not held');
    deq(norm(shape(NEW, which, { log: [['x']] })), norm(shape(OLD, which, { log: [['x']] })), which + ': a log with no header');
    deq(norm(shape(NEW, which, { log: LOG_TAX, divs: [] })), norm(shape(OLD, which, { log: LOG_TAX, divs: [] })), which + ': a tax row with no dividend in the history');
  }
  ok(holding(shape(OLD, 'pd', { log: LOG_TAX })).engine.endsWith(GROSS_TXT), 'the v1.4.4 build adjusts by the whole dividend: ' + holding(shape(OLD, 'pd', { log: LOG_TAX })).engine.slice(-70));
}
function checkNet(out) {
  const n = shape(NEW, 'pd', { log: LOG_TAX }), o = shape(OLD, 'pd', { log: LOG_TAX }), hn = holding(n), ho = holding(o);
  ok(hn.engine.endsWith(NET_TXT), 'the engine cell names the dividend and its tax: ' + hn.engine.slice(-90));
  ok(ho.engine.endsWith(GROSS_TXT) && hn.engine !== ho.engine, 'the v1.4.4 cell named the whole dividend');
  const c = makeEnv(NEW, { fetch: NO_NET }).ctx, bars = PRE.concat(POST);
  const replay = amount => { const adj = c.dp2ExitDivAdjust_(bars, 50, BUY_DAY, [{ date: EX_DAY, amount, source: 'chart' }], '2026-10-30'); return { adj, x: c.dp2ExitCompute_(adj.bars, adj.entry, BUY_DAY, 44.2, c.dp2StopRuleFor_(BUY_DAY)) }; };
  const net = replay(7), gross = replay(10);
  near(net.adj.entry, 50 * (1 - 7 / 50), 1e-9, 'the net entry'); near(gross.adj.entry, 40, 1e-9, 'the gross entry');
  ok(Math.abs(net.x.stop - gross.x.stop) > 0.01, 'the two replays give different stops: ' + net.x.stop + ' vs ' + gross.x.stop);
  ok(gross.x.beLocked && !net.x.beLocked, 'the whole dividend read as a gain that locked the break-even; the net one does not');
  ok(hn.dyn.indexOf(c.dp2Fmt_(net.x.stop, 2) + ' (') >= 0, 'the stop cell carries the net replay stop ' + c.dp2Fmt_(net.x.stop, 2) + ': ' + hn.dyn.split('\n').join(' / '));
  ok(ho.dyn.indexOf(c.dp2Fmt_(gross.x.stop, 2) + ' (') >= 0, 'the v1.4.4 stop cell carried the gross replay stop: ' + ho.dyn.split('\n').join(' / '));
  ok(hn.engine.indexOf('now ' + c.dp2RFmt_(net.x.lastR)) >= 0 && ho.engine.indexOf('now ' + c.dp2RFmt_(gross.x.lastR)) >= 0 && net.x.lastR < gross.x.lastR, 'the gain reads lower on the net basis');
  const cn = pdCells(n), co = pdCells(o), diff = cn.filter((x, i) => x !== co[i]);
  eq(cn.length, co.length, 'the same cells are written');
  const rowsOf = diff.map(x => x.split(':')[0]);
  deq(diff.map(x => x.split('=')[0]), [hn.row + ':12', hn.row + ':13', hn.row + ':16'], 'only the stop, engine and signal cells of that holding change');
  void rowsOf;
  deq(norm(shape(NEW, 't10', { log: LOG_TAX })), norm(shape(OLD, 't10', { log: LOG_TAX })), 'the Top 10 page does not read the log');
  const dated = logGrid([[new Date(Date.UTC(2026, 9, 14, 4)), 'AUTO', SYM, BUY_DAY, new Date(Date.UTC(2026, 9, 13, 4)), 10, 40, 400, 'USD', ''], ['', 'MANUAL', SYM.toLowerCase(), BUY_DAY, new Date(Date.UTC(2026, 9, 13, 4)), -3, 40, -120, 'USD', '']]);
  eq(holding(shape(NEW, 'pd', { log: dated })).engine, hn.engine, 'sheet dates and a lower-case symbol read the same');
  out.push([hn.dyn, hn.engine, hn.signal]);
}
function checkScope(out) {
  const same = (extra, why) => eq(JSON.stringify(pdCells(shape(NEW, 'pd', extra))), JSON.stringify(pdCells(shape(OLD, 'pd', extra))), why + ': the 1.4.4 page');
  const exBuyLog = logGrid([[ '', 'AUTO', SYM, EX_DAY, EX_DAY, 10, 40, 400, 'USD', ''], ['', 'MANUAL', SYM, EX_DAY, EX_DAY, -3, 40, -120, 'USD', '']]);
  same({ log: exBuyLog, buy: EX_DAY, px: 44 }, 'bought on the ex-date');
  ok(/^ATR /.test(holding(shape(NEW, 'pd', { log: exBuyLog, buy: EX_DAY, px: 44 })).engine), 'and that lot has an engine reading: ' + holding(shape(NEW, 'pd', { log: exBuyLog, buy: EX_DAY, px: 44 })).engine.slice(0, 60));
  same({ log: logGrid([logRow(SYM, EX_DAY, 400), logRow(SYM, EX_DAY, -320)]) }, '20% kept');
  same({ log: logGrid([logRow(SYM, EX_DAY, 400), logRow(SYM, EX_DAY, -1)]) }, '99.75% kept');
  same({ log: logGrid([logRow(SYM, EX_DAY, 400), logRow(SYM, EX_DAY, -500)]) }, 'a reversal');
  same({ log: logGrid([logRow(SYM, '2026-10-20', 400), logRow(SYM, '2026-10-20', -120)]) }, 'a log ex-date a week from the dividend');
  same({ log: logGrid([logRow(SYM, EX_DAY, -120)]) }, 'a tax row with no dividend row');
  ok(holding(shape(NEW, 'pd', { log: LOG_TAX, propsThrow: true })).engine.endsWith(NET_TXT), 'a property outage reads on');
  const nearLog = logGrid([logRow(SYM, '2026-10-14', 400), logRow(SYM, '2026-10-14', -120)]);
  const hn = holding(shape(NEW, 'pd', { log: nearLog }));
  ok(hn.engine.endsWith(NET_TXT), 'a log ex-date one day off is that dividend: ' + hn.engine.slice(-80));
  const tabOnly = { log: LOG_TAX, divs: [], divTab: divGrid([[SYM, EX_DAY, 10, 'USD']]) };
  const ht = holding(shape(NEW, 'pd', tabOnly)), hto = holding(shape(OLD, 'pd', tabOnly));
  ok(ht.engine.endsWith(NET_TXT) && hto.engine.endsWith(GROSS_TXT), 'a dividend that only Dividend_Events lists is net too: ' + ht.engine.slice(-80));
  const two = logGrid([logRow(SYM, EX_DAY, 400), logRow(SYM, EX_DAY, -60), logRow(SYM, EX_DAY, -60)]);
  eq(holding(shape(NEW, 'pd', { log: two })).engine, holding(shape(NEW, 'pd', { log: LOG_TAX })).engine, 'two tax rows add up');
  const fifteen = logGrid([logRow(SYM, EX_DAY, 400), logRow(SYM, EX_DAY, -60)]);
  ok(holding(shape(NEW, 'pd', { log: fifteen })).engine.endsWith('net of 15% tax (dividend log))'), 'the percent is the recorded one');
  out.push([hn.engine, ht.engine]);
}
function checkPure() {   // invented figures
  const c = makeEnv(NEW, { fetch: NO_NET }).ctx;
  ok(c.dp2V145CoreOk_() === true, 'the self-test core');
  const t = plain(c.dp2DivTaxFromValues_(LOG_TAX));
  deq(Object.keys(t), [SYM], 'only the symbol with a tax row'); near(t[SYM][EX_DAY], 0.7, 1e-12, 'kept share');
  deq(plain(c.dp2DivTaxFromValues_(null)), {}, 'null'); deq(plain(c.dp2DivTaxFromValues_([])), {}, 'empty');
  deq(plain(c.dp2DivTaxFromValues_([['Symbol', 'Ex-Div Date', 'Amount'], [SYM, EX_DAY, 400], [SYM, EX_DAY, -120]])), {}, 'the Dividend_Events header is not the log header');
  const deep = [[], [], [], [], [], [], [], [], LOG_HDR, logRow(SYM, EX_DAY, 400), logRow(SYM, EX_DAY, -120)];
  deq(plain(c.dp2DivTaxFromValues_(deep)), {}, 'a header below the first 8 rows is not searched');
  const txt = plain(c.dp2DivTaxFromValues_(logGrid([[ '', 'AUTO', SYM, '', EX_DAY, '', '', '400.00', '', ''], ['', 'MANUAL', SYM, '', EX_DAY, '', '', '(120.00)', '', ''], ['', 'AUTO', SYM, '', '', '', '', 50, '', ''], ['', 'AUTO', '', '', EX_DAY, '', '', 50, '', ''], ['', 'AUTO', SYM, '', EX_DAY, '', '', 'n/a', '', '']])));
  near(txt[SYM][EX_DAY], 0.7, 1e-12, 'text amounts, blank rows skipped');
  const divs = [{ date: '2026-09-01', amount: 1, source: 'chart' }, { date: EX_DAY, amount: 10, source: 'chart' }];
  const n = plain(c.dp2ExitDivsNet_(divs, { [EX_DAY]: 0.7 }, BUY_DAY));
  deq(n[0], divs[0], 'a dividend with no tax row is untouched'); near(n[1].amount, 7, 1e-12, 'net amount'); eq(n[1].gross, 10); near(n[1].taxPct, 30, 1e-9); eq(n[1].source, 'chart');
  eq(divs[1].amount, 10, 'the input is not changed');
  for (const [tax, entry, why] of [[null, BUY_DAY, 'no tax'], [{}, BUY_DAY, 'empty tax'], [{ [EX_DAY]: 0.7 }, '', 'no entry date'], [{ [EX_DAY]: 0.7 }, EX_DAY, 'bought on the ex-date'],
                                   [{ [EX_DAY]: 0.7 }, '2026-10-20', 'bought after it'], [{ [EX_DAY]: 0.2 }, BUY_DAY, 'implausible'], [{ [EX_DAY]: 1 }, BUY_DAY, 'nothing withheld'], [{ '2026-10-17': 0.7 }, BUY_DAY, 'four days off']]) {
    ok(c.dp2ExitDivsNet_(divs, tax, entry) === divs, why + ': the input itself');
  }
  ok(c.dp2ExitDivsNet_(null, { [EX_DAY]: 0.7 }, BUY_DAY) === null && plain(c.dp2ExitDivsNet_([], { [EX_DAY]: 0.7 }, BUY_DAY)).length === 0, 'no dividends');
  near(plain(c.dp2ExitDivsNet_(divs, { '2026-10-16': 0.7 }, BUY_DAY))[1].amount, 7, 1e-12, 'three days off is that dividend');
  // Codex P2 on #1014: two distributions a few days apart each take their own record, whatever the order of the log
  const pair = [{ date: EX_DAY, amount: 10, source: 'chart' }, { date: '2026-10-16', amount: 2, source: 'chart' }];
  for (const tax of [{ [EX_DAY]: 0.7, '2026-10-16': 0.85 }, { '2026-10-16': 0.85, [EX_DAY]: 0.7 }]) {
    const p2 = plain(c.dp2ExitDivsNet_(pair, tax, BUY_DAY));
    near(p2[0].amount, 7, 1e-12, 'the first takes its own share'); near(p2[1].amount, 1.7, 1e-12, 'the second takes its own share, not the first one listed');
  }
  for (const tax of [{ '2026-10-12': 0.9, '2026-10-15': 0.8 }, { '2026-10-15': 0.8, '2026-10-12': 0.9 }]) {
    near(plain(c.dp2ExitDivsNet_(divs, tax, '2026-10-01'))[1].amount, 9, 1e-12, 'no record on the ex-date: the nearest one');
  }
  for (const tax of [{ '2026-10-12': 0.9, '2026-10-14': 0.8 }, { '2026-10-14': 0.8, '2026-10-12': 0.9 }]) {
    near(plain(c.dp2ExitDivsNet_(divs, tax, '2026-10-01'))[1].amount, 9, 1e-12, 'two equally near: the earlier date, in either order');
  }
  near(plain(c.dp2ExitDivsNet_(divs, { '2026-10-12': 0.2, '2026-10-15': 0.8 }, '2026-10-01'))[1].amount, 8, 1e-12, 'an implausible nearer record is passed over');
  eq(c.dp2ExitDivTxt_({ date: EX_DAY, amount: 7, gross: 10, taxPct: 30 }), '10.00 ex ' + EX_DAY + ', net of 30% tax (dividend log)');
  eq(c.dp2ExitDivTxt_({ date: EX_DAY, amount: 10 }), '10.00 ex ' + EX_DAY);
  const adj = plain(c.dp2ExitDivAdjust_(PRE.concat(POST), 50, BUY_DAY, n, '2026-10-30'));
  eq(adj.applied.length, 2); ok(!('gross' in adj.applied[0]) && adj.applied[1].gross === 10 && Math.abs(adj.applied[1].taxPct - 30) < 1e-9, 'applied carries the tax only where there is one');
  for (const [v, want] of [[undefined, true], ['', true], ['on', true], ['1', true], ['off', false], [' OFF ', false], ['0', false], ['false', false], ['No', false]]) {
    eq(makeEnv(NEW, { fetch: NO_NET, props: v === undefined ? {} : { DP2_DIV_NET: v } }).ctx.dp2DivNetOn_(), want, 'DP2_DIV_NET=' + JSON.stringify(v));
  }
  eq(makeEnv(NEW, { fetch: NO_NET, propsThrow: true }).ctx.dp2DivNetOn_(), true, 'a property outage reads on');
  const st = (src, props) => plain(makeEnv(src, { fetch: NO_NET, props: props || {} }).ctx.dp2SelfTest());
  const sn = st(NEW), so = st(OLD), mine = sn.checks.filter(x => /^v1\.4\.5 core/.test(x.name));
  deq(mine, [{ name: 'v1.4.5 core: a dividend net of the tax recorded for it (DP2_DIV_NET: on)', ok: true }], 'one self-test line');
  deq(sn.checks.filter(x => !/^v1\.4\.5 core/.test(x.name)), so.checks, 'every other self-test line is the 1.4.4 one');
  eq(st(NEW, NET_OFF).checks.filter(x => /^v1\.4\.5 core/.test(x.name))[0].name, 'v1.4.5 core: a dividend net of the tax recorded for it (DP2_DIV_NET: off)', 'the line names the off state');
}
const MUTANTS = [
  ['the gate never off', "function dp2DivNetOn_() { return !/^(0|off|false|no)$/i.test(", "function dp2DivNetOn_() { return !/^(never)$/i.test("],
  ['the gate not asked', "    if (!dp2DivNetOn_()) return {};", "    if (false) return {};"],
  ['the tax rows not netted', "    sums[k].net += amt;", "    sums[k].net += Math.abs(amt);"],
  ['tax rows counted as dividends', "    if (amt > 0) sums[k].gross += amt;", "    sums[k].gross += Math.abs(amt);"],
  ['an implausible share applied', "!(keep >= DP2_DIV_KEEP_MIN) || !(keep < DP2_DIV_KEEP_MAX)) continue;", "false) continue;"],
  ['an implausible share scaled', "call(tax, k) || !(tax[k] >= DP2_DIV_KEEP_MIN) || !(tax[k] < DP2_DIV_KEEP_MAX)) continue;", "call(tax, k)) continue;"],
  ['the first record listed wins', "        if (bestGap < 0 || gap < bestGap || (gap === bestGap && k < best)) { best = k; bestGap = gap; }", "        if (bestGap < 0) { best = k; bestGap = gap; }"],
  ['a tie follows the log order', "(gap === bestGap && k < best)) { best = k; bestGap = gap; }", "false) { best = k; bestGap = gap; }"],
  ['a dividend before the purchase netted', "    if (d && d.date && d.amount > 0 && d.date > entryDate) {", "    if (d && d.date && d.amount > 0) {"],
  ['any log date matches', "        if (!(gap <= DP2_EXIT_DIV_NEAR_D * 86400000)) continue;", "        if (!(gap <= 30 * 86400000)) continue;"],
  ['the amount not scaled', "amount: d.amount * keep, source: d.source, gross: d.amount", "amount: d.amount, source: d.source, gross: d.amount"],
  ['the engine not handed the net list', "dp2ExitDivsNet_(dp2ExitDivsFor_(xb, l.symbol, divTab, xToday), divTax ? divTax[dp2Str_(l.symbol).trim().toUpperCase()] : null, entryDate), xToday);", "dp2ExitDivsFor_(xb, l.symbol, divTab, xToday), xToday);"],
  ['the log never read', "divTax = xSan ? dp2DivTax_() : null, xToday", "divTax = null, xToday"],
  ['the tax not named', " + (a.taxPct ? ', net of ' + dp2PctPlain_(a.taxPct, 0) + ' tax (dividend log)' : '');", ";"],
  ['the whole dividend not named', "  return dp2Fmt_(a.taxPct ? a.gross : a.amount, 2) + ' ex '", "  return dp2Fmt_(a.amount, 2) + ' ex '"],
  ['the tax dropped from the applied list', "    if (d.taxPct) { ap.gross = d.gross; ap.taxPct = d.taxPct; }", "    if (false) { ap.gross = d.gross; ap.taxPct = d.taxPct; }"]
];
function signature(src) {
  const h = extra => { const x = holding(shape(src, 'pd', extra)); return [x.dyn, x.engine, x.signal]; };
  const fifteen = logGrid([logRow(SYM, EX_DAY, 400), logRow(SYM, EX_DAY, -60)]);
  const c = makeEnv(src, { fetch: NO_NET }).ctx, one = [{ date: EX_DAY, amount: 10, source: 'chart' }];
  const pair = [{ date: EX_DAY, amount: 10, source: 'chart' }, { date: '2026-10-16', amount: 2, source: 'chart' }];
  const pick = [plain(c.dp2ExitDivsNet_(pair, { [EX_DAY]: 0.7, '2026-10-16': 0.85 }, BUY_DAY)), plain(c.dp2ExitDivsNet_(one, { '2026-10-12': 0.9, '2026-10-15': 0.8 }, '2026-10-01')),
                plain(c.dp2ExitDivsNet_(one, { '2026-10-14': 0.8, '2026-10-12': 0.9 }, '2026-10-01')), plain(c.dp2ExitDivsNet_(one, { '2026-10-12': 0.2, '2026-10-15': 0.8 }, '2026-10-01'))];
  const pure = [pick, plain(c.dp2DivTaxFromValues_(logGrid([logRow(SYM, EX_DAY, 400), logRow(SYM, EX_DAY, -320)]))), plain(c.dp2DivTaxFromValues_(logGrid([logRow(SYM, EX_DAY, 400), logRow(SYM, EX_DAY, -1)]))),
                plain(c.dp2ExitDivsNet_(one, { [EX_DAY]: 0.2 }, BUY_DAY)), plain(c.dp2ExitDivsNet_(one, { [EX_DAY]: 0.999 }, BUY_DAY))];
  return sha256(JSON.stringify([pure, h({ log: LOG_TAX }), h({ log: LOG_TAX, props: NET_OFF }), h({ log: LOG_GROSS }), h({ log: fifteen }),
    h({ log: logGrid([[ '', 'AUTO', SYM, EX_DAY, EX_DAY, 10, 40, 400, 'USD', ''], ['', 'MANUAL', SYM, EX_DAY, EX_DAY, -3, 40, -120, 'USD', '']]), buy: EX_DAY, px: 44 }),
    h({ log: logGrid([logRow(SYM, EX_DAY, 400), logRow(SYM, EX_DAY, -320)]) }), h({ log: logGrid([logRow(SYM, '2026-10-20', 400), logRow(SYM, '2026-10-20', -120)]) })]));
}
function battery() {
  passed = 0; failed = 0; const out = [];
  test('S0 oracle: the reversal of the v1.4.5 hunks is the v1.4.4 file (SHA-256 pinned, history chain); versions; both compile; +6 functions; ES5 / ASCII', () => checkOracle());
  test('S1 GOLDEN NEGATIVE: DP2_DIV_NET off (five spellings), no log, no tax row, another symbol, no header, no dividend -> the v1.4.4 pages', () => checkGolden());
  test('S2 net: the engine cell names the dividend and its tax; the stop is the net replay; only the holding row changes; the Top 10 page is untouched', () => checkNet(out));
  test('S3 scope: bought on the ex-date, implausible shares, a far ex-date, a property outage -> the 1.4.4 page; one day off, Dividend_Events only, two tax rows -> net', () => checkScope(out));
  test('S4 the pure helpers, the gate spellings and the self-test line', () => checkPure());
  test('S5 mutants: ' + MUTANTS.length + ' one-line mutations are each caught', () => {
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
