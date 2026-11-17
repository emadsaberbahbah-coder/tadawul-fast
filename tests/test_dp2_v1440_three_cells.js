#!/usr/bin/env node
/* tests/test_dp2_v1440_three_cells.js
 * Harness for apps_script/27_Decision_Pages_v2.gs v1.4.4 [PLAN-ACCEPT / TOTAL-RETURN / EVIDENCE-RECENT] (owner, Tue 17 Nov
 * 2026 09:44 Riyadh: "be sure that correct information is provided ... avoid misleading and inconsistent recommendation
 * delivery"). Three cells: (a) a broker stop a few cents under the stop of record read 'raise' while the plan asked for
 * nothing; (b) a lot that was paid a dividend showed its price loss only; (c) the all-time Model evidence tile could not
 * show why it falls. Shims copied from tests/test_dp2_v1430_stop_of_record.js. The v1.4.3 file (OLD, SHA-256 pinned
 * 58a3abca = origin/main 7a80c83) is rebuilt by reversing the enumerated v1.4.4 hunks (tests/js/dp2_edits_v1440.json).
 * Every row, price and date is synthetic.
 *   S0  oracle: reversal = v1.4.3; versions; both compile; +6 functions, 0 removed; ES5 / ASCII of the new lines
 *   S1  GOLDEN NEGATIVE: DP2_EVIDENCE_RECENT_DAYS off -> both pages equal the v1.4.3 build; with the default (30) only the
 *       Model evidence tile differs
 *   S2  plan accept: a HOLD row that asks for no stop change + a broker stop under the stop of record -> 'accepted by the
 *       plan (p% under the stop of record)', no arrow (1.4.3 showed the arrow); a raise / verify row keeps 1.4.3's words
 *   S3  total return: a lot with Dividends Recv adds its total return to the name cell; only that cell changes
 *   S4  evidence recent: the tile adds the INVESTABLE records whose target date lies in the window; 7, off, no column
 *   S5  the pure helpers and the self-test core
 *   S6  mutants
 * The battery runs three times and the digests must match; the last line is 'PASS k/k' (exit 1 on any failure).
 */
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const vm = require('vm');
const ROOT = path.join(__dirname, '..');
const NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '27_Decision_Pages_v2.gs');
// 2026-11-17: v1.4.5 [DIV-NET] is delivered; this harness proves the v1.4.4 diff, so it reads the v1.4.4 file rebuilt from
// the delivered one (tests/js/dp2_history.js: the v1.4.5 hunks reversed, SHA-256 pinned).
const NEW = require(path.join(__dirname, 'js', 'dp2_history.js')).v1440Source(fs.readFileSync(NEW_PATH, 'utf8'));
const FIX = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'fixtures', 'dp2_board_2026-11-02.json'), 'utf8'));
const EDITS = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'dp2_edits_v1440.json'), 'utf8'));
const HISTORY = require(path.join(ROOT, 'tests', 'js', 'dp2_history.js'));
const V1430_SHA256 = '58a3abcada56fa8f700ccbfa5e1c38496baffd740cdba1a0552a13e6917614cd';   // v1.4.3 (origin/main 7a80c83)
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
  for (const v of ['1.4.4', '1.4.3', '1.4.2', '1.4.1', '1.4.0', '1.3.1', '1.3.0', '1.2.36', '1.2.35', '1.2.34', '1.2.33', '1.2.32', '1.2.31']) s = s.split('Decision Pages v2 ' + v).join('Decision Pages v2 1.2.30');
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
    eq(s.split(cur).length - 1, 1, 'exactly one approved v1.4.4 hunk: ' + cur.split('\n')[0].slice(0, 90));
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
const OFF = { DP2_EVIDENCE_RECENT_DAYS: 'off' };
const STOP_CELL = '28:12', NAME_CELL = '28:2';   // the FRST.US 'Dyn stop' and name cells of the fixture's holdings table
const cellOf = (b, k) => { const x = sheet(b, PDV)._cells.get(k || STOP_CELL); return x ? String(x.v) : ''; };
const strip = (st, k) => { const x = clone(st); x.cells = x.cells.filter(c => c.split('=')[0] !== k); return x; };
const rowsWith = (todo, stop) => FINAL_ROWS.map(r => r[0] === 'FRST.US' ? ['FRST.US', 'HOLD', todo, 174, '', stop] : r);
const tileOf = b => { const hit = cellsWith(sheet(b, T10), /INVESTABLE hit rate, matured records/); return hit.length === 1 ? hit[0][1] : JSON.stringify(hit); };
const TILE_HEAD = 'INVESTABLE hit rate, matured records';
// a dated Performance_Log: [outcome, investability, target date (Riyadh)]; NOW is 2026-11-02 09:24 Riyadh
function perfDated(rows, noTarget) {
  const g = [['Record ID', 'Status', 'Outcome', 'Entry Investability', 'Entry Recommendation', 'Horizon'].concat(noTarget ? [] : ['Target Date (Riyadh)'])];
  rows.forEach((x, i) => g.push(['PL-' + (i + 1), x[3] || 'matured', x[0], x[1], 'BUY', '1W'].concat(noTarget ? [] : [x[2]])));
  return g;
}
const PERF = [
  ['WIN', 'INVESTABLE', '2026-11-01 09:00:00'], ['LOSS', 'INVESTABLE', '2026-10-28 09:00:00'], ['LOSS', 'INVESTABLE', '2026-10-20 12:00:00'],
  ['WIN', 'INVESTABLE', '2026-10-05 09:00:00'], ['LOSS', 'INVESTABLE', '2026-10-03 10:00:00'],        // 5 in the 30 days: 2 wins
  ['WIN', 'INVESTABLE', '2026-10-03 09:00:00'], ['WIN', 'INVESTABLE', '2026-09-01 09:00:00'], ['WIN', 'INVESTABLE', ''],   // before the window, or undated
  ['WIN', 'WATCHLIST', '2026-11-01 09:00:00'], ['WIN', 'INVESTABLE', '2026-11-01 09:00:00', 'open'], ['PENDING', 'INVESTABLE', '2026-11-01 09:00:00']   // not INVESTABLE / not matured / not scored
];
function ledgerWith(div, total) {
  const l = clone(FIX.ledger), H = l.headers;
  const row = l.lots.find(x => x[0] === 'FRST.US');
  row[H.indexOf('Dividends Recv')] = div; row[H.indexOf('Total Return SAR')] = total;
  return l;
}
function checkOracle() {
  ok(OLD !== null, 'the hunks reverse'); eq(sha256(OLD), V1430_SHA256, 'v1.4.3 SHA-256');
  eq(HISTORY.V1430_SHA256, V1430_SHA256, 'history pin'); eq(sha256(HISTORY.v1430Source(NEW)), V1430_SHA256, 'history chain');
  eq(sha256(HISTORY.v1420Source(NEW)), HISTORY.V1420_SHA256, 'the older chain still lands on v1.4.2');
  eq(EDITS.length, 11, 'eleven hunks');
  ok(/ \* Version: 1\.4\.4 \(2026-11-17\)/.test(NEW) && /var DP2_VERSION = '1\.4\.4';/.test(NEW), 'version lockstep');
  ok(NEW.indexOf(' *   1.4.3: [STOP-OF-RECORD]') > NEW.indexOf(' *   1.4.4: [PLAN-ACCEPT'), 'the new WHY block is first and the 1.4.3 one is kept');
  new vm.Script(NEW, { filename: '27_Decision_Pages_v2.gs' }); new vm.Script(OLD, { filename: 'old.gs' });
  const fns = s => new Set((s.match(/^function\s+([A-Za-z_$][\w$]*)/mg) || []).map(x => x.split(/\s+/)[1]));
  const fo = fns(OLD), fn = fns(NEW);
  deq([...fo].filter(k => !fn.has(k)), [], 'functions removed');
  deq([...fn].filter(k => !fo.has(k)).sort(), ['dp2EvidenceRecentDays_', 'dp2EvidenceRecentParse_', 'dp2EvidenceRecentTxt_', 'dp2FinalHoldsStop_', 'dp2TotalReturnTxt_', 'dp2V144CoreOk_'], 'functions added');
  for (const [cur, prev] of EDITS) {
    const na = s => s.split('').filter(ch => ch.charCodeAt(0) > 127).sort().join('');
    eq(na(cur), na(prev), 'no new non-ASCII character: ' + cur.split('\n')[0].slice(0, 70));
    const added = cur.split('\n').filter(l => prev.split('\n').indexOf(l) < 0);
    const code = added.filter(l => !/^\s*(\*|\/\*|\/\/)/.test(l)).map(l => l.replace(/\/\/.*$/, '')).join('\n');
    ok(!/=>|`|\blet\s|\bconst\s|\bclass\s/.test(code), 'ES5: ' + code.slice(0, 90));
  }
}
function checkGolden() {
  const fin = finalGrid(FINAL_ROWS);
  for (const which of ['pd', 't10']) {
    for (const bname of ['today', 'research']) {
      for (const [opts, tag] of [[{ final: fin }, 'final rows'], [{}, 'no Final_Decisions tab'], [{ final: fin, props: { DP2_FINAL: 'off' } }, 'DP2_FINAL off'], [{ final: fin, perf: perfDated(PERF) }, 'a dated log']]) {
        const props = Object.assign({}, OFF, opts.props || {});
        deq(norm(buildWith(NEW, which, Object.assign({ board: BOARDS[bname] }, opts, { props }))),
            norm(buildWith(OLD, which, Object.assign({ board: BOARDS[bname] }, opts, { props }))), 'recent days off: ' + which + ' ' + bname + ' ' + tag);
      }
    }
  }
  for (const off of ['0', ' OFF ', 'false', 'No']) {
    deq(norm(buildWith(NEW, 't10', { final: fin, perf: perfDated(PERF), props: { DP2_EVIDENCE_RECENT_DAYS: off } })), norm(buildWith(OLD, 't10', { final: fin, perf: perfDated(PERF) })), 'spelling ' + JSON.stringify(off));
  }
  // the default (property absent): the portfolio page is the 1.4.3 page, and on the Top 10 page only the tile differs
  deq(norm(buildWith(NEW, 'pd', { final: fin })), norm(buildWith(OLD, 'pd', { final: fin })), 'default: the portfolio page');
  const n = buildWith(NEW, 't10', { final: fin, perf: perfDated(PERF) }), o = buildWith(OLD, 't10', { final: fin, perf: perfDated(PERF) });
  const tn = tileOf(n), to = tileOf(o);
  eq(to, TILE_HEAD, 'GOLDEN NEGATIVE: the 1.4.3 tile'); ok(tn.indexOf(TILE_HEAD + ' ') === 0 && tn !== to, 'the tile differs: ' + tn);
  const blank = st => { const x = clone(st); x.cells = x.cells.map(c => c.split(tn).join(to)); return x; };
  deq(norm({ x: blank(n.sheets[T10]) }), norm({ x: o.sheets[T10] }), 'default: every other cell of the Top 10 page');
}
function checkAccept(out) {
  const hold = finalGrid(rowsWith('Hold; stop 14.60.', 14.6));
  const n = buildWith(NEW, 'pd', { final: hold, props: OFF }), o = buildWith(OLD, 'pd', { final: hold, props: OFF });
  const cn = cellOf(n), co = cellOf(o);
  ok(/^14\.60 \([^)]*\) stop of record\nbroker 14\.56 — accepted by the plan \(0\.3% under the stop of record\)/.test(cn), 'accepted: ' + JSON.stringify(cn));
  ok(cn.indexOf('↑') < 0 && cn.indexOf('raise') < 0, 'no arrow, no raise');
  ok(/\nbroker 14\.56 ↑ raise to 14\.60/.test(co), 'GOLDEN NEGATIVE: 1.4.3 told the owner to raise: ' + JSON.stringify(co));
  deq(norm({ x: strip(n.sheets[PDV], STOP_CELL) }), norm({ x: strip(o.sheets[PDV], STOP_CELL) }), 'every other cell of the page');
  eq(actionLines(sheet(n, PDV)).filter(l => /FRST\.US/.test(l)).length, actionLines(sheet(o, PDV)).filter(l => /FRST\.US/.test(l)).length, 'no action added or removed');
  const quiet = "Hold; the stop working at IBKR (14.56) already covers the plan's 14.60 on all 174 shares - nothing to change.";
  ok(/accepted by the plan/.test(cellOf(buildWith(NEW, 'pd', { final: finalGrid(rowsWith(quiet, 14.6)), props: OFF }))), 'the "already covers" form holds too');
  const capNote = 'Hold; stop 14.60. Above the position cap: no new buys of it (a trim is the owner\'s call).';
  ok(/accepted by the plan/.test(cellOf(buildWith(NEW, 'pd', { final: finalGrid(rowsWith(capNote, 14.6)), props: OFF }))), 'a cap note does not turn it into a raise');
  // rows that do ask for something keep the 1.4.3 words
  for (const todo of ['Hold; raise the stop to 14.60 for all 174 shares.', 'Hold. Verify at IBKR before any change: the quantity differs.', 'Hold; place a stop at 14.60 for all 174 shares.']) {
    const fg = finalGrid(rowsWith(todo, 14.6));
    eq(cellOf(buildWith(NEW, 'pd', { final: fg, props: OFF })), cellOf(buildWith(OLD, 'pd', { final: fg, props: OFF })), 'unchanged: ' + todo);
  }
  const part = clone(FIX.ibkr_capture);
  for (const od of part.json.orders.orders) if (/STP/.test(od.order_type)) { od.total_shares_qty = 40; od.remaining_shares_qty = 40; od.primary_description = 'SELL 40 FRST STP 14.56'; }
  const pc = cellOf(buildWith(NEW, 'pd', { final: hold, capture: part, props: OFF }));
  eq(pc, cellOf(buildWith(OLD, 'pd', { final: hold, capture: part, props: OFF })), 'a stop on part of the position is never accepted');
  ok(/on 40 of 174 sh only, below the stop of record/.test(pc), 'partial: ' + JSON.stringify(pc));
  const above = finalGrid(rowsWith('Hold; stop 14.00.', 14));
  eq(cellOf(buildWith(NEW, 'pd', { final: above, props: OFF })), cellOf(buildWith(OLD, 'pd', { final: above, props: OFF })), 'a broker stop at or above the stop of record: in place, as before');
  if (out) out.push(cn, pc);
}
function checkTotal(out) {
  const led = ledgerWith(50, 520.3), fin = finalGrid(FINAL_ROWS);
  for (const [opts, tag] of [[{ final: fin }, 'final on'], [{}, 'no final rows']]) {
    const n = buildWith(NEW, 'pd', Object.assign({ ledger: led, props: OFF }, opts)), o = buildWith(OLD, 'pd', Object.assign({ ledger: led, props: OFF }, opts));
    const cn = cellOf(n, NAME_CELL), co = cellOf(o, NAME_CELL);
    eq(cn, co + ' · total return +5.2% with dividends 50.00 USD (ledger)', tag + ': the name cell');
    deq(norm({ x: strip(n.sheets[PDV], NAME_CELL) }), norm({ x: strip(o.sheets[PDV], NAME_CELL) }), tag + ': every other cell');
    if (out) out.push(cn);
  }
  const neg = cellOf(buildWith(NEW, 'pd', { ledger: ledgerWith(50, -260.15), props: OFF }), NAME_CELL);
  ok(/ · total return −2\.6% with dividends 50\.00 USD \(ledger\)$/.test(neg), 'a negative total return: ' + neg);
  for (const [div, total, why] of [[0, 520.3, 'no dividend'], ['', 520.3, 'a blank dividend cell'], [50, '', 'no total return']]) {
    const l2 = ledgerWith(div, total);
    eq(cellOf(buildWith(NEW, 'pd', { ledger: l2, props: OFF }), NAME_CELL), cellOf(buildWith(OLD, 'pd', { ledger: l2, props: OFF }), NAME_CELL), why + ': the 1.4.3 cell');
  }
  if (out) out.push(neg);
}
function checkRecent(out) {
  const fin = finalGrid(FINAL_ROWS);
  const t = props => tileOf(buildWith(NEW, 't10', { final: fin, perf: perfDated(PERF), props }));
  eq(t({}), TILE_HEAD + ' · targets of the last 30 days: 40.0% (n 5)', 'default 30 days');
  eq(t({ DP2_EVIDENCE_RECENT_DAYS: '30' }), t({}), 'explicit 30');
  eq(t({ DP2_EVIDENCE_RECENT_DAYS: 'junk' }), t({}), 'junk reads the default');
  eq(t({ DP2_EVIDENCE_RECENT_DAYS: '7' }), TILE_HEAD + ' · targets of the last 7 days: 50.0% (n 2)', '7 days');
  eq(t({ DP2_EVIDENCE_RECENT_DAYS: '1' }), TILE_HEAD + ' · targets of the last 1 days: none matured', 'an empty window');
  eq(t({ DP2_EVIDENCE_RECENT_DAYS: '365' }), TILE_HEAD + ' · targets of the last 365 days: 57.1% (n 7)', '365 days: the undated record is left out');
  eq(t({ DP2_EVIDENCE_RECENT_DAYS: 'off' }), TILE_HEAD, 'off');
  const nocol = tileOf(buildWith(NEW, 't10', { final: fin, perf: perfDated(PERF, true) }));
  eq(nocol, TILE_HEAD + ' · targets of the last 30 days: n/a (no Target Date column)', 'no Target Date column');
  const both = t({ DP2_EVIDENCE_SINCE: '2026-10-01' });
  ok(/^INVESTABLE hit rate, matured records · since 2026-10-01: .* · targets of the last 30 days: 40\.0% \(n 5\)$/.test(both), 'with the since text: ' + both);
  const thrown = tileOf(buildWith(NEW, 't10', { final: fin, perf: perfDated(PERF), propsThrow: true }));
  eq(thrown, t({}), 'a property outage reads the default');
  const env = makeEnv(NEW, { fetch: NO_NET, perf: perfDated(PERF) }); env.ctx.buildTop10V2();
  eq(env.reads.DP2_EVIDENCE_RECENT_DAYS, 1, 'one property read per execution');
  if (out) out.push(t({}), t({ DP2_EVIDENCE_RECENT_DAYS: '7' }), nocol, both);
}
function checkPure() {   // invented figures
  const c = makeEnv(NEW, { fetch: NO_NET }).ctx;
  ok(c.dp2V144CoreOk_() === true, 'the self-test core');
  const st = (src, props) => plain(makeEnv(src, { fetch: NO_NET, props: props || {} }).ctx.dp2SelfTest());
  const sn = st(NEW), so = st(OLD), mine = sn.checks.filter(x => /^v1\.4\.4 cores/.test(x.name));
  deq(mine, [{ name: 'v1.4.4 cores: plan accept, total return, evidence recent (DP2_EVIDENCE_RECENT_DAYS: 30 d)', ok: true }], 'one self-test line');
  deq(sn.checks.filter(x => !/^v1\.4\.4 cores/.test(x.name)), so.checks, 'every other self-test line is the 1.4.3 one');
  eq(st(NEW, OFF).checks.filter(x => /^v1\.4\.4 cores/.test(x.name))[0].name, 'v1.4.4 cores: plan accept, total return, evidence recent (DP2_EVIDENCE_RECENT_DAYS: off)', 'the line names the off state');
  const L2 = (...a) => c.dp2StopOfRecordText_(...a).split('\n')[1];
  eq(L2(9.5, 10, 9.47, true, null, null, { covered: 100, shares: 100, planHolds: true }), 'broker 9.47 — accepted by the plan (0.3% under the stop of record)');
  eq(L2(9.5, 10, 9.47, true, null, null, { covered: null, shares: 100, planHolds: true }), 'broker 9.47 — accepted by the plan (0.3% under the stop of record)', 'coverage unknown');
  eq(L2(9.5, 10, 9.47, true, null, null, { covered: 100, shares: 100, planHolds: false }), 'broker 9.47 ↑ raise to 9.50');
  eq(L2(9.5, 10, 9.47, true, null, null, { covered: 100, shares: 100 }), 'broker 9.47 ↑ raise to 9.50', 'no planHolds: 1.4.3');
  eq(L2(9.5, 10, 9.47, true, null, null, { covered: 100, shares: 100, planHolds: true, verifyOnly: true }), 'broker 9.47 below the stop of record — verify first', 'verify wins');
  eq(L2(9.5, 10, 9.47, true, null, null, { covered: 40, shares: 100, planHolds: true }), 'broker 9.47 on 40 of 100 sh only, below the stop of record', 'partial wins');
  eq(L2(9.5, 10, 9.6, true, null, null, { covered: 100, shares: 100, planHolds: true }), 'broker 9.60 — in place');
  eq(L2(9.5, 10, null, true, null, null, { planHolds: true }), 'no broker stop — place 9.50', 'a missing stop is never accepted');
  const h = c.dp2FinalHoldsStop_;
  ok(h({ decision: 'HOLD', todo: 'Hold; stop 9.50.' }) && h({ decision: 'HOLD', todo: 'Hold; the Exit Plan has no rule for this holding.' }), 'plain holds');
  ok(!h({ decision: 'HOLD', todo: 'Hold; RAISE the stop to 9.50.' }) && !h({ decision: 'HOLD', todo: 'Hold; place a stop at 9.50.' }) && !h({ decision: 'HOLD', todo: 'Verify A.US at IBKR before acting: x.' }) &&
     !h({ decision: 'BUY', todo: 'Hold; stop 9.50.' }) && !h(null) && !h({}), 'rows that ask for something');
  ok(h({ decision: 'HOLD', todo: 'Hold; stop 9.50. A replacement is on the plan.' }), 'whole words only: "replacement" and "plan" are not "place"');
  const p = c.dp2EvidenceRecentParse_;
  deq([p(''), p(null), p(' 30 '), p('1'), p('365'), p('366'), p('0'), p('off'), p('FALSE'), p('no'), p('-5'), p('1.5'), p('abc')], [30, 30, 30, 1, 365, 30, 0, 0, 0, 0, 30, 30, 30], 'the days reader');
  const tr = c.dp2TotalReturnTxt_;
  eq(tr({ dividends: 12.5, totalReturnSar: 18.75, costBasis: 100, ccy: 'USD' }, 3.75), ' · total return +5.0% with dividends 12.50 USD (ledger)');
  eq(tr({ dividends: 12.5, totalReturnSar: 0, costBasis: 100, ccy: 'SAR' }, 1), ' · total return +0.0% with dividends 12.50 SAR (ledger)', 'a zero total return is a figure');
  deq([tr(null, 1), tr({ dividends: 0, totalReturnSar: 5, costBasis: 100, ccy: 'USD' }, 3.75), tr({ dividends: 5, totalReturnSar: null, costBasis: 100, ccy: 'USD' }, 3.75),
       tr({ dividends: 5, totalReturnSar: 5, costBasis: 0, ccy: 'USD' }, 3.75), tr({ dividends: 5, totalReturnSar: 5, costBasis: 100, ccy: 'USD' }, 0), tr({ dividends: -5, totalReturnSar: 5, costBasis: 100, ccy: 'USD' }, 3.75)], ['', '', '', '', '', ''], 'nothing to say');
}
const MUTANTS = [
  ['accept ignores the plan', "    else if (cov.planHolds === true && !verify) line2 +=", "    else if (!verify) line2 +="],
  ['the plan flag never passed', ", planHolds: dp2FinalHoldsStop_(fc.row) });", " });"],
  ['a raise row reads as a hold', "!/\\b(raise|place|verify)\\b/i.test(dp2Str_(r.todo));", "!/\\b(place|verify)\\b/i.test(dp2Str_(r.todo));"],
  ['a BUY row reads as a hold', "  return !!r && r.decision === 'HOLD' && !/", "  return !!r && !/"],
  ['total return without a dividend', "  if (!l || !(l.dividends > 0) || typeof l.totalReturnSar", "  if (!l || typeof l.totalReturnSar"],
  ['total return over the wrong base', "dp2Pct_(l.totalReturnSar / (l.costBasis * fx) * 100, 1)", "dp2Pct_(l.totalReturnSar / l.costBasis * 100, 1)"],
  ['the name cell not extended', " + dp2TotalReturnTxt_(l, lfx), spark,", ", spark,"],
  ['the window not applied', "dp2RecordedKey_(vals[r][cTgt]) >= rFrom) { rW += win; rN++; }", "dp2RecordedKey_(vals[r][cTgt]) >= '') { rW += win; rN++; }"],
  ['other labels counted in the window', "    if (rd && inv === 'INVESTABLE' && cTgt !== undefined", "    if (rd && cTgt !== undefined"],
  ['off does not disarm', "  if (/^(0|off|false|no)$/.test(t)) return 0;", "  if (/^(never)$/.test(t)) return 0;"],
  ['the tile not extended', " + dp2EvidenceRecentTxt_(hit.recent), color: DP2C.amber }", ", color: DP2C.amber }"]
];
function signature(src) {
  const hold = finalGrid(rowsWith('Hold; stop 14.60.', 14.6)), raise = finalGrid(rowsWith('Hold; raise the stop to 14.60 for all 174 shares.', 14.6));
  const a = buildWith(src, 'pd', { final: hold, props: OFF }), b = buildWith(src, 'pd', { final: raise, props: OFF });
  const buy = finalGrid(FINAL_ROWS.map(r => r[0] === 'FRST.US' ? ['FRST.US', 'BUY', 'Buy 5 more; stop 14.60.', 5, '', 14.6] : r));
  const c = buildWith(src, 'pd', { final: buy, props: OFF });
  const d = buildWith(src, 'pd', { ledger: ledgerWith(50, 520.3), props: OFF }), e = buildWith(src, 'pd', { ledger: ledgerWith(0, 520.3), props: OFF });
  const perf = perfDated(PERF);
  const f = buildWith(src, 't10', { perf }), g = buildWith(src, 't10', { perf, props: OFF }), h = buildWith(src, 't10', { perf, props: { DP2_EVIDENCE_RECENT_DAYS: '7' } });
  return sha256(JSON.stringify([cellOf(a), cellOf(b), cellOf(c), cellOf(d, NAME_CELL), cellOf(e, NAME_CELL), tileOf(f), tileOf(g), tileOf(h)]));
}
function battery() {
  passed = 0; failed = 0; const out = [];
  test('S0 oracle: the reversal of the v1.4.4 hunks is the v1.4.3 file (SHA-256 pinned, history chain); versions; both compile; +6 functions; ES5 / ASCII', () => checkOracle());
  test('S1 GOLDEN NEGATIVE: DP2_EVIDENCE_RECENT_DAYS off (five spellings) -> the v1.4.3 pages; by default only the Model evidence tile differs', () => checkGolden());
  test('S2 plan accept: a HOLD row with a broker stop just under the stop of record reads "accepted by the plan" (1.4.3 said raise); raise, verify, place and partial rows keep their words', () => checkAccept(out));
  test('S3 total return: a lot with Dividends Recv adds its total return to the name cell; only that cell changes; no dividend or no total = the 1.4.3 cell', () => checkTotal(out));
  test('S4 evidence recent: the tile adds the INVESTABLE records of the window (30 default, 7, 1, 365, off, no column, with the since text); one property read', () => checkRecent(out));
  test('S5 the pure helpers and the self-test core', () => checkPure());
  test('S6 mutants: ' + MUTANTS.length + ' one-line mutations are each caught', () => {
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
