#!/usr/bin/env node
/* tests/test_dp2_v1233_exit_sanity.js
 * Harness for apps_script/27_Decision_Pages_v2.gs v1.2.33 [EXIT-SANITY] (owner portfolio, Tue 3 Nov 2026 16:15 Riyadh,
 * dp2ExitStatus: 'SUPN.US . SELL NOW -- stop 38.32 hit . last 36.28 . dyn stop 38.32 . high 41.92'): the INDV shares
 * moved to the new contract SUPN while the chart key SUPN still carried the OLD Supernus Pharmaceuticals line (41.92),
 * and the same shares went ex-dividend 8.13 USD on 3 Nov. Script Property DP2_EXIT_SANITY = 1 / on / true / yes
 * (default OFF): (a) a history that does not hold the lot's entry price on its entry day gives no exit signal and no
 * alert, (b) cash dividends are back-adjusted so an ex-dividend gap is not a stop hit.
 *
 * Two trees over the REAL file, each in its own vm context: the delivered source (NEW) and the v1.2.32 file (OLD,
 * SHA-256 pinned 5ac92c36 = origin/main 3e326e26), by default rebuilt from NEW by reversing the enumerated v1.2.33
 * hunks (tests/js/dp2_edits_v1233.json); a second argument names the v1.2.32 file instead. The GAS shim, the fixture
 * (tests/fixtures/dp2_board_2026-11-02.json) and the board variants are those of tests/test_dp2_v1232_preview_label.js,
 * copied verbatim; makeEnv gains four options (ledger, my_portfolio, a chart-API stub and a Dividend_Events grid).
 * Every holding, amount and price below is synthetic except the 3 Nov SUPN shape (95 sh, entry 35.45 on 2 Nov, the
 * old line's 41.92, the held shares' 36.28 and 30.00, the 8.13 USD dividend ex 3 Nov) taken from the live pages.
 * Sections:
 *   P0  oracle (SHA-256), version lockstep, both files compile; the base lacks the new names
 *   P1  GOLDEN NEGATIVE: the gate absent / blank / off / junk / a property fault -> both pages, every cell, format,
 *       merge, chart, the call log, the e-mails, the chart-API requests and the return values equal the v1.2.32 build
 *       (three fixture boards, and the three SUPN shapes below)
 *   P2  the 3 Nov shape, gate on: SUPN.US reads 'HOLD -- exit engine n/a', its engine cell names the bar and the entry,
 *       one CHECK action, no SELL NOW action, no alert e-mail for it; the base reads SELL NOW and e-mails it
 *   P3  the ex-dividend day with the chart's dividend event, gate on: not a stop hit, the engine cell names the
 *       adjustment, the dynamic stop is the adjusted replay's; the request carries events=div; the base hits the stop
 *   P4  the ex-dividend day before its bar, no chart event: the Dividend_Events row of the same symbol adjusts; a row
 *       in another currency, a future ex-date or a dividend the chart already lists does not (no double count)
 *   P5  the pure helpers: dp2ExitAnchorMiss_, dp2ExitDivAdjust_, dp2DivTabFromValues_, dp2ExitDivsFor_, dp2YahooDivs_
 *   P6  the gate: spellings, a fault reads off, one property read per execution
 *   P7  dp2SelfTest: the v1.2.32 report while off; one 'exit sanity .. core ok' line while on
 *   P8  ES5 / ASCII lint of the v1.2.33 diff; inventory (10 functions + 8 vars added, 0 removed); the 3 replaced base
 *       lines pinned; 10 hunks whose reversal is the v1.2.32 file; WHY block; every prior WHY kept
 *   P9  mutants: each mutation of the delivered file fails at least one section check
 * The battery runs twice and the two digests must match; the last line is 'PASS k/k' (exit 1 on any failure).
 * Usage: node tests/test_dp2_v1233_exit_sanity.js [new.gs] [v1.2.32.gs]
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
const EDITS = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'dp2_edits_v1233.json'), 'utf8'));
const V1232_SHA256 = '5ac92c367d86a344ed7beefff33d490d292b82fb68b4ede7fd0c0a9afb18a723';   // v1.2.32 (origin/main 3e326e26)
const NOW = Date.parse(FIX.frozen_now);   // 2026-11-02T06:24:00Z = 09:24 Riyadh
const GATE = 'DP2_EXIT_SANITY';
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
    eq(s.split(cur).length - 1, 1, 'exactly one approved v1.2.33 hunk: ' + cur.split('\n')[0].slice(0, 90));
    s = s.replace(cur, () => prev);
  }
  eq(sha256(s), V1232_SHA256, 'v1.2.33 changes only its enumerated hunks (their reversal = the v1.2.32 file)');
  return s;
}
let OLD = null, oldErr = '';
try {
  OLD = OLD_PATH ? fs.readFileSync(OLD_PATH, 'utf8') : reverseEdits(NEW);
  eq(sha256(OLD), V1232_SHA256, 'v1.2.32 SHA-256');
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
  return JSON.parse(JSON.stringify(strip(b)).split('Decision Pages v2 1.2.33').join('Decision Pages v2 1.2.30').split('Decision Pages v2 1.2.33').join('Decision Pages v2 1.2.30').split('Decision Pages v2 1.2.32').join('Decision Pages v2 1.2.30').split('Decision Pages v2 1.2.31').join('Decision Pages v2 1.2.30'));
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




/* ============================================================ the 3 Nov scenarios */
const T10 = 'Top_10_Investments_v2', PDV = 'Portfolio_Decision_v2';
const BS = String.fromCharCode(92);
const ESC = hex => BS + 'u' + hex;   // an escape as TEXT in the .gs source
const ON = { [GATE]: 'on' };
const DIV_NOW = Date.parse('2026-11-03T15:00:00Z');   // 18:00 Riyadh on the ex-dividend day
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
const FRST_BARS = wbars('2026-09-01', '2026-11-02', i => 15.3 + 0.25 * Math.sin(i / 3));
const OLD_LINE = (() => { const b = wbars('2026-09-01', '2026-11-02', i => 41.3 + 0.3 * Math.sin(i / 2)); b[b.length - 1] = { date: '2026-11-02', o: 41.5, h: 42.3, l: 41.4, c: 41.92 }; return b; })();
const OWN = (() => { const b = wbars('2026-09-01', '2026-11-02', i => 35.6 + 0.35 * Math.sin(i / 2)); b[b.length - 1] = { date: '2026-11-02', o: 35.43, h: 37.01, l: 35.32, c: 36.28 }; return b; })();
const EX_BAR = { date: '2026-11-03', o: 29.6, h: 30.4, l: 29.4, c: 30.0 };
const SUPN_DIV = { date: '2026-11-03', amount: 8.13 };
/* the ledger and My_Portfolio of the fixture plus the SUPN lot (95 sh bought 2 Nov at 35.45; the feed prices 41.92) */
const LEDGER = (() => { const l = clone(FIX.ledger); l.lots.splice(1, 0, ['SUPN.US', 'Supernus Pharmaceuticals, Inc.', 'USD', 'Active', '2026-11-02', 35.45410526, 95, 3368.14, 41.92, 3.75, 14934, 2303, 0, 2303, 0.182, 1]); return l; })();
const MP = (() => { const m = clone(FIX.my_portfolio); const r = m.rows[0].slice(); r[0] = 'SUPN.US'; r[2] = 'Healthcare'; r[3] = 'Drug Manufacturers'; r[5] = 41.92; r[6] = 59.68; r[7] = 37.54; r[17] = 95; r[18] = 35.45; m.rows.splice(1, 0, r); return m; })();
const DIV_HDR = ['Symbol', 'Ex-Div Date', 'Days To ExDiv', 'Amount', 'Currency', 'Pay Date', 'Record Date', 'Period', 'Updated At (Riyadh)', 'Source'];
const divGrid = rows => [DIV_HDR].concat(rows.map(r => [r[0], r[1], 0, r[2], r[3], '2026-11-06', '2026-10-30', 'Other', '2026-11-03 16:45', 'eodhd /div']));
/* the chart API: { KEY: { bars, last, divs, ccy } }; events are returned only when the request asks (events=div) */
function chartStub(series) {
  const urls = [];
  return { urls, fetch(url) {
    urls.push(String(url));
    const m = /\/v8\/finance\/chart\/([^?]+)\?/.exec(String(url));
    const s = m ? series[decodeURIComponent(m[1])] : null;
    if (!s) return { getResponseCode: () => 404, getContentText: () => '{}' };
    const ts = s.bars.map(b => Date.parse(b.date + 'T13:30:00Z') / 1000);
    const res = { meta: { regularMarketPrice: s.last, currency: s.ccy || 'USD', exchangeTimezoneName: 'America/New_York', marketState: 'REGULAR' }, timestamp: ts,
                  indicators: { quote: [{ open: s.bars.map(b => b.o), high: s.bars.map(b => b.h), low: s.bars.map(b => b.l), close: s.bars.map(b => b.c) }] } };
    if (/[?&]events=div/.test(String(url)) && s.divs && s.divs.length) {
      res.events = { dividends: {} };
      for (const d of s.divs) { const t = Date.parse(d.date + 'T13:30:00Z') / 1000; res.events.dividends[String(t)] = { amount: d.amount, date: t }; }
    }
    return { getResponseCode: () => 200, getContentText: () => JSON.stringify({ chart: { result: [res], error: null } }) };
  } };
}
const SHAPES = {
  oldLine: () => ({ now: NOW, series: { FRST: { bars: FRST_BARS, last: 15.0 }, SUPN: { bars: OLD_LINE, last: 36.28 } } }),
  exDay: () => ({ now: DIV_NOW, series: { FRST: { bars: FRST_BARS, last: 15.0 }, SUPN: { bars: OWN.concat([EX_BAR]), last: 30.0, divs: [SUPN_DIV] } } }),
  exBefore: () => ({ now: DIV_NOW, series: { FRST: { bars: FRST_BARS, last: 15.0 }, SUPN: { bars: OWN, last: 30.0 } } })
};
function shapeBuild(source, key, which, extra) {
  extra = extra || {};
  const sh = SHAPES[key]();
  const stub = chartStub(extra.series || sh.series);
  const b = build(source, which || 'pd', { board: BOARDS.today, ledger: LEDGER, mp: MP, fetch: stub, now: sh.now, divTab: extra.divTab, props: extra.props || {}, propsThrow: extra.propsThrow });
  b.urls = stub.urls.slice();
  return b;
}
function holding(b, sym) {
  const sh = sheet(b, PDV);
  const r = firstRow(sh, 1, v => v === sym);
  ok(r > 0, 'holding row ' + sym);
  return { row: r, last: val(sh, r, 7), dyn: String(val(sh, r, 12)), engine: String(val(sh, r, 13)), signal: String(val(sh, r, 16)) };
}
function rowCells(b, r) { return norm(b).sheets[PDV].cells.filter(x => x.split(':')[0] === String(r)); }
function stateRow(b, sym) {
  const sh = sheet(b, '_DP2_Exit_State');
  const r = firstRow(sh, 1, v => v === sym);
  return r > 0 ? { signal: String(val(sh, r, 13)), text: String(val(sh, r, 14)), stop: val(sh, r, 12), hi: val(sh, r, 8) } : null;
}
const mailText = b => JSON.stringify(b.mails);
const ANCHOR_WHY = 'exit engine n/a ' + DASH + ' the price history does not match this lot: the 2026-11-02 bar (low 41.40, high 42.30, close 41.92) is +18.2% from the entry 35.45 (a ticker change, another listing, another unit or a split); check the symbol';
const CHECK_ACT = 'CHECK SUPN.US (95 sh): the price history does not match this lot: the 2026-11-02 bar (low 41.40, high 42.30, close 41.92) is +18.2% from the entry 35.45 (a ticker change, another listing, another unit or a split); check the symbol. No exit signal or alert is given for this lot until its symbol prices the shares the broker holds.';
/* the replay the page must show on the ex-dividend day (the pure helpers of the delivered file) */
function expectedAdjusted(divs, bars) {
  const e = makeEnv(NEW, { props: ON });
  const adj = e.ctx.dp2ExitDivAdjust_(bars, 3368.14 / 95, '2026-11-02', divs, '2026-11-03');
  return { adj, x: e.ctx.dp2ExitCompute_(adj.bars, adj.entry, '2026-11-02', 30.0), fmt: (n, d) => e.ctx.dp2Fmt_(n, d) };
}

/* ------------------------------------------------------------ section checks (also the mutant oracles) */
function checkGoldenNegative(src) {
  for (const props of [{}, { [GATE]: '' }, { [GATE]: 'off' }, { [GATE]: '0' }, { [GATE]: 'enable' }, { [GATE]: 'no' }]) {
    for (const key of ['oldLine', 'exDay', 'exBefore']) {
      const n = shapeBuild(src, key, 'pd', { props, divTab: key === 'exBefore' ? divGrid([['SUPN.US', '2026-11-03', 8.13, 'USD']]) : undefined });
      const o = shapeBuild(OLD, key, 'pd', { props, divTab: key === 'exBefore' ? divGrid([['SUPN.US', '2026-11-03', 8.13, 'USD']]) : undefined });
      deq(norm(n), norm(o), 'gate ' + JSON.stringify(props[GATE]) + ': Portfolio_Decision_v2 = v1.2.32 (' + key + ')');
      deq(n.urls, o.urls, 'gate ' + JSON.stringify(props[GATE]) + ': the same chart requests (' + key + ')');
      ok(n.env.reads[GATE] === 1, 'the gate is read once per execution (' + key + ')');
    }
  }
  const f1 = shapeBuild(src, 'oldLine', 'pd', { propsThrow: true }), f0 = shapeBuild(OLD, 'oldLine', 'pd', { propsThrow: true });
  deq(norm(f1), norm(f0), 'a property fault reads off');
  for (const key of ['today', 'research', 'todayNA']) {
    deq(norm(build(src, 'both', { board: BOARDS[key] })), norm(build(OLD, 'both', { board: BOARDS[key] })), 'fixture board ' + key + ': both pages = v1.2.32');
  }
}
function checkOldLine(src) {
  const n = shapeBuild(src, 'oldLine', 'pd', { props: ON }), o = shapeBuild(OLD, 'oldLine', 'pd', { props: ON });
  const hn = holding(n, 'SUPN.US'), ho = holding(o, 'SUPN.US');
  ok(/^SELL NOW /.test(ho.signal), 'the base reads SELL NOW: ' + ho.signal);
  ok(/SUPN\.US/.test(mailText(o)) && /SELL NOW|STOP HIT/.test(mailText(o)), 'the base e-mails the SUPN.US sell');
  eq(hn.signal, 'HOLD ' + DASH + ' exit engine n/a', 'the 3 Nov shape gives no signal');
  eq(hn.engine, ANCHOR_WHY, 'the engine cell names the bar and the entry');
  const acts = pdView(n).actions;
  eq(acts.filter(a => a === CHECK_ACT).length, 1, 'one CHECK action');
  eq(acts.filter(a => /^SELL( NOW)? SUPN/.test(a)).length, 0, 'no SELL action for SUPN.US');
  ok(!/SUPN/.test(mailText(n)), 'no alert e-mail names SUPN.US');
  const st = stateRow(n, 'SUPN.US');
  ok(st && st.signal === 'ENGINE_NA', 'state row ENGINE_NA: ' + JSON.stringify(st));
  deq(rowCells(n, holding(n, 'FRST.US').row), rowCells(o, holding(o, 'FRST.US').row), 'the FRST.US row is the v1.2.32 row');
  return n;
}
function checkExDay(src) {
  const n = shapeBuild(src, 'exDay', 'pd', { props: ON }), o = shapeBuild(OLD, 'exDay', 'pd', { props: ON });
  const hn = holding(n, 'SUPN.US'), ho = holding(o, 'SUPN.US');
  ok(/^SELL NOW /.test(ho.signal), 'the base reads the ex-dividend gap as a stop hit: ' + ho.signal);
  ok(!/^SELL/.test(hn.signal) && !/STOP HIT/.test(hn.signal), 'not a sell on the ex-dividend day: ' + hn.signal);
  ok(hn.engine.endsWith(' ' + DOT + ' dividend-adjusted (8.13 ex 2026-11-03)'), 'the engine cell names the adjustment: ' + hn.engine.slice(-80));
  const ex = expectedAdjusted([Object.assign({ source: 'chart' }, SUPN_DIV)], OWN.concat([EX_BAR]));
  eq(ex.adj.applied.length, 1, 'one adjustment');
  ok(ex.x && ex.x.last > ex.x.stop, 'the adjusted replay holds');
  ok(hn.dyn.indexOf(ex.fmt(ex.x.stop, 2) + ' (') === 0, 'the dynamic stop is the adjusted replay\'s ' + ex.fmt(ex.x.stop, 2) + ': ' + hn.dyn.split('\n')[0]);
  ok(n.urls.some(u => /\/chart\/SUPN\?/.test(u) && /&events=div$/.test(u)), 'the SUPN request asks for the dividend events');
  ok(o.urls.every(u => !/events=div/.test(u)), 'the base never asks');
  ok(!/SUPN/.test(mailText(n)), 'no alert e-mail names SUPN.US');
  deq(rowCells(n, holding(n, 'FRST.US').row), rowCells(o, holding(o, 'FRST.US').row), 'the FRST.US row is the v1.2.32 row');
  return n;
}
function checkTabFallback(src) {
  const tab = rows => divGrid(rows);
  const n = shapeBuild(src, 'exBefore', 'pd', { props: ON, divTab: tab([['SUPN.US', '2026-11-03', 8.13, 'USD']]) });
  const hn = holding(n, 'SUPN.US');
  ok(!/^SELL/.test(hn.signal), 'the tab dividend keeps the ex-day from reading as a sell: ' + hn.signal);
  ok(hn.engine.endsWith(' ' + DOT + ' dividend-adjusted (8.13 ex 2026-11-03)'), 'adjusted from Dividend_Events: ' + hn.engine.slice(-80));
  const ex = expectedAdjusted([{ date: '2026-11-03', amount: 8.13, source: 'Dividend_Events' }], OWN);
  ok(hn.dyn.indexOf(ex.fmt(ex.x.stop, 2) + ' (') === 0, 'the stop of the tab-adjusted replay ' + ex.fmt(ex.x.stop, 2) + ': ' + hn.dyn.split('\n')[0]);
  for (const [why, rows] of [['another currency', [['SUPN.US', '2026-11-03', 8.13, 'GBP']]], ['a future ex-date', [['SUPN.US', '2026-11-06', 8.13, 'USD']]], ['another symbol', [['INDV.US', '2026-11-03', 8.13, 'USD']]]]) {
    const h = holding(shapeBuild(src, 'exBefore', 'pd', { props: ON, divTab: tab(rows) }), 'SUPN.US');
    ok(/^SELL NOW /.test(h.signal) && h.engine.indexOf('dividend-adjusted') < 0, why + ': no adjustment: ' + h.signal);
  }
  // the chart already lists it (ex 3 Nov) and the tab says 2 Nov: one adjustment, not two
  const dbl = shapeBuild(src, 'exDay', 'pd', { props: ON, divTab: tab([['SUPN.US', '2026-11-02', 8.13, 'USD']]) });
  const now4 = holding(dbl, 'SUPN.US');
  ok(now4.engine.endsWith('dividend-adjusted (8.13 ex 2026-11-03)'), 'the chart dividend only (no double count): ' + now4.engine.slice(-60));
  return n;
}
function checkPure(src) {
  const c = makeEnv(src, { props: ON }).ctx;
  const bar = (date, o, h, l, cl) => ({ date, o, h, l, c: cl });
  const B = [bar('2026-10-30', 10, 10.4, 9.8, 10.2), bar('2026-11-02', 10.1, 10.6, 9.9, 10.5), bar('2026-11-03', 10.5, 10.9, 10.3, 10.8)];
  eq(c.dp2ExitAnchorMiss_(B, 10.3, '2026-11-02'), null, 'inside the day range');
  eq(c.dp2ExitAnchorMiss_(B, 9.9 * 0.921, '2026-11-02'), null, 'just inside the 8 % band below the low');
  ok(c.dp2ExitAnchorMiss_(B, 9.9 * 0.919, '2026-11-02') !== null, 'just outside the band below the low');
  eq(c.dp2ExitAnchorMiss_(B, 10.6 * 1.079, '2026-11-02'), null, 'just inside the band above the high');
  ok(c.dp2ExitAnchorMiss_(B, 10.6 * 1.081, '2026-11-02') !== null, 'just outside the band above the high');
  ok(c.dp2ExitAnchorMiss_(B, 0.103, '2026-11-02') !== null && c.dp2ExitAnchorMiss_(B, 1030, '2026-11-02') !== null, 'another unit (x100) both ways');
  ok(c.dp2ExitAnchorMiss_(B, 20.6, '2026-11-02') !== null, 'a 2:1 split the ledger does not carry');
  eq(c.dp2ExitAnchorMiss_(B, 30, ''), null, 'no entry date: not judged');
  eq(c.dp2ExitAnchorMiss_(B, 30, '2026-11-09'), null, 'the entry after the last bar: not judged');
  eq(c.dp2ExitAnchorMiss_([bar('2026-11-09', 10, 10.4, 9.8, 10.2)], 30, '2026-11-02'), null, 'first bar 7 days after the entry: not judged');
  ok(c.dp2ExitAnchorMiss_([bar('2026-11-07', 10, 10.4, 9.8, 10.2)], 30, '2026-11-02') !== null, 'first bar 5 days after the entry: judged');
  eq(c.dp2ExitAnchorMiss_(B, 0, '2026-11-02'), null, 'no entry price: not judged');
  const m = c.dp2ExitAnchorMiss_(B, 5, '2026-11-02');
  deq(plain(m), { date: '2026-11-02', low: 9.9, high: 10.6, close: 10.5, entry: 5, pct: 110.00000000000001 }, 'the miss record');
  // dividends
  const D = [bar('2026-10-29', 20, 20.2, 19.8, 20), bar('2026-10-30', 20, 20.2, 19.8, 20), bar('2026-11-02', 18, 18.2, 17.8, 18)];
  let a = c.dp2ExitDivAdjust_(D, 20, '2026-10-29', [{ date: '2026-11-02', amount: 2 }], '2026-11-03');
  eq(a.applied.length, 1, 'one dividend applied'); near(a.applied[0].factor, 0.9, 1e-12, 'factor 1 - 2/20');
  near(a.bars[0].c, 18, 1e-12, 'a bar before the ex-date scaled'); near(a.bars[1].h, 18.18, 1e-12, 'high scaled');
  ok(a.bars[2] === D[2], 'the ex-date bar unchanged'); near(a.entry, 18, 1e-12, 'bought before the ex-date: the entry scaled');
  a = c.dp2ExitDivAdjust_(D, 18, '2026-11-02', [{ date: '2026-11-02', amount: 2 }], '2026-11-03');
  near(a.entry, 18, 1e-12, 'bought on the ex-date: the entry kept');
  a = c.dp2ExitDivAdjust_(D, 20, '2026-10-29', [{ date: '2026-10-29', amount: 2 }], '2026-11-03');
  eq(a.applied.length, 0, 'no bar before the ex-date: skipped'); ok(a.bars === D, 'the input returned unchanged');
  eq(c.dp2ExitDivAdjust_(D, 20, '2026-10-29', [{ date: '2026-11-02', amount: 12.5 }], '2026-11-03').applied.length, 0, 'above 60 % of the close: skipped');
  eq(c.dp2ExitDivAdjust_(D, 20, '2026-10-29', [{ date: '2026-11-06', amount: 1 }], '2026-11-03').applied.length, 0, 'a future ex-date: skipped');
  a = c.dp2ExitDivAdjust_(D, 20, '2026-10-29', [{ date: '2026-11-03', amount: 1.8 }], '2026-11-03');
  eq(a.applied.length, 1, 'the ex-date arrived before its bar'); near(a.applied[0].factor, 0.9, 1e-12, 'factor on the last close');
  near(a.bars[2].c, 16.2, 1e-12, 'every bar scaled'); near(a.entry, 18, 1e-12, 'entry scaled');
  a = c.dp2ExitDivAdjust_(D, 20, '2026-10-29', [{ date: '2026-10-30', amount: 1 }, { date: '2026-11-02', amount: 2 }], '2026-11-03');
  eq(a.applied.length, 2, 'two dividends'); near(a.bars[0].c, 20 * 0.95 * 0.9, 1e-12, 'factors compound'); near(a.bars[1].c, 18, 1e-12, 'second bar once'); near(a.entry, 17.1, 1e-12, 'entry by both');
  eq(c.dp2ExitDivAdjust_(D, 20, '2026-10-29', [], '2026-11-03').bars, D, 'no dividends: the input');
  // Dividend_Events parse
  const d = new Date(Date.UTC(2026, 10, 2, 21, 0, 0));   // a Date cell = 2026-11-03 00:00 Riyadh
  const tab = c.dp2DivTabFromValues_([['Dividend detail'], DIV_HDR, ['supn.us', '2026-11-03', 0, '8.13', 'usd'], ['SUPN.US', '2026-12-01', 0, 1, 'USD'], ['BLX.US', d, 3, 0.6875, 'USD'], ['', '2026-11-06', 3, 1, 'USD'], ['JUNK.US', '', 3, 1, 'USD'], ['ZERO.US', '2026-11-06', 3, 0, 'USD']]);
  deq(plain(tab), { 'SUPN.US': { date: '2026-11-03', amount: 8.13, currency: 'USD' }, 'BLX.US': { date: '2026-11-03', amount: 0.6875, currency: 'USD' } }, 'the tab map');
  deq(plain(c.dp2DivTabFromValues_([['Symbol', 'Amount'], ['X', 1]])), {}, 'no Ex-Div Date column: empty');
  // the merge
  const t1 = { 'SUPN.US': { date: '2026-11-03', amount: 8.13, currency: 'USD' } };
  deq(plain(c.dp2ExitDivsFor_({ ccy: 'USD', divs: [] }, 'SUPN.US', t1, '2026-11-03')), [{ date: '2026-11-03', amount: 8.13, source: 'Dividend_Events' }], 'the tab fills');
  deq(plain(c.dp2ExitDivsFor_({ ccy: 'USD', divs: [] }, 'SUPN.US', t1, '2026-11-02')), [], 'not before it arrives');
  deq(plain(c.dp2ExitDivsFor_({ ccy: 'GBp', divs: [] }, 'SUPN.US', { 'SUPN.US': { date: '2026-11-03', amount: 8.13, currency: 'GBP' } }, '2026-11-03')), [], 'a subunit quote never matches');
  deq(plain(c.dp2ExitDivsFor_({ ccy: '', divs: [] }, 'SUPN.US', t1, '2026-11-03')), [], 'an unknown quote currency (cached bars): no fallback');
  deq(plain(c.dp2ExitDivsFor_({ ccy: 'USD', divs: [{ date: '2026-11-01', amount: 8.1, source: 'chart' }] }, 'supn.us', t1, '2026-11-03')), [{ date: '2026-11-01', amount: 8.1, source: 'chart' }], 'a chart dividend within 3 days wins');
  deq(plain(c.dp2ExitDivsFor_({ ccy: 'USD', divs: [{ date: '2026-10-20', amount: 0.5, source: 'chart' }] }, 'SUPN.US', t1, '2026-11-03')).length, 2, 'an older chart dividend and the tab one');
  // the chart events
  const ts = Date.parse('2026-11-03T13:30:00Z') / 1000;
  deq(plain(c.dp2YahooDivs_({ events: { dividends: { [String(ts)]: { amount: 8.13, date: ts }, x: { amount: 0, date: ts }, y: { amount: 1 } } } }, 'America/New_York')), [{ date: '2026-11-03', amount: 8.13, source: 'chart' }], 'events parsed');
  deq(plain(c.dp2YahooDivs_({}, 'America/New_York')), [], 'no events');
  eq(c.dp2ExitSanityCoreOk_(), true, 'the self-test core');
}
function checkGate(src) {
  for (const sp of ['1', 'on', 'ON', 'true', 'True', 'yes', ' Yes ', 'on\n']) eq(makeEnv(src, { props: { [GATE]: sp } }).ctx.dp2ExitSanityOn_(), true, 'on: ' + JSON.stringify(sp));
  for (const sp of ['', '0', 'off', 'false', 'no', 'enable', 'onn', 'y']) eq(makeEnv(src, { props: { [GATE]: sp } }).ctx.dp2ExitSanityOn_(), false, 'off: ' + JSON.stringify(sp));
  eq(makeEnv(src, { propsThrow: true }).ctx.dp2ExitSanityOn_(), false, 'a fault reads off');
  const e = makeEnv(src, { props: ON }); e.ctx.dp2ExitSanityOn_(); e.ctx.dp2ExitSanityOn_();
  eq(e.reads[GATE], 1, 'memoised: one read per execution');
  const both = shapeBuild(src, 'oldLine', 'both', { props: ON });
  eq(both.env.reads[GATE], 1, 'both pages and every chart request: one read');
}
function checkSelfTest(src) {
  const run = (s, props) => { const env = makeEnv(s, { props, fetch: chartStub(SHAPES.oldLine().series) }); return plain(env.ctx.dp2SelfTest()); };
  const n0 = run(src, {}), o0 = run(OLD, {});
  deq(n0.checks.map(x => [x.name.split('Decision Pages v2 1.2.33').join('v'), x.ok]), o0.checks.map(x => [x.name.split('Decision Pages v2 1.2.32').join('v'), x.ok]), 'off: the v1.2.32 report');
  const n1 = run(src, ON);
  eq(n1.checks.length, o0.checks.length + 1, 'on: one more line');
  const line = n1.checks.filter(x => /^exit sanity/.test(x.name));
  eq(line.length, 1, 'one exit sanity line');
  eq(line[0].name, 'exit sanity (DP2_EXIT_SANITY): on ' + DOT + ' core ok (a history of another listing gives no signal; an ex-dividend gap is not a stop)', 'the line');
  eq(line[0].ok, true, 'core ok');
}

function battery() {
  const digest = [];
  test('P0 oracle: v1.2.32 rebuilt from the delivered file (SHA-256 pinned); version lockstep 1.2.33; both files compile; the base lacks the new names', () => {
    eq(sha256(OLD), V1232_SHA256, 'v1.2.32');
    ok(/^ \* Version: 1\.2\.33 \(2026-11-03\)$/m.test(NEW), 'header Version line 1.2.33');
    ok(NEW.indexOf("var DP2_VERSION = '1.2.33';   // lockstep with the header") > 0, 'DP2_VERSION 1.2.33');
    ok(/^ \* Version: 1\.2\.32 \(2026-11-03\)$/m.test(OLD) && OLD.indexOf("var DP2_VERSION = '1.2.32';") > 0, 'base is 1.2.32');
    for (const s of [NEW, OLD]) new vm.Script(s, { filename: '27_Decision_Pages_v2.gs' });
    const e = makeEnv(OLD, {});
    for (const k of ['dp2ExitAnchorMiss_', 'dp2ExitDivAdjust_', 'dp2ExitSanityOn_', 'DP2_EXIT_SANITY_PROP']) eq(typeof e.ctx[k], 'undefined', 'the base lacks ' + k);
    eq(makeEnv(NEW, {}).ctx.DP2_EXIT_SANITY_PROP, GATE, 'the gate name');
  });
  test('P1 GOLDEN NEGATIVE: the gate absent / blank / off / junk / a fault -> Portfolio_Decision_v2 (cells, formats, calls, e-mails, chart requests, return) = the v1.2.32 build on the three SUPN shapes; both pages = v1.2.32 on three fixture boards', () => {
    checkGoldenNegative(NEW);
    digest.push(sha256(JSON.stringify(norm(shapeBuild(NEW, 'oldLine', 'pd')))));
  });
  test('P2 the 3 Nov shape (the chart of the old listing), gate on: HOLD -- exit engine n/a, the engine cell names the bar and the entry, one CHECK action, no SELL action or e-mail for SUPN.US; the base sells', () => {
    const n = checkOldLine(NEW);
    digest.push(holding(n, 'SUPN.US').engine);
  });
  test('P3 the ex-dividend day (chart event 8.13 ex 3 Nov, 30.00), gate on: not a stop hit; the adjustment named; the stop of the adjusted replay; events=div requested; the base hits the stop', () => {
    const n = checkExDay(NEW);
    digest.push(holding(n, 'SUPN.US').dyn);
  });
  test('P4 the ex-day before its bar: the Dividend_Events row of the same symbol adjusts; another currency, a future ex-date or another symbol do not; no double count', () => {
    const n = checkTabFallback(NEW);
    digest.push(holding(n, 'SUPN.US').dyn);
  });
  test('P5 pure helpers: the anchor band and its limits, the dividend factors and entry rule, the tab parse, the merge rules, the chart events; the self-test core', () => {
    checkPure(NEW);
  });
  test('P6 the gate: 1 / on / true / yes (any case, trimmed) = on, anything else or a fault = off; one property read per execution', () => {
    checkGate(NEW);
  });
  test('P7 dp2SelfTest: the v1.2.32 report while off; one exit sanity line (core ok) while on', () => {
    checkSelfTest(NEW);
  });
  test('P8 ES5 / ASCII lint of the v1.2.33 diff; inventory (10 functions + 8 vars added, 0 removed); the 3 replaced base lines pinned; 10 hunks whose reversal is the v1.2.32 file; WHY block', () => {
    const bl = OLD.split('\n'), nl = NEW.split('\n');
    const baseSet = new Set(bl), newSet = new Set(nl);
    const added = nl.filter(l => !baseSet.has(l));
    ok(added.length >= 150 && added.length <= 230, 'added lines ' + added.length);
    for (const l of added) {
      const code = l.replace(/\/\/.*$/, '');
      ok(!/\b(let|const|class)\s|=>|`|\.\.\.|\.includes\(|\.startsWith\(|\.endsWith\(|\.padStart\(|\.find\(/.test(code) || /^\s*(\*|\/\*)/.test(l), 'ES6 token: ' + l.slice(0, 120));
      ok(/^[\x09\x20-\x7e]*$/.test(l), 'non-ASCII added line: ' + l.slice(0, 120));
      ok(!/^\s{2,}function\s/.test(l), 'no block-level function declaration: ' + l.slice(0, 80));
    }
    ok(NEW.indexOf('\r') < 0, 'LF only');
    const NEWF = ['dp2DayMs_', 'dp2DivTabFromValues_', 'dp2DivTab_', 'dp2ExitAnchorMiss_', 'dp2ExitAnchorWhy_', 'dp2ExitDivAdjust_', 'dp2ExitDivsFor_', 'dp2ExitSanityCoreOk_', 'dp2ExitSanityOn_', 'dp2YahooDivs_'];
    for (const name of NEWF) {
      const start = NEW.indexOf('function ' + name + '(');
      ok(start > 0, 'defined ' + name);
      const body = NEW.slice(start, NEW.indexOf('\n}\n', start) + 3);
      ok(/^[\x00-\x7f]*$/.test(body), 'ASCII ' + name);
      new vm.Script('(function(){' + body + '})');
    }
    const names = (src, re) => { const out = {}; let m; while ((m = re.exec(src))) out[m[1]] = true; return out; };
    const fb = names(OLD, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), fn = names(NEW, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    deq(Object.keys(fb).filter(k => !fn[k]), [], 'top-level functions removed');
    deq(Object.keys(fn).filter(k => !fb[k]).sort(), NEWF, 'top-level functions added');
    const vb = names(OLD, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = names(NEW, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq(Object.keys(vb).filter(k => !vn[k]), [], 'vars removed');
    deq(Object.keys(vn).filter(k => !vb[k]).sort(), ['DP2X_SAN_', 'DP2_DIVIDEND_SHEET', 'DP2_EXIT_ANCHOR_GAP_D', 'DP2_EXIT_ANCHOR_PCT', 'DP2_EXIT_DIV_MAX', 'DP2_EXIT_DIV_NEAR_D', 'DP2_EXIT_SANITY_PROP', 'DP2_EXIT_SUBUNIT_'], 'vars added');
    const REMOVED = ['* Version: 1.2.32 (2026-11-03)', "var DP2_VERSION = '1.2.32';", 'return { bars: bars, last: r.meta ? dp2Num_(r.meta.regularMarketPrice) : null, ccy:'];
    const removed = bl.filter(l => !newSet.has(l)).map(l => l.trim());
    eq(removed.length, REMOVED.length, 'base lines replaced: ' + JSON.stringify(removed.map(x => x.slice(0, 60))));
    for (let i = 0; i < removed.length; i++) ok(removed[i].indexOf(REMOVED[i]) === 0, 'replaced base line ' + i + ': ' + removed[i].slice(0, 120));
    let pos = 0; for (const l of bl) { if (!newSet.has(l)) continue; const at = nl.indexOf(l, pos); ok(at >= pos, 'base line out of order: ' + l.slice(0, 80)); pos = at + 1; }
    ok(NEW.indexOf("  var outY = { bars: bars, last: r.meta ? dp2Num_(r.meta.regularMarketPrice) : null, ccy: (r.meta && r.meta.currency) || '', marketState: (r.meta && r.meta.marketState) || '', source: 'yahoo' };") > 0, 'the return object kept field for field');
    const why = NEW.slice(NEW.indexOf(' *   1.2.33: '), NEW.indexOf(' *   1.2.32: '));
    ok(why.length > 1500 && /^[\x09\x0a\x20-\x7e]*$/.test(why), 'WHY block (1.2.33) present, newest first, ASCII');
    for (const w of ['DP2_EXIT_SANITY', 'default OFF', 'Rollback: delete DP2_EXIT_SANITY', 'dp2ExitAnchorMiss_', 'dp2ExitDivAdjust_', 'Dividend_Events', 'events=div']) ok(why.indexOf(w) >= 0, 'WHY names ' + w);
    for (const l of bl.filter(x => /^ \*   1\.\d+\.\d+[: ]/.test(x))) ok(NEW.indexOf(l) > 0, 'prior WHY entry kept: ' + l.slice(0, 60));
    eq(reverseEdits(NEW), OLD, 'hunk reversal = the v1.2.32 file');
    eq(EDITS.length, 10, 'ten hunks');
    digest.push(String(EDITS.length));
  });
  test('P9 mutants: each mutation of the delivered file fails at least one section check', () => {
    const M = [
      ['gate always on', 'if (DP2X_SAN_ === null) DP2X_SAN_ = /^(1|on|true|yes)$/i.test(', 'if (DP2X_SAN_ === null) DP2X_SAN_ = true || /^(1|on|true|yes)$/i.test(', [checkGoldenNegative]],
      ['gate never on', 'if (DP2X_SAN_ === null) DP2X_SAN_ = /^(1|on|true|yes)$/i.test(', 'if (DP2X_SAN_ === null) DP2X_SAN_ = false && /^(1|on|true|yes)$/i.test(', [checkOldLine]],
      ['the gate not memoised', '  if (DP2X_SAN_ === null) DP2X_SAN_ =', '  DP2X_SAN_ =', [checkGate]],
      ['a miss still computes', '      if (xMiss) x = null;\n', '', [checkOldLine]],
      ['no band (exact close only)', 'var DP2_EXIT_ANCHOR_PCT = 8;', 'var DP2_EXIT_ANCHOR_PCT = 0;', [checkPure]],
      ['a wide band', 'var DP2_EXIT_ANCHOR_PCT = 8;', 'var DP2_EXIT_ANCHOR_PCT = 25;', [checkOldLine]],
      ['the entry not scaled', '    if (entryDate ? d.date > entryDate : k === n) entryF *= fac;\n', '', [checkPure]],
      ['the ex-day before its bar ignored', '    if (k < 0) { if (today && d.date <= today) k = n; else continue; }', '    if (k < 0) continue;', [checkTabFallback]],
      ['a future ex-date merged', "  if (t && t.date && today && t.date <= today && ccy", "  if (t && t.date && today && ccy", [checkTabFallback, checkPure]],
      ['the currency not checked', " && t.currency === ccy.toUpperCase()) {", ") {", [checkTabFallback]],
      ['double count', "    if (!near) out.push({ date: t.date, amount: t.amount, source: 'Dividend_Events' });", "    out.push({ date: t.date, amount: t.amount, source: 'Dividend_Events' });", [checkTabFallback, checkPure]],
      ['events never requested', "  if (dp2ExitSanityOn_()) url += '&events=div';", '', [checkExDay]],
      ['no CHECK action', "    if (xMiss) actions.push({ p: 1, t: 'CHECK ' + l.symbol", "    if (false) actions.push({ p: 1, t: 'CHECK ' + l.symbol", [checkOldLine]],
      ['the adjustment not named', "    else if (xDiv && x) engTxt += ' " + ESC('00b7') + " dividend-adjusted ('", "    else if (false) engTxt += ' " + ESC('00b7') + " dividend-adjusted ('", [checkExDay]],
      ['the self-test line always', '  if (dp2ExitSanityOn_()) { var xsOk = dp2ExitSanityCoreOk_();', '  if (true) { var xsOk = dp2ExitSanityCoreOk_();', [checkSelfTest]]
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
console.log('DP2 V1233 P0-P9 x2 identical digest ' + digests[0] + ' | new SHA256 ' + sha256(NEW) + ' | v1.2.32 SHA256 ' + sha256(OLD) + ' | ' + Math.round((Date.now() - t0) / 1000) + 's');
console.log('PASS ' + passed + '/' + total);
