#!/usr/bin/env node
/* tests/test_dp2_v1234_render_priority.js
 * Harness for apps_script/27_Decision_Pages_v2.gs v1.2.34 [RENDER-PRIORITY] (owner review, Thu 5 Nov 2026 01:48 Riyadh:
 * Top_10_Investments_v2 still read 'Status: Last run 2026-11-05 00:03:55 | status: running...' - the hourly build put
 * Portfolio_Decision_v2 first and skipped this page whenever that one used RUN_SOFT_LIMIT_MS, and it ran beside the
 * decision renders Apps Script stopped at 6 minutes in sync run 41901407829). Script Property TFB_RENDER_PRIORITY =
 * 1 / on / true / yes (default OFF; shared with 26_Decision_Chain 1.0.5 and 28_Plan_Of_Record 1.0.1): (a) the hourly
 * build yields while a Top_10 / Portfolio_Decision render runs (DT10_RUN_MARKER or the chain claim < 7 min), (b) it
 * builds the page built longest ago first (DP2_LAST_BUILT) and isolates a failing page, (c) Top_10_Investments_v2
 * prints its build time and names a cockpit render that is in progress or did not finish.
 *
 * Two trees over the REAL file, each in its own vm context: the delivered source (NEW) and the v1.2.33 file (OLD,
 * SHA-256 pinned dd5ee204 = origin/main c54eba40), by default rebuilt from NEW by reversing the enumerated v1.2.34
 * hunks (tests/js/dp2_edits_v1234.json); a second argument names the v1.2.33 file instead. The GAS shim, the fixture
 * (tests/fixtures/dp2_board_2026-11-02.json), the board variants, makeEnv and the read-backs are those of
 * tests/test_dp2_v1233_exit_sanity.js, copied verbatim. Every value is synthetic except the 5 Nov status shape.
 * Sections:
 *   P0  oracle (SHA-256), version lockstep, both files compile; the base lacks the new names
 *   P1  GOLDEN NEGATIVE: the gate absent / blank / off / 0 / junk / a property fault -> both pages and each page alone
 *       (every cell, format, merge, chart, the call log, toasts, logs, return values) = the v1.2.33 build, on five
 *       boards (two of them a cockpit 'running' status), with a live render marker, a live chain claim and old stamps
 *   P2  yield, gate on: a marker or claim younger than 7 min -> no page written, {yielded: true}, one toast and log
 *       line, no stamp; an old, future or junk marker / claim -> both pages built
 *   P3  order, gate on: no stamps -> Portfolio_Decision_v2 first; the older page first; an unstamped page first; the
 *       soft limit skips the second page and stamps only the first; a failing page is stamped, the other page is still
 *       built, the toast names the failure and the error is rethrown
 *   P4  Top_10_Investments_v2, gate on: the header names the build time; a cockpit render in progress (4 min) and one
 *       that did not finish (19 min) are named on the Status line (amber); a finished status is unchanged; every
 *       other cell of both pages equals the gate-off build
 *   P5  the pure helpers: dp2RpOnCore_, dp2RpBusyCore_, dp2RpOrderCore_, dp2RpRunningNoteCore_, dp2RpCoreOk_
 *   P6  the gate: spellings, a fault reads off, one property read per execution
 *   P7  dp2SelfTest: the v1.2.33 report while off; one 'render priority .. core ok' line while on
 *   P8  ES5 / ASCII lint of the v1.2.34 diff; inventory (9 functions + 4 vars added, 0 removed); the 3 replaced base
 *       lines pinned; 6 hunks whose reversal is the v1.2.33 file; WHY block; every prior WHY kept
 *   P9  mutants: each mutation of the delivered file fails at least one section check
 * The battery runs twice and the two digests must match; the last line is 'PASS k/k' (exit 1 on any failure).
 * Usage: node tests/test_dp2_v1234_render_priority.js [new.gs] [v1.2.33.gs]
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
const EDITS = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'dp2_edits_v1234.json'), 'utf8'));
const V1233_SHA256 = 'dd5ee2041b065b5e12aed7c41ea505fd3b75b3219e29807d060ff4d95a394756';   // v1.2.33 (origin/main c54eba40)
const NOW = Date.parse(FIX.frozen_now);   // 2026-11-02T06:24:00Z = 09:24 Riyadh
const GATE = 'TFB_RENDER_PRIORITY';
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
    eq(s.split(cur).length - 1, 1, 'exactly one approved v1.2.34 hunk: ' + cur.split('\n')[0].slice(0, 90));
    s = s.replace(cur, () => prev);
  }
  eq(sha256(s), V1233_SHA256, 'v1.2.34 changes only its enumerated hunks (their reversal = the v1.2.33 file)');
  return s;
}
let OLD = null, oldErr = '';
try {
  OLD = OLD_PATH ? fs.readFileSync(OLD_PATH, 'utf8') : reverseEdits(NEW);
  eq(sha256(OLD), V1233_SHA256, 'v1.2.33 SHA-256');
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
  return JSON.parse(JSON.stringify(strip(b)).split('Decision Pages v2 1.2.34').join('Decision Pages v2 1.2.30').split('Decision Pages v2 1.2.33').join('Decision Pages v2 1.2.30').split('Decision Pages v2 1.2.33').join('Decision Pages v2 1.2.30').split('Decision Pages v2 1.2.32').join('Decision Pages v2 1.2.30').split('Decision Pages v2 1.2.31').join('Decision Pages v2 1.2.30'));
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

function battery() {
  const digest = [];
  test('P0 oracle: v1.2.33 rebuilt from the delivered file (SHA-256 pinned); version lockstep 1.2.34; both files compile; the base lacks the new names', () => {
    eq(sha256(OLD), V1233_SHA256, 'v1.2.33');
    ok(/^ \* Version: 1\.2\.34 \(2026-11-05\)$/m.test(NEW), 'header Version line 1.2.34');
    ok(NEW.indexOf("var DP2_VERSION = '1.2.34';   // lockstep with the header") > 0, 'DP2_VERSION 1.2.34');
    ok(/^ \* Version: 1\.2\.33 \(2026-11-03\)$/m.test(OLD) && OLD.indexOf("var DP2_VERSION = '1.2.33';") > 0, 'base is 1.2.33');
    for (const s of [NEW, OLD]) new vm.Script(s, { filename: '27_Decision_Pages_v2.gs' });
    const e = makeEnv(OLD, {});
    for (const k of ['dp2RpOn_', 'dp2RpBusyCore_', 'dp2RpBuildBoth_', 'DP2_RP_PROP_']) eq(typeof e.ctx[k], 'undefined', 'the base lacks ' + k);
    eq(makeEnv(NEW, {}).ctx.DP2_RP_PROP_, GATE, 'the gate name');
    eq(makeEnv(NEW, {}).ctx.DP2_RP_BUILT_PROP_, 'DP2_LAST_BUILT', 'the stamp property');
  });
  test('P1 GOLDEN NEGATIVE: the gate absent / blank / off / 0 / junk / a fault -> both pages and each page alone = the v1.2.33 build (five boards, a live marker and claim, old stamps; no stamp written)', () => {
    checkGoldenNegative(NEW);
    digest.push(sha256(JSON.stringify(norm(build(NEW, 'both', { board: BOARDS.runningStopped })))));
  });
  test('P2 yield, gate on: a marker or claim younger than 7 min -> no page, {yielded}, one toast and log line, no stamp; an old / future / junk marker or claim -> both pages', () => {
    checkYield(NEW);
  });
  test('P3 order, gate on: no stamps -> Portfolio_Decision_v2 first; the older or unstamped page first; the soft limit skips the second page; a failing page is isolated, stamped and rethrown', () => {
    digest.push(checkOrder(NEW));
  });
  test('P4 Top_10_Investments_v2, gate on: the build time in the header; a render in progress / not finished named on the Status line (amber); everything else = the gate-off build', () => {
    digest.push(checkPage(NEW));
  });
  test('P5 pure helpers: the gate spelling, the busy window (marker and claim), the order, the running note, the self-test core', () => {
    checkPure(NEW);
  });
  test('P6 the gate: 1 / on / true / yes (any case, trimmed) = on, anything else or a fault = off; one property read per execution', () => {
    checkGate(NEW);
  });
  test('P7 dp2SelfTest: the v1.2.33 report while off; one render priority line (core ok) while on', () => {
    checkSelfTest(NEW);
  });
  test('P8 ES5 / ASCII lint of the v1.2.34 diff; inventory (9 functions + 4 vars added, 0 removed); the 3 replaced base lines pinned; 6 hunks whose reversal is the v1.2.33 file; WHY block', () => {
    const bl = OLD.split('\n'), nl = NEW.split('\n');
    const baseSet = new Set(bl), newSet = new Set(nl);
    const added = nl.filter(l => !baseSet.has(l));
    ok(added.length >= 110 && added.length <= 180, 'added lines ' + added.length);
    for (const l of added) {
      const code = l.replace(/\/\/.*$/, '');
      ok(!/\b(let|const|class)\s|=>|`|\.\.\.|\.includes\(|\.startsWith\(|\.endsWith\(|\.padStart\(|\.find\(/.test(code) || /^\s*(\*|\/\*)/.test(l), 'ES6 token: ' + l.slice(0, 120));
      ok(/^[\x09\x20-\x7e]*$/.test(l), 'non-ASCII added line: ' + l.slice(0, 120));
      ok(!/^\s{2,}function\s/.test(l), 'no block-level function declaration: ' + l.slice(0, 80));
    }
    ok(NEW.indexOf('\r') < 0, 'LF only');
    const NEWF = ['dp2RpBuildBoth_', 'dp2RpBusyCore_', 'dp2RpBusy_', 'dp2RpCoreOk_', 'dp2RpOnCore_', 'dp2RpOn_', 'dp2RpOrderCore_', 'dp2RpRunningNoteCore_', 'dp2RpStamp_'].sort();
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
    deq(Object.keys(vn).filter(k => !vb[k]).sort(), ['DP2_RP_BUILT_PROP_', 'DP2_RP_BUSY_MIN_', 'DP2_RP_MEMO_', 'DP2_RP_PROP_'], 'vars added');
    const REMOVED = ['* Version: 1.2.33 (2026-11-03)', "var DP2_VERSION = '1.2.33';", "dp2Line_(sh, row, 'Status: ' + t.status, { size: 8, bg: DP2C.surface, height: 20 }); row++;"];
    const removed = bl.filter(l => !newSet.has(l)).map(l => l.trim());
    eq(removed.length, REMOVED.length, 'base lines replaced: ' + JSON.stringify(removed.map(x => x.slice(0, 60))));
    for (let i = 0; i < removed.length; i++) ok(removed[i].indexOf(REMOVED[i]) === 0, 'replaced base line ' + i + ': ' + removed[i].slice(0, 120));
    let pos = 0; for (const l of bl) { if (!newSet.has(l)) continue; const at = nl.indexOf(l, pos); ok(at >= pos, 'base line out of order: ' + l.slice(0, 80)); pos = at + 1; }
    const why = NEW.slice(NEW.indexOf(' *   1.2.34: '), NEW.indexOf(' *   1.2.33: '));
    ok(why.length > 1500 && /^[\x09\x0a\x20-\x7e]*$/.test(why), 'WHY block (1.2.34) present, newest first, ASCII');
    for (const w of ['TFB_RENDER_PRIORITY', 'default OFF', 'DP2_LAST_BUILT', 'DT10_RUN_MARKER', 'RUN_SOFT_LIMIT_MS', '41901407829', 'Gate off', 'Rollback:']) ok(why.indexOf(w) >= 0, 'WHY names ' + w);
    for (const l of bl.filter(x => /^ \*   1\.\d+\.\d+[: ]/.test(x))) ok(NEW.indexOf(l) > 0, 'prior WHY entry kept: ' + l.slice(0, 60));
    eq(reverseEdits(NEW), OLD, 'hunk reversal = the v1.2.33 file');
    eq(EDITS.length, 6, 'six hunks');
    digest.push(String(EDITS.length));
  });
  test('P9 mutants: each mutation of the delivered file fails at least one section check', () => {
    const GA = "  return v === 'on' || v === '1' || v === 'true' || v === 'yes';\n}\nfunction dp2RpOn_() {";
    const M = [
      ['gate always on', GA, '  return true;\n}\nfunction dp2RpOn_() {', [checkGoldenNegative]],
      ['gate never on', GA, '  return false;\n}\nfunction dp2RpOn_() {', [checkYield]],
      ['the gate not memoised', '  if (DP2_RP_MEMO_ === null) {', '  if (true) {', [checkGate]],
      ['the gate-off path enters the new build', '  if (dp2RpOn_()) return dp2RpBuildBoth_(ss, t0);', '  return dp2RpBuildBoth_(ss, t0);', [checkGoldenNegative]],
      ['the marker ignored', '      if (isFinite(ms) && nowMs - ms >= 0 && nowMs - ms < lim) return true;', '      if (false) return true;', [checkYield]],
      ['the claim ignored', "    if (c && typeof c.at_ms === 'number' && nowMs - c.at_ms >= 0 && nowMs - c.at_ms < lim) return true;", '    if (false) return true;', [checkYield]],
      ['a 70-minute window', 'var DP2_RP_BUSY_MIN_ = 7;', 'var DP2_RP_BUSY_MIN_ = 70;', [checkYield]],
      ['never yields', '  if (dp2RpBusy_(new Date().getTime())) {', '  if (false) {', [checkYield]],
      ['the stamps ignored', "  return t10 < pd ? ['t10', 'pd'] : ['pd', 't10'];", "  return ['pd', 't10'];", [checkOrder]],
      ['an unstamped page counts as newest', "t10 = typeof b.t10 === 'number' ? b.t10 : 0;", "t10 = typeof b.t10 === 'number' ? b.t10 : Infinity;", [checkOrder]],
      ['no stamp', '    dp2RpStamp_(page, new Date().getTime());\n', '', [checkOrder]],
      ['the soft limit ignored', '    if (i > 0 && dp2RunElapsed_() > DP2X.RUN_SOFT_LIMIT_MS) {', '    if (false) {', [checkOrder]],
      ['a failing page not isolated', '    catch (eB) { res[page] = { rows: 0, error: String(eB && eB.message || eB) }; if (!firstErr) firstErr = eB; }', '    catch (eB) { throw eB; }', [checkOrder]],
      ['the error swallowed', '  if (firstErr) throw firstErr;\n', '', [checkOrder]],
      ['no build time', '  if (dp2RpOn_()) { var rpHr = sh.getRange(row, 13);', '  if (false) { var rpHr = sh.getRange(row, 13);', [checkPage]],
      ['no running note', "  var rpNote = dp2RpOn_() ? dp2RpRunningNoteCore_(t.status, t.lastRun, new Date().getTime()) : '';", "  var rpNote = '';", [checkPage]],
      ['the note window 70 min', 'if (age !== null && age < DP2_RP_BUSY_MIN_) return', 'if (age !== null && age < 70) return', [checkPure, checkPage]],
      ['the self-test line always', '  if (dp2RpOn_()) { var rpOk = dp2RpCoreOk_();', '  if (true) { var rpOk = dp2RpCoreOk_();', [checkSelfTest]]
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
console.log('DP2 V1234 P0-P9 x2 identical digest ' + digests[0] + ' | new SHA256 ' + sha256(NEW) + ' | v1.2.33 SHA256 ' + sha256(OLD) + ' | ' + Math.round((Date.now() - t0) / 1000) + 's');
console.log('PASS ' + passed + '/' + total);
