#!/usr/bin/env node
/* tests/test_dp2_v1231_engine_tickets.js
 * Harness for apps_script/27_Decision_Pages_v2.gs v1.2.31 [ENGINE-TICKET MIRROR] (owner review of Mon 2 Nov
 * 2026: Top_10_Investments_v2 read EXECUTABLE 6 beside 'SEATS - 10 . 0 plans . 3 grace-held' and flagged the six
 * engine tickets 'grace-held . not sized'; Portfolio_Decision_v2 said 'No board-originated capital .. stays
 * Tranche 0' on the same EXECUTABLE feed). v1.2.31 also installs the prepared v1.2.29 (D4) and v1.2.30 steps of
 * 28 Oct, which never reached the live file.
 *
 * Three trees over the REAL file, each in its own vm context: the delivered source (NEW), the v1.2.30 oracle
 * (ORACLE = the eleven prepared steps of 28 Oct applied to v1.2.28, SHA-256 pinned) and the live v1.2.28 file
 * (BASE, SHA-256 pinned a99af66a). By default ORACLE is rebuilt from NEW by reversing the enumerated v1.2.31 hunks
 * (tests/js/dp2_edits_v1231.json) and BASE from ORACLE by reversing the eleven steps (tests/js/dp2_steps_v1230.json);
 * a second argument names the v1.2.28 file instead (ORACLE is then the eleven steps applied to it). The GAS shim is
 * a recording spreadsheet (cells, formats, merges, borders, row heights, charts, an ordered call log),
 * PropertiesService, Utilities, LockService, MailApp, Session, a failing UrlFetchApp (cached bars only), the
 * 10_My_Portfolio IBKR capture reader and a frozen clock (2026-11-02 09:24 Riyadh). The board is today's live read
 * (tests/fixtures/dp2_board_2026-11-02.json).
 * Sections:
 *   T0  oracles (SHA-256), version lockstep, the three files compile
 *   T1  kill switch: DP2_COHORT_LEGACY = 1 / on / true / yes (any case, trimmed) -> both pages (every cell, format,
 *       merge, chart, the call log, the return values) equal the v1.2.30 build on an engine board, a strict
 *       research board and a not-strict research board; other spellings, absent and a property fault -> the new view
 *   T2  engine board (today): 'SEATS - 10 . 6 engine tickets . 1 research . 3 grace-held', Alloc = Ticket SAR,
 *       Buy qty = Shares, one class flag per seat, navy / amber status, Held seats and Allocated tiles, funnel
 *       'Seats -> plans', ALLOCATION 'Engine tickets: 6 . 68,995 SAR (feed ..)', the Executable tile unchanged;
 *       only the expected rows differ from v1.2.30 on both pages; engine tickets beyond the cohort caps all mirrored;
 *       once the release is not live (NOT_ACTIONABLE / EXPIRED) the engine allocation tile turns grey with the feed
 *       state; an active seat the engine left unlabelled reads 'not funded by the engine'; 'Refresh both' renders the
 *       same Top-10 page as the page alone
 *   T3  research board (no engine ticket, strict): the seven ACTIVE seats sized by the unchanged water-fill (= the
 *       v1.2.30 sizing of the same seats labelled FAST-TRACK), the FAST-TRACK seat suspended; not strict -> sized;
 *       a 'sizing suspended' Advisor Note suspends without the strict token; the cohort caps still bind the
 *       research cohort ('outside the cohort caps')
 *   T4  16_Decision_Top10 v1.20.0 'Held - ..' Funds From reads research (class table; both boards)
 *   T5  Portfolio_Decision_v2 action 3: EXECUTABLE -> 'Board: 6 engine tickets (68,995 SAR, feed valid until
 *       10:42). S-1 gate NOT_DECIDABLE (criteria 1 PENDING, 4 PENDING) - ..'; NOT_ACTIONABLE / SYNC RUNNING /
 *       EXPIRED / legacy / no engine ticket -> the v1.2.28 text; S-1 PASS -> neither
 *   T6  D4 (1.2.29): an IBKR order REPLACED with remaining 174 counts as working (no 'unprotected' action); a
 *       REPLACED order with nothing remaining still reads replaced; (Codex P1 on #896) a partly filled stop covers
 *       only its remaining shares (dp2CoverQty_), behind the same kill switch
 *   T7  1.2.30: certification expires with the capture (08:50 CERTIFIED, 09:24 UNCERTIFIED 'EXPIRED at 08:55'),
 *       a NOT_ACTIONABLE feed for the same run never certifies
 *   T8  golden negatives: the T2-T6 checks fail on v1.2.28, the T2-T5 checks on v1.2.30, the T7 check on v1.2.28
 *   T9  mutants: each mutation of the delivered file fails at least one section check
 *   T10 ES5 / ASCII lint of the added lines, function / var inventory, hunks whose reversal is the v1.2.30 oracle
 * The battery runs twice and the two digests must match; the last line is 'PASS k/k' (exit 1 on any failure).
 * Usage: node tests/test_dp2_v1231_engine_tickets.js [new.gs] [v1.2.28.gs]
 */
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const vm = require('vm');
const ROOT = path.join(__dirname, '..');
const NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '27_Decision_Pages_v2.gs');
const BASE_PATH = process.argv[3] || '';
const NEW = fs.readFileSync(NEW_PATH, 'utf8');
const FIX = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'fixtures', 'dp2_board_2026-11-02.json'), 'utf8'));
const EDITS = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'dp2_edits_v1231.json'), 'utf8'));
const STEPS = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'dp2_steps_v1230.json'), 'utf8'));
const V1228_SHA256 = 'a99af66a40ed71923e9828952c4d1da339c6d864b215579071d95733554d7193';   // live v1.2.28 (mirror ae4a03f0)
const V1230_SHA256 = '484806384b0d815d3f18aa8c2a6ab0d83b1dfd1652f080ff4990e9e53717b806';   // v1.2.28 + the 11 steps of 28 Oct
// 2026-11-03: v1.2.32 (preview label, DP2_PREVIEW_LABEL default off) stacked on this change; pins moved. Behaviour runs on
// the delivered file; the oracle chain and the v1.2.31 lint / inventory / hunks (T10) start from the v1.2.31 file rebuilt
// from it (the v1.2.32 hunks of tests/js/dp2_edits_v1232.json reversed, SHA-256 pinned).
const EDITS1232 = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'dp2_edits_v1232.json'), 'utf8'));
const V1231_SHA256 = 'd789cf502c08ed00278196707b4f6d3057c5c1bbd00b86ad47254ed77e3cac20';   // v1.2.31 (origin/main 8478029d)
// 2026-11-03: v1.2.33 (exit sanity, DP2_EXIT_SANITY default off) stacked on v1.2.32; the chain first reverses the
// v1.2.33 hunks of tests/js/dp2_edits_v1233.json (the v1.2.32 file, SHA-256 pinned), then the v1.2.32 hunks.
const EDITS1233 = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'js', 'dp2_edits_v1233.json'), 'utf8'));
const V1232_SHA256 = '5ac92c367d86a344ed7beefff33d490d292b82fb68b4ede7fd0c0a9afb18a723';   // v1.2.32 (origin/main 3e326e26)
function v1232Of(src) {
  if (src.indexOf("var DP2_VERSION = '1.2.33';") < 0) return src;
  let s = src;
  for (const [cur, prev] of EDITS1233) {
    if (s.split(cur).length - 1 !== 1) throw new Error('exactly one approved v1.2.33 hunk: ' + cur.split('\n')[0].slice(0, 90));
    s = s.replace(cur, () => prev);
  }
  if (sha256(s) !== V1232_SHA256) throw new Error('v1.2.33 changes only its enumerated hunks (their reversal = the v1.2.32 file)');
  return s;
}
function v1231Of(src) {
  src = v1232Of(src);
  if (src.indexOf("var DP2_VERSION = '1.2.31';") > 0) return src;
  let s = src;
  for (const [cur, prev] of EDITS1232) {
    if (s.split(cur).length - 1 !== 1) throw new Error('exactly one approved v1.2.32 hunk: ' + cur.split('\n')[0].slice(0, 90));
    s = s.replace(cur, () => prev);
  }
  if (sha256(s) !== V1231_SHA256) throw new Error('v1.2.32 changes only its enumerated hunks (their reversal = the v1.2.31 file)');
  return s;
}
const NOW = Date.parse(FIX.frozen_now);   // 2026-11-02T06:24:00Z = 09:24 Riyadh (the owner's build)
const LEGACY = 'DP2_COHORT_LEGACY';
const DOT = '·', DASH = '—', ARROW = '→', NDASH = '–';
const ESC = hex => '\\u' + hex;   // an escape as TEXT in the .gs source (the ASCII spelling of a page character)
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
function fails(fn, m) {
  let threw = false;
  try { fn(); } catch (e) { threw = true; }
  if (!threw) throw new Error('expected a failure: ' + m);
}

/* ------------------------------------------------------------ T0 oracles */
function reverseEdits(src) {
  let s = src;
  for (const [cur, prev] of EDITS) {
    eq(s.split(cur).length - 1, 1, 'exactly one approved v1.2.31 hunk: ' + cur.split('\n')[0].slice(0, 90));
    s = s.replace(cur, () => prev);
  }
  eq(sha256(s), V1230_SHA256, 'v1.2.31 changes only its enumerated hunks (their reversal = the v1.2.30 oracle)');
  return s;
}
function applySteps(src) {
  let s = src;
  for (const [find, repl] of STEPS) { eq(s.split(find).length - 1, 1, 'step Find occurs once: ' + find.slice(0, 70)); s = s.replace(find, () => repl); }
  eq(sha256(s), V1230_SHA256, 'the eleven prepared steps applied to v1.2.28 = the v1.2.30 oracle');
  return s;
}
function unapplySteps(src) {
  let s = src;
  for (let i = STEPS.length - 1; i >= 0; i--) { const [find, repl] = STEPS[i]; eq(s.split(repl).length - 1, 1, 'step Replace occurs once: ' + repl.slice(0, 70)); s = s.replace(repl, () => find); }
  eq(sha256(s), V1228_SHA256, 'the eleven steps reversed = the live v1.2.28');
  return s;
}
let ORACLE = null, BASE = null, S1231 = null, oracleErr = '';
try {
  S1231 = v1231Of(NEW);
  if (BASE_PATH) {
    BASE = fs.readFileSync(BASE_PATH, 'utf8');
    eq(sha256(BASE), V1228_SHA256, 'explicit base = the live v1.2.28');
    ORACLE = applySteps(BASE);
  } else {
    ORACLE = reverseEdits(S1231);
    BASE = unapplySteps(ORACLE);
  }
} catch (e) { oracleErr = String(e && e.message || e); }
if (!ORACLE || !BASE) { console.log('FAIL T0 oracles: ' + oracleErr); console.log('FAIL 0/1'); process.exit(1); }

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
  return JSON.parse(JSON.stringify(strip(b)).split('Decision Pages v2 1.2.33').join('Decision Pages v2 1.2.30').split('Decision Pages v2 1.2.32').join('Decision Pages v2 1.2.30').split('Decision Pages v2 1.2.31').join('Decision Pages v2 1.2.30'));
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
const TODAY_HEADER = 'SEATS ' + DASH + ' 10 ' + DOT + ' 6 engine tickets ' + DOT + ' 1 research ' + DOT + ' 3 grace-held';
const ACTION3_NEW = 'Board: 6 engine tickets (68,995 SAR, feed valid until 10:42). S-1 gate NOT_DECIDABLE (criteria 1 PENDING, 4 PENDING) ' + DASH +
  ' the tickets are not S-1 authorised; size under your interim rule (1' + NDASH + '2 tickets).';
const ACTION3_OLD = 'No board-originated capital: S-1 gate NOT_DECIDABLE (1 PENDING, 4 PENDING); deployable 63,547 SAR (after the 10% PF cash floor) stays Tranche 0.';
const NAVY = '#0b2a4a', AMBER = '#8a5a00', GREEN = '#1a7f37';

/* T2: the engine board as the v1.2.31 page must show it (throws otherwise) */
function checkEngineBoard(src) {
  const b = build(src, 't10', { board: BOARDS.today });
  const v = t10View(b);
  eq(v.title, TODAY_HEADER, 'SEATS header');
  deq(v.seats.map(s => s.sym), SEAT_SYMS, 'seat order');
  deq(v.seats.map(s => s.alloc), [12199, 12334, 8527, 12133, 11467, 12335, '', '', '', ''], 'Alloc SAR = the engine Ticket SAR');
  deq(v.seats.map(s => s.qty), [84, 308, 46, 70, 239, 141, '', '', '', ''], 'Buy qty = the engine Shares');
  for (let i = 0; i < 6; i++) { ok(v.seats[i].flags.indexOf('engine ticket') === 0, 'engine flag ' + v.seats[i].sym + ': ' + v.seats[i].flags); eq(v.seats[i].statusColor, NAVY, 'navy ' + v.seats[i].sym); eq(v.seats[i].allocColor, GREEN, 'funded green ' + v.seats[i].sym); }
  ok(v.seats[6].flags.indexOf('research ' + DOT + ' not sized (' + RESEARCH_FF + ')') === 0, 'OLP research flag: ' + v.seats[6].flags);
  for (let i = 7; i < 10; i++) ok(v.seats[i].flags.indexOf('grace-held ' + DOT + ' not sized') === 0, 'grace flag ' + v.seats[i].sym);
  for (let i = 6; i < 10; i++) eq(v.seats[i].statusColor, AMBER, 'amber ' + v.seats[i].sym);
  for (let i = 0; i < 7; i++) ok(v.seats[i].flags.indexOf('grace-held') < 0, 'an ACTIVE seat is never flagged grace-held: ' + v.seats[i].sym);
  ok(v.sub.indexOf("Alloc / Buy qty = the engine's own tickets as Top_10_Investments issued them (Ticket SAR / Shares; feed EXECUTABLE, valid until 10:42 Riyadh)") === 0, 'subtitle: ' + v.sub);
  const held = v.tile('HELD SEATS'); eq(held.sub, '7 active ' + DOT + ' 0 fast-track ' + DOT + ' 3 grace-held', 'Held seats tile');
  const al = v.tile('ALLOCATED (ENGINE)'); ok(al, 'Allocated (engine) tile'); eq(al.value, '68,995 SAR', 'allocated'); ok(String(al.sub).indexOf('6 of 6 plans funded') > 0, 'tile sub: ' + al.sub); eq(al.color, GREEN, 'tile colour');
  eq(v.tile('EXECUTABLE').value, '6', 'the Executable tile is the cockpit count (unchanged)');
  deq(v.funnel.slice(3), [['Seats ' + ARROW + ' plans (6)', 0.6], ['Plans ' + ARROW + ' executable (6)', 1]], 'funnel stages');
  ok(v.funnelSub.indexOf('plans 6  >  executable 6') > 0, 'funnel subtitle: ' + v.funnelSub);
  eq(v.allocTitle, 'ALLOCATION ' + DASH + " the engine's tickets, mirrored", 'ALLOCATION title');
  const co = v.kv('Plans in the sizing cohort');
  eq(co.label, 'Plans in the sizing cohort (basis: engine tickets)', 'cohort row names its basis');
  ok(co.value.indexOf('Engine tickets: 6 ' + DOT + ' 68,995 SAR (feed EXECUTABLE, valid until 10:42 Riyadh) ' + DOT + ' ITRN.US 12,199, DDI.US 12,334, CRC.US 8,527, GLPI.US 12,133, EFC.US 11,467, INDV.US 12,335') === 0, 'cohort row: ' + co.value);
  eq(v.kv('Allocated').value, '68,995 SAR ' + DOT + ' 3,752 SAR left', 'Allocated / cash after');
  ok(v.kv('Unfunded plans').value.indexOf('not re-planned') === 0, 'needs row');
  ok(v.funding.indexOf('Engine tickets mirrored (6, 68,995 SAR)') >= 0 && v.funding.indexOf('No funding need on the production rule') < 0, 'funding block: ' + v.funding.slice(0, 200));
  eq(b.ret.executable, 6, 'return value: executable');
  return b;
}
/* T2b: engine tickets are mirrored as issued (no cohort cap drops one) */
function checkEngineCaps(src) {
  const v = t10View(build(src, 't10', { board: BOARDS.engineCaps }));
  deq(v.seats.map(s => s.alloc).slice(0, 6), [12199, 12334, 8527, 12133, 11467, 12335], 'all six tickets with three in one sector');
  ok(v.kv('Plans in the sizing cohort').value.indexOf('Engine tickets: 6 ' + DOT + ' 68,995 SAR') === 0, 'cohort row');
}
/* T2c: an engine board whose release is no longer live keeps the tickets but not the green; an active seat the engine left
 * without any label reads 'not funded by the engine'; the combined build renders the same Top-10 page */
function checkEngineEdges(src) {
  for (const [name, opts, tag] of [['NOT_ACTIONABLE', { board: BOARDS.todayNA }, 'NOT ACTIONABLE'], ['EXPIRED 11:00', { board: BOARDS.today, now: Date.parse('2026-11-02T08:00:00Z') }, 'EXPIRED']]) {
    const v = t10View(build(src, 't10', opts));
    const al = v.tile('ALLOCATED (ENGINE)');
    ok(al && al.color === '#898781' && String(al.sub).indexOf('feed ' + tag + ' - ') === 0, name + ': grey engine tile with the feed state: ' + JSON.stringify(al));
    ok(v.kv('Plans in the sizing cohort').value.indexOf('Engine tickets: 6 ' + DOT + ' 68,995 SAR (feed ' + tag) === 0, name + ': cohort row names the feed: ' + v.kv('Plans in the sizing cohort').value.slice(0, 90));
  }
  const u = t10View(build(src, 't10', { board: BOARDS.engineActiveUnfunded }));
  eq(u.title, 'SEATS ' + DASH + ' 10 ' + DOT + ' 6 engine tickets ' + DOT + ' 1 not funded ' + DOT + ' 3 grace-held', 'active-unfunded header');
  ok(u.seats[6].flags.indexOf('not funded by the engine') === 0 && u.seats[6].alloc === '' && u.seats[6].statusColor === NAVY, 'active-unfunded seat: ' + JSON.stringify(u.seats[6]));
  const both = build(src, 'both', { board: BOARDS.today }), alone = build(src, 't10', { board: BOARDS.today });
  deq(both.sheets['Top_10_Investments_v2'], alone.sheets['Top_10_Investments_v2'], 'Refresh both renders the same Top-10 page as the page alone');
}
/* T3: research board (strict): the seven ACTIVE seats sized by the unchanged water-fill, the FAST-TRACK seat suspended */
function checkResearchBoard(src) {
  const b = build(src, 't10', { board: BOARDS.research });
  const v = t10View(b);
  eq(v.title, 'SEATS ' + DASH + ' 10 ' + DOT + ' 7 plans ' + DOT + ' 1 fast-track suspended ' + DOT + ' 2 grace-held', 'research header');
  const ft = v.seats[9];
  eq(ft.sym, '2020.HK'); ok(ft.flags.indexOf('fast-track ' + DOT + ' sizing suspended (strict)') === 0, 'suspended flag: ' + ft.flags);
  eq(ft.alloc, '', 'the suspended seat is not sized'); eq(ft.statusColor, AMBER, 'suspended amber');
  const o = t10View(build(ORACLE, 't10', { board: BOARDS.researchAsFastTrack }));
  deq(v.seats.slice(0, 7).map(s => [s.sym, s.alloc, s.qty, s.risk, s.gain]), o.seats.slice(0, 7).map(s => [s.sym, s.alloc, s.qty, s.risk, s.gain]), 'the v1.2.30 water-fill of the same seats');
  ok(v.seats.slice(0, 7).filter(s => typeof s.alloc === 'number' && s.alloc > 0).length >= 5, 'the active seats are sized');
  for (const k of ['Allocated', 'Expected gain, 3-month plan', 'Risk if every initial stop is hit', 'Deployable']) eq(v.kv(k).value, o.kv(k).value, 'ALLOCATION row ' + k);
  const co = v.kv('Plans in the sizing cohort');
  eq(co.label, 'Plans in the sizing cohort (basis: research mirror)', 'research basis named');
  ok(co.value.indexOf('7 ' + DOT + ' ITRN.US (') === 0 && /active seats, fast-track sizing suspended \(strict\)$/.test(co.value), 'cohort row: ' + co.value);
  eq(v.tile('SIZING PREVIEW ONLY').value, o.tile('SIZING PREVIEW ONLY').value, 'preview tile = the water-fill total');
  eq(v.tile('HELD SEATS').sub, '7 active ' + DOT + ' 1 fast-track ' + DOT + ' 2 grace-held', 'Held seats tile');
  deq(v.funnel.slice(3), [['Seats ' + ARROW + ' plans (7)', 0.7], ['Plans ' + ARROW + ' executable (0)', 0]], 'funnel');
  for (const s of v.seats) ok(s.flags.indexOf('engine ticket') < 0 && s.flags.indexOf('not funded by the engine') < 0, 'no engine wording on a research board');
  eq(v.allocTitle, 'ALLOCATION ' + DASH + " research mirror of the engine's conviction sizing", 'research ALLOCATION title kept');
  // not strict: the FAST-TRACK seat joins the cohort; a 'sizing suspended' Advisor Note still suspends
  const ns = t10View(build(src, 't10', { board: BOARDS.nonStrict }));
  eq(ns.title, 'SEATS ' + DASH + ' 10 ' + DOT + ' 8 plans ' + DOT + ' 2 grace-held', 'not strict header');
  ok(ns.seats[9].flags.indexOf('fast-track') < 0 && ns.seats[9].alloc !== '', 'not strict: the FAST-TRACK seat is a plan: ' + JSON.stringify(ns.seats[9]));
  ok(/fast-track included \(not strict\)$/.test(ns.kv('Plans in the sizing cohort').value), 'not strict cohort row');
  const nt = t10View(build(src, 't10', { board: BOARDS.noteSuspended }));
  eq(nt.title, 'SEATS ' + DASH + ' 10 ' + DOT + ' 7 plans ' + DOT + ' 1 fast-track suspended ' + DOT + ' 2 grace-held', 'note-suspended header');
  ok(nt.seats[9].flags.indexOf('fast-track ' + DOT + ' sizing suspended') === 0 && nt.seats[9].flags.indexOf('(strict)') < 0, 'note-suspended flag: ' + nt.seats[9].flags);
  // the research cohort keeps the cohort caps (as v1.2.30 did for its cohort): a third Real Estate seat is outside them
  const rc = t10View(build(src, 't10', { board: BOARDS.researchCaps }));
  eq(rc.title, 'SEATS ' + DASH + ' 10 ' + DOT + ' 6 plans ' + DOT + ' 1 outside the cohort caps ' + DOT + ' 1 fast-track suspended ' + DOT + ' 2 grace-held', 'capped header');
  ok(rc.seats[6].flags.indexOf('outside the sizing cohort (cohort caps)') === 0 && rc.seats[6].alloc === DASH, 'the capped seat: ' + JSON.stringify(rc.seats[6]));
}
/* T4: 'Held - ..' (16_Decision_Top10 v1.20.0) reads research on both boards */
const CLASS_TABLE = [
  [{ 'Ticket SAR': 12199, Stability: 'ACTIVE (day 6)' }, true, true, 'engine'],
  [{ 'Ticket SAR': '12,199', Stability: 'GRACE (1/3 missed)' }, true, true, 'engine'],
  [{ 'Ticket SAR': 0, Stability: 'GRACE (1/3 missed)', 'Funds From': RESEARCH_FF }, true, true, 'grace'],
  [{ 'Ticket SAR': DASH, Stability: 'GRACE (2/3 missed) - outage pause 1/5' }, false, false, 'grace'],
  [{ 'Ticket SAR': 0, Stability: 'FAST-TRACK (day 1)', 'Funds From': RESEARCH_FF }, true, true, 'suspended'],
  [{ 'Ticket SAR': 0, Stability: 'FAST-TRACK (day 1)', 'Funds From': RESEARCH_FF }, false, true, 'research'],
  [{ 'Ticket SAR': 0, Stability: 'FAST-TRACK (day 1)', 'Funds From': RESEARCH_FF }, false, false, 'active'],
  [{ Stability: 'FAST-TRACK (day 2, 1/3 confirmed)', 'Advisor Note': 'FAST-TRACK (day 2, 1/3 confirmed) ' + DASH + ' sizing suspended under strict until confirmed' }, false, false, 'suspended'],
  [{ Stability: 'FAST-TRACK (SIZING SUSPENDED)' }, false, false, 'suspended'],
  [{ 'Ticket SAR': 0, Stability: 'ACTIVE (day 3)', 'Funds From': 'Held ' + DASH + ' earnings in 0d' }, true, false, 'research'],
  [{ 'Ticket SAR': 0, Stability: 'ACTIVE (day 3)', 'Funds From': 'Held ' + DASH + ' owner rule' }, true, true, 'research'],
  [{ 'Ticket SAR': 0, Stability: 'NEW (confirmed 3/3)', 'Funds From': 'Held ' + DASH + ' ticket cap 2' }, false, false, 'research'],
  [{ 'Ticket SAR': 0, Stability: 'ACTIVE (day 2)', 'Funds From': RESEARCH_FF }, true, true, 'research'],
  [{ 'Ticket SAR': 0, Stability: 'ACTIVE (day 2)', 'Funds From': RESEARCH_FF }, true, false, 'active'],
  [{ 'Ticket SAR': 0, Stability: 'ACTIVE (day 2)', 'Funds From': 'Unfunded (no deployable capital)' }, true, true, 'active'],
  [{ 'Ticket SAR': DASH, Stability: 'NEW (confirmed 3/3)', 'Funds From': DASH }, true, true, 'active'],
  [{}, true, true, 'active'], [null, false, false, 'active']
];
function checkHeld(src) {
  const env = makeEnv(src, {});
  ok(typeof env.ctx.dp2SeatClass_ === 'function', 'dp2SeatClass_ defined');
  for (const [seat, strict, engineBoard, want] of CLASS_TABLE) eq(env.ctx.dp2SeatClass_(seat, strict, engineBoard), want, 'class of ' + JSON.stringify([seat, strict, engineBoard]));
  const v = t10View(build(src, 't10', { board: BOARDS.held }));
  ok(v.seats[6].flags.indexOf('research ' + DOT + ' not sized (Held ' + DASH + ' earnings in 0d)') === 0, 'held flag: ' + v.seats[6].flags);
  eq(v.title, TODAY_HEADER, 'held header on the engine board');
  const r = t10View(build(src, 't10', { board: BOARDS.researchHeld }));
  eq(r.title, 'SEATS ' + DASH + ' 10 ' + DOT + ' 6 plans ' + DOT + ' 1 research ' + DOT + ' 1 fast-track suspended ' + DOT + ' 2 grace-held', 'held header on a research board');
  eq(r.seats[0].alloc, '', 'the held seat is not sized'); ok(r.seats[0].flags.indexOf('research ' + DOT + ' not sized (Held ' + DASH + ' earnings in 0d)') === 0, 'held research flag');
  ok(r.kv('Plans in the sizing cohort').value.indexOf('6 ' + DOT + ' DDI.US (') === 0, 'the held seat is out of the cohort');
}
/* T5: Portfolio_Decision_v2 action 3 follows the feed */
function pdActions(src, opts) { return pdView(build(src, 'pd', opts)).actions; }
function checkAction3(src) {
  const a = pdActions(src, { board: BOARDS.today });
  ok(a.indexOf(ACTION3_NEW) >= 0, 'EXECUTABLE + engine tickets: ' + JSON.stringify(a));
  ok(!a.some(x => x.indexOf('No board-originated capital') === 0), 'the contradicting wording is gone');
  for (const [name, opts] of [['NOT_ACTIONABLE', { board: BOARDS.todayNA }], ['SYNC RUNNING', { board: BOARDS.todaySuper }], ['EXPIRED 11:00', { board: BOARDS.today, now: Date.parse('2026-11-02T08:00:00Z') }],
                              ['DP2_COHORT_LEGACY', { board: BOARDS.today, props: { [LEGACY]: '1' } }], ['no engine ticket, EXECUTABLE', { board: BOARDS.researchExec }]]) {
    const x = pdActions(src, opts);
    ok(x.indexOf(ACTION3_OLD) >= 0 && x.every(t => t.indexOf('Board: ') !== 0), name + ': the v1.2.28 text: ' + JSON.stringify(x));
  }
  const pass = clone(FIX.s1); pass.title = ['S-1 GATE', 'verdict: PASS']; pass.criteria.forEach(c => { c[2] = 'PASS'; });
  const p = pdActions(src, { board: BOARDS.today, s1: pass });
  ok(p.every(t => t.indexOf('Board: ') !== 0 && t.indexOf('No board-originated capital') !== 0), 'S-1 PASS: no action 3');
}
/* T6: D4 - an order IBKR lists as REPLACED but still working (remaining 174) counts as working */
function checkD4(src) {
  const v = pdView(build(src, 'pd', { board: BOARDS.today }));
  eq(v.frstStatus, 'working since 2026-10-26T06:23:34Z', 'FRST order status');
  ok(v.actions.every(t => t.indexOf('unprotected') < 0 && t.indexOf('REPLACED with no working successor') < 0), 'no false alarm: ' + JSON.stringify(v.actions));
  eq(v.limit('Brackets working on every equity lot'), '1 of 1 ' + DOT + ' pass', 'brackets limit');
  const gone = clone(FIX.ibkr_capture); gone.json.orders.orders.forEach(o => { o.remaining_shares_qty = 0; });
  const g = pdView(build(src, 'pd', { board: BOARDS.today, capture: gone }));
  ok(g.actions.some(t => t.indexOf('FRST.US bracket: IBKR lists the 17.90 / 14.56 pair as REPLACED with no working successor') === 0), 'nothing remaining still reads replaced');
}
/* T6b (Codex P1 on #896): a partly filled stop protects only its remaining shares */
function partialCapture() {
  const c = clone(FIX.ibkr_capture);
  c.json.orders.orders.forEach(o => { o.order_status = 'SUBMITTED'; });
  c.json.orders.orders[1].remaining_shares_qty = 100;   // the 14.56 stop: 74 of 174 filled, 100 still working
  return c;
}
function checkCoverage(src) {
  const env = makeEnv(src, {});
  ok(typeof env.ctx.dp2CoverQty_ === 'function', 'dp2CoverQty_ defined');
  for (const [o, use, want] of [[{ total_shares_qty: 174, remaining_shares_qty: 100 }, true, 100], [{ total_shares_qty: 174, remaining_shares_qty: 100 }, false, 174],
                                [{ total_shares_qty: 174 }, true, 174], [{ total_shares_qty: 100, remaining_shares_qty: 150 }, true, 100], [{ remaining_shares_qty: 50 }, true, 50],
                                [{ total_shares_qty: 174, remaining_shares_qty: -5 }, true, 0], [{ total_shares_qty: '174', remaining_shares_qty: '20' }, true, 20], [{}, true, null], [null, true, null]]) {
    eq(env.ctx.dp2CoverQty_(o, use), want, 'coverage of ' + JSON.stringify([o, use]));
  }
  const v = pdView(build(src, 'pd', { board: BOARDS.today, capture: partialCapture() }));
  eq(v.frstStatus, 'working since 2026-10-26T06:23:34Z ' + DASH + " stop covers 100 of the position's 174 sh (partial)", 'partly filled stop: partial coverage');
  eq(v.limit('Brackets working on every equity lot'), '0 of 1 ' + DOT + ' verify', 'brackets limit flags the gap');
  const full = pdView(build(src, 'pd', { board: BOARDS.today }));
  eq(full.frstStatus, 'working since 2026-10-26T06:23:34Z', 'remaining = total: full coverage as before');
}
/* T7: v1.2.30 certification expires with the capture */
function checkCertification(src) {
  const early = build(src, 'pd', { board: BOARDS.today, now: Date.parse('2026-11-02T05:50:00Z') });
  eq(early.ret.certified, true, '08:50: CERTIFIED (capture 08:41 + 14 min not passed, feed EXECUTABLE same run, PD ok)');
  eq(pdView(early).chip, 'CERTIFIED', 'chip at 08:50');
  const late = build(src, 'pd', { board: BOARDS.today });
  eq(late.ret.certified, false, '09:24: UNCERTIFIED');
  const v = pdView(late);
  eq(v.chip, 'UNCERTIFIED', 'chip at 09:24');
  ok(v.reason.indexOf('IBKR evidence EXPIRED at 08:55 (capture 08:41 + 14 min)') >= 0, 'reason: ' + v.reason);
  ok(v.right.indexOf('page built 2026-11-02 09:24 ' + DOT + ' decision (Portfolio_Decision run) 2026-11-02 08:46:00 ' + DOT + ' IBKR capture 08:41 expired 08:55') === 0, 'header: ' + v.right);
  eq(build(src, 'pd', { board: BOARDS.todayNA, now: Date.parse('2026-11-02T05:50:00Z') }).ret.certified, false, 'a NOT_ACTIONABLE feed for the same run never certifies');
}
/* T1: the kill switch restores the v1.2.30 build byte for byte (both pages, three boards) */
function checkLegacyIdentity(src, spellings) {
  for (const bd of [BOARDS.today, BOARDS.research, BOARDS.nonStrict]) {
    for (const which of ['t10', 'pd', 'both']) {
      const n = build(src, which, { board: bd, props: { [LEGACY]: '1' } });
      const o = build(ORACLE, which, { board: bd, props: { [LEGACY]: '1' } });
      deq(norm(n), norm(o), 'DP2_COHORT_LEGACY=1 = v1.2.30 (' + bd.name + ', ' + which + ')');
    }
  }
  const pc = partialCapture();   // the Codex P1 coverage fix is behind the same switch
  deq(norm(build(src, 'pd', { board: BOARDS.today, capture: pc, props: { [LEGACY]: '1' } })), norm(build(ORACLE, 'pd', { board: BOARDS.today, capture: pc, props: { [LEGACY]: '1' } })),
      'DP2_COHORT_LEGACY=1 = v1.2.30 on a partly filled stop');
  const o = norm(build(ORACLE, 't10', { board: BOARDS.today }));
  for (const sp of spellings) deq(norm(build(src, 't10', { board: BOARDS.today, props: { [LEGACY]: sp } })), o, 'legacy spelling ' + JSON.stringify(sp));
}

function battery() {
  const digest = [];
  test('T0 oracles: v1.2.30 = the eleven prepared steps on the live v1.2.28 (SHA-256 pinned); version lockstep 1.2.31; the three files compile', () => {
    eq(sha256(BASE), V1228_SHA256, 'base'); eq(sha256(ORACLE), V1230_SHA256, 'oracle');
    eq(applySteps(BASE), ORACLE, 'steps forward = oracle'); eq(unapplySteps(ORACLE), BASE, 'steps reversed = base');
    ok(/^ \* Version: 1\.2\.3[23] \(2026-11-03\)$/m.test(NEW), 'header Version line 1.2.32 or later');
    const S1232 = v1232Of(NEW);
    ok(/^ \* Version: 1\.2\.32 \(2026-11-03\)$/m.test(S1232) && S1232.indexOf("var DP2_VERSION = '1.2.32';   // lockstep with the header") > 0, 'v1.2.32 lockstep');
    eq(sha256(S1231), V1231_SHA256, 'rebuilt v1.2.31');
    ok(/^ \* Version: 1\.2\.31 \(2026-11-02\)$/m.test(S1231) && S1231.indexOf("var DP2_VERSION = '1.2.31';   // lockstep with the header") > 0, 'rebuilt v1.2.31 lockstep');
    ok(/^ \* Version: 1\.2\.30 \(2026-10-28\) - 1\.2\.30: /m.test(ORACLE) && ORACLE.indexOf("var DP2_VERSION = '1.2.30';") > 0, 'oracle is 1.2.30');
    ok(/^ \* Version: 1\.2\.28 \(2026-10-26\)$/m.test(BASE) && BASE.indexOf("var DP2_VERSION = '1.2.28';") > 0, 'base is 1.2.28');
    for (const s of [NEW, ORACLE, BASE]) new vm.Script(s, { filename: '27_Decision_Pages_v2.gs' });
  });

  test('T1 kill switch DP2_COHORT_LEGACY (1/on/true/yes, any case, trimmed): both pages, every cell / format / chart / call / return value = the v1.2.30 build on three boards; other spellings, absent, fault = the new view', () => {
    checkLegacyIdentity(NEW, ['on', 'true', 'yes', 'TRUE', ' On ', 'Yes', '1 ']);
    const def = norm(build(NEW, 't10', { board: BOARDS.today }));
    const oracle = norm(build(ORACLE, 't10', { board: BOARDS.today }));
    ok(JSON.stringify(def) !== JSON.stringify(oracle), 'the default differs from v1.2.30 on the engine board');
    for (const sp of ['0', 'off', 'false', '', 'no', 'legacy', 'onn', 'enable']) deq(norm(build(NEW, 't10', { board: BOARDS.today, props: { [LEGACY]: sp } })), def, 'not legacy: ' + JSON.stringify(sp));
    deq(norm(build(NEW, 't10', { board: BOARDS.today, propsThrow: true })), def, 'a property fault reads the new view');
    const env = makeEnv(NEW, { props: { [LEGACY]: 'on' } });
    eq(env.ctx.dp2CohortLegacy_(), true, 'helper reads on');
    digest.push(sha256(JSON.stringify(def)));
  });

  test('T2 engine board (today 09:24): SEATS - 10 . 6 engine tickets . 1 research . 3 grace-held; Alloc = Ticket SAR, Buy qty = Shares; class flags; ALLOCATION Engine tickets: 6 . 68,995 SAR; only the expected rows differ from v1.2.30', () => {
    const b = checkEngineBoard(NEW);
    checkEngineCaps(NEW);
    checkEngineEdges(NEW);
    // scope: compared with the v1.2.30 build of the same board, only the tiles, the seats block and the ALLOCATION block move
    const o = build(ORACLE, 't10', { board: BOARDS.today });
    const v = t10View(b), ov = t10View(o);
    eq(ov.title, 'SEATS ' + DASH + ' 10 ' + DOT + ' 0 plans ' + DOT + ' 3 grace-held', 'v1.2.30 header (the owner\'s read)');
    const allowed = new Set();
    const tileRow = v.tile('HELD SEATS').row; for (let r = tileRow; r <= tileRow + 2; r++) allowed.add(r);
    for (let r = v.titleRow; r <= v.hdr + 10; r++) allowed.add(r);
    for (let r = v.allocTitleRow; r <= v.aTop + 11; r++) allowed.add(r);
    const footer = firstRow(v.sh, 1, x => /^Decision Pages v2 1\.2\.3[12]/.test(x)); allowed.add(footer);
    const nb = norm(b), no = norm(o);
    const diffRows = new Set();
    const cn = new Map(nb.sheets['Top_10_Investments_v2'].cells.map(x => [x.split('=')[0], x])), co = new Map(no.sheets['Top_10_Investments_v2'].cells.map(x => [x.split('=')[0], x]));
    for (const k of new Set(Array.from(cn.keys()).concat(Array.from(co.keys())))) if (cn.get(k) !== co.get(k)) diffRows.add(+k.split(':')[0]);
    ok(diffRows.size > 0, 'the page changed');
    for (const r of diffRows) ok(allowed.has(r), 'unexpected change on Top_10_Investments_v2 row ' + r + ': ' + val(v.sh, r, 1));
    deq(nb.sheets['Top_10_Investments_v2'].merges, no.sheets['Top_10_Investments_v2'].merges, 'same merges');
    deq(nb.sheets['Top_10_Investments_v2'].rowH, no.sheets['Top_10_Investments_v2'].rowH, 'same row heights');
    const cnh = nb.sheets['Top_10_Investments_v2'].charts.map(c => JSON.stringify(c)), coh = no.sheets['Top_10_Investments_v2'].charts.map(c => JSON.stringify(c));
    eq(cnh.length, coh.length, 'same chart count');
    for (let i = 0; i < cnh.length; i++) if (cnh[i] !== coh[i]) ok(/Decision funnel/.test(cnh[i]), 'only the funnel chart changes: ' + cnh[i].slice(0, 120));
    // Portfolio_Decision_v2 on the same board: only action 3 (and the footer version) differ
    const pn = norm(build(NEW, 'pd', { board: BOARDS.today })), po = norm(build(ORACLE, 'pd', { board: BOARDS.today }));
    const pnc = new Map(pn.sheets['Portfolio_Decision_v2'].cells.map(x => [x.split('=')[0], x])), poc = new Map(po.sheets['Portfolio_Decision_v2'].cells.map(x => [x.split('=')[0], x]));
    const pdv = pdView(build(NEW, 'pd', { board: BOARDS.today }));
    for (const k of new Set(Array.from(pnc.keys()).concat(Array.from(poc.keys())))) {
      if (pnc.get(k) === poc.get(k)) continue;
      const [r, c] = k.split(':').map(Number);
      ok(c === 1 && /^\d+\. Board: 6 engine tickets/.test(String(val(pdv.sh, r, 1))), 'unexpected change on Portfolio_Decision_v2 ' + k + ': ' + String(val(pdv.sh, r, c)).slice(0, 120));
    }
    digest.push(sha256(JSON.stringify(nb)));
  });

  test('T3 research board (no engine ticket, strict): the seven ACTIVE seats sized by the unchanged water-fill (= v1.2.30 on the same seats as FAST-TRACK), the FAST-TRACK seat suspended; not strict -> a plan; a sizing-suspended note suspends', () => {
    checkResearchBoard(NEW);
    digest.push(sha256(JSON.stringify(norm(build(NEW, 't10', { board: BOARDS.research })))));
  });

  test('T4 16_Decision_Top10 v1.20.0 Held - .. reads research (class table of ' + CLASS_TABLE.length + ' rows; engine and research boards)', () => {
    checkHeld(NEW);
  });

  test('T5 Portfolio_Decision_v2 action 3: EXECUTABLE + 6 engine tickets -> Board: ..; NOT_ACTIONABLE / SYNC RUNNING / EXPIRED / legacy / no engine ticket -> the v1.2.28 text; S-1 PASS -> none', () => {
    checkAction3(NEW);
    const o = pdActions(ORACLE, { board: BOARDS.today });
    ok(o.indexOf(ACTION3_OLD) >= 0, 'v1.2.30 contradicts the feed (the owner\'s read): ' + JSON.stringify(o));
  });

  test('T6 D4 (1.2.29): FRST.US REPLACED with remaining 174 is working (no unprotected alarm); nothing remaining still reads replaced; a partly filled stop covers only its remaining shares (Codex P1)', () => {
    checkD4(NEW);
    checkCoverage(NEW);
  });

  test('T7 1.2.30 certification expires with the capture (08:50 CERTIFIED, 09:24 UNCERTIFIED EXPIRED at 08:55); NOT_ACTIONABLE never certifies', () => {
    checkCertification(NEW);
  });

  test('T8 golden negatives: the T2-T6 checks fail on the live v1.2.28, the T2-T5 checks on v1.2.30, the T7 check on v1.2.28', () => {
    const checks = { T2: checkEngineBoard, T2b: checkEngineCaps, T2c: checkEngineEdges, T3: checkResearchBoard, T4: checkHeld, T5: checkAction3, T6: checkD4, T6b: checkCoverage, T7: checkCertification };
    for (const k of ['T2', 'T2b', 'T2c', 'T3', 'T4', 'T5', 'T6', 'T6b', 'T7']) fails(() => checks[k](BASE), k + ' on v1.2.28');
    for (const k of ['T2', 'T2b', 'T2c', 'T3', 'T4', 'T5', 'T6b']) fails(() => checks[k](ORACLE), k + ' on v1.2.30');
    const oc = pdView(build(ORACLE, 'pd', { board: BOARDS.today, capture: partialCapture() }));
    eq(oc.frstStatus, 'working since 2026-10-26T06:23:34Z', 'v1.2.30 reads a partly filled stop (100 of 174 remaining) as full coverage');
    checkD4(ORACLE); checkCertification(ORACLE);   // 1.2.29 / 1.2.30 already hold on the oracle
    const bv = pdView(build(BASE, 'pd', { board: BOARDS.today }));
    ok(bv.actions.some(t => t.indexOf('FRST.US bracket: IBKR lists the 17.90 / 14.56 pair as REPLACED with no working successor. If nothing is working, 10,253 SAR is unprotected') === 0), 'v1.2.28 raises the FRST false alarm');
    eq(build(BASE, 'pd', { board: BOARDS.today }).ret.certified, true, 'v1.2.28 certifies on the expired 08:41 capture at 09:24');
  });

  test('T9 mutants: each mutation of the delivered file fails at least one section check', () => {
    const M = [
      ['cohort back to FAST-TRACK', "return co.cls(seat) === (co.basis === 'engine' ? 'engine' : 'active');", "return /FAST-TRACK/.test(dp2Str_(seat && seat['Stability']));", [checkEngineBoard, checkResearchBoard]],
      ['engine allocation ignored (re-sized by the water-fill)', "cohortA.basis === 'engine' ? 'engine' : 'conviction'", "'conviction'", [checkEngineBoard]],
      ['engine Shares ignored', "r.qty = (p.sharesEngine !== null && p.sharesEngine > 0) ? p.sharesEngine : 0;", 'r.qty = 0;', [checkEngineBoard]],
      ['ACTIVE seats flagged grace (the class flag skipped)', "if (!cohort.legacy) { var cflag = dp2SeatFlag_(sCls, s, cohort, !!planObj); if (cflag) flags.push(cflag); }", 'if (false) {}', [checkEngineBoard]],
      ['research read before grace (spec order)', "  if (/^GRACE/i.test(stab)) return 'grace';\n", '', [checkEngineBoard, checkHeld]],
      ['strict token not read', "/stab\\[strict\\]/.test(dp2Str_(t && t.status))", 'false', [checkResearchBoard]],
      ['legacy switch leaking (funnel post-pass unconditional)', 'if (!cohort.legacy) { stages[3] =', 'if (true) { stages[3] =', [n => checkLegacyIdentity(n, [])]],
      ['legacy switch ignored', "function dp2CohortLegacy_() { return /^(1|on|true|yes)$/i.test(", 'function dp2CohortLegacy_() { return false && /^(1|on|true|yes)$/i.test(', [n => checkLegacyIdentity(n, [])]],
      ['action 3 ignoring the feed', "  if (fv.label !== 'EXECUTABLE') return null;\n", '', [checkAction3]],
      ['action 3 under the kill switch', 'if (co.legacy || co.n.engine < 1) return null;', 'if (co.n.engine < 1) return null;', [checkAction3]],
      ['engine tickets capped by the cohort caps', "if (co.basis !== 'engine' && (((sectors[sec]", 'if (true && (((sectors[sec]', [checkEngineCaps]],
      ['D4 reverted', " && !(o.working === true && /REPLACED/.test(o.status) && typeof o.remaining === 'number' && o.remaining > 0)) continue;", ')) continue;', [checkD4]],
      ['certification without the live feed', " && dp2FeedView_(feed, new Date().getTime()).live === true;   // v1.2.30", ';   // v1.2.30', [checkCertification]],
      ['Held not read as research', "if (/^Held\\b/i.test(ff) || (engineBoard", 'if ((engineBoard', [checkHeld]],
      ['engine tile green on a dead feed', 'if (!fv.live) { tiles[tq].color = DP2C.grey;', 'if (false) { tiles[tq].color = DP2C.grey;', [checkEngineEdges]],
      ['capped active seats unaccounted in the header', "if (co.basis !== 'engine' && co.n.active > nPlans)", 'if (false)', [checkResearchBoard]],
      ['coverage from the order total (Codex P1 reverted)', 'qty: dp2CoverQty_(o, coverRem), total:', 'qty: dp2Num_(o.total_shares_qty), total:', [checkCoverage]],
      ['coverage fix leaking past the kill switch', 'var coverRem = !dp2CohortLegacy_();', 'var coverRem = true;', [n => checkLegacyIdentity(n, [])]]
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
    ok(killed >= 5, 'at least five mutants');
    digest.push(String(killed));
  });

  // 2026-11-03: T10 reads the rebuilt v1.2.31 file (S1231); the v1.2.32 diff has its own lint in
  // tests/test_dp2_v1232_preview_label.js.
  test('T10 ES5 / ASCII lint of the added lines; inventory (10 functions + 1 var added to v1.2.28, 0 removed); hunks whose reversal is the v1.2.30 oracle; LF only; WHY block', () => {
    const bl = BASE.split('\n'), nl = S1231.split('\n');
    const baseSet = new Set(bl), newSet = new Set(nl);
    const added = nl.filter(l => !baseSet.has(l));
    ok(added.length >= 150 && added.length <= 260, 'added lines ' + added.length);
    for (const l of added) {
      const code = l.replace(/\/\/.*$/, '');
      ok(!/\b(let|const|class)\s|=>|`|\.\.\.|\.includes\(|\.startsWith\(|\.endsWith\(|\.padStart\(|\.find\(/.test(code) || /^\s*(\*|\/\*)/.test(l), 'ES6 token: ' + l.slice(0, 120));
      if (!/^[\x09\x20-\x7e]*$/.test(l)) {
        // the one tolerated case: a line the prepared steps changed keeps a pre-existing character outside its Find text
        let back = l; for (const [find, repl] of STEPS) back = back.split(repl).join(find);
        ok(baseSet.has(back), 'non-ASCII added line: ' + l.slice(0, 120));
      }
    }
    ok(S1231.indexOf('\r') < 0, 'LF only');
    for (const name of ['dp2SeatClass_', 'dp2CohortLegacy_', 'dp2Cohort_', 'dp2SeatFlag_', 'dp2SeatsTitle_', 'dp2FeedTag_', 'dp2SeatsSub_', 'dp2BoardTicketsAction_', 'dp2CoverQty_']) {
      const start = S1231.indexOf('function ' + name + '(');
      ok(start > 0, 'defined ' + name);
      const body = S1231.slice(start, S1231.indexOf('\n}\n', start) + 3);
      ok(/^[\x00-\x7f]*$/.test(body), 'ASCII ' + name);
      new vm.Script('(function(){' + body + '})');
    }
    const names = (src, re) => { const out = {}; let m; while ((m = re.exec(src))) out[m[1]] = true; return out; };
    const fb = names(BASE, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), fn = names(S1231, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    deq(Object.keys(fb).filter(k => !fn[k]), [], 'top-level functions removed');
    deq(Object.keys(fn).filter(k => !fb[k]).sort(), ['dp2BoardTicketsAction_', 'dp2Cohort_', 'dp2CohortLegacy_', 'dp2CoverQty_', 'dp2FeedTag_', 'dp2SeatClass_', 'dp2SeatFlag_', 'dp2SeatsSub_', 'dp2SeatsTitle_'].sort(), 'top-level functions added at the start of a line');
    ok(S1231.indexOf('} function dp2EvidenceExpiry_(pd, nowMs) {') > 0, 'step 6 keeps dp2EvidenceExpiry_ on the dp2Today_ line (one-line step, as prepared)');
    const vb = names(BASE, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = names(S1231, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq(Object.keys(vb).filter(k => !vn[k]), [], 'vars removed');
    deq(Object.keys(vn).filter(k => !vb[k]), ['DP2_COHORT_LEGACY_PROP'], 'vars added');
    // exactly these v1.2.28 lines are gone: eleven rewritten by the prepared steps (two of them, the version lines, again
    // by 1.2.31) and eleven by 1.2.31; every other base line is still in the file, in order
    const REMOVED = ['* Version: 1.2.28 (2026-10-26)', "var DP2_VERSION = '1.2.28';", 'function dp2Today_() {', 'var mlr = /Last run', 'mapped.push({ symbol: mo',
      'if (!dp2OrderActive_(o.status)) continue;', '/* Plan seats = FAST-TRACK seats in rank order', 'if (!/FAST-TRACK/.test(stability)) continue;',
      'if (out.length >= caps.maxSelected) break;', 'if (((sectors[sec] || 0) >= caps.maxPerSector', 'stop0: (price !== null && atr !== null)',
      "var alloc = dp2Allocate_(plans, cashSar, pv, secCtx, reserve.sar, cfg, 'conviction');", 'alloc.secCtx = secCtx; alloc.reserve = reserve; alloc.cfg = cfg;',
      "var allocP = cfg.sizingMode === 'conviction' ? alloc", 'var planSyms = []; for (var pi = 0;', 'var evFresh = /SENT|OK|FRESH/', "var certified = evFresh && pd.readiness === 'VERIFIED'",
      "' " + DOT + " IBKR evidence ' + evState", "dp2Header_(sh, row, 'PORTFOLIO DECISION " + DASH + " v2'", "['IBKR evidence freshness ≤ '", 'var isPlan = /FAST-TRACK/.test(stability);',
      'var sc = sh.getRange(srr, 5); var stxt = dp2Str_(sRows[i][3]); sc.setFontColor(/FAST-TRACK/.test(stxt)'];
    const removed = bl.filter(l => !newSet.has(l)).map(l => l.trim());
    eq(removed.length, REMOVED.length, 'base lines replaced');
    for (let i = 0; i < removed.length; i++) ok(removed[i].indexOf(REMOVED[i]) === 0, 'replaced base line ' + i + ': ' + removed[i].slice(0, 120));
    let pos = 0; for (const l of bl) { if (!newSet.has(l)) continue; const at = nl.indexOf(l, pos); ok(at >= pos, 'base line out of order: ' + l.slice(0, 80)); pos = at + 1; }
    const why = S1231.slice(S1231.indexOf(' *   1.2.31: '), S1231.indexOf(' *   1.2.28: '));
    ok(why.length > 3000 && /^[\x09\x0a\x20-\x7e]*$/.test(why), 'WHY block (1.2.31, 1.2.30, 1.2.29) present, newest first, ASCII');
    ok(why.indexOf('1.2.30 (prepared 28 Oct 2026, never installed') > 0 && why.indexOf('1.2.29 (D4; prepared 28 Oct 2026 with 1.2.30, never installed)') > 0, 'the never-installed note');
    for (const l of bl.filter(x => /^ \*   1\.\d+\.\d+: /.test(x))) ok(S1231.indexOf(l) > 0, 'prior WHY entry kept: ' + l.slice(0, 60));
    eq(reverseEdits(S1231), ORACLE, 'hunk reversal = the v1.2.30 oracle');
    ok(EDITS.length >= 10 && EDITS.length <= 40, 'hunks ' + EDITS.length);
    digest.push(String(EDITS.length));
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
console.log('DP2 V1231 T0-T10 x2 identical digest ' + digests[0] + ' | new SHA256 ' + sha256(NEW) + ' | v1.2.30 SHA256 ' + sha256(ORACLE) + ' | v1.2.28 SHA256 ' + sha256(BASE) + ' | ' + Math.round((Date.now() - t0) / 1000) + ' s');
console.log('PASS ' + passed + '/' + total);
