#!/usr/bin/env node
/* tests/test_por_v100.js
 * Harness for apps_script/28_Plan_Of_Record.gs v1.0.1 (v1.0.0 [PLAN OF RECORD], owner review of Tue 3 Nov 2026 01:08
 * Riyadh: 'today morning there was 6 symboles as quailifed for investing, but after that now no one is
 * qualified ... the program should show in consistance beahviour and more stable recommdadtions').
 *
 * The REAL file runs in a vm context with a recording GAS shim (spreadsheet with cells and a write log,
 * PropertiesService with a write log, CacheService, ScriptApp triggers, Utilities.formatDate through Intl time
 * zones, Logger), the 10_My_Portfolio capture reader stubbed with the 2 Nov IBKR working orders, and a frozen
 * clock. The board is the live 2 Nov _Selection_Log (tests/fixtures/por_board_2026-11-02.json): 08:49
 * EXECUTABLE (6 tickets), the withheld and held renders, the 17:39 exits. Top_10_Investments pages are built
 * with the cockpit's own DT10_SELECTED_HEADERS read out of apps_script/16_Decision_Top10.gs, and the feed clock
 * is compared with the cockpit's own dt10UvParse_ run from that file.
 * Sections:
 *   T0  the file compiles, version, ES5 / ASCII lint, function inventory (65 since v1.0.1, prefix por)
 *   T13 v1.0.1 render priority (TFB_RENDER_PRIORITY): off = the v1.0.0 refresh; on = no refresh while the
 *       cockpit marker or a decision-chain claim is younger than 7 minutes; the run claim lives 360 s
 *   T1  gates: POR_MODE off (default, junk, property fault) -> nothing read or written; observe -> ledger and
 *       snapshot only; on -> ledger + page; POR_VALIDITY feed default, session only for 'session'
 *   T2  sessions: US DST / standard time, weekend roll, Tadawul Sun-Thu, LSE BST / GMT, HKEX; venue codes
 *   T3  the 2 Nov plan at 01:40 on 3 Nov (feed withdrawn, validity feed): one plan US|2026-11-02 locked
 *       08:49:43 pre-open, run 41363258387, six tickets in SAR->USD; ITRN / DDI / GLPI / INDV EXECUTED from the
 *       buy date, CRC / EFC EXPIRED; IBKR stops 49.94 / 12.90 / 37.33 against plan stops 48.27 / 12.40 / 35.21;
 *       INDV 'NO STOP AT IBKR' (the expired Day limit never counts); summary 1 plan, 6 tickets, 4 executed
 *   T4  during the US session, before any fill: validity feed -> NOT EXECUTABLE (the afternoon feed releases run
 *       41445971593, not the plan's run); validity session -> OPEN; 16:00 -> WAITING (opens 17:30); 09:00 with
 *       the morning feed -> WAITING; the 17:39 board (grace / hard exits) is shown beside the unchanged plan
 *   T5  lock rule: a newer EXECUTABLE render before the open replaces the plan (pre-open re-lock), one after the
 *       open is ignored (frozen); a render inside an open session locks only when no plan exists
 *   T6  kill switch (release_killed) invalidates unexecuted tickets under session validity; executed stay
 *   T7  lock-time quantity (Codex on #898): only from the snapshot of a refresh at or before the lock (<= 60
 *       min), carried in the ledger; a fill between the lock and the first refresh is EXECUTED (and its missing
 *       stop flagged); a snapshot after the lock or older than 60 min is never the baseline
 *   T8  robustness: Logged At as Date cells == text cells, also in a workbook whose time zone is Auckland (Date cells
 *       read in getSpreadsheetTimeZone); a ledger whose times Sheets turned into Dates keeps the
 *       lock-time quantity; a header without Shares fails closed (no write); missing My_Portfolio / capture reader
 *       degrade to notes; two refreshes are identical
 *   T9  only _Plan_Of_Record, Plan_Of_Record and POR_HOLDINGS_SNAPSHOT are ever written; porSelfTest writes
 *       nothing; triggers install and remove
 *   T10 board capture (Codex on #898): an unlogged pre-open render ('SelLog: unchanged') re-locks the plan with
 *       its own run and shares; a logged board is read once; the plan outlives the board after the open
 *   T11 the ledger is the record: plans outlive the log tail; header drift and inconsistent rows fail closed;
 *       plans age out 4 days after the close
 *   T12 feed expiry (Codex on #898): the cockpit clock (480 min from the first stamp, 15 min skew) and expires=
 *       decide; POR's reading equals the cockpit's dt10UvParse_ on 13 values
 *   T13 POR_IBKR_ALIASES: INDV.US -> SUPN keeps the stop match after the ticker change
 *   T14 contract with 16_Decision_Top10: board headers, status cell B2, 'Last run', 'output:', 'SelLog: +N',
 *       'VERIFIED (run N)', log headers, feed clock constants
 *   T15 merge: a carried in-session lock stays against a later in-session render; a pre-open render first seen
 *       after the open takes its place
 *   mutants: each mutation of the delivered file fails at least one section check
 * The battery runs twice and the two digests must match; the last line is 'PASS k/k' (exit 1 on any failure).
 * Usage: node tests/test_por_v100.js [file.gs]
 */
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const vm = require('vm');
const ROOT = path.join(__dirname, '..');
const SRC_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '28_Plan_Of_Record.gs');
const SRC = fs.readFileSync(SRC_PATH, 'utf8');
const COCKPIT = fs.readFileSync(path.join(ROOT, 'apps_script', '16_Decision_Top10.gs'), 'utf8');
const FIX = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'fixtures', 'por_board_2026-11-02.json'), 'utf8'));
const PROTECTED = ['_Selection_Log', 'My_Portfolio', '_Status', 'Top_10_Investments', 'Portfolio_Decision'];
const OWN = ['_Plan_Of_Record', 'Plan_Of_Record'];
/* A pre-open release of run 41445971593 in the release grammar (scripts/release_decision_feed.py
 * RELEASE_VALUE_RE), constructed: anchor 15:37 so that it expires at the real 23:37, issued 16:48. */
const FEED_B = 'EXECUTABLE | run=41445971593 | 2026-11-02 15:37:12+03:00 | issued=2026-11-02T16:48:00+03:00 | ' +
  'expires=2026-11-02T23:37:12+03:00 | ttl_min=480 | release=41463134866 | attempt=1 | ' +
  'receipts=T10:41445971593,PF:41445971593 | audits=0,0';

let passed = 0, failed = 0;
const DIGEST = [];
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n  ')); }
}
function ok(c, m) { if (!c) throw new Error(m || 'assertion failed'); }
function eq(a, b, m) { if (a !== b) throw new Error((m || 'not equal') + ': ' + JSON.stringify(a) + ' !== ' + JSON.stringify(b)); }

/* ------------------------------------------------------- the cockpit's own code */
function cockpitArray(name) {
  const m = new RegExp('\\nvar ' + name + ' = (\\[[\\s\\S]*?\\]);').exec(COCKPIT);
  if (!m) throw new Error('cockpit array ' + name + ' not found');
  return vm.runInNewContext(m[1]);
}
function cockpitNum(src, name) {
  const m = new RegExp('var ' + name + ' = (\\d+);').exec(src);
  if (!m) throw new Error('constant ' + name + ' not found');
  return +m[1];
}
function cockpitFn(name) {
  const start = COCKPIT.indexOf('\nfunction ' + name + '(');
  if (start < 0) throw new Error('cockpit function ' + name + ' not found');
  let depth = 0, end = -1;
  for (let i = COCKPIT.indexOf('{', start); i < COCKPIT.length; i++) {
    if (COCKPIT[i] === '{') depth++;
    else if (COCKPIT[i] === '}' && --depth === 0) { end = i + 1; break; }
  }
  const ctx = {Date, String, Math, DT10_UV_MAX_AGE_MIN: cockpitNum(COCKPIT, 'DT10_UV_MAX_AGE_MIN'),
    DT10_UV_FUTURE_SKEW_MIN: cockpitNum(COCKPIT, 'DT10_UV_FUTURE_SKEW_MIN')};
  vm.createContext(ctx);
  vm.runInContext(COCKPIT.slice(start, end), ctx);
  return ctx[name];
}
const BOARD_HEADERS = cockpitArray('DT10_SELECTED_HEADERS');

/* ------------------------------------------------------------ the GAS shim */
function fmtParts(ms, tz) {
  const f = new Intl.DateTimeFormat('en-GB', {timeZone: tz, hourCycle: 'h23', year: 'numeric', month: '2-digit',
    day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit'});
  const p = {};
  for (const x of f.formatToParts(new Date(ms))) p[x.type] = x.value;
  return p;
}
function formatDate(date, tz, pattern) {
  const p = fmtParts(date.getTime(), tz);
  let out = '', i = 0;
  while (i < pattern.length) {
    if (pattern[i] === "'") { const j = pattern.indexOf("'", i + 1); out += pattern.slice(i + 1, j); i = j + 1; continue; }
    const tok = pattern.slice(i, i + 4) === 'yyyy' ? 'yyyy' : pattern.slice(i, i + 2);
    if (tok === 'yyyy') { out += p.year; i += 4; }
    else if (tok === 'MM') { out += p.month; i += 2; }
    else if (tok === 'dd') { out += p.day; i += 2; }
    else if (tok === 'HH') { out += p.hour; i += 2; }
    else if (tok === 'mm') { out += p.minute; i += 2; }
    else if (tok === 'ss') { out += p.second; i += 2; }
    else { out += pattern[i]; i += 1; }
  }
  return out;
}

class Rng {
  constructor(sheet, r, c, nr, nc) { Object.assign(this, {sheet, r, c, nr, nc}); }
  getValues() {
    const out = [];
    for (let i = 0; i < this.nr; i++) {
      const row = [];
      for (let j = 0; j < this.nc; j++) {
        const src = this.sheet.cells[this.r - 1 + i];
        const v = src ? src[this.c - 1 + j] : undefined;
        row.push(v === undefined || v === null ? '' : v);
      }
      out.push(row);
    }
    return out;
  }
  setValues(vals) {
    if (vals.length !== this.nr || vals.some(row => row.length !== this.nc)) throw new Error('setValues dimension mismatch on ' + this.sheet.name);
    if (this.r - 1 + this.nr > this.sheet.maxRows || this.c - 1 + this.nc > this.sheet.maxCols) throw new Error('setValues beyond the grid on ' + this.sheet.name);
    for (let i = 0; i < this.nr; i++) {
      const ri = this.r - 1 + i;
      while (this.sheet.cells.length <= ri) this.sheet.cells.push([]);
      for (let j = 0; j < this.nc; j++) this.sheet.cells[ri][this.c - 1 + j] = vals[i][j];
    }
    this.sheet.log('setValues', this.r, this.c, this.nr, this.nc);
    return this;
  }
  clearContent() {
    for (let i = 0; i < this.nr; i++) {
      const row = this.sheet.cells[this.r - 1 + i];
      if (!row) continue;
      for (let j = 0; j < this.nc; j++) row[this.c - 1 + j] = '';
    }
    this.sheet.log('clearContent', this.r, this.c, this.nr, this.nc);
    return this;
  }
  setBackground(v) { this.sheet.log('setBackground', this.r, this.c, this.nr, this.nc, v); for (let i = 0; i < this.nr; i++) this.sheet.bg[(this.r + i) + ':' + this.c] = v; return this; }
  setFontWeight(v) { this.sheet.log('setFontWeight', this.r, this.c, this.nr, this.nc, v); return this; }
  setFontColor(v) { this.sheet.log('setFontColor', this.r, this.c, this.nr, this.nc, v); return this; }
  setNumberFormat(v) { this.sheet.log('setNumberFormat', this.r, this.c, this.nr, this.nc, v); this.sheet.nf[this.r + ':' + this.c + ':' + this.nr + ':' + this.nc] = v; return this; }
}
class Sheet {
  constructor(name, rows, world, maxRows, maxCols) {
    this.name = name; this.world = world; this.cells = rows ? rows.map(r => r.slice()) : [];
    this.maxRows = maxRows || Math.max(this.cells.length, 1); this.maxCols = maxCols || Math.max(1, ...this.cells.map(r => r.length));
    this.hidden = false; this.frozen = 0; this.bg = {}; this.nf = {};
  }
  log(op, ...args) { this.world.writes.push([this.name, op].concat(args)); }
  getName() { return this.name; }
  getLastRow() { for (let i = this.cells.length - 1; i >= 0; i--) if ((this.cells[i] || []).some(v => v !== '' && v !== undefined && v !== null)) return i + 1; return 0; }
  getLastColumn() { let m = 0; for (const r of this.cells) for (let j = (r || []).length - 1; j >= 0; j--) if (r[j] !== '' && r[j] !== undefined && r[j] !== null) { m = Math.max(m, j + 1); break; } return m; }
  getMaxRows() { return this.maxRows; }
  getMaxColumns() { return this.maxCols; }
  getRange(r, c, nr, nc) {
    if (this.world.failReads && this.world.failReads.indexOf(this.name) >= 0) throw new Error('read fault ' + this.name);
    if (nr === undefined) nr = 1; if (nc === undefined) nc = 1;
    if (r < 1 || c < 1 || nr < 1 || nc < 1) throw new Error('bad range ' + [r, c, nr, nc].join(','));
    return new Rng(this, r, c, nr, nc);
  }
  insertRowsAfter(after, n) { this.log('insertRowsAfter', after, n); this.maxRows += n; }
  deleteRows(start, n) { this.log('deleteRows', start, n); this.cells.splice(start - 1, n); this.maxRows -= n; }
  insertColumnsAfter(after, n) { this.log('insertColumnsAfter', after, n); this.maxCols += n; }
  deleteColumns(start, n) { this.log('deleteColumns', start, n); for (const r of this.cells) if (r) r.splice(start - 1, n); this.maxCols -= n; }
  setFrozenRows(n) { this.log('setFrozenRows', n); this.frozen = n; }
  hideSheet() { this.log('hideSheet'); this.hidden = true; }
}
function makeWorld(opts) {
  const world = {writes: [], propWrites: [], sheets: {}, props: Object.assign({}, opts.props || {}), cache: {}, triggers: [], logs: [],
    failReads: opts.failReads || []};
  const add = (name, rows) => { world.sheets[name] = new Sheet(name, rows, world); };
  if (opts.log !== null) add('_Selection_Log', opts.log || buildLog(FIX.renders));
  if (opts.portfolio !== null) add('My_Portfolio', [FIX.portfolio_header].concat(opts.portfolio || FIX.portfolio_after));
  if (opts.board) add('Top_10_Investments', opts.board);
  const status = [];
  for (let i = 0; i < 3; i++) { const row = new Array(13).fill(''); row[0] = 'Page' + i; status.push(row); }
  status[0][11] = 'Global Key'; status[0][12] = 'Value';
  status[1][11] = 'Last Global Update'; status[1][12] = '10/31/2026';
  status[2][11] = 'TFB Decision Feed'; status[2][12] = opts.feed === undefined ? FIX.feeds.withdrawn : opts.feed;
  add('_Status', status);
  for (const s of opts.extraSheets || []) add(s.name, s.rows);
  world.ss = {
    getSheetByName: n => world.sheets[n] || null,
    insertSheet: n => { if (world.sheets[n]) throw new Error('sheet exists ' + n); world.writes.push([n, 'insertSheet']); world.sheets[n] = new Sheet(n, [], world, 1000, 26); return world.sheets[n]; },
    getSpreadsheetTimeZone: () => opts.sheetTz || 'Asia/Riyadh'
  };
  return world;
}
function makeDateClass(nowMs) {
  const RealDate = Date;
  class D extends RealDate {
    constructor(...a) { if (a.length === 0) super(nowMs); else super(...a); }
    static now() { return nowMs; }
  }
  D.UTC = RealDate.UTC; D.parse = RealDate.parse;
  return D;
}
function load(src, world, nowMs, capture) {
  const ctx = {
    console: {log: () => {}}, Math, JSON, String, Number, Object, Array, RegExp, Error, isFinite, isNaN, parseInt, parseFloat,
    Date: makeDateClass(nowMs),
    Logger: {log: m => world.logs.push(String(m))},
    Utilities: {formatDate: (d, tz, p) => formatDate(d, tz, p)},
    PropertiesService: {getScriptProperties: () => ({
      getProperty: k => { if (world.propFault) throw new Error('fault'); return Object.prototype.hasOwnProperty.call(world.props, k) ? world.props[k] : null; },
      setProperty: (k, v) => { if (world.propFault) throw new Error('fault'); world.props[k] = String(v); world.propWrites.push(k); }
    })},
    CacheService: {getScriptCache: () => ({get: k => world.cache[k] || null, put: (k, v) => { world.cache[k] = v; }, remove: k => { delete world.cache[k]; }})},
    ScriptApp: {
      getProjectTriggers: () => world.triggers.slice(),
      deleteTrigger: t => { world.triggers = world.triggers.filter(x => x !== t); },
      newTrigger: h => ({timeBased: () => ({everyMinutes: n => ({create: () => { const t = {h, n, getHandlerFunction: () => h}; world.triggers.push(t); return t; }})})})
    },
    SpreadsheetApp: {getActiveSpreadsheet: () => world.ss}
  };
  if (capture !== null) {
    ctx.pdIbkrLatestCapture_ = () => (capture === undefined ? {ok: true, name: 'TFB_IBKR_Capture_20261103-013507.json', json: JSON.parse(JSON.stringify(FIX.capture))} : capture);
  }
  vm.createContext(ctx);
  vm.runInContext(src, ctx, {filename: '28_Plan_Of_Record.gs'});
  return ctx;
}
function wallMs(text, tz) {   // the instant whose wall clock in tz reads 'yyyy-MM-dd[ H:mm[:ss]]' (DST aware)
  const m = /^(\d{4})-(\d{2})-(\d{2})(?: (\d{1,2}):(\d{2})(?::(\d{2}))?)?$/.exec(text);
  const target = Date.UTC(+m[1], +m[2] - 1, +m[3], +(m[4] || 0), +(m[5] || 0), +(m[6] || 0));
  let guess = target;
  for (let k = 0; k < 4; k++) {
    const p = fmtParts(guess, tz);
    const diff = target - Date.UTC(+p.year, +p.month - 1, +p.day, +p.hour, +p.minute, +p.second);
    if (diff === 0) break;
    guess += diff;
  }
  return guess;
}
function riyadhMs(text) {   // 'yyyy-MM-dd H:mm[:ss]' Riyadh (UTC+3, no DST)
  const m = /^(\d{4})-(\d{2})-(\d{2}) (\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec(text);
  return Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4] - 3, +m[5], +(m[6] || 0));
}
function runInfo(r) {
  const m = /^(\d{4}-\d{2}-\d{2}) (\d{1,2}):(\d{2}):(\d{2})$/.exec(r.at);
  const at = m[1] + ' ' + (m[2].length === 1 ? '0' : '') + m[2] + ':' + m[3] + ':' + m[4];
  return 'Last run ' + at + ' | status: ok | output: ' + r.output + ' | sheets pool 9788 rows | source readiness VERIFIED (run ' + r.run + ') | route v4.19.0';
}
const FIELD = {rank: 'Rank', symbol: 'Symbol', name: 'Name', market: 'Market', sector: 'Sector', ccy: 'Ccy', price: 'Price',
  priceSar: 'Price SAR', zone: 'Entry Zone', ticket: 'Ticket SAR', shares: 'Shares', stop: 'Stop SAR', tp1: 'TP1 SAR', tp2: 'TP2 SAR',
  rel: 'Rel', dq: 'DQ', conf: 'Conf', note: 'Advisor Note', stability: 'Stability'};
function buildLog(renders, asDates, tz) {
  const H = FIX.selection_log_header;
  const idx = n => H.indexOf(n);
  const map = Object.assign({fx: H[9], roi: 'ROI %', eroi: 'Engine ROI %', outcome: 'Outcome'}, FIELD);
  const rows = [H.slice()];
  for (const r of renders) {
    for (const x of r.rows) {
      const row = new Array(H.length).fill('');
      row[idx('Logged At')] = asDates ? new Date(tz ? wallMs(r.at, tz) : riyadhMs(r.at)) : r.at;
      row[idx('Run Info')] = runInfo(r);
      row[idx('Source Page')] = 'Top_10_Investments';
      for (const k of Object.keys(map)) if (x[k] !== undefined) row[idx(map[k])] = x[k];
      rows.push(row);
    }
  }
  return rows;
}
/* Top_10_Investments as the cockpit renders it: title row 1, status line B2 (with the SelLog tail), the panel
 * and KPI rows, the selected table (DT10_SELECTED_HEADERS from 16_), then an ALL QUALIFIED table. */
function buildBoard(r, selLog) {
  const H = BOARD_HEADERS, W = H.length;
  const blank = () => new Array(W).fill('');
  const map = Object.assign({fx: H[6], roi: 'ROI % (TP1)', eroi: 'Engine ROI % (12M)'}, FIELD);
  const rows = [];
  const title = blank(); title[0] = 'TOP 10 INVESTMENTS'; rows.push(title);
  const status = blank(); status[0] = 'Status'; status[1] = runInfo(r) + ' | ' + selLog; rows.push(status);
  for (let i = 0; i < 17; i++) { const b = blank(); if (i === 2) { b[0] = 'Cash Available (SAR)'; b[1] = 70000; } rows.push(b); }
  const sec = blank(); sec[0] = 'SELECTED OPPORTUNITIES'; rows.push(sec);
  rows.push(H.slice());
  for (const x of r.rows) {
    if (x.outcome) continue;
    const row = blank();
    for (const k of Object.keys(map)) if (x[k] !== undefined && H.indexOf(map[k]) >= 0) row[H.indexOf(map[k])] = x[k];
    rows.push(row);
  }
  rows.push(blank());
  const q = blank(); q[0] = 'ALL QUALIFIED (1)'; rows.push(q);
  const qh = blank(); qh[0] = 'Rank'; qh[1] = 'Symbol'; qh[2] = 'Shares'; rows.push(qh);
  const qr = blank(); qr[0] = '1'; qr[1] = 'ZZZ.US'; qr[2] = 500; rows.push(qr);
  return rows;
}
const T = text => riyadhMs(text);
const snapAt = (text, q) => JSON.stringify({v: 1, at: T(text), q: q});
function refresh(opts) {
  const world = makeWorld(opts);
  const ctx = load(opts.src || SRC, world, opts.now, opts.capture);
  if (opts.beforeRun) opts.beforeRun(world, ctx);
  const res = ctx.porRefresh();
  return {world, ctx, res};
}
function ledger(world) {
  const sh = world.sheets._Plan_Of_Record;
  if (!sh) return null;
  const rows = sh.getRange(1, 1, sh.getLastRow(), sh.getLastColumn()).getValues();
  const H = rows[0];
  return rows.slice(1).map(r => { const o = {}; H.forEach((h, i) => { o[h] = r[i]; }); return o; });
}
function ledgerCells(world) { return world.sheets._Plan_Of_Record.cells.map(r => r.slice()); }
function bySym(rows) { const o = {}; for (const r of rows) o[r.Symbol] = r; return o; }
function page(world) {
  const sh = world.sheets.Plan_Of_Record;
  return sh ? sh.getRange(1, 1, sh.getLastRow(), sh.getLastColumn()).getValues() : null;
}
const logTo = ms => buildLog(FIX.renders.filter(r => riyadhMs(r.at) <= ms));

/* ------------------------------------------------------------ the battery */
function battery(src, label) {
  const ON = {POR_MODE: 'on'};
  const SES = {POR_MODE: 'on', POR_VALIDITY: 'session'};
  const AFTER_CLOSE = T('2026-11-03 1:40');

  test(label + 'T13 v1.0.1 render priority: off = v1.0.0; on = no refresh beside a decision render; claim TTL 360 s', () => {
    const ON2 = {POR_MODE: 'on'};
    const iso = ms => new Date(ms).toISOString();
    const marker = ageMin => JSON.stringify({started: iso(AFTER_CLOSE - ageMin * 60000), v: '1.21.0', entry: 'x'});
    const claim = ageMin => JSON.stringify({claim: {act: 't10e', run: '41901407829', at_ms: AFTER_CLOSE - ageMin * 60000}});
    // off (absent, blank, junk): a fresh marker and claim change nothing - the refresh runs as v1.0.0
    for (const g of [undefined, '', 'off', 'enable']) {
      const props = Object.assign({}, ON2, {DT10_RUN_MARKER: marker(2), TFB_DECISION_CHAIN_STATE: claim(2)});
      if (g !== undefined) props.TFB_RENDER_PRIORITY = g;
      const {world, res} = refresh({src, now: AFTER_CLOSE, props});
      ok(!res.skipped && world.writes.length > 0, 'off ' + String(g) + ': the refresh ran');
      eq(world.cache.POR_RUNNING, undefined, 'the claim is released');
    }
    // on: a fresh cockpit marker or a fresh chain claim skips the refresh before anything is read or written
    for (const [name, extra] of [['marker', {DT10_RUN_MARKER: marker(2)}], ['bare ISO marker', {DT10_RUN_MARKER: iso(AFTER_CLOSE - 60000)}],
                                 ['claim', {TFB_DECISION_CHAIN_STATE: claim(3)}]]) {
      const {world, res} = refresh({src, now: AFTER_CLOSE, props: Object.assign({}, ON2, {TFB_RENDER_PRIORITY: 'on'}, extra)});
      eq(res.skipped, 'a Top_10 / Portfolio_Decision render is running', name);
      eq(world.writes.length, 0, name + ': no cell written'); eq(world.propWrites.length, 0, name + ': no property written');
      eq(Object.keys(world.cache).length, 0, name + ': no run claim taken');
    }
    // on: an old marker (a stopped render), an old claim, a future stamp or junk does not block
    for (const [name, extra] of [['stale', {DT10_RUN_MARKER: marker(8), TFB_DECISION_CHAIN_STATE: claim(9)}],
                                 ['future', {DT10_RUN_MARKER: marker(-3)}], ['junk', {DT10_RUN_MARKER: 'x', TFB_DECISION_CHAIN_STATE: '{'}],
                                 ['none', {}]]) {
      let ttl = null;
      const {world, res} = refresh({src, now: AFTER_CLOSE, props: Object.assign({}, ON2, {TFB_RENDER_PRIORITY: 'on'}, extra),
        beforeRun: (w) => { const put = w.cacheTtl = []; }});
      ok(!res.skipped && world.writes.length > 0, name + ': the refresh ran');
    }
    // the claim TTL: 360 s while on, 240 s while off
    const ttls = [];
    const probe = (props) => {
      const world = makeWorld({props});
      const ctx = load(src, world, AFTER_CLOSE);
      ctx.CacheService = {getScriptCache: () => ({get: () => null, put: (k, v, t) => { ttls.push(t); }, remove: () => {}})};
      ctx.porRefresh();
    };
    probe(Object.assign({}, ON2, {TFB_RENDER_PRIORITY: 'on'})); probe(ON2);
    eq(ttls.join(','), '360,240', 'claim TTL');
    // the pure core
    const ctx = load(src, makeWorld({}), AFTER_CLOSE);
    for (const v of ['on', 'ON', ' on ', '1', 'true', 'Yes']) eq(ctx.porPriorityOn_(v), true, v);
    for (const v of [null, undefined, '', 'off', '0', 'enable']) eq(ctx.porPriorityOn_(v), false, String(v));
    const core = (m, st) => ctx.porRenderBusyCore_(m, st, AFTER_CLOSE);
    eq(core(marker(6.9), ''), true, '6.9 min marker'); eq(core(marker(7), ''), false, '7 min marker');
    eq(core('', claim(6.9)), true, '6.9 min claim'); eq(core('', claim(7)), false, '7 min claim');
    eq(core('', JSON.stringify({claim: null})), false, 'no claim'); eq(core(null, null), false, 'nothing');
  });


  test(label + 'T0 compiles, version, ES5 / ASCII, 65 functions prefixed por', () => {
    const ctx = load(src, makeWorld({}), AFTER_CLOSE);
    eq(ctx.POR_VERSION, '1.0.1', 'version');
    ok(!/[^\x00-\x7e]/.test(src.replace(/\r/g, '')), 'ASCII only');
    ok(!/=>|\blet\s|\bconst\s|`|\.startsWith\(|\.includes\(|Object\.assign|\.padStart\(/.test(src), 'ES5 only');
    const fns = src.match(/^function [A-Za-z0-9_]+/mg) || [];
    eq(fns.length, 65, 'function count (62 + 3 in v1.0.1)');
    ok(fns.every(f => /^function por/.test(f)), 'prefix por');
    ok(/Functions: 62, all new \(prefix por\)/.test(src) && /v1\.0\.1: 65 functions, 3 added, all prefixed por/.test(src), 'header inventory');
  });

  test(label + 'T1 gates: off by default, junk and faults are off; observe = ledger + snapshot; on = page', () => {
    for (const props of [{}, {POR_MODE: 'yes'}, {POR_MODE: ' ON '.toLowerCase() + 'x'}]) {
      const {world, res} = refresh({src, now: AFTER_CLOSE, props});
      eq(res.mode, 'off', 'mode'); eq(world.writes.length, 0, 'no writes when off'); eq(world.propWrites.length, 0, 'no property writes when off');
    }
    const fault = makeWorld({props: {POR_MODE: 'on'}}); fault.propFault = true;
    const fctx = load(src, fault, AFTER_CLOSE);
    eq(fctx.porRefresh().mode, 'off', 'property fault is off'); eq(fault.writes.length, 0, 'no writes on fault');
    const obs = refresh({src, now: AFTER_CLOSE, props: {POR_MODE: ' Observe '}});
    eq(obs.res.mode, 'observe'); ok(obs.world.sheets._Plan_Of_Record, 'ledger written'); ok(!obs.world.sheets.Plan_Of_Record, 'no page in observe');
    ok(obs.world.sheets._Plan_Of_Record.hidden, 'ledger hidden');
    eq(obs.res.snapshot, 'saved'); eq(obs.world.propWrites.join(','), 'POR_HOLDINGS_SNAPSHOT', 'snapshot saved in observe');
    const on = refresh({src, now: AFTER_CLOSE, props: ON});
    ok(on.world.sheets.Plan_Of_Record && !on.world.sheets.Plan_Of_Record.hidden, 'page written and visible');
    eq(on.res.validity, 'feed', 'validity default feed');
    eq(refresh({src, now: AFTER_CLOSE, props: SES}).res.validity, 'session');
    eq(refresh({src, now: AFTER_CLOSE, props: {POR_MODE: 'on', POR_VALIDITY: 'sessions'}}).res.validity, 'feed');
  });

  test(label + 'T2 sessions and venues', () => {
    const ctx = load(src, makeWorld({}), AFTER_CLOSE);
    const s = (v, iso) => { const x = ctx.porSessionFor_(v, Date.parse(iso)); return x ? [x.date, new Date(x.openMs).toISOString().slice(0, 16), new Date(x.closeMs).toISOString().slice(0, 16)].join(' ') : null; };
    eq(s('US', '2026-10-30T12:00:00Z'), '2026-10-30 2026-10-30T13:30 2026-10-30T20:00', 'US daylight time');
    eq(s('US', '2026-11-02T12:00:00Z'), '2026-11-02 2026-11-02T14:30 2026-11-02T21:00', 'US standard time');
    eq(s('US', '2026-11-02T21:30:00Z'), '2026-11-03 2026-11-03T14:30 2026-11-03T21:00', 'after the close -> next day');
    eq(s('US', '2026-10-31T12:00:00Z'), '2026-11-02 2026-11-02T14:30 2026-11-02T21:00', 'weekend roll across the DST change');
    eq(s('SR', '2026-10-30T09:00:00Z'), '2026-11-01 2026-11-01T07:00 2026-11-01T12:00', 'Tadawul Friday -> Sunday');
    eq(s('L', '2026-10-23T06:00:00Z'), '2026-10-23 2026-10-23T07:00 2026-10-23T15:30', 'LSE in BST');
    eq(s('L', '2026-11-02T06:00:00Z'), '2026-11-02 2026-11-02T08:00 2026-11-02T16:30', 'LSE in GMT');
    eq(s('HK', '2026-11-02T00:00:00Z'), '2026-11-02 2026-11-02T01:30 2026-11-02T08:00', 'HKEX');
    eq(s('XX', '2026-11-02T00:00:00Z'), null, 'unknown venue');
    const v = sym => ctx.porVenueOf_(sym);
    eq([v('ITRN.US'), v('1050.SR'), v('BBOX.L'), v('BPE.MI'), v('0440.HK'), v('8750.T'), v('XYZ.NS'), v('')].join(','), 'US,SR,L,EU,HK,T,,');
  });

  test(label + 'T3 the 2 Nov plan at 01:40 on 3 Nov: four executed, two expired, INDV unprotected', () => {
    const {world, res} = refresh({src, now: AFTER_CLOSE, props: ON});
    ok(!res.error, 'no error: ' + res.error);
    const L = ledger(world); eq(L.length, 6, 'six tickets');
    const b = bySym(L);
    for (const r of L) {
      eq(r['Plan Key'], 'US|2026-11-02'); eq(r['Locked At (Riyadh)'], '2026-11-02 08:49:43'); eq(r['Lock Rule'], 'pre-open'); eq(r['Lock Source'], 'log');
      eq(r['Source Run'], '41363258387'); eq(r['Session Open (Riyadh)'], '2026-11-02 17:30'); eq(r['Session Close (Riyadh)'], '2026-11-03 00:00');
    }
    eq([b['ITRN.US'].Shares, b['DDI.US'].Shares, b['CRC.US'].Shares, b['GLPI.US'].Shares, b['EFC.US'].Shares, b['INDV.US'].Shares].join(','), '62,244,41,83,268,93');
    eq(b['ITRN.US'].Stop, 48.272); eq(b['ITRN.US']['Entry Low'], 50.896); eq(b['ITRN.US']['Entry High'], 52.9947); eq(b['ITRN.US'].TP1, 61.5333);
    for (const sym of ['ITRN.US', 'DDI.US', 'GLPI.US', 'INDV.US']) eq(b[sym].Status, 'EXECUTED', sym);
    eq(b['ITRN.US']['Executed Qty'], 67); eq(b['ITRN.US']['Status Reason'], '67 sh held, bought 2026-11-02');
    eq(b['CRC.US'].Status, 'EXPIRED'); eq(b['EFC.US'].Status, 'EXPIRED');
    eq(b['CRC.US']['Status Reason'], 'session closed 2026-11-03 00:00 without a fill');
    eq(b['ITRN.US']['IBKR Protection'], 'stop 49.94 (plan 48.27)');
    eq(b['DDI.US']['IBKR Protection'], 'stop 12.90 (plan 12.40)');
    eq(b['GLPI.US']['IBKR Protection'], 'stop 37.33 (plan 35.21)');
    eq(b['INDV.US']['IBKR Protection'], 'NO STOP AT IBKR');
    eq(b['CRC.US']['IBKR Protection'], '');
    eq(b['GLPI.US']['Live Board'], 'not on the board (23:42)');
    eq(b['ITRN.US']['Live Board'], 'GRACE (0/3 missed) (23:42, WITHHELD)');
    const s = res.summary;
    eq([s.plans, s.tickets, s.executed, s.open, s.waiting, s.missingStops].join(','), '1,6,4,0,0,1', 'summary');
    const P = page(world);
    ok(/^TODAY'S PLAN OF RECORD/.test(P[0][0]), 'title');
    ok(P[1][0].indexOf('executed 4') > 0 && P[1][0].indexOf('missing stops 1') > 0 && P[1][0].indexOf('validity feed') > 0, 'status line');
    ok(P[1][0].indexOf('feed NOT_ACTIONABLE(FINAL_PUBLICATION) (run 41445971593)') > 0, 'feed in the status line');
    const hdr = P.findIndex(r => r[0] === 'Session');
    ok(hdr > 0, 'header row');
    const itrn = P.find(r => r[4] === 'ITRN.US');
    eq(itrn[0], '2026-11-02 NYSE/NASDAQ'); eq(itrn[1], '17:30-00:00'); eq(itrn[2], '11-02 08:49 pre-open');
    eq(itrn[6], '50.90 - 52.99 USD'); eq(itrn[9], 48.27); eq(itrn[10], 61.53); eq(itrn[12], 'EXECUTED'); eq(itrn[14], '67 sh @ 52.38');
    const indv = P.find(r => r[4] === 'INDV.US');
    eq(indv[15], 'NO STOP AT IBKR');
    ok(world.sheets.Plan_Of_Record.bg[(P.indexOf(indv) + 1) + ':16'] === '#f4cccc', 'missing stop highlighted');
    eq(world.sheets.Plan_Of_Record.bg[(P.indexOf(itrn) + 1) + ':13'], '#d9ead3', 'executed status colour');
    eq(world.sheets.Plan_Of_Record.nf[(hdr + 2) + ':10:6:3'], '0.00', 'prices with two decimals');
    eq(world.sheets.Plan_Of_Record.frozen, hdr + 1, 'header frozen');
    DIGEST.push(JSON.stringify(L.map(r => [r.Symbol, r.Status, r['IBKR Protection']])));
  });

  test(label + 'T4 during the US session before any fill: feed / session validity, waiting, live board beside the plan', () => {
    const at = T('2026-11-02 18:10');
    const feedRun = refresh({src, now: at, props: ON, feed: FIX.feeds.afternoon_executable, portfolio: FIX.portfolio_before});
    const fb = bySym(ledger(feedRun.world));
    for (const sym of Object.keys(fb)) eq(fb[sym].Status, 'NOT EXECUTABLE', 'feed validity ' + sym);
    eq(fb['ITRN.US']['Status Reason'], 'feed EXECUTABLE (run 41445971593) does not release this plan (run 41363258387)');
    const ses = refresh({src, now: at, props: SES, feed: FIX.feeds.afternoon_executable, portfolio: FIX.portfolio_before});
    const sb = bySym(ledger(ses.world));
    for (const sym of Object.keys(sb)) eq(sb[sym].Status, 'OPEN', 'session validity ' + sym);
    ok(sb['ITRN.US']['Status Reason'].indexOf('buy only inside the entry zone') === 0, 'open reason');
    ok(sb['ITRN.US']['Status Reason'].indexOf('held by session validity') > 0, 'session note');
    eq(sb['GLPI.US']['Live Board'], 'EXIT: hard (17:39)');
    const wait = refresh({src, now: T('2026-11-02 16:00'), props: SES, feed: FIX.feeds.stale, portfolio: FIX.portfolio_before});
    const wb = bySym(ledger(wait.world));
    eq(wb['CRC.US'].Status, 'WAITING'); eq(wb['CRC.US']['Status Reason'], 'market opens 2026-11-02 17:30');
    const morning = refresh({src, now: T('2026-11-02 9:00'), props: ON, feed: FIX.feeds.morning_executable, portfolio: FIX.portfolio_before,
      log: logTo(T('2026-11-02 9:00'))});
    eq(bySym(ledger(morning.world))['EFC.US'].Status, 'WAITING', 'the morning feed releases the plan');
    const stale = refresh({src, now: T('2026-11-02 11:30'), props: ON, feed: FIX.feeds.stale, portfolio: FIX.portfolio_before,
      log: logTo(T('2026-11-02 11:30'))});
    eq(bySym(ledger(stale.world))['EFC.US'].Status, 'NOT EXECUTABLE', 'stale feed, feed validity');
    const live = refresh({src, now: T('2026-11-02 17:45'), props: ON, feed: FIX.feeds.afternoon_executable, portfolio: FIX.portfolio_before,
      log: logTo(T('2026-11-02 17:45'))});
    const lb = bySym(ledger(live.world));
    eq(lb['GLPI.US']['Live Board'], 'EXIT: hard (17:39)'); eq(lb['ITRN.US']['Live Board'], 'GRACE (0/3 missed) (17:39, HELD)');
    eq(lb['ITRN.US'].Shares, 62, 'the plan is unchanged by the 17:39 board');
    DIGEST.push(JSON.stringify([fb['ITRN.US'].Status, sb['ITRN.US'].Status, wb['CRC.US'].Status]));
  });

  test(label + 'T5 lock rule: pre-open re-lock replaces, in-session renders never change a plan', () => {
    const pre = {at: '2026-11-02 16:50:00', output: 'EXECUTABLE', run: '41445971593', rows: [
      Object.assign({}, FIX.renders[1].rows[0], {shares: 70, ticket: 13700})]};
    const during = {at: '2026-11-02 17:45:00', output: 'EXECUTABLE', run: '41445971593', rows: [
      Object.assign({}, FIX.renders[1].rows[0], {shares: 80, ticket: 15700}),
      Object.assign({}, FIX.renders[1].rows[2], {shares: 99})]};
    const renders = FIX.renders.concat([pre, during]).sort((a, b) => riyadhMs(a.at) - riyadhMs(b.at));
    const {world} = refresh({src, now: T('2026-11-02 18:00'), props: SES, log: buildLog(renders), portfolio: FIX.portfolio_before});
    const L = ledger(world);
    eq(L.length, 1, 'the newer pre-open release is the plan (ITRN only)');
    eq(L[0].Shares, 70); eq(L[0]['Locked At (Riyadh)'], '2026-11-02 16:50:00'); eq(L[0]['Replaced Lock'], '2026-11-02 08:49'); eq(L[0]['Source Run'], '41445971593');
    const only = {at: '2026-11-02 18:20:00', output: 'EXECUTABLE', run: '41445971593', rows: [Object.assign({}, FIX.renders[1].rows[2], {shares: 99})]};
    const late = refresh({src, now: T('2026-11-02 18:30'), props: SES, portfolio: FIX.portfolio_before, log: buildLog([FIX.renders[0], only])});
    const ll = ledger(late.world);
    eq(ll.length, 1); eq(ll[0]['Lock Rule'], 'in-session'); eq(ll[0].Shares, 99);
    const twice = {at: '2026-11-02 18:25:00', output: 'EXECUTABLE', run: '41445971593', rows: [Object.assign({}, FIX.renders[1].rows[2], {shares: 120})]};
    const frozen = refresh({src, now: T('2026-11-02 18:30'), props: SES, portfolio: FIX.portfolio_before, log: buildLog([only, twice])});
    eq(ledger(frozen.world)[0].Shares, 99, 'first in-session lock stays');
    DIGEST.push(JSON.stringify([L[0].Shares, ll[0].Shares]));
  });

  test(label + 'T6 the release kill switch invalidates unexecuted tickets; executed tickets stay executed', () => {
    const {world} = refresh({src, now: T('2026-11-02 18:10'), props: SES, feed: FIX.feeds.killed});
    const b = bySym(ledger(world));
    eq(b['CRC.US'].Status, 'INVALIDATED'); eq(b['CRC.US']['Status Reason'], 'release kill switch');
    eq(b['ITRN.US'].Status, 'EXECUTED');
  });

  test(label + 'T7 lock-time quantity: only a snapshot at or before the lock (<= 60 min); a fill after the lock counts', () => {
    const older = [['ITRN.US', 'Ituran', 52.0, 10, 50.0, 520, '2026-09-01']].concat(FIX.portfolio_before);
    const pre = refresh({src, now: T('2026-11-02 8:45'), props: ON, feed: FIX.feeds.morning_executable, portfolio: older, log: logTo(T('2026-11-02 8:45'))});
    eq(pre.res.snapshot, 'saved'); eq(ledger(pre.world).length, 0, 'no plan before 08:49');
    const snap = JSON.parse(pre.world.props.POR_HOLDINGS_SNAPSHOT);
    eq(snap.at, T('2026-11-02 8:45')); eq(snap.q['ITRN.US'], 10); eq(snap.q['FRST.US'], 174);
    const first = refresh({src, now: T('2026-11-02 9:00'), props: Object.assign({POR_HOLDINGS_SNAPSHOT: pre.world.props.POR_HOLDINGS_SNAPSHOT}, ON),
      feed: FIX.feeds.morning_executable, portfolio: older, log: logTo(T('2026-11-02 9:00'))});
    const fb = bySym(ledger(first.world));
    eq(fb['ITRN.US']['Held At Lock'], 10, 'baseline from the 08:45 snapshot'); eq(fb['DDI.US']['Held At Lock'], 0, 'zero is recorded');
    const rows = ledgerCells(first.world);
    const added = [['ITRN.US', 'Ituran', 52.2, 77, 52.0, 4019, '2026-09-01']].concat(FIX.portfolio_before);
    const lb = bySym(ledger(refresh({src, now: AFTER_CLOSE, props: ON, portfolio: added, extraSheets: [{name: '_Plan_Of_Record', rows}]}).world));
    eq(lb['ITRN.US'].Status, 'EXECUTED'); eq(lb['ITRN.US']['Executed Qty'], 67); eq(lb['ITRN.US']['Status Reason'], '67 sh held, since the lock');
    eq(lb['ITRN.US']['Held At Lock'], 10, 'carried');
    eq(bySym(ledger(refresh({src, now: AFTER_CLOSE, props: ON, portfolio: older, extraSheets: [{name: '_Plan_Of_Record', rows}]}).world))['ITRN.US'].Status,
      'EXPIRED', 'an older holding is not a fill');
    const none = refresh({src, now: T('2026-11-02 9:00'), props: ON, feed: FIX.feeds.morning_executable, portfolio: older, log: logTo(T('2026-11-02 9:00'))});
    eq(bySym(ledger(none.world))['ITRN.US']['Held At Lock'], '', 'no snapshot: unknown baseline');
    // Codex on #898: a lock at 18:00 and 100 shares bought before the 18:10 refresh
    const lock = {at: '2026-11-02 18:00:00', output: 'EXECUTABLE', run: '41445971593', rows: [Object.assign({}, FIX.renders[1].rows[3], {shares: 100, ticket: 20000})]};
    const bought = [['CRC.US', 'California Resources Corporation', 49.5, 100, 49.4, 4950, '2026-11-02']].concat(FIX.portfolio_before);
    const crc = snap => bySym(ledger(refresh({src, now: T('2026-11-02 18:10'), feed: FEED_B, portfolio: bought, log: buildLog([lock]),
      props: Object.assign({POR_HOLDINGS_SNAPSHOT: snap}, ON)}).world))['CRC.US'];
    const c1 = crc(snapAt('2026-11-02 17:55', {'FRST.US': 174}));
    eq(c1.Status, 'EXECUTED', 'the fill after the lock'); eq(c1['Held At Lock'], 0); eq(c1['Status Reason'], '100 sh held, since the lock');
    eq(c1['IBKR Protection'], 'NO STOP AT IBKR', 'and its missing stop is flagged');
    const c2 = crc(snapAt('2026-11-02 18:05', {'CRC.US': 100, 'FRST.US': 174}));
    eq(c2['Held At Lock'], '', 'a snapshot after the lock is never the baseline'); eq(c2['Status Reason'], '100 sh held, bought 2026-11-02');
    const c3 = crc(snapAt('2026-11-02 16:30', {'FRST.US': 174}));
    eq(c3['Held At Lock'], '', 'a snapshot 90 min before the lock is too old'); eq(c3['Status Reason'], '100 sh held, bought 2026-11-02');
    eq(crc('{not json')['Held At Lock'], '', 'malformed snapshot ignored');
    DIGEST.push(JSON.stringify([fb['ITRN.US']['Held At Lock'], c1.Status, c2['Status Reason']]));
  });

  test(label + 'T8 robustness: Date cells, Date ledger times, header drift, missing inputs, determinism', () => {
    const asText = refresh({src, now: AFTER_CLOSE, props: ON});
    const asDate = refresh({src, now: AFTER_CLOSE, props: ON, log: buildLog(FIX.renders, true)});
    eq(JSON.stringify(ledger(asDate.world)), JSON.stringify(ledger(asText.world)), 'Date Logged At == text');
    const nz = 'Pacific/Auckland';
    const nzPf = FIX.portfolio_after.map(r => { const x = r.slice(); x[6] = new Date(wallMs(x[6], nz)); return x; });
    const asNz = refresh({src, now: AFTER_CLOSE, props: ON, sheetTz: nz, log: buildLog(FIX.renders, true, nz), portfolio: nzPf});
    eq(JSON.stringify(ledger(asNz.world).map(r => Object.assign({}, r, {'Updated At (Riyadh)': ''}))),
      JSON.stringify(ledger(asText.world).map(r => Object.assign({}, r, {'Updated At (Riyadh)': ''}))), 'a workbook in Auckland reads the same plan');
    const older = [['ITRN.US', 'Ituran', 52.0, 10, 50.0, 520, '2026-09-01']].concat(FIX.portfolio_before);
    const first = refresh({src, now: T('2026-11-02 9:10'), props: Object.assign({POR_HOLDINGS_SNAPSHOT: snapAt('2026-11-02 8:45', {'ITRN.US': 10})}, ON),
      feed: FIX.feeds.morning_executable, portfolio: older, log: logTo(T('2026-11-02 9:10'))});
    const rows = ledgerCells(first.world);
    const H = rows[0];
    for (const r of rows.slice(1)) for (const h of ['Session Open (Riyadh)', 'Session Close (Riyadh)', 'Locked At (Riyadh)']) r[H.indexOf(h)] = new Date(riyadhMs(r[H.indexOf(h)]));
    const added = [['ITRN.US', 'Ituran', 52.2, 77, 52.0, 4019, '2026-09-01']].concat(FIX.portfolio_before);
    const later = refresh({src, now: AFTER_CLOSE, props: ON, portfolio: added, extraSheets: [{name: '_Plan_Of_Record', rows}]});
    eq(bySym(ledger(later.world))['ITRN.US']['Executed Qty'], 67, 'Date ledger times keep the lock-time quantity');
    eq(bySym(ledger(later.world))['ITRN.US']['Locked At (Riyadh)'], '2026-11-02 08:49:43', 'and the lock');
    const drift = buildLog(FIX.renders); drift[0][FIX.selection_log_header.indexOf('Shares')] = 'Qty';
    const bad = refresh({src, now: AFTER_CLOSE, props: ON, log: drift});
    ok(/header lacks shares/.test(bad.res.error), 'fail closed on header drift'); eq(bad.world.writes.length, 0, 'no write on drift');
    eq(bad.world.propWrites.length, 0, 'no snapshot on drift');
    const noLog = refresh({src, now: AFTER_CLOSE, props: ON, log: null});
    ok(/missing/.test(noLog.res.error) && noLog.world.writes.length === 0, 'no log -> no write');
    const noPf = refresh({src, now: AFTER_CLOSE, props: ON, portfolio: null});
    ok(/My_Portfolio missing/.test(noPf.res.holdings), 'missing portfolio is reported');
    eq(noPf.res.snapshot, 'not saved (holdings unavailable)'); eq(noPf.world.propWrites.length, 0, 'no snapshot without holdings');
    eq(bySym(ledger(noPf.world))['ITRN.US'].Status, 'EXPIRED', 'no holdings -> nothing executed');
    const noReader = refresh({src, now: AFTER_CLOSE, props: ON, capture: null});
    eq(bySym(ledger(noReader.world))['ITRN.US']['IBKR Protection'], 'capture n/a: capture reader not installed (10_My_Portfolio)');
    const noCap = refresh({src, now: AFTER_CLOSE, props: ON, capture: {ok: false, reason: 'no capture in the last 36h'}});
    eq(bySym(ledger(noCap.world))['INDV.US']['IBKR Protection'], 'capture n/a: no capture (no capture in the last 36h)');
    const boardFault = refresh({src, now: AFTER_CLOSE, props: ON, board: buildBoard(FIX.renders[6], 'SelLog: +6'), failReads: ['Top_10_Investments']});
    eq(boardFault.res.board, 'board read failed'); eq(ledger(boardFault.world).length, 6, 'a board fault never stops the refresh');
    const a = refresh({src, now: AFTER_CLOSE, props: ON}), b2 = refresh({src, now: AFTER_CLOSE, props: ON});
    eq(JSON.stringify(page(a.world)), JSON.stringify(page(b2.world)), 'deterministic page');
    const again = load(src, a.world, AFTER_CLOSE); again.porRefresh();
    eq(JSON.stringify(page(a.world)), JSON.stringify(page(b2.world)), 'idempotent on the same workbook (ledger and snapshot carried)');
    ok(a.world.sheets.Plan_Of_Record.maxRows <= page(a.world).length + 5 && a.world.sheets.Plan_Of_Record.maxCols === 18, 'page grid trimmed to its content');
  });

  test(label + 'T9 only its own two sheets and POR_HOLDINGS_SNAPSHOT are written; self test writes nothing; triggers', () => {
    const {world} = refresh({src, now: AFTER_CLOSE, props: ON, board: buildBoard(FIX.renders[6], 'SelLog: +6')});
    const touched = Array.from(new Set(world.writes.map(w => w[0])));
    ok(touched.every(n => OWN.indexOf(n) >= 0), 'touched ' + touched.join(','));
    ok(PROTECTED.every(n => world.writes.every(w => w[0] !== n)), 'protected sheets untouched');
    ok(world.propWrites.length === 1 && world.propWrites[0] === 'POR_HOLDINGS_SNAPSHOT', 'property writes ' + world.propWrites.join(','));
    const ctx = load(src, world, AFTER_CLOSE);
    const w0 = world.writes.length, p0 = world.propWrites.length;
    const st = ctx.porSelfTest();
    eq(world.writes.length, w0, 'self test writes no cell'); eq(world.propWrites.length, p0, 'self test writes no property');
    ok(st[0] === 'Plan of Record 1.0.1', 'self test title');
    ok(st.some(l => /^plan US\|2026-11-02 locked 2026-11-02 08:49 pre-open run 41363258387 tickets 6 \(log\)$/.test(l)), 'self test plan line');
    ok(st.some(l => /^snapshot: 2026-11-03 01:40$/.test(l)) && st.some(l => /^board: board 11-02 23:42 is in the log$/.test(l)), 'self test notes ' + st.join(' / '));
    eq(ctx.porInstallTrigger(), 'installed porRefreshTrigger every 15 min');
    eq(ctx.porInstallTrigger(), 'installed porRefreshTrigger every 15 min (replaced 1)');
    eq(world.triggers.length, 1); eq(world.triggers[0].n, 15);
    eq(ctx.porRemoveTrigger(), 1); eq(world.triggers.length, 0);
  });

  test(label + 'T10 board capture: an unlogged pre-open render re-locks; a logged board is read once; the plan outlives the board', () => {
    const pre = {at: '2026-11-02 16:50:05', output: 'EXECUTABLE', run: '41445971593',
      rows: FIX.renders[1].rows.map(x => (x.symbol === 'ITRN.US' ? Object.assign({}, x, {shares: 70, ticket: 13700}) : x))};
    const a = refresh({src, now: T('2026-11-02 16:55'), props: ON, feed: FEED_B, portfolio: FIX.portfolio_before,
      log: logTo(T('2026-11-02 16:55')), board: buildBoard(pre, 'SelLog: unchanged (logged today)')});
    eq(a.res.board, 'board 11-02 16:50 captured (EXECUTABLE, not in the log)');
    const la = bySym(ledger(a.world));
    eq(Object.keys(la).length, 6, 'the six funded seats of the board (BBOX / OLP / KB / 2020 unfunded, ZZZ not a seat)');
    eq(la['ITRN.US'].Shares, 70); eq(la['ITRN.US']['Locked At (Riyadh)'], '2026-11-02 16:50:05'); eq(la['ITRN.US']['Lock Source'], 'board');
    eq(la['ITRN.US']['Replaced Lock'], '2026-11-02 08:49'); eq(la['ITRN.US']['Source Run'], '41445971593');
    eq(la['ITRN.US'].Status, 'WAITING', 'the pre-open release of run 41445971593 releases the re-locked plan');
    const held = refresh({src, now: T('2026-11-02 18:10'), props: ON, feed: FEED_B, portfolio: FIX.portfolio_before, log: logTo(T('2026-11-02 18:10')),
      board: buildBoard(FIX.renders[4], 'SelLog: +7'), extraSheets: [{name: '_Plan_Of_Record', rows: ledgerCells(a.world)}]});
    const lb = bySym(ledger(held.world));
    eq(lb['ITRN.US'].Shares, 70, 'the 16:50 plan stays after the open'); eq(lb['ITRN.US'].Status, 'OPEN', 'and is released');
    eq(lb['ITRN.US']['Replaced Lock'], '2026-11-02 08:49', 'replaced lock kept');
    eq(held.res.board, 'board 11-02 17:39 is in the log');
    const noLedger = refresh({src, now: T('2026-11-02 18:10'), props: ON, feed: FEED_B, portfolio: FIX.portfolio_before, log: logTo(T('2026-11-02 18:10')),
      board: buildBoard(FIX.renders[4], 'SelLog: +7')});
    eq(bySym(ledger(noLedger.world))['ITRN.US'].Status, 'NOT EXECUTABLE', 'without the record the 08:49 render is all that is left');
    const logged = Object.assign({}, pre, {at: '2026-11-02 16:50:04'});
    const once = refresh({src, now: T('2026-11-02 16:55'), props: ON, feed: FEED_B, portfolio: FIX.portfolio_before,
      log: buildLog(FIX.renders.filter(r => riyadhMs(r.at) <= T('2026-11-02 16:55')).concat([logged])), board: buildBoard(pre, 'SelLog: +10')});
    const ob = bySym(ledger(once.world));
    eq(ob['ITRN.US']['Locked At (Riyadh)'], '2026-11-02 16:50:04', 'a logged board is read from the log only'); eq(ob['ITRN.US']['Lock Source'], 'log');
    const old = refresh({src, now: T('2026-11-02 18:10'), props: ON, log: logTo(T('2026-11-02 18:10')), board: buildBoard(FIX.renders[1], 'SelLog: unchanged (logged today)')});
    eq(old.res.board, 'board 11-02 08:49 is not newer than the log');
    const noTable = buildBoard(pre, 'SelLog: unchanged (logged today)');
    for (const r of noTable) if (r[0] === 'Rank') r[0] = 'Seat';
    eq(refresh({src, now: T('2026-11-02 16:55'), props: ON, log: logTo(T('2026-11-02 16:55')), board: noTable}).res.board, 'board 11-02 16:50 has no ticket table');
    const noTime = buildBoard(pre, 'SelLog: unchanged (logged today)'); noTime[1][1] = 'status: ok';
    eq(refresh({src, now: T('2026-11-02 16:55'), props: ON, log: logTo(T('2026-11-02 16:55')), board: noTime}).res.board, 'board time unreadable');
    eq(refresh({src, now: AFTER_CLOSE, props: ON}).res.board, 'Top_10_Investments missing');
    DIGEST.push(JSON.stringify([la['ITRN.US'].Status, lb['ITRN.US'].Status, ob['ITRN.US']['Lock Source']]));
  });

  test(label + 'T11 the ledger is the record: plans outlive the log tail; drift and bad rows fail closed; plans age out', () => {
    const full = refresh({src, now: T('2026-11-02 23:50'), props: ON, portfolio: FIX.portfolio_after});
    const rows = ledgerCells(full.world);
    const tail = buildLog(FIX.renders.filter(r => riyadhMs(r.at) >= T('2026-11-02 23:00')));
    const kept = refresh({src, now: AFTER_CLOSE, props: ON, log: tail, extraSheets: [{name: '_Plan_Of_Record', rows}]});
    const kb = bySym(ledger(kept.world));
    eq(Object.keys(kb).length, 6); eq(kb['ITRN.US'].Status, 'EXECUTED'); eq(kb['CRC.US'].Status, 'EXPIRED');
    eq(kb['ITRN.US']['Locked At (Riyadh)'], '2026-11-02 08:49:43'); eq(kb['ITRN.US'].Stop, 48.272); eq(kb['ITRN.US']['IBKR Protection'], 'stop 49.94 (plan 48.27)');
    eq(kept.res.ledger, 'ledger 1 plans carried');
    eq(ledger(refresh({src, now: AFTER_CLOSE, props: ON, log: tail}).world).length, 0, 'the tail alone holds no plan');
    const drift = rows.map(r => r.slice()); drift[0][0] = 'Key';
    const d = refresh({src, now: AFTER_CLOSE, props: ON, log: tail, extraSheets: [{name: '_Plan_Of_Record', rows: drift}]});
    eq(ledger(d.world).length, 0); eq(d.res.ledger, 'ledger header differs; plans rebuilt from the cockpit');
    const bad = rows.map(r => r.slice()); const H = bad[0];
    bad[1][H.indexOf('Plan Key')] = 'US|2026-11-03';
    const b = refresh({src, now: AFTER_CLOSE, props: ON, log: tail, extraSheets: [{name: '_Plan_Of_Record', rows: bad}]});
    eq(ledger(b.world).length, 5, 'the edited row is dropped'); eq(b.res.ledger, 'ledger 1 plans carried, 1 rows dropped');
    const two = rows.map(r => r.slice()); two[2][H.indexOf('Locked At (Riyadh)')] = '2026-11-02 09:15:00';
    const t2 = refresh({src, now: AFTER_CLOSE, props: ON, log: tail, extraSheets: [{name: '_Plan_Of_Record', rows: two}]});
    eq(ledger(t2.world).length, 0, 'a key with two lock times is dropped whole');
    const aged = refresh({src, now: T('2026-11-07 1:00'), props: ON, log: tail, extraSheets: [{name: '_Plan_Of_Record', rows}]});
    eq(ledger(aged.world).length, 0, 'aged out 4 days after the close');
    DIGEST.push(JSON.stringify([kept.res.ledger, b.res.ledger]));
  });

  test(label + 'T12 feed expiry: the cockpit clock and expires= decide; the reading equals dt10UvParse_', () => {
    const at = T('2026-11-02 17:45');
    const crc = feed => bySym(ledger(refresh({src, now: at, props: ON, feed, portfolio: FIX.portfolio_before, log: logTo(at)}).world))['CRC.US'];
    const expired = crc(FIX.feeds.morning_executable);
    eq(expired.Status, 'NOT EXECUTABLE', 'an EXECUTABLE cell past its clock'); eq(expired['Status Reason'], 'the release of this plan (run 41363258387) expired 2026-11-02 12:19');
    eq(crc('EXECUTABLE | run=41363258387 | 2026-11-02 10:00:00+03:00 | release=41371798113').Status, 'OPEN', 'inside its clock');
    const tok = crc('EXECUTABLE | run=41363258387 | 2026-11-02 10:00:00+03:00 | issued=2026-11-02T10:05:00+03:00 | expires=2026-11-02T17:40:00+03:00 | ttl_min=460');
    eq(tok['Status Reason'], 'the release of this plan (run 41363258387) expired 2026-11-02 17:40', 'expires= ends first');
    eq(crc('EXECUTABLE | run=41363258387 | 2026-11-02 10:00:00+03:00 | expires=2026-11-02T14:50:00Z').Status, 'OPEN', 'Z offset: 17:50 Riyadh');
    eq(crc('EXECUTABLE | run=41363258387 | 2026-11-02 14:00:00 | release=1').Status, 'OPEN', 'naive stamp: the smaller plausible age');
    eq(crc('EXECUTABLE | run=41363258387 | release=1')['Status Reason'], 'the release of this plan (run 41363258387) release time unreadable');
    eq(crc('EXECUTABLE | run=41363258387 | 2026-11-02 18:05:00+03:00')['Status Reason'], 'the release of this plan (run 41363258387) release time is in the future');
    const P = page(refresh({src, now: at, props: ON, feed: FIX.feeds.morning_executable, portfolio: FIX.portfolio_before, log: logTo(at)}).world);
    ok(P[1][0].indexOf('feed EXECUTABLE (run 41363258387) expired 2026-11-02 12:19') > 0, 'status line ' + P[1][0]);
    const P2 = page(refresh({src, now: at, props: ON, feed: FEED_B, portfolio: FIX.portfolio_before, log: logTo(at)}).world);
    ok(P2[1][0].indexOf('feed EXECUTABLE (run 41445971593, until 11-02 23:37)') > 0, 'status line ' + P2[1][0]);
    const cockpit = cockpitFn('dt10UvParse_');
    const ctx = load(src, makeWorld({}), at);
    const values = [FIX.feeds.morning_executable, FIX.feeds.afternoon_executable, FIX.feeds.withdrawn, FEED_B,
      'EXECUTABLE | run=41363258387 | 2026-11-02 10:00:00+03:00', 'EXECUTABLE | run=41363258387 | 2026-11-02 09:44:00+03:00',
      'EXECUTABLE | run=41363258387 | 2026-11-02 14:00:00', 'EXECUTABLE | run=41363258387 | 2026-11-02 23:00:00',
      'EXECUTABLE | run=41363258387 | 2026-11-02 18:05:00+03:00', 'EXECUTABLE | run=41363258387 | 2026-11-02 17:55:00+03:00',
      'executable | run=41363258387 | 2026-11-02 17:00:00+03:00', '  EXECUTABLE  | run=41363258387 | 2026-11-02 17:00:00-0500', ''];
    for (const v of values) {
      const c = cockpit(v, at), p = ctx.porFeedOf_(v, at);
      eq(p.live, c.state === 'EXECUTABLE', 'live == cockpit EXECUTABLE for "' + v + '"');
      if (c.ageMin !== null) eq(Math.round(ctx.porFeedAgeMin_(v, at) * 1000), Math.round(c.ageMin * 1000), 'age for "' + v + '"');
    }
    DIGEST.push(JSON.stringify([expired['Status Reason'], tok['Status Reason']]));
  });

  test(label + 'T13 POR_IBKR_ALIASES: a renamed ticker keeps its stop match (INDV.US -> SUPN)', () => {
    const cap = JSON.parse(JSON.stringify(FIX.capture));
    cap.orders.orders.push({order_id: 1, order_status: 'SUBMITTED', order_type: 'STOP', side: 'SELL', total_shares_qty: '95',
      remaining_shares_qty: '95', primary_description: 'Sell 95 SUPN', secondary_description: 'STP 33.00, GTC'});
    const capture = {ok: true, name: 'TFB_IBKR_Capture_test.json', json: cap};
    eq(bySym(ledger(refresh({src, now: AFTER_CLOSE, props: ON, capture}).world))['INDV.US']['IBKR Protection'], 'NO STOP AT IBKR', 'without an alias');
    const w = refresh({src, now: AFTER_CLOSE, props: {POR_MODE: 'on', POR_IBKR_ALIASES: ' indv.us=SUPN.US ; JUNK, =X '}, capture}).world;
    eq(bySym(ledger(w))['INDV.US']['IBKR Protection'], 'stop 33.00 (plan 30.92)');
    eq(bySym(ledger(w))['ITRN.US']['IBKR Protection'], 'stop 49.94 (plan 48.27)', 'others unchanged');
    const ctx = load(src, makeWorld({}), AFTER_CLOSE);
    eq(JSON.stringify(ctx.porAliases_('INDV.US=SUPN, 0700.HK=700.HK x')), '{"INDV.US":"SUPN","0700.HK":"700"}');
  });

  test(label + 'T14 contract with 16_Decision_Top10: board headers, status cell, tokens, log headers, clock', () => {
    eq(BOARD_HEADERS[0], 'Rank'); eq(BOARD_HEADERS[1], 'Symbol');
    const norm = h => h.toLowerCase().replace(/[^a-z0-9]/g, '');
    const bh = BOARD_HEADERS.map(norm);
    for (const k of ['shares', 'entryzone', 'stopsar', 'tp1sar', 'tp2sar', 'fxsar', 'ticketsar', 'roitp1', 'engineroi12m', 'conf', 'stability', 'advisornote']) ok(bh.indexOf(k) >= 0, 'board header ' + k);
    const lh = cockpitArray('DT10_SELLOG_HEADERS').map(norm);
    for (const k of ['loggedat', 'runinfo', 'symbol', 'shares', 'entryzone', 'stopsar', 'tp1sar', 'tp2sar', 'fxsar', 'roi', 'engineroi', 'outcome', 'stability']) ok(lh.indexOf(k) >= 0, 'log header ' + k);
    ok(/\nvar DT10_ROW_STATUS = 2;/.test(COCKPIT) && COCKPIT.indexOf('var statusCell = sheet.getRange(DT10_ROW_STATUS, 2);') >= 0, 'status line in B2');
    for (const s of ["return 'Last run ' + stamp + ' | status: ' + status +", "(output ? ' | output: ' + output : '')",
      "'yyyy-MM-dd HH:mm:ss'", "return 'SelLog: +' + rows.length;", "return 'SelLog: unchanged (logged today)';",
      "'VERIFIED (run ' + finalSource.cohort + ')'", 'var finalStatusLine = selLogNote ? statusLine + \' | \' + selLogNote']) {
      ok(COCKPIT.indexOf(s) >= 0, 'cockpit token: ' + s);
    }
    eq(cockpitNum(src, 'POR_FEED_MAX_AGE_MIN'), cockpitNum(COCKPIT, 'DT10_UV_MAX_AGE_MIN'), 'feed clock');
    eq(cockpitNum(src, 'POR_FEED_SKEW_MIN'), cockpitNum(COCKPIT, 'DT10_UV_FUTURE_SKEW_MIN'), 'feed skew');
  });

  test(label + 'T15 merge: a carried in-session lock stays; a pre-open render first seen after the open takes its place', () => {
    const crcRow = n => Object.assign({}, FIX.renders[1].rows[3], {shares: n});
    const r1800 = {at: '2026-11-02 18:00:00', output: 'EXECUTABLE', run: '41445971593', rows: [crcRow(100)]};
    const a = refresh({src, now: T('2026-11-02 18:05'), props: SES, portfolio: FIX.portfolio_before, log: buildLog([FIX.renders[0]]),
      board: buildBoard(r1800, 'SelLog: unchanged (logged today)')});
    const ca = bySym(ledger(a.world))['CRC.US'];
    eq(ca['Lock Rule'], 'in-session'); eq(ca.Shares, 100); eq(ca['Lock Source'], 'board');
    const rows = ledgerCells(a.world);
    const r1820 = {at: '2026-11-02 18:20:00', output: 'EXECUTABLE', run: '41445971593', rows: [crcRow(120)]};
    const b = refresh({src, now: T('2026-11-02 18:30'), props: SES, portfolio: FIX.portfolio_before, log: buildLog([FIX.renders[0], r1820]),
      extraSheets: [{name: '_Plan_Of_Record', rows}]});
    eq(bySym(ledger(b.world))['CRC.US'].Shares, 100, 'the first in-session lock stays against a later one');
    const r1720 = {at: '2026-11-02 17:20:00', output: 'EXECUTABLE', run: '41445971593', rows: [crcRow(90)]};
    const c = refresh({src, now: T('2026-11-02 18:45'), props: SES, portfolio: FIX.portfolio_before, log: buildLog([FIX.renders[0], r1720, r1820]),
      extraSheets: [{name: '_Plan_Of_Record', rows}]});
    const cc = bySym(ledger(c.world))['CRC.US'];
    eq(cc.Shares, 90, 'the pre-open render was the plan at the open'); eq(cc['Lock Rule'], 'pre-open'); eq(cc['Replaced Lock'], '2026-11-02 18:00');
    DIGEST.push(JSON.stringify([ca.Shares, cc.Shares]));
  });
}

battery(SRC, '');
const firstDigest = crypto.createHash('sha256').update(DIGEST.join('|')).digest('hex');
DIGEST.length = 0;
battery(SRC, 're ');
const secondDigest = crypto.createHash('sha256').update(DIGEST.join('|')).digest('hex');
test('digests of the two runs match', () => eq(secondDigest, firstDigest));

/* ------------------------------------------------------------ mutants */
const MUTANTS = [
  ['executed threshold', "var POR_EXEC_FRACTION = 0.9;", "var POR_EXEC_FRACTION = 1.1;"],
  ['no pre-open re-lock', "} else if (s && preOpen && cur.lockRule === 'pre-open' && r.atMs > cur.lockedAtMs) {", "} else if (false) {"],
  ['US clock in UTC', "US: {label: 'NYSE/NASDAQ', tz: 'America/New_York'", "US: {label: 'NYSE/NASDAQ', tz: 'UTC'"],
  ['feed run ignored', "var released = ctx.feed.live && ctx.feed.run === plan.run;", "var released = ctx.feed.live;"],
  ['feed expiry ignored', "else if (age > POR_FEED_MAX_AGE_MIN || (exp !== null && nowMs >= exp)) out.why", "else if (false) out.why"],
  ['feed token case ignored', "var token = first === 'EXECUTABLE' ? 'EXECUTABLE' :", "var token = head === 'EXECUTABLE' ? 'EXECUTABLE' :"],
  ['expired orders counted', "if (/FILLED|CANCEL|EXPIRED|INACTIVE|REJECT/.test(st)) continue;", "if (/FILLED/.test(st)) continue;"],
  ['kill switch ignored', "} else if (ctx.feed.killed) {", "} else if (false) {"],
  ['post-lock snapshot as baseline', "ctx.snap.at <= plan.lockedAtMs &&", ""],
  ['snapshot gap unbounded', "var POR_SNAP_MAX_GAP_MS = 60 * 60000;", "var POR_SNAP_MAX_GAP_MS = 1e15;"],
  ['board capture ignored', "if (board.render) renders.push(board.render);", "if (false) renders.push(board.render);"],
  ['logged board read twice', "if (/SelLog: \\+\\d+/.test(status)) return", "if (false) return"],
  ['later in-session lock wins', "return a.lockedAtMs < b.lockedAtMs;", "return a.lockedAtMs > b.lockedAtMs;"],
  ['late pre-open ignored', "if (a.lockRule === 'pre-open') return b.lockRule !== 'pre-open' || a.lockedAtMs > b.lockedAtMs;",
    "if (a.lockRule === 'pre-open') return b.lockRule === 'pre-open' && a.lockedAtMs > b.lockedAtMs;"],
  ['ledger plans ignored', "var plans = porMergePlans_(porDerivePlans_(src.renders), ledger.plans);\n    var built",
    "var plans = porMergePlans_(porDerivePlans_(src.renders), []);\n    var built"],
  ['alias ignored', "if (m) out[m[1]] = porIbkrBase_(m[2]);", "if (false) out[m[1]] = porIbkrBase_(m[2]);"],
  ['workbook time zone ignored', "if (POR_SHEET_TZ === POR_TZ) return t;", "return t;"],
  ['writes _Status', "var POR_LEDGER_SHEET = '_Plan_Of_Record';", "var POR_LEDGER_SHEET = '_Status';"],
  ['v1.0.1 priority skip removed', "  if (prio && porRenderBusy_(porNow_())) {", "  if (false) {"],
  ['v1.0.1 gate always on', "function porPriorityOn_(raw) {\n  return /^\\s*(on|1|true|yes)\\s*$/i.test(String(raw === null || raw === undefined ? '' : raw));", "function porPriorityOn_(raw) {\n  return true;"],
  ['v1.0.1 marker window ignored', "if (isFinite(ms) && nowMs - ms >= 0 && nowMs - ms < limit) return true;", "if (isFinite(ms)) return true;"],
  ['v1.0.1 claim ignored', "if (c && typeof c.at_ms === 'number' && nowMs - c.at_ms >= 0 && nowMs - c.at_ms < limit) return true;", "if (false) return true;"],
  ['v1.0.1 TTL unchanged', "if (!porClaim_(prio ? POR_PRIORITY_TTL_S : POR_RUN_TTL_S)) {", "if (!porClaim_(POR_RUN_TTL_S)) {"]
];
for (const [name, from, to] of MUTANTS) {
  test('mutant killed: ' + name, () => {
    ok(SRC.indexOf(from) >= 0, 'mutation site present: ' + from);
    const mutated = SRC.replace(from, to);
    const before = failed, beforePassed = passed;
    const silent = console.log;
    console.log = () => {};
    try { battery(mutated, 'mutant '); } finally { console.log = silent; }
    const killed = failed > before;
    failed = before; passed = beforePassed;
    DIGEST.length = 0;
    ok(killed, 'the battery must fail on the mutant');
  });
}

console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
