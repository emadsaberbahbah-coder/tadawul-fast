#!/usr/bin/env node
/* tests/test_por_v100.js
 * Harness for apps_script/28_Plan_Of_Record.gs v1.0.0 [PLAN OF RECORD] (owner review of Tue 3 Nov 2026 01:08
 * Riyadh: 'today morning there was 6 symboles as quailifed for investing, but after that now no one is
 * qualified ... the program should show in consistance beahviour and more stable recommdadtions').
 *
 * The REAL file runs in a vm context with a recording GAS shim (spreadsheet with cells and a write log,
 * PropertiesService, CacheService, ScriptApp triggers, Utilities.formatDate through Intl time zones, Logger),
 * the 10_My_Portfolio capture reader stubbed with the 2 Nov IBKR working orders, and a frozen clock. The board is
 * the live 2 Nov _Selection_Log (tests/fixtures/por_board_2026-11-02.json): 08:49 EXECUTABLE (6 tickets), the
 * withheld and held renders, the 17:39 exits.
 * Sections:
 *   T0  the file compiles, version, ES5 / ASCII lint, function inventory (45, prefix por)
 *   T1  gates: POR_MODE off (default, junk, property fault) -> nothing read or written; observe -> ledger only;
 *       on -> ledger + page; POR_VALIDITY feed default, session only for 'session'
 *   T2  sessions: US DST / standard time, weekend roll, Tadawul Sun-Thu, LSE BST / GMT, HKEX; venue codes
 *   T3  the 2 Nov plan at 01:40 on 3 Nov (feed withdrawn, validity feed): one plan US|2026-11-02 locked 08:49
 *       pre-open, run 41363258387, six tickets in SAR->USD; ITRN / DDI / GLPI / INDV EXECUTED from the buy date,
 *       CRC / EFC EXPIRED; IBKR stops 49.94 / 12.90 / 37.33 against plan stops 48.27 / 12.40 / 35.21; INDV
 *       'NO STOP AT IBKR' (the expired Day limit never counts); summary 1 plan, 6 tickets, 4 executed, 1 missing
 *   T4  during the US session, before any fill: validity feed -> NOT EXECUTABLE (the afternoon feed releases run
 *       41445971593, not the plan's run); validity session -> OPEN; 16:00 -> WAITING (opens 17:30); 09:00 with
 *       the morning feed -> WAITING; the 17:39 board (grace / hard exits) is shown beside the unchanged plan
 *   T5  lock rule: a newer EXECUTABLE render before the open replaces the plan (pre-open re-lock), one after the
 *       open is ignored (frozen); a render inside an open session locks only when no plan exists
 *   T6  kill switch (release_killed) invalidates unexecuted tickets under session validity; executed stay
 *   T7  lock-time quantity: recorded within 30 minutes of the lock, carried in the ledger, and an add to an
 *       older holding counts from it (not from the buy date)
 *   T8  robustness: Logged At as Date cells == text cells; a ledger whose times Sheets turned into Dates keeps the
 *       lock-time quantity; a header without Shares fails closed (no write); missing My_Portfolio / capture reader
 *       degrade to notes; two refreshes are identical
 *   T9  only _Plan_Of_Record and Plan_Of_Record are ever written (no audited sheet, no _Status)
 *   T10 mutants: each mutation of the delivered file fails at least one section check
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
const FIX = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'fixtures', 'por_board_2026-11-02.json'), 'utf8'));
const PROTECTED = ['_Selection_Log', 'My_Portfolio', '_Status', 'Top_10_Investments', 'Portfolio_Decision'];
const OWN = ['_Plan_Of_Record', 'Plan_Of_Record'];

let passed = 0, failed = 0;
const DIGEST = [];
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n  ')); }
}
function ok(c, m) { if (!c) throw new Error(m || 'assertion failed'); }
function eq(a, b, m) { if (a !== b) throw new Error((m || 'not equal') + ': ' + JSON.stringify(a) + ' !== ' + JSON.stringify(b)); }

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
  const world = {writes: [], sheets: {}, props: Object.assign({}, opts.props || {}), cache: {}, triggers: [], logs: [], failReads: opts.failReads || []};
  const add = (name, rows) => { world.sheets[name] = new Sheet(name, rows, world); };
  if (opts.log !== null) add('_Selection_Log', opts.log || buildLog(FIX.renders));
  if (opts.portfolio !== null) add('My_Portfolio', [FIX.portfolio_header].concat(opts.portfolio || FIX.portfolio_after));
  const status = [];
  for (let i = 0; i < 3; i++) { const row = new Array(13).fill(''); row[0] = 'Page' + i; status.push(row); }
  status[0][11] = 'Global Key'; status[0][12] = 'Value';
  status[1][11] = 'Last Global Update'; status[1][12] = '10/31/2026';
  status[2][11] = 'TFB Decision Feed'; status[2][12] = opts.feed === undefined ? FIX.feeds.withdrawn : opts.feed;
  add('_Status', status);
  for (const s of opts.extraSheets || []) add(s.name, s.rows);
  world.ss = {
    getSheetByName: n => world.sheets[n] || null,
    insertSheet: n => { if (world.sheets[n]) throw new Error('sheet exists ' + n); world.writes.push([n, 'insertSheet']); world.sheets[n] = new Sheet(n, [], world, 1000, 26); return world.sheets[n]; }
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
    PropertiesService: {getScriptProperties: () => ({getProperty: k => { if (world.propFault) throw new Error('fault'); return Object.prototype.hasOwnProperty.call(world.props, k) ? world.props[k] : null; }})},
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
function riyadhMs(text) {   // 'yyyy-MM-dd H:mm[:ss]' Riyadh (UTC+3, no DST)
  const m = /^(\d{4})-(\d{2})-(\d{2}) (\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec(text);
  return Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4] - 3, +m[5], +(m[6] || 0));
}
function runInfo(r) {
  return 'Last run ' + r.at + ' | status: ok | output: ' + r.output + ' | sheets pool 9788 rows | source readiness VERIFIED (run ' + r.run + ') | route v4.19.0';
}
function buildLog(renders, asDates) {
  const H = FIX.selection_log_header;
  const idx = n => H.indexOf(n);
  const rows = [H.slice()];
  for (const r of renders) {
    for (const x of r.rows) {
      const row = new Array(H.length).fill('');
      row[idx('Logged At')] = asDates ? new Date(riyadhMs(r.at)) : r.at;
      row[idx('Run Info')] = runInfo(r);
      row[idx('Source Page')] = 'Top_10_Investments';
      const map = {rank: 'Rank', symbol: 'Symbol', name: 'Name', market: 'Market', sector: 'Sector', ccy: 'Ccy', fx: H[9], price: 'Price',
        priceSar: 'Price SAR', zone: 'Entry Zone', ticket: 'Ticket SAR', shares: 'Shares', stop: 'Stop SAR', tp1: 'TP1 SAR', tp2: 'TP2 SAR',
        roi: 'ROI %', eroi: 'Engine ROI %', rel: 'Rel', dq: 'DQ', conf: 'Conf', note: 'Advisor Note', outcome: 'Outcome', stability: 'Stability'};
      for (const k of Object.keys(map)) if (x[k] !== undefined) row[idx(map[k])] = x[k];
      rows.push(row);
    }
  }
  return rows;
}
const T = text => riyadhMs(text);
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
function bySym(rows) { const o = {}; for (const r of rows) o[r.Symbol] = r; return o; }
function page(world) {
  const sh = world.sheets.Plan_Of_Record;
  return sh ? sh.getRange(1, 1, sh.getLastRow(), sh.getLastColumn()).getValues() : null;
}

/* ------------------------------------------------------------ the battery */
function battery(src, label) {
  const ON = {POR_MODE: 'on'};
  const AFTER_CLOSE = T('2026-11-03 1:40');

  test(label + 'T0 compiles, version, ES5 / ASCII, 45 functions prefixed por', () => {
    const ctx = load(src, makeWorld({}), AFTER_CLOSE);
    eq(ctx.POR_VERSION, '1.0.0', 'version');
    ok(!/[^\x00-\x7e]/.test(src.replace(/\r/g, '')), 'ASCII only');
    ok(!/=>|\blet\s|\bconst\s|`|\.startsWith\(|\.includes\(|Object\.assign|\.padStart\(/.test(src), 'ES5 only');
    const fns = src.match(/^function [A-Za-z0-9_]+/mg) || [];
    eq(fns.length, 45, 'function count');
    ok(fns.every(f => /^function por/.test(f)), 'prefix por');
  });

  test(label + 'T1 gates: off by default, junk and faults are off; observe = ledger only; on = page', () => {
    for (const props of [{}, {POR_MODE: 'yes'}, {POR_MODE: ' ON '.toLowerCase() + 'x'}]) {
      const {world, res} = refresh({src, now: AFTER_CLOSE, props});
      eq(res.mode, 'off', 'mode'); eq(world.writes.length, 0, 'no writes when off');
    }
    const fault = makeWorld({props: {POR_MODE: 'on'}}); fault.propFault = true;
    const fctx = load(src, fault, AFTER_CLOSE);
    eq(fctx.porRefresh().mode, 'off', 'property fault is off'); eq(fault.writes.length, 0, 'no writes on fault');
    const obs = refresh({src, now: AFTER_CLOSE, props: {POR_MODE: ' Observe '}});
    eq(obs.res.mode, 'observe'); ok(obs.world.sheets._Plan_Of_Record, 'ledger written'); ok(!obs.world.sheets.Plan_Of_Record, 'no page in observe');
    ok(obs.world.sheets._Plan_Of_Record.hidden, 'ledger hidden');
    const on = refresh({src, now: AFTER_CLOSE, props: ON});
    ok(on.world.sheets.Plan_Of_Record && !on.world.sheets.Plan_Of_Record.hidden, 'page written and visible');
    eq(on.res.validity, 'feed', 'validity default feed');
    eq(refresh({src, now: AFTER_CLOSE, props: {POR_MODE: 'on', POR_VALIDITY: 'Session'}}).res.validity, 'session');
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
      eq(r['Plan Key'], 'US|2026-11-02'); eq(r['Locked At (Riyadh)'], '2026-11-02 08:49'); eq(r['Lock Rule'], 'pre-open');
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
    const ses = refresh({src, now: at, props: {POR_MODE: 'on', POR_VALIDITY: 'session'}, feed: FIX.feeds.afternoon_executable, portfolio: FIX.portfolio_before});
    const sb = bySym(ledger(ses.world));
    for (const sym of Object.keys(sb)) eq(sb[sym].Status, 'OPEN', 'session validity ' + sym);
    ok(sb['ITRN.US']['Status Reason'].indexOf('buy only inside the entry zone') === 0, 'open reason');
    ok(sb['ITRN.US']['Status Reason'].indexOf('held by session validity') > 0, 'session note');
    eq(sb['GLPI.US']['Live Board'], 'EXIT: hard (17:39)');
    const wait = refresh({src, now: T('2026-11-02 16:00'), props: {POR_MODE: 'on', POR_VALIDITY: 'session'}, feed: FIX.feeds.stale, portfolio: FIX.portfolio_before});
    const wb = bySym(ledger(wait.world));
    eq(wb['CRC.US'].Status, 'WAITING'); eq(wb['CRC.US']['Status Reason'], 'market opens 2026-11-02 17:30');
    const morning = refresh({src, now: T('2026-11-02 9:00'), props: ON, feed: FIX.feeds.morning_executable, portfolio: FIX.portfolio_before,
      log: buildLog(FIX.renders.filter(r => riyadhMs(r.at) <= T('2026-11-02 9:00')))});
    eq(bySym(ledger(morning.world))['EFC.US'].Status, 'WAITING', 'the morning feed releases the plan');
    const stale = refresh({src, now: T('2026-11-02 11:30'), props: ON, feed: FIX.feeds.stale, portfolio: FIX.portfolio_before,
      log: buildLog(FIX.renders.filter(r => riyadhMs(r.at) <= T('2026-11-02 11:30')))});
    eq(bySym(ledger(stale.world))['EFC.US'].Status, 'NOT EXECUTABLE', 'stale feed, feed validity');
    const live = refresh({src, now: T('2026-11-02 17:45'), props: ON, feed: FIX.feeds.afternoon_executable, portfolio: FIX.portfolio_before,
      log: buildLog(FIX.renders.filter(r => riyadhMs(r.at) <= T('2026-11-02 17:45')))});
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
    const {world} = refresh({src, now: T('2026-11-02 18:00'), props: {POR_MODE: 'on', POR_VALIDITY: 'session'}, log: buildLog(renders), portfolio: FIX.portfolio_before});
    const L = ledger(world);
    eq(L.length, 1, 'the newer pre-open release is the plan (ITRN only)');
    eq(L[0].Shares, 70); eq(L[0]['Locked At (Riyadh)'], '2026-11-02 16:50'); eq(L[0]['Replaced Lock'], '2026-11-02 08:49'); eq(L[0]['Source Run'], '41445971593');
    const only = {at: '2026-11-02 18:20:00', output: 'EXECUTABLE', run: '41445971593', rows: [Object.assign({}, FIX.renders[1].rows[2], {shares: 99})]};
    const late = refresh({src, now: T('2026-11-02 18:30'), props: {POR_MODE: 'on', POR_VALIDITY: 'session'}, portfolio: FIX.portfolio_before,
      log: buildLog([FIX.renders[0], only])});
    const ll = ledger(late.world);
    eq(ll.length, 1); eq(ll[0]['Lock Rule'], 'in-session'); eq(ll[0].Shares, 99);
    const twice = {at: '2026-11-02 18:25:00', output: 'EXECUTABLE', run: '41445971593', rows: [Object.assign({}, FIX.renders[1].rows[2], {shares: 120})]};
    const frozen = refresh({src, now: T('2026-11-02 18:30'), props: {POR_MODE: 'on', POR_VALIDITY: 'session'}, portfolio: FIX.portfolio_before,
      log: buildLog([only, twice])});
    eq(ledger(frozen.world)[0].Shares, 99, 'first in-session lock stays');
    DIGEST.push(JSON.stringify([L[0].Shares, ll[0].Shares]));
  });

  test(label + 'T6 the release kill switch invalidates unexecuted tickets; executed tickets stay executed', () => {
    const {world} = refresh({src, now: T('2026-11-02 18:10'), props: {POR_MODE: 'on', POR_VALIDITY: 'session'}, feed: FIX.feeds.killed});
    const b = bySym(ledger(world));
    eq(b['CRC.US'].Status, 'INVALIDATED'); eq(b['CRC.US']['Status Reason'], 'release kill switch');
    eq(b['ITRN.US'].Status, 'EXECUTED');
  });

  test(label + 'T7 lock-time quantity: recorded within 30 min, carried, and adds count from it', () => {
    const older = [['ITRN.US', 'Ituran', 52.0, 10, 50.0, 520, '2026-09-01']].concat(FIX.portfolio_before);
    const first = refresh({src, now: T('2026-11-02 9:10'), props: ON, feed: FIX.feeds.morning_executable, portfolio: older,
      log: buildLog(FIX.renders.filter(r => riyadhMs(r.at) <= T('2026-11-02 9:10')))});
    const fb = bySym(ledger(first.world));
    eq(fb['ITRN.US']['Held At Lock'], 10, 'recorded'); eq(fb['DDI.US']['Held At Lock'], 0, 'zero is recorded');
    const ledgerRows = first.world.sheets._Plan_Of_Record.cells.map(r => r.slice());
    const added = [['ITRN.US', 'Ituran', 52.2, 77, 52.0, 4019, '2026-09-01']].concat(FIX.portfolio_before);
    const later = refresh({src, now: AFTER_CLOSE, props: ON, portfolio: added,
      extraSheets: [{name: '_Plan_Of_Record', rows: ledgerRows}]});
    const lb = bySym(ledger(later.world));
    eq(lb['ITRN.US'].Status, 'EXECUTED'); eq(lb['ITRN.US']['Executed Qty'], 67); eq(lb['ITRN.US']['Status Reason'], '67 sh held, since the lock');
    eq(lb['ITRN.US']['Held At Lock'], 10, 'carried');
    const late = refresh({src, now: T('2026-11-02 9:30'), props: ON, feed: FIX.feeds.morning_executable, portfolio: older,
      log: buildLog(FIX.renders.filter(r => riyadhMs(r.at) <= T('2026-11-02 9:30')))});
    eq(bySym(ledger(late.world))['ITRN.US']['Held At Lock'], '', 'not recorded after 30 minutes');
    const noAdd = refresh({src, now: AFTER_CLOSE, props: ON, portfolio: older, extraSheets: [{name: '_Plan_Of_Record', rows: ledgerRows}]});
    eq(bySym(ledger(noAdd.world))['ITRN.US'].Status, 'EXPIRED', 'an older holding is not a fill');
  });

  test(label + 'T8 robustness: Date cells, Date ledger times, header drift, missing inputs, determinism', () => {
    const asText = refresh({src, now: AFTER_CLOSE, props: ON});
    const asDate = refresh({src, now: AFTER_CLOSE, props: ON, log: buildLog(FIX.renders, true)});
    eq(JSON.stringify(ledger(asDate.world)), JSON.stringify(ledger(asText.world)), 'Date Logged At == text');
    const older = [['ITRN.US', 'Ituran', 52.0, 10, 50.0, 520, '2026-09-01']].concat(FIX.portfolio_before);
    const first = refresh({src, now: T('2026-11-02 9:10'), props: ON, feed: FIX.feeds.morning_executable, portfolio: older,
      log: buildLog(FIX.renders.filter(r => riyadhMs(r.at) <= T('2026-11-02 9:10')))});
    const rows = first.world.sheets._Plan_Of_Record.cells.map(r => r.slice());
    const H = rows[0];
    for (const r of rows.slice(1)) for (const h of ['Session Open (Riyadh)', 'Session Close (Riyadh)', 'Locked At (Riyadh)']) r[H.indexOf(h)] = new Date(riyadhMs(r[H.indexOf(h)]));
    const added = [['ITRN.US', 'Ituran', 52.2, 77, 52.0, 4019, '2026-09-01']].concat(FIX.portfolio_before);
    const later = refresh({src, now: AFTER_CLOSE, props: ON, portfolio: added, extraSheets: [{name: '_Plan_Of_Record', rows}]});
    eq(bySym(ledger(later.world))['ITRN.US']['Executed Qty'], 67, 'Date ledger times keep the lock-time quantity');
    const drift = buildLog(FIX.renders); drift[0][FIX.selection_log_header.indexOf('Shares')] = 'Qty';
    const bad = refresh({src, now: AFTER_CLOSE, props: ON, log: drift});
    ok(/header lacks shares/.test(bad.res.error), 'fail closed on header drift'); eq(bad.world.writes.length, 0, 'no write on drift');
    const noLog = refresh({src, now: AFTER_CLOSE, props: ON, log: null});
    ok(/missing/.test(noLog.res.error) && noLog.world.writes.length === 0, 'no log -> no write');
    const noPf = refresh({src, now: AFTER_CLOSE, props: ON, portfolio: null});
    ok(/My_Portfolio missing/.test(noPf.res.holdings), 'missing portfolio is reported');
    eq(bySym(ledger(noPf.world))['ITRN.US'].Status, 'EXPIRED', 'no holdings -> nothing executed');
    const noReader = refresh({src, now: AFTER_CLOSE, props: ON, capture: null});
    eq(bySym(ledger(noReader.world))['ITRN.US']['IBKR Protection'], 'capture n/a: capture reader not installed (10_My_Portfolio)');
    const noCap = refresh({src, now: AFTER_CLOSE, props: ON, capture: {ok: false, reason: 'no capture in the last 36h'}});
    eq(bySym(ledger(noCap.world))['INDV.US']['IBKR Protection'], 'capture n/a: no capture (no capture in the last 36h)');
    const a = refresh({src, now: AFTER_CLOSE, props: ON}), b2 = refresh({src, now: AFTER_CLOSE, props: ON});
    eq(JSON.stringify(page(a.world)), JSON.stringify(page(b2.world)), 'deterministic page');
    const again = load(src, a.world, AFTER_CLOSE); again.porRefresh();
    eq(JSON.stringify(page(a.world)), JSON.stringify(page(b2.world)), 'idempotent on the same workbook');
    ok(a.world.sheets.Plan_Of_Record.maxRows <= page(a.world).length + 5 && a.world.sheets.Plan_Of_Record.maxCols === 18, 'page grid trimmed to its content');
  });

  test(label + 'T9 only its own two sheets are written; triggers install and remove', () => {
    const {world} = refresh({src, now: AFTER_CLOSE, props: ON});
    const touched = Array.from(new Set(world.writes.map(w => w[0])));
    ok(touched.every(n => OWN.indexOf(n) >= 0), 'touched ' + touched.join(','));
    ok(PROTECTED.every(n => world.writes.every(w => w[0] !== n)), 'protected sheets untouched');
    const ctx = load(src, world, AFTER_CLOSE);
    eq(ctx.porInstallTrigger(), 'installed porRefreshTrigger every 15 min');
    eq(ctx.porInstallTrigger(), 'installed porRefreshTrigger every 15 min (replaced 1)');
    eq(world.triggers.length, 1); eq(world.triggers[0].n, 15);
    eq(ctx.porRemoveTrigger(), 1); eq(world.triggers.length, 0);
    const st = ctx.porSelfTest();
    ok(st[0] === 'Plan of Record 1.0.0' && st.some(l => /^plan US\|2026-11-02 locked 2026-11-02 08:49 pre-open run 41363258387 tickets 6$/.test(l)), 'self test');
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
  ['feed run ignored', "if (ctx.feed.head === 'EXECUTABLE' && ctx.feed.run === plan.run) {", "if (ctx.feed.head === 'EXECUTABLE') {"],
  ['expired orders counted', "if (/FILLED|CANCEL|EXPIRED|INACTIVE|REJECT/.test(st)) continue;", "if (/FILLED/.test(st)) continue;"],
  ['kill switch ignored', "} else if (ctx.feed.killed) {", "} else if (false) {"],
  ['held quantity window', "var POR_HELD_CAPTURE_MS = 30 * 60000;", "var POR_HELD_CAPTURE_MS = 0;"],
  ['writes _Status', "var POR_LEDGER_SHEET = '_Plan_Of_Record';", "var POR_LEDGER_SHEET = '_Status';"]
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
