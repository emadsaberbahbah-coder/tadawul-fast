#!/usr/bin/env node
/* tests/test_dt10_v1290_verdict_reuse.js
 * Harness for apps_script/16_Decision_Top10.gs v1.29.0 [VERDICT-REUSE] (render timing, Mon 9 Nov 2026: the 12:09
 * evidence follow-up render read 'phases 205.6s: ... ready 26 | post 38 | ... verdict1 23.5 | stab 1.5 |
 * verdict2 22.9 | ... r_verdict 22.2 ...'; the source readiness runs five times per render, and two of the five
 * decide nothing). Script Property DT10_VERDICT_REUSE, default OFF.
 *
 * Dual-tree node harness over the REAL file: the delivered .gs and the v1.28.0 BASE (a file given as the second
 * argument, or the base rebuilt from the delivered file by reversing exactly the 10 enumerated v1.29.0 edits,
 * tests/js/dt10_edits_v1290.json, SHA-256 pinned) run the REAL refreshDecisionTop10 in the full-workbook shim of
 * tests/test_dt10_v1280_render_timing.js (copied verbatim: the 2 Nov board; POST stub for the research call and the
 * allocate replay; the board verdict stubbed EXECUTABLE), with console, setProperty and the clock captured.
 * Sections:
 *   V0 base oracle (SHA-256 pinned), versions in lockstep, both compile; census +1 function +1 var, 0 removed;
 *      10 hunks reverse to the base
 *   V1 GOLDEN NEGATIVE / kill identity: the property absent / off / blank / junk / faulted (timer on or off) ->
 *      the REAL refresh (B2, every sheet write, _Run_Log, _Selection_Log, Logger lines, console lines, POST
 *      bodies, saved state) deep-equals the base; one extra property read
 *   V2 armed ('on', ' ON ', 'On'): the same B2, writes, run log, selection log, POST bodies, state and properties;
 *      one Logger line differs, the POST line, in its readiness suffix only; one board verdict and one direct
 *      source readiness fewer; with the timer armed the 35 marks keep their names
 *   V3 the sources go blocked during the stability step: off and armed both end WITHHELD, the selection log
 *      withholds, and no executable ticket is painted (the pre-paint recheck catches it when verdict 2 is reused)
 *   V4 embedded dt10SelfTest: the render-timing line names the knob (off / on / spellings / faulted); the line
 *      count is the base's; every other line is the base's
 *   V5 lint: ES5 / ASCII of the diff, LF only, WHY block newest-first, every prior WHY kept
 *   V6 mutants: seven one-line mutations are each caught
 * The whole battery runs three times and the three digests must match; the last line is 'PASS k/k'
 * (exit 1 on any failure).
 * Usage: node tests/test_dt10_v1290_verdict_reuse.js [new.gs] [base.gs]
 */
'use strict';
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const vm = require('vm');
const ROOT = path.join(__dirname, '..');
const history = require(path.join(ROOT, 'tests', 'js', 'dt10_cash_utc_history.js'));
const EDITS = require(path.join(ROOT, 'tests', 'js', 'dt10_edits_v1290.json'));
const NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '16_Decision_Top10.gs');
const BASE_PATH = process.argv[3] || '';
const newSource = fs.readFileSync(NEW_PATH, 'utf8');
function sha256(s) { return crypto.createHash('sha256').update(s).digest('hex'); }
let baseSource;
try {
  baseSource = BASE_PATH ? fs.readFileSync(BASE_PATH, 'utf8') : history.v1280Source(newSource);
} catch (eBase) {
  console.log('FAIL V0 base oracle: ' + String(eBase && eBase.message || eBase).split('\n')[0]);
  console.log('FAIL 0/1');
  process.exit(1);
}
const FIX = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'fixtures', 'dt10_candidates_2026-10-10.json'), 'utf8'));
const PAGES = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'fixtures', 'dt10_pages_2026-10-05.json'), 'utf8'));
const GUARD = 'DT10_TICKET_GUARD', KPI = 'DT10_KPI_PLAN_GAIN', VENUE = 'DT10_VENUE_LINE', MARK = 'DT10_KPI_PLAN_LABEL_STATE';
const KNOBS = ['DT10_GUARD_EARN_DAYS', 'DT10_GUARD_OWNER_RULE', 'DT10_GUARD_MAX_TICKETS'];
const DASH = '—';
const NOW_R = Date.UTC(2026, 10, 2, 5, 49, 43);          // Mon 2 Nov 2026 08:49:43 Riyadh: the live render
const FEED_RAW = 'EXECUTABLE | run 39700000001 | 2026-11-02 02:42:14+03:00 | issued 04:19:49 | ' +
                 'expires 2026-11-02T10:42:14+03:00 | ttl_min 480';
const NOW_F = Date.parse(FIX.frozen_now);                 // 2026-10-10T19:28:22Z (Sat 22:28 Riyadh): the refresh env
const TODAY_F = FIX.frozen_now.slice(0, 10);

/* ------------------------------------------------------------ runner */
let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + String(e && e.stack || e).split('\n').slice(0, 8).join('\n  ')); }
}
function ok(c, m) { if (!c) throw new Error(m || 'assertion failed'); }
function eq(a, b, m) { if (a !== b) throw new Error((m || 'not equal') + ': ' + JSON.stringify(a) + ' !== ' + JSON.stringify(b)); }
function deq(a, b, m) {
  const ja = JSON.stringify(a), jb = JSON.stringify(b);
  if (ja !== jb) {
    let i = 0; while (i < ja.length && ja[i] === jb[i]) i++;
    throw new Error((m || 'deep-equal') + ' differs at ' + i + ': ...' + ja.slice(Math.max(0, i - 80), i + 120) + ' | ...' + jb.slice(Math.max(0, i - 80), i + 120));
  }
}
const plain = v => v === undefined ? undefined : JSON.parse(JSON.stringify(v));
const r2 = x => Math.round(x * 100) / 100;
const r1 = x => Math.round(x * 10) / 10;
const fmt = n => Math.round(n).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
/* The version string reaches the Logger lines and the run-marker property;
 * comparisons against the base read the delivered version as the base's. */
const nv = x => JSON.parse(JSON.stringify(x).replace(/v1\.(?:20\.0|21\.[01]|22\.0|23\.[01]|24\.0|25\.0|26\.0|27\.0|28\.0)/g, 'v1.19.0'));   // 2026-11-05: v1.21.1 too

/* ============================================================ the live board of 2 Nov 2026 */
const LIVE = [
  { sym: 'ITRN.US', name: 'Ituran Location and Control', sector: 'Technology', px: 39.65, sar: 12199.4, eroi: 35.0, band: 'Medium', g12: 9935, pay: 2110, rel: 74.5, days: 5 },
  { sym: 'DDI.US', name: 'DoubleDown Interactive', sector: 'Communication Services', px: 9.12, sar: 12334.4, eroi: 35.0, band: 'Medium', g12: 10045, pay: 2023, rel: 72.0, days: 5 },
  { sym: 'CRC.US', name: 'California Resources', sector: 'Energy', px: 50.56, sar: 8527.4, eroi: 31.2, band: 'Medium', g12: 6945, pay: 1424, rel: 76.5, days: 5 },
  { sym: 'GLPI.US', name: 'Gaming and Leisure Properties', sector: 'Real Estate', px: 47.21, sar: 12133.4, eroi: 29.2, band: 'High', g12: 9880, pay: 1771, rel: 81.0, days: 4 },
  { sym: 'EFC.US', name: 'Ellington Financial', sector: 'Financial Services', px: 13.05, sar: 11467.4, eroi: 30.4, band: 'High', g12: 9338, pay: 1628, rel: 79.5, days: 4 },
  { sym: 'INDV.US', name: 'Indivior', sector: 'Healthcare', px: 23.43, sar: 12335.4, eroi: 35.0, band: 'Medium', g12: 10060, pay: 2122, rel: 73.0, days: 3 }
];
const OLP = { sym: 'OLP.US', name: 'One Liberty Properties', sector: 'Real Estate', px: 21.5, sar: 9000, eroi: 33.0, band: 'Medium', g12: 7000, pay: 1300, rel: 71.0, days: 2 };
const SYMS = LIVE.map(l => l.sym);
const BY = {}; for (const l of LIVE.concat([OLP])) BY[l.sym] = l;
function liveTicket(L, rank) {
  const fx = 3.75, priceSar = r2(L.px * fx);
  const tp1 = r2(priceSar * (1 + L.pay / L.sar)), tp2 = r2(priceSar * (1 + 2 * L.pay / L.sar)), stop = r2(priceSar * 0.9);
  const shares = Math.floor(L.sar / priceSar);
  return { rank, symbol: L.sym, name: L.name, market: 'NYSE/NASDAQ', sector: L.sector, currency: 'USD', fx_to_sar: fx,
    price: L.px, price_sar: priceSar, entry_zone: r2(priceSar * 0.99) + '-' + r2(priceSar * 1.01) + ' SAR',
    suggested_sar: L.sar, suggested_shares: shares, stop_sar: stop, tp1_sar: tp1, tp2_sar: tp2,
    roi_pct: r1((tp1 / priceSar - 1) * 100), engine_roi_pct: L.eroi, ann_roi_pct: r1(L.g12 / L.sar * 100),
    exp_gain_12m_sar: L.g12, reliability: L.rel, dq: 100, confidence_band: L.band, opportunity_score: 97 - rank,
    detail: { funds_from: 'Cash ' + fmt(L.sar) + ' SAR', review_date: '2026-11-16', rr: 2.2, rr_tp2: 2.9 + rank / 10 },
    advisor_note: 'INVEST ' + DASH + ' ' + fmt(L.sar) + ' SAR (' + shares + ' sh) at ' + priceSar.toFixed(2) + ' SAR; TP1 ' +
      tp1.toFixed(2) + ' / stop ' + stop.toFixed(2) + ' [f1b-observe] gain ann-basis ' + fmt(L.g12) + ' SAR vs plan payoff ' +
      fmt(L.pay) + ' SAR',
    _stab_status: 'ACTIVE (day ' + L.days + ')', _stab_days: L.days, _stab_since: '2026-10-2' + (9 - L.days),
    _stab_smoothed: 90 - rank, _stab_trend: 'steady' };
}
function graceTicket(sym, name, missed, ccy, px, sector) {
  return { _grace_hold: true, _board_unfunded: true, rank: '', symbol: sym, name, market: ccy === 'GBp' ? 'LSE' : 'NYSE/NASDAQ',
    sector, currency: ccy, fx_to_sar: '', price: px, price_sar: '', entry_zone: DASH, suggested_sar: DASH, suggested_shares: DASH,
    stop_sar: DASH, tp1_sar: DASH, tp2_sar: DASH, roi_pct: '', engine_roi_pct: '', ann_roi_pct: '', exp_gain_12m_sar: DASH,
    reliability: '', dq: '', confidence_band: '', detail: {},
    advisor_note: 'Stability grace hold (' + missed + '/3 missed) ' + DASH + ' sizing suspended until it re-qualifies.',
    _stab_status: 'GRACE (' + missed + '/3 missed)', _stab_days: 6, _stab_since: '2026-10-25', _stab_smoothed: 70, _stab_trend: 'down' };
}
function liveCand(t, verdict) {
  return { symbol: t.symbol, name: t.name, market: t.market, sector: t.sector, currency: t.currency, price: t.price,
    price_sar: t.price_sar, roi_pct: t.roi_pct, engine_roi_pct: t.engine_roi_pct, ann_roi_pct: t.ann_roi_pct, rr: 2.2,
    reliability: t.reliability, dq: t.dq, risk_level: 'Low', news_trend: '', sector_trend: '', conflict: false,
    verdict: verdict || 'INVEST', confidence_band: t.confidence_band, opportunity_score: t.opportunity_score,
    first_fail: null, failure_reason: '', structural_block: false, selected: true, deferral: '' };
}
function fillerCand(i) {
  const verdict = ['INVEST', 'WATCH', 'DO_NOT_INVEST', 'BLOCKED'][i % 4];
  return { symbol: 'F' + i + '.US', name: 'Filler ' + i, market: 'US', sector: 'S' + (i % 5), currency: 'USD', price: 1 + i,
    price_sar: (1 + i) * 3.75, roi_pct: 10 + i % 7, engine_roi_pct: 20, ann_roi_pct: 15, rr: 1.5, reliability: 70, dq: 90,
    risk_level: 'Low', news_trend: '', sector_trend: '', conflict: false, verdict, confidence_band: 'Medium',
    opportunity_score: 60 - i, first_fail: verdict === 'INVEST' ? null : { gate: 'Quote Freshness' },
    failure_reason: verdict === 'INVEST' ? '' : 'stale', structural_block: false, selected: false, deferral: '' };
}
/* The SELECTED board after dt10ApplyStability_ and the allocate replay (the replay
 * funded the six; OLP.US and the grace ghosts carry _board_unfunded). */
function livePayload(opts) {
  opts = opts || {};
  const sel = LIVE.map((l, i) => liveTicket(l, i + 1));
  const olp = liveTicket(OLP, 7); olp._board_unfunded = true;
  sel.push(olp);
  sel.push(graceTicket('ZTO.US', 'ZTO Express', 1, 'USD', 19.88, 'Industrials'));
  sel.push(graceTicket('BBOX.L', 'Tritax Big Box', 2, 'GBp', 152.2, 'Real Estate'));
  sel.push(graceTicket('BHF.US', 'Brighthouse Financial', 1, 'USD', 48.1, 'Financial Services'));
  for (const s of opts.unfunded || []) for (const t of sel) if (t.symbol === s) t._board_unfunded = true;
  const eligible = SYMS.concat(['OLP.US']);
  if (opts.mutate) opts.mutate(sel, eligible);
  const cands = sel.filter(t => t._grace_hold !== true).map(t => liveCand(t, 'INVEST'))
    .concat(sel.filter(t => t._grace_hold === true).map(t => liveCand(t, 'WATCH')));
  for (let i = 0; i < 20; i++) cands.push(fillerCand(i));
  return { version: 'tfb-1.30.0', status: 'ok', selected: sel, candidates_rows: cands,
    near_miss: [{ symbol: 'F1.US', failed_gate: 'Quote Freshness', current: 1, required: 2, verdict: 'WATCH', improve_note: 'n1' }],
    alerts: [{ type: 'cash_floor', count: 0, required_action: 'Cash floor (observe): final executable board; 0 of 6 sized seat(s) would lose funding (0 SAR). No ticket changed.' }],
    kpis: { scanned: 9788, passed: 22, deployable_sar: 72747, max_selected: 10, selected_count: 6, fundable_by_rotation: 0,
            capital_call_topn_sar: 0 },
    meta: { route: { version: '4.18.2', pool: { source: 'body_rows', count: 9788 } }, versions: { opportunity_builder: 'tfb-1.30.0' },
            board_funding: { contract_version: 1, stage: 'allocate', finalized: true, allocation_version: 'tfb-1.30.0',
                             eligible_symbols: eligible } } };
}
/* Calendar_Events (Symbol | Next Earnings Date | Days To Earnings | Last Earnings Date |
 * Days Since Earnings | EPS Surprise %), dated relative to the Riyadh day `d0`. */
function isoShift(d0, days) { return new Date(Date.parse(d0 + 'T00:00:00Z') + days * 86400000).toISOString().slice(0, 10); }
function calendarGrid(d0) {
  const fwd = { 'CRC.US': 0, 'EFC.US': 2, 'OLP.US': 3, 'DDI.US': 9, 'ITRN.US': 22, 'BHF.US': 6, 'ZTO.US': 16 };
  const last = { 'INDV.US': [-5, 4.1], 'ZTO.US': [-6, -2.0] };
  const rows = [['Symbol', 'Next Earnings Date', 'Days To Earnings', 'Last Earnings Date', 'Days Since Earnings', 'EPS Surprise %']];
  for (const s of SYMS.concat(['OLP.US', 'ZTO.US', 'BBOX.L', 'BHF.US'])) {
    rows.push([s, fwd.hasOwnProperty(s) ? isoShift(d0, fwd[s]) : '', fwd.hasOwnProperty(s) ? fwd[s] : '',
               last.hasOwnProperty(s) ? isoShift(d0, last[s][0]) : '', last.hasOwnProperty(s) ? -last[s][0] : '',
               last.hasOwnProperty(s) ? last[s][1] : '']);
  }
  return rows;
}
function guardCfg(mode, extra) {
  return Object.assign({ mode, earnDays: 3, ownerRule: false, maxTickets: 0 }, extra || {});
}

/* ============================================================ shim 1: cell-model spreadsheet (REAL dt10RenderPayload_) */
function makeBook() {
  const counts = {}, sheets = {};
  const book = {
    counts, bump(m) { counts[m] = (counts[m] || 0) + 1; },
    getSheetByName(n) { return sheets[n] || null; },
    addSheet(name, maxRows, maxCols) { sheets[name] = makeSheet(book, name, maxRows, maxCols); return sheets[name]; }
  };
  return book;
}
function makeSheet(book, name, maxRows, maxCols) {
  const cells = new Map();
  let merges = [];
  const borders = [];
  const key = (r, c) => r + ':' + c;
  const upd = (r, c, f) => { const k = key(r, c); const cur = Object.assign({}, cells.get(k) || {}); f(cur); cells.set(k, cur); };
  const each = (r, c, nr, nc, fn) => { for (let i = 0; i < nr; i++) for (let j = 0; j < nc; j++) fn(r + i, c + j, i, j); };
  const sheet = {
    _cells: cells, _borders: borders, get _merges() { return merges; },
    getName() { return name; }, getMaxRows() { return maxRows; }, getMaxColumns() { return maxCols; },
    getParent() { return book; },
    getLastRow() { let m = 0; for (const [k, x] of cells) if (x.v !== undefined && x.v !== '') m = Math.max(m, +k.split(':')[0]); return m; },
    getLastColumn() { let m = 0; for (const [k, x] of cells) if (x.v !== undefined && x.v !== '') m = Math.max(m, +k.split(':')[1]); return m; },
    insertColumnsAfter(_after, n) { book.bump('insertColumnsAfter'); maxCols += n; return sheet; },
    getRange(a, b, nr, nc) {
      const [r, c, h, w] = [a, b, nr || 1, nc || 1];
      assert(r >= 1 && c >= 1 && h >= 1 && w >= 1, 'range shape ' + [r, c, h, w]);
      return makeRange(r, c, h, w);
    }
  };
  function makeRange(r, c, nr, nc) {
    const self = {};
    const all = (field, m) => v => { book.bump(m); each(r, c, nr, nc, (R, C) => upd(R, C, x => { x[field] = v; })); return self; };
    const mat = (field, m) => g => {
      book.bump(m);
      assert(Array.isArray(g) && g.length === nr && g.every(row => Array.isArray(row) && row.length === nc), m + ' shape ' + nr + 'x' + nc);
      each(r, c, nr, nc, (R, C, i, j) => upd(R, C, x => { x[field] = g[i][j]; }));
      return self;
    };
    const read = (field, dflt, m) => () => {
      book.bump(m);
      return Array.from({ length: nr }, (_, i) => Array.from({ length: nc }, (_, j) => {
        const v = (cells.get(key(r + i, c + j)) || {})[field];
        return v === undefined || v === null ? dflt : v;
      }));
    };
    Object.assign(self, {
      setValue(v) { book.bump('setValue'); upd(r, c, x => { x.v = v; }); return self; },
      setValues: mat('v', 'setValues'), getValues: read('v', '', 'getValues'),
      setBackground: all('bg', 'setBackground'), setBackgrounds: mat('bg', 'setBackgrounds'), getBackgrounds: read('bg', '#ffffff', 'getBackgrounds'),
      setFontColor: all('fg', 'setFontColor'), setFontColors: mat('fg', 'setFontColors'), getFontColors: read('fg', '#000000', 'getFontColors'),
      setFontWeight: all('fw', 'setFontWeight'), setFontWeights: mat('fw', 'setFontWeights'), getFontWeights: read('fw', 'normal', 'getFontWeights'),
      setFontSize: all('fs', 'setFontSize'), setWrap: all('wrap', 'setWrap'), setFontStyle: all('fst', 'setFontStyle'),
      setNumberFormat: all('nf', 'setNumberFormat'),
      setBorder(...args) { book.bump('setBorder'); borders.push([r, c, nr, nc].concat(args.map(String)).join('|')); return self; },
      merge() { book.bump('merge'); merges.push([r, c, nr, nc].join('|')); return self; },
      breakApart() {
        book.bump('breakApart');
        merges = merges.filter(m => { const [mr, mc, mh, mw] = m.split('|').map(Number);
          return mr + mh - 1 < r || mr > r + nr - 1 || mc + mw - 1 < c || mc > c + nc - 1; });
        return self;
      },
      clear() { book.bump('clear'); each(r, c, nr, nc, (R, C) => cells.delete(key(R, C))); return self; }
    });
    return self;
  }
  return sheet;
}
function snapshot(sheet) {
  const out = {};
  for (const [k, x] of [...sheet._cells.entries()].sort()) out[k] = x;
  return JSON.parse(JSON.stringify({ cells: out, merges: [...sheet._merges].sort(), borders: [...sheet._borders].sort() }));
}
function fakeDate(nowMs) {
  const D = Date;
  function FD(...args) {
    if (!(this instanceof FD)) return new D(nowMs).toString();
    return args.length ? new D(...args) : new D(nowMs);
  }
  FD.prototype = D.prototype;
  FD.now = () => nowMs; FD.UTC = D.UTC; FD.parse = D.parse;
  return FD;
}
function loadRender(source, props, opts) {
  opts = opts || {};
  const logs = [], propReads = [];
  const ctx = {
    Date: fakeDate(opts.now || NOW_R),
    Logger: { log(m) { logs.push(String(m)); } },
    Session: { getScriptTimeZone: () => 'Asia/Riyadh' },
    Utilities: { formatDate(d) { return new Date(d).toISOString().replace('T', ' ').slice(0, 19); } },
    SpreadsheetApp: { BorderStyle: { SOLID: 'SOLID' }, flush() {}, getActiveSpreadsheet() { return null; }, getUi() { throw new Error('no ui'); } },
    PropertiesService: { getScriptProperties() {
      if (opts.propsThrow) throw new Error('property service down');
      return { getProperty(k) { propReads.push(k); return Object.prototype.hasOwnProperty.call(props || {}, k) ? props[k] : null; },
               setProperty() {}, deleteProperty() {} };
    } }
  };
  vm.createContext(ctx);
  vm.runInContext(source, ctx, { filename: '16_Decision_Top10.gs' });
  ctx.__logs = logs; ctx.__propReads = propReads;
  return ctx;
}
/* The live render: DT10_POST_EARN on (the 2 Nov B2 carried 'post-earn 2/10'). */
const LIVE_PROPS = { DT10_POST_EARN: 'on' };
function render(source, opts) {
  opts = opts || {};
  const props = Object.assign({}, opts.noPostEarn ? {} : LIVE_PROPS, opts.props || {});
  const ctx = loadRender(source, props, opts);
  const book = makeBook();
  const sheet = book.addSheet('Top_10_Investments', 400, 30);
  if (opts.seedLabel) sheet.getRange(15, 2).setValue(opts.seedLabel);
  const cal = book.addSheet('Calendar_Events', 200, 10);
  const grid = calendarGrid(opts.calDay || '2026-11-02');
  cal.getRange(1, 1, grid.length, grid[0].length).setValues(grid);
  const calReads = [], calRange = cal.getRange;   // Calendar_Events reads of this render
  cal.getRange = (...a) => { const rg = calRange(...a); const gv = rg.getValues; rg.getValues = () => { calReads.push(a.join('x')); return gv(); }; return rg; };
  const uv = opts.state === 'NOT_ACTIONABLE'
    ? { state: 'NOT_ACTIONABLE', reason: 'final_publication', ageMin: 30, source_readiness: { ready: true, cohort: '39700000001' } }
    : Object.assign(ctx.dt10UvParse_(opts.feedRaw || FEED_RAW, opts.now || NOW_R), { source_readiness: { ready: true, cohort: '39700000001' } });
  ctx.dt10BoardVerdict_ = () => JSON.parse(JSON.stringify(uv));
  const p = opts.payload || livePayload(opts);
  if (opts.tguard) p._dt10_tguard = opts.tguard;
  if (opts.kpi) p._dt10_kpi_plan = { mode: opts.kpi };
  if (opts.venue) p._dt10_venue = { mode: opts.venue };
  if (opts.live) p._dt10_live = opts.live;
  // v1.28.0 [CLEAR-LATE] battery only: a previous board in the zones, the armed key, a fault injected into a step
  for (const [r, c, v] of opts.seedZone || []) sheet.getRange(r, c).setValue(v);
  if (opts.seedMerge) sheet.getRange(...opts.seedMerge).merge();
  if (opts.clearLate) p._dt10_clear_late = 1;
  if (opts.inject) opts.inject(ctx);
  let earn, threw = null;
  try { earn = ctx.dt10RenderPayload_(sheet, p, ctx.DT10_FALLBACK_TOKENS); }
  catch (e) { if (!opts.allowThrow) throw e; threw = String(e && e.message); }
  return { ctx, book, sheet, snap: snapshot(sheet), note: earn && earn.note, payload: p, propReads: ctx.__propReads.slice(),
           logs: ctx.__logs.slice(), token: p._dt10_tguard_note, uv, calReads, threw };
}
function diffCells(a, b) {
  const keys = new Set(Object.keys(a.cells).concat(Object.keys(b.cells)));
  const out = [];
  for (const k of keys) if (JSON.stringify(a.cells[k]) !== JSON.stringify(b.cells[k])) out.push(k);
  return out.sort();
}
function rowOf(snap, sym) {
  for (const k of Object.keys(snap.cells)) { const [r, c] = k.split(':').map(Number); if (c === 2 && r >= 20 && r <= 29 && snap.cells[k].v === sym) return r; }
  return -1;
}
function cellV(snap, r, c) { const x = snap.cells[r + ':' + c]; return x ? x.v : undefined; }
function seatCells(snap, sym) { const r = rowOf(snap, sym); const o = []; for (let c = 1; c <= 30; c++) o.push(cellV(snap, r, c)); return o; }
function kpiRow(snap) { const o = []; for (let c = 1; c <= 11; c++) o.push(cellV(snap, 16, c)); return o; }
function banner(snap) { return String(cellV(snap, 18, 1)); }
function execOf(ctx, payload) { return ctx.dt10TicketClasses_(payload.selected).exec; }

/* ============================================================ shim 2: full workbook (REAL refreshDecisionTop10) */
class FixedDate extends Date {
  constructor(...a) { super(...(a.length ? a : [NOW_F])); }
  static now() { return NOW_F; }
}
function fmtDate(date, zone, pattern) {
  const off = zone === 'Etc/UTC' || zone === 'UTC' ? 0 : 3 * 3600000;
  const iso = new Date(date.getTime() + off).toISOString();
  if (pattern === 'yyyy-MM-dd') return iso.slice(0, 10);
  if (pattern === 'yyyy-MM-dd HH:mm') return iso.replace('T', ' ').slice(0, 16);
  return iso.replace('T', ' ').slice(0, 19);
}
function mkSheet(env, name, opts) {
  opts = opts || {};
  const cells = {};
  let hidden = false;
  const g = opts.grid || null;
  const sh = {
    _name: name, _cells: cells, _a1: opts.a1 || {},
    _val(r, c) { const k = r + ':' + c; return Object.prototype.hasOwnProperty.call(cells, k) ? cells[k] : ''; },
    _set(r, c, v) { cells[r + ':' + c] = v; },
    _log(method, r, c, nr, nc, values) { env.calls.push([method, name, r, c, nr, nc, values === undefined ? null : JSON.stringify(values)]); },
    getName() { return name; }, getParent() { return env.ss; },
    getRange(a, b, nr, nc) { if (typeof a === 'string') return mkRange(sh, 1, 1, 60, 2, a); return mkRange(sh, a, b, nr, nc, null); },
    getDataRange() { return mkRange(sh, 1, 1, g ? g.length : 1, g ? g[0].length : 1, null); },
    getLastRow() { if (g) return g.length; let m = 0; for (const k of Object.keys(cells)) m = Math.max(m, +k.split(':')[0]); return m; },
    getLastColumn() { if (g) return g[0].length; let m = 0; for (const k of Object.keys(cells)) m = Math.max(m, +k.split(':')[1]); return m; },
    getMaxRows() { return opts.maxRows || 1000; }, getMaxColumns() { return sh._maxCols; },
    appendRow(row) { sh._log('appendRow', 0, 0, 1, row.length, row); env.appended[name] = (env.appended[name] || []); env.appended[name].push(plain(row)); return sh; },
    insertRows(a, n) { sh._log('insertRows', a, 0, n || 1, 0); return sh; },
    insertRowsAfter(a, n) { sh._log('insertRowsAfter', a, 0, n, 0); return sh; },
    insertColumnsAfter(a, n) { sh._log('insertColumnsAfter', 0, a, 0, n); sh._maxCols += n; return sh; },
    deleteRows(a, n) { sh._log('deleteRows', a, 0, n, 0); return sh; },
    deleteColumns(a, n) { sh._log('deleteColumns', 0, a, 0, n); return sh; },
    setColumnWidth(c, w) { sh._log('setColumnWidth', 0, c, 0, 0, w); return sh; },
    setColumnWidths(c, n, w) { sh._log('setColumnWidths', 0, c, 0, n, w); return sh; },
    setFrozenRows(n) { sh._log('setFrozenRows', n, 0, 0, 0); return sh; },
    autoResizeColumns() { sh._log('autoResizeColumns', 0, 0, 0, 0); return sh; },
    hideSheet() { sh._log('hideSheet', 0, 0, 0, 0); hidden = true; return sh; },
    showSheet() { sh._log('showSheet', 0, 0, 0, 0); hidden = false; return sh; },
    isSheetHidden() { return hidden; },
    clear() { sh._log('sheet.clear', 0, 0, 0, 0); for (const k of Object.keys(cells)) delete cells[k]; return sh; },
    clearContents() { sh._log('sheet.clearContents', 0, 0, 0, 0); for (const k of Object.keys(cells)) delete cells[k]; return sh; },
    getSheetId() { return 1; }
  };
  sh._maxCols = opts.maxCols || 26;
  if (opts.seed) for (const k of Object.keys(opts.seed)) cells[k] = opts.seed[k];
  if (g) sh._grid = g;
  return sh;
}
function mkRange(sheet, r, c, nr, nc, a1) {
  const env = sheet.getParent()._env;
  const log = (method, values) => sheet._log(method, r, c, nr || 1, nc || 1, values);
  const self = {
    getValue() { env.reads.push(['getValue', sheet._name, r, c]); return sheet._val(r, c); },
    getValues() {
      env.reads.push(['getValues', sheet._name, a1 || r, a1 ? 0 : c, nr || 1, nc || 1]);
      if (a1 && sheet._a1[a1]) return plain(sheet._a1[a1]);
      if (sheet._grid) return plain(sheet._grid);
      const out = [];
      for (let i = 0; i < (nr || 1); i++) { const row = []; for (let j = 0; j < (nc || 1); j++) row.push(sheet._val(r + i, c + j)); out.push(row); }
      return out;
    },
    setValue(v) { log('setValue', v); sheet._set(r, c, v); return self; },
    setValues(vv) { log('setValues', vv); for (let i = 0; i < vv.length; i++) for (let j = 0; j < vv[i].length; j++) sheet._set(r + i, c + j, vv[i][j]); return self; },
    getNumRows() { return nr || 1; }, getNumColumns() { return nc || 1; }, getRow() { return r; }, getColumn() { return c; }
  };
  const gridOf = def => Array.from({ length: nr || 1 }, () => Array.from({ length: nc || 1 }, () => def));
  self.getBackgrounds = () => gridOf('#ffffff'); self.getFontColors = () => gridOf('#000000'); self.getFontWeights = () => gridOf('normal');
  for (const m of ['setNumberFormat', 'setNumberFormats', 'setWrap', 'setFontSize', 'setBackground', 'setBackgrounds', 'setFontColor',
    'setFontColors', 'setFontWeight', 'setFontWeights', 'setFontStyle', 'setHorizontalAlignment', 'setBorder', 'clear', 'clearContent',
    'clearFormat', 'merge', 'breakApart', 'setNote']) {
    self[m] = (...a) => { log(m, a); return self; };
  }
  return self;
}
const STATUS_HEADER = ['Page', 'Last Updated', 'Status', 'Message', 'Endpoint', 'HTTP Code', 'Rows', 'Columns', 'Duration ms', 'Warnings'];
function makeEnv(opts) {
  opts = opts || {};
  const props = Object.assign({}, opts.props || {});
  const env = { calls: [], reads: [], appended: {}, propReads: { total: 0, byKey: {} }, props, sheets: {}, options: opts, logs: [], posted: [], cons: [], sets: [] };
  const ss = {
    _env: env,
    getSheetByName(n) { return env.sheets[n] || null; },
    getRangeByName(n) {
      env.reads.push(['getRangeByName', n]);
      if (n === 'TFB_PANEL_DEFAULTS') return { getValues() { return plain(FIX.lists_defaults); } };
      if (n === 'TFB_FX_LOOKUP') return { getValues() { return plain(FIX.fx_rows); } };
      return null;
    },
    getSheets() { return Object.keys(env.sheets).map(k => env.sheets[k]); },
    getId() { return 'stub'; }, getSpreadsheetTimeZone() { return 'Asia/Riyadh'; },
    insertSheet(n) { env.calls.push(['insertSheet', n, 0, 0, 0, 0, null]); env.sheets[n] = mkSheet(env, n, { maxRows: 1000, maxCols: 40 }); return env.sheets[n]; },
    setActiveSheet(sh) { env.calls.push(['setActiveSheet', sh && sh.getName(), 0, 0, 0, 0, null]); return ss; }
  };
  env.ss = ss;
  env.sheets['Top_10_Investments'] = mkSheet(env, 'Top_10_Investments', { maxRows: 800, maxCols: 30,
    seed: Object.assign({ '4:1': 'CONTROL PANEL', '14:1': 'KPIs' }, opts.seedTop10 || {}) });
  env.sheets['_Status'] = mkSheet(env, '_Status', { grid: [STATUS_HEADER].concat(['Market_Leaders', 'Global_Markets', 'Commodities_FX', 'Mutual_Funds'].map(p =>
    [p, '2026-10-10 21:19:47+03:00', 'SUCCESS', 'leg=success run=' + FIX.export_run, 'backend:run_dashboard_sync', '', 0, 115, 0, 0])),
    a1: { 'L1:M60': plain(FIX.status_lm) } });
  env.sheets['_Run_Log'] = mkSheet(env, '_Run_Log', {});
  env.sheets['Portfolio_Decision'] = mkSheet(env, 'Portfolio_Decision', { grid: plain(FIX.pd_block) });
  env.sheets['Calendar_Events'] = mkSheet(env, 'Calendar_Events', { grid: calendarGrid(TODAY_F) });
  env.sheets['My_Portfolio'] = mkSheet(env, 'My_Portfolio', { grid: [plain(PAGES.Global_Markets.header)] });
  for (const pg of ['Market_Leaders', 'Global_Markets', 'Commodities_FX', 'Mutual_Funds']) {
    env.sheets[pg] = mkSheet(env, pg, { grid: [PAGES[pg].header].concat(plain(PAGES[pg].rows)) });
  }
  const propsService = {
    getProperty(k) { env.propReads.total++; env.propReads.byKey[k] = (env.propReads.byKey[k] || 0) + 1; return Object.prototype.hasOwnProperty.call(props, k) ? props[k] : null; },
    setProperty(k, v) { env.sets.push([k, String(v)]); props[k] = String(v); return propsService; },
    deleteProperty(k) { delete props[k]; return propsService; },
    getProperties() { return props; }
  };
  env.ctx = {
    console: { log(m) { env.cons.push(String(m)); } }, Logger: { log(m) { env.logs.push(String(m)); } }, Date: opts.dateClass || FixedDate,
    Utilities: { formatDate: fmtDate, sleep() {}, getUuid() { return 'test-uuid'; },
      computeDigest(alg, value) { return [...crypto.createHash(alg === 'SHA_256' ? 'sha256' : 'md5').update(value).digest()]; },
      base64Encode() { return ''; }, DigestAlgorithm: { MD5: 'MD5', SHA_256: 'SHA_256' }, Charset: { UTF_8: 'UTF_8' } },
    PropertiesService: {
      getScriptProperties() { if (env.options.propsThrow) throw new Error('synthetic property outage'); return propsService; },
      getDocumentProperties() { return null; }, getUserProperties() { return propsService; }
    },
    SpreadsheetApp: { getActiveSpreadsheet() { return ss; }, getActive() { return ss; }, getUi() { throw new Error('no UI'); }, openById() { return ss; }, flush() {}, BorderStyle: { SOLID: 'SOLID' } },
    UrlFetchApp: { fetch() { throw new Error('no network'); } },
    Session: { getScriptTimeZone() { return 'Asia/Riyadh'; } },
    CacheService: { getScriptCache() { return { get() { return null; }, put() {} }; } },
    LockService: { getScriptLock() { return { tryLock() { return true; }, releaseLock() {} }; } }
  };
  return env;
}
/* Stability state the day before (2026-10-09): all ten names confirmed members;
 * the smoothed scores order the board ITRN, DDI, CRC, GLPI, EFC, INDV, OLP, ZTO, BBOX, BHF. */
const REFRESH_ORDER = SYMS.concat(['OLP.US', 'ZTO.US', 'BBOX.L', 'BHF.US']);
function seedR() {
  const symbols = {};
  REFRESH_ORDER.forEach((s, i) => { symbols[s] = { ci: 3, co: 0, member: true, since: '2026-10-0' + (5 - (i % 3)), ls: '2026-10-09', hist: [96 - i], ft: false }; });
  return { v: 1, date: '2026-10-09', symbols };
}
function researchTicket(L, i) {
  const t = liveTicket(L, i + 1);
  delete t.rank; delete t._stab_status; delete t._stab_days; delete t._stab_since; delete t._stab_smoothed; delete t._stab_trend;
  t.opportunity_score = 96 - i;
  return t;
}
function rawR() { return SYMS.concat(['OLP.US']).map((s, i) => researchTicket(BY[s], i)); }
function gridR() {
  const g = rawR().map(t => liveCand(t, 'INVEST'));
  ['ZTO.US', 'BBOX.L', 'BHF.US'].forEach((s, i) => g.push(Object.assign(liveCand({ symbol: s, name: s, market: 'NYSE/NASDAQ', sector: 'Industrials',
    currency: 'USD', price: 20, price_sar: 75, roi_pct: 5, engine_roi_pct: 10, ann_roi_pct: 8, reliability: 70, dq: 100,
    confidence_band: 'Medium', opportunity_score: 89 - i }, 'WATCH'), { selected: false })));
  for (let i = 0; i < 6; i++) g.push(fillerCand(i));
  return g;
}
const BUILDER = 'tfb-1.30.0';
function postStub(env) {
  return function (body) {
    env.posted.push(plain(body.criteria));
    if (body.criteria.board_funding_stage === 'allocate') {
      const eligible = body.criteria.board_funding_symbols || [];
      const sized = eligible.filter(s => s !== 'OLP.US').map(s => researchTicket(BY[s], SYMS.indexOf(s)));
      return { code: 200, json: { version: BUILDER, status: 'ok', selected: sized, candidates_rows: [], near_miss: [],
        alerts: [], kpis: { scanned: 9788, passed: 22 }, meta: { board_funding: { contract_version: 1, stage: 'allocate', snapshot_id: 'sid-1' } } } };
    }
    return { code: 200, json: { version: BUILDER, status: 'ok', selected: rawR(), candidates_rows: gridR(), near_miss: [], alerts: [],
      kpis: { scanned: 9788, passed: 22, max_selected: 10, selected_count: 7, deployable_sar: 72747 },
      meta: { route: { version: '4.18.2', pool: { source: 'body_rows', count: 9788 } }, versions: { opportunity_builder: BUILDER },
              board_funding: { contract_version: 1, stage: 'research', snapshot_available: true, snapshot_id: 'sid-1',
                               snapshot: { builder_version: BUILDER, snapshot_id: 'sid-1', rows: [] } } } } };
  };
}
function refresh(source, props, opts) {
  opts = opts || {};
  const env = makeEnv({ props: Object.assign({ DT10_STAB_STATE_V1: JSON.stringify(seedR()) }, props || {}), propsThrow: opts.propsThrow, dateClass: opts.dateClass,
                        seedTop10: opts.seedTop10 });
  vm.createContext(env.ctx);
  vm.runInContext(source, env.ctx, { filename: '16_Decision_Top10.gs' });
  env.ctx.dt10StabToday_ = () => TODAY_F;
  const top10 = env.sheets['Top_10_Investments'];
  env.ctx.DT10_PANEL.forEach((item, i) => {
    const pos = env.ctx.dt10PanelPos_(i);
    top10._set(pos.row, pos.labelCol, item.label);
    if (Object.prototype.hasOwnProperty.call(FIX.panel, item.label)) top10._set(pos.row, pos.valueCol, FIX.panel[item.label]);
    else if (item.kind === 'sar') top10._set(pos.row, pos.valueCol, 0);
  });
  env.ctx.dt10BoardVerdict_ = () => ({ state: 'EXECUTABLE', reason: '', ageMin: 286, source_readiness: { ready: true, cohort: FIX.export_run, reason: '', issues: [], generation: {} } });
  env.ctx.dt10Post_ = postStub(env);
  if (opts.inject) opts.inject(env.ctx);   // v1.28.0 [CLEAR-LATE] battery only
  env.ctx.refreshDecisionTop10();
  const seatRows = env.calls.filter(x => x[0] === 'setValues' && x[1] === 'Top_10_Investments' && x[2] === 20).map(x => JSON.parse(x[6]));
  return { env, top10, status: String(top10._val(2, 2)), calls: env.calls.slice(), runLog: plain(env.appended['_Run_Log'] || []),
           selLog: env.calls.filter(x => x[1] === '_Selection_Log' && x[0] === 'setValues').map(x => JSON.parse(x[6])),
           cons: env.cons.slice(), sets: plain(env.sets),
           logs: env.logs.slice(), posted: env.posted.slice(), propReads: plain(env.propReads), props: plain(env.props),
           seats: seatRows.length ? seatRows[0] : [], banner: String(top10._val(18, 1)),
           kpis: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11].map(c => top10._val(16, c)), label2: top10._val(15, 2),
           saved: JSON.parse(env.props.DT10_STAB_STATE_V1) };
}
function seatRow(run, sym) { for (const r of run.seats) if (r[1] === sym) return r; return null; }
const stripTok = (s, tok) => String(s).replace(' | ' + tok, '');



/* ============================================================ v1.29.0 battery */
const TIMING = 'DT10_RENDER_TIMING', REUSE = 'DT10_VERDICT_REUSE';
const MARKS = ['setup', 'gates', 'inputs', 'funding', 'src', 'pool', 'ready', 'post', 'attach', 'verdict1', 'stab',
  'verdict2', 'alloc', 'final', 'r_clear', 'r_verdict', 'r_final', 'r_live', 'r_earn', 'r_postearn', 'r_guard', 'r_kpi',
  'r_selected', 'r_qualified', 'r_nearmiss', 'r_alerts', 'r_gaps', 'r_cands', 'r_end', 'render', 'notes', 'sellog',
  'exitlog', 'shadow', 'status'];
const NOTE_ARMED = 'source readiness checked after the POST (DT10_VERDICT_REUSE)';
const SELF_TAIL = w => '; verdict reuse (DT10_VERDICT_REUSE): ' + w + ' (no log-only source check before the POST; ' +
  'verdict 1 stands for verdict 2; the pre-paint and selection-log rechecks stay)';
/* comparisons against the base read the delivered version as the base's */
const n29 = x => JSON.parse(JSON.stringify(x).replace(/1\.29\.0/g, '1.28.0'));
function census(src, re) { const out = new Set(); let m; while ((m = re.exec(src))) out.add(m[1]); return out; }
function ctxOf(src, props, throwIt) {
  const statusRows = []; for (let r = 0; r < 60; r++) statusRows.push(['', '']);
  const ss = { getSheetByName(n) { return n === '_Status' ? { getRange() { return { getValues() { return plain(statusRows); } }; } } : null; }, getRangeByName() { return null; } };
  const ctx = { Logger: { log() {} }, console: { log() {} }, SpreadsheetApp: { getActiveSpreadsheet() { return ss; }, getUi() { throw new Error('no ui'); } },
    PropertiesService: { getScriptProperties() { if (throwIt) throw new Error('down'); return { getProperty(k) { return Object.prototype.hasOwnProperty.call(props, k) ? props[k] : null; }, setProperty() {}, deleteProperty() {} }; } },
    Utilities: { formatDate() { return '2026-11-09 00:00:00'; } }, Session: { getScriptTimeZone() { return 'Asia/Riyadh'; } }, UrlFetchApp: { fetch() { throw new Error('no net'); } } };
  vm.createContext(ctx); vm.runInContext(src, ctx, { filename: '16_Decision_Top10.gs' });
  return ctx;
}
/* a refresh with call counters on the board verdict (the harness stub) and the REAL direct source readiness */
function counted(src, props, opts) {
  const counts = { verdict: 0, readiness: 0 };
  const o = Object.assign({}, opts || {}), prev = o.inject;
  o.inject = ctx => {
    const v = ctx.dt10BoardVerdict_, r = ctx.dt10SourceReadiness_;
    ctx.dt10BoardVerdict_ = function () { counts.verdict++; return v.apply(this, arguments); };
    ctx.dt10SourceReadiness_ = function () { counts.readiness++; return r.apply(this, arguments); };
    if (prev) prev(ctx);
  };
  const run = refresh(src, props, o);
  run.counts = counts;
  return run;
}
/* the sources go blocked while stability runs: verdict 1 EXECUTABLE, every later verdict NOT_ACTIONABLE */
function lateBlock(ctx) {
  const stab = ctx.dt10ApplyStability_, v = ctx.dt10BoardVerdict_;
  let after = false;
  ctx.dt10ApplyStability_ = function () { const out = stab.apply(this, arguments); after = true; return out; };
  ctx.dt10BoardVerdict_ = function () {
    if (!after) return v.apply(this, arguments);
    return { state: 'NOT_ACTIONABLE', reason: 'source readiness: Global_Markets publication in progress', ageMin: 286, source_blocked: true,
             source_readiness: { ready: false, cohort: '', reason: 'Global_Markets publication in progress', issues: [], generation: {} } };
  };
}
function checkGolden(src, out) {
  const PROPSETS = [{}, { [REUSE]: 'off' }, { [REUSE]: '' }, { [REUSE]: 'yes' }, { [REUSE]: '1' }, { [REUSE]: 'true' }, { [REUSE]: 'onn' },
                    { [REUSE]: 'no', [TIMING]: 'on' }, { [TIMING]: 'on' }];
  for (const props of PROPSETS) {
    const n = counted(src, props), b = counted(baseSource, props), tag = JSON.stringify(props);
    eq(n29(n.status), b.status, 'B2 ' + tag);
    deq(n29(n.calls), b.calls, 'sheet writes ' + tag);
    deq(n29(n.runLog), b.runLog, '_Run_Log ' + tag);
    deq(n29(n.selLog), b.selLog, '_Selection_Log ' + tag);
    deq(n29(n.logs), b.logs, 'Logger lines ' + tag);
    deq(n29(n.cons), b.cons, 'console lines ' + tag);
    deq(n.posted, b.posted, 'POST bodies ' + tag);
    deq(n.saved, b.saved, 'saved state ' + tag);
    deq(n.counts, b.counts, 'the same verdicts and readiness checks ' + tag);
    eq(n.propReads.total, b.propReads.total + 1, 'one extra property read ' + tag);
    eq(n.propReads.byKey[REUSE], 1, 'the knob is read once ' + tag);
    if (out && !Object.keys(props).length) out.push(['off', n.counts, n.status.length, n.logs.length]);
  }
  const nf = counted(src, { [REUSE]: 'on' }, { propsThrow: true }), bf = counted(baseSource, { [REUSE]: 'on' }, { propsThrow: true });
  eq(n29(nf.status), bf.status, 'B2 faulted'); deq(n29(nf.calls), bf.calls, 'writes faulted'); deq(n29(nf.logs), bf.logs, 'Logger faulted');
  deq(nf.counts, bf.counts, 'counts faulted');
}
function checkArmed(src, out) {
  const off = counted(src, {});
  for (const val of ['on', ' ON ', 'On']) {
    const a = counted(src, { [REUSE]: val }), tag = JSON.stringify(val);
    eq(a.status, off.status, 'B2 unchanged ' + tag);
    deq(a.calls, off.calls, 'every sheet write unchanged ' + tag);
    deq(a.runLog, off.runLog, '_Run_Log unchanged ' + tag);
    deq(a.selLog, off.selLog, '_Selection_Log unchanged ' + tag);
    deq(a.posted, off.posted, 'POST bodies unchanged ' + tag);
    deq(a.saved, off.saved, 'saved state unchanged ' + tag);
    deq(a.cons, off.cons, 'console lines unchanged ' + tag);
    const ap = plain(a.props); delete ap[REUSE];
    deq(ap, off.props, 'properties after the run unchanged ' + tag);
    eq(a.logs.length, off.logs.length, 'Logger line count ' + tag);
    const diffs = [];
    a.logs.forEach((l, i) => { if (l !== off.logs[i]) diffs.push([l, off.logs[i]]); });
    eq(diffs.length, 1, 'one Logger line differs ' + tag);
    ok(/\] POST \//.test(diffs[0][0]), 'the POST line ' + tag);
    ok(diffs[0][0].endsWith(' | ' + NOTE_ARMED), 'the deferred readiness note ' + tag + ': ' + diffs[0][0].slice(-120));
    const head = s => s.slice(0, s.lastIndexOf(' | source readiness '));
    eq(head(diffs[0][0]), head(diffs[0][1]), 'the rest of the POST line ' + tag);
    eq(a.counts.verdict, off.counts.verdict - 1, 'one board verdict fewer ' + tag);
    eq(a.counts.readiness, off.counts.readiness - 1, 'one direct source readiness fewer ' + tag);
    ok(a.counts.verdict >= 2, 'verdict 1 and the pre-paint recheck still run ' + tag);
    if (out && val === 'on') out.push(['armed', a.counts, off.counts, diffs[0][0].slice(-80)]);
  }
  const t = counted(src, { [REUSE]: 'on', [TIMING]: 'on' });
  const det = JSON.parse(t.runLog[t.runLog.length - 1][9]);
  deq(det.marks.map(m => m[0]), MARKS, 'with the timer: the 35 marks keep their names');
  eq(det.version, '1.29.0', 'TIMING details version');
}
function checkLateBlock(src, out) {
  const off = counted(src, {}, { inject: lateBlock }), on = counted(src, { [REUSE]: 'on' }, { inject: lateBlock });
  for (const [tag, r] of [['off', off], ['armed', on]]) {
    ok(/WITHHELD/.test(r.status), 'B2 WITHHELD (' + tag + '): ' + r.status.slice(0, 160));
    ok(r.runLog.some(row => row[3] === 'Top_10_Investments' && row[1] === 'INFO' && /WITHHELD/.test(String(row[4]) + String(row[5]))) || /WITHHELD/.test(r.status), 'run log or status names the withhold (' + tag + ')');
    for (const row of r.selLog.length ? r.selLog[0] : []) ok(!/EXECUTABLE/.test(JSON.stringify(row)) || /withh/i.test(JSON.stringify(row)), 'selection log row withheld (' + tag + ')');
  }
  eq(on.status.replace(/^.*?\| status: /, ''), off.status.replace(/^.*?\| status: /, ''), 'the same B2 status text after the stamp');
  deq(on.seats, off.seats, 'the same seats painted');
  eq(on.banner, off.banner, 'the same banner');
  deq(on.kpis, off.kpis, 'the same KPI strip');
  if (out) out.push(['late', on.banner, on.kpis]);
}
function checkSelfTest(src, out) {
  for (const [props, thr, want] of [[{}, false, 'off'], [{ [REUSE]: 'on' }, false, 'on'], [{ [REUSE]: ' ON ' }, false, 'on'],
                                    [{ [REUSE]: 'yes' }, false, 'off'], [{ [REUSE]: 'on' }, true, 'off']]) {
    const tag = JSON.stringify([props, thr]);
    const rep = String(ctxOf(src, props, thr).dt10SelfTest()), base = String(ctxOf(baseSource, props, thr).dt10SelfTest());
    const lines = rep.split('\n'), baseLines = base.split('\n');
    eq(lines.length, baseLines.length, 'line count = the base ' + tag);
    const i = lines.findIndex(l => l.indexOf('render timing (DT10_RENDER_TIMING): ') === 0);
    ok(i >= 0, 'the render timing line ' + tag);
    ok(lines[i].endsWith(SELF_TAIL(want)), 'the knob is named ' + tag + ': ' + lines[i].slice(-140));
    eq(lines[i].slice(0, lines[i].length - SELF_TAIL(want).length), baseLines[i], 'the rest of the line is the base line ' + tag);
    deq(n29(lines.filter((l, k) => k !== i)), baseLines.filter((l, k) => k !== i), 'every other line = the base ' + tag);
    if (out && want === 'on' && !thr) out.push(lines[i].slice(-60));
  }
}
function checkLint() {
  ok(newSource.indexOf('\r') < 0, 'LF only');
  for (const [cur, prev] of EDITS) {
    const na = s => s.split('').filter(c => c.charCodeAt(0) > 127).sort().join('');
    eq(na(cur), na(prev), 'no new non-ASCII character in a hunk: ' + cur.split('\n')[0].slice(0, 80));
    const added = cur.split('\n').filter(l => prev.split('\n').indexOf(l) < 0);
    const code = added.filter(l => !/^\s*(\*|\/\*|\/\/)/.test(l)).map(l => l.replace(/\/\/.*$/, '')).join('\n');
    ok(!/=>|`|\blet\s|\bconst\s|\bclass\s/.test(code), 'ES5 in the hunk: ' + code.slice(0, 120));
  }
  const top = newSource.slice(0, newSource.indexOf(' * v1.27.0 (2026-11-08)'));
  ok(top.indexOf(' * v1.29.0 (2026-11-09) [VERDICT-REUSE]') > 0 && top.indexOf(' * v1.29.0 (2026-11-09)') < top.indexOf(' * v1.28.0 (2026-11-09)'), 'the WHY block is newest-first');
  const baseHead = baseSource.slice(0, baseSource.indexOf('*/'));
  for (const l of baseHead.split('\n')) if (!/^ \* Version: /.test(l)) ok(newSource.indexOf(l) >= 0, 'prior WHY line kept: ' + l.slice(0, 90));
}
const MUTANTS = [
  ['verdict 2 always recomputed', "  if (!verdictReuse) payload._dt10_uv = dt10BoardVerdict_(ss, sourceContext);\n", "  payload._dt10_uv = dt10BoardVerdict_(ss, sourceContext);\n"],
  ['verdict 2 skipped while off', "  if (!verdictReuse) payload._dt10_uv = dt10BoardVerdict_(ss, sourceContext);\n", "  if (verdictReuse) payload._dt10_uv = dt10BoardVerdict_(ss, sourceContext);\n"],
  ['pre-POST readiness always computed', "dt10PoolNote_(pool, verdictReuse ? null : dt10SourceReadiness_(ss, sourceContext));", "dt10PoolNote_(pool, dt10SourceReadiness_(ss, sourceContext));"],
  ['any value arms', "        .getProperty(DT10_VERDICT_REUSE_PROP) || '')\n        .replace(/^\\s+|\\s+$/g, '').toLowerCase() === 'on';", "        .getProperty(DT10_VERDICT_REUSE_PROP) || '')\n        .replace(/^\\s+|\\s+$/g, '').toLowerCase() !== '';"],
  ['a fault arms', "  } catch (eVr) {}\n  return false;", "  } catch (eVr) { return true; }\n  return false;"],
  ['the deferred note reads VERIFIED', "(source === null ? 'checked after the POST (DT10_VERDICT_REUSE)' :", "(source === null ? 'VERIFIED' :"],
  ['the knob hard-wired on', "  var verdictReuse = dt10VerdictReuseOn_();", "  var verdictReuse = true;"]
];
function signature(src) {
  const off = counted(src, {}), on = counted(src, { [REUSE]: 'on' }), junk = counted(src, { [REUSE]: 'yes' }), fault = counted(src, { [REUSE]: 'on' }, { propsThrow: true });
  const st = String(ctxOf(src, { [REUSE]: 'on' }, true).dt10SelfTest()).split('\n').filter(l => /^render timing/.test(l))[0];
  return sha256(JSON.stringify([off.counts, off.logs, on.counts, on.logs, junk.counts, junk.logs, fault.counts, fault.logs, off.status, on.status, st]));
}

function battery() {
  passed = 0; failed = 0;
  const out = [];
  test('V0 base oracle is the reviewed v1.28.0 file (SHA-256 pinned); versions in lockstep; both compile; census +1 function +1 var, 0 removed; 10 hunks reverse to the base', () => {
    eq(sha256(baseSource), history.V1280_SHA256, 'base SHA-256');
    eq(history.V1280_SHA256, '03588d67a7ed94ce5a197f0c56cf321f84aaaed6f52baca14e2d813a3163301e', 'pinned v1.28.0 (origin/main 772a497)');
    ok(/^ \* Version: 1\.29\.0 /.test(newSource.split('\n')[3]) && newSource.indexOf("var DT10_VERSION = '1.29.0';") > 0, 'delivered version lockstep');
    ok(/^ \* Version: 1\.28\.0 /.test(baseSource.split('\n')[3]) && baseSource.indexOf("var DT10_VERSION = '1.28.0';") > 0, 'base version lockstep');
    new vm.Script(newSource); new vm.Script(baseSource);
    const fb = census(baseSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), fn = census(newSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    deq([...fb].filter(k => !fn.has(k)), [], 'functions removed');
    deq([...fn].filter(k => !fb.has(k)).sort(), ['dt10VerdictReuseOn_'], 'functions added');
    const vb = census(baseSource, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = census(newSource, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq([...vb].filter(k => !vn.has(k)), [], 'vars removed');
    deq([...vn].filter(k => !vb.has(k)).sort(), ['DT10_VERDICT_REUSE_PROP'], 'vars added');
    eq(fn.size, 280, 'function census'); eq(vn.size, 165, 'var census');
    eq(EDITS.length, 10, 'hunk count');
    let s = newSource;
    for (const [cur, prev] of EDITS) { eq(s.split(cur).length - 1, 1, 'one hunk: ' + cur.split('\n')[0].slice(0, 80)); s = s.replace(cur, () => prev); }
    eq(sha256(s), history.V1280_SHA256, 'the 10 hunks reverse to the base');
  });
  test('V1 GOLDEN NEGATIVE / kill identity: the property absent / off / blank / junk / faulted (timer on or off) -> the REAL refresh deep-equals the v1.28.0 base; the same verdict and readiness calls; one extra property read', () => checkGolden(newSource, out));
  test('V2 armed (on, " ON ", On): the same B2, writes, logs, POST bodies, state and properties; only the POST Logger line differs (deferred readiness); one board verdict and one direct readiness fewer; the 35 marks keep their names', () => checkArmed(newSource, out));
  test('V3 the sources go blocked during the stability step: off and armed both end WITHHELD with the same board, banner, KPIs and seats (the pre-paint recheck catches it when verdict 2 is reused)', () => checkLateBlock(newSource, out));
  test('V4 embedded dt10SelfTest: the render-timing line names the knob (off / on / spellings / faulted); line count and every other line are the base', () => checkSelfTest(newSource, out));
  test('V5 lint: no new non-ASCII character and ES5 in every hunk; LF only; the WHY block newest-first; every prior WHY line kept', () => checkLint());
  test('V6 mutants: ' + MUTANTS.length + ' one-line mutations are each caught', () => {
    const want = signature(newSource);
    for (const [label, from, to] of MUTANTS) {
      eq(newSource.split(from).length - 1, 1, 'mutant anchor unique: ' + label);
      const m = newSource.replace(from, () => to);
      let caught = false;
      try { caught = signature(m) !== want; } catch (e) { caught = true; }
      ok(caught, 'mutant not caught: ' + label);
    }
    out.push(want);
  });
  return sha256(JSON.stringify(out));
}
const digests = [];
for (let run = 0; run < 3; run++) {
  const d = battery();
  digests.push(d);
  if (failed) { console.log('FAIL ' + passed + '/' + (passed + failed)); process.exit(1); }
}
if (!(digests[0] === digests[1] && digests[1] === digests[2])) { console.log('FAIL digest mismatch ' + digests.join(' ')); process.exit(1); }
console.log('digest ' + digests[0].slice(0, 16) + ' x3 identical');
console.log('PASS ' + passed + '/' + (passed + failed));
