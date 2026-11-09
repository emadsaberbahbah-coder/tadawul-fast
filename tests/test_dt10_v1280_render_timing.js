#!/usr/bin/env node
/* tests/test_dt10_v1280_render_timing.js
 * Harness for apps_script/16_Decision_Top10.gs v1.28.0 [RENDER-TIMING] +
 * [CLEAR-LATE] (morning review, Mon 9 Nov 2026: the 08:44 evidence follow-up
 * render and the 09:12 Decision Pages v2 build were stopped at the Apps
 * Script 6-minute limit; 2-9 ABORTED_PREV rows a day since 4 Nov; the 10:14
 * follow-up was stopped after the zone clear and left an empty board). Two
 * Script Properties, DT10_RENDER_TIMING and DT10_CLEAR_LATE, default OFF.
 *
 * Dual-tree node harness over the REAL file: the delivered .gs and the
 * v1.27.0 BASE (a file given as the second argument, or the base rebuilt
 * from the delivered file by reversing exactly the 38 enumerated v1.28.0
 * edits, tests/js/dt10_edits_v1280.json, SHA-256 pinned) run the REAL
 * refreshDecisionTop10 in the full-workbook shim of
 * tests/test_dt10_v1200_ticket_guard.js (the 2 Nov board; POST stub for the
 * research call and the allocate replay), with console, setProperty and the
 * clock captured.
 * Sections:
 *   R0 base oracle (SHA-256 pinned), versions in lockstep, both compile;
 *      census +7 functions +4 vars, 0 removed; 38 hunks reverse to the base
 *   R1 GOLDEN NEGATIVE / kill identity: either property absent / off /
 *      blank / junk / faulted -> the REAL refresh (B2, every sheet write,
 *      _Run_Log, _Selection_Log, Logger lines, POST bodies, the properties
 *      after the run) deep-equals the base; no console line; one extra
 *      property read per knob
 *   R2 armed ('on', ' ON '): the same board, writes, logs and properties as
 *      off, plus 35 streamed phase lines in the designed order, one Logger
 *      phase line and one _Run_Log TIMING row (marks = the 35 phases, total
 *      = their sum); the P-195 marker gains ph / at at most every 5 s
 *   R3 a stopped armed run: the next run's ABORTED_PREV row ends '; last
 *      phase reached: <ph> at t+<at>s' and its details carry trail; a
 *      v1.27.0 marker (no ph) gives the base row byte for byte
 *   R4 pure: dt10PhaseLine_, dt10PhaseTrailNote_, dt10RenderTimingOn_
 *   R5 embedded dt10SelfTest: two lines added (off / on / faulted, both
 *      knobs), core ok, every base line kept; the base lacks them; zero FAIL
 *   R6 lint: ES5 / ASCII of the diff, LF only, WHY block newest-first
 *   R7 mutants: eight one-line mutations are each caught
 *   R8 CLEAR-LATE: the refresh attaches payload._dt10_clear_late only when
 *      armed (spellings) and paints the same board with every sheet write in
 *      the same order; a render stopped in the pre-paint steps keeps the
 *      previous board (cells and merges; off = the base = an empty zone);
 *      a finished armed render still wipes stale rows and merges and equals
 *      the off board; with the timer armed, one extra mark r_clear_late
 * The whole battery runs three times and the three digests must match; the
 * last line is 'PASS k/k' (exit 1 on any failure).
 * Usage: node tests/test_dt10_v1280_render_timing.js [new.gs] [base.gs]
 */
'use strict';
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const vm = require('vm');
const ROOT = path.join(__dirname, '..');
const history = require(path.join(ROOT, 'tests', 'js', 'dt10_cash_utc_history.js'));
const EDITS = require(path.join(ROOT, 'tests', 'js', 'dt10_edits_v1280.json'));
const NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '16_Decision_Top10.gs');
const BASE_PATH = process.argv[3] || '';
const newSource = fs.readFileSync(NEW_PATH, 'utf8');
function sha256(s) { return crypto.createHash('sha256').update(s).digest('hex'); }
let baseSource;
try {
  baseSource = BASE_PATH ? fs.readFileSync(BASE_PATH, 'utf8') : history.v1270Source(newSource);
} catch (eBase) {
  console.log('FAIL R0 base oracle: ' + String(eBase && eBase.message || eBase).split('\n')[0]);
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


/* ============================================================ v1.28.0 battery */
const TIMING = 'DT10_RENDER_TIMING', MARKER = 'DT10_RUN_MARKER', CLEAR = 'DT10_CLEAR_LATE';
const MARKS = ['setup', 'gates', 'inputs', 'funding', 'src', 'pool', 'ready', 'post', 'attach', 'verdict1', 'stab',
  'verdict2', 'alloc', 'final', 'r_clear', 'r_verdict', 'r_final', 'r_live', 'r_earn', 'r_postearn', 'r_guard', 'r_kpi',
  'r_selected', 'r_qualified', 'r_nearmiss', 'r_alerts', 'r_gaps', 'r_cands', 'r_end', 'render', 'notes', 'sellog',
  'exitlog', 'shadow', 'status'];
const MARKS_CL = MARKS.slice(0, MARKS.indexOf('r_guard') + 1).concat(['r_clear_late'], MARKS.slice(MARKS.indexOf('r_guard') + 1));
const NEW_F = ['dt10ClearLateOn_', 'dt10ClearZones_', 'dt10PhaseLine_', 'dt10PhaseMark_', 'dt10PhaseStart_', 'dt10PhaseTrailNote_', 'dt10RenderTimingOn_'];
const NEW_V = ['DT10_CLEAR_LATE_PROP', 'DT10_PH_', 'DT10_RENDER_TIMING_MARKER_MS', 'DT10_RENDER_TIMING_PROP'];
/* comparisons against the base read the delivered version as the base's */
const n28 = x => JSON.parse(JSON.stringify(x).replace(/1\.28\.0/g, '1.27.0'));
const isTimingCall = x => x[0] === 'appendRow' && x[1] === '_Run_Log' && JSON.parse(x[6])[4] === 'TIMING';
function tickingDate(stepMs, startMs) {
  let t = startMs;
  return class TickDate extends Date {
    constructor(...a) { if (a.length) super(...a); else { t += stepMs; super(t); } }
    static now() { t += stepMs; return t; }
  };
}
function laterDate(ms) {
  return class LaterDate extends Date {
    constructor(...a) { super(...(a.length ? a : [ms])); }
    static now() { return ms; }
  };
}
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
/* ---- reusable checks (also the mutant oracles) ---- */
function checkGolden(src, out) {
  const PROPSETS = [{}, { [TIMING]: 'off' }, { [TIMING]: '' }, { [TIMING]: 'yes' }, { [TIMING]: 'onn' }, { [TIMING]: '1' },
                    { [CLEAR]: 'off' }, { [CLEAR]: 'yes' }, { [CLEAR]: 'true' }, { [TIMING]: 'no', [CLEAR]: '1' }];
  for (const props of PROPSETS) {
    const n = refresh(src, props), b = refresh(baseSource, props), tag = JSON.stringify(props);
    eq(n28(n.status), b.status, 'B2 ' + tag);
    deq(n28(n.calls), b.calls, 'sheet writes ' + tag);
    deq(n28(n.runLog), b.runLog, '_Run_Log ' + tag);
    deq(n28(n.selLog), b.selLog, '_Selection_Log ' + tag);
    deq(n28(n.logs), b.logs, 'Logger lines ' + tag);
    deq(n.posted, b.posted, 'POST bodies ' + tag);
    deq(n28(n.props), b.props, 'properties after the run ' + tag);
    deq(n.cons, [], 'no console line ' + tag);
    deq(n28(n.sets), b.sets, 'property writes ' + tag);
    eq(n.propReads.byKey[TIMING], 1, 'the timing property is read once ' + tag);
    eq(n.propReads.byKey[CLEAR], 1, 'the clear-late property is read once ' + tag);
    const nr = plain(n.propReads); delete nr.byKey[TIMING]; delete nr.byKey[CLEAR]; nr.total -= 2;
    deq(nr, b.propReads, 'every other property read identical ' + tag);
    if (out) out.push([tag, n.status.length, n.calls.length]);
  }
  const nf = refresh(src, {}, { propsThrow: true }), bf = refresh(baseSource, {}, { propsThrow: true });
  eq(n28(nf.status), bf.status, 'B2 faulted'); deq(n28(nf.calls), bf.calls, 'writes faulted'); deq(n28(nf.runLog), bf.runLog, 'run log faulted');
  deq(n28(nf.logs), bf.logs, 'Logger faulted'); deq(nf.cons, [], 'no console line faulted');
}
function checkArmed(src, out) {
  const off = refresh(src, {});
  for (const val of ['on', ' ON ']) {
    const a = refresh(src, { [TIMING]: val }), tag = JSON.stringify(val);
    eq(a.status, off.status, 'B2 unchanged ' + tag);
    deq(a.calls.filter(x => !isTimingCall(x)), off.calls, 'every sheet write unchanged ' + tag);
    deq(a.selLog, off.selLog, '_Selection_Log unchanged ' + tag);
    deq(a.posted, off.posted, 'POST bodies unchanged ' + tag);
    const ap = plain(a.props); delete ap[TIMING];
    deq(ap, off.props, 'properties after the run unchanged (marker cleared) ' + tag);
    eq(a.runLog.length, off.runLog.length + 1, 'one _Run_Log row added ' + tag);
    const row = a.runLog[a.runLog.length - 1];
    deq(row.slice(1, 5), ['INFO', 'refreshDecisionTop10', 'Top_10_Investments', 'TIMING'], 'TIMING row ' + tag);
    const det = JSON.parse(row[9]);
    eq(det.version, '1.28.0', 'details version'); deq(det.marks.map(m => m[0]), MARKS, 'marks in the designed order ' + tag);
    eq(det.total_ms, det.marks.reduce((s, m) => s + m[1], 0), 'total = the sum of the phases');
    eq(row[5], 'phases 0s: ' + MARKS.map(m => m + ' 0').join(' | ').slice(0, 400 - 'phases 0s: '.length), 'the phase line (frozen clock) ' + tag);
    const extra = a.logs.filter(l => off.logs.indexOf(l) < 0);
    deq(extra, ['[DT10 v1.28.0] ' + row[5]], 'one Logger line added ' + tag);
    eq(a.logs.indexOf(extra[0]) + 1, a.logs.findIndex(l => /\] done status=/.test(l)), 'the phase line precedes done');
    deq(a.cons, MARKS.map(m => '[DT10 v1.28.0] t+0s ' + m + ' +0s'), 'streamed console lines ' + tag);
    const mw = a.sets.filter(s => s[0] === MARKER).map(s => JSON.parse(s[1]));
    eq(mw.length, 2, 'frozen clock: the start marker and one phase write ' + tag);
    eq(mw[0].ph, undefined, 'the start marker is the v1.27.0 marker');
    deq([mw[1].ph, mw[1].at, mw[1].started, mw[1].v, mw[1].entry], ['gates', 0, mw[0].started, '1.28.0', 'refreshDecisionTop10'], 'first phase write');
    if (out) out.push([tag, row[5]]);
  }
  // ticking clock (1 s per clock read): the marker is rewritten at most every 5 s; phases sum to the total
  const t = refresh(src, { [TIMING]: 'on' }, { dateClass: tickingDate(1000, NOW_F) });
  const det = JSON.parse(t.runLog[t.runLog.length - 1][9]);
  deq(det.marks.map(m => m[0]), MARKS, 'ticking: marks'); eq(det.total_ms, det.marks.reduce((s, m) => s + m[1], 0), 'ticking: total');
  ok(det.total_ms > 0, 'ticking: time passes');
  const mw = t.sets.filter(s => s[0] === MARKER).map(s => JSON.parse(s[1])).filter(m => m.ph);
  ok(mw.length >= 3, 'ticking: several phase writes (' + mw.length + ')');
  for (let i = 1; i < mw.length; i++) ok(Math.round((mw[i].at - mw[i - 1].at) * 10) >= 50, 'ticking: >= 5 s between marker writes ' + JSON.stringify([mw[i - 1], mw[i]]));
  ok(MARKS.indexOf(mw[mw.length - 1].ph) > 0, 'ticking: the last write names a phase');
  if (out) out.push(['ticking', det.total_ms, mw.length, mw[mw.length - 1].ph]);
  return mw[mw.length - 1];
}
function checkTrail(src, lastMarker, out) {
  const later = laterDate(Date.parse(lastMarker.started) + 30 * 60000);
  const prevArmed = JSON.stringify(lastMarker);
  const n = refresh(src, { [MARKER]: prevArmed }, { dateClass: later }), b = refresh(baseSource, { [MARKER]: prevArmed }, { dateClass: later });
  const nr = n.runLog.filter(r => r[4] === 'ABORTED_PREV'), br = b.runLog.filter(r => r[4] === 'ABORTED_PREV');
  eq(nr.length, 1, 'one ABORTED_PREV row'); eq(br.length, 1, 'the base writes one too');
  const trail = '; last phase reached: ' + lastMarker.ph + ' at t+' + lastMarker.at + 's';
  eq(nr[0][5], (br[0][5] + trail).slice(0, 400), 'the message ends with the trail');
  const nd = JSON.parse(nr[0][9]), bd = JSON.parse(br[0][9]);
  deq(Object.keys(nd), ['version', 'started', 'age_min', 'trail'], 'details keys');
  eq(nd.trail, trail.slice(2), 'details.trail'); delete nd.trail;
  deq(n28(nd), bd, 'every other detail identical');
  // a v1.27.0 marker (no phase) gives the base row byte for byte
  const prevOld = JSON.stringify({ started: lastMarker.started, v: '1.27.0', entry: 'refreshDecisionTop10' });
  const n2 = refresh(src, { [MARKER]: prevOld }, { dateClass: later }), b2 = refresh(baseSource, { [MARKER]: prevOld }, { dateClass: later });
  deq(n28(n2.runLog), b2.runLog, 'GOLDEN NEGATIVE: an unphased marker -> the base _Run_Log');
  if (out) out.push([nr[0][5]]);
}
function checkClearLate(src, out) {
  // (a) the REAL refresh: the key rides only when armed; the same board and every sheet write in the same order
  const seen = [];
  const spy = ctx => { const f = ctx.dt10RenderPayload_; ctx.dt10RenderPayload_ = (s, p, t) => {
    seen.push(Object.prototype.hasOwnProperty.call(p, '_dt10_clear_late') ? p._dt10_clear_late : 'absent'); return f(s, p, t); }; };
  const off = refresh(src, {}, { inject: spy });
  for (const val of ['on', ' ON ', 'On']) {
    const a = refresh(src, { [CLEAR]: val }, { inject: spy }), tag = JSON.stringify(val);
    eq(a.status, off.status, 'B2 unchanged ' + tag);
    deq(a.calls, off.calls, 'every sheet write unchanged, in the same order ' + tag);
    deq(a.runLog, off.runLog, '_Run_Log unchanged ' + tag); deq(a.selLog, off.selLog, '_Selection_Log unchanged ' + tag);
    deq(a.posted, off.posted, 'POST bodies unchanged ' + tag); deq(a.logs, off.logs, 'Logger unchanged ' + tag);
    const ap = plain(a.props); delete ap[CLEAR]; deq(ap, off.props, 'properties unchanged ' + tag);
  }
  for (const val of ['off', 'yes', '1', '']) refresh(src, { [CLEAR]: val }, { inject: spy });
  deq(seen, ['absent', 1, 1, 1, 'absent', 'absent', 'absent', 'absent'], 'payload._dt10_clear_late only for on / " ON " / On');
  // (b) a render stopped in the pre-paint steps (the ticket guard step throws, as a 6-minute stop would end it there)
  const STALE = [[40, 1, 'PREVIOUS BOARD ROW'], [390, 3, 'PREVIOUS FAR ROW']], MERGE = [395, 1, 1, 4];
  const kill = ctx => { ctx.dt10TicketGuard_ = () => { throw new Error('STOPPED in r_guard'); }; };
  const kOn = render(src, { clearLate: true, seedZone: STALE, seedMerge: MERGE, inject: kill, allowThrow: true });
  const kOff = render(src, { seedZone: STALE, seedMerge: MERGE, inject: kill, allowThrow: true });
  const kBase = render(baseSource, { seedZone: STALE, seedMerge: MERGE, inject: kill, allowThrow: true });
  for (const k of [kOn, kOff, kBase]) eq(k.threw, 'STOPPED in r_guard', 'the injected stop');
  eq(cellV(kOn.snap, 40, 1), 'PREVIOUS BOARD ROW', 'armed: a stopped render keeps the previous board');
  eq(cellV(kOn.snap, 390, 3), 'PREVIOUS FAR ROW', 'armed: every previous row');
  ok(kOn.snap.merges.indexOf(MERGE.join('|')) >= 0, 'armed: and its merges');
  eq(kOn.book.counts.clear || 0, 0, 'armed: nothing cleared before the stop');
  eq(cellV(kOff.snap, 40, 1), undefined, 'off: the zone is already empty (the 9 Nov 10:14 picture)');
  deq(kOff.snap, kBase.snap, 'off: stopped there = the base stopped there');
  // (c) a finished armed render: the late clear still wipes stale rows and merges; the same board as off
  const fOn = render(src, { clearLate: true, seedZone: STALE, seedMerge: MERGE });
  const fOff = render(src, { seedZone: STALE, seedMerge: MERGE });
  eq(cellV(fOn.snap, 390, 3), undefined, 'armed: the stale far row is wiped');
  ok(fOn.snap.merges.indexOf(MERGE.join('|')) < 0, 'armed: the stale merge is broken apart');
  deq(fOn.snap, fOff.snap, 'armed: the finished board is the off board');
  eq(fOn.book.counts.clear, fOff.book.counts.clear, 'one zone clear either way');
  deq(fOn.calReads, fOff.calReads, 'the same Calendar_Events reads');
  // (d) both knobs armed: one extra mark right after r_guard
  const t = refresh(src, { [TIMING]: 'on', [CLEAR]: 'on' });
  const det = JSON.parse(t.runLog[t.runLog.length - 1][9]);
  deq(det.marks.map(m => m[0]), MARKS_CL, 'marks with r_clear_late after r_guard');
  if (out) out.push([kOn.snap.merges.length, Object.keys(fOn.snap.cells).length, det.marks.length]);
}
function battery() {
  const digest = [];
  test('R0 base oracle is the reviewed v1.27.0 file (SHA-256 pinned); versions in lockstep; both compile; census +7 functions +4 vars, 0 removed; 38 hunks reverse to the base', () => {
    eq(sha256(baseSource), history.V1270_SHA256, 'base SHA-256');
    eq(sha256(history.v1270Source(newSource)), history.V1270_SHA256, 'reversal of the delivered file');
    eq(EDITS.length, 38, 'hunk count');
    const N = ctxOf(newSource, {}), B = ctxOf(baseSource, {});
    eq(N.DT10_VERSION, '1.28.0', 'delivered version'); eq(B.DT10_VERSION, '1.27.0', 'base version');
    ok(/^ \* Version: 1\.28\.0 \(see DT10_VERSION;/.test(newSource.split('\n')[3]), 'header line 4 lockstep');
    const fn = census(newSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), fb = census(baseSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    const vn = census(newSource, /^var\s+([A-Za-z_$][\w$]*)/mg), vb = census(baseSource, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq([...fb].filter(f => !fn.has(f)), [], 'functions removed'); deq([...vb].filter(v => !vn.has(v)), [], 'vars removed');
    deq([...fn].filter(f => !fb.has(f)).sort(), NEW_F, 'functions added'); deq([...vn].filter(v => !vb.has(v)).sort(), NEW_V, 'vars added');
    for (const f of NEW_F) eq(typeof N[f], 'function', f);
    digest.push([fn.size, vn.size]);
  });
  test('R1 GOLDEN NEGATIVE / kill identity: the property absent / off / blank / junk / faulted (either knob) -> the REAL refresh deep-equals the v1.27.0 base; no console line; one extra read per knob', () => {
    checkGolden(newSource, digest);
  });
  let lastMarker = null;
  test('R2 armed (on, " ON "): same board, writes, logs and properties as off; 35 streamed phase lines in order; one Logger phase line; one _Run_Log TIMING row; marker phase writes at most every 5 s', () => {
    lastMarker = checkArmed(newSource, digest);
  });
  test('R3 a stopped armed run: the next ABORTED_PREV row ends with the last phase reached and carries details.trail; a v1.27.0 marker gives the base row', () => {
    ok(lastMarker && lastMarker.ph, 'R2 left a phased marker');
    checkTrail(newSource, lastMarker, digest);
  });
  test('R4 pure: phase line (rounding, 400-char cap, null), trail note (valid / no phase / bad at / junk / long name), the property spellings and a fault', () => {
    const N = ctxOf(newSource, {});
    eq(N.dt10PhaseLine_({ t0: 1000, last: 4300, marks: [['setup', 1200], ['pool', 2100]] }), 'phases 3.3s: setup 1.2 | pool 2.1');
    eq(N.dt10PhaseLine_({ t0: 0, last: 349267, marks: [['alloc', 53049], ['r_earn', 79251]] }), 'phases 349.3s: alloc 53 | r_earn 79.3');
    eq(N.dt10PhaseLine_(null), ''); eq(N.dt10PhaseLine_({}), '');
    const many = { t0: 0, last: 0, marks: [] }; for (let i = 0; i < 80; i++) many.marks.push(['phase_' + i, 0]);
    eq(N.dt10PhaseLine_(many).length, 400, 'capped at 400');
    eq(N.dt10PhaseTrailNote_('{"started":"x","ph":"alloc","at":251.4}'), '; last phase reached: alloc at t+251.4s');
    eq(N.dt10PhaseTrailNote_('{"started":"x","ph":"setup","at":0}'), '; last phase reached: setup at t+0s');
    for (const raw of ['{"started":"x"}', '{"ph":"alloc"}', '{"ph":"alloc","at":null}', '{"ph":"alloc","at":""}', '{"ph":"alloc","at":"abc"}',
                       '{"ph":"","at":3}', '{"ph":5,"at":3}', 'junk', '', null, undefined, '2026-11-09T05:44:28.522Z']) {
      eq(N.dt10PhaseTrailNote_(raw), '', 'no trail for ' + JSON.stringify(raw));
    }
    eq(N.dt10PhaseTrailNote_('{"ph":"' + 'x'.repeat(60) + '","at":1}'), '; last phase reached: ' + 'x'.repeat(40) + ' at t+1s', 'phase name capped at 40');
    for (const [val, want] of [['on', true], [' ON ', true], ['On', true], ['off', false], ['', false], ['yes', false], ['1', false], ['true', false], [null, false]]) {
      eq(ctxOf(newSource, val === null ? {} : { [TIMING]: val }).dt10RenderTimingOn_(), want, 'spelling ' + JSON.stringify(val));
    }
    eq(ctxOf(newSource, { [TIMING]: 'on' }, true).dt10RenderTimingOn_(), false, 'a property fault reads off');
    const C = ctxOf(newSource, { [TIMING]: 'on' });
    eq(C.dt10PhaseStart_(5), C.DT10_PH_, 'armed start returns the clock'); eq(C.DT10_PH_.t0, 5);
    const D = ctxOf(newSource, {}); D.DT10_PH_ = { stale: true }; eq(D.dt10PhaseStart_(5), null, 'off start returns null'); eq(D.DT10_PH_, null, 'and clears a stale clock');
    D.dt10PhaseMark_('x'); eq(D.DT10_PH_, null, 'an unarmed mark is a no-op');
  });
  test('R5 embedded dt10SelfTest: two lines added (off / on / faulted), core ok, every base line kept; the base lacks them; zero FAIL', () => {
    const rep = String(ctxOf(newSource, {}).dt10SelfTest()), repB = String(ctxOf(baseSource, {}).dt10SelfTest());
    ok(rep.indexOf('[DT10 v1.28.0] self-test') === 0, 'version line');
    const nl = rep.split('\n'), bl = repB.split('\n');
    eq(nl.length, bl.length + 2, 'exactly two lines added');
    const want = ['render timing (DT10_RENDER_TIMING): off (phase lines stream to the execution log; a finished render writes one TIMING row to _Run_Log; the P-195 marker keeps the last phase reached); clear late (DT10_CLEAR_LATE): off (the board zones clear after the pre-paint steps, so a stopped render keeps the previous board)',
                  'render timing core: ok (phase line, last-phase trail; a marker without a phase adds nothing)'];
    for (const l of want) { ok(nl.indexOf(l) > 0, 'line: ' + l); ok(repB.indexOf(l.split(' (')[0]) < 0, 'GOLDEN NEGATIVE: the base lacks ' + l.split(' (')[0]); }
    deq(nl.filter(l => want.indexOf(l) < 0).map(l => l.replace('[DT10 v1.28.0]', '[DT10 v1.27.0]')), bl, 'every other line identical');
    ok(rep.indexOf('FAIL') < 0, 'no FAIL');
    ok(String(ctxOf(newSource, { [TIMING]: 'on' }).dt10SelfTest()).indexOf('render timing (DT10_RENDER_TIMING): on (') > 0, 'armed line');
    ok(String(ctxOf(newSource, { [TIMING]: 'on' }, true).dt10SelfTest()).indexOf('render timing (DT10_RENDER_TIMING): off (') > 0, 'a fault reads off');
    ok(String(ctxOf(newSource, { [CLEAR]: ' On ' }).dt10SelfTest()).indexOf('clear late (DT10_CLEAR_LATE): on (') > 0, 'clear late armed line');
    ok(String(ctxOf(newSource, { [CLEAR]: 'on' }, true).dt10SelfTest()).indexOf('clear late (DT10_CLEAR_LATE): off (') > 0, 'clear late: a fault reads off');
    digest.push(nl.length);
  });
  test('R6 lint: ES5 / ASCII of the diff, LF only, WHY block newest-first', () => {
    const bl = new Set(baseSource.split('\n'));
    const added = newSource.split('\n').filter(l => !bl.has(l));
    ok(added.length >= 150 && added.length <= 260, 'added lines ' + added.length);
    for (const l of added) {
      if (!/^[\x09\x20-\x7e]*$/.test(l)) ok(bl.has(l.replace('1.28.0', '1.27.0')), 'non-ASCII added line: ' + l);
      if (!/^\s*(\*|\/\*|\/\/)/.test(l)) ok(!/\b(let|const|class)\s|=>|`|\.\.\.|\.includes\(/.test(l.replace(/\/\/.*$/, '')), 'ES6 token: ' + l);
    }
    ok(newSource.indexOf('\r') < 0, 'LF only');
    const why = newSource.indexOf(' * v1.28.0 (2026-11-09) [RENDER-TIMING]'), prev = newSource.indexOf(' * v1.27.0 (2026-11-08) [CORP-EVENT-GUARD]');
    ok(why > 0 && prev > why, 'WHY block newest-first');
    digest.push(added.length);
  });
  test('R7 mutants: eight one-line mutations are each caught', () => {
    const MUT = [
      ['always armed', ".getProperty(DT10_RENDER_TIMING_PROP) || '')\n        .replace(/^\\s+|\\s+$/g, '').toLowerCase() === 'on';",
       ".getProperty(DT10_RENDER_TIMING_PROP) || '')\n        .replace(/^\\s+|\\s+$/g, '').toLowerCase() !== 'zz';", src => checkGolden(src)],
      ['the alloc mark dropped', "  dt10PhaseMark_('alloc');   // v1.28.0\n", '', src => checkArmed(src)],
      ['no throttle', 'now - ph.wrote >= DT10_RENDER_TIMING_MARKER_MS', 'now - ph.wrote >= 0', src => checkArmed(src)],
      ['no TIMING row', "dt10RunLogRow_('INFO', 'TIMING', phLine,", "dt10RunLogRow_('INFO', 'TIMNG', phLine,", src => checkArmed(src)],
      ['no trail in the row', "'out before clearing its marker; this run proceeds' + trail,", "'out before clearing its marker; this run proceeds',", src => checkTrail(src, lastMarker)],
      ['the render ignores the key', 'var clearLate = !!(payload && payload._dt10_clear_late === 1);', 'var clearLate = false;', src => checkClearLate(src)],
      ['the late clear dropped', "    dt10ClearZones_(sheet);\n    dt10PhaseMark_('r_clear_late');", "    dt10PhaseMark_('r_clear_late');", src => checkClearLate(src)],
      ['the key never attached', '  if (clearLate) payload._dt10_clear_late = 1;', '  if (false) payload._dt10_clear_late = 1;', src => checkClearLate(src)]
    ];
    for (const [name, from, to, run] of MUT) {
      eq(newSource.split(from).length - 1, 1, 'mutant anchor ' + name);
      let caught = false;
      try { run(newSource.replace(from, () => to)); } catch (e) { caught = true; }
      ok(caught, 'mutant survived: ' + name);
    }
    digest.push(MUT.length);
  });
  test('R8 CLEAR-LATE: the key rides only when armed; the same board and write order; a stopped render keeps the previous board; a finished one still wipes stale zones; one extra mark with the timer', () => {
    checkClearLate(newSource, digest);
  });
  return sha256(JSON.stringify(digest)).slice(0, 16);
}

const digests = [];
for (let i = 0; i < 3; i++) digests.push(battery());
const total = passed + failed;
if (digests[0] === digests[1] && digests[1] === digests[2]) console.log('digest ' + digests[0] + ' x3 identical');
else { console.log('FAIL digests differ: ' + digests.join(' ')); failed++; }
console.log((failed ? 'FAIL ' : 'PASS ') + (passed / 3) + '/' + (total / 3));
process.exit(failed ? 1 : 0);
