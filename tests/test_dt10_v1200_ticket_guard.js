#!/usr/bin/env node
/* tests/test_dt10_v1200_ticket_guard.js
 * Harness for apps_script/16_Decision_Top10.gs v1.20.0 [TICKET-GUARD /
 * KPI-PLAN-GAIN / VENUE-LINE] (live review, Mon 2 Nov 2026 09:50 Riyadh; the
 * Top_10_Investments render of 08:49:43): three Script Properties, each
 * default OFF.
 *
 * Dual-tree node harness over the REAL file: loads the delivered .gs source
 * and the v1.19.0 BASE -- a file given as the second argument (it must carry
 * the pinned SHA-256 of origin/main 59c4c56c / a90c4fb9) or, by default, the
 * base rebuilt from the delivered file by reversing exactly the enumerated
 * v1.20.0 edits (tests/js/dt10_edits_v1200.json, SHA-256 pinned) -- into
 * separate vm contexts with the two GAS shims of
 * tests/test_dt10_v1190_hold_live_rule.js: the cell-model spreadsheet (the
 * REAL dt10RenderPayload_, here with a Calendar_Events sheet and the clock
 * of the 08:49:43 render) and the full workbook (the REAL
 * refreshDecisionTop10 with a POST stub that answers the research call and
 * the allocate replay).
 * Fixture = the 2 Nov board: ITRN.US 12,199 / DDI.US 12,334 / CRC.US 8,527 /
 * GLPI.US 12,133 / EFC.US 11,467 / INDV.US 12,335 SAR executable (68,997 SAR
 * deployed; KPI 'Exp. Gain 12M (SAR)' 56,203), OLP.US research (the replay
 * did not fund it), ZTO.US / BBOX.L / BHF.US grace; Calendar_Events days to
 * earnings CRC 0, EFC 2, OLP 3, DDI 9, ITRN 22, GLPI none, INDV none (BHF 6
 * -> 'earn 5/10'); last reports INDV +4.1 % and ZTO -2.0 % ('post-earn
 * 2/10'); plan payoffs 2,110 / 2,023 / 1,424 / 1,771 / 1,628 / 2,122 =
 * 11,078 SAR; the feed stamp 02:42:14+03:00 (verdict age 367m).
 * Sections:
 *   V0 base oracle (SHA-256 pinned), version lockstep, both files compile
 *   V1 GOLDEN NEGATIVE / kill identity: the properties absent / off / blank /
 *      junk / faulted -> the REAL render (every cell, merge and border, the
 *      tickets, the status token, the property reads, the Logger lines) and
 *      the REAL refresh (B2, every sheet write, _Run_Log, _Selection_Log,
 *      Logger lines, POST bodies, the state blob) deep-equal the base
 *   V2 observe: every cell identical to off; token 'tguard: would hold
 *      CRC.US(earn 0d) EFC.US(earn 2d)'; one Logger line; REAL refresh B2 =
 *      the off line + the token
 *   V3 enforce (earnings 3): 4 tickets remain; CRC / EFC are EXACTLY the rows
 *      the existing research path writes for the same tickets left unfunded
 *      (a control render), but for Funds From 'Held -- earnings in Nd' and the
 *      '[held: ...] ' prefix; KPIs = the control's; Unallocated +19,994;
 *      banner '4 EXECUTABLE TICKETS ... + 2 HELD BY GUARD'; surviving
 *      tickets byte-identical; research-context tags and the live-fill
 *      suffix carried; REAL refresh (B2, seats, _Selection_Log, footer)
 *   V4 enforce + owner rule: only GLPI.US remains (High, 29.2 %, no earnings)
 *   V5 cap 2 without the owner rule: ITRN.US and DDI.US remain
 *   V6 never increases: a board with 0 tickets stays 0, a withheld feed
 *      holds nothing, every knob combination (8 x 2 x 6) only removes
 *   V7 KPI 'Exp. Gain TP1 (SAR)': 11,078 (+-3) for the six, recomputed after
 *      the guard; a withheld feed shows the em dash; restore after a
 *      rollback; REAL refresh label, value and marker
 *   V8 venue line: ' -- US opens 17:30 Riyadh (feed valid until 10:42)'; off
 *      -> absent; the REAL refresh of a Saturday board names Monday
 *   V9 the embedded dt10SelfTest: the three gate lines, core ok, armed and
 *      faulted lines, every base line kept; the base lacks them
 *   V10 ES5 / ASCII lint of the diff; inventory (17 functions, 14 vars added,
 *      0 removed); 15 hunks whose reversal is the reviewed v1.19.0 file
 *   V11 service reads: spellings -> modes; knobs; faults read off
 * The whole battery runs three times and the three digests must match; the
 * last line is 'PASS k/k' (exit 1 on any failure).
 * Usage: node tests/test_dt10_v1200_ticket_guard.js [new.gs] [base.gs]
 */
'use strict';
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const vm = require('vm');
const ROOT = path.join(__dirname, '..');
const history = require(path.join(ROOT, 'tests', 'js', 'dt10_cash_utc_history.js'));
const NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '16_Decision_Top10.gs');
const BASE_PATH = process.argv[3] || '';
const newSource = fs.readFileSync(NEW_PATH, 'utf8');
/* merge note 2026-11-02 (#778): held seats are v1.20.1 over #895's v1.20.0. The structural checks
 * (V0 pins, V10 diff inventory and reversal) read the layer: the delivered file with the bundle's
 * designed regions stripped (tests/js/dt10_audit_keep_regions.js), which is main's v1.20.0 byte for
 * byte; the behavioural checks run the delivered file.
 * merge note 2026-11-03 (#778): held seats are v1.21.1 over #906's v1.21.0; the layer is now main's
 * v1.21.0 byte for byte, and s1200 (the v1.20.0 rebuild V0 / V10 read) starts from the layer.
 * merge note 2026-11-05 (#778): held seats are v1.21.2 over #921's v1.21.1; the layer is now main's
 * v1.21.1 byte for byte; s1200 and S1210 (the v1.21.0 rebuild B2 reads) start from the layer. */
const keepRegions = require(path.join(ROOT, 'tests', 'js', 'dt10_audit_keep_regions.js'));
const layerSource = newSource.indexOf(keepRegions.CALL_REGION) >= 0 ? keepRegions.stripDesigned(newSource) : newSource;
function sha256(s) { return crypto.createHash('sha256').update(s).digest('hex'); }
let baseSource;
try {
  baseSource = BASE_PATH ? fs.readFileSync(BASE_PATH, 'utf8') : history.v1190Source(layerSource);
} catch (eBase) {
  console.log('FAIL V0 base oracle: ' + String(eBase && eBase.message || eBase).split('\n')[0]);
  console.log('FAIL 0/1');
  process.exit(1);
}
// 2026-11-03: v1.21.0 (ex-dividend guard) stacked on this change; pins moved. Behaviour runs on
// the delivered file; the v1.20.0 lint / inventory / hunks (V10) compare the v1.20.0 file rebuilt
// from it (v1.21.0 edits reversed, SHA-256 pinned) with the base.
const s1200 = /^var DT10_VERSION = '1\.20\.0';$/m.test(layerSource) ? layerSource : history.v1200Source(layerSource);   // merge note 2026-11-03 (#778): from the layer
// 2026-11-05: v1.21.1 (approved Mutual_Funds scope 2422, policy data only) stacked on top. Its fund
// count reaches the readiness reason in the REAL refresh's Logger lines, so the refresh-level golden
// negative reads the v1.21.0 file rebuilt from the delivered one (v1.21.1 edits reversed, SHA-256
// pinned); tests/test_dt10_v1211_mf_scope.js proves v1.21.1 = v1.21.0 apart from that data.
// merge note 2026-11-05 (#778): held seats are v1.21.2 over #921's v1.21.1; the layer is now
// main's v1.21.1 byte for byte, v1200Source reverses the v1.21.1 edits first, and the v1.21.0
// rebuild (S1210) starts from the layer, never from the delivered v1.21.2 file.
const S1210 = /^var DT10_VERSION = '1\.21\.1';$/m.test(layerSource) ? history.v1210Source(layerSource) : layerSource;
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
const nv = x => JSON.parse(JSON.stringify(x).replace(/v1\.2[01]\.\d+/g, 'v1.19.0'));   // 2026-11-05: v1.21.1 too; merge note 2026-11-05 (#778): held seats are v1.21.2 over #921's v1.21.1: v1.20.0, v1.21.0, v1.21.1 or the bundle's v1.21.2

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
  const earn = ctx.dt10RenderPayload_(sheet, p, ctx.DT10_FALLBACK_TOKENS);
  return { ctx, book, sheet, snap: snapshot(sheet), note: earn && earn.note, payload: p, propReads: ctx.__propReads.slice(),
           logs: ctx.__logs.slice(), token: p._dt10_tguard_note, uv, calReads };
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
  const env = { calls: [], reads: [], appended: {}, propReads: { total: 0, byKey: {} }, props, sheets: {}, options: opts, logs: [], posted: [] };
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
    setProperty(k, v) { props[k] = String(v); return propsService; },
    deleteProperty(k) { delete props[k]; return propsService; },
    getProperties() { return props; }
  };
  env.ctx = {
    console, Logger: { log(m) { env.logs.push(String(m)); } }, Date: FixedDate,
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
  const env = makeEnv({ props: Object.assign({ DT10_STAB_STATE_V1: JSON.stringify(seedR()) }, props || {}), propsThrow: opts.propsThrow,
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
  env.ctx.refreshDecisionTop10();
  const seatRows = env.calls.filter(x => x[0] === 'setValues' && x[1] === 'Top_10_Investments' && x[2] === 20).map(x => JSON.parse(x[6]));
  return { env, top10, status: String(top10._val(2, 2)), calls: env.calls.slice(), runLog: plain(env.appended['_Run_Log'] || []),
           selLog: env.calls.filter(x => x[1] === '_Selection_Log' && x[0] === 'setValues').map(x => JSON.parse(x[6])),
           logs: env.logs.slice(), posted: env.posted.slice(), propReads: plain(env.propReads), props: plain(env.props),
           seats: seatRows.length ? seatRows[0] : [], banner: String(top10._val(18, 1)),
           kpis: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11].map(c => top10._val(16, c)), label2: top10._val(15, 2),
           saved: JSON.parse(env.props.DT10_STAB_STATE_V1) };
}
function seatRow(run, sym) { for (const r of run.seats) if (r[1] === sym) return r; return null; }
const stripTok = (s, tok) => String(s).replace(' | ' + tok, '');

/* ============================================================ battery */
function battery() {
  const N = loadRender(newSource, {});
  const B = loadRender(baseSource, {});
  const OFF_PROPS = [{}, { [GUARD]: 'off', [KPI]: 'off', [VENUE]: 'off' }, { [GUARD]: '', [KPI]: '', [VENUE]: '' },
                     { [GUARD]: 'yes', [KPI]: '1', [VENUE]: 'enforce' }, { [GUARD]: 'enforced', [KPI]: 'onn', [VENUE]: 'observe' },
                     { [GUARD]: 'on', [KPI]: 'TP1', [VENUE]: 'yes', [MARK]: 'TP1', DT10_GUARD_EARN_DAYS: '30', DT10_GUARD_MAX_TICKETS: '1' }];

  test('V0 base oracle is the reviewed v1.19.0 file (SHA-256 pinned); versions in lockstep; both files compile', () => {
    eq(sha256(baseSource), history.V1190_SHA256, 'base SHA-256');
    eq(B.DT10_VERSION, '1.19.0', 'base version');
    ok(/^1\.(21\.[1-9]\d*|(2[2-9]|[3-9]\d)\.\d+)$/.test(N.DT10_VERSION), 'delivered version floor 1.21.1: ' + N.DT10_VERSION);   // 2026-11-05: v1.21.1 pins moved; merge note 2026-11-05 (#778): held seats are v1.21.2 over #921's v1.21.1
    ok(newSource.split('\n')[3].indexOf(' * Version: ' + N.DT10_VERSION + ' ') === 0, 'header line 4 lockstep');
    ok(newSource.indexOf("var DT10_VERSION = '" + N.DT10_VERSION + "';") > 0, 'DT10_VERSION literal');
    ok(/^ \* Version: 1\.21\.1 /.test(layerSource.split('\n')[3]) && layerSource.indexOf("var DT10_VERSION = '1.21.1';") > 0, 'layer pins 1.21.1');
    eq(sha256(s1200), history.V1200_SHA256, 'rebuilt v1.20.0 SHA-256');
    ok(/^ \* Version: 1\.20\.0 /.test(s1200.split('\n')[3]) && s1200.indexOf("var DT10_VERSION = '1.20.0';") > 0, 'rebuilt v1.20.0 lockstep');
    eq(N.DT10_TICKET_GUARD_PROP, GUARD); eq(N.DT10_KPI_PLAN_GAIN_PROP, KPI); eq(N.DT10_VENUE_LINE_PROP, VENUE); eq(N.DT10_KPI_PLAN_LABEL_PROP, MARK);
    deq([N.DT10_GUARD_EARN_DAYS_PROP, N.DT10_GUARD_OWNER_RULE_PROP, N.DT10_GUARD_MAX_TICKETS_PROP], KNOBS);
    deq([N.DT10_GUARD_EARN_DAYS_DEF, N.DT10_GUARD_EARN_DAYS_MAX, N.DT10_GUARD_MAX_TICKETS_MAX], [3, 30, 50]);
    eq(N.DT10_KPI_PLAN_GAIN_LABEL, 'Exp. Gain TP1 (SAR)');
    new vm.Script(newSource, { filename: '16_Decision_Top10.gs' });
    new vm.Script(baseSource, { filename: 'base.gs' });
  });

  let offR = null, offRefresh = null, baseRefresh = null;
  const getOff = () => offR || (offR = render(newSource, {}));
  const getOffRefresh = () => offRefresh || (offRefresh = refresh(newSource, {}));
  const getBaseRefresh = () => baseRefresh || (baseRefresh = refresh(baseSource, {}));
  test('V1 GOLDEN NEGATIVE / kill identity: properties absent, off, blank, junk, faulted -> the REAL render and the REAL refresh deep-equal the v1.19.0 base', () => {
    // (a) the REAL render: no payload key -> identical, whatever the properties say; junk keys too
    for (const state of ['EXECUTABLE', 'NOT_ACTIONABLE']) {
      const b = render(baseSource, { state });
      for (const props of OFF_PROPS) {
        const n = render(newSource, { state, props });
        const label = state + ' ' + JSON.stringify(props);
        deq(n.snap, b.snap, 'identical sheet ' + label + ' (diff ' + diffCells(n.snap, b.snap).slice(0, 4).join(',') + ')');
        eq(n.note, b.note, 'identical status token ' + label);
        deq(n.payload.selected, b.payload.selected, 'tickets untouched ' + label);
        deq(n.payload.kpis, b.payload.kpis, 'kpis untouched ' + label);
        deq(n.propReads.slice().sort(), b.propReads.slice().sort(), 'the render reads no new property ' + label);
        deq(nv(n.logs), b.logs, 'Logger lines ' + label);
        ok(n.token === undefined, 'no token key stored ' + label);
      }
      for (const junk of [{ tguard: { mode: 'off' } }, { tguard: { mode: 'ENFORCE' } }, { tguard: 'enforce' }, { kpi: 'off' }, { kpi: 'yes' }, { venue: 'ON' }]) {
        const n = render(newSource, Object.assign({ state }, junk));
        deq(n.snap, b.snap, 'a junk payload key changes nothing ' + state + ' ' + JSON.stringify(junk));
        ok(n.token === undefined, 'no token for ' + JSON.stringify(junk));
      }
      const f = render(newSource, { state, propsThrow: true });
      deq(f.snap, render(baseSource, { state, propsThrow: true }).snap, 'render under a property fault ' + state);
      if (state === 'EXECUTABLE') {
        offR = render(newSource, {});
        ok(/^✅ FEED ACTIONABLE — EXECUTABLE \(verdict age 367m\) — SELECTED — 6 EXECUTABLE TICKETS \+ 1 RESEARCH \(NO ALLOCATION\) \+ 3 GRACE-HELD \(NO PLAN TODAY\)$/.test(banner(b.snap)),
           'fixture: the live banner of 2 Nov: ' + banner(b.snap));
        deq(kpiRow(b.snap).slice(0, 3), [72747, 56203, '6 exec + 1 research + 3 grace / 10'], 'fixture: Entered cash, Exp. Gain 12M 56,203, Selected');
        eq(kpiRow(b.snap)[7], 3750, 'fixture: Unallocated'); eq(kpiRow(b.snap)[8], 6, 'fixture: Fundable Now');
        eq(b.note, 'earn ⚠5/10 | post-earn ⚠2/10', 'fixture: the live earnings tokens');
        deq(SYMS.map(s => cellV(b.snap, rowOf(b.snap, s), 11)), LIVE.map(l => l.sar), 'fixture: the six tickets');
        eq(cellV(b.snap, rowOf(b.snap, 'OLP.US'), 23), 'Research — no allocation', 'fixture: OLP.US is the research row');
      }
    }
    // (b) the REAL refresh: B2, every write, the run log, the selection log, the Logger lines, the POST bodies, the blob
    baseRefresh = refresh(baseSource, {});
    offRefresh = refresh(newSource, {});
    for (const props of OFF_PROPS) {
      const n = refresh(S1210, props);   // 2026-11-05: the rebuilt v1.21.0 file (see S1210)
      const label = JSON.stringify(props);
      eq(nv(n.status), baseRefresh.status, 'B2 status line ' + label);
      deq(nv(n.calls), baseRefresh.calls, 'every sheet write ' + label);
      deq(nv(n.runLog), baseRefresh.runLog, '_Run_Log rows ' + label);
      deq(nv(n.selLog), baseRefresh.selLog, '_Selection_Log rows ' + label);
      deq(nv(n.logs), baseRefresh.logs, 'Logger lines ' + label);
      deq(n.posted, baseRefresh.posted, 'POST bodies (research + replay) ' + label);
      deq(n.saved, baseRefresh.saved, 'saved state blob ' + label);
      // merge note 2026-11-02 (#778): held seats are v1.20.1 over #895's v1.20.0 (v1.21.1 over #906's v1.21.0 since 2026-11-03): the bundle adds its own memoized DT10_AUDIT_KEEP read
      eq(n.propReads.total - (n.propReads.byKey['DT10_AUDIT_KEEP'] || 0), baseRefresh.propReads.total + 4, 'exactly four property reads added ' + label);
      ok((n.propReads.byKey['DT10_AUDIT_KEEP'] || 0) <= 1, 'the bundle reads DT10_AUDIT_KEEP at most once ' + label);
      deq([n.propReads.byKey[GUARD], n.propReads.byKey[KPI], n.propReads.byKey[MARK], n.propReads.byKey[VENUE]], [1, 1, 1, 1], 'each read once ' + label);
      deq(KNOBS.map(k => n.propReads.byKey[k]), [undefined, undefined, undefined], 'the guard knobs are not read while off ' + label);
      eq(n.props[MARK], props[MARK], 'the label marker is never written while off ' + label);
    }
    ok(/\| output: EXECUTABLE \|/.test(baseRefresh.status) && /funding=final\/6exec/.test(baseRefresh.status), 'fixture: the replay funds six: ' + baseRefresh.status);
    deq(baseRefresh.posted[1].board_funding_symbols, SYMS.concat(['OLP.US']), 'the replay is offered the seven ACTIVE seats');
    deq(baseRefresh.seats.map(r => r[1]), REFRESH_ORDER, 'fixture: board order');
    ok(/6 EXECUTABLE TICKETS \+ 1 RESEARCH \(NO ALLOCATION\) \+ 3 GRACE-HELD \(NO PLAN TODAY\)$/.test(baseRefresh.banner), 'fixture banner: ' + baseRefresh.banner);
    const fb = refresh(baseSource, {}, { propsThrow: true }), fn = refresh(newSource, { [GUARD]: 'enforce', [KPI]: 'on', [VENUE]: 'on' }, { propsThrow: true });
    eq(nv(fn.status), fb.status, 'refresh under a property outage: B2'); deq(nv(fn.calls), fb.calls, 'refresh under a property outage: writes');
  });

  test('V2 observe: every cell identical to off; token names CRC.US(earn 0d) EFC.US(earn 2d); one Logger line; the REAL refresh adds only the B2 token', () => {
    for (const sp of ['observe', ' OBSERVE ', 'Observe']) {
      const o = render(newSource, { tguard: guardCfg('observe') });
      deq(o.snap, getOff().snap, 'observe changes no cell (' + sp + ')');
      deq(o.payload.selected, getOff().payload.selected, 'observe changes no ticket');
      deq(o.payload.kpis, getOff().payload.kpis, 'observe changes no KPI');
      eq(o.token, 'tguard: would hold CRC.US(earn 0d) EFC.US(earn 2d)', 'token');
      const gl = o.logs.filter(l => /ticket guard \(observe\)/.test(l));
      deq(gl, ['[DT10 v' + N.DT10_VERSION + '] ticket guard (observe): would hold 2 of 6 executable ticket(s): CRC.US(earn 0d) EFC.US(earn 2d) | earnings <= 3d, owner rule off, cap none'], 'one Logger line');
      deq(o.logs.filter(l => !/ticket guard/.test(l)), getOff().logs, 'every other Logger line identical');
    }
    // one Calendar_Events read per render: the guard takes the v1.6.0 tagger's map; it reads only when the tagger is switched off
    const reads = o => o.calReads.length;
    eq(reads(render(newSource, { noPostEarn: true })), 1, 'off: the tagger reads once');
    eq(reads(render(newSource, { noPostEarn: true, tguard: guardCfg('enforce') })), 1, 'armed: still one read');
    eq(reads(render(newSource, { tguard: guardCfg('enforce') })), 2, 'with DT10_POST_EARN on: the v1.18.0 read as before, nothing more');
    const tagOff = render(newSource, { noPostEarn: true, props: { DT10_EARNINGS_TAG: 'off' }, tguard: guardCfg('enforce') });
    eq(reads(tagOff), 1, 'tagger off: the guard reads once'); eq(tagOff.token, 'tguard: held 2 (CRC.US earn 0d, EFC.US earn 2d)', 'tagger off: same holds');
    eq(reads(render(newSource, { noPostEarn: true, props: { DT10_EARNINGS_TAG: 'off' } })), 0, 'tagger off, guard off: no read (v1.19.0)');
    eq(render(newSource, { tguard: guardCfg('observe', { earnDays: 1 }) }).token, 'tguard: would hold CRC.US(earn 0d)', 'earnings 1: CRC only');
    eq(render(newSource, { tguard: guardCfg('observe', { earnDays: 0 }) }).token, 'tguard: would hold CRC.US(earn 0d)', 'earnings 0: the day-0 reporter');
    eq(render(newSource, { tguard: guardCfg('observe', { earnDays: 9 }) }).token, 'tguard: would hold DDI.US(earn 9d) CRC.US(earn 0d) EFC.US(earn 2d)', 'earnings 9 adds DDI (board order)');
    eq(render(newSource, { tguard: guardCfg('observe', { ownerRule: true, maxTickets: 2 }) }).token,
       'tguard: would hold ITRN.US(owner rule) DDI.US(owner rule) CRC.US(earn 0d) EFC.US(earn 2d) INDV.US(owner rule)', 'observe names every filter');
    eq(render(newSource, { tguard: guardCfg('observe', { maxTickets: 2 }) }).token,
       'tguard: would hold CRC.US(earn 0d) GLPI.US(cap 2) EFC.US(earn 2d) INDV.US(cap 2)', 'observe: the cap (board order)');
    // REAL refresh (2026-10-10 board: CRC 0d, EFC 2d)
    const r = refresh(newSource, { [GUARD]: 'observe' });
    const tok = 'tguard: would hold CRC.US(earn 0d) EFC.US(earn 2d)';
    ok(r.status.indexOf(' | ' + tok + ' | ') > 0, 'B2 token: ' + r.status);
    eq(stripTok(r.status, tok), getOffRefresh().status, 'B2 = the off line + the token');
    const noB2 = calls => calls.filter(x => !(x[0] === 'setValue' && x[1] === 'Top_10_Investments' && x[2] === 2 && x[3] === 2) && x[1] !== '_Selection_Log');
    deq(noB2(r.calls), noB2(getOffRefresh().calls), 'every other write identical');
    deq(r.selLog.map(rows => rows.map(row => row.map((v, i) => i === 1 ? '<run info>' : v))),
        getOffRefresh().selLog.map(rows => rows.map(row => row.map((v, i) => i === 1 ? '<run info>' : v))), '_Selection_Log rows apart from Run Info');
    deq(r.posted, getOffRefresh().posted, 'POST bodies identical'); deq(r.saved, getOffRefresh().saved, 'state blob identical');
    ok(r.logs.some(l => /ticket guard \(observe\): would hold 2 of 6 executable ticket\(s\): CRC\.US\(earn 0d\) EFC\.US\(earn 2d\)/.test(l)), 'Logger line');
    deq(KNOBS.map(k => r.propReads.byKey[k]), [1, 1, 1], 'armed: each knob read once');
  });

  test('V3 enforce (earnings 3): CRC / EFC take the research path exactly (Funds From + the [held] prefix apart); KPIs recomputed; Unallocated +19,994; banner 4 EXECUTABLE + 2 HELD BY GUARD; nothing re-sized', () => {
    const e = render(newSource, { tguard: guardCfg('enforce') });
    const ctl = render(newSource, { unfunded: ['CRC.US', 'EFC.US'] });   // the existing path: the replay left both unfunded
    eq(e.token, 'tguard: held 2 (CRC.US earn 0d, EFC.US earn 2d)', 'token');
    eq(execOf(e.ctx, e.payload), 4, '4 tickets remain');
    // the research-row shape: every cell of the held rows equals the control's but Funds From (23) and the note prefix (30)
    for (const [sym, why] of [['CRC.US', 'earnings in 0d'], ['EFC.US', 'earnings in 2d']]) {
      const hr = seatCells(e.snap, sym), cr = seatCells(ctl.snap, sym);
      deq(hr.map((v, i) => i === 22 || i === 29 ? null : v), cr.map((v, i) => i === 22 || i === 29 ? null : v), sym + ' = the research row');
      eq(hr[22], 'Held — ' + why, sym + ' Funds From'); eq(cr[22], 'Research — no allocation', 'control Funds From');
      eq(hr[29], '[held: ' + why + '] ' + cr[29], sym + ' note = the prefix + the research note');
      deq([hr[9], hr[10], hr[11], hr[12], hr[13], hr[14], hr[18]], [DASH, 0, 0, DASH, DASH, DASH, 0], sym + ' Entry / Ticket / Shares / Stop / TP1 / TP2 / Gain');
      ok(/^\[held: earnings in \dd\] ⚠ earnings ≤\dd · Research seat — no executable allocation today\. ACTIVE \(day \d\)$/.test(hr[29]), sym + ' note: ' + hr[29]);
      const er = rowOf(e.snap, sym), olpR = rowOf(e.snap, 'OLP.US');
      for (let c = 1; c <= 30; c++) {   // formats and colours of a held row == those of the research row OLP.US
        const a = Object.assign({}, e.snap.cells[er + ':' + c]), b = Object.assign({}, e.snap.cells[olpR + ':' + c]);
        delete a.v; delete b.v; if ((er - olpR) % 2 !== 0) { delete a.bg; delete b.bg; }
        deq(a, b, sym + ' column ' + c + ' formatted like the research row');
      }
    }
    // everything else equals the control (KPIs, tables, footer) except the banner, which names the guard
    deq(diffCells(e.snap, ctl.snap), ['18:1', rowOf(e.snap, 'CRC.US') + ':23', rowOf(e.snap, 'CRC.US') + ':30',
         rowOf(e.snap, 'EFC.US') + ':23', rowOf(e.snap, 'EFC.US') + ':30'].sort(), 'enforce == the research-path control, apart from the banner and the two documented cells');
    deq(e.payload.kpis, ctl.payload.kpis, 'KPIs recomputed exactly as the existing path does');
    const k = kpiRow(e.snap), k0 = kpiRow(getOff().snap);
    eq(k[0], 72747, 'Entered cash unchanged');
    eq(k[1], 56203 - 6945 - 9338, 'Exp. Gain 12M over the remaining tickets');
    eq(k[2], '4 exec + 3 research + 3 grace / 10', 'Selected (the v1.19.0 vocabulary; held = research)');
    eq(k[7] - k0[7], 19994, 'Unallocated gains the held SAR (8,527 + 11,467)');
    eq(k[8], 4, 'Fundable Now');
    ok(k[3] !== k0[3] && k[4] !== k0[4], 'Blended Reliability / R-R recomputed over the remaining tickets');
    eq(banner(e.snap), '✅ FEED ACTIONABLE — EXECUTABLE (verdict age 367m) — SELECTED — 4 EXECUTABLE TICKETS + 1 RESEARCH (NO ALLOCATION) + 3 GRACE-HELD (NO PLAN TODAY) + 2 HELD BY GUARD', 'banner');
    eq(banner(ctl.snap), '✅ FEED ACTIONABLE — EXECUTABLE (verdict age 367m) — SELECTED — 4 EXECUTABLE TICKETS + 3 RESEARCH (NO ALLOCATION) + 3 GRACE-HELD (NO PLAN TODAY)', 'control banner');
    ok(String(cellV(e.snap, Math.max(...Object.keys(e.snap.cells).map(x => +x.split(':')[0])), 1)).indexOf('funding=final/4exec') > 0, 'meta footer funding=final/4exec');
    // surviving tickets byte-identical (no re-sizing, no redistribution); every other seat's note untouched
    for (const t of e.payload.selected) {
      if (t.symbol === 'CRC.US' || t.symbol === 'EFC.US') continue;
      deq(t, getOff().payload.selected.filter(x => x.symbol === t.symbol)[0], t.symbol + ' byte-identical to off');
    }
    eq(e.payload.kpis.total_suggested_sar, Math.round(LIVE.reduce((s, l) => s + l.sar, 0) - 8527.4 - 11467.4), 'spend = the four tickets, unchanged');
    eq(e.ctx.dt10TicketGuardHeldCount_(e.payload.selected), 2, 'two _tguard_held markers');
    ok(e.logs.some(l => l === '[DT10 v' + N.DT10_VERSION + '] ticket guard (enforce): held 2 of 6 executable ticket(s): CRC.US(earn 0d) EFC.US(earn 2d) | earnings <= 3d, owner rule off, cap none | executable 6 -> 4, unallocated +19994 SAR'), 'Logger line');
    // the research-context tags travel: the post-earnings prefix (INDV held by a cap of 3) and the live-fill suffix (a funded _qhold seat)
    const pe = render(newSource, { tguard: guardCfg('enforce', { maxTickets: 3 }) }), peC = render(newSource, { unfunded: ['CRC.US', 'EFC.US', 'INDV.US'] });
    eq(seatCells(pe.snap, 'INDV.US')[29], '[held: ticket cap 3] ' + seatCells(peC.snap, 'INDV.US')[29], 'post-earnings tag kept behind the prefix');
    ok(/^\[held: ticket cap 3\] ⚠ post-earnings 3\/5 · EPS \+4\.1% · Research seat/.test(seatCells(pe.snap, 'INDV.US')[29]), 'INDV note: ' + seatCells(pe.snap, 'INDV.US')[29]);
    const liveIdx = { idx: { 'ITRN.US': { price: 39.65, rel: 74.5, dq: 100, ccy: 'USD', status: 'INVESTABLE' } }, fx: { USD: 3.75 } };
    const qmut = sel => { sel[0]._qhold = true; };
    const lf = render(newSource, { tguard: guardCfg('enforce', { ownerRule: true }), live: liveIdx, mutate: qmut });
    const lfC = render(newSource, { live: liveIdx, mutate: qmut, unfunded: ['ITRN.US', 'DDI.US', 'CRC.US', 'EFC.US', 'INDV.US'] });
    eq(seatCells(lf.snap, 'ITRN.US')[29], '[held: owner rule] ' + seatCells(lfC.snap, 'ITRN.US')[29], 'live-fill suffix kept');
    ok(/ Source row today: investable \(INVEST on the audited grid\)\.$/.test(seatCells(lf.snap, 'ITRN.US')[29]), 'ITRN note: ' + seatCells(lf.snap, 'ITRN.US')[29]);
    // REAL refresh
    const r = refresh(newSource, { [GUARD]: 'enforce' });
    ok(/\| output: EXECUTABLE \|/.test(r.status) && /funding=final\/4exec/.test(r.status), 'B2 output / funding: ' + r.status);
    ok(r.status.indexOf(' | tguard: held 2 (CRC.US earn 0d, EFC.US earn 2d) | ') > 0, 'B2 token: ' + r.status);
    ok(/4 EXECUTABLE TICKETS \+ 1 RESEARCH \(NO ALLOCATION\) \+ 3 GRACE-HELD \(NO PLAN TODAY\) \+ 2 HELD BY GUARD$/.test(r.banner), 'banner: ' + r.banner);
    for (const [sym, why] of [['CRC.US', 'earnings in 0d'], ['EFC.US', 'earnings in 2d']]) {
      const row = seatRow(r, sym);
      deq([row[10], row[11], row[12], row[13], row[22]], [0, 0, DASH, DASH, 'Held — ' + why], sym + ' seat row: ' + JSON.stringify(row));
      ok(String(row[29]).indexOf('[held: ' + why + '] ') === 0, sym + ' note: ' + row[29]);
      const lr = [].concat(...r.selLog).filter(x => x[4] === sym)[0];
      deq([lr[13], lr[14], lr[25]], [0, 0, 'Held — ' + why], sym + ' _Selection_Log row (Ticket, Shares, Funds From)');
    }
    for (const s of ['ITRN.US', 'DDI.US', 'GLPI.US', 'INDV.US', 'OLP.US', 'ZTO.US']) deq(seatRow(r, s), seatRow(getOffRefresh(), s), s + ' row identical');
    eq(r.kpis[7] - getOffRefresh().kpis[7], 19994, 'Unallocated +19,994'); eq(r.kpis[8], 4, 'Fundable Now 4');
    deq(r.posted, getOffRefresh().posted, 'no new POST (no re-sizing)'); deq(r.saved, getOffRefresh().saved, 'state blob identical');
    ok(!/SEAT-CHECK/.test(r.status), 'funded KPI and board agree (no SEAT-CHECK)');
  });

  test('V4 enforce + owner rule: only GLPI.US remains (High, 29.2 %, no earnings); EFC fails earnings, the Medium ones fail the rule', () => {
    const e = render(newSource, { tguard: guardCfg('enforce', { ownerRule: true }) });
    eq(e.token, 'tguard: held 5 (ITRN.US owner rule, DDI.US owner rule, CRC.US earn 0d, EFC.US earn 2d, INDV.US owner rule)', 'token');
    eq(execOf(e.ctx, e.payload), 1, 'one ticket');
    deq(e.payload.selected.filter(t => Number(t.suggested_sar) > 0).map(t => t.symbol), ['GLPI.US'], 'GLPI.US alone');
    eq(e.ctx.dt10OwnerRuleCell_(BY['GLPI.US'] && liveTicket(BY['GLPI.US'], 4)), 'rule: yes', 'GLPI passes the v1.19.0 predicate');
    deq(['ITRN.US', 'DDI.US', 'INDV.US'].map(s => seatCells(e.snap, s)[22]), ['Held — owner rule', 'Held — owner rule', 'Held — owner rule'], 'Funds From');
    eq(seatCells(e.snap, 'EFC.US')[22], 'Held — earnings in 2d', 'EFC: earnings first');
    ok(/1 EXECUTABLE TICKET \+ 1 RESEARCH \(NO ALLOCATION\) \+ 3 GRACE-HELD \(NO PLAN TODAY\) \+ 5 HELD BY GUARD$/.test(banner(e.snap)), 'banner: ' + banner(e.snap));
    const ctl = render(newSource, { unfunded: ['ITRN.US', 'DDI.US', 'CRC.US', 'EFC.US', 'INDV.US'] });
    deq(e.payload.kpis, ctl.payload.kpis, 'KPIs = the research path');
    eq(kpiRow(e.snap)[8], 1, 'Fundable Now 1'); eq(kpiRow(e.snap)[7] - kpiRow(getOff().snap)[7], Math.round(68997.4 - 12133.4) - 0, 'Unallocated + everything but GLPI');
    deq(seatCells(e.snap, 'GLPI.US'), seatCells(getOff().snap, 'GLPI.US'), 'GLPI row untouched');
    // the v1.19.0 predicate, not a restatement: High with Engine ROI 34.0 fails it (ROI < 34), so nothing is left
    const hi = render(newSource, { tguard: guardCfg('enforce', { ownerRule: true }), mutate: sel => { sel[3].engine_roi_pct = 34.0; } });
    eq(hi.token, 'tguard: held 6 (ITRN.US owner rule, DDI.US owner rule, CRC.US earn 0d, GLPI.US owner rule, EFC.US earn 2d, INDV.US owner rule)', 'High + ROI 34.0 fails the rule');
    eq(execOf(hi.ctx, hi.payload), 0, 'no ticket left');
    eq(banner(hi.snap), 'ℹ FEED ACTIONABLE — HELD (verdict age 367m) — SELECTED — 0 EXECUTABLE TICKETS + 1 RESEARCH (NO ALLOCATION) + 3 GRACE-HELD (NO PLAN TODAY) + 6 HELD BY GUARD', 'all held: the board reads HELD');
    eq(hi.ctx.dt10OutputStatus_(hi.payload), 'QUALIFIED_UNFUNDED', 'output state with every ticket held');
    eq(kpiRow(hi.snap)[1], 0, 'Exp. Gain 12M 0'); eq(kpiRow(hi.snap)[7], 72747, 'everything unallocated');
  });

  test('V5 cap 2 without the owner rule: the first two survivors by board rank keep their plan (ITRN.US, DDI.US)', () => {
    const e = render(newSource, { tguard: guardCfg('enforce', { maxTickets: 2 }) });
    eq(e.token, 'tguard: held 4 (CRC.US earn 0d, GLPI.US cap 2, EFC.US earn 2d, INDV.US cap 2)', 'token (board order)');
    deq(e.payload.selected.filter(t => Number(t.suggested_sar) > 0).map(t => t.symbol), ['ITRN.US', 'DDI.US'], 'ITRN and DDI');
    deq(['GLPI.US', 'INDV.US'].map(s => seatCells(e.snap, s)[22]), ['Held — ticket cap 2', 'Held — ticket cap 2'], 'Funds From');
    ok(/^\[held: ticket cap 2\] ⚠ post-earnings 3\/5 /.test(seatCells(e.snap, 'INDV.US')[29]), 'INDV keeps its post-earnings tag');
    const ctl = render(newSource, { unfunded: ['CRC.US', 'EFC.US', 'GLPI.US', 'INDV.US'] });
    deq(e.payload.kpis, ctl.payload.kpis, 'KPIs = the research path');
    deq(diffCells(e.snap, ctl.snap).filter(k => !/^(18:1|\d+:23|\d+:30)$/.test(k)), [], 'apart from the banner, Funds From and the note: the control');
    const c1 = render(newSource, { tguard: guardCfg('enforce', { maxTickets: 1, earnDays: 0 }) });
    deq(c1.payload.selected.filter(t => Number(t.suggested_sar) > 0).map(t => t.symbol), ['ITRN.US'], 'cap 1 (earnings 0): ITRN only');
    eq(c1.token, 'tguard: held 5 (DDI.US cap 1, CRC.US earn 0d, GLPI.US cap 1, EFC.US cap 1, INDV.US cap 1)', 'cap counts survivors only');
    const c9 = render(newSource, { tguard: guardCfg('enforce', { maxTickets: 9 }) });
    eq(c9.token, 'tguard: held 2 (CRC.US earn 0d, EFC.US earn 2d)', 'a cap above the survivors holds nothing more');
  });

  test('V6 never increases: a board with 0 tickets stays 0, a withheld feed holds nothing, every knob combination only removes tickets', () => {
    const zero = sel => { for (const t of sel) t._board_unfunded = true; };
    const z0 = render(newSource, { mutate: zero }), z1 = render(newSource, { mutate: zero, tguard: guardCfg('enforce', { earnDays: 30, ownerRule: true, maxTickets: 1 }) });
    eq(execOf(z0.ctx, z0.payload), 0, 'fixture: 0 tickets');
    eq(execOf(z1.ctx, z1.payload), 0, 'still 0'); eq(z1.token, 'tguard: held 0', 'token'); deq(z1.snap, z0.snap, 'nothing changes');
    const w0 = render(newSource, { state: 'NOT_ACTIONABLE' });
    for (const mode of ['observe', 'enforce']) {
      const w = render(newSource, { state: 'NOT_ACTIONABLE', tguard: guardCfg(mode, { earnDays: 30, ownerRule: true, maxTickets: 1 }) });
      deq(w.snap, w0.snap, 'withheld feed: nothing changes (' + mode + ')');
      eq(w.token, mode === 'enforce' ? 'tguard: held 0' : 'tguard: would hold none', 'withheld token ' + mode);
    }
    const offSel = {}; for (const t of getOff().payload.selected) offSel[t.symbol] = JSON.stringify(t);
    const deploy = 72747, all = LIVE.reduce((s, l) => s + l.sar, 0);
    let combos = 0;
    for (const earnDays of [0, 1, 2, 3, 5, 9, 22, 30]) for (const ownerRule of [false, true]) for (const maxTickets of [0, 1, 2, 3, 6, 10]) {
      const e = render(newSource, { tguard: guardCfg('enforce', { earnDays, ownerRule, maxTickets }) });
      const lbl = JSON.stringify({ earnDays, ownerRule, maxTickets });
      const held = e.payload.selected.filter(t => typeof t._tguard_held === 'string');
      const after = execOf(e.ctx, e.payload);
      ok(after <= 6 && after === 6 - held.length, 'count only falls ' + lbl + ': ' + after + ' / held ' + held.length);
      ok(maxTickets === 0 || after <= maxTickets, 'cap respected ' + lbl);
      for (const t of e.payload.selected) if (typeof t._tguard_held !== 'string') eq(JSON.stringify(t), offSel[t.symbol], 'untouched ' + t.symbol + ' ' + lbl);
      const heldSar = held.reduce((s, t) => s + BY[t.symbol].sar, 0);
      eq(e.payload.kpis.capital_unallocated_sar, Math.round(deploy - Math.round(all - heldSar)), 'freed cash stays unallocated ' + lbl);
      ok(new RegExp('\\u2014 ' + after + ' EXECUTABLE TICKETS? ').test(banner(e.snap)), 'banner count ' + lbl + ': ' + banner(e.snap));
      combos++;
    }
    eq(combos, 96, 'every knob combination');
  });

  test('V7 KPI: Exp. Gain TP1 (SAR) = 11,078 (+-3) for the six, recomputed after the guard; withheld = em dash; off = v1.19.0; restore after a rollback; REAL refresh', () => {
    const want = LIVE.reduce((s, l) => { const t = liveTicket(l, 1); return s + t.suggested_sar * (t.tp1_sar / t.price_sar - 1); }, 0);
    ok(Math.abs(Math.round(want) - 11078) <= 3, 'the fixture reproduces the plan payoffs: ' + want);
    const k = render(newSource, { kpi: 'on' });
    eq(cellV(k.snap, 15, 2), 'Exp. Gain TP1 (SAR)', 'label'); eq(cellV(k.snap, 16, 2), Math.round(want), 'value = sum ticket x (TP1 / price - 1)');
    deq(diffCells(k.snap, getOff().snap), ['15:2', '16:2'], 'only the KPI pair changes');
    eq(cellV(getOff().snap, 15, 2), undefined, 'off: the render never writes the label'); eq(cellV(getOff().snap, 16, 2), 56203, 'off: the annualised KPI');
    ok(Math.abs(cellV(k.snap, 16, 2) - 11078) <= 3 && cellV(getOff().snap, 16, 2) / cellV(k.snap, 16, 2) > 5, 'the 5x gap closed');
    const kg = render(newSource, { kpi: 'on', tguard: guardCfg('enforce') });
    const want4 = LIVE.filter(l => l.sym !== 'CRC.US' && l.sym !== 'EFC.US').reduce((s, l) => { const t = liveTicket(l, 1); return s + t.suggested_sar * (t.tp1_sar / t.price_sar - 1); }, 0);
    eq(cellV(kg.snap, 16, 2), Math.round(want4), 'recomputed after the guard (four tickets)');
    ok(Math.abs(cellV(kg.snap, 16, 2) - (11078 - 1424 - 1628)) <= 3, '8,026 +-3');
    const kw = render(newSource, { kpi: 'on', state: 'NOT_ACTIONABLE' });
    deq([cellV(kw.snap, 15, 2), cellV(kw.snap, 16, 2)], ['Exp. Gain TP1 (SAR)', DASH], 'withheld feed: the em dash');
    const kz = render(newSource, { kpi: 'on', mutate: sel => { for (const t of sel) t._board_unfunded = true; } });
    eq(cellV(kz.snap, 16, 2), DASH, 'no ticket: the em dash');
    const rs = render(newSource, { kpi: 'restore', seedLabel: 'Exp. Gain TP1 (SAR)' });
    eq(cellV(rs.snap, 15, 2), 'Exp. Gain 12M (SAR)', 'restore writes the v1.19.0 label back'); eq(cellV(rs.snap, 16, 2), 56203, 'restore: the v1.19.0 value');
    deq(render(baseSource, { kpi: 'on' }).snap, render(baseSource, {}).snap, 'GOLDEN NEGATIVE: the base ignores the key');
    eq(N.dt10KpiPlanGain_([]), DASH); eq(N.dt10KpiPlanGain_([{ suggested_sar: 1000, suggested_shares: 10, price_sar: 100, tp1_sar: DASH }]), DASH, 'unusable TP1');
    eq(N.dt10KpiPlanGain_([{ suggested_sar: 1000, suggested_shares: 10, price_sar: 100, tp1_sar: 112 }, { suggested_sar: 500, suggested_shares: 0, price_sar: 100, tp1_sar: 150 }]), 120, 'executable tickets only');
    // REAL refresh: label + value + marker; the rollback run restores the label and clears the marker; then byte-identical again
    const r = refresh(newSource, { [KPI]: 'on' });
    eq(r.label2, 'Exp. Gain TP1 (SAR)', 'refresh label'); eq(r.props[MARK], 'tp1', 'marker set');
    const wantR = SYMS.reduce((s, sym) => { const t = researchTicket(BY[sym], SYMS.indexOf(sym)); return s + t.suggested_sar * (t.tp1_sar / t.price_sar - 1); }, 0);
    eq(r.kpis[1], Math.round(wantR), 'refresh value');
    eq(r.status, getOffRefresh().status, 'B2 unchanged by the KPI gate');
    const back = refresh(newSource, { [MARK]: 'tp1' }, { seedTop10: { '15:2': 'Exp. Gain TP1 (SAR)' } });
    eq(back.label2, 'Exp. Gain 12M (SAR)', 'rollback: label restored'); ok(!Object.prototype.hasOwnProperty.call(back.props, MARK), 'rollback: marker deleted');
    eq(back.kpis[1], getOffRefresh().kpis[1], 'rollback: the v1.19.0 value');
    eq(back.status, getOffRefresh().status, 'rollback: B2 unchanged');
    const again = refresh(newSource, {}, { seedTop10: { '15:2': 'Exp. Gain 12M (SAR)' } });
    eq(nv(again.calls.filter(x => !(x[1] === 'Top_10_Investments' && x[2] === 15 && x[3] === 2)).length), getBaseRefresh().calls.length, 'after the restore: v1.19.0 writes');
  });

  test('V8 venue line: the banner says US opens 17:30 Riyadh (feed valid until 10:42); off -> absent; the REAL refresh of a Saturday board names Monday', () => {
    const v = render(newSource, { venue: 'on' });
    eq(banner(v.snap), banner(getOff().snap) + ' — US opens 17:30 Riyadh (feed valid until 10:42)', 'banner');
    deq(diffCells(v.snap, getOff().snap), ['18:1'], 'only the banner changes');
    ok(banner(getOff().snap).indexOf('opens') < 0, 'off: no venue line');
    const vg = render(newSource, { venue: 'on', tguard: guardCfg('enforce', { ownerRule: true }) });
    ok(/\+ 5 HELD BY GUARD — US opens 17:30 Riyadh \(feed valid until 10:42\)$/.test(banner(vg.snap)), 'after the guard: ' + banner(vg.snap));
    const vz = render(newSource, { venue: 'on', mutate: sel => { for (const t of sel) t._board_unfunded = true; } });
    ok(banner(vz.snap).indexOf('opens') < 0, 'no executable ticket: no line');
    const vw = render(newSource, { venue: 'on', state: 'NOT_ACTIONABLE' });
    deq(vw.snap, render(newSource, { state: 'NOT_ACTIONABLE' }).snap, 'withheld feed: no line');
    const vOpen = render(newSource, { venue: 'on', now: Date.UTC(2026, 10, 2, 15, 0, 0), feedRaw: FEED_RAW.replace('2026-11-02 02:42:14', '2026-11-02 14:00:00') });
    ok(banner(vOpen.snap).indexOf('opens') < 0, 'the venue is open now: no line: ' + banner(vOpen.snap));
    const vEarly = render(newSource, { venue: 'on', now: Date.UTC(2026, 10, 2, 11, 0, 0), feedRaw: FEED_RAW.replace('2026-11-02 02:42:14', '2026-11-02 13:30:00') });
    ok(banner(vEarly.snap).indexOf('opens') < 0, 'the session opens before the feed expires: no line: ' + banner(vEarly.snap));
    const vL = render(newSource, { venue: 'on', mutate: (sel, el) => { sel[0].symbol = 'BBOX2.L'; el[0] = 'BBOX2.L'; } });
    ok(/ — London opens 11:00 Riyadh, US opens 17:30 Riyadh \(feed valid until 10:42\)$/.test(banner(vL.snap)), 'two venues, earliest first: ' + banner(vL.snap));
    deq(render(baseSource, { venue: 'on' }).snap, render(baseSource, {}).snap, 'GOLDEN NEGATIVE: the base ignores the key');
    // pure: the venue table and the summer-time rules
    deq(['CRC.US', 'BRK-B', '2222.SR', '1120', 'BBOX.L', 'MC.PA', 'ASML.AS', 'UCB.BR', 'EDP.LS', 'SAP.DE', '0700.HK', '7203.T', 'X.TO'].map(s => N.dt10VenueOf_(s)),
        ['US', 'US', 'SR', 'SR', 'L', 'EU', 'EU', 'EU', 'EU', 'DE', 'HK', 'T', ''], 'suffix table');
    const day = iso => Math.floor(Date.parse(iso + 'T00:00:00Z') / 86400000);
    deq(['2026-03-07', '2026-03-08', '2026-10-31', '2026-11-01', '2026-11-02'].map(d => N.dt10VenueSummer_('us', day(d))), [false, true, true, false, false], 'US rule 2026: 8 Mar - 1 Nov');
    deq(['2026-03-28', '2026-03-29', '2026-10-24', '2026-10-25'].map(d => N.dt10VenueSummer_('eu', day(d))), [false, true, true, false], 'EU rule 2026: 29 Mar - 25 Oct');
    deq(['2027-03-14', '2027-11-07'].map(d => N.dt10VenueSummer_('us', day(d))), [true, false], 'US rule 2027');
    const sess = (k, d) => { const s = N.dt10VenueSession_(N.DT10_VENUES[k], day(d)); return s ? [N.dt10RiyadhClock_(s.open, s.open), N.dt10RiyadhClock_(s.close, s.open)] : null; };
    deq([sess('US', '2026-11-02'), sess('US', '2026-10-30'), sess('SR', '2026-11-01'), sess('SR', '2026-11-06'), sess('L', '2026-11-02'), sess('L', '2026-07-01'),
         sess('EU', '2026-11-02'), sess('DE', '2026-11-02'), sess('HK', '2026-11-02'), sess('T', '2026-11-02'), sess('US', '2026-11-01')],
        [['17:30', 'Tue 00:00'], ['16:30', '23:00'], ['10:00', '15:00'], null, ['11:00', '19:30'], ['10:00', '18:30'], ['11:00', '19:30'], ['11:00', '19:30'],
         ['04:30', '11:00'], ['03:00', '09:30'], null], 'sessions in Riyadh time');
    // REAL refresh at 2026-10-10 19:28:22Z (Saturday 22:28 Riyadh), verdict age 286m: US (summer time) opens Monday 16:30
    const r = refresh(newSource, { [VENUE]: 'on' });
    eq(r.banner, getOffRefresh().banner + ' — US opens Mon 16:30 Riyadh (feed valid until Sun 01:42)', 'refresh banner');
    eq(r.status, getOffRefresh().status, 'B2 unchanged by the venue line');
    const rb = refresh(baseSource, { [VENUE]: 'on' });
    eq(rb.banner, getBaseRefresh().banner, 'GOLDEN NEGATIVE: the base ignores the property');
  });

  test('V9 embedded dt10SelfTest: one line per gate (core ok), armed and faulted lines, every base line kept; the base lacks them', () => {
    const mini = (src, props, throwIt) => {
      const statusRows = []; for (let r = 0; r < 60; r++) statusRows.push(['', '']);
      const ss = { getSheetByName(n) { return n === '_Status' ? { getRange() { return { getValues() { return plain(statusRows); } }; } } : null; }, getRangeByName() { return null; } };
      const ctx = { Logger: { log() {} }, SpreadsheetApp: { getActiveSpreadsheet() { return ss; }, getUi() { throw new Error('no ui'); } },
        PropertiesService: { getScriptProperties() { if (throwIt) throw new Error('down'); return { getProperty(k) { return Object.prototype.hasOwnProperty.call(props, k) ? props[k] : null; }, setProperty() {}, deleteProperty() {} }; } },
        Utilities: { formatDate() { return '2026-11-02 00:00:00'; } }, Session: { getScriptTimeZone() { return 'Asia/Riyadh'; } }, UrlFetchApp: { fetch() { throw new Error('no net'); } } };
      vm.createContext(ctx); vm.runInContext(src, ctx, { filename: '16_Decision_Top10.gs' });
      return String(ctx.dt10SelfTest());
    };
    const rep = mini(newSource, {}), repB = mini(baseSource, {});
    ok(rep.indexOf('[DT10 v' + N.DT10_VERSION + '] self-test') === 0, 'version line');   // merge note 2026-11-05 (#778): held seats are v1.21.2 over #921's v1.21.1
    const lines = ['ticket guard (DT10_TICKET_GUARD): off (off = v1.19.0 board; observe names; enforce holds) | core: ok (earnings 3 holds A, 5 adds C, owner rule adds B, cap 1 keeps B; apply ok)',
      'kpi plan gain (DT10_KPI_PLAN_GAIN): off (Exp. Gain TP1 (SAR) = sum Ticket SAR x (TP1 SAR / Price SAR - 1); off = v1.19.0 KPI) | core: ok (four tickets 1050 SAR; no ticket = em dash)',
      'venue line (DT10_VENUE_LINE): off (approximate regular sessions in Riyadh time, no holidays; display only) | core: ok (US 17:30 winter / 16:30 summer, London 11:00, Tadawul Sun 10:00, Friday night -> Mon; open now / withheld / unsized = none)'];
    for (const l of lines) { ok(rep.split('\n').indexOf(l) > 0, 'line: ' + l); ok(repB.indexOf(l.split(' (')[0]) < 0, 'GOLDEN NEGATIVE: the base lacks ' + l.split(' (')[0]); }
    ok(rep.indexOf(': FAIL') < 0 && rep.indexOf('FAIL [') < 0 && rep.indexOf('apply FAIL') < 0, 'no FAIL');
    const baseLines = repB.split('\n'), newLines = rep.split('\n');
    deq(baseLines.filter(l => l.indexOf('[DT10 v1.19.0]') !== 0 && newLines.indexOf(l) < 0), [], 'every base self-test line present');
    eq(newLines.length, baseLines.length + 3, 'exactly three lines added');
    const armed = mini(newSource, { [GUARD]: ' Enforce ', DT10_GUARD_EARN_DAYS: '5', DT10_GUARD_OWNER_RULE: 'on', DT10_GUARD_MAX_TICKETS: '2', [KPI]: 'ON', [VENUE]: 'on' });
    ok(armed.indexOf('ticket guard (DT10_TICKET_GUARD): enforce [earnings <= 5d, owner rule on, cap 2]') > 0 && armed.indexOf('kpi plan gain (DT10_KPI_PLAN_GAIN): on (') > 0 &&
       armed.indexOf('venue line (DT10_VENUE_LINE): on (') > 0 && armed.indexOf('FAIL') < 0, 'armed lines');
    ok(mini(newSource, { [MARK]: 'tp1' }).indexOf('kpi plan gain (DT10_KPI_PLAN_GAIN): off (label restore pending)') > 0, 'marker line');
    ok(mini(newSource, { DT10_BOARD_FUNDING_MODE: 'research' }).indexOf('apply skipped (DT10_BOARD_FUNDING_MODE research)') > 0, 'research-only mode skips the apply check');
    const faulted = mini(newSource, {}, true);
    ok(faulted.indexOf('ticket guard (DT10_TICKET_GUARD): off') > 0 && faulted.indexOf('kpi plan gain (DT10_KPI_PLAN_GAIN): off') > 0 &&
       faulted.indexOf('venue line (DT10_VENUE_LINE): off') > 0, 'a property fault reads off');
  });

  // 2026-11-03: V10 reads the rebuilt v1.20.0 file (s1200); the v1.21.0 diff has its own lint
  // in tests/test_dt10_v1210_exdiv_guard.js.
  test('V10 ES5 / ASCII lint of the diff; inventory: 17 functions and 14 vars added, 0 removed; 15 hunks whose reversal is the reviewed v1.19.0 file', () => {
    const newSource = layerSource;   // merge note 2026-11-03 (#778): held seats are v1.21.1 over #906's v1.21.0: the v1.19.0 reversal reads the layer (main's v1.21.0; main's v1.21.1 since the 2026-11-05 merge note)
    const baseLines = {}, bl = baseSource.split('\n'), nl = s1200.split('\n');
    for (const l of bl) baseLines[l] = true;
    const added = nl.filter(l => !baseLines[l]);
    ok(added.length >= 600 && added.length <= 760, 'added lines ' + added.length);
    for (const l of added) {
      const code = l.replace(/\/\/.*$/, '');
      ok(!/\b(let|const|class)\s|=>|`|\.\.\.|\.includes\(/.test(code) || /^\s*(\*|\/\*)/.test(l), 'ES6 token: ' + l);
      if (!/^[\x09\x20-\x7e]*$/.test(l)) ok(baseLines[l.replace('1.20.0', '1.19.0')] === true, 'non-ASCII added line: ' + l);
    }
    function nonAscii(s) { const m = {}; for (let j = 0; j < s.length; j++) { const c = s.charCodeAt(j); if (c > 126) m[c] = (m[c] || 0) + 1; } return m; }
    deq(nonAscii(s1200), nonAscii(baseSource), 'non-ASCII code point multiset unchanged');
    ok(s1200.indexOf('\r') < 0, 'LF only');
    const why = s1200.slice(s1200.indexOf(' * v1.20.0 (2026-11-02)'), s1200.indexOf(' * v1.19.0 (2026-11-01) [QUALIFIED-HOLD'));
    ok(why.length > 5000 && /^[\x09\x0a\x20-\x7e]*$/.test(why), 'WHY block present (newest first) and ASCII');
    for (const w of [' * WHY (live review', ' * WHAT -- three Script Properties', ' * GATES: ', ' * ROLLBACK: ']) ok(why.indexOf(w) >= 0, 'WHY section ' + w);
    ok(s1200.indexOf(baseSource.slice(baseSource.indexOf(' * v1.19.0 (2026-11-01) [QUALIFIED-HOLD'), baseSource.indexOf(' * v1.18.0 (2026-10-27) [POST-EARNINGS]'))) > 0, 'the v1.19.0 WHY block verbatim');
    for (const l of bl.filter(x => /^ \* v\d+\.\d+\.\d+ \(/.test(x))) ok(s1200.indexOf(l) > 0, 'WHY header lost: ' + l);
    const names = (src, re) => { const out = {}; let m; while ((m = re.exec(src))) out[m[1]] = true; return out; };
    const fb = names(baseSource, /function\s+([A-Za-z_$][\w$]*)\s*\(/g), fn = names(s1200, /function\s+([A-Za-z_$][\w$]*)\s*\(/g);
    deq(Object.keys(fb).filter(k => !fn[k]), [], 'functions removed (any level)');
    const tb = names(baseSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), tn = names(s1200, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    const FNS = ['dt10KpiPlanGainMode_', 'dt10KpiPlanGain_', 'dt10KpiPlanLabelMark_', 'dt10RiyadhClock_', 'dt10TicketGuardApply_', 'dt10TicketGuardConfig_',
      'dt10TicketGuardCore_', 'dt10TicketGuardExec_', 'dt10TicketGuardHeldCount_', 'dt10TicketGuardMode_', 'dt10TicketGuardToken_', 'dt10TicketGuard_',
      'dt10VenueLineOn_', 'dt10VenueLine_', 'dt10VenueOf_', 'dt10VenueSession_', 'dt10VenueSummer_'];
    deq(Object.keys(tn).filter(k => !tb[k]).sort(), FNS, 'top-level functions added');
    eq(Object.keys(tb).length, 233, 'base census'); eq(Object.keys(tn).length, 250, 'new census');
    const vb = names(baseSource, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = names(s1200, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq(Object.keys(vb).filter(k => !vn[k]), [], 'vars removed');
    deq(Object.keys(vn).filter(k => !vb[k]).sort(), ['DT10_GUARD_EARN_DAYS_DEF', 'DT10_GUARD_EARN_DAYS_MAX', 'DT10_GUARD_EARN_DAYS_PROP', 'DT10_GUARD_MAX_TICKETS_MAX',
      'DT10_GUARD_MAX_TICKETS_PROP', 'DT10_GUARD_OWNER_RULE_PROP', 'DT10_KPI_PLAN_GAIN_LABEL', 'DT10_KPI_PLAN_GAIN_PROP', 'DT10_KPI_PLAN_LABEL_PROP',
      'DT10_TGUARD_LIVE_RE', 'DT10_TGUARD_TAGS_RE', 'DT10_TICKET_GUARD_PROP', 'DT10_VENUES', 'DT10_VENUE_LINE_PROP'], 'vars added');
    eq(Object.keys(vb).length, 126); eq(Object.keys(vn).length, 140);
    const newSet = {}; for (const l of nl) newSet[l] = true;
    deq(bl.filter(l => !newSet[l]).map(l => l.trim()).sort(), ['* Version: 1.19.0 (see DT10_VERSION; header kept in lockstep — restored', "var DT10_VERSION = '1.19.0';"].sort(), 'only the two version lines left');
    for (const name of FNS) {
      const start = s1200.indexOf('function ' + name + '(');
      const body = s1200.slice(start, s1200.indexOf('\n}\n', start) + 3);
      ok(start > 0 && /^[\x00-\x7f]*$/.test(body), 'ASCII ' + name);
      new vm.Script('(function(){' + body + '})');
    }
    const edits = require(path.join(ROOT, 'tests', 'js', 'dt10_edits_v1200.json'));
    eq(edits.length, 15, 'fifteen hunks');
    eq(sha256(history.v1190Source(newSource)), history.V1190_SHA256, 'reversal = the reviewed v1.19.0 file');
    // the owner-rule filter calls the v1.19.0 predicate, it does not restate it
    const core = s1200.slice(s1200.indexOf('function dt10TicketGuardCore_('), s1200.indexOf('\n}\n', s1200.indexOf('function dt10TicketGuardCore_(')));
    ok(core.indexOf("dt10OwnerRuleCell_(t) !== 'rule: yes'") > 0 && core.indexOf('DT10_OWNER_RULE_ROI_MAX') < 0 && core.indexOf('HIGH') < 0, 'owner rule reused, not duplicated');
    const apply = s1200.slice(s1200.indexOf('function dt10TicketGuardApply_('), s1200.indexOf('\n}\n', s1200.indexOf('function dt10TicketGuardApply_(')));
    ok(apply.indexOf('dt10FinalizeBoard_(payload);') > 0 && apply.indexOf('t._board_unfunded = true;') > 0 && apply.indexOf('suggested_sar =') < 0, 'held rows go through dt10FinalizeBoard_, no new row shape');
  });

  test('V11 service reads: spellings -> modes; the knobs (read only while armed); faults read off', () => {
    const mk = (props, fault) => loadRender(newSource, props, { propsThrow: fault });
    for (const [v, want] of [['observe', 'observe'], [' ENFORCE ', 'enforce'], ['Observe', 'observe'], ['', 'off'], ['off', 'off'], ['on', 'off'],
      ['yes', 'off'], ['1', 'off'], ['enforced', 'off'], ['observ', 'off']]) eq(mk({ [GUARD]: v }).dt10TicketGuardMode_(), want, 'guard ' + JSON.stringify(v));
    eq(mk({}).dt10TicketGuardMode_(), 'off', 'guard absent'); eq(mk({}, true).dt10TicketGuardMode_(), 'off', 'guard fault');
    const off = mk({}); eq(off.dt10TicketGuardConfig_(), null, 'off -> null'); deq(off.__propReads, [GUARD], 'off: one read');
    const on = mk({ [GUARD]: 'enforce' }); deq(plain(on.dt10TicketGuardConfig_()), { mode: 'enforce', earnDays: 3, ownerRule: false, maxTickets: 0 }, 'defaults');
    deq(on.__propReads, [GUARD].concat(KNOBS, ['DT10_GUARD_EXDIV_DAYS']), 'armed: the three knobs, once each (+ the v1.21.0 ex-div days knob)');
    for (const [d, want] of [['0', 0], ['45', 30], ['-2', 0], ['abc', 3], [' 5 ', 5], ['2.9', 2]]) eq(mk({ [GUARD]: 'observe', DT10_GUARD_EARN_DAYS: d }).dt10TicketGuardConfig_().earnDays, want, 'earn days ' + d);
    for (const [d, want] of [['on', true], [' ON ', true], ['yes', false], ['1', false], ['', false]]) eq(mk({ [GUARD]: 'observe', DT10_GUARD_OWNER_RULE: d }).dt10TicketGuardConfig_().ownerRule, want, 'owner rule ' + d);
    for (const [d, want] of [['', 0], ['2', 2], ['99', 50], ['-1', 0], ['x', 0]]) eq(mk({ [GUARD]: 'observe', DT10_GUARD_MAX_TICKETS: d }).dt10TicketGuardConfig_().maxTickets, want, 'cap ' + d);
    eq(mk({ [GUARD]: 'enforce' }, true).dt10TicketGuardConfig_(), null, 'fault -> off');
    for (const [v, m, want] of [['on', null, 'on'], [' ON ', null, 'on'], ['off', 'tp1', 'restore'], ['', 'tp1', 'restore'], ['on', 'tp1', 'on'], ['', 'TP1', 'off'],
      ['yes', null, 'off'], ['', '', 'off']]) {
      const p = { [KPI]: v }; if (m !== null) p[MARK] = m;
      eq(mk(p).dt10KpiPlanGainMode_(), want, 'kpi ' + JSON.stringify([v, m]));
    }
    eq(mk({ [KPI]: 'on' }, true).dt10KpiPlanGainMode_(), 'off', 'kpi fault');
    for (const [v, want] of [['on', true], [' On ', true], ['', false], ['off', false], ['yes', false], ['1', false]]) eq(mk({ [VENUE]: v }).dt10VenueLineOn_(), want, 'venue ' + JSON.stringify(v));
    eq(mk({ [VENUE]: 'on' }, true).dt10VenueLineOn_(), false, 'venue fault');
  });

  return sha256(JSON.stringify([getOff().snap, getOffRefresh().status, render(newSource, { tguard: guardCfg('enforce', { ownerRule: true, maxTickets: 2 }), kpi: 'on', venue: 'on' }).snap,
    refresh(newSource, { [GUARD]: 'enforce', [KPI]: 'on', [VENUE]: 'on' }).status])).slice(0, 16);
}

const digests = [];
let total = 0;
for (let run = 1; run <= 3; run++) {
  passed = 0; failed = 0;
  const d = battery();
  digests.push(d);
  total = passed + failed;
  console.log('run ' + run + ': ' + passed + '/' + total + ' digest ' + d);
  if (failed) { console.log('FAIL ' + passed + '/' + total); process.exit(1); }
}
if (digests[0] !== digests[1] || digests[1] !== digests[2]) { console.log('FAIL x3 digest mismatch ' + digests.join(' ')); process.exit(1); }
console.log('V1200 V0-V11 x3 identical digest ' + digests[0] + ' | new SHA256 ' + sha256(newSource) + ' | base SHA256 ' + sha256(baseSource));
console.log('PASS ' + passed + '/' + total);
