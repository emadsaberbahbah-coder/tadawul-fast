#!/usr/bin/env node
/* tests/test_dt10_v1210_exdiv_guard.js
 * Harness for apps_script/16_Decision_Top10.gs v1.21.0 [EXDIV-GUARD] (Tue 3
 * Nov 2026): the ticket guard's ex-dividend filter, two Script Properties
 * of the EXISTING guard (DT10_GUARD_EXDIV_DAYS, DT10_GUARD_EXDIV_PCT), read
 * only while DT10_TICKET_GUARD is observe | enforce; default OFF.
 *
 * Dual-tree node harness over the REAL file: loads the delivered .gs source
 * and the v1.20.0 BASE -- a file given as the second argument (it must carry
 * the pinned SHA-256 of origin/main 8478029d) or, by default, the base
 * rebuilt from the delivered file by reversing exactly the enumerated
 * v1.21.0 edits (tests/js/dt10_edits_v1210.json, SHA-256 pinned) -- into
 * separate vm contexts with the two GAS shims of
 * tests/test_dt10_v1200_ticket_guard.js (copied verbatim: the cell-model
 * spreadsheet for the REAL dt10RenderPayload_, the full workbook for the
 * REAL refreshDecisionTop10), each with a Dividend_Events tab.
 * Fixture = the v1.20.0 harness's 2 Nov board with the real INDV.US close
 * (36.28 USD) and the Dividend_Events rows of scripts/run_calendar_sync.py
 * v1.3.0: INDV.US 8.13 USD ex in 1 day (Indivior's special: 22.4 % of the
 * price), GLPI.US 0.78 in 2 (1.7 %), CRC.US 0.405 in 1 (0.8 %; reports
 * today), EFC.US 0.13 in 0 (1.0 %; reports in 2), ITRN.US 0.50 in 10
 * (1.3 %), OLP.US 0.45 in 1 (research seat), ZTO.US 0.35 in 2 (grace),
 * DDI.US 1.00 yesterday (past).
 * Sections:
 *   E0  base oracle (SHA-256 pinned), version lockstep, both files compile,
 *       the new constants; the base lacks them
 *   E1  GOLDEN NEGATIVE: the days knob absent / blank / junk (any guard
 *       mode), or set while the guard is off, or a property fault -> the
 *       REAL render and the REAL refresh deep-equal the v1.20.0 base; the
 *       tab is never read; the only extra read is the days knob while armed
 *   E2  observe (days 3): every cell identical to off; token adds
 *       INDV.US(ex-div 1d 22.4%); one Logger line with the ex-div suffix;
 *       one tab read; thresholds and windows; REAL refresh B2 = off + token
 *   E3  enforce (days 3): INDV.US takes the research path exactly like an
 *       earnings hold (Funds From 'Held -- ex-dividend in 1d (22.4% of
 *       price)' + the '[held: ...] ' prefix apart); CRC / EFC exactly the
 *       v1.20.0 earnings holds; KPIs recomputed; banner + 3 HELD BY GUARD;
 *       survivors byte-identical; REAL refresh (B2, seats, _Selection_Log)
 *   E4  the day window: Riyadh calendar day, ex-date today = 0d, past never,
 *       a Date cell, the stored 'Days To ExDiv' column is not trusted
 *   E5  size unknown and units: no amount, another currency, MIXED, no
 *       price; GBp / GBX / ZAc subunits; a render-level 'size unknown' hold
 *   E6  precedence and monotonicity: earnings first; ex-div before the owner
 *       rule and the cap; non-executable seats never checked; every knob
 *       combination only adds holds to the v1.20.0 set; a withheld feed
 *       holds nothing
 *   E7  the map builder and the tab read (header search, column order,
 *       duplicates, amounts, dates, bounds, absent tab, faults)
 *   E8  a Dividend_Events read failure: no ex-div hold, earnings still
 *       hold, no FAILED line
 *   E9  the embedded dt10SelfTest: the v1.20.0 report while the knob is
 *       unset; one 'core: ok' line while it is set and the guard is armed
 *   E10 ES5 / ASCII lint of the v1.21.0 diff; inventory (3 functions, 6
 *       vars added, 0 removed); 13 hunks whose reversal is the v1.20.0 file;
 *       WHY block; every prior WHY verbatim
 *   E11 service reads: the days / pct spellings; reads only while armed;
 *       the pct knob only while the days knob is set; faults read off
 * The whole battery runs three times and the three digests must match; the
 * last line is 'PASS k/k' (exit 1 on any failure).
 * Usage: node tests/test_dt10_v1210_exdiv_guard.js [new.gs] [base.gs]
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
function sha256(s) { return crypto.createHash('sha256').update(s).digest('hex'); }
let baseSource;
try {
  baseSource = BASE_PATH ? fs.readFileSync(BASE_PATH, 'utf8') : history.v1200Source(newSource);
} catch (eBase) {
  console.log('FAIL E0 base oracle: ' + String(eBase && eBase.message || eBase).split('\n')[0]);
  console.log('FAIL 0/1');
  process.exit(1);
}
/* 2026-11-05: v1.21.1 (approved Mutual_Funds scope 2422, policy data only) is stacked on this change.
 * The rebuilt v1.21.0 file (the v1.21.1 edits reversed, SHA-256 pinned) feeds E1's refresh-level
 * golden negative and E10's lint; tests/test_dt10_v1211_mf_scope.js proves v1.21.1 = v1.21.0 apart
 * from that data. Every other check runs on the delivered file. */
const S1210 = history.v1210Source(newSource);
const FIX = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'fixtures', 'dt10_candidates_2026-10-10.json'), 'utf8'));
const PAGES = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'fixtures', 'dt10_pages_2026-10-05.json'), 'utf8'));
const GUARD = 'DT10_TICKET_GUARD', KNOB = 'DT10_GUARD_EXDIV_DAYS', PCTK = 'DT10_GUARD_EXDIV_PCT';
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
/* The version string reaches the Logger lines and B2; comparisons against
 * the base read the delivered version as the base's. */
const nv = x => JSON.parse(JSON.stringify(x).replace(/v1\.2(?:1\.[01]|2\.0|3\.[01]|4\.0|5\.0|6\.0|7\.0|8\.0)/g, 'v1.20.0'));   // 2026-11-05: delivered v1.21.1, rebuilt v1.21.0

/* The Dividend_Events grid (scripts/run_calendar_sync.py v1.3.0 DIV_HEADERS),
 * ex-dates relative to the Riyadh day `d0`. */
const DIV_HEADERS = ['Symbol', 'Ex-Div Date', 'Days To ExDiv', 'Amount', 'Currency', 'Pay Date', 'Record Date', 'Period', 'Updated At (Riyadh)', 'Source'];
const DIVS = [['INDV.US', 1, 8.13], ['GLPI.US', 2, 0.78], ['CRC.US', 1, 0.405], ['EFC.US', 0, 0.13], ['ITRN.US', 10, 0.5],
              ['OLP.US', 1, 0.45], ['ZTO.US', 2, 0.35], ['DDI.US', -1, 1.0]];
function divGrid(d0, rows) {
  const out = [DIV_HEADERS.slice()];
  for (const [s, d, a, ccy] of rows || DIVS) {
    out.push([s, isoShift(d0, d), d, a, ccy === undefined ? 'USD' : ccy, isoShift(d0, d + 3), isoShift(d0, d + 1), 'Quarterly',
              d0 + ' 16:28:00', 'eodhd_div']);
  }
  return out;
}

/* ============================================================ the live board of 2 Nov 2026 */
const LIVE = [
  { sym: 'ITRN.US', name: 'Ituran Location and Control', sector: 'Technology', px: 39.65, sar: 12199.4, eroi: 35.0, band: 'Medium', g12: 9935, pay: 2110, rel: 74.5, days: 5 },
  { sym: 'DDI.US', name: 'DoubleDown Interactive', sector: 'Communication Services', px: 9.12, sar: 12334.4, eroi: 35.0, band: 'Medium', g12: 10045, pay: 2023, rel: 72.0, days: 5 },
  { sym: 'CRC.US', name: 'California Resources', sector: 'Energy', px: 50.56, sar: 8527.4, eroi: 31.2, band: 'Medium', g12: 6945, pay: 1424, rel: 76.5, days: 5 },
  { sym: 'GLPI.US', name: 'Gaming and Leisure Properties', sector: 'Real Estate', px: 47.21, sar: 12133.4, eroi: 29.2, band: 'High', g12: 9880, pay: 1771, rel: 81.0, days: 4 },
  { sym: 'EFC.US', name: 'Ellington Financial', sector: 'Financial Services', px: 13.05, sar: 11467.4, eroi: 30.4, band: 'High', g12: 9338, pay: 1628, rel: 79.5, days: 4 },
  { sym: 'INDV.US', name: 'Indivior', sector: 'Healthcare', px: 36.28, sar: 12335.4, eroi: 35.0, band: 'Medium', g12: 10060, pay: 2122, rel: 73.0, days: 3 }
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
  const divReads = [];
  if (!opts.noDiv) {   // v1.21.0: the Dividend_Events tab of scripts/run_calendar_sync.py v1.3.0
    const div = book.addSheet('Dividend_Events', 200, 12);
    const dg = opts.divGrid || divGrid(opts.calDay || '2026-11-02');
    div.getRange(1, 1, dg.length, dg[0].length).setValues(dg);
    const divRange = div.getRange;
    div.getRange = (...a) => { const rg = divRange(...a); const gv = rg.getValues;
      rg.getValues = () => { divReads.push(a.join('x')); if (opts.divThrow) throw new Error('synthetic Dividend_Events outage'); return gv(); }; return rg; };
  }
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
           logs: ctx.__logs.slice(), token: p._dt10_tguard_note, uv, calReads, divReads };
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
  if (!opts.noDiv) env.sheets['Dividend_Events'] = mkSheet(env, 'Dividend_Events', { grid: opts.divGrid || divGrid(TODAY_F) });
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
                        seedTop10: opts.seedTop10, noDiv: opts.noDiv, divGrid: opts.divGrid });
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
const XD = (mode, extra) => guardCfg(mode, Object.assign({ exdivDays: 3, exdivPct: 2 }, extra || {}));
const divReadsOf = run => run.env.reads.filter(x => x[1] === 'Dividend_Events').length;
const noB2 = calls => calls.filter(x => !(x[0] === 'setValue' && x[1] === 'Top_10_Investments' && x[2] === 2 && x[3] === 2) && x[1] !== '_Selection_Log');
const runInfoOff = rows => rows.map(rr => rr.map(row => row.map((v, i) => i === 1 ? '<run info>' : v)));
const WHY_INDV = 'ex-dividend in 1d (22.4% of price)';
function battery() {
  const N = loadRender(newSource, {});
  const B = loadRender(baseSource, {});

  test('E0 base oracle is the reviewed v1.20.0 file (SHA-256 pinned); versions in lockstep; both files compile; the new constants (the base lacks them)', () => {
    eq(sha256(baseSource), history.V1200_SHA256, 'base SHA-256');
    eq(B.DT10_VERSION, '1.20.0', 'base version');
    eq(N.DT10_VERSION, '1.28.0', 'delivered version');   // 2026-11-05: v1.21.1 pins moved
    ok(/^ \* Version: 1\.28\.0 /.test(newSource.split('\n')[3]), 'header line 4 lockstep');   // 2026-11-08: v1.22.0 pin moved
    ok(newSource.indexOf("var DT10_VERSION = '1.28.0';") > 0, 'DT10_VERSION literal');
    deq([N.DT10_GUARD_EXDIV_DAYS_PROP, N.DT10_GUARD_EXDIV_PCT_PROP, N.DT10_DIVIDEND_SHEET], [KNOB, PCTK, 'Dividend_Events'], 'names');
    deq([N.DT10_GUARD_EXDIV_DAYS_MAX, N.DT10_GUARD_EXDIV_PCT_DEF, N.DT10_GUARD_EXDIV_PCT_MAX], [30, 2, 100], 'bounds');
    for (const k of ['DT10_GUARD_EXDIV_DAYS_PROP', 'DT10_DIVIDEND_SHEET', 'dt10ExDivHold_', 'dt10ExDivMap_', 'dt10ExDivMapFromValues_']) eq(typeof B[k], 'undefined', 'the base lacks ' + k);
    new vm.Script(newSource, { filename: '16_Decision_Top10.gs' });
    new vm.Script(baseSource, { filename: 'base.gs' });
    eq(r1(8.13 / 36.28 * 100), 22.4, 'fixture: INDV 8.13 on 36.28');
  });

  let offR = null, offRefresh = null;
  const getOff = () => offR || (offR = render(newSource, {}));
  const getOffRefresh = () => offRefresh || (offRefresh = refresh(newSource, {}));

  test('E1 GOLDEN NEGATIVE: days knob absent / blank / junk (any guard mode), set while the guard is off, or a property fault -> the REAL render and the REAL refresh deep-equal the v1.20.0 base; the tab is never read', () => {
    // render level: the guard object without exdiv keys is what dt10TicketGuardConfig_ returns without the knob
    for (const cfg of [null, guardCfg('observe'), guardCfg('enforce'), guardCfg('enforce', { ownerRule: true, maxTickets: 2 }), guardCfg('observe', { earnDays: 9 })]) {
      const o = cfg ? { tguard: cfg } : {};
      const n = render(newSource, o), b = render(baseSource, o), label = cfg ? JSON.stringify(cfg) : 'off';
      deq(n.snap, b.snap, 'every cell, merge and border ' + label);
      eq(n.token, b.token, 'status token ' + label);
      deq(nv(n.logs), b.logs, 'Logger lines ' + label);
      deq(n.propReads, b.propReads, 'property reads ' + label);
      deq(n.payload.selected, b.payload.selected, 'tickets ' + label); deq(n.payload.kpis, b.payload.kpis, 'KPIs ' + label);
      eq(n.divReads.length, 0, 'no Dividend_Events read ' + label);
    }
    for (const st of ['EXECUTABLE', 'NOT_ACTIONABLE']) {
      deq(render(newSource, { state: st, tguard: guardCfg('enforce') }).snap, render(baseSource, { state: st, tguard: guardCfg('enforce') }).snap, 'feed ' + st);
    }
    // refresh level: Script Properties as the owner would type them. 2026-11-05: on the rebuilt v1.21.0
    // file (S1210): v1.21.1's approved fund count reaches the readiness reason in the Logger lines
    const JUNK = [null, '', ' ', 'abc', '-1', '31', '1.5', '3d', '+3'];
    for (const mode of ['off', 'observe', 'enforce']) {
      const bp = mode === 'off' ? {} : { [GUARD]: mode };
      const b = refresh(baseSource, bp);
      for (const kv of mode === 'off' ? [null, '3', '0', '30', 'abc'] : JUNK) {
        const props = Object.assign({}, bp); if (kv !== null) { props[KNOB] = kv; props[PCTK] = '0.5'; }
        const n = refresh(S1210, props), label = mode + ' knob ' + JSON.stringify(kv);
        eq(nv(n.status), b.status, 'B2 status line ' + label);
        deq(nv(n.calls), b.calls, 'every sheet write ' + label);
        deq(nv(n.runLog), b.runLog, '_Run_Log rows ' + label);
        deq(nv(n.selLog), b.selLog, '_Selection_Log rows ' + label);
        deq(nv(n.logs), b.logs, 'Logger lines ' + label);
        deq(n.posted, b.posted, 'POST bodies ' + label); deq(n.saved, b.saved, 'state blob ' + label);
        const extra = mode === 'off' ? 0 : 1;   // the days knob, read once while the guard is armed; the pct knob never
        eq(n.propReads.total, b.propReads.total + extra, 'property reads ' + label);
        eq(n.propReads.byKey[KNOB] || 0, extra, 'days knob reads ' + label); eq(n.propReads.byKey[PCTK] || 0, 0, 'pct knob reads ' + label);
        for (const k of Object.keys(b.propReads.byKey)) eq(n.propReads.byKey[k], b.propReads.byKey[k], 'reads of ' + k + ' ' + label);
        eq(divReadsOf(n), 0, 'no Dividend_Events read ' + label);
      }
    }
    // a property outage: the guard is off whatever the knobs say
    const fb = refresh(baseSource, {}, { propsThrow: true }), fn = refresh(newSource, { [GUARD]: 'enforce', [KNOB]: '3' }, { propsThrow: true });
    eq(nv(fn.status), fb.status, 'property outage: B2'); deq(nv(fn.calls), fb.calls, 'property outage: writes'); eq(divReadsOf(fn), 0, 'property outage: no tab read');
    // the tab absent: the knob-less guard does not notice
    deq(render(newSource, { noDiv: true, tguard: guardCfg('enforce') }).snap, render(baseSource, { noDiv: true, tguard: guardCfg('enforce') }).snap, 'no tab');
  });

  test('E2 observe (days 3): every cell identical to off; token adds INDV.US(ex-div 1d 22.4%); one Logger line with the ex-div suffix; one tab read; thresholds and windows; REAL refresh B2 = the off line + the token', () => {
    const o = render(newSource, { tguard: XD('observe') });
    deq(o.snap, getOff().snap, 'observe changes no cell');
    deq(o.payload.selected, getOff().payload.selected, 'observe changes no ticket');
    deq(o.payload.kpis, getOff().payload.kpis, 'observe changes no KPI');
    eq(o.token, 'tguard: would hold CRC.US(earn 0d) EFC.US(earn 2d) INDV.US(ex-div 1d 22.4%)', 'token');
    deq(o.logs.filter(l => /ticket guard/.test(l)), ['[DT10 v1.28.0] ticket guard (observe): would hold 3 of 6 executable ticket(s): CRC.US(earn 0d) EFC.US(earn 2d) INDV.US(ex-div 1d 22.4%) | earnings <= 3d, owner rule off, cap none, ex-div <= 3d >= 2%'], 'one Logger line');
    deq(o.logs.filter(l => !/ticket guard/.test(l)), getOff().logs, 'every other Logger line identical');
    eq(o.divReads.length, 1, 'one Dividend_Events read per render');
    eq(o.calReads.length, getOff().calReads.length, 'Calendar_Events reads unchanged');
    const tok = extra => render(newSource, { tguard: XD('observe', extra) }).token;
    eq(tok({ exdivPct: 1.5 }), 'tguard: would hold CRC.US(earn 0d) GLPI.US(ex-div 2d 1.7%) EFC.US(earn 2d) INDV.US(ex-div 1d 22.4%)', 'pct 1.5 adds GLPI 1.7 % (board order)');
    eq(tok({ exdivPct: 22.4 }), 'tguard: would hold CRC.US(earn 0d) EFC.US(earn 2d) INDV.US(ex-div 1d 22.4%)', 'pct 22.4: inclusive');
    eq(tok({ exdivPct: 22.5 }), 'tguard: would hold CRC.US(earn 0d) EFC.US(earn 2d)', 'pct 22.5: INDV passes');
    eq(tok({ exdivPct: 100 }), 'tguard: would hold CRC.US(earn 0d) EFC.US(earn 2d)', 'pct 100: none measured');
    eq(tok({ exdivDays: 0 }), 'tguard: would hold CRC.US(earn 0d) EFC.US(earn 2d)', 'days 0: tomorrow is outside');
    eq(tok({ exdivDays: 1 }), 'tguard: would hold CRC.US(earn 0d) EFC.US(earn 2d) INDV.US(ex-div 1d 22.4%)', 'days 1: inside');
    eq(tok({ exdivPct: 0 }), 'tguard: would hold CRC.US(earn 0d) GLPI.US(ex-div 2d 1.7%) EFC.US(earn 2d) INDV.US(ex-div 1d 22.4%)', 'pct 0: every dividend in the window');
    eq(tok({ exdivDays: 10, exdivPct: 0 }), 'tguard: would hold ITRN.US(ex-div 10d 1.3%) CRC.US(earn 0d) GLPI.US(ex-div 2d 1.7%) EFC.US(earn 2d) INDV.US(ex-div 1d 22.4%)', 'days 10 adds ITRN; DDI (past) never');
    eq(tok({ exdivDays: 30, exdivPct: 0, earnDays: 0 }), 'tguard: would hold ITRN.US(ex-div 10d 1.3%) CRC.US(earn 0d) GLPI.US(ex-div 2d 1.7%) EFC.US(ex-div 0d 1.0%) INDV.US(ex-div 1d 22.4%)', 'earnings 0: EFC (reports in 2d) is held by its ex-date today');
    // the REAL refresh
    const r = refresh(newSource, { [GUARD]: 'observe', [KNOB]: '3' });
    const t = 'tguard: would hold CRC.US(earn 0d) EFC.US(earn 2d) INDV.US(ex-div 1d 22.4%)';
    ok(r.status.indexOf(' | ' + t + ' | ') > 0, 'B2 token: ' + r.status);
    eq(stripTok(r.status, t), getOffRefresh().status, 'B2 = the off line + the token');
    deq(noB2(r.calls), noB2(getOffRefresh().calls), 'every other write identical');
    deq(runInfoOff(r.selLog), runInfoOff(getOffRefresh().selLog), '_Selection_Log rows apart from Run Info');
    deq(r.posted, getOffRefresh().posted, 'POST bodies identical'); deq(r.saved, getOffRefresh().saved, 'state blob identical');
    ok(r.logs.some(l => l.indexOf('ticket guard (observe): would hold 3 of 6 executable ticket(s): CRC.US(earn 0d) EFC.US(earn 2d) INDV.US(ex-div 1d 22.4%) | earnings <= 3d, owner rule off, cap none, ex-div <= 3d >= 2%') > 0), 'Logger line');
    deq([r.propReads.byKey[KNOB], r.propReads.byKey[PCTK]], [1, 1], 'each ex-div knob read once');
    eq(divReadsOf(r), 1, 'one tab read');
    ok(refresh(newSource, { [GUARD]: 'observe', [KNOB]: '3', [PCTK]: '25' }).status.indexOf(' | tguard: would hold CRC.US(earn 0d) EFC.US(earn 2d) | ') > 0, 'pct 25 via the property: INDV passes');
  });

  test('E3 enforce (days 3): INDV.US takes the research path exactly like an earnings hold; CRC / EFC are the v1.20.0 holds; KPIs recomputed; banner + 3 HELD BY GUARD; survivors byte-identical; REAL refresh', () => {
    const e = render(newSource, { tguard: XD('enforce') });
    const g = render(newSource, { tguard: guardCfg('enforce') });                    // the v1.20.0 guard (earnings only)
    const ctl = render(newSource, { unfunded: ['CRC.US', 'EFC.US', 'INDV.US'] });     // the existing path: the replay left all three unfunded
    eq(e.token, 'tguard: held 3 (CRC.US earn 0d, EFC.US earn 2d, INDV.US ex-div 1d 22.4%)', 'token');
    eq(execOf(e.ctx, e.payload), 3, '3 tickets remain');
    const hr = seatCells(e.snap, 'INDV.US'), cr = seatCells(ctl.snap, 'INDV.US');
    deq(hr.map((v, i) => i === 22 || i === 29 ? null : v), cr.map((v, i) => i === 22 || i === 29 ? null : v), 'INDV = the research row');
    eq(hr[22], 'Held — ' + WHY_INDV, 'INDV Funds From'); eq(cr[22], 'Research — no allocation', 'control Funds From');
    eq(hr[29], '[held: ' + WHY_INDV + '] ' + cr[29], 'INDV note = the prefix + the research note');
    deq([hr[9], hr[10], hr[11], hr[12], hr[13], hr[14], hr[18]], [DASH, 0, 0, DASH, DASH, DASH, 0], 'INDV Entry / Ticket / Shares / Stop / TP1 / TP2 / Gain');
    for (const sym of ['CRC.US', 'EFC.US']) deq(seatCells(e.snap, sym), seatCells(g.snap, sym), sym + ' = the v1.20.0 earnings hold');
    const rowsOf = sym => [rowOf(e.snap, sym) + ':23', rowOf(e.snap, sym) + ':30'];
    deq(diffCells(e.snap, ctl.snap), ['18:1'].concat(rowsOf('CRC.US'), rowsOf('EFC.US'), rowsOf('INDV.US')).sort(), 'enforce == the research-path control, apart from the banner and the six documented cells');
    deq(e.payload.kpis, ctl.payload.kpis, 'KPIs recomputed exactly as the existing path does');
    const k = kpiRow(e.snap), k0 = kpiRow(getOff().snap);
    eq(k[0], k0[0], 'Entered cash unchanged');
    eq(k[2], '3 exec + 4 research + 3 grace / 10', 'Selected (held = research)');
    eq(k[7] - k0[7], kpiRow(ctl.snap)[7] - k0[7], 'Unallocated = the control');
    ok(Math.abs((k[7] - k0[7]) - (8527.4 + 11467.4 + 12335.4)) <= 1.5, 'Unallocated gains the held SAR: ' + (k[7] - k0[7]));
    eq(k[8], 3, 'Fundable Now');
    eq(banner(e.snap), '✅ FEED ACTIONABLE — EXECUTABLE (verdict age 367m) — SELECTED — 3 EXECUTABLE TICKETS + 1 RESEARCH (NO ALLOCATION) + 3 GRACE-HELD (NO PLAN TODAY) + 3 HELD BY GUARD', 'banner');
    for (const t of e.payload.selected) {
      if (['CRC.US', 'EFC.US', 'INDV.US'].indexOf(t.symbol) >= 0) continue;
      deq(t, getOff().payload.selected.filter(x => x.symbol === t.symbol)[0], t.symbol + ' byte-identical to off');
    }
    eq(e.ctx.dt10TicketGuardHeldCount_(e.payload.selected), 3, 'three _tguard_held markers');
    const un = Math.round(e.payload.kpis.capital_unallocated_sar - getOff().payload.kpis.capital_unallocated_sar);
    ok(e.logs.some(l => l === '[DT10 v1.28.0] ticket guard (enforce): held 3 of 6 executable ticket(s): CRC.US(earn 0d) EFC.US(earn 2d) INDV.US(ex-div 1d 22.4%) | earnings <= 3d, owner rule off, cap none, ex-div <= 3d >= 2% | executable 6 -> 3, unallocated +' + un + ' SAR'), 'Logger line');
    // the REAL refresh
    const r = refresh(newSource, { [GUARD]: 'enforce', [KNOB]: '3' });
    ok(/\| output: EXECUTABLE \|/.test(r.status) && /funding=final\/3exec/.test(r.status), 'B2 output / funding: ' + r.status);
    ok(r.status.indexOf(' | tguard: held 3 (CRC.US earn 0d, EFC.US earn 2d, INDV.US ex-div 1d 22.4%) | ') > 0, 'B2 token: ' + r.status);
    ok(/3 EXECUTABLE TICKETS \+ 1 RESEARCH \(NO ALLOCATION\) \+ 3 GRACE-HELD \(NO PLAN TODAY\) \+ 3 HELD BY GUARD$/.test(r.banner), 'banner: ' + r.banner);
    const row = seatRow(r, 'INDV.US');
    deq([row[10], row[11], row[12], row[13], row[22]], [0, 0, DASH, DASH, 'Held — ' + WHY_INDV], 'INDV seat row: ' + JSON.stringify(row));
    ok(String(row[29]).indexOf('[held: ' + WHY_INDV + '] ') === 0, 'INDV note: ' + row[29]);
    const lr = [].concat(...r.selLog).filter(x => x[4] === 'INDV.US')[0];
    deq([lr[13], lr[14], lr[25]], [0, 0, 'Held — ' + WHY_INDV], 'INDV _Selection_Log row (Ticket, Shares, Funds From)');
    const rg = refresh(newSource, { [GUARD]: 'enforce' });                         // the same refresh without the knob
    for (const s of ['CRC.US', 'EFC.US']) deq(seatRow(r, s), seatRow(rg, s), s + ' row = the v1.20.0 hold');
    for (const s of ['ITRN.US', 'DDI.US', 'GLPI.US', 'OLP.US', 'ZTO.US']) deq(seatRow(r, s), seatRow(getOffRefresh(), s), s + ' row identical');
    eq(r.kpis[8], 3, 'Fundable Now 3');
    deq(r.posted, getOffRefresh().posted, 'no new POST (no re-sizing)'); deq(r.saved, getOffRefresh().saved, 'state blob identical');
    ok(!/SEAT-CHECK/.test(r.status), 'funded KPI and board agree (no SEAT-CHECK)');
  });

  test('E4 the day window: a Riyadh calendar day; the ex-date itself = 0d; a past date never; a Date cell; the stored Days To ExDiv column is not trusted', () => {
    const ctx = N;
    const grid = divGrid('2026-11-02', [['INDV.US', 1, 8.13]]);
    const at = ms => plain(ctx.dt10ExDivMapFromValues_(grid, new Date(ms)));
    deq(at(Date.UTC(2026, 10, 2, 20, 59, 59)), { 'INDV.US': { days: 1, amount: 8.13, currency: 'USD' } }, '2 Nov 23:59:59 Riyadh: 1d');
    deq(at(Date.UTC(2026, 10, 2, 21, 0, 1)), { 'INDV.US': { days: 0, amount: 8.13, currency: 'USD' } }, '3 Nov 00:00:01 Riyadh: 0d (the ex-day)');
    deq(at(Date.UTC(2026, 10, 3, 21, 0, 1)), {}, '4 Nov Riyadh: past -> skipped');
    deq(at(Date.UTC(2026, 9, 30, 6, 0, 0)), { 'INDV.US': { days: 4, amount: 8.13, currency: 'USD' } }, '30 Oct: 4d');
    const stale = plain(grid); stale[1][2] = 9;   // the lane's Days To ExDiv (written at sync time) is ignored
    deq(plain(ctx.dt10ExDivMapFromValues_(stale, '2026-11-02')), { 'INDV.US': { days: 1, amount: 8.13, currency: 'USD' } }, 'days from the Ex-Div Date, not the stored column');
    const viaDate = ctx.dt10ExDivMapFromValues_([grid[0], ['INDV.US', new Date(Date.UTC(2026, 10, 2, 21, 0, 0)), 1, 8.13, 'USD']], '2026-11-02');   // a sheet Date: 3 Nov 00:00 Riyadh
    eq(viaDate['INDV.US'].days, 1, 'a Date cell (midnight Riyadh) reads its Riyadh day');
    const t = { symbol: 'INDV.US', price: 36.28, currency: 'USD' };
    const m = ctx.dt10ExDivMapFromValues_(grid, '2026-11-02');
    eq(ctx.dt10ExDivHold_(t, m, 0, 2), null, 'window 0: 1d outside');
    eq(ctx.dt10ExDivHold_(t, m, 1, 2).tag, 'ex-div 1d 22.4%', 'window 1: inside');
    eq(ctx.dt10ExDivHold_(t, m, 30, 2).tag, 'ex-div 1d 22.4%', 'window 30: inside');
    eq(ctx.dt10ExDivHold_(t, { 'INDV.US': { days: -1, amount: 8.13, currency: 'USD' } }, 30, 2), null, 'a negative day never holds');
    eq(ctx.dt10ExDivHold_(t, { 'INDV.US': { days: NaN, amount: 8.13, currency: 'USD' } }, 30, 2), null, 'NaN never holds');
    eq(ctx.dt10ExDivHold_(t, {}, 30, 2), null, 'no row');
    eq(ctx.dt10ExDivHold_(t, null, 30, 2), null, 'no map');
    eq(ctx.dt10ExDivHold_({ symbol: ' indv.us ', price: 36.28, currency: 'USD' }, m, 3, 2).tag, 'ex-div 1d 22.4%', 'symbol normalised');
  });

  test('E5 size unknown and units: no amount, another currency, MIXED, no price -> held "size unknown"; GBp / GBX / ZAc subunits measured only against a subunit label; a render-level size-unknown hold', () => {
    const ctx = N;
    const H = (px, ccy, amt, label) => ctx.dt10ExDivHold_({ symbol: 'X.L', price: px, currency: ccy }, { 'X.L': { days: 2, amount: amt, currency: label } }, 3, 2);
    deq(plain(H(36.28, 'USD', 8.13, 'USD')), { kind: 'exdiv', reason: 'ex-dividend in 2d (22.4% of price)', tag: 'ex-div 2d 22.4%' }, 'USD on USD');
    eq(H(36.28, 'USD', 0.5, 'USD'), null, 'USD 1.4 % < 2 %: no hold');
    for (const [px, ccy, amt, label, why] of [[36.28, 'USD', null, 'USD', 'no amount'], [36.28, 'USD', 8.13, 'EUR', 'another currency'],
      [36.28, 'USD', 8.13, 'MIXED', 'MIXED (the lane)'], [36.28, 'USD', 8.13, '', 'no currency'], ['', 'USD', 8.13, 'USD', 'no price'],
      [0, 'USD', 8.13, 'USD', 'zero price'], [DASH, 'USD', 8.13, 'USD', 'em-dash price'], [36.28, '', 8.13, 'USD', 'no ticket currency'],
      [152.2, 'GBp', 0.0385, 'GBP', 'GBP label on a GBp price (pounds, or the lane upper-cased GBp)'], [152.2, 'GBX', 3.85, 'GBP', 'GBP label on a GBX price'],
      [1000, 'ZAc', 0.5, 'ZAR', 'ZAR label on a ZAc price'], [152.2, 'GBP', 3.85, 'GBX', 'GBX label on a GBP (pounds) price']]) {
      deq(plain(H(px, ccy, amt, label)), { kind: 'exdiv', reason: 'ex-dividend in 2d (size unknown)', tag: 'ex-div 2d ?' }, why);
    }
    eq(H(152.2, 'GBp', 3.85, 'GBX').tag, 'ex-div 2d 2.5%', 'GBX on a GBp price: same unit (pence)');
    eq(H(152.2, 'GBX', 3.85, 'GBX').tag, 'ex-div 2d 2.5%', 'GBX on a GBX price');
    eq(H(152.2, 'GBp', 1.0, 'GBX'), null, 'GBX 0.7 % on a GBp price: no hold');
    eq(H(1000, 'ZAc', 50, 'ZAC').tag, 'ex-div 2d 5.0%', 'ZAC on a ZAc price');
    eq(H(36.28, 'usd', 8.13, 'USD').tag, 'ex-div 2d 22.4%', 'ticket currency case-folded');
    eq(H('36.28', 'USD', 8.13, 'USD').tag, 'ex-div 2d 22.4%', 'a numeric-string price');
    // the default threshold when pct is not a number
    eq(ctx.dt10ExDivHold_({ symbol: 'A', price: 100, currency: 'USD' }, { A: { days: 1, amount: 1.9, currency: 'USD' } }, 3, undefined), null, 'default 2 %: 1.9 passes');
    eq(ctx.dt10ExDivHold_({ symbol: 'A', price: 100, currency: 'USD' }, { A: { days: 1, amount: 2.0, currency: 'USD' } }, 3, NaN).tag, 'ex-div 1d 2.0%', 'default 2 %: 2.0 holds');
    // review fix (Codex P2 on PR #906): the threshold is compared unrounded; the tag is rounded
    eq(ctx.dt10ExDivHold_({ symbol: 'A', price: 100, currency: 'USD' }, { A: { days: 1, amount: 1.96, currency: 'USD' } }, 3, 2), null, '1.96 % at 2 %: no hold (it rounds to 2.0)');
    eq(ctx.dt10ExDivHold_({ symbol: 'A', price: 100, currency: 'USD' }, { A: { days: 1, amount: 2.04, currency: 'USD' } }, 3, 2.03).tag, 'ex-div 1d 2.0%', '2.04 % at 2.03 %: held (it rounds to 2.0)');
    eq(ctx.dt10ExDivHold_({ symbol: 'A', price: 100, currency: 'USD' }, { A: { days: 1, amount: 2.02, currency: 'USD' } }, 3, 2.03), null, '2.02 % at 2.03 %: no hold');
    // render level: INDV with a blank Amount is held as size unknown, through the same research path
    const rows = DIVS.map(r => r[0] === 'INDV.US' ? ['INDV.US', 1, ''] : r);
    const e = render(newSource, { tguard: XD('enforce'), divGrid: divGrid('2026-11-02', rows) });
    eq(e.token, 'tguard: held 3 (CRC.US earn 0d, EFC.US earn 2d, INDV.US ex-div 1d ?)', 'token');
    eq(seatCells(e.snap, 'INDV.US')[22], 'Held — ex-dividend in 1d (size unknown)', 'Funds From');
    ok(String(seatCells(e.snap, 'INDV.US')[29]).indexOf('[held: ex-dividend in 1d (size unknown)] ') === 0, 'note prefix');
  });

  test('E6 precedence and monotonicity: earnings first; ex-div before the owner rule and the cap; non-executable seats never checked; every combination only adds holds to the v1.20.0 set; a withheld feed holds nothing', () => {
    const tok = cfg => render(newSource, { tguard: cfg }).token;
    eq(tok(XD('observe', { ownerRule: true })), 'tguard: would hold ITRN.US(owner rule) DDI.US(owner rule) CRC.US(earn 0d) EFC.US(earn 2d) INDV.US(ex-div 1d 22.4%)', 'owner rule: INDV is held by its ex-date, not the rule');
    eq(tok(XD('observe', { maxTickets: 2 })), 'tguard: would hold CRC.US(earn 0d) GLPI.US(cap 2) EFC.US(earn 2d) INDV.US(ex-div 1d 22.4%)', 'cap 2: INDV held by its ex-date; the cap counts survivors');
    eq(tok(XD('observe', { maxTickets: 1, exdivPct: 1.5 })), 'tguard: would hold DDI.US(cap 1) CRC.US(earn 0d) GLPI.US(ex-div 2d 1.7%) EFC.US(earn 2d) INDV.US(ex-div 1d 22.4%)', 'cap 1 after the ex-div filter');
    const m = N.dt10ExDivMapFromValues_(divGrid('2026-11-02'), '2026-11-02');
    ok(!/OLP\.US|ZTO\.US/.test(tok(XD('observe', { exdivDays: 30, exdivPct: 0 }))), 'the research seat (OLP 2.1 %) and the grace ghost (ZTO) are never checked');
    // pure core: earnings wins over an ex-date the same day
    const core = (sel, earn, cfg) => N.dt10TicketGuardCore_(sel, true, earn, cfg, m);
    const sel = livePayload().selected;
    const res = core(sel, { 'INDV.US': 0 }, XD('observe'));
    eq(res.held.filter(h => h.symbol === 'INDV.US')[0].kind, 'earn', 'INDV reporting today: earnings first');
    // monotone: for every knob combination the held set contains the v1.20.0 held set, and nothing is ever added back
    let combos = 0;
    for (const earnDays of [0, 3, 9]) for (const ownerRule of [false, true]) for (const maxTickets of [0, 1, 3]) {
      const base = core(sel, { 'CRC.US': 0, 'EFC.US': 2, 'OLP.US': 3, 'DDI.US': 9, 'ITRN.US': 22 }, { earnDays, ownerRule, maxTickets });
      const baseHeld = base.held.map(h => h.symbol);
      for (const exdivDays of [0, 1, 3, 30]) for (const exdivPct of [0, 2, 25, 100]) {
        const r = core(sel, { 'CRC.US': 0, 'EFC.US': 2, 'OLP.US': 3, 'DDI.US': 9, 'ITRN.US': 22 }, { earnDays, ownerRule, maxTickets, exdivDays, exdivPct });
        const held = r.held.map(h => h.symbol);
        eq(r.checked, base.checked, 'checked');
        ok(r.kept <= base.kept, 'kept never increases ' + JSON.stringify([earnDays, ownerRule, maxTickets, exdivDays, exdivPct]));
        for (const s of baseHeld.filter(x => held.indexOf(x) < 0)) {
          // a ticket the v1.20.0 cap held may be released only because an ex-div hold took its place among the survivors
          ok(maxTickets > 0 && base.held.filter(h => h.symbol === s)[0].kind === 'cap', 'only a cap hold can move: ' + s);
        }
        eq(held.length >= baseHeld.length, true, 'never fewer holds');
        combos++;
      }
    }
    eq(combos, 3 * 2 * 3 * 4 * 4, 'combinations');
    // a withheld feed: nothing held, nothing read beyond the one tab read
    const w = render(newSource, { state: 'NOT_ACTIONABLE', tguard: XD('enforce', { exdivDays: 30, exdivPct: 0 }) });
    eq(w.token, 'tguard: held 0', 'withheld feed: held 0');
    deq(w.snap, render(newSource, { state: 'NOT_ACTIONABLE' }).snap, 'withheld feed: every cell identical to off');
    eq(N.dt10TicketGuardCore_(sel, false, {}, XD('enforce', { exdivDays: 30, exdivPct: 0 }), m).held.length, 0, 'core: feedOk false');
  });

  test('E7 the map builder and the tab read: header search, column order, duplicates, amounts, dates, bounds, absent tab, faults', () => {
    const f = (vals, asOf) => plain(N.dt10ExDivMapFromValues_(vals, asOf === undefined ? '2026-11-02' : asOf));
    deq(f(divGrid('2026-11-02')), { 'INDV.US': { days: 1, amount: 8.13, currency: 'USD' }, 'GLPI.US': { days: 2, amount: 0.78, currency: 'USD' },
      'CRC.US': { days: 1, amount: 0.405, currency: 'USD' }, 'EFC.US': { days: 0, amount: 0.13, currency: 'USD' }, 'ITRN.US': { days: 10, amount: 0.5, currency: 'USD' },
      'OLP.US': { days: 1, amount: 0.45, currency: 'USD' }, 'ZTO.US': { days: 2, amount: 0.35, currency: 'USD' } }, 'the fixture tab (DDI past)');
    deq(f([['Dividend events'], [''], ['Currency', 'Amount', 'Symbol', 'Ex-Div Date'], ['usd', '8.13', ' indv.us ', '2026-11-03']]),
        { 'INDV.US': { days: 1, amount: 8.13, currency: 'USD' } }, 'header on row 3, any column order, string amount, case');
    deq(f([['Symbol', 'Ex-Div Date'], ['INDV.US', '2026-11-03']]), { 'INDV.US': { days: 1, amount: null, currency: '' } }, 'no Amount / Currency columns');
    deq(f([['Symbol', 'Ex-Div Date', 'Amount', 'Currency'], ['A', '2026-11-03', 1, 'USD'], ['A', '2026-11-04', 9, 'USD'], ['Symbol', 'Ex-Div Date', 'Amount', 'Currency'],
          ['', '2026-11-03', 1, 'USD'], ['B', '2026-13-01', 1, 'USD'], ['C', '03/11/2026', 1, 'USD'], ['D', 46000, 1, 'USD'], ['E', '', 1, 'USD'],
          ['F', '2026-11-03', -1, 'USD'], ['G', '2026-11-03', 'abc', 'USD'], ['H', '2026-11-03', ' 2.5 ', 'USD'], ['I', '2026-11-03', NaN, 'USD'], ['J', '2026-11-03', '', 'USD']]),
        { A: { days: 1, amount: 1, currency: 'USD' }, F: { days: 1, amount: null, currency: 'USD' }, G: { days: 1, amount: null, currency: 'USD' },
          H: { days: 1, amount: 2.5, currency: 'USD' }, I: { days: 1, amount: null, currency: 'USD' }, J: { days: 1, amount: null, currency: 'USD' } },
        'first row wins; repeated header, blank symbol and bad dates skipped; bad amounts -> null');
    deq(f([]), {}, 'empty'); deq(f(null), {}, 'null'); deq(f([['Ticker', 'Date'], ['A', '2026-11-03']]), {}, 'no header');
    deq(f([['Symbol', 'Next Ex-Div Date'], ['A', '2026-11-03']]), {}, 'the Calendar_Events header is not the Dividend_Events header');
    deq(f(divGrid('2026-11-02'), 'junk'), {}, 'an invalid as-of day -> {}');
    // the tab read: bounds and faults (never throws)
    const sheetOf = (rows, cols, vals, throwIt) => ({ getSheetByName(n) { return n === 'Dividend_Events' ? {
      getLastRow() { return rows; }, getLastColumn() { return cols; },
      getRange(r, c, nr, nc) { return { getValues() { if (throwIt) throw new Error('read fault'); return vals.slice(0, nr).map(x => x.slice(0, nc)); } }; } } : null; } });
    const g = divGrid('2026-11-02');
    eq(Object.keys(plain(N.dt10ExDivMap_(sheetOf(g.length, 10, g), '2026-11-02'))).length, 7, 'the live read');
    eq(Object.keys(plain(N.dt10ExDivMap_(sheetOf(g.length, 14, g.map(r => r.concat(['x', 'y', 'z', 'w']))), '2026-11-02'))).length, 7, 'columns beyond J are not read');
    deq(plain(N.dt10ExDivMap_({ getSheetByName() { return null; } }, '2026-11-02')), {}, 'absent tab');
    deq(plain(N.dt10ExDivMap_(null, '2026-11-02')), {}, 'no spreadsheet');
    deq(plain(N.dt10ExDivMap_(sheetOf(1, 10, g), '2026-11-02')), {}, 'header only');
    deq(plain(N.dt10ExDivMap_(sheetOf(5002, 10, g), '2026-11-02')), {}, 'oversized (> 5000 body rows)');
    eq(Object.keys(plain(N.dt10ExDivMap_(sheetOf(5001, 10, g), '2026-11-02'))).length, 7, '5000 body rows: read');
    deq(plain(N.dt10ExDivMap_(sheetOf(g.length, 10, g, true), '2026-11-02')), {}, 'a read fault');
    deq(plain(N.dt10ExDivMap_({ getSheetByName() { throw new Error('ss fault'); } }, '2026-11-02')), {}, 'a spreadsheet fault');
  });

  test('E8 a Dividend_Events read failure (or no tab): no ex-div hold, the earnings holds stay, the Logger line keeps its suffix, no FAILED line', () => {
    for (const opts of [{ divThrow: true }, { noDiv: true }]) {
      const e = render(newSource, Object.assign({ tguard: XD('enforce') }, opts));
      eq(e.token, 'tguard: held 2 (CRC.US earn 0d, EFC.US earn 2d)', 'token ' + JSON.stringify(opts));
      deq(e.snap, render(newSource, Object.assign({ tguard: guardCfg('enforce') }, opts)).snap, 'the v1.20.0 enforce board ' + JSON.stringify(opts));
      ok(!e.logs.some(l => /FAILED/.test(l)), 'no FAILED line');
      ok(e.logs.some(l => /ticket guard \(enforce\): held 2 of 6 .*, ex-div <= 3d >= 2% \| executable 6 -> 4/.test(l)), 'Logger line');
    }
    const r = refresh(newSource, { [GUARD]: 'enforce', [KNOB]: '3' }, { noDiv: true });
    eq(r.status, refresh(newSource, { [GUARD]: 'enforce' }).status, 'REAL refresh without the tab: B2 = the v1.20.0 enforce B2');
    deq(noB2(r.calls), noB2(refresh(newSource, { [GUARD]: 'enforce' }).calls), 'REAL refresh without the tab: every write = the v1.20.0 enforce');
  });

  const vAll = s => String(s).replace(/1\.2(?:1\.[01]|2\.0|3\.[01]|4\.0|5\.0|6\.0|7\.0|8\.0)/g, '1.20.0');   // 2026-11-05: delivered v1.21.1, rebuilt v1.21.0; 2026-11-08: delivered v1.22.0
  // 2026-11-08: v1.22.0 adds two self-test lines ('confirm window (TFB_T10_CONFIRM_WINDOW): ...' and 'confirm window
  // core: ...'); they have their own harness, so the v1.20.0 report comparisons read the delivered report without them.
  const drop1220 = s => String(s).split('\n').filter(l => !/^(?:confirm window|reentry cooldown|hard-exit confirm|seat guard|owner rule bands|fundable seats|render timing) /.test(l)).join('\n');   // 2026-11-08: + the two v1.23.0 lines; + the four v1.24.0 lines; 2026-11-09: + the two v1.28.0 lines
  test('E9 embedded dt10SelfTest: the v1.20.0 report while the knob is unset (or the guard off); one core-ok line while it is set and the guard is armed; a fault reads off', () => {
    const mini = (src, props, throwIt) => {
      const statusRows = []; for (let r = 0; r < 60; r++) statusRows.push(['', '']);
      const ss = { getSheetByName(n) { return n === '_Status' ? { getRange() { return { getValues() { return plain(statusRows); } }; } } : null; }, getRangeByName() { return null; } };
      const ctx = { Logger: { log() {} }, SpreadsheetApp: { getActiveSpreadsheet() { return ss; }, getUi() { throw new Error('no ui'); } },
        PropertiesService: { getScriptProperties() { if (throwIt) throw new Error('down'); return { getProperty(k) { return Object.prototype.hasOwnProperty.call(props, k) ? props[k] : null; }, setProperty() {}, deleteProperty() {} }; } },
        Utilities: { formatDate() { return '2026-11-02 00:00:00'; } }, Session: { getScriptTimeZone() { return 'Asia/Riyadh'; } }, UrlFetchApp: { fetch() { throw new Error('no net'); } } };
      vm.createContext(ctx); vm.runInContext(src, ctx, { filename: '16_Decision_Top10.gs' });
      return String(ctx.dt10SelfTest());
    };
    for (const props of [{}, { [GUARD]: 'observe' }, { [GUARD]: 'enforce', [KNOB]: 'abc' }, { [KNOB]: '3' }, { [GUARD]: 'off', [KNOB]: '3', [PCTK]: '1' }]) {
      eq(vAll(drop1220(mini(newSource, props))), mini(baseSource, props), 'the v1.20.0 report ' + JSON.stringify(props));
    }
    const rep = mini(newSource, { [GUARD]: 'observe', [KNOB]: '3' });
    ok(rep.indexOf('[DT10 v1.28.0] self-test') === 0, 'version line');
    const lines = drop1220(rep).split('\n'), baseLines = mini(baseSource, { [GUARD]: 'observe', [KNOB]: '3' }).split('\n');
    eq(lines.length, baseLines.length + 1, 'exactly one line added');
    const want = 'ticket guard ex-div (DT10_GUARD_EXDIV_DAYS): <= 3d, >= 2% of price, tab Dividend_Events | core: ok (INDV 22.4% held, FRST 0.7% passes at 2% and holds at 0.5%, GBP size unknown, GBX on a GBp price measured, earnings first)';
    const at = lines.indexOf(want);
    ok(at > 0, 'the line: ' + lines.filter(l => /ex-div/.test(l)).join(' / '));
    ok(/^ticket guard \(DT10_TICKET_GUARD\): observe /.test(lines[at - 1]), 'right after the guard line');
    deq(lines.filter((l, i) => i !== at).map(vAll), baseLines, 'every other line identical');
    ok(rep.indexOf('FAIL') < 0, 'no FAIL');
    const p05 = mini(newSource, { [GUARD]: 'enforce', [KNOB]: '7', [PCTK]: '0.5' });
    ok(p05.indexOf('ticket guard ex-div (DT10_GUARD_EXDIV_DAYS): <= 7d, >= 0.5% of price, tab Dividend_Events | core: ok (') > 0 && p05.indexOf('FAIL') < 0, 'the knobs are reported');
    eq(vAll(drop1220(mini(newSource, { [GUARD]: 'enforce', [KNOB]: '3' }, true))), mini(baseSource, { [GUARD]: 'enforce', [KNOB]: '3' }, true), 'a property fault: the v1.20.0 report');
  });

  test('E10 ES5 / ASCII lint of the v1.21.0 diff; inventory: 3 functions and 6 vars added, 0 removed; 13 hunks whose reversal is the reviewed v1.20.0 file; WHY block; every prior WHY verbatim', () => {
    const newSource = S1210;   // 2026-11-05: the rebuilt v1.21.0 file (see S1210); the v1.21.1 diff has its own harness
    const baseLines = {}, bl = baseSource.split('\n'), nl = newSource.split('\n');
    for (const l of bl) baseLines[l] = true;
    const added = nl.filter(l => !baseLines[l]);
    ok(added.length >= 150 && added.length <= 260, 'added lines ' + added.length);
    for (const l of added) {
      const code = l.replace(/\/\/.*$/, '');
      ok(!/\b(let|const|class)\s|=>|`|\.\.\.|\.includes\(/.test(code) || /^\s*(\*|\/\*)/.test(l), 'ES6 token: ' + l);
      if (!/^[\x09\x20-\x7e]*$/.test(l)) ok(baseLines[l.replace('1.21.0', '1.20.0')] === true, 'non-ASCII added line: ' + l);
    }
    ok(!added.some(l => /^\s+function\s+[A-Za-z_$]/.test(l) && !/^\s*(\*|\/\/)/.test(l)), 'no nested function declaration added (ES5 blocks)');
    function nonAscii(s) { const m = {}; for (let j = 0; j < s.length; j++) { const c = s.charCodeAt(j); if (c > 126) m[c] = (m[c] || 0) + 1; } return m; }
    deq(nonAscii(newSource), nonAscii(baseSource), 'non-ASCII code point multiset unchanged');
    ok(newSource.indexOf('\r') < 0, 'LF only');
    const why = newSource.slice(newSource.indexOf(' * v1.21.0 (2026-11-03) [EXDIV-GUARD]'), newSource.indexOf(' * v1.20.0 (2026-11-02) [TICKET-GUARD'));
    ok(why.length > 2500 && /^[\x09\x0a\x20-\x7e]*$/.test(why), 'WHY block present (newest first) and ASCII');
    for (const w of [' * WHY (3 Nov 2026)', ' * WHAT: ', ' * GATES: ', ' * ROLLBACK: ', ' * Harness: tests/test_dt10_v1210_exdiv_guard.js']) ok(why.indexOf(w) >= 0, 'WHY section ' + w);
    ok(newSource.indexOf(baseSource.slice(baseSource.indexOf(' * v1.20.0 (2026-11-02) [TICKET-GUARD'), baseSource.indexOf(' * v1.19.0 (2026-11-01) [QUALIFIED-HOLD'))) > 0, 'the v1.20.0 WHY block verbatim');
    for (const l of bl.filter(x => /^ \* v\d+\.\d+\.\d+ \(/.test(x))) ok(newSource.indexOf(l) > 0, 'WHY header lost: ' + l);
    const names = (src, re) => { const out = {}; let m; while ((m = re.exec(src))) out[m[1]] = true; return out; };
    const fb = names(baseSource, /function\s+([A-Za-z_$][\w$]*)\s*\(/g), fn = names(newSource, /function\s+([A-Za-z_$][\w$]*)\s*\(/g);
    deq(Object.keys(fb).filter(k => !fn[k]), [], 'functions removed (any level)');
    const tb = names(baseSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), tn = names(newSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    const FNS = ['dt10ExDivHold_', 'dt10ExDivMapFromValues_', 'dt10ExDivMap_'];
    deq(Object.keys(tn).filter(k => !tb[k]).sort(), FNS, 'top-level functions added');
    eq(Object.keys(tb).length, 250, 'base census'); eq(Object.keys(tn).length, 253, 'new census');
    const vb = names(baseSource, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = names(newSource, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq(Object.keys(vb).filter(k => !vn[k]), [], 'vars removed');
    deq(Object.keys(vn).filter(k => !vb[k]).sort(), ['DT10_DIVIDEND_SHEET', 'DT10_GUARD_EXDIV_DAYS_MAX', 'DT10_GUARD_EXDIV_DAYS_PROP', 'DT10_GUARD_EXDIV_PCT_DEF',
      'DT10_GUARD_EXDIV_PCT_MAX', 'DT10_GUARD_EXDIV_PCT_PROP'], 'vars added');
    eq(Object.keys(vb).length, 140); eq(Object.keys(vn).length, 146);
    const newSet = {}; for (const l of nl) newSet[l] = true;
    // the two version lines and the five edited lines of the guard (config object, core doc + signature, the
    // core call and the Logger line) -- nothing else of the base is touched
    deq(bl.filter(l => !newSet[l]).map(l => l.trim()).sort(), ['* Version: 1.20.0 (see DT10_VERSION; header kept in lockstep — restored', "var DT10_VERSION = '1.20.0';",
      'return { mode: mode,', "* the v1.6.0 tagger's Calendar_Events {SYMBOL: days to earnings}; (2) owner", 'function dt10TicketGuardCore_(selected, feedOk, map, cfg) {',
      'var res = dt10TicketGuardCore_(payload.selected || [], feedOk, map, tg);', "(tg.ownerRule === true ? 'on' : 'off') + ', cap ' + (cap > 0 ? cap : 'none') + tail);"].sort(), 'only these seven base lines replaced');
    for (const name of FNS) {
      const start = newSource.indexOf('function ' + name + '(');
      const body = newSource.slice(start, newSource.indexOf('\n}\n', start) + 3);
      ok(start > 0 && /^[\x00-\x7f]*$/.test(body), 'ASCII ' + name);
      new vm.Script('(function(){' + body + '})');
    }
    const edits = require(path.join(ROOT, 'tests', 'js', 'dt10_edits_v1210.json'));
    eq(edits.length, 13, 'thirteen hunks');
    eq(sha256(history.v1200Source(newSource)), history.V1200_SHA256, 'reversal = the reviewed v1.20.0 file');
    // the filter sits inside the existing guard: after earnings, before the owner rule; no new row shape
    const core = newSource.slice(newSource.indexOf('function dt10TicketGuardCore_('), newSource.indexOf('\n}\n', newSource.indexOf('function dt10TicketGuardCore_(')));
    const iE = core.indexOf("kind: 'earn'"), iX = core.indexOf('dt10ExDivHold_(t, divMap'), iR = core.indexOf("kind: 'rule'"), iC = core.indexOf("kind: 'cap'");
    ok(iE > 0 && iE < iX && iX < iR && iR < iC, 'filter order earnings < ex-div < owner rule < cap');
    ok(core.indexOf("typeof cfg.exdivDays === 'number'") > 0, 'the filter runs only with the days knob');
    const guard = newSource.slice(newSource.indexOf('function dt10TicketGuard_('), newSource.indexOf('\n}\n', newSource.indexOf('function dt10TicketGuard_(')));
    ok(guard.indexOf("typeof tg.exdivDays === 'number' ? dt10ExDivMap_(ss) : null") > 0, 'the tab is read only with the days knob');
  });

  test('E11 service reads: days / pct spellings; reads only while armed; the pct knob only while the days knob is set; faults read off', () => {
    const mk = (props, fault) => loadRender(newSource, props, { propsThrow: fault });
    const mkB = props => loadRender(baseSource, props, {});
    for (const [v, want] of [['3', 3], [' 3 ', 3], ['03', 3], ['0', 0], ['30', 30], ['7', 7], ['00', 0]]) {
      const c = plain(mk({ [GUARD]: 'observe', [KNOB]: v }).dt10TicketGuardConfig_());
      eq(c.exdivDays, want, 'days ' + JSON.stringify(v)); eq(c.exdivPct, 2, 'pct default with days ' + JSON.stringify(v));
    }
    for (const v of ['31', '99', '100', '-1', '1.5', 'abc', '', ' ', '3d', '+3', '1e1', 'three']) {
      const c = plain(mk({ [GUARD]: 'enforce', [KNOB]: v, [PCTK]: '5' }).dt10TicketGuardConfig_());
      deq(c, plain(mkB({ [GUARD]: 'enforce', [KNOB]: v, [PCTK]: '5' }).dt10TicketGuardConfig_()), 'junk days ' + JSON.stringify(v) + ' = the v1.20.0 object');
      ok(!('exdivDays' in c) && !('exdivPct' in c), 'no exdiv keys for ' + JSON.stringify(v));
    }
    for (const [v, want] of [[null, 2], ['', 2], ['0', 0], ['0.5', 0.5], ['2.75', 2.75], [' 5 ', 5], ['150', 100], ['100', 100], ['-1', 2], ['x', 2], ['Infinity', 2]]) {
      const p = { [GUARD]: 'observe', [KNOB]: '3' }; if (v !== null) p[PCTK] = v;
      eq(mk(p).dt10TicketGuardConfig_().exdivPct, want, 'pct ' + JSON.stringify(v));
    }
    const off = mk({ [KNOB]: '3', [PCTK]: '1' }); eq(off.dt10TicketGuardConfig_(), null, 'off -> null'); deq(off.__propReads, [GUARD], 'off: one read');
    const a0 = mk({ [GUARD]: 'enforce' }); a0.dt10TicketGuardConfig_(); deq(a0.__propReads, [GUARD].concat(KNOBS, [KNOB, 'DT10_GUARD_CORP_DAYS']), 'armed, no days knob: + one read (+ the v1.27.0 corp-days knob)');
    const a1 = mk({ [GUARD]: 'enforce', [KNOB]: '3' }); a1.dt10TicketGuardConfig_(); deq(a1.__propReads, [GUARD].concat(KNOBS, [KNOB, PCTK, 'DT10_GUARD_CORP_DAYS']), 'armed + days: the pct knob once (+ the v1.27.0 corp-days knob)');
    const aj = mk({ [GUARD]: 'enforce', [KNOB]: 'abc', [PCTK]: '1' }); aj.dt10TicketGuardConfig_(); deq(aj.__propReads, [GUARD].concat(KNOBS, [KNOB, 'DT10_GUARD_CORP_DAYS']), 'junk days: the pct knob is not read (+ the v1.27.0 corp-days knob)');
    eq(mk({ [GUARD]: 'enforce', [KNOB]: '3' }, true).dt10TicketGuardConfig_(), null, 'fault -> off');
    deq(plain(mk({ [GUARD]: 'enforce', [KNOB]: '3' }).dt10TicketGuardConfig_()), { mode: 'enforce', earnDays: 3, ownerRule: false, maxTickets: 0, exdivDays: 3, exdivPct: 2 }, 'the armed object');
  });

  return sha256(JSON.stringify([getOff().snap, render(newSource, { tguard: XD('enforce') }).snap, render(newSource, { tguard: XD('observe', { exdivPct: 0 }) }).token,
    refresh(newSource, { [GUARD]: 'enforce', [KNOB]: '3' }).status])).slice(0, 16);
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
console.log('V1210 E0-E11 x3 identical digest ' + digests[0] + ' | new SHA256 ' + sha256(newSource) + ' | base SHA256 ' + sha256(baseSource));
console.log('PASS ' + passed + '/' + total);
