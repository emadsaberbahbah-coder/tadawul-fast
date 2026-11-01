#!/usr/bin/env node
/* tests/test_dt10_v1190_hold_live_rule.js
 * Harness for apps_script/16_Decision_Top10.gs v1.19.0 [QUALIFIED-HOLD /
 * SEAT-LIVE-FILL / OWNER-RULE] (recommendation pages review of 31 Oct 2026,
 * items R2.0 / R3.1 / R4.1): three Script Properties, each default OFF.
 *
 * Dual-tree node harness over the REAL file: loads the delivered .gs source
 * and the v1.18.0 BASE -- a file given as the second argument (it must carry
 * the pinned SHA-256 of origin/main 11af45c3) or, by default, the base
 * rebuilt from the delivered file by reversing exactly the enumerated
 * v1.19.0 edits (tests/js/dt10_edits_v1190.json, SHA-256 pinned) -- into
 * separate vm contexts with three GAS shims: the minimal one of
 * tests/test_dt10_confirm_basis.js (pure core + dt10ApplyStability_), the
 * cell-model spreadsheet of tests/test_dt10_post_earnings.js (the REAL
 * dt10RenderPayload_) and the full workbook of
 * tests/js/decision_input_truth.test.js (the REAL refreshDecisionTop10 with
 * a POST stub that answers the research call and the allocate replay).
 * Sections:
 *   T0 base oracle (SHA-256 pinned), version lockstep, both files compile
 *   T1 GOLDEN NEGATIVE / kill identity: the properties absent / off / junk /
 *      faulted -> the pure core (tickets, state, audit, note), the e2e
 *      dt10ApplyStability_ (note, selected, audit, saved blob, knobs), the
 *      REAL render (every cell, the token, the tickets, the property reads)
 *      and the REAL refresh (B2 status line, every sheet write, _Run_Log and
 *      _Selection_Log rows, Logger lines) deep-equal the v1.18.0 base
 *   T2 observe: the core, the e2e path and the REAL refresh add only the
 *      ', qhold: would hold N SYM' token (and audit.qualified_held); every
 *      ticket, clock, cell and row is the off output
 *   T3 enforce: the qualified seated member keeps 'ACTIVE (day n)' with co 0,
 *      no _grace_hold, _qhold, board-eligible; the REAL refresh names it in
 *      the replay's board_funding_symbols and the replay's sizing lands on
 *      the board (an executable ticket) -- while a member that is WATCH on the
 *      grid, a member absent from the grid and an unconfirmed fast-track seat
 *      go to GRACE exactly as before; the base never holds
 *   T4 live fill: the pure core (pool row / grid row / no row); the REAL
 *      render fills price, Rel, DQ, FX (and Price SAR) of a grace seat with a
 *      pool row, notes the investable source row, leaves a seat without a
 *      row blank, and the REAL refresh builds the index from the sheet pool
 *      (token 'live-fill k/n'); the base ignores the property
 *   T5 owner rule: yes only for High + ROI < 34, the failing part named; the
 *      REAL render appends the 'Owner Rule' column (31 cells; 33 with the
 *      vintage columns), keeps it on a withheld feed; the REAL refresh
 *      carries 'owner-rule k/n yes'; the base renders 30 columns
 *   T6 the embedded dt10SelfTest: the three new 'core: ok' lines, the
 *      property lines follow the properties, every base line kept, no FAIL
 *   T7 ES5 / ASCII lint of the diff, inventory (9 functions, 5 vars added,
 *      0 removed), 22 hunks whose reversal is the reviewed v1.18.0 file
 *   T8 service reads: spellings -> modes; a fault reads off
 * The whole battery runs three times and the three digests must match; the
 * last line is 'PASS k/k' (exit 1 on any failure).
 * Usage: node tests/test_dt10_v1190_hold_live_rule.js [new.gs] [base.gs]
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
  baseSource = BASE_PATH ? fs.readFileSync(BASE_PATH, 'utf8') : history.v1180Source(newSource);
} catch (eBase) {
  console.log('FAIL T0 base oracle: ' + String(eBase && eBase.message || eBase).split('\n')[0]);
  console.log('FAIL 0/1');
  process.exit(1);
}
const FIX = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'fixtures', 'dt10_candidates_2026-10-10.json'), 'utf8'));
const PAGES = JSON.parse(fs.readFileSync(path.join(ROOT, 'tests', 'fixtures', 'dt10_pages_2026-10-05.json'), 'utf8'));
const NOW = Date.parse(FIX.frozen_now);            // 2026-10-10T19:28:22Z
const TODAY = FIX.frozen_now.slice(0, 10);
const HOLD = 'DT10_QUALIFIED_HOLD', LIVE = 'DT10_SEAT_LIVE_FILL', RULE = 'DT10_OWNER_RULE_MARK';
const DASH = '—';

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

/* ============================================================ shim 1: minimal (pure core + dt10ApplyStability_) */
function miniCtx(props, options) {
  options = options || {};
  const store = Object.assign({}, props || {});
  const logs = [], reads = {};
  const statusRows = [];
  for (let r = 0; r < 60; r++) statusRows.push(['', '']);
  statusRows[0] = ['TFB Feed Global_Markets', 'OK | cov=99.0 | run=40079965055 | 2026-10-24 06:00:00+03:00'];
  const statusSheet = { getRange() { return { getValues() { return plain(statusRows); } }; } };
  const ssStub = { getSheetByName(n) { return n === '_Status' ? statusSheet : null; }, getRangeByName() { return null; } };
  const context = {
    PropertiesService: { getScriptProperties() {
      if (options.propsThrow) throw new Error('synthetic property outage');
      return { getProperty(k) { reads[k] = (reads[k] || 0) + 1; return Object.prototype.hasOwnProperty.call(store, k) ? store[k] : null; },
               setProperty(k, v) { store[k] = String(v); }, deleteProperty(k) { delete store[k]; } }; } },
    Logger: { log(m) { logs.push(String(m)); } },
    SpreadsheetApp: { getActiveSpreadsheet() { return ssStub; }, getUi() { throw new Error('no ui'); } },
    Utilities: { formatDate(d, tz, pattern) { return pattern === 'yyyy-MM-dd' ? '2026-10-24' : '2026-10-24 00:00:00'; } },
    Session: { getScriptTimeZone() { return 'Asia/Riyadh'; } },
    UrlFetchApp: { fetch() { throw new Error('no net'); } }
  };
  vm.createContext(context);
  context.__store = store; context.__logs = logs; context.__reads = reads;
  return context;
}
function loadMini(source, props, options) {
  const c = miniCtx(props, options);
  vm.runInContext(source, c, { filename: '16_Decision_Top10.gs' });
  return c;
}

/* ------------------------------------------------------------ core fixtures */
/* Board of up to 10 seats, day-advance 2026-10-23 -> 2026-10-24. Audited grid
 * ranks (opportunity_score desc): NEWC 97 (1), A 95 (2), FTM 90 (3), F1..F3
 * 89..87 (4..6), CHAL 86.5 (7), W 86 WATCH (8), F4..F9 85.5..83 (9..14),
 * B 82.5 (15 = the buffer boundary), OUT 70 INVEST (16, outside). Raw (the
 * backend's sized list): [A, CHAL]. State: members A (raw), B (absent, INVEST
 * inside the buffer = HOLDABLE), W (absent, WATCH inside the buffer), GHOST
 * (absent from the grid entirely; identity stash with a stale price), FTM
 * (absent, unconfirmed fast-track ci 1), OUT (absent, INVEST at rank 16). */
function knobs(extra) {
  return Object.assign({ enabled: true, confirm_days: 3, exit_days: 3, rank_buffer: 15, smooth_days: 5,
                         hard_strict: true, confirm_basis: 'qualified' }, extra || {});
}
function cand(sym, sc, verdict, extra) {
  return Object.assign({ symbol: sym, name: 'Name ' + sym, market: 'NYSE/NASDAQ', sector: 'Financial Services', currency: 'USD',
    opportunity_score: sc, verdict: verdict === undefined ? 'INVEST' : verdict, price: 10, price_sar: 37.5, roi_pct: 9,
    engine_roi_pct: 18, ann_roi_pct: 12, reliability: 76.5, dq: 100, confidence_band: 'High' }, extra || {});
}
function ticket(sym, sc) {
  return { symbol: sym, name: 'Name ' + sym, market: 'NYSE/NASDAQ', sector: 'Financial Services', currency: 'USD', fx_to_sar: 3.75,
    price: 10, price_sar: 37.5, entry_zone: '36-39 SAR', suggested_sar: 5000, suggested_shares: 133, stop_sar: 33, tp1_sar: 44,
    tp2_sar: 48, roi_pct: 9, engine_roi_pct: 18, ann_roi_pct: 12, exp_gain_12m_sar: 900, reliability: 76.5, dq: 100,
    confidence_band: 'High', opportunity_score: sc, advisor_note: 'INVEST - 5,000 SAR (133 sh)',
    detail: { funds_from: 'Cash 5,000 SAR', review_date: '2026-11-24' } };
}
function grid() {
  const g = [cand('NEWC.US', 97), cand('A.US', 95), cand('FTM.US', 90), cand('F1.US', 89), cand('F2.US', 88), cand('F3.US', 87),
    cand('CHAL.US', 86.5), cand('W.US', 86, 'WATCH', { confidence_band: 'Medium', engine_roi_pct: 34.8 })];
  for (let i = 4; i <= 9; i++) g.push(cand('F' + i + '.US', 85.5 - (i - 4) * 0.5));
  g.push(cand('B.US', 82.5, 'INVEST', { price: 13.4, price_sar: 50.25, reliability: 71.5, confidence_band: 'Medium', engine_roi_pct: 31.6 }));
  g.push(cand('OUT.US', 70));
  return g;
}
function raws() { return [ticket('A.US', 95), ticket('CHAL.US', 86.5)]; }
/* The state blob the day before `d0` (ISO date): the mini shim advances to 2026-10-24
 * (d0 = 2026-10-23), the refresh env's frozen clock is 2026-10-10 (d0 = 2026-10-09). */
function isoShift(d0, days) { return new Date(Date.parse(d0 + 'T00:00:00Z') + days * 86400000).toISOString().slice(0, 10); }
function seed(d0) {
  d0 = d0 || '2026-10-23';
  return { v: 1, date: d0, symbols: {
    'A.US': { ci: 3, co: 0, member: true, since: isoShift(d0, -8), ls: d0, hist: [95], ft: false },
    'B.US': { ci: 3, co: 0, member: true, since: isoShift(d0, -8), ls: d0, hist: [84], ft: false },
    'W.US': { ci: 3, co: 0, member: true, since: isoShift(d0, -7), ls: d0, hist: [85], ft: false },
    'GHOST.US': { ci: 3, co: 0, member: true, since: isoShift(d0, -6), ls: d0, hist: [83], ft: false,
                  id: { n: 'Ghost Corp', mk: 'NYSE/NASDAQ', se: 'Industrials', cy: 'USD', px: 19.88 } },
    'FTM.US': { ci: 1, co: 0, member: true, since: d0, ls: d0, hist: [90], ft: true },
    'OUT.US': { ci: 3, co: 0, member: true, since: isoShift(d0, -5), ls: d0, hist: [80], ft: false } } };
}
function core(g, k, extra) {
  const st = g.dt10StabParseState_(plain(seed()));
  const o = g.dt10StabCore_(plain(raws()), plain(grid()), st, plain(k), 10, '2026-10-24', (extra && extra.outage) || undefined);
  return plain({ tickets: o.tickets, state: o.state, audit: o.audit, note: o.note });
}
function bySym(list, s) { for (const t of list) if (t && t.symbol === s) return t; return null; }
/* End to end through the REAL dt10ApplyStability_ (service reads -> knobs, state load/save,
 * epoch from the _Status feed stamp = 2026-10-24). */
function applyE2E(source, props, options) {
  const g = loadMini(source, props, options);
  g.__store.DT10_STAB_STATE_V1 = JSON.stringify(seed());
  const payload = { selected: raws(), candidates_rows: grid(), meta: {} };
  const panel = { 'T10: Max Selected': 10, 'T10: Stability Enabled': 'Yes', 'T10: Stability Confirm Days': 3,
                  'T10: Stability Exit Days': 3, 'T10: Stability Rank Buffer': 15, 'T10: Stability Smooth Days': 5 };
  const out = g.dt10ApplyStability_(payload, panel, null);
  return { g, note: out.note, selected: plain(payload.selected), audit: plain(payload.meta.stability.audit),
           knobs: plain(payload.meta.stability.knobs), saved: JSON.parse(g.__store.DT10_STAB_STATE_V1), reads: plain(g.__reads),
           logs: g.__logs.slice() };
}

/* ============================================================ shim 2: cell-model spreadsheet (REAL dt10RenderPayload_) */
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
function fakeDate() {
  const D = Date;
  function FD(...args) {
    if (!(this instanceof FD)) return new D(NOW).toString();
    return args.length ? new D(...args) : new D(NOW);
  }
  FD.prototype = D.prototype;
  FD.now = () => NOW; FD.UTC = D.UTC; FD.parse = D.parse;
  return FD;
}
function loadRender(source, props, opts) {
  opts = opts || {};
  const logs = [], propReads = [];
  const ctx = {
    Date: fakeDate(),
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
/* Render fixture: the SELECTED board after dt10ApplyStability_ -- two sized ACTIVE
 * seats, a grace ghost with a pool row (stash price, blank Rel/DQ/FX), a grace ghost
 * without any row, a grid ghost (FX blank), a src-present P-145 grace seat and a
 * fast-track seat. */
function renderTicket(i) {
  const sym = ['ACT1.US', 'ACT2.US', 'ZTO.US', 'NONE.US', 'DDI.US', 'P145.US', 'FT.US'][i];
  const t = { rank: i + 1, symbol: sym, name: 'Name ' + sym, market: 'NYSE/NASDAQ', sector: 'Sector ' + (i % 3), currency: 'USD',
    fx_to_sar: 3.75, price: 10 + i, price_sar: (10 + i) * 3.75, entry_zone: '1-2', suggested_sar: 5000, suggested_shares: 100,
    stop_sar: 30 + i, tp1_sar: 50 + i, tp2_sar: 60 + i, roi_pct: 12.5, engine_roi_pct: [31.6, 34.8, 33.0, 20.0, 31.6, 34.0, 36.2][i],
    ann_roi_pct: 15, exp_gain_12m_sar: 600, reliability: 76.5, dq: 100, confidence_band: ['High', 'Medium', 'High', 'Medium', 'Medium', 'High', 'High'][i],
    detail: { funds_from: 'Cash 5,000 SAR', review_date: '2026-11-09' }, advisor_note: 'INVEST - 5,000 SAR (100 sh)',
    _stab_status: 'ACTIVE (day 4)', _stab_days: 4, _stab_since: '2026-10-07', _stab_smoothed: 80, _stab_trend: 'steady' };
  if (sym === 'ZTO.US' || sym === 'NONE.US') {   // stash ghosts (absent from the grid)
    Object.assign(t, { _grace_hold: true, rank: '', fx_to_sar: '', price: sym === 'ZTO.US' ? 19.88 : 5, price_sar: '', entry_zone: DASH,
      suggested_sar: DASH, suggested_shares: DASH, stop_sar: DASH, tp1_sar: DASH, tp2_sar: DASH, roi_pct: '', engine_roi_pct: '',
      ann_roi_pct: '', exp_gain_12m_sar: DASH, reliability: '', dq: '', confidence_band: '', detail: {},
      _stab_status: 'GRACE (2/3 missed)', advisor_note: 'Stability grace hold (2/3 missed) ' + DASH + ' sizing suspended until it re-qualifies.' });
  } else if (sym === 'DDI.US') {   // grid ghost
    Object.assign(t, { _grace_hold: true, rank: '', fx_to_sar: '', price: 13.4, price_sar: 50.25, entry_zone: DASH, suggested_sar: DASH,
      suggested_shares: DASH, stop_sar: DASH, tp1_sar: DASH, tp2_sar: DASH, exp_gain_12m_sar: DASH, reliability: 71.5, detail: {},
      _stab_status: 'GRACE (0/3 missed)', advisor_note: 'Stability grace hold (0/3 missed) ' + DASH + ' sizing suspended until it re-qualifies.' });
  } else if (sym === 'P145.US') {
    Object.assign(t, { _grace_hold: true, _p145_suspended: true, entry_zone: DASH, suggested_sar: DASH, suggested_shares: DASH, stop_sar: DASH,
      tp1_sar: DASH, tp2_sar: DASH, exp_gain_12m_sar: DASH, _stab_status: 'GRACE (1/3 missed)', detail: { funds_from: DASH } });
  } else if (sym === 'FT.US') {
    Object.assign(t, { _ft_suspended: true, entry_zone: DASH, suggested_sar: DASH, suggested_shares: DASH, stop_sar: DASH, tp1_sar: DASH,
      tp2_sar: DASH, exp_gain_12m_sar: DASH, _stab_status: 'FAST-TRACK (day 1)', detail: { funds_from: DASH } });
  }
  return t;
}
function renderCand(i) {
  const verdict = ['INVEST', 'WATCH', 'DO_NOT_INVEST', 'BLOCKED', ''][i % 5];
  return { symbol: 'C' + i + '.US', name: 'Cand ' + i, market: 'US', sector: 'S' + (i % 7), currency: 'USD', price: 1 + i,
    price_sar: (1 + i) * 3.75, roi_pct: 0.01 * (i % 30), engine_roi_pct: 0.02, ann_roi_pct: 0.03, rr: 1.5, reliability: 70, dq: 90,
    risk_level: 'Low', news_trend: '', sector_trend: '', conflict: i % 2 === 0, verdict, confidence_band: 'MID', opportunity_score: 1000 - i,
    first_fail: verdict === 'INVEST' ? null : { gate: 'Quote Freshness' }, failure_reason: verdict === 'INVEST' ? '' : 'stale',
    structural_block: false, selected: i < 3, deferral: i === 5 ? 'sector cap' : '' };
}
function renderPayload() {
  const cands = Array.from({ length: 30 }, (_, i) => renderCand(i));
  cands.push(cand('DDI.US', 80, 'INVEST', { price: 13.4, price_sar: 50.25, reliability: 71.5, confidence_band: 'Medium', engine_roi_pct: 31.6 }));
  return { version: 'synthetic', status: 'ok', selected: Array.from({ length: 7 }, (_, i) => renderTicket(i)), candidates_rows: cands,
    near_miss: [{ symbol: 'N0', failed_gate: 'ROI', current: 1, required: 2, verdict: 'WATCH', improve_note: 'n0' }],
    alerts: [{ type: 'T0', count: 0, required_action: 'act 0' }],
    kpis: { scanned: 9788, passed: 3, deployable_sar: 71374.54, max_selected: 10, selected_count: 2 },
    meta: { route: 'v4.18.2', board_funding: { contract_version: 1, stage: 'allocate', finalized: true, allocation_version: 'synthetic',
                                                eligible_symbols: ['ACT1.US', 'ACT2.US'] } } };
}
const LIVE_IDX = { 'ZTO.US': { price: 20.15, rel: 71.5, dq: 100, ccy: 'USD', status: 'INVESTABLE' },
                   'P145.US': { price: 99, rel: 1, dq: 1, ccy: 'USD', status: 'INVESTABLE' } };
function render(source, props, state, opts) {
  opts = opts || {};
  const ctx = loadRender(source, props, opts);
  const book = makeBook();
  const sheet = book.addSheet('Top_10_Investments', 400, 30);
  const uv = state === 'EXECUTABLE'
    ? { state: 'EXECUTABLE', reason: '', ageMin: 206, source_readiness: { ready: true, cohort: '39642923547' } }
    : { state: 'NOT_ACTIONABLE', reason: 'final_publication', ageMin: 30, source_readiness: { ready: true, cohort: '39642923547' } };
  ctx.dt10BoardVerdict_ = () => JSON.parse(JSON.stringify(uv));
  const p = renderPayload();
  if (opts.live) p._dt10_live = { idx: opts.liveIdx === undefined ? plain(LIVE_IDX) : opts.liveIdx, fx: { USD: 3.75, EUR: 4.2 } };
  if (opts.rule) p._dt10_rule = { mode: 'on' };
  if (opts.vint) p._dt10_vint = { mode: 'enforce', idx: {} };
  const earn = ctx.dt10RenderPayload_(sheet, p, ctx.DT10_FALLBACK_TOKENS);
  return { ctx, book, sheet, snap: snapshot(sheet), note: earn && earn.note, payload: p, propReads: ctx.__propReads.slice(),
           liveNote: p._dt10_live_note, ruleNote: p._dt10_rule_note, maxCols: sheet.getMaxColumns() };
}
function diffCells(a, b) {
  const keys = new Set(Object.keys(a.cells).concat(Object.keys(b.cells)));
  const out = [];
  for (const k of keys) if (JSON.stringify(a.cells[k]) !== JSON.stringify(b.cells[k])) out.push(k);
  return out.sort();
}
function rowOf(snap, sym) {
  for (const k of Object.keys(snap.cells)) { const [r, c] = k.split(':').map(Number); if (c === 2 && snap.cells[k].v === sym) return r; }
  return -1;
}
function cellV(snap, r, c) { const x = snap.cells[r + ':' + c]; return x ? x.v : undefined; }

/* ============================================================ shim 3: full workbook (REAL refreshDecisionTop10) */
class FixedDate extends Date {
  constructor(...a) { super(...(a.length ? a : [NOW])); }
  static now() { return NOW; }
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
  env.sheets['Top_10_Investments'] = mkSheet(env, 'Top_10_Investments', { maxRows: 800, maxCols: 30, seed: { '4:1': 'CONTROL PANEL', '14:1': 'KPIs' } });
  env.sheets['_Status'] = mkSheet(env, '_Status', { grid: [STATUS_HEADER].concat(['Market_Leaders', 'Global_Markets', 'Commodities_FX', 'Mutual_Funds'].map(p =>
    [p, '2026-10-10 21:19:47+03:00', 'SUCCESS', 'leg=success run=' + FIX.export_run, 'backend:run_dashboard_sync', '', 0, 115, 0, 0])),
    a1: { 'L1:M60': plain(FIX.status_lm) } });
  env.sheets['_Run_Log'] = mkSheet(env, '_Run_Log', {});
  env.sheets['Portfolio_Decision'] = mkSheet(env, 'Portfolio_Decision', { grid: plain(FIX.pd_block) });
  env.sheets['Calendar_Events'] = mkSheet(env, 'Calendar_Events', { grid: plain(FIX.calendar_rows) });
  env.sheets['My_Portfolio'] = mkSheet(env, 'My_Portfolio', { grid: [plain(PAGES.Global_Markets.header)] });
  for (const pg of ['Market_Leaders', 'Global_Markets', 'Commodities_FX', 'Mutual_Funds']) {
    const rows = plain(PAGES[pg].rows);
    if (pg === 'Global_Markets') {   // the live row of the GHOST seat: investable at source, a fresh price
      const h = PAGES[pg].header;
      rows[2][h.indexOf('Symbol')] = 'GHOST.US'; rows[2][h.indexOf('Current Price')] = 20.15;
      rows[2][h.indexOf('Forecast Reliability Score')] = 71.5; rows[2][h.indexOf('Data Quality Score')] = 100;
      rows[2][h.indexOf('Investability Status')] = 'INVESTABLE'; rows[2][h.indexOf('Currency')] = 'USD';
    }
    env.sheets[pg] = mkSheet(env, pg, { grid: [PAGES[pg].header].concat(rows) });
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
/* The backend stub: the research call answers with the fixture board (raw [A, CHAL], the
 * grid above, a frozen funding snapshot); the allocate replay sizes exactly the symbols
 * the cockpit named in board_funding_symbols (the backend's eligibility rule). */
const BUILDER = 'test-1.28.0';
function sizedTicket(sym) {
  const t = ticket(sym, 90);
  t.suggested_sar = sym === 'B.US' ? 4200 : 5000; t.suggested_shares = sym === 'B.US' ? 84 : 133; t.advisor_note = 'ALLOCATED ' + sym;
  return t;
}
function postStub(env) {
  return function (body) {
    env.posted.push(plain(body.criteria));
    if (body.criteria.board_funding_stage === 'allocate') {
      const eligible = body.criteria.board_funding_symbols || [];
      return { code: 200, json: { version: BUILDER, status: 'ok', selected: eligible.map(sizedTicket), candidates_rows: [], near_miss: [],
        alerts: [], kpis: { scanned: 40, passed: 13 }, meta: { board_funding: { contract_version: 1, stage: 'allocate', snapshot_id: 'sid-1' } } } };
    }
    return { code: 200, json: { version: BUILDER, status: 'ok', selected: raws(), candidates_rows: grid(), near_miss: [], alerts: [],
      kpis: { scanned: 40, passed: 13, max_selected: 10, selected_count: 2, deployable_sar: 50000 },
      meta: { route: { version: '4.18.2', pool: { source: 'body_rows', count: 40 } }, versions: { opportunity_builder: BUILDER },
              board_funding: { contract_version: 1, stage: 'research', snapshot_available: true, snapshot_id: 'sid-1',
                               snapshot: { builder_version: BUILDER, snapshot_id: 'sid-1', rows: [] } } } } };
  };
}
function refresh(source, props, opts) {
  opts = opts || {};
  const env = makeEnv({ props: Object.assign({ DT10_STAB_STATE_V1: JSON.stringify(seed('2026-10-09')) }, props || {}), propsThrow: opts.propsThrow });
  vm.createContext(env.ctx);
  vm.runInContext(source, env.ctx, { filename: '16_Decision_Top10.gs' });
  env.ctx.dt10StabToday_ = () => TODAY;
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
  const selRows = env.calls.filter(x => x[0] === 'setValues' && x[1] === 'Top_10_Investments' && x[2] === 19).map(x => JSON.parse(x[6]));
  const seatRows = env.calls.filter(x => x[0] === 'setValues' && x[1] === 'Top_10_Investments' && x[2] === 20).map(x => JSON.parse(x[6]));
  return { env, status: String(top10._val(2, 2)), calls: env.calls.slice(), runLog: plain(env.appended['_Run_Log'] || []),
           selLog: env.calls.filter(x => x[1] === '_Selection_Log' && x[0] === 'setValues').map(x => JSON.parse(x[6])),
           logs: env.logs.slice(), posted: env.posted.slice(), propReads: plain(env.propReads),
           header: selRows.length ? selRows[0][0] : null, seats: seatRows.length ? seatRows[0] : [],
           saved: JSON.parse(env.props.DT10_STAB_STATE_V1) };
}
function seatRow(run, sym) { for (const r of run.seats) if (r[1] === sym) return r; return null; }
const stripToken = s => String(s).replace(/, qhold: (?:would hold|held) \d+(?: [A-Z0-9.]+)*/, '');
/* The version string reaches the meta footer, the B2 status line, the Logger lines and the
 * run-marker property; comparisons against the base read the delivered version as the base's. */
const nv = x => JSON.parse(JSON.stringify(x).replace(/v1\.19\.0/g, 'v1.18.0'));

/* ============================================================ battery */
function battery() {
  const N = loadMini(newSource, {});
  const B = loadMini(baseSource, {});
  const OFF_PROPS = [{}, { [HOLD]: 'off', [LIVE]: 'off', [RULE]: 'off' }, { [HOLD]: '', [LIVE]: '', [RULE]: '' },
                     { [HOLD]: 'yes', [LIVE]: '1', [RULE]: 'enforce' }, { [HOLD]: 'enforced', [LIVE]: 'onn', [RULE]: 'observe' }];

  test('T0 base oracle is the reviewed v1.18.0 file (SHA-256 pinned); versions in lockstep; both files compile', () => {
    eq(sha256(baseSource), history.V1180_SHA256, 'base SHA-256');
    eq(B.DT10_VERSION, '1.18.0', 'base version');
    eq(N.DT10_VERSION, '1.19.0', 'delivered version');
    ok(/^ \* Version: 1\.19\.0 /.test(newSource.split('\n')[3]), 'header line 4 lockstep');
    ok(newSource.indexOf("var DT10_VERSION = '1.19.0';") > 0, 'DT10_VERSION literal');
    eq(N.DT10_QUALIFIED_HOLD_PROP, HOLD); eq(N.DT10_SEAT_LIVE_FILL_PROP, LIVE); eq(N.DT10_OWNER_RULE_PROP, RULE);
    eq(N.DT10_OWNER_RULE_ROI_MAX, 34, 'the owner rule threshold is a display constant');
    deq(N.DT10_OWNER_RULE_HEADERS, ['Owner Rule']);
    new vm.Script(newSource, { filename: '16_Decision_Top10.gs' });
    new vm.Script(baseSource, { filename: 'base.gs' });
  });

  let offCore = null, offE2E = null, offRefresh = null, baseRefresh = null, offRender = null;
  /* the off references, built in T1 and rebuilt on demand so a T1 failure (the golden
   * negative run on the base) still lets every later section report its own reason */
  const getOffCore = () => offCore || (offCore = core(N, knobs()));
  const getOffE2E = () => offE2E || (offE2E = applyE2E(newSource, {}));
  const getOffRender = () => offRender || (offRender = render(baseSource, {}, 'EXECUTABLE'));
  const getBaseRefresh = () => baseRefresh || (baseRefresh = refresh(baseSource, {}));
  const getOffRefresh = () => offRefresh || (offRefresh = refresh(newSource, {}));
  test('T1 GOLDEN NEGATIVE / kill identity: properties absent, off, blank, junk, faulted -> pure core, dt10ApplyStability_, the REAL render and the REAL refresh deep-equal the v1.18.0 base', () => {
    // (a) pure core: knob absent / off / junk == base
    const base = core(B, knobs());
    offCore = core(N, knobs());
    deq(offCore, base, 'core knob absent');
    deq(core(N, knobs({ qualified_hold: 'off' })), base, 'core knob off');
    deq(core(N, knobs({ qualified_hold: 'yes' })), base, 'core knob junk');
    deq(core(B, knobs({ qualified_hold: 'enforce' })), base, 'the base ignores the knob');
    ok(bySym(base.tickets, 'B.US')._stab_status === 'GRACE (0/3 missed)' && bySym(base.tickets, 'B.US')._grace_hold === true,
       'the defect on the base: the qualified member B is GRACE (0/3 missed) with no plan');
    ok(!B.dt10BoardEligible_(bySym(base.tickets, 'B.US')), 'the base never offers B to the replay');
    eq(base.note.indexOf('qhold'), -1, 'no token on the base');
    // (b) e2e dt10ApplyStability_: every property spelling and a service fault
    const e2eB = applyE2E(baseSource, {});
    offE2E = applyE2E(newSource, {});
    for (const props of OFF_PROPS) {
      const e = applyE2E(newSource, props);
      deq([e.note, e.selected, e.audit, e.saved, e.knobs], [e2eB.note, e2eB.selected, e2eB.audit, e2eB.saved, e2eB.knobs], 'e2e ' + JSON.stringify(props));
      eq(e.reads[HOLD], 1, 'one hold read per apply');
      deq(nv(e.logs), e2eB.logs, 'Logger lines ' + JSON.stringify(props));
    }
    const eF = applyE2E(newSource, { [HOLD]: 'enforce' }, { propsThrow: true });
    const eFB = applyE2E(baseSource, {}, { propsThrow: true });
    deq([eF.note, eF.selected, eF.audit, eF.knobs], [eFB.note, eFB.selected, eFB.audit, eFB.knobs], 'e2e service fault');
    eq(e2eB.reads[HOLD], undefined, 'the base never reads the property');
    ok(!Object.prototype.hasOwnProperty.call(offE2E.knobs, 'qualified_hold'), 'knobs object byte-identical when off');
    // (c) the REAL render (no _dt10_live / _dt10_rule keys attached, whatever the properties say)
    for (const state of ['EXECUTABLE', 'NOT_ACTIONABLE']) {
      const b = render(baseSource, {}, state);
      for (const props of OFF_PROPS) {
        const n = render(newSource, props, state);
        const label = state + ' ' + JSON.stringify(props);
        deq(n.snap, b.snap, 'identical sheet ' + label + ' (diff ' + diffCells(n.snap, b.snap).slice(0, 4).join(',') + ')');
        eq(n.note, b.note, 'identical status token ' + label);
        deq(n.payload.selected, b.payload.selected, 'tickets untouched ' + label);
        deq(n.propReads.slice().sort(), b.propReads.slice().sort(), 'the render reads no new property ' + label);
        ok(n.liveNote === undefined && n.ruleNote === undefined, 'no token keys stored ' + label);
        eq(n.maxCols, 30, '30 columns ' + label);
      }
      const f = render(newSource, { [LIVE]: 'on', [RULE]: 'on' }, state, { propsThrow: true });
      deq(f.snap, render(baseSource, {}, state, { propsThrow: true }).snap, 'render under a property fault ' + state);
      if (state === 'EXECUTABLE') offRender = b;
    }
    // (d) the REAL refresh: B2, every write, the run log, the selection log, the Logger lines
    baseRefresh = refresh(baseSource, {});
    offRefresh = refresh(newSource, {});
    for (const props of OFF_PROPS) {
      const n = refresh(newSource, props);
      const label = JSON.stringify(props);
      eq(nv(n.status), baseRefresh.status, 'B2 status line ' + label);
      deq(nv(n.calls), baseRefresh.calls, 'every sheet write ' + label);
      deq(nv(n.runLog), baseRefresh.runLog, '_Run_Log rows ' + label);
      deq(nv(n.selLog), baseRefresh.selLog, '_Selection_Log rows ' + label);
      deq(nv(n.logs), baseRefresh.logs, 'Logger lines ' + label);
      deq(n.posted, baseRefresh.posted, 'POST bodies (research + replay) ' + label);
      deq(n.saved, baseRefresh.saved, 'saved state blob ' + label);
      eq(n.propReads.total, baseRefresh.propReads.total + 3, 'exactly three property reads added ' + label);
      deq([n.propReads.byKey[HOLD], n.propReads.byKey[LIVE], n.propReads.byKey[RULE]], [1, 1, 1], 'each read once ' + label);
    }
    ok(/\| output: EXECUTABLE \|/.test(baseRefresh.status) && /funding=final\/1exec/.test(baseRefresh.status), 'fixture: the base funds A only: ' + baseRefresh.status);
    ok(/stab\[strict\]: 5 grace, 1 fast-track, confirm=qualified \| epoch=2026-10-10\/feed \| /.test(baseRefresh.status), 'fixture stab note: ' + baseRefresh.status);
    const bRow = seatRow(baseRefresh, 'B.US');
    ok(bRow && bRow[0] === DASH && bRow[24] === 'GRACE (0/3 missed)' && bRow[11] === 0, 'the defect on the base board: B rank dash, GRACE (0/3 missed), 0 shares: ' + JSON.stringify(bRow));
    deq(baseRefresh.posted[1].board_funding_symbols, ['A.US'], 'the base replay names A only');
    eq(baseRefresh.header.length, 30, '30 seat columns');
  });

  test('T2 observe: only the qhold token (and audit.qualified_held) is added; tickets, clocks, cells and rows are the off output', () => {
    // pure core
    const o = core(N, knobs({ qualified_hold: 'observe' }));
    const stripped = plain(o); delete stripped.audit.qualified_held; stripped.note = stripToken(stripped.note);
    deq(stripped, getOffCore(), 'observe == off apart from the token');
    ok(o.note.endsWith(', qhold: would hold 1 B.US'), 'token: ' + o.note);
    deq(o.audit.qualified_held, ['B.US'], 'audit list');
    ok(bySym(o.tickets, 'B.US')._stab_status === 'GRACE (0/3 missed)' && bySym(o.tickets, 'B.US')._grace_hold === true, 'B still GRACE under observe');
    // e2e
    for (const spelling of ['observe', ' OBSERVE ', 'Observe']) {
      const e = applyE2E(newSource, { [HOLD]: spelling });
      deq([e.selected, e.saved], [getOffE2E().selected, getOffE2E().saved], 'e2e tickets and blob ' + JSON.stringify(spelling));
      eq(stripToken(e.note), getOffE2E().note, 'e2e note apart from the token');
      ok(/, qhold: would hold 1 B\.US \| epoch=/.test(e.note), 'e2e token before the epoch token: ' + e.note);
      eq(e.knobs.qualified_hold, 'observe', 'knob published only when armed');
      const a = plain(e.audit); delete a.qualified_held; deq(a, getOffE2E().audit, 'e2e audit apart from the list');
      ok(e.logs.some(l => /qhold: would hold 1 B\.US/.test(l)), 'Logger carries the token');
    }
    // REAL refresh
    const r = refresh(newSource, { [HOLD]: 'observe' });
    eq(stripToken(r.status), getOffRefresh().status, 'B2 = off line + the token');
    ok(/, qhold: would hold 1 B\.US \| epoch=2026-10-10\/feed/.test(r.status), 'B2 token: ' + r.status);
    const noB2 = calls => calls.filter(x => !(x[0] === 'setValue' && x[1] === 'Top_10_Investments' && x[2] === 2 && x[3] === 2) && x[1] !== '_Selection_Log');
    deq(noB2(r.calls), noB2(getOffRefresh().calls), 'every other write identical');
    deq(r.selLog.map(rows => rows.map(row => row.map((v, i) => i === 1 ? '<run info>' : v))),
        getOffRefresh().selLog.map(rows => rows.map(row => row.map((v, i) => i === 1 ? '<run info>' : v))), '_Selection_Log rows apart from Run Info');
    ok(r.selLog.some(rows => rows.some(row => String(row[1]).indexOf('qhold: would hold 1 B.US') > 0)), 'Run Info carries the token');
    deq(r.posted, getOffRefresh().posted, 'POST bodies identical (B not offered)');
    deq(r.saved, getOffRefresh().saved, 'state blob identical');
    deq(r.runLog, getOffRefresh().runLog, '_Run_Log identical');
    deq(seatRow(r, 'B.US'), seatRow(getOffRefresh(), 'B.US'), 'B row identical');
  });

  test('T3 enforce: the qualified member keeps ACTIVE (co 0), is offered to the replay and the replay sizing lands; WATCH / absent / unconfirmed fast-track seats go to GRACE as before; the base never holds', () => {
    const o = core(N, knobs({ qualified_hold: 'enforce' }));
    const b = bySym(o.tickets, 'B.US');
    ok(b && b._stab_status === 'ACTIVE (day 10)' && b._grace_hold !== true && b._qhold === true && b._p145_suspended !== true, 'B ACTIVE: ' + JSON.stringify(b));
    eq(b.rank, 4, 'B keeps a numeric rank (board order A, FTM, W, B, GHOST, OUT, CHAL)');
    ok(N.dt10BoardEligible_(b), 'B is board-eligible');
    ok(b.suggested_sar === DASH && b.suggested_shares === DASH && b.entry_zone === DASH, 'no sizing invented by the cockpit');
    eq(b.price, 13.4, 'audited-row metrics kept'); eq(b.reliability, 71.5);
    ok(/^Qualified hold: INVEST on the audited grid/.test(b.advisor_note), 'note: ' + b.advisor_note);
    deq(o.state, getOffCore().state, 'clocks identical to off (co 0 either way)');
    eq(o.state.symbols['B.US'].co, 0); eq(o.state.symbols['B.US'].ci, 4); eq(o.state.symbols['B.US'].member, true);
    ok(o.note.indexOf(', qhold: held 1 B.US') > 0 && /^stab: 4 grace, 1 fast-track, confirm=qualified, qhold: held 1 B\.US$/.test(o.note), 'note: ' + o.note);
    deq(o.audit.held_by_grace.map(h => h.symbol), ['FTM.US', 'W.US', 'GHOST.US', 'OUT.US'], 'B left the grace ledger');
    deq(o.audit.qualified_held, ['B.US']);
    // the seats that must NOT be held
    const w = bySym(o.tickets, 'W.US'), gh = bySym(o.tickets, 'GHOST.US'), ft = bySym(o.tickets, 'FTM.US'), out = bySym(o.tickets, 'OUT.US');
    deq([w, gh, ft, out].map(t => t._stab_status), ['GRACE (0/3 missed)', 'GRACE (1/3 missed)', 'GRACE (0/3 missed)', 'GRACE (1/3 missed)'], 'labels as before');
    deq([w, gh, ft, out], [bySym(getOffCore().tickets, 'W.US'), bySym(getOffCore().tickets, 'GHOST.US'), bySym(getOffCore().tickets, 'FTM.US'), bySym(getOffCore().tickets, 'OUT.US')], 'those four tickets byte-identical to off');
    deq(bySym(o.tickets, 'A.US'), bySym(getOffCore().tickets, 'A.US'), 'A (in raw) identical');
    deq(bySym(o.tickets, 'CHAL.US'), bySym(getOffCore().tickets, 'CHAL.US'), 'CHAL fast-track identical');
    eq(o.tickets.length, getOffCore().tickets.length, 'no seat added');
    // raw basis: no hold (the condition needs the qualified basis)
    const raw = core(N, knobs({ qualified_hold: 'enforce', confirm_basis: 'raw' }));
    ok(bySym(raw.tickets, 'B.US')._stab_status === 'GRACE (0/3 missed)' && raw.note.indexOf('qhold: held 0') > 0, 'raw basis never holds: ' + raw.note);
    // outage-paused member is never held
    const paused = core(N, knobs({ qualified_hold: 'enforce' }), { outage: { 'B.US': true } });
    ok(/^GRACE \(0\/3 missed\) - outage pause 1\/5/.test(bySym(paused.tickets, 'B.US')._stab_status) && paused.note.indexOf('qhold: held 0') > 0, 'outage pause wins: ' + paused.note);
    // co > 0 member (missed a day outside the buffer) is never held even when qualified today
    const sd = seed(); sd.symbols['B.US'].co = 1;
    const st2 = N.dt10StabParseState_(sd);
    const o2 = N.dt10StabCore_(raws(), grid(), st2, knobs({ qualified_hold: 'enforce' }), 10, '2026-10-24');
    ok(bySym(o2.tickets, 'B.US')._stab_status === 'ACTIVE (day 10)', 'a qualified day resets co to 0 first (v1.14.0), so B is held');
    const o3 = N.dt10StabCore_(raws(), grid(), N.dt10StabParseState_(sd), knobs({ qualified_hold: 'enforce' }), 10, '2026-10-23');   // same-day re-run: clocks frozen, co stays 1
    ok(bySym(o3.tickets, 'B.US')._stab_status === 'GRACE (1/3 missed)' && o3.note.indexOf('qhold: held 0') > 0, 'frozen clocks with co 1: GRACE as before');
    // e2e
    const e = applyE2E(newSource, { [HOLD]: 'enforce' });
    eq(e.knobs.qualified_hold, 'enforce');
    ok(bySym(e.selected, 'B.US')._stab_status === 'ACTIVE (day 10)' && bySym(e.selected, 'B.US')._qhold === true, 'e2e held');
    deq(e.saved, getOffE2E().saved, 'e2e blob identical');
    // REAL refresh: offered to the replay, sized by the replay, rendered as an executable ticket
    const r = refresh(newSource, { [HOLD]: 'enforce' });
    deq(r.posted[1].board_funding_symbols, ['A.US', 'B.US'], 'the replay is offered B');
    const bRow = seatRow(r, 'B.US');
    ok(bRow && bRow[0] === 4 && bRow[24] === 'ACTIVE (day 10)' && bRow[11] === 84 && bRow[10] === 4200 && bRow[29] === 'ALLOCATED B.US', 'B rendered as the replay sized it: ' + JSON.stringify(bRow));
    ok(/funding=final\/2exec/.test(r.status) && /stab\[strict\]: 4 grace, 1 fast-track, confirm=qualified, qhold: held 1 B\.US \| epoch=/.test(r.status), 'B2: ' + r.status);
    deq(seatRow(r, 'A.US'), seatRow(getOffRefresh(), 'A.US'), 'A row identical');
    deq(seatRow(r, 'W.US'), seatRow(getOffRefresh(), 'W.US'), 'W row identical (GRACE as before)');
    deq(seatRow(r, 'FTM.US'), seatRow(getOffRefresh(), 'FTM.US'), 'FTM row identical (unconfirmed fast-track never held)');
    deq(r.saved, getOffRefresh().saved, 'state blob identical');
    eq(r.seats.length, getOffRefresh().seats.length, 'no seat added');
    // the base under enforce = the base
    const rb = refresh(baseSource, { [HOLD]: 'enforce' });
    eq(rb.status, getBaseRefresh().status, 'GOLDEN NEGATIVE: the base ignores the property');
    deq(rb.posted, getBaseRefresh().posted);
  });

  test('T4 live fill: price / Rel / DQ / FX filled from the pool or grid row on grace and fast-track seats only; a seat without a row stays blank; investable rows are noted; the base ignores the property', () => {
    // pure core through the REAL render
    const b = render(baseSource, { [LIVE]: 'on' }, 'EXECUTABLE');
    deq(b.snap, getOffRender().snap, 'GOLDEN NEGATIVE: the base renders the same sheet with the property on');
    const n = render(newSource, { [LIVE]: 'on' }, 'EXECUTABLE', { live: true });
    const changed = diffCells(n.snap, getOffRender().snap);
    const zr = rowOf(getOffRender().snap, 'ZTO.US'), dr = rowOf(getOffRender().snap, 'DDI.US'), nr = rowOf(getOffRender().snap, 'NONE.US');
    const pr = rowOf(getOffRender().snap, 'P145.US');
    ok(zr > 0 && dr > 0 && nr > 0 && pr > 0, 'rows located');
    deq(changed, [zr + ':7', zr + ':8', zr + ':9', zr + ':20', zr + ':21', zr + ':30', dr + ':7', dr + ':30', pr + ':30'].sort(), 'exactly the live cells change: ' + changed.join(','));
    deq([cellV(n.snap, zr, 7), cellV(n.snap, zr, 8), cellV(n.snap, zr, 9), cellV(n.snap, zr, 20), cellV(n.snap, zr, 21)], [3.75, 20.15, 75.56, 71.5, 100], 'ZTO: FX, price, price SAR, Rel, DQ');
    ok(String(cellV(n.snap, zr, 30)).endsWith(' Source row today: investable (Investability Status INVESTABLE).'), 'ZTO note: ' + cellV(n.snap, zr, 30));
    ok(String(cellV(n.snap, zr, 30)).indexOf('Stability grace hold (2/3 missed)') === 0, 'the finalized note is kept, the fill is a suffix');
    deq([cellV(n.snap, dr, 7), cellV(n.snap, dr, 8), cellV(n.snap, dr, 9)], [3.75, 13.4, 50.25], 'DDI: FX from the map, grid price kept');
    ok(String(cellV(n.snap, dr, 30)).endsWith(' Source row today: investable (INVEST on the audited grid).'), 'DDI note');
    deq([cellV(n.snap, nr, 7), cellV(n.snap, nr, 8), cellV(n.snap, nr, 20), cellV(n.snap, nr, 21)], ['', 5, '', ''], 'NONE stays blank');
    deq([cellV(n.snap, pr, 8), cellV(n.snap, pr, 20)], [15, 76.5], 'a src-present grace seat keeps its own live numbers');
    ok(String(cellV(n.snap, pr, 30)).endsWith(' Source row today: investable (Investability Status INVESTABLE).'), 'P145: nothing blank to fill, but the investable source row is noted');
    deq([cellV(n.snap, zr, 25), cellV(n.snap, zr, 10), cellV(n.snap, zr, 11), cellV(n.snap, zr, 1)], ['GRACE (2/3 missed)', DASH, 0, DASH], 'status, plan and rank untouched');
    eq(n.liveNote, 'live-fill 2/5 (3 investable at source)', 'token (2 seats filled; 3 investable rows noted)');
    eq(n.note, getOffRender().note, 'the earnings token is untouched');
    // withheld feed: numbers stay, the note cell is the withhold text (v1.10.0)
    const w = render(newSource, { [LIVE]: 'on' }, 'NOT_ACTIONABLE', { live: true });
    deq([cellV(w.snap, zr, 8), cellV(w.snap, zr, 20)], [20.15, 71.5], 'withheld: live numbers stay');
    ok(/SIZING WITHHELD/.test(String(cellV(w.snap, zr, 30))), 'withheld: note cell is the withhold text');
    // no sheet pool index: grid rows only, token says so
    const g = render(newSource, {}, 'EXECUTABLE', { live: true, liveIdx: null });
    eq(g.liveNote, 'live-fill 1/5 (1 investable at source) (no sheet pool)', 'grid-only token');
    deq([cellV(g.snap, zr, 8), cellV(g.snap, zr, 20)], [19.88, ''], 'ZTO untouched without a pool row');
    // pure helpers
    deq(N.dt10SeatLiveIndex_([{ Symbol: ' x.us ', 'Current Price': 1 }, { Symbol: 'X.US', 'Current Price': 2 }, null, {}]), { 'X.US': { price: 1 } }, 'index: first row wins, blanks skipped');
    deq(N.dt10SeatLiveIndex_(null), {}); deq(N.dt10SeatLiveIndex_([]), {});
    eq(N.dt10SeatLiveFill_({}), ''); eq(N.dt10SeatLiveFill_(null), ''); eq(N.dt10SeatLiveFill_({ _dt10_live: 'x' }), '');
    const sel = [{ symbol: 'G.US', currency: 'GBp', price: '', price_sar: '', fx_to_sar: '', reliability: '', dq: '', _grace_hold: true, _stab_status: 'GRACE (1/3 missed)', advisor_note: 'n' }];
    const res = N.dt10SeatLiveFillCore_(sel, [], { 'G.US': { price: 250, rel: 70.4, dq: 95, ccy: 'GBp', status: 'watchlist' } }, { GBP: 5 });
    deq(res, { seats: 1, filled: 1, investable: 0 });
    deq([sel[0].price, sel[0].fx_to_sar, sel[0].price_sar, sel[0].reliability, sel[0].dq, sel[0].advisor_note], [250, 0.05, 12.5, 70.4, 95, 'n'], 'pence subunit through dt10HoldingFxRate_');
    const sar = [{ symbol: 'S.SR', currency: 'SAR', price: 10, price_sar: '', fx_to_sar: '', reliability: 70, dq: 90, _ft_suspended: true, _stab_status: 'FAST-TRACK (day 1)', advisor_note: 'n' }];
    N.dt10SeatLiveFillCore_(sar, [cand('S.SR', 50, 'INVEST', { price: 11, price_sar: 11, currency: 'SAR' })], {}, {});
    deq([sar[0].fx_to_sar, sar[0].price, sar[0].price_sar, sar[0].advisor_note], [1, 10, 11, 'n Source row today: investable (INVEST on the audited grid).'], 'SAR = 1; a src-present fast-track seat keeps its price');
    // REAL refresh: the index is built from the sheet pool the cockpit collected
    const r = refresh(newSource, { [LIVE]: 'on' });
    const gh = seatRow(r, 'GHOST.US'), ghOff = seatRow(getOffRefresh(), 'GHOST.US');
    ok(ghOff && ghOff[7] === 19.88 && ghOff[19] === '' && ghOff[20] === '' && ghOff[6] === '', 'off: the GHOST seat shows the stash price and blank Rel/DQ/FX: ' + JSON.stringify(ghOff));
    ok(gh && gh[7] === 20.15 && gh[19] === 71.5 && gh[20] === 100 && gh[6] === 3.751874 && gh[8] === 75.6 && gh[24] === 'GRACE (1/3 missed)', 'on: filled from the Global_Markets row, FX from TFB_FX_LOOKUP: ' + JSON.stringify(gh));
    ok(String(gh[29]).endsWith(' Source row today: investable (Investability Status INVESTABLE).'), 'noted investable: ' + gh[29]);
    ok(/ \| live-fill 5\/6 \(5 investable at source\) \| /.test(r.status), 'B2 token (6 grace / fast-track seats; 5 gained a blank FX or the pool row; 5 investable rows noted): ' + r.status);
    eq(r.status.replace(' | live-fill 5/6 (5 investable at source)', ''), getOffRefresh().status, 'B2 = off line + the token');
    deq(r.posted, getOffRefresh().posted, 'POST bodies identical'); deq(r.saved, getOffRefresh().saved, 'blob identical');
    const bOn = seatRow(r, 'B.US'), bOff = seatRow(getOffRefresh(), 'B.US');
    ok(bOff[6] === '' && bOn[6] === 3.751874, 'B (grid ghost): only the blank FX is filled');
    deq(bOn.map((v, i) => (i === 6 || i === 29) ? null : v), bOff.map((v, i) => (i === 6 || i === 29) ? null : v), 'B: every other cell identical');
    ok(String(bOn[29]).endsWith(' Source row today: investable (INVEST on the audited grid).'), 'B noted from the grid row');
    deq(seatRow(r, 'A.US'), seatRow(getOffRefresh(), 'A.US'), 'an ACTIVE sized seat is never touched');
    const cOn = seatRow(r, 'CHAL.US'), cOff = seatRow(getOffRefresh(), 'CHAL.US');
    deq(cOn.slice(0, 29), cOff.slice(0, 29), 'the fast-track seat keeps its own raw numbers');
    const rb = refresh(baseSource, { [LIVE]: 'on' });
    eq(rb.status, getBaseRefresh().status, 'GOLDEN NEGATIVE: the base ignores the property');
  });

  test('T5 owner rule: yes only for High + ROI < 34, the failing part named; the column is appended after the seat block (and after the vintage columns), survives a withheld feed; the base renders 30 columns', () => {
    const cases = [[{ confidence_band: 'High', engine_roi_pct: 31.6 }, 'rule: yes'], [{ confidence_band: 'Medium', engine_roi_pct: 34.8 }, 'rule: no (Medium; ROI 34.8)'],
      [{ confidence_band: 'High', engine_roi_pct: 34 }, 'rule: no (ROI 34.0)'], [{ confidence_band: 'Medium', engine_roi_pct: 20 }, 'rule: no (Medium)'],
      [{ confidence_band: '', engine_roi_pct: '' }, 'rule: no (conf unknown; ROI unknown)'], [{ confidence_band: 'high', engine_roi_pct: '33.9' }, 'rule: yes'],
      [{ confidence_band: 'Low' }, 'rule: no (Low; ROI unknown)'], [{ confidence_band: 'HIGH', engine_roi_pct: 33.99 }, 'rule: yes'],
      [{ confidence_band: 'High', engine_roi_pct: 'n/a' }, 'rule: no (ROI unknown)'], [null, 'rule: no (conf unknown; ROI unknown)']];
    for (const [t, want] of cases) eq(N.dt10OwnerRuleCell_(t), want, JSON.stringify(t));
    eq(N.dt10OwnerRuleNote_(cases.map(c => c[0])), 'owner-rule 3/10 yes'); eq(N.dt10OwnerRuleNote_([]), 'owner-rule 0/0 yes');
    // REAL render
    const n = render(newSource, { [RULE]: 'on' }, 'EXECUTABLE', { rule: true });
    eq(n.maxCols, 31, 'one column inserted'); eq(n.book.counts.insertColumnsAfter, 1);
    const hdrRow = rowOf(getOffRender().snap, 'ACT1.US') - 1;
    eq(cellV(n.snap, hdrRow, 31), 'Owner Rule', 'header'); eq(cellV(n.snap, hdrRow, 30), 'Advisor Note', 'Advisor Note stays column 30');
    const want = { 'ACT1.US': 'rule: yes', 'ACT2.US': 'rule: no (Medium; ROI 34.8)', 'ZTO.US': 'rule: no (conf unknown; ROI unknown)',
      'NONE.US': 'rule: no (conf unknown; ROI unknown)', 'DDI.US': 'rule: no (Medium)', 'P145.US': 'rule: no (ROI 34.0)', 'FT.US': 'rule: no (ROI 36.2)' };
    for (const sym of Object.keys(want)) eq(cellV(n.snap, rowOf(n.snap, sym), 31), want[sym], sym);
    const changed = diffCells(n.snap, getOffRender().snap);
    ok(changed.every(k => /:31$/.test(k)), 'only column 31 differs: ' + changed.filter(k => !/:31$/.test(k)).join(','));
    eq(n.ruleNote, 'owner-rule 1/7 yes', 'token');
    ok(n.snap.cells[rowOf(n.snap, 'ACT1.US') + ':31'].nf === '@', 'text format');
    const w = render(newSource, {}, 'NOT_ACTIONABLE', { rule: true });
    eq(cellV(w.snap, rowOf(w.snap, 'ACT1.US'), 31), 'rule: yes', 'withheld feed keeps the marker');
    ok(/SIZING WITHHELD/.test(String(cellV(w.snap, rowOf(w.snap, 'ACT1.US'), 30))), 'withheld note untouched');
    const v = render(newSource, {}, 'EXECUTABLE', { rule: true, vint: true });
    eq(v.maxCols, 33, 'after the vintage columns'); eq(cellV(v.snap, hdrRow, 33), 'Owner Rule'); eq(cellV(v.snap, hdrRow, 31), 'Quote As-Of (UTC)');
    eq(cellV(v.snap, rowOf(v.snap, 'ACT1.US'), 33), 'rule: yes');
    const both = render(newSource, {}, 'EXECUTABLE', { rule: true, live: true });
    eq(both.maxCols, 31); eq(both.liveNote, 'live-fill 2/5 (3 investable at source)'); eq(both.ruleNote, 'owner-rule 1/7 yes');
    eq(cellV(both.snap, rowOf(both.snap, 'ZTO.US'), 31), 'rule: no (conf unknown; ROI unknown)', 'the fill does not invent a band or ROI');
    // REAL refresh
    const r = refresh(newSource, { [RULE]: 'on' });
    eq(r.header.length, 31); eq(r.header[30], 'Owner Rule');
    ok(r.calls.some(x => x[0] === 'insertColumnsAfter' && x[1] === 'Top_10_Investments' && x[5] === 1), 'one column inserted on the live sheet');
    const a = seatRow(r, 'A.US'), b = seatRow(r, 'B.US'), wr = seatRow(r, 'W.US');
    deq([a[30], b[30], wr[30]], ['rule: yes', 'rule: no (Medium)', 'rule: no (Medium; ROI 34.8)'], 'per-seat cells');
    ok(/ \| owner-rule 4\/7 yes \| /.test(r.status), 'B2 token: ' + r.status);
    eq(r.status.replace(' | owner-rule 4/7 yes', ''), getOffRefresh().status, 'B2 = off line + the token');
    deq(r.posted, getOffRefresh().posted); deq(r.saved, getOffRefresh().saved);
    deq(a.slice(0, 30), seatRow(getOffRefresh(), 'A.US'), 'the 30 existing cells of A unchanged');
    const rb = refresh(baseSource, { [RULE]: 'on' });
    eq(rb.header.length, 30, 'GOLDEN NEGATIVE: the base renders 30 columns with the property on');
    eq(rb.status, getBaseRefresh().status);
  });

  test('T6 embedded dt10SelfTest: the three new core lines ok, property lines follow the properties, every base line kept, zero FAIL; the base lacks the lines', () => {
    const rep = String(N.dt10SelfTest()), repB = String(B.dt10SelfTest());
    ok(rep.indexOf('[DT10 v1.19.0] self-test') === 0, 'version line');
    for (const l of ['qualified hold (DT10_QUALIFIED_HOLD): off (off = v1.18.0 board; observe lists; enforce holds)', 'qualified hold core: ok',
      'seat live fill (DT10_SEAT_LIVE_FILL): off (off = v1.18.0 render)', 'seat live fill core: ok',
      'owner rule mark (DT10_OWNER_RULE_MARK): off (High + Engine ROI 12M < 34; display only)', 'owner rule core: ok']) {
      ok(rep.indexOf(l) > 0, 'line: ' + l); ok(repB.indexOf(l) < 0, 'GOLDEN NEGATIVE: the base lacks ' + l);
    }
    ok(rep.indexOf(': FAIL') < 0 && rep.indexOf('FAIL [') < 0, 'no FAIL');
    const baseLines = repB.split('\n'), newLines = rep.split('\n');
    const missing = baseLines.filter(l => l.indexOf('[DT10 v1.18.0]') !== 0 && newLines.indexOf(l) < 0);
    deq(missing, [], 'every base self-test line present');
    eq(newLines.length, baseLines.length + 6, 'exactly six lines added');
    const armed = String(loadMini(newSource, { [HOLD]: ' Enforce ', [LIVE]: 'ON', [RULE]: 'on' }).dt10SelfTest());
    ok(armed.indexOf('qualified hold (DT10_QUALIFIED_HOLD): enforce') > 0 && armed.indexOf('seat live fill (DT10_SEAT_LIVE_FILL): on') > 0 &&
       armed.indexOf('owner rule mark (DT10_OWNER_RULE_MARK): on') > 0 && armed.indexOf('FAIL') < 0, 'armed property lines');
    const faulted = String(loadMini(newSource, {}, { propsThrow: true }).dt10SelfTest());
    ok(faulted.indexOf('qualified hold (DT10_QUALIFIED_HOLD): off') > 0 && faulted.indexOf('seat live fill (DT10_SEAT_LIVE_FILL): off') > 0 &&
       faulted.indexOf('owner rule mark (DT10_OWNER_RULE_MARK): off') > 0, 'a property fault reads off');
  });

  test('T7 ES5 / ASCII lint of the diff; inventory: 9 functions and 5 vars added, 0 removed; 22 hunks whose reversal is the reviewed v1.18.0 file', () => {
    const baseLines = {}, bl = baseSource.split('\n'), nl = newSource.split('\n');
    for (const l of bl) baseLines[l] = true;
    const added = nl.filter(l => !baseLines[l]);
    ok(added.length >= 400 && added.length <= 600, 'added lines ' + added.length);
    for (const l of added) {
      const code = l.replace(/\/\/.*$/, '');
      ok(!/\b(let|const|class)\s|=>|`|\.\.\.|\.includes\(/.test(code) || /^\s*(\*|\/\*)/.test(l), 'ES6 token: ' + l);
      if (!/^[\x09\x20-\x7e]*$/.test(l)) ok(baseLines[l.replace('1.19.0', '1.18.0')] === true, 'non-ASCII added line: ' + l);
    }
    function nonAscii(s) { const m = {}; for (let j = 0; j < s.length; j++) { const c = s.charCodeAt(j); if (c > 126) m[c] = (m[c] || 0) + 1; } return m; }
    deq(nonAscii(newSource), nonAscii(baseSource), 'non-ASCII code point multiset unchanged');
    ok(newSource.indexOf('\r') < 0, 'LF only');
    const why = newSource.slice(newSource.indexOf(' * v1.19.0 (2026-11-01)'), newSource.indexOf(' * v1.18.0 (2026-10-27) [POST-EARNINGS]'));
    ok(why.length > 3000 && /^[\x09\x0a\x20-\x7e]*$/.test(why), 'WHY block present (newest first) and ASCII');
    ok(newSource.indexOf(baseSource.slice(baseSource.indexOf(' * v1.18.0 (2026-10-27) [POST-EARNINGS]'), baseSource.indexOf(' * v1.17.1 (2026-10-27) [SHADOW-MOMENTUM]'))) > 0, 'the v1.18.0 WHY block verbatim');
    for (const l of bl.filter(x => /^ \* v\d+\.\d+\.\d+ \(/.test(x))) ok(newSource.indexOf(l) > 0, 'WHY header lost: ' + l);
    const names = (src, re) => { const out = {}; let m; while ((m = re.exec(src))) out[m[1]] = true; return out; };
    const fb = names(baseSource, /function\s+([A-Za-z_$][\w$]*)\s*\(/g), fn = names(newSource, /function\s+([A-Za-z_$][\w$]*)\s*\(/g);
    deq(Object.keys(fb).filter(k => !fn[k]), [], 'functions removed (any level)');
    const tb = names(baseSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), tn = names(newSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    deq(Object.keys(tn).filter(k => !tb[k]).sort(), ['dt10OwnerRuleCell_', 'dt10OwnerRuleMarkOn_', 'dt10OwnerRuleNote_', 'dt10SeatLiveFillCore_', 'dt10SeatLiveFillOn_',
      'dt10SeatLiveFill_', 'dt10SeatLiveIndex_', 'dt10StabQualifiedHoldMode_', 'dt10StabQualifiedHoldSeat_'], 'top-level functions added');
    eq(Object.keys(tb).length, 224, 'base census'); eq(Object.keys(tn).length, 233, 'new census');
    const vb = names(baseSource, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = names(newSource, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq(Object.keys(vb).filter(k => !vn[k]), [], 'vars removed');
    deq(Object.keys(vn).filter(k => !vb[k]).sort(), ['DT10_OWNER_RULE_HEADERS', 'DT10_OWNER_RULE_PROP', 'DT10_OWNER_RULE_ROI_MAX', 'DT10_QUALIFIED_HOLD_PROP', 'DT10_SEAT_LIVE_FILL_PROP'], 'vars added');
    eq(Object.keys(vb).length, 121); eq(Object.keys(vn).length, 126);
    const newSet = {}; for (const l of nl) newSet[l] = true;
    deq(bl.filter(l => !newSet[l]).map(l => l.trim()), ["var DT10_VERSION = '1.18.0';", '* Version: 1.18.0 (see DT10_VERSION; header kept in lockstep — restored'].sort().reverse().sort(), 'only the two version lines left');
    const core = newSource.slice(newSource.indexOf('function dt10StabCore_('), newSource.indexOf('\n// ----', newSource.indexOf('function dt10StabCore_(')));
    for (const f of ['outageOf', 'hardOut', 'scoreOf', 'smoothed', 'boardSort', 'inFinal', 'inList', 'qualifiedWithin', 'softMiss']) ok(core.indexOf('function ' + f + '(') > 0, 'nested ' + f);
    for (const name of ['dt10StabQualifiedHoldSeat_', 'dt10StabQualifiedHoldMode_', 'dt10SeatLiveFillOn_', 'dt10SeatLiveIndex_', 'dt10SeatLiveFillCore_', 'dt10SeatLiveFill_',
      'dt10OwnerRuleMarkOn_', 'dt10OwnerRuleCell_', 'dt10OwnerRuleNote_']) {
      const start = newSource.indexOf('function ' + name + '(');
      const body = newSource.slice(start, newSource.indexOf('\n}\n', start) + 3);
      ok(start > 0 && /^[\x00-\x7f]*$/.test(body), 'ASCII ' + name);
      new vm.Script('(function(){' + body + '})');
    }
    const edits = require(path.join(ROOT, 'tests', 'js', 'dt10_edits_v1190.json'));
    eq(edits.length, 22, 'twenty-two hunks');
    eq(sha256(history.v1180Source(newSource)), history.V1180_SHA256, 'reversal = the reviewed v1.18.0 file');
  });

  test('T8 service reads: observe / enforce spellings -> modes; on spellings -> on; blank, junk, absent, fault -> off', () => {
    for (const [v, want] of [['observe', 'observe'], [' ENFORCE ', 'enforce'], ['Observe', 'observe'], ['', 'off'], ['off', 'off'], ['on', 'off'], ['yes', 'off'], ['1', 'off'], ['enforced', 'off']]) {
      eq(loadMini(newSource, { [HOLD]: v }).dt10StabQualifiedHoldMode_(), want, 'hold ' + JSON.stringify(v));
    }
    eq(loadMini(newSource, {}).dt10StabQualifiedHoldMode_(), 'off', 'hold absent');
    eq(loadMini(newSource, {}, { propsThrow: true }).dt10StabQualifiedHoldMode_(), 'off', 'hold fault');
    for (const [v, want] of [['on', true], [' ON ', true], ['On', true], ['', false], ['off', false], ['yes', false], ['1', false], ['onn', false], ['enforce', false]]) {
      eq(loadMini(newSource, { [LIVE]: v }).dt10SeatLiveFillOn_(), want, 'live ' + JSON.stringify(v));
      eq(loadMini(newSource, { [RULE]: v }).dt10OwnerRuleMarkOn_(), want, 'rule ' + JSON.stringify(v));
    }
    const f = loadMini(newSource, {}, { propsThrow: true });
    ok(f.dt10SeatLiveFillOn_() === false && f.dt10OwnerRuleMarkOn_() === false, 'faults read off');
    const k = N.dt10StabKnobs_({ 'T10: Stability Rank Buffer': 15 });
    ok(!Object.prototype.hasOwnProperty.call(k, 'qualified_hold'), 'the criteria snapshot never carries the knob');
    deq(k, B.dt10StabKnobs_({ 'T10: Stability Rank Buffer': 15 }), 'knob mapping identical to the base');
  });

  return sha256(JSON.stringify([getOffCore(), getOffE2E().saved, getOffRefresh().status, getOffRefresh().seats,
    core(N, knobs({ qualified_hold: 'enforce' })), render(newSource, {}, 'EXECUTABLE', { live: true, rule: true }).snap])).slice(0, 16);
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
console.log('V1190 T0-T8 x3 identical digest ' + digests[0] + ' | new SHA256 ' + sha256(newSource) + ' | base SHA256 ' + sha256(baseSource));
console.log('PASS ' + passed + '/' + total);
