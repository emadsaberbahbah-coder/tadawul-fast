/* tests/js/dt10_bulk_format.test.js -- 16_Decision_Top10.gs v1.13.11 BULK
 * RENDER FORMATTING. Full-source node vm harness. VERSION 1.0.0 (2026-10-22).
 *
 * WHY (21 Oct 2026, Apps Script execution log): the automatic Top-10 refresh
 * after the first decision-feed release reached 'upstream verdict: EXECUTABLE;
 * final board: HELD' at 23:48:42 Riyadh and was killed at 23:50:59 ('Exceeded
 * maximum execution time'). B2 stayed 'running', so the next scheduled
 * release check withdrew the release. The render painted formats cell by
 * cell: ~1,750 range calls for the 500-row CANDIDATES grid alone.
 *
 * The ORACLE is the reviewed v1.13.10 file itself, rebuilt from the delivered
 * source by reversing exactly the enumerated v1.13.11 edits and checked
 * against its SHA-256 (tests/js/dt10_cash_utc_history.js v11310Source). Both
 * sources run in separate vm contexts against the same cell-model sheet, so
 * every difference comes from the four changed helpers.
 *
 *   B1  dt10WriteTable_: 0/1/2/3/7/500 rows x 3/25/30 columns, odd rows
 *       pre-painted: identical cell state (values, backgrounds, font colours,
 *       weights, sizes, wrap, styles, number formats, merges, borders); the
 *       delivered call count does not grow with the row count
 *   B2  dt10ColorVerdicts_: every verdict class, empty/null/0 cells, full,
 *       partial and empty token sets, pre-existing zebra: identical; O(1) calls
 *   B3  dt10ColorQualified_: selected / deferred / ranked-out, sparse rows,
 *       token sets: identical; O(1) calls
 *   B4  dt10ColorStability_: ACTIVE/NEW/GRACE/FAST-TRACK/other/empty,
 *       after a grace tint: identical; O(1) calls
 *   B5  guards: firstDataRow < 0 or count 0 -> no call in either version;
 *       no token -> no format read or write
 *   B6  REAL dt10RenderPayload_ (EXECUTABLE and NOT_ACTIONABLE feed, 9 seats,
 *       3 qualified, 10 near-miss, 4 alerts, 500 audit rows, a dirty zone):
 *       identical final sheet; delivered calls < 1/5 of the oracle's
 *   B7  dt10BulkPaint_ unit cases (undefined token fields -> reset, no
 *       token -> zero calls, shape preserved)
 *   B8  version lockstep, WHY block, oracle integrity, ES5 shape of the
 *       changed functions, ASCII-only WHY
 *
 * Usage: node tests/js/dt10_bulk_format.test.js   (last line: PASS k/k)
 *   DT10_DELIVERED=<file> runs every case against another copy. */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {v11310Source} = require('./dt10_cash_utc_history');

const ROOT = path.join(__dirname, '../..');
const SOURCE_PATH = process.env.DT10_DELIVERED || path.join(ROOT, 'apps_script/16_Decision_Top10.gs');
const source = fs.readFileSync(SOURCE_PATH, 'utf8');
const oracleSource = v11310Source(source);

let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + String(e && e.stack || e).split('\n').slice(0, 8).join('\n  ')); }
}

/* ------------------------------------------------------------ cell model */
function makeBook() {
  const counts = {};
  const sheets = {};
  const book = {
    counts,
    bump(m) { counts[m] = (counts[m] || 0) + 1; },
    total() { return Object.keys(counts).reduce((s, k) => s + counts[k], 0); },
    getSheetByName(n) { return sheets[n] || null; },
    addSheet(name, maxRows, maxCols) { sheets[name] = makeSheet(book, name, maxRows, maxCols); return sheets[name]; }
  };
  return book;
}
function a1(ref) {
  const m = /^([A-Z]+)(\d+)(?::([A-Z]+)(\d+))?$/.exec(ref);
  if (!m) throw new Error('unsupported A1 ' + ref);
  const col = s => s.split('').reduce((n, ch) => n * 26 + ch.charCodeAt(0) - 64, 0);
  const r = +m[2], c = col(m[1]);
  return m[3] ? [r, c, +m[4] - r + 1, col(m[3]) - c + 1] : [r, c, 1, 1];
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
    insertColumnsAfter(_after, n) { book.bump('insertColumnsAfter'); maxCols += n; return sheet; },
    getRange(a, b, nr, nc) {
      const [r, c, h, w] = typeof a === 'string' ? a1(a) : [a, b, nr || 1, nc || 1];
      assert(r >= 1 && c >= 1 && h >= 1 && w >= 1, 'range shape ' + [r, c, h, w]);
      return makeRange(r, c, h, w);
    }
  };
  function makeRange(r, c, nr, nc) {
    const self = {};
    const all = (field, m) => v => { book.bump(m); each(r, c, nr, nc, (R, C) => upd(R, C, x => { x[field] = v; })); return self; };
    const mat = (field, m) => grid => {
      book.bump(m);
      assert(Array.isArray(grid) && grid.length === nr && grid.every(row => Array.isArray(row) && row.length === nc),
        m + ' shape ' + nr + 'x' + nc);
      each(r, c, nr, nc, (R, C, i, j) => upd(R, C, x => { x[field] = grid[i][j]; }));
      return self;
    };
    const read = (field, dflt, m) => () => {
      book.bump(m);
      return Array.from({length: nr}, (_, i) => Array.from({length: nc}, (_, j) => {
        const v = (cells.get(key(r + i, c + j)) || {})[field];
        return v === undefined || v === null ? dflt : v;
      }));
    };
    Object.assign(self, {
      setValue(v) { book.bump('setValue'); upd(r, c, x => { x.v = v; }); return self; },
      setValues: mat('v', 'setValues'), getValues: read('v', '', 'getValues'),
      setBackground: all('bg', 'setBackground'), setBackgrounds: mat('bg', 'setBackgrounds'),
      getBackgrounds: read('bg', '#ffffff', 'getBackgrounds'),
      setFontColor: all('fg', 'setFontColor'), setFontColors: mat('fg', 'setFontColors'),
      getFontColors: read('fg', '#000000', 'getFontColors'),
      setFontWeight: all('fw', 'setFontWeight'), setFontWeights: mat('fw', 'setFontWeights'),
      getFontWeights: read('fw', 'normal', 'getFontWeights'),
      setFontSize: all('fs', 'setFontSize'), setWrap: all('wrap', 'setWrap'),
      setFontStyle: all('fst', 'setFontStyle'), setNumberFormat: all('nf', 'setNumberFormat'),
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
/* Visual state: a reset/never-set format equals the GAS default it reads as. */
function snapshot(sheet) {
  const norm = (v, d) => (v === undefined || v === null ? d : String(v).toLowerCase());
  const out = {};
  for (const [k, x] of [...sheet._cells.entries()].sort()) {
    const cell = {v: x.v === undefined ? '' : x.v, bg: norm(x.bg, '#ffffff'), fg: norm(x.fg, '#000000'),
      fw: norm(x.fw, 'normal'), fs: x.fs === undefined ? null : x.fs, wrap: x.wrap === undefined ? null : x.wrap,
      fst: x.fst === undefined ? null : x.fst, nf: x.nf === undefined ? null : x.nf};
    if (cell.v === '' && cell.bg === '#ffffff' && cell.fg === '#000000' && cell.fw === 'normal' &&
        cell.fs === null && cell.wrap === null && cell.fst === null && cell.nf === null) continue;
    out[k] = cell;
  }
  return {cells: out, merges: [...sheet._merges].sort(), borders: [...sheet._borders].sort()};
}

/* ------------------------------------------------------------ vm contexts */
function load(src) {
  const ctx = {
    Logger: {log() {}},
    PropertiesService: {getScriptProperties() { return {getProperty() { return null; }, setProperty() {}, deleteProperty() {}}; }},
    Session: {getScriptTimeZone() { return 'Asia/Riyadh'; }},
    Utilities: {formatDate(d) { return new Date(d).toISOString().replace('T', ' ').slice(0, 19); }},
    SpreadsheetApp: {BorderStyle: {SOLID: 'SOLID'}, flush() {}, getActiveSpreadsheet() { return null; }}
  };
  vm.createContext(ctx);
  vm.runInContext(src, ctx, {filename: '16_Decision_Top10.gs'});
  return ctx;
}
const NEW = load(source), OLD = load(oracleSource);
const TOK = NEW.DT10_FALLBACK_TOKENS;
const PARTIAL = {VERDICT_POSITIVE: TOK.VERDICT_POSITIVE, VERDICT_WATCH: TOK.VERDICT_WATCH};

/* Run fn(ctx, sheet) on a fresh sheet per version; return both snapshots and call counts. */
function both(prepare, fn, dims) {
  const run = ctx => {
    const book = makeBook(), sheet = book.addSheet('Top_10_Investments', (dims && dims[0]) || 800, (dims && dims[1]) || 40);
    if (prepare) prepare(sheet);
    const before = book.total();
    fn(ctx, sheet);
    return {snap: snapshot(sheet), calls: book.total() - before, counts: Object.assign({}, book.counts)};
  };
  return {old: run(OLD), now: run(NEW)};
}
function grid(rows, cols, f) { return Array.from({length: rows}, (_, i) => Array.from({length: cols}, (_, j) => f(i, j))); }
function prepaintOdd(sheet, row, rows, cols) {
  for (let i = 1; i < rows; i += 2) sheet.getRange(row + i, 1, 1, cols).setBackground(i % 4 === 1 ? '#123456' : null);
}

/* ================================================================== B1 */
test('B1 dt10WriteTable_: identical cell state for 0/1/2/3/7/500 rows x 3/25/30 columns; delivered calls flat in rows', () => {
  const flat = [];
  for (const rows of [0, 1, 2, 3, 7, 500]) {
    for (const cols of [3, 25, 30]) {
      const headers = Array.from({length: cols}, (_, j) => 'H' + j);
      const data = grid(rows, cols, (i, j) => (j === 0 ? 'SYM' + i : i * 100 + j));
      const r = both(sheet => prepaintOdd(sheet, 21, rows, cols), (ctx, sheet) => {
        const res = ctx.dt10WriteTable_(sheet, 20, headers, data, 'empty text');
        assert.deepEqual(JSON.parse(JSON.stringify(res)), rows ? {next: 21 + rows, firstDataRow: 21, count: rows} : {next: 22, firstDataRow: -1, count: 0});
      });
      assert.deepEqual(r.now.snap, r.old.snap, rows + 'x' + cols + ' state');
      if (rows) flat.push(r.now.calls);
      if (rows >= 7) assert(r.old.calls >= Math.ceil(rows / 2), 'oracle paints per row: ' + r.old.calls);
    }
  }
  assert.equal(Math.max(...flat), Math.min(...flat), 'delivered call count independent of rows/cols: ' + flat);
  assert(Math.max(...flat) <= 12, 'delivered dt10WriteTable_ uses at most 12 calls: ' + flat[0]);
});

/* ================================================================== B2 */
const VERDICTS = ['INVEST', 'WATCH', 'DO_NOT_INVEST', 'HOLD', '', null, 0, '—', 'invest', 'BLOCKED'];
test('B2 dt10ColorVerdicts_: every verdict class, full/partial/empty tokens, zebra underneath: identical; O(1) calls', () => {
  for (const tokens of [TOK, PARTIAL, {}]) {
    for (const rows of [1, 10, 500]) {
      const vals = Array.from({length: rows}, (_, i) => VERDICTS[i % VERDICTS.length]);
      const headers = ['A', 'B', 'C', 'D', 'Verdict', 'F'];
      const data = grid(rows, 6, (i, j) => (j === 4 ? vals[i] : 'x' + i));
      const r = both(null, (ctx, sheet) => {
        const t = ctx.dt10WriteTable_(sheet, 30, headers, data, 'none');
        ctx.dt10ColorVerdicts_(sheet, t.firstDataRow, t.count, 5, tokens);
      });
      assert.deepEqual(r.now.snap, r.old.snap, 'tokens ' + Object.keys(tokens).length + ' rows ' + rows);
    }
    const colOnly = rows => both(null, (ctx, sheet) => {
      sheet.getRange(40, 5, rows, 1).setValues(Array.from({length: rows}, (_, i) => [VERDICTS[i % VERDICTS.length]]));
      ctx.dt10ColorVerdicts_(sheet, 40, rows, 5, tokens);
    });
    const small = colOnly(10), large = colOnly(500);
    assert.deepEqual(large.now.snap, large.old.snap);
    assert.equal(small.now.calls, large.now.calls, 'O(1): ' + small.now.calls + ' vs ' + large.now.calls);
    assert(large.now.calls <= 8, 'delivered verdict colouring calls: ' + large.now.calls);
    if (tokens === TOK) assert(large.old.calls > 1000, 'oracle per-cell calls: ' + large.old.calls);
  }
});

/* ================================================================== B3 */
test('B3 dt10ColorQualified_: selected / deferred / ranked-out / sparse rows, token sets: identical; O(1) calls', () => {
  const kinds = [{selected: true}, {deferral: 'sector cap'}, {}, null, {selected: false, deferral: ''}, {deferral: 0}, {selected: 'yes'}];
  for (const tokens of [TOK, PARTIAL, {}]) {
    for (const rows of [1, 3, 40]) {
      const qualified = Array.from({length: rows}, (_, i) => kinds[i % kinds.length]);
      const data = grid(rows, 15, (i, j) => (j === 13 ? 'flag' + i : i + '/' + j));
      const r = both(null, (ctx, sheet) => {
        const t = ctx.dt10WriteTable_(sheet, 50, ctx.DT10_QUAL_HEADERS, data, 'none');
        ctx.dt10ColorQualified_(sheet, t.firstDataRow, t.count, 14, qualified, tokens);
      });
      assert.deepEqual(r.now.snap, r.old.snap, 'tokens ' + Object.keys(tokens).length + ' rows ' + rows);
    }
  }
  const paint = rows => both(null, (ctx, sheet) => ctx.dt10ColorQualified_(sheet, 60, rows, 14,
    Array.from({length: rows}, (_, i) => kinds[i % kinds.length]), TOK));
  assert.equal(paint(5).now.calls, paint(300).now.calls, 'O(1)');
  assert(paint(300).old.calls >= 900, 'oracle per-cell');
});

/* ================================================================== B4 */
test('B4 dt10ColorStability_: ACTIVE/NEW/GRACE/FAST-TRACK/other/empty after a grace tint: identical; O(1) calls', () => {
  const states = ['ACTIVE 3/3', 'NEW', 'GRACE 1/2', 'FAST-TRACK 1/3', 'EXIT', '', null, 'active', 'GRACE - outage pause 1/5'];
  for (const tokens of [TOK, {VERDICT_WATCH: TOK.VERDICT_WATCH}, {}]) {
    for (const rows of [1, 9, 60]) {
      const selected = Array.from({length: rows}, (_, i) => ({_grace_hold: states[i % states.length] !== null &&
        String(states[i % states.length]).indexOf('GRACE') === 0}));
      const data = grid(rows, 30, (i, j) => (j === 24 ? states[i % states.length] : 'c' + i + '_' + j));
      const r = both(null, (ctx, sheet) => {
        const t = ctx.dt10WriteTable_(sheet, 25, ctx.DT10_SELECTED_HEADERS, data, 'none');
        ctx.dt10TintGraceRows_(sheet, t.firstDataRow, t.count, selected, tokens);
        ctx.dt10ColorStability_(sheet, t.firstDataRow, t.count, 25, tokens);
      });
      assert.deepEqual(r.now.snap, r.old.snap, 'tokens ' + Object.keys(tokens).length + ' rows ' + rows);
    }
  }
  const col = rows => both(null, (ctx, sheet) => {
    sheet.getRange(70, 25, rows, 1).setValues(Array.from({length: rows}, (_, i) => [states[i % states.length]]));
    ctx.dt10ColorStability_(sheet, 70, rows, 25, TOK);
  });
  assert.equal(col(9).now.calls, col(90).now.calls, 'O(1)');
});

/* ================================================================== B5 */
test('B5 guards: firstDataRow -1 / count 0 make no call; no token -> no format read or write', () => {
  for (const ctx of [OLD, NEW]) {
    const book = makeBook(), sheet = book.addSheet('S', 100, 30);
    ctx.dt10ColorVerdicts_(sheet, -1, 5, 5, TOK); ctx.dt10ColorVerdicts_(sheet, 10, 0, 5, TOK);
    ctx.dt10ColorQualified_(sheet, -1, 5, 14, [{selected: true}], TOK); ctx.dt10ColorQualified_(sheet, 10, 0, 14, [], TOK);
    ctx.dt10ColorStability_(sheet, -1, 5, 25, TOK); ctx.dt10ColorStability_(sheet, 10, 0, 25, TOK);
    assert.equal(book.total(), 0, 'no calls');
  }
  const book = makeBook(), sheet = book.addSheet('S', 100, 30);
  sheet.getRange(10, 5, 3, 1).setValues([[''], [null], [0]]);
  const before = Object.assign({}, book.counts);
  NEW.dt10ColorVerdicts_(sheet, 10, 3, 5, TOK);
  NEW.dt10ColorStability_(sheet, 10, 3, 5, TOK);
  const after = book.counts;
  for (const m of ['getBackgrounds', 'getFontColors', 'getFontWeights', 'setBackgrounds', 'setFontColors', 'setFontWeights']) {
    assert.equal(after[m] || 0, before[m] || 0, m + ' not called without a token');
  }
});

/* ================================================================== B6 */
function ticket(i) {
  const fast = i < 2;
  return {rank: i + 1, symbol: 'SYM' + i + '.US', name: 'Name ' + i, market: 'US', sector: 'Sector ' + (i % 3),
    currency: 'USD', fx_to_sar: 3.75, price: 10 + i, price_sar: (10 + i) * 3.75, entry_zone: '1-2',
    suggested_sar: fast ? '' : 0, suggested_shares: '', stop_sar: 30 + i, tp1_sar: 50 + i, tp2_sar: 60 + i,
    roi_pct: 0.1, engine_roi_pct: 0.2, ann_roi_pct: 0.3, exp_gain_12m_sar: 100, reliability: 80, dq: 95,
    confidence_band: 'HIGH', detail: {funds_from: 'Cash', review_date: '2026-10-29'},
    _stab_status: fast ? 'FAST-TRACK 1/3' : 'GRACE 1/2', _stab_days: 1, _stab_since: '2026-10-21',
    _stab_smoothed: 70, _stab_trend: 'flat', _grace_hold: !fast, advisor_note: 'note ' + i};
}
function cand(i) {
  const verdict = ['INVEST', 'WATCH', 'DO_NOT_INVEST', 'BLOCKED', ''][i % 5];
  return {symbol: 'C' + i + '.US', name: 'Cand ' + i, market: 'US', sector: 'S' + (i % 7), currency: 'USD',
    price: 1 + i, price_sar: (1 + i) * 3.75, roi_pct: 0.01 * (i % 30), engine_roi_pct: 0.02, ann_roi_pct: 0.03,
    rr: 1.5, reliability: 70, dq: 90, risk_level: 'Low', news_trend: '', sector_trend: '', conflict: i % 2 === 0,
    verdict, confidence_band: 'MID', opportunity_score: 1000 - i, first_fail: verdict === 'INVEST' ? null : {gate: 'Quote Freshness'},
    failure_reason: verdict === 'INVEST' ? '' : 'stale', structural_block: false, selected: i < 3, deferral: i === 5 ? 'sector cap' : ''};
}
function payload(state) {
  return {version: 'synthetic', status: 'ok', selected: Array.from({length: 9}, (_, i) => ticket(i)),
    candidates_rows: Array.from({length: 500}, (_, i) => cand(i)),
    near_miss: Array.from({length: 10}, (_, i) => ({symbol: 'N' + i, failed_gate: 'ROI', current: 1, required: 2,
      verdict: ['WATCH', 'DO_NOT_INVEST', 'INVEST', 'HOLD', ''][i % 5], improve_note: 'n' + i})),
    alerts: Array.from({length: 4}, (_, i) => ({type: 'T' + i, count: i, required_action: 'act ' + i})),
    kpis: {scanned: 9788, passed: 3, deployable_sar: 71374.54, max_selected: 10, selected_count: 9},
    meta: {route: 'v4.18.2', board_funding: {contract_version: 1, stage: 'research', finalized: true, eligible_symbols: []}},
    _dt10_uv_fixture: state};
}
function renderBoth(state) {
  const run = ctx => {
    const book = makeBook(), sheet = book.addSheet('Top_10_Investments', 700, 40);
    // a dirty zone from a previous board: values, formats and merges to clear
    sheet.getRange(18, 1, 5, 30).setValues(grid(5, 30, (i, j) => 'old' + i + j)).setBackground('#abcdef');
    sheet.getRange(18, 1, 1, 30).merge();
    const uv = state === 'EXECUTABLE'
      ? {state: 'EXECUTABLE', reason: '', ageMin: 206, source_readiness: {ready: true, cohort: '39642923547'}}
      : {state: 'NOT_ACTIONABLE', reason: 'final_publication', ageMin: 30, source_readiness: {ready: true, cohort: '39642923547'}};
    ctx.dt10BoardVerdict_ = () => JSON.parse(JSON.stringify(uv));
    ctx.dt10EarningsAnnotate_ = () => ({note: ''});
    const p = payload(state);
    const before = book.total();
    ctx.dt10RenderPayload_(sheet, p, TOK);
    return {snap: snapshot(sheet), calls: book.total() - before, counts: Object.assign({}, book.counts)};
  };
  const ctxOld = load(oracleSource), ctxNew = load(source);
  return {old: run(ctxOld), now: run(ctxNew)};
}
for (const state of ['EXECUTABLE', 'NOT_ACTIONABLE']) {
  test('B6 REAL dt10RenderPayload_ (' + state + ', 9 seats, 500 audit rows, dirty zone): identical sheet; delivered calls < 1/5 of v1.13.10', () => {
    const r = renderBoth(state);
    assert.deepEqual(r.now.snap.merges, r.old.snap.merges, 'merges');
    assert.deepEqual(r.now.snap.borders, r.old.snap.borders, 'borders');
    const keys = Object.keys(r.old.snap.cells);
    assert.deepEqual(Object.keys(r.now.snap.cells), keys, 'same painted cells');
    for (const k of keys) assert.deepEqual(r.now.snap.cells[k], r.old.snap.cells[k], 'cell ' + k);
    assert(keys.length > 12000, 'the render painted the whole cockpit: ' + keys.length + ' cells');
    assert(r.old.calls > 2000, 'v1.13.10 render calls: ' + r.old.calls);
    assert(r.now.calls * 5 < r.old.calls, 'v1.13.11 render calls ' + r.now.calls + ' vs v1.13.10 ' + r.old.calls);
    console.log('  render calls ' + state + ': v1.13.10 ' + r.old.calls + ' -> v1.13.11 ' + r.now.calls +
      ' (setBackground ' + (r.old.counts.setBackground || 0) + ' -> ' + (r.now.counts.setBackground || 0) + ')');
  });
}

/* ================================================================== B7 */
test('B7 dt10BulkPaint_: token rows get bg/fg/bold, undefined fields reset, null rows unchanged, no token -> zero calls', () => {
  const book = makeBook(), sheet = book.addSheet('S', 50, 10);
  sheet.getRange(2, 3, 4, 1).setBackground('#111111').setFontColor('#222222').setFontWeight('normal');
  const rng = sheet.getRange(2, 3, 4, 1);
  const before = book.total();
  NEW.dt10BulkPaint_(rng, [null, null, null, null]);
  assert.equal(book.total(), before, 'no token -> no read/write');
  NEW.dt10BulkPaint_(rng, [{bg: '#AAAAAA', fg: '#BBBBBB'}, null, {}, {bg: '#CCCCCC'}]);
  assert.equal(book.total() - before, 6, 'three reads + three writes');
  const s = snapshot(sheet).cells;
  assert.deepEqual([s['2:3'].bg, s['2:3'].fg, s['2:3'].fw], ['#aaaaaa', '#bbbbbb', 'bold']);
  assert.deepEqual([s['3:3'].bg, s['3:3'].fg, s['3:3'].fw], ['#111111', '#222222', 'normal'], 'null row unchanged');
  assert.deepEqual([s['4:3'].bg, s['4:3'].fg, s['4:3'].fw], ['#ffffff', '#000000', 'bold'], 'undefined fields reset to default');
  assert.deepEqual([s['5:3'].bg, s['5:3'].fg, s['5:3'].fw], ['#cccccc', '#000000', 'bold']);
});

/* ================================================================== B8 */
test('B8 version lockstep, WHY block, oracle integrity, ES5 shape and ASCII of the change', () => {
  assert.equal(NEW.DT10_VERSION, '1.14.0');
  assert.equal(OLD.DT10_VERSION, '1.13.10');
  assert.match(source.split('\n')[3], /^ \* Version: 1\.14\.0 /);
  assert(source.includes(' * v1.13.11 (2026-10-22) -- BULK RENDER FORMATTING (6-MINUTE LIMIT)\n'));
  assert(source.includes(' * v1.13.10 (2026-10-20) -- HOLDING SUBUNIT FX, NO NATIVE-AS-SAR, FAILED STATE\n'), 'prior WHY kept');
  const why = source.slice(source.indexOf(' * v1.13.11 (2026-10-22)'), source.indexOf(' * v1.13.10 (2026-10-20)'));
  assert(/^[\x09\x0a\x20-\x7e]*$/.test(why), 'WHY block is ASCII');
  for (const name of ['dt10WriteTable_', 'dt10BulkPaint_', 'dt10ColorVerdicts_', 'dt10ColorQualified_', 'dt10ColorStability_']) {
    const start = source.indexOf('function ' + name + '(');
    assert(start > 0, name);
    const body = source.slice(start, source.indexOf('\n}\n', start) + 3);
    assert(!/\b(let|const|class)\s|=>|`/.test(body), name + ' is ES5');
    assert.doesNotThrow(() => new vm.Script('(function(){' + body + '})'), name + ' compiles');
  }
  assert.equal((source.match(/^function dt10BulkPaint_\(/mg) || []).length, 1);
  assert.equal((oracleSource.match(/^function dt10BulkPaint_\(/mg) || []).length, 0, 'oracle is the pre-change file');
});

console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
