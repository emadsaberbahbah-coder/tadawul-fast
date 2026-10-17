/* 16_Decision_Top10.gs v1.19.0 (2026-10-17) -- Decision Pages v2 plan, package B1:
 * the Top_10 v2 layout behind Script Property T10_V2_LAYOUT (default off = the
 * v1.18.0 render byte-for-byte; the layered harnesses prove that on the
 * reverted source). Pure cores, the reversal layer, and the v2 render on a
 * stub workbook. Run: node tests/js/dt10_v1190_v2_layout.test.js */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {revertV1190, addedLines1190, EDITS_1190} = require('./dt10_v1190_history');
const {revertV1180} = require('./dt10_v1180_history');
const {revertV1170} = require('./dt10_v1170_history');
const {revertV1160} = require('./dt10_v1160_history');
const {revertV1150} = require('./dt10_v1150_history');
const {revertV1140} = require('./dt10_v1140_history');
const {feedHistorySource} = require('./dt10_cash_utc_history');

const ROOT = path.join(__dirname, '../..');
const source = fs.readFileSync(path.join(ROOT, 'apps_script/16_Decision_Top10.gs'), 'utf8');
const plain = v => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));
let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + (e && e.stack || e)); }
}

/* ---- stub workbook: cells in a map, chainable range setters, charts ---- */
function stubSheet(book, name) {
  const cells = new Map(), merges = [], charts = [], hidden = { v: false }, frozen = { v: 0 };
  let maxCols = 30, maxRow = 0, maxCol = 0;
  const key = (r, c) => r + ',' + c;
  const sheet = {};
  function range(r, c, n, w) {
    n = n || 1; w = w || 1;
    const self = new Proxy({}, { get(_, k) {
      if (k === 'a1') return name + '!' + r + ',' + c + ',' + n + ',' + w;
      if (k === 'getValues') return () => { const out = []; for (let i = 0; i < n; i++) { const row = []; for (let j = 0; j < w; j++) row.push(cells.has(key(r + i, c + j)) ? cells.get(key(r + i, c + j)) : ''); out.push(row); } return out; };
      if (k === 'getValue') return () => (cells.has(key(r, c)) ? cells.get(key(r, c)) : '');
      if (k === 'setValues') return (vals) => { for (let i = 0; i < vals.length; i++) for (let j = 0; j < vals[i].length; j++) { cells.set(key(r + i, c + j), vals[i][j]); maxRow = Math.max(maxRow, r + i); maxCol = Math.max(maxCol, c + j); } return self; };
      if (k === 'setValue') return (v) => { cells.set(key(r, c), v); maxRow = Math.max(maxRow, r); maxCol = Math.max(maxCol, c); return self; };
      if (k === 'merge') return () => { merges.push([r, c, n, w]); return self; };
      if (k === 'breakApart') return () => { for (let i = merges.length - 1; i >= 0; i--) { const m = merges[i]; if (m[0] >= r && m[0] < r + n) merges.splice(i, 1); } return self; };
      if (k === 'clear' || k === 'clearContent' || k === 'clearContents') return () => { for (let i = 0; i < n; i++) for (let j = 0; j < w; j++) cells.delete(key(r + i, c + j)); return self; };
      return () => self;   // every styling setter is chainable
    } });
    return self;
  }
  Object.assign(sheet, {
    name, cells, merges, charts, hidden, frozen,
    getName() { return name; }, getParent() { return book.ss; },
    getRange(a, b, n, w) {
      if (typeof a === 'string') { const m = a.match(/^([A-Z]+)(\d+):([A-Z]+)(\d+)$/); const col = s => s.split('').reduce((x, ch) => x * 26 + ch.charCodeAt(0) - 64, 0); return range(+m[2], col(m[1]), +m[4] - +m[2] + 1, col(m[3]) - col(m[1]) + 1); }
      return range(a, b, n, w);
    },
    getDataRange() { return range(1, 1, Math.max(1, maxRow), Math.max(1, maxCol)); },
    getMaxRows() { return 1000; }, getMaxColumns() { return maxCols; }, getLastRow() { return maxRow; }, getLastColumn() { return maxCol; },
    insertColumnsAfter(_, k) { maxCols += k; }, setFrozenRows(n) { frozen.v = n; }, hideSheet() { hidden.v = true; },
    setColumnWidths() {}, setColumnWidth() {}, clear() { cells.clear(); merges.length = 0; maxRow = 0; maxCol = 0; }, clearContents() { cells.clear(); maxRow = 0; maxCol = 0; },
    getCharts() { return charts.slice(); }, removeChart(ch) { const i = charts.indexOf(ch); if (i >= 0) charts.splice(i, 1); }, insertChart(ch) { charts.push(ch); },
    newChart() {
      const opts = {}; const b = { kind: '', ranges: [], pos: null,
        asColumnChart() { b.kind = 'column'; return b; }, asBarChart() { b.kind = 'bar'; return b; }, asLineChart() { b.kind = 'line'; return b; },
        addRange(rg) { b.ranges.push(rg.a1); return b; }, setNumHeaders() { return b; }, setOption(k, v) { opts[k] = v; return b; },
        setPosition(r, c) { b.pos = [r, c]; return b; },
        build() { return { kind: b.kind, ranges: b.ranges.slice(), pos: b.pos, opts, getOptions() { return { get(k) { return opts[k]; } }; } }; } };
      return b;
    },
    text(r, c) { return cells.has(key(r, c)) ? cells.get(key(r, c)) : ''; },
    rowsWith(pred) { const out = []; for (const [k, v] of cells) if (pred(v)) out.push(+k.split(',')[0]); return out.sort((a, b) => a - b); }
  });
  return sheet;
}
function stubBook(names) {
  const book = { sheets: {} };
  book.ss = { getSheetByName(n) { return book.sheets[n] || null; }, insertSheet(n) { book.sheets[n] = stubSheet(book, n); return book.sheets[n]; },
    getRangeByName() { return null; }, toast() {} };
  for (const n of names || []) book.ss.insertSheet(n);
  return book;
}
function load(props, book) {
  const store = Object.assign({}, props || {});
  const propsService = { getProperty(k) { return Object.prototype.hasOwnProperty.call(store, k) ? store[k] : null; },
    setProperty(k, v) { store[k] = String(v); return propsService; }, deleteProperty(k) { delete store[k]; return propsService; } };
  const ctx = {
    console, Logger: { log() {} }, Date,
    Utilities: { formatDate(d, tz, fmt) { return fmt === 'HH:mm' ? '08:07' : '2026-10-17 08:07:00'; }, sleep() {}, getUuid() { return 'u'; }, computeDigest() { return []; },
      base64Encode() { return ''; }, DigestAlgorithm: { MD5: 'MD5', SHA_256: 'SHA_256' }, Charset: { UTF_8: 'UTF_8' } },
    PropertiesService: { getScriptProperties() { return propsService; }, getDocumentProperties() { return null; }, getUserProperties() { return propsService; } },
    SpreadsheetApp: { getActiveSpreadsheet() { if (!book) throw new Error('no sheet'); return book.ss; }, getActive() { if (!book) throw new Error('no sheet'); return book.ss; }, flush() {},
      BorderStyle: { SOLID: 'SOLID', SOLID_THICK: 'SOLID_THICK' }, getUi() { throw new Error('no ui'); } },
    UrlFetchApp: { fetch() { throw new Error('no network'); } }, Session: { getScriptTimeZone() { return 'Asia/Riyadh'; } },
    CacheService: { getScriptCache() { return { get() { return null; }, put() {} }; } },
    LockService: { getScriptLock() { return { tryLock() { return true; }, releaseLock() {} }; } }
  };
  vm.createContext(ctx);
  vm.runInContext(source, ctx, { filename: '16_Decision_Top10.gs' });
  return ctx;
}
const FIX = () => ({
  status: 'ok', version: '1.30.0',
  kpis: { passed: 24, deployable_sar: 100000, scanned: 9786, selected_count: 0, max_selected: 10, pregate: { pool: 9786, eligible: 238, kept: 24 } },
  _dt10_uv: { state: 'EXECUTABLE' }, meta: { route: { request_id: 'abc', version: '4.18.1' }, versions: { opportunity_builder: '1.30.0' } },
  selected: [
    { rank: 1, symbol: 'SHG.US', name: 'Shinhan', sector: 'Financials', price: 84.3, currency: 'USD', roi_pct: 12.5, engine_roi_pct: 9.1, reliability: 72, dq: 85,
      suggested_shares: 0, suggested_sar: 0, stop_sar: '—', _stab_status: 'ACTIVE (day 3)', _stab_trend: 'improving', detail: { rr_tp2: 2.1 } },
    { rank: 2, symbol: 'KRP.US', name: 'Kimbell', sector: 'Energy', price: 14.7, currency: 'USD', roi_pct: 11.6, engine_roi_pct: 7.2, reliability: 68, dq: 80,
      suggested_shares: 120, suggested_sar: 6615, stop_sar: 52.3, _stab_status: 'ACTIVE (day 9)', detail: { rr: 1.9 } },
    { symbol: 'G.US', _grace_hold: true, _stab_status: 'GRACE (1/3 missed)' },
    { symbol: 'H.US', _grace_hold: true, _stab_status: 'GRACE (2/3 missed)' }],
  near_miss: [{ symbol: 'NM.US', failed_gate: 'Reliability', current: '61', required: '70', verdict: 'WATCH', improve_note: 'x' }],
  alerts: [{ type: 'capital_call', count: 3, required_action: 'Deposit' }, { type: 'missing_fx', count: 1, required_action: 'Add FX' }],
  candidates_rows: [
    { symbol: 'SHG.US', name: 'Shinhan', sector: 'Financials', verdict: 'INVEST', opportunity_score: 71.8, risk_level: 'Medium', cluster: 'Banks ×19', failure_reason: '[w52-observe] pos 19% | 1d +4.3%', selected: true },
    { symbol: 'KRP.US', name: 'Kimbell', sector: 'Energy', verdict: 'INVEST', opportunity_score: 70.1, risk_level: 'High', selected: true },
    { symbol: 'Q3.US', sector: 'Energy', verdict: 'INVEST', opportunity_score: 66 },
    { symbol: 'NM.US', sector: 'Tech', verdict: 'WATCH', opportunity_score: 60, first_fail: { gate: 'Reliability' } },
    { symbol: 'B1.US', sector: 'Tech', verdict: 'DO_NOT_INVEST', opportunity_score: 40, first_fail: { gate: 'Investability' } }]
});

test('V1 version lockstep 1.19.0, census 243/120, the 24 v2 functions and 14 vars exist, header WHY block', () => {
  const ctx = load();
  assert.equal(ctx.DT10_VERSION, '1.19.0');
  assert.match(source.split('\n')[3], /^ \* Version: 1\.19\.0 /);
  assert(source.includes(' * v1.19.0 (2026-10-17) -- TOP_10 V2 LAYOUT BEHIND T10_V2_LAYOUT (Decision Pages v2 plan, B1)'));
  const fns = new Set(); let m; const re = /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg; while ((m = re.exec(source))) fns.add(m[1]);
  const vars = new Set(); const rv = /^var\s+([A-Za-z_$][\w$]*)/mg; while ((m = rv.exec(source))) vars.add(m[1]);
  assert.equal(fns.size, 243); assert.equal(vars.size, 120);
  for (const f of ['dt10V2LayoutOn_', 'dt10V2FailChip_', 'dt10V2State_', 'dt10V2Blocker_', 'dt10V2RunTokens_', 'dt10V2Fmt_', 'dt10V2Pct_', 'dt10V2NavSar_',
    'dt10V2SeatFlags_', 'dt10V2SeatRows_', 'dt10V2AlertChips_', 'dt10V2Funnel_', 'dt10V2FunnelText_', 'dt10V2Universe_', 'dt10V2Concentration_', 'dt10V2HitRate_',
    'dt10V2PanelScan_', 'dt10V2ReadPanel_', 'dt10V2EnsureLayout_', 'dt10V2StatusBlock_', 'dt10V2PanelWrite_', 'dt10V2ChartsData_', 'dt10V2ChartsDraw_', 'dt10RenderPayloadV2_']) {
    assert.equal(typeof ctx[f], 'function', f);
  }
  assert.equal(ctx.DT10_V2_PROP, 'T10_V2_LAYOUT'); assert.equal(ctx.DT10_V2_SEAT_HEADERS.length, 10); assert.deepEqual(plain(ctx.DT10_V2_STATES), ['NOT ACTIONABLE', 'RESEARCH', 'ACTIONABLE']);
});
test('V2 reversal layer: fourteen edits once; the reverted file is v1.18.0 without any v2 symbol; chain to the pinned v1.13.8 digest; ES5 guard on every added line', () => {
  assert.equal(EDITS_1190.length, 14);
  const prior = revertV1190(source);
  assert(prior.includes("var DT10_VERSION = '1.18.0';") && !/dt10V2|dt10RenderPayloadV2_|T10_V2_LAYOUT/.test(prior));
  feedHistorySource(revertV1140(revertV1150(revertV1160(revertV1170(revertV1180(prior))))));
  assert.throws(() => revertV1190(source + "\nvar DT10_VERSION = '1.19.0';"), /exactly one approved v1\.19\.0 edit/);
  const bad = /\b(let|const)\b|=>|`|\bclass\b|\.\.\.|\.includes\(/;
  for (const line of addedLines1190()) { assert(!bad.test(line), 'ES5: ' + line); assert(!/[‘’“”\r]/.test(line), 'quotes/CR: ' + line); }
});
test('G1 gate: on for "1" / "on" (trimmed, any case), off otherwise and on a property fault; one memoized read per execution; the render forks only when on', () => {
  assert.equal(load().dt10V2LayoutOn_(), false);
  assert.equal(load({ T10_V2_LAYOUT: '1' }).dt10V2LayoutOn_(), true);
  assert.equal(load({ T10_V2_LAYOUT: ' On ' }).dt10V2LayoutOn_(), true);
  for (const v of ['0', 'off', 'true', 'yes', '']) assert.equal(load({ T10_V2_LAYOUT: v }).dt10V2LayoutOn_(), false, v);
  const ctx = load(); let reads = 0; ctx.PropertiesService.getScriptProperties = () => { reads++; return { getProperty() { return '1'; } }; };
  ctx.dt10PropMemoReset_(); ctx.dt10V2LayoutOn_(); ctx.dt10V2LayoutOn_(); ctx.dt10V2LayoutOn_();
  assert.equal(reads, 1);
  assert(source.includes("  if (dt10V2LayoutOn_()) return dt10RenderPayloadV2_(sheet, payload, tokens);   // v1.19.0 [B1]"));
});
test('P1 state chip + blocker: ACTIONABLE on an executable board, NOT ACTIONABLE on a withheld feed or blocked source, RESEARCH for held / unfunded / empty; run tokens', () => {
  const ctx = load();
  const exec = { _dt10_uv: { state: 'EXECUTABLE' }, kpis: { passed: 3 }, selected: [{ symbol: 'E', suggested_shares: 5, suggested_sar: 100 }] };
  assert.equal(ctx.dt10V2State_(exec), 'ACTIONABLE'); assert.equal(ctx.dt10V2Blocker_(exec), 'None — 1 executable ticket');
  const feed = { _dt10_uv: { state: 'NOT_ACTIONABLE', reason: 'verdict stale (500m old)' }, kpis: { passed: 3 }, selected: [{ symbol: 'E', suggested_shares: 5 }] };
  assert.equal(ctx.dt10V2State_(feed), 'NOT ACTIONABLE'); assert.equal(ctx.dt10V2Blocker_(feed), 'Feed not actionable: verdict stale (500m old)');
  const src = { _dt10_uv: { state: 'EXECUTABLE', source_blocked: true, source_readiness: { reason: 'Global_Markets aged' } }, kpis: {}, selected: [] };
  assert.equal(ctx.dt10V2State_(src), 'NOT ACTIONABLE'); assert.equal(ctx.dt10V2Blocker_(src), 'Source readiness withheld: Global_Markets aged');
  const held = { _dt10_uv: { state: 'EXECUTABLE' }, kpis: { passed: 3 }, selected: [{ symbol: 'G', _grace_hold: true }, { symbol: 'F', _ft_suspended: true }] };
  assert.equal(ctx.dt10V2State_(held), 'RESEARCH'); assert.equal(ctx.dt10V2Blocker_(held), 'Every seat is held (1 grace, 1 suspended/research) — no executable allocation today');
  const empty = { _dt10_uv: { state: 'EXECUTABLE' }, kpis: { passed: 0 }, selected: [] };
  assert.equal(ctx.dt10V2State_(empty), 'RESEARCH'); assert.equal(ctx.dt10V2Blocker_(empty), 'No candidate passed every gate under the current criteria');
  const unf = { _dt10_uv: { state: 'EXECUTABLE' }, kpis: { passed: 4 }, selected: [], meta: { board_funding: { finalized: true, stage: 'allocate', eligible_symbols: ['A'], reason: 'cash floor' } } };
  assert.equal(ctx.dt10V2State_(unf), 'RESEARCH'); assert.equal(ctx.dt10V2Blocker_(unf), 'Qualified names, no funded seat — cash floor');
  assert.equal(ctx.dt10V2RunTokens_({ meta: { route: { request_id: 'd2c0' }, run_id: '38486122061' } }, 'stab[strict]: 1 grace | epoch=2026-10-17/feed | STATE-EMPTY', '08:07'),
    'epoch 2026-10-17/feed · 08:07 Riyadh · v1.19.0 · req d2c0 · run 38486122061');
  assert.equal(ctx.dt10V2RunTokens_({}, 'stab: steady | epoch=2026-10-16/held(frozen)', ''), 'epoch 2026-10-16/held frozen · v1.19.0');
});
test('P2 seat rows: ten cells, never an em dash; research seat shows TP1 and [withheld] stop and the 5%-NAV reference; executable seat shows stop and size; grace collapses to one row; flags', () => {
  const ctx = load(); const p = FIX();
  const r = ctx.dt10V2SeatRows_(p, p.candidates_rows, 500000, { 'SHG.US': 4 });
  assert.equal(r.rows.length, 3); assert.equal(r.grace, 2); assert.equal(r.exec, 1);
  for (const row of r.rows) { assert.equal(row.length, 10); for (const cell of row) assert(String(cell).indexOf('—') < 0, 'em dash in ' + JSON.stringify(row)); }
  assert.deepEqual(plain(r.rows[0]), [1, 'SHG.US · Shinhan · Financials', 'RESEARCH · ACTIVE (day 3)', '84.30 USD', 'TP1 94.84 · stop [withheld]',
    'plan +12.5% / eng +9.1%', '2.10', '25,000 SAR', '72 · 85 · 71.8', 'earnings ≤4d · [w52-observe] pos 19% · risk Medium · cluster Banks ×19 · trend improving']);
  assert.deepEqual(plain(r.rows[1]), [2, 'KRP.US · Kimbell · Energy', 'EXECUTABLE · ACTIVE (day 9)', '14.70 USD', 'TP1 16.41 · stop 52.30 SAR',
    'plan +11.6% / eng +7.2%', '1.90', '6,615 SAR (120 sh)', '68 · 80 · 70.1', 'risk High']);
  assert.deepEqual(plain(r.rows[2]), ['', 'GRACE-HELD: G.US, H.US', 'GRACE (2)', '', 'no plan today', '', '', '', '', 'missed G.US 1/3, H.US 2/3']);
  const withheld = FIX(); withheld._dt10_uv = { state: 'NOT_ACTIONABLE', reason: 'x' };
  const w = ctx.dt10V2SeatRows_(withheld, withheld.candidates_rows, 0, null);
  assert.equal(w.exec, 0); assert.equal(w.rows[1][2], 'RESEARCH · ACTIVE (day 9)'); assert.equal(w.rows[1][4], 'TP1 16.41 · stop [withheld]'); assert.equal(w.rows[1][7], '5,000 SAR (of deployable; NAV unknown)');
  const none = ctx.dt10V2SeatRows_({ selected: [{ symbol: 'X' }], kpis: {} }, [], 0, null);
  assert.equal(none.rows[0][4], 'plan n/a'); assert.equal(none.rows[0][7], 'n/a (NAV unknown)'); assert.equal(none.rows[0][8], 'n/a · n/a · n/a');
  assert.equal(ctx.dt10V2NavSar_({ portfolio: { cash_available_sar: 1000, holdings: [{ value_sar: 2500.4 }, { value_sar: 'x' }] } }), 3500);
});
test('P3 funnel, universe, concentration, hit rate, chips', () => {
  const ctx = load(); const p = FIX();
  const f = ctx.dt10V2Funnel_(p, 1);
  assert.deepEqual(plain(f), [['Pool rows', 9786, ''], ['Eligible (pre-gate)', 238, 2.4], ['Kept (audited)', 24, 10.1], ['Passed all gates', 24, 100], ['Seated', 4, 16.7], ['Executable', 1, 25]]);
  assert.equal(ctx.dt10V2FunnelText_(f), '9,786 → 238 → 24 → 24 → 4 → 1');
  const f2 = ctx.dt10V2Funnel_({ kpis: {}, selected: [] }, 0);
  assert.equal(ctx.dt10V2FunnelText_(f2), '? → ? → ? → ? → 0 → 0');
  const u = ctx.dt10V2Universe_([{ Symbol: 'A', 'Investability Status': 'INVESTABLE', 'Source Page': 'Global_Markets' }, { Symbol: 'B', 'Investability Status': 'watchlist', 'Source Page': 'Global_Markets' },
    { Symbol: 'C', 'Investability Status': 'BLOCKED', Exchange: 'Tadawul' }, { Symbol: 'D', 'Source Page': 'Global_Markets' }, { Symbol: 'E', 'Investability Status': 'REVIEW', 'Source Page': 'Market_Leaders' }]);
  assert.deepEqual(plain(u), [['INVESTABLE', 1, 1], ['WATCHLIST', 1, 1], ['BLOCKED', 0, 1], ['REVIEW', 0, 1], ['(blank)', 1, 1]]);
  const q = ctx.dt10QualifiedFromCands_(p.candidates_rows);
  assert.deepEqual(plain(ctx.dt10V2Concentration_(q, p.selected)), [['Energy', 2, 1], ['Financials', 1, 1]]);
  const idx = { map: { 'Investability|1M|INVESTABLE': { n: 963, hit: 47.1 }, 'ALL|3M|ALL': { n: 100, hit: 50 }, 'Investability|1Y|INVESTABLE': { n: 10, hit: null } } };
  assert.deepEqual(plain(ctx.dt10V2HitRate_(idx)), [['1M', 47.1, 963], ['3M', 50, 100]]);
  assert.deepEqual(plain(ctx.dt10V2HitRate_(null)), []);
  assert.equal(ctx.dt10V2AlertChips_(p.alerts), 'capital_call ×3 · missing_fx ×1'); assert.equal(ctx.dt10V2AlertChips_([]), 'no alerts');
});
test('P4 panel: label scan finds a v1 fixed panel or a v2 panel anywhere; blanks resolve to _Lists_Config defaults then built-ins', () => {
  const ctx = load();
  const grid = [['TOP 10'], [], [], ['CONTROL PANEL'], ['T10: Max Selected', 7, '', 'T10: Period (Months)', '', '', 'Cash Available (SAR)', 3825]];
  const found = plain(ctx.dt10V2PanelScan_(grid));
  assert.deepEqual(found, { 'T10: Max Selected': 7, 'Cash Available (SAR)': 3825 });
  const book = stubBook(['Top_10_Investments']); const sh = book.sheets.Top_10_Investments;
  sh.getRange(40, 1, 1, 8).setValues([['T10: Max Selected', 6, '', 'T10: Required ROI %', 15, '', 'Pool Limit', '']]);
  const panel = plain(ctx.dt10V2ReadPanel_(sh, book.ss));
  assert.equal(panel['T10: Max Selected'], 6); assert.equal(panel['T10: Required ROI %'], 15); assert.equal(panel['Pool Limit'], 50000); assert.equal(panel['T10: Max Per Sector'], 3); assert.equal(panel['T10: Stability Enabled'], 'Yes');
  assert.equal(Object.keys(panel).length, ctx.DT10_PANEL.length);
  const none = plain(ctx.dt10V2ReadPanel_(null, book.ss)); assert.equal(none['Cash Available (SAR)'], 100000);
  // v1 preserve gains the same fallback: a v2 sheet (panel below) still yields the operator inputs on a switch back
  const kept = plain(ctx.dt10PanelPreserve_(sh)); assert.equal(kept['T10: Max Selected'], 6);
});
test('R1 fixed block: dt10V2EnsureLayout_ builds rows 1-4 on a blank or v1 sheet (A1 title, A2 Blocker:, A3 Run:, frozen 3) and is idempotent on the fingerprint; the fail chip writes NOT ACTIONABLE', () => {
  const ctx = load({ T10_V2_LAYOUT: '1' });
  const book = stubBook([]);
  const sh = ctx.dt10V2EnsureLayout_(book.ss, null);
  assert.equal(sh.text(1, 1), 'TOP 10 INVESTMENTS — DECISION'); assert.equal(sh.text(2, 1), 'Blocker:'); assert.equal(sh.text(3, 1), 'Run:'); assert.equal(sh.frozen.v, 3);
  assert.equal(sh.text(4, 1), 'Never run — use menu: Refresh Decision Top 10');
  sh.getRange(2, 2).setValue('keep me');
  assert.equal(ctx.dt10V2EnsureLayout_(book.ss, sh), sh); assert.equal(sh.text(2, 2), 'keep me', 'fingerprint present: no rebuild');
  ctx.dt10V2FailChip_(sh, true, 'HTTP 502');
  assert.equal(sh.text(1, 2), 'NOT ACTIONABLE'); assert.equal(sh.text(2, 2), 'Refresh failed: HTTP 502 (board kept from the last render)');
  const v1 = stubBook(['Top_10_Investments']).sheets.Top_10_Investments; v1.getRange(4, 1).setValue('CONTROL PANEL'); v1.getRange(14, 1).setValue('KPIs');
  const re = ctx.dt10V2EnsureLayout_(v1.getParent(), v1); assert.equal(re.text(3, 1), 'Run:'); assert.equal(re.text(14, 1), '');
});
test('R2 v2 render on the stub workbook: status block, KPI strip rows 5-6, seat header at row 8, DATA GAPS + chips under the seats, ALL QUALIFIED, CONTROL PANEL below it, FUNNEL with four charts, hidden _Charts_Data_T10 with the four blocks, candidates, footer, note', () => {
  const book = stubBook(['Top_10_Investments', '_Forecast_Scorecard']);
  book.sheets._Forecast_Scorecard.getRange(1, 1, 3, 13).setValues([
    ['As Of (Riyadh)', 'Horizon', 'Dimension', 'Cohort', 'n', 'Wins', 'Losses', 'Hit Rate %', 'Hit Lo %', 'Hit Hi %', 'Mean ROI %', 'Median ROI %', 'Tracker Version'],
    ['2026-10-17 06:00', '1M', 'Investability', 'INVESTABLE', 963, 453, 510, 47.1, 44, 50, 1.2, 0.8, '6.48.0'],
    ['2026-10-17 06:00', '3M', 'ALL', 'ALL', 11791, 5836, 5955, 49.5, 48.6, 50.4, 2.0, 1.5, '6.48.0']]);
  const ctx = load({ T10_V2_LAYOUT: '1', DT10_UPSTREAM_VERDICT: 'off' }, book);
  const sh = ctx.dt10V2EnsureLayout_(book.ss, book.sheets.Top_10_Investments);
  const payload = FIX();
  payload._dt10_v2 = { panel: ctx.dt10V2ReadPanel_(null, book.ss), universe: ctx.dt10V2Universe_([{ Symbol: 'A', 'Investability Status': 'INVESTABLE', 'Source Page': 'Global_Markets' }]),
    stab_note: 'stab: steady | epoch=2026-10-17/feed', nav_sar: 500000 };
  const tokens = ctx.dt10Tokens_(book.ss);
  const earn = ctx.dt10RenderPayload_(sh, payload, tokens);   // forks to V2
  assert.equal(typeof earn, 'object');
  assert(ctx.DT10_V2_STATES.indexOf(sh.text(1, 2)) >= 0, 'chip is one of the enum: ' + sh.text(1, 2));
  assert.equal(sh.text(1, 2), 'NOT ACTIONABLE', 'the stub workbook has no _Status tab: source readiness withholds');
  assert(/^(Source readiness withheld|Feed not actionable)/.test(String(sh.text(2, 2))), sh.text(2, 2));
  assert.equal(sh.text(3, 2), 'epoch 2026-10-17/feed · 08:07 Riyadh · v1.19.0 · req abc');
  assert.equal(sh.text(5, 1), ctx.DT10_KPI_LABELS[0]); assert.equal(sh.text(6, 6), 9786, 'Scanned KPI');
  const seatHdr = sh.rowsWith(v => v === '#')[0];
  const evRow = sh.rowsWith(v => typeof v === 'string' && v.indexOf('Seats: SHG.US') === 0)[0];
  assert(evRow >= 8 && evRow < seatHdr, 'MODEL EVIDENCE strip (L3) sits between row 8 and the seats: ' + evRow);
  assert(seatHdr > 8 && seatHdr < 16, 'seat header row ' + seatHdr);
  assert.deepEqual(plain(sh.getRange(seatHdr, 1, 1, 10).getValues()[0]), plain(ctx.DT10_V2_SEAT_HEADERS), 'ten-column seat header');
  assert(String(sh.text(seatHdr - 1, 1)).indexOf('SELECTED') >= 0 || String(sh.text(seatHdr - 1, 1)).indexOf('FEED') >= 0, 'section title above the header: ' + sh.text(seatHdr - 1, 1));
  const seatRows = sh.getRange(seatHdr + 1, 1, 3, 10).getValues();
  for (const row of seatRows) for (const cell of row) assert(String(cell).indexOf('—') < 0, 'em dash in the seat zone');
  assert.equal(seatRows[2][2], 'GRACE (2)');
  const gapsRow = sh.rowsWith(v => typeof v === 'string' && v.indexOf('WHY CANDIDATES DIDN') === 0)[0];
  const chipsRow = sh.rowsWith(v => typeof v === 'string' && /^ALERTS \(\d+\): .*missing_fx \u00d71/.test(v))[0];   // finalize drops the funding solicitation on a research board
  const qualRow = sh.rowsWith(v => typeof v === 'string' && v.indexOf('ALL QUALIFIED') === 0)[0];
  const panelRow = sh.rowsWith(v => typeof v === 'string' && v.indexOf('CONTROL PANEL') === 0)[0];
  const funnelRow = sh.rowsWith(v => typeof v === 'string' && v.indexOf('FUNNEL — 9,786 → 238 → 24 → 24 → 4 → 0') === 0)[0];
  const nmRow = sh.rowsWith(v => typeof v === 'string' && v.indexOf('NEAR MISS') === 0)[0];
  const candRow = sh.rowsWith(v => typeof v === 'string' && v.indexOf('CANDIDATES — FULL AUDIT') === 0)[0];
  assert(gapsRow > seatHdr + 3 && chipsRow > gapsRow && qualRow > chipsRow && panelRow > qualRow && funnelRow > panelRow && nmRow >= funnelRow + ctx.DT10_V2_CHART_ROWS && candRow > nmRow,
    [gapsRow, chipsRow, qualRow, panelRow, funnelRow, nmRow, candRow].join(' '));
  assert.equal(sh.text(panelRow + 1, 1), 'T10: Universe Scope'); assert.equal(sh.text(panelRow + 1, 2), 'All Main Sheets'); assert.equal(sh.text(panelRow + 6, 8), 100000, 'Cash Available re-seeded');
  const data = book.sheets._Charts_Data_T10;
  assert(data && data.hidden.v, 'charts data tab hidden');
  assert.deepEqual(plain(data.getRange(1, 1, 2, 3).getValues()), [['Stage', 'Count', 'Conversion %'], ['Pool rows', 9786, '']]);
  assert.deepEqual(plain(data.getRange(1, 5, 2, 3).getValues()), [['Investability', 'Global_Markets', 'All pages'], ['INVESTABLE', 1, 1]]);
  assert.deepEqual(plain(data.getRange(1, 9, 2, 3).getValues()), [['Sector', 'Qualified', 'Seats'], ['Energy', 2, 1]]);
  assert.deepEqual(plain(data.getRange(1, 13, 3, 3).getValues()), [['Horizon', 'Hit Rate %', 'n'], ['1M', 47.1, 963], ['3M', 49.5, 11791]]);
  assert.equal(sh.charts.length, 4);
  assert.deepEqual(sh.charts.map(c => c.kind), ['column', 'column', 'bar', 'line']);
  assert.deepEqual(sh.charts.map(c => c.opts.title), ['TFB_T10 Funnel', 'TFB_T10 Universe (Investability Status)', 'TFB_T10 Concentration (sector)', 'TFB_T10 Hit rate by horizon']);
  assert.deepEqual(plain(sh.charts[3].opts.colors), ['#2a78d6']); assert.equal(sh.charts[3].opts.lineWidth, 2);
  assert.deepEqual(plain(sh.charts[0].pos), [funnelRow + 1, 1]); assert.deepEqual(plain(sh.charts[3].pos), [funnelRow + 1, 22]);
  assert.equal(payload._dt10_v2_note, 'v2 layout: 4 charts');
  // second render: charts replaced, not stacked; panel edit survives via the scan
  sh.getRange(panelRow + 1, 5).setValue(6);   // T10: Max Selected (slot 2 value cell)
  payload._dt10_v2.panel = ctx.dt10V2ReadPanel_(sh, book.ss);
  ctx.dt10RenderPayload_(sh, payload, tokens);
  assert.equal(sh.charts.length, 4);
  const panelRow2 = sh.rowsWith(v => typeof v === 'string' && v.indexOf('CONTROL PANEL') === 0)[0];
  assert.equal(sh.text(panelRow2 + 1, 5), 6, 'operator edit read back by the label scan');
  assert.equal(sh.text(3, 1), 'Run:', 'frozen block untouched by the zone clear');
});
test('R3 refresh wiring: v2 branches in refreshDecisionTop10 (scan before ensure, A4 status cell, panel from the scan, render inputs, status token) and the four fail chips', () => {
  assert(source.includes('  var v2 = dt10V2LayoutOn_();\n  var v2Panel = v2 ? dt10V2ReadPanel_(sheet, ss) : null;\n  if (v2) sheet = dt10V2EnsureLayout_(ss, sheet);\n'));
  assert(source.includes("  if (!v2 && (!sheet || sheet.getRange(DT10_ROW_PANEL_HEAD, 1).getValue() === '' ||"));
  assert(source.includes('  var statusCell = v2 ? sheet.getRange(DT10_V2_ROW_STATUS, 1) :   // v1.19.0 [B1]: A4 in v2'));
  assert(source.includes('  var panel = v2 ? v2Panel : dt10ReadPanel_(sheet, ss);   // v1.19.0 [B1]'));
  assert(source.includes("    payload._dt10_v2 = { panel: panel, universe: pool ? dt10V2Universe_(pool.rows) : [],"));
  assert(source.includes("      (v2Note ? ' | ' + v2Note : '') +                 // v1.19.0 [B1]"));
  assert.equal((source.match(/dt10V2FailChip_\(sheet, v2, '/g) || []).length, 4, 'four call sites');
  assert(source.includes("    if (!anyFixed) out = dt10V2PanelScan_(sheet.getDataRange().getValues());   // v1.19.0 [B1]: v2 panel below"));
});
test('S1 dt10SelfTest prints the v2 core line and the property state', () => {
  const rep = load({}, stubBook(['Top_10_Investments'])).dt10SelfTest();
  assert(rep.includes('v2 layout core: ok | layout (T10_V2_LAYOUT): v1 (default)'), rep.split('\n').filter(l => l.indexOf('v2 layout') >= 0).join('\n'));
  assert(load({ T10_V2_LAYOUT: '1' }, stubBook(['Top_10_Investments'])).dt10SelfTest().includes('v2 layout core: ok | layout (T10_V2_LAYOUT): v2 ON'));
});

console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
