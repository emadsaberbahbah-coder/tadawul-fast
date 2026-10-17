/* tests/js/decision_input_truth.test.js -- full-source node vm harness for
 * 16_Decision_Top10.gs v1.13.9 [WP-8 COCKPIT INPUT TRUTH].
 * (2026-10-14, v1.13.9: cash-label and UTC adapter pins; no census change.)
 * (2026-10-13, v1.13.8: pins moved 1.13.7 -> 1.13.8; census +2 functions
 *  dt10FeedCohortBindOn_ / dt10FeedCohortCheck_, +1 var
 *  DT10_FEED_COHORT_BIND_PROP; T8 also needs the two feed cohort lines.
 *  The binding itself is proved by tests/js/dt10_feed_cohort_binding.test.js.)
 *
 * Executes the COMPLETE delivered Apps Script source (and, with --base, the
 * pristine v1.13.3 file in a second context) under service stubs whose every
 * sheet write is appended to a WRITE LOG, then drives the REAL renderer
 * (dt10RenderPayload_), the REAL orchestrator (refreshDecisionTop10) and the
 * REAL dt10SelfTest on a payload rebuilt from the 2026-10-10 export fixture
 * (tests/fixtures/dt10_candidates_2026-10-10.json -- market data only).
 *
 *   T1  mode readers (off|observe|enforce, fail-closed, memoized)
 *   T2  evaluability core on the 500 exported audit rows
 *   T3  panel-vs-reference core (seed / built-in / decision / policy)
 *   T4  DEFAULT OFF IS BYTE-IDENTICAL: write log, read log, payload object
 *       base (v1.13.3) vs delivered (v1.13.9); delivered idempotent
 *   T5  observe = tokens only (write log identical to off)
 *   T6  enforce = KPI L15:M16, hidden _T10_Candidates, vintage columns,
 *       footer = 75 + alerts.length (pointer path) <= 80
 *   T7  fail-open: a tab write fault keeps today's in-cockpit grid
 *   T8  REAL dt10SelfTest (fresh context) -- every 'core: ok', zero FAIL
 *   T9  static: version lockstep, function census, ES5 guard, smart quotes
 *   T10 _Run_Log INPUT-TRUTH row (observe/enforce only)
 *   T11 REAL refreshDecisionTop10: status line / _Run_Log / _Selection_Log
 *       identical base vs delivered when off; tokens + one row when observe
 *
 * The clock is frozen at the fixture's run stamp (2026-10-10T19:28:22Z) so
 * the RUN-DIGEST printed last is identical on every run (x3).
 *
 * Usage: node tests/js/decision_input_truth.test.js [--base <v1.13.3 file>]
 *        [--delivered <file>]  (CI runs without --base; dual-tree cases then
 *        print SKIP and the single-tree determinism cases still run) */
'use strict';
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {feedHistorySource} = require('./dt10_cash_utc_history');
const {revertV1140} = require('./dt10_v1140_history');   // v1.14.0 reversal layer

const ROOT = path.join(__dirname, '../..');
const args = process.argv.slice(2);
function arg(k) { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : null; }
const DELIVERED_PATH = arg('--delivered') || path.join(ROOT, 'apps_script/16_Decision_Top10.gs');
const BASE_PATH = arg('--base');
const source = fs.readFileSync(DELIVERED_PATH, 'utf8');
const historySource = feedHistorySource(revertV1140(source));
const baseSource = BASE_PATH ? fs.readFileSync(BASE_PATH, 'utf8') : null;
const FIX = JSON.parse(fs.readFileSync(path.join(__dirname, '../fixtures/dt10_candidates_2026-10-10.json'), 'utf8'));
const PAGES = JSON.parse(fs.readFileSync(path.join(__dirname, '../fixtures/dt10_pages_2026-10-05.json'), 'utf8'));
const NOW = Date.parse(FIX.frozen_now);            // 2026-10-10T19:28:22Z
const TODAY_UTC = FIX.frozen_now.slice(0, 10);     // dt10StabToday_ pin
const copy = v => v === undefined ? undefined : JSON.parse(JSON.stringify(v));
// vm-realm objects carry a foreign prototype: compare by JSON shape.
const deq = (a, b, msg) => assert.deepEqual(copy(a), copy(b), msg);
const digestParts = [];
let passed = 0, skipped = 0;
function check(name, fn) { fn(); passed++; console.log('PASS ' + name); }
function skip(name) { skipped++; console.log('SKIP ' + name + ' (no --base)'); }

/* ------------------------------------------------------------------ stubs */
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
  const grid = opts.grid || null;
  const sh = {
    _name: name, _cells: cells, _a1: opts.a1 || {},
    _val(r, c) { const k = r + ':' + c; return Object.prototype.hasOwnProperty.call(cells, k) ? cells[k] : ''; },
    _set(r, c, v) { cells[r + ':' + c] = v; },
    _log(method, r, c, nr, nc, values) {
      env.calls.push([method, name, r, c, nr, nc, values === undefined ? null : JSON.stringify(values)]);
    },
    getName() { return name; },
    getParent() { return env.ss; },
    getRange(a, b, nr, nc) {
      if (typeof a === 'string') return mkRange(sh, 1, 1, 60, 2, a);
      return mkRange(sh, a, b, nr, nc, null);
    },
    getDataRange() { return mkRange(sh, 1, 1, grid ? grid.length : 1, grid ? grid[0].length : 1, null); },
    getLastRow() {
      if (grid) return grid.length;
      let m = 0; for (const k of Object.keys(cells)) m = Math.max(m, +k.split(':')[0]); return m;
    },
    getLastColumn() {
      if (grid) return grid[0].length;
      let m = 0; for (const k of Object.keys(cells)) m = Math.max(m, +k.split(':')[1]); return m;
    },
    getMaxRows() { return opts.maxRows || 1000; },
    getMaxColumns() { return opts.maxCols || 26; },
    appendRow(row) { sh._log('appendRow', 0, 0, 1, row.length, row); env.appended[name] = (env.appended[name] || []); env.appended[name].push(copy(row)); return sh; },
    insertRows(a, n) { sh._log('insertRows', a, 0, n || 1, 0); return sh; },
    insertRowsAfter(a, n) { sh._log('insertRowsAfter', a, 0, n, 0); return sh; },
    insertColumnsAfter(a, n) { sh._log('insertColumnsAfter', 0, a, 0, n); return sh; },
    deleteRows(a, n) { sh._log('deleteRows', a, 0, n, 0); return sh; },
    setColumnWidth(c, w) { sh._log('setColumnWidth', 0, c, 0, 0, w); return sh; },
    setFrozenRows(n) { sh._log('setFrozenRows', n, 0, 0, 0); return sh; },
    autoResizeColumns() { sh._log('autoResizeColumns', 0, 0, 0, 0); return sh; },
    hideSheet() { sh._log('hideSheet', 0, 0, 0, 0); hidden = true; return sh; },
    showSheet() { sh._log('showSheet', 0, 0, 0, 0); hidden = false; return sh; },
    isSheetHidden() { return hidden; },
    clear() { sh._log('sheet.clear', 0, 0, 0, 0); for (const k of Object.keys(cells)) delete cells[k]; return sh; },
    clearContents() { sh._log('sheet.clearContents', 0, 0, 0, 0); for (const k of Object.keys(cells)) delete cells[k]; return sh; },
    getSheetId() { return 1; }
  };
  if (opts.seed) for (const k of Object.keys(opts.seed)) cells[k] = opts.seed[k];
  if (grid) sh._grid = grid;
  return sh;
}
function mkRange(sheet, r, c, nr, nc, a1) {
  const env = sheet.getParent()._env;
  const log = (method, values) => sheet._log(method, r, c, nr || 1, nc || 1, values);
  function fault(method) {
    const f = env.failWrite;
    if (!f || f.sheet !== sheet._name || f.method !== method || (f.r !== undefined && f.r !== r) || (f.c !== undefined && f.c !== c)) return null;
    if (f.skip) { f.skip--; return null; }
    env.failWrite = null; return f;
  }
  const self = {
    getValue() { env.reads.push(['getValue', sheet._name, r, c]); return sheet._val(r, c); },
    getValues() {
      env.reads.push(['getValues', sheet._name, a1 || r, a1 ? 0 : c, nr || 1, nc || 1]);
      if (env.onReadValues) env.onReadValues(sheet, r, c, nr || 1, nc || 1);
      if (a1 && sheet._a1[a1]) return copy(sheet._a1[a1]);
      if (sheet._grid) return copy(sheet._grid);
      const out = [];
      for (let i = 0; i < (nr || 1); i++) { const row = []; for (let j = 0; j < (nc || 1); j++) row.push(sheet._val(r + i, c + j)); out.push(row); }
      return out;
    },
    setValue(v) { log('setValue', v); if (fault('setValue')) throw new Error('synthetic write fault'); sheet._set(r, c, v); return self; },
    setValues(vv) {
      log('setValues', vv);
      const f = fault('setValues');
      const limit = f ? (f.partialRows || 0) : vv.length;
      for (let i = 0; i < limit; i++) for (let j = 0; j < vv[i].length; j++) sheet._set(r + i, c + j, vv[i][j]);
      if (f) throw new Error('synthetic partial write fault');
      return self;
    },
    getNumRows() { return nr || 1; }, getNumColumns() { return nc || 1; },
    getRow() { return r; }, getColumn() { return c; }
  };
  for (const m of ['setNumberFormat', 'setNumberFormats', 'setWrap', 'setFontSize', 'setBackground', 'setBackgrounds',
    'setFontColor', 'setFontColors', 'setFontWeight', 'setFontWeights', 'setFontStyle', 'setHorizontalAlignment',
    'setBorder', 'clear', 'clearContent', 'clearFormat', 'merge', 'breakApart', 'setNote']) {
    self[m] = (...a) => { log(m, a); return self; };
  }
  return self;
}
const STATUS_HEADER = ['Page', 'Last Updated', 'Status', 'Message', 'Endpoint', 'HTTP Code', 'Rows', 'Columns', 'Duration ms', 'Warnings'];
function makeEnv(opts) {
  opts = opts || {};
  const props = Object.assign({}, opts.props || {});
  const env = { calls: [], reads: [], appended: {}, propReads: { total: 0, byKey: {} }, props, sheets: {}, options: opts, failWrite: null };
  const ss = {
    _env: env,
    getSheetByName(n) { return env.sheets[n] || null; },
    getRangeByName(n) {
      env.reads.push(['getRangeByName', n]);
      if (n === 'TFB_PANEL_DEFAULTS') return { getValues() { return copy(FIX.lists_defaults); } };
      if (n === 'TFB_FX_LOOKUP') return { getValues() { return copy(FIX.fx_rows); } };
      return null;
    },
    getSheets() { return Object.keys(env.sheets).map(k => env.sheets[k]); },
    getId() { return 'stub'; }, getSpreadsheetTimeZone() { return 'Asia/Riyadh'; },
    insertSheet(n) {
      env.calls.push(['insertSheet', n, 0, 0, 0, 0, null]);
      if (opts.insertSheetThrows) throw new Error('synthetic insertSheet fault');
      env.sheets[n] = mkSheet(env, n, { maxRows: 1000, maxCols: 26 }); return env.sheets[n];
    },
    setActiveSheet(sh) { env.calls.push(['setActiveSheet', sh && sh.getName(), 0, 0, 0, 0, null]); return ss; }
  };
  env.ss = ss;
  env.sheets['Top_10_Investments'] = mkSheet(env, 'Top_10_Investments', { maxRows: 800, maxCols: 118, seed: { '4:1': 'CONTROL PANEL', '14:1': 'KPIs' } });
  env.sheets['_Status'] = mkSheet(env, '_Status', { grid: [STATUS_HEADER].concat(['Market_Leaders', 'Global_Markets', 'Commodities_FX', 'Mutual_Funds'].map(p =>
    [p, '2026-10-10 21:19:47+03:00', 'SUCCESS', 'leg=success run=' + FIX.export_run, 'backend:run_dashboard_sync', '', 0, 115, 0, 0])),
    a1: { 'L1:M60': copy(FIX.status_lm) } });
  env.sheets['_Run_Log'] = mkSheet(env, '_Run_Log', {});
  env.sheets['Portfolio_Decision'] = opts.noPortfolioDecision ? undefined : mkSheet(env, 'Portfolio_Decision', { grid: copy(FIX.pd_block) });
  if (opts.noPortfolioDecision) delete env.sheets['Portfolio_Decision'];
  env.sheets['Calendar_Events'] = mkSheet(env, 'Calendar_Events', { grid: copy(FIX.calendar_rows) });
  env.sheets['My_Portfolio'] = mkSheet(env, 'My_Portfolio', { grid: [copy(PAGES.Global_Markets.header)] });
  for (const pg of ['Market_Leaders', 'Global_Markets', 'Commodities_FX', 'Mutual_Funds']) {
    env.sheets[pg] = mkSheet(env, pg, { grid: [PAGES[pg].header].concat(PAGES[pg].rows) });
  }
  const propsService = {
    getProperty(k) { env.propReads.total++; env.propReads.byKey[k] = (env.propReads.byKey[k] || 0) + 1; return Object.prototype.hasOwnProperty.call(props, k) ? props[k] : null; },
    setProperty(k, v) { props[k] = String(v); return propsService; },
    deleteProperty(k) { delete props[k]; return propsService; },
    getProperties() { return props; }
  };
  const ctx = {
    console, Logger: { log() {} }, Date: FixedDate,
    Utilities: { formatDate: fmtDate, sleep() {}, getUuid() { env.uuidCount = (env.uuidCount || 0) + 1; return 'test-uuid-' + env.uuidCount; }, computeDigest(alg, value) { return [...crypto.createHash(alg === 'SHA_256' ? 'sha256' : 'md5').update(value).digest()]; }, base64Encode() { return ''; }, DigestAlgorithm: { MD5: 'MD5', SHA_256: 'SHA_256' }, Charset: { UTF_8: 'UTF_8' } },
    PropertiesService: {
      getScriptProperties() { if (env.options.propsThrow) throw new Error('synthetic property outage'); return propsService; },
      getDocumentProperties() { return null; },
      getUserProperties() { return propsService; }
    },
    SpreadsheetApp: { getActiveSpreadsheet() { return ss; }, getActive() { return ss; }, getUi() { throw new Error('no UI'); }, openById() { return ss; }, flush() {}, BorderStyle: { SOLID: 'SOLID' } },
    UrlFetchApp: { fetch() { throw new Error('no network'); } },
    Session: { getScriptTimeZone() { return 'Asia/Riyadh'; } },
    CacheService: { getScriptCache() { return { get() { return null; }, put() {} }; } },
    LockService: { getScriptLock() { return { tryLock() { return !env.options.lockHeld; }, releaseLock() { env.lockReleases = (env.lockReleases || 0) + 1; } }; } }
  };
  env.ctx = ctx;
  return env;
}
function loadTree(src, opts) {
  const env = makeEnv(opts);
  vm.createContext(env.ctx);
  vm.runInContext(src, env.ctx, { filename: '16_Decision_Top10.gs' });
  env.ctx.dt10StabToday_ = () => TODAY_UTC;
  // Seed the panel cells (labels + the fixture's four gate inputs; cash cells 0).
  const top10 = env.sheets['Top_10_Investments'];
  env.ctx.DT10_PANEL.forEach((item, i) => {
    const pos = env.ctx.dt10PanelPos_(i);
    top10._set(pos.row, pos.labelCol, item.label);
    if (Object.prototype.hasOwnProperty.call(FIX.panel, item.label)) top10._set(pos.row, pos.valueCol, FIX.panel[item.label]);
    else if (item.kind === 'sar') top10._set(pos.row, pos.valueCol, 0);
  });
  return { ctx: env.ctx, env };
}
function verdictStub(state, reason) {
  return () => ({ state: state || 'EXECUTABLE', reason: reason || '', ageMin: 286,
    source_readiness: { ready: true, cohort: FIX.export_run, reason: '', issues: [], generation: {} } });
}
function buildPayload() {
  return { version: FIX.meta.versions.opportunity_builder, status: 'ok',
    selected: copy(FIX.selected), candidates_rows: copy(FIX.candidates), near_miss: copy(FIX.near_miss),
    alerts: copy(FIX.alerts), kpis: copy(FIX.kpis), meta: copy(FIX.meta) };
}
/* Drive the REAL renderer with private inputs attached exactly as refreshDecisionTop10 would. */
function render(tree, modes, extra) {
  modes = modes || {}; extra = extra || {};
  const { ctx, env } = tree;
  ctx.dt10BoardVerdict_ = verdictStub(extra.state, extra.reason);
  const sheet = env.sheets['Top_10_Investments'];
  const p = buildPayload();
  if (extra.candidates !== undefined) p.candidates_rows = copy(extra.candidates);
  if (extra.alerts !== undefined) p.alerts = copy(extra.alerts);
  p._dt10_source_context = { captured_at_ms: NOW, initial_ready: true, generation: {} };
  if (modes.it && modes.it !== 'off') {
    p._dt10_it = { mode: modes.it, ref: ctx.dt10PanelRefCore_(copy(FIX.panel), ctx.dt10PanelDefaults_(env.ss),
      ctx.dt10PfPanelRead_(env.ss, ctx.DT10_IT_REF_LABELS), modes.policyRef || null) };
  }
  if (modes.cand && modes.cand !== 'off') p._dt10_cand_mode = modes.cand;
  if (modes.vint && modes.vint !== 'off') {
    p._dt10_vint = { mode: modes.vint, idx: ctx.dt10PoolVintageIndex_(extra.pool || copy(FIX.pool_sample)) };
  }
  const before = env.calls.length;
  const earn = ctx.dt10RenderPayload_(sheet, p, ctx.dt10Tokens_(env.ss));
  return { p, earn, calls: env.calls.slice(before), reads: env.reads.slice(), env };
}
const find = (calls, pred) => calls.filter(pred);
const setValuesAt = (calls, sheetName, r, c, nr, nc) => find(calls, x => x[0] === 'setValues' && x[1] === sheetName && x[2] === r && x[3] === c && x[4] === nr && x[5] === nc).map(x => JSON.parse(x[6]));
const withoutOwnershipReads = reads => reads.filter(x => !(x[0] === 'getValues' && x[1] === 'Top_10_Investments' && x[2] === 15 && x[3] === 12 && x[4] === 1 && x[5] === 2));
function withoutApprovedScopeReads(reads) {
  const kept=[],added=[];
  for(let i=0;i<reads.length;i++) {
    const head=reads[i],column=reads[i+1];
    if(head[0]==='getValues' && ['Market_Leaders','Mutual_Funds'].includes(head[1]) &&
      head[2]===1 && head[3]===1 && head[4]===11 && head[5]===115 && column &&
      column[0]==='getValues' && column[1]===head[1] && column[2]===2 && column[3]===1 && column[4]===10 && column[5]===1) {
      added.push(head[1]); i++;
    } else kept.push(head);
  }
  deq(added,['Market_Leaders','Mutual_Funds','Market_Leaders','Mutual_Funds'],'exactly the two complete-column readiness-boundary reads added');
  return kept;
}
const mergesOn = (calls, sheetName) => find(calls, x => x[0] === 'merge' && x[1] === sheetName).map(x => x[2]);
const lastWrittenRow = (calls, sheetName) => Math.max(...find(calls, x => x[1] === sheetName && (x[0] === 'setValue' || x[0] === 'setValues' || x[0] === 'merge')).map(x => x[2] + (x[0] === 'setValues' ? x[4] - 1 : 0)));

/* ================================================================== T1 */
check('T1 mode readers: absent/junk/OFF -> off; Observe/ENFORCE bind; service error -> off; memoized', () => {
  let t = loadTree(source);
  deq([t.ctx.dt10InputTruthMode_(), t.ctx.dt10CandTabMode_(), t.ctx.dt10VintageColsMode_()], ['off', 'off', 'off']);
  assert.equal(t.ctx.dt10PolicyRef_(), null);
  t = loadTree(source, { props: { DT10_INPUT_TRUTH: 'junk', DT10_CAND_TAB: 'OFF', DT10_VINTAGE_COLS: ' Observe ' } });
  deq([t.ctx.dt10InputTruthMode_(), t.ctx.dt10CandTabMode_(), t.ctx.dt10VintageColsMode_()], ['off', 'off', 'observe']);
  t = loadTree(source, { props: { DT10_INPUT_TRUTH: 'ENFORCE', DT10_CAND_TAB: 'enforce', DT10_VINTAGE_COLS: 'Enforce', DT10_POLICY_REF: '{"T10: Max Per Sector": 3, "PF: Max Sector %": "40"}' } });
  deq([t.ctx.dt10InputTruthMode_(), t.ctx.dt10CandTabMode_(), t.ctx.dt10VintageColsMode_()], ['enforce', 'enforce', 'enforce']);
  deq(t.ctx.dt10PolicyRef_(), { 'T10: Max Per Sector': 3, 'PF: Max Sector %': 40 });
  t = loadTree(source, { props: { DT10_INPUT_TRUTH: 'enforce' }, propsThrow: true });
  deq([t.ctx.dt10InputTruthMode_(), t.ctx.dt10CandTabMode_(), t.ctx.dt10VintageColsMode_(), t.ctx.dt10PolicyRef_()], ['off', 'off', 'off', null]);
  t = loadTree(source, { props: { DT10_INPUT_TRUTH: 'observe', DT10_POLICY_REF: 'junk' } });
  assert.equal(t.ctx.dt10InputTruthMode_(), 'observe'); t.ctx.dt10InputTruthMode_(); t.ctx.dt10InputTruthMode_();
  assert.equal(t.env.propReads.byKey.DT10_INPUT_TRUTH, 1, 'one property read per execution');
  assert.equal(t.ctx.dt10PolicyRef_(), null); t.ctx.dt10PolicyRef_();
  assert.equal(t.env.propReads.byKey.DT10_POLICY_REF, 1);
  t.env.props.DT10_INPUT_TRUTH = 'enforce';
  assert.equal(t.ctx.dt10InputTruthMode_(), 'observe', 'memo is stable within an execution');
  t.ctx.dt10PropMemoReset_();
  assert.equal(t.ctx.dt10InputTruthMode_(), 'enforce'); assert.equal(t.env.propReads.byKey.DT10_INPUT_TRUTH, 2);
});

/* ================================================================== T2 */
check('T2 display labels do not prove evaluation: 500 missing News receipts; sector 500 unknown; conflict 150/350', () => {
  const { ctx } = loadTree(source);
  const ev = ctx.dt10GateEvaluabilityCore_(copy(FIX.candidates), FIX.meta.route.news_display);
  assert.equal(ev.total, 500);
  deq([ev.news.unknown, ev.news.evaluable, ev.news.pct_unknown, ev.news.not_evaluable, ev.news.display_only, ev.news.cap], [500, 0, 100, true, true, 60]);
  deq([ev.sector.unknown, ev.sector.evaluable, ev.sector.pct_unknown, ev.sector.not_evaluable], [500, 0, 100, true]);
  deq(ev.conflict, { yes: 150, no: 350, unknown: 0 });
  const note = ctx.dt10GateEvaluabilityNote_(ev, 'enforce');
  assert.equal(note, 'gates: news 0/500 evaluable (100% unknown; display labels 60/500, display-only; backend gate receipts required) NOT EVALUABLE · sector 0/500 evaluable (100% unknown) NOT EVALUABLE · conflict yes 150/no 350');
  assert(ctx.dt10GateEvaluabilityNote_(ev, 'observe').endsWith(' [observe]'));
  const alerts = ctx.dt10GateEvaluabilityAlerts_(ev);
  assert.equal(alerts.length, 2);
  deq(alerts.map(a => [a.type, a.count]), [['gate_not_evaluable', 500], ['gate_not_evaluable', 500]]);
  assert(alerts[0].required_action.includes('News labels are display-only (route NEWS-DISPLAY cap 60)'));
  const kpi = ctx.dt10GateKpiCells_(ev);
  deq(kpi[0], ['News Gate', 'Sector Gate']);
  assert.match(kpi[1][0], /^NOT EVALUABLE — 500\/500 Unknown$/); assert.match(kpi[1][1], /^NOT EVALUABLE — 500\/500 Unknown$/);
  const empty = ctx.dt10GateEvaluabilityCore_([], null);
  assert.equal(ctx.dt10GateEvaluabilityNote_(empty, 'observe'), 'gates: no candidates [observe]');
  assert.equal(ctx.dt10GateEvaluabilityAlerts_(empty).length, 0); deq(ctx.dt10GateKpiCells_(empty)[1], ['n/a', 'n/a']);
  const rows = n => Array.from({ length: 100 }, (_, i) => ({ news_trend: 'Positive', gates: [{ gate: 'News', passed: true, current: i < n ? ' Unknown ' : 'Neutral' }], sector_trend: 'Positive', conflict: false }));
  assert.equal(ctx.dt10GateEvaluabilityCore_(rows(20), null).news.not_evaluable, false, '20% is the pass boundary');
  assert.equal(ctx.dt10GateEvaluabilityCore_(rows(21), null).news.not_evaluable, true);
  assert.equal(ctx.dt10GateEvaluabilityCore_(rows(21), null).sector.not_evaluable, false);
  digestParts.push(['T2', ev, note, alerts, kpi]);
});

/* ================================================================== T3 */
check('T3 panel reference: panel 2/30/20 vs seed 2/30/15, built-in 3, decision-0910 3/40; policy only when DT10_POLICY_REF is set', () => {
  const { ctx, env } = loadTree(source);
  const defaults = ctx.dt10PanelDefaults_(env.ss);
  assert.equal(defaults['T10: Max Per Sector'], 2); assert.equal(defaults['PF: Max Position %'], 15);
  const pf = ctx.dt10PfPanelRead_(env.ss, ctx.DT10_IT_REF_LABELS);
  deq(pf, { found: true, values: { 'PF: Max Sector %': 30, 'PF: Max Position %': 20 }, err: '' });
  const ref = ctx.dt10PanelRefCore_(copy(FIX.panel), defaults, pf, null);
  const pick = it => [it.panel, it.seed, it.builtin, it.decision, it.policy];
  deq(pick(ref.items[0]), [2, 2, 3, 3, null]);
  deq(pick(ref.items[1]), [30, 30, null, 40, null]);
  deq(pick(ref.items[2]), [20, 15, null, null, null]);
  deq([ref.drift_seed, ref.drift_decision, ref.drift_policy, ref.ref, ref.pf_err], [1, 2, null, 'seed', '']);
  const note = ctx.dt10PanelRefNote_(ref, 'enforce');
  assert.equal(note, ' | panel: Max Per Sector 2 (seed 2 · built-in 3 · decision-0910 3 · policy —) · PF Max Sector % 30 (seed 30 · decision-0910 40 · policy —) · PF Max Position % 20 (seed 15 · policy —) · ref=seed drift 1 · policy ref none');
  assert(!/policy \d/.test(note), 'never a numeric policy without DT10_POLICY_REF');
  const al = ctx.dt10PanelRefAlerts_(ref);
  assert.equal(al.length, 1); assert.equal(al[0].count, 1); assert(al[0].required_action.includes('PF: Max Position % 20 vs seed 15'));
  const refP = ctx.dt10PanelRefCore_(copy(FIX.panel), defaults, pf, { 'T10: Max Per Sector': 3, 'PF: Max Sector %': 40, 'PF: Max Position %': 10 });
  deq([refP.drift_policy, refP.ref], [3, 'policy']);
  const noteP = ctx.dt10PanelRefNote_(refP, 'observe');
  assert(noteP.includes('policy 3') && noteP.includes('policy 40') && noteP.includes('policy 10') && noteP.includes('ref=policy drift 3') && noteP.includes('policy ref set') && noteP.endsWith('[observe]'));
  assert.equal(ctx.dt10PanelRefAlerts_(refP)[0].count, 3);
  const missing = loadTree(source, { noPortfolioDecision: true });
  const pfM = missing.ctx.dt10PfPanelRead_(missing.env.ss, missing.ctx.DT10_IT_REF_LABELS);
  deq(pfM, { found: false, values: {}, err: 'sheet-missing' });
  const refM = missing.ctx.dt10PanelRefCore_(copy(FIX.panel), defaults, pfM, null);
  deq([refM.items[0].panel, refM.items[1].panel, refM.items[2].panel, refM.drift_seed], [2, null, null, 0]);
  assert(missing.ctx.dt10PanelRefNote_(refM, 'observe').includes('PF n/a (sheet-missing)'));
  assert.equal(ctx.dt10LabelScan_([['PF: Max Sector %', 30, '', 'PF: Min Reliability to Add', 70, '', 'PF: Min DQ to Add', 80]], 'PF: Max Sector %').value, 30);
  assert.equal(ctx.dt10LabelScan_(copy(FIX.pd_block), 'PF: Max Position %').value, 20);
  assert.equal(ctx.dt10LabelScan_(copy(FIX.pd_block), 'PF: Cash Available SAR').value, '', 'the fixture carries no cash figure');
  digestParts.push(['T3', ref, note, al, refP.drift_policy]);
});

/* ================================================================== T4 */
let offRun = null;
check('T4 delivered OFF render is deterministic (idempotent write log + payload) and attaches no _dt10_* keys', () => {
  const a = render(loadTree(source), {}); const b = render(loadTree(source), {});
  deq(a.calls, b.calls); assert.equal(JSON.stringify(a.p), JSON.stringify(b.p));
  assert(!Object.keys(a.p).some(k => /^_dt10_(it|cand|vint|cal)/.test(k)), 'off => no private input-truth keys');
  assert(a.calls.length > 1000, 'the real render wrote the whole cockpit: ' + a.calls.length + ' calls');
  assert.equal(lastWrittenRow(a.calls, 'Top_10_Investments'), FIX.shape.footer_row, 'footer at the exported row 578');
  deq(setValuesAt(a.calls, 'Top_10_Investments', FIX.shape.cand_header_row, 1, 1, 25)[0][0], a.env.ctx.DT10_CAND_HEADERS);
  assert.equal(setValuesAt(a.calls, 'Top_10_Investments', FIX.shape.cand_first_data_row, 1, 500, 25).length, 1);
  assert.equal(setValuesAt(a.calls, 'Top_10_Investments', 15, 12, 2, 2).length, 0, 'no L15:M16 write when off');
  assert.equal(find(a.calls, x => x[0] === 'insertSheet').length, 0);
  assert.equal(a.earn.note, 'earn ⚠1/10', 'EBC.US earnings in 12 days under the frozen clock');
  offRun = a;
  digestParts.push(['T4-off', a.calls, a.p]);
});
if (baseSource) {
  check('T4 historical default-off values/write log/payload match v1.13.3; only KPI ownership reads added', () => {
    const historicalOff = render(loadTree(historySource), {});
    const b = render(loadTree(baseSource), {});
    deq(historicalOff.calls, b.calls, 'write log');
    deq(withoutOwnershipReads(historicalOff.reads), b.reads, 'only ownership reads added');
    assert.equal(JSON.stringify(historicalOff.p), JSON.stringify(b.p), 'payload object');
    assert.equal(historicalOff.earn.note, b.earn.note);
    assert.equal(b.env.ctx.DT10_VERSION, '1.13.3');
  });
} else skip('T4 OFF BYTE-IDENTITY base vs delivered');

/* ================================================================== T5 */
let observeRun = null;
check('T5 observe: write log identical to off; tokens present; cal(earn-dated) 8/10 and 10/13; vintage 10/10 and 7/10', () => {
  const o = render(loadTree(source), { it: 'observe', cand: 'observe', vint: 'observe' });
  deq(o.calls, offRun.calls, 'observe never changes a written value');
  assert.equal(JSON.stringify(o.p.alerts), JSON.stringify(offRun.p.alerts));
  assert(o.p._dt10_it_note.endsWith('[observe]') && o.p._dt10_cand_note.endsWith('[observe]') && o.p._dt10_vint_note.endsWith('[observe]'));
  assert.equal(o.p._dt10_it_note, 'gates: news 0/500 evaluable (100% unknown; display labels 60/500, display-only; backend gate receipts required) NOT EVALUABLE · sector 0/500 evaluable (100% unknown) NOT EVALUABLE · conflict yes 150/no 350 [observe]' +
    ' | panel: Max Per Sector 2 (seed 2 · built-in 3 · decision-0910 3 · policy —) · PF Max Sector % 30 (seed 30 · decision-0910 40 · policy —) · PF Max Position % 20 (seed 15 · policy —) · ref=seed drift 1 · policy ref none [observe]');
  assert.equal(o.p._dt10_cand_note, 'cands 500 in cockpit (footer row 578; target <= 80) [observe]');
  assert.equal(Math.max(...mergesOn(o.calls, 'Top_10_Investments')), 578);
  assert.equal(o.p._dt10_vint_note, 'vintage: as-of 10/10 · lkg 7/10 [observe]');
  const cal = o.env.ctx.dt10CalCoverageCore_(o.p, o.earn.map);
  deq([cal.sel_covered, cal.sel_total, cal.qual_covered, cal.qual_total], [8, 10, 10, 13]);
  assert.equal(cal.note, ' | cal(earn-dated): sel 8/10 · qual 10/13');
  assert(!Object.prototype.hasOwnProperty.call(o.earn.map, '2286.SR'), 'blank date row is not dated');
  assert(!Object.prototype.hasOwnProperty.call(o.earn.map, 'MGY.US') && !Object.prototype.hasOwnProperty.call(o.earn.map, 'COSO.US'));
  assert(Object.prototype.hasOwnProperty.call(o.earn.map, 'FBK.US'), 'FBK.US (2026-10-13) is still upcoming on the frozen clock');
  assert.equal(o.env.ctx.dt10CalCoverageCore_(o.p, null).note, ' | cal(earn-dated): n/a');
  observeRun = o;
  digestParts.push(['T5', o.p._dt10_it_note, o.p._dt10_cand_note, o.p._dt10_vint_note, cal]);
});

/* ================================================================== T6 */
let enforceRun = null;
check('T6 enforce: KPI L15:M16, hidden _T10_Candidates (stamp/headers/rows), pointer line, footer 79 = 75 + alerts.length, vintage columns 31/32', () => {
  const e = render(loadTree(source), { it: 'enforce', cand: 'enforce', vint: 'enforce' });
  const C = e.calls, ctx = e.env.ctx;
  deq(setValuesAt(C, 'Top_10_Investments', 15, 12, 2, 2), [[['News Gate', 'Sector Gate'], ['NOT EVALUABLE — 500/500 Unknown', 'NOT EVALUABLE — 500/500 Unknown']]]);
  // ALERTS unchanged: 4 rows, no input-truth rows (cockpit <= 80 rows).
  const alertsTitle = find(C, x => x[0] === 'setValue' && x[1] === 'Top_10_Investments' && /^"ALERTS \(/.test(x[6]));
  assert.equal(alertsTitle.length, 1); assert.equal(alertsTitle[0][6], JSON.stringify('ALERTS (' + FIX.alerts.length + ')')); assert.equal(alertsTitle[0][2], 60);
  assert.equal(setValuesAt(C, 'Top_10_Investments', 62, 1, 4, 3).length, 1);
  assert.equal(JSON.stringify(e.p.alerts), JSON.stringify(offRun.p.alerts), 'payload.alerts never mutated');
  // hidden tab
  deq(find(C, x => x[0] === 'insertSheet').map(x => x[1]), ['_T10_Candidates_Staging', '_T10_Candidates']);
  const tabGrid = setValuesAt(C, '_T10_Candidates', 1, 1, 502, 25)[0];
  const stamp = tabGrid[0][0];
  assert.match(stamp, /^run 2026-10-10T19:28:22\.000Z \| route v4\.18\.1 \| builder v1\.25\.0 \| pool=body_rows\/9786 \| funding=final\/0exec \| 27895ms \| req d2c065e9fd85 \| rows 500 \| DT10 v1\.14\.0$/);
  deq(tabGrid[1], ctx.DT10_CAND_HEADERS);
  deq(tabGrid.slice(2), setValuesAt(offRun.calls, 'Top_10_Investments', 77, 1, 500, 25)[0], 'tab rows == the off run cockpit grid rows');
  assert.equal(find(C, x => x[0] === 'hideSheet' && x[1] === '_T10_Candidates').length, 1);
  deq(find(C, x => x[0] === 'setFrozenRows' && x[1] === '_T10_Candidates').map(x => x[2]), [2]);
  deq(find(C, x => x[0] === 'setActiveSheet').map(x => x[1]), ['Top_10_Investments']);
  assert(find(C, x => x[0] === 'setBackground' && x[1] === '_T10_Candidates').length >= 500, 'verdict colouring kept on the hidden tab');
  assert.equal(setValuesAt(C, 'Top_10_Investments', 77, 1, 500, 25).length, 0, 'no grid in the cockpit');
  // cockpit: section 75, header 76, pointer 77, footer 79
  deq(setValuesAt(C, 'Top_10_Investments', 76, 1, 1, 25)[0][0], ctx.DT10_CAND_HEADERS);
  const pointer = find(C, x => x[0] === 'setValue' && x[1] === 'Top_10_Investments' && x[2] === 77);
  assert.equal(pointer.length, 1);
  const ptxt = JSON.parse(pointer[0][6]);
  assert(ptxt.includes('_T10_Candidates') && ptxt.includes('(500 rows)') && ptxt.includes('req d2c065e9fd85'));
  const merges = mergesOn(C, 'Top_10_Investments');
  const footer = Math.max(...merges);
  assert.equal(footer, 79); assert.equal(footer, 75 + FIX.alerts.length, 'footer = 75 + alerts.length (pointer path)');
  assert.equal(footer, FIX.shape.cand_section_row + 4); assert(footer <= 80);
  assert.equal(lastWrittenRow(C, 'Top_10_Investments'), 79);
  assert.equal(e.p._dt10_cand_note, 'cands 500 -> _T10_Candidates (cockpit footer row 79)');
  const committed = ctx.dt10CandTabRead_(e.env.ss, 'd2c065e9fd85');
  assert.equal(committed.ok, true); assert.equal(committed.rows, 500);
  assert.match(committed.checksum, /^[0-9a-f]{64}$/);
  deq(e.p._dt10_cand_result, { ok: true, rows: 500, run_id: committed.run_id, checksum: committed.checksum, state: 'COMMITTED' });
  // SELECTED 32 columns
  const selHdr = setValuesAt(C, 'Top_10_Investments', 19, 1, 1, 32);
  assert.equal(selHdr.length, 1); assert.equal(selHdr[0][0].length, 32);
  deq(selHdr[0][0].slice(30), ['Quote As-Of (UTC)', 'Target Vintage']);
  deq(selHdr[0][0].slice(0, 30), ctx.DT10_SELECTED_HEADERS);
  const selRows = setValuesAt(C, 'Top_10_Investments', 20, 1, 10, 32)[0];
  const row = sym => selRows.find(r => r[1] === sym);
  deq(row('INDV.US').slice(30), ['2026-10-09 20:00Z', 'lkg 45h']);
  deq(row('GRT-UN.TO').slice(30), ['2026-10-09 20:00Z', 'lkg 17h']);
  deq(row('2286.SR').slice(30), ['2026-10-08 12:10Z', '—'], 'no Target Price -> em dash');
  deq(row('CRC.US').slice(30), ['2026-10-09 20:00Z', 'unknown (analyst date unavailable)'], 'Target Price without analyst date remains unknown');
  deq(row('NVDA.US').slice(30), ['2026-10-09 20:29Z', 'lkg 6h']);
  deq(row('EBC.US').slice(0, 30), setValuesAt(offRun.calls, 'Top_10_Investments', 20, 1, 10, 30)[0].find(r => r[1] === 'EBC.US'), 'first 30 cells identical to the off board');
  assert.equal(find(C, x => x[0] === 'setNumberFormat' && x[1] === 'Top_10_Investments' && x[2] === 20 && x[3] === 31 && x[4] === 10 && x[5] === 2).length, 1);
  assert.equal(e.p._dt10_vint_note, 'vintage: as-of 10/10 · lkg 7/10');
  // synthetic pool row without the as-of token -> retrieved clock, labelled
  const pool = copy(FIX.pool_sample).map(r => r.Symbol === 'INDV.US' ? Object.assign(r, { Warnings: 'analyst_lkg:45h' }) : r);
  const e2 = render(loadTree(source), { vint: 'enforce' }, { pool });
  const r2 = setValuesAt(e2.calls, 'Top_10_Investments', 20, 1, 10, 32)[0].find(r => r[1] === 'INDV.US');
  deq(r2.slice(30), ['retrieved 2026-10-10 17:48Z', 'lkg 45h']);
  assert.equal(e2.p._dt10_vint_note, 'vintage: as-of 9/10 · lkg 7/10');
  // withhold path: idx 30/31 intact, withhold indices em-dashed
  const w = render(loadTree(source), { vint: 'enforce' }, { state: 'NOT_ACTIONABLE', reason: 'synthetic feed withholding' });
  const wr = setValuesAt(w.calls, 'Top_10_Investments', 20, 1, 10, 32)[0].find(r => r[1] === 'INDV.US');
  for (const i of ctx.DT10_UV_BOARD_WITHHOLD_IDX) assert.equal(wr[i], '—');
  assert(String(wr[29]).includes('SIZING WITHHELD')); deq(wr.slice(30), ['2026-10-09 20:00Z', 'lkg 45h']);
  enforceRun = e;
  digestParts.push(['T6', C, e.p._dt10_cand_note, e.p._dt10_vint_note, r2.slice(30), wr.slice(30)]);
});

/* ================================================================== T7 */
check('T7 fail-open: insertSheet fault keeps the in-cockpit grid (footer 578) and tokens the failure', () => {
  const f = render(loadTree(source, { insertSheetThrows: true }), { cand: 'enforce' });
  assert.equal(setValuesAt(f.calls, 'Top_10_Investments', 77, 1, 500, 25).length, 1);
  assert.equal(Math.max(...mergesOn(f.calls, 'Top_10_Investments')), 578);
  assert(f.p._dt10_cand_note.startsWith('cands _T10_Candidates write FAILED (synthetic insertSheet fault)'));
  assert(f.p._dt10_cand_note.includes('grid kept in cockpit (footer row 578)'));
  assert.equal(f.p._dt10_cand_result.ok, false);
  assert.equal(find(f.calls, x => x[0] === 'hideSheet').length, 0);
  // the grid write log equals the off run except the attempted insertSheet
  deq(f.calls.filter(x => x[0] !== 'insertSheet' && x[0] !== 'setActiveSheet'), offRun.calls);
  // A current empty result commits zero rows, replacing any prior grid.
  const zz = loadTree(source); const p = buildPayload(); p.candidates_rows = []; p._dt10_cand_mode = 'enforce';
  p._dt10_source_context = { captured_at_ms: NOW, initial_ready: true, generation: {} };
  zz.ctx.dt10BoardVerdict_ = verdictStub();
  zz.ctx.dt10RenderPayload_(zz.env.sheets['Top_10_Investments'], p, {});
  assert(p._dt10_cand_note.includes('cands 0 -> _T10_Candidates'));
  const empty = zz.ctx.dt10CandTabRead_(zz.env.ss, 'd2c065e9fd85');
  assert.equal(empty.ok, true); assert.equal(empty.rows, 0); deq(empty.data, []);
  digestParts.push(['T7', f.p._dt10_cand_note]);
});

/* ================================================================== T8 */
let selfTestReport = '';
check('T8 REAL dt10SelfTest (fresh context): every prior core line, the USX line, the mode line and the seven new core lines; zero FAIL', () => {
  const { ctx } = loadTree(source);
  const rep = ctx.dt10SelfTest();
  const need = ['property memo core: ok', 'w52 projection core: ok', 'sync inflight core: ok', 'run marker core: ok', 'epoch key core: ok',
    'outage pause core: ok', 'grace sizing core: ok', 'funding containment core: ok', 'cash source core: ok', 'earnings tag core: ok',
    'source clock core: ok', 'final banner core: ok', 'held seat labels core: ok', 'final earnings annotation core: ok',
    'fx USX row: ABSENT', 'finding usx-subunit',
    'input truth (DT10_INPUT_TRUTH): off | cand tab (DT10_CAND_TAB): off | vintage cols (DT10_VINTAGE_COLS): off | policy ref: none',
    'evaluability core: ok', 'panel reference core: ok', 'label scan core: ok', 'vintage core: ok', 'cand pointer core: ok',
    'cal coverage core: ok', 'policy ref parse core: ok', '[DT10 v1.14.0] self-test',
    'feed cohort bind (DT10_FEED_COHORT_BIND): off', 'feed cohort core: ok'];
  const missing = need.filter(n => !rep.includes(n));
  deq(missing, []);
  assert(!/FAIL/.test(rep), 'self-test must not print FAIL');
  assert(rep.includes('fx rates from TFB_FX_LOOKUP: 55'));
  const armed = loadTree(source, { props: { DT10_INPUT_TRUTH: 'observe', DT10_CAND_TAB: 'enforce', DT10_POLICY_REF: '{"T10: Max Per Sector": 3}' } });
  assert(armed.ctx.dt10SelfTest().includes('input truth (DT10_INPUT_TRUTH): observe | cand tab (DT10_CAND_TAB): enforce | vintage cols (DT10_VINTAGE_COLS): off | policy ref: set'));
  // v1.13.8 (2026-10-13): the feed cohort binding mode line follows its Script Property.
  const bound = loadTree(source, { props: { DT10_FEED_COHORT_BIND: ' On ' } }).ctx.dt10SelfTest();
  assert(bound.includes('feed cohort bind (DT10_FEED_COHORT_BIND): on') && bound.includes('feed cohort core: ok') && !/FAIL/.test(bound));
  selfTestReport = rep;
  digestParts.push(['T8', rep.replace(/\d{4}-\d\d-\d\d[ T][\d:.Z+]+/g, 'T')]);
});

/* ================================================================== T9 */
const DESIGNED_FUNCTIONS = ['dt10FeedCohortBindOn_', 'dt10FeedCohortCheck_', 'dt10PoolFrameSha_', 'dt10PoolExclusionOrder_', 'dt10PoolFrameBuild_', 'dt10PoolFrameValidate_', 'dt10SourceScopeCore_', 'dt10SourceScopeRead_', 'dt10PolicyRefInspect_', 'dt10PolicyRefState_', 'dt10InputTruthKpiState_', 'dt10CandTabChecksum_', 'dt10CandRunId_', 'dt10CandTabRead_', 'dt10ModeProp_', 'dt10InputTruthMode_', 'dt10CandTabMode_', 'dt10VintageColsMode_', 'dt10PolicyRefParse_', 'dt10PolicyRef_',
  'dt10GateEvaluabilityCore_', 'dt10GateEvaluabilityNote_', 'dt10GateEvaluabilityAlerts_', 'dt10GateKpiCells_', 'dt10PanelRefCore_', 'dt10PanelRefNote_',
  'dt10PanelRefAlerts_', 'dt10CalCoverageCore_', 'dt10CandPointerText_', 'dt10CandTabStampLine_', 'dt10PoolVintageIndex_', 'dt10VintageFmt_',
  'dt10VintageCells_', 'dt10VintageNote_', 'dt10LabelScan_', 'dt10PfPanelRead_', 'dt10CandTabWrite_', 'dt10InputTruthRunLog_'].sort();
const DESIGNED_VARS = ['DT10_FEED_COHORT_BIND_PROP', 'DT10_SOURCE_APPROVED_SCOPE', 'DT10_CANDTAB_STAGE_SHEET', 'DT10_VINT_PUBLISHED_RE', 'DT10_IT_PROP', 'DT10_IT_POLICY_PROP', 'DT10_IT_UNKNOWN_MAX_PCT', 'DT10_IT_REF_LABELS', 'DT10_IT_DECISION_REF', 'DT10_CANDTAB_PROP',
  'DT10_CANDTAB_SHEET', 'DT10_CANDTAB_COCKPIT_TARGET_ROWS', 'DT10_VINTAGE_PROP', 'DT10_VINTAGE_HEADERS', 'DT10_VINT_ASOF_RE', 'DT10_VINT_LKG_RE'].sort();
function census(src, re) { const out = new Set(); let m; while ((m = re.exec(src))) out.add(m[1]); return out; }
check('T9 static: DT10_VERSION 1.14.0 in lockstep with header line 4, ES5 source compiles, zero smart quotes, exact census', () => {
  const { ctx } = loadTree(source);
  assert.equal(ctx.DT10_VERSION, '1.14.0');
  assert.match(source.split('\n')[3], /^ \* Version: 1\.14\.0 /);
  assert(source.includes("var DT10_VERSION = '1.14.0';"));
  assert.doesNotThrow(() => new vm.Script(source, { filename: '16_Decision_Top10.gs' }), 'syntax (node --check equivalent)');
  assert.equal((source.match(/[‘’“”]/g) || []).length, 0, 'smart quotes');
  assert(!source.includes('\r'), 'LF only');
  const fns = census(source, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), vars = census(source, /^var\s+([A-Za-z_$][\w$]*)/mg);
  assert.equal(fns.size,199,'full v1.14.0 function census (v1.13.9 191 + 8)'); assert.equal(vars.size,104,'full v1.14.0 variable census (v1.13.9 103 + 1)');
  for (const f of DESIGNED_FUNCTIONS) assert(fns.has(f), 'missing ' + f);
  for (const v of DESIGNED_VARS) assert(vars.has(v), 'missing ' + v);
  for (const f of DESIGNED_FUNCTIONS) assert.equal(typeof ctx[f], 'function', f);
  assert(source.includes(' * v1.13.4 (2026-10-11) [WP-8'), 'WHY block present');
  assert(source.includes(' * v1.13.3 (2026-10-10) -- SOURCE READINESS AND PUBLICATION COHORT'), 'prior WHY block kept');
});
if (baseSource) {
  check('T9 census vs base: every base function/var kept (0 removed); added exactly the designed names; added code lines are ES5', () => {
    const bf = census(baseSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), df = census(source, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    const bv = census(baseSource, /^var\s+([A-Za-z_$][\w$]*)/mg), dv = census(source, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq([...bf].filter(f => !df.has(f)), [], 'removed functions'); deq([...bv].filter(v => !dv.has(v)), [], 'removed vars');
    deq([...df].filter(f => !bf.has(f)).sort(), DESIGNED_FUNCTIONS); deq([...dv].filter(v => !bv.has(v)).sort(), DESIGNED_VARS);
    assert.equal(bf.size, 153); assert.equal(df.size, 153 + DESIGNED_FUNCTIONS.length); assert.equal(bv.size, 87); assert.equal(dv.size, 87 + DESIGNED_VARS.length);
    const baseLines = new Set(baseSource.split('\n'));
    const added = source.split('\n').filter(l => !baseLines.has(l));
    const code = added.filter(l => !/^\s*(\*|\/\*|\/\/)/.test(l)).map(l => l.replace(/\/\/.*$/, ''));
    assert(added.length > 500, 'added lines ' + added.length);
    for (const l of code) assert(!/\b(let|const)\b|=>|`|\bclass\b|\.\.\.|\.includes\(/.test(l), 'ES5 guard: ' + l);
    for (const l of added) assert(!/[‘’“”]/.test(l), 'smart quote: ' + l);
    // every base WHY block header line survives verbatim
    for (const l of baseSource.split('\n').filter(l => /^ \* v\d+\.\d+\.\d+ \(/.test(l))) assert(source.includes(l), 'WHY header lost: ' + l);
  });
} else skip('T9 census vs base');

/* ================================================================== T10 */
check('T10 _Run_Log: off -> no row; observe/enforce -> exactly one INPUT-TRUTH row with parseable details (ev/ref/cal/alerts, no cash)', () => {
  const t = loadTree(source);
  assert.equal(t.ctx.dt10InputTruthRunLog_('off', 'off', 'off', observeRun.p, null, ['', '', '']), false);
  assert.equal((t.env.appended['_Run_Log'] || []).length, 0);
  const ref = observeRun.p._dt10_it.ref;
  observeRun.p._dt10_cal = t.ctx.dt10CalCoverageCore_(observeRun.p, observeRun.earn.map);
  assert.equal(t.ctx.dt10InputTruthRunLog_('observe', 'observe', 'observe', observeRun.p, ref, [observeRun.p._dt10_it_note + observeRun.p._dt10_cal.note, observeRun.p._dt10_cand_note, observeRun.p._dt10_vint_note]), true);
  const rows = t.env.appended['_Run_Log'];
  assert.equal(rows.length, 1);
  const row = rows[0];
  assert.equal(row[1], 'INFO'); assert.equal(row[2], 'refreshDecisionTop10'); assert.equal(row[3], 'Top_10_Investments'); assert.equal(row[4], 'INPUT-TRUTH');
  assert(row[5].startsWith('gates: news 0/500')); assert(row[5].length <= 400);
  const details = JSON.parse(row[9]);
  deq(Object.keys(details).sort(), ['alerts', 'cal', 'cand_rows', 'cand_tab', 'cand_tab_result', 'ev', 'input_truth', 'notes', 'ref', 'version', 'vintage_cols']);
  deq([details.version, details.input_truth, details.cand_tab, details.vintage_cols, details.cand_rows], ['1.14.0', 'observe', 'observe', 'observe', 500]);
  assert.equal(details.ev.news.unknown, 500); assert.equal(details.ref.drift_seed, 1); assert.equal(details.cal.sel_covered, 8);
  deq(details.alerts.map(a => [a.type, a.count]), [['gate_not_evaluable', 500], ['gate_not_evaluable', 500], ['panel_reference_drift', 1]]);
  assert(!/cash|nav|deployable/i.test(row[9]), 'details carry no portfolio words');
  assert.equal(row[0], new Date(NOW).toISOString(), 'frozen clock stamp');
  digestParts.push(['T10', rows]);
});

/* ================================================================== T11 */
function orchestrate(src, props, extra) {
  extra = extra || {};
  const tree = loadTree(src, { props: Object.assign({}, props || {}) });
  tree.ctx.dt10BoardVerdict_ = verdictStub();
  tree.ctx.dt10Post_ = body => { tree.posted = (tree.posted || []).concat([copy(body.criteria)]); return { code: 200, json: buildPayload() }; };
  tree.ctx.refreshDecisionTop10();
  const top10 = tree.env.sheets['Top_10_Investments'];
  return { tree, status: String(top10._val(2, 2)), calls: tree.env.calls.slice(), reads: tree.env.reads.slice(),
    runLog: copy(tree.env.appended['_Run_Log'] || []), propReads: copy(tree.env.propReads), posted: tree.posted };
}
let orchOff = null;
check('T11 REAL refreshDecisionTop10 off: deterministic status line, one POST, no INPUT-TRUTH row, no _dt10_* token', () => {
  const a = orchestrate(source, {}), b = orchestrate(source, {});
  assert.equal(a.status, b.status); deq(a.calls, b.calls); deq(a.runLog, b.runLog);
  assert.equal(a.posted.length, 1);
  assert.match(a.status, /^Last run 2026-10-10 22:28:22 \| status: ok \| output: /);
  assert(a.status.includes('route v4.18.1') && a.status.includes('req d2c065e9fd85'));
  assert(!/gates:|panel:|cal\(earn-dated\)|cands |vintage:/.test(a.status), 'no input-truth token when off');
  assert.equal(a.runLog.filter(r => r[4] === 'INPUT-TRUTH').length, 0);
  deq([a.propReads.byKey.DT10_INPUT_TRUTH, a.propReads.byKey.DT10_CAND_TAB, a.propReads.byKey.DT10_VINTAGE_COLS], [1, 1, 1], 'three memoized reads');
  assert.equal(a.propReads.byKey.DT10_POLICY_REF, undefined, 'policy ref never read when off');
  assert(!/cash|nav/i.test(a.status), 'status line names no cash');
  orchOff = a;
  digestParts.push(['T11-off', a.status, a.calls.length, a.runLog]);
});
if (baseSource) {
  check('T11 historical unchanged off writes/status plus exactly the new physical-scope and KPI-ownership reads', () => {
    const historicalOrch = orchestrate(historySource, {});
    const b = orchestrate(baseSource, {});
    assert.equal(historicalOrch.status, b.status, 'B2 status line');
    deq(historicalOrch.calls, b.calls, 'write log incl. _Selection_Log / _Run_Log');
    deq(withoutApprovedScopeReads(withoutOwnershipReads(historicalOrch.reads)), b.reads, 'only approved physical-scope and ownership reads added');
    deq(historicalOrch.runLog, b.runLog);
    assert.equal(historicalOrch.propReads.total, b.propReads.total + 3);
    const keys = Object.keys(historicalOrch.propReads.byKey).filter(k => historicalOrch.propReads.byKey[k] !== b.propReads.byKey[k]).sort();
    deq(keys, ['DT10_CAND_TAB', 'DT10_INPUT_TRUTH', 'DT10_VINTAGE_COLS']);
  });
} else skip('T11 OFF BYTE-IDENTITY orchestrator base vs delivered');
check('T11 observe orchestrator: tokens in B2, one INPUT-TRUTH row, sheet writes identical to off', () => {
  const o = orchestrate(source, { DT10_INPUT_TRUTH: 'observe', DT10_CAND_TAB: 'observe', DT10_VINTAGE_COLS: 'observe' });
  // observe = countable tags + log lines only: the B2 status cell, the _Selection_Log
  // Run Info column (it carries the status line) and one _Run_Log row differ; every
  // other write is identical to off.
  const valueWrites = calls => calls.filter(x => !(x[0] === 'appendRow' && x[1] === '_Run_Log') && x[1] !== '_Selection_Log' &&
    !(x[0] === 'setValue' && x[1] === 'Top_10_Investments' && x[2] === 2 && x[3] === 2));
  deq(valueWrites(o.calls), valueWrites(orchOff.calls), 'every cockpit value write identical to off');
  const selLogRows = calls => calls.filter(x => x[0] === 'setValues' && x[1] === '_Selection_Log').map(x => JSON.parse(x[6]).map(r => r.map((v, i) => i === 1 ? '<run info>' : v)));
  deq(selLogRows(o.calls), selLogRows(orchOff.calls), '_Selection_Log rows identical apart from the Run Info status line');
  assert.equal(o.status.replace(/ \| gates: .*? \| vintage: as-of 0\/10 · lkg 0\/10 \[observe\]/, ''), orchOff.status, 'B2 = off line + tokens');
  assert.equal(o.posted.length, 1, 'one POST (no extra network call)');
  deq(o.calls.filter(x => x[0] === 'appendRow' && x[1] === '_Run_Log').length, orchOff.calls.filter(x => x[0] === 'appendRow' && x[1] === '_Run_Log').length + 1);
  const it = o.runLog.filter(r => r[4] === 'INPUT-TRUTH');
  assert.equal(it.length, 1);
  assert(o.status.includes(' | gates: news 0/500 evaluable (100% unknown; display labels 60/500, display-only; backend gate receipts required) NOT EVALUABLE'));
  assert(o.status.includes(' | panel: Max Per Sector 2 (seed 2 · built-in 3 · decision-0910 3 · policy —)'));
  assert(o.status.includes(' | cal(earn-dated): sel 8/10 · qual 10/13'));
  // the orchestrator path fails the frozen funding replay closed (+1 board_funding_unavailable
  // alert => 5), so the grid footer is 71 + alerts.length + 3 + 500 = 579 on this payload.
  const nAlertsO = +JSON.parse(find(o.calls, x => x[0] === 'setValue' && x[1] === 'Top_10_Investments' && /^"ALERTS \(/.test(x[6]))[0][6]).match(/\((\d+)\)/)[1];
  assert.equal(Math.max(...mergesOn(o.calls, 'Top_10_Investments')), 71 + nAlertsO + 3 + 500, 'grid-path footer formula');
  assert(o.status.includes(' | cands 500 in cockpit (footer row ' + (71 + nAlertsO + 503) + '; target <= 80) [observe]'));
  assert(o.status.includes(' | vintage: as-of 0/10 · lkg 0/10 [observe]'), 'the 40-row page fixture holds none of the 10 selected symbols');
  assert(o.status.indexOf(' | gates:') > o.status.indexOf('route v4.18.1'), 'tokens sit after the meta line');
  const details = JSON.parse(it[0][9]);
  assert.equal(details.ev.total, 500); assert.equal(details.cal.qual_covered, 10); assert.equal(details.cand_tab, 'observe');
  assert.equal(o.propReads.byKey.DT10_POLICY_REF, 1);
  // same tokens reach the _Selection_Log Run Info column (status line pre-'SelLog' tail)
  const selLog = o.calls.filter(x => x[0] === 'setValues' && x[1] === '_Selection_Log' && x[4] === 10);
  assert.equal(selLog.length, 1); assert(JSON.parse(selLog[0][6])[0][1].includes(' | gates: news 0/500'));
  digestParts.push(['T11-observe', o.status, it]);
});
check('T11 enforce orchestrator: hidden tab written once, footer = 71 + alerts.length + 4 <= 80, KPI cells, 32 selected columns', () => {
  const e = orchestrate(source, { DT10_INPUT_TRUTH: 'enforce', DT10_CAND_TAB: 'enforce', DT10_VINTAGE_COLS: 'enforce' });
  deq(find(e.calls, x => x[0] === 'insertSheet').map(x => x[1]).filter(n => n === '_T10_Candidates'), ['_T10_Candidates']);
  const alertsTitle = find(e.calls, x => x[0] === 'setValue' && x[1] === 'Top_10_Investments' && /^"ALERTS \(/.test(x[6]))[0];
  const nAlerts = +JSON.parse(alertsTitle[6]).match(/\((\d+)\)/)[1];
  const footer = Math.max(...mergesOn(e.calls, 'Top_10_Investments'));
  assert.equal(footer, 71 + nAlerts + 4, 'pointer-path footer formula'); assert(footer <= 80, 'cockpit <= 80 rows');
  assert.equal(setValuesAt(e.calls, 'Top_10_Investments', 15, 12, 2, 2).length, 1);
  assert.equal(setValuesAt(e.calls, 'Top_10_Investments', 19, 1, 1, 32).length, 1);
  assert(e.status.includes(' | cands 500 -> _T10_Candidates (cockpit footer row ' + footer + ')'));
  assert(!e.status.includes('[observe]'));
  assert.equal(e.runLog.filter(r => r[4] === 'INPUT-TRUTH').length, 1);
  digestParts.push(['T11-enforce', e.status, footer, nAlerts]);
});

check('T12 same-sheet mode/property transitions deactivate owned KPIs; never-active cells keep their values', () => {
  for (const mode of ['observe', 'off', 'property-fault']) {
    const t = loadTree(source, { props: { DT10_INPUT_TRUTH: 'enforce' } });
    render(t, { it: 'enforce' });
    assert.equal(t.env.sheets.Top_10_Investments._val(16, 12), 'NOT EVALUABLE — 500/500 Unknown');
    t.env.props.DT10_INPUT_TRUTH = mode;
    if (mode === 'property-fault') t.env.options.propsThrow = true;
    t.ctx.dt10PropMemoReset_();
    const effective = t.ctx.dt10InputTruthMode_();
    render(t, { it: effective });
    for (const col of [12, 13]) assert.equal(t.env.sheets.Top_10_Investments._val(16, col), 'INACTIVE — ' + effective);
    render(t, { it: 'enforce' });
    assert.equal(t.env.sheets.Top_10_Investments._val(16, 12), 'NOT EVALUABLE — 500/500 Unknown');
  }
  const t = loadTree(source), sh = t.env.sheets.Top_10_Investments;
  sh._set(15, 12, 'Operator metric'); sh._set(16, 12, 42);
  for (const mode of ['off', 'observe']) {
    const r = render(t, { it: mode });
    assert.equal(sh._val(15, 12), 'Operator metric'); assert.equal(sh._val(16, 12), 42);
    assert.equal(find(r.calls, x => x[0] === 'setValues' && x[1] === 'Top_10_Investments' && x[2] >= 15 && x[2] <= 16 && x[3] === 12).length, 0);
  }
});

check('T13 policy rejects unknown keys/ranges/types; partial references retain seed drift; zero comparisons NOT EVALUABLE', () => {
  const { ctx, env } = loadTree(source);
  for (const raw of [JSON.stringify({ typo: 3 }), '{}', '{"T10: Max Per Sector":null}', '{"T10: Max Per Sector":true}',
      '{"T10: Max Per Sector":0}', '{"T10: Max Per Sector":2.5}', '{"T10: Max Per Sector":50001}',
      '{"PF: Max Sector %":101}', '{"PF: Max Position %":-1}', '{"PF: Max Position %":""}',
      '{"T10: Max Per Sector":3,"typo":2}']) {
    assert.equal(ctx.dt10PolicyRefParse_(raw), null, raw);
    const state = ctx.dt10PolicyRefInspect_(raw);
    assert.equal(state.status, 'invalid'); assert(state.errors.length > 0);
    const ref = ctx.dt10PanelRefCore_(copy(FIX.panel), ctx.dt10PanelDefaults_(env.ss), ctx.dt10PfPanelRead_(env.ss, ctx.DT10_IT_REF_LABELS), state);
    assert.equal(ref.drift_policy, null); assert.equal(ref.drift_active, 1); assert.equal(ref.not_evaluable, true);
    const alerts = ctx.dt10PanelRefAlerts_(ref);
    assert(alerts.some(a => a.type === 'policy_reference_not_evaluable'));
    assert(alerts.some(a => a.type === 'panel_reference_drift' && a.required_action.includes('PF: Max Position % 20 vs seed 15')));
  }
  const state = ctx.dt10PolicyRefInspect_('{"T10: Max Per Sector":3}');
  assert.equal(state.status, 'partial'); assert.equal(state.missing.length, 2);
  const ref = ctx.dt10PanelRefCore_(copy(FIX.panel), ctx.dt10PanelDefaults_(env.ss), ctx.dt10PfPanelRead_(env.ss, ctx.DT10_IT_REF_LABELS), state);
  assert.equal(ref.ref, 'mixed'); assert.equal(ref.policy_comparisons, 1); assert.equal(ref.comparisons, 3);
  assert.equal(ref.drift_policy, 1); assert.equal(ref.drift_active, 2);
  const action = ctx.dt10PanelRefAlerts_(ref)[0].required_action;
  assert(action.includes('T10: Max Per Sector 2 vs policy 3')); assert(action.includes('PF: Max Position % 20 vs seed 15'));
  assert(ctx.dt10PanelRefNote_(ref, 'observe').includes('policy ref partial (undeclared controls use seed)'));
  const zero = ctx.dt10PanelRefCore_({}, {}, { found: false, values: {} }, state);
  assert.equal(zero.policy_comparisons, 0); assert.equal(zero.drift_policy, null);
  assert(ctx.dt10PanelRefNote_(zero, 'enforce').includes('NOT EVALUABLE'));
  const fault = loadTree(source, { propsThrow: true });
  assert.equal(fault.ctx.dt10PolicyRefState_().status, 'unavailable');
});

check('T14 News uses backend gate receipts independently of display labels and normalizes unknown trends', () => {
  const { ctx } = loadTree(source);
  const labelsOnly = Array.from({ length: 3 }, () => ({ news_trend: 'Positive', sector_trend: ' unknown ' }));
  const noReceipt = ctx.dt10GateEvaluabilityCore_(labelsOnly, { enabled: true, labeled: 3 });
  assert.equal(noReceipt.news.display_evaluable, 3); assert.equal(noReceipt.news.evaluable, 0); assert(noReceipt.news.not_evaluable);
  const rows = [
    { news_trend: 'Positive', gates: [{ gate: 'News', passed: true, current: 'Unknown' }], sector_trend: 'UNKNOWN' },
    { news_trend: 'Unknown', gates: [{ gate: 'News', passed: false, current: 'Negative' }], sector_trend: ' Neutral ' },
    { news_trend: 'Neutral', gates: [{ gate: 'News', passed: true, current: 'Positive' }, { gate: 'News', passed: true, current: 'Positive' }], sector_trend: null },
    { news_trend: 'Positive', gates: [{ gate: 'News', passed: 'true', current: 'Positive' }], sector_trend: 'N/A' }
  ];
  const ev = ctx.dt10GateEvaluabilityCore_(rows, { enabled: true });
  assert.equal(ev.news.evaluable, 1); assert.equal(ev.news.display_evaluable, 3);
  assert.equal(ev.news.unknown, 3); assert.equal(ev.sector.unknown, 3);
});

check('T15 target/retrieval/quote presence never implies analyst vintage; provider dates and held age stay distinct', () => {
  const { ctx } = loadTree(source);
  const idx = ctx.dt10PoolVintageIndex_([
    { Symbol: 'UNDATED.US', 'Target Price': 10, 'Last Updated (UTC)': '2026-10-10T20:00:00+03:00', Warnings: 'acquisition_quote_asof:2026-10-09T20:00:00Z' },
    { Symbol: 'DATED.US', 'Target Price': 10, 'Analyst Target Published At': '2026-09-01T00:00:00+03:00' },
    { Symbol: 'TOKEN.US', 'Target Price': 10, Warnings: 'analyst_target_asof:2026-09-01' },
    { Symbol: 'FUTURE.US', 'Target Price': 10, 'Target Publication Date': '2027-01-01' },
    { Symbol: 'INVALID.US', 'Target Price': 10, 'Target Publication Date': '2026-02-31' },
    { Symbol: 'NAIVE.US', 'Target Price': 10, 'Analyst Target Published At': '2026-09-01T00:00:00' },
    { Symbol: 'HELD.US', 'Target Price': 10, Warnings: 'analyst_lkg:30h' }
  ]);
  deq(ctx.dt10VintageCells_({ symbol: 'UNDATED.US' }, idx), ['2026-10-09 20:00Z', 'unknown (analyst date unavailable)']);
  assert.equal(ctx.dt10VintageCells_({ symbol: 'DATED.US' }, idx)[1], 'analyst 2026-08-31 21:00Z');
  assert.equal(ctx.dt10VintageCells_({ symbol: 'TOKEN.US' }, idx)[1], 'analyst 2026-09-01');
  for (const symbol of ['FUTURE.US', 'INVALID.US', 'NAIVE.US']) assert.equal(ctx.dt10VintageCells_({ symbol }, idx)[1], 'unknown (analyst date unavailable)');
  assert.equal(ctx.dt10VintageCells_({ symbol: 'HELD.US' }, idx)[1], 'lkg 30h');
  assert.equal(ctx.dt10VintageFmt_('2026-10-10T20:00:00+03:00'), '2026-10-10 17:00Z');
  assert(ctx.dt10VintageFmt_('2026-10-10T20:00:00').includes('timezone unknown'));
});

check('T16 candidate commits replace stale/empty runs; readers reject corruption and partial-write boundaries', () => {
  const t = loadTree(source), { ctx, env } = t;
  const rows = copy(FIX.candidates.slice(0, 3)).map(c => ctx.dt10CandToRow_(c));
  const write = (run, data) => ctx.dt10CandTabWrite_(env.ss, data, 'run ' + run, null, run);
  assert(write('old', rows).ok); assert(ctx.dt10CandTabRead_(env.ss, 'old').ok);
  assert.equal(ctx.dt10CandTabRead_(env.ss, 'new').ok, false);
  env.failWrite = { sheet: '_T10_Candidates_Staging', method: 'setValues', r: 1, partialRows: 3 };
  assert.equal(write('stage-fault', rows).ok, false);
  assert(ctx.dt10CandTabRead_(env.ss, 'old').ok); assert.equal(ctx.dt10CandTabRead_(env.ss, 'stage-fault').ok, false);
  env.failWrite = { sheet: '_T10_Candidates', method: 'setValues', r: 1, partialRows: 3 };
  assert.equal(write('copy-fault', rows).ok, false); assert.equal(ctx.dt10CandTabRead_(env.ss, 'copy-fault').ok, false);
  assert.equal(JSON.parse(env.sheets._T10_Candidates._val(1, 2)).state, 'PENDING');
  env.failWrite = { sheet: '_T10_Candidates', method: 'setValue', r: 1, c: 2, skip: 1 };
  assert.equal(write('commit-fault', rows).ok, false); assert.equal(ctx.dt10CandTabRead_(env.ss, 'commit-fault').ok, false);
  assert(write('new', rows.slice(0, 1)).ok); assert.equal(env.sheets._T10_Candidates.getLastRow(), 3);
  assert.equal(ctx.dt10CandTabRead_(env.ss, 'old').ok, false);
  env.sheets._T10_Candidates._set(3, 1, 'CORRUPT.US'); assert.equal(ctx.dt10CandTabRead_(env.ss, 'new').ok, false);
  assert(write('empty', []).ok);
  const empty = ctx.dt10CandTabRead_(env.ss, 'empty');
  assert.equal(empty.ok, true); assert.equal(empty.rows, 0); deq(empty.data, []);
  assert.equal(env.sheets._T10_Candidates.getLastRow(), 2); assert.equal(ctx.dt10CandTabRead_(env.ss, 'new').ok, false);
  assert.equal(write('bad-shape', [['bad']]).ok, false); assert(ctx.dt10CandTabRead_(env.ss, 'empty').ok);
  assert(write('interleave', rows).ok);
  env.onReadValues = (sheet, r) => {
    if (sheet._name === '_T10_Candidates' && r === 3) {
      env.onReadValues = null;
      const receipt = JSON.parse(sheet._val(1, 2)); receipt.state = 'PENDING'; sheet._set(1, 2, JSON.stringify(receipt));
    }
  };
  const changed = ctx.dt10CandTabRead_(env.ss, 'interleave');
  assert.equal(changed.ok, false); assert(changed.err.includes('commit changed during read'));
  assert(write('checksum-base', rows).ok);
  for (const [sheetName, run] of [['_T10_Candidates_Staging', 'stage-corrupt'], ['_T10_Candidates', 'live-corrupt']]) {
    env.onReadValues = (sheet, r) => {
      if (sheet._name === sheetName && r === 3) {
        env.onReadValues = null; sheet._set(3, 1, 'CORRUPT.US');
      }
    };
    const result = write(run, rows);
    assert.equal(result.ok, false); assert(result.err.includes('readback mismatch'));
    assert.equal(ctx.dt10CandTabRead_(env.ss, run).ok, false);
  }
  assert(write('lock-base', rows).ok);
  env.options.lockHeld = true;
  assert.equal(write('lock-held', rows).ok, false); assert(ctx.dt10CandTabRead_(env.ss, 'lock-base').ok);
  env.options.lockHeld = false;
  assert(env.lockReleases > 8, 'lock releases on success and write faults');
});

check('T17 overflow retains every alert and discloses excess rows; USX must equal USD/100', () => {
  const r = render(loadTree(source), { cand: 'enforce' }, { alerts: copy(FIX.alerts).concat(copy(FIX.alerts)) });
  assert(r.p._dt10_cand_note.includes('layout OVERFLOW: 83 > 80 rows; all decision evidence retained'));
  assert.equal(r.p.alerts.length, 8);
  const t = loadTree(source);
  t.ctx.dt10FxRates_ = () => ({ USD: 3.75, USX: 3.75 });
  assert(!t.ctx.dt10SelfTest().includes('fx USX row: valid'));
  t.ctx.dt10FxRates_ = () => ({ USD: 3.75, USX: 0.0375 });
  assert(t.ctx.dt10SelfTest().includes('fx USX row: valid USD/100 (0.0375)'));
});

check('T18 real native-empty refresh commits a distinct current zero-row snapshot without a backend call', () => {
  const t = loadTree(source, { props: { DT10_CAND_TAB: 'enforce' } });
  t.ctx.dt10BoardVerdict_ = verdictStub();
  const oldRows = copy(FIX.candidates.slice(0, 2)).map(c => t.ctx.dt10CandToRow_(c));
  assert(t.ctx.dt10CandTabWrite_(t.env.ss, oldRows, 'old', null, 'old').ok);
  t.ctx.dt10CollectPoolRows_ = () => ({ rows: [], available: {}, perPage: {}, counts: {}, totalRows: 0 });
  t.ctx.dt10Post_ = () => { throw new Error('native-empty must not call backend'); };
  t.ctx.refreshDecisionTop10();
  const receipt = JSON.parse(t.env.sheets._T10_Candidates._val(1, 2));
  assert.equal(receipt.state, 'COMMITTED'); assert.equal(receipt.rows, 0);
  assert.match(receipt.run_id, /^native-empty-sheet-pool:2026-10-10T19:28:22.000Z:test-uuid-1$/);
  assert.equal(t.ctx.dt10CandTabRead_(t.env.ss, receipt.run_id).ok, true);
  assert.equal(t.ctx.dt10CandTabRead_(t.env.ss, 'old').ok, false);
  assert(String(t.env.sheets.Top_10_Investments._val(2, 2)).includes('cands 0 -> _T10_Candidates'));
  t.ctx.refreshDecisionTop10();
  const next = JSON.parse(t.env.sheets._T10_Candidates._val(1, 2));
  assert.notEqual(next.run_id, receipt.run_id); assert.equal(next.rows, 0);
  assert.equal(t.ctx.dt10CandTabRead_(t.env.ss, receipt.run_id).ok, false);
});

/* ================================================================ digest */
const digest = crypto.createHash('sha256').update(JSON.stringify(digestParts)).digest('hex');
console.log('Full-source input truth: ' + passed + ' passed' + (skipped ? ', ' + skipped + ' skipped (dual-tree cases need --base)' : ''));
console.log('SOURCE_SHA256 ' + crypto.createHash('sha256').update(source).digest('hex'));
console.log('RUN-DIGEST ' + digest.slice(0, 16));
