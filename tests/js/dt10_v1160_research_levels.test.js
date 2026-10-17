/* 16_Decision_Top10.gs v1.16.0 (2026-10-17) -- T7: research levels when sizing
 * is withheld. Gate DT10_RESEARCH_LEVELS = 'on' (Script Property, default
 * off). Off => the rendered rows are byte-identical to v1.15.0; on => on a
 * non-executable board the plan levels (entry zone, stop, TP1, TP2) return to
 * the rendered row under a RESEARCH watermark with R/R and the at-risk SAR at
 * a 5%-NAV reference size, while Ticket SAR / Shares / Gain / Funds From stay
 * withheld; EXECUTABLE boards are untouched. Payload tickets, KPIs and SelLog
 * rows are never mutated by the view. Run: node tests/js/dt10_v1160_research_levels.test.js */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {revertV1160, addedLines1160, EDITS_1160} = require('./dt10_v1160_history');
const {revertV1150} = require('./dt10_v1150_history');
const {revertV1140} = require('./dt10_v1140_history');
const {feedHistorySource} = require('./dt10_cash_utc_history');

const ROOT = path.join(__dirname, '../..');
// v1.17.0: this harness proves the v1.16.0 layer, so it runs on the delivered
// file with the v1.17.0 edits reversed (proven by dt10_v1170_model_evidence.test.js).
const {revertV1170} = require('./dt10_v1170_history');
const {revertV1180} = require('./dt10_v1180_history');
const {revertV1190} = require('./dt10_v1190_history');
const source = revertV1170(revertV1180(revertV1190(fs.readFileSync(path.join(ROOT, 'apps_script/16_Decision_Top10.gs'), 'utf8'))));
const plain = v => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));
let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + (e && e.stack || e)); }
}
function load(props) {
  const store = Object.assign({}, props || {});
  const propsService = {
    getProperty(k) { return Object.prototype.hasOwnProperty.call(store, k) ? store[k] : null; },
    setProperty(k, v) { store[k] = String(v); return propsService; },
    deleteProperty(k) { delete store[k]; return propsService; },
    getProperties() { return store; }
  };
  const ctx = {
    console, Logger: { log() {} }, Date,
    Utilities: { formatDate() { return ''; }, sleep() {}, getUuid() { return 'u'; }, computeDigest() { return []; },
      base64Encode() { return ''; }, DigestAlgorithm: { MD5: 'MD5', SHA_256: 'SHA_256' }, Charset: { UTF_8: 'UTF_8' } },
    PropertiesService: { getScriptProperties() { return propsService; }, getDocumentProperties() { return null; }, getUserProperties() { return propsService; } },
    SpreadsheetApp: { getActiveSpreadsheet() { throw new Error('no sheet in this harness'); }, getActive() { throw new Error('no sheet'); }, flush() {}, BorderStyle: { SOLID: 'SOLID' } },
    UrlFetchApp: { fetch() { throw new Error('no network'); } },
    Session: { getScriptTimeZone() { return 'Asia/Riyadh'; } },
    CacheService: { getScriptCache() { return { get() { return null; }, put() {} }; } },
    LockService: { getScriptLock() { return { tryLock() { return true; }, releaseLock() {} }; } }
  };
  vm.createContext(ctx);
  vm.runInContext(source, ctx, { filename: '16_Decision_Top10.gs' });
  return ctx;
}
const FNS = ['dt10ResearchLevelsOn_', 'dt10ResearchLevelReal_', 'dt10ResearchLevelsCapture_', 'dt10ResearchFmtSar_',
  'dt10ResearchLevelsCtx_', 'dt10ResearchLevelsRow_', 'dt10ResearchLevelsPaint_'];

/* ---------------------------------------------------------------- V */
test('V1 version lockstep, header line 4, syntax, census 212/105, the seven functions exist, no new var', () => {
  const ctx = load();
  assert.equal(ctx.DT10_VERSION, '1.16.0');
  assert.match(source.split('\n')[3], /^ \* Version: 1\.16\.0 /);
  assert(source.includes(' * v1.16.0 (2026-10-17) -- RESEARCH LEVELS WHEN SIZING IS WITHHELD (T7)'));
  assert(source.includes(' * v1.15.0 (2026-10-17) -- SECTOR CAP ON CARRIED SEATS + CLUSTER COLUMN (T2)'), 'prior WHY kept');
  assert.doesNotThrow(() => new vm.Script(source, { filename: '16_Decision_Top10.gs' }));
  const fns = new Set(); let m; const re = /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg; while ((m = re.exec(source))) fns.add(m[1]);
  const vars = new Set(); const rv = /^var\s+([A-Za-z_$][\w$]*)/mg; while ((m = rv.exec(source))) vars.add(m[1]);
  assert.equal(fns.size, 212); assert.equal(vars.size, 105);
  for (const f of FNS) { assert(fns.has(f), f); assert.equal(typeof ctx[f], 'function', f); }
});
test('V2 reversal layer: the nine v1.16.0 edits occur exactly once and the chain reaches the pinned v1.13.8 digest via v1.15.0 and v1.14.0', () => {
  assert.equal(EDITS_1160.length, 9);
  const prior = revertV1160(source);
  assert(prior.includes("var DT10_VERSION = '1.15.0';") && !prior.includes('dt10ResearchLevelsCtx_'));
  feedHistorySource(revertV1140(revertV1150(prior)));   // throws on any digest mismatch
  assert.throws(() => revertV1160(source + "\nvar DT10_VERSION = '1.16.0';"), /exactly one approved v1\.16\.0 edit/);
});
test('V3 every added line is ES5 and free of smart quotes / CR', () => {
  const bad = /\b(let|const)\b|=>|`|\bclass\b|\.\.\.|\.includes\(/;
  for (const line of addedLines1160()) {
    assert(!bad.test(line), 'ES5: ' + line);
    assert(!/[‘’“”\r]/.test(line), 'quotes/CR: ' + line);
  }
  assert.equal((source.match(/[‘’“”]/g) || []).length, 0);
});

/* ---------------------------------------------------------------- fixtures */
function ticket(sym, extra) {
  return Object.assign({ symbol: sym, name: sym + ' Co', market: 'US', sector: 'Real Estate', currency: 'USD', fx_to_sar: 3.75,
    price: 26.67, price_sar: 100, entry_zone: '96.5–98.0', suggested_sar: 4650, suggested_shares: 46, stop_sar: 90, tp1_sar: 110,
    tp2_sar: 120, roi_pct: 10, engine_roi_pct: 12, ann_roi_pct: 10, exp_gain_12m_sar: 465, reliability: 80, dq: 90,
    confidence_band: 'HIGH', advisor_note: 'Buy the dip.', rank: 1, _stab_status: 'ACTIVE (day 9)', _stab_days: 9,
    detail: { funds_from: 'Cash 53,769 SAR', review_date: '2026-11-16', rr: 1.8, rr_tp2: 2.5 } }, extra || {});
}
function withheldPayload(sel, extraMeta) {
  return { version: 'x', status: 'ok', selected: sel, kpis: { passed: sel.length, deployable_sar: 50000, max_selected: 10 },
    meta: Object.assign({}, extraMeta || {}), alerts: [], candidates_rows: [], _dt10_uv: { state: 'NOT_ACTIONABLE', reason: 'stale', ageMin: 400 } };
}

/* ---------------------------------------------------------------- R */
test('R1 gate reads DT10_RESEARCH_LEVELS = on only (case/space tolerant); default off', () => {
  assert.equal(load().dt10ResearchLevelsOn_(), false);
  assert.equal(load({ DT10_RESEARCH_LEVELS: 'on' }).dt10ResearchLevelsOn_(), true);
  assert.equal(load({ DT10_RESEARCH_LEVELS: ' ON ' }).dt10ResearchLevelsOn_(), true);
  for (const v of ['off', '1', 'true', 'observe', '']) assert.equal(load({ DT10_RESEARCH_LEVELS: v }).dt10ResearchLevelsOn_(), false, v);
});
test('R2 capture: only when on; skips holds, dashes and seats without a plan; snapshots levels + rr + price', () => {
  const off = load();
  const p0 = { selected: [ticket('A')] };
  assert.equal(off.dt10ResearchLevelsCapture_(p0), 0); assert.equal(p0.selected[0]._research_levels, undefined);
  const on = load({ DT10_RESEARCH_LEVELS: 'on' });
  const sel = [ticket('A'), ticket('G', { _grace_hold: true, entry_zone: '—', stop_sar: '—' }), ticket('F', { _ft_suspended: true }),
    ticket('P', { _p145_suspended: true }), ticket('E', { _earn_hold: true }), ticket('S', { _seat_cap_deferred: true }),
    ticket('D', { entry_zone: '—', stop_sar: '—' }), ticket('N', { entry_zone: null, stop_sar: null }), null];
  assert.equal(on.dt10ResearchLevelsCapture_({ selected: sel }), 1);
  assert.deepEqual(plain(sel[0]._research_levels), { entry_zone: '96.5–98.0', stop_sar: 90, tp1_sar: 110, tp2_sar: 120, rr: 1.8, rr_tp2: 2.5, price_sar: 100 });
  for (let i = 1; i < 8; i++) assert.equal(sel[i]._research_levels, undefined, 'seat ' + i);
  assert.equal(on.dt10ResearchLevelsCapture_(null), 0); assert.equal(on.dt10ResearchLevelsCapture_({}), 0);
  assert.equal(on.dt10ResearchLevelReal_('—'), false); assert.equal(on.dt10ResearchLevelReal_('-'), false); assert.equal(on.dt10ResearchLevelReal_(''), false);
  assert.equal(on.dt10ResearchLevelReal_(0), false); assert.equal(on.dt10ResearchLevelReal_(NaN), false); assert.equal(on.dt10ResearchLevelReal_(90), true);
  assert.equal(on.dt10ResearchLevelReal_('96.5–98.0'), true);
});
test('R3 ctx: empty when off / EXECUTABLE output / nothing captured / captured seat sized; NAV basis first, deployable fallback, n/a last', () => {
  const on = load({ DT10_RESEARCH_LEVELS: 'on' });
  const a = ticket('A', { _research_levels: { entry_zone: '96.5–98.0', stop_sar: 90, tp1_sar: 110, tp2_sar: 120, rr: 1.8, rr_tp2: 2.5, price_sar: 100 }, suggested_sar: 0, suggested_shares: 0 });
  const b = ticket('B', { suggested_sar: 0, suggested_shares: 0 });
  assert.deepEqual(plain(load().dt10ResearchLevelsCtx_(withheldPayload([a, b]), [a, b]).rows), [], 'off');
  const execPayload = { selected: [ticket('X')], kpis: { passed: 1 }, meta: {}, _dt10_uv: { state: 'EXECUTABLE' } };
  assert.equal(on.dt10OutputStatus_(execPayload), 'EXECUTABLE');
  assert.deepEqual(plain(on.dt10ResearchLevelsCtx_(execPayload, execPayload.selected).rows), [], 'executable days untouched');
  const c1 = on.dt10ResearchLevelsCtx_(withheldPayload([a, b]), [a, b]);
  assert.deepEqual(plain(c1.rows), [0]); assert.equal(c1.ref_sar, 2500); assert.equal(c1.basis_label, '5%-deployable ref 2,500 SAR (NAV unknown)');
  const c2 = on.dt10ResearchLevelsCtx_(withheldPayload([a, b], { cash_floor: { mode: 'observe', nav_sar: 23200 } }), [a, b]);
  assert.equal(c2.ref_sar, 1160); assert.equal(c2.basis_label, '5%-NAV ref 1,160 SAR');
  const p3 = withheldPayload([a], {}); p3.kpis.deployable_sar = 0;
  const c3 = on.dt10ResearchLevelsCtx_(p3, [a]); assert.equal(c3.ref_sar, 0); assert.equal(c3.basis_label, 'reference size n/a (NAV unknown)');
  const sized = ticket('S', { _research_levels: { entry_zone: 'z' } });
  const held = withheldPayload([sized], {}); held._dt10_uv = { state: 'EXECUTABLE' }; held.meta.board_funding = {};
  // HELD output (feed ok, nothing executable because kpis say so) with a sized ticket -> the seat is skipped
  assert.deepEqual(plain(on.dt10ResearchLevelsCtx_(withheldPayload([sized]), [sized]).rows), [], 'a sized seat shows no research view');
  assert.deepEqual(plain(on.dt10ResearchLevelsCtx_(null, null).rows), []);
});
test('R4 row: levels back at 9/12/13/14 after the withhold pass; Ticket SAR / Shares / Gain / Funds From stay withheld; note watermark with R/R and at-risk', () => {
  const ctx = load({ DT10_RESEARCH_LEVELS: 'on' });
  const t = ticket('A', { _research_levels: { entry_zone: '96.5–98.0', stop_sar: 90, tp1_sar: 110, tp2_sar: 120, rr: 1.8, rr_tp2: 2.5, price_sar: 100 } });
  const row = ctx.dt10UvWithholdRow_(ctx.dt10TicketToRow_(t), ctx.DT10_UV_BOARD_WITHHOLD_IDX, ctx.DT10_UV_BOARD_NOTE_IDX, 'stale');
  for (const i of [9, 12, 13, 14, 10, 11, 18, 22]) assert.equal(row[i], '—', 'withheld ' + i);
  const before = plain(row);
  ctx.dt10ResearchLevelsRow_(row, t, { rows: [0], ref_sar: 1160, basis_label: '5%-NAV ref 1,160 SAR', pct: 5 });
  assert.equal(row[9], '96.5–98.0'); assert.equal(row[12], 90); assert.equal(row[13], 110); assert.equal(row[14], 120);
  for (const i of [10, 11, 18, 22, 15, 17]) assert.equal(row[i], '—', 'still withheld ' + i);
  assert.equal(row[29], 'RESEARCH LEVELS — NOT EXECUTABLE — reference size only [R/R(TP2) 2.5; ref 5% = 1,160 SAR (11 sh) -> at risk 110 SAR (10%)]. ' + before[29]);
  assert.match(before[29], /^SIZING WITHHELD — feed NOT_ACTIONABLE \(stale\)/);
  for (const i of [0, 1, 2, 3, 4, 5, 6, 7, 8, 16, 19, 20, 21, 23, 24, 25, 26, 27, 28]) assert.equal(row[i], before[i], 'untouched ' + i);
  // no reference size: the basis label rides instead of the risk figure; rr falls back to plan rr
  const t2 = ticket('B', { _research_levels: { entry_zone: 'z', stop_sar: 90, tp1_sar: 110, tp2_sar: 120, rr: 1.8, rr_tp2: null, price_sar: 100 } });
  const row2 = ctx.dt10ResearchLevelsRow_(ctx.dt10TicketToRow_(t2), t2, { rows: [0], ref_sar: 0, basis_label: 'reference size n/a (NAV unknown)', pct: 5 });
  assert.match(row2[29], /^RESEARCH LEVELS — NOT EXECUTABLE — reference size only \[R\/R\(TP2\) 1\.8; reference size n\/a \(NAV unknown\)\]\. Buy the dip\.$/);
  // stop above price or missing: no at-risk figure, no crash
  const t3 = ticket('C', { _research_levels: { entry_zone: 'z', stop_sar: 120, tp1_sar: 130, tp2_sar: 140, price_sar: 100 } });
  assert.match(ctx.dt10ResearchLevelsRow_(ctx.dt10TicketToRow_(t3), t3, { rows: [0], ref_sar: 1000, basis_label: 'b', pct: 5 })[29], /^RESEARCH LEVELS — NOT EXECUTABLE — reference size only \[b\]\. /);
  assert.equal(ctx.dt10ResearchLevelsRow_(null, t, {}), null);
  const plainRow = ctx.dt10TicketToRow_(ticket('D')); const copy = plain(plainRow);
  assert.deepEqual(plain(ctx.dt10ResearchLevelsRow_(plainRow, ticket('D'), {})), copy, 'no snapshot -> untouched');
});
test('R5 end to end: capture -> dt10FinalizeBoard_ blanks the tickets -> withheld rows restore the levels; off and EXECUTABLE boards byte-identical; tickets never mutated by the view', () => {
  for (const props of [{}, { DT10_RESEARCH_LEVELS: 'on' }]) {
    const ctx = load(props);
    const on = !!props.DT10_RESEARCH_LEVELS;
    const sel = [ticket('VICI.US'), ticket('AX.US', { sector: 'Financials', rank: 2 }), ticket('G.US', { _grace_hold: true, entry_zone: '—', stop_sar: '—', tp1_sar: '—', tp2_sar: '—', suggested_sar: '—', suggested_shares: '—', rank: 3 })];
    const payload = withheldPayload(sel, { cash_floor: { mode: 'observe', nav_sar: 23200 } });
    ctx.dt10ResearchLevelsCapture_(payload);
    ctx.dt10FinalizeBoard_(payload);
    assert.equal(payload.selected[0].entry_zone, '—', 'finalize blanks the ticket'); assert.equal(payload.selected[0].suggested_sar, 0);
    const rows = payload.selected.map(t => ctx.dt10UvWithholdRow_(ctx.dt10TicketToRow_(t), ctx.DT10_UV_BOARD_WITHHOLD_IDX, ctx.DT10_UV_BOARD_NOTE_IDX, 'stale'));
    const baseline = plain(rows);
    const rl = ctx.dt10ResearchLevelsCtx_(payload, payload.selected);
    const ticketsBefore = JSON.stringify(payload.selected);
    for (const i of rl.rows) ctx.dt10ResearchLevelsRow_(rows[i], payload.selected[i], rl);
    if (!on) {
      assert.deepEqual(plain(rl.rows), []); assert.deepEqual(plain(rows), baseline, 'OFF: byte-identical rows');
    } else {
      assert.deepEqual(plain(rl.rows), [0, 1], 'grace ghost excluded');
      assert.equal(rl.basis_label, '5%-NAV ref 1,160 SAR');
      assert.equal(rows[0][9], '96.5–98.0'); assert.equal(rows[0][12], 90); assert.equal(rows[1][14], 120);
      assert.equal(rows[0][10], '—'); assert.equal(rows[0][11], '—'); assert.equal(rows[0][22], '—');
      assert.match(rows[0][29], /^RESEARCH LEVELS — NOT EXECUTABLE — reference size only \[R\/R\(TP2\) 2\.5; ref 5% = 1,160 SAR \(11 sh\) -> at risk 110 SAR \(10%\)\]\. SIZING WITHHELD/);
      assert.deepEqual(plain(rows[2]), baseline[2], 'grace row untouched');
    }
    assert.equal(JSON.stringify(payload.selected), ticketsBefore, 'view never mutates the tickets (SelLog / KPIs unchanged)');
    // EXECUTABLE board: nothing captured matters, ctx rows empty
    const ex = { version: 'x', selected: [ticket('E.US')], kpis: { passed: 1, deployable_sar: 50000 }, meta: {}, _dt10_uv: { state: 'EXECUTABLE' } };
    ctx.dt10ResearchLevelsCapture_(ex);
    assert.deepEqual(plain(ctx.dt10ResearchLevelsCtx_(ex, ex.selected).rows), []);
  }
});
test('R6 hooks: capture before the refresh finalize, ctx before the SELECTED section title, restore after the withhold loop, paint after the stability colouring, selftest line', () => {
  const cap = source.indexOf('  dt10ResearchLevelsCapture_(payload);   // v1.16.0 [T7]'), fin = source.indexOf('  dt10FinalizeBoard_(payload);\n  var earnHoldNote');
  assert(cap > 0 && cap < fin && fin - cap < 120, 'capture immediately before the refresh finalize');
  const ctxIdx = source.indexOf('var rlCtx = dt10ResearchLevelsCtx_(payload, selected);'), title = source.indexOf('row = dt10WriteSection_(sheet, row, dt10SelTitle, tokens);');
  assert(ctxIdx > 0 && ctxIdx < title, 'ctx computed before the title is written');
  assert(source.includes("dt10SelTitle += ' | RESEARCH LEVELS x' + rlCtx.rows.length +"));
  const wh = source.indexOf('DT10_UV_BOARD_NOTE_IDX, dt10Uv.reason);'), rest = source.indexOf('dt10ResearchLevelsRow_(selRows[rlCtx.rows[rlI]], selected[rlCtx.rows[rlI]], rlCtx);'), write = source.indexOf('var selT = dt10WriteTable_(sheet, row, selHeaders, selRows,');
  assert(wh > 0 && wh < rest && rest < write, 'restore sits after the withhold loop and before the table write');
  const col = source.indexOf('dt10ColorStability_(sheet, selT.firstDataRow, selT.count, 25, tokens);'), paint = source.indexOf('dt10ResearchLevelsPaint_(sheet, selT.firstDataRow, rlCtx.rows);');
  assert(col > 0 && col < paint && paint - col < 200, 'paint right after the stability colouring');
  assert(source.includes("report.push('research levels core: ' +"));
  // the paint helper never throws on a faulty sheet
  const ctx = load();
  assert.doesNotThrow(() => ctx.dt10ResearchLevelsPaint_({ getRange() { throw new Error('boom'); } }, 5, [0, 1]));
  assert.doesNotThrow(() => ctx.dt10ResearchLevelsPaint_(null, 5, []));
  let painted = []; const sheet = { getRange(r, c) { painted.push([r, c]); return { setFontStyle() { return this; }, setFontColor() { return this; } }; } };
  ctx.dt10ResearchLevelsPaint_(sheet, 40, [0, 2]);
  assert.deepEqual(painted, [[40, 10], [40, 13], [40, 14], [40, 15], [42, 10], [42, 13], [42, 14], [42, 15]]);
});

console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
