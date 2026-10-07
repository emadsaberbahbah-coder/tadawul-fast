/* Execute the COMPLETE deployed Apps Script source. Only service boundaries
 * (HTTP, PropertiesService, spreadsheet painting) are replaced with fixtures. */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../../apps_script/16_Decision_Top10.gs'), 'utf8');
const copy = value => JSON.parse(JSON.stringify(value));
let passed = 0;
function context() {
  const properties = {DT10_HARD_VERDICT_STRICT: '1', DT10_P144_EPOCH_KEY_LEGACY: '1'};
  const ctx = {Logger: {log() {}}, PropertiesService: {getScriptProperties() {
    return {getProperty(k) {return properties[k] || null;},
      setProperty(k, v) {properties[k] = v;}, deleteProperty(k) {delete properties[k];}};
  }}, SpreadsheetApp: {getActiveSpreadsheet() {return {};}}};
  vm.createContext(ctx); vm.runInContext(source, ctx, {filename: '16_Decision_Top10.gs'});
  ctx.dt10StabToday_ = () => '2026-10-07';
  return {ctx, properties};
}
function ticket(symbol, score = 90, overrides = {}) {
  return Object.assign({symbol, name: 'Synthetic ' + symbol, market: 'Tadawul',
    sector: 'Energy', currency: 'SAR', price: 100, price_sar: 100,
    entry_zone: '100', stop_sar: 90, tp1_sar: 120, tp2_sar: 130,
    suggested_sar: 10000, suggested_shares: 100, exp_gain_12m_sar: 2000,
    engine_exp_gain_12m_sar: 2500, valuation_exp_gain_12m_sar: 3000,
    reliability: 80, roi_pct: 20, ann_roi_pct: 80, opportunity_score: score,
    advisor_note: 'Synthetic plan', detail: {rr: 2, rr_tp2: 3, funds_from: 'Cash'}}, overrides);
}
function payload(seats) {
  const snapshot = {contract_version: 1, builder_version: '1.24.1', snapshot_id: 'synthetic-signed-id',
    rows: seats.map(t => ({symbol: t.symbol})), xchecks: {}};
  return {version: '1.24.1', status: 'ok', selected: seats,
    candidates_rows: seats.map(t => ({symbol: t.symbol, opportunity_score: t.opportunity_score,
      verdict: 'INVEST', selected: true, structural_block: false})),
    kpis: {deployable_sar: 10000, expected_gain_12m_sar: 12345,
      engine_expected_gain_12m_sar: 40000, valuation_expected_gain_12m_sar: 50000,
      selected_count: 3, fundable_now: 3, fundable_by_rotation: 1, capital_call: 2,
      capital_call_topn_sar: 10000, capital_unallocated_sar: 0, passed: seats.length,
      max_selected: 10, scanned: seats.length},
    alerts: [{type: 'capital_call', required_action: 'Deposit synthetic cash'},
      {type: 'rotation_proposal', required_action: 'Synthetic rotation'}, {type: 'missing_fx'}],
    near_miss: [{symbol: 'RESEARCH.SR', failed_gate: 'Funding', required: 'Deposit 5000',
      current: 'CAPITAL_CALL', improve_note: 'Deposit synthetic cash'}],
    meta: {board_funding: {contract_version: 1, stage: 'research', snapshot_available: true,
      snapshot_id: snapshot.snapshot_id, snapshot}, board_execution_basis: {blended_rr: 'tp2'}},
    _dt10_uv: {state: 'EXECUTABLE', reason: ''}};
}
const request = {criteria: {board_funding_stage: 'research', max_selected: 10},
  portfolio: {cash_available_sar: 10000}, fx_rates: {SAR: 1}};
function allocated(original, tickets, extraKpis = {}) {
  return {code: 200, json: {version: original.version, status: 'ok', selected: tickets,
    candidates_rows: tickets.map(t => ({symbol: t.symbol, verdict: 'INVEST'})),
    alerts: [], near_miss: [], kpis: Object.assign({fundable_by_rotation: 0, capital_call: 0,
      capital_call_topn_sar: 0}, extraKpis), meta: {board_funding: {contract_version: 1,
      stage: 'allocate', snapshot_id: original.meta.board_funding.snapshot_id}}}};
}
function check(name, fn) {fn(); passed++; console.log('PASS ' + name);}
function zeroMoney(ctx, p) {
  assert.equal(p.kpis.selected_count, 0); assert.equal(p.kpis.fundable_now, 0);
  assert.equal(p.kpis.expected_gain_12m_sar, 0); assert.equal(p.kpis.total_suggested_sar, 0);
  assert.equal(p.kpis.capital_unallocated_sar, 10000);
  assert.equal(p.kpis.capital_call_topn_sar, 0); assert.equal(p.kpis.fundable_by_rotation, 0);
  assert.equal(p.kpis.engine_expected_gain_12m_sar, 0);
  assert.equal(p.kpis.valuation_expected_gain_12m_sar, 0);
  assert(!p.alerts.some(ctx.dt10BoardFundingAlert_));
  assert(!JSON.stringify(p.near_miss).match(/deposit|capital_call/i));
  assert.equal(ctx.dt10SeatCheckNote_(p), ''); assert.equal(ctx.dt10KpiCheckNote_(p), '');
}
check('HELD zero exec removes spend/gain/reservations/capital and rotation alerts', () => {
  const {ctx} = context();
  const p = payload([ticket('GRACE.SR', 90, {_grace_hold: true, _stab_status: 'GRACE (1/3 missed)'}),
    ticket('FAST.SR', 95, {_ft_suspended: true, _stab_status: 'FAST-TRACK (day 1)'})]);
  ctx.dt10Post_ = () => {throw new Error('HELD board must not request allocation');};
  ctx.dt10ReallocateBoard_(p, request); ctx.dt10FinalizeBoard_(p);
  zeroMoney(ctx, p); assert.equal(ctx.dt10OutputStatus_(p), 'HELD');
  assert.equal(p.selected[0].name, 'Synthetic GRACE.SR');
  assert.equal(p.selected[1]._stab_status, 'FAST-TRACK (day 1)');
  p.selected.forEach(t => {
    const row = ctx.dt10SelLogRowFromTicket_(t, 'synthetic-stamp', 'synthetic-run', '{}');
    assert.equal(row[13], 0); assert.equal(row[14], 0); assert.equal(row[21], 0);
  });
  assert.equal(p.meta.board_funding.snapshot, undefined);
  const once = JSON.stringify(p); ctx.dt10FinalizeBoard_(p); assert.equal(JSON.stringify(p), once);
});
check('mixed board funds later confirmed seat without research cash/sector reservation', () => {
  const {ctx} = context();
  const p = payload([ticket('HIGH.SR', 99, {_ft_suspended: true, _board_research: true}),
    ticket('LATER.SR', 80, {_board_research: true, suggested_sar: 0, suggested_shares: 0})]);
  let calls = 0;
  ctx.dt10Post_ = body => {
    calls++; assert.deepEqual(copy(body.criteria.board_funding_symbols), ['LATER.SR']);
    assert.deepEqual(copy(body.rows), p.meta.board_funding.snapshot.rows);
    assert.equal(body.criteria.board_funding_stage, 'allocate');
    assert.deepEqual(copy(body.portfolio), request.portfolio);
    return allocated(p, [ticket('LATER.SR', 80)]);
  };
  ctx.dt10ReallocateBoard_(p, request); ctx.dt10FinalizeBoard_(p);
  assert.equal(calls, 1); assert.equal(p.selected[0].suggested_sar, 0);
  assert.equal(p.selected[1].suggested_sar, 10000); assert.equal(p.kpis.total_suggested_sar, 10000);
  assert.equal(p.kpis.expected_gain_12m_sar, 2000); assert.equal(p.kpis.selected_count, 1);
  assert.equal(p.kpis.blended_reliability, 80); assert.equal(p.kpis.blended_rr, 3);
  assert.equal(ctx.dt10SeatCheckNote_(p), ''); assert.equal(ctx.dt10KpiCheckNote_(p), '');
  assert.equal(p.candidates_rows[0].selected, false); assert.equal(p.candidates_rows[1].selected, true);
  const once = JSON.stringify(p); ctx.dt10FinalizeBoard_(p); assert.equal(JSON.stringify(p), once);
});
for (const failure of ['throw', 'http', 'version', 'snapshot', 'unsupported']) {
  check('allocation ' + failure + ' fails closed with private snapshot removed', () => {
    const {ctx} = context(); const p = payload([ticket('ACTIVE.SR')]);
    if (failure === 'unsupported') delete p.meta.board_funding.contract_version;
    ctx.dt10Post_ = () => {
      if (failure === 'throw') throw new Error('synthetic network failure');
      const reply = allocated(p, [ticket('ACTIVE.SR')]);
      if (failure === 'http') reply.code = 503;
      if (failure === 'version') reply.json.version = 'legacy';
      if (failure === 'snapshot') reply.json.meta.board_funding.snapshot_id = 'different';
      return reply;
    };
    ctx.dt10ReallocateBoard_(p, request); ctx.dt10FinalizeBoard_(p); zeroMoney(ctx, p);
    assert.equal(p.meta.board_funding.snapshot, undefined);
    assert(p.alerts.some(a => a.type === 'board_funding_unavailable'));
  });
}
for (const mode of ['research', 'unknown']) {
check('rollout mode ' + mode + ' withholds replay and all monetary claims', () => {
  const {ctx, properties} = context(); const p = payload([ticket('ACTIVE.SR')]);
  properties.DT10_BOARD_FUNDING_MODE = mode;
  ctx.dt10Post_ = () => {throw new Error('research-only mode must not request allocation');};
  ctx.dt10ReallocateBoard_(p, request); ctx.dt10FinalizeBoard_(p); zeroMoney(ctx, p);
  assert.equal(p.meta.board_funding.mode, 'research');
  assert(ctx.dt10MetaLine_(p.meta).includes('funding=research/0exec'));
  const once = JSON.stringify(p); ctx.dt10FinalizeBoard_(p); assert.equal(JSON.stringify(p), once);
});
}
check('rollout brake changed after allocation still withholds at render finalization', () => {
  const {ctx, properties} = context(); const p = payload([ticket('ACTIVE.SR')]);
  ctx.dt10Post_ = () => allocated(p, [ticket('ACTIVE.SR')]);
  ctx.dt10ReallocateBoard_(p, request);
  properties.DT10_BOARD_FUNDING_MODE = 'research';
  ctx.dt10FinalizeBoard_(p); zeroMoney(ctx, p);
  assert.equal(p.meta.board_funding.finalized, false);
});
check('rollout control read failure withholds allocation and monetary claims', () => {
  const {ctx} = context(); const p = payload([ticket('ACTIVE.SR')]);
  ctx.PropertiesService.getScriptProperties = () => {throw new Error('synthetic property outage');};
  ctx.dt10Post_ = () => {throw new Error('unknown rollout control must not allocate');};
  ctx.dt10ReallocateBoard_(p, request); ctx.dt10FinalizeBoard_(p); zeroMoney(ctx, p);
  assert.equal(p.meta.board_funding.mode, 'research');
});
check('feed withheld blocks replay and all funding even after successful allocation', () => {
  const {ctx} = context(); const p = payload([ticket('ACTIVE.SR')]);
  p._dt10_uv.state = 'NOT_ACTIONABLE';
  ctx.dt10Post_ = () => {throw new Error('withheld feed must not allocate');};
  ctx.dt10ReallocateBoard_(p, request); ctx.dt10FinalizeBoard_(p); zeroMoney(ctx, p);
  assert.equal(ctx.dt10OutputStatus_(p), 'WITHHELD');
});
for (const mode of ['legacy', 'research']) {
check('direct ' + mode + ' renderer finalizes before writing monetary KPI/alert and removes snapshot', () => {
  const {ctx} = context(); const p = payload([ticket('LEGACY.SR')]);
  if (mode === 'legacy') delete p.meta.board_funding;
  const writes = [], tables = [];
  const range = new Proxy({}, {get(_target, method) {
    if (method === 'setValues') return values => {writes.push(copy(values)); return range;};
    return () => range;
  }});
  const sheet = {getMaxRows() {return 100;}, getMaxColumns() {return 60;},
    getRange() {return range;}, getParent() {return {};}};
  ctx.dt10BoardVerdict_ = () => ({state: 'EXECUTABLE', reason: ''});
  ctx.dt10WriteSection_ = (_sheet, row) => row + 1;
  ctx.dt10WriteTable_ = (_sheet, row, headers, rows) => {
    tables.push(copy({headers, rows})); return {firstDataRow: 0, count: 0, next: row + 1};
  };
  ctx.dt10RenderPayload_(sheet, p, {}); zeroMoney(ctx, p);
  assert.equal(writes[0][0][1], 0); assert.equal(writes[0][0][7], 10000);
  assert(!JSON.stringify(tables).match(/Deposit synthetic|Synthetic rotation/));
  assert.equal(p.selected[0].entry_zone, '\u2014');
  assert(!p.meta.board_funding || p.meta.board_funding.snapshot === undefined);
});
}
check('stability confirmation advances once and confirmed lower rank precedes fast-track fill', () => {
  const {ctx, properties} = context();
  properties[ctx.DT10_STAB_PROP] = JSON.stringify({v: 1, date: '2026-10-06', symbols: {
    'LATER.SR': {ci: 2, co: 0, member: false, since: '', ls: '2026-10-06', hist: [80], ft: false}
  }});
  const panel = {'T10: Stability Enabled': 'Yes', 'T10: Max Selected': 2,
    'T10: Stability Confirm Days': 3};
  const make = () => payload([ticket('HIGH.SR', 99, {_board_research: true, suggested_shares: 0, suggested_sar: 0}),
    ticket('LATER.SR', 80, {_board_research: true, suggested_shares: 0, suggested_sar: 0})]);
  const p = make(); ctx.dt10ApplyStability_(p, panel, null);
  assert.equal(p.selected[0].symbol, 'LATER.SR'); assert.equal(p.selected[1]._ft_suspended, true);
  const first = JSON.parse(properties[ctx.DT10_STAB_PROP]);
  assert.equal(first.symbols['LATER.SR'].ci, 3); assert.equal(first.symbols['HIGH.SR'].ci, 1);
  const rerun = make(); ctx.dt10ApplyStability_(rerun, panel, null);
  const second = JSON.parse(properties[ctx.DT10_STAB_PROP]);
  assert.equal(second.symbols['LATER.SR'].ci, 3); assert.equal(second.symbols['HIGH.SR'].ci, 1);
  assert.deepEqual(second, first);
});
check('stability OFF mirrors capped backend allocation and freezes clocks', () => {
  const {ctx, properties} = context();
  properties[ctx.DT10_STAB_PROP] = JSON.stringify({v: 1, date: '2026-10-06', symbols: {}});
  const before = properties[ctx.DT10_STAB_PROP];
  const p = payload(Array.from({length: 30}, (_, i) => ticket('S' + i + '.SR', 99-i, {_board_research: true})));
  ctx.dt10ApplyStability_(p, {'T10: Stability Enabled': 'No'}, null);
  ctx.dt10Post_ = () => allocated(p, [ticket('S29.SR')]);
  ctx.dt10ReallocateBoard_(p, request); ctx.dt10FinalizeBoard_(p);
  assert.equal(p.selected.length, 1); assert.equal(p.selected[0].symbol, 'S29.SR');
  assert.equal(properties[ctx.DT10_STAB_PROP], before);
});
check('legitimate shortfall targets only confirmed final actions with zero reservation', () => {
  const {ctx} = context(); const p = payload([ticket('ACTIVE.SR', 90, {_board_research: true})]);
  ctx.dt10Post_ = () => {
    const reply = allocated(p, [], {capital_call: 1, capital_call_topn_sar: 1000});
    reply.json.alerts = [{type: 'capital_call', required_action: 'Synthetic confirmed shortfall'}];
    reply.json.near_miss = [{symbol: 'ACTIVE.SR', failed_gate: 'Funding', required: 'Synthetic confirmed shortfall'}];
    return reply;
  };
  ctx.dt10ReallocateBoard_(p, request); ctx.dt10FinalizeBoard_(p);
  assert.equal(p.kpis.total_suggested_sar, 0); assert.equal(p.kpis.expected_gain_12m_sar, 0);
  assert.equal(p.kpis.capital_call_topn_sar, 1000);
  assert.equal(p.alerts.filter(a => a.type === 'capital_call').length, 1);
  assert.equal(ctx.dt10OutputStatus_(p), 'QUALIFIED_UNFUNDED');
});
check('weighted KPI verification uses executable seats and preserves unknown parallel gain', () => {
  const {ctx} = context(); const p = payload([ticket('A.SR'), ticket('B.SR')]);
  ctx.dt10Post_ = () => allocated(p, [
    ticket('A.SR', 90, {suggested_sar: 5000, reliability: 80,
      engine_exp_gain_12m_sar: null, detail: {rr: 2, rr_tp2: 3}}),
    ticket('B.SR', 80, {suggested_sar: 2000, reliability: 60,
      engine_exp_gain_12m_sar: null, detail: {rr: 2, rr_tp2: 5}})
  ]);
  ctx.dt10ReallocateBoard_(p, request); ctx.dt10FinalizeBoard_(p);
  assert.equal(p.kpis.blended_reliability, 74.3); assert.equal(p.kpis.blended_rr, 3.57);
  assert.equal(p.kpis.engine_expected_gain_12m_sar, null);
  assert.equal(ctx.dt10KpiCheckNote_(p), '');
});

/* Final cash-floor observations use the final executable list. The oracle runs
 * the unchanged real Python backend once; no HTTP, credentials, or saved data. */
function cashContext(post = 1000, mode = 'observe', updates = {}) {
  return Object.assign({mode, pct: 10, nav_sar: 50000, pct_floor_sar: 5000,
    abs_floor_sar: 200, floor_sar: 5000, deployable_pre_sar: 9000,
    deployable_post_sar: post, seats_sized: 9, would_unfund_seats: 8,
    would_unfund_sar: 777, synthetic_preserve_marker: {keep: true}}, updates);
}
function cashCase(name, values, post = 1000, mode = 'observe', updates = {}) {
  return {name, ctx: cashContext(post, mode, updates), picked: values.map((value, i) =>
    ticket('ROUND' + i + '.SR', 90-i, {suggested_sar: value, suggested_shares: 1}))};
}
const cashCases = [
  cashCase('zero executable', []),
  cashCase('single under budget', [999]),
  cashCase('single exact budget', [1000]),
  cashCase('single over budget', [1001]),
  cashCase('all integers fit', [200, 300, 500]),
  cashCase('partial crossing then full seat', [600, 600, 400]),
  cashCase('ordered crossing first permutation', [800, 300, 100]),
  cashCase('ordered crossing second permutation', [100, 300, 800]),
  cashCase('zero post budget', [1, 2, 3], 0),
  cashCase('large post budget', [1, 2, 3], 100000),
  cashCase('exact post plus half tolerance', [1000], 999.5),
  cashCase('above post plus half tolerance', [1000], 999.499999999),
  cashCase('below post plus half tolerance', [1000], 999.500000001),
  cashCase('cumulative final tolerance seat', [400, 600], 999.5),
  cashCase('ticket just below half', [1000.499999999]),
  cashCase('ticket even half rounds down', [1000.5]),
  cashCase('ticket odd half rounds up', [1001.5]),
  cashCase('ticket just above half', [1000.500000001]),
  cashCase('displayed shortfall half sum', [1000, 1], 999.5),
  cashCase('shortfall even half', [1000], 997.5),
  cashCase('shortfall odd half', [1000], 998.5),
  cashCase('small half-even ticket sizes', [0.5, 1.5, 2.5, 3.5], 4),
  cashCase('zero raw backend picked value', [0]),
  cashCase('null raw backend picked value', [null]),
  cashCase('integer and fractional combination', [101, 99.49, 200.5, 299.51], 500),
  cashCase('absolute reserve stricter', [600, 600], 1000, 'observe',
    {abs_floor_sar: 6000, floor_sar: 6000}),
  cashCase('fractional policy formatting remains exact', [600, 600], 1000, 'observe',
    {pct: 12.345, nav_sar: 54321.49, pct_floor_sar: 6789.01,
      abs_floor_sar: 987.655, floor_sar: 6789.01, deployable_pre_sar: 9999.5}),
  cashCase('missing post value is zero', [100], 1000, 'observe', {deployable_post_sar: null}),
  cashCase('enforce selected', [600, 600], 1000, 'enforce'),
  cashCase('enforce empty', [], 1000, 'enforce'),
  cashCase('off selected', [600, 600], 1000, 'off'),
  cashCase('off empty', [], 1000, 'off'),
  cashCase('final mixed board executable subset', [600, 600]),
  cashCase('later rendering withholds all executables', [])
];
const roundValues = [0, 0.5, 1.5, 2.5, 3.5, -0.5, -1.5, -2.5, -3.5,
  1000.499999999, 1000.5, 1001.5, 1000.500000001];
function backendCashOracle(cases) {
  const {spawnSync} = require('node:child_process');
  const program = String.raw`
import copy, json, sys
from core.analysis import opportunity_builder as ob
body = json.load(sys.stdin)
rows = []
for case in body['cases']:
    initial = copy.deepcopy(case['ctx'])
    initial_alert = ob._cash_floor_alert_text(initial)
    final = ob._cash_floor_finalize(copy.deepcopy(initial), copy.deepcopy(case['picked']))
    rows.append({'name': case['name'], 'ctx': final, 'initial_alert': initial_alert,
                 'alert': ob._cash_floor_alert_text(final)})
print(json.dumps({'cases': rows, 'rounds': [round(float(v), 0) for v in body['rounds']]}))
`;
  const response = spawnSync(process.env.PYTHON || 'python', ['-c', program], {
    cwd: path.join(__dirname, '../..'),
    input: JSON.stringify({cases, rounds: roundValues}), encoding: 'utf8', timeout: 30000
  });
  assert.equal(response.status, 0, 'real backend cash-floor oracle: ' + response.stderr);
  return JSON.parse(response.stdout);
}
const cashOracle = backendCashOracle(cashCases);
function addCashAlert(p, ctx, initialAction) {
  p.meta.cash_floor = copy(ctx);
  p.alerts.push({type: 'cash_floor', count: ctx.would_unfund_seats,
    required_action: initialAction, synthetic_alert_marker: 'keep'});
  return p;
}
function onlyCashAlert(p) {
  const alerts = p.alerts.filter(a => a.type === 'cash_floor');
  assert.equal(alerts.length, 1); return alerts[0];
}
function stableCashPolicy(before, after) {
  const immutable = Object.keys(before).filter(key => ![
    'seats_sized', 'would_unfund_seats', 'would_unfund_sar'].includes(key));
  immutable.forEach(key => assert.deepEqual(copy(after[key]), copy(before[key]), key));
}
function zeroFinalCash(p) {
  assert.equal(p.meta.cash_floor.seats_sized, 0);
  assert.equal(p.meta.cash_floor.would_unfund_seats, 0);
  assert.equal(p.meta.cash_floor.would_unfund_sar, 0);
  const alert = onlyCashAlert(p);
  assert.equal(alert.count, 0);
  assert(alert.required_action.includes('0 of 0 sized seat(s) would lose funding (0 SAR).'));
  assert(alert.required_action.endsWith('No ticket changed.'));
}
check('cash-floor banker rounding matches Python including negative and neighboring half values', () => {
  const {ctx} = context();
  // Signed zero has the same cash value and displayed amount.
  assert.deepEqual(roundValues.map(v => ctx.dt10CashFloorRound_(v) || 0),
    cashOracle.rounds.map(v => v || 0));
});
cashCases.forEach((test, i) => check('cash-floor real backend parity: ' + test.name, () => {
  const {ctx} = context();
  const p = addCashAlert(payload(copy(test.picked)), test.ctx, cashOracle.cases[i].initial_alert);
  const immutable = copy(p), executables = copy(test.picked), beforeTickets = copy(executables);
  ctx.dt10Post_ = () => {throw new Error('pure cash-floor observation must never allocate');};
  ctx.dt10BoardEligible_ = () => {throw new Error('observation must not add an eligibility gate');};
  ctx.dt10BoardFundingMode_ = () => {throw new Error('observation must not change rollout policy');};
  ctx.dt10FinalizeCashFloor_(p, executables);
  assert.deepEqual(copy(p.meta.cash_floor), cashOracle.cases[i].ctx);
  assert.deepEqual(executables, beforeTickets);
  assert.deepEqual(p.selected, immutable.selected); assert.deepEqual(p.kpis, immutable.kpis);
  assert.deepEqual(p.near_miss, immutable.near_miss);
  assert.deepEqual(p.meta.board_funding, immutable.meta.board_funding);
  assert.deepEqual(p._dt10_uv, immutable._dt10_uv);
  const withoutCashObservation = value => {
    const out = copy(value); delete out.meta.cash_floor;
    out.alerts = out.alerts.filter(a => a.type !== 'cash_floor'); return out;
  };
  assert.deepEqual(withoutCashObservation(p), withoutCashObservation(immutable));
  stableCashPolicy(test.ctx, p.meta.cash_floor);
  const alert = onlyCashAlert(p);
  assert.equal(alert.synthetic_alert_marker, 'keep');
  assert.equal(alert.required_action, test.ctx.mode === 'observe' ?
    cashOracle.cases[i].alert : cashOracle.cases[i].initial_alert);
  assert.equal(alert.count, test.ctx.mode === 'observe' ?
    cashOracle.cases[i].ctx.would_unfund_seats : test.ctx.would_unfund_seats);
  assert.deepEqual(p.alerts.filter(a => a.type !== 'cash_floor'),
    immutable.alerts.filter(a => a.type !== 'cash_floor'));
  const once = JSON.stringify(p);
  ctx.dt10FinalizeCashFloor_(p, executables); assert.equal(JSON.stringify(p), once);
}));
check('research cash-floor sizing is cleared by final grace and suspended board without replay', () => {
  const {ctx} = context();
  const p = addCashAlert(payload([
    ticket('GRACE_CASH.SR', 99, {_grace_hold: true, _stab_status: 'GRACE (1/3 missed)'}),
    ticket('FAST_CASH.SR', 95, {_ft_suspended: true}),
    ticket('RISK_CASH.SR', 90, {_p145_suspended: true})
  ]), cashContext(1000, 'observe', {seats_sized: 1, would_unfund_seats: 1, would_unfund_sar: 9000}),
  'Cash floor 10% of NAV 50,000 SAR = 5,000 SAR (observe): deployable would fall 9,000 SAR -> 1,000 SAR; 1 of 1 sized seat(s) would lose funding (9,000 SAR). No ticket changed.');
  const policy = copy(p.meta.cash_floor), otherAlerts = p.alerts.filter(a => a.type === 'missing_fx');
  ctx.dt10Post_ = () => {throw new Error('no final eligible seat: no allocation HTTP');};
  ctx.dt10ReallocateBoard_(p, request); ctx.dt10FinalizeBoard_(p);
  zeroMoney(ctx, p); zeroFinalCash(p); stableCashPolicy(policy, p.meta.cash_floor);
  assert.deepEqual(p.alerts.filter(a => a.type === 'missing_fx'), otherAlerts);
  const once = JSON.stringify(p); ctx.dt10FinalizeBoard_(p); assert.equal(JSON.stringify(p), once);
});
check('successful replay cash-floor uses only final seats in board order and rounded ticket sizes', () => {
  const {ctx} = context();
  const seats = [ticket('HOLD_CASH.SR', 99, {_ft_suspended: true}),
    ticket('ONE_CASH.SR', 90), ticket('TWO_CASH.SR', 80), ticket('THREE_CASH.SR', 70),
    ticket('ZERO_CASH.SR', 60)];
  const p = payload(seats), cf = cashContext(4001.5);
  const expected = [2000.5, 2001.5, 3000.5].map((amount, i) =>
    ticket(['ONE_CASH.SR', 'TWO_CASH.SR', 'THREE_CASH.SR'][i], 90-i*10,
      {suggested_sar: amount, suggested_shares: 1}));
  let calls = 0;
  ctx.dt10Post_ = body => {
    calls++; assert.deepEqual(copy(body.criteria.board_funding_symbols),
      ['ONE_CASH.SR', 'TWO_CASH.SR', 'THREE_CASH.SR', 'ZERO_CASH.SR']);
    const reply = allocated(p, expected.concat(ticket('ZERO_CASH.SR', 60,
      {suggested_sar: 20000, suggested_shares: 0})));
    reply.json.meta.cash_floor = copy(cf);
    reply.json.alerts.push({type: 'cash_floor', count: 8,
      required_action: 'Cash floor 10% of NAV 50,000 SAR = 5,000 SAR (observe): deployable would fall 9,000 SAR -> 4,002 SAR; 8 of 9 sized seat(s) would lose funding (777 SAR). No ticket changed.'});
    return reply;
  };
  ctx.dt10ReallocateBoard_(p, request); ctx.dt10FinalizeBoard_(p);
  assert.equal(calls, 1); assert.equal(p.kpis.selected_count, 3);
  assert.equal(p.meta.cash_floor.seats_sized, 3);
  assert.equal(p.meta.cash_floor.would_unfund_seats, 1);
  assert.equal(p.meta.cash_floor.would_unfund_sar, 3000);
  stableCashPolicy(cf, p.meta.cash_floor);
  assert.deepEqual(p.selected.filter(t => t.suggested_shares > 0).map(t => t.suggested_sar),
    expected.map(t => t.suggested_sar));
  assert.equal(p.selected[0].suggested_sar, 0); assert.equal(p.selected[4].suggested_sar, 0);
  const alert = onlyCashAlert(p);
  assert.equal(alert.count, 1);
  assert(alert.required_action.includes('deployable would fall 9,000 SAR -> 4,002 SAR; 1 of 3'));
  assert(alert.required_action.includes('would lose funding (3,000 SAR). No ticket changed.'));
  const once = JSON.stringify(p); ctx.dt10FinalizeBoard_(p);
  assert.equal(JSON.stringify(p), once); assert.equal(calls, 1);
});
for (const reason of ['feed before replay', 'feed after replay', 'mode before replay',
  'mode after replay', 'control read failure', 'replay failure']) {
  check('cash-floor final executable denominator cleared on ' + reason, () => {
    const {ctx, properties} = context();
    const p = addCashAlert(payload([ticket('ACTIVE_CASH.SR')]), cashContext(),
      cashOracle.cases[0].initial_alert);
    const policy = copy(p.meta.cash_floor);
    let calls = 0;
    ctx.dt10Post_ = () => {
      calls++;
      if (reason === 'replay failure') throw new Error('synthetic replay failure');
      const reply = allocated(p, [ticket('ACTIVE_CASH.SR')]);
      reply.json.meta.cash_floor = copy(policy);
      reply.json.alerts.push(copy(onlyCashAlert(p)));
      return reply;
    };
    if (reason === 'feed before replay') p._dt10_uv.state = 'NOT_ACTIONABLE';
    if (reason === 'mode before replay') properties.DT10_BOARD_FUNDING_MODE = 'research';
    if (reason === 'control read failure') ctx.PropertiesService.getScriptProperties = () => {
      throw new Error('synthetic property outage');
    };
    ctx.dt10ReallocateBoard_(p, request);
    if (reason === 'feed after replay') p._dt10_uv.state = 'NOT_ACTIONABLE';
    if (reason === 'mode after replay') properties.DT10_BOARD_FUNDING_MODE = 'research';
    ctx.dt10FinalizeBoard_(p); zeroMoney(ctx, p); zeroFinalCash(p);
    stableCashPolicy(policy, p.meta.cash_floor);
    assert.equal(calls, ['feed after replay', 'mode after replay', 'replay failure'].includes(reason) ? 1 : 0);
    const once = JSON.stringify(p); ctx.dt10FinalizeBoard_(p);
    assert.equal(JSON.stringify(p), once);
  });
}
check('missing cash-floor metadata or alert never creates a reserve policy or a countable alert', () => {
  const {ctx} = context();
  const p = payload([ticket('NO_FLOOR.SR')]), before = copy(p);
  ctx.dt10FinalizeCashFloor_(p, p.selected);
  assert.deepEqual(p, before); assert(!p.meta.hasOwnProperty('cash_floor'));
  p.meta.cash_floor = cashContext();
  ctx.dt10FinalizeCashFloor_(p, []);
  assert.equal(p.meta.cash_floor.seats_sized, 0);
  assert(!p.alerts.some(a => a.type === 'cash_floor'));
});
check('unexpected observe wording emits final facts without invented policy arithmetic or stale counts', () => {
  const {ctx} = context();
  const p = addCashAlert(payload([]), cashContext(),
    'Synthetic older message: 8 of 9 sized seat(s) would lose funding (777 SAR).');
  ctx.dt10FinalizeCashFloor_(p, []);
  const action = onlyCashAlert(p).required_action;
  assert(action.includes('0 of 0 sized seat(s) would lose funding (0 SAR).'));
  assert(!action.includes('8 of 9')); assert(!action.includes('777'));
  assert(!action.includes('50,000') && !action.includes('5,000') && !action.includes('9,000'));
  const once = JSON.stringify(p); ctx.dt10FinalizeCashFloor_(p, []);
  assert.equal(JSON.stringify(p), once);
});


check('authoritative render feed withholding after successful replay clears displayed floor observations', () => {
  const {ctx} = context();
  const p = addCashAlert(payload([ticket('RENDER_CASH.SR')]), cashContext(),
    cashOracle.cases[0].initial_alert);
  const policy = copy(p.meta.cash_floor);
  let calls = 0, verdictReads = 0;
  ctx.dt10Post_ = () => {
    calls++; assert.equal(calls, 1, 'renderer must not request a new allocation');
    const reply = allocated(p, [ticket('RENDER_CASH.SR')]);
    reply.json.meta.cash_floor = copy(policy);
    reply.json.alerts.push(copy(onlyCashAlert(p)));
    return reply;
  };
  ctx.dt10ReallocateBoard_(p, request);
  assert.equal(calls, 1); assert.equal(p._dt10_uv.state, 'EXECUTABLE');
  const writes = [], tables = [];
  const range = new Proxy({}, {get(_target, method) {
    if (method === 'setValues') return values => {writes.push(copy(values)); return range;};
    return () => range;
  }});
  const sheet = {getMaxRows() {return 100;}, getMaxColumns() {return 60;},
    getRange() {return range;}, getParent() {return {};}};
  ctx.dt10BoardVerdict_ = () => {
    verdictReads++; return {state: 'NOT_ACTIONABLE', reason: 'Synthetic final feed withholding'};
  };
  ctx.dt10WriteSection_ = (_sheet, row) => row + 1;
  ctx.dt10WriteTable_ = (_sheet, row, headers, rows) => {
    tables.push(copy({headers, rows})); return {firstDataRow: 0, count: 0, next: row + 1};
  };
  ctx.dt10RenderPayload_(sheet, p, {});
  assert(verdictReads > 0); assert.equal(p._dt10_uv.state, 'NOT_ACTIONABLE');
  assert.equal(calls, 1); zeroMoney(ctx, p); zeroFinalCash(p);
  stableCashPolicy(policy, p.meta.cash_floor);
  assert.equal(writes[0][0][1], 0); assert.equal(writes[0][0][7], 10000);
  const displayed = JSON.stringify(tables);
  assert(displayed.includes('0 of 0 sized seat(s) would lose funding (0 SAR).'));
  assert(!displayed.includes('8 of 9') && !displayed.includes('777 SAR'));
  const once = JSON.stringify(p); ctx.dt10RenderPayload_(sheet, p, {});
  assert.equal(JSON.stringify(p), once); assert.equal(calls, 1);
});

console.log('Full-source final board funding: ' + passed + ' passed');
