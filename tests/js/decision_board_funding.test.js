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
  const snapshot = {contract_version: 1, builder_version: '1.24.0', snapshot_id: 'synthetic-signed-id',
    rows: seats.map(t => ({symbol: t.symbol})), xchecks: {}};
  return {version: '1.24.0', status: 'ok', selected: seats,
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
console.log('Full-source final board funding: ' + passed + ' passed');
