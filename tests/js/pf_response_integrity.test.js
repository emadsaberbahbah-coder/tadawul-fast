/* Refused HTTP-200 builds cannot replace a decision or retire its capture. */
'use strict';
const assert = require('node:assert/strict');
const {world, VALID, CAPTURE_ID, copy} = require('./pf_integrity_harness');
let passed = 0;
function check(label, fn) { fn(); passed++; console.log('PASS ' + label); }
function unchanged(state) {
  assert.equal(state.decision.value(11, 1), 'prior-kpi');
  assert.equal(state.decision.value(13, 1), 'prior-board');
  assert(state.operations.every(op => op.sheet === 'Portfolio_Decision' && op.row === 2 && op.col === 2));
  assert.equal(state.sheets._Portfolio_Action_Log, undefined);
  assert.equal(state.properties.PD_IBKR_LAST_PROCESSED, undefined);
}

for (const status of ['unavailable', 'degraded', 'failed', 'error', ' ERROR ']) {
  check('operational ' + status + ' refuses before any board, KPI or log publication', () => {
    const response = Object.assign(copy(VALID), {status, message: 'private response must not appear',
      error: 'private error must not appear'});
    const state = world({response});
    assert.throws(() => state.call('refreshPortfolioDecision'), /^Error: RESPONSE ERROR: backend status /);
    unchanged(state);
    const display = state.decision.value(2, 2) + state.logs.join(' ');
    assert(display.includes('RESPONSE ERROR'));
    assert(!display.includes('private'));
    assert(!display.includes('source readiness VERIFIED'));
    assert.equal(state.requests.length, 1);
  });
}

const malformed = [true, 5, 'truthy response', [], {},
  Object.assign(copy(VALID), {status: 1}), Object.assign(copy(VALID), {status: ''}),
  Object.assign(copy(VALID), {kpis: null}), Object.assign(copy(VALID), {kpis: []}),
  Object.assign(copy(VALID), {actions: {}}), Object.assign(copy(VALID), {actions: [null]}),
  Object.assign(copy(VALID), {sector_summary: null}), Object.assign(copy(VALID), {sector_summary: [[]]}),
  Object.assign(copy(VALID), {alerts: 'wrong type'}), Object.assign(copy(VALID), {alerts: [7]}),
  Object.assign(copy(VALID), {meta: 'wrong type'}),
  Object.assign(copy(VALID), {actions: [{}]}),
  Object.assign(copy(VALID), {actions: [{action: ' ', symbol: 'SYNTH.US'}]}),
  Object.assign(copy(VALID), {actions: [{action: 'HOLD', symbol: ''}]}),
  Object.assign(copy(VALID), {actions: [{action: 'HOLD', symbol: 123}]}),
  Object.assign(copy(VALID), {actions: [{action: {}, symbol: 'SYNTH.US'}]}),
  Object.assign(copy(VALID), {actions: [{action: 'HOLD', symbol: 'SYNTH.US', name: {private: true}}]}),
  Object.assign(copy(VALID), {actions: [{action: 'HOLD', symbol: 'SYNTH.US', price: [6]}]}),
  Object.assign(copy(VALID), {kpis: {portfolio_value_sar: {private: true}}}),
  Object.assign(copy(VALID), {kpis: {action_counts: []}}),
  Object.assign(copy(VALID), {kpis: {action_counts: {HOLD: {private: true}}}}),
  Object.assign(copy(VALID), {sector_summary: [{sector: ['nested']}] }),
  Object.assign(copy(VALID), {alerts: [{required_action: {private: true}}]})];
for (let i = 0; i < malformed.length; i++) {
  check('malformed renderer envelope ' + i + ' preserves the prior decision', () => {
    const state = world({response: malformed[i]});
    assert.throws(() => state.call('refreshPortfolioDecision'), /RESPONSE ERROR:/);
    unchanged(state);
  });
}

for (const status of ['ok', 'empty', 'disabled', 'research']) {
  check('successful or policy-controlled ' + status + ' still renders its supported envelope', () => {
    const response = Object.assign(copy(VALID), {status});
    if (status !== 'ok') response.actions = [];
    const state = world({response});
    assert.equal(state.call('refreshPortfolioDecision'), status);
    assert.equal(state.operations.filter(op => op.method === 'clear').length, 1);
    assert.notEqual(state.decision.value(13, 1), 'prior-board');
    assert(state.decision.value(2, 2).includes('status: ' + status));
    if (status === 'ok') {
      assert(state.sheets._Portfolio_Action_Log);
      assert.equal(state.sheets._Portfolio_Action_Log.getLastRow(), 2);
    }
  });
}

check('valid scalar cells and unfamiliar policy action labels retain their exact values', () => {
  const response = Object.assign(copy(VALID), {status: 'policy_result',
    kpis: {portfolio_value_sar: 0, cash_sar: null, action_counts: {BLOCK: 1}},
    actions: [{action: 'POLICY_BLOCK', symbol: 'SYNTH.US', name: '', price: 0}],
    sector_summary: [{sector: 'Synthetic', over_cap: false}],
    alerts: [{type: 'synthetic', count: 0, required_action: 'Review'}]});
  const state = world({response});
  assert.equal(state.call('refreshPortfolioDecision'), 'policy_result');
  assert.equal(state.decision.value(11, 1), 0);
  assert.equal(state.operations.filter(op => op.method === 'clear').length, 1);
});

for (const quantity of [0.4, 1.6, 10]) {
  check('action quantity and signed exit delta display exact fractional/integer values: ' + quantity, () => {
    const response = Object.assign(copy(VALID), {actions: [{action: 'EXIT', symbol: 'SYNTH.US',
      quantity, suggested_delta_shares: -quantity, proceeds_sar: 100}]});
    const state = world({response});
    assert.equal(state.call('refreshPortfolioDecision'), 'ok');
    assert.equal(state.decision.value(15, 6), quantity);
    assert.equal(state.decision.value(15, 16), -quantity);
    for (const col of [6, 16]) {
      assert(state.operations.some(op => op.method === 'setNumberFormat' &&
        op.row === 15 && op.col === col && op.format === '#,##0.########'));
    }
  });
  check('outside-scope quantity keeps its exact value and fractional format: ' + quantity, () => {
    const response = Object.assign(copy(VALID), {status: 'empty', actions: []});
    const state = world({response, properties: {PD_IBKR_EVIDENCE: 'enforce',
      PD_EXECUTION_EXCLUDE: 'SYNTH.US'}});
    state.sheets.My_Portfolio.put(2, 4, quantity);
    assert.equal(state.call('refreshPortfolioDecision'), 'empty');
    assert.deepEqual(state.requests[0].body.rows, []);
    const section = state.operations.find(op => typeof op.value === 'string' &&
      op.value.startsWith('OUTSIDE EXECUTION SCOPE'));
    assert(section);
    assert.equal(state.decision.value(section.row + 2, 4), quantity);
    assert(state.operations.some(op => op.method === 'setNumberFormat' &&
      op.row === section.row + 2 && op.col === 4 && op.format === '#,##0.########'));
  });
}

check('watcher leaves a failed fresh capture retryable, then acknowledges the recovered build', () => {
  const state = world({watcher: true, response: Object.assign(copy(VALID), {status: 'unavailable'})});
  assert.equal(state.call('pdIbkrCaptureWatch'), 'error');
  unchanged(state);
  assert.equal(state.locks, 0);
  state.response = copy(VALID);
  assert.equal(state.call('pdIbkrCaptureWatch'), 'refreshed');
  assert.equal(state.properties.PD_IBKR_LAST_PROCESSED, CAPTURE_ID);
  assert.equal(state.requests.length, 2);
  assert.equal(state.locks, 0);
  assert.equal(state.call('pdIbkrCaptureWatch'), 'already processed');
  assert.equal(state.requests.length, 2);
});

for (const kind of ['network', 'http', 'null-json']) {
  check('existing ' + kind + ' transport result remains unchanged and retryable', () => {
    const state = world({watcher: true});
    let expected;
    if (kind === 'network') { state.networkError = true; expected = 'NETWORK ERROR'; }
    else if (kind === 'http') { state.code = 503; expected = 'HTTP 503'; }
    else { state.response = null; expected = 'HTTP 200'; }
    assert.equal(state.call('refreshPortfolioDecision'), expected);
    unchanged(state);
    assert.equal(state.call('pdIbkrCaptureWatch'), 'refresh failed: ' + expected);
    unchanged(state);
    assert.equal(state.locks, 0);
  });
}
console.log('PF response integrity: ' + passed + ' checks passed');
