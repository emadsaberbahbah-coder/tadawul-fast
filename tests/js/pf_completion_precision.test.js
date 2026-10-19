/* Full native renderer/completion tests; only external Apps Script services fake. */
'use strict';
const assert = require('node:assert/strict');
const {world, VALID, CAPTURE_ID, copy} = require('./pf_integrity_harness');
let passed = 0;
function check(label, fn) { fn(); passed++; console.log('PASS ' + label); }
const RUN = '8787878787';
const plain = value => JSON.parse(JSON.stringify(value));

function precisionWorld(meta) {
  const response = copy(VALID);
  response.actions = [{action: 'EXIT', symbol: 'SYNTH.US', quantity: 0.4,
    avg_cost: 0.00000123, price: 0.00000547, price_sar: 0.0000205125,
    market_value_sar: 100.35, cost_sar: 99.75, pnl_sar: 0.6, pnl_pct: 0.61,
    suggested_delta_shares: -0.4, proceeds_sar: 100.35, stop_sar: 0.00001641,
    tp1_sar: 0.00002461, tp2_sar: 0.00002872}];
  response.meta = meta;
  const state = world({response});
  const original = state.decision.getRange.bind(state.decision);
  state.decision.getRange = function (row, col, nr = 1, nc = 1) {
    const range = original(row, col, nr, nc);
    return new Proxy(range, {get(target, method) {
      if (method === 'setNumberFormats') return formats => {
        assert.equal(formats.length, nr);
        assert(formats.every(values => values.length === nc));
        state.operations.push({sheet: 'Portfolio_Decision', method, row, col, nr, nc, formats: copy(formats)});
        return range;
      };
      return target[method];
    }});
  };
  state.render = excluded => state.ctx.pdRenderPayload_(state.decision, state.response,
    state.ctx.PD_FALLBACK_TOKENS, excluded || []);
  state.formatAt = (row, col) => {
    let format;
    for (const op of state.operations) {
      if (op.sheet !== 'Portfolio_Decision' || row < op.row || row >= op.row + op.nr ||
          col < op.col || col >= op.col + op.nc) continue;
      if (op.method === 'setNumberFormat') format = op.format;
      if (op.method === 'setNumberFormats') format = op.formats[row - op.row][col - op.col];
    }
    return format;
  };
  return state;
}

for (const [value, digits] of [[9.876543, 6], [1, 6], [0.1, 7], [0.01, 8],
  [1e-5, 11], [0.00000547, 12], [1e-12, 18], [-0.00000547, 12]]) {
  check('price-only adaptive decimal rule preserves ' + value, () => {
    const state = world();
    assert.equal(state.ctx.pdPriceFormat_(value), '#,##0.' + '0'.repeat(digits));
  });
}
for (const value of [1e-20, 1e-100, 1e-300, Number.MIN_VALUE]) {
  check('extreme positive is visible through seven-significant-digit scientific display ' + value, () => {
    const state = world();
    assert.equal(state.ctx.pdPriceFormat_(value), '0.000000E+00');
    assert(Number(value.toExponential(6)) > 0);
  });
}
for (const value of [null, undefined, '', '0.00000547', true, NaN, Infinity, -Infinity, 0, 10, 100]) {
  check('legacy format retains unknown/non-price/high value ' + String(value), () => {
    assert.equal(world().ctx.pdPriceFormat_(value), '#,##0.00');
  });
}
for (const meta of [undefined, {}, {price_precision: 'off'}, {price_precision: true},
  {price_precision: 'true'}, {price_precision: 'ON'}, {price_precision: ' on '}, {price_precision: 1}]) {
  check('only exact backend metadata enables extra formatting ' + JSON.stringify(meta), () => {
    const state = precisionWorld(meta);
    state.properties.TFB_PRICE_PRECISION = 'on'; // an independent native flag has no authority
    state.render();
    assert(!state.operations.some(op => op.method === 'setNumberFormats'));
    const baseline = precisionWorld(undefined); baseline.render();
    assert.deepEqual(state.operations, baseline.operations);
    assert.deepEqual(state.requests, baseline.requests);
  });
}

check('armed display changes price formats only, preserving exact values/money/quantity', () => {
  const off = precisionWorld({}), on = precisionWorld({price_precision: 'on'});
  off.render(); on.render();
  assert.deepEqual(on.operations.filter(op => op.method !== 'setNumberFormats'), off.operations);
  assert.equal(on.operations.filter(op => op.method === 'setNumberFormats').length, 6);
  for (const col of [7, 8, 9, 18, 19, 20]) {
    assert.notEqual(on.formatAt(15, col), '#,##0.00');
    assert.equal(on.decision.value(15, col), off.decision.value(15, col));
  }
  for (const col of [10, 11, 12, 15]) assert.equal(on.formatAt(15, col), '#,##0 "SAR"');
  for (const col of [6, 16]) assert.equal(on.formatAt(15, col), '#,##0.########');
  assert.equal(on.decision.value(15, 6), 0.4);
  assert.equal(on.decision.value(15, 16), -0.4);
  assert.equal(on.decision.value(15, 15), -100.35);
});
check('already rendered precision formats reset when backend flag turns off', () => {
  const state = precisionWorld({price_precision: 'on'}); state.render();
  assert.notEqual(state.formatAt(15, 8), '#,##0.00');
  delete state.response.meta.price_precision;
  state.render();
  for (const col of [7, 8, 9, 18, 19, 20]) assert.equal(state.formatAt(15, col), '#,##0.00');
});
check('scope unit cost and price use adaptive formats while scope quantity remains exact', () => {
  const state = precisionWorld({price_precision: 'on'});
  state.render([{Symbol: 'OUTSIDE.US', Name: 'Synthetic', Currency: 'USD',
    Quantity: 0.4, 'Avg Cost': 0.00000123, 'Current Price': 0.00000547}]);
  const section = state.operations.find(op => op.value && typeof op.value === 'string' &&
    op.value.startsWith('OUTSIDE EXECUTION SCOPE'));
  const row = section.row + 2;
  assert.equal(state.decision.value(row, 4), 0.4);
  assert.equal(state.formatAt(row, 4), '#,##0.########');
  for (const col of [5, 6]) assert.equal(state.formatAt(row, col), '#,##0.' + '0'.repeat(12));
});

function completedWorld(options = {}) {
  const state = world(options);
  const Sheet = state.sheets.My_Portfolio.constructor;
  state.sheets.My_Portfolio.put(1, 7, 'Last Updated (UTC)');
  state.sheets.My_Portfolio.put(2, 7, '2026-10-15T05:44:30Z');
  state.sheets._Status = new Sheet('_Status', [
    ['Page', 'Last Updated', 'Status', 'Message', 'Endpoint', 'HTTP Code', 'Rows', 'Columns', 'Duration ms', 'Warnings'],
    ['My_Portfolio', '2026-10-15 08:44:45+03:00', 'SUCCESS',
      'acquired=1/1 acquisition=COMPLETE data=COMPLETE fetchfail_effective=enforce run=' + RUN,
      'synthetic', 200, 1, 7, 1, 0],
  ]);
  let reads = 0;
  const original = state.sheets._Status.getRange.bind(state.sheets._Status);
  state.sheets._Status.getRange = (...args) => { reads++; return original(...args); };
  state.statusReads = () => reads;
  state.completion = options => plain(state.ctx.refreshPortfolioDecision(
    Object.assign({completionContract: true}, options)));
  return state;
}
check('ordinary refresh retains its status string and zero source reads while receipt mode is off', () => {
  const state = completedWorld();
  assert.equal(state.call('refreshPortfolioDecision'), 'ok');
  assert.equal(state.statusReads(), 0);
});
check('typed completion uses the actual consumed cohort without arming B2 receipts', () => {
  const state = completedWorld();
  assert.deepEqual(state.completion({source_run: '999999'}), {
    contract: 'tfb_native_refresh_v1', page: 'Portfolio_Decision', state: 'completed',
    source_run: RUN, source_stamp_ms: Date.parse('2026-10-15T05:44:45Z'),
    reason: '', output_state: 'ok',
  });
  assert(state.statusReads() >= 2);
  assert(!state.decision.value(2, 2).includes('source readiness'));
  assert.equal(state.requests.length, 1);
});
for (const [label, mutation, reason] of [
  ['source changed during request', s => s.sheets._Status.put(2, 4,
    'acquired=1/1 acquisition=COMPLETE data=COMPLETE fetchfail_effective=enforce run=989898'), 'source_unverified'],
  ['source partial', s => s.sheets._Status.put(2, 3, 'FAILED'), 'source_unverified'],
  ['source run duplicated', s => s.sheets._Status.put(2, 4,
    'acquired=1/1 acquisition=COMPLETE data=COMPLETE fetchfail_effective=enforce run=' + RUN + ' run=' + RUN), 'source_unverified'],
  ['consumed timestamp changed', s => s.sheets.My_Portfolio.put(2, 7, '2026-10-15T05:44:31Z'), 'holdings_unverified'],
]) {
  check('render completion leaves source_run unverified when ' + label, () => {
    const state = completedWorld();
    const post = state.ctx.pdPost_;
    state.ctx.pdPost_ = body => { const response = post(body); mutation(state); return response; };
    const result = state.completion();
    assert.equal(result.state, 'completed');
    assert.equal(result.source_run, '');
    assert.equal(result.source_stamp_ms, undefined);
    assert.equal(result.reason, reason);
  });
}
check('missing consumed timestamp cannot establish typed completion', () => {
  const state = completedWorld(); state.sheets.My_Portfolio.put(2, 7, '');
  assert.equal(state.completion().reason, 'holdings_unverified');
});
for (const status of ['empty', 'disabled', 'research']) {
  check('valid policy render can prove source consumption without certifying funding: ' + status, () => {
    const response = copy(VALID); response.status = status; response.actions = [];
    const state = completedWorld({response});
    const result = state.completion();
    assert.equal(result.state, 'completed'); assert.equal(result.source_run, RUN);
    assert.equal(result.output_state, status);
  });
}
for (const reason of ['network', 'http']) {
  check('typed operational ' + reason + ' throws so the chain retains its failure attempt', () => {
    const state = completedWorld();
    if (reason === 'network') state.networkError = true; else state.code = 503;
    assert.throws(() => state.completion(), reason === 'network'
      ? /^Error: PD_REFRESH_FAILED: network request unavailable$/
      : /^Error: PD_REFRESH_FAILED: HTTP response unavailable$/);
    assert.equal(state.decision.value(13, 1), 'prior-board');
  });
}
check('legacy watcher keeps ordinary status behavior and capture acknowledgment', () => {
  const state = completedWorld({watcher: true});
  assert.equal(state.call('pdIbkrCaptureWatch'), 'refreshed');
  assert.equal(state.properties.PD_IBKR_LAST_PROCESSED, CAPTURE_ID);
  assert.equal(state.statusReads(), 0);
});
console.log('PF completion/precision: ' + passed + ' checks passed');
