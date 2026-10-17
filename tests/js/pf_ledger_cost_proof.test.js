/* Native ledger aggregation must not extrapolate partial lot cost to all shares. */
'use strict';
const assert = require('node:assert/strict');
const {world, HOLD_ROW} = require('./pf_integrity_harness');
let passed = 0;
function check(label, fn) { fn(); passed++; console.log('PASS ' + label); }
const HEADERS = ['Symbol', 'Status', 'Buy Price', 'Shares', 'Buy Fees', 'Cost Basis'];
const lot = (price = 5, fees = 1, witness = 51, quantity = 10, status = 'Active') =>
  ['SYNTH.US', status, price, quantity, fees, witness];
function ledger(rows, headers = HEADERS) {
  return world({ledger: {headers, rows}});
}
function assertUnknown(state, expectedQuantity = 20) {
  const aggregate = state.ctx._myPortfolioLedgerMap_()['SYNTH.US'];
  assert.equal(aggregate.qty, expectedQuantity);
  assert.equal(aggregate.buyPrice, null);
  assert.equal(aggregate.costIncomplete, true);
  const row = state.ctx.pdCollectHoldings_().rows[0];
  assert.equal(row.Quantity, expectedQuantity);
  assert(!Object.prototype.hasOwnProperty.call(row, 'Buy Price'));
  assert(!Object.prototype.hasOwnProperty.call(row, 'Avg Cost'));
  assert.equal(row['Current Price'], HOLD_ROW[5]);
  return row;
}

check('all priced lots retain the exact unrounded fee-inclusive weighted average', () => {
  const state = ledger([lot(), lot(7, 2, 72)]);
  const aggregate = state.ctx._myPortfolioLedgerMap_()['SYNTH.US'];
  assert.equal(aggregate.qty, 20);
  assert.equal(aggregate.buyPrice, 123 / 20);
  const row = state.ctx.pdCollectHoldings_().rows[0];
  assert.equal(row['Buy Price'], 123 / 20);
  assert.equal(row['Avg Cost'], 123 / 20);
});

for (const price of [null, '', 'unknown', 0, -1, NaN, Infinity, true, false, '5%']) {
  check('a positive-quantity lot with unknown/invalid price withholds the full aggregate: ' + String(price), () => {
    assertUnknown(ledger([lot(), lot(price, 0, '')]));
  });
}
for (const fees of [null, '', ' ', 'unknown', -1, NaN, Infinity, true, false, '5%']) {
  check('an unproven fee never becomes zero: ' + String(fees), () => {
    assertUnknown(ledger([lot(), lot(5, fees, '')]));
  });
}
for (const fees of [null, '', ' ']) {
  check('blank fee with the exact native zero-delta witness remains supported: ' + JSON.stringify(fees), () => {
    const state = ledger([lot(5, fees, 50)]);
    const aggregate = state.ctx._myPortfolioLedgerMap_()['SYNTH.US'];
    assert.equal(aggregate.qty, 10);
    assert.equal(aggregate.buyPrice, 5);
    assert.equal(state.sheets._Portfolio_CostBasis.value(5, 5), fees);
  });
}
for (const witness of [49.999999, 50.000001, '', 0, -50, 'unknown']) {
  check('approximate or invalid native cost is not a zero-fee witness: ' + String(witness), () => {
    assertUnknown(ledger([lot(5, '', witness)]), 10);
  });
}
for (const header of ['Cost Basis (SAR)', 'Total Cost', 'Cost']) {
  check('a non-native cost total cannot witness a blank fee: ' + header, () => {
    const headers = HEADERS.slice(); headers[5] = header;
    assertUnknown(ledger([lot(5, '', 50)], headers), 10);
  });
}
check('a malformed fee cannot use a matching zero-delta witness', () => {
  assertUnknown(ledger([lot(5, 'unknown', 50)]), 10);
});
check('duplicate native totals and quantity aliases cannot certify a blank fee', () => {
  assertUnknown(ledger([lot(5, '', 50).concat(50)], HEADERS.concat('Cost Basis')), 10);
  const headers = HEADERS.slice(); headers[3] = 'Quantity';
  assertUnknown(ledger([lot(5, '', 50)], headers), 10);
  assertUnknown(ledger([lot(5, '', 50).concat(5)], HEADERS.concat('Buy Price')), 10);
  assertUnknown(ledger([lot(5, '', 50).concat(10)], HEADERS.concat('Shares')), 10);
  const quantityFirst = ['Symbol', 'Status', 'Buy Price', 'Quantity', 'Shares', 'Buy Fees', 'Cost Basis'];
  assertUnknown(ledger([['SYNTH.US', 'Active', 5, 10, 99, '', 50]], quantityFirst), 10);
  assertUnknown(ledger([lot(5, '', 50).concat(99)], HEADERS.concat('Quantity')), 10);
});
check('explicit zero fee does not require a witness', () => {
  const state = ledger([lot(5, 0, '')]);
  assert.equal(state.ctx._myPortfolioLedgerMap_()['SYNTH.US'].buyPrice, 5);
});
check('inactive lots do not taint the active positive-quantity cost', () => {
  const state = ledger([lot(), lot('', '', '', 10, 'Inactive'), lot('', '', '', 0, 'Inactive')]);
  const aggregate = state.ctx._myPortfolioLedgerMap_()['SYNTH.US'];
  assert.equal(aggregate.qty, 10);
  assert.equal(aggregate.buyPrice, 5.1);
});
check('layouts without a fee column retain the existing unit-cost contract', () => {
  const state = ledger([['SYNTH.US', 'Active', 5, 10]], HEADERS.slice(0, 4));
  assert.equal(state.ctx._myPortfolioLedgerMap_()['SYNTH.US'].buyPrice, 5);
});
check('qualified or duplicate fee headers cannot silently omit a fee component', () => {
  const headers = HEADERS.slice(); headers[4] = 'Buy Fees SAR';
  assertUnknown(ledger([lot()], headers), 10);
  headers[4] = 'buy fees';
  assertUnknown(ledger([lot()], headers), 10);
  assertUnknown(ledger([lot().concat(1)], HEADERS.concat('Buy Fees')), 10);
});
check('no recognized v2 ledger retains the legacy cost-map fallback', () => {
  const state = world({legacyCost: {'SYNTH.US': {qty: 10, buyPrice: 5.5}}});
  assert.equal(state.ctx.pdCollectHoldings_().rows[0]['Avg Cost'], 5.5);
});
check('the real POST omits both stale cost aliases while retaining known quantity', () => {
  const state = ledger([lot(), lot('', '', '')]);
  assert.equal(state.call('refreshPortfolioDecision'), 'ok');
  const row = state.requests[0].body.rows[0];
  assert.equal(row.Quantity, 20);
  assert(!Object.prototype.hasOwnProperty.call(row, 'Buy Price'));
  assert(!Object.prototype.hasOwnProperty.call(row, 'Avg Cost'));
});
for (const quantity of [null, '', 'unknown', 0, -1, NaN, Infinity, true, false, '5%', '0x10']) {
  check('unknown quantity cannot publish a known-subset custody total: ' + String(quantity), () => {
    const state = ledger([lot(), lot(5, 0, '', quantity)]);
    const aggregate = state.ctx._myPortfolioLedgerMap_()['SYNTH.US'];
    assert.equal(aggregate.qty, null);
    assert.equal(aggregate.qtyIncomplete, true);
    assert.equal(aggregate.buyPrice, null);
    const row = state.ctx.pdCollectHoldings_().rows[0];
    assert(!Object.prototype.hasOwnProperty.call(row, 'Quantity'));
    assert(!Object.prototype.hasOwnProperty.call(row, 'Buy Price'));
    assert(!Object.prototype.hasOwnProperty.call(row, 'Avg Cost'));
    if (quantity === 0) {
      assert.equal(state.call('refreshPortfolioDecision'), 'ok');
      assert(!Object.prototype.hasOwnProperty.call(state.requests[0].body.rows[0], 'Quantity'));
      assert(!Object.prototype.hasOwnProperty.call(state.requests[0].body.rows[0], 'Buy Price'));
      assert(!Object.prototype.hasOwnProperty.call(state.requests[0].body.rows[0], 'Avg Cost'));
    }
  });
}
check('all-zero active lots cannot resurrect stale display quantity or cost in the real POST', () => {
  const state = ledger([lot(5, 0, 0, 0)]);
  const aggregate = state.ctx._myPortfolioLedgerMap_()['SYNTH.US'];
  assert.equal(aggregate.qty, null);
  assert.equal(aggregate.qtyIncomplete, true);
  assert.equal(state.call('refreshPortfolioDecision'), 'ok');
  const row = state.requests[0].body.rows[0];
  assert(!Object.prototype.hasOwnProperty.call(row, 'Quantity'));
  assert(!Object.prototype.hasOwnProperty.call(row, 'Buy Price'));
  assert(!Object.prototype.hasOwnProperty.call(row, 'Avg Cost'));
});
console.log('PF ledger cost proof: ' + passed + ' checks passed');
