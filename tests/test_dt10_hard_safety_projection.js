/* Real GAS projection preserves every current-row hard alias before ingress. */
'use strict';
const fs = require('fs');
const vm = require('vm');
const assert = require('assert');
const path = require('path');
const source = fs.readFileSync(path.join(__dirname, '../apps_script/16_Decision_Top10.gs'), 'utf8');
const context = {
  PropertiesService: { getScriptProperties: () => ({getProperty: () => null}) },
  Logger: {log: () => {}},
};
vm.createContext(context);
vm.runInContext(source, context);
for (const headers of [
  ['Symbol', 'Final Action', 'final_action', 'Investability', 'Investability Status', 'Source Snapshot ID'],
  ['Symbol', 'final_action', 'Final Action', 'Investability Status', 'Investability', 'Source Snapshot ID'],
]) {
  const map = context.dt10MapHeaderCols_(headers);
  const projected = context.dt10PoolRowFromSheetRow_(
    ['SAFE.US', 'INVEST', 'DO_NOT_INVEST', 'INVESTABLE', 'BLOCKED', 'snapshot-123'], map, 'Global_Markets');
  assert.strictEqual(projected['Final Action'], 'DO_NOT_INVEST');
  assert.strictEqual(projected['Investability Status'], 'BLOCKED');
  assert.strictEqual(projected['Source Snapshot ID'], 'snapshot-123');
}
const map = context.dt10MapHeaderCols_(['Symbol', 'Investability Status', 'Currency', 'Current Price']);
const watch = context.dt10PoolRowFromSheetRow_(['SAFE.US', 'WATCHLIST', 'USD', 100], map, 'Global_Markets');
assert.strictEqual(watch['Investability Status'], 'WATCHLIST');
assert.strictEqual(watch['Currency'], 'USD');
assert.strictEqual(watch['Current Price'], 100);
assert.strictEqual(context.DT10_PANEL.find(field => field.label === 'T10: Max Per Sector').def, 2);
console.log('GAS hard safety projection: 10 assertions passed');
