/* 10_My_Portfolio.gs v1.5.15 (2026-10-17) -- C4 cockpit half: gated 'Switch
 * (adv)' column on the ACTIONS table (Script Property PD_SWITCH_ADVISORY_COL =
 * on; default off = headers / rows / golden byte-identical, proven by
 * pf_ibkr_evidence.test.js). Run: node tests/js/pf_switch_advisory_col.test.js */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const ROOT = path.join(__dirname, '../..');
const source = fs.readFileSync(path.join(ROOT, 'apps_script/10_My_Portfolio.gs'), 'utf8');
const plain = v => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));
let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + (e && e.stack || e)); }
}
function load(props) {
  const store = Object.assign({}, props || {});
  const propsService = { getProperty(k) { return Object.prototype.hasOwnProperty.call(store, k) ? store[k] : null; },
    setProperty(k, v) { store[k] = String(v); return propsService; } };
  const ss = { getSheetByName() { return null; } };
  const ctx = {
    SHEET_NAMES: { MY_PORTFOLIO: 'My_Portfolio' }, ensureSheetExists_() { return null; }, readCurrentSheetHeaders_() { return []; },
    console, Logger: { log() {} }, Date,
    Utilities: { formatDate() { return ''; } },
    PropertiesService: { getScriptProperties() { return propsService; }, getDocumentProperties() { return propsService; } },
    SpreadsheetApp: { getActiveSpreadsheet() { return ss; }, getActive() { return ss; }, flush() {} },
    UrlFetchApp: {}, DriveApp: {}, Session: { getScriptTimeZone() { return 'Asia/Riyadh'; } },
    ScriptApp: { getProjectTriggers() { return []; } }, LockService: {}
  };
  vm.createContext(ctx);
  vm.runInContext(source, ctx, { filename: '10_My_Portfolio.gs' });
  return ctx;
}

test('V1 version lockstep, functions and var exist, PD_ACTION_HEADERS still 26 (action-log contract)', () => {
  const ctx = load();
  assert.equal(ctx.PD_VERSION, '1.5.16');
  assert.match(source.split('\n')[3], /^ \* Version: 1\.5\.16$/);
  assert(source.includes(' * v1.5.15 MAIN CHANGES  (SWITCH ADVISORY COLUMN -- Codex brief 2026-10-17 C4,'));
  for (const f of ['pdSwitchAdvisoryColOn_', 'pdActionHeaders_', 'pdSwitchAdvisoryCell_']) assert.equal(typeof ctx[f], 'function', f);
  assert.equal(ctx.PD_SWITCH_COL_PROP, 'PD_SWITCH_ADVISORY_COL');
  assert.equal(ctx.PD_ACTION_HEADERS.length, 26); assert.equal(ctx.PD_ACTLOG_HEADERS.length, 32);
});
test('C1 gate: on only for "on"; headers 26 off / 27 on with Switch (adv) last; off returns the very same array', () => {
  const ctx = load();
  assert.equal(ctx.pdSwitchAdvisoryColOn_(), false);
  assert.equal(load({ PD_SWITCH_ADVISORY_COL: 'on' }).pdSwitchAdvisoryColOn_(), true);
  for (const v of ['off', '1', 'true', '']) assert.equal(load({ PD_SWITCH_ADVISORY_COL: v }).pdSwitchAdvisoryColOn_(), false, v);
  assert.equal(ctx.pdActionHeaders_(false), ctx.PD_ACTION_HEADERS); assert.equal(ctx.pdActionHeaders_(), ctx.PD_ACTION_HEADERS);
  const on = plain(ctx.pdActionHeaders_(true));
  assert.equal(on.length, 27); assert.equal(on[26], 'Switch (adv)'); assert.equal(on[25], 'Advisor Note');
  assert.deepEqual(on.slice(0, 26), plain(ctx.PD_ACTION_HEADERS));
});
test('C2 cell: the backend text or blank; a row with the column appended keeps the 26 base cells', () => {
  const ctx = load();
  assert.equal(ctx.pdSwitchAdvisoryCell_({ switch_advisory: 'switch: → ATAT Δ+24.0% (adv)' }), 'switch: → ATAT Δ+24.0% (adv)');
  assert.equal(ctx.pdSwitchAdvisoryCell_({}), ''); assert.equal(ctx.pdSwitchAdvisoryCell_(null), ''); assert.equal(ctx.pdSwitchAdvisoryCell_({ switch_advisory: null }), '');
  const a = { action: 'HOLD', symbol: 'KRP.US', switch_advisory: 'switch: → ATAT Δ+24.0% (adv)' };
  const row = plain(ctx.pdActionToRow_(a)); row.push(ctx.pdSwitchAdvisoryCell_(a));
  assert.equal(row.length, 27); assert.equal(row[26], 'switch: → ATAT Δ+24.0% (adv)'); assert.equal(row[0], 'HOLD');
});
test('C3 hooks: gated headers/rows in the ACTIONS render, column insertion, wrap on the new last column, selftest line; ES5-safe added lines', () => {
  const rows = source.indexOf('  var swCol = pdSwitchAdvisoryColOn_();   /* v1.5.15 [C4] */'), write = source.indexOf('  var aT = pdWriteTable_(sheet, row, aHeaders, aRows,');
  assert(rows > 0 && rows < write);
  assert(source.includes("    if (swCol) aRows[aRows.length - 1].push(pdSwitchAdvisoryCell_(actions[a]));"));
  assert(source.includes('  if (swCol && sheet.getMaxColumns() < aHeaders.length) {'));
  assert(source.includes('  if (swCol && aT.firstDataRow > 0) {   /* v1.5.15 [C4]: wrap the advisory column */'));
  assert(source.includes("  report.push('switch advisory col (' + PD_SWITCH_COL_PROP + '): ' +"));
  assert(!source.includes('pdWriteTable_(sheet, row, PD_ACTION_HEADERS, aRows,'), 'the render reads the gated header list');
  const bad = /\b(let|const)\b|=>|`|\bclass\b|\.\.\.|\.includes\(/;
  for (const line of source.split('\n').filter(l => /v1\.5\.15|pdSwitchAdvisory|pdActionHeaders_|PD_SWITCH_COL_PROP/.test(l))) assert(!bad.test(line), line);
});

console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
