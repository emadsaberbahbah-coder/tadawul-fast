/* 10_My_Portfolio.gs v1.5.16 (2026-10-17) -- Decision Pages v2 plan, package B3
 * (cockpit half): gated, HIDDEN 'Spark 90d' column on the ACTIONS table carrying
 * =IFERROR(SPARKLINE(GOOGLEFINANCE(ticker,"close",TODAY()-91,TODAY()),...),"")
 * (Script Property PD_SPARKLINE = on; default off = headers / rows / golden
 * byte-identical, proven by pf_ibkr_evidence.test.js).
 * Run: node tests/js/pf_sparkline_col.test.js */
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

test('V1 version lockstep, functions and vars exist, PD_ACTION_HEADERS still 26 (action-log contract)', () => {
  const ctx = load();
  assert.equal(ctx.PD_VERSION, '1.5.17');
  assert.match(source.split('\n')[3], /^ \* Version: 1\.5\.17$/);
  assert(source.includes(' * v1.5.16 MAIN CHANGES  (90-DAY PRICE SPARKLINE COLUMN -- Decision Pages v2'));
  for (const f of ['pdSparklineColOn_', 'pdSparklineTicker_', 'pdSparklineFormula_']) assert.equal(typeof ctx[f], 'function', f);
  assert.equal(ctx.PD_SPARK_COL_PROP, 'PD_SPARKLINE'); assert.equal(ctx.PD_SPARK_HEADER, 'Spark 90d'); assert.equal(ctx.PD_SPARK_DAYS, 91);
  assert.equal(ctx.PD_ACTION_HEADERS.length, 26); assert.equal(ctx.PD_ACTLOG_HEADERS.length, 32);
});
test('B1 gate: on only for "on" (trimmed, any case); property failure reads off', () => {
  assert.equal(load().pdSparklineColOn_(), false);
  assert.equal(load({ PD_SPARKLINE: 'on' }).pdSparklineColOn_(), true);
  assert.equal(load({ PD_SPARKLINE: ' ON ' }).pdSparklineColOn_(), true);
  for (const v of ['off', '1', 'true', '', 'enforce']) assert.equal(load({ PD_SPARKLINE: v }).pdSparklineColOn_(), false, v);
  const ctx = load(); ctx.PropertiesService.getScriptProperties = () => { throw new Error('boom'); };
  assert.equal(ctx.pdSparklineColOn_(), false);
});
test('B2 ticker mapper: US bare, LON/HKG/TADAWUL prefixes, bare pass-through, unknown suffix / unsafe / blank -> ""', () => {
  const ctx = load();
  const m = ctx.pdSparklineTicker_;
  assert.equal(m('SHG.US'), 'SHG'); assert.equal(m(' shg.us '), 'SHG');
  assert.equal(m('HSBA.L'), 'LON:HSBA'); assert.equal(m('0005.HK'), 'HKG:0005'); assert.equal(m('2222.SR'), 'TADAWUL:2222');
  assert.equal(m('7203.T'), 'TYO:7203'); assert.equal(m('AEFES.IS'), 'IST:AEFES'); assert.equal(m('T82U.SI'), 'SGX:T82U'); assert.equal(m('ACA.PA'), 'EPA:ACA');
  assert.equal(m('KRP'), 'KRP'); assert.equal(m('BRK-B.US'), 'BRK-B');
  assert.equal(m('BRK.B'), ''); assert.equal(m('.US'), ''); assert.equal(m(''), ''); assert.equal(m(null), ''); assert.equal(m('SYM BOL.US'), ''); assert.equal(m('A"B.US'), '');
});
test('B3 formula: blank ticker -> ""; otherwise the IFERROR/SPARKLINE/GOOGLEFINANCE close 91-day formula with the line style', () => {
  const ctx = load();
  assert.equal(ctx.pdSparklineFormula_(''), ''); assert.equal(ctx.pdSparklineFormula_(null), ''); assert.equal(ctx.pdSparklineFormula_('bad"tick'), '');
  const f = ctx.pdSparklineFormula_('LON:HSBA');
  assert.equal(f, '=IFERROR(SPARKLINE(GOOGLEFINANCE("LON:HSBA","close",TODAY()-91,TODAY()),{"charttype","line";"linewidth",1;"color","#2a78d6"}),"")');
  assert.equal(ctx.pdSparklineFormula_(ctx.pdSparklineTicker_('CWBC.US')).indexOf('GOOGLEFINANCE("CWBC","close"'), 19);
});
test('B4 render hooks: header appended after the C4 column, cell pushed per row, column insertion guard, hidden column, selftest line; C4 wrap column corrected; ES5-safe added lines', () => {
  const headers = source.indexOf("  var spCol = pdSparklineColOn_();   /* v1.5.16 [B3] */"), write = source.indexOf('  var aT = pdWriteTable_(sheet, row, aHeaders, aRows,');
  assert(headers > 0 && headers < write);
  assert(source.includes('  if (spCol) aHeaders = aHeaders.concat([PD_SPARK_HEADER]);'));
  assert(source.includes('          pdSparklineFormula_(pdSparklineTicker_(actions[a] && actions[a].symbol)));'));
  assert(source.includes('  if (spCol && sheet.getMaxColumns() < aHeaders.length) {   /* v1.5.16 [B3] */'));
  assert(source.includes('    try { sheet.hideColumns(aHeaders.length); } catch (eSp) {}'));
  assert(source.includes('    sheet.getRange(aT.firstDataRow, aHeaders.length - (spCol ? 1 : 0), aT.count, 1)'));
  assert(source.includes("  report.push('sparkline col (' + PD_SPARK_COL_PROP + '): ' +   /* v1.5.16 [B3] */"));
  const bad = /\b(let|const)\b|=>|`|\bclass\b|\.\.\.|\.includes\(/;
  for (const line of source.split('\n').filter(l => /v1\.5\.16|pdSparkline|PD_SPARK_/.test(l))) { assert(!bad.test(line), line); assert(!/[‘’“”\r]/.test(line), 'quotes/CR: ' + line); }
});
test('B5 both columns on: Switch (adv) is column 27 and Spark 90d column 28; row cells align with the headers', () => {
  const ctx = load({ PD_SPARKLINE: 'on', PD_SWITCH_ADVISORY_COL: 'on' });
  let aHeaders = ctx.pdActionHeaders_(ctx.pdSwitchAdvisoryColOn_());
  if (ctx.pdSparklineColOn_()) aHeaders = aHeaders.concat([ctx.PD_SPARK_HEADER]);
  aHeaders = plain(aHeaders);
  assert.equal(aHeaders.length, 28); assert.equal(aHeaders[26], 'Switch (adv)'); assert.equal(aHeaders[27], 'Spark 90d');
  const a = { action: 'HOLD', symbol: 'KRP.US', switch_advisory: 'switch: → ATAT Δ+24.0% (adv)' };
  const row = plain(ctx.pdActionToRow_(a)); row.push(ctx.pdSwitchAdvisoryCell_(a)); row.push(ctx.pdSparklineFormula_(ctx.pdSparklineTicker_(a.symbol)));
  assert.equal(row.length, 28); assert.equal(row[27].indexOf('=IFERROR(SPARKLINE(GOOGLEFINANCE("KRP","close",TODAY()-91,TODAY())'), 0);
});

console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
