/* 10_My_Portfolio v1.5.14 + 16_Decision_Top10 v1.13.10 correctness fixes.
 * Runs the REAL Apps Script sources in a vm with service stubs; all
 * holdings, lots and rates are synthetic.
 *   L*  _myPortfolioLedgerMap_ excludes Sold/Closed/Inactive lots exactly
 *       like the Python ledger readers; blank status stays active.
 *   F*  holdings FX: subunits resolve on the RAW token (GBp/GBX/ZAc/ILa ->
 *       parent / 100); a missing rate sends value_sar null and marks the
 *       holdings input incomplete instead of a native amount labelled SAR.
 *   R*  refreshDecisionTop10: a throw ends in a terminal 'status: FAILED'
 *       banner with failed_at_utc, _Status ERROR, rethrown; the P-195 marker
 *       is cleared only when this run stored it (R1 vs R5).
 *
 *   node tests/js/gas_ledger_fx_failed_state.test.js
 *   GAS_FIX_DUMP=<file>  write the produced status lines as JSON (the Python
 *                        wrapper feeds them to the real freshness parser)
 */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const ROOT = path.join(__dirname, '../..');
const PF = fs.readFileSync(path.join(ROOT, 'apps_script/10_My_Portfolio.gs'), 'utf8');
const DT10 = fs.readFileSync(path.join(ROOT, 'apps_script/16_Decision_Top10.gs'), 'utf8');
const copy = value => JSON.parse(JSON.stringify(value));
let passed = 0;
const dump = {};
function check(label, fn) { fn(); passed++; console.log('PASS ' + label); }

/* ------------------------------------------------------------ ledger */
const LEDGER_HEADER = ['Symbol', 'Status', 'Buy Date', 'Buy Price', 'Shares', 'Buy Fees'];
function ledgerMap(rows) {
  const grid = [['Portfolio Ledger'], [''], [''], LEDGER_HEADER].concat(rows);
  const width = LEDGER_HEADER.length;
  const sheet = {
    getLastRow() { return grid.length; },
    getLastColumn() { return width; },
    getRange(r, c, nr, nc) {
      nr = nr || 1; nc = nc || 1;
      const block = grid.slice(r - 1, r - 1 + nr).map(row => {
        const out = [];
        for (let j = 0; j < nc; j++) out.push(row[c - 1 + j] === undefined ? '' : row[c - 1 + j]);
        return out;
      });
      return {getValue() { return block[0][0]; }, getValues() { return block; }};
    }
  };
  const ctx = {SHEET_NAMES: {MY_PORTFOLIO: 'My_Portfolio'}, Logger: {log() {}},
    SpreadsheetApp: {getActive() { return {
    getSheetByName(name) { return name === '_Portfolio_CostBasis' ? sheet : null; }}; }}};
  vm.createContext(ctx);
  vm.runInContext(PF, ctx, {filename: '10_My_Portfolio.gs'});
  assert.equal(ctx.PD_VERSION, '1.5.18');
  return copy(ctx._myPortfolioLedgerMap_());
}

check('L1 Active 10 @ 100 (fee 5) + Sold 10 @ 80 -> qty 10, cost 100.5 (was 20 / 90.5)', () => {
  const m = ledgerMap([['AAA.US', 'Active', '', 100, 10, 5], ['AAA.US', 'Sold', '', 80, 10, 0]]);
  assert.equal(m['AAA.US'].qty, 10);
  assert.equal(m['AAA.US'].buyPrice, 100.5);
  assert.equal(m['AAA.US'].lots, 1);
});
check('L2 closed/sold/inactive excluded trimmed and case-insensitively; blank stays active', () => {
  const m = ledgerMap([
    ['BBB.US', ' CLOSED ', '', 50, 4, 0], ['BBB.US', 'sold', '', 60, 4, 0],
    ['BBB.US', 'Inactive', '', 70, 4, 0], ['BBB.US', '', '', 40, 2, 0],
    ['CCC.US', 'Sold', '', 10, 1, 0], ['DDD.US', 'Pending', '', 10, 3, 0]]);
  assert.equal(m['BBB.US'].qty, 2);
  assert.equal(m['BBB.US'].buyPrice, 40);
  assert.equal(m['CCC.US'], undefined, 'a fully sold symbol is not held');
  assert.equal(m['DDD.US'].qty, 3, 'statuses outside the excluded set are unchanged');
});

/* ------------------------------------------------------------ holdings FX */
function dt10Context(extra) {
  const ctx = Object.assign({Logger: {log() {}},
    PropertiesService: {
      getDocumentProperties() { return {getProperty() { return null; }}; },
      getScriptProperties() { return {getProperty() { return null; }, deleteProperty() {}, setProperty() {}}; }
    }}, extra || {});
  vm.createContext(ctx);
  vm.runInContext(DT10, ctx, {filename: '16_Decision_Top10.gs'});
  assert.equal(ctx.DT10_VERSION, '1.13.14');
  return ctx;
}
const FX_ROWS = [['USD', '', '', 3.75], ['GBP', '', '', 5], ['ZAR', '', '', 0.2], ['ILS', '', '', 1]];
const HOLD_HEADER = ['Symbol', 'Name', 'Sector', 'Position Value', 'Currency'];
function holdings(rows, fxRows) {
  const ctx = dt10Context();
  const values = [HOLD_HEADER].concat(rows.map(r => [r[0], 'Synthetic ' + r[0]].concat(r.slice(1))));
  const sheet = {getLastRow() { return values.length; }, getLastColumn() { return HOLD_HEADER.length; },
    getRange(r, c, nr, nc) { return {getValues() { return values.slice(0, nr).map(x => x.slice(0, nc)); }}; }};
  const ss = {getSheetByName(name) { return name === 'My_Portfolio' ? sheet : null; },
    getRangeByName(name) { return name === 'TFB_FX_LOOKUP' ? {getValues() { return fxRows || FX_ROWS; }} : null; }};
  const p = ctx.dt10PortfolioInputs_(ss, {'Cash Available (SAR)': 1000});
  return {p: copy(p), raw: p, ctx};
}

check('F1 subunit tokens resolve on the RAW token before case folding', () => {
  const {ctx} = holdings([]);
  const fx = {GBP: 5, ZAR: 0.2, ILS: 1, USD: 3.75};
  assert.equal(ctx.dt10HoldingFxRate_('GBp', fx), 0.05);
  assert.equal(ctx.dt10HoldingFxRate_('GBX', fx), 0.05);
  assert.equal(ctx.dt10HoldingFxRate_('gbx', fx), 0.05);
  assert.equal(ctx.dt10HoldingFxRate_('GBP', fx), 5, 'pounds are not pence');
  assert.equal(ctx.dt10HoldingFxRate_('ZAc', fx), 0.002);
  assert.equal(ctx.dt10HoldingFxRate_('ZAC', fx), 0.002);
  assert.equal(ctx.dt10HoldingFxRate_('ILa', fx), 0.01);
  assert.equal(ctx.dt10HoldingFxRate_('ILA', fx), 0.01);
  assert.equal(ctx.dt10HoldingFxRate_('usd', fx), 3.75);
  assert.equal(ctx.dt10HoldingFxRate_(' SAR ', fx), null);
  assert.equal(ctx.dt10HoldingFxRate_('', fx), null);
  assert(Number.isNaN(ctx.dt10HoldingFxRate_('SGD', fx)));
  assert.equal(ctx.dt10HoldingFxRate_('GBp', {GBp: 0.0499, GBP: 5}), 0.0499, 'exact raw subunit key wins');
  assert(Number.isNaN(ctx.dt10HoldingFxRate_('GBp', {USD: 3.75})), 'no parent rate');
  /* USX gate is backend-only and DEFAULT OFF: only its own row converts it. */
  assert(Number.isNaN(ctx.dt10HoldingFxRate_('USX', fx)));
  assert.equal(ctx.dt10HoldingFxRate_('USX', {USX: 0.0375, USD: 3.75}), 0.0375);
});
check('F2 GBp holding converts at GBP/100, not the pound rate (was 100x)', () => {
  const {p} = holdings([['VOD.L', 'Telecom', 1000, 'GBp'], ['BP.L', 'Energy', 10, 'GBP'],
    ['NPN.JO', 'Tech', 5000, 'ZAc'], ['TEVA.TA', 'Health', 2000, 'ILa'],
    ['PFS', 'Financials', 100, 'USD'], ['2222.SR', 'Energy', 500, 'SAR']]);
  const v = Object.fromEntries(p.holdings.map(h => [h.symbol, h.value_sar]));
  assert.equal(v['VOD.L'], 50);
  assert.equal(v['BP.L'], 50);
  assert.equal(v['NPN.JO'], 10);
  assert.equal(v['TEVA.TA'], 20);
  assert.equal(v['PFS'], 375);
  assert.equal(v['2222.SR'], 500);
  assert.equal(p.holdings[0].currency, 'GBp', 'minor-unit spelling preserved for the backend');
  assert.equal(p.holdings_input_incomplete, false);
});
check('F3 missing FX rate: value_sar null (never the native amount) and input incomplete', () => {
  const {p, raw} = holdings([['T82U.SI', 'REIT', 5852, 'SGD'], ['PFS', 'Financials', 100, 'USD']]);
  const sgd = p.holdings.find(h => h.symbol === 'T82U.SI');
  assert.equal(sgd.value_sar, null);
  assert.equal(raw.holdings[0].value_sar, null, 'null, not NaN or 0');
  assert.equal(p.holdings.find(h => h.symbol === 'PFS').value_sar, 375);
  assert.equal(p.holdings_input_incomplete, true);
  const serial = JSON.parse(JSON.stringify(raw));
  assert.equal(serial.holdings[0].value_sar, null);
  assert.equal(serial.holdings.some(h => Object.keys(h).some(k => k.charAt(0) === '_')), false);
  const gbp = holdings([['VOD.L', 'Telecom', 1000, 'GBp']], [['USD', '', '', 3.75]]).p;
  assert.equal(gbp.holdings[0].value_sar, null, 'GBp without a GBP rate is not left in pence');
  assert.equal(gbp.holdings_input_incomplete, true);
});
check('F4 the real dt10SelfTest holdings FX line reports ok (GBp and unknown cases)', () => {
  const ctx = dt10Context({Utilities: {formatDate() { return '2026-10-20'; }},
    Session: {getScriptTimeZone() { return 'Asia/Riyadh'; }},
    SpreadsheetApp: {getActiveSpreadsheet() { return {getRangeByName() { return null; },
      getSheetByName() { return null; }, toast() {}}; }, getUi() { return {alert() {}, ButtonSet: {OK: 1}}; }}});
  const report = String(ctx.dt10SelfTest());
  const line = report.split('\n').find(l => l.indexOf('holdings value_sar via FX') === 0);
  assert(line && /GBp 1000 -> 50/.test(line) && /: ok$/.test(line), line);
});

/* ------------------------------------------------------------ FAILED state */
function refreshHarness(opts) {
  const writes = [], pageStatuses = [], cleared = [];
  let flushed = 0;
  const cells = {};
  let ctx = null;
  const sheet = {getRange(r, c) {
    const key = r + ',' + c;
    return {getValue() { return opts.cellValue ? opts.cellValue(r, c, ctx) : ''; },
      setValue(v) { cells[key] = v; writes.push({r, c, v}); return this; }};
  }};
  const ss = {getSheetByName(name) { return opts.noSheet ? null : (name === 'Top_10_Investments' ? sheet : null); }};
  ctx = dt10Context({
    SpreadsheetApp: {getActiveSpreadsheet() { return ss; }, flush() { flushed++; }},
    Session: {getScriptTimeZone() { return 'Asia/Riyadh'; }},
    Utilities: {formatDate(d) {
      return new Date(d.getTime() + 3 * 3600 * 1000).toISOString().replace('T', ' ').slice(0, 19);
    }}
  });
  ctx.dt10Tokens_ = () => ({});
  const checked = [];
  ctx.dt10RunMarkerCheck_ = (entry) => { checked.push(entry); return null; };
  ctx.dt10RunMarkerClear_ = () => { cleared.push(true); };
  ctx.dt10WritePageStatus_ = (...args) => { pageStatuses.push(args); };
  if (opts.layoutThrows) ctx.buildDecisionTop10Layout = () => { throw new Error('synthetic layout failure'); };
  else ctx.buildDecisionTop10Layout = () => sheet;
  if (opts.afterMarkerThrows) {
    // the panel and the input-truth setup succeed; the first step after the
    // P-195 marker check throws
    ctx.dt10ReadPanel_ = () => ({});
    ctx.dt10InputTruthMode_ = () => 'off';
    ctx.dt10CandTabMode_ = () => 'off';
    ctx.dt10VintageColsMode_ = () => 'off';
    ctx.dt10InputTruthKpiState_ = () => null;
    ctx.dt10SyncInflight_ = () => { throw new Error('synthetic post-marker failure'); };
  } else {
    ctx.dt10ReadPanel_ = () => { throw new Error(opts.message || 'synthetic panel read failure'); };
  }
  let threw = null;
  try { ctx.refreshDecisionTop10(); } catch (e) { threw = e; }
  return {ctx, writes, pageStatuses, cleared, checked, flushed, threw, cells};
}
const LAYOUT_OK = (r, c, ctx) => (c === 1 && r === ctx.DT10_ROW_KPI_HEAD ? 'KPIs' : 'x');

check('R1 a throw after running... ends in a terminal FAILED banner and is rethrown', () => {
  const before = Date.now();
  const h = refreshHarness({cellValue: LAYOUT_OK});
  assert(h.threw && /synthetic panel read failure/.test(String(h.threw)), 'error rethrown');
  const row = h.ctx.DT10_ROW_STATUS;
  const lines = h.writes.filter(w => w.r === row && w.c === 2).map(w => w.v);
  assert.equal(lines.length, 2);
  assert(/\| status: running…$/.test(lines[0]), lines[0]);
  const failed = lines[1];
  assert(/^Last run \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \| status: FAILED \| failed_at_utc=\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z \| /.test(failed), failed);
  assert(failed.includes('synthetic panel read failure'));
  assert(failed.includes('BOARD NOT REFRESHED'));
  const utc = Date.parse(failed.match(/failed_at_utc=(\S+)/)[1]);
  assert(utc >= Math.floor(before / 1000) * 1000 && utc <= Date.now());
  assert.equal(h.pageStatuses.length, 1);
  assert.equal(h.pageStatuses[0][0], 'ERROR');
  assert(h.pageStatuses[0][1].startsWith('Decision cockpit: FAILED at '));
  // the throw came before the P-195 check: a previous run's marker must stay
  // for the next check to report ABORTED_PREV (review fix on PR #810)
  assert.equal(h.checked.length, 0);
  assert.equal(h.cleared.length, 0, 'a throw before the marker check leaves the previous marker');
  assert(h.flushed >= 2);
  dump.running = lines[0];
  dump.failed = failed;
});
check('R2 a throw before the status cell is bound still writes FAILED to the existing sheet', () => {
  const h = refreshHarness({cellValue: () => '', layoutThrows: true});
  assert(h.threw && /synthetic layout failure/.test(String(h.threw)));
  const lines = h.writes.filter(w => w.r === h.ctx.DT10_ROW_STATUS && w.c === 2).map(w => w.v);
  assert.equal(lines.length, 1);
  assert(/\| status: FAILED \| failed_at_utc=/.test(lines[0]), lines[0]);
  assert(lines[0].includes('synthetic layout failure'));
  dump.failed_before_bind = lines[0];
});
check('R3 no sheet at all: nothing written, _Status ERROR, error still rethrown', () => {
  const h = refreshHarness({noSheet: true, layoutThrows: true});
  assert(h.threw);
  assert.equal(h.writes.length, 0);
  assert.equal(h.pageStatuses[0][0], 'ERROR');
});
check('R5 a throw after the P-195 check clears the marker this run stored', () => {
  const h = refreshHarness({cellValue: LAYOUT_OK, afterMarkerThrows: true});
  assert(h.threw && /synthetic post-marker failure/.test(String(h.threw)), 'error rethrown');
  assert.deepEqual(h.checked, ['refreshDecisionTop10']);
  assert.equal(h.cleared.length, 1, 'a reported failure clears its own marker');
  const failed = h.writes.filter(w => w.r === h.ctx.DT10_ROW_STATUS && w.c === 2).pop().v;
  assert(/\| status: FAILED \| failed_at_utc=/.test(failed), failed);
});
check('R4 long multi-line error is collapsed and bounded in the banner', () => {
  const h = refreshHarness({cellValue: LAYOUT_OK, message: 'line one\n\tline two ' + 'x'.repeat(500)});
  const failed = h.writes.filter(w => w.r === h.ctx.DT10_ROW_STATUS && w.c === 2).pop().v;
  assert(!/\n/.test(failed));
  assert(failed.includes('line one line two '));
  assert(failed.indexOf('x'.repeat(201)) < 0);
});

if (process.env.GAS_FIX_DUMP) fs.writeFileSync(process.env.GAS_FIX_DUMP, JSON.stringify(dump, null, 1));
console.log('RESULT ' + passed + '/' + passed + ' PASS');
