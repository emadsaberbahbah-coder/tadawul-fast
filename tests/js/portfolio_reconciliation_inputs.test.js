/* Full native source: declared capture/holdings input and real HTTP serialization. */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../../apps_script/16_Decision_Top10.gs'), 'utf8');
const copy = value => JSON.parse(JSON.stringify(value));
let passed = 0;
function check(label, fn) { fn(); passed++; console.log('PASS ' + label); }
function context(raw = null) {
  const script = {};
  const ctx = {Logger: {log() {}}, PropertiesService: {
    getDocumentProperties() { return {getProperty(key) {
      assert.equal(key, 'TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1'); return raw;
    }}; },
    getScriptProperties() { return {getProperty(key) {return script[key] || null;}}; }
  }};
  vm.createContext(ctx); vm.runInContext(source, ctx, {filename: '16_Decision_Top10.gs'});
  return {ctx, script};
}
const header = ['Symbol', 'Name', 'Sector', 'Position Value (SAR)', 'Currency',
  'Position Qty', 'Current Price', 'Data Provider', 'Warnings', 'Last Updated'];
const row = ['SYNTH.US', 'Synthetic holding', 'Energy', 750, 'USD', 10, 20,
  'synthetic-provider', 'quote_asof=2026-10-09T09:00:00Z', '2026-10-09T09:00:01Z'];
function workbook(values = [header, row], options = {}) {
  const ranges = [];
  const sheet = {getLastRow() {return options.lastRow || values.length;},
    getLastColumn() {return options.lastCol || values[0].length;},
    getRange(r, c, rows, cols) {ranges.push([r,c,rows,cols]); return {getValues() {return values.slice(0,rows).map(x=>x.slice(0,cols));}};}
  };
  return {ranges, getSheetByName(name) {assert.equal(name, 'My_Portfolio'); return options.missing ? null : sheet;}};
}
const panel = {'Cash Available (SAR)': 1000, 'Pending Proceeds (SAR)': 99999};
const packet = {schema_version:1, source_ref:'synthetic://declared-capture', captured_at:'2026-10-09T09:00:00Z',
  accounts:[{account_id:'synthetic-account'}], holding_links:[], funding_accounts:['synthetic-account']};

check('same bounded native row carries quantity currency price and quote provenance', () => {
  const {ctx} = context(); const ss = workbook();
  const p = copy(ctx.dt10PortfolioInputs_(ss, panel));
  assert.equal(p.holdings_input_incomplete, false); assert.equal(p.holdings.length, 1);
  assert.deepEqual(p.holdings[0], {symbol:'SYNTH.US',sector:'Energy',value_sar:750,currency:'USD',
    quantity:10,current_price:20,data_provider:'synthetic-provider',
    warnings:row[8],last_updated:row[9]});
  assert.deepEqual(ss.ranges, [[1,1,2,10]]); assert.equal(p.reconciliation_evidence, undefined);
});
check('declared private document property forwards only the supplied packet', () => {
  const {ctx} = context(JSON.stringify(packet));
  const p = copy(ctx.dt10PortfolioInputs_(workbook(), panel));
  assert.deepEqual(p.reconciliation_evidence, packet);
  assert.equal(p.cash_available_sar, 1000); // Paper cash is never promoted to proof.
  assert.equal(p.reconciliation_evidence.accounts[0].positions, undefined);
});
for (const raw of ['', 'malformed-private', '[]', 'null', '{"schema_version":true}',
  '{"schema_version":2}', JSON.stringify({schema_version:1,oversized:'x'.repeat(9000)})]) {
  check('invalid or oversized document evidence is omitted safely ' + passed, () => {
    const {ctx} = context(raw); assert.equal(ctx.dt10ReconciliationEvidence_(), null);
  });
}
check('document property service failure does not fall back to global script properties', () => {
  const {ctx} = context(JSON.stringify(packet));
  ctx.PropertiesService.getDocumentProperties = () => {throw new Error('private failure');};
  assert.equal(ctx.dt10ReconciliationEvidence_(), null);
});
check('unknown or disabled holding source never means confirmed empty custody', () => {
  const {ctx,script} = context();
  assert.equal(ctx.dt10PortfolioInputs_(workbook(undefined,{missing:true}),panel).holdings_input_incomplete, true);
  script.DT10_SEND_HOLDINGS='0';
  const p=ctx.dt10PortfolioInputs_(workbook(),panel);
  assert.equal(p.holdings_input_incomplete,true); assert.equal(p.holdings,undefined);
});
check('duplicate rows and oversized row or column ranges are explicitly incomplete', () => {
  const {ctx} = context();
  for (const ss of [workbook([header,row,row]), workbook([header,row],{lastRow:1000}), workbook([header,row],{lastCol:300})]) {
    const p=ctx.dt10PortfolioInputs_(ss,panel); assert.equal(p.holdings_input_incomplete,true);
    const [,,rows,cols]=ss.ranges[0]; assert(rows<=513 && cols<=250);
  }
});
check('ambiguous native quantity headers and boolean quantity are not certified inputs', () => {
  const {ctx} = context();
  const p=ctx.dt10PortfolioInputs_(workbook([header.concat('Shares'),row.concat(99)]),panel);
  assert.equal(p.holdings_input_incomplete,true);
  const bad=row.slice(); bad[5]=true;
  assert.equal(ctx.dt10PortfolioInputs_(workbook([header,bad]),panel).holdings[0].quantity,undefined);
});
check('minor-unit spelling and absent fields remain exact unknowns', () => {
  const {ctx}=context(); const bad=row.slice(); bad[4]='GBp'; bad[5]=''; bad[9]='';
  const h=ctx.dt10PortfolioInputs_(workbook([header,bad]),panel).holdings[0];
  assert.equal(h.currency,'GBp'); assert.equal(h.quantity,undefined); assert.equal(h.last_updated,undefined);
});
check('native Date conversion retains its actual instant without manufacturing a new timestamp', () => {
  const {ctx}=context(); ctx.testRow=row.slice();
  vm.runInContext("testRow[9] = new Date('2026-10-09T08:00:00Z')",ctx);
  const h=ctx.dt10PortfolioInputs_(workbook([header,ctx.testRow]),panel).holdings[0];
  assert.equal(h.last_updated,'2026-10-09T08:00:00.000Z');
});
check('actual native POST serializes the exact private evidence and all protective fields', () => {
  const {ctx}=context(JSON.stringify(packet)); let sent;
  ctx.dt10BackendUrl_=()=> 'https://synthetic.invalid'; ctx.dt10AppToken_=()=> 'synthetic-token';
  ctx.UrlFetchApp={fetch(url,options) {
    assert.equal(url,'https://synthetic.invalid/sheet-rows/opportunity-candidates');
    sent=JSON.parse(options.payload); return {getResponseCode(){return 200;},getContentText(){return '{"status":"ok"}';}};
  }};
  const body={portfolio:ctx.dt10PortfolioInputs_(workbook(),panel),fx_rates:{USD:3.75}};
  assert.equal(ctx.dt10Post_(body).code,200);
  assert.deepEqual(sent.portfolio.reconciliation_evidence,packet);
  assert.equal(sent.portfolio.holdings_input_incomplete,false);
  assert.equal(sent.portfolio.holdings[0].quantity,10); assert.equal(sent.portfolio.holdings[0].currency,'USD');
  assert.equal(sent.portfolio.holdings[0].warnings,row[8]); assert.equal(sent.portfolio.holdings[0].last_updated,row[9]);
});
console.log(`${passed} native reconciliation input checks passed`);
