/* Execute the full native renderer/request source. Only external services and
 * unrelated refresh orchestration are fixtures; readiness and finalization
 * are production functions, including the real source read and HTTP body seam. */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const crypto = require('node:crypto');
const source = fs.readFileSync(path.join(__dirname, '../../apps_script/16_Decision_Top10.gs'), 'utf8');
const plain = value => JSON.parse(JSON.stringify(value));
const NOW = Date.parse('2026-10-12T09:30:00Z');
const STAMP = '2026-10-12 12:00:00+03:00';
const FLOORS = {Market_Leaders:255, Global_Markets:6512, Commodities_FX:453, Mutual_Funds:2474};
const SCOPE_PAGES = ['Market_Leaders','Mutual_Funds'];
const FULL_PAGES = ['Market_Leaders','Global_Markets','Commodities_FX','Mutual_Funds'];
const SYMBOLS = Object.fromEntries(FULL_PAGES.map(page => [page, Array.from({length:FLOORS[page]},(_,i) => 'SYNTH_' + page.toUpperCase() + '_' + String(i).padStart(4,'0'))]));
const symbolDigest = values => crypto.createHash('sha256').update(values.map(x=>x.trim().toUpperCase()).sort().join('\n')+'\n').digest('hex');
const HEADER = ['Page','Last Updated','Status','Message','Endpoint','HTTP Code','Rows','Columns','Duration ms','Warnings'];
let passed = 0;
function test(name, fn) {fn(); passed++; console.log('PASS ' + name);}
function goodGrid() {
  return [HEADER, ...Object.entries(FLOORS).map(([page,count]) => [page,STAMP,'SUCCESS',
    `acquired=${count}/${count} acquisition=COMPLETE fetchfail_effective=enforce | data=COMPLETE run=90001`,
    'synthetic-source',200,count,115,1,0])];
}
function load(props = {}, options = {}) {
  const digestCalls=[];
  class FixedDate extends Date {
    constructor(...args) {super(...(args.length ? args : [NOW]));}
    static now() {return NOW;}
  }
  const ctx = {Date:FixedDate, Logger:{log() {}}, PropertiesService:{getScriptProperties() {
    return {getProperty(k) {return props[k] || null;},setProperty(k,v) {props[k]=v;},deleteProperty(k) {delete props[k];}};
  }}, Session:{getScriptTimeZone() {return 'Asia/Riyadh';}}, Utilities:{DigestAlgorithm:{SHA_256:'SHA_256'}, Charset:{UTF_8:'UTF_8'},
    computeDigest(algorithm,value,charset) {assert.equal(algorithm,'SHA_256');assert.equal(charset,'UTF_8');digestCalls.push(value);return [...crypto.createHash('sha256').update(value).digest()].map(n=>n>127?n-256:n);}, formatDate(date,_zone,pattern) {
    const local = new Date(date.getTime()+3*3600000).toISOString();
    return pattern === 'yyyy-MM-dd' ? local.slice(0,10) : local.replace('T',' ').slice(0,19);
  }}};
  vm.createContext(ctx); vm.runInContext(source,ctx,{filename:'16_Decision_Top10.gs'});
  ctx._digestCalls=digestCalls; ctx._embeddedScope=plain(ctx.DT10_SOURCE_APPROVED_SCOPE);
  if(!options.nativeHashes) for(const page of SCOPE_PAGES) ctx.DT10_SOURCE_APPROVED_SCOPE.pages[page].symbol_set_sha256=symbolDigest(SYMBOLS[page]);
  return ctx;
}
function core(ctx,grid=goodGrid(),sourceContext,policy,bookOptions) {
  policy=policy||ctx.dt10SourcePolicy_();
  const receipts=ctx.dt10SourceScopeRead_(statusWorkbook(grid,bookOptions),policy,sourceContext);
  return ctx.dt10SourceReadinessCore_(grid,NOW,sourceContext,policy,receipts);
}
function capture(ctx,grid = goodGrid(),pool = FLOORS) {
  const source = core(ctx,grid);
  const before={captured_at_ms:NOW,initial_ready:source.ready,generation:plain(source.generation),cohort:source.cohort};
  const collected=ctx.dt10CollectPoolRows_(statusWorkbook(grid),50000,before);
  Object.assign(before,{pool_counts:{...pool},pool_scope_receipts:plain(collected.approved_scope),pool_included_counts:plain(collected.perPage),pool_sent_total:collected.rows.length,pool_reconciliation:plain(collected.pool_reconciliation)});
  return before;
}
function statusWorkbook(grid = goodGrid(), options = {}) {
  const calls = [], scopeCalls = [];
  const status = {getLastRow() {return options.rows || grid.length;}, getLastColumn() {return 13;},
    getRange(...args) {
      calls.push(args);
      return {getValues() {
        if(options.fail) throw new Error('Synthetic service failure');
        if(typeof args[0] === 'string') return [['TFB Decision Feed',options.verdict ||
          'EXECUTABLE | run=synthetic | 2026-10-12 12:00:00+03:00 | ML:OK GM:OK CFX:OK MF:OK']];
        return grid;
      }};
    }};
  const sourceGrids = options.sourceGrids || Object.fromEntries(FULL_PAGES.map(page=>[page,[['Name','Symbol'],...SYMBOLS[page].map(s=>['fixture',s])]]));
  return {calls,scopeCalls,sourceGrids, getSheetByName(name) {
    if(name==='_Status') return !options.missing ? status : null;
    const rows=sourceGrids[name]; if(!rows) return null;
    return {getLastRow(){return rows.length;},getLastColumn(){return rows[0].length;},getDataRange(){return {getValues(){return plain(rows);}};},getRange(r,c,n,m){
      scopeCalls.push([name,r,c,n,m]);
      return {getValues(){
        if(options.scopeFail) throw new Error('Synthetic membership read failure');
        const values=rows.slice(r-1,r-1+n).map(row=>row.slice(c-1,c-1+m));
        if(options.afterScopeRead) options.afterScopeRead(name,r,c,n,m,rows);
        return values;
      }};
    }};
  }};
}
test('complete current receipts and unchanged pool generation are ready',()=> {
  const ctx=load(),grid=goodGrid();const result=core(ctx,grid,capture(ctx,grid));
  assert.equal(result.ready,true);assert.equal(result.reason,'');
});
test('declared clocks agree and date-only/impossible clocks stay unverified',()=> {
  const ctx=load(),expected=Date.parse('2026-10-12T09:00:00Z');
  for(const stamp of [STAMP,'2026-10-12 09:00:00Z','2026-10-12 12:00:00',new Date(expected)])
    assert.equal(ctx.dt10SourceStampMs_(stamp),expected);
  for(const stamp of ['2026-10-12','2026-02-30 12:00:00+03:00','2026-10-12 25:00:00Z','junk'])
    assert.equal(ctx.dt10SourceStampMs_(stamp),null);
});
for(const [name,change,expected] of [
  ['partial page',r=>r[2]='PARTIAL','status=PARTIAL'],
  ['stale page',r=>r[1]='2026-10-08 12:00:00+03:00','source stale'],
  ['unknown acquisition',r=>r[3]='acquired=unknown/1025 acquisition=UNKNOWN | data=PARTIAL','receipt unknown'],
  ['observe policy factual partial',r=>r[3]='acquired=900/1025 acquisition=PARTIAL fetchfail_effective=observe | data=PARTIAL','acquisition not COMPLETE'],
  ['policy error',r=>r[3]=r[3].replace('fetchfail_effective=enforce','fetchfail_effective=error'),'policy error'],
  ['duplicated receipt',r=>r[3]+=' acquired=1025/1025','receipt unknown'],
  ['date-only stamp',r=>r[1]='2026-10-12','timestamp unverified'],
  ['future publication',r=>r[1]='2026-10-12 12:31:00+03:00','future'],
  ['physical row deficit',r=>r[6]=254,'rows 254 below floor 255'],
  ['small acquisition denominator',r=>r[3]='acquired=254/254 acquisition=COMPLETE | data=COMPLETE','denominator 254 below floor 255'],
  ['acquisition greater than requested',r=>r[3]='acquired=1026/1025 acquisition=COMPLETE | data=COMPLETE','receipt unknown'],
  ['missing data evidence',r=>r[3]='acquired=1025/1025 acquisition=COMPLETE','data not COMPLETE']
]) {
  test(name+' cannot certify a successful status cell',()=> {
    const ctx=load(),grid=goodGrid();change(grid[1]);const result=core(ctx,grid);
    assert.equal(result.ready,false);assert(result.reason.includes(expected),result.reason);
  });
}
test('duplicate policy tokens cannot certify readiness',()=> {
  const ctx=load(),grid=goodGrid();grid[1][3]+=' fetchfail_effective=error';
  // Duplicate effective policy is ambiguous even with valid acquisition.
  assert.equal(core(ctx,grid).ready,false);
});
test('missing or duplicated required source rows are explicit failures',()=> {
  const ctx=load();for(const grid of [goodGrid().slice(0,-1), [...goodGrid(),goodGrid()[1]]])
    assert.equal(core(ctx,grid).ready,false);
});
test('source acquisition exactly 95 percent passes and lower percentage fails',()=> {
  const ctx=load(),grid=goodGrid();grid[2][3]='acquired=6270/6600 acquisition=COMPLETE fetchfail_effective=enforce | data=COMPLETE run=90001';
  assert.equal(core(ctx,grid).ready,true);
  grid[2][3]='acquired=6269/6600 acquisition=COMPLETE fetchfail_effective=enforce | data=COMPLETE run=90001';
  assert.equal(core(ctx,grid).ready,false);
});
test('current scoped counts and Global/commodities floors match the approved contract',()=>assert.deepEqual(plain(load().DT10_SOURCE_MIN_ROWS),FLOORS));
test('dated scoped approval supersedes old properties; unscoped properties may only tighten floors',()=> {
  const ctx=load({TFB_EXPECTED_MIN_ROWS_MARKET_LEADERS:'1025',TFB_EXPECTED_MIN_ROWS_MUTUAL_FUNDS:'4496',TFB_EXPECTED_MIN_ROWS_GLOBAL_MARKETS:'6500',TFB_EXPECTED_MIN_ROWS_COMMODITIES_FX:'454'});
  const policy=ctx.dt10SourcePolicy_();assert.equal(policy.floors.Market_Leaders,255);assert.equal(policy.floors.Mutual_Funds,2474);
  assert.equal(policy.floors.Global_Markets,6512);assert.equal(policy.floors.Commodities_FX,454);
  assert.deepEqual(plain(policy.superseded_properties),{Market_Leaders:1025,Mutual_Funds:4496});
  assert.equal(core(ctx,goodGrid(),null,policy).ready,false);
});
test('embedded dated scope counts/hashes exactly match the approved manifest without an embedded raw roster',()=> {
  const ctx=load({}, {nativeHashes:true});
  const manifest=JSON.parse(fs.readFileSync(path.join(__dirname,'../../config/approved_universe_scope_v1.json'),'utf8'));
  assert.equal(ctx.DT10_VERSION,'1.13.10');assert.equal(ctx.DT10_SOURCE_APPROVED_SCOPE.policy_revision,manifest.policy_revision);
  assert.equal(ctx.DT10_SOURCE_APPROVED_SCOPE.approved_on,manifest.approved_on);
  for(const page of SCOPE_PAGES) {
    const declared=ctx.DT10_SOURCE_APPROVED_SCOPE.pages[page],approved=manifest.pages[page];
    for(const field of ['approved_count','symbol_set_sha256','supersedes_min_rows']) assert.equal(declared[field],approved[field]);
    assert.deepEqual(Object.keys(declared).sort(),['approved_count','supersedes_min_rows','symbol_set_sha256']);
  }
});
test('actual full physical scope reads consume one digest per page and produce observed dated receipts',()=> {
  const ctx=load({TFB_EXPECTED_MIN_ROWS_MARKET_LEADERS:'1025',TFB_EXPECTED_MIN_ROWS_MUTUAL_FUNDS:'4496'}),ss=statusWorkbook();
  const result=ctx.dt10SourceReadiness_(ss);
  assert.equal(result.ready,true);assert.equal(ctx._digestCalls.length,2);
  assert.deepEqual(ss.scopeCalls.map(r=>r.slice(0,3)),[['Market_Leaders',1,1],['Market_Leaders',2,2],['Mutual_Funds',1,1],['Mutual_Funds',2,2]]);
  assert.deepEqual(ss.scopeCalls.filter(r=>r[2]===2).map(r=>r.slice(3)),[[255,1],[2474,1]]);
  assert.equal(result.scope_policy_revision,'2026-10-12.current-workbook.v1');
  assert.deepEqual(plain(result.superseded_scope_properties),{Market_Leaders:1025,Mutual_Funds:4496});
  for(const page of SCOPE_PAGES) {
    const receipt=result.approved_scope[page];
    assert.equal(receipt.ready,true);assert.equal(receipt.actual_count,FLOORS[page]);
    assert.equal(receipt.observed_symbol_set_sha256,symbolDigest(SYMBOLS[page]));
    assert.equal(receipt.policy_revision,'2026-10-12.current-workbook.v1');
  }
  assert(!JSON.stringify(result.approved_scope).includes(SYMBOLS.Market_Leaders[0]),'receipt contains no raw symbols');
});
test('scope normalization only trims/uppercases; ordering does not change exact membership',()=> {
  const ctx=load(),ss=statusWorkbook();
  for(const page of SCOPE_PAGES) ss.sourceGrids[page]=[['Symbol'],...SYMBOLS[page].slice().reverse().map(s=>['  '+s.toLowerCase()+'  '])];
  assert.equal(ctx.dt10SourceReadiness_(ss).ready,true);
  const spec={approved_count:2,supersedes_min_rows:1025,symbol_set_sha256:symbolDigest(['ABC.US','0001.SR'])};
  const approval=ctx.DT10_SOURCE_APPROVED_SCOPE;
  assert.equal(ctx.dt10SourceScopeCore_(['abc.us','0001.sr'],spec,approval).ready,true);
  assert.equal(ctx.dt10SourceScopeCore_(['ABC','0001.SR'],spec,approval).ready,false,'bare/provider alias cannot substitute');
});
for(const [name,mutate,reason] of [
  ['same-count substitution',rows=>rows[1][1]='SUBSTITUTE.US','membership mismatch'],
  ['missing member',rows=>rows.pop(),'physical count'],
  ['extra member',rows=>rows.push(['fixture','EXTRA.US']),'physical count'],
  ['duplicate normalized member',rows=>rows[2][1]='  '+rows[1][1].toLowerCase()+'  ','duplicate Symbol'],
  ['blank Symbol with other data',rows=>rows[2][1]='','invalid Symbol'],
  ['non-string Symbol',rows=>rows[2][1]=1234,'invalid Symbol'],
  ['internal whitespace',rows=>rows[2][1]='BAD SYMBOL','invalid Symbol']
]) test(name+' fails despite current successful count/acquisition status',()=> {
  const ctx=load(),ss=statusWorkbook();mutate(ss.sourceGrids.Market_Leaders);
  const result=ctx.dt10SourceReadiness_(ss);assert.equal(result.ready,false);assert(result.reason.includes(reason),result.reason);
  assert.equal(ctx.dt10BoardVerdict_(ss).state,'NOT_ACTIONABLE');
});
test('missing/ambiguous Symbol headers, missing physical tabs and read failures are unverified',()=> {
  for(const mode of ['missing-tab','missing-header','duplicate-header','read-fault','torn-read']) {
    const ctx=load(),options={},ss=statusWorkbook(undefined,options);
    if(mode==='missing-tab') delete ss.sourceGrids.Mutual_Funds;
    if(mode==='missing-header') ss.sourceGrids.Mutual_Funds[0]=['Name','Ticker'];
    if(mode==='duplicate-header') ss.sourceGrids.Mutual_Funds[0]=['Symbol','Symbol'];
    if(mode==='read-fault') options.scopeFail=true;
    if(mode==='torn-read') options.afterScopeRead=(page,r,_c,_n,_m,rows)=>{if(page==='Mutual_Funds'&&r===2) rows.pop();};
    assert.equal(ctx.dt10SourceReadiness_(ss).ready,false,mode);
  }
});
test('status receipts alone cannot supply a scope receipt or re-authorize a changed physical roster',()=> {
  const ctx=load(),ss=statusWorkbook(),policy=ctx.dt10SourcePolicy_();
  assert.equal(ctx.dt10SourceReadinessCore_(goodGrid(),NOW,null,policy).ready,false);
  const before=ctx.dt10SourceCapture_(ss);assert.equal(before.initial_ready,true);
  ss.sourceGrids.Mutual_Funds[1][1]='REPLACEMENT.US';
  const result=ctx.dt10SourceReadiness_(ss,before);
  assert.equal(result.ready,false);assert(result.reason.includes('membership mismatch'));
  assert(result.reason.includes('changed after decision source capture'));
});
test('the actual full-grid collector attests complete physical membership before cap or duplicate projection',()=> {
  const ctx=load(),ss=statusWorkbook();
  const collected=ctx.dt10CollectPoolRows_(ss,1);
  assert.equal(collected.rows.length,1);assert.equal(collected.truncated,true);
  assert.equal(ctx._digestCalls.length,4);
  for(const page of SCOPE_PAGES) {
    assert.equal(collected.approved_scope[page].ready,true);
    assert.equal(collected.approved_scope[page].actual_count,FLOORS[page]);
    assert.equal(collected.approved_scope[page].observed_symbol_set_sha256,symbolDigest(SYMBOLS[page]));
  }
});
test('a transient collected substitution cannot be certified by later restoration of physical cells/status',()=> {
  const ctx=load(),ss=statusWorkbook(),before=ctx.dt10SourceCapture_(ss);
  const old=ss.sourceGrids.Market_Leaders[1][1];ss.sourceGrids.Market_Leaders[1][1]='TEMPORARY.SUBSTITUTION';
  const collected=ctx.dt10CollectPoolRows_(ss,50000,before);assert.equal(collected.approved_scope.Market_Leaders.ready,false);
  ss.sourceGrids.Market_Leaders[1][1]=old;
  before.pool_counts=collected.available;before.pool_scope_receipts=collected.approved_scope;
  before.pool_included_counts=collected.perPage;before.pool_sent_total=collected.rows.length;before.pool_reconciliation=collected.pool_reconciliation;
  const result=ctx.dt10SourceReadiness_(ss,before);
  assert.equal(result.approved_scope.Market_Leaders.ready,true,'current restored membership itself is approved');
  assert.equal(result.ready,false);assert(result.reason.includes('collected pool reconciliation unverified'));
  assert.equal(ctx.dt10BoardVerdict_(ss,before).state,'NOT_ACTIONABLE');
  delete before.pool_scope_receipts;
  assert.equal(ctx.dt10SourceReadiness_(ss,before).ready,false,'no consumption witness can be invented from current status');
});
function overlappedPool(cap=50000, symbolFirst=false) {
  const ctx=load(),ss=statusWorkbook();
  const funds=['URA.US','XOP.US','REMX.US','LIT.US','COPX.US'];
  for(let i=0;i<funds.length;i++) ss.sourceGrids.Mutual_Funds[i+1][1]=funds[i];
  for(let i=0;i<4;i++) ss.sourceGrids.Commodities_FX[i+1][1]=['URA.US','REMX.US','LIT.US','COPX.US'][i];
  ss.sourceGrids.Global_Markets[1][1]='XOP.US';
  ctx.DT10_SOURCE_APPROVED_SCOPE.pages.Mutual_Funds.symbol_set_sha256=symbolDigest(ss.sourceGrids.Mutual_Funds.slice(1).map(r=>r[1]));
  if(symbolFirst) for(const page of FULL_PAGES) ss.sourceGrids[page]=ss.sourceGrids[page].map(r=>[r[1],r[0]]);
  const before=ctx.dt10SourceCapture_(ss);
  const collected=ctx.dt10CollectPoolRows_(ss,cap,before);
  Object.assign(before,{pool_counts:collected.available,pool_scope_receipts:collected.approved_scope,
    pool_included_counts:collected.perPage,pool_sent_total:collected.rows.length,pool_reconciliation:collected.pool_reconciliation});
  return {ctx,ss,before,collected};
}
test('five individual earlier-owner duplicates reconcile approved physical funds with the smaller complete unique pool',()=> {
  const {ctx,ss,before,collected}=overlappedPool();
  assert.equal(collected.available.Mutual_Funds,2469);assert.equal(collected.approved_scope.Mutual_Funds.actual_count,2474);
  assert.equal(collected.pool_reconciliation.run,'90001');assert.equal(collected.pool_reconciliation.excluded.length,5);
  assert.deepEqual(plain(collected.pool_reconciliation.excluded),[
    {symbol:'COPX.US',page:'Mutual_Funds',owner:'Commodities_FX'},
    {symbol:'LIT.US',page:'Mutual_Funds',owner:'Commodities_FX'},
    {symbol:'REMX.US',page:'Mutual_Funds',owner:'Commodities_FX'},
    {symbol:'URA.US',page:'Mutual_Funds',owner:'Commodities_FX'},
    {symbol:'XOP.US',page:'Mutual_Funds',owner:'Global_Markets'}
  ]);
  const result=ctx.dt10SourceReadiness_(ss,before);assert.equal(result.ready,true,result.reason);
  assert.equal(result.pool_reconciliation.ready,true);
  const text=ctx.dt10PoolNote_(collected,result);
  assert(text.includes('Mutual_Funds 2469/2474'));assert(text.includes('pool_reconciliation_v1='));
  assert(text.includes('source readiness VERIFIED (run 90001)'));
  const unsigned=plain(collected.pool_reconciliation);delete unsigned.sha256;
  assert.equal(collected.pool_reconciliation.sha256,crypto.createHash('sha256').update(JSON.stringify(unsigned)).digest('hex'));
});
for(const [name,mutate] of [
  ['missing exclusion',f=>f.excluded.pop()],
  ['extra exclusion',f=>f.excluded.push({symbol:'NOT.A.DUPLICATE',page:'Mutual_Funds',owner:'Global_Markets'})],
  ['wrong owner',f=>f.excluded[0].owner='Global_Markets'],
  ['same-page owner',f=>f.excluded[0].owner='Mutual_Funds'],
  ['different excluded identity',f=>f.excluded[0].symbol='UNPROVED.US'],
  ['unique count overclaim',f=>f.pages.Mutual_Funds.unique++],
  ['wrong physical count',f=>f.pages.Global_Markets.physical--],
  ['extra receipt field',f=>f.unrelated=true],
  ['altered hash',f=>f.physical_symbol_sha256.Commodities_FX='0'.repeat(64)],
  ['another cohort',f=>f.run='90002']
]) test(name+' cannot permit a below-physical pool even with a recomputed receipt checksum',()=> {
  const {ctx,ss,before}=overlappedPool();mutate(before.pool_reconciliation);
  const unsigned=plain(before.pool_reconciliation);delete unsigned.sha256;
  before.pool_reconciliation.sha256=ctx.dt10PoolFrameSha_(unsigned);
  const result=ctx.dt10SourceReadiness_(ss,before);assert.equal(result.ready,false);
  assert.equal(result.pool_reconciliation.ready,false);
});
test('missing receipt, changed earlier-owner grid, bad checksum and explicit caps remain WITHHELD',()=> {
  for(const mode of ['missing','owner-changed','bad-checksum','cap']) {
    const {ctx,ss,before,collected}=overlappedPool(mode==='cap'?100:50000);
    if(mode==='missing') before.pool_reconciliation=null;
    if(mode==='owner-changed') ss.sourceGrids.Commodities_FX[1][1]='OWNER.REPLACED';
    if(mode==='bad-checksum') before.pool_reconciliation.sha256='0'.repeat(64);
    const result=ctx.dt10SourceReadiness_(ss,before);assert.equal(result.ready,false,mode);
    assert.equal(ctx.dt10BoardVerdict_(ss,before).state,'NOT_ACTIONABLE');
    if(mode==='cap') {
      assert.equal(collected.pool_reconciliation.pages.Mutual_Funds.physical,2474);
      assert(collected.pool_reconciliation.pages.Mutual_Funds.included<2469);
      assert(ctx.dt10PoolNote_(collected,result).includes('TRUNCATED'));
    }
  }
});
test('a legitimate exclusion receipt does not suppress stale, mixed-run or deficient acquisition guards',()=> {
  for(const mode of ['stale','mixed','acquired-low','status-count','acquired-denominator']) {
    const {ctx,ss,before}=overlappedPool();
    const row=ss.getSheetByName('_Status').getRange(1,1,5,10).getValues()[3]; // Commodities_FX
    if(mode==='stale') row[1]='2026-10-08 12:00:00+03:00';
    if(mode==='mixed') row[3]=row[3].replace('run=90001','run=90002');
    if(mode==='acquired-low') row[3]=row[3].replace('acquired=453/453','acquired=400/453');
    if(mode==='status-count') row[6]=454;
    if(mode==='acquired-denominator') row[3]=row[3].replace('acquired=453/453','acquired=453/454');
    const result=ctx.dt10SourceReadiness_(ss,before);assert.equal(result.ready,false,mode);
  }
});
test('source generation changes after pool capture block even when still fresh and complete',()=> {
  const ctx=load(),grid=goodGrid(),before=capture(ctx,grid);grid[2][1]='2026-10-12 12:01:00+03:00';
  const result=core(ctx,grid,before);
  assert.equal(result.ready,false);assert(result.reason.includes('changed after decision source capture'));
});
test('fresh COMPLETE pages from different producer runs cannot form an executable cycle',()=> {
  const ctx=load(),grid=goodGrid();grid[1][3]=grid[1][3].replace('run=90001','run=90002');
  const result=core(ctx,grid);
  assert.equal(result.ready,false);assert(result.reason.includes('differs from cohort'));
});
for(const [name,run] of [['missing',''],['duplicate','run=90001 run=90001'],['local','run=local']]) {
  test(name+' producer run cannot certify a shared publication cohort',()=> {
    const ctx=load(),grid=goodGrid();grid[2][3]=grid[2][3].replace('run=90001',run);
    const result=core(ctx,grid);
    assert.equal(result.ready,false);assert(result.reason.includes('source publication run unverified'));
  });
}
test('source publication after capture cannot certify an older board',()=> {
  const ctx=load(),grid=goodGrid(),before=capture(ctx,grid);before.captured_at_ms=Date.parse('2026-10-12T08:59:59Z');
  const result=core(ctx,grid,before);
  assert.equal(result.ready,false);assert(result.reason.includes('source newer than decision capture'));
});
test('an initially unready capture cannot become executable during its request',()=> {
  const ctx=load(),before=capture(ctx);before.initial_ready=false;
  assert.equal(core(ctx,goodGrid(),before).ready,false);
});
test('available pool deficits block even if source status reports a large roster',()=> {
  const ctx=load(),before=capture(ctx);before.pool_counts.Market_Leaders=254;
  const result=core(ctx,goodGrid(),before);
  assert.equal(result.ready,false);assert(result.reason.includes('available pool 254 below floor 255'));
});
test('bounded _Status reads and unreadable/missing/oversized inputs fail closed',()=> {
  const ctx=load(),ss=statusWorkbook();assert.equal(ctx.dt10SourceReadiness_(ss).ready,true);
  assert.deepEqual(ss.calls,[[1,1,5,10]]);
  for(const opts of [{missing:true},{fail:true},{rows:1001}])
    assert.equal(ctx.dt10SourceReadiness_(statusWorkbook(undefined,opts)).ready,false);
});
test('upstream executable and gate-off settings cannot override deficient factual sources',()=> {
  const grid=goodGrid();grid[1][6]=254;
  for(const props of [{},{DT10_UPSTREAM_VERDICT:'off'}]) {
    const ctx=load(props),verdict=ctx.dt10BoardVerdict_(statusWorkbook(grid));
    assert.equal(verdict.state,'NOT_ACTIONABLE');assert.equal(verdict.source_blocked,true);
  }
});
test('post-matrix final publication failure remains blocking on otherwise good sources',()=> {
  const ctx=load(),ss=statusWorkbook(undefined,{verdict:'NOT_ACTIONABLE(final_publication) | run=synthetic | 2026-10-12 12:00:00+03:00'});
  const verdict=ctx.dt10BoardVerdict_(ss,capture(ctx));assert.equal(verdict.state,'NOT_ACTIONABLE');
  assert.equal(verdict.reason,'final_publication');
});
function pool() {return {rows:[{Symbol:'SYNTH.US','Current Price':100}],perPage:{...FLOORS},available:{...FLOORS},
  cap:50000,total:12486,truncated:false,duplicatesSkipped:0};}
test('pool notes disclose available rows and defects without a full-universe claim',()=> {
  const ctx=load(),note=ctx.dt10PoolNote_(pool(),{ready:false,reason:'Market_Leaders rows 254 below floor 255'});
  assert(note.includes('all available sheet rows'));assert(note.includes('WITHHELD'));
  assert(!note.includes('(full universe)'));
});
function ticket() {return {symbol:'SYNTH.US',rank:1,price:100,price_sar:375,currency:'USD',
  suggested_shares:20,suggested_sar:7500,exp_gain_12m_sar:1500,entry_zone:'370-380',
  stop_sar:350,tp1_sar:420,tp2_sar:450,advisor_note:'Buy synthetic 20 shares',detail:{funds_from:'Cash'}};}
function payload(ctx) {return {version:'synthetic-builder',status:'ok',selected:[ticket()],
  candidates_rows:[{symbol:'SYNTH.US',verdict:'INVEST',selected:true}],
  kpis:{scanned:1,passed:1,deployable_sar:10000,max_selected:10},alerts:[],near_miss:[],
  meta:{board_funding:{contract_version:1,stage:'allocate',finalized:true,allocation_version:'synthetic-builder',eligible_symbols:['SYNTH.US']}},
  _dt10_source_context:capture(ctx)};}
function renderer(ctx,ss) {
  const titles=[],tables=[],writes=[];
  const range=new Proxy({}, {get(_obj,method) {if(method==='setValues')return values=>{writes.push(plain(values));return range;};return ()=>range;}});
  const sheet={getMaxRows(){return 100;},getMaxColumns(){return 60;},getRange(){return range;},getParent(){return ss;}};
  ctx.dt10WriteSection_=(_s,row,title)=>{titles.push(title);return row+1;};
  ctx.dt10WriteTable_=(_s,row,headers,rows)=>{tables.push(plain({headers,rows}));return {firstDataRow:0,count:0,next:row+1};};
  return {sheet,titles,tables,writes};
}
test('the real final renderer retains a funded board only for bound ready sources',()=> {
  const ctx=load(),p=payload(ctx),r=renderer(ctx,statusWorkbook());ctx.dt10RenderPayload_(r.sheet,p,{});
  assert.equal(p.kpis.selected_count,1);assert.equal(p.kpis.total_suggested_sar,7500);
  assert(r.titles.some(title=>title.includes('FEED ACTIONABLE — EXECUTABLE')));
});
test('late source publication clears same-render sizing levels funding and monetary aggregates',()=> {
  const ctx=load(),p=payload(ctx),grid=goodGrid();grid[2][1]='2026-10-12 12:01:00+03:00';
  const r=renderer(ctx,statusWorkbook(grid));ctx.dt10RenderPayload_(r.sheet,p,{});
  assert.equal(ctx.dt10OutputStatus_(p),'WITHHELD');assert.equal(p.kpis.total_suggested_sar,0);
  assert.equal(p.kpis.selected_count,0);assert.equal(p.kpis.expected_gain_12m_sar,0);
  const row=r.tables[0].rows[0];for(const idx of ctx.DT10_UV_BOARD_WITHHOLD_IDX) assert.equal(row[idx],'—');
  assert(row[29].startsWith('SIZING WITHHELD'));assert(!row[29].includes('20 shares'));
  assert(r.titles.some(title=>title.includes('FEED NOT ACTIONABLE')));
});
test('source changes during sheet preparation are rechecked before painting money',()=> {
  const ctx=load(),p=payload(ctx),grid=goodGrid(),r=renderer(ctx,statusWorkbook(grid));
  const original=r.sheet.getRange;
  r.sheet.getRange=(...args)=>new Proxy(original(...args),{get(target,method) {
    if(method==='clear') return ()=>{grid[2][1]='2026-10-12 12:01:00+03:00';return target;};
    return target[method];
  }});
  ctx.dt10RenderPayload_(r.sheet,p,{});assert.equal(p.kpis.total_suggested_sar,0);
  assert.equal(ctx.dt10OutputStatus_(p),'WITHHELD');
});
test('actual render preserves final earnings prefix while withholding the old order narrative',()=> {
  const ctx=load(),p=payload(ctx),grid=goodGrid();grid[1][2]='PARTIAL';const ss=statusWorkbook(grid);
  const oldGetter=ss.getSheetByName.bind(ss);
  ss.getSheetByName=name=>name==='Calendar_Events'?{getLastRow(){return 2;},getLastColumn(){return 7;},
    getRange(){return {getValues(){return [['Symbol','Next Earnings Date','Days To Earnings'],
      ['SYNTH.US','2026-10-14',99]];}};}}:oldGetter(name);
  const r=renderer(ctx,ss),earn=ctx.dt10RenderPayload_(r.sheet,p,{});
  assert(r.tables[0].rows[0][29].startsWith('⚠ earnings ≤2d · SIZING WITHHELD'));
  assert(!r.tables[0].rows[0][29].includes('20 shares'));assert.equal(earn.note,'earn ⚠1/1');
});
test('direct render without an original source capture cannot promote an old payload',()=> {
  const ctx=load(),p=payload(ctx);delete p._dt10_source_context;
  const r=renderer(ctx,statusWorkbook());ctx.dt10RenderPayload_(r.sheet,p,{});
  assert.equal(ctx.dt10OutputStatus_(p),'WITHHELD');assert.equal(p.kpis.total_suggested_sar,0);
});
test('empty board still visibly reports source WITHHELD',()=> {
  const ctx=load(),p=payload(ctx);p.selected=[];p.kpis.passed=0;
  const grid=goodGrid();grid[1][2]='PARTIAL';const r=renderer(ctx,statusWorkbook(grid));
  ctx.dt10RenderPayload_(r.sheet,p,{});assert.equal(ctx.dt10OutputStatus_(p),'WITHHELD');
  assert(r.titles.some(title=>title.includes('0 EXECUTABLE')));
});
test('real pool adapter preserves quote clocks and held display/deferral semantics',()=> {
  const ctx=load();const headings=['Symbol','Data Provider','primary_provider','Current Price','regularMarketTime','price_bar_ts'];
  const row=ctx.dt10PoolRowFromSheetRow_(['SYNTH.US','yahoo','history',100,1791622800,'2001-01-01T00:00:00Z'],
    ctx.dt10MapHeaderCols_(headings),'Global_Markets');
  assert.equal(row.regularMarketTime,1791622800);assert.equal(row.price_bar_ts,'2001-01-01T00:00:00Z');
  assert.equal(row.primary_provider,'history');
  const p=payload(ctx);p.selected[0]._ft_suspended=true;p.selected[0]._stab_status='FAST-TRACK (day 2, 1/3 confirmed)';
  p.candidates_rows[0].deferral='';ctx.dt10FinalizeBoard_(p);
  assert.equal(p.candidates_rows[0].selected,false);assert(ctx.dt10CandToRow_(p.candidates_rows[0])[24].includes('1/3 confirmed'));
  p.candidates_rows[0].deferral='Backend deferral';assert.equal(ctx.dt10CandToRow_(p.candidates_rows[0])[24],'Backend deferral');
});
function refreshFixture({initialDeficit=false,initialMixed=false,finalPublicationBlocked=false,changeDuringResearch=false,changeScopeDuringResearch=false,changeDuringAllocation=false,empty=false}={}) {
  const ctx=load(),grid=goodGrid(),ss=statusWorkbook(grid,finalPublicationBlocked ?
    {verdict:'NOT_ACTIONABLE(final_publication) | run=90001 | 2026-10-12 12:00:00+03:00 | decision_failed'} : {}),painting=renderer(ctx,ss);
  const calls=[],pageStatuses=[],selectionLogs=[];let stabilityCalls=0;
  if(initialDeficit) grid[1][6]=254;
  if(initialMixed) grid[1][3]=grid[1][3].replace('run=90001','run=90002');
  const baseRange=painting.sheet.getRange;
  painting.sheet.getRange=(row,col,...args)=>{
    const range=baseRange(row,col,...args);
    return new Proxy(range,{get(target,method) {
      if(method==='getValue') return ()=>row===4?'CONTROL PANEL':row===14?'KPIs':'';
      if(method==='setValue') return value=>{painting.writes.push({row,col,value});return range;};
      return target[method];
    }});
  };
  const statusGetter=ss.getSheetByName.bind(ss);
  ss.getSheetByName=name=>name==='Top_10_Investments'?painting.sheet:statusGetter(name);
  ctx.SpreadsheetApp={getActiveSpreadsheet(){return ss;},flush(){}};
  ctx.dt10Tokens_=()=>({});ctx.dt10RunMarkerCheck_=()=>{};ctx.dt10RunMarkerClear_=()=>{};
  ctx.dt10ReadPanel_=()=>({'Pool Source':'Sheets','Pool Limit':0,'T10: Max Selected':10});
  ctx.dt10SyncInflight_=()=>({active:false});ctx.dt10FxRates_=()=>({USD:3.75,SAR:1});
  ctx.dt10PortfolioInputs_=()=>({cash_available_sar:10000,holdings:[],holdings_input_incomplete:false});
  const realCollect=ctx.dt10CollectPoolRows_;
  ctx.dt10CollectPoolRows_=(_ss,limit,sourceContext)=>empty?{...pool(),rows:[],perPage:{},available:{},total:0}:realCollect(ss,limit,sourceContext);
  ctx.dt10ApplyStability_=()=>{stabilityCalls++;return {note:'synthetic unchanged stability'};};
  ctx.dt10WritePageStatus_=(...args)=>pageStatuses.push(args);
  ctx.dt10AppendSelectionLog_=(...args)=>{selectionLogs.push(plain(args.slice(1)));return '';};
  ctx.dt10AppendExitLog_=()=>'';ctx.dt10RememberSuccess_=()=>{};
  ctx.dt10Post_=body=>{
    calls.push(plain(body));const result=payload(ctx);delete result._dt10_source_context;
    if(body.criteria.board_funding_stage==='research') {
      result.meta.board_funding={contract_version:1,stage:'research',snapshot_available:true,snapshot_id:'synthetic-snapshot',
        snapshot:{contract_version:1,builder_version:result.version,snapshot_id:'synthetic-snapshot',rows:plain(body.rows)}};
      result.selected[0]._board_research=true;
      if(changeDuringResearch) grid[2][1]='2026-10-12 12:01:00+03:00';
      if(changeScopeDuringResearch) ss.sourceGrids.Market_Leaders[1][1]='REPLACEMENT.US';
    } else {
      result.meta.board_funding.snapshot_id='synthetic-snapshot';
      if(changeDuringAllocation) grid[2][1]='2026-10-12 12:01:00+03:00';
    }
    return {code:200,json:result};
  };
  ctx.refreshDecisionTop10();
  return {ctx,calls,pageStatuses,selectionLogs,stabilityCalls,painting};
}
test('actual refresh transports research/replay and declares current ready final board',()=> {
  const r=refreshFixture();assert.equal(r.calls.length,2);assert.equal(r.stabilityCalls,1);
  assert.equal(r.calls[0].criteria.board_funding_stage,'research');assert.equal(r.calls[1].criteria.board_funding_stage,'allocate');
  assert.equal(r.pageStatuses[0][0],'OK');assert(r.pageStatuses[0][1].includes('source readiness VERIFIED'));
  assert(!r.pageStatuses[0][1].includes('(full universe)'));
  assert(r.painting.titles.some(t=>t.includes('FEED ACTIONABLE — EXECUTABLE')));
  assert(r.selectionLogs[0][4].generation.Global_Markets);
});
for(const [name,options] of [['initial source deficit',{initialDeficit:true}],['mixed source runs',{initialMixed:true}],['physical roster substituted during research',{changeScopeDuringResearch:true}],
  ['source changed during research',{changeDuringResearch:true}]]) {
  test('actual refresh '+name+' blocks allocation and freezes stability writes',()=> {
    const r=refreshFixture(options);assert.equal(r.calls.length,1);assert.equal(r.stabilityCalls,0);
    assert.equal(r.pageStatuses[0][0],'WITHHELD');assert(r.pageStatuses[0][1].includes('output: WITHHELD'));
    assert(r.painting.titles.some(t=>t.includes('FEED NOT ACTIONABLE')));
    for(const idx of r.ctx.DT10_UV_BOARD_WITHHOLD_IDX) assert.equal(r.painting.tables[0].rows[0][idx],'—');
  });
}
test('actual refresh source change during allocation withholds the replay in final rendering',()=> {
  const r=refreshFixture({changeDuringAllocation:true});assert.equal(r.calls.length,2);
  assert.equal(r.pageStatuses[0][0],'WITHHELD');assert(r.pageStatuses[0][1].includes('changed after decision source capture'));
  for(const idx of r.ctx.DT10_UV_BOARD_WITHHOLD_IDX) assert.equal(r.painting.tables[0].rows[0][idx],'—');
});
test('actual refresh cannot self-clear a final publication blocker on complete shared-cycle sources',()=> {
  const r=refreshFixture({finalPublicationBlocked:true});assert.equal(r.calls.length,1);
  assert(r.pageStatuses[0][1].includes('source readiness VERIFIED (run 90001)'));
  assert(r.pageStatuses[0][1].includes('output: WITHHELD'));
  assert(r.painting.titles.some(t=>t.includes('final_publication')));
  for(const idx of r.ctx.DT10_UV_BOARD_WITHHOLD_IDX) assert.equal(r.painting.tables[0].rows[0][idx],'—');
});
test('actual empty Sheets refresh renders zero with no provider fallback or fictitious HTTP result',()=> {
  const r=refreshFixture({empty:true});assert.equal(r.calls.length,0);assert.equal(r.stabilityCalls,0);
  assert.equal(r.pageStatuses[0][0],'WITHHELD');assert.equal(r.pageStatuses[0][2],'');
  assert(r.pageStatuses[0][1].includes('no backend request or allocation'));
  assert(!r.pageStatuses[0][1].includes('(full universe)'));
  assert.equal(r.painting.tables[0].rows.length,0);
});
// Owner-provided current snapshot stays outside the repository. This optional
// check runs the embedded production hashes against its complete real columns.
const approvedArg=process.argv.indexOf('--approved-roster');
if(approvedArg>=0) {
  const approved=JSON.parse(fs.readFileSync(process.argv[approvedArg+1],'utf8'));
  test('current owner-approved physical fixture matches the exact embedded production hashes',()=> {
    const ctx=load({}, {nativeHashes:true}),ss=statusWorkbook(undefined,{sourceGrids:approved.pages});
    const result=ctx.dt10SourceReadiness_(ss);assert.equal(result.ready,true,result.reason);
    assert.equal(ctx._digestCalls.length,2);
    for(const page of SCOPE_PAGES) assert.equal(result.approved_scope[page].observed_symbol_set_sha256,ctx._embeddedScope.pages[page].symbol_set_sha256);
  });
  test('a same-count substitution in the approved physical fixture is rejected with unchanged successful status',()=> {
    const ctx=load({}, {nativeHashes:true}),grids=plain(approved.pages);
    grids.Market_Leaders[1][0]='UNAPPROVED.SUBSTITUTION';
    const result=ctx.dt10SourceReadiness_(statusWorkbook(undefined,{sourceGrids:grids}));
    assert.equal(result.ready,false);assert(result.reason.includes('exact approved membership mismatch'));
  });
}
const receiptArg=process.argv.indexOf('--receipt-fixture');
if(receiptArg>=0) {
  const {ctx,ss,before,collected}=overlappedPool(50000,true);
  const result=ctx.dt10SourceReadiness_(ss,before);
  const fixture={synthetic_test_fixture:true,not_a_live_execution_receipt:true,source_grids:ss.sourceGrids,
    status_grid:goodGrid(),frame:plain(collected.pool_reconciliation),
    b2:ctx.dt10PoolNote_(collected,result)};
  fs.writeFileSync(process.argv[receiptArg+1],JSON.stringify(fixture));
  console.log('WROTE synthetic cross-language pool frame fixture');
}
console.log('Full-source native source readiness: '+passed+' passed');
