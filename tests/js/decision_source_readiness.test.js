/* Execute the full native renderer/request source. Only external services and
 * unrelated refresh orchestration are fixtures; readiness and finalization
 * are production functions, including the real source read and HTTP body seam. */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../../apps_script/16_Decision_Top10.gs'), 'utf8');
const plain = value => JSON.parse(JSON.stringify(value));
const NOW = Date.parse('2026-10-10T09:30:00Z');
const STAMP = '2026-10-10 12:00:00+03:00';
const FLOORS = {Market_Leaders:1025, Global_Markets:6512, Commodities_FX:453, Mutual_Funds:4496};
const HEADER = ['Page','Last Updated','Status','Message','Endpoint','HTTP Code','Rows','Columns','Duration ms','Warnings'];
let passed = 0;
function test(name, fn) {fn(); passed++; console.log('PASS ' + name);}
function goodGrid() {
  return [HEADER, ...Object.entries(FLOORS).map(([page,count]) => [page,STAMP,'SUCCESS',
    `acquired=${count}/${count} acquisition=COMPLETE fetchfail_effective=enforce | data=COMPLETE run=90001`,
    'synthetic-source',200,count,115,1,0])];
}
function load(props = {}) {
  class FixedDate extends Date {
    constructor(...args) {super(...(args.length ? args : [NOW]));}
    static now() {return NOW;}
  }
  const ctx = {Date:FixedDate, Logger:{log() {}}, PropertiesService:{getScriptProperties() {
    return {getProperty(k) {return props[k] || null;},setProperty(k,v) {props[k]=v;},deleteProperty(k) {delete props[k];}};
  }}, Session:{getScriptTimeZone() {return 'Asia/Riyadh';}}, Utilities:{formatDate(date,_zone,pattern) {
    const local = new Date(date.getTime()+3*3600000).toISOString();
    return pattern === 'yyyy-MM-dd' ? local.slice(0,10) : local.replace('T',' ').slice(0,19);
  }}};
  vm.createContext(ctx); vm.runInContext(source,ctx,{filename:'16_Decision_Top10.gs'});
  return ctx;
}
function capture(ctx,grid = goodGrid(),pool = FLOORS) {
  const source = ctx.dt10SourceReadinessCore_(grid,NOW);
  return {captured_at_ms:NOW,initial_ready:source.ready,generation:plain(source.generation),pool_counts:{...pool}};
}
function statusWorkbook(grid = goodGrid(), options = {}) {
  const calls = [];
  const status = {getLastRow() {return options.rows || grid.length;}, getLastColumn() {return 13;},
    getRange(...args) {
      calls.push(args);
      return {getValues() {
        if(options.fail) throw new Error('Synthetic service failure');
        if(typeof args[0] === 'string') return [['TFB Decision Feed',options.verdict ||
          'EXECUTABLE | run=synthetic | 2026-10-10 12:00:00+03:00 | ML:OK GM:OK CFX:OK MF:OK']];
        return grid;
      }};
    }};
  return {calls, getSheetByName(name) {return name === '_Status' && !options.missing ? status : null;}};
}
test('complete current receipts and unchanged pool generation are ready',()=> {
  const ctx=load(),grid=goodGrid();const result=ctx.dt10SourceReadinessCore_(grid,NOW,capture(ctx,grid));
  assert.equal(result.ready,true);assert.equal(result.reason,'');
});
test('declared clocks agree and date-only/impossible clocks stay unverified',()=> {
  const ctx=load(),expected=Date.parse('2026-10-10T09:00:00Z');
  for(const stamp of [STAMP,'2026-10-10 09:00:00Z','2026-10-10 12:00:00',new Date(expected)])
    assert.equal(ctx.dt10SourceStampMs_(stamp),expected);
  for(const stamp of ['2026-10-10','2026-02-30 12:00:00+03:00','2026-10-10 25:00:00Z','junk'])
    assert.equal(ctx.dt10SourceStampMs_(stamp),null);
});
for(const [name,change,expected] of [
  ['partial page',r=>r[2]='PARTIAL','status=PARTIAL'],
  ['stale page',r=>r[1]='2026-10-08 12:00:00+03:00','source stale'],
  ['unknown acquisition',r=>r[3]='acquired=unknown/1025 acquisition=UNKNOWN | data=PARTIAL','receipt unknown'],
  ['observe policy factual partial',r=>r[3]='acquired=900/1025 acquisition=PARTIAL fetchfail_effective=observe | data=PARTIAL','acquisition not COMPLETE'],
  ['policy error',r=>r[3]=r[3].replace('fetchfail_effective=enforce','fetchfail_effective=error'),'policy error'],
  ['duplicated receipt',r=>r[3]+=' acquired=1025/1025','receipt unknown'],
  ['date-only stamp',r=>r[1]='2026-10-10','timestamp unverified'],
  ['future publication',r=>r[1]='2026-10-10 12:31:00+03:00','future'],
  ['physical row deficit',r=>r[6]=255,'rows 255 below floor 1025'],
  ['small acquisition denominator',r=>r[3]='acquired=255/255 acquisition=COMPLETE | data=COMPLETE','denominator 255 below floor 1025'],
  ['acquisition greater than requested',r=>r[3]='acquired=1026/1025 acquisition=COMPLETE | data=COMPLETE','receipt unknown'],
  ['missing data evidence',r=>r[3]='acquired=1025/1025 acquisition=COMPLETE','data not COMPLETE']
]) {
  test(name+' cannot certify a successful status cell',()=> {
    const ctx=load(),grid=goodGrid();change(grid[1]);const result=ctx.dt10SourceReadinessCore_(grid,NOW);
    assert.equal(result.ready,false);assert(result.reason.includes(expected),result.reason);
  });
}
test('duplicate policy tokens cannot certify readiness',()=> {
  const ctx=load(),grid=goodGrid();grid[1][3]+=' fetchfail_effective=error';
  // Duplicate effective policy is ambiguous even with valid acquisition.
  assert.equal(ctx.dt10SourceReadinessCore_(grid,NOW).ready,false);
});
test('missing or duplicated required source rows are explicit failures',()=> {
  const ctx=load();for(const grid of [goodGrid().slice(0,-1), [...goodGrid(),goodGrid()[1]]])
    assert.equal(ctx.dt10SourceReadinessCore_(grid,NOW).ready,false);
});
test('source acquisition exactly 95 percent passes and lower percentage fails',()=> {
  const ctx=load(),grid=goodGrid();grid[1][3]='acquired=1900/2000 acquisition=COMPLETE fetchfail_effective=enforce | data=COMPLETE run=90001';
  assert.equal(ctx.dt10SourceReadinessCore_(grid,NOW).ready,true);
  grid[1][3]='acquired=1899/2000 acquisition=COMPLETE fetchfail_effective=enforce | data=COMPLETE run=90001';
  assert.equal(ctx.dt10SourceReadinessCore_(grid,NOW).ready,false);
});
test('all four approved source floors remain unchanged',()=>assert.deepEqual(plain(load().DT10_SOURCE_MIN_ROWS),FLOORS));
test('configuration can raise floors but cannot lower them',()=> {
  const ctx=load({TFB_EXPECTED_MIN_ROWS_MARKET_LEADERS:'255',TFB_EXPECTED_MIN_ROWS_MUTUAL_FUNDS:'5000'});
  const policy=ctx.dt10SourcePolicy_();assert.equal(policy.floors.Market_Leaders,1025);
  assert.equal(policy.floors.Mutual_Funds,5000);
  assert.equal(ctx.dt10SourceReadinessCore_(goodGrid(),NOW,null,policy).ready,false);
});
test('source generation changes after pool capture block even when still fresh and complete',()=> {
  const ctx=load(),grid=goodGrid(),before=capture(ctx,grid);grid[2][1]='2026-10-10 12:01:00+03:00';
  const result=ctx.dt10SourceReadinessCore_(grid,NOW,before);
  assert.equal(result.ready,false);assert(result.reason.includes('changed after decision source capture'));
});
test('fresh COMPLETE pages from different producer runs cannot form an executable cycle',()=> {
  const ctx=load(),grid=goodGrid();grid[1][3]=grid[1][3].replace('run=90001','run=90002');
  const result=ctx.dt10SourceReadinessCore_(grid,NOW);
  assert.equal(result.ready,false);assert(result.reason.includes('differs from cohort'));
});
for(const [name,run] of [['missing',''],['duplicate','run=90001 run=90001'],['local','run=local']]) {
  test(name+' producer run cannot certify a shared publication cohort',()=> {
    const ctx=load(),grid=goodGrid();grid[2][3]=grid[2][3].replace('run=90001',run);
    const result=ctx.dt10SourceReadinessCore_(grid,NOW);
    assert.equal(result.ready,false);assert(result.reason.includes('source publication run unverified'));
  });
}
test('source publication after capture cannot certify an older board',()=> {
  const ctx=load(),grid=goodGrid(),before=capture(ctx,grid);before.captured_at_ms=Date.parse('2026-10-10T08:59:59Z');
  const result=ctx.dt10SourceReadinessCore_(grid,NOW,before);
  assert.equal(result.ready,false);assert(result.reason.includes('source newer than decision capture'));
});
test('an initially unready capture cannot become executable during its request',()=> {
  const ctx=load(),before=capture(ctx);before.initial_ready=false;
  assert.equal(ctx.dt10SourceReadinessCore_(goodGrid(),NOW,before).ready,false);
});
test('available pool deficits block even if source status reports a large roster',()=> {
  const ctx=load(),before=capture(ctx);before.pool_counts.Market_Leaders=255;
  const result=ctx.dt10SourceReadinessCore_(goodGrid(),NOW,before);
  assert.equal(result.ready,false);assert(result.reason.includes('available pool 255 below floor 1025'));
});
test('bounded _Status reads and unreadable/missing/oversized inputs fail closed',()=> {
  const ctx=load(),ss=statusWorkbook();assert.equal(ctx.dt10SourceReadiness_(ss).ready,true);
  assert.deepEqual(ss.calls,[[1,1,5,10]]);
  for(const opts of [{missing:true},{fail:true},{rows:1001}])
    assert.equal(ctx.dt10SourceReadiness_(statusWorkbook(undefined,opts)).ready,false);
});
test('upstream executable and gate-off settings cannot override deficient factual sources',()=> {
  const grid=goodGrid();grid[1][6]=255;
  for(const props of [{},{DT10_UPSTREAM_VERDICT:'off'}]) {
    const ctx=load(props),verdict=ctx.dt10BoardVerdict_(statusWorkbook(grid));
    assert.equal(verdict.state,'NOT_ACTIONABLE');assert.equal(verdict.source_blocked,true);
  }
});
test('post-matrix final publication failure remains blocking on otherwise good sources',()=> {
  const ctx=load(),ss=statusWorkbook(undefined,{verdict:'NOT_ACTIONABLE(final_publication) | run=synthetic | 2026-10-10 12:00:00+03:00'});
  const verdict=ctx.dt10BoardVerdict_(ss,capture(ctx));assert.equal(verdict.state,'NOT_ACTIONABLE');
  assert.equal(verdict.reason,'final_publication');
});
function pool() {return {rows:[{Symbol:'SYNTH.US','Current Price':100}],perPage:{...FLOORS},available:{...FLOORS},
  cap:50000,total:12486,truncated:false,duplicatesSkipped:0};}
test('pool notes disclose available rows and defects without a full-universe claim',()=> {
  const ctx=load(),note=ctx.dt10PoolNote_(pool(),{ready:false,reason:'Market_Leaders rows 255 below floor 1025'});
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
  const ctx=load(),p=payload(ctx),grid=goodGrid();grid[2][1]='2026-10-10 12:01:00+03:00';
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
    if(method==='clear') return ()=>{grid[2][1]='2026-10-10 12:01:00+03:00';return target;};
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
  assert(r.tables[0].rows[0][29].startsWith('⚠ earnings ≤4d · SIZING WITHHELD'));
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
function refreshFixture({initialDeficit=false,initialMixed=false,finalPublicationBlocked=false,changeDuringResearch=false,changeDuringAllocation=false,empty=false}={}) {
  const ctx=load(),grid=goodGrid(),ss=statusWorkbook(grid,finalPublicationBlocked ?
    {verdict:'NOT_ACTIONABLE(final_publication) | run=90001 | 2026-10-10 12:00:00+03:00 | decision_failed'} : {}),painting=renderer(ctx,ss);
  const calls=[],pageStatuses=[],selectionLogs=[];let stabilityCalls=0;
  if(initialDeficit) grid[1][6]=255;
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
  ctx.dt10CollectPoolRows_=()=>empty?{...pool(),rows:[],perPage:{},available:{},total:0}:pool();
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
      if(changeDuringResearch) grid[2][1]='2026-10-10 12:01:00+03:00';
    } else {
      result.meta.board_funding.snapshot_id='synthetic-snapshot';
      if(changeDuringAllocation) grid[2][1]='2026-10-10 12:01:00+03:00';
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
for(const [name,options] of [['initial source deficit',{initialDeficit:true}],['mixed source runs',{initialMixed:true}],
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
console.log('Full-source native source readiness: '+passed+' passed');
