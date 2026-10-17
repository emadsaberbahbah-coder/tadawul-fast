/* Synthetic Apps Script boundaries; actual native completion/render/format code. */
'use strict';
const assert=require('node:assert/strict');
const {load,clone,Sheet,RUN}=require('./native_top10_harness');
let passed=0;
function test(name,fn){fn();passed++;console.log('PASS '+name);}
function sheets(w){w.ctx.dt10ReadPanel_=()=>({'Pool Source':'Sheets','T10: Stability Enabled':'No'});return w;}
function held(w){w.props.DT10_SYNC_INFLIGHT='enforce';
  w.feedRows[1][1]='OK | run=90000 | 2026-10-12 11:55:00+03:00';return w;}
function rowPayload(w){w.payload.status='ok';w.payload.selected=[{symbol:'SYNTH.US',
  name:'Synthetic',rank:1,price:0.000001234567,price_sar:0.000004321,
  suggested_shares:5,suggested_sar:50,entry_zone:'synthetic',stop_sar:0.000001,
  tp1_sar:0.000002,tp2_sar:0.000003,advisor_note:'Synthetic research'}];
  w.payload.candidates_rows=[{symbol:'CAND.US',name:'Synthetic candidate',
    price:1.23e-12,price_sar:4.56e-12,verdict:'UNKNOWN',engine_roi_pct:null}];return w;}

test('verified actual Sheets consumption returns completed exact cohort',()=>{
  const w=sheets(load('valid'));const r=w.ctx.refreshDecisionTop10({completionContract:true});
  assert.deepEqual(clone(r),{contract:'tfb_native_refresh_v1',page:'Top_10_Investments',
    state:'completed',source_run:RUN,reason:'',output_state:'EMPTY',
    source_latest_ms:Date.parse('2026-10-12T09:00:00Z')});
  assert(w.fetches[0].body.rows.length>9000);assert.equal(w.statuses.at(-1).status,'OK');
});
test('backend selector render is complete but cannot invent consumed-row proof',()=>{
  const w=load('valid');const r=w.ctx.refreshDecisionTop10();
  assert.equal(r.state,'completed');assert.equal(r.source_run,'');
  assert.equal(r.reason,'consumed_source_unverified');assert(w.clears.length>0);
  assert(!Object.hasOwn(r,'source_latest_ms'));
});
for(const page of ['Commodities_FX','Mutual_Funds'])
  test('typed actual consumed timestamp includes newer '+page+' same-run publication',()=>{
    const w=sheets(load('valid'));
    w.statusGrid.find(row=>row[0]===page)[1]='2026-10-12 12:20:00+03:00';
    const r=w.ctx.refreshDecisionTop10({completionContract:true});
    assert.equal(r.state,'completed');assert.equal(r.source_run,RUN);
    assert.equal(r.source_latest_ms,Date.parse('2026-10-12T09:20:00Z'));
  });
test('source timestamp projection refuses missing or malformed consumed generations',()=>{
  const w=load('valid'),g={};
  for(const page of w.ctx.DT10_POOL_PAGES) g[page]=JSON.stringify(['2026-10-12 12:00:00+03:00','OK','',1]);
  assert.equal(w.ctx.dt10SourceLatestMs_({ready:true,generation:g}),Date.parse('2026-10-12T09:00:00Z'));
  for(const bad of [undefined,'{}','not JSON',JSON.stringify(['not a timestamp','OK','',1])]) {
    const copy={...g};copy.Mutual_Funds=bad;
    assert.equal(w.ctx.dt10SourceLatestMs_({ready:true,generation:copy}),null);
  }
  assert.equal(w.ctx.dt10SourceLatestMs_({ready:false,generation:g}),null);
});
test('opt-in backend selector completion defers before any POST or board mutation',()=>{
  const w=load('valid');const r=w.ctx.refreshDecisionTop10({completionContract:true});
  assert.equal(r.state,'deferred');assert.equal(r.reason,'sheets_pool_required');
  assert.equal(r.source_run,'');assert.equal(w.fetches.length,0);assert.equal(w.clears.length,0);
  assert.equal(w.statuses.at(-1).status,'HELD');assert(!w.props.DT10_RUN_MARKER);
});
test('real sync-inflight HELD returns deferred without a post or board clear',()=>{
  const w=held(load('valid'));const r=w.ctx.refreshDecisionTop10({completionContract:true});
  assert.equal(r.state,'deferred');assert.equal(r.reason,'sync_inflight');
  assert.equal(r.source_run,'');assert.equal(r.output_state,'HELD');
  assert.equal(w.fetches.length,0);assert.equal(w.clears.length,0);
  assert.equal(w.statuses.at(-1).status,'HELD');assert(!w.props.DT10_RUN_MARKER);
});
test('morning entrypoint records legitimate HELD as WARN DEFERRED',()=>{
  const w=held(load('valid'));assert.equal(w.ctx.tfbMorningCockpitRefresh().state,'deferred');
  const log=w.logs.find(x=>x.row[2]==='tfbMorningCockpitRefresh');
  assert.equal(log.row[1],'WARN');assert.equal(log.row[4],'DEFERRED');
  assert(!w.logs.some(x=>x.row[2]==='tfbMorningCockpitRefresh'&&['OK','ERROR'].includes(x.row[4])));
});
for(const [feed,output] of [['NOT_ACTIONABLE','EMPTY'],['EXECUTABLE','EMPTY']])
  test(feed+' genuine empty output is completed and morning remains OK',()=>{
    const w=sheets(load('valid',feed));const r=w.ctx.tfbMorningCockpitRefresh();
    assert.equal(r.state,'completed');assert.equal(r.output_state,output);
    assert(w.logs.some(x=>x.row[2]==='tfbMorningCockpitRefresh'&&x.row[4]==='OK'));
  });
test('rendered WITHHELD research output remains completed, not failed/deferred',()=>{
  const w=rowPayload(sheets(load('valid')));const r=w.ctx.refreshDecisionTop10();
  assert.equal(r.state,'completed');assert.equal(r.output_state,'WITHHELD');assert.equal(r.source_run,RUN);
});

for(const value of [0.4,0.00001234567,1.23e-12,-1.23e-12,Number.MIN_VALUE])
  test('armed native price format preserves finite magnitude '+value,()=>{
    const w=load('valid');const f=w.ctx.dt10PriceFormat_(value);
    if(Math.abs(value)<1e-14) assert.equal(f,'0.000000E+00');
    else {const decimals=f.split('.')[1].length;
      assert(decimals>=2);assert.notEqual(value.toFixed(decimals),'0.'+'0'.repeat(decimals));}
    assert.equal(value<0,(-Math.abs(value))===value);
  });
for(const value of [0,10,123,null,undefined,'0.00001',NaN,Infinity,true])
  test('legacy native format retained for non-small-numeric '+String(value),()=>{
    assert.equal(load('valid').ctx.dt10PriceFormat_(value),'#,##0.00');
  });
test('power-of-ten formats use exactly seven significant digits for either sign',()=>{
  const w=load('valid');
  for(let exponent=-13;exponent<=0;exponent++) for(const sign of [1,-1]) {
    const value=sign*Math.pow(10,exponent),digits=Math.max(2,6-exponent);
    assert.equal(w.ctx.dt10PriceFormat_(value),'#,##0.'+'0'.repeat(digits));
  }
});
test('price matrix writes are bounded by columns and never alter stored values',()=>{
  const w=load('valid');const sh=new Sheet(w,'Synthetic');
  const rows=Array.from({length:500},()=>[1.23e-12,4.56e-12,999,0.4]);const before=clone(rows);
  w.ctx.dt10ApplyPriceFormats_(sh,3,rows,[1,2],'on');
  assert.equal(w.formatMatrices.length,2);assert.equal(w.formatMatrices[0].values.length,500);
  assert.deepEqual(rows,before);assert.equal(w.writes.length,0);assert.equal(w.fetches.length,0);
});
for(const mode of [undefined,null,'off','observe','ON',true,1,'true'])
  test('only exact backend metadata on enables native formatting: '+String(mode),()=>{
    const w=load('valid');w.ctx.dt10ApplyPriceFormats_(w.board,3,[[1.23e-12]],[1],mode);
    assert.equal(w.formatMatrices.length,0);
  });
test('full render price precision off preserves output values and legacy formats',()=>{
  const a=rowPayload(load('valid')),b=rowPayload(load('valid'));
  b.payload.meta.price_precision='off';a.ctx.refreshDecisionTop10();b.ctx.refreshDecisionTop10();
  assert.deepEqual(a.writes,b.writes);assert.deepEqual(a.formats,b.formats);
  assert.equal(a.formatMatrices.length,0);assert.equal(b.formatMatrices.length,0);
});
test('armed full render touches only price columns and keeps withholding/math intact',()=>{
  const a=rowPayload(load('valid')),b=rowPayload(load('valid'));
  b.payload.meta.price_precision='on';a.ctx.refreshDecisionTop10();b.ctx.refreshDecisionTop10();
  assert.deepEqual(a.writes,b.writes);assert.deepEqual(a.fetches,b.fetches);
  assert(b.formatMatrices.length>0);const cols=new Set(b.formatMatrices.map(x=>x.col));
  assert.deepEqual([...cols].sort((x,y)=>x-y),[6,7,8,9,13,14,15]);
  const selected=b.writes.find(x=>x.values.some(row=>row[1]==='SYNTH.US'));
  const r=selected.values.find(row=>row[1]==='SYNTH.US');
  assert.equal(r[7],0.000001234567);assert.equal(r[11],'\u2014');
  assert.equal(b.board.formats.get(selected.row+':8'),'#,##0.000000000000');
});
test('on-to-off full render resets earlier precision to legacy column formats',()=>{
  const w=rowPayload(load('valid'));w.payload.meta.price_precision='on';w.ctx.refreshDecisionTop10();
  const selected=w.writes.find(x=>x.values.some(row=>row[1]==='SYNTH.US'));
  const key=selected.row+':8';assert.notEqual(w.board.formats.get(key),'#,##0.00');
  w.formatMatrices=[];delete w.payload.meta.price_precision;w.ctx.refreshDecisionTop10();
  assert.equal(w.board.formats.get(key),'#,##0.00');assert.equal(w.formatMatrices.length,0);
});
test('hidden candidate publication uses identical price-only formatting matrices',()=>{
  const w=load('valid'),stage=new Sheet(w,'_DT10_Candidates_Stage'),out=new Sheet(w,'_DT10_Candidates');
  const extra={};extra[w.ctx.DT10_CANDTAB_STAGE_SHEET]=stage;extra[w.ctx.DT10_CANDTAB_SHEET]=out;
  for(const sh of [stage,out]) {sh.clear=()=>sh;sh.isSheetHidden=()=>true;
    sh.setFrozenRows=()=>sh;sh.getLastRow=()=>3;}
  const orig=w.book.getSheetByName.bind(w.book);w.book.getSheetByName=n=>extra[n]||orig(n);
  const rows=[w.ctx.dt10CandToRow_({symbol:'CAND.US',price:1.23e-12,price_sar:4.56e-12})];
  const before=clone(rows);const r=w.ctx.dt10CandTabWrite_(w.book,rows,'Synthetic',null,'native-synthetic','on');
  assert.equal(r.ok,true,r.err);assert.deepEqual(clone(rows),before);
  assert.deepEqual(w.formatMatrices.map(x=>x.col),[6,7]);
  assert.equal(out.formats.get('3:6'),'#,##0.000000000000000000');
  assert.equal(w.lockAcquires,1);assert.equal(w.lockReleases,1);assert.equal(w.lockOwned,false);
});
for(const invalid of [false,true])
  test('candidate publication preserves coordinator-owned lock on '+(invalid?'failure':'success'),()=>{
    const w=load('valid'),stage=new Sheet(w,'Stage'),out=new Sheet(w,'Published');
    const extra={};extra[w.ctx.DT10_CANDTAB_STAGE_SHEET]=stage;extra[w.ctx.DT10_CANDTAB_SHEET]=out;
    for(const sh of [stage,out]) {sh.clear=()=>sh;sh.isSheetHidden=()=>true;
      sh.setFrozenRows=()=>sh;sh.getLastRow=()=>3;}
    if(invalid) stage.getLastRow=()=>99;
    const orig=w.book.getSheetByName.bind(w.book);w.book.getSheetByName=n=>extra[n]||orig(n);
    w.lockOwned=true;const rows=[w.ctx.dt10CandToRow_({symbol:'CAND.US'})];
    const r=w.ctx.dt10CandTabWrite_(w.book,rows,'Synthetic',null,'native-synthetic');
    assert.equal(r.ok,!invalid);assert.equal(w.lockAcquires,0);assert.equal(w.lockReleases,0);
    assert.equal(w.lockOwned,true,'nested writer cannot release caller ownership');
  });

test('audit keep integrates pending native handoff off by default',()=>{
  const w=load('valid');w.ctx.refreshDecisionTop10();
  assert(!Object.hasOwn(w.fetches[0].body.criteria,'audit_keep_symbols'));
});
test('audit keep only exact member=true sorted bounded handoff and no source grants',()=>{
  const w=load('valid');w.props.DT10_AUDIT_KEEP=' On ';
  const symbols=Object.fromEntries(Array.from({length:75},(_,i)=>['SYM'+String(i).padStart(3,'0'),{member:true}]));
  symbols.FALSE={member:false};symbols.TRUTHY={member:1};symbols.EMPTY=null;
  w.ctx.dt10StabLoad_=()=>({symbols});w.ctx.refreshDecisionTop10();
  const body=w.fetches[0].body;assert.equal(body.criteria.audit_keep_symbols.length,60);
  assert.equal(body.criteria.audit_keep_symbols[0],'SYM000');assert.equal(body.criteria.audit_keep_symbols.at(-1),'SYM059');
  assert(!body.criteria.audit_keep_symbols.includes('TRUTHY'));assert(!body.rows);
});
test('audit keep property-service failure remains default off',()=>{
  const w=load('valid');w.propertyError='DT10_AUDIT_KEEP';w.ctx.refreshDecisionTop10();
  assert(!Object.hasOwn(w.fetches[0].body.criteria,'audit_keep_symbols'));
});
console.log('PASS '+passed+'/'+passed);
