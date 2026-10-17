/* Offline actual-chain regressions. Services/transport are synthetic; planner,
 * source parsing, claims, completion classification and finalization are real.
 * Integrated cases also execute delivered native16 without replacing its gates.
 */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {load, clone, NOW, RUN, STAMP} = require('./native_top10_harness');
const source = fs.readFileSync(path.join(__dirname, '../../apps_script/26_Decision_Chain.gs'), 'utf8');
const coordinatorSource = fs.readFileSync(path.join(__dirname, '../../apps_script/11_Manual_Refresh_Coordinator.gs'), 'utf8');
const KEY = 'TFB_DECISION_CHAIN_STATE';
const SOURCE_MS = Date.parse(STAMP.replace(' ','T'));
const keyFor = (action='t10', identity=SOURCE_MS) => action+':'+RUN+':'+identity;
let passed = 0;
function test(name, fn) { fn(); passed++; console.log('PASS ' + name); }
function receipt(action='t10', fields={}) {
  return {contract:'tfb_native_refresh_v1',
    page:action === 'pd' ? 'Portfolio_Decision' : 'Top_10_Investments',
    state:'completed',source_run:RUN,source_latest_ms:SOURCE_MS,source_stamp_ms:SOURCE_MS,
    output_state:'WITHHELD',...fields};
}
function world() {
  const w = load('valid');
  vm.runInContext(coordinatorSource,w.ctx,{filename:'11_Manual_Refresh_Coordinator.gs'});
  vm.runInContext(source,w.ctx,{filename:'26_Decision_Chain.gs'});
  w.now = NOW; w.ctx.Date.now = () => w.now;
  w.calls = 0; w.uuid = 0; w.lockDepth = 0;
  w.io = {books:0,locks:0,sets:0,deletes:0,triggerWrites:0,guardCalls:0};
  w.lockResults = []; w.finishError = false;
  w.ctx.Utilities.getUuid = () => 'synthetic-owner-' + (++w.uuid);
  const getBook = w.ctx.SpreadsheetApp.getActiveSpreadsheet;
  w.ctx.SpreadsheetApp.getActiveSpreadsheet = () => {w.io.books++;return getBook();};
  const props = {
    getProperty(k) {return Object.hasOwn(w.props,k) ? w.props[k] : null;},
    setProperty(k,v) {
      if (w.finishError && k === KEY && JSON.parse(v).claim === null) throw Error('synthetic write failed');
      w.io.sets++; w.props[k]=String(v);
    },
    deleteProperty(k) {w.io.deletes++;delete w.props[k];}
  };
  w.ctx.PropertiesService = {getScriptProperties(){return props;},getDocumentProperties(){return props;}};
  w.ctx.LockService = {getScriptLock() {
    w.io.locks++;
    return {hasLock(){return w.lockDepth>0;},tryLock(wait) {
      const ok = w.lockResults.length ? w.lockResults.shift() : true;
      if (ok) {assert.equal(w.lockDepth,0,'no nested ScriptLock transaction');w.lockDepth++;
        w.lockKind=wait===5000?'transaction':'writer';}
      return ok;
    },releaseLock(){assert.equal(w.lockDepth,1);w.lockDepth--;}};
  }};
  w.ctx.ScriptApp = {getProjectTriggers(){return [];},deleteTrigger(){w.io.triggerWrites++;},
    newTrigger(){w.io.triggerWrites++;throw Error('no trigger mutation expected');}};
  const getSheet = w.book.getSheetByName.bind(w.book);
  const status = getSheet('_Status');
  let feedReads = 0, gridReads = 0;
  w.statusTransform = (values) => values;
  w.book.getSheetByName = name => name === '_Status' ? {
    getLastRow:()=>status.getLastRow(),getLastColumn:()=>status.getLastColumn(),
    getRange(...args) {
      const values = status.getRange(...args).getValues();
      const kind = typeof args[0] === 'string' ? 'feeds' : 'grid';
      const count = kind === 'feeds' ? ++feedReads : ++gridReads;
      if(kind==='feeds') {
        const decision=values.find(r=>r[0]==='TFB Decision Feed');
        decision[1]='NOT_ACTIONABLE(final_publication) | run='+RUN+' | '+STAMP+' | coverage_failed,decision_failed';
      }
      return {getValues:()=>w.statusTransform(clone(values),kind,count)};
    }
  } : getSheet(name);
  w.stub = fn => {
    w.ctx.refreshDecisionTop10 = options => {
      w.calls++;assert.equal(w.lockDepth,1,'native uses existing coordinator writer lock');
      assert.equal(w.lockKind,'writer','native execution outside short transaction lock');
      assert.equal(options.completionContract,true);return fn();
    };
  };
  w.tick = () => w.ctx.tfbDecisionChainTick();
  w.state = () => JSON.parse(w.props[KEY] || '{}');
  w.chainLogs = () => w.logs.filter(x=>x.row[2] === 'tfbDecisionChainTick');
  return w;
}
function noAcknowledgement(w) {
  assert(!w.state().t10 && !w.state().pd);
  assert(!w.chainLogs().some(x=>x.row[4] === 'OK'));
  assert(!w.chainLogs().some(x=>x.row[4] === 'FAILED'));
}
function refunded(w) {
  assert.equal(w.state().claim,null);
  assert.deepEqual(w.state().attempts,{});
  noAcknowledgement(w);
}
function unverified(w,action='t10') {
  assert.equal(w.state().claim,null);
  assert.equal(w.state().attempts[keyFor(action)],1);
  assert(w.chainLogs().some(x=>x.row[4]==='UNVERIFIED'));
  noAcknowledgement(w);
}

test('delivered chain version uses its separate runtime source',()=>{
  const w=world(); assert.equal(w.ctx.TFB_DC_VERSION,'1.0.1');
  assert(source.includes('native\n *   function OUTSIDE'));
});
for (const mode of [undefined,'off','invalid']) test('default/off mode has no service writes or native work: '+mode,()=>{
  const w=world();if(mode===undefined)delete w.props.TFB_DECISION_CHAIN;else w.props.TFB_DECISION_CHAIN=mode;
  w.stub(()=>receipt());assert.equal(w.tick(),'off');assert.equal(w.calls,0);
  assert.deepEqual(w.io,{books:0,locks:0,sets:0,deletes:0,triggerWrites:0,guardCalls:0});
  assert.equal(w.logs.length,0);assert.equal(w.fetches.length,0);
});
test('real native Sheets empty/WITHHELD result completes its actually consumed cohort',()=>{
  const w=world();w.ctx.dt10ReadPanel_=()=>({'Pool Source':'Sheets','T10: Stability Enabled':'No'});
  assert.equal(w.tick(),'refreshed t10 run '+RUN);
  assert.equal(w.state().t10,RUN);assert.equal(w.state().claim,null);
  assert.deepEqual(w.state().attempts,{});assert.equal(w.fetches.length,1);
  assert.equal(w.fetches[0].body.rows.length,9694);
  assert(w.chainLogs().some(x=>x.row[4]==='OK'));
});
test('real native Backend preflight defers before POST/publication',()=>{
  const w=world();assert.match(w.tick(),/native refresh deferred before publication/);
  assert.equal(w.fetches.length,0);assert.equal(w.clears.length,0);refunded(w);
});
test('real native HELD race performs no POST and is not acknowledged',()=>{
  const w=world();w.props.DT10_SYNC_INFLIGHT='enforce';
  w.statusTransform=(rows,kind,n)=>{
    if(kind==='feeds' && n>1)rows.find(r=>r[0]==='TFB Feed Market_Leaders')[1]=
      'OK | run=90002 | 2026-10-12 12:20:00+03:00';
    return rows;
  };
  assert.match(w.tick(),/native refresh deferred/);assert.equal(w.fetches.length,0);
  assert.equal(w.clears.length,0);refunded(w);
});
test('real native cohort advance is rendered but never acknowledged for the old plan',()=>{
  const w=world();w.ctx.dt10ReadPanel_=()=>({'Pool Source':'Sheets','T10: Stability Enabled':'No'});
  w.statusTransform=(rows,kind,n)=>{
    if(kind==='grid')rows.slice(1).forEach(r=>{r[3]=r[3].replace('run='+RUN,'run=90002');});
    if(kind==='feeds' && n>1)rows.forEach(r=>{r[1]=r[1].replace('run='+RUN,'run=90002');});
    return rows;
  };
  assert.match(w.tick(),/consumed cohort changed/);assert.equal(w.fetches.length,1);unverified(w);
});
for(const page of ['Mutual_Funds','Commodities_FX']) test('real native '+page+' newer same-run generation prevents old final-witness acknowledgement',()=>{
  const w=world();w.ctx.dt10ReadPanel_=()=>({'Pool Source':'Sheets','T10: Stability Enabled':'No'});
  w.statusTransform=(rows,kind)=>{if(kind==='grid')rows.find(r=>r[0]===page)[1]='2026-10-12 12:20:00+03:00';return rows;};
  assert.match(w.tick(),/final publication witness predates/);assert.equal(w.fetches.length,1);unverified(w);
});
test('real native latest-source proof completes when final witness follows all four sources',()=>{
  const w=world();w.ctx.dt10ReadPanel_=()=>({'Pool Source':'Sheets','T10: Stability Enabled':'No'});
  w.statusTransform=(rows,kind)=>{
    if(kind==='grid')rows.find(r=>r[0]==='Mutual_Funds')[1]='2026-10-12 12:20:00+03:00';
    if(kind==='feeds')rows.find(r=>r[0]==='TFB Decision Feed')[1]=
      'NOT_ACTIONABLE(final_publication) | run='+RUN+' | 2026-10-12 12:25:00+03:00 | decision_failed';
    return rows;
  };
  assert.equal(w.tick(),'refreshed t10 run '+RUN);assert.equal(w.state().t10,RUN);
});
for (const output of ['WITHHELD','EMPTY','unknown_policy_state']) test('typed financial output does not create an extra policy gate: '+output,()=>{
  const w=world();w.stub(()=>receipt('t10',{output_state:output}));
  assert.equal(w.tick(),'refreshed t10 run '+RUN);assert.equal(w.state().t10,RUN);assert.equal(w.calls,1);
});
for (const result of ['ok','empty','HTTP 503','NETWORK ERROR',undefined,null,{},
  receipt('pd'),receipt('t10',{contract:'other'}),receipt('t10',{state:'held'}),
  receipt('t10',{source_run:''}),receipt('t10',{source_run:90001}),
  receipt('t10',{source_run:RUN+'x'}),receipt('t10',{output_state:''})]) {
  test('unsupported/unverified receipt retains bounded attempt '+JSON.stringify(result),()=>{
    const w=world();w.stub(()=>result);assert.match(w.tick(),/^unverified /);assert.equal(w.calls,1);unverified(w);
  });
}
test('PF uses opt-in API, rejects its legacy normal-return string',()=>{
  const w=world();w.ctx.pdSrcReadiness_=()=>({ready:true,cohort:RUN,stamp_ms:SOURCE_MS});
  w.ctx.refreshPortfolioDecision=opts=>{w.calls++;assert.equal(opts.completionContract,true);return 'ok';};
  assert.match(w.tick(),/completion receipt unsupported/);assert.equal(w.calls,1);unverified(w,'pd');
});
test('PF valid typed empty result acknowledges only PF',()=>{
  const w=world();w.ctx.pdSrcReadiness_=()=>({ready:true,cohort:RUN,stamp_ms:SOURCE_MS});
  w.ctx.refreshPortfolioDecision=opts=>{w.calls++;assert.equal(opts.completionContract,true);return receipt('pd',{output_state:'EMPTY'});};
  assert.equal(w.tick(),'refreshed pd run '+RUN);assert.equal(w.state().pd,RUN);assert(!w.state().t10);
});
test('owned deferred result refunds each attempt with no immediate retry',()=>{
  const w=world();w.stub(()=>receipt('t10',{state:'deferred',source_run:'',reason:'sync_inflight',output_state:'HELD'}));
  for(let i=0;i<5;i++){assert.match(w.tick(),/native refresh deferred/);assert.equal(w.calls,i+1);refunded(w);}
  w.stub(()=>receipt());assert.equal(w.tick(),'refreshed t10 run '+RUN);assert.equal(w.calls,6);
});
for(const result of [receipt('t10',{source_run:''}), 'legacy normal return',
  receipt('t10',{state:'deferred',source_run:'',reason:'future_unknown',output_state:'HELD'})]) {
  test('unverifiable publication is capped at three operations '+JSON.stringify(result),()=>{
    const w=world();w.stub(()=>result);
    for(let i=0;i<3;i++)assert.match(w.tick(),/^unverified /);
    assert.match(w.tick(),/gave up after 3 attempts/);assert.equal(w.calls,3);
    assert.equal(w.state().attempts[keyFor()],3);assert.equal(w.state().claim,null);noAcknowledgement(w);
  });
}
test('unknown PF deferred receipt cannot refund a possible rendered operation',()=>{
  const w=world();w.ctx.pdSrcReadiness_=()=>({ready:true,cohort:RUN,stamp_ms:SOURCE_MS});
  w.ctx.refreshPortfolioDecision=()=>receipt('pd',{state:'deferred',source_run:'',reason:'source_unverified'});
  assert.match(w.tick(),/native deferral not verified/);unverified(w,'pd');
});
for(const action of ['pd','t10']) test('operational throw retains exactly three '+action+' failure attempts',()=>{
  const w=world();const fail=()=>{throw Error('PD_REFRESH_FAILED synthetic sensitive raw text');};
  if(action==='pd') {
    w.ctx.pdSrcReadiness_=()=>({ready:true,cohort:RUN,stamp_ms:SOURCE_MS});
    w.ctx.refreshPortfolioDecision=opts=>{w.calls++;assert.equal(opts.completionContract,true);return fail();};
    // Once PF is exhausted, no T10 fallback is installed in this focused case.
    w.ctx.refreshDecisionTop10=undefined;
  } else w.stub(fail);
  for(let i=0;i<3;i++)assert.equal(w.tick(),'failed '+action+' run '+RUN);
  assert(!w.tick().startsWith('failed'));assert.equal(w.calls,3);
  assert.equal(w.state().attempts[keyFor(action)],3);assert.equal(w.state().claim,null);
  assert.equal(w.chainLogs().filter(x=>x.row[4]==='FAILED').length,3);
  assert(!JSON.stringify(w.logs).includes('sensitive raw text'));
});
for(const field of ['owner','act','run','at_ms','identity_ms']) test('finisher never touches replacement claim differing in '+field,()=>{
  const w=world();let replacement;
  w.stub(()=>{
    const st=w.state();st.claim[field]=field==='at_ms'?NOW+1:field==='identity_ms'?SOURCE_MS+1:field==='act'?'pd':field==='run'?'90002':'replacement-owner';
    st.attempts['pd:90002']=2;replacement=JSON.stringify(st);w.props[KEY]=replacement;return receipt();
  });
  assert.match(w.tick(),/claim ownership changed/);assert.equal(w.props[KEY],replacement);noAcknowledgement(w);
});
test('expired owner replaced during native work cannot be cleared by its old finisher',()=>{
  const w=world();let replacement;
  w.stub(()=>{w.now+=8*60000;const st=w.state();
    st.claim={owner:'new-expired-replacement',act:'t10',run:RUN,at_ms:w.now};
    st.attempts['t10:'+RUN]=2;replacement=JSON.stringify(st);w.props[KEY]=replacement;return receipt();});
  assert.match(w.tick(),/claim ownership changed/);assert.equal(w.props[KEY],replacement);noAcknowledgement(w);
});
test('new-owner state survives an old native throw too',()=>{
  const w=world();let replacement;
  w.stub(()=>{const st=w.state();st.claim.owner='replacement';replacement=JSON.stringify(st);
    w.props[KEY]=replacement;throw Error('synthetic old failure');});
  assert.match(w.tick(),/claim ownership changed/);assert.equal(w.props[KEY],replacement);noAcknowledgement(w);
});
test('planning lock busy performs no native work or state change',()=>{
  const w=world();w.lockResults=[false];w.stub(()=>receipt());
  assert.match(w.tick(),/planning lock busy/);assert.equal(w.calls,0);assert(!w.props[KEY]);noAcknowledgement(w);
});
test('missing existing writer coordinator fails closed before a claim',()=>{
  const w=world();w.ctx.tfbRunAutomaticRefresh_=undefined;w.stub(()=>receipt());
  assert.match(w.tick(),/automatic refresh coordinator unavailable/);assert.equal(w.calls,0);assert(!w.props[KEY]);
});
test('existing coordinator writer-lock skip refunds without native execution',()=>{
  const w=world();w.lockResults=[true,false,true];w.stub(()=>receipt());
  assert.match(w.tick(),/coordinator deferred/);assert.equal(w.calls,0);refunded(w);
});
for(const guardAt of [2,3]) test('existing coordinator pause race before callback refunds at guard '+guardAt,()=>{
  const w=world();let reads=0;w.ctx.tfbAutomaticRefreshAllowed_=()=>++reads<guardAt;w.stub(()=>receipt());
  assert.match(w.tick(),/coordinator deferred/);assert.equal(w.calls,0);refunded(w);
});
for(const finish of ['lock','property']) for(const outcome of ['completed','deferred']) {
  test('unverified '+finish+' finish retains bounded reservation for '+outcome,()=>{
    const w=world();w.props[KEY]=JSON.stringify({attempts:{[keyFor()]:2}});
    if(finish==='lock')w.lockResults=[true,true,false];else w.finishError=true;
    w.stub(()=>receipt('t10',{state:outcome,source_run:outcome==='completed'?RUN:'',
      reason:'sync_inflight',output_state:outcome==='deferred'?'HELD':'WITHHELD'}));
    assert.match(w.tick(),/completion commit unverified.*manual reconciliation required/);
    const uncommitted=w.props[KEY];assert.equal(w.state().attempts[keyFor()],3);
    assert(w.state().claim);noAcknowledgement(w);
    w.finishError=false;w.now+=8*60000;
    assert.match(w.tick(),/unfinalized claim expired; manual reconciliation required/);assert.equal(w.calls,1);
    assert.equal(w.props[KEY],uncommitted,'no blind expired reservation refund or fourth native operation');
  });
}
for(const finish of ['lock','property']) test('first unverified '+finish+' finish never replays any native work after expiry',()=>{
  const w=world();if(finish==='lock')w.lockResults=[true,true,false];else w.finishError=true;
  w.stub(()=>receipt());assert.match(w.tick(),/completion commit unverified/);assert.equal(w.calls,1);
  const retained=w.props[KEY];w.finishError=false;w.now+=8*60000;
  assert.match(w.tick(),/unfinalized claim expired/);assert.equal(w.calls,1);assert.equal(w.props[KEY],retained);
  // Even a clearly different source publication cannot reconcile unknown side effects.
  decisionValue(w,'NOT_ACTIONABLE(final_publication) | run='+RUN+' | 2026-10-12 12:01:00+03:00 | decision_failed');
  assert.match(w.tick(),/unfinalized claim expired/);assert.equal(w.calls,1);assert.equal(w.props[KEY],retained);
});
test('expired legacy ownerless claim blocks every new publication until reconciliation',()=>{
  const w=world();w.props[KEY]=JSON.stringify({claim:{act:'t10',run:'89999',at_ms:NOW-8*60000},attempts:{'t10:89999':1}});
  const retained=w.props[KEY];w.stub(()=>receipt());assert.match(w.tick(),/unfinalized claim expired/);
  assert.equal(w.calls,0);assert.equal(w.props[KEY],retained);
});
for(const stamp of ['2026-02-31 12:00:00+03:00','2026-10-12 12:00:00+99:00',
  '2026-10-12 12:00:00+03:00junk','2026-10-12 12:00:00+03:00 | 2026-10-12 12:00:00+03:00']) {
  test('malformed/ambiguous market clock never authorizes native work '+stamp,()=>{
    const w=world();w.stub(()=>receipt());w.statusTransform=(rows,kind)=>{
      if(kind==='feeds')rows.find(r=>r[0]==='TFB Feed Global_Markets')[1]='OK | run='+RUN+' | '+stamp;return rows;};
    assert.match(w.tick(),/^deferred /);assert.equal(w.calls,0);assert(!w.props[KEY]);
  });
}
for(const raw of ['{','[]','null','"value"','{"attempts":[]}',
  '{"observed":[]}','{"attempts":{"t10:90001":-1}}','{"attempts":{"t10:90001":"2"}}',
  '{"attempts":{"t10:90001":1.5}}','{"attempts":{"t10:90001":4}}',
  '{"attempts":{"t10:90001x":1}}','{"t10":90001}',
  '{"claim":{"act":"t10","run":"90001","at_ms":"1"}}']) {
  test('malformed nonempty persisted state defers without reset: '+raw,()=>{
    const w=world();w.props[KEY]=raw;w.stub(()=>receipt());
    assert.match(w.tick(),/persisted state invalid/);assert.equal(w.calls,0);assert.equal(w.props[KEY],raw);
    assert.equal(w.io.sets,0);noAcknowledgement({...w,state:()=>({})});
  });
}
for(const raw of ['', '{}',JSON.stringify({pd:'',t10:'',claim:null,attempts:{},observed:{}})]) {
  test('absent/empty/legacy valid state remains compatible '+raw,()=>{
    const w=world();w.props[KEY]=raw;w.stub(()=>receipt());assert.equal(w.tick(),'refreshed t10 run '+RUN);
  });
}
for(const which of ['missingML','missingGM','missingDecision','duplicateML','blankGM',
  'noRun','duplicateRun','uppercaseDuplicateRun','junkRun','noClock','unreadable']) {
  test('unknown/ambiguous feed prevents native execution: '+which,()=>{
    const w=world();w.stub(()=>receipt());w.statusTransform=(rows,kind)=>{
      if(kind!=='feeds')return rows;
      if(which==='unreadable')throw Error('synthetic status denied');
      const ml=rows.find(r=>r[0]==='TFB Feed Market_Leaders');
      const gm=rows.find(r=>r[0]==='TFB Feed Global_Markets');
      if(which==='missingML')return rows.filter(r=>r!==ml);
      if(which==='missingGM')return rows.filter(r=>r!==gm);
      if(which==='missingDecision')return rows.filter(r=>r[0]!=='TFB Decision Feed');
      if(which==='duplicateML')rows.push(clone(ml));
      if(which==='blankGM')gm[1]='';
      if(which==='noRun')gm[1]='OK | '+STAMP;
      if(which==='duplicateRun')gm[1]+=' | run='+RUN;
      if(which==='uppercaseDuplicateRun')gm[1]+=' | RUN = 90002';
      if(which==='junkRun')gm[1]=gm[1].replace('run='+RUN,'run='+RUN+'x');
      if(which==='noClock')gm[1]='OK | run='+RUN;
      return rows;
    };
    assert.match(w.tick(),/^deferred /);assert.equal(w.calls,0);assert(!w.props[KEY]);noAcknowledgement(w);
  });
}
test('missing inflight parser is unknown instead of inactive',()=>{
  const w=world();w.ctx.dt10SyncInflightCore_=undefined;w.stub(()=>receipt());
  assert.match(w.tick(),/^deferred /);assert.equal(w.calls,0);assert(!w.props[KEY]);
});
for(const state of ['RUNNING','UNKNOWN']) for(const key of ['TFB Decision Feed','TFB Feed Global_Markets']) {
  test('nonterminal '+state+' in '+key+' never plans native work',()=>{
    const w=world();w.stub(()=>receipt());w.statusTransform=(rows,kind)=>{
      if(kind==='feeds')rows.find(r=>r[0]===key)[1]=state+' | run='+RUN+' | '+STAMP;return rows;};
    assert.match(w.tick(),/^deferred /);assert.equal(w.calls,0);assert(!w.props[KEY]);
  });
}
for(const state of ['OK','PARTIAL','PARTIAL_FRESH','STALE_COV','FAILED','SKIPPED','POLICY_ERROR']) {
  test('supported terminal page state does not invent financial eligibility: '+state,()=>{
    const w=world();w.stub(()=>receipt());w.statusTransform=(rows,kind)=>{
      if(kind==='feeds')rows.find(r=>r[0]==='TFB Feed Global_Markets')[1]=state+' | run='+RUN+' | '+STAMP;return rows;};
    assert.equal(w.tick(),'refreshed t10 run '+RUN);
  });
}
function decisionValue(w,value) {
  w.statusTransform=(rows,kind)=>{if(kind==='feeds')rows.find(r=>r[0]==='TFB Decision Feed')[1]=value;return rows;};
}
const releaseValue='EXECUTABLE | run='+RUN+' | 2026-10-12 11:00:00+03:00'+
  ' | issued=2026-10-12T12:01:00+03:00 | expires=2026-10-12T19:00:00+03:00'+
  ' | ttl_min=480 | release=91000 | attempt=1 | receipts=T10:'+RUN+',PF:'+RUN+
  ' | t10_at=2026-10-12T11:00:00+03:00 | pf_at=2026-10-12T11:00:00+03:00'+
  ' | mp_at=2026-10-12T11:00:00+03:00 | audits=0,0';
for(const value of ['EXECUTABLE | run='+RUN+' | '+STAMP+' | ML:OK GM:OK',
  'NOT_ACTIONABLE(partial:GM) | run='+RUN+' | '+STAMP+' | ML:OK GM:PARTIAL',
  'NOT_ACTIONABLE(final_publication) | run='+RUN+' | '+STAMP+' | superseded_by_run='+RUN+'.2',
  'NOT_ACTIONABLE(final_publication) | run='+RUN+' | '+STAMP+' | coverage_failed | release=91000',
  'NOT_ACTIONABLE(final_publication) | run='+RUN+' | '+STAMP+' | coverage_failed,coverage_failed',
  'NOT_ACTIONABLE(final_publication) | run='+RUN+' | '+STAMP+' | made_up_reason',
  'NOT_ACTIONABLE(final_publication) | run='+RUN+' | 2026-02-31 12:00:00+03:00 | coverage_failed',
  'NOT_ACTIONABLE(final_publication) | run='+RUN+' | 2026-10-12 12:31:00+03:00 | coverage_failed',
  releaseValue.replace('audits=0,0','audits=0,1'),
  releaseValue.replace('PF:'+RUN,'PF:90002')]) {
  test('nonfinal/malformed publication marker cannot spend a native attempt: '+value,()=>{
    const w=world();w.stub(()=>receipt());decisionValue(w,value);
    assert.match(w.tick(),/^deferred /);assert.equal(w.calls,0);assert(!w.props[KEY]);
  });
}
test('old final witness for same run does not authorize a newer market attempt',()=>{
  const w=world();w.stub(()=>receipt());w.statusTransform=(rows,kind)=>{
    if(kind==='feeds')rows.filter(r=>r[0]!=='TFB Decision Feed').forEach(r=>{r[1]=r[1].replace('12:00:00','12:10:00');});return rows;};
  assert.match(w.tick(),/final publication witness unavailable or older/);assert.equal(w.calls,0);assert(!w.props[KEY]);
});
for(const value of [releaseValue,
  'NOT_ACTIONABLE(final_publication) | run='+RUN+' | '+STAMP+' | coverage_unavailable,decision_unavailable',
  'NOT_ACTIONABLE(final_publication) | run='+RUN+' | '+STAMP+' | approved_scope_unavailable,coverage_unavailable,decision_unavailable']) {
  test('exact existing terminal witness authorizes research refresh: '+value,()=>{
    const w=world();w.stub(()=>receipt());decisionValue(w,value);assert.equal(w.tick(),'refreshed t10 run '+RUN);
  });
}
test('effectful automation guard still applies to actual tick',()=>{
  const w=world();w.ctx.tfbAutomaticRefreshAllowed_=()=>{w.io.guardCalls++;return false;};w.stub(()=>receipt());
  assert.match(w.tick(),/automation paused/);assert.equal(w.io.guardCalls,1);assert.equal(w.calls,0);
});
for(const pause of ['active pause','expired pause','backend hold']) test('status has no mutation even with '+pause,()=>{
  const w=world();
  if(pause==='backend hold') {
    w.props.TFB_GAS_BACKEND_HOLD='on';
    const getSheet=w.book.getSheetByName.bind(w.book);
    w.book.getSheetByName=name=>name==='_Sync_Control'?{getRange(){return {getValues(){
      return [['backend sync hold until','2026-10-12T12:35:00+03:00']];}};}}:getSheet(name);
    w.ctx.SpreadsheetApp.getActive=()=>w.book;
  } else {
    w.props.TFB_MANUAL_REFRESH_UNTIL_MS=String(NOW+(pause==='active pause'?60000:-60000));
    w.props.TFB_MANUAL_REFRESH_REQUEST_ID='synthetic-pause-owner';
  }
  // Keep the real coordinator helpers: an accidental invocation could delete
  // the expired pause or record a manual/backend-hold event.
  const allowed=w.ctx.tfbAutomaticRefreshAllowed_;
  w.ctx.tfbAutomaticRefreshAllowed_=(...args)=>{w.io.guardCalls++;return allowed(...args);};
  w.ctx.LockService.getDocumentLock=()=>({tryLock(){return true;},releaseLock(){}});
  const before=JSON.stringify(w.props);const msg=w.ctx.tfbDecisionChainStatus();
  assert.match(msg,/automation guard not evaluated \(diagnostic\)/);assert.equal(JSON.stringify(w.props),before);
  assert.equal(w.io.guardCalls,0);assert.equal(w.io.sets,0);assert.equal(w.io.deletes,0);
  assert.equal(w.io.triggerWrites,0);assert.equal(w.logs.length,0);assert.equal(w.fetches.length,0);
});
test('status malformed state is read-only and explicit',()=>{
  const w=world();w.props[KEY]='{';assert.match(w.ctx.tfbDecisionChainStatus(),/persisted state invalid/);
  assert.equal(w.props[KEY],'{');assert.equal(w.io.sets,0);assert.equal(w.logs.length,0);
});
test('observe records once without executing or claiming a native refresh',()=>{
  const w=world();w.props.TFB_DECISION_CHAIN='observe';w.stub(()=>receipt());
  assert.equal(w.tick(),'observed t10 run '+RUN);assert.match(w.tick(),/already refreshed/);
  assert.equal(w.calls,0);assert.equal(w.state().observed.t10,RUN);assert.equal(w.state().claim,null);
  assert.equal(w.chainLogs().filter(x=>x.row[4]==='OBSERVE').length,1);
});
test('real native same-run later final witness refreshes again, exact identity skips',()=>{
  const w=world();w.ctx.dt10ReadPanel_=()=>({'Pool Source':'Sheets','T10: Stability Enabled':'No'});
  assert.equal(w.tick(),'refreshed t10 run '+RUN);assert.equal(w.state().t10_ms,SOURCE_MS);
  assert.match(w.tick(),/already refreshed/);assert.equal(w.fetches.length,1);
  w.statusTransform=(rows,kind)=>{
    if(kind==='grid')rows.slice(1).forEach(r=>{r[1]='2026-10-12 12:10:00+03:00';});
    if(kind==='feeds')rows.forEach(r=>{r[1]=r[1].replace('12:00:00',r[0]==='TFB Decision Feed'?'12:20:00':'12:10:00');});
    return rows;
  };
  assert.equal(w.tick(),'refreshed t10 run '+RUN);assert.equal(w.fetches.length,2);
  assert.equal(w.state().t10_ms,SOURCE_MS+20*60000);assert.match(w.tick(),/already refreshed/);
});
test('PF same-run later publication refreshes again and binds returned stamp',()=>{
  const w=world();let stamp=SOURCE_MS;w.ctx.refreshDecisionTop10=undefined;
  w.ctx.pdSrcReadiness_=()=>({ready:true,cohort:RUN,stamp_ms:stamp});
  w.ctx.refreshPortfolioDecision=()=>{w.calls++;return receipt('pd',{source_stamp_ms:stamp});};
  assert.equal(w.tick(),'refreshed pd run '+RUN);assert.equal(w.state().pd_ms,stamp);
  w.tick();assert.equal(w.calls,1);stamp+=60000;
  assert.equal(w.tick(),'refreshed pd run '+RUN);assert.equal(w.state().pd_ms,stamp);assert.equal(w.calls,2);
});
test('PF changed consumed publication stamp is UNVERIFIED despite same run',()=>{
  const w=world();w.ctx.pdSrcReadiness_=()=>({ready:true,cohort:RUN,stamp_ms:SOURCE_MS});
  w.ctx.refreshPortfolioDecision=()=>receipt('pd',{source_stamp_ms:SOURCE_MS+1});
  assert.match(w.tick(),/consumed portfolio publication changed/);unverified(w,'pd');
});
for(const action of ['pd','t10']) test('legacy completed same-run '+action+' without identity needs reconciliation',()=>{
  const w=world();w.props[KEY]=JSON.stringify({[action]:RUN});w.stub(()=>receipt());
  if(action==='pd') {w.ctx.pdSrcReadiness_=()=>({ready:true,cohort:RUN,stamp_ms:SOURCE_MS});
    w.ctx.refreshPortfolioDecision=()=>{w.calls++;return receipt('pd');};}
  const before=w.props[KEY];assert.match(w.tick(),/completion identity unknown; manual reconciliation required/);
  assert.equal(w.calls,0);assert.equal(w.props[KEY],before);
});
for(const action of ['pd','t10']) test('same-run backward '+action+' publication identity never replays native work',()=>{
  const w=world();w.props[KEY]=JSON.stringify({[action]:RUN,[action+'_ms']:SOURCE_MS+60000});w.stub(()=>receipt());
  if(action==='pd') {w.ctx.pdSrcReadiness_=()=>({ready:true,cohort:RUN,stamp_ms:SOURCE_MS});
    w.ctx.refreshPortfolioDecision=()=>{w.calls++;return receipt('pd');};}
  const before=w.props[KEY];assert.match(w.tick(),/identity moved backward; manual reconciliation required/);
  assert.equal(w.calls,0);assert.equal(w.props[KEY],before);
});
test('legacy completed different run can proceed without erasing its meaning',()=>{
  const w=world();w.props[KEY]=JSON.stringify({t10:'89999'});w.stub(()=>receipt());
  assert.equal(w.tick(),'refreshed t10 run '+RUN);assert.equal(w.state().t10_ms,SOURCE_MS);
});
for(const legacy of [1,3]) test('legacy unknown-epoch budget remains conservative floor '+legacy,()=>{
  const w=world();w.props[KEY]=JSON.stringify({attempts:{['t10:'+RUN]:legacy}});
  w.stub(()=>receipt('t10',{source_run:''}));
  for(let i=0;i<3-legacy;i++)assert.match(w.tick(),/^unverified /);
  assert.match(w.tick(),/gave up after 3 attempts/);assert.equal(w.calls,3-legacy);
  assert.equal(w.state().attempts['t10:'+RUN],legacy);
});
test('known old-identity failure cap does not impersonate a new same-run publication',()=>{
  const w=world();w.stub(()=>{throw Error('synthetic operational failure');});
  for(let i=0;i<3;i++)assert.match(w.tick(),/^failed /);
  assert.match(w.tick(),/gave up after 3 attempts/);
  decisionValue(w,'NOT_ACTIONABLE(final_publication) | run='+RUN+' | 2026-10-12 12:01:00+03:00 | decision_failed');
  w.stub(()=>receipt());assert.equal(w.tick(),'refreshed t10 run '+RUN);
  assert.equal(w.state().attempts[keyFor()],3);assert.equal(w.state().t10_ms,SOURCE_MS+60000);
});
test('observe tracks publication identity for a later same-run witness',()=>{
  const w=world();w.props.TFB_DECISION_CHAIN='observe';w.stub(()=>receipt());
  assert.equal(w.tick(),'observed t10 run '+RUN);assert.match(w.tick(),/already refreshed/);
  decisionValue(w,'NOT_ACTIONABLE(final_publication) | run='+RUN+' | 2026-10-12 12:01:00+03:00 | decision_failed');
  assert.equal(w.tick(),'observed t10 run '+RUN);assert.equal(w.state().observed.t10_ms,SOURCE_MS+60000);
  assert.equal(w.calls,0);assert.equal(w.chainLogs().filter(x=>x.row[4]==='OBSERVE').length,2);
});
test('legacy observed same-run unknown identity is not silently replayed',()=>{
  const w=world();w.props.TFB_DECISION_CHAIN='observe';w.props[KEY]=JSON.stringify({observed:{t10:RUN}});w.stub(()=>receipt());
  const before=w.props[KEY];assert.match(w.tick(),/identity unknown/);assert.equal(w.props[KEY],before);assert.equal(w.calls,0);
});
for(const count of [96,97]) test('bounded attempt-key growth fails closed at '+count+' existing keys',()=>{
  const w=world(),attempts={};for(let i=0;i<count;i++)attempts['t10:'+(10000+i)+':'+SOURCE_MS]=1;
  w.props[KEY]=JSON.stringify({attempts});const before=w.props[KEY];w.stub(()=>receipt());
  assert.match(w.tick(),count===96?/state bound reached/:/persisted state invalid/);
  assert.equal(w.calls,0);assert.equal(w.props[KEY],before);assert.equal(w.io.sets,0);
});
test('UTF8 state limit is byte accurate at the exact boundary',()=>{
  const w=world();assert.equal(w.ctx.tfbDcTextFits_('a'.repeat(8000)),true);
  assert.equal(w.ctx.tfbDcTextFits_('a'.repeat(8001)),false);
  assert.equal(w.ctx.tfbDcTextFits_('\u{1f30d}'.repeat(2000)),true);
  assert.equal(w.ctx.tfbDcTextFits_('\u{1f30d}'.repeat(2001)),false);
  w.props[KEY]='{"unknown":"'+'a'.repeat(8000)+'"}';const before=w.props[KEY];w.stub(()=>receipt());
  assert.match(w.tick(),/persisted state invalid/);assert.equal(w.calls,0);assert.equal(w.props[KEY],before);
});
for(const raw of ['   ','{"unknown":1}','{"pd_ms":1}','{"t10":"90001","t10_ms":0}',
  '{"observed":{"t10":"90001","t10_ms":"1"}}',
  '{"attempts":{"t10:90001:9999999999999999":1}}']) {
  test('unsupported/nonempty invalid identity state never resets '+raw,()=>{
    const w=world();w.props[KEY]=raw;w.stub(()=>receipt());assert.match(w.tick(),/persisted state invalid/);
    assert.equal(w.calls,0);assert.equal(w.props[KEY],raw);assert.equal(w.io.sets,0);
  });
}
console.log('PASS '+passed+'/'+passed+' decision-chain completion checks');
