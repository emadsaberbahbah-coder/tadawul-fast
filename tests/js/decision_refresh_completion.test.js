/* Real native refresh + morning wrapper + unchanged pending PR #788 caller.
 * Only external Apps Script/HTTP inputs and the panel read are fixtures.
 * No financial, source-readiness, stability, finalization or render function
 * is replaced. The PR caller is an immutable source fixture, not runtime code.
 */
'use strict';
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const ROOT = path.join(__dirname, '../..');
const source = fs.readFileSync(process.env.DT10_DELIVERED ||
  path.join(ROOT, 'apps_script/16_Decision_Top10.gs'), 'utf8');
const chainSource = fs.readFileSync(path.join(ROOT,
  'tests/fixtures/decision_chain_pr788_v1_0_0.gs'), 'utf8');
const CHAIN_SHA = '8e00a48d30752c14f088ad7625642e7d4ccdeb36da1b3acab6c9452bee580663';
const NOW = Date.parse('2026-10-12T09:30:00Z');
const STAMP = '2026-10-12 12:00:00+03:00';
const RUN = '90001';
const COUNTS = {Market_Leaders:255, Global_Markets:6512,
  Commodities_FX:453, Mutual_Funds:2474};
const clone = value => JSON.parse(JSON.stringify(value));
let passed = 0;
function test(name, fn) {fn(); passed++; console.log('PASS ' + name);}

class Range {
  constructor(world, sheet, row, col, rows=1, cols=1) {
    Object.assign(this, {world,sheet,row,col,rows,cols});
    return new Proxy(this, {get(target, key, receiver) {
      if (key in target) {
        const value = target[key];
        return typeof value === 'function' ? value.bind(receiver) : value;
      }
      // Apps Script styling has no effect on receipt/control behavior.
      return () => receiver;
    }});
  }
  getValue() {return this.sheet.cells.get(this.row + ':' + this.col) || '';}
  getValues() {
    return Array.from({length:this.rows}, (_,r) => Array.from({length:this.cols}, (_,c) =>
      this.sheet.cells.get((this.row+r) + ':' + (this.col+c)) || ''));
  }
  getDisplayValues() {return this.getValues().map(row=>row.map(String));}
  setValue(value) {return this.setValues([[value]]);}
  setValues(values) {
    this.world.writes.push({page:this.sheet.name,row:this.row,col:this.col,values:clone(values)});
    values.forEach((row,r)=>row.forEach((value,c)=>
      this.sheet.cells.set((this.row+r) + ':' + (this.col+c), value)));
    return this;
  }
  clear() {this.world.clears.push({page:this.sheet.name,row:this.row});return this;}
  breakApart() {return this;}
  merge() {return this;}
}

class Sheet {
  constructor(world,name) {this.world=world;this.name=name;this.cells=new Map();}
  getRange(row,col,rows=1,cols=1) {return new Range(this.world,this,row,col,rows,cols);}
  getParent() {return this.world.book;}
  getMaxRows() {return 100;}
  getMaxColumns() {return 118;}
  getLastRow() {return 100;}
  getName() {return this.name;}
  appendRow(row) {this.world.logs.push({page:this.name,row:clone(row)});}
}

function emptyPayload() {
  return {version:'synthetic-builder',status:'no_candidates',selected:[],
    candidates_rows:[],near_miss:[],alerts:[],kpis:{scanned:0,passed:0},
    meta:{route:{request_id:'synthetic-request'},board_funding:{contract_version:1,
      stage:'research',snapshot_available:false,reason:'honest empty result'}}};
}

function load(response='network', feed='NOT_ACTIONABLE') {
  const world = {response,fetches:[],writes:[],clears:[],logs:[],statuses:[],
    props:{TFB_DECISION_CHAIN:'on',TFB_DECISION_CHAIN_QUIET:'none',
      DT10_SELECTION_LOG:'off',DT10_LAST_SUCCESS:'2026-10-12 11:00|1'},
    payload:emptyPayload(),sourceRows:{}};
  if (response === 'empty-object') world.payload = {};
  if (response === 'missing-status') delete world.payload.status;
  if (response === 'missing-kpis') delete world.payload.kpis;
  if (response === 'selected-not-array') world.payload.selected = {};
  if (response === 'null-selected-row') world.payload.selected = [null];
  if (response === 'invalid-meta') world.payload.meta = 'not an object';
  const sheets = {};
  const grid = [['Page','Last Updated','Status','Message','Endpoint','HTTP Code',
    'Rows','Columns','Duration ms','Warnings'], ...Object.entries(COUNTS).map(([page,count])=>
    [page,STAMP,'SUCCESS',`acquired=${count}/${count} acquisition=COMPLETE fetchfail_effective=enforce | data=COMPLETE run=${RUN}`,
      'synthetic',200,count,115,1,0])];
  const feedRows = [['TFB Decision Feed',`${feed} | run=${RUN} | ${STAMP}`],
    ['TFB Feed Global_Markets',`OK | run=${RUN} | ${STAMP}`],
    ['TFB Feed Market_Leaders',`OK | run=${RUN} | ${STAMP}`]];
  const statusSheet = {getLastRow(){return grid.length;},getLastColumn(){return 13;},
    getRange(row){return {getValues(){return clone(typeof row === 'string' ? feedRows : grid);}};}};
  for (const [page,count] of Object.entries(COUNTS)) {
    world.sourceRows[page] = [['Name','Symbol'], ...Array.from({length:count},(_,i)=>
      ['Synthetic',page.toUpperCase() + '_' + i])];
  }
  world.book = {getSheetByName(name) {
    if (name === '_Status') return statusSheet;
    if (world.sourceRows[name]) {
      const values = world.sourceRows[name];
      return {getLastRow(){return values.length;},getLastColumn(){return 2;},
        getRange(row,col,rows,cols){return {getValues(){return values.slice(row-1,row-1+rows)
          .map(r=>r.slice(col-1,col-1+cols));}};}};
    }
    return sheets[name] || null;
  },getRangeByName(){return null;}};
  sheets.Top_10_Investments = new Sheet(world,'Top_10_Investments');
  sheets._Run_Log = new Sheet(world,'_Run_Log');
  world.board = sheets.Top_10_Investments;
  class FixedDate extends Date {
    constructor(...args) {super(...(args.length ? args : [NOW]));}
    static now() {return NOW;}
  }
  const props = {getProperty(key){return world.props[key] || null;},
    setProperty(key,value){world.props[key]=String(value);},
    deleteProperty(key){delete world.props[key];}};
  const ctx = {Date:FixedDate,Logger:{log(){}},
    PropertiesService:{getScriptProperties(){return props;},getDocumentProperties(){return props;}},
    Session:{getScriptTimeZone(){return 'Asia/Riyadh';}},
    SpreadsheetApp:{getActiveSpreadsheet(){return world.book;},flush(){},BorderStyle:{SOLID:'SOLID'}},
    Utilities:{DigestAlgorithm:{SHA_256:'SHA_256'},Charset:{UTF_8:'UTF_8'},
      computeDigest(_algorithm,value){return [...crypto.createHash('sha256').update(value).digest()]
        .map(n=>n>127?n-256:n);},
      formatDate(date,_zone,pattern){const local=new Date(date.getTime()+3*3600000).toISOString();
        return pattern === 'yyyy-MM-dd' ? local.slice(0,10) : local.replace('T',' ').slice(0,19);}},
    LockService:{getScriptLock(){return {tryLock(){return true;},releaseLock(){}};}},
    UrlFetchApp:{fetch(url,options) {
      world.fetches.push({url,body:JSON.parse(options.payload)});
      if (world.response === 'network') throw new Error('synthetic transport unavailable');
      const code = world.response === 'http502' ? 502 : 200;
      const payload = world.response === 'degraded' ? {status:'degraded',message:'builder_timeout'} : world.payload;
      return {getResponseCode(){return code;},getContentText(){
        return world.response === 'invalid-json' ? '<html>synthetic unavailable</html>' : JSON.stringify(payload);
      }};
    }},
    writePageStatus_(page,receipt){world.statuses.push({page,...clone(receipt)});}};
  vm.createContext(ctx);vm.runInContext(source,ctx,{filename:'16_Decision_Top10.gs'});
  vm.runInContext(chainSource,ctx,{filename:'PR788_26_Decision_Chain.gs'});
  for (const page of ['Market_Leaders','Mutual_Funds']) {
    ctx.DT10_SOURCE_APPROVED_SCOPE.pages[page].symbol_set_sha256 = crypto.createHash('sha256')
      .update(world.sourceRows[page].slice(1).map(row=>row[1]).sort().join('\n')+'\n').digest('hex');
  }
  world.board.cells.set(ctx.DT10_ROW_PANEL_HEAD+':1','CONTROL PANEL');
  world.board.cells.set(ctx.DT10_ROW_KPI_HEAD+':1','KPIs');
  world.board.cells.set(ctx.DT10_ZONES_START+':1','PREVIOUS VERIFIED BOARD');
  ctx.dt10ReadPanel_ = () => ({'Pool Source':'Backend','T10: Stability Enabled':'No'});
  // _Status is a synthetic external observation. The real chain planner,
  // claim persistence, native refresh, HTTP and completion caller are used.
  ctx.tfbDcReadInputs_ = (_ss,mode,state,nowMs)=>({mode,state,nowMs,pd:{available:false},
    t10:{available:true,gmRun:RUN,decisionRun:RUN},inflight:false,
    cockpitRunning:false,quiet:false,automationAllowed:true});
  world.ctx=ctx;return world;
}

function noPublication(world) {
  assert.equal(world.clears.length,0,'old dynamic board must not be cleared');
  assert.equal(world.board.cells.get(world.ctx.DT10_ZONES_START+':1'),'PREVIOUS VERIFIED BOARD');
  assert(!world.props.DT10_RUN_MARKER,'failed run marker must be cleared');
  assert(!world.logs.some(item=>item.row[2] !== 'tfbMorningCockpitRefresh' &&
    item.row[2] !== 'tfbDecisionChainTick'),'no selection/exit publication');
}

test('pending caller fixture is exactly PR788 commit64c7ec9b source',()=> {
  assert.equal(crypto.createHash('sha256').update(chainSource).digest('hex'),CHAIN_SHA);
});
for (const response of ['network','http502','degraded','invalid-json',
  'empty-object','missing-status','missing-kpis','selected-not-array',
  'null-selected-row','invalid-meta']) {
  test(response+' direct refresh throws after preserved-board failure receipt',()=> {
    const w=load(response);
    assert.throws(()=>w.ctx.refreshDecisionTop10(),/DT10_REFRESH_FAILED:/);
    noPublication(w);assert.equal(w.fetches.length,1,'no new provider retry');
    assert(['ERROR','WARN'].includes(w.statuses.at(-1).status));
  });
  test(response+' morning wrapper reports error rather than OK',()=> {
    const w=load(response);w.ctx.tfbMorningCockpitRefresh();noPublication(w);
    assert.equal(w.logs.at(-1).row[4],'ERROR');
    assert(!w.logs.some(item=>item.row[4]==='OK'));assert.equal(w.fetches.length,1);
  });
  test(response+' unchanged pending chain refuses acknowledgement and can retry',()=> {
    const w=load(response);assert.equal(w.ctx.tfbDecisionChainTick(),'failed t10 run '+RUN);
    noPublication(w);assert.equal(w.logs.at(-1).row[4],'FAILED');
    const state=JSON.parse(w.props.TFB_DECISION_CHAIN_STATE);
    assert.equal(state.t10,'');assert.equal(state.attempts['t10:'+RUN],1);
    w.response='valid';w.payload=emptyPayload();
    assert.equal(w.ctx.tfbDecisionChainTick(),'refreshed t10 run '+RUN,
      JSON.stringify(w.logs.at(-1)));
    assert.equal(w.fetches.length,2,'only a later caller tick retries');
    assert.equal(JSON.parse(w.props.TFB_DECISION_CHAIN_STATE).t10,RUN);
  });
}
for (const [feed,output] of [['NOT_ACTIONABLE','EMPTY'],['EXECUTABLE','EMPTY']]) {
  test('legitimate empty render on '+feed+' feed is still completed successfully',()=> {
    const w=load('valid',feed);assert.doesNotThrow(()=>w.ctx.refreshDecisionTop10());
    assert(w.clears.length>0);assert.equal(w.statuses.at(-1).status,'OK');
    assert(w.statuses.at(-1).message.includes('output: '+output));
    assert.equal(w.fetches.length,1);assert(!w.props.DT10_RUN_MARKER);
  });
}
test('withheld nonempty research board still renders without executable allocation',()=> {
  const w=load('valid');w.payload.selected=[{symbol:'SYNTH.US',name:'Synthetic',rank:1,
    price_sar:10,suggested_shares:5,suggested_sar:50,entry_zone:'9-10',stop_sar:8,
    tp1_sar:12,tp2_sar:13,advisor_note:'Synthetic research'}];
  assert.doesNotThrow(()=>w.ctx.refreshDecisionTop10());
  assert(w.statuses.at(-1).message.includes('output: WITHHELD'));
  const selectedWrite=w.writes.find(item=>item.values.some(row=>row[1]==='SYNTH.US'));
  assert(selectedWrite,'research identity still published');
  const row=selectedWrite.values.find(row=>row[1]==='SYNTH.US');
  assert.equal(row[11],'\u2014');assert.equal(row[12],'\u2014');
  assert.equal(w.fetches.length,1);
});
test('future nonfailure status with valid containers remains a successful result',()=> {
  const w=load('valid');w.payload.status='future_policy_status';
  assert.doesNotThrow(()=>w.ctx.refreshDecisionTop10());
  assert(w.clears.length>0);assert.equal(w.statuses.at(-1).status,'OK');
  assert.equal(w.fetches.length,1);
});
test('each table requires objects while valid empty containers remain supported',()=> {
  const w=load('valid');assert.equal(w.ctx.dt10ResponseProblem_(emptyPayload()),'');
  for (const field of ['selected','candidates_rows','near_miss','alerts']) {
    for (const invalid of [null,{},'not an array',[null],[false],['not a row']]) {
      const payload=emptyPayload();payload[field]=invalid;
      assert(w.ctx.dt10ResponseProblem_(payload).startsWith('malformed '+field));
    }
  }
});
test('blank selection identity is refused before any previous-board clear',()=> {
  const w=load('valid');w.payload.selected=[{}];
  assert.throws(()=>w.ctx.refreshDecisionTop10(),/DT10_REFRESH_FAILED:/);
  noPublication(w);
});
for (const [table,item] of [
  ['selected',{symbol:'SYNTH.US',price_sar:{unexpected:'object'}}],
  ['candidates_rows',{symbol:'SYNTH.US',reliability:{unexpected:'object'}}],
  ['near_miss',{symbol:'SYNTH.US',current:{unexpected:'object'}}],
  ['qualified',{symbol:'SYNTH.US',engine_roi_pct:{unexpected:'object'}}],
  ['alerts',{type:'synthetic',required_action:{unexpected:'object'}}],
]) {
  test(table+' projected objects cannot reach Sheets after clearing',()=> {
    const w=load('valid');w.payload[table]=[item];
    assert.throws(()=>w.ctx.refreshDecisionTop10(),/DT10_REFRESH_FAILED:/);
    noPublication(w);
  });
}
test('projected unknown financial cells remain blank and valid',()=> {
  const w=load('valid');w.payload.candidates_rows=[{symbol:'SYNTH.US',
    price_sar:null,engine_roi_pct:null,reliability:null,verdict:'UNKNOWN'}];
  w.payload.qualified=[];
  assert.equal(w.ctx.dt10ResponseProblem_(w.payload),'');
  assert.doesNotThrow(()=>w.ctx.refreshDecisionTop10());
  assert(w.clears.length>0);
});
console.log('PASS '+passed+'/'+passed);
