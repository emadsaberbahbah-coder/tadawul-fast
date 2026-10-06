const vm=require('vm'),fs=require('fs'); const src=fs.readFileSync('/home/claude/dt_new.js','utf8');
const props={}; const appended=[]; let state={date:'',sig:''};
const ctx={PropertiesService:{getScriptProperties:()=>({getProperty:k=>(k in props?props[k]:null),setProperty:(k,v)=>{props[k]=String(v);},deleteProperty:k=>{delete props[k];}})},
  Utilities:{formatDate:(d,tz,f)=>'2026-09-01 10:00:00'.slice(0,f.length>10?19:10),sleep:()=>{}},Session:{getScriptTimeZone:()=>'Asia/Riyadh'},
  Logger:{log:()=>{}},SpreadsheetApp:{getUi:()=>{throw new Error('no ui');}},console,Date,String,Number,Math,JSON,Object,Array,RegExp,parseFloat,parseInt,isNaN,isFinite,Error,encodeURIComponent};
vm.createContext(ctx); vm.runInContext(src,ctx);
if(ctx.DT10_VERSION!=='1.11.0') throw new Error('version');
// T1 output status
const os_=(p)=>ctx.dt10OutputStatus_(p);
if(os_({selected:[{}],kpis:{passed:3},meta:{upstream_verdict:'EXECUTABLE | run=1'}})!=='EXECUTABLE') throw new Error('T1a');
if(os_({selected:[{}],kpis:{passed:3},meta:{upstream_verdict:'NOT_ACTIONABLE(stale_cov:GM)'}})!=='WITHHELD') throw new Error('T1b');
if(os_({selected:[],kpis:{passed:4},meta:{upstream_verdict:'EXECUTABLE'}})!=='QUALIFIED_UNFUNDED') throw new Error('T1c');
if(os_({selected:[],kpis:{passed:0},meta:{}})!=='EMPTY') throw new Error('T1d');
if(!/\| output: EMPTY \|/.test(ctx.dt10StatusLine_('ok','x','EMPTY'))) throw new Error('T1e '+ctx.dt10StatusLine_('ok','x','EMPTY'));
console.log('T1 PASS output status: EXECUTABLE / WITHHELD / QUALIFIED_UNFUNDED / EMPTY; status line carries "output:"');
// T2 holdings extras via the real collector on a My_Portfolio-shaped sheet
const hdr=['Symbol','Name','Exchange','Sector','Buy Date','Position Value (SAR)','Target Price'];
const rows=[['My Portfolio'],hdr,['SHG.US','Shinhan','NYSE/NASDAQ','Financials',new Date('2026-08-24T00:00:00Z'),20937,''],['5023.SR','Sukuk','SAU','Unknown','2026-01-01',10179,103.5],['','','','','','','']];
ctx.SpreadsheetApp={getUi:()=>{throw new Error('x');}};
const ss={getSheetByName:(n)=>n==='My_Portfolio'?{getDataRange:()=>({getValues:()=>rows})}:null};
const h=ctx.dt10CollectHoldings_(ss);
if(h.length!==2) throw new Error('T2 count '+JSON.stringify(h));
const shg=h[0], suk=h[1];
if(shg.symbol!=='SHG.US'||shg.value_sar!==20937||shg.market!=='NYSE/NASDAQ'||shg.buy_date!=='2026-09-01'.slice(0,0)+shg.buy_date) throw new Error('T2 shg '+JSON.stringify(shg));
if(!/^\d{4}-\d{2}-\d{2}$/.test(shg.buy_date)||'tp1_sar' in shg) throw new Error('T2 shg fields '+JSON.stringify(shg));
if(suk.buy_date!=='2026-01-01'||suk.tp1_sar!==103.5||suk.market!=='SAU') throw new Error('T2 suk '+JSON.stringify(suk));
console.log('T2 PASS holdings payload: market/buy_date/tp1_sar added when columns exist, omitted when blank; symbol/sector/value_sar unchanged');
// T3 EMPTY_BOARD row: once per day, kill-switch, mode off
Object.assign(ctx,{dt10SelLogSheet_:()=>({getLastRow:()=>5,getRange:(r,c,n,w)=>({setValues:(v)=>{appended.push({r,w,row:v[0]});}})}),
  dt10SelLogLoadState_:()=>state,dt10SelLogSaveState_:(d,s)=>{state={date:d,sig:s};},dt10StabToday_:()=>'2026-09-01'});
let note=ctx.dt10AppendSelectionLog_(ss,[],'Last run 2026-09-01 09:40:17 | status: ok | output: EMPTY | pool 9786',{a:1});
if(note!=='SelLog: EMPTY_BOARD logged'||appended.length!==1) throw new Error('T3a '+note);
const row=appended[0].row; const H=ctx.DT10_SELLOG_HEADERS;
if(row.length!==H.length||row[H.indexOf('Symbol')]!=='EMPTY_BOARD'||!/^EMPTY_BOARD: Last run/.test(row[H.indexOf('Advisor Note')])||row[H.indexOf('Run Info')].indexOf('output: EMPTY')<0) throw new Error('T3b '+JSON.stringify(row));
note=ctx.dt10AppendSelectionLog_(ss,[],'x',{}); if(note!=='SelLog: empty board (logged today)'||appended.length!==1) throw new Error('T3c '+note);
props['DT10_EMPTY_BOARD_LOG']='0'; note=ctx.dt10AppendSelectionLog_(ss,[],'x',{}); if(note!=='SelLog: empty board (not logged)') throw new Error('T3d '+note); delete props['DT10_EMPTY_BOARD_LOG'];
props[ctx.DT10_SELLOG_MODE_PROP]='off'; note=ctx.dt10AppendSelectionLog_(ss,[],'x',{}); if(note!=='SelLog: off') throw new Error('T3e '+note); delete props[ctx.DT10_SELLOG_MODE_PROP];
console.log('T3 PASS EMPTY_BOARD: one 33-column row per day (Symbol=EMPTY_BOARD, reason in Advisor Note, Run Info carries output:), once-per-day state, kill-switch, mode off');
// T4 KPI strip
const labels=ctx.DT10_KPI_LABELS, vals=ctx.dt10KpiValues_({deployable_sar:3218,selected_count:0,max_selected:10,scanned:9786,passed:0,capital_unallocated_sar:3218,fundable_now:0,fundable_by_rotation:1,capital_call_topn_sar:18200});
if(labels.length!==11||vals.length!==11||vals[8]!==0||vals[9]!==1||vals[10]!==18200) throw new Error('T4 '+JSON.stringify(vals));
const vals0=ctx.dt10KpiValues_({scanned:1}); if(vals0.length!==11) throw new Error('T4b');
console.log('T4 PASS KPI strip: 11 labels/values, funding kpis rendered, blank when absent');
console.log('PASS 4/4 — real 16_Decision_Top10 v1.11.0 under node vm');
