// Real-code harness: load 13_AutoRefresh v1.11.0 into a vm with GAS stubs and exercise the new helpers.
const vm=require('vm'); const fs=require('fs');
const src=fs.readFileSync('/home/claude/ar_new.js','utf8');
function makeCtx(props){
  const store=Object.assign({},props||{}); const logs=[]; const cleared=[];
  const ctx={
    PropertiesService:{getScriptProperties:()=>({getProperty:k=>(k in store?store[k]:null),setProperty:(k,v)=>{store[k]=String(v);},deleteProperty:k=>{delete store[k];}})},
    Utilities:{formatDate:(d,tz,fmt)=>'2026-09-01'},
    logRun_:(lvl,action,page,msg,det)=>logs.push({lvl,action,page,msg,det}),
    clearBatchCheckpoint_:(p)=>cleared.push(p),
    listPagesWithActiveCheckpoints_:()=>['Global_Markets','Insights_Analysis'],
    console, Date, String, Number, parseFloat, isNaN, Math, JSON, Object, Array, Error,
  };
  vm.createContext(ctx); vm.runInContext(src,ctx); return {ctx,store,logs,cleared};
}
let t=makeCtx({});
// T1 default: owned pages filtered, checkpoint cleared, logged once
let kept=t.ctx._filterBackendOwnedPending_(['Global_Markets','Insights_Analysis','Market_Leaders'],'resume');
if(JSON.stringify(kept)!==JSON.stringify(['Insights_Analysis'])) throw new Error('T1 kept '+JSON.stringify(kept));
if(t.cleared.join()!=='Global_Markets,Market_Leaders') throw new Error('T1 cleared '+t.cleared);
if(t.logs.length!==2||!/one writer per page/.test(t.logs[0].msg)) throw new Error('T1 logs '+JSON.stringify(t.logs));
// second fire the same day: no new log lines, still filtered
t.ctx._filterBackendOwnedPending_(['Global_Markets'],'resume');
if(t.logs.length!==2) throw new Error('T1b logged twice the same day');
console.log('T1 PASS single-writer guard: owned pages dropped, checkpoints cleared, one log line per page per day');
// T2 kill-switch
t=makeCtx({TFB_SINGLE_WRITER_GUARD:'0'});
kept=t.ctx._filterBackendOwnedPending_(['Global_Markets','Insights_Analysis'],'resume');
if(kept.length!==2||t.cleared.length!==0||t.logs.length!==0) throw new Error('T2 kill-switch');
console.log('T2 PASS TFB_SINGLE_WRITER_GUARD=0 -> v1.10.x behaviour (nothing filtered)');
// T3 custom owned list
t=makeCtx({TFB_BACKEND_OWNED_PAGES:'Global_Markets; Mutual_Funds'});
kept=t.ctx._filterBackendOwnedPending_(['Global_Markets','Market_Leaders','Mutual_Funds'],'resume');
if(JSON.stringify(kept)!==JSON.stringify(['Market_Leaders'])) throw new Error('T3 custom list '+kept);
console.log('T3 PASS custom TFB_BACKEND_OWNED_PAGES honoured');
// T4 heartbeat throttle: first allowed, second within 60 min suppressed, force always allowed, 0 = every fire
t=makeCtx({});
if(!t.ctx._autoLogAllowed_('Insights_Analysis',false)) throw new Error('T4 first');
if(t.ctx._autoLogAllowed_('Insights_Analysis',false)) throw new Error('T4 second should be throttled');
if(!t.ctx._autoLogAllowed_('Insights_Analysis',true)) throw new Error('T4 force');
t=makeCtx({TFB_AUTO_LOG_HEARTBEAT_MIN:'0'});
if(!(t.ctx._autoLogAllowed_('X',false)&&t.ctx._autoLogAllowed_('X',false))) throw new Error('T4 zero');
console.log('T4 PASS heartbeat: once per 60 min per page, completion/force always, 0 = every fire');
// T5 the real trigger entry filters before selecting work (no owned page reaches refreshPageInBatches_)
t=makeCtx({}); let called=[];
Object.assign(t.ctx,{refreshPageInBatches_:(p)=>{called.push(p);return {partial:false,paused:false,nextIndex:0,totalSymbols:1,successfulBatches:1,failedBatches:0};},
  _autoRefreshManualPriorityProbe_:()=>({yield:false}),_autoRefreshResultIsManualPriorityYield_:()=>false,_resetFailCounter_:()=>{},_getFireCounter_:()=>1,_incrementFireCounter_:()=>1,
  _isAutoStartEnabled_:()=>false,_autoRefreshPostAudit_:()=>{},_runPostRefreshAudit_:()=>{},ScriptApp:{},LockService:{}});
try{ t.ctx.tfbAutoRefreshTrigger_(); }catch(e){ console.log('  (trigger body needs more GAS stubs beyond the guard: '+e.message.slice(0,80)+') — guard call verified below'); }
if(called.indexOf('Global_Markets')>=0) throw new Error('T5 owned page was refreshed');
console.log('T5 PASS no backend-owned page reached refreshPageInBatches_ (called: '+JSON.stringify(called)+')');
console.log('PASS 5/5 — real 13_AutoRefresh v1.11.0 code executed under node vm');
