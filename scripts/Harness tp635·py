import sys, os, importlib.util, pickle, threading, json
from unittest.mock import MagicMock
sys.path.insert(0,"/home/claude/repo/tadawul-fast-main")
def load(path,name):
    spec=importlib.util.spec_from_file_location(name,path); m=importlib.util.module_from_spec(spec); sys.modules[name]=m
    try: spec.loader.exec_module(m)
    except ModuleNotFoundError as e:
        missing=str(e).split("'")[1]; mm=MagicMock(); mm.__name__=missing; sys.modules[missing]=mm; return load(path,name)
    return m
for k in ("TRACK_PUBLISH_CALIBRATION","TRACK_DEDUP_KEYS","TRACK_MAX_RECORDS"): os.environ.pop(k,None)
M=load("/home/claude/tp636.py","tp636"); assert M.SCRIPT_VERSION=="6.36.0"
# --- REAL calibrator on the REAL log (loaded through the v6.35.0 store path) ---
hdr,rows=pickle.load(open("/home/claude/perf_log.pkl","rb"))
DATA=[[("" if v is None else v) for v in r] for r in rows[4:] if r and r[0] not in (None,"")]
class WS:
    def __init__(s,data,row_count): s.data=data; s.row_count=row_count; s.updates=[]; s.cleared=[]
    def get(s,rng):
        import re; m=re.match(r"[A-Z]+(\d+):[A-Z]+(\d+)",rng); r0,r1=int(m.group(1)),int(m.group(2)); return [list(r) for r in s.data[r0-6:r1-6+1]]
    def batch_clear(s,r): s.cleared+=r
    def update(s,values=None,range_name=None,value_input_option=None): s.updates.append((range_name,values))
class Backoff:
    def execute_sync(s,fn): return fn()
st=M.PerformanceStore.__new__(M.PerformanceStore); st.ws=WS(DATA,38045); st.backoff=Backoff(); st.cache={}; st.cache_lock=threading.Lock()
recs=st.load_records(200000)
cal=M.ReliabilityCalibrator(min_sample=10, shrink_k=20).calibrate(recs, signal_mix={})
env_s,note=M._calibration_env_string(cal)
print("real log calibration -> env:",env_s or "NONE","| samples:",note,"| brier:",cal.brier_score)
assert all(":" in p and float(p.split(":")[1])>0 for p in env_s.split(",") if p)
# --- publisher on a recorder Spreadsheet: _Status upsert, _S1_Calibration block, _Run_Log change line ---
class SheetWS:
    def __init__(s,grid=None): s.grid=grid or []; s.updates=[]; s.appended=[]
    def get(s,rng): return s.grid
    def update(s,values=None,range_name=None,value_input_option=None): s.updates.append((range_name,values,value_input_option))
    def append_row(s,row,value_input_option=None): s.appended.append(row)
class Sheet:
    def __init__(s,status_grid): s.tabs={"_Status":SheetWS(status_grid),"_S1_Calibration":SheetWS(),"_Run_Log":SheetWS()}
    def worksheet(s,name): return s.tabs[name]
class Store: pass
class Runner: pass
def run(status_grid, report):
    r=M.PerformanceRunner.__new__(M.PerformanceRunner) if hasattr(M,"PerformanceRunner") else None
    cls=[c for c in vars(M).values() if isinstance(c,type) and hasattr(c,"_publish_reliability_calibration")][0]
    obj=cls.__new__(cls); obj.store=Store(); obj.store.sheet=Sheet(status_grid); return obj, obj._publish_reliability_calibration(report)
# T1 first publication: key absent -> first blank L-slot, block rows A12:D18, change line (none -> env)
obj,out=run([["Global Key","Value"],["Backend URL","https://x"],[],[]], cal)
st_ws=obj.store.sheet.tabs["_Status"]; assert out["published"] and out["env"]==env_s and out["changed"], out
assert st_ws.updates[0][0]=="L3:M3" and st_ws.updates[0][1][0][0]=="TFB Calibration" and st_ws.updates[0][1][0][1].startswith((env_s or "NONE")+" | n="), st_ws.updates
cal_ws=obj.store.sheet.tabs["_S1_Calibration"]; assert cal_ws.updates[0][0]=="A12:D18" and len(cal_ws.updates[0][1])==7 and cal_ws.updates[0][1][0][0].startswith("RELIABILITY FACTORS"), cal_ws.updates
log=obj.store.sheet.tabs["_Run_Log"].appended; assert len(log)==1 and log[0][5].startswith("[CALIBRATION v6.36.0] factors changed: (none) -> "+(env_s or "NONE")), log
print("T1 PASS first publication: _Status L3:M3 'TFB Calibration', _S1_Calibration A12:D18 block, _Run_Log change line (none -> env)")
# T2 unchanged republish: existing key updated in place, NO change line
grid=[["Global Key","Value"],["Backend URL","https://x"],["TFB Calibration",(env_s or "NONE")+" | n=old | brier=0.2 | as_of=t"],[]]
obj2,out2=run(grid, cal); assert out2["published"] and not out2["changed"]
assert obj2.store.sheet.tabs["_Status"].updates[0][0]=="L3:M3" and obj2.store.sheet.tabs["_Run_Log"].appended==[]
print("T2 PASS unchanged factors: key rewritten in place at L3:M3, no _Run_Log line")
# T3 changed factors: change line names old -> new
grid=[["Global Key","Value"],["TFB Calibration","INVESTABLE:0.500 | n=x | brier= | as_of=t"]]
obj3,out3=run(grid, cal); assert out3["changed"] and obj3.store.sheet.tabs["_Run_Log"].appended[0][5].startswith("[CALIBRATION v6.36.0] factors changed: INVESTABLE:0.500 -> ")
print("T3 PASS changed factors -> audited old -> new")
# T4 insufficient buckets never published as factors
class B: pass
def bucket(name,decided,f,suff): b=B(); b.name=name; b.decided=decided; b.calibration_factor_shrunk=f; b.sufficient=suff; return b
class R: pass
rep=R(); rep.by_investability=[bucket("INVESTABLE",9,0.6,False),bucket("WATCHLIST",120,0.95,True)]; rep.brier_score=None; rep.min_sample=10
e,n=M._calibration_env_string(rep); assert e=="WATCHLIST:0.950" and n=="INVESTABLE=n/a(9) WATCHLIST=120", (e,n)
rep.by_investability=[bucket("INVESTABLE",5,0.6,False)]; assert M._calibration_env_string(rep)==("", "INVESTABLE=n/a(5)")
obj4,out4=run([["Global Key","Value"],[]], rep); assert out4["published"] and out4["env"]=="" and "NONE | n=" in obj4.store.sheet.tabs["_Status"].updates[0][1][0][1]
print("T4 PASS insufficient buckets -> n/a(decided), env NONE published with the sample note")
# T5 kill-switch: nothing written
os.environ["TRACK_PUBLISH_CALIBRATION"]="0"; obj5,out5=run([["Global Key","Value"],[]], cal); os.environ.pop("TRACK_PUBLISH_CALIBRATION")
assert out5=={"published":False,"env":"","changed":False} and obj5.store.sheet.tabs["_Status"].updates==[]
print("T5 PASS kill-switch: no cell written")
# T6 no free slot / missing tab: fail-open
obj6,out6=run([["k%d"%i,"v"] for i in range(60)], cal); assert not out6["published"]
class Sheet2(Sheet):
    def worksheet(s,name):
        if name=="_S1_Calibration": raise RuntimeError("no tab")
        return s.tabs[name]
cls=[c for c in vars(M).values() if isinstance(c,type) and hasattr(c,"_publish_reliability_calibration")][0]
o7=cls.__new__(cls); o7.store=Store(); o7.store.sheet=Sheet2([["Global Key","Value"],[]]); out7=o7._publish_reliability_calibration(cal); assert out7["published"]
print("T6 PASS fail-open: no free L-slot -> not published, no exception; missing _S1_Calibration tab -> _Status still published")
print("PASS 6/6 — real track_performance v6.36.0: real calibrator on the real log + publisher contract")
