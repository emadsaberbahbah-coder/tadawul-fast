import sys, os, importlib.util, threading, time, asyncio, datetime as dt
from unittest.mock import MagicMock
sys.path.insert(0,"/home/claude/repo/tadawul-fast-main")
def load(path,name):
    spec=importlib.util.spec_from_file_location(name,path); m=importlib.util.module_from_spec(spec); sys.modules[name]=m
    try: spec.loader.exec_module(m)
    except ModuleNotFoundError as e:
        missing=str(e).split("'")[1]; mm=MagicMock(); mm.__name__=missing; sys.modules[missing]=mm; return load(path,name)
    return m
for k in ("TRACK_CA_VERIFY_BUDGET","TRACK_CA_VERIFY_TIME_SEC","TRACK_CA_GUARD","TFB_TRACK_MATURE_FRESH_ONLY","TFB_TRACK_CA_GUARD"): os.environ.pop(k,None)
M=load("/home/claude/tp637.py","tp637"); assert M.SCRIPT_VERSION=="6.37.0"
# --- instrument the ONLY network seam: _yf_deep_history (count + latency) ---
calls={"n":0}
def fake_hist(sym, period="2y"):
    calls["n"]+=1; time.sleep(0.01)
    base=dt.date(2026,1,1); return [{"date":(base+dt.timedelta(days=i)).isoformat(),"close":100.0+i} for i in range(240)]
M._yf_deep_history=fake_hist
# --- 4,478-record overdue backlog: 900 symbols x ~5 records, 50% with print ROI >= 15% (needs CA verification) ---
H=M.HorizonType; R=M.PerformanceRecord
recs=[]
for i in range(4478):
    sym=f"SYM{i%900}"; entry=100.0; px=125.0 if i%2==0 else 104.0      # 25% or 4% print ROI
    r=R(record_id=f"r{i}",symbol=sym,horizon=H.WEEK_1 if hasattr(H,"WEEK_1") else list(H)[0],date_recorded=M.RiyadhTime.now()-dt.timedelta(days=20),
        entry_price=entry,entry_recommendation=list(M.RecommendationType)[0],entry_score=60.0,entry_risk_bucket="LOW",entry_confidence="MEDIUM",origin_tab="T",
        target_price=110.0,target_roi=0.1,target_date=M.RiyadhTime.now()-dt.timedelta(days=5),status=M.PerformanceStatus.ACTIVE,current_price=px,unrealized_roi=0.0)
    recs.append(r)
prices={f"SYM{i}": (125.0 if True else 0.0) for i in range(900)}
class Backend:
    base_url="https://x"
    async def fetch_prices(self,syms): return {s:(125.0 if int(s[3:])%2==0 else 104.0) for s in syms}
cls=[c for c in vars(M).values() if isinstance(c,type) and hasattr(c,"audit_active_records")][0]
obj=cls.__new__(cls); obj.backend=Backend()
M._price_fallback_enabled=lambda: False
# even records: SYM(even) price 125 -> ROI 25% -> needs CA pull; odd: 4% -> plain maturation
os.environ["TRACK_CA_VERIFY_BUDGET"]="60"; os.environ["TRACK_CA_VERIFY_TIME_SEC"]="0"
t0=time.time(); out=asyncio.run(obj.audit_active_records(recs)); dt_s=time.time()-t0
st=obj._last_audit_stats
mat=sum(1 for r in out if r.status==M.PerformanceStatus.MATURED); exp=sum(1 for r in out if r.status==M.PerformanceStatus.EXPIRED)
act=sum(1 for r in out if r.status==M.PerformanceStatus.ACTIVE)
print(f"T1 run: {dt_s:.1f}s | matured={mat} expired={exp} active(deferred)={act} | yf pulls={calls['n']} | stats ca_pulls={st.get('ca_pulls')} ca_deferred={st.get('ca_deferred')}")
assert calls["n"]==60, calls          # budget respected: exactly 60 symbol pulls
assert exp==0, "an exhausted budget must never expire a record"
assert st["ca_deferred"]>0 and act==st["ca_deferred"], (act, st["ca_deferred"])
assert mat>=2000, mat                 # plain (<15%) records still mature; verified ones within budget mature
print("T1 PASS budget=60: 60 pulls, zero expiries, deferred records stay ACTIVE, the rest mature")
# T2 per-symbol cache: unlimited budget, 900 symbols x many records -> exactly one pull per symbol needing verification
calls["n"]=0; os.environ["TRACK_CA_VERIFY_BUDGET"]="0"
recs2=[]
for i in range(4478):
    r=R(record_id=f"q{i}",symbol=f"SYM{i%900}",horizon=list(H)[0],date_recorded=M.RiyadhTime.now()-dt.timedelta(days=20),entry_price=100.0,
        entry_recommendation=list(M.RecommendationType)[0],entry_score=60.0,entry_risk_bucket="LOW",entry_confidence="MEDIUM",origin_tab="T",
        target_price=110.0,target_roi=0.1,target_date=M.RiyadhTime.now()-dt.timedelta(days=5),status=M.PerformanceStatus.ACTIVE,current_price=125.0,unrealized_roi=0.0)
    recs2.append(r)
out2=asyncio.run(obj.audit_active_records(recs2)); st2=obj._last_audit_stats
assert calls["n"]==450, calls         # even-numbered symbols only (price 125): 450 distinct symbols, one pull each, NOT 2239 records
assert st2["ca_deferred"]==0 and sum(1 for r in out2 if r.status==M.PerformanceStatus.MATURED)>=2000
print("T2 PASS unlimited budget: one pull per SYMBOL (450), not per record (2,239); zero deferred")
# T3 time budget exhaustion defers, never expires
calls["n"]=0; os.environ["TRACK_CA_VERIFY_BUDGET"]="0"; os.environ["TRACK_CA_VERIFY_TIME_SEC"]="0.05"
def slow_hist(sym,period="2y"): calls["n"]+=1; time.sleep(0.03); return fake_hist(sym)
M._yf_deep_history=slow_hist
recs3=[R(record_id=f"z{i}",symbol=f"SYM{i}",horizon=list(H)[0],date_recorded=M.RiyadhTime.now()-dt.timedelta(days=20),entry_price=100.0,
        entry_recommendation=list(M.RecommendationType)[0],entry_score=60.0,entry_risk_bucket="LOW",entry_confidence="MEDIUM",origin_tab="T",
        target_price=110.0,target_roi=0.1,target_date=M.RiyadhTime.now()-dt.timedelta(days=5),status=M.PerformanceStatus.ACTIVE,current_price=125.0,unrealized_roi=0.0) for i in range(40)]
out3=asyncio.run(obj.audit_active_records(recs3)); st3=obj._last_audit_stats
assert calls["n"]<40 and st3["ca_deferred"]>0 and not [r for r in out3 if r.status==M.PerformanceStatus.EXPIRED]
print(f"T3 PASS time budget: pulls stopped at {calls['n']}/40, deferred={st3['ca_deferred']}, expired=0")
print("PASS 3/3 — real audit_active_records v6.37.0 on a 4,478-record backlog")
