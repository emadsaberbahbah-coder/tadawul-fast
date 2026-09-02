"""Harness v6.56.0 RB-TOLERANCE — REAL module (original vs patched), meta dicts
taken from today's actual runs: GM/MF/CFX of #3835 and the 07:00 enforce run."""
import importlib.util, sys, os, json
def load(path, name):
    root=os.path.dirname(os.path.dirname(path)); sys.path[:0]=[os.path.join(root,"scripts"), root]
    spec=importlib.util.spec_from_file_location(name, path); m=importlib.util.module_from_spec(spec)
    sys.modules[name]=m; spec.loader.exec_module(m); return m
d0=load("/home/claude/live3/tadawul-fast-main/scripts/run_dashboard_sync.py","ds_orig")
d1=load("/home/claude/ci_ds/tadawul-fast-main/scripts/run_dashboard_sync.py","ds_new")
assert d0.SCRIPT_VERSION=="6.55.0" and d1.SCRIPT_VERSION=="6.56.0"
cases={  # name: (pw_flagged, rb_flagged, rb_checked)
 "GM_3835_unarmed":(63,78,6609), "GM_0700_enforce":(66,72,6609), "MF_3835_unarmed":(0,12,2474),
 "MF_0700_enforce":(0,4,2474),   "CFX_3835":(1,3,453),           "ML_clean":(0,0,255),
 "GM_big_survival":(63,140,6609), "missing_counters":(5,None,None), "negative_delta":(10,4,6609),
}
def meta(pw,rb,ck):
    m={"rb_status":"DIVERGENT" if (rb is None or rb!=pw) else "MATCHED","pw_flagged":pw}
    if rb is not None: m["rb_flagged"]=rb
    if ck is not None: m["rb_checked"]=ck
    return m
def run(mod, env):
    saved={k:os.environ.get(k) for k in ("TFB_SYNC_RB_TOL_ROWS","TFB_SYNC_RB_TOL_PCT")}
    for k,v in env.items():
        if v is None: os.environ.pop(k,None)
        else: os.environ[k]=v
    try:
        return {n: mod._status_data_verdict("success",0,99.0,95.0,meta(*c)) for n,c in cases.items()}
    finally:
        for k,v in saved.items():
            if v is None: os.environ.pop(k,None)
            else: os.environ[k]=v
fails=[]
# A: defaults (both unset) -> byte-identical verdicts vs v6.55.0
A0=run(d0,{}); A1=run(d1,{})
print("A defaults byte-identical:",A0==A1, "|", A1)
if A0!=A1: fails.append("A: default not byte-identical")
# B: recommended arming PCT=0.25 ROWS=2
B1=run(d1,{"TFB_SYNC_RB_TOL_PCT":"0.25","TFB_SYNC_RB_TOL_ROWS":"2"})
exp={"GM_3835_unarmed":"COMPLETE","GM_0700_enforce":"COMPLETE","MF_3835_unarmed":"PARTIAL","MF_0700_enforce":"COMPLETE",
     "CFX_3835":"COMPLETE","ML_clean":"COMPLETE","GM_big_survival":"PARTIAL","missing_counters":"PARTIAL","negative_delta":"PARTIAL"}
for k,v in exp.items():
    print(f"  B {k:18} -> {B1[k]:8} (expected {v})")
    if B1[k]!=v: fails.append(f"B: {k} {B1[k]} != {v}")
# C: stamp note text
os.environ["TFB_SYNC_RB_TOL_PCT"]="0.25"; os.environ["TFB_SYNC_RB_TOL_ROWS"]="2"
note=d1._rb_tolerance_note(meta(63,78,6609),63); note2=d1._rb_tolerance_note(meta(0,12,2474),0); note3=d1._rb_tolerance_note(meta(0,0,255),0)
os.environ.pop("TFB_SYNC_RB_TOL_PCT"); os.environ.pop("TFB_SYNC_RB_TOL_ROWS")
print("C note GM:",repr(note),"| MF (not tolerated):",repr(note2),"| ML matched:",repr(note3))
if note!=" rb_tol=17(+15)" or note2!="" or note3!="": fails.append("C: note text wrong")
# D: garbage env -> safe
D1=run(d1,{"TFB_SYNC_RB_TOL_PCT":"abc","TFB_SYNC_RB_TOL_ROWS":"-5"})
print("D garbage env == defaults:",D1==A1)
if D1!=A1: fails.append("D: garbage env changed verdicts")
print("HARNESS:","PASS" if not fails else "FAIL "+str(fails)); sys.exit(1 if fails else 0)
