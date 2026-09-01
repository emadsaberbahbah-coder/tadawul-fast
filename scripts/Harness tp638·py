import sys, os, importlib.util, pickle, threading, glob
from unittest.mock import MagicMock
sys.path.insert(0,"/home/claude/repo/tadawul-fast-main")
def load(path,name):
    spec=importlib.util.spec_from_file_location(name,path); m=importlib.util.module_from_spec(spec); sys.modules[name]=m
    try: spec.loader.exec_module(m)
    except ModuleNotFoundError as e:
        missing=str(e).split("'")[1]; mm=MagicMock(); mm.__name__=missing; sys.modules[missing]=mm; return load(path,name)
    return m
os.chdir("/tmp"); [os.remove(f) for f in glob.glob("dropped_duplicates_*.csv")]
for k in ("TRACK_DROP_DUPLICATE_ROWS","TRACK_DEDUP_KEYS","TRACK_MAX_RECORDS"): os.environ.pop(k,None)
M=load("/home/claude/tp638.py","tp638"); assert M.SCRIPT_VERSION=="6.38.0"
hdr,rows=pickle.load(open("/home/claude/perf_log.pkl","rb")); DATA=[[("" if v is None else v) for v in r] for r in rows[4:] if r and r[0] not in (None,"")]
class WS:
    def __init__(s,data,rc): s.data=data; s.row_count=rc; s.cleared=[]; s.updates=[]; s.deleted=[]
    def get(s,rng):
        import re; m=re.match(r"[A-Z]+(\d+):[A-Z]+(\d+)",rng); r0,r1=int(m.group(1)),int(m.group(2)); return [list(r) for r in s.data[r0-6:r1-6+1]]
    def batch_clear(s,r): s.cleared+=r
    def update(s,values=None,range_name=None): s.updates.append((range_name,len(values)))
    def delete_rows(s,a,b=None): s.deleted.append((a,b))
class Backoff:
    def execute_sync(s,fn): return fn()
def store(): st=M.PerformanceStore.__new__(M.PerformanceStore); st.ws=WS(DATA,38045); st.backoff=Backoff(); st.cache={}; st.cache_lock=threading.Lock(); return st
# T1 default OFF: v6.37.0 behaviour — all rows rewritten, no delete, no archive
st=store(); recs=st.load_records(200000); dups=sum(1 for r in recs if r.outcome=="DUPLICATE_KEY"); st.save_records(recs)
assert st.ws.deleted==[] and not glob.glob("dropped_duplicates_*.csv") and sum(n for _,n in st.ws.updates)==38040
print(f"T1 PASS flag OFF: 38,040 rows rewritten, no delete, no archive (duplicates present: {dups:,})")
# T2 flag ON: archive, kept written, tail rows deleted, extent shrinks
os.environ["TRACK_DROP_DUPLICATE_ROWS"]="1"; st=store(); recs=st.load_records(200000); ok=st.save_records(recs)
kept=38040-dups; files=glob.glob("dropped_duplicates_*.csv"); assert ok and len(files)==1, files
n_arch=sum(1 for _ in open(files[0],encoding="utf-8"))-1; assert n_arch==dups, (n_arch,dups)
assert sum(n for _,n in st.ws.updates)==kept and st.ws.cleared==["A6:AF38045"], (sum(n for _,n in st.ws.updates),kept,st.ws.cleared)
assert st.ws.deleted==[(6+kept,38045)], st.ws.deleted
assert st._loaded_extent==6+kept-1
print(f"T2 PASS flag ON: {dups:,} duplicates archived to {files[0]}, {kept:,} kept rows written, rows {6+kept}..38045 deleted, extent -> {st._loaded_extent}")
# T3 second run after the drop: nothing to drop, no delete
kept_data=[r for r in DATA if not (str(r[6]).lower()=="expired" and str(r[7])=="DUPLICATE_KEY")]  # Status col 6? use record objects instead
st2=store(); recs2=[r for r in recs if not (r.status==M.PerformanceStatus.EXPIRED and r.outcome=="DUPLICATE_KEY")]
st2._loaded_extent=6+len(recs2)-1; st2.save_records(recs2); assert st2.ws.deleted==[], st2.ws.deleted
print("T3 PASS after the drop: no duplicates -> no archive/delete (one-shot)")
# T4 archive failure -> nothing dropped
os.environ["TRACK_DROP_DUPLICATE_ROWS"]="1"; M._archive_dropped_records=lambda h,r: ""; st3=store(); recs3=st3.load_records(200000); st3.save_records(recs3)
assert st3.ws.deleted==[] and sum(n for _,n in st3.ws.updates)==38040
print("T4 PASS archive failure -> fail-closed: nothing dropped, all rows rewritten")
print("PASS 4/4 — real track_performance v6.38.0 store on the real log")
