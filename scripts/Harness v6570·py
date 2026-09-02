"""Harness v6.57.0 NULL-CLEAR SCOPE — real _ohlc_fill_guard_apply, original vs patched."""
import importlib.util,sys,os,json
root="/home/claude/ci_ds/tadawul-fast-main"; sys.path[:0]=[root+"/scripts",root]
def load(path,name):
    spec=importlib.util.spec_from_file_location(name,path); m=importlib.util.module_from_spec(spec); sys.modules[name]=m; spec.loader.exec_module(m); return m
m1=load(root+"/scripts/run_dashboard_sync.py","ds_new"); m0=load("/home/claude/live3/tadawul-fast-main/scripts/run_dashboard_sync.py","ds_old")
assert m1.SCRIPT_VERSION=="6.57.0"
hdr=["Symbol","Name","Sector","Open","Day High","Day Low","Current Price","Target Price","Intrinsic Value","P/E (TTM)","EPS (TTM)","Warnings"]
def mtx():  # 12 None cells: MCHPP 5, NBRG 7
    return [["MCHPP.US","Microchip",None,None,61.0,60.0,62.95,None,None,None,-1.49,"tag"],
            ["AAPL.US","Apple","Tech",324.0,326.0,323.0,325.4,324.45,301.6,28.0,11.6,""],
            ["NBRG.US",None,"Fin",None,None,None,10.07,None,19.39,None,None,"intrinsic_rejected_outlier(x0.1)"]]
KEYS=("TFB_SYNC_OHLC_FILL_GUARD","TFB_SYNC_OHLC_FILL_GUARD_MODE","TFB_SYNC_NULL_CLEAR_SCOPE","TFB_SYNC_NULL_KEEP_COLS","TFB_SYNC_OHLC_FILL_GUARD_COLS")
def run(mod,env):
    saved={k:os.environ.get(k) for k in KEYS}
    for k in KEYS: os.environ.pop(k,None)
    os.environ.update(env)
    try:
        mod._OHLC_FILLGUARD_SELFTEST_OK=None; return mod._ohlc_fill_guard_apply(list(hdr),mtx())
    finally:
        for k,v in saved.items():
            if v is None: os.environ.pop(k,None)
            else: os.environ[k]=v
fails=[]
A0=run(m0,{"TFB_SYNC_OHLC_FILL_GUARD":"1","TFB_SYNC_OHLC_FILL_GUARD_MODE":"enforce"}); A1=run(m1,{"TFB_SYNC_OHLC_FILL_GUARD":"1","TFB_SYNC_OHLC_FILL_GUARD_MODE":"enforce"})
same=json.dumps(A0[0])==json.dumps(A1[0]) and {k:v for k,v in A1[1].items() if k!="scope"}=={k:v for k,v in A0[1].items() if k!="scope"}
print("A default scope (ohlc) enforce: byte-identical to main:",same,"| cleared",A1[1]["total"],A1[1]["cols"]); fails+=[] if same else ["A"]
mx,st=run(m1,{"TFB_SYNC_OHLC_FILL_GUARD":"1","TFB_SYNC_OHLC_FILL_GUARD_MODE":"enforce","TFB_SYNC_NULL_CLEAR_SCOPE":"all","TFB_SYNC_NULL_KEEP_COLS":"P/E (TTM)"})
okB = st["total"]==10 and mx[0][7]=="" and mx[0][8]=="" and mx[0][2]=="" and mx[2][1]=="" and mx[0][9] is None and "Symbol" not in st["cols"] and st.get("scope")=="all"
print("B scope=all enforce + keep P/E: cleared",st["total"],"| target/iv/sector blanked, Symbol untouched, P/E kept:",okB); fails+=[] if okB else ["B"]
C=run(m1,{"TFB_SYNC_OHLC_FILL_GUARD":"1","TFB_SYNC_OHLC_FILL_GUARD_MODE":"observe","TFB_SYNC_NULL_CLEAR_SCOPE":"all"})
okC = C[0]==mtx() and C[1]["total"]==12 and C[1]["action"]=="observed"
print("C scope=all observe: untouched, counts 12:",okC); fails+=[] if okC else ["C"]
D=run(m1,{"TFB_SYNC_NULL_CLEAR_SCOPE":"all"})
okD = D[0]==mtx() and D[1] is None
print("D guard OFF + scope=all: untouched, stats None:",okD); fails+=[] if okD else ["D"]
print("HARNESS:","PASS" if not fails else "FAIL "+str(fails)); sys.exit(1 if fails else 0)
