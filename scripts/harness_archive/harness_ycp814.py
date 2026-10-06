import sys, os, importlib.util
from unittest.mock import MagicMock
sys.path.insert(0,"/home/claude/repo/tadawul-fast-main")
def load(path,name):
    spec=importlib.util.spec_from_file_location(name,path); m=importlib.util.module_from_spec(spec); sys.modules[name]=m
    try: spec.loader.exec_module(m)
    except ModuleNotFoundError as e:
        missing=str(e).split("'")[1]; mm=MagicMock(); mm.__name__=missing; sys.modules[missing]=mm; return load(path,name)
    return m
os.environ.pop("TFB_YC_OPEN_FALLBACK",None)
M=load("/home/claude/ycp_new.py","ycp814"); assert M.PROVIDER_VERSION=="8.14.0"
hist=[{"timestamp":"2026-08-31T12:00:00Z","open":30.0,"high":31.0,"low":29.5,"close":30.4},{"timestamp":"2026-09-01T12:00:00Z","open":30.6,"high":31.2,"low":30.1,"close":30.9}]
# T1 missing open, coherent candle -> healed with provenance
prov={}; assert M._apply_open_fallback(None,31.2,30.1,hist,prov)==30.6 and prov=={"open_source":"history_last_candle"}
# T2 provider open present -> never overridden
assert M._apply_open_fallback(30.0,31.2,30.1,hist,{})==30.0
# T3 stale candle open outside today's range -> rejected (stays None)
assert M._apply_open_fallback(None,31.2,30.1,[{"open":29.0,"high":29.5,"low":28.0,"close":29.2}],{}) is None
assert M._apply_open_fallback(None,31.2,30.1,[{"open":32.0,"high":32.5,"low":31.5,"close":32.1}],{}) is None
# T4 unknown range -> candle open accepted (no range to contradict); zero/None candle -> None; empty history -> None
assert M._apply_open_fallback(None,None,None,hist,{})==30.6 and M._apply_open_fallback(None,None,None,[{"open":None}],{}) is None and M._apply_open_fallback(None,1,2,[],{}) is None
# T5 kill-switch
os.environ["TFB_YC_OPEN_FALLBACK"]="0"; assert M._apply_open_fallback(None,31.2,30.1,hist,{}) is None; os.environ.pop("TFB_YC_OPEN_FALLBACK")
# T6 range fallback + open fallback together, as in the quote path: .SR info without open/range
dh,dl,_,_=M._apply_range_fallbacks(None,None,1.0,1.0,hist,provenance={}); op=M._apply_open_fallback(None,dh,dl,hist,{})
assert (dh,dl,op)==(31.2,30.1,30.6)
print("PASS 6/6 — real yahoo_chart_provider v8.14.0: heal, no-override, stale-rejection (both sides), unknown-range/empty, kill-switch, combined path")
