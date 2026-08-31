import sys, os, importlib.util
from unittest.mock import MagicMock
sys.path.insert(0,"/home/claude/repo/tadawul-fast-main")
def load(path,name):
    spec=importlib.util.spec_from_file_location(name,path); m=importlib.util.module_from_spec(spec); sys.modules[name]=m
    try: spec.loader.exec_module(m)
    except ModuleNotFoundError as e:
        missing=str(e).split("'")[1]; mm=MagicMock(); mm.__name__=missing; sys.modules[missing]=mm; return load(path,name)
    return m
for k in ("TFB_T10_CAP_BAND_RAW_EXEMPT","TFB_T10_EXCLUDE_CAP_BAND","TFB_T10_CAP_BAND"): os.environ.pop(k,None)
M=load("/home/claude/ob1170.py","ob1170"); assert M.OPPORTUNITY_BUILDER_VERSION=="1.17.0"
# T1 - parser: sheet glyph forms now parse; everything that parsed before is unchanged
assert M._to_float("\u25b2 31.90%")==31.9 and M._to_float("\u25bc -3.82%")==-3.82 and M._to_float("\u25b2 0.34%")==0.34
for v,exp in [("12.5%",12.5),("1,234.5",1234.5),("0.34464",0.34464),("-1.4761",-1.4761),("",None),("n/a",None),("—",None),("abc",None),(7,7.0),(True,None)]:
    assert M._to_float(v)==exp,(v,M._to_float(v),exp)
print("T1 PASS _to_float: glyph forms parse; legacy forms identical")
lo,hi=M._env_cap_band(); assert (lo,hi)==(1.335,1.365)
# T2 - genuine soft-capped target: engine ratio in band, raw target above band -> PASS with provenance text
c={"engine_roi_12m_pct":0.35,"target_price":4100.0,"price":3053.0}   # NOS.LS/2503.T-like: raw x1.3430? no -> use above band
c={"engine_roi_12m_pct":0.35,"target_price":4400.0,"price":3053.0}   # raw x1.4412
ok,cur=M._cap_band_assessment(c); assert ok and "(raw" in cur, (ok,cur)
print("T2 PASS soft-capped genuine target (engine x1.3500, raw x1.4412) -> passes:",cur)
# T3 - minted fingerprint: engine in band AND raw target in band -> still BLOCKED (v1.16.0 behaviour)
c2={"engine_roi_12m_pct":0.35,"target_price":1350.0,"price":1000.0}
ok,cur=M._cap_band_assessment(c2); assert not ok and "(raw" not in cur,(ok,cur)
print("T3 PASS mint (engine x1.35, raw x1.35) -> still blocked")
# T4 - in band with NO raw target (blank Target Price) -> blocked exactly as v1.16.0
ok,cur=M._cap_band_assessment({"engine_roi_12m_pct":0.35,"target_price":None,"price":10.0}); assert not ok
ok,cur=M._cap_band_assessment({"engine_roi_12m_pct":0.35,"target_price":"","price":10.0}); assert not ok
print("T4 PASS in band, no raw target -> blocked (unchanged)")
# T5 - outside band passes regardless; blank engine forecast passes (unchanged)
assert M._cap_band_assessment({"engine_roi_12m_pct":0.20,"target_price":None,"price":10.0})[0]
assert M._cap_band_assessment({"engine_roi_12m_pct":None})==(True,"\u2014")
print("T5 PASS outside-band / blank forecast unchanged")
# T6 - kill-switch restores v1.16.0: genuine soft-capped target blocked again
os.environ["TFB_T10_CAP_BAND_RAW_EXEMPT"]="0"; ok,cur=M._cap_band_assessment(c); os.environ.pop("TFB_T10_CAP_BAND_RAW_EXEMPT")
assert not ok and cur=="\u00d71.3500",(ok,cur)
print("T6 PASS kill-switch -> v1.16.0 assessment")
# T7 - control: v1.16.0 blocks the genuine soft-capped target and cannot parse glyphs
M0=load("/home/claude/ob1160.py","ob1160"); assert M0.OPPORTUNITY_BUILDER_VERSION=="1.16.0"
assert M0._cap_band_assessment(c)==(False,"\u00d71.3500") and M0._to_float("\u25b2 31.90%") is None
print("T7 PASS control: v1.16.0 reproduces both defects on identical input")
print("PASS 7/7 — real builder module opportunity_builder v1.17.0")
