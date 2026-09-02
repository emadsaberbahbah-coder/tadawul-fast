"""Harness v5.135.0 Fix AJ — REAL module, original vs patched, rows taken from the
live 2026-09-02 Global_Markets export (KLAC/MCHPP/KIDZ garbage; AAPL/NVDA/JNJ provider)."""
import importlib.util, sys, os, json, copy
def load(path, name, root):
    sys.path.insert(0, root)
    spec=importlib.util.spec_from_file_location(name, path); m=importlib.util.module_from_spec(spec)
    sys.modules[name]=m; spec.loader.exec_module(m); return m
d0=load("/home/claude/live2/tadawul-fast-main/core/data_engine_v2.py","de_orig","/home/claude/live2/tadawul-fast-main")
d1=load("/home/claude/ci_de/tadawul-fast-main/core/data_engine_v2.py","de_new","/home/claude/ci_de/tadawul-fast-main")
assert d0.__version__=="5.134.0" and d1.__version__=="5.135.0"
# rows shaped as the engine sees them at the final boundary (values from the live export)
rows={
 "KLAC.US":  dict(symbol="KLAC.US", current_price=170.89, target_mean_price=3812.3035, intrinsic_value=3022.8613, upside_pct=-0.251582, provider_rating="HOLD", rsi_14=45.0, warnings="quote_exchange_from_suffix"),
 "MCHPP.US": dict(symbol="MCHPP.US", current_price=61.87, target_mean_price=129500.0, intrinsic_value=119949.76, upside_pct=0.3478, provider_rating="BUY", warnings=""),
 "KIDZ.US":  dict(symbol="KIDZ.US", current_price=4.05, target_mean_price=69730.766, intrinsic_value=119.8103, upside_pct=0.387015, provider_rating="HOLD", warnings=""),
 "AAPL.US":  dict(symbol="AAPL.US", current_price=325.13, target_mean_price=324.45282, intrinsic_value=301.5864, upside_pct=-0.072413, provider_rating="BUY", warnings="x"),
 "NVDA.US":  dict(symbol="NVDA.US", current_price=217.44, target_mean_price=323.41898, intrinsic_value=271.1239, upside_pct=0.246891, provider_rating="STRONG_BUY", warnings=""),
 "JNJ.US":   dict(symbol="JNJ.US", current_price=271.19, target_mean_price=272.5, intrinsic_value=221.6112, upside_pct=-0.182819, provider_rating="BUY", warnings=""),
 "NOPX.US":  dict(symbol="NOPX.US", current_price=None, target_mean_price=99999.0, intrinsic_value=88888.0, upside_pct=0.5, warnings=""),  # no price => fail-open
}
def run(mod, env):
    saved={k:os.environ.get(k) for k in ("TFB_TARGET_SANITY_GUARD",)}
    for k,v in env.items():
        if v is None: os.environ.pop(k,None)
        else: os.environ[k]=v
    out={}
    try:
        for s,r in rows.items():
            rr=copy.deepcopy(r); mod._apply_analyst_trend_block(rr); out[s]=rr
    finally:
        for k,v in saved.items():
            if v is None: os.environ.pop(k,None)
            else: os.environ[k]=v
    return out
fails=[]
A0=run(d0,{"TFB_TARGET_SANITY_GUARD":None}); A1=run(d1,{"TFB_TARGET_SANITY_GUARD":None})
for s in ("KLAC.US","MCHPP.US","KIDZ.US"):
    a0,a1=A0[s],A1[s]
    print(f"{s:9} v5.134: target={a0.get('target_price')} iv={a0.get('intrinsic_value')} | v5.135: target={a1.get('target_price')} iv={a1.get('intrinsic_value')} upside={a1.get('upside_pct')} warn={a1.get('warnings')[:80]}")
    if a1.get("target_price") is not None or a1.get("intrinsic_value") is not None: fails.append(f"A: {s} garbage not blanked")
    if "target_rejected_outlier" not in str(a1.get("warnings")) or "intrinsic_rejected_outlier" not in str(a1.get("warnings")): fails.append(f"A: {s} tags missing")
    if a1.get("upside_pct") is not None or a1.get("upside_downside_pct") is not None: fails.append(f"A: {s} upside not blanked")
for s in ("AAPL.US","NVDA.US","JNJ.US","NOPX.US"):
    a0,a1=A0[s],A1[s]
    j0=json.dumps(a0,sort_keys=True,default=str); j1=json.dumps(a1,sort_keys=True,default=str)
    print(f"{s:9} provider/fail-open row byte-identical: {j0==j1}  target={a1.get('target_price')} iv={a1.get('intrinsic_value')}")
    if j0!=j1: fails.append(f"A: {s} provider row changed")
B0=run(d0,{"TFB_TARGET_SANITY_GUARD":"0"}); B1=run(d1,{"TFB_TARGET_SANITY_GUARD":"0"})
same=all(json.dumps(B0[s],sort_keys=True,default=str)==json.dumps(B1[s],sort_keys=True,default=str) for s in rows)
print("B kill-switch byte-identical on all rows:", same)
if not same: fails.append("B: kill-switch not byte-identical")
# C: idempotence — a second pass at the next boundary must not re-fill or re-tag
C1=A1["KLAC.US"]; before=json.dumps(C1,sort_keys=True,default=str); d1._apply_analyst_trend_block(C1); after=json.dumps(C1,sort_keys=True,default=str)
print("C second boundary pass idempotent:", before==after, "| tags:", str(C1.get("warnings")).count("rejected_outlier"))
if before!=after: fails.append("C: not idempotent")
print("HARNESS:", "PASS" if not fails else "FAIL "+str(fails)); sys.exit(1 if fails else 0)
