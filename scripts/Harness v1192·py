"""Harness v1.19.2 ELIGIBILITY (VENUE) — REAL build_opportunity_payload, original vs patched."""
import importlib.util, sys, os, json, copy
def load(path, name, root):
    sys.path.insert(0, root); spec=importlib.util.spec_from_file_location(name, path); m=importlib.util.module_from_spec(spec)
    sys.modules[name]=m; spec.loader.exec_module(m); return m
ob0=load("/home/claude/live3/tadawul-fast-main/core/analysis/opportunity_builder.py","ob_a","/home/claude/live3/tadawul-fast-main")
ob1=load("/home/claude/ci_ob2/tadawul-fast-main/core/analysis/opportunity_builder.py","ob_b","/home/claude/ci_ob2/tadawul-fast-main")
assert ob0.OPPORTUNITY_BUILDER_VERSION=="1.19.1" and ob1.OPPORTUNITY_BUILDER_VERSION=="1.19.2"
def row(**kw):
    base={"symbol":"1120.SR","name":"Al Rajhi","sector":"Banks","market":"Tadawul","currency":"SAR","current_price":100.0,
          "intrinsic_value":130.0,"forecast_reliability_score":82.0,"data_quality_score":91.0,"risk_bucket":"Moderate",
          "provider_engine_conflict":"No","volatility_30d":4.0,"avg_volume_30d":2500000,"expected_roi_12m":24.0,
          "recommendation_detailed":"STRONG BUY","investability_status":"INVESTABLE","block_reason":""}
    base.update(kw); return base
pool=[row(), row(symbol="NVDA.US",name="Nvidia",market="NASDAQ",currency="USD",sector="Tech"),
      row(symbol="HDFCBANK.NS",name="HDFC",market="NSE",currency="INR",sector="Banks2"),
      row(symbol="2317.TW",name="Hon Hai",market="TWSE",currency="USD",sector="Elec"),
      row(symbol="OTIS",name="Otis bare",market="Unknown",currency="USD",sector="Ind"),
      row(symbol="XYZ",name="No venue at all",market="",currency="USD",sector="Ind2")]
crit={"cash_available_sar":200000.0,"max_selected":10}
def run(mod, allow):
    if allow is None: os.environ.pop("TFB_T10_VENUE_ALLOWLIST",None)
    else: os.environ["TFB_T10_VENUE_ALLOWLIST"]=allow
    try: return mod.build_opportunity_payload(copy.deepcopy(pool), criteria=crit)
    finally: os.environ.pop("TFB_T10_VENUE_ALLOWLIST",None)
def canon(p):
    q=copy.deepcopy(p); q.pop("generated_at_utc",None); (q.get("meta") or {}).pop("generated_at_utc",None)
    return json.dumps(q,sort_keys=True,default=str).replace('"1.19.2"','"1.19.1"')
def ff(p,s):
    r=[x for x in p["candidates_rows"] if x["symbol"]==s]; return (r[0].get("first_fail") or {}).get("gate") if r else "ABSENT", (r[0]["verdict"] if r else None)
fails=[]
A0,A1=run(ob0,None),run(ob1,None)
print("A unset -> payload byte-identical:", canon(A0)==canon(A1))
if canon(A0)!=canon(A1): fails.append("A")
B1=run(ob1,"US, SR, nyse, nasdaq, tadawul")
for s,exp in (("1120.SR",None),("NVDA.US",None),("HDFCBANK.NS","Eligibility (Venue)"),("2317.TW","Eligibility (Venue)"),("OTIS",None),("XYZ",None)):
    g,v=ff(B1,s); print(f"  B {s:12} first_fail={g} verdict={v}")
    if exp and g!=exp: fails.append(f"B {s} {g}")
    if not exp and g=="Eligibility (Venue)": fails.append(f"B {s} wrongly failed venue")
print("  B GATE_ORDER index of venue gate:", ob1.GATE_ORDER.index("Eligibility (Venue)"), "(after KSA:", ob1.GATE_ORDER.index("Eligibility (KSA)"),")")
if ob1.GATE_ORDER.index("Eligibility (Venue)")!=ob1.GATE_ORDER.index("Eligibility (KSA)")+1: fails.append("order")
C1=run(ob1,"NSE"); g,v=ff(C1,"NVDA.US"); print("  C allow=NSE only -> NVDA first_fail:",g)
if g!="Eligibility (Venue)": fails.append("C")
print("HARNESS:","PASS" if not fails else "FAIL "+str(fails)); sys.exit(1 if fails else 0)
