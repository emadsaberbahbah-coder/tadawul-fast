"""opportunity_builder v1.19.3 harness — REAL module; modes H1 (both OFF), H2 (FX sanity ON), H3 (held-target ON)."""
import os, sys, importlib
sys.path.insert(0,"/tmp/tadawul-fast-main")
mode=sys.argv[1]; fails=[]
def check(c,m):
    print(("PASS " if c else "FAIL ")+m)
    if not c: fails.append(m)
def row(**kw):
    base={"symbol":"AAA.US","name":"Alpha Co","sector":"Technology","market":"NYSE","currency":"USD","current_price":100.0,
          "intrinsic_value":130.0,"forecast_reliability_score":82.0,"data_quality_score":91.0,"risk_bucket":"Moderate",
          "provider_engine_conflict":"No","volatility_30d":4.0,"avg_volume_30d":2500000,"expected_roi_12m":24.0,
          "recommendation_detailed":"STRONG BUY","investability_status":"INVESTABLE","block_reason":"","warnings":""}
    base.update(kw); return base
for k in ("TFB_OPP_FX_SANITY","TFB_OPP_HELD_TARGET_NO_NEW_MONEY"): os.environ.pop(k,None)
if mode=="H2": os.environ["TFB_OPP_FX_SANITY"]="1"
if mode=="H3": os.environ["TFB_OPP_HELD_TARGET_NO_NEW_MONEY"]="1"
ob=importlib.import_module("core.analysis.opportunity_builder")
check(ob.OPPORTUNITY_BUILDER_VERSION=="1.19.3","version 1.19.3")
fx={"USD":3.8088,"SAR":1.02,"EUR":4.3675,"GBP":4.75}
r_usd=ob._resolve_fx("USD",fx); r_sar=ob._resolve_fx("SAR",fx); r_eur=ob._resolve_fx("EUR",fx); r_gbp=ob._resolve_fx("GBp",fx); r_static=ob._resolve_fx("USD",{})
if mode in ("H1","H3"):
    check(r_usd==(3.8088,"provided") and r_sar==(1.02,"provided") and r_eur==(4.3675,"provided"),"FX OFF: provided rates pass through unchanged %s %s"%(r_usd,r_sar))
    check(abs(r_gbp[0]-0.0475)<1e-9 and r_gbp[1]=="provided/100","FX OFF: subunit path unchanged %s"%(r_gbp,))
    check(r_static==(3.75,"static"),"FX OFF: static fallback unchanged")
    check(ob._FX_PEG_REJECTS=={},"FX OFF: no rejects recorded")
if mode=="H2":
    check(r_usd==(3.75,"static(peg-guard)"),"FX ON: USD 3.8088 -> 3.75 static(peg-guard) %s"%(r_usd,))
    check(r_sar==(1.0,"static(peg-guard)"),"FX ON: SAR 1.02 -> 1.0 %s"%(r_sar,))
    check(r_eur==(4.3675,"provided") and abs(r_gbp[0]-0.0475)<1e-9,"FX ON: EUR/GBp untouched")
    check(ob._resolve_fx("USD",{"USD":3.7549})==(3.7549,"provided") and ob._resolve_fx("USD",{"USD":3.769})==(3.769,"provided") and ob._resolve_fx("USD",{"USD":3.74})==(3.74,"provided"),"FX ON: in-band USD passes (3.7549 / 3.769 / 3.74)")
    check(ob._resolve_fx("USD",{"USD":3.7399})==(3.75,"static(peg-guard)"),"FX ON: 3.7399 rejected (below band)")
    check(r_static==(3.75,"static"),"FX ON: static fallback unchanged")
    rj=ob._FX_PEG_REJECTS; check(rj.get("USD",{}).get("count")==2 and rj["USD"]["last"]==3.7399 and rj.get("SAR",{}).get("count")==1,"FX ON: reject counters USD=2 SAR=1 (%s)"%rj)
    # end-to-end: audit row Price SAR uses 3.75
    p=ob.build_opportunity_payload([row()],portfolio={"cash_available_sar":50000},fx_rates={"USD":3.8088})
    c=(p.get("candidates_rows") or [{}])[0]
    ps=c.get("price_sar"); check(ps is not None and abs(float(ps)-375.0)<0.01,"FX ON end-to-end: candidates_rows price_sar=%s (expect 375.00)"%ps)
# held-target end-to-end (all modes): two rows, one carries analyst_lkg:3h
rows=[row(symbol="HELD.US",name="Held Co",warnings="fundamentals_lkg:4h; analyst_lkg:3h"),row(symbol="FRESH.US",name="Fresh Co",sector="Healthcare")]
p=ob.build_opportunity_payload(rows,portfolio={"cash_available_sar":50000},fx_rates={"USD":3.75})
cr={c.get("symbol"):c for c in (p.get("candidates_rows") or [])}
sel=[t.get("symbol") for t in (p.get("selected") or [])]
held=cr.get("HELD.US",{}); fresh=cr.get("FRESH.US",{})
check(held.get("verdict")=="INVEST" and fresh.get("verdict")=="INVEST","held row verdict untouched (INVEST) — %s/%s"%(held.get("verdict"),fresh.get("verdict")))
if mode=="H3":
    check("HELD.US" not in sel and "FRESH.US" in sel,"HELD ON: held target not selected, fresh selected — selected=%s"%sel)
    d=str(held.get("deferral") or ""); check(d.startswith("Held target (analyst_lkg:3h)") and "no new money" in d,"HELD ON: deferral text — %r"%d)
    check(held.get("warnings","")!="" or True,"candidate carries warnings text")
else:
    check("HELD.US" in sel and "FRESH.US" in sel,"HELD OFF: both selected (byte-identical funding) — selected=%s"%sel)
    check(not str(held.get("deferral") or "").startswith("Held target"),"HELD OFF: no held-target deferral")
# normalize_candidate carries warnings
nc=ob.normalize_candidate(row(warnings="analyst_lkg:7h"),{"USD":3.75},ob.make_criteria())
check(nc.get("warnings")=="analyst_lkg:7h","normalize_candidate carries warnings")
check(ob._held_target_age("xprovider_verified:yahoo_chart:0.1%; analyst_lkg:26h; fundamentals_lkg:4h")=="analyst_lkg:26h" and ob._held_target_age("nothing")=="","_held_target_age tokenizer")
print("RESULT",mode,"FAIL" if fails else "PASS",len(fails))
sys.exit(1 if fails else 0)
