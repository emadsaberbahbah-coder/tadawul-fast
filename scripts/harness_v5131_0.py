#!/usr/bin/env python3
"""v5.131.0 harness — W1A-0b TARGET-BLOCK LKG. REAL _phase_ii_quality_forecast,
Self-contained; pass paths as argv[1]=old argv[2]=new. Exits non-zero on any fail."""
import importlib.util, os, sys, copy, json
def load(path,name):
    spec=importlib.util.spec_from_file_location(name,path)
    m=importlib.util.module_from_spec(spec); sys.modules[name]=m; spec.loader.exec_module(m); return m
def target_row():
    return {"symbol":"AAPL","name":"Apple Inc.","current_price":100.0,
            "forecast_price_12m":130.0,"forecast_source":"provider_target","warnings":""}
def degraded_row(name="Apple Inc."):
    # Must carry enough score/intrinsic inputs that the OLD build
    # demonstrably reaches the phase_ii_synthetic stamp (the control
    # arm) — a six-field row bails out of the synthetic section with
    # NO source at all, which is not the production shape.
    return {"symbol":"AAPL","name":name,"current_price":100.0,
            "rsi_14":55.0,"volatility_30d":20.0,"pe_ttm":25.0,
            "intrinsic_value":115.0,"overall_score":60.0,
            "momentum_score":55.0,"quality_score":58.0,"warnings":""}
def main(oldp,newp):
    for k in list(os.environ):
        if k.startswith("TFB_"): os.environ.pop(k)
    old=load(oldp,'h_eng_old'); new=load(newp,'h_eng_new')
    assert old.__version__=="5.130.3" and new.__version__=="5.131.0"
    assert old._SAI_REQUIRED_VERSION==new._SAI_REQUIRED_VERSION=="1.4.1"
    # A: gate OFF => byte-identical, store untouched
    for mk in (target_row,degraded_row,lambda:{"symbol":"X","current_price":50.0,"warnings":""}):
        ro,rn=mk(),mk(); old._phase_ii_quality_forecast(ro); new._phase_ii_quality_forecast(rn)
        assert ro==rn, "A: OFF divergence"
    assert len(new._TGT_LKG_STORE)==0
    os.environ["TFB_ENGINE_TARGET_KLG"]="1"
    # B: capture
    new._phase_ii_quality_forecast(target_row())
    assert new._TGT_LKG_STORE["AAPL"]["fp12"]==130.0
    # C: carry (the feature) — new carries, old synthesizes
    ro,rn=degraded_row(),degraded_row()
    old._phase_ii_quality_forecast(ro); new._phase_ii_quality_forecast(rn)
    assert rn["forecast_source"]=="provider_target" and rn["forecast_price_12m"]==130.0
    assert "analyst_lkg:0h" in rn["warnings"] and abs(rn["expected_roi_12m"]-0.3)<1e-9
    # constant-free: the carry must derive EXACTLY what the OLD build derives
    # for a GENUINE provider target with the same fp12/cp.
    ref=target_row(); old._phase_ii_quality_forecast(ref)
    for k in ("forecast_price_3m","forecast_price_1m","expected_roi_3m","expected_roi_1m"):
        assert rn[k]==ref[k], (k, rn[k], ref[k])
    assert ro["forecast_source"]=="phase_ii_synthetic"
    # D: carried row never re-seeds
    ts=new._TGT_LKG_STORE["AAPL"]["ts"]; new._phase_ii_quality_forecast(copy.deepcopy(rn))
    assert new._TGT_LKG_STORE["AAPL"]["ts"]==ts
    # E: identity mismatch refuses
    r=degraded_row(name="Toyota Motor Corporation"); new._phase_ii_quality_forecast(r)
    assert r["forecast_source"]=="phase_ii_synthetic" and "analyst_lkg" not in r["warnings"]
    # F: taint refuses
    r=degraded_row(); r["warnings"]="identity_patch_refused"; new._phase_ii_quality_forecast(r)
    assert r["forecast_source"]=="phase_ii_synthetic"
    # G: TTL expiry pops + refuses
    new._TGT_LKG_STORE["AAPL"]["ts"]-=73*3600
    r=degraded_row(); new._phase_ii_quality_forecast(r)
    assert r["forecast_source"]=="phase_ii_synthetic" and "AAPL" not in new._TGT_LKG_STORE
    # H: ON + genuine target — honor path byte-identical
    ro,rn=target_row(),target_row(); old._phase_ii_quality_forecast(ro); new._phase_ii_quality_forecast(rn)
    assert ro==rn
    # tag scan-safety
    assert not any(s in "analyst_lkg:36h" for s in ("cap","forecast","target","roi","drop","reject"))
    print("SELFTEST 8/8 + tag: ALL GREEN")
if __name__=="__main__":
    main(sys.argv[1],sys.argv[2])
