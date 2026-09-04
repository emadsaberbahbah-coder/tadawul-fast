"""v5.136.0 R-6b harness — REAL module, three runs (H1 gate-off, H2 cross-process, H3 breaker)."""
import os, sys, json, subprocess, time
REPO="/tmp/tadawul-fast-main"; sys.path.insert(0, REPO)
mode=sys.argv[1]
def load():
    import importlib
    return importlib.import_module("core.data_engine_v2")
def row_ok(sym, name, fp12):
    return {"symbol": sym, "name": name, "current_price": 100.0, "price": 100.0,
            "forecast_price_12m": fp12, "target_mean_price": fp12, "forecast_source": "provider_target", "warnings": ""}
def row_missing(sym, name):
    return {"symbol": sym, "name": name, "current_price": 100.0, "price": 100.0, "warnings": ""}
fails=[]
def check(cond, msg):
    print(("PASS " if cond else "FAIL ")+msg); 
    if not cond: fails.append(msg)

if mode=="H1":
    # gate ON, redis layer OFF (default) -> byte-identical memory-only
    os.environ["TFB_ENGINE_TARGET_KLG"]="1"; os.environ.pop("TFB_ENGINE_TARGET_KLG_REDIS",None); os.environ["REDIS_URL"]="redis://127.0.0.1:6390/0"
    m=load()
    check(m.__version__=="5.136.0","version 5.136.0")
    check(m._tgt_lkg_capture(row_ok("H1TEST.US","H1 Test Co",123.4)) is True,"capture (memory) True")
    r=row_missing("H1TEST.US","H1 Test Co"); check(m._tgt_lkg_restore(r) is True and r.get("forecast_price_12m")==123.4 and r.get("forecast_source")=="provider_target","restore from memory")
    check(m._TGT_LKG_REDIS_STATE["client"] is None and m._tgt_lkg_redis_state_label()=="off","no client constructed, state=off")
    check(m._tgt_lkg_redis_set("H1TEST.US",{"ts":time.time(),"name":"x","fp12":1.0}) is False and m._tgt_lkg_redis_get("H1TEST.US") is None,"L2 set/get refuse when OFF")
    b=m.surface_gate_states(); check(b.get("engine_target_klg") is True and b.get("engine_target_klg_redis") is False and b.get("tgt_lkg_redis_state")=="off" and isinstance(b.get("tgt_lkg_redis_stats"),dict),"banner keys present")
    # both gates OFF -> dead code
    os.environ["TFB_ENGINE_TARGET_KLG"]="0"
    check(m._tgt_lkg_capture(row_ok("H1DEAD.US","Dead Co",50.0)) is False and m._tgt_lkg_restore(row_missing("H1DEAD.US","Dead Co")) is False,"master gate OFF => capture/restore False (dead code)")
elif mode=="H2A":
    os.environ["TFB_ENGINE_TARGET_KLG"]="1"; os.environ["TFB_ENGINE_TARGET_KLG_REDIS"]="1"; os.environ["TFB_ENGINE_TARGET_KLG_TTL_H"]="168"; os.environ["REDIS_URL"]="redis://127.0.0.1:6390/0"
    m=load()
    check(m._tgt_lkg_capture(row_ok("H2TEST.US","H2 Test Company Inc",123.4)) is True,"A: capture True")
    st=m._tgt_lkg_redis_stats(); check(st["writes"]==1 and st["errors"]==0 and m._tgt_lkg_redis_state_label()=="ok","A: L2 write=1 errors=0 state=ok")
    import redis; c=redis.Redis.from_url(os.environ["REDIS_URL"],decode_responses=True)
    ttl=c.ttl("tfb:tgt_lkg:v1:H2TEST.US"); raw=json.loads(c.get("tfb:tgt_lkg:v1:H2TEST.US"))
    check(168*3600-5<=ttl<=168*3600 and raw["fp12"]==123.4 and raw["name"]=="H2 Test Company Inc","A: key ttl~168h payload schema ok (ttl=%d)"%ttl)
elif mode=="H2B":
    os.environ["TFB_ENGINE_TARGET_KLG"]="1"; os.environ["TFB_ENGINE_TARGET_KLG_REDIS"]="1"; os.environ["TFB_ENGINE_TARGET_KLG_TTL_H"]="168"; os.environ["REDIS_URL"]="redis://127.0.0.1:6390/0"
    m=load()
    check(len(m._TGT_LKG_STORE)==0,"B: fresh process, memory store empty")
    r=row_missing("H2TEST.US","H2 Test Company Inc")
    check(m._tgt_lkg_restore(r) is True and r["forecast_price_12m"]==123.4 and r["forecast_source"]=="provider_target" and "analyst_lkg:0h" in r["warnings"],"B: cross-process restore via L2 (tag analyst_lkg:0h)")
    st=m._tgt_lkg_redis_stats(); check(st["hits"]==1 and st["errors"]==0,"B: hits=1 errors=0")
    check("H2TEST.US" in m._TGT_LKG_STORE,"B: memory hydrated from L2")
    # identity guard still decides: different name -> refusal
    r2=row_missing("H2TEST.US","Completely Different Name Ltd"); m._TGT_LKG_STORE.pop("H2TEST.US",None)
    check(m._tgt_lkg_restore(r2) is False and "forecast_price_12m" not in r2,"B: identity guard refuses cross-name carry (unchanged v5.131.0 guard)")
    # real seam: _phase_ii_quality_forecast on a row with failed target leg
    r3=row_missing("H2TEST.US","H2 Test Company Inc"); m._TGT_LKG_STORE.pop("H2TEST.US",None)
    try:
        m._phase_ii_quality_forecast(r3); seam_ok=True
    except Exception as e:
        seam_ok=False; print("seam exception:",repr(e))
    check(seam_ok and r3.get("forecast_price_12m")==123.4 and r3.get("forecast_source")=="provider_target","B: REAL seam _phase_ii_quality_forecast restores via L2 (src=%s fp12=%s roi12=%s)"%(r3.get("forecast_source"),r3.get("forecast_price_12m"),r3.get("expected_roi_12m")))
    # unknown symbol -> miss, no exception
    r4=row_missing("H2NONE.US","Nobody Inc"); check(m._tgt_lkg_restore(r4) is False and m._tgt_lkg_redis_stats()["misses"]==1,"B: L2 miss counted, refusal clean")
elif mode=="H3":
    os.environ["TFB_ENGINE_TARGET_KLG"]="1"; os.environ["TFB_ENGINE_TARGET_KLG_REDIS"]="1"; os.environ["REDIS_URL"]="redis://127.0.0.1:6399/0"  # dead port
    m=load(); t0=time.time()
    for i in range(3):
        check(m._tgt_lkg_capture(row_ok("H3TEST%d.US"%i,"H3 Co %d"%i,10.0+i)) is True,"breaker: capture #%d still True (memory)"%(i+1))
    dt=time.time()-t0; st=m._tgt_lkg_redis_stats()
    check(st["errors"]==3 and st["breaker_trips"]==1 and m._tgt_lkg_redis_state_label()=="breaker" and m._TGT_LKG_REDIS_STATE["client"] is None,"breaker open after 3 errors (errors=%d trips=%d state=%s)"%(st["errors"],st["breaker_trips"],m._tgt_lkg_redis_state_label()))
    check(dt<3.0,"3 failed attempts bounded by timeouts (%.2fs)"%dt)
    r=row_missing("H3TEST0.US","H3 Co 0"); check(m._tgt_lkg_restore(r) is True and r["forecast_price_12m"]==10.0,"restore memory-only while breaker open")
    check(m._tgt_lkg_capture(row_ok("H3TEST9.US","H3 Co 9",99.0)) is True and m._tgt_lkg_redis_stats()["errors"]==3,"while breaker open: no further attempts, capture still True")
    m._TGT_LKG_REDIS_STATE["breaker_until"]=0.0  # simulate 300s elapsed
    m._tgt_lkg_capture(row_ok("H3TEST8.US","H3 Co 8",88.0)); check(m._tgt_lkg_redis_stats()["errors"]==4,"after breaker window: one retry, error counted, no exception")
    b=m.surface_gate_states(); check(b.get("tgt_lkg_redis_state")=="degraded" and b["tgt_lkg_redis_stats"]["errors"]==4,"banner reflects degraded state + error counter (state=%s)"%b.get("tgt_lkg_redis_state"))
elif mode=="H3B":
    os.environ["TFB_ENGINE_TARGET_KLG"]="1"; os.environ["TFB_ENGINE_TARGET_KLG_REDIS"]="1"; os.environ["REDIS_URL"]="redis://10.255.255.1:6379/0"  # non-routable -> connect timeout path
    m=load(); t0=time.time()
    for i in range(3): m._tgt_lkg_capture(row_ok("H3B%d.US"%i,"H3B Co %d"%i,10.0))
    dt=time.time()-t0; st=m._tgt_lkg_redis_stats()
    check(st["errors"]==3 and st["breaker_trips"]==1,"non-routable host: 3 errors -> breaker (errors=%d)"%st["errors"])
    check(dt<2.0,"3 attempts bounded by 250ms timeouts (%.2fs)"%dt)
print("RESULT",mode,"FAIL" if fails else "PASS",len(fails),"failures")
sys.exit(1 if fails else 0)
