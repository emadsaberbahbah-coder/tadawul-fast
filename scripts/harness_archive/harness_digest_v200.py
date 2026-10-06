import importlib.util, sys, os, datetime as dt
spec=importlib.util.spec_from_file_location("sd","send_digest.py")
sd=importlib.util.module_from_spec(spec); sys.modules["sd"]=sd; spec.loader.exec_module(sd)
NOW=dt.datetime.now(sd.RIYADH_TZ).replace(tzinfo=None)
def ts(h): return (NOW-dt.timedelta(hours=h)).strftime("%Y-%m-%d %H:%M:%S")
def stamp(page,data="COMPLETE",age=1.0,tstamp=True,run="r1"):
    msg=f"leg=full | data={data} guard=pw:4/6618 sha=abcd1234 run={run}"
    return [page,"SUCCESS", ts(age) if tstamp else "", msg,"","","","","",0]
PAGES=["Market_Leaders","Global_Markets","Commodities_FX","Mutual_Funds"]
def rr(grid=None, raise_=False):
    def f(sid,rng):
        if raise_: raise RuntimeError("boom")
        assert "_Status" in rng
        return grid or []
    return f
def run(tag):
    ok=tot=0
    def t(name,c):
        nonlocal ok,tot; tot+=1; assert c,name; ok+=1; print("PASS",name)
    os.environ.pop("DIGEST_REQUIRE_MANIFEST",None)
    g=[stamp(p) for p in PAGES]
    r=sd._manifest_gate(rr(g),"SID"); t("T1 PASS all COMPLETE fresh", r[0] and "Global_Markets=COMPLETE" in r[1])
    g2=[stamp(p) for p in PAGES if p!="Global_Markets"]
    r=sd._manifest_gate(rr(g2),"SID"); t("T2 missing page fails", not r[0] and any("Global_Markets: no backend" in f_ for f_ in r[2]))
    g3=[stamp(p, data=("PARTIAL" if p=="Commodities_FX" else "COMPLETE")) for p in PAGES]
    r=sd._manifest_gate(rr(g3),"SID"); t("T3 PARTIAL fails", not r[0] and any("Commodities_FX: data=PARTIAL"==f_ for f_ in r[2]))
    g4=[stamp(p, age=(9.0 if p=="Mutual_Funds" else 1.0)) for p in PAGES]
    r=sd._manifest_gate(rr(g4),"SID"); t("T4 stale fails", not r[0] and any("stale" in f_ for f_ in r[2]))
    g5=[stamp(p, tstamp=(p!="Market_Leaders")) for p in PAGES]
    r=sd._manifest_gate(rr(g5),"SID"); t("T5 unparseable ts fails", not r[0] and any("unparseable" in f_ for f_ in r[2]))
    g6=[stamp(p, age=(-3.0 if p=="Global_Markets" else 1.0)) for p in PAGES]
    r=sd._manifest_gate(rr(g6),"SID"); t("T6 future ts fails", not r[0] and any("future" in f_ for f_ in r[2]))
    r=sd._manifest_gate(rr(raise_=True),"SID"); t("T7 read error fail-closed", not r[0])
    t("T8a unstamped row STALE (default)", sd._is_stale({"updated":""})==True)
    os.environ["DIGEST_REQUIRE_MANIFEST"]="0"
    t("T8b escape restores v1.2.1", sd._is_stale({"updated":""})==False and sd._manifest_required()==False)
    os.environ.pop("DIGEST_REQUIRE_MANIFEST",None)
    subj,text,html=sd._compose_incident("10:45 Riyadh",["Global_Markets: data=PARTIAL"],"pages ...")
    t("T9 incident research-only", "RESEARCH ONLY" in subj and "BUY" not in text and "SELL" not in text and "no trade guidance" in text)
    t("T10 ISO ts parse", sd._parse_any_ts("2026-08-26T07:30:00+00:00") is not None and sd._parse_any_ts("garbage") is None)
    print(f"[{tag}] {ok}/{tot} PASS")
for i in (1,2,3): run(f"run{i}")
print("DIGEST HARNESS 3/3 CLEAN")
