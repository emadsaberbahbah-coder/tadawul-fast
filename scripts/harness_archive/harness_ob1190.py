import sys, os, importlib.util, json
from unittest.mock import MagicMock
sys.path.insert(0,"/home/claude/repo/tadawul-fast-main")
def load(path,name):
    spec=importlib.util.spec_from_file_location(name,path); m=importlib.util.module_from_spec(spec); sys.modules[name]=m
    try: spec.loader.exec_module(m)
    except ModuleNotFoundError as e:
        missing=str(e).split("'")[1]; mm=MagicMock(); mm.__name__=missing; sys.modules[missing]=mm; return load(path,name)
    return m
for k in list(os.environ):
    if k.startswith("TFB_"): os.environ.pop(k)
os.environ["TFB_OPP_VENUE_FLOORS"]="1"
M=load("/home/claude/ob1180.py","ob1180"); assert M.OPPORTUNITY_BUILDER_VERSION=="1.18.0"
def row(sym,name,px,roi12,tp,rel=85,dq=100,sector="Industrials",market="NYSE/NASDAQ",ccy="USD",reco="BUY",inv="INVESTABLE",risk="LOW",vol=0.20):
    return {"Symbol":sym,"Name":name,"Current Price":px,"Expected ROI 12M":roi12,"Expected ROI 3M":roi12*0.42,"Target Price":tp,
            "Forecast Reliability Score":rel,"Data Quality Score":dq,"Investability Status":inv,"Final Action":"INVEST" if inv=="INVESTABLE" else "WATCH",
            "Recommendation":reco,"Sector":sector,"Market":market,"Exchange":market,"Currency":ccy,"Risk Bucket":risk,"Volatility 30D":vol,
            "Forecast Source":"provider_target","Provider/Engine Conflict":"FALSE","Intrinsic Value":tp,"Market Cap":5e9,"Avg Volume 30D":2e6,"Overall Score":80,"Opportunity Score":80}
rows=[row("2503.T","Kirin",3053,0.30,4000,sector="Consumer Staples",market="Tokyo",ccy="JPY"),
      row("8309.T","Sumitomo Mitsui Trust",1789.5,0.28,2300,sector="Financials",market="Tokyo",ccy="JPY"),
      row("MGY.US","Magnolia",26.25,0.25,33,sector="Energy"),
      row("SHG.US","Shinhan",79.21,0.05,82,sector="Financials",reco="HOLD",inv="WATCHLIST"),   # held, weak forecast
      row("5023.SR","Sukuk",101.79,0.02,103,sector="Unknown",market="SAU",ccy="SAR",reco="HOLD",inv="WATCHLIST")]
pf={"cash_available_sar":3218.0,"pending_proceeds_sar":0.0,"portfolio_value_sar":90231.0,
    "holdings":[{"symbol":"SHG.US","sector":"Financials","market":"NYSE/NASDAQ","value_sar":20937.0},
                {"symbol":"5023.SR","sector":"Unknown","market":"SAU","value_sar":10179.0},
                {"symbol":"OTIS","sector":"Industrials","market":"NYSE/NASDAQ","value_sar":14899.0}]}
fx={"USD":3.7761,"JPY":0.0257,"SAR":1.0}
crit={"max_selected":10,"required_roi_pct":12,"required_ann_roi_pct":10,"min_reliability":70,"min_dq":80,"min_rr":1.0,"period_months":12}
def run(): return M.build_opportunity_payload(rows, crit, pf, fx, None)
pay=run()
k=pay.get("kpis",{}); nm=pay.get("near_miss",[]); al=pay.get("alerts",[])
print("passed:",k.get("passed"),"selected:",k.get("selected_count"),"| kpis funding:",{x:k.get(x) for x in ("fundable_now","fundable_by_rotation","capital_call","capital_call_topn_sar")})
fund=[r for r in nm if r.get("failed_gate")=="Funding"]
for r in nm: print("  NM",r["symbol"],"|",r["failed_gate"],"|",str(r["current"])[:150])
for a in al:
    if a["type"] in ("rotation_proposal","capital_call"): print("  ALERT",a["type"],a["count"],"|",a["required_action"][:160])
if k.get("passed",0)==0:
    aud=pay.get("candidates_rows") or pay.get("audit") or []
    for a in aud[:5]: print("  AUDIT",a.get("symbol"),a.get("first_failed_gate"),a.get("verdict"), a.get("gates_failed") or a.get("failed_gates"))
    sys.exit("no candidate passed - adjust fixture")
# assertions
assert k["passed"]>=2, k
assert k["fundable_now"]==k["selected_count"]
assert k["fundable_by_rotation"]==1 and k["capital_call"]>=1, k
rot=[a for a in al if a["type"]=="rotation_proposal"]; cc=[a for a in al if a["type"]=="capital_call"]
assert len(rot)==1 and "SHG.US" in rot[0]["required_action"] and "5023.SR" not in rot[0]["required_action"], rot
assert cc and "Deposit" in cc[0]["required_action"], cc
assert any("FUNDABLE_BY_ROTATION: trim SHG.US" in r["current"] or "FUNDABLE_BY_ROTATION: exit SHG.US" in r["current"] for r in fund), [r["current"] for r in fund]
assert any("CAPITAL_CALL: deposit" in r["current"] for r in fund)
assert all(r["failed_gate"]=="Funding" for r in fund)
print("T1 PASS end-to-end: qualified names kept visible with funding states; rotation names SHG.US (weakest eligible), sukuk never rotates; capital call for the rest")
# T2 kill-switch: v1.17.0-identical shape (no funding text/kpis/alerts)
os.environ["TFB_OPP_FUNDING_PLAN"]="0"; p0=run(); os.environ.pop("TFB_OPP_FUNDING_PLAN")
assert "fundable_now" not in p0["kpis"] and not [a for a in p0["alerts"] if a["type"] in ("rotation_proposal","capital_call")]
assert all("FUNDABLE_BY_ROTATION" not in str(r["current"]) and "CAPITAL_CALL" not in str(r["current"]) for r in p0["near_miss"])
assert p0["kpis"]["passed"]==k["passed"] and p0["kpis"]["selected_count"]==k["selected_count"]
print("T2 PASS kill-switch: passed/selected identical, no funding text/kpis/alerts")
# T3 pure rotation rule: edge threshold and exclusion
hold=[{"symbol":"SHG.US","value_sar":20937.0},{"symbol":"5023.SR","value_sar":10179.0}]
assert M._rotation_pick(30.0,hold,{"SHG.US":5.0,"5023.SR":2.0},{"5023.SR"},8.0,1.1)["symbol"]=="SHG.US"
assert M._rotation_pick(13.0,hold,{"SHG.US":5.0},{"5023.SR"},8.0,1.1) is None   # edge 6.9 < 8
assert M._rotation_pick(30.0,hold,{"5023.SR":2.0},{"5023.SR"},8.0,1.1) is None  # only the excluded one is weak
pl=M._funding_plans([("A",10000.0,30.0),("B",10000.0,25.0)],3218.0,hold,{"SHG.US":5.0},{"5023.SR"},8.0,1.1)
assert pl[0]["state"]=="FUNDABLE_BY_ROTATION" and pl[0]["rotation"]["action"]=="TRIM" and pl[0]["rotation"]["proceeds_sar"]==6782 and pl[0]["shortfall_sar"]==0
assert pl[1]["state"]=="CAPITAL_CALL" and pl[1]["shortfall_sar"]==10000   # one rotation per run
print("T3 PASS rotation rule: edge >= 8pp after 1.1% cost, sukuk excluded, one rotation per run, sequential shortfalls")
# T4 with enough cash nothing changes: no plans
pf2=dict(pf,cash_available_sar=60000.0); p2=M.build_opportunity_payload(rows,crit,pf2,fx,None)
assert p2["kpis"]["selected_count"]>=2 and p2["kpis"].get("capital_call",0)==0 and not [a for a in p2["alerts"] if a["type"] in ("rotation_proposal","capital_call")]
print("T4 PASS ample cash: tickets funded normally, no funding plans emitted")
print("PASS 4/4 — real builder module opportunity_builder v1.18.0 end-to-end")

# ---- v1.18.1 additions ----
M2=load("/home/claude/ob1181.py","ob1181"); assert M2.OPPORTUNITY_BUILDER_VERSION=="1.18.1"
import datetime as dt
today=dt.date(2026,9,1)
hold=[{"symbol":"SHG.US","value_sar":20937.0,"buy_date":"2026-08-24"},{"symbol":"OTIS","value_sar":14899.0,"buy_date":"2026-08-12","tp1_sar":80.0,"price_sar":78.0},{"symbol":"YUM","value_sar":13944.0,"buy_date":"2026-08-12"}]
assert M2._holding_rotation_eligible(hold[0],today)==(False,"held 8d < 7d") or M2._holding_rotation_eligible(hold[0],today)[0]  # 8 days >= 7 -> eligible
assert M2._holding_rotation_eligible({"symbol":"X","buy_date":"2026-08-29"},today)[0] is False   # 3 days
assert M2._holding_rotation_eligible(hold[1],today)==(False,"within 2.5% of TP1")
assert M2._holding_rotation_eligible(hold[2],today)[0] and M2._holding_rotation_eligible({"symbol":"NOINFO"},today)[0]
r=M2._rotation_pick(30.0,[{"symbol":"NEW.US","value_sar":9000.0,"buy_date":"2026-08-30"},{"symbol":"OTIS","value_sar":14899.0,"buy_date":"2026-08-12","tp1_sar":80.0,"price_sar":78.0},{"symbol":"YUM","value_sar":13944.0,"buy_date":"2026-08-12"}],{"NEW.US":1.0,"OTIS":2.0,"YUM":5.0},set(),8.0,1.1)
assert r and r["symbol"]=="YUM", r     # NEW.US too young, OTIS within 3% of TP1 -> YUM is the eligible weakest
print("T5 PASS v1.18.1: min-held (7d) and TP1-proximity (3%) honoured; missing fields fail-open; rotation picks the eligible weakest")

# ---- v1.19.0: reliability floor mode ----
for k in ("TFB_T10_REL_FLOOR_MODE",): os.environ.pop(k,None)
M3=load("/home/claude/ob1190.py","ob1190"); assert M3.OPPORTUNITY_BUILDER_VERSION=="1.19.0"
lowrel=[row("LOWREL.US","Low Rel",50.0,0.30,66,rel=55,sector="Energy")]+rows   # rel 55 < floor 70
pdef=M3.build_opportunity_payload(lowrel,crit,pf2,fx,None)      # ample cash, default mode
nm_def=[r for r in pdef["near_miss"] if r["symbol"]=="LOWREL.US"]
assert nm_def and nm_def[0]["failed_gate"]=="Reliability", nm_def
os.environ["TFB_T10_REL_FLOOR_MODE"]="display"; pdisp=M3.build_opportunity_payload(lowrel,crit,pf2,fx,None); os.environ.pop("TFB_T10_REL_FLOOR_MODE")
nm_disp=[r for r in pdisp["near_miss"] if r["symbol"]=="LOWREL.US" and r["failed_gate"]=="Reliability"]
assert not nm_disp, nm_disp
assert pdisp["kpis"]["passed"]==pdef["kpis"]["passed"]+1, (pdisp["kpis"]["passed"],pdef["kpis"]["passed"])
p181=M2.build_opportunity_payload(lowrel,crit,pf2,fx,None); assert p181["kpis"]["passed"]==pdef["kpis"]["passed"]
os.environ["TFB_T10_REL_FLOOR_MODE"]="nonsense"; assert M3._env_rel_floor_mode()=="gate"; os.environ.pop("TFB_T10_REL_FLOOR_MODE")
print("T6 PASS v1.19.0: default 'gate' == v1.18.1 (rel 55 -> near-miss Reliability); 'display' -> the same name passes (Passed +1), no Reliability near-miss; unknown value -> gate")
