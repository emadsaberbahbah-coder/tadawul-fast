import sys, os, importlib.util, io, contextlib
from unittest.mock import MagicMock
sys.path.insert(0,"/home/claude/repo/tadawul-fast-main")
def load(path,name):
    spec=importlib.util.spec_from_file_location(name,path); m=importlib.util.module_from_spec(spec); sys.modules[name]=m
    try: spec.loader.exec_module(m)
    except ModuleNotFoundError as e:
        missing=str(e).split("'")[1]; mm=MagicMock(); mm.__name__=missing; sys.modules[missing]=mm; return load(path,name)
    return m
for k in list(os.environ):
    if k.startswith(("DIGEST_","TFB_","TARGET_SHEET")): os.environ.pop(k)
os.environ["DIGEST_MANIFEST_PAGES"]="Market_Leaders,Global_Markets,Commodities_FX,Mutual_Funds"
M=load("/home/claude/sd210.py","sd210"); assert M.__version__=="2.1.0"
from datetime import datetime
now=datetime.now(M.RIYADH_TZ).strftime("%Y-%m-%d %H:%M:%S+03:00")
def stamp(page,data): return [page,now,"SUCCESS",f"[STATUS-STAMP v6.54.0] leg=success written=1 | data={data} guard=pw:0/1,rb:0/1 sha=abcd1234 run=1","backend:run_dashboard_sync","","1","115","1","0"]
TODAY_STATUS=[["Page","Last Updated","Status","Message","Endpoint","HTTP Code","Rows","Columns","Duration ms","Warnings"],
              stamp("Market_Leaders","COMPLETE"),stamp("Global_Markets","PARTIAL"),stamp("Commodities_FX","PARTIAL"),stamp("Mutual_Funds","PARTIAL")]
ALL_COMPLETE=[TODAY_STATUS[0]]+[stamp(p,"COMPLETE") for p in ("Market_Leaders","Global_Markets","Commodities_FX","Mutual_Funds")]
GLOBALS_NA=[["Global Key","Value"],["Backend URL","https://x"],["TFB Decision Feed","NOT_ACTIONABLE(aged:GM) | run=33358823331 | 2026-08-31 08:28:00+03:00 | ML:OK GM:AGED CFX:OK MF:OK"],
            ["TFB Grid Capacity","AT-LIMIT | allocated=9,999,977 (99.99%) | free=23 | run=1 | t"],["TFB Calibration","INVESTABLE:0.814,WATCHLIST:1.147 | n=INVESTABLE=421 WATCHLIST=4975 | brier=0.2675 | as_of=t"]]
GLOBALS_EX=[["Global Key","Value"],["TFB Decision Feed","EXECUTABLE | run=1 | t | ML:OK GM:OK CFX:OK MF:OK"]]
TOP10=[["TOP 10 INVESTMENTS — DECISION"],["Status:","Last run 2026-08-31 08:47:17 | status: ok | sheets pool 9786 rows"],[],[],
       ["Deployable (SAR)","Exp. Gain 12M (SAR)","Selected","Blended Reliability","Blended R/R (TP2)","Scanned","Passed","Unallocated (SAR)"],
       ["3218.0","0.0","0 / 10","","","9786.0","0.0","3218.0"],[],["⛔ FEED NOT ACTIONABLE — aged:GM — SELECTED — 0 EXECUTABLE"],[],
       ["ALERTS (3)"],["Type","Count","Required Action"],["rotation_proposal","1","2503.T is fundable by trim of SHG.US 9,982 SAR"],["capital_call","2","Deposit ≥ 18,200 SAR"],["missing_valuation","350","x"],[]]
PAGE=[["Symbol","Name","Current Price","Recommendation","Investability Status","Last Updated (UTC)"],["ZZZ","Zed","10","HOLD","WATCHLIST",now]]
def make_reader(status_rows,globals_rows,top10_rows):
    def rr(sid,rng):
        if "'_Status'!L1" in rng: return globals_rows
        if "'_Status'" in rng: return status_rows
        if "Top_10_Investments" in rng: return top10_rows
        if "_Portfolio_CostBasis" in rng: return [["Symbol","Status"]]
        return PAGE
    return rr
def run(reader):
    M._get_read_range=lambda: reader
    buf=io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        rc=M.main(["--sheet-id","SID","--session","09:00 Riyadh","--dry-run"])
    return rc, buf.getvalue()
# T1 today: manifest FAIL (PARTIAL pages) + feed NOT_ACTIONABLE -> state brief, no picks, exit 0
rc,out=run(make_reader(TODAY_STATUS,GLOBALS_NA,TOP10))
assert rc==0, (rc,out[-600:])
assert "NOT ACTIONABLE (aged:GM)" in out and "DECISION SURFACE: NOT ACTIONABLE" in out
assert "AT-LIMIT | allocated=9,999,977" in out and "INVESTABLE:0.814" in out and "Scanned=9786.0" in out and "Passed=0.0" in out
assert "rotation_proposal=1" in out and "capital_call=2" in out
assert "SHG.US" not in out and "2503.T" not in out and "BUY " not in out and "SELL " not in out and "trim" not in out, "action language / symbols leaked into the research-only brief"
assert "Research only" in out
print("T1 PASS today: state brief sent (feed NOT_ACTIONABLE, manifest FAIL), exit 0, capacity+calibration+Top10 KPIs+alert types shown, no symbols/action verbs")
# T2 manifest PASS but feed NOT_ACTIONABLE -> feed wins (AT-07)
rc,out=run(make_reader(ALL_COMPLETE,GLOBALS_NA,TOP10)); assert rc==0 and "NOT ACTIONABLE" in out and "manifest: PASS" in out and "Picks follow" not in out
print("T2 PASS manifest PASS + feed NOT_ACTIONABLE -> still not actionable (feed is the authority)")
# T3 manifest PASS + feed EXECUTABLE -> pick flow with state block prepended
rc,out=run(make_reader(ALL_COMPLETE,GLOBALS_EX,TOP10)); assert rc==0 and "DECISION SURFACE: EXECUTABLE" in out and "Picks follow below." in out, out[-800:]
print("T3 PASS actionable: state block prepended to the v2.0.0 pick email")
# T4 feed key absent -> manifest decides (v2.0.0 semantics)
rc,out=run(make_reader(ALL_COMPLETE,[["Global Key","Value"]],TOP10)); assert rc==0 and "DECISION SURFACE: EXECUTABLE" in out
rc,out=run(make_reader(TODAY_STATUS,[["Global Key","Value"]],TOP10)); assert rc==0 and "NOT ACTIONABLE (manifest)" in out
print("T4 PASS absent feed key -> manifest-only decision (v2.0.0 semantics)")
# T5 kill-switch -> v2.0.0 verbatim: incident notice + DIGEST_FAIL_EXIT (3)
os.environ["DIGEST_STATE_BRIEF"]="0"; rc,out=run(make_reader(TODAY_STATUS,GLOBALS_NA,TOP10)); os.environ.pop("DIGEST_STATE_BRIEF")
assert rc==3 and "RESEARCH ONLY (manifest FAIL)" in out, (rc,out[-300:])
print("T5 PASS kill-switch: v2.0.0 incident + exit 3")
# T6 DIGEST_STATE_EXIT honoured; globals unreadable -> fail-open to manifest
os.environ["DIGEST_STATE_EXIT"]="4"; rc,_=run(make_reader(TODAY_STATUS,GLOBALS_NA,TOP10)); os.environ.pop("DIGEST_STATE_EXIT"); assert rc==4
def boom(sid,rng):
    if "'_Status'!L1" in rng: raise RuntimeError("no L:M")
    return make_reader(ALL_COMPLETE,[],TOP10)(sid,rng)
rc,out=run(boom); assert rc==0 and "DECISION SURFACE: EXECUTABLE" in out
print("T6 PASS DIGEST_STATE_EXIT honoured; unreadable globals -> fail-open to manifest")
# T7 pure parsers
assert M._decision_feed_state({"tfb decision feed":"NOT_ACTIONABLE(partial:GM) | run=1"})[:2]==("NOT_ACTIONABLE","partial:GM")
assert M._decision_feed_state({"tfb decision feed":"EXECUTABLE | run=1"})[0]=="EXECUTABLE" and M._decision_feed_state({})[0]=="UNKNOWN"
print("T7 PASS feed parser")
print("PASS 7/7 — real send_digest v2.1.0 main() end-to-end (dry-run)")
