import sys, os, importlib.util, csv
from unittest.mock import MagicMock
for k in ["google","google.oauth2","google.oauth2.service_account","googleapiclient","googleapiclient.discovery",
          "googleapiclient.errors","gspread","requests","httplib2","google_auth_httplib2","dotenv"]:
    if k not in sys.modules:
        try: __import__(k)
        except Exception:
            mm=MagicMock(); mm.__name__=k; sys.modules[k]=mm
sys.path.insert(0,"/home/claude/repo/tadawul-fast-main"); sys.path.insert(0,"/home/claude/repo/tadawul-fast-main/scripts")
spec=importlib.util.spec_from_file_location("rds654","/home/claude/sync654.py"); m=importlib.util.module_from_spec(spec); sys.modules["rds654"]=m
for k in ("TFB_SYNC_FALSE_GREEN_SCREEN","TFB_SYNC_KLG_IDENTITY_GATE"): os.environ.pop(k,None)
spec.loader.exec_module(m); assert m.SCRIPT_VERSION=="6.54.0"
U="/mnt/user-data/uploads/_Market_Share_Deepseek-V3_-_%s.tsv"
def load(p):
    with open(U%p,encoding="utf-8",newline="") as f:
        rows=list(csv.reader(f,delimiter="\t",quoting=csv.QUOTE_NONE))
    return rows[0],[r for r in rows[1:] if any(r)]
# T1 - domain regex vs the entire live universe
bad=[]; n=0
for p in ["Market_Leaders","Global_Markets","Commodities_FX","Mutual_Funds"]:
    h,rows=load(p); si=h.index("Symbol")
    for r in rows:
        s=r[si].strip()
        if not s: continue
        n+=1
        if not m._klg_symbol_domain_ok(s): bad.append(s)
assert bad==["Copper Futures"], bad
assert not m._klg_symbol_domain_ok("COPPER FUTURES") and m._klg_symbol_domain_ok("^GSPC") and m._klg_symbol_domain_ok("GC=F") and m._klg_symbol_domain_ok("ANDINA-B.SN")
print(f"T1 PASS domain regex: {n} live symbols, only failure = {bad}")
# T2 - false-green screen on the REAL pages
import copy
for p,exp in [("Commodities_FX",1),("Market_Leaders",0),("Global_Markets",0),("Mutual_Funds",0)]:
    h,rows=load(p); before=copy.deepcopy(rows); ai=h.index("Final Action"); ii=h.index("Investability Status"); bi=h.index("Block Reason"); wi=h.index("Warnings"); si=h.index("Symbol")
    out,st=m._apply_false_green_screen(h,rows,p)
    assert st["checked"]==len([r for r in rows if r[si].strip()]), st
    assert st["blocked"]==exp, (p,st)
    changed=[i for i,(a,b) in enumerate(zip(before,out)) if a!=b]
    assert len(changed)==exp, (p,len(changed))
    if exp:
        i=changed[0]; r=out[i]
        assert r[si]=="Copper Futures" and r[ai]=="DO_NOT_INVEST" and r[ii]=="BLOCKED" and r[bi].endswith("sync_false_green:identity_domain+fetch_failed") and r[wi].endswith("false_green_blocked:v6.54.0"), r[bi]
        assert all(a==b for j,(a,b) in enumerate(zip(before[i],r)) if j not in (ai,ii,bi,wi)), "other cells touched"
        assert st["domain"]==1 and st["fetchfail"]==1 and st["examples"]==["Copper Futures"]
    print(f"T2 PASS false-green screen {p}: checked={st['checked']} blocked={st['blocked']} INVEST after={sum(1 for r in out if r[ai]=='INVEST')}")
# T3 - kill-switch: matrix byte-identical
os.environ["TFB_SYNC_FALSE_GREEN_SCREEN"]="0"; assert m._false_green_screen_enabled() is False; os.environ.pop("TFB_SYNC_FALSE_GREEN_SCREEN")
assert m._false_green_screen_enabled() is True
print("T3 PASS kill-switch semantics")
# T4 - REAL _keep_last_good_rows: old grid holds a 'COPPER FUTURES' row that passes every pre-existing keep-leg; fresh stub for it + a good ticker
h=["Symbol","Name","Current Price","Data Provider","EPS (TTM)","P/E (TTM)"]
old_grid=[h,["COPPER FUTURES","Commodity","6.597","eodhd","",""],["HG=F","Copper Futures Mar 26","4.41","eodhd","",""]]
class SW:
    def read_values(self,sid,sheet,rng): return old_grid
fresh=[["COPPER FUTURES","","","fallback_error","",""],["HG=F","","","fallback_error","",""]]
out,swapped=m._keep_last_good_rows(SW(),"sid","Commodities_FX",h,[list(r) for r in fresh])
assert swapped==["HG=F"], swapped
assert list(m._LAST_KLG_ID_SUSPECTS)==["COPPER FUTURES"], m._LAST_KLG_ID_SUSPECTS
assert out[0]==fresh[0] and out[1][2]=="4.41", out
print("T4 PASS KLG Leg 0: 'COPPER FUTURES' never certified last-GOOD (named suspect); HG=F still restored")
# T5 - Leg 0 obeys the existing identity-gate kill-switch
os.environ["TFB_SYNC_KLG_IDENTITY_GATE"]="0"
out,swapped=m._keep_last_good_rows(SW(),"sid","Commodities_FX",h,[list(r) for r in fresh]); os.environ.pop("TFB_SYNC_KLG_IDENTITY_GATE")
assert swapped==["COPPER FUTURES","HG=F"], swapped
print("T5 PASS Leg 0 rides the existing TFB_SYNC_KLG_IDENTITY_GATE kill-switch")
print("PASS 5/5 — real module rds v6.54.0")
