import sys, os, importlib.util, datetime as dt
from unittest.mock import MagicMock
sys.path.insert(0,"/home/claude/repo/tadawul-fast-main"); sys.path.insert(0,"/home/claude/repo/tadawul-fast-main/scripts")
def load(path,name):
    spec=importlib.util.spec_from_file_location(name,path); m=importlib.util.module_from_spec(spec); sys.modules[name]=m
    try: spec.loader.exec_module(m)
    except ModuleNotFoundError as e:
        missing=str(e).split("'")[1]; mm=MagicMock(); mm.__name__=missing; sys.modules[missing]=mm; return load(path,name)
    return m
os.environ.pop("TFB_S1_DRILL_LEGACY",None)
M=load("/home/claude/ci/scripts/run_shadow_scorer.py","ss170"); assert M.SCRIPT_VERSION=="1.7.0"
# a realistic log: 3,000 GAS filler rows AFTER the drill lines (so they are far outside any 400-row tail)
filler=[["8/30/2026 9:%02d:00"%(i%60),"INFO","tfbAutoRefreshTrigger","Global_Markets","OK","Resuming batch refresh"] for i in range(3000)]
rows=[["Timestamp","Level","Action","Page","Status","Message"],
      ["2026-07-01 10:00:00","INFO","shadow_scorer","S1_Gate","OK","[ROLLBACK-DRILL] passed (operator-recorded) v1.5.0"],   # outside 45d window
      ["2026-08-17 09:12:00","INFO","workbook_backup","_Backup","OK","[RESTORE-TEST v1.4.0] PASS DAILY_2026-08-16.xlsx bytes=22340000 tabs=48 core_tabs=all open_time=7.9s"],
      ["2026-08-19 02:31:00","ERROR","workbook_backup","_Backup","FAIL","[RESTORE-TEST v1.4.0] FAIL DAILY_2026-08-18.xlsx missing tabs: ['Top_10_Investments']"],
     ]+filler
class WS:
    def __init__(s,v): s.v=v
    def get_all_values(s): return s.v
class SH:
    def __init__(s,v): s.ws=WS(v)
    def worksheet(s,n): assert n=="_Run_Log"; return s.ws
today=dt.date(2026,8,31); since=today-dt.timedelta(days=45)
r=M.find_drill_marker(SH(rows),since)
assert r=="2026-08-17 (RESTORE-TEST)", r
print("T1 PASS v1.7.0 finds the 08-17 RESTORE-TEST PASS (FAIL line ignored, out-of-window ROLLBACK-DRILL ignored) beyond a 3,000-row tail ->",r)
# manual marker still wins when newer; GAS-format stamp parsed
rows2=rows+[["8/29/2026 8:00:00","INFO","shadow_scorer","S1_Gate","OK","[ROLLBACK-DRILL] passed (operator-recorded) v1.7.0"]]
assert M.find_drill_marker(SH(rows2),since)=="2026-08-29 (ROLLBACK-DRILL)"
print("T2 PASS newer manual marker wins; M/D/YYYY stamp parsed")
# nothing in window -> None; unreadable sheet -> None
assert M.find_drill_marker(SH(rows[:1]+filler),since) is None
class Boom:
    def worksheet(s,n): raise RuntimeError("x")
assert M.find_drill_marker(Boom(),since) is None
print("T3 PASS no evidence -> None (PENDING); unreadable -> None")
# evaluate_s1 consumes the string: criterion 6 PASS with provenance in detail
import inspect
sig=inspect.signature(M.evaluate_s1); print("evaluate_s1 params:",list(sig.parameters))
# kill-switch: v1.6.0 semantics (last 400 rows, ROLLBACK-DRILL only) -> the 08-17 drill is invisible again
os.environ["TFB_S1_DRILL_LEGACY"]="1"; assert M.find_drill_marker(SH(rows),since) is None; os.environ.pop("TFB_S1_DRILL_LEGACY")
print("T4 PASS TFB_S1_DRILL_LEGACY=1 reproduces v1.6.0 (drill invisible)")
# control on the real v1.6.0
M0=load("/home/claude/ci0/scripts/run_shadow_scorer.py","ss160"); assert M0.SCRIPT_VERSION=="1.6.0" and M0.find_drill_marker(SH(rows),since) is None
print("T5 PASS control: v1.6.0 cannot see the passed drill on the identical log")
print("PASS 5/5 — real run_shadow_scorer v1.7.0")
