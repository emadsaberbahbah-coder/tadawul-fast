#!/usr/bin/env python3
"""harness_tp_6340.py — proof battery for track_performance.py v6.34.0
(IR-078a: capacity guard + evidence-append registry + fatal semantics +
evidence-workbook destination override).

DISCIPLINE: imports the REAL module and drives the REAL classes
(PerformanceStore / SignalHistoryStore / SignalTrendStore) and the REAL
dataclasses (PerformanceRecord / SignalSnapshot). Test doubles exist ONLY
at the external Google boundary (a fake `gspread` client + recorder
worksheets), per the standing FakeRes ruling. Env is saved/restored around
every case. Exit 0 iff every check passes.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import threading
from datetime import datetime, timezone

MODULE_PATH = os.environ.get("TP_UNDER_TEST", "/home/claude/new_tp_6340.py")

_ENV_KEYS = [
    "TFB_TP_CAPACITY_GUARD", "TFB_TP_CAPACITY_GUARD_MODE",
    "TFB_TP_CAPACITY_LIMIT", "TFB_TP_CAPACITY_PCT",
    "TFB_TP_APPEND_FATAL", "TFB_TP_EVIDENCE_SPREADSHEET_ID",
]


def _clear_env():
    for k in _ENV_KEYS:
        os.environ.pop(k, None)


def _load():
    _clear_env()
    spec = importlib.util.spec_from_file_location("tp_under_test", MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["tp_under_test"] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


TP = _load()

PASS = 0
FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


# --------------------------------------------------------------------------- #
# Google-boundary doubles (external service only)                             #
# --------------------------------------------------------------------------- #
class RecorderWS:
    def __init__(self, fail=False):
        self.fail = fail
        self.append_calls = []
        self.row_values_calls = 0

    def append_rows(self, rows, value_input_option=None):
        self.append_calls.append((len(rows), value_input_option))
        if self.fail:
            raise RuntimeError("HTTP 400: over grid limits")

    def row_values(self, n):
        self.row_values_calls += 1
        return []

    def update(self, *a, **k):
        return None

    def batch_clear(self, *a, **k):
        return None


class RecorderBook:
    def __init__(self, key, meta=None, ws=None):
        self.key = key
        self._meta = meta
        self.meta_calls = 0
        self._ws = ws or RecorderWS()
        self.worksheet_names = []

    def fetch_sheet_metadata(self):
        self.meta_calls += 1
        if isinstance(self._meta, Exception):
            raise self._meta
        return self._meta

    def worksheet(self, name):
        self.worksheet_names.append(name)
        return self._ws

    def add_worksheet(self, title=None, rows=None, cols=None):
        self.worksheet_names.append(title)
        return self._ws


class FakeGC:
    """Stands in for gspread's authorized client (external boundary)."""

    def __init__(self, books):
        self.books = books
        self.opened = []

    def open_by_key(self, key):
        self.opened.append(key)
        return self.books[key]


class FakeGspread:
    def __init__(self, gc):
        self._gc = gc

    def authorize(self, creds):
        return self._gc

    def service_account(self):
        return self._gc


def meta_for(cells_per_tab):
    return {"sheets": [
        {"properties": {"gridProperties": {"rowCount": r, "columnCount": c}}}
        for (r, c) in cells_per_tab]}


def reset_module_state():
    _clear_env()
    with TP._CAPACITY_LOCK:
        TP._CAPACITY_CACHE.clear()
    with TP._EVIDENCE_APPEND_LOCK:
        del TP._EVIDENCE_APPEND_FAILURES[:]


# --------------------------------------------------------------------------- #
print("== H1  env gates: defaults, kill switches, clamps ==")
reset_module_state()
check("guard default ON", TP._capacity_guard_enabled() is True)
os.environ["TFB_TP_CAPACITY_GUARD"] = "0"
check("guard kill-switch", TP._capacity_guard_enabled() is False)
reset_module_state()
check("mode default observe", TP._capacity_guard_mode() == "observe")
os.environ["TFB_TP_CAPACITY_GUARD_MODE"] = "enforce"
check("mode enforce", TP._capacity_guard_mode() == "enforce")
os.environ["TFB_TP_CAPACITY_GUARD_MODE"] = "sideways"
check("mode unknown -> observe (fail-safe)", TP._capacity_guard_mode() == "observe")
reset_module_state()
check("limit default 10M", TP._capacity_limit() == 10_000_000)
os.environ["TFB_TP_CAPACITY_LIMIT"] = "5"
check("limit floor 1M", TP._capacity_limit() == 1_000_000)
os.environ["TFB_TP_CAPACITY_LIMIT"] = "garbage"
check("limit unparseable -> default", TP._capacity_limit() == 10_000_000)
reset_module_state()
check("pct default 85", TP._capacity_pct() == 85)
os.environ["TFB_TP_CAPACITY_PCT"] = "5"
check("pct clamp low 50", TP._capacity_pct() == 50)
os.environ["TFB_TP_CAPACITY_PCT"] = "150"
check("pct clamp high 99", TP._capacity_pct() == 99)
reset_module_state()
check("fatal default OFF", TP._append_fatal_enabled() is False)
os.environ["TFB_TP_APPEND_FATAL"] = "1"
check("fatal armed", TP._append_fatal_enabled() is True)
reset_module_state()
check("override unset -> passthrough", TP._evidence_spreadsheet_id("MAIN") == "MAIN")
os.environ["TFB_TP_EVIDENCE_SPREADSHEET_ID"] = "EVID"
check("override set -> override", TP._evidence_spreadsheet_id("MAIN") == "EVID")

print("== H2  _allocated_cells_from_meta (pure) ==")
reset_module_state()
check("sum over tabs", TP._allocated_cells_from_meta(
    meta_for([(1000, 100), (2000, 50)])) == 200_000)
check("malformed -> None", TP._allocated_cells_from_meta({"sheets": "x"}) is None)
check("empty sheets -> None", TP._allocated_cells_from_meta({"sheets": []}) is None)
check("non-dict -> None", TP._allocated_cells_from_meta(None) is None)
check("partial rows counted", TP._allocated_cells_from_meta(
    {"sheets": [{"properties": {}},
                {"properties": {"gridProperties": {"rowCount": 10,
                                                   "columnCount": 10}}}]}) == 100)

print("== H3  _capacity_probe: math, one-fetch cache, fail-open ==")
reset_module_state()
book = RecorderBook("B1", meta=meta_for([(9_000_000, 1)]))
p1 = TP._capacity_probe(book, "B1")
check("allocated read", p1["allocated"] == 9_000_000)
check("pct math", p1["pct_used"] == 90.0)
check("breach at 90% vs 85", p1["breach"] is True)
p2 = TP._capacity_probe(book, "B1")
check("cache: ONE metadata fetch", book.meta_calls == 1 and p2 is p1)
book2 = RecorderBook("B2", meta=RuntimeError("api down"))
p3 = TP._capacity_probe(book2, "B2")
check("fetch error -> fail-open no breach", p3["breach"] is False and "error" in p3)
p4 = TP._capacity_probe(None, "B3")
check("no book -> fail-open", p4["breach"] is False and "error" in p4)
reset_module_state()
os.environ["TFB_TP_CAPACITY_GUARD"] = "0"
bookk = RecorderBook("B4", meta=meta_for([(9_999_999, 1)]))
pk = TP._capacity_probe(bookk, "B4")
check("guard off -> no fetch, no breach", bookk.meta_calls == 0 and pk["breach"] is False)

print("== H4  _capacity_block_reason matrix ==")
reset_module_state()
big = meta_for([(9_500_000, 1)])
small = meta_for([(1_000_000, 1)])
check("default(observe) -> ''", TP._capacity_block_reason(
    RecorderBook("C1", meta=big), "C1") == "")
reset_module_state()
os.environ["TFB_TP_CAPACITY_GUARD_MODE"] = "enforce"
check("enforce+no-breach -> ''", TP._capacity_block_reason(
    RecorderBook("C2", meta=small), "C2") == "")
r = TP._capacity_block_reason(RecorderBook("C3", meta=big), "C3")
check("enforce+breach -> reason", "95.00%" in r and "10,000,000" in r)
reset_module_state()
os.environ["TFB_TP_CAPACITY_GUARD"] = "0"
os.environ["TFB_TP_CAPACITY_GUARD_MODE"] = "enforce"
check("kill-switch beats enforce", TP._capacity_block_reason(
    RecorderBook("C4", meta=big), "C4") == "")

print("== H5  registry: note/list, thread-safety ==")
reset_module_state()
check("starts empty", TP._evidence_append_failures() == [])
TP._note_evidence_append_failure("Performance_Log", "x" * 500)
fl = TP._evidence_append_failures()
check("noted + truncated", len(fl) == 1 and fl[0].startswith("Performance_Log: ")
      and len(fl[0]) <= 320)
reset_module_state()
threads = [threading.Thread(
    target=lambda i=i: TP._note_evidence_append_failure("S", str(i)))
    for i in range(40)]
[t.start() for t in threads]
[t.join() for t in threads]
check("40 concurrent notes all land", len(TP._evidence_append_failures()) == 40)

print("== H6  REAL PerformanceStore._init_sheet via fake gspread ==")
reset_module_state()
ws_main = RecorderWS()
main_book = RecorderBook("MAIN", meta=meta_for([(1_000_000, 1)]), ws=ws_main)
gc = FakeGC({"MAIN": main_book})
TP.gspread = FakeGspread(gc)
TP.GSPREAD_AVAILABLE = True
os.environ["GOOGLE_SHEETS_CREDENTIALS"] = ""  # force service_account() path
store = TP.PerformanceStore("MAIN", "Performance_Log")
check("real init ran", store.ws is ws_main and store.sheet is main_book)
check("override unset -> _ws_book IS sheet (same object)",
      store._ws_book is store.sheet and store._ws_sid == "MAIN")
check("probe ran once on MAIN", main_book.meta_calls == 1)

reset_module_state()
ws_ev = RecorderWS()
main2 = RecorderBook("MAIN", meta=meta_for([(1_000_000, 1)]), ws=RecorderWS())
evid = RecorderBook("EVID", meta=meta_for([(500_000, 1)]), ws=ws_ev)
gc2 = FakeGC({"MAIN": main2, "EVID": evid})
TP.gspread = FakeGspread(gc2)
os.environ["TFB_TP_EVIDENCE_SPREADSHEET_ID"] = "EVID"
store2 = TP.PerformanceStore("MAIN", "Performance_Log")
check("override -> sheet stays MAIN", store2.sheet is main2)
check("override -> ws from EVID book",
      store2._ws_book is evid and store2.ws is ws_ev and store2._ws_sid == "EVID")
check("both books opened in order", gc2.opened == ["MAIN", "EVID"])
check("probe hit EVID (append destination)", evid.meta_calls == 1)

print("== H7  REAL append_records: default path byte-equivalent ==")
reset_module_state()
now = datetime.now(timezone.utc)
rec = TP.PerformanceRecord(
    record_id="r1", symbol="TEST.SR", horizon=list(TP.HorizonType)[0],
    date_recorded=now, entry_price=10.0,
    entry_recommendation=list(TP.RecommendationType)[0], entry_score=50.0,
    entry_risk_bucket="Medium", entry_confidence="Medium", origin_tab="T",
    target_price=12.0, target_roi=20.0, target_date=now,
    status=list(TP.PerformanceStatus)[0])
ws3 = RecorderWS()
b3 = RecorderBook("MAIN", meta=meta_for([(9_900_000, 1)]), ws=ws3)  # 99% full!
gc3 = FakeGC({"MAIN": b3})
TP.gspread = FakeGspread(gc3)
s3 = TP.PerformanceStore("MAIN", "Performance_Log")
ok = s3.append_records([rec])
check("observe(default): append PROCEEDS even at 99%",
      ok is True and len(ws3.append_calls) == 1
      and ws3.append_calls[0] == (1, "RAW"))
check("cache updated, registry EMPTY",
      rec.key in s3.cache and TP._evidence_append_failures() == [])
ok2 = s3.append_records([rec])
check("dedup short-circuit unchanged", ok2 is True and len(ws3.append_calls) == 1)

print("== H8  REAL append_records: enforce refusal + registry, no Google call ==")
reset_module_state()
os.environ["TFB_TP_CAPACITY_GUARD_MODE"] = "enforce"
ws4 = RecorderWS()
b4 = RecorderBook("MAIN", meta=meta_for([(9_900_000, 1)]), ws=ws4)
TP.gspread = FakeGspread(FakeGC({"MAIN": b4}))
s4 = TP.PerformanceStore("MAIN", "Performance_Log")
ok = s4.append_records([rec])
fl = TP._evidence_append_failures()
check("refused: False, ZERO Google calls",
      ok is False and ws4.append_calls == [])
check("registry notes Performance_Log capacity",
      len(fl) == 1 and fl[0].startswith("Performance_Log:") and "limit" in fl[0])
check("cache NOT updated on refusal", rec.key not in s4.cache)

print("== H9  REAL append_records: Google failure -> registry ==")
reset_module_state()
ws5 = RecorderWS(fail=True)
b5 = RecorderBook("MAIN", meta=meta_for([(1_000, 1)]), ws=ws5)
TP.gspread = FakeGspread(FakeGC({"MAIN": b5}))
s5 = TP.PerformanceStore("MAIN", "Performance_Log")
ok = s5.append_records([rec])
fl = TP._evidence_append_failures()
check("exception -> False + registry HTTP 400",
      ok is False and len(fl) == 1 and "HTTP 400" in fl[0])

print("== H10  REAL SignalHistoryStore: same three paths ==")
reset_module_state()
snap = TP.SignalSnapshot(
    snapshot_id="s1", symbol="TEST.SR", date_recorded=now,
    recommendation=list(TP.RecommendationType)[0], final_action="HOLD",
    investability_status="", overall_score=50.0, forecast_reliability=40.0,
    data_quality=70.0, risk_score=30.0, price=10.0, origin_tab="T")
wsA = RecorderWS()
bA = RecorderBook("MAIN", meta=meta_for([(1_000, 1)]), ws=wsA)
TP.gspread = FakeGspread(FakeGC({"MAIN": bA}))
sh = TP.SignalHistoryStore("MAIN", "Signal_History")
n = sh.append_snapshots([snap])
check("default: 1 written, registry empty",
      n == 1 and len(wsA.append_calls) == 1
      and TP._evidence_append_failures() == [])
reset_module_state()
os.environ["TFB_TP_CAPACITY_GUARD_MODE"] = "enforce"
wsB = RecorderWS()
bB = RecorderBook("MAIN", meta=meta_for([(9_999_999, 1)]), ws=wsB)
TP.gspread = FakeGspread(FakeGC({"MAIN": bB}))
sh2 = TP.SignalHistoryStore("MAIN", "Signal_History")
n2 = sh2.append_snapshots([snap])
fl = TP._evidence_append_failures()
check("enforce refusal: 0, no call, Signal_History noted",
      n2 == 0 and wsB.append_calls == []
      and len(fl) == 1 and fl[0].startswith("Signal_History:"))
reset_module_state()
wsC = RecorderWS(fail=True)
bC = RecorderBook("MAIN", meta=meta_for([(1_000, 1)]), ws=wsC)
TP.gspread = FakeGspread(FakeGC({"MAIN": bC}))
sh3 = TP.SignalHistoryStore("MAIN", "Signal_History")
n3 = sh3.append_snapshots([snap])
check("google failure: 0 + registry",
      n3 == 0 and len(TP._evidence_append_failures()) == 1)

print("== H11  REAL SignalTrendStore: override yes, refusal machinery NO ==")
reset_module_state()
os.environ["TFB_TP_EVIDENCE_SPREADSHEET_ID"] = "EVID"
os.environ["TFB_TP_CAPACITY_GUARD_MODE"] = "enforce"
wsT = RecorderWS()
mT = RecorderBook("MAIN", meta=meta_for([(9_999_999, 1)]), ws=RecorderWS())
eT = RecorderBook("EVID", meta=meta_for([(9_999_999, 1)]), ws=wsT)
TP.gspread = FakeGspread(FakeGC({"MAIN": mT, "EVID": eT}))
st = TP.SignalTrendStore("MAIN", os.getenv("TFB_SIGNAL_TRENDS_TAB") or "Signal_Trends")
check("trend ws from EVID; sheet stays MAIN",
      st.sheet is mT and st._ws_book is eT and st.ws is wsT)
src = open(MODULE_PATH, encoding="utf-8").read()
import re as _re
seg = _re.search(r"class SignalTrendStore:.*?\nclass ", src, _re.S).group(0)
check("no refusal/registry inside SignalTrendStore",
      "_capacity_block_reason" not in seg
      and "_note_evidence_append_failure" not in seg)

print("== H12  run_once fatal tail wiring (source contract) ==")
tail = _re.search(r"async def run_once.*?\n        return 0\n", src, _re.S).group(0)
check("tail consults registry", "_evidence_append_failures()" in tail)
check("tail gates exit on _append_fatal_enabled",
      "_append_fatal_enabled()" in tail and "return 1" in tail)
_vt1 = _re.search(r"\n    def _append_runlog_verdict.*?\n    def ", src, _re.S)
_cal = _re.search(r"\n    def _publish_s1_calibration.*?\n    def ", src, _re.S)
check("VT-1 verdict method has NO registry/refusal hook",
      _vt1 and "_note_evidence_append_failure" not in _vt1.group(0)
      and "_capacity_block_reason" not in _vt1.group(0))
check("S-1 calibration publish has NO registry/refusal hook",
      _cal and "_note_evidence_append_failure" not in _cal.group(0)
      and "_capacity_block_reason" not in _cal.group(0))

print("== H13  lean-import safety: module loads with gspread ABSENT ==")
import subprocess
r = subprocess.run(
    [sys.executable, "-c",
     "import importlib.util,sys;"
     "sys.modules['gspread']=None;"  # import gspread now raises -> lean path
     "spec=importlib.util.spec_from_file_location('tp2', %r);"
     "m=importlib.util.module_from_spec(spec);"
     "sys.modules['tp2']=m;"
     "spec.loader.exec_module(m);"
     "s=m.PerformanceStore('SID','Performance_Log');"
     "assert s.ws is None;"
     "assert m._evidence_append_failures()==[];"
     "print('LEANOK')" % MODULE_PATH],
    capture_output=True, text=True, env={**os.environ,
                                         "TFB_TP_CAPACITY_GUARD": "1"})
check("subprocess lean import + store best-effort", "LEANOK" in r.stdout, r.stderr[-200:])

print()
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
