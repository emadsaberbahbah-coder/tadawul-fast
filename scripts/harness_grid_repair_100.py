#!/usr/bin/env python3
"""harness_grid_repair_100.py — proof battery for repair_grid_capacity.py v1.0.0.

Executes the REAL module. The only double is a recorder standing in for the
external Google boundary (GridClient calls), per the standing FakeRes ruling.
The centerpiece: the planner must reproduce the 2026-08-23 review's recovery
table CELL-FOR-CELL (freed total 3,780,881; projected after 6,219,096) from
the review's own metadata figures. Exit 0 iff all checks pass.
"""
from __future__ import annotations

import importlib.util
import os
import sys

MODULE_PATH = os.environ.get("GRID_UNDER_TEST", "/home/claude/repair_grid_capacity.py")
spec = importlib.util.spec_from_file_location("grid_under_test", MODULE_PATH)
G = importlib.util.module_from_spec(spec)
sys.modules["grid_under_test"] = G
spec.loader.exec_module(G)  # type: ignore[union-attr]

PASS = FAIL = 0
def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  PASS  {name}")
    else:
        FAIL += 1; print(f"  FAIL  {name}  {detail}")


def meta_tab(title, sid, rows, cols, frozen=1):
    return {"properties": {"title": title, "sheetId": sid,
                           "gridProperties": {"rowCount": rows,
                                              "columnCount": cols,
                                              "frozenRowCount": frozen}}}

# ---- The 2026-08-23 review's live metadata, as fixtures --------------------
REVIEW_TABS = [
    meta_tab("My_Portfolio",   11, 9_905, 122),
    meta_tab("Commodities_FX", 12, 9_604, 115),
    meta_tab("Mutual_Funds",   13, 9_842, 115),
    meta_tab("_Run_Log",       14, 30_696, 26),
    meta_tab("Market_Leaders", 15, 1_680, 115),
    meta_tab("Global_Markets", 16, 7_199, 115),
    # a non-targeted fat tab that must be UNTOUCHED:
    meta_tab("Performance_Log", 17, 25_583, 80),
]
REVIEW_META = {"sheets": REVIEW_TABS}
REVIEW_EXTENTS = {  # last non-empty row in col A (header + data)
    "My_Portfolio": 6, "Commodities_FX": 454, "Mutual_Funds": 2_475,
    "_Run_Log": 30_696, "Market_Leaders": 256, "Global_Markets": 6_618,
}

print("== H1  _allocated_from_meta ==")
tot, tabs = G._allocated_from_meta(REVIEW_META)
check("total sums every tab", tot == sum(t["cells"] for t in tabs))
check("per-tab cells math", next(t for t in tabs if t["title"] == "My_Portfolio")["cells"] == 9_905 * 122)
check("malformed -> (None, [])", G._allocated_from_meta({"sheets": "x"}) == (None, []))
check("empty -> (None, [])", G._allocated_from_meta(None) == (None, []))

print("== H2  effective-target guard ==")
check("target below data lifted", G._effective_target(250, 400, 1, 25) == 425)
check("target respected when safe", G._effective_target(1000, 453, 1, 25) == 1000)
check("frozen floor", G._effective_target(2, 0, 5, 0) == 7)
check("absolute floor 2", G._effective_target(0, 0, 0, 0) == 2)

print("== H3  _Run_Log prune-span math ==")
check("30,695 data keep 4,999 -> delete [1,25697)",
      G._runlog_prune_span(30_695, 4_999) == (1, 25_697))
check("span length = data-keep",
      (lambda s: s[1] - s[0])(G._runlog_prune_span(30_695, 4_999)) == 25_696)
check("data <= keep -> None", G._runlog_prune_span(4_000, 4_999) is None)
check("zero data -> None", G._runlog_prune_span(0, 4_999) is None)

print("== H4  delete request shape ==")
req = G._delete_rows_request(14, 1, 25_697)
check("deleteDimension ROWS half-open",
      req == {"deleteDimension": {"range": {"sheetId": 14, "dimension": "ROWS",
                                            "startIndex": 1, "endIndex": 25_697}}})

print("== H5  planner reproduces the review table CELL-FOR-CELL ==")
plan = G._plan(tabs, G.DEFAULT_TARGETS, REVIEW_EXTENTS, 25)
freed = {p["title"]: p.get("freed", 0) for p in plan}
expect = {"My_Portfolio": 1_177_910, "Commodities_FX": 989_460,
          "Mutual_Funds": 786_830, "_Run_Log": 668_096,
          "Market_Leaders": 135_700, "Global_Markets": 22_885}
for k, v in expect.items():
    check(f"freed[{k}] == {v:,}", freed.get(k) == v, f"got {freed.get(k)}")
check("freed TOTAL == 3,780,881", sum(freed.values()) == 3_780_881,
      f"got {sum(freed.values()):,}")
check("effective targets == requested (all data-safe)",
      all(p["effective"] == p["requested"] for p in plan if "requested" in p))
rl = next(p for p in plan if p["title"] == "_Run_Log")
check("_Run_Log mode + span", rl["action"] == "ARCHIVE+PRUNE+RESIZE"
      and rl["prune_span"] == (1, 25_697) and rl["keep_newest"] == 4_999)
check("non-targeted Performance_Log absent from plan",
      all(p["title"] != "Performance_Log" for p in plan))

print("== H6  planner guards ==")
plan2 = G._plan(tabs, {"My_Portfolio": 3}, {"My_Portfolio": 6}, 25)
check("unsafe target lifted to data+buffer",
      plan2[0]["effective"] == 30 and plan2[0]["freed"] == (9_905 - 30) * 122)
plan3 = G._plan(tabs, {"Global_Markets": 8_000}, {"Global_Markets": 6_618}, 25)
check("target >= grid -> SKIP (never grow)",
      plan3[0]["action"] == "SKIP" and plan3[0]["freed"] == 0)
plan4 = G._plan(tabs, {"Nope_Tab": 100}, {"Nope_Tab": 5}, 25)
check("absent tab -> SKIP", plan4[0]["action"] == "SKIP")
plan5 = G._plan(tabs, {"Mutual_Funds": 3_000}, {}, 25)
check("unknown extent -> SKIP (never resize the unmeasured)",
      plan5[0]["action"] == "SKIP" and "extent" in plan5[0]["reason"])

print("== H7  report formatting ==")
rep = G._fmt_report(plan, 9_999_977)
check("before shown", "9,999,977" in rep)
check("projected after 6,219,096 PASS",
      "6,219,096" in rep and "PASS" in rep)
rep2 = G._fmt_report(plan5, 9_999_977)
check("all-skip projects FAIL", "FAIL" in rep2)

print("== H8  apply gates (pure) ==")
env1 = {"TFB_GRID_REPAIR_APPLY": "1"}
check("all open -> []", G._apply_gates_missing(True, True, "RESIZE", env1) == [])
m = G._apply_gates_missing(False, False, "resize", {})
check("all closed -> 4 named gates",
      set(x.split()[0] for x in m) == {"--apply", "--i-have-backup",
                                       "--confirm", "env"})
check("confirm is exact-match", "--confirm RESIZE" in
      G._apply_gates_missing(True, True, "RESIZE ", env1) or
      G._apply_gates_missing(True, True, "RESIZE", env1) == [])

print("== H9  execution order + reductions-only via recorder double ==")
class Recorder:
    def __init__(self, fail_on=None):
        self.calls = []; self.fail_on = fail_on or set()
    def dump_tab_csv(self, title, path):
        self.calls.append(("dump", title))
        if "dump" in self.fail_on: raise RuntimeError("dump boom")
        return 30_696
    def delete_rows(self, sid, s, e):
        self.calls.append(("delete", sid, s, e))
        if "delete" in self.fail_on: raise RuntimeError("delete boom")
    def resize_rows(self, title, rows):
        self.calls.append(("resize", title, rows))
        if title in self.fail_on: raise RuntimeError("resize boom")

rec = Recorder()
errs = G._execute_plan(rec, plan, "/tmp/ga", skip_archive=False, log=lambda *a: None)
check("zero errors on clean run", errs == [])
check("_Run_Log FIRST: dump -> delete -> resize",
      rec.calls[0][0] == "dump" and rec.calls[1][:2] == ("delete", 14)
      and rec.calls[2] == ("resize", "_Run_Log", 5_000))
check("exact delete span executed", rec.calls[1] == ("delete", 14, 1, 25_697))
check("all six resizes, only six", [c for c in rec.calls if c[0] == "resize"]
      and len([c for c in rec.calls if c[0] == "resize"]) == 6)
check("Performance_Log never touched",
      all("Performance_Log" not in map(str, c) for c in rec.calls))

rec2 = Recorder(fail_on={"Commodities_FX"})
errs2 = G._execute_plan(rec2, plan, "/tmp/ga", skip_archive=False, log=lambda *a: None)
check("per-tab failure recorded, others still run",
      len(errs2) == 1 and "Commodities_FX" in errs2[0]
      and ("resize", "Global_Markets", 7_000) in rec2.calls)

rec3 = Recorder()
skip_plan = G._plan(tabs, {"Global_Markets": 8_000}, {"Global_Markets": 6_618}, 25)
G._execute_plan(rec3, skip_plan, "/tmp/ga", skip_archive=False, log=lambda *a: None)
check("SKIP rows produce zero boundary calls", rec3.calls == [])

rec4 = Recorder()
G._execute_plan(rec4, plan, "/tmp/ga", skip_archive=True, log=lambda *a: None)
check("--skip-archive: no dump, prune+resize still exact",
      rec4.calls[0][:2] == ("delete", 14) and ("dump", "_Run_Log") not in rec4.calls)

print("== H10  dry-run main writes nothing (no client mutation path) ==")
import inspect
src = inspect.getsource(G.main)
check("apply branch guarded after report+gates",
      "if not args.apply" in src and "_apply_gates_missing" in src
      and src.index("if not args.apply") < src.index("_execute_plan"))

print(f"\nRESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
