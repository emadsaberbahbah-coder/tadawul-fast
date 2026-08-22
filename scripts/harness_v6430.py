#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/harness_v6430.py — W1A-6e harness (v1.0.0, 2026-08-23)
==============================================================================
Certifies scripts/run_dashboard_sync.py v6.43.0: the [OHLC-LAKE] probe and
the [IDENTITY-REFETCH] heal-first extension.

Executes the REAL functions AST-lifted from the REAL source file — the
harness_sync_6410 pattern, standing rule after the TaskResult slots defect.
The only double is a Sheets RECORDER: every API call it receives is captured
and asserted on, which is how "read-only, exactly one read, zero writes" is
PROVED rather than claimed.

EXIT 0 iff every assertion passes.
"""
from __future__ import annotations
import ast, copy, json, logging, os, re, sys, time
from typing import Any, Dict, List, Optional, Sequence, Tuple

SRC = os.environ.get("H_SRC", "run_dashboard_sync.py")
EXPECT_VERSION = "6.43.0"

LIFT = {
    # under test (new in 6.43.0)
    "_identity_refetch_enabled", "_ohlc_lake_enabled",
    "_identity_suspect_symbols", "_ohlc_lake_probe",
    "_append_runlog_ohlc_lake",
    # real dependencies (pre-existing, lifted verbatim)
    "_read_existing_page_symbols", "_heal_first_enabled",
    "_apply_ohlc_prewrite_guard", "_ohlc_prewrite_enabled",
    "_ohlc_prewrite_mode", "_ohlc_prewrite_tol", "_ohlc_prewrite_num",
    "_guard_find_col", "_guard_is_blank", "_guard_norm",
    "_name_is_fabricated", "_placeholder_guard_enabled",
    "_name_dedup_min", "_page_read_row_bound", "_universe_cap_v2_enabled",
}
_CONST_RE = re.compile(r"^_?[A-Z][A-Z0-9_]*$")  # module constants, incl. regexes

def _is_const_assign(node: ast.Assign) -> bool:
    return all(isinstance(t, ast.Name) and _CONST_RE.match(t.id)
               for t in node.targets)

def lift(path: str) -> Dict[str, Any]:
    src = open(path, encoding="utf-8").read()
    tree = ast.parse(src)
    g: Dict[str, Any] = {
        "os": os, "re": re, "json": json, "time": time, "copy": copy,
        "logging": logging, "logger": logging.getLogger("h6430"),
        "Any": Any, "Dict": Dict, "List": List, "Optional": Optional,
        "Sequence": Sequence, "Tuple": Tuple, "print": print,
    }
    picked = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in LIFT:
            picked.append(node)
        elif isinstance(node, ast.Assign) and _is_const_assign(node):
            try:  # skip constants whose RHS needs unlifted names (e.g. dataclass refs)
                compile(ast.Expression(node.value), "<c>", "eval")
                picked.append(node)
            except Exception:
                pass
    mod = ast.Module(body=picked, type_ignores=[])
    exec(compile(ast.fix_missing_locations(mod), path, "exec"), g)
    missing = sorted(LIFT - set(g))
    assert not missing, f"lift missing: {missing}"
    return g

class RecorderSheets:
    """Recorder double for the Sheets I/O seam ONLY (sanctioned pattern).
    Captures every call; the grid it serves is the test fixture."""
    def __init__(self, grid): self.grid = grid; self.reads = []; self.appends = []
    def read_values(self, sid, name, a1_range="A1:EZ2000"):
        self.reads.append((sid, name, a1_range)); return copy.deepcopy(self.grid)
    def _get_service(self): return _RecSvc(self)

class _RecSvc:
    def __init__(self, rec): self.rec = rec
    def spreadsheets(self): return self
    def values(self): return self
    def append(self, **kw): self.rec.appends.append(kw); return self
    def execute(self): return {}

P = F = 0
def check(name, cond, detail=""):
    global P, F
    ok = bool(cond); P += ok; F += (not ok)
    print(("PASS" if ok else "FAIL"), "-", name, ("| " + detail if detail and not ok else ""))

def env(**kw):
    for k in ("TFB_SYNC_OHLC_PREWRITE","TFB_SYNC_OHLC_PREWRITE_MODE",
              "TFB_SYNC_OHLC_LAKE","TFB_SYNC_IDENTITY_REFETCH",
              "TFB_SYNC_HEAL_FIRST","TFB_SYNC_NAME_DEDUP_MIN"):
        os.environ.pop(k, None)
    os.environ.update({k: v for k, v in kw.items() if v is not None})

def main() -> int:
    g = lift(SRC)
    check("lift: SCRIPT_VERSION", g["SCRIPT_VERSION"] == EXPECT_VERSION, g["SCRIPT_VERSION"])

    # ---------------- H1: gates -------------------------------------------
    env()
    check("H1a refetch default OFF", g["_identity_refetch_enabled"]() is False)
    env(TFB_SYNC_IDENTITY_REFETCH="1")
    check("H1b refetch arms on 1", g["_identity_refetch_enabled"]() is True)
    env()  # prewrite off
    check("H1c lake follows W1A-6 gate (off)", g["_ohlc_lake_enabled"]() is False)
    env(TFB_SYNC_OHLC_PREWRITE="1")
    check("H1d lake on under gate", g["_ohlc_lake_enabled"]() is True)
    env(TFB_SYNC_OHLC_PREWRITE="1", TFB_SYNC_OHLC_LAKE="0")
    check("H1e lake kill-switch", g["_ohlc_lake_enabled"]() is False)
    env()
    check("H1f probe gate-off: no touch, {} return",
          g["_ohlc_lake_probe"](None, "SID", "Global_Markets", [], []) == {})

    # ---------------- H2: suspect classifier (pure) -----------------------
    env(TFB_SYNC_IDENTITY_REFETCH="1")
    grid = [
        ["Symbol", "Name", "x", "y", "z"],
        ["AAA.US", "Microsoft Corporation", "", "", ""],
        ["BBB.US", "Microsoft Corporation", "", "", ""],
        ["CCC.US", "Microsoft Corporation", "", "", ""],
        ["DDD.US", "Apple Inc", "", "", ""],
        ["EEE.US", "", "", "", ""],                      # blank name -> stub
        ["FFF.US", "Global_Markets FFF.US", "", "", ""],  # fabricated -> stub
        ["GGG.US", "Nvidia Corp", "", "", ""],
        ["AAA.US", "Dup Row Ignored", "", "", ""],        # dup symbol
    ]
    susp, ng = g["_identity_suspect_symbols"](grid, 0, 0, 1)
    check("H2a group of 3 flagged", susp == {"AAA.US","BBB.US","CCC.US"}, str(susp))
    check("H2b group count", ng == 1, str(ng))
    env(TFB_SYNC_IDENTITY_REFETCH="1", TFB_SYNC_NAME_DEDUP_MIN="4")
    s2, n2 = g["_identity_suspect_symbols"](grid, 0, 0, 1)
    check("H2c honors TFB_SYNC_NAME_DEDUP_MIN", s2 == set() and n2 == 0)
    s3, n3 = g["_identity_suspect_symbols"](grid, 0, 0, -1)
    check("H2d fail-safe on missing Name col", s3 == set() and n3 == 0)

    # ---------------- H3: tri-partition via REAL page-symbol reader -------
    env(TFB_SYNC_HEAL_FIRST="1")  # refetch OFF
    rec = RecorderSheets(grid)
    base_order = g["_read_existing_page_symbols"](rec, "SID", "Global_Markets", 999)
    check("H3a gate OFF => v6.42.0 order (stubs first, no hoist)",
          base_order == ["EEE.US","FFF.US","AAA.US","BBB.US","CCC.US","DDD.US","GGG.US"],
          str(base_order))
    env(TFB_SYNC_HEAL_FIRST="1", TFB_SYNC_IDENTITY_REFETCH="1")
    rec2 = RecorderSheets(grid)
    on_order = g["_read_existing_page_symbols"](rec2, "SID", "Global_Markets", 999)
    check("H3b armed => stubs, then suspects, then healthy",
          on_order == ["EEE.US","FFF.US","AAA.US","BBB.US","CCC.US","DDD.US","GGG.US"][:2]
                      + ["AAA.US","BBB.US","CCC.US"] + ["DDD.US","GGG.US"],
          str(on_order))
    check("H3c no symbol dropped/added", sorted(on_order) == sorted(base_order))
    rec3 = RecorderSheets(grid)
    capped = g["_read_existing_page_symbols"](rec3, "SID", "Global_Markets", 3)
    check("H3d cap semantics preserved (suspects inside slice)",
          capped == ["EEE.US","FFF.US","AAA.US"], str(capped))

    # ---------------- H4: lake probe attribution (REAL guard on lake) -----
    env(TFB_SYNC_OHLC_PREWRITE="1")
    lake = [
        ["Symbol", "Name", "Open", "Day High", "Day Low", "Current Price"],
        ["S1.US", "Alpha Co",   "313.61", "20.0", "10.0", "15.0"],  # open outside band -> flagged
        ["S2.US", "Beta Co",    "12.0",   "20.0", "10.0", "15.0"],  # clean, populated
        ["S3.US", "Gamma WRONG","11.0",   "20.0", "10.0", "15.0"],  # name diff target
        ["S4.US", "Delta Co",   "",       "20.0", "10.0", "15.0"],  # lake blank open
        ["S5.US", "Eps Co",     "500.0",  "20.0", "10.0", "15.0"],  # open outside -> flagged
    ]
    hdrs = ["Symbol", "Name", "Open", "Day High", "Day Low", "Current Price"]
    matrix = [
        ["S1.US", "Alpha Co", "",     "20.0", "10.0", "15.0"],  # blank -> lake filled => foreign fill
        ["S2.US", "Beta Co",  "",     "20.0", "10.0", "15.0"],  # blank -> lake filled => foreign fill
        ["S3.US", "Gamma Co", "11.0", "20.0", "10.0", "15.0"],  # name differs => name diff
        ["S4.US", "Delta Co", "",     "20.0", "10.0", "15.0"],  # both blank -> NOT a fill
        ["S9.US", "New Co",   "",     "20.0", "10.0", "15.0"],  # not in lake -> ignored
    ]
    m_before = copy.deepcopy(matrix)
    os.environ["TFB_SYNC_OHLC_PREWRITE_MODE"] = "enforce"  # probe must force observe + restore
    rec4 = RecorderSheets(lake)
    st = g["_ohlc_lake_probe"](rec4, "SID", "Global_Markets", hdrs, matrix)
    check("H4a lake_checked", st.get("lake_checked") == 5, str(st))
    check("H4b lake_flagged (real guard, 2 opens outside)", st.get("lake_flagged") == 2, str(st))
    check("H4c lake_blank_open", st.get("lake_blank_open") == 1, str(st))
    check("H4d foreign_open_fill==2", st.get("foreign_open_fill") == 2, str(st))
    check("H4e foreign_name_diff==1", st.get("foreign_name_diff") == 1, str(st))
    check("H4f examples tagged", set(st.get("examples") or []) ==
          {"S1.US(open)","S2.US(open)","S3.US(name)"}, str(st.get("examples")))
    check("H4g rows_matrix untouched", matrix == m_before)
    check("H4h exactly ONE read, ZERO writes",
          len(rec4.reads) == 1 and len(rec4.appends) == 0,
          f"reads={rec4.reads} appends={len(rec4.appends)}")
    check("H4i MODE restored after forced-observe",
          os.environ.get("TFB_SYNC_OHLC_PREWRITE_MODE") == "enforce")
    os.environ.pop("TFB_SYNC_OHLC_PREWRITE_MODE", None)

    # ---------------- H5: appender shape ----------------------------------
    rec5 = RecorderSheets(lake)
    g["_append_runlog_ohlc_lake"](rec5, "SID", "Global_Markets", st)
    check("H5a one append", len(rec5.appends) == 1)
    kw = rec5.appends[0] if rec5.appends else {}
    row = (kw.get("body") or {}).get("values", [[None]])[0]
    check("H5b _Run_Log target + 10 cols",
          kw.get("range") == "'_Run_Log'!A1" and len(row) == 10, str(kw.get("range")))
    check("H5c WARNING level on foreign residue",
          len(row) == 10 and row[1] == "WARNING" and row[4] == "SUSPECT", str(row[:6]))
    check("H5d tag + counts in msg",
          len(row) == 10 and "[OHLC-LAKE v6.43.0]" in str(row[5])
          and "foreign_open_fill=2" in str(row[5]), str(row[5])[:120])
    _det = json.loads(row[9]) if len(row) == 10 else {}
    check("H5e details JSON carries version", _det.get("version") == EXPECT_VERSION)

    # ---------------- H6: probe fail-open on bad lake ---------------------
    env(TFB_SYNC_OHLC_PREWRITE="1")
    rec6 = RecorderSheets([["NoSym", "x"], ["a", "b"]])
    st6 = g["_ohlc_lake_probe"](rec6, "SID", "Global_Markets", hdrs, matrix)
    check("H6a error surfaced, never raised", "error" in st6, str(st6))
    check("H6b appender no-ops on empty stats",
          (g["_append_runlog_ohlc_lake"](RecorderSheets([]), "SID", "P", {}) is None))

    env()
    print(f"\nRESULT: {P} passed, {F} failed")
    return 0 if F == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
