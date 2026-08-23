#!/usr/bin/env python3
"""harness_batch3.py — proof battery for Batch 3 (IR-090 + IR-078b + IR-082).

3b: REAL import of core.analysis.portfolio_actions v1.8.1 (lean-importable —
    the CI suite already proves it); drives the REAL _apply_deminimis and
    asserts decide_action is byte-untouched.
3c: REAL import of integrations.google_sheets_service v6.2.0 with a recorder
    double injected ONLY at get_sheets_service (the external Google
    boundary); drives the REAL write_range / write_grid_chunked seams.
3a: name-contract + additive-only were proven in the edit step; here the
    parsed YAML is re-checked for the two mappings.
Exit 0 iff all checks pass.
"""
from __future__ import annotations

import importlib.util
import io
import logging
import os
import sys

REPO = "/home/claude/ci2"
sys.path.insert(0, REPO)

PASS = FAIL = 0
def check(name, cond, detail=""):
    global PASS, FAIL
    if cond: PASS += 1; print(f"  PASS  {name}")
    else:    FAIL += 1; print(f"  FAIL  {name}  {detail}")

# --------------------------------------------------------------------------- #
print("== 3b  portfolio_actions v1.8.1 — REAL module ==")
os.environ.pop("TFB_PF_MIN_TICKET_SAR", None)
spec = importlib.util.spec_from_file_location(
    "pa_under_test", "/home/claude/new_pa_181.py")
PA = importlib.util.module_from_spec(spec)
sys.modules["pa_under_test"] = PA
spec.loader.exec_module(PA)  # type: ignore[union-attr]

check("version 1.8.1", PA.PORTFOLIO_ACTIONS_VERSION == "1.8.1")
D = PA._apply_deminimis
check("default floor 0 -> passthrough (TRIM 66 survives)",
      D("TRIM", "cap", 66.0) == ("TRIM", "cap", 66.0))
os.environ["TFB_PF_MIN_TICKET_SAR"] = "500"
a, r, p = D("TRIM", "Position 17.0% > cap 15.0%", 66.0)
check("YUM case: 66 < 500 -> HOLD, proceeds zeroed",
      a == "HOLD" and p == 0.0)
check("reason explains + preserves original",
      r.startswith("De-minimis: TRIM 66 SAR below floor 500")
      and "Position 17.0% > cap 15.0%" in r)
check("TRIM at floor exactly -> untouched (strict <)",
      D("TRIM", "x", 500.0) == ("TRIM", "x", 500.0))
check("TRIM above floor untouched", D("TRIM", "x", 1302.0)[0] == "TRIM")
check("TRIM zero proceeds untouched (no phantom demotion)",
      D("TRIM", "x", 0.0) == ("TRIM", "x", 0.0))
check("EXIT exempt (verdict, not sizing)",
      D("EXIT", "EXIT-BY-RULE", 66.0) == ("EXIT", "EXIT-BY-RULE", 66.0))
check("ADD exempt", D("ADD", "x", 66.0)[0] == "ADD")
check("HOLD/BLOCK exempt", D("BLOCK", "x", 66.0)[0] == "BLOCK")
os.environ["TFB_PF_MIN_TICKET_SAR"] = "garbage"
check("unparseable floor -> passthrough (fail-open)",
      D("TRIM", "x", 66.0)[0] == "TRIM")
os.environ.pop("TFB_PF_MIN_TICKET_SAR")
src = io.open("/home/claude/new_pa_181.py", encoding="utf-8").read()
base = io.open(REPO + "/core/analysis/portfolio_actions.py",
               encoding="utf-8").read()
import re as _re
def body(text, name):
    m = _re.search(rf"\ndef {name}\(.*?(?=\ndef )", text, _re.S)
    return m.group(0) if m else None
check("decide_action byte-identical to v1.8.0",
      body(src, "decide_action") is not None
      and body(src, "decide_action") == body(base, "decide_action"),
      "region diff")
check("exactly ONE de-minimis seam, adjacent to decide_action call",
      src.count("action, reason, proceeds = _apply_deminimis(") == 1)

# --------------------------------------------------------------------------- #
print("== 3c  google_sheets_service v6.2.0 — REAL module, boundary recorder ==")
spec = importlib.util.spec_from_file_location(
    "svc_under_test", "/home/claude/new_svc_620.py")
SV = importlib.util.module_from_spec(spec)
sys.modules["svc_under_test"] = SV
spec.loader.exec_module(SV)  # type: ignore[union-attr]
check("version 6.2.0", SV.SERVICE_VERSION == "6.2.0")

class _Exec:
    def __init__(self, payload): self._p = payload
    def execute(self): 
        if isinstance(self._p, Exception): raise self._p
        return self._p

class RecorderSvc:
    def __init__(self, meta):
        self.meta = meta
        self.get_calls = []
        self.update_calls = []
    def spreadsheets(self): return self
    def get(self, spreadsheetId=None, fields=None):
        self.get_calls.append((spreadsheetId, fields))
        return _Exec(self.meta)
    def values(self): return self
    def update(self, spreadsheetId=None, range=None, valueInputOption=None, body=None):
        self.update_calls.append((spreadsheetId, range, valueInputOption,
                                  len((body or {}).get("values", []))))
        return _Exec({"updatedCells": 4})

def meta(cells_rows_cols):
    return {"sheets": [{"properties": {"gridProperties":
            {"rowCount": r, "columnCount": c}}} for r, c in cells_rows_cols]}

def wire(rec):
    SV._SVC_CAP_SEEN.clear()
    SV.get_sheets_service = lambda: rec
    return rec

logrec: list = []
class H(logging.Handler):
    def emit(self, r): logrec.append(r.getMessage())
SV.logger.addHandler(H()); SV.logger.setLevel(logging.INFO)

check("pure summer", SV._svc_allocated_from_meta(meta([(9_000_000, 1),
      (500_000, 2)])) == 10_000_000)
check("malformed -> None", SV._svc_allocated_from_meta({"sheets": 3}) is None)

os.environ.pop("TFB_SVC_CAPACITY_GUARD", None)
rec = wire(RecorderSvc(meta([(9_000_000, 1)])))
logrec.clear()
n = SV.write_range("SID12345", "Tab!A1:B2", [["a", "b"], ["c", "d"]])
check("real write path intact (updatedCells)", n == 4
      and rec.update_calls[0][:2] == ("SID12345", "Tab!A1:B2"))
check("probe: ONE metadata get, narrow fields",
      len(rec.get_calls) == 1 and "gridProperties" in rec.get_calls[0][1])
cap_lines = [m for m in logrec if "[CAPACITY-SVC v6.2.0]" in m]
check("one CAPACITY-SVC line with NEAR-LIMIT at 90%",
      len(cap_lines) == 1 and "90.00%" in cap_lines[0]
      and "NEAR-LIMIT" in cap_lines[0])
logrec.clear()
SV.write_range("SID12345", "Tab!A3", [["x"]])
check("second write same sid: NO second probe, no second line",
      len(rec.get_calls) == 1 and not logrec)
SV.write_range("OTHERSID", "T!A1", [["x"]])
check("new sid probes once more", len(rec.get_calls) == 2)

rec = wire(RecorderSvc(meta([(1_000_000, 1)])))
logrec.clear()
SV.write_range("SIDLOW", "T!A1", [["x"]])
check("healthy book: line without NEAR-LIMIT",
      len([m for m in logrec if "CAPACITY-SVC" in m]) == 1
      and "NEAR-LIMIT" not in logrec[-1])

rec = wire(RecorderSvc(RuntimeError("api down")))
logrec.clear()
n = SV.write_range("SIDERR", "T!A1", [["x"]])
check("probe failure is silent-open; write still lands",
      n == 4 and not [m for m in logrec if "CAPACITY-SVC" in m])

os.environ["TFB_SVC_CAPACITY_GUARD"] = "0"
rec = wire(RecorderSvc(meta([(9_999_999, 1)])))
SV.write_range("SIDKILL", "T!A1", [["x"]])
check("kill-switch: zero metadata calls", rec.get_calls == [])
os.environ.pop("TFB_SVC_CAPACITY_GUARD")

rec = wire(RecorderSvc(meta([(9_000_000, 1)])))
SV.write_grid_chunked("SIDGRID", "Tab", "A1",
                      [["H1", "H2"], ["r1", "r2"]])
check("grid seam probes too", len(rec.get_calls) == 1)

print(f"\nRESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
