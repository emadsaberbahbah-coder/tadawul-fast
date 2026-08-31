#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
W1A-6 / W1A-4 BEHAVIORAL HARNESS v2.2.0  (2026-08-18)
================================================================================
v2.0.0 (external W1A-6 Deployment Audit adjudicated):
  F-07 enforce-mode mutation contract (deterministic synthetic fixtures);
  F-08 decision-owned _Status suppression test (new v6.39.5 behaviour);
  item-18 full _Status writer contract (update/append/RAW/A:J/PARTIAL_FRESH/
          dry-run/manual-hold/fail-open) on the REAL classes;
  F-14 portable: runner path via TFB_HARNESS_RUNNER (default: sibling
          run_dashboard_sync.py), fixtures via TFB_HARNESS_FIXTURES; the
          actual-data suite SKIPS cleanly when exports are absent (CI);
  F-15 T1.2 is now a real assertion (credential-free _get_service() is None);
  item-20 CAP_BELOW_UNIVERSE telemetry test on _read_existing_page_symbols;
  T7-drift: source-ordering contract pins guard -> appender -> write inside
          _run_one_task so call-site drift fails CI even without executing
          the async task.
DISCIPLINE (unchanged from v1): the REAL module is executed — real
@dataclass(slots=True) TaskResult, real SheetsWriter, real guard, real
status writer. The ONLY double is a recorder at the third-party Google
client boundary (the same `svc` seam the shipped code already isolates).
MODES:
  --deterministic : synthetic-fixture suites only (CI, merge-blocking)
  (default)       : deterministic + actual-data evidence suite if fixtures
                    are present, else actual-data is SKIPPED (not failed).
Exit code: 0 iff every executed check passes.
"""
import contextlib
import csv
import importlib.util
import io
import json
import os
import re
import sys

csv.field_size_limit(10 ** 9)

DETERMINISTIC_ONLY = "--deterministic" in sys.argv[1:]

_HERE = os.path.dirname(os.path.abspath(__file__))
RUNNER = os.environ.get("TFB_HARNESS_RUNNER") or os.path.join(
    _HERE, "run_dashboard_sync.py")
FIXROOT = os.environ.get("TFB_HARNESS_FIXTURES") or "/mnt/user-data/uploads"
FIXPREFIX = "_Market_Share_Deepseek-V3_-"

# critical_symbol_identity lives beside the runner in the repo.
for _p in (_HERE, os.path.dirname(RUNNER)):
    if _p and _p not in sys.path:
        sys.path.insert(0, _p)

spec = importlib.util.spec_from_file_location("rds", RUNNER)
M = importlib.util.module_from_spec(spec)
sys.modules["rds"] = M
spec.loader.exec_module(M)

RESULTS = []
SKIPPED = []


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond), detail))
    print(("  PASS  " if cond else "  FAIL  ") + name
          + ((" | " + detail) if detail else ""))


def skip(name, why):
    SKIPPED.append((name, why))
    print("  SKIP  " + name + " | " + why)


def env(**kw):
    for k, v in kw.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def load_tsv(path):
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        return list(csv.reader(f, delimiter="\t"))


def fresh_res(**kw):
    """REAL slots=True TaskResult with required fields defaulted."""
    base = dict(key="k", sheet_name="Global_Markets", status="success",
                start_utc="2026-08-18T05:00:00+00:00")
    base.update(kw)
    return M.TaskResult(**base)


class GoogleClientRecorder:
    """Stands in ONLY for the third-party googleapiclient resource.
    Records get/update/append; can raise per-method to test fail-open."""

    def __init__(self, grid=None, fail=None, fail_times=0):
        self.grid = grid if grid is not None else []
        self.fail = set(fail or [])
        self.fail_times = fail_times
        self.calls = []

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def _ex(self, kind, kw):
        self.calls.append((kind, kw))
        outer = self

        class _E:
            def execute(self):
                if kind in outer.fail:
                    raise RuntimeError(f"simulated {kind} failure")
                if kind == "append" and outer.fail_times > 0:
                    outer.fail_times -= 1
                    raise RuntimeError("simulated Sheets 503")
                if kind == "get":
                    return {"values": outer.grid}
                return {"updates": {"updatedRows": 1}}

        return _E()

    def get(self, **kw):
        return self._ex("get", kw)

    def update(self, **kw):
        return self._ex("update", kw)

    def append(self, **kw):
        return self._ex("append", kw)


def rec_writer(**kw):
    rec = GoogleClientRecorder(**kw)
    sw = M.SheetsWriter()                      # REAL class, real __init__
    sw._get_service = lambda: rec              # third-party boundary only
    return rec, sw


GHDR = ["Symbol", "Open", "Day High", "Day Low", "Current Price", "Warnings"]

# =============================================================================
print("=" * 78)
print("S1  REAL SheetsWriter without credentials (F-15: real assertion)")
print("=" * 78)
_saved = {k: os.environ.pop(k, None) for k in list(os.environ)
          if "GOOGLE" in k or "SERVICE_ACCOUNT" in k or k == "GCP_SA_JSON"}
real_sw = M.SheetsWriter()
raised = None
svc_val = "unset"
try:
    svc_val = real_sw._get_service()
except Exception as e:
    raised = e
check("S1.1 credential-free _get_service() returns None and raises nothing",
      raised is None and svc_val is None, f"raised={raised!r} svc={svc_val!r}")
buf = io.StringIO()
raised = None
try:
    with contextlib.redirect_stdout(buf):
        M._append_runlog_ohlc_prewrite(real_sw, "sid", "Global_Markets",
                                       {"checked": 1, "flagged": 0})
except Exception as e:
    raised = e
check("S1.2 appender is a no-op (no exception, no output) without a service",
      raised is None and buf.getvalue() == "", f"raised={raised!r}")
for k, v in _saved.items():
    if v is not None:
        os.environ[k] = v

# =============================================================================
print()
print("=" * 78)
print("S2  Run-log gate matrix (unchanged v1 contract)")
print("=" * 78)
for val, want in [(None, True), ("1", True), ("true", True), ("0", False),
                  ("false", False), ("off", False), ("no", False),
                  ("On", True)]:
    env(TFB_SYNC_OHLC_RUNLOG=val)
    got = M._ohlc_prewrite_runlog_enabled()
    check(f"S2 TFB_SYNC_OHLC_RUNLOG={val!r} -> {got}", got == want)
env(TFB_SYNC_OHLC_RUNLOG=None)

# =============================================================================
print()
print("=" * 78)
print("S3  _Run_Log payload contract (synthetic, CI-stable)")
print("=" * 78)
env(TFB_SYNC_OHLC_PREWRITE="1", TFB_SYNC_OHLC_PREWRITE_MODE="observe",
    TFB_SYNC_OHLC_PREWRITE_TOL=None)
rec, sw = rec_writer()
M._append_runlog_ohlc_prewrite(sw, "SHEETID", "Commodities_FX",
                               {"checked": 453, "flagged": 95, "open": 94,
                                "price_band": 0, "range": 1,
                                "examples": ["LTC-USD", "ZS=F"]})
appends = [kw for kind, kw in rec.calls if kind == "append"]
check("S3.1 exactly one append issued", len(appends) == 1)
kw = appends[0]
row = kw["body"]["values"][0]
check("S3.2 row width == 10", len(row) == 10, f"width={len(row)}")
check("S3.3 range targets _Run_Log!A1", kw["range"] == "'_Run_Log'!A1")
check("S3.4 insertDataOption=INSERT_ROWS",
      kw["insertDataOption"] == "INSERT_ROWS")
check("S3.5 Action == run_dashboard_sync", row[2] == "run_dashboard_sync")
check("S3.6 flagged>0 -> WARNING/SUSPECT",
      row[1] == "WARNING" and row[4] == "SUSPECT", f"{row[1]}/{row[4]}")
check("S3.7 pinned v6.38.0 grep tag intact",
      row[5].startswith("[OHLC-PREWRITE v6.38.0]"), row[5][:40])
d = json.loads(row[9])
check("S3.8 Details JSON counts + version",
      (d["checked"], d["flagged"], d["open"], d["range"]) == (453, 95, 94, 1)
      and d["version"] == M.SCRIPT_VERSION, f'v={d["version"]}')
rec2, sw2 = rec_writer()
M._append_runlog_ohlc_prewrite(sw2, "SID", "Market_Leaders",
                               {"checked": 255, "flagged": 0, "open": 0,
                                "price_band": 0, "range": 0, "examples": []})
row2 = [kw for k, kw in rec2.calls if k == "append"][0]["body"]["values"][0]
check("S3.9 clean page -> INFO/OK, no ' | ex:'",
      row2[1] == "INFO" and row2[4] == "OK" and " | ex:" not in row2[5])

print()
print("S4  Run-log failure discipline")
rec3, sw3 = rec_writer(fail_times=1)
M._append_runlog_ohlc_prewrite(sw3, "S", "GM",
                               {"checked": 1, "flagged": 0})
check("S4.1 transient failure recovered on 2nd attempt",
      len([1 for k, _ in rec3.calls if k == "append"]) == 2)
rec4, sw4 = rec_writer(fail=["append"])
buf = io.StringIO()
raised = None
try:
    with contextlib.redirect_stdout(buf):
        M._append_runlog_ohlc_prewrite(sw4, "S", "GM",
                                       {"checked": 1, "flagged": 1,
                                        "open": 1, "examples": ["X"]})
except Exception as e:
    raised = e
check("S4.2 permanent failure: no raise, exactly 2 attempts, loud ::warning::",
      raised is None
      and len([1 for k, _ in rec4.calls if k == "append"]) == 2
      and "::warning::" in buf.getvalue())
env(TFB_SYNC_OHLC_RUNLOG="0")
rec5, sw5 = rec_writer()
M._append_runlog_ohlc_prewrite(sw5, "S", "GM", {"checked": 9, "flagged": 9})
check("S4.3 kill switch suppresses append", len(rec5.calls) == 0)
env(TFB_SYNC_OHLC_RUNLOG=None)
M._append_runlog_ohlc_prewrite(None, "S", "GM", {"checked": 1})
check("S4.4 sheets=None returns silently", True)

# =============================================================================
print()
print("=" * 78)
print("S5  Regression: unarmed repo is behaviourally pre-W1A-6")
print("=" * 78)
for var, fn in [("TFB_SYNC_OHLC_PREWRITE", M._ohlc_prewrite_enabled),
                ("TFB_SYNC_STATUS_STAMP", M._status_stamp_enabled)]:
    env(**{var: None})
    check(f"S5 {var} unset -> False", fn() is False)
    env(**{var: "0"})
    check(f"S5 {var}='0' (workflow default) -> False", fn() is False)
    env(**{var: "1"})
    check(f"S5 {var}='1' -> True", fn() is True)
env(TFB_SYNC_STATUS_STAMP="0")

# =============================================================================
print()
print("=" * 78)
print("S6  ENFORCE mutation contract (F-07) — real guard, synthetic rows")
print("=" * 78)
env(TFB_SYNC_OHLC_PREWRITE="1", TFB_SYNC_OHLC_PREWRITE_MODE="enforce",
    TFB_SYNC_OHLC_PREWRITE_TOL=None)


def run_guard(rows):
    m = [list(r) for r in rows]
    out, st = M._apply_ohlc_prewrite_guard(list(GHDR), m, "Global_Markets")
    return out, st


out, st = run_guard([["OPN1", "50", "100", "90", "95", ""]])
r = out[0]
check("S6.1 open offense: Open blanked ONLY",
      r[1] == "" and r[2] == "100" and r[3] == "90" and r[4] == "95",
      str(r))
check("S6.2 open offense: warning tag appended",
      r[5] == "ohlc_incoherent_dropped:open:prewrite", r[5])
check("S6.3 open offense: stats open=1 flagged=1",
      st["open"] == 1 and st["flagged"] == 1 and st["price_band"] == 0)

out, st = run_guard([["BND1", "95", "100", "90", "150", "prior_warn"]])
r = out[0]
check("S6.4 price_band offense: High+Low blanked, price is the KEPT anchor",
      r[2] == "" and r[3] == "" and r[4] == "150", str(r))
check("S6.5 price_band: Open untouched (short-circuit, one class per row)",
      r[1] == "95" and st["price_band"] == 1 and st["open"] == 0)
check("S6.6 price_band: tag appended AFTER existing warning with '; '",
      r[5] == "prior_warn; ohlc_incoherent_dropped:price_band:prewrite",
      r[5])

out, st = run_guard([["RNG1", "95", "90", "100", "95", ""]])
r = out[0]
check("S6.7 range offense (High<Low): High+Low blanked, price kept",
      r[2] == "" and r[3] == "" and r[4] == "95" and st["range"] == 1)
check("S6.8 range: tag == ohlc_incoherent_dropped:range:prewrite",
      r[5] == "ohlc_incoherent_dropped:range:prewrite", r[5])

clean = ["CLN1", "95", "100", "90", "96", ""]
out, st = run_guard([list(clean)])
check("S6.9 clean row: zero mutation, zero flags",
      out[0] == clean and st["flagged"] == 0)

zero = ["ZRO1", "20.91", "0", "0", "0", ""]
out, st = run_guard([list(zero)])
check("S6.10 all-zero band row is not judged (positivity screen) — "
      "documented guard gap, pinned as-is",
      out[0] == zero and st["flagged"] == 0)

rows = [["OPN1", "50", "100", "90", "95",
         "ohlc_incoherent_dropped:open:prewrite"]]
out, st = run_guard(rows)
check("S6.11 idempotent: existing tag not duplicated on re-run",
      out[0][5] == "ohlc_incoherent_dropped:open:prewrite", out[0][5])

mixed = [["OPN1", "50", "100", "90", "95", ""],
         list(clean),
         ["RNG1", "95", "90", "100", "95", ""]]
out, st = run_guard(mixed)
check("S6.12 mixed matrix: counts flagged=2 open=1 range=1, "
      "price NEVER blanked anywhere",
      st["flagged"] == 2 and st["open"] == 1 and st["range"] == 1
      and all(row[4] != "" for row in out))

env(TFB_SYNC_OHLC_PREWRITE_MODE="observe")
out, st = run_guard(mixed)
check("S6.13 same fixtures in OBSERVE: flags counted, ZERO cells mutated",
      st["flagged"] == 2 and out == mixed)

# =============================================================================
print()
print("=" * 78)
print("S7  _Status writer contract (item 18 + F-08) — real writer")
print("=" * 78)
env(TFB_SYNC_STATUS_STAMP="1", TFB_SYNC_STATUS_STAMP_PAGES=None,
    TFB_SYNC_STATUS_FRESH_MIN=None)
STATGRID = [["Page"], ["Market_Leaders"], ["Global_Markets"],
            ["Commodities_FX"]]

res = fresh_res(rows_written=6626, symbols_requested=6626,
                warnings=["w1", "w2"])
res._stamp_meta = {"requested": 6626, "pre_persist_rows": 6626,
                   "klg_kept": 100}
rec, sw = rec_writer(grid=STATGRID)
M._stamp_page_status(sw, "SID", "Global_Markets", res, 115)
ups = [kw for k, kw in rec.calls if k == "update"]
check("S7.1 existing page -> exactly one UPDATE, zero appends",
      len(ups) == 1 and not [1 for k, _ in rec.calls if k == "append"])
check("S7.2 update bounded to A{row}:J{row} of the page's own row (row 3)",
      ups[0]["range"] == "'_Status'!A3:J3", ups[0]["range"])
check("S7.3 valueInputOption=RAW (IR-029 locale-parse class blocked)",
      ups[0]["valueInputOption"] == "RAW")
p = ups[0]["body"]["values"][0]
check("S7.4 payload: A=page, E=backend endpoint, G=rows WRITTEN, J=warn count",
      p[0] == "Global_Markets" and p[4] == "backend:run_dashboard_sync"
      and p[6] == 6626 and p[9] == 2, f"G={p[6]} J={p[9]}")
check("S7.5 fresh coverage healthy -> Status=SUCCESS",
      p[2] == "SUCCESS", p[2])

res = fresh_res(rows_written=6626, symbols_requested=6626)
res._stamp_meta = {"requested": 6626, "pre_persist_rows": 3000,
                   "klg_kept": 100}
rec, sw = rec_writer(grid=STATGRID)
M._stamp_page_status(sw, "SID", "Global_Markets", res, 115)
p = [kw for k, kw in rec.calls if k == "update"][0]["body"]["values"][0]
check("S7.6 fresh 2900/6626 (43.8%) < min 95 -> Status=PARTIAL_FRESH",
      p[2] == "PARTIAL_FRESH", p[2])
check("S7.7 message publishes fresh_cov explicitly",
      "fresh_cov=43.8%" in p[3], p[3][:120])

res = fresh_res(sheet_name="Mutual_Funds", rows_written=2474)
rec, sw = rec_writer(grid=STATGRID)
M._stamp_page_status(sw, "SID", "Mutual_Funds", res, 115)
aps = [kw for k, kw in rec.calls if k == "append"]
check("S7.8 missing page -> APPEND to A1:J1, RAW",
      len(aps) == 1 and aps[0]["range"] == "'_Status'!A1:J1"
      and aps[0]["valueInputOption"] == "RAW")

res = fresh_res(status="failed", error="boom",
                symbols_requested=453)
res._stamp_meta = {}
rec, sw = rec_writer(grid=STATGRID)
M._stamp_page_status(sw, "SID", "Commodities_FX", res, 115)
p = [kw for k, kw in rec.calls if k == "update"][0]["body"]["values"][0]
check("S7.9 F-09: early exit (empty meta) still stamps requested=453 "
      "via res.symbols_requested fallback",
      "requested=453" in p[3] and p[2] == "FAILED", p[3][:110])

res = fresh_res(dry_run=True)
rec, sw = rec_writer(grid=STATGRID)
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    M._stamp_page_status(sw, "SID", "Global_Markets", res, 115)
check("S7.10 dry-run: ZERO Google calls + ::notice:: suppression",
      len(rec.calls) == 0 and "::notice::" in buf.getvalue()
      and "dry-run" in buf.getvalue())

res = fresh_res(status="skipped",
                warnings=["[MANUAL-HOLD v6.32.0] operator hold"])
rec, sw = rec_writer(grid=STATGRID)
M._stamp_page_status(sw, "SID", "Global_Markets", res, 115)
check("S7.11 MANUAL-HOLD: zero-write contract outranks telemetry",
      len(rec.calls) == 0)

res = fresh_res(sheet_name="Top_10_Investments", status="skipped",
                warnings=[f"{M._DECISION_GUARD_TAG} Top_10_Investments is "
                          f"decision-owned (cockpit); daily sync write "
                          f"skipped."])
check("S7.12 F-08: should_skip classifies the guard marker as "
      "'decision-owned'",
      M._status_stamp_should_skip(res) == "decision-owned")
rec, sw = rec_writer(grid=STATGRID)
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    M._stamp_page_status(sw, "SID", "Top_10_Investments", res, 30)
check("S7.13 F-08: decision-owned page is NEVER stamped (0 Google calls)",
      len(rec.calls) == 0 and "decision-owned" in buf.getvalue())

res = fresh_res()
rec, sw = rec_writer(grid=STATGRID, fail=["get"])
buf = io.StringIO()
raised = None
try:
    with contextlib.redirect_stdout(buf):
        M._stamp_page_status(sw, "SID", "Global_Markets", res, 115)
except Exception as e:
    raised = e
check("S7.14 key-column read failure: fail-open + loud ::warning::",
      raised is None and "::warning::" in buf.getvalue()
      and not [1 for k, _ in rec.calls if k in ("update", "append")])

rec, sw = rec_writer(grid=STATGRID, fail=["update"])
buf = io.StringIO()
raised = None
try:
    with contextlib.redirect_stdout(buf):
        M._stamp_page_status(sw, "SID", "Global_Markets", fresh_res(), 115)
except Exception as e:
    raised = e
check("S7.15 write failure: fail-open + loud 'stamp FAILED'",
      raised is None and "stamp FAILED" in buf.getvalue())

env(TFB_SYNC_STATUS_STAMP_PAGES="Market_Leaders")
rec, sw = rec_writer(grid=STATGRID)
M._stamp_page_status(sw, "SID", "Global_Markets", fresh_res(), 115)
check("S7.16 allow-list excludes non-listed page (0 calls)",
      len(rec.calls) == 0)
env(TFB_SYNC_STATUS_STAMP_PAGES=None, TFB_SYNC_STATUS_STAMP="0")

# =============================================================================
print()
print("=" * 78)
print("S8  CAP_BELOW_UNIVERSE on the readback path (F-10 / item 20)")
print("=" * 78)
SYMGRID = ([["Symbol", "Name", "C", "D", "E"]]
           + [[f"SYM{i}", f"Name {i}", "", "", ""] for i in range(10)])
env(TFB_SYNC_HEAL_FIRST=None)
rec, sw = rec_writer()
sw.read_values = lambda sid, sh, rng: [list(r) for r in SYMGRID]
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    out = M._read_existing_page_symbols(sw, "SID", "Global_Markets", 4)
check("S8.1 heal-first branch: truncates to cap AND emits pinned CAP literal",
      len(out) == 4
      and "[CAP v6.39.1] CAP_BELOW_UNIVERSE" in buf.getvalue()
      and "readback" in buf.getvalue(), buf.getvalue().strip()[:100])
env(TFB_SYNC_HEAL_FIRST="0")
rec, sw = rec_writer()
sw.read_values = lambda sid, sh, rng: [list(r) for r in SYMGRID]
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    out = M._read_existing_page_symbols(sw, "SID", "Global_Markets", 4)
check("S8.2 legacy branch: same cap + same pinned literal",
      len(out) == 4
      and "[CAP v6.39.1] CAP_BELOW_UNIVERSE" in buf.getvalue())
env(TFB_SYNC_HEAL_FIRST=None)
rec, sw = rec_writer()
sw.read_values = lambda sid, sh, rng: [list(r) for r in SYMGRID]
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    out = M._read_existing_page_symbols(sw, "SID", "Global_Markets", 50)
check("S8.3 cap above universe: full set, ZERO cap warnings",
      len(out) == 10 and "CAP_BELOW_UNIVERSE" not in buf.getvalue())

# =============================================================================
print()
print("=" * 78)
print("S9  Source-ordering contract (T7 drift guard)")
print("=" * 78)
src = open(RUNNER, encoding="utf-8").read()
m = re.search(r"async def _run_one_task\b.*?(?=\nasync def |\ndef main\b)",
              src, re.S)
body = m.group(0) if m else ""
i_en = body.find("_ohlc_prewrite_enabled():")
i_gd = body.find("_apply_ohlc_prewrite_guard(")
i_ap = body.find("_append_runlog_ohlc_prewrite(")
i_wr = body.find("v6.18.0 (Fix 2)")
i_st = body.find("_stamp_page_status(")
i_fin = body.find("finally:")
check("S9.1 guard chain ordered inside _run_one_task: "
      "enabled-gate < guard < appender < write seam",
      0 < i_en < i_gd < i_ap < i_wr,
      f"en={i_en} gd={i_gd} ap={i_ap} wr={i_wr}")
check("S9.2 appender is INSIDE the armed block (before the seam comment, "
      "after the gate)", i_en < i_ap < i_wr)
check("S9.3 _stamp_page_status remains in finally (covers every exit)",
      0 < i_fin < i_st, f"fin={i_fin} st={i_st}")

# =============================================================================
print()
print("=" * 78)
print("S10 TaskResult integration replay (real slots dataclass)")
print("=" * 78)
env(TFB_SYNC_OHLC_PREWRITE="1", TFB_SYNC_OHLC_PREWRITE_MODE="observe")
res = fresh_res(sheet_name="Commodities_FX")
rows = [["OPN1", "50", "100", "90", "95", ""]]
rec, sw = rec_writer()
raised = None
try:
    if M._ohlc_prewrite_enabled():
        matrix, _oc = M._apply_ohlc_prewrite_guard(list(GHDR),
                                                   [list(rows[0])],
                                                   res.sheet_name)
        if _oc.get("checked") and _oc["flagged"]:
            res.warnings.append("guardline")
        try:
            M._append_runlog_ohlc_prewrite(sw, "SID", res.sheet_name, _oc)
        except Exception:
            pass
except Exception as e:
    raised = e
check("S10.1 no exception; slots TaskResult accepted warning; one append",
      raised is None and len(res.warnings) == 1
      and len([1 for k, _ in rec.calls if k == "append"]) == 1)
check("S10.2 res.status untouched by telemetry", res.status == "success")

# =============================================================================
print()
print("=" * 78)
print("S11 ACTUAL-DATA evidence suite (2026-08-18 exports)")
print("=" * 78)
EXPECT = {"Commodities_FX": ("_Commodities_FX__10_.tsv", 453, 95, 94, 0, 1),
          "Market_Leaders": ("_Market_Leaders__10_.tsv", 255, 0, 0, 0, 0),
          "Mutual_Funds": ("_Mutual_Funds__11_.tsv", 2474, 55, 55, 0, 0),
          "Global_Markets": ("_Global_Markets__17_.tsv", 6626, 302, 301,
                             1, 0)}
first_fx = os.path.join(FIXROOT, FIXPREFIX + EXPECT["Market_Leaders"][0])
if DETERMINISTIC_ONLY:
    skip("S11 actual-data suite", "--deterministic mode (CI)")
elif not os.path.exists(first_fx):
    skip("S11 actual-data suite",
         f"fixtures not found under {FIXROOT} (set TFB_HARNESS_FIXTURES)")
else:
    env(TFB_SYNC_OHLC_PREWRITE="1", TFB_SYNC_OHLC_PREWRITE_MODE="observe",
        TFB_SYNC_OHLC_PREWRITE_TOL=None)
    total = 0
    for page, (fn, n, fl, o, b, rg) in EXPECT.items():
        rows = load_tsv(os.path.join(FIXROOT, FIXPREFIX + fn))
        headers, matrix = rows[0], [list(r) for r in rows[1:]]
        before = json.dumps(matrix)
        out, st = M._apply_ohlc_prewrite_guard(headers, matrix, page)
        total += st["flagged"]
        check(f"S11 {page}: checked={n} flagged={fl} open={o} band={b} "
              f"range={rg}",
              (st["checked"], st["flagged"], st["open"], st["price_band"],
               st["range"]) == (n, fl, o, b, rg),
              f'got {st["checked"]}/{st["flagged"]}/{st["open"]}/'
              f'{st["price_band"]}/{st["range"]}')
        check(f"S11 {page}: OBSERVE mutated ZERO cells",
              json.dumps(out) == before)
    check("S11 ML blank-Open negative control: flagged==0",
          EXPECT["Market_Leaders"][2] == 0)
    check(f"S11 total flagged {total} within ±25% of the 551 projection",
          abs(total - 551) / 551 <= 0.25,
          f"delta={100 * (total - 551) / 551:+.1f}%")

# =============================================================================
print()
print("=" * 78)
print("S12 W1A-4a upstream verdict (v6.40.0) — pure compose + writer")
print("=" * 78)
env(TFB_SYNC_UPSTREAM_VERDICT=None)
check("S12.1 gate unset -> disabled (v6.39.5 byte-behaviour)",
      M._upstream_verdict_enabled() is False)
env(TFB_SYNC_UPSTREAM_VERDICT="0")
check("S12.2 '0' (workflow default) -> disabled",
      M._upstream_verdict_enabled() is False)
env(TFB_SYNC_UPSTREAM_VERDICT="1", TFB_SYNC_VERDICT_PAGES=None,
    TFB_SYNC_VERDICT_MAX_AGE_MIN=None)
import time as _t
_now = _t.time()
ALL_OK = {p: ("OK", _now - 60) for p in
          ["Market_Leaders", "Global_Markets", "Commodities_FX",
           "Mutual_Funds"]}
v, s = M._uv_compose(dict(ALL_OK), _now)
check("S12.3 all four OK+fresh -> EXECUTABLE", v == "EXECUTABLE", v)
check("S12.4 summary lists all four abbreviations",
      all(t in s for t in ("ML:OK", "GM:OK", "CFX:OK", "MF:OK")), s)
bad = dict(ALL_OK); bad["Global_Markets"] = ("STALE_COV", _now - 60)
v, _ = M._uv_compose(bad, _now)
check("S12.5 one STALE_COV -> NOT_ACTIONABLE(stale_cov:GM)",
      v == "NOT_ACTIONABLE(stale_cov:GM)", v)
aged = dict(ALL_OK); aged["Mutual_Funds"] = ("OK", _now - 300 * 60)
v, _ = M._uv_compose(aged, _now)
check("S12.6 OK but older than 240min -> AGED -> NOT_ACTIONABLE",
      v == "NOT_ACTIONABLE(aged:MF)", v)
miss = dict(ALL_OK); miss.pop("Commodities_FX")
v, _ = M._uv_compose(miss, _now)
check("S12.7 missing page -> NOT_ACTIONABLE(missing:CFX)",
      v == "NOT_ACTIONABLE(missing:CFX)", v)
st, cov = M._uv_page_state(fresh_res(
    status="success", symbols_requested=100))
check("S12.8 success w/o meta -> OK, cov None (no fake numbers)",
      st == "OK" and cov is None, f"{st}/{cov}")
r = fresh_res(status="success", symbols_requested=100)
r._stamp_meta = {"requested": 100, "pre_persist_rows": 50, "klg_kept": 10}
check("S12.9 success at 40% coverage -> STALE_COV",
      M._uv_page_state(r) == ("STALE_COV", 40.0))
check("S12.10 parser round-trips writer format",
      M._uv_parse_value("OK | cov=99.1 | run=7 | 2026-08-18 21:00:00")[0]
      == "OK")

UVGRID = [["Global Key", "Value"], ["Last Global Update", "6/6/2026"],
          ["Backend URL", "https://x"], [], [], []]
res_gm = fresh_res(sheet_name="Global_Markets", status="success",
                   symbols_requested=6626)
res_gm._stamp_meta = {"requested": 6626, "pre_persist_rows": 6626,
                      "klg_kept": 0}
rec, sw = rec_writer(grid=UVGRID)
M._write_upstream_verdict(sw, "SID", [res_gm])
ups = [kw for k, kw in rec.calls if k == "update"]
# v2.2.0 (sync v6.53.0): the writer now upserts a THIRD bounded key,
# "TFB Grid Capacity", through the same L:M closure - still ZERO appends.
_cap_on = getattr(M, "_capacity_status_enabled", lambda: False)()
_exp_updates = 3 if _cap_on else 2
check("S12.11 writer: bounded L{r}:M{r} updates ONLY, ZERO appends",
      len(ups) == _exp_updates
      and not [1 for k, _ in rec.calls if k == "append"]
      and all(str(u["range"]).startswith("'_Status'!L")
              and ":M" in str(u["range"]) for u in ups),
      f"updates={len(ups)} (expected {_exp_updates})")
check("S12.11b capacity key: third bounded L6:M6 RAW update, UNKNOWN when "
      "metadata is unreadable, never an append",
      (not _cap_on) or (
          ups[2]["range"] == "'_Status'!L6:M6"
          and ups[2]["valueInputOption"] == "RAW"
          and ups[2]["body"]["values"][0][0] == "TFB Grid Capacity"
          and str(ups[2]["body"]["values"][0][1]).startswith(
              ("UNKNOWN | allocated=n/a", "OK | allocated=",
               "NEAR-LIMIT | allocated=", "AT-LIMIT | allocated="))),
      (ups[2]["range"] if len(ups) > 2 else "no third update"))
check("S12.12 new page key took first blank slot L4:M4, RAW",
      ups[0]["range"] == "'_Status'!L4:M4"
      and ups[0]["valueInputOption"] == "RAW", ups[0]["range"])
check("S12.13 composite upserted at next blank L5:M5",
      ups[1]["range"] == "'_Status'!L5:M5", ups[1]["range"])
comp = ups[1]["body"]["values"][0]
check("S12.14 composite key/value: GM fresh-OK but ML/CFX/MF missing "
      "-> NOT_ACTIONABLE(missing:ML)",
      comp[0] == "TFB Decision Feed"
      and comp[1].startswith("NOT_ACTIONABLE(missing:ML)"), comp[1][:60])
FULLGRID = [["Global Key", "Value"], ["Backend URL", "https://x"],
            ["TFB Feed Market_Leaders",
             f"OK | cov=100 | run=1 | {_t.strftime('%Y-%m-%d %H:%M:%S')}"],
            ["TFB Feed Commodities_FX",
             f"OK | cov=100 | run=1 | {_t.strftime('%Y-%m-%d %H:%M:%S')}"],
            ["TFB Feed Mutual_Funds",
             f"OK | cov=100 | run=1 | {_t.strftime('%Y-%m-%d %H:%M:%S')}"],
            ["TFB Feed Global_Markets", "FAILED | cov=n/a | run=1 | "
             + _t.strftime('%Y-%m-%d %H:%M:%S')],
            ["TFB Decision Feed", "stale"], []]
rec, sw = rec_writer(grid=FULLGRID)
M._write_upstream_verdict(sw, "SID", [res_gm])
comp = [kw for k, kw in rec.calls if k == "update"
        and kw["range"].endswith("L7:M7")][0]["body"]["values"][0]
check("S12.15 overlay: this job's fresh GM:OK overrides stored FAILED; "
      "other legs' keys honoured -> EXECUTABLE",
      comp[1].startswith("EXECUTABLE"), comp[1][:70])
NOKEY = [["Wrong", "Block"], [], []]
rec, sw = rec_writer(grid=NOKEY)
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    M._write_upstream_verdict(sw, "SID", [res_gm])
check("S12.16 layout self-check: unknown block -> refuse, loud, ZERO writes",
      not [1 for k, _ in rec.calls if k == "update"]
      and "self-check failed" in buf.getvalue())
rec, sw = rec_writer(grid=UVGRID, fail=["update"])
buf = io.StringIO(); raised = None
try:
    with contextlib.redirect_stdout(buf):
        M._write_upstream_verdict(sw, "SID", [res_gm])
except Exception as e:
    raised = e
check("S12.17 write failure: fail-open, no raise, loud ::warning::",
      raised is None and "::warning::" in buf.getvalue())
env(TFB_SYNC_UPSTREAM_VERDICT="0")

# =============================================================================
print()
print("=" * 78)
npass = sum(1 for _, ok, _ in RESULTS if ok)
print(f"HARNESS v2.2.0 RESULT: {npass}/{len(RESULTS)} PASS"
      + (f"  ({len(SKIPPED)} suite(s) skipped)" if SKIPPED else ""))
for n, ok, d in RESULTS:
    if not ok:
        print("  FAILED:", n, d)
print("=" * 78)
sys.exit(0 if npass == len(RESULTS) else 1)
