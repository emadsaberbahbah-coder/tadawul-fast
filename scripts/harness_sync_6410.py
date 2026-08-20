#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/harness_sync_6410.py — W1A-6c harness (v1.0.0, 2026-08-20)
==============================================================================
Certifies scripts/run_dashboard_sync.py v6.41.0: the blank_* counters and the
post-write readback.

Executes the REAL functions lifted from the REAL source file. No stand-in
guard, no re-implemented predicate, no mocked stats dict — the standing rule
after the TaskResult slots defect was missed by a FakeRes pattern. The only
fake object here is a Sheets double, and it is deliberately a RECORDER: every
API call it receives is captured and asserted on, which is how "read-only" is
proved rather than claimed.

PORTABILITY (the harness_v5130_1 / harness_ob_1_13_0 defect, not repeated):
paths resolve from this file's own location; --old / --data / --require-* are
optional overrides; missing inputs DEGRADE with a message in smoke mode and
FAIL in certification mode. Runs from a bare `git clone` with no arguments.

  NEW source : <repo>/scripts/run_dashboard_sync.py
  OLD source : --old <dir> | TFB_HARNESS_OLD_TREE   (differential suite)
  Exports    : --data <dir> | TFB_HARNESS_DATA | ./ | /mnt/user-data/uploads
               (--data NONE forces fixture-only)

USAGE
  python3 scripts/harness_sync_6410.py
  python3 scripts/harness_sync_6410.py --old /tmp/head_640 --data /tmp/tsv \
      --require-old --require-live

EXIT 0 iff every assertion passes.
"""
from __future__ import annotations

import argparse
import ast
import copy
import csv
import glob
import os
import sys
import types
import typing
from typing import Any, Dict, List, Optional

HARNESS_VERSION = "1.1.0"
EXPECT_NEW_VERSION = "6.41.0"
EXPECT_OLD_VERSION = "6.40.0"

PAGES = ("Global_Markets", "Market_Leaders", "Commodities_FX", "Mutual_Funds")
EXPORT_PREFIX = "_Market_Share_Deepseek-V3_-_"
REQUIRED_HEADERS = ("Symbol", "Open", "Day High", "Day Low", "Current Price")

LIFT_FUNCS = {
    "_apply_ohlc_prewrite_guard", "_guard_find_col", "_guard_is_blank",
    "_guard_norm", "_ohlc_prewrite_num", "_ohlc_prewrite_tol",
    "_ohlc_prewrite_mode", "_ohlc_prewrite_enabled",
    "_ohlc_readback_enabled", "_ohlc_readback_verify",
    "_ohlc_readback_status",
    "_page_read_row_bound", "_idx_to_a1_col", "re",
}
LIFT_CONSTS = {
    "_GUARD_SYMBOL_ALIASES", "_GUARD_OPEN_ALIASES", "_GUARD_PRICE_ALIASES",
    "_GUARD_DAYHIGH_ALIASES", "_GUARD_DAYLOW_ALIASES",
    "_OHLC_PREWRITE_TAG", "_OHLC_READBACK_TAG", "SCRIPT_VERSION",
}
GATE_KEYS = ("TFB_SYNC_OHLC_PREWRITE", "TFB_SYNC_OHLC_PREWRITE_MODE",
             "TFB_SYNC_OHLC_PREWRITE_TOL", "TFB_SYNC_OHLC_READBACK",
             "TFB_SYNC_OHLC_RUNLOG", "TFB_SYNC_PAGE_READ_MAX_ROW")

_FAILS: List[str] = []
_PASSES = 0


def ck(name: str, cond: bool, detail: str = "") -> None:
    global _PASSES
    if cond:
        _PASSES += 1
        print("  PASS  " + name + ((" | " + detail) if detail else ""))
    else:
        _FAILS.append(name)
        print("  FAIL  " + name + ((" | " + detail) if detail else ""))


def section(t: str) -> None:
    print("\n" + "-" * 78 + "\n" + t + "\n" + "-" * 78)


def lift(path: str) -> Dict[str, Any]:
    """Extract the real functions/constants into a clean namespace without
    importing the module (it has heavy prod deps and start-up side effects)."""
    src = open(path, encoding="utf-8").read()
    tree = ast.parse(src)
    segs: List[str] = []
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name in LIFT_FUNCS:
            segs.append(ast.get_source_segment(src, n))
        elif isinstance(n, ast.Assign) and any(
                getattr(t, "id", "") in LIFT_CONSTS for t in n.targets):
            segs.append(ast.get_source_segment(src, n))
    ns: Dict[str, Any] = {
        "os": os, "re": __import__("re"), "time": __import__("time"),
        "json": __import__("json"), "Any": typing.Any, "List": typing.List,
        "Optional": typing.Optional, "Dict": typing.Dict,
        "Tuple": typing.Tuple, "logger": _NullLogger(),
    }
    exec("\n\n".join(s for s in segs if s), ns)
    ns["__source__"] = src
    return ns


class _NullLogger:
    def __getattr__(self, _):
        return lambda *a, **k: None


class RecorderSheets:
    """Sheets double that RECORDS every call. Proving read-only means proving
    no write method was ever reached, so this exposes writers too."""

    def __init__(self, grid: Optional[List[List[Any]]], fail: bool = False):
        self.grid = grid
        self.fail = fail
        self.calls: List[str] = []

    def read_values(self, sid, name, a1_range="A1:EZ2000"):
        self.calls.append("read:%s:%s" % (name, a1_range))
        if self.fail:
            raise RuntimeError("simulated transport failure")
        return self.grid

    def write_table(self, *a, **k):
        self.calls.append("WRITE")
        raise AssertionError("readback must never write")

    def clear_from(self, *a, **k):
        self.calls.append("CLEAR")
        raise AssertionError("readback must never clear")

    def _get_service(self):
        self.calls.append("service")
        return None

    @property
    def reads(self):
        return [c for c in self.calls if c.startswith("read:")]

    @property
    def mutations(self):
        return [c for c in self.calls if c in ("WRITE", "CLEAR")]


def set_gates(**kw) -> None:
    for k in GATE_KEYS:
        os.environ.pop(k, None)
    for k, v in kw.items():
        if v is not None:
            os.environ[k] = str(v)


def find_export(d: str, page: str) -> Optional[str]:
    exact = os.path.join(d, EXPORT_PREFIX + page + ".tsv")
    if os.path.isfile(exact):
        return exact
    toks = page.split("_")
    c: List[str] = []
    for pat in ("*" + "*".join(toks) + "*.tsv", "*" + " ".join(toks) + "*.tsv"):
        c += glob.glob(os.path.join(d, pat))
    c = sorted(set(x for x in c if os.path.isfile(x)),
               key=os.path.getmtime, reverse=True)
    return c[0] if c else None


def load_page(d: str, page: str):
    p = find_export(d, page)
    if p is None:
        return None, None
    csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))
    with open(p, newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.reader(fh, delimiter="\t"))
    if not rows:
        return None, None
    hdr = [str(c or "").strip() for c in rows[0]]
    missing = [h for h in REQUIRED_HEADERS if h not in hdr]
    if missing:
        raise RuntimeError("%s missing headers: %s"
                           % (os.path.basename(p), missing))
    body = [list(r) for r in rows[1:] if any(str(c or "").strip() for c in r)]
    return hdr, body


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", default=os.getenv("TFB_HARNESS_OLD_TREE", ""))
    ap.add_argument("--data", default=os.getenv("TFB_HARNESS_DATA", ""))
    ap.add_argument("--require-old", action="store_true")
    ap.add_argument("--require-live", action="store_true")
    args = ap.parse_args()

    here = os.path.dirname(os.path.abspath(__file__))
    repo = os.path.dirname(here) if os.path.basename(here) == "scripts" else here
    new_path = os.path.join(repo, "scripts", "run_dashboard_sync.py")
    if not os.path.isfile(new_path):
        new_path = os.path.join(repo, "run_dashboard_sync.py")

    print("=" * 78)
    print("HARNESS harness_sync_6410 v%s — W1A-6c blank counters + readback"
          % HARNESS_VERSION)
    print("=" * 78)
    if args.require_old or args.require_live:
        print("MODE      : CERTIFICATION")
    print("new source: " + new_path)
    if not os.path.isfile(new_path):
        print("FATAL: source not found")
        return 2
    new = lift(new_path)
    print("new version: " + str(new.get("SCRIPT_VERSION")))

    old = None
    if args.old:
        op = os.path.join(args.old, "scripts", "run_dashboard_sync.py")
        if not os.path.isfile(op):
            op = os.path.join(args.old, "run_dashboard_sync.py")
        if os.path.isfile(op):
            old = lift(op)
            print("old source : %s (v%s)" % (op, old.get("SCRIPT_VERSION")))
        else:
            print("old source : NOT FOUND at %s" % op)
            if args.require_old:
                ck("S0.1 CERT: old baseline required", False, op)
    elif args.require_old:
        ck("S0.1 CERT: old baseline required", False, "--old not supplied")

    data_dir = ""
    if str(args.data).upper() != "NONE":
        for cand in (args.data, os.getcwd(), "/mnt/user-data/uploads"):
            if cand and find_export(cand, "Global_Markets"):
                data_dir = cand
                break
        print("exports    : " + (data_dir or "NOT FOUND — live suite skipped"))
    else:
        print("exports    : DISABLED by --data NONE")
    if args.require_live and not data_dir:
        ck("S0.2 CERT: live exports required", False, "none resolved")

    # ---------------- S1 contract ----------------
    section("S1  CONTRACT")
    ck("S1.1 version is " + EXPECT_NEW_VERSION,
       str(new.get("SCRIPT_VERSION")) == EXPECT_NEW_VERSION,
       str(new.get("SCRIPT_VERSION")))
    if old is not None:
        ck("S1.2 old baseline is " + EXPECT_OLD_VERSION,
           str(old.get("SCRIPT_VERSION")) == EXPECT_OLD_VERSION,
           str(old.get("SCRIPT_VERSION")))
    src = new["__source__"]
    import re as _re
    _new_keys = set(_re.findall(r"TFB_SYNC_[A-Z_]+", src))
    _old_keys = set(_re.findall(r"TFB_SYNC_[A-Z_]+",
                                old["__source__"])) if old else None
    if _old_keys is not None:
        ck("S1.3 ENV key-set delta is EXACTLY {TFB_SYNC_OHLC_READBACK}",
           _new_keys - _old_keys == {"TFB_SYNC_OHLC_READBACK"},
           str(sorted(_new_keys - _old_keys)))
    else:
        ck("S1.3 readback gate present in source",
           "TFB_SYNC_OHLC_READBACK" in _new_keys)
    set_gates()
    ck("S1.4 readback gate DEFAULT OFF",
       new["_ohlc_readback_enabled"]() is False)
    ck("S1.5 readback tag present", "_OHLC_READBACK_TAG" in new)

    # ---------------- S2 blank counters ----------------
    section("S2  BLANK COUNTERS (W1A-6c part A)")
    set_gates(TFB_SYNC_OHLC_PREWRITE="1", TFB_SYNC_OHLC_PREWRITE_MODE="observe")
    hdr = ["Symbol", "Open", "Day High", "Day Low", "Current Price",
           "Warnings"]
    rows = [
        ["CLEAN", "9.9", "10.5", "9.0", "10.0", ""],      # clean
        ["NOOPEN", "", "10.5", "9.0", "10.0", ""],        # blank open
        ["NOHI", "9.9", "", "9.0", "10.0", ""],           # blank high
        ["NOLO", "9.9", "10.5", "", "10.0", ""],          # blank low
        ["BAD", "4.64", "7.12", "7.05", "7.09", ""],      # real offense
    ]
    _, st = new["_apply_ohlc_prewrite_guard"](hdr, copy.deepcopy(rows), "GM")
    ck("S2.1 blank_open counted", st.get("blank_open") == 1,
       str(st.get("blank_open")))
    ck("S2.2 blank_hi counted", st.get("blank_hi") == 1,
       str(st.get("blank_hi")))
    ck("S2.3 blank_lo counted", st.get("blank_lo") == 1,
       str(st.get("blank_lo")))
    ck("S2.4 blanks are NOT offenses", st.get("flagged") == 1,
       "flagged=%s open=%s" % (st.get("flagged"), st.get("open")))
    ck("S2.5 checked counts every row", st.get("checked") == 5,
       str(st.get("checked")))
    if old is not None:
        _, sto = old["_apply_ohlc_prewrite_guard"](
            hdr, copy.deepcopy(rows), "GM")
        ck("S2.6 v6.40.0 had no blank_* keys (the gap this closes)",
           "blank_open" not in sto, str(sorted(sto.keys())))
        ck("S2.7 flagged/checked UNCHANGED vs v6.40.0 — predicate untouched",
           sto.get("flagged") == st.get("flagged")
           and sto.get("checked") == st.get("checked")
           and sto.get("open") == st.get("open"),
           "old=%s/%s new=%s/%s" % (sto.get("flagged"), sto.get("checked"),
                                    st.get("flagged"), st.get("checked")))

    # ---------------- S3 readback behaviour ----------------
    section("S3  READBACK (W1A-6c part B)")
    live = [hdr] + [list(r) for r in rows]
    set_gates(TFB_SYNC_OHLC_PREWRITE="1", TFB_SYNC_OHLC_READBACK="1")
    sh = RecorderSheets(live)
    pw = {"checked": 5, "flagged": 1, "blank_open": 1}
    matrix = copy.deepcopy(rows)
    snap = copy.deepcopy(matrix)
    rb = new["_ohlc_readback_verify"](sh, "sid", "Global_Markets", hdr,
                                      matrix, "A1", pw)
    ck("S3.1 returns a delta dict", isinstance(rb, dict) and not rb.get("error"),
       str(rb)[:90])
    ck("S3.2 READ-ONLY — zero write/clear calls", sh.mutations == [],
       str(sh.calls))
    ck("S3.3 exactly ONE page read", len(sh.reads) == 1, str(sh.reads))
    ck("S3.4 outgoing matrix byte-untouched", matrix == snap)
    ck("S3.5 delta computed", rb.get("delta_flagged") == 0
       and rb.get("readback_flagged") == 1,
       "pw=%s rb=%s" % (rb.get("prewrite_flagged"),
                        rb.get("readback_flagged")))

    set_gates(TFB_SYNC_OHLC_PREWRITE="1", TFB_SYNC_OHLC_READBACK="1")
    dirty = [hdr] + [list(r) for r in rows] + [
        ["RESIDENT1", "2.0", "10.5", "9.0", "10.0", ""],
        ["RESIDENT2", "99.0", "10.5", "9.0", "10.0", ""]]
    sh2 = RecorderSheets(dirty)
    rb2 = new["_ohlc_readback_verify"](sh2, "sid", "Global_Markets", hdr,
                                       copy.deepcopy(rows), "A1", pw)
    ck("S3.6 DIVERGENCE detected — the 618-row case in miniature",
       rb2.get("delta_flagged") == 2 and rb2.get("delta_checked") == 2,
       "delta_flagged=%s delta_checked=%s"
       % (rb2.get("delta_flagged"), rb2.get("delta_checked")))

    set_gates(TFB_SYNC_OHLC_PREWRITE="1", TFB_SYNC_OHLC_READBACK="1",
              TFB_SYNC_OHLC_PREWRITE_MODE="enforce")
    en = [hdr] + [list(r) for r in rows]
    sh3 = RecorderSheets(en)
    before = copy.deepcopy(en)
    new["_ohlc_readback_verify"](sh3, "sid", "GM", hdr,
                                 copy.deepcopy(rows), "A1", pw)
    ck("S3.7 forced-observe: enforce armed, read copy NOT mutated",
       en == before)
    ck("S3.8 operator's enforce setting restored after readback",
       os.environ.get("TFB_SYNC_OHLC_PREWRITE_MODE") == "enforce",
       str(os.environ.get("TFB_SYNC_OHLC_PREWRITE_MODE")))

    set_gates(TFB_SYNC_OHLC_PREWRITE="1", TFB_SYNC_OHLC_READBACK="1")
    ck("S3.9 transport failure -> error dict, no raise",
       (new["_ohlc_readback_verify"](RecorderSheets(None, fail=True), "s",
                                     "GM", hdr, [], "A1", pw) or {}
        ).get("error") is not None)
    ck("S3.10 read returns None -> read_failed",
       (new["_ohlc_readback_verify"](RecorderSheets(None), "s", "GM", hdr,
                                     [], "A1", pw) or {}
        ).get("error") == "read_failed")
    ck("S3.11 sheets=None -> None, no raise",
       new["_ohlc_readback_verify"](None, "s", "GM", hdr, [], "A1", pw)
       is None)
    set_gates(TFB_SYNC_OHLC_PREWRITE="1")
    sh4 = RecorderSheets(live)
    ck("S3.12 gate OFF -> returns None and performs ZERO reads",
       new["_ohlc_readback_verify"](sh4, "s", "GM", hdr, [], "A1", pw) is None
       and sh4.calls == [])
    # ---- v1.0.1: review-battery cases promoted into the harness ----
    set_gates(TFB_SYNC_OHLC_PREWRITE="1", TFB_SYNC_OHLC_READBACK="1")
    sh13 = RecorderSheets(live)
    new["_ohlc_readback_verify"](sh13, "s", "GM", hdr, [], "A1", pw)
    _rng = sh13.reads[0].split(":", 2)[2] if sh13.reads else ""
    _want_col = new["_idx_to_a1_col"](len(hdr))     # 1-BASED: 6 cols -> F
    ck("S3.13 read range covers ALL columns (E6 fix: 1-based col math)",
       _rng.startswith("A1:" + _want_col),
       "range=%s want_end_col=%s" % (sh13.reads, _want_col))
    sh14 = RecorderSheets([hdr] + [list(r) for r in rows])
    rb14 = new["_ohlc_readback_verify"](sh14, "s", "GM", hdr,
                                        copy.deepcopy(rows), "B5", pw)
    ck("S3.14 start_cell honored on BOTH axes — B5 + 6 cols reads B5:G "
       "(audit F3)",
       sh14.reads and sh14.reads[0].split(":", 2)[2].startswith("B5:G")
       and isinstance(rb14, dict) and not rb14.get("error"),
       str(sh14.reads))
    ck("S3.15 empty grid -> empty_readback",
       (new["_ohlc_readback_verify"](RecorderSheets([]), "s", "GM", hdr,
                                     [], "A1", pw) or {}
        ).get("error") == "empty_readback")
    set_gates(TFB_SYNC_OHLC_PREWRITE="1", TFB_SYNC_OHLC_READBACK="1",
              TFB_SYNC_OHLC_PREWRITE_MODE="enforce")
    _real = new["_apply_ohlc_prewrite_guard"]
    def _boom(*a, **k):
        raise ValueError("guard-explodes")
    new["_apply_ohlc_prewrite_guard"] = _boom
    rb16 = new["_ohlc_readback_verify"](RecorderSheets(live), "s", "GM",
                                        hdr, [], "A1", pw)
    new["_apply_ohlc_prewrite_guard"] = _real
    ck("S3.16 in-guard exception -> error dict AND enforce mode restored",
       isinstance(rb16, dict) and "ValueError" in str(rb16.get("error"))
       and os.environ.get("TFB_SYNC_OHLC_PREWRITE_MODE") == "enforce")
    m17 = [list(rows[4])]                       # the BAD row
    set_gates(TFB_SYNC_OHLC_PREWRITE="1", TFB_SYNC_OHLC_PREWRITE_MODE="enforce")
    _, st17 = new["_apply_ohlc_prewrite_guard"](hdr, m17, "GM")
    set_gates(TFB_SYNC_OHLC_PREWRITE="1", TFB_SYNC_OHLC_READBACK="1",
              TFB_SYNC_OHLC_PREWRITE_MODE="enforce")
    rb17 = new["_ohlc_readback_verify"](RecorderSheets([hdr] + m17), "s",
                                        "GM", hdr, m17, "A1", st17)
    ck("S3.17 enforce-blanked write -> NEGATIVE delta, blank_open on rb",
       rb17.get("delta_flagged") == -1
       and rb17.get("readback_blank_open") == 1,
       "delta=%s rb_blank=%s" % (rb17.get("delta_flagged"),
                                 rb17.get("readback_blank_open")))
    # ---- v1.1.0: pre-merge audit remediations promoted into the suite ----
    set_gates(TFB_SYNC_OHLC_READBACK="1")          # baseline OFF (audit F2)
    sh18 = RecorderSheets(live)
    ck("S3.18 baseline contract — READBACK without PREWRITE self-disables, "
       "ZERO reads (audit F2)",
       new["_ohlc_readback_verify"](sh18, "s", "GM", hdr, [], "A1", {})
       is None and sh18.calls == [])
    ck("S3.19a status helper exists in module (audit F4)",
       "_ohlc_readback_status" in new)
    _stat = new.get("_ohlc_readback_status") or (lambda d: ("", ""))
    ck("S3.19 status taxonomy (audit F4): DIVERGENT/REDUCED/ROWS_DELTA/"
       "MATCHED",
       _stat({"delta_flagged": 3, "delta_checked": 0})
       == ("WARNING", "DIVERGENT")
       and _stat({"delta_flagged": -1, "delta_checked": 0})
       == ("INFO", "REDUCED")
       and _stat({"delta_flagged": 0, "delta_checked": 2})
       == ("WARNING", "ROWS_DELTA")
       and _stat({"delta_flagged": 0, "delta_checked": -1})
       == ("INFO", "ROWS_DELTA")
       and _stat({"delta_flagged": 0, "delta_checked": 0})
       == ("INFO", "MATCHED"))

    # ---- S7 INTEGRATION (audit F1): the feature must be WIRED, not just
    # defined. The auditor deleted the production call site and this
    # harness still certified 36/36 — a suite that cannot notice its
    # feature being removed certifies nothing. These are static AST proofs
    # over the REAL source of the REAL integration point.
    section("S7  INTEGRATION — the call site itself (audit F1)")
    import ast as _ast
    _tree = _ast.parse(src)
    _rot = None
    for _n in _ast.walk(_tree):          # method or function, any nesting
        if isinstance(_n, (_ast.FunctionDef, _ast.AsyncFunctionDef)) \
                and _n.name == "_run_one_task":
            _rot = _ast.get_source_segment(src, _n)
            break
    ck("S7.1 _run_one_task exists in source", _rot is not None)
    _rot = _rot or ""
    ck("S7.2 _run_one_task CALLS _ohlc_readback_verify",
       "_ohlc_readback_verify(" in _rot)
    ck("S7.3 _run_one_task appends the readback _Run_Log line",
       "_append_runlog_ohlc_readback(" in _rot)
    ck("S7.4 call is gated by _ohlc_readback_enabled()",
       "_ohlc_readback_enabled()" in _rot)
    ck("S7.5 baseline skip branch present at the call site (audit F2)",
       "not _ohlc_prewrite_enabled()" in _rot)
    _wi = _rot.find("write_table(")
    _ri = _rot.find("_ohlc_readback_verify(")
    ck("S7.6 readback sits AFTER write_table in the task flow",
       0 <= _wi < _ri, "write@%d readback@%d" % (_wi, _ri))
    ck("S7.7 call site uses the status helper (audit F4)",
       "_ohlc_readback_status(" in _rot)

    # ---------------- S4 live data ----------------
    if data_dir:
        section("S4  LIVE EXPORT — real guard, real rows")
        set_gates(TFB_SYNC_OHLC_PREWRITE="1",
                  TFB_SYNC_OHLC_PREWRITE_MODE="observe")
        tot = {"checked": 0, "flagged": 0, "blank_open": 0}
        for page in PAGES:
            try:
                h, b = load_page(data_dir, page)
            except RuntimeError as e:
                ck("S4.%s headers valid" % page, not args.require_live, str(e))
                continue
            if h is None:
                print("  ....  %s absent — skipped" % page)
                continue
            _, st = new["_apply_ohlc_prewrite_guard"](h, copy.deepcopy(b), page)
            for k in tot:
                tot[k] += int(st.get(k) or 0)
            ck("S4.%s guard ran, counters coherent" % page,
               st["checked"] == len(b)
               and st["flagged"] <= st["checked"]
               and st["blank_open"] <= st["checked"],
               "checked=%d flagged=%d blank_open=%d"
               % (st["checked"], st["flagged"], st["blank_open"]))
            if page == "Market_Leaders":
                ck("S4.ML negative control — Open unpopulated",
                   st["blank_open"] == st["checked"] and st["flagged"] == 0,
                   "blank_open=%d/%d flagged=%d"
                   % (st["blank_open"], st["checked"], st["flagged"]))
            # readback over the same page must reproduce the guard exactly
            sh5 = RecorderSheets([h] + copy.deepcopy(b))
            set_gates(TFB_SYNC_OHLC_PREWRITE="1", TFB_SYNC_OHLC_READBACK="1")
            rbl = new["_ohlc_readback_verify"](
                sh5, "sid", page, h, copy.deepcopy(b), "A1", st)
            ck("S4.%s readback == prewrite on identical rows" % page,
               isinstance(rbl, dict) and rbl.get("delta_flagged") == 0
               and rbl.get("delta_checked") == 0,
               "delta=%s/%s" % (rbl.get("delta_flagged"),
                                rbl.get("delta_checked")) if rbl else "None")
            set_gates(TFB_SYNC_OHLC_PREWRITE="1",
                      TFB_SYNC_OHLC_PREWRITE_MODE="observe")
        ck("S4.TOTAL pool measured",
           tot["checked"] > 0,
           "checked=%d flagged=%d blank_open=%d"
           % (tot["checked"], tot["flagged"], tot["blank_open"]))
        if old is not None:
            section("S5  DIFFERENTIAL v6.40.0 -> v6.41.0")
            for page in PAGES:
                try:
                    h, b = load_page(data_dir, page)
                except RuntimeError:
                    continue
                if h is None:
                    continue
                _, a = old["_apply_ohlc_prewrite_guard"](
                    h, copy.deepcopy(b), page)
                _, n2 = new["_apply_ohlc_prewrite_guard"](
                    h, copy.deepcopy(b), page)
                ck("S5.%s verdict identical — only telemetry added" % page,
                   a["checked"] == n2["checked"]
                   and a["flagged"] == n2["flagged"]
                   and a["open"] == n2["open"]
                   and a["price_band"] == n2["price_band"]
                   and a["range"] == n2["range"],
                   "old %d/%d new %d/%d" % (a["flagged"], a["checked"],
                                            n2["flagged"], n2["checked"]))
    else:
        section("S4/S5  LIVE SUITES SKIPPED")

    set_gates()
    print("\n" + "=" * 78)
    print("HARNESS RESULT: %d/%d PASS%s"
          % (_PASSES, _PASSES + len(_FAILS),
             "" if not _FAILS else "  —  FAILURES: " + ", ".join(_FAILS)))
    print("=" * 78)
    return 0 if not _FAILS else 1


if __name__ == "__main__":
    sys.exit(main())
