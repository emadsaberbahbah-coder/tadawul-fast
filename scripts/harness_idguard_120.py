#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/harness_idguard_120.py — identity_guard v1.2.0 harness (v1.0.0)
==============================================================================
Certifies the TOTAL-WIPE detector added in core/analysis/identity_guard.py
v1.2.0, and — the point of the exercise — proves that NOTHING ELSE MOVED.

Executes the REAL module. No stand-in guard, no re-implemented predicate.
Portable: paths resolve from this file; --old/--data optional; --require-old
makes the differential mandatory; runs from a bare clone with no arguments.

WHAT IT PROVES
  S1  contract: version, ENV default OFF, exactly one new ENV key
  S2  the production defect, reproduced: 10 rows -> 0, and v1.1.1 SILENT
  S3  the fix: v1.2.0 warns, names symbols and reasons, still returns []
  S4  escalation: TFB_IDENTITY_WIPE_RAISE=1 raises; unset never raises
  S5  NO REGRESSION: the >25% rule and every non-wipe path byte-identical
  S6  differential vs v1.1.1 over a matrix of batch shapes

USAGE
  python3 scripts/harness_idguard_120.py
  python3 scripts/harness_idguard_120.py --old /tmp/head --require-old
"""
from __future__ import annotations

import argparse
import copy
import importlib.util
import io
import contextlib
import os
import sys
from typing import Any, Dict, List, Optional

HARNESS_VERSION = "1.0.1"
EXPECT_NEW = "1.2.0"
EXPECT_OLD = "1.1.1"
ENV_KEY = "TFB_IDENTITY_WIPE_RAISE"

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


def load(alias: str, repo: str):
    """Import the real module with its real package deps on sys.path.

    v1.0.1 isolation: the guard imports core.analysis.symbol_dedup, and
    sys.modules caches the FIRST tree's copy — so loading old after new
    would silently hand the old guard the NEW dedup (the harness_v5130_1
    P1-A contamination class). Each load now snapshots and strips the
    core.* cache and pins its own tree to the front of sys.path; the
    loaded module keeps references to ITS deps, so restoring the cache
    afterwards is safe."""
    snap = {k: sys.modules[k] for k in list(sys.modules)
            if k == "core" or k.startswith("core.")}
    for k in snap:
        del sys.modules[k]
    sys.path.insert(0, repo)
    try:
        path = os.path.join(repo, "core", "analysis", "identity_guard.py")
        spec = importlib.util.spec_from_file_location(alias, path)
        if spec is None or spec.loader is None:
            raise ImportError(path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[alias] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        sys.path.remove(repo)
        for k in [k for k in list(sys.modules)
                  if k == "core" or k.startswith("core.")]:
            del sys.modules[k]
        sys.modules.update(snap)


def shell(sym: str) -> Dict[str, Any]:
    """A blank shell: symbol present, no name, no price — the exact shape
    the 2026-08-21 production batch carried (dead symbol -> 404 -> shell)."""
    return {"symbol": sym, "name": "", "current_price": "",
            "currency": "", "exchange": ""}


def healthy(sym: str, px: float = 10.0) -> Dict[str, Any]:
    return {"symbol": sym, "name": sym + " Corp", "current_price": px,
            "currency": "USD", "exchange": "NYSE"}


def run(mod, rows, sheet="Global_Markets"):
    """Call the real entry point, capturing stdout (the ::warning:: line)."""
    buf = io.StringIO()
    err: Optional[BaseException] = None
    plan = None
    with contextlib.redirect_stdout(buf):
        try:
            plan = mod.guard_sheet_rows(copy.deepcopy(rows), sheet=sheet)
        except BaseException as e:      # noqa: BLE001 - we assert on it
            err = e
    return plan, buf.getvalue(), err


def clear_env():
    os.environ.pop(ENV_KEY, None)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", default=os.getenv("TFB_HARNESS_OLD_TREE", ""))
    ap.add_argument("--require-old", action="store_true")
    args = ap.parse_args()

    here = os.path.dirname(os.path.abspath(__file__))
    repo = os.path.dirname(here) if os.path.basename(here) == "scripts" else here

    print("=" * 78)
    print("HARNESS harness_idguard_120 v%s — total-wipe detector"
          % HARNESS_VERSION)
    print("=" * 78)
    print("repo : " + repo)
    try:
        new = load("idg_new", repo)
    except Exception as e:
        print("FATAL: cannot load new module: %s" % e)
        return 2
    print("new  : v%s" % new.IDENTITY_GUARD_VERSION)

    old = None
    if args.old:
        try:
            old = load("idg_old", args.old)
            print("old  : v%s (%s)" % (old.IDENTITY_GUARD_VERSION, args.old))
        except Exception as e:
            print("old  : NOT LOADABLE (%s)" % e)
            if args.require_old:
                ck("S0.1 CERT: old baseline required", False, str(e))
    elif args.require_old:
        ck("S0.1 CERT: old baseline required", False, "--old not supplied")
    else:
        print("old  : not supplied — differential suite SKIPPED")

    clear_env()

    # ---------------- S1 contract ----------------
    section("S1  CONTRACT")
    ck("S1.1 version is " + EXPECT_NEW,
       new.IDENTITY_GUARD_VERSION == EXPECT_NEW, new.IDENTITY_GUARD_VERSION)
    if old is not None:
        ck("S1.2 old baseline is " + EXPECT_OLD,
           old.IDENTITY_GUARD_VERSION == EXPECT_OLD,
           old.IDENTITY_GUARD_VERSION)
    ck("S1.3 escalation ENV DEFAULT OFF",
       new._wipe_raise_enabled() is False)
    src = open(os.path.join(repo, "core", "analysis",
                            "identity_guard.py"), encoding="utf-8").read()
    import re as _re
    keys = set(_re.findall(r"TFB_[A-Z0-9_]+", src))
    if old is not None:
        osrc = open(os.path.join(args.old, "core", "analysis",
                                 "identity_guard.py"), encoding="utf-8").read()
        okeys = set(_re.findall(r"TFB_[A-Z0-9_]+", osrc))
        ck("S1.4 ENV key-set delta is EXACTLY {%s}" % ENV_KEY,
           keys - okeys == {ENV_KEY}, str(sorted(keys - okeys)))

    # ---------------- S2 the production defect ----------------
    section("S2  THE PRODUCTION DEFECT, REPRODUCED (10 shells -> 0 rows)")
    dead = ["ERJ.US", "P10.US", "SCVL.US", "SEMR.US", "ALFAA.MX",
            "AUO.US", "GES.US", "6641.T", "APLS.US", "NVEI.TO"]
    batch = [shell(s) for s in dead]
    plan, out, err = run(new, batch)
    ck("S2.1 batch of 10 shells yields 0 rows (the observed behaviour)",
       err is None and plan is not None and len(plan.rows) == 0,
       "in=%d out=%d" % (len(batch), len(plan.rows) if plan else -1))
    ck("S2.2 summary reproduces the production line shape",
       plan is not None and "10->0 rows" in plan.summary(),
       plan.summary() if plan else "")
    if old is not None:
        oplan, oout, oerr = run(old, batch)
        ck("S2.3 v1.1.1 was SILENT on the same input (the blind spot)",
           oerr is None and len(oplan.rows) == 0
           and "TOTAL WIPE" not in oout and oout.strip() == "",
           "stdout=%r" % oout[:60])

    # ---------------- S3 the fix ----------------
    section("S3  THE FIX — loud, self-diagnosing, non-breaking")
    ck("S3.1 v1.2.0 emits a ::warning:: on total wipe",
       "::warning::" in out and "TOTAL WIPE" in out, out.strip()[:100])
    ck("S3.2 report names the reason class",
       "pre_existing_blank_shell" in out)
    ck("S3.3 report names the symbols",
       all(s in out for s in dead[:5]), out.strip()[:120])
    ck("S3.4 report names the sheet",
       "Global_Markets" in out)
    ck("S3.5 return value UNCHANGED — still returns the empty plan",
       err is None and plan.rows == [])

    # ---------------- S4 escalation ----------------
    section("S4  ESCALATION (opt-in, default OFF)")
    os.environ[ENV_KEY] = "1"
    _, out4, err4 = run(new, batch)
    ck("S4.1 armed -> RuntimeError naming the wipe",
       isinstance(err4, RuntimeError) and "TOTAL WIPE" in str(err4),
       type(err4).__name__)
    clear_env()
    _, _, err4b = run(new, batch)
    ck("S4.2 unset -> never raises", err4b is None)

    # ---------------- S5 no regression ----------------
    section("S5  NO REGRESSION — every non-wipe path untouched")
    clean = [healthy("SYM%02d" % i, 10 + i) for i in range(30)]
    p5, o5, e5 = run(new, clean)
    ck("S5.1 healthy 30-row batch: all rows survive, no warning",
       e5 is None and len(p5.rows) == 30 and "TOTAL WIPE" not in o5,
       "out=%d" % len(p5.rows))
    mixed = [healthy("OK%02d" % i) for i in range(25)] + \
            [shell("DEAD%02d" % i) for i in range(3)]
    p6, o6, e6 = run(new, mixed)
    ck("S5.2 partial drop under 25%: no wipe warning, no raise",
       e6 is None and len(p6.rows) == 25 and "TOTAL WIPE" not in o6,
       "in=28 out=%d" % len(p6.rows))
    mass = [shell("D%02d" % i) for i in range(25)] + \
           [healthy("OK%02d" % i) for i in range(5)]
    p7, o7, e7 = run(new, mass)
    ck("S5.3 >25% rule still RAISES for input>=20 (byte-identical rule)",
       isinstance(e7, RuntimeError) and "refused to write" in str(e7),
       type(e7).__name__)
    p8, o8, e8 = run(new, [])
    ck("S5.4 empty input is NOT a wipe (0->0 must stay silent)",
       e8 is None and p8.rows == [] and "TOTAL WIPE" not in o8)
    p9, o9, e9 = run(new, [healthy("SOLO")])
    ck("S5.5 single healthy row survives silently",
       e9 is None and len(p9.rows) == 1 and "TOTAL WIPE" not in o9)
    p10, o10, e10 = run(new, [shell("DEADSOLO")])
    ck("S5.6 single shell IS a wipe — detector fires below the old floor",
       e10 is None and p10.rows == [] and "TOTAL WIPE" in o10)
    all25 = [shell("W%02d" % i) for i in range(25)]
    p11, o11, e11 = run(new, all25)
    ck("S5.7 >=20 TOTAL wipe still RAISES (E1 regression closed) — with "
       "the report line first",
       isinstance(e11, RuntimeError) and "TOTAL WIPE" in o11,
       type(e11).__name__)
    b20 = [shell("E%02d" % i) for i in range(20)]
    _, _, e12 = run(new, b20)
    ck("S5.8 boundary input=20 raises like v1.1.1",
       isinstance(e12, RuntimeError), type(e12).__name__)

    # ---------------- S6 differential ----------------
    if old is not None:
        section("S6  DIFFERENTIAL v%s -> v%s"
                % (old.IDENTITY_GUARD_VERSION, new.IDENTITY_GUARD_VERSION))
        shapes = {
            "healthy_30": clean,
            "mixed_28": mixed,
            "empty": [],
            "solo_healthy": [healthy("SOLO")],
            "mass_destruction_30": mass,
            "all_shell_25": [shell("W%02d" % i) for i in range(25)],
            "all_shell_20_boundary": [shell("E%02d" % i) for i in range(20)],
        }
        for label, rows in shapes.items():
            pa, _, ea = run(old, rows)
            pb, _, eb = run(new, rows)
            same_err = type(ea) is type(eb)
            same_rows = (pa.rows if pa else None) == (pb.rows if pb else None)
            ck("S6.%s identical rows AND identical exception class" % label,
               same_err and same_rows,
               "%s/%s" % (type(ea).__name__, type(eb).__name__))
        pa, oa, ea = run(old, batch)
        pb, ob, eb = run(new, batch)
        ck("S6.total_wipe rows identical; ONLY the log line differs",
           pa.rows == pb.rows and ea is None and eb is None
           and "TOTAL WIPE" not in oa and "TOTAL WIPE" in ob)

    clear_env()
    print("\n" + "=" * 78)
    print("HARNESS RESULT: %d/%d PASS%s"
          % (_PASSES, _PASSES + len(_FAILS),
             "" if not _FAILS else "  —  FAILURES: " + ", ".join(_FAILS)))
    print("=" * 78)
    return 0 if not _FAILS else 1


if __name__ == "__main__":
    sys.exit(main())
