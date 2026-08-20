#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/harness_sai_140.py — W1A-7b/W1A-8 harness (v1.1.0, 2026-08-20)
==============================================================================
Proves core/surface_action_invariants.py v1.4.0 against v1.3.0 by executing
BOTH REAL MODULES over REAL exported page rows. No stand-in objects, no fake
row classes, no mocked apply() — the standing rule after the TaskResult slots
defect was missed by a FakeRes pattern.

PORTABILITY CONTRACT (why this file exists in this shape)
---------------------------------------------------------
harness_v5130_1.py and harness_ob_1_13_0.py are both UNRUNNABLE from a clean
checkout — they carry absolute session paths ('/home/claude/build/...') baked
in, and die with FileNotFoundError for anyone who did not author them. This
harness resolves everything relative to its own location, accepts overrides by
argv or ENV, and DEGRADES to fixture-only mode when the exports are absent
rather than crashing. It must run from a bare `git clone` on any machine.

  NEW tree  : resolved from this file  -> <repo>/core/surface_action_invariants.py
  OLD tree  : --old <dir> | TFB_HARNESS_OLD_TREE | skipped (fixture mode only)
  Exports   : --data <dir> | TFB_HARNESS_DATA | ./ | /mnt/user-data/uploads

USAGE
  python3 scripts/harness_sai_140.py
  python3 scripts/harness_sai_140.py --old /tmp/head_v130 --data /tmp/tsv
  TFB_HARNESS_DATA=/tmp/tsv python3 scripts/harness_sai_140.py

EXIT 0 iff every assertion passes. Any FAIL is fatal.
"""
from __future__ import annotations

import argparse
import copy
import csv
import importlib.util
import io
import os
import contextlib
import sys
from typing import Any, Dict, List, Optional

HARNESS_VERSION = "1.1.0"
EXPECT_NEW_VERSION = "1.4.1"
# The row-sanity marker names the VOCABULARY, not the module (same rule as
# the 52W tags pinned at :v1.2.0). v1.4.1 changed W1A-8 semantics, not the
# row-sanity class set, so the vocab pin stays at the v1.4.0 value.
EXPECT_RS_VOCAB = "1.4.0"
EXPECT_OLD_VERSION = "1.3.0"

PAGES = ("Global_Markets", "Market_Leaders", "Commodities_FX", "Mutual_Funds")
EXPORT_PREFIX = "_Market_Share_Deepseek-V3_-_"

GATE_KEYS = ("TFB_T10_BLOCKED_INVARIANT", "TFB_T10_FETCHFAIL_BLOCKED",
             "TFB_SURFACE_BLOCKED_INVARIANT", "TFB_SURFACE_FETCHFAIL_BLOCKED",
             "TFB_WARN_INVEST_INVARIANT", "TFB_SURFACE_WARN_INVEST",
             "TFB_ROW_SANITY_QUARANTINE", "TFB_SURFACE_ROW_SANITY")

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


def section(title: str) -> None:
    print("\n" + "-" * 78 + "\n" + title + "\n" + "-" * 78)


# ---------------------------------------------------------------------------
# module loading — real files, loaded under distinct names so both coexist
# ---------------------------------------------------------------------------
def load_module(alias: str, path: str):
    spec = importlib.util.spec_from_file_location(alias, path)
    if spec is None or spec.loader is None:
        raise ImportError("cannot build spec for " + path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    spec.loader.exec_module(mod)
    return mod


def set_gates(**kw: bool) -> None:
    for k in GATE_KEYS:
        os.environ.pop(k, None)
    for k, v in kw.items():
        if v:
            os.environ[k] = "1"


# ---------------------------------------------------------------------------
# export loading
# ---------------------------------------------------------------------------
def _num(v: Any) -> Optional[float]:
    s = str(v or "").strip().replace(",", "").replace("\u25b2", "")
    s = s.replace("\u25bc", "").replace("%", "").strip()
    if s in ("", "\u2014", "-", "N/A", "n/a", "None", "#N/A"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


REQUIRED_HEADERS = ("Symbol", "Open", "Day High", "Day Low",
                    "Final Action", "Investability Status", "Warnings",
                    "Block Reason")


def find_export(data_dir: str, page: str) -> Optional[str]:
    """v1.1.0 (audit F-06): exact name first, then a normalized glob so the
    real-world variants (spaces for underscores, download-counter suffixes)
    are found instead of silently skipped."""
    import glob as _g
    exact = os.path.join(data_dir, EXPORT_PREFIX + page + ".tsv")
    if os.path.isfile(exact):
        return exact
    tokens = page.split("_")
    cands = []
    for pat in ("*" + "*".join(tokens) + "*.tsv",
                "*" + " ".join(tokens) + "*.tsv"):
        cands += _g.glob(os.path.join(data_dir, pat))
    cands = sorted(set(c for c in cands if os.path.isfile(c)),
                   key=os.path.getmtime, reverse=True)
    if cands:
        print("  ....  %s matched by glob: %s"
              % (page, os.path.basename(cands[0])))
        return cands[0]
    return None


def load_page(data_dir: str, page: str) -> Optional[List[Dict[str, Any]]]:
    path = find_export(data_dir, page)
    if path is None:
        return None
    csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))
    with open(path, newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.reader(fh, delimiter="\t"))
    if not rows:
        return None
    hdr = rows[0]
    idx = {h: i for i, h in enumerate(hdr)}
    missing = [h for h in REQUIRED_HEADERS if h not in idx]
    if missing:
        # v1.1.0 (audit F-03): header drift must be LOUD — silently turning
        # every numeric into None makes oracle == module == 0 and would
        # certify garbage.
        raise RuntimeError("%s missing required headers: %s"
                           % (os.path.basename(path), missing))

    def g(r, name):
        i = idx.get(name)
        return r[i] if i is not None and i < len(r) else ""

    out = []
    for r in rows[1:]:
        if not any(c.strip() for c in r):
            continue
        out.append({
            "symbol": g(r, "Symbol"),
            "recommendation": g(r, "Recommendation"),
            "recommendation_detailed": g(r, "Recommendation Detail"),
            "final_action": g(r, "Final Action"),
            "investability_status": g(r, "Investability Status"),
            "block_reason": g(r, "Block Reason"),
            "warnings": g(r, "Warnings"),
            "current_price": _num(g(r, "Current Price")),
            "open": _num(g(r, "Open")),
            "day_high": _num(g(r, "Day High")),
            "day_low": _num(g(r, "Day Low")),
            "week_52_high": _num(g(r, "52W High")),
            "week_52_low": _num(g(r, "52W Low")),
            "exchange": g(r, "Exchange"),
            "currency": g(r, "Currency"),
            "country": g(r, "Country"),
        })
    return out


def independent_open_violations(rows, tol: float) -> int:
    """Oracle computed WITHOUT the module — the module must agree with it."""
    n = 0
    for r in rows:
        o, hi, lo = r.get("open"), r.get("day_high"), r.get("day_low")
        if None in (o, hi, lo) or hi < lo or o <= 0 or hi <= 0 or lo <= 0:
            continue
        if o > hi * (1.0 + tol) or o < lo * (1.0 - tol):
            n += 1
    return n


def invest_count(rows) -> int:
    return sum(1 for r in rows
               if str(r.get("final_action") or "").upper() == "INVEST")


# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", default=os.getenv("TFB_HARNESS_OLD_TREE", ""))
    ap.add_argument("--data", default=os.getenv("TFB_HARNESS_DATA", ""))
    ap.add_argument("--engine", default="",
                    help="path to data_engine_v2.py for the pin cross-check "
                         "(default: <repo>/core/data_engine_v2.py)")
    # v1.1.0 certification mode (audit F-03): in smoke mode missing inputs
    # are skipped politely; with these flags their absence is a FAILURE, so
    # CI can never report certified without the evidence.
    ap.add_argument("--require-old", action="store_true")
    ap.add_argument("--require-live", action="store_true")
    args = ap.parse_args()

    if args.require_old or args.require_live:
        print("MODE      : CERTIFICATION (--require-old=%s "
              "--require-live=%s)" % (args.require_old, args.require_live))
    here = os.path.dirname(os.path.abspath(__file__))
    repo = os.path.dirname(here) if os.path.basename(here) == "scripts" else here
    new_path = os.path.join(repo, "core", "surface_action_invariants.py")

    print("=" * 78)
    print("HARNESS harness_sai_140 v%s — W1A-7b open_outside_day_range"
          % HARNESS_VERSION)
    print("=" * 78)
    print("repo root : " + repo)
    print("new module: " + new_path)

    if not os.path.isfile(new_path):
        print("FATAL: new module not found at " + new_path)
        return 2
    new = load_module("sai_new", new_path)
    print("new version: " + new.__version__)

    old = None
    if args.old:
        op = os.path.join(args.old, "core", "surface_action_invariants.py")
        if os.path.isfile(op):
            old = load_module("sai_old", op)
            print("old module: %s (v%s)" % (op, old.__version__))
            ck("S0.1 old baseline is v" + EXPECT_OLD_VERSION,
               old.__version__ == EXPECT_OLD_VERSION, old.__version__)
        else:
            print("old module: NOT FOUND at %s" % op)
            if args.require_old:
                ck("S0.1 CERT: old baseline required and present", False, op)
    else:
        print("old module: not supplied (--old / TFB_HARNESS_OLD_TREE)")
        if args.require_old:
            ck("S0.1 CERT: old baseline required and present", False,
               "--require-old set but --old missing")

    data_dir = ""
    if str(args.data).upper() == "NONE":          # v1.0.3: explicit off switch
        print("exports   : DISABLED by --data NONE — fixture-only run")
    else:
     for cand in (args.data, os.getcwd(), "/mnt/user-data/uploads"):
        if cand and os.path.isfile(os.path.join(
                cand, EXPORT_PREFIX + "Global_Markets.tsv")):
            data_dir = cand
            break
    if str(args.data).upper() != "NONE":
        print("exports   : " + (data_dir
                                or "NOT FOUND — live-data suite skipped"))
    if args.require_live and not data_dir:
        ck("S0.2 CERT: live exports required and present", False,
           "--require-live set but no export directory resolved")

    # v1.1.0 (audit gap): the module can match while the ENGINE still pins
    # another version — cross-check the pin when the engine source is
    # reachable.
    eng_path = args.engine or os.path.join(repo, "core", "data_engine_v2.py")
    if os.path.isfile(eng_path):
        import re as _re
        _pin = _re.search(r'^_SAI_REQUIRED_VERSION\s*=\s*"([\d.]+)"',
                          open(eng_path, encoding="utf-8").read(), _re.M)
        ck("S0.3 engine pin == module version",
           bool(_pin) and _pin.group(1) == EXPECT_NEW_VERSION,
           "engine pins %s at %s" % (_pin.group(1) if _pin else "?",
                                     eng_path))
    elif args.require_old or args.require_live:
        ck("S0.3 CERT: engine source required for pin cross-check", False,
           eng_path)
    else:
        print("engine pin: %s absent — cross-check skipped" % eng_path)

    # -- S1 contract ------------------------------------------------------
    section("S1  MODULE CONTRACT")
    ck("S1.1 version is " + EXPECT_NEW_VERSION,
       new.__version__ == EXPECT_NEW_VERSION, new.__version__)
    ck("S1.2 row-sanity vocab marker == vocabulary pin (not module ver)",
       new._RS_MARK == ":v" + EXPECT_RS_VOCAB, new._RS_MARK)
    ck("S1.3 apply() returns the SIX-tuple",
       len(new.apply_surface_action_invariants([], "GM")) == 6)
    # v1.1.0: S1.4 was tautological (it inspected the harness constant).
    # Now it scans the MODULE SOURCE for every ENV it actually reads and
    # requires the set to be within the approved gate manifest.
    import re as _re2
    _msrc = open(new_path, encoding="utf-8").read()
    _read = set(_re2.findall(r'_flag\(\s*"([A-Z0-9_]+)"', _msrc))
    _read |= set(_re2.findall(r'os\.getenv\(\s*"(TFB_[A-Z0-9_]+)"', _msrc))
    ck("S1.4 module reads ONLY approved gate ENVs",
       _read <= set(GATE_KEYS),
       "unapproved: %s" % sorted(_read - set(GATE_KEYS)) if
       (_read - set(GATE_KEYS)) else "%d gates" % len(_read))
    ck("S1.5 tolerance reuses the existing _RANGE_TOL",
       abs(new._RANGE_TOL - 0.001) < 1e-12, str(new._RANGE_TOL))

    # -- S2 embedded self-test -------------------------------------------
    section("S2  EMBEDDED SELF-TEST (real module)")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = new._selftest()
    o = buf.getvalue()
    ck("S2.1 self-test rc == 0", rc == 0)
    ck("S2.2 zero FAIL lines", o.count("  FAIL  ") == 0)
    ck("S2.3 fixture count grew", o.count("  PASS  ") >= 63,
       "%d fixtures" % o.count("  PASS  "))
    if old is not None:
        b2 = io.StringIO()
        with contextlib.redirect_stdout(b2):
            old._selftest()
        oldnames = {l[8:] for l in b2.getvalue().splitlines()
                    if l.startswith("  PASS  ")}
        newnames = {l[8:] for l in o.splitlines()
                    if l.startswith("  PASS  ")}
        lost = oldnames - newnames
        ck("S2.4 ZERO v1.3.0 fixtures lost", not lost,
           ("lost: %s" % sorted(lost)) if lost else "0 lost, %d added"
           % len(newnames - oldnames))

    # -- S3 gate discipline ----------------------------------------------
    section("S3  GATE DISCIPLINE")
    set_gates()
    rows = [{"symbol": "X", "final_action": "INVEST",
             "investability_status": "INVESTABLE", "open": 4.64,
             "day_high": 7.12, "day_low": 7.05, "current_price": 7.09,
             "block_reason": "", "warnings": []}]
    out = new.apply_surface_action_invariants(rows, "GM")
    ck("S3.1 all gates OFF -> SAME object, strict no-op",
       out[0] is rows and out[1:] == (0, 0, 0, 0, 0)
       and rows[0]["final_action"] == "INVEST")
    set_gates(TFB_ROW_SANITY_QUARANTINE=True)
    snap = copy.deepcopy(rows)
    res = new.apply_surface_action_invariants(rows, "GM")
    ck("S3.2 ROW_SANITY without BLOCKED_INVARIANT -> SELF-DISABLED",
       rows == snap and res[4] == 0
       and bool(new.env_combo_violations()))
    set_gates(TFB_ROW_SANITY_QUARANTINE=True, TFB_T10_BLOCKED_INVARIANT=True)
    ck("S3.3 combo satisfied -> no violations",
       new.env_combo_violations() == [])

    # -- preload pages once (v1.1.0) --------------------------------------
    page_rows: Dict[str, Any] = {}
    if data_dir:
        for _pg in PAGES:
            try:
                page_rows[_pg] = load_page(data_dir, _pg)
            except RuntimeError as _he:
                page_rows[_pg] = None
                if args.require_live:
                    ck("S0.4 CERT: %s headers valid" % _pg, False, str(_he))
                else:
                    print("  ....  %s skipped: %s" % (_pg, _he))
        if args.require_live and all(v is None for v in page_rows.values()):
            ck("S0.5 CERT: at least one live page loaded", False, data_dir)

    # -- S4 live data ------------------------------------------------------
    if data_dir:
        section("S4  LIVE EXPORT — real module over real rows")
        set_gates(TFB_ROW_SANITY_QUARANTINE=True,
                  TFB_T10_BLOCKED_INVARIANT=True)
        # NOTE (harness v1.0.1): the class name is carried in the WARNINGS
        # marker, NOT block_reason — the v1.2.0 contract preserves a
        # pre-populated reason and never overwrites it. And n4 is
        # DETECTION-gated (rows matching any hard class), not
        # mutation-gated, in BOTH v1.3.0 and v1.4.0 — so idempotency is
        # asserted on row equality, never on n4.
        tot_q = tot_oracle = 0
        inv_before = inv_after = 0
        tot_oracle_invest = 0     # v1.0.2: INVEST rows inside the oracle set
        tot_open_demoted = tot_other_demoted = 0   # v1.0.3 attribution
        ml_q = None
        ml_seen = False
        for page in PAGES:
            base = page_rows.get(page)
            if base is None:
                print("  ....  %s export absent — skipped" % page)
                continue
            oracle = independent_open_violations(base, new._RANGE_TOL)
            oracle_invest = sum(
                1 for r in base
                if str(r.get("final_action") or "").upper() == "INVEST"
                and independent_open_violations([r], new._RANGE_TOL) == 1)
            tot_oracle_invest += oracle_invest
            work = copy.deepcopy(base)
            page_before = invest_count(work)
            inv_before += page_before
            _, n1, n2, n3, n4, err = new.apply_surface_action_invariants(
                work, page)
            page_after = invest_count(work)
            inv_after += page_after
            # v1.0.3 attribution: of the INVEST rows this page lost, how
            # many carry the open-class marker vs another hard class?
            def _w(r):
                w = r.get("warnings")
                return "; ".join(str(x) for x in w) if isinstance(
                    w, (list, tuple)) else str(w or "")
            open_demoted = other_demoted = unmarked = 0
            for i, r in enumerate(work):
                was = str(base[i].get("final_action") or "").upper()
                now = str(r.get("final_action") or "").upper()
                if was != "INVEST" or now == "INVEST":
                    continue
                wtxt = _w(r)
                if "open_outside_day_range" in wtxt:
                    open_demoted += 1
                elif ("row_sanity_quarantined:" in wtxt
                      or "blocked_invariant_applied" in wtxt):
                    other_demoted += 1
                else:
                    unmarked += 1
            tot_open_demoted += open_demoted
            tot_other_demoted += other_demoted
            ck("S4.%s open-attributed demotions == oracle-INVEST" % page,
               open_demoted == oracle_invest,
               "open=%d oracle=%d other=%d" % (open_demoted, oracle_invest,
                                               other_demoted))
            ck("S4.%s every demoted INVEST row carries an invariant marker"
               % page, unmarked == 0, "unmarked=%d" % unmarked)
            opened = sum(1 for r in work if "open_outside_day_range"
                         in str(r.get("warnings") or ""))
            tot_q += opened
            tot_oracle += oracle
            if page == "Market_Leaders":
                ml_q = opened
                ml_seen = True
            ck("S4.%s open-class count == independent oracle" % page,
               opened == oracle and err == 0,
               "module=%d oracle=%d errors=%d" % (opened, oracle, err))
            ck("S4.%s n4 is a superset of the open class" % page,
               n4 >= opened, "n4=%d open=%d" % (n4, opened))
            snap = copy.deepcopy(work)
            _, m1, m2, m3, m4, merr = new.apply_surface_action_invariants(
                work, page)
            ck("S4.%s idempotent — rows byte-identical on pass 2" % page,
               work == snap and (m1 + m2 + m3 + merr) == 0,
               "mutation counters %d/%d/%d err=%d (n4=%d is detection)"
               % (m1, m2, m3, merr, m4))
            dupes = sum(1 for r in work if str(r.get("warnings") or "")
                        .count("row_sanity_quarantined:") > 1)
            ck("S4.%s no duplicated quarantine marker" % page, dupes == 0)
        ck("S4.TOTAL open-range count matches oracle",
           tot_q == tot_oracle, "%d rows (oracle %d)" % (tot_q, tot_oracle))
        if ml_seen:
            ck("S4.ML negative control — Open unpopulated, class silent",
               ml_q == 0, "Market_Leaders open-class=%s" % ml_q)
        else:
            print("  ....  S4.ML control skipped — Market_Leaders export "
                  "absent (partial data set)")
        # v1.0.2: the DATA-INDEPENDENT invariant. v1.0.1 asserted a
        # non-zero reduction, which hardcoded the 2026-08-20 contamination
        # into the harness — it FAILED on a synthetic clean export (the
        # exact portability defect this file's docstring condemns in
        # harness_v5130_1 / harness_ob_1_13_0). The true invariant: the
        # INVEST reduction equals EXACTLY the count of INVEST rows inside
        # the independent oracle set. Today: 13 == 13. Clean data: 0 == 0.
        # v1.0.3: v1.0.2 asserted reduction == oracle-INVEST and FAILED on
        # the 2026-08-20 export by exactly 1 — Copper Futures, an INVEST
        # row demoted by the PRE-EXISTING classes (symbol_whitespace,
        # day_high_lt_day_low, currency), not by the class under test.
        # The correct invariant is attribution-decomposed: the OPEN-class
        # leg equals the independent oracle EXACTLY, other-class demotions
        # are >= 0 and every demoted row is marker-attributed (asserted
        # per page above). Reduction == open + other is then arithmetic.
        ck("S4.TOTAL open-attributed == oracle-INVEST exactly",
           tot_open_demoted == tot_oracle_invest,
           "open=%d oracle=%d other=%d reduction=%d"
           % (tot_open_demoted, tot_oracle_invest, tot_other_demoted,
              inv_before - inv_after))
        ck("S4.TOTAL reduction decomposes exactly",
           (inv_before - inv_after)
           == tot_open_demoted + tot_other_demoted,
           "INVEST %d -> %d" % (inv_before, inv_after))

        if old is not None:
            section("S5  DIFFERENTIAL v%s -> v%s  (v1.1.0: RESTORED — the "
                    "v1.0.1 span-replace silently deleted this suite; "
                    "every 'four-mode' run since certified without it)"
                    % (old.__version__, new.__version__))
            set_gates(TFB_ROW_SANITY_QUARANTINE=True,
                      TFB_T10_BLOCKED_INVARIANT=True)
            for page in PAGES:
                base = page_rows.get(page)
                if base is None:
                    continue
                a, b = copy.deepcopy(base), copy.deepcopy(base)
                old.apply_surface_action_invariants(a, page)
                new.apply_surface_action_invariants(b, page)
                changed = [i for i in range(len(a))
                           if a[i]["final_action"] != b[i]["final_action"]
                           or a[i]["investability_status"]
                           != b[i]["investability_status"]]
                regress = [i for i in changed
                           if str(a[i]["final_action"]).upper()
                           in ("DO_NOT_INVEST", "WATCH")
                           and str(b[i]["final_action"]).upper() == "INVEST"]
                ck("S5.%s no row is PROMOTED by the upgrade (sanity gates)"
                   % page, not regress,
                   "%d changed, %d promoted" % (len(changed), len(regress)))

        section("S6  W1A-8 CORRECTED SEMANTICS (v1.4.1 F-01) — live rows")
        # Independent oracles straight off the warnings text:
        #   PURE = INVEST + xprovider :0.0% + none of the four identity
        #          markers + no fetch_failed  -> must SURVIVE in new
        #   IDW  = INVEST + any identity marker -> must DEMOTE in new
        # Under the WARN gate the upgrade DOES promote the PURE set
        # relative to v1.3.0 — that promotion IS the F-01 fix, so S6
        # asserts it explicitly instead of S5's blanket no-promote.
        import re as _re6
        _zero = _re6.compile(r"xprovider_verified:[^;\s]*:0\.0+%")
        _subs = ("quote_current_price_missing", "quote_exchange_missing",
                 "quote_currency_missing", "name_unresolved")

        def _wt(r):
            w = r.get("warnings")
            return ("; ".join(str(x) for x in w) if isinstance(
                w, (list, tuple)) else str(w or "")).lower()
        set_gates(TFB_SURFACE_WARN_INVEST=True)
        pure_o = idw_o = pure_kept = idw_dem = 0
        old_pure_dem = 0
        for page in PAGES:
            base = page_rows.get(page)
            if base is None:
                continue
            for r in base:
                if str(r.get("final_action") or "").upper() != "INVEST":
                    continue
                w = _wt(r)
                if any(t in w for t in _subs):
                    idw_o += 1
                elif _zero.search(w) and "fetch_failed" not in w:
                    pure_o += 1
            work = copy.deepcopy(base)
            new.apply_surface_action_invariants(work, page)
            for i, r in enumerate(work):
                if str(base[i].get("final_action") or "").upper() != "INVEST":
                    continue
                w = _wt(base[i])
                now = str(r.get("final_action") or "").upper()
                if any(t in w for t in _subs):
                    idw_dem += (now != "INVEST")
                elif _zero.search(w) and "fetch_failed" not in w:
                    pure_kept += (now == "INVEST")
            if old is not None:
                wold = copy.deepcopy(base)
                old.apply_surface_action_invariants(wold, page)
                for i, r in enumerate(wold):
                    if (str(base[i].get("final_action") or "").upper()
                            == "INVEST"):
                        w = _wt(base[i])
                        if (_zero.search(w) and "fetch_failed" not in w
                                and not any(t in w for t in _subs)):
                            old_pure_dem += (str(r.get("final_action")
                                                 or "").upper() != "INVEST")
        ck("S6.1 every PURE exact-agreement INVEST row SURVIVES in new",
           pure_kept == pure_o, "kept %d / oracle %d" % (pure_kept, pure_o))
        ck("S6.2 every identity-marked INVEST row still DEMOTES in new",
           idw_dem == idw_o, "demoted %d / oracle %d" % (idw_dem, idw_o))
        if old is not None:
            ck("S6.3 old v1.3.0 falsely demoted the PURE set (the fixed "
               "defect, documented)", old_pure_dem == pure_o,
               "old demoted %d / oracle %d" % (old_pure_dem, pure_o))

    else:
        section("S4/S5  LIVE EXPORT SUITES SKIPPED (no exports found)")

    for k in GATE_KEYS:
        os.environ.pop(k, None)
    print("\n" + "=" * 78)
    print("HARNESS RESULT: %d/%d PASS%s"
          % (_PASSES, _PASSES + len(_FAILS),
             "" if not _FAILS else "  —  FAILURES: " + ", ".join(_FAILS)))
    print("=" * 78)
    return 0 if not _FAILS else 1


if __name__ == "__main__":
    sys.exit(main())
