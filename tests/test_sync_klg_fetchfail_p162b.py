#!/usr/bin/env python3
"""tests/test_sync_klg_fetchfail_p162b.py
P-162b - run_dashboard_sync v6.63.0 KEEP-LAST-GOOD FOR FETCH-FAILED ROWS.
Dual-tree, REAL-module harness (the K-battery loader): drives the REAL
_keep_last_good_rows with a Sheets boundary double that serves a canned
"old grid" (read_values only), on hand fixtures AND on the real 2026-09-26
Global_Markets export (TFB_TEST_EXPORT_DIR); proves the caller line, the
stamp effect through the REAL TaskResult, the seam order and base parity
(TFB_TEST_SYNC_BASE = the v6.62.0 file).
Run: pytest -q tests/test_sync_klg_fetchfail_p162b.py
  or python3 tests/test_sync_klg_fetchfail_p162b.py   (x3, digest)"""
from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SYNC = os.environ.get("TFB_TEST_SYNC_DELIVERED") or os.path.join(ROOT, "scripts", "run_dashboard_sync.py")
BASE = os.environ.get("TFB_TEST_SYNC_BASE", "")
EXPORT = os.environ.get("TFB_TEST_EXPORT_DIR", "")
csv.field_size_limit(10 ** 9)
os.environ.setdefault("GITHUB_RUN_ID", "36231358321")


def _load(path, name):
    d = os.path.dirname(os.path.abspath(path))
    if d not in sys.path:
        sys.path.insert(0, d)
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


_M = {}


def m():
    if "d" not in _M:
        _M["d"] = _load(SYNC, "rds_p162b_delivered")
    return _M["d"]


def b():
    if "b" not in _M:
        _M["b"] = _load(BASE, "rds_p162b_base") if BASE and os.path.exists(BASE) else None
    return _M["b"]


def _mode(v):
    if v is None:
        os.environ.pop("TFB_SYNC_KLG_FETCHFAIL", None)
    else:
        os.environ["TFB_SYNC_KLG_FETCHFAIL"] = v


class _Grid(object):
    """Sheets boundary double: the ONE read the guard makes (read_values)."""
    def __init__(self, grid):
        self.grid, self.reads = grid, 0

    def read_values(self, *_a, **_k):
        self.reads += 1
        return [list(r) for r in self.grid]


HDR = ["Symbol", "Name", "Current Price", "EPS (TTM)", "P/E (TTM)", "Data Provider", "Warnings", "Last Updated (UTC)"]
OLD = [list(HDR),
       ["A.US", "Alpha Corp", 10.0, 1.0, 10.0, "eodhd", "yahoo_enrichment_applied", "2026-09-25T13:00:00+00:00"],   # good prior
       ["B.US", "Beta Corp", 20.0, 2.0, 10.0, "eodhd", "fetch_failed:HTTP 402", "2026-09-25T23:20:00+00:00"],       # poisoned prior
       ["C.US", "Gamma Corp", 30.0, 3.0, 10.0, "eodhd", "", "2026-09-25T13:00:00+00:00"],                            # clean, fresh row is clean too
       ["E.US", "Eps Corp", "", 1.0, 10.0, "eodhd", "", "2026-09-25T13:00:00+00:00"]]                               # prior without a price: not GOOD


def fresh():
    return [["A.US", "Alpha Corp", 9.9, 1.0, 9.9, "eodhd", "fetch_failed:HTTP 402; dq_capped:coherence:fetch_failed", "2026-09-26T00:40:00+00:00"],
            ["B.US", "Beta Corp", 19.8, 2.0, 9.9, "eodhd", "fetch_failed:HTTP 402", "2026-09-26T00:40:00+00:00"],
            ["C.US", "Gamma Corp", 30.3, 3.0, 10.1, "eodhd", "ok", "2026-09-26T00:41:00+00:00"],
            ["D.US", "Delta Corp", 5.0, 0.5, 10.0, "eodhd", "fetch_failed:HTTP 404 not_found", "2026-09-26T00:41:00+00:00"],   # no prior at all
            ["E.US", "Eps Corp", 4.0, 1.0, 4.0, "eodhd", "FETCH_FAILED:timeout", "2026-09-26T00:41:00+00:00"],                # prior not GOOD
            ["F.US", "", "", "", "", "fallback_error", "", "2026-09-26T00:41:00+00:00"]]                                       # classic priceless stub (form a/b)


def run(mod, mode, grid=OLD, rows=None, hdr=HDR):
    _mode(mode)
    g = _Grid(grid)
    out, sw = mod._keep_last_good_rows(g, "sid", "Global_Markets", list(hdr), rows if rows is not None else fresh())
    st = dict(mod._LAST_KLG_FF) if hasattr(mod, "_LAST_KLG_FF") else {}
    _mode(None)
    return out, sw, st, g.reads


# T1 - vocabulary / self-test / version ------------------------------------------------
def test_t1_vocabulary():
    mod = m()
    for v, exp in ((None, "off"), ("", "off"), ("1", "off"), ("observe", "observe"), ("ENFORCE ", "enforce")):
        _mode(v)
        assert mod._klg_fetchfail_mode() == exp, (v, exp)
    _mode(None)
    assert mod._klg_fetchfail_selftest() == "PASS"
    assert tuple(int(x) for x in mod.SCRIPT_VERSION.split(".")) >= (6, 63, 0)


# T2 - off: byte-identical to the v6.62.0 guard (fetch-failed rows untouched; classic stub still swapped? no prior -> kept)
def test_t2_off_unchanged():
    mod = m()
    out, sw, st, reads = run(mod, None)
    assert sw == [] and st.get("cand") == 0 and out[0][2] == 9.9 and out[1][2] == 19.8
    assert reads == 1                       # the classic stub F.US triggers the one read (no GOOD prior -> nothing swapped)
    out2, sw2, st2, reads2 = run(mod, None, rows=fresh()[:5])
    assert reads2 == 0 and sw2 == []         # zero-stub fast path untouched


# T3 - observe: candidates certified, nothing substituted, one read
def test_t3_observe_counts_only():
    mod = m()
    out, sw, st, reads = run(mod, "observe")
    assert sw == [] and reads == 1
    assert st["cand"] == 4 and st["good_prior"] == 1 and st["prior_ff"] == 1 and st["swapped"] == 0
    assert out[0][2] == 9.9 and out[0][6].startswith("fetch_failed")       # A.US still the fresh poisoned row


# T4 - enforce: only the certified prior rides back; poisoned / missing / priceless priors keep the fresh row
def test_t4_enforce_substitutes_certified_only():
    mod = m()
    out, sw, st, reads = run(mod, "enforce")
    assert sw == ["A.US"] and st["swapped"] == 1 and st["syms"] == ["A.US"] and reads == 1
    assert out[0][2] == 10.0 and out[0][6] == "yahoo_enrichment_applied" and out[0][7] == "2026-09-25T13:00:00+00:00"
    assert out[1][2] == 19.8 and out[1][6] == "fetch_failed:HTTP 402"     # B: prior itself poisoned -> fresh kept
    assert out[2][2] == 30.3                                               # C: clean fresh row untouched
    assert out[3][2] == 5.0                                                # D: no prior -> fresh kept
    assert out[4][2] == 4.0                                                # E: prior without price -> fresh kept
    assert out[5][5] == "fallback_error"                                   # F: classic stub, no prior -> unchanged
    # idempotent: a second pass over the substituted matrix changes nothing more
    out2, sw2, st2, _ = run(mod, "enforce", rows=[list(r) for r in out])
    assert sw2 == [] and st2["cand"] == 3 and out2 == out


# T5 - a symbol that is BOTH a classic stub and fetch-failed keeps the classic path; forced-refetch never rides back
def test_t5_mixed_forms_and_forced():
    mod = m()
    rows = fresh()
    rows.append(["A.US", "", "", "", "", "fallback_error", "fetch_failed:HTTP 502", "2026-09-26T00:42:00+00:00"])  # priceless AND fetch-failed
    out, sw, st, _ = run(mod, "observe", rows=rows)
    assert sw == [] and st["cand"] == 5                                    # observe: the extra A.US row is a candidate too, nothing swapped
    out, sw, st, _ = run(mod, "enforce", rows=rows)
    assert sw == ["A.US"] and out[0][2] == 10.0 and out[6][2] == 10.0       # both A.US rows carry the prior
    saved = os.environ.get("TFB_SYNC_FORCE_REFETCH_SYMBOLS")
    try:
        os.environ["TFB_SYNC_FORCE_REFETCH_SYMBOLS"] = "A.US"
        if hasattr(mod, "_force_refetch_symbols") and "A.US" in mod._force_refetch_symbols():
            out, sw, st, _ = run(mod, "enforce")
            assert sw == [] and out[0][2] == 9.9                            # forced symbol: the old row may NEVER ride back in
    finally:
        if saved is None:
            os.environ.pop("TFB_SYNC_FORCE_REFETCH_SYMBOLS", None)
        else:
            os.environ["TFB_SYNC_FORCE_REFETCH_SYMBOLS"] = saved


# T6 - the real 2026-09-26 Global_Markets page: 6,349 fetch-failed rows vs a clean prior grid
def test_t6_real_page():
    if not EXPORT:
        return
    mod = m()
    p = os.path.join(EXPORT, "Global_Markets.tsv")
    with open(p, encoding="utf-8", errors="replace", newline="") as fh:
        rows = list(csv.reader(fh, delimiter="\t"))
    hdr, body = rows[0], [r for r in rows[1:] if any(c.strip() for c in r)]
    wi, ui = hdr.index("Warnings"), hdr.index("Last Updated (UTC)")
    # a clean prior grid = the same page with the fetch_failed tags removed and the 09-25 evening stamp
    prior = [list(hdr)]
    for r in body:
        o = list(r)
        o[wi] = re.sub(r"fetch_failed:[^;]*;?\s*", "", o[wi]).replace("dq_capped:coherence:fetch_failed;", "").strip("; ")
        o[ui] = "2026-09-25T20:59:00+00:00"
        prior.append(o)
    n_ff = sum(1 for r in body if "fetch_failed" in r[wi].lower())
    out, sw, st, reads = run(mod, "observe", grid=prior, rows=[list(r) for r in body], hdr=hdr)
    assert st["cand"] == n_ff == 6349 and st["swapped"] == 0 and sw == [] and reads == 1
    out, sw, st, reads = run(mod, "enforce", grid=prior, rows=[list(r) for r in body], hdr=hdr)
    assert st["cand"] == 6349 and st["swapped"] == len(sw) and len(sw) >= 6000, (st, len(sw))
    left = sum(1 for r in out if "fetch_failed" in str(r[wi]).lower())
    assert left == 6349 - st["swapped"]
    # stamp effect through the REAL TaskResult: klg_kept -> honest fresh_cov, no P-162 enforce needed
    res = mod.TaskResult(key="GLOBAL_MARKETS", sheet_name="Global_Markets", status="success",
                         start_utc="2026-09-26T00:17:04+00:00", rows_written=len(out))
    res._stamp_meta.update({"requested": len(body), "pre_persist_rows": len(body), "klg_kept": st["swapped"],
                            "pw_checked": len(body), "pw_flagged": 0, "rb_checked": len(body), "rb_flagged": 0,
                            "rb_status": "MATCH", "payload_sha8": "2351fc21"})
    row = mod._status_stamp_row("Global_Markets", res, 115)
    assert "data=PARTIAL" in row[3] and row[2] == "PARTIAL_FRESH" and ("preserved=%d" % st["swapped"]) in row[3]
    assert mod._uv_page_state(res)[0] == "STALE_COV"
    return st["swapped"], left


# T7 - seam order + caller line present at the right site
def test_t7_source_order():
    src = open(SYNC, encoding="utf-8").read()
    i_klg = src.index("# --- Keep-last-good substitution (v6.22.3 L4c)")
    i_line = src.index("# v6.63.0 [P-162b]: fetch-failed candidates, one line per page.")
    i_post = src.index("# --- v6.61.0 P-154d: EODHD quota guard, POST-FETCH seam")
    i_cen = src.index("# --- v6.62.0 P-162: fetch-failed census of the OUTGOING matrix")
    assert i_klg < i_line < i_post < i_cen                     # substitute -> refuse-if-still-poisoned -> census -> stamp
    assert "_klg_fetchfail_selftest()" in src and "_KLG_FF_TAG" in src


# T8 - base parity (v6.62.0): identical outputs with the gate off; base cannot substitute fetch-failed rows at all
def test_t8_base_parity():
    bb = b()
    if bb is None:
        return "skipped"
    assert tuple(int(x) for x in bb.SCRIPT_VERSION.split(".")) == (6, 62, 0) and not hasattr(bb, "_klg_fetchfail_mode")
    mod = m()
    _mode(None)
    ob, swb = bb._keep_last_good_rows(_Grid(OLD), "sid", "Global_Markets", list(HDR), fresh())
    od, swd, _, _ = run(mod, None)
    assert ob == od and swb == swd == []
    _mode("enforce")
    ob2, swb2 = bb._keep_last_good_rows(_Grid(OLD), "sid", "Global_Markets", list(HDR), fresh())
    _mode(None)
    assert swb2 == [] and ob2[0][2] == 9.9                     # base ignores the gate: the poisoned row still overwrites
    return "parity"


def _run_all():
    return {"t1": test_t1_vocabulary(), "t2": test_t2_off_unchanged(), "t3": test_t3_observe_counts_only(),
            "t4": test_t4_enforce_substitutes_certified_only(), "t5": test_t5_mixed_forms_and_forced(),
            "t6": test_t6_real_page(), "t7": test_t7_source_order(), "t8": test_t8_base_parity()}


if __name__ == "__main__":
    digests, last = [], None
    for i in range(3):
        last = _run_all()
        digests.append(hashlib.sha256(json.dumps(last, sort_keys=True, default=str).encode()).hexdigest()[:12])
    print("T1..T8 PASS x3 | real page (substituted, left):", last["t6"], "| base parity:", last["t8"])
    print("digests:", " ".join(digests), "identical=%s" % (len(set(digests)) == 1))
