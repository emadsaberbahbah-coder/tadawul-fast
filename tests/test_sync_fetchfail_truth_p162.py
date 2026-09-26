#!/usr/bin/env python3
"""tests/test_sync_fetchfail_truth_p162.py
P-162 - run_dashboard_sync v6.62.0 FETCH-FAILED STAMP TRUTH.
Dual-tree, REAL-module harness (the K-battery loader): loads the delivered
script and, when TFB_TEST_SYNC_BASE points at the v6.61.0 file, the base too.
Drives the REAL TaskResult / TaskSpec classes and the REAL pure functions
(_fetchfail_count_rows, _fetchfail_truth_apply, _status_stamp_row,
_uv_page_state) on the REAL 2026-09-26 Global_Markets export matrix
(TFB_TEST_EXPORT_DIR, browser TSVs) and replays the inserted census seam
verbatim from the delivered source. No stand-in result objects.
Run: pytest -q tests/test_sync_fetchfail_truth_p162.py
  or python3 tests/test_sync_fetchfail_truth_p162.py   (x3, digest)"""
from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import os
import re
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SYNC = os.environ.get("TFB_TEST_SYNC_DELIVERED") or os.path.join(ROOT, "scripts", "run_dashboard_sync.py")
BASE = os.environ.get("TFB_TEST_SYNC_BASE", "")
EXPORT = os.environ.get("TFB_TEST_EXPORT_DIR", "")
csv.field_size_limit(10 ** 9)
os.environ.setdefault("GITHUB_RUN_ID", "36199188352")   # both trees stamp the same run id (else each module mints a uuid)


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
        _M["d"] = _load(SYNC, "rds_p162_delivered")
    return _M["d"]


def b():
    if "b" not in _M:
        _M["b"] = _load(BASE, "rds_p162_base") if BASE and os.path.exists(BASE) else None
    return _M["b"]


def _setmode(mod, v):
    if v is None:
        os.environ.pop("TFB_SYNC_FETCHFAIL_TRUTH", None)
    else:
        os.environ["TFB_SYNC_FETCHFAIL_TRUTH"] = v


def _gm_matrix():
    """Real Global_Markets page from the browser export -> (headers, rows)."""
    p = os.path.join(EXPORT, "Global_Markets.tsv")
    with open(p, encoding="utf-8", errors="replace", newline="") as fh:
        rows = list(csv.reader(fh, delimiter="\t"))
    hdr = rows[0]
    body = [r for r in rows[1:] if any(c.strip() for c in r)]
    return hdr, body


def _res(mod, page="Global_Markets", **meta):
    r = mod.TaskResult(key=page.upper(), sheet_name=page, status="success",
                       start_utc="2026-09-26T00:17:04+00:00", end_utc="2026-09-26T01:18:17+00:00",
                       symbols_requested=int(meta.get("requested") or 0),
                       rows_written=int(meta.get("pre_persist_rows") or 0), rows_failed=0)
    r._stamp_meta.update(meta)
    return r


def _norm(row):
    return [re.sub(r"v6\.6\d\.\d", "vX", str(c)) for c in row]


GM_META = {"requested": 6609, "pre_persist_rows": 6609, "klg_kept": 165, "pw_checked": 6609,
           "pw_flagged": 5, "rb_checked": 6609, "rb_flagged": 5, "rb_status": "MATCH",
           "payload_sha8": "2351fc21"}


# T1 - vocabulary / defaults / certification -------------------------------------
def test_t1_vocabulary():
    mod = m()
    for v, exp in ((None, "off"), ("", "off"), ("1", "off"), ("ON", "off"), ("observe", "observe"),
                   ("Observe ", "observe"), ("enforce", "enforce"), ("ENFORCE", "enforce")):
        _setmode(mod, v)
        assert mod._fetchfail_truth_mode() == exp, (v, exp)
    assert mod._fetchfail_truth_selftest() == "PASS"
    saved = mod._FFT_SELFTEST_MSG
    try:
        mod._FFT_SELFTEST_MSG = "FAIL 4/5"
        _setmode(mod, "enforce")
        assert mod._fetchfail_truth_mode() == "observe"          # FG-3 / DS-03: FAIL degrades enforce
        _setmode(mod, "observe")
        assert mod._fetchfail_truth_mode() == "observe"
    finally:
        mod._FFT_SELFTEST_MSG = saved
        _setmode(mod, None)
    # v6.63.0: version pin loosened to a floor (the 09-22 practice) so later builds carry this battery.
    assert tuple(int(x) for x in mod.SCRIPT_VERSION.split(".")) >= (6, 62, 0), mod.SCRIPT_VERSION


# T2 - census on the REAL export ----------------------------------------------------
def test_t2_census_real_export():
    if not EXPORT:
        return
    mod = m()
    hdr, body = _gm_matrix()
    t0 = datetime(2026, 9, 26, 0, 17, 4, tzinfo=timezone.utc).timestamp()
    c = mod._fetchfail_count_rows(hdr, body, t0)
    # independent count
    wi, ui = hdr.index("Warnings"), hdr.index("Last Updated (UTC)")
    new = car = 0
    for r in body:
        if "fetch_failed" not in r[wi].lower():
            continue
        ts = r[ui].strip()
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            isnew = dt.timestamp() >= t0 - 120.0
        except Exception:
            isnew = True
        if isnew:
            new += 1
        else:
            car += 1
    assert c == {"ff_new": new, "ff_carried": car}
    assert new + car == 6349 and new >= 6300, c        # 6,302 x 402 + 47 x 404 on the 09-26 page
    assert mod._fetchfail_count_rows(hdr[:5], body, t0) == {"ff_new": 0, "ff_carried": 0}
    return c


# T3 - stamp row on the REAL leg numbers (REAL TaskResult) ---------------------------
def test_t3_stamp_row_modes():
    mod = m()
    meta = dict(GM_META, ff_new=6302, ff_carried=0)
    _setmode(mod, None)
    off = mod._status_stamp_row("Global_Markets", _res(mod, **meta), 115)
    _setmode(mod, "observe")
    obs = mod._status_stamp_row("Global_Markets", _res(mod, **meta), 115)
    _setmode(mod, "enforce")
    enf = mod._status_stamp_row("Global_Markets", _res(mod, **meta), 115)
    _setmode(mod, None)
    for row in (off, obs, enf):
        assert len(row) == 10 and row[0] == "Global_Markets" and row[6] == 6609
    assert "fresh=6444" in off[3] and "fresh_cov=97.5%" in off[3] and "data=COMPLETE" in off[3] and "fetchfail" not in off[3]
    assert off[2] == "SUCCESS"
    assert "fresh=6444" in obs[3] and "fresh_cov=97.5%" in obs[3] and "data=COMPLETE" in obs[3]
    assert " fetchfail=6302/0 would_cov=2.1%" in obs[3] and obs[2] == "SUCCESS"
    assert "fresh=142" in enf[3] and "fresh_cov=2.1%" in enf[3] and "data=PARTIAL" in enf[3]
    assert " fetchfail=6302/0" in enf[3] and "would_cov" not in enf[3]
    assert enf[2] == "PARTIAL_FRESH"
    # message order: fetchfail sits between preserved and fresh_cov
    assert enf[3].index("preserved=165") < enf[3].index("fetchfail=") < enf[3].index("fresh_cov=")
    return off, obs, enf


# T4 - feed token mirrors the stamp --------------------------------------------------
def test_t4_feed_token():
    mod = m()
    meta = dict(GM_META, ff_new=6302, ff_carried=0)
    _setmode(mod, None)
    assert mod._uv_page_state(_res(mod, **meta)) == ("OK", 97.5)
    _setmode(mod, "observe")
    assert mod._uv_page_state(_res(mod, **meta)) == ("OK", 97.5)
    _setmode(mod, "enforce")
    assert mod._uv_page_state(_res(mod, **meta)) == ("STALE_COV", 2.1)
    # partial storm below the 95% floor still flips; above it stays OK
    small = dict(GM_META, ff_new=200, ff_carried=4)          # 6444-200 = 6244 -> 94.5%
    assert mod._uv_page_state(_res(mod, **small)) == ("STALE_COV", 94.5)
    tiny = dict(GM_META, ff_new=100, ff_carried=4)           # 6344 -> 96.0%
    assert mod._uv_page_state(_res(mod, **tiny)) == ("OK", 96.0)
    _setmode(mod, None)


# T5 - healthy leg: byte-identical stamp text in every mode ---------------------------
def test_t5_healthy_leg_identical():
    mod = m()
    ml = {"requested": 255, "pre_persist_rows": 255, "klg_kept": 0, "pw_checked": 255, "pw_flagged": 0,
          "rb_checked": 255, "rb_flagged": 0, "rb_status": "MATCH", "payload_sha8": "4153d92b",
          "ff_new": 0, "ff_carried": 0}
    rows = []
    for v in (None, "observe", "enforce"):
        _setmode(mod, v)
        rows.append(mod._status_stamp_row("Market_Leaders", _res(mod, "Market_Leaders", **ml), 115))
    _setmode(mod, None)
    assert rows[0][3] == rows[1][3] == rows[2][3] and "fetchfail" not in rows[0][3]
    assert "fresh_cov=100.0%" in rows[0][3] and "data=COMPLETE" in rows[0][3] and rows[0][2] == "SUCCESS"
    # carried-only rows: values unchanged, disclosure only
    car = dict(ml, ff_new=0, ff_carried=3)
    _setmode(mod, "enforce")
    r = mod._status_stamp_row("Market_Leaders", _res(mod, "Market_Leaders", **car), 115)
    _setmode(mod, None)
    assert "fresh_cov=100.0%" in r[3] and " fetchfail=0/3" in r[3] and "data=COMPLETE" in r[3]


# T6 - base parity (v6.61.0 vs delivered off / observe values) -------------------------
def test_t6_base_parity():
    bb = b()
    if bb is None:
        return "skipped"
    assert bb.SCRIPT_VERSION == "6.61.0" and not hasattr(bb, "_fetchfail_truth_apply")
    meta = dict(GM_META, ff_new=6302, ff_carried=0)   # base ignores the two keys
    base_row = bb._status_stamp_row("Global_Markets", _res(bb, **meta), 115)
    mod = m()
    _setmode(mod, None)
    off_row = mod._status_stamp_row("Global_Markets", _res(mod, **meta), 115)
    _setmode(mod, "enforce")
    enf_row = mod._status_stamp_row("Global_Markets", _res(mod, **meta), 115)
    _setmode(mod, None)
    nb, no = _norm(base_row), _norm(off_row)
    assert nb[0] == no[0] and nb[2] == no[2] and nb[3] == no[3] and nb[4:] == no[4:]   # B is a timestamp
    assert bb._uv_page_state(_res(bb, **meta)) == ("OK", 97.5) == mod._uv_page_state(_res(mod, **meta))
    assert "data=COMPLETE" in base_row[3] and "data=PARTIAL" in enf_row[3]          # the lie vs the truth
    return "parity"


# T7 - seam replay: the inserted census block, verbatim from the delivered source -------
_SEAM_RE = re.compile(
    r"        # --- v6\.62\.0 P-162: fetch-failed census of the OUTGOING matrix --------\n(.*?)\n        # -{70}\n",
    re.S)


def _seam_src():
    src = open(SYNC, encoding="utf-8").read()
    mm = _SEAM_RE.search(src)
    assert mm, "census seam not found in the delivered source"
    block = "\n".join(l[8:] for l in mm.group(0).split("\n"))   # dedent 8
    # placement proof: after the v6.61.0 post-fetch seam, before persistence verification
    i_post = src.index("# --- v6.61.0 P-154d: EODHD quota guard, POST-FETCH seam")
    i_seam = src.index("# --- v6.62.0 P-162: fetch-failed census")
    i_pv = src.index("# --- Persistence outcome verification (v6.22.2 L4b)")
    assert i_post < i_seam < i_pv
    return block


def test_t7_seam_replay():
    mod = m()
    block = _seam_src()
    hdr = ["Symbol", "Warnings", "Last Updated (UTC)"]
    rows = [["A.US", "fetch_failed:HTTP 402", "2026-09-26T00:40:00+00:00"],
            ["B.US", "ok", "2026-09-26T00:40:00+00:00"],
            ["C.US", "fetch_failed:HTTP 404 not_found", "2026-09-25T13:05:00+00:00"]]
    mod._EQ_STATE["t0"] = datetime(2026, 9, 26, 0, 17, 4, tzinfo=timezone.utc).timestamp()
    ns = dict(vars(mod))
    for v, exp in ((None, None), ("observe", (1, 1)), ("enforce", (1, 1))):
        _setmode(mod, v)
        task = mod.TaskSpec(key="GLOBAL_MARKETS", sheet_name="Global_Markets", gateway="analysis")
        res = mod.TaskResult(key="GLOBAL_MARKETS", sheet_name="Global_Markets", status="success",
                             start_utc="2026-09-26T00:17:04+00:00")
        ns.update({"task": task, "res": res, "headers": hdr, "rows_matrix": rows})
        exec(compile(block, "<p162-seam>", "exec"), ns)
        if exp is None:
            assert "ff_new" not in res._stamp_meta and "ff_carried" not in res._stamp_meta
        else:
            assert (res._stamp_meta["ff_new"], res._stamp_meta["ff_carried"]) == exp
    # scope: a decision-owned / non-market page never gets the keys
    _setmode(mod, "enforce")
    task = mod.TaskSpec(key="TOP_10_INVESTMENTS", sheet_name="Top_10_Investments", gateway="analysis")
    res = mod.TaskResult(key="TOP_10_INVESTMENTS", sheet_name="Top_10_Investments", status="success",
                         start_utc="2026-09-26T00:17:04+00:00")
    ns.update({"task": task, "res": res})
    exec(compile(block, "<p162-seam>", "exec"), ns)
    assert "ff_new" not in res._stamp_meta
    _setmode(mod, None)
    if EXPORT:  # the real page through the same seam
        hdr, body = _gm_matrix()
        _setmode(mod, "enforce")
        res = mod.TaskResult(key="GLOBAL_MARKETS", sheet_name="Global_Markets", status="success",
                             start_utc="2026-09-26T00:17:04+00:00")
        ns.update({"task": mod.TaskSpec(key="GLOBAL_MARKETS", sheet_name="Global_Markets", gateway="analysis"),
                   "res": res, "headers": hdr, "rows_matrix": body})
        exec(compile(block, "<p162-seam>", "exec"), ns)
        _setmode(mod, None)
        assert res._stamp_meta["ff_new"] + res._stamp_meta["ff_carried"] == 6349
        return res._stamp_meta["ff_new"], res._stamp_meta["ff_carried"]


# T8 - the real 09-26 page end to end: census -> stamp -> feed ---------------------------
def test_t8_real_page_end_to_end():
    if not EXPORT:
        return
    mod = m()
    hdr, body = _gm_matrix()
    t0 = datetime(2026, 9, 26, 0, 17, 4, tzinfo=timezone.utc).timestamp()
    c = mod._fetchfail_count_rows(hdr, body, t0)
    meta = dict(GM_META, **c)
    _setmode(mod, "enforce")
    row = mod._status_stamp_row("Global_Markets", _res(mod, **meta), 115)
    st = mod._uv_page_state(_res(mod, **meta))
    _setmode(mod, None)
    assert "data=PARTIAL" in row[3] and row[2] == "PARTIAL_FRESH" and st[0] == "STALE_COV" and st[1] < 5.0
    return row[3], st


def _run_all():
    out = {}
    out["t1"] = test_t1_vocabulary()
    out["t2"] = test_t2_census_real_export()
    out["t3"] = test_t3_stamp_row_modes()
    out["t4"] = test_t4_feed_token()
    out["t5"] = test_t5_healthy_leg_identical()
    out["t6"] = test_t6_base_parity()
    out["t7"] = test_t7_seam_replay()
    out["t8"] = test_t8_real_page_end_to_end()
    return out


if __name__ == "__main__":
    digests = []
    last = None
    for i in range(3):
        last = _run_all()
        blob = json.dumps({k: (re.sub(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d[^ ]*", "TS", json.dumps(v, default=str)) if v is not None else None)
                           for k, v in last.items()}, sort_keys=True)
        digests.append(hashlib.sha256(blob.encode()).hexdigest()[:12])
    print("T1..T8 PASS x3 | base parity:", last["t6"], "| real census:", last["t2"], "| seam replay:", last["t7"])
    print("enforce stamp (real 09-26 GM):", last["t8"][0] if last["t8"] else "n/a")
    print("digests:", " ".join(digests), "identical=%s" % (len(set(digests)) == 1))
