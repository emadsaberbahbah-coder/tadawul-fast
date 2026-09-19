"""P-146 (data_engine_v2 v5.143.0) — FUND-SENTRY repair leg + mode disclosure.

Repo-runnable battery T1-T9 over the REAL module (no stand-ins). Goldens:
  * ROW_0845 / ROW_0858 — the v5.140.0 DDI.US production snapshots
    (2026-09-10; the 100x fraction signature and its coherent twin).
  * Inputs lifted VERBATIM from the 2026-09-19 Global_Markets / My_Portfolio
    exports (market_cap / pe_ttm / revenue_ttm as exported): BRK-B.US and
    BNY.US (quarantined on that export with PLAUSIBLE implied margins
    21.68 / 14.31), ESSA.JK / HQH.US / GDHG.US (unit-inconsistent inputs,
    implied 265,434 / 4,613 / 39,679 pp), DDI.US holding row (implied 33.25).
Properties asserted at HEAD:
  off inert; observe byte-identical to v5.140.0 (tag-only, never repairs or
  skips); enforce = three-way verdict (fail-open above 100pp, repair inside
  the 100x band [90,110] in both directions, quarantine otherwise); repaired
  values always land inside the plausible window; band edges; tags
  substring-safe; mode disclosed in surface_gate_states() and wired into
  the [GUARDS] boot line.

Run:  python tests/test_de_fund_sentry_repair_p146.py
      (or pytest -q tests/test_de_fund_sentry_repair_p146.py)
"""
import copy
import hashlib
import importlib
import inspect
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
de = importlib.import_module("core.data_engine_v2")

# ---- goldens -----------------------------------------------------------------
ROW_0845 = {"symbol": "DDI.US", "pe_ttm": 5.04, "market_cap": 631806336.0,
            "revenue_ttm": 380037581.0, "profit_margin": 0.33}
ROW_0858 = {"symbol": "DDI.US", "pe_ttm": 5.05, "market_cap": 630319770.0,
            "revenue_ttm": 380043008.0, "profit_margin": 32.91}
# 2026-09-19 export inputs (implied margin recomputed from these exact numbers)
BRKB = {"symbol": "BRK-B.US", "pe_ttm": 13.07, "market_cap": 1090049409024.0,
        "revenue_ttm": 384687000000.0}                     # implied 21.68
BNY = {"symbol": "BNY.US", "pe_ttm": 17.92, "market_cap": 103824703488.0,
       "revenue_ttm": 40481000000.0}                       # implied 14.31
DDI_0919 = {"symbol": "DDI.US", "pe_ttm": 5.00, "market_cap": 631806360.0,
            "revenue_ttm": 380043008.0}                    # implied 33.25
ESSA = {"symbol": "ESSA.JK", "pe_ttm": 11.5723, "market_cap": 10422320103424.0,
        "revenue_ttm": 339303576.0}                        # implied 265,434
HQH = {"symbol": "HQH.US", "pe_ttm": 4.796421, "market_cap": 1226201570.0,
       "revenue_ttm": 5541667.0}                           # implied 4,613
GDHG = {"symbol": "GDHG.US", "pe_ttm": 0.0061, "market_cap": 67819624.0,
        "revenue_ttm": 28020070.0}                         # implied 39,679

REPAIRED = "fund_coherence_repaired:profit_margin"
SKIPPED = "fund_coherence_skipped:profit_margin:implied_oob"
QUAR = "fund_coherence_quarantined:profit_margin"


def _implied(r):
    return 100.0 * (r["market_cap"] / r["pe_ttm"]) / r["revenue_ttm"]


def _row(base, pm):
    r = copy.deepcopy(base)
    r["profit_margin"] = pm
    return r


def _run(base, pm, mode):
    r = _row(base, pm)
    tag = de._fund_coherence_sentry(r, mode)
    return tag, r["profit_margin"]


# ---- T1 off is inert ----------------------------------------------------------
def test_t1_off_inert():
    for base, pm in ((ROW_0845, 0.33), (BRKB, 0.2168), (ESSA, 12.5)):
        r = _row(base, pm)
        assert de._fund_coherence_sentry(r, "off") is None
        assert r == _row(base, pm)


# ---- T2 observe byte-identical to v5.140.0 -----------------------------------
def test_t2_observe_tag_only_never_repairs_or_skips():
    # 100x signature, plausible-implied 100x, and OOB shapes: observe ALWAYS
    # returns the v5.140.0 observe tag and NEVER mutates the row.
    for base, pm in ((ROW_0845, 0.33), (BRKB, 0.2168), (BNY, 0.1431),
                     (ESSA, 12.5), (HQH, 40.0), (BRKB, 2168.0)):
        tag, val = _run(base, pm, "observe")
        assert tag == QUAR + ":observe", (base["symbol"], tag)
        assert val == pm
    # coherent twin stays quiet in observe as before
    assert de._fund_coherence_sentry(copy.deepcopy(ROW_0858), "observe") is None


# ---- T3 enforce repairs the 100x fraction (x100) -------------------------------
def test_t3_enforce_repair_x100():
    tag, val = _run(ROW_0845, 0.33, "enforce")
    assert tag == REPAIRED + ":x100" and abs(val - 33.0) < 1e-9
    for base, frac in ((BRKB, 0.2168), (BNY, 0.1431), (DDI_0919, 0.3325)):
        tag, val = _run(base, frac, "enforce")
        assert tag == REPAIRED + ":x100", (base["symbol"], tag)
        assert abs(val - frac * 100.0) < 1e-6
        # repaired value coheres with the implied benchmark (< 8x, ~1x here)
        assert abs(val / _implied(base) - 1.0) < 0.02


# ---- T4 enforce repairs the 100x-inflated percent (d100) -----------------------
def test_t4_enforce_repair_d100():
    for base, pct in ((BRKB, 2168.0), (BNY, 1431.0)):
        tag, val = _run(base, pct, "enforce")
        assert tag == REPAIRED + ":d100", (base["symbol"], tag)
        assert abs(val - pct / 100.0) < 1e-6


# ---- T5 enforce fails OPEN above 100pp implied ---------------------------------
def test_t5_enforce_oob_fail_open():
    for base, pm in ((ESSA, 12.5), (HQH, 40.0), (GDHG, 3.2), (ESSA, 0.125)):
        tag, val = _run(base, pm, "enforce")
        assert tag == SKIPPED, (base["symbol"], tag)
        assert val == pm                                   # value untouched


# ---- T6 enforce quarantines non-signature divergence exactly as v5.140.0 -------
def test_t6_enforce_quarantine_unchanged_outside_band():
    imp = _implied(BRKB)                                   # 21.68
    for pm in (imp / 20.0, imp * 20.0, imp / 50.0, imp * 12.0):
        tag, val = _run(BRKB, pm, "enforce")
        assert tag == QUAR and val is None, (pm, tag)
    # coherent rows stay quiet; tiny implied never judged (v5.140.0 guards)
    assert de._fund_coherence_sentry(copy.deepcopy(ROW_0858), "enforce") is None
    tiny = {"pe_ttm": 100.0, "market_cap": 1e8, "revenue_ttm": 1e9,
            "profit_margin": 90.0}
    assert de._fund_coherence_sentry(tiny, "enforce") is None


# ---- T7 band edges + plausible-window invariant --------------------------------
def test_t7_band_edges_and_window():
    imp = _implied(BRKB)
    lo, hi = de._FUND_SENTRY_REPAIR_RATIO_LO, de._FUND_SENTRY_REPAIR_RATIO_HI
    assert (lo, hi) == (90.0, 110.0)
    assert _run(BRKB, imp / lo, "enforce")[0] == REPAIRED + ":x100"
    assert _run(BRKB, imp / hi, "enforce")[0] == REPAIRED + ":x100"
    assert _run(BRKB, imp / (lo - 0.5), "enforce")[0] == QUAR
    assert _run(BRKB, imp / (hi + 0.5), "enforce")[0] == QUAR
    assert _run(BRKB, imp * lo, "enforce")[0] == REPAIRED + ":d100"
    assert _run(BRKB, imp * (hi + 0.5), "enforce")[0] == QUAR
    # a repair can only ever land inside [MIN, MAX] pp (the guards bound it)
    for base in (BRKB, BNY, DDI_0919):
        for pm in (_implied(base) / 100.0, _implied(base) * 100.0):
            tag, val = _run(base, pm, "enforce")
            assert tag.startswith(REPAIRED)
            assert (de._FUND_SENTRY_IMPLIED_MARGIN_MIN_PCT <= abs(val)
                    <= de._FUND_SENTRY_IMPLIED_MARGIN_MAX_PCT)


# ---- T8 tags substring-safe (reliability-scan bans) ----------------------------
def test_t8_tags_substring_safe():
    banned = ("cap", "forecast", "target", "roi", "drop", "reject")
    for t in (de._FUND_SENTRY_REPAIRED_TAG, de._FUND_SENTRY_SKIPPED_TAG,
              de._FUND_SENTRY_QUARANTINE_TAG, de._FUND_SENTRY_TAG_PREFIX):
        assert not any(b in t.lower() for b in banned), t
    assert de._FUND_SENTRY_QUARANTINE_TAG == "fund_coherence_quarantined"


# ---- T9 mode disclosure: health engine_gates + boot-line wiring ----------------
def test_t9_mode_disclosure():
    saved = os.environ.get("TFB_FUND_UNIT_SENTRY")
    try:
        for raw, want in ((None, "off"), ("observe", "observe"),
                          ("enforce", "enforce"), ("1", "enforce"),
                          ("garbage", "off")):
            if raw is None:
                os.environ.pop("TFB_FUND_UNIT_SENTRY", None)
            else:
                os.environ["TFB_FUND_UNIT_SENTRY"] = raw
            assert de._fund_unit_sentry_mode() == want
            assert de.surface_gate_states().get("fund_unit_sentry") == want
    finally:
        if saved is None:
            os.environ.pop("TFB_FUND_UNIT_SENTRY", None)
        else:
            os.environ["TFB_FUND_UNIT_SENTRY"] = saved
    src = inspect.getsource(de)
    assert src.count('"fund_lkg=%s fund_unit_sentry=%s",') == 1
    assert src.count("_fund_unit_sentry_mode(),   # v5.143.0") == 1
    assert de.__version__ == "5.143.0"


TESTS = [test_t1_off_inert, test_t2_observe_tag_only_never_repairs_or_skips,
         test_t3_enforce_repair_x100, test_t4_enforce_repair_d100,
         test_t5_enforce_oob_fail_open,
         test_t6_enforce_quarantine_unchanged_outside_band,
         test_t7_band_edges_and_window, test_t8_tags_substring_safe,
         test_t9_mode_disclosure]

if __name__ == "__main__":
    trail = []
    for fn in TESTS:
        fn()
        trail.append(fn.__name__)
        print("PASS", fn.__name__)
    # deterministic digest of the enforce verdicts on every golden
    verdicts = []
    for base, pm in ((ROW_0845, 0.33), (BRKB, 0.2168), (BNY, 0.1431),
                     (DDI_0919, 0.3325), (BRKB, 2168.0), (ESSA, 12.5),
                     (HQH, 40.0), (GDHG, 3.2), (BRKB, 1.0)):
        verdicts.append([base["symbol"], pm, list(_run(base, pm, "enforce"))])
    digest = hashlib.sha256(json.dumps(verdicts, sort_keys=True).encode()).hexdigest()[:16]
    print("ALL PASS", len(trail), "digest", digest)
