#!/usr/bin/env python3
"""data_engine_v2 v5.141.0 horizon-coherence harness — REAL module.
J1 off/off deep-equal vs pinned base (both entry points, 5 fixtures)
J2 observe: legacy values + countable incoherence tags (both live defect
   shapes: 3M+365 and 30+1Y) | J3 enforce: blank side derived (90 from 3M,
   1M from 30), both-blank -> 1Y/365 untagged, both-present never rewritten
J4 label<->days round-trip stable for all five labels. Run x3, same digest."""
import sys, os, json, copy, hashlib
FIX = [
    ("Global_Markets", {"symbol": "A.US", "invest_period_label": "3M"}),   # live defect
    ("Global_Markets", {"symbol": "B.US", "horizon_days": 30}),            # inverse defect
    ("Global_Markets", {"symbol": "C.US"}),                                # both blank
    ("Global_Markets", {"symbol": "D.US", "invest_period_label": "1Y",
                        "horizon_days": 365}),                             # coherent pair
    ("Mutual_Funds",   {"symbol": "E_FUND", "invest_period_label": "3M",
                        "horizon_days": 365}),                             # legacy stamped pair
]
def canon(o): return json.dumps(o, sort_keys=True, default=str)
def run_all(de):
    outs = []
    for page, row in FIX:
        outs.append(de._apply_page_row_backfill(page, copy.deepcopy(row)))
    outs.append(de._apply_symbol_context_defaults(
        {"symbol": "CL=F", "invest_period_label": "3M"}, page="Commodities_FX"))
    return outs
def hz(o): return (o.get("invest_period_label"), o.get("horizon_days"),
                   o.get("warnings"))
def main():
    os.environ.pop("TFB_HORIZON_COHERENT", None)
    sys.path.insert(0, "bpkg"); import core.data_engine_v2 as db
    assert db.__version__ == "5.140.0"
    base = run_all(db)
    for m in list(sys.modules):
        if m.startswith("core"): del sys.modules[m]
    sys.path.insert(0, "rpkg"); import core.data_engine_v2 as dr
    assert dr.__version__ == "5.141.0"
    # J1 off/off
    rev = run_all(dr)
    assert canon(base) == canon(rev), "J1 FAIL off/off differs"
    assert hz(rev[0])[:2] == ("3M", 365), "defect shape changed"
    print("J1 PASS  off/off deep-equal on both entry points (defect 3M+365 intact)")
    # J2 observe
    os.environ["TFB_HORIZON_COHERENT"] = "observe"
    obs = run_all(dr)
    for i in range(len(obs)):
        a, b = dict(rev[i]), dict(obs[i]); a.pop("warnings", None); b.pop("warnings", None)
        assert canon(a) == canon(b), f"J2 FAIL: observe changed values row {i}"
    assert "horizon_incoherent:3M!=365:observe" in (obs[0]["warnings"] or "")
    assert "horizon_incoherent:1Y!=30:observe" in (obs[1]["warnings"] or "")
    assert "horizon_incoherent" not in (obs[3].get("warnings") or "")
    assert "horizon_incoherent:3M!=365:observe" in (obs[4]["warnings"] or "")
    assert "horizon_incoherent:3M!=365:observe" in (obs[5]["warnings"] or "")
    print("J2 PASS  observe log-only; both defect shapes + legacy pair + =F site tagged; coherent pair clean")
    # J3 enforce
    os.environ["TFB_HORIZON_COHERENT"] = "enforce"
    enf = run_all(dr)
    assert hz(enf[0])[:2] == ("3M", 90), hz(enf[0])
    assert "horizon_coherent_fill:days_from_3M:enforce" in (enf[0]["warnings"] or "")
    assert hz(enf[1])[:2] == ("1M", 30), hz(enf[1])
    assert hz(enf[2])[:2] == ("1Y", 365) and "horizon" not in (enf[2].get("warnings") or "")
    assert hz(enf[3])[:2] == ("1Y", 365) and "horizon" not in (enf[3].get("warnings") or "")
    assert hz(enf[4])[:2] == ("3M", 365), "J3 FAIL: both-present pair was rewritten"
    assert "horizon_incoherent:3M!=365:enforce" in (enf[4]["warnings"] or "")
    assert hz(enf[5])[:2] == ("3M", 90), hz(enf[5])
    print("J3 PASS  enforce: 3M->90, 30->1M, blank->1Y/365, both-present preserved+tagged, =F site coherent")
    # J4 round trip
    for lbl, d in dr._HZC_LABEL_TO_DAYS.items():
        assert dr._hzc_label_for_days(d) == lbl, (lbl, d)
    print("J4 PASS  label<->days round-trip stable for all five labels")
    os.environ.pop("TFB_HORIZON_COHERENT", None)
    print("RUN-DIGEST", hashlib.sha256((canon(obs)+canon(enf)).encode()).hexdigest()[:16])
if __name__ == "__main__":
    main()
