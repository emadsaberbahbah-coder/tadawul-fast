"""v1.4.0 harness — stub repo deps, import BOTH trees, exercise pure paths
against the REAL Shadow_Board evidence shape. x3 identical digests."""
import hashlib, importlib.util, json, os, sys, types

def stub_modules():
    core = types.ModuleType("core")
    def mk(name, **attrs):
        m = types.ModuleType(name)
        for k, v in attrs.items(): setattr(m, k, v)
        sys.modules[name] = m
        return m
    class V:  # compliance verdict shape
        def __init__(s): s.invest_eligible=True; s.shariah_status="SCREEN_RETIRED"; s.shariah_source="retired"; s.tradability="BROKER_TRADABLE"; s.venue="NYSE"; s.floor_unlocked=True
    cg = mk("core.compliance_gate", evaluate=lambda *a, **k: V(),
            INVEST_OK_STATUSES={"SCREEN_RETIRED"},
            build_authority_index=lambda *a, **k: {})
    sa = mk("core.shariah_authority")
    rg = mk("core.regime")
    rl = mk("core.risk_limits")
    sys.modules["core"] = core
    core.compliance_gate = cg; core.shariah_authority = sa
    core.regime = rg; core.risk_limits = rl
    ca = types.ModuleType("core.analysis"); sys.modules["core.analysis"] = ca
    ob = mk("core.analysis.opportunity_builder",
            rt_cost_pct=lambda sym, ticket: 0.5,
            _venue_floor=lambda sym: 10000.0)
    pa = mk("core.analysis.portfolio_actions")
    ca.opportunity_builder = ob; ca.portfolio_actions = pa
    core.analysis = ca

def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    sys.modules[name] = m
    spec.loader.exec_module(m)
    return m

stub_modules()
BASE = load("board.py", "sb_base")
FIX  = load("board_v140.py", "sb_fix")

def run():
    out = {}
    # geometry contracts
    out["hdr"] = {"base_last": BASE.OUT_HEADER[-1], "fix_last": FIX.OUT_HEADER[-1],
                  "fix_penult": FIX.OUT_HEADER[-2],
                  "widths": [len(BASE.OUT_HEADER), len(FIX.OUT_HEADER)]}
    assert out["hdr"]["base_last"] == out["hdr"]["fix_last"] == "Gen2 Eligible"
    assert out["hdr"]["fix_penult"] == "ROI Src" and out["hdr"]["widths"] == [17, 18]
    # parse battery (fix only; base lacks fn)
    assert not hasattr(BASE, "_parse_engine_roi_cell")
    P = FIX._parse_engine_roi_cell
    cases = {"arrow_pct": P("\u25b2 35.00%"), "frac": P("0.325"),
             "cap": P("999"), "dash": P("\u2014"), "junk": P("n/a"),
             "neg_pct": P("-6.11%"), "plain": P("22")}
    assert cases["arrow_pct"] == (35.0, "pct") and cases["frac"] == (32.5, "fraction_fixed")
    assert cases["cap"] == (None, "capped") and cases["dash"] == (None, "")
    assert cases["junk"] == (None, "unparsed") and cases["neg_pct"] == (-6.11, "pct")
    assert cases["plain"] == (22.0, "pct")
    out["parse"] = {k: list(v) for k, v in cases.items()}
    # tonight's REAL evidence shape: 9 blank-ROI candidates
    syms = ["PINFRA.MX","ADAM.US","FRST.US","KRP.US","MRP.US","TSM.US","NBIX.US","GLNG.US","AMG.US"]
    def cands():
        return [{"symbol": s, "name": s, "action": "", "roi_pct": None,
                 "confidence_band": "High", "market_value_sar": 10000.0,
                 "sector": "X"} for s in syms]
    roi_map = {s: {"roi": 20.0 + i, "tag": ("fraction_fixed" if i == 1 else "pct")}
               for i, s in enumerate(syms[:7])}     # 7 resolved, 2 unresolved
    # OFF: board rows byte-equal to base except one empty trailing-before-last col
    c_off = cands(); n_off = FIX.apply_engine_roi(c_off, roi_map, "off")
    rows_off, _ = FIX.evaluate_board(c_off, {}, {}, 130000.0)
    rows_base, _ = BASE.evaluate_board(cands(), {}, {}, 130000.0)
    strip = [r[:16] + r[17:] for r in rows_off]     # drop ROI Src cell
    assert strip == rows_base and all(r[16] == "" for r in rows_off)
    assert all(r[-1] == "NO" and r[14] == "NO_ROI" for r in rows_off)
    assert n_off == {"blank": 0, "applied": 0, "fraction_fixed": 0, "unresolved": 0}
    # OBSERVE: annotations, eligibility unchanged
    c_obs = cands(); n_obs = FIX.apply_engine_roi(c_obs, roi_map, "observe")
    rows_obs, _ = FIX.evaluate_board(c_obs, {}, {}, 130000.0)
    assert n_obs["applied"] == 7 and n_obs["unresolved"] == 2 and n_obs["fraction_fixed"] == 1
    assert all(r[-1] == "NO" and r[14] == "NO_ROI" for r in rows_obs)
    assert sum(1 for r in rows_obs if str(r[16]).startswith("obs:")) == 7
    assert any("\u2020" in str(r[16]) for r in rows_obs)
    # ENFORCE: NO_ROI melts, Gen2 flips where edge clears
    c_enf = cands(); n_enf = FIX.apply_engine_roi(c_enf, roi_map, "enforce")
    rows_enf, _ = FIX.evaluate_board(c_enf, {}, {}, 130000.0)
    verd = {r[0]: (r[14], r[-1], r[16]) for r in rows_enf}
    assert n_enf["applied"] == 7
    trade = [s for s, v in verd.items() if v[0] == "TRADE" and v[1] == "YES"]
    still = [s for s, v in verd.items() if v[0] == "NO_ROI"]
    assert len(trade) == 7 and sorted(still) == sorted(syms[7:])
    assert all(verd[s][2] in ("engine", "engine/frac") for s in trade)
    assert verd["ADAM.US"][2] == "engine/frac"
    # eligible_symbols still keys on last column in BOTH trees
    assert FIX.eligible_symbols(rows_enf) == set(trade)
    assert BASE.eligible_symbols(rows_base) == set()
    out["counts"] = {"off": n_off, "obs": n_obs, "enf": n_enf,
                     "trade": sorted(trade), "still_no_roi": sorted(still)}
    # env mode plumbing
    for v, exp in (("", "off"), ("observe", "observe"), ("ENFORCE", "enforce"), ("bogus", "off")):
        os.environ["TFB_BOARD_ENGINE_ROI"] = v
        assert FIX._engine_roi_mode() == exp
    os.environ.pop("TFB_BOARD_ENGINE_ROI", None)
    return out

digs = []
for i in range(3):
    r = run()
    d = hashlib.sha256(json.dumps(r, sort_keys=True).encode()).hexdigest()[:16]
    digs.append(d)
    if i == 0: print(json.dumps(r["counts"], indent=1))
    print("pass", i + 1, "digest", d)
assert len(set(digs)) == 1
print("HARNESS PASS x3, digest", digs[0])
print("versions:", BASE.SCRIPT_VERSION, "->", FIX.SCRIPT_VERSION)
