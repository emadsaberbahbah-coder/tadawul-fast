#!/usr/bin/env python3
"""F-1a dual-tree harness — REAL modules (base v1.11.1 vs delivered v1.12.0),
REAL dependency (opportunity_builder v1.19.6, live-fetched), REAL rows
(today's My_Portfolio export, display headers). No stand-ins.
F1 legacy byte-identity (unit + integration) | F2 observe evidence on the
real book | F3 plan3m golden-negative flip pair | F4 DATA_GAP fail-closed |
F5 threshold env override. Battery runs 3x; digests must be identical."""
import csv, hashlib, importlib.util, json, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
MP_TSV = ("/mnt/user-data/uploads/"
          "_Market_Share_Deepseek-V3_-_My_Portfolio.tsv")
FX = {"USD": 3.7515, "SAR": 1.0}
# Today's LIVE control panel (Portfolio_Decision export 2026-09-14 07:15):
PANEL = {"cash_available_sar": 23242.50, "target_cash_pct": 10.0,
         "max_position_pct": 20.0, "max_sector_pct": 30.0,
         "min_reliability_add": 70.0, "min_dq_add": 80.0,
         "rebalance_mode": "Advisory"}


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    sys.modules[name] = m
    spec.loader.exec_module(m)
    return m


def clear_env():
    for k in list(os.environ):
        if k.startswith("TFB_"):
            del os.environ[k]


def real_rows():
    with open(MP_TSV, encoding="utf-8") as f:
        rd = csv.reader(f, delimiter="\t")
        hdr = next(rd)
        return [dict(zip(hdr, r)) for r in rd if r and r[0].strip()]


def syn(sym, roi, tp1, rel=90.0, dq=95.0, price=100.0):
    return {"symbol": sym, "name": sym, "sector": "TestSec",
            "price": price, "fx_to_sar": 3.75, "quantity": 10,
            "market_value_sar": price * 3.75 * 10,
            "cost_sar": price * 3.75 * 10, "avg_cost": price,
            "reliability": rel, "dq": dq, "roi_pct": roi, "tp1": tp1,
            "conflict": False,
            "engine_gate": {"investability": "INVESTABLE", "reasons": ""},
            "engine_recommendation": "BUY",
            "forecast_source": "provider_target"}


def canon(obj, cut_tags=False):
    """Canonical JSON for identity checks: drop per-call wall-clock and the
    additive new control echo, normalize version strings, optionally strip
    the observe evidence suffix from string values. Behavioral fields
    (actions, reasons, proceeds, sizing, alerts) are untouched — the
    base-vs-new diff was adjudicated to exactly these classes."""
    import copy
    def walk(x):
        if isinstance(x, dict):
            return {k: walk(v) for k, v in x.items()
                    if k not in ("generated_utc", "add_roi_3m_pct")}
        if isinstance(x, list):
            return [walk(v) for v in x]
        if isinstance(x, str):
            if cut_tags and "[f1-observe]" in x:
                # excise ONLY the tag segment (it may sit mid-note, before
                # the stop/TP/review suffix the note builder appends):
                x = re.sub(r"; \[f1-observe\] [^;]*; legacy basis kept",
                           "", x)
            return x.replace("1.12.0", "1.11.1")
        return x
    return json.dumps(walk(copy.deepcopy(obj)), sort_keys=True)


def battery(base, new):
    out = {}
    rows = real_rows()
    W = (10.0, 15.0, 0.0)  # weight, sector-weight, excess share

    # ---- F1: legacy identity (unit) --------------------------------------
    clear_env()
    cb, cn = base.make_controls(None), new.make_controls(None)
    assert "add_roi_3m_pct" not in cb and cn["add_roi_3m_pct"] == 3.0
    unit = []
    for r in (syn("A", 13.0, 102.0), syn("B", 8.0, 105.0),
              syn("C", 15.0, None)):
        tb = base.decide_action(dict(r), cb, *W)
        tn = new.decide_action(dict(r), cn, *W)
        assert tb == tn, ("F1 unit drift", r["symbol"], tb, tn)
        unit.append(tn)
    out["F1_unit"] = unit

    # ---- F1: legacy identity (integration, real rows, LIVE panel) --------
    clear_env()
    rb = base.build_portfolio_actions([dict(r) for r in rows], dict(PANEL),
                                      dict(FX))
    rn = new.build_portfolio_actions([dict(r) for r in rows], dict(PANEL),
                                     dict(FX))
    jb, jn = canon(rb), canon(rn)
    assert "[f1-" not in jn, "f1 tags leaked in legacy"
    assert jn == jb, "F1 integration drift"
    acts = [a["action"] for a in rn["actions"]]
    assert acts == ["HOLD"] * 7, acts          # reproduces the live 07:15 run
    joined = " | ".join(a["action_reason"] for a in rn["actions"])
    for frag in ("Upside 3.4% below add threshold 12.0%",
                 "Upside 2.5% below add threshold 12.0%",
                 "PRECEDENCE (\u00a74.7): engine verdict WATCHLIST"):
        assert frag in joined, frag             # live-verbatim reason heads
    assert sum("pending confirmation" in a["action_reason"]
               for a in rn["actions"]) == 2     # CARE + DDI, as on the page
    out["F1_integration_sha"] = hashlib.sha256(jb.encode()).hexdigest()[:16]
    # defaults-path identity (no panel) stays covered too:
    rb0 = base.build_portfolio_actions([dict(r) for r in rows], None, dict(FX))
    rn0 = new.build_portfolio_actions([dict(r) for r in rows], None, dict(FX))
    assert canon(rn0) == canon(rb0), "F1 defaults-path drift"

    # ---- F2: observe evidence on the real book ---------------------------
    clear_env()
    os.environ["TFB_FORECAST_BASIS"] = "observe"
    ro = new.build_portfolio_actions([dict(r) for r in rows], dict(PANEL),
                                     dict(FX))
    reasons = [a["action_reason"] for a in ro["actions"]]
    tags = sum("[f1-observe]" in s for s in reasons)
    flips = sum("- FLIP" in s for s in reasons)
    gaps = sum("DATA_GAP" in s for s in reasons)
    assert (tags, flips, gaps) == (7, 0, 0), (tags, flips, gaps)
    assert all(s.count("[f1-observe]") == 1 for s in reasons)   # idempotent
    assert canon(ro, cut_tags=True) == jb, "observe changed a decision"
    out["F2"] = {"tags": tags, "flips": flips, "gaps": gaps}

    # ---- F3: plan3m golden-negative flip pair ----------------------------
    clear_env()
    os.environ["TFB_FORECAST_BASIS"] = "plan3m"
    cp = new.make_controls(None)
    a = new.decide_action(syn("A", 13.0, 102.0), cp, *W)   # legacy-pass, plan 2.0
    b = new.decide_action(syn("B", 8.0, 105.0), cp, *W)    # legacy-fail, plan 5.0
    assert a[0] == "HOLD" and "[f1:plan3m]" in a[1] and "2.0%" in a[1], a
    assert b[0] == "ADD" and "Plan 3M ROI 5.0%" in b[1], b
    clear_env()
    a0 = new.decide_action(syn("A", 13.0, 102.0), new.make_controls(None), *W)
    b0 = new.decide_action(syn("B", 8.0, 105.0), new.make_controls(None), *W)
    assert a0[0] == "ADD" and b0[0] == "HOLD", (a0, b0)   # the flip is real
    out["F3"] = {"plan3m": [a, b], "legacy": [a0, b0]}

    # ---- F4: DATA_GAP fails closed / observe discloses -------------------
    os.environ["TFB_FORECAST_BASIS"] = "plan3m"
    c = new.decide_action(syn("C", 15.0, None), new.make_controls(None), *W)
    assert c[0] == "HOLD" and "[f1:DATA_GAP]" in c[1], c
    os.environ["TFB_FORECAST_BASIS"] = "observe"
    tag = new._apply_f1_observe_tag(syn("C", 15.0, None), "X",
                                    new.make_controls(None))
    assert "DATA_GAP" in tag, tag
    out["F4"] = [c, tag]

    # ---- F5: threshold env override --------------------------------------
    clear_env()
    os.environ["TFB_FORECAST_BASIS"] = "plan3m"
    os.environ["TFB_PF_ADD_ROI_3M_PCT"] = "7"
    c7 = new.make_controls(None)
    assert c7["add_roi_3m_pct"] == 7.0
    b7 = new.decide_action(syn("B", 8.0, 105.0), c7, *W)
    assert b7[0] == "HOLD" and "below add threshold 7.0%" in b7[1], b7
    out["F5"] = b7
    clear_env()
    return out


base = load("portfolio_actions_base_m", os.path.join(HERE,
            "portfolio_actions_base.py"))
new = load("portfolio_actions", os.path.join(HERE, "portfolio_actions.py"))
assert base.PORTFOLIO_ACTIONS_VERSION == "1.11.1"
assert new.PORTFOLIO_ACTIONS_VERSION == "1.12.0"

digests = []
for i in range(3):
    d = hashlib.sha256(json.dumps(battery(base, new), sort_keys=True,
                                  default=str).encode()).hexdigest()
    digests.append(d)
    print("run %d digest %s" % (i + 1, d[:16]))
assert len(set(digests)) == 1, digests
print("F1-F5 PASS x3 — digest %s" % digests[0][:16])
