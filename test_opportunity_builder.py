# -*- coding: utf-8 -*-
"""Mocked exec tests for opportunity_builder v1.0.0 (P2 verification gate)."""
import json
import math

import opportunity_builder as ob

FAILS = []


def ok(cond, msg):
    if not cond:
        FAILS.append(msg)


def base_row(sym="4030.SR", **kw):
    """Display-header keys on purpose — exercises the alias adapter."""
    row = {
        "Symbol": sym, "Name": "Test Co", "Market": "Tadawul",
        "Sector": "Energy", "Currency": "SAR", "Current Price": 100.0,
        "Target Price": 130.0, "Intrinsic Value": 125.0,
        "Reliability Score": 80.0, "Data Quality": 90.0,
        "Risk Level": "Medium", "News Trend": "Neutral",
        "Sector Trend": "Positive", "Conflict": "No",
        "Volatility 30D": 4.0, "Avg Volume 30D": 500000,
        "Expected ROI 12M": 22.0, "Recommendation": "BUY",
    }
    row.update(kw)
    return row


PF = {"cash_available_sar": 100000, "pending_proceeds_sar": 20000,
      "portfolio_value_sar": 200000, "holdings": []}

# ---------------------------------------------------------------- truth table
cases = [
    (dict(), "INVEST"),
    ({"Current Price": ""}, "DO_NOT_INVEST"),                  # Price MAJOR
    ({"Currency": "XXX"}, "DO_NOT_INVEST"),                    # FX MAJOR
    ({"Target Price": "", "Intrinsic Value": ""},
     "DO_NOT_INVEST"),                                         # Valuation
    ({"Reliability Score": 62}, "WATCH"),                      # tiered band
    ({"Reliability Score": 50}, "DO_NOT_INVEST"),              # < Min−15
    ({"Data Quality": 70}, "DO_NOT_INVEST"),                   # DQ MAJOR
    ({"Risk Level": "High"}, "DO_NOT_INVEST"),                 # High vs Medium
    ({"Volatility 30D": 20.0}, "DO_NOT_INVEST"),               # R/R MAJOR
    ({"Conflict": "Yes"}, "DO_NOT_INVEST"),                    # Conflict
    ({"News Trend": "Negative"}, "DO_NOT_INVEST"),             # News
    ({"Sector Trend": "Negative"}, "DO_NOT_INVEST"),           # Sector
    ({"News Trend": "Unknown", "Sector Trend": ""}, "INVEST"), # Unknown passes
]
# ROI/AnnROI non-critical isolation needs min_rr relaxed: under the 8%
# stop floor, ROI<12 forces R/R<2 (a MAJOR) — plan-consistent, so test with
# min_rr=0.5 to expose the WATCH path.
p = ob.build_opportunity_payload(
    [base_row(**{"Target Price": 108, "Intrinsic Value": ""})],
    portfolio=PF, criteria={"min_rr": 0.5})
ok(p["candidates_rows"][0]["verdict"] == "WATCH",
   "ROI non-critical case: expected WATCH got %s"
   % p["candidates_rows"][0]["verdict"])
ng = [g["gate"] for g in p["candidates_rows"][0]["gates"]
      if not g["passed"]]
ok(set(ng) == {"ROI", "Annualized ROI"},
   "ROI case failing gates wrong: %s" % ng)

for i, (mut, expect) in enumerate(cases):
    p = ob.build_opportunity_payload([base_row(**mut)], portfolio=PF)
    got = p["candidates_rows"][0]["verdict"]
    ok(got == expect, "truth-table case %d: expected %s got %s (%s)"
       % (i, expect, got, mut))

# allow-flags flip MAJOR fails back to pass
for mut, flag in ((dict(Conflict="Yes"), "allow_conflict"),
                  ({"News Trend": "Negative"}, "allow_negative_news"),
                  ({"Sector Trend": "Negative"}, "allow_negative_sector")):
    p = ob.build_opportunity_payload([base_row(**mut)], portfolio=PF,
                                     criteria={flag: True})
    ok(p["candidates_rows"][0]["verdict"] == "INVEST",
       "allow-flag %s did not pass gate" % flag)

# L8: Low confidence caps INVEST at WATCH (gate passes via min_reliability=50)
p = ob.build_opportunity_payload([base_row(**{"Reliability Score": 58})],
                                 portfolio=PF,
                                 criteria={"min_reliability": 50})
row = p["candidates_rows"][0]
ok(row["verdict"] == "WATCH" and row["confidence_band"] == "Low",
   "Low-confidence cap failed: %s/%s" % (row["verdict"],
                                         row["confidence_band"]))

# ---------------------------------------------------- 1:1 verdict↔gate-trace
syms = ["A.SR", "B.SR", "C.SR", "D.SR"]
rows = [base_row(syms[0]),
        base_row(syms[1], **{"Reliability Score": 62}),
        base_row(syms[2], **{"Data Quality": 10}),
        base_row(syms[3], **{"Reliability Score": 58})]
p = ob.build_opportunity_payload(rows, portfolio=PF,
                                 criteria={"min_reliability": 50})
for a in p["candidates_rows"]:
    re_verdict = ob.derive_verdict(a["gates"], a["reliability"])
    ok(re_verdict == a["verdict"],
       "trace recompute mismatch %s: %s vs %s"
       % (a["symbol"], re_verdict, a["verdict"]))

# --------------------------------------------- Selected⇔INVEST + cap (L2)
rows = [base_row("S%d.SR" % i, **{"Target Price": 130 + i,
                                  "Sector": "Sec%d" % i}) for i in range(5)]
p = ob.build_opportunity_payload(rows, portfolio=PF)
ok(p["kpis"]["passed"] == 5, "expected 5 INVEST, got %s" % p["kpis"]["passed"])
ok(p["kpis"]["selected_count"] == 3 and len(p["selected"]) == 3,
   "Max Selected cap broken")
ok([t["rank"] for t in p["selected"]] == [1, 2, 3], "ranks not 1..3")
sel_syms = {t["symbol"] for t in p["selected"]}
for a in p["candidates_rows"]:
    ok(a["selected"] == (a["symbol"] in sel_syms), "selected flag mismatch")
    if a["selected"]:
        ok(a["verdict"] == "INVEST", "selected non-INVEST row!")
nm_syms = {n["symbol"] for n in p["near_miss"]}
ok(len(nm_syms & {r["Symbol"] for r in rows} - sel_syms) == 2,
   "capacity rows missing from near miss")
cap_rows = [n for n in p["near_miss"] if n["failed_gate"] == "Capacity"]
ok(len(cap_rows) == 2 and all(n["verdict"] == "INVEST" for n in cap_rows),
   "Capacity near-miss rows wrong")

# ------------------------------------------------- sector-cap deferral (§4.2)
rows = [base_row("E%d.SR" % i, **{"Sector": "Energy",
                                  "Target Price": 140 - i}) for i in range(3)]
p = ob.build_opportunity_payload(rows, portfolio=PF)
ok(p["kpis"]["selected_count"] == 2, "max_per_sector=2 not enforced")
deferred = [a for a in p["candidates_rows"] if a["deferral"]]
ok(len(deferred) == 1 and deferred[0]["verdict"] == "INVEST",
   "deferral must keep INVEST verdict")
ok(any(n["failed_gate"] == "Diversification" for n in p["near_miss"]),
   "deferred row missing Diversification near-miss")

# ------------------------------------------------------- funding math (L7)
rows = [base_row("F%d.SR" % i, **{"Sector": "Sec%d" % i}) for i in range(3)]
pf = {"cash_available_sar": 50000, "pending_proceeds_sar": 10000,
      "portfolio_value_sar": 140000, "holdings": []}
p = ob.build_opportunity_payload(rows, portfolio=pf)
ok(p["kpis"]["deployable_sar"] == 60000, "deployable != cash+proceeds")
tot = sum(t["suggested_sar"] for t in p["selected"])
ok(tot <= 60000 + 1e-6, "Σ suggested exceeds deployable")
ok(abs(p["kpis"]["capital_unallocated_sar"] - (60000 - tot)) < 1.0,
   "unallocated identity broken")
# per-ticket cap = 15% × (140k+60k) = 30000
ok(all(t["suggested_sar"] <= 30000 + 1e-6 for t in p["selected"]),
   "max_weight cap broken")
ok(all(("Cash" in t["detail"]["funds_from"]) or
       ("proceeds" in t["detail"]["funds_from"]) or
       ("Unfunded" in t["detail"]["funds_from"])
       for t in p["selected"]),
   "funds_from not named")
# third ticket must dip into proceeds (30k+30k > 50k cash)
ok(any("proceeds" in t["detail"]["funds_from"] for t in p["selected"]),
   "proceeds funding never named despite cash shortfall")
# exp gain = suggested × annROI
for t in p["selected"]:
    ok(abs(t["exp_gain_12m_sar"] -
           round(t["suggested_sar"] * t["ann_roi_pct"] / 100.0, 0)) <= 1.0,
       "exp gain formula mismatch")

# lot rounding
p = ob.build_opportunity_payload([base_row()], portfolio=pf,
                                 criteria={"lot_size": 10})
sh = p["selected"][0]["suggested_shares"]
ok(sh % 10 == 0 and sh > 0, "lot rounding broken: %s" % sh)

# zero deployable ⇒ honest 0-size + alert (L13)
p = ob.build_opportunity_payload([base_row()],
                                 portfolio={"portfolio_value_sar": 100000})
ok(p["selected"][0]["suggested_sar"] == 0, "zero-deployable must size 0")
ok(any(a["type"] == "no_deployable_capital" for a in p["alerts"]),
   "missing no_deployable_capital alert")

# ----------------------------------------------- empty-pool honesty (L13)
p = ob.build_opportunity_payload(
    [base_row("G.SR", **{"Data Quality": 10}),
     base_row("H.SR", **{"Conflict": "Yes"})], portfolio=PF)
ok(p["status"] == "ok" and p["selected"] == [], "empty pool must stay empty")
ok(p["kpis"]["selected_count"] == 0 and p["kpis"]["passed"] == 0,
   "funnel kpis wrong on empty pool")
ok(len(p["near_miss"]) == 2, "near miss must still populate")
p = ob.build_opportunity_payload([])
ok(p["status"] == "no_candidates" and p["selected"] == [],
   "no-rows skeleton wrong")

# ------------------------------------------- budget/coverage pass-through
meta_in = {"budget": {"exhausted": True, "spent_s": 92},
           "coverage": {"Market_Leaders": "76/76"},
           "versions": {"selector": "4.19.0", "engine": "5.85.3"}}
p = ob.build_opportunity_payload([base_row()], portfolio=PF,
                                 upstream_meta=meta_in)
ok(p["meta"]["budget"] == meta_in["budget"], "budget not passed through")
ok(p["meta"]["coverage"] == meta_in["coverage"], "coverage not passed through")
ok(p["meta"]["versions"]["selector"] == "4.19.0" and
   p["meta"]["versions"]["engine"] == "5.85.3", "versions not passed through")
ok(any(a["type"] == "budget_exhausted" for a in p["alerts"]),
   "budget_exhausted alert missing")

# ------------------------------------------------- FX subunits + missing FX
p = ob.build_opportunity_payload(
    [base_row("UK1.L", **{"Currency": "GBp", "Current Price": 250.0,
                          "Target Price": 320.0})],
    portfolio=PF, fx_rates={"GBP": 4.80})
a = p["candidates_rows"][0]
ok(abs(a["price_sar"] - 250.0 * 0.048) < 1e-6,
   "GBp subunit FX wrong: %s" % a["price_sar"])
p = ob.build_opportunity_payload(
    [base_row("ZZ.X", **{"Currency": "QQQ"})], portfolio=PF)
a = p["candidates_rows"][0]
ok(a["first_fail"]["gate"] == "FX" and a["verdict"] == "DO_NOT_INVEST",
   "missing FX must MAJOR-fail")
ok(any(al["type"] == "missing_fx" for al in p["alerts"]),
   "missing_fx alert absent")

# held-symbol structural exclusion
pf_held = dict(PF, holdings=[{"symbol": "4030.SR", "sector": "Energy",
                              "market": "Tadawul", "value_sar": 50000}])
p = ob.build_opportunity_payload([base_row("4030.SR")], portfolio=pf_held)
a = p["candidates_rows"][0]
ok(a["structural_block"] and a["verdict"] == "INVEST" and not a["selected"],
   "structural exclusion must block selection, not verdict")

# ----------------------------------------------- L8 sentence completeness
p = ob.build_opportunity_payload([base_row()], portfolio=PF)
note = p["selected"][0]["advisor_note"]
for token in ("INVEST", "SAR", "stop", "TP1", "TP2", "Confidence",
              "Review by"):
    ok(token in note, "advisor sentence missing '%s': %s" % (token, note))

# ------------------------------------------------- JSON safety (NaN input)
p = ob.build_opportunity_payload(
    [base_row(**{"Volatility 30D": float("nan"),
                 "Avg Volume 30D": float("inf")})], portfolio=PF)
json.dumps(p, allow_nan=False)  # raises on violation

# blended kpis sanity
p = ob.build_opportunity_payload(
    [base_row("S%d.SR" % i, **{"Sector": "Sec%d" % i}) for i in range(3)],
    portfolio=PF)
ok(60 <= p["kpis"]["blended_reliability"] <= 100, "blended reliability odd")
ok(p["kpis"]["blended_rr"] is not None, "blended rr missing")

# score weights integrity
ok(abs(sum(ob.SCORE_WEIGHTS.values()) - 100.0) < 1e-9,
   "§4.3 weights must sum to 100")

# kill switch
import os
os.environ["TFB_OPP_ENABLED"] = "0"
p = ob.build_opportunity_payload([base_row()], portfolio=PF)
ok(p["status"] == "disabled", "kill switch ignored")
os.environ["TFB_OPP_ENABLED"] = "1"

if FAILS:
    print("EXEC-TEST FAIL (%d):" % len(FAILS))
    for f in FAILS:
        print(" -", f)
    raise SystemExit(1)
print("EXEC-TEST PASS (all assertions)")
