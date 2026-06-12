# -*- coding: utf-8 -*-
"""Behavioral suite for portfolio_actions v1.0.0 (real opportunity_builder)."""
import sys
sys.path.insert(0, '.')
from core.analysis import portfolio_actions as pa

FAILS = []


def ok(name, cond, extra=""):
    if not cond:
        FAILS.append("%s %s" % (name, extra))


def H(symbol, qty=100, cost=30.0, price=34.5, iv=41.4, rel=83, dq=92,
      ccy="SAR", sector="Financials", reco="BUY", conflict="No", **kw):
    row = {
        "Symbol": symbol, "Name": symbol + " Co", "Sector": sector,
        "Exchange": "Tadawul", "Currency": ccy, "Quantity": qty,
        "Buy Price": cost, "Current Price": price, "Intrinsic Value": iv,
        "Expected ROI 12M": 15.0, "Forecast Reliability Score": rel,
        "Data Quality Score": dq, "Risk Bucket": "Low",
        "Provider/Engine Conflict": conflict, "Volatility 30D": 4.0,
        "Avg Volume 30D": 1000000, "Recommendation Detail": reco,
        "Investability Status": "INVESTABLE", "Block Reason": "",
    }
    row.update(kw)
    return row


FX = {"SAR": 1.0, "USD": 3.75}
CTL = {"cash_available_sar": 100000, "target_cash_pct": 10,
       "max_position_pct": 15, "max_sector_pct": 30,
       "min_reliability_add": 70, "min_dq_add": 80,
       "rebalance_mode": "Advisory Only"}


def find(p, sym):
    for a in p["actions"]:
        if a["symbol"] == sym:
            return a
    return None


# --- 1. clean ADD with sizing + funding -------------------------------------
p = pa.build_portfolio_actions([H("1050.SR")], controls=CTL, fx_rates=FX)
a = find(p, "1050.SR")
ok("t1-status", p["status"] == "ok", p["status"])
ok("t1-action", a["action"] == "ADD", a["action"])
ok("t1-mv", a["market_value_sar"] == 3450)
ok("t1-pnl", a["pnl_sar"] == 450 and a["pnl_pct"] == 15.0,
   (a["pnl_sar"], a["pnl_pct"]))
ok("t1-roi", a["roi_pct"] == 20.0, a["roi_pct"])
ok("t1-sized", a["suggested_delta_sar"] > 0 and
   a["suggested_delta_shares"] > 0)
ok("t1-funds", a["funds_from"] == "cash", a["funds_from"])
# position cap: total = 3450 + 100000; cap 15% => max position 15517.5;
# headroom = 15517.5-3450 = 12067.5; deployable = 100000-10345 = 89655
total = 103450.0
ok("t1-cap", a["suggested_delta_sar"] <= 0.15 * total - 3450 + 40)
ok("t1-deployable", p["kpis"]["deployable_sar"] == round(100000 - 0.10 *
                                                         total))
ok("t1-note", "ADD" in a["advisor_note"] and "review by" in a["advisor_note"])
ok("t1-stop", a["stop_sar"] is not None and a["stop_sar"] < a["price_sar"])

# --- 2. BLOCK on missing price ----------------------------------------------
p = pa.build_portfolio_actions([H("X.SR", price="N/A")], controls=CTL,
                               fx_rates=FX)
a = find(p, "X.SR")
ok("t2-block", a["action"] == "BLOCK", a["action"])
ok("t2-reason", "price" in a["action_reason"])
ok("t2-alert", any(al["type"] == "blocked_positions" for al in p["alerts"]))

# --- 3. BLOCK on missing quantity --------------------------------------------
p = pa.build_portfolio_actions([H("Q.SR", qty="")], controls=CTL, fx_rates=FX)
ok("t3-block", find(p, "Q.SR")["action"] == "BLOCK")

# --- 4. EXIT on valuation ROI <= -15 ------------------------------------------
p = pa.build_portfolio_actions([H("OV.SR", price=50.0, iv=40.0)],
                               controls=CTL, fx_rates=FX)  # roi = -20%
a = find(p, "OV.SR")
ok("t4-exit", a["action"] == "EXIT", a["action"])
ok("t4-proceeds", a["proceeds_sar"] == 5000, a["proceeds_sar"])
ok("t4-reason", "exit threshold" in a["action_reason"])

# --- 5. EXIT on sell-tier engine reco ----------------------------------------
p = pa.build_portfolio_actions([H("SL.SR", reco="STRONG_SELL")],
                               controls=CTL, fx_rates=FX)
ok("t5-exit", find(p, "SL.SR")["action"] == "EXIT")

# --- 6. low confidence caps EXIT -> HOLD --------------------------------------
p = pa.build_portfolio_actions([H("LC.SR", price=50.0, iv=40.0, rel=45)],
                               controls=CTL, fx_rates=FX)
a = find(p, "LC.SR")
ok("t6-hold", a["action"] == "HOLD", a["action"])
ok("t6-capped", a["detail"]["capped_from"] == "EXIT")
ok("t6-alert", any(al["type"] == "low_confidence_capped"
                   for al in p["alerts"]))

# --- 7. TRIM on position-cap breach (trim exactly to cap) --------------------
ctl7 = dict(CTL)
ctl7["cash_available_sar"] = 10000
# qty 1000 x 34.5 = 34500 mv; total = 44500; weight 77.5% > 15%
p = pa.build_portfolio_actions([H("BIG.SR", qty=1000, iv=36.0)],
                               controls=ctl7, fx_rates=FX)
a = find(p, "BIG.SR")
ok("t7-trim", a["action"] == "TRIM", a["action"])
target_mv = 0.15 * 44500.0
ok("t7-amount", abs(a["proceeds_sar"] - round(34500 - target_mv)) <= 1,
   a["proceeds_sar"])
ok("t7-reason", "Max Position" in a["action_reason"])

# --- 8. sector-cap pro-rata trim across two holdings -------------------------
ctl8 = dict(CTL)
ctl8["cash_available_sar"] = 0
ctl8["max_position_pct"] = 60  # disable position cap for the test
rows8 = [H("S1.SR", qty=500, iv=36.0), H("S2.SR", qty=500, iv=36.0)]
# each mv 17250, sector total 34500 = 100% > 30%; excess = 24150
p = pa.build_portfolio_actions(rows8, controls=ctl8, fx_rates=FX)
a1, a2 = find(p, "S1.SR"), find(p, "S2.SR")
ok("t8-both-trim", a1["action"] == "TRIM" and a2["action"] == "TRIM",
   (a1["action"], a2["action"]))
excess = 34500 - 0.30 * 34500
ok("t8-prorata", abs(a1["proceeds_sar"] - round(excess / 2)) <= 1,
   (a1["proceeds_sar"], excess / 2))
ok("t8-sector-flag", p["sector_summary"][0]["over_cap"] is True)

# --- 9. valuation TRIM (between -15 and -5) trims 50% -------------------------
p = pa.build_portfolio_actions([H("VT.SR", price=44.0, iv=40.0)],
                               controls=CTL, fx_rates=FX)  # roi ~ -9.1%
a = find(p, "VT.SR")
ok("t9-trim", a["action"] == "TRIM", a["action"])
ok("t9-half", abs(a["proceeds_sar"] - round(4400 * 0.5)) <= 1,
   a["proceeds_sar"])

# --- 10. New Cash Only excludes proceeds from deployable ----------------------
rows10 = [H("EX.SR", qty=400, price=50.0, iv=40.0),     # EXIT 20000 proceeds
          H("AD.SR", qty=10)]                            # ADD candidate
ctl10 = dict(CTL)
ctl10["cash_available_sar"] = 5000
ctl10["rebalance_mode"] = "New Cash Only"
p = pa.build_portfolio_actions(rows10, controls=ctl10, fx_rates=FX)
ok("t10-proceeds-excluded", p["kpis"]["proceeds_pending_sar"] == 0,
   p["kpis"]["proceeds_pending_sar"])
total10 = 400 * 50.0 + 10 * 34.5 + 5000
floor10 = 0.10 * total10
ok("t10-deployable", p["kpis"]["deployable_sar"] ==
   round(max(0, 5000 - floor10)), p["kpis"]["deployable_sar"])

# --- 11. Advisory mode includes proceeds; funding split names both sources ---
ctl11 = dict(ctl10)
ctl11["rebalance_mode"] = "Advisory Only"
ctl11["target_cash_pct"] = 0
ctl11["max_position_pct"] = 95
ctl11["max_sector_pct"] = 95
rows11 = [H("EX2.SR", qty=400, price=50.0, iv=40.0, sector="Energy"),
          H("AD2.SR", qty=10, sector="Tech")]
p = pa.build_portfolio_actions(rows11, controls=ctl11, fx_rates=FX)
a = find(p, "AD2.SR")
ok("t11-add-sized", a["action"] == "ADD" and a["suggested_delta_sar"] > 5000,
   (a["action"], a["suggested_delta_sar"]))
ok("t11-funds-split", a["funds_from"] is not None and
   "proceeds" in a["funds_from"], a["funds_from"])
ok("t11-identity", p["kpis"]["adds_funded_sar"] <=
   p["kpis"]["deployable_sar"])

# --- 12. qualified ADD with zero deployable stays ADD, unsized (L13) ----------
ctl12 = dict(CTL)
# cash 60000, target cash 95% -> floor 60277.5 > cash => deployable 0,
# while weight 3450/63450 = 5.4% stays under the 15% position cap.
ctl12["cash_available_sar"] = 60000
ctl12["target_cash_pct"] = 95
p = pa.build_portfolio_actions([H("ZD.SR")], controls=ctl12, fx_rates=FX)
a = find(p, "ZD.SR")
ok("t12-add", a["action"] == "ADD", a["action"])
ok("t12-zero", a["suggested_delta_sar"] == 0)
ok("t12-note", "no deployable" in a["action_reason"])
ok("t12-alert", any(al["type"] == "no_deployable_capital"
                    for al in p["alerts"]))

# --- 13. missing cost basis: action still computed, P&L None, alert ----------
p = pa.build_portfolio_actions([H("NC.SR", cost="")], controls=CTL,
                               fx_rates=FX)
a = find(p, "NC.SR")
ok("t13-action", a["action"] == "ADD")
ok("t13-pnl-none", a["pnl_sar"] is None and a["pnl_pct"] is None)
ok("t13-alert", any(al["type"] == "missing_cost_basis"
                    for al in p["alerts"]))

# --- 14. USD holding: FX applied to mv/pnl/levels -----------------------------
p = pa.build_portfolio_actions(
    [H("O.US", ccy="USD", price=56.1, cost=50.0, iv=70.0, qty=100)],
    controls=CTL, fx_rates=FX)
a = find(p, "O.US")
ok("t14-fx", a["fx_to_sar"] == 3.75)
ok("t14-mv", a["market_value_sar"] == round(100 * 56.1 * 3.75))
ok("t14-pnl", a["pnl_sar"] == round(100 * 6.1 * 3.75))

# --- 15. missing FX currency -> BLOCK ------------------------------------------
p = pa.build_portfolio_actions([H("Z.ZZ", ccy="XXX")], controls=CTL,
                               fx_rates=FX)
ok("t15-block", find(p, "Z.ZZ")["action"] == "BLOCK")

# --- 16. HOLD reason names binding fact ----------------------------------------
p = pa.build_portfolio_actions([H("HL.SR", iv=36.0)], controls=CTL,
                               fx_rates=FX)  # roi 4.3% < 12
a = find(p, "HL.SR")
ok("t16-hold", a["action"] == "HOLD")
ok("t16-why", "below add threshold" in a["action_reason"])

# --- 17. conflict blocks ADD -> HOLD with reason --------------------------------
p = pa.build_portfolio_actions([H("CF.SR", conflict="Yes")], controls=CTL,
                               fx_rates=FX)
a = find(p, "CF.SR")
ok("t17-hold", a["action"] == "HOLD", a["action"])
ok("t17-why", "conflict" in a["action_reason"].lower())

# --- 18. empty holdings: honest empty payload -----------------------------------
p = pa.build_portfolio_actions([], controls=CTL, fx_rates=FX)
ok("t18-empty", p["status"] == "empty")
ok("t18-cash", p["kpis"]["cash_sar"] == 100000)

# --- 19. disabled kill-switch ---------------------------------------------------
import os
os.environ["TFB_PF_ENABLED"] = "0"
p = pa.build_portfolio_actions([H("A.SR")], controls=CTL, fx_rates=FX)
ok("t19-disabled", p["status"] == "disabled")
os.environ["TFB_PF_ENABLED"] = "1"

# --- 20. KPI integrity + action ordering (EXIT first, HOLD last) -----------------
rows20 = [H("AA.SR"), H("BB.SR", price=50.0, iv=40.0),
          H("CC.SR", iv=36.0)]
p = pa.build_portfolio_actions(rows20, controls=CTL, fx_rates=FX)
acts = [a["action"] for a in p["actions"]]
ok("t20-order", acts[0] == "EXIT" and acts[-1] == "HOLD", acts)
k = p["kpis"]
ok("t20-counts", k["action_counts"]["EXIT"] == 1 and k["positions"] == 3)
ok("t20-value", k["portfolio_value_sar"] ==
   round(3450 + 5000 + 3450 + 100000))
ok("t20-blended", k["blended_reliability"] == 83.0)
ok("t20-meta", p["meta"]["versions"]["opportunity_builder_floor_ok"] is True)

# --- 21. controls coercion (strings/Yes-No/mode aliases) --------------------------
ctl = pa.make_controls({"target_cash_pct": "12.5", "lot_size": "0",
                        "rebalance_mode": "new cash only please"})
ok("t21-float", ctl["target_cash_pct"] == 12.5)
ok("t21-lot", ctl["lot_size"] == 1)
ok("t21-mode", ctl["rebalance_mode"] == "New Cash Only")

# --- 22. JSON serializable end-to-end --------------------------------------------
import json
p = pa.build_portfolio_actions(rows20, controls=CTL, fx_rates=FX)
json.dumps(p)
ok("t22-json", True)

if FAILS:
    print("FAILURES (%d):" % len(FAILS))
    for f in FAILS:
        print("  -", f)
    sys.exit(1)
print("ALL 22 TEST GROUPS PASS")
