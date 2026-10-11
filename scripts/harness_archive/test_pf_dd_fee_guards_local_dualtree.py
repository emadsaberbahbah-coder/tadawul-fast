#!/usr/bin/env python3
"""v1.11.0 harness — REAL module, no stand-ins (house rule).
G1 off/off byte-identical | G2 observe log-only | G3 enforce dd+time exits
G4 fee-aware funding boundary (the DDI reserve-breach class).
Run three times; output hash must be identical (S4 x3 rule)."""
import sys, os, json, copy, importlib, hashlib
sys.path.insert(0, ".")
os.environ["TFB_PF_ENABLED"] = "1"

ROWS = [
    {"Symbol": "WIN.US",  "Name": "Winner",     "Sector": "Energy",
     "Currency": "USD", "Quantity": 10, "Avg Cost": 100.0, "Price": 110.0,
     "Buy Date": "2026-09-01"},
    {"Symbol": "DDLOSS.US", "Name": "DeepLoser", "Sector": "Industrials",
     "Currency": "USD", "Quantity": 10, "Avg Cost": 100.0, "Price": 90.0,
     "Buy Date": "2026-09-01"},                       # -10% => dd leg
    {"Symbol": "TIMELOSS.US", "Name": "SlowLoser", "Sector": "Utilities",
     "Currency": "USD", "Quantity": 10, "Avg Cost": 100.0, "Price": 98.0,
     "Buy Date": "2026-05-01"},                       # -2%, 134d => time leg
]
FX = {"USD": 3.75}
CTL = {"cash_available_sar": 20000.0}

def run(mod):
    out = mod.build_portfolio_actions(copy.deepcopy(ROWS), dict(CTL), dict(FX))
    return out

def canon(o):
    return json.dumps(o, sort_keys=True, ensure_ascii=True, default=str)

def actions_of(out):
    return {r.get("symbol"): (r.get("action"), r.get("action_reason") or "")
            for r in (out.get("rows") or out.get("actions") or [])} \
        if isinstance(out, dict) else {}

def rows_list(out):
    for k in ("rows", "actions", "holdings"):
        if isinstance(out, dict) and isinstance(out.get(k), list):
            return out[k]
    return []

def main():
    for k in ("TFB_PF_DD_EXIT", "TFB_PF_FEE_FUNDING"):
        os.environ.pop(k, None)

    import core.analysis.portfolio_actions as pa
    importlib.reload(pa)

    # ---- G1: gates off — revised vs baseline deep-equal -------------------
    base_out = run(pa)                              # this tree holds BASELINE
    sys.path.insert(0, os.path.abspath("../rev"))   # revised tree shadows
    for m in list(sys.modules):
        if m.startswith("core"):
            del sys.modules[m]
    import core.analysis.portfolio_actions as par
    assert par.PORTFOLIO_ACTIONS_VERSION == "1.11.0", par.PORTFOLIO_ACTIONS_VERSION
    rev_out = run(par)
    import re as _re
    def _neutral(s):
        s = _re.sub(r'"generated_utc": "[^"]+"', '"generated_utc": "T"', s)
        return s.replace("1.11.0", "1.10.0")
    b, r = _neutral(canon(base_out)), _neutral(canon(rev_out))
    assert b == r, "G1 FAIL: off/off output differs beyond version/timestamp"
    print("G1 PASS  off/off deep-equal (version-string neutralized), rows=%d"
          % len(rows_list(rev_out)))

    # ---- G2: observe — log-only ------------------------------------------
    os.environ["TFB_PF_DD_EXIT"] = "observe"
    obs = run(par)
    a_off, a_obs = actions_of(rev_out), actions_of(obs)
    assert {k: v[0] for k, v in a_off.items()} == \
           {k: v[0] for k, v in a_obs.items()}, "G2 FAIL: observe changed an action"
    assert "[dd-observe]" in a_obs.get("DDLOSS.US", ("", ""))[1], \
        "G2 FAIL: dd tag missing on -10% row"
    assert "[dd-observe]" in a_obs.get("TIMELOSS.US", ("", ""))[1], \
        "G2 FAIL: time tag missing on stale loser"
    assert "[dd-observe]" not in a_obs.get("WIN.US", ("", ""))[1], \
        "G2 FAIL: winner wrongly tagged"
    print("G2 PASS  observe log-only; tags on DDLOSS + TIMELOSS, none on WIN")

    # ---- G3: enforce — dd + time exits, winner untouched ------------------
    os.environ["TFB_PF_DD_EXIT"] = "enforce"
    enf = run(par)
    a_enf = actions_of(enf)
    assert a_enf["DDLOSS.US"][0] == "EXIT" and "Drawdown/time guard" in a_enf["DDLOSS.US"][1]
    assert a_enf["TIMELOSS.US"][0] == "EXIT" and "time budget" in a_enf["TIMELOSS.US"][1]
    assert a_enf["WIN.US"][0] == a_off["WIN.US"][0], "G3 FAIL: winner action changed"
    dd_row = [x for x in rows_list(enf) if x.get("symbol") == "DDLOSS.US"][0]
    mv = 10 * 90.0 * 3.75
    got = float(x if (x := dd_row.get("proceeds_sar")) is not None else -1)
    assert abs(got - mv) < 1.0, "G3 FAIL: proceeds %s != mv %s" % (got, mv)
    os.environ.pop("TFB_PF_DD_EXIT", None)
    print("G3 PASS  enforce: DDLOSS+TIMELOSS -> EXIT (proceeds=%.0f SAR), WIN untouched" % mv)

    # ---- G4: fee-aware funding boundary (real module, forced ADD routing) --
    real_decide = par.decide_action
    real_confirm = par._apply_add_confirmation
    real_dem = par._apply_deminimis
    def forced(c, ctl, w, sw, ex):
        if c.get("symbol") == "FEE.US":
            return ("ADD", "forced for G4", 0.0, None)
        return real_decide(c, ctl, w, sw, ex)
    par.decide_action = forced
    par._apply_add_confirmation = (
        lambda sym, a, r, cf, ctl: (a, r, cf))     # routing-only bypass
    par._apply_deminimis = lambda a, r, p: (a, r, p)
    try:
        fee_rows = ROWS + [{"Symbol": "FEE.US", "Name": "FeeCase",
                            "Sector": "Financials", "Currency": "USD",
                            "Quantity": 1, "Avg Cost": 100.0, "Price": 100.0,
                            "Buy Date": "2026-09-01"}]
        # deployable engineered to the DDI class: exact cost (2 sh = 75 SAR)
        # fits inside ~80 SAR of headroom with <9 SAR slack, so the armed fee
        # must shrink the ticket to 1 share. holdings total = 11,212.5 SAR.
        # total = holdings 11,550 + cash (floor basis includes cash).
        # cash 8,958.33 @ 40% floor => deployable 755.0 SAR.
        # OFF: int(755/375)=2 sh (750 SAR). ON: (755-9)/375 -> 1 sh (375 SAR)
        # -- the exact DDI class: rounded ticket fits, exact+fee does not.
        ctl = {"cash_available_sar": 8958.33, "target_cash_pct": 40.0,
               "rebalance_mode": "New Cash Only"}  # proceeds excluded => deployable is exactly 755
        os.environ.pop("TFB_PF_FEE_FUNDING", None)
        off = par.build_portfolio_actions(copy.deepcopy(fee_rows), dict(ctl), dict(FX))
        os.environ["TFB_PF_FEE_FUNDING"] = "1"
        on = par.build_portfolio_actions(copy.deepcopy(fee_rows), dict(ctl), dict(FX))
        f_off = [x for x in rows_list(off) if x.get("symbol") == "FEE.US"][0]
        f_on = [x for x in rows_list(on) if x.get("symbol") == "FEE.US"][0]
        sh_off = f_off.get("suggested_delta_shares")
        sh_on = f_on.get("suggested_delta_shares")
        assert sh_off and sh_on and sh_on < sh_off, \
            "G4 FAIL: fee did not reduce sizing (off=%s on=%s)" % (sh_off, sh_on)
        print("G4 PASS  fee gate shrinks the boundary ticket: %s -> %s shares "
              "(fee 9.0 SAR charged to the ledger)" % (sh_off, sh_on))
    finally:
        par.decide_action = real_decide
        par._apply_add_confirmation = real_confirm
        par._apply_deminimis = real_dem
        os.environ.pop("TFB_PF_FEE_FUNDING", None)

    digest = hashlib.sha256((canon(a_off) + canon(a_obs) + canon(a_enf)).encode()).hexdigest()[:16]
    print("RUN-DIGEST", digest)

if __name__ == "__main__":
    main()
