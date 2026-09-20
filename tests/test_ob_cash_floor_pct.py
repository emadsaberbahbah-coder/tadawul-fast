"""tests/test_ob_cash_floor_pct.py — opportunity_builder v1.22.0 [CASH-FLOOR-PCT].

Runs the REAL core.analysis.opportunity_builder end-to-end (no stand-ins).

T1  env readers: pct unset/blank/invalid/out-of-range => None (gate off);
    mode default observe, "enforce" only when spelled.
T2  unset == byte-identical payload vs an explicit blank; no alert, no meta.
T3  observe: selection, sizing, funding, KPIs identical to off; ONE
    countable "cash_floor" alert with the floor, pre/post deployable and the
    would-lose-funding seat count; meta.cash_floor carries the numbers.
T4  enforce: floor = pct x (holdings + cash); deployable KPI = cash - floor;
    sum(suggested) <= post-floor deployable; funds_from never names the
    reserve; the tail reads the existing unfunded/floor semantics.
T5  stricter-of: an absolute TFB_OPP_CASH_FLOOR_SAR above the pct floor
    wins and is disclosed.
T6  NAV basis: portfolio_value_sar absent => holdings value_sar sum; no
    holdings at all => NAV = cash (disclosed nav_sar).
T7  idempotence.
T8  wiring: one reserve site, one finalize site, one alert, one meta key.

Evidence run (dual-tree replay on the real 2026-09-20 export with the live
cash 24,763.73 and holdings 68,957 => floor 9,372 = the PF page's
cash_floor, x3 identical digest) is on the commit sheet.
"""
import copy
import hashlib
import importlib
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
ob = importlib.import_module("core.analysis.opportunity_builder")

FX = {"USD": 3.7576, "SAR": 1.0}
CRIT = ob.make_criteria({"period_months": 3, "required_roi_pct": 12,
                         "required_ann_roi_pct": 10, "min_reliability": 70,
                         "min_dq": 80, "min_rr": 2, "max_per_sector": 2,
                         "max_per_market": 10, "max_selected": 4,
                         "include_portfolio_holdings": False})
ENVS = ("TFB_OPP_CASH_FLOOR_PCT", "TFB_OPP_CASH_FLOOR_MODE",
        "TFB_OPP_CASH_FLOOR_SAR", "TFB_T10_PRICE_XCHECK")


def _row(symbol, price, target, sector="Energy", rel=76.5, dq=100.0):
    return {"Symbol": symbol, "Name": symbol + " Inc.", "Sector": sector,
            "Market": "NYSE/NASDAQ", "Currency": "USD",
            "Current Price": price, "Target Price": target,
            "Expected ROI 12M": round((target / price - 1.0) * 100.0, 2),
            "Forecast Reliability Score": rel, "Data Quality Score": dq,
            "Risk Bucket": "Low", "Investability Status": "INVESTABLE",
            "Final Action": "INVEST", "Recommendation": "BUY",
            "Volatility 30D": 2.0, "Forecast Source": "provider_target",
            "Last Updated (UTC)": "2026-09-20T05:25:00+00:00"}


ROWS = [_row("CRC.US", 54.07, 72.72, "Energy"),
        _row("KRP.US", 14.62, 19.20, "Energy"),
        _row("MRP.US", 28.87, 37.15, "Real Estate"),
        _row("PINE.US", 17.73, 22.60, "Real Estate"),
        _row("GBCI.US", 44.67, 56.80, "Financials"),
        _row("RDN.US", 35.01, 43.80, "Financials")]

PF = {"cash_available_sar": 30000.0, "portfolio_value_sar": 60000.0}


def _env(pct=None, mode=None, abs_sar=None):
    for k in ENVS:
        os.environ.pop(k, None)
    if pct is not None:
        os.environ["TFB_OPP_CASH_FLOOR_PCT"] = str(pct)
    if mode is not None:
        os.environ["TFB_OPP_CASH_FLOOR_MODE"] = mode
    if abs_sar is not None:
        os.environ["TFB_OPP_CASH_FLOOR_SAR"] = str(abs_sar)


def _build(pct=None, mode=None, abs_sar=None, pf=None):
    _env(pct, mode, abs_sar)
    try:
        p = ob.build_opportunity_payload(
            [copy.deepcopy(r) for r in ROWS], criteria=dict(CRIT),
            portfolio=dict(pf if pf is not None else PF), fx_rates=dict(FX))
    finally:
        _env(None)
    p = json.loads(json.dumps(p, default=str, sort_keys=True))
    (p.get("meta") or {}).pop("generated_at_utc", None)
    return p


def _digest(p):
    return hashlib.sha256(json.dumps(p, sort_keys=True).encode()).hexdigest()


def _alert(p):
    a = [x for x in p["alerts"] if x["type"] == "cash_floor"]
    return a[0] if a else None


# ---- T1 env readers ------------------------------------------------------------
def test_t1_env():
    _env(None)
    assert ob._env_cash_floor_pct() is None
    for bad in ("", "  ", "abc", "0", "100", "-5", "150"):
        os.environ["TFB_OPP_CASH_FLOOR_PCT"] = bad
        assert ob._env_cash_floor_pct() is None, bad
    os.environ["TFB_OPP_CASH_FLOOR_PCT"] = "10"
    assert ob._env_cash_floor_pct() == 10.0
    os.environ["TFB_OPP_CASH_FLOOR_PCT"] = "12.5%"
    assert ob._env_cash_floor_pct() == 12.5
    os.environ.pop("TFB_OPP_CASH_FLOOR_MODE", None)
    assert ob._env_cash_floor_mode() == "observe"
    os.environ["TFB_OPP_CASH_FLOOR_MODE"] = "ENFORCE"
    assert ob._env_cash_floor_mode() == "enforce"
    os.environ["TFB_OPP_CASH_FLOOR_MODE"] = "yes"
    assert ob._env_cash_floor_mode() == "observe"
    _env(None)


# ---- T2 off identity -------------------------------------------------------------
def test_t2_off_identity():
    a, b = _build(), _build(pct="")
    assert a == b
    assert _alert(a) is None
    assert "cash_floor" not in (a.get("meta") or {})
    assert a["kpis"]["deployable_sar"] == 30000
    assert len(a["selected"]) >= 2


# ---- T3 observe -----------------------------------------------------------------
def test_t3_observe():
    off, obs = _build(), _build(pct="10")
    assert [t["symbol"] for t in off["selected"]] == [t["symbol"] for t in obs["selected"]]
    assert [t["suggested_sar"] for t in off["selected"]] == [t["suggested_sar"] for t in obs["selected"]]
    assert [t["detail"]["funds_from"] for t in off["selected"]] == [t["detail"]["funds_from"] for t in obs["selected"]]
    assert off["kpis"] == obs["kpis"]
    assert off["near_miss"] == obs["near_miss"]
    al = _alert(obs)
    assert al is not None
    m = obs["meta"]["cash_floor"]
    assert m["mode"] == "observe" and m["pct"] == 10.0
    assert m["nav_sar"] == 90000 and m["floor_sar"] == 9000.0
    assert m["deployable_pre_sar"] == 30000 and m["deployable_post_sar"] == 21000
    # would-lose-funding math, recomputed here from the off tickets
    cum, seats, short = 0.0, 0, 0.0
    for t in off["selected"]:
        s = float(t["suggested_sar"]); cum += s
        if s > 0 and cum > 21000.5:
            seats += 1; short += min(s, cum - 21000)
    assert m["would_unfund_seats"] == seats and m["would_unfund_sar"] == round(short, 0)
    assert al["count"] == seats
    assert "Cash floor 10% of NAV 90,000 SAR = 9,000 SAR (observe)" in al["required_action"]
    assert "30,000 SAR -> 21,000 SAR" in al["required_action"]
    assert al["required_action"].endswith("No ticket changed.")


# ---- T4 enforce -------------------------------------------------------------------
def test_t4_enforce():
    off, enf = _build(), _build(pct="10", mode="enforce")
    assert enf["kpis"]["deployable_sar"] == 21000
    assert enf["kpis"]["deployable_current_sar"] == 30000    # raw split untouched
    tot = sum(float(t["suggested_sar"]) for t in enf["selected"])
    assert tot <= 21000.5
    assert tot <= sum(float(t["suggested_sar"]) for t in off["selected"])
    assert enf["kpis"]["capital_unallocated_sar"] == round(21000 - tot, 0)
    for t in enf["selected"]:
        ff = t["detail"]["funds_from"]
        if ff.startswith("Cash "):
            amt = float(ff.split("Cash ")[1].split(" SAR")[0].replace(",", ""))
            assert amt <= 21000.5
    m = enf["meta"]["cash_floor"]
    assert m["mode"] == "enforce" and m["floor_sar"] == 9000.0
    al = _alert(enf)
    assert al["count"] == 1 and "ENFORCED: deployable 21,000 SAR" in al["required_action"]
    # every selected symbol under enforce was also selectable under off
    assert set(t["symbol"] for t in enf["selected"]) <= set(
        [t["symbol"] for t in off["selected"]] +
        [r["symbol"] for r in off["candidates_rows"] if r["verdict"] == "INVEST"])


# ---- T5 stricter-of --------------------------------------------------------------
def test_t5_stricter_of():
    enf = _build(pct="10", mode="enforce", abs_sar=12000)
    m = enf["meta"]["cash_floor"]
    assert m["abs_floor_sar"] == 12000.0 and m["pct_floor_sar"] == 9000.0
    assert m["floor_sar"] == 12000.0
    assert enf["kpis"]["deployable_sar"] == 18000
    assert "is the stricter" in _alert(enf)["required_action"]
    lo = _build(pct="10", mode="enforce", abs_sar=5000)
    assert lo["meta"]["cash_floor"]["floor_sar"] == 9000.0
    assert lo["kpis"]["deployable_sar"] == 21000


# ---- T6 NAV basis ----------------------------------------------------------------
def test_t6_nav_basis():
    pf = {"cash_available_sar": 30000.0,
          "holdings": [{"symbol": "YUM", "sector": "Consumer", "market": "NYSE/NASDAQ", "value_sar": 12444},
                       {"symbol": "SBAC", "sector": "Real Estate", "market": "NYSE/NASDAQ", "value_sar": 14093}]}
    obs = _build(pct="10", pf=pf)
    m = obs["meta"]["cash_floor"]
    assert m["nav_sar"] == 56537 and m["floor_sar"] == round(56537 * 0.10, 2)
    bare = _build(pct="10", pf={"cash_available_sar": 30000.0})
    mb = bare["meta"]["cash_floor"]
    assert mb["nav_sar"] == 30000 and mb["floor_sar"] == 3000.0
    assert "NAV 30,000 SAR" in _alert(bare)["required_action"]


# ---- T7 idempotence ----------------------------------------------------------------
def test_t7_idempotent():
    assert _digest(_build(pct="10")) == _digest(_build(pct="10"))
    assert _digest(_build(pct="10", mode="enforce")) == _digest(_build(pct="10", mode="enforce"))
    assert _digest(_build()) == _digest(_build(pct=""))


# ---- T8 wiring --------------------------------------------------------------------
def test_t8_wiring():
    import inspect
    src = inspect.getsource(ob)
    assert src.count("_cf = _cash_floor_pct_ctx(pf, deployable, _floor)") == 1
    assert src.count("remaining = max(0.0, remaining - _res)") == 1
    assert src.count("_cash_floor_finalize(_cf, picked)") == 1
    assert src.count('"type": "cash_floor"') == 1
    assert src.count('meta["cash_floor"] = dict(_LAST_CASH_FLOOR)') == 1
    assert src.index("_cf = _cash_floor_pct_ctx(") < src.index("if _floor > 0:  # v1.16.0")
    assert ob.OPPORTUNITY_BUILDER_VERSION == "1.22.0"


if __name__ == "__main__":
    import traceback
    fails = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print("PASS", name)
            except Exception:
                fails += 1
                print("FAIL", name)
                traceback.print_exc()
    print("RESULT", "FAIL %d" % fails if fails else "ALL PASS")
    sys.exit(1 if fails else 0)
