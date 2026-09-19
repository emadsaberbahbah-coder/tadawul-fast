"""F-1b (opportunity_builder v1.20.0) — cockpit qualification on the plan-3M basis.

Repo-runnable battery T1-T7 over the REAL module (no stand-ins), synthetic
candidate rows shaped like the 2026-09-19 board (CRC.US-class: price 54.07,
target +34.5% -> TP1 plan +17.2%; a low-upside row: +8%; a no-target row).
Properties asserted at HEAD:
  * legacy (env unset / unknown value) -> gates, verdicts, selection, kpis
    and notes byte-identical to v1.19.6 shapes (no tag, no "(plan3m)" text);
  * observe -> gates/verdicts/selection identical, ONE "[f1b-observe]" tag
    per audit row on failure_reason with FLIP / FLIP(strict) semantics and a
    "gain ann-basis X vs plan payoff Y" disclosure on every ticket note;
  * plan3m -> ROI gate judges the TP1 plan ROI against the translated floor
    (12%/yr x 3/12 = 3.0), Annualized ROI judges the compounded plan ROI,
    "(plan3m)" is stamped on both required strings, the ticket gain equals
    suggested x plan ROI / 100 (the P-141 correction), and
    TFB_T10_REQ_ROI_3M_PCT=12 gives the strict reading;
  * no TP1 ladder -> the legacy gates evaluate (never invents a plan).
Dual-tree proof vs pristine v1.19.6 on the full 9,791-row 2026-09-19 export
(frozen clock, H1-H5 x3, digest d82defae1179a6e9) is on the commit sheet.

Run:  python tests/test_ob_f1b_plan_basis.py
      (or pytest -q tests/test_ob_f1b_plan_basis.py)
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
                         "max_selected": 10, "include_portfolio_holdings": False})


def _row(symbol, price, target, rel=76.5, dq=100.0):
    return {"Symbol": symbol, "Name": symbol + " Inc.", "Sector": "Energy",
            "Market": "NYSE/NASDAQ", "Currency": "USD",
            "Current Price": price, "Target Price": target,
            "Expected ROI 12M": (round((target / price - 1.0) * 100.0, 2)
                                 if target else None),
            "Forecast Reliability Score": rel, "Data Quality Score": dq,
            "Risk Bucket": "Low", "Investability Status": "INVESTABLE",
            "Final Action": "INVEST", "Recommendation": "BUY",
            "Volatility 30D": 2.0, "Forecast Source": "provider_target",
            "Last Updated (UTC)": "2026-09-19T05:07:00+00:00"}


ROW_CRC = _row("CRC.US", 54.07, 72.72)      # +34.5% valuation -> plan +17.2%
ROW_LOW = _row("LOW.US", 100.0, 108.0)      # +8% valuation  -> plan +4.0%
ROW_NOTGT = _row("NOTGT.US", 20.0, None)    # no ladder -> legacy gates


def _env(mode, strict=None):
    if mode is None:
        os.environ.pop("TFB_FORECAST_BASIS", None)
    else:
        os.environ["TFB_FORECAST_BASIS"] = mode
    if strict is None:
        os.environ.pop("TFB_T10_REQ_ROI_3M_PCT", None)
    else:
        os.environ["TFB_T10_REQ_ROI_3M_PCT"] = str(strict)


def _cand(row):
    return ob.normalize_candidate(copy.deepcopy(row), FX, CRIT)


def _gates(row, mode, strict=None):
    _env(mode, strict)
    try:
        g = ob.evaluate_gates(_cand(row), CRIT, set())
    finally:
        _env(None)
    return {x["gate"]: x for x in g}


# ---- T1 helpers ----------------------------------------------------------------
def test_t1_helpers():
    for raw, want in ((None, "legacy"), ("observe", "observe"),
                      ("plan3m", "plan3m"), ("PLAN3M", "plan3m"),
                      ("garbage", "legacy"), ("1", "legacy")):
        _env(raw)
        assert ob._f1b_basis() == want, (raw, ob._f1b_basis())
    _env(None)
    assert ob._f1b_required_roi_3m(CRIT) == 3.0          # 12 x 3/12
    assert ob._f1b_required_roi_3m({"period_months": 6,
                                    "required_roi_pct": 12}) == 6.0
    _env(None, strict=12)
    assert ob._f1b_required_roi_3m(CRIT) == 12.0
    _env(None)
    assert ob.OPPORTUNITY_BUILDER_VERSION == "1.20.0"


# ---- T2 plan eval --------------------------------------------------------------
def test_t2_plan_eval():
    ev = ob._f1b_plan_eval(_cand(ROW_CRC), CRIT)
    assert ev is not None and abs(ev["plan3m"] - 17.2) < 0.11   # (tp1-price)/price
    assert ev["req3m"] == 3.0 and ev["strict"] == 12.0
    assert ev["roi_ok"] and ev["strict_ok"] and ev["ann_ok"]
    assert abs(ev["ann"] - ob._ann_from_plan_roi(ev["plan3m"], CRIT)) < 1e-9
    lo = ob._f1b_plan_eval(_cand(ROW_LOW), CRIT)
    assert lo["roi_ok"] and not lo["strict_ok"]                  # 4.0 >= 3, < 12
    assert ob._f1b_plan_eval(_cand(ROW_NOTGT), CRIT) is None      # no ladder
    assert ob._f1b_plan_eval({}, CRIT) is None


# ---- T3 gates: legacy vs plan3m vs strict ---------------------------------------
def test_t3_gates():
    lg = _gates(ROW_CRC, None)
    assert lg["ROI"]["required"] == ">= 12%" and lg["ROI"]["passed"]
    assert "(plan3m)" not in lg["ROI"]["required"]
    assert _gates(ROW_CRC, "observe")["ROI"] == lg["ROI"]        # observe = legacy
    p = _gates(ROW_CRC, "plan3m")
    assert p["ROI"]["required"] == ">= 3% (plan3m)" and p["ROI"]["passed"]
    assert abs(p["ROI"]["current"] - 17.2) < 0.11
    assert p["Annualized ROI"]["required"] == ">= 10% (plan3m)"
    assert p["Annualized ROI"]["passed"]
    # low-upside row: legacy fails ROI (8 < 12); translated passes (4 >= 3);
    # strict fails (4 < 12)
    assert not _gates(ROW_LOW, None)["ROI"]["passed"]
    assert _gates(ROW_LOW, "plan3m")["ROI"]["passed"]
    assert not _gates(ROW_LOW, "plan3m", strict=12)["ROI"]["passed"]
    # no ladder -> plan3m falls back to the legacy gate strings
    n = _gates(ROW_NOTGT, "plan3m")
    assert n["ROI"]["required"] == ">= 12%" and not n["ROI"]["passed"]
    # every other gate identical across modes
    for k in lg:
        if k not in ("ROI", "Annualized ROI"):
            assert lg[k] == p[k], k


# ---- T4 observe post-pass through the real entry point --------------------------
def _build(mode, strict=None):
    _env(mode, strict)
    try:
        p = ob.build_opportunity_payload(
            [copy.deepcopy(r) for r in (ROW_CRC, ROW_LOW, ROW_NOTGT)],
            criteria=dict(CRIT), portfolio={"cash_available_sar": 30000.0},
            fx_rates=dict(FX))
    finally:
        _env(None)
    return json.loads(json.dumps(p, default=str, sort_keys=True))


def test_t4_observe_post_pass():
    lg, ob_ = _build(None), _build("observe")
    assert lg["status"] == "ok" and ob_["status"] == "ok"
    rows_l = {r["symbol"]: r for r in lg["candidates_rows"]}
    rows_o = {r["symbol"]: r for r in ob_["candidates_rows"]}
    assert set(rows_l) == set(rows_o) == {"CRC.US", "LOW.US", "NOTGT.US"}
    for s in rows_l:
        assert rows_l[s]["gates"] == rows_o[s]["gates"]
        assert rows_l[s]["verdict"] == rows_o[s]["verdict"]
        assert rows_l[s]["selected"] == rows_o[s]["selected"]
        assert rows_l[s]["deferral"] == rows_o[s]["deferral"]
        assert "[f1b-observe]" not in (rows_l[s]["failure_reason"] or "")
        assert "[f1b-observe]" in rows_o[s]["failure_reason"]
    assert rows_o["CRC.US"]["failure_reason"].endswith(
        "[f1b-observe] plan3m 17.2% vs 3%/3M")                   # no FLIP
    low = rows_o["LOW.US"]["failure_reason"]
    assert low.startswith("ROI: ") and " - FLIP" in low \
        and "FLIP(strict)" not in low                            # translated flips only
    assert rows_o["NOTGT.US"]["failure_reason"].endswith(
        "[f1b-observe] plan3m DATA_GAP")
    assert lg["kpis"] == ob_["kpis"]
    assert [t["symbol"] for t in lg["selected"]] == \
        [t["symbol"] for t in ob_["selected"]]


# ---- T5 ticket gain: observe disclosure vs plan3m payoff ------------------------
def test_t5_ticket_gain():
    lg, ob_, pl = _build(None), _build("observe"), _build("plan3m")
    assert lg["selected"] and lg["selected"][0]["symbol"] == "CRC.US"
    t_l, t_o, t_p = lg["selected"][0], ob_["selected"][0], pl["selected"][0]
    assert "[f1b-observe] gain ann-basis" not in t_l["advisor_note"]
    assert "[f1b-observe] gain ann-basis" in t_o["advisor_note"]
    assert t_o["exp_gain_12m_sar"] == t_l["exp_gain_12m_sar"]      # observe = legacy
    # legacy: gain == suggested x ann/100 (the v1.0.23 identity)
    assert abs(t_l["exp_gain_12m_sar"]
               - round(t_l["suggested_sar"] * t_l["ann_roi_pct"] / 100.0, 0)) <= 1
    # plan3m: gain == suggested x plan ROI/100 (P-141 correction), ann unchanged
    assert abs(t_p["exp_gain_12m_sar"]
               - round(t_p["suggested_sar"] * t_p["roi_pct"] / 100.0, 0)) <= 1
    assert t_p["ann_roi_pct"] == t_l["ann_roi_pct"]
    assert t_p["exp_gain_12m_sar"] < t_l["exp_gain_12m_sar"]
    assert pl["kpis"]["expected_gain_12m_sar"] == sum(
        t["exp_gain_12m_sar"] for t in pl["selected"])


# ---- T6 strict reading through the entry point ---------------------------------
def test_t6_strict():
    st = _build("plan3m", strict=12)
    rows = {r["symbol"]: r for r in st["candidates_rows"]}
    g = {x["gate"]: x for x in rows["LOW.US"]["gates"]}
    assert g["ROI"]["required"] == ">= 12% (plan3m)" and not g["ROI"]["passed"]
    g2 = {x["gate"]: x for x in rows["CRC.US"]["gates"]}
    assert g2["ROI"]["passed"]                                    # 17.2 >= 12


# ---- T7 wiring present in source -------------------------------------------------
def test_t7_wiring():
    import inspect
    src = inspect.getsource(ob)
    assert src.count("_f1b_plan_eval(") >= 3      # def + gate site + observe pass
    assert src.count("_f1b_basis()") >= 3         # gate, observe pass, ticket
    assert src.count('[f1b-observe] gain ann-basis') == 1


TESTS = [test_t1_helpers, test_t2_plan_eval, test_t3_gates,
         test_t4_observe_post_pass, test_t5_ticket_gain, test_t6_strict,
         test_t7_wiring]

if __name__ == "__main__":
    for fn in TESTS:
        fn()
        print("PASS", fn.__name__)
    d = hashlib.sha256(json.dumps({
        "obs": _build("observe")["candidates_rows"][0]["failure_reason"],
        "plan": _build("plan3m")["kpis"]["expected_gain_12m_sar"],
        "legacy": _build(None)["kpis"]["expected_gain_12m_sar"]},
        sort_keys=True).encode()).hexdigest()[:16]
    print("ALL PASS", len(TESTS), "digest", d)
