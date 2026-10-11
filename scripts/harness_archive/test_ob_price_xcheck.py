"""tests/test_ob_price_xcheck.py — opportunity_builder v1.21.0 [PRICE-XCHECK].

Runs the REAL core.analysis.opportunity_builder end-to-end (no stand-ins):
the only substituted leg is the network fetch, injected through the
module's own _XCHECK_FETCH_OVERRIDE seam with recorded EODHD /real-time
payload shapes that still pass through the real _xcheck_parse_quote.

T1  helpers: env readers, symbol mapping (canonical + provider alias),
    quote parser (NA / negative / junk / epoch -> iso).
T2  off  == byte-identical payload (gate unset) vs an explicit "off".
T3  observe: selection, deferrals and KPIs identical to off; every ticket
    carries the "[price-xcheck observe]" note + detail.price_xcheck; ONE
    countable price_xcheck alert; meta.price_xcheck present only when armed.
T4  enforce (STRICT=0): the divergent seat is deferred with a countable
    "PRICE_XCHECK DIVERGE" reason, classified as a "Price Verification"
    near-miss, and the seat passes to the next candidate; single-source
    seats stay funded (fail-open).
T5  enforce (STRICT=1): single-source seats defer too (fail-closed).
T6  fail-open: a fetcher that raises for every symbol never raises out of
    the builder; observe selection stays identical to off; budget caps
    (MAX_FETCH, BUDGET_S) produce "budget" verdicts, never divergences.
T7  idempotence: two observe builds are identical (state reset per build).
T8  wiring: the gate sits before _size_one, the note before the ticket dict,
    the alert before the audit sort — one site each.

Evidence run (dual-tree replay on the real 2026-09-20 export, x3 identical
digest) is on the commit sheet.
"""
import copy
import hashlib
import importlib
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
ob = importlib.import_module("core.analysis.opportunity_builder")

FX = {"USD": 3.7576, "SAR": 1.0, "GBX": 0.0502}
CRIT = ob.make_criteria({"period_months": 3, "required_roi_pct": 12,
                         "required_ann_roi_pct": 10, "min_reliability": 70,
                         "min_dq": 80, "min_rr": 2, "max_per_sector": 2,
                         "max_per_market": 10, "max_selected": 4,
                         "include_portfolio_holdings": False})

_XC_ENVS = ("TFB_T10_PRICE_XCHECK", "TFB_T10_PRICE_XCHECK_TOL_PCT",
            "TFB_T10_PRICE_XCHECK_STRICT", "TFB_T10_PRICE_XCHECK_MAX_FETCH",
            "TFB_T10_PRICE_XCHECK_TIMEOUT_S", "TFB_T10_PRICE_XCHECK_BUDGET_S")


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


# Six INVEST-grade rows across three sectors; max_selected=4 so the enforce
# deferral has a visible successor.
ROWS = [_row("CRC.US", 54.07, 72.72, "Energy"),
        _row("KRP.US", 14.62, 19.20, "Energy"),
        _row("MRP.US", 28.87, 37.15, "Real Estate"),
        _row("PINE.US", 17.73, 22.60, "Real Estate"),
        _row("GBCI.US", 44.67, 56.80, "Financials"),
        _row("RDN.US", 35.01, 43.80, "Financials")]

# Recorded EODHD /real-time payload shapes (parsed by the REAL parser).
_PAYLOAD = {
    "CRC.US":  {"code": "CRC.US", "timestamp": 1758297600, "close": 54.10,
                "previousClose": 54.07, "change": 0.03, "change_p": 0.06},
    "KRP.US":  {"code": "KRP.US", "timestamp": 1758297600, "close": 14.28,
                "previousClose": 14.62, "change": -0.34, "change_p": -2.33},
    "MRP.US":  {"code": "MRP.US", "timestamp": "NA", "close": "NA",
                "previousClose": "NA"},
    "PINE.US": {"code": "PINE.US", "timestamp": 1758297600, "close": 17.78,
                "previousClose": 17.73},
    "GBCI.US": {"code": "GBCI.US", "timestamp": 1758297600, "close": 44.60,
                "previousClose": 44.67},
    "RDN.US":  {"code": "RDN.US", "timestamp": 1758297600, "close": 35.00,
                "previousClose": 35.01},
}


def _fetch_recorded(symbol, timeout_s):
    assert timeout_s > 0
    return ob._xcheck_parse_quote(_PAYLOAD.get(symbol))


def _fetch_raises(symbol, timeout_s):
    raise RuntimeError("provider down: " + symbol)


def _env(mode=None, **kw):
    for k in _XC_ENVS:
        os.environ.pop(k, None)
    if mode is not None:
        os.environ["TFB_T10_PRICE_XCHECK"] = mode
    for k, v in kw.items():
        os.environ[k] = str(v)


def _build(mode=None, fetch=_fetch_recorded, **kw):
    _env(mode, **kw)
    ob._XCHECK_FETCH_OVERRIDE = fetch
    try:
        p = ob.build_opportunity_payload(
            [copy.deepcopy(r) for r in ROWS], criteria=dict(CRIT),
            portfolio={"cash_available_sar": 60000.0}, fx_rates=dict(FX))
    finally:
        ob._XCHECK_FETCH_OVERRIDE = None
        _env(None)
    p = json.loads(json.dumps(p, default=str, sort_keys=True))
    (p.get("meta") or {}).pop("generated_at_utc", None)  # the one clock field
    return p


def _digest(p):
    return hashlib.sha256(json.dumps(p, sort_keys=True).encode()).hexdigest()


def _sel(p):
    return [t["symbol"] for t in p["selected"]]


# ---- T1 helpers ----------------------------------------------------------------
def test_t1_helpers():
    _env(None)
    assert ob._env_xcheck_mode() == "off"
    os.environ["TFB_T10_PRICE_XCHECK"] = "ENFORCE"
    assert ob._env_xcheck_mode() == "enforce"
    os.environ["TFB_T10_PRICE_XCHECK"] = "banana"
    assert ob._env_xcheck_mode() == "off"
    os.environ["TFB_T10_PRICE_XCHECK_TOL_PCT"] = "-3"
    assert ob._env_xcheck_tol_pct() == 1.0
    os.environ["TFB_T10_PRICE_XCHECK_MAX_FETCH"] = "x"
    assert ob._env_xcheck_max_fetch() == 15
    _env(None)
    assert ob._xcheck_eodhd_symbol("PHP.L") == "PHP.LSE"
    assert ob._xcheck_eodhd_symbol("2286.SR") == "2286.SR"
    assert ob._xcheck_eodhd_symbol("DDI.US") == "DDI.US"
    assert ob._xcheck_eodhd_symbol("GBPNZD=X") == "GBPNZD=X"
    assert ob._xcheck_eodhd_symbol("HG=F") == "HG=F"
    assert ob._xcheck_eodhd_symbol("") == ""
    assert ob._xcheck_parse_quote(_PAYLOAD["MRP.US"]) is None
    assert ob._xcheck_parse_quote({"close": -1}) is None
    assert ob._xcheck_parse_quote("junk") is None
    px, ts, pc = ob._xcheck_parse_quote(_PAYLOAD["CRC.US"])
    assert px == 54.10 and pc == 54.07 and ts.startswith("2025-09-19T")
    assert ob._xcheck_should_defer({"verdict": "diverge"}, False)
    assert not ob._xcheck_should_defer({"verdict": "single_source"}, False)
    assert ob._xcheck_should_defer({"verdict": "single_source"}, True)
    assert not ob._xcheck_should_defer({"verdict": "verified"}, True)
    assert not ob._xcheck_should_defer(None, True)


# ---- T2 off byte-identical -----------------------------------------------------
def test_t2_off_identity():
    a, b = _build(None), _build("off")
    assert a == b
    assert "price_xcheck" not in (a.get("meta") or {})
    assert not any(x["type"] == "price_xcheck" for x in a["alerts"])
    assert all("[price-xcheck" not in t["advisor_note"] for t in a["selected"])
    assert all("price_xcheck" not in t["detail"] for t in a["selected"])
    assert len(_sel(a)) == 4


# ---- T3 observe ------------------------------------------------------------------
def test_t3_observe():
    off, obs = _build(None), _build("observe")
    assert _sel(off) == _sel(obs)
    assert off["kpis"] == obs["kpis"]
    rows_o = {r["symbol"]: r for r in off["candidates_rows"]}
    rows_b = {r["symbol"]: r for r in obs["candidates_rows"]}
    for s in rows_o:
        assert rows_o[s]["deferral"] == rows_b[s]["deferral"]
        assert rows_o[s]["selected"] == rows_b[s]["selected"]
        assert rows_o[s]["gates"] == rows_b[s]["gates"]
    notes = {t["symbol"]: t["advisor_note"] for t in obs["selected"]}
    assert all("[price-xcheck observe]" in n for n in notes.values())
    assert "verified: eodhd 54.10 vs sheet 54.07" in notes["CRC.US"]
    assert "DIVERGE" in notes["KRP.US"]
    assert "single-source" in notes["MRP.US"]
    det = {t["symbol"]: t["detail"]["price_xcheck"] for t in obs["selected"]}
    assert det["CRC.US"]["verdict"] == "verified"
    assert det["KRP.US"]["verdict"] == "diverge" and det["KRP.US"]["delta_pct"] == -2.33
    assert det["MRP.US"]["verdict"] == "single_source"
    al = [x for x in obs["alerts"] if x["type"] == "price_xcheck"]
    assert len(al) == 1 and al[0]["count"] == 4
    assert "verified 2, diverge 1, single-source 1, budget 0 of 4" in \
        al[0]["required_action"]
    m = obs["meta"]["price_xcheck"]
    assert m["mode"] == "observe" and m["fetched"] == 4 and m["deferred"] == 0


# ---- T4 enforce, STRICT=0 --------------------------------------------------------
def test_t4_enforce_fail_open():
    off, enf = _build(None), _build("enforce")
    assert "KRP.US" in _sel(off)
    assert "KRP.US" not in _sel(enf)
    assert "MRP.US" in _sel(enf)             # single-source stays funded
    assert len(_sel(enf)) == 4               # the seat passed to the next name
    assert "GBCI.US" in _sel(enf)
    rows = {r["symbol"]: r for r in enf["candidates_rows"]}
    assert rows["KRP.US"]["deferral"].startswith("PRICE_XCHECK DIVERGE")
    assert rows["KRP.US"]["deferral"].endswith("sizing deferred")
    nm = {r["symbol"]: r for r in enf["near_miss"]}
    assert nm["KRP.US"]["failed_gate"] == "Price Verification"
    m = enf["meta"]["price_xcheck"]
    assert m["deferred"] == 1 and m["diverge"] == 1 and m["fetched"] == 5
    al = [x for x in enf["alerts"] if x["type"] == "price_xcheck"][0]
    assert "Enforce: 1 seat(s) deferred." in al["required_action"]


# ---- T5 enforce, STRICT=1 --------------------------------------------------------
def test_t5_enforce_strict():
    enf = _build("enforce", TFB_T10_PRICE_XCHECK_STRICT="1")
    assert "KRP.US" not in _sel(enf) and "MRP.US" not in _sel(enf)
    rows = {r["symbol"]: r for r in enf["candidates_rows"]}
    assert rows["MRP.US"]["deferral"].startswith("PRICE_XCHECK single-source")
    assert enf["meta"]["price_xcheck"]["deferred"] == 2


# ---- T6 fail-open + budget -------------------------------------------------------
def test_t6_fail_open_and_budget():
    off = _build(None)
    obs = _build("observe", fetch=_fetch_raises)
    assert _sel(obs) == _sel(off)
    m = obs["meta"]["price_xcheck"]
    assert m["single_source"] == 4 and m["diverge"] == 0 and m["fetched"] == 4
    enf = _build("enforce", fetch=_fetch_raises)
    assert _sel(enf) == _sel(off)            # STRICT=0: outage never blanks
    cap = _build("observe", TFB_T10_PRICE_XCHECK_MAX_FETCH="2")
    m = cap["meta"]["price_xcheck"]
    assert m["fetched"] == 2 and m["budget"] == 2
    det = {t["symbol"]: t["detail"]["price_xcheck"]["verdict"]
           for t in cap["selected"]}
    assert list(det.values()).count("budget") == 2
    assert _sel(cap) == _sel(off)
    tb = _build("observe", TFB_T10_PRICE_XCHECK_BUDGET_S="0.0000001")
    m = tb["meta"]["price_xcheck"]
    assert m["fetched"] >= 1 and m["fetched"] + m["budget"] == 4
    assert m["diverge"] <= 1


# ---- T7 idempotence --------------------------------------------------------------
def test_t7_idempotent():
    assert _digest(_build("observe")) == _digest(_build("observe"))
    assert _digest(_build("enforce")) == _digest(_build("enforce"))
    assert _digest(_build(None)) == _digest(_build("off"))


# ---- T8 wiring ---------------------------------------------------------------------
def test_t8_wiring():
    import inspect
    src = inspect.getsource(ob)
    assert src.count("_price_xcheck(cand, _xc_ctx)") == 1
    assert src.index("_price_xcheck(cand, _xc_ctx)") < src.index(
        "suggested, shares = _size_one(cand, criteria, budget_base, remaining)")
    assert src.count('" [price-xcheck "') == 1
    assert src.count('"type": "price_xcheck"') == 1
    assert src.count('gate, cur, req = "Price Verification"') == 1
    # v1.22.0 note: the xcheck seam ships from 1.21.0 onward; assert a floor,
    # not an exact pin, so later same-file builds keep this battery green.
    assert tuple(int(x) for x in ob.OPPORTUNITY_BUILDER_VERSION.split(".")) \
        >= (1, 21, 0)


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
