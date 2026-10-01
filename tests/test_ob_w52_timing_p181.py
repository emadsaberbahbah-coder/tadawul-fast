#!/usr/bin/env python3
"""tests/test_ob_w52_timing_p181.py — opportunity_builder v1.23.0
[P-181 TIMING GATE: two-sided 52W window + one-session shock veto].

REAL module end-to-end (build_opportunity_payload / evaluate_gates /
normalize_candidate; no stand-ins). Fixtures carry the VERBATIM 2026-10-01
export values of the seats and qualified names (price, 52W High/Low,
Percent Change, Expected ROI 12M, Target Price, reliability, DQ).

Dual-tree: set OB_BASE=<path to the v1.22.2 file> to prove the OFF tree is
byte-identical to the base payload. Set TFB_TEST_GM_TSV=<Global_Markets.tsv>
to additionally run the three modes over the REAL full page (6,609 rows).
Set OB_FILE=<path> to test a file outside the repo tree (One-Pass delivery).

W1 helpers: env readers, _w52_eval on real values (recompute == sheet field;
   fraction vs percent change; unknown passes; shock leg disabled by a
   non-negative floor)
W2 OFF: gates list, cand dict and payload identical to the base (dual-tree)
W3 observe: verdicts / seats / KPIs / near-miss identical to OFF; exactly ONE
   "[w52-observe]" tag per audit row; meta.timing_gate counters; RDN/MRP/CIE
   WOULD_FAIL low, NVDA/DDI WOULD_FAIL high, AED/VLY/FBK ok
W4 enforce: those rows -> WATCH with first_fail "Timing (52W)"; near-miss
   text starts "Timing:"; they are never selected; untouched rows keep their
   OFF verdict; seats re-fill from the next qualified names
W5 GATE_ORDER registration: "Timing (52W)" immediately before "Portfolio";
   first_failed_gate ranks it after Sector Trend
W6 env hygiene: off after enforce == OFF payload again
W7 idempotence x2 (same digest per mode)
"""
import copy, hashlib, importlib, importlib.util, json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


OB_FILE = os.environ.get("OB_FILE")
ob = _load(OB_FILE, "ob_under_test") if OB_FILE else importlib.import_module(
    "core.analysis.opportunity_builder")
assert ob.OPPORTUNITY_BUILDER_VERSION == "1.23.0", ob.OPPORTUNITY_BUILDER_VERSION

NOW = "2026-10-01T03:40:00+00:00"
from datetime import datetime as _dt, timezone as _tz


class _FrozenDT(_dt):
    """Frozen clock (2026-10-01 06:00Z): the only run-to-run variance in a
    payload is quote age (freshness/trust details), so every digest below is
    reproducible x3 and the dual-tree compares like with like."""
    @classmethod
    def now(cls, tz=None):
        fixed = _dt(2026, 10, 1, 6, 0, 0, tzinfo=_tz.utc)
        return fixed if tz is not None else fixed.replace(tzinfo=None)


ob.datetime = _FrozenDT
FX = {"USD": 3.7624, "SAR": 1.0, "EUR": 4.41, "CAD": 2.75, "MXN": 0.21}
CRIT = {"period_months": 3, "required_roi_pct": 12, "required_ann_roi_pct": 10,
        "min_reliability": 70, "min_dq": 80, "min_rr": 2, "max_per_sector": 2,
        "max_per_market": 10, "max_selected": 3, "include_portfolio_holdings": False,
        "min_ticket_sar": 1000.0, "near_miss_n": 15}
ENVS = ("TFB_T10_W52_TIMING", "TFB_T10_W52_LOW_PCT", "TFB_T10_W52_HIGH_PCT",
        "TFB_T10_SHOCK_PCT")


def _row(symbol, price, hi, lo, pos, chg, target, roi12, sector, currency="USD",
         market="NYSE/NASDAQ", rel=71.5, name=None):
    """Sheet-header-keyed row with the VERBATIM 2026-10-01 export values."""
    return {"Symbol": symbol, "Name": name or symbol, "Sector": sector,
            "Market": market, "Currency": currency, "Current Price": price,
            "52W High": hi, "52W Low": lo, "52W Position %": pos,
            "Percent Change": chg, "Target Price": target,
            "Expected ROI 12M": roi12, "Forecast Reliability Score": rel,
            "Data Quality Score": 100.0, "Risk Bucket": "Low",
            "Investability Status": "INVESTABLE", "Final Action": "INVEST",
            "Recommendation": "BUY", "Volatility 30D": 2.0,
            "Forecast Source": "provider_target", "Last Updated (UTC)": NOW}


# verbatim 2026-10-01 Global_Markets export (sync run 36793616445)
ROWS = [
    _row("RDN.US", 30.61, 41.05, 30.595, 0.143472, -0.06534351, 44.5, 0.34769,
         "Financials", name="Radian Group Inc."),
    _row("MRP.US", 25.97, 34.03, 25.92, 0.616523, -0.02036967, 37.16667, 0.346369,
         "Real Estate", name="Millrose Properties, Inc."),
    _row("NVDA.US", 228.38, 236.54, 164.27, 88.709008, 0.00514942, 327.7, 0.346632,
         "Information Technology", rel=70.4, name="NVIDIA Corporation"),
    _row("DDI.US", 13.29, 13.4, 8.1, 97.924528, 0.01761103, 18.05, 0.332995,
         "Communication Services", rel=70.4, name="DoubleDown Interactive"),
    _row("CIE.MC", 25.75, 33.0, 24.9, 10.493827, 0.00194553, 35.85778, 0.342144,
         "Consumer Discretionary", currency="EUR", market="BME Spain",
         name="CIE Automotive, S.A."),
    _row("AED.BR", 63.8, 80.05, 59.5, 20.924574, -0.00854701, 81.9, 0.283699,
         "Real Estate", currency="EUR", market="Euronext Brussels", rel=76.5,
         name="Aedifica NV/SA"),
    _row("VLY.US", 12.62, 15.2, 9.64, 53.597122, -0.00786164, 16.67857, 0.317536,
         "Financials", name="Valley National Bancorp"),
    _row("FBK.US", 51.7, 62.655, 49.24, 18.337682, -0.00366159, 66.34375, 0.283245,
         "Financials", rel=76.5, name="FB Financial Corporation"),
    _row("PINFRA.MX", 267.65, 317.99, 226.13, 45.199216, 0.01152683, 325.0, 0.214272,
         "Industrials", currency="MXN", market="BMV", rel=76.5,
         name="Promotora y Operadora de Infraestructura"),
    # unknown timing fields (no 52W, no change) -> must pass
    _row("UNK.US", 50.0, None, None, None, None, 65.0, 0.30, "Utilities",
         rel=76.5, name="Unknown Fields Co."),
]
PF = {"cash_available_sar": 34166.25,
      "holdings": [{"symbol": "DDI.US", "sector": "Communication Services",
                    "market": "NYSE/NASDAQ", "value_sar": 11451.0},
                   {"symbol": "YUM", "sector": "Consumer Discretionary",
                    "market": "NYSE/NASDAQ", "value_sar": 12311.0}]}


def _env(mode=None, low=None, high=None, shock=None):
    for k in ENVS:
        os.environ.pop(k, None)
    if mode is not None:
        os.environ["TFB_T10_W52_TIMING"] = mode
    if low is not None:
        os.environ["TFB_T10_W52_LOW_PCT"] = str(low)
    if high is not None:
        os.environ["TFB_T10_W52_HIGH_PCT"] = str(high)
    if shock is not None:
        os.environ["TFB_T10_SHOCK_PCT"] = str(shock)


def _build(mod, mode=None, rows=None, **kw):
    _env(mode, **kw)
    try:
        p = mod.build_opportunity_payload([copy.deepcopy(r) for r in (rows or ROWS)],
                                          criteria=dict(CRIT), portfolio=copy.deepcopy(PF),
                                          fx_rates=dict(FX))
    finally:
        _env(None)
    p = json.loads(json.dumps(p, default=str, sort_keys=True))
    (p.get("meta") or {}).pop("generated_at_utc", None)
    return p


def _digest(p):
    return hashlib.sha256(json.dumps(p, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _aud(p, sym):
    for r in p["candidates_rows"]:
        if r["symbol"] == sym:
            return r
    return None


def _nm(p, sym):
    return [r for r in p["near_miss"] if r["symbol"] == sym]


out = []


def T(name, cond, detail=""):
    out.append(("PASS " if cond else "FAIL ") + name + (" | " + str(detail) if detail else ""))
    if not cond:
        raise AssertionError(name + " " + str(detail))


# ---------------------------------------------------------------- W1 helpers
_env(None)
T("W1 mode default off", ob._env_w52_timing_mode() == "off")
_env("Observe"); T("W1 mode case-insensitive observe", ob._env_w52_timing_mode() == "observe")
_env("bogus"); T("W1 unknown value -> off", ob._env_w52_timing_mode() == "off")
_env(None, low=150, high=-3); T("W1 out-of-range edges -> defaults",
                                (ob._env_w52_low_pct(), ob._env_w52_high_pct()) == (15.0, 85.0))
_env(None); T("W1 defaults 15/85/-5", (ob._env_w52_low_pct(), ob._env_w52_high_pct(),
                                      ob._env_shock_pct()) == (15.0, 85.0, -5.0))
_env("observe")
c_rdn = ob.normalize_candidate(ROWS[0], FX, ob.make_criteria(CRIT))
ev = ob._w52_eval(c_rdn, 15.0, 85.0, -5.0)
T("W1 RDN recompute == sheet field (0.14) and shock -6.53",
  abs(ev["pos"] - 0.143472) < 0.01 and abs(ev["chg"] + 6.534351) < 0.01
  and ev["fail_low"] and ev["fail_shock"] and not ev["fail_high"], ev)
c_nvda = ob.normalize_candidate(ROWS[2], FX, ob.make_criteria(CRIT))
ev2 = ob._w52_eval(c_nvda, 15.0, 85.0, -5.0)
T("W1 NVDA 88.71 -> fail_high only", abs(ev2["pos"] - 88.709008) < 0.02 and ev2["fail_high"]
  and not ev2["fail_low"] and not ev2["fail_shock"], ev2)
c_unk = ob.normalize_candidate(ROWS[-1], FX, ob.make_criteria(CRIT))
ev3 = ob._w52_eval(c_unk, 15.0, 85.0, -5.0)
T("W1 unknown fields -> unknown, no fail", ev3["unknown"] and not (ev3["fail_low"] or ev3["fail_high"] or ev3["fail_shock"]), ev3)
ev4 = ob._w52_eval(dict(c_rdn, pct_change_1d=-6.534351), 15.0, 85.0, -5.0)
T("W1 percent-points change (|v|>=1.5) read as percent", abs(ev4["chg"] + 6.534351) < 1e-9, ev4["chg"])
ev5 = ob._w52_eval(c_rdn, 15.0, 85.0, 0.0)
T("W1 non-negative shock floor disables the shock leg", not ev5["fail_shock"] and ev5["fail_low"], ev5)
ev6 = ob._w52_eval(dict(c_rdn, w52_high=None), 15.0, 85.0, -5.0)
T("W1 missing 52W High -> falls back to the sheet field", abs(ev6["pos"] - 0.143472) < 1e-6, ev6["pos"])
_env(None)
c_off = ob.normalize_candidate(ROWS[0], FX, ob.make_criteria(CRIT))
T("W1 OFF cand carries no timing keys", not any(k in c_off for k in ("w52_high", "w52_low", "w52_position_pct", "pct_change_1d")))
T("W1 armed cand carries the four timing keys", all(k in c_rdn for k in ("w52_high", "w52_low", "w52_position_pct", "pct_change_1d")))

# ---------------------------------------------------------------- W2 OFF
p_off = _build(ob, None)
g_off = [g["gate"] for g in _aud(p_off, "RDN.US")["gates"]]
T("W2 OFF gate list has no Timing gate", "Timing (52W)" not in g_off, g_off[-3:])
T("W2 OFF meta has no timing_gate block", "timing_gate" not in p_off["meta"])
sel_off = [t["symbol"] for t in p_off["selected"]]
T("W2 OFF seats (3) include RDN and MRP (the 52W-low pair)", len(sel_off) == 3 and {"RDN.US", "MRP.US"} <= set(sel_off), sel_off)

base_path = os.environ.get("OB_BASE")
if base_path:
    obb = _load(base_path, "ob_base_v1222")
    assert obb.OPPORTUNITY_BUILDER_VERSION == "1.22.2", obb.OPPORTUNITY_BUILDER_VERSION
    obb.datetime = _FrozenDT
    p_base = _build(obb, None)
    p_base["version"] = p_off["version"]
    p_base["meta"]["versions"]["opportunity_builder"] = p_off["meta"]["versions"]["opportunity_builder"]
    T("W2 dual-tree: OFF payload == base payload (version string aside)", _digest(p_base) == _digest(p_off),
      (_digest(p_base), _digest(p_off)))
    # observe/enforce on the base must be inert (base has no such env)
    p_base_obs = _build(obb, "observe"); p_base_obs["version"] = p_off["version"]
    p_base_obs["meta"]["versions"]["opportunity_builder"] = p_off["meta"]["versions"]["opportunity_builder"]
    T("W2 dual-tree: base ignores the new env", _digest(p_base_obs) == _digest(p_off))
else:
    out.append("SKIP W2 dual-tree (set OB_BASE=<v1.22.2 file>)")

# ---------------------------------------------------------------- W3 observe
p_obs = _build(ob, "observe")
T("W3 observe: seats identical to OFF", [t["symbol"] for t in p_obs["selected"]] == sel_off)
T("W3 observe: kpis identical to OFF", p_obs["kpis"] == p_off["kpis"])
T("W3 observe: near-miss identical to OFF", p_obs["near_miss"] == p_off["near_miss"])
T("W3 observe: verdicts identical to OFF",
  [(r["symbol"], r["verdict"]) for r in p_obs["candidates_rows"]] ==
  [(r["symbol"], r["verdict"]) for r in p_off["candidates_rows"]])
tags = {r["symbol"]: str(r.get("failure_reason") or "") for r in p_obs["candidates_rows"]}
T("W3 observe: exactly ONE [w52-observe] tag per audit row",
  all(v.count("[w52-observe]") == 1 for v in tags.values()), tags)
T("W3 observe: RDN WOULD_FAIL low+shock", "WOULD_FAIL" in tags["RDN.US"] and "52W low" in tags["RDN.US"] and "shock" in tags["RDN.US"], tags["RDN.US"])
T("W3 observe: MRP WOULD_FAIL low only", "WOULD_FAIL" in tags["MRP.US"] and "52W low" in tags["MRP.US"] and "shock" not in tags["MRP.US"], tags["MRP.US"])
T("W3 observe: NVDA WOULD_FAIL high", "WOULD_FAIL" in tags["NVDA.US"] and "52W high" in tags["NVDA.US"], tags["NVDA.US"])
T("W3 observe: CIE.MC WOULD_FAIL low (10.5%)", "WOULD_FAIL" in tags["CIE.MC"], tags["CIE.MC"])
T("W3 observe: AED/VLY/FBK/PINFRA ok", all(tags[s].endswith("[w52-observe] ok") for s in ("AED.BR", "VLY.US", "FBK.US", "PINFRA.MX")),
  {s: tags[s] for s in ("AED.BR", "VLY.US", "FBK.US", "PINFRA.MX")})
T("W3 observe: UNK n/a tag", "n/a" in tags["UNK.US"], tags["UNK.US"])
tg = p_obs["meta"]["timing_gate"]
T("W3 observe: meta.timing_gate counters", tg["mode"] == "observe" and tg["evaluated"] == len(ROWS)
  and tg["fail_low"] == 3 and tg["fail_high"] == 2 and tg["fail_shock"] == 1 and tg["would_fail"] == 5
  and tg["unknown"] == 1, tg)
g_obs = _aud(p_obs, "RDN.US")["gates"]
T("W3 observe: Timing gate present, PASSED, before Portfolio",
  [g["gate"] for g in g_obs][-2:] == ["Timing (52W)", "Portfolio"] and g_obs[-2]["passed"] is True
  and g_obs[-2]["fail_class"] is None, [g["gate"] for g in g_obs][-3:])

# ---------------------------------------------------------------- W4 enforce
p_enf = _build(ob, "enforce")
for s in ("RDN.US", "MRP.US", "NVDA.US", "CIE.MC"):
    r = _aud(p_enf, s)
    T("W4 enforce: %s -> WATCH via Timing (52W)" % s,
      r["verdict"] == "WATCH" and (r.get("first_fail") or {}).get("gate") == "Timing (52W)",
      (r["verdict"], r.get("first_fail")))
r_ddi = _aud(p_enf, "DDI.US")
T("W4 enforce: DDI (held) first_fail is Timing, not Portfolio (Timing sorts before Portfolio)",
  (r_ddi.get("first_fail") or {}).get("gate") == "Timing (52W)" and r_ddi["verdict"] == "WATCH", r_ddi.get("first_fail"))
sel_enf = [t["symbol"] for t in p_enf["selected"]]
T("W4 enforce: no timing-failed symbol is seated", not ({"RDN.US", "MRP.US", "NVDA.US", "CIE.MC"} & set(sel_enf)), sel_enf)
T("W4 enforce: seats re-fill from the passing names", len(sel_enf) == 3 and set(sel_enf) <= {"AED.BR", "VLY.US", "FBK.US", "PINFRA.MX", "UNK.US"}, sel_enf)
for s in ("AED.BR", "VLY.US", "FBK.US", "PINFRA.MX", "UNK.US"):
    T("W4 enforce: %s verdict unchanged vs OFF" % s, _aud(p_enf, s)["verdict"] == _aud(p_off, s)["verdict"], s)
nm_rdn = _nm(p_enf, "RDN.US")
T("W4 enforce: RDN near-miss names the Timing gate with the 'Timing:' hint",
  len(nm_rdn) == 1 and nm_rdn[0]["failed_gate"] == "Timing (52W)" and str(nm_rdn[0]["improve_note"]).startswith("Timing:")
  and "15% <= 52W pos <= 85%" in str(nm_rdn[0]["required"]), nm_rdn)
T("W4 enforce: meta.timing_gate mode=enforce", p_enf["meta"]["timing_gate"]["mode"] == "enforce" and p_enf["meta"]["timing_gate"]["would_fail"] == 5)
T("W4 enforce: no [w52-observe] tags", all("[w52-observe]" not in str(r.get("failure_reason") or "") for r in p_enf["candidates_rows"]))
p_enf2 = _build(ob, "enforce", low=0, high=100, shock=-10)
T("W4 enforce with window 0-100 / shock -10: only RDN still fails? no - RDN -6.53 > -10 -> all pass",
  all(_aud(p_enf2, s)["verdict"] == _aud(p_off, s)["verdict"] for s in ("RDN.US", "MRP.US", "NVDA.US", "CIE.MC")),
  [(s, _aud(p_enf2, s)["verdict"]) for s in ("RDN.US", "MRP.US", "NVDA.US", "CIE.MC")])

# ---------------------------------------------------------------- W5 GATE_ORDER
go = list(ob.GATE_ORDER)
T("W5 GATE_ORDER: Timing (52W) immediately before Portfolio", go[-2:] == ["Timing (52W)", "Portfolio"], go[-3:])
T("W5 GATE_ORDER: Timing after Sector Trend", go.index("Sector Trend") < go.index("Timing (52W)"))
ff = ob.first_failed_gate([ob._gate("Portfolio", False, ob.FAIL_STRUCTURAL, "held", "x"),
                           ob._gate("Timing (52W)", False, ob.FAIL_NON_CRITICAL, "pos 0.1%", "y"),
                           ob._gate("Sector Trend", False, ob.FAIL_MAJOR, "Negative", "z")])
T("W5 first_failed_gate order Sector Trend < Timing < Portfolio", ff["gate"] == "Sector Trend")

# ---------------------------------------------------------------- W6 hygiene
p_off2 = _build(ob, None)
T("W6 OFF after enforce == OFF", _digest(p_off2) == _digest(p_off))

# ---------------------------------------------------------------- W7 idempotence
T("W7 idempotence observe", _digest(_build(ob, "observe")) == _digest(p_obs))
T("W7 idempotence enforce", _digest(_build(ob, "enforce")) == _digest(p_enf))

# ---------------------------------------------------------------- real page (optional)
gm_tsv = os.environ.get("TFB_TEST_GM_TSV")
if gm_tsv and os.path.exists(gm_tsv):
    import csv
    with open(gm_tsv, encoding="utf-8") as fh:
        rr = list(csv.reader(fh, delimiter="\t"))
    hdr = rr[0]
    real = []
    for r in rr[1:]:
        if not r or not r[0].strip():
            continue
        r = r + [""] * (len(hdr) - len(r))
        d = dict(zip(hdr, r))
        real.append({k: (v if v != "" else None) for k, v in d.items()})
    p_r_off = _build(ob, None, rows=real)
    p_r_obs = _build(ob, "observe", rows=real)
    p_r_enf = _build(ob, "enforce", rows=real)
    T("R1 real page: observe seats/kpis == OFF", [t["symbol"] for t in p_r_obs["selected"]] == [t["symbol"] for t in p_r_off["selected"]]
      and p_r_obs["kpis"] == p_r_off["kpis"])
    tgr = p_r_obs["meta"]["timing_gate"]
    T("R2 real page: counters populated", tgr["evaluated"] > 1000 and tgr["would_fail"] > 0, tgr)
    n_tag = sum(1 for r in p_r_obs["candidates_rows"] if "[w52-observe]" in str(r.get("failure_reason") or ""))
    T("R3 real page: every written audit row tagged", n_tag == len(p_r_obs["candidates_rows"]), (n_tag, len(p_r_obs["candidates_rows"])))
    out.append("INFO real page OFF seats=%s | enforce seats=%s | counters=%s" % (
        [t["symbol"] for t in p_r_off["selected"]], [t["symbol"] for t in p_r_enf["selected"]], tgr))
    if base_path:
        p_rb = _build(obb, None, rows=real)
        p_rb["version"] = p_r_off["version"]; p_rb["meta"]["versions"]["opportunity_builder"] = p_r_off["meta"]["versions"]["opportunity_builder"]
        T("R4 real page dual-tree: OFF == base", _digest(p_rb) == _digest(p_r_off), (_digest(p_rb), _digest(p_r_off)))
else:
    out.append("SKIP R1-R4 real page (set TFB_TEST_GM_TSV=<Global_Markets.tsv>)")

digest = hashlib.sha256("\n".join(out).encode()).hexdigest()[:16]
print("\n".join(out))
print("RESULT %d checks, digest %s" % (sum(1 for l in out if l.startswith("PASS")), digest))


def test_all():
    assert not any(l.startswith("FAIL") for l in out)
