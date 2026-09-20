"""P-115b (data_engine_v2 v5.144.0) — REL-PATH-TAG: per-row disclosure of how
forecast_reliability_score was composed. Repo-runnable battery T1-T8 over
the REAL module (no stand-ins), driving _apply_investability_gate on rows
shaped like the 2026-09-20 export holdings (DDI.US / YUM / CWBC.US with the
exact fc / dq / source / warnings combinations the cross-surface split
showed) and on adversarial rows.

Properties asserted at HEAD:
  off (unset) => forecast_reliability_score, data_quality_score, verdict and
  warnings byte-identical to v5.143.0 (no tag); observe => ONE tag per row,
  values untouched, penalty codes exactly equal to the legs that fired, raw
  and final agree with the row, calibration factor disclosed when applied;
  the tag can never contain a substring the gate itself tests on warnings —
  even for adversarial source tokens; re-running the gate on a row that
  already carries the tag (preserved-row replay) changes nothing;
  mode disclosed in surface_gate_states().
Run:  python tests/test_de_rel_path_tag_p115b.py
      (or pytest -q tests/test_de_rel_path_tag_p115b.py)
"""
import copy
import hashlib
import importlib
import inspect
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
de = importlib.import_module("core.data_engine_v2")

ENVS = ("TFB_REL_PATH_TAG", "TFB_RELIABILITY_RECALIBRATION",
        "TFB_CALIBRATION_ADJUST", "TFB_CALIBRATION_FACTORS")
FORBIDDEN = ("cap", "forecast", "target", "roi", "drop", "reject",
             "provider_target", "price_bar_stale", "xprovider_price_conflict")


def _base_row(symbol, price, fc=0.664, opp_src="", fc_src="provider_target",
              warnings="", **extra):
    px = price if price is not None else 1.0    # only for the derived fields
    r = {"symbol": symbol, "current_price": price, "price": price,
         "week_52_high": px * 1.3, "week_52_low": px * 0.7,
         "avg_volume_30d": 500000.0, "volume": 400000.0,
         "pe_ttm": 9.5, "eps_ttm": 1.2, "market_cap": 6.3e8,
         "volatility_30d": 2.1, "max_drawdown_1y": -18.0,
         "forecast_price_3m": px * 1.06, "forecast_price_12m": px * 1.20,
         "expected_roi_3m": 0.06, "expected_roi_12m": 0.20,
         "overall_score": 72.0, "forecast_confidence": fc,
         "opportunity_source": opp_src, "forecast_source": fc_src,
         "recommendation": "BUY", "warnings": warnings,
         "asset_class": "Equity", "industry": "Banks",
         "debt_to_equity": 0.37, "free_cash_flow_ttm": 4.0e7,
         "data_provider": "yahoo_chart"}
    r.update(extra)
    return r


# The 2026-09-20 cross-surface specimens (shapes, not claims about the wire):
DDI_ADV = _base_row("DDI.US", 12.75, fc=0.648)                               # advisor path
DDI_GM = _base_row("DDI.US", 12.75, fc=0.648, opp_src="both_present_fallback")  # sync path
YUM_GM = _base_row("YUM.US", 137.99, fc=0.664, fc_src="phase_ii_synthetic")
CWBC_GM = _base_row("CWBC.US", 26.05, fc=0.664,
                    warnings="provider_target_dropped_implausible; sector_from_eodhd_verified")
NOPRICE = _base_row("NOPX.US", None, fc=0.60)
ADVERSARIAL = _base_row("ADV.US", 10.0, fc=0.70,
                        opp_src="momentum_forecast_target_cap_drop_reject_roi",
                        fc_src="xprovider_price_conflict/price_bar_stale/provider_target")
ADVERSARIAL2 = _base_row("ADV2.US", 10.0, fc=0.70,
                         opp_src="both_present_fallback",
                         fc_src="synthetic_momentum_fallback_cap_target")


def _env(mode=None, recal="1", adjust=None, factors=None):
    for k in ENVS:
        os.environ.pop(k, None)
    if mode is not None:
        os.environ["TFB_REL_PATH_TAG"] = mode
    if recal is not None:
        os.environ["TFB_RELIABILITY_RECALIBRATION"] = recal
    if adjust is not None:
        os.environ["TFB_CALIBRATION_ADJUST"] = adjust
    if factors is not None:
        os.environ["TFB_CALIBRATION_FACTORS"] = factors


def _run(row, **env):
    _env(**env)
    r = copy.deepcopy(row)
    try:
        de._apply_investability_gate(r)
    finally:
        _env(None, recal=None)
    return r


def _tag(r):
    parts = [p.strip() for p in str(r.get("warnings") or "").split(";")]
    t = [p for p in parts if p.startswith("rel_path:")]
    return t[0] if t else None


def _fields(tag):
    body = tag[len("rel_path:"):]
    out = {}
    for kv in body.split(":"):
        k, v = kv.split("=", 1)
        out[k] = v
    return out


def _digest(r):
    return hashlib.sha256(json.dumps(r, sort_keys=True, default=str).encode()).hexdigest()


# ---- T1 helpers ------------------------------------------------------------------
def test_t1_helpers():
    _env(None, recal=None)
    assert de._rel_path_tag_mode() == "off" and not de._rel_path_tag_enabled()
    for v in ("observe", "1", "ON", "true"):
        os.environ["TFB_REL_PATH_TAG"] = v
        assert de._rel_path_tag_mode() == "observe"
    os.environ["TFB_REL_PATH_TAG"] = "enforce"       # no enforce mode exists
    assert de._rel_path_tag_mode() == "off"
    _env(None, recal=None)
    assert de._rel_path_src_code("") == "na" and de._rel_path_src_code(None) == "na"
    assert de._rel_path_src_code("both_present_fallback") == "both_present_fb"
    assert de._rel_path_src_code("phase_ii_synthetic") == "phase_ii_sy"
    assert de._rel_path_src_code("provider_target") == "pt"
    bad = "provider_target momentum forecast target cap drop reject roi price_bar_stale xprovider_price_conflict"
    code = de._rel_path_src_code(bad)
    assert not any(f in code for f in FORBIDDEN), code
    t = de._rel_path_tag(True, 66.4, 100.0, ["FS"], 61.5, 0.781, 48.0,
                         "momentum_only_fallback", "phase_ii_synthetic")
    assert t == "rel_path:b=B:fc=66.4:dq=100.0:pen=FS:os=mo_only_fb:fs=phase_ii_sy:raw=61.5:cf=0.781:fin=48.0"
    assert de._rel_path_tag(False, None, None, None, None, None, None, None, None) == "rel_path:tag_error"
    t2 = de._rel_path_tag(True, 50.0, 80.0, [], 50.0, None, 50.0, "", "")
    assert t2 == "rel_path:b=B:fc=50.0:dq=80.0:pen=none:os=na:fs=na:raw=50.0:cf=none:fin=50.0"


# ---- T2 off identity -------------------------------------------------------------
def test_t2_off_identity():
    for row in (DDI_ADV, DDI_GM, YUM_GM, CWBC_GM, NOPRICE, ADVERSARIAL):
        a, b = _run(row), _run(row, mode="off")
        assert a == b
        assert _tag(a) is None
        assert "rel_path" not in str(a.get("warnings") or "")


# ---- T3 observe: values untouched, one tag, decomposition exact -----------------
def test_t3_observe_decomposition():
    for row, expect_pen in ((DDI_ADV, "none"), (DDI_GM, "OS"), (YUM_GM, "FS"),
                            (CWBC_GM, "PD"), (NOPRICE, "NP")):
        off, obs = _run(row), _run(row, mode="observe")
        for k in ("forecast_reliability_score", "data_quality_score",
                  "investability_status", "final_action", "recommendation"):
            assert off.get(k) == obs.get(k), (row["symbol"], k)
        off_w = [p.strip() for p in str(off.get("warnings") or "").split(";") if p.strip()]
        obs_w = [p.strip() for p in str(obs.get("warnings") or "").split(";") if p.strip()]
        assert [w for w in obs_w if not w.startswith("rel_path:")] == off_w
        t = _tag(obs)
        assert t is not None and str(obs["warnings"]).count("rel_path:") == 1
        f = _fields(t)
        assert f["pen"] == expect_pen, (row["symbol"], f)
        assert f["b"] == "B"                                   # recal ON in _run
        assert float(f["fin"]) == float(obs["forecast_reliability_score"])
        assert float(f["raw"]) == float(obs["forecast_reliability_score"])  # no display calibration here
        assert float(f["dq"]) == float(obs["data_quality_score"])
        assert f["cf"] == "none"
    # the numeric legs reproduce the score: base - penalties == raw
    obs = _run(DDI_GM, mode="observe")
    f = _fields(_tag(obs))
    base = 0.7 * float(f["fc"]) + 0.3 * float(f["dq"])
    assert abs(round(base - 15.0, 1) - float(f["raw"])) <= 0.1
    # legacy base (recalibration OFF) discloses b=F
    leg = _run(DDI_ADV, mode="observe", recal=None)
    assert _fields(_tag(leg))["b"] == "F"


# ---- T4 display calibration factor disclosed --------------------------------------
def test_t4_calibration_factor():
    obs = _run(DDI_ADV, mode="observe", adjust="1",
               factors="INVESTABLE:0.781,WATCHLIST:1.025,BLOCKED:0.693")
    f = _fields(_tag(obs))
    st = str(obs.get("investability_status") or "").upper()
    factor = {"INVESTABLE": 0.781, "WATCHLIST": 1.025, "BLOCKED": 0.693}.get(st)
    if factor is None:
        assert f["cf"] == "none"
    else:
        assert f["cf"] == "%.3f" % factor
        assert abs(float(f["raw"]) * factor - float(f["fin"])) <= 0.11
        assert "reliability_calibrated" in str(obs["warnings"])
    assert float(f["fin"]) == float(obs["forecast_reliability_score"])


# ---- T5 substring safety, incl. adversarial sources -------------------------------
def test_t5_substring_safety():
    for row in (DDI_ADV, DDI_GM, YUM_GM, CWBC_GM, NOPRICE, ADVERSARIAL, ADVERSARIAL2):
        t = _tag(_run(row, mode="observe"))
        low = t.lower()
        for bad in FORBIDDEN:
            assert bad not in low, (row["symbol"], bad, t)
    adv = _run(ADVERSARIAL, mode="observe")
    f = _fields(_tag(adv))
    assert f["pen"] == "OS"           # opp-source leg only: fc_src has no synthetic/fallback/momentum
    assert "unsafe_token_suppressed" not in _tag(adv)
    adv2 = _run(ADVERSARIAL2, mode="observe")
    f2 = _fields(_tag(adv2))
    assert f2["pen"] == "OS,FS"       # both source legs fired
    low = _tag(adv2).lower()
    assert not any(b in low for b in FORBIDDEN), _tag(adv2)


# ---- T6 preserved-row replay: a row already carrying the tag is inert ----------
def test_t6_replay_inert():
    first = _run(CWBC_GM, mode="observe")
    again = _run(first, mode="observe")        # warnings now include the tag
    assert again["forecast_reliability_score"] == first["forecast_reliability_score"]
    assert again["data_quality_score"] == first["data_quality_score"]
    assert str(again["warnings"]).count("rel_path:") == 1
    off_again = _run(first)                    # gate off on a tagged row: values identical
    assert off_again["forecast_reliability_score"] == first["forecast_reliability_score"]


# ---- T7 idempotence ----------------------------------------------------------------
def test_t7_idempotent():
    for row in (DDI_ADV, DDI_GM, ADVERSARIAL):
        assert _digest(_run(row, mode="observe")) == _digest(_run(row, mode="observe"))
        assert _digest(_run(row)) == _digest(_run(row, mode="off"))


# ---- T8 wiring + disclosure --------------------------------------------------------
def test_t8_wiring():
    src = inspect.getsource(de)
    assert src.count('_rp.append("NP")') == 1 and src.count('_rp.append("NF")') == 1
    assert src.count('_rp.append("PD")') == 1 and src.count('_rp.append("BS")') == 1
    assert src.count('_rp.append("XC")') == 1 and src.count('_rp.append("OS")') == 1
    assert src.count('_rp.append("FS")') == 1 and src.count('_rp.append("SC%g"') == 1
    assert src.count("_rp_cal = _cal_factor") == 1
    assert src.count("if _rel_path_tag_enabled():") == 1
    assert src.index("if _rel_path_tag_enabled():") < src.index('row["forecast_reliability_score"] = rel')
    _env(None, recal=None)
    assert de.surface_gate_states().get("rel_path_tag") == "off"
    os.environ["TFB_REL_PATH_TAG"] = "observe"
    assert de.surface_gate_states().get("rel_path_tag") == "observe"
    _env(None, recal=None)
    assert de.__version__ == "5.144.0"


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
