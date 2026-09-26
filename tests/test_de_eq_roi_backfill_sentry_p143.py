"""P-143 (data_engine_v2 v5.142.0) — EQ-ROI backfill write-site sentry.

Repo-runnable battery T1-T6 over the REAL module (no stand-ins), using
fixture rows captured verbatim from the live 2026-09-16 Global_Markets
export (provider_target rows; numbers are the exported values).
Dual-tree off-mode equivalence vs pristine v5.141.0 was additionally
proven at build time (H1-H5 x3, digest a3d652f0560ebde9 — see the
commit sheet); this file asserts the properties that must hold at HEAD:
value invariance in every mode, exact tags, enforce deferral,
idempotence. NOTE: the synthesis fixtures carry the row's real
score fields — with every score input None the function returns before
synthesis (harness discovery during this build).

Run:  python tests/test_de_eq_roi_backfill_sentry_p143.py
"""
import copy, os, sys, importlib

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
de = importlib.import_module("core.data_engine_v2")

# Real 2026-09-16 GM export rows (Forecast Source = provider_target).
FIX_T12 = {"symbol": "FISV.US", "current_price": 50.02, "forecast_price_1m": 51.9,
           "forecast_price_3m": 54.4, "forecast_price_12m": 60.44,
           "forecast_source": "provider_target", "warnings": "",
           "expected_roi_1m": None, "expected_roi_3m": None, "expected_roi_12m": None}
FIX_T3 = {"symbol": "BNY.US", "current_price": 154.67, "forecast_price_1m": 157.0308,
          "forecast_price_3m": 160.1786, "forecast_price_12m": None,
          "forecast_source": "provider_target", "warnings": "",
          "expected_roi_1m": None, "expected_roi_3m": None, "expected_roi_12m": None}
FIX_SYN = {"symbol": "VNCE.US", "current_price": 8.4, "forecast_price_1m": None,
           "forecast_price_3m": None, "forecast_price_12m": None,
           "forecast_source": "", "warnings": "",
           "intrinsic_value": 11.707, "momentum_score": 78.95, "quality_score": 49.03,
           "value_score": 67.32, "growth_score": 69.5, "overall_score": 51.42,
           "volatility_30d": 1.776475, "volatility_90d": 1.300306,
           "expected_roi_1m": None, "expected_roi_3m": None, "expected_roi_12m": None}
FIX_FB = {"symbol": "3328.HK", "current_price": 8.02, "forecast_price_1m": None,
          "forecast_price_3m": None, "forecast_price_12m": None,
          "forecast_source": "", "warnings": "",
          "momentum_score": 41.7, "quality_score": 87.98, "value_score": 62.83,
          "growth_score": 69.67, "overall_score": 63.12,
          "expected_roi_1m": None, "expected_roi_3m": None, "expected_roi_12m": None}

ROI = ("expected_roi_1m", "expected_roi_3m", "expected_roi_12m")

def _run(fn, fixture, mode):
    if mode is None:
        os.environ.pop("TFB_EQ_ROI_UNIT_SENTRY", None)
    else:
        os.environ["TFB_EQ_ROI_UNIT_SENTRY"] = mode
    d = copy.deepcopy(fixture)
    fn(d)
    return d

def _vals(d):
    return {k: v for k, v in d.items() if k != "warnings"}

def _tags(d):
    return {t.strip() for t in (d.get("warnings") or "").split(";")
            if "eq_roi_backfill" in t}

def t1_off_no_tags_and_fraction_values():
    for fx, fn in ((FIX_T12, de._phase_ii_quality_forecast),
                   (FIX_T3, de._phase_ii_quality_forecast),
                   (FIX_SYN, de._phase_ii_quality_forecast),
                   (FIX_FB, de._compute_scores_local_fallback)):
        d = _run(fn, fx, None)
        assert not _tags(d), "off mode must add no tags"
        for k in ROI:
            v = d.get(k)
            assert v is not None and abs(v) <= 1.5, (fx["symbol"], k, v)
    # t12 branch value check against the export numbers, by hand:
    d = _run(de._phase_ii_quality_forecast, FIX_T12, None)
    assert abs(d["expected_roi_12m"] - round((60.44 - 50.02) / 50.02, 6)) < 1e-9
    assert abs(d["expected_roi_3m"] - round((54.4 - 50.02) / 50.02, 6)) < 1e-9
    print("T1 ok")

def t2_observe_exact_tags_zero_value_drift():
    exp = {
        "t12": {"eq_roi_backfill:%s:t12:observe" % k for k in ROI},
        "t3": {"eq_roi_backfill:%s:t3:observe" % k for k in ROI},
        "synth": {"eq_roi_backfill:%s:synth:observe" % k for k in ROI},
        "fallback": {"eq_roi_backfill:%s:fallback:observe" % k for k in ROI},
    }
    for src, fx, fn in (("t12", FIX_T12, de._phase_ii_quality_forecast),
                        ("t3", FIX_T3, de._phase_ii_quality_forecast),
                        ("synth", FIX_SYN, de._phase_ii_quality_forecast),
                        ("fallback", FIX_FB, de._compute_scores_local_fallback)):
        off = _run(fn, fx, None)
        obs = _run(fn, fx, "observe")
        assert _vals(off) == _vals(obs), "observe changed a value (%s)" % src
        assert _tags(obs) == exp[src], (src, _tags(obs))
    print("T2 ok")

def t3_enforce_is_deferred():
    for fx, fn in ((FIX_T12, de._phase_ii_quality_forecast),
                   (FIX_FB, de._compute_scores_local_fallback)):
        obs = _run(fn, fx, "observe")
        enf = _run(fn, fx, "enforce")
        assert _vals(obs) == _vals(enf), "enforce changed a value"
        assert "eq_roi_backfill:enforce_deferred" in enf["warnings"]
        assert "enforce_deferred" not in obs["warnings"]
    print("T3 ok")

def t4_idempotent_under_observe():
    os.environ["TFB_EQ_ROI_UNIT_SENTRY"] = "observe"
    d = copy.deepcopy(FIX_T12)
    de._phase_ii_quality_forecast(d)
    w1, v1 = d["warnings"], _vals(d)
    de._phase_ii_quality_forecast(d)
    assert d["warnings"] == w1 and _vals(d) == v1, "second pass mutated"
    print("T4 ok")

def t5_bad_mode_is_off():
    for mode in ("", "1", "on", "OFF", "Enforce "):
        d = _run(de._phase_ii_quality_forecast, FIX_T12,
                 mode if mode.strip().lower() in ("observe", "enforce") else mode)
        if mode.strip().lower() in ("observe", "enforce"):
            assert _tags(d)
        else:
            assert not _tags(d), (mode, d["warnings"])
    print("T5 ok")

def t6_existing_roi_untagged():
    fx = copy.deepcopy(FIX_T12)
    fx["expected_roi_1m"], fx["expected_roi_3m"], fx["expected_roi_12m"] = 0.01, 0.02, 0.03
    d = _run(de._phase_ii_quality_forecast, fx, "observe")
    assert not _tags(d), "tag fired without a write"
    assert (d["expected_roi_1m"], d["expected_roi_3m"], d["expected_roi_12m"]) == (0.01, 0.02, 0.03)
    print("T6 ok")

if __name__ == "__main__":
    for i in range(3):
        t1_off_no_tags_and_fraction_values(); t2_observe_exact_tags_zero_value_drift()
        t3_enforce_is_deferred(); t4_idempotent_under_observe()
        t5_bad_mode_is_off(); t6_existing_roi_untagged()
    print("P-143 battery T1-T6 PASS x3")
