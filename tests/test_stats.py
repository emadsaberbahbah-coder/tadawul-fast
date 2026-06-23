# -*- coding: utf-8 -*-
"""
Known-answer tests for core.stats v1.0.0.
Every primitive is checked against a textbook/closed-form value so the module is
auditable. Run: pytest tests/test_stats.py  (or: python tests/test_stats.py)

These run with scipy ABSENT to exercise the pure-numpy fallback path (the path
that executes in the lean web image).
"""
import json
import math

try:
    from core import stats as S  # production location: core/stats.py
except ImportError:  # allow standalone run from the build/test dir
    import stats as S


def approx(a, b, tol=1e-6):
    return abs(a - b) <= tol


# --- Section 1: distributions -------------------------------------------------

def test_normal_cdf_ppf():
    assert approx(S.normal_cdf(0.0), 0.5)
    assert approx(S.normal_cdf(1.96), 0.9750021, 1e-5)
    assert approx(S.normal_ppf(0.975), 1.959963985, 1e-6)
    assert approx(S.normal_ppf(0.5), 0.0, 1e-9)
    # round-trip
    assert approx(S.normal_cdf(S.normal_ppf(0.83)), 0.83, 1e-9)


def test_t_two_sided():
    # df=1 (Cauchy): P(|T|>=1) = 0.5
    assert approx(S.t_sf_two_sided(1.0, 1), 0.5, 1e-6)
    # large df approaches normal: t=1.96 -> ~0.05
    assert approx(S.t_sf_two_sided(1.959963985, 1_000_000), 0.05, 1e-3)
    # t=0 -> p=1
    assert approx(S.t_sf_two_sided(0.0, 10), 1.0, 1e-9)


# --- Section 2: hygiene -------------------------------------------------------

def test_winsorize():
    x = [1, 2, 3, 4, 5, 6, 7, 8, 9, 1000]
    w = S.winsorize(x, 0.0, 0.9)
    assert max(w) < 1000  # the outlier is pulled in
    assert min(w) == 1


def test_cross_sectional_z():
    z = S.cross_sectional_z([10, 12, 14, 16, 18])
    assert approx(float(sum(z)), 0.0, 1e-9)  # mean 0
    # zero variance -> all zeros
    z0 = S.cross_sectional_z([5, 5, 5, 5])
    assert all(v == 0.0 for v in z0)


def test_mad_outlier_bbd():
    # the BBD.US case: ~$3.50 history, $250 entered
    hist = [3.40, 3.50, 3.45, 3.55, 3.50, 3.60]
    assert S.is_mad_outlier(hist, 250.0) is True
    assert S.is_mad_outlier(hist, 3.52) is False


# --- Section 3: bootstrap -----------------------------------------------------

def test_block_bootstrap_ci_deterministic_and_brackets():
    data = [0.0, 1.0] * 50  # mean 0.5
    r1 = S.block_bootstrap_ci(data, n_boot=500, seed=7)
    r2 = S.block_bootstrap_ci(data, n_boot=500, seed=7)
    assert r1 == r2  # deterministic given the seed
    assert approx(r1["point"], 0.5, 1e-9)
    assert r1["ci_low"] <= 0.5 <= r1["ci_high"]
    assert r1["block_size"] >= 1


# --- Section 4: multiple testing ---------------------------------------------

def test_benjamini_hochberg_all_reject():
    res = S.benjamini_hochberg([0.01, 0.02, 0.03, 0.04, 0.05], alpha=0.05)
    assert res["n_significant"] == 5
    assert all(res["reject"])


def test_benjamini_hochberg_only_smallest():
    res = S.benjamini_hochberg([0.001, 0.5, 0.5, 0.5, 0.5], alpha=0.05)
    assert res["n_significant"] == 1
    assert res["reject"][0] is True
    assert approx(res["pvalues_adjusted"][0], 0.005, 1e-9)


# --- Section 5: power / testability (B0) -------------------------------------

def test_required_n_textbook():
    # p0=0.5, p1=0.6, alpha=.05, power=.8 -> ~388 per group (well known)
    n = S.required_n_two_prop(0.5, 0.6, alpha=0.05, power=0.8)
    assert abs(n - 388) <= 2


def test_mde_roundtrip():
    n = S.required_n_two_prop(0.5, 0.6, alpha=0.05, power=0.8)
    mde = S.min_detectable_effect(0.5, n, alpha=0.05, power=0.8)
    assert approx(mde, 0.1, 0.01)


def test_testability_b0_untestable():
    # the master-plan B0 case: 384 windows, 3% trigger, 50% base, want 10pp edge
    rep = S.testability_report(total_windows=384, trigger_rate=0.03,
                               base_rate=0.5, plausible_effect=0.10)
    assert rep["verdict"] == "UNTESTABLE_BY_DESIGN"
    assert approx(rep["effective_n"], 11.52, 1e-6)
    assert rep["required_effective_n"] > rep["effective_n"]


def test_testability_testable():
    # short horizon, common trigger, wide universe -> plenty of power
    rep = S.testability_report(total_windows=2000, trigger_rate=0.5,
                               base_rate=0.5, plausible_effect=0.15)
    assert rep["verdict"] == "TESTABLE"


# --- Section 6: information coefficient ---------------------------------------

def test_ic_perfect_monotone():
    res = S.information_coefficient([1, 2, 3, 4, 5], [2, 4, 6, 8, 10])
    assert approx(res["ic"], 1.0, 1e-9)


def test_ic_negative_and_significant():
    scores = [1, 2, 3, 4, 5, 6, 7, 8]
    rets = [8, 7, 6, 5, 4, 3, 2, 1]
    res = S.information_coefficient(scores, rets)
    assert approx(res["ic"], -1.0, 1e-9)


def test_ic_small_sample_guard():
    res = S.information_coefficient([1, 2], [1, 2])
    assert math.isnan(res["ic"])


# --- Section 7: calibration ---------------------------------------------------

def test_wilson_known():
    lo, hi = S.wilson_interval(50, 100, 0.95)
    assert approx(lo, 0.4038, 2e-3)
    assert approx(hi, 0.5962, 2e-3)


def test_brier_baseline():
    res = S.brier_score([0.5, 0.5, 0.5, 0.5], [1, 0, 1, 0])
    assert approx(res["brier"], 0.25, 1e-9)
    assert approx(res["uncertainty"], 0.25, 1e-9)
    assert approx(res["skill_score"], 0.0, 1e-9)


def test_brier_perfect():
    res = S.brier_score([0.0, 1.0, 0.0, 1.0], [0, 1, 0, 1])
    assert approx(res["brier"], 0.0, 1e-9)
    assert approx(res["skill_score"], 1.0, 1e-9)


def test_ece_and_curve():
    probs = [0.2, 0.25, 0.75, 0.8, 0.5, 0.55]
    outs = [0, 0, 1, 1, 1, 0]
    e = S.expected_calibration_error(probs, outs, n_bins=5)
    assert 0.0 <= e["ece"] <= 1.0
    curve = S.reliability_curve(probs, outs, n_bins=5)
    assert all("ci_low" in b and "ci_high" in b for b in curve)


def test_spiegelhalter_wellcalibrated():
    # perfectly calibrated, non-0.5 forecasts: 0.3-group with 30% hits,
    # 0.7-group with 70% hits -> z is exactly 0 (deviations cancel).
    probs = [0.3] * 100 + [0.7] * 100
    outs = [1] * 30 + [0] * 70 + [1] * 70 + [0] * 30
    res = S.spiegelhalter_z(probs, outs)
    assert approx(res["z"], 0.0, 1e-9)


def test_spiegelhalter_miscalibrated():
    # overconfident: claims 0.9 but only 50% happen -> large negative-tail z
    probs = [0.9] * 100
    outs = [1] * 50 + [0] * 50
    res = S.spiegelhalter_z(probs, outs)
    assert abs(res["z"]) > 3.0
    assert res["p_value"] < 0.01


def test_spiegelhalter_degenerate_all_half():
    # all-0.5 forecasts carry no calibration information -> undefined (NaN)
    res = S.spiegelhalter_z([0.5] * 10, [1, 0] * 5)
    assert math.isnan(res["z"])


def test_isotonic_monotone():
    fit = S.isotonic_fit([1, 2, 3, 4], [0, 1, 0, 1])
    yk = fit["y_knots"]
    assert all(yk[i] <= yk[i + 1] + 1e-12 for i in range(len(yk) - 1))
    applied = S.isotonic_apply(fit, [1, 2.5, 4])
    assert all(applied[i] <= applied[i + 1] + 1e-9 for i in range(len(applied) - 1))


# --- Section 8: event study ---------------------------------------------------

def test_car_event_study():
    # 3 events, asset beats market by event-specific CAR of 0.04/0.05/0.06
    market = [[0.0] * 5 for _ in range(3)]
    cars = [0.04, 0.05, 0.06]
    asset = [[cars[i] / 5.0] * 5 for i in range(3)]
    res = S.car_event_study(asset, market)
    assert approx(res["mean_car"], 0.05, 1e-9)
    assert res["t_stat"] > 0
    assert res["p_value"] < 0.1
    assert res["n_events"] == 3


# --- serialization ------------------------------------------------------------

def test_outputs_json_serializable():
    for obj in [
        S.brier_score([0.3, 0.7], [0, 1]),
        S.testability_report(100, 0.2, 0.5, 0.1),
        S.information_coefficient([1, 2, 3], [3, 2, 1]),
        S.car_event_study([[0.01, 0.02]], [[0.0, 0.0]]),
        S.reliability_curve([0.2, 0.8], [0, 1], n_bins=2),
    ]:
        json.dumps(obj)  # must not raise


# --- Section 5b: magnitude / effect-size power --------------------------------

def test_cohens_d_required_n_textbook():
    # one-sample t, medium effect d=0.5, alpha .05 two-sided, power .8 -> ~34
    assert abs(S.cohens_d_required_n(0.5) - 34) <= 2


def test_min_detectable_cohens_d_roundtrip():
    n = S.cohens_d_required_n(0.5)
    d = S.min_detectable_cohens_d(n)
    assert approx(d, 0.5, 0.03)


def test_magnitude_testability():
    # common trigger, ample events, medium effect -> testable
    ok = S.magnitude_testability_report(total_windows=252, trigger_rate=0.20,
                                        plausible_d=0.5)
    assert ok["verdict"] == "TESTABLE"
    # rare trigger -> too few events even for a medium effect
    bad = S.magnitude_testability_report(total_windows=63, trigger_rate=0.03,
                                         plausible_d=0.5)
    assert bad["verdict"] == "UNTESTABLE_BY_DESIGN"


def test_magnitude_far_more_powerful_than_proportion():
    # same scenario: magnitude test needs far fewer events than the proportion test
    n_mag = S.cohens_d_required_n(0.5)             # ~34 events
    n_prop = S.required_n_two_prop(0.5, 0.6)       # ~388 events
    assert n_mag < n_prop / 5


# --- end magnitude tests ------------------------------------------------------


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = 0
    for fn in fns:
        try:
            fn()
            print("PASS", fn.__name__)
            passed += 1
        except AssertionError as e:
            print("FAIL", fn.__name__, "->", e)
        except Exception as e:
            print("ERROR", fn.__name__, "->", repr(e))
    print(f"\n{passed}/{len(fns)} passed")
