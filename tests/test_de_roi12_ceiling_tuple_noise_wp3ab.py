"""WP-3ab (data_engine_v2 v5.152.0) -- PUBLISH-BOUNDARY TUPLE SEAMS.

(a) TFB_TUPLE_NOISE_RELEASE  off|observe|enforce  _tuple_noise_release(row)
    The presenter (core/sheet_presentation.py) withholds an Expected ROI leg
    whose stored fraction disagrees with FP/CP-1 by more than
    max(5e-6, |implied|*1e-6); forecast prices are written round(x, 4), so
    4-dp rounding noise (0.00005/cp + 5e-7) alone blanks every cp < 10 row
    (2026-10-10 export: 1,238 FP12-present / ROI12-blank rows, 1,190 of them
    noise-class). enforce snaps the row's OWN fraction to round(implied, 6)
    inside the narrow band (pres_tol, noise_bound + 0.0005]; precision-limited
    (cp < 0.05) and gap legs are tagged, never rewritten.
(b) TFB_ROI12_SOFTCAP_GATE    off|observe|enforce  _roi12_softcap(row)
    A keep-last-good provider target is restored AFTER Fix-O ran, so its 12M
    leg is never re-capped against today's price (153 GM rows > 0.35 on the
    2026-10-10 export, all analyst_lkg, none Fix-O-tagged). enforce applies
    the Fix-O mapping (asymptote 0.30 + band) to forecast_price_12m and
    expected_roi_12m in lockstep; tag r12_ceiling:<mode>:<hi|band>.
Both: default OFF = byte-identical; observe = tags only; wrapper
_publish_tuple_seams(row) runs (b) then (a) at the two first-presentation
sites (_strict_project_row, get_page_rows loop).

Run: python -m pytest -q tests/test_de_roi12_ceiling_tuple_noise_wp3ab.py
     python tests/test_de_roi12_ceiling_tuple_noise_wp3ab.py        (battery x3)
Dual-tree proof: WP3AB_ENGINE_ROOT=<dir holding a base core/> makes this file
import the engine from that tree first; it must FAIL there (T1/T10 at least).
"""
from __future__ import annotations

import copy
import inspect
import math
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_ENGINE_ROOT = (os.environ.get("WP3AB_ENGINE_ROOT") or "").strip()
if _ENGINE_ROOT:
    sys.path.insert(0, _ENGINE_ROOT)
sys.path.insert(1 if _ENGINE_ROOT else 0, _ROOT)

import core.data_engine_v2 as de  # noqa: E402
from core.sheet_presentation import present_instrument_row  # noqa: E402

TNR = "TFB_TUPLE_NOISE_RELEASE"
R12 = "TFB_ROI12_SOFTCAP_GATE"
GATES = (TNR, R12, "TFB_PROVIDER_TARGET_SOFT_CAP", "TFB_PROVIDER_TARGET_SOFT_CAP_BAND",
         "TFB_FORECAST_PAIR_COHERENCE", "TFB_EQ_ROI_UNIT_SENTRY", "TFB_FC_TUPLE_COHERENT", "TFB_MARGIN_PUBLISH")
FORBIDDEN = ("cap", "forecast", "target", "roi", "drop", "reject", "provider_target",
             "price_bar_stale", "xprovider_price_conflict")
PROD_ENV = {"TFB_EQ_ROI_UNIT_SENTRY": "observe", "TFB_FC_TUPLE_COHERENT": "observe", "TFB_MARGIN_PUBLISH": "observe"}

# ---------------------------------------------------------------------------
# Real 2026-10-10 export values. The noise rows publish the ROI leg BLANK in
# the export (that is DQ-32), so the withheld stored fraction is reconstructed
# by the interval rule used by tests/fixtures/wp3ab_tuple_rows_2026-10-10.json:
# any value inside (pres_tol, noise_bound] is indistinguishable to the seam.
# ---------------------------------------------------------------------------
NOISE = [
    {"symbol": "8150.SR", "current_price": 6.7, "forecast_price_12m": 6.6615, "expected_roi_12m": -0.005753,
     "forecast_price_3m": 6.6865, "expected_roi_3m": -0.002021, "forecast_price_1m": 6.6954, "expected_roi_1m": -0.000689,
     "forecast_source": "phase_ii_synthetic", "warnings": "eq_roi_backfill:expected_roi_12m:synth:observe"},
    {"symbol": "XXII.US", "current_price": 0.8116, "forecast_price_12m": 0.7788, "expected_roi_12m": -0.040448,
     "forecast_source": "phase_ii_synthetic"},
    {"symbol": "LSE.US", "current_price": 5.168, "forecast_price_12m": 5.2084, "expected_roi_12m": 0.007825,
     "forecast_source": "phase_ii_synthetic"},
]
SNAPPED = {"8150.SR": -0.005746, "XXII.US": -0.040414, "LSE.US": 0.007817}
PRECISION = {"symbol": "SUI-USD", "current_price": 0.0003001941, "forecast_price_12m": 0.0003, "expected_roi_12m": -0.025,
             "forecast_source": "phase_ii_synthetic"}
VINTAGE = {"symbol": "GRC.US", "current_price": 73.915, "forecast_price_12m": 80.6667, "expected_roi_12m": 0.09,
           "forecast_source": "provider_target", "target_mean_price": 80.66667, "warnings": "fctuple_vintage:12m:observe"}
COHERENT = {"symbol": "COH.X", "current_price": 100.0, "forecast_price_12m": 112.5, "expected_roi_12m": 0.125}
# P-102's own "coherent" row is NOT coherent at the presenter: |0.28432 - 0.2843| = 2.0e-5 > 5e-6 (a 4-dp ROI);
# the seam classes it drift (2.0e-5 <= noise_bound + 0.0005) and would release it under enforce.
DRIFT_P102 = {"symbol": "SBAC.US", "current_price": 176.28, "forecast_price_12m": 226.4, "expected_roi_12m": 0.2843}

LKG_TAGS = "provider_target_capped_for_short_horizon_derivation; eq_roi_backfill:expected_roi_12m:t12:observe"
CEIL = [
    {"symbol": "AKAM.US", "current_price": 106.95, "forecast_price_12m": 146.8138, "expected_roi_12m": 0.372733,
     "forecast_price_3m": 118.1797, "expected_roi_3m": 0.105, "forecast_price_1m": 110.8002, "expected_roi_1m": 0.036,
     "target_mean_price": 158.87318, "forecast_source": "provider_target",
     "warnings": "analyst_lkg:137h; " + LKG_TAGS},
    {"symbol": "AC.TO", "current_price": 22.76, "forecast_price_12m": 34.4893, "expected_roi_12m": 0.515347,
     "forecast_price_3m": 25.1498, "expected_roi_3m": 0.105, "forecast_price_1m": 23.5794, "expected_roi_1m": 0.036002,
     "target_mean_price": 35.33571, "forecast_source": "provider_target",
     "warnings": "analyst_lkg:89h; " + LKG_TAGS + "; f7_settle_failed:non_converged"},
    {"symbol": "TMUS.US", "current_price": 148.58, "forecast_price_12m": 230.2848, "expected_roi_12m": 0.549904,
     "target_mean_price": 241.24, "forecast_source": "provider_target",
     "warnings": "analyst_lkg:40h; " + LKG_TAGS + "; f7_settle_failed:non_converged"},
    {"symbol": "WGO.US", "current_price": 23.39, "forecast_price_12m": 35.4205, "expected_roi_12m": 0.514344,
     "target_mean_price": 37.8, "forecast_source": "provider_target",
     "warnings": "analyst_lkg:125h; " + LKG_TAGS + "; f7_settle_failed:non_converged"},
    {"symbol": "PXLW.US", "current_price": 5.94, "forecast_price_12m": 9.4602, "expected_roi_12m": 0.592626,
     "target_mean_price": 10.0, "forecast_source": "provider_target", "warnings": "analyst_lkg:137h; " + LKG_TAGS},
    {"symbol": "PMT.US", "current_price": 7.36, "forecast_price_12m": 10.6348, "expected_roi_12m": 0.444946,
     "target_mean_price": 11.28571, "forecast_source": "provider_target", "investability_status": "BLOCKED",
     "warnings": "analyst_lkg:64h; " + LKG_TAGS},
]
# expected (forecast_price_12m, expected_roi_12m) after the Fix-O mapping at the default band 0.05
CAPPED = {"AKAM.US": (143.134, 0.338326), "AC.TO": (30.7107, 0.349328), "TMUS.US": (200.5328, 0.349662),
          "WGO.US": (31.5604, 0.349312), "PXLW.US": (8.0181, 0.349848), "PMT.US": (9.9157, 0.347242)}
HARD = {"AKAM.US": 139.035, "AC.TO": 29.588, "TMUS.US": 193.154, "WGO.US": 30.407, "PXLW.US": 7.722, "PMT.US": 9.568}
FIXO_CONTROL = {"symbol": "FIXO.X", "current_price": 100.0, "forecast_price_12m": 134.0, "expected_roi_12m": 0.34,
                "forecast_source": "provider_target", "warnings": "provider_target_12m_capped_to_phase_ii_ceiling"}
BAND_LKG = {"symbol": "BAND.X", "current_price": 100.0, "forecast_price_12m": 132.0, "expected_roi_12m": 0.32,
            "forecast_source": "provider_target", "warnings": "analyst_lkg:10h"}
NEG_LKG = {"symbol": "NEG.X", "current_price": 100.0, "forecast_price_12m": 55.0, "expected_roi_12m": -0.45,
           "forecast_source": "provider_target", "warnings": "analyst_lkg:10h"}
NOT_PT = {"symbol": "SYN.X", "current_price": 100.0, "forecast_price_12m": 150.0, "expected_roi_12m": 0.5,
          "forecast_source": "phase_ii_synthetic"}

ALL_ROWS = NOISE + [PRECISION, VINTAGE, COHERENT] + CEIL + [FIXO_CONTROL, BAND_LKG, NEG_LKG, NOT_PT]


def _clean():
    for k in GATES:
        os.environ.pop(k, None)


def _env(name, mode):
    if mode is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = mode


def _implied(r, h="12m"):
    return (r["forecast_price_%s" % h] - r["current_price"]) / r["current_price"]


def _tags(row, prefix):
    return [p for p in de._mpc_warning_parts(row) if p.startswith(prefix)]


def _vals(row):
    return {k: v for k, v in row.items() if k != "warnings"}


def test_t1_gate_explicit_words_only():
    _clean()
    try:
        for raw, want in ((None, "off"), ("", "off"), ("1", "off"), ("true", "off"), ("on", "off"), ("yes", "off"),
                          ("off", "off"), ("observe", "observe"), (" ENFORCE ", "enforce"), ("garbage", "off")):
            _env(TNR, raw)
            _env(R12, raw)
            assert de._tuple_noise_mode() == want, (raw, want)
            assert de._roi12_softcap_mode() == want, (raw, want)
            states = de.surface_gate_states()
            assert states.get("tuple_noise_release") == want
            assert states.get("roi12_softcap") == want
    finally:
        _clean()


def test_t2_off_inert_both_envs():
    _clean()
    try:
        for extra in ({}, PROD_ENV):
            for k, v in extra.items():
                os.environ[k] = v
            for raw in (None, "off", "1", "true", "on"):
                _env(TNR, raw)
                _env(R12, raw)
                for r0 in ALL_ROWS:
                    r = copy.deepcopy(r0)
                    assert de._publish_tuple_seams(r) is None
                    assert r == r0, r0["symbol"]
                    assert de._tuple_noise_release(copy.deepcopy(r0)) == 0
                    assert de._roi12_softcap(copy.deepcopy(r0)) == 0
            _clean()
    finally:
        _clean()


def test_t3_noise_observe_tag_only():
    _clean()
    try:
        _env(TNR, "observe")
        for r0 in NOISE:
            r = copy.deepcopy(r0)
            n = de._tuple_noise_release(r)
            assert n >= 1 and _vals(r) == _vals(r0), r0["symbol"]
            t = _tags(r, "tuple_noise:")
            assert "tuple_noise:12m:round:observe" in t, (r0["symbol"], t)
            assert all(x.endswith(":round:observe") for x in t), t
            # the other legs: exactly as the arithmetic dictates
            for h in ("3m", "1m"):
                if ("forecast_price_%s" % h) in r0:
                    diff = abs(_implied(r0, h) - r0["expected_roi_%s" % h])
                    pres = max(de._TNR_PRES_TOL_ABS, abs(_implied(r0, h)) * de._TNR_PRES_TOL_REL)
                    assert (("tuple_noise:%s:round:observe" % h) in t) == (diff > pres), (r0["symbol"], h)
            assert n == len(t)
            assert present_instrument_row(r)["expected_roi_12m"] is None   # observe: still withheld downstream
        r = copy.deepcopy(PRECISION)
        assert de._tuple_noise_release(r) == 1 and _vals(r) == _vals(PRECISION)
        assert _tags(r, "tuple_noise:") == ["tuple_noise:12m:precision_limited"]
        r = copy.deepcopy(VINTAGE)
        assert de._tuple_noise_release(r) == 0 and r == VINTAGE          # > _fc_tuple_tol: P-102's class
        r = copy.deepcopy(COHERENT)
        assert de._tuple_noise_release(r) == 0 and r == COHERENT
        r = copy.deepcopy(DRIFT_P102)
        assert present_instrument_row(dict(DRIFT_P102))["expected_roi_12m"] is None     # withheld today
        assert de._tuple_noise_release(r) == 1 and _vals(r) == _vals(DRIFT_P102)
        assert _tags(r, "tuple_noise:") == ["tuple_noise:12m:drift:observe"]
        again = copy.deepcopy(NOISE[0])
        de._tuple_noise_release(again)
        twice = copy.deepcopy(again)
        de._tuple_noise_release(twice)
        assert twice == again                                            # tags dedupe: observe idempotent
    finally:
        _clean()


def test_t4_noise_enforce_snaps_and_is_idempotent():
    _clean()
    try:
        _env(TNR, "enforce")
        for r0 in NOISE:
            base_pub = present_instrument_row(copy.deepcopy(r0))
            assert base_pub["expected_roi_12m"] is None                  # base behaviour: withheld
            r = copy.deepcopy(r0)
            n = de._tuple_noise_release(r)
            assert n >= 1
            assert r["expected_roi_12m"] == round(_implied(r0), 6) == SNAPPED[r0["symbol"]], r0["symbol"]
            assert r["forecast_price_12m"] == r0["forecast_price_12m"] and r["current_price"] == r0["current_price"]
            assert "tuple_noise:12m:round:enforce" in _tags(r, "tuple_noise:")
            for h in ("3m", "1m"):
                if ("expected_roi_%s" % h) in r0 and ("tuple_noise:%s:round:enforce" % h) in _tags(r, "tuple_noise:"):
                    assert r["expected_roi_%s" % h] == round(_implied(r0, h), 6)
            pub = present_instrument_row(r)
            assert pub["expected_roi_12m"] == SNAPPED[r0["symbol"]]      # the presenter now accepts it
            assert abs(pub["expected_roi_12m"] - _implied(r0)) <= 5e-7
            again = copy.deepcopy(r)
            assert de._tuple_noise_release(again) == 0 and again == r     # second pass: no-op
        r = copy.deepcopy(PRECISION)
        assert de._tuple_noise_release(r) == 1 and _vals(r) == _vals(PRECISION)
        assert _tags(r, "tuple_noise:") == ["tuple_noise:12m:precision_limited"]
        assert present_instrument_row(r)["expected_roi_12m"] is None     # stays withheld until PRICE_PRECISION
        r = copy.deepcopy(VINTAGE)
        assert de._tuple_noise_release(r) == 0 and r == VINTAGE
    finally:
        _clean()


def test_t5_tolerance_mirror_and_bands():
    _clean()
    try:
        _env(TNR, "observe")
        # (1) the seam's acceptance rule == the presenter's acceptance rule, on a sweep
        for cp in (0.0003001941, 0.07, 0.8116, 5.168, 6.7, 25.0, 106.95, 1500.0):
            for ret in (-0.45, -0.0057, 0.0, 0.0078, 0.12, 0.3727, 1.2):
                fp = round(cp * (1.0 + ret), 4)
                if fp <= 0:
                    continue
                implied = (fp - cp) / cp
                for k in (0.0, 0.5, 0.99, 1.01, 1.5, 3.0, 20.0):
                    stored = round(implied + k * de._TNR_PRES_TOL_ABS, 6)
                    diff = abs(implied - stored)
                    pres_tol = max(de._TNR_PRES_TOL_ABS, abs(implied) * de._TNR_PRES_TOL_REL)
                    row = {"current_price": cp, "forecast_price_12m": fp, "expected_roi_12m": stored}
                    accepted = present_instrument_row(dict(row))["expected_roi_12m"] is not None
                    assert accepted == (diff <= pres_tol), (cp, ret, k, diff, pres_tol)
                    n = de._tuple_noise_release(dict(row))
                    if accepted:
                        assert n == 0
        # (2) release band: round | drift | gap | vintage (classed against a 25.0 row)
        cp, fp = 25.0, 27.5
        implied = (fp - cp) / cp
        nb = de._TNR_PRICE_HALF_ULP / cp + de._TNR_ROI_HALF_ULP
        assert abs(de._fc_tuple_tol(cp) - 0.0006) < 1e-12
        cases = (
            (implied + 0.000008, "tuple_noise:12m:drift:observe"),                       # > nb (2.5e-6), < nb+0.0005
            (implied + 0.00051, "tuple_noise:12m:gap"),                                   # > nb + 0.0005, <= fct_tol 0.0006
        )
        for stored, tag in cases:
            r = {"current_price": cp, "forecast_price_12m": fp, "expected_roi_12m": round(stored, 6)}
            r0 = dict(r)
            assert de._tuple_noise_release(r) == 1 and _vals(r) == _vals(r0)
            assert _tags(r, "tuple_noise:") == [tag], (stored, _tags(r, "tuple_noise:"))
        r = {"current_price": cp, "forecast_price_12m": fp, "expected_roi_12m": round(implied + 0.0007, 6)}
        assert de._tuple_noise_release(r) == 0 and not _tags(r, "tuple_noise:")           # > fct_tol: skipped
        r = {"current_price": 6.7, "forecast_price_12m": 6.6615, "expected_roi_12m": -0.005753}
        assert de._tuple_noise_release(r) == 1 and _tags(r, "tuple_noise:") == ["tuple_noise:12m:round:observe"]
        _env(TNR, "enforce")
        r = {"current_price": cp, "forecast_price_12m": fp, "expected_roi_12m": round(implied + 0.00051, 6)}
        r0 = dict(r)
        assert de._tuple_noise_release(r) == 1 and _vals(r) == _vals(r0)                 # gap: never rewritten
        assert _tags(r, "tuple_noise:") == ["tuple_noise:12m:gap"]
        assert nb < de._TNR_MAX_NOISE and (de._TNR_PRICE_HALF_ULP / 0.049 + de._TNR_ROI_HALF_ULP) > de._TNR_MAX_NOISE
    finally:
        _clean()


def test_t6_ceiling_observe_tag_only():
    _clean()
    try:
        _env(R12, "observe")
        for r0 in CEIL:
            r = copy.deepcopy(r0)
            assert de._roi12_softcap(r) == 1 and _vals(r) == _vals(r0), r0["symbol"]
            assert _tags(r, "r12_ceiling:") == ["r12_ceiling:observe:hi"], r0["symbol"]
            assert present_instrument_row(r)["expected_roi_12m"] == r0["expected_roi_12m"]
        r = copy.deepcopy(BAND_LKG)
        assert de._roi12_softcap(r) == 1 and _vals(r) == _vals(BAND_LKG)
        assert _tags(r, "r12_ceiling:") == ["r12_ceiling:observe:band"]
        r = copy.deepcopy(NEG_LKG)
        assert de._roi12_softcap(r) == 1 and _vals(r) == _vals(NEG_LKG)
        assert _tags(r, "r12_ceiling:") == ["r12_ceiling:observe:hi"]
        for r0 in (FIXO_CONTROL, NOT_PT, COHERENT):
            r = copy.deepcopy(r0)
            assert de._roi12_softcap(r) == 0 and r == r0, r0["symbol"]
        again = copy.deepcopy(CEIL[0])
        de._roi12_softcap(again)
        twice = copy.deepcopy(again)
        de._roi12_softcap(twice)
        assert twice == again
    finally:
        _clean()


def test_t7_ceiling_enforce_lockstep_idempotent_hardclamp():
    _clean()
    try:
        _env(R12, "enforce")
        band = de._provider_target_soft_cap_band()
        assert abs(band - 0.05) < 1e-12
        for r0 in CEIL:
            r = copy.deepcopy(r0)
            assert de._roi12_softcap(r) == 1
            cp = r0["current_price"]
            fp12, roi12 = CAPPED[r0["symbol"]]
            assert r["forecast_price_12m"] == fp12, (r0["symbol"], r["forecast_price_12m"])
            assert r["expected_roi_12m"] == roi12, (r0["symbol"], r["expected_roi_12m"])
            assert r["expected_roi_12m"] == round((r["forecast_price_12m"] - cp) / cp, 6)
            assert abs(r["forecast_price_12m"] / cp - 1.0) < 0.30 + band
            assert r["expected_roi_12m"] < r0["expected_roi_12m"]
            for k in ("forecast_price_3m", "forecast_price_1m", "expected_roi_3m", "expected_roi_1m",
                      "target_mean_price", "current_price", "investability_status"):
                if k in r0:
                    assert r[k] == r0[k], (r0["symbol"], k)
            assert _tags(r, "r12_ceiling:") == ["r12_ceiling:enforce:hi"]
            assert present_instrument_row(r)["expected_roi_12m"] == roi12            # presenter keeps it published
            again = copy.deepcopy(r)
            assert de._roi12_softcap(again) == 0 and again == r                          # second pass: no-op
        # ordering preserved across the breaches (monotone mapping)
        order0 = sorted(CEIL, key=lambda x: x["expected_roi_12m"])
        order1 = sorted(CEIL, key=lambda x: CAPPED[x["symbol"]][1])
        assert [x["symbol"] for x in order0] == [x["symbol"] for x in order1]
        r = copy.deepcopy(BAND_LKG)
        assert de._roi12_softcap(r) == 1 and r["forecast_price_12m"] < 132.0 and r["expected_roi_12m"] > 0.30
        assert r["expected_roi_12m"] == round((r["forecast_price_12m"] - 100.0) / 100.0, 6)
        assert _tags(r, "r12_ceiling:") == ["r12_ceiling:enforce:band"]
        r = copy.deepcopy(NEG_LKG)
        assert de._roi12_softcap(r) == 1 and -0.35 < r["expected_roi_12m"] < -0.30
        assert r["expected_roi_12m"] == round((r["forecast_price_12m"] - 100.0) / 100.0, 6)
        r = copy.deepcopy(FIXO_CONTROL)
        assert de._roi12_softcap(r) == 0 and r == FIXO_CONTROL                             # Fix-O already bounded it
        # ROI-absent provider_target row: price capped, ROI never invented
        r = {"symbol": "NOROI.X", "current_price": 100.0, "forecast_price_12m": 150.0, "forecast_source": "provider_target"}
        assert de._roi12_softcap(r) == 1 and r["forecast_price_12m"] < 135.0 and "expected_roi_12m" not in r
        # stale enforce tag never suppresses a real re-breach (value-conditioned guard)
        r = {"symbol": "STALE.X", "current_price": 100.0, "forecast_price_12m": 160.0, "expected_roi_12m": 0.6,
             "forecast_source": "provider_target", "warnings": "r12_ceiling:enforce:hi"}
        assert de._roi12_softcap(r) == 1 and r["forecast_price_12m"] < 135.0
        # soft cap OFF -> exact hard clamp to +/-0.30 (Fix-O parity)
        os.environ["TFB_PROVIDER_TARGET_SOFT_CAP"] = "0"
        for r0 in CEIL:
            r = copy.deepcopy(r0)
            assert de._roi12_softcap(r) == 1
            assert r["forecast_price_12m"] == round(r0["current_price"] * 1.30, 4) == HARD[r0["symbol"]]
            assert r["expected_roi_12m"] == round((r["forecast_price_12m"] - r0["current_price"]) / r0["current_price"], 6)
            again = copy.deepcopy(r)
            assert de._roi12_softcap(again) == 0 and again == r
    finally:
        _clean()


def test_t8_guards():
    _clean()
    try:
        _env(TNR, "enforce")
        _env(R12, "enforce")
        pct = {"symbol": "P", "current_price": 100.0, "forecast_price_12m": 130.0, "expected_roi_12m": 24.13}
        r = copy.deepcopy(pct)
        de._publish_tuple_seams(r)
        assert r == pct                                                       # percent domain: never scaled
        pct_pt = dict(pct, forecast_source="provider_target", forecast_price_12m=150.0, expected_roi_12m=50.0)
        r = copy.deepcopy(pct_pt)
        assert de._roi12_softcap(r) == 1 and r["expected_roi_12m"] == 50.0 and r["forecast_price_12m"] < 135.0
        for bad in ({"current_price": 0.0, "forecast_price_12m": 1.3, "expected_roi_12m": 0.1, "forecast_source": "provider_target"},
                    {"current_price": None, "forecast_price_12m": 1.3, "expected_roi_12m": 0.1, "forecast_source": "provider_target"},
                    {"current_price": 1.0, "forecast_price_12m": None, "expected_roi_12m": 0.1, "forecast_source": "provider_target"},
                    {"current_price": 1.0, "forecast_price_12m": 0.0, "expected_roi_12m": 0.1, "forecast_source": "provider_target"},
                    {"current_price": 1.0, "forecast_price_12m": float("nan"), "expected_roi_12m": 0.1, "forecast_source": "provider_target"},
                    {"current_price": 1.0, "forecast_price_12m": 1.3, "expected_roi_12m": float("nan"), "forecast_source": "provider_target"},
                    {"current_price": 1.0, "forecast_price_12m": 1.3, "expected_roi_12m": "junk", "forecast_source": ""},
                    {"current_price": "0.8116", "forecast_price_12m": "0.7788", "expected_roi_12m": "x"}):
            r = copy.deepcopy(bad)
            assert de._tuple_noise_release(r) == 0, bad
            assert de._roi12_softcap(r) in (0, 1)
            if bad.get("forecast_source") == "provider_target":
                assert de._roi12_softcap(copy.deepcopy(bad)) == 0 or math.isnan(float(bad["expected_roi_12m"]))
        assert de._tuple_noise_release("not a row") == 0 and de._roi12_softcap("not a row") == 0
        assert de._publish_tuple_seams("not a row") is None and de._publish_tuple_seams(None) is None
        # string numerics and list-typed warnings
        r = {"symbol": "S", "current_price": "0.8116", "forecast_price_12m": "0.7788", "expected_roi_12m": "-0.040448",
             "warnings": ["acquisition_status:success"]}
        assert de._tuple_noise_release(r) == 1 and r["expected_roi_12m"] == -0.040414
        assert r["warnings"] == "acquisition_status:success; tuple_noise:12m:round:enforce"
        r = dict(CEIL[0], warnings=["analyst_lkg:137h"])
        assert de._roi12_softcap(r) == 1 and r["forecast_price_12m"] == 143.134
        assert r["warnings"].endswith("r12_ceiling:enforce:hi")
    finally:
        _clean()


def test_t9_tags_substring_safe():
    tags = [de._R12_TAG, de._TNR_TAG]
    for mode in ("observe", "enforce"):
        for cls in ("hi", "band"):
            tags.append("%s:%s:%s" % (de._R12_TAG, mode, cls))
        for h in ("12m", "3m", "1m"):
            for cls in ("round", "drift"):
                tags.append("%s:%s:%s:%s" % (de._TNR_TAG, h, cls, mode))
            tags.append("%s:%s:precision_limited" % (de._TNR_TAG, h))
            tags.append("%s:%s:gap" % (de._TNR_TAG, h))
    for tag in tags:
        for bad in FORBIDDEN:
            assert bad not in tag, (tag, bad)
    assert de._R12_FIXO_TAG == "provider_target_12m_capped_to_phase_ii_ceiling"   # the Fix-O witness, read not emitted


def test_t10_wiring_and_version():
    src = inspect.getsource(de)
    assert src.count("_publish_tuple_seams(row)  # v5.152.0") == 1
    i = src.index("    _f7_settle_holdback(row)\n    _publish_tuple_seams(row)  # v5.152.0")
    j = src.index("    displayed = present_instrument_row(row)", i)
    assert 0 < j - i < 200, j - i
    assert src.count("_publish_tuple_seams(_r)  # v5.152.0") == 1
    i = src.index("            _apply_analyst_trend_block(_r)  # v5.85.0")
    j = src.index("            _publish_tuple_seams(_r)  # v5.152.0", i)
    assert 0 < j - i < 200, j - i
    # the Top_10 direct block is NOT touched (rows there project through _strict_project_row)
    k = src.index("                    _margin_publish_contract(_r)  # v5.151.0")
    assert "_publish_tuple_seams" not in src[k:k + 1200]
    assert src.count('"roi12_softcap": _roi12_softcap_mode(),') == 1
    assert src.count('"tuple_noise_release": _tuple_noise_mode(),') == 1
    assert src.count("r12_ceiling=%s") == 1
    assert src.count("tuple_noise=%s") == 1
    assert src.count("fc_tuple=%s") == 1 and src.count("margin_publish=%s") == 1     # P-102 / P-152 contracts intact
    assert tuple(int(x) for x in de.__version__.split(".")[:3]) >= (5, 152, 0)
    # every seam constant is a literal defined before use (import succeeded, so the module-level rule held)
    for name in ("_TNR_ENV", "_TNR_TAG", "_TNR_PRES_TOL_ABS", "_TNR_PRES_TOL_REL", "_TNR_PRICE_HALF_ULP",
                 "_TNR_ROI_HALF_ULP", "_TNR_MAX_NOISE", "_TNR_DRIFT_SLACK", "_R12_ENV", "_R12_TAG", "_R12_FIXO_TAG"):
        assert isinstance(getattr(de, name), (str, float)), name
    assert de._TNR_PRES_TOL_ABS == 0.000005 and de._TNR_PRES_TOL_REL == 0.000001
    assert de._TNR_DRIFT_SLACK == 0.0005 and de._TNR_MAX_NOISE == 0.001


def test_t11_forecast_pair_coherence_fixed_point():
    """With TFB_FORECAST_PAIR_COHERENCE on (off in production), the seams' output
    is a fixed point for _apply_forecast_pair_coherence: a snapped ROI or a
    lockstep (fp, roi) satisfies |cp(1+roi) - fp| <= 5e-7*cp << fp*0.0005."""
    _clean()
    try:
        _env(TNR, "enforce")
        _env(R12, "enforce")
        os.environ["TFB_FORECAST_PAIR_COHERENCE"] = "1"
        assert de._forecast_pair_coherence_enabled()
        for r0 in NOISE + CEIL + [BAND_LKG, NEG_LKG]:
            r = copy.deepcopy(r0)
            de._publish_tuple_seams(r)
            cp = r["current_price"]
            assert abs(cp * (1.0 + r["expected_roi_12m"]) - r["forecast_price_12m"]) <= 5e-7 * cp + 1e-12
            snap = copy.deepcopy(r)
            de._apply_forecast_pair_coherence(snap)
            assert snap == r, r0["symbol"]
            assert "display_pair_synced" not in (snap.get("warnings") or "")
    finally:
        _clean()


def test_t12_wrapper_order_b_then_a():
    _clean()
    try:
        _env(TNR, "enforce")
        _env(R12, "enforce")
        r = copy.deepcopy(CEIL[0])
        de._publish_tuple_seams(r)
        cp = r["current_price"]
        assert (r["forecast_price_12m"], r["expected_roi_12m"]) == CAPPED["AKAM.US"]
        assert r["expected_roi_12m"] == round((r["forecast_price_12m"] - cp) / cp, 6)
        assert _tags(r, "r12_ceiling:") == ["r12_ceiling:enforce:hi"] and not _tags(r, "tuple_noise:")
        again = copy.deepcopy(r)
        de._publish_tuple_seams(again)
        assert again == r
        _env(R12, None)                                                   # (b) off, (a) enforce: coherent uncapped tuple untouched
        r = copy.deepcopy(CEIL[0])
        de._publish_tuple_seams(r)
        assert r == CEIL[0]
        r = copy.deepcopy(NOISE[1])
        de._publish_tuple_seams(r)
        assert r["expected_roi_12m"] == SNAPPED["XXII.US"]
    finally:
        _clean()


TESTS = [test_t1_gate_explicit_words_only, test_t2_off_inert_both_envs, test_t3_noise_observe_tag_only,
         test_t4_noise_enforce_snaps_and_is_idempotent, test_t5_tolerance_mirror_and_bands,
         test_t6_ceiling_observe_tag_only, test_t7_ceiling_enforce_lockstep_idempotent_hardclamp, test_t8_guards,
         test_t9_tags_substring_safe, test_t10_wiring_and_version, test_t11_forecast_pair_coherence_fixed_point,
         test_t12_wrapper_order_b_then_a]

if __name__ == "__main__":
    import traceback
    fails = 0
    for i in range(3):
        for t in TESTS:
            try:
                t()
            except Exception:
                fails += 1
                print("FAIL", t.__name__, "(pass %d)" % (i + 1))
                traceback.print_exc()
    print("RESULT", "PASS" if not fails else "FAIL", "WP-3ab battery T1-T12 x3 (%d tests, engine %s)" % (len(TESTS), de.__version__))
    sys.exit(1 if fails else 0)
