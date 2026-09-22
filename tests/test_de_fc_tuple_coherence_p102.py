"""P-102 (data_engine_v2 v5.148.0) -- FC-TUPLE COHERENCE at the publish boundary.

The sheet publishes rows whose Expected ROI contradicts their own
(forecast price, current price) pair because expected_roi_* is derived once
and later target restores / price refreshes never re-derive it (2026-09-22
export: 180 rows, ML 34 / GM 146, all 12M). v5.148.0 runs
_fc_tuple_coherence(row) immediately BEFORE _apply_investability_gate at all
three publish boundaries.

  off (unset) -> inert, rows byte-identical
  observe     -> ONE tag per incoherent leg (fctuple_vintage:<h>:observe), values untouched
  enforce     -> expected_roi_<h> = round((fp-cp)/cp, 6) + :enforce tag; prices never
                 touched; percent-domain ROI never scaled; missing ROI left alone; idempotent

Run: python -m pytest -q tests/test_de_fc_tuple_coherence_p102.py   (or python tests/...)
"""
from __future__ import annotations

import copy
import inspect
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import core.data_engine_v2 as de  # noqa: E402

ENV = "TFB_FC_TUPLE_COHERENT"
FORBIDDEN = ("cap", "forecast", "target", "roi", "drop", "reject", "provider_target",
             "price_bar_stale", "xprovider_price_conflict")

# real 2026-09-22 export triples (12M leg): stored ROI vs implied
ROWS = [
    {"symbol": "4017.SR", "current_price": 36.38, "forecast_price_12m": 32.36, "expected_roi_12m": -0.107005},   # implied -11.05%
    {"symbol": "4142.SR", "current_price": 107.10, "forecast_price_12m": 88.03, "expected_roi_12m": -0.2062},    # implied -17.81%
    {"symbol": "2286.SR", "current_price": 3.88, "forecast_price_12m": 4.89, "expected_roi_12m": 0.2674},        # implied +26.03% (board seat)
    {"symbol": "SBAC.US", "current_price": 176.28, "forecast_price_12m": 226.4, "expected_roi_12m": 0.2843},     # coherent
    {"symbol": "KRP.US", "current_price": 14.44, "forecast_price_12m": 19.2, "expected_roi_12m": 0.3224,
     "forecast_price_3m": 15.6, "expected_roi_3m": 0.0803},                                                      # 12m ok (0.02pp), 3m ok
]


def _env(mode):
    if mode is None:
        os.environ.pop(ENV, None)
    else:
        os.environ[ENV] = mode


def _implied(r, h="12m"):
    return (r["forecast_price_%s" % h] - r["current_price"]) / r["current_price"]


def _tags(row):
    w = row.get("warnings") or ""
    return [p.strip() for p in str(w).split(";") if p.strip().startswith("fctuple_vintage:")]


def test_t1_gate_explicit_words_only():
    for raw, want in ((None, "off"), ("", "off"), ("1", "off"), ("true", "off"), ("on", "off"), ("yes", "off"),
                      ("off", "off"), ("observe", "observe"), (" ENFORCE ", "enforce"), ("garbage", "off")):
        _env(raw)
        assert de._fc_tuple_mode() == want, (raw, want)
        assert de.surface_gate_states().get("fc_tuple_coherent") == want
    _env(None)


def test_t2_off_inert():
    for raw in (None, "off", "1"):
        _env(raw)
        for r0 in ROWS:
            r = copy.deepcopy(r0)
            assert de._fc_tuple_coherence(r) == 0 and r == r0
    _env(None)


def test_t3_observe_tag_only():
    _env("observe")
    for r0 in ROWS:
        r = copy.deepcopy(r0)
        n = de._fc_tuple_coherence(r)
        incoherent = abs(_implied(r0) - r0["expected_roi_12m"]) > de._fc_tuple_tol(r0["current_price"])
        assert n == (1 if incoherent else 0), r0["symbol"]
        assert {k: v for k, v in r.items() if k != "warnings"} == r0
        if incoherent:
            assert _tags(r) == ["fctuple_vintage:12m:observe"]
            assert not any(f in _tags(r)[0] for f in FORBIDDEN)
        else:
            assert not _tags(r)
    assert de._fc_tuple_coherence(copy.deepcopy(ROWS[0])) == 1 and de._fc_tuple_coherence(copy.deepcopy(ROWS[3])) == 0
    _env(None)


def test_t4_enforce_repairs_and_is_idempotent():
    _env("enforce")
    for r0 in ROWS:
        r = copy.deepcopy(r0)
        n = de._fc_tuple_coherence(r)
        incoherent = abs(_implied(r0) - r0["expected_roi_12m"]) > de._fc_tuple_tol(r0["current_price"])
        assert n == (1 if incoherent else 0)
        assert r["current_price"] == r0["current_price"] and r["forecast_price_12m"] == r0["forecast_price_12m"]
        if incoherent:
            assert r["expected_roi_12m"] == round(_implied(r0), 6)
            assert _tags(r) == ["fctuple_vintage:12m:enforce"]
        else:
            assert r == r0
        again = copy.deepcopy(r)
        assert de._fc_tuple_coherence(again) == 0 and again == r      # second pass: 0 residual
    _env(None)


def test_t5_guards_percent_domain_missing_and_zero_price():
    _env("enforce")
    r = {"symbol": "X", "current_price": 100.0, "forecast_price_12m": 130.0, "expected_roi_12m": 24.13}   # percent domain
    assert de._fc_tuple_coherence(r) == 0 and r["expected_roi_12m"] == 24.13 and not _tags(r)
    r = {"symbol": "X", "current_price": 100.0, "forecast_price_12m": 130.0}                               # missing ROI -> backfill's job
    assert de._fc_tuple_coherence(r) == 0 and "expected_roi_12m" not in r
    r = {"symbol": "X", "current_price": 0.0, "forecast_price_12m": 130.0, "expected_roi_12m": 0.1}          # zero price
    assert de._fc_tuple_coherence(r) == 0 and r["expected_roi_12m"] == 0.1
    r = {"symbol": "X", "current_price": 100.0, "forecast_price_12m": None, "expected_roi_12m": 0.1}         # no forecast
    assert de._fc_tuple_coherence(r) == 0 and r["expected_roi_12m"] == 0.1
    assert de._fc_tuple_coherence("not a row") == 0
    _env(None)


def test_t6_tolerance_boundary():
    _env("observe")
    tol = de._fc_tuple_tol(25.0)
    assert abs(tol - 0.0006) < 1e-12
    implied = (27.5 - 25.0) / 25.0                        # 0.10
    r = {"current_price": 25.0, "forecast_price_12m": 27.5, "expected_roi_12m": implied - 0.0005}
    assert de._fc_tuple_coherence(r) == 0
    r = {"current_price": 25.0, "forecast_price_12m": 27.5, "expected_roi_12m": implied - 0.0007}
    assert de._fc_tuple_coherence(r) == 1
    assert de._fc_tuple_tol(0.0) == 0.0005 or de._fc_tuple_tol(0.0) > 0.0005   # never raises on a zero price
    _env(None)


def test_t7_wiring_and_version():
    src = inspect.getsource(de)
    # three live seams, each immediately before the gate call
    assert src.count("_fc_tuple_coherence(row)  # v5.148.0") == 1
    assert src.count("_fc_tuple_coherence(_r)  # v5.148.0") == 2
    for seam, gate in (("    _fc_tuple_coherence(row)  # v5.148.0", "    _apply_investability_gate(row)  # v5.78.0"),
                       ("            _fc_tuple_coherence(_r)  # v5.148.0", "            _apply_investability_gate(_r)  # v5.78.0"),
                       ("                    _fc_tuple_coherence(_r)  # v5.148.0", "                    _apply_investability_gate(_r)\n")):
        i = src.index(seam)
        j = src.index(gate, i)
        assert 0 < j - i < 200, (seam, j - i)
    assert src.count('"fc_tuple_coherent": _fc_tuple_mode(),') == 1
    assert src.count("fc_tuple=%s") == 1
    assert tuple(int(x) for x in de.__version__.split(".")[:3]) >= (5, 148, 0)


TESTS = [test_t1_gate_explicit_words_only, test_t2_off_inert, test_t3_observe_tag_only,
         test_t4_enforce_repairs_and_is_idempotent, test_t5_guards_percent_domain_missing_and_zero_price,
         test_t6_tolerance_boundary, test_t7_wiring_and_version]

if __name__ == "__main__":
    import traceback
    fails = 0
    for t in TESTS:
        try:
            t()
            print("PASS", t.__name__)
        except Exception:
            fails += 1
            print("FAIL", t.__name__)
            traceback.print_exc()
    print("RESULT", "PASS" if not fails else "FAIL", "%d/%d" % (len(TESTS) - fails, len(TESTS)))
    sys.exit(1 if fails else 0)
