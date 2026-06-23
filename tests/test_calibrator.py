# -*- coding: utf-8 -*-
"""
Tests for scripts/calibrator.py v1.0.0 — the pure pipeline (parse -> join ->
engine), exercised on synthetic Signal_History / Performance_Log grids. No
Sheets access is required, so these run anywhere.

Run: pytest tests/test_calibrator.py   (or: python tests/test_calibrator.py)
"""
import datetime as dt
import json
import math

try:
    from scripts import calibrator as C
except Exception:
    import calibrator as C  # standalone fallback


def approx(a, b, tol=1e-6):
    return abs(a - b) <= tol


# ----- grid builder: perfectly/over-confident synthetic data -----------------

def build_grids(specs, date="2026-01-01", horizon="1M", action="BUY"):
    """specs: list of (reliability_pct, n_claims, n_hits). Unique symbol per claim."""
    sig = [["symbol", "date", "action", "reliability"]]
    perf = [["symbol", "date", "horizon", "outcome"]]
    i = 0
    for prob_pct, n, hits in specs:
        for j in range(n):
            sym = f"S{i:04d}"
            i += 1
            sig.append([sym, date, action, prob_pct])
            perf.append([sym, date, horizon, 1 if j < hits else 0])
    return sig, perf


# ----- header resolution / parsing -------------------------------------------

def test_header_resolution_aliases_and_case():
    sig, perf = build_grids([(70, 2, 1)])
    rows = C.parse_table(sig, C.SIGNAL_HISTORY_COLUMNS, C.SIGNAL_HISTORY_REQUIRED)
    assert len(rows) == 2 and "claimed_reliability" in rows[0]


def test_missing_required_header_raises():
    bad = [["symbol", "date"], ["S1", "2026-01-01"]]  # no action/reliability
    try:
        C.parse_table(bad, C.SIGNAL_HISTORY_COLUMNS, C.SIGNAL_HISTORY_REQUIRED)
        assert False, "should have raised"
    except ValueError:
        pass


# ----- pure unit helpers ------------------------------------------------------

def test_normalize_reliability():
    assert approx(C.normalize_reliability(75), 0.75)
    assert approx(C.normalize_reliability(0.8), 0.8)
    assert approx(C.normalize_reliability("90%"), 0.9)
    assert C.normalize_reliability("") is None


def test_classify_direction():
    assert C.classify_direction("BUY") == 1
    assert C.classify_direction("sell") == -1
    assert C.classify_direction("HOLD") == 0
    assert C.classify_direction("frobnicate") is None


def test_derive_outcome_paths():
    assert C.derive_outcome({"realized_return": 0.05}, 1) == 1   # bullish, up
    assert C.derive_outcome({"realized_return": 0.05}, -1) == 0  # bearish, up
    assert C.derive_outcome({"realized_return": -0.03}, -1) == 1  # bearish, down
    assert C.derive_outcome({"realized_outcome": 1}, 1) == 1     # precomputed
    assert C.derive_outcome({"realized_outcome": "win"}, 1) == 1
    assert C.derive_outcome({"realized_outcome": "miss"}, 1) == 0
    assert C.derive_outcome({"entry_price": 100, "exit_price": 110}, 1) == 1


# ----- join -------------------------------------------------------------------

def test_join_neutral_excluded_and_one_to_many():
    sig = [["symbol", "date", "action", "reliability"],
           ["AAA", "2026-01-01", "BUY", 70],
           ["BBB", "2026-01-01", "HOLD", 60]]      # neutral -> dropped
    perf = [["symbol", "date", "horizon", "outcome"],
            ["AAA", "2026-01-01", "1M", 1],
            ["AAA", "2026-01-01", "3M", 0],
            ["AAA", "2026-01-01", "12M", 1]]        # one claim -> 3 horizons
    s = C.parse_table(sig, C.SIGNAL_HISTORY_COLUMNS, C.SIGNAL_HISTORY_REQUIRED)
    p = C.parse_table(perf, C.PERFORMANCE_LOG_COLUMNS, C.PERFORMANCE_LOG_REQUIRED)
    joined = C.join_claims_outcomes(s, p)
    assert len(joined) == 3                      # BBB excluded, AAA x3 horizons
    assert all(r["symbol"] == "AAA" for r in joined)
    assert {r["horizon"] for r in joined} == {"1M", "3M", "12M"}


def test_join_unmatched_dropped():
    sig = [["symbol", "date", "action", "reliability"],
           ["ZZZ", "2026-01-01", "BUY", 70]]
    perf = [["symbol", "date", "horizon", "outcome"]]  # no rows
    s = C.parse_table(sig, C.SIGNAL_HISTORY_COLUMNS, C.SIGNAL_HISTORY_REQUIRED)
    p = C.parse_table(perf, C.PERFORMANCE_LOG_COLUMNS, C.PERFORMANCE_LOG_REQUIRED)
    assert C.join_claims_outcomes(s, p) == []


# ----- engine: insufficiency / maturity gating -------------------------------

def test_insufficient_below_min_sample():
    sig, perf = build_grids([(70, 10, 7)])  # only 10 claims
    s = C.parse_table(sig, C.SIGNAL_HISTORY_COLUMNS, C.SIGNAL_HISTORY_REQUIRED)
    p = C.parse_table(perf, C.PERFORMANCE_LOG_COLUMNS, C.PERFORMANCE_LOG_REQUIRED)
    joined = C.join_claims_outcomes(s, p)
    rep = C.CalibrationEngine(as_of=dt.date(2026, 7, 18), min_sample=50).run(joined)
    assert rep["horizons"]["1M"]["verdict"] == "INSUFFICIENT_DATA"
    assert rep["status"] == "NOT_YET_MEASURABLE"


def test_next_measurable_date_when_pending():
    # 60 claims (>= min) but as_of too early -> not matured -> pending date set
    sig, perf = build_grids([(70, 60, 42)], date="2026-06-01")
    s = C.parse_table(sig, C.SIGNAL_HISTORY_COLUMNS, C.SIGNAL_HISTORY_REQUIRED)
    p = C.parse_table(perf, C.PERFORMANCE_LOG_COLUMNS, C.PERFORMANCE_LOG_REQUIRED)
    joined = C.join_claims_outcomes(s, p)
    rep = C.CalibrationEngine(as_of=dt.date(2026, 6, 10), min_sample=50).run(joined)
    h = rep["horizons"]["1M"]
    assert h["verdict"] == "INSUFFICIENT_DATA"
    assert h["next_measurable_date"] == "2026-07-01"  # 2026-06-01 + 30d


def test_12m_not_matured_even_with_data():
    sig, perf = build_grids([(70, 80, 56)], date="2026-01-01", horizon="12M")
    s = C.parse_table(sig, C.SIGNAL_HISTORY_COLUMNS, C.SIGNAL_HISTORY_REQUIRED)
    p = C.parse_table(perf, C.PERFORMANCE_LOG_COLUMNS, C.PERFORMANCE_LOG_REQUIRED)
    joined = C.join_claims_outcomes(s, p)
    rep = C.CalibrationEngine(as_of=dt.date(2026, 7, 18), min_sample=50).run(joined)
    assert rep["horizons"]["12M"]["verdict"] == "INSUFFICIENT_DATA"  # 365d not elapsed
    assert rep["horizons"]["12M"]["matured_usable"] == 0


# ----- engine: calibration quality -------------------------------------------

def test_well_calibrated():
    # claimed prob == realized rate in every bucket -> calibrated
    specs = [(10, 40, 4), (30, 40, 12), (50, 40, 20), (70, 40, 28), (90, 40, 36)]
    sig, perf = build_grids(specs)
    s = C.parse_table(sig, C.SIGNAL_HISTORY_COLUMNS, C.SIGNAL_HISTORY_REQUIRED)
    p = C.parse_table(perf, C.PERFORMANCE_LOG_COLUMNS, C.PERFORMANCE_LOG_REQUIRED)
    joined = C.join_claims_outcomes(s, p)
    h = C.CalibrationEngine(as_of=dt.date(2026, 7, 18), min_sample=50).run(joined)["horizons"]["1M"]
    assert h["verdict"] == "MEASURED"
    assert h["n"] == 200
    assert abs(h["calibration_gap"]) < 0.02
    assert h["ece"] < 0.05
    assert h["well_calibrated"] is True


def test_overconfident_flagged():
    # claims 90% but only 50% happen -> large gap, significant miscalibration
    specs = [(90, 200, 100)]
    sig, perf = build_grids(specs)
    s = C.parse_table(sig, C.SIGNAL_HISTORY_COLUMNS, C.SIGNAL_HISTORY_REQUIRED)
    p = C.parse_table(perf, C.PERFORMANCE_LOG_COLUMNS, C.PERFORMANCE_LOG_REQUIRED)
    joined = C.join_claims_outcomes(s, p)
    h = C.CalibrationEngine(as_of=dt.date(2026, 7, 18), min_sample=50).run(joined)["horizons"]["1M"]
    assert h["verdict"] == "MEASURED"
    assert approx(h["calibration_gap"], 0.4, 0.02)
    assert h["miscalibration_significant"] is True
    assert h["well_calibrated"] is False


# ----- report shape -----------------------------------------------------------

def test_report_json_serializable_and_framed():
    specs = [(50, 60, 30)]
    sig, perf = build_grids(specs)
    s = C.parse_table(sig, C.SIGNAL_HISTORY_COLUMNS, C.SIGNAL_HISTORY_REQUIRED)
    p = C.parse_table(perf, C.PERFORMANCE_LOG_COLUMNS, C.PERFORMANCE_LOG_REQUIRED)
    joined = C.join_claims_outcomes(s, p)
    rep = C.CalibrationEngine(as_of=dt.date(2026, 7, 18), min_sample=50).run(joined)
    json.dumps(rep)                              # must not raise
    assert "NOT hit-rate" in rep["metric_meaning"]
    assert "Directional claims only" in rep["scope_note"]
    # console formatter must not raise
    assert isinstance(C.format_report_text(rep), str)


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and callable(v)]
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
