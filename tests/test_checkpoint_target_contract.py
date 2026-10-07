"""Future checkpoint price/ROI tuples and strict historical calibration.

All prices and identities are synthetic. The creation regression exercises
the real engine forecast producer and tracker recorder; only Sheets I/O is
recorded in memory. Historical fixtures explicitly retain the old units.
"""
import asyncio
import copy
import importlib.util
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture(scope="module")
def tracker():
    path = Path(__file__).parents[1] / "scripts" / "track_performance.py"
    spec = importlib.util.spec_from_file_location("tp_checkpoint_contract", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch):
    for key in ("TFB_PERF_TARGET_UNIT_SENTRY", "TFB_S1_ZERO_BASELINE",
                "TFB_S1_CAL_MIN_SAMPLE", "TFB_S1_CAL_BAND_PP"):
        monkeypatch.delenv(key, raising=False)


def _app(mod):
    app = object.__new__(mod.PerformanceTrackerApp)
    app.args = SimpleNamespace(horizons=["1W", "2W", "1M", "3M"])
    return app


def _record(mod, horizon, *, day=0, entry=100.0, price=107.0,
            roi=0.07, realized=0.07, identity="SYNTH.US"):
    stamp = datetime(2026, 9, 1, tzinfo=timezone(timedelta(hours=3))) + timedelta(days=day)
    return mod.PerformanceRecord(
        record_id=f"synthetic-{identity}-{horizon.value}-{day}", symbol=identity,
        horizon=horizon, date_recorded=stamp, entry_price=entry,
        entry_recommendation=mod.RecommendationType.HOLD, entry_score=70,
        entry_risk_bucket="LOW", entry_confidence="HIGH", origin_tab="test",
        target_price=price, target_roi=roi,
        target_date=stamp + timedelta(days=horizon.days),
        status=mod.PerformanceStatus.MATURED, realized_roi=realized,
        current_price=entry)


def _historical_pair(mod, day=0):
    # Historical 1W fraction ROI was misread as pp when its price was made.
    return [
        _record(mod, mod.HorizonType.WEEK_1, day=day,
                price=100.0 * (1.0 + 0.07 / 100.0)),
        _record(mod, mod.HorizonType.MONTH_1, day=day,
                price=130.0, roi=0.3, realized=None),
    ]


@pytest.mark.parametrize("mode", ["off", "observe", "enforce"])
def test_real_engine_producer_to_checkpoint_recorder(tracker, monkeypatch, mode):
    from core import data_engine_v2

    monkeypatch.setenv("TFB_PERF_TARGET_UNIT_SENTRY", mode)
    monkeypatch.setenv("TFB_TARGET_BLOCK_LKG", "0")
    row = {"symbol": "SYNTH.US", "current_price": 100.0,
           "forecast_source": "provider_target", "forecast_price_12m": 124.0}
    data_engine_v2._phase_ii_quality_forecast(row)
    assert 0 < row["expected_roi_1m"] < 1  # the real producer returns a fraction
    assert row["forecast_price_1m"] > row["current_price"]
    original = copy.deepcopy(row)
    records = asyncio.run(_app(tracker).record_from_top10([], rows=[row]))
    assert row == original
    assert len(records) == 4
    for rec in records[:2]:
        truth_pp = (row["forecast_price_1m"] / 100.0 - 1.0) * 100 * rec.horizon.days / 30
        assert rec.target_roi == pytest.approx(truth_pp)
        assert rec.target_price == pytest.approx(100 * (1 + truth_pp / 100))
        assert rec.to_dict()["target_roi_pct"] == pytest.approx(truth_pp)
    # Existing monthly creation is still governed by the original sentry.
    expected_month = row["expected_roi_1m"] * (100 if mode == "enforce" else 1)
    assert records[2].target_roi == pytest.approx(expected_month)
    assert records[2].target_price == row["forecast_price_1m"]


@pytest.mark.parametrize("mode", ["off", "observe", "enforce"])
@pytest.mark.parametrize("raw_roi", [0.3, 30.0, -999.0, None, float("nan"), float("inf")])
def test_raw_roi_never_controls_price_witness(tracker, monkeypatch, mode, raw_roi):
    monkeypatch.setenv("TFB_PERF_TARGET_UNIT_SENTRY", mode)
    row = {"forecast_price_1m": 130.0, "expected_roi_1m": raw_roi}
    before = copy.copy(row)
    for hz in (tracker.HorizonType.WEEK_1, tracker.HorizonType.WEEK_2):
        price, roi = _app(tracker)._derive_target(row, hz, 100.0)
        assert roi == pytest.approx(hz.days)
        assert price == pytest.approx(100 + hz.days)
    assert row == before


@pytest.mark.parametrize("forecast", [80.0, 100.0, 130.0, 10000.0])
def test_negative_flat_and_large_finite_forecasts(tracker, forecast):
    for hz in (tracker.HorizonType.WEEK_1, tracker.HorizonType.WEEK_2):
        price, roi = _app(tracker)._derive_target({"forecast_price_1m": forecast}, hz, 100.0)
        assert roi == pytest.approx((forecast / 100 - 1) * 100 * hz.days / 30)
        assert price == pytest.approx(100 * (1 + roi / 100))
        assert math.isfinite(price) and price > 0


@pytest.mark.parametrize("entry,forecast", [
    (100.0, None), (100.0, 0), (100.0, -1), (100.0, float("nan")),
    (100.0, float("inf")), (0, 130), (-1, 130), (None, 130),
    (float("nan"), 130), (float("inf"), 130), (1e-308, 1e308),
])
def test_missing_nonfinite_or_unrepresentable_price_witness(tracker, entry, forecast):
    row = {"forecast_price_1m": forecast, "expected_roi_1m": 0.3}
    assert _app(tracker)._derive_target(row, tracker.HorizonType.WEEK_1, entry) == (0.0, 0.0)


def test_finite_extreme_prices_do_not_overflow_tuple(tracker):
    price, roi = _app(tracker)._derive_target(
        {"forecast_price_1m": 1e308}, tracker.HorizonType.WEEK_2, 1e308)
    assert price == 1e308 and roi == 0.0


@pytest.mark.parametrize("invalid_kind", ["different_entry", "duplicate", "contradictory", "invalid_duplicate"])
def test_historical_witness_must_be_unique_and_share_entry(tracker, invalid_kind):
    records = _historical_pair(tracker)
    if invalid_kind == "different_entry":
        records[1].entry_price = 200.0
        records[1].target_price = 260.0  # same ROI cannot disguise the different entry
    else:
        duplicate = copy.deepcopy(records[1])
        duplicate.record_id += "-duplicate"
        if invalid_kind == "contradictory":
            duplicate.target_price = 160.0
        elif invalid_kind == "invalid_duplicate":
            duplicate.target_price = float("nan")
        records.append(duplicate)
    for order in (records, list(reversed(records))):
        rep = tracker._s1_unit_sentry_measure(order)
        assert rep["n"] == 0
        assert rep["counts"]["unresolved"] == 1
        assert rep["unresolved_why"] == {"no_sibling": 0, "mismatch": 1}


def test_historical_measurement_corrects_only_in_memory(tracker, monkeypatch):
    records = sum((_historical_pair(tracker, day) for day in range(20)), [])
    snapshot = [r.to_dict() for r in records]
    monkeypatch.setenv("TFB_PERF_TARGET_UNIT_SENTRY", "observe")
    observed = tracker.s1_checkpoint_calibration(records)
    assert observed["state"] == "PASS"
    assert observed["mean_abs_error_pp"] == 0.0
    assert observed["unit_sentry"]["mean_abs_error_pp"] == 6.93
    monkeypatch.setenv("TFB_PERF_TARGET_UNIT_SENTRY", "enforce")
    enforced = tracker.s1_checkpoint_calibration(records)
    assert enforced["n"] == 20
    assert enforced["mean_abs_error_pp"] == 6.93
    assert enforced["unit_sentry"]["legacy"]["mean_abs_error_pp"] == 0.0
    assert [r.to_dict() for r in records] == snapshot


def _assert_unavailable(rep):
    assert rep["state"] == "PENDING" and rep["n"] == 0
    assert rep["mean_abs_error_pp"] is None
    assert rep["mean_signed_error_pp"] is None
    assert rep["zero_mae_pp"] is None and rep["by_horizon"] == {}
    assert rep["unit_sentry"]["legacy"]["state"] == "PASS"
    assert rep["unit_sentry"]["legacy"]["n"] == 20
    assert rep["band_pp"] == 10 and rep["min_sample"] == 20


def test_no_witness_cannot_republish_legacy_pass(tracker, monkeypatch):
    records = [_historical_pair(tracker, day)[0] for day in range(20)]
    off = tracker.s1_checkpoint_calibration(records)
    monkeypatch.setenv("TFB_PERF_TARGET_UNIT_SENTRY", "observe")
    observed = tracker.s1_checkpoint_calibration(records)
    for key in off:
        assert observed[key] == off[key]
    monkeypatch.setenv("TFB_PERF_TARGET_UNIT_SENTRY", "enforce")
    _assert_unavailable(tracker.s1_checkpoint_calibration(records))


def test_real_measurement_exception_cannot_republish_legacy_pass(tracker, monkeypatch):
    records = sum((_historical_pair(tracker, day) for day in range(20)), [])

    class InterruptedRead:
        def __init__(self):
            self.reads = 0

        def __iter__(self):
            self.reads += 1
            if self.reads > 1:
                raise RuntimeError("synthetic measurement interruption")
            return iter(records)

    monkeypatch.setenv("TFB_PERF_TARGET_UNIT_SENTRY", "enforce")
    rep = tracker.s1_checkpoint_calibration(InterruptedRead())
    _assert_unavailable(rep)
    assert rep["unit_sentry"]["error"] == "unit_sentry_error:RuntimeError"


def test_assembly_exception_cannot_republish_legacy_pass(tracker, monkeypatch):
    records = sum((_historical_pair(tracker, day) for day in range(20)), [])
    out = tracker._s1_checkpoint_calibration_legacy(records)
    # A malformed measurement result tests the actual assembly guard.
    monkeypatch.setattr(tracker, "_s1_unit_sentry_measure", lambda rows: {
        "n": 20, "mean_abs_error_pp": None, "mean_signed_error_pp": 0,
    })
    rep = tracker._s1_unit_sentry_apply(out, records, "enforce")
    _assert_unavailable(rep)
    assert "enforce_error:TypeError" in rep["detail"]


def test_resolved_nonfinite_realized_value_cannot_be_published(tracker, monkeypatch):
    records = _historical_pair(tracker)
    records[0].realized_roi = float("inf")
    monkeypatch.setenv("TFB_PERF_TARGET_UNIT_SENTRY", "enforce")
    rep = tracker.s1_checkpoint_calibration(records)
    assert rep["state"] == "PENDING" and rep["n"] == 0
    assert rep["mean_abs_error_pp"] is None
    assert rep["unit_sentry"]["error"] == "unit_sentry_error:ValueError"


def test_publisher_writes_pending_and_blank_unknown_metrics(tracker, monkeypatch):
    class Recorder:
        def __init__(self):
            self.updates = []

        def update(self, *args, **kwargs):
            self.updates.append((args, kwargs))

        def append_row(self, *args, **kwargs):
            pass

    tabs = {}
    sheet = SimpleNamespace(worksheet=lambda name: tabs.setdefault(name, Recorder()))
    app = _app(tracker)
    app.store = SimpleNamespace(
        sheet=sheet, is_available=lambda: True,
        backoff=SimpleNamespace(execute_sync=lambda fn, *a, **kw: fn(*a, **kw)))
    monkeypatch.setenv("TFB_PERF_TARGET_UNIT_SENTRY", "enforce")
    monkeypatch.setenv("TFB_S1_ZERO_BASELINE", "1")
    records = [_historical_pair(tracker, day)[0] for day in range(20)]
    assert app._publish_s1_calibration(records)
    args, _ = tabs[tracker.S1_CAL_TAB].updates[0]
    header, values = args[1]
    row = dict(zip(header, values))
    assert row["State"] == "PENDING" and row["N Checkpoints"] == 0
    for key in ("Mean Abs Error (pp)", "Mean Signed Error (pp)", "Zero MAE (pp)"):
        assert row[key] == ""
    assert row["Writer Version"] == "6.42.1"
    assert "zero_mae=" not in row["Detail"]
