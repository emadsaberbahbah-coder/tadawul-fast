"""Independent, labelled outcome evidence; no historical row mutation.

Set TFB_PERF_SUMMARY_REF=70a88fa to run these regressions against the baseline.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import types

import pytest

from scripts import track_performance as current


if os.environ.get("TFB_PERF_SUMMARY_REF"):
    tp = types.ModuleType("baseline_performance_summary")
    tp.__file__ = current.__file__
    sys.modules[tp.__name__] = tp
    source = subprocess.run(
        ["git", "show", os.environ["TFB_PERF_SUMMARY_REF"] + ":scripts/track_performance.py"],
        cwd=Path(current.__file__).parents[1], capture_output=True, text=True, check=True).stdout
    exec(compile(source, tp.__file__, "exec"), tp.__dict__)
else:
    tp = current


def record(symbol, roi, call="BUY", status="matured", rid=None):
    now = datetime(2026, 10, 1, 12, tzinfo=timezone.utc)
    return tp.PerformanceRecord(
        record_id=rid or symbol, symbol=symbol, horizon=tp.HorizonType.MONTH_1,
        date_recorded=now, entry_price=10, entry_recommendation=tp.RecommendationType(call),
        entry_score=75, entry_risk_bucket="LOW", entry_confidence="HIGH", origin_tab="test",
        target_price=11, target_roi=10, target_date=now + timedelta(days=30),
        status=tp.PerformanceStatus(status), realized_roi=roi)


def test_summary_deduplicates_only_valid_matured_outcomes_without_mutating_records():
    records = [record("A.US", None, status="active"), record("A.US", 5, rid="valid"),
               record("A.US", -3, rid="duplicate"), record("B.US", float("nan")),
               record("B.US", -2, rid="later-valid"), record("C.US", 0)]
    before = [(r.record_id, r.status, r.realized_roi, r.notes) for r in records]
    summary = tp.PerformanceAnalyzer().analyze(records)
    assert summary.matured_records == 3
    assert (summary.duplicate_matured_excluded, summary.invalid_matured_excluded) == (1, 1)
    assert (summary.wins, summary.losses, summary.breakeven) == (1, 1, 1)
    assert summary.avg_roi == pytest.approx(1)
    assert before == [(r.record_id, r.status, r.realized_roi, r.notes) for r in records]


def test_directional_and_long_price_rates_have_explicit_different_denominators():
    summary = tp.PerformanceAnalyzer().analyze([
        record("B.US", 4), record("S.US", -4, "SELL"),
        record("H.US", 3, "HOLD"), record("Z.US", 0)])
    assert (summary.wins, summary.losses, summary.breakeven, summary.neutral_records) == (2, 0, 1, 1)
    assert summary.win_rate == 100
    assert (summary.price_positive, summary.price_negative, summary.price_breakeven) == (2, 1, 1)
    assert summary.price_positive_rate == pytest.approx(200 / 3)


def test_malformed_loaded_outcome_does_not_become_breakeven():
    invalid = record("BAD.US", 0)
    invalid.realized_roi_source_valid = False
    summary = tp.PerformanceAnalyzer().analyze([invalid, record("A.US", 5)])
    assert summary.matured_records == 1
    assert summary.invalid_matured_excluded == 1 and summary.breakeven == 0


def test_json_win_rate_interval_uses_same_unique_decided_cohort():
    records = [record("A.US", 3), record("A.US", -4, rid="duplicate"), record("B.US", -2)]
    analyzer = tp.PerformanceAnalyzer()
    result = json.loads(tp.ReportGenerator(analyzer).generate_json(records, analyzer.analyze(records)))
    assert result["summary"]["win_rate"] == 50
    assert result["confidence_intervals"]["win_rate"]["observed"] == .5
    assert len(result["records"]) == 3  # raw evidence remains exported


def test_advisory_calibration_uses_the_same_unique_outcome_cohort():
    records = [record("A.US", 3), record("A.US", -4, rid="duplicate"), record("B.US", -2)]
    result = tp.ReliabilityCalibrator().calibrate(records)
    assert result.matured_records == result.decided_records == 2
    assert result.duplicate_matured_excluded == 1
    assert result.by_recommendation[0].realized_win_rate == 50


def test_panel_discloses_directional_denominator_and_retains_13_by_5_footprint():
    class Backoff:
        def execute_sync(self, function):
            return function()
    class Worksheet:
        def batch_clear(self, ranges):
            assert ranges == ["A1:E4", "AG1:AK13"]
        def update(self, *, values, range_name):
            self.values = values
            assert range_name == "AG1:AK13"
    store = object.__new__(tp.PerformanceStore)
    store.ws, store.backoff = Worksheet(), Backoff()
    summary = tp.PerformanceAnalyzer().analyze([record("H.US", 4, "HOLD")])
    assert store.update_summary(summary)
    rows = store.ws.values
    assert len(rows) == 13 and all(len(row) == 5 for row in rows)
    assert rows[6][0:2] == ["Directional Win Rate", "unavailable"]
    assert rows[6][3:] == ["Denominator: directional wins + losses", 0]
    assert rows[4][3:] == ["HOLD / neutral excluded", 1]
    assert rows[8][3] == "Price-positive rate (nonzero returns)"


def test_fallback_median_is_average_of_the_middle_pair(monkeypatch):
    monkeypatch.setattr(tp, "NUMPY_AVAILABLE", False)
    summary = tp.PerformanceAnalyzer().analyze([record("A.US", 1), record("B.US", 3)])
    assert summary.median_roi == 2


def test_both_workflow_recorders_default_to_canonical_targets_with_explicit_rollback():
    root = Path(current.__file__).parents[1]
    for path in (".github/workflows/daily_sync.yml", ".github/workflows/track_performance.yml"):
        source = (root / path).read_text()
        assert "TRACK_CANONICAL_TARGETS: ${{ vars.TRACK_CANONICAL_TARGETS || '1' }}" in source


def checkpoint(symbol, *, proven):
    item = record(symbol, 10)
    item.horizon = tp.HorizonType.WEEK_1
    item.target_date = item.date_recorded + timedelta(days=7)
    if proven:
        item.notes = tp._TARGET_SCHEMA_TAG + json.dumps({
            "version": 2, "horizon": "1W", "roi_unit": "percent_points",
            "basis": "forecast_price", "producer_version": tp.SCRIPT_VERSION}) + "]"
    return item


def test_s1_enforce_rejects_legacy_inferred_units_that_observe_can_pass(monkeypatch):
    records = []
    for i in range(20):
        cp = checkpoint(f"LEGACY{i}.US", proven=False)
        sibling = record(cp.symbol, None, status="active")
        sibling.target_price = 10 * (1 + .1 * 30 / 7)
        # The old writer can fabricate a self-consistent target from raw ROI.
        # An unversioned sibling does not establish independent provenance.
        records.extend((cp, sibling))
    before = [(r.key, r.notes, r.target_roi, r.target_price, r.realized_roi) for r in records]
    monkeypatch.setenv("TFB_PERF_TARGET_UNIT_SENTRY", "observe")
    assert tp.s1_checkpoint_calibration(records)["state"] == "PASS"
    monkeypatch.setenv("TFB_PERF_TARGET_UNIT_SENTRY", "enforce")
    result = tp.s1_checkpoint_calibration(records)
    assert result["state"] == "PENDING" and result["n"] == 0
    assert result["mean_abs_error_pp"] is None
    assert result["unit_sentry"]["unresolved_why"] == {"unproven_forecast_provenance": 20}
    assert before == [(r.key, r.notes, r.target_roi, r.target_price, r.realized_roi) for r in records]


def test_s1_enforce_allows_proven_canonical_cohort_at_unchanged_sample_and_band(monkeypatch):
    monkeypatch.setenv("TFB_PERF_TARGET_UNIT_SENTRY", "enforce")
    monkeypatch.delenv("TFB_S1_CAL_MIN_SAMPLE", raising=False)
    monkeypatch.delenv("TFB_S1_CAL_BAND_PP", raising=False)
    records = [checkpoint(f"PROVEN{i}.US", proven=True) for i in range(20)]
    assert tp.s1_checkpoint_calibration(records[:19])["state"] == "PENDING"
    result = tp.s1_checkpoint_calibration(records)
    assert result["state"] == "PASS" and result["n"] == result["min_sample"] == 20
    assert result["mean_abs_error_pp"] == 0 and result["band_pp"] == 10
    assert result["unit_sentry"]["basis"] == "canonical_forecast_provenance"
    records[0].realized_roi = -300
    assert tp.s1_checkpoint_calibration(records)["state"] == "FAIL"


def test_s1_enforce_quarantines_duplicate_and_malformed_canonical_targets(monkeypatch):
    monkeypatch.setenv("TFB_PERF_TARGET_UNIT_SENTRY", "enforce")
    items = [checkpoint(f"VALID{i}.US", proven=True) for i in range(20)]
    duplicate = checkpoint(items[0].symbol, proven=True)
    duplicate.record_id = "later-copy"
    items.append(duplicate)
    items[1].target_roi = .1  # contradicts the prospective percentage-point receipt
    result = tp.s1_checkpoint_calibration(items)
    assert result["state"] == "PENDING" and result["n"] == 18
    assert result["unit_sentry"]["unresolved_why"] == {
        "duplicate_matured_key": 2, "schema_price_mismatch": 1}

