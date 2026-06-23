"""
tests/test_harness_ic.py
========================
Tests for the v6.12.0 Information Coefficient wiring in scripts/track_performance.py.

The IC metric is disclosure-only and env-gated (TFB_HARNESS_IC, default OFF). These
tests assert three things:

  1. OFF path is inert  — IC fields stay None, no IC line prints, and every other
     calibration metric is still produced (behaviour unchanged).
  2. ON path is correct — the harness IC equals a direct core.stats.information_coefficient
     call on the same (entry_score, realized_roi) pairs, and the line renders.
  3. The >= 3 decided-outcome guard holds even when enabled.

Runs with numpy only (no scipy / gspread required).
"""
import io
import os
import sys
import contextlib
from datetime import datetime
from pathlib import Path

import pytest

# --- make track_performance + core importable from a tests/ dir in the repo ---
_HERE = Path(__file__).resolve()
for _cand in (_HERE.parent.parent, _HERE.parent.parent / "scripts"):
    _p = str(_cand)
    if _p not in sys.path:
        sys.path.insert(0, _p)

tp = pytest.importorskip("track_performance")
from core.stats import information_coefficient  # noqa: E402


def _make_rec(i, score, roi):
    return tp.PerformanceRecord(
        record_id=f"r{i}", symbol=f"S{i}", horizon=tp.HorizonType.MONTH_1,
        date_recorded=datetime(2026, 1, 1), entry_price=100.0,
        entry_recommendation=tp.RecommendationType.BUY, entry_score=float(score),
        entry_risk_bucket="MODERATE", entry_confidence="MEDIUM", origin_tab="T",
        target_price=110.0, target_roi=10.0, target_date=datetime(2026, 2, 1),
        status=tp.PerformanceStatus.MATURED, realized_roi=float(roi),
    )


_SCORES = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
_ROIS = [-3, -1, 0.5, 2, 1.5, 4, 5, 4.5, 7, 8, 7.5, 10]   # none == 0 -> all decided


@pytest.fixture
def recs():
    return [_make_rec(i, s, r) for i, (s, r) in enumerate(zip(_SCORES, _ROIS))]


def _render(rep):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        tp.render_calibration_report(rep, verbose=False)
    return buf.getvalue()


def test_core_stats_importable():
    assert tp._IC_AVAILABLE is True
    assert tp.SCRIPT_VERSION == "6.12.0"


def test_off_path_is_inert(recs, monkeypatch):
    monkeypatch.delenv("TFB_HARNESS_IC", raising=False)
    rep = tp.ReliabilityCalibrator().calibrate(recs)
    # IC suppressed
    assert rep.information_coefficient is None
    assert rep.ic_t_stat is None and rep.ic_p_value is None and rep.ic_n == 0
    # existing metrics unaffected
    assert rep.brier_score is not None
    assert len(rep.by_reliability_band) > 0
    # additive schema, value None
    d = rep.to_dict()
    assert "information_coefficient" in d and d["information_coefficient"] is None
    # no IC line in the rendered report
    assert "Information Coefficient" not in _render(rep)


def test_on_path_matches_direct_core_stats(recs, monkeypatch):
    monkeypatch.setenv("TFB_HARNESS_IC", "1")
    rep = tp.ReliabilityCalibrator().calibrate(recs)
    direct = information_coefficient(_SCORES, _ROIS)
    assert rep.information_coefficient is not None
    assert rep.ic_n == len(_SCORES)
    assert abs(rep.information_coefficient - round(direct["ic"], 4)) < 1e-9
    assert rep.ic_t_stat is not None and rep.ic_p_value is not None
    out = _render(rep)
    assert "Information Coefficient" in out
    assert f"{rep.information_coefficient:+.4f}" in out


def test_guard_requires_three_decided(recs, monkeypatch):
    monkeypatch.setenv("TFB_HARNESS_IC", "1")
    rep = tp.ReliabilityCalibrator().calibrate(recs[:2])
    assert rep.information_coefficient is None
    assert rep.ic_n == 0


def test_positive_ic_when_score_orders_returns(recs, monkeypatch):
    # by construction score rank-orders roi, so IC should be strongly positive
    monkeypatch.setenv("TFB_HARNESS_IC", "1")
    rep = tp.ReliabilityCalibrator().calibrate(recs)
    assert rep.information_coefficient > 0.8


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
