"""Learning-metric definition fixes (track_performance v6.46.0,
run_shadow_scorer v1.9.4). Each case pins the audit's demo input:

  DIR-OUTCOME   two SELL calls that rose +5% / +3% are 2 LOSSES (was 2 wins,
                100% win rate); HOLD is neutral; stored ROI is untouched.
  HIT-DECIDED   [2,-1,0,0] -> win_rate 50 AND hit_rate_by_horizon 50 (was 25).
  RISK-HORIZON  1M ROIs [2,-1,3,1]% -> Sharpe ~2.54 (was 13.42), per horizon.
  TURNOVER      {2222.SR} -> {2222.SR,1120.SR} -> 50% one-way turnover and a
                two-sided cost drag (was 0 / 0).
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import pytest

from scripts import run_shadow_scorer as ss
from scripts import track_performance as tp


def _rec(reco, roi, hz="1M", inv="", rel=0.0):
    now = datetime(2026, 9, 1, 12, tzinfo=timezone.utc)
    return tp.PerformanceRecord(
        record_id="r", symbol="X.SR", horizon=tp.HorizonType(hz), date_recorded=now,
        entry_price=10, entry_recommendation=reco, entry_score=50, entry_risk_bucket="LOW",
        entry_confidence="HIGH", origin_tab="Decision_Coverage", target_price=9, target_roi=-10,
        target_date=now + timedelta(days=30), status=tp.PerformanceStatus.MATURED,
        realized_roi=roi, outcome=None, entry_investability_status=inv,
        entry_forecast_reliability=rel)


R = tp.RecommendationType


# ------------------------------------------------------------ DIR-OUTCOME --- #
@pytest.mark.parametrize("reco,roi,expected", [
    (R.BUY, 5.0, "WIN"), (R.BUY, -5.0, "LOSS"), (R.BUY, 0.0, "BREAKEVEN"),
    (R.STRONG_BUY, 1.0, "WIN"), (R.ACCUMULATE, -1.0, "LOSS"),
    (R.SELL, 5.0, "LOSS"), (R.SELL, -5.0, "WIN"), (R.STRONG_SELL, -0.1, "WIN"),
    (R.REDUCE, 2.0, "LOSS"), (R.AVOID, -2.0, "WIN"), (R.SELL, 0.0, "BREAKEVEN"),
    (R.HOLD, 5.0, None), (R.HOLD, -5.0, None),
    ("EXIT", -3.0, "WIN"), ("TRIM", 3.0, "LOSS"), ("STRONG_SELL", -1.0, "WIN"),
    (R.BUY, None, None), (R.BUY, float("nan"), None),
])
def test_directional_outcome_scores_the_call_not_the_price(reco, roi, expected):
    assert tp._directional_outcome(reco, roi) == expected


def test_sell_calls_that_rose_are_losses_in_the_summary():
    recs = [_rec(R.SELL, 5.0), _rec(R.SELL, 3.0)]
    s = tp.PerformanceAnalyzer().analyze(recs)
    assert (s.wins, s.losses, s.win_rate) == (0, 2, 0.0)        # was 2 / 0 / 100.0
    assert s.hit_rate_by_horizon == {"1M": 0.0}
    # the published ROI (price return) is unchanged
    assert [r.realized_roi for r in recs] == [5.0, 3.0]
    assert s.avg_roi == pytest.approx(4.0)


def test_hold_is_neutral_and_excluded_from_directional_rates():
    s = tp.PerformanceAnalyzer().analyze(
        [_rec(R.BUY, 2.0), _rec(R.SELL, -1.0), _rec(R.HOLD, 4.0), _rec(R.HOLD, -4.0)])
    assert (s.wins, s.losses, s.breakeven, s.neutral_records) == (2, 0, 0, 2)
    assert s.win_rate == 100.0 and s.matured_records == 4


def test_segmented_rates_are_direction_aware():
    recs = [_rec(R.SELL, -2.0, inv="WATCHLIST"), _rec(R.BUY, -2.0, inv="WATCHLIST")]
    for r in recs:
        r.sector = "Energy"
    s = tp.PerformanceAnalyzer().analyze(recs)
    assert s.hit_rate_by_investability["WATCHLIST"] == 50.0
    assert (s.performance_by_sector["Energy"]["wins"],
            s.performance_by_sector["Energy"]["losses"]) == (1.0, 1.0)


def test_calibrator_buckets_and_brier_are_direction_aware():
    cal = tp.ReliabilityCalibrator().calibrate(
        [_rec(R.SELL, 5.0, rel=80.0), _rec(R.HOLD, 5.0, rel=80.0)])
    by_rec = {b.name: b for b in cal.by_recommendation}
    assert (by_rec["SELL"].wins, by_rec["SELL"].losses) == (0, 1)   # was 1 / 0
    assert (by_rec["HOLD"].decided, by_rec["HOLD"].n) == (0, 1)     # neutral
    assert cal.decided_records == 1
    assert cal.brier_score == pytest.approx(0.64)                   # (0.8 - 0)^2


def test_report_win_rate_ci_uses_decided_directional_outcomes():
    analyzer = tp.PerformanceAnalyzer()
    recs = [_rec(R.SELL, 5.0), _rec(R.SELL, 3.0), _rec(R.HOLD, 1.0)]
    import json
    report = json.loads(tp.ReportGenerator(analyzer).generate_json(recs, analyzer.analyze(recs)))
    assert report["confidence_intervals"]["win_rate"]["observed"] == 0.0


# ------------------------------------------------------------ HIT-DECIDED --- #
def test_hit_rate_by_horizon_matches_decided_only_win_rate():
    s = tp.PerformanceAnalyzer().analyze([_rec(R.BUY, r) for r in (2.0, -1.0, 0.0, 0.0)])
    assert s.win_rate == 50.0
    assert s.hit_rate_by_horizon == {"1M": 50.0}                    # was 25.0
    assert s.breakeven == 2


# ----------------------------------------------------------- RISK-HORIZON --- #
def _sharpe_expected(rois_pct, period_days, rfr=0.02):
    x = [v / 100.0 for v in rois_pct]
    m = sum(x) / len(x)
    sd = math.sqrt(sum((v - m) ** 2 for v in x) / len(x))
    return (m - rfr * period_days / 252.0) / sd * math.sqrt(252.0 / period_days)


def test_sharpe_uses_fractional_horizon_returns_and_horizon_annualisation():
    s = tp.PerformanceAnalyzer().analyze([_rec(R.BUY, r) for r in (2.0, -1.0, 3.0, 1.0)])
    assert s.sharpe_ratio == pytest.approx(_sharpe_expected([2, -1, 3, 1], 21))
    assert round(s.sharpe_ratio, 2) == 2.54                         # was 13.42
    assert s.sharpe_by_horizon == {"1M": pytest.approx(s.sharpe_ratio)}


def test_sortino_uses_downside_deviation_against_period_risk_free():
    rc = tp.RiskCalculator()
    rois = [2.0, -1.0, 3.0, 1.0]
    x = [v / 100.0 - 0.02 * 21 / 252.0 for v in rois]
    dd = math.sqrt(sum(min(v, 0.0) ** 2 for v in x) / len(x))
    expected = (sum(x) / len(x)) / dd * math.sqrt(252.0 / 21)
    assert rc.sortino_ratio(rois, 21) == pytest.approx(expected)
    assert rc.sortino_ratio([1.0, 2.0, 3.0], 21) == 0.0              # no downside


def test_mixed_horizons_are_never_pooled():
    recs = [_rec(R.BUY, r, "1M") for r in (2.0, -1.0, 3.0, 1.0)]
    recs += [_rec(R.BUY, r, "3M") for r in (6.0, -4.0, 9.0)]
    s = tp.PerformanceAnalyzer().analyze(recs)
    assert s.sharpe_by_horizon["1M"] == pytest.approx(_sharpe_expected([2, -1, 3, 1], 21))
    assert s.sharpe_by_horizon["3M"] == pytest.approx(_sharpe_expected([6, -4, 9], 63))
    assert s.sharpe_ratio == 0.0 and s.sortino_ratio == 0.0         # no pooled scalar
    cell = tp._fmt_ratio_by_horizon(s.sharpe_by_horizon, s.sharpe_ratio)
    assert cell.startswith("1M ") and " | 3M " in cell


def test_single_horizon_panel_cell_keeps_the_plain_number():
    assert tp._fmt_ratio_by_horizon({"1M": 2.5367}, 2.5367) == "2.54"


# --------------------------------------------------------------- TURNOVER --- #
def test_add_only_day_has_turnover_and_two_sided_cost():
    prev, cur = ["2222.SR"], ["2222.SR", "1120.SR"]
    assert ss.turnover_pct(prev, cur) == 50.0                       # was 0.0
    rt_a = ss.ob.rt_cost_pct("2222.SR", 15000.0)
    rt_b = ss.ob.rt_cost_pct("1120.SR", 15000.0)
    assert rt_a and rt_b
    # sell 0.5 of 2222.SR + buy 0.5 of 1120.SR, each side half a round trip
    assert ss.cost_drag_pct(prev, cur) == pytest.approx(0.5 * rt_a / 2 + 0.5 * rt_b / 2)
    assert ss.cost_drag_pct(prev, cur) > 0.0                        # was 0.0


@pytest.mark.parametrize("prev,cur,expected", [
    (["A"], ["B"], 100.0), (["A", "B"], ["A", "C"], 50.0), (["A", "B"], ["B", "A"], 0.0),
    ([], ["A"], 50.0), (["A"], [], 50.0), ([], [], 0.0),
    (["A", "B", "C", "D"], ["A", "B", "C"], 25.0),
])
def test_one_way_turnover_is_half_the_sum_of_weight_changes(prev, cur, expected):
    assert ss.turnover_pct(prev, cur) == pytest.approx(expected)


def test_full_swap_cost_is_unchanged_and_unknown_venues_stay_free():
    rt_a = ss.ob.rt_cost_pct("STNG.US", 15000.0)
    rt_b = ss.ob.rt_cost_pct("TRMD.US", 15000.0)
    assert ss.cost_drag_pct(["STNG.US"], ["TRMD.US"]) == pytest.approx((rt_a + rt_b) / 2)
    assert ss.cost_drag_pct(["RELIANCE.NS"], ["INFY.NS"]) == 0.0
    assert ss.cost_drag_pct(["STNG.US"], ["STNG.US"]) == 0.0
