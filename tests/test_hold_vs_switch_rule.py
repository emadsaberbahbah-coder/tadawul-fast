"""v_switch_calibration and v_hold_vs_switch (apply_bq_views 1.9.0): the hold-or-switch rule was fixed on 15 Nov 2026.
Owner: a holding is kept while it beats the best available opportunity over the same period after the cost of both
trades. This test keeps the rule from changing silently: the costs, the 60% bar, "an unproven signal moves nothing"
and the order of the verdicts. A change to the rule must change this test too, in a reviewed commit."""
from __future__ import annotations

import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CAL_SQL = (ROOT / "sql/bigquery/81_v_switch_calibration.sql").read_text(encoding="utf-8")
HVS_SQL = (ROOT / "sql/bigquery/82_v_hold_vs_switch.sql").read_text(encoding="utf-8")
CAL = "\n".join(line.split("--")[0] for line in CAL_SQL.splitlines())
HVS = "\n".join(line.split("--")[0] for line in HVS_SQL.splitlines())


def test_the_signal_verdict_is_the_house_rule_and_is_judged_before_rounding():
    verdict = CAL[CAL.index("CASE\n      WHEN days < 10"):CAL.index("END AS verdict")]
    order = ["WHEN days < 10 OR days < 2 * horizon_days THEN 'thin'",
             "WHEN t_adj >= 2 AND m1 > 0 AND m2 > 0 THEN 'proven'",
             "WHEN t_adj <= -2 AND m1 < 0 AND m2 < 0 THEN 'inverse'",
             "WHEN ABS(t_adj) < 1 THEN 'no edge'", "ELSE 'unstable'"]
    places = [verdict.index(item) for item in order]
    assert places == sorted(places)
    assert "SAFE_DIVIDE(m, sd) * SQRT(days / horizon_days) AS t_adj" in CAL          # overlap counted once per horizon
    assert "WHEN '1W' THEN 7 WHEN '2W' THEN 14 WHEN '1M' THEN 30 WHEN '3M' THEN 91" in CAL
    assert "ROUND(" not in verdict and "mean_ic" not in verdict
    assert "WHERE n >= 10" in CAL and "HAVING SUM(fd * fd) > 0" in CAL


def test_a_forecast_point_is_never_worth_more_than_one_outcome_point_and_unproven_is_zero():
    assert "SAFE_DIVIDE(SUM(sxy), SUM(sxx)) AS slope" in CAL
    assert "LEAST(1.0, GREATEST(0.0, IFNULL(slope, 0.0))) AS k_indicative" in CAL
    assert "ROUND(IF(verdict = 'proven', k_indicative, 0.0), 3) AS k_used" in CAL
    assert "in_engine_cohort" in CAL and "ABS(target_roi_pp) < 100 AND ABS(engine_roi_pp) < 100" in CAL


def test_both_trades_are_charged_and_the_bar_is_sixty_percent():
    assert "SELECT 0.10 AS commission_side_pp, 0.15 AS spread_side_pp, 60.0 AS min_chance_pct" in HVS
    assert "2 * (q.commission_side_pp + q.spread_side_pp) AS cost_pp" in HVS          # one sale and one purchase
    assert "(best_f - holding_f) * k_indicative - cost_pp AS net_pp" in HVS
    assert "100 / (1 + EXP(-1.702 * SAFE_DIVIDE(net_pp, SQRT(2) * noise_pp))) AS chance_pct" in HVS


def test_the_verdicts_come_in_the_fixed_order_and_only_a_proven_signal_can_say_switch():
    verdict = HVS[HVS.index("CASE\n    WHEN holding_f IS NULL OR best_f IS NULL"):HVS.index("END AS verdict")]
    order = ["THEN 'NO DATA'", "WHEN gap_pp <= 0 THEN 'KEEP'", "WHEN signal_verdict != 'proven' THEN 'KEEP'",
             "WHEN net_pp <= 0 THEN 'KEEP'", "WHEN chance_pct < min_chance_pct THEN 'KEEP'", "ELSE 'SWITCH CANDIDATE'"]
    places = [verdict.index(item) for item in order]
    assert places == sorted(places) and verdict.count("SWITCH CANDIDATE") == 1
    assert "ROUND(" not in verdict                                                    # judged before rounding
    assert HVS.count("'SWITCH CANDIDATE'") == 1 and "never an order" in HVS_SQL.replace("\n-- ", " ").replace("\n", " ")


def test_candidates_are_investable_fresh_not_held_and_of_the_same_market():
    for clause in ("UPPER(u.investability_status) = 'INVESTABLE'", "UPPER(u.final_action) = 'INVEST'",
                   "AND u.meets_floors", "AND u.snapshot_age_days <= 3",
                   "AND u.symbol NOT IN (SELECT symbol FROM hold)",
                   "ON b.market_group = h.market_group AND b.horizon = h.horizon",
                   "ON c.market_group = h.market_group AND c.horizon = h.horizon"):
        assert clause in HVS, clause
    assert "ORDER BY f DESC, overall_score DESC, symbol" in HVS
    # 1.9.1: 1W and 2W are the 1M figure x days/30, the tracker's own scaling (_derive_checkpoint_target)
    assert HVS.count("h.f_1m * 7 / 30") == 1 and HVS.count("h.f_1m * 14 / 30") == 1
    assert HVS.count("c.f_1m * 7 / 30") == 1 and HVS.count("c.f_1m * 14 / 30") == 1
    assert "0.16" not in HVS and "0.27" not in HVS
    tracker = (ROOT / "scripts/track_performance.py").read_text(encoding="utf-8")
    assert "_CHECKPOINT_BASE_DAYS = 30.0" in tracker and "float(horizon.days) / self._CHECKPOINT_BASE_DAYS" in tracker


def test_the_arithmetic_of_a_worked_example():
    # 15 Nov 2026, one month, US: holding forecast 0.55, best candidate 6.00, one forecast point worth 1.0
    # outcome point, outcomes spread 10.65 pp across names. 5.45 - 0.50 = 4.95 net; chance 64%.
    gap, k, cost, noise = 6.00 - 0.55, 1.0, 2 * (0.10 + 0.15), 10.65
    net = gap * k - cost
    chance = 100 / (1 + math.exp(-1.702 * net / (math.sqrt(2) * noise)))
    assert round(net, 2) == 4.95 and round(chance) == 64
    exact = 100 * 0.5 * (1 + math.erf(net / (math.sqrt(2) * noise) / math.sqrt(2)))  # the normal curve itself
    assert abs(chance - exact) < 1.0
    assert round(100 / (1 + math.exp(0))) == 50                                       # no advantage is a coin flip


def test_both_views_are_research_only_and_in_the_ci_lane():
    for sql, code in ((CAL_SQL, CAL), (HVS_SQL, HVS)):
        assert "never reads BigQuery" in sql and sql.rstrip().endswith(";") and code.count(";") == 1
    ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "tests/test_hold_vs_switch_rule.py" in ci
