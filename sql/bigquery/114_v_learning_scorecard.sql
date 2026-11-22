-- v_learning_scorecard: the learning loop's weekly scorecard. A fixed list of
-- rule variants, each measured on the project's own scored calls against a
-- baseline, split into the calls that suggested the rule and the calls made
-- after it was written down.
-- WHY (owner, 22 Nov 2026): "check the system is getting learning day over
-- day and how we can improve and accelerate the learning ... how system can
-- learn from the previous loss". The review of that day found that the hit
-- rate follows the market's condition, that stops caused most of the real
-- loss, and that nothing in the project re-tests a rule by itself: every
-- finding so far was a one-off query. A rule needs about 20 call days to be
-- judged and that clock cannot be shortened; what can change is how many
-- variants are on the clock at once. This view puts ten on it.
-- THE LIST, FIXED ON 22 NOV 2026 BEFORE ANY FORWARD DATA (period 'forward'
-- = calls recorded from 23 Nov 2026; earlier calls are 'in-sample, not
-- evidence': they are the calls that suggested the rules):
--   B0  every INVESTABLE buy call, held to its horizon (the baseline)
--   E1  B0 without the calls made while their market was on the armed hold
--       list (weak, stress, drawdown, crash_day)
--   E2  the same without 'weak' on the list (stress, drawdown, crash_day)
--   E3  B0 without Tadawul
--   E4  the selection gate: an analyst target 15 % or more above the price
--       and a risk score under 30, both as Signal_History recorded them on
--       the day of the call (no record that day = not kept)
--   E5  E1 and E4 together (the two gates the engine enforces from 22 Nov)
--   X1 / X2 / X3  B0 with a stop 5 % / 8 % / 10 % under the entry: a call
--       whose lowest close before its target date reached the stop is booked
--       at the stop, any other at its result (daily closes; no gap, no
--       commission, so an estimate)
--   S1  the sell side: calls rated SELL, STRONG SELL, REDUCE or AVOID, a hit
--       when the share fell, against the buy-rated calls of the same day
-- THE MEASURE: hit_pct = the share of the arm's calls with a result above
-- zero (S1: the share that fell); mean_pp = their mean result; base_* = the
-- same for the baseline calls (X arms: the same calls without the stop; S1:
-- the buy-rated calls, counted as a hit when they fell); lift_pp = the mean,
-- over call days, of the arm's mean result minus the baseline's that day;
-- t_stat = that mean over its standard error; worst5_pp = the 5th percentile
-- of the arm's results.
-- THE READING (fixed now): a forward row needs 20 call days and 40 calls;
-- then BETTER THAN THE BASELINE at t >= 2, WORSE at t <= -2, else NO CLEAR
-- DIFFERENCE. Even then the owner decides; nothing here changes a page, a
-- gate or an order.
-- NO LOOK-AHEAD: the market's label is the one of its last close BEFORE the
-- day of the call; the signal is the last one recorded for the symbol on
-- the day of the call; the reading is judged on unrounded statistics.
-- COLUMNS: period, horizon (1W / 2W / 1M / 3M / ALL), arm, calls, days,
-- hit_pct, mean_pp, worst5_pp, base_calls, base_hit_pct, base_mean_pp,
-- lift_pp, t_stat, first_day, last_day, reading.
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_learning_scorecard`
OPTIONS (description = "TFB learning scorecard (list fixed 22 Nov 2026): ten rule variants - market hold lists, the selection gate, Tadawul, stops at 5 / 8 / 10 %, the sell side - each against its baseline on the project's own scored calls, in-sample and forward from 23 Nov 2026 - research only")
AS
WITH cond AS (
  SELECT market_id, date, label,
    LEAD(date) OVER (PARTITION BY market_id ORDER BY date) AS next_date
  FROM `{{DS}}.v_market_condition`
),
sig AS (
  SELECT symbol, DATE(recorded_at) AS day,
    ARRAY_AGG(STRUCT(target_upside_pp AS upside, risk_score AS risk)
              ORDER BY recorded_at DESC LIMIT 1)[OFFSET(0)] AS s
  FROM `{{DS}}.v_signal_history`
  WHERE recorded_at IS NOT NULL
  GROUP BY symbol, day
),
calls AS (
  SELECT o.record_id, o.recorded_date, o.horizon, o.market, o.entry_stance AS stance,
    o.engine_roi_pp AS roi, o.mae_pct,
    IFNULL(o.entry_investability, '') = 'INVESTABLE' AS investable,
    IFNULL(c.label, 'unread') AS label,
    IFNULL(g.s.upside >= 15 AND g.s.risk < 30, FALSE) AS selected,
    IF(o.recorded_date >= DATE '2026-11-23', 'forward from 2026-11-23', 'in-sample, not evidence') AS period
  FROM `{{DS}}.v_outcome_engine` o
  LEFT JOIN cond c
    ON c.market_id = CASE o.market WHEN 'US' THEN 'us' WHEN 'Tadawul' THEN 'sa' WHEN 'Europe' THEN 'eu'
                                   WHEN 'London' THEN 'eu' WHEN 'Tokyo' THEN 'jp' WHEN 'Hong Kong' THEN 'cn' END
   AND c.date < o.recorded_date AND (c.next_date IS NULL OR c.next_date >= o.recorded_date)
  LEFT JOIN sig g ON g.symbol = o.symbol AND g.day = o.recorded_date
  WHERE o.in_engine_cohort AND o.engine_roi_pp IS NOT NULL AND o.entry_stance IN ('bullish', 'bearish')
),
arms AS (
  SELECT c.period, c.recorded_date, c.horizon, a.arm, a.kept, a.in_base, a.r, a.b
  FROM calls c, UNNEST([
    STRUCT('B0 every INVESTABLE buy' AS arm, TRUE AS kept, TRUE AS in_base, roi AS r, roi AS b),
    STRUCT('E1 market not on the armed hold list', label NOT IN ('weak', 'stress', 'drawdown', 'crash_day'), TRUE, roi, roi),
    STRUCT('E2 hold list without weak', label NOT IN ('stress', 'drawdown', 'crash_day'), TRUE, roi, roi),
    STRUCT('E3 without Tadawul', market != 'Tadawul', TRUE, roi, roi),
    STRUCT('E4 selection gate', selected, TRUE, roi, roi),
    STRUCT('E5 both gates (E1 and E4)', selected AND label NOT IN ('weak', 'stress', 'drawdown', 'crash_day'), TRUE, roi, roi),
    STRUCT('X1 stop at 5%', mae_pct IS NOT NULL, mae_pct IS NOT NULL, IF(mae_pct <= -5, -5.0, roi), roi),
    STRUCT('X2 stop at 8%', mae_pct IS NOT NULL, mae_pct IS NOT NULL, IF(mae_pct <= -8, -8.0, roi), roi),
    STRUCT('X3 stop at 10%', mae_pct IS NOT NULL, mae_pct IS NOT NULL, IF(mae_pct <= -10, -10.0, roi), roi)
  ]) AS a
  WHERE c.investable AND c.stance = 'bullish'
  UNION ALL
  SELECT period, recorded_date, horizon, 'S1 sell side: rated SELL, REDUCE or AVOID' AS arm,
    stance = 'bearish' AS kept, stance = 'bullish' AS in_base, -roi AS r, -roi AS b
  FROM calls
),
wide AS (
  SELECT period, recorded_date, hz, arm, kept, in_base, r, b
  FROM arms, UNNEST([horizon, 'ALL']) AS hz
),
tot AS (
  SELECT period, hz, arm,
    COUNTIF(kept) AS calls, COUNTIF(kept AND r > 0) AS wins, AVG(IF(kept, r, NULL)) AS mean_r,
    APPROX_QUANTILES(IF(kept, r, NULL), 20 IGNORE NULLS)[SAFE_OFFSET(1)] AS p5,
    COUNTIF(in_base) AS base_calls, COUNTIF(in_base AND b > 0) AS base_wins, AVG(IF(in_base, b, NULL)) AS base_mean,
    MIN(IF(kept, recorded_date, NULL)) AS first_day, MAX(IF(kept, recorded_date, NULL)) AS last_day
  FROM wide
  GROUP BY period, hz, arm
),
day AS (
  SELECT period, hz, arm, recorded_date,
    AVG(IF(kept, r, NULL)) - AVG(IF(in_base, b, NULL)) AS d
  FROM wide
  GROUP BY period, hz, arm, recorded_date
  HAVING COUNTIF(kept) > 0 AND COUNTIF(in_base) > 0
),
lift AS (
  SELECT period, hz, arm, COUNT(*) AS days, AVG(d) AS lift,
    SAFE_DIVIDE(AVG(d), SAFE_DIVIDE(STDDEV_SAMP(d), SQRT(COUNT(*)))) AS t
  FROM day
  GROUP BY period, hz, arm
)
SELECT t.period, t.hz AS horizon, t.arm, t.calls, IFNULL(l.days, 0) AS days,
  ROUND(100 * SAFE_DIVIDE(t.wins, t.calls), 1) AS hit_pct,
  ROUND(t.mean_r, 2) AS mean_pp,
  ROUND(t.p5, 2) AS worst5_pp,
  t.base_calls,
  ROUND(100 * SAFE_DIVIDE(t.base_wins, t.base_calls), 1) AS base_hit_pct,
  ROUND(t.base_mean, 2) AS base_mean_pp,
  ROUND(l.lift, 2) AS lift_pp,
  ROUND(l.t, 2) AS t_stat,
  t.first_day, t.last_day,
  CASE
    WHEN STARTS_WITH(t.arm, 'B0') THEN 'the baseline'
    WHEN t.calls = 0 THEN 'no calls yet'
    WHEN STARTS_WITH(t.period, 'in-sample') THEN 'in-sample, not evidence'
    WHEN IFNULL(l.days, 0) < 20 OR t.calls < 40
      THEN CONCAT('TOO FEW TO JUDGE (', CAST(IFNULL(l.days, 0) AS STRING), ' of 20 days, ',
                  CAST(t.calls AS STRING), ' of 40 calls)')
    WHEN l.t >= 2 THEN 'BETTER THAN THE BASELINE'
    WHEN l.t <= -2 THEN 'WORSE THAN THE BASELINE'
    ELSE 'NO CLEAR DIFFERENCE'
  END AS reading
FROM tot t
LEFT JOIN lift l USING (period, hz, arm);
