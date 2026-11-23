-- v_projection_scorecard: the record of the projection-context rules of v_projection_context, measured on TFB's own
-- scored US buy calls against every US buy call, split into the calls that suggested the rules and the calls made
-- after they were written down.
-- WHY (owner, 23 Nov 2026 10:03 Riyadh): link the news, options, futures and other market data to the projections,
-- and support their accuracy. The review of that day found two in-sample effects on the US calls (see
-- v_projection_context): the count of adverse US market states at the call, and the promise measured in the
-- stock's own one-sigma move. A rule enters the score only once it holds on new calls; this view is that test.
-- THE LIST, FIXED ON 23 NOV 2026 BEFORE ANY FORWARD DATA (period 'forward from 2026-11-24' = calls recorded from
-- 24 Nov 2026; earlier calls are 'in-sample, not evidence'):
--   B0  every US INVESTABLE buy call scored by v_outcome_engine (the baseline)
--   P1  B0 with at most 1 adverse US market state at the last market close before the call (VIX 20 or more, VIX
--       curve inverted, VVIX high, MOVE high, credit weakening, 10-year yield jumping; v_market_drivers)
--   P2  B0 whose promise is within 0.7 of the stock's own one-sigma move to the horizon (20-session realised
--       volatility from TFB's daily bars, the last bar before the call; a call with no volatility is not kept)
--   P3  P1 and P2 together
--   P4  B0 with 3 or more adverse states (the side the rule would leave out)
-- THE MEASURE: hit_pct, mean_pp and their baseline; inside_1sigma_pct = the share of the arm's results inside -/+ one
-- realised sigma (68 % when the range is right). Two lifts, because the rules are of two kinds:
--   lift_pp / t_stat            a rule that picks among the calls of the same day (P2): the mean over call days of the
--                               arm's mean minus the baseline's that day, and that mean over its standard error
--   timing_lift_pp / timing_t   a rule on the market's state, the same for every call of a day (P1, P3, P4): the mean
--                               of the kept days' results minus the mean of the other days' results (day means; Welch
--                               t on the two sets of days). Within a day such a rule keeps all calls or none, so its
--                               within-day lift is always 0
-- THE READING (fixed now): a forward row needs 20 call days and 40 calls; then BETTER THAN THE BASELINE at t >= 2,
-- WORSE at t <= -2, else NO CLEAR DIFFERENCE, on timing_t for P1, P3 and P4 and on t_stat for P2. In-sample rows read
-- 'in-sample, not evidence'.
-- NO LOOK-AHEAD: the market state and the volatility are those of the last close BEFORE the call's recording day.
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_projection_scorecard`
OPTIONS (description = "TFB projection scorecard (views 1.24.0): five fixed rules on TFB's scored US buy calls - the baseline, at most 1 adverse US market state, the promise within 0.7 of the stock's own one-sigma move, both, and 3 or more adverse states - with hit rate, mean result, daily lift and its t against the baseline, and the share of results inside one sigma, in-sample and on new calls from 24 Nov 2026 - research only")
AS
WITH c AS (
  SELECT record_id, symbol, horizon, recorded_date, engine_roi_pp AS r, engine_win_flag AS w, target_roi_pp AS p,
    CASE horizon WHEN '1W' THEN 5 WHEN '2W' THEN 10 WHEN '1M' THEN 21 WHEN '3M' THEN 63 END AS hd
  FROM `{{DS}}.v_outcome_engine`
  WHERE engine_scored AND entry_investability = 'INVESTABLE' AND entry_stance = 'bullish' AND market = 'US'
),
md AS (
  SELECT date,
    IF(vix_level_state IN ('20 to 30', '30 and over'), 1, 0) + IF(vix_term_state = 'inverted', 1, 0)
      + IF(vvix_state = 'high', 1, 0) + IF(move_state = 'high', 1, 0) + IF(credit_state = 'weakening', 1, 0)
      + IF(rates_state = 'yields jumping', 1, 0) AS adverse,
    LEAD(date) OVER (ORDER BY date) AS next_date
  FROM `{{DS}}.v_market_drivers`
),
bars AS (
  SELECT symbol, date, close
  FROM `{{DS}}.symbol_bars_daily`
  WHERE close > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY captured_at DESC) = 1
),
lr AS (
  SELECT symbol, date, LN(close / LAG(close) OVER (PARTITION BY symbol ORDER BY date)) AS x FROM bars
),
vol AS (
  SELECT symbol, date,
    STDDEV(x) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) * SQRT(252) * 100 AS v20,
    COUNT(x) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS n20,
    LEAD(date) OVER (PARTITION BY symbol ORDER BY date) AS next_date
  FROM lr
),
j AS (
  SELECT c.*, m.adverse,
    IF(v.n20 >= 15 AND v.v20 > 0, v.v20 * SQRT(c.hd / 252), NULL) AS sigma,
    IF(c.recorded_date >= DATE '2026-11-24', 'forward from 2026-11-24', 'in-sample, not evidence') AS period
  FROM c
  LEFT JOIN md m ON c.recorded_date > m.date AND (m.next_date IS NULL OR c.recorded_date <= m.next_date)
  LEFT JOIN vol v ON v.symbol = c.symbol AND c.recorded_date > v.date
    AND (v.next_date IS NULL OR c.recorded_date <= v.next_date)
),
k AS (
  SELECT j.*,
    IFNULL(adverse <= 1, FALSE) AS calm,
    IFNULL(SAFE_DIVIDE(p, sigma) <= 0.7, FALSE) AS measured
  FROM j
),
long AS (
  SELECT k.period, hz AS horizon, k.recorded_date, k.r, k.w, k.sigma, a.arm, a.kept
  FROM k,
    UNNEST([k.horizon, 'ALL']) AS hz,
    UNNEST([
      STRUCT('B0 every US INVESTABLE buy' AS arm, TRUE AS kept),
      STRUCT('P1 at most 1 adverse market state', k.calm),
      STRUCT('P2 promise within 0.7 of its own sigma', k.measured),
      STRUCT('P3 both (P1 and P2)', k.calm AND k.measured),
      STRUCT('P4 3 or more adverse market states', IFNULL(k.adverse >= 3, FALSE))
    ]) AS a
),
tot AS (
  SELECT period, horizon, arm,
    COUNTIF(kept) AS calls,
    COUNT(DISTINCT IF(kept, recorded_date, NULL)) AS call_days,
    AVG(IF(kept, w, NULL)) AS hit,
    AVG(IF(kept, r, NULL)) AS mean_r,
    AVG(w) AS base_hit,
    AVG(r) AS base_mean,
    AVG(IF(kept AND sigma IS NOT NULL, IF(ABS(r) <= sigma, 1, 0), NULL)) AS inside,
    MIN(IF(kept, recorded_date, NULL)) AS first_day,
    MAX(IF(kept, recorded_date, NULL)) AS last_day
  FROM long
  GROUP BY period, horizon, arm
),
daily AS (
  SELECT period, horizon, arm, recorded_date, COUNTIF(kept) AS n_kept,
    AVG(IF(kept, r, NULL)) AS arm_mean, AVG(r) AS day_mean
  FROM long
  GROUP BY period, horizon, arm, recorded_date
),
lift AS (
  SELECT period, horizon, arm,
    COUNTIF(n_kept > 0) AS days,
    AVG(IF(n_kept > 0, arm_mean - day_mean, NULL)) AS lift,
    SAFE_DIVIDE(AVG(IF(n_kept > 0, arm_mean - day_mean, NULL)),
                SAFE_DIVIDE(STDDEV_SAMP(IF(n_kept > 0, arm_mean - day_mean, NULL)), SQRT(COUNTIF(n_kept > 0)))) AS t,
    AVG(IF(n_kept > 0, arm_mean, NULL)) - AVG(IF(n_kept = 0, day_mean, NULL)) AS timing_lift,
    SAFE_DIVIDE(AVG(IF(n_kept > 0, arm_mean, NULL)) - AVG(IF(n_kept = 0, day_mean, NULL)),
                SQRT(SAFE_DIVIDE(VAR_SAMP(IF(n_kept > 0, arm_mean, NULL)), COUNTIF(n_kept > 0))
                     + SAFE_DIVIDE(VAR_SAMP(IF(n_kept = 0, day_mean, NULL)), COUNTIF(n_kept = 0)))) AS timing_t,
    COUNTIF(n_kept = 0) AS other_days
  FROM daily
  GROUP BY period, horizon, arm
)
SELECT
  t.period, t.horizon, t.arm, t.calls, t.call_days,
  ROUND(100 * t.hit, 1) AS hit_pct, ROUND(t.mean_r, 2) AS mean_pp,
  ROUND(100 * t.base_hit, 1) AS base_hit_pct, ROUND(t.base_mean, 2) AS base_mean_pp,
  ROUND(l.lift, 2) AS lift_pp, ROUND(l.t, 2) AS t_stat,
  ROUND(l.timing_lift, 2) AS timing_lift_pp, ROUND(l.timing_t, 2) AS timing_t, l.other_days,
  ROUND(100 * t.inside, 1) AS inside_1sigma_pct,
  t.first_day, t.last_day,
  CASE
    WHEN STARTS_WITH(t.arm, 'B0') THEN 'the baseline'
    WHEN STARTS_WITH(t.period, 'in-sample') THEN 'in-sample, not evidence'
    WHEN IFNULL(l.days, 0) < 20 OR t.calls < 40 THEN 'collecting'
    WHEN IF(STARTS_WITH(t.arm, 'P2'), l.t, l.timing_t) >= 2 THEN 'BETTER THAN THE BASELINE'
    WHEN IF(STARTS_WITH(t.arm, 'P2'), l.t, l.timing_t) <= -2 THEN 'WORSE THAN THE BASELINE'
    ELSE 'NO CLEAR DIFFERENCE'
  END AS reading
FROM tot t
LEFT JOIN lift l USING (period, horizon, arm)
WHERE t.calls > 0 OR STARTS_WITH(t.period, 'forward');
