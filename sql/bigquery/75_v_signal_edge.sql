-- v_signal_edge: the verdict on every live signal, per market group and
-- horizon, from the daily rank tests in v_signal_edge_daily. WHY: a single
-- day's rank correlation is noise; what a rule change needs is the average
-- over many days, corrected for the overlap of the outcome windows (a 1M
-- outcome recorded on two neighbouring days is mostly the same month), and a
-- check that the sign held in both halves of the history. This view states
-- that verdict with the same fixed rule every day, so an owner rule, a
-- ranking challenger or a currency choice reads one row instead of a study.
-- COLUMNS:
--   days                signal days with an IC (each day >= 20 names)
--   first_day/last_day  the span of those days
--   avg_n               mean names per day
--   mean_ic / sd_ic     mean and standard deviation of the daily IC
--   t_adj               mean_ic / sd_ic x SQRT(days / horizon_days): the
--                       t-statistic with the overlapping windows counted as
--                       one per horizon length (conservative: the recording
--                       days include weekends)
--   mean_ic_first_half / mean_ic_second_half  the mean IC over the older and
--                       the newer half of the days (NTILE(2) by day)
--   pct_days_positive   share of days with IC > 0
--   top_ex_pp / bottom_ex_pp / spread_pp  mean daily excess return of the
--                       feature's top and bottom third, and their difference
--   verdict             fixed rule, in this order:
--     thin      fewer than 10 days, or fewer than two horizon lengths of
--               days (days < 2 x horizon_days): not enough history to judge
--     edge      t_adj >= 2 and the mean IC positive in both halves
--     inverse   t_adj <= -2 and the mean IC negative in both halves (higher
--               values did WORSE: the signal ranks backwards)
--     no edge   |t_adj| < 1
--     unstable  anything else (a sign that flipped between the halves, or
--               1 <= |t_adj| < 2)
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_signal_edge`
OPTIONS (description = "TFB Looker v6: per market group, feature and horizon - days, mean daily rank IC, overlap-adjusted t, first/second-half IC, share of positive days, top/bottom-third excess return and a fixed verdict (thin / edge / inverse / no edge / unstable)")
AS
WITH d AS (
  SELECT
    *,
    NTILE(2) OVER (PARTITION BY market_group, feature, horizon ORDER BY signal_day) AS half
  FROM `{{DS}}.v_signal_edge_daily`
  WHERE ic IS NOT NULL
),
s AS (
  SELECT
    market_group,
    feature,
    horizon,
    MIN(horizon_days) AS horizon_days,
    COUNT(*) AS days,
    MIN(signal_day) AS first_day,
    MAX(signal_day) AS last_day,
    AVG(n) AS avg_n,
    AVG(ic) AS m,
    STDDEV(ic) AS sd,
    AVG(IF(half = 1, ic, NULL)) AS m1,
    AVG(IF(half = 2, ic, NULL)) AS m2,
    AVG(IF(ic > 0, 1, 0)) AS pos,
    AVG(top_ex_pp) AS top_ex,
    AVG(bottom_ex_pp) AS bottom_ex,
    AVG(spread_pp) AS spread
  FROM d
  GROUP BY market_group, feature, horizon
),
t AS (
  SELECT s.*, SAFE_DIVIDE(m, sd) * SQRT(days / horizon_days) AS t_adj
  FROM s
)
SELECT
  market_group,
  feature,
  horizon,
  days,
  first_day,
  last_day,
  ROUND(avg_n, 1) AS avg_n,
  ROUND(m, 4) AS mean_ic,
  ROUND(sd, 4) AS sd_ic,
  ROUND(t_adj, 2) AS t_adj,
  ROUND(m1, 4) AS mean_ic_first_half,
  ROUND(m2, 4) AS mean_ic_second_half,
  ROUND(pos * 100, 1) AS pct_days_positive,
  ROUND(top_ex, 2) AS top_ex_pp,
  ROUND(bottom_ex, 2) AS bottom_ex_pp,
  ROUND(spread, 2) AS spread_pp,
  CASE
    WHEN days < 10 OR days < 2 * horizon_days THEN 'thin'
    WHEN t_adj >= 2 AND m1 > 0 AND m2 > 0 THEN 'edge'
    WHEN t_adj <= -2 AND m1 < 0 AND m2 < 0 THEN 'inverse'
    WHEN ABS(t_adj) < 1 THEN 'no edge'
    ELSE 'unstable'
  END AS verdict
FROM t;
