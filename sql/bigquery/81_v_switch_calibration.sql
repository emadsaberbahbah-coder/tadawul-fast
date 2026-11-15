-- v_switch_calibration: what a gap between two forecasts has been worth, per
-- market group and horizon, measured on the project's own matured records.
-- WHY (owner, 15 Nov 2026): "a holding should be kept only while it is
-- better than the best available opportunity over the same period, after the
-- transaction cost of both trades - be accurate when calculating that". A
-- forecast gap is not a return gap: on the matured records the forecasts
-- overstate levels, and whether a higher forecast means a higher result has
-- to be measured, per horizon, before a switch can rest on it.
-- METHOD: one row per (horizon, call day, symbol) from v_outcome_excess (the
-- engine's dividend-adjusted outcome; |values| under 100 pp). Within each
-- call day and market group (days with 10 names or more):
--   ic        the correlation between forecast and outcome that day
--   slope     pooled over days: sum(fd x rd) / sum(fd x fd), fd and rd being
--             forecast and outcome less that day's group mean - the outcome
--             points one forecast point has been worth
--   noise     the mean spread (standard deviation) of outcomes across names
-- VERDICT (the fixed rule of v_signal_edge, on the daily ic):
--   thin      fewer than 10 days, or fewer than two horizon lengths of days
--   proven    t_adj >= 2 and the mean ic positive in both halves of the days
--   inverse   t_adj <= -2 and negative in both halves
--   no edge   |t_adj| < 1
--   unstable  anything else
--   t_adj = mean ic / sd ic x SQRT(days / horizon_days): overlapping outcome
--   windows count once per horizon length. Judged before rounding.
-- k_indicative = slope held inside 0..1 (a forecast point is never counted
--   as more than one outcome point). k_used = k_indicative when the verdict
--   is 'proven', else 0: an unproven signal moves no money.
-- Research view: the live system never reads BigQuery; no page, gate or
-- broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_switch_calibration`
OPTIONS (description = "TFB hold-or-switch calibration: per market group and horizon, what one point of forecast gap has been worth in outcome points on matured records (pooled within-day slope), the spread of outcomes, the overlap-adjusted t of the daily forecast/outcome correlation and a fixed verdict. k_used is 0 unless the verdict is proven. Research only.")
AS
WITH b AS (
  SELECT
    horizon,
    recorded_date AS d,
    symbol,
    IF(market IN ('US', 'Tadawul'), market, 'Other markets') AS market_group,
    AVG(target_roi_pp) AS f,
    AVG(engine_roi_pp) AS r
  FROM `{{DS}}.v_outcome_excess`
  WHERE in_engine_cohort
    AND engine_roi_pp IS NOT NULL AND target_roi_pp IS NOT NULL
    AND ABS(target_roi_pp) < 100 AND ABS(engine_roi_pp) < 100
  GROUP BY horizon, d, symbol, market_group
),
g AS (
  SELECT market_group, horizon, d, symbol, f, r FROM b
  UNION ALL
  SELECT 'ALL' AS market_group, horizon, d, symbol, f, r FROM b
),
w AS (
  SELECT
    g.*,
    f - AVG(f) OVER p AS fd,
    r - AVG(r) OVER p AS rd,
    COUNT(*) OVER p AS n
  FROM g
  WINDOW p AS (PARTITION BY market_group, horizon, d)
),
day AS (
  SELECT
    market_group, horizon, d,
    ANY_VALUE(n) AS n,
    CORR(f, r) AS ic,
    SUM(fd * rd) AS sxy,
    SUM(fd * fd) AS sxx,
    STDDEV_SAMP(r) AS sd_r
  FROM w
  WHERE n >= 10
  GROUP BY market_group, horizon, d
  HAVING SUM(fd * fd) > 0
),
h AS (
  SELECT day.*, NTILE(2) OVER (PARTITION BY market_group, horizon ORDER BY d) AS half
  FROM day
  WHERE ic IS NOT NULL
),
s AS (
  SELECT
    market_group, horizon,
    CASE horizon WHEN '1W' THEN 7 WHEN '2W' THEN 14 WHEN '1M' THEN 30 WHEN '3M' THEN 91 END AS horizon_days,
    COUNT(*) AS days,
    MIN(d) AS first_day,
    MAX(d) AS last_day,
    AVG(n) AS avg_n,
    AVG(ic) AS m,
    STDDEV(ic) AS sd,
    AVG(IF(half = 1, ic, NULL)) AS m1,
    AVG(IF(half = 2, ic, NULL)) AS m2,
    SAFE_DIVIDE(SUM(sxy), SUM(sxx)) AS slope,
    AVG(sd_r) AS noise
  FROM h
  GROUP BY market_group, horizon
),
t AS (
  SELECT s.*, SAFE_DIVIDE(m, sd) * SQRT(days / horizon_days) AS t_adj
  FROM s
  WHERE horizon_days IS NOT NULL
),
v AS (
  SELECT
    t.*,
    CASE
      WHEN days < 10 OR days < 2 * horizon_days THEN 'thin'
      WHEN t_adj >= 2 AND m1 > 0 AND m2 > 0 THEN 'proven'
      WHEN t_adj <= -2 AND m1 < 0 AND m2 < 0 THEN 'inverse'
      WHEN ABS(t_adj) < 1 THEN 'no edge'
      ELSE 'unstable'
    END AS verdict,
    LEAST(1.0, GREATEST(0.0, IFNULL(slope, 0.0))) AS k_indicative
  FROM t
)
SELECT
  market_group,
  horizon,
  horizon_days,
  days,
  first_day,
  last_day,
  ROUND(avg_n, 1) AS avg_n,
  ROUND(m, 3) AS mean_ic,
  ROUND(t_adj, 2) AS t_adj,
  ROUND(m1, 3) AS mean_ic_first_half,
  ROUND(m2, 3) AS mean_ic_second_half,
  ROUND(slope, 3) AS slope,
  ROUND(k_indicative, 3) AS k_indicative,
  ROUND(IF(verdict = 'proven', k_indicative, 0.0), 3) AS k_used,
  ROUND(noise, 2) AS noise_pp,
  verdict
FROM v;
