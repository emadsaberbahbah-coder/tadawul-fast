-- v_driver_learning: each stock-level price driver's record - does a higher reading of it come before a better
-- next week for the stock, against the other US names TFB carries on the same day?
-- WHY (owner, 22 Nov 2026 19:53 Riyadh): the options reading, short volume, the week against the market and the
-- other drivers in v_price_drivers may enter TFB's score only once they proved themselves (owner's ground rules,
-- 21 Nov: observe 10 sessions, then review, then switch on only what proved itself). This view is that proof.
-- WHAT IT MEASURES, per snapshot day (TFB's nightly pages, US stocks of $2bn or more, from 31 Oct 2026):
--   next_week_excess_pp  the stock's price 7 days later over today's, minus the same-day average of all names
--   rank_ic              the correlation of the driver's rank with the rank of next_week_excess_pp that day
--                        (Spearman), on days with 20 names or more
--   q1..q5               the mean next_week_excess_pp of each fifth of the names by the driver, low to high
-- PER DRIVER: days, weeks (distinct calendar weeks: snapshots 7 days apart overlap, so weeks is the count of
-- independent readings), names_per_day, mean_ic, ic_positive_share, ic_t (mean / sd x sqrt(weeks)),
-- q5_minus_q1_pp, first and last day, and a reading:
--   'too few weeks'            under 8 weeks
--   'higher is better'         mean_ic >= 0.03 and the IC positive on 55% of days or more
--   'lower is better'          mean_ic <= -0.03 and the IC positive on 45% of days or fewer
--   'no edge yet'              otherwise
-- The options drivers have rows only from the first night bq_options ran: until then they read 'too few weeks'.
-- 'score with drivers (shadow)' is v_price_drivers' observe-only score (overall + 1.5 x options tilt - 1 when
-- short selling runs high); set beside 'TFB overall score', it shows whether the drivers would rank names better.
-- Every row is in-sample and starts short. Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_driver_learning`
OPTIONS (description = "TFB stock driver record (views 1.23.0): for TFB's overall score, the observe-only score with drivers and each stock-level driver (options tilt, skew, calls-minus-puts IV spread, puts/calls volume, IV percentile, implied over realised volatility, short-volume z, volume ratio, distance from the 52-week high, RSI, the past week against the market) the daily rank IC and quintile spread against the next week's excess return of US names of $2bn or more, from 31 Oct 2026, with a plain reading - research only")
AS
WITH px AS (
  SELECT symbol, snapshot_date, current_price, overall_score, volume, avg_volume_30d, from_52w_high_pct,
    rsi_14, volatility_30d_pct
  FROM `{{DS}}.v_universe`
  WHERE snapshot_date >= DATE '2026-10-24'
    AND source_page IN ('Global_Markets', 'Market_Leaders')
    AND market_code = 'US' AND market_cap >= 2e9 AND current_price > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, snapshot_date ORDER BY captured_at DESC, source_page) = 1
),
ret AS (
  SELECT p.*,
    (SAFE_DIVIDE(f.current_price, p.current_price) - 1) * 100 AS next_week_pct,
    (SAFE_DIVIDE(p.current_price, b.current_price) - 1) * 100 AS past_week_pct
  FROM px p
  JOIN px f ON f.symbol = p.symbol AND f.snapshot_date = DATE_ADD(p.snapshot_date, INTERVAL 7 DAY)
  LEFT JOIN px b ON b.symbol = p.symbol AND b.snapshot_date = DATE_SUB(p.snapshot_date, INTERVAL 7 DAY)
  WHERE p.snapshot_date >= DATE '2026-10-31'
),
opt AS (
  SELECT r.symbol, r.snapshot_date, o.options_tilt, o.skew_25d_pp, o.cp_iv_spread_pp, o.pc_volume_ratio,
    o.iv_pctile_1y, o.short_ratio_z, o.iv30
  FROM ret r
  JOIN `{{DS}}.v_options_reading` o
    ON o.symbol = r.symbol
   AND o.snapshot_date BETWEEN DATE_SUB(r.snapshot_date, INTERVAL 6 DAY) AND DATE_SUB(r.snapshot_date, INTERVAL 1 DAY)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY r.symbol, r.snapshot_date ORDER BY o.snapshot_date DESC) = 1
),
j AS (
  SELECT r.symbol, r.snapshot_date,
    r.next_week_pct - AVG(r.next_week_pct) OVER (PARTITION BY r.snapshot_date) AS next_week_excess_pp,
    r.past_week_pct - AVG(r.past_week_pct) OVER (PARTITION BY r.snapshot_date) AS past_week_rel_pp,
    r.overall_score,
    IF(r.overall_score IS NULL, NULL,
       GREATEST(0, LEAST(100, r.overall_score + IFNULL(1.5 * o.options_tilt, 0) - IF(o.short_ratio_z >= 2, 1, 0))))
      AS shadow_score,
    SAFE_DIVIDE(r.volume, r.avg_volume_30d) AS volume_ratio,
    r.from_52w_high_pct, r.rsi_14,
    o.options_tilt, o.skew_25d_pp, o.cp_iv_spread_pp, o.pc_volume_ratio, o.iv_pctile_1y, o.short_ratio_z,
    SAFE_DIVIDE(o.iv30, r.volatility_30d_pct) AS iv_hv_ratio
  FROM ret r
  LEFT JOIN opt o ON o.symbol = r.symbol AND o.snapshot_date = r.snapshot_date
),
long AS (
  SELECT j.snapshot_date, j.symbol, j.next_week_excess_pp, x.driver, x.ord, x.v
  FROM j, UNNEST([
    STRUCT('TFB overall score' AS driver, 1 AS ord, j.overall_score AS v),
    STRUCT('score with drivers (shadow)', 2, j.shadow_score),
    STRUCT('options tilt', 3, j.options_tilt),
    STRUCT('put skew (25-delta, pp)', 4, j.skew_25d_pp),
    STRUCT('calls minus puts IV (pp)', 5, j.cp_iv_spread_pp),
    STRUCT('puts/calls volume', 6, j.pc_volume_ratio),
    STRUCT('IV percentile (1 year)', 7, j.iv_pctile_1y),
    STRUCT('implied over realised volatility', 8, j.iv_hv_ratio),
    STRUCT('short volume z', 9, j.short_ratio_z),
    STRUCT('volume over 30-day average', 10, j.volume_ratio),
    STRUCT('distance from 52-week high (pp)', 11, j.from_52w_high_pct),
    STRUCT('RSI 14', 12, j.rsi_14),
    STRUCT('past week against the market (pp)', 13, j.past_week_rel_pp)
  ]) AS x
  WHERE x.v IS NOT NULL AND j.next_week_excess_pp IS NOT NULL
),
ranked AS (
  SELECT *,
    COUNT(*) OVER (PARTITION BY snapshot_date, driver) AS n,
    PERCENT_RANK() OVER (PARTITION BY snapshot_date, driver ORDER BY v) AS rank_v,
    PERCENT_RANK() OVER (PARTITION BY snapshot_date, driver ORDER BY next_week_excess_pp) AS rank_r,
    NTILE(5) OVER (PARTITION BY snapshot_date, driver ORDER BY v) AS q
  FROM long
),
daily AS (
  SELECT snapshot_date, driver, ANY_VALUE(ord) AS ord, ANY_VALUE(n) AS n,
    CORR(rank_v, rank_r) AS rank_ic,
    AVG(IF(q = 1, next_week_excess_pp, NULL)) AS q1,
    AVG(IF(q = 2, next_week_excess_pp, NULL)) AS q2,
    AVG(IF(q = 3, next_week_excess_pp, NULL)) AS q3,
    AVG(IF(q = 4, next_week_excess_pp, NULL)) AS q4,
    AVG(IF(q = 5, next_week_excess_pp, NULL)) AS q5
  FROM ranked
  WHERE n >= 20
  GROUP BY snapshot_date, driver
),
agg AS (
  SELECT driver, ANY_VALUE(ord) AS ord,
    COUNT(*) AS days,
    COUNT(DISTINCT DATE_TRUNC(snapshot_date, WEEK(MONDAY))) AS weeks,
    ROUND(AVG(n), 0) AS names_per_day,
    AVG(rank_ic) AS mean_ic,
    STDDEV_SAMP(rank_ic) AS sd_ic,
    AVG(IF(rank_ic > 0, 1, 0)) AS ic_positive_share,
    AVG(q1) AS q1, AVG(q2) AS q2, AVG(q3) AS q3, AVG(q4) AS q4, AVG(q5) AS q5,
    MIN(snapshot_date) AS first_day, MAX(snapshot_date) AS last_day
  FROM daily
  GROUP BY driver
),
drivers AS (
  SELECT d AS driver, o AS ord
  FROM UNNEST([
    STRUCT('TFB overall score' AS d, 1 AS o), STRUCT('score with drivers (shadow)', 2), STRUCT('options tilt', 3),
    STRUCT('put skew (25-delta, pp)', 4), STRUCT('calls minus puts IV (pp)', 5), STRUCT('puts/calls volume', 6),
    STRUCT('IV percentile (1 year)', 7), STRUCT('implied over realised volatility', 8),
    STRUCT('short volume z', 9), STRUCT('volume over 30-day average', 10),
    STRUCT('distance from 52-week high (pp)', 11), STRUCT('RSI 14', 12),
    STRUCT('past week against the market (pp)', 13)
  ])
)
SELECT
  d.ord, d.driver,
  IFNULL(a.days, 0) AS days,
  IFNULL(a.weeks, 0) AS weeks,
  a.names_per_day,
  ROUND(a.mean_ic, 4) AS mean_ic,
  ROUND(a.ic_positive_share, 3) AS ic_positive_share,
  ROUND(SAFE_DIVIDE(a.mean_ic, a.sd_ic) * SQRT(a.weeks), 2) AS ic_t,
  ROUND(a.q1, 3) AS q1_pp, ROUND(a.q2, 3) AS q2_pp, ROUND(a.q3, 3) AS q3_pp, ROUND(a.q4, 3) AS q4_pp,
  ROUND(a.q5, 3) AS q5_pp,
  ROUND(a.q5 - a.q1, 3) AS q5_minus_q1_pp,
  a.first_day, a.last_day,
  CASE
    WHEN IFNULL(a.weeks, 0) < 8 THEN 'too few weeks'
    WHEN a.mean_ic >= 0.03 AND a.ic_positive_share >= 0.55 THEN 'higher is better'
    WHEN a.mean_ic <= -0.03 AND a.ic_positive_share <= 0.45 THEN 'lower is better'
    ELSE 'no edge yet'
  END AS reading
FROM drivers d
LEFT JOIN agg a ON a.driver = d.driver;
