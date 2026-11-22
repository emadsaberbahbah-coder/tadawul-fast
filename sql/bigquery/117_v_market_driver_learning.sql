-- v_market_driver_learning: each market driver's record - for every state a reading of v_market_drivers can be in,
-- what the S&P 500 (SPY, total return) did over the next 5 and the next 20 sessions, against all days.
-- WHY (owner, 22 Nov 2026 19:53 Riyadh): the drivers are worth a place in the score only if they told something
-- about the market's next move. The archive holds about three years of each series (market_series_daily, 1,100
-- days), so this record exists from the first night, unlike a stock-level reading that starts empty.
-- HOW TO READ IT: days = trading days in that state; the next 5 sessions of neighbouring days overlap, so about
-- days / 5 of them are independent (indep_5d). lift_5d_pp = the state's mean minus the mean of every day.
-- A state with indep_5d under 10 is 'too few days'. Every row is in-sample: the states were written on
-- 22 Nov 2026 from common use, not fitted, but three years hold one or two market regimes - a guide, not proof.
-- The latest sessions have no outcome yet: they count in no mean and no hit rate (review of #1059).
-- is_now marks the state each driver is in on the latest day.
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_market_driver_learning`
OPTIONS (description = "TFB market driver record (views 1.23.0): for each state of each market driver (VIX level and curve, VVIX, SKEW, MOVE, credit, breadth, small caps, dollar, rates) the next 5 and 20 sessions of SPY against all days, about three years in-sample, with the state each driver is in now - research only")
AS
WITH m AS (
  SELECT * FROM `{{DS}}.v_market_drivers`
),
latest AS (
  SELECT * FROM m QUALIFY ROW_NUMBER() OVER (ORDER BY date DESC) = 1
),
long AS (
  SELECT m.date, x.driver, x.state, x.ord, m.spy_next5_pct, m.spy_next20_pct
  FROM m, UNNEST([
    STRUCT('VIX level' AS driver, m.vix_level_state AS state, 1 AS ord),
    STRUCT('VIX curve (VIX / VIX3M)', m.vix_term_state, 2),
    STRUCT('VVIX (volatility of VIX)', m.vvix_state, 3),
    STRUCT('SKEW (crash protection)', m.skew_state, 4),
    STRUCT('MOVE (Treasury volatility)', m.move_state, 5),
    STRUCT('Credit (HYG vs IEF, 20 days)', m.credit_state, 6),
    STRUCT('Breadth (RSP vs SPY, 20 days)', m.breadth_state, 7),
    STRUCT('Small caps (IWM vs SPY, 20 days)', m.small_caps_state, 8),
    STRUCT('Dollar (DXY, 20 days)', m.dollar_state, 9),
    STRUCT('10-year yield (5 days)', m.rates_state, 10)
  ]) AS x
  WHERE x.state IS NOT NULL
),
base AS (
  SELECT AVG(spy_next5_pct) AS b5, AVG(IF(spy_next5_pct IS NULL, NULL, IF(spy_next5_pct > 0, 1, 0))) AS bh5, AVG(spy_next20_pct) AS b20,
    AVG(IF(spy_next20_pct IS NULL, NULL, IF(spy_next20_pct > 0, 1, 0))) AS bh20
  FROM m WHERE spy_next5_pct IS NOT NULL
),
now AS (
  SELECT x.driver, x.state FROM latest, UNNEST([
    STRUCT('VIX level' AS driver, latest.vix_level_state AS state),
    STRUCT('VIX curve (VIX / VIX3M)', latest.vix_term_state),
    STRUCT('VVIX (volatility of VIX)', latest.vvix_state),
    STRUCT('SKEW (crash protection)', latest.skew_state),
    STRUCT('MOVE (Treasury volatility)', latest.move_state),
    STRUCT('Credit (HYG vs IEF, 20 days)', latest.credit_state),
    STRUCT('Breadth (RSP vs SPY, 20 days)', latest.breadth_state),
    STRUCT('Small caps (IWM vs SPY, 20 days)', latest.small_caps_state),
    STRUCT('Dollar (DXY, 20 days)', latest.dollar_state),
    STRUCT('10-year yield (5 days)', latest.rates_state)
  ]) AS x
),
agg AS (
  SELECT driver, state, ANY_VALUE(ord) AS ord,
    COUNTIF(spy_next5_pct IS NOT NULL) AS days,
    AVG(spy_next5_pct) AS mean_5d, AVG(IF(spy_next5_pct IS NULL, NULL, IF(spy_next5_pct > 0, 1, 0))) AS hit_5d,
    APPROX_QUANTILES(spy_next5_pct, 20)[OFFSET(1)] AS worst5_5d,
    AVG(spy_next20_pct) AS mean_20d, AVG(IF(spy_next20_pct IS NULL, NULL, IF(spy_next20_pct > 0, 1, 0))) AS hit_20d,
    COUNT(*) AS days_all, MIN(date) AS first_day, MAX(date) AS last_day
  FROM long
  GROUP BY driver, state
)
SELECT
  a.ord AS driver_order, a.driver, a.state, n.state IS NOT NULL AS is_now,
  a.days, CAST(FLOOR(a.days / 5) AS INT64) AS indep_5d,
  ROUND(a.mean_5d, 2) AS mean_5d_pct, ROUND(100 * a.hit_5d, 1) AS hit_5d_pct, ROUND(a.worst5_5d, 2) AS worst5_5d_pct,
  ROUND(a.mean_20d, 2) AS mean_20d_pct, ROUND(100 * a.hit_20d, 1) AS hit_20d_pct,
  ROUND(b.b5, 2) AS base_mean_5d_pct, ROUND(100 * b.bh5, 1) AS base_hit_5d_pct,
  ROUND(b.b20, 2) AS base_mean_20d_pct, ROUND(100 * b.bh20, 1) AS base_hit_20d_pct,
  ROUND(a.mean_5d - b.b5, 2) AS lift_5d_pp, ROUND(a.mean_20d - b.b20, 2) AS lift_20d_pp,
  a.first_day, a.last_day,
  CASE WHEN a.days < 50 THEN 'too few days'
       WHEN a.mean_5d - b.b5 <= -0.5 AND a.hit_5d < b.bh5 THEN 'weaker than usual'
       WHEN a.mean_5d - b.b5 >= 0.5 AND a.hit_5d > b.bh5 THEN 'stronger than usual'
       ELSE 'close to usual' END AS reading
FROM agg a
CROSS JOIN base b
LEFT JOIN now n ON n.driver = a.driver AND n.state = a.state;
