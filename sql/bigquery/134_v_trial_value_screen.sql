-- v_trial_value_screen: the record of the value screen (v_value_screen) and, beside it, of the engine's own
-- INVESTABLE names - each night's list against all liquid US names of the same night, one and two weeks on.
-- WHY (owner, 26 Nov 2026 01:22 and 01:37 Riyadh): are there real chances at low prices that the labels do not show?
-- The list is measured before it may be used (owner's ground rules, 21 Nov 2026).
-- THE MEASURE: v_forward_universe gives every liquid market-page equity its price return 1W and 2W after each
-- night (price only, local currency). Per night and horizon the mean return of the US names is the market; an arm
-- counts on a night when it has 5 names or more with a return (and the market 100). Per period, horizon and arm:
-- days, names_per_day, mean_pp (the arm's mean return), edge_pp (the arm less the market, averaged over the
-- nights), days_ahead_pct (the share of nights above the market) and t_adj (the nightly edge's mean over its
-- standard error, divided by SQRT(7) for 1W and SQRT(14) for 2W - every calendar night is archived, so neighbouring
-- nights share most of their window).
-- THE READING (fixed on 26 Nov 2026): 'in-sample, not evidence' for the nights before 26 Nov 2026 (four weeks of one
-- falling market); for 'forward from 2026-11-26' 'collecting' under 10 nights, then 'works: beats the US names' at
-- t_adj >= 2, 'fails: trails the US names' at t_adj <= -2, else 'no clear edge'.
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_trial_value_screen`
OPTIONS (description = "TFB value-screen record (views 1.30.0): the nightly value screen and the engine's INVESTABLE names against all liquid US names of the same night, 1W and 2W on - nights, the mean return, the edge, and a fixed reading")
AS
WITH f AS (
  SELECT symbol, base_date, horizon, ret_pp, status
  FROM `{{DS}}.v_forward_universe`
  WHERE ENDS_WITH(symbol, '.US')
),
scr AS (
  SELECT snapshot_date, symbol FROM `{{DS}}.v_value_screen`
),
per_day AS (
  SELECT f.base_date, f.horizon,
    COUNT(*) AS us_n, AVG(f.ret_pp) AS us_mean,
    COUNTIF(s.symbol IS NOT NULL) AS scr_n, AVG(IF(s.symbol IS NOT NULL, f.ret_pp, NULL)) AS scr_mean,
    COUNTIF(f.status = 'INVESTABLE') AS inv_n, AVG(IF(f.status = 'INVESTABLE', f.ret_pp, NULL)) AS inv_mean
  FROM f
  LEFT JOIN scr s ON s.symbol = f.symbol AND s.snapshot_date = f.base_date
  GROUP BY f.base_date, f.horizon
),
arms AS (
  SELECT d.base_date, d.horizon, a.arm, a.n, a.m AS mean_pp, a.m - d.us_mean AS edge_pp
  FROM per_day d,
    UNNEST([STRUCT('value screen' AS arm, d.scr_n AS n, d.scr_mean AS m), ('engine INVESTABLE', d.inv_n, d.inv_mean)]) AS a
  WHERE a.n >= 5 AND d.us_n >= 100
),
agg AS (
  SELECT
    CASE WHEN base_date < DATE '2026-11-26' THEN 'history, rule fixed before reading it' ELSE 'forward from 2026-11-26' END AS period,
    horizon, arm,
    COUNT(*) AS days, ROUND(AVG(n), 1) AS names_per_day,
    ROUND(AVG(mean_pp), 2) AS mean_pp, ROUND(AVG(edge_pp), 2) AS edge_pp,
    ROUND(100 * COUNTIF(edge_pp > 0) / COUNT(*), 1) AS days_ahead_pct,
    ROUND(SAFE_DIVIDE(AVG(edge_pp), SAFE_DIVIDE(STDDEV(edge_pp), SQRT(COUNT(*)))) / SQRT(IF(horizon = '1W', 7, 14)), 2) AS t_adj
  FROM arms
  GROUP BY period, horizon, arm
)
SELECT
  period, horizon, arm, days, names_per_day, mean_pp, edge_pp, days_ahead_pct, t_adj,
  CASE
    WHEN period != 'forward from 2026-11-26' THEN 'in-sample, not evidence'
    WHEN days < 10 THEN 'collecting'
    WHEN t_adj >= 2 THEN 'works: beats the US names'
    WHEN t_adj <= -2 THEN 'fails: trails the US names'
    ELSE 'no clear edge'
  END AS reading
FROM agg;
