-- v_trial_c1: the first controlled challenger, C-1 "evidence-weighted
-- confidence", measured on the project's own buy calls against the champion
-- (all buy calls). Rule fixed on 14 Nov 2026 BEFORE any forward data:
--   C-1 keeps a buy call (BUY / STRONG BUY / ACCUMULATE) unless
--     (a) its confidence is HIGH or its forecast reliability is 80 or more, or
--     (b) it is a Tadawul call whose own 1-month return to the call date did
--         not beat TASI's (a missing price counts as not beating).
-- WHY: the 14 Nov full review found HIGH confidence (-0.57 pp vs market,
-- 1,303 calls), reliability 80+ (-1.12 pp, 580) and Tadawul buy calls
-- (-1.38 pp, 1,064) losing to their market. That finding is IN-SAMPLE; only
-- calls recorded from 15 Nov 2026 count as evidence (period 'forward').
-- PROMOTION RULE (fixed now): forward, at least 20 call days and 40 matured
-- C-1 calls, and C-1's mean daily edge above the champion's with t >= 2.
-- Even then the owner decides; nothing here changes a page or an order.
-- COLUMNS: period ('forward from 2026-11-15' / 'in-sample, not evidence'),
-- horizon (1W / 2W / 1M / 3M / ALL), arm (CHAMPION / C1 / LEFT_OUT /
-- C1_MINUS_CHAMPION), calls, days, edge_pp (mean return minus the market
-- benchmark; for C1_MINUS_CHAMPION the mean daily difference), right_pct,
-- t_stat (C1_MINUS_CHAMPION only), first_day, last_day, reading.
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_trial_c1`
OPTIONS (description = "TFB trial C-1 (fixed 14 Nov 2026): buy calls without HIGH confidence / reliability 80+ and Tadawul buys only when beating TASI over 1 month, against all buy calls, forward evidence from 15 Nov 2026")
AS
WITH base AS (
  SELECT record_id, bars_symbol, market, horizon, recorded_date, confidence,
    entry_forecast_reliability AS rel, excess_pp AS edge
  FROM `{{DS}}.v_outcome_excess`
  WHERE in_engine_cohort AND excess_pp IS NOT NULL
    AND UPPER(IFNULL(entry_recommendation, '')) IN ('BUY', 'STRONG BUY', 'ACCUMULATE')
),
tasi_d AS (
  SELECT 'Tadawul' AS mkt, date, ANY_VALUE(COALESCE(adj_close, close)) AS c
  FROM `{{DS}}.market_series_daily` WHERE symbol = '^TASI.SR' AND COALESCE(adj_close, close) > 0
  GROUP BY date
),
calls AS (
  SELECT b.record_id, ANY_VALUE(b.market) AS market, ANY_VALUE(b.horizon) AS horizon,
    ANY_VALUE(b.recorded_date) AS recorded_date, ANY_VALUE(b.confidence) AS confidence,
    ANY_VALUE(b.rel) AS rel, ANY_VALUE(b.edge) AS edge,
    ARRAY_AGG(IF(s.date <= b.recorded_date, COALESCE(s.adj_close, s.close), NULL) IGNORE NULLS
              ORDER BY s.date DESC LIMIT 1)[SAFE_OFFSET(0)] AS p0,
    ARRAY_AGG(IF(s.date <= DATE_SUB(b.recorded_date, INTERVAL 30 DAY), COALESCE(s.adj_close, s.close), NULL)
              IGNORE NULLS ORDER BY s.date DESC LIMIT 1)[SAFE_OFFSET(0)] AS p30,
    ARRAY_AGG(IF(t.date <= b.recorded_date, t.c, NULL) IGNORE NULLS ORDER BY t.date DESC LIMIT 1)[SAFE_OFFSET(0)] AS i0,
    ARRAY_AGG(IF(t.date <= DATE_SUB(b.recorded_date, INTERVAL 30 DAY), t.c, NULL) IGNORE NULLS
              ORDER BY t.date DESC LIMIT 1)[SAFE_OFFSET(0)] AS i30
  FROM base b
  LEFT JOIN `{{DS}}.symbol_bars_daily` s
    ON b.market = 'Tadawul' AND s.symbol = b.bars_symbol
   AND s.date BETWEEN DATE_SUB(b.recorded_date, INTERVAL 45 DAY) AND b.recorded_date
  LEFT JOIN tasi_d t
    ON t.mkt = b.market AND t.date BETWEEN DATE_SUB(b.recorded_date, INTERVAL 45 DAY) AND b.recorded_date
  GROUP BY b.record_id
),
f AS (
  SELECT *,
    IF(recorded_date >= DATE '2026-11-15', 'forward from 2026-11-15', 'in-sample, not evidence') AS period,
    NOT (UPPER(IFNULL(confidence, '')) = 'HIGH' OR IFNULL(rel, 0) >= 80)
      AND (market != 'Tadawul' OR IFNULL(SAFE_DIVIDE(p0, p30) - SAFE_DIVIDE(i0, i30), 0) > 0) AS c1_keep
  FROM calls
),
daily AS (
  SELECT period, IF(GROUPING(horizon) = 1, 'ALL', horizon) AS hz, recorded_date,
    COUNT(*) AS n_all, SUM(edge) AS s_all, COUNTIF(edge > 0) AS w_all,
    COUNTIF(c1_keep) AS n_c1, SUM(IF(c1_keep, edge, 0)) AS s_c1, COUNTIF(c1_keep AND edge > 0) AS w_c1,
    COUNTIF(NOT c1_keep) AS n_out, SUM(IF(c1_keep, 0, edge)) AS s_out, COUNTIF(NOT c1_keep AND edge > 0) AS w_out
  FROM f
  GROUP BY GROUPING SETS ((period, horizon, recorded_date), (period, recorded_date))
),
agg AS (
  SELECT period, hz,
    SUM(n_all) AS n_all, SUM(s_all) AS s_all, SUM(w_all) AS w_all, COUNTIF(n_all > 0) AS d_all,
    SUM(n_c1) AS n_c1, SUM(s_c1) AS s_c1, SUM(w_c1) AS w_c1, COUNTIF(n_c1 > 0) AS d_c1,
    SUM(n_out) AS n_out, SUM(s_out) AS s_out, SUM(w_out) AS w_out, COUNTIF(n_out > 0) AS d_out,
    AVG(IF(n_c1 > 0, s_c1 / n_c1 - s_all / n_all, NULL)) AS dm,
    STDDEV_SAMP(IF(n_c1 > 0, s_c1 / n_c1 - s_all / n_all, NULL)) AS dsd,
    MIN(recorded_date) AS first_day, MAX(recorded_date) AS last_day
  FROM daily GROUP BY period, hz
),
rows_ AS (
  SELECT period, hz AS horizon, arm,
    CASE arm WHEN 'CHAMPION' THEN n_all WHEN 'C1' THEN n_c1 WHEN 'LEFT_OUT' THEN n_out ELSE n_c1 END AS calls,
    CASE arm WHEN 'CHAMPION' THEN d_all WHEN 'C1' THEN d_c1 WHEN 'LEFT_OUT' THEN d_out ELSE d_c1 END AS days,
    ROUND(CASE arm WHEN 'CHAMPION' THEN SAFE_DIVIDE(s_all, n_all) WHEN 'C1' THEN SAFE_DIVIDE(s_c1, n_c1)
                   WHEN 'LEFT_OUT' THEN SAFE_DIVIDE(s_out, n_out) ELSE dm END, 2) AS edge_pp,
    ROUND(100 * CASE arm WHEN 'CHAMPION' THEN SAFE_DIVIDE(w_all, n_all) WHEN 'C1' THEN SAFE_DIVIDE(w_c1, n_c1)
                         WHEN 'LEFT_OUT' THEN SAFE_DIVIDE(w_out, n_out) END, 1) AS right_pct,
    IF(arm = 'C1_MINUS_CHAMPION', ROUND(SAFE_DIVIDE(dm, SAFE_DIVIDE(dsd, SQRT(d_c1))), 2), NULL) AS t_stat,
    first_day, last_day
  FROM agg CROSS JOIN UNNEST(['CHAMPION', 'C1', 'LEFT_OUT', 'C1_MINUS_CHAMPION']) AS arm
)
SELECT *,
  CASE
    WHEN arm != 'C1_MINUS_CHAMPION' THEN NULL
    WHEN period != 'forward from 2026-11-15' THEN 'in-sample: shows why the rule was chosen, not evidence'
    WHEN days < 20 OR calls < 40 THEN CONCAT('collecting: ', CAST(days AS STRING), ' of 20 days, ',
                                             CAST(calls AS STRING), ' of 40 calls')
    WHEN t_stat >= 2 AND edge_pp > 0 THEN 'C-1 ahead: the owner may consider it'
    WHEN edge_pp <= 0 THEN 'C-1 not ahead: keep the champion'
    ELSE 'C-1 ahead but not yet clear (t under 2): keep collecting'
  END AS reading
FROM rows_;
