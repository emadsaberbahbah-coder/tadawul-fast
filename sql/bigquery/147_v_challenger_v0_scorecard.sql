-- v_challenger_v0_scorecard: the record of the shadow challenger v0 - per market and variant, on formation dates
-- whose results have closed: the rank correlation of the score with the result, the top fifth against the market's
-- benchmark net of cost, the top fifth against the bottom fifth, each with a t adjusted for overlap, and a reading.
-- WHY (week plan of 23 Nov 2026, package 5): the audit's promotion bar - a result per market, net of costs, against
-- that market's benchmark, on dates the rule never saw. Nothing is promoted from this view: it is read on 8 Dec.
-- VARIANTS: 'walk-forward weights' (score_wf, only the dates with trained weights: the earlier ones are warm-up and
-- the dates with no positive input have no view) and 'equal weights (fixed)' (score_eq, every date: the mix fixed
-- before any result was read, so it is out of sample too, but it learns nothing).
-- PER DATE (each date with at least 20 closed results counts once, whatever its number of names): ic = the
-- correlation of the score's percentile with label_pr (empty when the score does not vary); q5_net_pp = the mean
-- excess_w_pp (the result above the benchmark, held inside the date's 1st and 99th percentile) of quintile 5 minus
-- 0.50 points of cost (a round trip, as
-- v_forward_signals: 0.10 commission + 0.15 spread a side); spread_pp = quintile 5 minus quintile 1
-- in excess_w_pp, gross, and spread_net_pp minus 1.00 (a round trip on each side).
-- OVER DATES: dates, names (mean per date), ic_mean, ic_t_adj, q5_net_mean_pp, q5_t_adj, q5_beat_pct (dates on
-- which quintile 5 beat the benchmark net of cost), spread_mean_pp, spread_net_mean_pp, spread_t_adj. Each t is
-- the mean over its standard error across dates, divided by SQRT(21 / 5): a result runs 21 sessions and the dates
-- are a week (about 5 sessions) apart, so neighbouring dates share most of their window.
-- PERIOD: 'history (backtest)' for formation dates before 27 Nov 2026, 'forward from 2026-11-27' after.
-- THE READING (fixed on 27 Nov 2026, before the first read): 'THIN' under 26 dates; 'REVERSED' at ic_t_adj <= -2;
-- 'CANDIDATE (forward test)' at ic_t_adj >= 2 with q5_net_mean_pp > 0 and spread_t_adj >= 2; else 'NO EDGE'. A
-- history row is never evidence for switching anything on: only the forward rows can be.
-- Research only: no decision, gate or count reads this view. (views 1.34.0)
CREATE OR REPLACE VIEW `{{DS}}.v_challenger_v0_scorecard`
OPTIONS (description = "TFB shadow challenger v0 scorecard (views 1.34.0): per period, market and variant (walk-forward or equal weights), on closed results - rank correlation with the next 21 sessions, the top fifth against the market's benchmark net of 0.50 points, top minus bottom fifth, overlap-adjusted t and a fixed reading. Research view, never an order.")
AS
WITH v AS (
  SELECT IF(w.d < DATE '2026-11-27', 'history (backtest)', 'forward from 2026-11-27') AS period,
         w.market, w.d, w.symbol, x.variant, x.score, x.q, w.excess_w_pp, w.label_pr
  FROM `{{DS}}.v_challenger_v0_walkforward` AS w
  CROSS JOIN UNNEST([
    STRUCT('walk-forward weights' AS variant, IF(w.weights_basis = 'trained', w.score_wf, NULL) AS score,
           IF(w.weights_basis = 'trained', w.q_wf, NULL) AS q),
    STRUCT('equal weights (fixed)' AS variant, w.score_eq AS score, w.q_eq AS q)]) AS x
  WHERE w.label_pr IS NOT NULL AND x.score IS NOT NULL
),
pr AS (
  SELECT v.*, PERCENT_RANK() OVER (PARTITION BY v.period, v.market, v.variant, v.d ORDER BY v.score) AS score_pr
  FROM v
),
per_date AS (
  SELECT period, market, variant, d, COUNT(*) AS n,
         IF(COUNT(DISTINCT score) > 1, CORR(score_pr, label_pr), NULL) AS ic,
         AVG(IF(q = 5, excess_w_pp, NULL)) - 0.50 AS q5_net_pp,
         AVG(IF(q = 5, excess_w_pp, NULL)) - AVG(IF(q = 1, excess_w_pp, NULL)) AS spread_pp
  FROM pr
  GROUP BY period, market, variant, d
  HAVING COUNT(*) >= 20
),
agg AS (
  SELECT period, market, variant, COUNT(*) AS dates, MIN(d) AS first_date, MAX(d) AS last_date,
         ROUND(AVG(n), 1) AS names,
         AVG(ic) AS ic_mean, STDDEV_SAMP(ic) AS ic_sd, COUNT(ic) AS ic_n,
         AVG(q5_net_pp) AS q5_mean, STDDEV_SAMP(q5_net_pp) AS q5_sd, COUNT(q5_net_pp) AS q5_n,
         COUNTIF(q5_net_pp > 0) AS q5_beat, AVG(spread_pp) AS sp_mean, STDDEV_SAMP(spread_pp) AS sp_sd,
         COUNT(spread_pp) AS sp_n
  FROM per_date
  GROUP BY period, market, variant
),
t AS (
  SELECT a.*,
         SAFE_DIVIDE(a.ic_mean, SAFE_DIVIDE(a.ic_sd, SQRT(a.ic_n))) / SQRT(21 / 5) AS ic_t_adj,
         SAFE_DIVIDE(a.q5_mean, SAFE_DIVIDE(a.q5_sd, SQRT(a.q5_n))) / SQRT(21 / 5) AS q5_t_adj,
         SAFE_DIVIDE(a.sp_mean, SAFE_DIVIDE(a.sp_sd, SQRT(a.sp_n))) / SQRT(21 / 5) AS spread_t_adj
  FROM agg AS a
)
SELECT
  period, market, variant, dates, first_date, last_date, names,
  ROUND(ic_mean, 4) AS ic_mean, ROUND(ic_t_adj, 2) AS ic_t_adj,
  ROUND(q5_mean, 3) AS q5_net_mean_pp, ROUND(q5_t_adj, 2) AS q5_t_adj,
  ROUND(100 * SAFE_DIVIDE(q5_beat, q5_n), 1) AS q5_beat_pct,
  ROUND(sp_mean, 3) AS spread_mean_pp, ROUND(sp_mean - 1.00, 3) AS spread_net_mean_pp, ROUND(spread_t_adj, 2) AS spread_t_adj,
  CASE
    WHEN dates < 26 THEN 'THIN'
    WHEN ic_t_adj <= -2 THEN 'REVERSED'
    WHEN ic_t_adj >= 2 AND q5_mean > 0 AND spread_t_adj >= 2 THEN 'CANDIDATE (forward test)'
    ELSE 'NO EDGE'
  END AS reading
FROM t;
