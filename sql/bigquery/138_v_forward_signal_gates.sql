-- v_forward_signal_gates: what the gates kept out and what they let in - per period, market, horizon, gate and
-- block reason: how many signals, how many went on to beat their benchmark net of cost (blocked winners among the
-- names kept out) and how many did not (admitted losers among the names let in).
-- WHY (week plan of 23 Nov 2026, package 4): a gate is right when the names it keeps out trail their market and the
-- names it lets in beat it. The count of blocked winners is the price of a gate; the count of admitted losers is
-- what it missed. Base: v_forward_signals (the long side, benchmark and cost as defined there).
-- THE ROWS: gate 'admitted' has one block_reason_class, '(admitted)'; gate 'kept out' has one row per reason class
-- and one '(all reasons)'. Signals with no tier recorded are left out.
-- THE MEASURE: signals (all, open ones too), symbols, with_outcome (a return and a benchmark), days (distinct
-- recording dates with an outcome), winners (active_net_pp > 0), losers (the rest), winner_pct, blocked_winners
-- (winners of a 'kept out' row), admitted_losers (losers of the 'admitted' row), mean_stock_pp, mean_bench_pp,
-- mean_active_net_pp and t_adj (the mean over distinct recording dates - a date's signals averaged first - over
-- its standard error, divided by SQRT of the horizon in days, 7 / 14 / 30 / 91: neighbouring dates share most of
-- their window).
-- THE READING (fixed on 26 Nov 2026): 'in-sample, not evidence' for every row but 'forward from 2026-11-26'; for the
-- forward rows 'collecting' under 30 outcomes or 8 dates; then, for a 'kept out' row, 'works: the names kept out
-- trailed their benchmark' at t_adj <= -2 and 'fails: the names kept out beat their benchmark' at t_adj >= 2; for
-- the 'admitted' row, 'works: the admitted names beat their benchmark' at t_adj >= 2 and 'fails: the admitted names
-- trailed their benchmark' at t_adj <= -2; else 'no clear edge'.
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_forward_signal_gates`
OPTIONS (description = "TFB gate record (views 1.31.0): per period, market, horizon, gate and block reason - signals, outcomes, blocked winners and admitted losers against the benchmark net of cost, the mean return above the benchmark, and a fixed reading - research only")
AS
WITH f AS (
  SELECT s.period, s.market_group, s.horizon, s.gate, r AS block_reason_class, s.recorded_date, s.symbol,
    s.stock_roi_pp, s.bench_roi_pp, s.active_net_pp
  FROM `{{DS}}.v_forward_signals` s,
    UNNEST(IF(s.gate = 'kept out', [s.block_reason_class, '(all reasons)'], [s.block_reason_class])) AS r
  WHERE s.gate IN ('admitted', 'kept out')
),
per_day AS (
  SELECT period, market_group, horizon, gate, block_reason_class, recorded_date, AVG(active_net_pp) AS m
  FROM f
  WHERE active_net_pp IS NOT NULL
  GROUP BY period, market_group, horizon, gate, block_reason_class, recorded_date
),
d AS (
  SELECT period, market_group, horizon, gate, block_reason_class, COUNT(*) AS days, AVG(m) AS e_mean, STDDEV(m) AS e_sd
  FROM per_day
  GROUP BY period, market_group, horizon, gate, block_reason_class
),
a AS (
  SELECT period, market_group, horizon, gate, block_reason_class,
    COUNT(*) AS signals, COUNT(DISTINCT symbol) AS symbols,
    COUNTIF(active_net_pp IS NOT NULL) AS with_outcome,
    COUNTIF(active_net_pp > 0) AS winners,
    COUNTIF(active_net_pp <= 0) AS losers,
    ROUND(AVG(IF(active_net_pp IS NOT NULL, stock_roi_pp, NULL)), 2) AS mean_stock_pp,
    ROUND(AVG(IF(active_net_pp IS NOT NULL, bench_roi_pp, NULL)), 2) AS mean_bench_pp,
    ROUND(AVG(active_net_pp), 2) AS mean_active_net_pp
  FROM f
  GROUP BY period, market_group, horizon, gate, block_reason_class
),
j AS (
  SELECT a.*, IFNULL(d.days, 0) AS days,
    ROUND(SAFE_DIVIDE(d.e_mean, SAFE_DIVIDE(d.e_sd, SQRT(d.days)))
          / SQRT(CASE a.horizon WHEN '1W' THEN 7 WHEN '2W' THEN 14 WHEN '1M' THEN 30 ELSE 91 END), 2) AS t_adj
  FROM a LEFT JOIN d USING (period, market_group, horizon, gate, block_reason_class)
)
SELECT
  period, market_group, horizon, gate, block_reason_class, signals, symbols, with_outcome, days, winners, losers,
  ROUND(100 * SAFE_DIVIDE(winners, with_outcome), 1) AS winner_pct,
  IF(gate = 'kept out', winners, NULL) AS blocked_winners,
  IF(gate = 'admitted', losers, NULL) AS admitted_losers,
  mean_stock_pp, mean_bench_pp, mean_active_net_pp, t_adj,
  CASE
    WHEN period != 'forward from 2026-11-26' THEN 'in-sample, not evidence'
    WHEN with_outcome < 30 OR days < 8 THEN 'collecting'
    WHEN gate = 'kept out' AND t_adj <= -2 THEN 'works: the names kept out trailed their benchmark'
    WHEN gate = 'kept out' AND t_adj >= 2 THEN 'fails: the names kept out beat their benchmark'
    WHEN gate = 'admitted' AND t_adj >= 2 THEN 'works: the admitted names beat their benchmark'
    WHEN gate = 'admitted' AND t_adj <= -2 THEN 'fails: the admitted names trailed their benchmark'
    ELSE 'no clear edge'
  END AS reading
FROM j;
