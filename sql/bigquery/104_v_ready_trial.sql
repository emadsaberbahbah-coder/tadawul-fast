-- v_ready_trial: the scorecard of the ready list and of the market-by-market
-- hold on the names themselves - what the qualified names did next, by the
-- state of the market each tracks. WHY (owner, 21 Nov 2026 11:03 Riyadh, the
-- build list): "Accuracy: log and score everything. Observe for two weeks (10
-- sessions), then review with the owner, and switch on only what proved
-- itself." The live US rule rests on the names (v_regime_outcomes, 18 Nov:
-- picks made while the US was weak did worse), not on the index; the market
-- rule must be judged the same way, so this view scores the names.
-- THE OUTCOME: v_forward_universe - the name's price change from the day to its
--   first archived price one week (1W) or two weeks (2W) later, local
--   currency, and the same change less the median of every liquid market-page
--   equity that day (excess_pp). Only names of the Global_Markets and
--   Market_Leaders pages are scored there; a horizon that has not arrived has
--   no row (matured outcomes only).
-- period: 'forward' from Monday 23 Nov 2026 (the owner's 10-session review),
--   'backtest' before it (the same rules read back on the archive).
-- grp: 'all qualified'; 'market open' / 'market hold' / 'market not read';
--   'ready list (top 3)' / 'other qualified'; 'market rule holds it' / 'market
--   rule does not'; 'live US rule holds it' / 'live US rule does not' (with its
--   workflow defaults; see v_ready_list). Each name-day counts once in each of
--   the five groupings (one pass over the names: UNNEST, not five reads).
-- COLUMNS: period, horizon, grp, n (name-days), days, mean_ret_pp,
--   mean_excess_pp, up_pct (share that rose), p5_ret_pp (the worst 5 %: the
--   exact 5th percentile, PERCENTILE_CONT - the same on every read).
--   Consecutive days overlap: read the means.
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it. Observe only.
CREATE OR REPLACE VIEW `{{DS}}.v_ready_trial`
OPTIONS (description = "TFB markets v1: what the qualified names did over the next week and two weeks (v_forward_universe), by the state of the market each tracks, on or off the ready list, and whether the market rule or the live US rule would hold it - mean change, mean change against the day's median name, share that rose, worst 5 % - 'forward' from 23 Nov 2026, 'backtest' before it. Observe only.")
AS
WITH r AS (
  SELECT snapshot_date, symbol, market_state, in_ready, market_rule_holds, live_us_rule_holds
  FROM `{{DS}}.v_ready_list`
),
f AS (
  SELECT symbol, base_date, horizon, ret_pp, market_median_pp
  FROM `{{DS}}.v_forward_universe`
),
j AS (
  SELECT
    IF(r.snapshot_date >= DATE '2026-11-23', 'forward', 'backtest') AS period,
    f.horizon,
    r.snapshot_date,
    r.market_state,
    r.in_ready,
    r.market_rule_holds,
    r.live_us_rule_holds,
    f.ret_pp,
    f.ret_pp - f.market_median_pp AS excess_pp
  FROM r
  JOIN f ON f.symbol = r.symbol AND f.base_date = r.snapshot_date
),
g AS (
  SELECT j.period, j.horizon, grp, j.snapshot_date, j.ret_pp, j.excess_pp
  FROM j, UNNEST([
    'all qualified',
    CONCAT('market ', j.market_state),
    IF(j.in_ready, 'ready list (top 3)', 'other qualified'),
    IF(j.market_rule_holds, 'market rule holds it', 'market rule does not'),
    IF(j.live_us_rule_holds, 'live US rule holds it', 'live US rule does not')
  ]) AS grp
),
q AS (
  SELECT g.*, PERCENTILE_CONT(g.ret_pp, 0.05) OVER (PARTITION BY g.period, g.horizon, g.grp) AS p5
  FROM g
)
SELECT
  period,
  horizon,
  grp,
  COUNT(*) AS n,
  COUNT(DISTINCT snapshot_date) AS days,
  ROUND(AVG(ret_pp), 2) AS mean_ret_pp,
  ROUND(AVG(excess_pp), 2) AS mean_excess_pp,
  ROUND(SAFE_DIVIDE(COUNTIF(ret_pp > 0), COUNT(*)) * 100, 1) AS up_pct,
  ROUND(MAX(p5), 2) AS p5_ret_pp
FROM q
GROUP BY period, horizon, grp;
