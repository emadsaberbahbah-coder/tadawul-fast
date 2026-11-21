-- v_market_trial_summary: the scorecard of the market readings, one row per
-- period (backtest / forward) and horizon (5 / 10 / 20 closes). WHY (owner, 21 Nov
-- 2026 11:03 Riyadh, the build list): "Observe for two weeks (10 sessions),
-- then review with the owner, and switch on only what proved itself." This is
-- the table the review reads; every figure counts the buyable markets only
-- (oil is a reading only) and only outcomes that exist (matured).
-- COLUMNS:
--   days                   US sessions with a reading
--   open_n / open_mean_pct / open_up_pct / open_p5_pct    the open markets'
--                          funds: how many outcomes, their mean change, the
--                          share that rose, and the worst 5 % (the 5th
--                          percentile: the loss the hold is meant to avoid)
--   hold_n / hold_mean_pct / hold_up_pct / hold_p5_pct    the same for the
--                          markets on hold
--   open_minus_hold_pp     open_mean_pct - hold_mean_pct: the hold earns its
--                          place when this is above 0
--   top_n / top_mean_pct   the day's first open market (buyable_rank 1) when
--                          one is open
--   top_us_n               of those, the days it was the US itself (no switch)
--   top_vs_spy_pp / top_beat_spy_pct        on the other days, that market's fund
--                          against SPY over the same span: what a switch out of
--                          the US market would have changed
--   cash_days / cash_spy_mean_pct           days with no open market: SPY's mean
--                          change after them (below 0 = cash avoided a loss)
--   ic_days / mean_ic      the mean over days (5 markets or more with an
--                          outcome) of the rank correlation between the trend
--                          score and the fund's change: above 0 = the order helps
-- The windows of consecutive days overlap: the means are fair, the counts are
-- not independent trials.
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_market_trial_summary`
OPTIONS (description = "TFB markets v1: the scorecard of the market readings by period (backtest / forward from 23 Nov 2026) and horizon (5 / 10 / 20 closes): open against held markets, the day's first open market against SPY, SPY after the days with no open market (cash), and the rank correlation of the trend score with the outcome. Observe only.")
AS
WITH t AS (
  SELECT *
  FROM `{{DS}}.v_market_trial`
  WHERE buyable
),
h AS (
  SELECT period, 5 AS horizon, decision_day, market_id, state, buyable_rank, trend_score, open_markets,
         f5_pct AS f, spy_f5_pct AS spy
  FROM t
  UNION ALL
  SELECT period, 10, decision_day, market_id, state, buyable_rank, trend_score, open_markets,
         f10_pct, spy_f10_pct
  FROM t
  UNION ALL
  SELECT period, 20, decision_day, market_id, state, buyable_rank, trend_score, open_markets,
         f20_pct, spy_f20_pct
  FROM t
),
base AS (
  SELECT
    period,
    horizon,
    COUNT(DISTINCT decision_day) AS days,
    COUNTIF(state = 'open' AND f IS NOT NULL) AS open_n,
    ROUND(AVG(IF(state = 'open', f, NULL)), 2) AS open_mean_pct,
    ROUND(SAFE_DIVIDE(COUNTIF(state = 'open' AND f > 0), COUNTIF(state = 'open' AND f IS NOT NULL)) * 100, 1)
      AS open_up_pct,
    ROUND(APPROX_QUANTILES(IF(state = 'open', f, NULL), 20)[SAFE_OFFSET(1)], 2) AS open_p5_pct,
    COUNTIF(state = 'hold' AND f IS NOT NULL) AS hold_n,
    ROUND(AVG(IF(state = 'hold', f, NULL)), 2) AS hold_mean_pct,
    ROUND(SAFE_DIVIDE(COUNTIF(state = 'hold' AND f > 0), COUNTIF(state = 'hold' AND f IS NOT NULL)) * 100, 1)
      AS hold_up_pct,
    ROUND(APPROX_QUANTILES(IF(state = 'hold', f, NULL), 20)[SAFE_OFFSET(1)], 2) AS hold_p5_pct
  FROM h
  GROUP BY period, horizon
),
top AS (
  SELECT
    period,
    horizon,
    COUNTIF(f IS NOT NULL) AS top_n,
    ROUND(AVG(f), 2) AS top_mean_pct,
    COUNTIF(market_id = 'us' AND f IS NOT NULL) AS top_us_n,
    ROUND(AVG(IF(market_id != 'us', f - spy, NULL)), 2) AS top_vs_spy_pp,
    ROUND(SAFE_DIVIDE(COUNTIF(market_id != 'us' AND f > spy),
                      COUNTIF(market_id != 'us' AND f IS NOT NULL AND spy IS NOT NULL)) * 100, 1)
      AS top_beat_spy_pct
  FROM h
  WHERE state = 'open' AND buyable_rank = 1
  GROUP BY period, horizon
),
cash AS (
  SELECT
    period,
    horizon,
    COUNT(*) AS cash_days,
    ROUND(AVG(spy), 2) AS cash_spy_mean_pct
  FROM (
    SELECT DISTINCT period, horizon, decision_day, spy
    FROM h
    WHERE open_markets = 0 AND spy IS NOT NULL
  )
  GROUP BY period, horizon
),
ranked AS (
  SELECT
    period,
    horizon,
    decision_day,
    RANK() OVER (PARTITION BY period, horizon, decision_day ORDER BY trend_score) AS rs,
    RANK() OVER (PARTITION BY period, horizon, decision_day ORDER BY f) AS rf
  FROM h
  WHERE f IS NOT NULL AND trend_score IS NOT NULL
),
day_ic AS (
  SELECT period, horizon, decision_day, CORR(rs, rf) AS ic
  FROM ranked
  GROUP BY period, horizon, decision_day
  HAVING COUNT(*) >= 5
),
ic AS (
  SELECT period, horizon, COUNTIF(ic IS NOT NULL) AS ic_days, ROUND(AVG(ic), 3) AS mean_ic
  FROM day_ic
  GROUP BY period, horizon
)
SELECT
  b.period, b.horizon, b.days,
  b.open_n, b.open_mean_pct, b.open_up_pct, b.open_p5_pct,
  b.hold_n, b.hold_mean_pct, b.hold_up_pct, b.hold_p5_pct,
  ROUND(b.open_mean_pct - b.hold_mean_pct, 2) AS open_minus_hold_pp,
  COALESCE(tp.top_n, 0) AS top_n, tp.top_mean_pct, COALESCE(tp.top_us_n, 0) AS top_us_n, tp.top_vs_spy_pp,
  tp.top_beat_spy_pct,
  COALESCE(ca.cash_days, 0) AS cash_days, ca.cash_spy_mean_pct,
  COALESCE(i.ic_days, 0) AS ic_days, i.mean_ic
FROM base AS b
LEFT JOIN top AS tp ON tp.period = b.period AND tp.horizon = b.horizon
LEFT JOIN cash AS ca ON ca.period = b.period AND ca.horizon = b.horizon
LEFT JOIN ic AS i ON i.period = b.period AND i.horizon = b.horizon;
