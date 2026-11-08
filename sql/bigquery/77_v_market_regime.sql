-- v_market_regime: one row per US session with the market's state at that
-- close, so a board seat, a ticket or a stop can be read against the regime
-- it was taken in. WHY (owner, 8 Nov 2026, after the 5 Nov -4.4 % day took
-- three stops in one afternoon): the archive holds the reference series but
-- never says what kind of day it was; the drawdown brake in the F-8 plan
-- needs that evidence before it is armed.
-- COLUMNS (SPY, price closes from market_series_daily):
--   date, spy_close
--   spy_r1_pct / spy_r5_pct / spy_r20_pct   close-to-close change over 1 /
--                                           5 / 20 sessions
--   spy_vs_50d_pct                          close against its 50-session
--                                           average (NULL for the first 49)
--   spy_vol20_pct                           annualised standard deviation of
--                                           the last 20 daily changes
--   spy_dd_from_high_pct                    distance from the 250-session high
--   vix_close                               ^VIX close (series 14; NULL
--                                           before it is archived)
--   regime                                  one label, first match wins:
--     crash_day   spy_r1_pct <= -2
--     drawdown    spy_r5_pct <= -3 or spy_dd_from_high_pct <= -8
--     stress      vix_close >= 25 or spy_vol20_pct >= 25
--     weak        spy_vs_50d_pct < 0
--     calm        everything else
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_market_regime`
OPTIONS (description = "TFB Looker v6: one row per US session - SPY 1 / 5 / 20-session change, distance from the 50-session average and the 250-session high, 20-session volatility, the VIX close and one regime label (crash_day / drawdown / stress / weak / calm)")
AS
WITH spy AS (
  SELECT date, close
  FROM `{{DS}}.market_series_daily`
  WHERE symbol = 'SPY' AND close IS NOT NULL AND close > 0
),
vix AS (
  SELECT date, close AS vix_close
  FROM `{{DS}}.market_series_daily`
  WHERE symbol = '^VIX' AND close IS NOT NULL
),
lagged AS (
  SELECT
    s.date,
    s.close,
    LAG(s.close, 1) OVER (ORDER BY s.date) AS c1,
    LAG(s.close, 5) OVER (ORDER BY s.date) AS c5,
    LAG(s.close, 20) OVER (ORDER BY s.date) AS c20
  FROM spy AS s
),
w AS (
  SELECT
    l.date,
    l.close AS spy_close,
    l.c1,
    l.c5,
    l.c20,
    COUNT(*) OVER (ORDER BY l.date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS n50,
    AVG(l.close) OVER (ORDER BY l.date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS ma50,
    MAX(l.close) OVER (ORDER BY l.date ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) AS high250,
    STDDEV_SAMP(SAFE_DIVIDE(l.close, l.c1) - 1)
      OVER (ORDER BY l.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sd20,
    COUNT(*) OVER (ORDER BY l.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS n20
  FROM lagged AS l
),
m AS (
  SELECT
    w.date,
    w.spy_close,
    ROUND((SAFE_DIVIDE(w.spy_close, w.c1) - 1) * 100, 4) AS spy_r1_pct,
    ROUND((SAFE_DIVIDE(w.spy_close, w.c5) - 1) * 100, 4) AS spy_r5_pct,
    ROUND((SAFE_DIVIDE(w.spy_close, w.c20) - 1) * 100, 4) AS spy_r20_pct,
    IF(w.n50 = 50, ROUND((SAFE_DIVIDE(w.spy_close, w.ma50) - 1) * 100, 4), NULL) AS spy_vs_50d_pct,
    IF(w.n20 = 20, ROUND(w.sd20 * SQRT(252) * 100, 4), NULL) AS spy_vol20_pct,
    ROUND((SAFE_DIVIDE(w.spy_close, w.high250) - 1) * 100, 4) AS spy_dd_from_high_pct,
    v.vix_close
  FROM w
  LEFT JOIN vix AS v ON v.date = w.date
)
SELECT
  m.*,
  CASE
    WHEN m.spy_r1_pct <= -2 THEN 'crash_day'
    WHEN m.spy_r5_pct <= -3 OR m.spy_dd_from_high_pct <= -8 THEN 'drawdown'
    WHEN m.vix_close >= 25 OR m.spy_vol20_pct >= 25 THEN 'stress'
    WHEN m.spy_vs_50d_pct < 0 THEN 'weak'
    ELSE 'calm'
  END AS regime
FROM m;
