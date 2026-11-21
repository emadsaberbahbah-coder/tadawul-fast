-- v_market_rank: on every calendar day, each market's latest condition and
-- the day's order of the markets by trend and risk. WHY (owner, 21 Nov 2026
-- 11:03 Riyadh, the build list): "A daily ranking of the markets by trend and
-- risk. The Daily Plan's hold then works market by market. It follows the
-- market an asset tracks, not the currency it trades in. When every market is
-- weak, cash stays cash." Observe only: shown and logged, it controls no buy.
-- THE DAY: as_of runs over every calendar day from the first close of
--   v_market_condition to its last; each market carries its latest labelled
--   close on or before that day (days_old calendar days back; a close more
--   than 6 days old is not a reading - a holiday or a series that stopped -
--   and the market is left out of that day). The markets close at different
--   hours: on a US session day the Asian, Saudi and European closes of the
--   same date came first, so nothing in a row is later than its day.
-- state        open = the market is calm (the same test as the Daily Plan's US
--              rule, which holds a buy unless the US is calm); hold = any other
--              label (crash_day, drawdown, stress or weak)
-- rank         1 = first: the open markets before the held ones, each group by
--              trend_score (highest first), then market_order
-- buyable_rank the same order counted over the markets with a fund (oil is a
--              reading only); the ready list takes its names from the top
-- open_markets the buyable markets that are open that day, of buyable_markets
-- verdict      'ALL ON HOLD - cash stays cash' when none is open, else
--              '<n> of <m> open'
-- fund_r5_pct / fund_r20_pct  the fund's own change (dividend-adjusted) over
--              its last 5 / 20 closes on or before the day (6 days at most):
--              the fund can differ from the market it follows (currency, make-up)
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_market_rank`
OPTIONS (description = "TFB markets v1: one row per calendar day and market - its latest condition (v_market_condition, 6 days at most), open (calm) or hold, and the day's rank by trend and risk (open markets first, each group by trend score), with the count of open markets and the verdict (ALL ON HOLD - cash stays cash when none is open). Observe only.")
AS
WITH c AS (
  SELECT *
  FROM `{{DS}}.v_market_condition`
  WHERE label IS NOT NULL
),
bounds AS (
  SELECT MIN(date) AS d0, MAX(date) AS d1
  FROM c
),
days AS (
  SELECT as_of
  FROM bounds, UNNEST(GENERATE_DATE_ARRAY(bounds.d0, bounds.d1)) AS as_of
),
grid AS (
  SELECT d.as_of, m.market_id
  FROM days AS d
  CROSS JOIN (SELECT DISTINCT market_id FROM c) AS m
),
filled AS (
  SELECT
    g.as_of,
    g.market_id,
    LAST_VALUE(c.date IGNORE NULLS)
      OVER (PARTITION BY g.market_id ORDER BY g.as_of ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session
  FROM grid AS g
  LEFT JOIN c ON c.market_id = g.market_id AND c.date = g.as_of
),
fund AS (
  SELECT
    s.symbol,
    s.date,
    ROUND((SAFE_DIVIDE(COALESCE(s.adj_close, s.close),
                       LAG(COALESCE(s.adj_close, s.close), 5) OVER (PARTITION BY s.symbol ORDER BY s.date)) - 1)
          * 100, 4) AS f5,
    ROUND((SAFE_DIVIDE(COALESCE(s.adj_close, s.close),
                       LAG(COALESCE(s.adj_close, s.close), 20) OVER (PARTITION BY s.symbol ORDER BY s.date)) - 1)
          * 100, 4) AS f20
  FROM `{{DS}}.market_series_daily` AS s
  WHERE s.close > 0 AND s.symbol IN ('SPY', 'KSA', 'VGK', 'EWJ', 'MCHI', 'GLD', 'IEF', 'SPSK', 'VNQ')
  UNION ALL
  SELECT 'SA_REITS', date, r5_pct, r20_pct
  FROM c
  WHERE market_id = 'reits_sa'
),
fund_grid AS (
  SELECT d.as_of, f.symbol
  FROM days AS d
  CROSS JOIN (SELECT DISTINCT symbol FROM fund) AS f
),
fund_filled AS (
  SELECT
    g.as_of,
    g.symbol,
    LAST_VALUE(f.date IGNORE NULLS)
      OVER (PARTITION BY g.symbol ORDER BY g.as_of ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fdate
  FROM fund_grid AS g
  LEFT JOIN fund AS f ON f.symbol = g.symbol AND f.date = g.as_of
),
r AS (
  SELECT
    fl.as_of,
    DATE_DIFF(fl.as_of, c.date, DAY) AS days_old,
    c.*,
    IF(c.label = 'calm', 'open', 'hold') AS state,
    fd.f5 AS fund_r5_pct,
    fd.f20 AS fund_r20_pct
  FROM filled AS fl
  JOIN c ON c.market_id = fl.market_id AND c.date = fl.session
  LEFT JOIN fund_filled AS ff
    ON ff.symbol = c.fund_symbol AND ff.as_of = fl.as_of AND DATE_DIFF(fl.as_of, ff.fdate, DAY) <= 6
  LEFT JOIN fund AS fd ON fd.symbol = ff.symbol AND fd.date = ff.fdate
  WHERE DATE_DIFF(fl.as_of, fl.session, DAY) <= 6
),
ranked AS (
  SELECT
    r.*,
    ROW_NUMBER() OVER (PARTITION BY r.as_of
                       ORDER BY IF(r.state = 'open', 0, 1), r.trend_score DESC NULLS LAST, r.market_order) AS rank,
    IF(r.buyable,
       ROW_NUMBER() OVER (PARTITION BY r.as_of, r.buyable
                          ORDER BY IF(r.state = 'open', 0, 1), r.trend_score DESC NULLS LAST, r.market_order),
       NULL) AS buyable_rank,
    COUNTIF(r.state = 'open' AND r.buyable) OVER (PARTITION BY r.as_of) AS open_markets,
    COUNTIF(r.buyable) OVER (PARTITION BY r.as_of) AS buyable_markets,
    COUNT(*) OVER (PARTITION BY r.as_of) AS markets_read
  FROM r
)
SELECT
  ranked.*,
  IF(ranked.open_markets = 0, 'ALL ON HOLD - cash stays cash',
     FORMAT('%d of %d open', ranked.open_markets, ranked.buyable_markets)) AS verdict
FROM ranked;
