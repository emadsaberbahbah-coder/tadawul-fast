-- f_market_summary(start_date, end_date): one row per reference market series
-- for a date window (the same window rules as f_market_window) - the table,
-- the bars and the region averages of the global markets page. Looker custom
-- query (date range parameters enabled):
--   SELECT * FROM `<project>.<dataset>.f_market_summary`(
--     PARSE_DATE('%Y%m%d', @DS_START_DATE), PARSE_DATE('%Y%m%d', @DS_END_DATE))
-- period_return_pct: from the window start to the last close in it;
-- up_day_share_pct: share of the window's days that closed higher;
-- volatility_ann_pct: standard deviation of the daily log returns x SQRT(252);
-- max_drawdown_pct: the deepest fall from a high inside the window. Averages
-- over series (a region, a kind) are plain averages of these rows.
-- Percent points throughout.
CREATE OR REPLACE TABLE FUNCTION `{{DS}}.f_market_summary`(start_date DATE, end_date DATE)
OPTIONS (description = "TFB Looker v1: one row per market series for a date window - period return, up days, volatility, drawdown (percent points)")
AS
SELECT
  series_id,
  ANY_VALUE(symbol) AS symbol,
  ANY_VALUE(name) AS name,
  ANY_VALUE(region) AS region,
  ANY_VALUE(country) AS country,
  ANY_VALUE(kind) AS kind,
  ANY_VALUE(currency) AS currency,
  ANY_VALUE(sector) AS sector,
  MIN(date) AS first_date,
  MAX(date) AS last_date,
  ANY_VALUE(base_date) AS base_date,
  ANY_VALUE(start_price) AS start_price,
  ARRAY_AGG(close ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS last_close,
  ROUND((SAFE_DIVIDE(ARRAY_AGG(return_price ORDER BY date DESC LIMIT 1)[OFFSET(0)],
                     ANY_VALUE(start_price)) - 1) * 100, 4) AS period_return_pct,
  COUNT(*) AS trading_days,
  COUNTIF(up_day = 1) AS up_days,
  COUNTIF(down_day = 1) AS down_days,
  ROUND(SAFE_DIVIDE(COUNTIF(up_day = 1), COUNTIF(up_day IS NOT NULL)) * 100, 2) AS up_day_share_pct,
  ROUND(AVG(return_pct), 4) AS avg_daily_return_pct,
  ROUND(MAX(return_pct), 4) AS best_day_pct,
  ROUND(MIN(return_pct), 4) AS worst_day_pct,
  ROUND(STDDEV_SAMP(log_return_pct) * SQRT(252), 4) AS volatility_ann_pct,
  ROUND(MIN(drawdown_pct), 4) AS max_drawdown_pct
FROM `{{DS}}.f_market_window`(start_date, end_date)
GROUP BY series_id;
