-- f_market_window(start_date, end_date): the reference market series inside
-- a date window - what the global markets page draws for the Looker date
-- range. Looker custom query (date range parameters enabled):
--   SELECT * FROM `<project>.<dataset>.f_market_window`(
--     PARSE_DATE('%Y%m%d', @DS_START_DATE), PARSE_DATE('%Y%m%d', @DS_END_DATE))
-- Each series starts from base_price, its last return price BEFORE
-- start_date, so a one-day window is that day's move and a month is the
-- month's move from the previous month's last close; a series with no earlier
-- day starts from its first day in the window (base_date NULL).
-- cum_return_pct: return from the start to each day's close; index_100: the
-- same as a level that starts at 100; drawdown_pct: fall from the highest
-- level so far in the window (start included). return_pct / log_return_pct
-- are the daily moves of v_market_daily (the window's log returns add up to
-- its return exactly). Percent points throughout.
CREATE OR REPLACE TABLE FUNCTION `{{DS}}.f_market_window`(start_date DATE, end_date DATE)
OPTIONS (description = "TFB Looker v1: market series inside a date window - cumulative return and drawdown from the window start (percent points)")
AS
WITH d AS (
  SELECT *
  FROM `{{DS}}.v_market_daily`
  WHERE date <= end_date
),
base AS (
  SELECT
    series_id,
    ARRAY_AGG(STRUCT(date AS base_date, return_price AS base_price) ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS b
  FROM d
  WHERE date < start_date
  GROUP BY series_id
),
w AS (
  SELECT
    d.*,
    base.b.base_date,
    COALESCE(base.b.base_price,
             FIRST_VALUE(d.return_price) OVER (PARTITION BY d.series_id ORDER BY d.date)) AS start_price
  FROM d
  LEFT JOIN base
    ON base.series_id = d.series_id
  WHERE d.date >= start_date
),
x AS (
  SELECT
    w.*,
    GREATEST(start_price,
             MAX(return_price) OVER (PARTITION BY series_id ORDER BY date
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS peak_price,
    ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY date DESC) = 1 AS is_last_day,
    COUNT(*) OVER (PARTITION BY series_id) AS days_in_window
  FROM w
)
SELECT
  series_id, symbol, name, region, country, kind, currency, sector,
  date, week, month, year,
  close, return_price, base_date, start_price,
  return_pct, log_return_pct, up_day, down_day,
  ROUND((SAFE_DIVIDE(return_price, start_price) - 1) * 100, 6) AS cum_return_pct,
  ROUND(SAFE_DIVIDE(return_price, start_price) * 100, 6) AS index_100,
  ROUND((SAFE_DIVIDE(return_price, peak_price) - 1) * 100, 6) AS drawdown_pct,
  is_last_day, days_in_window
FROM x;
