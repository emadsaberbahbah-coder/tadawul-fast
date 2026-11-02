-- v_market_daily: one row per reference market series and trading day, from
-- market_series_daily (scripts/archive_market_series.py: Yahoo daily bars,
-- refreshed nightly, one partition per series). Returns use return_price:
-- the dividend-adjusted close when the series has one on every row (ETFs:
-- total return), else the close (indices: price return). return_pct and
-- log_return_pct compare with the previous trading day of the same series
-- (NULL on its first row); log returns add up exactly over consecutive days,
-- so a period's return % = (EXP(SUM(log_return_pct) / 100) - 1) x 100 for any
-- grouping (week, month, custom). ytd_return_pct compares with the last
-- trading day of the previous calendar year (NULL in the first year held).
-- Weeks start on Monday (most venues; a Tadawul Sunday belongs to the week
-- before). Prices in local currency for indices, USD for ETFs and futures.
CREATE OR REPLACE VIEW `{{DS}}.v_market_daily`
OPTIONS (description = "TFB Looker v1: daily bars and returns of the reference market series (percent points)")
AS
WITH s AS (
  SELECT
    series_id, symbol, name, region, country, kind, currency, sector, date,
    open, high, low, close, adj_close, volume, captured_at,
    LOGICAL_AND(adj_close IS NOT NULL) OVER (PARTITION BY series_id) AS dividend_adjusted
  FROM `{{DS}}.market_series_daily`
),
p AS (
  SELECT s.*, IF(dividend_adjusted, adj_close, close) AS return_price, EXTRACT(YEAR FROM date) AS year
  FROM s
),
ye AS (
  SELECT
    series_id,
    year + 1 AS next_year,
    ARRAY_AGG(return_price ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS year_base_price
  FROM p
  GROUP BY series_id, year
),
r AS (
  SELECT
    p.*,
    LAG(return_price) OVER (PARTITION BY series_id ORDER BY date) AS prev_price,
    LAG(date) OVER (PARTITION BY series_id ORDER BY date) AS prev_date
  FROM p
)
SELECT
  r.series_id, r.symbol, r.name, r.region, r.country, r.kind, r.currency, r.sector,
  r.date, r.open, r.high, r.low, r.close, r.adj_close, r.volume,
  r.dividend_adjusted, r.return_price, r.prev_date, r.prev_price,
  ROUND((SAFE_DIVIDE(r.return_price, r.prev_price) - 1) * 100, 6) AS return_pct,
  ROUND(SAFE.LN(SAFE_DIVIDE(r.return_price, r.prev_price)) * 100, 8) AS log_return_pct,
  CASE WHEN r.prev_price IS NULL THEN NULL WHEN r.return_price > r.prev_price THEN 1 ELSE 0 END AS up_day,
  CASE WHEN r.prev_price IS NULL THEN NULL WHEN r.return_price < r.prev_price THEN 1 ELSE 0 END AS down_day,
  ROUND((SAFE_DIVIDE(r.return_price, ye.year_base_price) - 1) * 100, 6) AS ytd_return_pct,
  DATE_TRUNC(r.date, WEEK(MONDAY)) AS week,
  DATE_TRUNC(r.date, MONTH) AS month,
  r.year,
  r.captured_at
FROM r
LEFT JOIN ye
  ON ye.series_id = r.series_id AND ye.next_year = r.year;
