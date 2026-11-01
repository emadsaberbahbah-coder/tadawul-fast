-- f_universe_return(start_date, end_date): the realized price return of
-- every market-page symbol over a date window, from the nightly archive.
-- Base = the last archived price before the window (the first one inside
-- it when the archive does not reach back that far - covers_window is then
-- FALSE); end = the last archived price on or before end_date. Local
-- currency, not adjusted for splits or dividends, percent points. Looker
-- custom query (date range parameters enabled):
--   SELECT * FROM `<project>.<dataset>.f_universe_return`(
--     PARSE_DATE('%Y%m%d', @DS_START_DATE), PARSE_DATE('%Y%m%d', @DS_END_DATE))
CREATE OR REPLACE TABLE FUNCTION `{{DS}}.f_universe_return`(start_date DATE, end_date DATE)
OPTIONS (description = "TFB Looker v2: realized return per market-page symbol over a date window, from archived nightly prices (percent points, local currency)")
AS
WITH px AS (
  SELECT
    symbol, snapshot_date, captured_at, source_page, current_price, name, asset_class, region,
    market_country, country, sector, currency, reliability, data_quality, expected_roi_12m_pct
  FROM `{{DS}}.v_universe`
  WHERE snapshot_date >= DATE_SUB(start_date, INTERVAL 10 DAY)
    AND snapshot_date <= end_date
    AND current_price > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, snapshot_date ORDER BY captured_at DESC, source_page) = 1
),
agg AS (
  SELECT
    symbol,
    ARRAY_AGG(IF(snapshot_date < start_date, STRUCT(snapshot_date AS d, current_price AS p), NULL)
              IGNORE NULLS ORDER BY snapshot_date DESC LIMIT 1)[SAFE_OFFSET(0)] AS before_start,
    ARRAY_AGG(IF(snapshot_date >= start_date, STRUCT(snapshot_date AS d, current_price AS p), NULL)
              IGNORE NULLS ORDER BY snapshot_date LIMIT 1)[SAFE_OFFSET(0)] AS first_in,
    ARRAY_AGG(IF(snapshot_date >= start_date, px, NULL)
              IGNORE NULLS ORDER BY snapshot_date DESC LIMIT 1)[SAFE_OFFSET(0)] AS last_in,
    COUNTIF(snapshot_date >= start_date) AS snapshots_in_window
  FROM px
  GROUP BY symbol
)
SELECT
  symbol,
  last_in.name AS name,
  last_in.asset_class AS asset_class,
  last_in.region AS region,
  last_in.market_country AS market_country,
  last_in.country AS country,
  last_in.sector AS sector,
  last_in.currency AS currency,
  COALESCE(before_start.d, first_in.d) AS base_date,
  COALESCE(before_start.p, first_in.p) AS base_price,
  last_in.snapshot_date AS last_date,
  last_in.current_price AS last_price,
  ROUND((SAFE_DIVIDE(last_in.current_price, COALESCE(before_start.p, first_in.p)) - 1) * 100, 4) AS period_return_pct,
  before_start IS NOT NULL AS covers_window,
  snapshots_in_window,
  last_in.reliability AS reliability,
  last_in.data_quality AS data_quality,
  last_in.expected_roi_12m_pct AS expected_roi_12m_pct
FROM agg
WHERE last_in IS NOT NULL;
