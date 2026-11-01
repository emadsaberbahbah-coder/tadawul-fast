-- v_fx_sar: one row per currency the market pages quote, with its rate to
-- the Saudi riyal, read from the latest Commodities_FX page (v_universe_latest,
-- asset class FX). USD comes from SAR=X (riyals per dollar). Every other
-- currency goes through its dollar cross: CCYUSD=X quotes one unit in
-- dollars, CCY=X quotes dollars per unit (its inverse); the direct quote wins
-- when both exist. GBX and GBp, the London pence listings, are a hundredth
-- of a pound. fx_source says which quote made the rate. A currency without a
-- quote has no row, so a join leaves its SAR values NULL rather than wrong.
CREATE OR REPLACE VIEW `{{DS}}.v_fx_sar`
OPTIONS (description = "TFB Looker v4: currency -> SAR rate from the latest Commodities_FX page (USD via SAR=X, other currencies via their dollar cross, pence as a hundredth of a pound)")
AS
WITH fx AS (
  SELECT symbol, current_price AS price, snapshot_date
  FROM `{{DS}}.v_universe_latest`
  WHERE source_page = 'Commodities_FX' AND asset_class = 'FX' AND current_price > 0
),
usd_sar AS (
  SELECT MAX(price) AS price, MAX(snapshot_date) AS snapshot_date FROM fx WHERE symbol = 'SAR=X'
),
to_usd AS (
  SELECT REGEXP_EXTRACT(symbol, r'^([A-Z]{3})USD=X$') AS ccy, price AS usd_per_unit, 'direct' AS quote
  FROM fx
  WHERE REGEXP_CONTAINS(symbol, r'^[A-Z]{3}USD=X$')
  UNION ALL
  SELECT REGEXP_EXTRACT(symbol, r'^([A-Z]{3})=X$') AS ccy, 1 / price AS usd_per_unit, 'inverse' AS quote
  FROM fx
  WHERE REGEXP_CONTAINS(symbol, r'^[A-Z]{3}=X$') AND symbol != 'SAR=X'
),
best AS (
  SELECT ccy, usd_per_unit, quote
  FROM to_usd
  WHERE ccy NOT IN ('SAR', 'USD')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ccy ORDER BY IF(quote = 'direct', 0, 1)) = 1
),
rates AS (
  SELECT 'SAR' AS currency, 1.0 AS rate_to_sar, 'identity' AS fx_source
  UNION ALL
  SELECT 'USD', u.price, 'SAR=X' FROM usd_sar AS u WHERE u.price IS NOT NULL
  UNION ALL
  SELECT b.ccy, b.usd_per_unit * u.price,
         CONCAT(b.ccy, IF(b.quote = 'direct', 'USD=X x SAR=X', '=X inverse x SAR=X'))
  FROM best AS b CROSS JOIN usd_sar AS u
  WHERE u.price IS NOT NULL
)
SELECT r.currency, r.rate_to_sar, r.fx_source, u.snapshot_date
FROM rates AS r CROSS JOIN usd_sar AS u
UNION ALL
SELECT pence, r.rate_to_sar / 100, CONCAT('GBP/100 via ', r.fx_source), u.snapshot_date
FROM rates AS r CROSS JOIN usd_sar AS u CROSS JOIN UNNEST(['GBX', 'GBp']) AS pence
WHERE r.currency = 'GBP';
