-- v_symbol_sector: one GICS sector per symbol, so any outcome can be read by
-- sector. WHY (owner, 19 Nov 2026 01:18 Riyadh: "make full review ... how to
-- improve our forecasting and recommendation ... check data for better
-- alignment"): the review of that night found that Performance_Log records
-- carry no sector (its Sector column is empty), so no outcome view could say
-- which sectors the engine picks well, while 71% of the US names the market
-- pages rated INVESTABLE since 31 Oct were financial-services stocks. The
-- sector names also differ by page: the market pages write the data
-- provider's names ("Financial Services", "Healthcare", "Technology", "Basic
-- Materials", "Consumer Cyclical", "Consumer Defensive"), the Top 10 pages the
-- GICS names ("Financials", "Health Care", "Information Technology", ...).
-- RULE: a symbol's sector is the latest non-empty sector the market pages
-- gave it (v_universe); a symbol the pages never listed takes the latest
-- sector the Top 10 board gave it (v_top10_board). The six provider
-- spellings are folded to GICS as in core/sectors.py YAHOO_TO_GICS_SECTOR;
-- every other name is kept as written.
-- COLUMNS: symbol, sector (GICS), sector_source ('market pages' or 'top 10
-- board'), last_seen (the snapshot that gave it).
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_symbol_sector`
OPTIONS (description = "TFB one GICS sector per symbol - the latest non-empty sector of the market pages, else of the Top 10 board, with the six provider spellings folded to GICS")
AS
WITH pages AS (
  SELECT symbol, TRIM(sector) AS sector, snapshot_date AS last_seen, 1 AS pri
  FROM `{{DS}}.v_universe`
  WHERE symbol IS NOT NULL AND TRIM(IFNULL(sector, '')) != ''
),
board AS (
  SELECT symbol, TRIM(sector) AS sector, snapshot_date AS last_seen, 2 AS pri
  FROM `{{DS}}.v_top10_board`
  WHERE symbol IS NOT NULL AND TRIM(IFNULL(sector, '')) != ''
),
one AS (
  SELECT UPPER(TRIM(symbol)) AS symbol, sector, last_seen, pri FROM pages
  UNION ALL
  SELECT UPPER(TRIM(symbol)) AS symbol, sector, last_seen, pri FROM board
)
SELECT
  symbol,
  CASE sector
    WHEN 'Basic Materials' THEN 'Materials'
    WHEN 'Healthcare' THEN 'Health Care'
    WHEN 'Consumer Cyclical' THEN 'Consumer Discretionary'
    WHEN 'Consumer Defensive' THEN 'Consumer Staples'
    WHEN 'Technology' THEN 'Information Technology'
    WHEN 'Financial Services' THEN 'Financials'
    ELSE sector
  END AS sector,
  IF(pri = 1, 'market pages', 'top 10 board') AS sector_source,
  last_seen
FROM one
QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY pri, last_seen DESC, sector) = 1;
