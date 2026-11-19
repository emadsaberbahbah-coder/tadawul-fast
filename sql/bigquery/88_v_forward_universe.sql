-- v_forward_universe: every liquid market-page equity, every archived day,
-- with what its price did over the next week and two weeks, and what the
-- project said about it that day. The base of the standing "investable list
-- against the best of the market" comparison (v_investable_vs_market,
-- v_missed_winners). WHY (owner, 19 Nov 2026 10:04 Riyadh, verbatim: "make
-- better compsion between the ROI as the investable recommdadtion and the best
-- avalaible in the market as actual result and what we are missed to be
-- consider in the future as well").
-- UNIVERSE: v_universe rows from the Global_Markets and Market_Leaders pages,
-- asset_class EQUITY, a price, and a 30-day traded value (avg_volume_30d x
-- price) of at least 1 million in local currency; one record per symbol and
-- day (the last capture of the day).
-- FORWARD RETURN: from the base day's price to the first archived price of the
-- symbol from base + 7 days (1W) or base + 14 days (2W), no later than 6 days
-- after that; price only, local currency, percent points. A base day whose
-- horizon has not arrived has no row for it. Moves of 300 points or more are
-- left out as data errors.
-- SECTOR: the provider's six spellings folded to GICS as v_symbol_sector and
-- core/sectors.py do (Financial Services -> Financials, and so on).
-- COLUMNS: symbol, base_date, horizon, ret_pp, status (investability_status),
-- reco (recommendation, spaces as _), block_reason, sector, volatility_30d_pct,
-- vol_band (LOW / MID / HIGH: the day's terciles of volatility_30d_pct; NULL
-- when unknown), ret_pct_rank (0..1 within the day and horizon), ret_pos (1 =
-- the day's best; ties broken by symbol), n_day (names that day and horizon),
-- market_median_pp (the day's median ret_pp for the horizon, PERCENTILE_CONT).
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_forward_universe`
OPTIONS (description = "TFB Looker: every liquid market-page equity and day with its forward 1W / 2W price return and that day's status, recommendation, sector and volatility band")
AS
WITH px AS (
  SELECT
    symbol, snapshot_date, current_price,
    UPPER(IFNULL(CAST(investability_status AS STRING), '')) AS status,
    UPPER(REPLACE(IFNULL(CAST(recommendation AS STRING), ''), ' ', '_')) AS reco,
    NULLIF(CAST(block_reason AS STRING), '') AS block_reason,
    CASE IFNULL(NULLIF(CAST(sector AS STRING), ''), '(no sector)')
      WHEN 'Basic Materials' THEN 'Materials'
      WHEN 'Healthcare' THEN 'Health Care'
      WHEN 'Consumer Cyclical' THEN 'Consumer Discretionary'
      WHEN 'Consumer Defensive' THEN 'Consumer Staples'
      WHEN 'Technology' THEN 'Information Technology'
      WHEN 'Financial Services' THEN 'Financials'
      ELSE IFNULL(NULLIF(CAST(sector AS STRING), ''), '(no sector)')
    END AS sector,
    volatility_30d_pct
  FROM `{{DS}}.v_universe`
  WHERE source_page IN ('Global_Markets', 'Market_Leaders')
    AND UPPER(CAST(asset_class AS STRING)) = 'EQUITY'
    AND current_price > 0
    AND avg_volume_30d * current_price >= 1000000
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, snapshot_date ORDER BY captured_at DESC, source_page) = 1
),
prices AS (
  SELECT symbol, snapshot_date, current_price
  FROM `{{DS}}.v_universe`
  WHERE current_price > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, snapshot_date ORDER BY captured_at DESC, source_page) = 1
),
hz AS (
  SELECT * FROM UNNEST(ARRAY<STRUCT<horizon STRING, days INT64>>[('1W', 7), ('2W', 14)])
),
fwd AS (
  SELECT
    p.symbol, p.snapshot_date AS base_date, h.horizon, p.status, p.reco, p.block_reason, p.sector,
    p.volatility_30d_pct,
    (ARRAY_AGG(q.current_price ORDER BY q.snapshot_date LIMIT 1)[OFFSET(0)] / p.current_price - 1) * 100 AS ret_pp
  FROM px p
  CROSS JOIN hz h
  JOIN prices q
    ON q.symbol = p.symbol
   AND q.snapshot_date >= DATE_ADD(p.snapshot_date, INTERVAL h.days DAY)
   AND q.snapshot_date <= DATE_ADD(p.snapshot_date, INTERVAL h.days + 6 DAY)
  GROUP BY p.symbol, p.snapshot_date, h.horizon, p.status, p.reco, p.block_reason, p.sector,
           p.volatility_30d_pct, p.current_price
),
ok AS (
  SELECT * FROM fwd WHERE ret_pp IS NOT NULL AND ABS(ret_pp) < 300
),
vb AS (
  SELECT symbol, base_date, horizon,
    NTILE(3) OVER (PARTITION BY base_date, horizon ORDER BY volatility_30d_pct, symbol) AS vol_t
  FROM ok
  WHERE volatility_30d_pct IS NOT NULL
)
SELECT
  o.symbol, o.base_date, o.horizon, o.ret_pp, o.status, o.reco, o.block_reason, o.sector, o.volatility_30d_pct,
  CASE vb.vol_t WHEN 1 THEN 'LOW' WHEN 2 THEN 'MID' WHEN 3 THEN 'HIGH' END AS vol_band,
  PERCENT_RANK() OVER (PARTITION BY o.base_date, o.horizon ORDER BY o.ret_pp) AS ret_pct_rank,
  ROW_NUMBER() OVER (PARTITION BY o.base_date, o.horizon ORDER BY o.ret_pp DESC, o.symbol) AS ret_pos,
  COUNT(*) OVER (PARTITION BY o.base_date, o.horizon) AS n_day,
  PERCENTILE_CONT(o.ret_pp, 0.5) OVER (PARTITION BY o.base_date, o.horizon) AS market_median_pp
FROM ok o
LEFT JOIN vb USING (symbol, base_date, horizon);
