-- v_news_overlay_list: the investable list of each day beside the same list
-- as the news would change it - a shadow list, measured by
-- v_trial_news_overlay and shown to nobody as an instruction. WHY (owner,
-- 19 Nov 2026 12:31 Riyadh: see whether the news changes the
-- recommendations - names that leave, names that come in - and by how much
-- the success rate improves).
-- UNIVERSE: as v_forward_universe - market-page equities (Global_Markets,
-- Market_Leaders, asset_class EQUITY) with a price and a 30-day traded value
-- of at least 1 million in local currency, one record per symbol and day,
-- the six provider sector spellings folded to GICS; vol_band = the day's
-- terciles of volatility_30d_pct.
-- RULE (fixed 19 Nov 2026, per arm of v_news_sector_tilt; the tilt that
-- applies to a day is the one whose apply_from .. apply_to holds it):
--   out   an INVESTABLE name of an 'avoided' sector leaves;
--   in    for each name that left, one comes in: WATCHLIST names rated HOLD,
--         of a 'favoured' sector, in the LOW or MID volatility band, taken by
--         overall_score, highest first (ties by symbol);
--   keep  every other INVESTABLE name stays.
-- A day with no tilt, or with no investable name in an avoided sector,
-- leaves the list as it is.
-- COLUMNS: arm, base_date, symbol, name, sector, status, reco,
-- overall_score, vol_band, tilt_group, news_week, sector_score, sector_pos,
-- active_list, in_champion (on the day's own list), in_overlay (on the news
-- version), news_action (keep / out / in).
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_news_overlay_list`
OPTIONS (description = "TFB news loop: each day's investable list beside its news version (shadow) - names kept, moved out of avoided sectors, and brought in from favoured sectors, per arm (events / all)")
AS
WITH u AS (
  SELECT
    symbol, snapshot_date AS base_date,
    CAST(name AS STRING) AS name,
    UPPER(IFNULL(CAST(investability_status AS STRING), '')) AS status,
    UPPER(REPLACE(IFNULL(CAST(recommendation AS STRING), ''), ' ', '_')) AS reco,
    CASE IFNULL(NULLIF(CAST(sector AS STRING), ''), '(no sector)')
      WHEN 'Basic Materials' THEN 'Materials'
      WHEN 'Healthcare' THEN 'Health Care'
      WHEN 'Consumer Cyclical' THEN 'Consumer Discretionary'
      WHEN 'Consumer Defensive' THEN 'Consumer Staples'
      WHEN 'Technology' THEN 'Information Technology'
      WHEN 'Financial Services' THEN 'Financials'
      ELSE IFNULL(NULLIF(CAST(sector AS STRING), ''), '(no sector)')
    END AS sector,
    volatility_30d_pct, overall_score
  FROM `{{DS}}.v_universe`
  WHERE source_page IN ('Global_Markets', 'Market_Leaders')
    AND UPPER(CAST(asset_class AS STRING)) = 'EQUITY'
    AND current_price > 0
    AND avg_volume_30d * current_price >= 1000000
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, snapshot_date ORDER BY captured_at DESC, source_page) = 1
),
vb AS (
  SELECT symbol, base_date,
    NTILE(3) OVER (PARTITION BY base_date ORDER BY volatility_30d_pct, symbol) AS vol_t
  FROM u
  WHERE volatility_30d_pct IS NOT NULL
),
j AS (
  SELECT
    news_arm AS arm, u.*,
    CASE vb.vol_t WHEN 1 THEN 'LOW' WHEN 2 THEN 'MID' WHEN 3 THEN 'HIGH' END AS vol_band,
    IFNULL(t.tilt_group, 'neutral') AS tilt_group,
    t.week_start AS news_week, t.score AS sector_score, t.pos AS sector_pos, t.active_list
  FROM u
  CROSS JOIN UNNEST(['events', 'all']) AS news_arm
  LEFT JOIN vb USING (symbol, base_date)
  LEFT JOIN `{{DS}}.v_news_sector_tilt` AS t
    ON t.arm = news_arm AND t.sector = u.sector AND u.base_date BETWEEN t.apply_from AND t.apply_to
),
fl AS (
  SELECT
    j.*,
    status = 'INVESTABLE' AS in_champion,
    status = 'INVESTABLE' AND tilt_group = 'avoided' AS moved_out,
    status = 'WATCHLIST' AND reco = 'HOLD' AND tilt_group = 'favoured'
      AND IFNULL(vol_band, '') IN ('LOW', 'MID') AND overall_score IS NOT NULL AS in_pool
  FROM j
),
rk AS (
  SELECT
    fl.*,
    COUNTIF(moved_out) OVER (PARTITION BY arm, base_date) AS n_out,
    IF(in_pool, ROW_NUMBER() OVER (PARTITION BY arm, base_date, in_pool ORDER BY overall_score DESC, symbol), NULL) AS pool_pos
  FROM fl
  WHERE in_champion OR in_pool
)
SELECT
  arm, base_date, symbol, name, sector, status, reco,
  ROUND(overall_score, 1) AS overall_score,
  vol_band, tilt_group, news_week, sector_score, sector_pos, active_list,
  in_champion,
  (in_champion AND NOT moved_out) OR (in_pool AND pool_pos <= n_out) AS in_overlay,
  CASE WHEN moved_out THEN 'out' WHEN in_champion THEN 'keep' ELSE 'in' END AS news_action
FROM rk
WHERE in_champion OR (in_pool AND pool_pos <= n_out);
