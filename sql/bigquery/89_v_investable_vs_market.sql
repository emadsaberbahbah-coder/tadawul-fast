-- v_investable_vs_market: per day and horizon, what the investable list and
-- the buy calls went on to return beside the market's median and the best
-- tenth of the market - the standing lost-opportunity scoreboard. WHY (owner,
-- 19 Nov 2026 10:04 Riyadh: compare the ROI of the investable recommendations
-- with the best available in the market as actual results, and what was
-- missed). Base: v_forward_universe.
-- GROUPS per base day and horizon:
--   MARKET / ALL         every liquid equity (its median is the bar);
--   INVESTABLE / ALL     investability_status INVESTABLE that day;
--   BUY_CALL / ALL       recommendation BUY, STRONG_BUY or ACCUMULATE that day;
--   BEST_DECILE / ALL    the day's best CEIL(n_day / 10) by ret_pos, so a tie at
--                        the cut cannot empty it (what was available);
--   SECTOR / <sector>    every liquid equity of the sector;
--   VOL / LOW|MID|HIGH   the day's volatility tercile;
--   INV_SECTOR / <s>     the investable names of a sector;
--   INV_VOL / <band>     the investable names of a volatility tercile (set
--                        beside VOL / <band>: selection inside the band).
-- MEDIANS: the group's median and the market's are both PERCENTILE_CONT 0.5,
-- so MARKET reads 0 against itself (Codex P2 on #1029).
-- COLUMNS: base_date, horizon, group_type, group_key, n, mean_pp, median_pp,
-- market_median_pp, vs_market_pp (median_pp - market_median_pp),
-- beat_market_pct (share of the group above the market median), best_symbol,
-- best_pp.
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_investable_vs_market`
OPTIONS (description = "TFB Looker: per day and horizon, the investable list and buy calls beside the market median, the best decile, sectors and volatility terciles - the standing lost-opportunity scoreboard")
AS
WITH tagged AS (
  SELECT f.*, g.group_type, g.group_key
  FROM `{{DS}}.v_forward_universe` f,
  UNNEST(ARRAY<STRUCT<group_type STRING, group_key STRING>>[
    ('MARKET', 'ALL'),
    IF(f.status = 'INVESTABLE', ('INVESTABLE', 'ALL'), NULL),
    IF(f.reco IN ('BUY', 'STRONG_BUY', 'ACCUMULATE'), ('BUY_CALL', 'ALL'), NULL),
    IF(f.ret_pos <= CAST(CEIL(f.n_day / 10) AS INT64), ('BEST_DECILE', 'ALL'), NULL),
    ('SECTOR', f.sector),
    IF(f.vol_band IS NULL, NULL, ('VOL', f.vol_band)),
    IF(f.status = 'INVESTABLE', ('INV_SECTOR', f.sector), NULL),
    IF(f.status = 'INVESTABLE' AND f.vol_band IS NOT NULL, ('INV_VOL', f.vol_band), NULL)
  ]) AS g
  WHERE g.group_type IS NOT NULL
),
med AS (
  SELECT t.*,
    PERCENTILE_CONT(ret_pp, 0.5) OVER (PARTITION BY base_date, horizon, group_type, group_key) AS group_median_pp
  FROM tagged t
)
SELECT
  base_date, horizon, group_type, group_key,
  COUNT(*) AS n,
  ROUND(AVG(ret_pp), 2) AS mean_pp,
  ROUND(ANY_VALUE(group_median_pp), 2) AS median_pp,
  ROUND(ANY_VALUE(market_median_pp), 2) AS market_median_pp,
  ROUND(ANY_VALUE(group_median_pp) - ANY_VALUE(market_median_pp), 2) AS vs_market_pp,
  ROUND(100 * COUNTIF(ret_pp > market_median_pp) / COUNT(*), 1) AS beat_market_pct,
  ARRAY_AGG(symbol ORDER BY ret_pp DESC, symbol LIMIT 1)[OFFSET(0)] AS best_symbol,
  ROUND(MAX(ret_pp), 2) AS best_pp
FROM med
GROUP BY base_date, horizon, group_type, group_key;
