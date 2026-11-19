-- v_missed_winners: per day and horizon, the 20 liquid equities that rose
-- most, with what the project said about each that day and why it was not a
-- pick - so each missed opportunity is named with its cause. WHY (owner,
-- 19 Nov 2026 10:04 Riyadh: "what we are missed to be consider in the future
-- as well"). Base: v_forward_universe.
-- missed_cause, in this order:
--   'picked (investable)'          investability_status INVESTABLE;
--   'blocked: <block_reason>'      BLOCKED, with its reason when one is given;
--   'rated <reco>'                 a SELL / STRONG_SELL / REDUCE / AVOID call;
--   'rated buy, not investable'     a buy call left on the watchlist;
--   'watchlist <reco>'             anything else (HOLD and unrated).
-- COLUMNS: base_date, horizon, pos (1 = the day's best), symbol, ret_pp,
-- market_median_pp, sector, vol_band, volatility_30d_pct, status, reco,
-- block_reason, missed_cause.
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_missed_winners`
OPTIONS (description = "TFB Looker: per day and horizon, the 20 liquid equities that rose most, with that day's status, recommendation and the cause they were not picked")
AS
WITH r AS (
  SELECT f.*,
    ROW_NUMBER() OVER (PARTITION BY base_date, horizon ORDER BY ret_pp DESC, symbol) AS pos
  FROM `{{DS}}.v_forward_universe` f
)
SELECT
  base_date, horizon, pos, symbol, ROUND(ret_pp, 2) AS ret_pp, ROUND(market_median_pp, 2) AS market_median_pp,
  sector, vol_band, volatility_30d_pct, status, reco, block_reason,
  CASE
    WHEN status = 'INVESTABLE' THEN 'picked (investable)'
    WHEN status = 'BLOCKED' THEN CONCAT('blocked: ', IFNULL(block_reason, 'no reason given'))
    WHEN reco IN ('SELL', 'STRONG_SELL', 'REDUCE', 'AVOID') THEN CONCAT('rated ', reco)
    WHEN reco IN ('BUY', 'STRONG_BUY', 'ACCUMULATE') THEN 'rated buy, not investable'
    ELSE CONCAT('watchlist ', IF(reco = '', 'unrated', reco))
  END AS missed_cause
FROM r
WHERE pos <= 20;
