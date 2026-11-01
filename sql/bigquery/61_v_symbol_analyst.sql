-- v_symbol_analyst: the analyst consensus per symbol (EODHD AnalystRatings,
-- symbol_analyst_daily, scripts/archive_symbol_deep.py) - the latest night
-- per symbol: the Strong Buy / Buy / Hold / Sell / Strong Sell counts and
-- shares, a consensus score recomputed from the counts (5 Strong Buy .. 1
-- Strong Sell, so its direction never depends on the provider's scale; the
-- provider's own rating when there are no counts), its label, the mean
-- target and the upside to it from the latest market-page price, and how
-- the score moved against the copy of about a month before.
CREATE OR REPLACE VIEW `{{DS}}.v_symbol_analyst`
OPTIONS (description = "TFB Looker v3: analyst consensus per symbol - counts, score, label, target upside and the month's change")
AS
WITH a AS (
  SELECT
    t.*,
    SAFE_DIVIDE(5 * IFNULL(t.strong_buy, 0) + 4 * IFNULL(t.buy, 0) + 3 * IFNULL(t.hold, 0)
                + 2 * IFNULL(t.sell, 0) + IFNULL(t.strong_sell, 0), NULLIF(t.analysts, 0)) AS counts_score
  FROM `{{DS}}.symbol_analyst_daily` AS t
),
latest AS (
  SELECT a.*
  FROM a
  WHERE a.symbol IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.symbol ORDER BY a.snapshot_date DESC) = 1
),
month_ago AS (
  SELECT a.symbol, COALESCE(a.counts_score, a.rating) AS score_before, a.snapshot_date AS date_before
  FROM a
  WHERE a.snapshot_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.symbol ORDER BY a.snapshot_date DESC) = 1
),
s AS (
  SELECT
    l.*,
    ROUND(COALESCE(l.counts_score, l.rating), 2) AS consensus_score
  FROM latest AS l
)
SELECT
  s.symbol,
  COALESCE(u.symbol_label, s.symbol) AS symbol_label,
  u.name,
  s.snapshot_date,
  s.strong_buy,
  s.buy,
  s.hold,
  s.sell,
  s.strong_sell,
  s.analysts,
  ROUND(SAFE_DIVIDE(IFNULL(s.strong_buy, 0) + IFNULL(s.buy, 0), NULLIF(s.analysts, 0)) * 100, 1) AS buy_share_pct,
  ROUND(SAFE_DIVIDE(IFNULL(s.hold, 0), NULLIF(s.analysts, 0)) * 100, 1) AS hold_share_pct,
  ROUND(SAFE_DIVIDE(IFNULL(s.sell, 0) + IFNULL(s.strong_sell, 0), NULLIF(s.analysts, 0)) * 100, 1) AS sell_share_pct,
  s.consensus_score,
  CASE
    WHEN s.consensus_score >= 4.5 THEN 'Strong Buy'
    WHEN s.consensus_score >= 3.5 THEN 'Buy'
    WHEN s.consensus_score >= 2.5 THEN 'Hold'
    WHEN s.consensus_score >= 1.5 THEN 'Sell'
    WHEN s.consensus_score IS NOT NULL THEN 'Strong Sell'
  END AS consensus_label,
  s.rating AS provider_rating,
  s.target_price,
  u.current_price,
  u.currency,
  ROUND((SAFE_DIVIDE(s.target_price, u.current_price) - 1) * 100, 2) AS target_upside_pct,
  b.date_before,
  ROUND(s.consensus_score - b.score_before, 2) AS consensus_change_month
FROM s
LEFT JOIN `{{DS}}.v_universe_latest` AS u ON u.symbol = s.symbol
LEFT JOIN month_ago AS b ON b.symbol = s.symbol;
