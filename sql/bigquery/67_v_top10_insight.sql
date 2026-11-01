-- v_top10_insight: the latest archived Top-10 board (the snapshot of the
-- newest v_top10_status row, so an empty board shows as empty, not as the
-- last full one) with the evidence a decision needs beside each row: the engine's newest market-page view of
-- the name (mkt_*: price, forecast, risk, signals, data age), its next
-- earnings and ex-dividend dates, its news of the last 7 days and how its
-- earlier board episodes ended, and the TFB rating with the analyst
-- consensus (v_symbol_rating, apply_bq_views 1.3.0). caution_flags lists, in plain words, what
-- argues for a second look before acting (earnings or ex-dividend within 5
-- days, market data older than 36 h when archived, a market copy more than
-- a day old (a missed nightly archive), negative or high-severity news, below
-- the reliability / data-quality floors, a block reason, a negative 12-month
-- forecast, an analyst consensus below Hold, no market-page row);
-- caution_count counts them. Percent points. symbol_label keys the
-- symbol picker shared with the candle, news and analyst views.
CREATE OR REPLACE VIEW `{{DS}}.v_top10_insight`
OPTIONS (description = "TFB Looker v2: the latest Top-10 board with market-page evidence, events, 7-day news, past episodes and plain-word caution flags per row")
AS
WITH board AS (
  SELECT b.*
  FROM `{{DS}}.v_top10_board` AS b
  WHERE b.symbol IS NOT NULL
    AND b.snapshot_date = (SELECT MAX(snapshot_date) FROM `{{DS}}.v_top10_status`)
),
cal AS (
  SELECT symbol, next_earnings_date, days_to_earnings_now, next_exdiv_date, days_to_exdiv_now,
         last_earnings_date, days_since_earnings_now, eps_surprise_pct
  FROM `{{DS}}.v_calendar_events`
  WHERE is_latest
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY updated_at DESC) = 1
),
news AS (
  SELECT
    symbol,
    COUNT(*) AS news_7d,
    COUNTIF(direction < 0) AS news_negative_7d,
    COUNTIF(direction > 0) AS news_positive_7d,
    COUNTIF(severity = 'H') AS news_high_severity_7d,
    MAX(COALESCE(published_at, first_seen_at)) AS last_news_at,
    ARRAY_AGG(headline IGNORE NULLS ORDER BY COALESCE(published_at, first_seen_at) DESC LIMIT 1)[SAFE_OFFSET(0)] AS last_headline
  FROM `{{DS}}.v_news_items`
  WHERE symbol IS NOT NULL
    AND COALESCE(published_at, first_seen_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  GROUP BY symbol
),
past AS (
  SELECT
    symbol,
    COUNT(*) AS past_episodes,
    COUNTIF(win_plus5 IS NOT NULL) AS past_scored_plus5,
    ROUND(SAFE_DIVIDE(COUNTIF(win_plus5 = 1), COUNTIF(win_plus5 IS NOT NULL)) * 100, 1) AS past_win_plus5_pct,
    ROUND(AVG(ret_to_exit_pct), 2) AS past_avg_ret_to_exit_pct,
    MAX(exit_date) AS past_last_exit_date
  FROM `{{DS}}.v_board_episodes`
  GROUP BY symbol
),
j AS (
  SELECT
    b.*,
    COALESCE(u.symbol_label, b.symbol) AS symbol_label,
    u.symbol IS NOT NULL AS on_market_page,
    u.source_page AS mkt_page,
    u.region AS mkt_region,
    u.market_country AS mkt_country,
    u.current_price AS mkt_price,
    u.change_pct AS mkt_change_pct,
    u.position_52w_pct AS mkt_position_52w_pct,
    u.expected_roi_1m_pct AS mkt_expected_roi_1m_pct,
    u.expected_roi_3m_pct AS mkt_expected_roi_3m_pct,
    u.expected_roi_12m_pct AS mkt_expected_roi_12m_pct,
    u.forecast_confidence_pts AS mkt_forecast_confidence_pts,
    u.reliability AS mkt_reliability,
    u.data_quality AS mkt_data_quality,
    u.meets_floors AS mkt_meets_floors,
    u.overall_score AS mkt_overall_score,
    u.risk_score AS mkt_risk_score,
    u.risk_bucket AS mkt_risk_bucket,
    u.volatility_30d_pct AS mkt_volatility_30d_pct,
    u.max_drawdown_1y_pct AS mkt_max_drawdown_1y_pct,
    u.rsi_14 AS mkt_rsi_14,
    u.signal AS mkt_signal,
    u.st_signal AS mkt_st_signal,
    u.candle_signal AS mkt_candle_signal,
    u.analyst_rating AS mkt_analyst_rating,
    u.target_upside_pct AS mkt_target_upside_pct,
    u.recommendation AS mkt_recommendation,
    u.final_action AS mkt_final_action,
    u.investability_status AS mkt_investability_status,
    u.block_reason AS mkt_block_reason,
    u.top_factors AS mkt_top_factors,
    u.top_risks AS mkt_top_risks,
    u.data_age_hours AS mkt_data_age_hours,
    u.snapshot_age_days AS mkt_snapshot_age_days,
    c.next_earnings_date,
    c.days_to_earnings_now,
    c.next_exdiv_date,
    c.days_to_exdiv_now,
    c.last_earnings_date,
    c.days_since_earnings_now,
    c.eps_surprise_pct,
    COALESCE(n.news_7d, 0) AS news_7d,
    COALESCE(n.news_negative_7d, 0) AS news_negative_7d,
    COALESCE(n.news_positive_7d, 0) AS news_positive_7d,
    COALESCE(n.news_high_severity_7d, 0) AS news_high_severity_7d,
    n.last_news_at,
    n.last_headline,
    COALESCE(p.past_episodes, 0) AS past_episodes,
    p.past_scored_plus5,
    p.past_win_plus5_pct,
    p.past_avg_ret_to_exit_pct,
    p.past_last_exit_date,
    rt.tfb_rating_score,
    rt.tfb_rating_label,
    rt.tfb_rating_stars,
    rt.rating_confidence,
    rt.rating_parts,
    rt.analyst_consensus_label,
    rt.analyst_consensus_score,
    rt.analysts,
    ARRAY(
      SELECT f FROM UNNEST([
        IF(c.days_to_earnings_now BETWEEN 0 AND 5, 'earnings within 5 days', NULL),
        IF(c.days_to_exdiv_now BETWEEN 0 AND 5, 'ex-dividend within 5 days', NULL),
        IF(u.data_age_hours > 36, 'market data older than 36 h', NULL),
        IF(u.snapshot_age_days > 1, CONCAT('market copy ', CAST(u.snapshot_age_days AS STRING), ' days old'), NULL),
        IF(n.news_negative_7d > 0, 'negative news in 7 days', NULL),
        IF(n.news_high_severity_7d > 0, 'high-severity news in 7 days', NULL),
        IF(u.symbol IS NOT NULL AND NOT u.meets_floors, 'below reliability / data-quality floors', NULL),
        IF(u.block_reason IS NOT NULL, CONCAT('blocked: ', u.block_reason), NULL),
        IF(u.expected_roi_12m_pct < 0, 'negative 12-month forecast', NULL),
        IF(rt.analyst_consensus_score < 2.5, 'analyst consensus below Hold', NULL),
        IF(u.symbol IS NULL, 'no market-page row', NULL)
      ]) AS f
      WHERE f IS NOT NULL
    ) AS flags
  FROM board AS b
  LEFT JOIN `{{DS}}.v_universe_latest` AS u ON u.symbol = b.symbol
  LEFT JOIN cal AS c ON c.symbol = b.symbol
  LEFT JOIN news AS n ON n.symbol = b.symbol
  LEFT JOIN past AS p ON p.symbol = b.symbol
  LEFT JOIN `{{DS}}.v_symbol_rating` AS rt ON rt.symbol = b.symbol
)
SELECT
  j.* EXCEPT (flags),
  ARRAY_TO_STRING(j.flags, ' · ') AS caution_flags,
  ARRAY_LENGTH(j.flags) AS caution_count
FROM j;
