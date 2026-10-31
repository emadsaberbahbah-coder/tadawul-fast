-- v_portfolio_insight: the latest archived My_Portfolio holdings, each with
-- the evidence a hold / trim / exit decision needs: the latest
-- Portfolio_Decision row for the name (pd_*: action, SAR P&L, weight, stop
-- and targets, advisor note), the engine's newest market-page view (mkt_*),
-- the next earnings and ex-dividend dates and the news of the last 7 days.
-- caution_flags lists, in plain words, what argues for a review (the price
-- at or below the stop, at or above the first target, earnings or
-- ex-dividend within 5 days, negative or high-severity news, a bearish
-- engine call, a negative 12-month forecast, data older than 36 h, below
-- the floors); caution_count counts them. Percent points.
CREATE OR REPLACE VIEW `{{DS}}.v_portfolio_insight`
OPTIONS (description = "TFB Looker v2: the latest holdings with the portfolio decision, market-page evidence, events, 7-day news and plain-word caution flags per holding")
AS
WITH hold AS (
  SELECT m.*
  FROM `{{DS}}.v_my_portfolio` AS m
  WHERE m.symbol IS NOT NULL
  QUALIFY m.snapshot_date = MAX(m.snapshot_date) OVER ()
),
pd AS (
  SELECT d.*
  FROM `{{DS}}.v_portfolio_decision` AS d
  WHERE d.symbol IS NOT NULL
  QUALIFY d.snapshot_date = MAX(d.snapshot_date) OVER ()
    AND ROW_NUMBER() OVER (PARTITION BY d.symbol ORDER BY d.snapshot_date DESC) = 1
),
cal AS (
  SELECT symbol, next_earnings_date, days_to_earnings_now, next_exdiv_date, days_to_exdiv_now,
         last_earnings_date, eps_surprise_pct
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
    ARRAY_AGG(headline IGNORE NULLS ORDER BY COALESCE(published_at, first_seen_at) DESC LIMIT 1)[SAFE_OFFSET(0)] AS last_headline
  FROM `{{DS}}.v_news_items`
  WHERE symbol IS NOT NULL
    AND COALESCE(published_at, first_seen_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  GROUP BY symbol
),
j AS (
  SELECT
    h.*,
    DATE_DIFF(h.snapshot_date, h.buy_date, DAY) AS days_held,
    d.action AS pd_action,
    d.price_sar AS pd_price_sar,
    d.market_value_sar AS pd_market_value_sar,
    d.pnl_sar AS pd_pnl_sar,
    d.pnl_pct AS pd_pnl_pct,
    d.weight_pct AS pd_weight_pct,
    d.delta_sar AS pd_delta_sar,
    d.stop_sar AS pd_stop_sar,
    d.tp1_sar AS pd_tp1_sar,
    d.tp2_sar AS pd_tp2_sar,
    d.roi_pct AS pd_roi_pct,
    d.reliability AS pd_reliability,
    d.data_quality AS pd_data_quality,
    d.confidence AS pd_confidence,
    d.advisor_note AS pd_advisor_note,
    d.pd_status,
    ROUND((SAFE_DIVIDE(d.price_sar, d.stop_sar) - 1) * 100, 2) AS pd_above_stop_pct,
    ROUND((SAFE_DIVIDE(d.tp1_sar, d.price_sar) - 1) * 100, 2) AS pd_to_tp1_pct,
    u.symbol IS NOT NULL AS on_market_page,
    u.region AS mkt_region,
    u.change_pct AS mkt_change_pct,
    u.position_52w_pct AS mkt_position_52w_pct,
    u.expected_roi_1m_pct AS mkt_expected_roi_1m_pct,
    u.expected_roi_3m_pct AS mkt_expected_roi_3m_pct,
    u.expected_roi_12m_pct AS mkt_expected_roi_12m_pct,
    u.reliability AS mkt_reliability,
    u.data_quality AS mkt_data_quality,
    u.meets_floors AS mkt_meets_floors,
    u.risk_bucket AS mkt_risk_bucket,
    u.volatility_30d_pct AS mkt_volatility_30d_pct,
    u.max_drawdown_1y_pct AS mkt_max_drawdown_1y_pct,
    u.rsi_14 AS mkt_rsi_14,
    u.signal AS mkt_signal,
    u.candle_signal AS mkt_candle_signal,
    u.analyst_rating AS mkt_analyst_rating,
    u.target_upside_pct AS mkt_target_upside_pct,
    u.recommendation AS mkt_recommendation,
    u.final_action AS mkt_final_action,
    u.top_risks AS mkt_top_risks,
    u.data_age_hours AS mkt_data_age_hours,
    c.next_earnings_date,
    c.days_to_earnings_now,
    c.next_exdiv_date,
    c.days_to_exdiv_now,
    c.last_earnings_date,
    c.eps_surprise_pct,
    COALESCE(n.news_7d, 0) AS news_7d,
    COALESCE(n.news_negative_7d, 0) AS news_negative_7d,
    COALESCE(n.news_positive_7d, 0) AS news_positive_7d,
    COALESCE(n.news_high_severity_7d, 0) AS news_high_severity_7d,
    n.last_headline,
    ARRAY(
      SELECT f FROM UNNEST([
        IF(d.price_sar <= d.stop_sar, 'at or below the stop', NULL),
        IF(d.price_sar >= d.tp1_sar, 'at or above the first target', NULL),
        IF(c.days_to_earnings_now BETWEEN 0 AND 5, 'earnings within 5 days', NULL),
        IF(c.days_to_exdiv_now BETWEEN 0 AND 5, 'ex-dividend within 5 days', NULL),
        IF(n.news_negative_7d > 0, 'negative news in 7 days', NULL),
        IF(n.news_high_severity_7d > 0, 'high-severity news in 7 days', NULL),
        IF(UPPER(u.recommendation) IN ('REDUCE', 'SELL', 'STRONG SELL', 'AVOID', 'EXIT', 'TRIM'),
           CONCAT('engine says ', UPPER(u.recommendation)), NULL),
        IF(u.expected_roi_12m_pct < 0, 'negative 12-month forecast', NULL),
        IF(u.data_age_hours > 36, 'market data older than 36 h', NULL),
        IF(u.symbol IS NOT NULL AND NOT u.meets_floors, 'below reliability / data-quality floors', NULL)
      ]) AS f
      WHERE f IS NOT NULL
    ) AS flags
  FROM hold AS h
  LEFT JOIN pd AS d ON d.symbol = h.symbol
  LEFT JOIN `{{DS}}.v_universe_latest` AS u ON u.symbol = h.symbol
  LEFT JOIN cal AS c ON c.symbol = h.symbol
  LEFT JOIN news AS n ON n.symbol = h.symbol
)
SELECT
  j.* EXCEPT (flags),
  ARRAY_TO_STRING(j.flags, ' · ') AS caution_flags,
  ARRAY_LENGTH(j.flags) AS caution_count
FROM j;
