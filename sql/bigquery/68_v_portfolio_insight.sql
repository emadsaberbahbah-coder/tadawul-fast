-- v_portfolio_insight: the latest archived My_Portfolio holdings, each with
-- the evidence a hold / trim / exit decision needs: the Portfolio_Decision
-- row for the name from the same nightly snapshot (pd_*: action, SAR P&L,
-- weight, stop and targets, advisor note - NULL when that night's decision
-- had no row for it, never an older night's), the engine's newest market-page view (mkt_*),
-- the next earnings and ex-dividend dates, the news of the last 7 days and
-- the TFB rating with the analyst consensus (v_symbol_rating, 1.3.0).
-- caution_flags lists, in plain words, what argues for a review (the price
-- at or below the stop, at or above the first target, earnings or
-- ex-dividend within 5 days, negative or high-severity news, a bearish
-- engine call, a negative 12-month forecast, data older than 36 h when
-- archived, a market copy more than a day old, below the floors, an
-- analyst consensus below Hold);
-- caution_count counts them. Percent points. symbol_label keys the
-- symbol picker shared with the candle, news and analyst views.
-- 1.4.0: every holding carries its value, cost and P&L in riyals
-- (value_sar, cost_sar, pnl_sar, pnl_sar_pct) and its share of the book
-- (weight_sar_pct). The Portfolio_Decision figures are used when that night
-- had a row for the name; otherwise the My_Portfolio figures, which are in
-- the holding's own currency, are converted with v_fx_sar, and value_source
-- says which. Before 1.4.0 the page totals left out any holding the
-- decision page did not carry, and the sheet's position value mixed
-- currencies in one column.
CREATE OR REPLACE VIEW `{{DS}}.v_portfolio_insight`
OPTIONS (description = "TFB Looker v4: the latest holdings with riyal values for every holding, the portfolio decision, market-page evidence, events, 7-day news, the TFB rating with its gate and plain-word caution flags")
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
  QUALIFY ROW_NUMBER() OVER (PARTITION BY d.snapshot_date, d.symbol ORDER BY d.captured_at DESC) = 1
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
    COALESCE(u.symbol_label, h.symbol) AS symbol_label,
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
    u.snapshot_age_days AS mkt_snapshot_age_days,
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
    rt.tfb_rating_score,
    rt.tfb_rating_label,
    rt.tfb_rating_stars,
    rt.rating_confidence,
    rt.rating_parts,
    rt.rating_gate,
    rt.tfb_rating_display,
    rt.analyst_source,
    rt.news_source,
    rt.analyst_consensus_label,
    rt.analyst_consensus_score,
    rt.analysts,
    fx.rate_to_sar AS fx_to_sar,
    fx.fx_source,
    COALESCE(d.market_value_sar, h.position_value * fx.rate_to_sar) AS value_sar,
    COALESCE(d.cost_sar, h.position_cost * fx.rate_to_sar) AS cost_sar,
    COALESCE(d.pnl_sar, (h.position_value - h.position_cost) * fx.rate_to_sar) AS pnl_sar,
    CASE
      WHEN d.market_value_sar IS NOT NULL THEN 'Portfolio_Decision'
      WHEN fx.rate_to_sar IS NOT NULL THEN CONCAT('My_Portfolio x ', fx.fx_source)
      ELSE 'no rate for the currency'
    END AS value_source,
    ARRAY(
      SELECT f FROM UNNEST([
        IF(d.price_sar <= d.stop_sar, 'at or below the stop', NULL),
        IF(d.price_sar >= d.tp1_sar, 'at or above the first target', NULL),
        IF(c.days_to_earnings_now BETWEEN 0 AND 5, 'earnings within 5 days', NULL),
        IF(c.days_to_exdiv_now BETWEEN 0 AND 5, 'ex-dividend within 5 days', NULL),
        IF(n.news_negative_7d > 0, 'negative news in 7 days', NULL),
        IF(n.news_high_severity_7d > 0, 'high-severity news in 7 days', NULL),
        IF(UPPER(REPLACE(u.recommendation, '_', ' ')) IN ('REDUCE', 'SELL', 'STRONG SELL', 'AVOID', 'EXIT', 'TRIM'),
           CONCAT('engine says ', UPPER(REPLACE(u.recommendation, '_', ' '))), NULL),
        IF(u.expected_roi_12m_pct < 0, 'negative 12-month forecast', NULL),
        IF(u.data_age_hours > 36, 'market data older than 36 h', NULL),
        IF(u.snapshot_age_days > 1, CONCAT('market copy ', CAST(u.snapshot_age_days AS STRING), ' days old'), NULL),
        IF(u.symbol IS NOT NULL AND NOT u.meets_floors, 'below reliability / data-quality floors', NULL),
        IF(rt.analyst_consensus_score < 2.5, 'analyst consensus below Hold', NULL)
      ]) AS f
      WHERE f IS NOT NULL
    ) AS flags
  FROM hold AS h
  LEFT JOIN pd AS d ON d.symbol = h.symbol AND d.snapshot_date = h.snapshot_date
  LEFT JOIN `{{DS}}.v_universe_latest` AS u ON u.symbol = h.symbol
  LEFT JOIN cal AS c ON c.symbol = h.symbol
  LEFT JOIN news AS n ON n.symbol = h.symbol
  LEFT JOIN `{{DS}}.v_symbol_rating` AS rt ON rt.symbol = h.symbol
  LEFT JOIN `{{DS}}.v_fx_sar` AS fx ON fx.currency = h.currency
)
SELECT
  j.* EXCEPT (flags),
  ROUND(SAFE_DIVIDE(j.pnl_sar, j.cost_sar) * 100, 2) AS pnl_sar_pct,
  ROUND(SAFE_DIVIDE(j.value_sar, SUM(j.value_sar) OVER ()) * 100, 2) AS weight_sar_pct,
  ARRAY_TO_STRING(j.flags, ' · ') AS caution_flags,
  ARRAY_LENGTH(j.flags) AS caution_count
FROM j;
