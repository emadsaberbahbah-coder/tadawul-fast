-- v_price_drivers: for every stock on TFB's pages, the forces on its price beside its own score, on one row -
-- what the options market expects and leans to, how much of its volume is sold short, how it moved against its
-- sector this past week, how heavily it traded, what analysts changed, how far the next earnings are, and the
-- state of the whole market - with an observe-only "score with drivers" beside TFB's own score.
-- WHY (owner, 22 Nov 2026 19:53 Riyadh): "add how future options and other options affect the current price and
-- recommendations, as well we can add this effect to our score and analysis, also check other factors that
-- effect current price and add them to our project in best way". TFB's overall score reads value, quality,
-- momentum, growth and risk from the stock's own data. This view puts the other forces next to it, so the
-- strategist, the plan pages and the owner see them in one place, and v_driver_learning can test each one.
-- COLUMNS (besides the universe row: price, change, overall score, recommendation, final action):
--   opt_*            the latest options reading of the name (v_options_reading, at most 6 days old)
--   iv_hv_ratio      30-day implied over 30-day realised volatility: above 1.3 the market expects more movement
--                    than the stock has shown (news ahead), below 0.8 less
--   rs_1w_vs_sector_pp  the stock's move over about a week (TFB's snapshots 7 days apart) minus its US sector
--                    ETF over the same sessions (a snapshot dated D carries D's US close: the archive runs after it). Over a week, studies find reversal more often than momentum:
--                    v_driver_learning measures which holds for TFB's names
--   volume_ratio     the last session's volume over its 30-day average
--   analyst_change_1m  the change of the analysts' consensus score over a month (v_symbol_analyst)
--   days_to_earnings the days to the next earnings date TFB last recorded, counted to today
--   market_*         the state of the US market (v_market_drivers, latest day)
--   driver_tilt_pp   observe-only: 1.5 x options tilt, minus 1 when short selling runs 2 sd above its usual
--                    share. Range about -5.5 .. +4.5 points.
--   overall_with_drivers_shadow  TFB's overall score plus driver_tilt_pp, 0..100. It decides nothing: it is
--                    logged here so the record can show whether it would have ranked the names better.
--   flags            plain words for whatever stands out.
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_price_drivers`
OPTIONS (description = "TFB price drivers (views 1.23.0): per stock on TFB's pages the latest options reading (implied volatility and move, event priced, skew, puts/calls, options tilt), short volume, implied over realised volatility, the week against its sector ETF, volume against average, the analysts' monthly change, days to earnings and the US market's state, with an observe-only score with drivers beside TFB's overall score - research only")
AS
WITH u AS (
  SELECT symbol, name, sector, market_code, snapshot_date, current_price, change_pct, volume, avg_volume_30d,
    from_52w_high_pct, beta_5y, volatility_30d_pct, rsi_14, overall_score, recommendation, final_action,
    investability_status
  FROM `{{DS}}.v_universe_latest`
  WHERE source_page IN ('Global_Markets', 'Market_Leaders')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY source_page DESC) = 1
),
week_ago AS (
  SELECT symbol, ANY_VALUE(current_price) AS p0, ANY_VALUE(snapshot_date) AS d0
  FROM `{{DS}}.v_universe`
  WHERE snapshot_date = DATE_SUB(CURRENT_DATE('Asia/Riyadh'), INTERVAL 7 DAY)
    AND source_page IN ('Global_Markets', 'Market_Leaders') AND current_price > 0
  GROUP BY symbol
),
etf AS (
  SELECT symbol, date, close FROM `{{DS}}.v_market_daily`
  WHERE symbol IN ('XLK', 'XLF', 'XLE', 'XLV', 'XLI', 'XLY', 'XLP', 'XLU', 'XLB', 'XLRE', 'XLC', 'SPY')
    AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
),
sect AS (
  SELECT u.symbol,
    CASE u.sector WHEN 'Technology' THEN 'XLK' WHEN 'Financial Services' THEN 'XLF' WHEN 'Financials' THEN 'XLF'
      WHEN 'Energy' THEN 'XLE' WHEN 'Healthcare' THEN 'XLV' WHEN 'Industrials' THEN 'XLI'
      WHEN 'Consumer Cyclical' THEN 'XLY' WHEN 'Consumer Defensive' THEN 'XLP' WHEN 'Utilities' THEN 'XLU'
      WHEN 'Basic Materials' THEN 'XLB' WHEN 'Real Estate' THEN 'XLRE' WHEN 'Communication Services' THEN 'XLC'
      ELSE 'SPY' END AS etf_symbol
  FROM u
),
dd AS (
  SELECT MAX(snapshot_date) AS d1, DATE_SUB(CURRENT_DATE('Asia/Riyadh'), INTERVAL 7 DAY) AS d0 FROM u
),
etf_ret AS (
  -- each ETF's move between the last trading days before the two snapshot dates (the days whose prices they hold)
  SELECT e.symbol AS etf_symbol,
    (SAFE_DIVIDE(ARRAY_AGG(IF(e.date <= dd.d1, e.close, NULL) IGNORE NULLS ORDER BY e.date DESC LIMIT 1)[SAFE_OFFSET(0)],
                 ARRAY_AGG(IF(e.date <= dd.d0, e.close, NULL) IGNORE NULLS ORDER BY e.date DESC LIMIT 1)[SAFE_OFFSET(0)]) - 1) * 100 AS etf_r_pct
  FROM etf e CROSS JOIN dd
  GROUP BY e.symbol
),
opt AS (
  SELECT * FROM `{{DS}}.v_options_reading`
  WHERE snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY snapshot_date DESC) = 1
),
an AS (
  SELECT symbol, consensus_change_month, consensus_label, analysts
  FROM `{{DS}}.v_symbol_analyst`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY snapshot_date DESC) = 1
),
earn AS (
  SELECT symbol,
    days_to_earnings - DATE_DIFF(CURRENT_DATE('Asia/Riyadh'), DATE(recorded_at, 'Asia/Riyadh'), DAY) AS days_to_earnings
  FROM `{{DS}}.v_signal_history`
  WHERE days_to_earnings IS NOT NULL AND recorded_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY recorded_at DESC) = 1
),
mkt AS (
  SELECT date AS market_date, vix, vix_term_ratio, vix_term_state, vix_level_state, credit_state, breadth_state,
    dollar_state, rates_state
  FROM `{{DS}}.v_market_drivers`
  QUALIFY ROW_NUMBER() OVER (ORDER BY date DESC) = 1
),
j AS (
  SELECT u.*, o.snapshot_date AS opt_date, o.iv30 AS opt_iv30, o.iv_pctile_1y AS opt_iv_pctile_1y,
    o.iv30_chg_5d_pp AS opt_iv30_chg_5d_pp, o.implied_move_pct AS opt_implied_move_pct, o.front_expiry AS opt_front_expiry,
    o.front_days AS opt_front_days, o.event_priced AS opt_event_priced, o.skew_25d_pp AS opt_skew_25d_pp,
    o.cp_iv_spread_pp AS opt_cp_iv_spread_pp, o.pc_volume_ratio AS opt_pc_volume_ratio, o.pc_oi_ratio AS opt_pc_oi_ratio,
    o.option_volume AS opt_volume, o.options_tilt AS opt_tilt, o.reading AS opt_reading,
    o.short_ratio_pct, o.short_ratio_5d_pct, o.short_ratio_z,
    SAFE_DIVIDE(o.iv30, u.volatility_30d_pct) AS iv_hv_ratio,
    (SAFE_DIVIDE(u.current_price, w.p0) - 1) * 100 - r.etf_r_pct AS rs_1w_vs_sector_pp,
    sc.etf_symbol AS sector_etf,
    SAFE_DIVIDE(u.volume, u.avg_volume_30d) AS volume_ratio,
    a.consensus_change_month AS analyst_change_1m, a.consensus_label AS analyst_consensus,
    IF(e.days_to_earnings >= 0, e.days_to_earnings, NULL) AS days_to_earnings,
    m.*
  FROM u
  LEFT JOIN opt o ON o.symbol = u.symbol
  LEFT JOIN week_ago w ON w.symbol = u.symbol
  LEFT JOIN sect sc ON sc.symbol = u.symbol
  LEFT JOIN etf_ret r ON r.etf_symbol = sc.etf_symbol
  LEFT JOIN an a ON a.symbol = u.symbol
  LEFT JOIN earn e ON e.symbol = u.symbol
  CROSS JOIN mkt m
)
SELECT
  symbol, name, sector, market_code, snapshot_date, current_price, change_pct, overall_score, recommendation,
  final_action, investability_status,
  opt_date, opt_iv30, opt_iv_pctile_1y, opt_iv30_chg_5d_pp, opt_implied_move_pct, opt_front_expiry, opt_front_days,
  opt_event_priced, opt_skew_25d_pp, opt_cp_iv_spread_pp, opt_pc_volume_ratio, opt_pc_oi_ratio, opt_volume, opt_tilt,
  opt_reading, short_ratio_pct, short_ratio_5d_pct, short_ratio_z,
  ROUND(iv_hv_ratio, 2) AS iv_hv_ratio, sector_etf, ROUND(rs_1w_vs_sector_pp, 2) AS rs_1w_vs_sector_pp,
  ROUND(volume_ratio, 2) AS volume_ratio, from_52w_high_pct, beta_5y, rsi_14,
  analyst_change_1m, analyst_consensus, days_to_earnings,
  market_date, ROUND(vix, 2) AS market_vix, vix_term_state AS market_vix_curve, vix_level_state AS market_vix_level,
  credit_state AS market_credit, breadth_state AS market_breadth, dollar_state AS market_dollar, rates_state AS market_rates,
  ROUND(IFNULL(1.5 * opt_tilt, 0) - IF(short_ratio_z >= 2, 1, 0), 2) AS driver_tilt_pp,
  IF(overall_score IS NULL, NULL,
     ROUND(GREATEST(0, LEAST(100, overall_score + IFNULL(1.5 * opt_tilt, 0) - IF(short_ratio_z >= 2, 1, 0))), 1))
    AS overall_with_drivers_shadow,
  NULLIF(ARRAY_TO_STRING(ARRAY(SELECT x FROM UNNEST([
    opt_reading,
    IF(days_to_earnings BETWEEN 0 AND 10, FORMAT('earnings in %d days', days_to_earnings), NULL),
    IF(iv_hv_ratio >= 1.3, 'options expect more movement than the stock has shown', NULL),
    IF(iv_hv_ratio <= 0.8, 'options expect less movement than the stock has shown', NULL),
    IF(volume_ratio >= 2, FORMAT('traded %.1f times its usual volume', volume_ratio), NULL),
    IF(rs_1w_vs_sector_pp >= 8, FORMAT('beat its sector by %.1f points this week', rs_1w_vs_sector_pp), NULL),
    IF(rs_1w_vs_sector_pp <= -8, FORMAT('lagged its sector by %.1f points this week', -rs_1w_vs_sector_pp), NULL),
    IF(vix_term_state = 'inverted', 'the whole market is under stress (VIX above VIX3M)', NULL)
  ]) AS x WHERE x IS NOT NULL), ' | '), '') AS flags
FROM j;
