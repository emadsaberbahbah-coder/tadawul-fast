-- v_options_reading: what the options market says about each stock, one row per name and trading day, with its
-- own history and its place among the day's other stocks.
-- WHY (owner, 22 Nov 2026 19:53 Riyadh): "add how future options and other options affect the current price and
-- recommendations, as well we can add this effect to our score and analysis". archive_options_reading 1.0.0 keeps
-- the Cboe delayed chain readings and FINRA short volume of the US stocks TFB follows in options_daily.
-- WHAT this adds to each row:
--   iv30_chg_5d_pp     the 30-day implied volatility against 5 trading days before (points)
--   iv_pctile_1y       where today's iv30 sits in the name's own last year (0 to 100; NULL under 20 days)
--   term_ratio         front-expiry ATM volatility over the 90-day one: above 1.15 an event is priced before
--                      the front expiry (an earnings date, a ruling, a vote) - event_priced
--   implied_move_daily_pct  the implied move spread over the trading days to the front expiry
--   short_ratio_5d_pct and short_ratio_z  the share of volume sold short, 5-day mean, and its z-score against
--                      the name's own previous 20 days
--   z_skew, z_cp, z_pc  each stock against the other stocks of the day (ETFs apart): the 25-delta skew, the
--                      call-minus-put volatility spread at the money, and the log of the put/call volume ratio
--                      (only where 100 contracts or more traded)
--   options_tilt       z_cp - z_skew - 0.5 * z_pc, capped at -3..3. Published studies of US stocks found calls
--                      dear against puts (Cremers & Weinbaum 2010) followed by higher returns, and a steep put skew
--                      (Xing, Zhang & Zhao 2010) and heavy put buying (Pan & Poteshman 2006) by lower ones, over
--                      days to weeks. Whether that holds for TFB's names is v_driver_learning's question:
--                      the tilt is observe-only and enters no score until it proves itself.
--   reading            plain words for the row.
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_options_reading`
OPTIONS (description = "TFB options reading (views 1.23.0): per US stock or ETF and trading day the Cboe 30-day implied volatility, its 5-day change and 1-year percentile, the implied move to the front expiry, the event-priced flag, the 25-delta skew, the calls-minus-puts volatility spread, puts/calls volume and open interest, FINRA short volume and its z-score, each stock's z-scores against the day and the observe-only options tilt - research only")
AS
WITH o AS (
  SELECT * FROM `{{DS}}.options_daily`
),
hist AS (
  SELECT a.symbol, a.snapshot_date, COUNT(*) AS iv_days,
    100 * COUNTIF(b.iv30 <= a.iv30) / COUNT(*) AS iv_pctile
  FROM o a
  JOIN o b ON b.symbol = a.symbol AND b.snapshot_date BETWEEN DATE_SUB(a.snapshot_date, INTERVAL 365 DAY) AND a.snapshot_date
  WHERE a.iv30 IS NOT NULL AND b.iv30 IS NOT NULL
  GROUP BY a.symbol, a.snapshot_date
),
w AS (
  SELECT o.*,
    o.iv30 - LAG(o.iv30, 5) OVER s AS iv30_chg_5d,
    AVG(o.short_ratio_pct) OVER (PARTITION BY o.symbol ORDER BY o.snapshot_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS short_5d,
    AVG(o.short_ratio_pct) OVER (PARTITION BY o.symbol ORDER BY o.snapshot_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS short_mean_20,
    STDDEV(o.short_ratio_pct) OVER (PARTITION BY o.symbol ORDER BY o.snapshot_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS short_sd_20,
    COUNT(o.short_ratio_pct) OVER (PARTITION BY o.symbol ORDER BY o.snapshot_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS short_n_20,
    SAFE_DIVIDE(o.atm_iv_front, o.atm_iv_90d) AS term_ratio,
    IF(o.call_volume + o.put_volume >= 100 AND o.call_volume > 0 AND o.put_volume > 0, LN(o.put_volume / o.call_volume), NULL) AS ln_pc
  FROM o
  WINDOW s AS (PARTITION BY o.symbol ORDER BY o.snapshot_date)
),
z AS (
  SELECT w.*,
    IF(is_etf, NULL, SAFE_DIVIDE(skew_25d_pp - AVG(IF(is_etf, NULL, skew_25d_pp)) OVER d, STDDEV(IF(is_etf, NULL, skew_25d_pp)) OVER d)) AS z_skew,
    IF(is_etf, NULL, SAFE_DIVIDE(cp_iv_spread_pp - AVG(IF(is_etf, NULL, cp_iv_spread_pp)) OVER d, STDDEV(IF(is_etf, NULL, cp_iv_spread_pp)) OVER d)) AS z_cp,
    IF(is_etf, NULL, SAFE_DIVIDE(ln_pc - AVG(IF(is_etf, NULL, ln_pc)) OVER d, STDDEV(IF(is_etf, NULL, ln_pc)) OVER d)) AS z_pc
  FROM w
  WINDOW d AS (PARTITION BY snapshot_date)
),
t AS (
  SELECT z.*,
    IF(z_cp IS NULL AND z_skew IS NULL, NULL,
       GREATEST(-3, LEAST(3, IFNULL(z_cp, 0) - IFNULL(z_skew, 0) - 0.5 * IFNULL(z_pc, 0)))) AS tilt,
    IFNULL(term_ratio >= 1.15 AND front_days <= 45, FALSE) AS event_priced,
    IF(short_n_20 >= 10, SAFE_DIVIDE(short_ratio_pct - short_mean_20, short_sd_20), NULL) AS short_z
  FROM z
)
SELECT
  t.snapshot_date, t.symbol, t.underlying, t.is_etf, t.price,
  t.iv30, ROUND(t.iv30_chg_5d, 2) AS iv30_chg_5d_pp,
  IF(h.iv_days >= 20, ROUND(h.iv_pctile, 0), NULL) AS iv_pctile_1y, IFNULL(h.iv_days, 0) AS iv_history_days,
  t.atm_iv_front, t.atm_iv_30d, t.atm_iv_90d, ROUND(t.term_ratio, 3) AS term_ratio,
  t.front_expiry, t.front_days, t.implied_move_pct,
  ROUND(SAFE_DIVIDE(t.implied_move_pct, SQRT(GREATEST(1, t.front_days * 5 / 7))), 2) AS implied_move_daily_pct,
  t.event_priced,
  t.skew_25d_pp, t.cp_iv_spread_pp, t.pc_volume_ratio, t.pc_oi_ratio, t.volume_oi_ratio,
  t.call_volume + t.put_volume AS option_volume, t.call_oi + t.put_oi AS open_interest,
  t.short_ratio_pct, ROUND(t.short_5d, 2) AS short_ratio_5d_pct, ROUND(t.short_z, 2) AS short_ratio_z, t.short_date,
  ROUND(t.z_skew, 2) AS z_skew, ROUND(t.z_cp, 2) AS z_cp, ROUND(t.z_pc, 2) AS z_pc, ROUND(t.tilt, 2) AS options_tilt,
  NULLIF(ARRAY_TO_STRING(ARRAY(SELECT x FROM UNNEST([
    IF(t.event_priced, FORMAT('an event is priced before %t: options expect about %.1f %% either way', t.front_expiry, t.implied_move_pct), NULL),
    IF(t.tilt >= 1, 'options lean up (calls dear against puts)', NULL),
    IF(t.tilt <= -1, 'options lean down (puts dear, or heavy put buying)', NULL),
    IF(h.iv_days >= 20 AND h.iv_pctile >= 90, 'implied volatility near its high of the year', NULL),
    IF(h.iv_days >= 20 AND h.iv_pctile <= 10, 'implied volatility near its low of the year', NULL),
    IF(t.short_z >= 2, 'short selling well above its usual share', NULL),
    IF(t.iv30_chg_5d >= 10, 'implied volatility up sharply this week', NULL)
  ]) AS x WHERE x IS NOT NULL), ' | '), '') AS reading,
  t.source, t.captured_at
FROM t
LEFT JOIN hist h ON h.symbol = t.symbol AND h.snapshot_date = t.snapshot_date;
