-- v_market_drivers: what moves the whole US market beside the stocks themselves, one row per US trading day -
-- the options market's fear curve, index futures, rates, the dollar, credit and breadth - with the S&P 500's
-- next 5 and 20 sessions for the record.
-- WHY (owner, 22 Nov 2026 19:53 Riyadh): "add how future options and other options affect the current price and
-- recommendations ... also check other factors that affect current price and add them to our project". A stock
-- falls with its market more often than alone: on 20 Nov 2026 the median US stock TFB follows lost 6.2 % in a
-- week while VIX rose 86 %. The archive held the S&P 500 and VIX only. archive_market_series 1.4.0 adds the
-- 9-day, 3-month and 6-month VIX, VVIX, SKEW, MOVE, the E-mini futures, the 13-week to 30-year yields, the dollar
-- index, high-yield and investment-grade bonds, equal-weight S&P 500, Russell 2000 and Nasdaq 100.
-- WHAT each reading means (as commonly read by strategists, not as TFB evidence - v_market_driver_learning is):
--   vix_term_ratio   VIX / VIX3M. Above 1 the market pays more for one month of protection than for three:
--                    stress now (backwardation). Below about 0.9 calm (contango).
--   vix9d_ratio      VIX9D / VIX. Above 1 an event is feared within days.
--   vvix, skew_index the price of volatility itself and of crash protection.
--   move             Treasury volatility: rate shocks spill into equities.
--   futures_vs_cash_pct  the front E-mini over the cash index (carry, roll and the move after the cash close).
--   ust10y, curve_10y_3m_pp, ust10y_chg_5d_bp  the 10-year yield, its gap to the 13-week bill, its 5-day change.
--   dxy_r20_pct      the dollar's last month: a strong dollar weighs on US multinationals and emerging markets.
--   credit_r20_pct   high-yield bonds against Treasuries (HYG / IEF), last month: falling = credit stress.
--   breadth_r20_pct  equal-weight against cap-weight S&P 500 (RSP / SPY), last month: falling = a narrow market.
--   small_r20_pct    Russell 2000 against the S&P 500 (IWM / SPY), last month: risk appetite.
-- Each series' value is the last one known on or before the day (a yield or future that did not print that
-- day keeps its previous value). z-scores are against the series' own previous 252 trading days.
-- states: plain words for each reading, used by v_market_driver_learning and v_price_drivers.
-- spy_next5_pct / spy_next20_pct: the SPY total return over the next 5 / 20 sessions (NULL until they exist).
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_market_drivers`
OPTIONS (description = "TFB market drivers (views 1.23.0): per US trading day the VIX curve (9D, 1M, 3M, 6M), VVIX, SKEW, MOVE, E-mini futures against cash, Treasury yields and curve, the dollar, credit (HYG vs IEF), breadth (RSP vs SPY), small caps (IWM vs SPY), oil and gold, each with its z-score, plain-word states and the next 5 and 20 sessions of SPY - research only")
AS
WITH raw AS (
  SELECT date,
    MAX(IF(symbol = '^GSPC', close, NULL)) AS spx,
    MAX(IF(symbol = 'SPY', adj_close, NULL)) AS spy,
    MAX(IF(symbol = '^VIX', close, NULL)) AS vix,
    MAX(IF(symbol = '^VIX9D', close, NULL)) AS vix9d,
    MAX(IF(symbol = '^VIX3M', close, NULL)) AS vix3m,
    MAX(IF(symbol = '^VIX6M', close, NULL)) AS vix6m,
    MAX(IF(symbol = '^VVIX', close, NULL)) AS vvix,
    MAX(IF(symbol = '^SKEW', close, NULL)) AS skew_index,
    MAX(IF(symbol = '^MOVE', close, NULL)) AS move,
    MAX(IF(symbol = 'ES=F', close, NULL)) AS es,
    MAX(IF(symbol = 'NQ=F', close, NULL)) AS nq,
    MAX(IF(symbol = '^IRX', close, NULL)) AS ust3m,
    MAX(IF(symbol = '^FVX', close, NULL)) AS ust5y,
    MAX(IF(symbol = '^TNX', close, NULL)) AS ust10y,
    MAX(IF(symbol = '^TYX', close, NULL)) AS ust30y,
    MAX(IF(symbol = 'DX-Y.NYB', close, NULL)) AS dxy,
    MAX(IF(symbol = 'HYG', adj_close, NULL)) AS hyg,
    MAX(IF(symbol = 'IEF', adj_close, NULL)) AS ief,
    MAX(IF(symbol = 'RSP', adj_close, NULL)) AS rsp,
    MAX(IF(symbol = 'IWM', adj_close, NULL)) AS iwm,
    MAX(IF(symbol = 'BZ=F', close, NULL)) AS brent,
    MAX(IF(symbol = 'GC=F', close, NULL)) AS gold
  FROM `{{DS}}.v_market_daily`
  WHERE symbol IN ('^GSPC', 'SPY', '^VIX', '^VIX9D', '^VIX3M', '^VIX6M', '^VVIX', '^SKEW', '^MOVE', 'ES=F', 'NQ=F',
                   '^IRX', '^FVX', '^TNX', '^TYX', 'DX-Y.NYB', 'HYG', 'IEF', 'RSP', 'IWM', 'BZ=F', 'GC=F')
  GROUP BY date
),
filled AS (
  SELECT date, spx,
    LAST_VALUE(spy IGNORE NULLS) OVER w AS spy, LAST_VALUE(vix IGNORE NULLS) OVER w AS vix,
    LAST_VALUE(vix9d IGNORE NULLS) OVER w AS vix9d, LAST_VALUE(vix3m IGNORE NULLS) OVER w AS vix3m,
    LAST_VALUE(vix6m IGNORE NULLS) OVER w AS vix6m, LAST_VALUE(vvix IGNORE NULLS) OVER w AS vvix,
    LAST_VALUE(skew_index IGNORE NULLS) OVER w AS skew_index, LAST_VALUE(move IGNORE NULLS) OVER w AS move,
    LAST_VALUE(es IGNORE NULLS) OVER w AS es, LAST_VALUE(nq IGNORE NULLS) OVER w AS nq,
    LAST_VALUE(ust3m IGNORE NULLS) OVER w AS ust3m, LAST_VALUE(ust5y IGNORE NULLS) OVER w AS ust5y,
    LAST_VALUE(ust10y IGNORE NULLS) OVER w AS ust10y, LAST_VALUE(ust30y IGNORE NULLS) OVER w AS ust30y,
    LAST_VALUE(dxy IGNORE NULLS) OVER w AS dxy, LAST_VALUE(hyg IGNORE NULLS) OVER w AS hyg,
    LAST_VALUE(ief IGNORE NULLS) OVER w AS ief, LAST_VALUE(rsp IGNORE NULLS) OVER w AS rsp,
    LAST_VALUE(iwm IGNORE NULLS) OVER w AS iwm, LAST_VALUE(brent IGNORE NULLS) OVER w AS brent,
    LAST_VALUE(gold IGNORE NULLS) OVER w AS gold
  FROM raw
  WINDOW w AS (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
d AS (
  SELECT *,
    SAFE_DIVIDE(vix, vix3m) AS vix_term_ratio,
    SAFE_DIVIDE(vix9d, vix) AS vix9d_ratio,
    (SAFE_DIVIDE(es, spx) - 1) * 100 AS futures_vs_cash_pct,
    ust10y - ust3m AS curve_10y_3m_pp,
    (ust10y - LAG(ust10y, 5) OVER o) * 100 AS ust10y_chg_5d_bp,
    (SAFE_DIVIDE(dxy, LAG(dxy, 20) OVER o) - 1) * 100 AS dxy_r20_pct,
    (SAFE_DIVIDE(SAFE_DIVIDE(hyg, ief), LAG(SAFE_DIVIDE(hyg, ief), 20) OVER o) - 1) * 100 AS credit_r20_pct,
    (SAFE_DIVIDE(SAFE_DIVIDE(rsp, spy), LAG(SAFE_DIVIDE(rsp, spy), 20) OVER o) - 1) * 100 AS breadth_r20_pct,
    (SAFE_DIVIDE(SAFE_DIVIDE(iwm, spy), LAG(SAFE_DIVIDE(iwm, spy), 20) OVER o) - 1) * 100 AS small_r20_pct,
    (SAFE_DIVIDE(spy, LAG(spy, 5) OVER o) - 1) * 100 AS spy_r5_pct,
    (SAFE_DIVIDE(brent, LAG(brent, 5) OVER o) - 1) * 100 AS brent_r5_pct,
    (SAFE_DIVIDE(gold, LAG(gold, 5) OVER o) - 1) * 100 AS gold_r5_pct,
    (SAFE_DIVIDE(LEAD(spy, 5) OVER o, spy) - 1) * 100 AS spy_next5_pct,
    (SAFE_DIVIDE(LEAD(spy, 20) OVER o, spy) - 1) * 100 AS spy_next20_pct
  FROM filled
  WHERE spx IS NOT NULL
  WINDOW o AS (ORDER BY date)
),
z AS (
  SELECT *,
    SAFE_DIVIDE(vix - AVG(vix) OVER h, STDDEV(vix) OVER h) AS vix_z,
    SAFE_DIVIDE(vix_term_ratio - AVG(vix_term_ratio) OVER h, STDDEV(vix_term_ratio) OVER h) AS vix_term_z,
    SAFE_DIVIDE(vvix - AVG(vvix) OVER h, STDDEV(vvix) OVER h) AS vvix_z,
    SAFE_DIVIDE(skew_index - AVG(skew_index) OVER h, STDDEV(skew_index) OVER h) AS skew_z,
    SAFE_DIVIDE(move - AVG(move) OVER h, STDDEV(move) OVER h) AS move_z,
    SAFE_DIVIDE(credit_r20_pct - AVG(credit_r20_pct) OVER h, STDDEV(credit_r20_pct) OVER h) AS credit_z
  FROM d
  WINDOW h AS (ORDER BY date ROWS BETWEEN 252 PRECEDING AND 1 PRECEDING)
)
SELECT
  date, ROUND(spx, 2) AS spx, ROUND(spy, 2) AS spy, ROUND(spy_r5_pct, 2) AS spy_r5_pct,
  ROUND(vix, 2) AS vix, ROUND(vix9d, 2) AS vix9d, ROUND(vix3m, 2) AS vix3m, ROUND(vix6m, 2) AS vix6m,
  ROUND(vix_term_ratio, 3) AS vix_term_ratio, ROUND(vix9d_ratio, 3) AS vix9d_ratio,
  ROUND(vvix, 2) AS vvix, ROUND(skew_index, 2) AS skew_index, ROUND(move, 2) AS move,
  ROUND(es, 2) AS es, ROUND(nq, 2) AS nq, ROUND(futures_vs_cash_pct, 2) AS futures_vs_cash_pct,
  ROUND(ust3m, 3) AS ust3m, ROUND(ust5y, 3) AS ust5y, ROUND(ust10y, 3) AS ust10y, ROUND(ust30y, 3) AS ust30y,
  ROUND(curve_10y_3m_pp, 3) AS curve_10y_3m_pp, ROUND(ust10y_chg_5d_bp, 1) AS ust10y_chg_5d_bp,
  ROUND(dxy, 3) AS dxy, ROUND(dxy_r20_pct, 2) AS dxy_r20_pct, ROUND(credit_r20_pct, 2) AS credit_r20_pct,
  ROUND(breadth_r20_pct, 2) AS breadth_r20_pct, ROUND(small_r20_pct, 2) AS small_r20_pct,
  ROUND(brent_r5_pct, 2) AS brent_r5_pct, ROUND(gold_r5_pct, 2) AS gold_r5_pct,
  ROUND(vix_z, 2) AS vix_z, ROUND(vix_term_z, 2) AS vix_term_z, ROUND(vvix_z, 2) AS vvix_z, ROUND(skew_z, 2) AS skew_z,
  ROUND(move_z, 2) AS move_z, ROUND(credit_z, 2) AS credit_z,
  CASE WHEN vix_term_ratio IS NULL THEN NULL WHEN vix_term_ratio >= 1 THEN 'inverted' WHEN vix_term_ratio >= 0.9 THEN 'flat'
       ELSE 'normal' END AS vix_term_state,
  CASE WHEN vix IS NULL THEN NULL WHEN vix < 15 THEN 'under 15' WHEN vix < 20 THEN '15 to 20' WHEN vix < 30 THEN '20 to 30'
       ELSE '30 and over' END AS vix_level_state,
  CASE WHEN vvix_z IS NULL THEN NULL WHEN vvix_z >= 1 THEN 'high' WHEN vvix_z <= -1 THEN 'low' ELSE 'usual' END AS vvix_state,
  CASE WHEN skew_z IS NULL THEN NULL WHEN skew_z >= 1 THEN 'high' WHEN skew_z <= -1 THEN 'low' ELSE 'usual' END AS skew_state,
  CASE WHEN move_z IS NULL THEN NULL WHEN move_z >= 1 THEN 'high' WHEN move_z <= -1 THEN 'low' ELSE 'usual' END AS move_state,
  CASE WHEN credit_r20_pct IS NULL THEN NULL WHEN credit_r20_pct <= -1 THEN 'weakening' WHEN credit_r20_pct >= 1 THEN 'improving'
       ELSE 'steady' END AS credit_state,
  CASE WHEN breadth_r20_pct IS NULL THEN NULL WHEN breadth_r20_pct <= -1.5 THEN 'narrowing' WHEN breadth_r20_pct >= 1.5 THEN 'broadening'
       ELSE 'steady' END AS breadth_state,
  CASE WHEN small_r20_pct IS NULL THEN NULL WHEN small_r20_pct <= -2 THEN 'small caps lagging' WHEN small_r20_pct >= 2 THEN 'small caps leading'
       ELSE 'in line' END AS small_caps_state,
  CASE WHEN dxy_r20_pct IS NULL THEN NULL WHEN dxy_r20_pct >= 2 THEN 'dollar rising' WHEN dxy_r20_pct <= -2 THEN 'dollar falling'
       ELSE 'dollar steady' END AS dollar_state,
  CASE WHEN ust10y_chg_5d_bp IS NULL THEN NULL WHEN ust10y_chg_5d_bp >= 15 THEN 'yields jumping' WHEN ust10y_chg_5d_bp <= -15 THEN 'yields falling'
       ELSE 'yields steady' END AS rates_state,
  ROUND(spy_next5_pct, 2) AS spy_next5_pct, ROUND(spy_next20_pct, 2) AS spy_next20_pct
FROM z;
