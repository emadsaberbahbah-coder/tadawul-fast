-- v_regime_shadow: one shadow regime label per equity market and close - BULL, CHOPPY or RISK_OFF - from the
-- 23 Nov deep audit's design (the index against a rising 200-close average, the share of the market's names over
-- their own 50-close average, the percentile of the index's 20-close volatility) joined with TFB's count of adverse
-- US market states. WHY (week plan of 23 Nov 2026, package 6: "the audit's bull / choppy / risk-off regime and
-- our adverse-state count become one shadow rule"; the market-state count was folded into it as the candidate
-- gate; owner, 23 Nov 2026: "go and Start build"). The live gate reads one test per market (v_market_condition's
-- five labels, the engine's market_gate); nothing says whether a market's trend, its breadth and its volatility
-- agree. Observe only: no page, gate, pick or count reads this view.
-- THE MARKETS (v_market_condition's index readings): us SPY (price close), sa ^TASI.SR, eu ^STOXX, jp ^N225,
--   cn ^HSI. Breadth from TFB's followed names of the same market (v_bars_clean, by exchange suffix: us .US,
--   sa .SR, jp .T, cn .HK, eu the European exchanges .L .PA .DE .MC .MI .AS .BR .LS .OL .ST .CO .HE .SW .VI .IR
--   .AT .WA).
-- THE INPUTS at a close (from that close and the closes before it; no later one):
--   sma200, sma200_prev20   the 200-close average (200 closes present) and the same 20 closes earlier
--   trend                   up = close over a rising average (sma200 > sma200_prev20); down = close under a
--                           falling one; mixed otherwise; NULL before 220 closes
--   breadth50_pct           the share of the market's names whose close is over their own 50-close average (50
--                           closes inside one price basis of v_bars_clean); NULL under 20 such names
--   vol20_pct               v_market_condition's annualised deviation of the last 20 daily changes
--   vol_pct_rank            the share of the last 252 closes' vol20_pct at or under today's, 0-100 (200 present)
--   adverse                 the US only: the count of adverse US states that day, 0-6, exactly as
--                           v_projection_scorecard counts them (VIX 20 or more, VIX curve inverted, VVIX high,
--                           MOVE high, credit weakening, 10-year yield jumping); NULL for the other markets
--   condition_label         v_market_condition's label that close (calm / weak / stress / drawdown / crash_day)
-- THE RULE (rule_version 1, fixed on 28 Nov 2026 before any forward close; first match wins; an unknown input
-- never matches, as NULL does not):
--   RISK_OFF  trend = 'down', or vol_pct_rank >= 80, or breadth50_pct < 30, or adverse >= 3
--   BULL      trend = 'up' and vol_pct_rank < 60 and (breadth50_pct >= 50 or unknown) and (adverse <= 1 or the
--             market is not the US)
--   CHOPPY    everything else
--   NULL      while trend or vol_pct_rank is not known
--   regime_why  the tests behind the label, joined by ' | '
-- Research view: the live system never reads BigQuery. scripts/regime_shadow.py freezes each final close's row
-- once into regime_shadow_log; that log, not this view, is the forward record. (views 1.38.0)
CREATE OR REPLACE VIEW `{{DS}}.v_regime_shadow`
OPTIONS (description = "TFB shadow regime rule v1 (views 1.38.0): per equity market (us, sa, eu, jp, cn) and close - the index against its rising or falling 200-close average, the share of followed names over their 50-close average, the volatility percentile, the US adverse-state count, TFB's own condition label and one shadow label BULL / CHOPPY / RISK_OFF. Observe only, frozen daily into regime_shadow_log.")
AS
WITH idx AS (
  SELECT market_id, market, market_order, reading_symbol, date, close, vol20_pct, label AS condition_label
  FROM `{{DS}}.v_market_condition`
  WHERE market_id IN ('us', 'sa', 'eu', 'jp', 'cn') AND close > 0
),
avg200 AS (
  SELECT i.*,
    AVG(i.close) OVER (PARTITION BY i.market_id ORDER BY i.date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS a200,
    COUNT(i.close) OVER (PARTITION BY i.market_id ORDER BY i.date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS n200,
    ARRAY_AGG(IFNULL(i.vol20_pct, -1)) OVER (PARTITION BY i.market_id ORDER BY i.date
      ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS vols
  FROM idx AS i
),
trend_in AS (
  SELECT a.*,
    IF(a.n200 = 200, a.a200, NULL) AS sma200,
    LAG(IF(a.n200 = 200, a.a200, NULL), 20) OVER (PARTITION BY a.market_id ORDER BY a.date) AS sma200_prev20,
    IF(a.vol20_pct IS NULL, NULL,
       (SELECT IF(COUNTIF(v >= 0) < 200, NULL, ROUND(100 * COUNTIF(v >= 0 AND v <= a.vol20_pct) / COUNTIF(v >= 0), 1))
        FROM UNNEST(a.vols) AS v)) AS vol_pct_rank
  FROM avg200 AS a
),
names AS (
  SELECT symbol, date, basis, COALESCE(adj_close, close) AS p,
    CASE REGEXP_EXTRACT(symbol, r'\.([A-Z]+)$')
      WHEN 'US' THEN 'us' WHEN 'SR' THEN 'sa' WHEN 'T' THEN 'jp' WHEN 'HK' THEN 'cn'
      WHEN 'L' THEN 'eu' WHEN 'PA' THEN 'eu' WHEN 'DE' THEN 'eu' WHEN 'MC' THEN 'eu' WHEN 'MI' THEN 'eu'
      WHEN 'AS' THEN 'eu' WHEN 'BR' THEN 'eu' WHEN 'LS' THEN 'eu' WHEN 'OL' THEN 'eu' WHEN 'ST' THEN 'eu'
      WHEN 'CO' THEN 'eu' WHEN 'HE' THEN 'eu' WHEN 'SW' THEN 'eu' WHEN 'VI' THEN 'eu' WHEN 'IR' THEN 'eu'
      WHEN 'AT' THEN 'eu' WHEN 'WA' THEN 'eu' END AS market_id
  FROM `{{DS}}.v_bars_clean`
  WHERE close > 0 AND REGEXP_CONTAINS(symbol, r'\.[A-Z]+$')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY captured_at DESC) = 1
),
name50 AS (
  SELECT n.*,
    AVG(n.p) OVER (PARTITION BY n.symbol, n.basis ORDER BY n.date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS a50,
    COUNT(n.p) OVER (PARTITION BY n.symbol, n.basis ORDER BY n.date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS n50
  FROM names AS n
  WHERE n.market_id IS NOT NULL
),
breadth AS (
  SELECT market_id, date, COUNT(*) AS breadth_names,
    ROUND(100 * COUNTIF(p > a50) / COUNT(*), 1) AS breadth50_pct
  FROM name50
  WHERE n50 = 50
  GROUP BY market_id, date
),
adv AS (
  SELECT date,
    IF(vix_level_state IN ('20 to 30', '30 and over'), 1, 0) + IF(vix_term_state = 'inverted', 1, 0)
      + IF(vvix_state = 'high', 1, 0) + IF(move_state = 'high', 1, 0) + IF(credit_state = 'weakening', 1, 0)
      + IF(rates_state = 'yields jumping', 1, 0) AS adverse
  FROM `{{DS}}.v_market_drivers`
),
j AS (
  SELECT t.market_id, t.market, t.market_order, t.reading_symbol, t.date, t.close,
    ROUND(t.sma200, 4) AS sma200, ROUND(t.sma200_prev20, 4) AS sma200_prev20,
    CASE
      WHEN t.sma200 IS NULL OR t.sma200_prev20 IS NULL THEN NULL
      WHEN t.close > t.sma200 AND t.sma200 > t.sma200_prev20 THEN 'up'
      WHEN t.close < t.sma200 AND t.sma200 < t.sma200_prev20 THEN 'down'
      ELSE 'mixed'
    END AS trend,
    IF(b.breadth_names >= 20, b.breadth50_pct, NULL) AS breadth50_pct,
    b.breadth_names,
    t.vol20_pct, t.vol_pct_rank,
    IF(t.market_id = 'us', a.adverse, NULL) AS adverse,
    t.condition_label
  FROM trend_in AS t
  LEFT JOIN breadth AS b ON b.market_id = t.market_id AND b.date = t.date
  LEFT JOIN adv AS a ON a.date = t.date
)
SELECT
  j.*,
  1 AS rule_version,
  CASE
    WHEN j.trend IS NULL OR j.vol_pct_rank IS NULL THEN NULL
    WHEN j.trend = 'down' OR j.vol_pct_rank >= 80 OR j.breadth50_pct < 30 OR j.adverse >= 3 THEN 'RISK_OFF'
    WHEN j.trend = 'up' AND j.vol_pct_rank < 60 AND IFNULL(j.breadth50_pct >= 50, TRUE)
         AND (j.market_id != 'us' OR j.adverse <= 1) THEN 'BULL'
    ELSE 'CHOPPY'
  END AS regime,
  ARRAY_TO_STRING([
    FORMAT('trend %s', IFNULL(j.trend, 'not known')),
    IF(j.vol_pct_rank IS NULL, 'volatility percentile not known', FORMAT('volatility percentile %.0f', j.vol_pct_rank)),
    IF(j.breadth50_pct IS NULL, 'breadth not known', FORMAT('%.0f%% of %d names over their 50-close average',
       j.breadth50_pct, j.breadth_names)),
    IF(j.market_id != 'us', NULL, IF(j.adverse IS NULL, 'adverse states not known', FORMAT('%d adverse US states', j.adverse)))
  ], ' | ') AS regime_why
FROM j;
