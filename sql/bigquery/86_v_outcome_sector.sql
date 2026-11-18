-- v_outcome_sector: the project's buy calls by market group, sector and
-- horizon against their market benchmark. WHY (owner, 19 Nov 2026 01:18
-- Riyadh: "how to improve our forecasting and recommendation"): the review of
-- that night found the INVESTABLE list concentrated in one sector (US
-- financials) and no view able to say whether the engine picks well inside a
-- sector, because Performance_Log records carry no sector. This view joins
-- each scored buy call to v_symbol_sector and measures it the way
-- v_trial_c1 measures calls: the call's return minus its market benchmark's
-- over the same days (v_outcome_excess.excess_pp).
-- Buy calls: entry recommendation BUY, STRONG BUY or ACCUMULATE, in the
-- engine cohort, with an excess return. A name recorded on many days counts
-- once per day, so calls overlap; read 'names' beside 'calls'.
-- READING, in this order: 'too few calls' under 100 calls; 'beats its market'
-- at an average edge of +0.5 points or more with at least 55% of calls ahead;
-- 'loses to its market' at -0.5 points or worse with at most 45% ahead;
-- otherwise 'no clear edge'. The same 100-call floor as the calls-against-
-- the-market panel.
-- COLUMNS: market_group (US / Tadawul / Other markets / ALL), sector
-- ('(no sector)' when v_symbol_sector has none), horizon, calls, names, days,
-- first_day, last_day, edge_pp, right_pct, avg_forecast_pp, avg_realized_pp,
-- reading.
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_outcome_sector`
OPTIONS (description = "TFB buy calls by market group, GICS sector and horizon against the market benchmark - calls, names, average edge, share ahead, forecast against realized, and a fixed reading (at least 100 calls)")
AS
WITH c AS (
  SELECT
    IF(e.market IN ('US', 'Tadawul'), e.market, 'Other markets') AS mg,
    IFNULL(s.sector, '(no sector)') AS sector,
    e.horizon,
    e.symbol,
    e.recorded_date,
    e.excess_pp AS edge,
    e.target_roi_pp AS fc,
    e.engine_roi_pp AS realized
  FROM `{{DS}}.v_outcome_excess` AS e
  LEFT JOIN `{{DS}}.v_symbol_sector` AS s ON s.symbol = UPPER(TRIM(e.symbol))
  WHERE e.in_engine_cohort AND e.excess_pp IS NOT NULL
    AND UPPER(IFNULL(e.entry_recommendation, '')) IN ('BUY', 'STRONG BUY', 'ACCUMULATE')
),
g AS (
  SELECT mg AS market_group, sector, horizon, symbol, recorded_date, edge, fc, realized FROM c
  UNION ALL
  SELECT 'ALL' AS market_group, sector, horizon, symbol, recorded_date, edge, fc, realized FROM c
),
a AS (
  SELECT
    market_group,
    sector,
    horizon,
    COUNT(*) AS calls,
    COUNT(DISTINCT symbol) AS names,
    COUNT(DISTINCT recorded_date) AS days,
    MIN(recorded_date) AS first_day,
    MAX(recorded_date) AS last_day,
    AVG(edge) AS m_edge,
    SAFE_DIVIDE(COUNTIF(edge > 0), COUNT(*)) AS ahead,
    AVG(fc) AS m_fc,
    AVG(realized) AS m_real
  FROM g
  GROUP BY market_group, sector, horizon
)
SELECT
  market_group,
  sector,
  horizon,
  calls,
  names,
  days,
  first_day,
  last_day,
  ROUND(m_edge, 2) AS edge_pp,
  ROUND(100 * ahead, 1) AS right_pct,
  ROUND(m_fc, 2) AS avg_forecast_pp,
  ROUND(m_real, 2) AS avg_realized_pp,
  CASE
    WHEN calls < 100 THEN 'too few calls'
    WHEN m_edge >= 0.5 AND ahead >= 0.55 THEN 'beats its market'
    WHEN m_edge <= -0.5 AND ahead <= 0.45 THEN 'loses to its market'
    ELSE 'no clear edge'
  END AS reading
FROM a;
