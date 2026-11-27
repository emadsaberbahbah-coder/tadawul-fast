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
-- at an average edge of +0.5 points or more with at least 55% of calls ahead
-- AND t_adj of 2 or more; 'loses to its market' at -0.5 points or worse with
-- at most 45% ahead AND t_adj of -2 or less; otherwise 'no clear edge'. The
-- same 100-call floor as the calls-against-the-market panel.
-- t_adj (views 1.35.0, full review of 27 Nov 2026): the mean over distinct
-- recording dates - a date's calls averaged first - over its standard error,
-- divided by SQRT of the horizon in days (7 / 14 / 30 / 91), as
-- v_forward_signal_gates does: neighbouring dates share most of their
-- holding window, and a name recorded on many days is one bet, not many.
-- WHY: without it 15 cells read 'beats its market' on the Learning Watch page
-- as PROVEN, on overlapping calls and with no test at all.
-- COLUMNS: market_group (US / Tadawul / Other markets / ALL), sector
-- ('(no sector)' when v_symbol_sector has none), horizon, calls, names, days,
-- first_day, last_day, edge_pp, right_pct, avg_forecast_pp, avg_realized_pp,
-- t_adj, reading.
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_outcome_sector`
OPTIONS (description = "TFB buy calls by market group, GICS sector and horizon against the market benchmark - calls, names, average edge, share ahead, forecast against realized, an overlap-adjusted t over recording dates, and a fixed reading (at least 100 calls, t of 2 or more either way)")
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
),
dd AS (
  SELECT market_group, sector, horizon, recorded_date, AVG(edge) AS m
  FROM g
  GROUP BY market_group, sector, horizon, recorded_date
),
tt AS (
  SELECT
    market_group,
    sector,
    horizon,
    ROUND(SAFE_DIVIDE(AVG(m), SAFE_DIVIDE(STDDEV(m), SQRT(COUNT(*))))
          / SQRT(CASE horizon WHEN '1W' THEN 7 WHEN '2W' THEN 14 WHEN '1M' THEN 30 ELSE 91 END), 2) AS t_adj
  FROM dd
  GROUP BY market_group, sector, horizon
)
SELECT
  a.market_group,
  a.sector,
  a.horizon,
  a.calls,
  a.names,
  a.days,
  a.first_day,
  a.last_day,
  ROUND(a.m_edge, 2) AS edge_pp,
  ROUND(100 * a.ahead, 1) AS right_pct,
  ROUND(a.m_fc, 2) AS avg_forecast_pp,
  ROUND(a.m_real, 2) AS avg_realized_pp,
  tt.t_adj,
  CASE
    WHEN a.calls < 100 THEN 'too few calls'
    WHEN a.m_edge >= 0.5 AND a.ahead >= 0.55 AND tt.t_adj >= 2 THEN 'beats its market'
    WHEN a.m_edge <= -0.5 AND a.ahead <= 0.45 AND tt.t_adj <= -2 THEN 'loses to its market'
    ELSE 'no clear edge'
  END AS reading
FROM a
JOIN tt USING (market_group, sector, horizon);
