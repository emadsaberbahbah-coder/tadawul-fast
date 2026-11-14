-- v_signal_edge_daily: does each live signal put the names that later did
-- best at the top - tested one recording day at a time. WHY (owner, 8 Nov
-- 2026: "make full review for the Project and need to improve the outcome
-- and analysis"): the October review (71 first seats) and the 8 Nov archive
-- study (8,230 symbol-days, 28 Jun - 8 Nov) found that the engine's ROI
-- forecast ranks the next weeks backwards at the top (the highest fifth
-- trails the middle), that forecast reliability carries no edge, and that the
-- engine's overall_score is the one signal with a stable positive rank
-- correlation with the next week (US names especially). Those were one-off
-- queries. This view keeps the same test running on every archived day, so
-- an owner rule, a ranking challenger or a fundable-currency choice reads
-- evidence that is current instead of a snapshot.
-- RULE: one row per symbol and recording day from v_outcome_engine, taken
-- from the day's FIRST recording time (a symbol recorded twice in a day
-- keeps its first forecast, so a later re-record cannot borrow the day's
-- move). Features: the engine's forecast ROI of each horizon (target_roi_pp
-- of the 1W / 2W / 1M / 3M record), the entry fields (forecast reliability,
-- data quality, confidence, recommendation, investability, stance; the
-- highest value when the horizons disagree) and, from v_signal_history,
-- overall_score, risk_score and target_upside_pp of the forecast's OWN
-- observation: track_performance appends the forecast first and then writes
-- that run's Signal_History snapshot from the same prefetched rows, seconds
-- later (8 Nov: 8,230 of 8,230 symbol-days have one within 60 s, median 2 s;
-- the next run's snapshot comes 68 s or more after the forecast). So the
-- join takes the EARLIEST signal recorded 0-60 s after the forecast, and only
-- when a symbol has none, the newest signal recorded before the forecast and
-- at most 3 days older. Nothing from a later run is read (Codex P2 on #932:
-- a strict "at or before" cut read the previous run for 5,995 symbol-days
-- and no signal at all for 2,235).
-- Outcomes: the realized ladder roi_1w_pp / roi_2w_pp / roi_1m_pp /
-- roi_3m_pp (the close of the first archived bar 7 / 14 / 30 / 91 days after
-- the recording, against the entry price; NULL until reached). A realized
-- return beyond +/-300 % is a data error and is left out.
-- For each signal day, market group (ALL, US, Tadawul, Other markets),
-- feature and horizon with at least 20 names and two distinct feature values:
--   n             names with both the feature and the realized return
--   ic            Spearman rank correlation (average ranks for ties) between
--                 the feature and the realized return; > 0 = higher values
--                 did better
--   top_ex_pp     mean realized return of the feature's top third minus the
--                 group's mean that day (percent points)
--   bottom_ex_pp  the same for the bottom third
--   spread_pp     top_ex_pp - bottom_ex_pp
--   horizon_days  calendar days of the ladder step (7 / 14 / 30 / 91), the
--                 overlap v_signal_edge corrects for
-- Values are rank-tied when equal to 4 decimals (BigQuery cannot partition a
-- window by FLOAT64, so ties are keyed on the value x 10,000 as INT64).
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_signal_edge_daily`
OPTIONS (description = "TFB Looker v6: per signal day, market group, feature and horizon - the Spearman rank IC between each live signal (engine ROI forecasts, reliability, data quality, confidence, recommendation, investability, stance, overall/risk score, target upside) and the realized 1W/2W/1M/3M return, with top/bottom-third excess returns")
AS
WITH first_rec AS (
  SELECT symbol, recorded_date, MIN(recorded_at) AS first_at
  FROM `{{DS}}.v_outcome_engine`
  GROUP BY symbol, recorded_date
),
rec AS (
  SELECT
    e.symbol,
    e.recorded_date AS d,
    MIN(e.recorded_at) AS rec_at,
    MIN(e.market) AS market,
    CAST(MAX(IF(e.horizon = '1W', e.target_roi_pp, NULL)) AS FLOAT64) AS engine_roi_1w,
    CAST(MAX(IF(e.horizon = '2W', e.target_roi_pp, NULL)) AS FLOAT64) AS engine_roi_2w,
    CAST(MAX(IF(e.horizon = '1M', e.target_roi_pp, NULL)) AS FLOAT64) AS engine_roi_1m,
    CAST(MAX(IF(e.horizon = '3M', e.target_roi_pp, NULL)) AS FLOAT64) AS engine_roi_3m,
    CAST(MAX(e.entry_forecast_reliability) AS FLOAT64) AS reliability,
    CAST(MAX(e.entry_data_quality) AS FLOAT64) AS data_quality,
    CAST(MAX(CASE UPPER(TRIM(e.confidence)) WHEN 'HIGH' THEN 3 WHEN 'MEDIUM' THEN 2 WHEN 'LOW' THEN 1 END) AS FLOAT64) AS confidence,
    CAST(MAX(CASE UPPER(TRIM(e.entry_recommendation))
      WHEN 'STRONG BUY' THEN 5 WHEN 'BUY' THEN 4 WHEN 'ACCUMULATE' THEN 3 WHEN 'HOLD' THEN 2
      WHEN 'REDUCE' THEN 1 WHEN 'SELL' THEN 0 WHEN 'STRONG SELL' THEN 0 WHEN 'AVOID' THEN 0 END) AS FLOAT64) AS recommendation,
    CAST(MAX(CASE UPPER(TRIM(e.entry_investability)) WHEN 'INVESTABLE' THEN 2 WHEN 'WATCHLIST' THEN 1 WHEN 'BLOCKED' THEN 0 END) AS FLOAT64) AS investability,
    CAST(MAX(CASE LOWER(TRIM(e.entry_stance)) WHEN 'bullish' THEN 1 WHEN 'neutral' THEN 0 WHEN 'bearish' THEN -1 END) AS FLOAT64) AS stance,
    MAX(e.roi_1w_pp) AS r_1w,
    MAX(e.roi_2w_pp) AS r_2w,
    MAX(e.roi_1m_pp) AS r_1m,
    MAX(e.roi_3m_pp) AS r_3m
  FROM `{{DS}}.v_outcome_engine` AS e
  JOIN first_rec AS f
    ON f.symbol = e.symbol AND f.recorded_date = e.recorded_date AND f.first_at = e.recorded_at
  GROUP BY e.symbol, e.recorded_date
),
sig AS (
  SELECT r.symbol, r.d, s.overall_score, s.risk_score, s.target_upside_pp
  FROM rec AS r
  JOIN `{{DS}}.v_signal_history` AS s
    ON s.symbol = r.symbol
   AND s.recorded_at <= TIMESTAMP_ADD(r.rec_at, INTERVAL 60 SECOND)
   AND s.recorded_at >= TIMESTAMP_SUB(r.rec_at, INTERVAL 3 DAY)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY r.symbol, r.d
    ORDER BY IF(s.recorded_at >= r.rec_at, 0, 1),
      IF(s.recorded_at >= r.rec_at, UNIX_MICROS(s.recorded_at), -UNIX_MICROS(s.recorded_at))) = 1
),
j AS (
  SELECT
    r.*,
    CAST(s.overall_score AS FLOAT64) AS overall_score,
    CAST(s.risk_score AS FLOAT64) AS risk_score,
    CAST(s.target_upside_pp AS FLOAT64) AS target_upside
  FROM rec AS r
  LEFT JOIN sig AS s ON s.symbol = r.symbol AND s.d = r.d
),
grp AS (
  SELECT 'ALL' AS market_group, j.* FROM j
  UNION ALL
  SELECT IF(j.market IN ('US', 'Tadawul'), j.market, 'Other markets') AS market_group, j.* FROM j
),
feat AS (
  SELECT d, market_group, symbol, feature, x, r_1w, r_2w, r_1m, r_3m
  FROM grp
  UNPIVOT (x FOR feature IN (engine_roi_1w, engine_roi_2w, engine_roi_1m, engine_roi_3m, reliability,
    data_quality, confidence, recommendation, investability, stance, overall_score, risk_score, target_upside))
),
lng AS (
  SELECT
    d, market_group, feature, x, horizon, r,
    CAST(ROUND(x * 10000) AS INT64) AS xk,
    CAST(ROUND(r * 10000) AS INT64) AS rk
  FROM feat
  UNPIVOT (r FOR horizon IN (r_1w AS '1W', r_2w AS '2W', r_1m AS '1M', r_3m AS '3M'))
  WHERE ABS(r) <= 300
),
ranked AS (
  SELECT
    d, market_group, feature, horizon, x, r,
    RANK() OVER (PARTITION BY d, market_group, feature, horizon ORDER BY xk)
      + (COUNT(*) OVER (PARTITION BY d, market_group, feature, horizon, xk) - 1) / 2 AS rx,
    RANK() OVER (PARTITION BY d, market_group, feature, horizon ORDER BY rk)
      + (COUNT(*) OVER (PARTITION BY d, market_group, feature, horizon, rk) - 1) / 2 AS rr,
    COUNT(*) OVER (PARTITION BY d, market_group, feature, horizon) AS n,
    COUNT(DISTINCT xk) OVER (PARTITION BY d, market_group, feature, horizon) AS nx,
    AVG(r) OVER (PARTITION BY d, market_group, feature, horizon) AS r_mean
  FROM lng
),
daily AS (
  SELECT
    d AS signal_day,
    market_group,
    feature,
    horizon,
    MIN(n) AS n,
    CORR(rx, rr) AS ic,
    AVG(IF(rx > 2 * n / 3, r - r_mean, NULL)) AS top_ex,
    AVG(IF(rx <= n / 3, r - r_mean, NULL)) AS bottom_ex
  FROM ranked
  WHERE n >= 20 AND nx >= 2
  GROUP BY d, market_group, feature, horizon
)
SELECT
  signal_day,
  market_group,
  feature,
  horizon,
  CASE horizon WHEN '1W' THEN 7 WHEN '2W' THEN 14 WHEN '1M' THEN 30 WHEN '3M' THEN 91 END AS horizon_days,
  n,
  ROUND(ic, 4) AS ic,
  ROUND(top_ex, 4) AS top_ex_pp,
  ROUND(bottom_ex, 4) AS bottom_ex_pp,
  ROUND(top_ex - bottom_ex, 4) AS spread_pp
FROM daily;
