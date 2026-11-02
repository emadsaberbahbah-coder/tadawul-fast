-- v_outcome_calibration: what the forecasts actually delivered, sliced by the
-- way they were issued - confidence, reliability tier, data-quality tier,
-- sector, market, horizon, origin tab, risk bucket, recommendation tier and
-- recording week - plus one ALL row. WHY (owner, 2 Nov 2026): learning must
-- show up the next morning as "High confidence delivered X %, Medium Y %",
-- not as a single rate; a band whose stated confidence is not borne out is
-- flagged, so the next change is aimed where the system is overconfident.
-- Every rate is taken over the engine cohort of v_outcome_engine (the first
-- engine-scored record per Key): records and due count every record of the
-- slice; engine_scored / engine_wins / engine_win_rate_pct, the average and
-- median realized ROI, roi_capture_pct (realized / target, both averaged;
-- NULL when the average target is under 1 point, a ratio of nothing),
-- the average best and worst excursions, touched_target_pct and the sheet's
-- own tracker counterpart (sheet_scored / sheet_wins / sheet_win_rate_pct over
-- the sheet's outcome cohort) are cohort figures. calibration_flag:
-- 'small sample' under 30 scored calls; on the confidence rows 'High
-- confidence under-delivers' when the HIGH band's win rate sits below the
-- MEDIUM or LOW band's. Percent points. Research view; the live system never
-- reads BigQuery.
CREATE OR REPLACE VIEW `{{DS}}.v_outcome_calibration`
OPTIONS (description = "TFB Looker v5: engine win rate, realized ROI, ROI capture, excursions and the sheet's own rate by confidence, reliability tier, data-quality tier, sector, market, horizon, origin, risk, recommendation and week, with a calibration flag")
AS
WITH e AS (
  SELECT
    e.*,
    CASE
      WHEN e.entry_forecast_reliability IS NULL THEN 'unknown'
      WHEN e.entry_forecast_reliability < 70 THEN '< 70'
      WHEN e.entry_forecast_reliability < 80 THEN '70-79'
      WHEN e.entry_forecast_reliability < 90 THEN '80-89'
      ELSE '90+'
    END AS reliability_tier,
    CASE
      WHEN e.entry_data_quality IS NULL THEN 'unknown'
      WHEN e.entry_data_quality < 70 THEN '< 70'
      WHEN e.entry_data_quality < 80 THEN '70-79'
      WHEN e.entry_data_quality < 90 THEN '80-89'
      ELSE '90+'
    END AS data_quality_tier
  FROM `{{DS}}.v_outcome_engine` AS e
),
sliced AS (
  SELECT
    d.dimension,
    d.value,
    e.* EXCEPT (reliability_tier, data_quality_tier)
  FROM e
  CROSS JOIN UNNEST([
    STRUCT('ALL' AS dimension, 'ALL' AS value),
    STRUCT('confidence', IFNULL(e.confidence, 'unknown')),
    STRUCT('reliability_tier', e.reliability_tier),
    STRUCT('data_quality_tier', e.data_quality_tier),
    STRUCT('sector', IFNULL(e.sector, 'unknown')),
    STRUCT('market', IFNULL(e.market, 'Other')),
    STRUCT('horizon', IFNULL(e.horizon, 'unknown')),
    STRUCT('origin_tab', IFNULL(e.origin_tab, 'unknown')),
    STRUCT('risk_bucket', IFNULL(e.risk_bucket, 'unknown')),
    STRUCT('entry_recommendation', IFNULL(e.entry_recommendation, 'unknown')),
    STRUCT('recorded_week', IFNULL(CAST(e.recorded_week AS STRING), 'unknown'))
  ]) AS d
),
g AS (
  SELECT
    dimension,
    value,
    COUNT(*) AS records,
    COUNTIF(target_session_date <= CURRENT_DATE('Asia/Riyadh')) AS due,
    COUNTIF(in_engine_cohort) AS engine_scored,
    SUM(IF(in_engine_cohort, IFNULL(engine_win_flag, 0), 0)) AS engine_wins,
    ROUND(AVG(IF(in_engine_cohort, engine_roi_pp, NULL)), 4) AS avg_engine_roi_pp,
    ROUND(APPROX_QUANTILES(IF(in_engine_cohort, engine_roi_pp, NULL), 100 IGNORE NULLS)[SAFE_OFFSET(50)], 4) AS median_engine_roi_pp,
    ROUND(AVG(IF(in_engine_cohort, target_roi_pp, NULL)), 4) AS avg_target_roi_pp,
    ROUND(AVG(IF(in_engine_cohort, mfe_pct, NULL)), 4) AS avg_mfe_pct,
    ROUND(AVG(IF(in_engine_cohort, mae_pct, NULL)), 4) AS avg_mae_pct,
    COUNTIF(in_engine_cohort AND touched_target) AS touched_target,
    COUNTIF(in_outcome_cohort AND sheet_scored) AS sheet_scored,
    SUM(IF(in_outcome_cohort, IFNULL(sheet_win_flag, 0), 0)) AS sheet_wins
  FROM sliced
  GROUP BY dimension, value
),
rated AS (
  SELECT
    g.*,
    ROUND(SAFE_DIVIDE(g.engine_wins, g.engine_scored) * 100, 1) AS engine_win_rate_pct,
    IF(ABS(g.avg_target_roi_pp) >= 1, ROUND(SAFE_DIVIDE(g.avg_engine_roi_pp, g.avg_target_roi_pp) * 100, 1), NULL) AS roi_capture_pct,
    ROUND(SAFE_DIVIDE(g.touched_target, g.engine_scored) * 100, 1) AS touched_target_pct,
    ROUND(SAFE_DIVIDE(g.sheet_wins, g.sheet_scored) * 100, 1) AS sheet_win_rate_pct
  FROM g
),
flagged AS (
  SELECT
    r.*,
    MAX(IF(r.dimension = 'confidence' AND r.value = 'HIGH', r.engine_win_rate_pct, NULL)) OVER () AS high_rate,
    MAX(IF(r.dimension = 'confidence' AND r.value = 'MEDIUM', r.engine_win_rate_pct, NULL)) OVER () AS medium_rate,
    MAX(IF(r.dimension = 'confidence' AND r.value = 'LOW', r.engine_win_rate_pct, NULL)) OVER () AS low_rate
  FROM rated AS r
)
SELECT
  f.* EXCEPT (high_rate, medium_rate, low_rate),
  CASE
    WHEN f.engine_scored < 30 THEN 'small sample'
    WHEN f.dimension = 'confidence' AND f.value = 'HIGH'
         AND (f.high_rate < f.medium_rate OR f.high_rate < f.low_rate) THEN 'High confidence under-delivers'
    ELSE NULL
  END AS calibration_flag,
  "archived daily close at the first session on/after the target - research view, not the sheet's Directional Win Rate" AS basis
FROM flagged AS f;
