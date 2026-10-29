-- v_performance_log: every forecast record of Performance_Log, the LATEST
-- archived version of each Record ID across all snapshots (a record moves
-- from active to matured / expired, and a row trimmed from the sheet later
-- stays here). Unit contract: realized / unrealized ROI are percent points
-- (they equal (price / entry - 1) x 100 on the sheet); the sheet's own
-- "Target ROI %" mixes fractions and percent points (only part of October
-- declares roi_unit=percent_points), so target_roi_pp is recomputed from the
-- two prices and target_roi_sheet keeps the raw cell for reference.
CREATE OR REPLACE VIEW `{{DS}}.v_performance_log`
OPTIONS (description = "TFB Looker v1: Performance_Log forecast records, latest version per Record ID (percent points)")
AS
WITH snaps AS (
  SELECT snapshot_date, row_num, cells
  FROM `{{DS}}.performance_log`
),
hdr AS (
  SELECT snapshot_date, row_num AS hrow, cells AS h
  FROM snaps
  WHERE 'Record ID' IN UNNEST(cells) AND 'Symbol' IN UNNEST(cells)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY snapshot_date ORDER BY row_num) = 1
),
r AS (
  SELECT s.snapshot_date, s.cells AS c, hdr.h
  FROM snaps AS s
  JOIN hdr USING (snapshot_date)
  WHERE s.row_num > hdr.hrow
),
x AS (
  SELECT
    snapshot_date,
    `{{DS}}.f_cell`(h, c, 'Record ID') AS record_id,
    `{{DS}}.f_cell`(h, c, 'Key') AS record_key,
    `{{DS}}.f_cell`(h, c, 'Symbol') AS symbol,
    `{{DS}}.f_cell`(h, c, 'Horizon') AS horizon,
    `{{DS}}.f_ts`(`{{DS}}.f_cell`(h, c, 'Date Recorded (Riyadh)')) AS recorded_at,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Entry Price')) AS entry_price,
    `{{DS}}.f_cell`(h, c, 'Entry Recommendation') AS entry_recommendation,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Entry Score')) AS entry_score,
    `{{DS}}.f_cell`(h, c, 'Risk Bucket') AS risk_bucket,
    `{{DS}}.f_cell`(h, c, 'Confidence') AS confidence,
    `{{DS}}.f_cell`(h, c, 'Origin Tab') AS origin_tab,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Target Price')) AS target_price,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Target ROI %')) AS target_roi_sheet,
    `{{DS}}.f_ts`(`{{DS}}.f_cell`(h, c, 'Target Date (Riyadh)')) AS target_at,
    `{{DS}}.f_cell`(h, c, 'Status') AS status,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Current Price')) AS current_price,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Unrealized ROI %')) AS unrealized_roi_pp,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Realized ROI %')) AS realized_roi_pp,
    `{{DS}}.f_cell`(h, c, 'Outcome') AS outcome,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Volatility')) AS volatility,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Max Drawdown %')) AS max_drawdown_sheet,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Sharpe Ratio')) AS sharpe_ratio,
    `{{DS}}.f_cell`(h, c, 'Sector') AS sector,
    `{{DS}}.f_ts`(`{{DS}}.f_cell`(h, c, 'Last Updated (Riyadh)')) AS last_updated_at,
    `{{DS}}.f_ts`(`{{DS}}.f_cell`(h, c, 'Maturity Date')) AS matured_at,
    `{{DS}}.f_cell`(h, c, 'Notes') AS notes,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Entry Data Quality')) AS entry_data_quality,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Entry Forecast Reliability')) AS entry_forecast_reliability,
    `{{DS}}.f_cell`(h, c, 'Entry Investability') AS entry_investability,
    `{{DS}}.f_cell`(h, c, 'Entry Final Action') AS entry_final_action,
    `{{DS}}.f_cell`(h, c, 'Entry Selected') AS entry_selected
  FROM r
),
latest AS (
  SELECT *
  FROM x
  WHERE record_id IS NOT NULL AND symbol IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY snapshot_date DESC) = 1
)
SELECT
  latest.*,
  DATE(recorded_at, 'Asia/Riyadh') AS recorded_date,
  DATE_TRUNC(DATE(recorded_at, 'Asia/Riyadh'), WEEK(SUNDAY)) AS recorded_week,
  DATE_TRUNC(DATE(recorded_at, 'Asia/Riyadh'), MONTH) AS recorded_month,
  ROUND((SAFE_DIVIDE(target_price, entry_price) - 1) * 100, 4) AS target_roi_pp,
  CASE
    WHEN REGEXP_CONTAINS(IFNULL(notes, ''), r'roi_unit"\s*:\s*"percent_points') THEN 'percent_points (declared)'
    WHEN target_roi_sheet IS NULL THEN NULL
    WHEN ABS(target_roi_sheet * 100 - (SAFE_DIVIDE(target_price, entry_price) - 1) * 100) < 0.05 THEN 'fraction'
    WHEN ABS(target_roi_sheet - (SAFE_DIVIDE(target_price, entry_price) - 1) * 100) < 0.05 THEN 'percent_points'
    ELSE 'inconsistent'
  END AS target_roi_sheet_unit,
  status = 'matured' AS is_matured,
  outcome IN ('WIN', 'LOSS', 'BREAKEVEN') AS is_scored,
  CASE WHEN outcome = 'WIN' THEN 1 WHEN outcome IN ('LOSS', 'BREAKEVEN') THEN 0 END AS win_flag,
  CASE
    WHEN entry_recommendation IN ('BUY', 'STRONG BUY', 'ACCUMULATE') THEN 'bullish'
    WHEN entry_recommendation IN ('SELL', 'REDUCE', 'AVOID') THEN 'bearish'
    WHEN entry_recommendation IS NULL THEN NULL
    ELSE 'neutral'
  END AS entry_stance
FROM latest;
