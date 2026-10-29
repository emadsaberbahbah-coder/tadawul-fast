-- v_signal_history: every engine signal recorded in Signal_History, the
-- latest archived version of each Snapshot ID (one id per recorded row; the
-- sheet's Key is symbol|date and repeats when a symbol is recorded several
-- times a day). Scores are the engine's 0-100 scales; Price / Target Price
-- are in the instrument's own currency.
CREATE OR REPLACE VIEW `{{DS}}.v_signal_history`
OPTIONS (description = "TFB Looker v1: engine signals per symbol and recording (Signal_History), latest version per Snapshot ID")
AS
WITH snaps AS (
  SELECT snapshot_date, row_num, cells
  FROM `{{DS}}.signal_history`
),
hdr AS (
  SELECT snapshot_date, cells AS h
  FROM snaps
  WHERE row_num = 1
),
r AS (
  SELECT s.snapshot_date, s.cells AS c, hdr.h
  FROM snaps AS s
  JOIN hdr USING (snapshot_date)
  WHERE s.row_num > 1
),
x AS (
  SELECT
    snapshot_date,
    `{{DS}}.f_cell`(h, c, 'Key') AS signal_key,
    `{{DS}}.f_cell`(h, c, 'Snapshot ID') AS snapshot_id,
    SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(`{{DS}}.f_cell`(h, c, 'Date (Riyadh)'), 1, 10)) AS signal_date,
    `{{DS}}.f_ts`(`{{DS}}.f_cell`(h, c, 'Recorded At (Riyadh)')) AS recorded_at,
    `{{DS}}.f_cell`(h, c, 'Symbol') AS symbol,
    `{{DS}}.f_cell`(h, c, 'Origin Tab') AS origin_tab,
    `{{DS}}.f_cell`(h, c, 'Recommendation') AS recommendation,
    `{{DS}}.f_cell`(h, c, 'Final Action') AS final_action,
    `{{DS}}.f_cell`(h, c, 'Investability') AS investability,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Overall Score')) AS overall_score,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Risk Score')) AS risk_score,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Data Quality')) AS data_quality,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Forecast Reliability')) AS forecast_reliability,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Price')) AS price,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Target Price')) AS target_price,
    `{{DS}}.f_cell`(h, c, 'Analyst Rating') AS analyst_rating,
    SAFE_CAST(`{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Days To Earnings')) AS INT64) AS days_to_earnings,
    SAFE_CAST(`{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Days To ExDiv')) AS INT64) AS days_to_exdiv
  FROM r
)
SELECT
  x.*,
  DATE_TRUNC(signal_date, WEEK(SUNDAY)) AS signal_week,
  DATE_TRUNC(signal_date, MONTH) AS signal_month,
  ROUND((SAFE_DIVIDE(target_price, price) - 1) * 100, 4) AS target_upside_pp
FROM x
WHERE symbol IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY IFNULL(snapshot_id, CONCAT(symbol, '|', IFNULL(signal_key, ''), '|', IFNULL(CAST(recorded_at AS STRING), '')))
  ORDER BY snapshot_date DESC) = 1;
