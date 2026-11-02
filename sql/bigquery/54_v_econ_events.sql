-- v_econ_events: the economic calendar the news gate keeps (tab
-- _Econ_Events), one row per event Key - the copy from the latest snapshot
-- that holds it, so released figures stay after the tab moves on. event_at
-- is the release time (UTC); estimate / actual / previous stay as written
-- (units vary by event); surprise_pct is the gate's own figure.
CREATE OR REPLACE VIEW `{{DS}}.v_econ_events`
OPTIONS (description = "TFB Looker v2: economic calendar - one row per event, with estimate, actual and surprise")
AS
WITH snaps AS (
  SELECT snapshot_date, captured_at, row_num, cells
  FROM `{{DS}}.econ_events`
),
hdr AS (
  SELECT snapshot_date, cells AS h
  FROM snaps
  WHERE row_num = 1
),
typed AS (
  SELECT
    s.snapshot_date,
    s.captured_at,
    `{{DS}}.f_cell`(h, s.cells, 'Key') AS event_key,
    `{{DS}}.f_ts_utc`(`{{DS}}.f_cell`(h, s.cells, 'Date (UTC)')) AS event_at,
    `{{DS}}.f_cell`(h, s.cells, 'Country') AS country,
    `{{DS}}.f_cell`(h, s.cells, 'Event') AS event,
    `{{DS}}.f_cell`(h, s.cells, 'Comparison') AS comparison,
    `{{DS}}.f_cell`(h, s.cells, 'Period') AS period,
    `{{DS}}.f_cell`(h, s.cells, 'Importance') AS importance,
    `{{DS}}.f_cell`(h, s.cells, 'Estimate') AS estimate,
    `{{DS}}.f_cell`(h, s.cells, 'Actual') AS actual,
    `{{DS}}.f_cell`(h, s.cells, 'Previous') AS previous,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Surprise')) AS surprise,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Surprise %')) AS surprise_pct,
    `{{DS}}.f_ts`(`{{DS}}.f_cell`(h, s.cells, 'Updated At (Riyadh)')) AS updated_at
  FROM snaps AS s
  JOIN hdr USING (snapshot_date)
  WHERE s.row_num > 1
)
SELECT
  t.*,
  DATE(t.event_at, 'Asia/Riyadh') AS event_date,
  t.actual IS NOT NULL AS is_released
FROM typed AS t
WHERE t.event_key IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.event_key ORDER BY t.snapshot_date DESC) = 1;
