-- v_run_log: every _Run_Log entry (system health: syncs, guards, renders,
-- releases), each distinct row once across all snapshots, so entries the
-- sheet trims later stay here. message_tag / message_version come from the
-- leading "[TAG vX.Y.Z]" of the message when present.
CREATE OR REPLACE VIEW `{{DS}}.v_run_log`
OPTIONS (description = "TFB Looker v1: _Run_Log entries (system health), each distinct row once across snapshots")
AS
WITH snaps AS (
  SELECT snapshot_date, row_num, cells
  FROM `{{DS}}.run_log`
),
hdr AS (
  SELECT snapshot_date, cells AS h
  FROM snaps
  WHERE row_num = 1
),
x AS (
  SELECT
    s.snapshot_date,
    s.row_num,
    `{{DS}}.f_ts`(`{{DS}}.f_cell`(h, s.cells, 'Timestamp')) AS logged_at,
    UPPER(`{{DS}}.f_cell`(h, s.cells, 'Level')) AS level,
    `{{DS}}.f_cell`(h, s.cells, 'Action') AS action,
    `{{DS}}.f_cell`(h, s.cells, 'Page') AS page,
    `{{DS}}.f_cell`(h, s.cells, 'Status') AS status,
    `{{DS}}.f_cell`(h, s.cells, 'Message') AS message,
    `{{DS}}.f_cell`(h, s.cells, 'Endpoint') AS endpoint,
    SAFE_CAST(`{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'HTTP Code')) AS INT64) AS http_code,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Duration ms')) AS duration_ms,
    FARM_FINGERPRINT(ARRAY_TO_STRING(s.cells, CHR(31))) AS row_fingerprint
  FROM snaps AS s
  JOIN hdr USING (snapshot_date)
  WHERE s.row_num > 1
)
SELECT
  x.* EXCEPT (row_fingerprint, row_num),
  DATE(logged_at, 'Asia/Riyadh') AS log_date,
  DATE_TRUNC(DATE(logged_at, 'Asia/Riyadh'), WEEK(SUNDAY)) AS log_week,
  EXTRACT(HOUR FROM logged_at AT TIME ZONE 'Asia/Riyadh') AS log_hour_riyadh,
  REGEXP_EXTRACT(message, r'^\[([A-Za-z0-9_\-]+(?: [A-Za-z0-9_\-]+)*?)(?: v\d[\d.]*)?\]') AS message_tag,
  REGEXP_EXTRACT(message, r'^\[[^\]]*? v(\d[\d.]*)\]') AS message_version,
  level IN ('ERROR', 'CRITICAL', 'FATAL') AS is_error,
  level IN ('WARN', 'WARNING') AS is_warning
FROM x
WHERE logged_at IS NOT NULL OR message IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY row_fingerprint ORDER BY snapshot_date DESC, row_num) = 1;
