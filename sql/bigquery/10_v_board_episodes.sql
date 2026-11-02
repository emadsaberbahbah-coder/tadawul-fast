-- v_board_episodes: one row per Top-10 seat episode from the latest
-- _Board_Scorecard snapshot (the nightly scorecard recomputes every episode,
-- so the latest copy is complete). Returns are percent points from the
-- board's printed seat price to the k-th later session close of the venue;
-- "vs" columns are percentage-point differences against SPUS and against
-- the sector reference (Ref).
CREATE OR REPLACE VIEW `{{DS}}.v_board_episodes`
OPTIONS (description = "TFB Looker v1: Top-10 seat episodes and their outcomes (latest _Board_Scorecard snapshot, percent points)")
AS
WITH snap AS (
  SELECT snapshot_date, row_num, cells
  FROM `{{DS}}.board_scorecard`
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM `{{DS}}.board_scorecard`)
),
hdr AS (
  SELECT row_num AS hrow, cells AS h
  FROM snap
  WHERE 'Seat Date' IN UNNEST(cells) AND 'Symbol' IN UNNEST(cells)
  QUALIFY ROW_NUMBER() OVER (ORDER BY row_num) = 1
),
r AS (
  SELECT s.snapshot_date, s.row_num, s.cells AS c, hdr.h
  FROM snap AS s
  CROSS JOIN hdr
  WHERE s.row_num > hdr.hrow
),
x AS (
  SELECT
    snapshot_date,
    row_num,
    SAFE.PARSE_DATE('%Y-%m-%d', `{{DS}}.f_cell`(h, c, 'Seat Date')) AS seat_date,
    `{{DS}}.f_cell`(h, c, 'Seat Time') AS seat_time,
    `{{DS}}.f_cell`(h, c, 'Symbol') AS symbol,
    `{{DS}}.f_cell`(h, c, 'Name') AS name,
    `{{DS}}.f_cell`(h, c, 'Sector') AS sector,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Seat Price')) AS seat_price,
    `{{DS}}.f_cell`(h, c, 'Ccy') AS ccy,
    `{{DS}}.f_cell`(h, c, 'Output at Seat') AS output_at_seat,
    `{{DS}}.f_cell`(h, c, 'Stability at Seat') AS stability_at_seat,
    SAFE.PARSE_DATE('%Y-%m-%d', `{{DS}}.f_cell`(h, c, 'Exit Date')) AS exit_date,
    `{{DS}}.f_cell`(h, c, 'Exit Kind') AS exit_kind,
    SAFE_CAST(`{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Days on Board')) AS INT64) AS days_on_board,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Hours on Board')) AS hours_on_board,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, '+1 %')) AS ret_plus1_pct,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, '+5 %')) AS ret_plus5_pct,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, '+10 %')) AS ret_plus10_pct,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, '+20 %')) AS ret_plus20_pct,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'To Exit %')) AS ret_to_exit_pct,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'To Now %')) AS ret_to_now_pct,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'vs SPUS +5 pp')) AS vs_spus_plus5_pp,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'vs SPUS Now pp')) AS vs_spus_now_pp,
    `{{DS}}.f_cell`(h, c, 'Status') AS status,
    SAFE_CAST(`{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Rank')) AS INT64) AS seat_rank,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Engine ROI %')) AS engine_roi_pct,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Rel')) AS reliability,
    `{{DS}}.f_cell`(h, c, 'Conf') AS confidence,
    `{{DS}}.f_cell`(h, c, 'Ref') AS sector_ref,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'vs Ref +5 pp')) AS vs_ref_plus5_pp,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'vs Ref Now pp')) AS vs_ref_now_pp
  FROM r
)
SELECT
  x.*,
  `{{DS}}.f_ts`(CONCAT(CAST(seat_date AS STRING), ' ', IFNULL(seat_time, '00:00'))) AS seat_at,
  DATE_TRUNC(seat_date, WEEK(SUNDAY)) AS seat_week,
  DATE_TRUNC(seat_date, MONTH) AS seat_month,
  CASE WHEN ret_plus5_pct IS NULL THEN NULL WHEN ret_plus5_pct > 0 THEN 1 ELSE 0 END AS win_plus5,
  CASE WHEN vs_spus_plus5_pp IS NULL THEN NULL WHEN vs_spus_plus5_pp > 0 THEN 1 ELSE 0 END AS beat_spus_plus5,
  CASE WHEN vs_ref_plus5_pp IS NULL THEN NULL WHEN vs_ref_plus5_pp > 0 THEN 1 ELSE 0 END AS beat_ref_plus5,
  REGEXP_EXTRACT(stability_at_seat, r'^([A-Z][A-Z\-]+)') AS stability_state
FROM x
WHERE seat_date IS NOT NULL AND symbol IS NOT NULL;
