-- v_selection_log: every seat the Top-10 board logged (_Selection_Log since
-- 20 Jun 2026), the latest archived version of each (Logged At, Source Page,
-- Rank, Symbol) across all snapshots. ROI columns are percent points as the
-- board prints them; SAR columns are the ticket plan in SAR. board_output and
-- board_status are parsed from the run line ("status: ok | output: WITHHELD").
CREATE OR REPLACE VIEW `{{DS}}.v_selection_log`
OPTIONS (description = "TFB Looker v1: Top-10 seats logged by every board run (_Selection_Log), latest version per seat row")
AS
WITH snaps AS (
  SELECT snapshot_date, row_num, cells
  FROM `{{DS}}.selection_log`
),
hdr AS (
  SELECT snapshot_date, cells AS h
  FROM snaps
  WHERE row_num = 1
),
r AS (
  SELECT s.snapshot_date, s.row_num, s.cells AS c, hdr.h
  FROM snaps AS s
  JOIN hdr USING (snapshot_date)
  WHERE s.row_num > 1
),
x AS (
  SELECT
    snapshot_date,
    `{{DS}}.f_cell`(h, c, 'Logged At') AS logged_at_text,
    `{{DS}}.f_ts`(`{{DS}}.f_cell`(h, c, 'Logged At')) AS logged_at,
    `{{DS}}.f_cell`(h, c, 'Run Info') AS run_info,
    `{{DS}}.f_cell`(h, c, 'Source Page') AS source_page,
    SAFE_CAST(`{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Rank')) AS INT64) AS seat_rank,
    `{{DS}}.f_cell`(h, c, 'Symbol') AS symbol,
    `{{DS}}.f_cell`(h, c, 'Name') AS name,
    `{{DS}}.f_cell`(h, c, 'Market') AS market,
    `{{DS}}.f_cell`(h, c, 'Sector') AS sector,
    `{{DS}}.f_cell`(h, c, 'Ccy') AS ccy,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'FX→SAR')) AS fx_to_sar,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Price')) AS price,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Price SAR')) AS price_sar,
    `{{DS}}.f_cell`(h, c, 'Entry Zone') AS entry_zone,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Ticket SAR')) AS ticket_sar,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Shares')) AS shares,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Stop SAR')) AS stop_sar,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'TP1 SAR')) AS tp1_sar,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'TP2 SAR')) AS tp2_sar,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'ROI %')) AS roi_tp1_pct,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Engine ROI %')) AS engine_roi_pct,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Ann ROI %')) AS ann_roi_pct,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Gain 12M SAR')) AS gain_12m_sar,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Rel')) AS reliability,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'DQ')) AS data_quality,
    `{{DS}}.f_cell`(h, c, 'Conf') AS confidence,
    `{{DS}}.f_cell`(h, c, 'Funds From') AS funds_from,
    SAFE.PARSE_DATE('%Y-%m-%d', `{{DS}}.f_cell`(h, c, 'Review By')) AS review_by,
    `{{DS}}.f_cell`(h, c, 'Advisor Note') AS advisor_note,
    `{{DS}}.f_cell`(h, c, 'Outcome') AS outcome,
    `{{DS}}.f_cell`(h, c, 'Review Notes') AS review_notes,
    `{{DS}}.f_cell`(h, c, 'Stability') AS stability,
    SAFE_CAST(`{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Days')) AS INT64) AS days_on_board
  FROM r
),
latest AS (
  SELECT *
  FROM x
  WHERE symbol IS NOT NULL AND logged_at_text IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY logged_at_text, IFNULL(source_page, ''), IFNULL(CAST(seat_rank AS STRING), ''), symbol
    ORDER BY snapshot_date DESC) = 1
)
SELECT
  latest.* EXCEPT (logged_at_text),
  DATE(logged_at, 'Asia/Riyadh') AS logged_date,
  DATE_TRUNC(DATE(logged_at, 'Asia/Riyadh'), WEEK(SUNDAY)) AS logged_week,
  DATE_TRUNC(DATE(logged_at, 'Asia/Riyadh'), MONTH) AS logged_month,
  REGEXP_EXTRACT(run_info, r'output:\s*([A-Z_]+)') AS board_output,
  REGEXP_EXTRACT(run_info, r'status:\s*([A-Za-z_]+)') AS board_status,
  REGEXP_EXTRACT(stability, r'^([A-Z][A-Z\-]+)') AS stability_state,
  REGEXP_EXTRACT(advisor_note, r'^([A-Z][A-Z \-]+?)\s+—') AS advisor_verdict
FROM latest;
