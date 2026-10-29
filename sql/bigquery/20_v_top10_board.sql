-- v_top10_board: the Top-10 board as archived each night (one row per board
-- row and snapshot): the seated names (seat_rank 1..10) and the grace /
-- fast-track rows printed between them (seat_rank NULL). ROI columns are the
-- board's percent points; SAR columns are the ticket plan. board_* columns
-- come from the cockpit status line of the same snapshot. The main table
-- ends at the first row after its header without a symbol (blank rows are
-- not archived; the next section starts with a title row).
CREATE OR REPLACE VIEW `{{DS}}.v_top10_board`
OPTIONS (description = "TFB Looker v1: nightly Top-10 board rows per snapshot (Top_10_Investments main table)")
AS
WITH snaps AS (
  SELECT snapshot_date, captured_at, row_num, cells
  FROM `{{DS}}.top_10_investments`
),
hdr AS (
  SELECT snapshot_date, row_num AS hrow, cells AS h
  FROM snaps
  WHERE 'Rank' IN UNNEST(cells) AND 'Symbol' IN UNNEST(cells) AND 'Entry Zone' IN UNNEST(cells)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY snapshot_date ORDER BY row_num) = 1
),
status_line AS (
  SELECT snapshot_date, ANY_VALUE(captured_at) AS captured_at,
         STRING_AGG(IF(row_num = 2, ARRAY_TO_STRING(cells, ' '), NULL), ' ') AS status_text
  FROM snaps
  GROUP BY snapshot_date
),
body AS (
  SELECT s.snapshot_date, s.row_num, s.cells AS c, hdr.h, hdr.hrow
  FROM snaps AS s
  JOIN hdr USING (snapshot_date)
  WHERE s.row_num > hdr.hrow
),
table_end AS (
  SELECT snapshot_date, MIN(row_num) AS end_row
  FROM body
  WHERE `{{DS}}.f_cell`(h, c, 'Symbol') IS NULL
  GROUP BY snapshot_date
)
SELECT
  b.snapshot_date,
  st.captured_at,
  b.row_num - b.hrow AS board_line,
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
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'ROI % (TP1)')) AS roi_tp1_pct,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Engine ROI % (12M)')) AS engine_roi_12m_pct,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Ann ROI %')) AS ann_roi_pct,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Gain 12M SAR')) AS gain_12m_sar,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Rel')) AS reliability,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'DQ')) AS data_quality,
  `{{DS}}.f_cell`(h, c, 'Conf') AS confidence,
  `{{DS}}.f_cell`(h, c, 'Funds From') AS funds_from,
  SAFE.PARSE_DATE('%Y-%m-%d', `{{DS}}.f_cell`(h, c, 'Review By')) AS review_by,
  `{{DS}}.f_cell`(h, c, 'Stability') AS stability,
  REGEXP_EXTRACT(`{{DS}}.f_cell`(h, c, 'Stability'), r'^([A-Z][A-Z\-]+)') AS stability_state,
  SAFE_CAST(`{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Days')) AS INT64) AS days_on_board,
  SAFE.PARSE_DATE('%Y-%m-%d', `{{DS}}.f_cell`(h, c, 'Since')) AS seated_since,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Sm Score')) AS smoothed_score,
  `{{DS}}.f_cell`(h, c, 'Trend') AS trend,
  `{{DS}}.f_cell`(h, c, 'Advisor Note') AS advisor_note,
  REGEXP_EXTRACT(st.status_text, r'output:\s*([A-Z_]+)') AS board_output,
  REGEXP_EXTRACT(st.status_text, r'status:\s*([A-Za-z_]+)') AS board_status,
  `{{DS}}.f_ts`(REGEXP_EXTRACT(st.status_text, r'Last run\s+(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')) AS board_rendered_at
FROM body AS b
LEFT JOIN table_end AS e USING (snapshot_date)
LEFT JOIN status_line AS st USING (snapshot_date)
WHERE b.row_num < IFNULL(e.end_row, b.hrow + 41)
  AND `{{DS}}.f_cell`(h, c, 'Symbol') IS NOT NULL;
