-- v_top10_status: one row per nightly Top_10_Investments snapshot - the
-- cockpit status line (status, output, render time) and how many board rows
-- and seats it printed, including the nights the board was honestly empty
-- (those nights have no rows in v_top10_board).
CREATE OR REPLACE VIEW `{{DS}}.v_top10_status`
OPTIONS (description = "TFB Looker v1: nightly Top-10 cockpit status (status, output, seats), one row per snapshot")
AS
WITH status_line AS (
  SELECT snapshot_date, ANY_VALUE(captured_at) AS captured_at, ANY_VALUE(run_id) AS run_id,
         STRING_AGG(IF(row_num = 2, ARRAY_TO_STRING(cells, ' '), NULL), ' ') AS status_text
  FROM `{{DS}}.top_10_investments`
  GROUP BY snapshot_date
),
seats AS (
  SELECT snapshot_date, COUNT(*) AS board_rows, COUNTIF(seat_rank IS NOT NULL) AS seats_ranked,
         COUNTIF(ticket_sar > 0) AS seats_funded
  FROM `{{DS}}.v_top10_board`
  GROUP BY snapshot_date
)
SELECT
  st.snapshot_date,
  st.captured_at,
  st.run_id,
  `{{DS}}.f_ts`(REGEXP_EXTRACT(st.status_text, r'Last run\s+(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')) AS board_rendered_at,
  REGEXP_EXTRACT(st.status_text, r'status:\s*([A-Za-z_]+)') AS board_status,
  REGEXP_EXTRACT(st.status_text, r'output:\s*([A-Z_]+)') AS board_output,
  REGEXP_EXTRACT(st.status_text, r'source readiness ([A-Z]+)') AS source_readiness,
  IFNULL(se.board_rows, 0) AS board_rows,
  IFNULL(se.seats_ranked, 0) AS seats_ranked,
  IFNULL(se.seats_funded, 0) AS seats_funded,
  SUBSTR(st.status_text, 1, 600) AS status_text
FROM status_line AS st
LEFT JOIN seats AS se USING (snapshot_date);
