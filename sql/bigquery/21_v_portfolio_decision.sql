-- v_portfolio_decision: the holdings decision table of Portfolio_Decision as
-- archived each night (one row per holding and snapshot). SAR columns are
-- valuations in SAR; P&L % / Weight % / ROI columns are percent points as
-- the page prints them. pd_* columns come from the page's status line;
-- pd_ibkr_evidence reads the note pdIbkrNote_ (apps_script/10_My_Portfolio.gs)
-- appends: SENT / NOT SENT in enforce mode, WOULD SEND / WOULD NOT SEND
-- (observe) in observe mode, NULL when the line carries no IBKR note.
CREATE OR REPLACE VIEW `{{DS}}.v_portfolio_decision`
OPTIONS (description = "TFB Looker v1: nightly Portfolio_Decision holdings table per snapshot")
AS
WITH snaps AS (
  SELECT snapshot_date, captured_at, row_num, cells
  FROM `{{DS}}.portfolio_decision`
),
hdr AS (
  SELECT snapshot_date, row_num AS hrow, cells AS h
  FROM snaps
  WHERE 'Action' IN UNNEST(cells) AND 'Symbol' IN UNNEST(cells) AND 'Qty' IN UNNEST(cells)
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
     OR `{{DS}}.f_cell`(h, c, 'Action') = 'Symbol'
  GROUP BY snapshot_date
)
SELECT
  b.snapshot_date,
  st.captured_at,
  `{{DS}}.f_cell`(h, c, 'Action') AS action,
  `{{DS}}.f_cell`(h, c, 'Symbol') AS symbol,
  `{{DS}}.f_cell`(h, c, 'Name') AS name,
  `{{DS}}.f_cell`(h, c, 'Sector') AS sector,
  `{{DS}}.f_cell`(h, c, 'Ccy') AS ccy,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Qty')) AS qty,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Avg Cost')) AS avg_cost,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Price')) AS price,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Price SAR')) AS price_sar,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'MV SAR')) AS market_value_sar,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Cost SAR')) AS cost_sar,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'P&L SAR')) AS pnl_sar,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'P&L %')) AS pnl_pct,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Weight %')) AS weight_pct,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Δ SAR')) AS delta_sar,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Δ Shares')) AS delta_shares,
  `{{DS}}.f_cell`(h, c, 'Funds From') AS funds_from,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Stop SAR')) AS stop_sar,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'TP1 SAR')) AS tp1_sar,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'TP2 SAR')) AS tp2_sar,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'ROI %')) AS roi_pct,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Engine ROI %')) AS engine_roi_pct,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'Rel')) AS reliability,
  `{{DS}}.f_num`(`{{DS}}.f_cell`(h, c, 'DQ')) AS data_quality,
  `{{DS}}.f_cell`(h, c, 'Conf') AS confidence,
  `{{DS}}.f_cell`(h, c, 'Advisor Note') AS advisor_note,
  REGEXP_EXTRACT(st.status_text, r'status:\s*([A-Za-z_]+)') AS pd_status,
  CASE
    WHEN REGEXP_CONTAINS(st.status_text, r'ibkr evidence NOT SENT') THEN 'NOT SENT'
    WHEN REGEXP_CONTAINS(st.status_text, r'ibkr evidence SENT') THEN 'SENT'
    WHEN REGEXP_CONTAINS(st.status_text, r'\[ibkr-observe\] would not send') THEN 'WOULD NOT SEND (observe)'
    WHEN REGEXP_CONTAINS(st.status_text, r'\[ibkr-observe\] would send') THEN 'WOULD SEND (observe)'
  END AS pd_ibkr_evidence,
  `{{DS}}.f_ts`(REGEXP_EXTRACT(st.status_text, r'Last run\s+(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')) AS pd_rendered_at
FROM body AS b
LEFT JOIN table_end AS e USING (snapshot_date)
LEFT JOIN status_line AS st USING (snapshot_date)
WHERE b.row_num < IFNULL(e.end_row, b.hrow + 61)
  AND `{{DS}}.f_cell`(h, c, 'Symbol') IS NOT NULL;
