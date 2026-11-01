-- v_news_gate: every news-gate run (tab _News_Gate), one row per Run ID:
-- the verdict (GO / CAUTION / NO_NEW_ENTRY), what drove it, the names it
-- flagged, the item counts (new / classified / collected) and the sources
-- that answered or failed.
CREATE OR REPLACE VIEW `{{DS}}.v_news_gate`
OPTIONS (description = "TFB Looker v2: news gate runs - verdict, drivers, flagged names, item counts, source health")
AS
WITH snaps AS (
  SELECT snapshot_date, captured_at, row_num, cells
  FROM `{{DS}}.news_gate`
),
hdr AS (
  SELECT snapshot_date, cells AS h
  FROM snaps
  WHERE row_num = 1
),
typed AS (
  SELECT
    s.snapshot_date,
    `{{DS}}.f_ts`(`{{DS}}.f_cell`(h, s.cells, 'Run At (Riyadh)')) AS run_at,
    `{{DS}}.f_cell`(h, s.cells, 'Run ID') AS run_id,
    UPPER(`{{DS}}.f_cell`(h, s.cells, 'Verdict')) AS verdict,
    `{{DS}}.f_cell`(h, s.cells, 'Drivers') AS drivers,
    `{{DS}}.f_cell`(h, s.cells, 'Scheduled Events') AS scheduled_events,
    `{{DS}}.f_cell`(h, s.cells, 'Market') AS market,
    `{{DS}}.f_cell`(h, s.cells, 'Names Flagged') AS names_flagged,
    `{{DS}}.f_cell`(h, s.cells, 'Items (new/classified/collected)') AS items_text,
    `{{DS}}.f_cell`(h, s.cells, 'Sources OK') AS sources_ok,
    `{{DS}}.f_cell`(h, s.cells, 'Sources Failed') AS sources_failed,
    `{{DS}}.f_cell`(h, s.cells, 'Mode') AS mode,
    `{{DS}}.f_cell`(h, s.cells, 'Version') AS version
  FROM snaps AS s
  JOIN hdr USING (snapshot_date)
  WHERE s.row_num > 1
)
SELECT
  t.*,
  SAFE_CAST(REGEXP_EXTRACT(t.items_text, r'^\s*(\d+)/') AS INT64) AS items_new,
  SAFE_CAST(REGEXP_EXTRACT(t.items_text, r'^\s*\d+/(\d+)/') AS INT64) AS items_classified,
  SAFE_CAST(REGEXP_EXTRACT(t.items_text, r'^\s*\d+/\d+/(\d+)') AS INT64) AS items_collected,
  DATE(t.run_at, 'Asia/Riyadh') AS run_date
FROM typed AS t
WHERE t.run_id IS NOT NULL AND t.run_at IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.run_id ORDER BY t.snapshot_date DESC) = 1;
