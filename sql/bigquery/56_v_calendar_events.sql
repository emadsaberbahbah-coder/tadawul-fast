-- v_calendar_events: Calendar_Events (scripts/run_calendar_sync.py) per
-- snapshot - the next earnings and ex-dividend dates of each name, the last
-- report and its EPS surprise (percent points, as the sync writes it).
-- days_to_*_now count from the snapshot date (the tab's own Days To figures
-- are as of its last sync); is_latest marks the newest snapshot.
CREATE OR REPLACE VIEW `{{DS}}.v_calendar_events`
OPTIONS (description = "TFB Looker v2: next earnings and ex-dividend dates per name and snapshot, last report and EPS surprise")
AS
WITH snaps AS (
  SELECT snapshot_date, captured_at, row_num, cells
  FROM `{{DS}}.calendar_events`
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
    `{{DS}}.f_cell`(h, s.cells, 'Symbol') AS symbol,
    SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(`{{DS}}.f_cell`(h, s.cells, 'Next Earnings Date'), 1, 10)) AS next_earnings_date,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Days To Earnings')) AS days_to_earnings_sheet,
    SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(`{{DS}}.f_cell`(h, s.cells, 'Next Ex-Div Date'), 1, 10)) AS next_exdiv_date,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Days To ExDiv')) AS days_to_exdiv_sheet,
    `{{DS}}.f_ts`(`{{DS}}.f_cell`(h, s.cells, 'Updated At (Riyadh)')) AS updated_at,
    `{{DS}}.f_cell`(h, s.cells, 'Source') AS source,
    SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(`{{DS}}.f_cell`(h, s.cells, 'Last Earnings Date'), 1, 10)) AS last_earnings_date,
    `{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'EPS Surprise %')) AS eps_surprise_pct
  FROM snaps AS s
  JOIN hdr USING (snapshot_date)
  WHERE s.row_num > 1
)
SELECT
  t.*,
  DATE_DIFF(t.next_earnings_date, t.snapshot_date, DAY) AS days_to_earnings_now,
  DATE_DIFF(t.next_exdiv_date, t.snapshot_date, DAY) AS days_to_exdiv_now,
  DATE_DIFF(t.snapshot_date, t.last_earnings_date, DAY) AS days_since_earnings_now,
  t.snapshot_date = MAX(t.snapshot_date) OVER () AS is_latest
FROM typed AS t
WHERE t.symbol IS NOT NULL;
