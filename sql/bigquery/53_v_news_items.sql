-- v_news_items: every headline the news gate (scripts/news_macro_gate.py,
-- tab _News_Items) has collected, one row per Item ID - the copy from the
-- latest snapshot that still holds it, so items the tab has since dropped
-- stay here. symbol is the Scope when the item is about a name (NULL for
-- MACRO). direction: 1 positive, -1 negative, 0 neutral; severity H / M / L;
-- tier A (official) .. F (unknown source).
CREATE OR REPLACE VIEW `{{DS}}.v_news_items`
OPTIONS (description = "TFB Looker v2: news gate headlines, one row per item, with the symbol it concerns (NULL for MACRO)")
AS
WITH snaps AS (
  SELECT snapshot_date, captured_at, row_num, cells
  FROM `{{DS}}.news_items`
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
    `{{DS}}.f_cell`(h, s.cells, 'Item ID') AS item_id,
    `{{DS}}.f_ts`(`{{DS}}.f_cell`(h, s.cells, 'First Seen (Riyadh)')) AS first_seen_at,
    `{{DS}}.f_ts_utc`(`{{DS}}.f_cell`(h, s.cells, 'Published (UTC)')) AS published_at,
    `{{DS}}.f_cell`(h, s.cells, 'Scope') AS scope,
    `{{DS}}.f_cell`(h, s.cells, 'Event Type') AS event_type,
    SAFE_CAST(`{{DS}}.f_num`(`{{DS}}.f_cell`(h, s.cells, 'Direction')) AS INT64) AS direction,
    UPPER(`{{DS}}.f_cell`(h, s.cells, 'Severity')) AS severity,
    `{{DS}}.f_cell`(h, s.cells, 'Rumour/Denial') AS rumour_denial,
    `{{DS}}.f_cell`(h, s.cells, 'Source') AS source,
    `{{DS}}.f_cell`(h, s.cells, 'Domain') AS domain,
    UPPER(`{{DS}}.f_cell`(h, s.cells, 'Tier')) AS tier,
    `{{DS}}.f_cell`(h, s.cells, 'Cluster Grade') AS cluster_grade,
    `{{DS}}.f_cell`(h, s.cells, 'Headline') AS headline,
    `{{DS}}.f_cell`(h, s.cells, 'URL') AS url,
    `{{DS}}.f_cell`(h, s.cells, 'Cluster') AS cluster,
    `{{DS}}.f_cell`(h, s.cells, 'Run ID') AS run_id
  FROM snaps AS s
  JOIN hdr USING (snapshot_date)
  WHERE s.row_num > 1
)
SELECT
  t.*,
  IF(UPPER(t.scope) = 'MACRO', NULL, t.scope) AS symbol,
  UPPER(t.scope) = 'MACRO' AS is_macro,
  CASE t.direction WHEN 1 THEN 'positive' WHEN -1 THEN 'negative' WHEN 0 THEN 'neutral' END AS direction_label,
  DATE(COALESCE(t.published_at, t.first_seen_at), 'Asia/Riyadh') AS news_date
FROM typed AS t
WHERE t.item_id IS NOT NULL AND t.scope IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.item_id ORDER BY t.snapshot_date DESC) = 1;
