-- Reader-only news. Does not feed v_symbol_rating, the gate or decisions.
-- A repeated URL is one article, not independent corroboration. Query strings
-- and path case stay intact; only scheme, host case and fragments are normalized.
-- Undated, future and unsafe-link records stay visible as quality exceptions.
CREATE OR REPLACE VIEW `{{DS}}.v_symbol_news_research`
OPTIONS (description = "TFB news reader: conservative article dedup, source evidence and honest publication/archive time. Research only")
AS
WITH items AS (
  SELECT
    n.symbol, n.published_at, CAST(NULL AS TIMESTAMP) AS first_seen_at,
    n.captured_at, n.snapshot_date, n.run_id,
    n.title, n.link AS original_url, n.domain,
    CAST(NULL AS STRING) AS publisher_name, 'eodhd' AS source_feed,
    'eodhd' AS source_kind, CAST(NULL AS STRING) AS source_item_id,
    n.polarity,
    CASE WHEN n.polarity >= 0.25 THEN 'positive'
         WHEN n.polarity <= -0.25 THEN 'negative'
         WHEN n.polarity IS NOT NULL THEN 'neutral' END AS sentiment_label,
    'provider_text_sentiment' AS sentiment_basis,
    CAST(NULL AS STRING) AS severity, CAST(NULL AS STRING) AS tier,
    CAST(NULL AS STRING) AS rumour_denial,
    CAST(NULL AS STRING) AS cluster_grade, CAST(NULL AS STRING) AS cluster,
    n.tags AS topic
  FROM `{{DS}}.symbol_news` AS n
  WHERE NULLIF(TRIM(n.symbol), '') IS NOT NULL
  UNION ALL
  SELECT
    g.symbol, g.published_at, g.first_seen_at,
    g.captured_at, g.snapshot_date, g.run_id,
    g.headline, g.url, g.domain, CAST(NULL AS STRING), g.source,
    'gate', g.item_id, CAST(NULL AS FLOAT64), g.direction_label,
    'gate_event_direction', g.severity, g.tier, g.rumour_denial,
    g.cluster_grade, g.cluster, g.event_type
  FROM `{{DS}}.v_news_items` AS g
  WHERE NULLIF(TRIM(g.symbol), '') IS NOT NULL
),
clean AS (
  SELECT
    i.* EXCEPT (title, original_url),
    REGEXP_REPLACE(TRIM(i.title), r'\s+', ' ') AS title,
    NULLIF(TRIM(i.original_url), '') AS original_url
  FROM items AS i
  WHERE NULLIF(TRIM(i.title), '') IS NOT NULL
),
links AS (
  SELECT c.*,
    CASE WHEN REGEXP_CONTAINS(original_url, r'(?i)^https?://[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?(?::[0-9]{1,5})?(?:[/?#]|$)')
               AND NOT REGEXP_CONTAINS(original_url, r'\s')
               AND NOT REGEXP_CONTAINS(original_url, r'[\\\x00-\x20\x7f]')
               AND IFNULL(CAST(REGEXP_EXTRACT(original_url, r'(?i)^https?://[^/?#]+:([0-9]{1,5})(?:[/?#]|$)') AS INT64), 1) BETWEEN 1 AND 65535
               AND NET.HOST(original_url) IS NOT NULL
         THEN original_url END AS link
  FROM clean AS c
),
keyed AS (
  SELECT l.*,
    CASE WHEN link IS NOT NULL THEN CONCAT('url:', LOWER(REGEXP_EXTRACT(link, r'(?i)^https?://([^/?#]+)')),
      COALESCE(NULLIF(REGEXP_EXTRACT(link, r'(?i)^https?://[^/?#]+([^#]*)'), ''), '/'))
    ELSE CONCAT('unlinked:', source_kind, ':', LOWER(title), ':',
      COALESCE(CAST(published_at AS STRING), source_item_id, CAST(captured_at AS STRING), 'unknown'))
    END AS article_key,
    COALESCE(published_at, first_seen_at) AS display_at,
    CASE WHEN published_at IS NOT NULL THEN 'publisher_timestamp'
         WHEN first_seen_at IS NOT NULL THEN 'first_seen_only'
         ELSE 'unknown' END AS time_basis,
    CASE WHEN published_at IS NULL THEN 'unknown_publication_time'
         WHEN published_at > CURRENT_TIMESTAMP() THEN 'future_timestamp'
         WHEN published_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) THEN 'older_than_30d'
         ELSE 'recent_30d' END AS freshness_state,
    CASE WHEN original_url IS NULL THEN 'missing_link'
         WHEN link IS NULL THEN 'unsafe_or_invalid_link'
         ELSE 'http_link' END AS link_state
  FROM links AS l
),
ranked AS (
  SELECT k.*,
    COUNT(*) OVER (PARTITION BY symbol, article_key) AS archive_copy_count,
    MAX(captured_at) OVER (PARTITION BY symbol, article_key) AS last_archived_at,
    MIN(title) OVER (PARTITION BY symbol, article_key) != MAX(title) OVER (PARTITION BY symbol, article_key) AS has_headline_variants,
    MIN(published_at) OVER (PARTITION BY symbol, article_key) != MAX(published_at) OVER (PARTITION BY symbol, article_key) AS has_publication_time_variants,
    MAX(IF(source_kind = 'eodhd', 1, 0)) OVER (PARTITION BY symbol, article_key) AS seen_in_eodhd,
    MAX(IF(source_kind = 'gate', 1, 0)) OVER (PARTITION BY symbol, article_key) AS seen_in_gate,
    ROW_NUMBER() OVER (PARTITION BY symbol, article_key ORDER BY
      IF(published_at IS NOT NULL AND published_at <= CURRENT_TIMESTAMP(), 0, 1),
      IF(source_kind = 'gate', 0, 1), captured_at DESC, snapshot_date DESC,
      published_at DESC, title, original_url, COALESCE(source_item_id, ''),
      COALESCE(run_id, ''), TO_JSON_STRING(k)) AS article_rank
  FROM keyed AS k
)
SELECT
  r.* EXCEPT (article_rank),
  LOWER(NET.HOST(r.link)) AS link_domain,
  COALESCE(u.symbol_label, r.symbol) AS symbol_label,
  u.name,
  DATE(r.display_at, 'Asia/Riyadh') AS news_date,
  r.seen_in_eodhd + r.seen_in_gate AS feed_count,
  CASE WHEN r.published_at <= CURRENT_TIMESTAMP()
       THEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), r.published_at, HOUR) END AS article_age_hours,
  CASE WHEN r.last_archived_at <= CURRENT_TIMESTAMP()
       THEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), r.last_archived_at, HOUR) END AS archive_age_hours,
  r.freshness_state = 'recent_30d' AND r.link_state = 'http_link' AS is_recent_linked_article,
  'Provider/scope association. Independent company identity not verified' AS symbol_match_basis,
  CASE WHEN r.source_kind = 'gate' THEN 'Gate event direction. Not a price forecast'
       ELSE 'Provider news text sentiment. Not a price forecast' END AS interpretation_note
FROM ranked AS r
LEFT JOIN `{{DS}}.v_universe_latest` AS u ON u.symbol = r.symbol
WHERE r.article_rank = 1;
