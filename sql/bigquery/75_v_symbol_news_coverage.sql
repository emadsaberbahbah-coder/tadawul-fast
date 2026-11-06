-- Coverage of archived evidence, not proof the provider was checked successfully.
-- Collection currently has no successful-empty/error/budget-skip receipt per symbol.
CREATE OR REPLACE VIEW `{{DS}}.v_symbol_news_coverage`
OPTIONS (description = "TFB news coverage per symbol: recent, stale and quality exceptions. Absence is unknown coverage, never evidence of no news")
AS
WITH symbols AS (
  SELECT symbol FROM `{{DS}}.v_universe_latest`
  UNION DISTINCT
  SELECT symbol FROM `{{DS}}.v_symbol_news_research`
),
counts AS (
  SELECT symbol,
    COUNT(*) AS archived_articles,
    COUNTIF(is_recent_linked_article) AS recent_linked_articles_30d,
    COUNTIF(freshness_state = 'older_than_30d') AS stale_articles,
    COUNTIF(freshness_state = 'unknown_publication_time') AS undated_articles,
    COUNTIF(freshness_state = 'future_timestamp') AS future_timestamp_articles,
    COUNTIF(link_state != 'http_link') AS missing_or_invalid_link_articles,
    MAX(published_at) AS latest_reported_publication_at,
    MAX(IF(published_at <= CURRENT_TIMESTAMP(), published_at, NULL)) AS latest_valid_publication_at,
    MAX(last_archived_at) AS latest_article_capture_at
  FROM `{{DS}}.v_symbol_news_research`
  GROUP BY symbol
)
SELECT s.symbol, COALESCE(u.symbol_label, s.symbol) AS symbol_label, u.name,
  IFNULL(c.archived_articles, 0) AS archived_articles,
  IFNULL(c.recent_linked_articles_30d, 0) AS recent_linked_articles_30d,
  IFNULL(c.stale_articles, 0) AS stale_articles,
  IFNULL(c.undated_articles, 0) AS undated_articles,
  IFNULL(c.future_timestamp_articles, 0) AS future_timestamp_articles,
  IFNULL(c.missing_or_invalid_link_articles, 0) AS missing_or_invalid_link_articles,
  c.latest_reported_publication_at, c.latest_valid_publication_at, c.latest_article_capture_at,
  CASE WHEN c.recent_linked_articles_30d > 0 THEN 'recent_archived_evidence'
       WHEN c.archived_articles > 0 THEN 'no_recent_valid_linked_evidence'
       ELSE 'not_observed_in_archive' END AS coverage_state,
  'unknown_no_per_symbol_receipt' AS collection_check_state,
  CASE WHEN c.recent_linked_articles_30d > 0 THEN 'أخبار مؤرخة وروابط متاحة خلال ٣٠ يوماً؛ التغطية ليست شاملة'
       WHEN c.archived_articles > 0 THEN 'المتاح قديم أو بلا تاريخ موثوق أو رابط صالح؛ راجع تفاصيل الجودة'
       ELSE 'لم تظهر أخبار في الأرشيف لهذا الرمز؛ لا يعني ذلك عدم وجود أخبار أو نجاح فحص المصدر' END AS coverage_note_ar,
  'Archive capture time is not publication time. No row does not mean no news.' AS coverage_note
FROM symbols AS s
LEFT JOIN counts AS c ON c.symbol = s.symbol
LEFT JOIN `{{DS}}.v_universe_latest` AS u ON u.symbol = s.symbol
WHERE s.symbol IS NOT NULL;
