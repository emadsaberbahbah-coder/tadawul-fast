-- v_symbol_news: headlines per symbol - EODHD news with the provider's
-- sentiment polarity (-1 .. 1; symbol_news, scripts/archive_symbol_deep.py),
-- one row per symbol and article (the copy from the latest night that holds
-- it), plus the news gate's own items about the symbol (v_news_items,
-- source_kind 'gate', direction as the sentiment). sentiment_label:
-- positive at polarity >= 0.25, negative at <= -0.25, else neutral.
-- symbol_label keys the symbol picker.
CREATE OR REPLACE VIEW `{{DS}}.v_symbol_news`
OPTIONS (description = "TFB Looker v3: headlines per symbol with sentiment - EODHD news and the news gate's items")
AS
WITH e AS (
  SELECT n.*
  FROM `{{DS}}.symbol_news` AS n
  WHERE n.symbol IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY n.symbol, COALESCE(n.link, n.title) ORDER BY n.snapshot_date DESC) = 1
),
items AS (
  SELECT
    e.symbol, e.published_at, e.title, e.link, e.domain, e.polarity,
    CASE WHEN e.polarity >= 0.25 THEN 'positive' WHEN e.polarity <= -0.25 THEN 'negative'
         WHEN e.polarity IS NOT NULL THEN 'neutral' END AS sentiment_label,
    'eodhd' AS source_kind, CAST(NULL AS STRING) AS severity, e.tags
  FROM e
  UNION ALL
  SELECT
    g.symbol, COALESCE(g.published_at, g.first_seen_at), g.headline, g.url, g.domain,
    CAST(NULL AS FLOAT64),
    g.direction_label, 'gate', g.severity, g.event_type
  FROM `{{DS}}.v_news_items` AS g
  WHERE g.symbol IS NOT NULL AND g.headline IS NOT NULL
)
SELECT
  i.symbol,
  COALESCE(u.symbol_label, i.symbol) AS symbol_label,
  u.name,
  i.published_at,
  DATE(i.published_at, 'Asia/Riyadh') AS news_date,
  i.title,
  i.link,
  i.domain,
  i.polarity,
  i.sentiment_label,
  i.source_kind,
  i.severity,
  i.tags
FROM items AS i
LEFT JOIN `{{DS}}.v_universe_latest` AS u ON u.symbol = i.symbol;
