-- v_news_theme_weekly: the world-news themes per week, and how far each week
-- stands from its own recent norm - the input of the news learning loop
-- (v_news_sector_weights, v_news_sector_tilt, v_news_overlay_list,
-- v_trial_news_overlay). WHY (owner, 19 Nov 2026: world news - wars,
-- politics, the economy, AI fear, elections, summits - must be classified,
-- weighted, tested on the past and allowed to correct itself; then 13:06
-- Riyadh, verbatim: "Let's Start build the new news and it's effect in the
-- file by the best way that you propse and before doing that make more final
-- test then apply the changes by the best possible and effective way").
-- SOURCE: news_theme_daily (scripts/archive_news_themes.py): one row per UTC
-- day and theme with a numerator and a denominator from the public GDELT
-- tables - 'share' themes (num = matching events or documents, den = all of
-- that day) and 'mean' themes (num = the sum of a tone, den = its count).
-- WEEK: Monday to Sunday, UTC. value = 1000 x SUM(num) / SUM(den) for a share
-- (per thousand), SUM(num) / SUM(den) for a mean. surprise = value minus the
-- average of the four weeks before. A week is closed when all 7 days are
-- loaded, or a later day is (GDELT has a few days with nothing published).
-- usable = the week is closed and has at least 5 days, and the four weeks
-- before it are there, one after the other, each with at least 5 days: only
-- a usable week may teach a weight or move a sector.
-- COLUMNS: week_start, week_end, feature, kind, source, days, is_complete
-- (all 7 days), is_closed, value, prev_4w_avg, surprise, usable.
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_news_theme_weekly`
OPTIONS (description = "TFB news loop: world-news themes per week (GDELT events and documents) with each week's surprise against its own previous four weeks and a usable flag")
AS
WITH wk AS (
  SELECT
    DATE_TRUNC(date, WEEK(MONDAY)) AS week_start, feature,
    ANY_VALUE(kind) AS kind, ANY_VALUE(source) AS source,
    COUNT(DISTINCT date) AS days, SUM(num) AS num, SUM(den) AS den,
    MAX(MAX(date)) OVER (PARTITION BY feature) AS last_day
  FROM `{{DS}}.news_theme_daily`
  GROUP BY week_start, feature
),
v AS (
  SELECT wk.*, IF(kind = 'share', 1000, 1) * SAFE_DIVIDE(num, den) AS value
  FROM wk
),
p AS (
  SELECT
    v.*,
    AVG(value) OVER p4 AS prev_avg,
    COUNTIF(days >= 5 AND value IS NOT NULL) OVER p4 AS prev_ok,
    MIN(week_start) OVER p4 AS prev_first
  FROM v
  WINDOW p4 AS (PARTITION BY feature ORDER BY week_start ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING)
)
SELECT
  week_start, DATE_ADD(week_start, INTERVAL 6 DAY) AS week_end, feature, kind, source, days,
  days = 7 AS is_complete,
  days = 7 OR last_day > DATE_ADD(week_start, INTERVAL 6 DAY) AS is_closed,
  ROUND(value, 4) AS value,
  ROUND(prev_avg, 4) AS prev_4w_avg,
  value - prev_avg AS surprise,
  (days = 7 OR last_day > DATE_ADD(week_start, INTERVAL 6 DAY)) AND days >= 5 AND value IS NOT NULL
    AND prev_ok = 4 AND prev_first = DATE_SUB(week_start, INTERVAL 28 DAY) AS usable
FROM p;
