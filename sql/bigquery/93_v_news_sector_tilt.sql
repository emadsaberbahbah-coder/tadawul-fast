-- v_news_sector_tilt: what the news of a week says about each US sector for
-- the week after - the active weights of v_news_sector_weights added up.
-- ARMS: 'events' uses the war and US-China themes read from GDELT events;
-- 'all' adds the document themes (the Fed and rates, trade policy,
-- uncertainty, elections, AI and technology and its tone, the tone of
-- stock-market news, inflation, the oil price, armed conflict).
-- SCORE, per arm, news week and sector: the sum over active weights of
-- past_corr x prediction_pp (a weight with a better record counts for more).
-- pos ranks the 11 sectors by score, best first (ties by ETF symbol).
-- tilt_group: 'favoured' = the first three with a score above 0; 'avoided' =
-- the last three with a score under 0; 'neutral' otherwise - a sector the
-- news says nothing about is never moved. A week with no active weight at
-- all has no rows.
-- APPLIES: from the Tuesday after the news week to the Monday after that
-- (apply_from = week_start + 8 days, apply_to = week_start + 14 days), so a
-- list dated in that window never sees news later than its own Sunday.
-- realized_excess_pp: the sector ETF minus SPY over the week entered at the
-- first close after the news week (NULL until known) - for checking only.
-- COLUMNS: arm, week_start, apply_from, apply_to, etf, sector, score, pos,
-- sectors, active_features, active_list, tilt_group, realized_excess_pp.
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_news_sector_tilt`
OPTIONS (description = "TFB news loop: per news week and arm (events / all), each US sector's news score, rank and tilt group (favoured / avoided / neutral) with the window of list dates it applies to")
AS
WITH sp AS (
  SELECT
    news_arm AS arm, w.week_start, w.etf,
    ANY_VALUE(w.sector) AS sector,
    ANY_VALUE(w.excess_pp) AS excess_pp,
    SUM(IF(w.active, w.past_corr * w.prediction_pp, 0)) AS score_x,
    COUNTIF(w.active) AS active_features,
    STRING_AGG(IF(w.active, w.feature, NULL), ', ' ORDER BY w.feature) AS active_list
  FROM `{{DS}}.v_news_sector_weights` AS w
  CROSS JOIN UNNEST(['events', 'all']) AS news_arm
  WHERE w.prediction_pp IS NOT NULL
    AND (news_arm = 'all' OR w.source = 'gdelt_events')
  GROUP BY news_arm, w.week_start, w.etf
),
rk AS (
  SELECT
    sp.*,
    ROW_NUMBER() OVER (PARTITION BY arm, week_start ORDER BY score_x DESC, etf) AS pos,
    COUNT(*) OVER (PARTITION BY arm, week_start) AS sectors,
    MAX(active_features) OVER (PARTITION BY arm, week_start) AS max_active
  FROM sp
)
SELECT
  arm, week_start,
  DATE_ADD(week_start, INTERVAL 8 DAY) AS apply_from,
  DATE_ADD(week_start, INTERVAL 14 DAY) AS apply_to,
  etf, sector,
  ROUND(score_x, 4) AS score,
  pos, sectors, active_features, active_list,
  CASE
    WHEN pos <= 3 AND score_x > 0 THEN 'favoured'
    WHEN pos > sectors - 3 AND score_x < 0 THEN 'avoided'
    ELSE 'neutral'
  END AS tilt_group,
  ROUND(excess_pp, 2) AS realized_excess_pp
FROM rk
WHERE max_active > 0;
