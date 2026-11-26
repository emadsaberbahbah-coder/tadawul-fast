-- v_news_llm_eval: the scoreboard of each reading alone - does the word lists' or the language model's class of a
-- day say anything about the next sessions? One row per reader x kind x class, from v_news_llm_days (method there).
-- OUTPUT: reader ('rules' / 'model'), kind ('COMPANY' / 'MACRO'), class, days, matured_1d, matured_5d,
--   mean_1d_pp, mean_5d_pp (a name: against the same-day US names; MACRO: the S&P 500 fund itself), with_5d_pct (share
--   of the matured days whose 5-session move went the class's way; NULL for neutral), corr_5d (the day's summed score
--   against the 5-session move, every day of that reader and kind), share_pct, first_day, last_day, reading.
-- READING (per reader and kind, never "proven"):
--   'NOT MEASURABLE (one side)'  the reader has no positive or no negative day
--   'BIASED (one side > 90 %)'   one side holds more than 90 % of its positive and negative days
--   'THIN'                       the smaller side has fewer than 30 matured 5-session days - checked before the edge,
--                                because on a few weeks of days a "no edge" call is as noisy as an edge
--   'NO EDGE'                    corr_5d <= 0.05, or positive not above negative on mean_5d_pp
--   'CANDIDATE (forward test)'   otherwise
-- WHY (owner, 26 Nov 2026 15:34 Riyadh, in Arabic: "let us start with this too ... linked with BigQuery"; the
-- 21 Nov rule: observe for ten sessions, then review, and switch on only what proved itself). The model reads in
-- observe mode; nothing may move a decision on its reading before this view and a forward test agree.
-- (apply_bq_views 1.32.0)
-- Research only: no decision, gate or count reads this view. (views 1.32.0)
CREATE OR REPLACE VIEW `{{DS}}.v_news_llm_eval`
OPTIONS (description = "TFB scoreboard of the two news readings - the word lists and the language model - each class of day against the next 1 and 5 sessions (a name against the same-day US names, MACRO against the S&P 500 fund), with a fixed reading. Research view, never an order.")
AS
WITH obs AS (
  SELECT reader, kind, news_date, score, class, move_1d_pp, move_5d_pp, went_its_way
  FROM `{{DS}}.v_news_llm_days`
),
fit AS (
  SELECT reader, kind, CORR(score, move_5d_pp) AS c
  FROM obs
  GROUP BY reader, kind
),
class_rows AS (
  SELECT o.reader, o.kind, o.class, COUNT(*) AS days,
         COUNTIF(o.move_1d_pp IS NOT NULL) AS matured_1d, COUNTIF(o.move_5d_pp IS NOT NULL) AS matured_5d,
         ROUND(AVG(o.move_1d_pp), 2) AS mean_1d_pp, ROUND(AVG(o.move_5d_pp), 2) AS mean_5d_pp,
         ROUND(SAFE_DIVIDE(COUNTIF(o.went_its_way), COUNTIF(o.went_its_way IS NOT NULL)) * 100, 1) AS with_5d_pct,
         MIN(o.news_date) AS first_day, MAX(o.news_date) AS last_day
  FROM obs AS o
  GROUP BY o.reader, o.kind, o.class
),
shaped AS (
  SELECT r.*, ROUND(c.c, 3) AS corr_5d,
         ROUND(r.days / SUM(r.days) OVER w * 100, 1) AS share_pct,
         COUNTIF(r.class IN ('positive', 'negative')) OVER w AS n_sides,
         SAFE_DIVIDE(MAX(IF(r.class IN ('positive', 'negative'), r.days, NULL)) OVER w,
                     SUM(IF(r.class IN ('positive', 'negative'), r.days, 0)) OVER w) AS top_side_share,
         MIN(IF(r.class IN ('positive', 'negative'), r.matured_5d, NULL)) OVER w AS min_side_matured,
         MAX(IF(r.class = 'positive', r.mean_5d_pp, NULL)) OVER w AS hi_mean,
         MAX(IF(r.class = 'negative', r.mean_5d_pp, NULL)) OVER w AS lo_mean
  FROM class_rows AS r
  LEFT JOIN fit AS c ON c.reader = r.reader AND c.kind = r.kind
  WINDOW w AS (PARTITION BY r.reader, r.kind)
)
SELECT reader, kind, class, days, matured_1d, matured_5d, mean_1d_pp, mean_5d_pp, with_5d_pct, corr_5d, share_pct,
       first_day, last_day,
       CASE
         WHEN n_sides < 2 THEN 'NOT MEASURABLE (one side)'
         WHEN top_side_share > 0.9 THEN 'BIASED (one side > 90 %)'
         WHEN min_side_matured IS NULL OR min_side_matured < 30 THEN 'THIN'
         WHEN corr_5d IS NULL OR corr_5d <= 0.05 THEN 'NO EDGE'
         WHEN hi_mean IS NULL OR lo_mean IS NULL OR hi_mean <= lo_mean THEN 'NO EDGE'
         ELSE 'CANDIDATE (forward test)'
       END AS reading
FROM shaped;
