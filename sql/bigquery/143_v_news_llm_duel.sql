-- v_news_llm_duel: the days the two readings part - who was right? One row per kind x duel, from v_news_llm_days
-- (method there): each name-day (COMPANY) or news day (MACRO) with both readers' classes side by side.
--   duel   'agree'       both readers gave the day the same class
--          'opposite'    one positive, the other negative
--          'model only'  the model called the day (positive or negative), the word lists read it neutral
--          'rules only'  the word lists called it, the model read it neutral
-- OUTPUT: kind, duel, days, matured_5d, model_calls / model_right_pct and rules_calls / rules_right_pct (matured calls
--   whose 5-session move went their way), mean_5d_pp, first_day, last_day, reading.
-- READING (never "proven"; a call is a positive or negative class with a matured 5-session move):
--   'agree'       'BOTH'
--   'opposite'    'THIN' under 20 matured days, else 'MODEL AHEAD (forward test)' when the model was right on at
--                 least 60 % of them, 'RULES AHEAD (forward test)' at 40 % or less, else 'LEVEL'
--   'model only'  'THIN' under 30 model calls, else 'MODEL ADDS (forward test)' at 55 % right or more,
--                 'MODEL NOISE' at 45 % or less, else 'UNCLEAR'
--   'rules only'  the same for the word lists: 'RULES ADD (forward test)' / 'RULES NOISE' / 'UNCLEAR' / 'THIN'
-- WHY (owner, 26 Nov 2026 15:34 Riyadh, in Arabic: "let us start with this too ... linked with BigQuery"): the model
-- is worth keeping only where it reads a day differently from the word lists AND is right more often there; the
-- days both agree say nothing about which to trust. (apply_bq_views 1.32.0)
-- Research only: no decision, gate or count reads this view. (views 1.32.0)
CREATE OR REPLACE VIEW `{{DS}}.v_news_llm_duel`
OPTIONS (description = "TFB news read twice - the days the word lists and the language model disagree, and which was right over the next 5 sessions, with a fixed reading. Research view, never an order.")
AS
WITH pair AS (
  SELECT kind, symbol, news_date, ANY_VALUE(move_5d_pp) AS move_5d_pp,
         MAX(IF(reader = 'rules', class, NULL)) AS rules_class,
         MAX(IF(reader = 'model', class, NULL)) AS model_class
  FROM `{{DS}}.v_news_llm_days`
  GROUP BY kind, symbol, news_date
),
duels AS (
  SELECT p.*,
         CASE
           WHEN p.rules_class = p.model_class THEN 'agree'
           WHEN p.rules_class != 'neutral' AND p.model_class != 'neutral' THEN 'opposite'
           WHEN p.rules_class = 'neutral' THEN 'model only'
           ELSE 'rules only'
         END AS duel,
         CASE
           WHEN p.move_5d_pp IS NULL THEN NULL
           WHEN p.model_class = 'positive' THEN p.move_5d_pp > 0
           WHEN p.model_class = 'negative' THEN p.move_5d_pp < 0
         END AS model_right,
         CASE
           WHEN p.move_5d_pp IS NULL THEN NULL
           WHEN p.rules_class = 'positive' THEN p.move_5d_pp > 0
           WHEN p.rules_class = 'negative' THEN p.move_5d_pp < 0
         END AS rules_right
  FROM pair AS p
  WHERE p.rules_class IS NOT NULL AND p.model_class IS NOT NULL
),
tally AS (
  SELECT kind, duel, COUNT(*) AS days, COUNTIF(move_5d_pp IS NOT NULL) AS matured_5d,
         COUNTIF(model_right IS NOT NULL) AS model_calls,
         ROUND(SAFE_DIVIDE(COUNTIF(model_right), COUNTIF(model_right IS NOT NULL)) * 100, 1) AS model_right_pct,
         COUNTIF(rules_right IS NOT NULL) AS rules_calls,
         ROUND(SAFE_DIVIDE(COUNTIF(rules_right), COUNTIF(rules_right IS NOT NULL)) * 100, 1) AS rules_right_pct,
         ROUND(AVG(move_5d_pp), 2) AS mean_5d_pp,
         MIN(news_date) AS first_day, MAX(news_date) AS last_day
  FROM duels
  GROUP BY kind, duel
)
SELECT t.*,
       CASE
         WHEN t.duel = 'agree' THEN 'BOTH'
         WHEN t.duel = 'opposite' AND t.matured_5d < 20 THEN 'THIN'
         WHEN t.duel = 'opposite' AND t.model_right_pct >= 60 THEN 'MODEL AHEAD (forward test)'
         WHEN t.duel = 'opposite' AND t.model_right_pct <= 40 THEN 'RULES AHEAD (forward test)'
         WHEN t.duel = 'opposite' THEN 'LEVEL'
         WHEN t.duel = 'model only' AND t.model_calls < 30 THEN 'THIN'
         WHEN t.duel = 'model only' AND t.model_right_pct >= 55 THEN 'MODEL ADDS (forward test)'
         WHEN t.duel = 'model only' AND t.model_right_pct <= 45 THEN 'MODEL NOISE'
         WHEN t.duel = 'model only' THEN 'UNCLEAR'
         WHEN t.rules_calls < 30 THEN 'THIN'
         WHEN t.rules_right_pct >= 55 THEN 'RULES ADD (forward test)'
         WHEN t.rules_right_pct <= 45 THEN 'RULES NOISE'
         ELSE 'UNCLEAR'
       END AS reading
FROM tally AS t;
