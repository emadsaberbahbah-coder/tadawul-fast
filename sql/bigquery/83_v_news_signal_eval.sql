-- v_news_signal_eval: does the news layer predict anything yet? One row per signal and class, refreshed by
-- itself (a view): the News & Macro Gate verdict against the next SPY returns, and each name's news sentiment
-- against that name's own next 5-session return.
-- WHY (owner, 16 Nov 2026: read all news, measure every recommendation against what happened, learn and correct
-- itself, design TFB_News_Scenarios_and_Continuous_Learning_Design, phase 0 "measure daily"). On 16 Nov the
-- measurement read by hand: the gate said NO_NEW_ENTRY on 13 of 16 days and GO on none (nothing to compare),
-- 98 % of name-days read "positive", and sentiment correlated -0.13 with the next 5 sessions. No news signal may
-- move a decision until this view shows it adds something, it is the scoreboard the later phases are judged on.
-- INPUTS:
--   gate       v_news_gate: the last verdict of each New York day
--   market     market_series_daily, symbol SPY: close, next close, close 5 sessions on
--   names      v_symbol_news: mean polarity of a name's items per news day
--   bars       symbol_bars_daily: adj_close (total return), the first bar on or after the news day and the bar
--              5 sessions after it
-- OUTPUT (one row per signal x class):
--   signal          'gate' | 'name_sentiment'
--   class           the gate verdict | positive (>= 0.15) / neutral / negative (<= -0.15)
--   days            observations (gate days, or name-days)
--   matured_1d / matured_5d, mean_1d_pct / mean_5d_pct, up_5d_pct
--   corr_5d         name_sentiment only: correlation of polarity with the next 5-session return, all classes
--   share_pct       the class's share of all observations of its signal
--   first_day / last_day
--   reading         'NOT MEASURABLE (one class)' when a signal has a single class, 'BIASED (class > 90 %)' when one
--                   class holds more than 90 % of the observations, 'NO EDGE' when corr_5d <= 0.05 or the classes'
--                   5-session means are not ordered as the signal claims, 'THIN' for fewer than 60 matured
--                   observations in a class, otherwise 'CANDIDATE (forward test)'. Never "proven": a candidate
--                   still needs a forward test before it moves a decision.
-- Research view: the live system never reads BigQuery, no decision, gate or order depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_news_signal_eval`
OPTIONS (description = "TFB news scoreboard: the News & Macro Gate verdict against the next SPY sessions, and the news sentiment of each name against its own next 5 sessions, with a reading per signal (NOT MEASURABLE, BIASED, NO EDGE, THIN, CANDIDATE). Research view, never an order.")
AS
WITH gate_day AS (
  SELECT DATE(run_at, 'America/New_York') AS d,
         ARRAY_AGG(verdict ORDER BY run_at DESC LIMIT 1)[OFFSET(0)] AS verdict
  FROM `{{DS}}.v_news_gate`
  WHERE verdict IS NOT NULL
  GROUP BY d
),
spy AS (
  SELECT d, close,
         LEAD(close, 1) OVER (ORDER BY d) AS c1,
         LEAD(close, 5) OVER (ORDER BY d) AS c5
  FROM (SELECT date AS d, ANY_VALUE(close) AS close
        FROM `{{DS}}.market_series_daily` WHERE symbol = 'SPY' AND close IS NOT NULL GROUP BY date)
),
gate_obs AS (
  -- the last SPY close on or before the verdict day (a weekend verdict is read against Friday's close)
  SELECT g.d, g.verdict,
         ARRAY_AGG(STRUCT((s.c1 / s.close - 1) * 100 AS r1, (s.c5 / s.close - 1) * 100 AS r5)
                   ORDER BY s.d DESC LIMIT 1)[OFFSET(0)] AS r
  FROM gate_day AS g
  JOIN spy AS s ON s.d <= g.d AND s.d >= DATE_SUB(g.d, INTERVAL 5 DAY)
  GROUP BY g.d, g.verdict
),
gate_rows AS (
  SELECT 'gate' AS signal, verdict AS class, COUNT(*) AS days,
         COUNTIF(r.r1 IS NOT NULL) AS matured_1d, COUNTIF(r.r5 IS NOT NULL) AS matured_5d,
         ROUND(AVG(r.r1), 2) AS mean_1d_pct, ROUND(AVG(r.r5), 2) AS mean_5d_pct,
         ROUND(SAFE_DIVIDE(COUNTIF(r.r5 > 0), COUNTIF(r.r5 IS NOT NULL)) * 100, 1) AS up_5d_pct,
         CAST(NULL AS FLOAT64) AS corr_5d, MIN(d) AS first_day, MAX(d) AS last_day
  FROM gate_obs GROUP BY verdict
),
name_day AS (
  SELECT symbol, news_date AS d, AVG(polarity) AS pol
  FROM `{{DS}}.v_symbol_news`
  WHERE polarity IS NOT NULL AND symbol IS NOT NULL AND news_date IS NOT NULL
  GROUP BY symbol, news_date
),
bars AS (
  SELECT symbol, date, close, LEAD(close, 5) OVER (PARTITION BY symbol ORDER BY date) AS c5
  FROM (SELECT symbol, date, ANY_VALUE(COALESCE(adj_close, close)) AS close
        FROM `{{DS}}.symbol_bars_daily` WHERE close IS NOT NULL GROUP BY symbol, date)
),
name_obs AS (
  -- the first bar on or after the news day (within 4 days), and its return over the next 5 sessions
  SELECT n.symbol, n.d, n.pol,
         ARRAY_AGG((b.c5 / b.close - 1) * 100 ORDER BY b.date ASC LIMIT 1)[OFFSET(0)] AS r5
  FROM name_day AS n
  JOIN bars AS b ON b.symbol = n.symbol AND b.date >= n.d AND b.date <= DATE_ADD(n.d, INTERVAL 4 DAY)
  GROUP BY n.symbol, n.d, n.pol
),
name_corr AS (SELECT CORR(pol, r5) AS c FROM name_obs),
name_rows AS (
  SELECT 'name_sentiment' AS signal,
         CASE WHEN pol >= 0.15 THEN 'positive' WHEN pol <= -0.15 THEN 'negative' ELSE 'neutral' END AS class,
         COUNT(*) AS days, CAST(NULL AS INT64) AS matured_1d, COUNTIF(r5 IS NOT NULL) AS matured_5d,
         CAST(NULL AS FLOAT64) AS mean_1d_pct, ROUND(AVG(r5), 2) AS mean_5d_pct,
         ROUND(SAFE_DIVIDE(COUNTIF(r5 > 0), COUNTIF(r5 IS NOT NULL)) * 100, 1) AS up_5d_pct,
         ROUND((SELECT c FROM name_corr), 3) AS corr_5d, MIN(d) AS first_day, MAX(d) AS last_day
  FROM name_obs GROUP BY class
),
all_rows AS (SELECT * FROM gate_rows UNION ALL SELECT * FROM name_rows),
shaped AS (
  SELECT a.*,
         ROUND(a.days / SUM(a.days) OVER (PARTITION BY a.signal) * 100, 1) AS share_pct,
         COUNT(*) OVER (PARTITION BY a.signal) AS n_classes,
         MAX(a.days / 1.0) OVER (PARTITION BY a.signal) / SUM(a.days) OVER (PARTITION BY a.signal) AS top_share,
         -- the order the signal claims: positive above negative, GO above CAUTION above NO_NEW_ENTRY
         MAX(IF(a.class IN ('positive', 'GO'), a.mean_5d_pct, NULL)) OVER (PARTITION BY a.signal) AS hi_mean,
         MAX(IF(a.class IN ('negative', 'NO_NEW_ENTRY'), a.mean_5d_pct, NULL)) OVER (PARTITION BY a.signal) AS lo_mean
  FROM all_rows AS a
)
SELECT signal, class, days, matured_1d, matured_5d, mean_1d_pct, mean_5d_pct, up_5d_pct, corr_5d, share_pct,
       first_day, last_day,
       CASE
         WHEN n_classes < 2 THEN 'NOT MEASURABLE (one class)'
         WHEN top_share > 0.9 THEN 'BIASED (class > 90 %)'
         WHEN signal = 'name_sentiment' AND (corr_5d IS NULL OR corr_5d <= 0.05) THEN 'NO EDGE'
         WHEN hi_mean IS NULL OR lo_mean IS NULL OR hi_mean <= lo_mean THEN 'NO EDGE'
         WHEN matured_5d < 60 THEN 'THIN'
         ELSE 'CANDIDATE (forward test)'
       END AS reading
FROM shaped;
