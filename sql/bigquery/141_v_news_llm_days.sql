-- v_news_llm_days: the two readings of the same headlines (v_news_llm), summed per day and set against what the
-- prices did next - one row per reader x day: for a name, per name and news day; for MACRO, per news day.
-- METHOD (the method of v_company_news_eval, so the scoreboards can be compared):
--   readers    'rules' (the gate's word lists) and 'model' (the language model), over the SAME headlines: every
--              headline the model labelled. A headline with no event counts 0 for that reader.
--   score      the day's sum of directions (1 good, -1 bad) of that reader; class positive >= 1, negative <= -1,
--              neutral otherwise (no event, or events that net to nothing)
--   names      US names only (symbol ending '.US'), so each return can be set against one market
--   news day   the Riyadh date of the headline (v_news_llm.news_date)
--   entry      the close of the first session AFTER the news day - never the news day's own close, which could come
--              before the headline (no look-ahead); the first bar within 6 calendar days, else the day has no move
--   move       a name: its adjusted return over the next 1 and 5 sessions from that close minus the same-day average
--              of every US name in symbol_bars_daily over the same sessions ("peers"), in percentage points.
--              MACRO: the S&P 500 fund's (SPY) adjusted return over the next 1 and 5 sessions from the first SPY
--              close after the news day, in per cent - the MACRO direction is the effect on equity markets.
--   went_its_way  positive and the 5-session move above 0, or negative and below 0; NULL when neutral or not matured
-- WHY (owner, 26 Nov 2026 15:34 Riyadh, in Arabic: "let us start with this too ... linked with BigQuery"): which of
-- the two readings says more about the next sessions - read in v_news_llm_eval (each reader alone) and
-- v_news_llm_duel (the days they disagree). (apply_bq_views 1.32.0)
-- Research only: no decision, gate or count reads this view. (views 1.32.0)
CREATE OR REPLACE VIEW `{{DS}}.v_news_llm_days`
OPTIONS (description = "TFB news read twice - the word lists and the language model - summed per day and set against the next 1 and 5 sessions: a name against the same-day US names, MACRO against the S&P 500 fund. Research view, never an order.")
AS
WITH base AS (
  SELECT IF(is_macro, 'MACRO', 'COMPANY') AS kind, IFNULL(symbol, 'MACRO') AS symbol, news_date AS d,
         rule_dir, llm_dir
  FROM `{{DS}}.v_news_llm`
  WHERE news_date IS NOT NULL AND (is_macro OR ENDS_WITH(symbol, '.US'))
),
day_scores AS (
  SELECT kind, symbol, d, COUNT(*) AS headlines, SUM(rule_dir) AS rules_x, SUM(llm_dir) AS model_x
  FROM base
  GROUP BY kind, symbol, d
),
bars AS (
  SELECT symbol, date, close,
         LEAD(close, 1) OVER (PARTITION BY symbol ORDER BY date) AS c1,
         LEAD(close, 5) OVER (PARTITION BY symbol ORDER BY date) AS c5
  FROM (SELECT symbol, date, ANY_VALUE(COALESCE(adj_close, close)) AS close
        FROM `{{DS}}.symbol_bars_daily` WHERE close IS NOT NULL GROUP BY symbol, date)
),
peer AS (
  SELECT date, AVG((c1 / close - 1) * 100) AS p1, AVG((c5 / close - 1) * 100) AS p5
  FROM bars
  WHERE ENDS_WITH(symbol, '.US') AND close > 0
  GROUP BY date
),
spy AS (
  SELECT date, close, LEAD(close, 1) OVER (ORDER BY date) AS c1, LEAD(close, 5) OVER (ORDER BY date) AS c5
  FROM (SELECT date, ANY_VALUE(COALESCE(adj_close, close)) AS close
        FROM `{{DS}}.market_series_daily` WHERE symbol = 'SPY' AND close IS NOT NULL GROUP BY date)
),
name_entry AS (
  SELECT y.symbol, y.d,
         ARRAY_AGG(STRUCT(b.date AS date, (b.c1 / b.close - 1) * 100 AS r1, (b.c5 / b.close - 1) * 100 AS r5)
                   ORDER BY b.date ASC LIMIT 1)[OFFSET(0)] AS e
  FROM day_scores AS y
  JOIN bars AS b ON b.symbol = y.symbol AND b.date > y.d AND b.date <= DATE_ADD(y.d, INTERVAL 6 DAY)
  WHERE y.kind = 'COMPANY' AND b.close > 0
  GROUP BY y.symbol, y.d
),
macro_entry AS (
  SELECT y.d,
         ARRAY_AGG(STRUCT(s.date AS date, (s.c1 / s.close - 1) * 100 AS r1, (s.c5 / s.close - 1) * 100 AS r5)
                   ORDER BY s.date ASC LIMIT 1)[OFFSET(0)] AS e
  FROM day_scores AS y
  JOIN spy AS s ON s.date > y.d AND s.date <= DATE_ADD(y.d, INTERVAL 6 DAY)
  WHERE y.kind = 'MACRO' AND s.close > 0
  GROUP BY y.d
),
moves AS (
  SELECT y.kind, y.symbol, y.d, y.headlines, y.rules_x, y.model_x,
         IF(y.kind = 'MACRO', m.e.date, n.e.date) AS entry_date,
         IF(y.kind = 'MACRO', m.e.r1, n.e.r1 - p.p1) AS move_1d_pp,
         IF(y.kind = 'MACRO', m.e.r5, n.e.r5 - p.p5) AS move_5d_pp
  FROM day_scores AS y
  LEFT JOIN name_entry AS n ON y.kind = 'COMPANY' AND n.symbol = y.symbol AND n.d = y.d
  LEFT JOIN peer AS p ON p.date = n.e.date
  LEFT JOIN macro_entry AS m ON y.kind = 'MACRO' AND m.d = y.d
),
read_twice AS (
  SELECT r.reader, mv.kind, mv.symbol, mv.d AS news_date, mv.headlines, r.x AS score,
         CASE WHEN r.x >= 1 THEN 'positive' WHEN r.x <= -1 THEN 'negative' ELSE 'neutral' END AS class,
         mv.entry_date, ROUND(mv.move_1d_pp, 4) AS move_1d_pp, ROUND(mv.move_5d_pp, 4) AS move_5d_pp
  FROM moves AS mv
  CROSS JOIN UNNEST([STRUCT('rules' AS reader, mv.rules_x AS x), STRUCT('model' AS reader, mv.model_x AS x)]) AS r
)
SELECT
  t.*,
  CASE
    WHEN t.move_5d_pp IS NULL THEN NULL
    WHEN t.class = 'positive' THEN t.move_5d_pp > 0
    WHEN t.class = 'negative' THEN t.move_5d_pp < 0
  END AS went_its_way
FROM read_twice AS t;
