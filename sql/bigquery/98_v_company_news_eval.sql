-- v_company_news_eval: does the re-read company news say anything about the next sessions? One row per signal and
-- class, refreshed by itself (a view). The scoreboard of v_symbol_news_class, kept apart from v_news_signal_eval so
-- that the earlier scoreboard stays as it was read.
-- WHY (owner, 20 Nov 2026 12:54 Riyadh, in Arabic: "let us do the expected improvement to the news classification,
-- improve it and re-classify it"). A new classification is only an improvement once it is measured; nothing may move
-- a decision on it before this view reads CANDIDATE and a forward test agrees.
-- METHOD (the same for both signals, so they can be compared):
--   names      US names only (symbol ending '.US'), so each return can be set against one market, the S&P 500 (SPY)
--   news day   the Riyadh date of the headline (news_date of v_symbol_news_class)
--   entry      the close of the first session AFTER the news day - never the news day's own close, which could come
--              before the headline (no look-ahead); the first bar within 6 calendar days, else the day is not scored
--   outcome    the name's adjusted return over the next 1 and 5 sessions from that close, minus SPY's over the same
--              sessions, in percentage points; and (apply_bq_views 1.17.0) minus the same-day average of every US
--              name in symbol_bars_daily over the same sessions from the same close ("peers")
--   signals    company_news: the day's summed score of the headlines about the company (relevance 'company'),
--                positive >= 1, negative <= -1, neutral otherwise (a day with company headlines that net to little)
--              provider_sentiment: the provider's mean polarity of every headline filed under the name that day,
--                positive >= 0.15, negative <= -0.15, neutral otherwise (the reading v_news_signal_eval scores)
-- OUTPUT (one row per signal x class):
--   signal, class, days, matured_1d, matured_5d, mean_1d_excess_pp, mean_5d_excess_pp, beat_5d_pct (share of matured
--   days ahead of SPY), corr_5d (the signal's raw score against the 5-session excess, all its days), share_pct,
--   first_day, last_day, reading, and (1.17.0) mean_1d_peer_pp, mean_5d_peer_pp, matured_5d_peer, beat_5d_peer_pct,
--   corr_5d_peer (the same against the same-day US names) and peers (how many names the day's average holds, on
--   average). The reading is taken from the peer columns:
--     'NOT MEASURABLE (one class)', 'BIASED (class > 90 %)', 'NO EDGE' (corr_5d_peer <= 0.05, or positive not
--     above negative on mean_5d_peer_pp), 'THIN' (a class with fewer than 60 matured_5d_peer days), else
--     'CANDIDATE (forward test)'. Never "proven". The name itself is one of the day's peers (about 1 in 490). Since
--     1.17.0 a day with no SPY bar keeps its peer figures (its SPY columns are empty) instead of being dropped.
-- First read (20 Nov 2026, 2 Oct - 20 Nov): company_news positive 58 days +1.14 pp, neutral 213 days +1.16 pp, negative
-- 6 days +3.19 pp, corr 0.02 - balanced at last (the provider read 455 of 464 days positive) but no edge yet.
-- WHY THE PEERS (apply_bq_views 1.17.0; owner, 21 Nov 2026 02:20 Riyadh, in Arabic: "let us also review the news
-- analysis"): every class beat SPY - neutral as much as positive - because the US names of the archive beat the index
-- in a falling market, not because of their news. Against the same-day average of about 490 US names (21 Nov, news
-- days 2 Oct - 18 Nov): positive +0.69 pp on 52 matured days, neutral +0.67 pp on 186, negative +2.55 pp on 5,
-- corr 0.019 - NO EDGE: the classes still do not separate, and the index comparison had added about half a point to
-- each. A news day of any class still runs ahead of its peers (about 62 % of matured days); that is common to every
-- class, so it more likely belongs to the names that draw news than to what the news says (not tested further
-- here). The SPY columns stay for continuity; the reading comes from the peer columns.
-- Research view: the live system never reads BigQuery; no decision, gate or order depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_company_news_eval`
OPTIONS (description = "TFB scoreboard of the re-read company news: each class against the next 1 and 5 sessions of the name, entry the first close after the news day, return against the same-day average of the US names (the reading) and against the S&P 500, US names, and the provider sentiment scored the same way beside it. Research view, never an order.")
AS
WITH nd AS (
  SELECT symbol, news_date AS d, SUM(score) AS x
  FROM `{{DS}}.v_symbol_news_class`
  WHERE relevance = 'company' AND ENDS_WITH(symbol, '.US') AND news_date IS NOT NULL
  GROUP BY symbol, news_date
),
od AS (
  SELECT symbol, news_date AS d, AVG(provider_polarity) AS x
  FROM `{{DS}}.v_symbol_news_class`
  WHERE provider_polarity IS NOT NULL AND ENDS_WITH(symbol, '.US') AND news_date IS NOT NULL
  GROUP BY symbol, news_date
),
name_days AS (
  SELECT 'company_news' AS signal, symbol, d, x FROM nd
  UNION ALL
  SELECT 'provider_sentiment', symbol, d, x FROM od
),
bars AS (
  SELECT symbol, date, close,
         LEAD(close, 1) OVER (PARTITION BY symbol ORDER BY date) AS c1,
         LEAD(close, 5) OVER (PARTITION BY symbol ORDER BY date) AS c5
  FROM (SELECT symbol, date, ANY_VALUE(COALESCE(adj_close, close)) AS close
        FROM `{{DS}}.symbol_bars_daily` WHERE close IS NOT NULL GROUP BY symbol, date)
),
peer AS (
  SELECT date, AVG((c1 / close - 1) * 100) AS p1, AVG((c5 / close - 1) * 100) AS p5, COUNT(*) AS n_peers
  FROM bars
  WHERE ENDS_WITH(symbol, '.US') AND close > 0
  GROUP BY date
),
spy AS (
  SELECT date, close, LEAD(close, 1) OVER (ORDER BY date) AS c1, LEAD(close, 5) OVER (ORDER BY date) AS c5
  FROM (SELECT date, ANY_VALUE(COALESCE(adj_close, close)) AS close
        FROM `{{DS}}.market_series_daily` WHERE symbol = 'SPY' AND close IS NOT NULL GROUP BY date)
),
entry AS (
  SELECT y.signal, y.symbol, y.d, y.x,
         ARRAY_AGG(STRUCT(b.date AS date, (b.c1 / b.close - 1) * 100 AS r1, (b.c5 / b.close - 1) * 100 AS r5)
                   ORDER BY b.date ASC LIMIT 1)[OFFSET(0)] AS e
  FROM name_days AS y
  JOIN bars AS b ON b.symbol = y.symbol AND b.date > y.d AND b.date <= DATE_ADD(y.d, INTERVAL 6 DAY)
  GROUP BY y.signal, y.symbol, y.d, y.x
),
obs AS (
  SELECT n.signal, n.symbol, n.d, n.x,
         n.e.r1 - (s.c1 / s.close - 1) * 100 AS ex1,
         n.e.r5 - (s.c5 / s.close - 1) * 100 AS ex5,
         n.e.r1 - p.p1 AS px1,
         n.e.r5 - p.p5 AS px5,
         p.n_peers,
         CASE
           WHEN n.signal = 'company_news' AND n.x >= 1 THEN 'positive'
           WHEN n.signal = 'company_news' AND n.x <= -1 THEN 'negative'
           WHEN n.signal = 'company_news' THEN 'neutral'
           WHEN n.x >= 0.15 THEN 'positive'
           WHEN n.x <= -0.15 THEN 'negative'
           ELSE 'neutral'
         END AS class
  FROM entry AS n
  LEFT JOIN spy AS s ON s.date = n.e.date
  JOIN peer AS p ON p.date = n.e.date
),
sig_corr AS (SELECT signal, CORR(x, ex5) AS c, CORR(x, px5) AS cp FROM obs GROUP BY signal),
class_rows AS (
  SELECT o.signal, o.class, COUNT(*) AS days,
         COUNTIF(o.ex1 IS NOT NULL) AS matured_1d, COUNTIF(o.ex5 IS NOT NULL) AS matured_5d,
         ROUND(AVG(o.ex1), 2) AS mean_1d_excess_pp, ROUND(AVG(o.ex5), 2) AS mean_5d_excess_pp,
         ROUND(SAFE_DIVIDE(COUNTIF(o.ex5 > 0), COUNTIF(o.ex5 IS NOT NULL)) * 100, 1) AS beat_5d_pct,
         ROUND(AVG(o.px1), 2) AS mean_1d_peer_pp, ROUND(AVG(o.px5), 2) AS mean_5d_peer_pp,
         COUNTIF(o.px5 IS NOT NULL) AS matured_5d_peer,
         ROUND(SAFE_DIVIDE(COUNTIF(o.px5 > 0), COUNTIF(o.px5 IS NOT NULL)) * 100, 1) AS beat_5d_peer_pct,
         CAST(ROUND(AVG(o.n_peers)) AS INT64) AS peers,
         MIN(o.d) AS first_day, MAX(o.d) AS last_day
  FROM obs AS o
  GROUP BY o.signal, o.class
),
shaped AS (
  SELECT r.*, ROUND(c.c, 3) AS corr_5d, ROUND(c.cp, 3) AS corr_5d_peer,
         ROUND(r.days / SUM(r.days) OVER (PARTITION BY r.signal) * 100, 1) AS share_pct,
         COUNT(*) OVER (PARTITION BY r.signal) AS n_classes,
         MAX(r.days / 1.0) OVER (PARTITION BY r.signal) / SUM(r.days) OVER (PARTITION BY r.signal) AS top_share,
         MIN(r.matured_5d_peer) OVER (PARTITION BY r.signal) AS min_matured,
         MAX(IF(r.class = 'positive', r.mean_5d_peer_pp, NULL)) OVER (PARTITION BY r.signal) AS hi_mean,
         MAX(IF(r.class = 'negative', r.mean_5d_peer_pp, NULL)) OVER (PARTITION BY r.signal) AS lo_mean
  FROM class_rows AS r
  LEFT JOIN sig_corr AS c ON c.signal = r.signal
)
SELECT signal, class, days, matured_1d, matured_5d, mean_1d_excess_pp, mean_5d_excess_pp, beat_5d_pct, corr_5d,
       share_pct, first_day, last_day,
       CASE
         WHEN n_classes < 2 THEN 'NOT MEASURABLE (one class)'
         WHEN top_share > 0.9 THEN 'BIASED (class > 90 %)'
         WHEN corr_5d_peer IS NULL OR corr_5d_peer <= 0.05 THEN 'NO EDGE'
         WHEN hi_mean IS NULL OR lo_mean IS NULL OR hi_mean <= lo_mean THEN 'NO EDGE'
         WHEN min_matured < 60 THEN 'THIN'
         ELSE 'CANDIDATE (forward test)'
       END AS reading,
       mean_1d_peer_pp, mean_5d_peer_pp, matured_5d_peer, beat_5d_peer_pct, corr_5d_peer, peers
FROM shaped;
