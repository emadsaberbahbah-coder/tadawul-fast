-- v_news_trend_daily: every US stock TFB follows the news of, every US session from 1 Oct 2026 - what its company
-- news said, how its price answered against the market, which way it was trending before, and what the research says
-- usually follows. One row per symbol and session; the record of each reading is v_news_trend_learning.
-- WHY (owner, 23 Nov 2026 12:34 Riyadh): "deep understanding of information ... analysis news ... using data and
-- information and in same time use strategy and trend". The news views read a headline alone (v_company_news_eval:
-- no edge) and the trend views read a price alone. Research reads them together: a price move WITH company news tends
-- to continue and a large move WITHOUT news tends to reverse (Chan 2003, JFE 70), news the price has not yet answered
-- tends to drift its own way (Tetlock 2011, Lopez-Lira and Tang 2023), and good news the price refuses says more than
-- the news. This view writes those readings down, per session, before anyone knows what followed.
-- THE NEWS of a session t: the company headlines filed under the symbol (v_symbol_news_class, relevance 'company',
-- score = direction x weight) and the company items of the news gate (v_news_items, one per story cluster and day,
-- direction x 2 / 1 / 0.5 for severity H / M / L), published after the previous session's date up to t's date (a
-- weekend item belongs to Monday). news_score = their sum, news_items = their count.
-- THE ANSWER of the price: reaction_pp = the stock's return from the close before t to the close after t, minus SPY's
-- over the same two sessions (until the session after t exists: to t's close); reaction_z = reaction_pp over the
-- stock's own daily volatility (20 sessions before t) x SQRT(2). day_pp = t's own move against SPY (log points x
-- 100), day_z = day_pp over that volatility.
-- THE TREND before t (closes up to the session before t): 'up' above its 50-session average and above its close 20
-- sessions earlier, 'down' below both, else 'mixed' ('unknown' under 40 sessions of history).
-- THE STATE, fixed on 23 Nov 2026 from the research above, before reading any of this data (expected: +1 up, -1 down,
-- 0 no expectation, against SPY over the next sessions):
--   good news, price agreed      news_score > 0 and reaction_z >= 1             +1  (a move with news continues)
--   bad news, price agreed       news_score < 0 and reaction_z <= -1            -1
--   good news, price fell        news_score > 0 and reaction_z <= -1            -1  (the market read it as worse)
--   bad news, price rose         news_score < 0 and reaction_z >= 1             +1  (the bad news was already priced)
--   news, price quiet            news_score <> 0 and ABS(reaction_z) < 1        sign of the news (drift)
--   unsigned news, big move      news_score = 0, items > 0, ABS(reaction_z) >= 2   the move's own sign
--   routine news                 news_score = 0, items > 0, ABS(reaction_z) < 2    0
--   big rise, no news            no company news from the session before t to the session after, day_z >= 2.5   -1
--   big fall, no news            the same with day_z <= -2.5                    +1  (a move without news reverses)
--   quiet                        everything else                                0   (not an event)
-- trend_fit: 'with the trend' when expected is +1 in an up trend or -1 in a down trend, 'against the trend' for the
-- reverse, else 'no clear trend'.
-- THE OUTCOME, measured from the close of the session AFTER t (so the state is known before it starts, no look-ahead):
-- f1_pp, f5_pp, f10_pp, f20_pp = the stock's return over the next 1 / 5 / 10 / 20 sessions minus SPY's, NULL until
-- they exist. period: 'forward from 2026-11-24' for sessions from 24 Nov 2026, else 'history, rules fixed before
-- reading it'.
-- COVERAGE: the US symbols with at least one company headline in either feed (the holdings, the boards and the ROI
-- leaders: the EODHD news budget covers about 100 names); prices from symbol_bars_daily (adjusted closes).
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_news_trend_daily`
OPTIONS (description = "TFB news and trend by session (views 1.25.0): for each US stock TFB follows the company news of, each session from 1 Oct 2026 - the company news score and items, the price answer against SPY over two sessions and its z, the trend before, a state fixed from research before the data was read (news the price agreed with, refused or did not answer yet, big moves without news) with its expected direction and fit with the trend, and the returns against SPY over the next 1, 5, 10 and 20 sessions - research only")
AS
WITH spy AS (
  SELECT date, close
  FROM `{{DS}}.market_series_daily`
  WHERE symbol = 'SPY' AND close > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY captured_at DESC) = 1
),
cls AS (
  SELECT symbol, news_date, published_at, title, event_type, CAST(direction AS INT64) AS direction,
    CAST(score AS FLOAT64) AS score, 'company feed' AS feed
  FROM `{{DS}}.v_symbol_news_class`
  WHERE relevance = 'company' AND ENDS_WITH(symbol, '.US') AND news_date >= DATE '2026-09-20'
),
itm AS (
  SELECT symbol, news_date, published_at, headline AS title, event_type, CAST(direction AS INT64) AS direction,
    CAST(direction AS FLOAT64) * CASE severity WHEN 'H' THEN 2.0 WHEN 'M' THEN 1.0 ELSE 0.5 END AS score,
    'news gate' AS feed
  FROM `{{DS}}.v_news_items`
  WHERE NOT is_macro AND symbol IS NOT NULL AND ENDS_WITH(symbol, '.US') AND news_date >= DATE '2026-09-20'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, news_date, IFNULL(cluster, item_id) ORDER BY published_at) = 1
),
news AS (
  SELECT * FROM cls
  UNION ALL
  SELECT * FROM itm
),
covered AS (
  SELECT DISTINCT symbol FROM news
),
bars AS (
  SELECT b.symbol, b.date, COALESCE(b.adj_close, b.close) AS close, b.volume
  FROM `{{DS}}.symbol_bars_daily` b
  JOIN covered c ON c.symbol = b.symbol
  WHERE b.close > 0 AND b.date >= DATE '2026-06-01'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.symbol, b.date ORDER BY b.captured_at DESC) = 1
),
px AS (
  SELECT b.symbol, b.date, b.close, b.volume, s.close AS spy,
    LN(b.close / LAG(b.close) OVER w) - LN(s.close / LAG(s.close) OVER w) AS x
  FROM bars b
  JOIN spy s ON s.date = b.date
  WINDOW w AS (PARTITION BY b.symbol ORDER BY b.date)
),
seq AS (
  SELECT px.*,
    LAG(date) OVER w AS prev_date,
    LAG(close) OVER w AS c_prev, LAG(spy) OVER w AS s_prev,
    LEAD(close) OVER w AS c_next, LEAD(spy) OVER w AS s_next,
    LEAD(close, 2) OVER w AS c_f1, LEAD(spy, 2) OVER w AS s_f1,
    LEAD(close, 6) OVER w AS c_f5, LEAD(spy, 6) OVER w AS s_f5,
    LEAD(close, 11) OVER w AS c_f10, LEAD(spy, 11) OVER w AS s_f10,
    LEAD(close, 21) OVER w AS c_f20, LEAD(spy, 21) OVER w AS s_f20,
    STDDEV(x) OVER (w ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) * 100 AS sd_pp,
    AVG(close) OVER (w ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING) AS sma50_prev,
    COUNT(close) OVER (w ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING) AS n50_prev,
    LAG(close, 21) OVER w AS c_prev21,
    AVG(volume) OVER (w ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS vol20_prev
  FROM px
  WINDOW w AS (PARTITION BY symbol ORDER BY date)
),
day_news AS (
  SELECT s.symbol, s.date,
    COUNT(n.symbol) AS news_items,
    IFNULL(SUM(n.score), 0) AS news_score,
    COUNTIF(ABS(n.score) >= 2) AS hard_items,
    ARRAY_AGG(IF(n.symbol IS NULL, NULL, STRUCT(n.news_date, n.event_type, n.direction, n.score, n.title, n.feed))
              IGNORE NULLS ORDER BY ABS(n.score) DESC, n.published_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS top
  FROM seq s
  LEFT JOIN news n ON n.symbol = s.symbol AND n.news_date <= s.date
    AND (s.prev_date IS NULL OR n.news_date > s.prev_date)
  GROUP BY s.symbol, s.date
),
j AS (
  SELECT s.*, d.news_items, d.news_score, d.hard_items, d.top,
    LAG(d.news_items) OVER w AS items_before, LEAD(d.news_items) OVER w AS items_after
  FROM seq s
  JOIN day_news d USING (symbol, date)
  WINDOW w AS (PARTITION BY s.symbol ORDER BY s.date)
),
m AS (
  SELECT j.*,
    100 * (SAFE_DIVIDE(COALESCE(c_next, close), c_prev) - SAFE_DIVIDE(COALESCE(s_next, spy), s_prev)) AS reaction_pp,
    SAFE_DIVIDE(100 * (SAFE_DIVIDE(COALESCE(c_next, close), c_prev) - SAFE_DIVIDE(COALESCE(s_next, spy), s_prev)),
                sd_pp * SQRT(2)) AS reaction_z,
    SAFE_DIVIDE(100 * x, sd_pp) AS day_z,
    CASE
      WHEN n50_prev < 40 OR c_prev21 IS NULL THEN 'unknown'
      WHEN c_prev > sma50_prev AND c_prev > c_prev21 THEN 'up'
      WHEN c_prev < sma50_prev AND c_prev < c_prev21 THEN 'down'
      ELSE 'mixed'
    END AS trend,
    SAFE_DIVIDE((volume + COALESCE(LEAD(volume) OVER (PARTITION BY symbol ORDER BY date), volume)) / 2, vol20_prev)
      AS volume_ratio
  FROM j
),
st AS (
  SELECT m.*,
    CASE
      WHEN sd_pp IS NULL OR sd_pp <= 0 OR c_prev IS NULL THEN 'quiet'
      WHEN news_items > 0 AND news_score > 0 AND reaction_z >= 1 THEN 'good news, price agreed'
      WHEN news_items > 0 AND news_score < 0 AND reaction_z <= -1 THEN 'bad news, price agreed'
      WHEN news_items > 0 AND news_score > 0 AND reaction_z <= -1 THEN 'good news, price fell'
      WHEN news_items > 0 AND news_score < 0 AND reaction_z >= 1 THEN 'bad news, price rose'
      WHEN news_items > 0 AND news_score != 0 THEN 'news, price quiet'
      WHEN news_items > 0 AND ABS(reaction_z) >= 2 THEN 'unsigned news, big move'
      WHEN news_items > 0 THEN 'routine news'
      WHEN IFNULL(items_before, 0) = 0 AND IFNULL(items_after, 0) = 0 AND day_z >= 2.5 THEN 'big rise, no news'
      WHEN IFNULL(items_before, 0) = 0 AND IFNULL(items_after, 0) = 0 AND day_z <= -2.5 THEN 'big fall, no news'
      ELSE 'quiet'
    END AS state
  FROM m
),
ex AS (
  SELECT st.*,
    CASE state
      WHEN 'good news, price agreed' THEN 1
      WHEN 'bad news, price agreed' THEN -1
      WHEN 'good news, price fell' THEN -1
      WHEN 'bad news, price rose' THEN 1
      WHEN 'news, price quiet' THEN CAST(SIGN(news_score) AS INT64)
      WHEN 'unsigned news, big move' THEN CAST(SIGN(reaction_z) AS INT64)
      WHEN 'big rise, no news' THEN -1
      WHEN 'big fall, no news' THEN 1
      ELSE 0
    END AS expected
  FROM st
)
SELECT
  symbol, date AS session_date,
  IF(date >= DATE '2026-11-24', 'forward from 2026-11-24', 'history, rules fixed before reading it') AS period,
  news_items, ROUND(news_score, 2) AS news_score, hard_items,
  top.event_type AS top_event, top.title AS top_headline, top.feed AS top_feed,
  ROUND(reaction_pp, 2) AS reaction_pp, ROUND(reaction_z, 2) AS reaction_z,
  ROUND(100 * x, 2) AS day_pp, ROUND(day_z, 2) AS day_z,
  c_next IS NOT NULL AS reaction_complete,
  ROUND(sd_pp, 2) AS daily_vol_pp, ROUND(volume_ratio, 2) AS volume_ratio,
  trend,
  state, expected,
  CASE
    WHEN expected = 0 OR trend IN ('mixed', 'unknown') THEN 'no clear trend'
    WHEN (expected = 1 AND trend = 'up') OR (expected = -1 AND trend = 'down') THEN 'with the trend'
    ELSE 'against the trend'
  END AS trend_fit,
  ROUND(100 * (SAFE_DIVIDE(c_f1, c_next) - SAFE_DIVIDE(s_f1, s_next)), 2) AS f1_pp,
  ROUND(100 * (SAFE_DIVIDE(c_f5, c_next) - SAFE_DIVIDE(s_f5, s_next)), 2) AS f5_pp,
  ROUND(100 * (SAFE_DIVIDE(c_f10, c_next) - SAFE_DIVIDE(s_f10, s_next)), 2) AS f10_pp,
  ROUND(100 * (SAFE_DIVIDE(c_f20, c_next) - SAFE_DIVIDE(s_f20, s_next)), 2) AS f20_pp
FROM ex
WHERE date >= DATE '2026-10-01';
