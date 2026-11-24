-- v_news_now_eval: each hourly news reading of scripts/news_now.py scored against what the price did next.
-- WHY (owner, 24 Nov 2026 10:10 Riyadh, in Arabic: the news analysis should act on the reading of a stock's price now;
-- 12:17: "ok let's start the next stage"). News Now reads each holding and Top 10 name every hour of the US session;
-- this view is its score, so the hourly reading earns (or loses) trust the same way every TFB signal does.
-- THE EVENT (fixed on 24 Nov 2026, before any outcome was read): per US symbol and session, the FIRST hourly reading of
-- that session (open or closed today) whose news was refreshed in that run and whose state carries an expected move
-- (+1 up / -1 down against SPY) - the earliest moment the reading was on the owner's page. Later readings of the same
-- session never replace it, so a state that appears only after the move cannot score itself.
-- THE OUTCOMES, against SPY on the adjusted basis:
--   rest_pp   from the price at that reading to the session's close (SPY from its own price at the same moment)
--   f1_pp     from the session's close to the next session's close
--   f5_pp     from the session's close to the close five sessions later
-- Every outcome starts at or after the reading; nothing before it is scored.
-- Research only: no decision, gate or count reads this view. The live system never reads BigQuery.
CREATE OR REPLACE VIEW `{{DS}}.v_news_now_eval`
OPTIONS (description = "TFB hourly news reading scored (views 1.28.0): per US symbol and session, the first News Now reading with refreshed news and an expected move, then the stock against SPY for the rest of that session, the next session and the next five. Research only: no decision, gate or count reads this view.")
AS
WITH spy AS (
  SELECT date, COALESCE(adj_close, close) AS close
  FROM `{{DS}}.market_series_daily`
  WHERE symbol = 'SPY' AND close > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY captured_at DESC) = 1
),
spy_seq AS (
  SELECT date, close, LEAD(close) OVER w AS s1, LEAD(close, 5) OVER w AS s5
  FROM spy
  WINDOW w AS (ORDER BY date)
),
r AS (
  SELECT symbol, session_date, prev_date, captured_at, listed_as, state, expected, trend, trend_fit, agreement,
    recommendation, news_items, news_score, reaction_z, day_z, price, spy_change_pct
  FROM `{{DS}}.news_now_log`
  WHERE ENDS_WITH(symbol, '.US') AND session_state IN ('open', 'closed today') AND news_coverage = 'fresh'
    AND state IS NOT NULL AND expected != 0 AND session_date IS NOT NULL AND price > 0
    AND spy_change_pct IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, session_date ORDER BY captured_at) = 1
),
bars AS (
  SELECT b.symbol, b.date, COALESCE(b.adj_close, b.close) AS close
  FROM `{{DS}}.symbol_bars_daily` b
  JOIN (SELECT DISTINCT symbol FROM r) k ON k.symbol = b.symbol
  WHERE b.close > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.symbol, b.date ORDER BY b.captured_at DESC) = 1
),
seq AS (
  SELECT symbol, date, close, LEAD(close) OVER w AS c1, LEAD(close, 5) OVER w AS c5
  FROM bars
  WINDOW w AS (PARTITION BY symbol ORDER BY date)
)
SELECT
  r.symbol, r.session_date, r.listed_as, r.captured_at AS first_read_at,
  'forward from 2026-11-24' AS period,
  r.state, r.expected, r.trend, r.trend_fit, r.recommendation, r.agreement,
  r.news_items, r.news_score, r.reaction_z, r.day_z,
  ROUND(100 * (SAFE_DIVIDE(q.close, r.price) - SAFE_DIVIDE(sd.close, sp.close * (1 + r.spy_change_pct / 100))), 2)
    AS rest_pp,
  ROUND(100 * (SAFE_DIVIDE(q.c1, q.close) - SAFE_DIVIDE(sd.s1, sd.close)), 2) AS f1_pp,
  ROUND(100 * (SAFE_DIVIDE(q.c5, q.close) - SAFE_DIVIDE(sd.s5, sd.close)), 2) AS f5_pp
FROM r
LEFT JOIN seq q ON q.symbol = r.symbol AND q.date = r.session_date
LEFT JOIN spy_seq sd ON sd.date = r.session_date
LEFT JOIN spy sp ON sp.date = r.prev_date;
