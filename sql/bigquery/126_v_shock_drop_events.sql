-- v_shock_drop_events: a shadow rule, recorded and scored but never acted on - a large US company whose price falls
-- 30 % or more within 15 sessions, read two ways: buy at the first close of the fall (A, 'at the drop') or wait for
-- the first close above the day before's high within the next 10 sessions (B, 'after a confirmed turn').
-- WHY (owner, 24 Nov 2026 00:58 Riyadh, after NVDA fell 39 % in 13 sessions on its CEO's resignation and rose 31 %
-- the day he was reinstated): "a company like that, the first in the world, is not allowed to collapse ... many
-- people would buy at that low price ... if we expected that, the stock became attractive to buy". The answer of
-- that night: the direction was right, the timing hard (it fell 20 % more after the President's remark of 16 Nov)
-- - so the idea is logged and measured, not switched on (owner's ground rules, 21 Nov 2026).
-- THE RULE (fixed on 24 Nov 2026, before reading any result):
--   universe    US equities (from v_universe: equity on every page of a night, never a Mutual_Funds row) among
--               the 100 largest by market cap on any night from 17 Nov 2026 (today's included), so a company that
--               leaves the 100 after its fall keeps its signals (Codex P2 on #1068) - that have daily bars in symbol_bars_daily (stored as NVDA.US or, from the outcome set,
--               as NVDA - one series per company); the history uses today's size, so it favours the companies
--               that are large NOW (survivorship) and is read as context, never as evidence
--   the fall    close / the highest close of the last 16 sessions (today and 15 before) - 1 <= -30 %
--   A           the first close of a fall with no other fall close in the 20 sessions before
--   B           the first close above the previous session's high within 10 sessions after a fall close, with no
--               other B in the 20 sessions before
--   outcome     the close h = 5, 10, 20 sessions after the signal close against SPY over the same sessions (both
--               adjusted closes), in percentage points; NULL until h sessions have passed
-- PERIOD: signals before 24 Nov 2026 are 'history, rule fixed before reading it' (with the survivorship caveat
-- above); from 24 Nov a signal is 'forward from 2026-11-24' only when the company was in the 100 on a night 1 to 5
-- days BEFORE the signal (its size known before the signal close, no look-ahead), else 'forward, not top 100
-- before the signal' - never evidence.
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_shock_drop_events`
OPTIONS (description = "TFB shadow rule (views 1.26.0): large US companies falling 30 % or more within 15 sessions - buying at the first close of the fall (A) or after the first confirmed turn (B), each scored against SPY over the next 5, 10 and 20 sessions - research only")
AS
WITH uni AS (
  SELECT snapshot_date, symbol, MAX(market_cap) AS mc
  FROM `{{DS}}.v_universe`
  WHERE ENDS_WITH(symbol, '.US') AND snapshot_date >= DATE '2026-11-17'
  GROUP BY snapshot_date, symbol
  HAVING LOGICAL_AND(asset_class = 'EQUITY') AND LOGICAL_AND(source_page != 'Mutual_Funds') AND MAX(market_cap) > 0
),
top100 AS (
  SELECT snapshot_date, symbol
  FROM uni
  QUALIFY ROW_NUMBER() OVER (PARTITION BY snapshot_date ORDER BY mc DESC, symbol) <= 100
),
big AS (
  SELECT DISTINCT symbol, REGEXP_REPLACE(symbol, r'\.US$', '') AS base
  FROM top100
),
spy AS (
  SELECT date, COALESCE(adj_close, close) AS s_close
  FROM `{{DS}}.market_series_daily`
  WHERE symbol = 'SPY'
),
bars AS (
  SELECT g.symbol, b.date, COALESCE(b.adj_close, b.close) AS close,
    b.high * SAFE_DIVIDE(COALESCE(b.adj_close, b.close), b.close) AS high
  FROM `{{DS}}.symbol_bars_daily` b
  JOIN big g ON g.base = REGEXP_REPLACE(b.symbol, r'\.US$', '')
  WHERE b.close > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY g.symbol, b.date ORDER BY b.captured_at DESC, b.symbol) = 1
),
seq AS (
  SELECT x.*, s.s_close,
    ROW_NUMBER() OVER w AS i,
    SAFE_DIVIDE(x.close, MAX(x.close) OVER (w ROWS BETWEEN 15 PRECEDING AND CURRENT ROW)) - 1 AS dd,
    LAG(x.high) OVER w AS prev_high
  FROM bars x
  LEFT JOIN spy s USING (date)
  WINDOW w AS (PARTITION BY x.symbol ORDER BY x.date)
),
flag AS (
  SELECT q.*,
    q.dd <= -0.30 AS fall,
    IFNULL(MAX(IF(q.dd <= -0.30, 1, 0)) OVER (w ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING), 0) = 1 AS fall_before,
    IFNULL(MAX(IF(q.dd <= -0.30, 1, 0)) OVER (w ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING), 0) = 1 AS armed,
    MIN(q.dd) OVER (w ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS worst_dd,
    LEAD(q.close, 5) OVER w AS c5, LEAD(q.close, 10) OVER w AS c10, LEAD(q.close, 20) OVER w AS c20,
    LEAD(q.s_close, 5) OVER w AS s5, LEAD(q.s_close, 10) OVER w AS s10, LEAD(q.s_close, 20) OVER w AS s20
  FROM seq q
  WINDOW w AS (PARTITION BY q.symbol ORDER BY q.date)
),
sig AS (
  SELECT f.*, arm
  FROM flag f,
    UNNEST([
      IF(f.fall AND NOT f.fall_before, 'A at the drop', NULL),
      IF(f.armed AND f.close > f.prev_high, 'B after a confirmed turn', NULL)
    ]) AS arm
  WHERE arm IS NOT NULL
),
kept AS (
  SELECT s.*
  FROM sig s
  QUALIFY IFNULL(s.i - LAG(s.i) OVER (PARTITION BY s.symbol, s.arm ORDER BY s.date), 999) > 20
),
pit AS (
  SELECT k.*, EXISTS (
      SELECT 1 FROM top100 m
      WHERE m.symbol = k.symbol AND m.snapshot_date BETWEEN DATE_SUB(k.date, INTERVAL 5 DAY) AND DATE_SUB(k.date, INTERVAL 1 DAY)
    ) AS member_before
  FROM kept k
)
SELECT
  symbol, arm, date AS signal_date,
  CASE
    WHEN date < DATE '2026-11-24' THEN 'history, rule fixed before reading it'
    WHEN member_before THEN 'forward from 2026-11-24'
    ELSE 'forward, not top 100 before the signal'
  END AS period,
  ROUND(close, 2) AS entry_close,
  ROUND(100 * worst_dd, 1) AS fall_pct,
  ROUND(100 * (SAFE_DIVIDE(c5, close) - 1), 2) AS r5_pct,
  ROUND(100 * (SAFE_DIVIDE(c5, close) - SAFE_DIVIDE(s5, s_close)), 2) AS f5_pp,
  ROUND(100 * (SAFE_DIVIDE(c10, close) - SAFE_DIVIDE(s10, s_close)), 2) AS f10_pp,
  ROUND(100 * (SAFE_DIVIDE(c20, close) - SAFE_DIVIDE(s20, s_close)), 2) AS f20_pp
FROM pit;
