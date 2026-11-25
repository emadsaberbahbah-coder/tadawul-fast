-- v_stop_cause_events: a shadow judgment, recorded and scored but never acted on - when a stock falls 8 % in 15
-- sessions, did it fall WITH ITS MARKET or ON ITS OWN, and what happened next? The record behind the Exit Plan's
-- stop-judgment note (scripts/exit_plan.py 1.7.0, observe only), with the same limits.
-- WHY (owner, 26 Nov 2026 01:07 Riyadh, in Arabic, the day after a stop filled in the first seconds of a session
-- that closed higher): "is this fall temporary or lasting ... is it better to adjust the stop points or keep them
-- ... the system must have reason, wisdom and flexibility"; 01:37: "please do the required build now". The answer
-- of that night was a ladder - name the cause first, keep the stop on a stock's own weakness, wait on a fall with
-- the market inside a margin, always keep a hard limit - shown as a note, to be switched on only if its record
-- proves it (owner's ground rules, 21 Nov 2026: observe, log, score, review, then switch on what proved itself).
-- THE RULE (fixed on 26 Nov 2026):
--   universe    the US (.US) and Saudi (.SR) shares with daily bars in symbol_bars_daily - the names the project
--               followed, not the whole market - each beside its market's own reading in market_series_daily
--               (SPY, ^TASI.SR), adjusted closes, on the days both have a close
--   the fall    a close whose 15-session return is -8 % or lower when the day before was above -8 % (a crossing);
--               a share's first crossing is an event, and then the first crossing more than 20 sessions after the
--               last EVENT (Codex P2 on #1085: the cooldown runs from the last kept event, never from a crossing
--               that was itself left out). In SQL without recursion: per share the crossings as an array a; p1[i]
--               the position of the first crossing more than 20 sessions after crossing i (-1: none); p2 .. p32
--               that pointer doubled; r1 .. r6 the positions reached from the first crossing in up to 63 steps.
--               A fall beyond -60 % or a later move of 150 % is left out as a data error (an unadjusted split)
--   cause       the market's return over the same 15 sessions: 'with the market' at -5 % or lower, 'on its own' at
--               -1 % or better, else 'mixed'
--   verdict     'hard limit' at a fall of 15 % or more; 'wait' for a fall with the market that is at most 5 points
--               worse than it; else 'stop stands'
--   after       the close 5, 10 and 20 sessions after the event close, in % of it (selling at the fall = 0);
--               x20_pp the same 20 sessions against the market, in points; worst20_pct the lowest close on the way
--   the rule    for a 'wait' event, held until the first close where the fall from the base reaches 15 % or the
--               stock stands more than 5 points under its market since the base, else 20 sessions: rule_pct, with
--               rule_exit_session (NULL = held the 20 sessions)
-- PERIOD: events before 26 Nov 2026 are 'history, in-sample' - the limits were chosen after a first reading of this
-- history, so it is context, never evidence; from 26 Nov 2026 'forward from 2026-11-26'.
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_stop_cause_events`
OPTIONS (description = "TFB shadow judgment (views 1.30.0): US and Saudi shares falling 8 % in 15 sessions - the cause (with the market, on its own, mixed), the verdict of the rule under test, and the next 5, 10 and 20 sessions")
AS
WITH idx AS (
  SELECT IF(symbol = 'SPY', 'US', 'Saudi') AS market, date, COALESCE(adj_close, close) AS m_close
  FROM `{{DS}}.market_series_daily`
  WHERE symbol IN ('SPY', '^TASI.SR') AND close > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY captured_at DESC) = 1
),
bars AS (
  SELECT b.symbol, IF(ENDS_WITH(b.symbol, '.US'), 'US', 'Saudi') AS market, b.date,
    COALESCE(b.adj_close, b.close) AS close
  FROM `{{DS}}.symbol_bars_daily` b
  WHERE b.close > 0 AND (ENDS_WITH(b.symbol, '.US') OR ENDS_WITH(b.symbol, '.SR'))
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.symbol, b.date ORDER BY b.captured_at DESC) = 1
),
seq AS (
  SELECT x.symbol, x.market, x.date, x.close, i.m_close,
    ROW_NUMBER() OVER w AS n,
    LAG(x.close, 15) OVER w AS c_base, LAG(i.m_close, 15) OVER w AS m_base,
    LAG(x.close, 1) OVER w AS c_prev, LAG(x.close, 16) OVER w AS c_prev_base
  FROM bars x
  JOIN idx i ON i.market = x.market AND i.date = x.date
  WINDOW w AS (PARTITION BY x.symbol ORDER BY x.date)
),
ev AS (
  SELECT s.*,
    100 * (SAFE_DIVIDE(s.close, s.c_base) - 1) AS s15,
    100 * (SAFE_DIVIDE(s.m_close, s.m_base) - 1) AS m15
  FROM seq s
  WHERE s.c_base IS NOT NULL AND s.m_base IS NOT NULL AND s.c_prev_base IS NOT NULL
    AND 100 * (SAFE_DIVIDE(s.close, s.c_base) - 1) <= -8
    AND 100 * (SAFE_DIVIDE(s.c_prev, s.c_prev_base) - 1) > -8
),
cand AS (
  SELECT symbol, ARRAY_AGG(n ORDER BY n) AS a
  FROM ev
  GROUP BY symbol
),
h1 AS (
  SELECT symbol, a,
    ARRAY(SELECT IFNULL((SELECT MIN(k) FROM UNNEST(a) AS y WITH OFFSET k WHERE y > x + 20), -1)
          FROM UNNEST(a) AS x WITH OFFSET i ORDER BY i) AS p1
  FROM cand
),
h2 AS (SELECT *, ARRAY(SELECT IFNULL(p1[SAFE_OFFSET(v)], -1) FROM UNNEST(p1) AS v WITH OFFSET i ORDER BY i) AS p2 FROM h1),
h4 AS (SELECT *, ARRAY(SELECT IFNULL(p2[SAFE_OFFSET(v)], -1) FROM UNNEST(p2) AS v WITH OFFSET i ORDER BY i) AS p4 FROM h2),
h8 AS (SELECT *, ARRAY(SELECT IFNULL(p4[SAFE_OFFSET(v)], -1) FROM UNNEST(p4) AS v WITH OFFSET i ORDER BY i) AS p8 FROM h4),
h16 AS (SELECT *, ARRAY(SELECT IFNULL(p8[SAFE_OFFSET(v)], -1) FROM UNNEST(p8) AS v WITH OFFSET i ORDER BY i) AS p16 FROM h8),
h32 AS (SELECT *, ARRAY(SELECT IFNULL(p16[SAFE_OFFSET(v)], -1) FROM UNNEST(p16) AS v WITH OFFSET i ORDER BY i) AS p32 FROM h16),
g1 AS (SELECT *, ARRAY(SELECT DISTINCT v FROM UNNEST([0, p1[OFFSET(0)]]) AS v WHERE v >= 0) AS r1 FROM h32),
g2 AS (SELECT *, ARRAY(SELECT DISTINCT v FROM UNNEST(ARRAY_CONCAT(r1, ARRAY(SELECT IFNULL(p2[SAFE_OFFSET(u)], -1) FROM UNNEST(r1) AS u))) AS v WHERE v >= 0) AS r2 FROM g1),
g3 AS (SELECT *, ARRAY(SELECT DISTINCT v FROM UNNEST(ARRAY_CONCAT(r2, ARRAY(SELECT IFNULL(p4[SAFE_OFFSET(u)], -1) FROM UNNEST(r2) AS u))) AS v WHERE v >= 0) AS r3 FROM g2),
g4 AS (SELECT *, ARRAY(SELECT DISTINCT v FROM UNNEST(ARRAY_CONCAT(r3, ARRAY(SELECT IFNULL(p8[SAFE_OFFSET(u)], -1) FROM UNNEST(r3) AS u))) AS v WHERE v >= 0) AS r4 FROM g3),
g5 AS (SELECT *, ARRAY(SELECT DISTINCT v FROM UNNEST(ARRAY_CONCAT(r4, ARRAY(SELECT IFNULL(p16[SAFE_OFFSET(u)], -1) FROM UNNEST(r4) AS u))) AS v WHERE v >= 0) AS r5 FROM g4),
g6 AS (SELECT symbol, a, ARRAY(SELECT DISTINCT v FROM UNNEST(ARRAY_CONCAT(r5, ARRAY(SELECT IFNULL(p32[SAFE_OFFSET(u)], -1) FROM UNNEST(r5) AS u))) AS v WHERE v >= 0) AS r6 FROM g5),
chain AS (
  SELECT g.symbol, g.a[OFFSET(v)] AS n
  FROM g6 g, UNNEST(g.r6) AS v
),
kept AS (
  SELECT e.*
  FROM ev e
  JOIN chain USING (symbol, n)
),
path AS (
  SELECT k.symbol, k.date AS event_date, f.n - k.n AS d,
    100 * (SAFE_DIVIDE(f.close, k.close) - 1) AS r,
    100 * (SAFE_DIVIDE(f.close, k.c_base) - 1) AS s_base,
    100 * (SAFE_DIVIDE(f.close, k.c_base) - SAFE_DIVIDE(f.m_close, k.m_base)) AS x_base,
    100 * (SAFE_DIVIDE(f.close, k.close) - SAFE_DIVIDE(f.m_close, k.m_close)) AS x_evt
  FROM kept k
  JOIN seq f ON f.symbol = k.symbol AND f.n BETWEEN k.n + 1 AND k.n + 20
),
agg AS (
  SELECT symbol, event_date,
    MAX(d) AS sessions_after, MAX(ABS(r)) AS widest,
    MAX(IF(d = 5, r, NULL)) AS r5, MAX(IF(d = 10, r, NULL)) AS r10, MAX(IF(d = 20, r, NULL)) AS r20,
    MAX(IF(d = 20, x_evt, NULL)) AS x20,
    MIN(r) AS worst20,
    MIN(IF(s_base <= -15 OR x_base < -5, d, NULL)) AS exit_d
  FROM path
  GROUP BY symbol, event_date
),
rule AS (
  SELECT a.*, p.r AS exit_r
  FROM agg a
  LEFT JOIN path p ON p.symbol = a.symbol AND p.event_date = a.event_date AND p.d = a.exit_d
)
SELECT
  k.symbol, k.market, k.date AS event_date,
  CASE WHEN k.date < DATE '2026-11-26' THEN 'history, in-sample' ELSE 'forward from 2026-11-26' END AS period,
  ROUND(k.s15, 2) AS stock_15s_pct, ROUND(k.m15, 2) AS market_15s_pct, ROUND(k.s15 - k.m15, 2) AS excess_pp,
  CASE WHEN k.m15 <= -5 THEN 'with the market' WHEN k.m15 >= -1 THEN 'on its own' ELSE 'mixed' END AS cause,
  CASE
    WHEN k.s15 <= -15 THEN 'hard limit'
    WHEN k.m15 <= -5 AND k.s15 - k.m15 >= -5 THEN 'wait'
    ELSE 'stop stands'
  END AS verdict,
  r.sessions_after,
  ROUND(r.r5, 2) AS r5_pct, ROUND(r.r10, 2) AS r10_pct, ROUND(r.r20, 2) AS r20_pct,
  ROUND(r.x20, 2) AS x20_pp, ROUND(r.worst20, 2) AS worst20_pct,
  r.exit_d AS rule_exit_session,
  ROUND(CASE WHEN r.exit_d IS NOT NULL THEN r.exit_r WHEN r.sessions_after >= 20 THEN r.r20 END, 2) AS rule_pct
FROM kept k
LEFT JOIN rule r ON r.symbol = k.symbol AND r.event_date = k.date
WHERE k.s15 > -60 AND IFNULL(r.widest, 0) < 150;
