-- v_event_reactions: how each market moved around each event that is out.
-- WHY (owner, 21 Nov 2026 11:03 Riyadh, item 5 of the build list): "forward
-- event scenarios ... score every news signal" - a scenario for the next
-- release is what the same release did before, market by market.
-- THE CLOSES: each market's outcome series as v_market_trial reads it (the
--   fund's dividend-adjusted close; oil: Brent BZ=F; the Saudi REITs: their
--   equal-weight index from v_market_condition), each close placed at its
--   own time: 16:00 New York for the funds (they trade in New York), 19:30
--   London for Brent (the settlement), 15:00 Riyadh for the Saudi REITs.
-- THE EVENT CLOSE: the first close after the event's time (within 10 days);
--   the close before it is the last one the event could not have moved (it
--   must lie at most 7 days before, or the row is left out).
--   r1_pct  the change from the close before to the event close: the
--           event-day move
--   r5_pct  the change from the close before to the 4th close after the
--           event close (5 closes)
--   f5_pct  the change from the event close to the 5th close after it: what
--           followed once the figure was known at a close one could trade
--   (NULL until the closes exist)
-- THE NORMAL MOVE: sd1_pct = the standard deviation of the series' daily
--   changes over the 250 closes up to the close before (at least 120; NULL
--   before); z1 = r1_pct / sd1_pct; move = 'up' (z1 >= 1) / 'down'
--   (z1 <= -1) / 'normal'; any_up_share / any_down_share = the share of
--   those 250 days that moved +1 / -1 sd1 or more: how often any day moves
--   that much (event days included), the base a big move is read against.
-- COLUMNS: event_id, family, kind, event_at, figure, market_order, market_id,
--   market, outcome_symbol, pre_day, event_close_day, r1_pct, r5_pct, f5_pct,
--   sd1_pct, z1, move, any_up_share, any_down_share
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it. Observe only.
CREATE OR REPLACE VIEW `{{DS}}.v_event_reactions`
OPTIONS (description = "TFB events v1: one row per event and market - the market's move from the close before the event to the first close after it (its own close time: New York funds, Brent settlement, Tadawul), over 5 closes, and over the 5 closes after the event close, with the move in units of the market's normal daily move (up / normal / down at 1 sd) and how often any day moves that much. Observe only.")
AS
WITH mk AS (
  SELECT *
  FROM UNNEST([
    STRUCT(1 AS market_order, 'us' AS market_id, 'United States' AS market, 'SPY' AS outcome_symbol,
           'America/New_York' AS close_tz, TIME '16:00:00' AS close_time),
    STRUCT(2, 'sa', 'Saudi Arabia', 'KSA', 'America/New_York', TIME '16:00:00'),
    STRUCT(3, 'eu', 'Europe', 'VGK', 'America/New_York', TIME '16:00:00'),
    STRUCT(4, 'jp', 'Japan', 'EWJ', 'America/New_York', TIME '16:00:00'),
    STRUCT(5, 'cn', 'China / Hong Kong', 'MCHI', 'America/New_York', TIME '16:00:00'),
    STRUCT(6, 'gold', 'Gold', 'GLD', 'America/New_York', TIME '16:00:00'),
    STRUCT(7, 'oil', 'Oil', 'BZ=F', 'Europe/London', TIME '19:30:00'),
    STRUCT(8, 'bonds', 'Bonds', 'IEF', 'America/New_York', TIME '16:00:00'),
    STRUCT(9, 'sukuk', 'Sukuk', 'SPSK', 'America/New_York', TIME '16:00:00'),
    STRUCT(10, 'reits_us', 'US REITs', 'VNQ', 'America/New_York', TIME '16:00:00'),
    STRUCT(11, 'reits_sa', 'Saudi REITs', 'SA_REITS', 'Asia/Riyadh', TIME '15:00:00')
  ])
),
px AS (
  SELECT s.symbol, s.date, COALESCE(s.adj_close, s.close) AS px
  FROM `{{DS}}.market_series_daily` AS s
  WHERE s.close > 0 AND s.symbol IN ('SPY', 'KSA', 'VGK', 'EWJ', 'MCHI', 'GLD', 'IEF', 'SPSK', 'VNQ', 'BZ=F')
  UNION ALL
  SELECT 'SA_REITS', date, close
  FROM `{{DS}}.v_market_condition`
  WHERE market_id = 'reits_sa'
),
seq AS (
  SELECT
    mk.market_id, p.date, p.px,
    TIMESTAMP(DATETIME(p.date, mk.close_time), mk.close_tz) AS close_at,
    ROW_NUMBER() OVER (PARTITION BY mk.market_id ORDER BY p.date) AS n
  FROM px AS p
  JOIN mk ON mk.outcome_symbol = p.symbol
),
chg AS (
  SELECT s.market_id, s.n, (SAFE_DIVIDE(s.px, LAG(s.px) OVER (PARTITION BY s.market_id ORDER BY s.n)) - 1) * 100 AS c1
  FROM seq AS s
),
vol AS (
  SELECT c.market_id, c.n, STDDEV_SAMP(c.c1) OVER w AS sd250, COUNT(c.c1) OVER w AS n250
  FROM chg AS c
  WINDOW w AS (PARTITION BY c.market_id ORDER BY c.n ROWS BETWEEN 249 PRECEDING AND CURRENT ROW)
),
ev AS (
  SELECT event_id, family, kind, event_at, figure
  FROM `{{DS}}.v_event_calendar`
  WHERE is_out
),
post AS (
  SELECT e.event_id, e.family, e.kind, e.event_at, e.figure, s.market_id, s.n AS post_n, s.date AS event_close_day,
         s.px AS post_px
  FROM ev AS e
  CROSS JOIN mk
  JOIN seq AS s
    ON s.market_id = mk.market_id AND s.close_at > e.event_at AND s.close_at <= TIMESTAMP_ADD(e.event_at, INTERVAL 10 DAY)
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY e.event_id, s.market_id ORDER BY s.n) = 1
),
r AS (
  SELECT
    p.*, b.date AS pre_day, b.px AS pre_px, v.sd250, v.n250, x4.px AS px4, x5.px AS px5
  FROM post AS p
  JOIN seq AS b ON b.market_id = p.market_id AND b.n = p.post_n - 1
  JOIN vol AS v ON v.market_id = p.market_id AND v.n = p.post_n - 1
  LEFT JOIN seq AS x4 ON x4.market_id = p.market_id AND x4.n = p.post_n + 4
  LEFT JOIN seq AS x5 ON x5.market_id = p.market_id AND x5.n = p.post_n + 5
  WHERE DATE_DIFF(p.event_close_day, b.date, DAY) <= 7
),
anyday AS (
  SELECT
    r.event_id, r.market_id,
    SAFE_DIVIDE(COUNTIF(c.c1 >= r.sd250), COUNT(c.c1)) AS up_share,
    SAFE_DIVIDE(COUNTIF(c.c1 <= -r.sd250), COUNT(c.c1)) AS down_share
  FROM r
  JOIN chg AS c ON c.market_id = r.market_id AND c.n BETWEEN r.post_n - 250 AND r.post_n - 1
  GROUP BY r.event_id, r.market_id
),
calc AS (
  SELECT
    r.*,
    (SAFE_DIVIDE(r.post_px, r.pre_px) - 1) * 100 AS r1,
    IF(r.n250 >= 120 AND r.sd250 > 0, r.sd250, NULL) AS sd1
  FROM r
)
SELECT
  c.event_id, c.family, c.kind, c.event_at, c.figure, mk.market_order, c.market_id, mk.market, mk.outcome_symbol,
  c.pre_day, c.event_close_day,
  ROUND(c.r1, 4) AS r1_pct,
  ROUND((SAFE_DIVIDE(c.px4, c.pre_px) - 1) * 100, 4) AS r5_pct,
  ROUND((SAFE_DIVIDE(c.px5, c.post_px) - 1) * 100, 4) AS f5_pct,
  ROUND(c.sd1, 4) AS sd1_pct,
  ROUND(SAFE_DIVIDE(c.r1, c.sd1), 3) AS z1,
  CASE
    WHEN c.sd1 IS NULL THEN NULL
    WHEN c.r1 / c.sd1 >= 1 THEN 'up'
    WHEN c.r1 / c.sd1 <= -1 THEN 'down'
    ELSE 'normal'
  END AS move,
  IF(c.sd1 IS NULL, NULL, ROUND(a.up_share, 4)) AS any_up_share,
  IF(c.sd1 IS NULL, NULL, ROUND(a.down_share, 4)) AS any_down_share
FROM calc AS c
JOIN mk USING (market_id)
LEFT JOIN anyday AS a USING (event_id, market_id);
