-- v_news_sector_weights: the weight of every news theme for every US sector,
-- relearned every week from the past only - the self-correcting part of the
-- news loop. WHY (owner, 19 Nov 2026 12:31 Riyadh: a weight for every kind of
-- news, different from market to market and sector to sector, tested on the
-- past and able to update itself after its own errors).
-- TARGET: the sector ETF (XLB .. XLY, market_series_daily) minus SPY, in
-- percent points, from the close of the first trading day of a week to the
-- close of the first trading day of the next week.
-- LINK: the themes of news week W (Monday - Sunday UTC, v_news_theme_weekly,
-- usable weeks only) are set beside the target entered in week W + 1: the
-- news is complete on Sunday night and the position opens at Monday's close.
-- WEIGHT, per sector and theme, walk-forward: at week W the slope (beta) of
-- target on surprise is fitted on the pairs up to week W - 2 only (the pair
-- of W - 1 is still open at the decision), at least 52 pairs;
-- prediction_pp = beta x (surprise - its mean in those pairs).
-- SELF-CORRECTION: past_corr is the correlation of the weight's own earlier
-- predictions with what then happened (again up to W - 2); a weight is
-- active only with 26 or more scored predictions and past_corr above 0 - a
-- weight that stops working switches itself off, and on again if it recovers.
-- NO LOOK-AHEAD: nothing dated after the Sunday of week W enters a row.
-- COLUMNS: week_start, etf, sector, feature, source, surprise, excess_pp (the
-- target, NULL until known), pairs, beta, prediction_pp, past_corr,
-- past_weeks, active.
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_news_sector_weights`
OPTIONS (description = "TFB news loop: walk-forward weight of each news theme for each US sector (sector ETF minus SPY, next week), with the weight's own past hit record and an active flag that switches a failing weight off")
AS
WITH etf AS (
  SELECT * FROM UNNEST(ARRAY<STRUCT<etf STRING, sector STRING>>[
    ('XLB', 'Materials'), ('XLC', 'Communication Services'), ('XLE', 'Energy'), ('XLF', 'Financials'),
    ('XLI', 'Industrials'), ('XLK', 'Information Technology'), ('XLP', 'Consumer Staples'),
    ('XLRE', 'Real Estate'), ('XLU', 'Utilities'), ('XLV', 'Health Care'), ('XLY', 'Consumer Discretionary')])
),
pxd AS (
  SELECT symbol, date, adj_close
  FROM `{{DS}}.market_series_daily`
  WHERE symbol IN ('SPY', 'XLB', 'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLRE', 'XLU', 'XLV', 'XLY') AND adj_close > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY captured_at DESC) = 1
),
px AS (
  SELECT symbol, DATE_TRUNC(date, WEEK(MONDAY)) AS wk, ARRAY_AGG(adj_close ORDER BY date ASC LIMIT 1)[OFFSET(0)] AS c
  FROM pxd
  GROUP BY symbol, wk
),
wr AS (
  SELECT
    symbol, wk,
    LEAD(c) OVER (PARTITION BY symbol ORDER BY wk) / c - 1 AS r,
    DATE_DIFF(LEAD(wk) OVER (PARTITION BY symbol ORDER BY wk), wk, DAY) AS gap
  FROM px
),
ex AS (
  SELECT a.symbol AS etf, a.wk AS entry_week, 100 * (a.r - s.r) AS excess_pp
  FROM wr AS a
  JOIN wr AS s ON s.symbol = 'SPY' AND s.wk = a.wk
  WHERE a.symbol != 'SPY' AND a.r IS NOT NULL AND a.gap = 7 AND s.gap = 7
),
d AS (
  SELECT e.etf, e.sector, t.feature, t.source, t.week_start, t.surprise AS x, ex.excess_pp AS y
  FROM `{{DS}}.v_news_theme_weekly` AS t
  CROSS JOIN etf AS e
  LEFT JOIN ex ON ex.etf = e.etf AND ex.entry_week = DATE_ADD(t.week_start, INTERVAL 7 DAY)
  WHERE t.usable
),
w AS (
  SELECT
    d.*,
    COUNTIF(y IS NOT NULL) OVER past AS n,
    SUM(IF(y IS NULL, NULL, x)) OVER past AS sx,
    SUM(y) OVER past AS sy,
    SUM(IF(y IS NULL, NULL, x * x)) OVER past AS sxx,
    SUM(x * y) OVER past AS sxy
  FROM d
  WINDOW past AS (PARTITION BY etf, feature ORDER BY week_start ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING)
),
pr AS (
  SELECT
    w.*,
    IF(n >= 52, SAFE_DIVIDE(sxy - sx * sy / n, sxx - sx * sx / n), NULL) AS beta
  FROM w
),
pp AS (
  SELECT pr.*, beta * (x - SAFE_DIVIDE(sx, n)) AS p
  FROM pr
),
va AS (
  SELECT
    pp.*,
    CORR(IF(y IS NULL, NULL, p), y) OVER past AS val,
    COUNTIF(y IS NOT NULL AND p IS NOT NULL) OVER past AS nv
  FROM pp
  WINDOW past AS (PARTITION BY etf, feature ORDER BY week_start ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING)
)
SELECT
  week_start, etf, sector, feature, source,
  x AS surprise,
  y AS excess_pp,
  n AS pairs,
  beta,
  p AS prediction_pp,
  val AS past_corr,
  nv AS past_weeks,
  p IS NOT NULL AND nv >= 26 AND IFNULL(val, 0) > 0 AS active
FROM va;
