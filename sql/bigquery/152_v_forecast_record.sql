-- v_forecast_record: what TFB's buy calls delivered against what they promised, per market, horizon and promise third -
-- the record behind the honest forecast and the measured label that the Daily Plan shows beside each name (observe).
-- WHY (owner, 28 Nov 2026 01:02 Riyadh, in Arabic: a return of at least 5 % a month on the capital, in all cases, or stop
-- before the losses grow. 01:30: "Try to make full review and engening restructured looking for better results"). The
-- review of that night (v_outcome_engine, the INVESTABLE buy calls scored to 27 Nov 2026): the US one-month calls promised
-- +3.66 % on average and delivered +1.13 % (31 % of the promise), the Saudi ones promised +4.64 % and delivered -4.37 %,
-- the Hong Kong ones promised +2.97 % and delivered +3.28 %. The pages show the promise, and a HIGH / MEDIUM / LOW
-- confidence that the record does not bear out (v_confidence_calibration: Saudi HIGH rose 23.8 % of the time). What was
-- delivered also depends on the size of the promise: the US one-month calls of the lowest third of promises lost 1.28 %
-- on average, those of the highest third made 3.26 % (72 % rose, 71 % beat SPY) - so the record is kept per third.
-- THE CALLS: the records of v_outcome_engine scored from the archived bars (engine_scored), the first per Key
-- (in_engine_cohort), with entry investability INVESTABLE and stance bullish (the population of v_projection_scorecard
-- and v_learning_scorecard), every market, horizons 1W, 2W, 1M and 3M. 1W and 2W promises are the one-month thesis
-- scaled to the window (track_performance), so only 1M and 3M carry the page's own forecast.
-- THE THIRDS, FIXED ON 28 NOV 2026: per market and horizon, the calls recorded up to 28 Nov 2026 and scored by 27 Nov 2026
-- are cut into thirds by their promise (target_roi_pp, NTILE(3), lowest first, ties by symbol and day). low_to_pp and
-- mid_to_pp are the highest promise of the first and second third, and they do not move as new calls are scored: a call
-- is 'low' at or under low_to_pp, 'mid' at or under mid_to_pp, else 'high' (no band where the market and horizon had no
-- such call). 'all' is the whole market and horizon.
-- THE PERIODS: 'all' (every scored call) and 'forward' (calls recorded from 29 Nov 2026, after the thirds were fixed):
-- the forward rows say whether the thirds keep their order on calls that did not shape them.
-- THE MEASURE per (market, horizon, band, period), all in percent points:
--   calls, symbols, days (distinct recording days), first_call, last_call, scored_through (the last outcome bar)
--   promise_from_pp, promise_to_pp  the band's lowest and highest promise
--   promised_pp, delivered_pp       the mean promise (target_roi_pp) and the mean result (engine_roi_pp, total return)
--   share_of_promise                delivered / promised, the ratio of the means (NULL unless the promise mean is above 0)
--   rose_pct                        the share that rose (engine_win_flag)
--   index_symbol, index_pp          the mean return of the market's own index over the same dates: from the last index
--                                   close BEFORE the recording day (calls are recorded before the US open, mostly) to
--                                   the last close on or before the outcome bar's date. SPY for the US, ^TASI.SR for
--                                   Tadawul (a price index: it leaves out about 0.3 % a month of dividends, so Saudi calls
--                                   read slightly better against it than they are), ^HSI, ^STOXX for Europe, ^FTSE,
--                                   ^N225, 000300.SS for China, ACWI elsewhere - adjusted closes where the archive has them
--   excess_pp, beat_pct             the mean result above that index, and the share of calls above it
--   suffixes                        the listing suffixes of the market's scored calls, so a reader can place a symbol
-- THE HONEST FORECAST (scripts/daily_plan.py 1.12.0, TFB_PLAN_HONEST observe): a name of market m whose page promises p at
--   horizon h (1M or 3M, from its forecast price) reads the period-'all' row of its band of (m, h) if that row has 30
--   calls on 10 days, else the 'all' band's row if it has them, else nothing ('not measured'). The honest forecast is that
--   row's delivered_pp, beside the promise, with the row's rose_pct, beat_pct and calls (the measured label). It is the
--   record of past calls, not a tested rule and not a promise.
-- THE READING (fixed on 28 Nov 2026), per row: 'too few calls' under 30 calls or 10 days, 'lost money' when delivered_pp
--   is 0 or below, 'no index read' when no call has an index return, 'beat its index' when excess_pp is above 0 and
--   beat_pct is 50 or more, else 'made money, behind its index'.
-- OVERLAP: calls of neighbouring days share most of their window (a 1M call holds 21 sessions), so the calls of a row are
--   far fewer independent results than their count - the reason the reading needs 10 days as well as 30 calls.
-- Research view: the live system never reads BigQuery (the Daily Plan reads the Forecast_Record tab that
-- scripts/forecast_record.py writes from this view), and no decision, gate, count or order depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_forecast_record`
OPTIONS (description = "TFB forecast record (views 1.39.0): per market, horizon and promise third (cut once, on the calls scored by 27 Nov 2026) and for all calls and the calls from 29 Nov 2026, what TFB's scored INVESTABLE buy calls promised and delivered, the share of the promise, how often they rose and beat their own index, with a fixed reading - the record behind the honest forecast the Daily Plan shows in observe - research only")
AS
WITH c AS (
  SELECT symbol, market, horizon, recorded_date, target_bar_date,
    target_roi_pp AS p, engine_roi_pp AS r, engine_win_flag AS w,
    CASE market
      WHEN 'US' THEN 'SPY' WHEN 'Tadawul' THEN '^TASI.SR' WHEN 'Hong Kong' THEN '^HSI' WHEN 'Europe' THEN '^STOXX'
      WHEN 'London' THEN '^FTSE' WHEN 'Tokyo' THEN '^N225' WHEN 'China' THEN '000300.SS' ELSE 'ACWI'
    END AS idx
  FROM `{{DS}}.v_outcome_engine`
  WHERE engine_scored AND in_engine_cohort AND entry_investability = 'INVESTABLE' AND entry_stance = 'bullish'
    AND horizon IN ('1W', '2W', '1M', '3M')
    AND target_roi_pp IS NOT NULL AND engine_roi_pp IS NOT NULL AND target_bar_date IS NOT NULL
),
fixed AS (
  SELECT market, horizon, p,
    NTILE(3) OVER (PARTITION BY market, horizon ORDER BY p, symbol, recorded_date) AS third
  FROM c
  WHERE recorded_date <= DATE '2026-11-28' AND target_bar_date <= DATE '2026-11-27'
),
cuts AS (
  SELECT market, horizon, MAX(IF(third = 1, p, NULL)) AS low_to, MAX(IF(third = 2, p, NULL)) AS mid_to
  FROM fixed
  GROUP BY market, horizon
),
ix_day AS (
  SELECT symbol, date, COALESCE(adj_close, close) AS px
  FROM `{{DS}}.market_series_daily`
  WHERE symbol IN ('SPY', '^TASI.SR', '^HSI', '^STOXX', '^FTSE', '^N225', '000300.SS', 'ACWI')
    AND COALESCE(adj_close, close) > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY captured_at DESC) = 1
),
ix AS (
  SELECT symbol, date, px, LEAD(date) OVER (PARTITION BY symbol ORDER BY date) AS next_date
  FROM ix_day
),
banded AS (
  SELECT c.*, k.low_to, k.mid_to,
    100 * (SAFE_DIVIDE(b1.px, b0.px) - 1) AS idx_pp,
    CASE
      WHEN k.low_to IS NULL THEN NULL
      WHEN c.p <= k.low_to THEN 'low'
      WHEN k.mid_to IS NULL OR c.p <= k.mid_to THEN 'mid'
      ELSE 'high'
    END AS band
  FROM c
  LEFT JOIN cuts AS k ON k.market = c.market AND k.horizon = c.horizon
  LEFT JOIN ix AS b0 ON b0.symbol = c.idx AND b0.date < c.recorded_date
    AND (b0.next_date IS NULL OR b0.next_date >= c.recorded_date)
  LEFT JOIN ix AS b1 ON b1.symbol = c.idx AND b1.date <= c.target_bar_date
    AND (b1.next_date IS NULL OR b1.next_date > c.target_bar_date)
),
g AS (
  SELECT b.market, b.horizon, bd AS band, pd AS period,
    ANY_VALUE(b.idx) AS index_symbol, ANY_VALUE(b.low_to) AS low_to, ANY_VALUE(b.mid_to) AS mid_to,
    COUNT(*) AS calls, COUNT(DISTINCT b.symbol) AS symbols, COUNT(DISTINCT b.recorded_date) AS days,
    MIN(b.recorded_date) AS first_call, MAX(b.recorded_date) AS last_call, MAX(b.target_bar_date) AS scored_through,
    MIN(b.p) AS promise_from, MAX(b.p) AS promise_to, AVG(b.p) AS promised, AVG(b.r) AS delivered,
    100 * AVG(b.w) AS rose, AVG(b.idx_pp) AS index_ret, AVG(b.r - b.idx_pp) AS excess,
    100 * SAFE_DIVIDE(COUNTIF(b.r > b.idx_pp), COUNTIF(b.idx_pp IS NOT NULL)) AS beat
  FROM banded AS b,
    UNNEST(ARRAY_CONCAT(['all'], IF(b.band IS NULL, ARRAY<STRING>[], [b.band]))) AS bd,
    UNNEST(ARRAY_CONCAT(['all'], IF(b.recorded_date >= DATE '2026-11-29', ['forward'], ARRAY<STRING>[]))) AS pd
  GROUP BY b.market, b.horizon, bd, pd
),
sfx AS (
  SELECT market, ARRAY_TO_STRING(ARRAY_AGG(DISTINCT s IGNORE NULLS ORDER BY s), ' ') AS suffixes
  FROM (SELECT market, REGEXP_EXTRACT(symbol, r'\.([A-Z]+)$') AS s FROM c)
  GROUP BY market
)
SELECT
  g.market, g.horizon, g.band, g.period,
  CASE g.band WHEN 'all' THEN 0 WHEN 'low' THEN 1 WHEN 'mid' THEN 2 ELSE 3 END AS band_order,
  ROUND(g.low_to, 2) AS low_to_pp, ROUND(g.mid_to, 2) AS mid_to_pp,
  ROUND(g.promise_from, 2) AS promise_from_pp, ROUND(g.promise_to, 2) AS promise_to_pp,
  g.calls, g.symbols, g.days, g.first_call, g.last_call, g.scored_through,
  ROUND(g.promised, 2) AS promised_pp, ROUND(g.delivered, 2) AS delivered_pp,
  ROUND(IF(g.promised > 0, g.delivered / g.promised, NULL), 3) AS share_of_promise,
  ROUND(g.rose, 1) AS rose_pct,
  g.index_symbol, ROUND(g.index_ret, 2) AS index_pp, ROUND(g.excess, 2) AS excess_pp, ROUND(g.beat, 1) AS beat_pct,
  IFNULL(x.suffixes, '') AS suffixes,
  CASE
    WHEN g.calls < 30 OR g.days < 10 THEN 'too few calls'
    WHEN g.delivered <= 0 THEN 'lost money'
    WHEN g.excess IS NULL THEN 'no index read'
    WHEN g.excess > 0 AND g.beat >= 50 THEN 'beat its index'
    ELSE 'made money, behind its index'
  END AS reading
FROM g
LEFT JOIN sfx AS x ON x.market = g.market;
