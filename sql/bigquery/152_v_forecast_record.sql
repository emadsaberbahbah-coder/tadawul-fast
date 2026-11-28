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
-- THE THIRDS, WRITTEN INTO THIS VIEW (views 1.40.0): per market and horizon, the calls recorded up to 28 Nov 2026 and
-- scored by 27 Nov 2026 were cut into thirds by their promise (target_roi_pp, NTILE(3), lowest first, ties by symbol and
-- day). low_to_pp and mid_to_pp are the highest promise of the first and second third. 1.39.0 worked them out again on
-- every read, so a late correction of an old record (a duplicate expired, a bar restated) could move a cut and with it
-- the band of every call - after the forward test had begun (external audit of 28 Nov 2026, finding 08). The 37 pairs
-- below are the values 1.39.0 read on 28 Nov 2026 at 18:30 Riyadh, after that day's duplicate clean-up
-- (TRACK_DEDUP_MATURED enforce, 27 records): they are constants now. A call is 'low' at or under low_to_pp, 'mid' at
-- or under mid_to_pp, else 'high'. A market and horizon outside the list has no band ('all' only). A NULL mid_to (a
-- pair that had one call) leaves 'low' and 'mid'. 'all' is the whole market and horizon.
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
OPTIONS (description = "TFB forecast record (views 1.40.0): per market, horizon and promise third (cuts fixed as constants, read on the calls scored by 27 Nov 2026) and for all calls and the calls from 29 Nov 2026, what TFB's scored INVESTABLE buy calls promised and delivered, the share of the promise, how often they rose and beat their own index, with a fixed reading - the record behind the honest forecast the Daily Plan shows in observe - research only")
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
cuts AS (
  SELECT market, horizon, low_to, mid_to
  FROM UNNEST([
    STRUCT('Canada' AS market, '1W' AS horizon, 0.6851 AS low_to, 0.8377 AS mid_to),
    ('Canada', '2W', 1.3701, 1.6755),
    ('Europe', '1M', 5.1307, 6.0414),
    ('Europe', '1W', 1.2923, 1.3644),
    ('Europe', '2W', 2.5845, 2.7287),
    ('Europe', '3M', 13.4784, 14.2909),
    ('Hong Kong', '1M', 2.0639, 3.3083),
    ('Hong Kong', '1W', 0.0064, 0.5214),
    ('Hong Kong', '2W', 0.0127, 1.0427),
    ('Hong Kong', '3M', 10.8987, 14.2538),
    ('India', '1M', 6.2783, CAST(NULL AS FLOAT64)),
    ('India', '3M', 14.6494, CAST(NULL AS FLOAT64)),
    ('London', '1M', 2.789, 5.9998),
    ('London', '1W', 0.9761, 1.3113),
    ('London', '2W', 1.9438, 2.6225),
    ('London', '3M', 13.9994, 14.2349),
    ('Malaysia', '1M', 6.12, 6.12),
    ('Malaysia', '3M', 14.28, 14.28),
    ('Mexico', '1M', 3.6, CAST(NULL AS FLOAT64)),
    ('Mexico', '1W', 0.0084, CAST(NULL AS FLOAT64)),
    ('Mexico', '2W', 0.0168, CAST(NULL AS FLOAT64)),
    ('Singapore', '1M', 1.6714, 4.1504),
    ('Singapore', '1W', 0.3686, 0.429),
    ('Singapore', '2W', 0.0196, 0.78),
    ('Singapore', '3M', 9.7867, 9.82),
    ('Tadawul', '1M', 4.1929, 5.4891),
    ('Tadawul', '1W', 0.0107, 0.0147),
    ('Tadawul', '2W', 0.0212, 0.0294),
    ('Tadawul', '3M', 10.3032, 12.0912),
    ('Tokyo', '1M', 6.2948, 6.2988),
    ('Tokyo', '1W', 0.0147, 1.4233),
    ('Tokyo', '2W', 0.0294, 2.8466),
    ('Tokyo', '3M', 14.69, 14.6973),
    ('US', '1M', 2.6724, 4.2143),
    ('US', '1W', 0.4547, 0.9752),
    ('US', '2W', 0.7969, 1.8286),
    ('US', '3M', 6.0638, 10.5)
  ])
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
  g.low_to AS low_to_pp, g.mid_to AS mid_to_pp,
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
