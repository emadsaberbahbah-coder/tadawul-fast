-- v_confidence_calibration: does TFB's confidence mean what it says, market by market - and did the INVESTABLE calls
-- beat their own market, not only rise?
-- WHY (owner, 24 Nov 2026 01:07 Riyadh: "check now we are able to achieve our target" - the INVESTABLE hit rate
-- above 70 %). The check of that night (v_performance_log, 2,389 scored INVESTABLE records): 47.7 % rose; Saudi
-- calls marked HIGH confidence rose 23.8 % of the time (449 records on 6 to 13 names a month) against 53.8 % for
-- Saudi MEDIUM - the confidence is inverted there; and the share that rose follows the market (about 57 % in the
-- rising months, about 30 % in the falling ones), so it measures the market more than the choice. Owner, 01:11:
-- "ok, no objection".
-- THE MEASURE, per market ('SA' .SR, 'US' .US, 'OTHER', and 'ALL'), confidence (HIGH, MEDIUM, LOW, 'ALL') and
-- period (each recording month 'YYYY-MM' and 'all'):
--   records, symbols, days      the scored INVESTABLE records (the Model Evidence tile's population: entry
--                               investability INVESTABLE, is_scored), their symbols and distinct recording dates
--   hit_pct                     the share that rose (win_flag, as the tile)
--   mean_roi_pp                 the mean realised return
--   bench_pp                    the mean return of the record's own market over the same dates: SPY for US,
--                               ^TASI.SR for SA, ACWI otherwise (adjusted closes, the last close on or before the
--                               recording date and on or before the matured date - the target date while none)
--   mean_excess_pp, beat_pct    the mean return above that market, and the share of records that beat it
-- THE READING, per market and period, on the HIGH and MEDIUM rows (fixed on 24 Nov 2026): 'collecting' while either
-- has under 30 records or under 10 days; 'inverted: HIGH below MEDIUM' when HIGH's hit rate is 10 points or more
-- under MEDIUM's; 'in order' when HIGH's is at or above MEDIUM's; else 'close'.
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_confidence_calibration`
OPTIONS (description = "TFB confidence calibration (views 1.26.0): the scored INVESTABLE records by market, confidence and month - the share that rose (the Model Evidence tile), the mean return, the market's own return over the same dates, the share that beat it, and whether HIGH confidence did better than MEDIUM - research only")
AS
WITH p AS (
  SELECT symbol, UPPER(IFNULL(confidence, 'UNKNOWN')) AS confidence, recorded_date, win_flag, realized_roi_pp,
    COALESCE(DATE(matured_at), DATE(target_at)) AS end_date,
    CASE WHEN ENDS_WITH(symbol, '.SR') THEN 'SA' WHEN ENDS_WITH(symbol, '.US') THEN 'US' ELSE 'OTHER' END AS market,
    FORMAT_DATE('%Y-%m', recorded_month) AS month
  FROM `{{DS}}.v_performance_log`
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM `{{DS}}.v_performance_log`)
    AND UPPER(entry_investability) = 'INVESTABLE' AND is_scored AND win_flag IS NOT NULL
),
bench AS (
  SELECT
    CASE symbol WHEN 'SPY' THEN 'US' WHEN '^TASI.SR' THEN 'SA' ELSE 'OTHER' END AS market,
    date, COALESCE(adj_close, close) AS close,
    LEAD(date) OVER (PARTITION BY symbol ORDER BY date) AS next_date
  FROM `{{DS}}.market_series_daily`
  WHERE symbol IN ('SPY', '^TASI.SR', 'ACWI')
),
r AS (
  SELECT p.*,
    100 * (SAFE_DIVIDE(b1.close, b0.close) - 1) AS bench_pp,
    p.realized_roi_pp - 100 * (SAFE_DIVIDE(b1.close, b0.close) - 1) AS excess_pp
  FROM p
  LEFT JOIN bench b0 ON b0.market = p.market AND b0.date <= p.recorded_date
    AND (b0.next_date IS NULL OR b0.next_date > p.recorded_date)
  LEFT JOIN bench b1 ON b1.market = p.market AND b1.date <= p.end_date
    AND (b1.next_date IS NULL OR b1.next_date > p.end_date)
),
g AS (
  SELECT mk, cf, period,
    COUNT(*) AS records, COUNT(DISTINCT symbol) AS symbols, COUNT(DISTINCT recorded_date) AS days,
    ROUND(100 * AVG(win_flag), 1) AS hit_pct,
    ROUND(AVG(realized_roi_pp), 2) AS mean_roi_pp,
    ROUND(AVG(bench_pp), 2) AS bench_pp,
    ROUND(AVG(excess_pp), 2) AS mean_excess_pp,
    ROUND(100 * SAFE_DIVIDE(COUNTIF(excess_pp > 0), COUNTIF(excess_pp IS NOT NULL)), 1) AS beat_pct
  FROM r, UNNEST([market, 'ALL']) AS mk, UNNEST([confidence, 'ALL']) AS cf, UNNEST([month, 'all']) AS period
  GROUP BY mk, cf, period
),
pair AS (
  SELECT g.*,
    MAX(IF(cf = 'HIGH', hit_pct, NULL)) OVER k AS high_hit,
    MAX(IF(cf = 'MEDIUM', hit_pct, NULL)) OVER k AS medium_hit,
    MIN(IF(cf IN ('HIGH', 'MEDIUM'), records, NULL)) OVER k AS pair_records,
    MIN(IF(cf IN ('HIGH', 'MEDIUM'), days, NULL)) OVER k AS pair_days,
    COUNTIF(cf IN ('HIGH', 'MEDIUM')) OVER k AS pair_n
  FROM g
  WINDOW k AS (PARTITION BY mk, period)
)
SELECT
  mk AS market, cf AS confidence, period, records, symbols, days, hit_pct, mean_roi_pp, bench_pp, mean_excess_pp,
  beat_pct,
  CASE
    WHEN cf NOT IN ('HIGH', 'MEDIUM') THEN NULL
    WHEN pair_n < 2 OR pair_records < 30 OR pair_days < 10 THEN 'collecting'
    WHEN high_hit <= medium_hit - 10 THEN 'inverted: HIGH below MEDIUM'
    WHEN high_hit >= medium_hit THEN 'in order'
    ELSE 'close'
  END AS reading
FROM pair;
