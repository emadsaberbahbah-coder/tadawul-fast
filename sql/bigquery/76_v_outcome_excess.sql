-- v_outcome_excess: every engine-scored forecast beside its market, so a
-- -3 % week in a -4.4 % market reads as stock selection, not as a wrong
-- call. WHY (owner, 8 Nov 2026: the 5 Nov -4.4 % day took three stops; "how
-- we can avoid that in the future"): the engine's win rate is absolute, so a
-- market-wide fall lowers every pick's score at once and the calibration
-- learns market direction instead of selection. The reference series are
-- already archived (market_series_daily): the forecast's market picks its
-- benchmark - US -> SPY, Tadawul -> ^TASI.SR, Hong Kong -> ^HSI, London ->
-- ^FTSE, Europe -> ^STOXX50E, Tokyo -> ^N225, every other market -> ACWI.
-- RULE: the benchmark return runs from the forecast's base bar (the first
-- archived bar on/after the recording day, v_outcome_engine.base_bar_date)
-- to the outcome bar, each benchmark close being the first archived
-- benchmark bar on/after that date; the ladder steps use the first benchmark
-- bar 7 / 14 / 30 / 91 days after the recording, as the engine does for the
-- stock. Benchmark closes are price closes (indices carry no dividends; the
-- ETF dividend over a few weeks is below the rounding).
-- COLUMNS beyond v_outcome_engine's own: benchmark_symbol, bench_roi_pp (to
-- the target bar), excess_pp = engine_roi_pp - bench_roi_pp, excess_call
-- (WIN when a bullish call beat its market or a bearish one trailed it; LOSS
-- otherwise; NULL when either leg is missing), excess_1w_pp .. excess_3m_pp
-- for the ladder, bench_1w_pp .. bench_3m_pp.
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it.
CREATE OR REPLACE VIEW `{{DS}}.v_outcome_excess`
OPTIONS (description = "TFB Looker v6: every engine-scored forecast beside its market benchmark (SPY / TASI / HSI / FTSE / Euro Stoxx / Nikkei / ACWI by market) - benchmark return, excess return and an excess verdict at the target and on the 1W / 2W / 1M / 3M ladder")
AS
WITH bench_map AS (
  SELECT * FROM UNNEST(ARRAY<STRUCT<market STRING, benchmark STRING>>[
    ('US', 'SPY'), ('Tadawul', '^TASI.SR'), ('Hong Kong', '^HSI'), ('London', '^FTSE'),
    ('Europe', '^STOXX50E'), ('Tokyo', '^N225')
  ])
),
e AS (
  SELECT
    o.*,
    COALESCE(m.benchmark, 'ACWI') AS benchmark_symbol,
    DATE(o.recorded_at, 'Asia/Riyadh') AS recorded_day
  FROM `{{DS}}.v_outcome_engine` AS o
  LEFT JOIN bench_map AS m ON m.market = o.market
),
b AS (
  SELECT symbol, date, close
  FROM `{{DS}}.market_series_daily`
  WHERE close IS NOT NULL AND close > 0
),
-- the first benchmark close on/after each date the engine used
agg AS (
  SELECT
    e.record_id,
    ARRAY_AGG(IF(b.date >= e.base_bar_date, b.close, NULL) IGNORE NULLS ORDER BY b.date LIMIT 1)[SAFE_OFFSET(0)] AS bench_base,
    ARRAY_AGG(IF(e.target_bar_date IS NOT NULL AND b.date >= e.target_bar_date, b.close, NULL) IGNORE NULLS ORDER BY b.date LIMIT 1)[SAFE_OFFSET(0)] AS bench_target,
    ARRAY_AGG(IF(b.date >= DATE_ADD(e.recorded_day, INTERVAL 7 DAY), b.close, NULL) IGNORE NULLS ORDER BY b.date LIMIT 1)[SAFE_OFFSET(0)] AS bench_1w,
    ARRAY_AGG(IF(b.date >= DATE_ADD(e.recorded_day, INTERVAL 14 DAY), b.close, NULL) IGNORE NULLS ORDER BY b.date LIMIT 1)[SAFE_OFFSET(0)] AS bench_2w,
    ARRAY_AGG(IF(b.date >= DATE_ADD(e.recorded_day, INTERVAL 30 DAY), b.close, NULL) IGNORE NULLS ORDER BY b.date LIMIT 1)[SAFE_OFFSET(0)] AS bench_1m,
    ARRAY_AGG(IF(b.date >= DATE_ADD(e.recorded_day, INTERVAL 91 DAY), b.close, NULL) IGNORE NULLS ORDER BY b.date LIMIT 1)[SAFE_OFFSET(0)] AS bench_3m
  FROM e
  JOIN b
    ON b.symbol = e.benchmark_symbol
   AND b.date >= e.base_bar_date
   AND b.date <= DATE_ADD(e.recorded_day, INTERVAL 98 DAY)
  WHERE e.base_bar_date IS NOT NULL
  GROUP BY e.record_id
),
x AS (
  SELECT
    e.*,
    IF(a.bench_target IS NOT NULL AND e.engine_roi_pp IS NOT NULL, ROUND((a.bench_target / a.bench_base - 1) * 100, 4), NULL) AS bench_roi_pp,
    IF(e.roi_1w_pp IS NOT NULL, ROUND((a.bench_1w / a.bench_base - 1) * 100, 4), NULL) AS bench_1w_pp,
    IF(e.roi_2w_pp IS NOT NULL, ROUND((a.bench_2w / a.bench_base - 1) * 100, 4), NULL) AS bench_2w_pp,
    IF(e.roi_1m_pp IS NOT NULL, ROUND((a.bench_1m / a.bench_base - 1) * 100, 4), NULL) AS bench_1m_pp,
    IF(e.roi_3m_pp IS NOT NULL, ROUND((a.bench_3m / a.bench_base - 1) * 100, 4), NULL) AS bench_3m_pp
  FROM e
  LEFT JOIN agg AS a ON a.record_id = e.record_id
)
SELECT
  x.* EXCEPT (recorded_day),
  ROUND(x.engine_roi_pp - x.bench_roi_pp, 4) AS excess_pp,
  CASE
    WHEN x.engine_roi_pp IS NULL OR x.bench_roi_pp IS NULL THEN NULL
    WHEN x.entry_stance = 'bullish' THEN IF(x.engine_roi_pp > x.bench_roi_pp, 'WIN', 'LOSS')
    WHEN x.entry_stance = 'bearish' THEN IF(x.engine_roi_pp < x.bench_roi_pp, 'WIN', 'LOSS')
  END AS excess_call,
  ROUND(x.roi_1w_pp - x.bench_1w_pp, 4) AS excess_1w_pp,
  ROUND(x.roi_2w_pp - x.bench_2w_pp, 4) AS excess_2w_pp,
  ROUND(x.roi_1m_pp - x.bench_1m_pp, 4) AS excess_1m_pp,
  ROUND(x.roi_3m_pp - x.bench_3m_pp, 4) AS excess_3m_pp
FROM x;
