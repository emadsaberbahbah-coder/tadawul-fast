-- v_news_trial: the forward test of every news signal - on each US session,
-- each market's news reading of the day before and what the market's fund
-- did next.
-- WHY (owner, 21 Nov 2026 11:03 Riyadh, item 5 of the build list): "score
-- every news signal"; item 6: "Accuracy: log and score everything. Observe
-- for two weeks (10 sessions), then review with the owner, and switch on only
-- what proved itself." The rules of v_market_news and v_people_news were
-- fixed on 21 Nov 2026, before any forward session: period = 'forward' from
-- Monday 23 Nov 2026, 'backtest' before (v_market_trial's own split).
-- THE DAY: decision_day = a US session (SPY's own dates), as in
-- v_market_trial; news_day = the last news day (UTC) at least one calendar
-- day before it and at most three (a UTC day is complete only when the next
-- begins, and the decision day's own news is not complete at its US close).
-- THE SIGNALS (one row per decision day, market and signal):
--   market_news   reading favourable / neutral / unfavourable, value =
--                 market_score (v_market_news)
--   momentum      improving / steady / worsening, value = market_momentum
--   surprise      surge_favourable / surge_unfavourable / none (no value)
--   person_<id>   spike_positive / spike_negative / quiet, on each market the
--                 person is expected to move (v_people_news.markets); value =
--                 the person's att_z
--   expected      the direction the reading should move the market: +1
--                 favourable, improving, surge_favourable, spike_positive;
--                 -1 their opposites; 0 the middle readings
-- THE OUTCOME: v_market_trial's, unchanged - the fund's change over the
--   next 5, 10 and 20 closes from the first close after the decision day
--   (f5_pct / f10_pct / f20_pct, NULL until it exists) and against SPY's
--   over the same span (vs_spy_*_pp).
-- The windows of consecutive days overlap: read the means and shares, not a
-- count of independent trials.
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it. Observe only.
CREATE OR REPLACE VIEW `{{DS}}.v_news_trial`
OPTIONS (description = "TFB news v1: one row per US session, market and news signal - the market's news reading of the day before (its place's news, its momentum, a surprise, and each person expected to move it) and the market fund's change over the next 5, 10 and 20 closes (v_market_trial) - period 'forward' from 23 Nov 2026, 'backtest' before it. Observe only.")
AS
WITH t AS (
  SELECT
    decision_day, period, market_id, market, market_order, outcome_symbol, entry_date,
    f5_pct, f10_pct, f20_pct, vs_spy_f5_pp, vs_spy_f10_pp, vs_spy_f20_pp
  FROM `{{DS}}.v_market_trial`
),
us AS (
  SELECT DISTINCT date AS d
  FROM `{{DS}}.market_series_daily`
  WHERE symbol = 'SPY' AND close > 0
),
news_days AS (
  SELECT DISTINCT date
  FROM `{{DS}}.news_entity_daily`
  WHERE feature = 'place_us_share'
),
nd AS (
  SELECT us.d AS decision_day, MAX(n.date) AS news_day
  FROM us
  JOIN news_days AS n
    ON n.date >= DATE_SUB(us.d, INTERVAL 3 DAY) AND n.date <= DATE_SUB(us.d, INTERVAL 1 DAY)
  GROUP BY us.d
),
sig AS (
  SELECT mn.date, mn.market_id, x.signal, x.reading, x.value
  FROM `{{DS}}.v_market_news` AS mn,
    UNNEST([
      STRUCT('market_news' AS signal, mn.reading AS reading, mn.market_score AS value),
      STRUCT('momentum', mn.momentum_label, mn.market_momentum),
      STRUCT('surprise', mn.surprise, CAST(NULL AS FLOAT64))
    ]) AS x
  WHERE x.reading IS NOT NULL
  UNION ALL
  SELECT pn.date, mk, CONCAT('person_', pn.person_id), pn.reading, pn.att_z
  FROM `{{DS}}.v_people_news` AS pn, UNNEST(SPLIT(pn.markets, ',')) AS mk
  WHERE pn.reading IS NOT NULL
)
SELECT
  t.decision_day,
  t.period,
  nd.news_day,
  t.market_id, t.market, t.market_order,
  sig.signal, sig.reading, sig.value,
  CASE
    WHEN sig.reading IN ('favourable', 'improving', 'surge_favourable', 'spike_positive') THEN 1
    WHEN sig.reading IN ('unfavourable', 'worsening', 'surge_unfavourable', 'spike_negative') THEN -1
    ELSE 0
  END AS expected,
  t.outcome_symbol, t.entry_date,
  t.f5_pct, t.f10_pct, t.f20_pct,
  t.vs_spy_f5_pp, t.vs_spy_f10_pp, t.vs_spy_f20_pp
FROM t
JOIN nd ON nd.decision_day = t.decision_day
JOIN sig ON sig.market_id = t.market_id AND sig.date = nd.news_day;
