-- v_regime_shadow_insample: what each label of the shadow regime rule (v_regime_shadow, rule_version 1) and each of
-- TFB's own condition labels (v_market_condition) were followed by, per market, on the closes before the rule was
-- fixed. WHY (week plan of 23 Nov 2026, package 6): a regime rule is only worth a gate if RISK_OFF closes are
-- followed by worse returns than BULL ones, per market; this is the in-sample answer, read once, before the forward
-- record starts. IN-SAMPLE, NOT EVIDENCE: the thresholds were chosen on 28 Nov 2026 with this history in view; the
-- forward record is regime_shadow_log (closes after 28 Nov 2026, frozen daily by scripts/regime_shadow.py) and
-- its scorecard follows when its first 21-close results are in.
-- RESULTS: the market's own index (v_market_condition's close; the US SPY price close) 5 and 21 closes later.
-- COLUMNS: rule ('shadow v1' or 'condition'), market_id, label, days, first_date, last_date,
--   mean_fwd5_pct / mean_fwd21_pct   the mean result 5 / 21 closes later
--   hit21_pct                        the share of days the index stood higher 21 closes later
--   excess5_pp / excess21_pp         the label's mean less the market's mean over every labelled day of the sample
--   t5_adj / t21_adj                 that excess over its standard deviation x SQRT(days / 5 or 21): overlapping
--                                    windows counted once (the project's overlap-adjusted t)
--   reading                          'in-sample to 2026-11-28: the rule was fixed after this history, not evidence'
-- Research view: no page, gate, pick or count reads it. (views 1.38.0)
CREATE OR REPLACE VIEW `{{DS}}.v_regime_shadow_insample`
OPTIONS (description = "TFB shadow regime rule v1, in-sample (views 1.38.0): per market and label - the shadow BULL / CHOPPY / RISK_OFF and TFB's own condition labels - the index result 5 and 21 closes later, the hit rate, the excess over the market's mean and an overlap-adjusted t, on the closes before the rule was fixed on 28 Nov 2026. Not evidence, research only.")
AS
WITH fwd AS (
  SELECT market_id, date,
    SAFE_DIVIDE(LEAD(close, 5) OVER w, close) - 1 AS f5,
    SAFE_DIVIDE(LEAD(close, 21) OVER w, close) - 1 AS f21
  FROM `{{DS}}.v_market_condition`
  WHERE market_id IN ('us', 'sa', 'eu', 'jp', 'cn') AND close > 0
  WINDOW w AS (PARTITION BY market_id ORDER BY date)
),
labelled AS (
  SELECT 'shadow v1' AS rule, r.market_id, r.date, r.regime AS label, f.f5, f.f21
  FROM `{{DS}}.v_regime_shadow` AS r
  JOIN fwd AS f USING (market_id, date)
  WHERE r.regime IS NOT NULL AND r.date <= DATE '2026-11-28'
  UNION ALL
  SELECT 'condition', r.market_id, r.date, r.condition_label, f.f5, f.f21
  FROM `{{DS}}.v_regime_shadow` AS r
  JOIN fwd AS f USING (market_id, date)
  WHERE r.regime IS NOT NULL AND r.condition_label IS NOT NULL AND r.date <= DATE '2026-11-28'
),
mkt AS (
  SELECT rule, market_id, AVG(f5) AS m5, AVG(f21) AS m21
  FROM labelled
  GROUP BY rule, market_id
),
x AS (
  SELECT l.*, l.f5 - m.m5 AS x5, l.f21 - m.m21 AS x21
  FROM labelled AS l
  JOIN mkt AS m USING (rule, market_id)
)
SELECT
  rule, market_id, label,
  COUNT(*) AS days, MIN(date) AS first_date, MAX(date) AS last_date,
  ROUND(100 * AVG(f5), 2) AS mean_fwd5_pct,
  ROUND(100 * AVG(f21), 2) AS mean_fwd21_pct,
  ROUND(100 * SAFE_DIVIDE(COUNTIF(f21 > 0), COUNT(f21)), 1) AS hit21_pct,
  ROUND(100 * AVG(x5), 2) AS excess5_pp,
  ROUND(100 * AVG(x21), 2) AS excess21_pp,
  ROUND(SAFE_DIVIDE(AVG(x5), STDDEV_SAMP(x5)) * SQRT(COUNT(x5) / 5), 2) AS t5_adj,
  ROUND(SAFE_DIVIDE(AVG(x21), STDDEV_SAMP(x21)) * SQRT(COUNT(x21) / 21), 2) AS t21_adj,
  'in-sample to 2026-11-28: the rule was fixed after this history, not evidence' AS reading
FROM x
GROUP BY rule, market_id, label;
