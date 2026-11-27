-- v_challenger_v0_walkforward: the shadow challenger v0 with weights learned walk-forward - on each formation date,
-- each input's weight comes only from earlier formation dates whose results had all closed before that date - and
-- the shadow action and shadow size per name.
-- WHY (week plan of 23 Nov 2026, package 5): an equal mix of the four inputs assumes each one works in each market.
-- The audit found momentum clear only on Tadawul, weak in the US and negative elsewhere, so each market learns its
-- own mix, and only from what it could have known.
-- THE WEIGHTS of market m on formation date D: for each input, its rank correlation with label_pr (the input's dev_*
-- against the result's percentile among the market's names) on every earlier formation date d of m with at least 20
-- results (an input counts on a date only when at least 20 of the names have a rank for it), kept only when the LAST
-- result of d closed before D (exit_max < D: a result still running on D is purged, so no date trains on an outcome
-- it could not have seen). w = the mean correlation of each input, negatives set to 0, scaled to sum to 1.
-- weights_basis: 'equal (warm-up)' (each 0.25) while fewer than 12 dates trained; 'no input positive' (every weight
-- 0, no view) when no input has a positive mean; else 'trained'.
-- score_wf = 50 + 100 x the weighted sum of the dev_* (0 to 100). q_wf / q_eq = the quintile of score_wf /
-- score_eq among the market's names on D (5 the highest); q_wf is empty when there is no view.
-- SHADOW ACTION (of score_wf): 'shadow buy' in quintile 5, 'shadow avoid' in quintile 1, else 'shadow hold', 'no
-- view' when the weights are all 0. SHADOW SIZE: for a shadow buy, its share of the market's shadow buys on D in
-- inverse volatility (1 / vol63), capped at 10 per cent; 0 otherwise. is_latest marks the market's last formation
-- date. Nothing reads the action or the size: they are recorded so the challenger can be judged on dates to come.
-- Research only: no decision, gate or count reads this view. (views 1.34.0)
CREATE OR REPLACE VIEW `{{DS}}.v_challenger_v0_walkforward`
OPTIONS (description = "TFB shadow challenger v0 walk-forward (views 1.34.0): per market and formation date, each input's weight from earlier dates whose results had closed (purged), the weighted score, its quintile, and a shadow action and shadow size per name - recorded, never read for a decision. Research view, never an order.")
AS
WITH r AS (
  SELECT * FROM `{{DS}}.v_challenger_v0_ranks`
),
ic AS (
  SELECT market, d, COUNT(*) AS n, MAX(exit_date) AS exit_max,
         IF(COUNTIF(dev_m12 != 0) >= 20, CORR(dev_m12, label_pr), NULL) AS ic_m12,
         IF(COUNTIF(dev_m6 != 0) >= 20, CORR(dev_m6, label_pr), NULL) AS ic_m6,
         IF(COUNTIF(dev_rs != 0) >= 20, CORR(dev_rs, label_pr), NULL) AS ic_rs,
         IF(COUNTIF(dev_dd != 0) >= 20, CORR(dev_dd, label_pr), NULL) AS ic_dd
  FROM r
  WHERE label_pr IS NOT NULL
  GROUP BY market, d
  HAVING COUNT(*) >= 20
),
fdates AS (
  SELECT DISTINCT market, d FROM r
),
learned AS (
  SELECT f.market, f.d, COUNT(i.d) AS train_dates,
         AVG(i.ic_m12) AS ic_m12, AVG(i.ic_m6) AS ic_m6, AVG(i.ic_rs) AS ic_rs, AVG(i.ic_dd) AS ic_dd
  FROM fdates AS f
  LEFT JOIN ic AS i ON i.market = f.market AND i.exit_max < f.d
  GROUP BY f.market, f.d
),
pos AS (
  SELECT l.*,
         GREATEST(IFNULL(l.ic_m12, 0), 0) AS p_m12, GREATEST(IFNULL(l.ic_m6, 0), 0) AS p_m6,
         GREATEST(IFNULL(l.ic_rs, 0), 0) AS p_rs, GREATEST(IFNULL(l.ic_dd, 0), 0) AS p_dd
  FROM learned AS l
),
w AS (
  SELECT market, d, train_dates, ic_m12 AS train_ic_m12, ic_m6 AS train_ic_m6, ic_rs AS train_ic_rs,
         ic_dd AS train_ic_dd,
         CASE WHEN train_dates < 12 THEN 'equal (warm-up)'
              WHEN p_m12 + p_m6 + p_rs + p_dd <= 0 THEN 'no input positive' ELSE 'trained' END AS weights_basis,
         CASE WHEN train_dates < 12 THEN 0.25 ELSE SAFE_DIVIDE(p_m12, p_m12 + p_m6 + p_rs + p_dd) END AS w_m12,
         CASE WHEN train_dates < 12 THEN 0.25 ELSE SAFE_DIVIDE(p_m6, p_m12 + p_m6 + p_rs + p_dd) END AS w_m6,
         CASE WHEN train_dates < 12 THEN 0.25 ELSE SAFE_DIVIDE(p_rs, p_m12 + p_m6 + p_rs + p_dd) END AS w_rs,
         CASE WHEN train_dates < 12 THEN 0.25 ELSE SAFE_DIVIDE(p_dd, p_m12 + p_m6 + p_rs + p_dd) END AS w_dd
  FROM pos
),
s AS (
  SELECT r.*, w.train_dates, w.weights_basis, w.train_ic_m12, w.train_ic_m6, w.train_ic_rs, w.train_ic_dd,
         IFNULL(w.w_m12, 0) AS w_m12, IFNULL(w.w_m6, 0) AS w_m6, IFNULL(w.w_rs, 0) AS w_rs, IFNULL(w.w_dd, 0) AS w_dd,
         50 + 100 * (IFNULL(w.w_m12, 0) * r.dev_m12 + IFNULL(w.w_m6, 0) * r.dev_m6
                     + IFNULL(w.w_rs, 0) * r.dev_rs + IFNULL(w.w_dd, 0) * r.dev_dd) AS score_wf
  FROM r
  JOIN w USING (market, d)
),
q AS (
  SELECT s.*,
    IF(s.weights_basis = 'no input positive', NULL,
       1 + LEAST(4, CAST(FLOOR(5 * PERCENT_RANK() OVER (PARTITION BY s.market, s.d ORDER BY s.score_wf)) AS INT64))) AS q_wf,
    1 + LEAST(4, CAST(FLOOR(5 * PERCENT_RANK() OVER (PARTITION BY s.market, s.d ORDER BY s.score_eq)) AS INT64)) AS q_eq
  FROM s
),
act AS (
  SELECT q.*,
    CASE WHEN q.q_wf IS NULL THEN 'no view' WHEN q.q_wf = 5 THEN 'shadow buy' WHEN q.q_wf = 1 THEN 'shadow avoid'
         ELSE 'shadow hold' END AS shadow_action,
    SUM(IF(q.q_wf = 5 AND q.vol63 > 0, 1 / q.vol63, 0)) OVER (PARTITION BY q.market, q.d) AS inv_vol_buys
  FROM q
)
SELECT
  a.* EXCEPT (inv_vol_buys),
  IF(a.shadow_action = 'shadow buy' AND a.vol63 > 0,
     ROUND(LEAST(10, 100 * SAFE_DIVIDE(1 / a.vol63, a.inv_vol_buys)), 2), 0) AS shadow_size_pct,
  a.d = MAX(a.d) OVER (PARTITION BY a.market) AS is_latest
FROM act AS a;
