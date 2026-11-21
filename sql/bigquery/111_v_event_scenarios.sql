-- v_event_scenarios: for each event (held, or ahead) and each market, what the
-- earlier events of the same family did - the scenarios a reader sees before
-- the event, and the forecasts v_event_trial scores after it.
-- WHY (owner, 21 Nov 2026 11:03 Riyadh, item 5 of the build list): "forward
-- event scenarios; momentum/surprise ... score every news signal"; the
-- design of 16 Nov 2026: every scenario carries its probability, is logged
-- before the outcome and scored after it (Brier score).
-- READ FROM every earlier event of the family (event_at before this one's)
--   with a move in v_event_reactions - so a past event's scenario is what
--   could have been known before it, and an event ahead reads them all:
--   n_prior                       the earlier events behind the market's row
--   r1_p10 / r1_p50 / r1_p90      the event-day move (r1_pct): the band of
--                                 the middle 80 % and the median (exact,
--                                 PERCENTILE_CONT)
--   p_big                         the chance of a move of 1 normal day or more
--                                 (up or down): (big + 1) / (moves + 2)
--   mean_abs_z                    the mean size of the move, in normal days
--   n_above / n_in_line / n_below the earlier events by their figure against
--                                 the forecast (v_event_calendar.figure)
--   r1_p50_above / _in_line / _below  the median event-day move after each
--   f5_p50_above / _below, n_f5_above / _below  the median of what followed
--                                 the event close (f5_pct) after each
--   n_fig, p_above / p_in_line / p_below  the family's figures (every market
--                                 alike): (count + 1) / (n_fig + 3)
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it. Observe only.
CREATE OR REPLACE VIEW `{{DS}}.v_event_scenarios`
OPTIONS (description = "TFB events v1: one row per event (held or ahead) and market - the earlier events of the same family as scenarios: the band of their event-day moves (p10 / p50 / p90), the chance of a move of a normal day or more, the median move after a figure above / in line / below its forecast, and how often the family's figures came in above / in line / below. Observe only.")
AS
WITH ev AS (
  SELECT event_id, family, family_name, kind, country, event_at, event_day, period, actual, estimate, previous,
         surprise, surprise_z, figure, is_out
  FROM `{{DS}}.v_event_calendar`
),
re AS (
  SELECT event_id, family, event_at, figure, market_order, market_id, market, r1_pct, f5_pct, z1, move
  FROM `{{DS}}.v_event_reactions`
  WHERE r1_pct IS NOT NULL
),
pairs AS (
  SELECT e.event_id, r.market_order, r.market_id, r.market, r.figure AS prior_figure, r.r1_pct, r.f5_pct, r.z1, r.move
  FROM ev AS e
  JOIN re AS r ON r.family = e.family AND r.event_at < e.event_at
),
pw AS (
  SELECT
    p.*,
    PERCENTILE_CONT(p.r1_pct, 0.1) OVER (PARTITION BY p.event_id, p.market_id) AS q10,
    PERCENTILE_CONT(p.r1_pct, 0.5) OVER (PARTITION BY p.event_id, p.market_id) AS q50,
    PERCENTILE_CONT(p.r1_pct, 0.9) OVER (PARTITION BY p.event_id, p.market_id) AS q90,
    PERCENTILE_CONT(p.r1_pct, 0.5) OVER (PARTITION BY p.event_id, p.market_id, p.prior_figure) AS q50_fig,
    PERCENTILE_CONT(p.f5_pct, 0.5) OVER (PARTITION BY p.event_id, p.market_id, p.prior_figure) AS f5_q50_fig
  FROM pairs AS p
),
agg AS (
  SELECT
    event_id, market_id,
    ANY_VALUE(market_order) AS market_order, ANY_VALUE(market) AS market,
    COUNT(*) AS n_prior,
    ANY_VALUE(q10) AS r1_p10, ANY_VALUE(q50) AS r1_p50, ANY_VALUE(q90) AS r1_p90,
    COUNTIF(move IN ('up', 'down')) AS n_big,
    COUNTIF(move IS NOT NULL) AS n_move,
    AVG(ABS(z1)) AS mean_abs_z,
    COUNTIF(prior_figure = 'above') AS n_above,
    COUNTIF(prior_figure = 'in_line') AS n_in_line,
    COUNTIF(prior_figure = 'below') AS n_below,
    MAX(IF(prior_figure = 'above', q50_fig, NULL)) AS r1_p50_above,
    MAX(IF(prior_figure = 'in_line', q50_fig, NULL)) AS r1_p50_in_line,
    MAX(IF(prior_figure = 'below', q50_fig, NULL)) AS r1_p50_below,
    MAX(IF(prior_figure = 'above', f5_q50_fig, NULL)) AS f5_p50_above,
    MAX(IF(prior_figure = 'below', f5_q50_fig, NULL)) AS f5_p50_below,
    COUNTIF(prior_figure = 'above' AND f5_pct IS NOT NULL) AS n_f5_above,
    COUNTIF(prior_figure = 'below' AND f5_pct IS NOT NULL) AS n_f5_below
  FROM pw
  GROUP BY event_id, market_id
),
fig AS (
  SELECT
    e.event_id,
    COUNTIF(p.figure IS NOT NULL) AS n_fig,
    COUNTIF(p.figure = 'above') AS f_above,
    COUNTIF(p.figure = 'in_line') AS f_in_line,
    COUNTIF(p.figure = 'below') AS f_below
  FROM ev AS e
  JOIN ev AS p ON p.family = e.family AND p.event_at < e.event_at AND p.is_out
  GROUP BY e.event_id
)
SELECT
  e.event_id, e.family, e.family_name, e.kind, e.country, e.event_at, e.event_day, e.period,
  e.actual, e.estimate, e.previous, e.surprise, e.surprise_z, e.figure, e.is_out,
  a.market_order, a.market_id, a.market, a.n_prior,
  ROUND(a.r1_p10, 4) AS r1_p10, ROUND(a.r1_p50, 4) AS r1_p50, ROUND(a.r1_p90, 4) AS r1_p90,
  a.n_big, a.n_move,
  ROUND(SAFE_DIVIDE(a.n_big + 1, a.n_move + 2), 4) AS p_big,
  ROUND(a.mean_abs_z, 3) AS mean_abs_z,
  a.n_above, a.n_in_line, a.n_below,
  ROUND(a.r1_p50_above, 4) AS r1_p50_above, ROUND(a.r1_p50_in_line, 4) AS r1_p50_in_line,
  ROUND(a.r1_p50_below, 4) AS r1_p50_below,
  ROUND(a.f5_p50_above, 4) AS f5_p50_above, ROUND(a.f5_p50_below, 4) AS f5_p50_below,
  a.n_f5_above, a.n_f5_below,
  IFNULL(f.n_fig, 0) AS n_fig,
  IF(IFNULL(f.n_fig, 0) = 0, NULL, ROUND((f.f_above + 1) / (f.n_fig + 3), 4)) AS p_above,
  IF(IFNULL(f.n_fig, 0) = 0, NULL, ROUND((f.f_in_line + 1) / (f.n_fig + 3), 4)) AS p_in_line,
  IF(IFNULL(f.n_fig, 0) = 0, NULL, ROUND((f.f_below + 1) / (f.n_fig + 3), 4)) AS p_below
FROM ev AS e
JOIN agg AS a USING (event_id)
LEFT JOIN fig AS f USING (event_id);
