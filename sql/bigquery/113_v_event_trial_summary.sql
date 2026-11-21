-- v_event_trial_summary: the scorecard of the event scenarios - for each
-- period, family and market (and all families, all markets), how often the
-- band held, whether the family's chance of a big move beat any day's, how
-- often the direction after a figure held, and whether the family's figure
-- chances beat equal thirds.
-- WHY (owner, 21 Nov 2026 11:03 Riyadh, items 5 and 6 of the build list):
-- "score every news signal" - "Observe for two weeks (10 sessions), then
-- review with the owner, and switch on only what proved itself."
-- READ FROM v_event_trial (one pass, GROUPING SETS; a market row of an event
-- counts once; the figure lines count each event once):
--   events, rows          the events and the event-market rows behind the line
--   band_n, band_pct      the rows with a band and the share inside it (a
--                         true band: 80 %)
--   big_n, big_pct        the rows with a big-move chance, and the share that
--                         moved 1 normal day or more; any_pct = the share any
--                         day does
--   big_skill             1 - Brier(family chance) / Brier(any day's share):
--                         above 0 = the family's chance told more than any day
--   dir_n, dir_events     the rows scored for direction, and their events
--   dir_pct, dir_z        the share that held and its z against a coin: the
--                         sum of (hit - 1/2) over the square root of the sum
--                         of its squares, each event's markets summed first
--                         on an all-markets line (the markets of one event
--                         move together, so an event counts once: on a
--                         one-market line this is (hits - n/2) / sqrt(n/4))
--   drift_n, drift_events, drift_pct, drift_z  the same for what followed the
--                         event close
--   fig_n, fig_skill      (the family lines and the all line) the events with
--                         a figure scored, and 1 - Brier / (2/3): above 0 =
--                         the family's chances beat equal thirds
--   verdict               a forward line reads 'collecting' until it has 10
--                         events; then: band 'calibrated' (70-90 %), 'too
--                         narrow' or 'too wide' at 20 rows or more;
--                         direction 'holds' at z 2 or more over 20 events or
--                         more, else 'not proven'
-- Research view: the live system never reads BigQuery; no decision, gate,
-- count or broker action depends on it. Observe only.
CREATE OR REPLACE VIEW `{{DS}}.v_event_trial_summary`
OPTIONS (description = "TFB events v1: the scorecard of the event scenarios per period, family and market (and all) - band coverage against 80 %, the big-move chance against any day's (Brier skill), the direction and drift hits after a figure with a z against a coin (each event once), and the figure chances against equal thirds - forward lines read 'collecting' until 10 events. Observe only.")
AS
WITH t0 AS (
  SELECT
    *,
    IF(dir_hit IS NULL, NULL, IF(dir_hit, 0.5, -0.5)) AS d_dir,
    IF(drift_hit IS NULL, NULL, IF(drift_hit, 0.5, -0.5)) AS d_drift
  FROM `{{DS}}.v_event_trial`
),
t AS (
  -- each row carries its share of its event's squared deviation, so that a sum over all of an event's rows gives it
  SELECT
    *,
    SAFE_DIVIDE(POW(SUM(d_dir) OVER (PARTITION BY period, event_id), 2), COUNT(*) OVER (PARTITION BY period, event_id))
      AS w_dir,
    SAFE_DIVIDE(POW(SUM(d_drift) OVER (PARTITION BY period, event_id), 2), COUNT(*) OVER (PARTITION BY period, event_id))
      AS w_drift
  FROM t0
),
grp AS (
  SELECT
    period, family, market_id,
    GROUPING(family) AS g_family, GROUPING(market_id) AS g_market,
    ANY_VALUE(family_name) AS any_family_name, ANY_VALUE(market) AS any_market,
    ANY_VALUE(market_order) AS any_market_order,
    COUNT(DISTINCT event_id) AS events,
    COUNT(*) AS n_rows,
    COUNTIF(band_hit IS NOT NULL) AS band_n,
    ROUND(100 * AVG(IF(band_hit, 1, IF(band_hit IS NULL, NULL, 0))), 1) AS band_pct,
    COUNTIF(brier_big IS NOT NULL) AS big_n,
    ROUND(100 * AVG(IF(brier_big IS NULL, NULL, big)), 1) AS big_pct,
    ROUND(100 * AVG(IF(brier_big IS NULL, NULL, p_big_any)), 1) AS any_pct,
    ROUND(1 - SAFE_DIVIDE(AVG(IF(brier_any IS NULL, NULL, brier_big)), AVG(brier_any)), 3) AS big_skill,
    COUNTIF(dir_hit IS NOT NULL) AS dir_n,
    COUNT(DISTINCT IF(dir_hit IS NULL, NULL, event_id)) AS dir_events,
    ROUND(100 * AVG(IF(dir_hit, 1, IF(dir_hit IS NULL, NULL, 0))), 1) AS dir_pct,
    SAFE_DIVIDE(SUM(d_dir), SQRT(SUM(d_dir * d_dir))) AS dir_z_row,
    SAFE_DIVIDE(SUM(d_dir), SQRT(SUM(w_dir))) AS dir_z_event,
    COUNTIF(drift_hit IS NOT NULL) AS drift_n,
    COUNT(DISTINCT IF(drift_hit IS NULL, NULL, event_id)) AS drift_events,
    ROUND(100 * AVG(IF(drift_hit, 1, IF(drift_hit IS NULL, NULL, 0))), 1) AS drift_pct,
    SAFE_DIVIDE(SUM(d_drift), SQRT(SUM(d_drift * d_drift))) AS drift_z_row,
    SAFE_DIVIDE(SUM(d_drift), SQRT(SUM(w_drift))) AS drift_z_event
  FROM t
  GROUP BY GROUPING SETS ((period, family, market_id), (period, family), (period, market_id), (period))
),
lines AS (
  SELECT
    g.* EXCEPT (family, market_id, g_family, g_market, any_family_name, any_market, any_market_order,
                dir_z_row, dir_z_event, drift_z_row, drift_z_event),
    -- an all-markets line counts each event once in its z
    ROUND(IF(g.g_market = 1, g.dir_z_event, g.dir_z_row), 2) AS dir_z,
    ROUND(IF(g.g_market = 1, g.drift_z_event, g.drift_z_row), 2) AS drift_z,
    IF(g.g_family = 1, 'all', g.family) AS family,
    IF(g.g_market = 1, 'all', g.market_id) AS market_id,
    IF(g.g_family = 1, 'All families', g.any_family_name) AS family_name,
    IF(g.g_market = 1, 'All markets', g.any_market) AS market,
    IF(g.g_market = 1, 0, g.any_market_order) AS market_order
  FROM grp AS g
),
fig AS (
  SELECT period, family, ANY_VALUE(brier_figure) AS brier_figure
  FROM t
  WHERE brier_figure IS NOT NULL
  GROUP BY period, family, event_id
),
figs AS (
  SELECT
    period, family, GROUPING(family) AS g_family,
    COUNT(*) AS fig_n,
    ROUND(1 - AVG(brier_figure) / (2 / 3), 3) AS fig_skill
  FROM fig
  GROUP BY GROUPING SETS ((period, family), (period))
),
figl AS (
  SELECT period, IF(g_family = 1, 'all', family) AS family, fig_n, fig_skill
  FROM figs
)
SELECT
  r.period, r.family, r.family_name, r.market_order, r.market_id, r.market, r.events, r.n_rows,
  r.band_n, r.band_pct, r.big_n, r.big_pct, r.any_pct, r.big_skill,
  r.dir_n, r.dir_events, r.dir_pct, r.dir_z, r.drift_n, r.drift_events, r.drift_pct, r.drift_z,
  IF(r.market_id = 'all', f.fig_n, NULL) AS fig_n,
  IF(r.market_id = 'all', f.fig_skill, NULL) AS fig_skill,
  CASE
    WHEN r.period = 'forward' AND r.events < 10 THEN 'collecting'
    ELSE CONCAT(
      'band ',
      CASE WHEN r.band_n < 20 THEN 'too few' WHEN r.band_pct < 70 THEN 'too narrow' WHEN r.band_pct > 90 THEN 'too wide'
           ELSE 'calibrated' END,
      ', direction ',
      CASE WHEN r.dir_events < 20 THEN 'too few' WHEN r.dir_z >= 2 THEN 'holds' ELSE 'not proven' END)
  END AS verdict
FROM lines AS r
LEFT JOIN figl AS f ON f.period = r.period AND f.family = r.family;
