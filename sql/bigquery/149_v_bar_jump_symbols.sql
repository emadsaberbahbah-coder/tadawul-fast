-- v_bar_jump_symbols: one row per symbol of v_bar_jumps - how many jumps its archived closes hold, of which kind,
-- and what that says about the series.
-- WHY (full project review of 27 Nov 2026, plan item 'Split and bad-print check on the nightly bars'): the list a
-- person reads before trusting a return on that symbol, and the list a morning check reads for new jumps.
-- THE READING (fixed on 27 Nov 2026), in this order:
--   'two price bases'     at least one detour: a stretch of the series sits on another level
--   'bad bar'             at least one spike: one to three bars on another level
--   'large step'          a step of 3 times or more in one session: a split or a reverse split recorded without
--                         adjustment is likely
--   'too new'             only jumps with fewer than 3 later bars
--   'steps under 3 times' steps only, each under 3 times: a real move is possible (price alone cannot tell)
-- COLUMNS: symbol, market_group (Tadawul for .SR, US for .US or no suffix, else Other markets), jumps, spikes,
-- detours (pairs, counted once), steps, too_new, bars_on_another_level (bars inside the spikes and detours),
-- largest_factor (the largest one-session factor, up or down), largest_step_factor, first_jump, last_jump,
-- is_new (the last jump is within 7 days of the archive's newest bar), reading.
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_bar_jump_symbols`
OPTIONS (description = "TFB bar archive check by symbol (views 1.36.0): jumps of 1.5 times or more in the archived closes per symbol - spikes, detours, steps, the largest factor, whether the last one is new - and a fixed reading - research only")
AS
WITH a AS (
  SELECT
    symbol,
    COUNT(*) AS jumps,
    COUNTIF(kind = 'spike' AND leg = 'out') AS spikes,
    COUNTIF(kind = 'detour' AND leg = 'out') AS detours,
    COUNTIF(kind = 'step') AS steps,
    COUNTIF(kind = 'too new') AS too_new,
    SUM(IF(leg = 'out', bars_away, 0)) AS bars_on_another_level,
    MAX(GREATEST(ratio, SAFE_DIVIDE(1, ratio))) AS f_max,
    MAX(IF(kind = 'step', GREATEST(ratio, SAFE_DIVIDE(1, ratio)), NULL)) AS f_step,
    MIN(date) AS first_jump,
    MAX(date) AS last_jump
  FROM `{{DS}}.v_bar_jumps`
  GROUP BY symbol
),
newest AS (
  SELECT MAX(date) AS d FROM `{{DS}}.symbol_bars_daily`
)
SELECT
  a.symbol,
  CASE
    WHEN ENDS_WITH(a.symbol, '.SR') THEN 'Tadawul'
    WHEN ENDS_WITH(a.symbol, '.US') OR NOT CONTAINS_SUBSTR(a.symbol, '.') THEN 'US'
    ELSE 'Other markets'
  END AS market_group,
  a.jumps,
  a.spikes,
  a.detours,
  a.steps,
  a.too_new,
  a.bars_on_another_level,
  ROUND(a.f_max, 2) AS largest_factor,
  ROUND(a.f_step, 2) AS largest_step_factor,
  a.first_jump,
  a.last_jump,
  DATE_DIFF(n.d, a.last_jump, DAY) <= 7 AS is_new,
  CASE
    WHEN a.detours >= 1 THEN 'two price bases'
    WHEN a.spikes >= 1 THEN 'bad bar'
    WHEN a.f_step >= 3 THEN 'large step'
    WHEN a.steps = 0 THEN 'too new'
    ELSE 'steps under 3 times'
  END AS reading
FROM a
CROSS JOIN newest AS n;
