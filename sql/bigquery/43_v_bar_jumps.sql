-- v_bar_jumps: every session in the nightly bar archive (symbol_bars_daily) whose close is 1.5 times the previous
-- close or more, or two thirds of it or less - and whether a later jump undid it.
-- WHY (full project review of 27 Nov 2026, plan item 'Split and bad-print check on the nightly bars'; owner,
-- 27 Nov 2026 16:30 Riyadh: "ok let's start do your recommdadtions and optimzation phase"): every return in the
-- research views is a ratio of two archived closes. A bar recorded on another price basis (a split applied to
-- part of the history, a bad print) or a split recorded without adjustment becomes a return of +200 % or -70 %
-- that never happened, and no view lists such bars. On 27 Nov the archive held 107 of them on 29 symbols.
-- THE RULE (fixed on 27 Nov 2026):
--   jump       close >= 1.5 x the previous archived close, or close x 1.5 <= it. Bars with a close of 0 or less
--              are left out. The previous bar is the one before it in the archive, whatever the gap (gap_days).
--   undone     a jump is undone by the NEXT jump of the same symbol when the product of the two ratios is between
--              0.8 and 1.25 and the two are at most 60 days apart: the price went to another level and came back.
--   leg        'out' and 'back' for the two jumps of such a pair (a chain of them alternates out, back, out ...),
--              'alone' for a jump that no later jump undid.
--   bars_away  the bars from an 'out' jump to its 'back' jump (the same number on both legs).
--   kind       'spike'    out and back within 3 bars: one to three bars on another level
--              'detour'   out and back after more than 3 bars: a stretch of the series on another level
--              'step'     alone, with 3 or more later bars: the level changed and stayed - a split or a reverse
--                         split recorded without adjustment, or a real move (price alone cannot tell them apart)
--              'too new'  alone, with fewer than 3 later bars: not yet known
-- READ IT AS: a 'spike' or a 'detour' is a bad bar or two price bases in one series, and a return measured from
-- or to one of its bars is wrong. A 'step' of 3 times or more in one session is rarely a real move.
-- COLUMNS: symbol, date, prev_date, gap_days, prev_close, close, ratio (rounded, to read), factor (the larger of
-- close / previous close and its inverse, NOT rounded - v_bar_jump_symbols tests it against 3, and a rounded ratio
-- of 0.333 would turn a factor of 2.9994 into 3.003: review of PR 1107), direction (up / down), volume, source,
-- leg, bars_away, back_date (on the 'out' leg), bars_after (bars archived after this one), kind.
-- This view looks at later bars on purpose: it checks the archive, it is not a signal.
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_bar_jumps`
OPTIONS (description = "TFB bar archive check (views 1.36.0): sessions whose close is 1.5 times the previous archived close or more, or two thirds of it or less, with the later jump that undid them - spike, detour, step or too new - research only")
AS
WITH b AS (
  SELECT
    symbol, date, close, volume, source,
    LAG(close) OVER w AS prev_close,
    LAG(date) OVER w AS prev_date,
    ROW_NUMBER() OVER w AS bar_n,
    COUNT(*) OVER (PARTITION BY symbol) AS bars
  FROM `{{DS}}.symbol_bars_daily`
  WHERE close > 0
  WINDOW w AS (PARTITION BY symbol ORDER BY date)
),
j AS (
  SELECT
    symbol, date, prev_date, prev_close, close, volume, source, bar_n, bars,
    SAFE_DIVIDE(close, prev_close) AS ratio,
    LAG(SAFE_DIVIDE(close, prev_close)) OVER s AS prev_ratio,
    LAG(date) OVER s AS prev_jump_date
  FROM b
  WHERE prev_close > 0 AND (close >= 1.5 * prev_close OR close * 1.5 <= prev_close)
  WINDOW s AS (PARTITION BY symbol ORDER BY date)
),
c AS (
  SELECT
    *,
    -- this jump undoes the one before it: the product of the two ratios is near 1
    IFNULL(ratio * prev_ratio BETWEEN 0.8 AND 1.25 AND DATE_DIFF(date, prev_jump_date, DAY) <= 60, FALSE) AS undoes_prev
  FROM j
),
ch AS (
  SELECT *, COUNTIF(NOT undoes_prev) OVER (PARTITION BY symbol ORDER BY date) AS chain
  FROM c
),
r AS (
  SELECT
    *,
    ROW_NUMBER() OVER k AS chain_n,
    COUNT(*) OVER (PARTITION BY symbol, chain) AS chain_len,
    LAG(bar_n) OVER k AS chain_prev_bar,
    LEAD(bar_n) OVER k AS chain_next_bar,
    LEAD(date) OVER k AS chain_next_date
  FROM ch
  WINDOW k AS (PARTITION BY symbol, chain ORDER BY date)
),
x AS (
  SELECT
    *,
    CASE WHEN MOD(chain_n, 2) = 0 THEN 'back' WHEN chain_n < chain_len THEN 'out' ELSE 'alone' END AS leg,
    CASE WHEN MOD(chain_n, 2) = 0 THEN bar_n - chain_prev_bar WHEN chain_n < chain_len THEN chain_next_bar - bar_n END AS bars_away
  FROM r
)
SELECT
  symbol,
  date,
  prev_date,
  DATE_DIFF(date, prev_date, DAY) AS gap_days,
  ROUND(prev_close, 4) AS prev_close,
  ROUND(close, 4) AS close,
  ROUND(ratio, 3) AS ratio,
  GREATEST(SAFE_DIVIDE(close, prev_close), SAFE_DIVIDE(prev_close, close)) AS factor,
  IF(ratio > 1, 'up', 'down') AS direction,
  volume,
  source,
  leg,
  bars_away,
  IF(leg = 'out', chain_next_date, NULL) AS back_date,
  bars - bar_n AS bars_after,
  CASE
    WHEN leg = 'alone' AND bars - bar_n < 3 THEN 'too new'
    WHEN leg = 'alone' THEN 'step'
    WHEN bars_away <= 3 THEN 'spike'
    ELSE 'detour'
  END AS kind
FROM x;
