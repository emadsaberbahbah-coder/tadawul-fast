-- v_bars_clean: the nightly bar archive (symbol_bars_daily) with its recording errors taken out, for the views that
-- compute returns from it - and a price basis per bar, so that no window spans a jump the archive cannot explain.
-- WHY (full project review of 27 Nov 2026, plan item 'Split and bad-print check on the nightly bars'; owner,
-- 27 Nov 2026 16:30 Riyadh: "ok let's start do your recommdadtions and optimzation phase"): v_bar_jumps found 107
-- one-session jumps of 1.5 times or more on 29 symbols. symbol_bars_daily is Yahoo's own history, reloaded whole
-- every night, so a jump is in Yahoo's data: a bad print, a stretch on another price basis, a reverse split left
-- unadjusted, or a real move. 27 of the 29 symbols are in the shadow challenger's books; a label that spans one
-- of them averaged 302 points of excess in the US and 98 in Saudi, against 7 and 6 for the others.
-- THE RULE (fixed on 27 Nov 2026), from v_bar_jumps:
--   removed     the bars of a 'spike' (from its 'out' jump up to the bar before its 'back' jump): one to three bars
--               on another level that came back; without them the series runs on as it was.
--   basis       a new price basis starts on every jump of a 'detour' (out and back after more than 3 bars: a whole
--               stretch on another level, or a crash and a recovery - the archive cannot tell) and on every jump of
--               3 times or more that nothing undid ('step' or 'too new'). basis = the number of such jumps of the
--               symbol on or before the date. A window partitioned by symbol and basis never spans one.
--   kept        a jump under 3 times that nothing undid: a real move is possible, so the series stays whole.
-- This view reads later bars on purpose (whether a jump came back): it removes recording errors, it never reads a
-- result. The shadow challenger's inputs and results read it from views 1.37.0.
-- COLUMNS: the columns of symbol_bars_daily (bars with a close above 0), and basis.
-- Research only: no decision, gate or count reads this view.
CREATE OR REPLACE VIEW `{{DS}}.v_bars_clean`
OPTIONS (description = "TFB bar archive without its recording errors (views 1.37.0): symbol_bars_daily less the bars of the one-to-three-bar spikes of v_bar_jumps, with a price basis that starts anew at every detour jump and every unexplained jump of 3 times or more - research only")
AS
WITH j AS (
  SELECT symbol, date, back_date, leg, kind, factor
  FROM `{{DS}}.v_bar_jumps`
),
spike_bars AS (
  SELECT symbol, date AS d0, back_date AS d1
  FROM j
  WHERE kind = 'spike' AND leg = 'out'
),
breaks AS (
  SELECT DISTINCT symbol, date
  FROM j
  WHERE kind = 'detour' OR (leg = 'alone' AND factor >= 3)
),
b AS (
  SELECT x.*
  FROM `{{DS}}.symbol_bars_daily` AS x
  LEFT JOIN spike_bars AS s ON s.symbol = x.symbol AND x.date >= s.d0 AND x.date < s.d1
  WHERE x.close > 0 AND s.symbol IS NULL
),
days AS (
  SELECT DISTINCT symbol, date FROM b
),
basis AS (
  SELECT d.symbol, d.date, COUNT(k.date) AS basis
  FROM days AS d
  LEFT JOIN breaks AS k ON k.symbol = d.symbol AND k.date <= d.date
  GROUP BY d.symbol, d.date
)
SELECT b.*, s.basis
FROM b
JOIN basis AS s USING (symbol, date);
