-- v_dive_events: the newest Calendar_Events row of each name (next earnings
-- and ex-dividend dates, last report and EPS surprise) with the symbol
-- picker key, for the one-symbol page.
CREATE OR REPLACE VIEW `{{DS}}.v_dive_events`
OPTIONS (description = "TFB Looker v3: next earnings and ex-dividend dates with the symbol picker key (newest snapshot)")
AS
SELECT
  COALESCE(l.symbol_label, c.symbol) AS symbol_label,
  c.*
FROM `{{DS}}.v_calendar_events` AS c
LEFT JOIN `{{DS}}.v_universe_latest` AS l ON l.symbol = c.symbol
WHERE c.is_latest;
