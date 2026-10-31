-- v_dive_signals: the signal history (v_signal_history) with the symbol
-- picker key (symbol_label from v_universe_latest), so the one-symbol page
-- filters it with the same control as its other sections.
CREATE OR REPLACE VIEW `{{DS}}.v_dive_signals`
OPTIONS (description = "TFB Looker v3: signal history with the symbol picker key")
AS
SELECT
  COALESCE(l.symbol_label, s.symbol) AS symbol_label,
  s.*
FROM `{{DS}}.v_signal_history` AS s
LEFT JOIN `{{DS}}.v_universe_latest` AS l ON l.symbol = s.symbol;
