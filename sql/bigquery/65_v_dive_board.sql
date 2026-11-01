-- v_dive_board: every Top-10 seat episode (v_board_episodes) with the symbol
-- picker key, for the one-symbol page's board history.
CREATE OR REPLACE VIEW `{{DS}}.v_dive_board`
OPTIONS (description = "TFB Looker v3: Top-10 seat episodes with the symbol picker key")
AS
SELECT
  COALESCE(l.symbol_label, b.symbol) AS symbol_label,
  b.*
FROM `{{DS}}.v_board_episodes` AS b
LEFT JOIN `{{DS}}.v_universe_latest` AS l ON l.symbol = b.symbol;
