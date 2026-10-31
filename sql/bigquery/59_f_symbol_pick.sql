-- f_symbol_pick(q, market): the symbols on the market pages that match what
-- the reader typed - a symbol (2222.SR), a bare ticker (2222, AAPL) or part
-- of a company name (aramco) - inside an optional market: a listing country
-- (Saudi Arabia), a region (Gulf), an exchange, a listing suffix (SR) or the
-- company's own country; empty / All = every market. match_rank: 1 exact
-- symbol, 2 exact ticker, 3 exact name, 4 name starts with, 5 symbol starts
-- with, 6 name contains, 7 name holds every typed word (saudi oil).
-- pick_order 1 is the best match (then the larger company); the 20 best are
-- returned. The symbol page reads every section
-- through the best match with Looker text parameters q and market:
--   SELECT * FROM `<project>.<dataset>.f_symbol_pick`(@q, @market)
--   SELECT * FROM `<project>.<dataset>.v_universe` WHERE symbol IN (
--     SELECT symbol FROM `<project>.<dataset>.f_symbol_pick`(@q, @market) WHERE pick_order = 1)
-- (the same filter for v_signal_history, v_performance_log, v_board_episodes,
-- v_selection_log, v_news_items and v_calendar_events).
CREATE OR REPLACE TABLE FUNCTION `{{DS}}.f_symbol_pick`(q STRING, market STRING)
OPTIONS (description = "TFB Looker v2: market-page symbols matching a typed symbol, ticker or company name inside an optional market, best match first")
AS
WITH arg AS (
  SELECT
    UPPER(TRIM(COALESCE(q, ''))) AS uq,
    UPPER(TRIM(COALESCE(market, ''))) AS um
),
c AS (
  SELECT
    u.symbol, u.symbol_label, u.name, u.market_code, u.market_country, u.country, u.region,
    u.exchange, u.asset_class, u.sector, u.currency, u.market_cap, u.source_page,
    CASE
      WHEN UPPER(u.symbol) = a.uq THEN 1
      WHEN u.symbol_base = a.uq THEN 2
      WHEN UPPER(u.name) = a.uq THEN 3
      WHEN STARTS_WITH(UPPER(u.name), a.uq) THEN 4
      WHEN STARTS_WITH(UPPER(u.symbol), a.uq) THEN 5
      WHEN STRPOS(UPPER(u.name), a.uq) > 0 THEN 6
      WHEN (SELECT LOGICAL_AND(STRPOS(UPPER(u.name), w) > 0)
            FROM UNNEST(SPLIT(a.uq, ' ')) AS w WHERE w != '') THEN 7
    END AS match_rank
  FROM `{{DS}}.v_universe_latest` AS u
  CROSS JOIN arg AS a
  WHERE a.uq != ''
    AND (a.um IN ('', 'ALL', 'ANY')
         OR a.um IN (UPPER(u.market_country), UPPER(u.region), UPPER(u.exchange),
                     UPPER(u.market_code), UPPER(u.country)))
)
SELECT
  c.*,
  ROW_NUMBER() OVER (ORDER BY c.match_rank, c.market_cap DESC NULLS LAST, c.symbol) AS pick_order
FROM c
WHERE c.match_rank IS NOT NULL
QUALIFY pick_order <= 20;
