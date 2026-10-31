-- v_universe_latest: the newest archived copy of the four market pages, one
-- row per symbol. Each page contributes its last snapshot of the past 10
-- days (a page that failed to archive keeps its previous copy for at most
-- that long); a symbol listed on two pages keeps the copy captured last.
-- meets_floors marks the rows the Top-10 panel's default floors admit:
-- reliability >= 70 and data quality >= 80 (both 0-100 points) and no block
-- reason. Freshness: data_age_hours is the quote's age when it was
-- archived (the provider's lag), data_age_now_hours its age now, and
-- snapshot_age_days the age of the archived copy (more than 1 = a nightly
-- archive of that page was missed). Source of the World Top-25 ROI table,
-- the evidence views and the symbol picker.
CREATE OR REPLACE VIEW `{{DS}}.v_universe_latest`
OPTIONS (description = "TFB Looker v2: the latest market-page row of every symbol (percent points), with the Top-10 panel floors flag")
AS
WITH recent AS (
  SELECT u.*
  FROM `{{DS}}.v_universe` AS u
  WHERE u.snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 10 DAY)
  QUALIFY u.snapshot_date = MAX(u.snapshot_date) OVER (PARTITION BY u.source_page)
)
SELECT
  r.*,
  COALESCE(r.reliability >= 70 AND r.data_quality >= 80, FALSE) AND r.block_reason IS NULL AS meets_floors,
  COUNT(*) OVER (PARTITION BY r.symbol) AS pages_listing,
  DATE_DIFF(CURRENT_DATE(), r.snapshot_date, DAY) AS snapshot_age_days,
  ROUND(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), r.last_updated_at, MINUTE) / 60, 1) AS data_age_now_hours
FROM recent AS r
QUALIFY ROW_NUMBER() OVER (PARTITION BY r.symbol ORDER BY r.captured_at DESC, r.source_page) = 1;
