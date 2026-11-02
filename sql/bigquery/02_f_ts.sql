-- f_ts(s): a workbook time stamp as a TIMESTAMP. Text without an offset is
-- Riyadh local time (the workbook's clock); both the Python writers'
-- "2026-10-28 20:28:36" and Apps Script's "8/24/2026 20:15:11" are read, as
-- are date-only values and ISO strings that carry their own offset.
CREATE OR REPLACE FUNCTION `{{DS}}.f_ts`(s STRING)
RETURNS TIMESTAMP
AS (
  COALESCE(
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', TRIM(s), 'Asia/Riyadh'),
    SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', TRIM(s), 'Asia/Riyadh'),
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M', TRIM(s), 'Asia/Riyadh'),
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d', TRIM(s), 'Asia/Riyadh'),
    SAFE.PARSE_TIMESTAMP('%m/%d/%Y', TRIM(s), 'Asia/Riyadh'),
    SAFE_CAST(TRIM(s) AS TIMESTAMP)
  )
)
OPTIONS (description = "TFB Looker v1: workbook time stamp (Riyadh local unless the text carries an offset)");
