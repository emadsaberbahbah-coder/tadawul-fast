-- f_ts_utc: a workbook time stamp written in UTC - the market pages' Last
-- Updated (UTC) and the news gate's Published (UTC) / Date (UTC) - read as
-- UTC (f_ts reads Riyadh local time). A text that carries its own offset
-- keeps it.
CREATE OR REPLACE FUNCTION `{{DS}}.f_ts_utc`(s STRING)
RETURNS TIMESTAMP
AS (
  COALESCE(
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', TRIM(s), 'UTC'),
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M', TRIM(s), 'UTC'),
    SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', TRIM(s), 'UTC'),
    SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M', TRIM(s), 'UTC'),
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d', TRIM(s), 'UTC'),
    SAFE_CAST(TRIM(s) AS TIMESTAMP)
  )
)
OPTIONS (description = "TFB Looker v2: workbook time stamp written in UTC (an explicit offset in the text wins)");
