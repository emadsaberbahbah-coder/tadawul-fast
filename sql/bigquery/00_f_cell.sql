-- f_cell(h, c, name): the trimmed value of row `c` in the column whose header
-- (row `h`) is `name`; NULL when the column is missing, the cell is blank, or
-- the cell holds the workbook's "not applicable" dash.
CREATE OR REPLACE FUNCTION `{{DS}}.f_cell`(h ARRAY<STRING>, c ARRAY<STRING>, name STRING)
RETURNS STRING
AS ((
  SELECT NULLIF(NULLIF(TRIM(c[SAFE_OFFSET(o)]), ''), '—')
  FROM UNNEST(h) AS x WITH OFFSET AS o
  WHERE TRIM(x) = name
  ORDER BY o
  LIMIT 1
))
OPTIONS (description = "TFB Looker v1: cell of row c under header `name` of header row h (NULL when blank, missing or a dash)");
