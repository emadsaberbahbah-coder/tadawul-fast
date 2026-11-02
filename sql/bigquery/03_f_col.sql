-- f_col: the position (0-based) of a header in a header row - the first
-- column whose trimmed name equals `name`, NULL when the row has none. A
-- view over a wide tab looks each header up once per snapshot with f_col
-- and reads every row by position with f_at, instead of searching the
-- header row again for every cell as f_cell does:
-- f_at(c, f_col(h, name)) = f_cell(h, c, name).
CREATE OR REPLACE FUNCTION `{{DS}}.f_col`(h ARRAY<STRING>, name STRING)
RETURNS INT64
AS ((
  SELECT o
  FROM UNNEST(h) AS x WITH OFFSET AS o
  WHERE TRIM(x) = name
  ORDER BY o
  LIMIT 1
))
OPTIONS (description = "TFB Looker v2: position (0-based) of header `name` in header row h, NULL when absent - pairs with f_at");
