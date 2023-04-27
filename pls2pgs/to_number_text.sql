
CREATE OR REPLACE FUNCTION %%schema%%.to_number(a TEXT)
RETURNS NUMERIC
LANGUAGE plpgsql
AS
$$
BEGIN
  -- Be careful! This will return a decimal place for an integer. '123' returns 123.0
  -- Consider changing the SQL to CAST(s AS INT) instead, which works with both Oracle and Postgresql
  RETURN CAST(a AS NUMERIC);
END;
$$
;
