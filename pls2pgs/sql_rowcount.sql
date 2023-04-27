CREATE OR REPLACE FUNCTION %%schema%%.sql_rowcount()
RETURNS DATE
LANGUAGE plpgsql
AS
$$
DECLARE
  num_rows     INTEGER;
BEGIN
  GET DIAGNOSTICS num_rows = ROW_COUNT;
  RETURN num_rows;
END;
$$
;
