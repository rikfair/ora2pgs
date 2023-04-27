CREATE OR REPLACE PROCEDURE %%schema%%.raise_application_error(en NUMERIC, et VARCHAR)
LANGUAGE plpgsql
AS
$$
DECLARE
  msg  VARCHAR := CAST(en AS VARCHAR) || ': ' || et;
BEGIN
  RAISE EXCEPTION '%', msg;
END;
$$
;
