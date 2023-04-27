
CREATE FUNCTION %%schema%%.to_date(d INTEGER, f VARCHAR)
RETURNS TIMESTAMP
LANGUAGE plpgsql
AS
$$
DECLARE
  textdate TEXT := d;
BEGIN
  RETURN TO_DATE(textdate, f);
END;
$$
;
