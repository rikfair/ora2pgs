CREATE FUNCTION %%schema%%.instr(
    a TEXT,
    b TEXT
) RETURNS INTEGER AS $$
BEGIN
    RETURN POSITION(b IN a);
END; $$
LANGUAGE plpgsql
;

