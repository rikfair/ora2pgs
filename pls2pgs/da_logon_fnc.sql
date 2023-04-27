
CREATE FUNCTION %%schema%%.da_logon_fnc(p_username IN VARCHAR)
RETURNS VARCHAR
LANGUAGE plpgsql
AS
$$
DECLARE
  l_cmd            VARCHAR;
  l_return         VARCHAR;
BEGIN

  SET SESSION SCHEMA '%%schema%%';

  l_cmd = 'SET SESSION "%%schema%%.user" = "' || UPPER(p_username) || '"';
  EXECUTE l_cmd;
  SELECT CURRENT_SETTING('%%schema%%.user') || ':' || PG_BACKEND_PID() INTO l_return;

  RETURN(l_return);

END;
$$
;
