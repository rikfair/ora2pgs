
CREATE FUNCTION %%schema%%.mod_trigger_fnc()
RETURNS TRIGGER
LANGUAGE PLPGSQL
AS
$$
DECLARE
  l_username     VARCHAR(30);
BEGIN

  l_username := CURRENT_USER;
  IF l_username = '%%schema%%_user' THEN
    l_username := CURRENT_SETTING('%%schema%%.user');
  END IF;

  NEW.modified_by   := l_username;
  NEW.modified_date := CURRENT_TIMESTAMP;

  IF TG_OP = 'INSERT' THEN
    NEW.created_by   := NEW.modified_by;
    NEW.created_date := NEW.modified_date;
  END IF;

  RETURN NEW;
END;
$$
;
