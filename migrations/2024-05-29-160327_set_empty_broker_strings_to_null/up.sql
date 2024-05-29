CREATE FUNCTION set_empty_broker_strings_to_null() RETURNS TRIGGER AS
$BODY$
BEGIN
    NEW.name := NULLIF(TRIM(regexp_replace(NEW.name, '\s+', ' ', 'g')), '');
    NEW.bucket := NULLIF(NEW.bucket, '');
    NEW.endpoint := NULLIF(NEW.endpoint, '');
    NEW.access_key := NULLIF(NEW.access_key, '');
    NEW.secret_key := NULLIF(NEW.secret_key, '');
    RETURN NEW;
END;
$BODY$
language plpgsql;

CREATE TRIGGER before_insert_or_update_set_empty_broker_strings_to_null BEFORE INSERT OR UPDATE ON broker
FOR EACH ROW
EXECUTE PROCEDURE set_empty_broker_strings_to_null();
