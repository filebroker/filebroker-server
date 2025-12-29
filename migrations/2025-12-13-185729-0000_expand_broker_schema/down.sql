CREATE OR REPLACE FUNCTION set_empty_broker_strings_to_null() RETURNS TRIGGER AS
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

ALTER TABLE broker DROP COLUMN description;

DROP TABLE broker_audit_log;
DROP TYPE broker_audit_action;
