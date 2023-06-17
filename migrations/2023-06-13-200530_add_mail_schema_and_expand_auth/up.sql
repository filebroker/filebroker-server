ALTER TABLE registered_user ADD COLUMN email_confirmed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE registered_user ADD COLUMN display_name VARCHAR(32);
ALTER TABLE registered_user ALTER COLUMN user_name TYPE varchar(25);

CREATE FUNCTION set_empty_user_strings_to_null() RETURNS TRIGGER AS
$BODY$
BEGIN
    NEW.display_name := NULLIF(TRIM(regexp_replace(NEW.display_name, '\s+', ' ', 'g')), '');
    NEW.user_name := NULLIF(TRIM(NEW.user_name), '');
    NEW.email := NULLIF(TRIM(NEW.email), '');
    NEW.avatar_url := NULLIF(TRIM(NEW.avatar_url), '');
    RETURN NEW;
END;
$BODY$
language plpgsql;

CREATE TRIGGER before_insert_or_update_set_empty_user_strings_to_null BEFORE INSERT OR UPDATE ON registered_user
FOR EACH ROW
EXECUTE PROCEDURE set_empty_user_strings_to_null();

ALTER TABLE refresh_token DROP COLUMN pk;
ALTER TABLE refresh_token RENAME COLUMN fk_registered_user TO fk_user;
ALTER TABLE refresh_token ADD PRIMARY KEY (uuid);

CREATE TABLE email_confirmation_token(
    uuid UUID UNIQUE NOT NULL PRIMARY KEY,
    expiry TIMESTAMP WITH TIME ZONE NOT NULL,
    invalidated BOOLEAN NOT NULL,
    fk_user INTEGER REFERENCES registered_user(pk) NOT NULL
);

CREATE FUNCTION unverify_email_on_change() RETURNS TRIGGER AS
$BODY$
BEGIN
    IF OLD.email != NEW.email THEN
        NEW.email_confirmed = FALSE;
        UPDATE email_confirmation_token SET invalidated = TRUE WHERE fk_user = NEW.pk;
    END IF;
    RETURN NEW;
END;
$BODY$
language plpgsql;

CREATE TRIGGER before_update_unverify_email_on_change BEFORE UPDATE ON registered_user
FOR EACH ROW
EXECUTE PROCEDURE unverify_email_on_change();

ALTER TABLE registered_user ADD COLUMN jwt_version INTEGER NOT NULL DEFAULT 0;

CREATE FUNCTION increment_jwt_version_on_password_change() RETURNS TRIGGER AS
$BODY$
BEGIN
    IF OLD.password != NEW.password THEN
        NEW.jwt_version = COALESCE(NEW.jwt_version, 0) + 1;
    END IF;
    RETURN NEW;
END;
$BODY$
language plpgsql;

CREATE TRIGGER before_update_increment_jwt_version_on_password_change BEFORE UPDATE ON registered_user
FOR EACH ROW
EXECUTE PROCEDURE increment_jwt_version_on_password_change();

ALTER TABLE registered_user ADD COLUMN password_fail_count INTEGER NOT NULL DEFAULT 0;

CREATE TABLE one_time_password(
    password VARCHAR(255) NOT NULL,
    expiry TIMESTAMP WITH TIME ZONE NOT NULL,
    invalidated BOOLEAN NOT NULL,
    fk_user INTEGER REFERENCES registered_user(pk) NOT NULL PRIMARY KEY
);
