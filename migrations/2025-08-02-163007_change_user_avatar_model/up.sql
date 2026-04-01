ALTER TABLE registered_user DROP COLUMN avatar_url;
ALTER TABLE registered_user ADD COLUMN avatar_object_key VARCHAR(255) REFERENCES s3_object(object_key);

CREATE OR REPLACE FUNCTION set_empty_user_strings_to_null() RETURNS TRIGGER AS
$BODY$
BEGIN
    NEW.display_name := NULLIF(TRIM(regexp_replace(NEW.display_name, '\s+', ' ', 'g')), '');
    NEW.user_name := NULLIF(TRIM(NEW.user_name), '');
    NEW.email := NULLIF(TRIM(NEW.email), '');
    NEW.avatar_object_key := NULLIF(TRIM(NEW.avatar_object_key), '');
    RETURN NEW;
END;
$BODY$
    language plpgsql;

ALTER TABLE broker ADD COLUMN is_system_bucket BOOLEAN NOT NULL DEFAULT FALSE;
CREATE UNIQUE INDEX broker_is_system_bucket_unique_idx ON broker(is_system_bucket) WHERE is_system_bucket IS TRUE;
