ALTER TABLE broker ADD COLUMN description TEXT;

CREATE OR REPLACE FUNCTION set_empty_broker_strings_to_null() RETURNS TRIGGER AS
    $BODY$
BEGIN
    NEW.name := NULLIF(TRIM(regexp_replace(NEW.name, '\s+', ' ', 'g')), '');
    NEW.description := NULLIF(TRIM(NEW.description), '');
    NEW.bucket := NULLIF(NEW.bucket, '');
    NEW.endpoint := NULLIF(NEW.endpoint, '');
    NEW.access_key := NULLIF(NEW.access_key, '');
    NEW.secret_key := NULLIF(NEW.secret_key, '');
RETURN NEW;
END;
$BODY$
language plpgsql;

CREATE TYPE broker_audit_action AS ENUM (
    'edit',
    'bucket_connection_edit',
    'access_granted',
    'access_revoked',
    'access_quota_edit',
    'access_admin_promote',
    'access_admin_demote'
);

CREATE TABLE broker_audit_log(
    pk BIGSERIAL PRIMARY KEY,
    fk_broker BIGINT REFERENCES broker(pk) ON DELETE CASCADE NOT NULL,
    fk_user BIGINT REFERENCES registered_user(pk) NOT NULL,
    action broker_audit_action NOT NULL,
    fk_target_group BIGINT REFERENCES user_group(pk) ON DELETE CASCADE,
    new_quota BIGINT,
    creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
