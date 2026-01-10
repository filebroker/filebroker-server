ALTER TABLE broker ADD COLUMN total_quota BIGINT;
ALTER TABLE broker ADD COLUMN disable_uploads BOOLEAN NOT NULL DEFAULT FALSE;

-- since postgres does not allow removing values from enum types, we cannot undo this in down.sql
ALTER TYPE broker_audit_action ADD VALUE IF NOT EXISTS 'disable_uploads';
ALTER TYPE broker_audit_action ADD VALUE IF NOT EXISTS 'enable_uploads';

ALTER TABLE broker_access ADD COLUMN fk_granted_user BIGINT REFERENCES registered_user(pk);
ALTER TABLE broker_access ADD COLUMN public BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE broker_audit_log ADD COLUMN fk_target_user BIGINT REFERENCES registered_user(pk);

UPDATE broker_access SET public = TRUE WHERE fk_granted_group IS NULL AND fk_granted_user IS NULL;

ALTER TABLE broker_access ADD CONSTRAINT broker_access_target_check
    CHECK (
        (public AND fk_granted_user IS NULL AND fk_granted_group IS NULL) OR
        (NOT public AND fk_granted_user IS NOT NULL AND fk_granted_group IS NULL) OR
        (NOT public AND fk_granted_user IS NULL AND fk_granted_group IS NOT NULL)
    );

ALTER TABLE broker_access ADD CONSTRAINT broker_access_no_unlimited_public_access
    CHECK (
        quota IS NOT NULL OR public IS NOT TRUE
    );

CREATE FUNCTION set_broker_access_public_flag()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.fk_granted_user IS NULL AND NEW.fk_granted_group IS NULL THEN
        NEW.public := TRUE;
    ELSE
        NEW.public := FALSE;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_sync_broker_access_public_flag BEFORE INSERT OR UPDATE ON broker_access
FOR EACH ROW
EXECUTE FUNCTION set_broker_access_public_flag();

DROP INDEX broker_access_fk_granted_group_fk_broker_unique_idx;

CREATE UNIQUE INDEX broker_access_fk_granted_group_unique_idx
    ON broker_access(fk_granted_group, fk_broker)
    WHERE fk_granted_group IS NOT NULL;

CREATE UNIQUE INDEX broker_access_fk_granted_user_unique_idx
    ON broker_access(fk_granted_user, fk_broker)
    WHERE fk_granted_user IS NOT NULL;

CREATE UNIQUE INDEX broker_access_public_unique
    ON broker_access(fk_broker)
    WHERE public;
