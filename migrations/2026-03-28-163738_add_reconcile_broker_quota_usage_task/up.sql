CREATE TABLE reconcile_broker_quota_usage_task(
    pk BIGSERIAL PRIMARY KEY,
    fk_user BIGINT REFERENCES registered_user(pk) NOT NULL,
    fk_broker BIGINT REFERENCES broker(pk) NOT NULL,
    creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    locked_at TIMESTAMP WITH TIME ZONE,
    fail_count INTEGER NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX reconcile_broker_quota_usage_task_unique_idx
    ON reconcile_broker_quota_usage_task (fk_user, fk_broker)
    WHERE locked_at IS NULL AND fail_count < 3;



CREATE OR REPLACE FUNCTION create_reconcile_broker_quota_usage_task_on_membership_change()
    RETURNS TRIGGER AS
$$
BEGIN
    IF (TG_OP = 'INSERT' AND NEW.revoked = false)
        OR (TG_OP = 'DELETE' AND OLD.revoked = false)
        OR (TG_OP = 'UPDATE' AND OLD.revoked IS DISTINCT FROM NEW.revoked)
    THEN
        INSERT INTO reconcile_broker_quota_usage_task (fk_user, fk_broker)
        SELECT COALESCE(NEW.fk_user, OLD.fk_user),
               ba.fk_broker
        FROM broker_access ba
        WHERE ba.fk_granted_group = COALESCE(NEW.fk_group, OLD.fk_group)
        ON CONFLICT DO NOTHING;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER on_user_group_membership_change_reconcile_broker_quota_usage
    AFTER INSERT OR UPDATE OR DELETE
    ON user_group_membership
    FOR EACH ROW
EXECUTE FUNCTION create_reconcile_broker_quota_usage_task_on_membership_change();


CREATE OR REPLACE FUNCTION create_reconcile_broker_quota_usage_task_on_access_change()
    RETURNS TRIGGER AS
$$
DECLARE
    broker_pk BIGINT;
BEGIN
    broker_pk := COALESCE(NEW.fk_broker, OLD.fk_broker);

    IF (TG_OP = 'INSERT')
        OR (TG_OP = 'DELETE')
        OR (
           TG_OP = 'UPDATE' AND (OLD.quota IS DISTINCT FROM NEW.quota)
           )
    THEN
        -- Direct user access
        IF COALESCE(NEW.fk_granted_user, OLD.fk_granted_user) IS NOT NULL THEN
            INSERT INTO reconcile_broker_quota_usage_task (fk_user, fk_broker)
            VALUES (COALESCE(NEW.fk_granted_user, OLD.fk_granted_user),
                    broker_pk)
            ON CONFLICT DO NOTHING;
        END IF;

        -- Group-based access
        IF COALESCE(NEW.fk_granted_group, OLD.fk_granted_group) IS NOT NULL THEN
            INSERT INTO reconcile_broker_quota_usage_task (fk_user, fk_broker)
            SELECT DISTINCT ugm.fk_user,
                            broker_pk
            FROM user_group_membership ugm
                JOIN s3_object o ON o.fk_uploader = ugm.fk_user AND o.fk_broker = broker_pk
            WHERE ugm.fk_group = COALESCE(NEW.fk_granted_group, OLD.fk_granted_group)
              AND ugm.revoked = false
            ON CONFLICT DO NOTHING;
        END IF;

        -- Public access → only users who actually used the broker
        IF COALESCE(NEW.public, OLD.public) = true THEN
            INSERT INTO reconcile_broker_quota_usage_task (fk_user, fk_broker)
            SELECT DISTINCT o.fk_uploader,
                            broker_pk
            FROM s3_object o
            WHERE o.fk_broker = broker_pk
            ON CONFLICT DO NOTHING;
        END IF;

    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER on_broker_access_change_reconcile_broker_quota_usage
    AFTER INSERT OR UPDATE OR DELETE
    ON broker_access
    FOR EACH ROW
EXECUTE FUNCTION create_reconcile_broker_quota_usage_task_on_access_change();



CREATE INDEX s3_object_fk_broker_fk_uploader_idx ON s3_object(fk_broker, fk_uploader);

ALTER TABLE broker ADD COLUMN quota_audit_locked_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE broker ADD COLUMN last_quota_audit TIMESTAMP WITH TIME ZONE;
