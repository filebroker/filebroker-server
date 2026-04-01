DROP INDEX broker_access_public_unique;
DROP INDEX broker_access_fk_granted_user_unique_idx;
DROP INDEX broker_access_fk_granted_group_unique_idx;

CREATE UNIQUE INDEX broker_access_fk_granted_group_fk_broker_unique_idx ON broker_access(fk_granted_group, fk_broker) NULLS NOT DISTINCT;

DROP TRIGGER trigger_sync_broker_access_public_flag ON broker_access;
DROP FUNCTION set_broker_access_public_flag();

ALTER TABLE broker_access DROP CONSTRAINT broker_access_no_unlimited_public_access;
ALTER TABLE broker_access DROP CONSTRAINT broker_access_target_check;

ALTER TABLE broker_access DROP COLUMN fk_granted_user;
ALTER TABLE broker_access DROP COLUMN public;
ALTER TABLE broker_audit_log DROP COLUMN fk_target_user;

ALTER TABLE broker DROP COLUMN disable_uploads;
ALTER TABLE broker DROP COLUMN total_quota;
