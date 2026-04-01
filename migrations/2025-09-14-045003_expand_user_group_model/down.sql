DROP INDEX user_group_name_gin_idx;
DROP INDEX user_group_description_gin_idx;
DROP INDEX user_group_description_idx;
DROP INDEX user_group_name_idx;
DROP INDEX user_group_name_desc_idx;
DROP INDEX user_group_name_creation_timestamp_idx;
DROP INDEX user_group_name_creation_timestamp_desc_idx;
DROP INDEX user_group_name_desc_creation_timestamp_idx;
DROP INDEX user_group_name_desc_creation_timestamp_desc_idx;
DROP INDEX user_group_creation_timestamp_idx;
DROP INDEX user_group_creation_timestamp_desc_idx;
DROP INDEX user_group_creation_timestamp_name_idx;
DROP INDEX user_group_creation_timestamp_name_desc_idx;
DROP INDEX user_group_creation_timestamp_desc_name_idx;
DROP INDEX user_group_creation_timestamp_desc_name_desc_idx;

DROP INDEX user_group_tag_fk_tag_idx;
DROP TABLE user_group_tag;

ALTER TABLE user_group DROP COLUMN description;
ALTER TABLE user_group DROP COLUMN allow_member_invite;
ALTER TABLE user_group DROP COLUMN avatar_object_key;
ALTER TABLE user_group ADD COLUMN hidden BOOLEAN NOT NULL DEFAULT FALSE;

DROP TABLE user_group_edit_history_tag;
DROP INDEX user_group_edit_history_fk_user_group_idx;
DROP TABLE user_group_edit_history;

DROP INDEX user_group_invite_fk_user_group_idx;
DROP INDEX user_group_invite_fk_create_user_idx;
DROP INDEX user_group_invite_fk_invited_user_idx;

DROP INDEX user_group_audit_log_fk_target_user_idx;
DROP INDEX user_group_audit_log_fk_user_idx;
DROP INDEX user_group_audit_log_fk_user_group_idx;
DROP TABLE user_group_audit_log;
DROP TYPE user_group_audit_action;

DROP TABLE user_group_invite;

DROP TRIGGER before_insert_user_group_set_initial_change_values ON user_group;
ALTER TABLE user_group DROP COLUMN fk_create_user;
ALTER TABLE user_group DROP COLUMN edit_timestamp;
ALTER TABLE user_group DROP COLUMN fk_edit_user;

DROP TRIGGER before_insert_or_update_set_empty_user_group_strings_to_null ON user_group;
DROP FUNCTION set_empty_user_group_strings_to_null();
