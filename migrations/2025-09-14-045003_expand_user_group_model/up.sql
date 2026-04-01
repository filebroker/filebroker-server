CREATE TABLE user_group_tag(
    fk_user_group BIGINT REFERENCES user_group(pk) ON DELETE CASCADE NOT NULL,
    fk_tag BIGINT REFERENCES tag(pk) ON DELETE CASCADE NOT NULL,
    auto_matched BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (fk_user_group, fk_tag)
);

CREATE INDEX user_group_tag_fk_tag_idx ON user_group_tag(fk_tag);

ALTER TABLE user_group ADD COLUMN description TEXT;
ALTER TABLE user_group ADD COLUMN allow_member_invite BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE user_group ADD COLUMN avatar_object_key VARCHAR(255) REFERENCES s3_object(object_key);
ALTER TABLE user_group DROP COLUMN hidden;

CREATE TABLE user_group_edit_history(
    pk BIGSERIAL PRIMARY KEY,
    fk_user_group BIGINT REFERENCES user_group(pk) ON DELETE CASCADE NOT NULL,
    fk_edit_user BIGINT REFERENCES registered_user(pk) NOT NULL,
    edit_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    name VARCHAR(255) NOT NULL,
    name_changed BOOLEAN NOT NULL DEFAULT false,
    public BOOLEAN NOT NULL DEFAULT false,
    public_changed BOOLEAN NOT NULL DEFAULT false,
    description TEXT,
    description_changed BOOLEAN NOT NULL DEFAULT false,
    allow_member_invite BOOLEAN NOT NULL DEFAULT false,
    allow_member_invite_changed BOOLEAN NOT NULL DEFAULT false,
    tags_changed BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX user_group_edit_history_fk_user_group_idx ON user_group_edit_history(fk_user_group);

CREATE TABLE user_group_edit_history_tag(
    fk_user_group_edit_history BIGINT REFERENCES user_group_edit_history(pk) ON DELETE CASCADE NOT NULL,
    fk_tag BIGINT REFERENCES tag(pk) ON DELETE CASCADE NOT NULL,
    auto_matched BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (fk_user_group_edit_history, fk_tag)
);

CREATE TABLE user_group_invite(
    code VARCHAR(10) PRIMARY KEY,
    fk_user_group BIGINT REFERENCES user_group(pk) ON DELETE CASCADE NOT NULL,
    fk_create_user BIGINT REFERENCES registered_user(pk) NOT NULL,
    fk_invited_user BIGINT REFERENCES registered_user(pk),
    creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expiration_timestamp TIMESTAMP WITH TIME ZONE,
    last_used_timestamp TIMESTAMP WITH TIME ZONE,
    max_uses INT,
    uses_count INT NOT NULL DEFAULT 0,
    revoked BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX user_group_invite_fk_user_group_idx ON user_group_invite(fk_user_group, creation_timestamp);
CREATE INDEX user_group_invite_fk_create_user_idx ON user_group_invite(fk_create_user, creation_timestamp);
CREATE INDEX user_group_invite_fk_invited_user_idx ON user_group_invite(fk_invited_user, creation_timestamp);

CREATE TYPE user_group_audit_action AS ENUM (
    'edit',
    'join',
    'invite',
    'revoke_invite',
    'leave',
    'kick',
    'ban',
    'unban',
    'admin_promote',
    'admin_demote',
    'avatar_change'
);

CREATE TABLE user_group_audit_log(
    pk BIGSERIAL PRIMARY KEY,
    fk_user_group BIGINT REFERENCES user_group(pk) ON DELETE CASCADE NOT NULL,
    fk_user BIGINT REFERENCES registered_user(pk) NOT NULL,
    action user_group_audit_action NOT NULL,
    fk_target_user BIGINT REFERENCES registered_user(pk),
    invite_code VARCHAR(10),
    reason TEXT,
    creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX user_group_audit_log_fk_user_group_idx ON user_group_audit_log(fk_user_group);
CREATE INDEX user_group_audit_log_fk_user_idx ON user_group_audit_log(fk_user);
CREATE INDEX user_group_audit_log_fk_target_user_idx ON user_group_audit_log(fk_target_user);

ALTER TABLE user_group ADD COLUMN fk_create_user BIGINT REFERENCES registered_user(pk);
UPDATE user_group SET fk_create_user = fk_owner;
ALTER TABLE user_group ADD COLUMN edit_timestamp TIMESTAMP WITH TIME ZONE;
ALTER TABLE user_group ADD COLUMN fk_edit_user BIGINT REFERENCES registered_user(pk);
UPDATE user_group SET edit_timestamp = creation_timestamp, fk_edit_user = fk_create_user;
ALTER TABLE user_group ALTER COLUMN fk_create_user SET NOT NULL, ALTER COLUMN edit_timestamp SET NOT NULL, ALTER COLUMN fk_edit_user SET NOT NULL;

CREATE TRIGGER before_insert_user_group_set_initial_change_values
    BEFORE INSERT ON user_group
    FOR EACH ROW
EXECUTE PROCEDURE set_initial_change_values();

CREATE FUNCTION set_empty_user_group_strings_to_null() RETURNS TRIGGER AS
$BODY$
BEGIN
    NEW.name := NULLIF(TRIM(regexp_replace(NEW.name, '\s+', ' ', 'g')), '');
    NEW.description := NULLIF(TRIM(NEW.description), '');
    RETURN NEW;
END;
$BODY$
    language plpgsql;

CREATE TRIGGER before_insert_or_update_set_empty_user_group_strings_to_null
    BEFORE INSERT OR UPDATE ON user_group
    FOR EACH ROW
    EXECUTE PROCEDURE set_empty_user_group_strings_to_null();

CREATE INDEX user_group_name_gin_idx ON user_group USING gin(LOWER(name) gin_trgm_ops);
CREATE INDEX user_group_description_gin_idx ON user_group USING gin(LOWER(description) gin_trgm_ops);

CREATE INDEX user_group_description_idx ON user_group(LOWER(description)) WHERE LENGTH(description) < 2048;

CREATE INDEX user_group_name_idx ON user_group(LOWER(name), pk DESC);
CREATE INDEX user_group_name_desc_idx ON user_group(LOWER(name) DESC, pk DESC);

CREATE INDEX user_group_name_creation_timestamp_idx ON user_group(LOWER(name), creation_timestamp, pk DESC);
CREATE INDEX user_group_name_creation_timestamp_desc_idx ON user_group(LOWER(name), creation_timestamp DESC, pk DESC);
CREATE INDEX user_group_name_desc_creation_timestamp_idx ON user_group(LOWER(name) DESC, creation_timestamp, pk DESC);
CREATE INDEX user_group_name_desc_creation_timestamp_desc_idx ON user_group(LOWER(name) DESC, creation_timestamp DESC, pk DESC);

CREATE INDEX user_group_creation_timestamp_idx ON user_group(creation_timestamp, pk DESC);
CREATE INDEX user_group_creation_timestamp_desc_idx ON user_group(creation_timestamp DESC, pk DESC);

CREATE INDEX user_group_creation_timestamp_name_idx ON user_group(creation_timestamp, LOWER(name), pk DESC);
CREATE INDEX user_group_creation_timestamp_name_desc_idx ON user_group(creation_timestamp, LOWER(name) DESC, pk DESC);
CREATE INDEX user_group_creation_timestamp_desc_name_idx ON user_group(creation_timestamp DESC, LOWER(name), pk DESC);
CREATE INDEX user_group_creation_timestamp_desc_name_desc_idx ON user_group(creation_timestamp DESC, LOWER(name) DESC, pk DESC);
