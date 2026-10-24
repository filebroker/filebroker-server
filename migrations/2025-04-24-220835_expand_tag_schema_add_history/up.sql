ALTER TABLE registered_user ADD COLUMN is_admin BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE registered_user ADD COLUMN is_banned BOOLEAN NOT NULL DEFAULT false;
INSERT INTO registered_user(pk, user_name, password, is_admin) VALUES(0, 'system', '', true) ON CONFLICT DO NOTHING;

ALTER TABLE tag ADD COLUMN fk_create_user BIGINT REFERENCES registered_user(pk);
UPDATE tag SET fk_create_user = 1;

ALTER TABLE tag ADD COLUMN edit_timestamp TIMESTAMP WITH TIME ZONE;
ALTER TABLE tag ADD COLUMN fk_edit_user BIGINT REFERENCES registered_user(pk);
UPDATE tag SET edit_timestamp = creation_timestamp, fk_edit_user = fk_create_user;
ALTER TABLE tag ALTER COLUMN fk_create_user SET NOT NULL, ALTER COLUMN edit_timestamp SET NOT NULL, ALTER COLUMN fk_edit_user SET NOT NULL;

CREATE TABLE tag_category(
    id VARCHAR(255) UNIQUE NOT NULL PRIMARY KEY,
    label VARCHAR(255) NOT NULL,
    auto_match_condition_post VARCHAR(1023),
    auto_match_condition_collection VARCHAR(1023)
);

ALTER TABLE tag ADD COLUMN tag_category VARCHAR(255) REFERENCES tag_category(id);

ALTER TABLE tag ADD COLUMN auto_match_condition_post VARCHAR(1023);
ALTER TABLE tag ADD COLUMN auto_match_condition_collection VARCHAR(1023);
ALTER TABLE tag ADD COLUMN compiled_auto_match_condition_post TEXT;
ALTER TABLE tag ADD COLUMN compiled_auto_match_condition_collection TEXT;

ALTER TABLE post_tag ADD COLUMN auto_matched BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE post_collection_tag ADD COLUMN auto_matched BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE post_edit_history_tag ADD COLUMN auto_matched BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE post_collection_edit_history_tag ADD COLUMN auto_matched BOOLEAN NOT NULL DEFAULT false;

CREATE TABLE tag_edit_history(
    pk BIGSERIAL PRIMARY KEY,
    fk_tag BIGINT REFERENCES tag(pk) ON DELETE CASCADE NOT NULL,
    fk_edit_user BIGINT REFERENCES registered_user(pk) NOT NULL,
    edit_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    tag_category VARCHAR(255) REFERENCES tag_category(id),
    tag_category_changed BOOLEAN NOT NULL DEFAULT FALSE,
    parents_changed BOOLEAN NOT NULL DEFAULT FALSE,
    aliases_changed BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE tag_edit_history_parent(
    fk_tag_edit_history BIGINT REFERENCES tag_edit_history(pk) ON DELETE CASCADE NOT NULL,
    fk_parent BIGINT REFERENCES tag(pk) ON DELETE CASCADE NOT NULL,
    PRIMARY KEY (fk_tag_edit_history, fk_parent)
);

CREATE TABLE tag_edit_history_alias(
    fk_tag_edit_history BIGINT REFERENCES tag_edit_history(pk) ON DELETE CASCADE NOT NULL,
    fk_alias BIGINT REFERENCES tag(pk) ON DELETE CASCADE NOT NULL,
    PRIMARY KEY (fk_tag_edit_history, fk_alias)
);

CREATE TRIGGER before_insert_tag_set_initial_change_values
    BEFORE INSERT ON tag
    FOR EACH ROW
    EXECUTE PROCEDURE set_initial_change_values();

CREATE INDEX tag_edit_history_fk_tag ON tag_edit_history(fk_tag);

CREATE FUNCTION set_empty_tag_strings_to_null() RETURNS TRIGGER AS
$BODY$
BEGIN
    NEW.tag_name := NULLIF(TRIM(regexp_replace(NEW.tag_name, '\s+', ' ', 'g')), '');
    NEW.tag_category := NULLIF(NEW.tag_category, '');
    NEW.auto_match_condition_post := NULLIF(TRIM(NEW.auto_match_condition_post), '');
    NEW.auto_match_condition_collection := NULLIF(TRIM(NEW.auto_match_condition_collection), '');
    NEW.compiled_auto_match_condition_post := NULLIF(TRIM(NEW.compiled_auto_match_condition_post), '');
    NEW.compiled_auto_match_condition_collection := NULLIF(TRIM(NEW.compiled_auto_match_condition_collection), '');
    RETURN NEW;
END;
$BODY$
language plpgsql;

CREATE TRIGGER before_insert_or_update_set_empty_tag_strings_to_null
    BEFORE INSERT OR UPDATE ON tag
    FOR EACH ROW
    EXECUTE PROCEDURE set_empty_tag_strings_to_null();

CREATE FUNCTION set_empty_tag_category_strings_to_null() RETURNS TRIGGER AS
$BODY$
BEGIN
    NEW.id := NULLIF(TRIM(regexp_replace(NEW.id, '\s+', ' ', 'g')), '');
    NEW.label := NULLIF(TRIM(regexp_replace(NEW.label, '\s+', ' ', 'g')), '');
    NEW.auto_match_condition_post := NULLIF(TRIM(NEW.auto_match_condition_post), '');
    NEW.auto_match_condition_collection := NULLIF(TRIM(NEW.auto_match_condition_collection), '');
    RETURN NEW;
END;
$BODY$
    language plpgsql;

CREATE TRIGGER before_insert_or_update_set_empty_tag_category_strings_to_null
    BEFORE INSERT OR UPDATE ON tag_category
    FOR EACH ROW
    EXECUTE PROCEDURE set_empty_tag_category_strings_to_null();

CREATE FUNCTION evaluate_tag_auto_match_condition(
    compiled_query text,
    filter_condition text DEFAULT NULL
) RETURNS boolean AS $$
    DECLARE
        complete_query text;
        result boolean;
    BEGIN
        IF compiled_query IS NULL OR compiled_query = '' THEN
            RETURN FALSE;
        END IF;

        complete_query := format(
            'SELECT EXISTS(%s)',
            replace(compiled_query, '__filter_condition_placeholder__', COALESCE(NULLIF(filter_condition, ''), 'TRUE'))
        );

        EXECUTE complete_query INTO result;
        RETURN result;
    END;
$$ LANGUAGE plpgsql;

CREATE TABLE apply_auto_tags_task(
    pk BIGSERIAL PRIMARY KEY,
    creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    tag_to_apply BIGINT REFERENCES tag(pk) ON DELETE CASCADE,
    tag_category_to_apply VARCHAR(255) REFERENCES tag_category(id) ON DELETE CASCADE,
    post_to_apply BIGINT REFERENCES post(pk) ON DELETE CASCADE,
    post_collection_to_apply BIGINT REFERENCES post_collection(pk) ON DELETE CASCADE
);
