CREATE TABLE post_edit_history(
    pk BIGSERIAL PRIMARY KEY,
    fk_post BIGINT REFERENCES post(pk) NOT NULL,
    fk_edit_user BIGINT REFERENCES registered_user(pk) NOT NULL,
    edit_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    data_url VARCHAR(2048),
    data_url_changed BOOLEAN NOT NULL DEFAULT FALSE,
    source_url VARCHAR(2048),
    source_url_changed BOOLEAN NOT NULL DEFAULT FALSE,
    title VARCHAR(300),
    title_changed BOOLEAN NOT NULL DEFAULT FALSE,
    public BOOLEAN NOT NULL DEFAULT FALSE,
    public_changed BOOLEAN NOT NULL DEFAULT FALSE,
    public_edit BOOLEAN NOT NULL DEFAULT FALSE,
    public_edit_changed BOOLEAN NOT NULL DEFAULT FALSE,
    description TEXT,
    description_changed BOOLEAN NOT NULL DEFAULT FALSE,
    tags_changed BOOLEAN NOT NULL DEFAULT FALSE,
    group_access_changed BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE post_edit_history_tag(
    fk_post_edit_history BIGINT REFERENCES post_edit_history(pk) NOT NULL,
    fk_tag BIGINT REFERENCES tag(pk) NOT NULL,
    PRIMARY KEY(fk_post_edit_history, fk_tag)
);

CREATE TABLE post_edit_history_group_access(
    fk_post_edit_history BIGINT REFERENCES post_edit_history(pk) NOT NULL,
    fk_granted_group BIGINT REFERENCES user_group(pk) NOT NULL,
    write BOOLEAN NOT NULL DEFAULT FALSE,
    fk_granted_by BIGINT REFERENCES registered_user(pk) NOT NULL,
    creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY(fk_post_edit_history, fk_granted_group)
);

CREATE TABLE post_collection_edit_history(
    pk BIGSERIAL PRIMARY KEY,
    fk_post_collection BIGINT REFERENCES post_collection(pk) NOT NULL,
    fk_edit_user BIGINT REFERENCES registered_user(pk) NOT NULL,
    edit_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    title VARCHAR(300) NOT NULL,
    title_changed BOOLEAN NOT NULL DEFAULT FALSE,
    public BOOLEAN NOT NULL DEFAULT FALSE,
    public_changed BOOLEAN NOT NULL DEFAULT FALSE,
    public_edit BOOLEAN NOT NULL DEFAULT FALSE,
    public_edit_changed BOOLEAN NOT NULL DEFAULT FALSE,
    description TEXT,
    description_changed BOOLEAN NOT NULL DEFAULT FALSE,
    poster_object_key VARCHAR(255),
    poster_object_key_changed BOOLEAN NOT NULL DEFAULT FALSE,
    tags_changed BOOLEAN NOT NULL DEFAULT FALSE,
    group_access_changed BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE post_collection_edit_history_tag(
    fk_post_collection_edit_history BIGINT REFERENCES post_collection_edit_history(pk) NOT NULL,
    fk_tag BIGINT REFERENCES tag(pk) NOT NULL,
    PRIMARY KEY(fk_post_collection_edit_history, fk_tag)
);

CREATE TABLE post_collection_edit_history_group_access(
    fk_post_collection_edit_history BIGINT REFERENCES post_collection_edit_history(pk) NOT NULL,
    fk_granted_group BIGINT REFERENCES user_group(pk) NOT NULL,
    write BOOLEAN NOT NULL DEFAULT FALSE,
    fk_granted_by BIGINT REFERENCES registered_user(pk) NOT NULL,
    creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY(fk_post_collection_edit_history, fk_granted_group)
);

ALTER TABLE post ADD COLUMN edit_timestamp TIMESTAMP WITH TIME ZONE;
ALTER TABLE post ADD COLUMN fk_edit_user BIGINT REFERENCES registered_user(pk);
UPDATE post SET edit_timestamp = creation_timestamp, fk_edit_user = fk_create_user;
ALTER TABLE post ALTER COLUMN edit_timestamp SET NOT NULL, ALTER COLUMN fk_edit_user SET NOT NULL;

ALTER TABLE post_collection ADD COLUMN edit_timestamp TIMESTAMP WITH TIME ZONE;
ALTER TABLE post_collection ADD COLUMN fk_edit_user BIGINT REFERENCES registered_user(pk);
UPDATE post_collection SET edit_timestamp = creation_timestamp, fk_edit_user = fk_create_user;
ALTER TABLE post_collection ALTER COLUMN edit_timestamp SET NOT NULL, ALTER COLUMN fk_edit_user SET NOT NULL;

CREATE FUNCTION set_initial_change_values() RETURNS TRIGGER AS
$BODY$
BEGIN
    IF NEW.edit_timestamp IS NULL THEN
        NEW.edit_timestamp = NEW.creation_timestamp;
    END IF;
    IF NEW.fk_edit_user IS NULL THEN
        NEW.fk_edit_user = NEW.fk_create_user;
    END IF;
    RETURN NEW;
END;
$BODY$
language plpgsql;

CREATE TRIGGER before_insert_post_set_initial_change_values BEFORE INSERT ON post
FOR EACH ROW
EXECUTE PROCEDURE set_initial_change_values();

CREATE TRIGGER before_insert_post_collection_set_initial_change_values BEFORE INSERT ON post_collection
FOR EACH ROW
EXECUTE PROCEDURE set_initial_change_values();

CREATE INDEX post_edit_history_fk_post ON post_edit_history(fk_post);
CREATE INDEX post_collection_edit_history_fk_post_collection ON post_collection_edit_history(fk_post_collection);
