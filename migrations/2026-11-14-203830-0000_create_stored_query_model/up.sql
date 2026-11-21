CREATE TYPE stored_query_scope AS ENUM (
    'post',
    'collection',
    'collection_item',
    'user_group'
);

CREATE TABLE stored_query
(
    pk                  BIGSERIAL PRIMARY KEY,
    fk_user             BIGINT                   NOT NULL REFERENCES registered_user (pk) ON DELETE CASCADE,

    scope               stored_query_scope       NOT NULL,
    query_definition    JSONB                    NOT NULL,

    is_saved            BOOLEAN                  NOT NULL DEFAULT FALSE,
    name                VARCHAR(255),

    creation_timestamp  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_used_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX stored_query_fk_user_idx ON stored_query (fk_user);

CREATE UNIQUE INDEX stored_query_recent_unique_idx
    ON stored_query (
        fk_user,
        scope,
        md5(query_definition::text)
    ) WHERE NOT is_saved;

CREATE INDEX stored_query_recent_idx
    ON stored_query (fk_user, last_used_timestamp DESC) WHERE NOT is_saved;

CREATE INDEX stored_query_saved_idx
    ON stored_query (fk_user, creation_timestamp DESC) WHERE is_saved;

CREATE FUNCTION set_empty_stored_query_strings_to_null() RETURNS TRIGGER AS
$BODY$
BEGIN
    NEW.name := NULLIF(TRIM(regexp_replace(NEW.name, '\s+', ' ', 'g')), '');
    RETURN NEW;
END;
$BODY$
language plpgsql;

CREATE TRIGGER before_insert_or_update_set_empty_stored_query_strings_to_null BEFORE INSERT OR UPDATE ON stored_query
FOR EACH ROW
EXECUTE PROCEDURE set_empty_stored_query_strings_to_null();
