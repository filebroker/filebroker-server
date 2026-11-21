CREATE TABLE stored_constriction
(
    pk                 BIGSERIAL PRIMARY KEY,
    fk_user            BIGINT REFERENCES registered_user (pk) ON DELETE CASCADE,

    scope              stored_query_scope       NOT NULL,
    name               VARCHAR(255)             NOT NULL,
    condition          VARCHAR(1024)            NOT NULL,

    creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX stored_constriction_fk_user_idx ON stored_constriction (fk_user);

CREATE FUNCTION set_empty_stored_constriction_strings_to_null() RETURNS TRIGGER AS
$BODY$
BEGIN
    NEW.name := NULLIF(TRIM(regexp_replace(NEW.name, '\s+', ' ', 'g')), '');
    RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER before_insert_or_update_set_empty_stored_constriction_strings_to_null
BEFORE INSERT OR UPDATE ON stored_constriction
FOR EACH ROW
EXECUTE PROCEDURE set_empty_stored_constriction_strings_to_null();
