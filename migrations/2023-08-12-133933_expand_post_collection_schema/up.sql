CREATE TABLE post_collection_tag (
    fk_post_collection BIGINT REFERENCES post_collection(pk) NOT NULL,
    fk_tag BIGINT REFERENCES tag(pk) NOT NULL,
    PRIMARY KEY(fk_post_collection, fk_tag)
);

CREATE INDEX post_collection_tag_fk_tag_idx ON post_collection_tag(fk_tag);

ALTER TABLE post_collection_item DROP CONSTRAINT post_collection_item_pkey;
ALTER TABLE post_collection_item ADD COLUMN pk BIGSERIAL PRIMARY KEY;
CREATE INDEX post_collection_item_fk_post_idx ON post_collection_item(fk_post);

ALTER TABLE post_collection RENAME COLUMN name TO title;
ALTER TABLE post_collection RENAME COLUMN fk_owner TO fk_create_user;
ALTER TABLE post_collection ADD COLUMN public_edit BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE post_collection ADD COLUMN poster_object_key VARCHAR(255) REFERENCES s3_object(object_key);
ALTER TABLE post_collection ADD COLUMN description TEXT;

ALTER INDEX post_collection_fk_owner_idx RENAME TO post_collection_fk_create_user_idx;

CREATE INDEX post_collection_fk_create_user_desc_idx ON post_collection(fk_create_user DESC, pk DESC);

CREATE INDEX post_collection_creation_timestamp_idx ON post_collection(creation_timestamp, pk DESC);
CREATE INDEX post_collection_creation_timestamp_desc_idx ON post_collection(creation_timestamp DESC, pk DESC);

CREATE INDEX post_collection_fk_create_user_creation_timestamp_idx ON post_collection(fk_create_user, creation_timestamp, pk DESC);
CREATE INDEX post_collection_fk_create_user_desc_creation_timestamp_idx ON post_collection(fk_create_user DESC, creation_timestamp, pk DESC);
CREATE INDEX post_collection_fk_create_user_creation_timestamp_desc_idx ON post_collection(fk_create_user, creation_timestamp DESC, pk DESC);
CREATE INDEX post_collection_fk_create_user_desc_creation_timestamp_desc_idx ON post_collection(fk_create_user DESC, creation_timestamp DESC, pk DESC);
CREATE INDEX post_collection_creation_timestamp_fk_create_user_idx ON post_collection(creation_timestamp, fk_create_user, pk DESC);
CREATE INDEX post_collection_creation_timestamp_fk_create_user_desc_idx ON post_collection(creation_timestamp, fk_create_user DESC, pk DESC);
CREATE INDEX post_collection_creation_timestamp_desc_fk_create_user_idx ON post_collection(creation_timestamp DESC, fk_create_user, pk DESC);
CREATE INDEX post_collection_creation_timestamp_desc_fk_create_user_desc_idx ON post_collection(creation_timestamp DESC, fk_create_user DESC, pk DESC);

CREATE INDEX post_collection_title_idx ON post_collection(LOWER(title), pk DESC);
CREATE INDEX post_collection_title_desc_idx ON post_collection(LOWER(title) DESC NULLS LAST, pk DESC);

CREATE INDEX post_collection_title_creation_timestamp_idx ON post_collection(LOWER(title), creation_timestamp, pk DESC);
CREATE INDEX post_collection_title_desc_creation_timestamp_idx ON post_collection(LOWER(title) DESC NULLS LAST, creation_timestamp, pk DESC);
CREATE INDEX post_collection_title_creation_timestamp_desc_idx ON post_collection(LOWER(title), creation_timestamp DESC, pk DESC);
CREATE INDEX post_collection_title_desc_creation_timestamp_desc_idx ON post_collection(LOWER(title) DESC NULLS LAST, creation_timestamp DESC, pk DESC);
CREATE INDEX post_collection_creation_timestamp_title_idx ON post_collection(creation_timestamp, LOWER(title), pk DESC);
CREATE INDEX post_collection_creation_timestamp_title_desc_idx ON post_collection(creation_timestamp, LOWER(title) DESC NULLS LAST, pk DESC);
CREATE INDEX post_collection_creation_timestamp_desc_title_idx ON post_collection(creation_timestamp DESC, LOWER(title), pk DESC);
CREATE INDEX post_collection_creation_timestamp_desc_title_desc_idx ON post_collection(creation_timestamp DESC, LOWER(title) DESC NULLS LAST, pk DESC);
CREATE INDEX post_collection_title_fk_create_user_idx ON post_collection(LOWER(title), fk_create_user, pk DESC);
CREATE INDEX post_collection_title_fk_create_user_desc_idx ON post_collection(LOWER(title), fk_create_user DESC, pk DESC);
CREATE INDEX post_collection_title_desc_fk_create_user_idx ON post_collection(LOWER(title) DESC NULLS LAST, fk_create_user, pk DESC);
CREATE INDEX post_collection_title_desc_fk_create_user_desc_idx ON post_collection(LOWER(title) DESC NULLS LAST, fk_create_user DESC, pk DESC);
CREATE INDEX post_collection_fk_create_user_title_idx ON post_collection(fk_create_user, LOWER(title), pk DESC);
CREATE INDEX post_collection_fk_create_user_desc_title_idx ON post_collection(fk_create_user DESC, LOWER(title), pk DESC);
CREATE INDEX post_collection_fk_create_user_title_desc_idx ON post_collection(fk_create_user, LOWER(title) DESC NULLS LAST, pk DESC);
CREATE INDEX post_collection_fk_create_user_desc_title_desc_idx ON post_collection(fk_create_user DESC, LOWER(title) DESC NULLS LAST, pk DESC);

CREATE INDEX post_collection_description_idx ON post_collection(LOWER(description)) WHERE LENGTH(description) < 2048;

CREATE INDEX post_collection_title_gin_idx ON post_collection USING gin(LOWER(title) gin_trgm_ops);
CREATE INDEX post_collection_description_gin_idx ON post_collection USING gin(LOWER(description) gin_trgm_ops);

CREATE FUNCTION set_empty_post_collection_strings_to_null() RETURNS TRIGGER AS
$BODY$
BEGIN
    NEW.title := NULLIF(TRIM(regexp_replace(NEW.title, '\s+', ' ', 'g')), '');
    NEW.description := NULLIF(TRIM(NEW.description), '');
    NEW.poster_object_key := NULLIF(TRIM(NEW.poster_object_key), '');
    RETURN NEW;
END;
$BODY$
language plpgsql;

CREATE TRIGGER before_insert_or_update_set_empty_post_collection_strings_to_null BEFORE INSERT OR UPDATE ON post_collection
FOR EACH ROW
EXECUTE PROCEDURE set_empty_post_collection_strings_to_null();

CREATE INDEX s3_object_sha256_hash_idx ON s3_object(sha256_hash);

ALTER TABLE post_collection_item ADD COLUMN ordinal INTEGER;
-- order all post_collection_items by creation_timestamp, pk and get the row number, then subtract the lowest ordinal per collection to have each collection start at 0
WITH ordered_post_collection_items AS (
    SELECT pk, fk_post, fk_post_collection, row_number() OVER (ORDER BY creation_timestamp, pk) AS ordinal FROM post_collection_item
) UPDATE post_collection_item SET ordinal = (SELECT ordinal FROM ordered_post_collection_items WHERE pk = post_collection_item.pk) - (SELECT MIN(ordinal) FROM ordered_post_collection_items WHERE fk_post_collection = post_collection_item.fk_post_collection);
ALTER TABLE post_collection_item ALTER COLUMN ordinal SET NOT NULL;
ALTER TABLE post_collection_item ADD CONSTRAINT post_collection_item_fk_post_collection_ordinal_unique_constr UNIQUE(fk_post_collection, ordinal) DEFERRABLE INITIALLY DEFERRED;
