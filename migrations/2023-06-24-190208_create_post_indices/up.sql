CREATE UNIQUE INDEX post_pk_desc_idx ON post(pk DESC NULLS LAST);
CREATE INDEX post_fk_create_user_idx ON post(fk_create_user);
CREATE INDEX post_fk_create_user_desc_idx ON post(fk_create_user DESC NULLS LAST);
CREATE INDEX post_creation_timestamp_idx ON post(creation_timestamp);
CREATE INDEX post_creation_timestamp_desc_idx ON post(creation_timestamp DESC NULLS LAST);
CREATE INDEX post_title_idx ON post(LOWER(title));
CREATE INDEX post_title_desc_idx ON post(LOWER(title) DESC NULLS LAST);
CREATE INDEX post_description_idx ON post(LOWER(description)) WHERE LENGTH(description) < 2048;
CREATE INDEX post_description_desc_idx ON post(LOWER(description) DESC NULLS LAST) WHERE LENGTH(description) < 2048;

CREATE EXTENSION pg_trgm;

CREATE INDEX post_title_gin_idx ON post USING gin(LOWER(title) gin_trgm_ops);
CREATE INDEX post_description_gin_idx ON post USING gin(LOWER(description) gin_trgm_ops);
