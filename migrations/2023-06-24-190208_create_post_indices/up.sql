CREATE INDEX post_fk_create_user_idx ON post(fk_create_user, pk DESC);
CREATE INDEX post_fk_create_user_desc_idx ON post(fk_create_user DESC, pk DESC);

CREATE INDEX post_creation_timestamp_idx ON post(creation_timestamp, pk DESC);
CREATE INDEX post_creation_timestamp_desc_idx ON post(creation_timestamp DESC, pk DESC);

CREATE INDEX post_fk_create_user_creation_timestamp_idx ON post(fk_create_user, creation_timestamp, pk DESC);
CREATE INDEX post_fk_create_user_desc_creation_timestamp_idx ON post(fk_create_user DESC, creation_timestamp, pk DESC);
CREATE INDEX post_fk_create_user_creation_timestamp_desc_idx ON post(fk_create_user, creation_timestamp DESC, pk DESC);
CREATE INDEX post_fk_create_user_desc_creation_timestamp_desc_idx ON post(fk_create_user DESC, creation_timestamp DESC, pk DESC);
CREATE INDEX post_creation_timestamp_fk_create_user_idx ON post(creation_timestamp, fk_create_user, pk DESC);
CREATE INDEX post_creation_timestamp_fk_create_user_desc_idx ON post(creation_timestamp, fk_create_user DESC, pk DESC);
CREATE INDEX post_creation_timestamp_desc_fk_create_user_idx ON post(creation_timestamp DESC, fk_create_user, pk DESC);
CREATE INDEX post_creation_timestamp_desc_fk_create_user_desc_idx ON post(creation_timestamp DESC, fk_create_user DESC, pk DESC);

CREATE INDEX post_title_idx ON post(LOWER(title), pk DESC);
CREATE INDEX post_title_desc_idx ON post(LOWER(title) DESC NULLS LAST, pk DESC);

CREATE INDEX post_title_creation_timestamp_idx ON post(LOWER(title), creation_timestamp, pk DESC);
CREATE INDEX post_title_desc_creation_timestamp_idx ON post(LOWER(title) DESC NULLS LAST, creation_timestamp, pk DESC);
CREATE INDEX post_title_creation_timestamp_desc_idx ON post(LOWER(title), creation_timestamp DESC, pk DESC);
CREATE INDEX post_title_desc_creation_timestamp_desc_idx ON post(LOWER(title) DESC NULLS LAST, creation_timestamp DESC, pk DESC);
CREATE INDEX post_creation_timestamp_title_idx ON post(creation_timestamp, LOWER(title), pk DESC);
CREATE INDEX post_creation_timestamp_title_desc_idx ON post(creation_timestamp, LOWER(title) DESC NULLS LAST, pk DESC);
CREATE INDEX post_creation_timestamp_desc_title_idx ON post(creation_timestamp DESC, LOWER(title), pk DESC);
CREATE INDEX post_creation_timestamp_desc_title_desc_idx ON post(creation_timestamp DESC, LOWER(title) DESC NULLS LAST, pk DESC);
CREATE INDEX post_title_fk_create_user_idx ON post(LOWER(title), fk_create_user, pk DESC);
CREATE INDEX post_title_fk_create_user_desc_idx ON post(LOWER(title), fk_create_user DESC, pk DESC);
CREATE INDEX post_title_desc_fk_create_user_idx ON post(LOWER(title) DESC NULLS LAST, fk_create_user, pk DESC);
CREATE INDEX post_title_desc_fk_create_user_desc_idx ON post(LOWER(title) DESC NULLS LAST, fk_create_user DESC, pk DESC);
CREATE INDEX post_fk_create_user_title_idx ON post(fk_create_user, LOWER(title), pk DESC);
CREATE INDEX post_fk_create_user_desc_title_idx ON post(fk_create_user DESC, LOWER(title), pk DESC);
CREATE INDEX post_fk_create_user_title_desc_idx ON post(fk_create_user, LOWER(title) DESC NULLS LAST, pk DESC);
CREATE INDEX post_fk_create_user_desc_title_desc_idx ON post(fk_create_user DESC, LOWER(title) DESC NULLS LAST, pk DESC);

CREATE INDEX post_description_idx ON post(LOWER(description)) WHERE LENGTH(description) < 2048;

CREATE EXTENSION pg_trgm;

CREATE INDEX post_title_gin_idx ON post USING gin(LOWER(title) gin_trgm_ops);
CREATE INDEX post_description_gin_idx ON post USING gin(LOWER(description) gin_trgm_ops);
