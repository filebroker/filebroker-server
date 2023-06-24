DROP INDEX post_pk_desc_idx;
DROP INDEX post_fk_create_user_idx;
DROP INDEX post_fk_create_user_desc_idx;
DROP INDEX post_creation_timestamp_idx;
DROP INDEX post_creation_timestamp_desc_idx;
DROP INDEX post_title_idx;
DROP INDEX post_title_desc_idx;
DROP INDEX post_description_idx;
DROP INDEX post_description_desc_idx;

DROP INDEX post_title_gin_idx;
DROP INDEX post_description_gin_idx;

DROP EXTENSION pg_trgm;
