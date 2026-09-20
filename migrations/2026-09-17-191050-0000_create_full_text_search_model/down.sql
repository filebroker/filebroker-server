-- Remove fulltext search triggers

DROP TRIGGER post_update_search_index
    ON post;

DROP TRIGGER s3_object_metadata_update_post_search_index
    ON s3_object_metadata;

DROP TRIGGER post_collection_update_search_index
    ON post_collection;

DROP TRIGGER user_group_update_search_index
    ON user_group;


-- Remove fulltext search update functions

DROP FUNCTION update_post_search_index_from_post();

DROP FUNCTION update_post_search_index_from_s3_object_metadata();

DROP FUNCTION update_post_collection_search_index();

DROP FUNCTION update_user_group_search_index();

DROP FUNCTION update_post_search_index(BIGINT);


-- Remove per-attribute fulltext indexes

DROP INDEX post_title_fts_idx;
DROP INDEX post_description_fts_idx;

DROP INDEX s3_object_metadata_mime_type_fts_idx;
DROP INDEX s3_object_metadata_artist_fts_idx;
DROP INDEX s3_object_metadata_album_fts_idx;
DROP INDEX s3_object_metadata_composer_fts_idx;
DROP INDEX s3_object_metadata_genre_fts_idx;

DROP INDEX post_collection_title_fts_idx;
DROP INDEX post_collection_description_fts_idx;

DROP INDEX user_group_name_fts_idx;
DROP INDEX user_group_description_fts_idx;


-- Remove composer indexes that did not exist before this migration

DROP INDEX s3_object_metadata_composer_gin_idx;
DROP INDEX s3_object_metadata_composer_idx;


-- Restore previous lower(...) trigram indexes

DROP INDEX tag_name_gin_idx;
CREATE INDEX tag_name_gin_idx
    ON tag
    USING GIN (lower(tag_name::text) gin_trgm_ops);


DROP INDEX post_title_gin_idx;
CREATE INDEX post_title_gin_idx
    ON post
    USING GIN (lower(title::text) gin_trgm_ops);

DROP INDEX post_description_gin_idx;
CREATE INDEX post_description_gin_idx
    ON post
    USING GIN (lower(description) gin_trgm_ops);


DROP INDEX post_collection_title_gin_idx;
CREATE INDEX post_collection_title_gin_idx
    ON post_collection
    USING GIN (lower(title::text) gin_trgm_ops);

DROP INDEX post_collection_description_gin_idx;
CREATE INDEX post_collection_description_gin_idx
    ON post_collection
    USING GIN (lower(description) gin_trgm_ops);


DROP INDEX user_group_name_gin_idx;
CREATE INDEX user_group_name_gin_idx
    ON user_group
    USING GIN (lower(name::text) gin_trgm_ops);

DROP INDEX user_group_description_gin_idx;
CREATE INDEX user_group_description_gin_idx
    ON user_group
    USING GIN (lower(description) gin_trgm_ops);


DROP INDEX s3_object_metadata_artist_gin_idx;
CREATE INDEX s3_object_metadata_artist_gin_idx
    ON s3_object_metadata
    USING GIN (lower(artist) gin_trgm_ops);

DROP INDEX s3_object_metadata_album_gin_idx;
CREATE INDEX s3_object_metadata_album_gin_idx
    ON s3_object_metadata
    USING GIN (lower(album) gin_trgm_ops);

DROP INDEX s3_object_metadata_genre_gin_idx;
CREATE INDEX s3_object_metadata_genre_gin_idx
    ON s3_object_metadata
    USING GIN (lower(genre) gin_trgm_ops);

DROP INDEX s3_object_metadata_mime_type_gin_idx;
CREATE INDEX s3_object_metadata_mime_type_gin_idx
    ON s3_object_metadata
    USING GIN (lower(mime_type) gin_trgm_ops);


-- Remove fulltext search tables.
-- Their search_vector/search_text indexes are dropped automatically with them.

DROP TABLE post_search_index;
DROP TABLE post_collection_search_index;
DROP TABLE user_group_search_index;
