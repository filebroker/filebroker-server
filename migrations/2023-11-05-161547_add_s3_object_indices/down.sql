DROP INDEX s3_object_mime_type_idx;
DROP INDEX s3_object_fk_broker_idx;
DROP INDEX s3_object_fk_uploader_idx;
DROP INDEX s3_object_thumbnail_object_key_idx;
DROP INDEX s3_object_creation_timestamp_idx;
DROP INDEX s3_object_hls_master_playlist_idx;
DROP INDEX post_s3_object_idx;

DROP TRIGGER before_insert_or_update_sanitise_s3_object_input ON s3_object;
DROP FUNCTION sanitise_s3_object_input();
