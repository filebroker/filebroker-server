DROP INDEX s3_object_derived_from_idx;
DROP INDEX s3_object_originals_idx;
DROP INDEX s3_object_fk_broker_object_type_idx;
DROP INDEX s3_object_fk_broker_fk_uploader_object_type_idx;
ALTER TABLE s3_object DROP COLUMN object_type;
ALTER TABLE s3_object DROP COLUMN derived_from;

DROP TYPE object_type;

UPDATE s3_object SET hls_master_playlist = NULL
WHERE hls_master_playlist IS NOT NULL
AND NOT EXISTS(SELECT * FROM post WHERE s3_object = object_key)
AND EXISTS(SELECT * FROM hls_stream WHERE master_playlist = object_key OR stream_playlist = object_key OR stream_file = object_key);
