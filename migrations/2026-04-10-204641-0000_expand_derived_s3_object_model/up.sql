UPDATE s3_object
SET hls_master_playlist = (
    SELECT master_playlist
    FROM hls_stream
    WHERE stream_playlist = object_key OR stream_file = object_key
)
WHERE hls_master_playlist IS NULL AND EXISTS(SELECT * FROM hls_stream WHERE stream_playlist = object_key OR stream_file = object_key);

UPDATE s3_object SET hls_master_playlist = object_key WHERE hls_master_playlist IS NULL AND EXISTS(SELECT * FROM hls_stream WHERE master_playlist = object_key);

ALTER TABLE s3_object ADD COLUMN derived_from VARCHAR(255) REFERENCES s3_object(object_key) ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED;

CREATE TYPE object_type AS ENUM (
    'original',
    'thumbnail',
    'hls_playlist',
    'hls_segment',
    'avatar'
);

ALTER TABLE s3_object ADD COLUMN object_type object_type;

UPDATE s3_object SET object_type = 'thumbnail' WHERE object_type IS NULL AND EXISTS(SELECT * FROM s3_object AS source_object WHERE source_object.thumbnail_object_key = s3_object.object_key);
UPDATE s3_object SET object_type = 'hls_playlist' WHERE object_type IS NULL AND EXISTS(SELECT * FROM hls_stream WHERE stream_playlist = object_key OR master_playlist = object_key);
UPDATE s3_object SET object_type = 'hls_segment' WHERE object_type IS NULL AND EXISTS(SELECT * FROM hls_stream WHERE stream_file = object_key);
UPDATE s3_object SET object_type = 'avatar' WHERE object_type IS NULL AND (EXISTS(SELECT * FROM registered_user WHERE avatar_object_key = s3_object.object_key) OR EXISTS(SELECT * FROM user_group WHERE avatar_object_key = s3_object.object_key));
UPDATE s3_object SET object_type = 'avatar' WHERE object_type IS NULL AND object_key ~ '^[^/]+/avatar_[^_]+_[^/]+$';
UPDATE s3_object SET object_type = 'original' WHERE object_type IS NULL;

ALTER TABLE s3_object ALTER COLUMN object_type SET NOT NULL;
CREATE INDEX s3_object_originals_idx ON s3_object(fk_broker, fk_uploader) WHERE derived_from IS NULL;
CREATE INDEX s3_object_fk_broker_object_type_idx ON s3_object(fk_broker, object_type);
CREATE INDEX s3_object_fk_broker_fk_uploader_object_type_idx ON s3_object(fk_broker, fk_uploader, object_type);

UPDATE s3_object AS target_object SET derived_from = source_object.object_key FROM s3_object AS source_object WHERE source_object.thumbnail_object_key = target_object.object_key;
UPDATE s3_object SET derived_from = (SELECT object_key FROM s3_object AS source_object WHERE source_object.object_type = 'original' AND source_object.hls_master_playlist = s3_object.hls_master_playlist) WHERE hls_master_playlist IS NOT NULL;
UPDATE s3_object AS avatar
    SET derived_from = source_object.object_key FROM s3_object AS source_object
    WHERE avatar.object_type = 'avatar'
        AND source_object.object_type = 'original'
        AND split_part(
            split_part(
                source_object.object_key,
                '/',
                array_length(string_to_array(source_object.object_key, '/'), 1)
            ),
            '.',
            1
        ) = substring(avatar.object_key from 'avatar_([^_]+)_');

-- CONSTRAINT needs to execute pending deferred checks before creating index
SET CONSTRAINTS s3_object_derived_from_fkey IMMEDIATE;
CREATE INDEX s3_object_derived_from_idx ON s3_object(derived_from);
