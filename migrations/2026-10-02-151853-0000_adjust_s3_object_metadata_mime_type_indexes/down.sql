DROP INDEX s3_object_metadata_video_idx;
CREATE INDEX s3_object_metadata_video_idx
    ON s3_object_metadata(object_key)
    WHERE LOWER(mime_type) LIKE 'video/%';

DROP INDEX s3_object_metadata_audio_idx;
CREATE INDEX s3_object_metadata_audio_idx
    ON s3_object_metadata(object_key)
    WHERE LOWER(mime_type) LIKE 'audio/%';

DROP INDEX s3_object_metadata_image_idx;
CREATE INDEX s3_object_metadata_image_idx
    ON s3_object_metadata(object_key)
    WHERE LOWER(mime_type) LIKE 'image/%';
