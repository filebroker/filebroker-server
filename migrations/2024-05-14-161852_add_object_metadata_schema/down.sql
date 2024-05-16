ALTER TABLE s3_object DROP COLUMN metadata_locked_at;
ALTER TABLE s3_object DROP COLUMN metadata_fail_count;

DROP TABLE s3_object_metadata;
