ALTER TABLE s3_object ADD COLUMN thumbnail_object_key VARCHAR(255) REFERENCES s3_object(object_key);
ALTER TABLE post ADD COLUMN thumbnail_url VARCHAR(2048);
