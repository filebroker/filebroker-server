ALTER TABLE s3_object ADD COLUMN hls_locked_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE s3_object ADD COLUMN thumbnail_locked_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE s3_object ADD COLUMN hls_fail_count INTEGER;
ALTER TABLE s3_object ADD COLUMN thumbnail_fail_count INTEGER;
