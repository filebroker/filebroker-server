ALTER TABLE broker ADD COLUMN creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();
ALTER TABLE s3_object ADD COLUMN creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();