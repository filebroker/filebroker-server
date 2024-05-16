ALTER TABLE s3_object_metadata ALTER COLUMN audio_sample_rate TYPE DOUBLE PRECISION USING audio_sample_rate::DOUBLE PRECISION;
