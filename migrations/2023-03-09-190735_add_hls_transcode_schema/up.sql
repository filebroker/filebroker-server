CREATE TABLE hls_stream(
    stream_playlist VARCHAR(255) NOT NULL UNIQUE REFERENCES s3_object(object_key),
    stream_file VARCHAR(255) NOT NULL UNIQUE REFERENCES s3_object(object_key),
    master_playlist VARCHAR(255) NOT NULL REFERENCES s3_object(object_key),
    resolution INTEGER NOT NULL,
    x264_preset VARCHAR(255) NOT NULL,
    target_bitrate VARCHAR(255) NOT NULL,
    min_bitrate VARCHAR(255) NOT NULL,
    max_bitrate VARCHAR(255) NOT NULL,
    PRIMARY KEY(stream_playlist)
);

ALTER TABLE s3_object ADD COLUMN hls_master_playlist VARCHAR(255) REFERENCES s3_object(object_key);
