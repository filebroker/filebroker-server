CREATE TABLE hls_audio_stream
(
    stream_playlist     VARCHAR(255) NOT NULL UNIQUE REFERENCES s3_object (object_key) ON DELETE CASCADE,
    stream_file         VARCHAR(255) NOT NULL UNIQUE REFERENCES s3_object (object_key) ON DELETE CASCADE,
    master_playlist     VARCHAR(255) NOT NULL REFERENCES s3_object (object_key) ON DELETE CASCADE,
    source_stream_index INTEGER      NOT NULL,
    language            VARCHAR(32),
    title               TEXT,
    is_default          BOOLEAN      NOT NULL DEFAULT FALSE,
    autoselect          BOOLEAN      NOT NULL DEFAULT TRUE,
    source_codec        VARCHAR(64),
    codec               VARCHAR(64)  NOT NULL,
    bitrate             VARCHAR(32)  NOT NULL,
    channels            INTEGER,
    PRIMARY KEY (stream_playlist)
);

CREATE INDEX hls_audio_stream_master_playlist_idx ON hls_audio_stream (master_playlist);

CREATE TABLE hls_subtitle_stream
(
    stream_playlist     VARCHAR(255) NOT NULL UNIQUE REFERENCES s3_object (object_key) ON DELETE CASCADE,
    stream_file         VARCHAR(255) NOT NULL UNIQUE REFERENCES s3_object (object_key) ON DELETE CASCADE,
    master_playlist     VARCHAR(255) NOT NULL REFERENCES s3_object (object_key) ON DELETE CASCADE,
    source_stream_index INTEGER      NOT NULL,
    language            VARCHAR(32),
    title               TEXT,
    is_default          BOOLEAN      NOT NULL DEFAULT FALSE,
    autoselect          BOOLEAN      NOT NULL DEFAULT TRUE,
    forced              BOOLEAN      NOT NULL DEFAULT FALSE,
    source_codec        VARCHAR(64),
    codec               VARCHAR(64)  NOT NULL,
    PRIMARY KEY (stream_playlist)
);

CREATE INDEX hls_subtitle_stream_master_playlist_idx ON hls_subtitle_stream (master_playlist);

UPDATE s3_object
SET derived_from = NULL
WHERE derived_from = object_key;

ALTER TABLE hls_stream ADD COLUMN has_muxed_audio BOOLEAN;

UPDATE hls_stream hs
SET has_muxed_audio = metadata.audio_stream_count > 0
FROM s3_object master
    JOIN s3_object_metadata metadata ON metadata.object_key = master.derived_from
WHERE master.object_key = hs.master_playlist;
UPDATE s3_object_metadata SET has_muxed_audio = FALSE WHERE has_muxed_audio IS NULL;

ALTER TABLE hls_stream ALTER COLUMN has_muxed_audio SET NOT NULL;
