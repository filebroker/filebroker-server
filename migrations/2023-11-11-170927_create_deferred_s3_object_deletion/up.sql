CREATE TABLE deferred_s3_object_deletion (
    object_key VARCHAR(255) NOT NULL UNIQUE PRIMARY KEY,
    locked_at TIMESTAMP WITH TIME ZONE,
    fail_count INTEGER,
    fk_broker BIGINT REFERENCES broker(pk) NOT NULL
);
