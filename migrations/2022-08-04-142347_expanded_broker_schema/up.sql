ALTER TABLE post ADD COLUMN s3_object VARCHAR(255) REFERENCES s3_object(object_key);
UPDATE post SET s3_object = (SELECT object_key FROM s3_object WHERE pk = post.fk_s3_object);
ALTER TABLE post DROP COLUMN fk_s3_object;

ALTER TABLE s3_object DROP COLUMN pk;
ALTER TABLE s3_object ADD PRIMARY KEY(object_key);

CREATE TABLE object_upload_status (
    unique_id VARCHAR(255) PRIMARY KEY
);

INSERT INTO object_upload_status(unique_id) VALUES('running'), ('paused'), ('stopped'), ('completed'), ('failed');

CREATE TABLE object_upload (
    object_key VARCHAR(255) PRIMARY KEY,
    status VARCHAR(255) NOT NULL REFERENCES object_upload_status(unique_id),
    bytes_written BIGINT NOT NULL,
    total_bytes BIGINT NOT NULL,
    current_rate BIGINT,
    estimated_millis_remaining BIGINT,
    completed_object VARCHAR(255) REFERENCES s3_object(object_key),
    mime_type VARCHAR(255) NOT NULL,
    fk_broker INTEGER REFERENCES broker(pk) NOT NULL,
    fk_uploader INTEGER REFERENCES registered_user(pk) NOT NULL
);
