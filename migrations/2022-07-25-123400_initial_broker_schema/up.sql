CREATE TABLE broker (
    pk SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    bucket VARCHAR(255) NOT NULL,
    endpoint VARCHAR(2048) NOT NULL,
    access_key VARCHAR(255) NOT NULL,
    secret_key VARCHAR(255) NOT NULL,
    is_aws_region BOOLEAN NOT NULL,
    remove_duplicate_files BOOLEAN NOT NULL DEFAULT false,
    fk_owner INTEGER REFERENCES registered_user(pk) NOT NULL
);

CREATE TABLE s3_object (
    pk SERIAL PRIMARY KEY,
    object_key VARCHAR(255) NOT NULL UNIQUE,
    sha256_hash CHAR(64) NULL,
    size_bytes BIGINT NOT NULL,
    mime_type VARCHAR(255) NOT NULL,
    fk_broker INTEGER REFERENCES broker(pk) NOT NULL,
    fk_uploader INTEGER REFERENCES registered_user(pk) NOT NULL
);

ALTER TABLE post ADD COLUMN fk_s3_object INTEGER REFERENCES s3_object(pk);
