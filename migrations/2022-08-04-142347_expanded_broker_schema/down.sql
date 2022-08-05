DROP TABLE object_upload;
DROP TABLE object_upload_status;

ALTER TABLE s3_object ADD COLUMN pk SERIAL UNIQUE;

ALTER TABLE post ADD COLUMN fk_s3_object INTEGER REFERENCES s3_object(pk);
UPDATE post SET fk_s3_object = (SELECT pk FROM s3_object WHERE object_key = post.s3_object);
ALTER TABLE post DROP COLUMN s3_object;

ALTER TABLE s3_object DROP CONSTRAINT s3_object_pkey;
ALTER TABLE s3_object ADD PRIMARY KEY(pk);
