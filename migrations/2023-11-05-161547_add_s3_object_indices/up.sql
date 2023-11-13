CREATE INDEX s3_object_mime_type_idx ON s3_object(mime_type);
CREATE INDEX s3_object_fk_broker_idx ON s3_object(fk_broker);
CREATE INDEX s3_object_fk_uploader_idx ON s3_object(fk_uploader);
CREATE INDEX s3_object_thumbnail_object_key_idx ON s3_object(thumbnail_object_key);
CREATE INDEX s3_object_creation_timestamp_idx ON s3_object(creation_timestamp);
CREATE INDEX s3_object_hls_master_playlist_idx ON s3_object(hls_master_playlist);
CREATE INDEX post_s3_object_idx ON post(s3_object);

CREATE FUNCTION sanitise_s3_object_input() RETURNS TRIGGER AS
$BODY$
BEGIN
    NEW.mime_type := NULLIF(TRIM(LOWER(NEW.mime_type)), '');
    NEW.filename := NULLIF(TRIM(NEW.filename), '');
    RETURN NEW;
END;
$BODY$
language plpgsql;

CREATE TRIGGER before_insert_or_update_sanitise_s3_object_input BEFORE INSERT OR UPDATE ON s3_object
FOR EACH ROW
EXECUTE PROCEDURE sanitise_s3_object_input();
