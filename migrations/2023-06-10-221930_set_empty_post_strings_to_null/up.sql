UPDATE post SET title = NULL WHERE title = '';
UPDATE post SET description = NULL WHERE description = '';
UPDATE post SET data_url = NULL WHERE data_url = '';
UPDATE post SET source_url = NULL WHERE source_url = '';
UPDATE post SET s3_object = NULL WHERE s3_object = '';
UPDATE post SET thumbnail_url = NULL WHERE thumbnail_url = '';

CREATE FUNCTION set_empty_post_strings_to_null() RETURNS TRIGGER AS
$BODY$
BEGIN
    NEW.title := NULLIF(NEW.title, '');
    NEW.description := NULLIF(NEW.description, '');
    NEW.data_url := NULLIF(NEW.data_url, '');
    NEW.source_url := NULLIF(NEW.source_url, '');
    NEW.s3_object := NULLIF(NEW.s3_object, '');
    NEW.thumbnail_url := NULLIF(NEW.thumbnail_url, '');
    RETURN NEW;
END;
$BODY$
language plpgsql;

CREATE TRIGGER before_insert_or_update_set_empty_post_strings_to_null BEFORE INSERT OR UPDATE ON post
FOR EACH ROW
EXECUTE PROCEDURE set_empty_post_strings_to_null();
