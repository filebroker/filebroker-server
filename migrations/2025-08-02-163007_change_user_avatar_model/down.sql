ALTER TABLE registered_user ADD COLUMN avatar_url VARCHAR(2048);
ALTER TABLE registered_user DROP COLUMN avatar_object_key;

DROP INDEX broker_is_system_bucket_unique_idx;
ALTER TABLE broker DROP COLUMN is_system_bucket;
