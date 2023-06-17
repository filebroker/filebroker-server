DROP TRIGGER before_insert_or_update_set_empty_user_strings_to_null ON registered_user;
DROP FUNCTION set_empty_user_strings_to_null();

ALTER TABLE registered_user DROP COLUMN email_confirmed;
ALTER TABLE registered_user DROP COLUMN display_name;
ALTER TABLE registered_user ALTER COLUMN user_name TYPE varchar(50);

ALTER TABLE refresh_token ADD COLUMN pk SERIAL;
ALTER TABLE refresh_token RENAME COLUMN fk_user TO fk_registered_user;
ALTER TABLE refresh_token DROP CONSTRAINT refresh_token_pkey;
ALTER TABLE refresh_token ADD PRIMARY KEY (pk);

DROP TABLE email_confirmation_token;

DROP TRIGGER before_update_unverify_email_on_change ON registered_user;
DROP FUNCTION unverify_email_on_change();

DROP TRIGGER before_update_increment_jwt_version_on_password_change ON registered_user;
DROP FUNCTION increment_jwt_version_on_password_change();

ALTER TABLE registered_user DROP COLUMN jwt_version;

ALTER TABLE registered_user DROP COLUMN password_fail_count;

DROP TABLE one_time_password;
