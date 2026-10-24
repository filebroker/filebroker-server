DROP TABLE apply_auto_tags_task;

ALTER TABLE registered_user DROP COLUMN is_admin;
ALTER TABLE registered_user DROP COLUMN is_banned;

DROP TRIGGER before_insert_or_update_set_empty_tag_strings_to_null ON tag;
DROP FUNCTION set_empty_tag_strings_to_null();
DROP TRIGGER before_insert_or_update_set_empty_tag_category_strings_to_null ON tag_category;
DROP FUNCTION set_empty_tag_category_strings_to_null();

DROP FUNCTION evaluate_tag_auto_match_condition(compiled_query text, filter_condition text);

ALTER TABLE tag DROP COLUMN auto_match_condition_post;
ALTER TABLE tag DROP COLUMN auto_match_condition_collection;
ALTER TABLE tag DROP COLUMN compiled_auto_match_condition_post;
ALTER TABLE tag DROP COLUMN compiled_auto_match_condition_collection;

ALTER TABLE tag DROP COLUMN tag_category;
ALTER TABLE tag DROP COLUMN fk_create_user;
ALTER TABLE tag DROP COLUMN edit_timestamp;
ALTER TABLE tag DROP COLUMN fk_edit_user;

ALTER TABLE post_tag DROP COLUMN auto_matched;
ALTER TABLE post_collection_tag DROP COLUMN auto_matched;
ALTER TABLE post_edit_history_tag DROP COLUMN auto_matched;
ALTER TABLE post_collection_edit_history_tag DROP COLUMN auto_matched;

DROP TABLE tag_edit_history_parent;
DROP TABLE tag_edit_history_alias;
DROP TABLE tag_edit_history;

DROP TABLE tag_category;

DROP TRIGGER before_insert_tag_set_initial_change_values ON tag;
