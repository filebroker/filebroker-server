DROP TABLE post_edit_history_group_access;
DROP TABLE post_edit_history_tag;
DROP TABLE post_edit_history;

DROP TABLE post_collection_edit_history_group_access;
DROP TABLE post_collection_edit_history_tag;
DROP TABLE post_collection_edit_history;

DROP TRIGGER before_insert_post_set_initial_change_values ON post;
DROP TRIGGER before_insert_post_collection_set_initial_change_values ON post_collection;
DROP FUNCTION set_initial_change_values();

ALTER TABLE post DROP COLUMN edit_timestamp;
ALTER TABLE post DROP COLUMN fk_edit_user;
ALTER TABLE post_collection DROP COLUMN edit_timestamp;
ALTER TABLE post_collection DROP COLUMN fk_edit_user;
