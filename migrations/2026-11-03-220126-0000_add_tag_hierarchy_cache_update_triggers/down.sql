DROP TRIGGER after_update_tag_notify_cache ON tag;
DROP TRIGGER after_delete_tag_notify_cache ON tag;
DROP TRIGGER after_insert_tag_notify_cache ON tag;

DROP TRIGGER after_update_tag_alias_notify_cache ON tag_alias;
DROP TRIGGER after_delete_tag_alias_notify_cache ON tag_alias;
DROP TRIGGER after_insert_tag_alias_notify_cache ON tag_alias;

DROP TRIGGER after_update_tag_closure_notify_cache ON tag_closure_table;
DROP TRIGGER after_delete_tag_closure_notify_cache ON tag_closure_table;
DROP TRIGGER after_insert_tag_closure_notify_cache ON tag_closure_table;

DROP FUNCTION notify_tag_cache_tag_update_trigger();
DROP FUNCTION notify_tag_cache_tag_delete_trigger();
DROP FUNCTION notify_tag_cache_tag_insert_trigger();
DROP FUNCTION notify_tag_cache_alias_update_trigger();
DROP FUNCTION notify_tag_cache_alias_delete_trigger();
DROP FUNCTION notify_tag_cache_alias_insert_trigger();
DROP FUNCTION notify_tag_cache_closure_update_trigger();
DROP FUNCTION notify_tag_cache_closure_delete_trigger();
DROP FUNCTION notify_tag_cache_closure_insert_trigger();
DROP FUNCTION notify_tag_cache_changed(BIGINT[]);

DROP TABLE tag_cache_generation;
