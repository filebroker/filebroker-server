DROP TRIGGER after_insert_synchronise_tag_edge_with_alias ON tag_alias;
DROP FUNCTION synchronise_tag_edge_with_alias();

DROP TRIGGER after_insert_copy_tag_edge_to_alias ON tag_edge;
DROP FUNCTION copy_tag_edge_to_alias();

DROP TRIGGER after_delete_remove_tag_edge_from_aliases ON tag_edge;
DROP FUNCTION remove_tag_edge_from_aliases();

DROP TRIGGER before_insert_prevent_duplicate_ancestry ON tag_edge;
DROP FUNCTION prevent_duplicate_ancestry();

DROP TRIGGER after_insert_ensure_tag_hierarchy_integrity ON tag_edge;
DROP FUNCTION ensure_tag_hierarchy_integrity();
