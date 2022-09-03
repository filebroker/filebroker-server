DELETE FROM tag_closure_table;
CREATE UNIQUE INDEX duplicate_or_cyclic_tag_closure ON tag_closure_table(greatest(fk_parent, fk_child), least(fk_child, fk_parent));
ALTER TABLE tag_closure_table ADD CONSTRAINT tag_closure_table_fk_child_depth_key UNIQUE(fk_child, depth);

DROP TRIGGER create_tag_closure_self_reference ON tag;
DROP FUNCTION create_self_reference();
DROP TRIGGER after_inserting_edge_update_closure ON tag_edge;
DROP FUNCTION insert_edge_into_closure_table();
DROP TRIGGER after_removing_edge_update_closure ON tag_edge;
DROP FUNCTION remove_edge_from_closure_table();
DROP TABLE tag_edge;
