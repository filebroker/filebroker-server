CREATE OR REPLACE FUNCTION insert_edge_into_closure_table() RETURNS TRIGGER AS
$BODY$
BEGIN
    INSERT INTO tag_closure_table(fk_parent, fk_child, depth)
    SELECT p.fk_parent, c.fk_child, p.depth + c.depth + 1
    FROM tag_closure_table p, tag_closure_table c
    WHERE p.fk_child = NEW.fk_parent AND c.fk_parent = NEW.fk_child
    ON CONFLICT DO NOTHING;
    RETURN NULL;
END;
$BODY$
language plpgsql;

DROP TRIGGER before_insert_prevent_alias_hierarchy_conflict ON tag_alias;
DROP FUNCTION prevent_alias_hierarchy_conflict();

DROP TRIGGER before_insert_prevent_alias_hierarchy_edge ON tag_edge;
DROP FUNCTION prevent_alias_hierarchy_edge();
