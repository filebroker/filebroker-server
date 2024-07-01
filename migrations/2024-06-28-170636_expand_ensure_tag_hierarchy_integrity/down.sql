CREATE OR REPLACE FUNCTION ensure_tag_hierarchy_integrity() RETURNS TRIGGER AS
$BODY$
BEGIN
    -- 3) to prevent cycles, delete edges where the child is the new parent and the parent is a child or descendant of the new child (or the new child itself)
    DELETE FROM tag_edge
    WHERE fk_child = NEW.fk_parent AND fk_parent IN(SELECT fk_child FROM tag_closure_table WHERE fk_parent = NEW.fk_child);

    -- cycles need be removed before step 2), otherwise it would remove edges from the new child to all existing parents, since they're also parents of the new parent before the cycle is removed

    -- 2) delete edges to existing parents that are parents of the newly added parent
    DELETE FROM tag_edge
    WHERE fk_child = NEW.fk_child AND fk_parent IN(SELECT fk_parent FROM tag_closure_table WHERE fk_child = NEW.fk_parent AND depth > 0);

    RETURN NULL;
END;
$BODY$
language plpgsql;
