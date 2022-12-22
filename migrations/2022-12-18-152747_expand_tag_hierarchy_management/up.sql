
-- when a tag_alias is created, synchronise tag_edges between the two tags

CREATE FUNCTION synchronise_tag_edge_with_alias() RETURNS TRIGGER AS
$BODY$
BEGIN
    INSERT INTO tag_edge(fk_parent, fk_child)
    SELECT fk_parent, NEW.fk_target FROM tag_edge WHERE fk_child = NEW.fk_source OR fk_child = NEW.fk_target
    UNION ALL
    SELECT NEW.fk_target, fk_child FROM tag_edge WHERE fk_parent = NEW.fk_source OR fk_parent = NEW.fk_target
    ON CONFLICT DO NOTHING;
    RETURN NULL;
END;
$BODY$
language plpgsql;

CREATE TRIGGER after_insert_synchronise_tag_edge_with_alias AFTER INSERT ON tag_alias
FOR EACH ROW
EXECUTE PROCEDURE synchronise_tag_edge_with_alias();

-- when a tag_edge is created, copy the tag_edge to the tag's aliases

CREATE FUNCTION copy_tag_edge_to_alias() RETURNS TRIGGER AS
$BODY$
BEGIN
    INSERT INTO tag_edge(fk_parent, fk_child)
    SELECT NEW.fk_parent, pk FROM tag WHERE EXISTS(SELECT * FROM tag_alias WHERE (fk_source = tag.pk AND fk_target = NEW.fk_child) OR (fk_source = NEW.fk_child AND fk_target = tag.pk))
    UNION ALL
    SELECT pk, NEW.fk_child FROM tag WHERE EXISTS(SELECT * FROM tag_alias WHERE (fk_source = tag.pk AND fk_target = NEW.fk_parent) OR (fk_source = NEW.fk_parent AND fk_target = tag.pk))
    ON CONFLICT DO NOTHING;
    RETURN NULL;
END;
$BODY$
language plpgsql;

CREATE TRIGGER after_insert_copy_tag_edge_to_alias AFTER INSERT ON tag_edge
FOR EACH ROW
EXECUTE PROCEDURE copy_tag_edge_to_alias();

-- remove deleted tag_edges from tag aliases

CREATE FUNCTION remove_tag_edge_from_aliases() RETURNS TRIGGER AS
$BODY$
BEGIN
    DELETE FROM tag_edge
    WHERE
    fk_parent IN(SELECT OLD.fk_parent UNION ALL SELECT fk_target FROM tag_alias WHERE fk_source = OLD.fk_parent UNION ALL SELECT fk_source FROM tag_alias WHERE fk_target = OLD.fk_parent)
    AND fk_child IN(SELECT OLD.fk_child UNION ALL SELECT fk_target FROM tag_alias WHERE fk_source = OLD.fk_child UNION ALL SELECT fk_source FROM tag_alias WHERE fk_target = OLD.fk_child);
    RETURN NULL;
END;
$BODY$
language plpgsql;

CREATE TRIGGER after_delete_remove_tag_edge_from_aliases AFTER DELETE ON tag_edge
FOR EACH ROW
EXECUTE PROCEDURE remove_tag_edge_from_aliases();

/*

Ensure tag hierarchy integrity

1) prevent insertion if the added parent already is a parent or ancestor at any depth:

    1                                      1
   /                                      /
  2        set 1 as parent of 3 ->       2
   \                                      \
    3                                      3
   hierarchy stays the same because 1 already is an ancestor of 3

2) delete edges to existing parents that are parents of the newly added parent

      1                                   1
     / \   set 2 as parent of 3 ->       /
    2   3                               2
                                         \
                                          3
   the edge between tag 1 and 3 has been deleted as 3 is now a descendant of 1 at a higher depth

3) to prevent cycles, delete edges where the child is the new parent and the parent is a child or descendant of the new child (or the new child itself)

     1                                      4   1
    /                                        \ /
   2                                          2
    \      set 4 as parent of 2 ->             \
     3                                          3
    /
    4
   now that 4 is a parent of 2 it has been removed as a child of 3, otherwise there would be a cycle

*/

CREATE FUNCTION prevent_duplicate_ancestry() RETURNS TRIGGER AS
$BODY$
BEGIN
    IF EXISTS(
        SELECT *
        FROM tag_closure_table
        WHERE fk_parent = NEW.fk_parent
        AND fk_child = NEW.fk_child
    )
    THEN
        -- 1) prevent insertion if the added parent already is a parent or ancestor at any depth
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$BODY$
language plpgsql;

CREATE TRIGGER before_insert_prevent_duplicate_ancestry BEFORE INSERT ON tag_edge
FOR EACH ROW
EXECUTE PROCEDURE prevent_duplicate_ancestry();

CREATE FUNCTION ensure_tag_hierarchy_integrity() RETURNS TRIGGER AS
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

CREATE TRIGGER after_insert_ensure_tag_hierarchy_integrity AFTER INSERT ON tag_edge
FOR EACH ROW
EXECUTE PROCEDURE ensure_tag_hierarchy_integrity();
