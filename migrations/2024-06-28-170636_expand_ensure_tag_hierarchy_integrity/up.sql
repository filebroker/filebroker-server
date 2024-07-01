/*

(see 2022-12-18-152747_expand_tag_hierarchy_management)

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

(addition 2024-06-28-170636_expand_ensure_tag_hierarchy_integrity)

4) delete edges to existing children when the newly added child is a parent of the existing child

    1   2                                 1
     \ /   set 1 as parent of 2 ->       /
      3                                 2
                                         \
                                          3

    1 has been removed as a direct parent of 3 because it has been "moved up" to be a parent of 2 and thus a grandparent of 3

*/

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

    -- addition 2024-06-28-170636_expand_ensure_tag_hierarchy_integrity
    -- 4) delete edges to existing children when the newly added child is a parent of the existing child
    DELETE FROM tag_edge
    WHERE fk_parent = NEW.fk_parent AND fk_child IN(SELECT fk_child FROM tag_closure_table WHERE fk_parent = NEW.fk_child AND depth > 0);

    RETURN NULL;
END;
$BODY$
language plpgsql;
