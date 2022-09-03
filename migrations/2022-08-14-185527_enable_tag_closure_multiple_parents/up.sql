DELETE FROM tag_closure_table;
ALTER TABLE tag_closure_table DROP CONSTRAINT tag_closure_table_fk_child_depth_key;
DROP INDEX duplicate_or_cyclic_tag_closure;

CREATE FUNCTION create_self_reference() RETURNS TRIGGER AS
$BODY$
BEGIN
    INSERT INTO tag_closure_table(fk_parent, fk_child, depth) VALUES(NEW.pk, NEW.pk, 0);
    RETURN NULL;
END;
$BODY$
language plpgsql;

CREATE TRIGGER create_tag_closure_self_reference AFTER INSERT ON tag
FOR EACH ROW
EXECUTE PROCEDURE create_self_reference();

CREATE TABLE tag_edge(
    fk_parent INTEGER REFERENCES tag(pk) NOT NULL,
    fk_child INTEGER REFERENCES tag(pk) NOT NULL,
    PRIMARY KEY(fk_parent, fk_child)
);

-- create triggers that update tag_closure_table when inserting or deleting tag_edges, see https://ruiyang.me/blog/post/a-simple-way-to-store-directed-acyclic-graph-in-a-relational-database

CREATE FUNCTION insert_edge_into_closure_table() RETURNS TRIGGER AS
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

CREATE TRIGGER after_inserting_edge_update_closure AFTER INSERT ON tag_edge 
FOR EACH ROW
EXECUTE PROCEDURE insert_edge_into_closure_table();

CREATE FUNCTION remove_edge_from_closure_table() RETURNS TRIGGER AS
$BODY$
DECLARE pk_to_delete INT;
DECLARE closure_to_delete CURSOR FOR
    SELECT path.pk
    FROM tag_closure_table p, tag_closure_table path, tag_closure_table c
    WHERE p.fk_parent = path.fk_parent AND c.fk_child = path.fk_child AND p.fk_child = OLD.fk_parent AND c.fk_parent = OLD.fk_child AND path.depth = p.depth + c.depth + 1 AND NOT EXISTS (
        SELECT *
        FROM tag_closure_table p1, tag_closure_table c1
        WHERE p1.pk NOT IN (
            SELECT path2.pk
            FROM tag_closure_table p2, tag_closure_table path2, tag_closure_table c2
            WHERE p2.fk_parent = path2.fk_parent AND c2.fk_child = path2.fk_child AND p2.fk_child = OLD.fk_parent AND c2.fk_parent = OLD.fk_child AND path2.depth = p2.depth + c2.depth + 1)
        AND c1.pk NOT IN (
            SELECT path3.pk
            FROM tag_closure_table p3, tag_closure_table path3, tag_closure_table c3
            WHERE p3.fk_parent = path3.fk_parent AND c3.fk_child = path3.fk_child AND p3.fk_child = OLD.fk_parent AND c3.fk_parent = OLD.fk_child AND path3.depth = p3.depth + c3.depth + 1)
        AND p1.fk_parent = path.fk_parent AND p1.fk_child = c1.fk_parent AND c1.fk_child = path.fk_child AND p1.depth + c1.depth = path.depth);
BEGIN
    IF NOT EXISTS (
        SELECT *
        FROM tag_edge
        WHERE fk_parent = OLD.fk_parent AND fk_child = OLD.fk_child
    )
    THEN
        OPEN closure_to_delete;
        LOOP
            FETCH closure_to_delete INTO pk_to_delete;
            IF NOT FOUND THEN EXIT;
            END IF;
            DELETE FROM tag_closure_table WHERE pk = pk_to_delete;
        END LOOP;
        CLOSE closure_to_delete;
    END IF;
    RETURN NULL;
END;
$BODY$
language plpgsql;

CREATE TRIGGER after_removing_edge_update_closure AFTER DELETE ON tag_edge
FOR EACH ROW
EXECUTE PROCEDURE remove_edge_from_closure_table();
