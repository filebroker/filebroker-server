CREATE FUNCTION prevent_alias_hierarchy_conflict() RETURNS TRIGGER AS
$BODY$
BEGIN
    IF EXISTS (
        WITH source_aliases(pk) AS (
            SELECT NEW.fk_source

            UNION

            SELECT CASE
                WHEN fk_source = NEW.fk_source THEN fk_target
                ELSE fk_source
            END
            FROM tag_alias
            WHERE fk_source = NEW.fk_source
               OR fk_target = NEW.fk_source
        ),
        target_aliases(pk) AS (
            SELECT NEW.fk_target

            UNION

            SELECT CASE
                WHEN fk_source = NEW.fk_target THEN fk_target
                ELSE fk_source
            END
            FROM tag_alias
            WHERE fk_source = NEW.fk_target
               OR fk_target = NEW.fk_target
        )
        SELECT *
        FROM tag_closure_table
        WHERE depth > 0
          AND (
              (
                  fk_parent IN (SELECT pk FROM source_aliases)
                  AND fk_child IN (SELECT pk FROM target_aliases)
              )
              OR
              (
                  fk_parent IN (SELECT pk FROM target_aliases)
                  AND fk_child IN (SELECT pk FROM source_aliases)
              )
          )
    )
    THEN
        RAISE EXCEPTION
            'Cannot create alias relationship between tags % and % because their alias groups have a hierarchy relationship',
            NEW.fk_source,
            NEW.fk_target
            USING
                ERRCODE = '23514',
                CONSTRAINT = 'tag_alias_hierarchy_conflict';
    END IF;

    RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER before_insert_prevent_alias_hierarchy_conflict
BEFORE INSERT ON tag_alias
FOR EACH ROW
EXECUTE PROCEDURE prevent_alias_hierarchy_conflict();


CREATE FUNCTION prevent_alias_hierarchy_edge() RETURNS TRIGGER AS
$BODY$
BEGIN
    IF EXISTS (
        SELECT *
        FROM tag_alias
        WHERE
            (fk_source = NEW.fk_parent AND fk_target = NEW.fk_child)
            OR
            (fk_source = NEW.fk_child AND fk_target = NEW.fk_parent)
    )
    THEN
        RAISE EXCEPTION
            'Cannot create hierarchy relationship between alias tags % and %',
            NEW.fk_parent,
            NEW.fk_child
            USING
                ERRCODE = '23514',
                CONSTRAINT = 'tag_edge_alias_conflict';
    END IF;

    RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

-- runs before before_insert_prevent_duplicate_ancestry so an alias conflict is reported as invalid rather than being suppressed as redundant ancestry
CREATE TRIGGER before_insert_prevent_alias_hierarchy_edge
BEFORE INSERT ON tag_edge
FOR EACH ROW
EXECUTE PROCEDURE prevent_alias_hierarchy_edge();

-- an earlier AFTER INSERT trigger may recursively delete the newly inserted edge;
-- only materialise its closure rows if the edge still exists
CREATE OR REPLACE FUNCTION insert_edge_into_closure_table() RETURNS TRIGGER AS
$BODY$
BEGIN
    IF EXISTS (
        SELECT *
        FROM tag_edge
        WHERE fk_parent = NEW.fk_parent
          AND fk_child = NEW.fk_child
    )
    THEN
        INSERT INTO tag_closure_table(fk_parent, fk_child, depth)
        SELECT
            p.fk_parent,
            c.fk_child,
            p.depth + c.depth + 1
        FROM tag_closure_table p,
             tag_closure_table c
        WHERE p.fk_child = NEW.fk_parent
          AND c.fk_parent = NEW.fk_child
        ON CONFLICT DO NOTHING;
    END IF;

    RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;
