DROP TRIGGER after_insert_synchronise_tag_aliases ON tag_alias;
DROP FUNCTION synchronise_tag_aliases();

ALTER TABLE tag_alias DROP CONSTRAINT no_self_reference;
ALTER TABLE tag_edge DROP CONSTRAINT no_self_reference;

CREATE OR REPLACE FUNCTION synchronise_tag_edge_with_alias() RETURNS TRIGGER AS
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
