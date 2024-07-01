-- a tag cannot be an alias of a tag without also being an alias of all other aliases of that tag, these triggers ensure that integrity

-- when creating a tag_alias, synchronise the aliases of both tags

CREATE FUNCTION synchronise_tag_aliases() RETURNS TRIGGER AS
$BODY$
BEGIN
    INSERT INTO tag_alias(fk_source, fk_target)
    SELECT NEW.fk_source, pk FROM tag WHERE pk != NEW.fk_source AND EXISTS(SELECT * FROM tag_alias WHERE (fk_source = tag.pk AND fk_target = NEW.fk_target) OR (fk_source = NEW.fk_target AND fk_target = tag.pk))
    UNION
    SELECT NEW.fk_target, pk FROM tag WHERE pk != NEW.fk_target AND EXISTS(SELECT * FROM tag_alias WHERE (fk_source = tag.pk AND fk_target = NEW.fk_source) OR (fk_source = NEW.fk_source AND fk_target = tag.pk))
    ON CONFLICT DO NOTHING;
    RETURN NULL;
END;
$BODY$
language plpgsql;

CREATE TRIGGER after_insert_synchronise_tag_aliases AFTER INSERT ON tag_alias
FOR EACH ROW
EXECUTE PROCEDURE synchronise_tag_aliases();

ALTER TABLE tag_alias ADD CONSTRAINT no_self_reference CHECK (fk_source != fk_target);
ALTER TABLE tag_edge ADD CONSTRAINT no_self_reference CHECK (fk_parent != fk_child);

CREATE OR REPLACE FUNCTION synchronise_tag_edge_with_alias() RETURNS TRIGGER AS
$BODY$
BEGIN
    INSERT INTO tag_edge(fk_parent, fk_child)
    SELECT fk_parent, NEW.fk_target FROM tag_edge WHERE fk_child = NEW.fk_source
    UNION
    SELECT NEW.fk_target, fk_child FROM tag_edge WHERE fk_parent = NEW.fk_source
    UNION
    SELECT fk_parent, NEW.fk_source FROM tag_edge WHERE fk_child = NEW.fk_target
    UNION
    SELECT NEW.fk_source, fk_child FROM tag_edge WHERE fk_parent = NEW.fk_target
    ON CONFLICT DO NOTHING;
    RETURN NULL;
END;
$BODY$
language plpgsql;
