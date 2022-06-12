CREATE TABLE tag_closure_table (
    pk SERIAL PRIMARY KEY,
    fk_parent INTEGER REFERENCES tag(pk) NOT NULL,
    fk_child INTEGER REFERENCES tag(pk) NOT NULL,
    depth INTEGER NOT NULL,
    UNIQUE(fk_parent, fk_child),
    UNIQUE(fk_child, fk_parent),
    UNIQUE(fk_parent, fk_child, depth),
    UNIQUE(fk_child, fk_parent, depth)
);

ALTER TABLE tag DROP COLUMN fk_parent;

-- create bi-directional unique index on tag_alias
CREATE UNIQUE INDEX unique_tag_alias ON tag_alias(greatest(fk_source, fk_target), least(fk_target, fk_source));
