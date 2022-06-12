DROP TABLE IF EXISTS tag_closure_table;

ALTER TABLE tag ADD COLUMN fk_parent INTEGER REFERENCES tag(pk) NULL;
DROP INDEX unique_tag_alias;
