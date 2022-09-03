INSERT INTO tag_closure_table(fk_parent, fk_child, depth) SELECT pk, pk, 0 FROM tag ON CONFLICT DO NOTHING;
