ALTER TABLE broker_access DROP CONSTRAINT broker_access_fk_broker_fkey;
ALTER TABLE broker_access ADD CONSTRAINT broker_access_fk_broker_fkey FOREIGN KEY (fk_broker) REFERENCES broker(pk);
ALTER TABLE broker_access DROP CONSTRAINT broker_access_fk_granted_group_fkey;
ALTER TABLE broker_access ADD CONSTRAINT broker_access_fk_granted_group_fkey FOREIGN KEY (fk_granted_group) REFERENCES user_group(pk);

ALTER TABLE post_collection_tag DROP CONSTRAINT post_collection_tag_fk_post_collection_fkey;
ALTER TABLE post_collection_tag ADD CONSTRAINT post_collection_tag_fk_post_collection_fkey FOREIGN KEY (fk_post_collection) REFERENCES post_collection(pk);
ALTER TABLE post_collection_tag DROP CONSTRAINT post_collection_tag_fk_tag_fkey;
ALTER TABLE post_collection_tag ADD CONSTRAINT post_collection_tag_fk_tag_fkey FOREIGN KEY (fk_tag) REFERENCES tag(pk);

ALTER TABLE post_group_access DROP CONSTRAINT post_group_access_fk_granted_group_fkey;
ALTER TABLE post_group_access ADD CONSTRAINT post_group_access_fk_granted_group_fkey FOREIGN KEY (fk_granted_group) REFERENCES user_group(pk);
ALTER TABLE post_group_access DROP CONSTRAINT post_group_access_fk_post_fkey;
ALTER TABLE post_group_access ADD CONSTRAINT post_group_access_fk_post_fkey FOREIGN KEY (fk_post) REFERENCES post(pk);

ALTER TABLE post_tag DROP CONSTRAINT post_tag_fk_post_fkey;
ALTER TABLE post_tag ADD CONSTRAINT post_tag_fk_post_fkey FOREIGN KEY (fk_post) REFERENCES post(pk);
ALTER TABLE post_tag DROP CONSTRAINT post_tag_fk_tag_fkey;
ALTER TABLE post_tag ADD CONSTRAINT post_tag_fk_tag_fkey FOREIGN KEY (fk_tag) REFERENCES tag(pk);

ALTER TABLE tag_alias DROP CONSTRAINT tag_alias_fk_source_fkey;
ALTER TABLE tag_alias ADD CONSTRAINT tag_alias_fk_source_fkey FOREIGN KEY (fk_source) REFERENCES tag(pk);
ALTER TABLE tag_alias DROP CONSTRAINT tag_alias_fk_target_fkey;
ALTER TABLE tag_alias ADD CONSTRAINT tag_alias_fk_target_fkey FOREIGN KEY (fk_target) REFERENCES tag(pk);

ALTER TABLE tag_closure_table DROP CONSTRAINT tag_closure_table_fk_child_fkey;
ALTER TABLE tag_closure_table ADD CONSTRAINT tag_closure_table_fk_child_fkey FOREIGN KEY (fk_child) REFERENCES tag(pk);
ALTER TABLE tag_closure_table DROP CONSTRAINT tag_closure_table_fk_parent_fkey;
ALTER TABLE tag_closure_table ADD CONSTRAINT tag_closure_table_fk_parent_fkey FOREIGN KEY (fk_parent) REFERENCES tag(pk);

ALTER TABLE tag_edge DROP CONSTRAINT tag_edge_fk_child_fkey;
ALTER TABLE tag_edge ADD CONSTRAINT tag_edge_fk_child_fkey FOREIGN KEY (fk_child) REFERENCES tag(pk);
ALTER TABLE tag_edge DROP CONSTRAINT tag_edge_fk_parent_fkey;
ALTER TABLE tag_edge ADD CONSTRAINT tag_edge_fk_parent_fkey FOREIGN KEY (fk_parent) REFERENCES tag(pk);

ALTER TABLE user_group_membership DROP CONSTRAINT user_group_membership_fk_group_fkey;
ALTER TABLE user_group_membership ADD CONSTRAINT user_group_membership_fk_group_fkey FOREIGN KEY (fk_group) REFERENCES user_group(pk);
