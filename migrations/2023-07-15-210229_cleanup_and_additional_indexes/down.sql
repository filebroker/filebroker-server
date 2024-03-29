DROP INDEX tag_name_gin_idx;
DROP INDEX post_tag_fk_tag_idx;
DROP INDEX broker_fk_owner_idx;

DROP INDEX broker_access_fk_granted_group_fk_broker_unique_idx;
DROP INDEX broker_access_fk_broker_idx;
ALTER TABLE broker_access ADD COLUMN public BOOLEAN DEFAULT false;

DROP INDEX hls_stream_master_playlist_idx;
DROP INDEX post_collection_fk_owner_idx;
DROP INDEX post_collection_group_access_fk_granted_group_idx;
DROP INDEX post_collection_item_fk_post_collection_idx;
DROP INDEX post_group_access_fk_granted_group_idx;
DROP INDEX refresh_token_fk_user_idx;
DROP INDEX tag_alias_fk_target_idx;
DROP INDEX tag_edge_fk_child_idx;
DROP INDEX user_group_fk_owner_idx;

DROP INDEX user_group_membership_fk_group_idx;
ALTER TABLE user_group_membership DROP CONSTRAINT user_group_membership_pkey;
ALTER TABLE user_group_membership ADD PRIMARY KEY(fk_group, fk_user);

ALTER TABLE broker ALTER COLUMN is_aws_region DROP DEFAULT;
ALTER TABLE email_confirmation_token ALTER COLUMN invalidated DROP DEFAULT;
ALTER TABLE one_time_password ALTER COLUMN invalidated DROP DEFAULT;
ALTER TABLE post ALTER COLUMN creation_timestamp DROP DEFAULT;
ALTER TABLE post_collection ALTER COLUMN creation_timestamp DROP DEFAULT;
ALTER TABLE post_collection_item ALTER COLUMN creation_timestamp DROP DEFAULT;
ALTER TABLE registered_user ALTER COLUMN creation_timestamp DROP DEFAULT;
ALTER TABLE tag ALTER COLUMN creation_timestamp DROP DEFAULT;
ALTER TABLE user_group ALTER COLUMN creation_timestamp DROP DEFAULT;
ALTER TABLE user_group_membership ALTER COLUMN creation_timestamp DROP DEFAULT;

ALTER TABLE registered_user ALTER COLUMN pk TYPE INT;
ALTER TABLE refresh_token ALTER COLUMN fk_user TYPE INT;
ALTER TABLE post ALTER COLUMN pk TYPE INT;
ALTER TABLE post ALTER COLUMN fk_create_user TYPE INT;
ALTER TABLE post_tag ALTER COLUMN fk_post TYPE INT;
ALTER TABLE post_tag ALTER COLUMN fk_tag TYPE INT;
ALTER TABLE tag ALTER COLUMN pk TYPE INT;
ALTER TABLE tag_alias ALTER COLUMN fk_source TYPE INT;
ALTER TABLE tag_alias ALTER COLUMN fk_target TYPE INT;
ALTER TABLE tag_closure_table ALTER COLUMN pk TYPE INT;
ALTER TABLE tag_closure_table ALTER COLUMN fk_parent TYPE INT;
ALTER TABLE tag_closure_table ALTER COLUMN fk_child TYPE INT;
ALTER TABLE tag_edge ALTER COLUMN fk_parent TYPE INT;
ALTER TABLE tag_edge ALTER COLUMN fk_child TYPE INT;
ALTER TABLE broker ALTER COLUMN pk TYPE INT;
ALTER TABLE broker ALTER COLUMN fk_owner TYPE INT;
ALTER TABLE s3_object ALTER COLUMN fk_broker TYPE INT;
ALTER TABLE s3_object ALTER COLUMN fk_uploader TYPE INT;
ALTER TABLE user_group ALTER COLUMN pk TYPE INT;
ALTER TABLE user_group ALTER COLUMN fk_owner TYPE INT;
ALTER TABLE user_group_membership ALTER COLUMN fk_group TYPE INT;
ALTER TABLE user_group_membership ALTER COLUMN fk_user TYPE INT;
ALTER TABLE user_group_membership ALTER COLUMN fk_granted_by TYPE INT;
ALTER TABLE post_collection ALTER COLUMN pk TYPE INT;
ALTER TABLE post_collection ALTER COLUMN fk_owner TYPE INT;
ALTER TABLE post_collection_item ALTER COLUMN fk_post TYPE INT;
ALTER TABLE post_collection_item ALTER COLUMN fk_post_collection TYPE INT;
ALTER TABLE post_collection_item ALTER COLUMN fk_added_by TYPE INT;
ALTER TABLE post_group_access ALTER COLUMN fk_post TYPE INT;
ALTER TABLE post_group_access ALTER COLUMN fk_granted_group TYPE INT;
ALTER TABLE post_group_access ALTER COLUMN fk_granted_by TYPE INT;
ALTER TABLE post_collection_group_access ALTER COLUMN fk_post_collection TYPE INT;
ALTER TABLE post_collection_group_access ALTER COLUMN fk_granted_group TYPE INT;
ALTER TABLE post_collection_group_access ALTER COLUMN fk_granted_by TYPE INT;
ALTER TABLE broker_access ALTER COLUMN pk TYPE INT;
ALTER TABLE broker_access ALTER COLUMN fk_broker TYPE INT;
ALTER TABLE broker_access ALTER COLUMN fk_granted_group TYPE INT;
ALTER TABLE broker_access ALTER COLUMN fk_granted_by TYPE INT;
ALTER TABLE email_confirmation_token ALTER COLUMN fk_user TYPE INT;
ALTER TABLE one_time_password ALTER COLUMN fk_user TYPE INT;
