CREATE INDEX tag_name_gin_idx ON tag USING gin(LOWER(tag_name) gin_trgm_ops);
CREATE INDEX post_tag_fk_tag_idx ON post_tag(fk_tag);
CREATE INDEX broker_fk_owner_idx ON broker(fk_owner);

ALTER TABLE broker_access DROP COLUMN public;
CREATE UNIQUE INDEX broker_access_fk_granted_group_fk_broker_unique_idx ON broker_access(fk_granted_group, fk_broker) NULLS NOT DISTINCT;
CREATE INDEX broker_access_fk_broker_idx ON broker_access(fk_broker);

CREATE INDEX hls_stream_master_playlist_idx ON hls_stream(master_playlist);
CREATE INDEX post_collection_fk_owner_idx ON post_collection(fk_owner);
CREATE INDEX post_collection_group_access_fk_granted_group_idx ON post_collection_group_access(fk_granted_group);
CREATE INDEX post_collection_item_fk_post_collection_idx ON post_collection_item(fk_post_collection);
CREATE INDEX post_group_access_fk_granted_group_idx ON post_group_access(fk_granted_group);
CREATE INDEX refresh_token_fk_user_idx ON refresh_token(fk_user);
CREATE INDEX tag_alias_fk_target_idx ON tag_alias(fk_target);
CREATE INDEX tag_edge_fk_child_idx ON tag_edge(fk_child);
CREATE INDEX user_group_fk_owner_idx ON user_group(fk_owner);

ALTER TABLE user_group_membership DROP CONSTRAINT user_group_membership_pkey;
ALTER TABLE user_group_membership ADD PRIMARY KEY(fk_user, fk_group);
CREATE INDEX user_group_membership_fk_group_idx ON user_group_membership(fk_group);

ALTER TABLE broker ALTER COLUMN is_aws_region SET DEFAULT false;
ALTER TABLE email_confirmation_token ALTER COLUMN invalidated SET DEFAULT false;
ALTER TABLE one_time_password ALTER COLUMN invalidated SET DEFAULT false;
ALTER TABLE post ALTER COLUMN creation_timestamp SET DEFAULT now();
ALTER TABLE post_collection ALTER COLUMN creation_timestamp SET DEFAULT now();
ALTER TABLE post_collection_item ALTER COLUMN creation_timestamp SET DEFAULT now();
ALTER TABLE registered_user ALTER COLUMN creation_timestamp SET DEFAULT now();
ALTER TABLE tag ALTER COLUMN creation_timestamp SET DEFAULT now();
ALTER TABLE user_group ALTER COLUMN creation_timestamp SET DEFAULT now();
ALTER TABLE user_group_membership ALTER COLUMN creation_timestamp SET DEFAULT now();

ALTER TABLE registered_user ALTER COLUMN pk TYPE BIGINT;
ALTER TABLE refresh_token ALTER COLUMN fk_user TYPE BIGINT;
ALTER TABLE post ALTER COLUMN pk TYPE BIGINT;
ALTER TABLE post ALTER COLUMN fk_create_user TYPE BIGINT;
ALTER TABLE post_tag ALTER COLUMN fk_post TYPE BIGINT;
ALTER TABLE post_tag ALTER COLUMN fk_tag TYPE BIGINT;
ALTER TABLE tag ALTER COLUMN pk TYPE BIGINT;
ALTER TABLE tag_alias ALTER COLUMN fk_source TYPE BIGINT;
ALTER TABLE tag_alias ALTER COLUMN fk_target TYPE BIGINT;
ALTER TABLE tag_closure_table ALTER COLUMN pk TYPE BIGINT;
ALTER TABLE tag_closure_table ALTER COLUMN fk_parent TYPE BIGINT;
ALTER TABLE tag_closure_table ALTER COLUMN fk_child TYPE BIGINT;
ALTER TABLE tag_edge ALTER COLUMN fk_parent TYPE BIGINT;
ALTER TABLE tag_edge ALTER COLUMN fk_child TYPE BIGINT;
ALTER TABLE broker ALTER COLUMN pk TYPE BIGINT;
ALTER TABLE broker ALTER COLUMN fk_owner TYPE BIGINT;
ALTER TABLE s3_object ALTER COLUMN fk_broker TYPE BIGINT;
ALTER TABLE s3_object ALTER COLUMN fk_uploader TYPE BIGINT;
ALTER TABLE user_group ALTER COLUMN pk TYPE BIGINT;
ALTER TABLE user_group ALTER COLUMN fk_owner TYPE BIGINT;
ALTER TABLE user_group_membership ALTER COLUMN fk_group TYPE BIGINT;
ALTER TABLE user_group_membership ALTER COLUMN fk_user TYPE BIGINT;
ALTER TABLE user_group_membership ALTER COLUMN fk_granted_by TYPE BIGINT;
ALTER TABLE post_collection ALTER COLUMN pk TYPE BIGINT;
ALTER TABLE post_collection ALTER COLUMN fk_owner TYPE BIGINT;
ALTER TABLE post_collection_item ALTER COLUMN fk_post TYPE BIGINT;
ALTER TABLE post_collection_item ALTER COLUMN fk_post_collection TYPE BIGINT;
ALTER TABLE post_collection_item ALTER COLUMN fk_added_by TYPE BIGINT;
ALTER TABLE post_group_access ALTER COLUMN fk_post TYPE BIGINT;
ALTER TABLE post_group_access ALTER COLUMN fk_granted_group TYPE BIGINT;
ALTER TABLE post_group_access ALTER COLUMN fk_granted_by TYPE BIGINT;
ALTER TABLE post_collection_group_access ALTER COLUMN fk_post_collection TYPE BIGINT;
ALTER TABLE post_collection_group_access ALTER COLUMN fk_granted_group TYPE BIGINT;
ALTER TABLE post_collection_group_access ALTER COLUMN fk_granted_by TYPE BIGINT;
ALTER TABLE broker_access ALTER COLUMN pk TYPE BIGINT;
ALTER TABLE broker_access ALTER COLUMN fk_broker TYPE BIGINT;
ALTER TABLE broker_access ALTER COLUMN fk_granted_group TYPE BIGINT;
ALTER TABLE broker_access ALTER COLUMN fk_granted_by TYPE BIGINT;
ALTER TABLE email_confirmation_token ALTER COLUMN fk_user TYPE BIGINT;
ALTER TABLE one_time_password ALTER COLUMN fk_user TYPE BIGINT;
