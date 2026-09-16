CREATE INDEX post_group_access_covering_acl_idx
ON post_group_access (fk_granted_group, fk_post)
INCLUDE (write);

CREATE INDEX post_collection_group_access_covering_acl_idx
ON post_collection_group_access (fk_granted_group, fk_post_collection)
INCLUDE (write);

CREATE INDEX s3_object_fk_broker_covering_idx
ON s3_object (fk_broker)
INCLUDE (object_key);

CREATE INDEX user_group_membership_active_user_idx
ON user_group_membership (fk_user, fk_group)
WHERE NOT revoked;

CREATE INDEX user_group_membership_active_admin_user_idx
ON user_group_membership (fk_user, fk_group)
WHERE NOT revoked AND administrator;

CREATE INDEX post_public_pk_idx
ON post (pk)
WHERE public;

CREATE INDEX post_collection_public_pk_idx
ON post_collection (pk)
WHERE public;

CREATE INDEX post_public_edit_pk_idx
ON post (pk)
WHERE public AND public_edit;

CREATE INDEX post_collection_public_edit_pk_idx
ON post_collection (pk)
WHERE public AND public_edit;
