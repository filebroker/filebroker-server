table! {
    broker (pk) {
        pk -> Int4,
        name -> Varchar,
        bucket -> Varchar,
        endpoint -> Varchar,
        access_key -> Varchar,
        secret_key -> Varchar,
        is_aws_region -> Bool,
        remove_duplicate_files -> Bool,
        fk_owner -> Int4,
        creation_timestamp -> Timestamptz,
    }
}

table! {
    object_upload (object_key) {
        object_key -> Varchar,
        status -> Varchar,
        bytes_written -> Int8,
        total_bytes -> Int8,
        current_rate -> Nullable<Int8>,
        estimated_millis_remaining -> Nullable<Int8>,
        completed_object -> Nullable<Varchar>,
        mime_type -> Varchar,
        fk_broker -> Int4,
        fk_uploader -> Int4,
    }
}

table! {
    object_upload_status (unique_id) {
        unique_id -> Varchar,
    }
}

table! {
    permission_target (pk) {
        pk -> Int4,
        public -> Bool,
        administrator -> Bool,
        fk_granted_group -> Nullable<Int4>,
        quota -> Nullable<Int8>,
        fk_post -> Nullable<Int4>,
        fk_broker -> Nullable<Int4>,
        fk_post_collection -> Nullable<Int4>,
        fk_granted_by -> Nullable<Int4>,
        creation_timestamp -> Timestamptz,
    }
}

table! {
    post (pk) {
        pk -> Int4,
        data_url -> Nullable<Varchar>,
        source_url -> Nullable<Varchar>,
        title -> Nullable<Varchar>,
        creation_timestamp -> Timestamptz,
        fk_create_user -> Int4,
        score -> Int4,
        s3_object -> Nullable<Varchar>,
        thumbnail_url -> Nullable<Varchar>,
    }
}

table! {
    post_collection (pk) {
        pk -> Int4,
        name -> Varchar,
        fk_owner -> Int4,
        creation_timestamp -> Timestamptz,
    }
}

table! {
    post_collection_item (fk_post, fk_post_collection) {
        fk_post -> Int4,
        fk_post_collection -> Int4,
        fk_added_by -> Int4,
        creation_timestamp -> Timestamptz,
    }
}

table! {
    post_tag (fk_post, fk_tag) {
        fk_post -> Int4,
        fk_tag -> Int4,
    }
}

table! {
    refresh_token (pk) {
        pk -> Int4,
        uuid -> Uuid,
        expiry -> Timestamptz,
        invalidated -> Bool,
        fk_registered_user -> Int4,
    }
}

table! {
    registered_user (pk) {
        pk -> Int4,
        user_name -> Varchar,
        password -> Varchar,
        email -> Nullable<Varchar>,
        avatar_url -> Nullable<Varchar>,
        creation_timestamp -> Timestamptz,
    }
}

table! {
    s3_object (object_key) {
        object_key -> Varchar,
        sha256_hash -> Nullable<Bpchar>,
        size_bytes -> Int8,
        mime_type -> Varchar,
        fk_broker -> Int4,
        fk_uploader -> Int4,
        thumbnail_object_key -> Nullable<Varchar>,
        creation_timestamp -> Timestamptz,
    }
}

table! {
    tag (pk) {
        pk -> Int4,
        tag_name -> Varchar,
        creation_timestamp -> Timestamptz,
    }
}

table! {
    tag_alias (fk_source, fk_target) {
        fk_source -> Int4,
        fk_target -> Int4,
    }
}

table! {
    tag_closure_table (pk) {
        pk -> Int4,
        fk_parent -> Int4,
        fk_child -> Int4,
        depth -> Int4,
    }
}

table! {
    user_group (pk) {
        pk -> Int4,
        name -> Varchar,
        public -> Bool,
        hidden -> Bool,
        fk_owner -> Int4,
        creation_timestamp -> Timestamptz,
    }
}

table! {
    user_group_membership (fk_group, fk_user) {
        fk_group -> Int4,
        fk_user -> Int4,
        administrator -> Bool,
        revoked -> Bool,
        fk_granted_by -> Int4,
        creation_timestamp -> Timestamptz,
    }
}

joinable!(broker -> registered_user (fk_owner));
joinable!(object_upload -> broker (fk_broker));
joinable!(object_upload -> object_upload_status (status));
joinable!(object_upload -> registered_user (fk_uploader));
joinable!(object_upload -> s3_object (completed_object));
joinable!(permission_target -> broker (fk_broker));
joinable!(permission_target -> post (fk_post));
joinable!(permission_target -> post_collection (fk_post_collection));
joinable!(permission_target -> registered_user (fk_granted_by));
joinable!(permission_target -> user_group (fk_granted_group));
joinable!(post -> registered_user (fk_create_user));
joinable!(post -> s3_object (s3_object));
joinable!(post_collection -> registered_user (fk_owner));
joinable!(post_collection_item -> post (fk_post));
joinable!(post_collection_item -> post_collection (fk_post_collection));
joinable!(post_collection_item -> registered_user (fk_added_by));
joinable!(post_tag -> post (fk_post));
joinable!(post_tag -> tag (fk_tag));
joinable!(refresh_token -> registered_user (fk_registered_user));
joinable!(s3_object -> broker (fk_broker));
joinable!(s3_object -> registered_user (fk_uploader));
joinable!(user_group -> registered_user (fk_owner));
joinable!(user_group_membership -> user_group (fk_group));

allow_tables_to_appear_in_same_query!(
    broker,
    object_upload,
    object_upload_status,
    permission_target,
    post,
    post_collection,
    post_collection_item,
    post_tag,
    refresh_token,
    registered_user,
    s3_object,
    tag,
    tag_alias,
    tag_closure_table,
    user_group,
    user_group_membership,
);
