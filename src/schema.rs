// @generated automatically by Diesel CLI.

diesel::table! {
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
        hls_enabled -> Bool,
    }
}

diesel::table! {
    broker_access (pk) {
        pk -> Int4,
        fk_broker -> Int4,
        fk_granted_group -> Nullable<Int4>,
        write -> Bool,
        public -> Bool,
        quota -> Nullable<Int8>,
        fk_granted_by -> Int4,
        creation_timestamp -> Timestamptz,
    }
}

diesel::table! {
    hls_stream (stream_playlist) {
        stream_playlist -> Varchar,
        stream_file -> Varchar,
        master_playlist -> Varchar,
        resolution -> Int4,
        x264_preset -> Varchar,
        target_bitrate -> Nullable<Varchar>,
        min_bitrate -> Nullable<Varchar>,
        max_bitrate -> Nullable<Varchar>,
    }
}

diesel::table! {
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
        public -> Bool,
        public_edit -> Bool,
        description -> Nullable<Text>,
    }
}

diesel::table! {
    post_collection (pk) {
        pk -> Int4,
        name -> Varchar,
        fk_owner -> Int4,
        creation_timestamp -> Timestamptz,
        public -> Bool,
    }
}

diesel::table! {
    post_collection_group_access (fk_post_collection, fk_granted_group) {
        fk_post_collection -> Int4,
        fk_granted_group -> Int4,
        write -> Bool,
        fk_granted_by -> Int4,
        creation_timestamp -> Timestamptz,
    }
}

diesel::table! {
    post_collection_item (fk_post, fk_post_collection) {
        fk_post -> Int4,
        fk_post_collection -> Int4,
        fk_added_by -> Int4,
        creation_timestamp -> Timestamptz,
    }
}

diesel::table! {
    post_group_access (fk_post, fk_granted_group) {
        fk_post -> Int4,
        fk_granted_group -> Int4,
        write -> Bool,
        fk_granted_by -> Int4,
        creation_timestamp -> Timestamptz,
    }
}

diesel::table! {
    post_tag (fk_post, fk_tag) {
        fk_post -> Int4,
        fk_tag -> Int4,
    }
}

diesel::table! {
    refresh_token (pk) {
        pk -> Int4,
        uuid -> Uuid,
        expiry -> Timestamptz,
        invalidated -> Bool,
        fk_registered_user -> Int4,
    }
}

diesel::table! {
    registered_user (pk) {
        pk -> Int4,
        user_name -> Varchar,
        password -> Varchar,
        email -> Nullable<Varchar>,
        avatar_url -> Nullable<Varchar>,
        creation_timestamp -> Timestamptz,
    }
}

diesel::table! {
    s3_object (object_key) {
        object_key -> Varchar,
        sha256_hash -> Nullable<Bpchar>,
        size_bytes -> Int8,
        mime_type -> Varchar,
        fk_broker -> Int4,
        fk_uploader -> Int4,
        thumbnail_object_key -> Nullable<Varchar>,
        creation_timestamp -> Timestamptz,
        filename -> Nullable<Varchar>,
        hls_master_playlist -> Nullable<Varchar>,
        hls_disabled -> Bool,
        hls_locked_at -> Nullable<Timestamptz>,
        thumbnail_locked_at -> Nullable<Timestamptz>,
        hls_fail_count -> Nullable<Int4>,
        thumbnail_fail_count -> Nullable<Int4>,
    }
}

diesel::table! {
    tag (pk) {
        pk -> Int4,
        tag_name -> Varchar,
        creation_timestamp -> Timestamptz,
    }
}

diesel::table! {
    tag_alias (fk_source, fk_target) {
        fk_source -> Int4,
        fk_target -> Int4,
    }
}

diesel::table! {
    tag_closure_table (pk) {
        pk -> Int4,
        fk_parent -> Int4,
        fk_child -> Int4,
        depth -> Int4,
    }
}

diesel::table! {
    tag_edge (fk_parent, fk_child) {
        fk_parent -> Int4,
        fk_child -> Int4,
    }
}

diesel::table! {
    user_group (pk) {
        pk -> Int4,
        name -> Varchar,
        public -> Bool,
        hidden -> Bool,
        fk_owner -> Int4,
        creation_timestamp -> Timestamptz,
    }
}

diesel::table! {
    user_group_membership (fk_group, fk_user) {
        fk_group -> Int4,
        fk_user -> Int4,
        administrator -> Bool,
        revoked -> Bool,
        fk_granted_by -> Int4,
        creation_timestamp -> Timestamptz,
    }
}

diesel::joinable!(broker -> registered_user (fk_owner));
diesel::joinable!(broker_access -> broker (fk_broker));
diesel::joinable!(broker_access -> registered_user (fk_granted_by));
diesel::joinable!(broker_access -> user_group (fk_granted_group));
diesel::joinable!(post -> registered_user (fk_create_user));
diesel::joinable!(post -> s3_object (s3_object));
diesel::joinable!(post_collection -> registered_user (fk_owner));
diesel::joinable!(post_collection_group_access -> post_collection (fk_post_collection));
diesel::joinable!(post_collection_group_access -> registered_user (fk_granted_by));
diesel::joinable!(post_collection_group_access -> user_group (fk_granted_group));
diesel::joinable!(post_collection_item -> post (fk_post));
diesel::joinable!(post_collection_item -> post_collection (fk_post_collection));
diesel::joinable!(post_collection_item -> registered_user (fk_added_by));
diesel::joinable!(post_group_access -> post (fk_post));
diesel::joinable!(post_group_access -> registered_user (fk_granted_by));
diesel::joinable!(post_group_access -> user_group (fk_granted_group));
diesel::joinable!(post_tag -> post (fk_post));
diesel::joinable!(post_tag -> tag (fk_tag));
diesel::joinable!(refresh_token -> registered_user (fk_registered_user));
diesel::joinable!(s3_object -> broker (fk_broker));
diesel::joinable!(s3_object -> registered_user (fk_uploader));
diesel::joinable!(user_group -> registered_user (fk_owner));
diesel::joinable!(user_group_membership -> user_group (fk_group));

diesel::allow_tables_to_appear_in_same_query!(
    broker,
    broker_access,
    hls_stream,
    post,
    post_collection,
    post_collection_group_access,
    post_collection_item,
    post_group_access,
    post_tag,
    refresh_token,
    registered_user,
    s3_object,
    tag,
    tag_alias,
    tag_closure_table,
    tag_edge,
    user_group,
    user_group_membership,
);
