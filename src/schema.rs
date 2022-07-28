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
    }
}

table! {
    post (pk) {
        pk -> Int4,
        data_url -> Varchar,
        source_url -> Nullable<Varchar>,
        title -> Nullable<Varchar>,
        creation_timestamp -> Timestamptz,
        fk_create_user -> Int4,
        score -> Int4,
        fk_s3_object -> Nullable<Int4>,
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
    s3_object (pk) {
        pk -> Int4,
        object_key -> Varchar,
        sha256_hash -> Nullable<Bpchar>,
        size_bytes -> Int8,
        mime_type -> Varchar,
        fk_broker -> Int4,
        fk_uploader -> Int4,
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

joinable!(broker -> registered_user (fk_owner));
joinable!(post -> registered_user (fk_create_user));
joinable!(post -> s3_object (fk_s3_object));
joinable!(post_tag -> post (fk_post));
joinable!(post_tag -> tag (fk_tag));
joinable!(refresh_token -> registered_user (fk_registered_user));
joinable!(s3_object -> broker (fk_broker));
joinable!(s3_object -> registered_user (fk_uploader));

allow_tables_to_appear_in_same_query!(
    broker,
    post,
    post_tag,
    refresh_token,
    registered_user,
    s3_object,
    tag,
    tag_alias,
    tag_closure_table,
);
