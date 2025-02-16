// @generated automatically by Diesel CLI.

diesel::table! {
    broker (pk) {
        pk -> Int8,
        #[max_length = 255]
        name -> Varchar,
        #[max_length = 255]
        bucket -> Varchar,
        #[max_length = 2048]
        endpoint -> Varchar,
        #[max_length = 255]
        access_key -> Varchar,
        #[max_length = 255]
        secret_key -> Varchar,
        is_aws_region -> Bool,
        remove_duplicate_files -> Bool,
        fk_owner -> Int8,
        creation_timestamp -> Timestamptz,
        hls_enabled -> Bool,
    }
}

diesel::table! {
    broker_access (pk) {
        pk -> Int8,
        fk_broker -> Int8,
        fk_granted_group -> Nullable<Int8>,
        write -> Bool,
        quota -> Nullable<Int8>,
        fk_granted_by -> Int8,
        creation_timestamp -> Timestamptz,
    }
}

diesel::table! {
    deferred_s3_object_deletion (object_key) {
        #[max_length = 255]
        object_key -> Varchar,
        locked_at -> Nullable<Timestamptz>,
        fail_count -> Nullable<Int4>,
        fk_broker -> Int8,
    }
}

diesel::table! {
    email_confirmation_token (uuid) {
        uuid -> Uuid,
        expiry -> Timestamptz,
        invalidated -> Bool,
        fk_user -> Int8,
    }
}

diesel::table! {
    hls_stream (stream_playlist) {
        #[max_length = 255]
        stream_playlist -> Varchar,
        #[max_length = 255]
        stream_file -> Varchar,
        #[max_length = 255]
        master_playlist -> Varchar,
        resolution -> Int4,
        #[max_length = 255]
        x264_preset -> Varchar,
        #[max_length = 255]
        target_bitrate -> Nullable<Varchar>,
        #[max_length = 255]
        min_bitrate -> Nullable<Varchar>,
        #[max_length = 255]
        max_bitrate -> Nullable<Varchar>,
    }
}

diesel::table! {
    one_time_password (fk_user) {
        #[max_length = 255]
        password -> Varchar,
        expiry -> Timestamptz,
        invalidated -> Bool,
        fk_user -> Int8,
    }
}

diesel::table! {
    post (pk) {
        pk -> Int8,
        #[max_length = 2048]
        data_url -> Nullable<Varchar>,
        #[max_length = 2048]
        source_url -> Nullable<Varchar>,
        #[max_length = 300]
        title -> Nullable<Varchar>,
        creation_timestamp -> Timestamptz,
        fk_create_user -> Int8,
        score -> Int4,
        #[max_length = 255]
        s3_object -> Varchar,
        #[max_length = 2048]
        thumbnail_url -> Nullable<Varchar>,
        public -> Bool,
        public_edit -> Bool,
        description -> Nullable<Text>,
        edit_timestamp -> Timestamptz,
        fk_edit_user -> Int8,
    }
}

diesel::table! {
    post_collection (pk) {
        pk -> Int8,
        #[max_length = 255]
        title -> Varchar,
        fk_create_user -> Int8,
        creation_timestamp -> Timestamptz,
        public -> Bool,
        public_edit -> Bool,
        #[max_length = 255]
        poster_object_key -> Nullable<Varchar>,
        description -> Nullable<Text>,
        edit_timestamp -> Timestamptz,
        fk_edit_user -> Int8,
    }
}

diesel::table! {
    post_collection_edit_history (pk) {
        pk -> Int8,
        fk_post_collection -> Int8,
        fk_edit_user -> Int8,
        edit_timestamp -> Timestamptz,
        #[max_length = 300]
        title -> Varchar,
        title_changed -> Bool,
        public -> Bool,
        public_changed -> Bool,
        public_edit -> Bool,
        public_edit_changed -> Bool,
        description -> Nullable<Text>,
        description_changed -> Bool,
        #[max_length = 255]
        poster_object_key -> Nullable<Varchar>,
        poster_object_key_changed -> Bool,
        tags_changed -> Bool,
        group_access_changed -> Bool,
    }
}

diesel::table! {
    post_collection_edit_history_group_access (fk_post_collection_edit_history, fk_granted_group) {
        fk_post_collection_edit_history -> Int8,
        fk_granted_group -> Int8,
        write -> Bool,
        fk_granted_by -> Int8,
        creation_timestamp -> Timestamptz,
    }
}

diesel::table! {
    post_collection_edit_history_tag (fk_post_collection_edit_history, fk_tag) {
        fk_post_collection_edit_history -> Int8,
        fk_tag -> Int8,
    }
}

diesel::table! {
    post_collection_group_access (fk_post_collection, fk_granted_group) {
        fk_post_collection -> Int8,
        fk_granted_group -> Int8,
        write -> Bool,
        fk_granted_by -> Int8,
        creation_timestamp -> Timestamptz,
    }
}

diesel::table! {
    post_collection_item (pk) {
        fk_post -> Int8,
        fk_post_collection -> Int8,
        fk_added_by -> Int8,
        creation_timestamp -> Timestamptz,
        pk -> Int8,
        ordinal -> Int4,
    }
}

diesel::table! {
    post_collection_tag (fk_post_collection, fk_tag) {
        fk_post_collection -> Int8,
        fk_tag -> Int8,
    }
}

diesel::table! {
    post_edit_history (pk) {
        pk -> Int8,
        fk_post -> Int8,
        fk_edit_user -> Int8,
        edit_timestamp -> Timestamptz,
        #[max_length = 2048]
        data_url -> Nullable<Varchar>,
        data_url_changed -> Bool,
        #[max_length = 2048]
        source_url -> Nullable<Varchar>,
        source_url_changed -> Bool,
        #[max_length = 300]
        title -> Nullable<Varchar>,
        title_changed -> Bool,
        public -> Bool,
        public_changed -> Bool,
        public_edit -> Bool,
        public_edit_changed -> Bool,
        description -> Nullable<Text>,
        description_changed -> Bool,
        tags_changed -> Bool,
        group_access_changed -> Bool,
    }
}

diesel::table! {
    post_edit_history_group_access (fk_post_edit_history, fk_granted_group) {
        fk_post_edit_history -> Int8,
        fk_granted_group -> Int8,
        write -> Bool,
        fk_granted_by -> Int8,
        creation_timestamp -> Timestamptz,
    }
}

diesel::table! {
    post_edit_history_tag (fk_post_edit_history, fk_tag) {
        fk_post_edit_history -> Int8,
        fk_tag -> Int8,
    }
}

diesel::table! {
    post_group_access (fk_post, fk_granted_group) {
        fk_post -> Int8,
        fk_granted_group -> Int8,
        write -> Bool,
        fk_granted_by -> Int8,
        creation_timestamp -> Timestamptz,
    }
}

diesel::table! {
    post_tag (fk_post, fk_tag) {
        fk_post -> Int8,
        fk_tag -> Int8,
    }
}

diesel::table! {
    refresh_token (uuid) {
        uuid -> Uuid,
        expiry -> Timestamptz,
        invalidated -> Bool,
        fk_user -> Int8,
    }
}

diesel::table! {
    registered_user (pk) {
        pk -> Int8,
        #[max_length = 25]
        user_name -> Varchar,
        #[max_length = 255]
        password -> Varchar,
        #[max_length = 320]
        email -> Nullable<Varchar>,
        #[max_length = 2048]
        avatar_url -> Nullable<Varchar>,
        creation_timestamp -> Timestamptz,
        email_confirmed -> Bool,
        #[max_length = 32]
        display_name -> Nullable<Varchar>,
        jwt_version -> Int4,
        password_fail_count -> Int4,
    }
}

diesel::table! {
    s3_object (object_key) {
        #[max_length = 255]
        object_key -> Varchar,
        #[max_length = 64]
        sha256_hash -> Nullable<Bpchar>,
        size_bytes -> Int8,
        #[max_length = 255]
        mime_type -> Varchar,
        fk_broker -> Int8,
        fk_uploader -> Int8,
        #[max_length = 255]
        thumbnail_object_key -> Nullable<Varchar>,
        creation_timestamp -> Timestamptz,
        #[max_length = 255]
        filename -> Nullable<Varchar>,
        #[max_length = 255]
        hls_master_playlist -> Nullable<Varchar>,
        hls_disabled -> Bool,
        hls_locked_at -> Nullable<Timestamptz>,
        thumbnail_locked_at -> Nullable<Timestamptz>,
        hls_fail_count -> Nullable<Int4>,
        thumbnail_fail_count -> Nullable<Int4>,
        thumbnail_disabled -> Bool,
        metadata_locked_at -> Nullable<Timestamptz>,
        metadata_fail_count -> Nullable<Int4>,
    }
}

diesel::table! {
    s3_object_metadata (object_key) {
        #[max_length = 255]
        object_key -> Varchar,
        file_type -> Nullable<Text>,
        file_type_extension -> Nullable<Text>,
        mime_type -> Nullable<Text>,
        title -> Nullable<Text>,
        artist -> Nullable<Text>,
        album -> Nullable<Text>,
        album_artist -> Nullable<Text>,
        composer -> Nullable<Text>,
        genre -> Nullable<Text>,
        date -> Nullable<Timestamptz>,
        track_number -> Nullable<Int4>,
        disc_number -> Nullable<Int4>,
        duration -> Nullable<Interval>,
        width -> Nullable<Int4>,
        height -> Nullable<Int4>,
        size -> Nullable<Int8>,
        bit_rate -> Nullable<Int8>,
        format_name -> Nullable<Text>,
        format_long_name -> Nullable<Text>,
        video_stream_count -> Int4,
        video_codec_name -> Nullable<Text>,
        video_codec_long_name -> Nullable<Text>,
        video_frame_rate -> Nullable<Float8>,
        video_bit_rate_max -> Nullable<Int8>,
        audio_stream_count -> Int4,
        audio_codec_name -> Nullable<Text>,
        audio_codec_long_name -> Nullable<Text>,
        audio_sample_rate -> Nullable<Float8>,
        audio_channels -> Nullable<Int4>,
        audio_bit_rate_max -> Nullable<Int8>,
        raw -> Jsonb,
        loaded -> Bool,
        track_count -> Nullable<Int4>,
        disc_count -> Nullable<Int4>,
    }
}

diesel::table! {
    tag (pk) {
        pk -> Int8,
        #[max_length = 50]
        tag_name -> Varchar,
        creation_timestamp -> Timestamptz,
    }
}

diesel::table! {
    tag_alias (fk_source, fk_target) {
        fk_source -> Int8,
        fk_target -> Int8,
    }
}

diesel::table! {
    tag_closure_table (pk) {
        pk -> Int8,
        fk_parent -> Int8,
        fk_child -> Int8,
        depth -> Int4,
    }
}

diesel::table! {
    tag_edge (fk_parent, fk_child) {
        fk_parent -> Int8,
        fk_child -> Int8,
    }
}

diesel::table! {
    user_group (pk) {
        pk -> Int8,
        #[max_length = 255]
        name -> Varchar,
        public -> Bool,
        hidden -> Bool,
        fk_owner -> Int8,
        creation_timestamp -> Timestamptz,
    }
}

diesel::table! {
    user_group_membership (fk_user, fk_group) {
        fk_group -> Int8,
        fk_user -> Int8,
        administrator -> Bool,
        revoked -> Bool,
        fk_granted_by -> Int8,
        creation_timestamp -> Timestamptz,
    }
}

diesel::joinable!(broker -> registered_user (fk_owner));
diesel::joinable!(broker_access -> broker (fk_broker));
diesel::joinable!(broker_access -> registered_user (fk_granted_by));
diesel::joinable!(broker_access -> user_group (fk_granted_group));
diesel::joinable!(deferred_s3_object_deletion -> broker (fk_broker));
diesel::joinable!(email_confirmation_token -> registered_user (fk_user));
diesel::joinable!(one_time_password -> registered_user (fk_user));
diesel::joinable!(post -> s3_object (s3_object));
diesel::joinable!(post_collection -> s3_object (poster_object_key));
diesel::joinable!(post_collection_edit_history -> post_collection (fk_post_collection));
diesel::joinable!(post_collection_edit_history -> registered_user (fk_edit_user));
diesel::joinable!(post_collection_edit_history_group_access -> post_collection_edit_history (fk_post_collection_edit_history));
diesel::joinable!(post_collection_edit_history_group_access -> registered_user (fk_granted_by));
diesel::joinable!(post_collection_edit_history_group_access -> user_group (fk_granted_group));
diesel::joinable!(post_collection_edit_history_tag -> post_collection_edit_history (fk_post_collection_edit_history));
diesel::joinable!(post_collection_edit_history_tag -> tag (fk_tag));
diesel::joinable!(post_collection_group_access -> post_collection (fk_post_collection));
diesel::joinable!(post_collection_group_access -> registered_user (fk_granted_by));
diesel::joinable!(post_collection_group_access -> user_group (fk_granted_group));
diesel::joinable!(post_collection_item -> post (fk_post));
diesel::joinable!(post_collection_item -> post_collection (fk_post_collection));
diesel::joinable!(post_collection_item -> registered_user (fk_added_by));
diesel::joinable!(post_collection_tag -> post_collection (fk_post_collection));
diesel::joinable!(post_collection_tag -> tag (fk_tag));
diesel::joinable!(post_edit_history -> post (fk_post));
diesel::joinable!(post_edit_history -> registered_user (fk_edit_user));
diesel::joinable!(post_edit_history_group_access -> post_edit_history (fk_post_edit_history));
diesel::joinable!(post_edit_history_group_access -> registered_user (fk_granted_by));
diesel::joinable!(post_edit_history_group_access -> user_group (fk_granted_group));
diesel::joinable!(post_edit_history_tag -> post_edit_history (fk_post_edit_history));
diesel::joinable!(post_edit_history_tag -> tag (fk_tag));
diesel::joinable!(post_group_access -> post (fk_post));
diesel::joinable!(post_group_access -> registered_user (fk_granted_by));
diesel::joinable!(post_group_access -> user_group (fk_granted_group));
diesel::joinable!(post_tag -> post (fk_post));
diesel::joinable!(post_tag -> tag (fk_tag));
diesel::joinable!(refresh_token -> registered_user (fk_user));
diesel::joinable!(s3_object -> broker (fk_broker));
diesel::joinable!(s3_object -> registered_user (fk_uploader));
diesel::joinable!(s3_object_metadata -> s3_object (object_key));
diesel::joinable!(user_group -> registered_user (fk_owner));
diesel::joinable!(user_group_membership -> user_group (fk_group));

diesel::allow_tables_to_appear_in_same_query!(
    broker,
    broker_access,
    deferred_s3_object_deletion,
    email_confirmation_token,
    hls_stream,
    one_time_password,
    post,
    post_collection,
    post_collection_edit_history,
    post_collection_edit_history_group_access,
    post_collection_edit_history_tag,
    post_collection_group_access,
    post_collection_item,
    post_collection_tag,
    post_edit_history,
    post_edit_history_group_access,
    post_edit_history_tag,
    post_group_access,
    post_tag,
    refresh_token,
    registered_user,
    s3_object,
    s3_object_metadata,
    tag,
    tag_alias,
    tag_closure_table,
    tag_edge,
    user_group,
    user_group_membership,
);
