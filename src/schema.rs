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

joinable!(refresh_token -> registered_user (fk_registered_user));

allow_tables_to_appear_in_same_query!(refresh_token, registered_user,);
