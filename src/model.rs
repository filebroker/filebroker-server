use chrono::{offset::Utc, DateTime};
use diesel::{Associations, Identifiable, Insertable, Queryable};
use serde::Serialize;

use crate::schema::{refresh_token, registered_user};

#[derive(Associations, Identifiable, Queryable, Serialize)]
#[table_name = "registered_user"]
#[primary_key(pk)]
pub struct User {
    pub pk: i32,
    pub user_name: String,
    pub password: String,
    pub email: Option<String>,
    pub avatar_url: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Insertable)]
#[table_name = "registered_user"]
pub struct NewUser {
    pub user_name: String,
    pub password: String,
    pub email: Option<String>,
    pub avatar_url: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Associations, Identifiable, Queryable)]
#[belongs_to(User, foreign_key = "fk_registered_user")]
#[table_name = "refresh_token"]
#[primary_key(pk)]
pub struct RefreshToken {
    pub pk: i32,
    pub uuid: uuid::Uuid,
    pub expiry: DateTime<Utc>,
    pub invalidated: bool,
    pub fk_registered_user: i32,
}

#[derive(Insertable)]
#[table_name = "refresh_token"]
pub struct NewRefreshToken {
    pub uuid: uuid::Uuid,
    pub expiry: DateTime<Utc>,
    pub invalidated: bool,
    pub fk_registered_user: i32,
}
