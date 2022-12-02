use bcrypt::{hash, verify, DEFAULT_COST};
use chrono::{offset::Utc, DateTime, Duration};
use diesel::{dsl::count, expression_methods::BoolExpressionMethods, Connection};
use exec_rs::sync::MutexSync;
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator::Validate;
use warp::{
    filters::header::headers_cloned,
    http::{
        header::{self, HeaderMap},
        Response, StatusCode,
    },
    hyper, Filter, Rejection, Reply,
};

use crate::{
    acquire_db_connection,
    diesel::{ExpressionMethods, OptionalExtension, QueryDsl, RunQueryDsl},
    error::Error,
    model::{NewRefreshToken, NewUser, RefreshToken, User},
    schema::{refresh_token, registered_user},
    DbConnection,
};

lazy_static! {
    pub static ref ACCESS_TOKEN_EXPIRATION: Duration = Duration::hours(3);
    pub static ref REFRESH_TOKEN_EXPIRATION: Duration = Duration::weeks(1);
}

/// Struct received by the /login request.
#[derive(Deserialize)]
pub struct LoginRequest {
    pub user_name: String,
    pub password: String,
}

/// Struct returned by the /login and /refresh-login endpoints.
#[derive(Serialize)]
pub struct LoginResponse {
    pub token: String,
    pub refresh_token: String,
    pub expiration_secs: i64,
    pub user: UserInfo,
}

/// Struct received by the /register endpoint used to create a user.
#[derive(Deserialize, Validate)]
pub struct UserRegistration {
    #[validate(length(min = 1, max = 50))]
    pub user_name: String,
    #[validate(length(min = 1, max = 255))]
    pub password: String,
    #[validate(email)]
    pub email: Option<String>,
    #[validate(url)]
    pub avatar_url: Option<String>,
}
/// Struct encoded in the JWT that contains its expiry and subject user.
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    exp: usize,
    sub: String,
}

/// Warp filter for requests that optionally receive the logged in user from the auth header.
pub fn with_user_optional(
) -> impl warp::Filter<Extract = (Option<User>,), Error = Rejection> + Clone {
    headers_cloned().and_then(get_user_from_auth_header)
}

/// Warp filter for requests that require a logged in user provided by the auth header.
pub fn with_user() -> impl warp::Filter<Extract = (User,), Error = Rejection> + Clone {
    headers_cloned().and_then(require_user_from_auth_header)
}

async fn require_user_from_auth_header(header_map: HeaderMap) -> Result<User, Rejection> {
    match get_user_from_auth_header(header_map).await {
        Ok(Some(user)) => Ok(user),
        Ok(None) => Err(warp::reject::custom(Error::MissingAuthHeaderError)),
        Err(e) => Err(e),
    }
}

/// Decodes the user name provided by the JWT if provided and finds the matching User.
///
/// Failure to find the User would return a QueryError causing a 500 response as the username
/// should always refer to an existing User.
async fn get_user_from_auth_header(header_map: HeaderMap) -> Result<Option<User>, Rejection> {
    const JWT_BEARER_PREFIX: &str = "Bearer ";
    let auth_header = match header_map.get(header::AUTHORIZATION) {
        Some(h) => match std::str::from_utf8(h.as_bytes()) {
            Ok(v) => v,
            Err(_) => return Err(warp::reject::custom(Error::UtfEncodingError)),
        },
        None => return Ok(None),
    };

    if !auth_header.starts_with(JWT_BEARER_PREFIX) {
        return Err(warp::reject::custom(Error::InvalidAuthHeaderError));
    }

    let jwt_token = auth_header.trim_start_matches(JWT_BEARER_PREFIX);
    let token_data = decode::<Claims>(
        jwt_token,
        &DecodingKey::from_secret(&crate::JWT_SECRET.to_be_bytes()),
        &Validation::new(Algorithm::HS512),
    )
    .map_err(|_| warp::reject::custom(Error::InvalidJwtError))?;
    let claims = &token_data.claims;

    let mut connection = acquire_db_connection()?;
    match registered_user::table
        .filter(registered_user::user_name.eq(&claims.sub))
        .first::<User>(&mut connection)
    {
        Ok(registered_user) => Ok(Some(registered_user)),
        Err(e) => Err(warp::reject::custom(Error::QueryError(e.to_string()))),
    }
}

/// Handler for the /login endpoint that receives a json deserialized to the [`LoginRequest`] struct
/// and returns a [`LoginResponse`] if the credentials are correct or a InvalidCredentialsError, which
/// results in a 403, if the credentials are not correct.
pub async fn login_handler(request: LoginRequest) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection()?;
    connection
        .transaction(|connection| {
            let found_registered_user = registered_user::table
                .filter(registered_user::user_name.eq(&request.user_name))
                .first::<User>(connection);
            let registered_user = match found_registered_user {
                Ok(registered_user) => {
                    let hashed_password = &registered_user.password;
                    match verify(&request.password, hashed_password) {
                        Ok(valid) => {
                            if valid {
                                registered_user
                            } else {
                                return Err(Error::InvalidCredentialsError);
                            }
                        }
                        Err(_) => return Err(Error::EncryptionError),
                    }
                }
                Err(diesel::NotFound) => return Err(Error::InvalidCredentialsError),
                Err(e) => return Err(Error::QueryError(e.to_string())),
            };

            let refresh_token_cookie = create_refresh_token_cookie(&registered_user, connection)?;
            create_login_response(registered_user, refresh_token_cookie)
        })
        .map_err(warp::reject::custom)
}

struct RefreshTokenCookie {
    token: String,
    cookie: String,
}

/// Create a HttpOnly Cookie that may be used to refresh logins by generating a UUID which is persisted
/// to the database as a RefreshToken entity which links the UUID to the User.
fn create_refresh_token_cookie(
    registered_user: &User,
    connection: &mut DbConnection,
) -> Result<RefreshTokenCookie, Error> {
    let uuid = Uuid::new_v4();
    let current_utc = Utc::now();
    let expiry = current_utc + *REFRESH_TOKEN_EXPIRATION;

    let new_refresh_token = NewRefreshToken {
        uuid,
        expiry,
        invalidated: false,
        fk_registered_user: registered_user.pk,
    };

    let refresh_token = match diesel::insert_into(refresh_token::table)
        .values(&new_refresh_token)
        .get_result::<RefreshToken>(connection)
    {
        Ok(refresh_token) => refresh_token,
        Err(e) => return Err(Error::QueryError(e.to_string())),
    };

    let uuid = refresh_token.uuid.to_string();
    let expiry = refresh_token.expiry.to_rfc2822();
    let cookie = format_refresh_token_cookie(&uuid, &expiry);

    Ok(RefreshTokenCookie {
        token: uuid,
        cookie,
    })
}

#[inline]
fn format_refresh_token_cookie(uuid: &str, expiry: &str) -> String {
    if cfg!(debug_assertions) {
        // unlike firefox, chrome (and postman and other chromium / electron based apps) does not allow setting Secure cookies on localhost
        format!("refresh_token={}; Expires={}; HttpOnly", uuid, expiry)
    } else {
        format!(
            "refresh_token={}; Expires={}; HttpOnly; Secure; SameSite=None",
            uuid, expiry
        )
    }
}

/// Create a [`LoginResponse`] for the provided User and add the provided refresh token cookie.
/// Used when a /login or /refresh-login succeeds.
fn create_login_response(
    registered_user: User,
    refresh_token_cookie: RefreshTokenCookie,
) -> Result<impl Reply, Error> {
    let login_response = create_login_response_struct(registered_user, refresh_token_cookie.token)?;

    let json_response =
        serde_json::to_vec(&login_response).map_err(|_| Error::SerialisationError)?;

    let response_body = Response::builder()
        .status(StatusCode::OK)
        .header(header::SET_COOKIE, refresh_token_cookie.cookie)
        .header(header::CONTENT_TYPE, "application/json")
        .body(json_response)
        .map_err(|_| Error::SerialisationError)?;

    Ok(response_body)
}

fn create_login_response_struct(
    registered_user: User,
    refresh_token: String,
) -> Result<LoginResponse, Error> {
    let expiration_period = *ACCESS_TOKEN_EXPIRATION;
    let expiration_secs = expiration_period.num_seconds();
    let expiration = Utc::now()
        .checked_add_signed(expiration_period)
        .expect("Invalid timestamp")
        .timestamp();

    let claims = Claims {
        exp: expiration as usize,
        sub: registered_user.user_name.clone(),
    };

    let header = Header::new(Algorithm::HS512);
    let token = match encode(
        &header,
        &claims,
        &EncodingKey::from_secret(&crate::JWT_SECRET.to_be_bytes()),
    ) {
        Ok(token) => token,
        Err(_) => return Err(Error::JwtCreationError),
    };

    Ok(LoginResponse {
        token,
        refresh_token,
        expiration_secs,
        user: registered_user.into(),
    })
}

/// Refreshes a login for the provided refresh token by creating a fresh JWT for the User linked
/// to the refresh token and refreshes the refresh token with a new UUID and resets its expiration.
///
/// Returns a [`LoginResponse`] with the new JWT if the refresh token is valid (the UUID exists and
/// the refresh token is not expired) or else returns a InvalidRefreshTokenError which results in a 401.
pub async fn refresh_login_handler(refresh_token: String) -> Result<impl Reply, Rejection> {
    let (user, refresh_token_cookie) = refresh_user_login_data(refresh_token)?;
    create_login_response(user, refresh_token_cookie).map_err(warp::reject::custom)
}

pub async fn try_refresh_login_handler(
    refresh_token: Option<String>,
) -> Result<impl Reply, Rejection> {
    let (login_response, refresh_token_cookie) = match refresh_token {
        Some(refresh_token) if !refresh_token.is_empty() => {
            if let Some((user, refresh_token_cookie)) = match refresh_user_login_data(refresh_token)
            {
                Ok(res) => Some(res),
                Err(Error::InvalidRefreshTokenError) => None,
                Err(e) => return Err(warp::reject::custom(e)),
            } {
                (
                    Some(create_login_response_struct(
                        user,
                        refresh_token_cookie.token,
                    )?),
                    Some(refresh_token_cookie.cookie),
                )
            } else {
                (None, None)
            }
        }
        _ => (None, None),
    };

    let json_response = serde_json::to_vec(&login_response)
        .map_err(|_| warp::reject::custom(Error::SerialisationError))?;

    let mut response_builder = Response::builder().status(StatusCode::OK);

    if let Some(refresh_token_cookie) = refresh_token_cookie {
        response_builder = response_builder.header(header::SET_COOKIE, refresh_token_cookie);
    }

    let response_body = response_builder
        .header(header::CONTENT_TYPE, "application/json")
        .body(json_response)
        .map_err(|_| warp::reject::custom(Error::SerialisationError))?;

    Ok(response_body)
}

pub async fn logout_handler(refresh_token: Option<String>) -> Result<impl Reply, Rejection> {
    let mut response_builder = Response::builder().status(StatusCode::OK);
    if let Some(refresh_token) = refresh_token {
        let curr_token_uuid = Uuid::parse_str(&refresh_token)
            .map_err(|_| Error::BadRequestError(String::from("Invalid refresh token")))?;
        let mut connection = acquire_db_connection()?;
        diesel::delete(refresh_token::table.filter(refresh_token::uuid.eq(&curr_token_uuid)))
            .execute(&mut connection)
            .map_err(Error::from)?;

        let refresh_token_cookie = format_refresh_token_cookie("", &Utc::now().to_rfc2822());
        response_builder = response_builder.header(header::SET_COOKIE, refresh_token_cookie);
    }

    Ok(response_builder.body(hyper::Body::empty()))
}

fn refresh_user_login_data(refresh_token: String) -> Result<(User, RefreshTokenCookie), Error> {
    let mut connection = acquire_db_connection()?;
    connection.transaction(|connection| {
        let curr_token_uuid = Uuid::parse_str(&refresh_token)
            .map_err(|_| Error::BadRequestError(String::from("Invalid refresh token")))?;
        let current_utc = Utc::now();

        let refresh_token = refresh_token::table
            .filter(
                refresh_token::uuid
                    .eq(&curr_token_uuid)
                    .and(refresh_token::expiry.ge(&current_utc))
                    .and(refresh_token::invalidated.eq(false)),
            )
            .first::<RefreshToken>(connection)
            .optional()
            .map_err(|e| Error::QueryError(e.to_string()))?
            .ok_or(Error::InvalidRefreshTokenError)?;

        let user = registered_user::table
            .filter(registered_user::pk.eq(refresh_token.fk_registered_user))
            .first::<User>(connection)
            .map_err(|e| Error::QueryError(e.to_string()))?;

        let expiry = current_utc + *REFRESH_TOKEN_EXPIRATION;
        let new_token = Uuid::new_v4();

        let updated_token = diesel::update(refresh_token::table)
            .filter(refresh_token::pk.eq(refresh_token.pk))
            .set((
                refresh_token::uuid.eq(new_token),
                refresh_token::expiry.eq(expiry),
            ))
            .get_result::<RefreshToken>(connection)
            .map_err(|e| Error::QueryError(e.to_string()))?;

        let uuid = updated_token.uuid.to_string();
        let expiry = updated_token.expiry.to_rfc2822();

        let cookie = format_refresh_token_cookie(&uuid, &expiry);
        Ok((
            user,
            RefreshTokenCookie {
                token: uuid,
                cookie,
            },
        ))
    })
}

lazy_static! {
    static ref USER_NAME_SYNC: MutexSync<String> = MutexSync::new();
}

/// Registers a user by creating a new User. This request receives a json that
/// is deserialized to the [`UserRegistration`] struct which contains all information to create a
/// new User. Additionally, this performs a login for the created user by setting a refresh_token
/// cookie and returning a [`LoginResponse`].
///
/// If the given user_name already exists the endpoint returns a UserExistsError which results
/// in a 400.
///
/// Creating the User is synchronised based on the value of user_name by mapping a mutex to it.
/// This means that concurrent attempts to register the same user_name will be synchronised so that
/// one request is guaranteed to see the User created by other, instead of receiving unique
/// constraint violation when committing either transaction.
pub async fn register_handler(
    user_registration: UserRegistration,
) -> Result<impl Reply, Rejection> {
    user_registration.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for UserRegistration: {}",
            e
        )))
    })?;

    // synchronise user creation based on user_name
    USER_NAME_SYNC.evaluate(user_registration.user_name.clone(), || {
        let mut connection = acquire_db_connection()?;
        connection
            .transaction(|connection| {
                let existing_count: Result<i64, _> = registered_user::table
                    .select(count(registered_user::pk))
                    .filter(registered_user::user_name.eq(&user_registration.user_name))
                    .first(connection);

                match existing_count {
                    Ok(count) => {
                        if count != 0 {
                            return Err(Error::UserExistsError(user_registration.user_name));
                        }
                    }
                    Err(e) => return Err(Error::QueryError(e.to_string())),
                };

                let hashed_password = match hash(&user_registration.password, DEFAULT_COST) {
                    Ok(hashed_password) => hashed_password,
                    Err(_) => return Err(Error::EncryptionError),
                };

                let new_user = NewUser {
                    user_name: user_registration.user_name,
                    password: hashed_password,
                    email: user_registration.email,
                    avatar_url: user_registration.avatar_url,
                    creation_timestamp: Utc::now(),
                };

                match diesel::insert_into(registered_user::table)
                    .values(&new_user)
                    .get_result::<User>(connection)
                {
                    Ok(registered_user) => {
                        let refresh_token_cookie =
                            create_refresh_token_cookie(&registered_user, connection)?;
                        create_login_response(registered_user, refresh_token_cookie)
                    }
                    Err(e) => Err(Error::QueryError(e.to_string())),
                }
            })
            .map_err(warp::reject::custom)
    })
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UserInfo {
    pub user_name: String,
    pub email: Option<String>,
    pub avatar_url: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
}

impl From<User> for UserInfo {
    fn from(user: User) -> Self {
        Self {
            user_name: user.user_name,
            email: user.email,
            avatar_url: user.avatar_url,
            creation_timestamp: user.creation_timestamp,
        }
    }
}

pub async fn current_user_info_handler(user: User) -> Result<impl Reply, Rejection> {
    Ok(warp::reply::json(&UserInfo::from(user)))
}
