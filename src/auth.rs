use std::net::SocketAddr;

use bcrypt::{DEFAULT_COST, hash, verify};
use chrono::{DateTime, Duration, offset::Utc};
use diesel::{dsl::count, expression_methods::BoolExpressionMethods};
use diesel_async::{AsyncPgConnection, RunQueryDsl, scoped_futures::ScopedFutureExt};
use exec_rs::sync::MutexSync;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use lazy_static::lazy_static;
use passwords::PasswordGenerator;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator::Validate;
use warp::{
    Filter, Rejection, Reply,
    filters::header::headers_cloned,
    host::Authority,
    http::{
        Response, StatusCode,
        header::{self, HeaderMap},
    },
    hyper,
};
use zxcvbn::zxcvbn;

use crate::{
    CERT_PATH, HOST_BASE_PATH, acquire_db_connection,
    diesel::{ExpressionMethods, OptionalExtension, QueryDsl},
    error::{Error, TransactionRuntimeError},
    mail,
    model::{EmailConfirmationToken, NewUser, OneTimePassword, RefreshToken, User},
    query::functions::lower,
    retry_on_constraint_violation, run_retryable_transaction,
    schema::{email_confirmation_token, one_time_password, refresh_token, registered_user},
};

mod captcha;

lazy_static! {
    pub static ref ACCESS_TOKEN_EXPIRATION: Duration = Duration::hours(3);
    pub static ref REFRESH_TOKEN_EXPIRATION: Duration = Duration::weeks(1);
    pub static ref EMAIL_CONFIRMATION_TOKEN_EXPIRATION: Duration = Duration::hours(24);
    pub static ref ONE_TIME_PASSWORD_EXPIRATION: Duration = Duration::minutes(30);
}

/// Struct received by the /login request.
#[derive(Deserialize)]
pub struct LoginRequest {
    pub user_name: String,
    pub password: String,
    pub captcha_token: Option<String>,
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
    #[validate(length(max = 32))]
    pub display_name: Option<String>,
    #[validate(length(min = 1, max = 25))]
    pub user_name: String,
    #[validate(length(min = 1, max = 255))]
    pub password: String,
    #[validate(email)]
    pub email: Option<String>,
    #[validate(url)]
    pub avatar_url: Option<String>,
    pub captcha_token: Option<String>,
}

/// Struct encoded in the JWT that contains its expiry and subject user.
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    exp: usize,
    sub: String,
    ver: i32,
}

/// Warp filter for requests that optionally receive the logged in user from the auth header.
pub fn with_user_optional() -> impl Filter<Extract = (Option<User>,), Error = Rejection> + Clone {
    headers_cloned().and_then(get_user_from_auth_header)
}

/// Warp filter for requests that require a logged in user provided by the auth header.
pub fn with_user() -> impl Filter<Extract = (User,), Error = Rejection> + Clone {
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
/// If the token is expired or has been invalidate (the `ver` field doesnt match the `jwt_version` of the user)
/// this returns a 401.
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
    // fails if expired
    let token_data = decode::<Claims>(
        jwt_token,
        &DecodingKey::from_secret(&crate::JWT_SECRET.to_be_bytes()),
        &Validation::new(Algorithm::HS512),
    )
    .map_err(|_| warp::reject::custom(Error::InvalidJwtError))?;
    let claims = &token_data.claims;

    let mut connection = acquire_db_connection().await?;
    match registered_user::table
        .filter(registered_user::user_name.eq(&claims.sub))
        .get_result::<User>(&mut connection)
        .await
        .optional()
    {
        Ok(Some(registered_user)) if registered_user.jwt_version == claims.ver => {
            Ok(Some(registered_user))
        }
        Ok(_) => Err(warp::reject::custom(Error::InvalidJwtError)),
        Err(e) => Err(warp::reject::custom(Error::QueryError(e.to_string()))),
    }
}

/// Handler for the /login endpoint that receives a json deserialized to the [`LoginRequest`] struct
/// and returns a [`LoginResponse`] if the credentials are correct or a InvalidCredentialsError, which
/// results in a 403, if the credentials are not correct.
pub async fn login_handler(
    request: LoginRequest,
    remote_addr: Option<SocketAddr>,
) -> Result<impl Reply, Rejection> {
    let captcha_verified = match request.captcha_token {
        Some(captcha_token) => {
            if let Some(ref captcha_secret) = *captcha::CAPTCHA_SECRET {
                captcha::verify_captcha(captcha_secret.clone(), captcha_token, remote_addr).await?;
                true
            } else {
                true
            }
        }
        None if captcha::CAPTCHA_SECRET.is_some() => false,
        None => true,
    };

    let mut connection = acquire_db_connection().await?;
    let found_registered_user = registered_user::table
        .filter(lower(registered_user::user_name).eq(&request.user_name.to_lowercase()))
        .get_result::<User>(&mut connection)
        .await;
    let registered_user = match found_registered_user {
        Ok(registered_user) => {
            if registered_user.password_fail_count >= 10 && !captcha_verified {
                return Err(warp::reject::custom(Error::InvalidCaptchaError));
            }
            let hashed_password = &registered_user.password;
            match verify(&request.password, hashed_password) {
                Ok(valid) => {
                    if valid {
                        diesel::update(registered_user::table)
                            .filter(registered_user::pk.eq(&registered_user.pk))
                            .set(registered_user::password_fail_count.eq(0))
                            .execute(&mut connection)
                            .await
                            .map_err(Error::from)?;
                        registered_user
                    } else {
                        diesel::update(registered_user::table)
                            .filter(registered_user::pk.eq(&registered_user.pk))
                            .set(
                                registered_user::password_fail_count
                                    .eq(registered_user::password_fail_count + 1),
                            )
                            .execute(&mut connection)
                            .await
                            .map_err(Error::from)?;
                        return Err(warp::reject::custom(Error::InvalidCredentialsError));
                    }
                }
                Err(_) => return Err(warp::reject::custom(Error::EncryptionError)),
            }
        }
        Err(diesel::NotFound) => return Err(warp::reject::custom(Error::InvalidCredentialsError)),
        Err(e) => return Err(warp::reject::custom(Error::QueryError(e.to_string()))),
    };

    let refresh_token_cookie =
        create_refresh_token_cookie(&registered_user, &mut connection).await?;
    create_login_response(registered_user, refresh_token_cookie).map_err(warp::reject::custom)
}

struct RefreshTokenCookie {
    token: String,
    cookie: String,
}

/// Create a HttpOnly Cookie that may be used to refresh logins by generating a UUID which is persisted
/// to the database as a RefreshToken entity which links the UUID to the User.
async fn create_refresh_token_cookie(
    registered_user: &User,
    connection: &mut AsyncPgConnection,
) -> Result<RefreshTokenCookie, Error> {
    let uuid = Uuid::new_v4();
    let current_utc = Utc::now();
    let expiry = current_utc + *REFRESH_TOKEN_EXPIRATION;

    let new_refresh_token = RefreshToken {
        uuid,
        expiry,
        invalidated: false,
        fk_user: registered_user.pk,
    };

    let refresh_token = match diesel::insert_into(refresh_token::table)
        .values(&new_refresh_token)
        .get_result::<RefreshToken>(connection)
        .await
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

    let json_response = serde_json::to_vec(&login_response)
        .map_err(|e| Error::SerialisationError(e.to_string()))?;

    let response_body = Response::builder()
        .status(StatusCode::OK)
        .header(header::SET_COOKIE, refresh_token_cookie.cookie)
        .header(header::CONTENT_TYPE, "application/json")
        .body(json_response)
        .map_err(|e| Error::SerialisationError(e.to_string()))?;

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
        ver: registered_user.jwt_version,
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
    let (user, refresh_token_cookie) = refresh_user_login_data(refresh_token).await?;
    create_login_response(user, refresh_token_cookie).map_err(warp::reject::custom)
}

pub async fn try_refresh_login_handler(
    refresh_token: Option<String>,
) -> Result<impl Reply, Rejection> {
    let (login_response, refresh_token_cookie) = match refresh_token {
        Some(refresh_token) if !refresh_token.is_empty() => {
            if let Some((user, refresh_token_cookie)) =
                match refresh_user_login_data(refresh_token).await {
                    Ok(res) => Some(res),
                    Err(Error::InvalidRefreshTokenError) => None,
                    Err(e) => return Err(warp::reject::custom(e)),
                }
            {
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
        .map_err(|e| warp::reject::custom(Error::SerialisationError(e.to_string())))?;

    let mut response_builder = Response::builder().status(StatusCode::OK);

    if let Some(refresh_token_cookie) = refresh_token_cookie {
        response_builder = response_builder.header(header::SET_COOKIE, refresh_token_cookie);
    }

    let response_body = response_builder
        .header(header::CONTENT_TYPE, "application/json")
        .body(json_response)
        .map_err(|e| warp::reject::custom(Error::SerialisationError(e.to_string())))?;

    Ok(response_body)
}

pub async fn logout_handler(refresh_token: Option<String>) -> Result<impl Reply, Rejection> {
    let mut response_builder = Response::builder().status(StatusCode::OK);
    if let Some(refresh_token) = refresh_token {
        let curr_token_uuid = Uuid::parse_str(&refresh_token)
            .map_err(|_| Error::BadRequestError(String::from("Invalid refresh token")))?;
        let mut connection = acquire_db_connection().await?;
        diesel::delete(refresh_token::table.filter(refresh_token::uuid.eq(&curr_token_uuid)))
            .execute(&mut connection)
            .await
            .map_err(Error::from)?;

        let refresh_token_cookie = format_refresh_token_cookie("", &Utc::now().to_rfc2822());
        response_builder = response_builder.header(header::SET_COOKIE, refresh_token_cookie);
    }

    Ok(response_builder.body(hyper::Body::empty()))
}

async fn refresh_user_login_data(
    refresh_token: String,
) -> Result<(User, RefreshTokenCookie), Error> {
    let mut connection = acquire_db_connection().await?;
    run_retryable_transaction(&mut connection, |connection| {
        async move {
            let curr_token_uuid = Uuid::parse_str(&refresh_token)
                .map_err(|_| Error::BadRequestError(String::from("Invalid refresh token")))?;
            let current_utc = Utc::now();

            let expiry = current_utc + *REFRESH_TOKEN_EXPIRATION;
            let new_token = Uuid::new_v4();

            let updated_token = diesel::update(refresh_token::table)
                .filter(
                    refresh_token::uuid
                        .eq(&curr_token_uuid)
                        .and(refresh_token::expiry.ge(&current_utc))
                        .and(refresh_token::invalidated.eq(false)),
                )
                .set((
                    refresh_token::uuid.eq(new_token),
                    refresh_token::expiry.eq(expiry),
                ))
                .get_result::<RefreshToken>(connection)
                .await
                .optional()
                .map_err(|e| Error::QueryError(e.to_string()))?
                .ok_or(Error::InvalidRefreshTokenError)?;

            let user = registered_user::table
                .filter(registered_user::pk.eq(updated_token.fk_user))
                .get_result::<User>(connection)
                .await
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
        }
        .scope_boxed()
    })
    .await
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
    mut user_registration: UserRegistration,
    remote_addr: Option<SocketAddr>,
    authority: Authority,
) -> Result<impl Reply, Rejection> {
    // set empty mail to None since an empty string would not validate
    if let Some(ref email) = user_registration.email {
        if email.trim().is_empty() {
            user_registration.email = None;
        }
    }
    user_registration.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for UserRegistration: {}",
            e
        )))
    })?;

    let mut zxcvbn_user_data = vec![user_registration.user_name.as_str()];
    if let Some(ref email) = user_registration.email {
        zxcvbn_user_data.push(email.as_str());
    }
    if let Some(ref display_name) = user_registration.display_name {
        zxcvbn_user_data.push(display_name.as_str());
    }
    let entropy = zxcvbn(&user_registration.password, &zxcvbn_user_data);
    if <u8>::from(entropy.score()) < 3 {
        return Err(warp::reject::custom(Error::WeakPasswordError));
    }

    let check_user_name_response = check_username(&user_registration.user_name).await?;
    if !check_user_name_response.valid {
        return Err(warp::reject::custom(Error::InvalidUserNameError));
    }
    if !check_user_name_response.available {
        return Err(warp::reject::custom(Error::UniqueValueError(
            user_registration.user_name,
        )));
    }

    if let Some(ref captcha_secret) = *captcha::CAPTCHA_SECRET {
        let captcha_token = user_registration
            .captcha_token
            .ok_or(Error::InvalidCaptchaError)?;
        captcha::verify_captcha(captcha_secret.clone(), captcha_token, remote_addr).await?;
    }
    let hashed_password = match hash(&user_registration.password, DEFAULT_COST) {
        Ok(hashed_password) => hashed_password,
        Err(_) => return Err(warp::reject::custom(Error::EncryptionError)),
    };

    let new_user = NewUser {
        user_name: user_registration.user_name,
        password: hashed_password,
        email: user_registration.email,
        avatar_url: user_registration.avatar_url,
        creation_timestamp: Utc::now(),
        email_confirmed: false,
        display_name: user_registration.display_name,
    };
    let mut connection = acquire_db_connection().await?;
    let (created_user, login_response) = run_retryable_transaction(&mut connection, |connection| {
        async move {
            let existing_count: Result<i64, _> = registered_user::table
                .select(count(registered_user::pk))
                .filter(lower(registered_user::user_name).eq(&new_user.user_name.to_lowercase()))
                .get_result(connection)
                .await;

            match existing_count {
                Ok(count) => {
                    if count != 0 {
                        return Err(TransactionRuntimeError::Rollback(Error::UniqueValueError(
                            new_user.user_name.clone(),
                        )));
                    }
                }
                Err(e) => {
                    return Err(TransactionRuntimeError::Rollback(Error::QueryError(
                        e.to_string(),
                    )));
                }
            };

            match diesel::insert_into(registered_user::table)
                .values(&new_user)
                .get_result::<User>(connection)
                .await
                .map_err(retry_on_constraint_violation)
            {
                Ok(registered_user) => {
                    let refresh_token_cookie =
                        create_refresh_token_cookie(&registered_user, connection).await?;
                    let login_response =
                        create_login_response(registered_user.clone(), refresh_token_cookie)
                            .map_err(TransactionRuntimeError::Rollback)?;
                    Ok((registered_user, login_response))
                }
                Err(e) => Err(TransactionRuntimeError::Rollback(Error::QueryError(
                    e.to_string(),
                ))),
            }
        }
        .scope_boxed()
    })
    .await?;

    if mail::mail_enabled() {
        if created_user
            .email
            .as_ref()
            .map(|str| !str.is_empty())
            .unwrap_or(false)
        {
            tokio::spawn(async move {
                match prepare_email_confirmation_token(created_user.pk).await {
                    Ok(email_confirmation_token) => {
                        let mut context = tera::Context::new();
                        prepare_email_confirmation_context(
                            &mut context,
                            &email_confirmation_token,
                            &authority,
                        );
                        mail::send_mail(
                            "welcome.html",
                            context,
                            String::from("Welcome"),
                            created_user,
                        );
                    }
                    Err(e) => log::error!(
                        "Failed to prepare email confirmation token for user pk {}: {e}",
                        created_user.pk
                    ),
                }
            });
        }
    } else {
        log::warn!("Not sending welcome mail because mail is not set up.");
    }

    Ok(login_response)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UserInfo {
    pub user_name: String,
    pub email: Option<String>,
    pub avatar_url: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
    pub email_confirmed: bool,
    pub display_name: Option<String>,
}

impl From<User> for UserInfo {
    fn from(user: User) -> Self {
        Self {
            user_name: user.user_name,
            email: user.email,
            avatar_url: user.avatar_url,
            creation_timestamp: user.creation_timestamp,
            email_confirmed: user.email_confirmed,
            display_name: user.display_name,
        }
    }
}

pub async fn current_user_info_handler(user: User) -> Result<impl Reply, Rejection> {
    Ok(warp::reply::json(&UserInfo::from(user)))
}

#[derive(Serialize)]
pub struct CheckUsernameResponse {
    pub valid: bool,
    pub available: bool,
}

pub async fn check_username_handler(user_name: String) -> Result<impl Reply, Rejection> {
    let user_name = percent_encoding::percent_decode(user_name.as_bytes())
        .decode_utf8()
        .map_err(|_| Error::UtfEncodingError)?;

    let response = check_username(&user_name).await?;
    Ok(warp::reply::json(&response))
}

async fn check_username(user_name: &str) -> Result<CheckUsernameResponse, Error> {
    let valid = !user_name.is_empty()
        && user_name.len() <= 25
        && user_name
            .chars()
            .all(|c| !c.is_whitespace() && !c.is_control());
    if !valid {
        return Ok(CheckUsernameResponse {
            valid,
            available: false,
        });
    }

    let mut connection = acquire_db_connection().await?;
    let existing_count: i64 = registered_user::table
        .select(count(registered_user::pk))
        .filter(lower(registered_user::user_name).eq(&user_name.to_lowercase()))
        .get_result(&mut connection)
        .await
        .map_err(Error::from)?;

    Ok(CheckUsernameResponse {
        valid,
        available: existing_count == 0,
    })
}

async fn prepare_email_confirmation_token(user_pk: i64) -> Result<EmailConfirmationToken, Error> {
    let current_utc = Utc::now();
    let expiry = current_utc + *EMAIL_CONFIRMATION_TOKEN_EXPIRATION;
    let mut connection = acquire_db_connection().await?;
    diesel::insert_into(email_confirmation_token::table)
        .values(&EmailConfirmationToken {
            uuid: Uuid::new_v4(),
            expiry,
            invalidated: false,
            fk_user: user_pk,
        })
        .get_result::<EmailConfirmationToken>(&mut connection)
        .await
        .map_err(Error::from)
}

fn prepare_email_confirmation_context(
    context: &mut tera::Context,
    email_confirmation_token: &EmailConfirmationToken,
    authority: &Authority,
) {
    context.insert("email_confirmation_token", email_confirmation_token);
    let mut host = HOST_BASE_PATH
        .clone()
        .unwrap_or_else(|| authority.to_string());
    if !host.starts_with("http://") && !host.starts_with("https://") {
        if CERT_PATH.is_some() {
            host.insert_str(0, "https://");
        } else {
            host.insert_str(0, "http://");
        }
    }
    if !host.ends_with('/') {
        host.push('/');
    }
    context.insert("host", &host);
    host.push_str("confirm-email/");
    host.push_str(&email_confirmation_token.uuid.to_string());
    context.insert("email_confirmation_link", &host);
}

pub async fn confirm_email_handler(
    email_confirmation_token: Uuid,
    user: User,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    let deleted_token = run_retryable_transaction(&mut connection, |connection| {
        async move {
            let current_utc = Utc::now();
            let deleted_token = diesel::delete(email_confirmation_token::table)
                .filter(
                    email_confirmation_token::uuid
                        .eq(&email_confirmation_token)
                        .and(email_confirmation_token::fk_user.eq(user.pk))
                        .and(email_confirmation_token::expiry.ge(&current_utc))
                        .and(email_confirmation_token::invalidated.eq(false)),
                )
                .returning(email_confirmation_token::uuid)
                .get_result::<Uuid>(connection)
                .await
                .optional()?;

            if deleted_token.is_some() {
                diesel::update(registered_user::table)
                    .filter(registered_user::pk.eq(user.pk))
                    .set(registered_user::email_confirmed.eq(true))
                    .execute(connection)
                    .await?;
            }

            Ok(deleted_token)
        }
        .scope_boxed()
    })
    .await?;

    if deleted_token.is_none() {
        Err(warp::reject::custom(Error::InvalidTokenError(
            String::from("Invalid email confirmation token"),
        )))
    } else {
        Ok(warp::reply())
    }
}

#[derive(AsChangeset, Deserialize, Validate)]
#[diesel(table_name = registered_user)]
pub struct UpdateUserRequest {
    #[validate(length(max = 32))]
    pub display_name: Option<String>,
    #[validate(email)]
    pub email: Option<String>,
    #[validate(url)]
    pub avatar_url: Option<String>,
}

impl UpdateUserRequest {
    pub fn has_changes(&self) -> bool {
        self.display_name.is_some() || self.email.is_some() || self.avatar_url.is_some()
    }
}

pub async fn edit_user_handler(
    mut request: UpdateUserRequest,
    user: User,
    authority: Authority,
) -> Result<impl Reply, Rejection> {
    // set empty mail to None since an empty string would not validate
    let mut cleared_mail = false;
    if let Some(ref email) = request.email {
        if email.trim().is_empty() {
            request.email = None;
            cleared_mail = true;
        }
    }
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for UpdateUserRequest: {}",
            e
        )))
    })?;
    if cleared_mail {
        request.email = Some(String::from(""));
    }

    let mut connection = acquire_db_connection().await?;
    let updated_user = if request.has_changes() {
        diesel::update(registered_user::table)
            .filter(registered_user::pk.eq(user.pk))
            .set(&request)
            .get_result::<User>(&mut connection)
            .await
            .map_err(Error::from)?
    } else {
        registered_user::table
            .filter(registered_user::pk.eq(user.pk))
            .get_result::<User>(&mut connection)
            .await
            .map_err(Error::from)?
    };

    let email_changed = request.email.is_some()
        && user
            .email
            .as_ref()
            .map(|addr| addr != request.email.as_ref().unwrap())
            .unwrap_or(true);
    if request.has_changes() && email_changed {
        let new_user = updated_user.clone();
        tokio::spawn(async move {
            let mut removed_context = tera::Context::new();
            removed_context.insert("new_email_address", request.email.as_ref().unwrap());

            let mut confirmation_context = tera::Context::new();
            let email_confirmation_token = match prepare_email_confirmation_token(user.pk).await {
                Ok(email_confirmation_token) => email_confirmation_token,
                Err(e) => {
                    log::error!(
                        "Failed to send mail for email change for user pk {}: {e}",
                        user.pk
                    );
                    return;
                }
            };
            prepare_email_confirmation_context(
                &mut confirmation_context,
                &email_confirmation_token,
                &authority,
            );
            if let Some(ref prev_email) = user.email {
                confirmation_context.insert("prev_email_address", prev_email);
            }

            if user.email.is_some() && user.email_confirmed {
                mail::send_mail(
                    "email_change_removed.html",
                    removed_context,
                    String::from("Email changed"),
                    user,
                );
            }

            if new_user.email.is_some() {
                mail::send_mail(
                    "email_change_notification.html",
                    confirmation_context,
                    String::from("Email changed"),
                    new_user,
                );
            }
        });
    }

    Ok(warp::reply::json(&updated_user))
}

pub async fn send_email_confirmation_link_handler(
    user: User,
    authority: Authority,
) -> Result<impl Reply, Rejection> {
    if user.email_confirmed {
        return Err(warp::reject::custom(Error::EmailAlreadyConfirmedError));
    }
    if user
        .email
        .as_ref()
        .map(|email| email.trim().is_empty())
        .unwrap_or(true)
    {
        return Err(warp::reject::custom(Error::BadRequestError(String::from(
            "User does not have an email address",
        ))));
    }

    let email_confirmation_token = prepare_email_confirmation_token(user.pk).await?;
    let mut context = tera::Context::new();
    prepare_email_confirmation_context(&mut context, &email_confirmation_token, &authority);
    mail::send_mail(
        "email_confirmation.html",
        context,
        String::from("Email confirmation"),
        user,
    );

    Ok(warp::reply::reply())
}

#[derive(Deserialize)]
pub struct ChangePasswordRequest {
    pub password: String,
    pub new_password: String,
    pub captcha_token: Option<String>,
}

pub async fn change_password_handler(
    request: ChangePasswordRequest,
    user: User,
    remote_addr: Option<SocketAddr>,
) -> Result<impl Reply, Rejection> {
    match request.captcha_token {
        Some(captcha_token) => {
            if let Some(ref captcha_secret) = *captcha::CAPTCHA_SECRET {
                captcha::verify_captcha(captcha_secret.clone(), captcha_token, remote_addr).await?;
            }
        }
        None if captcha::CAPTCHA_SECRET.is_some() && user.password_fail_count >= 10 => {
            return Err(warp::reject::custom(Error::InvalidCaptchaError));
        }
        None => {}
    };

    let current_password = &user.password;
    let valid = verify(&request.password, current_password).map_err(|_| Error::EncryptionError)?;
    let mut connection = acquire_db_connection().await?;
    if !valid {
        diesel::update(registered_user::table)
            .filter(registered_user::pk.eq(&user.pk))
            .set(registered_user::password_fail_count.eq(registered_user::password_fail_count + 1))
            .execute(&mut connection)
            .await
            .map_err(Error::from)?;
        return Err(warp::reject::custom(Error::InvalidCredentialsError));
    } else {
        diesel::update(registered_user::table)
            .filter(registered_user::pk.eq(&user.pk))
            .set(registered_user::password_fail_count.eq(0))
            .execute(&mut connection)
            .await
            .map_err(Error::from)?;
    }

    let mut zxcvbn_user_data = vec![user.user_name.as_str()];
    if let Some(ref email) = user.email {
        zxcvbn_user_data.push(email.as_str());
    }
    if let Some(ref display_name) = user.display_name {
        zxcvbn_user_data.push(display_name.as_str());
    }
    let entropy = zxcvbn(&request.new_password, &zxcvbn_user_data);
    if <u8>::from(entropy.score()) < 3 {
        return Err(warp::reject::custom(Error::WeakPasswordError));
    }

    let hashed_password =
        hash(&request.new_password, DEFAULT_COST).map_err(|_| Error::EncryptionError)?;

    run_retryable_transaction(&mut connection, |connection| {
        async move {
            let user = diesel::update(registered_user::table)
                .filter(registered_user::pk.eq(user.pk))
                .set(registered_user::password.eq(&hashed_password))
                .get_result::<User>(connection)
                .await?;

            diesel::update(refresh_token::table)
                .filter(refresh_token::fk_user.eq(user.pk))
                .set(refresh_token::invalidated.eq(true))
                .execute(connection)
                .await?;

            let refresh_token_cookie = create_refresh_token_cookie(&user, connection).await?;
            create_login_response(user, refresh_token_cookie).map_err(|e| e.into())
        }
        .scope_boxed()
    })
    .await
    .map_err(warp::reject::custom)
}

#[derive(Deserialize, Validate)]
pub struct SendPasswordResetRequest {
    #[validate(length(min = 1, max = 25))]
    pub user_name: String,
    #[validate(email)]
    pub email: String,
    pub captcha_token: Option<String>,
}

pub async fn send_password_reset_handler(
    request: SendPasswordResetRequest,
    remote_addr: Option<SocketAddr>,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for ChangePasswordRequest: {}",
            e
        )))
    })?;

    if let Some(ref captcha_secret) = *captcha::CAPTCHA_SECRET {
        let captcha_token = request.captcha_token.ok_or(Error::InvalidCaptchaError)?;
        captcha::verify_captcha(captcha_secret.clone(), captcha_token, remote_addr).await?;
    }

    let mut connection = acquire_db_connection().await?;
    let matching_user = registered_user::table
        .filter(
            lower(registered_user::user_name)
                .eq(&request.user_name.to_lowercase())
                .and(registered_user::email.eq(&request.email))
                .and(registered_user::email_confirmed),
        )
        .get_result::<User>(&mut connection)
        .await
        .optional()
        .map_err(Error::from)?;

    if let Some(user) = matching_user {
        let password_generator = PasswordGenerator::new()
            .length(8)
            .numbers(true)
            .lowercase_letters(true)
            .uppercase_letters(true)
            .symbols(false)
            .spaces(false)
            .exclude_similar_characters(true)
            .strict(true);

        let password = password_generator
            .generate_one()
            .map_err(|e| Error::InternalError(format!("Failed to generate password: {e}")))?;
        let hashed_password = hash(&password, DEFAULT_COST).map_err(|_| Error::EncryptionError)?;

        let current_utc = Utc::now();
        let expiry = current_utc + *ONE_TIME_PASSWORD_EXPIRATION;

        let otp = OneTimePassword {
            password: hashed_password,
            expiry,
            invalidated: false,
            fk_user: user.pk,
        };

        diesel::insert_into(one_time_password::table)
            .values(&otp)
            .on_conflict(one_time_password::fk_user)
            .do_update()
            .set((
                one_time_password::password.eq(&otp.password),
                one_time_password::expiry.eq(&otp.expiry),
                one_time_password::invalidated.eq(false),
            ))
            .execute(&mut connection)
            .await
            .map_err(Error::from)?;

        let mut tera_context = tera::Context::new();
        tera_context.insert("otp", &password);
        mail::send_mail(
            "password_reset.html",
            tera_context,
            String::from("Password Reset"),
            user,
        );
    }

    Ok(warp::reply())
}

#[derive(Deserialize, Validate)]
pub struct PasswordResetRequest {
    #[validate(length(min = 1, max = 25))]
    pub user_name: String,
    #[validate(email)]
    pub email: String,
    pub otp: String,
    pub new_password: String,
}

pub async fn reset_password_handler(
    request: PasswordResetRequest,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for PasswordResetRequest: {}",
            e
        )))
    })?;

    let mut connection = acquire_db_connection().await?;
    run_retryable_transaction(&mut connection, |connection| {
        async move {
            let user = registered_user::table
                .filter(
                    lower(registered_user::user_name)
                        .eq(&request.user_name.to_lowercase())
                        .and(registered_user::email.eq(&request.email))
                        .and(registered_user::email_confirmed),
                )
                .get_result::<User>(connection)
                .await
                .optional()
                .map_err(Error::from)?
                .ok_or(TransactionRuntimeError::from(
                    Error::InvalidCredentialsError,
                ))?;

            let opt = one_time_password::table
                .filter(one_time_password::fk_user.eq(user.pk))
                .get_result::<OneTimePassword>(connection)
                .await
                .optional()?
                .ok_or(TransactionRuntimeError::from(
                    Error::InvalidCredentialsError,
                ))?;

            match verify(&request.otp, &opt.password) {
                Ok(valid) => {
                    if !valid {
                        return Err(TransactionRuntimeError::from(
                            Error::InvalidCredentialsError,
                        ));
                    }
                }
                Err(_) => return Err(TransactionRuntimeError::from(Error::EncryptionError)),
            }

            let mut zxcvbn_user_data = vec![user.user_name.as_str()];
            if let Some(ref email) = user.email {
                zxcvbn_user_data.push(email.as_str());
            }
            if let Some(ref display_name) = user.display_name {
                zxcvbn_user_data.push(display_name.as_str());
            }
            let entropy = zxcvbn(&request.new_password, &zxcvbn_user_data);
            if <u8>::from(entropy.score()) < 3 {
                return Err(TransactionRuntimeError::from(Error::WeakPasswordError));
            }

            let hashed_password = hash(&request.new_password, DEFAULT_COST)
                .map_err(|_| TransactionRuntimeError::from(Error::EncryptionError))?;

            // changing password will increment jwt_version, updated user needs to be loaded to create valid JWT
            let user = diesel::update(registered_user::table)
                .filter(registered_user::pk.eq(user.pk))
                .set(registered_user::password.eq(&hashed_password))
                .get_result::<User>(connection)
                .await?;

            diesel::update(refresh_token::table)
                .filter(refresh_token::fk_user.eq(user.pk))
                .set(refresh_token::invalidated.eq(true))
                .execute(connection)
                .await?;

            diesel::delete(one_time_password::table)
                .filter(one_time_password::fk_user.eq(user.pk))
                .execute(connection)
                .await?;

            let refresh_token_cookie = create_refresh_token_cookie(&user, connection).await?;
            create_login_response(user, refresh_token_cookie).map_err(|e| e.into())
        }
        .scope_boxed()
    })
    .await
    .map_err(warp::reject::custom)
}
