#[macro_use]
extern crate diesel;
#[cfg(feature = "auto_migration")]
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};

use diesel::{
    r2d2::{self, ConnectionManager, Pool, PooledConnection},
    PgConnection,
};
use dotenv::dotenv;
use error::{Error, TransactionRuntimeError};
use lazy_static::lazy_static;
use mime::Mime;
use query::QueryParametersFilter;
use std::str::FromStr;
use warp::Filter;

mod auth;
mod data;
mod error;
mod model;
mod perms;
mod post;
mod query;
mod schema;

pub type DbConnection = PooledConnection<ConnectionManager<PgConnection>>;

lazy_static! {
    pub static ref CONNECTION_POOL: Pool<ConnectionManager<PgConnection>> = {
        let database_url = std::env::var("DATABASE_URL")
            .expect("Missing environment variable DATABASE_URL must be set to connect to postgres");
        let database_connection_manager =
            r2d2::ConnectionManager::<PgConnection>::new(database_url);
        let max_db_connections = std::env::var("MAX_DB_CONNECTIONS")
            .unwrap_or_else(|_| String::from("25"))
            .parse::<u32>()
            .expect("MAX_DB_CONNECTIONS is not a valid u32");
        r2d2::Builder::new()
            .min_idle(Some(5))
            .max_size(max_db_connections)
            .build(database_connection_manager)
            .expect("Failed to initialise connection pool")
    };
    pub static ref JWT_SECRET: u64 = {
        let secret_str = std::env::var("JWT_SECRET")
            .expect("Missing environment variable JWT_SECRET must be set to generate JWT tokens.");
        u64::from_str(&secret_str).expect("JWT_SECRET var is not a valid u64 value")
    };
    pub static ref PORT: u16 = {
        let port_str =
            std::env::var("API_PORT").expect("Missing environment variable API_PORT must be set.");
        u16::from_str(&port_str).expect("API_PORT var is not a valid u16 value")
    };
    pub static ref CERT_PATH: Option<String> = std::env::var("CERT_PATH").ok();
    pub static ref KEY_PATH: Option<String> = std::env::var("KEY_PATH").ok();
}

#[cfg(feature = "auto_migration")]
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!();

fn main() {
    dotenv().ok();

    // initialise certain lazy statics on startup
    lazy_static::initialize(&CONNECTION_POOL);
    lazy_static::initialize(&JWT_SECRET);
    lazy_static::initialize(&PORT);

    setup_logger();

    #[cfg(feature = "auto_migration")]
    {
        log::info!("Running diesel migrations");
        let mut connection =
            acquire_db_connection().expect("Failed to acquire database connection");
        if let Err(e) = connection.run_pending_migrations(MIGRATIONS) {
            panic!("Failed running db migrations: {}", e);
        }
        log::info!("Done running diesel migrations");
    }

    setup_tokio_runtime();
}

pub fn acquire_db_connection() -> Result<DbConnection, Error> {
    CONNECTION_POOL
        .get()
        .map_err(|_| Error::DatabaseConnectionError)
}

pub fn run_retryable_transaction<
    T,
    F: Fn(&mut PgConnection) -> Result<T, TransactionRuntimeError>,
>(
    connection: &mut DbConnection,
    function: F,
) -> Result<T, Error> {
    let mut retry_count: usize = 0;
    loop {
        retry_count += 1;
        let transaction_result = connection
            .build_transaction()
            .read_committed()
            .run::<_, TransactionRuntimeError, _>(&function);

        match transaction_result {
            Err(TransactionRuntimeError::Retry(_)) if retry_count <= 10 => { /* retry max 10 attempts */
            }
            Err(TransactionRuntimeError::Retry(e)) => break Err(e),
            Err(TransactionRuntimeError::Rollback(e)) => break Err(e),
            Ok(res) => break Ok(res),
        }
    }
}

/// Retry a transaction if it fails due to a unique or foreign key constraint violation when concurrent transactions insert the same data
/// or concurrent transaction deletes data used as a foreign key by another transaction.
pub fn retry_on_constraint_violation(e: diesel::result::Error) -> TransactionRuntimeError {
    match e {
        diesel::result::Error::DatabaseError(
            diesel::result::DatabaseErrorKind::UniqueViolation
            | diesel::result::DatabaseErrorKind::ForeignKeyViolation,
            _,
        ) => TransactionRuntimeError::Retry(e.into()),
        _ => TransactionRuntimeError::Rollback(e.into()),
    }
}

/// Start a tokio runtime that runs a warp server.
#[tokio::main]
async fn setup_tokio_runtime() {
    let login_route = warp::path("login")
        .and(warp::post())
        .and(warp::body::json())
        .and_then(auth::login_handler);

    let refresh_login_route = warp::path("refresh-login")
        .and(warp::post())
        .and(warp::cookie("refresh_token"))
        .and_then(auth::refresh_login_handler);

    let try_refresh_login_route = warp::path("try-refresh-login")
        .and(warp::post())
        .and(warp::cookie::optional("refresh_token"))
        .and_then(auth::try_refresh_login_handler);

    let logout_route = warp::path("logout")
        .and(warp::post())
        .and(warp::cookie::optional("refresh_token"))
        .and_then(auth::logout_handler);

    let register_route = warp::path("register")
        .and(warp::post())
        .and(warp::body::json())
        .and_then(auth::register_handler);

    let current_user_info_route = warp::path("current-user-info")
        .and(warp::get())
        .and(auth::with_user())
        .and_then(auth::current_user_info_handler);

    let create_post_route = warp::path("create-post")
        .and(warp::post())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(post::create_post_handler);

    let create_tags_route = warp::path("create-tags")
        .and(warp::post())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(post::create_tags_handler);

    let upsert_tag_route = warp::path("upsert-tag")
        .and(warp::post())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(post::upsert_tag_handler);

    let search_route = warp::path("search")
        .and(warp::get())
        .and(auth::with_user_optional())
        .and(warp::query::<QueryParametersFilter>())
        .and_then(query::search_handler);

    let get_post_route = warp::path("get-post")
        .and(warp::get())
        .and(auth::with_user_optional())
        .and(warp::path::param())
        .and(warp::query::<QueryParametersFilter>())
        .and_then(query::get_post_handler);

    let upload_route = warp::path("upload")
        .and(warp::post())
        .and(warp::path::param())
        .and(auth::with_user())
        .and(warp::header::<Mime>("content-type"))
        .and(warp::body::stream())
        .and_then(data::upload_handler);

    let get_object_route = warp::path("get-object")
        .and(warp::get())
        .and(warp::path::param())
        .and(warp::header::optional::<String>("Range"))
        .and_then(data::get_object_handler);

    let get_object_head_route = warp::path("get-object")
        .and(warp::head())
        .and(warp::path::param())
        .and(warp::header::optional::<String>("Range"))
        .and_then(data::get_object_head_handler);

    let create_broker_route = warp::path("create-broker")
        .and(warp::post())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(data::create_broker_handler);

    let get_brokers_route = warp::path("get-brokers")
        .and(warp::get())
        .and(auth::with_user_optional())
        .and_then(data::get_brokers_handler);

    let find_tag_route = warp::path("find-tag")
        .and(warp::get())
        .and(warp::path::param())
        .and_then(post::find_tag_handler);

    let create_user_group_route = warp::path("create-user-group")
        .and(warp::post())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(perms::create_user_group_handler);

    let get_user_groups_route = warp::path("get-user-groups")
        .and(warp::get())
        .and(auth::with_user_optional())
        .and_then(perms::get_user_groups_handler);

    let get_current_user_groups_route = warp::path("get-current-user-groups")
        .and(warp::get())
        .and(auth::with_user())
        .and_then(perms::get_current_user_groups_handler);

    let edit_post_route = warp::path("edit-post")
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(post::edit_post_handler);

    let routes = login_route
        .or(refresh_login_route)
        .or(try_refresh_login_route)
        .or(logout_route)
        .or(register_route)
        .or(current_user_info_route)
        .or(create_post_route)
        .or(create_tags_route)
        .or(upsert_tag_route)
        .or(search_route)
        .or(get_post_route)
        .or(upload_route)
        .or(get_object_route)
        .or(get_object_head_route)
        .or(create_broker_route)
        .or(get_brokers_route)
        .or(find_tag_route)
        .or(create_user_group_route)
        .or(get_user_groups_route)
        .or(get_current_user_groups_route)
        .or(edit_post_route);

    let filter = routes
        .recover(error::handle_rejection)
        .with(warp::log("filebroker::api"));

    #[cfg(debug_assertions)]
    let filter = filter.with(
        warp::cors()
            .allow_any_origin()
            .allow_header("content-type")
            .allow_header("Authorization")
            .allow_credentials(true)
            .allow_method(warp::http::Method::DELETE)
            .allow_method(warp::http::Method::GET)
            .allow_method(warp::http::Method::OPTIONS)
            .allow_method(warp::http::Method::PATCH)
            .allow_method(warp::http::Method::POST)
            .allow_method(warp::http::Method::PATCH),
    );

    if CERT_PATH.is_some() && KEY_PATH.is_some() {
        warp::serve(filter)
            .tls()
            .cert_path(CERT_PATH.as_ref().unwrap())
            .key_path(KEY_PATH.as_ref().unwrap())
            .run(([0, 0, 0, 0], *PORT))
            .await;
    } else {
        warp::serve(filter).run(([0, 0, 0, 0], *PORT)).await;
    }
}

fn setup_logger() {
    // create logs dir as fern does not appear to handle that itself
    if !std::path::Path::new("logs/").exists() {
        std::fs::create_dir("logs").expect("Failed to create logs/ directory");
    }

    let logging_level = if cfg!(debug_assertions) {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Info
    };

    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{}]{}[{}] {}",
                record.level(),
                chrono::Local::now().format("[%Y-%m-%d %H:%M:%S]"),
                record.target(),
                message
            ))
        })
        .level(logging_level)
        .level_for("filebroker::api", logging_level)
        .chain(std::io::stdout())
        .chain(fern::DateBased::new("logs/", "logs_%Y-%m-%d.log"))
        .apply()
        .expect("Failed to set up logging");
}
