#[macro_use]
extern crate diesel;
#[cfg(feature = "auto_migration")]
#[macro_use]
extern crate diesel_migrations;

use diesel::{
    r2d2::{self, ConnectionManager, Pool, PooledConnection},
    PgConnection,
};
use dotenv::dotenv;
use error::{Error, TransactionRuntimeError};
use lazy_static::lazy_static;
use std::str::FromStr;
use warp::Filter;

mod auth;
mod error;
mod model;
mod post;
mod schema;

pub type DbConnection = PooledConnection<ConnectionManager<PgConnection>>;

lazy_static! {
    pub static ref CONNECTION_POOL: Pool<ConnectionManager<PgConnection>> = {
        let database_url = std::env::var("DATABASE_URL")
            .expect("Missing environment variable DATABASE_URL must be set to connect to postgres");
        let database_connection_manager =
            r2d2::ConnectionManager::<PgConnection>::new(database_url);
        r2d2::Builder::new()
            .min_idle(Some(10))
            .max_size(50)
            .build(database_connection_manager)
            .expect("Failed to initialise connection pool")
    };
    pub static ref JWT_SECRET: u64 = {
        let secret_str = std::env::var("JWT_SECRET")
            .expect("Missing environment variable JWT_SECRET must be set to generate JWT tokens.");
        u64::from_str(&secret_str).expect("JWT_SECRET var is not a valid u64 value")
    };
    pub static ref PORT: u16 = {
        let port_str = std::env::var("API_PORT")
            .expect("Missing environment variable API_PORT must be set to generate JWT tokens.");
        u16::from_str(&port_str).expect("API_PORT var is not a valid u16 value")
    };
}

#[cfg(feature = "auto_migration")]
diesel_migrations::embed_migrations!();

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
        let connection = acquire_db_connection().expect("Failed to acquire database connection");
        if let Err(e) = embedded_migrations::run_with_output(&connection, &mut std::io::stdout()) {
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

pub fn run_retryable_transaction<T, F: Fn() -> Result<T, TransactionRuntimeError>>(
    connection: &DbConnection,
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

    let routes = login_route
        .or(refresh_login_route)
        .or(try_refresh_login_route)
        .or(register_route)
        .or(current_user_info_route)
        .or(create_post_route)
        .or(create_tags_route)
        .or(upsert_tag_route);

    let filter = routes
        .recover(error::handle_rejection)
        .with(warp::log("filebroker::api"));

    #[cfg(debug_assertions)]
    let filter = filter.with(
        warp::cors()
            .allow_origin("http://localhost:3000")
            .allow_header("content-type")
            .allow_credentials(true)
            .allow_method(warp::http::Method::DELETE)
            .allow_method(warp::http::Method::GET)
            .allow_method(warp::http::Method::OPTIONS)
            .allow_method(warp::http::Method::PATCH)
            .allow_method(warp::http::Method::POST)
            .allow_method(warp::http::Method::PATCH),
    );

    warp::serve(filter).run(([0, 0, 0, 0], *PORT)).await;
}

fn setup_logger() {
    // create logs dir as fern does not appear to handle that itself
    if !std::path::Path::new("logs/").exists() {
        std::fs::create_dir("logs").expect("Failed to create logs/ directory");
    }

    let (logging_level, api_logging_level) = if cfg!(debug_assertions) {
        (log::LevelFilter::Debug, log::LevelFilter::Debug)
    } else {
        (log::LevelFilter::Info, log::LevelFilter::Warn)
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
        .level_for("filebroker::api", api_logging_level)
        .chain(std::io::stdout())
        .chain(fern::DateBased::new("logs/", "logs_%Y-%W.log"))
        .apply()
        .expect("Failed to set up logging");
}
