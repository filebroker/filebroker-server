#[macro_use]
extern crate diesel;
use crate::server::run_warp_server;
use chrono::Utc;
use clokwerk::{Job, Scheduler};
use diesel::{ConnectionError, ConnectionResult};
use diesel_async::{
    AsyncPgConnection,
    pg::TransactionBuilder,
    pooled_connection::{
        AsyncDieselConnectionManager, ManagerConfig,
        deadpool::{Object, Pool},
    },
};
use dotenvy::dotenv;
use error::{Error, TransactionRuntimeError};
use futures::{FutureExt, future::BoxFuture};
use lazy_static::lazy_static;
use std::{
    str::FromStr,
    sync::{Arc, atomic::AtomicBool},
    thread::JoinHandle,
    time::Duration,
};
use tokio::runtime::Runtime;
use url::Url;

#[cfg(feature = "auto_migration")]
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

mod auth;
mod broker;
mod data;
mod error;
mod mail;
mod model;
mod perms;
mod post;
mod query;
mod schema;
mod server;
mod tag;
mod task;
mod user_group;
mod util;

pub type DbConnection = Object<AsyncPgConnection>;

lazy_static! {
    pub static ref DATABASE_URL: String = std::env::var("FILEBROKER_DATABASE_URL").expect(
        "Missing environment variable FILEBROKER_DATABASE_URL must be set to connect to postgres"
    );
    pub static ref PG_ENABLE_SSL: bool = std::env::var("FILEBROKER_PG_ENABLE_SSL")
        .map(|val| val
            .parse::<bool>()
            .expect("FILEBROKER_PG_ENABLE_SSL is not a valid boolean"))
        .unwrap_or_default();
    pub static ref CONNECTION_POOL: Pool<AsyncPgConnection> = {
        let database_connection_manager = if *PG_ENABLE_SSL {
            let mut config = ManagerConfig::default();
            config.custom_setup = Box::new(establish_pg_ssl_connection);
            AsyncDieselConnectionManager::<AsyncPgConnection>::new_with_config(
                DATABASE_URL.clone(),
                config,
            )
        } else {
            AsyncDieselConnectionManager::<AsyncPgConnection>::new(DATABASE_URL.clone())
        };
        let max_db_connections = std::env::var("FILEBROKER_MAX_DB_CONNECTIONS")
            .unwrap_or_else(|_| String::from("25"))
            .parse::<usize>()
            .expect("FILEBROKER_MAX_DB_CONNECTIONS is not a valid usize");
        Pool::builder(database_connection_manager)
            .max_size(max_db_connections)
            .build()
            .expect("Failed to initialise connection pool")
    };
    pub static ref JWT_SECRET: u64 = {
        let secret_str = std::env::var("FILEBROKER_JWT_SECRET")
            .expect("Missing environment variable FILEBROKER_JWT_SECRET must be set to generate JWT tokens.");
        u64::from_str(&secret_str).expect("FILEBROKER_JWT_SECRET var is not a valid u64 value")
    };
    pub static ref PORT: u16 = {
        let port_str = std::env::var("FILEBROKER_API_PORT")
            .expect("Missing environment variable FILEBROKER_API_PORT must be set.");
        u16::from_str(&port_str).expect("FILEBROKER_API_PORT var is not a valid u16 value")
    };
    pub static ref CERT_PATH: Option<String> = std::env::var("FILEBROKER_CERT_PATH").ok();
    pub static ref KEY_PATH: Option<String> = std::env::var("FILEBROKER_KEY_PATH").ok();
    pub static ref API_BASE_URL: Url = std::env::var("FILEBROKER_API_BASE_URL")
        .map(|url| Url::parse(&url).expect("FILEBROKER_API_BASE_URL is not valid"))
        .unwrap_or_else(|_| {
            let protocol = if CERT_PATH.is_some() { "https" } else { "http" };

            Url::parse(&format!("{protocol}://localhost:{}/", *PORT)).unwrap()
        });
    pub static ref CONCURRENT_VIDEO_TRANSCODE_LIMIT: Option<usize> =
        std::env::var("FILEBROKER_CONCURRENT_VIDEO_TRANSCODE_LIMIT")
            .map(|v| v
                .parse::<usize>()
                .expect("FILEBROKER_CONCURRENT_VIDEO_TRANSCODE_LIMIT is not a valid usize"))
            .ok();
    pub static ref HOST_BASE_PATH: Option<String> = std::env::var("FILEBROKER_HOST_BASE_PATH").ok();
    pub static ref PG_SSL_CERT_PATH: Option<String> =
        std::env::var("FILEBROKER_PG_SSL_CERT_PATH").ok();
    pub static ref HTTP_SHUTDOWN_TIMEOUT: Duration = {
        std::env::var("FILEBROKER_HTTP_SHUTDOWN_TIMEOUT_SECONDS")
            .map(|val| {
                Duration::from_secs(
                    u64::from_str(&val)
                        .expect("FILEBROKER_HTTP_SHUTDOWN_TIMEOUT_SECONDS must be a valid u64"),
                )
            })
            .unwrap_or_else(|_| Duration::from_secs(30 * 60))
    };
}

#[cfg(feature = "auto_migration")]
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!();

fn main() {
    dotenvy::from_path(
        std::env::current_dir()
            .map(|wd| wd.join(".env.local"))
            .unwrap(),
    )
    .ok();
    dotenvy::from_path(
        std::env::current_dir()
            .map(|wd| wd.join(".env.secret"))
            .unwrap(),
    )
    .ok();
    dotenv().ok();

    // initialise certain lazy statics on startup
    lazy_static::initialize(&CONNECTION_POOL);
    lazy_static::initialize(&JWT_SECRET);
    lazy_static::initialize(&PORT);
    lazy_static::initialize(&API_BASE_URL);
    lazy_static::initialize(&CONCURRENT_VIDEO_TRANSCODE_LIMIT);
    lazy_static::initialize(&mail::TEMPLATES);
    lazy_static::initialize(&mail::MAILS_PER_HOUR_LIMIT);
    lazy_static::initialize(&mail::RATE_LIMITER);

    setup_logger();

    #[cfg(feature = "auto_migration")]
    {
        use crate::diesel::Connection;
        log::info!("Running diesel migrations");
        let mut connection = diesel::pg::PgConnection::establish(&DATABASE_URL)
            .expect("Failed to acquire database connection");
        if let Err(e) = connection.run_pending_migrations(MIGRATIONS) {
            panic!("Failed running db migrations: {}", e);
        }
        log::info!("Done running diesel migrations");
    }

    let task_scheduler = start_task_scheduler_runtime(configure_scheduler());

    let http_worker_rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to start http server worker runtime");
    let http_worker_rt = Arc::new(http_worker_rt);
    setup_tokio_runtime(http_worker_rt.clone());
    let _ = task_scheduler.join();
}

pub async fn acquire_db_connection() -> Result<DbConnection, Error> {
    CONNECTION_POOL
        .get()
        .await
        .map_err(|e| Error::DatabaseConnectionError(e.to_string()))
}

pub trait AsyncFunc<T, R>:
    AsyncFnOnce(T) -> R + FnOnce(T) -> <Self as AsyncFunc<T, R>>::Fut
{
    type Fut: Future<Output = R>;
}

impl<F, T, Fut, R> AsyncFunc<T, R> for F
where
    F: AsyncFnOnce(T) -> R + FnOnce(T) -> Fut,
    Fut: Future<Output = R>,
{
    type Fut = Fut;
}

pub async fn run_retryable_transaction<'a, T, F>(
    connection: &mut AsyncPgConnection,
    function: F,
) -> Result<T, Error>
where
    T: Send + 'a,
    F: Clone + Send + 'a,
    for<'r> F: AsyncFnOnce(&'r mut AsyncPgConnection) -> Result<T, TransactionRuntimeError>
        + AsyncFunc<&'r mut AsyncPgConnection, Result<T, TransactionRuntimeError>>,
    for<'r> <F as AsyncFunc<&'r mut AsyncPgConnection, Result<T, TransactionRuntimeError>>>::Fut:
        Send,
    TransactionRuntimeError: From<diesel::result::Error>,
{
    run_retryable_transaction_with_level(connection.build_transaction().read_committed(), function)
        .await
}

pub async fn run_serializable_transaction<'a, T, F>(
    connection: &mut AsyncPgConnection,
    function: F,
) -> Result<T, Error>
where
    T: Send + 'a,
    F: Clone + Send + 'a,
    for<'r> F: AsyncFnOnce(&'r mut AsyncPgConnection) -> Result<T, TransactionRuntimeError>
        + AsyncFunc<&'r mut AsyncPgConnection, Result<T, TransactionRuntimeError>>,
    for<'r> <F as AsyncFunc<&'r mut AsyncPgConnection, Result<T, TransactionRuntimeError>>>::Fut:
        Send,
    TransactionRuntimeError: From<diesel::result::Error>,
{
    run_retryable_transaction_with_level(connection.build_transaction().serializable(), function)
        .await
}

pub async fn run_repeatable_read_transaction<'a, T, F>(
    connection: &mut AsyncPgConnection,
    function: F,
) -> Result<T, Error>
where
    T: Send + 'a,
    F: Clone + Send + 'a,
    for<'r> F: AsyncFnOnce(&'r mut AsyncPgConnection) -> Result<T, TransactionRuntimeError>
        + AsyncFunc<&'r mut AsyncPgConnection, Result<T, TransactionRuntimeError>>,
    for<'r> <F as AsyncFunc<&'r mut AsyncPgConnection, Result<T, TransactionRuntimeError>>>::Fut:
        Send,
    TransactionRuntimeError: From<diesel::result::Error>,
{
    run_retryable_transaction_with_level(connection.build_transaction().repeatable_read(), function)
        .await
}

async fn run_retryable_transaction_with_level<'a, T, F>(
    mut transaction_builder: TransactionBuilder<'a, AsyncPgConnection>,
    function: F,
) -> Result<T, Error>
where
    T: Send + 'a,
    F: Clone + Send + 'a,
    for<'r> F: AsyncFnOnce(&'r mut AsyncPgConnection) -> Result<T, TransactionRuntimeError>
        + AsyncFunc<&'r mut AsyncPgConnection, Result<T, TransactionRuntimeError>>,
    for<'r> <F as AsyncFunc<&'r mut AsyncPgConnection, Result<T, TransactionRuntimeError>>>::Fut:
        Send,
    TransactionRuntimeError: From<diesel::result::Error>,
{
    let mut retry_count: usize = 0;
    loop {
        retry_count += 1;

        let transaction_result = transaction_builder
            .run::<_, TransactionRuntimeError, _>(function.clone())
            .await;

        match transaction_result {
            Err(TransactionRuntimeError::Retry(_)) if retry_count <= 10 => {
                /* retry max 10 attempts */
                log::warn!(
                    "Retrying transaction after serialization failure, constraint violation or other Retry Error"
                );
            }
            Err(TransactionRuntimeError::Retry(e)) => {
                log::error!("Maximum transaction retry count reached, aborting transaction: {e}");
                break Err(e);
            }
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
            | diesel::result::DatabaseErrorKind::ForeignKeyViolation
            | diesel::result::DatabaseErrorKind::SerializationFailure,
            _,
        ) => TransactionRuntimeError::Retry(e.into()),
        _ => TransactionRuntimeError::Rollback(e.into()),
    }
}

pub fn retry_on_serialization_failure(e: diesel::result::Error) -> TransactionRuntimeError {
    match e {
        diesel::result::Error::DatabaseError(
            diesel::result::DatabaseErrorKind::SerializationFailure,
            _,
        ) => TransactionRuntimeError::Retry(e.into()),
        _ => TransactionRuntimeError::Rollback(e.into()),
    }
}

#[cfg(feature = "auto_migration")]
async fn run_startup_migration_tasks() -> Result<(), Error> {
    recompile_tag_auto_match_conditions().await?;
    Ok(())
}

#[cfg(feature = "auto_migration")]
async fn recompile_tag_auto_match_conditions() -> Result<(), Error> {
    use crate::model::{Tag, TagCategory};
    use crate::tag::auto_matching::{AutoMatchTarget, compile_tag_auto_match_condition};
    use diesel::{BoolExpressionMethods, ExpressionMethods, QueryDsl};
    use diesel_async::RunQueryDsl;

    let mut connection = acquire_db_connection().await?;
    run_serializable_transaction(&mut connection, |connection| async {
        diesel::update(schema::tag::table)
            .filter(
                schema::tag::compiled_auto_match_condition_post
                    .is_not_null()
                    .or(schema::tag::compiled_auto_match_condition_collection.is_not_null()),
            )
            .set((
                schema::tag::compiled_auto_match_condition_post.eq(None::<String>),
                schema::tag::compiled_auto_match_condition_collection.eq(None::<String>),
            ))
            .execute(connection)
            .await?;

        let tags_with_conditions = schema::tag::table
            .left_join(schema::tag_category::table)
            .filter(
                schema::tag::auto_match_condition_post
                    .is_not_null()
                    .or(schema::tag::auto_match_condition_collection.is_not_null())
                    .or(schema::tag_category::auto_match_condition_post.is_not_null())
                    .or(schema::tag_category::auto_match_condition_collection.is_not_null()),
            )
            .load::<(Tag, Option<TagCategory>)>(connection)
            .await?;

        for (tag, tag_category) in tags_with_conditions {
            let tag_pk = tag.pk;
            let compiled_tag_auto_match_condition_post = compile_tag_auto_match_condition(
                tag.clone(),
                tag_category.clone(),
                AutoMatchTarget::Post,
            )?;
            let compiled_tag_auto_match_condition_collection =
                compile_tag_auto_match_condition(tag, tag_category, AutoMatchTarget::Collection)?;

            if compiled_tag_auto_match_condition_post.is_some()
                || compiled_tag_auto_match_condition_collection.is_some()
            {
                diesel::update(schema::tag::table)
                    .filter(schema::tag::pk.eq(tag_pk))
                    .set((
                        schema::tag::compiled_auto_match_condition_post
                            .eq(compiled_tag_auto_match_condition_post),
                        schema::tag::compiled_auto_match_condition_collection
                            .eq(compiled_tag_auto_match_condition_collection),
                    ))
                    .execute(connection)
                    .await?;
            }
        }

        Ok(())
    })
    .await
}

/// Start a tokio runtime that runs a warp server.
#[tokio::main(flavor = "current_thread")]
async fn setup_tokio_runtime(http_worker_rt: Arc<Runtime>) {
    #[cfg(feature = "auto_migration")]
    {
        log::info!("Running startup migration tasks");
        if let Err(e) = crate::run_startup_migration_tasks().await {
            log::error!("Error running startup migration tasks: {e}");
            crate::server::shutdown_server().await;
            return;
        }
        log::info!("Done running startup migration tasks");
    }

    run_warp_server(http_worker_rt).await;
}

// enable TLS for AsyncPgConnection, see https://github.com/weiznich/diesel_async/blob/main/examples/postgres/pooled-with-rustls

fn establish_pg_ssl_connection(config: &str) -> BoxFuture<'_, ConnectionResult<AsyncPgConnection>> {
    let fut = async {
        // We first set up the way we want rustls to work.
        let rustls_config = rustls::ClientConfig::builder()
            .with_root_certificates(server::root_certs())
            .with_no_client_auth();
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(rustls_config);
        let (client, conn) = tokio_postgres::connect(config, tls)
            .await
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("Database connection: {e}");
            }
        });
        AsyncPgConnection::try_from(client).await
    };
    fut.boxed()
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
        .level(log::LevelFilter::Info)
        .level_for("filebroker", logging_level)
        .level_for("filebroker_server", logging_level)
        .chain(std::io::stdout())
        .chain(fern::DateBased::new("logs/", "logs_%Y-%m-%d.log"))
        .apply()
        .expect("Failed to set up logging");
}

fn configure_scheduler() -> Scheduler<Utc> {
    let mut scheduler = Scheduler::with_tz(Utc);
    scheduler.every(clokwerk::Interval::Minutes(40)).run(|| {
        task::submit_task(
            "generate_missing_hls_streams",
            task::object_tasks::generate_missing_hls_streams,
        )
    });
    scheduler.every(clokwerk::Interval::Minutes(30)).run(|| {
        task::submit_task(
            "generate_missing_thumbnails",
            task::object_tasks::generate_missing_thumbnails,
        )
    });
    scheduler.every(clokwerk::Interval::Minutes(20)).run(|| {
        task::submit_task(
            "load_missing_object_metadata",
            task::object_tasks::load_missing_object_metadata,
        )
    });
    scheduler.every(clokwerk::Interval::Hours(1)).run(|| {
        task::submit_task("clear_old_object_locks", task::clear_old_object_locks);
    });
    scheduler.every(clokwerk::Interval::Minutes(30)).run(|| {
        task::submit_task("clear_old_tokens", task::clear_old_tokens);
    });
    scheduler.every(clokwerk::Interval::Minutes(15)).run(|| {
        task::submit_task(
            "execute_deferred_s3_object_deletions",
            task::object_tasks::execute_deferred_s3_object_deletions,
        );
    });
    scheduler.every(clokwerk::Interval::Minutes(10)).run(|| {
        task::submit_task(
            "run_apply_auto_tags_tasks",
            task::tag_tasks::run_apply_auto_tags_tasks,
        );
    });
    scheduler
        .every(clokwerk::Interval::Days(1))
        .at("03:00")
        .run(|| {
            task::submit_task(
                "run_reconcile_broker_quota_usage_tasks",
                task::broker_tasks::run_reconcile_broker_quota_usage_tasks,
            );
        });
    scheduler
        .every(clokwerk::Interval::Days(1))
        .at("01:00")
        .run(|| {
            task::submit_task(
                "run_broker_quota_usage_audits",
                task::broker_tasks::run_broker_quota_usage_audits,
            );
        });

    scheduler
}

fn start_task_scheduler_runtime(scheduler: Scheduler<Utc>) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name(String::from("task_scheduler"))
        .spawn(move || {
            let mut task_scheduler_sentinel = TaskSchedulerSentinel { scheduler };

            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .thread_name("task_tokio_worker")
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(e) => {
                    eprintln!("Failed to start task scheduler runtime: {e}");
                    std::process::exit(1);
                }
            };

            runtime.block_on(async {
                loop {
                    if AtomicBool::load(&task::IS_SHUTDOWN, std::sync::atomic::Ordering::Relaxed) {
                        return;
                    }
                    task_scheduler_sentinel.scheduler.run_pending();
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            });

            runtime.shutdown_timeout(Duration::from_secs(10));
        })
        .expect("Failed to spawn task scheduler thread")
}

struct TaskSchedulerSentinel {
    scheduler: Scheduler<Utc>,
}

impl Drop for TaskSchedulerSentinel {
    fn drop(&mut self) {
        if std::thread::panicking() {
            start_task_scheduler_runtime(configure_scheduler());
        }
    }
}
