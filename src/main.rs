#[macro_use]
extern crate diesel;
use chrono::Utc;
use clokwerk::Scheduler;
use diesel::{ConnectionError, ConnectionResult};
use diesel_async::{
    pg::TransactionBuilder,
    pooled_connection::{
        deadpool::{Object, Pool},
        AsyncDieselConnectionManager, ManagerConfig,
    },
    scoped_futures::ScopedBoxFuture,
    AsyncPgConnection,
};
#[cfg(feature = "auto_migration")]
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};

use dotenvy::dotenv;
use error::{Error, TransactionRuntimeError};
use futures::{future::BoxFuture, Future, FutureExt, StreamExt, TryFuture};
use lazy_static::lazy_static;
use mime::Mime;
use query::QueryParametersFilter;
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer},
    ServerConfig,
};
use std::{
    convert::Infallible,
    fs,
    future::ready,
    io,
    pin::Pin,
    str::FromStr,
    sync::{atomic::AtomicBool, Arc},
    thread::JoinHandle,
    time::Duration,
};
use tags::GetTagsFilter;
use tls_listener::{AsyncTls, TlsListener};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    runtime::Runtime,
};
use tokio_rustls::{server::TlsStream, TlsAcceptor};
use url::Url;
use warp::{
    host::Authority,
    hyper::{
        self,
        server::{
            accept::{self, Accept},
            conn::AddrIncoming,
        },
        service::Service,
        Body, Request, Server,
    },
    Filter, Reply,
};

use crate::util::OptFmt;

mod auth;
mod data;
mod error;
mod mail;
mod model;
mod perms;
mod post;
mod query;
mod schema;
mod tags;
mod task;
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

pub async fn run_retryable_transaction<'b, 'c, T: 'b, F>(
    connection: &mut AsyncPgConnection,
    function: F,
) -> Result<T, Error>
where
    F: for<'r> FnOnce(
            &'r mut AsyncPgConnection,
        ) -> ScopedBoxFuture<'b, 'r, Result<T, TransactionRuntimeError>>
        + Clone
        + Send
        + 'c,
{
    run_retryable_transaction_with_level(connection.build_transaction().read_committed(), function)
        .await
}

pub async fn run_serializable_transaction<'b, 'c, T: 'b, F>(
    connection: &mut AsyncPgConnection,
    function: F,
) -> Result<T, Error>
where
    F: for<'r> FnOnce(
            &'r mut AsyncPgConnection,
        ) -> ScopedBoxFuture<'b, 'r, Result<T, TransactionRuntimeError>>
        + Clone
        + Send
        + 'c,
{
    run_retryable_transaction_with_level(connection.build_transaction().serializable(), function)
        .await
}

async fn run_retryable_transaction_with_level<'b, 'c, T, F>(
    mut transaction_builder: TransactionBuilder<'c, AsyncPgConnection>,
    function: F,
) -> Result<T, Error>
where
    T: 'b,
    F: for<'r> FnOnce(
            &'r mut AsyncPgConnection,
        ) -> ScopedBoxFuture<'b, 'r, Result<T, TransactionRuntimeError>>
        + Clone
        + Send
        + 'c,
{
    let mut retry_count: usize = 0;
    loop {
        retry_count += 1;
        let transaction_result = transaction_builder
            .run::<_, TransactionRuntimeError, _>(function.clone())
            .await;

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

pub fn retry_on_serialization_failure(e: diesel::result::Error) -> TransactionRuntimeError {
    match e {
        diesel::result::Error::DatabaseError(
            diesel::result::DatabaseErrorKind::SerializationFailure,
            _,
        ) => TransactionRuntimeError::Retry(e.into()),
        _ => TransactionRuntimeError::Rollback(e.into()),
    }
}

/// Start a tokio runtime that runs a warp server.
#[tokio::main(flavor = "current_thread")]
async fn setup_tokio_runtime(http_worker_rt: Arc<Runtime>) {
    let login_route = warp::path("login")
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::filters::addr::remote())
        .and_then(auth::login_handler);

    let refresh_login_route = warp::path("refresh-login")
        .and(warp::post())
        .and(warp::cookie("refresh_token"))
        .and_then(auth::refresh_login_handler);

    let refresh_token_route = warp::path("refresh-token")
        .and(warp::post())
        .and(warp::path::param())
        .and_then(auth::refresh_login_handler);

    let try_refresh_login_route = warp::path("try-refresh-login")
        .and(warp::post())
        .and(warp::cookie::optional("refresh_token"))
        .and_then(auth::try_refresh_login_handler);

    let try_refresh_token_route = warp::path("try-refresh-token")
        .and(warp::post())
        .and(warp::path::param().map(Some))
        .and_then(auth::try_refresh_login_handler);

    let logout_route = warp::path("logout")
        .and(warp::post())
        .and(warp::cookie::optional("refresh_token"))
        .and_then(auth::logout_handler);

    let register_route = warp::path("register")
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::filters::addr::remote())
        .and(warp::filters::header::header::<Authority>("Host"))
        .and_then(auth::register_handler);

    let current_user_info_route = warp::path("current-user-info")
        .and(warp::get())
        .and(auth::with_user())
        .and_then(auth::current_user_info_handler);

    let create_post_route = warp::path("create-post")
        .and(warp::post())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(post::create::create_post_handler);

    let create_tags_route = warp::path("create-tags")
        .and(warp::post())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(tags::create_tags_handler);

    let upsert_tag_route = warp::path("upsert-tag")
        .and(warp::post())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(tags::upsert_tag_handler);

    let search_route = warp::path("search")
        .and(warp::get())
        .and(auth::with_user_optional())
        .and(
            warp::path::param::<String>()
                .map(Some)
                .or_else(|_| async { Ok::<(Option<String>,), std::convert::Infallible>((None,)) }),
        )
        .and(
            warp::path::param::<String>()
                .map(Some)
                .or_else(|_| async { Ok::<(Option<String>,), std::convert::Infallible>((None,)) }),
        )
        .and(warp::query::<QueryParametersFilter>())
        .and_then(query::search_handler);

    let get_post_route = warp::path("get-post")
        .and(warp::get())
        .and(auth::with_user_optional())
        .and(warp::path::param())
        .and(
            warp::path::param::<i64>()
                .map(Some)
                .or_else(|_| async { Ok::<(Option<i64>,), std::convert::Infallible>((None,)) }),
        )
        .and(warp::query::<QueryParametersFilter>())
        .and_then(query::get_post_handler);

    let get_posts_route = warp::path("get-posts")
        .and(warp::get())
        .and(auth::with_user_optional())
        .and(warp::path::param())
        .and_then(query::get_posts_handler);

    let upload_route = warp::path("upload")
        .and(warp::post())
        .and(warp::path::param())
        .and(auth::with_user())
        .and(warp::header::<Mime>("content-type"))
        .and(warp::header::<usize>("Filebroker-Upload-Size"))
        .and(warp::header::optional::<bool>("Disable-HLS-Transcoding"))
        .and(warp::body::stream())
        .and_then(data::upload_handler);

    let get_object_metadata_route = warp::path("get-object-metadata")
        .and(warp::get())
        .and(warp::filters::path::peek())
        .and_then(data::get_object_metadata_handler);

    let get_object_route = warp::path("get-object")
        .and(warp::get())
        .and(warp::filters::path::peek())
        .and(warp::header::optional::<String>("Range"))
        .and_then(data::get_object_handler);

    let get_object_head_route = warp::path("get-object")
        .and(warp::head())
        .and(warp::filters::path::peek())
        .and(warp::header::optional::<String>("Range"))
        .and_then(data::get_object_head_handler);

    let create_broker_route = warp::path("create-broker")
        .and(warp::post())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(data::create_broker_handler);

    let get_brokers_route = warp::path("get-brokers")
        .and(warp::get())
        .and(auth::with_user())
        .and_then(data::get_brokers_handler);

    let find_tag_route = warp::path("find-tag")
        .and(warp::get())
        .and(warp::path::param())
        .and_then(tags::find_tag_handler);

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
        .and_then(post::update::edit_post_handler);

    let analyze_query_route = warp::path("analyze-query")
        .and(warp::post())
        .and(warp::body::json())
        .and_then(query::analyze_query_handler);

    let check_username_route = warp::path("check-username")
        .and(warp::get())
        .and(warp::path::param())
        .and_then(auth::check_username_handler);

    let confirm_email_route = warp::path("confirm-email")
        .and(warp::post())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(auth::confirm_email_handler);

    let edit_user_route = warp::path("edit-user")
        .and(warp::post())
        .and(warp::body::json())
        .and(auth::with_user())
        .and(warp::filters::header::header::<Authority>("Host"))
        .and_then(auth::edit_user_handler);

    let send_email_confirmation_link_route = warp::path("send-email-confirmation-link")
        .and(warp::post())
        .and(auth::with_user())
        .and(warp::filters::header::header::<Authority>("Host"))
        .and_then(auth::send_email_confirmation_link_handler);

    let change_password_route = warp::path("change-password")
        .and(warp::post())
        .and(warp::body::json())
        .and(auth::with_user())
        .and(warp::filters::addr::remote())
        .and_then(auth::change_password_handler);

    let send_password_reset_route = warp::path("send-password-reset")
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::filters::addr::remote())
        .and_then(auth::send_password_reset_handler);

    let reset_password_route = warp::path("reset-password")
        .and(warp::post())
        .and(warp::body::json())
        .and_then(auth::reset_password_handler);

    let create_post_collection_route = warp::path("create-collection")
        .and(warp::post())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(post::create::create_post_collection_handler);

    let get_post_collection_route = warp::path("get-collection")
        .and(warp::get())
        .and(auth::with_user_optional())
        .and(warp::path::param())
        .and_then(query::get_post_collection_handler);

    let edit_post_collection_route = warp::path("edit-collection")
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(post::update::edit_post_collection_handler);

    let delete_posts_route = warp::path("delete-posts")
        .and(warp::post())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(post::delete::delete_posts_handler);

    let delete_post_collections_route = warp::path("delete-collections")
        .and(warp::post())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(post::delete::delete_posts_collections_handler);

    let get_tag_route = warp::path("get-tag")
        .and(warp::get())
        .and(warp::path::param::<i64>())
        .and_then(tags::get_tag_handler);

    let get_tags_route = warp::path("get-tags")
        .and(warp::get())
        .and(warp::query::<GetTagsFilter>())
        .and_then(tags::get_tags_handler);

    let update_tag_route = warp::path("update-tag")
        .and(warp::post())
        .and(warp::path::param::<i64>())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(tags::update_tag_handler);

    let get_tag_hierarchy_route = warp::path("get-tag-hierarchy")
        .and(warp::get())
        .and(warp::path::param::<i64>())
        .and_then(tags::get_tag_hierarchy_handler);

    let routes = login_route
        .or(refresh_login_route)
        .or(refresh_token_route)
        .or(try_refresh_login_route)
        .or(try_refresh_token_route)
        .boxed()
        .or(logout_route)
        .or(register_route)
        .or(current_user_info_route)
        .or(create_post_route)
        .or(create_tags_route)
        .boxed()
        .or(upsert_tag_route)
        .or(search_route)
        .or(get_post_route)
        .or(get_posts_route)
        .or(upload_route)
        .or(get_object_metadata_route)
        .or(get_object_route)
        .boxed()
        .or(get_object_head_route)
        .or(create_broker_route)
        .or(get_brokers_route)
        .or(find_tag_route)
        .or(create_user_group_route)
        .boxed()
        .or(get_user_groups_route)
        .or(get_current_user_groups_route)
        .or(edit_post_route)
        .or(analyze_query_route)
        .or(check_username_route)
        .boxed()
        .or(confirm_email_route)
        .or(edit_user_route)
        .or(send_email_confirmation_link_route)
        .or(change_password_route)
        .or(send_password_reset_route)
        .boxed()
        .or(reset_password_route)
        .or(create_post_collection_route)
        .or(get_post_collection_route)
        .or(edit_post_collection_route)
        .or(delete_posts_route)
        .boxed()
        .or(delete_post_collections_route)
        .or(get_tag_route)
        .or(get_tags_route)
        .or(update_tag_route)
        .or(get_tag_hierarchy_route)
        .boxed();

    let filter = routes
        .recover(error::handle_rejection)
        .with(warp::log::custom(|info| {
            let log_level = if info.elapsed().as_secs() >= 10 && !info.path().starts_with("/upload")
            {
                log::Level::Warn
            } else if info.elapsed().as_millis() >= 250 || !info.status().is_success() {
                log::Level::Info
            } else {
                log::Level::Debug
            };

            log::log!(
                target: "filebroker::api",
                log_level,
                "{} \"{} {} {:?}\" {} \"{}\" \"{}\" {:?}",
                OptFmt(info.remote_addr()),
                info.method(),
                info.path(),
                info.version(),
                info.status().as_u16(),
                OptFmt(info.referer()),
                OptFmt(info.user_agent()),
                info.elapsed(),
            );
        }));

    #[cfg(debug_assertions)]
    let filter = filter.with(
        warp::cors()
            .allow_any_origin()
            .allow_header("content-type")
            .allow_header("Authorization")
            .allow_header("Range")
            .allow_header("if-modified-since")
            .allow_header("Filebroker-Upload-Size")
            .allow_header("Disable-HLS-Transcoding")
            .allow_credentials(true)
            .allow_method(warp::http::Method::DELETE)
            .allow_method(warp::http::Method::GET)
            .allow_method(warp::http::Method::OPTIONS)
            .allow_method(warp::http::Method::PATCH)
            .allow_method(warp::http::Method::POST)
            .allow_method(warp::http::Method::PATCH)
            .allow_method(warp::http::Method::HEAD),
    );

    let incoming =
        AddrIncoming::bind(&([0, 0, 0, 0], *PORT).into()).expect("Failed to bind server to port");
    if CERT_PATH.is_some() && KEY_PATH.is_some() {
        let certs = load_certs(CERT_PATH.as_ref().unwrap()).expect("Failed to load TLS cert");
        let key =
            load_private_key(KEY_PATH.as_ref().unwrap()).expect("Failed to load TLS private key");
        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .expect("Failed to build TLS ServerConfig");
        let acceptor: TlsAcceptor = Arc::new(server_config).into();
        let incoming = TlsListener::new(TlsAcceptorAdapter(acceptor), incoming).filter(|conn| {
            if let Err(e) = conn {
                log::error!("Failed to open TLS connection: {e}");
                ready(false)
            } else {
                ready(true)
            }
        });
        log::info!("Enabled TLS");
        run_server(accept::from_stream(incoming), filter, http_worker_rt)
            .await
            .expect("Failed running server");
    } else {
        run_server(incoming, filter, http_worker_rt)
            .await
            .expect("Failed running server");
    }
}

// cannot use the latest version of tls_listener as it upgraded to hyper 1.0 while warp is still on 0.14, use this to adapt the new tokio_rustls version to the old tls_listener version
#[derive(Clone)]
struct TlsAcceptorAdapter(TlsAcceptor);

impl<C> AsyncTls<C> for TlsAcceptorAdapter
where
    C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = TlsStream<C>;
    type Error = io::Error;
    type AcceptFuture = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send>>;

    fn accept(&self, conn: C) -> Self::AcceptFuture {
        let tls = self.clone();
        Box::pin(async move { TlsAcceptor::accept(&tls.0, conn).await })
    }
}

#[derive(Clone)]
pub struct HttpRequestExecutor {
    worker_rt: Arc<Runtime>,
}

impl<F> hyper::rt::Executor<F> for HttpRequestExecutor
where
    F: Future + Send + 'static,
    <F as futures::Future>::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        self.worker_rt.spawn(fut);
    }
}

async fn run_server<I, F>(
    incoming: I,
    filter: F,
    http_worker_rt: Arc<Runtime>,
) -> Result<(), warp::hyper::Error>
where
    I: Accept,
    I::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    I::Conn: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: Filter + Clone + Send + 'static,
    <F::Future as TryFuture>::Ok: Reply,
{
    let warp_service = warp::service(filter);
    let service_fn = hyper::service::make_service_fn(move |_| {
        let warp_service = warp_service.clone();
        async move {
            let service = hyper::service::service_fn(move |req: Request<Body>| {
                let mut warp_service = warp_service.clone();
                async move { warp_service.call(req).await }
            });
            Ok::<_, Infallible>(service)
        }
    });
    let server = Server::builder(incoming)
        .executor(HttpRequestExecutor {
            worker_rt: http_worker_rt,
        })
        .serve(service_fn);
    log::info!("Server listening on port {}", *PORT);
    let server = server.with_graceful_shutdown(async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl-c signal");
        log::info!("Shutting down server");
        log::info!("Waiting for scheduled tasks to finish");
        tokio::runtime::Handle::current()
            .spawn_blocking(task::shutdown_join)
            .await
            .expect("Failed to join scheduled tasks");
        log::info!("Shutting down http server worker runtime");
    });
    server.await
}

fn load_certs(cert_path: &str) -> io::Result<Vec<CertificateDer<'static>>> {
    let certfile = fs::File::open(cert_path)?;
    let mut reader = io::BufReader::new(certfile);

    let certs = rustls_pemfile::certs(&mut reader);
    certs.collect()
}

fn load_private_key(key_path: &str) -> io::Result<PrivateKeyDer<'static>> {
    let keyfile = fs::File::open(key_path)?;
    let mut reader = io::BufReader::new(keyfile);

    let mut keys: Vec<io::Result<PrivatePkcs1KeyDer<'static>>> =
        rustls_pemfile::rsa_private_keys(&mut reader).collect::<Vec<_>>();

    if keys.len() != 1 {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            String::from("Expected a single private key"),
        ));
    }

    keys.pop().unwrap().map(|key| key.into())
}

// enable TLS for AsyncPgConnection, see https://github.com/weiznich/diesel_async/blob/main/examples/postgres/pooled-with-rustls

fn establish_pg_ssl_connection(config: &str) -> BoxFuture<ConnectionResult<AsyncPgConnection>> {
    let fut = async {
        // We first set up the way we want rustls to work.
        let rustls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_certs())
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

fn root_certs() -> rustls::RootCertStore {
    let mut roots = rustls::RootCertStore::empty();
    let certs =
        rustls_native_certs::load_native_certs().expect("Failed to load native certificates");
    roots.add_parsable_certificates(certs);
    if let Some(ref pg_ssl_cert_path) = *PG_SSL_CERT_PATH {
        let certs =
            load_certs(pg_ssl_cert_path.as_str()).expect("Failed to load pg ssl certificate");
        roots.add_parsable_certificates(certs);
    }
    roots
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
            task::generate_missing_hls_streams,
        )
    });
    scheduler.every(clokwerk::Interval::Minutes(30)).run(|| {
        task::submit_task(
            "generate_missing_thumbnails",
            task::generate_missing_thumbnails,
        )
    });
    scheduler.every(clokwerk::Interval::Minutes(20)).run(|| {
        task::submit_task(
            "load_missing_object_metadata",
            task::load_missing_object_metadata,
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
            task::execute_deferred_s3_object_deletions,
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
                    eprintln!("Failed to start task scheduler runtime: {}", e);
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
