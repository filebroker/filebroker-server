use crate::broker::update::GetBrokerAuditLogsParams;
use crate::broker::{GetBrokerAccessParams, GetBrokersParams};
use crate::post::history::HistoryPaginationQueryParams;
use crate::query::QueryParametersFilter;
use crate::tag::GetTagsFilter;
use crate::user_group::history::AuditPaginationQueryParams;
use crate::user_group::invite::{GetCurrentUserGroupInvitesParams, GetUserGroupInvitesParams};
use crate::user_group::{
    GetCurrentUserGroupMembershipsParams, GetUserGroupBrokersParams, GetUserGroupMembersParams,
};
use crate::util::OptFmt;
use crate::{
    CERT_PATH, HTTP_SHUTDOWN_TIMEOUT, KEY_PATH, PG_SSL_CERT_PATH, PORT, auth, broker, data, error,
    perms, post, query, tag, task, user_group,
};
use futures::{Stream, StreamExt, TryFuture, stream};
use hyper_util::rt::TokioExecutor;
use hyper_util::{
    rt::TokioIo,
    server::{conn::auto::Builder as HyperServerBuilder, graceful::GracefulShutdown},
};
use mime::Mime;
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer};
use std::future::ready;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::{fs, io};
use tls_listener::TlsListener;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tower::Service;
use warp::{
    Filter, Reply,
    host::Authority,
    hyper::{Request, body, service::service_fn},
};

pub fn client_ip() -> impl Filter<Extract = (Option<IpAddr>,), Error = warp::Rejection> + Clone {
    warp::filters::ext::optional::<SocketAddr>()
        .and(warp::header::optional::<String>("x-forwarded-for"))
        .and(warp::header::optional::<String>("x-real-ip"))
        .map(
            |peer: Option<SocketAddr>, xff: Option<String>, x_real: Option<String>| {
                let peer_ip = peer.map(|p| p.ip());

                // 1. Prefer X-Real-IP
                if let Some(ip) = x_real.as_deref().and_then(|v| v.parse::<IpAddr>().ok()) {
                    return Some(ip);
                }

                // 2. X-Forwarded-For (right-most = nginx)
                if let Some(xff) = xff
                    && let Some(ip) = xff
                        .split(',')
                        .map(|s| s.trim())
                        .rev()
                        .find_map(|s| s.parse::<IpAddr>().ok())
                {
                    return Some(ip);
                }

                // 3. fallback
                peer_ip
            },
        )
}

pub async fn run_warp_server(http_worker_rt: Arc<Runtime>) {
    let filter = build_warp_filter();

    let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, *PORT))
        .await
        .expect("Failed to bind server to port");
    if CERT_PATH.is_some() && KEY_PATH.is_some() {
        let certs = load_certs(CERT_PATH.as_ref().unwrap()).expect("Failed to load TLS cert");
        let key =
            load_private_key(KEY_PATH.as_ref().unwrap()).expect("Failed to load TLS private key");
        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .expect("Failed to build TLS ServerConfig");
        let acceptor: TlsAcceptor = Arc::new(server_config).into();
        let incoming = TlsListener::new(acceptor, listener).filter(|conn| {
            if let Err(e) = conn {
                log::error!("Failed to open TLS connection: {e}");
                ready(false)
            } else {
                ready(true)
            }
        });
        log::info!("Enabled TLS");
        run_server(incoming, filter, http_worker_rt)
            .await
            .expect("Failed running server");
    } else {
        let incoming = stream::unfold(listener, |listener| async move {
            let accepted = listener.accept().await;
            Some((accepted, listener))
        });
        run_server(incoming, filter, http_worker_rt)
            .await
            .expect("Failed running server");
    }
}

pub async fn run_server<I, S, E, F>(
    incoming: I,
    filter: F,
    http_worker_rt: Arc<Runtime>,
) -> io::Result<()>
where
    I: Stream<Item = Result<(S, SocketAddr), E>> + Send + 'static,
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    E: std::fmt::Display,
    F: Filter + Clone + Send + Sync + 'static,
    F::Extract: Send,
    <F::Future as TryFuture>::Ok: Reply + Send,
{
    futures::pin_mut!(incoming);

    let warp_service = warp::service(filter);
    let graceful = GracefulShutdown::new();

    log::info!("Server listening on port {}", *PORT);

    loop {
        tokio::select! {
            _ = shutdown_signal() => {
                log::info!("Stopping TCP accept loop");
                break;
            }

            accepted = incoming.next() => {
                let Some(accepted) = accepted else {
                    break;
                };

                let (stream, remote_addr) = match accepted {
                    Ok(v) => v,
                    Err(e) => {
                        log::error!("TCP accept connection error: {e}");
                        continue;
                    }
                };

                let io = TokioIo::new(stream);
                let warp_service = warp_service.clone();
                let watcher = graceful.watcher();

                http_worker_rt.spawn(async move {
                    let service = service_fn(move |mut req: Request<body::Incoming>| {
                        let mut warp_service = warp_service.clone();

                        req.extensions_mut().insert(remote_addr);

                        async move {
                            warp_service.call(req).await
                        }
                    });

                    let builder = HyperServerBuilder::new(TokioExecutor::new());
                    let conn = builder.serve_connection(io, service);

                    if let Err(e) = watcher.watch(conn).await {
                        // Log connection error at debug level
                        // Connection errors for clients disconnecting while streaming are common and expected,
                        // and the source error will have already been logged by the warp filter
                        log::debug!("connection watcher: HTTP connection terminated with error: {e}");
                    }
                });
            }
        }
    }

    log::info!("Shutting down HTTP connections");
    match timeout(*HTTP_SHUTDOWN_TIMEOUT, graceful.shutdown()).await {
        Ok(()) => log::info!("HTTP connections drained"),
        Err(_) => log::warn!("Timed out waiting for HTTP connections to drain"),
    }

    shutdown_server().await;

    Ok(())
}

pub fn build_warp_filter() -> impl Filter<Extract = (impl Reply,), Error = warp::Rejection> + Clone
{
    let login_route = warp::path("login")
        .and(warp::post())
        .and(warp::body::json())
        .and(client_ip())
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
        .and(client_ip())
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
        .and_then(tag::create::create_tags_handler);

    let upsert_tag_route = warp::path("upsert-tag")
        .and(warp::post())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(tag::update::upsert_tag_handler);

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
        .and_then(broker::create::create_broker_handler);

    let get_available_brokers_route = warp::path("get-available-brokers")
        .and(warp::get())
        .and(auth::with_user())
        .and_then(broker::get_available_brokers_handler);

    let find_tag_route = warp::path("find-tag")
        .and(warp::get())
        .and(warp::path::param())
        .and_then(tag::find_tag_handler);

    let create_user_group_route = warp::path("create-user-group")
        .and(warp::post())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(user_group::create::create_user_group_handler);

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
        .and(client_ip())
        .and_then(auth::change_password_handler);

    let send_password_reset_route = warp::path("send-password-reset")
        .and(warp::post())
        .and(warp::body::json())
        .and(client_ip())
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
        .and_then(tag::get_tag_handler);

    let get_tags_route = warp::path("get-tags")
        .and(warp::get())
        .and(warp::query::<GetTagsFilter>())
        .and_then(tag::get_tags_handler);

    let update_tag_route = warp::path("update-tag")
        .and(warp::post())
        .and(warp::path::param::<i64>())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(tag::update::update_tag_handler);

    let get_tag_hierarchy_route = warp::path("get-tag-hierarchy")
        .and(warp::get())
        .and(warp::path::param::<i64>())
        .and_then(tag::get_tag_hierarchy_handler);

    let get_post_edit_history_route = warp::path("get-post-edit-history")
        .and(warp::get())
        .and(warp::query::<HistoryPaginationQueryParams>())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(post::history::get_post_edit_history_handler);

    let get_post_collection_edit_history_route = warp::path("get-post-collection-edit-history")
        .and(warp::get())
        .and(warp::query::<HistoryPaginationQueryParams>())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(post::history::get_post_collection_edit_history_handler);

    let rewind_post_history_snapshot_route = warp::path("rewind-post-history-snapshot")
        .and(warp::post())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(post::history::rewind_post_history_snapshot_handler);

    let rewind_post_collection_history_snapshot_route =
        warp::path("rewind-post-collection-history-snapshot")
            .and(warp::post())
            .and(warp::path::param())
            .and(auth::with_user())
            .and_then(post::history::rewind_post_collection_history_snapshot_handler);

    let get_tag_categories_route = warp::path("get-tag-categories")
        .and(warp::get())
        .and_then(tag::get_tag_categories_handler);

    let create_tag_category_route = warp::path("create-tag-category")
        .and(warp::post())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(tag::create::create_tag_category_handler);

    let update_tag_category_route = warp::path("update-tag-category")
        .and(warp::post())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(tag::update::update_tag_category_handler);

    let get_tag_edit_history_route = warp::path("get-tag-edit-history")
        .and(warp::get())
        .and(warp::query::<HistoryPaginationQueryParams>())
        .and(warp::path::param())
        .and_then(tag::history::get_tag_edit_history_handler);

    let rewind_tag_history_snapshot_route = warp::path("rewind-tag-history-snapshot")
        .and(warp::post())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(tag::history::rewind_tag_history_snapshot_handler);

    let get_presigned_hls_playlist_route = warp::path("get-presigned-hls-playlist")
        .and(warp::get())
        .and(warp::filters::path::peek())
        .and_then(data::get_presigned_hls_playlist_handler);

    let create_user_avatar_route = warp::path("create-user-avatar")
        .and(warp::post())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(data::create_user_avatar_handler);

    let get_current_user_group_memberships_route = warp::path("get-current-user-group-memberships")
        .and(warp::get())
        .and(warp::query::<GetCurrentUserGroupMembershipsParams>())
        .and(auth::with_user())
        .and_then(user_group::get_current_user_group_memberships_handler);

    let edit_user_group_route = warp::path("edit-user-group")
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(user_group::update::edit_user_group_handler);

    let get_user_group_route = warp::path("get-user-group")
        .and(warp::get())
        .and(warp::path::param())
        .and(auth::with_user_optional())
        .and_then(user_group::get_user_group_handler);

    let create_user_group_avatar_route = warp::path("create-user-group-avatar")
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(data::create_user_group_avatar_handler);

    let get_user_group_edit_history_route = warp::path("get-user-group-edit-history")
        .and(warp::get())
        .and(warp::query::<HistoryPaginationQueryParams>())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(user_group::history::get_user_group_history_handler);

    let rewind_user_group_history_snapshot_route = warp::path("rewind-user-group-history-snapshot")
        .and(warp::post())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(user_group::history::rewind_user_group_history_snapshot_handler);

    let create_user_group_invite_route = warp::path("create-user-group-invite")
        .and(warp::post())
        .and(warp::path::param())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(user_group::invite::create_user_group_invite_handler);

    let leave_user_group_route = warp::path("leave-user-group")
        .and(warp::delete())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(user_group::leave_user_group_handler);

    let get_user_public_route = warp::path("get-user-public")
        .and(warp::get())
        .and(warp::path::param())
        .and_then(auth::get_user_public_handler);

    let get_user_public_name_route = warp::path("get-user-public-name")
        .and(warp::get())
        .and(warp::path::param())
        .and_then(auth::get_user_public_name_handler);

    let redeem_user_group_invite_route = warp::path("redeem-user-group-invite")
        .and(warp::post())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(user_group::invite::redeem_user_group_invite_handler);

    let get_user_group_audit_logs_route = warp::path("get-user-group-audit-logs")
        .and(warp::get())
        .and(warp::query::<AuditPaginationQueryParams>())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(user_group::history::get_user_group_audit_logs_handler);

    let join_user_group_route = warp::path("join-user-group")
        .and(warp::post())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(user_group::invite::join_user_group_handler);

    let get_user_group_members_route = warp::path("get-user-group-members")
        .and(warp::get())
        .and(warp::path::param())
        .and(warp::query::<GetUserGroupMembersParams>())
        .and(auth::with_user())
        .and_then(user_group::get_user_group_members_handler);

    let get_user_group_invites_route = warp::path("get-user-group-invites")
        .and(warp::get())
        .and(warp::path::param())
        .and(warp::query::<GetUserGroupInvitesParams>())
        .and(auth::with_user())
        .and_then(user_group::invite::get_user_group_invites_handler);

    let get_current_user_group_invites_route = warp::path("get-current-user-group-invites")
        .and(warp::get())
        .and(warp::query::<GetCurrentUserGroupInvitesParams>())
        .and(auth::with_user())
        .and_then(user_group::invite::get_current_user_group_invites_handler);

    let get_user_group_brokers_route = warp::path("get-user-group-brokers")
        .and(warp::get())
        .and(warp::path::param())
        .and(warp::query::<GetUserGroupBrokersParams>())
        .and(auth::with_user())
        .and_then(user_group::get_user_group_brokers_handler);

    let change_user_group_membership_route = warp::path("change-user-group-membership")
        .and(warp::post())
        .and(warp::path::param())
        .and(warp::path::param())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(user_group::update::change_user_group_membership_handler);

    let revoke_user_group_invite_route = warp::path("revoke-user-group-invite")
        .and(warp::post())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(user_group::invite::revoke_user_group_invite_handler);

    let get_brokers_route = warp::path("get-brokers")
        .and(warp::get())
        .and(warp::query::<GetBrokersParams>())
        .and(auth::with_user())
        .and_then(broker::get_brokers_handler);

    let get_broker_route = warp::path("get-broker")
        .and(warp::get())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(broker::get_broker_handler);

    let edit_broker_route = warp::path("edit-broker")
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(broker::update::edit_broker_handler);

    let edit_broket_bucket_route = warp::path("edit-broket-bucket")
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(broker::update::edit_broket_bucket_handler);

    let verify_bucket_connection_route = warp::path("verify-bucket-connection")
        .and(warp::post())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(broker::verify_bucket_connection_handler);

    let get_broker_access_route = warp::path("get-broker-access")
        .and(warp::get())
        .and(warp::path::param())
        .and(warp::query::<GetBrokerAccessParams>())
        .and(auth::with_user())
        .and_then(broker::get_broker_access_handler);

    let get_broker_audit_logs_route = warp::path("get-broker-audit-logs")
        .and(warp::get())
        .and(warp::query::<GetBrokerAuditLogsParams>())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(broker::update::get_broker_audit_logs_handler);

    let create_broker_access_route = warp::path("create-broker-access")
        .and(warp::post())
        .and(warp::path::param())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(broker::create::create_broker_access_handler);

    let change_broker_access_quota_route = warp::path("change-broker-access-quota")
        .and(warp::post())
        .and(warp::path::param())
        .and(warp::path::param())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(broker::update::change_broker_access_quota_handler);

    let delete_broker_access_route = warp::path("delete-broker-access")
        .and(warp::delete())
        .and(warp::path::param())
        .and(warp::path::param())
        .and(auth::with_user())
        .and_then(broker::update::delete_broker_access_handler);

    let change_broker_access_admin_route = warp::path("change-broker-access-admin")
        .and(warp::post())
        .and(warp::path::param())
        .and(warp::path::param())
        .and(warp::body::json())
        .and(auth::with_user())
        .and_then(broker::update::change_broker_access_admin_handler);

    let auth_routes = login_route
        .or(refresh_login_route)
        .or(refresh_token_route)
        .or(try_refresh_login_route)
        .or(try_refresh_token_route)
        .or(logout_route)
        .or(register_route)
        .or(check_username_route);

    let user_routes = current_user_info_route
        .or(confirm_email_route)
        .or(edit_user_route)
        .or(send_email_confirmation_link_route)
        .or(change_password_route)
        .or(send_password_reset_route)
        .or(reset_password_route)
        .or(create_user_avatar_route)
        .or(get_user_public_route)
        .or(get_user_public_name_route);

    let query_routes = search_route.or(analyze_query_route);

    let post_routes = create_post_route
        .or(get_post_route)
        .or(get_posts_route)
        .or(edit_post_route)
        .or(delete_posts_route)
        .or(get_post_edit_history_route)
        .or(rewind_post_history_snapshot_route);

    let post_collection_routes = create_post_collection_route
        .or(get_post_collection_route)
        .or(edit_post_collection_route)
        .or(delete_post_collections_route)
        .or(get_post_collection_edit_history_route)
        .or(rewind_post_collection_history_snapshot_route);

    let tag_routes = create_tags_route
        .or(upsert_tag_route)
        .or(find_tag_route)
        .or(get_tag_route)
        .or(get_tags_route)
        .or(update_tag_route)
        .or(get_tag_hierarchy_route)
        .or(get_tag_categories_route)
        .or(create_tag_category_route)
        .or(update_tag_category_route)
        .or(get_tag_edit_history_route)
        .or(rewind_tag_history_snapshot_route);

    let object_routes = upload_route
        .or(get_object_metadata_route)
        .or(get_object_route)
        .or(get_object_head_route)
        .or(get_presigned_hls_playlist_route);

    let broker_routes = create_broker_route
        .or(get_available_brokers_route)
        .or(get_user_group_brokers_route)
        .or(get_brokers_route)
        .or(get_broker_route)
        .or(edit_broker_route)
        .or(edit_broket_bucket_route)
        .or(verify_bucket_connection_route)
        .or(get_broker_access_route)
        .or(get_broker_audit_logs_route)
        .or(create_broker_access_route)
        .or(change_broker_access_quota_route)
        .or(delete_broker_access_route)
        .or(change_broker_access_admin_route);

    let user_group_routes = create_user_group_route
        .or(get_current_user_groups_route)
        .or(get_current_user_group_memberships_route)
        .or(edit_user_group_route)
        .or(get_user_group_route)
        .or(create_user_group_avatar_route)
        .or(get_user_group_edit_history_route)
        .or(rewind_user_group_history_snapshot_route)
        .or(create_user_group_invite_route)
        .or(leave_user_group_route)
        .or(redeem_user_group_invite_route)
        .or(get_user_group_audit_logs_route)
        .or(join_user_group_route)
        .or(get_user_group_members_route)
        .or(get_user_group_invites_route)
        .or(get_current_user_group_invites_route)
        .or(change_user_group_membership_route)
        .or(revoke_user_group_invite_route);

    let routes = auth_routes
        .boxed()
        .or(user_routes.boxed())
        .or(query_routes.boxed())
        .or(post_routes.boxed())
        .or(post_collection_routes.boxed())
        .or(tag_routes.boxed())
        .or(object_routes.boxed())
        .or(broker_routes.boxed())
        .or(user_group_routes.boxed())
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
                "\"{} {} {:?}\" {} \"{}\" \"{}\" {:?}",
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

    filter
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigint = signal(SignalKind::interrupt()).expect("Failed to register SIGINT handler");
    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");

    tokio::select! {
        _ = sigint.recv() => {
            log::info!("Received SIGINT");
        }
        _ = sigterm.recv() => {
            log::info!("Received SIGTERM");
        }
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl-c");
    log::info!("Received Ctrl+C");
}

pub async fn shutdown_server() {
    log::info!("Shutting down server");
    log::info!("Waiting for scheduled tasks to finish");
    tokio::runtime::Handle::current()
        .spawn_blocking(task::shutdown_join)
        .await
        .expect("Failed to join scheduled tasks");
    log::info!("Shutting down http server worker runtime");
}

pub fn root_certs() -> rustls::RootCertStore {
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

pub fn load_certs(cert_path: &str) -> io::Result<Vec<CertificateDer<'static>>> {
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
        return Err(io::Error::other(String::from(
            "Expected a single private key",
        )));
    }

    keys.pop().unwrap().map(|key| key.into())
}
